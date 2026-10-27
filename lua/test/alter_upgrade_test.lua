-- A separate process per stage models upgrading an already populated node.
-- Invocation: tarantool alter_upgrade_test.lua WORKDIR seed|upgrade|reopen
local fio = require('fio')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local work, stage = assert(arg[1]), assert(arg[2])
fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'write',
        checkpoint_interval = 0, log_level = 3}
local tarn = require('tarn').init{shard_id = 1, peers = {}, native = false}
-- Let the initial box.status callback finish its idempotent schema setup
-- before seed deliberately removes the metadata space being introduced.
require('fiber').sleep(0.02)

if stage == 'seed' then
    -- All old metadata already exists when the new metadata space is
    -- introduced. Its next auto-ID would otherwise collide with SQL's
    -- next reserved ID, above an existing high-ID user table.
    local undo = box.space._tarn_alter_undo
    if undo ~= nil then assert(undo:len() == 0); undo:drop() end
    local low = box.schema.SYSTEM_ID_MAX + 1
    while box.space[low] ~= nil do low = low + 1 end
    assert(low < 9999)
    local used_low = box.schema.space.create('_tarn_test_reserved_low', {id = low})
    used_low:create_index('primary', {parts = {{1, 'unsigned'}}})
    used_low:insert{1, 'occupied low internal ID'}
    low = low + 1
    while box.space[low] ~= nil do low = low + 1 end
    box.space._tarn_meta:replace{'test:expected_undo_space_id', low}
    local used = box.schema.space.create('_tarn_test_reserved', {id = 9999})
    used:create_index('primary', {parts = {{1, 'unsigned'}}})
    used:insert{1, 'occupied internal ID'}
    local old = box.schema.space.create('test.bf70_upgrade_existing', {id = 50100})
    old:create_index('primary', {parts = {{1, 'unsigned'}}})
    old:insert{1, 'existing user row'}
    tarn_counter_bump('space_id', 40100)
    assert(box.space._tarn_alter_undo == nil, 'seed must precede creation of the new undo space')
    print('  ok   upgrade seed has old metadata, an occupied internal ID and a high-ID user table')
elseif stage == 'upgrade' then
    local undo = assert(box.space._tarn_alter_undo, 'upgrade did not create the undo space')
    assert(box.schema.SYSTEM_ID_MAX == 511, 'unexpected native system-space ID boundary')
    assert(undo.id > box.schema.SYSTEM_ID_MAX and undo.id < 10000 and undo.id ~= 9999,
           'undo space must use a free internal ID, outside SQL allocation')
    assert(undo.id == box.space._tarn_meta:get{'test:expected_undo_space_id'}.value,
           'undo allocation must take the lowest free internal ID: got ' .. undo.id
           .. ', expected ' .. box.space._tarn_meta:get{'test:expected_undo_space_id'}.value)
    assert(box.space._tarn_test_reserved_low:get{1}[2] == 'occupied low internal ID')
    assert(box.space._tarn_test_reserved:get{1}[2] == 'occupied internal ID')
    assert(box.space[50100]:get{1}[2] == 'existing user row')
    assert(box.space[50101] == nil, 'undo allocation consumed the next SQL space ID')
    local next_id = 10000 + tarn_counter_alloc('space_id', 1)
    assert(next_id == 50101)
    local next_space = box.schema.space.create('test.bf70_upgrade_next', {id = next_id})
    next_space:create_index('primary', {parts = {{1, 'unsigned'}}})
    next_space:insert{1, 'next SQL row'}
    box.space._tarn_meta:replace{'test:undo_space_id', undo.id}
    print('  ok   upgrade uses a free internal ID and leaves the next SQL allocation usable')
elseif stage == 'reopen' then
    local undo = assert(box.space._tarn_alter_undo)
    assert(undo.id == box.space._tarn_meta:get{'test:undo_space_id'}.value,
           'reopening must preserve the existing undo space by name')
    assert(box.space[50101]:get{1}[2] == 'next SQL row')
    print('  ok   another restart preserves the undo space and the new SQL table')
else
    error('unknown upgrade stage ' .. stage)
end
os.exit(0)
