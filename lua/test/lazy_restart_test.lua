-- fsync WAL recovery of pending delivery, prepared capacity and GC floors.
local fio, ffi = require('fio'), require('ffi')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local work, engine, stage = assert(arg[1]), assert(arg[2]), assert(arg[3])
fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'fsync',
        checkpoint_interval = 0, txn_isolation = 'read-confirmed', log_level = 3}
local tarn = require('tarn'); tarn.lazy.paused = true
-- Recovery is deliberately delayed so this fixture controls the known
-- committed participant. This does not turn transport loss into replay.
tarn.init{shard_id = 1, peers = {}, native = false, test_hooks = true, check_yields = true,
          recovery_after = 1000000}
-- Let the initial status watch and independent quota-rebuild worker run;
-- a caller never rebuilds/waits on its own transaction INFLIGHT marker.
require('fiber').sleep(0.03)
local meter_deadline = require('fiber').clock() + 3
while not tarn.lazy.quota.ready() and require('fiber').clock() < meter_deadline do require('fiber').sleep(0.01) end
assert(tarn.lazy.quota.ready(), 'lazy admission meter did not reach READY')
local sid, gen, name = 63001, 63002, 'test.lazy_restart'
local function ok(r) assert(r[1] == 'ok', require('json').encode(r)); return r end
local function counter() return ok(tarn_lazy_frontiers({1}, {1}))[2][1] end
local function write(w) return tarn_autocommit({w}, {fence = {sid, 0}, wb = {1}}) end
local function pending() return ok(tarn_lazy_control('pending'))[2] end
local function deliver()
    local events, route = pending(), {}
    if #events == 0 then return end
    for _, e in ipairs(events) do route[#route + 1] = e[5] end
    local ack = ok(tarn_lazy_apply(events, route)); ok(tarn_lazy_control('ack', ack[2]))
end
local function crash(label)
    print('  ok   ' .. engine .. ': ' .. label); io.stdout:flush()
    pcall(ffi.cdef, 'int getpid(void); int kill(int pid, int sig);')
    ffi.C.kill(ffi.C.getpid(), 9); error('SIGKILL returned')
end
if stage == 'pending' then
    ok(tarn_bucket_init(16, {{1, 16}}))
    local def = {id = sid, name = name, ver = 0, engine = engine, json = '{}',
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'string'}, {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{2, 'string'}}}}}
    ok(tarn_create_table(def))
    def.lazy = {{gen = gen, base_space_id = sid, local_index_id = 2, route_parts = 1,
                 key_fields = {2}, key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1},
                 pk_types = {'unsigned'}, bucket_field = 3}}
    ok(tarn_lazy_control('register', def))
    ok(write({'insert', sid, {1, 'old', 1}}))
    local vote = tarn_prepare('lazy-restart-committed', 1, {1}, {{'insert', sid, {2, 'prepared', 1}}},
                             {}, {}, require('clock').realtime(), nil, {sid, 0}, {1})
    assert(vote[1] == 'yes')
    assert(#pending() == 1 and tarn_lazy_stats()[2].reserved_entries == 1 and counter()[2] == 1)
    box.space._tarn_meta:replace{'test.lazy.replay', pending()[1]}
    crash('SIGKILL with durable row/outbox and a separate prepared reservation')
elseif stage == 'floor' then
    assert(box.space[sid]:get{1} ~= nil and box.space[sid]:get{2} == nil)
    assert(#pending() == 1 and tarn_lazy_stats()[2].reserved_entries == 1)
    assert(box.space._tarn_lazy_slots:get{1}.reserved == 1 and counter()[2] == 1)
    ok(tarn_apply('lazy-restart-committed'))
    assert(counter()[2] == 2 and #pending() == 2 and tarn_lazy_stats()[2].reserved_entries == 0)
    deliver()
    ok(write({'delete', sid, {1}})); deliver()
    local old = box.space._tarn_meta:get{'test.lazy.replay'}.value
    ok(tarn_lazy_gc(gen, old[5], {old[5]}))
    assert(box.space._tarn_lazy_floor:get{gen, old[5], 1}.seq == 3)
    crash('recovered prepared promise, drained delivery and persisted a GC floor')
elseif stage == 'finish' then
    local old = box.space._tarn_meta:get{'test.lazy.replay'}.value
    assert(counter()[2] == 3 and #pending() == 0 and box.space[sid]:get{1} == nil)
    assert(box.space[sid]:get{2} ~= nil and box.space._tarn_lazy_slots:get{1} == nil)
    ok(tarn_lazy_apply({old}, {old[5]}))
    assert(box.space[gen].index.ordered:count{true, 'old'} == 0)
    ok(write({'insert', sid, {1, 'new', 1}})); assert(counter()[2] == 4); deliver()
    assert(box.space[gen].index.ordered:count{true, 'new'} == 1)
    print('  ok   ' .. engine .. ': restart preserves floors and DELETE/reinsert sequence identity')
else error('unknown stage') end
os.exit(0)
