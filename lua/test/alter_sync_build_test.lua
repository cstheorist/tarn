-- An index ALTER TABLE adds to a vinyl table of a replicated shard misses no
-- row (issue #263). Tarantool's vinyl index build waits for the transactions
-- already in the WAL, not for their quorum: a synchronous write that changed
-- a row before the build began and still waits for its quorum is neither in
-- the build's read view nor caught by its trigger, and the new index never
-- gets the row's new key. So BUILD leaves such an index to PREPARE, which
-- builds it behind the fence once the table's commits have drained.
--
-- One node with synchronous spaces: raising replication_synchro_quorum above
-- the replica set keeps a write waiting for its quorum.
local fiber, json = require('fiber'), require('json')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local dir = assert(arg[1]); require('fio').mktree(dir)
box.cfg{work_dir = dir, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'write',
        replication_synchro_quorum = 1, replication_synchro_timeout = 1000, election_mode = 'off',
        txn_isolation = 'read-confirmed', log_level = 3}
box.ctl.promote()
local tarn = require('tarn').init{shard_id = 1, peers = {}, sync = true, test_hooks = true,
                                  check_yields = true, native = false}
local ready = fiber.clock() + 10
while (box.space._tarn_buckets == nil or box.info.ro or tarn.clock.read_check() ~= nil)
      and fiber.clock() < ready do fiber.sleep(0.01) end
assert(box.space._tarn_buckets ~= nil and tarn.clock.read_check() == nil, 'the shard did not become ready')
local passed, failed = 0, 0
local function check(label, yes, extra)
    if yes then passed = passed + 1; print('  ok   ' .. label)
    else failed = failed + 1; print('  FAIL ' .. label .. ' ' .. json.encode(extra)) end
end
local function ok(r) assert(r[1] == 'ok', json.encode(r)); return r end
ok(tarn_bucket_init(16, {{1, 16}}))

local name = 'test.sync_build'
local format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'unsigned'},
                {name = 'bucket_id', type = 'unsigned'}}
local indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}},
                 {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}}}
ok(tarn_create_table({name = name, engine = 'vinyl', format = format, indexes = indexes}))
local sp = box.space[name]
check('the table is synchronous', sp.is_sync == true)
local ROWS = 3000
for start = 1, ROWS, 500 do
    local writes = {}
    for id = start, math.min(start + 499, ROWS) do writes[#writes + 1] = {'insert', sp.id, {id, id % 7, 1}} end
    ok(tarn_autocommit(writes))
end

local parts = {{field = 2, type = 'unsigned'}, {field = 1, type = 'unsigned'}}
local all = table.deepcopy(indexes)
all[3] = {name = 'v_idx', unique = false, parts = parts}
local alt = {ver = 1, timeout = 5, checks = {}, builds = {{2, 'v_idx', false, parts}}, drop = {}, renames = {},
             def = {name = name, id = sp.id, engine = 'vinyl', format = format, indexes = all, json = '{}', ver = 1}}

-- A write that changes row 886's key waits for a quorum the shard lacks.
box.cfg{replication_synchro_quorum = 2}
local wrote
local writer = fiber.new(function()
    wrote = tarn_autocommit({{'update', sp.id, {886}, {{'=', 2, 6}}}}, {fence = {sp.id, 0}})
end)
writer:set_joinable(true)
local deadline = fiber.clock() + 5
while box.info.synchro.queue.len == 0 and fiber.clock() < deadline do fiber.sleep(0.01) end
check('the write waits for its quorum', box.info.synchro.queue.len == 1 and wrote == nil)

-- (A BUILD that builds returns only once the write commits: its DDL is
-- queued behind it.)
local built
local builder = fiber.new(function() built = tarn_alter_build(name, alt) end)
builder:set_joinable(true)
deadline = fiber.clock() + 2
while built == nil and fiber.clock() < deadline do fiber.sleep(0.01) end
check('BUILD of a replicated vinyl table leaves the index to PREPARE',
      built ~= nil and built[1] == 'ok' and sp.index[2] == nil, {built, sp.index[2] and sp.index[2].name})

box.cfg{replication_synchro_quorum = 1}
builder:join()
writer:join()
check('the write commits once its quorum is met', wrote ~= nil and wrote[1] == 'ok', wrote)

ok(tarn_alter_prepare(name, alt, 1))
check('PREPARE builds the index', sp.index[2] ~= nil and sp.index[2].name == '_tarn_tmp_2')
if sp.index[2] == nil then
    print(string.format('%d passed, %d failed', passed, failed))
    os.exit(1)
end
local np, ni, hit = 0, 0, false
for _ in sp.index[0]:pairs() do np = np + 1 end
for _, t in sp.index[2]:pairs() do
    ni = ni + 1
    if t[1] == 886 then hit = t[2] == 6 end
end
check('the index holds every row', np == ROWS and ni == ROWS, {primary = np, index = ni})
check('... with the row the waiting write changed under its new key', hit)
ok(tarn_alter_apply(name, alt, 1))
check('APPLY names the index', sp.index[2].name == 'v_idx')

print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
