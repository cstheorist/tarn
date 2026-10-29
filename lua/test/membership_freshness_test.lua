-- Membership ACKs restore local read policy without a background-clock race.
-- Private RF1 instance; synthetic old-peer evidence tests invalidation only,
-- not real peer-clock certification or RF3 replication.
local fio, fiber = require('fio'), require('fiber')
local here = fio.dirname(arg[0]); package.path = here .. '/../?.lua;' .. package.path
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 64 * 1048576,
        vinyl_memory = 16 * 1048576, wal_mode = 'fsync', txn_isolation = 'read-confirmed',
        checkpoint_interval = 0, log_level = 3}
local tarn = require('tarn')
tarn.init{shard_id = 1, peers = {}, native = false}
tarn.clock.stop() -- every readiness restoration below must be synchronous
local C, passed = tarn.clock, 0
local function check(yes, name) assert(yes, name); passed = passed + 1; print('ok ' .. name) end
local function membership(ver)
    return {ver = ver, shards = {{id = 1, replicas = {}, state = 'active'}}}
end
local function version() return box.space._tarn_meta:get{'members'}.value.ver end
local rows = box.schema.space.create('test.membership_fresh', {
    format = {{'id', 'unsigned'}, {'value', 'unsigned'}}})
rows:create_index('pk'); rows:insert{1, 10}
local function read()
    local r = tarn_select(rows.id, 0, {1}, 'EQ', 1, nil, true, true)
    return r[1] == 'ok' and r[2][1][2] == 10
end
check(C.loaded and C.read_check() == nil and read(), 'actual init starts with local policy and strong read ready')
local ticket = assert(C.gc_enter())
box.begin{txn_isolation = 'read-confirmed'}; assert(C.gc_commit(ticket) == nil); box.commit()
local high = box.space._tarn_meta:get{'clock:policy:1'}.value.high
local probe = C.probe()
assert(C.certify{epoch = C.epoch, started = probe.mono, peers = {
    {id = 'old-peer', low = 0, high = 0, max_age = tarn.cfg.max_txn_age,
     skew = tarn.cfg.clock_skew, local_ok = true}}} == nil)
assert(C.check({'old-peer'}) == nil)
local epoch = C.epoch
local r = tarn_members_set(membership(1))
check(r[1] == 'ok' and r[2] == 1 and C.epoch > epoch and C.loaded and read(),
      'new membership ACK is immediately strong-read ready with the clock worker stopped')
check(high > 0 and C.high >= high and box.space._tarn_meta:get{'clock:policy:1'}.value.high >= high,
      'post-membership confirmed reload never lowers the durable GC high-water mark')
check(C.check({'old-peer'}) ~= nil and C.check({1}) == nil,
      'membership clears old certificates before restoring the RF1 local writer')
C.invalidate('test existing-version readiness recovery')
r = tarn_members_set(membership(1))
check(r[1] == 'ok' and r[2] == 1 and version() == 1 and C.loaded and read(),
      'the existing-version branch restores readiness synchronously too')

local original_load = C.load
C.load = function() return false, {'clock_unknown', 1213, 'injected policy reload failure'} end
r = tarn_members_set(membership(2))
C.load = original_load
check(r[1] == 'error' and r[2] == 1180 and r[3]:find('version 2', 1, true)
      and r[3]:find('recorded and committed', 1, true) and r[3]:find('DDL job', 1, true),
      'post-commit reload failure reports recorded membership with nonretryable 1180')
check(version() == 2 and not C.loaded and not pcall(read),
      'readiness failure retains membership and cannot bypass the strong-read guard')
r = tarn_members_set(membership(2))
check(r[1] == 'ok' and r[2] == 2 and version() == 2 and read(),
      'an explicit existing-version recovery succeeds after reload becomes available')
C.load = function() error('injected policy reload exception') end
local called, result = pcall(tarn_members_set, membership(3))
C.load = original_load
check(called and result[1] == 'error' and result[2] == 1180 and version() == 3,
      'a raised post-commit reload error also preserves the recorded-effect status')
assert(tarn_members_set(membership(3))[1] == 'ok')
local original_check = C.read_check
C.read_check = function() return {'clock_unknown', 1213, 'injected final readiness failure'} end
r = tarn_members_set(membership(3))
C.read_check = original_check
check(r[1] == 'error' and r[2] == 1180 and version() == 3,
      'the final health check cannot turn an existing recorded version into a no-effect response')
assert(tarn_members_set(membership(3))[1] == 'ok')

box.begin{txn_isolation = 'read-confirmed'}
assert(rows:get{1}[2] == 10)
local caller = box.txn_id()
r = tarn_members_set(membership(4))
check(r[1] == 'error' and r[2] == 1210 and box.is_in_txn() and box.txn_id() == caller and version() == 3,
      'ambient membership call refuses before effects and preserves the exact caller transaction')
local writer = fiber.new(function() rows:replace{1, 20} end)
writer:set_joinable(true); assert(writer:join())
local committed = pcall(function() rows:replace{99, 99}; box.commit() end)
if box.is_in_txn() then box.rollback() end
check(not committed and rows:get{99} == nil and version() == 3,
      'ambient refusal also preserves the original caller read-set conflict')
print(string.format('%d passed, 0 failed', passed)); os.exit(0)
