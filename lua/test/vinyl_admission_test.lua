-- Shared estimated credits and actual fresh-entry/decided-APPLY boundaries.
local fio, fiber, json, kd = require('fio'), require('fiber'), require('json'), require('key_def')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1048576,
    vinyl_memory = 16 * 1048576, vinyl_cache = 0, vinyl_timeout = .25,
    wal_mode = 'fsync', checkpoint_interval = 0, txn_isolation = 'read-confirmed', log_level = 3}
local tarn = require('tarn').init{shard_id = 1, peers = {}, native = false, test_hooks = true, check_yields = true}
fiber.sleep(.03)
local pass, fail = 0, 0
local function check(name, yes, extra)
    if yes then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. ' ' .. json.encode(extra)) end
end
local function ok(r) assert(r[1] == 'ok', json.encode(r)); return r end
local function def(id, engine)
    return {name = 'test.vinyl_admit_' .. engine, id = id, ver = 0, engine = engine, json = '{}',
        format = {{'id', 'unsigned'}, {'v', 'unsigned'}, {'pad', 'string'}, {'bucket_id', 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{4, 'unsigned'}}}}}
end
local sid, mid = 76001, 76002
ok(tarn_create_table(def(sid, 'vinyl'))); ok(tarn_create_table(def(mid, 'memtx')))
box.space[sid]:insert{1, 1, string.rep('w', 16384), 1}
local t, reads = 10, 0
local stat = {regulator = {rate_limit = 1000000, dump_bandwidth = 1000000, blocked_writers = 1, write_rate = 0},
              scheduler = {compaction_queue = 16 * 1048576}, memory = {level0 = 0, tuple = 0, tx = 0}}
local function idx(runs, ranges) return {stat = function() return {run_count = runs, range_count = ranges, run_avg = math.floor(runs / ranges)} end} end
local a, b = idx(1, 2), idx(3, 1)
local fake = {engine = 'vinyl', index = {[0] = a, pk = a, [1] = b, secondary = b}}
local v = require('tarn_vinyl').new{cfg = {test_hooks = true, vinyl_attempt_timeout = .25}, now = function() return t end,
    native_stats = function() reads = reads + 1; return stat end, memory_limit = function() return 16 * 1048576 end,
    spaces = function() return {[1] = fake, alias = fake} end, can_sample = function() return true end,
    key_def = function(s, i) return kd.new(box.space[s].index[i].parts) end}
local info = v.stats()
check('telemetry counts numeric aliases once and weights raw run/range counts',
      info.run_count == 4 and info.range_count == 3 and math.abs(info.runs_per_range - 4 / 3) < 1e-12)
local w = {{'insert', sid, {2, 2, 'x', 1}}}
local function attempt(ws)
    box.begin{txn_isolation = 'read-committed'}
    local r, d = v.guard(ws or w); box.rollback(); return r, d
end
local r = attempt()
check('entering pressure starts with zero shared credit', r and r[1] == 'throttled' and v.credit == 0, r)
t = t + .05; local before = 50000
r = attempt(); local charge = before - v.credit
check('elapsed refill admits a finite estimated debit', r == nil and charge > 0 and v.credit >= 0)
v.credit, v.last = charge, t
local one, two = attempt(), attempt()
check('two callers sharing exactly one debit cannot both spend it', one == nil and two and two[1] == 'throttled' and v.credit == 0, two)
v.reset(); r = attempt()
check('promotion/reset cannot manufacture a fresh full burst', r and r[1] == 'throttled' and v.credit == 0)
t = t + .05; v.credit, v.last, v.active = 50000, t, true
r = attempt({{'update', sid, {1}, {{'=', 2, 2}}}})
check('wide UPDATE charges unchanged old-row bytes instead of its tiny packet', r and r[1] == 'throttled', r)
stat.regulator.rate_limit = 18446744073709551615ULL; v.reset(); r = attempt()
check('SIZE_MAX/unrepresentable rates fail closed without rounding', r and r[1] == 'throttled' and v.stats().rate_limit == box.NULL)
stat.regulator.rate_limit = 0; stat.regulator.blocked_writers = 0; stat.scheduler.compaction_queue = 0
v.reset(); r = attempt()
check('a known zero rate remains closed even with otherwise idle gauges', r and r[1] == 'throttled')
stat.regulator.rate_limit = 1000000; v.reset(); reads = 0
attempt(); attempt(); local n = reads; t = t + .021; attempt()
check('guard snapshots expire after20ms and reset instead of sampling every healthy write', n == 1 and reads == 2)
local lease = {rate_limit = 0, dump_bandwidth = 0, blocked_writers = 1,
               memory_level0 = 0, memory_limit = 16 * 1048576, compaction_queue = 16 * 1048576}
ok(v.test_pressure('owner', lease, 1)); r = v.test_pressure('other', nil)
check('test pressure leases reject another live owner', r[1] == 'error')
t = t + 1.01; ok(v.test_pressure('other', nil))
check('expired test pressure leases clear safely', v.fixture == nil and v.credit == 0)
local writes = {{'insert', sid, {3, 3, 'prepared', 1}}}
local yes = tarn_prepare('vinyl-yes', 1, {1}, writes, {}, {}, fiber.time(), box.NULL, {sid, 0}, {1})
assert(yes[1] == 'yes', json.encode(yes))
ok(tarn.vinyl.test_pressure('entries', lease, 30))
local denied = tarn_autocommit(w, {fence = {sid, 0}, wb = {1}})
check('autocommit refusal is explicit and leaves no user write', denied[1] == 'throttled' and denied[2] == 1205
      and #denied == 4 and denied[4] >= 1 and denied[4] <= 50 and box.space[sid]:get{2} == nil, denied)
denied = tarn_commit_local('vinyl-local', w, {}, {}, box.NULL, {sid, 0}, {1})
check('local refusal rolls back its stream/native transaction', denied[1] == 'throttled'
      and not box.is_in_txn() and box.space[sid]:get{2} == nil, denied)
local lock = ok(tarn_lock('vinyl-rc', {{sid, 0, {2}}}, 'X', {timeout = 1}))
denied = tarn_commit_rc('vinyl-rc', w, lock[2], {sid, 0}, {1})
check('RC refusal releases held row locks and leaves no row', denied[1] == 'throttled'
      and tarn_info().row_locks == 0 and box.space[sid]:get{2} == nil, denied)
denied = tarn_autocommit({{'insert', mid, {2, 2, 'first', 1}}, w[1]}, {fence = {mid, 0, sid, 0}, wb = {1, 1}})
check('mixed-engine refusal happens before its first memtx write', denied[1] == 'throttled'
      and box.space[mid]:get{2} == nil and box.space[sid]:get{2} == nil, denied)
denied = tarn_prepare('vinyl-no', 1, {1}, w, {}, {}, fiber.time(), box.NULL, {sid, 0}, {1})
check('fresh PREPARE votes NO1205 with durable abort metadata but no business effect',
      denied[1] == 'no' and denied[2] == 1205 and box.space._tarn_part:get{'vinyl-no'}.state == 'aborted'
      and box.space._tarn_txn:get{'vinyl-no'}.status == 'aborted' and box.space[sid]:get{2} == nil, denied)
local rejected = tarn.vinyl.rejected
yes = tarn_prepare('vinyl-yes', 1, {1}, writes, {}, {}, fiber.time(), box.NULL, {sid, 0}, {1})
check('an existing durable YES bypasses new admission', yes[1] == 'yes' and tarn.vinyl.rejected == rejected)
assert(tarn_txn_cas('vinyl-yes', 'staging', 'committed', {1}) == 'committed')
local applied = tarn_apply('vinyl-yes')
check('decided APPLY bypasses pressure and fresh attempt deadlines', applied[1] == 'ok'
      and box.space[sid]:get{3} ~= nil and tarn.vinyl.rejected == rejected, applied)
ok(tarn.vinyl.test_pressure('entries', nil))
local pause = function(_, new) if new and new[1] == 5 then fiber.sleep(.03) end end
box.space[sid]:on_replace(pause)
local old_timeout = tarn.cfg.vinyl_attempt_timeout; tarn.cfg.vinyl_attempt_timeout = .01
denied = tarn_autocommit({{'insert', sid, {5, 5, 'first', 1}}, {'insert', sid, {6, 6, 'second', 1}}},
                         {fence = {sid, 0}, wb = {1, 1}})
tarn.cfg.vinyl_attempt_timeout = old_timeout; box.space[sid]:on_replace(nil, pause)
check('deadline after a partial physical batch rolls everything back with1205, never throttled',
      denied[1] == 'error' and denied[2] == 1205 and box.space[sid]:get{5} == nil and box.space[sid]:get{6} == nil, denied)
info = ok(tarn_vinyl_stats())[2]
check('RPC and tarn_info expose genuine native gauges and pressure counters',
      info.memory_limit == box.cfg.vinyl_memory and info.blocked_writers == 0
      and info.admitted > 0 and info.rejected >= 5 and tarn_info().vinyl.rate_limit == info.rate_limit)
print(string.format('%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
