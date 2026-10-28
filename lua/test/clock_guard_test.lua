-- Focused RecordExpiryPolicy refinement. Injects ONLY module-local clocks;
-- never changes the host clock. Peer intervals below are synthetic guard
-- controls, NOT evidence of real peer-clock bounds or RF3 replication.
local fio, ffi, fiber = require('fio'), require('ffi'), require('fiber')
io.stdout:setvbuf('no')
local here = fio.dirname(arg[0])
package.path = here .. '/../?.lua;' .. package.path
local work = arg[1] or fio.tempdir()
fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 64 * 1048576,
        wal_mode = 'fsync', checkpoint_interval = 0, txn_isolation = 'read-confirmed',
        log = work .. '/clock.log'}
local guard = require('tarn_clock')
local passed, n = 0, 0
local function check(name, yes)
    assert(yes, name); passed = passed + 1; print('  ok   ' .. name)
end
local function status(r, s) return type(r) == 'table' and r[1] == s end
local function new(sync)
    n = n + 1
    local meta = box.schema.space.create('clock_meta_' .. n, {
        format = {{'key', 'string'}, {'value', 'map'}}})
    meta:create_index('pk')
    local cfg = {max_txn_age = 10, clock_skew = 2, clock_max_gap = 5,
                 clock_max_drift = .25, test_hooks = true, sync = sync or false}
    local g = guard.new{cfg = cfg, meta = meta, required_peers = function() return {'p1', 'p2'} end}
    assert(g.test_time(1000, 100)[1] == 'ok')
    return g, cfg, meta
end
local function cert(g, ids)
    local peers = {}
    for _, id in ipairs(ids or {'p1', 'p2'}) do
        peers[#peers + 1] = {id = id, low = -.1, high = .1, max_age = 10, skew = 2, local_ok = true}
    end
    return {epoch = g.epoch, started = g.probe().mono, peers = peers}
end
local g, cfg, meta = new()
check('UNKNOWN cannot admit before confirmed policy installation', status(g.admit(1000), 'clock_unknown'))
check('RF1 initial trust follows confirmed policy and local observation', g.load() and g.check() == nil)
local p = meta:get{'clock:policy:1'}.value
check('immutable policy persists age, skew and detector settings', p.max_age == 10 and p.skew == 2
      and p.max_gap == 5 and p.max_drift == .25 and p.high == 0)
check('retention uses TWO skew margins', g.expiry() == 1014)
local err, epoch = g.admit(1000)
check('valid finite timestamp captures final-guard epoch', err == nil and epoch == g.epoch)
check('exact age boundary remains admissible; older is no-vote expired', g.admit(990) == nil
      and status(g.admit(989.99), 'expired'))
check('missing, nonfinite and future timestamps never become a fresh NO',
      status(g.admit(nil), 'clock_unknown') and status(g.admit(0 / 0), 'clock_unknown')
      and status(g.admit(math.huge), 'clock_unknown') and status(g.admit(1002.01), 'clock_unknown'))
local ok, mono, wall, gap, drift = g.native_state()
check('native mirror uses exact nanosecond uint64 values', ok and mono == 100000000000ULL
      and wall == 1000000000000ULL and gap == 5000000000ULL and drift == 250000000ULL)
local rows = box.schema.space.create('clock_rows', {format = {{'id', 'unsigned'}, {'n', 'unsigned'}}})
rows:create_index('pk'); rows:insert{1, 0}
box.begin{txn_isolation = 'read-confirmed'}
rows:get{1}; local txid = box.txn_id()
local loaded = g.load()
local healthy = g.check() == nil and g.admit(1000) == nil
check('guards and refused load preserve the caller transaction/readset', not loaded and healthy
      and box.is_in_txn() and box.txn_id() == txid)
local writer = fiber.create(function() rows:replace{1, 1} end); writer:set_joinable(true); assert(writer:join())
-- A read-only transaction may serialize before the writer. Tarn's RO
-- validation uses a probe write: becoming writable must retain the old read.
local committed = pcall(function() rows:replace{99, 99}; box.commit() end)
if box.is_in_txn() then box.rollback() end
check('tracked caller read still conflicts at validation probe', not committed and rows:get{99} == nil)

local ticket = assert(g.gc_enter())
check('GC high-water update refuses an unrelated no-transaction call', status(g.gc_commit(ticket), 'clock_unknown'))
rows:insert{2, 2}
box.begin{txn_isolation = 'read-confirmed'}
assert(g.gc_commit(ticket) == nil); rows:delete{2}; box.commit()
check('GC deletion and durable high-water commit together', rows:get{2} == nil
      and meta:get{'clock:policy:1'}.value.high == 1000 and g.pending == 0)
local restart = guard.new{cfg = cfg, meta = meta}
restart.test_time(999, 101)
check('restart cannot reset a persisted rejection floor after clock rollback', not restart.load()
      and status(restart.admit(990), 'clock_unknown'))
local tolerance = guard.new{cfg = cfg, meta = meta}; tolerance.test_time(999.9, 101)
check('persisted floor rejects old admission even inside bootstrap drift tolerance', tolerance.load()
      and status(tolerance.admit(989.95), 'expired'))

local a, acfg, ameta = new()
assert(a.load()); rows:insert{3, 3}
ticket = assert(a.gc_enter())
box.begin{txn_isolation = 'read-confirmed'}
assert(a.gc_commit(ticket) == nil); rows:delete{3}; box.rollback()
check('rolled-back GC retains receipt and durable prior watermark', rows:get{3} ~= nil
      and ameta:get{'clock:policy:1'}.value.high == 0 and a.pending == 0)
check('live rejection floor may conservatively retain rolled-back high-water', a.high == 1000)
ticket = assert(a.gc_enter())
box.begin()
local on_rollback = box.on_rollback
box.on_rollback = function() error('injected clock callback registration failure') end
local registered = pcall(a.gc_commit, ticket)
box.on_rollback = on_rollback
box.commit()
check('failed second callback registration cannot leak a GC terminal ticket or mutate policy',
      not registered and a.pending == 0 and ameta:get{'clock:policy:1'}.value.high == 0)
ticket = assert(a.gc_enter())
a.invalidate('test role change')
assert(a.load())
box.begin{txn_isolation = 'read-confirmed'}
err = a.gc_commit(ticket); box.rollback()
check('a GC plan cannot cross a role/health epoch even after recertification', status(err, 'clock_unknown'))
check('a paused PREPARE cannot reuse its earlier healthy epoch', status(a.admit(1000, ticket.epoch), 'clock_unknown'))
acfg.max_txn_age = 11
check('raising age cannot resurrect expired admission or authorize GC', status(a.admit(995), 'clock_unknown')
      and a.gc_enter() == nil and a.fault ~= nil)
check('decided completion can still compute expiry on a clock fault', a.expiry() == 1014)
check('fault flush never owns or rolls back an ambient transaction', (function()
    box.begin(); local id = box.txn_id(); local flushed = a.flush_fault()
    local same = box.is_in_txn() and box.txn_id() == id; box.rollback(); return not flushed and same
end)())
acfg.max_txn_age = 10
check('detected fault persists independently when the shard is writable', a.flush_fault()
      and type(ameta:get{'clock:policy:1'}.value.fault) == 'string')
local later = guard.new{cfg = acfg, meta = ameta}; later.test_time(1000, 100)
check('restart/good clock samples cannot automatically clear durable fault', not later.load()
      and later.fault ~= nil and not later.native_state())

local b = new(); assert(b.load()); b.test_time(1001, 100)
check('wall step while monotonic is unchanged latches unsafe', status(b.check(), 'clock_unknown') and b.fault ~= nil)
local d = new(); assert(d.load()); d.test_time(999, 101)
check('backward wall time latches unsafe', status(d.check(), 'clock_unknown') and d.fault ~= nil)
local e = new(); assert(e.load()); e.test_time(1006, 106)
local raw = e.probe()
check('raw probe remains available and checks a resumed peer before its worker', raw.wall == 1006
      and raw.local_ok == false and raw.fault ~= nil)
check('a resumed process must detect the monitor gap at request entry', status(e.check(), 'clock_unknown')
      and e.fault ~= nil and not e.native_state())
local f = new(); assert(f.load()); ticket = assert(f.gc_enter()); f.test_time(1001, 100)
box.begin(); err = f.gc_commit(ticket); box.rollback()
check('GC rechecks health after a yielded status/plan phase', status(err, 'clock_unknown'))

local r = new(true)
check('replicated policy installation alone cannot certify clock health', r.load()
      and status(r.check(), 'clock_unknown') and r.probe().wall == 1000)
check('missing peer clock evidence remains UNKNOWN', status(r.certify(cert(r, {'p1'})), 'clock_unknown'))
check('complete bounded peer intervals certify the current epoch', r.certify(cert(r)) == nil and r.check() == nil)
local uncertified = cert(r); uncertified.peers[1].local_ok = false
check('unhealthy remote local monitor cannot certify an otherwise narrow offset',
      status(r.certify(uncertified), 'clock_unknown') and r.fault == nil)
assert(r.certify(cert(r)) == nil)
r.test_time(1004, 104); assert(r.observe() == nil)
local native, _, _, remaining = r.native_state()
check('local sampling never extends a peer certificate native lifetime', native and remaining == 1000000000ULL)
r.test_time(1005.1, 105.1)
check('peer evidence expires even while local monitor samples remain timely', status(r.check(), 'clock_unknown')
      and not r.native_state())
check('a fresh complete certificate can recover UNKNOWN but not a latched fault', r.certify(cert(r)) == nil)
local wide = cert(r); wide.peers[1].low, wide.peers[1].high = -3, 3
check('network uncertainty spanning the bound is UNKNOWN, not a proven clock violation',
      status(r.certify(wide), 'clock_unknown') and r.fault == nil)
assert(r.certify(cert(r)) == nil)
local bad = cert(r); bad.peers[1].low, bad.peers[1].high = 2.1, 2.2
check('observed peer bound violation latches unsafe', status(r.certify(bad), 'clock_unknown')
      and r.fault ~= nil and status(r.certify(cert(r)), 'clock_unknown'))
local no_hooks = guard.new{cfg = {test_hooks = false}}
check('clock injection requires actual enabled test_hooks', status(no_hooks.test_time(1, 1), 'error'))
local ro = new(); assert(ro.load())
box.cfg{read_only = true}
local refused_ro = status(ro.check(), 'clock_unknown') and not ro.native_state()
box.cfg{read_only = false}
check('read-only role closes the clock provider without latching a clock fault', refused_ro and ro.fault == nil)
print(string.format('%d passed, 0 failed', passed))
os.exit(0)
