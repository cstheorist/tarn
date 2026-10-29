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
local function new(sync, public_clock)
    n = n + 1
    local meta = box.schema.space.create('clock_meta_' .. n, {
        format = {{'key', 'string'}, {'value', 'map'}}})
    meta:create_index('pk')
    local cfg = {max_txn_age = 10, clock_skew = 2, clock_max_gap = 5,
                 clock_max_drift = .25, test_hooks = true, sync = sync or false}
    local g = guard.new{cfg = cfg, meta = meta,
        required_peers = function() return sync and {'p1', 'p2'} or {} end,
        can_run = function() return cfg.monitor_allowed ~= false end}
    if not public_clock then assert(g.test_time(1000, 100)[1] == 'ok') end
    return g, cfg, meta
end
-- Stub only the public Lua clock functions for exact integer controls. No
-- production hook, OS clock change, or background worker is involved.
local function with_clock64(run)
    local source = require('clock')
    local old_wall, old_mono = source.realtime64, source.monotonic64
    local wall, mono, wall_reads, mono_reads
    local function set(w, m) wall, mono, wall_reads, mono_reads = w, m, 0, 0 end
    set(1000000000000ULL, 100000000000ULL)
    source.realtime64 = function() wall_reads = wall_reads + 1; return ffi.cast('int64_t', wall) end
    source.monotonic64 = function() mono_reads = mono_reads + 1; return ffi.cast('int64_t', mono) end
    local ok, err = pcall(run, set, function() return wall_reads, mono_reads end)
    source.realtime64, source.monotonic64 = old_wall, old_mono
    assert(ok, err)
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
restart.test_time(997, 101)
check('restart cannot reset a persisted rejection floor after clock rollback', not restart.load()
      and status(restart.admit(990), 'clock_unknown'))
local tolerance = guard.new{cfg = cfg, meta = meta}; tolerance.test_time(999.9, 101)
check('persisted floor rejects old admission even inside bootstrap drift tolerance', tolerance.load()
      and status(tolerance.admit(989.95), 'expired'))
local slower = guard.new{cfg = cfg, meta = meta}; slower.test_time(998.5, 101)
check('a valid slower replacement clock uses persisted H without a spurious clock fault', slower.load()
      and slower.fault == nil and status(slower.admit(989.95), 'expired'))

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
check('pure native policy eligibility sees configuration mutation before monitor publication',
      not a.policy_ok() and not a.read_native_state())
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
local diagnostic = b.status().fault_observation
check('drift diagnostic retains exact samples and the unchanged detector operands',
      b.fault == 'wall/monotonic drift exceeded' and diagnostic.kind == 'wall_monotonic_drift'
      and diagnostic.previous_wall_ns == '1000000000000' and diagnostic.observed_wall_ns == '1001000000000'
      and diagnostic.previous_mono_ns == '100000000000' and diagnostic.observed_mono_ns == '100000000000'
      and diagnostic.wall_delta_ns == '1000000000' and diagnostic.mono_delta_ns == '0'
      and diagnostic.wall_delta_seconds == 1 and diagnostic.mono_delta_seconds == 0
      and diagnostic.wall_minus_mono_delta_seconds == 1 and diagnostic.max_drift_seconds == .25)
local d = new(); assert(d.load()); d.test_time(999, 101)
check('backward wall time latches unsafe', status(d.check(), 'clock_unknown') and d.fault ~= nil)
diagnostic = d.status().fault_observation
check('backward diagnostic identifies the clock and exact signed displacement',
      d.fault == 'clock moved backward' and diagnostic.kind == 'wall_backward'
      and diagnostic.observed_wall_ns == '999000000000' and diagnostic.observed_mono_ns == '101000000000'
      and diagnostic.wall_delta_ns == '-1000000000' and diagnostic.mono_delta_ns == '1000000000')
local first_epoch = diagnostic.observation_epoch
diagnostic.observed_wall_ns = 'caller mutation'
d.test_time(998, 102)
local first = d.probe().fault_observation
check('later samples and diagnostic callers cannot overwrite the first rejected observation',
      first.observed_wall_ns == '999000000000' and first.observed_mono_ns == '101000000000'
      and first.observation_epoch == first_epoch and d.fault == 'clock moved backward')
local backwards_mono = new(); assert(backwards_mono.load()); backwards_mono.test_time(1000, 99)
backwards_mono.check()
check('diagnostic distinguishes monotonic-only and simultaneous backward movement',
      backwards_mono.status().fault_observation.kind == 'monotonic_backward' and (function()
          local both = new(); assert(both.load()); both.test_time(999, 99); both.check()
          return both.status().fault_observation.kind == 'wall_and_monotonic_backward'
      end)())
local large = new(); large.test_time(2000000000, 100); assert(large.load())
large.test_time(1999999999, 101); large.check()
check('nanosecond diagnostic strings retain integers beyond double exact-integer range',
      large.status().fault_observation.previous_wall_ns == '2000000000000000000'
      and large.status().fault_observation.observed_wall_ns == '1999999999000000000'
      and large.status().fault_observation.wall_delta_ns == '-1000000000')
local diagnostic_failure = new(); assert(diagnostic_failure.load()); diagnostic_failure.test_time(999, 101)
local original_gsub = string.gsub
string.gsub = function() error('injected diagnostic formatting failure') end
local guarded, rejected = pcall(diagnostic_failure.check)
string.gsub = original_gsub
check('diagnostic formatting failure cannot prevent the original fault latch and rejection',
      guarded and status(rejected, 'clock_unknown') and diagnostic_failure.fault == 'clock moved backward'
      and diagnostic_failure.status().fault_observation == nil
      and status(diagnostic_failure.check(), 'clock_unknown') and not diagnostic_failure.read_native_state())
with_clock64(function(set, reads)
    local corrected = new(false, true); assert(corrected.load())
    set(999875000000ULL, 100125000000ULL)
    check('a backward raw correction at the unchanged drift boundary retains logical time',
          corrected.check() == nil and corrected.fault == nil and corrected.status().wall == 1000
          and corrected.status().accepted_raw_wall_ns == '999875000000')
    local probe = corrected.probe()
    local _, native_mono, native_wall = corrected.read_native_state()
    check('probe status and native mirror publish the same accepted logical pair',
          probe.wall_ns == corrected.status().wall_ns and probe.mono_ns == corrected.status().mono_ns
          and native_wall == 1000000000000ULL and native_mono == 100125000000ULL
          and probe.wall_ns == '1000000000000' and probe.mono_ns == '100125000000')
    set(999875000000ULL, 100125000000ULL)
    probe = corrected.probe()
    local wall_reads, mono_reads = reads()
    check('one probe takes one validated sample and no later raw sample',
          wall_reads == 1 and mono_reads == 1 and probe.local_ok and probe.wall == 1000)
    set(1000000000000ULL, 100000000000ULL)
    local over = new(false, true); assert(over.load())
    set(999875000000ULL, 100125000001ULL)
    check('one nanosecond beyond the negative drift boundary retains the old backward fault',
          status(over.check(), 'clock_unknown') and over.fault == 'clock moved backward')
    set(1000000000000ULL, 100000000000ULL)
    local zero, zero_cfg = new(false, true); zero_cfg.clock_max_drift = 0; assert(zero.load())
    check('zero drift permits equal raw and logical samples', zero.check() == nil)
    set(999999999999ULL, 100000000000ULL)
    check('zero drift still rejects a distinguishable raw backward step',
          status(zero.check(), 'clock_unknown') and zero.fault == 'clock moved backward')
end)
with_clock64(function(set)
    set(1800000000000000000ULL, 100000000000ULL)
    local correction = new(false, true); assert(correction.load())
    set(1799999999999200319ULL, 100000096750ULL)
    check('the observed 799681ns wall correction with 96750ns monotonic advance stays within drift',
          correction.check() == nil and correction.fault == nil
          and correction.status().wall_ns == '1800000000000000000'
          and correction.status().accepted_raw_wall_ns == '1799999999999200319')
    set(1800000000000000000ULL, 4000000000000000000ULL)
    local precision = new(false, true); assert(precision.load())
    set(1799999999999999999ULL, 4000000000000000000ULL)
    local before = tonumber(1800000000000000000ULL)
    check('double-equal wall samples still retain the exact uint64 logical maximum',
          before == tonumber(1799999999999999999ULL) and precision.check() == nil
          and precision.status().wall_ns == '1800000000000000000'
          and precision.status().accepted_raw_wall_ns == '1799999999999999999')
    set(1799999999999999999ULL, 3999999999999999999ULL)
    check('double-equal monotonic samples still reject an exact one-nanosecond reversal',
          tonumber(4000000000000000000ULL) == tonumber(3999999999999999999ULL)
          and status(precision.check(), 'clock_unknown') and precision.fault == 'clock moved backward')
end)
with_clock64(function(set)
    local epoch_wall, epoch_mono = 1793000000000000000ULL, 4000000000000000000ULL
    set(epoch_wall, epoch_mono)
    local boundary = new(false, true); assert(boundary.load())
    set(epoch_wall - 250000000ULL, epoch_mono)
    check('epoch-scale exact negative drift at the nanosecond budget remains admissible',
          boundary.check() == nil and boundary.fault == nil
          and boundary.status().wall_ns == '1793000000000000000')
    set(epoch_wall, epoch_mono)
    local rounded = new(false, true); assert(rounded.load())
    set(epoch_wall - 250000001ULL, epoch_mono)
    check('epoch rounding cannot admit a negative drift violation one nanosecond over budget',
          status(rounded.check(), 'clock_unknown') and rounded.fault == 'clock moved backward'
          and rounded.status().fault_observation.wall_delta_ns == '-250000001')
    set(epoch_wall + 129ULL, epoch_mono)
    local hidden = new(false, true); assert(hidden.load())
    set(epoch_wall + 129ULL - 250000001ULL, epoch_mono)
    check('exact negative drift refusal also covers the adjacent epoch rounding bucket',
          status(hidden.check(), 'clock_unknown') and hidden.fault == 'clock moved backward')
    set(epoch_wall + 129ULL, epoch_mono)
    local forward = new(false, true); assert(forward.load())
    set(epoch_wall + 129ULL + 250000001ULL, epoch_mono)
    check('exact forward drift also refuses a one-nanosecond epoch rounding violation',
          status(forward.check(), 'clock_unknown') and forward.fault == 'wall/monotonic drift exceeded')
    set(epoch_wall, epoch_mono)
    local zero, settings = new(false, true); settings.clock_max_drift = 0; assert(zero.load())
    set(epoch_wall - 1ULL, epoch_mono)
    check('zero drift refuses a double-equal one-nanosecond raw backward correction',
          tonumber(epoch_wall) == tonumber(epoch_wall - 1ULL)
          and status(zero.check(), 'clock_unknown') and zero.fault == 'clock moved backward')
    set(epoch_wall, epoch_mono)
    local gap = new(true, true); assert(gap.load()); assert(gap.certify(cert(gap)) == nil)
    set(epoch_wall + 5000000001ULL, epoch_mono + 5000000001ULL)
    local gap_result = gap.observe()
    check('an exact gap one nanosecond over budget remains UNKNOWN despite double rounding',
          tonumber(epoch_mono + 5000000001ULL) / 1000000000 - tonumber(epoch_mono) / 1000000000 == 5
          and status(gap_result, 'clock_unknown') and gap_result[3] == 'Clock safety unavailable: clock monitor gap exceeded'
          and gap.fault == nil and not gap.native_state() and gap.status().covered_peer_count == 0)
    check('exact gap detection clears old certificates before local health can recover',
          gap.read_check() == nil and status(gap.check(), 'clock_unknown') and gap.read_native_state())
end)
do
    local ratchet = new(); assert(ratchet.load())
    ratchet.test_time(999.9375, 100.0625); assert(ratchet.check() == nil)
    ratchet.test_time(999.8125, 100.125); assert(ratchet.check() == nil)
    ratchet.test_time(999.75, 100.1875)
    check('repeated corrections compare against the logical maximum and cannot ratchet downward',
          status(ratchet.check(), 'clock_unknown') and ratchet.fault == 'clock moved backward')
    local observed = ratchet.status().fault_observation
    check('fault diagnostics distinguish previous raw time from the logical detector baseline',
          observed.previous_wall_ns == '999812500000' and observed.previous_logical_wall_ns == '1000000000000'
          and observed.wall_delta_ns == '-62500000' and observed.wall_from_logical_delta_ns == '-250000000')
    local floor = new(); assert(floor.load()); floor.high = 1002.2
    floor.test_time(999.875, 100.125)
    check('raw persisted-floor validation precedes logical clipping', status(floor.check(), 'clock_unknown')
          and floor.fault == 'clock precedes persisted high-water mark'
          and floor.status().fault_observation.kind == 'persisted_floor')
    local sticky = new(); assert(sticky.load()); sticky.fault_now('existing fault')
    local old_epoch = sticky.epoch
    sticky.test_time(999.875, 100.125)
    check('an allowed raw correction cannot clear or recertify an existing latched fault',
          status(sticky.check(), 'clock_unknown') and sticky.fault == 'existing fault'
          and sticky.epoch == old_epoch and not sticky.read_native_state())
    local bounded = new(true); assert(bounded.load()); assert(bounded.certify(cert(bounded)) == nil)
    bounded.test_time(1004, 104); assert(bounded.observe() == nil)
    bounded.test_time(1003.9375, 104.0625); assert(bounded.observe() == nil)
    local certified, _, logical_wall, remaining = bounded.native_state()
    check('clipping wall time never renews a peer certificate lifetime', certified
          and logical_wall == 1004000000000ULL and remaining == 937500000ULL)
    bounded.test_time(1005.0625, 105.0625)
    check('logical wall correction cannot revive an expired peer certificate',
          status(bounded.check(), 'clock_unknown') and not bounded.native_state() and bounded.read_native_state())
end
local e = new(); assert(e.load()); e.test_time(1006, 106)
local raw = e.probe()
check('raw probe remains available and checks a resumed peer before its worker', raw.wall == 1006
      and raw.local_ok == false and raw.fault == nil)
check('healthy and ordinary-gap observations do not create fault diagnostics',
      g.status().fault_observation == nil and raw.fault_observation == nil and e.status().fault_observation == nil)
check('a resumed process must detect the monitor gap at request entry', status(e.check(), 'clock_unknown')
      and e.fault == nil and not e.native_state())
check('normal pause uncertainty can regain local health before full peer certification', e.read_check() == nil
      and e.read_native_state() and status(e.check(), 'clock_unknown'))
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
local partial = new(true); assert(partial.load()); assert(partial.certify(cert(partial)) == nil)
local relevant, irrelevant = assert(partial.gc_enter({'p1'})), assert(partial.gc_enter({'p2'}))
local original_epoch = partial.epoch
partial.test_time(1003, 103); assert(partial.observe() == nil)
assert(partial.certify(cert(partial, {'p1'})) == nil)
partial.test_time(1005.1, 105.1); assert(partial.observe() == nil)
check('unrelated expired coverage is globally UNKNOWN without invalidating current relevant evidence',
      status(partial.certify(cert(partial, {'p1'})), 'clock_unknown')
      and partial.epoch == original_epoch and partial.status().state == 'unknown'
      and partial.check({'p1'}) == nil and partial.read_check() == nil and partial.read_native_state())
check('wrong stable writer identity and expired relevant interval fail closed',
      status(partial.check({'wrong'}), 'clock_unknown') and status(partial.check({'p2'}), 'clock_unknown'))
box.begin(); local relevant_ok = partial.gc_commit(relevant); box.rollback()
check('unrelated peer loss does not invalidate an otherwise-current relevant GC ticket', relevant_ok == nil)
box.begin(); local irrelevant_no = partial.gc_commit(irrelevant); box.rollback()
check('GC rechecks relevant coverage after a yielded plan', status(irrelevant_no, 'clock_unknown'))
local ro = new(); assert(ro.load())
box.cfg{read_only = true}
local refused_ro = status(ro.check(), 'clock_unknown') and not ro.native_state()
box.cfg{read_only = false}
check('read-only role closes the clock provider without latching a clock fault', refused_ro and ro.fault == nil)
local heap, hcfg = new(); assert(heap.load()); local observed = heap.status().mono
hcfg.monitor_allowed = false; heap.test_time(1006, 106)
check('heap preflight skips all monitor/certificate renewal without clearing policy or faults',
      heap.tick() == false and heap.status().mono == observed and not heap.policy_ok() and heap.fault == nil)
hcfg.monitor_allowed = true
check('a skipped monitor leaves the elapsed-gap guard to fail closed on resumption',
      status(heap.read_check(), 'clock_unknown') and heap.fault == nil)
local real_meta = box.schema.space.create('clock_meta_real', {format = {{'key', 'string'}, {'value', 'map'}}})
real_meta:create_index('pk')
local real_cfg = {test_hooks = false}
local real = guard.new{cfg = real_cfg, meta = real_meta}
assert(real.load())
local ready, actual_mono, actual_wall = real.read_native_state()
check('actual public signed clock64 values publish exact nonnegative unsigned native mirrors', ready
      and ffi.istype('uint64_t', actual_mono) and ffi.istype('uint64_t', actual_wall)
      and actual_mono > 0 and actual_wall > 0)
-- Differential compatibility with the former cfg()+matches() eligibility:
-- loaded defaults, exact boundaries, false/string and equal numeric cdata.
-- No observe() is called while testing mutations, so no unsafe policy can
-- get installed or a sticky fault obscure the comparison.
local defaults = {max_txn_age = 120, clock_skew = 60, clock_max_gap = 2, clock_max_drift = .25}
local function previous(settings)
    local function value(k) return settings[k] == nil and defaults[k] or settings[k] end
    local p = {max_age = value('max_txn_age'), skew = value('clock_skew'),
        max_gap = value('clock_max_gap'), max_drift = value('clock_max_drift')}
    local function valid(x) return type(x) == 'number' and x == x and x >= 0 and x <= 9007199254 end
    return valid(p.max_age) and p.max_age > 0 and valid(p.skew)
        and valid(p.max_gap) and p.max_gap > 0 and valid(p.max_drift) and p.max_drift < p.max_gap
        and p.max_age + 2 * p.skew <= 9007199254
        and p.max_age == 120 and p.skew == 60 and p.max_gap == 2 and p.max_drift == .25
end
local matrix = {{}, defaults}
for key, original in pairs(defaults) do
    for _, x in ipairs({0, -1, original, original + 1, 0 / 0, math.huge, -math.huge,
                       9007199254, 9007199255, false, tostring(original), ffi.new('int64_t', original),
                       ffi.new('uint64_t', original), {}}) do
        local s = {}; for k,v in pairs(defaults) do s[k] = v end
        s[key] = x; matrix[#matrix+1] = s
    end
    local s = {}; for k,v in pairs(defaults) do s[k] = v end
    s[key] = nil; matrix[#matrix+1] = s
end
for _, settings in ipairs(matrix) do
    for k in pairs(defaults) do real_cfg[k] = settings[k] end
    assert(real.policy_ok() == previous(settings), 'scalar eligibility differs from validated policy contract')
end
for k in pairs(defaults) do real_cfg[k] = nil end
check('scalar native policy check preserves all 62 default/type/boundary eligibility cases',
      #matrix == 62 and real.policy_ok())
local completion, completion_cfg, completion_meta = new(); assert(completion.load())
local ct, ce = completion.record_time()
check('a new receipt samples one timestamp and its two-skew horizon', ct == 1000 and ce == 1014)
completion.test_time(900, 101); completion.check()
check('decided completion keeps the last valid and policy-created wall floor on a backward fault',
      completion.fault ~= nil and completion.now() == 1000)
local prior = {ts = 1100, expires = 1200}
ct, ce = completion.record_time(prior)
check('a prior leader receipt preserves its own later timestamp and longer expiry independently',
      ct == 1100 and ce == 1200 and completion.status().state == 'fault')
local real_now, sampled = completion.now, 0
completion.now = function() sampled = sampled + 1; return sampled == 1 and 1105 or 900 end
ct, ce = completion.record_time{ts = 1100, expires = 1114}
completion.now = real_now
check('completion timestamp and expiry use the same one clock sample',
      sampled == 1 and ct == 1105 and ce == 1119)
ct, ce = completion.record_time{ts = 1100, expires = 1114}
completion_meta:replace{'terminal', {ts = ct, expires = ce, state = 'applied'}}
local successor = guard.new{cfg = completion_cfg, meta = completion_meta}
assert(successor.test_time(1101, 200)[1] == 'ok' and successor.load())
local terminal = completion_meta:get{'terminal'}.value
check('loss of only volatile fault state cannot make the preserved terminal receipt collectable',
      completion_meta:get{'clock:policy:1'}.value.fault == nil and successor.fault == nil
      and successor.retained_until(terminal) > successor.now() and successor.admit(1100) == nil)
print(string.format('%d passed, 0 failed', passed))
os.exit(0)
