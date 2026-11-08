-- The phases of a READ COMMITTED transaction on a shard (issue #186): an
-- opt-in profiler of the row-lock and commit procedures (tarn_lock,
-- tarn_commit_rc). With tarn.cfg.rc_phases (lua/storage.lua:
-- TARN_RC_PHASES), each phase that a probe brackets adds its wall time and
-- the bytes Lua allocated meanwhile to its counters; off, a probe returns
-- at once. A probe neither yields nor holds anything: it reads the
-- monotonic clock and LuaJIT's count of allocated bytes (misc.getmetrics),
-- and leaves out what the probes themselves allocate (misc.getmetrics's
-- tables, calibrated at each reset). A phase's time includes its waits.
-- The totals (lock_total, commit_total) are the whole procedure's, the
-- phases within included.
local clock = require('clock')
local misc = require('misc')

local P = {on = false}

-- The phases, in the order of a transaction.
P.NAMES = {
    'lock_begin',        -- tarn_lock's checks before its first lock (pl_begin)
    'lock_barrier',      -- ... of them, the freshness barrier (M.fresh.barrier)
    'lock_acquire',      -- a row lock's bookkeeping (pl_acquire), its wait aside
    'lock_wait',         -- waiting for a row lock another transaction holds
    'lock_commit_wait',  -- waiting for commits in flight, or prepared writes, on the row
    'lock_read',         -- the row read under its lock
    'lock_total',        -- the whole of tarn_lock
    'commit_fresh',      -- tarn_commit_rc's freshness, fence and bucket checks
    'commit_plan',       -- its write plans: storage reads, guards, captures
    'commit_checks',     -- its final checks (atomic CommitRC)
    'commit_wal',        -- box.commit: the WAL write, its fsync and the quorum
    'commit_release',    -- releasing the transaction's row locks (on_commit)
    'commit_lock_wait',  -- waiting for a prepared transaction's lock on a write
    'commit_total',      -- the whole of tarn_commit_rc
}

local T, A, N = {}, {}, {}
local probe, stops = 0, 0

local function allocated() return misc.getmetrics().gc_allocated end

function P.reset()
    for _, k in ipairs(P.NAMES) do T[k], A[k], N[k] = 0, 0, 0 end
    -- (what one probe allocates: the table of a misc.getmetrics call)
    local a0 = allocated()
    local a1 = allocated()
    probe = a1 - a0
end

function P.enable(on)
    P.on = on == true
    P.reset()
end

-- A phase starts: its clock, its allocation and the stops so far, or nil
-- when the profiler is off.
function P.start()
    if not P.on then return nil end
    return clock.monotonic(), allocated(), stops
end

-- Phase `name`, started at (t0, a0, s0), stops. The bytes of the probes of
-- the phases within it are left out.
function P.stop(name, t0, a0, s0)
    if t0 == nil then return end
    local a1 = allocated()
    stops = stops + 1
    T[name] = T[name] + (clock.monotonic() - t0)
    -- (each probe within: a start and a stop, one table each; and this stop's)
    A[name] = A[name] + (a1 - a0) - probe * (1 + 2 * (stops - 1 - s0))
    N[name] = N[name] + 1
end

-- {name = {calls, seconds, bytes}}, then the counters start again if `reset`.
function P.report(reset)
    local r = {}
    for _, k in ipairs(P.NAMES) do r[k] = {N[k], T[k], A[k]} end
    r.probe_bytes = probe
    if reset then P.reset() end
    return r
end

P.reset()
return P
