-- Focused nonyielding final-plan guard checks, using actual physical spaces.
-- Run: tarantool lua/test/rc_plan_guard_test.lua /tmp/owned-empty-directory
-- Optional negative controls: RC_PLAN_GUARD_MUTANT=scalar|skip-last|stale-mode.
local ffi, fio, fiber = require('ffi'), require('fio'), require('fiber')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, txn_isolation = 'read-confirmed',
        wal_mode = 'fsync', log_level = 3}
local tarn = require('tarn')
tarn.init{shard_id = 1, native = false}
local F, checks = tarn.fresh, 0
local function check(yes, label)
    assert(yes, label); checks = checks + 1; print('ok ' .. label)
end
-- Explicit private-helper inspection: execute the real final guard without
-- entering a DML/barrier path that would add unrelated waits and checks.
local function upvalue(fn, wanted)
    for i = 1, 64 do
        local name, value = debug.getupvalue(fn, i)
        if name == nil then break end
        if name == wanted then return value end
    end
    error('missing private test binding: ' .. wanted)
end
local BK = upvalue(tarn_autocommit, 'BK')
local c = upvalue(F.clock_check, 'c')
local fresh_plans = assert(BK.fresh_plans)
local mode, controls, writable = F.mode, F.controls, c.writable
local function healthy() F.set_clock{read_check = function() return nil end} end
local function guard(plans)
    local txn, active, switches = ffi.C.box_txn_id(), box.is_in_txn(), fiber.self():csw()
    local ok, err = pcall(fresh_plans, plans)
    check(fiber.self():csw() == switches, 'final plan guard does not switch fibers')
    check(box.is_in_txn() == active and ffi.C.box_txn_id() == txn,
          'final plan guard preserves exact ambient transaction ownership')
    return ok, err
end
local function accepts(plans, label) check(guard(plans), label) end
local function refuses(plans, reason, label)
    local ok, err = guard(plans)
    check(not ok and err.code == box.error.TRANSACTION_CONFLICT
          and tostring(err) == 'strong read unavailable: ' .. reason, label)
end
local sync = box.schema.space.create('test.plan_sync', {is_sync = true})
sync:create_index('pk')
local async = box.schema.space.create('test.plan_async')
async:create_index('pk')
local plans = {{sync}, {sync}, {sync}}
healthy()

-- Deliberate test-only helper substitutions. Each must make this same suite
-- fail; normal execution always uses the real production implementation.
local mutant = os.getenv('RC_PLAN_GUARD_MUTANT')
if mutant == 'scalar' then
    BK.fresh_plans = function(ps)
        F.clock_check(); F.controls(); c.writable()
        for _, p in ipairs(ps) do F.space(p[1]) end
    end
    fresh_plans = BK.fresh_plans
elseif mutant == 'skip-last' then
    local actual = F.plans
    F.plans = function(ps, pos)
        local shortened = {}
        for i = 1, #ps - 1 do shortened[i] = ps[i] end
        return actual(shortened, pos)
    end
elseif mutant == 'stale-mode' then
    local space_mode, retained = upvalue(F.plans, 'space_mode'), nil
    F.plans = function(ps, pos)
        local current = F.controls()
        c.writable()
        if retained == nil then retained = current end
        for _, p in ipairs(ps) do space_mode(p[pos], retained) end
    end
elseif mutant ~= nil then error('unknown RC_PLAN_GUARD_MUTANT: ' .. mutant) end

-- A mechanism assertion distinguishes the optimization from the old scalar
-- loop: exactly one fresh mode and control scan, even with many/zero plans.
local mode_calls, control_calls = 0, 0
F.mode = function() mode_calls = mode_calls + 1; return mode() end
F.controls = function() control_calls = control_calls + 1; return controls() end
for _, ps in ipairs({{}, plans}) do
    mode_calls, control_calls = 0, 0
    accepts(ps, 'empty and nonempty plans accepted in standalone mode')
    check(mode_calls == 1 and control_calls == 1, 'one current mode and controls scan per final check')
end
F.mode, F.controls = mode, controls

-- Real topology changes, with no fabricated space properties or box.info.
-- A registered second replica makes an asynchronous configuration ineligible;
-- deleting it restores standalone eligibility on the next invocation.
box.space._cluster:insert{2, tostring(require('uuid').new()), 'plan_guard_peer'}
check(F.mode() == 3, 'runtime membership makes asynchronous configuration ineligible')
refuses({}, 'replicated asynchronous spaces or unsupported quorum configuration',
        'even empty plans recheck runtime eligibility')
box.space._cluster:delete{2}
accepts(plans, 'next invocation observes restored standalone membership')

-- No live RF-many topology is started. Only the mode decision is synthetic;
-- F.controls, F.plans, F.space and all space properties remain real.
-- First prove controls win over user-space errors, including an empty batch.
F.mode = function() return 2 end
for _, ps in ipairs({{}, {{async}}}) do
    refuses(ps, 'control space _tarn_meta is not synchronous',
            'asynchronous control space takes precedence over planned spaces')
end
for id, sp in pairs(box.space) do
    if type(id) == 'number' and sp.name:sub(1, 6) == '_tarn_' and sp.name ~= '_tarn_scratch' then
        sp:alter{is_sync = true}
    end
end
accepts(plans, 'actual synchronous controls and user spaces accepted')
refuses({{async}, {sync}}, 'space test.plan_async is not synchronous', 'first asynchronous plan rejected')
refuses({{sync}, {async}}, 'space test.plan_async is not synchronous', 'last asynchronous plan rejected')

-- Local mode reuse must stop at the invocation boundary. This also rejects
-- a mode retained when the earlier invocation allowed an asynchronous space.
local selected = 1
F.mode = function() return selected end
accepts({{async}}, 'standalone mode allows asynchronous physical space')
selected = 2
refuses({{async}}, 'space test.plan_async is not synchronous', 'mode is refreshed for the next invocation')
selected = 1
check(pcall(F.space, async), 'independent F.space observes standalone mode')
selected = 2
local ok, err = pcall(F.space, async)
check(not ok and tostring(err) == 'strong read unavailable: space test.plan_async is not synchronous',
      'independent F.space freshly checks mode each time')
selected = 3
refuses({}, 'replicated asynchronous spaces or unsupported quorum configuration',
        'configuration ineligibility is refreshed even for empty plans')
F.mode = mode

-- Real configuration eligibility between invocations. No replicated writes
-- or barriers run with this synthetic registered peer/quorum configuration.
box.space._cluster:insert{2, tostring(require('uuid').new()), 'plan_guard_peer'}
local configured_sync = tarn.cfg.sync
local configured_quorum = box.cfg.replication_synchro_quorum
local configured_read_quorum = box.cfg.replication_linearizable_quorum
box.cfg{replication_synchro_quorum = 2, replication_linearizable_quorum = 'N - Q + 1'}
tarn.cfg.sync = true
check(F.mode() == 2, 'real public quorum and sync settings are eligible')
accepts(plans, 'guard accepts current eligible public configuration')
tarn.cfg.sync = false
refuses(plans, 'replicated asynchronous spaces or unsupported quorum configuration',
        'changed asynchronous configuration is rejected')
tarn.cfg.sync = true
accepts(plans, 'guard observes eligibility restored on next invocation')
tarn.cfg.sync = configured_sync
box.cfg{replication_synchro_quorum = configured_quorum,
        replication_linearizable_quorum = configured_read_quorum}
box.space._cluster:delete{2}
accepts({}, 'configuration and membership restored between invocations')

-- Trace the exact existing clock -> controls -> writable -> properties order.
-- The writable wrapper delegates to the real guard; no injected callback yields.
local events = {}
c.writable = function() events[#events + 1] = 'writable'; return writable() end
F.controls = function() events[#events + 1] = 'controls'; return controls() end
F.set_clock{read_check = function() events[#events + 1] = 'clock'; return nil end}
accepts(plans, 'ordered guard succeeds')
check(table.concat(events, ',') == 'writable,clock,controls,writable',
      'clock check precedes controls and final writable check')
F.set_clock(nil)
refuses({{async}}, 'clock health is unknown', 'unknown clock wins before configuration and properties')
F.mode = function() return 3 end
refuses({{async}}, 'clock health is unknown', 'unknown clock wins over unsupported controls')
healthy()
refuses({{async}}, 'replicated asynchronous spaces or unsupported quorum configuration',
        'unsupported controls win over asynchronous planned space')
F.mode = function() return 2 end
-- A deterministic writable failure at the second check competes with a bad
-- planned space. This explicit test-only writable probe never yields.
local writes = 0
c.writable = function()
    writes = writes + 1
    if writes == 2 then box.error(box.error.READONLY) end
    return writable()
end
local writable_ok, writable_err = guard({{async}})
check(not writable_ok and writable_err.code == box.error.READONLY and writes == 2,
      'final writable rejection takes precedence over planned-space properties')
c.writable = writable

-- Exercise both success and rejection while an existing, unmarked transaction
-- owns a real read: these guards must never refresh/replace its read view.
box.begin{txn_isolation = 'read-confirmed'}
sync:get{1}
accepts(plans, 'guard accepts synchronous plans in existing transaction')
refuses({{sync}, {async}}, 'space test.plan_async is not synchronous',
        'guard rejection retains existing transaction and prior reads')
box.rollback()
F.mode, F.controls, c.writable = mode, controls, writable
F.set_clock(nil)
box.cfg{read_only = true}
local demoted_ok, demoted_err = guard({{async}})
check(not demoted_ok and demoted_err.code == box.error.READONLY,
      'known demotion wins over unknown clock and bad planned properties')
box.cfg{read_only = false}
healthy()
print(string.format('RC plan guard: %d checks passed', checks))
os.exit(0)
