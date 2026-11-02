-- Helper-only #186 census on an owned RF3/fsync fixture. No schema/data writes.
-- Usage: tarantool bench/rc_guard_probe.lua <leader-address> <control|candidate>
-- Each lane emits five rotated repeats, not five alternating cross-lane pairs.
-- The runner owns lane pairing, source hashes and the end-to-end comparison.
local net_box, json = require('net.box'), require('json')
local address, variant = assert(arg[1], 'leader address required'), assert(arg[2], 'variant required')
assert(variant == 'control' or variant == 'candidate', 'unknown guard variant')
local connection = net_box.connect('tarn:tarn@' .. address, {wait_connected = 5})
assert(connection:is_connected(), 'owned fixture is unavailable')
local ok, result = pcall(connection.eval, connection, [=[
local variant = ...
local fiber, clock, misc = require('fiber'), require('clock'), require('misc')
local tarn, module = require('tarn'), require('tarn_fresh')
local result = {
    schema = 1, complete = false, variant = variant, samples = {}, census = {},
    iterations = 200, warmup_iterations = 20, yield_every = 10, repeats = 5,
    scope = 'RF3/fsync helper-only current-mode and space-property inspection',
    limitations = 'Existing synchronous control spaces stand in for plan spaces. '
        .. 'No clock check, barrier, lock, write, commit or transaction throughput is measured. '
        .. 'Allocation intervals include background work at cooperative yields. '
        .. 'Per-lane repeats are not alternating cross-lane pairs.',
}
local run_ok, failure = pcall(function()
    result.runtime = {
        version = box.info.version, pid = box.info.pid, wal = box.cfg.wal_mode,
        quorum = box.info.synchro.quorum, term = box.info.election.term,
        uuid = tostring(box.info.uuid), native = tarn_info().native,
        source = debug.getinfo(module.new, 'S').source,
        test_flags = {check_locks = tarn.cfg.check_locks, check_yields = tarn.cfg.check_yields},
    }
    assert(result.runtime.wal == 'fsync', 'helper probe requires fsync')
    assert(result.runtime.test_flags.check_locks == false
           and result.runtime.test_flags.check_yields == false, 'debug checks are enabled')
    local replicas = 0
    for id in pairs(box.info.replication) do
        if type(id) == 'number' and id > 0 then replicas = replicas + 1 end
    end
    result.runtime.replicas = replicas
    assert(replicas == 3, 'helper probe requires exactly three registered replicas')
    local function writable() assert(not box.info.ro, 'probe leader is read-only') end
    -- This private helper instance does not replace any live Tarn guard.
    local fresh = module.new{cfg = {sync = true}, writable = writable}
    assert(fresh.mode() == 2 and fresh.controls() == 2, 'unsupported fresh-read mode')
    assert((variant == 'candidate') == (type(fresh.plans) == 'function'),
           'requested variant differs from loaded source')
    local function scalar(plans)
        fresh.controls(); writable()
        for _, plan in ipairs(plans) do fresh.space(plan[1]) end
    end
    local check = variant == 'candidate'
        and function(plans) fresh.plans(plans, 1) end or scalar
    local standins = {assert(box.space._tarn_meta), assert(box.space._tarn_catalog)}
    result.plan_spaces = {standins[1].name, standins[2].name}
    for _, space in ipairs(standins) do
        assert(space.is_sync and space.state.is_sync, 'asynchronous plan stand-in')
    end
    local widths, plans = {0, 1, 2, 8, 32}, {}
    for _, width in ipairs(widths) do
        plans[width] = {}
        for i = 1, width do plans[width][i] = {standins[(i - 1) % #standins + 1]} end
    end

    -- Diagnostic mode-call counts and context-switch checks are separate
    -- from timing. Restore the private wrapper even if a census check fails.
    local original_mode, calls = fresh.mode, 0
    fresh.mode = function() calls = calls + 1; return original_mode() end
    local counted, count_error = pcall(function()
        for _, width in ipairs(widths) do
            calls = 0
            local before = fiber.self():csw()
            check(plans[width])
            local switches = fiber.self():csw() - before
            local expected = variant == 'candidate' and 1 or width + 1
            assert(calls == expected, 'unexpected final-guard mode-call count')
            assert(switches == 0, 'final guard yielded')
            result.census[#result.census + 1] = {
                width = width, mode_calls = calls, expected_mode_calls = expected,
                context_switches = switches,
            }
        end
        calls = 0
        fresh.space(standins[1]); fresh.space(standins[2])
        assert(calls == 2, 'direct space calls no longer obtain their own mode')
        result.direct_space_mode_calls = calls
    end)
    fresh.mode = original_mode
    result.census_mode_restored = fresh.mode == original_mode
    if not counted then error(count_error) end
    assert(result.census_mode_restored)

    local deadline = clock.monotonic() + 60
    local function yield_bounded()
        assert(clock.monotonic() < deadline, 'helper probe exceeded its deadline')
        fiber.yield()
    end
    for repeat_id = 0, result.repeats - 1 do
        for offset = 0, #widths - 1 do
            local width = widths[(repeat_id + offset) % #widths + 1]
            local plan = plans[width]
            for i = 1, result.warmup_iterations do
                check(plan)
                if i % result.yield_every == 0 then yield_bounded() end
            end
            collectgarbage('collect')
            yield_bounded()
            local before = misc.getmetrics()
            local started = clock.monotonic()
            for i = 1, result.iterations do
                check(plan)
                if i % result.yield_every == 0 then yield_bounded() end
            end
            local elapsed = clock.monotonic() - started
            local after = misc.getmetrics()
            result.samples[#result.samples + 1] = {
                repeat_id = repeat_id, width = width, elapsed_seconds = elapsed,
                lua_allocated = after.gc_allocated - before.gc_allocated,
                lua_freed = after.gc_freed - before.gc_freed,
            }
        end
    end
    assert(fresh.mode == original_mode, 'timing changed the mode function')
    assert(fresh.controls() == 2 and not box.info.ro, 'fixture lost read eligibility')
    assert(box.info.election.term == result.runtime.term, 'leader term changed during probe')
    assert(box.info.pid == result.runtime.pid, 'storage process changed during probe')
    assert(tarn.cfg.check_locks == false and tarn.cfg.check_yields == false,
           'debug flags changed during probe')
    result.complete = true
end)
if not run_ok then result.failure = tostring(failure):sub(1, 4096) end
return result
]=], {variant}, {timeout = 90})
connection:close()
if not ok then result = {schema = 1, complete = false, variant = variant, failure = tostring(result):sub(1, 4096)} end
print(json.encode(result))
os.exit(result.complete and 0 or 1)
