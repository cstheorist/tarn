local nb, json = require('net.box'), require('json')
local c = nb.connect('tarn:tarn@127.0.0.1:3951', {wait_connected = 5})
assert(c:is_connected())
local result = c:eval([[
local fiber, clock, misc = require('fiber'), require('clock'), require('misc')
local original = dofile('/tmp/tarn186-guard-source/lua/tarn_fresh.lua')
local prototype = require('tarn_fresh')
local cfg = {sync = true}
local writable = function() assert(not box.info.ro) end
local scalar = original.new{cfg = cfg, writable = writable}
local batched = prototype.new{cfg = cfg, writable = writable}
local scalar_plans = function(plans)
    scalar.controls(); writable()
    for _, p in ipairs(plans) do scalar.space(p[1]) end
end
local checks = {scalar = scalar_plans, batched = batched.plans}
local sp = box.schema.space.create('guard_batch_probe', {is_sync = true})
sp:create_index('pk')
local async = box.schema.space.create('guard_batch_async', {is_sync = false})
async:create_index('pk')
local refusals = {}
for name, check in pairs(checks) do
    check({{sp}})
    cfg.sync = false
    assert(not pcall(check, {{sp}}), 'mode was reused across invocations')
    cfg.sync = true
    for _, position in ipairs({1, 32}) do
        local plans = {}; for i = 1, 32 do plans[i] = {i == position and async or sp} end
        assert(not pcall(check, plans), 'asynchronous planned space admitted')
    end
    check({{sp}})
    refusals[#refusals + 1] = {path = name, changed_mode_refused = true, first_and_last_async_refused = true}
end
local result = {scope = 'RF3/fsync helper-only comparison; no transaction throughput claim',
    version = box.info.version, wal = box.cfg.wal_mode, mode = scalar.mode(),
    quorum = box.info.synchro.quorum, iterations = 1000, yield_every = 10,
    native = tarn_info().native, refusals = refusals, samples = {}}
for _, width in ipairs({1, 2, 8, 32}) do
    local plans = {}; for i = 1, width do plans[i] = {sp} end
    for pair = 0, 4 do
        local order = pair % 2 == 0 and {'scalar', 'batched'} or {'batched', 'scalar'}
        for _, name in ipairs(order) do
            local check = checks[name]
            for i = 1, 100 do check(plans); if i % 10 == 0 then fiber.yield() end end
            collectgarbage('collect')
            local before, started = misc.getmetrics(), clock.monotonic()
            for i = 1, result.iterations do
                check(plans)
                if i % result.yield_every == 0 then fiber.yield() end
            end
            local elapsed, after = clock.monotonic() - started, misc.getmetrics()
            result.samples[#result.samples + 1] = {width = width, pair = pair, path = name,
                elapsed_seconds = elapsed, lua_allocated = after.gc_allocated - before.gc_allocated,
                lua_freed = after.gc_freed - before.gc_freed}
        end
    end
end
return result
]])
c:close(); print(json.encode(result)); os.exit(0)
