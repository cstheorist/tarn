-- Offline controls: the first attempted mutation may commit before its
-- unknown reply. That history must never acquire a second increment.
local retry = dofile(arg[1])
local function run(fn)
    local calls, effects, observed = 0, 0, {}
    local ok, result = pcall(retry, 'owned-test-operation', function()
        calls = calls + 1
        return fn(calls, function() effects = effects + 1 end)
    end, function(e) observed[#observed + 1] = e end)
    return ok, result, calls, effects, observed
end
local ok, r, calls, effects, events = run(function(n, commit)
    if n == 1 then return {'conflict', 1213, 'known rollback'} end
    commit(); return {'ok', 1}
end)
assert(ok and r[1] == 'ok' and calls == 2 and effects == 1 and events[1].known_abort)
for _, reply in ipairs({{'error', 1180, 'unknown'}, {'conflict', 1213, 'prefix', 'extra'},
                         {'conflict', '1213', 'wrong type'}, {'conflict', 1213, false},
                         {'conflict', 1213, 'hidden field', extra = 1}}) do
    ok, r, calls, effects = run(function(_, commit) commit(); return reply end)
    assert(ok and r == reply and calls == 1 and effects == 1)
end
ok, r, calls, effects = run(function(_, commit) commit(); error('lost after commit') end)
assert(not ok and calls == 1 and effects == 1)
ok, r, calls, effects = run(function(_, commit) commit(); return {'conflict', 1213, 'extra return'}, nil end)
assert(ok and calls == 1 and effects == 1)
ok, r, calls, effects = run(function() return {'conflict', 1213, 'persistent abort'} end)
assert(not ok and calls == 8 and effects == 0)
print('fixture retry controls passed: known abort bounded, uncertain effects never repeated')
