-- Test-only, bounded guest-stack profiler. No files or host-stack unwinding.
-- Loaded by the isolated benchmark through its administrative connection.
local profile, fiber = require('jit.profile'), require('fiber')
local limit, depth, bytes = 256, 8, 2048
local state = {samples = 0, states = {}, stacks = {}, distinct = 0,
               overflow_samples = 0, max_stacks = limit, max_depth = depth,
               max_stack_bytes = bytes, interval_ms = 10, auto_stopped = false}
local running = false
local function stop()
    if running then profile.stop(); running = false end
    return state
end
local function sample(thread, samples, vmstate)
    state.samples = state.samples + samples
    state.states[vmstate] = (state.states[vmstate] or 0) + samples
    local stack = vmstate .. ':' .. profile.dumpstack(thread, 'pl;', depth):sub(1, bytes)
    local previous = state.stacks[stack]
    if previous ~= nil then
        state.stacks[stack] = previous + samples
    elseif state.distinct < limit then
        state.distinct = state.distinct + 1
        state.stacks[stack] = samples
    else
        state.overflow_samples = state.overflow_samples + samples
    end
end
return {
    start = function(seconds)
        assert(not running and seconds >= 1 and seconds <= 30)
        profile.start('fi10', sample)
        running = true
        fiber.create(function()
            fiber.sleep(seconds + 2)
            if running then state.auto_stopped = true; stop() end
        end)
        return {max_stacks = limit, max_depth = depth, max_stack_bytes = bytes,
                interval_ms = 10, deadline_seconds = seconds + 2}
    end,
    stop = stop,
}
