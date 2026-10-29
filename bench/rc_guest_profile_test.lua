-- Worst-case sample cardinality and watchdog controls; no storage instance.
local fio, fiber = require('fio'), require('fiber')
local path = fio.dirname(arg[0]) .. '/rc_guest_profile.lua'
local callback, stops, stack = nil, 0, ''
package.loaded['jit.profile'] = {
    start = function(_, cb) callback = cb end,
    stop = function() stops = stops + 1 end,
    dumpstack = function(_, _, depth) assert(depth == 8); return stack end,
}
local p = dofile(path)
p.start(1)
for i = 1, 1000 do
    stack = tostring(i) .. string.rep('x', 4000)
    callback(nil, 3, 'N')
end
local r = p.stop()
assert(r.distinct == 256 and r.samples == 3000 and r.overflow_samples == 744 * 3)
local total = r.overflow_samples
for k, v in pairs(r.stacks) do assert(#k <= 2050); total = total + v end
assert(total == r.samples and stops == 1)
p.stop(); assert(stops == 1)
local watched = dofile(path)
watched.start(1)
fiber.sleep(3.1)
assert(watched.stop().auto_stopped and stops == 2)
print('bounded stacks, overflow accounting, idempotent stop and watchdog passed')
os.exit(0)
