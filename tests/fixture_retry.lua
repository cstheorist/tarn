-- Test-fixture retry at ONE atomic procedure boundary. Only a complete
-- known-abort acknowledgement authorizes another attempt. Earlier stages,
-- transport exceptions, unknown outcomes and malformed replies are never
-- retried. Keep each observation in the owned node's retained log.
local clock, fiber, json = require('clock'), require('fiber'), require('json')
local function pack(...) return {n = select('#', ...), ...} end
local function known_abort(replies)
    if replies.n ~= 1 then return false end
    local r = replies[1]
    if type(r) ~= 'table' or r[1] ~= 'conflict' or r[2] ~= 1213 or type(r[3]) ~= 'string' then return false end
    local n = 0
    for key in pairs(r) do
        if key ~= 1 and key ~= 2 and key ~= 3 then return false end
        n = n + 1
    end
    return n == 3
end
return function(label, invoke, observe)
    local deadline = clock.monotonic() + 20
    for attempt = 1, 8 do
        local replies = pack(invoke()) -- Exceptions propagate; no resubmission.
        local abort = known_abort(replies)
        local event = {procedure = label, attempt = attempt, known_abort = abort,
                       return_count = replies.n, reply = replies[1]}
        if observe then observe(event) else io.stderr:write(json.encode(event) .. '\n') end
        if not abort then return unpack(replies, 1, replies.n) end
        if attempt == 8 or clock.monotonic() >= deadline then
            error('known-abort retry budget exhausted for ' .. label)
        end
        fiber.sleep(.02)
    end
end
