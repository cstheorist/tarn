-- The election watchdog (#254) on one shard of three replicas, run by
-- tests/election.sh: DIR and the URIs of the three replicas. With a period
-- of 1 s on every replica:
--
--   * while the shard has a leader, no replica stands;
--   * when both followers stop, the leader resigns (strict fencing), and it
--     does not stand without its quorum, however long it has no leader;
--   * once they are back, the shard has a leader again within a bound.
--
-- lua/test/election_watchdog_test.lua covers when the watchdog stands.
local net = require('net.box')
local fiber = require('fiber')
local json = require('json')

local dir = arg[1]
local uris = {arg[2], arg[3], arg[4]}
local c = {}
for i, uri in ipairs(uris) do
    c[i] = net.connect(uri, {user = 'tarn', password = 'tarn', wait_connected = 30})
    assert(c[i]:is_connected(), 'cannot connect to ' .. uri)
end

local fails = 0
local function check(name, cond, extra)
    if cond then print('  ok   ' .. name)
    else fails = fails + 1; print('  FAIL ' .. name .. (extra and ('  -- ' .. json.encode(extra)) or '')) end
end
local function eval(i, script, ...) return c[i]:eval(script, {...}, {timeout = 10}) end
local function states()
    local out = {}
    for i = 1, 3 do
        local ok, r = pcall(eval, i, [[local e = box.info.election
            return {state = e.state, term = e.term, leader = e.leader,
                    stands = tarn_info().stats.election_stands}]])
        out[i] = ok and r or {error = tostring(r)}
    end
    return out
end
local function leader()
    for i, s in ipairs(states()) do
        if s.state == 'leader' then return i end
    end
end
local function wait_leader(secs)
    local deadline = fiber.clock() + secs
    repeat
        local l = leader()
        if l then return l end
        fiber.sleep(0.1)
    until fiber.clock() > deadline
end
local function stands()
    local n = 0
    for i = 1, 3 do n = n + eval(i, 'return tarn_info().stats.election_stands') end
    return n
end
local function pid(i)
    local f = assert(io.open(string.format('%s/s1.%d.pid', dir, i)))
    local p = f:read('*l'); f:close()
    return assert(tonumber(p))
end

local x = assert(wait_leader(60), 'no first leader: ' .. json.encode(states()))
for i = 1, 3 do eval(i, 'tarn.cfg.election_watchdog = 1') end
local stood = stands()
fiber.sleep(4)
check('while the shard has a leader, no replica stands', stands() == stood and leader() == x, states())

local others = {}
for i = 1, 3 do if i ~= x then others[#others + 1] = i end end
for _, i in ipairs(others) do os.execute('kill -STOP ' .. pid(i)) end
local deadline = fiber.clock() + 15
while fiber.clock() < deadline and eval(x, 'return box.info.election.state') == 'leader' do fiber.sleep(0.1) end
check('without its quorum the leader resigns', eval(x, 'return box.info.election.state') ~= 'leader')
stood = eval(x, 'return tarn_info().stats.election_stands')
fiber.sleep(5)
check('without a quorum it does not stand', eval(x, 'return tarn_info().stats.election_stands') == stood,
      {stands = eval(x, 'return tarn_info().stats.election_stands')})
for _, i in ipairs(others) do os.execute('kill -CONT ' .. pid(i)) end
check('with its replicas back the shard has a leader within 10 s', wait_leader(10) ~= nil, states())

for i = 1, 3 do eval(i, 'tarn.cfg.election_watchdog = nil') end
print(fails == 0 and 'election watchdog: all checks passed' or string.format('election watchdog: %d FAILED', fails))
os.exit(fails == 0 and 0 or 1)
