-- The lock calls' barrier without the synchronous queue's wait (issue #314,
-- F.owner_barrier in lua/tarn_fresh.lua) on a private RF3 shard started by
-- owner_barrier.sh with TARN_LOCK_BARRIER=owner: a lock call waits for a
-- round of replies from the other replicas; concurrent barriers share
-- rounds; one other replica is a quorum of two; no barrier passes without
-- one, on a reply from another replica set or with a malformed vclock, or
-- on one ahead of this instance's WAL; a replica without tarn_peer_vclock
-- sends the call to the full barrier.
local net, fiber, ffi, json = require('net.box'), require('fiber'), require('ffi'), require('json')
io.stdout:setvbuf('no')
local work, base = assert(os.getenv('OWNER_BARRIER_WORK')), tonumber(assert(os.getenv('OWNER_BARRIER_BASE')))
local conns, passed = {}, 0
for i = 1, 3 do
    conns[i] = net.connect('127.0.0.1:' .. (base + i), {user = 'tarn', password = 'tarn',
        wait_connected = false, connect_timeout = .3, reconnect_after = .05})
end
local function check(label, yes, value)
    assert(yes, label .. ': ' .. json.encode(value or {}))
    passed = passed + 1
    print('  ok   ' .. label)
end
local function ev(i, code, args) return conns[i]:eval(code, args or {}, {timeout = 15}) end
local function await(label, fn, seconds)
    local deadline, last = fiber.clock() + (seconds or 60)
    repeat
        local ok, value = pcall(fn)
        if ok and value then return value end
        last = value
        fiber.sleep(.05)
    until fiber.clock() >= deadline
    error(label .. ' timed out: ' .. tostring(last))
end
local function state(i)
    if not conns[i]:is_connected() then return false end
    return ev(i, [[if not rawget(_G, 'tarn') then return false end
        local t = require('tarn')
        return {ro = box.info.ro, queue = box.info.synchro.queue.len, clock = t.clock.status().read_ready,
                barrier = t.cfg.lock_barrier, ready = box.space._tarn_meta ~= nil and box.space._tarn_meta.is_sync}]])
end
local function leader()
    return await('a writable leader with a healthy clock', function()
        local found
        for i = 1, 3 do
            local s = state(i)
            if s and not s.ro and s.queue == 0 and s.clock and s.ready then
                assert(not found, 'two writable leaders')
                found = i
            end
        end
        return found
    end)
end
pcall(ffi.cdef, 'int kill(int, int); int getpid(void);')
-- SIGSTOP (19) or SIGCONT (18) to a fixture process, checked by its kernel
-- parent and start time.
local function signal(i, sig)
    local f = assert(io.open(work .. '/s' .. i .. '.pid')); local pid = tonumber(f:read('*a')); f:close()
    f = assert(io.open(work .. '/s' .. i .. '.process-identity'))
    local recorded = f:read('*a'):match('^%s*(.-)%s*$'); f:close()
    f = assert(io.open('/proc/' .. pid .. '/stat')); local stat = f:read('*a'); f:close()
    local fields = {}
    for value in stat:match('.*%) (.*)$'):gmatch('%S+') do fields[#fields + 1] = value end
    -- (after the command's closing parenthesis: field 4 is the 2nd, 22 the 20th)
    assert(fields[2] .. ':' .. fields[20] == recorded, 'owned process identity changed')
    assert(pid ~= ffi.C.getpid())
    assert(ffi.C.kill(pid, sig) == 0)
end
for i = 1, 3 do await('replica startup', function() return state(i) end) end
local l = leader()
check('the shard runs the owner barrier', state(l).barrier == 'owner')

-- A lock call waits for a round.
local r = ev(l, [[
    local s = box.schema.space.create('ob_t', {is_sync = true, if_not_exists = true})
    s:create_index('pk', {if_not_exists = true})
    s:replace{1, 'one'}
    local F = require('tarn').fresh
    local before = F.ob.rounds
    local res = tarn_lock('ob-1', {{s.id, 0, {1}}}, 'X', {read = true})
    tarn_unlock('ob-1')
    return {status = res[1], row = res[3] and res[3][1], rounds = F.ob.rounds - before}]])
check('a lock call reads its row after a round of replies', r.status == 'ok' and r.row ~= nil and r.row[2] == 'one'
      and r.rounds >= 1, r)

-- Concurrent barriers share rounds.
r = ev(l, [[
    local F, fiber = require('tarn').fresh, require('fiber')
    local before, n, ok, done = F.ob.rounds, 40, 0, fiber.channel(40)
    for _ = 1, n do fiber.new(function() done:put((pcall(F.owner_barrier))) end) end
    for _ = 1, n do if done:get(10) then ok = ok + 1 end end
    return {ok = ok, rounds = F.ob.rounds - before}]])
check('forty concurrent barriers share their rounds', r.ok == 40 and r.rounds >= 1 and r.rounds < 40, r)

local barrier = [[
    local clock = require('clock')
    local t0 = clock.monotonic()
    local ok, err = pcall(require('tarn').fresh.owner_barrier, ...)
    return {ok = ok, err = err ~= nil and tostring(err) or nil, took = clock.monotonic() - t0}]]
local f1, f2 = l % 3 + 1, (l + 1) % 3 + 1

-- One other replica is a quorum of two (N - Q + 1); none is not.
signal(f2, 19)
r = ev(l, barrier, {5})
check('one other replica answers for a read quorum of two', r.ok, r)
signal(f1, 19)
r = ev(l, barrier, {1})
signal(f1, 18); signal(f2, 18)
check('no barrier passes without a reply from another replica', not r.ok, r)

-- Replies that don't count: another replica set's, and one ahead of this
-- instance's WAL, whose catch-up never ends.
l = leader()
f1, f2 = l % 3 + 1, (l + 1) % 3 + 1
ev(f1, [[rawset(_G, 'ob_saved', tarn_peer_vclock)
    tarn_peer_vclock = function() local v = ob_saved(); v.replicaset = 'another'; return v end]])
signal(f2, 19)
r = ev(l, barrier, {1})
signal(f2, 18)
check('a reply from another replica set does not count', not r.ok, r)
ev(f1, [[tarn_peer_vclock = function() local v = ob_saved(); v.vclock[1] = 'one'; return v end]])
signal(f2, 19)
r = ev(l, barrier, {1})
signal(f2, 18)
check('a reply whose vclock has an entry that is not an LSN does not count', not r.ok, r)
local id = ev(l, 'return box.info.id')
ev(f1, [[local id = ...
    tarn_peer_vclock = function() local v = ob_saved(); v.vclock[id] = (v.vclock[id] or 0) + 1000000; return v end]], {id})
signal(f2, 19)
r = ev(l, barrier, {1})
signal(f2, 18)
check('a reply ahead of the local WAL holds the barrier until its deadline', not r.ok and r.took >= .9, r)
ev(f1, 'tarn_peer_vclock = ob_saved')

-- A replica without tarn_peer_vclock (a rolling upgrade): the full barrier.
l = leader()
f1, f2 = l % 3 + 1, (l + 1) % 3 + 1
for _, i in ipairs({f1, f2}) do ev(i, [[rawset(_G, 'ob_saved', tarn_peer_vclock); tarn_peer_vclock = nil]]) end
r = ev(l, [[
    local F = require('tarn').fresh
    local before = F.ob.fallbacks
    local ok, err = pcall(F.owner_barrier, 5)
    return {ok = ok, err = err ~= nil and tostring(err) or nil, fallbacks = F.ob.fallbacks - before}]])
for _, i in ipairs({f1, f2}) do ev(i, 'tarn_peer_vclock = ob_saved') end
check('replicas without tarn_peer_vclock send the call to the full barrier', r.ok and r.fallbacks == 1, r)
r = ev(l, barrier, {5})
check('the owner barrier again once they have it', r.ok, r)

print(('owner barrier RF3 checks passed: %d'):format(passed))
os.exit(0)
