-- The election watchdog's decision (lua/tarn.lua "Election watchdog", #254):
-- when a replica stands, from synthetic election states and times.
-- tests/election.sh runs it on a replicated shard.
local fio = require('fio')
local here = fio.abspath(debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './') .. '/'
package.path = here .. '../?.lua;' .. package.path
fio.mktree(assert(arg[1]))
box.cfg{work_dir = arg[1], memtx_use_mvcc_engine = true, log_level = 3}
local fiber = require('fiber')
local tarn = require('tarn')
tarn.init{shard_id = 1}
local EW = tarn.election_watchdog
assert(EW.fiber ~= nil, 'the watchdog runs on every storage node')
EW.fiber:cancel()   -- (the checks below call it themselves)

local fails = 0
local function check(name, cond)
    if cond then print('  ok   ' .. name) else fails = fails + 1; print('  FAIL ' .. name) end
end
local function state(o)
    local s = {leader = 0, leading = false, candidate = true, running = true, quorum = true}
    for k, v in pairs(o or {}) do s[k] = v end
    return s
end
-- A fresh watchdog with a period of p seconds and an extra wait of a quarter
-- of it.
local function fresh(p)
    tarn.cfg.election_watchdog = p
    EW.jitter = function(q) return q / 4 end
    EW.seen, EW.standing = nil, false
end
local function decide(t, o) return EW.decide(t, state(o)) end

fresh(4)
check('the first look only starts the period', not decide(100) and EW.seen == 100 and EW.extra == 1)
check('no stand before the period and its extra', not decide(104.9))
check('a stand once they have passed', decide(105))
check('... which starts another period', EW.seen == 105 and not decide(109.9) and decide(110))

fresh(4); decide(100)
check('a leader restarts the period', not decide(104, {leader = 2}) and EW.seen == 104 and not decide(108.9))
check('so does leading', not decide(109, {leading = true}) and EW.seen == 109)
check('so do voter and manual modes', not decide(120, {candidate = false}) and EW.seen == 120)
check('so does a status other than running', not decide(130, {running = false}) and EW.seen == 130)
EW.standing = true
check('so does a stand that has not ended', not decide(140) and EW.seen == 140)
EW.standing = false
check('... after which a whole period passes again', not decide(144.9) and decide(145))

fresh(4); decide(100)
check('no stand without a quorum', not decide(110, {quorum = false}) and EW.seen == 100)
check('... and one as soon as the quorum is back', decide(110.5))

fresh(0); decide(100)
check('a period of 0 never stands', not decide(1e6))

fresh(nil)
check('the default period is three election timeouts', EW.period() == 3 * box.cfg.election_timeout)

-- A stand: counted, and box.ctl.promote() called in a fiber of its own.
-- No other stand starts until it returns, won or lost.
fresh(4)
local decide, promote = EW.decide, EW.promote
local calls, release = 0, fiber.channel()
EW.promote = function() calls = calls + 1; release:get(); error('lost') end
EW.decide = function() return true end
local before = tarn_info().stats.election_stands
EW.tick(1000)
fiber.yield()
check('a stand is counted', tarn_info().stats.election_stands == before + 1)
check('... and calls promote in a fiber of its own', calls == 1 and EW.standing)
EW.decide = decide
check('... no other stand starts while it runs', not EW.decide(2000, state()) and EW.seen == 2000)
release:put(true)
fiber.yield(); fiber.yield()
check('... which ends when promote returns', not EW.standing and calls == 1)
EW.promote = promote

tarn.cfg.election_watchdog = nil
print(fails == 0 and 'election watchdog decisions: all checks passed'
      or string.format('election watchdog decisions: %d FAILED', fails))
os.exit(fails == 0 and 0 or 1)
