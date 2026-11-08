-- Issue #160: a trace that LuaJIT records while an upvalue is open stores
-- into it, once the upvalue has closed, without the collector's barrier,
-- and the collector then frees the stored table while the upvalue still
-- refers to it. A diagnostic, not a test:
--
--   tarantool tests/evidence/native-gc-crash/upvalue-barrier.lua [open|closed|field] [rounds]
--
-- open:   make() declares `mark` and calls set(), which stores a new table
--         into it, until set() is hot: with JIT off for make(), set() gets
--         a trace of its own while `mark` is open. make() then returns,
--         closing `mark`. This is lua/tarn_engine.lua's constructor and
--         invalidate_tickets before #160's fix.
-- closed: set() first gets hot once make() has returned, so its trace is
--         recorded with `mark` closed.
-- field:  as open, with `mark` a field of a table, as the fix holds it.
--
-- Each round runs a GC cycle in small steps, calling set() once after a
-- given number of steps (every step of the cycle, in turn, over a round),
-- then finishes the cycle. The table set() stored is still the value of
-- `mark`, so it must survive the cycle; a weak-keyed table tells whether
-- the collector freed it. A freed value is only compared, never
-- dereferenced, and is replaced before anything allocates. set() and its
-- siblings hang off the string metatable, a root the collector marks first,
-- so that it marks `mark` early in the cycle.
--
-- Tarantool 3.9.0-0-g8b9a0f5 (LuaJIT 8af934d9) on Linux x86-64, 2026-11-08,
-- one round: open freed the live value after 4,783 of 6,125 stores, closed
-- after 0 of 5,657, field after 0 of 5,787.
local mode = arg[1] or 'open'
local rounds = tonumber(arg[2]) or 1

local function make_upvalue(hot)
    local mark = false
    local function set() mark = {} end
    local function get() return mark end
    local function reset(v) mark = v end
    if hot then for _ = 1, 1000 do set() end end
    return set, get, reset
end
local function make_field(hot)
    local st = {mark = false}
    local function set() st.mark = {} end
    local function get() return st.mark end
    local function reset(v) st.mark = v end
    if hot then for _ = 1, 1000 do set() end end
    return set, get, reset
end
jit.off(make_upvalue)
jit.off(make_field)

-- (functions share hotness counters: started again, at the lowest
-- threshold, set() is traced within two calls; the default afterwards)
local jutil = require('jit.util')
local started, compiled = {}, {}
local function on_trace(what, tr, fn)
    if what == 'start' then started[tr] = jutil.funcinfo(fn).linedefined end
    if what == 'stop' and started[tr] then compiled[started[tr]] = true end
end
jit.attach(on_trace, 'trace')
local set, get, reset
jit.opt.start('hotloop=1')
if mode == 'open' then
    set, get, reset = make_upvalue(true)
elseif mode == 'closed' then
    set, get, reset = make_upvalue(false)
    -- (from an interpreted loop, so that set() is traced on its own)
    local function heat() for _ = 1, 1000 do set() end end
    jit.off(heat)
    heat()
elseif mode == 'field' then
    set, get, reset = make_field(true)
else
    error('mode: open, closed or field')
end
jit.opt.start('hotloop=56')
jit.attach(on_trace)
assert(compiled[jutil.funcinfo(set).linedefined], 'set() was not traced')
rawset(getmetatable(''), 'UPVALUE_BARRIER', {set, get, reset})

local weak = setmetatable({}, {__mode = 'k'})
-- (in their own frames, so that no stack slot of the loop keeps the table)
local function note() weak[get()] = true end
local function survived()
    for k in pairs(weak) do if rawequal(k, get()) then return true end end
    return false
end

local keep = {}
-- One store after k steps of a cycle: true if the value survived, false if
-- the collector freed it, nil if the cycle ended first. (JIT off here, so
-- that set() runs its own trace: a trace of this loop would take set() in,
-- recorded with `mark` closed, barrier and all.)
local function trial(k)
    collectgarbage('collect')
    for x in pairs(weak) do weak[x] = nil end
    for _ = 1, k do
        if collectgarbage('step', 0) then return nil end
    end
    set()
    note()
    repeat until collectgarbage('step', 0)
    local ok = survived()
    reset(keep)
    return ok
end
jit.off(trial)

collectgarbage('setstepmul', 50)
collectgarbage('collect')
local cycle = 0
repeat cycle = cycle + 1 until collectgarbage('step', 0)
local stores, freed = 0, 0
for _ = 1, rounds do
    for k = 0, cycle do
        local ok = trial(k)
        if ok ~= nil then stores = stores + 1 end
        if ok == false then freed = freed + 1 end
    end
end
print(string.format('%s: the collector freed the live value after %d of %d stores (%d steps a cycle)',
    mode, freed, stores, cycle))
os.exit(0)
