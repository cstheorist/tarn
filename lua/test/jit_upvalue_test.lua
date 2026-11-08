-- The storage code stores no collectable value into an open upvalue from
-- a JIT trace (issue #160).
--
-- In Tarantool 3.9's LuaJIT, a trace recorded while a function's local is
-- an open upvalue (its function still running) stores into it with no
-- barrier for the garbage collector (IR UREFO, then USTORE), and the trace
-- goes on storing so once the upvalue has closed. Stored between the
-- collector's marking of the closed upvalue and its atomic phase, a new
-- table stays unmarked and is freed while the upvalue still refers to it.
-- The engine's constructor calls invalidate_tickets before it returns,
-- which stored the tickets' cut into such a local: storage processes
-- crashed in the collector on the memory reused. Upstream LuaJIT's
-- 07b3cd3c guards these traces; this runtime doesn't have it.
--
-- The test traces eagerly (hotloop=1) and inspects each trace compiled:
--   - a control function that stores a table into a local of the function
--     that calls it is found;
--   - the engine's constructor, run many times, traces invalidate_tickets
--     and stores nothing so;
--   - neither does loading the storage module.
local fio = require('fio')
io.stdout:setvbuf('no')
-- (absolute: box.cfg's work_dir changes the directory)
local here = fio.abspath(debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './') .. '/'
package.path = here .. '../?.lua;' .. package.path
local work = fio.abspath(arg[1] or fio.tempdir())
local jutil, vmdef = require('jit.util'), require('jit.vmdef')

local passed = 0
local function check(name, ok, detail)
    assert(ok, name .. (detail ~= nil and ('  -- ' .. tostring(detail)) or ''))
    passed = passed + 1
    print('  ok   ' .. name)
end

-- The collectable IR types (lj_ir.h: str, thr, pro, fun, cdt, tab, udt).
local GC = {[4] = true, [6] = true, [7] = true, [8] = true, [10] = true, [11] = true, [12] = true}
local function irop(ot) local i = 6 * math.floor(ot / 256); return vmdef.irnames:sub(i + 1, i + 6) end
local function fname(fn)
    local fi = jutil.funcinfo(fn)
    return fi.source and (fi.source:gsub('^@', '') .. ':' .. tostring(fi.linedefined)) or '?'
end
local started = {}  -- trace number -> the function its recording started in
local compiled = {} -- function -> true, once a trace starting in it compiled
local stores = {}   -- {function, trace} for each store found
jit.attach(function(what, tr, fn)
    if what == 'start' then started[tr] = type(fn) == 'function' and fname(fn) or '?'; return end
    if what ~= 'stop' then return end
    local start = started[tr] or '?'
    compiled[start] = true
    local info = jutil.traceinfo(tr)
    for ins = 1, info.nins - 1 do
        local _, ot, ref = jutil.traceir(tr, ins)
        if irop(ot) == 'USTORE' and GC[ot % 32] then
            local _, uot = jutil.traceir(tr, ref)
            if uot and irop(uot) == 'UREFO ' then stores[#stores + 1] = {start, tr} end
        end
    end
end, 'trace')
jit.opt.start('hotloop=1')

local function stores_from(pattern)
    local out = {}
    for _, s in ipairs(stores) do
        if s[1]:find(pattern, 1, true) then out[#out + 1] = s[1] .. ' (trace ' .. s[2] .. ')' end
    end
    return out
end

-- The control: set() stores a new table into make()'s local; make() calls
-- it until it is traced, with JIT off for make() itself so that set() is
-- traced on its own while the local is open.
local function make()
    local mark
    local function set() mark = {} end
    for _ = 1, 50 do set() end
    return function() return mark end
end
jit.off(make)
make()
check('a function storing a table into a local of the function that calls it is found',
      #stores_from('jit_upvalue_test.lua') > 0, #stores)

-- The engine's constructor, with what its construction uses of the shard.
local src = assert(io.open(here .. '../tarn_engine.lua')):read('*a')
local line, at = 0, nil
for l in (src .. '\n'):gmatch('([^\n]*)\n') do
    line = line + 1
    if l:find('local function invalidate_tickets()', 1, true) then at = line end
end
assert(at, 'invalidate_tickets not found in tarn_engine.lua')
local engine = require('tarn_engine')
local shard = {cfg = {}, atomic = function(f, ...) return f(...) end,
               mark = function() return {lua = 0} end, cut = function() return true end}
local function traced()
    for f in pairs(compiled) do
        if f:find('tarn_engine.lua:' .. at, 1, true) and f:sub(-#tostring(at) - 1) == ':' .. at then return true end
    end
    return false
end
-- How a trace of invalidate_tickets gets recorded while the constructor
-- runs, every time:
--   - JIT off for the constructor itself, and for the meter's that it
--     calls, as for make() above: a trace recorded from either would
--     abort on their closures, taking invalidate_tickets in;
--   - functions share hotness counters, and an aborted trace sets its
--     counter back: each round starts the counters again (jit.opt.start)
--     and runs the constructor twice, so that invalidate_tickets is hot
--     the second time unless a function run before it shares its counter;
--     such a function gets compiled or, aborting, blacklisted.
jit.off(engine.new)
jit.off(require('tarn_lazy_meter').new)
local rounds = 0
repeat
    jit.opt.start('hotloop=1')
    engine.new(shard)
    engine.new(shard)
    rounds = rounds + 1
until traced() or rounds == 2000
check("the engine's constructor got invalidate_tickets traced", traced(), rounds)
local bad = stores_from('tarn_engine.lua')
check("and no trace from it stores a collectable value into an open upvalue", #bad == 0, table.concat(bad, ', '))

-- Loading the storage module, with every function it runs traced eagerly.
fio.mktree(work)
box.cfg{work_dir = work, memtx_memory = 64 * 1048576, vinyl_memory = 16 * 1048576, vinyl_cache = 0,
        wal_mode = 'write', checkpoint_interval = 0, log_level = 2}
local before = #stores
require('tarn')
-- (the storage code's own functions: those loaded from this directory)
local dir = here .. '../'
local function ours(f) return f:sub(1, #dir) == dir end
local loaded = {}
for i = before + 1, #stores do
    if ours(stores[i][1]) then loaded[#loaded + 1] = stores[i][1] .. ' (trace ' .. stores[i][2] .. ')' end
end
local n = 0
for f in pairs(compiled) do if ours(f) then n = n + 1 end end
check('loading the storage module traced its functions', n > 0, n)
check('and no trace stores a collectable value into an open upvalue', #loaded == 0, table.concat(loaded, ', '))

print(passed .. ' JIT upvalue checks passed')
os.exit(0)
