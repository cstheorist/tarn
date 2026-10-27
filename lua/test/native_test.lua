-- Native procedures (issue #28): tarn_storage.ts_select against tarn_select,
-- read by read. Each read the native procedure serves must get the Lua
-- procedure's reply; every other one it hands back (["lua"]). Run via
-- lua/test/run.sh after rebalance_test.lua, with the module loaded
-- (TARN_NATIVE, or the release build of this checkout); without it the
-- file only says so.
local net = require('net.box')
local json = require('json')
local fiber = require('fiber')

local S1 = os.getenv('S1')
local opts = {user = 'tarn', password = 'tarn', wait_connected = 5}
local c = net.connect(S1, opts)
assert(c:is_connected(), 'cannot connect to shard 1')

local pass, fail = 0, 0
local function check(name, cond, extra)
    if cond then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. (extra and ('  -- ' .. json.encode(extra)) or '')) end
end
-- A value as text, with the keys of each table in order. (net.box gives the
-- Lua procedure's rows as tuples, the native one's as tables: a SQL server
-- gets both as MessagePack arrays, as it announces no tuple extension.)
local function canon(v)
    if box.tuple.is(v) then v = v:totable() end
    if type(v) ~= 'table' then return json.encode(v) end
    local keys = {}
    for k in pairs(v) do keys[#keys + 1] = k end
    table.sort(keys, function(a, b) return tostring(a) < tostring(b) end)
    local out = {}
    for _, k in ipairs(keys) do out[#out + 1] = tostring(k) .. '=' .. canon(v[k]) end
    return '{' .. table.concat(out, ',') .. '}'
end

local info = c:call('tarn_info')
if not info.native then
    print('the native procedures are not loaded here (TARN_NATIVE): nothing to test')
    print(string.format('%d passed, %d failed', pass, fail))
    os.exit(0)
end

-- test.nat(id, v, pad, bucket_id), memtx, with a non-unique index on v;
-- test.natv the same on vinyl
local function def(name, engine, id)
    return {
        name = name, engine = engine, id = id,
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
                  {name = 'pad', type = 'string'}, {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{field = 'id', type = 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{field = 'v', type = 'integer'}}}},
    }
end
c:call('tarn_create_table', {def('test.nat', 'memtx', 28001)})
c:call('tarn_create_table', {def('test.natv', 'vinyl', 28002)})
c:reload_schema()
local sid, vsid = c.space['test.nat'].id, c.space['test.natv'].id
local map = c:call('tarn_buckets')
if map[2] == 0 then   -- (run alone: the earlier files made a map)
    c:call('tarn_bucket_init', {16, {{1, 8}}})
    map = c:call('tarn_buckets')
end
local b = map[4][1][1]   -- a bucket this shard owns
local other = nil        -- one it does not
for x = 1, map[2] do
    local owned = false
    for _, r in ipairs(map[4]) do if x >= r[1] and x <= r[2] then owned = true end end
    if not owned then other = x break end
end
local epoch = map[3]
for id = 1, 50 do
    c.space['test.nat']:replace{id, id % 7, string.rep('p', id), b}
    c.space['test.natv']:replace{id, id % 7, string.rep('p', id), b}
end

local function stats() local i = c:call('tarn_info').native return i.served, i.to_lua end
-- tarn_select's arguments (route: b unless given; box.NULL: none)
local function args(o)
    local route = o.route
    if rawequal(route, nil) then route = b end
    return {o.sid or sid, o.index or 0, o.key, o.iter or 0, o.limit, o.end_key, o.end_incl ~= false,
            o.lock_check ~= false, o.offset, o.begin or false, o.after, route}
end
-- The native reply must be the Lua one (served), or ["lua"] (handed back).
local function same(name, o, want_served)
    local a = args(o)
    local s0, l0 = stats()
    local n = c:call('tarn_storage.ts_select', a)
    local s1, l1 = stats()
    local l = c:call('tarn_select', a)
    if want_served then
        check(name .. ": served, as the Lua procedure serves it",
              s1 == s0 + 1 and canon(n) == canon(l), {native = n, lua = l, served = {s0, s1}})
    else
        check(name .. ': handed to the Lua procedure', l1 == l0 + 1 and n[1] == 'lua', {native = n, lua = l})
    end
    return n, l
end

-- (the first Lua read with lock_check refreshes the lock index and the held
-- tables; until then the state says they are stale)
c:call('tarn_select', args({key = {1}, limit = 1}))

print('reads it serves')
same('a point read', {key = {7}, limit = 1}, true)
same('a point read of a missing row', {key = {777}, limit = 1}, true)
same('a point read without a limit', {key = {8}}, true)
same('a point read with an offset of 0', {key = {9}, limit = 1, offset = 0}, true)
same('equal rows of a non-unique index', {index = 2, key = {3}, limit = 20}, true)
same('... reversed (REQ)', {index = 2, key = {3}, iter = 1, limit = 3}, true)
same('rows from a key (GE)', {key = {10}, iter = 5, limit = 5}, true)
same('rows after a key (GT)', {key = {10}, iter = 6, limit = 5}, true)
same('rows before a key (LT)', {key = {20}, iter = 3, limit = 4}, true)
same('rows to a key (LE)', {key = {20}, iter = 4, limit = 4}, true)
same('every row (ALL), a limit', {key = {}, iter = 2, limit = 6}, true)
same('every row, no limit (one page)', {key = box.NULL, iter = 2}, true)
same('a read without the lock check', {key = {11}, limit = 1, lock_check = false}, true)
same('a read routed by no bucket', {key = {12}, limit = 1, route = box.NULL}, true)
same('a read routed by buckets this shard owns', {key = {13}, limit = 1, route = {b, b}}, true)
same('a read of every shard with the current epoch', {key = {14}, limit = 1, route = {e = epoch}}, true)
same('... and the current membership', {key = {14}, limit = 1, route = {e = epoch, m = map[6] or 0}}, true)

print('reads it hands to the Lua procedure')
same('a read that ends at a key', {key = {10}, iter = 5, limit = 5, end_key = {12}}, false)
same('a read with an offset', {key = {10}, iter = 5, limit = 5, offset = 2}, false)
same('a limit of 0', {key = {10}, iter = 5, limit = 0}, false)
same('a vinyl table', {sid = vsid, key = {7}, limit = 1}, false)
same('a missing index', {index = 9, key = {7}, limit = 1}, false)
local n, l
if other ~= nil then   -- (a shard that owns every bucket has none of these)
    n, l = same('a read routed by a bucket this shard does not own', {key = {7}, limit = 1, route = other}, false)
    check('... which the Lua procedure refuses', l[1] == 'moved', l)
    same('a read routed by buckets one of which it does not own', {key = {7}, limit = 1, route = {b, other}}, false)
end
n, l = same('a read of every shard with an old epoch', {key = {7}, limit = 1, route = {e = epoch - 1}}, false)
check('... refused', l[1] == 'moved', l)
same('... or an old membership', {key = {7}, limit = 1, route = {e = epoch, m = -1}}, false)
-- pages: the Lua procedure ends them early (rows, then bytes)
c:eval('tarn.cfg.scan_page_rows = 3; tarn.ns_sync()')
n, l = same('a read past the page\'s rows', {key = {10}, iter = 5}, false)
check('... (the Lua procedure ends the page and says where it goes on)', l[1] == 'ok' and #l[2] == 3 and l[3] ~= nil, l)
same('the page after it', {key = {10}, iter = 5, after = l[3]}, false)
same('a read of exactly the limit within the page\'s rows', {key = {10}, iter = 5, limit = 3}, true)
c:eval('tarn.cfg.scan_page_rows = 4096; tarn.cfg.scan_page_bytes = 100; tarn.ns_sync()')
same('a read past the page\'s bytes', {key = {40}, iter = 5, limit = 5}, false)
same('... unless the last row reaches them', {key = {49}, iter = 5, limit = 5}, true)
c:eval('tarn.cfg.scan_page_bytes = 4 * 1024 * 1024; tarn.ns_sync()')
-- a stream transaction
local st = c:new_stream()
st:begin()
n = st:call('tarn_storage.ts_select', args({key = {7}, limit = 1}))
check('a read in a stream transaction: handed to the Lua procedure', n[1] == 'lua', n)
n = st:call('tarn_storage.ts_select', args({key = {7}, limit = 1, begin = true}))
check('... and one that begins it', n[1] == 'lua', n)
st:rollback()
-- a prepared transaction's lock
local ps = c:new_stream()
ps:begin()
local txid = 'NAT-' .. require('uuid').str()
local r = ps:call('tarn_prepare', {txid, 1, {1}, {{'replace', sid, {20, 1, 'x', b}}}, {}, {}, fiber.time(),
                                   box.NULL, box.NULL, {b}})
check('a prepared transaction holds a lock', r[1] == 'yes', r)
same('then a read with the lock check', {key = {7}, limit = 1}, false)
same('... but not one without', {key = {7}, limit = 1, lock_check = false}, true)
c:call('tarn_abort', {txid, 1, {1}})
same('once it ended, a read with the lock check', {key = {7}, limit = 1}, true)
ps = c:new_stream()
ps:begin()
local txid2 = 'NAT-' .. require('uuid').str()
r = ps:call('tarn_prepare', {txid2, 1, {1}, {{'replace', vsid, {21, 1, 'x', b}}}, {}, {}, fiber.time(),
                             box.NULL, box.NULL, {b}})
check('a prepared transaction holds a lock on another table', r[1] == 'yes', r)
same('then a read with the lock check of this one', {key = {7}, limit = 1}, true)
same('... and of that one, handed back as a vinyl table', {sid = vsid, key = {21}, limit = 1}, false)
c:call('tarn_abort', {txid2, 1, {1}})
-- the bucket map read again (a leader change)
c:eval('box.cfg{read_only = true}')
fiber.sleep(0.1)
n = c:call('tarn_storage.ts_select', args({key = {7}, limit = 1}))
check('on a read-only node: handed to the Lua procedure', n[1] == 'lua', n)
c:eval('box.cfg{read_only = false}')
local deadline = fiber.clock() + 5
while c:eval('return box.info.ro') and fiber.clock() < deadline do fiber.sleep(0.05) end
n = c:call('tarn_storage.ts_select', args({key = {7}, limit = 1}))
check('then, until the map is read again, too', n[1] == 'lua', n)
c:call('tarn_select', args({key = {7}, limit = 1}))
same('once a Lua read read it again: served', {key = {7}, limit = 1}, true)
-- the fences and the held tables read again (a leader change, a DDL)
c:eval('tarn.stale_fences()')
same('with the held tables to be read again, a read with the lock check', {key = {7}, limit = 1}, false)
same('once a Lua read read them again: served', {key = {7}, limit = 1}, true)
-- a table held by a TRUNCATE or DROP TABLE (its hold, as their PREPARE
-- stores it)
local hold = 'hold:' .. vsid
c:eval('box.space._tarn_meta:replace{..., {op = "TRUNCATE", ver = 0, label = "NAT-HOLD"}} tarn.stale_fences()',
       {hold})
c:call('tarn_select', args({key = {7}, limit = 1}))
same('while a TRUNCATE or DROP TABLE holds a table, a read with the lock check', {key = {7}, limit = 1}, false)
same('... but not one without', {key = {7}, limit = 1, lock_check = false}, true)
c:eval('box.space._tarn_meta:delete{...} tarn.stale_fences()', {hold})
c:call('tarn_select', args({key = {7}, limit = 1}))
same('once it ends, a read with the lock check', {key = {7}, limit = 1}, true)

print('the load of a bucket')
local function reads_of(x)
    for _, e in ipairs(c:call('tarn_bucket_stats')[3]) do if e[1] == x then return e[2] end end
    return 0
end
local r0 = reads_of(b)
for _ = 1, 5 do c:call('tarn_storage.ts_select', args({key = {7}, limit = 1})) end
check('reads the native procedure serves count for their bucket', reads_of(b) - r0 == 5, {r0, reads_of(b)})
-- The counts go by windows (cfg.load_window), which the Lua procedures
-- start: once one ended, they count the next read.
c:eval('tarn.cfg.load_window = 1; tarn.ns_sync()')
fiber.sleep(1.1)
same('once a window of the load counts ended, a read routed by no bucket (it counts for none)',
     {key = {7}, limit = 1, route = box.NULL}, true)
same('... but one routed by a bucket', {key = {7}, limit = 1}, false)
same('... which the Lua procedure counted, starting the next window: then served', {key = {7}, limit = 1}, true)
check('... and they count', reads_of(b) >= 3, reads_of(b))
c:eval('tarn.cfg.load_window = 60; tarn.ns_sync()')

print('a module of another build')
-- (its state's layout is not the Lua module's: it fails as a module that
-- does not load, ER_LOAD_FUNCTION, and the SQL servers stop calling it)
local layout = c:eval("return require('ffi').cast('struct tarn_ns *', box.func['tarn_storage.ts_state']:call()).layout")
c:eval("box.func['tarn_storage.ts_state']:call({...})", {layout + 1})
local ok, err = pcall(c.call, c, 'tarn_storage.ts_select', args({key = {7}, limit = 1}))
check('its reads fail (99)', not ok and err.code == 99, tostring(err))
c:eval("box.func['tarn_storage.ts_state']:call({...})", {layout})
same('the same layout again: served', {key = {7}, limit = 1}, true)

c:call('tarn_drop_table', {'test.nat'})
c:call('tarn_drop_table', {'test.natv'})
print(string.format('%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
