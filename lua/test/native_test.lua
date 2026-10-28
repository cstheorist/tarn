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

-- test.nat(id, v, pad, bucket_id, w), memtx: indexes 2 on v, 3 on (v, id),
-- 4 on w (nullable), 5 on pad (case-insensitive); test.natv the same on
-- vinyl; `dbl`: a field d after w, a double (nullable)
local function def(name, engine, id, dbl)
    local format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
                    {name = 'pad', type = 'string'}, {name = 'bucket_id', type = 'unsigned'},
                    {name = 'w', type = 'integer', is_nullable = true}}
    if dbl then format[6] = {name = 'd', type = 'double', is_nullable = true} end
    return {
        name = name, engine = engine, id = id,
        format = format,
        indexes = {{name = 'primary', unique = true, parts = {{field = 'id', type = 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{field = 'v', type = 'integer'}}},
                   {name = 'vid', unique = true, parts = {{field = 'v', type = 'integer'},
                                                          {field = 'id', type = 'unsigned'}}},
                   {name = 'w', unique = false, parts = {{field = 'w', type = 'integer', is_nullable = true}}},
                   {name = 'padci', unique = false,
                    parts = {{field = 'pad', type = 'string', collation = 'unicode_ci'}}}},
    }
end
c:call('tarn_create_table', {def('test.nat', 'memtx', 28001)})
c:call('tarn_create_table', {def('test.natv', 'vinyl', 28002)})
-- twins for the writes: test.wa written by the native procedure, test.wb by
-- the Lua one
c:call('tarn_create_table', {def('test.wa', 'memtx', 28003, true)})
c:call('tarn_create_table', {def('test.wb', 'memtx', 28004, true)})
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
-- (w: NULL for every third row; pad: 'p's, then 'P's)
local function row(id)
    return {id, id % 7, string.rep(id % 2 == 0 and 'p' or 'P', id), b, id % 3 == 0 and box.NULL or id}
end
for id = 1, 50 do
    c.space['test.nat']:replace(row(id))
    c.space['test.natv']:replace(row(id))
end

local function stats() local i = c:call('tarn_info').native return i.served, i.to_lua end
-- tarn_select's arguments (route: b unless given; box.NULL: none)
local function args(o)
    local route = o.route
    if rawequal(route, nil) then route = b end
    return {o.sid or sid, o.index or 0, o.key, o.iter or 0, o.limit, o.end_key, o.end_incl ~= false,
            o.lock_check ~= false, o.offset, o.begin or false, o.after, route}
end
-- The native reply must be the Lua one (served), or ["lua"] (handed back);
-- `via`: a stream to call both in (default: no stream).
local function same(name, o, want_served, via)
    via = via or c
    local a = args(o)
    local s0, l0 = stats()
    local n = via:call('tarn_storage.ts_select', a)
    local s1, l1 = stats()
    local l = via:call('tarn_select', a)
    if want_served then
        check(name .. ": served, as the Lua procedure serves it",
              s1 == s0 + 1 and canon(n) == canon(l), {native = n, lua = l, served = {s0, s1}})
    else
        check(name .. ': handed to the Lua procedure', l1 == l0 + 1 and n[1] == 'lua', {native = n, lua = l})
    end
    return n, l
end

-- Why the native procedures handed requests back: the count of procedure p
-- (read, write, commit, validate) for `reason` (tarn_info().native.fallbacks).
local function fallbacks(p, reason)
    local f = c:call('tarn_info').native.fallbacks
    return f and f[p] and f[p][reason] or 0
end
-- f() hands one request of procedure p back, for `reason`.
local function because(p, reason, f)
    local all0 = c:call('tarn_info').native.fallbacks[p] or {}
    f()
    local all1 = c:call('tarn_info').native.fallbacks[p] or {}
    local n0, n1 = all0[reason] or 0, all1[reason] or 0
    check('... counted as ' .. p .. ' ' .. reason, n1 == n0 + 1, {before = all0, after = all1})
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

print('reads to an end key')
same('rows from a key to an end key (GE, included)', {key = {10}, iter = 5, end_key = {15}, limit = 100}, true)
same('... the end key excluded', {key = {10}, iter = 5, end_key = {15}, end_incl = false, limit = 100}, true)
same('... no limit', {key = {10}, iter = 5, end_key = {15}}, true)
same('... a limit before the end key', {key = {10}, iter = 5, end_key = {40}, limit = 3}, true)
same('rows after a key, to an end key (GT)', {key = {10}, iter = 6, end_key = {12}}, true)
same('going down (LE), to an end key', {key = {20}, iter = 4, end_key = {16}}, true)
same('... excluded (LT)', {key = {20}, iter = 3, end_key = {16}, end_incl = false}, true)
same('an end key past every row', {key = {45}, iter = 5, end_key = {1000}}, true)
same('an end key before the start key', {key = {30}, iter = 5, end_key = {20}}, true)
same('every row (ALL), to an end key', {key = {}, iter = 2, end_key = {5}}, true)
same('the equal rows of an index (EQ), to an end key', {index = 2, key = {3}, end_key = {3}}, true)
same('... going down (REQ)', {index = 2, key = {3}, iter = 1, end_key = {3}, limit = 4}, true)
same('a prefix of a two-part index, to another prefix', {index = 3, key = {2}, iter = 5, end_key = {4}}, true)
same('... to a whole key, excluded', {index = 3, key = {2}, iter = 5, end_key = {4, 18}, end_incl = false}, true)
same('a nullable index, from NULL', {index = 4, key = {box.NULL}, iter = 5, end_key = {10}}, true)
same('... going down to NULL', {index = 4, key = {10}, iter = 4, end_key = {box.NULL}}, true)
same('an index with a collation', {index = 5, key = {'ppp'}, iter = 5, end_key = {'PPPPPPP'}}, true)
same('... going down', {index = 5, key = {'PPPPPPP'}, iter = 4, end_key = {'ppp'}, end_incl = false}, true)
n = c:call('tarn_storage.ts_select', args({key = {10}, iter = 5, end_key = {'x'}}))
check('an end key the index cannot take: handed to the Lua procedure', n[1] == 'lua', n)
n = c:call('tarn_storage.ts_select', args({key = {10}, iter = 5, end_key = {11, 12}}))
check('... nor one longer than its key', n[1] == 'lua', n)

print('reads it hands to the Lua procedure')
-- (a read to an end key: see above)
same('a read with an offset', {key = {10}, iter = 5, limit = 5, offset = 2}, false)
same('a limit of 0', {key = {10}, iter = 5, limit = 0}, false)
because('read', 'vinyl', function() same('a vinyl table', {sid = vsid, key = {7}, limit = 1}, false) end)
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
because('read', 'page', function() n, l = same('a read past the page\'s rows', {key = {10}, iter = 5}, false) end)
check('... (the Lua procedure ends the page and says where it goes on)', l[1] == 'ok' and #l[2] == 3 and l[3] ~= nil, l)
because('read', 'next_page', function() same('the page after it', {key = {10}, iter = 5, after = l[3]}, false) end)
same('a read of exactly the limit within the page\'s rows', {key = {10}, iter = 5, limit = 3}, true)
c:eval('tarn.cfg.scan_page_rows = 4096; tarn.cfg.scan_page_bytes = 100; tarn.ns_sync()')
same('a read past the page\'s bytes', {key = {40}, iter = 5, limit = 5}, false)
same('... unless the last row reaches them', {key = {49}, iter = 5, limit = 5}, true)
c:eval('tarn.cfg.scan_page_bytes = 4 * 1024 * 1024; tarn.ns_sync()')
-- a read to an end key scans: it ends the page once its rows reach the
-- page's, short of its limit, or its bytes do, the last row's included
c:eval('tarn.cfg.scan_page_rows = 3; tarn.ns_sync()')
n, l = same('a read to an end key whose rows reach the page\'s', {key = {10}, iter = 5, end_key = {12}}, false)
check('... (the Lua procedure ends the page there, though the end key is reached)', l[3] ~= nil, l)
same('... one that stops at its limit first', {key = {10}, iter = 5, end_key = {12}, limit = 3}, true)
same('... or at the end key', {key = {10}, iter = 5, end_key = {11}}, true)
c:eval('tarn.cfg.scan_page_rows = 4096; tarn.cfg.scan_page_bytes = 100; tarn.ns_sync()')
same('a read to an end key whose last row reaches the page\'s bytes', {key = {49}, iter = 5, end_key = {50}}, false)
c:eval('tarn.cfg.scan_page_bytes = 4 * 1024 * 1024; tarn.ns_sync()')

print('reads in a stream\'s transaction')
local st = c:new_stream()
st:begin()
same('a point read', {key = {7}, limit = 1, lock_check = false}, true, st)
same('rows to an end key', {key = {10}, iter = 5, end_key = {20}, lock_check = false}, true, st)
same('rows with no limit', {index = 2, key = {3}, lock_check = false}, true, st)
st.space['test.nat']:replace(row(60))
same('a row the transaction wrote', {key = {60}, limit = 1, lock_check = false}, true, st)
same('a read with begin, the transaction begun already', {key = {7}, limit = 1, begin = true, lock_check = false},
     true, st)
c:eval('tarn.cfg.scan_page_rows = 3; tarn.ns_sync()')
same('a read with no limit: a scan, whose rows end the page', {index = 2, key = {3}, lock_check = false}, false, st)
same('... a limit within the scan chunk: a select', {index = 2, key = {3}, limit = 3, lock_check = false}, true, st)
c:eval('tarn.cfg.scan_page_rows = 4096; tarn.ns_sync()')
st:rollback()
-- MVCC tracks the native reads of a transaction: one that read a row that
-- was replaced since cannot commit a write
st = c:new_stream()
st:begin()
n = st:call('tarn_storage.ts_select', args({key = {7}, limit = 1, lock_check = false}))
c.space['test.nat']:replace{7, 100, 'changed', b}
local ok = pcall(function()
    st.space['test.nat']:replace(row(61))
    st:commit()
end)
check('a transaction whose native read of a row was overwritten cannot commit', n[1] == 'ok' and not ok, n)
pcall(st.rollback, st)
c.space['test.nat']:replace(row(7))
st = c:new_stream()
st:begin()
n = st:call('tarn_storage.ts_select', args({key = {8}, limit = 1, lock_check = false}))
c.space['test.nat']:replace{7, 100, 'changed', b}
ok = pcall(function()
    st.space['test.nat']:replace(row(61))
    st:commit()
end)
check('... one that read another row commits', n[1] == 'ok' and ok, n)
c.space['test.nat']:replace(row(7))
c.space['test.nat']:delete{61}
c.space['test.nat']:delete{60}
-- a prepared transaction's locks: W on the row it writes, and on that row's
-- keys in the other indexes (index 3 is vid, unique: (v, id))
local ps = c:new_stream()
ps:begin()
local txid = 'NAT-' .. require('uuid').str()
local r = ps:call('tarn_prepare', {txid, 1, {1}, {{'replace', sid, {20, 1, 'x', b}}}, {}, {}, fiber.time(),
                                   box.NULL, box.NULL, {b}})
check('a prepared transaction holds a lock', r[1] == 'yes', r)
-- (a read the Lua procedure would wait for: the native procedure alone)
local function handed(name, o)
    local _, l0 = stats()
    local n = c:call('tarn_storage.ts_select', args(o))
    local _, l1 = stats()
    check(name .. ': handed to the Lua procedure', l1 == l0 + 1 and n[1] == 'lua', n)
end
handed('then a point read of the row it writes, with the lock check', {key = {20}, limit = 1})
same('... but not one without', {key = {20}, limit = 1, lock_check = false}, true)
same('... a point read of another row', {key = {7}, limit = 1}, true)
same('... of a key of another index that it does not write', {index = 3, key = {0, 7}, limit = 1}, true)
handed('... of the key it gives the row there', {index = 3, key = {1, 20}, limit = 1})
handed('... of the key the row had there', {index = 3, key = {6, 20}, limit = 1})
-- reads of ranges: the range from the read's key to its end key, or to its
-- last row when its limit cut it short, compared with the W-locked keys of
-- the index (select_locked, its bounds included)
same('... a read of a range of the primary key short of the row', {key = {1}, iter = 5, limit = 3}, true)
because('read', 'w_lock', function() handed('... one whose last row is the row', {key = {18}, iter = 5, limit = 3}) end)
same('... one past it', {key = {21}, iter = 5, limit = 3}, true)
handed('... one from it, though the row is excluded (GT)', {key = {20}, iter = 6, limit = 3})
same('... to an end key short of it', {key = {10}, iter = 5, end_key = {19}}, true)
handed('... to an end key past it', {key = {10}, iter = 5, end_key = {25}})
same('... with no end and no limit, past it', {key = {21}, iter = 5}, true)
handed('... from the start', {key = {1}, iter = 5})
same('... backwards, short of it', {key = {19}, iter = 3, limit = 3}, true)
same('... backwards, past it', {key = {22}, iter = 4, limit = 2}, true)
handed('... backwards, to it', {key = {22}, iter = 4, limit = 3})
same('... backwards to an end key short of it', {key = {30}, iter = 4, end_key = {21}}, true)
handed('... backwards to an end key past it', {key = {30}, iter = 4, end_key = {20}})
-- (index v: the row's key was 6, is 1)
same('... rows of a key of index v the row neither had nor takes', {index = 2, key = {5}}, true)
handed('... of the key it had', {index = 2, key = {6}})
handed('... of the key it takes', {index = 2, key = {1}})
same('... a range of keys between them, cut by its limit', {index = 2, key = {2}, iter = 5, limit = 3}, true)
handed('... a range of keys that holds one of them', {index = 2, key = {5}, iter = 5, limit = 50})
-- (index vid: a key of its first part is a prefix of the row's keys there)
handed('... rows of index vid by the first part of the key the row takes', {index = 3, key = {1}})
same('... by another', {index = 3, key = {2}}, true)
-- (index padci compares without case: the row's key was 20 'p's, is 'x')
handed('... rows of index padci by the key the row had, in capitals', {index = 5, key = {string.rep('P', 20)}})
handed('... by the key it takes, in capitals', {index = 5, key = {'X'}})
same('... by another', {index = 5, key = {'PPP'}}, true)
-- (index w: the row's key was 20, is NULL)
same('... a range of index w above both', {index = 4, key = {21}, iter = 5, limit = 2}, true)
handed('... one below 21, backwards, whose last row is the row', {index = 4, key = {21}, iter = 3, limit = 1})
c:call('tarn_abort', {txid, 1, {1}})
same('once it ended, a read with the lock check', {key = {20}, limit = 1}, true)
same('... of a range', {key = {1}, iter = 5, limit = 3}, true)
-- R locks: a prepared transaction that read rows of the table, a range of it
-- (and wrote another table)
ps = c:new_stream()
ps:begin()
txid = 'NAT-' .. require('uuid').str()
r = ps:call('tarn_prepare', {txid, 1, {1}, {{'replace', vsid, {20, 1, 'x', b}}}, {{sid, 0, {7}, b}},
                             {{sid, 2, {1}, {3}, true, true, b}}, fiber.time(), box.NULL, box.NULL, {b}})
check('a prepared transaction read rows of the table', r[1] == 'yes', r)
same('then a point read of one of them', {key = {7}, limit = 1}, true)
same('... a read of the range it read', {index = 2, key = {2}, limit = 20}, true)
c:call('tarn_abort', {txid, 1, {1}})
ps = c:new_stream()
ps:begin()
local txid2 = 'NAT-' .. require('uuid').str()
r = ps:call('tarn_prepare', {txid2, 1, {1}, {{'replace', vsid, {21, 1, 'x', b}}}, {}, {}, fiber.time(),
                             box.NULL, box.NULL, {b}})
check('a prepared transaction holds a lock on another table', r[1] == 'yes', r)
same('then a read with the lock check of this one', {key = {7}, limit = 1}, true)
same('... and of that one, handed back as a vinyl table', {sid = vsid, key = {21}, limit = 1}, false)
c:call('tarn_abort', {txid2, 1, {1}})
-- a leader change: the state read again
c:eval('box.cfg{read_only = true}')
fiber.sleep(0.1)
because('read', 'read_only', function() n = c:call('tarn_storage.ts_select', args({key = {7}, limit = 1})) end)
check('on a read-only node: handed to the Lua procedure', n[1] == 'lua', n)
c:eval('box.cfg{read_only = false}')
local deadline = fiber.clock() + 5
while c:eval('return box.info.ro') and fiber.clock() < deadline do fiber.sleep(0.05) end
because('read', 'stale_locks', function() n = c:call('tarn_storage.ts_select', args({key = {7}, limit = 1})) end)
check('then, until the lock index is made again, too', n[1] == 'lua', n)
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

print('reads that begin a stream\'s transaction')
local function in_txn(s) return s:eval('return box.is_in_txn()') end
do
st = c:new_stream()
local sl = c:new_stream()
local s0 = stats()
n = st:call('tarn_storage.ts_select', args({key = {7}, limit = 1, begin = true, lock_check = false}))
local s1 = stats()
l = sl:call('tarn_select', args({key = {7}, limit = 1, begin = true, lock_check = false}))
check('the first read of a stream: served, as the Lua procedure serves it, beginning its transaction',
      s1 == s0 + 1 and canon(n) == canon(l) and in_txn(st) and in_txn(sl), {native = n, lua = l})
local iso = 'return box.internal.txn_isolation()'
check('... with the isolation of the Lua procedure\'s (read-confirmed)', st:eval(iso) == sl:eval(iso),
      {native = st:eval(iso), lua = sl:eval(iso)})
same('... the reads after it, in it', {key = {8}, limit = 1, lock_check = false}, true, st)
-- (a transaction MVCC tracks: once a row it read is replaced, it cannot
-- validate)
c.space['test.nat']:replace{7, 100, 'changed', b}
n = st:call('tarn_validate', {'NAT-B', {{sid, 0, {7}, b}}, {}, false, box.NULL, box.NULL})
check('... whose reads MVCC tracks', n[1] == 'conflict', n)
c.space['test.nat']:replace(row(7))
pcall(st.rollback, st)
pcall(sl.rollback, sl)
-- a first read handed back takes its transaction back: tarn_select begins it
c:eval('tarn.cfg.scan_page_rows = 3; tarn.ns_sync()')
st = c:new_stream()
n = st:call('tarn_storage.ts_select', args({index = 2, key = {3}, begin = true, lock_check = false}))
check('a first read handed to the Lua procedure (its rows end the page) has begun no transaction',
      n[1] == 'lua' and not in_txn(st), n)
l = st:call('tarn_select', args({index = 2, key = {3}, begin = true, lock_check = false}))
check('... which tarn_select begins then', l[1] == 'ok' and in_txn(st), l)
pcall(st.rollback, st)
c:eval('tarn.cfg.scan_page_rows = 4096; tarn.ns_sync()')
end

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

print('autocommit writes')
local wa, wb = c.space['test.wa'].id, c.space['test.wb'].id
local function wstats() local i = c:call('tarn_info').native return i.writes, i.writes_to_lua end
-- tarn_autocommit's arguments for the writes `ws` ({op, payload[, ops,
-- ranges, pad]}) to space sid; opts: as tarn_autocommit takes them (wb: b
-- unless given)
local function wargs(sid, ws, opts)
    local out = {}
    for i, w in ipairs(ws) do
        local x = {w[1], sid, w[2]}
        for j = 3, #w do x[j + 1] = w[j] end
        out[i] = x
    end
    local o = {lock_timeout = 1, found_rows = opts and opts.found_rows or false,
               wb = opts and opts.wb or {b}}
    if opts and opts.fence then o.fence = {sid, opts.fence} end
    return {out, o}
end
-- the rows of a table, as their bytes (which a later write compares)
local function rows(sid)
    return c:eval([[local m, out = require('msgpack'), {}
        for _, t in box.space[...]:pairs() do
            out[#out + 1] = (m.encode(t):gsub('.', function(x) return string.format('%02x', x:byte()) end))
        end
        return table.concat(out, ' ')]], {sid})
end
-- The writes go to both twins: the native procedure's reply must be the Lua
-- one's, and the tables the same (served); or it hands them back, having
-- written nothing, and the Lua procedure writes them to both.
local function wsame(name, ws, opts, want_served)
    local before = rows(wa)
    local s0, l0 = wstats()
    local n = c:call('tarn_storage.ts_autocommit', wargs(wa, ws, opts))
    local s1, l1 = wstats()
    local l = c:call('tarn_autocommit', wargs(wb, ws, opts))
    if want_served then
        check(name .. ': served, as the Lua procedure serves it',
              s1 == s0 + 1 and canon(n) == canon(l) and rows(wa) == rows(wb), {native = n, lua = l})
    else
        local untouched = rows(wa) == before
        local l2 = c:call('tarn_autocommit', wargs(wa, ws, opts))
        -- (an error names its table)
        local alike = l2[1] == l[1] and l2[2] == l[2]
        check(name .. ': handed to the Lua procedure, nothing written',
              l1 == l0 + 1 and n[1] == 'lua' and untouched and alike and rows(wa) == rows(wb),
              {native = n, lua = l, untouched = untouched})
    end
    return n, l
end
local function trow(id, v, w) return {id, v or id % 7, 'r' .. id, b, w} end
wsame('an insert', {{'insert', trow(1)}}, nil, true)
wsame('... of rows: they count, the last one is returned', {{'insert', trow(2)}, {'insert', trow(3, 5, 9)}}, nil, true)
local n, l = wsame('an insert of a row that is there', {{'insert', trow(1)}}, nil, false)
check('... (the Lua procedure refuses it: 1062)', l[1] == 'duplicate' and l[2] == 1062, l)
because('write', 'write_error', function()
    wsame('rows of which one is there', {{'insert', trow(4)}, {'insert', trow(2)}}, nil, false)
end)
wsame('a replace of a new row', {{'replace', trow(5)}}, nil, true)
wsame('... of a row that is there, changed (2 affected)', {{'replace', trow(5, 1)}}, nil, true)
wsame('... unchanged (1 affected)', {{'replace', trow(5, 1)}}, nil, true)
wsame('a delete', {{'delete', {5}}}, nil, true)
wsame('a delete of a row that is not there', {{'delete', {5}}}, nil, true)
wsame('an update', {{'update', {1}, {{'=', 3, 'x'}}, {}, box.NULL}}, nil, true)
wsame('... that changes nothing', {{'update', {1}, {{'=', 3, 'x'}}, {}, box.NULL}}, nil, true)
wsame('... with found rows', {{'update', {1}, {{'=', 3, 'x'}}, {}, box.NULL}}, {found_rows = true}, true)
wsame('... of a row that is not there', {{'update', {99}, {{'=', 3, 'x'}}, {}, box.NULL}}, nil, true)
wsame('an addition in range', {{'update', {3}, {{'+', 5, 2}}, {{5, -2147483648, 2147483647, 'w'}}, box.NULL}},
      nil, true)
wsame('an addition to NULL (stays NULL)', {{'update', {1}, {{'+', 5, 2}}, {{5, 0, 255, 'w'}}, box.NULL}}, nil, true)
because('write', 'arith', function()
    n, l = wsame('an addition out of range', {{'update', {3}, {{'+', 5, 1000}}, {{5, 0, 255, 'w'}}, box.NULL}}, nil,
                 false)
end)
check('... (the Lua procedure refuses it: 1264)', l[1] == 'error' and l[2] == 1264, l)
-- (a pad {4, 0}: the rows have 5 fields, which rows written before an
-- ALTER TABLE added the fifth lack)
wsame('an update with a pad, of a row that has every field', {{'update', {3}, {{'=', 3, 'y'}}, {}, {4, 0}}}, nil,
      true)
wsame('a row without its last fields', {{'insert', {8, 1, 'r8', b}}}, nil, true)
because('write', 'pad', function()
    wsame('... updated with a pad (the Lua procedure fills them in)', {{'update', {8}, {{'=', 3, 'y'}}, {}, {4, 0}}},
          nil, false)
end)
wsame('an update of an indexed column', {{'update', {2}, {{'=', 2, 6}}, {}, box.NULL}}, nil, true)
wsame('a write to a bucket this shard does not own', {{'insert', trow(6)}}, {wb = {other or 1000000}}, false)
wsame('... of a row of such a bucket', {{'insert', {7, 0, 'x', other or 1000000}}}, nil, false)
-- a prepared transaction's locks on the table: W on the row it writes and on
-- its keys in the other indexes, R on a key of index v (2) it read.
-- (tarn_autocommit waits for those its writes meet: the native procedure
-- alone is called then)
local wps = c:new_stream()
wps:begin()
local wtx = 'NAT-' .. require('uuid').str()
r = wps:call('tarn_prepare', {wtx, 1, {1}, {{'replace', wa, trow(40)}}, {{wa, 2, {6}, b}}, {}, fiber.time(),
                               box.NULL, box.NULL, {b}})
check('a prepared transaction holds locks on the table', r[1] == 'yes', r)
because('write', 'lock', function() n = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'insert', trow(40)}})) end)
check('then a write of the row it writes: handed to the Lua procedure', n[1] == 'lua', n)
n = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'insert', trow(41)}}))
check('... of a row with the key of index v it read (6): handed to the Lua procedure', n[1] == 'lua', n)
n = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'update', {3}, {{'=', 2, 6}}, {}, box.NULL}}))
check('... an update that gives a row that key: handed to the Lua procedure', n[1] == 'lua', n)
-- (row 43 has the keys of row 40 in the indexes bucket_id and w, which are
-- not unique: their W locks are no conflict)
wsame('... an insert of another row', {{'insert', trow(43)}}, nil, true)
wsame('... its delete', {{'delete', {43}}}, nil, true)
c:call('tarn_abort', {wtx, 1, {1}})
-- range locks on the table (a prepared transaction read ranges of it): the
-- keys a write gives each index are compared with the ranges of the
-- index's locks (range_locked, every bound taken as included)
wps = c:new_stream()
wps:begin()
wtx = 'NAT-' .. require('uuid').str()
local function rl(iid, lo, hi, lo_incl) return {wa, iid, lo, hi, lo_incl ~= false, true, b} end
r = wps:call('tarn_prepare', {wtx, 1, {1}, {{'replace', sid, {21, 1, 'x', b}}}, {},
                               {rl(0, {100}, {200}), rl(2, {2}, {3}), rl(3, {5}, {5}), rl(4, box.NULL, {5}),
                                rl(5, {'R40'}, {'R41'}), rl(2, {0}, {0}, false)},
                               fiber.time(), box.NULL, box.NULL, {b}})
check('a prepared transaction read ranges of the table', r[1] == 'yes', r)
for _, x in ipairs({{'then an insert of a row whose primary key is in a range', trow(150, 4, 50)},
                    {'... whose key of index v is', trow(44, 2, 50)},
                    {'... on its upper bound', trow(45, 3, 50)},
                    {'... whose key of index vid is, by a prefix', trow(47, 5, 50)},
                    {'... whose NULL of index w is (no lower bound)', trow(48, 4)},
                    {'... whose key of index padci is (without case)', trow(41, 6, 50)},
                    {'... whose key of index v is on a bound its range excludes (taken as included)',
                     trow(49, 0, 50)}}) do
    because('write', 'range_lock', function()
        n = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'insert', x[2]}}))
    end)
    check(x[1] .. ': handed to the Lua procedure', n[1] == 'lua', n)
end
n = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'delete', {150}}}))
check('... a delete of a row that is not there, by a key in a range: handed to the Lua procedure', n[1] == 'lua', n)
wsame('... an insert of a row whose keys are in none', {{'insert', trow(46, 4, 50)}}, nil, true)
n = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'update', {46}, {{'=', 2, 2}}, {}, box.NULL}}))
check('... an update that gives it a key in one: handed to the Lua procedure', n[1] == 'lua', n)
wsame('... one that keeps its keys out of them', {{'update', {46}, {{'=', 5, 60}}, {}, box.NULL}}, nil, true)
wsame('... its delete', {{'delete', {46}}}, nil, true)
c:call('tarn_abort', {wtx, 1, {1}})
-- a row lock (READ COMMITTED): tarn_autocommit waits for one on what it
-- writes
r = c:call('tarn_lock', {'NAT-RC', {{wa, 0, {9}}}, 'X', {timeout = 1}})
check('a row lock is held on a row of the table', r[1] == 'ok', r)
because('write', 'row_lock', function() n = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'replace', trow(9)}})) end)
check('then a write of that row: handed to the Lua procedure', n[1] == 'lua', n)
wsame('... of another row', {{'insert', trow(41)}}, nil, true)
c:call('tarn_unlock', {'NAT-RC'})
-- ... on another table
r = c:call('tarn_lock', {'NAT-RC', {{sid, 0, {9}}}, 'X', {timeout = 1}})
check('a row lock is held on another table', r[1] == 'ok', r)
wsame('then a write', {{'replace', trow(41, 2)}}, nil, true)
c:call('tarn_unlock', {'NAT-RC'})
-- a definition fence raised (ALTER TABLE): writes with the old one refused
c:eval("box.space._tarn_meta:replace{'fence:' .. ..., {ver = 7}} tarn.stale_fences()", {wa})
c:eval("box.space._tarn_meta:replace{'fence:' .. ..., {ver = 7}} tarn.stale_fences()", {wb})
because('write', 'stale_fences', function()
    n, l = wsame('a write with an older definition than the fence', {{'insert', trow(42)}}, {fence = 6}, false)
end)
check('... (refused: 1412)', l[2] == 1412, l)
-- (the Lua procedure read the fences again: now the native one compares
-- the write's definition with them)
because('write', 'fence', function()
    n, l = wsame('... again, with the fences read', {{'insert', trow(42)}}, {fence = 6}, false)
end)
check('... (refused: 1412)', l[2] == 1412, l)
wsame('... and one with the current definition', {{'insert', trow(42)}}, {fence = 7}, true)
c:eval("box.space._tarn_meta:delete{'fence:' .. ...} tarn.stale_fences()", {wa})
c:eval("box.space._tarn_meta:delete{'fence:' .. ...} tarn.stale_fences()", {wb})
-- (the stale fences: read again)
c:call('tarn_autocommit', wargs(wa, {{'delete', {42}}}))
c:call('tarn_autocommit', wargs(wb, {{'delete', {42}}}))
wsame('a write with the fences read again', {{'delete', {41}}}, nil, true)
-- values as the Lua procedure stores them: a double that holds an integer
-- is stored as that integer (the SQL server sends DOUBLE values as doubles)
local ffi = require('ffi')
local function D(x) return ffi.cast('double', x) end
local function drow(id, d) return {id, 1, 'd' .. id, b, box.NULL, d} end
wsame('inserts of doubles and floats that hold integers',
      {{'insert', drow(60, D(5))}, {'insert', drow(61, D(-0.0))}, {'insert', drow(62, D(2^63))},
       {'insert', drow(63, D(-2^63))}, {'insert', drow(64, ffi.cast('float', 2))}}, nil, true)
wsame('... and of other ones',
      {{'insert', drow(65, D(1.5))}, {'insert', drow(66, D(1e300))}, {'insert', drow(67, ffi.cast('float', 0.5))},
       {'insert', drow(68, D(-1e19))}}, nil, true)
n, l = wsame('an update that sets a double to the integer the row holds', {{'update', {60}, {{'=', 6, D(5)}}, {}, box.NULL}},
             nil, true)
check('... (0 affected)', l[2] == 0, l)
wsame('... that adds a double to it', {{'update', {60}, {{'+', 6, D(1)}}, {}, box.NULL}}, nil, true)
n, l = wsame('a replace by the same row, with doubles', {{'replace', drow(61, D(0))}}, nil, true)
check('... (1 affected)', l[2] == 1, l)
wsame('a delete by a key given as a double', {{'delete', {D(68)}}}, nil, true)
wsame('a value that is not a number', {{'insert', drow(69, D(0 / 0))}}, nil, false)
-- synchronous tables (a shard with replicas): served, each commit waiting
-- for its quorum, as the Lua procedure's does (here this node alone, once
-- it owns the queue of synchronous transactions)
local function sync_twin(name)
    return c:eval([[local name, like = ...
        local s = box.schema.space.create(name, {is_sync = true, format = box.space[like]:format()})
        s:create_index('primary', {parts = {'id'}})
        s:create_index('bucket_id', {parts = {'bucket_id'}, unique = false})
        return s.id]], {name, 'test.wa'})
end
c:eval("box.ctl.promote() require('fiber').sleep(0.1)")
local wa0, wb0 = wa, wb
wa, wb = sync_twin('test_native_sync_a'), sync_twin('test_native_sync_b')
-- (the promotion makes the state stale: the Lua procedures read it again)
local deadline0 = fiber.clock() + 5
repeat
    c:call('tarn_autocommit', wargs(wb, {{'delete', {999}}}))
    n = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'delete', {999}}}))
until n[1] ~= 'lua' or fiber.clock() > deadline0
wsame('inserts into a synchronous table', {{'insert', trow(1)}, {'insert', trow(2)}}, nil, true)
wsame('... an update', {{'update', {1}, {{'=', 3, 'x'}}, {}, box.NULL}}, nil, true)
wsame('... a delete', {{'delete', {2}}}, nil, true)
-- (a quorum of 2, which this node alone cannot give)
c:eval('box.cfg{replication_synchro_quorum = 2, replication_synchro_timeout = 0.5}')
local tn = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'insert', trow(4)}}))
local tl = c:call('tarn_autocommit', wargs(wb, {{'insert', trow(4)}}))
check('a commit whose quorum times out: the Lua procedure\'s reply',
      tn[1] == tl[1] and tn[2] == tl[2] and tn[3] == tl[3] and tn[1] ~= 'ok', {native = tn, lua = tl})
check('... nothing written', rows(wa) == rows(wb))
c:eval('box.cfg{replication_synchro_timeout = 30}')
local fw = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'insert', trow(3)}}), {is_async = true})
fiber.sleep(0.2)
check('a write whose commit waits for its quorum', not fw:is_ready())
local fl = c:call('tarn_lock', {'NAT-SYNC', {{wa, 0, {3}}}, 'X', {timeout = 5}}, {is_async = true})
fiber.sleep(0.2)
check('... is in flight: a row lock on its row waits for it', not fl:is_ready())
c:eval('box.cfg{replication_synchro_quorum = 1}')
local rw = fw:wait_result(5)
check('... it is made once its quorum is', rw ~= nil and rw[1][1] == 'ok', rw)
local rl = fl:wait_result(5)
check('... then the lock is granted', rl ~= nil and rl[1][1] == 'ok', rl)
c:call('tarn_unlock', {'NAT-SYNC'})
-- a PREPARE that waits for its quorum (its records synchronous): its locks
-- are in the lock index, and its copy, from the step that took them
c:eval([[box.space._tarn_locks:alter{is_sync = true} box.space._tarn_part:alter{is_sync = true}
         box.cfg{replication_synchro_quorum = 2}]])
local qtx = 'NAT-' .. require('uuid').str()
local fp = c:call('tarn_prepare', {qtx, 1, {1}, {{'replace', wa, trow(5)}}, {}, {}, fiber.time(), box.NULL,
                                   box.NULL, {b}}, {is_async = true})
fiber.sleep(0.2)
check('a PREPARE that waits for its quorum', not fp:is_ready())
because('read', 'w_lock', function() n = c:call('tarn_storage.ts_select', args({sid = wa, key = {5}, limit = 1})) end)
check('... then a read of the row it writes: handed to the Lua procedure', n[1] == 'lua', n)
n = c:call('tarn_storage.ts_select', args({sid = wa, key = {1}, limit = 1}))
check('... a read of another row: served', n[1] == 'ok' and #n[2] == 1, n)
because('write', 'lock', function() n = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'replace', trow(5)}})) end)
check('... a write of the row it writes: handed to the Lua procedure', n[1] == 'lua', n)
c:eval('box.cfg{replication_synchro_quorum = 1}')
local rp = fp:wait_result(5)
check('... it is prepared once its quorum is', rp ~= nil and rp[1][1] == 'yes', rp)
c:call('tarn_abort', {qtx, 1, {1}})
c:eval('box.space._tarn_locks:alter{is_sync = false} box.space._tarn_part:alter{is_sync = false}')
c:eval('box.cfg{replication_synchro_timeout = 5}')
wa, wb = wa0, wb0
c:eval('box.space.test_native_sync_a:drop() box.space.test_native_sync_b:drop()')
-- a bucket being sent: the Lua procedure notes its writes for the move
local b2   -- another bucket this shard owns
for _, rg in ipairs(map[4]) do for x = rg[1], rg[2] do if x ~= b and b2 == nil then b2 = x end end end
if b2 ~= nil then
    r = c:call('tarn_bucket_send_begin', {b2, 2, 'NAT-M', 5})
    check('a bucket is being sent', r[1] == 'ok', r)
    wsame('a write to it', {{'insert', {48, 1, 'x', b2}}}, {wb = {b2}}, false)
    c:call('tarn_bucket_send_abort', {b2, 'NAT-M'})
    wsame('... once its move was given up', {{'insert', {49, 1, 'x', b2}}}, {wb = {b2}}, true)
end
-- a read-only node
c:eval('box.cfg{read_only = true}')
n = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'insert', trow(47)}}))
check('a write on a read-only node: handed to the Lua procedure', n[1] == 'lua', n)
local ok_ro, err_ro = pcall(c.call, c, 'tarn_autocommit', wargs(wa, {{'insert', trow(47)}}))
check('... which refuses it (ER_READONLY)', not ok_ro and err_ro.code == box.error.READONLY, tostring(err_ro))
-- (the node notices the change in the background; then the Lua procedures
-- read the state again)
c:eval("box.cfg{read_only = false} require('fiber').sleep(0.1)")
local deadline = fiber.clock() + 5
repeat
    c:call('tarn_autocommit', wargs(wb, {{'delete', {999}}}))
    n = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'delete', {999}}}))
until n[1] ~= 'lua' or fiber.clock() > deadline
wsame('once the node is writable again, a write', {{'insert', trow(47)}}, nil, true)
-- a vinyl table
n = c:call('tarn_storage.ts_autocommit', wargs(vsid, {{'insert', trow(43)}}))
check('a write to a vinyl table: handed to the Lua procedure', n[1] == 'lua', n)
-- a test hook held
c:call('tarn_test_hook', {'scan', 'hold'})
n = c:call('tarn_storage.ts_autocommit', wargs(wa, {{'insert', trow(44)}}))
check('while a test hook is held: handed to the Lua procedure', n[1] == 'lua', n)
c:call('tarn_test_hook', {'scan', 'release'})
-- the writes count for their bucket's load
local function writes_of(x)
    for _, e in ipairs(c:call('tarn_bucket_stats')[3]) do if e[1] == x then return e[3] or 0 end end
    return 0
end
local w0 = writes_of(b)
c:call('tarn_storage.ts_autocommit', wargs(wa, {{'insert', trow(45)}, {'insert', trow(46)}}))
check('the rows written count for their bucket', writes_of(b) - w0 == 2, {w0, writes_of(b)})
-- what waits for the commits in flight waits for the native ones
c:eval('tarn.ns_inflight(true)')
local t0 = fiber.clock()
r = c:call('tarn_lock', {'NAT-RC2', {{wa, 0, {1}}}, 'X', {timeout = 0.3, read = true}})
check('a row lock waits for a native commit in flight', fiber.clock() - t0 >= 0.25, {r, fiber.clock() - t0})
c:eval('tarn.ns_inflight(false)')
c:call('tarn_unlock', {'NAT-RC2'})
c:eval('tarn.ns_inflight(true)')
local fut = c:call('tarn_lock', {'NAT-RC3', {{wa, 0, {2}}}, 'X', {timeout = 2}}, {is_async = true})
fiber.sleep(0.2)
check('... a commit that began before it', not fut:is_ready())
c:eval('tarn.ns_inflight(true)')   -- (it ended, and another one began)
local r3 = fut:wait_result(1)
check('... but not one that began after it', r3 ~= nil and r3[1][1] == 'ok', r3)
c:eval('tarn.ns_inflight(false)')
c:call('tarn_unlock', {'NAT-RC3'})

-- A new election term: a stream begun before it may have missed writes of
-- another leader (TarnCommit: lost).
local function new_term()
    -- (this node owns the queue of synchronous transactions already: a
    -- promotion alone keeps the term)
    local t0 = c:eval('return box.info.election.term')
    c:eval("box.ctl.demote() box.ctl.promote() require('fiber').sleep(0.1)")
    check('a new election term', c:eval('return box.info.election.term') > t0)
    local deadline = fiber.clock() + 5
    while c:eval('return box.info.ro') and fiber.clock() < deadline do fiber.sleep(0.05) end
    -- (a Lua read with the lock check reads the lock index again: the state
    -- is fresh)
    c:call('tarn_select', args({key = {7}, limit = 1}))
end

print('commits of one shard')
-- (the twins the same again: the native procedure alone wrote some rows)
c:eval('local a, b = ... box.space[b]:truncate() for _, t in box.space[a]:pairs() do box.space[b]:replace(t) end',
       {wa, wb})
local function cstats() local i = c:call('tarn_info').native return i.commits, i.commits_to_lua end
-- tarn_commit_local's arguments for the writes `ws` ({op, payload}) to
-- space sid, after point reads of the keys `rkeys` (of space o.rsid, else
-- sid; routed by o.rb, else b) and reads of the ranges o.ranges ({space,
-- index, lo, hi, lo_incl, hi_incl, bucket}); o: lock_gen, fence (the
-- definition the writes were made with), wb (b for each write unless
-- given), mver
local function cargs(sid, ws, rkeys, o)
    o = o or {}
    local out, pts, wbs = {}, {}, {}
    for i, w in ipairs(ws) do out[i], wbs[i] = {w[1], sid, w[2]}, b end
    for i, k in ipairs(rkeys or {}) do pts[i] = {o.rsid or sid, 0, k, o.rb or b} end
    return {'NAT-C-' .. require('uuid').str(), out, pts, o.ranges or {}, o.lock_gen or box.NULL,
            o.fence and {sid, o.fence} or box.NULL, o.wb or wbs, o.mver or box.NULL}
end
-- A transaction on each twin: one that reads (rkeys) in a stream, whose
-- first read begins its transaction, as the SQL servers' do (o.no_stream:
-- without one); then o.between(). The native procedure commits test.wa's:
-- it must reply as the Lua procedure, which commits test.wb's, the tables
-- then the same (served); or hand the commit back, having written nothing,
-- its stream's transaction as it was -- then o.after(), and the Lua
-- procedure commits it there, as it did test.wb's.
local function csame(name, ws, rkeys, o, want_served)
    o = o or {}
    local function txn(t)
        if rkeys == nil or o.no_stream then return c end
        local st = c:new_stream()
        for i, k in ipairs(rkeys) do
            local r = st:call('tarn_select', args({sid = o.rsid or t, key = k, limit = 1, lock_check = false,
                                                   begin = i == 1}))
            assert(r[1] == 'ok', json.encode(r))
        end
        return st
    end
    local sa, sb = txn(wa), txn(wb)
    if o.between then o.between() end
    local before = rows(wa)
    local s0, l0 = cstats()
    local n = sa:call('tarn_storage.ts_commit_local', cargs(wa, ws, rkeys, o))
    local s1, l1 = cstats()
    local untouched = rows(wa) == before
    if o.after then o.after() end
    local l = sb:call('tarn_commit_local', cargs(wb, ws, rkeys, o))
    if want_served then
        check(name .. ': served, as the Lua procedure serves it',
              s1 == s0 + 1 and canon(n) == canon(l) and rows(wa) == rows(wb), {native = n, lua = l})
    else
        local l2 = sa:call('tarn_commit_local', cargs(wa, ws, rkeys, o))
        -- (an error names its table)
        local alike = l2[1] == l[1] and l2[2] == l[2]
        check(name .. ': handed to the Lua procedure, nothing written',
              l1 == l0 + 1 and n[1] == 'lua' and untouched and alike and rows(wa) == rows(wb),
              {native = n, lua = l, lua_after = l2, untouched = untouched})
    end
    for _, st in ipairs({sa, sb}) do if st ~= c then pcall(st.rollback, st) end end
    return n, l
end
-- (writes that read nothing here: no stream, the commit its own transaction)
n, l = csame('a commit of rows', {{'insert', trow(70)}, {'replace', trow(71)}}, nil, nil, true)
check('... (its writes counted)', l[1] == 'ok' and l[2] == 2, l)
n, l = csame('... replaces of rows that are there, a delete, a delete of a row that is not there',
             {{'replace', trow(70, 3)}, {'replace', trow(71)}, {'delete', {70}}, {'delete', {700}}}, nil, nil, true)
check('... (the delete of a row that is not there does not count)', l[2] == 3, l)
csame('a commit in a stream, after its reads', {{'insert', trow(72)}, {'delete', {71}}}, {{1}, {72}}, nil, true)
n, l = csame('an insert of a row that is there', {{'insert', trow(1)}}, nil, nil, false)
check('... (the Lua procedure refuses it: 1062)', l[1] == 'duplicate' and l[2] == 1062, l)
csame('... after another write, in a stream', {{'insert', trow(73)}, {'insert', trow(1)}}, {{1}}, nil, false)
local function overwrite(id)
    return function()
        for _, t in ipairs({wa, wb}) do c:eval('local t, r = ... box.space[t]:replace(r)', {t, trow(id, 6)}) end
    end
end
n, l = csame('a commit whose read was overwritten since', {{'insert', trow(74)}}, {{1}}, {between = overwrite(1)},
             false)
check('... (the Lua procedure finds the conflict)', l[1] == 'conflict' and l[2] == 1213, l)
-- a unique value moved from a row to another: the Lua procedure writes the
-- row that takes it once the one that gives it up is written
local function unique_twin(name)
    return c:eval([[local name, like = ...
        local s = box.schema.space.create(name, {format = box.space[like]:format()})
        s:create_index('primary', {parts = {'id'}})
        s:create_index('bucket_id', {parts = {'bucket_id'}, unique = false})
        s:create_index('w', {parts = {{'w', is_nullable = true}}, unique = true})
        return s.id]], {name, 'test.wa'})
end
wa0, wb0 = wa, wb
wa, wb = unique_twin('test_native_u_a'), unique_twin('test_native_u_b')
local function unique_rows()
    for _, t in ipairs({wa, wb}) do
        c:eval('local t, b = ... local s = box.space[t] s:truncate() s:replace{2, 1, "r2", b, 5} s:replace{9, 1, "r9", b, 9}',
               {t, b})
    end
end
unique_rows()
local moves = {{'insert', {3, 1, 'r3', b, 30}}, {'insert', {1, 1, 'r1', b, 5}}, {'replace', {2, 1, 'r2', b, 6}}}
n, l = csame('a commit that moves a unique value to another row, in a stream', moves, {{9}}, nil, false)
check('... (which the Lua procedure commits there: the native one took back its first write)',
      l[1] == 'ok' and l[2] == 3, l)
unique_rows()
n, l = csame('... whose read is overwritten once it was handed back', moves, {{9}}, {after = overwrite(9)}, false)
check('... (its stream\'s transaction, with its read, as it was: a conflict)', l[1] == 'conflict', l)
unique_rows()
n, l = csame('... one begun before a new election term', moves, {{9}}, {between = new_term}, false)
check('... (which the Lua procedure refuses, as it may have missed writes)', l[1] == 'conflict', l)
-- ... or whose vinyl table read is emptied by a TRUNCATE then: the TRUNCATE
-- dooms it (DDL_TXN), the rollback to its savepoint notwithstanding
local vdef = c:eval('return box.space._tarn_catalog:get{...}.def', {'test.natv'})
vdef.id, vdef.ver, vdef.json = vsid, (vdef.ver or 1) + 1, '{}'
local vtrunc = {id = vsid, ver = vdef.ver, timeout = 5, def = vdef}
local function truncate_natv()
    local r1 = c:call('tarn_truncate_prepare', {'test.natv', vtrunc})
    local r2 = c:call('tarn_truncate_apply', {'test.natv', vtrunc})
    check('a TRUNCATE of the vinyl table it read, applied', r1[1] == 'ok' and r2[1] == 'ok', {r1, r2})
end
unique_rows()
n, l = csame('... one that read a vinyl table', moves, {{7}}, {rsid = vsid, after = truncate_natv}, false)
check('... (which the TRUNCATE doomed, as the Lua procedure\'s twin: a conflict)', l[1] == 'conflict', l)
-- per key, on the unique index w (2): the W locks of a prepared transaction
-- on the keys of the rows it writes (row 2: from 5 to 6; row 9: from 9 to
-- NULL), and a row lock on another key (8). (The Lua procedure waits for, or
-- refuses, what meets them: the native procedure alone is called then.)
unique_rows()
local ups = c:new_stream()
ups:begin()
local utx = 'NAT-' .. require('uuid').str()
r = ups:call('tarn_prepare', {utx, 1, {1}, {{'replace', wa, {2, 1, 'r2', b, 6}}, {'replace', wa, {9, 1, 'r9', b, box.NULL}}},
                              {}, {}, fiber.time(), box.NULL, box.NULL, {b, b}})
check('a prepared transaction holds W locks on keys of a unique index', r[1] == 'yes', r)
r = c:call('tarn_lock', {'NAT-RC5', {{wa, 2, {8}}}, 'X', {timeout = 1}})
check('... and a row lock is held on another key of it', r[1] == 'ok', r)
for _, x in ipairs({{'then a commit of a row that takes a key it holds a W lock on', {3, 1, 'r3', b, 6}},
                    {'... of one that takes the key with the row lock', {3, 1, 'r3', b, 8}},
                    {'... of a row it writes', {9, 1, 'r9', b, 10}}}) do
    n = c:call('tarn_storage.ts_commit_local', cargs(wa, {{'replace', x[2]}}))
    check(x[1] .. ': handed to the Lua procedure', n[1] == 'lua', n)
end
csame('... of one that takes NULL there (a NULL takes no key)', {{'insert', {4, 1, 'r4', b, box.NULL}}}, nil, nil,
      true)
csame('... of one that takes another key', {{'insert', {5, 1, 'r5', b, 7}}}, nil, nil, true)
c:call('tarn_unlock', {'NAT-RC5'})
c:call('tarn_abort', {utx, 1, {1}})
wa, wb = wa0, wb0
c:eval('box.space.test_native_u_a:drop() box.space.test_native_u_b:drop()')
-- what its checks leave to the Lua procedure
because('commit', 'own_row_locks', function()
    n, l = csame('a commit of a transaction with row locks here', {{'insert', trow(75)}}, nil, {lock_gen = 'x/1'}, false)
end)
check('... (which the Lua procedure refuses: they were lost)', l[1] == 'error' and l[2] == 1213, l)
csame('a commit of a transaction that read every shard with the current membership', {{'insert', trow(75)}}, nil,
      {mver = map[6] or 0}, true)
n, l = csame('... with an older one', {{'insert', trow(76)}}, nil, {mver = -1}, false)
check('... (refused)', l[1] == 'moved', l)
if other ~= nil then
    csame('a commit that writes a bucket this shard does not own', {{'insert', trow(76)}}, nil, {wb = {other}}, false)
    csame('... whose read was routed by one', {{'insert', trow(76)}}, {{1}}, {rb = other}, false)
end
csame('... that writes a row of one', {{'insert', {77, 0, 'x', other or 1000000}}}, nil, nil, false)
if b2 ~= nil then   -- (a bucket being sent: the Lua procedure notes its writes for the move)
    r = c:call('tarn_bucket_send_begin', {b2, 2, 'NAT-M2', 5})
    check('a bucket is being sent', r[1] == 'ok', r)
    csame('a commit that writes a row of it', {{'insert', {91, 1, 'x', b2}}}, nil, {wb = {b2}}, false)
    c:call('tarn_bucket_send_abort', {b2, 'NAT-M2'})
    csame('... once its move was given up', {{'insert', {92, 1, 'x', b2}}}, nil, {wb = {b2}}, true)
end
local cps = c:new_stream()
cps:begin()
local ctx = 'NAT-' .. require('uuid').str()
r = cps:call('tarn_prepare', {ctx, 1, {1}, {{'replace', sid, {20, 1, 'x', b}}}, {}, {}, fiber.time(), box.NULL,
                              box.NULL, {b}})
check('a prepared transaction holds a lock on test.nat', r[1] == 'yes', r)
csame('then a commit that read another row of it', {{'insert', trow(78)}}, {{7}}, {rsid = sid}, true)
because('commit', 'w_lock', function()
    n, l = csame('... one that read the row it writes', {{'insert', trow(79)}}, {{20}}, {rsid = sid}, false)
end)
check('... (which the Lua procedure refuses)', l[1] == 'conflict' and l[2] == 1213, l)
-- (ranges read: compared with the W-locked keys of their index)
local function rr(iid, lo, hi, lo_incl) return {{sid, iid, lo, hi, lo_incl ~= false, true, b}} end
csame('... one that read a range of it short of the row', {{'insert', trow(93)}}, {{7}},
      {rsid = sid, ranges = rr(0, {1}, {19})}, true)
csame('... one with no lower bound', {{'insert', trow(94)}}, {{7}}, {rsid = sid, ranges = rr(0, box.NULL, {19})},
      true)
csame('... one with no upper bound, past the row', {{'insert', trow(95)}}, {{7}},
      {rsid = sid, ranges = rr(0, {21}, box.NULL)}, true)
csame('... one of the keys of index v between those the row had and takes', {{'insert', trow(96)}}, {{7}},
      {rsid = sid, ranges = rr(2, {2}, {5})}, true)
for _, x in ipairs({{'... one that read a range that holds the row', rr(0, {15}, {25})},
                    {'... one with no bounds', rr(0, box.NULL, box.NULL)},
                    {'... one of keys of index v that holds the key the row takes', rr(2, {1}, {1})},
                    {'... one of keys of index vid that hold it, by a prefix', rr(3, {6}, {6})}}) do
    n, l = csame(x[1], {{'insert', trow(97)}}, {{7}}, {rsid = sid, ranges = x[2]}, false)
    check('... (which the Lua procedure refuses)', l[1] == 'conflict' and l[2] == 1213, l)
end
n, l = csame('... one whose range excludes the row, its bound', {{'insert', trow(97)}}, {{7}},
             {rsid = sid, ranges = rr(0, {20}, {25}, false)}, false)
check('... (handed back: the bounds are taken as included; the Lua procedure commits it)', l[1] == 'ok', l)
c:call('tarn_abort', {ctx, 1, {1}})
csame('once it ended', {{'insert', trow(79)}}, {{20}}, {rsid = sid}, true)
r = c:call('tarn_lock', {'NAT-RC4', {{wa, 0, {9}}}, 'X', {timeout = 1}})
check('a row lock is held on a row of the table', r[1] == 'ok', r)
n = c:call('tarn_storage.ts_commit_local', cargs(wa, {{'replace', trow(9)}}))
check('then a commit that writes that row: handed to the Lua procedure', n[1] == 'lua', n)
csame('... one that writes another row', {{'insert', trow(80)}}, nil, nil, true)
c:call('tarn_unlock', {'NAT-RC4'})
c:eval("box.space._tarn_meta:replace{'fence:' .. ..., {ver = 7}} tarn.stale_fences()", {wa})
c:eval("box.space._tarn_meta:replace{'fence:' .. ..., {ver = 7}} tarn.stale_fences()", {wb})
n, l = csame('a commit with an older definition than the fence', {{'insert', trow(81)}}, nil, {fence = 6}, false)
check('... (refused: 1412)', l[2] == 1412, l)
csame('... again, with the fences read', {{'insert', trow(81)}}, nil, {fence = 6}, false)
csame('... and one with the current definition', {{'insert', trow(81)}}, nil, {fence = 7}, true)
c:eval("box.space._tarn_meta:delete{'fence:' .. ...} tarn.stale_fences()", {wa})
c:eval("box.space._tarn_meta:delete{'fence:' .. ...} tarn.stale_fences()", {wb})
c:call('tarn_commit_local', cargs(wb, {{'delete', {81}}}))   -- (the fences: read again)
c:call('tarn_commit_local', cargs(wa, {{'delete', {81}}}))
c:eval('box.space._tarn_meta:replace{..., {op = "TRUNCATE", ver = 0, label = "NAT-HOLD"}} tarn.stale_fences()', {hold})
c:call('tarn_select', args({key = {7}, limit = 1}))
because('commit', 'held_table', function()
    csame('a commit while a TRUNCATE or DROP TABLE holds a table', {{'insert', trow(82)}}, nil, nil, false)
end)
c:eval('box.space._tarn_meta:delete{...} tarn.stale_fences()', {hold})
c:call('tarn_select', args({key = {7}, limit = 1}))
csame('... once it ends', {{'insert', trow(83)}}, nil, nil, true)
-- a TRUNCATE applied while transactions are open: they fail if they read
-- its table (the Lua procedure checks)
c:call('tarn_create_table', {def('test.ntr', 'memtx', 28005)})
local tdef = c:eval('return box.space._tarn_catalog:get{...}.def', {'test.ntr'})
local tid = c:eval('return box.space[...].id', {'test.ntr'})
tdef.id, tdef.ver, tdef.json = tid, 2, '{}'
local ta = {id = tid, ver = 2, timeout = 5, def = tdef}
local function truncate()
    local r1 = c:call('tarn_truncate_prepare', {'test.ntr', ta})
    local r2 = c:call('tarn_truncate_apply', {'test.ntr', ta})
    check('a TRUNCATE of another table, applied', r1[1] == 'ok' and r2[1] == 'ok', {r1, r2})
end
n, l = csame('a commit of a transaction that was open then', {{'insert', trow(84)}}, {{1}}, {between = truncate},
             false)
check('... (which the Lua procedure commits: it did not read that table)', l[1] == 'ok', l)
csame('... once those transactions ended', {{'insert', trow(85)}}, {{1}}, nil, true)
c:call('tarn_drop_table', {'test.ntr'})
n, l = csame('a commit that read rows here, without a stream', {{'insert', trow(86)}}, {{1}}, {no_stream = true},
             false)
check('... (which the Lua procedure refuses: those reads cannot be validated)', l[1] == 'conflict', l)
n = c:call('tarn_storage.ts_commit_local', cargs(vsid, {{'insert', trow(87)}}))
check('a commit that writes a vinyl table: handed to the Lua procedure', n[1] == 'lua', n)
c:eval('box.cfg{read_only = true}')
n = c:call('tarn_storage.ts_commit_local', cargs(wa, {{'insert', trow(87)}}))
check('a commit on a read-only node: handed to the Lua procedure', n[1] == 'lua', n)
c:eval("box.cfg{read_only = false} require('fiber').sleep(0.1)")
deadline = fiber.clock() + 5
repeat
    c:call('tarn_commit_local', cargs(wb, {{'delete', {999}}}))
    n = c:call('tarn_storage.ts_commit_local', cargs(wa, {{'delete', {999}}}))
until n[1] ~= 'lua' or fiber.clock() > deadline
csame('once the node is writable again, a commit', {{'insert', trow(87)}}, nil, nil, true)
w0 = writes_of(b)
c:call('tarn_storage.ts_commit_local', cargs(wa, {{'insert', trow(88)}, {'insert', trow(89)}}))
check('the rows a commit wrote count for their bucket', writes_of(b) - w0 == 2, {w0, writes_of(b)})
c:call('tarn_drop_table', {'test.wa'})
c:call('tarn_drop_table', {'test.wb'})

print('validations of read-only streams')
do
local function vstats() local i = c:call('tarn_info').native return i.validations, i.validations_to_lua end
-- tarn_validate's arguments, after point reads of the keys `rkeys` of
-- test.nat (o.rsid; routed by o.rb, else b) and reads of the ranges
-- o.ranges; o: lock_gen, mver
local function vargs(rkeys, o)
    local pts = {}
    for i, k in ipairs(rkeys) do pts[i] = {o.rsid or sid, 0, k, o.rb or b} end
    return {'NAT-V-' .. require('uuid').str(), pts, o.ranges or {}, false, o.lock_gen or box.NULL,
            o.mver or box.NULL}
end
-- A read-only transaction on each of two streams: its reads (rkeys; the
-- first begins its transaction, natively with o.native_begin; o.no_stream:
-- none), then o.between(). The native procedure validates the first: its
-- reply must be the Lua procedure's for the second (served); or it hands it
-- back, having changed nothing, and the Lua procedure validates it, as it
-- did the second.
local function vsame(name, rkeys, o, want_served)
    o = o or {}
    local function txn()
        if o.no_stream then return c end
        local st = c:new_stream()
        for i, k in ipairs(rkeys) do
            local r = st:call(o.native_begin and 'tarn_storage.ts_select' or 'tarn_select',
                              args({sid = o.rsid or sid, key = k, limit = 1, lock_check = false, begin = i == 1}))
            assert(r[1] == 'ok', json.encode(r))
        end
        return st
    end
    local sa, sb = txn(), txn()
    if o.between then o.between() end
    local s0, l0 = vstats()
    local n = sa:call('tarn_storage.ts_validate', vargs(rkeys, o))
    local s1, l1 = vstats()
    local l = sb:call('tarn_validate', vargs(rkeys, o))
    if want_served then
        check(name .. ': served, as the Lua procedure serves it', s1 == s0 + 1 and canon(n) == canon(l),
              {native = n, lua = l})
    else
        local l2 = sa:call('tarn_validate', vargs(rkeys, o))
        check(name .. ': handed to the Lua procedure', l1 == l0 + 1 and n[1] == 'lua' and l2[1] == l[1] and l2[2] == l[2],
              {native = n, lua = l, lua_after = l2})
        l = l2
    end
    for _, st in ipairs({sa, sb}) do if st ~= c then pcall(st.rollback, st) end end
    return n, l
end
local function overwrite7() c.space['test.nat']:replace{7, 100, 'changed', b} end
vsame('a read-only stream', {{7}, {8}}, nil, true)
vsame('... whose transaction a native read began', {{7}, {8}}, {native_begin = true}, true)
local n, l = vsame('one whose read was overwritten since', {{7}}, {between = overwrite7}, true)
check('... (a conflict, its probe write refused)', l[1] == 'conflict' and l[2] == 1213, l)
c.space['test.nat']:replace(row(7))
n, l = vsame('one with row locks here', {{7}}, {lock_gen = 'x/1'}, false)
check('... (which the Lua procedure refuses: they were lost)', l[1] == 'error' and l[2] == 1213, l)
n, l = vsame('one that read every shard with an older membership', {{7}}, {mver = -1}, false)
check('... (refused)', l[1] == 'moved', l)
if other ~= nil then
    vsame('one whose read was routed by a bucket this shard does not own', {{7}}, {rb = other}, false)
end
because('validate', 'stream', function() n, l = vsame('reads without a stream', {{7}}, {no_stream = true}, false) end)
check('... (which the Lua procedure refuses)', l[1] == 'conflict', l)
local vps = c:new_stream()
vps:begin()
local vtx = 'NAT-' .. require('uuid').str()
local r = vps:call('tarn_prepare', {vtx, 1, {1}, {{'replace', sid, {20, 1, 'x', b}}}, {}, {}, fiber.time(), box.NULL,
                                    box.NULL, {b}})
check('a prepared transaction holds a lock on test.nat', r[1] == 'yes', r)
vsame('then one that read another row of it', {{7}}, nil, true)
n, l = vsame('... one that read the row it writes', {{8}, {20}}, nil, false)
check('... (which the Lua procedure refuses)', l[1] == 'conflict' and l[2] == 1213, l)
local function rr(iid, lo, hi) return {{sid, iid, lo, hi, true, true, b}} end
vsame('... one that read a range short of it', {{7}}, {ranges = rr(0, {1}, {19})}, true)
vsame('... one that read a range of index padci without its keys', {{7}}, {ranges = rr(5, {'Q'}, {'W'})}, true)
for _, x in ipairs({{'... one that read a range that holds it', rr(0, {19}, box.NULL)},
                    {'... one that read the key of index padci it had, in capitals',
                     rr(5, {string.rep('P', 20)}, {string.rep('P', 20)})}}) do
    n, l = vsame(x[1], {{7}}, {ranges = x[2]}, false)
    check('... (which the Lua procedure refuses)', l[1] == 'conflict' and l[2] == 1213, l)
end
c:call('tarn_abort', {vtx, 1, {1}})
vsame('once it ended', {{8}, {20}}, nil, true)
-- a TRUNCATE applied while the streams were open: they fail if they read
-- its table (the Lua procedure checks)
c:call('tarn_create_table', {def('test.ntr2', 'memtx', 28006)})
local tdef = c:eval('return box.space._tarn_catalog:get{...}.def', {'test.ntr2'})
local tid = c:eval('return box.space[...].id', {'test.ntr2'})
tdef.id, tdef.ver, tdef.json = tid, 2, '{}'
local function truncate()
    local t = {id = tid, ver = 2, timeout = 5, def = tdef}
    local r1, r2 = c:call('tarn_truncate_prepare', {'test.ntr2', t}), c:call('tarn_truncate_apply', {'test.ntr2', t})
    check('a TRUNCATE of another table, applied', r1[1] == 'ok' and r2[1] == 'ok', {r1, r2})
end
n, l = vsame('one open when a TRUNCATE was applied', {{7}}, {between = truncate}, false)
check('... (which the Lua procedure validates: it did not read that table)', l[1] == 'ok', l)
vsame('one begun after it', {{7}}, nil, true)
c:call('tarn_drop_table', {'test.ntr2'})
because('validate', 'term', function()
    n, l = vsame('one begun before a new election term', {{7}}, {between = new_term}, false)
end)
check('... (which the Lua procedure refuses)', l[1] == 'conflict', l)
vsame('one begun in the new term', {{7}}, nil, true)
-- a table held by a TRUNCATE or DROP TABLE (its hold, as their PREPARE
-- stores it), and the held tables to be read again (a restart, a new
-- leader): tarn_validate reads them, and then the native procedure serves
c:eval('box.space._tarn_meta:replace{..., {op = "TRUNCATE", ver = 0, label = "NAT-HOLD"}} tarn.stale_fences()', {hold})
vsame('one while the held tables are to be read again', {{7}}, nil, false)
vsame('... while a TRUNCATE or DROP TABLE holds a table', {{7}}, nil, false)
c:eval('box.space._tarn_meta:delete{...} tarn.stale_fences()', {hold})
vsame('... once it ends, the held tables to be read again', {{7}}, nil, false)
vsame('... which the Lua procedure read: then', {{7}}, nil, true)
local ro = c:new_stream()
ro:call('tarn_select', args({key = {7}, limit = 1, lock_check = false, begin = true}))
c:eval('box.cfg{read_only = true}')
n = ro:call('tarn_storage.ts_validate', vargs({{7}}, {}))
check('a validation on a read-only node: handed to the Lua procedure', n[1] == 'lua', n)
local ok_ro, err_ro = pcall(ro.call, ro, 'tarn_validate', vargs({{7}}, {}))
check('... which refuses it (ER_READONLY)', not ok_ro and err_ro.code == box.error.READONLY, tostring(err_ro))
pcall(ro.rollback, ro)
c:eval("box.cfg{read_only = false} require('fiber').sleep(0.1)")
local deadline = fiber.clock() + 5
repeat
    c:call('tarn_select', args({key = {7}, limit = 1}))
    local st = c:new_stream()
    st:call('tarn_select', args({key = {7}, limit = 1, lock_check = false, begin = true}))
    n = st:call('tarn_storage.ts_validate', vargs({{7}}, {}))
    pcall(st.rollback, st)
until n[1] ~= 'lua' or fiber.clock() > deadline
vsame('once the node is writable again, a validation', {{7}}, nil, true)
end

print('a module of another build')
-- (its state's layout is not the Lua module's: it fails as a module that
-- does not load, ER_LOAD_FUNCTION, and the SQL servers stop calling it)
local layout = c:eval("return require('ffi').cast('struct tarn_ns *', box.func['tarn_storage.ts_state']:call()).layout")
c:eval("box.func['tarn_storage.ts_state']:call({...})", {layout + 1})
local ok, err = pcall(c.call, c, 'tarn_storage.ts_select', args({key = {7}, limit = 1}))
check('its reads fail (99)', not ok and err.code == 99, tostring(err))
ok, err = pcall(c.call, c, 'tarn_storage.ts_commit_local', {'NAT-X', {}, {}, {}, box.NULL, box.NULL, {}, box.NULL})
check('... and its commits', not ok and err.code == 99, tostring(err))
ok, err = pcall(c.call, c, 'tarn_storage.ts_validate', {'NAT-X', {}, {}, false, box.NULL, box.NULL})
check('... and its validations', not ok and err.code == 99, tostring(err))
c:eval("box.func['tarn_storage.ts_state']:call({...})", {layout})
same('the same layout again: served', {key = {7}, limit = 1}, true)

c:call('tarn_drop_table', {'test.nat'})
c:call('tarn_drop_table', {'test.natv'})
-- (with cfg.check_locks, as lua/test/run.sh starts the shards: the copy of
-- the lock index held what the index held after each change)
if c:eval('return tarn.cfg.check_locks') then
    local e = c:call('tarn_info').stats.lock_copy_errors
    check('the copy of the lock index held what the index held', e == 0, e)
end
print(string.format('%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
