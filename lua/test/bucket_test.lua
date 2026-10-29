-- Protocol streams start with the first-read quorum contract. Raw IPROTO
-- BEGIN refusal and unchanged ambient read sets are tested separately.
-- Bucket moves (issue #24): the bucket map, the refusals of requests routed
-- by buckets a shard does not own, and the steps of a move, on two
-- single-node shards. Run via lua/test/run.sh (after storage_test.lua, on
-- the same shards: this file uses its own table and buckets).
local net = require('net.box')
local fiber = require('fiber')
local json = require('json')

local S1, S2 = os.getenv('S1'), os.getenv('S2')
local opts = {user = 'tarn', password = 'tarn', wait_connected = 5}
local c = {net.connect(S1, opts), net.connect(S2, opts)}
assert(c[1]:is_connected() and c[2]:is_connected(), 'cannot connect to shards')

local pass, fail = 0, 0
local function check(name, cond, extra)
    if cond then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. (extra and ('  -- ' .. json.encode(extra)) or '')) end
end
local function wait_for(cond, secs)
    local deadline = fiber.clock() + (secs or 5)
    while fiber.clock() < deadline do
        if cond() then return true end
        fiber.sleep(0.05)
    end
    return cond()
end
local function ready(i, required)
    assert(wait_for(function()
        return c[i]:eval([[local ids = ...
            return not box.info.ro and tarn.clock.read_check() == nil
                and (ids == nil or tarn.clock.check(ids) == nil)]], {required})
    end, 10), 'shard ' .. i .. ' did not become ready')
end
for i = 1, 2 do ready(i, {i}) end
local function cfg(i, k, v)
    return c[i]:eval('local t = package.loaded.tarn.cfg local o = t[...] t[...] = select(2, ...) return o', {k, v})
end

-- test.bk(id, v, k, bucket_id) on both shards; the bucket of a row is
-- whatever the test says it is (the shards never compute one).
local def = {
    name = 'test.bk', engine = 'memtx', id = 24024,   -- (the same space id on both shards)
    format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
              {name = 'k', type = 'string'}, {name = 'bucket_id', type = 'unsigned'}},
    indexes = {{name = 'primary', unique = true, parts = {{field = 'id', type = 'unsigned'}}},
               {name = 'k', unique = false, parts = {{field = 'k', type = 'string'}}},
               {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}}},
}
for i = 1, 2 do c[i]:call('tarn_create_table', {def}) end
c[1]:reload_schema(); c[2]:reload_schema()
local sid = c[1].space['test.bk'].id
assert(sid == c[2].space['test.bk'].id, 'space ids must match across shards for this test')

local function rows_of(i, b)
    return c[i]:eval('local s, b = ... local n = 0 for _ in box.space[s].index.bucket_id:pairs({b}) do n = n + 1 end return n',
                     {'test.bk', b})
end
local function get(i, id) return c[i].space['test.bk']:get{id} end
local function sel(i, id, route)
    return c[i]:call('tarn_select', {sid, 0, {id}, 'EQ', 1, box.NULL, true, true, box.NULL, false, box.NULL, route})
end
local function upd(i, id, b, v)
    return c[i]:call('tarn_autocommit', {{{'update', sid, {id}, {{'=', 2, v}}}}, {wb = {b}}})
end
local function prepare(i, txid, writes, rpoints, rranges, wb, stream)
    local s = stream or c[i]:new_stream()
    if not stream then s:eval('tarn.fresh.begin_stream()') end
    return s:call('tarn_prepare', {txid, i, {i}, writes, rpoints or {}, rranges or {}, fiber.time(), box.NULL,
                                   box.NULL, wb})
end

-- Rows: ids 1..10 in bucket 5, 11..20 in bucket 6, 41..45 in bucket 7 and
-- 51..55 in bucket 8 on shard 1; 21..30 in bucket 12 on shard 2.
for id = 1, 10 do c[1].space['test.bk']:replace{id, id, 'a' .. id, 5} end
for id = 11, 20 do c[1].space['test.bk']:replace{id, id, 'b' .. id, 6} end
for id = 41, 45 do c[1].space['test.bk']:replace{id, id, 'c' .. id, 7} end
for id = 51, 55 do c[1].space['test.bk']:replace{id, id, 'd' .. id, 8} end
for id = 21, 30 do c[2].space['test.bk']:replace{id, id, 'e' .. id, 12} end

print('the bucket map')
local r = c[1]:call('tarn_select', {sid, 0, {21}, 'EQ', 1, box.NULL, true, true, box.NULL, false, box.NULL, 12})
check('a shard without a map refuses nothing', r[1] == 'ok', r)
r = c[1]:call('tarn_buckets')
check('... and has none; it reports its tables (a server then keeps the count their rows use)',
      r[1] == 'ok' and r[2] == 0 and type(r[3]) == 'number' and r[3] >= 1, r)
r = c[1]:call('tarn_bucket_init', {16, {{1, 8}}})
-- Two servers make shard 2's map at once, with different counts: one map
-- is made, and the other server learns its count.
local f1 = c[2]:call('tarn_bucket_init', {16, {{9, 16}}}, {is_async = true})
local f2 = c[2]:call('tarn_bucket_init', {32, {{9, 32}}}, {is_async = true})
local r2, r3 = f1:wait_result(5)[1], f2:wait_result(5)[1]   -- (a future returns its results as a list)
check('16 buckets: shard 1 owns 1..8, shard 2 owns 9..16', r[1] == 'ok' and r2[1] == 'ok', {r, r2})
check('... a map made at the same moment with 32 is not made, and says 16', r3[1] == 'exists' and r3[2] == 16, r3)
r = c[2]:call('tarn_buckets')
check('... nor any of its buckets', r[2] == 16 and #r[4] == 1 and r[4][1][1] == 9 and r[4][1][2] == 16
      and c[2]:eval('return box.space._tarn_buckets:count()') == 8, r)
r = c[1]:call('tarn_bucket_init', {16, {{1, 16}}})
check('a second map is not made', r[1] == 'exists' and r[2] == 16, r)
r = c[1]:call('tarn_buckets')
check('tarn_buckets: count, epoch, owned ranges', r[1] == 'ok' and r[2] == 16 and r[3] == 1 and #r[4] == 1
      and r[4][1][1] == 1 and r[4][1][2] == 8 and #r[5] == 0, r)
check('tarn_info counts them', c[2]:call('tarn_info').buckets == 8 and c[2]:call('tarn_info').bucket_epoch == 1)

print('requests routed by buckets the shard does not own')
r = c[1]:call('tarn_autocommit', {{{'insert', sid, {100, 0, 'x', 12}}}, {wb = {12}}})
check('autocommit write: refused (moved, 1213)', r[1] == 'moved' and r[2] == 1213, r)
check('... and nothing written', get(1, 100) == nil)
r = c[1]:call('tarn_autocommit', {{{'insert', sid, {100, 0, 'x', 5}}}, {wb = {5}}})
check('... for a bucket it owns: written', r[1] == 'ok' and get(1, 100) ~= nil, r)
c[1]:call('tarn_autocommit', {{{'delete', sid, {100}}}, {wb = {5}}})
r = c[1]:call('tarn_autocommit', {{{'insert', sid, {100, 0, 'x', 5}}}})
check('a write that names no bucket (an older SQL server): served', r[1] == 'ok', r)
c[1]:call('tarn_autocommit', {{{'delete', sid, {100}}}})
r = c[1]:call('tarn_autocommit', {{{'insert', sid, {100, 0, 'x', 12}}}})
check('... unless the row\'s bucket_id is of a bucket the shard does not own', r[1] == 'moved' and get(1, 100) == nil, r)
r = sel(1, 21, 12)
check('read: refused, naming no shard to go to', r[1] == 'moved' and r[2] == 1213 and r[4] == nil, r)
r = sel(1, 1, 5)
check('... by a bucket it owns: served', r[1] == 'ok' and #r[2] == 1, r)
r = c[1]:call('tarn_select', {sid, 0, {}, 'ALL', box.NULL, box.NULL, true, true, box.NULL, false, box.NULL, {e = 1}})
check('a read of every shard at the epoch of the map: served', r[1] == 'ok' and #r[2] >= 30, r[1])
r = c[1]:call('tarn_select', {sid, 0, {}, 'ALL', box.NULL, box.NULL, true, true, box.NULL, false, box.NULL, {e = 7}})
check('... at another epoch: refused', r[1] == 'moved', r)
r = c[1]:call('tarn_select_keys', {sid, 0, {{1}, {21}}, box.NULL, true, false, box.NULL, {5, 12}})
check('multi-key read naming a bucket it does not own: refused', r[1] == 'moved', r)
r = c[1]:call('tarn_select_keys', {sid, 0, {{1}, {2}}, box.NULL, true, false, box.NULL, {5, 5}})
check('... naming its own: served', r[1] == 'ok' and #r[2] == 2, r)
r = c[1]:call('tarn_count', {sid, 0, {}, 'ALL', box.NULL, true, {}, {}, true, false, 12})
check('count: refused', r[1] == 'moved', r)
r = c[1]:call('tarn_reduce', {sid, 0, {}, 'ALL', box.NULL, true, {}, {}, true, false, 12, {{'sum', 1, box.NULL}}})
check('typed reduction: refused on the wrong owner', r[1] == 'moved', r)
r = c[1]:call('tarn_select_projected', {sid, 0, {}, 'ALL', box.NULL, box.NULL, true,
    true, box.NULL, false, box.NULL, 12, {{1, box.NULL}}, {}, {{1, box.NULL}}})
check('typed projection: refused on the wrong owner', r[1] == 'moved', r)
r = c[1]:call('tarn_count', {sid, 0, {}, 'ALL', box.NULL, true, {}, {}, true, false, {e = 1}})
check('... at the epoch: served', r[1] == 'ok', r)
local st = c[1]:new_stream(); st:eval('tarn.fresh.begin_stream()')
r = st:call('tarn_commit_local', {'BK1', {{'replace', sid, {100, 0, 'x', 12}}}, {}, {}, box.NULL, box.NULL, {12}})
check('commit_local writing it: refused', r[1] == 'moved', r)
st = c[1]:new_stream(); st:eval('tarn.fresh.begin_stream()')
st.space['test.bk']:get{1}
r = st:call('tarn_commit_local', {'BK2', {{'replace', sid, {1, 1, 'a1', 5}}}, {{sid, 0, {21}, 12}}, {}, box.NULL,
                                  box.NULL, {5}})
check('commit_local whose read was routed by it: refused', r[1] == 'moved', r)
r = prepare(1, 'BK3', {{'replace', sid, {100, 0, 'x', 12}}}, {}, {}, {12})
check('PREPARE writing it: refused (a NO vote)', r[1] == 'moved' and r[2] == 1213, r)
check('... that leaves a tombstone', c[1]:call('tarn_part_status', {'BK3'}) == 'aborted')
st = c[1]:new_stream(); st:eval('tarn.fresh.begin_stream()')
st.space['test.bk']:get{1}
r = st:call('tarn_validate', {'BK4', {{sid, 0, {1}, 5}, {sid, 0, {21}, 12}}, {}, true, box.NULL})
check('validation of a read routed by it: refused', r[1] == 'moved', r)
r = c[1]:call('tarn_lock', {'BK5', {{sid, 0, {21}, 12}}, 'X', {}})
check('row lock: refused', r[1] == 'moved', r)
r = c[1]:call('tarn_lock_range', {'BK6', {{sid, {21}, 1, {}, 12}}, 'X', {}})
check('locking range read: refused', r[1] == 'moved', r)
r = c[1]:call('tarn_commit_rc', {'BK7', {{'replace', sid, {100, 0, 'x', 12}}}, box.NULL, box.NULL, {12}})
check('READ COMMITTED commit writing it: refused', r[1] == 'moved' and get(1, 100) == nil, r)
check('no locks left behind', c[1]:call('tarn_info').locks == 0 and c[1]:call('tarn_info').row_locks == 0)

-- Copy bucket b from shard `from` to shard `to` (move `move`), a page at a
-- time: the number of rows of test.bk (the rows of the bucket in the tables
-- of storage_test.lua move too), or nil and the failed reply.
local function copy(b, from, to, move)
    local after, n, pages = box.NULL, 0, 0
    repeat
        local p = c[from]:call('tarn_bucket_rows', {b, after})
        if p[1] ~= 'ok' then return nil, p end
        local q = c[to]:call('tarn_bucket_recv_rows', {b, move, p[2]})
        if q[1] ~= 'ok' then return nil, q end
        for _, row in ipairs(p[2]) do if row[1] == sid then n = n + 1 end end
        pages, after = pages + 1, p[3]
    until after == nil
    return n, pages
end
-- The last copy: the rows written since `gen`.
local function delta(b, from, to, move, gen)
    local i, n = 1, 0
    repeat
        local p = c[from]:call('tarn_bucket_delta', {b, move, gen, i})
        if p[1] ~= 'ok' then return nil, p end
        local q = c[to]:call('tarn_bucket_recv_rows', {b, move, p[2]})
        if q[1] ~= 'ok' then return nil, q end
        n, i = n + #p[2], p[3]
    until i == nil
    return n
end
local function staged(i, b)
    return c[i]:eval('local b, s = ... local n = 0 for _ in box.space._tarn_recv:pairs({b, s}) do n = n + 1 end return n',
                     {b, sid})
end

print('a move: bucket 5 from shard 1 to shard 2')
r = c[2]:call('tarn_bucket_recv_begin', {5, 1, 'M1'})
check('R: receiving', r[1] == 'ok', r)
check('... again (a repeated step): the same', c[2]:call('tarn_bucket_recv_begin', {5, 1, 'M1'})[1] == 'ok')
r = c[2]:call('tarn_bucket_recv_begin', {5, 1, 'M2'})
check('... another move of it: refused', r[1] == 'error', r)
r = c[2]:call('tarn_bucket_recv_begin', {12, 1, 'M2'})
check('receiving a bucket it owns: refused', r[1] == 'error', r)
r = c[1]:call('tarn_bucket_send_begin', {5, 2, 'M1', 5})
check('S: sending, with the generation of its notes', r[1] == 'ok' and type(r[2]) == 'string', r)
local gen = r[2]
r = c[1]:call('tarn_bucket_send_begin', {5, 2, 'M2', 5})
check('... for another move: refused', r[1] == 'error', r)
check('S serves writes while it sends', upd(1, 1, 5, 101)[1] == 'ok')
local old = cfg(1, 'scan_page_rows', 3)
local n, pages = copy(5, 1, 2, 'M1')
cfg(1, 'scan_page_rows', old)
check('copy: the rows of the bucket, in pages', n == 10 and pages == 4, {n, pages})
check('R keeps them aside, where nothing reads them', staged(2, 5) == 10 and get(2, 1) == nil and rows_of(2, 5) == 0)
check('R refuses requests for it meanwhile', sel(2, 1, 5)[1] == 'moved')
-- Writes after the copy: in the last copy.
check('an update after the copy', upd(1, 2, 5, 77)[1] == 'ok')
check('a delete', c[1]:call('tarn_autocommit', {{{'delete', sid, {3}}}, {wb = {5}}})[1] == 'ok')
check('an insert', c[1]:call('tarn_autocommit', {{{'insert', sid, {31, 0, 'n', 5}}}, {wb = {5}}})[1] == 'ok')
r = c[2]:call('tarn_bucket_hold', {5, 'M1'})
check('R holds it', r[1] == 'ok', r)
-- The hold: a read of every shard that covers a row of the bucket fails
-- its validation on R; a read routed by another bucket does not.
st = c[2]:new_stream()
st:call('tarn_select', {sid, 0, {}, 'ALL', box.NULL, box.NULL, false, true, box.NULL, true})
r = st:call('tarn_validate', {'BK8', {}, {{sid, 0, box.NULL, box.NULL, true, true}}, true, box.NULL})
check('a scan of R covering the held bucket fails validation', r[1] == 'conflict', r)
st = c[2]:new_stream()
st:call('tarn_select', {sid, 1, {'a1'}, 'EQ', box.NULL, box.NULL, false, true, box.NULL, true})
r = st:call('tarn_validate', {'BK9', {}, {{sid, 1, {'a1'}, {'a1'}, true, true}}, true, box.NULL})
check('... by a secondary index too', r[1] == 'conflict', r)
st = c[2]:new_stream()
st:call('tarn_select', {sid, 1, {'zz'}, 'EQ', box.NULL, box.NULL, false, true, box.NULL, true})
r = st:call('tarn_validate', {'BK10', {}, {{sid, 1, {'zz'}, {'zz'}, true, true}}, true, box.NULL})
check('... one that covers none of its rows validates', r[1] == 'ok', r)
st = c[2]:new_stream()
st:call('tarn_select', {sid, 0, {21}, 'EQ', 1, box.NULL, false, true, box.NULL, true})
r = st:call('tarn_validate', {'BK11', {{sid, 0, {21}, 12}}, {}, true, box.NULL})
check('... and a read routed by another bucket validates', r[1] == 'ok', r)
r = c[1]:call('tarn_bucket_fence', {5, 'M1', 5})
check('S: fenced and drained', r[1] == 'ok', r)
r = upd(1, 1, 5, 102)
check('a fenced bucket takes no writes', r[1] == 'moved' and r[3]:find('moving') ~= nil and r[4] == 2, r)
r = c[1]:call('tarn_autocommit', {{{'update', sid, {1}, {{'=', 2, 102}}}}})
check('... not even from a request that names no bucket (the row\'s bucket_id says)', r[1] == 'moved', r)
r = c[1]:call('tarn_autocommit', {{{'insert', sid, {32, 0, 'n', 5}}}})
check('... an insert of a row of it either', r[1] == 'moved' and get(1, 32) == nil, r)
r = sel(1, 1, 5)
check('... but serves reads', r[1] == 'ok' and #r[2] == 1, r)
r = c[1]:call('tarn_lock', {'BK12', {{sid, 0, {1}, 5}}, 'S', {}})
check('... and takes no row locks', r[1] == 'moved', r)
r = prepare(1, 'BK13', {}, {{sid, 0, {1}, 5}}, {}, {})
check('... nor a PREPARE that reads it', r[1] == 'moved', r)
st = c[1]:new_stream(); st:eval('tarn.fresh.begin_stream()')
st.space['test.bk']:get{1}
r = st:call('tarn_validate', {'BK14', {{sid, 0, {1}, 5}}, {}, true, box.NULL})
check('a read-only validation of a read of it succeeds', r[1] == 'ok', r)
n = delta(5, 1, 2, 'M1', gen)
-- (1, 2, 3 and 31, and 32, whose insert failed: the last copy reads each
-- noted row again, and sends a delete for one that is not there)
check('the last copy: the rows written since the copy began', n == 5, n)
r = c[1]:call('tarn_bucket_delta', {5, 'M1', 'another generation', 1})
check('... the notes of another generation are lost', r[1] == 'lost', r)
check('R has the rows as S has them', staged(2, 5) == 10, staged(2, 5))
r = c[1]:call('tarn_bucket_send', {5, 'M1'})
check('S sent it: its rows are gone', r[1] == 'ok' and rows_of(1, 5) == 0, r)
check('... the epoch moved', c[1]:call('tarn_buckets')[3] == 2)
r = c[1]:call('tarn_bucket_send', {5, 'M1'})
check('... sending again: done already', r[1] == 'ok' and r[2] == 'sent', r)
r = sel(1, 1, 5)
check('S refuses it, naming the shard it went to', r[1] == 'moved' and r[4] == 2 and r[3]:find('moved') ~= nil, r)
check('R refuses it until it receives it', sel(2, 1, 5)[1] == 'moved')
r = c[1]:call('tarn_bucket_send_abort', {5, 'M1'})
check('a bucket sent cannot be given up', r[1] == 'error', r)
r = c[2]:call('tarn_bucket_recv', {5, 'M1'})
check('R took it over', r[1] == 'ok' and rows_of(2, 5) == 10 and staged(2, 5) == 0, r)
check('... with every write: the updates, the delete and the insert',
      get(2, 1)[2] == 101 and get(2, 2)[2] == 77 and get(2, 3) == nil and get(2, 31) ~= nil)
check('... the epoch moved there too', c[2]:call('tarn_buckets')[3] == 2)
check('... receiving again: done already', c[2]:call('tarn_bucket_recv', {5, 'M1'})[2] == 'received')
check('R serves it now', sel(2, 2, 5)[1] == 'ok' and sel(2, 2, 5)[2][1][2] == 77)
check('... and takes its writes', upd(2, 2, 5, 78)[1] == 'ok' and get(2, 2)[2] == 78)
r = c[1]:call('tarn_buckets')
check('the maps: S has 1..4 and 6..8, and remembers where 5 went',
      json.encode(r[4]) == '[[1,4],[6,8]]' and json.encode(r[5]) == '[[5,"sent",2,"M1"]]', r)
r = c[2]:call('tarn_buckets')
check('... R has 5 and 9..16', json.encode(r[4]) == '[[5,5],[9,16]]' and #r[5] == 0, r)
check('a read of every shard with the old epoch is refused on both',
      c[1]:call('tarn_count', {sid, 0, {}, 'ALL', box.NULL, true, {}, {}, true, false, {e = 1}})[1] == 'moved'
      and c[2]:call('tarn_count', {sid, 0, {}, 'ALL', box.NULL, true, {}, {}, true, false, {e = 1}})[1] == 'moved')

print('the fence and the drain: bucket 6')
c[2]:call('tarn_bucket_recv_begin', {6, 1, 'M3'})
-- A row lock on a row of the bucket, and a request waiting for it.
r = c[1]:call('tarn_lock', {'RC1', {{sid, 0, {11}, 6}}, 'X', {read = true}})
check('RC1 locks row 11', r[1] == 'ok', r)
local rc1gen = r[2]
local waited
fiber.create(function()
    waited = c[1]:call('tarn_lock', {'RC2', {{sid, 0, {11}, 6}}, 'X', {timeout = 10}})
end)
-- A prepared transaction that writes a row of the bucket.
r = prepare(1, 'P6', {{'replace', sid, {12, 1200, 'b12', 6}}}, {}, {}, {6})
check('P6 prepared, writing row 12', r[1] == 'yes', r)
check('... its lock names the bucket', c[1]:eval('return box.space._tarn_locks:select({...})[1].bucket',
                                                 {sid, 0}) == 6)
r = c[1]:call('tarn_bucket_send_begin', {6, 2, 'M3', 5})
check('S: sending', r[1] == 'ok', r)
gen = r[2]
n = copy(6, 1, 2, 'M3')
check('copied', n == 10, n)
c[2]:call('tarn_bucket_hold', {6, 'M3'})
r = c[1]:call('tarn_bucket_fence', {6, 'M3', 0.3})
check('the fence waits for the prepared transaction, then gives up', r[1] == 'timeout' and r[2] == 1205, r)
check('... the row locks on the bucket are broken', waited ~= nil and waited[1] ~= 'ok', waited)
r = c[1]:call('tarn_commit_rc', {'RC1', {{'replace', sid, {11, 0, 'b11', 6}}}, rc1gen, box.NULL, {6}})
check('... and their transaction cannot commit', r[1] ~= 'ok' and get(1, 11)[2] == 11, r)
check('the bucket stays fenced', upd(1, 13, 6, 0)[1] == 'moved')
-- The leader changes (here: read-only and back): the lock index is
-- rebuilt from _tarn_locks, which names the bucket of each lock.
c[1]:eval('box.cfg{read_only = true}')
c[1]:eval('box.cfg{read_only = false}')
ready(1, {1})
r = c[1]:call('tarn_bucket_fence', {6, 'M3', 0.3})
check('after a leader change: still waiting for P6', r[1] == 'timeout', r)
c[1]:call('tarn_apply', {'P6'})
r = c[1]:call('tarn_bucket_fence', {6, 'M3', 5})
check('P6 applied: drained', r[1] == 'ok', r)
r = c[1]:call('tarn_bucket_delta', {6, 'M3', gen, 1})
check('the notes of the leader before the change are lost', r[1] == 'lost', r)
-- The move copies the bucket again: new notes, new copy.
check('R drops what it kept', c[2]:call('tarn_bucket_recv_clear', {6, 'M3'})[1] == 'ok' and staged(2, 6) == 0)
r = c[1]:call('tarn_bucket_send_begin', {6, 2, 'M3', 5})
check('S notes the writes again, under a new generation', r[1] == 'ok' and r[2] ~= gen, r)
gen = r[2]
n = copy(6, 1, 2, 'M3')
check('copied again', n == 10 and staged(2, 6) == 10, n)
check('... nothing written since', delta(6, 1, 2, 'M3', gen) == 0)
check('sent', c[1]:call('tarn_bucket_send', {6, 'M3'})[1] == 'ok')
check('received, with P6\'s write', c[2]:call('tarn_bucket_recv', {6, 'M3'})[1] == 'ok' and get(2, 12)[2] == 1200)

print('a local commit in flight when the fence goes up: bucket 7')
c[2]:call('tarn_bucket_recv_begin', {7, 1, 'M4'})
-- Held after its writes (before its checks), a commit that was in flight
-- when the move began noting writes: send_begin waits for it.
c[1]:call('tarn_test_hook', {'written', 'hold'})
local done1
fiber.create(function() done1 = upd(1, 41, 7, 4100) end)
c[1]:call('tarn_test_hook', {'written', 'reached'})
r = c[1]:call('tarn_bucket_send_begin', {7, 2, 'M4', 0.3})
check('send_begin waits for the commits in flight', r[1] == 'timeout', r)
c[1]:call('tarn_test_hook', {'written', 'release'})
wait_for(function() return done1 ~= nil end)
check('... which then commits (the bucket is still served)', done1 and done1[1] == 'ok', done1)
r = c[1]:call('tarn_bucket_send_begin', {7, 2, 'M4', 5})
check('send_begin again', r[1] == 'ok', r)
gen = r[2]
n = copy(7, 1, 2, 'M4')
check('the copy has the commit', n == 5 and c[2]:eval('return box.space._tarn_recv:get{7, ...}.tuple[2]',
      {sid, require('msgpack').encode({41})}) == 4100, n)
c[2]:call('tarn_bucket_hold', {7, 'M4'})
-- A commit held after its writes while the fence goes up: the fence waits
-- for it, and its checks find the bucket fenced.
c[1]:call('tarn_test_hook', {'written', 'hold'})
local done2
fiber.create(function() done2 = upd(1, 42, 7, 4200) end)
c[1]:call('tarn_test_hook', {'written', 'reached'})
local fenced
fiber.create(function() fenced = c[1]:call('tarn_bucket_fence', {7, 'M4', 5}) end)
fiber.sleep(0.2)
check('the fence waits for the local commit in flight', fenced == nil, fenced)
c[1]:call('tarn_test_hook', {'written', 'release'})
wait_for(function() return fenced ~= nil and done2 ~= nil end)
check('... which then fails its check: the bucket is fenced', done2[1] == 'moved' and get(1, 42)[2] == 42, done2)
check('... and the fence is up', fenced[1] == 'ok', fenced)
-- A PREPARE whose read covers rows of the bucket but names no bucket (a
-- read of every shard) does not hold the move up.
st = c[1]:new_stream()
st:call('tarn_select', {sid, 1, {'c43'}, 'EQ', box.NULL, box.NULL, false, true, box.NULL, true})
r = prepare(1, 'P7', {{'replace', sid, {9, 9, 'a9', 4}}}, {}, {{sid, 1, {'c43'}, {'c43'}, true, true}}, {4}, st)
check('a PREPARE reading the bucket by a scan of every shard', r[1] == 'yes', r)
check('... does not hold the drain up', c[1]:call('tarn_bucket_fence', {7, 'M4', 0.3})[1] == 'ok')
c[1]:call('tarn_abort', {'P7', 1, {1}})

print('giving a move up')
r = c[1]:call('tarn_bucket_send_abort', {7, 'M4'})
check('S: active again', r[1] == 'ok' and upd(1, 43, 7, 4300)[1] == 'ok', r)
r = c[2]:call('tarn_bucket_recv_abort', {7, 'M4'})
check('R: forgotten, and what it kept is gone', r[1] == 'ok' and staged(2, 7) == 0 and sel(2, 41, 7)[1] == 'moved', r)
check('... repeated: nothing to do', c[2]:call('tarn_bucket_recv_abort', {7, 'M4'})[1] == 'ok'
      and c[1]:call('tarn_bucket_send_abort', {7, 'M4'})[1] == 'ok')
check('S serves it', sel(1, 41, 7)[1] == 'ok' and rows_of(1, 7) == 5)
r = c[1]:call('tarn_buckets')
check('... and owns it', json.encode(r[4]) == '[[1,4],[7,8]]', r)

print('APPLY of a prepared transaction while a bucket is sent: bucket 8')
c[2]:call('tarn_bucket_recv_begin', {8, 1, 'M5'})
r = prepare(1, 'P8', {{'replace', sid, {51, 5100, 'd51', 8}}}, {}, {}, {8})
check('P8 prepared before the move', r[1] == 'yes', r)
r = c[1]:call('tarn_bucket_send_begin', {8, 2, 'M5', 5})
gen = r[2]
n = copy(8, 1, 2, 'M5')
check('copied without P8\'s write', n == 5, n)
c[1]:call('tarn_apply', {'P8'})
c[2]:call('tarn_bucket_hold', {8, 'M5'})
check('fenced', c[1]:call('tarn_bucket_fence', {8, 'M5', 5})[1] == 'ok')
check('the last copy has the APPLY\'s write', delta(8, 1, 2, 'M5', gen) == 1)
c[1]:call('tarn_bucket_send', {8, 'M5'})
c[2]:call('tarn_bucket_recv', {8, 'M5'})
check('R has it', get(2, 51)[2] == 5100 and rows_of(2, 8) == 5)

print('a read that waits while a bucket is sent')
-- Rows in buckets 3 (61..63) and 4 (64..66), both on shard 1. A prepared
-- transaction holds a W lock on row 64; a read of every shard over
-- 61..66 waits for it; meanwhile bucket 3 moves to shard 2.
for id = 61, 66 do c[1].space['test.bk']:replace{id, id, 'f' .. id, id <= 63 and 3 or 4} end
r = prepare(1, 'P9', {{'replace', sid, {64, 6400, 'f64', 4}}}, {}, {}, {4})
check('P9 holds row 64', r[1] == 'yes', r)
local e0 = c[1]:call('tarn_buckets')[3]
local waiting
fiber.create(function()
    waiting = c[1]:call('tarn_select', {sid, 0, {61}, 'GE', box.NULL, {66}, true, true, box.NULL, false, box.NULL,
                                        {e = e0}})
end)
fiber.sleep(0.1)
check('the read waits for P9', waiting == nil, waiting)
c[2]:call('tarn_bucket_recv_begin', {3, 1, 'M6'})
gen = c[1]:call('tarn_bucket_send_begin', {3, 2, 'M6', 5})[2]
copy(3, 1, 2, 'M6')
c[2]:call('tarn_bucket_hold', {3, 'M6'})
check('bucket 3 fenced (P9 holds a row of bucket 4)', c[1]:call('tarn_bucket_fence', {3, 'M6', 5})[1] == 'ok')
delta(3, 1, 2, 'M6', gen)
check('sent', c[1]:call('tarn_bucket_send', {3, 'M6'})[1] == 'ok')
check('received', c[2]:call('tarn_bucket_recv', {3, 'M6'})[1] == 'ok')
c[1]:call('tarn_abort', {'P9', 1, {1}})
wait_for(function() return waiting ~= nil end)
check('the read, done after the send, is refused: its epoch is gone', waiting ~= nil and waiting[1] == 'moved', waiting)
r = c[1]:call('tarn_select', {sid, 0, {61}, 'GE', box.NULL, {66}, true, true, box.NULL, false, box.NULL, {e = e0 + 1}})
check('... read again with the new epoch: rows 64..66 here', r[1] == 'ok' and #r[2] == 3, r)

print('moves and schema changes wait for each other')
local function job(owner, kind, name)
    return c[1]:call('tarn_job_begin', {{owner = owner, kind = kind, space = name, names = {name}, ver = 0,
                                         a = {}, phase = 'prepare'}, 0})
end
check('a move begins', job('o1', 'move', 'bucket:9')[1] == 'ok')
check('another move does not wait for it', job('o3', 'move', 'bucket:10')[1] == 'ok')
r = job('o2', 'alter', 'test.other')
check('a schema change waits for them', r[1] == 'busy' and (r[2] == 'o1' or r[2] == 'o3'), r)
c[1]:call('tarn_job_end', {'o1'}); c[1]:call('tarn_job_end', {'o3'})
check('the schema change begins', job('o2', 'alter', 'test.other')[1] == 'ok')
r = job('o4', 'move', 'bucket:9')
check('a move waits for it', r[1] == 'busy' and r[2] == 'o2', r)
c[1]:call('tarn_job_end', {'o2'})

print('the map read again while a step waits for its quorum: bucket 2')
-- A leader change while a step's commit waits for the quorum makes the
-- shard read its map again: the map must keep a send in progress (its rows
-- are gone once it commits), not serve a receive in progress (its rows are
-- not seen until it commits), and keep the rows of a last copy in progress
-- for the hold. Here the commits wait for a quorum of 2 that a single node
-- cannot give, until it is lowered again.
local function quorum(i, n) c[i]:eval('box.cfg{replication_synchro_quorum = ...}', {n}) end
local function flip(i)   -- read-only and back: the shard reads its map again
    c[i]:eval('box.cfg{read_only = true}'); fiber.sleep(0.1)
    c[i]:eval('box.cfg{read_only = false}')
    ready(i) -- local health only: peer certificates wait for pending sync work
end
for i = 1, 2 do
    c[i]:eval([[box.ctl.promote()
        box.space._tarn_buckets:alter{is_sync = true} box.space._tarn_recv:alter{is_sync = true}
        box.cfg{replication_synchro_timeout = 30}]])
end
for i = 1, 2 do ready(i, {i}) end
for id = 71, 73 do c[1].space['test.bk']:replace{id, id, 'f' .. id, 2} end
c[2]:call('tarn_bucket_recv_begin', {2, 1, 'M7'})
gen = c[1]:call('tarn_bucket_send_begin', {2, 2, 'M7', 5})[2]
copy(2, 1, 2, 'M7')
c[2]:call('tarn_bucket_hold', {2, 'M7'})
r = c[1]:call('tarn_autocommit', {{{'insert', sid, {74, 74, 'g74', 2}}}, {wb = {2}}})
check('a row written after the copy (k = g74)', r[1] == 'ok', r)
check('fenced', c[1]:call('tarn_bucket_fence', {2, 'M7', 5})[1] == 'ok')
local d = c[1]:call('tarn_bucket_delta', {2, 'M7', gen, 1})
quorum(2, 2)
local f = c[2]:call('tarn_bucket_recv_rows', {2, 'M7', d[2]}, {is_async = true})
fiber.sleep(0.2)
check('the last copy waits for its quorum', not f:is_ready())
flip(2)
c[2]:call('tarn_info')   -- (reads the map again)
quorum(2, 1)
r = f:wait_result(5)[1]
check('... and is made', r[1] == 'ok', r)
local st = c[2]:new_stream()
st:call('tarn_select', {sid, 1, {'g74'}, 'EQ', box.NULL, box.NULL, false, false, box.NULL, true})
r = st:call('tarn_validate', {'T7', {}, {{sid, 1, {'g74'}, {'g74'}, true, true}}, true, box.NULL})
check('a scan of R covering the row of the last copy fails its validation (the hold has it)', r[1] ~= 'ok', r)
pcall(st.rollback, st)
-- First-read barriers must settle pending synchronous work. Open these
-- observers before the steps wait for quorum, then retain the route oracle
-- across the map reload without trying to refresh an existing snapshot.
local send_reader = c[1]:new_stream()
send_reader:eval('tarn.fresh.begin_stream()')
quorum(1, 2)
f = c[1]:call('tarn_bucket_send', {2, 'M7'}, {is_async = true})
fiber.sleep(0.2)
check('the send waits for its quorum', not f:is_ready())
flip(1)
r = send_reader:call('tarn_select', {sid, 0, {71}, 'EQ', 1, box.NULL, true, true, box.NULL, false, box.NULL, 2})
check('... meanwhile a read of the bucket on S is refused, also after the map was read again',
      not f:is_ready() and r[1] == 'moved', r)
pcall(send_reader.rollback, send_reader)
quorum(1, 1)
r = f:wait_result(5)[1]
check('the send is made', r[1] == 'ok', r)
r = sel(1, 71, 2)
check('... and S refuses the bucket', r[1] == 'moved', r)
local m = c[1]:call('tarn_buckets')
check('... its epoch in memory is the stored one', c[1]:call('tarn_info').bucket_epoch == m[3]
      and c[1]:eval("return box.space._tarn_meta:get{'buckets'}.value.epoch") == m[3], m)
local recv_reader = c[2]:new_stream()
recv_reader:eval('tarn.fresh.begin_stream()')
quorum(2, 2)
f = c[2]:call('tarn_bucket_recv', {2, 'M7'}, {is_async = true})
fiber.sleep(0.2)
check('the receive waits for its quorum', not f:is_ready())
flip(2)
r = recv_reader:call('tarn_select', {sid, 0, {71}, 'EQ', 1, box.NULL, true, true, box.NULL, false, box.NULL, 2})
check('... meanwhile R does not serve the bucket, also after the map was read again',
      not f:is_ready() and r[1] == 'moved', r)
pcall(recv_reader.rollback, recv_reader)
quorum(2, 1)
r = f:wait_result(5)[1]
check('the receive is made', r[1] == 'ok', r)
r = sel(2, 74, 2)
check('... and R serves the bucket, with the row of the last copy', r[1] == 'ok' and #r[2] == 1, r)
for i = 1, 2 do
    c[i]:eval('box.space._tarn_buckets:alter{is_sync = false} box.space._tarn_recv:alter{is_sync = false}')
end

check('no locks or row locks left', c[1]:call('tarn_info').locks == 0 and c[2]:call('tarn_info').locks == 0
      and c[1]:call('tarn_info').row_locks == 0)

print(string.format('\n%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
