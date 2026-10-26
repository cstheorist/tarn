-- Rebalancing (issue #25): the load and the size of each bucket as a shard
-- reports them (tarn_bucket_stats, tarn_bucket_sizes), the rebalancer's
-- record on the first shard (tarn_rebalance_get / _put), and the moves held
-- back while a schema change waits for them (tarn_job_begin). Run via
-- lua/test/run.sh after member_test.lua: shards 1 and 2 have bucket maps.
local net = require('net.box')
local json = require('json')
local fiber = require('fiber')

local S1, S2 = os.getenv('S1'), os.getenv('S2')
local opts = {user = 'tarn', password = 'tarn', wait_connected = 5}
local c = {net.connect(S1, opts), net.connect(S2, opts)}
assert(c[1]:is_connected() and c[2]:is_connected(), 'cannot connect to shards')

local pass, fail = 0, 0
local function check(name, cond, extra)
    if cond then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. (extra and ('  -- ' .. json.encode(extra)) or '')) end
end

-- test.rb(id, v, pad, bucket_id) on shards 1 and 2.
local def = {
    name = 'test.rb', engine = 'memtx', id = 25027,
    format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
              {name = 'pad', type = 'string'}, {name = 'bucket_id', type = 'unsigned'}},
    indexes = {{name = 'primary', unique = true, parts = {{field = 'id', type = 'unsigned'}}},
               {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}}},
}
for i = 1, 2 do c[i]:call('tarn_create_table', {def}) end
c[1]:reload_schema()
local sid = c[1].space['test.rb'].id
local map1, map2 = c[1]:call('tarn_buckets'), c[2]:call('tarn_buckets')
local epoch = map1[3]
-- a bucket shard 1 owns and one shard 2 owns
local b = map1[4][#map1[4]][2]
local b2 = map2[4][1][1]

-- {reads, writes, bytes} of bucket x in shard i's report (zeros when absent)
local function stats(i, x)
    local r = c[i]:call('tarn_bucket_stats')
    assert(r[1] == 'ok', json.encode(r))
    for _, e in ipairs(r[3]) do
        if e[1] == x then return {e[2], e[3], e[4]}, r end
    end
    return {0, 0, 0}, r
end
-- {rows, bytes} of bucket x in shard i's report (zeros when absent)
local function size(i, x)
    local r = c[i]:call('tarn_bucket_sizes')
    assert(r[1] == 'ok', json.encode(r))
    for _, e in ipairs(r[2]) do
        if e[1] == x then return {e[2], e[3]}, r end
    end
    return {0, 0}, r
end
local function sel(id, route)
    return c[1]:call('tarn_select', {sid, 0, {id}, 'EQ', 1, box.NULL, true, true, box.NULL, false, box.NULL, route})
end

local z0 = size(1, b)

print('the load of a bucket')
for id = 1, 4 do c[1].space['test.rb']:replace{id, id, string.rep('x', 100), b} end
local tsize = c[1]:eval('return box.space[...]:get{1}:bsize()', {'test.rb'})
local l0 = stats(1, b)
for _ = 1, 3 do sel(1, b) end
local l1 = stats(1, b)
check('three reads routed by the bucket count three', l1[1] - l0[1] == 3 and l1[2] == l0[2], {l0, l1})
sel(1, {e = epoch})
sel(1, {b, b2 + 0})   -- (a list of buckets: shard 1 refuses b2, so nothing is served)
local l2 = stats(1, b)
check('a read of every shard counts for no bucket', l2[1] == l1[1], {l1, l2})
for v = 1, 2 do
    local r = c[1]:call('tarn_autocommit', {{{'update', sid, {2}, {{'=', 2, v}}}}, {wb = {b}}})
    assert(r[1] == 'ok', json.encode(r))
end
local l3 = stats(1, b)
check('two writes to its rows count two', l3[2] - l2[2] == 2 and l3[1] == l2[1], {l2, l3})
check('... and the bytes of the rows they wrote', l3[3] - l2[3] == 2 * tsize, {l2, l3, tsize})
local r = c[1]:call('tarn_autocommit', {{{'update', sid, {3}, {{'=', 2, 0}}}}, {wb = {b2}}})
check('a write refused (the bucket is not here) counts nothing', r[1] == 'moved' and stats(1, b)[2] == l3[2], r)
local _, rep = stats(2, b)
local listed = false
for _, e in ipairs(rep[3]) do if e[1] == b then listed = true end end
check('a shard reports only the buckets it owns', not listed and rep[2] >= 0, rep)
c[1]:eval('tarn.cfg.load_window = 0.2')
fiber.sleep(0.5)
local l4 = stats(1, b)
check('counts older than two windows are dropped', l4[1] == 0 and l4[2] == 0, l4)
sel(1, b)
fiber.sleep(0.25)
local l5, rep5 = stats(1, b)
check('... those of the window before are kept, with the seconds they cover',
      l5[1] == 1 and rep5[2] >= 0.2 and rep5[2] < 1, rep5)
c[1]:eval('tarn.cfg.load_window = 60')

print('the size of a bucket')
local z = size(1, b)
check('its rows and their bytes', z[1] - z0[1] == 4 and z[2] - z0[2] == 4 * tsize, {z0, z, tsize})
local _, zr = size(2, b)
local seen = false
for _, e in ipairs(zr[2]) do if e[1] == b then seen = true end end
check('a shard reports only the buckets it owns', not seen, zr)
c[1].space['test.rb']:replace{5, 5, 'y', b}
check('a row written since is counted', size(1, b)[1] == z[1] + 1)
local y0 = size(2, b2)
c[2].space['test.rb']:replace{6, 6, 'z', b2}
check("another shard's bucket is counted there", size(2, b2)[1] == y0[1] + 1)

print("the rebalancer's record")
local function put(owner, ver, rec) return c[1]:call('tarn_rebalance_put', {owner, ver, rec}) end
r = c[1]:call('tarn_rebalance_get')
check('none at first', r[1] == 'ok' and r[2] == nil, r)
r = put(box.NULL, box.NULL, {owner = 'a', status = 'running', moves = {}})
check('one begins when none runs: version 1', r[1] == 'ok' and r[2] == 1, r)
r = put(box.NULL, box.NULL, {owner = 'b', status = 'running', moves = {}})
check('a second one does not', r[1] == 'busy' and r[2].owner == 'a', r)
r = put('b', 1, {owner = 'b', status = 'running', moves = {}})
check('only its owner writes it', r[1] == 'busy', r)
r = put('a', 0, {owner = 'a', status = 'paused', moves = {}})
check('... at the version it read', r[1] == 'busy', r)
r = put('a', 1, {owner = 'a', status = 'paused', moves = {{7, 1, 2, 100, 'planned', ''}}})
check('... then it is replaced: version 2', r[1] == 'ok' and r[2] == 2, r)
r = c[1]:call('tarn_rebalance_get')
check('it reads back, with its version and age', r[1] == 'ok' and r[2].owner == 'a' and r[2].status == 'paused'
      and r[2].ver == 2 and r[2].moves[1][1] == 7 and r[3] >= 0 and r[3] < 5, r)
r = put('a', 2, {owner = 'c', status = 'running', moves = {}})
check('its owner hands it over (a takeover)', r[1] == 'ok' and r[2] == 3, r)
r = put('a', 3, {owner = 'a', status = 'running', moves = {}})
check('... and the old owner no longer writes it', r[1] == 'busy' and r[2].owner == 'c', r)
r = put('c', 3, {owner = 'c', status = 'cancelled', moves = {}})
check('it ends', r[1] == 'ok' and r[2] == 4, r)
r = put(box.NULL, box.NULL, {owner = 'd', status = 'running', moves = {}})
check('then another one begins', r[1] == 'ok' and r[2] == 5, r)
r = put('d', 5, {owner = 'd', status = 'done', moves = {}})
check('... and ends', r[1] == 'ok' and r[2] == 6, r)

print('moves wait while a schema change waits for moves')
fiber.sleep(1.1)   -- (the earlier tests' jobs that waited are forgotten)
local function begin(owner, kind, names, wait)
    return c[1]:call('tarn_job_begin', {{owner = owner, kind = kind, space = names[1], names = names, ver = 0,
                                         a = {}, phase = 'prepare'}, wait})
end
local function job_end(owner) return c[1]:call('tarn_job_end', {owner}) end
r = begin('m1', 'move', {'bucket:9001'}, 0)
check('a move begins', r[1] == 'ok', r)
local t0 = fiber.clock()
r = begin('ddl1', 'alter', {'test.rb'}, 0.3)
check('a schema change waits for it', r[1] == 'busy' and r[2] == 'm1' and fiber.clock() - t0 >= 0.25, r)
r = begin('m2', 'move', {'bucket:9002'}, 0)
check('... and meanwhile another move does not begin', r[1] == 'busy' and r[2] == '', r)
-- the schema change waits again (as its SQL server does); the move ends
local ddl = fiber.new(function() return begin('ddl1', 'alter', {'test.rb'}, 3) end)
ddl:set_joinable(true)
fiber.sleep(0.1)
r = begin('m3', 'move', {'bucket:9003'}, 0)
check('... nor while it waits again', r[1] == 'busy', r)
job_end('m1')
local ok, rr = ddl:join()
check('once the move ended, the schema change begins', ok and rr[1] == 'ok', rr)
r = begin('m2', 'move', {'bucket:9002'}, 0)
check('... and moves wait for it, as before', r[1] == 'busy' and r[2] == 'ddl1', r)
job_end('ddl1')
r = begin('m2', 'move', {'bucket:9002'}, 0)
check('then moves begin again', r[1] == 'ok', r)
r = begin('ddl2', 'alter', {'test.rb'}, 0.1)
check('a schema change that gives up waiting', r[1] == 'busy' and r[2] == 'm2', r)
fiber.sleep(1.2)
r = begin('m4', 'move', {'bucket:9004'}, 0)
check('... holds moves back only for a while', r[1] == 'ok', r)
job_end('m2'); job_end('m4')
r = begin('m7', 'move', {'bucket:9007'}, 0)
local r2 = begin('mb1', 'members', {'members'}, 0.1)
local r3 = begin('m8', 'move', {'bucket:9008'}, 0)
check('a change of the shards waiting for moves holds them back too',
      r[1] == 'ok' and r2[1] == 'busy' and r2[2] == 'm7' and r3[1] == 'busy' and r3[2] == '', {r, r2, r3})
job_end('m7')
r = begin('mb1', 'members', {'members'}, 0)
check('... until it begins', r[1] == 'ok', r)
job_end('mb1')
r = begin('m8', 'move', {'bucket:9008'}, 0)
check('... and ends', r[1] == 'ok', r)
job_end('m8')

c[1]:call('tarn_drop_table', {'test.rb'})
c[2]:call('tarn_drop_table', {'test.rb'})
print(string.format('%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
