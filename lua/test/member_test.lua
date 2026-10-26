-- Membership (issue #25): the cluster's shards as every shard keeps them,
-- the refusal of requests routed with an older membership, and what a shard
-- that joins gets: the tables and a bucket map with no buckets. Run via
-- lua/test/run.sh after bucket_test.lua: shards 1 and 2 have bucket maps,
-- shard 3 is a new one.
local net = require('net.box')
local json = require('json')

local S1, S2, S3 = os.getenv('S1'), os.getenv('S2'), os.getenv('S3')
local opts = {user = 'tarn', password = 'tarn', wait_connected = 5}
local c = {net.connect(S1, opts), net.connect(S2, opts), net.connect(S3, opts)}
assert(c[1]:is_connected() and c[2]:is_connected() and c[3]:is_connected(), 'cannot connect to shards')

local pass, fail = 0, 0
local function check(name, cond, extra)
    if cond then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. (extra and ('  -- ' .. json.encode(extra)) or '')) end
end
-- A value as text, with the keys of each table in order.
local function canon(v)
    if type(v) ~= 'table' then return json.encode(v) end
    local keys = {}
    for k in pairs(v) do keys[#keys + 1] = k end
    table.sort(keys, function(a, b) return tostring(a) < tostring(b) end)
    local out = {}
    for _, k in ipairs(keys) do out[#out + 1] = tostring(k) .. '=' .. canon(v[k]) end
    return '{' .. table.concat(out, ',') .. '}'
end

-- test.mb(id, v, bucket_id) on shards 1 and 2; rows 1..3 on shard 1, in a
-- bucket it owns.
local def = {
    name = 'test.mb', engine = 'memtx', id = 25025,
    format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
              {name = 'bucket_id', type = 'unsigned'}},
    indexes = {{name = 'primary', unique = true, parts = {{field = 'id', type = 'unsigned'}}},
               {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}}},
}
for i = 1, 2 do c[i]:call('tarn_create_table', {def}) end
c[1]:reload_schema()
local sid = c[1].space['test.mb'].id
local map = c[1]:call('tarn_buckets')
local b = map[4][1][1]   -- the first bucket shard 1 owns
for id = 1, 3 do c[1].space['test.mb']:replace{id, id, b} end
local epoch = map[3]

print('the membership')
local r = c[3]:call('tarn_members')
check('a shard without a membership record has version 0 and no shards',
      r[1] == 'ok' and r[2] == 0 and next(r[3]) == nil, r)
local m1 = {ver = 1, shards = {{id = 1, replicas = {S1}, state = 'active'},
                               {id = 2, replicas = {S2}, state = 'active'},
                               {id = 3, replicas = {S3}, state = 'active'}}}
r = c[1]:call('tarn_members_set', {m1})
check('a newer membership is recorded', r[1] == 'ok' and r[2] == 1, r)
r = c[1]:call('tarn_members_set', {{ver = 0, shards = {}}})
check('an older one is not', r[1] == 'ok' and r[2] == 1, r)
r = c[1]:call('tarn_members')
check('... it reads back as recorded', r[1] == 'ok' and r[2] == 1 and #r[3] == 3 and r[3][3].id == 3
      and r[3][3].replicas[1] == S3 and r[3][3].state == 'active', r)
r = c[1]:call('tarn_buckets')
check("the shard's part of the bucket map names the version", r[1] == 'ok' and r[6] == 1, r)
c[1]:eval('box.cfg{read_only = true} box.cfg{read_only = false}')   -- (a leader change: read again)
r = c[1]:call('tarn_buckets')
check('... also after the shard read its state again', r[1] == 'ok' and r[6] == 1, r)

print('requests routed with an older membership')
local function sel(route)
    return c[1]:call('tarn_select', {sid, 0, {1}, 'EQ', 1, box.NULL, true, true, box.NULL, false, box.NULL, route})
end
r = sel({e = epoch, m = 0})
check('a read of every shard routed with version 0 is refused', r[1] == 'moved' and r[2] == 1213, r)
r = sel({e = epoch, m = 1})
check('... with version 1 it is served', r[1] == 'ok' and #r[2] == 1, r)
r = sel({e = epoch})
check('... and without one (a server from before membership)', r[1] == 'ok' and #r[2] == 1, r)
r = sel(b)
check('a read routed by its bucket is served', r[1] == 'ok' and #r[2] == 1, r)

-- A stream that read row 1 as a read of every shard (rpoint without a
-- bucket), then commits in one of the ways a transaction does.
local function stream()
    local s = c[1]:new_stream()
    s:begin()
    s:call('tarn_select', {sid, 0, {1}, 'EQ', 1, box.NULL, true, true, box.NULL, false, box.NULL, box.NULL})
    return s
end
local scan = {{sid, 0, {1}, box.NULL}}
local s = stream()
r = s:call('tarn_validate', {'MB1', scan, {}, true, box.NULL, 0})
check('a validation routed with version 0 is refused', r[1] == 'moved', r)
s = stream()
r = s:call('tarn_validate', {'MB2', scan, {}, true, box.NULL, 1})
check('... with version 1 it passes', r[1] == 'ok', r)
s = stream()
r = s:call('tarn_validate', {'MB3', scan, {}, true, box.NULL, box.NULL})
check('... and without one', r[1] == 'ok', r)
s = stream()
r = s:call('tarn_commit_local', {'MB4', {{'replace', sid, {2, 20, b}}}, scan, {}, box.NULL, box.NULL, {b}, 0})
check('a local commit routed with version 0 is refused', r[1] == 'moved' and c[1].space['test.mb']:get{2}[2] == 2, r)
s = stream()
r = s:call('tarn_commit_local', {'MB5', {{'replace', sid, {2, 21, b}}}, scan, {}, box.NULL, box.NULL, {b}, 1})
check('... with version 1 it commits', r[1] == 'ok' and c[1].space['test.mb']:get{2}[2] == 21, r)
s = stream()
r = s:call('tarn_prepare', {'MB6', 1, {1, 2}, {{'replace', sid, {3, 30, b}}}, scan, {}, require('fiber').time(),
                            box.NULL, box.NULL, {b}, 0})
check('a PREPARE routed with version 0 votes no (moved)', r[1] == 'moved' and r[2] == 1213, r)
check('... and leaves a tombstone', c[1]:eval("return box.space._tarn_part:get{'MB6'}[2]") == 'aborted')

print('a change of the shards and the other jobs wait for each other')
local function job(owner, kind, name)
    return c[1]:call('tarn_job_begin', {{owner = owner, kind = kind, space = name, names = {name}, ver = 0,
                                         a = {}, phase = 'prepare'}, 0})
end
r = job('m1', 'members', 'members')
check('a change of the shards begins, and learns the first shard\'s version', r[1] == 'ok' and r[2] == 1, r)
r = job('m2', 'move', 'bucket:3')
check('a move waits for it', r[1] == 'busy' and r[2] == 'm1', r)
r = job('m3', 'create', 'test.other')
check('a schema change waits for it', r[1] == 'busy' and r[2] == 'm1', r)
c[1]:call('tarn_job_end', {'m1'})
check('a move begins', job('m2', 'move', 'bucket:3')[1] == 'ok')
r = job('m4', 'members', 'members')
check('a change of the shards waits for it', r[1] == 'busy' and r[2] == 'm2', r)
c[1]:call('tarn_job_end', {'m2'})
check('a schema change begins', job('m3', 'create', 'test.other')[1] == 'ok')
r = job('m4', 'members', 'members')
check('a change of the shards waits for it too', r[1] == 'busy' and r[2] == 'm3', r)
c[1]:call('tarn_job_end', {'m3'})

print('a shard that joins: the tables, then a map with no buckets')
local ex = c[1]:call('tarn_schema_export')
local names = {}
for _, t in ipairs(ex[2]) do names[t.name] = t end
check('the first shard exports its tables, with their spaces and global index spaces',
      ex[1] == 'ok' and names['test.mb'] ~= nil and names['test.mb'].space.id == sid
      and #names['test.mb'].space.indexes == 2 and names['test.bk'] ~= nil, ex)
-- (The tables of the other test files may have space ids that the new
-- shard's own spaces took: SQL servers give tables ids from 10000 up.)
local want = {names['test.bk'], names['test.mb']}
r = c[3]:call('tarn_schema_import', {want})
check('the new shard makes them', r[1] == 'ok' and r[2] == 2, r)
r = c[3]:call('tarn_schema_import', {want})
check('... again: nothing is missing', r[1] == 'ok' and r[2] == 0, r)
local ex3 = c[3]:call('tarn_schema_export')
check('... and has them as the first shard has: ids, formats, indexes, catalog entries',
      ex3[1] == 'ok' and canon(ex3[2]) == canon(want), {ex3[2], want})
c[3]:reload_schema()
c[3].space['test.mb']:replace{9, 9, b}
r = c[3]:call('tarn_schema_import', {want})
check('a shard with rows in a table is refused', r[1] == 'error' and r[2] == 1210 and r[3]:find('rows') ~= nil, r)
c[3].space['test.mb']:delete{9}
c[3]:call('tarn_create_table', {{name = 'test.extra', engine = 'memtx', id = 25026,
                                  format = {{name = 'id', type = 'unsigned'}},
                                  indexes = {{name = 'primary', unique = true, parts = {{field = 'id', type = 'unsigned'}}}}}})
r = c[3]:call('tarn_schema_import', {want})
check('... and so is one with a table the cluster has not', r[1] == 'error' and r[3]:find('test.extra') ~= nil, r)
c[3]:call('tarn_drop_table', {'test.extra'})
r = c[3]:call('tarn_bucket_init', {map[2], {}})
check('the new shard gets a map with no buckets', r[1] == 'ok', r)
r = c[3]:call('tarn_buckets')
check('... it owns none and moves none', r[1] == 'ok' and r[2] == map[2] and #r[4] == 0 and next(r[5]) == nil, r)
r = c[3]:call('tarn_bucket_init', {map[2] + 1, {}})
check('... and a second map is not made', r[1] == 'exists' and r[2] == map[2], r)
r = c[3]:call('tarn_members_set', {m1})
check('it learns the membership', r[1] == 'ok' and r[2] == 1 and c[3]:call('tarn_buckets')[6] == 1, r)

check('no locks or row locks left', c[1]:call('tarn_info').locks == 0 and c[1]:call('tarn_info').row_locks == 0)
for i = 1, 2 do c[i]:call('tarn_drop_table', {'test.mb'}) end
c[3]:call('tarn_drop_table', {'test.mb'})

print(string.format('\n%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
