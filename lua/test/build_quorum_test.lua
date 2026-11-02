-- A vinyl index build on a replicated shard by a leader that resigns
-- (issue #271). A follower applies a build in its applier and acknowledges
-- nothing until it ends; when that takes more than twice replication_timeout
-- the leader resigns, and a build it had started still committed on it
-- afterwards, a row the replica set never got. Now, before each build, the
-- leader waits until a quorum of the replica set has applied what it wrote,
-- and a leader that resigned does not start its next build; and a build
-- commits only if the node is still writable once it has built. Either way
-- the node refuses the step with ER_READONLY and writes nothing.
--
-- One node with synchronous spaces. The followers' acknowledgements come
-- from tarn.quorum_view, which stands in for box.info.replication,
-- box.info.ro and a quorum of two (one follower); the node itself keeps a
-- quorum of one, so that PREPARE's synchronous writes commit.
local fiber, json = require('fiber'), require('json')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local dir = assert(arg[1]); require('fio').mktree(dir)
box.cfg{work_dir = dir, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'write',
        replication_synchro_quorum = 1, replication_synchro_timeout = 1000, election_mode = 'off',
        txn_isolation = 'read-confirmed', log_level = 3}
box.ctl.promote()
local tarn = require('tarn').init{shard_id = 1, peers = {}, sync = true, test_hooks = true,
                                  check_yields = true, native = false}
local ready = fiber.clock() + 10
while (box.space._tarn_buckets == nil or box.info.ro or tarn.clock.read_check() ~= nil)
      and fiber.clock() < ready do fiber.sleep(0.01) end
assert(box.space._tarn_buckets ~= nil and tarn.clock.read_check() == nil, 'the shard did not become ready')
local passed, failed = 0, 0
local function check(label, yes, extra)
    if yes then passed = passed + 1; print('  ok   ' .. label)
    else failed = failed + 1; print('  FAIL ' .. label .. ' ' .. json.encode(extra)) end
end
local function ok(r) assert(r[1] == 'ok', json.encode(r)); return r end
ok(tarn_bucket_init(16, {{1, 16}}))

local name = 'test.build_quorum'
local format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'unsigned'},
                {name = 'bucket_id', type = 'unsigned'}}
local indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}},
                 {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}}}
ok(tarn_create_table({name = name, engine = 'vinyl', format = format, indexes = indexes}))
local sp = box.space[name]
local writes = {}
for id = 1, 200 do writes[#writes + 1] = {'insert', sp.id, {id, id % 7, 1}} end
ok(tarn_autocommit(writes))

-- The follower (id 2) has applied up to `acked` of this node's rows.
local me = box.info.id
local view = {ro = false, acked = 0}
tarn.quorum_view = function()
    return {ro = view.ro, quorum = 2, replication = {
        [me] = {id = me},
        [2] = {id = 2, downstream = {vclock = {[me] = view.acked}}},
    }}
end

view.acked = box.info.lsn
local t0 = fiber.clock()
local done, err = pcall(tarn.quorum_applied, 5)
check('a follower that applied every row lets the build start at once',
      done and fiber.clock() - t0 < 1, {err = tostring(err)})

view.acked = box.info.lsn - 1
fiber.create(function() fiber.sleep(0.3); view.acked = box.info.lsn end)
t0 = fiber.clock()
done, err = pcall(tarn.quorum_applied, 5)
local waited = fiber.clock() - t0
check('... one still applying holds it until it has applied them',
      done and waited >= 0.25 and waited < 3, {waited = waited, err = tostring(err)})

view.acked = box.info.lsn - 1
done, err = pcall(tarn.quorum_applied, 0.2)
check('... and for too long, the build is refused as a conflict, to be sent again',
      not done and err.code == box.error.TRANSACTION_CONFLICT, {err = tostring(err)})

view.ro = true
done, err = pcall(tarn.quorum_applied, 5)
check('a leader that resigned while it waited refuses the build (ER_READONLY)',
      not done and err.code == box.error.READONLY, {err = tostring(err)})

-- Through PREPARE: the next build does not start, and nothing is written.
local parts = {{field = 2, type = 'unsigned'}, {field = 1, type = 'unsigned'}}
local all = table.deepcopy(indexes)
all[3] = {name = 'v_idx', unique = false, parts = parts}
local alt = {ver = 1, timeout = 5, checks = {}, builds = {{2, 'v_idx', false, parts}}, drop = {}, renames = {},
             def = {name = name, id = sp.id, engine = 'vinyl', format = format, indexes = all, json = '{}', ver = 1}}
local lsn = box.info.lsn
local r
done, err = pcall(function() r = tarn_alter_prepare(name, alt, 1) end)
local refused = (not done and type(err) == 'cdata' and err.code == box.error.READONLY)
    or (done and r ~= nil and r[1] ~= 'ok')
check('PREPARE of a leader that resigned builds nothing', refused and sp.index[2] == nil and box.info.lsn == lsn,
      {done = done, err = tostring(err), reply = r, index = sp.index[2] ~= nil, lsn = {lsn, box.info.lsn}})

-- With the follower caught up and the node writable, PREPARE builds as before.
view.ro = false
tarn.quorum_view = function()
    return {ro = false, quorum = 2,
            replication = {[me] = {id = me}, [2] = {id = 2, downstream = {vclock = {[me] = box.info.lsn}}}}}
end
ok(tarn_alter_prepare(name, alt, 1))
check('PREPARE builds once a quorum has applied', sp.index[2] ~= nil and sp.index[2].name == '_tarn_tmp_2')
tarn.quorum_view = nil
ok(tarn_alter_apply(name, alt, 1))
check('APPLY names the index', sp.index[2] ~= nil and sp.index[2].name == 'v_idx')

-- A leader that resigns while it builds rolls the build back: a trigger on
-- _index makes the node read-only as the build starts.
local parts3 = {{field = 3, type = 'unsigned'}, {field = 1, type = 'unsigned'}}
local all3 = table.deepcopy(all)
all3[4] = {name = 'b_idx', unique = false, parts = parts3}
local alt3 = {ver = 2, timeout = 5, checks = {}, builds = {{3, 'b_idx', false, parts3}}, drop = {}, renames = {},
              def = {name = name, id = sp.id, engine = 'vinyl', format = format, indexes = all3, json = '{}', ver = 2}}
view.ro = false
tarn.quorum_view = function()
    return {ro = view.ro, quorum = 2,
            replication = {[me] = {id = me}, [2] = {id = 2, downstream = {vclock = {[me] = box.info.lsn}}}}}
end
local started = 0
local function resign(_, new)
    if new ~= nil and new[1] == sp.id and new[2] == 3 then started = started + 1; view.ro = true end
end
box.space._index:before_replace(resign)
lsn = box.info.lsn
local result = {pcall(tarn_alter_prepare, name, alt3, 1)}
box.space._index:before_replace(nil, resign)
check('a leader that resigns while it builds rolls the build back and refuses it (ER_READONLY)',
      started == 1 and result[1] == false and type(result[2]) == 'cdata'
          and result[2].code == box.error.READONLY and sp.index[3] == nil and box.info.lsn == lsn,
      {started = started, result = {result[1], tostring(result[2])}, index = sp.index[3] ~= nil,
       lsn = {lsn, box.info.lsn}})
view.ro = false
ok(tarn_alter_prepare(name, alt3, 1))
check('... and builds it again once writable', sp.index[3] ~= nil and sp.index[3].name == '_tarn_tmp_3')
tarn.quorum_view = nil
ok(tarn_alter_apply(name, alt3, 1))
check('... which APPLY names', sp.index[3] ~= nil and sp.index[3].name == 'b_idx' and sp.index[3]:count() == 200)

print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
