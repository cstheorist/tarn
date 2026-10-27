-- Actual lazy target calls: a zero-write ACK cannot depend on a marker,
-- floor or retirement record whose synchronous commit can still roll back.
local fio, fiber, mp = require('fio'), require('fiber'), require('msgpack')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local native = os.getenv('TARN_NATIVE')
if native ~= nil and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'fsync',
        txn_isolation = 'read-confirmed', replication_synchro_quorum = 1,
        replication_synchro_timeout = .5, election_mode = 'off', log_level = 3}
box.ctl.promote()
local tarn = require('tarn'); tarn.lazy.paused = true
-- Assert the target reply BEFORE any source ACK: a source quorum timeout
-- must never hide a defective early target ACK in this primitive fixture.
tarn.init{shard_id = 1, peers = {}, sync = false, native = native ~= nil and native ~= '0' and native or false,
          test_hooks = true, check_yields = true}
fiber.sleep(.03)
assert(tarn.lazy.quota.ready())
assert(tarn_bucket_init(16, {{1, 16}})[1] == 'ok')
local passed, failed = 0, 0
local function check(name, yes, extra)
    if yes then passed = passed + 1; print('  ok   ' .. name)
    else failed = failed + 1; print('  FAIL ' .. name .. ' ' .. require('json').encode(extra)) end
end
local function ok(r) assert(r[1] == 'ok', require('json').encode(r)); return r end
local function wait(label, fn)
    local deadline = fiber.clock() + 4
    repeat if fn() then return end; fiber.sleep(.005) until fiber.clock() >= deadline
    error(label .. ' timed out')
end
for engine_no, engine in ipairs({'memtx', 'vinyl'}) do
    local sid, gen = 71000 + engine_no * 10, 71001 + engine_no * 10
    local def = {id = sid, name = 'test.lazy_target_confirm_' .. engine, ver = 0, engine = engine, json = '{}',
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'string'}, {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{2, 'string'}}}}}
    ok(tarn_create_table(def))
    def.lazy = {{gen = gen, base_space_id = sid, local_index_id = 2, route_parts = 1,
                 key_fields = {2}, key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1},
                 pk_types = {'unsigned'}, bucket_field = 3}}
    ok(tarn_lazy_control('register', def))
    box.space[gen]:alter{is_sync = true}
    box.space._tarn_lazy_floor:alter{is_sync = true}
    box.space._tarn_lazy_defs:alter{is_sync = true}
    for case_no, proof in ipairs({'marker', 'floor', 'retirement'}) do
        local label = engine .. ' ' .. proof
        ok(tarn_autocommit({{'insert', sid, {case_no, proof, 1}}}, {fence = {sid, 0}, wb = {1}}))
        local events = ok(tarn_lazy_control('pending'))[2]
        assert(#events == 1, 'fixture must retain exactly one source assignment')
        local a, first, duplicate = events[1]
        local route = {a[5]}
        box.cfg{replication_synchro_quorum = 2}
        local writer = fiber.new(function()
            if proof == 'marker' then
                -- Real target entrypoint, SAME sole assignment as the retry.
                first = tarn_lazy_apply(events, route)
            else
                -- Isolate other no-write proof branches. These direct
                -- metadata writes do not claim a full GC/DDL loss history.
                local wrote, err = pcall(function()
                    box.begin{txn_isolation = 'read-committed'}
                    if proof == 'floor' then box.space._tarn_lazy_floor:replace{gen, a[5], a[3], a[4]}
                    else box.space._tarn_lazy_defs:update(gen, {{'=', 4, 'retired'}}) end
                    box.commit()
                end)
                if box.is_in_txn() then box.rollback() end
                first = wrote and {'ok'} or {'error', err.code, tostring(err)}
            end
        end)
        writer:set_joinable(true)
        wait(label .. ' first commit reaches a real quorum wait', function()
            return first == nil and box.info.synchro.queue.len == 1
        end)
        duplicate = tarn_lazy_apply(events, route)
        check(label .. ': unconfirmed state cannot produce a target ACK', duplicate[1] ~= 'ok', duplicate)
        wait(label .. ' first transaction rolls back', function() return first ~= nil and box.info.synchro.queue.len == 0 end)
        assert(writer:join())
        local retained = ok(tarn_lazy_control('pending'))[2]
        box.begin{txn_isolation = 'read-confirmed'}
        local absent = box.space[gen]:get{require('varbinary').new(mp.encode({a[6], a[7]}))} == nil
        local floor_absent = box.space._tarn_lazy_floor:get{gen, a[5], a[3]} == nil
        local active = box.space._tarn_lazy_defs:get{gen}.phase == 'ready'
        box.rollback()
        check(label .. ': rollback keeps the sole source obligation and no target marker', first[1] ~= 'ok'
              and absent and #retained == 1 and tostring(retained[1][1]) == tostring(a[1])
              and floor_absent and active, {first, retained})
        box.cfg{replication_synchro_quorum = 1}
        local retry = ok(tarn_lazy_apply(events, route))
        ok(tarn_lazy_control('ack', retry[2]))
        local again = ok(tarn_lazy_apply(events, route))
        local frontier = ok(tarn_lazy_frontiers({1}, {1}))[2][1]
        check(label .. ': confirmed retry and duplicate converge exactly once', #again[2] == 1
              and #ok(tarn_lazy_control('pending'))[2] == 0
              and box.space[gen].index.ordered:count{true, proof} == 1
              and frontier[2] == a[4] and frontier[3] == a[4])
    end
end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
