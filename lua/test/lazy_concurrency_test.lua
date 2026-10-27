-- Exact ReadConfirmed stream shape: independent buckets must not acquire
-- a shared native usage-row conflict while another writer awaits WAL.
local fio, fiber = require('fio'), require('fiber')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'fsync',
        txn_isolation = 'read-confirmed', log_level = 3}
local tarn = require('tarn'); tarn.lazy.paused = true
tarn.init{shard_id = 1, peers = {}, native = false, test_hooks = true, check_yields = true}
fiber.sleep(.03)
assert(tarn.lazy.quota.ready())
assert(tarn_bucket_init(16, {{1, 16}})[1] == 'ok')
local pass = 0
for i, engine in ipairs({'memtx', 'vinyl'}) do
    local sid, gen = 66000 + i * 10, 66001 + i * 10
    local def = {id = sid, name = 'test.lazy_concurrency_' .. engine, ver = 0, engine = engine, json = '{}',
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'unsigned'}, {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{2, 'unsigned'}}}}}
    assert(tarn_create_table(def)[1] == 'ok')
    def.lazy = {{gen = gen, base_space_id = sid, local_index_id = 2, route_parts = 1,
        key_fields = {2}, key_types = {'unsigned'}, pk_fields = {1}, pk_native_fields = {1}, pk_types = {'unsigned'}, bucket_field = 3}}
    assert(tarn_lazy_control('register', def)[1] == 'ok')
    assert(tarn_autocommit({{'insert', sid, {1, 0, 1}}, {'insert', sid, {2, 0, 2}}}, {fence = {sid, 0}, wb = {1, 2}})[1] == 'ok')
    tarn.lazy.fresh()
    local sp = box.space[sid]
    for round = 1, 3 do
        local go, ready = fiber.channel(1), fiber.channel(1)
        local state, a_result, b_result, overlapped = 'idle'
        local function publish(id, bucket)
            local old = sp:get{id}
            local new = sp:replace{id, old[2] + 1, bucket}
            tarn.lazy.capture({{sp, {id}, old, new, new, false}})
        end
        local a = fiber.new(function()
            a_result = {pcall(function()
                box.begin{txn_isolation = 'read-confirmed', timeout = 60}
                assert(sp:get{1} ~= nil)
                assert(go:put(true, 3)); assert(ready:get(3))
                overlapped = state == 'committing'
                publish(1, 1)
                box.commit()
            end)}
            if box.is_in_txn() then box.rollback() end
        end)
        a:set_joinable(true)
        local b = fiber.new(function()
            b_result = {pcall(function()
                assert(go:get(3))
                box.begin{txn_isolation = 'read-confirmed', timeout = 60}
                publish(2, 2)
                assert(ready:put(true, 3))
                state = 'committing'; box.commit(); state = 'done'
            end)}
            if box.is_in_txn() then box.rollback() end
        end)
        b:set_joinable(true)
        assert(a:join()); assert(b:join())
        assert(overlapped, engine .. ': no actual WAL-wait overlap')
        assert(a_result[1] and b_result[1], engine .. ': independent publishers conflicted: '
               .. tostring(a_result[2]) .. '/' .. tostring(b_result[2]))
        assert(sp:get{1}[2] == round and sp:get{2}[2] == round)
        local stats = tarn_lazy_stats(); assert(stats[1] == 'ok')
        assert(stats[2].depth == box.space._tarn_lazy_outbox:len())
        pass = pass + 1; print('  ok   ' .. engine .. ': disjoint-bucket publishers both commit across WAL overlap ' .. round)
    end
end
local ordinary = {name='test.lazy_cut_unrelated',id=67001,ver=0,engine='memtx',json='{}',
    format={{name='id',type='unsigned'},{name='bucket_id',type='unsigned'}},
    indexes={{name='primary',unique=true,parts={{1,'unsigned'}}},
             {name='bucket_id',unique=false,parts={{2,'unsigned'}}}}}
assert(tarn_create_table(ordinary)[1]=='ok')
local function held_writer(id)
    assert(tarn_test_hook('written','hold')[1]=='ok')
    local result
    local f=fiber.new(function() result=tarn_autocommit({{'insert',ordinary.id,{id,3}}},{wb={3}}) end)
    f:set_joinable(true)
    assert(tarn_test_hook('written','reached')[2]==1)
    return f,function() return result end
end
local old, old_result=held_writer(1)
tarn.lazy.quota_invalidate()
fiber.sleep(.04)
assert(not tarn.lazy.quota.ready(),'a pre-invalidation writer was omitted from the cut')
assert(tarn_test_hook('written','release')[1]=='ok');assert(old:join());assert(old_result()[1]=='ok')
local deadline=fiber.clock()+3
while not tarn.lazy.quota.ready() and fiber.clock()<deadline do fiber.sleep(.01) end
assert(tarn.lazy.quota.ready())
pass=pass+1;print('  ok   rebuild waits for the captured old local-commit prefix')
tarn.lazy.quota_invalidate()
local later,later_result=held_writer(2)
deadline=fiber.clock()+3
while not tarn.lazy.quota.ready() and fiber.clock()<deadline do fiber.sleep(.01) end
local ready_while_busy=tarn.lazy.quota.ready() and later:status()~='dead'
assert(tarn_test_hook('written','release')[1]=='ok');assert(later:join());assert(later_result()[1]=='ok')
assert(ready_while_busy,'new unrelated traffic starved the quota rebuild')
pass=pass+1;print('  ok   later unrelated in-flight traffic cannot starve the captured rebuild cut')
print(pass .. ' passed, 0 failed')
os.exit(0)
