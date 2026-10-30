-- Native DROP/TRUNCATE and bounded version cleanup recover behind a hold.
local fio, fiber, ffi = require('fio'), require('fiber'), require('ffi')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local work, engine, op, stage = assert(arg[1]), assert(arg[2]), assert(arg[3]), assert(arg[4])
fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'fsync',
        checkpoint_interval = 0, txn_isolation = 'read-confirmed', log_level = 3}
local tarn = require('tarn'); tarn.lazy.paused = true
tarn.init{shard_id = 1, peers = {}, native = false, test_hooks = true, check_yields = true}
-- Let the initial status watch and independent quota-rebuild worker run;
-- a caller never rebuilds/waits on its own transaction INFLIGHT marker.
require('fiber').sleep(0.03)
local meter_deadline = require('fiber').clock() + 3
while not tarn.lazy.quota.ready() and require('fiber').clock() < meter_deadline do require('fiber').sleep(0.01) end
assert(tarn.lazy.quota.ready(), 'lazy admission meter did not reach READY')
local sid, gen, name = 64001, 64002, 'test.lazy_clear'
local function ok(r) assert(r[1] == 'ok', require('json').encode(r)); return r end
local function call(phase, a) return _G['tarn_' .. op .. '_' .. phase](name, a, 1) end
local function crash(label)
    print('  ok   ' .. engine .. ' ' .. op .. ': ' .. label); io.stdout:flush()
    pcall(ffi.cdef, 'int getpid(void); int kill(int pid, int sig);')
    ffi.C.kill(ffi.C.getpid(), 9); error('SIGKILL returned')
end
local a
if stage == 'native' then
    ok(tarn_bucket_init(16, {{1, 16}}))
    local def = {id = sid, name = name, ver = 0, engine = engine, json = '{}',
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'string'}, {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{2, 'string'}}}}}
    ok(tarn_create_table(def))
    def.lazy = {{gen = gen, base_space_id = sid, local_index_id = 2, route_parts = 1,
                 key_fields = {2}, key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1},
                 pk_types = {'unsigned'}, bucket_field = 3}}
    ok(tarn_lazy_control('register', def))
    local writes, wb = {}, {}
    for i = 1, 260 do writes[i], wb[i] = {'insert', sid, {i, 'old', 1}}, 1 end
    ok(tarn_autocommit(writes, {fence = {sid, 0}, wb = wb}))
    -- Writes keep no per-row version (issue #177); DROP and TRUNCATE still
    -- clear what an older release wrote.
    assert(box.space._tarn_lazy_version:count{sid} == 0)
    local vb, mpk = require('varbinary'), require('msgpack')
    box.begin()
    for i = 1, 260 do box.space._tarn_lazy_version:insert{sid, vb.new(mpk.encode({i})), 1, i} end
    box.commit()
    a = {id = sid, ver = 1, timeout = 5, lazy_drop = {gen}}
    if op == 'truncate' then
        local nd = {}; for k, v in pairs(def) do nd[k] = v end
        local spec = {}; for k, v in pairs(def.lazy[1]) do spec[k] = v end; spec.gen = gen + 1
        nd.ver, nd.lazy = 1, {spec}; a.def, a.lazy_empty = nd, true
    end
    box.space._tarn_meta:replace{'test.lazy.clear_args', a}
    ok(call('prepare', a))
    if op == 'truncate' then
        repeat local r = ok(tarn_lazy_bootstrap(name, a, 1)); if r[2] then break end until false
        ok(tarn_lazy_ready(name, a, 1))
    end
    ok(tarn_test_hook('lazy_clear_schema', 'hold'))
    fiber.create(function() call('apply', a) end)
    assert(tarn_test_hook('lazy_clear_schema', 'reached')[2] == 1)
    assert(box.space._tarn_meta:get{'hold:' .. sid} ~= nil)
    assert(box.space._tarn_catalog:get{name}.def.ver == 0)
    assert(box.space._tarn_lazy_version:count{sid} == 260)
    assert(op == 'drop' and box.space[sid] == nil or op == 'truncate' and box.space[sid]:len() == 0)
    crash('SIGKILL after durable native clear, before catalog/capture publication')
elseif stage == 'cleanup' then
    a = box.space._tarn_meta:get{'test.lazy.clear_args'}.value
    assert(box.space._tarn_meta:get{'hold:' .. sid} ~= nil)
    ok(tarn_test_hook('lazy_clear_versions', 'hold'))
    fiber.create(function() call('apply', a) end)
    assert(tarn_test_hook('lazy_clear_versions', 'reached')[2] == 1)
    assert(box.space._tarn_lazy_version:count{sid} == 132)
    assert(box.space._tarn_meta:get{'hold:' .. sid} ~= nil and box.space._tarn_catalog:get{name}.def.ver == 0)
    crash('SIGKILL after one durable 128-row cleanup chunk, with hold retained')
elseif stage == 'finish' then
    a = box.space._tarn_meta:get{'test.lazy.clear_args'}.value
    assert(box.space._tarn_lazy_version:count{sid} == 132)
    ok(call('apply', a)); ok(call('apply', a))
    assert(box.space._tarn_lazy_version:count{sid} == 0 and box.space._tarn_meta:get{'hold:' .. sid} == nil)
    assert(box.space._tarn_lazy_defs:get{gen}.phase == 'retired')
    local front = ok(tarn_lazy_frontiers({1}, {1}))[2][1]
    assert(front[2] == 260)
    if op == 'drop' then
        assert(box.space[sid] == nil and box.space._tarn_catalog:get{name} == nil
               and box.space._tarn_lazy_capture:get{sid} == nil)
    else
        assert(box.space[sid]:len() == 0 and box.space._tarn_catalog:get{name}.def.ver == 1)
        assert(box.space._tarn_lazy_capture:get{sid}.ver == 1)
        ok(tarn_autocommit({{'insert', sid, {1, 'new', 1}}}, {fence = {sid, 1}, wb = {1}}))
        assert(ok(tarn_lazy_frontiers({1}, {1}))[2][1][2] == 261)
    end
    print('  ok   ' .. engine .. ' ' .. op .. ': retry completes cleanup/publication without resetting counters')
else error('unknown stage') end
os.exit(0)
