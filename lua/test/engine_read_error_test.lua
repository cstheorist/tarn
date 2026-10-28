-- A confirmed read can retain a vinyl index handle while an engine swap
-- removes that physical space. Known schema changes report 1412, while
-- native errors for tables this migration never owned keep their meaning.
local fio, fiber, json = require('fio'), require('fiber'), require('json')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local native = os.getenv('TARN_NATIVE')
if native and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1048576,
    vinyl_memory = 32 * 1048576, vinyl_cache = 0, wal_mode = 'fsync',
    txn_isolation = 'read-confirmed', log_level = 3}
local tarn = require('tarn').init{shard_id = 1, peers = {},
    native = native and native ~= '0' and native or false, test_hooks = true, check_yields = true}
fiber.sleep(.03)
if native and native ~= '0' then
    local ffi = require('ffi')
    local ns = ffi.cast('struct tarn_ns *', box.func['tarn_storage.ts_state']:call())
    assert(ns.layout == 12 and ns.ready == 1 and ffi.sizeof('struct tarn_ns') == 392)
    assert(ffi.offsetof('struct tarn_ns', 'capture_stale') == 200
        and ffi.offsetof('struct tarn_ns', 'write_excluded') == 208
        and ffi.offsetof('struct tarn_ns', 'locks_range') == 296
        and ffi.offsetof('struct tarn_ns', 'fallbacks') == 304)
else assert(tarn.cfg.native == false and tarn_info().native == false) end
local function ok(r) assert(r[1] == 'ok', json.encode(r)); return r end
ok(tarn_bucket_init(16, {{1, 16}}))
local passed, failed = 0, 0
local function check(name, yes, extra)
    if yes then passed = passed + 1; print('  ok   ' .. name)
    else failed = failed + 1; print('  FAIL ' .. name .. ' ' .. json.encode(extra)) end
end
local sid, target_id, name = 87001, 87002, 'test.engine_read_error'
local def = {id = sid, name = name, engine = 'vinyl', ver = 0, json = '{}', lazy = {},
    format = {{'id', 'unsigned'}, {'v', 'unsigned'}, {'bucket_id', 'unsigned'}},
    indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
               {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}}}}
ok(tarn_create_table(def))
for i = 1, 300 do box.space[sid]:insert{i, i, 1} end
local cached = box.space[sid].index[0]
local nd = {}; for k, v in pairs(def) do nd[k] = v end
nd.id, nd.ver, nd.engine = target_id, 1, 'memtx'
local a = {ver = 1, timeout = 5,
    engine_change = {old_id = sid, old_ver = 0, old_engine = 'vinyl', old_name = name},
    def = nd, uix = {}, uix_drop = {}, lazy_drop = {}}
local owner = 'engine-read-error'
ok(tarn_job_begin({owner = owner, kind = 'alter', space = name, names = {name}, ver = 1, a = a}, 0))
ok(tarn_test_hook('scan', 'hold'))
local done, worked, reply
local reader = fiber.new(function()
    worked, reply = pcall(tarn_select, sid, 0, {}, 'ALL', 500, box.NULL, true, true, 0, false)
    done = true
end)
reader:set_joinable(true)
check('public scan captured the old physical index before cutover', tarn_test_hook('scan', 'reached')[2] > 0)
ok(tarn_alter_build(name, a, 1)); ok(tarn_alter_prepare(name, a, 1))
ok(tarn_job_phase(owner, 'apply')); ok(tarn_alter_apply(name, a, 1)); ok(tarn_job_end(owner))
check('cutover removed the old physical space and preserved the image',
    box.space[sid] == nil and box.space[target_id]:len() == 300)
local yes, err = pcall(cached.get, cached, {1})
check('the actual retained handle produces typed NO_SUCH_SPACE with its old ID',
    not yes and err.code == box.error.NO_SUCH_SPACE and tonumber(err.space) == sid)
print('NATIVE_ERROR ' .. json.encode(err:unpack()))
local changed = tarn.engine.translate_error(err)
check('only the known migration identity gets the schema retry', changed and changed[2] == 1412, changed)
local other = box.schema.space.create('never_migrated', {id = 87999})
other:create_index('pk')
local unknown = other.index[0]; other:drop()
local unknown_ok, unknown_err = pcall(unknown.get, unknown, {1})
check('an unrelated typed missing-space error is not reclassified',
    not unknown_ok and unknown_err.code == box.error.NO_SUCH_SPACE
    and tarn.engine.translate_error(unknown_err) == nil)
ok(tarn_test_hook('scan', 'release')); assert(reader:join())
check('the real held public scan returns 1412 instead of leaking native 1146',
    done and worked and reply[1] == 'error' and reply[2] == 1412,
    worked and reply or tostring(reply))
local r = tarn_select(87999, 0, {1}, 'EQ', 1, box.NULL, true, true)
check('unknown table requests still return ordinary 1146', r[1] == 'error' and r[2] == 1146, r)
r = tarn_lock('engine-old-point', {{sid, 0, {1}, 1}}, 'X', {read = true})
check('old point-lock requests return schema retry and release their locks',
    r[1] == 'error' and r[2] == 1412 and tarn_info().row_locks == 0, r)
r = tarn_lock_range('engine-old-range', {{sid, {1}, 1, {}, 1}}, 'X', {})
check('old range-lock requests return schema retry and release their locks',
    r[1] == 'error' and r[2] == 1412 and tarn_info().row_locks == 0, r)
r = tarn_select(target_id, 0, {1}, 'EQ', 1, box.NULL, true, true)
check('fresh requests still read the replacement normally',
    r[1] == 'ok' and #r[2] == 1 and r[2][1][2] == 1, r)
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
