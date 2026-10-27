-- Real detached shadow commit, plus a partial dirty-set failure. Both
-- witnesses exercise the engine module and the shared zero-delta tickets.
local fio, fiber, json = require('fio'), require('fiber'), require('json')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local native = os.getenv('TARN_NATIVE')
if native and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1048576,
    vinyl_memory = 32 * 1048576, vinyl_cache = 0, wal_mode = 'fsync',
    txn_isolation = 'read-confirmed', replication_synchro_quorum = 1,
    replication_synchro_timeout = 5, election_mode = 'off', log_level = 3}
box.ctl.promote()
local tarn = require('tarn').init{shard_id = 1, peers = {}, sync = true,
    native = native and native ~= '0' and native or false,
    test_hooks = true, check_yields = true, recovery_after = 1000000}
local function wait(label, fn)
    local deadline = fiber.clock() + 5
    repeat if fn() then return end; fiber.sleep(.005) until fiber.clock() >= deadline
    error(label .. ' timed out')
end
wait('schema/native', function()
    return box.space._tarn_meta ~= nil and
        (not native or native == '0' or (tarn_info().native or {}).ready == 1)
end)
if native and native ~= '0' then
    local ffi = require('ffi')
    local ns = ffi.cast('struct tarn_ns *', box.func['tarn_storage.ts_state']:call())
    assert(ns.layout == 8 and ns.ready == 1 and ffi.sizeof('struct tarn_ns') == 240)
else assert(tarn.cfg.native == false and tarn_info().native == false) end
print('IDENTITY ' .. json.encode{version = box.info.version, native = tarn_info().native,
    sync = tarn.cfg.sync, wal = box.cfg.wal_mode})
local function ok(r) assert(r[1] == 'ok', json.encode(r)); return r end
ok(tarn_bucket_init(16, {{1, 16}}))
local passed, failed = 0, 0
local function check(name, yes, extra)
    if yes then passed = passed + 1; print('  ok   ' .. name)
    else failed = failed + 1; print('  FAIL ' .. name .. ' ' .. json.encode(extra)) end
end
local function manifest(sid) return box.space._tarn_meta:get{'engine:' .. sid}.value end
for n, source_engine in ipairs({'memtx', 'vinyl'}) do
    local sid, first_target = 83000 + n * 20, 83001 + n * 20
    local name = 'test.engine_terminal_' .. source_engine
    local def = {id = sid, name = name, engine = source_engine, ver = 0, json = '{}', lazy = {},
        format = {{'id', 'unsigned'}, {'bucket_id', 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{2, 'unsigned'}}}}}
    ok(tarn_create_table(def))
    for i = 1, 300 do box.space[sid]:insert{i, 1} end
    local function args(target, ver)
        local nd = {}; for k, v in pairs(def) do nd[k] = v end
        nd.id, nd.ver, nd.engine = target, ver, source_engine == 'memtx' and 'vinyl' or 'memtx'
        return {ver = ver, timeout = .2,
            engine_change = {old_id = sid, old_ver = 0, old_engine = source_engine, old_name = name},
            def = nd, uix = {}, uix_drop = {}, lazy_drop = {}}
    end
    local a, owner = args(first_target, 1), 'engine-terminal-' .. source_engine
    ok(tarn_job_begin({owner = owner, kind = 'alter', space = name, names = {name}, ver = 1, a = a}, 0))
    ok(tarn_test_hook('engine_copy', 'hold'))
    local result
    local builder = fiber.new(function() result = tarn_alter_build(name, a, 1) end)
    builder:set_joinable(true)
    assert(tarn_test_hook('engine_copy', 'reached')[2] > 0)
    -- Authorize ABORT first; then let the already-running old BUILD enter
    -- its next real synchronous shadow/cursor transaction.
    ok(tarn_job_phase(owner, 'abort'))
    box.cfg{replication_synchro_quorum = 2}
    ok(tarn_test_hook('engine_copy', 'release'))
    wait('pending shadow commit', function()
        return result == nil and box.info.synchro.queue.len > 0 and tarn.engine.tickets.outstanding > 0
    end)
    check(source_engine .. ': actual shadow chunk owns a ticket during quorum wait',
        box.space[first_target] ~= nil and tarn.engine.tickets.outstanding == 1)
    builder:cancel()
    wait('detached caller return', function() return result ~= nil end)
    check(source_engine .. ': caller returns while the detached native effect remains pending',
        result[1] ~= 'ok' and box.info.synchro.queue.len > 0 and tarn.engine.tickets.outstanding == 1, result)
    tarn.engine.invalidate(true)
    local denied = tarn_alter_abort(name, a, 1)
    check(source_engine .. ': UNKNOWN cut refuses cleanup without dropping the unresolved shadow',
        denied[1] ~= 'ok' and denied[2] == 1205 and box.space[first_target] ~= nil
        and tarn.engine.tickets.outstanding == 1, denied)
    box.cfg{replication_synchro_quorum = 1}
    wait('native transaction terminal callback', function() return tarn.engine.tickets.outstanding == 0 end)
    pcall(builder.join, builder) -- a completed joinable fiber can still report suspended
    a.timeout = 5
    ok(tarn_alter_abort(name, a, 1)); ok(tarn_job_end(owner))
    check(source_engine .. ': confirmed retry cleans its shadow and preserves the original',
        box.space[first_target] == nil and box.space[sid]:len() == 300
        and tarn.engine.tickets.outstanding == 0 and tarn.engine.triggers[sid] == nil)

    -- Deterministically fail the same list append that an allocation can
    -- fail, after its dedup marker was installed. This is failure injection,
    -- not a claim to exhaust the real allocator. The retry inserts behind
    -- the baseline cursor, so a missed dirty mark would lose a real row.
    local second_target = first_target + 1
    a, owner = args(second_target, 2), owner .. '-capture'
    a.timeout = 5
    ok(tarn_job_begin({owner = owner, kind = 'alter', space = name, names = {name}, ver = 2, a = a}, 0))
    ok(tarn_test_hook('engine_copy', 'hold'))
    result = nil
    builder = fiber.new(function() result = tarn_alter_build(name, a, 1) end)
    builder:set_joinable(true)
    assert(tarn_test_hook('engine_copy', 'reached')[2] > 0)
    local track = assert(tarn.engine.tracks[sid])
    local old_token = track.token
    setmetatable(track.list, {__newindex = function() error('injected dirty-list append allocation failure') end})
    local refused = tarn_autocommit({{'insert', sid, {0, 1}}}, {fence = {sid, 0}, wb = {1}})
    setmetatable(track.list, nil)
    check(source_engine .. ': partial marker failure aborts the source write and invalidates capture',
        refused[1] ~= 'ok' and box.space[sid]:get{0} == nil and not track.valid
        and next(track.keys) ~= nil and #track.list == 0, refused)
    ok(tarn_autocommit({{'insert', sid, {0, 1}}}, {fence = {sid, 0}, wb = {1}}))
    check(source_engine .. ': the same key can commit after failure without trusting the broken set',
        box.space[sid]:get{0} ~= nil and not track.valid)
    ok(tarn_test_hook('engine_copy', 'release')); assert(builder:join()); ok(result)
    check(source_engine .. ': lost capture forces full recopy including the committed key behind its cursor',
        manifest(sid).capture ~= old_token and box.space[second_target]:len() == 301
        and box.space[second_target]:get{0} ~= nil)
    ok(tarn_alter_prepare(name, a, 1)); ok(tarn_job_phase(owner, 'apply'))
    ok(tarn_alter_apply(name, a, 1)); ok(tarn_job_end(owner))
    check(source_engine .. ': recovered image publishes and fully cleans',
        box.space[name].id == second_target and box.space[name]:len() == 301
        and box.space[name]:get{0} ~= nil and manifest(sid).cleaned == true
        and box.space._tarn_meta:get{'hold:' .. sid} == nil
        and box.space._tarn_meta:get{'hold:' .. second_target} == nil)
end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
