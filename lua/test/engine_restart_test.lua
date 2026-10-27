-- Actual SIGKILL and WAL recovery at engine-copy/publication boundaries.
-- Invoked by engine_faults.sh with a private data directory per scenario.
local fio, fiber, ffi, json = require('fio'), require('fiber'), require('ffi'), require('json')
io.stdout:setvbuf('no')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local work, source_engine, scenario, stage = assert(arg[1]), assert(arg[2]), assert(arg[3]), assert(arg[4])
local native = os.getenv('TARN_NATIVE')
if native and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1048576,
    vinyl_memory = 32 * 1048576, vinyl_cache = 0, wal_mode = 'fsync', checkpoint_interval = 0,
    txn_isolation = 'read-confirmed', election_mode = 'off', replication_synchro_quorum = 1,
    replication_synchro_timeout = 5, log_level = 3}
box.ctl.promote()
local tarn = require('tarn')
tarn.lazy.paused = true
tarn.init{shard_id = 1, peers = {}, sync = true, test_hooks = true, check_yields = true, check_locks = true,
    recovery_after = 1000000, native = native and native ~= '0' and native or false}
local function ok(r) assert(type(r) == 'table' and r[1] == 'ok', json.encode(r)); return r end
local function wait(label, fn)
    local deadline = fiber.clock() + 8
    repeat if fn() then return end; fiber.sleep(.005) until fiber.clock() >= deadline
    error(label .. ' timed out')
end
wait('schema/native', function()
    -- Replicated setup runs asynchronously. The meta space is created
    -- before the bucket/read-lock schema that tarn_info itself reads.
    for _, name in ipairs({'_tarn_meta', '_tarn_buckets', '_tarn_catalog', '_tarn_locks', '_tarn_part', '_tarn_txn'}) do
        if box.space[name] == nil or box.space[name].index[0] == nil then return false end
    end
    return box.space._tarn_part.index.state ~= nil and
        (not native or native == '0' or (tarn_info().native or {}).ready == 1)
end)
if native and native ~= '0' then
    local ns = ffi.cast('struct tarn_ns *', box.func['tarn_storage.ts_state']:call())
    assert(ns.layout == 10 and ns.ready == 1 and ffi.sizeof('struct tarn_ns') == 304)
else assert(tarn.cfg.native == false and tarn_info().native == false) end
print('IDENTITY ' .. json.encode{version = box.info.version, native = tarn_info().native,
    sync = tarn.cfg.sync, wal = box.cfg.wal_mode, source_engine = source_engine, scenario = scenario, stage = stage})
local name, old_id, new_id, count = 'test.engine_restart', 90400, 90401, 300
local target_engine = source_engine == 'memtx' and 'vinyl' or 'memtx'
local owner = 'engine-restart-' .. scenario .. '-' .. source_engine
local function receipt() return assert(box.space._tarn_meta:get{'engine:' .. old_id}).value end
local function persisted() return assert(box.space._tarn_meta:get{'test:engine_restart'}).value end
local function killed()
    assert(tarn_info().stats.lock_copy_errors == 0, 'native lock copy differed before process loss')
    io.stdout:flush()
    pcall(ffi.cdef, 'int getpid(void);')
    pcall(ffi.cdef, 'int kill(int pid, int sig);')
    ffi.C.kill(ffi.C.getpid(), 9)
    error('SIGKILL unexpectedly returned')
end
local function held(hook, fn)
    ok(tarn_test_hook(hook, 'hold'))
    local state = {}
    local worker = fiber.new(function()
        state.result = fn()
        state.done = true
    end)
    worker:set_joinable(true)
    wait(hook, function()
        local reached = tarn_test_hook(hook, 'reached')[2] == 1
        if state.done and not reached then error('operation ended before ' .. hook .. ': ' .. json.encode(state.result)) end
        return reached
    end)
    return worker, state
end
local function released(hook, worker, state)
    ok(tarn_test_hook(hook, 'release'))
    wait('worker completion', function() return state.done end)
    local joined, err = worker:join(); assert(joined, tostring(err)); ok(state.result)
end
local function audit(extra)
    local target = assert(box.space[name])
    assert(target.id == new_id and target.engine == target_engine)
    assert(box.space[old_id] == nil, 'the retired source survived cleanup')
    assert(target.index[0].id == 0 and target.index.by_v.id == 4 and target.index.bucket_id.id == 7)
    assert(target.index[0]:count() == count + (extra and 1 or 0))
    for i = 1, count do
        local row = assert(target:get{i}); assert(#row == 3 and row[2] == i * 10 and row[3] == 1)
    end
    if extra then assert(target:get{0}[2] == 777) else assert(target:get{0} == nil) end
    assert(box.space._tarn_catalog:get{name}.def.id == new_id)
    assert(receipt().phase == 'published' and receipt().cleaned == true)
    assert(box.space._tarn_meta:get{'hold:' .. old_id} == nil)
    assert(box.space._tarn_meta:get{'hold:' .. new_id} == nil)
    local stale = tarn_select(old_id, 0, {1}, 'EQ', 1)
    assert(stale[1] == 'error' and stale[2] == 1412, json.encode(stale))
    assert(tarn_info().stats.lock_copy_errors == 0, 'native lock copy differed during restart recovery')
end
local a
if stage == 'crash' then
    ok(tarn_bucket_init(16, {{1, 16}}))
    local format = {{'id', 'unsigned'}, {'v', 'integer'}, {'bucket_id', 'unsigned'}}
    local indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
        {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}}}
    ok(tarn_create_table{id = old_id, name = name, engine = source_engine, ver = 0,
        format = format, indexes = indexes, lazy = {}, json = json.encode{space_id = old_id, ver = 0}})
    local source = box.space[old_id]
    source.index.bucket_id:drop()
    source:create_index('by_v', {id = 4, unique = true, parts = {{2, 'integer'}}})
    source:create_index('bucket_id', {id = 7, unique = false, parts = {{3, 'unsigned'}}})
    for i = 1, count do source:insert{i, i * 10, 1} end
    a = {ver = 5, timeout = 8, engine_change = {old_id = old_id, old_ver = 0,
        old_engine = source_engine, old_name = name}, uix = {}, uix_drop = {}, lazy_drop = {},
        def = {id = new_id, name = name, engine = target_engine, ver = 5, lazy = {},
            format = format, indexes = indexes, json = json.encode{space_id = new_id, ver = 5}}}
    box.space._tarn_meta:replace{'test:engine_restart', {a = a, owner = owner, scenario = scenario}}
    ok(tarn_job_begin({owner = owner, kind = 'alter', space = name, names = {name}, ver = a.ver, a = a}, 0))
    if scenario == 'copy' then
        held('engine_copy', function() return tarn_alter_build(name, a, 1) end)
        assert(receipt().phase == 'copying' and receipt().copied == 128)
        assert(box.space[new_id].index[0]:count() == 128)
    else
        ok(tarn_alter_build(name, a, 1))
        if scenario == 'prepared' then
            held('engine_prepared', function() return tarn_alter_prepare(name, a, 1) end)
            assert(receipt().phase == 'prepared')
        else
            ok(tarn_alter_prepare(name, a, 1)); ok(tarn_job_phase(owner, 'apply'))
            local hook = scenario == 'rename' and 'engine_publish' or 'engine_published'
            held(hook, function() return tarn_alter_apply(name, a, 1) end)
            assert(box.space[name].id == new_id and box.space[old_id] ~= nil)
            local catalog_id = box.space._tarn_catalog:get{name}.def.id
            assert(catalog_id == (scenario == 'rename' and old_id or new_id))
            assert(receipt().phase == (scenario == 'rename' and 'applying' or 'published'))
            assert(box.space._tarn_meta:get{'hold:' .. new_id} ~= nil)
        end
    end
    print('  ok   ' .. source_engine .. '/' .. scenario .. ': SIGKILL at durable boundary')
    killed()
elseif stage == 'finish' then
    local state = persisted(); a = state.a
    local resumed = owner .. '-resumed'
    local takeover = ok(tarn_job_touch(resumed, state.owner))
    local gen = takeover[2]; assert(gen == 2)
    if scenario == 'copy' then
        assert(receipt().copied == 128 and box.space[new_id].index[0]:count() == 128)
        -- A successful write behind the old cursor must survive loss of
        -- volatile dirty keys. Continuing the old cursor would miss it.
        ok(tarn_autocommit({{'insert', old_id, {0, 777, 1}}}, {fence = {old_id, 0}}))
        local worker, result = held('engine_copy', function() return tarn_alter_build(name, a, gen) end)
        assert(box.space[new_id].index[0]:count() == 128 and box.space[new_id]:get{0} ~= nil,
            'capture loss did not clear and recopy the shadow from its beginning')
        released('engine_copy', worker, result)
        ok(tarn_alter_prepare(name, a, gen)); ok(tarn_job_phase(resumed, 'apply'))
    elseif scenario == 'prepared' then
        local before = receipt()
        assert(before.phase == 'prepared' and box.space[new_id].index[0]:count() == count)
        ok(tarn_alter_build(name, a, gen)); ok(tarn_alter_prepare(name, a, gen))
        local after = receipt()
        assert(after.capture == before.capture and after.copied == before.copied,
            'a durable PREPARED image was rebuilt after restart')
        ok(tarn_job_phase(resumed, 'apply'))
    elseif scenario == 'rename' then
        assert(box.space[name].id == new_id and box.space._tarn_catalog:get{name}.def.id == old_id)
    else
        assert(scenario == 'published' and receipt().phase == 'published' and not receipt().cleaned)
        assert(box.space[old_id] ~= nil and box.space._tarn_meta:get{'hold:' .. new_id} ~= nil)
        -- The confirmed local publication must finish cleanup even when
        -- its coordinator has already removed the central job record.
        ok(tarn_job_end(resumed))
    end
    ok(tarn_alter_apply(name, a, gen)); ok(tarn_job_end(resumed))
    audit(scenario == 'copy')
    ok(tarn_alter_apply(name, a, gen)); ok(tarn_alter_abort(name, a, 1))
    print('  ok   ' .. source_engine .. '/' .. scenario .. ': restart completes the owned operation')
elseif stage == 'reopen' then
    a = persisted().a
    audit(scenario == 'copy')
    ok(tarn_alter_apply(name, a, 2)); ok(tarn_alter_abort(name, a, 1))
    print('  ok   ' .. source_engine .. '/' .. scenario .. ': second restart preserves cleanup, data and old-ID fence')
else error('unknown engine restart stage ' .. stage) end
os.exit(0)
