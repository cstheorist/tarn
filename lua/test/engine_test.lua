-- Black-box engine replacement contract (EngineCopy / EngineCutover).
-- Each invocation owns a fresh private database. Both directions run in
-- native-enabled and explicitly Lua-only stock jobs; no SQL fixture needed.
local fio, fiber, clock, json, mp, net = require('fio'), require('fiber'), require('clock'),
    require('json'), require('msgpack'), require('net.box')
io.stdout:setvbuf('no')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local native = os.getenv('TARN_NATIVE')
if native ~= nil and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
local work = assert(arg[1], 'private engine test directory required'); fio.mktree(work)
local uri = 'unix/:' .. fio.pathjoin(work, 'iproto.sock')
box.cfg{work_dir = work, listen = uri, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1048576,
    vinyl_memory = 32 * 1048576, vinyl_cache = 0, wal_mode = 'fsync', checkpoint_interval = 0,
    txn_isolation = 'read-confirmed', election_mode = 'off', replication_synchro_quorum = 1,
    replication_synchro_timeout = .5, log_level = 3}
box.ctl.promote()
local tarn = require('tarn'); tarn.lazy.paused = true
tarn.init{shard_id = 1, peers = {}, sync = true, test_hooks = true, check_yields = true, check_locks = true,
    recovery_after = 1000000, native = native ~= nil and native ~= '0' and native or false}

local pass, fail, sequence = 0, 0, 88000
local jobs, workers, streams = {}, {}, {}
local hooks = {'engine_copy', 'engine_tail', 'engine_prepared', 'engine_publish', 'engine_published'}
local function ok(r) assert(type(r) == 'table' and r[1] == 'ok', json.encode(r)); return r end
local function wait(label, fn, seconds)
    local until_ = fiber.clock() + (seconds or 5)
    repeat if fn() then return end; fiber.sleep(.005) until fiber.clock() >= until_
    error(label .. ' timed out')
end
local function check(label, yes, extra)
    if yes then pass = pass + 1; print('  ok   ' .. label)
    else fail = fail + 1; print('  FAIL ' .. label .. ' ' .. json.encode(extra)) end
end
local function confirmed(fn)
    box.begin{txn_isolation = 'read-confirmed'}
    local yes, r = pcall(fn)
    if box.is_in_txn() then box.rollback() end
    if not yes then error(r) end
    return r
end
local function id() sequence = sequence + 1; return sequence end
local function phase(t)
    return confirmed(function()
        local r = box.space._tarn_meta:get{'engine:' .. t.old_id}
        return r and table.deepcopy(r.value) or nil
    end)
end
local function job_phase(t)
    return confirmed(function()
        local r = box.space._tarn_meta:get{'job:' .. t.name}
        return r and r.value.phase or false
    end)
end
local function rows(sid)
    return confirmed(function()
        local out = {}
        if box.space[sid] then
            for _, row in box.space[sid].index[0]:pairs() do out[#out + 1] = row:totable() end
        end
        return out
    end)
end
local function catalog_row(t)
    for _, row in ipairs(tarn_catalog()) do if row[1] == t.name then return row end end
    return false
end
local function wanted(t)
    local out = {}; for _, row in pairs(t.expected) do out[#out + 1] = row end
    table.sort(out, function(a, b) return a[1] < b[1] end)
    return out
end
local function same(a, b)
    if type(a) ~= 'table' or type(b) ~= 'table' then return a == b end
    if #a ~= #b then return false end
    for n = 1, #a do if not same(a[n], b[n]) then return false end end
    return true
end
local function exact(t, sid) return same(rows(sid), wanted(t)) end
local function difference(t, sid)
    local actual, expected, differences = rows(sid), wanted(t), {}
    for n = 1, math.max(#actual, #expected) do
        if not same(actual[n], expected[n]) then
            differences[#differences + 1] = {at = n, actual = actual[n], expected = expected[n]}
            if #differences == 3 then break end
        end
    end
    return {actual_count = #actual, expected_count = #expected, differences = differences,
        semantic_equal = same(actual, expected), bytes_equal = mp.encode(actual) == mp.encode(expected)}
end
local function schema(sid)
    local sp, format, indexes = assert(box.space[sid]), {}, {}
    for _, f in ipairs(sp:format()) do
        format[#format + 1] = {f.name, f.type, f.is_nullable == true}
    end
    for n, ix in pairs(sp.index) do
        if type(n) == 'number' then
            local parts = {}
            for _, p in ipairs(ix.parts) do
                parts[#parts + 1] = {p.fieldno, p.type, p.is_nullable == true,
                    p.collation or box.NULL, p.path or box.NULL}
            end
            indexes[#indexes + 1] = {ix.id, ix.name, ix.type, ix.unique, parts}
        end
    end
    table.sort(indexes, function(a, b) return a[1] < b[1] end)
    return {format, indexes}
end
local function clean(t)
    local r = phase(t)
    local no_holds = confirmed(function()
        return box.space._tarn_meta:get{'hold:' .. t.old_id} == nil
            and box.space._tarn_meta:get{'hold:' .. t.new_id} == nil
    end)
    return r and r.phase == 'published' and r.cleaned == true and no_holds
        and box.space[t.old_id] == nil and box.space[t.new_id] ~= nil
        and box.space[t.new_id].name == t.name
        and box.space._tarn_catalog:get{t.name}.def.id == t.new_id
end
local function call(op, t, gen)
    return _G['tarn_alter_' .. op](t.name, t.args, gen or t.gen)
end
local function hook(name, action) return ok(tarn_test_hook(name, action)) end
local function spawn(fn)
    local s = {}
    s.fiber = fiber.new(function()
        s.ok, s.result = pcall(fn)
        s.done = true
    end)
    s.fiber:set_joinable(true); workers[#workers + 1] = s
    return s
end
local function result(s, cancelled)
    -- A completed joinable fiber is still reported "suspended" until join.
    wait('owned engine worker stops', function()
        return s.done or (cancelled and fiber.status(s.fiber) == 'dead')
    end)
    local joined, err = s.fiber:join()
    s.joined = true
    if not cancelled then
        assert(joined, tostring(err)); assert(s.ok, tostring(s.result)); return s.result
    end
end
local function interrupt(s, name)
    s.fiber:cancel()
    result(s, true)
    hook(name, 'release')
end
local function begin_job(t)
    ok(tarn_job_begin({owner = t.owner, kind = 'alter', space = t.name, names = {t.name},
        ver = t.args.ver, a = t.args, phase = 'prepare'}, 0))
    jobs[t.owner] = t
end
local function decide(t, value) return ok(tarn_job_phase(t.owner, value)) end
local function end_job(t) ok(tarn_job_end(t.owner)); jobs[t.owner] = nil end
local function change(t, writes)
    return tarn_autocommit(writes, {fence = {t.old_id, t.old_ver}})
end
local function create(engine, tag, count, with_uix)
    local sid, new_sid = id(), id()
    local old_uid, new_uid
    if with_uix then old_uid, new_uid = id(), id() end
    local name = 'test.engine_' .. engine .. '_' .. tag
    local def = {name = name, id = sid, ver = 0, engine = engine,
        json = json.encode{space_id = sid, ver = 0, engine = engine, marker = 'before'}, lazy = {},
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
            {name = 'bucket_id', type = 'unsigned'}, {name = 'c', type = 'integer', is_nullable = true}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
            {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}},
            {name = 'u', unique = true, parts = {{2, 'integer'}}},
            {name = 'c_idx', unique = false, parts = {{field = 4, type = 'integer', is_nullable = true}}}}}
    if with_uix then
        def.uix = {{name = '_tarn_uix_' .. sid .. '_7', id = old_uid, engine = engine,
            format = {{name = 'v', type = 'integer'}, {name = 'id', type = 'unsigned'},
                      {name = 'bucket_id', type = 'unsigned'}},
            indexes = {{name = 'u', unique = true, parts = {{1, 'integer'}}},
                       {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}}}}}
    end
    ok(tarn_create_table(def))
    -- Deliberately make native IDs sparse and the bucket index non-default.
    -- A compact LuaDef.indexes list must not silently renumber this schema.
    local sp = box.space[sid]
    sp.index.bucket_id:drop(); sp.index.u:drop(); sp.index.c_idx:drop()
    sp:create_index('bucket_id', {id = 4, unique = false, parts = {{3, 'unsigned'}}})
    sp:create_index('u', {id = 7, unique = true, parts = {{2, 'integer'}}})
    sp:create_index('c_idx', {id = 11, unique = false,
        parts = {{field = 4, type = 'integer', is_nullable = true}}})
    local t = {name = name, old_id = sid, new_id = new_sid, old_ver = 0, gen = 1,
        owner = 'engine-test-' .. sid, expected = {}, def = def, old_uid = old_uid, new_uid = new_uid}
    local writes = {}
    for n = 1, count do
        local row = {n, n * 10, 1}
        if n % 3 == 1 then row[4] = box.NULL elseif n % 3 == 2 then row[4] = n + 7 end
        t.expected[n] = row; writes[#writes + 1] = {'insert', sid, row}
        if with_uix then writes[#writes + 1] = {'insert', old_uid, {n * 10, n, 1}} end
    end
    ok(change(t, writes)); t.schema = schema(sid); t.original_bytes = mp.encode(rows(sid))
    local nd = table.deepcopy(def)
    nd.id, nd.engine, nd.ver = new_sid, engine == 'memtx' and 'vinyl' or 'memtx', 1
    nd.json = json.encode{space_id = new_sid, ver = 1, engine = nd.engine, marker = 'after'}
    local new_globals, old_globals = {}, {}
    if with_uix then
        local u = table.deepcopy(def.uix[1])
        u.id, u.name, u.engine = new_uid, '_tarn_uix_' .. new_sid .. '_7', nd.engine
        new_globals, old_globals, nd.uix = {u}, {old_uid}, {u}
    end
    t.args = {ver = 1, timeout = 5, def = nd, uix = new_globals, uix_drop = old_globals, lazy_drop = {},
        engine_change = {old_id = sid, old_ver = 0, old_engine = engine, old_name = name}}
    begin_job(t)
    return t
end
local function ready_apply(t)
    ok(call('build', t)); ok(call('prepare', t)); decide(t, 'apply'); ok(call('apply', t))
end

local function exercise(engine)
    local label = function(s) return engine .. ': ' .. s end
    local t = create(engine, 'online', 260)
    local c = net.connect(uri, {user = 'tarn', password = 'tarn', wait_connected = 3})
    assert(c:is_connected()); streams[#streams + 1] = c
    local stream = c:new_stream()
    -- This row is untouched by concurrent writes, avoiding a trivial
    -- overwritten-row conflict as the old-stream invalidation witness.
    ok(stream:call('tarn_select', {t.old_id, 0, {256}, 'EQ', 1, box.NULL, false, false, box.NULL, true}))
    local ai, rid = 'ai:' .. t.old_id, 'rid:' .. t.old_id
    assert(tonumber(tarn_counter_alloc(ai, 100000)) == 1)
    assert(tonumber(tarn_counter_alloc(rid, 200000)) == 1)
    hook('engine_copy', 'hold')
    local building = spawn(function() return call('build', t) end)
    local reached = hook('engine_copy', 'reached')
    check(label('baseline really pauses after a committed page while the old engine is public'),
        reached[2] == 1 and not building.done and box.space[t.name].id == t.old_id
        and box.space[t.name].engine == engine and box.space._tarn_catalog:get{t.name}.def.id == t.old_id)
    -- A uniqueness cycle across copied and not-yet-copied rows: source A
    -- gives up a key that source B takes. A secondary index on an unfinished
    -- shadow would incorrectly reject B against stale shadow A.
    ok(change(t, {{'update', t.old_id, {1}, {{'=', 2, 99901}}}})); t.expected[1][2] = 99901
    ok(change(t, {{'update', t.old_id, {129}, {{'=', 2, 10}}}})); t.expected[129][2] = 10
    ok(change(t, {{'delete', t.old_id, {2}}})); t.expected[2] = nil
    -- As Session::update_row encodes a changed PK: one atomic DELETE+INSERT,
    -- rather than the arithmetic UPDATE opcode for a row at an unchanged PK.
    local moved = table.deepcopy(t.expected[3]); moved[1], moved[2] = 1003, 99903
    ok(change(t, {{'delete', t.old_id, {3}}, {'insert', t.old_id, moved}}))
    t.expected[1003] = moved
    t.expected[3] = nil
    ok(change(t, {{'insert', t.old_id, {1004, 99904, 1}}})); t.expected[1004] = {1004, 99904, 1}
    box.begin{txn_isolation = 'read-confirmed'}
    box.space[t.old_id]:update({4}, {{'=', 2, 99999}})
    box.rollback()
    check(label('committed writers, deletes and PK moves progress during the held copy; rollback stays absent'),
        not building.done and exact(t, t.old_id) and box.space[t.old_id]:get{4}[2] == 40,
        difference(t, t.old_id))
    hook('engine_copy', 'release'); ok(result(building))
    check(label('online baseline tolerates the unique-key cycle without publishing early'),
        box.space[t.name].id == t.old_id and box.space[t.new_id] ~= nil
        and box.space[t.new_id].index[7] == nil and box.space._tarn_catalog:get{t.name}.def.id == t.old_id)
    ok(change(t, {{'update', t.old_id, {5}, {{'=', 2, 99905}}}})); t.expected[5][2] = 99905
    hook('engine_prepared', 'hold')
    local preparing = spawn(function() return call('prepare', t) end)
    reached = hook('engine_prepared', 'reached')
    local frozen_source = mp.encode(rows(t.old_id))
    check(label('PREPARED certifies exact confirmed rows, sparse indexes and physical NULL/missing suffixes'),
        reached[2] == 1 and phase(t).phase == 'prepared' and exact(t, t.new_id)
        and mp.encode(rows(t.new_id)) == frozen_source
        and mp.encode(schema(t.new_id)) == mp.encode(t.schema)
        and box.space[t.new_id].engine == t.args.def.engine)
    local old = change(t, {{'update', t.old_id, {1}, {{'+', 2, 1}}}})
    local future = tarn_autocommit({{'insert', t.new_id, {9000, 90000, 1}}},
        {fence = {t.new_id, t.args.ver}})
    check(label('both stale source writes and unpublished replacement writes fail before effects'),
        old[1] == 'error' and old[2] == 1412 and future[1] == 'error' and future[2] == 1412
        and exact(t, t.old_id) and box.space[t.new_id]:get{9000} == nil, {old, future})
    hook('engine_prepared', 'release'); ok(result(preparing))
    local early = call('apply', t)
    check(label('APPLY without the global decision cannot rename or publish'),
        early[1] ~= 'ok' and early[2] == 1412 and box.space[t.name].id == t.old_id
        and box.space._tarn_catalog:get{t.name}.def.id == t.old_id, early)
    tarn.engine.invalidate(true)
    ok(call('build', t))
    check(label('capture loss after PREPARED preserves its certified image instead of rebuilding'),
        phase(t).phase == 'prepared' and exact(t, t.new_id)
        and mp.encode(schema(t.new_id)) == mp.encode(t.schema))
    decide(t, 'apply')
    local flip = tarn_job_phase(t.owner, 'abort')
    check(label('an APPLY decision cannot be flipped to ABORT'), flip[1] ~= 'ok' and job_phase(t) == 'apply', flip)
    hook('engine_publish', 'hold')
    local applying = spawn(function() return call('apply', t) end)
    reached = hook('engine_publish', 'reached')
    check(label('the native-rename interruption is after names change and before catalog publication'),
        reached[2] == 1 and box.space[t.name].id == t.new_id
        and box.space._tarn_catalog:get{t.name}.def.id == t.old_id and box.space[t.old_id] ~= nil)
    local observed = catalog_row(t)
    check(label('catalog readers bind original SID, JSON and sparse bucket index across native rename'),
        observed and observed[2] == t.old_id and observed[3] == t.def.json and observed[4] == 4, observed)
    interrupt(applying, 'engine_publish')
    check(label('interrupted rename retains both explicitly identified spaces'),
        box.space[t.old_id] ~= nil and box.space[t.new_id] ~= nil
        and box.space._tarn_catalog:get{t.name}.def.id == t.old_id)
    hook('engine_published', 'hold')
    applying = spawn(function() return call('apply', t) end)
    reached = hook('engine_published', 'reached')
    check(label('the cleanup interruption is after catalog commit with the original space still present'),
        reached[2] == 1 and box.space._tarn_catalog:get{t.name}.def.id == t.new_id
        and phase(t).phase == 'published' and box.space[t.old_id] ~= nil
        and box.space._tarn_meta:get{'hold:' .. t.new_id} ~= nil)
    observed = catalog_row(t)
    check(label('catalog publication switches SID and JSON together with the original bucket index'),
        observed and observed[2] == t.new_id and observed[3] == t.args.def.json and observed[4] == 4, observed)
    interrupt(applying, 'engine_published')
    end_job(t) -- A terminal local receipt must permit finishing cleanup without its old job.
    ok(call('apply', t))
    check(label('published replay finishes old-space/hold cleanup even after the job is gone'), clean(t))
    check(label('the replacement has exactly the acknowledged data and original native schema'),
        exact(t, t.new_id) and mp.encode(schema(t.new_id)) == mp.encode(t.schema)
        and box.space[t.name].engine == t.args.def.engine
        and box.space[t.new_id].index[7]:count{10} == 1
        and box.space[t.new_id].index[7]:get{10}[1] == 129)
    local retired_read = tarn_select(t.old_id, 0, {256}, 'EQ', 1, box.NULL, false, true, box.NULL, false)
    check(label('a read with the retired physical SID requests catalog refresh instead of missing data'),
        retired_read[1] == 'error' and retired_read[2] == 1412, retired_read)
    local valid = stream:call('tarn_validate', {'engine-old-stream-' .. t.old_id,
        {{t.old_id, 0, {256}, 1}}, {}, false})
    check(label('an old stream cannot validate through the physical replacement'),
        valid[1] == 'conflict' and valid[2] == 1213, valid)
    pcall(stream.rollback, stream)
    local late_abort = call('abort', t)
    check(label('late ABORT cannot erase a published replacement'),
        late_abort[1] == 'ok' and exact(t, t.new_id) and clean(t), late_abort)
    check(label('legacy ai/rid names at the new SID share reserved counter blocks with the old SID'),
        tonumber(tarn_counter_alloc('ai:' .. t.new_id, 1)) == 100001
        and tonumber(tarn_counter_alloc(ai, 1)) == 100002
        and tonumber(tarn_counter_alloc('rid:' .. t.new_id, 1)) == 200001
        and tonumber(tarn_counter_alloc(rid, 1)) == 200002)
    assert(tarn_counter_bump('ai:' .. t.new_id, 300000))
    check(label('a legacy counter bump through the replacement also advances the original name'),
        tonumber(tarn_counter_alloc(ai, 1)) == 300001)
    ok(tarn_autocommit({{'update', t.new_id, {1}, {{'=', 2, 77777}}}},
        {fence = {t.new_id, t.args.ver}})); t.expected[1][2] = 77777
    local after_write = mp.encode(rows(t.new_id))
    ok(call('apply', t)); ok(call('build', t))
    check(label('terminal replays never recopy over writes made after publication'),
        exact(t, t.new_id) and clean(t) and mp.encode(rows(t.new_id)) == after_write)
    -- A second move intentionally carries no optional counter_id field,
    -- as an older gateway's catalog JSON roundtrip would do.
    local next_def = table.deepcopy(t.args.def)
    next_def.id, next_def.engine, next_def.ver = id(), engine, 2
    next_def.json = json.encode{space_id = next_def.id, ver = 2, engine = engine, marker = 'again'}
    next_def.counter_id = nil
    local twice = {name = t.name, old_id = t.new_id, new_id = next_def.id, old_ver = 1,
        gen = 1, owner = 'engine-test-' .. next_def.id, expected = t.expected, schema = t.schema,
        args = {ver = 2, timeout = 5, def = next_def, uix = {}, uix_drop = {}, lazy_drop = {},
            engine_change = {old_id = t.new_id, old_ver = 1, old_engine = t.args.def.engine, old_name = t.name}}}
    begin_job(twice); ready_apply(twice); end_job(twice)
    check(label('a second physical change without counter_id preserves the canonical allocation history'),
        clean(twice) and exact(twice, twice.new_id)
        and tonumber(tarn_counter_alloc('ai:' .. twice.new_id, 1)) == 300002
        and tonumber(tarn_counter_alloc('ai:' .. twice.old_id, 1)) == 300003
        and tonumber(tarn_counter_alloc(ai, 1)) == 300004
        and tonumber(tarn_counter_alloc('rid:' .. twice.new_id, 1)) == 200003)

    local aborted = create(engine, 'abort', 8)
    local bad_name = tarn_alter_build(aborted.name .. '_wrong', aborted.args, aborted.gen)
    check(label('a mismatched RPC name is refused before any local engine record or shadow exists'),
        bad_name[1] == 'error' and bad_name[2] == 1210 and phase(aborted) == nil
        and box.space[aborted.new_id] == nil and exact(aborted, aborted.old_id)
        and job_phase(aborted) == 'prepare', bad_name)
    local sentinel_hits = 0
    local sentinel = function() sentinel_hits = sentinel_hits + 1 end
    box.space[aborted.old_id]:on_replace(sentinel)
    local original_triggers = #box.space[aborted.old_id]:on_replace()
    ok(call('build', aborted))
    local early_abort = call('abort', aborted)
    check(label('ABORT without a global decision leaves the source and owned shadow intact'),
        early_abort[1] ~= 'ok' and early_abort[2] == 1412 and exact(aborted, aborted.old_id)
        and box.space[aborted.new_id] ~= nil, early_abort)
    decide(aborted, 'abort')
    flip = tarn_job_phase(aborted.owner, 'apply')
    check(label('an ABORT decision cannot be flipped to APPLY'),
        flip[1] ~= 'ok' and job_phase(aborted) == 'abort', flip)
    ok(call('abort', aborted)); end_job(aborted)
    local final_triggers, sentinel_kept = box.space[aborted.old_id]:on_replace(), false
    for _, fn in ipairs(final_triggers) do if fn == sentinel then sentinel_kept = true end end
    check(label('ABORT detaches only its capture trigger and preserves an unrelated source trigger'),
        #final_triggers == original_triggers and sentinel_kept,
        {before = original_triggers, after = #final_triggers, sentinel_kept = sentinel_kept})
    local stale = call('build', aborted)
    check(label('ABORT retains exact source data and late BUILD cannot recreate its shadow'),
        exact(aborted, aborted.old_id) and box.space[aborted.name].id == aborted.old_id
        and mp.encode(rows(aborted.old_id)) == aborted.original_bytes
        and box.space[aborted.new_id] == nil and stale[1] ~= 'ok' and stale[2] == 1412, stale)
    ok(change(aborted, {{'update', aborted.old_id, {1}, {{'+', 2, 1}}}}))
    check(label('old-engine writes and the unrelated trigger resume after completed ABORT'),
        box.space[aborted.old_id]:get{1}[2] == 11 and sentinel_hits > 0)
    if native ~= nil and native ~= '0' and engine == 'memtx' then
        ok(tarn_autocommit({}, {})) -- refresh any invalidated native eligibility caches
        local served, hit = tarn_info().native.writes, sentinel_hits
        local r = c:call('tarn_storage.ts_autocommit',
            {{{'update', aborted.old_id, {1}, {{'+', 2, 1}}}}, {fence = {aborted.old_id, 0}, wb = {1}}})
        check(label('an aborted change still permits an actual served native source write'),
            r[1] == 'ok' and box.space[aborted.old_id]:get{1}[2] == 12
            and tarn_info().native.writes == served + 1 and sentinel_hits > hit, r)
    end

    local loss = create(engine, 'capture_loss', 260)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', loss) end)
    reached = hook('engine_copy', 'reached')
    local before = phase(loss)
    assert(reached[2] == 1 and before.copied > 0 and before.copied < 260)
    tarn.engine.invalidate(true)
    ok(change(loss, {{'update', loss.old_id, {1}, {{'=', 2, 88888}}}})); loss.expected[1][2] = 88888
    hook('engine_copy', 'release'); ok(result(building))
    local after = phase(loss)
    check(label('capture-generation loss recopies already copied rows from confirmed source'),
        after.capture ~= before.capture and after.copied == 260 and exact(loss, loss.new_id)
        and mp.encode(rows(loss.new_id)) == mp.encode(rows(loss.old_id)), {before, after})
    local old_owner = loss.owner; loss.owner = old_owner .. '-takeover'
    local takeover = ok(tarn_job_touch(loss.owner, old_owner)); loss.gen = takeover[2]
    assert(loss.gen > 1)
    jobs[old_owner], jobs[loss.owner] = nil, loss
    ok(call('build', loss))
    stale = call('build', loss, 1)
    check(label('a stale worker cannot change a higher-generation shadow'),
        stale[1] ~= 'ok' and stale[2] == 1412 and phase(loss).gen == loss.gen
        and exact(loss, loss.new_id), stale)
    ok(call('prepare', loss)); decide(loss, 'apply')
    stale = call('apply', loss, 1)
    check(label('a stale finisher cannot use another generation global decision'),
        stale[1] ~= 'ok' and stale[2] == 1412 and box.space[loss.name].id == loss.old_id, stale)
    ok(call('apply', loss)); end_job(loss)
    check(label('the higher-generation copy publishes its complete exact image'), clean(loss) and exact(loss, loss.new_id))

    -- A strong global-unique lookup that MISSES fetches no base row. Its
    -- stream therefore has only the old auxiliary ID in its read set.
    local global = create(engine, 'global_miss', 8, true)
    local aux_stream = c:new_stream()
    local miss_key = 999999
    local miss = aux_stream:call('tarn_select',
        {global.old_uid, 0, {miss_key}, 'EQ', 1, box.NULL, false, true, box.NULL, true})
    check(label('the strong global lookup fixture really reads only a missing auxiliary key'),
        miss[1] == 'ok' and #miss[2] == 0 and box.space[global.old_uid]:len() == 8, miss)
    ok(call('build', global)); ok(call('prepare', global))
    local entries = {}
    for _, row in ipairs(wanted(global)) do entries[#entries + 1] = {row[2], row[1], row[3]} end
    ok(tarn_uix_fill(global.new_uid, entries, global.new_id, global.args.ver, global.gen))
    decide(global, 'apply')
    hook('engine_published', 'hold')
    local publishing = spawn(function() return call('apply', global) end)
    reached = hook('engine_published', 'reached')
    check(label('the global miss is validated before either original strong space is dropped'),
        reached[2] == 1 and box.space[global.old_id] ~= nil and box.space[global.old_uid] ~= nil
        and box.space[global.old_uid].index[0]:get{miss_key} == nil
        and box.space._tarn_catalog:get{global.name}.def.id == global.new_id)
    local aux_valid = aux_stream:call('tarn_validate', {'engine-old-global-stream-' .. global.old_id,
        {{global.old_uid, 0, {miss_key}, 1}}, {}, false})
    check(label('an auxiliary-only old stream cannot validate through global-index rotation'),
        aux_valid[1] == 'conflict' and aux_valid[2] == 1213, aux_valid)
    pcall(aux_stream.rollback, aux_stream)
    hook('engine_published', 'release'); ok(result(publishing)); end_job(global)
    local no_group_holds = confirmed(function()
        return box.space._tarn_meta:get{'hold:' .. global.old_uid} == nil
            and box.space._tarn_meta:get{'hold:' .. global.new_uid} == nil
    end)
    check(label('global rotation cleans original auxiliary space and every strong-group hold'),
        clean(global) and box.space[global.old_uid] == nil and no_group_holds
        and box.space[global.new_uid] ~= nil and box.space[global.new_uid]:len() == 8
        and exact(global, global.new_id))
    local duplicate = tarn_autocommit({{'insert', global.new_uid, {10, 9999, 1}}},
        {fence = {global.new_id, global.args.ver}})
    check(label('the rotated global unique index still rejects a duplicate without changing rows'),
        duplicate[1] == 'duplicate' and duplicate[2] == 1062
        and box.space[global.new_uid]:len() == 8 and exact(global, global.new_id), duplicate)
end

local function cleanup()
    for _, name in ipairs(hooks) do pcall(tarn_test_hook, name, 'release') end
    for _, worker in ipairs(workers) do
        if not worker.joined then
            if fiber.status(worker.fiber) ~= 'dead' then pcall(worker.fiber.cancel, worker.fiber) end
            pcall(worker.fiber.join, worker.fiber)
        end
    end
    for _, t in pairs(jobs) do
        -- Only the exact jobs created above; never recover another transaction.
        local yes, p = pcall(job_phase, t)
        if yes and p == 'apply' then pcall(call, 'apply', t)
        elseif yes and p then
            pcall(tarn_job_phase, t.owner, 'abort'); pcall(call, 'abort', t)
        end
        pcall(tarn_job_end, t.owner)
    end
    for _, c in ipairs(streams) do pcall(c.close, c) end
end
local yes, err = xpcall(function()
    wait('schema and quota ready', function()
        return box.space._tarn_part ~= nil and tarn.lazy.quota.ready() and tarn.engine ~= nil
            and tarn.clock.status().read_ready
            and (native == nil or native == '0' or (tarn_info().native or {}).ready == 1)
    end)
    ok(tarn_bucket_init(16, {{1, 16}}))
    local ni, layout = tarn_info().native
    if native ~= nil and native ~= '0' then
        local ffi = require('ffi')
        layout = tonumber(ffi.cast('struct tarn_ns *', box.func['tarn_storage.ts_state']:call()).layout)
        assert(ni and ni.ready == 1 and layout == tarn.NS_LAYOUT and layout == 13
               and ffi.sizeof('struct tarn_ns') == 392 and ffi.offsetof('struct tarn_ns', 'fallbacks') == 304,
               'actual native ABI13/size392/fallbacks304 required')
    else assert(tarn.cfg.native == false and ni == false, 'explicit Lua mode required') end
    print('IDENTITY ' .. json.encode{version = box.info.version, native = ni, layout = layout,
        sync = tarn.cfg.sync, wal = box.cfg.wal_mode})
    assert(mp.encode(tarn_engine_capabilities()) == mp.encode({'ok', 1}), 'engine capability V1 required')
    exercise('memtx'); exercise('vinyl')
end, debug.traceback)
cleanup()
if native ~= nil and native ~= '0' then
    local errors = tarn_info().stats.lock_copy_errors
    check('native lock copy held the lock index through engine cleanup',
          tarn.cfg.check_locks == true and errors == 0, errors)
end
if not yes then fail = fail + 1; print('  FAIL engine test exception: ' .. tostring(err)) end
print(string.format('%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
