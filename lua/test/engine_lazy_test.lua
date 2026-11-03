-- A copy of a table with lazy indexes (issue #184): an ALTER that builds a
-- local index copies the table into a new space (lua/tarn_engine.lua,
-- rebuild = 'lazy') and keeps each lazy generation. Its target, entries and
-- deliveries name only the generation; its descriptor and the capture name
-- the table's space, and publication binds them to the new one, in the
-- catalog's transaction (L.rebind). Writes during the copy reach the
-- target, later writes too; ABORT retires nothing; the lazy registry can be
-- rebuilt between the renames and the catalog commit; descriptors that
-- would make, change or leave out a lazy index, or keep and drop the same
-- one, are refused (a copy that drops one: engine_drop_test.lua).
local fio, fiber, json, mp = require('fio'), require('fiber'), require('json'), require('msgpack')
io.stdout:setvbuf('no')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local native = os.getenv('TARN_NATIVE')
if native ~= nil and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
local dir = assert(arg[1], 'private test directory required'); fio.mktree(dir)
box.cfg{work_dir = dir, listen = 'unix/:' .. fio.pathjoin(dir, 'iproto.sock'), memtx_use_mvcc_engine = true,
    memtx_memory = 128 * 1048576, vinyl_memory = 64 * 1048576, vinyl_cache = 0, wal_mode = 'write',
    txn_isolation = 'read-confirmed', election_mode = 'off', replication_synchro_quorum = 1,
    replication_synchro_timeout = .5, log_level = 3}
box.ctl.promote()
local tarn = require('tarn')
tarn.init{shard_id = 1, peers = {}, sync = true, test_hooks = true, check_yields = true, recovery_after = 1000000,
    native = native ~= nil and native ~= '0' and native or false}

local passed, failed, sequence = 0, 0, 70000
local jobs = {}
local function check(label, yes, extra)
    if yes then passed = passed + 1; print('  ok   ' .. label)
    else failed = failed + 1; print('  FAIL ' .. label .. ' ' .. json.encode(extra)) end
end
local function ok(r) assert(type(r) == 'table' and r[1] == 'ok', json.encode(r)); return r end
local function wait(fn, secs)
    local deadline = fiber.clock() + (secs or 10)
    repeat if fn() then return true end; fiber.sleep(0.01) until fiber.clock() > deadline
    return fn()
end
local function copy(t) local o = {}; for k, v in pairs(t) do o[k] = v end; return o end
local function id() sequence = sequence + 1; return sequence end
local function depth() return tarn_lazy_stats()[2].depth end
local function hook(name, action) return ok(tarn_test_hook(name, action)) end
local BUCKETS = 16
local function bucket(n) return n % BUCKETS + 1 end
local function defs(gen)
    local d = box.space._tarn_lazy_defs:get{gen}
    return d and {base = d.base_id, spec_base = d.spec.base_space_id, phase = d.phase, done = d.done} or false
end
local function captured(sid)
    local cap = box.space._tarn_lazy_capture:get{sid}
    local out = {}
    for _, s in ipairs(cap and cap.specs or {}) do out[#out + 1] = s.gen end
    return out
end

-- A table of `count` rows with a lazy index on v, built online and
-- published: {name, sid, gen, def, rows (id -> v), write}.
local function lazy_table(engine, tag, count)
    local t = {name = 'test.engine_lazy_' .. engine .. '_' .. tag, sid = id(), gen = id(), rows = {}, engine = engine}
    local def = {name = t.name, id = t.sid, ver = 0, engine = engine, json = '{}',
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'string', is_nullable = true},
                  {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{field = 2, type = 'string', is_nullable = true}}}},
        lazy = {}}
    ok(tarn_create_prepare(t.name, {def = def, timeout = 5}, 1)); ok(tarn_create_apply(t.name, {def = def, timeout = 5}))
    function t.write(w, n, sid, ver)
        local deadline = fiber.clock() + 30
        repeat
            local r = tarn_autocommit({w}, {fence = {sid or t.sid, ver or t.def.ver}, wb = {bucket(n)}})
            if r[1] ~= 'throttled' or fiber.clock() > deadline then return r end
            fiber.sleep(math.max(tonumber(r[4]) or 50, 10) / 1000)
        until false
    end
    t.def = def
    for n = 1, count do
        ok(t.write({'insert', t.sid, {n, 'k' .. (n % 7), bucket(n)}}, n)); t.rows[n] = 'k' .. (n % 7)
    end
    local spec = {gen = t.gen, base_space_id = t.sid, local_index_id = 2, route_parts = 1,
                  key_fields = {2}, key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1},
                  pk_types = {'unsigned'}, bucket_field = 3}
    local more = copy(def); more.ver, more.lazy = 1, {spec}
    local alt = {ver = 1, timeout = 5, def = more, builds = {}, checks = {}, lazy_drop = {}}
    ok(tarn_lazy_start(t.name, alt, 2))
    repeat local b = ok(tarn_lazy_bootstrap(t.name, alt, 2)) until b[2]
    ok(tarn_alter_prepare(t.name, alt, 2))
    ok(tarn_lazy_bootstrap(t.name, alt, 2))
    assert(wait(function() local s = ok(tarn_lazy_bootstrap_status(t.name, alt, 2)); return s[2] and s[3] == 0 end))
    ok(tarn_lazy_ready(t.name, alt, 2)); ok(tarn_alter_apply(t.name, alt, 2))
    t.def, t.spec = more, spec
    assert(defs(t.gen).phase == 'ready' and defs(t.gen).base == t.sid, json.encode(defs(t.gen)))
    return t
end
-- The live entries of t's lazy index, as 'v/id', and what t's rows give.
local function entries(t)
    local out, target = {}, box.space[t.gen]
    for _, e in target.index.ordered:pairs({true}, {iterator = 'GE'}) do
        if e.live then out[#out + 1] = e.k1 .. '/' .. e.p1 end
    end
    table.sort(out)
    return out
end
local function wanted(t)
    local out = {}
    for n, v in pairs(t.rows) do out[#out + 1] = v .. '/' .. n end
    table.sort(out)
    return out
end
local function current(t)
    return wait(function() return depth() == 0 and json.encode(entries(t)) == json.encode(wanted(t)) end)
end
-- What a schema export says of t's lazy generation: the space its active
-- descriptor names, or the export's error (it refuses a generation bound to
-- another space than its table's).
local function exported(t)
    local r = tarn_schema_export()
    if r[1] ~= 'ok' then return {r[1], r[2]} end
    local meta = type(r[2][1]) == 'table' and r[2][1].lazy_v1
    for _, s in ipairs(meta and meta.active or {}) do
        if s.gen == t.gen then return s.base_space_id end
    end
    return false
end

-- A copy of t into a new space that adds the local index vb (3) on (v,
-- bucket_id) and keeps the lazy one: its job and descriptor.
local function rebuild(t, tag)
    local j = {t = t, name = t.name, old_id = t.sid, new_id = id(), gen = 1, owner = 'engine-lazy-' .. tag .. '-' .. t.sid}
    local nd = copy(t.def)
    -- (A job's version is above every earlier job's, as the catalog's.)
    t.last_ver = math.max(t.last_ver or 0, t.def.ver) + 1
    nd.id, nd.ver = j.new_id, t.last_ver
    nd.indexes = copy(t.def.indexes)
    local parts = {{field = 2, type = 'string', is_nullable = true}, {field = 3, type = 'unsigned'}}
    nd.indexes[#nd.indexes + 1] = {name = 'vb' .. nd.ver, unique = false, parts = parts}
    local spec = copy(t.spec); spec.base_space_id = j.new_id
    nd.lazy = {spec}
    j.def, j.spec = nd, spec
    j.args = {ver = nd.ver, timeout = 5, def = nd, uix = {}, uix_drop = {}, lazy_drop = {},
        builds = {{2 + nd.ver, 'vb' .. nd.ver, false, parts}}, drop = {}, renames = {},
        engine_change = {old_id = t.sid, old_ver = t.def.ver, old_engine = t.engine, old_name = t.name, rebuild = 'lazy'}}
    ok(tarn_job_begin({owner = j.owner, kind = 'alter', space = t.name, names = {t.name}, ver = nd.ver,
        a = j.args, phase = 'prepare'}, 0))
    jobs[j.owner] = j
    return j
end
local function call(op, j) return _G['tarn_alter_' .. op](j.name, j.args, j.gen) end
local function decide(j, value) return ok(tarn_job_phase(j.owner, value)) end
local function end_job(j) ok(tarn_job_end(j.owner)); jobs[j.owner] = nil end
-- After j is published: t lives in j's space under j's definition.
local function moved(j)
    local t = j.t
    t.sid, t.def, t.spec = j.new_id, j.def, j.spec
end
-- t's lazy generation as it was before a refused or aborted job.
local function intact(t)
    local d = defs(t.gen)
    return d and d.phase == 'ready' and d.done == true and d.base == t.sid and d.spec_base == t.sid
        and json.encode(captured(t.sid)) == json.encode({t.gen})
end
-- A job the copy refuses at BUILD, which ABORT then ends with the lazy
-- generation as it was.
local function refused(j, label)
    local r = call('build', j)
    decide(j, 'abort')
    local a = call('abort', j)
    end_job(j)
    check(label, r[1] ~= 'ok' and r[2] == 1210 and box.space[j.new_id] == nil and a[1] == 'ok' and intact(j.t)
        and box.space[j.name].id == j.t.sid, {r, a, defs(j.t.gen)})
end

local function exercise(engine)
    local label = function(s) return engine .. ': ' .. s end
    local t = lazy_table(engine, 'kept', 300)
    local target = box.space[t.gen]
    local j = rebuild(t, 'kept')
    hook('engine_copy', 'hold')
    local building = fiber.new(function() return call('build', j) end)
    building:set_joinable(true)
    hook('engine_copy', 'reached')
    -- Writes during the copy, to the old space: a key changes, rows come
    -- and go. The kept generation captures them as before.
    for n = 1, 40 do
        local v = 'w' .. (n % 5)
        ok(t.write({'update', t.sid, {n}, {{'=', 2, v}}}, n)); t.rows[n] = v
    end
    ok(t.write({'delete', t.sid, {41}}, 41)); t.rows[41] = nil
    ok(t.write({'insert', t.sid, {1001, 'n1', bucket(1001)}}, 1001)); t.rows[1001] = 'n1'
    check(label('during the copy the generation stays bound to the old space, and takes its writes'),
        current(t) and defs(t.gen).base == t.sid and json.encode(captured(t.sid)) == json.encode({t.gen}),
        {defs(t.gen), #entries(t), #wanted(t)})
    hook('engine_copy', 'release')
    local joined, built = building:join()
    assert(joined and built[1] == 'ok', json.encode(built))
    ok(call('prepare', j))
    check(label('PREPARED neither holds nor fences the kept generation, nor rebinds it'),
        box.space._tarn_meta:get{'hold:' .. t.gen} == nil and box.space._tarn_meta:get{'fence:' .. t.gen} == nil
        and defs(t.gen).base == t.sid and #(box.space._tarn_meta:get{'engine:' .. t.sid}.value.kept_lazy) == 1,
        defs(t.gen))
    decide(j, 'apply')
    -- Between the renames and the catalog commit the table's name is the
    -- new space's, while the catalog and the capture still name the old one.
    hook('engine_renamed', 'hold')
    local applying = fiber.new(function() return call('apply', j) end)
    applying:set_joinable(true)
    hook('engine_renamed', 'reached')
    tarn.lazy.invalidate()
    local rebuilt, err = pcall(tarn.lazy.fresh)
    check(label('the lazy registry can be rebuilt between the renames and the catalog commit'),
        rebuilt and box.space[t.name].id == j.new_id and box.space._tarn_catalog:get{t.name}.def.id == t.sid,
        tostring(err))
    -- That APPLY is interrupted there, and the next just before the catalog
    -- commit: until the commit, the generation is the old space's.
    local function interrupt(f, name)
        f:cancel()
        -- (APPLY reports the cancellation as its error, or raises it)
        local joined, res = f:join()
        assert(not joined or type(res) ~= 'table' or res[1] ~= 'ok', 'the interrupted APPLY completed')
        hook(name, 'release')
    end
    interrupt(applying, 'engine_renamed')
    hook('engine_publish', 'hold')
    applying = fiber.new(function() return call('apply', j) end)
    applying:set_joinable(true)
    hook('engine_publish', 'reached')
    check(label('an APPLY interrupted before the catalog commit leaves the generation bound to the old space'),
        box.space[t.name].id == j.new_id and box.space._tarn_catalog:get{t.name}.def.id == t.sid
        and defs(t.gen).base == t.sid and json.encode(captured(t.sid)) == json.encode({t.gen})
        and #captured(j.new_id) == 0, {defs(t.gen), captured(t.sid), captured(j.new_id)})
    -- A lazy registry read now, as one read while the catalog's transaction
    -- waits for its quorum and used after it, has the new space pending: it
    -- refuses the new space's writes rather than take them uncaptured.
    tarn.lazy.invalidate()
    local read_ok = pcall(tarn.lazy.fresh)
    local pending = tarn.lazy.sources[j.new_id]
    local guarded = tarn.lazy.guard({{'insert', j.new_id, {9001, 'p', bucket(9001)}}}, {j.new_id, j.def.ver})
    check(label('a lazy registry read before the catalog commit refuses writes to the new space'),
        read_ok and pending ~= nil and pending.requires == true and pending.ver == -1
        and type(guarded) == 'table' and guarded[2] == 1412, {pending, guarded})
    interrupt(applying, 'engine_publish')
    -- Published, before the old space is dropped: with both registries read
    -- again, the new space's writes reach the generation.
    hook('engine_published', 'hold')
    applying = fiber.new(function() return call('apply', j) end)
    applying:set_joinable(true)
    hook('engine_published', 'reached')
    tarn.engine.invalidate(false); tarn.engine.fresh(); tarn.lazy.fresh()
    local early = t.write({'insert', j.new_id, {1003, 'n3', bucket(1003)}}, 1003, j.new_id, j.def.ver)
    hook('engine_published', 'release')
    local applied, res = applying:join()
    assert(applied and res[1] == 'ok', json.encode(res))
    end_job(j)
    if early[1] == 'ok' then t.rows[1003] = 'n3' end
    moved(j)
    check(label('a write between publication and cleanup reaches the generation'), early[1] == 'ok' and current(t),
        {early, #entries(t), #wanted(t)})
    check(label('publication binds the generation, and its capture, to the new space'),
        defs(t.gen).base == t.sid and defs(t.gen).spec_base == t.sid and defs(t.gen).phase == 'ready'
        and json.encode(captured(t.sid)) == json.encode({t.gen}) and #captured(j.old_id) == 0
        and box.space[j.old_id] == nil and box.space[t.name].id == t.sid and box.space[t.gen] == target,
        {defs(t.gen), captured(t.sid)})
    check(label('the generation keeps its target and entries'), current(t), {#entries(t), #wanted(t)})
    check(label('a schema export takes the generation as the new space\'s'), exported(t) == t.sid, exported(t))
    for n = 50, 60 do
        ok(t.write({'update', t.sid, {n}, {{'=', 2, 'later'}}}, n)); t.rows[n] = 'later'
    end
    ok(t.write({'insert', t.sid, {1002, 'n2', bucket(1002)}}, 1002)); t.rows[1002] = 'n2'
    local stale = t.write({'update', j.old_id, {70}, {{'=', 2, 'old'}}}, 70, j.old_id, j.def.ver - 1)
    check(label('later writes to the new space reach it, and the old space takes none'),
        current(t) and stale[1] == 'error' and stale[2] == 1412, stale)
    -- The rebound generation is kept again by another copy.
    local again = rebuild(t, 'again')
    ok(call('build', again)); ok(call('prepare', again)); decide(again, 'apply'); ok(call('apply', again))
    end_job(again)
    moved(again)
    check(label('a second copy keeps it too, bound to its own space'),
        defs(t.gen).base == t.sid and json.encode(captured(t.sid)) == json.encode({t.gen}) and current(t)
        and exported(t) == t.sid, {defs(t.gen), exported(t)})
    -- An ALTER in place then, which adds a local index, keeps the generation:
    -- its PREPARE takes it only if it is bound to the table's space.
    local nd = copy(t.def)
    t.last_ver = math.max(t.last_ver or 0, t.def.ver) + 1
    nd.ver, nd.indexes = t.last_ver, copy(t.def.indexes)
    local parts = {{field = 3, type = 'unsigned'}, {field = 1, type = 'unsigned'}}
    nd.indexes[#nd.indexes + 1] = {name = 'bi' .. nd.ver, unique = false, parts = parts}
    local alt = {ver = nd.ver, timeout = 5, def = nd, builds = {{2 + nd.ver, 'bi' .. nd.ver, false, parts}},
        checks = {}, lazy_drop = {}}
    local prepared = tarn_alter_prepare(t.name, alt, 3)
    local applied = prepared[1] == 'ok' and tarn_alter_apply(t.name, alt, 3) or prepared
    if applied[1] == 'ok' then t.def = nd end
    for n = 61, 64 do ok(t.write({'update', t.sid, {n}, {{'=', 2, 'inplace'}}}, n)); t.rows[n] = 'inplace' end
    check(label('an ALTER in place afterwards keeps the generation, bound to the table, taking its writes'),
        prepared[1] == 'ok' and applied[1] == 'ok' and box.space[t.sid].index['bi' .. nd.ver] ~= nil
        and defs(t.gen).base == t.sid and defs(t.gen).phase == 'ready' and current(t),
        {prepared, applied, defs(t.gen)})

    -- ABORT of such a copy retires nothing.
    local u = lazy_table(engine, 'aborted', 100)
    local k = rebuild(u, 'aborted')
    ok(call('build', k)); ok(call('prepare', k)); decide(k, 'abort'); ok(call('abort', k)); end_job(k)
    for n = 1, 10 do ok(u.write({'update', u.sid, {n}, {{'=', 2, 'x'}}}, n)); u.rows[n] = 'x' end
    check(label('ABORT leaves the generation ready, bound to the table\'s space, taking its writes'),
        intact(u) and box.space[k.new_id] == nil and current(u), defs(u.gen))

    -- Descriptors refused before any copy begins; ABORT ends each.
    local r = rebuild(u, 'aux'); r.args.engine_change.rebuild = 'aux'
    refused(r, label('a copy of a table with a lazy index that does not say it keeps it is refused'))
    r = rebuild(u, 'omitted'); r.args.engine_change.rebuild, r.args.def.lazy = true, {}
    refused(r, label('so is one that leaves the index out'))
    r = rebuild(u, 'left'); r.args.def.lazy = {}
    refused(r, label('a copy that would leave a lazy index out is refused'))
    r = rebuild(u, 'new'); r.args.def.lazy = {r.spec, copy(r.spec)}; r.args.def.lazy[2].gen = id()
    refused(r, label('a copy that would make a lazy index is refused'))
    r = rebuild(u, 'dropped'); r.args.lazy_drop = {u.gen}
    refused(r, label('a copy that would keep and drop the same lazy index is refused'))
    r = rebuild(u, 'changed'); r.args.def.lazy = {copy(r.spec)}; r.args.def.lazy[1].local_index_id = 9
    refused(r, label('a copy that would change a lazy index is refused'))
    r = rebuild(u, 'unbound'); r.args.def.lazy = {copy(r.spec)}; r.args.def.lazy[1].base_space_id = u.sid
    refused(r, label('a copy whose lazy index would still name the old space is refused'))
    check(label('writes reach the generation after all that'), (function()
        ok(u.write({'update', u.sid, {11}, {{'=', 2, 'y'}}}, 11)); u.rows[11] = 'y'
        return current(u)
    end)())
end

local function cleanup()
    for _, name in ipairs({'engine_copy', 'engine_renamed'}) do pcall(tarn_test_hook, name, 'release') end
    for _, j in pairs(jobs) do
        pcall(tarn_job_phase, j.owner, 'abort'); pcall(call, 'abort', j); pcall(tarn_job_end, j.owner)
    end
end
local yes, err = xpcall(function()
    local deadline = fiber.clock() + 5
    while not (tarn.lazy.quota.ready() and tarn.engine ~= nil and tarn.clock.status().read_ready)
          and fiber.clock() < deadline do fiber.sleep(0.01) end
    assert(tarn.lazy.quota.ready(), 'lazy admission meter did not reach READY')
    ok(tarn_bucket_init(BUCKETS, {{1, BUCKETS}}))
    assert(mp.encode(tarn_engine_rebuild_lazy_capabilities()) == mp.encode({'ok', 1}), 'lazy rebuild capability')
    exercise('memtx'); exercise('vinyl')
end, debug.traceback)
cleanup()
if not yes then failed = failed + 1; print('  FAIL engine lazy test exception: ' .. tostring(err)) end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
