-- A copy of a table that makes a lazy index (issue #184): an ALTER that
-- adds a lazy index copies the table into a new space (lua/tarn_engine.lua,
-- rebuild = 'lazy', lazy_made) and keeps the table's other lazy indexes.
-- The new index's generation is built as an ALTER in place builds it,
-- online against the old space before the copy: its descriptor names the
-- old space, whose writes reach it while its baseline scan reads the rows,
-- and publication binds it to the new space with the kept ones (L.rebind).
-- A shard that did not build it online scans the old space behind
-- PREPARE's fence, within alter_check_rows; ABORT retires it and none of
-- the kept ones; a takeover owns it; malformed descriptors are refused
-- before the copy begins.
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

local passed, failed, sequence = 0, 0, 80000
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
local function same(a, b) return json.encode(a) == json.encode(b) end
local BUCKETS = 16
local function bucket(n) return n % BUCKETS + 1 end
local function defs(gen)
    local d = box.space._tarn_lazy_defs:get{gen}
    return d and {base = d.base_id, spec_base = d.spec.base_space_id, phase = d.phase, done = d.done,
                  owner = d.owner} or false
end
-- Whether gen's descriptor names space sid, in phase `phase`, its scan
-- done or not, owned by `owner`.
local function bound_to(gen, sid, phase, done, owner)
    local d = defs(gen)
    return d and d.base == sid and d.spec_base == sid and d.phase == phase and d.done == done and d.owner == owner
end
local function online(gen)
    local o = box.space._tarn_lazy_online:get{gen}
    return o and o.base_id or false
end
local function captured(sid)
    local cap = box.space._tarn_lazy_capture:get{sid}
    local out = {}
    for _, s in ipairs(cap and cap.specs or {}) do out[#out + 1] = s.gen end
    table.sort(out)
    return out
end
local function sorted(list) table.sort(list); return list end

-- A table (id, v, w, bucket_id) of `count` rows with a lazy index on v,
-- built online and published, unless `plain`: {name, sid, gen, def, spec,
-- rows (id -> {v, w}), write}.
local function lazy_table(engine, tag, count, plain)
    local t = {name = 'test.engine_made_' .. engine .. '_' .. tag, sid = id(), gen = id(), rows = {}, engine = engine}
    local def = {name = t.name, id = t.sid, ver = 0, engine = engine, json = '{}',
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'string', is_nullable = true},
                  {name = 'w', type = 'string', is_nullable = true}, {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{field = 4, type = 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{field = 2, type = 'string', is_nullable = true}}}},
        lazy = {}}
    ok(tarn_create_prepare(t.name, {def = def, timeout = 5}, 1)); ok(tarn_create_apply(t.name, {def = def, timeout = 5}))
    function t.write(w, n, sid, ver)
        local deadline = fiber.clock() + 30
        repeat
            local r = tarn_autocommit({w}, {fence = {sid or t.sid, ver or t.def.ver}, wb = {bucket(n)}})
            -- (and one the lazy capture registry refused because a schema change made
            -- it stale while the write yielded: SQL servers retry it, 1412)
            local stale = type(r) == 'table' and r[1] == 'error' and r[2] == 1412
                and tostring(r[3]):find('registry is stale', 1, true) ~= nil
            if (r[1] ~= 'throttled' and not stale) or fiber.clock() > deadline then return r end
            fiber.sleep(math.max(tonumber(r[4]) or 50, 10) / 1000)
        until false
    end
    -- Writes of the table's current definition, kept in t.rows.
    function t.set(n, field, value)
        ok(t.write({'update', t.sid, {n}, {{'=', field == 'v' and 2 or 3, value}}}, n))
        t.rows[n][field] = value
    end
    function t.add(n, v, w)
        ok(t.write({'insert', t.sid, {n, v, w, bucket(n)}}, n)); t.rows[n] = {v = v, w = w}
    end
    function t.remove(n) ok(t.write({'delete', t.sid, {n}}, n)); t.rows[n] = nil end
    t.def = def
    for n = 1, count do t.add(n, 'k' .. (n % 7), 'w' .. (n % 5)) end
    t.last_ver = 0
    if plain then t.gen = nil; return t end
    local spec = {gen = t.gen, base_space_id = t.sid, local_index_id = 2, route_parts = 1,
                  key_fields = {2}, key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1},
                  pk_types = {'unsigned'}, bucket_field = 4}
    local more = copy(def); more.ver, more.lazy = 1, {spec}
    local alt = {ver = 1, timeout = 5, def = more, builds = {}, checks = {}, lazy_drop = {}}
    ok(tarn_lazy_start(t.name, alt, 2))
    repeat local b = ok(tarn_lazy_bootstrap(t.name, alt, 2)) until b[2]
    ok(tarn_alter_prepare(t.name, alt, 2))
    ok(tarn_lazy_bootstrap(t.name, alt, 2))
    assert(wait(function() local s = ok(tarn_lazy_bootstrap_status(t.name, alt, 2)); return s[2] and s[3] == 0 end))
    ok(tarn_lazy_ready(t.name, alt, 2)); ok(tarn_alter_apply(t.name, alt, 2))
    t.def, t.spec, t.last_ver = more, spec, 1
    assert(defs(t.gen).phase == 'ready' and defs(t.gen).base == t.sid, json.encode(defs(t.gen)))
    return t
end
-- The live entries of lazy generation gen, as 'key/id', and what t's rows
-- give for field ('v' or 'w').
local function entries(gen)
    local out = {}
    for _, e in box.space[gen].index.ordered:pairs({true}, {iterator = 'GE'}) do
        if e.live then out[#out + 1] = tostring(e.k1) .. '/' .. e.p1 end
    end
    return sorted(out)
end
local function wanted(t, field)
    local out = {}
    for n, row in pairs(t.rows) do out[#out + 1] = tostring(row[field]) .. '/' .. n end
    return sorted(out)
end
local function current(t, gen, field)
    return wait(function() return depth() == 0 and same(entries(gen), wanted(t, field)) end)
end

-- A copy of t into a new space that adds the lazy index on w, with its
-- local index w (3), and keeps the one on v: its job and descriptor, with
-- j.made the new generation.
local function made_copy(t, tag)
    local j = {t = t, name = t.name, old_id = t.sid, new_id = id(), made = id(), gen = 1,
               owner = 'engine-made-' .. tag .. '-' .. t.sid}
    local nd = copy(t.def)
    t.last_ver = math.max(t.last_ver, t.def.ver) + 1
    nd.id, nd.ver = j.new_id, t.last_ver
    nd.indexes = copy(t.def.indexes)
    local parts = {{field = 3, type = 'string', is_nullable = true}}
    nd.indexes[#nd.indexes + 1] = {name = 'w', unique = false, parts = parts}
    local kept = t.spec and copy(t.spec)
    if kept then kept.base_space_id = j.new_id end
    local made = {gen = j.made, base_space_id = j.new_id, local_index_id = 3, route_parts = 1,
                  key_fields = {3}, key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1},
                  pk_types = {'unsigned'}, bucket_field = 4}
    nd.lazy = kept and {kept, made} or {made}
    j.def, j.kept, j.spec = nd, kept, made
    j.args = {ver = nd.ver, timeout = 5, def = nd, uix = {}, uix_drop = {}, lazy_drop = {},
        builds = {{3, 'w', false, parts}}, drop = {}, renames = {}, checks = {}, lazy_made = {j.made},
        engine_change = {old_id = t.sid, old_ver = t.def.ver, old_engine = t.engine, old_name = t.name, rebuild = 'lazy'}}
    ok(tarn_job_begin({owner = j.owner, kind = 'alter', space = t.name, names = {t.name}, ver = nd.ver,
        a = j.args, phase = 'prepare'}, 0))
    jobs[j.owner] = j
    return j
end
local function call(op, j) return _G['tarn_alter_' .. op](j.name, j.args, j.gen) end
local function lazy(op, j, gen) return _G['tarn_lazy_' .. op](j.name, j.args, gen or j.gen) end
local function decide(j, value) return ok(tarn_job_phase(j.owner, value)) end
local function end_job(j) ok(tarn_job_end(j.owner)); jobs[j.owner] = nil end
local function scanned(j) repeat local b = ok(lazy('bootstrap', j)) until b[2] end
-- The coordinator's steps between PREPARE and APPLY: the scan's batches
-- (behind the fence, when it did not end online), the deliveries, READY.
local function readied(j)
    local b
    for _ = 1, 1000 do
        b = lazy('bootstrap', j)
        if b[1] ~= 'ok' or b[2] then break end
    end
    local drained = wait(function() local s = lazy('bootstrap_status', j); return s[1] == 'ok' and s[2] and s[3] == 0 end)
    return b[1] == 'ok' and b[2] == true and drained and lazy('ready', j)[1] == 'ok'
end
-- After j is published: t lives in j's space under j's definition.
local function moved(j)
    local t = j.t
    t.sid, t.def, t.spec, t.made = j.new_id, j.def, j.kept, j.made
end
-- t's kept generation as it was before a refused or aborted job.
local function intact(t)
    local d = defs(t.gen)
    return d and d.phase == 'ready' and d.done == true and d.base == t.sid and d.spec_base == t.sid
        and same(captured(t.sid), {t.gen})
end
local function retired(j) return defs(j.made) and defs(j.made).phase == 'retired' and online(j.made) == false end
-- A job whose descriptor is refused at every step, ABORT included, as one
-- from an older SQL server: nothing begins.
local function malformed(j, label)
    local s0 = lazy('start', j)
    local r = call('build', j)
    decide(j, 'abort')
    local a = call('abort', j)
    end_job(j)
    local record = box.space._tarn_meta:get{'engine:' .. j.t.sid}
    check(label, s0[1] ~= 'ok' and s0[2] == 1210 and r[1] ~= 'ok' and r[2] == 1210 and a[1] ~= 'ok'
        and a[2] == 1210 and box.space[j.new_id] == nil and defs(j.made) == false and intact(j.t)
        and (record == nil or record.value.ver ~= j.args.ver), {s0, r, a, defs(j.made)})
end
-- A job the copy refuses at BUILD (or the online start, with `op`), which
-- ABORT then ends with the kept generation as it was. The generation the
-- job says it makes is retired, a descriptor written for it if it had none
-- (a late start finds it retired), unless `unnamed`.
local function refused(j, label, op, code, unnamed)
    local r = op and lazy(op, j) or call('build', j)
    decide(j, 'abort')
    local a = call('abort', j)
    end_job(j)
    check(label, r[1] ~= 'ok' and r[2] == (code or 1210) and box.space[j.new_id] == nil and a[1] == 'ok'
        and intact(j.t) and (unnamed and defs(j.made) == false or not unnamed and retired(j))
        and box.space[j.name].id == j.t.sid, {r, a, defs(j.t.gen), defs(j.made)})
end

local function exercise(engine)
    local label = function(s) return engine .. ': ' .. s end
    local t = lazy_table(engine, 'made', 300)
    local j = made_copy(t, 'made')
    -- The online build starts before the copy, against the old space, as an
    -- ALTER in place starts it.
    ok(lazy('start', j))
    local moving = tarn_bucket_send_begin(1, 2, 'm-made-' .. engine, 1)
    check(label('the online start binds the new generation to the old space and captures its writes'),
        bound_to(j.made, t.sid, 'building', false, 1) and online(j.made) == t.sid and same(captured(t.sid), {t.gen})
        and moving[1] == 'timeout' and tostring(moving[3]):find('lazy index build') ~= nil, {defs(j.made), moving})
    tarn.lazy.invalidate(); tarn.lazy.fresh()
    local building = {}
    for _, s in ipairs(tarn.lazy.sources[t.sid].building) do building[#building + 1] = s.gen end
    check(label('the lazy registry captures the old space\'s writes for it, besides the kept one'),
        same(building, {j.made}) and same(sorted({tarn.lazy.sources[t.sid].capture[1].gen,
            tarn.lazy.sources[t.sid].capture[2].gen}), sorted({t.gen, j.made})), tarn.lazy.sources[t.sid])
    -- Rows change while a scan batch is held between its read and its write.
    hook('lazy_bootstrap', 'hold')
    local scanning = fiber.new(function() return lazy('bootstrap', j) end)
    scanning:set_joinable(true)
    hook('lazy_bootstrap', 'reached')
    for n = 1, 30 do t.set(n, 'w', 'x' .. (n % 3)) end
    t.remove(31); t.add(2001, 'n1', 'y1')
    hook('lazy_bootstrap', 'release')
    local joined, res = scanning:join()
    assert(joined and res[1] == 'ok', json.encode(res))
    scanned(j)
    check(label('the scan, and the writes during it, give the new generation every row'),
        current(t, j.made, 'w') and defs(j.made).done == true, {#entries(j.made), #wanted(t, 'w')})
    -- The copy, with writes to the old space throughout.
    hook('engine_copy', 'hold')
    local copying = fiber.new(function() return call('build', j) end)
    copying:set_joinable(true)
    hook('engine_copy', 'reached')
    for n = 40, 60 do t.set(n, 'w', 'z' .. (n % 4)) end
    for n = 61, 70 do t.set(n, 'v', 'q' .. (n % 3)) end
    t.remove(32); t.add(2002, 'n2', 'y2')
    check(label('during the copy both generations take the old space\'s writes'),
        current(t, j.made, 'w') and current(t, t.gen, 'v'), {#entries(j.made), #entries(t.gen)})
    hook('engine_copy', 'release')
    joined, res = copying:join()
    assert(joined and res[1] == 'ok', json.encode(res))
    local record = box.space._tarn_meta:get{'engine:' .. t.sid}.value
    tarn.lazy.invalidate(); tarn.lazy.fresh()
    local pending = tarn.lazy.sources[j.new_id]
    check(label('the copy lists the generation it makes, and the new space is pending until publication'),
        same(record.made_lazy, {j.made}) and same(record.kept_lazy, {t.gen})
        and pending ~= nil and pending.requires == true and pending.ver == -1, {record.made_lazy, pending})
    ok(call('prepare', j))
    check(label('PREPARED, the new generation is still the old space\'s, unpublished'),
        defs(j.made).base == t.sid and defs(j.made).phase == 'building' and same(captured(t.sid), {t.gen})
        and #captured(j.new_id) == 0, defs(j.made))
    check(label('READY marks it ready after its deliveries'), readied(j) and defs(j.made).phase == 'ready'
        and defs(j.made).base == t.sid, defs(j.made))
    decide(j, 'apply')
    -- An APPLY interrupted just before the catalog commit: until the commit
    -- the generation is the old space's, and a registry read refuses the
    -- new space's writes.
    hook('engine_publish', 'hold')
    local applying = fiber.new(function() return call('apply', j) end)
    applying:set_joinable(true)
    hook('engine_publish', 'reached')
    tarn.lazy.invalidate()
    local read_ok = pcall(tarn.lazy.fresh)
    local guarded = tarn.lazy.guard({{'insert', j.new_id, {9001, 'p', 'p', bucket(9001)}}}, {j.new_id, j.def.ver})
    check(label('an APPLY interrupted before the catalog commit leaves both generations the old space\'s'),
        defs(j.made).base == t.sid and defs(j.made).phase == 'ready' and defs(t.gen).base == t.sid
        and #captured(j.new_id) == 0 and read_ok and type(guarded) == 'table' and guarded[2] == 1412,
        {defs(j.made), guarded})
    applying:cancel()
    local done, out = applying:join()
    assert(not done or type(out) ~= 'table' or out[1] ~= 'ok', 'the interrupted APPLY completed')
    hook('engine_publish', 'release')
    ok(call('apply', j)); end_job(j)
    moved(j)
    check(label('publication binds the new generation, with the kept one, to the new space'),
        bound_to(j.made, t.sid, 'ready', true, 1) and defs(t.gen).base == t.sid and same(captured(t.sid), sorted({t.gen, j.made}))
        and #captured(j.old_id) == 0 and online(j.made) == false and tarn.lazy.building() == nil
        and box.space[j.old_id] == nil and box.space[t.name].id == t.sid
        and box.space[t.sid].index.w ~= nil, {defs(j.made), captured(t.sid)})
    check(label('both keep their targets and entries'), current(t, j.made, 'w') and current(t, t.gen, 'v'),
        {#entries(j.made), #wanted(t, 'w')})
    for n = 80, 90 do t.set(n, 'w', 'later') end
    t.set(91, 'v', 'later'); t.remove(92); t.add(2003, 'n3', 'y3')
    local stale = t.write({'update', j.old_id, {93}, {{'=', 3, 'old'}}}, 93, j.old_id, j.def.ver - 1)
    check(label('later writes to the new space reach both, and the old definition\'s are refused'),
        current(t, j.made, 'w') and current(t, t.gen, 'v') and stale[1] == 'error' and stale[2] == 1412, stale)
    local export = tarn_schema_export()
    local bases = {}
    for _, s in ipairs(export[1] == 'ok' and type(export[2][1]) == 'table' and export[2][1].lazy_v1
                       and export[2][1].lazy_v1.active or {}) do bases[s.gen] = s.base_space_id end
    check(label('a schema export takes both as the new space\'s'),
        bases[t.gen] == t.sid and bases[j.made] == t.sid, {export[1], bases})

    -- A table's first lazy index: the copy keeps none, and its new space is
    -- pending until publication all the same.
    local p = lazy_table(engine, 'first', 60, true)
    local q = made_copy(p, 'first')
    ok(lazy('start', q)); scanned(q); ok(call('build', q))
    tarn.lazy.invalidate(); tarn.lazy.fresh()
    pending = tarn.lazy.sources[q.new_id]
    check(label('a copy that makes a table\'s first lazy index has its new space pending until publication'),
        pending ~= nil and pending.requires == true and pending.ver == -1, pending)
    ok(call('prepare', q))
    local first = readied(q)
    decide(q, 'apply'); ok(call('apply', q)); end_job(q); moved(q)
    p.set(1, 'w', 'first')
    check(label('and publishes it, bound to the new space, with every row'),
        first and bound_to(q.made, p.sid, 'ready', true, 1) and same(captured(p.sid), {q.made})
        and current(p, q.made, 'w'), {defs(q.made), captured(p.sid)})

    -- ABORT of such a copy retires the generation it made, and none other:
    -- after its copy, and before it, once the online build began.
    local u = lazy_table(engine, 'aborted', 100)
    local k = made_copy(u, 'aborted')
    ok(lazy('start', k)); scanned(k)
    ok(call('build', k)); ok(call('prepare', k)); decide(k, 'abort'); ok(call('abort', k)); end_job(k)
    for n = 1, 10 do u.set(n, 'v', 'x') end
    check(label('ABORT retires the generation the copy made, and leaves the kept one taking writes'),
        retired(k) and tarn.lazy.building() == nil and intact(u) and box.space[k.new_id] == nil
        and current(u, u.gen, 'v'), {defs(k.made), defs(u.gen)})
    k = made_copy(u, 'early')
    ok(lazy('start', k)); decide(k, 'abort'); ok(call('abort', k))
    local late = lazy('start', k)
    end_job(k)
    check(label('so does an ABORT before the copy, and a late start is refused'),
        retired(k) and intact(u) and late[1] == 'error' and late[2] == 1412
        and tostring(late[3]):find('aborted') ~= nil, {defs(k.made), late})

    -- A shard that did not build it online: PREPARE takes it for the old
    -- space, and the coordinator's batches scan that space behind the
    -- fence, the writes before it included.
    local f = lazy_table(engine, 'fenced', 120)
    local m = made_copy(f, 'fenced')
    ok(call('build', m))
    for n = 1, 20 do f.set(n, 'w', 'f' .. (n % 3)) end
    f.remove(21); f.add(3001, 'n4', 'y4')
    local before = defs(m.made)
    ok(call('prepare', m))
    check(label('without the online build, PREPARE takes the generation for the old space, unscanned'),
        before == false and bound_to(m.made, f.sid, 'building', false, 1) and online(m.made) == false,
        {before, defs(m.made)})
    check(label('the coordinator\'s batches scan it behind the fence, and READY follows'), readied(m), defs(m.made))
    decide(m, 'apply'); ok(call('apply', m)); end_job(m); moved(m)
    check(label('which publishes it with every row'), current(f, m.made, 'w') and defs(m.made).base == f.sid
        and same(captured(f.sid), sorted({f.gen, m.made})), {#entries(m.made), #wanted(f, 'w')})
    -- A coordinator from before such copies sends neither those batches nor
    -- READY: APPLY runs the scan, waiting out the outbox's admission limit
    -- while deliveries go on, then their deliveries, and makes it ready.
    local e = lazy_table(engine, 'unscanned', 120)
    local v = made_copy(e, 'unscanned')
    ok(call('build', v))
    -- (Their deliveries held back, these writes' assignments fill the outbox
    -- past its admission limit, less a scan batch.)
    ok(tarn_lazy_control('pause'))
    for n = 1, 20 do e.set(n, 'v', 'e' .. (n % 3)); e.set(n, 'w', 'e' .. (n % 3)) end
    ok(call('prepare', v)); decide(v, 'apply')
    local entries_limit = tarn.cfg.lazy_outbox_entries
    tarn.cfg.lazy_outbox_entries = 128
    fiber.new(function() fiber.sleep(0.5); tarn_lazy_control('resume') end)
    local began = fiber.clock()
    local published = call('apply', v)
    local waited = fiber.clock() - began
    tarn.cfg.lazy_outbox_entries = entries_limit
    tarn_lazy_control('resume')
    end_job(v); moved(v)
    check(label('without the batches or READY, APPLY scans, waiting out the outbox\'s limit, and publishes it'),
        published[1] == 'ok' and waited >= 0.4 and bound_to(v.made, e.sid, 'ready', true, 1)
        and current(e, v.made, 'w'), {published, waited, defs(v.made)})
    -- That scan counts against alter_check_rows, before the copy begins.
    local g = lazy_table(engine, 'bounded', 50)
    local b = made_copy(g, 'bounded')
    local limit = tarn.cfg.alter_check_rows
    tarn.cfg.alter_check_rows = 10
    local over = call('build', b)
    tarn.cfg.alter_check_rows = limit
    local copied = box.space[b.new_id] ~= nil
    decide(b, 'abort'); ok(call('abort', b)); end_job(b)
    check(label('the copy refuses that scan over alter_check_rows before it copies, and ABORT retires it'),
        over[1] ~= 'ok' and over[2] == 1846 and not copied and retired(b) and intact(g), {over, defs(b.made)})

    -- A coordinator from before such copies sends no READY: APPLY makes the
    -- generation ready itself once its deliveries are acknowledged, and
    -- refuses for now (1213) while they are pending.
    local o = lazy_table(engine, 'unready', 60)
    local z = made_copy(o, 'unready')
    ok(lazy('start', z))
    ok(tarn_lazy_control('pause'))
    scanned(z)
    ok(call('build', z)); ok(call('prepare', z)); decide(z, 'apply')
    local hurried = copy(z.args); hurried.timeout = 0.5
    local early = tarn_alter_apply(z.name, hurried, z.gen)
    local still = defs(z.made)
    tarn_lazy_control('resume')
    local applied = call('apply', z)
    end_job(z); moved(z)
    check(label('without READY, APPLY refuses while the generation\'s deliveries are pending'),
        early[1] ~= 'ok' and early[2] == 1213 and still.phase == 'building' and still.base == z.old_id,
        {early, still})
    check(label('and then makes it ready and publishes it'),
        applied[1] == 'ok' and bound_to(z.made, o.sid, 'ready', true, 1) and current(o, z.made, 'w'),
        {applied, defs(z.made)})

    -- Takeovers: one once the online scan has ended, in the coordinator's
    -- order (its start, then the scan's next call), and one after the copy,
    -- whose PREPARE owns the generation; the previous workers' steps are
    -- refused.
    local h = lazy_table(engine, 'takeover', 80)
    local n = made_copy(h, 'takeover')
    local function take_over()
        local previous = n.owner
        n.owner = previous .. '-takeover'
        n.gen = ok(tarn_job_touch(n.owner, previous))[2]
        jobs[previous], jobs[n.owner] = nil, n
    end
    ok(lazy('start', n)); scanned(n)
    take_over()
    local second = n.gen
    local restarted, finished = lazy('start', n), lazy('bootstrap', n)
    local stale_scan = lazy('bootstrap', n, 1)
    check(label('after the online scan, the next worker\'s start owns the generation, and finds the scan done'),
        second > 1 and restarted[1] == 'ok' and finished[1] == 'ok' and finished[2] == true
        and defs(n.made).owner == second and stale_scan[2] == 1412, {restarted, finished, stale_scan, defs(n.made)})
    ok(call('build', n))
    take_over()
    ok(call('prepare', n))
    local stale_ready = lazy('ready', n, second)
    stale_scan = lazy('bootstrap', n, second)
    check(label('after the copy, the next worker\'s PREPARE owns it; the previous one\'s steps are refused'),
        n.gen > second and defs(n.made).owner == n.gen and stale_scan[2] == 1412 and stale_ready[2] == 1412,
        {n.gen, defs(n.made), stale_scan, stale_ready})
    check(label('and it publishes the generation'), (function()
        if not readied(n) then return false end
        decide(n, 'apply'); ok(call('apply', n)); end_job(n); moved(n)
        return defs(n.made).base == h.sid and defs(n.made).phase == 'ready' and current(h, n.made, 'w')
    end)(), defs(n.made))

    -- Descriptors refused: at the online start, or before the copy begins.
    local r = lazy_table(engine, 'refused', 40)
    local x = made_copy(r, 'ineligible')
    x.args.def.format = copy(x.def.format)
    x.args.def.format[3] = {name = 'w', type = 'unsigned', is_nullable = true}
    refused(x, label('the online start refuses an index on a column the copy retypes (built behind the fence)'),
        'start', 1846)
    x = made_copy(r, 'absent'); x.args.lazy_made = {x.made, id()}
    refused(x, label('a copy that would make a generation its definition does not name is refused'))
    x = made_copy(r, 'kept'); x.args.lazy_made = {r.gen}; x.args.def.lazy = {x.kept}
    refused(x, label('so is one that would make the generation it keeps'), nil, nil, true)
    x = made_copy(r, 'unsaid'); x.args.lazy_made = nil
    refused(x, label('so is one that makes a generation without saying so'), nil, nil, true)
    x = made_copy(r, 'plain'); x.args.engine_change.rebuild = true
    malformed(x, label('one that makes a generation but does not say it keeps the table\'s is refused at every step'))
    x = made_copy(r, 'padded'); x.args.materialize = true
    malformed(x, label('so is one that materializes fields of the rows its scan reads'))
    x = made_copy(r, 'changed')
    ok(lazy('start', x)); x.args.def.lazy[2] = copy(x.spec); x.args.def.lazy[2].local_index_id = 9
    refused(x, label('so is one whose generation differs from the one built online'))
    x = made_copy(r, 'partial')
    local extra = copy(x.spec); extra.gen = id(); extra.local_index_id = 4
    x.args.def.lazy[3] = extra
    refused(x, label('the online start refuses a copy that would make a generation it does not list'), 'start')
    check(label('and builds none of its generations'), defs(extra.gen) == false and online(extra.gen) == false,
        defs(extra.gen))
    x = made_copy(r, 'element'); x.args.lazy_made = {'x'}
    malformed(x, label('one whose generations are not numbers is refused at every step'))
    check(label('writes reach the kept generation after all that'), (function()
        r.set(1, 'v', 'after'); return current(r, r.gen, 'v') and tarn.lazy.building() == nil
    end)())
end

local function cleanup()
    for _, name in ipairs({'engine_copy', 'engine_publish', 'lazy_bootstrap'}) do
        pcall(tarn_test_hook, name, 'release')
    end
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
    assert(mp.encode(tarn_engine_rebuild_lazy_made_capabilities()) == mp.encode({'ok', 1}), 'made lazy capability')
    exercise('memtx'); exercise('vinyl')
end, debug.traceback)
cleanup()
if not yes then failed = failed + 1; print('  FAIL engine made-lazy test exception: ' .. tostring(err)) end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
