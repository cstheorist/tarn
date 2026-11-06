-- A copy that also renames its table (issue #292): an ALTER that builds an
-- index and renames the table copies it into a new space, as other index
-- builds on replicated shards do (lua/tarn_engine.lua), and publishes it
-- under the new name: the catalog's transaction moves the table's entry
-- from the old name to the new one. Until that transaction no catalog entry
-- is under the new name, so APPLY renames the global index spaces the copy
-- keeps after the new space before the new space takes the new name
-- (tla/EngineCutover.tla, BugRenameBeforeKept), repeatably, and the copy's
-- lazy generations are rebound to it as for a copy that keeps the name.
-- ABORT leaves the table under its old name, as it was. A rename to a name
-- another table has, and one that makes a lazy index, are refused before
-- the copy begins.
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

local passed, failed, sequence = 0, 0, 97000
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
local function id() sequence = sequence + 1; return sequence end
local function depth() return tarn_lazy_stats()[2].depth end
local function hook(name, action) return ok(tarn_test_hook(name, action)) end
local function same(a, b) return json.encode(a) == json.encode(b) end
local function sorted(list) table.sort(list); return list end
local function meta(key) local t = box.space._tarn_meta:get{key}; return t and t.value or nil end
local function catalog(name) local t = box.space._tarn_catalog:get{name}; return t and t.def.id or false end
local BUCKETS = 16
local function bucket(n) return n % BUCKETS + 1 end
-- Vinyl admission refuses a write while dump or compaction catches up
-- ('throttled', to retry after the delay it hints).
local function write(writes, fence, buckets)
    local deadline = fiber.clock() + 30
    repeat
        local r = tarn_autocommit(writes, {fence = fence, wb = buckets})
        -- (and one the lazy capture registry refused because a schema change made
        -- it stale while the write yielded: SQL servers retry it, 1412)
        local stale = type(r) == 'table' and r[1] == 'error' and r[2] == 1412
            and tostring(r[3]):find('registry is stale', 1, true) ~= nil
        if type(r) ~= 'table' or (r[1] ~= 'throttled' and not stale) or fiber.clock() > deadline then return r end
        fiber.sleep(math.max(tonumber(r[4]) or 50, 10) / 1000)
    until false
end
local function rows(sid)
    local out = {}
    if box.space[sid] then for _, row in box.space[sid].index[0]:pairs() do out[#out + 1] = row:totable() end end
    return out
end
local function call(op, j) return _G['tarn_alter_' .. op](j.name, j.args, j.gen) end
local function begin(j)
    ok(tarn_job_begin({owner = j.owner, kind = 'alter', space = j.name, names = {j.name, j.new_name},
        ver = j.args.ver, a = j.args, phase = 'prepare'}, 0))
    jobs[j.owner] = j
end
local function decide(j, value) return ok(tarn_job_phase(j.owner, value)) end
local function end_job(j) ok(tarn_job_end(j.owner)); jobs[j.owner] = nil end
-- t's table in this shard's schema export, by the name it is under:
-- {catalog id, ids of its global index spaces}, or false.
local function exported(name)
    local r = tarn_schema_export()
    if r[1] ~= 'ok' then return {r[1], r[2], r[3]} end
    for _, x in ipairs(r[2]) do
        if x.lazy_v1 == nil and x.name == name then
            local uix = {}
            for _, u in ipairs(x.uix) do uix[#uix + 1] = u.id end
            return {x.def.id, sorted(uix)}
        end
    end
    return false
end
-- The job that copies t adding index c_idx on c (field 5, or `field`) and
-- renames it to `to`; `adjust` changes its arguments before it begins.
-- (A version above every earlier job's of t, as the catalog's.)
local function rename_copy(t, tag, to, adjust, field)
    local j = {t = t, name = t.name, new_name = to, old_id = t.sid, new_id = id(), gen = 1,
        owner = 'engine-rename-' .. tag .. '-' .. t.sid}
    t.last_ver = math.max(t.last_ver or 0, t.def.ver) + 1
    local nd = table.deepcopy(t.def)
    nd.name, nd.id, nd.ver = to, j.new_id, t.last_ver
    local part = {field = field or 5, type = field and 'string' or 'integer', is_nullable = true}
    nd.indexes[#nd.indexes + 1] = {name = 'c_idx', unique = false, parts = {part}}
    for _, u in ipairs(nd.uix or {}) do u.name = u.name:gsub('^_tarn_uix_%d+_', '_tarn_uix_' .. j.new_id .. '_') end
    for _, s in ipairs(nd.lazy or {}) do s.base_space_id = j.new_id end
    j.def = nd
    j.args = {ver = nd.ver, timeout = 5, def = nd, uix = {}, uix_drop = {}, lazy_drop = {}, rename = to,
        builds = {{12, 'c_idx', false, {part}}}, drop = {}, renames = {}, checks = {},
        engine_change = {old_id = t.sid, old_ver = t.def.ver, old_engine = t.engine, old_name = t.name,
            rebuild = t.lazy and 'lazy' or 'aux'}}
    if adjust then adjust(j.args) end
    begin(j)
    return j
end

---------------------------------------------------------------------------
-- Global unique indexes: kept, renamed after the copy, found throughout.

-- A table (id, v, w, bucket_id, c) of `count` rows with two global unique
-- indexes, u on v (2) and x on w (3), whose spaces hold {key, id,
-- bucket_id}: {name, sid, def, uid, xid, rows (id -> {v, w})}.
local function global_table(engine, tag, count)
    local t = {name = 'test.engine_rename_' .. engine .. '_' .. tag, sid = id(), uid = id(), xid = id(), rows = {},
        engine = engine}
    local function uix(ix, sid)
        return {name = '_tarn_uix_' .. t.sid .. '_' .. ix, id = sid, engine = engine,
            format = {{name = 'k', type = 'integer'}, {name = 'id', type = 'unsigned'},
                      {name = 'bucket_id', type = 'unsigned'}},
            indexes = {{name = 'k', unique = true, parts = {{1, 'integer'}}},
                       {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}}}}
    end
    t.def = {name = t.name, id = t.sid, ver = 0, engine = engine, json = '{}', lazy = {},
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'}, {name = 'w', type = 'integer'},
                  {name = 'bucket_id', type = 'unsigned'}, {name = 'c', type = 'integer', is_nullable = true}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{4, 'unsigned'}}},
                   {name = 'u', unique = true, parts = {{2, 'integer'}}},
                   {name = 'x', unique = true, parts = {{3, 'integer'}}}},
        uix = {uix(2, t.uid), uix(3, t.xid)}}
    ok(tarn_create_table(t.def))
    function t.write(writes, sid, ver) return write(writes, {sid or t.sid, ver or t.def.ver}) end
    function t.add(n, v, w, sid, ver)
        local r = t.write({{'insert', sid or t.sid, {n, v, w, 1}}, {'insert', t.uid, {v, n, 1}},
            {'insert', t.xid, {w, n, 1}}}, sid, ver)
        if r[1] == 'ok' then t.rows[n] = {v, w} end
        return r
    end
    for n = 1, count do ok(t.add(n, n * 10, n * 100)) end
    return t
end
-- The entries t's rows give the global index on field k (2: v, 3: w).
local function global_entries(t, k)
    local out = {}
    for n, row in pairs(t.rows) do out[#out + 1] = {row[k - 1], n, 1} end
    table.sort(out, function(a, b) return a[1] < b[1] end)
    return out
end
local function globals_intact(t)
    return box.space[t.uid] ~= nil and box.space[t.uid].name == t.def.uix[1].name
        and box.space[t.xid] ~= nil and box.space[t.xid].name == t.def.uix[2].name
        and same(rows(t.uid), global_entries(t, 2)) and same(rows(t.xid), global_entries(t, 3))
end
-- t as it was before an aborted or refused job j: under its old name, in
-- its space, its catalog entry, no table under the new name.
local function intact(t, j)
    return box.space[t.name] ~= nil and box.space[t.name].id == t.sid and catalog(t.name) == t.sid
        and box.space[j.new_name] == nil and not catalog(j.new_name) and box.space[j.new_id] == nil
        and globals_intact(t)
end

local function exercise_global(engine)
    local label = function(s) return engine .. ' global: ' .. s end
    local t = global_table(engine, 'global', 200)
    local to = t.name .. '_renamed'
    local j = rename_copy(t, 'global', to)
    hook('engine_copy', 'hold')
    local building = fiber.new(function() return call('build', j) end)
    building:set_joinable(true)
    hook('engine_copy', 'reached')
    -- Writes during the copy, through both global indexes, under the old name.
    ok(t.add(1001, 10010, 100100))
    ok(t.write({{'update', t.sid, {4}, {{'=', 3, 40040}}}, {'delete', t.xid, {400}}, {'insert', t.xid, {40040, 4, 1}}}))
    t.rows[4][2] = 40040
    ok(t.write({{'delete', t.sid, {6}}, {'delete', t.uid, {60}}, {'delete', t.xid, {600}}})); t.rows[6] = nil
    hook('engine_copy', 'release')
    local joined, built = building:join()
    assert(joined and built[1] == 'ok', json.encode(built))
    ok(call('build', j)); ok(call('prepare', j))
    check(label('the copy prepares under the old name, which the table still has'),
        meta('engine:' .. t.sid).phase == 'prepared' and meta('engine:' .. t.sid).new_name == to
        and box.space[t.name].id == t.sid and catalog(t.name) == t.sid and not catalog(to), meta('engine:' .. t.sid))
    decide(j, 'apply')
    -- Once the copy has the new name, the kept spaces are named after it:
    -- the catalog lists no table under that name until its transaction.
    hook('engine_renamed', 'hold')
    local applying = fiber.new(function() return call('apply', j) end)
    applying:set_joinable(true)
    hook('engine_renamed', 'reached')
    local function named_after_copy()
        return box.space[t.uid].name == '_tarn_uix_' .. j.new_id .. '_2'
            and box.space[t.xid].name == '_tarn_uix_' .. j.new_id .. '_3'
    end
    check(label('the copy has the new name only after the kept spaces are named after it, '
        .. 'while the catalog lists the table under the old name'),
        box.space[to] ~= nil and box.space[to].id == j.new_id and box.space[t.name] == nil
        and named_after_copy() and catalog(t.name) == t.sid and not catalog(to),
        {box.space[t.uid].name, box.space[t.xid].name})
    applying:cancel()
    local joined_, res = applying:join()
    assert(not joined_ or type(res) ~= 'table' or res[1] ~= 'ok', 'the interrupted APPLY completed')
    hook('engine_renamed', 'release')
    hook('engine_publish', 'hold')
    applying = fiber.new(function() return call('apply', j) end)
    applying:set_joinable(true)
    hook('engine_publish', 'reached')
    check(label('an APPLY sent again after one interrupted there goes on from the names it finds'),
        box.space[to].id == j.new_id and named_after_copy() and catalog(t.name) == t.sid and not catalog(to),
        {box.space[t.uid].name, box.space[t.xid].name})
    applying:cancel()
    joined_, res = applying:join()
    assert(not joined_ or type(res) ~= 'table' or res[1] ~= 'ok', 'the interrupted APPLY completed')
    hook('engine_publish', 'release')
    ok(call('apply', j)); end_job(j)
    local old_name, old_sid = t.name, t.sid
    check(label('publication moves the catalog entry to the new name, keeps both global indexes, '
        .. 'and drops the old space'),
        catalog(to) == j.new_id and not catalog(old_name) and box.space[old_name] == nil
        and box.space[old_sid] == nil and box.space[to].index.c_idx ~= nil
        and same(rows(t.uid), global_entries(t, 2)) and same(rows(t.xid), global_entries(t, 3))
        and same(exported(to), {j.new_id, sorted({t.uid, t.xid})}) and exported(old_name) == false,
        {exported(to), exported(old_name)})
    t.name, t.sid, t.def = to, j.new_id, j.def
    local stale = t.add(2001, 20010, 200100, old_sid, 0)
    local later = t.add(2002, 20020, 200200)
    local dup = t.write({{'insert', t.uid, {20020, 2003, 1}}, {'insert', t.sid, {2003, 20020, 200300, 1}},
        {'insert', t.xid, {200300, 2003, 1}}})
    check(label('then the old definition\'s writes retry (1412), and the new one\'s go through both indexes'),
        stale[1] == 'error' and stale[2] == 1412 and later[1] == 'ok' and dup[1] == 'duplicate' and dup[2] == 1062
        and same(rows(t.uid), global_entries(t, 2)), {stale, later, dup})
    ok(tarn_drop_table(t.name))
    check(label('DROP TABLE under the new name drops the kept spaces'),
        box.space[t.uid] == nil and box.space[t.xid] == nil and box.space[t.sid] == nil)

    -- ABORT leaves the table under its old name, as it was.
    local a = global_table(engine, 'aborted', 30)
    local k = rename_copy(a, 'aborted', a.name .. '_renamed')
    ok(call('build', k)); ok(call('prepare', k)); decide(k, 'abort'); ok(call('abort', k)); end_job(k)
    local resumed = a.add(3001, 30010, 300100)
    check(label('ABORT leaves the table under its old name, with its global indexes, taking writes'),
        resumed[1] == 'ok' and intact(a, k), {resumed, catalog(a.name), catalog(k.new_name)})

    -- A rename to the name another table has: refused before the copy.
    local other = global_table(engine, 'other', 2)
    local r = rename_copy(a, 'taken', other.name)
    local res2 = call('build', r)
    decide(r, 'abort'); ok(call('abort', r)); end_job(r)
    check(label('a copy that would rename the table to another table\'s name is refused (1050)'),
        res2[1] ~= 'ok' and res2[2] == 1050 and box.space[a.name].id == a.sid and catalog(other.name) == other.sid
        and box.space[r.new_id] == nil, res2)
    ok(tarn_drop_table(other.name)); ok(tarn_drop_table(a.name))
end

---------------------------------------------------------------------------
-- Lazy indexes: kept, rebound to the copy.

-- A table (id, v, w, bucket_id) of `count` rows with two lazy indexes, on v
-- (local index 2) and on w (3), built online and published: {name, sid,
-- def, gv, gw, rows (id -> {v, w}), lazy}.
local function lazy_table(engine, tag, count)
    local t = {name = 'test.engine_rename_lazy_' .. engine .. '_' .. tag, sid = id(), gv = id(), gw = id(), rows = {},
        engine = engine, lazy = true}
    local def = {name = t.name, id = t.sid, ver = 0, engine = engine, json = '{}',
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'string', is_nullable = true},
                  {name = 'w', type = 'string', is_nullable = true}, {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{field = 4, type = 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{field = 2, type = 'string', is_nullable = true}}},
                   {name = 'w', unique = false, parts = {{field = 3, type = 'string', is_nullable = true}}}},
        lazy = {}}
    ok(tarn_create_prepare(t.name, {def = def, timeout = 5}, 1)); ok(tarn_create_apply(t.name, {def = def, timeout = 5}))
    t.def = def
    function t.write(w, n, sid, ver)
        return write({w}, {sid or t.sid, ver or t.def.ver}, {bucket(n)})
    end
    function t.set(n, field, value)
        ok(t.write({'update', t.sid, {n}, {{'=', field == 'v' and 2 or 3, value}}}, n))
        t.rows[n][field] = value
    end
    function t.add(n, v, w)
        ok(t.write({'insert', t.sid, {n, v, w, bucket(n)}}, n)); t.rows[n] = {v = v, w = w}
    end
    for n = 1, count do t.add(n, 'k' .. (n % 7), 'w' .. (n % 5)) end
    local function spec(gen, ix, field)
        return {gen = gen, base_space_id = t.sid, local_index_id = ix, route_parts = 1, key_fields = {field},
                key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1}, pk_types = {'unsigned'},
                bucket_field = 4}
    end
    local more = table.deepcopy(def); more.ver, more.lazy = 1, {spec(t.gv, 2, 2), spec(t.gw, 3, 3)}
    local alt = {ver = 1, timeout = 5, def = more, builds = {}, checks = {}, lazy_drop = {}}
    ok(tarn_lazy_start(t.name, alt, 2))
    repeat local b = ok(tarn_lazy_bootstrap(t.name, alt, 2)) until b[2]
    ok(tarn_alter_prepare(t.name, alt, 2))
    ok(tarn_lazy_bootstrap(t.name, alt, 2))
    assert(wait(function() local s = ok(tarn_lazy_bootstrap_status(t.name, alt, 2)); return s[2] and s[3] == 0 end))
    ok(tarn_lazy_ready(t.name, alt, 2)); ok(tarn_alter_apply(t.name, alt, 2))
    t.def = more
    return t
end
local function defs(gen)
    local d = box.space._tarn_lazy_defs:get{gen}
    return d and {base = d.base_id, spec_base = d.spec.base_space_id, phase = d.phase, done = d.done} or false
end
local function captured(sid)
    local cap = box.space._tarn_lazy_capture:get{sid}
    local out = {}
    for _, s in ipairs(cap and cap.specs or {}) do out[#out + 1] = s.gen end
    return sorted(out)
end
-- The live entries of generation gen, as 'key/id', and what t's rows give
-- for field ('v' or 'w').
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
-- Whether gen is ready, built and bound to space sid.
local function bound(gen, sid)
    local d = defs(gen)
    return d and d.phase == 'ready' and d.done == true and d.base == sid and d.spec_base == sid
end

local function exercise_lazy(engine)
    local label = function(s) return engine .. ' lazy: ' .. s end
    local t = lazy_table(engine, 'lazy', 120)
    local to = t.name .. '_renamed'
    local j = rename_copy(t, 'lazy', to, nil, 2)
    hook('engine_copy', 'hold')
    local building = fiber.new(function() return call('build', j) end)
    building:set_joinable(true)
    hook('engine_copy', 'reached')
    for n = 1, 20 do t.set(n, 'w', 'x' .. (n % 3)) end
    t.add(1001, 'n1', 'm1')
    hook('engine_copy', 'release')
    local joined, built = building:join()
    assert(joined and built[1] == 'ok', json.encode(built))
    ok(call('build', j)); ok(call('prepare', j)); decide(j, 'apply'); ok(call('apply', j)); end_job(j)
    local old_sid = t.sid
    t.name, t.sid, t.def = to, j.new_id, j.def
    check(label('publication under the new name rebinds both generations, ready, to the copy'),
        catalog(to) == t.sid and box.space[to].id == t.sid and bound(t.gv, t.sid) and bound(t.gw, t.sid)
        and same(captured(t.sid), sorted({t.gv, t.gw})) and #captured(old_sid) == 0,
        {defs(t.gv), defs(t.gw), captured(t.sid)})
    for n = 30, 40 do t.set(n, 'v', 'later') end
    t.add(2001, 'n2', 'm2')
    check(label('later writes under the new name reach both generations'),
        current(t, t.gv, 'v') and current(t, t.gw, 'w'), {#entries(t.gv), #wanted(t, 'v')})

    -- A copy that renames the table and makes a lazy index: refused.
    local made = id()
    local r = rename_copy(t, 'made', to .. '_again', function(a)
        local s = table.deepcopy(a.def.lazy[1]); s.gen, s.local_index_id = made, 12
        a.def.lazy[#a.def.lazy + 1] = s
        a.lazy_made = {made}
    end, 2)
    local res = call('build', r)
    decide(r, 'abort'); ok(call('abort', r)); end_job(r)
    check(label('a copy that would rename the table and make a lazy index is refused'),
        res[1] ~= 'ok' and res[2] == 1210 and tostring(res[3]):find('makes no lazy index', 1, true) ~= nil
        and box.space[to].id == t.sid and bound(t.gv, t.sid), res)
end

local function cleanup()
    for _, name in ipairs({'engine_copy', 'engine_renamed', 'engine_publish'}) do
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
    assert(mp.encode(tarn_engine_rebuild_rename_capabilities()) == mp.encode({'ok', 1}), 'rename capability')
    for _, engine in ipairs({'memtx', 'vinyl'}) do exercise_global(engine); exercise_lazy(engine) end
end, debug.traceback)
cleanup()
if not yes then failed = failed + 1; print('  FAIL engine rename test exception: ' .. tostring(err)) end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
