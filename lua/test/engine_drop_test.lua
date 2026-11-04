-- A copy that drops global or lazy indexes of its table (issue #184): an
-- ALTER that builds a local index and drops a global unique index, or a
-- lazy one, copies the table into a new space (lua/tarn_engine.lua). The
-- dropped index takes the table's writes until PREPARE, which holds its
-- space with the table's; publication retires it (a lazy generation in the
-- catalog's transaction), and cleanup drops a global index's space. The
-- table's other global and lazy indexes are kept as before, a lazy index
-- renewed in the same ALTER is made as before, and ABORT leaves every
-- index as it was. Descriptors that drop an index that is not the table's,
-- keep and drop the same one, or drop one without saying that the table
-- has such indexes, are refused before any copy.
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

local passed, failed, sequence = 0, 0, 90000
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
local function sorted(list) table.sort(list); return list end
local function meta(key) local t = box.space._tarn_meta:get{key}; return t and t.value or nil end
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
local function lazy(op, j, gen) return _G['tarn_lazy_' .. op](j.name, j.args, gen or j.gen) end
local function begin(j)
    ok(tarn_job_begin({owner = j.owner, kind = 'alter', space = j.name, names = {j.name}, ver = j.args.ver,
        a = j.args, phase = 'prepare'}, 0))
    jobs[j.owner] = j
end
local function decide(j, value) return ok(tarn_job_phase(j.owner, value)) end
local function end_job(j) ok(tarn_job_end(j.owner)); jobs[j.owner] = nil end
-- A new job of t's: a version above every earlier job's, as the catalog's.
local function next_ver(t) t.last_ver = math.max(t.last_ver or 0, t.def.ver) + 1; return t.last_ver end
-- t's table in this shard's schema export: {catalog id, ids of its global
-- index spaces, its active lazy generations}, or the export's error.
local function exported(t)
    local r = tarn_schema_export()
    if r[1] ~= 'ok' then return {r[1], r[2], r[3]} end
    local entry, gens = false, {}
    for _, x in ipairs(r[2]) do
        if x.lazy_v1 ~= nil then
            for _, s in ipairs(x.lazy_v1.active) do
                if s.base_space_id == t.sid then gens[#gens + 1] = s.gen end
            end
        elseif x.name == t.name then
            local uix = {}
            for _, u in ipairs(x.uix) do uix[#uix + 1] = u.id end
            entry = {x.def.id, sorted(uix)}
        end
    end
    if entry then entry[3] = sorted(gens) end
    return entry
end
-- A table (id, v, bucket_id) without global or lazy indexes, and a job
-- that copies it adding an index on v: a plain rebuild.
local function plain_copy(engine, tag)
    local t = {name = 'test.engine_drop_' .. engine .. '_' .. tag, sid = id(), engine = engine}
    t.def = {name = t.name, id = t.sid, ver = 0, engine = engine, json = '{}', lazy = {},
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
                  {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}}}}
    ok(tarn_create_table(t.def))
    local j = {t = t, name = t.name, old_id = t.sid, new_id = id(), gen = 1,
        owner = 'engine-drop-' .. tag .. '-' .. t.sid}
    local nd = table.deepcopy(t.def)
    nd.id, nd.ver = j.new_id, 1
    local v_part = {field = 2, type = 'integer'}
    nd.indexes[3] = {name = 'kv', unique = false, parts = {v_part}}
    j.def = nd
    j.args = {ver = 1, timeout = 5, def = nd, uix = {}, uix_drop = {}, lazy_drop = {},
        builds = {{12, 'kv', false, {v_part}}}, drop = {}, renames = {}, checks = {},
        engine_change = {old_id = t.sid, old_ver = 0, old_engine = engine, old_name = t.name, rebuild = true}}
    begin(j)
    return j
end

---------------------------------------------------------------------------
-- Global unique indexes.

-- A table (id, v, w, bucket_id, c) of `count` rows with two global unique
-- indexes, u on v (2) and x on w (3), whose spaces hold {key, id,
-- bucket_id}: {name, sid, def, uid, xid, rows (id -> {v, w})}.
local function global_table(engine, tag, count)
    local t = {name = 'test.engine_drop_' .. engine .. '_' .. tag, sid = id(), uid = id(), xid = id(), rows = {},
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
    assert(box.space[t.sid].index.x.id == 3 and box.space[t.xid] ~= nil and box.space[t.uid] ~= nil)
    function t.write(writes, sid, ver) return write(writes, {sid or t.sid, ver or t.def.ver}) end
    -- A row, through both global indexes (and only u once x is dropped).
    function t.add(n, v, w)
        local writes = {{'insert', t.sid, {n, v, w, 1}}, {'insert', t.uid, {v, n, 1}}}
        if t.def.uix[2] then writes[3] = {'insert', t.xid, {w, n, 1}} end
        local r = t.write(writes)
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
-- A copy of t that adds the local index c_idx on c, keeps u and drops x:
-- the index x (3) of the table and its global index space.
local function drop_global(t, tag)
    local j = {t = t, name = t.name, old_id = t.sid, new_id = id(), gen = 1,
        owner = 'engine-drop-' .. tag .. '-' .. t.sid}
    local nd = table.deepcopy(t.def)
    nd.id, nd.ver = j.new_id, next_ver(t)
    local c_part = {field = 5, type = 'integer', is_nullable = true}
    nd.indexes = {nd.indexes[1], nd.indexes[2], nd.indexes[3], {name = 'c_idx', unique = false, parts = {c_part}}}
    nd.uix = {table.deepcopy(t.def.uix[1])}
    nd.uix[1].name = '_tarn_uix_' .. j.new_id .. '_2'
    j.def, j.uix_new = nd, nd.uix[1].name
    j.args = {ver = nd.ver, timeout = 5, def = nd, uix = {}, uix_drop = {t.xid}, lazy_drop = {},
        builds = {{12, 'c_idx', false, {c_part}}}, drop = {3}, renames = {}, checks = {},
        engine_change = {old_id = t.sid, old_ver = t.def.ver, old_engine = t.engine, old_name = t.name,
            rebuild = 'aux'}}
    begin(j)
    return j
end
-- t's global indexes as they were before a refused or aborted job.
local function globals_intact(t)
    return box.space[t.uid] ~= nil and box.space[t.uid].name == t.def.uix[1].name
        and box.space[t.xid] ~= nil and box.space[t.xid].name == t.def.uix[2].name
        and same(rows(t.uid), global_entries(t, 2)) and same(rows(t.xid), global_entries(t, 3))
        and meta('hold:' .. t.xid) == nil and meta('fence:' .. t.xid) == nil
end
-- A job the copy refuses at BUILD, which ABORT then ends with the table and
-- every global index as it was.
local function refused(j, label, intact)
    local r = call('build', j)
    decide(j, 'abort')
    local a = call('abort', j)
    end_job(j)
    check(label, r[1] ~= 'ok' and r[2] == 1210 and box.space[j.new_id] == nil and a[1] == 'ok' and intact()
        and box.space[j.name].id == j.t.sid, {r, a})
end

local function exercise_global(engine)
    local label = function(s) return engine .. ' global: ' .. s end
    local t = global_table(engine, 'global', 200)
    local j = drop_global(t, 'global')
    hook('engine_copy', 'hold')
    local building = fiber.new(function() return call('build', j) end)
    building:set_joinable(true)
    hook('engine_copy', 'reached')
    -- Writes during the copy go through both indexes: a row comes in, one
    -- takes a new w, one goes, and x refuses a duplicate.
    ok(t.add(1001, 10010, 100100))
    ok(t.write({{'update', t.sid, {4}, {{'=', 3, 40040}}}, {'delete', t.xid, {400}}, {'insert', t.xid, {40040, 4, 1}}}))
    t.rows[4][2] = 40040
    ok(t.write({{'delete', t.sid, {6}}, {'delete', t.uid, {60}}, {'delete', t.xid, {600}}})); t.rows[6] = nil
    local dup = t.write({{'insert', t.xid, {100100, 1002, 1}}})
    check(label('during the copy the dropped index takes the table\'s writes, and refuses a duplicate'),
        same(rows(t.xid), global_entries(t, 3)) and same(rows(t.uid), global_entries(t, 2))
        and dup[1] ~= 'ok' and dup[2] == 1062, dup)
    hook('engine_copy', 'release')
    local joined, built = building:join()
    assert(joined and built[1] == 'ok', json.encode(built))
    ok(call('prepare', j))
    local record = meta('engine:' .. t.sid)
    check(label('PREPARE holds the dropped index\'s space with the table\'s, not the kept one'),
        record.phase == 'prepared' and same(record.old_uix, {t.xid}) and #record.kept_uix == 1
        and record.kept_uix[1].id == t.uid and meta('hold:' .. t.xid) ~= nil
        and meta('fence:' .. t.xid).phase == 'preparing' and meta('hold:' .. t.uid) == nil
        and box.space[t.xid] ~= nil, {record.old_uix, record.kept_uix})
    -- (A write naming only the index's space: the table's fence does not
    -- refuse it.)
    local stale = write({{'insert', t.xid, {99, 99, 1}}}, {t.xid, 0})
    check(label('behind the fence the dropped index takes no write'),
        stale[1] == 'error' and stale[2] == 1412 and box.space[t.xid]:get{99} == nil, stale)
    decide(j, 'apply')
    -- Interrupted before the catalog commit: the dropped index is the
    -- table's still, under its name, with every entry.
    hook('engine_publish', 'hold')
    local applying = fiber.new(function() return call('apply', j) end)
    applying:set_joinable(true)
    hook('engine_publish', 'reached')
    check(label('until the catalog names the copy, the dropped index stays as it was'),
        box.space._tarn_catalog:get{t.name}.def.id == t.sid and box.space[t.xid] ~= nil
        and box.space[t.xid].name == t.def.uix[2].name and same(rows(t.xid), global_entries(t, 3)),
        exported(t))
    applying:cancel()
    local _, res = applying:join()
    assert(type(res) ~= 'table' or res[1] ~= 'ok', 'the interrupted APPLY completed')
    hook('engine_publish', 'release')
    ok(call('apply', j)); end_job(j)
    local old_def = t.def
    t.sid, t.def = j.new_id, j.def
    check(label('publication keeps u, renamed after the copy, and cleanup drops x\'s space'),
        box.space[t.name].id == t.sid and box.space[t.uid] ~= nil and box.space[t.uid].name == j.uix_new
        and same(rows(t.uid), global_entries(t, 2)) and box.space[t.xid] == nil
        and box.space[old_def.uix[2].name] == nil and box.space[j.old_id] == nil
        and box.space[t.sid].index.x == nil and box.space[t.sid].index.c_idx ~= nil
        and meta('hold:' .. t.xid) == nil and meta('engine:' .. j.old_id).cleaned == true,
        {box.space[t.uid] and box.space[t.uid].name, exported(t)})
    check(label('the table\'s export lists the kept index only'), same(exported(t), {t.sid, {t.uid}, {}}),
        exported(t))
    local gone = write({{'insert', t.xid, {77, 77, 1}}}, {t.xid, 0})
    local later = t.add(2001, 20010, 200100)
    local twice = t.write({{'insert', t.uid, {20010, 2002, 1}}, {'insert', t.sid, {2002, 20010, 200200, 1}}})
    check(label('then a write naming the dropped space retries (1412), and the new definition\'s go through u'),
        gone[1] == 'error' and gone[2] == 1412 and later[1] == 'ok' and twice[1] == 'duplicate'
        and twice[2] == 1062 and same(rows(t.uid), global_entries(t, 2)), {gone, later, twice})
    ok(tarn_drop_table(t.name))
    check(label('DROP TABLE drops the kept index with the table'), box.space[t.uid] == nil and box.space[t.sid] == nil)

    -- ABORT leaves both indexes as they were.
    local a = global_table(engine, 'aborted', 40)
    local k = drop_global(a, 'aborted')
    ok(call('build', k)); ok(call('prepare', k)); decide(k, 'abort'); ok(call('abort', k)); end_job(k)
    local resumed = a.add(3001, 30010, 300100)
    local dup2 = a.write({{'insert', a.xid, {300100, 3002, 1}}})
    check(label('ABORT leaves the index the copy would have dropped, holding and refusing as before'),
        resumed[1] == 'ok' and dup2[2] == 1062 and globals_intact(a) and box.space[k.new_id] == nil
        and box.space[a.name].id == a.sid, {resumed, dup2})

    -- Descriptors refused before any copy begins; ABORT ends each.
    local intact = function() return globals_intact(a) end
    local other = global_table(engine, 'other', 4)
    local r = drop_global(a, 'stray'); r.args.uix_drop = {other.uid}
    refused(r, label('a copy that would drop another table\'s global index is refused'), function()
        return intact() and box.space[other.uid] ~= nil and same(rows(other.uid), global_entries(other, 2))
    end)
    r = drop_global(a, 'base'); r.args.uix_drop = {a.sid}
    refused(r, label('so is one that would drop a space that is not a global index'), intact)
    r = drop_global(a, 'both'); r.args.uix_drop = {a.uid}
    refused(r, label('so is one that would keep and drop the same index'), intact)
    r = drop_global(a, 'twice'); r.args.uix_drop, r.args.def.uix = {a.xid, a.xid}, {}
    refused(r, label('so is one that would drop an index twice and leave the other out'), intact)
    r = drop_global(a, 'left'); r.args.uix_drop = {}
    refused(r, label('so is one that would leave the dropped index out'), intact)
    r = drop_global(a, 'made'); r.args.uix = {table.deepcopy(a.def.uix[2])}
    r.args.uix[1].id, r.args.uix[1].name = id(), '_tarn_uix_' .. r.new_id .. '_12'
    refused(r, label('so is one that would make a global index'), intact)
    -- A rebuild of a table without global indexes, which does not say it has
    -- them, drops none: not another table's.
    local p = plain_copy(engine, 'plain_uix'); p.args.uix_drop = {a.xid}
    refused(p, label('a plain rebuild that would drop a global index is refused'), intact)
    return a
end

---------------------------------------------------------------------------
-- Lazy indexes.

-- A table (id, v, w, bucket_id) of `count` rows with two lazy indexes, on v
-- (local index 2) and on w (3), built online and published: {name, sid,
-- def, gv, gw, sv, sw (their specs), rows (id -> {v, w})}.
local function lazy_table(engine, tag, count)
    local t = {name = 'test.engine_drop_lazy_' .. engine .. '_' .. tag, sid = id(), gv = id(), gw = id(), rows = {},
        engine = engine}
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
    function t.remove(n) ok(t.write({'delete', t.sid, {n}}, n)); t.rows[n] = nil end
    for n = 1, count do t.add(n, 'k' .. (n % 7), 'w' .. (n % 5)) end
    local function spec(gen, ix, field)
        return {gen = gen, base_space_id = t.sid, local_index_id = ix, route_parts = 1, key_fields = {field},
                key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1}, pk_types = {'unsigned'},
                bucket_field = 4}
    end
    t.sv, t.sw = spec(t.gv, 2, 2), spec(t.gw, 3, 3)
    local more = copy(def); more.ver, more.lazy = 1, {t.sv, t.sw}
    local alt = {ver = 1, timeout = 5, def = more, builds = {}, checks = {}, lazy_drop = {}}
    ok(tarn_lazy_start(t.name, alt, 2))
    repeat local b = ok(tarn_lazy_bootstrap(t.name, alt, 2)) until b[2]
    ok(tarn_alter_prepare(t.name, alt, 2))
    ok(tarn_lazy_bootstrap(t.name, alt, 2))
    assert(wait(function() local s = ok(tarn_lazy_bootstrap_status(t.name, alt, 2)); return s[2] and s[3] == 0 end))
    ok(tarn_lazy_ready(t.name, alt, 2)); ok(tarn_alter_apply(t.name, alt, 2))
    t.def, t.last_ver = more, 1
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
-- A copy of t that adds the local index vb on (v, bucket_id), keeps the
-- lazy index on v and drops the one on w: its local index (3) and its
-- generation.
local function drop_lazy(t, tag)
    local j = {t = t, name = t.name, old_id = t.sid, new_id = id(), gen = 1,
        owner = 'engine-drop-' .. tag .. '-' .. t.sid}
    local nd = copy(t.def)
    nd.id, nd.ver = j.new_id, next_ver(t)
    local parts = {{field = 2, type = 'string', is_nullable = true}, {field = 4, type = 'unsigned'}}
    nd.indexes = {t.def.indexes[1], t.def.indexes[2], t.def.indexes[3], {name = 'vb', unique = false, parts = parts}}
    local kept = copy(t.sv); kept.base_space_id = j.new_id
    nd.lazy = {kept}
    j.def, j.kept = nd, kept
    j.args = {ver = nd.ver, timeout = 5, def = nd, uix = {}, uix_drop = {}, lazy_drop = {t.gw},
        builds = {{12, 'vb', false, parts}}, drop = {3}, renames = {}, checks = {},
        engine_change = {old_id = t.sid, old_ver = t.def.ver, old_engine = t.engine, old_name = t.name,
            rebuild = 'lazy'}}
    begin(j)
    return j
end
-- t's lazy indexes as they were before a refused or aborted job.
local function lazy_intact(t)
    return bound(t.gv, t.sid) and bound(t.gw, t.sid) and same(captured(t.sid), sorted({t.gv, t.gw}))
        and meta('hold:' .. t.gw) == nil and meta('fence:' .. t.gw) == nil
end

local function exercise_lazy(engine)
    local label = function(s) return engine .. ' lazy: ' .. s end
    local t = lazy_table(engine, 'lazy', 300)
    local j = drop_lazy(t, 'lazy')
    hook('engine_copy', 'hold')
    local building = fiber.new(function() return call('build', j) end)
    building:set_joinable(true)
    hook('engine_copy', 'reached')
    for n = 1, 30 do t.set(n, 'w', 'x' .. (n % 3)) end
    for n = 31, 40 do t.set(n, 'v', 'y' .. (n % 4)) end
    t.remove(41); t.add(1001, 'n1', 'm1')
    check(label('during the copy both generations stay bound to the old space, and take its writes'),
        current(t, t.gw, 'w') and current(t, t.gv, 'v') and bound(t.gw, t.sid) and bound(t.gv, t.sid),
        {defs(t.gw), #entries(t.gw), #wanted(t, 'w')})
    hook('engine_copy', 'release')
    local joined, built = building:join()
    assert(joined and built[1] == 'ok', json.encode(built))
    ok(call('prepare', j))
    local record = meta('engine:' .. t.sid)
    check(label('PREPARE holds the dropped generation\'s target with the table, not the kept one\'s'),
        record.phase == 'prepared' and same(record.kept_lazy, {t.gv}) and same(record.old_aux, {t.gw})
        and meta('hold:' .. t.gw) ~= nil and meta('hold:' .. t.gv) == nil and bound(t.gw, t.sid),
        {record.kept_lazy, record.old_aux})
    decide(j, 'apply')
    hook('engine_publish', 'hold')
    local applying = fiber.new(function() return call('apply', j) end)
    applying:set_joinable(true)
    hook('engine_publish', 'reached')
    check(label('until the catalog commit, the dropped generation is ready and captured for the old space'),
        bound(t.gw, t.sid) and same(captured(t.sid), sorted({t.gv, t.gw})) and #captured(j.new_id) == 0,
        {defs(t.gw), captured(t.sid)})
    applying:cancel()
    local _, res = applying:join()
    assert(type(res) ~= 'table' or res[1] ~= 'ok', 'the interrupted APPLY completed')
    hook('engine_publish', 'release')
    ok(call('apply', j)); end_job(j)
    local old_sid = t.sid
    t.sid, t.def = j.new_id, j.def
    check(label('publication rebinds the kept generation and retires the dropped one'),
        bound(t.gv, t.sid) and defs(t.gw).phase == 'retired' and defs(t.gw).base == old_sid
        and same(captured(t.sid), {t.gv}) and #captured(old_sid) == 0 and meta('hold:' .. t.gw) == nil
        and box.space[old_sid] == nil and box.space[t.sid].index.w == nil, {defs(t.gv), defs(t.gw)})
    check(label('the table\'s export lists the kept generation only'), same(exported(t), {t.sid, {}, {t.gv}}),
        exported(t))
    local frozen = entries(t.gw)
    for n = 50, 60 do t.set(n, 'v', 'later'); t.set(n, 'w', 'later') end
    t.add(2001, 'n2', 'm2')
    check(label('later writes reach the kept generation, and none the dropped one'),
        current(t, t.gv, 'v') and same(entries(t.gw), frozen), {#entries(t.gv), #wanted(t, 'v')})

    -- ABORT retires nothing.
    local a = lazy_table(engine, 'aborted', 60)
    local k = drop_lazy(a, 'aborted')
    ok(call('build', k)); ok(call('prepare', k)); decide(k, 'abort'); ok(call('abort', k)); end_job(k)
    for n = 1, 10 do a.set(n, 'w', 'z') end
    check(label('ABORT leaves both generations ready, bound to the table and taking its writes'),
        lazy_intact(a) and box.space[k.new_id] == nil and current(a, a.gw, 'w') and current(a, a.gv, 'v'),
        {defs(a.gv), defs(a.gw)})

    -- Descriptors refused before any copy begins; ABORT ends each.
    local intact = function() return lazy_intact(a) end
    local function refused_lazy(r, label_, also)
        local res = call('build', r)
        decide(r, 'abort')
        local ab = call('abort', r)
        end_job(r)
        check(label_, res[1] ~= 'ok' and res[2] == 1210 and box.space[r.new_id] == nil and ab[1] == 'ok'
            and intact() and (also == nil or also()) and box.space[r.name].id == a.sid, {res, ab})
    end
    local other = lazy_table(engine, 'other', 4)
    local r = drop_lazy(a, 'stray'); r.args.lazy_drop = {a.gw, other.gw}
    refused_lazy(r, label('a copy that would drop another table\'s generation is refused'),
        function() return lazy_intact(other) end)
    r = drop_lazy(a, 'both'); r.args.lazy_drop = {a.gv, a.gw}
    refused_lazy(r, label('so is one that would keep and drop the same generation'))
    -- A malformed list or id is refused at every step, ABORT included: the
    -- job's record would name it among the spaces the copy retires. (Also
    -- a lazy_drop without a uix_drop.)
    for _, case in ipairs({{'lazy_drop', {'x'}}, {'uix_drop', {'x'}}, {'lazy_drop', {'x'}, true},
                           {'lazy_drop', box.NULL}, {'uix_drop', 7}}) do
        local field, value, alone = unpack(case)
        r = drop_lazy(a, 'element_' .. field .. '_' .. tostring(value) .. tostring(alone)); r.args[field] = value
        if alone then r.args.uix_drop = nil end
        local built = call('build', r)
        decide(r, 'abort')
        local ab = call('abort', r)
        end_job(r)
        local record = meta('engine:' .. a.sid)
        check(label(string.format('a copy whose %s is %s%s is refused at every step, ABORT included', field,
                json.encode(value), alone and ', with no uix_drop,' or '')),
            built[2] == 1210 and ab[1] ~= 'ok' and ab[2] == 1210 and box.space[r.new_id] == nil and intact()
            and (record == nil or record.ver ~= r.args.ver), {built, ab})
    end
    r = drop_lazy(a, 'left'); r.args.lazy_drop = {}
    refused_lazy(r, label('so is one that would leave the dropped generation out'))
    r = drop_lazy(a, 'unsaid'); r.args.engine_change.rebuild = 'aux'
    refused_lazy(r, label('so is one that drops lazy indexes without saying the table has them'))
    -- A rebuild of a table without lazy indexes, which does not say it has
    -- them, drops none: not another table's generation.
    local pj = plain_copy(engine, 'plain_lazy'); pj.args.lazy_drop = {a.gw}
    local pres = call('build', pj)
    decide(pj, 'abort'); local pab = call('abort', pj); end_job(pj)
    check(label('a plain rebuild that would drop a generation is refused'),
        pres[1] ~= 'ok' and pres[2] == 1210 and pab[1] == 'ok' and intact(), {pres, pab})
    local g = global_table(engine, 'nolazy', 4)
    local gj = drop_global(g, 'nolazy'); gj.args.lazy_drop = {a.gw}
    local res = call('build', gj)
    decide(gj, 'abort'); local ab = call('abort', gj); end_job(gj)
    check(label('a rebuild of a table without lazy indexes that would drop a generation is refused'),
        res[1] ~= 'ok' and res[2] == 1210 and ab[1] == 'ok' and intact() and globals_intact(g), {res, ab})
    -- One of a table without global indexes that would drop another
    -- table's global index space, the same.
    local q = lazy_table(engine, 'noglobal', 4)
    local qj = drop_lazy(q, 'noglobal'); qj.args.uix_drop = {g.xid}
    res = call('build', qj)
    decide(qj, 'abort'); ab = call('abort', qj); end_job(qj)
    check(label('a rebuild of a table without global indexes that would drop a global index is refused'),
        res[1] ~= 'ok' and res[2] == 1210 and ab[1] == 'ok' and lazy_intact(q) and globals_intact(g), {res, ab})
end

-- A copy that renews the lazy index on w: drops its generation and local
-- index, and makes a new one on w (local index 13), built online against
-- the old space before the copy, which keeps the one on v.
local function exercise_renewed(engine)
    local label = function(s) return engine .. ' renewed: ' .. s end
    local t = lazy_table(engine, 'renewed', 200)
    local j = drop_lazy(t, 'renewed')
    end_job(j)
    j.made = id()
    local w_part = {field = 3, type = 'string', is_nullable = true}
    local made = copy(t.sw); made.gen, made.base_space_id, made.local_index_id = j.made, j.new_id, 13
    j.def.indexes[#j.def.indexes + 1] = {name = 'w2', unique = false, parts = {w_part}}
    j.def.lazy = {j.kept, made}
    j.args.builds[2] = {13, 'w2', false, {w_part}}
    j.args.lazy_made = {j.made}
    j.owner = j.owner .. '-made'
    begin(j)
    ok(lazy('start', j))
    repeat local b = ok(lazy('bootstrap', j)) until b[2]
    for n = 1, 20 do t.set(n, 'w', 'r' .. (n % 3)) end
    check(label('the new generation is built online against the old space, beside the one it renews'),
        current(t, j.made, 'w') and current(t, t.gw, 'w') and defs(j.made).base == t.sid
        and bound(t.gw, t.sid), {defs(j.made), defs(t.gw)})
    ok(call('build', j)); ok(call('prepare', j))
    local b
    for _ = 1, 1000 do b = lazy('bootstrap', j); if b[1] ~= 'ok' or b[2] then break end end
    assert(b[1] == 'ok' and b[2] == true, json.encode(b))
    assert(wait(function() local s = lazy('bootstrap_status', j); return s[1] == 'ok' and s[2] and s[3] == 0 end))
    ok(lazy('ready', j))
    decide(j, 'apply'); ok(call('apply', j)); end_job(j)
    local old_sid = t.sid
    t.sid, t.def = j.new_id, j.def
    for n = 21, 30 do t.set(n, 'w', 's' .. (n % 3)) end
    check(label('publication binds the kept and the new generations and retires the old one'),
        bound(t.gv, t.sid) and bound(j.made, t.sid) and defs(t.gw).phase == 'retired'
        and same(captured(t.sid), sorted({t.gv, j.made})) and #captured(old_sid) == 0
        and current(t, j.made, 'w') and current(t, t.gv, 'v') and same(exported(t), {t.sid, {}, sorted({t.gv, j.made})}),
        {defs(j.made), defs(t.gw), exported(t)})
end

local function cleanup()
    for _, name in ipairs({'engine_copy', 'engine_publish'}) do pcall(tarn_test_hook, name, 'release') end
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
    assert(mp.encode(tarn_engine_rebuild_drops_capabilities()) == mp.encode({'ok', 1}), 'drops capability')
    for _, engine in ipairs({'memtx', 'vinyl'}) do
        exercise_global(engine); exercise_lazy(engine); exercise_renewed(engine)
    end
end, debug.traceback)
cleanup()
if not yes then failed = failed + 1; print('  FAIL engine drop test exception: ' .. tostring(err)) end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
