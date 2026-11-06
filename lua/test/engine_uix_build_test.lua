-- The step of an online build of a global unique index that makes it
-- enforcing, as a copy (issue #292): on a cluster that copies its index
-- builds, the SQL server copies the table into a new space whose indexes,
-- the key's local unique index among them, exist before its rows
-- (lua/tarn_engine.lua), rather than build that index in place. The table's
-- writes maintain the building index while the copy runs, through the old
-- space; PREPARE checks, behind its fence, that the index's conflicts on
-- the shard are gone (a.uix_check), or fails with MySQL's 1062; APPLY
-- publishes the index enforcing, keeps its entries, renamed after the new
-- space, and drops its conflicts. Two rows of the shard that share the key
-- fail the copy with 1062 too, with the key. The job's phase is the copy's
-- decision: 'enforcing:apply', or 'enforcing:abort' and the phases after
-- it, which do not change once made. A copy that would check a space it
-- does not drop is refused before it begins.
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

local passed, failed, sequence = 0, 0, 95000
local jobs = {}
local function check(label, yes, extra)
    if yes then passed = passed + 1; print('  ok   ' .. label)
    else failed = failed + 1; print('  FAIL ' .. label .. ' ' .. json.encode(extra)) end
end
local function ok(r) assert(type(r) == 'table' and r[1] == 'ok', json.encode(r)); return r end
local function id() sequence = sequence + 1; return sequence end
local function hook(name, action) return ok(tarn_test_hook(name, action)) end
local function same(a, b) return json.encode(a) == json.encode(b) end
local function meta(key) local t = box.space._tarn_meta:get{key}; return t and t.value or nil end
local function exists(sid) return box.space[sid] ~= nil end
-- The pair by which a SQL server says that it maintains building indexes.
local MARK = 0xffffffffffffffffULL
-- Vinyl admission refuses a write while dump or compaction catches up
-- ('throttled', to retry after the delay it hints).
local function write(writes, fence)
    local deadline = fiber.clock() + 30
    repeat
        local r = tarn_autocommit(writes, {fence = fence})
        if type(r) ~= 'table' or r[1] ~= 'throttled' or fiber.clock() > deadline then return r end
        fiber.sleep(math.max(tonumber(r[4]) or 50, 10) / 1000)
    until false
end
local function rows(sid)
    local out = {}
    if box.space[sid] then for _, row in box.space[sid].index[0]:pairs() do out[#out + 1] = row:totable() end end
    return out
end

local FORMAT = {{name = 'id', type = 'unsigned'}, {name = 'h', type = 'string'}, {name = 'bucket_id', type = 'unsigned'}}
local PRIMARY = {name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}}
local BUCKET = {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}}
local H = {field = 2, type = 'string'}
-- A building index's entries ('_tarn_uix_<table>_2', keyed by the key) and
-- conflicts (the same name and 'c', keyed by the key and the row's primary
-- key), as the SQL server describes them.
local function hidden(name, sid, parts)
    return {name = name, id = sid, engine = 'memtx',
        format = {{name = 'h', type = 'string'}, {name = 'id', type = 'unsigned'}, {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'uh', unique = true, parts = parts},
                   {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}}}}
end

-- A table (id, h, bucket_id) of `count` rows, h = 'h<id>', whose global
-- unique index uh on h the first step of an online build has published
-- building, and whose backfill has given every row its entry: {name, sid,
-- eid, cid (the index's entries and conflicts), entries, conflicts, def
-- (the table's definition before the build), engine, rows (id -> h)}.
local function building_table(engine, tag, count)
    local t = {name = 'test.engine_uix_' .. engine .. '_' .. tag, sid = id(), eid = id(), cid = id(), rows = {},
        engine = engine}
    t.def = {name = t.name, id = t.sid, ver = 0, engine = engine, json = '{}', lazy = {}, format = FORMAT,
        indexes = {PRIMARY, BUCKET}}
    ok(tarn_create_table(t.def))
    local key = {field = 1, type = 'string'}
    t.entries = hidden('_tarn_uix_' .. t.sid .. '_2', t.eid, {key})
    t.conflicts = hidden('_tarn_uix_' .. t.sid .. '_2c', t.cid, {key, {field = 2, type = 'unsigned'}})
    t.entries.engine, t.conflicts.engine = engine, engine
    -- (as the building definition's writes: with the fence pair)
    function t.write(writes, fence) return write(writes, fence or {t.sid, 1, 0, MARK}) end
    function t.add(n, h)
        local r = t.write({{'insert', t.sid, {n, h, 1}}, {'insert', t.eid, {h, n, 1}}})
        if r[1] == 'ok' then t.rows[n] = h end
        return r
    end
    for n = 1, count do ok(write({{'insert', t.sid, {n, 'h' .. n, 1}}}, {t.sid, 0})) end
    local def = table.deepcopy(t.def)
    def.ver, def.uix, def.building = 1, {t.entries, t.conflicts}, true
    t.building = {ver = 1, timeout = 5, checks = {}, builds = {}, drop = {}, renames = {}, uix_drop = {},
        uix = {t.entries, t.conflicts}, building = true, def = def}
    ok(tarn_alter_prepare(t.name, t.building, 1)); ok(tarn_alter_apply(t.name, t.building, 1))
    for n = 1, count do
        ok(t.write({{'insert', t.eid, {'h' .. n, n, 1}}})); t.rows[n] = 'h' .. n
    end
    return t
end

-- The online build's job on t at its enforcing step, which copies the
-- table into a new space: {t, name, owner, gen, new_id, args (the copy's
-- step), dropping (the step that drops the index if it fails)}.
local function copy_step(t, tag, adjust)
    local j = {t = t, name = t.name, new_id = id(), gen = 1, owner = 'engine-uix-' .. tag .. '-' .. t.sid}
    local def = table.deepcopy(t.def)
    def.id, def.ver = j.new_id, 2
    def.indexes = {PRIMARY, {name = 'uh', unique = true, parts = {H}}, BUCKET}
    def.uix = {table.deepcopy(t.entries)}
    def.uix[1].name = '_tarn_uix_' .. j.new_id .. '_2'
    j.def = def
    j.args = {ver = 2, timeout = 5, def = def, uix = {}, uix_drop = {t.cid}, uix_check = {t.cid}, lazy_drop = {},
        builds = {{2, 'uh', true, {H}}}, drop = {}, renames = {}, checks = {}, building = true,
        engine_change = {old_id = t.sid, old_ver = 1, old_engine = t.engine, old_name = t.name, rebuild = 'aux',
            durable = true}}
    if adjust then adjust(j.args) end
    local back = table.deepcopy(t.def)
    back.ver = 3
    j.dropping = {ver = 3, timeout = 5, checks = {}, builds = {}, drop = {}, renames = {}, uix = {},
        uix_drop = {t.eid, t.cid}, building = true, def = back}
    ok(tarn_job_begin({owner = j.owner, kind = 'unique_build', space = j.name, names = {j.name}, ver = 1,
        a = {db = 'test', table = j.name, ver = 1, uix_build = {t.building, j.args, j.dropping}, cursors = {},
             version = 2},
        phase = 'enforcing:prepare'}, 0))
    jobs[j.owner] = j
    return j
end
local function call(op, j, args) return _G['tarn_alter_' .. op](j.name, args or j.args, j.gen) end
local function phase(j, p) return tarn_job_phase(j.owner, p) end
local function end_job(j) ok(tarn_job_end(j.owner)); jobs[j.owner] = nil end
local function refused_change(r) return r[1] == 'conflict' and tostring(r[3]):find('immutable', 1, true) ~= nil end
-- t's entries as its rows give them, and as the index's space holds them.
local function wanted(t)
    local out = {}
    for n, h in pairs(t.rows) do out[#out + 1] = {h, n, 1} end
    table.sort(out, function(a, b) return a[1] < b[1] end)
    return out
end
-- t as the first step left it: its space, the index building, both of
-- the index's spaces as they were, nothing of a copy.
local function building_intact(t, j)
    local cat = box.space._tarn_catalog:get{t.name}
    return box.space[t.name].id == t.sid and cat.def.id == t.sid and cat.def.building == true
        and exists(t.cid) and same(rows(t.eid), wanted(t)) and box.space[j.new_id] == nil
        and box.space[t.eid].name == t.entries.name
end

local function exercise(engine)
    local label = function(s) return engine .. ': ' .. s end

    -- The copy while the building index takes the table's writes.
    local t = building_table(engine, 'copy', 200)
    local j = copy_step(t, 'copy')
    hook('engine_copy', 'hold')
    local building = fiber.new(function() return call('build', j) end)
    building:set_joinable(true)
    hook('engine_copy', 'reached')
    -- A row comes in, one takes another value, one goes: with the entries.
    ok(t.add(1001, 'n1001'))
    ok(t.write({{'update', t.sid, {4}, {{'=', 2, 'm4'}}}, {'delete', t.eid, {'h4'}}, {'insert', t.eid, {'m4', 4, 1}}}))
    t.rows[4] = 'm4'
    ok(t.write({{'delete', t.sid, {6}}, {'delete', t.eid, {'h6'}}})); t.rows[6] = nil
    local unmarked = write({{'insert', t.sid, {1002, 'n1002', 1}}}, {t.sid, 1})
    check(label('during the copy the building index takes the writes, from SQL servers that maintain it'),
        same(rows(t.eid), wanted(t)) and unmarked[1] == 'error' and unmarked[2] == 1412, unmarked)
    hook('engine_copy', 'release')
    local joined, built = building:join()
    assert(joined and built[1] == 'ok', json.encode(built))
    -- (the coordinator's catch-up round)
    ok(call('build', j))
    ok(call('prepare', j))
    local fenced = t.write({{'insert', t.sid, {1003, 'n1003', 1}}})
    local early = call('apply', j)
    check(label('PREPARE fences the table, and APPLY waits for the job to decide'),
        meta('engine:' .. t.sid).phase == 'prepared' and fenced[1] == 'error' and fenced[2] == 1412
        and early[1] ~= 'ok' and tostring(early[3]):find('decision', 1, true) ~= nil, {fenced, early})
    ok(phase(j, 'enforcing:apply'))
    local back, onward = phase(j, 'enforcing:abort'), phase(j, 'dropping:prepare')
    check(label('once the job decided to apply the copy, no phase changes that'),
        refused_change(back) and refused_change(onward), {back, onward})
    ok(call('apply', j))
    end_job(j)
    local cat = box.space._tarn_catalog:get{t.name}
    check(label('APPLY publishes the index enforcing on the copy: its local unique index, its entries kept, '
        .. 'renamed after the copy, its conflicts dropped'),
        box.space[t.name].id == j.new_id and cat.def.id == j.new_id and cat.def.building == nil
        and box.space[j.new_id].index.uh ~= nil and box.space[j.new_id].index.uh.unique
        and box.space[t.eid] ~= nil and box.space[t.eid].name == j.def.uix[1].name
        and same(rows(t.eid), wanted(t)) and not exists(t.cid) and not exists(t.sid)
        and box.space[j.new_id].index.uh:count() == box.space[j.new_id]:count(),
        {cat.def.building, box.space[t.eid] and box.space[t.eid].name})
    local plain = write({{'insert', j.new_id, {1004, 'n1004', 1}}, {'insert', t.eid, {'n1004', 1004, 1}}},
        {j.new_id, 2})
    local dup = write({{'insert', j.new_id, {1005, 'h1', 1}}}, {j.new_id, 2})
    check(label('then the writes need no mark, and the local index refuses a duplicate of the shard'),
        plain[1] == 'ok' and dup[1] == 'duplicate' and dup[2] == 1062, {plain, dup})
    ok(tarn_drop_table(t.name))

    -- A conflict left, as of a row on another shard: PREPARE fails with
    -- MySQL's 1062; the job aborts the copy, then drops the index.
    t = building_table(engine, 'conflict', 50)
    ok(t.write({{'insert', t.cid, {'h9', 5009, 1}}}))
    j = copy_step(t, 'conflict')
    ok(call('build', j)); ok(call('build', j))
    local r = call('prepare', j)
    check(label("PREPARE refuses a conflict left behind its fence (1062), with its key"),
        r[1] ~= 'ok' and r[2] == 1062 and r[3] == "Duplicate entry 'h9' for key 'uh'", r)
    ok(phase(j, 'enforcing:abort'))
    local forth = phase(j, 'enforcing:apply')
    ok(call('abort', j))
    local resumed = t.add(2001, 'n2001')
    check(label('ABORT leaves the table building as it was, which takes writes again'),
        refused_change(forth) and resumed[1] == 'ok' and building_intact(t, j) and box.space[t.cid]:count() == 1,
        {forth, resumed})
    ok(phase(j, 'dropping:prepare'))
    ok(call('abort', j))
    ok(call('prepare', j, j.dropping)); ok(phase(j, 'dropping:apply')); ok(call('apply', j, j.dropping))
    end_job(j)
    local after = write({{'insert', t.sid, {2002, 'n2002', 1}}}, {t.sid, 3})
    cat = box.space._tarn_catalog:get{t.name}
    check(label('the decision holds through the step that then drops the index, as it was before'),
        not exists(t.eid) and not exists(t.cid) and box.space[t.name].id == t.sid and cat.def.ver == 3
        and cat.def.building == nil and after[1] == 'ok', after)
    ok(tarn_drop_table(t.name))

    -- Two rows of the shard that share the key: the copy fails (1062).
    t = building_table(engine, 'twins', 100)
    j = copy_step(t, 'twins')
    hook('engine_copy', 'hold')
    building = fiber.new(function() return call('build', j) end)
    building:set_joinable(true)
    hook('engine_copy', 'reached')
    -- (h3's entry is row 3's: the new row is a conflict)
    ok(t.write({{'insert', t.sid, {3003, 'h3', 1}}, {'insert', t.cid, {'h3', 3003, 1}}}))
    hook('engine_copy', 'release')
    joined, built = building:join()
    r = built
    if joined and built[1] == 'ok' then r = call('prepare', j) end
    check(label('two rows of the shard that share the key fail the copy (1062), with the key'),
        joined and r[1] ~= 'ok' and r[2] == 1062 and r[3] == "Duplicate entry 'h3' for key 'uh'", {built, r})
    ok(phase(j, 'enforcing:abort')); ok(call('abort', j)); end_job(j)
    check(label('... which ABORT leaves as it was'), building_intact(t, j), box.space[t.name].id)

    ok(tarn_drop_table(t.name))

    -- A copy that would check a space it does not drop: refused (1210).
    t = building_table(engine, 'stray', 10)
    j = copy_step(t, 'stray', function(a) a.uix_check = {t.eid} end)
    r = call('build', j)
    ok(phase(j, 'enforcing:abort')); ok(call('abort', j)); end_job(j)
    check(label('a copy that would check a space it keeps is refused before it begins'),
        r[1] ~= 'ok' and r[2] == 1210 and tostring(r[3]):find('checks only conflicts it drops', 1, true) ~= nil
        and building_intact(t, j), r)
    ok(tarn_drop_table(t.name))
end

local function cleanup()
    pcall(tarn_test_hook, 'engine_copy', 'release')
    for _, j in pairs(jobs) do
        pcall(tarn_job_phase, j.owner, 'enforcing:abort'); pcall(call, 'abort', j); pcall(tarn_job_end, j.owner)
    end
end
local yes, err = xpcall(function()
    local deadline = fiber.clock() + 5
    while not (tarn.lazy.quota.ready() and tarn.engine ~= nil and tarn.clock.status().read_ready)
          and fiber.clock() < deadline do fiber.sleep(0.01) end
    assert(tarn.lazy.quota.ready(), 'lazy admission meter did not reach READY')
    ok(tarn_bucket_init(16, {{1, 16}}))
    assert(mp.encode(tarn_uix_building_copy_capabilities()) == mp.encode({'ok', 1}), 'copy capability')
    for _, engine in ipairs({'memtx', 'vinyl'}) do exercise(engine) end
end, debug.traceback)
cleanup()
if not yes then failed = failed + 1; print('  FAIL engine uix build test exception: ' .. tostring(err)) end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
