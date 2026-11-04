-- A rebuild that checks rows (issue #184): an ALTER that builds indexes and
-- narrows a column's type, or makes it NOT NULL, copies the table on a
-- replicated cluster (lua/tarn_engine.lua), and checks every row it writes to
-- the new space against the change's checks (a.checks): a page's rows, a
-- pass's rereads and the final pass's. A failing row fails the change with
-- the error the in-place change gives (1265 or 1264), naming the row by its
-- position in the shard's primary-key order, and ABORT leaves the table as
-- it was. A row written during the copy is written again by a pass or the
-- final pass, so every row of a prepared copy was checked at its last write.
local fio, fiber, json, decimal = require('fio'), require('fiber'), require('json'), require('decimal')
io.stdout:setvbuf('no')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local native = os.getenv('TARN_NATIVE')
if native ~= nil and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
local work = assert(arg[1], 'private test directory required'); fio.mktree(work)
box.cfg{work_dir = work, listen = 'unix/:' .. fio.pathjoin(work, 'iproto.sock'), memtx_use_mvcc_engine = true,
    memtx_memory = 128 * 1048576, vinyl_memory = 32 * 1048576, vinyl_cache = 0, wal_mode = 'write',
    txn_isolation = 'read-confirmed', election_mode = 'off', replication_synchro_quorum = 1,
    replication_synchro_timeout = .5, log_level = 3}
box.ctl.promote()
local tarn = require('tarn'); tarn.lazy.paused = true
tarn.init{shard_id = 1, peers = {}, sync = true, test_hooks = true, check_yields = true, check_locks = true,
    recovery_after = 1000000, native = native ~= nil and native ~= '0' and native or false}

local passed, failed, sequence = 0, 0, 94000
local jobs = {}
local function check(label, yes, extra)
    if yes then passed = passed + 1; print('  ok   ' .. label)
    else failed = failed + 1; print('  FAIL ' .. label .. ' ' .. json.encode(extra)) end
end
local function ok(r) assert(type(r) == 'table' and r[1] == 'ok', json.encode(r)); return r end
local function id() sequence = sequence + 1; return sequence end
local function hook(name, action) return ok(tarn_test_hook(name, action)) end
local function confirmed(fn)
    box.begin{txn_isolation = 'read-confirmed'}
    local yes, r = pcall(fn)
    if box.is_in_txn() then box.rollback() end
    if not yes then error(r) end
    return r
end
local function rows(sid)
    return confirmed(function()
        local out = {}
        for _, row in box.space[sid].index[0]:pairs() do out[#out + 1] = json.encode(row:totable()) end
        return out
    end)
end
local function spawn(fn)
    local s = {}
    s.fiber = fiber.new(function() s.ok, s.result = pcall(fn); s.done = true end)
    s.fiber:set_joinable(true)
    return s
end
local function join(s)
    local joined, err = s.fiber:join()
    assert(joined, tostring(err)); assert(s.ok, tostring(s.result))
    return s.result
end
local function write(t, writes)
    local deadline = fiber.clock() + 30
    repeat
        local r = tarn_autocommit(writes, {fence = {t.old_id, 0}})
        -- (and one the lazy capture registry refused because a schema change made
        -- it stale while the write yielded: SQL servers retry it, 1412)
        local stale = type(r) == 'table' and r[1] == 'error' and r[2] == 1412
            and tostring(r[3]):find('registry is stale', 1, true) ~= nil
        if type(r) ~= 'table' or (r[1] ~= 'throttled' and not stale) or fiber.clock() > deadline then return r end
        fiber.sleep(math.max(tonumber(r[4]) or 50, 10) / 1000)
    until false
end
-- Row n's column c (field 4).
local function set_c(t, n, value) return write(t, {{'update', t.old_id, {n}, {{'=', 4, value}}}}) end
local function call(op, t) return _G['tarn_alter_' .. op](t.name, t.args, t.gen) end
local function decide(t, value) return ok(tarn_job_phase(t.owner, value)) end
local function end_job(t) ok(tarn_job_end(t.owner)); jobs[t.owner] = nil end
local function set(knobs)
    local before = {}
    for k, v in pairs(knobs) do before[k] = tarn.cfg[k] == nil and box.NULL or tarn.cfg[k]; tarn.cfg[k] = v end
    return function()
        for k, v in pairs(before) do tarn.cfg[k] = v ~= box.NULL and v or nil end
    end
end

-- The checks of `ALTER TABLE t MODIFY c TINYINT NOT NULL, MODIFY s
-- VARCHAR(3), MODIFY d DECIMAL(4,2)`, as the SQL server sends them: field,
-- column, kind, its bounds, then the column's fill.
local CHECKS = {
    {4, 'c', 'range', -128, 127, box.NULL},
    {4, 'c', 'notnull', box.NULL, box.NULL, box.NULL},
    {5, 's', 'chars', 3, box.NULL, box.NULL},
    {6, 'd', 'digits', 2, box.NULL, box.NULL},
}
-- A table (id, v, bucket_id, c, s, d) of `count` rows within the checks,
-- `odd` changing some of them, and a rebuild that adds an index on c and
-- checks the rows: a copy the SQL server marks durable.
local function table_and_copy(engine, tag, count, odd)
    local t = {old_id = id(), new_id = id(), gen = 1, name = 'test.checks_' .. engine .. '_' .. tag}
    t.owner = 'engine-checks-' .. t.old_id
    local def = {name = t.name, id = t.old_id, ver = 0, engine = engine, json = '{}', lazy = {},
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
            {name = 'bucket_id', type = 'unsigned'}, {name = 'c', type = 'integer', is_nullable = true},
            {name = 's', type = 'string', is_nullable = true}, {name = 'd', type = 'decimal', is_nullable = true}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
            {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}}}}
    ok(tarn_create_table(def))
    local writes = {}
    for n = 1, count do
        local row = {n, n * 10, 1, n % 100, 'r' .. n % 10, decimal.new(n % 10)}
        if odd then odd(n, row) end
        writes[#writes + 1] = {'insert', t.old_id, row}
        if #writes == 200 then ok(write(t, writes)); writes = {} end
    end
    if #writes > 0 then ok(write(t, writes)) end
    local nd = table.deepcopy(def)
    nd.id, nd.ver = t.new_id, 1
    local c_part = {field = 4, type = 'integer', is_nullable = true}
    nd.indexes[#nd.indexes + 1] = {name = 'c_k', unique = false, parts = {c_part}}
    t.args = {ver = 1, timeout = 5, def = nd, uix = {}, uix_drop = {}, lazy_drop = {}, checks = CHECKS,
        builds = {{12, 'c_k', false, {c_part}}}, drop = {}, renames = {},
        engine_change = {old_id = t.old_id, old_ver = 0, old_engine = engine, old_name = t.name, rebuild = true,
            durable = true}}
    ok(tarn_job_begin({owner = t.owner, kind = 'alter', space = t.name, names = {t.name}, ver = 1,
        a = t.args, phase = 'prepare'}, 0))
    jobs[t.owner] = t
    t.before = rows(t.old_id)
    return t
end
-- A copy held after its first page (rows 1 to 4).
local function held_copy(t)
    hook('engine_copy', 'hold')
    local building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    return building
end
-- Whether the copy's new space holds the table's rows, and its index on c
-- every row.
local function image(t)
    local dst = box.space[t.new_id]
    return dst ~= nil and json.encode(rows(t.old_id)) == json.encode(rows(t.new_id))
        and dst.index.c_k:count() == dst.index[0]:count()
end
-- The copy's spaces left on the shard: its new space, and its durable
-- capture's.
local function leftovers(t)
    local out = {}
    if box.space[t.new_id] ~= nil then out[#out + 1] = box.space[t.new_id].name end
    for _, prefix in ipairs({'_tarn_engine_capture_' .. t.old_id .. '_', '_tarn_engine_holes_' .. t.old_id .. '_'}) do
        for _, s in box.space._space.index.name:pairs({prefix}, {iterator = 'GE'}) do
            if s.name:sub(1, #prefix) ~= prefix then break end
            out[#out + 1] = s.name
        end
    end
    return out
end
-- ABORT ends a change that failed, and the table is as it was (`writes`:
-- the rows changed meanwhile), with no copy left.
local function aborted(t, label)
    decide(t, 'abort')
    local ended = call('abort', t)
    end_job(t)
    check(label .. ': ABORT ends it, leaving the table as it was and no copy',
        ended[1] == 'ok' and json.encode(rows(t.old_id)) == json.encode(t.before) and #leftovers(t) == 0,
        {ended, leftovers(t)})
end
local function refused(r, code, text)
    return type(r) == 'table' and r[1] == 'error' and r[2] == code and tostring(r[3]) == text
end

local function exercise(engine)
    local label = function(s) return engine .. ': ' .. s end
    local restore, t, building, built, prepared

    -- Rows within every check copy exactly.
    restore = set{engine_copy_rows = 4, engine_fence_keys = 64}
    t = table_and_copy(engine, 'valid', 40)
    built = call('build', t)
    prepared = call('prepare', t)
    check(label('a copy whose rows pass its checks is exact'),
        built[1] == 'ok' and prepared[1] == 'ok' and image(t), {built, prepared})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- A page's row that fails a check fails the change with the in-place
    -- change's error, naming the row by its position.
    for _, case in ipairs({
        {'range', 10, function(row) row[4] = 1000 end, 1264, "Out of range value for column 'c' at row 10"},
        {'notnull', 7, function(row) row[4] = box.NULL end, 1265, "Data truncated for column 'c' at row 7"},
        {'chars', 5, function(row) row[5] = 'abcdef' end, 1265, "Data truncated for column 's' at row 5"},
        {'digits', 3, function(row) row[6] = decimal.new('123.45') end, 1264,
         "Out of range value for column 'd' at row 3"},
    }) do
        local kind, n, change, code, text = unpack(case)
        restore = set{engine_copy_rows = 4, engine_fence_keys = 64}
        t = table_and_copy(engine, 'page_' .. kind, 40, function(i, row) if i == n then change(row) end end)
        built = call('build', t)
        check(label('a page\'s row that fails a ' .. kind .. ' check fails the change: ' .. text),
            refused(built, code, text), built)
        aborted(t, label('that change'))
        restore()
    end

    -- A pass rereads a row a write made invalid during the copy (row 2,
    -- behind the cursor), and fails the change.
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 1, engine_fence_keys = 64}
    t = table_and_copy(engine, 'pass', 40)
    building = held_copy(t)
    ok(set_c(t, 2, 1000))
    t.before = rows(t.old_id)
    local passes = tarn.engine.passes or 0
    hook('engine_copy', 'release')
    built = join(building)
    check(label('a pass that rereads a row made invalid during the copy fails the change'),
        refused(built, 1264, "Out of range value for column 'c' at row 2") and (tarn.engine.passes or 0) == passes,
        {built, passes, tarn.engine.passes})
    aborted(t, label('that change'))
    restore()

    -- So does the final pass (row 3, written after the copy).
    restore = set{engine_copy_rows = 4, engine_fence_keys = 64}
    t = table_and_copy(engine, 'tail', 40)
    built = call('build', t)
    ok(set_c(t, 3, box.NULL))
    t.before = rows(t.old_id)
    prepared = call('prepare', t)
    check(label('the final pass that rereads a row made invalid after the copy fails the change'),
        built[1] == 'ok' and refused(prepared, 1265, "Data truncated for column 'c' at row 3"), {built, prepared})
    aborted(t, label('that change'))
    restore()

    -- A row made invalid and valid again before a pass or the final pass
    -- reads it fails nothing: the copy checks the rows it writes.
    restore = set{engine_copy_rows = 4, engine_fence_keys = 64}
    t = table_and_copy(engine, 'fixed', 40)
    building = held_copy(t)
    ok(set_c(t, 2, 1000)); ok(set_c(t, 2, 5))
    hook('engine_copy', 'release')
    built = join(building)
    prepared = call('prepare', t)
    check(label('a row made invalid and valid again before the copy reads it fails nothing'),
        built[1] == 'ok' and prepared[1] == 'ok' and image(t) and box.space[t.new_id]:get{2}[4] == 5,
        {built, prepared})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- A descriptor whose checks storage does not know is refused before
    -- the copy begins.
    t = table_and_copy(engine, 'malformed', 8)
    t.args.checks = {{4, 'c', 'longer', 3, box.NULL, box.NULL}}
    built = call('build', t)
    check(label('a check of an unknown kind is refused before the copy begins'),
        built[1] ~= 'ok' and built[2] == 1210 and box.space[t.new_id] == nil, built)
    t.args.checks = CHECKS
    aborted(t, label('that change'))
end

local function cleanup()
    for _, name in ipairs({'engine_copy'}) do pcall(tarn_test_hook, name, 'release') end
    for _, t in pairs(jobs) do
        pcall(tarn_job_phase, t.owner, 'abort'); pcall(call, 'abort', t); pcall(tarn_job_end, t.owner)
    end
end
local yes, err = xpcall(function()
    local deadline = fiber.clock() + 5
    while not (tarn.engine ~= nil and tarn.clock.status().read_ready) and fiber.clock() < deadline do
        fiber.sleep(0.01)
    end
    ok(tarn_bucket_init(16, {{1, 16}}))
    exercise('memtx'); exercise('vinyl')
end, debug.traceback)
cleanup()
if not yes then failed = failed + 1; print('  FAIL engine checks test exception: ' .. tostring(err)) end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
