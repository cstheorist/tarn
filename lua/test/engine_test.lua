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
local hooks = {'engine_copy', 'engine_tail', 'engine_prepared', 'engine_renamed', 'engine_publish', 'engine_published',
    'write', 'scan', 'reduce_scan'}
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
-- Vinyl admission refuses a write while dump or compaction catches up
-- ('throttled', to retry after the delay it hints): the copy's tests write
-- many vinyl tables into a small vinyl_memory.
local function write(writes, fence)
    local deadline = fiber.clock() + 30
    repeat
        local r = tarn_autocommit(writes, {fence = fence})
        if type(r) ~= 'table' or r[1] ~= 'throttled' or fiber.clock() > deadline then return r end
        fiber.sleep(math.max(tonumber(r[4]) or 50, 10) / 1000)
    until false
end
local function change(t, writes) return write(writes, {t.old_id, t.old_ver}) end
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
        engine_change = {old_id = sid, old_ver = 0, old_engine = engine, old_name = name, durable = true}}
    begin_job(t)
    return t
end
local function ready_apply(t)
    ok(call('build', t)); ok(call('prepare', t)); decide(t, 'apply'); ok(call('apply', t))
end

-- A same-engine rebuild (issue #267): the ALTER drops c_idx (11), renames u
-- (7) and adds a unique index on the nullable c, one on (v, c) and a unique
-- one on (bucket_id, c), whose keys have NULL in their last part, all of
-- which the shadow has before its first row.
local function rebuild(engine, tag, count, timeout, volatile)
    local t = create(engine, tag, count)
    end_job(t) -- (create's job changes the engine)
    local nd = table.deepcopy(t.def)
    nd.id, nd.ver = t.new_id, 1
    nd.json = json.encode{space_id = t.new_id, ver = 1, engine = engine, marker = 'rebuilt'}
    local c_part = {field = 4, type = 'integer', is_nullable = true}
    nd.indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
        {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}},
        {name = 'v_u', unique = true, parts = {{2, 'integer'}}},
        {name = 'c_u', unique = true, parts = {c_part}},
        {name = 'v_c', unique = false, parts = {{2, 'integer'}, c_part}},
        {name = 'b_c_u', unique = true, parts = {{3, 'unsigned'}, c_part}}}
    t.args = {ver = 1, timeout = timeout or 5, def = nd, uix = {}, uix_drop = {}, lazy_drop = {},
        builds = {{12, 'c_u', true, {c_part}}, {13, 'v_c', false, {{field = 2, type = 'integer'}, c_part}},
            {14, 'b_c_u', true, {{field = 3, type = 'unsigned'}, c_part}}},
        drop = {11}, renames = {{7, 'v_u'}},
        engine_change = {old_id = t.old_id, old_ver = 0, old_engine = engine, old_name = t.name, rebuild = true,
            durable = not volatile or nil}}
    begin_job(t)
    return t
end
local function indexes_of(sid)
    local out = {}
    for n, ix in pairs(box.space[sid].index) do
        if type(n) == 'number' then
            local parts = {}
            for _, p in ipairs(ix.parts) do parts[#parts + 1] = {p.fieldno, p.type, p.is_nullable == true} end
            out[#out + 1] = {ix.id, ix.name, ix.unique, parts}
        end
    end
    table.sort(out, function(a, b) return a[1] < b[1] end)
    return out
end
local REBUILT = {{0, 'primary', true, {{1, 'unsigned', false}}}, {4, 'bucket_id', false, {{3, 'unsigned', false}}},
    {7, 'v_u', true, {{2, 'integer', false}}}, {12, 'c_u', true, {{4, 'integer', true}}},
    {13, 'v_c', false, {{2, 'integer', false}, {4, 'integer', true}}},
    {14, 'b_c_u', true, {{3, 'unsigned', false}, {4, 'integer', true}}}}
-- c of row n as create() writes it: NULL, n + 7, or no field at all.
local function c_of(n) return n % 3 == 2 and n + 7 or nil end
-- The keys the copy's final pass will reread.
local function captured_keys(t)
    local track = tarn.engine.tracks[t.old_id]
    if track == nil or not track.valid then return nil end
    local out = {}
    for _, key in ipairs(track.list) do out[#out + 1] = key[1] end
    table.sort(out)
    return out
end
local function update(t, n, field, value)
    local r = change(t, {{'update', t.old_id, {n}, {{'=', field, value}}}})
    if r[1] == 'ok' then t.expected[n][field] = value end
    return r
end
local function publish(t)
    ok(call('prepare', t)); decide(t, 'apply'); ok(call('apply', t)); end_job(t)
end

local function exercise_rebuild(engine)
    local label = function(s) return engine .. ' rebuild: ' .. s end
    local t = rebuild(engine, 'rebuild', 260)
    -- Row 230 takes row 2's c (9) before the copy begins, so no write to it
    -- is captured: the table holds a duplicate until row 2 changes below.
    ok(update(t, 230, 4, 9))
    hook('engine_copy', 'hold')
    local building = spawn(function() return call('build', t) end)
    local reached = hook('engine_copy', 'reached')
    check(label('the shadow has every new index, and none dropped, before the copy goes on'),
        reached[2] == 1 and box.space[t.new_id] ~= nil and box.space[t.new_id].engine == engine
        and same(indexes_of(t.new_id), REBUILT) and box.space[t.name].id == t.old_id,
        indexes_of(t.new_id))
    -- Row 2 (copied, c = 9) gives up 9: its copy is stale when row 230's
    -- page comes, so it leaves the shadow and row 230 goes in, although no
    -- write touched row 230.
    ok(update(t, 2, 4, 1000))
    -- Rows 5 (c = 12) and 8 (c = 15) swap their keys by way of NULL: the
    -- table never holds a duplicate, but each copy holds the other's key.
    ok(change(t, {{'update', t.old_id, {5}, {{'=', 4, box.NULL}}}}))
    ok(update(t, 8, 4, 12)); ok(update(t, 5, 4, 15))
    hook('engine_copy', 'release'); ok(result(building))
    check(label('a stale copy that holds a row\'s key leaves the shadow, and the row goes in'),
        phase(t).phase == 'copied' and box.space[t.new_id]:get{230} ~= nil
        and box.space[t.new_id]:get{230}[4] == 9 and box.space[t.new_id]:get{2} == nil, phase(t))
    check(label('only the keys writes touched go to the final pass'),
        same(captured_keys(t), {2, 5, 8}), captured_keys(t))
    ok(call('prepare', t))
    local shadow = box.space[t.new_id]
    check(label('the final pass settles stale copies and swapped keys'),
        phase(t).phase == 'prepared' and exact(t, t.new_id)
        and shadow.index.c_u:get{9}[1] == 230 and shadow.index.c_u:get{12}[1] == 8
        and shadow.index.c_u:get{15}[1] == 5 and shadow.index.b_c_u:get{1, 1000}[1] == 2
        and shadow.index.b_c_u:count() == #wanted(t), difference(t, t.new_id))
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    check(label('the published table has the new indexes, renamed and without the dropped one'),
        clean(t) and exact(t, t.new_id) and box.space[t.name].engine == engine
        and same(indexes_of(t.new_id), REBUILT) and box.space[t.name].index.v_c:count() == #wanted(t)
        and box.space[t.name].index.c_u:count() == #wanted(t),
        indexes_of(t.new_id))
    local dup = tarn_autocommit({{'insert', t.new_id, {5000, 5000, 1, 9}}}, {fence = {t.new_id, t.args.ver}})
    check(label('the new unique index refuses a duplicate'), dup[1] ~= 'ok' and dup[2] == 1062, dup)
    local nulls = tarn_autocommit({{'insert', t.new_id, {5001, 5001, 1, box.NULL}},
        {'insert', t.new_id, {5002, 5002, 1}}}, {fence = {t.new_id, t.args.ver}})
    check(label('keys with NULL in their last part are no one\'s'), nulls[1] == 'ok', nulls)

    -- Rows 150 and 160 hold row 2's c (9) too, untouched. Row 2's stale
    -- copy leaves for row 150's sake, and row 160 then meets row 150: both
    -- held 9 when their page was read.
    local many = rebuild(engine, 'rebuild_many', 260)
    ok(update(many, 150, 4, 9)); ok(update(many, 160, 4, 9))
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', many) end)
    hook('engine_copy', 'reached')
    ok(update(many, 2, 4, 1000))
    hook('engine_copy', 'release')
    local refused = result(building)
    check(label('two untouched rows behind one stale copy fail the copy with 1062'),
        refused[1] ~= 'ok' and refused[2] == 1062 and tostring(refused[3]):find("'c_u'") ~= nil, refused)
    check(label('the copy left them to no final pass'), same(captured_keys(many), {2}), captured_keys(many))
    decide(many, 'abort'); ok(call('abort', many)); end_job(many)
    check(label('ABORT after that leaves the table as it was'),
        box.space[many.new_id] == nil and box.space[many.name].id == many.old_id and exact(many, many.old_id))

    -- A duplicate the table holds when the copy reads it: row 201 takes row
    -- 11's c (18) while row 11's copy is current.
    local early = rebuild(engine, 'rebuild_dup', 260)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', early) end)
    hook('engine_copy', 'reached')
    ok(change(early, {{'update', early.old_id, {201}, {{'=', 4, c_of(11)}}}}))
    hook('engine_copy', 'release')
    refused = result(building)
    check(label('a duplicate in the table fails the copy with 1062'),
        refused[1] ~= 'ok' and refused[2] == 1062 and tostring(refused[3]):find("'c_u'") ~= nil, refused)
    decide(early, 'abort'); ok(call('abort', early)); end_job(early)
    check(label('ABORT after a duplicate leaves the table as it was'),
        box.space[early.new_id] == nil and box.space[early.name].id == early.old_id
        and box.space[early.name].index.u ~= nil and box.space[early.name].index.c_u == nil)

    -- A duplicate made after its rows were copied, still there at the fence:
    -- row 14 takes row 17's c (24), and both copies were current before.
    local late = rebuild(engine, 'rebuild_late', 40)
    ok(call('build', late))
    ok(change(late, {{'update', late.old_id, {14}, {{'=', 4, c_of(17)}}}}))
    local fenced = call('prepare', late)
    check(label('a duplicate the fenced table holds fails PREPARE with 1062'),
        fenced[1] ~= 'ok' and fenced[2] == 1062 and tostring(fenced[3]):find("'c_u'") ~= nil, fenced)
    decide(late, 'abort'); ok(call('abort', late)); end_job(late)
    local resumed = change(late, {{'update', late.old_id, {14}, {{'=', 4, 1}}}})
    check(label('writes go on after that ABORT'), resumed[1] == 'ok'
        and box.space[late.name].id == late.old_id and box.space[late.new_id] == nil, resumed)

    -- A copy that runs past its call's deadline (the ALTER's lock wait
    -- timeout) waits its own deadline for a cut. Here another job's cut has
    -- just invalidated the shared meter, and a commit is in flight.
    local slow = rebuild(engine, 'rebuild_slow', 260, 1)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', slow) end)
    hook('engine_copy', 'reached')
    fiber.sleep(1.2)
    hook('write', 'hold')
    local writing = spawn(function() return update(slow, 3, 2, 31) end)
    hook('write', 'reached')
    tarn.engine.tickets.invalidate()
    hook('engine_copy', 'release')
    fiber.sleep(.2)
    hook('write', 'release')
    local built = result(building)
    check(label('a copy past its deadline waits for a cut with a deadline of its own'),
        built[1] == 'ok' and result(writing)[1] == 'ok' and phase(slow).phase == 'copied', built)
    -- Its final pass (row 3) runs past PREPARE's deadline too, and the cut
    -- after it meets a commit in flight on another table.
    hook('engine_tail', 'hold')
    local preparing = spawn(function() return call('prepare', slow) end)
    hook('engine_tail', 'reached')
    fiber.sleep(1.2)
    hook('write', 'hold')
    writing = spawn(function()
        return tarn_autocommit({{'update', t.new_id, {10}, {{'=', 2, 101}}}}, {fence = {t.new_id, t.args.ver}})
    end)
    hook('write', 'reached')
    hook('engine_tail', 'release')
    fiber.sleep(.2)
    hook('write', 'release')
    local prepared = result(preparing)
    check(label('a final pass past its deadline waits for its cut with a deadline of its own'),
        prepared[1] == 'ok' and result(writing)[1] == 'ok' and phase(slow).phase == 'prepared', prepared)
    decide(slow, 'apply'); ok(call('apply', slow)); end_job(slow)
    check(label('that copy is published with the write made meanwhile'), clean(slow) and exact(slow, slow.new_id),
        difference(slow, slow.new_id))

    -- A capture that writes outgrow after the deadline starts the copy
    -- again (reset), whose cut and drain have deadlines of their own. (A
    -- copy without a durable capture: engine_resume_test.lua has one that
    -- resumes first.)
    local lost = rebuild(engine, 'rebuild_lost', 260, 1, true)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', lost) end)
    hook('engine_copy', 'reached')
    local token = phase(lost).capture
    fiber.sleep(1.2)
    tarn.cfg.engine_dirty_keys = 2
    ok(update(lost, 3, 2, 31)); ok(update(lost, 6, 2, 61)); ok(update(lost, 9, 2, 91))
    tarn.cfg.engine_dirty_keys = nil
    hook('write', 'hold')
    writing = spawn(function() return update(lost, 12, 2, 121) end)
    hook('write', 'reached')
    hook('engine_copy', 'release')
    fiber.sleep(.2)
    hook('write', 'release')
    built = result(building)
    check(label('a capture lost after the deadline starts the copy again'),
        built[1] == 'ok' and result(writing)[1] == 'ok' and phase(lost).phase == 'copied'
        and phase(lost).capture ~= token and phase(lost).resumes == nil, {built, phase(lost)})
    publish(lost)
    check(label('that copy is published with every write'), clean(lost) and exact(lost, lost.new_id),
        difference(lost, lost.new_id))

    local bad = rebuild(engine, 'rebuild_bad', 4)
    bad.args.builds = {}
    local refused_bad = call('build', bad)
    check(label('a rebuild without new indexes is refused before any shadow exists'),
        refused_bad[1] ~= 'ok' and refused_bad[2] == 1210 and box.space[bad.new_id] == nil, refused_bad)
    bad.args.engine_change.rebuild = nil
    refused_bad = call('build', bad)
    check(label('a same-engine change that is not a rebuild is refused'),
        refused_bad[1] ~= 'ok' and refused_bad[2] == 1210 and box.space[bad.new_id] == nil, refused_bad)
    decide(bad, 'abort'); end_job(bad)
end

-- A rebuild that also adds a column e (field 5) and indexes it: e_idx (14)
-- on e, unique when `unique_e`, and the unique v_e (15) on (v, e). With a
-- non-NULL `fill`, every row the copy writes is padded (a.pad): a row that
-- ends before c takes `cfill`, the fill an earlier ALTER gave c, and every
-- row takes `fill` for e, as PREPARE would materialize them in place. With
-- a NULL fill, rows are copied as they are.
local function rebuild_layout(engine, tag, count, fill, unique_e, cfill)
    local t = create(engine, tag, count)
    end_job(t) -- (create's job changes the engine)
    local nd = table.deepcopy(t.def)
    nd.id, nd.ver = t.new_id, 1
    nd.json = json.encode{space_id = t.new_id, ver = 1, engine = engine, marker = 'layout'}
    nd.format[5] = {name = 'e', type = 'integer', is_nullable = true}
    local e_part = {field = 5, type = 'integer', is_nullable = true}
    nd.indexes[#nd.indexes + 1] = {name = 'e_idx', unique = unique_e == true, parts = {e_part}}
    nd.indexes[#nd.indexes + 1] = {name = 'v_e', unique = true, parts = {{2, 'integer'}, e_part}}
    t.args = {ver = 1, timeout = 5, def = nd, uix = {}, uix_drop = {}, lazy_drop = {},
        builds = {{14, 'e_idx', unique_e == true, {e_part}}, {15, 'v_e', true, {{field = 2, type = 'integer'}, e_part}}},
        drop = {}, renames = {}, format = nd.format, old_width = 4, materialize = fill ~= nil,
        pad = fill ~= nil and {3, cfill == nil and box.NULL or cfill, fill} or box.NULL,
        checks = fill ~= nil and {{5, 'e', 'indexed', box.NULL, box.NULL, fill}} or {},
        engine_change = {old_id = t.old_id, old_ver = 0, old_engine = engine, old_name = t.name, rebuild = 'layout',
            durable = true}}
    begin_job(t)
    return t
end
-- A space's format, and a descriptor's, as {name, type, nullable} fields.
local function format_of(fields)
    local out = {}
    for _, f in ipairs(fields) do out[#out + 1] = {f.name, f.type, f.is_nullable == true} end
    return out
end
-- t's expected rows as the copy writes them: a row that ends before c takes
-- `cfill`, and every row shorter than e takes `fill`.
local function padded(t, fill, cfill)
    local out = {}
    for _, row in ipairs(wanted(t)) do
        local r = table.deepcopy(row)
        if #r < 5 then
            if #r < 4 then r[4] = cfill == nil and box.NULL or cfill end
            r[5] = fill
        end
        out[#out + 1] = r
    end
    return out
end
-- A job the copy refuses at BUILD: no shadow, and ABORT still ends it with
-- the table as it was.
local function refused_then_aborted(t, label, code)
    local r = call('build', t)
    local refused = r[1] ~= 'ok' and r[2] == (code or 1210) and box.space[t.new_id] == nil
    decide(t, 'abort')
    local aborted = call('abort', t)
    end_job(t)
    check(label, refused and aborted[1] == 'ok' and box.space[t.name].id == t.old_id
        and mp.encode(rows(t.old_id)) == t.original_bytes and phase(t).phase == 'aborted', {r, aborted})
end

local function exercise_layout(engine)
    local label = function(s) return engine .. ' rebuild with a new column: ' .. s end
    -- c's earlier fill is 5: rows that end before c take it, and rows that
    -- hold an explicit NULL keep it.
    local t = rebuild_layout(engine, 'layout', 260, 7, false, 5)
    hook('engine_copy', 'hold')
    local building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    local shadow = box.space[t.new_id]
    check(label('the shadow has the new format and every index before the copy goes on'),
        shadow ~= nil and same(format_of(shadow:format()), format_of(t.args.format))
        and same(indexes_of(t.new_id), {{0, 'primary', true, {{1, 'unsigned', false}}},
            {4, 'bucket_id', false, {{3, 'unsigned', false}}}, {7, 'u', true, {{2, 'integer', false}}},
            {11, 'c_idx', false, {{4, 'integer', true}}}, {14, 'e_idx', false, {{5, 'integer', true}}},
            {15, 'v_e', true, {{2, 'integer', false}, {5, 'integer', true}}}}),
        {shadow and format_of(shadow:format()), indexes_of(t.new_id)})
    check(label('copied rows take the old slot\'s fill and the new one, and keep explicit NULLs'),
        same(shadow:get{9}:totable(), {9, 90, 1, 5, 7}) and same(shadow:get{1}:totable(), {1, 10, 1, box.NULL, 7})
        and same(shadow:get{2}:totable(), {2, 20, 1, 9, 7}), {shadow:get{9}, shadow:get{1}, shadow:get{2}})
    -- Writes in the old layout go on: a short row changes, rows of three and
    -- four fields come in, one goes, and a short row gains its c.
    ok(update(t, 3, 2, 31))
    ok(change(t, {{'insert', t.old_id, {1001, 10010, 1}}})); t.expected[1001] = {1001, 10010, 1}
    ok(change(t, {{'insert', t.old_id, {1002, 10020, 1, 5}}})); t.expected[1002] = {1002, 10020, 1, 5}
    ok(change(t, {{'delete', t.old_id, {5}}})); t.expected[5] = nil
    ok(change(t, {{'update', t.old_id, {6}, {{'=', 4, 1066}}}})); t.expected[6][4] = 1066
    hook('engine_copy', 'release'); ok(result(building))
    ok(call('prepare', t))
    shadow = box.space[t.new_id]
    local want = padded(t, 7, 5)
    check(label('PREPARED holds every row padded, written during the copy or not'),
        phase(t).phase == 'prepared' and same(rows(t.new_id), want), {rows(t.new_id)[3], want[3]})
    check(label('the new index holds the fill for every row'),
        shadow.index.e_idx:count{7} == #want and shadow.index.v_e:count() == #want
        and shadow.index.c_idx:count{1066} == 1,
        {shadow.index.e_idx:count{7}, shadow.index.v_e:count(), shadow.index.c_idx:count{1066}})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    check(label('the published table is the padded copy, in its own engine and the new format'),
        clean(t) and same(rows(t.new_id), want) and box.space[t.name].engine == engine
        and same(format_of(box.space[t.name]:format()), format_of(t.args.format)), difference(t, t.new_id))
    local written = tarn_autocommit({{'insert', t.new_id, {5000, 50000, 1, box.NULL, 8}}},
        {fence = {t.new_id, t.args.ver}})
    check(label('rows in the new layout go in after it'), written[1] == 'ok'
        and box.space[t.name].index.e_idx:count{8} == 1, written)

    -- Writes outgrow the capture mid-copy, past its limits in the durable
    -- capture too: the copy starts again, and pads alike, from the job's
    -- record.
    local lost = rebuild_layout(engine, 'layout_lost', 260, 7, false, 5)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', lost) end)
    hook('engine_copy', 'reached')
    local token = phase(lost).capture
    tarn.cfg.engine_dirty_keys = 2
    ok(update(lost, 3, 2, 31)); ok(update(lost, 6, 2, 61)); ok(update(lost, 9, 2, 91))
    hook('engine_copy', 'release'); ok(result(building))
    tarn.cfg.engine_dirty_keys = nil
    ok(call('prepare', lost)); decide(lost, 'apply'); ok(call('apply', lost)); end_job(lost)
    check(label('a copy started again after its capture was lost pads every row alike'),
        phase(lost).capture ~= token and clean(lost) and same(rows(lost.new_id), padded(lost, 7, 5)),
        difference(lost, lost.new_id))

    -- A COUNT that yields while the copy is published and the old space
    -- goes fails with 1412, which the SQL server runs again with the new
    -- definition, rather than with a storage error.
    local raced = rebuild_layout(engine, 'layout_race', 40, 7)
    ok(call('build', raced))
    hook('scan', 'hold')
    local counting = spawn(function()
        return {pcall(tarn_count, raced.old_id, 0, {}, 'ALL', nil, true, {}, {{1}}, true, false)}
    end)
    local scanning = hook('scan', 'reached')
    ok(call('prepare', raced)); decide(raced, 'apply'); ok(call('apply', raced)); end_job(raced)
    hook('scan', 'release')
    local counted = result(counting)
    check(label('a COUNT whose space the copy replaced while it yielded fails with 1412'),
        scanning[2] == 1 and clean(raced) and counted[1] == true and type(counted[2]) == 'table'
        and counted[2][1] == 'error' and counted[2][2] == 1412, tostring(counted[2]))
    -- An aggregate the publication's DDL makes scan again, once the old
    -- space is gone: vinyl scans again (1412); memtx answers from its scan.
    local summed = rebuild_layout(engine, 'layout_sum', 40, 7)
    ok(call('build', summed))
    hook('reduce_scan', 'hold')
    local reducing = spawn(function()
        return {pcall(tarn_count, summed.old_id, 0, {}, 'ALL', nil, true, {}, {}, true, false, nil,
            {{'countstar', 1}})}
    end)
    local reduced_at = hook('reduce_scan', 'reached')
    ok(call('prepare', summed)); decide(summed, 'apply'); ok(call('apply', summed)); end_job(summed)
    hook('reduce_scan', 'release')
    local reduced = result(reducing)
    local answer = reduced[2]
    check(label('an aggregate across the publication answers, or fails with 1412'),
        reduced_at[2] == 1 and reduced[1] == true and type(answer) == 'table'
        and ((answer[1] == 'error' and answer[2] == 1412)
             or (engine == 'memtx' and answer[1] == 'ok' and tonumber(answer[2][1]) == 40)),
        {reduced[1], tostring(answer and answer[1]), tostring(answer and answer[2])})

    -- Columns renamed and dropped, with a new index: the copy takes the new
    -- format and copies the rows as they are.
    local renamed = create(engine, 'layout_rename', 40)
    end_job(renamed)
    local nd = table.deepcopy(renamed.def)
    nd.id, nd.ver = renamed.new_id, 1
    nd.json = json.encode{space_id = renamed.new_id, ver = 1, engine = engine, marker = 'renamed'}
    nd.format[2] = {name = 'w', type = 'integer'}
    nd.format[4] = {name = '_dropped_4', type = 'any', is_nullable = true}
    renamed.args = {ver = 1, timeout = 5, def = nd, uix = {}, uix_drop = {}, lazy_drop = {},
        builds = {{12, 'w_b', false, {{field = 2, type = 'integer'}, {field = 3, type = 'unsigned'}}}},
        drop = {11}, renames = {{7, 'w_u'}}, format = nd.format, old_width = 4, materialize = false,
        pad = box.NULL, checks = {},
        engine_change = {old_id = renamed.old_id, old_ver = 0, old_engine = engine, old_name = renamed.name,
            rebuild = 'layout'}}
    begin_job(renamed)
    ok(call('build', renamed)); ok(call('prepare', renamed)); decide(renamed, 'apply'); ok(call('apply', renamed))
    end_job(renamed)
    check(label('columns renamed and dropped: the new format, and every row as it was'),
        clean(renamed) and same(format_of(box.space[renamed.name]:format()), format_of(nd.format))
        and mp.encode(rows(renamed.new_id)) == renamed.original_bytes
        and box.space[renamed.name].index.w_b:count() == 40 and box.space[renamed.name].index.w_u ~= nil
        and box.space[renamed.name].index.c_idx == nil, format_of(box.space[renamed.name]:format()))

    -- A NULL fill needs no padding: rows are copied as they are.
    local short = rebuild_layout(engine, 'layout_null', 40)
    ok(call('build', short)); ok(call('prepare', short)); decide(short, 'apply'); ok(call('apply', short))
    end_job(short)
    check(label('with a NULL fill, rows keep their physical length'),
        clean(short) and mp.encode(rows(short.new_id)) == short.original_bytes
        and box.space[short.name].index.e_idx:count() == 40, difference(short, short.new_id))

    -- A unique index on the new column: every row takes the fill.
    local dup = rebuild_layout(engine, 'layout_dup', 40, 7, true)
    local refused = call('build', dup)
    check(label('a unique index on a shared fill fails the copy with 1062'),
        refused[1] ~= 'ok' and refused[2] == 1062 and tostring(refused[3]):find("'e_idx'") ~= nil, refused)
    decide(dup, 'abort'); ok(call('abort', dup)); end_job(dup)
    check(label('ABORT leaves the table and its rows as they were'),
        box.space[dup.new_id] == nil and box.space[dup.name].id == dup.old_id
        and mp.encode(rows(dup.old_id)) == dup.original_bytes and #box.space[dup.name]:format() == 4)

    -- Descriptors the copy refuses before any shadow exists. ABORT still
    -- ends each job (as a tombstone a stale BUILD meets).
    local bad = create(engine, 'layout_engine', 4)
    bad.args.format = table.deepcopy(bad.def.format)
    refused_then_aborted(bad, label('an engine change that would change the layout is refused, and aborts'))
    local unsaid = rebuild_layout(engine, 'layout_unsaid', 4, 7)
    unsaid.args.engine_change.rebuild = true
    refused_then_aborted(unsaid, label('a layout change in a rebuild that does not say so is refused, and aborts'))
    local badpad = rebuild_layout(engine, 'layout_badpad', 4, 7)
    badpad.args.pad = {0, 7}
    refused_then_aborted(badpad, label('malformed padding is refused, and aborts'))
    local checked = rebuild_layout(engine, 'layout_checked', 4, 7)
    checked.args.checks[2] = {2, 'v', 'range', 0, 10, box.NULL}
    refused_then_aborted(checked, label('a rebuild that would check rows is refused, and aborts'))
    local moved = rebuild_layout(engine, 'layout_moved', 4, 7)
    moved.args.rename = moved.name .. '_2'
    refused_then_aborted(moved, label('a rebuild that would rename the table is refused, and aborts'))
    local stale = call('build', moved)
    check(label('a BUILD after that ABORT meets its tombstone'), stale[1] ~= 'ok' and stale[2] == 1412, stale)
end

-- A rebuild of a table with a global unique index (issue #184), which it
-- keeps (rebuild = 'aux'): the index's space, on v (u, 7), keeps its id and
-- its entries, which both definitions write, and is renamed after the new
-- space as the copy is published. The ALTER adds a unique index on c, and
-- with a `fill` a column e (field 5) and an index on it, which pads rows.
local function rebuild_aux(engine, tag, count, fill)
    local t = create(engine, tag, count, true)
    end_job(t) -- (create's job changes the engine)
    local nd = table.deepcopy(t.def)
    nd.id, nd.ver = t.new_id, 1
    nd.json = json.encode{space_id = t.new_id, ver = 1, engine = engine, marker = 'kept'}
    local c_part = {field = 4, type = 'integer', is_nullable = true}
    nd.indexes[#nd.indexes + 1] = {name = 'c_u', unique = true, parts = {c_part}}
    nd.uix[1].name = '_tarn_uix_' .. t.new_id .. '_7'
    t.uix_old, t.uix_new = t.def.uix[1].name, nd.uix[1].name
    t.args = {ver = 1, timeout = 5, def = nd, uix = {}, uix_drop = {}, lazy_drop = {},
        builds = {{12, 'c_u', true, {c_part}}}, drop = {}, renames = {},
        engine_change = {old_id = t.old_id, old_ver = 0, old_engine = engine, old_name = t.name, rebuild = 'aux'}}
    if fill ~= nil then
        local e_part = {field = 5, type = 'integer', is_nullable = true}
        nd.format[5] = {name = 'e', type = 'integer', is_nullable = true}
        nd.indexes[#nd.indexes + 1] = {name = 'e_idx', unique = false, parts = {e_part}}
        t.args.builds[2] = {13, 'e_idx', false, {e_part}}
        t.args.format, t.args.old_width, t.args.materialize = nd.format, 4, true
        t.args.pad, t.args.checks = {3, box.NULL, fill}, {{5, 'e', 'indexed', box.NULL, box.NULL, fill}}
    end
    begin_job(t)
    return t
end
-- A second such rebuild of t once it was published: from t's new space to
-- another, adding v_b (20).
local function rebuild_again(t)
    local u = {name = t.name, old_id = t.new_id, new_id = id(), old_ver = t.args.ver, gen = 1,
        owner = 'engine-test-' .. t.new_id .. '-again', expected = t.expected, old_uid = t.old_uid}
    local nd = table.deepcopy(t.args.def)
    nd.id, nd.ver = u.new_id, t.args.ver + 1
    nd.json = json.encode{space_id = u.new_id, ver = nd.ver, engine = nd.engine, marker = 'again'}
    local parts = {{field = 2, type = 'integer'}, {field = 3, type = 'unsigned'}}
    nd.indexes[#nd.indexes + 1] = {name = 'v_b', unique = false, parts = parts}
    nd.uix[1].name = '_tarn_uix_' .. u.new_id .. '_7'
    u.uix_old, u.uix_new = t.uix_new, nd.uix[1].name
    u.args = {ver = nd.ver, timeout = 5, def = nd, uix = {}, uix_drop = {}, lazy_drop = {},
        builds = {{20, 'v_b', false, parts}}, drop = {}, renames = {},
        engine_change = {old_id = u.old_id, old_ver = t.args.ver, old_engine = nd.engine, old_name = t.name,
            rebuild = 'aux'}}
    begin_job(u)
    return u
end
-- The entries t's rows give its global index: {v, id, bucket_id}.
local function entries(t)
    local out = {}
    for _, row in ipairs(wanted(t)) do out[#out + 1] = {row[2], row[1], row[3]} end
    table.sort(out, function(a, b) return a[1] < b[1] end)
    return out
end
-- t's table in this shard's schema export: {catalog id, id of the space with
-- its name, ids of its global index spaces}.
local function exported(t)
    for _, x in ipairs(ok(tarn_schema_export())[2]) do
        if x.name == t.name then
            local ids = {}
            for _, u in ipairs(x.uix) do ids[#ids + 1] = u.id end
            table.sort(ids)
            return {x.def.id, x.space.id, ids}
        end
    end
    return false
end
-- A job the copy refuses that leaves the global index as it was too.
local function aux_refused(t, label)
    refused_then_aborted(t, label)
    check(label .. ' (the global index too)', box.space[t.old_uid].name == t.uix_old
        and same(rows(t.old_uid), entries(t)), {box.space[t.old_uid].name})
end

local function exercise_aux(engine)
    local label = function(s) return engine .. ' rebuild keeping a global index: ' .. s end
    local t = rebuild_aux(engine, 'aux', 260)
    local uid = t.old_uid
    hook('engine_copy', 'hold')
    local building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    check(label('the copy makes no global index space of its own'),
        box.space[t.new_id] ~= nil and box.space[t.new_id].index.c_u ~= nil and box.space[t.uix_new] == nil
        and box.space[uid].name == t.uix_old, {box.space[uid].name})
    -- Writes go on through the global index: a row comes in, one takes a
    -- new unique value, one goes, and a duplicate fails (the entry of a row
    -- another shard would hold).
    ok(change(t, {{'insert', t.old_id, {1001, 10010, 1}}, {'insert', uid, {10010, 1001, 1}}}))
    t.expected[1001] = {1001, 10010, 1}
    ok(change(t, {{'update', t.old_id, {4}, {{'=', 2, 40040}}}, {'delete', uid, {40}},
        {'insert', uid, {40040, 4, 1}}}))
    t.expected[4][2] = 40040
    ok(change(t, {{'delete', t.old_id, {6}}, {'delete', uid, {60}}})); t.expected[6] = nil
    local dup = change(t, {{'insert', uid, {10010, 1002, 1}}})
    check(label('the global index refuses a duplicate during the copy'),
        dup[1] ~= 'ok' and dup[2] == 1062 and tostring(dup[3]):find("'10010' for key 'u'") ~= nil
        and box.space[uid]:get{10010}[2] == 1001, dup)
    hook('engine_copy', 'release'); ok(result(building))
    ok(call('prepare', t))
    check(label('PREPARED holds every row, and the global index, in its own space, every entry'),
        phase(t).phase == 'prepared' and exact(t, t.new_id) and same(rows(uid), entries(t))
        and box.space[t.uix_new] == nil and #phase(t).kept_uix == 1, difference(t, t.new_id))
    local stale = change(t, {{'insert', uid, {99, 99, 1}}})
    check(label('behind the fence it refuses writes made with the old definition'),
        stale[1] == 'error' and stale[2] == 1412 and box.space[uid]:get{99} == nil, stale)
    decide(t, 'apply')
    -- Interrupted once the copy has the table's name: until the catalog
    -- names the copy, the catalog finds the index under its old name.
    hook('engine_renamed', 'hold')
    local applying = spawn(function() return call('apply', t) end)
    hook('engine_renamed', 'reached')
    check(label('with the old name, it is the table\'s still once the copy has the table\'s name'),
        box.space[t.name].id == t.new_id and box.space[uid].name == t.uix_old
        and box.space._tarn_catalog:get{t.name}.def.id == t.old_id
        and same(exported(t), {t.old_id, t.new_id, {uid}}), exported(t))
    interrupt(applying, 'engine_renamed')
    hook('engine_publish', 'hold')
    applying = spawn(function() return call('apply', t) end)
    hook('engine_publish', 'reached')
    check(label('APPLY again renames it after the copy, before the catalog names the copy'),
        box.space[uid].name == t.uix_new and box.space._tarn_catalog:get{t.name}.def.id == t.old_id
        and same(exported(t), {t.old_id, t.new_id, {uid}}), exported(t))
    interrupt(applying, 'engine_publish')
    ok(call('apply', t)); end_job(t)
    check(label('the published table keeps the index\'s space, id and entries, under the new name'),
        clean(t) and exact(t, t.new_id) and box.space[uid] ~= nil and box.space[uid].name == t.uix_new
        and same(rows(uid), entries(t)) and box.space[t.uix_old] == nil
        and same(exported(t), {t.new_id, t.new_id, {uid}}), {exported(t), box.space[uid] and box.space[uid].name})
    local old = write({{'insert', uid, {99, 99, 1}}}, {t.old_id, 0})
    local new = write({{'insert', t.new_id, {2001, 20010, 1}}, {'insert', uid, {20010, 2001, 1}}},
        {t.new_id, t.args.ver})
    local twice = write({{'insert', uid, {20010, 2002, 1}}, {'insert', t.new_id, {2002, 20010, 1}}},
        {t.new_id, t.args.ver})
    check(label('then it refuses the old definition\'s writes, takes the new one\'s, and refuses a duplicate'),
        old[1] == 'error' and old[2] == 1412 and new[1] == 'ok' and twice[1] == 'duplicate' and twice[2] == 1062
        and box.space[uid]:get{20010}[2] == 2001 and box.space[uid]:get{99} == nil, {old, new, twice})
    ok(tarn_drop_table(t.name))
    check(label('DROP TABLE drops it with the table'), box.space[uid] == nil and box.space[t.new_id] == nil)

    -- The same rebuild adding a column with a fill and an index on it: rows
    -- are padded, and the global index stays as it was.
    local wide = rebuild_aux(engine, 'aux_layout', 40, 7)
    ok(call('build', wide)); ok(call('prepare', wide)); decide(wide, 'apply'); ok(call('apply', wide)); end_job(wide)
    check(label('a rebuild that also pads rows keeps the index as it was'),
        clean(wide) and same(rows(wide.new_id), padded(wide, 7)) and box.space[wide.name].index.e_idx:count{7} == 40
        and box.space[wide.old_uid].name == wide.uix_new and same(rows(wide.old_uid), entries(wide)),
        difference(wide, wide.new_id))
    local again = rebuild_again(wide)
    ok(call('build', again)); ok(call('prepare', again)); decide(again, 'apply'); ok(call('apply', again))
    end_job(again)
    check(label('a second rebuild names the index after its own copy'),
        clean(again) and box.space[again.name].index.v_b:count() == 40 and box.space[again.old_uid].name == again.uix_new
        and same(rows(again.old_uid), entries(wide)) and same(exported(again), {again.new_id, again.new_id, {again.old_uid}}),
        {box.space[again.old_uid].name, exported(again)})

    -- An index on a column made nullable gets a new id (alter.rs), so the
    -- new definition names the kept space after it: the copy renames the
    -- space so, and so it does a space an older change left under another
    -- index id.
    local renumbered = rebuild_aux(engine, 'aux_renumbered', 20)
    renumbered.args.drop = {7}
    renumbered.args.builds[2] = {16, 'u', true, {{field = 2, type = 'integer'}}}
    renumbered.args.def.uix[1].name = '_tarn_uix_' .. renumbered.new_id .. '_16'
    local drifted = rebuild_aux(engine, 'aux_drifted', 20)
    box.space[drifted.old_uid]:rename('_tarn_uix_' .. drifted.old_id .. '_5')
    for _, t2 in ipairs({renumbered, drifted}) do
        ok(call('build', t2)); ok(call('prepare', t2)); decide(t2, 'apply'); ok(call('apply', t2)); end_job(t2)
    end
    check(label('the index takes the name the new definition gives it, whatever its index id'),
        clean(renumbered) and box.space[renumbered.old_uid].name == renumbered.args.def.uix[1].name
        and box.space[renumbered.name].index.u.id == 16 and same(rows(renumbered.old_uid), entries(renumbered))
        and clean(drifted) and box.space[drifted.old_uid].name == drifted.uix_new
        and same(rows(drifted.old_uid), entries(drifted)),
        {box.space[renumbered.old_uid].name, box.space[drifted.old_uid].name})

    local aborted = rebuild_aux(engine, 'aux_abort', 20)
    ok(call('build', aborted)); ok(call('prepare', aborted))
    decide(aborted, 'abort'); ok(call('abort', aborted)); end_job(aborted)
    local resumed = change(aborted, {{'insert', aborted.old_id, {3001, 30010, 1}},
        {'insert', aborted.old_uid, {30010, 3001, 1}}})
    if resumed[1] == 'ok' then aborted.expected[3001] = {3001, 30010, 1} end
    check(label('ABORT leaves it under its name, with every entry, and writes go on'),
        box.space[aborted.new_id] == nil and box.space[aborted.name].id == aborted.old_id
        and box.space[aborted.old_uid].name == aborted.uix_old and resumed[1] == 'ok'
        and same(rows(aborted.old_uid), entries(aborted)), resumed)

    -- Descriptors the copy refuses before any shadow exists.
    local made = rebuild_aux(engine, 'aux_new', 4)
    made.args.uix = {table.deepcopy(made.args.def.uix[1])}
    made.args.uix[1].id, made.args.uix[1].name = id(), '_tarn_uix_' .. made.new_id .. '_12'
    aux_refused(made, label('a rebuild that would make a global index is refused, and aborts'))
    local dropped = rebuild_aux(engine, 'aux_drop', 4)
    dropped.args.uix_drop = {dropped.old_uid}
    aux_refused(dropped, label('a rebuild that would drop a global index is refused, and aborts'))
    local left = rebuild_aux(engine, 'aux_left', 4)
    left.args.def.uix = nil
    aux_refused(left, label('a rebuild that would leave a global index out is refused, and aborts'))
    local other = rebuild_aux(engine, 'aux_other', 4)
    other.args.def.uix[1].id = other.old_id
    aux_refused(other, label('a rebuild that would keep a space that is not the table\'s index is refused, and aborts'))
    local named = rebuild_aux(engine, 'aux_named', 4)
    named.args.def.uix[1].name = named.uix_old
    aux_refused(named, label('a rebuild that would keep the old name is refused, and aborts'))
    -- (ABORT checks the lazy generations it retires: this one has none.)
    local lazy = rebuild_aux(engine, 'aux_lazy', 4)
    lazy.args.def.lazy = {{gen = id()}}
    local refused = call('build', lazy)
    lazy.args.def.lazy = {}
    decide(lazy, 'abort'); ok(call('abort', lazy)); end_job(lazy)
    check(label('a rebuild with a lazy index is refused'),
        refused[1] ~= 'ok' and refused[2] == 1210 and box.space[lazy.new_id] == nil, refused)
    local unsaid = rebuild_aux(engine, 'aux_unsaid', 4)
    unsaid.args.engine_change.rebuild = true
    aux_refused(unsaid, label('a rebuild that keeps a global index without saying so is refused, and aborts'))
    for _, say in ipairs({true, 'layout'}) do
        local omitted = rebuild_aux(engine, 'aux_omitted_' .. tostring(say), 4)
        omitted.args.engine_change.rebuild, omitted.args.def.uix = say, nil
        aux_refused(omitted, label('a rebuild (' .. tostring(say) .. ') of a table with a global index that '
            .. 'leaves it out is refused, and aborts'))
    end
    local taken = rebuild_aux(engine, 'aux_taken', 4)
    local squatter = box.schema.space.create(taken.uix_new, {engine = engine})
    aux_refused(taken, label('a rebuild that would rename the index to a name a space has is refused, and aborts'))
    squatter:drop()
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
    -- The shadow has its indexes from the start (issue #267): the cycle's
    -- stale copy of row 1 holds 10 when row 129 is copied, so that copy
    -- leaves the shadow (the final pass puts row 1 back) instead of row 129
    -- failing as a duplicate.
    check(label('online baseline evicts the unique-key cycle\'s stale copy without publishing early'),
        box.space[t.name].id == t.old_id and box.space[t.new_id] ~= nil
        and box.space[t.new_id].index[7] ~= nil and box.space[t.new_id]:get{129} ~= nil
        and box.space[t.new_id]:get{1} == nil and box.space._tarn_catalog:get{t.name}.def.id == t.old_id)
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
    local track, noted = tarn.engine.tracks[loss.old_id], false
    for _, key in ipairs(track and track.valid and track.list or {}) do noted = noted or key[1] == 1 end
    check(label('capture-generation loss resumes the copy from the durable capture, which noted a write to a copied row'),
        after.capture == before.capture and (after.resumes or 0) == 1 and after.copied == 260 and noted,
        {before, after, noted})
    local old_owner = loss.owner; loss.owner = old_owner .. '-takeover'
    local takeover = ok(tarn_job_touch(loss.owner, old_owner)); loss.gen = takeover[2]
    assert(loss.gen > 1)
    jobs[old_owner], jobs[loss.owner] = nil, loss
    ok(call('build', loss))
    local shadow_rows = mp.encode(rows(loss.new_id))
    stale = call('build', loss, 1)
    check(label('a stale worker cannot change a higher-generation shadow'),
        stale[1] ~= 'ok' and stale[2] == 1412 and phase(loss).gen == loss.gen
        and mp.encode(rows(loss.new_id)) == shadow_rows, stale)
    ok(call('prepare', loss))
    check(label('the prepared copy rereads the row written after the loss'), exact(loss, loss.new_id)
        and mp.encode(rows(loss.new_id)) == mp.encode(rows(loss.old_id)), difference(loss, loss.new_id))
    decide(loss, 'apply')
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
        assert(ni and ni.ready == 1 and layout == tarn.NS_LAYOUT and layout == 14
               and ffi.sizeof('struct tarn_ns') == 392 and ffi.offsetof('struct tarn_ns', 'fallbacks') == 304,
               'actual native ABI14/size392/fallbacks304 required')
    else assert(tarn.cfg.native == false and ni == false, 'explicit Lua mode required') end
    print('IDENTITY ' .. json.encode{version = box.info.version, native = ni, layout = layout,
        sync = tarn.cfg.sync, wal = box.cfg.wal_mode})
    assert(mp.encode(tarn_engine_capabilities()) == mp.encode({'ok', 1}), 'engine capability V1 required')
    assert(mp.encode(tarn_engine_rebuild_capabilities()) == mp.encode({'ok', 1}), 'rebuild capability required')
    assert(mp.encode(tarn_engine_rebuild_format_capabilities()) == mp.encode({'ok', 1}),
        'layout rebuild capability required')
    assert(mp.encode(tarn_engine_rebuild_aux_capabilities()) == mp.encode({'ok', 1}),
        'global-index rebuild capability required')
    exercise('memtx'); exercise('vinyl')
    exercise_rebuild('memtx'); exercise_rebuild('vinyl')
    exercise_layout('memtx'); exercise_layout('vinyl')
    exercise_aux('memtx'); exercise_aux('vinyl')
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
