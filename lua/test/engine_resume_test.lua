-- A copy's durable capture (issue #184): while a table is copied into a new
-- space (lua/tarn_engine.lua), every write to it notes its keys in a space of
-- its own engine, in the write's own transaction, with the generation of
-- the catch-up passes it was written in. A pass records the generation it
-- settled in the job's record. When the in-memory capture is lost (a status
-- change, a leader change, a restart), BUILD or PREPARE rebuilds it from the
-- keys above the settled generation, once the writes in flight have ended,
-- and goes on from the cursor instead of copying the table again. Past the
-- capture's limits, the copy starts over, as before.
local fio, fiber, json, mp = require('fio'), require('fiber'), require('json'), require('msgpack')
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

local passed, failed, sequence = 0, 0, 93000
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
        for _, row in box.space[sid].index[0]:pairs() do
            local t = row:totable()
            out[#out + 1] = {t[1], t[2], t[3], t[4] == nil and box.NULL or t[4]}
        end
        return out
    end)
end
local function record(t)
    return confirmed(function()
        local r = box.space._tarn_meta:get{'engine:' .. t.old_id}
        return r and table.deepcopy(r.value) or nil
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
        if type(r) ~= 'table' or r[1] ~= 'throttled' or fiber.clock() > deadline then return r end
        fiber.sleep(math.max(tonumber(r[4]) or 50, 10) / 1000)
    until false
end
local function set_v(t, n, v) return write(t, {{'update', t.old_id, {n}, {{'=', 2, v}}}}) end
local function call(op, t, gen) return _G['tarn_alter_' .. op](t.name, t.args, gen or t.gen) end
local function decide(t, value) return ok(tarn_job_phase(t.owner, value)) end
local function end_job(t) ok(tarn_job_end(t.owner)); jobs[t.owner] = nil end

-- A table (id, v unique, bucket_id, c) of `count` rows, and a rebuild that
-- adds a unique index on the nullable c, which the copy fills as it goes:
-- a copy the SQL server marks durable, unless `volatile`.
local function table_and_copy(engine, tag, count, volatile)
    local t = {old_id = id(), new_id = id(), gen = 1, name = 'test.resume_' .. engine .. '_' .. tag}
    t.owner = 'engine-resume-' .. t.old_id
    local def = {name = t.name, id = t.old_id, ver = 0, engine = engine, json = '{}', lazy = {},
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
            {name = 'bucket_id', type = 'unsigned'}, {name = 'c', type = 'integer', is_nullable = true}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
            {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}},
            {name = 'u', unique = true, parts = {{2, 'integer'}}}}}
    ok(tarn_create_table(def))
    local writes = {}
    for n = 1, count do
        writes[#writes + 1] = {'insert', t.old_id, {n, n * 10, 1, n % 2 == 0 and n or box.NULL}}
        if #writes == 200 then ok(write(t, writes)); writes = {} end
    end
    if #writes > 0 then ok(write(t, writes)) end
    local nd = table.deepcopy(def)
    nd.id, nd.ver = t.new_id, 1
    local c_part = {field = 4, type = 'integer', is_nullable = true}
    nd.indexes[#nd.indexes + 1] = {name = 'c_u', unique = true, parts = {c_part}}
    t.args = {ver = 1, timeout = 5, def = nd, uix = {}, uix_drop = {}, lazy_drop = {},
        builds = {{12, 'c_u', true, {c_part}}}, drop = {}, renames = {},
        engine_change = {old_id = t.old_id, old_ver = 0, old_engine = engine, old_name = t.name, rebuild = true,
            durable = not volatile or nil}}
    ok(tarn_job_begin({owner = t.owner, kind = 'alter', space = t.name, names = {t.name}, ver = 1,
        a = t.args, phase = 'prepare'}, 0))
    jobs[t.owner] = t
    return t
end
-- Whether the copy's new space holds the table's rows, and its unique index
-- on c one entry per non-NULL c.
local function image(t)
    local src, dst = rows(t.old_id), rows(t.new_id)
    local cs = 0
    for _, r in ipairs(src) do if r[4] ~= box.NULL then cs = cs + 1 end end
    local filled = 0
    for _, r in box.space[t.new_id].index.c_u:pairs(box.NULL, {iterator = 'GT'}) do
        if r[4] ~= nil then filled = filled + 1 end
    end
    return json.encode(src) == json.encode(dst) and filled == cs
end
local function set(knobs)
    local before = {}
    for k, v in pairs(knobs) do before[k] = tarn.cfg[k] == nil and box.NULL or tarn.cfg[k]; tarn.cfg[k] = v end
    return function()
        for k, v in pairs(before) do tarn.cfg[k] = v ~= box.NULL and v or nil end
    end
end
-- The copy's durable captures: the spaces named after its table.
local PREFIX = '_tarn_engine_capture_'
local function durables(t)
    local out = {}
    for _, prefix in ipairs({PREFIX .. t.old_id .. '_', '_tarn_engine_holes_' .. t.old_id .. '_'}) do
        for _, s in box.space._space.index.name:pairs({prefix}, {iterator = 'GE'}) do
            if s.name:sub(1, #prefix) ~= prefix then break end
            out[#out + 1] = s.name
        end
    end
    return out
end
-- The durable capture's entry for a key ({key, generation}), or nil; and
-- its hole for one.
local function entry(t, key, prefix)
    local r = record(t)
    local sp = box.space[(prefix or PREFIX) .. t.old_id .. '_' .. (r.capture:gsub('-', ''))]
    local e = sp and confirmed(function() return sp:get{mp.encode(key)} end)
    return e and e:totable() or nil
end
local function hole(t, key) return entry(t, key, '_tarn_engine_holes_') end
-- The keys of the in-memory capture, sorted.
local function mirror(t)
    local track, out = tarn.engine.tracks[t.old_id], {}
    for _, key in ipairs(track and track.valid and track.list or {}) do out[#out + 1] = key[1] end
    table.sort(out)
    return out
end
local function same(a, b) return json.encode(a) == json.encode(b) end
-- A copy held after its first page (rows 1 to 4).
local function held_copy(t)
    hook('engine_copy', 'hold')
    local building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    return building
end
local function publish(t)
    local prepared = call('prepare', t)
    local exact = prepared[1] == 'ok' and image(t)
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    return prepared, exact
end

local function exercise(engine)
    local label = function(s) return engine .. ': ' .. s end
    local restore, t, building, built, prepared, exact, resumes, names

    -- The capture lost during the copy: rows written before the loss, after
    -- it and after the resume all reach the copy, which goes on from its
    -- cursor with the same capture. (Rows 1 to 4 are in the first page.)
    restore = set{engine_copy_rows = 4, engine_fence_keys = 64}
    t = table_and_copy(engine, 'lost', 40)
    building = held_copy(t)
    local token, copied = record(t).capture, record(t).copied
    ok(set_v(t, 2, 2002))
    tarn.engine.invalidate(true)
    ok(set_v(t, 3, 3003))
    resumes = tarn.engine.resumes or 0
    hook('engine_resumed', 'hold')
    hook('engine_copy', 'release')
    hook('engine_resumed', 'reached')
    local resumed = mirror(t)
    ok(set_v(t, 1, 1001))
    hook('engine_resumed', 'release')
    built = join(building)
    local r = record(t)
    check(label('a capture lost during the copy resumes from the durable capture, with the same capture and cursor'),
        built[1] == 'ok' and r.capture == token and (r.resets or 0) == 0 and r.resumes == 1
        and (tarn.engine.resumes or 0) == resumes + 1 and copied == 4 and r.copied == 40
        and #durables(t) == 2, {built, token, r, copied, durables(t)})
    check(label('the resumed capture holds the rows written before and after the loss, and after the resume'),
        same(resumed, {2, 3}) and same(mirror(t), {1, 2, 3}), {resumed, mirror(t)})
    prepared, exact = publish(t)
    check(label('that copy is exact'), prepared[1] == 'ok' and exact, prepared)
    check(label('publication drops the durable capture'), #durables(t) == 0, durables(t))
    restore()

    -- A pass settles the keys it reread: the capture rebuilt after a loss
    -- holds only the rows written since its swap.
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 2, engine_fence_keys = 64}
    t = table_and_copy(engine, 'settled', 40)
    building = held_copy(t)
    ok(set_v(t, 1, 1001)); ok(set_v(t, 2, 2002))
    hook('engine_catch_up', 'hold')
    hook('engine_copy', 'release')
    hook('engine_catch_up', 'reached')
    local settled = record(t).settled or 0
    ok(set_v(t, 3, 3003))
    tarn.engine.invalidate(true)
    ok(set_v(t, 4, 4004))
    hook('engine_resumed', 'hold')
    hook('engine_catch_up', 'release')
    hook('engine_resumed', 'reached')
    resumed = mirror(t)
    hook('engine_resumed', 'release')
    built = join(building)
    check(label('a pass records the generation it settled, and the capture rebuilt after a loss holds only the rows written since'),
        built[1] == 'ok' and settled >= 1 and same(resumed, {3, 4}) and record(t).resumes == 1,
        {built, settled, resumed, record(t)})
    prepared, exact = publish(t)
    check(label('that copy is exact'), prepared[1] == 'ok' and exact, prepared)
    restore()

    -- The capture lost during a pass, after its swap: the pass fails for now
    -- (1213), and the call sent again rebuilds the capture with the keys the
    -- pass took, which it had not settled, and those written since.
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 2, engine_fence_keys = 64}
    t = table_and_copy(engine, 'mid_pass', 40)
    building = held_copy(t)
    ok(set_v(t, 1, 1001)); ok(set_v(t, 2, 2002))
    hook('engine_catch_up_swapped', 'hold')
    hook('engine_copy', 'release')
    hook('engine_catch_up_swapped', 'reached')
    ok(set_v(t, 3, 3003))
    tarn.engine.invalidate(true)
    hook('engine_catch_up_swapped', 'release')
    local first = join(building)
    settled = record(t).settled or 0
    hook('engine_resumed', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_resumed', 'reached')
    resumed = mirror(t)
    hook('engine_resumed', 'release')
    built = join(building)
    check(label('a capture lost during a pass fails it for now; the capture rebuilt holds the keys it had not settled'),
        first[1] ~= 'ok' and first[2] == 1213 and built[1] == 'ok' and settled == 0
        and same(resumed, {1, 2, 3}), {first, built, resumed, settled})
    prepared, exact = publish(t)
    check(label('that copy is exact'), prepared[1] == 'ok' and exact, prepared)
    restore()

    -- A pass clears and rereads only the keys the durable capture notes: a
    -- key whose write rolled back keeps its copy, so that a capture lost
    -- during the pass, rebuilt without that key, misses no copy the pass
    -- took out. (The capture is lost once the pass's deletes commit.)
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 2, engine_fence_keys = 64}
    t = table_and_copy(engine, 'pass_rolled_back', 40)
    building = held_copy(t)
    local refused = write(t, {{'update', t.old_id, {1}, {{'=', 2, 1001}}}, {'insert', t.old_id, {2, 2, 1, box.NULL}}})
    ok(set_v(t, 3, 3003))
    local deleted, lost = {}, false
    local trigger = box.space[t.new_id]:on_replace(function(old, new)
        if old ~= nil and new == nil and old[1] <= 4 then
            deleted[#deleted + 1] = old[1]
            if not lost then
                lost = true
                box.on_commit(function() tarn.engine.tracks[t.old_id].valid = false end)
            end
        end
    end)
    hook('engine_copy', 'release')
    first = join(building)
    box.space[t.new_id]:on_replace(nil, trigger)
    built = call('build', t)
    prepared, exact = publish(t)
    check(label('a pass clears only the keys the durable capture notes; one whose write rolled back keeps its copy'),
        refused[1] ~= 'ok' and same(deleted, {3}) and first[1] ~= 'ok' and first[2] == 1213 and built[1] == 'ok'
        and prepared[1] == 'ok' and exact, {refused, deleted, first, built, prepared})
    restore()

    -- So does the final pass: a key whose write rolled back keeps its copy
    -- when the capture is lost once the final pass's deletes commit, and
    -- PREPARE sent again resumes and is exact.
    restore = set{engine_copy_rows = 4, engine_fence_keys = 64}
    t = table_and_copy(engine, 'tail_rolled_back', 40)
    ok(call('build', t))
    refused = write(t, {{'update', t.old_id, {1}, {{'=', 2, 1001}}}, {'insert', t.old_id, {2, 2, 1, box.NULL}}})
    ok(set_v(t, 3, 3003))
    deleted, lost = {}, false
    trigger = box.space[t.new_id]:on_replace(function(old, new)
        if old ~= nil and new == nil and old[1] <= 4 then
            deleted[#deleted + 1] = old[1]
            if not lost then
                lost = true
                box.on_commit(function() tarn.engine.tracks[t.old_id].valid = false end)
            end
        end
    end)
    first = call('prepare', t)
    box.space[t.new_id]:on_replace(nil, trigger)
    prepared = call('prepare', t)
    exact = prepared[1] == 'ok' and image(t)
    check(label('the final pass clears only the keys the durable capture notes; one whose write rolled back keeps its copy'),
        refused[1] ~= 'ok' and same(deleted, {3}) and first[1] ~= 'ok' and first[2] == 1213 and prepared[1] == 'ok'
        and exact and record(t).resumes == 1, {refused, deleted, first, prepared})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- A copy the holder rule takes out of the shadow is a hole in the
    -- durable capture: a capture lost after it is rebuilt with that key, and
    -- the final pass puts its row back. Here row 6 takes row 2's c, and a
    -- write to row 2 rolls back, so the source holds a duplicate, which
    -- PREPARE meets (1062).
    restore = set{engine_copy_rows = 4, engine_fence_keys = 64}
    t = table_and_copy(engine, 'hole', 40)
    building = held_copy(t)
    ok(write(t, {{'update', t.old_id, {6}, {{'=', 4, 2}}}}))
    refused = write(t, {{'update', t.old_id, {2}, {{'=', 4, 22}}}, {'insert', t.old_id, {1, 1, 1, box.NULL}}})
    hook('engine_copy', 'release'); hook('engine_copy', 'hold')
    hook('engine_copy', 'reached')
    local copy2, h2 = box.space[t.new_id]:get{2}, hole(t, {2})
    tarn.engine.invalidate(true)
    hook('engine_copy', 'release')
    built = join(building)
    prepared = call('prepare', t)
    decide(t, 'abort'); local ended = call('abort', t); end_job(t)
    check(label('a copy the holder rule takes out is a hole; a capture lost after it is rebuilt with it, and PREPARE meets the duplicate'),
        refused[1] ~= 'ok' and copy2 == nil and h2 ~= nil and built[1] == 'ok' and prepared[1] ~= 'ok'
        and prepared[2] == 1062 and ended[1] == 'ok' and #durables(t) == 0, {refused, copy2, h2, built, prepared, ended})
    restore()

    -- A write rolled back leaves no entry, a committed one leaves its key's.
    restore = set{engine_copy_rows = 4, engine_fence_keys = 64}
    t = table_and_copy(engine, 'rolled_back', 40)
    building = held_copy(t)
    refused = write(t, {{'update', t.old_id, {5}, {{'=', 2, 5005}}}, {'insert', t.old_id, {1, 1, 1, box.NULL}}})
    ok(set_v(t, 6, 6006))
    local e5, e6 = entry(t, {5}), entry(t, {6})
    hook('engine_copy', 'release')
    built = join(building)
    check(label('a write rolled back leaves no entry in the durable capture; a committed one leaves its key\'s'),
        refused[1] ~= 'ok' and e5 == nil and e6 ~= nil and e6[2] == 1 and built[1] == 'ok', {refused, e5, e6})
    prepared, exact = publish(t)
    check(label('that copy is exact'), prepared[1] == 'ok' and exact, prepared)
    restore()

    -- A write in flight at the loss: the resume waits for it to commit
    -- before it reads the durable capture. (The capture is lost as a write
    -- that fails to note its key loses it, with no status change: no
    -- transaction cut waits for the write first.)
    restore = set{engine_copy_rows = 4, engine_fence_keys = 64}
    t = table_and_copy(engine, 'in_flight', 40)
    building = held_copy(t)
    hook('written', 'hold')
    local writer = spawn(function() return set_v(t, 2, 2002) end)
    hook('written', 'reached')
    tarn.engine.tracks[t.old_id].valid = false
    hook('engine_resume_marked', 'hold')
    hook('engine_copy', 'release')
    fiber.sleep(0.2)
    local waited = record(t).resumes == nil
    hook('written', 'release')
    hook('engine_resume_marked', 'reached')
    hook('engine_resume_marked', 'release')
    local wrote = join(writer)
    built = join(building)
    prepared, exact = publish(t)
    check(label('a resume waits for a write in flight at the loss, and the copy holds it'),
        waited and wrote[1] == 'ok' and built[1] == 'ok' and prepared[1] == 'ok' and exact
        and box.space[t.new_id]:get{2}[2] == 2002, {waited, wrote, built, prepared})
    restore()

    -- A write whose caller left it in the synchronous queue at the loss: the
    -- resume's marker commits after it, and the capture rebuilt holds its key.
    -- (The write inserts row 3 again, behind the copy's cursor. The capture
    -- is lost as above, with no transaction cut.)
    restore = set{engine_copy_rows = 4, engine_fence_keys = 64}
    t = table_and_copy(engine, 'detached', 40)
    ok(write(t, {{'delete', t.old_id, {3}}}))
    building = held_copy(t)
    box.cfg{replication_synchro_quorum = 2}
    local detached = spawn(function() return write(t, {{'insert', t.old_id, {3, 888, 1, box.NULL}}}) end)
    local deadline = fiber.clock() + 5
    while box.info.synchro.queue.len == 0 and fiber.clock() < deadline do fiber.sleep(0.01) end
    local queued = box.info.synchro.queue.len > 0
    detached.fiber:cancel()
    deadline = fiber.clock() + 15
    while not detached.done and fiber.clock() < deadline do fiber.sleep(0.01) end
    local left = detached.done and box.info.synchro.queue.len > 0
    tarn.engine.tracks[t.old_id].valid = false
    hook('engine_resumed', 'hold')
    hook('engine_copy', 'release')
    fiber.sleep(0.2)
    box.cfg{replication_synchro_quorum = 1}
    pcall(detached.fiber.join, detached.fiber)
    hook('engine_resumed', 'reached')
    resumed = mirror(t)
    hook('engine_resumed', 'release')
    built = join(building)
    prepared, exact = publish(t)
    local copy = box.space[t.new_id]:get{3}
    check(label('a write left in the synchronous queue at the loss is in the capture rebuilt after it commits'),
        queued and left and same(resumed, {3}) and built[1] == 'ok' and prepared[1] == 'ok' and exact
        and copy ~= nil and copy[2] == 888, {queued, left, resumed, built, prepared, copy})
    restore()

    -- PREPARE sent again behind its fence, its capture lost: it resumes, and
    -- copies nothing again.
    restore = set{engine_copy_rows = 4, engine_fence_keys = 64}
    t = table_and_copy(engine, 'fenced', 40)
    ok(call('build', t))
    ok(set_v(t, 7, 7007)); ok(set_v(t, 8, 8008))
    token, copied = record(t).capture, record(t).copied
    hook('engine_tail', 'hold')
    local preparing = spawn(function() return call('prepare', t) end)
    hook('engine_tail', 'reached')
    local fenced = record(t).phase
    tarn.engine.invalidate(true)
    hook('engine_tail', 'release')
    first = join(preparing)
    prepared = call('prepare', t)
    r = record(t)
    check(label('PREPARE sent again behind its fence resumes with its capture, and copies nothing again'),
        fenced == 'preparing' and first[1] ~= 'ok' and first[2] == 1213 and prepared[1] == 'ok'
        and r.phase == 'prepared' and r.capture == token and r.copied == copied and (r.resets or 0) == 0
        and r.resumes == 1 and image(t), {fenced, first, prepared, r})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- A copy the SQL server does not mark durable (one from before, or one
    -- with a replica that can't note its writes): no durable capture, and a
    -- capture lost starts the copy over, as before.
    restore = set{engine_copy_rows = 4, engine_fence_keys = 64}
    t = table_and_copy(engine, 'volatile', 40, true)
    building = held_copy(t)
    token = record(t).capture
    ok(set_v(t, 2, 2002))
    tarn.engine.invalidate(true)
    hook('engine_copy', 'release')
    built = join(building)
    names = durables(t)
    prepared, exact = publish(t)
    check(label('a copy not marked durable keeps no durable capture, and a capture lost starts it over'),
        built[1] == 'ok' and record(t).capture ~= token and record(t).resumes == nil and #names == 0
        and prepared[1] == 'ok' and exact, {built, names, prepared})
    restore()

    -- Past the capture's limits, the copy starts over, with a durable capture
    -- of its own; ABORT drops it.
    restore = set{engine_copy_rows = 4, engine_dirty_keys = 8, engine_fence_keys = 64}
    t = table_and_copy(engine, 'over', 40)
    building = held_copy(t)
    token = record(t).capture
    tarn.engine.invalidate(true)
    for n = 1, 10 do ok(set_v(t, n, 100000 + n)) end
    hook('engine_copy', 'release')
    built = join(building)
    names = durables(t)
    check(label('a capture rebuilt past its limits starts the copy over, with a durable capture of its own'),
        built[1] == 'ok' and record(t).capture ~= token and #names == 2
        and names[1] == PREFIX .. t.old_id .. '_' .. (record(t).capture:gsub('-', ''))
        and names[2] == '_tarn_engine_holes_' .. t.old_id .. '_' .. (record(t).capture:gsub('-', '')), {built, names})
    prepared = call('prepare', t)
    exact = prepared[1] == 'ok' and image(t)
    decide(t, 'abort'); ended = call('abort', t); end_job(t)
    check(label('that copy is exact, and ABORT drops its durable capture'),
        exact and ended[1] == 'ok' and #durables(t) == 0, {prepared, ended, durables(t)})
    restore()
end

local function cleanup()
    for _, name in ipairs({'engine_copy', 'engine_catch_up', 'engine_catch_up_swapped', 'engine_resumed',
                           'engine_resume_marked', 'engine_tail', 'written'}) do
        pcall(tarn_test_hook, name, 'release')
    end
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
if not yes then failed = failed + 1; print('  FAIL engine resume test exception: ' .. tostring(err)) end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
