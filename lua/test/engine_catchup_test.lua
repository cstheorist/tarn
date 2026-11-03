-- A copy's catch-up passes (issue #184): while a table is copied into a new
-- space (lua/tarn_engine.lua), the keys written meanwhile are copied again
-- from the confirmed source in passes, so that the capture needs room only
-- for the writes since the last pass, and the final pass behind the fence
-- rereads at most tarn.cfg.engine_fence_keys keys. Passes run during the
-- copy once the capture holds engine_catchup_keys keys, and after it until
-- the capture is within the fence's bound; writes that outpace them fail
-- the change (1205), and ABORT leaves the table as it was.
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

local passed, failed, sequence = 0, 0, 91000
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
local function call(op, t, gen) return _G['tarn_alter_' .. op](t.name, t.args, gen or t.gen) end
local function decide(t, value) return ok(tarn_job_phase(t.owner, value)) end
local function end_job(t) ok(tarn_job_end(t.owner)); jobs[t.owner] = nil end

-- A table (id, v unique, bucket_id, c) of `count` rows, and a rebuild that
-- adds a unique index on the nullable c, which the copy fills as it goes.
local function table_and_copy(engine, tag, count)
    local t = {old_id = id(), new_id = id(), gen = 1, name = 'test.catchup_' .. engine .. '_' .. tag}
    t.owner = 'engine-catchup-' .. t.old_id
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
        engine_change = {old_id = t.old_id, old_ver = 0, old_engine = engine, old_name = t.name, rebuild = true}}
    ok(tarn_job_begin({owner = t.owner, kind = 'alter', space = t.name, names = {t.name}, ver = 1,
        a = t.args, phase = 'prepare'}, 0))
    jobs[t.owner] = t
    t.next = count
    return t
end
-- Writes to three keys from n on: a new v, a new c, and a delete. (Rows
-- inserted after the copy's cursor would lengthen the copy.)
local function churn(t, n)
    local base = 100000 + t.next * 10
    t.next = t.next + 1
    return write(t, {{'update', t.old_id, {n}, {{'=', 2, base + 1}}},
                     {'update', t.old_id, {n + 1}, {{'=', 4, base + 2}}},
                     {'delete', t.old_id, {n + 2}}})
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

local function exercise(engine)
    local label = function(s) return engine .. ': ' .. s end
    local built, prepared, passes, failed

    -- A copy under writes to more keys than the capture holds: passes keep
    -- it small, and the copy never starts over. (A pass comes once the
    -- capture holds half its limit, under engine_catchup_keys.)
    local restore = set{engine_copy_rows = 4, engine_dirty_keys = 48, engine_fence_keys = 8}
    local t = table_and_copy(engine, 'busy', 400)
    passes = tarn.engine.passes or 0
    local building = spawn(function() return call('build', t) end)
    local token, writes, n = nil, 0, 1
    while not building.done do
        local r = record(t)
        token = token or (r and r.phase == 'copying' and r.capture or nil)
        if r and r.phase == 'copied' then break end
        if churn(t, n)[1] == 'ok' then writes = writes + 1 end
        n = n + 3
        if n > 395 then n = 1 end
        fiber.sleep(0)
    end
    built = join(building)
    local after = record(t)
    local track = tarn.engine.tracks[t.old_id]
    check(label('passes run during the copy, which writes to more keys than its capture holds do not restart'),
        built[1] == 'ok' and (tarn.engine.passes or 0) - passes >= 2 and (after.passes or 0) >= 2 and token ~= nil
        and after.capture == token and writes * 3 > 48,
        {built, (tarn.engine.passes or 0) - passes, after.passes, token, after.capture, writes})
    check(label('the final pass is left at most engine_fence_keys keys'), track ~= nil and #track.list <= 8,
        track and #track.list)
    ok(call('prepare', t))
    check(label('the prepared copy holds the table\'s rows'), image(t), {#rows(t.old_id), #rows(t.new_id)})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- A threshold of 0 runs no pass over an empty capture: a copy without
    -- writes isn't taken for one that writes outpace.
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 0, engine_catchup_passes = 2}
    t = table_and_copy(engine, 'zero', 40)
    passes = tarn.engine.passes or 0
    built = call('build', t)
    prepared = built[1] == 'ok' and call('prepare', t) or built
    check(label('a threshold of 0 runs no pass over an empty capture'),
        built[1] == 'ok' and prepared[1] == 'ok' and (tarn.engine.passes or 0) == passes and image(t),
        {built, prepared, (tarn.engine.passes or 0) - passes})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- BUILD and PREPARE in slices (opts.slice, from an SQL server that sends
    -- the step again): a slice of 0 copies a page, or runs a pass, per call,
    -- each returning {'ok', 'more'}; writes between the calls are captured,
    -- and the copy is exact. Then the same with PREPARE alone, which copies
    -- the table in its slices before it fences.
    restore = set{engine_copy_rows = 4, engine_fence_keys = 0}
    for _, first in ipairs({'build', 'prepare'}) do
        t = table_and_copy(engine, 'sliced_' .. first, 60)
        local calls, more, n, last = 0, 0, 1, nil
        for _, op in ipairs(first == 'build' and {'build', 'prepare'} or {'prepare'}) do
            repeat
                last = _G['tarn_alter_' .. op](t.name, t.args, t.gen, {slice = 0})
                calls = calls + 1
                local again = type(last) == 'table' and last[1] == 'ok' and last[2] == 'more'
                if again then more = more + 1 end
                -- (Writes while the copy copies: after it, each call's pass
                -- would meet as many new keys as it reread.)
                if again and record(t).phase == 'copying' then ok(churn(t, n)); n = n + 3; if n > 55 then n = 1 end end
            until not again or calls > 400
        end
        check(label(first .. ' in slices returns for more, and the copy it makes is exact'),
            type(last) == 'table' and last[1] == 'ok' and last[2] == nil and more >= 15 and image(t)
            and record(t).phase == 'prepared', {last, calls, more, record(t).phase})
        decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    end
    restore()
    -- Writes that refill the capture between the calls after the copy: the
    -- budget of passes before the fence counts across calls, and fails the
    -- change.
    restore = set{engine_copy_rows = 4, engine_fence_keys = 0, engine_catchup_passes = 2}
    t = table_and_copy(engine, 'sliced_outpaced', 40)
    local calls, n, last = 0, 1, nil
    repeat
        last = tarn_alter_build(t.name, t.args, t.gen, {slice = 0})
        calls = calls + 1
        ok(churn(t, n)); n = n + 3; if n > 35 then n = 1 end
    until not (last[1] == 'ok' and last[2] == 'more') or calls > 200
    decide(t, 'abort'); local ended = call('abort', t); end_job(t)
    check(label('writes that outpace the passes between calls in slices fail the change'),
        last[1] ~= 'ok' and last[2] == 1205 and tostring(last[3]):find('outpace') ~= nil and calls < 40
        and ended[1] == 'ok', {last, calls, ended})
    restore()
    -- A takeover between slices goes on with the capture and the cursor, and
    -- the old worker's next call is refused; ABORT between slices ends the
    -- change, and a later call is refused.
    restore = set{engine_copy_rows = 4, engine_fence_keys = 0}
    t = table_and_copy(engine, 'sliced_takeover', 40)
    local more3 = true
    for _ = 1, 3 do
        local r = tarn_alter_build(t.name, t.args, t.gen, {slice = 0})
        more3 = more3 and r[1] == 'ok' and r[2] == 'more'
    end
    local token, cursor = record(t).capture, record(t).cursor
    local owner = t.owner .. '-taken'
    local gen2 = ok(tarn_job_touch(owner, t.owner))[2]
    -- (The new worker's first call tells the shard of the takeover.)
    local taken = tarn_alter_build(t.name, t.args, gen2, {slice = 0})
    local stale = tarn_alter_build(t.name, t.args, t.gen, {slice = 0})
    calls = 1
    while taken[1] == 'ok' and taken[2] == 'more' and calls <= 100 do
        taken = tarn_alter_build(t.name, t.args, gen2, {slice = 0}); calls = calls + 1
    end
    jobs[t.owner] = nil; t.owner, t.gen = owner, gen2; jobs[owner] = t
    prepared = call('prepare', t)
    check(label('a takeover between slices goes on with the capture and the cursor; the old worker is refused'),
        more3 and gen2 == 2 and cursor ~= nil and stale[1] ~= 'ok' and stale[2] == 1412 and taken[1] == 'ok'
        and record(t).capture == token and prepared[1] == 'ok' and image(t), {more3, gen2, stale, taken, prepared})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    t = table_and_copy(engine, 'sliced_abort', 40)
    more3 = true
    for _ = 1, 3 do
        local r = tarn_alter_build(t.name, t.args, t.gen, {slice = 0})
        more3 = more3 and r[1] == 'ok' and r[2] == 'more'
    end
    decide(t, 'abort'); ended = call('abort', t)
    local later = tarn_alter_build(t.name, t.args, t.gen, {slice = 0})
    end_job(t)
    check(label('ABORT between slices ends the change, and a later call is refused'),
        more3 and ended[1] == 'ok' and later[1] ~= 'ok' and later[2] == 1412 and box.space[t.new_id] == nil
        and tarn.engine.tracks[t.old_id] == nil, {more3, ended, later})
    restore()
    -- A slice runs from the call's start: a call that waited longer than its
    -- slice for the table's serial lock copies a page, and returns.
    restore = set{engine_copy_rows = 4, engine_fence_keys = 0}
    t = table_and_copy(engine, 'sliced_waited', 40)
    hook('engine_copy', 'hold')
    building = spawn(function() return tarn_alter_build(t.name, t.args, t.gen, {slice = 0}) end)
    hook('engine_copy', 'reached')
    local waiting = spawn(function() return tarn_alter_build(t.name, t.args, t.gen, {slice = 0.2}) end)
    fiber.sleep(0.3)
    local copied = record(t).copied
    hook('engine_copy', 'release')
    local first, waited = join(building), join(waiting)
    check(label('a call that waited longer than its slice for the serial lock copies a page, and returns'),
        first[1] == 'ok' and first[2] == 'more' and waited[1] == 'ok' and waited[2] == 'more'
        and record(t).copied == copied + 4, {first, waited, copied, record(t).copied})
    repeat built = tarn_alter_build(t.name, t.args, t.gen, {slice = 0}) until not (built[1] == 'ok' and built[2] == 'more')
    prepared = built[1] == 'ok' and call('prepare', t) or built
    check(label('... and the copy then goes on, exact'), prepared[1] == 'ok' and image(t), {built, prepared})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- The budgets. PREPARE's passes before the fence have a budget of their
    -- own, as BUILD's have: a BUILD whose passes used all of it, then writes
    -- while the other shards build, and PREPARE still catches up.
    restore = set{engine_copy_rows = 4, engine_fence_keys = 0, engine_catchup_passes = 2}
    t = table_and_copy(engine, 'step_budgets', 40)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    ok(churn(t, 1))
    hook('engine_catch_up', 'hold')
    hook('engine_copy', 'release')
    hook('engine_catch_up', 'reached')
    ok(churn(t, 10))
    hook('engine_catch_up', 'release')
    built = join(building)
    local build_passes = record(t).passes or 0
    ok(churn(t, 20))
    prepared = call('prepare', t)
    check(label('PREPARE\'s passes have a budget of their own after BUILD\'s used all of theirs'),
        built[1] == 'ok' and build_passes == 2 and prepared[1] == 'ok' and (record(t).passes or 0) > build_passes
        and image(t), {built, build_passes, prepared, record(t).passes})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()
    -- The copy's restarts count until it reaches the end of the table: ten
    -- BUILDs, each after a capture loss, each copy the table again; but a
    -- capture lost eight times before the copy gets there fails the change.
    restore = set{engine_copy_rows = 4, engine_fence_keys = 0}
    t = table_and_copy(engine, 'losses', 40)
    local outcomes = {}
    for _ = 1, 10 do
        local r = call('build', t)
        outcomes[#outcomes + 1] = r[1] == 'ok' and 'ok' or tostring(r[2])
        tarn.engine.invalidate(true)
    end
    prepared = call('prepare', t)
    check(label('ten BUILDs, each after a capture loss, each copy the table again'),
        table.concat(outcomes, ',') == string.rep('ok,', 9) .. 'ok' and prepared[1] == 'ok' and image(t),
        {outcomes, prepared})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    t = table_and_copy(engine, 'lost_often', 40)
    for i = 1, 8 do
        hook('engine_copy', 'hold')
        if i == 1 then building = spawn(function() return call('build', t) end) end
        hook('engine_copy', 'reached')
        tarn.engine.invalidate(true)
        hook('engine_copy', 'release')
    end
    built = join(building)
    decide(t, 'abort'); ended = call('abort', t); end_job(t)
    check(label('a capture lost eight times before the copy reaches the end of the table fails the change'),
        built[1] ~= 'ok' and built[2] == 1205 and tostring(built[3]):find('repeatedly') ~= nil and ended[1] == 'ok',
        {built, ended})
    restore()

    -- Unique values that change hands across a pass: no false duplicate.
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 2, engine_fence_keys = 0}
    t = table_and_copy(engine, 'swap', 60)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    -- Rows 2 and 4 swap c by way of NULL, as do rows 6 and 8, the second
    -- pair after a pass.
    ok(write(t, {{'update', t.old_id, {2}, {{'=', 4, box.NULL}}}}))
    ok(write(t, {{'update', t.old_id, {4}, {{'=', 4, 2}}}}))
    ok(write(t, {{'update', t.old_id, {2}, {{'=', 4, 4}}}}))
    hook('engine_catch_up', 'hold')
    hook('engine_copy', 'release')
    hook('engine_catch_up', 'reached')
    ok(write(t, {{'update', t.old_id, {6}, {{'=', 4, box.NULL}}}}))
    ok(write(t, {{'update', t.old_id, {8}, {{'=', 4, 6}}}}))
    ok(write(t, {{'update', t.old_id, {6}, {{'=', 4, 8}}}}))
    -- An insert, and a row moved to a new primary key, which takes the c
    -- row 10 gives up.
    ok(write(t, {{'insert', t.old_id, {1001, 10010, 1, 1001}}}))
    ok(write(t, {{'update', t.old_id, {10}, {{'=', 4, box.NULL}}}}))
    ok(write(t, {{'delete', t.old_id, {12}}, {'insert', t.old_id, {1012, 120, 1, 10}}}))
    hook('engine_catch_up', 'release')
    built = join(building)
    prepared = built[1] == 'ok' and call('prepare', t) or built
    check(label('unique values that change hands across a pass meet no false duplicate'),
        built[1] == 'ok' and prepared[1] == 'ok' and image(t), {built, prepared})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- A row written since a pass's swap that held the unique value a reread
    -- row holds now: its copy leaves the shadow, and the final pass puts it
    -- back. (Rows 2 and 4 are in the first page; row 2 takes the c of row
    -- 4, which gives it up, after the swap.)
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 1, engine_fence_keys = 0}
    t = table_and_copy(engine, 'holder', 40)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    ok(write(t, {{'update', t.old_id, {2}, {{'=', 2, 2222}}}}))
    hook('engine_catch_up_swapped', 'hold')
    hook('engine_copy', 'release')
    hook('engine_catch_up_swapped', 'reached')
    ok(write(t, {{'update', t.old_id, {4}, {{'=', 4, box.NULL}}}}))
    ok(write(t, {{'update', t.old_id, {2}, {{'=', 4, 4}}}}))
    hook('engine_catch_up_swapped', 'release')
    built = join(building)
    prepared = built[1] == 'ok' and call('prepare', t) or built
    check(label('a row written since a pass\'s swap that held a reread row\'s unique value leaves until the final pass'),
        built[1] == 'ok' and prepared[1] == 'ok' and image(t) and box.space[t.new_id]:get{2}[4] == 4,
        {built, prepared})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- A write in flight as a pass swaps in its fresh set: its key is in the
    -- old set, and the pass rereads it only once it has committed.
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 1, engine_fence_keys = 0}
    t = table_and_copy(engine, 'settle', 40)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    hook('written', 'hold')
    local writer = spawn(function() return write(t, {{'update', t.old_id, {30}, {{'=', 2, 777}}}}) end)
    hook('written', 'reached')
    hook('engine_copy', 'release')
    fiber.sleep(0.2)
    hook('written', 'release')
    local wrote = join(writer)
    built = join(building)
    prepared = built[1] == 'ok' and call('prepare', t) or built
    check(label('a pass rereads a key whose write was in flight at its swap once that write commits'),
        wrote[1] == 'ok' and built[1] == 'ok' and prepared[1] == 'ok' and image(t)
        and box.space[t.new_id]:get{30}[2] == 777, {wrote, built, prepared})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- A write whose caller left it waiting in the synchronous queue at a
    -- pass's swap: the pass's marker commits after it, and its row is reread
    -- as it committed. (The write inserts row 3 again, behind the copy's
    -- cursor and not in the shadow: no other write of the pass waits for
    -- it, and no page reads it again.)
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 1, engine_fence_keys = 0}
    t = table_and_copy(engine, 'detached', 40)
    ok(write(t, {{'delete', t.old_id, {3}}}))
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    box.cfg{replication_synchro_quorum = 2}
    local detached = spawn(function() return write(t, {{'insert', t.old_id, {3, 888, 1, box.NULL}}}) end)
    local deadline = fiber.clock() + 5
    while box.info.synchro.queue.len == 0 and fiber.clock() < deadline do fiber.sleep(0.01) end
    local queued = box.info.synchro.queue.len > 0
    detached.fiber:cancel()
    deadline = fiber.clock() + 15
    while not detached.done and fiber.clock() < deadline do fiber.sleep(0.01) end
    local left = detached.done and box.info.synchro.queue.len > 0
    hook('engine_copy', 'release')
    fiber.sleep(0.2)
    box.cfg{replication_synchro_quorum = 1}
    pcall(detached.fiber.join, detached.fiber)
    built = join(building)
    prepared = built[1] == 'ok' and call('prepare', t) or built
    local copy = box.space[t.new_id]:get{3}
    check(label('a write left in the synchronous queue at a pass\'s swap is reread as it committed'),
        queued and left and built[1] == 'ok' and prepared[1] == 'ok' and box.space[t.old_id]:get{3} ~= nil
        and copy ~= nil and copy[2] == 888 and image(t), {queued, left, built, prepared, copy})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- A pass that fails after its swap puts its keys back: the next BUILD
    -- goes on with them, and the copy is exact.
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 1, engine_fence_keys = 0}
    t = table_and_copy(engine, 'interrupted', 40)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    ok(write(t, {{'update', t.old_id, {2}, {{'=', 2, 2222}}}}))
    hook('engine_catch_up_swapped', 'hold')
    hook('engine_copy', 'release')
    hook('engine_catch_up_swapped', 'reached')
    local token = record(t).capture
    building.fiber:cancel()
    hook('engine_catch_up_swapped', 'release')
    pcall(building.fiber.join, building.fiber)
    local track = tarn.engine.tracks[t.old_id]
    local kept = false
    for _, key in ipairs(track and track.list or {}) do kept = kept or key[1] == 2 end
    built = call('build', t)
    prepared = built[1] == 'ok' and call('prepare', t) or built
    check(label('a pass that fails after its swap puts its keys back, and the copy is exact'),
        kept and record(t).capture == token and built[1] == 'ok' and prepared[1] == 'ok' and image(t)
        and box.space[t.new_id]:get{2}[2] == 2222, {kept, built, prepared})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- A pass that fails after its deletes, with row 2's copy gone from the
    -- shadow: its keys go back too, and the call sent again rereads them.
    -- (The engine's transaction cut is lost as another job's cut would lose
    -- it, once the delete of row 2's copy commits.)
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 1, engine_fence_keys = 0}
    t = table_and_copy(engine, 'deleted', 40)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    ok(write(t, {{'update', t.old_id, {2}, {{'=', 2, 2222}}}}))
    local fired = false
    local trigger = box.space[t.new_id]:on_replace(function(old, new)
        if old ~= nil and new == nil and old[1] == 2 and not fired then
            fired = true
            box.on_commit(function() tarn.engine.tickets.invalidate() end)
        end
    end)
    hook('engine_copy', 'release')
    built = join(building)
    box.space[t.new_id]:on_replace(nil, trigger)
    failed = built
    built = call('build', t)
    prepared = built[1] == 'ok' and call('prepare', t) or built
    check(label('a pass that fails after removing its keys\' copies puts them back, and the call sent again rereads them'),
        fired and failed[1] ~= 'ok' and failed[2] == 1213 and built[1] == 'ok' and prepared[1] == 'ok' and image(t)
        and box.space[t.new_id]:get{2} ~= nil and box.space[t.new_id]:get{2}[2] == 2222, {fired, failed, built, prepared})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- Another engine job's cut during a pass, while a commit to its table is
    -- in flight: the pass fails for now (1213), and puts its keys back; both
    -- copies then finish exact.
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 1, engine_fence_keys = 0}
    t = table_and_copy(engine, 'first', 40)
    local other = table_and_copy(engine, 'second', 40)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    ok(write(t, {{'update', t.old_id, {2}, {{'=', 2, 2222}}}}))
    hook('engine_catch_up_swapped', 'hold')
    hook('engine_copy', 'release')
    hook('engine_catch_up_swapped', 'reached')
    hook('written', 'hold')
    local writer = spawn(function() return write(other, {{'update', other.old_id, {5}, {{'=', 2, 555}}}}) end)
    hook('written', 'reached')
    local second = spawn(function() return call('build', other) end)
    local deadline = fiber.clock() + 5
    while tarn.engine.tickets.ready() and fiber.clock() < deadline do fiber.sleep(0.01) end
    local cutting = not tarn.engine.tickets.ready()
    hook('engine_catch_up_swapped', 'release')
    deadline = fiber.clock() + 15
    while not building.done and fiber.clock() < deadline do fiber.sleep(0.01) end
    hook('written', 'release')
    failed = join(building)
    local wrote, built2 = join(writer), join(second)
    built = call('build', t)
    prepared = built[1] == 'ok' and call('prepare', t) or built
    local prepared2 = built2[1] == 'ok' and call('prepare', other) or built2
    check(label('another job\'s cut fails a pass for now, which puts its keys back; both copies are exact'),
        cutting and failed[1] ~= 'ok' and failed[2] == 1213 and wrote[1] == 'ok' and built[1] == 'ok'
        and prepared[1] == 'ok' and prepared2[1] == 'ok' and image(t) and image(other)
        and box.space[t.new_id]:get{2}[2] == 2222, {cutting, failed, wrote, built, built2, prepared, prepared2})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    decide(other, 'apply'); ok(call('apply', other)); end_job(other)
    restore()

    -- A pass that fails once the writes since its swap have filled the
    -- capture: its keys don't fit back, so the capture is lost, and the copy
    -- starts over. (Passes come at 4 keys.)
    restore = set{engine_copy_rows = 4, engine_dirty_keys = 8, engine_fence_keys = 0}
    t = table_and_copy(engine, 'overflow', 40)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    ok(churn(t, 1)); ok(churn(t, 4))
    hook('engine_catch_up_swapped', 'hold')
    hook('engine_copy', 'release')
    hook('engine_catch_up_swapped', 'reached')
    token = record(t).capture
    ok(churn(t, 20)); ok(churn(t, 23))
    building.fiber:cancel()
    hook('engine_catch_up_swapped', 'release')
    pcall(building.fiber.join, building.fiber)
    track = tarn.engine.tracks[t.old_id]
    local lost = track ~= nil and not track.valid
    built = call('build', t)
    prepared = built[1] == 'ok' and call('prepare', t) or built
    check(label('a pass that fails with no room left for its keys loses the capture, and the copy starts over'),
        lost and record(t).capture ~= token and built[1] == 'ok' and prepared[1] == 'ok' and image(t),
        {lost, built, prepared})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- A pass inside PREPARE, over more keys than a batch: PREPARE catches up
    -- first, then rereads the rest behind the fence.
    restore = set{engine_copy_rows = 64, engine_fence_keys = 16}
    t = table_and_copy(engine, 'prepare', 400)
    ok(call('build', t))
    for n = 1, 300 do ok(write(t, {{'update', t.old_id, {n}, {{'=', 2, 200000 + n}}}})) end
    passes = record(t).passes or 0
    prepared = call('prepare', t)
    check(label('PREPARE catches up first, over more keys than a batch, and the copy is exact'),
        prepared[1] == 'ok' and (record(t).passes or 0) > passes and image(t), {prepared, record(t).passes})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- Writes that refill the capture during each pass of the copy fail the
    -- change too.
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 3, engine_catchup_passes = 2}
    t = table_and_copy(engine, 'refilled', 60)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    ok(churn(t, 1))
    hook('engine_catch_up', 'hold')
    hook('engine_copy', 'release')
    for i = 1, 3 do
        hook('engine_catch_up', 'reached')
        ok(churn(t, 10 + i * 3))
        hook('engine_catch_up', 'release'); hook('engine_catch_up', 'hold')
    end
    hook('engine_catch_up', 'release')
    built = join(building)
    decide(t, 'abort'); local ended = call('abort', t); end_job(t)
    check(label('writes that refill the capture during each pass of the copy fail the change, and ABORT ends it'),
        built[1] ~= 'ok' and built[2] == 1205 and tostring(built[3]):find('outpace') ~= nil and ended[1] == 'ok'
        and box.space[t.new_id] == nil, {built, ended})
    restore()

    -- A capture lost after a pass: the copy starts over, with a new capture.
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 2, engine_fence_keys = 0}
    t = table_and_copy(engine, 'lost', 40)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    ok(churn(t, 1)); ok(churn(t, 4))
    hook('engine_catch_up', 'hold')
    hook('engine_copy', 'release')
    hook('engine_catch_up', 'reached')
    local first = record(t).capture
    tarn.engine.invalidate(true)
    hook('engine_catch_up', 'release')
    built = join(building)
    if built[1] ~= 'ok' then built = call('build', t) end
    prepared = built[1] == 'ok' and call('prepare', t) or built
    check(label('a capture lost after a pass starts the copy over, which is then exact'),
        built[1] == 'ok' and prepared[1] == 'ok' and record(t).capture ~= first and image(t),
        {built, prepared, first, record(t).capture})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- A capture lost during a pass fails it for now (1213), and the call sent
    -- again starts the copy over.
    restore = set{engine_copy_rows = 4, engine_catchup_keys = 1, engine_fence_keys = 0}
    t = table_and_copy(engine, 'lost_during', 40)
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    ok(write(t, {{'update', t.old_id, {2}, {{'=', 2, 2222}}}}))
    hook('engine_catch_up_swapped', 'hold')
    hook('engine_copy', 'release')
    hook('engine_catch_up_swapped', 'reached')
    first = record(t).capture
    tarn.engine.invalidate(true)
    hook('engine_catch_up_swapped', 'release')
    failed = join(building)
    built = call('build', t)
    prepared = built[1] == 'ok' and call('prepare', t) or built
    check(label('a capture lost during a pass fails it for now, and the copy starts over, exact'),
        failed[1] ~= 'ok' and failed[2] == 1213 and built[1] == 'ok' and prepared[1] == 'ok'
        and record(t).capture ~= first and image(t), {failed, built, prepared})
    decide(t, 'apply'); ok(call('apply', t)); end_job(t)
    restore()

    -- Writes that outpace the passes before the fence fail the change
    -- (1205), and ABORT leaves the table as it was.
    restore = set{engine_copy_rows = 4, engine_fence_keys = 2, engine_catchup_passes = 2}
    t = table_and_copy(engine, 'outpaced', 40)
    local original = mp.encode(rows(t.old_id))
    hook('engine_copy', 'hold')
    building = spawn(function() return call('build', t) end)
    hook('engine_copy', 'reached')
    ok(churn(t, 1))
    hook('engine_catch_up', 'hold')
    hook('engine_copy', 'release')
    local outpaced = {}
    for _ = 1, 2 do
        hook('engine_catch_up', 'reached')
        local r = churn(t, 10 + #outpaced * 3); outpaced[#outpaced + 1] = r[1]
        hook('engine_catch_up', 'release'); hook('engine_catch_up', 'hold')
    end
    hook('engine_catch_up', 'release')
    built = join(building)
    local current = mp.encode(rows(t.old_id))
    decide(t, 'abort'); local aborted = call('abort', t); end_job(t)
    check(label('writes that outpace the passes before the fence fail the change, and ABORT ends it'),
        built[1] ~= 'ok' and built[2] == 1205 and tostring(built[3]):find('outpace') ~= nil and aborted[1] == 'ok'
        and box.space[t.new_id] == nil and box.space[t.name].id == t.old_id and current ~= original,
        {built, aborted, outpaced})
    restore()
end

local function cleanup()
    for _, name in ipairs({'engine_copy', 'engine_catch_up', 'engine_catch_up_swapped', 'written'}) do
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
if not yes then failed = failed + 1; print('  FAIL engine catch-up test exception: ' .. tostring(err)) end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
