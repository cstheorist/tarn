-- ALTER TABLE adds three indexes to a vinyl table on one shard of three
-- replicas (fsync) while writes to another vinyl table go on (issues #254
-- and #263), run by tests/vinylbuild.sh: DIR and the URIs of the three
-- replicas.
--
--   * every replica applies the builds and keeps applying the leader's
--     rows: none waits forever in a build for a write whose CONFIRM comes
--     after the build's DDL row;
--   * each index holds every row of the table, on every replica;
--   * writes go on during the builds and after them.
--
-- lua/test/alter_sync_build_test.lua covers a write to the table itself
-- that waits for its quorum when the build begins.
local net = require('net.box')
local fiber = require('fiber')
local json = require('json')

local uris = {arg[2], arg[3], arg[4]}
local c = {}
for i, uri in ipairs(uris) do
    c[i] = net.connect(uri, {user = 'tarn', password = 'tarn', wait_connected = 30})
    assert(c[i]:is_connected(), 'cannot connect to ' .. uri)
end

local fails = 0
local function check(name, cond, extra)
    if cond then print('  ok   ' .. name)
    else fails = fails + 1; print('  FAIL ' .. name .. (extra and ('  -- ' .. json.encode(extra)) or '')) end
end
local function eval(i, script, ...) return c[i]:eval(script, {...}, {timeout = 60}) end
local function leader()
    for i = 1, 3 do
        local ok, s = pcall(eval, i, 'return box.info.election.state')
        if ok and s == 'leader' then return i end
    end
end
local deadline = fiber.clock() + 60
local L = leader()
while L == nil and fiber.clock() < deadline do fiber.sleep(0.1); L = leader() end
assert(L, 'the shard has no leader')

local ROWS = 20000
-- Two vinyl tables: t, which gets the index, and w, which takes the writes.
local setup = eval(L, [[
    local fiber, json = require('fiber'), require('json')
    local rows = ...
    local deadline = fiber.clock() + 30
    while (box.space._tarn_buckets == nil or tarn.clock.read_check() ~= nil) and fiber.clock() < deadline do
        fiber.sleep(0.05)
    end
    local function ok(r) assert(r[1] == 'ok' or r[1] == 'exists', json.encode(r)); return r end
    ok(tarn_bucket_init(16, {{1, 16}}))
    local format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'}}
    local indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}},
                     {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}}}
    for _, name in ipairs({'test.t', 'test.w'}) do
        ok(tarn_create_table({name = name, engine = 'vinyl', format = format, indexes = indexes}))
    end
    local t = box.space['test.t']
    for start = 1, rows, 500 do
        local writes = {}
        for id = start, math.min(start + 499, rows) do writes[#writes + 1] = {'insert', t.id, {id, id % 7, 1}} end
        ok(tarn_autocommit(writes))
    end
    return {t = t.id, sync = t.is_sync, engine = t.engine, max = box.cfg.replication_synchro_queue_max_size}
]], ROWS)
check('the table is vinyl and synchronous', setup.engine == 'vinyl' and setup.sync == true, setup)

-- Sixteen writers to w, so that writes are in flight, waiting for their
-- quorum, whenever the build's DDL row is written. A write the schema change
-- refuses as stale (1412) is one the SQL server retries.
eval(L, [[
    local fiber = require('fiber')
    local w = box.space['test.w']
    _G.vb = {writes = 0, stale = 0, errors = {}, stop = false, running = 0}
    for f = 1, 16 do
        fiber.create(function()
            _G.vb.running = _G.vb.running + 1
            local id = f * 10000000
            while not _G.vb.stop do
                id = id + 1
                local ok, r = pcall(tarn_autocommit, {{'insert', w.id, {id, id % 7, 1}}})
                if ok and r[1] == 'ok' then _G.vb.writes = _G.vb.writes + 1
                elseif ok and r[2] == 1412 then _G.vb.stale = _G.vb.stale + 1
                elseif #_G.vb.errors < 5 then _G.vb.errors[#_G.vb.errors + 1] = tostring(ok and r[3] or r) end
            end
            _G.vb.running = _G.vb.running - 1
        end)
    end
]])
fiber.sleep(0.3)

-- The steps run in a fiber on the leader: a replica stuck in the build makes
-- the leader lose its quorum, and APPLY then waits.
eval(L, [[
    local fiber = require('fiber')
    local name = 'test.t'
    local sp = box.space[name]
    -- Three indexes, three builds: each DDL row may come ahead of a write's
    -- CONFIRM.
    local function parts(...)
        local p = {}
        for _, f in ipairs({...}) do p[#p + 1] = {field = f, type = 'unsigned'} end
        return p
    end
    local indexes = {{name = 'primary', unique = true, parts = parts(1)},
                     {name = 'bucket_id', unique = false, parts = parts(3)},
                     {name = 'v_idx', unique = false, parts = parts(2, 1)},
                     {name = 'bv_idx', unique = false, parts = parts(3, 2, 1)},
                     {name = 'vb_idx', unique = false, parts = parts(2, 3, 1)}}
    local builds = {}
    for id = 2, 4 do builds[#builds + 1] = {id, indexes[id + 1].name, false, indexes[id + 1].parts} end
    local alt = {ver = 1, timeout = 30, checks = {}, builds = builds, drop = {}, renames = {},
                 def = {name = name, id = sp.id, engine = 'vinyl', format = sp:format(), indexes = indexes,
                        json = '{}', ver = 1}}
    fiber.create(function()
        local before = _G.vb.writes
        local out = {}
        local ok, err = pcall(function()
            out.build = tarn_alter_build(name, alt)
            out.prepare = tarn_alter_prepare(name, alt, 1)
            out.apply = tarn_alter_apply(name, alt, 1)
        end)
        out.error = not ok and tostring(err) or nil
        out.writes_during = _G.vb.writes - before
        out.max = box.cfg.replication_synchro_queue_max_size
        out.vclock = box.info.vclock[box.info.id]
        _G.vb.alter = out
    end)
]])
local function outcome()
    local ok, r = pcall(eval, L, 'return _G.vb.alter')
    return ok and r or nil
end
-- Replicas whose applier waits in a vinyl build's dump: the deadlock of #254.
local in_dump = {}
local function look()
    for i = 1, 3 do
        local ok, r = pcall(c[i].eval, c[i], [[
            for _, f in pairs(require('fiber').info({backtrace = true})) do
                if f.name:find('^applier') then
                    for _, fr in ipairs(f.backtrace or {}) do
                        if tostring(fr.C or ''):find('vy_scheduler_dump') then return true end
                    end
                end
            end
            return false
        ]], {}, {timeout = 2})
        if ok and r then in_dump[i] = true end
    end
end
deadline = fiber.clock() + 30
local alter = outcome()
while alter == nil and fiber.clock() < deadline do
    fiber.sleep(0.5)
    look()
    alter = outcome()
end
if alter == nil then
    local seen = {}
    for i in pairs(in_dump) do seen[#seen + 1] = i end
    table.sort(seen)
    local states = {}
    for i = 1, 3 do
        local ok, r = pcall(c[i].eval, c[i], 'return {vclock = box.info.vclock, election = box.info.election.state}',
                            {}, {timeout = 5})
        states[i] = ok and r or tostring(r)
    end
    check('ALTER finishes within 30 s', false, {appliers_seen_in_vinyl_dump = seen, replicas = states})
    print(string.format('vinyl build: %d FAILED', fails))
    os.exit(1)
end
local function done(r) return r ~= nil and r[1] == 'ok' end
check('ALTER runs BUILD, PREPARE and APPLY', done(alter.build) and done(alter.prepare) and done(alter.apply), alter)
check('the synchro queue limit is back', alter.max == setup.max, {before = setup.max, after = alter.max})

local id = eval(L, 'return box.info.id')
local function applied(i)
    local ok, r = pcall(eval, i, [[
        local id = ...
        local t = box.space['test.t']
        return {vclock = box.info.vclock[id] or 0, index = t.index.vb_idx ~= nil}
    ]], id)
    return ok and r or {error = tostring(r)}
end
for i = 1, 3 do
    if i ~= L then
        local r = applied(i)
        deadline = fiber.clock() + 15
        while not (r.index and (r.vclock or 0) >= alter.vclock) and fiber.clock() < deadline do
            fiber.sleep(0.1)
            r = applied(i)
        end
        check(string.format('replica %d applies the build and the rows after it', i),
              r.index and (r.vclock or 0) >= alter.vclock, {leader_lsn = alter.vclock, replica = r})
    end
end

local writes = eval(L, 'return _G.vb.writes')
fiber.sleep(1)
local after = eval(L, 'return {writes = _G.vb.writes, errors = _G.vb.errors}')
check('writes went on during the build', alter.writes_during > 0, alter)
check('writes go on after the build', after.writes > writes, {before = writes, after = after.writes})
check('no write failed but for retries of stale ones', #after.errors == 0, after.errors)

eval(L, '_G.vb.stop = true')
deadline = fiber.clock() + 15
while eval(L, 'return _G.vb.running') > 0 and fiber.clock() < deadline do fiber.sleep(0.05) end
local lsn = eval(L, 'return box.info.vclock[box.info.id]')
for i = 1, 3 do
    deadline = fiber.clock() + 15
    local r = applied(i)
    while (r.vclock or 0) < lsn and fiber.clock() < deadline do fiber.sleep(0.1); r = applied(i) end
    local counts = eval(i, [[
        local t = box.space['test.t']
        local out = {}
        for _, name in ipairs({'primary', 'v_idx', 'bv_idx', 'vb_idx'}) do
            local n = 0
            if t.index[name] ~= nil then for _ in t.index[name]:pairs() do n = n + 1 end end
            out[name] = n
        end
        return out
    ]])
    check(string.format('replica %d: each index holds every row', i), counts.primary == ROWS
          and counts.v_idx == ROWS and counts.bv_idx == ROWS and counts.vb_idx == ROWS, counts)
end

print(fails == 0 and 'vinyl build: all checks passed' or string.format('vinyl build: %d FAILED', fails))
os.exit(fails == 0 and 0 or 1)
