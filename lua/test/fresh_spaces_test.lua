-- BK.fresh_spaces: the check that the spaces a request writes and reads may
-- serve it reads the replication mode once per call, and checks each space
-- once, in the order the request names them, without yielding
-- (tla/PlanGuardBatch.tla, issue #310).
-- Run: tarantool lua/test/fresh_spaces_test.lua /tmp/owned-empty-directory
-- Negative controls (each must make this test fail):
-- FRESH_SPACES_MUTANT=scalar|skip-last|stale-mode.
local ffi, fio, fiber = require('ffi'), require('fio'), require('fiber')
local here = fio.abspath(debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './') .. '/'
package.path = here .. '../?.lua;' .. package.path
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, txn_isolation = 'read-confirmed',
        wal_mode = 'fsync', log_level = 3}
local tarn = require('tarn')
tarn.init{shard_id = 1, native = false}
local F, checks = tarn.fresh, 0
local function check(yes, label)
    assert(yes, label); checks = checks + 1; print('ok ' .. label)
end
local function upvalue(fn, wanted)
    for i = 1, 64 do
        local name, value = debug.getupvalue(fn, i)
        if name == nil then break end
        if name == wanted then return value end
    end
    error('missing private test binding: ' .. wanted)
end
local BK = upvalue(tarn_autocommit, 'BK')
local mode = F.mode

local mutant = os.getenv('FRESH_SPACES_MUTANT')
if mutant == 'scalar' then
    BK.fresh_spaces = function(writes, points, ranges)
        for _, w in ipairs(writes or {}) do local sp = box.space[w[2]]; if sp then F.space(sp) end end
        for _, reads in ipairs({points or {}, ranges or {}}) do
            for _, r in ipairs(reads) do local sp = box.space[r[1]]; if sp then F.space(sp) end end
        end
    end
elseif mutant == 'skip-last' then
    local actual = BK.fresh_spaces
    BK.fresh_spaces = function(writes, points, ranges)
        local short = {}
        for i = 1, #(points or {}) - 1 do short[i] = points[i] end
        return actual(writes, short, ranges)
    end
elseif mutant == 'stale-mode' then
    local retained
    BK.fresh_spaces = function(writes, points, ranges)
        retained = retained or F.mode()
        for _, w in ipairs(writes or {}) do local sp = box.space[w[2]]; if sp then F.space_in(sp, retained) end end
        for _, reads in ipairs({points or {}, ranges or {}}) do
            for _, r in ipairs(reads) do local sp = box.space[r[1]]; if sp then F.space_in(sp, retained) end end
        end
    end
elseif mutant ~= nil then error('unknown FRESH_SPACES_MUTANT: ' .. mutant) end

local function guard(writes, points, ranges)
    local txn, active, switches = ffi.C.box_txn_id(), box.is_in_txn(), fiber.self():csw()
    local ok, err = pcall(BK.fresh_spaces, writes, points, ranges)
    check(fiber.self():csw() == switches, 'the check does not switch fibers')
    check(box.is_in_txn() == active and ffi.C.box_txn_id() == txn, 'the check keeps the transaction it runs in')
    return ok, err
end
local function accepts(label, ...) check(guard(...), label) end
local function refuses(reason, label, ...)
    local ok, err = guard(...)
    check(not ok and tostring(err) == 'strong read unavailable: ' .. reason, label)
end

local sync = box.schema.space.create('test.fresh_sync', {is_sync = true})
sync:create_index('pk')
local sync2 = box.schema.space.create('test.fresh_sync2', {is_sync = true})
sync2:create_index('pk')
local async = box.schema.space.create('test.fresh_async')
async:create_index('pk')
local function writes_of(sp, n)
    local w = {}
    for i = 1, n do w[i] = {'replace', sp.id, {i}} end
    return w
end
local function points_of(sp, n)
    local p = {}
    for i = 1, n do p[i] = {sp.id, 0, {i}} end
    return p
end
local page = points_of(sync, 64)

-- The mechanism: one reading of the mode per call, however many spaces the
-- request names; none when it names none that exists.
local calls = 0
F.mode = function() calls = calls + 1; return mode() end
accepts('a page of reads and some writes of synchronous spaces is accepted',
        writes_of(sync2, 8), page, {{sync.id, 0, {1}, {64}, true, true}})
check(calls == 1, 'one reading of the mode for 64 reads, 8 writes and a range')
calls = 0
accepts('a request naming no space is accepted', {}, {}, {})
accepts('nor one naming only dropped spaces', {{'replace', 99999, {1}}}, {{99998, 0, {1}}})
check(calls == 0, 'no reading of the mode when no space named exists')
F.mode = mode

-- Each space is checked: the first one that may not serve the request, in
-- the order it names them (writes, then point reads, then range reads).
F.mode = function() return 2 end
for id, sp in pairs(box.space) do
    if type(id) == 'number' and sp.name:sub(1, 6) == '_tarn_' and sp.name ~= '_tarn_scratch' then
        sp:alter{is_sync = true}
    end
end
accepts('synchronous spaces are accepted when replicated', writes_of(sync2, 2), page)
local tail = points_of(sync, 63)
tail[64] = {async.id, 0, {1}}
refuses('space test.fresh_async is not synchronous', 'an asynchronous space as the last read is refused', {}, tail)
refuses('space test.fresh_async is not synchronous', 'an asynchronous space written first is refused',
        writes_of(async, 1), page)
refuses('space test.fresh_async is not synchronous', 'an asynchronous range read is refused',
        {}, page, {{async.id, 0, {1}, {2}, true, true}})

-- The mode is read again by the next call.
local selected = 1
F.mode = function() return selected end
accepts('unreplicated, an asynchronous space may serve', {}, points_of(async, 4))
selected = 2
refuses('space test.fresh_async is not synchronous', 'the next call reads the mode again', {}, points_of(async, 4))
selected = 3
refuses('unsupported replicated configuration', 'an unsupported configuration is refused at the first space',
        writes_of(sync, 1), page)
F.mode = mode

-- In a transaction holding a read, as PREPARE runs it: kept either way.
box.begin{txn_isolation = 'read-confirmed'}
sync:get{1}
accepts('accepted inside a transaction', {}, page)
F.mode = function() return 2 end
refuses('space test.fresh_async is not synchronous', 'refused inside a transaction, which it keeps',
        {}, {{async.id, 0, {1}}})
F.mode = mode
box.rollback()
print(string.format('fresh spaces: %d checks passed', checks))
os.exit(0)
