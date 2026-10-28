-- Recovery and garbage collection move on past what they cannot resolve
-- (issue #129). Run by recovery_faults.sh: S1, S2, S3 are the three shards,
-- S3_PID the third's process, which this test pauses and resumes.
--
-- While shard 3 is paused, shards 1 and 2 each hold more prepared
-- participants that need it (its decision records) than a recovery pass
-- takes, older than every other undecided transaction: each healthy shard's
-- recovery meets that blocked prefix first. Newer abandoned transactions of
-- shards 1 and 2 alone must still be resolved, and their records collected
-- past the blocked ones, which a garbage-collection pass looks at more of
-- than its scan takes; the blocked ones must keep their records and locks.
-- Once shard 3 is resumed, the blocked ones are resolved too.
local net = require('net.box')
local json = require('json')
local fiber = require('fiber')

local opts = {user = 'tarn', password = 'tarn', wait_connected = 5}
local c = {net.connect(os.getenv('S1'), opts), net.connect(os.getenv('S2'), opts),
           net.connect(os.getenv('S3'), opts)}
for i = 1, 3 do assert(c[i]:is_connected(), 'cannot connect to shard ' .. i) end
local pid3 = assert(os.getenv('S3_PID'))

local pass, fail = 0, 0
local function check(name, cond, extra)
    if cond then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. (extra and ('  -- ' .. json.encode(extra)) or '')) end
end
local function wait_for(cond, secs)
    local deadline = fiber.clock() + secs
    while fiber.clock() < deadline do
        if cond() then return true end
        fiber.sleep(0.2)
    end
    return cond()
end

local def = {
    name = 'test.rec', engine = 'memtx',
    format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'unsigned'},
              {name = 'bucket_id', type = 'unsigned'}},
    indexes = {{name = 'primary', unique = true, parts = {{field = 'id', type = 'unsigned'}}},
               {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}}},
}
for i = 1, 3 do c[i]:call('tarn_create_table', {def}) end
c[1]:reload_schema()
local sid = c[1].space['test.rec'].id
local BLOCKED = 250   -- (more than a recovery pass takes: recovery_batch is 200)
for i = 1, 2 do
    -- Undecided transactions are resolved after a second; a failed one, and
    -- an unreachable shard, are tried again after at most 2 s; records
    -- expire 2 s after they are written; a GC pass looks at no more than
    -- 200 records of each space (gc_batch * 40), fewer than the blocked
    -- ones, and deletes at most 20 (gc_batch * 4).
    c[i]:eval([[tarn.cfg.recovery_after = 1; tarn.cfg.recovery_backoff_max = 2
                tarn.cfg.max_txn_age = 1; tarn.cfg.clock_skew = 1; tarn.cfg.gc_batch = 5]])
end
local function prepare(i, txid, dshard, parts, id)
    return c[i]:call('tarn_prepare', {txid, dshard, parts, {{'replace', sid, {id, 1, 1}}}, {}, {}, fiber.time()})
end
local function info(i) return c[i]:call('tarn_info') end

print('shard 3 paused; transactions that need it, then newer ones that do not')
os.execute('kill -STOP ' .. pid3)
local ok_all = true
for k = 1, BLOCKED do
    -- (shard 3 is their decision shard, and never prepared)
    local a = prepare(1, 'B1-' .. k, 3, {1, 3}, 1000 + k)
    local b = prepare(2, 'B2-' .. k, 3, {2, 3}, 2000 + k)
    if a[1] ~= 'yes' or b[1] ~= 'yes' then ok_all = false end
end
check(BLOCKED .. ' prepared on each healthy shard that need shard 3', ok_all)
fiber.sleep(1.2)
ok_all = true
for k = 1, 10 do
    -- N: prepared everywhere, decision shard 1: recovery commits them;
    -- A: prepared on shard 2 alone: recovery fences shard 1 and aborts them
    local a = prepare(1, 'N-' .. k, 1, {1, 2}, 3000 + k)
    local b = prepare(2, 'N-' .. k, 1, {1, 2}, 4000 + k)
    local d = prepare(2, 'A-' .. k, 1, {1, 2}, 5000 + k)
    if a[1] ~= 'yes' or b[1] ~= 'yes' or d[1] ~= 'yes' then ok_all = false end
end
check('newer transactions of shards 1 and 2 alone prepared', ok_all)

local function newer_resolved()
    for k = 1, 10 do
        if c[1]:call('tarn_txn_status', {'N-' .. k}) ~= 'committed'
           and c[1]:call('tarn_txn_status', {'N-' .. k}) ~= 'none' then return false end
        if c[1]:call('tarn_txn_status', {'A-' .. k}) ~= 'aborted'
           and c[1]:call('tarn_txn_status', {'A-' .. k}) ~= 'none' then return false end
        if c[2]:call('tarn_part_status', {'N-' .. k}) == 'prepared'
           or c[2]:call('tarn_part_status', {'A-' .. k}) == 'prepared' then return false end
    end
    return true
end
check('recovery resolves the newer ones behind the blocked prefix', wait_for(newer_resolved, 30),
      {info(1).recovery, info(2).recovery})
c[1]:reload_schema()
c[2]:reload_schema()
check('... their writes committed', c[1].space['test.rec']:get{3001} ~= nil and c[2].space['test.rec']:get{4010} ~= nil
      and c[2].space['test.rec']:get{5001} == nil)
local i1, i2 = info(1), info(2)
check('the blocked ones stay prepared', i1.prepared == BLOCKED and i2.prepared == BLOCKED, {i1.prepared, i2.prepared})
check('... with their locks', i1.lock_index >= BLOCKED and i2.lock_index >= BLOCKED, {i1.lock_index, i2.lock_index})
local r = c[1]:call('tarn_autocommit', {{{'replace', sid, {1001, 2, 1}}}})
check('... which a write of one of their rows waits for, then fails', r[1] == 'conflict', r)
check('shard 3 waits out a backoff on both', i1.recovery.shards_down[1] == 3 and i2.recovery.shards_down[1] == 3,
      {i1.recovery, i2.recovery})
check('... and recovery counts the failed attempts', i1.stats.recovery_failures > 0 and i1.stats.recovery_attempts > 0,
      i1.stats)
check('a full lap of the prepared participants counts the blocked backlog',
      wait_for(function() return info(1).recovery.prepared >= BLOCKED end, 10), info(1).recovery)
check('the oldest undecided participant is reported', info(1).recovery.oldest_prepared > 1, info(1).recovery)

print('garbage collection past the blocked records')
local function newer_collected()
    for k = 1, 10 do
        for i = 1, 2 do
            if c[i]:call('tarn_part_status', {'N-' .. k}) ~= 'none' then return false end
        end
        if c[2]:call('tarn_part_status', {'A-' .. k}) ~= 'none' then return false end
        if c[1]:call('tarn_txn_status', {'N-' .. k}) ~= 'none'
           or c[1]:call('tarn_txn_status', {'A-' .. k}) ~= 'none' then return false end
    end
    return true
end
check('the newer transactions\' records are collected', wait_for(newer_collected, 30),
      {info(1).recovery.gc, info(2).recovery.gc})
i1, i2 = info(1), info(2)
check('... the blocked ones kept', i1.prepared == BLOCKED and i2.prepared == BLOCKED
      and c[1]:call('tarn_part_status', {'B1-1'}) == 'prepared' and c[2]:call('tarn_part_status', {'B2-' .. BLOCKED}) == 'prepared',
      {i1.prepared, i2.prepared})
check('... in more than one lap of the expired records', i1.recovery.gc.laps.part > 1 and i2.recovery.gc.laps.part > 1,
      {i1.recovery.gc, i2.recovery.gc})

print('shard 3 resumed')
os.execute('kill -CONT ' .. pid3)
check('recovery resolves the blocked ones', wait_for(function()
    return info(1).prepared == 0 and info(2).prepared == 0 end, 60), {info(1).recovery, info(2).recovery})
-- (shard 3 keeps its records past the test: its expiry is the default; the
-- tombstones on shards 1 and 2 expire after 2 s, so check their rows)
check('... aborted: shard 3 fenced their decision', c[3]:call('tarn_txn_status', {'B1-1'}) == 'aborted'
      and c[3]:call('tarn_txn_status', {'B2-' .. BLOCKED}) == 'aborted'
      and c[1].space['test.rec']:get{1000 + BLOCKED} == nil and c[2].space['test.rec']:get{2001} == nil)
check('... their locks released', info(1).lock_index == 0 and info(2).lock_index == 0)
check('... and shard 3 out of backoff', wait_for(function()
    return #info(1).recovery.shards_down == 0 and #info(2).recovery.shards_down == 0 end, 10),
    {info(1).recovery, info(2).recovery})
r = c[1]:call('tarn_autocommit', {{{'replace', sid, {1001, 2, 1}}}})
check('a write of one of their rows goes through', r[1] == 'ok', r)
check('their records are collected in turn', wait_for(function()
    return c[1]:call('tarn_part_status', {'B1-1'}) == 'none' and c[2]:call('tarn_part_status', {'B2-' .. BLOCKED}) == 'none'
end, 60), {info(1).recovery.gc, info(2).recovery.gc})

print(string.format('%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
