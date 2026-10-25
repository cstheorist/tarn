-- Exercises lua/tarn.lua on two single-node shards, playing the role of the
-- Tarn coordinator with net.box streams. Run via lua/test/run.sh.
local net = require('net.box')
local fiber = require('fiber')
local json = require('json')

local S1, S2 = os.getenv('S1'), os.getenv('S2')
local opts = {user = 'tarn', password = 'tarn', wait_connected = 5}
local c = {net.connect(S1, opts), net.connect(S2, opts)}
assert(c[1]:is_connected() and c[2]:is_connected(), 'cannot connect to shards')

local pass, fail = 0, 0
local function check(name, cond, extra)
    if cond then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. (extra and ('  -- ' .. json.encode(extra)) or '')) end
end

-- DDL on both shards: test.acct(id unsigned PK, bal integer, email string unique, bucket_id)
local def = {
    name = 'test.acct', engine = 'memtx',
    format = {{name = 'id', type = 'unsigned'}, {name = 'bal', type = 'integer'},
              {name = 'email', type = 'string', is_nullable = true}, {name = 'bucket_id', type = 'unsigned'}},
    indexes = {{name = 'primary', unique = true, parts = {{field = 'id', type = 'unsigned'}}},
               {name = 'email', unique = true, parts = {{field = 'email', type = 'string', is_nullable = true}}},
               {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}}},
}
for i = 1, 2 do c[i]:call('tarn_create_table', {def}) end
c[1]:reload_schema(); c[2]:reload_schema()
local sid = c[1].space['test.acct'].id
local sid2 = c[2].space['test.acct'].id
assert(sid == sid2, 'space ids must match across shards for this test')

local function get(i, id) return c[i].space['test.acct']:get{id} end
local function bal(i, id) local t = get(i, id); return t and t[2] end

print('autocommit writes')
check('insert on shard 1', c[1]:call('tarn_autocommit', {{{'insert', sid, {1, 100, 'a@x', 1}}}})[1] == 'ok')
check('insert on shard 2', c[2]:call('tarn_autocommit', {{{'insert', sid, {2, 100, 'b@x', 2}}}})[1] == 'ok')
local r = c[1]:call('tarn_autocommit', {{{'insert', sid, {1, 5, 'z@x', 1}}}})
check('duplicate PK reported as 1062', r[1] == 'duplicate' and r[2] == 1062, r)
r = c[1]:call('tarn_autocommit', {{{'update', sid, {1}, {{'+', 2, 1}}}}})
check('update ops applied', r[1] == 'ok' and bal(1, 1) == 101, r)
c[1]:call('tarn_autocommit', {{{'update', sid, {1}, {{'=', 2, 100}}}}})

print('single-shard commit (TarnCommit!CommitLocal)')
local st = c[1]:new_stream(); st:begin()
local cur = st.space['test.acct']:get{1}
r = st:call('tarn_commit_local', {'L1', {{'replace', sid, {1, cur[2] - 10, 'a@x', 1}}}})
check('commit_local ok', r[1] == 'ok' and bal(1, 1) == 90, r)

-- MVCC validation: a read invalidated by a concurrent commit makes commit_local fail.
st = c[1]:new_stream(); st:begin()
cur = st.space['test.acct']:get{1}
c[1]:call('tarn_autocommit', {{{'update', sid, {1}, {{'+', 2, 1}}}}})
r = st:call('tarn_commit_local', {'L2', {{'replace', sid, {1, cur[2] + 1000, 'a@x', 1}}}})
check('stale read -> conflict 1213', r[1] == 'conflict' and r[2] == 1213 and bal(1, 1) == 91, r)

print('cross-shard commit (Prepare/ApplyAt)')
local function prepare(i, txid, writes, rkeys, rranges, _, stream)
    local s = stream or c[i]:new_stream()
    if not stream then s:begin() end
    return s:call('tarn_prepare', {txid, 1, {1, 2}, writes, rkeys or {}, rranges or {}, fiber.time()})
end
-- transfer 40 from acct 1 (shard 1) to acct 2 (shard 2)
local s1 = c[1]:new_stream(); s1:begin(); local a = s1.space['test.acct']:get{1}
local s2 = c[2]:new_stream(); s2:begin(); local b = s2.space['test.acct']:get{2}
local v1 = prepare(1, 'X1', {{'replace', sid, {1, a[2] - 40, 'a@x', 1}}}, {{sid, 0, {1}}}, nil, nil, s1)
local v2 = prepare(2, 'X1', {{'replace', sid, {2, b[2] + 40, 'b@x', 2}}}, {{sid, 0, {2}}}, nil, nil, s2)
check('both participants vote yes', v1[1] == 'yes' and v2[1] == 'yes', {v1, v2})
check('decision record is staging on D', c[1]:call('tarn_txn_status', {'X1'}) == 'staging')
check('intents invisible to direct reads', bal(1, 1) == 91 and bal(2, 2) == 100)
-- A single-shard writer and an autocommit writer must not overwrite a prepared row.
st = c[1]:new_stream(); st:begin()
r = st:call('tarn_commit_local', {'L3', {{'replace', sid, {1, 0, 'a@x', 1}}}})
check('commit_local blocked by prepared W lock', r[1] == 'conflict', r)
local t0 = fiber.clock()
r = c[2]:call('tarn_autocommit', {{{'update', sid, {2}, {{'+', 2, 1}}}}})
check('autocommit waits then conflicts on W lock', r[1] == 'conflict' and fiber.clock() - t0 >= 0.9, r)
-- Apply on both shards.
check('apply shard 1', c[1]:call('tarn_apply', {'X1'})[1] == 'ok')
check('apply shard 2', c[2]:call('tarn_apply', {'X1'})[1] == 'ok')
check('balances moved atomically', bal(1, 1) == 51 and bal(2, 2) == 140, {bal(1, 1), bal(2, 2)})
check('locks released', c[1]:call('tarn_info').locks == 0 and c[2]:call('tarn_info').locks == 0)
check('apply is idempotent', c[1]:call('tarn_apply', {'X1'})[1] == 'ok' and bal(1, 1) == 51)

print('prepare conflicts')
v1 = prepare(1, 'X2', {{'replace', sid, {1, 1, 'a@x', 1}}})
check('X2 prepared on shard 1', v1[1] == 'yes', v1)
v2 = prepare(1, 'X3', {{'replace', sid, {1, 2, 'a@x', 1}}})
check('W-W conflict votes no (X3)', v2[1] == 'no' and v2[2] == 1213, v2)
check('NO leaves a tombstone', c[1]:call('tarn_part_status', {'X3'}) == 'aborted')
v2 = prepare(1, 'X3', {{'replace', sid, {1, 3, 'a@x', 1}}})
check('re-delivered PREPARE after tombstone votes no', v2[1] == 'no', v2)
-- Read validation: a reader of acct 1 is refused while X2 holds its W lock.
s1 = c[1]:new_stream(); s1:begin(); s1.space['test.acct']:get{1}
v1 = prepare(1, 'X4', {}, {{sid, 0, {1}}}, nil, nil, s1)
check('read of W-locked key votes no', v1[1] == 'no', v1)
-- R lock blocks a later writer: X5 reads acct 2 and prepares; a writer of acct 2 is refused.
s2 = c[2]:new_stream(); s2:begin(); s2.space['test.acct']:get{2}
v2 = prepare(2, 'X5', {}, {{sid, 0, {2}}}, nil, nil, s2)
check('reader X5 prepared', v2[1] == 'yes', v2)
r = c[2]:call('tarn_autocommit', {{{'update', sid, {2}, {{'+', 2, 1}}}}})
check('writer blocked by R lock', r[1] == 'conflict', r)
c[2]:call('tarn_abort', {'X5', 1, {1, 2}})
r = c[2]:call('tarn_autocommit', {{{'update', sid, {2}, {{'+', 2, 1}}}}})
check('writer proceeds after R lock released', r[1] == 'ok', r)
c[1]:call('tarn_abort', {'X2', 1, {1, 2}})
check('abort restores nothing (intents dropped)', bal(1, 1) == 51)

print('range locks and unique keys')
s2 = c[2]:new_stream(); s2:begin(); s2.space['test.acct']:select({10}, {iterator = 'GE', limit = 100})
v2 = prepare(2, 'X6', {}, {}, {{sid, 0, {10}, {20}, true, true}}, nil, s2)
check('range reader prepared', v2[1] == 'yes', v2)
r = c[2]:call('tarn_autocommit', {{{'insert', sid, {15, 1, 'r@x', 2}}}})
check('insert into locked range refused', r[1] == 'conflict', r)
r = c[2]:call('tarn_autocommit', {{{'insert', sid, {25, 1, 'r@x', 2}}}})
check('insert outside range proceeds', r[1] == 'ok', r)
c[2]:call('tarn_abort', {'X6', 1, {1, 2}})
v2 = prepare(2, 'X7', {{'insert', sid, {26, 1, 'u@x', 2}}})
check('X7 claims unique email u@x', v2[1] == 'yes', v2)
r = c[2]:call('tarn_autocommit', {{{'insert', sid, {27, 1, 'u@x', 2}}}})
check('second claimant of u@x refused', r[1] == 'conflict', r)
v1 = prepare(2, 'X8', {{'insert', sid, {28, 1, 'r@x', 2}}})
check('prepare of existing unique value votes 1062', v1[1] == 'no' and v1[2] == 1062, v1)
c[2]:call('tarn_abort', {'X7', 1, {1, 2}})

print('lock-aware reads (strict serializability fix)')
-- A committed-but-unapplied cross-shard writer must not be read around.
v1 = prepare(1, 'W1', {{'replace', sid, {1, 7, 'a@x', 1}}})
check('W1 prepared', v1[1] == 'yes', v1)
local got
local f = fiber.create(function() got = c[1]:call('tarn_read', {sid, 0, {1}, 'EQ', 1}) end)
fiber.sleep(0.05)
check('tarn_read blocks on W lock', got == nil)
c[1]:call('tarn_apply', {'W1'})
fiber.sleep(0.05)
check('tarn_read returns the applied value', got and got[1] == 'ok' and got[2][1][2] == 7, got)
-- range read covering a W-locked key waits too
v1 = prepare(1, 'W2', {{'replace', sid, {3, 33, 'c@x', 1}}})
got = nil
f = fiber.create(function() got = c[1]:call('tarn_read', {sid, 0, {0}, 'GE', 10}) end)
fiber.sleep(0.05)
check('range tarn_read blocks on W lock inside range', got == nil)
c[1]:call('tarn_apply', {'W2'})
fiber.sleep(0.05)
check('range tarn_read sees the applied row', got and got[1] == 'ok' and #got[2] >= 2, got)
-- commit_local validates its read set against W locks
v1 = prepare(1, 'W3', {{'replace', sid, {1, 8, 'a@x', 1}}})
st = c[1]:new_stream(); st:begin(); st.space['test.acct']:get{1}
r = st:call('tarn_commit_local', {'L4', {{'replace', sid, {50, 1, 'q@x', 1}}}, {{sid, 0, {1}}}})
check('commit_local with W-locked read key -> conflict', r[1] == 'conflict', r)
st = c[1]:new_stream(); st:begin(); st.space['test.acct']:get{1}
r = st:call('tarn_validate', {'L5', {{sid, 0, {1}}}})
check('read-only validate with W-locked read key -> conflict', r[1] == 'conflict', r)
c[1]:call('tarn_abort', {'W3', 1, {1, 2}})
st = c[1]:new_stream(); st:begin(); st.space['test.acct']:get{1}
r = st:call('tarn_validate', {'L6', {{sid, 0, {1}}}})
check('read-only validate passes once unlocked', r[1] == 'ok', r)
-- secondary-index reads see W locks on old and new index keys
v1 = prepare(1, 'W4', {{'replace', sid, {1, 9, 'new@x', 1}}})
got = c[1]:call('tarn_read', {sid, 1, {'a@x'}, 'EQ', 1})
local got2 = nil
check('read of old unique key waits/conflicts while locked', got[1] == 'conflict', got)
c[1]:call('tarn_apply', {'W4'})
got = c[1]:call('tarn_read', {sid, 1, {'new@x'}, 'EQ', 1})
check('read of new unique key after apply', got[1] == 'ok' and #got[2] == 1, got)

print('recovery (coordinator crashed)')
-- (a) both prepared, coordinator vanished: implicitly committed -> applied.
s1 = c[1]:new_stream(); s1:begin(); a = s1.space['test.acct']:get{1}
s2 = c[2]:new_stream(); s2:begin(); b = s2.space['test.acct']:get{2}
prepare(1, 'R1', {{'replace', sid, {1, a[2] - 1, 'a@x', 1}}}, {{sid, 0, {1}}}, nil, nil, s1)
prepare(2, 'R1', {{'replace', sid, {2, b[2] + 1, 'b@x', 2}}}, {{sid, 0, {2}}}, nil, nil, s2)
-- (b) only the non-decision shard prepared: D fenced -> aborted.
v2 = prepare(2, 'R2', {{'replace', sid, {40, -999, 'r2@x', 2}}})
check('R2 prepared on shard 2 only', v2[1] == 'yes', v2)
-- (c) D prepared (staging) but shard 2 never got PREPARE: fenced -> aborted.
v1 = prepare(1, 'R3', {{'replace', sid, {41, -999, 'r3@x', 1}}})
check('R3 prepared on D only', v1[1] == 'yes', v1)
local before1, before2 = a[2], b[2]
local deadline = fiber.clock() + 15
while fiber.clock() < deadline do
    if c[1]:call('tarn_info').prepared == 0 and c[2]:call('tarn_info').prepared == 0
       and c[1]:call('tarn_txn_status', {'R3'}) == 'aborted' then break end
    fiber.sleep(0.2)
end
check('R1 committed by recovery', bal(1, 1) == before1 - 1 and bal(2, 2) == before2 + 1,
      {bal(1, 1), bal(2, 2), before1, before2})
check('R1 decision recorded', c[1]:call('tarn_txn_status', {'R1'}) == 'committed')
check('R2 fenced at D and aborted', c[1]:call('tarn_txn_status', {'R2'}) == 'aborted'
      and c[2]:call('tarn_part_status', {'R2'}) == 'aborted')
check('R3 participant fenced, txn aborted', c[2]:call('tarn_part_status', {'R3'}) == 'aborted'
      and c[1]:call('tarn_txn_status', {'R3'}) == 'aborted')
check('no aborted intents leaked', get(2, 40) == nil and get(1, 41) == nil)
check('all locks released', c[1]:call('tarn_info').locks == 0 and c[2]:call('tarn_info').locks == 0)
-- A late PREPARE for a fenced transaction must be refused.
v2 = prepare(2, 'R3', {{'replace', sid, {42, -999, 'r4@x', 2}}})
check('late PREPARE after fence votes no', v2[1] == 'no', v2)

local function wait_for(cond, secs)
    local deadline = fiber.clock() + (secs or 5)
    while fiber.clock() < deadline do
        if cond() then return true end
        fiber.sleep(0.1)
    end
    return cond()
end
local function raises_readonly(ok, err)
    return not ok and tostring(err):find('read%-only') ~= nil
end

print('APPLY outcomes')
check('APPLY on the decision shard marked X1 committed', c[1]:call('tarn_txn_status', {'X1'}) == 'committed')
r = c[2]:call('tarn_apply', {'NOPE'})
check('APPLY without a participant record reports none', r[1] == 'ok' and r[2] == 'none', r)
r = c[1]:call('tarn_apply', {'X3'})   -- X3 left a tombstone on shard 1
check('APPLY of an aborted participant is an error', r[1] == 'error', r)

print('expiry and failed PREPAREs')
r = c[2]:call('tarn_prepare', {'OLD1', 1, {1, 2}, {{'replace', sid, {70, 1, 'o@x', 2}}}, {}, {}, fiber.time() - 1000})
check('PREPARE of an expired transaction is refused as expired (neither yes nor no)', r[1] == 'expired', r)
check('... without a tombstone', c[2]:call('tarn_part_status', {'OLD1'}) == 'none')
-- An error after the locks entered the index (here: an invalid range) must
-- undo them (audit finding: they used to stay forever).
r = c[2]:call('tarn_prepare', {'BAD1', 1, {1, 2}, {{'replace', sid, {71, 1, 'u71@x', 2}}}, {},
                               {{'not-a-space-id', 0, box.NULL, box.NULL, true, true}}, fiber.time()})
check('PREPARE failing after taking its locks votes no', r[1] == 'no', r)
check('... and leaves no lock in the index', c[2]:call('tarn_info').lock_index == 0, c[2]:call('tarn_info'))
r = c[2]:call('tarn_autocommit', {{{'insert', sid, {71, 1, 'u71@x', 2}}}})
check('... so its row stays writable', r[1] == 'ok', r)

print('stale leaders')
local function stream_begin(i, key)
    local s = c[i]:new_stream()
    s:call('tarn_select', {sid, 0, key, 0, 1, box.NULL, true, false, box.NULL, true})
    return s
end
st = stream_begin(2, {2})
c[2]:eval('box.cfg{read_only = true}')
check('validate on a demoted node raises READONLY', raises_readonly(pcall(st.call, st, 'tarn_validate', {'V1', {{sid, 0, {2}}}, {}, true})))
check('autocommit on a demoted node raises READONLY',
      raises_readonly(pcall(c[2].call, c[2], 'tarn_autocommit', {{{'update', sid, {2}, {{'+', 2, 1}}}}})))
check('PREPARE on a demoted node raises READONLY',
      raises_readonly(pcall(c[2].call, c[2], 'tarn_prepare', {'RO1', 1, {1, 2}, {}, {}, {}, fiber.time()})))
check('lock-checked read on a demoted node raises READONLY',
      raises_readonly(pcall(c[2].call, c[2], 'tarn_read', {sid, 0, {2}, 'EQ', 1})))
c[2]:eval('box.cfg{read_only = false}')
st = stream_begin(2, {2})
c[2]:eval('box.ctl.promote()')   -- new election term
r = st:call('tarn_validate', {'V2', {{sid, 0, {2}}}, {}, true})
check('validate of a stream begun in an earlier term -> conflict', r[1] == 'conflict', r)
st = stream_begin(2, {2})
r = st:call('tarn_validate', {'V3', {{sid, 0, {2}}}, {}, true})
check('validate of a stream begun in the current term passes', r[1] == 'ok', r)

print('lock index rebuild with a PREPARE waiting for quorum')
c[2]:eval([[box.space._tarn_locks:alter{is_sync = true}; box.space._tarn_part:alter{is_sync = true}
            box.cfg{replication_synchro_quorum = 2, replication_synchro_timeout = 2}]])
local pend
fiber.create(function()
    pend = c[2]:call('tarn_prepare', {'Q1', 1, {1, 2}, {{'replace', sid, {2, 5, 'b@x', 2}}}, {}, {}, fiber.time()})
end)
fiber.sleep(0.3)
-- A read-only round trip makes the index stale; the next check rebuilds it
-- while Q1's lock records still wait for a quorum.
c[2]:eval('box.cfg{read_only = true}; require("fiber").sleep(0.05); box.cfg{read_only = false}')
fiber.sleep(0.05)
r = c[2]:call('tarn_autocommit', {{{'update', sid, {2}, {{'+', 2, 1}}}}})
check('rebuilt index keeps the lock of a PREPARE waiting for quorum', r[1] == 'conflict', r)
check('the waiting PREPARE fails once the quorum times out', wait_for(function() return pend ~= nil end, 10) and pend[1] == 'no', pend)
c[2]:eval([[box.cfg{replication_synchro_quorum = 1}
            box.space._tarn_locks:alter{is_sync = false}; box.space._tarn_part:alter{is_sync = false}]])
check('... and its lock is gone', wait_for(function() return c[2]:call('tarn_info').lock_index == 0 end), c[2]:call('tarn_info'))
r = c[2]:call('tarn_autocommit', {{{'update', sid, {2}, {{'+', 2, 1}}}}})
check('... so the row is writable again', r[1] == 'ok', r)

print('garbage collection')
for i = 1, 2 do c[i]:eval('tarn.cfg.max_txn_age = 0.5; tarn.cfg.clock_skew = 0.5; tarn.cfg.recovery_after = 1000') end
-- G1 is applied on shard 2 only; its record on D (shard 1) stays STAGING.
prepare(1, 'G1', {{'replace', sid, {80, 1, 'g80@x', 1}}})
prepare(2, 'G1', {{'replace', sid, {81, 1, 'g81@x', 2}}})
c[2]:call('tarn_apply', {'G1'})
fiber.sleep(2.5)
check('applied participant record kept while the decision record is STAGING',
      c[2]:call('tarn_part_status', {'G1'}) == 'applied')
c[1]:call('tarn_apply', {'G1'})
check('decision record COMMITTED once D applied', c[1]:call('tarn_txn_status', {'G1'}) == 'committed')
check('then every G1 record is collected', wait_for(function()
    return c[2]:call('tarn_part_status', {'G1'}) == 'none' and c[1]:call('tarn_part_status', {'G1'}) == 'none'
       and c[1]:call('tarn_txn_status', {'G1'}) == 'none' end))
-- G2's decision record is final (ABORTED) while shard 2 is still prepared.
prepare(2, 'G2', {{'replace', sid, {82, 1, 'g82@x', 2}}})
c[1]:call('tarn_txn_cas', {'G2', 'none', 'aborted', {1, 2}})
fiber.sleep(2.5)
check('decision record kept while a participant is prepared', c[1]:call('tarn_txn_status', {'G2'}) == 'aborted')
c[2]:call('tarn_abort', {'G2', 1, {1, 2}})
check('collected once nobody is prepared, tombstone too', wait_for(function()
    return c[1]:call('tarn_txn_status', {'G2'}) == 'none' and c[2]:call('tarn_part_status', {'G2'}) == 'none' end))
check('resolve of a vanished transaction writes nothing',
      c[1]:eval("return tarn.resolve('GONE1', 1, {1, 2})") == 'gone' and c[1]:call('tarn_txn_status', {'GONE1'}) == 'none')
for i = 1, 2 do c[i]:eval('tarn.cfg.max_txn_age = 120; tarn.cfg.clock_skew = 60; tarn.cfg.recovery_after = 2') end

print(string.format('\n%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
