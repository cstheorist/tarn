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
-- issue #39: a no-op UPDATE changes 0 rows, but matches 1 (CLIENT_FOUND_ROWS)
r = c[1]:call('tarn_autocommit', {{{'update', sid, {1}, {{'=', 2, 100}}}}})
check('no-op update counts 0 changed rows', r[1] == 'ok' and r[2] == 0, r)
r = c[1]:call('tarn_autocommit', {{{'update', sid, {1}, {{'=', 2, 100}}}}, {found_rows = true}})
check('no-op update counts 1 found row', r[1] == 'ok' and r[2] == 1, r)
r = c[1]:call('tarn_autocommit', {{{'update', sid, {99}, {{'=', 2, 100}}}}, {found_rows = true}})
check('update of a missing row counts 0 found rows', r[1] == 'ok' and r[2] == 0, r)

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

print('multi-key reads (issue #10)')
for id = 200, 205 do
    c[1]:call('tarn_autocommit', {{{'replace', sid, {id, id, 'k' .. id .. '@x', 7 + id % 2}}}})
end
got = c[1]:call('tarn_select_keys', {sid, 0, {{201}, {203}, {999}}, nil, true, false})
check('unique keys: one row per existing key, in key order',
      got[1] == 'ok' and #got[2] == 2 and got[2][1][1] == 201 and got[2][2][1] == 203, got)
got = c[1]:call('tarn_select_keys', {sid, 2, {{7}, {8}}, nil, true, false})
local n78 = 0
for _, t in ipairs(got[2] or {}) do if t[4] == 7 or t[4] == 8 then n78 = n78 + 1 end end
check('non-unique keys: every row of each key', got[1] == 'ok' and n78 == #got[2] and n78 >= 6, got)
got = c[1]:call('tarn_select_keys', {sid, 2, {{7}, {8}}, 1, true, false})
check('... at most limit rows per key', got[1] == 'ok' and #got[2] == 2, got)
v1 = prepare(1, 'W5', {{'replace', sid, {203, 1, 'k203@x', 8}}})
got = nil
f = fiber.create(function() got = c[1]:call('tarn_select_keys', {sid, 0, {{201}, {203}}, nil, true, false}) end)
fiber.sleep(0.05)
check('autocommit multi-key read waits for a W-locked key', got == nil)
c[1]:call('tarn_apply', {'W5'})
fiber.sleep(0.05)
check('... and returns the applied row', got and got[1] == 'ok' and #got[2] == 2 and got[2][2][2] == 1, got)
st = c[1]:new_stream()
got = st:call('tarn_select_keys', {sid, 0, {{200}, {202}}, nil, false, true})
check('a stream request begins the transaction',
      got[1] == 'ok' and #got[2] == 2 and st:eval('return box.is_in_txn()') == true, got)
st:rollback()

print('counts in storage (issue #52)')
for id = 300, 309 do
    local email = (id % 3 ~= 0) and ('n' .. id .. '@x') or box.NULL
    c[1]:call('tarn_autocommit', {{{'replace', sid, {id, id % 4, email, 9}}}})
end
-- tarn_count(space, index, key, iterator, end_key, end_incl, preds, exclude, lock_check, begin)
local function count(...) local r = c[1]:call('tarn_count', {...}); return r[1] == 'ok' and r[2] or r end
local by_bucket = #c[1].space['test.acct'].index[2]:select({9})
check('an equality prefix is counted by the index', count(sid, 2, {9}, 'EQ', box.NULL, true, {}, {}, true, false) == by_bucket)
check('a whole index', count(sid, 0, {}, 'ALL', box.NULL, true, {}, {}, true, false) == c[1]:eval('return box.space[...]:count()', {sid}))
check('a bounded range', count(sid, 0, {302}, 'GE', {306}, false, {}, {}, true, false) == 4)
check('residual comparisons', count(sid, 0, {300}, 'GE', {309}, true, {{2, 'ge', 2, box.NULL, true}}, {}, true, false) == 4)
check('NULL and NOT NULL', count(sid, 0, {300}, 'GE', {309}, true, {{3, 'null', box.NULL, box.NULL, false}}, {}, true, false) == 4
      and count(sid, 0, {300}, 'GE', {309}, true, {{3, 'notnull', box.NULL, box.NULL, false}}, {}, true, false) == 6)
check('a comparison with a NULL field never holds',
      count(sid, 0, {300}, 'GE', {309}, true, {{3, 'ne', 'zzz', box.NULL, false}}, {}, true, false) == 6)
check('IN lists', count(sid, 0, {300}, 'GE', {309}, true, {{2, 'in', {0, 3}, box.NULL, true}}, {}, true, false) == 5)
check('a field past the end of the tuple takes its fill value',
      count(sid, 0, {300}, 'GE', {309}, true, {{9, 'eq', 5, 5, true}}, {}, true, false) == 10)
check('excluded rows are left out where the scan covers them',
      count(sid, 2, {9}, 'EQ', box.NULL, true, {}, {{301}, {302}, {1}}, true, false) == by_bucket - 2
      and count(sid, 0, {300}, 'GE', {309}, true, {{2, 'eq', 1, box.NULL, true}}, {{301}, {302}}, true, false) == 2)
v1 = prepare(1, 'W6', {{'replace', sid, {304, 1, 'n304@x', 9}}})
got = nil
f = fiber.create(function() got = c[1]:call('tarn_count', {sid, 2, {9}, 'EQ', box.NULL, true, {}, {}, true, false}) end)
fiber.sleep(0.05)
check('an autocommit count waits for a W lock in its range', got == nil)
c[1]:call('tarn_apply', {'W6'})
fiber.sleep(0.05)
check('... and then counts', got and got[1] == 'ok' and got[2] == by_bucket, got)
st = c[1]:new_stream()
got = st:call('tarn_count', {sid, 0, {300}, 'GE', {309}, true, {}, {}, false, true})
check('a stream count begins the transaction',
      got[1] == 'ok' and got[2] == 10 and st:eval('return box.is_in_txn()') == true, got)
c[1]:call('tarn_autocommit', {{{'delete', sid, {300}}}})
got = st:call('tarn_count', {sid, 0, {300}, 'GE', {309}, true, {}, {}, false, false})
check('... and counts in its snapshot', got[1] == 'ok' and got[2] == 10, got)
st:rollback()

print('row counts for the access path (issue #81)')
got = c[1]:call('tarn_estimate', {sid, 2, {9}, 0, {}, 51})
check('memtx counts both prefixes in its indexes',
      got[1] == 'ok' and got[2] == #c[1].space['test.acct'].index[2]:select({9})
      and got[3] == c[1]:eval('return box.space[...]:count()', {sid}), got)
got = c[1]:call('tarn_estimate', {sid, 2, {12345}, 0, {301}, 51})
check('an empty prefix counts 0', got[1] == 'ok' and got[2] == 0 and got[3] == 1, got)

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

print('arithmetic updates keep MySQL integer semantics (issue #11)')
c[1]:call('tarn_autocommit', {{{'replace', sid, {90, 0, 'a90@x', 1}}}})
-- BIGINT bounds arrive as int64 / uint64 cdata, as the SQL layer encodes them.
local BIG = {'bal', -9223372036854775807LL - 1, 9223372036854775807ULL}
r = c[1]:call('tarn_autocommit', {{{'update', sid, {90}, {{'+', 'bal', -1}}, {BIG}}}})
check('signed BIGINT may go negative', r[1] == 'ok' and bal(1, 90) == -1, r)
r = c[1]:call('tarn_autocommit', {{{'update', sid, {90}, {{'+', 'bal', -9223372036854775807LL}}, {BIG}}}})
check('... down to the minimum', r[1] == 'ok' and bal(1, 90) == -9223372036854775807LL - 1, r)
r = c[1]:call('tarn_autocommit', {{{'update', sid, {90}, {{'+', 'bal', -1}}, {BIG}}}})
check('BIGINT overflow is 1690', r[1] == 'error' and r[2] == 1690 and bal(1, 90) == -9223372036854775807LL - 1, r)
c[1]:call('tarn_autocommit', {{{'replace', sid, {90, 0, 'a90@x', 1}}}})
r = c[1]:call('tarn_autocommit', {{{'update', sid, {90}, {{'+', 'bal', -1}}, {{'bal', 0, 18446744073709551615ULL}}}}})
check('an unsigned column cannot go below zero: 1690', r[1] == 'error' and r[2] == 1690 and bal(1, 90) == 0, r)
r = c[1]:call('tarn_autocommit', {{{'update', sid, {90}, {{'+', 'bal', 128}}, {{'bal', -128, 127}}}}})
check('TINYINT range is 1264', r[1] == 'error' and r[2] == 1264 and bal(1, 90) == 0, r)
r = c[1]:call('tarn_autocommit', {{{'update', sid, {90}, {{'+', 'bal', 127}}, {{'bal', -128, 127}}}}})
check('... and its maximum fits', r[1] == 'ok' and bal(1, 90) == 127, r)
check('int_lt compares signed and unsigned exactly', c[1]:eval([[
    local lt = tarn.int_lt
    return lt(-1, 9223372036854775807ULL) and not lt(9223372036854775807ULL, -1)
       and lt(-9223372036854775807LL, 0) and lt(5, 18446744073709551615ULL)
       and not lt(18446744073709551615ULL, 5) and lt(-2LL, -1) and not lt(3, 3)]]))

print('row locks (READ COMMITTED, #17)')
for id = 300, 305 do
    c[1]:call('tarn_autocommit', {{{'replace', sid, {id, id, 'r' .. id .. '@x', 1}}}})
end
local function lock(i, txid, keys, mode, opts) return c[i]:call('tarn_lock', {txid, keys, mode, opts}) end
local function k(id) return {sid, 0, {id}} end
r = lock(1, 'P1', {k(300)}, 'X', {read = true})
check('X lock granted with the latest row', r[1] == 'ok' and r[3][1][2] == 300, r)
local gen = r[2]
r = lock(1, 'P2', {k(300)}, 'X', {timeout = 0.2})
check('a conflicting X request times out with 1205', r[1] == 'timeout' and r[2] == 1205, r)
r = lock(1, 'P2', {k(300)}, 'X', {nowait = true})
check('NOWAIT fails at once with 3572', r[1] == 'locked' and r[2] == 3572, r)
r = lock(1, 'P2', {k(300), k(301)}, 'X', {skip = true, read = true})
check('SKIP LOCKED skips the held row and takes the free one',
      r[1] == 'ok' and r[3][1] == false and r[3][2][1] == 301, r)
c[1]:call('tarn_unlock', {'P2'})
r = lock(1, 'P3', {k(302)}, 'S', {}); local r2 = lock(1, 'P4', {k(302)}, 'S', {})
check('S locks are shared', r[1] == 'ok' and r2[1] == 'ok', {r, r2})
r = lock(1, 'P3', {k(302)}, 'X', {timeout = 0.2})
check('S -> X upgrade waits for the other S holder', r[1] == 'timeout', r)
c[1]:call('tarn_unlock', {'P4'})
r = lock(1, 'P3', {k(302)}, 'X', {timeout = 0.2})
check('... and is granted once it is the only holder', r[1] == 'ok', r)
c[1]:call('tarn_unlock', {'P3'})
-- a waiter is granted when the holder releases
got = nil
f = fiber.create(function() got = lock(1, 'P5', {k(300)}, 'X', {read = true, timeout = 5}) end)
fiber.sleep(0.05)
check('a waiter queues behind the holder', got == nil)
r = c[1]:call('tarn_commit_rc', {'P1', {{'replace', sid, {300, 999, 'r300@x', 1}}}, gen})
fiber.sleep(0.05)
check('RC commit applies the write and releases the lock',
      r[1] == 'ok' and bal(1, 300) == 999 and got and got[1] == 'ok' and got[3][1][2] == 999, {r, got})
c[1]:call('tarn_unlock', {'P5'})
-- local deadlock: P6 holds 303 and waits for 304; P7 holds 304 and asks for 303
lock(1, 'P6', {k(303)}, 'X', {}); lock(1, 'P7', {k(304)}, 'X', {})
got = nil
f = fiber.create(function() got = lock(1, 'P6', {k(304)}, 'X', {timeout = 5}) end)
fiber.sleep(0.05)
r = lock(1, 'P7', {k(303)}, 'X', {timeout = 5})
check('the request that closes a cycle fails with 1213 at once', r[1] == 'deadlock' and r[2] == 1213, r)
c[1]:call('tarn_unlock', {'P7'})
fiber.sleep(0.05)
check('... and the other transaction proceeds', got and got[1] == 'ok', got)
c[1]:call('tarn_unlock', {'P6'})
-- the cross-shard detector's interface
lock(1, 'P8', {k(305)}, 'X', {})
got = nil
f = fiber.create(function() got = lock(1, 'P9', {k(305)}, 'X', {timeout = 5, held = {2}}) end)
fiber.sleep(0.05)
local waits = c[1]:call('tarn_lock_waits')
local w9
for _, w in ipairs(waits) do if w[1] == 'P9' then w9 = w end end
check('tarn_lock_waits reports the wait-for edge', w9 ~= nil and w9[3][1] == 'P8', waits)
check('... and the other shards where the waiter holds row locks',
      w9 ~= nil and #w9[4] == 1 and w9[4][1] == 2, waits)
check('tarn_lock_victim fails a pending wait with 1213',
      w9 ~= nil and c[1]:call('tarn_lock_victim', {'P9', w9[2]}) == true)
fiber.sleep(0.05)
check('... seen by the waiter', got and got[1] == 'deadlock', got)
c[1]:call('tarn_unlock', {'P8'}); c[1]:call('tarn_unlock', {'P9'})
-- a request that fails after some of its locks were granted keeps them and
-- names their generation, so that the SQL server releases them later
r = lock(1, 'P20', {k(305)}, 'X', {})
local p20gen = r[2]
r = lock(1, 'P21', {k(304), k(305)}, 'X', {timeout = 0.2})
check('a request that times out after taking locks reports their generation',
      r[1] == 'timeout' and r[2] == 1205 and r[4] == p20gen, r)
r = lock(1, 'P22', {k(304)}, 'X', {nowait = true})
check('... and keeps them', r[1] == 'locked', r)
r = lock(1, 'P22', {k(305)}, 'X', {timeout = 0.2})
check('a request that took no lock reports no generation', r[1] == 'timeout' and r[4] == nil, r)
for _, t in ipairs({'P20', 'P21', 'P22'}) do c[1]:call('tarn_unlock', {t}) end
-- other writers respect row locks
r = lock(1, 'P10', {k(301)}, 'X', {})
local p10gen = r[2]
r = c[1]:call('tarn_autocommit', {{{'replace', sid, {301, 1, 'r301@x', 1}}}, {lock_timeout = 0.2}})
check('an autocommit write waits for a row lock, then fails with 1205', r[1] == 'timeout' and r[2] == 1205, r)
got = nil
f = fiber.create(function()
    got = c[1]:call('tarn_autocommit', {{{'replace', sid, {301, 2, 'r301@x', 1}}}, {lock_timeout = 5}})
end)
fiber.sleep(0.05)
check('... it keeps waiting while the lock is held', got == nil)
c[1]:call('tarn_unlock', {'P10'})
fiber.sleep(0.05)
check('... and writes once it is released', got and got[1] == 'ok' and bal(1, 301) == 2, got)
lock(1, 'P11', {k(301)}, 'X', {})
st = c[1]:new_stream(); st:begin(); st.space['test.acct']:get{301}
r = st:call('tarn_commit_local', {'L20', {{'replace', sid, {301, 3, 'r301@x', 1}}}, {{sid, 0, {301}}}})
check('a serializable commit over a row lock conflicts', r[1] == 'conflict', r)
-- a stale generation (leader change) or a lost connection breaks a transaction
r = c[1]:call('tarn_commit_rc', {'P11', {{'replace', sid, {301, 4, 'r301@x', 1}}}, 'not-the-generation'})
check('RC commit with a stale lock generation is refused', r[1] == 'error' and r[2] == 1213 and bal(1, 301) == 2, r)
r = lock(1, 'P12', {k(301)}, 'X', {timeout = 0.2})
check('... and releases the transaction\'s locks', r[1] == 'ok', r)
c[1]:call('tarn_unlock', {'P12'})
local c3 = net.connect(S1, opts)
r = c3:call('tarn_lock', {'P13', {k(302)}, 'X', {}})
local p13gen = r[2]
c3:close()
fiber.sleep(0.1)
r = lock(1, 'P14', {k(302)}, 'X', {timeout = 0.5})
check('a closed connection releases its locks', r[1] == 'ok', r)
c[1]:call('tarn_unlock', {'P14'})
r = c[1]:call('tarn_commit_rc', {'P13', {{'replace', sid, {302, 5, 'r302@x', 1}}}, p13gen})
check('... and its transaction can no longer commit', r[1] == 'error' and r[2] == 1213 and bal(1, 302) == 302, r)
r = lock(1, 'P13', {k(302)}, 'X', {})
check('... nor take new locks', r[1] == 'error' and r[2] == 1213, r)
c[1]:call('tarn_unlock', {'P13'})   -- (rollback) forgets that P13 was broken
r = c[1]:call('tarn_commit_rc', {'P13', {{'replace', sid, {302, 5, 'r302@x', 1}}}, p13gen})
check('... even once that is forgotten: the generation names the closed session',
      r[1] == 'error' and r[2] == 1213 and bal(1, 302) == 302, r)
v1 = prepare(1, 'P15', {{'replace', sid, {303, 6, 'r303@x', 1}}}, nil, nil, nil, nil)
check('PREPARE without row locks is unaffected', v1[1] == 'yes', v1)
c[1]:call('tarn_apply', {'P15'})
r = lock(1, 'P16', {k(304)}, 'X', {})
local s16 = c[1]:new_stream(); s16:begin()
v1 = s16:call('tarn_prepare', {'P16', 1, {1}, {{'replace', sid, {304, 7, 'r304@x', 1}}}, {}, {}, fiber.time(), 'stale'})
check('PREPARE of an RC transaction whose locks were lost votes NO', v1[1] == 'no' and v1[2] == 1213, v1)
c[1]:call('tarn_abort', {'P16', 1, {1}})
check('... and its ABORT releases the row locks', lock(1, 'P19', {k(304)}, 'X', {timeout = 0.2})[1] == 'ok')
c[1]:call('tarn_unlock', {'P19'})
-- a lock request waits out a prepared transaction's W lock, then reads its value
v1 = prepare(1, 'W20', {{'replace', sid, {305, 77, 'r305@x', 1}}})
got = nil
f = fiber.create(function() got = lock(1, 'P17', {k(305)}, 'X', {read = true, timeout = 5}) end)
fiber.sleep(0.05)
check('a lock request waits while a prepared transaction W-locks the row', got == nil)
c[1]:call('tarn_apply', {'W20'})
fiber.sleep(0.05)
check('... and reads the applied value', got and got[1] == 'ok' and got[3][1][2] == 77, got)
c[1]:call('tarn_unlock', {'P17'})
r = lock(1, 'P18', {{sid, 1, {'r305@x'}}}, 'X', {read = true})
check('a unique-key value lock reads the row holding that value', r[1] == 'ok' and r[3][1][1] == 305, r)
c[1]:call('tarn_unlock', {'P18'})
check('every row lock is released', c[1]:call('tarn_info').row_locks == 0, c[1]:call('tarn_info').row_locks)

print('locking reads of a primary-key range in one call (issue #49)')
local qdef = {
    name = 'test.q', engine = 'memtx',
    format = {{name = 'g', type = 'unsigned'}, {name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
              {name = 'bucket_id', type = 'unsigned'}},
    indexes = {{name = 'primary', unique = true, parts = {{field = 'g', type = 'unsigned'}, {field = 'id', type = 'unsigned'}}},
               {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}}},
}
for i = 1, 2 do c[i]:call('tarn_create_table', {qdef}) end
c[1]:reload_schema()
local qsid = c[1].space['test.q'].id
for id = 1, 40 do c[1]:call('tarn_autocommit', {{{'replace', qsid, {1, id, id, 1}}}}) end
c[1]:call('tarn_autocommit', {{{'replace', qsid, {2, 1, 0, 1}}}})
local function lrange(txid, ranges, mode, o) return c[1]:call('tarn_lock_range', {txid, ranges, mode, o}) end
local function ids(rows) local out = {} for _, t in ipairs(rows or {}) do out[#out + 1] = t[2] end return table.concat(out, ',') end
r = lrange('Q1', {{qsid, {1}, 3, {}}}, 'X', {})
check('the first n rows of the range, locked and read', r[1] == 'ok' and ids(r[3][1]) == '1,2,3' and r[3][1][1][3] == 1, r)
r = lrange('Q2', {{qsid, {1}, 3, {}}, {qsid, {1}, 1, {}}}, 'X', {skip = true})
check('SKIP LOCKED: another transaction gets the next free rows; a second range does not skip its locks',
      r[1] == 'ok' and ids(r[3][1]) == '4,5,6' and ids(r[3][2]) == '4', r)
r = lrange('Q2', {{qsid, {1}, 4, {}}}, 'X', {skip = true})
check('... its own locks are not skipped', r[1] == 'ok' and ids(r[3][1]) == '4,5,6,7', r)
r = lrange('Q3', {{qsid, {1}, 2, {{1, 8}}}}, 'X', {skip = true})
check('... the keys it is told to leave out are left out', r[1] == 'ok' and ids(r[3][1]) == '9,10', r)
r = lrange('Q4', {{qsid, {1}, 1, {}}}, 'X', {nowait = true})
check('NOWAIT on a held row: 3572, with no lock taken', r[1] == 'locked' and r[2] == 3572 and r[4] == nil, r)
r = lrange('Q4', {{qsid, {1}, 2, {}}}, 'S', {timeout = 0.2})
check('a waiting request times out with 1205', r[1] == 'timeout' and r[2] == 1205, r)
r = lrange('Q5', {{qsid, {1}, 100, {}}}, 'X', {skip = true})
check('a range shorter than n ends with what it has (all free rows, across pages)',
      r[1] == 'ok' and #r[3][1] == 31 and r[3][1][1][2] == 8 and r[3][1][31][2] == 40, #r[3][1])
r = lrange('Q6', {{qsid, {2}, 5, {}}, {qsid, {3}, 5, {}}}, 'X', {})
check('... and stays in its prefix', r[1] == 'ok' and #r[3][1] == 1 and r[3][1][1][1] == 2 and #r[3][2] == 0, r)
for _, t in ipairs({'Q2', 'Q3', 'Q4', 'Q5', 'Q6'}) do c[1]:call('tarn_unlock', {t}) end
-- a waiter whose row is deleted by the holder moves on to the next row
local q1gen = lrange('Q1', {{qsid, {1}, 1, {}}}, 'X', {})[2]
got = nil
f = fiber.create(function() got = lrange('Q7', {{qsid, {1}, 1, {}}}, 'X', {timeout = 5}) end)
fiber.sleep(0.05)
check('a request queues behind the holder of the first row', got == nil)
r = c[1]:call('tarn_commit_rc', {'Q1', {{'delete', qsid, {1, 1}}, {'delete', qsid, {1, 2}}}, q1gen})
fiber.sleep(0.05)
check('... and, once the holder deleted the rows it waited for, takes the next one',
      r[1] == 'ok' and got and got[1] == 'ok' and ids(got[3][1]) == '3', {r, got})
r = lrange('Q8', {{qsid, {1}, 1, {}}}, 'X', {nowait = true})
check('... having released the lock of the deleted row', r[1] == 'locked', r)
c[1]:call('tarn_unlock', {'Q7'}); c[1]:call('tarn_unlock', {'Q8'})
-- a prepared transaction's W lock: waited out, or skipped under SKIP LOCKED
v1 = prepare(1, 'W21', {{'replace', qsid, {1, 3, 33, 1}}})
r = lrange('Q9', {{qsid, {1}, 1, {}}}, 'X', {skip = true})
check('SKIP LOCKED skips a row a prepared transaction W-locks', v1[1] == 'yes' and r[1] == 'ok' and ids(r[3][1]) == '4', {v1, r})
c[1]:call('tarn_unlock', {'Q9'})
got = nil
f = fiber.create(function() got = lrange('Q10', {{qsid, {1}, 1, {}}}, 'X', {timeout = 5}) end)
fiber.sleep(0.05)
check('a waiting request waits for the prepared transaction', got == nil)
c[1]:call('tarn_apply', {'W21'})
fiber.sleep(0.05)
check('... and reads the applied value', got and got[1] == 'ok' and ids(got[3][1]) == '3' and got[3][1][1][3] == 33, got)
c[1]:call('tarn_unlock', {'Q10'})
r = lrange('Q11', {{qsid + 1000, {1}, 1, {}}}, 'X', {})
check('an unknown space is 1146', r[1] == 'error' and r[2] == 1146, r)
c[1]:call('tarn_unlock', {'Q11'})
check('every row lock is released', c[1]:call('tarn_info').row_locks == 0, c[1]:call('tarn_info').row_locks)


print('schema changes (ALTER TABLE)')
local afmt = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
              {name = 's', type = 'string', is_nullable = true}, {name = 'bucket_id', type = 'unsigned'}}
local aidx = {{name = 'primary', unique = true, parts = {{field = 'id', type = 'unsigned'}}},
              {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}}}
for i = 1, 2 do c[i]:call('tarn_create_table', {{name = 'test.alt', engine = 'memtx', format = afmt, indexes = aidx}}) end
c[1]:reload_schema()
local asid = c[1].space['test.alt'].id
c[1]:call('tarn_autocommit', {{{'insert', asid, {1, 10, 'abc', 1}}, {'insert', asid, {2, 20, 'abcdef', 1}}}})
local function aw(ver, op) return c[1]:call('tarn_autocommit', {{op}, {fence = {asid, ver}}}) end
local function aget(id) return c[1]:eval('return box.space["test.alt"]:get{...}', {id}) end
local function aindex(id) return c[1]:eval('local i = box.space["test.alt"].index[...] return i and i.name', {id}) end
local function aargs(ver, extra)
    local a = {ver = ver, timeout = 1, checks = {}, builds = {}, drop = {}, renames = {},
               def = {name = 'test.alt', id = asid, format = afmt, indexes = aidx, json = '{}', ver = ver}}
    for key, val in pairs(extra or {}) do a[key] = val end
    return a
end
local iv = {{7, 'iv', true, {{field = 2, type = 'integer'}}}}
r = c[1]:call('tarn_alter_prepare', {'test.alt', aargs(5, {checks = {{3, 's', 'chars', 4, box.NULL, box.NULL}}})})
check('PREPARE refuses rows that do not fit the new type (1265)', r[1] == 'error' and r[2] == 1265, r)
r = aw(0, {'update', asid, {1}, {{'+', 2, 1}}})
check('... and lifts its fence again', r[1] == 'ok', r)
r = c[1]:call('tarn_alter_prepare', {'test.alt', aargs(5, {builds = iv})})
check('PREPARE builds a new index under a temporary name', r[1] == 'ok' and aindex(7) == '_tarn_tmp_7', {r, aindex(7)})
r = aw(0, {'update', asid, {1}, {{'+', 2, 1}}})
check('a write made with the replaced definition is refused (1412)', r[1] == 'error' and r[2] == 1412, r)
r = aw(5, {'update', asid, {1}, {{'+', 2, 1}}})
check('... one made with the new definition is not', r[1] == 'ok', r)
local sa = c[1]:new_stream(); sa:begin()
r = sa:call('tarn_commit_local', {'AL1', {{'replace', asid, {1, 0, 'abc', 1}}}, {}, {}, box.NULL, {asid, 0}})
check('CommitLocal refuses it too', r[1] == 'error' and r[2] == 1412 and aget(1)[2] == 12, r)
sa = c[1]:new_stream(); sa:begin()
v1 = sa:call('tarn_prepare', {'AP1', 1, {1, 2}, {{'replace', asid, {1, 0, 'abc', 1}}}, {}, {}, fiber.time(), box.NULL, {asid, 0}})
check('... and PREPARE votes NO', v1[1] == 'no' and v1[2] == 1412, v1)
r = c[1]:call('tarn_alter_abort', {'test.alt', aargs(5, {builds = iv})})
check('ABORT drops the temporary index and lifts the fence',
      r[1] == 'ok' and aindex(7) == nil and aw(0, {'update', asid, {1}, {{'+', 2, 1}}})[1] == 'ok', r)
v1 = prepare(1, 'AX1', {{'replace', asid, {2, 21, 'abcdef', 1}}})
r = c[1]:call('tarn_alter_prepare', {'test.alt', aargs(6)})
check('PREPARE waits for prepared transactions writing the table, up to its timeout (1205)',
      r[1] == 'timeout' and r[2] == 1205, r)
got = nil
f = fiber.create(function() got = c[1]:call('tarn_alter_prepare', {'test.alt', aargs(6, {timeout = 5})}) end)
fiber.sleep(0.1)
check('... it waits', got == nil)
c[1]:call('tarn_apply', {'AX1'})
fiber.sleep(0.1)
check('... until the transaction is applied', got and got[1] == 'ok', got)
local afmt2 = table.deepcopy(afmt)
afmt2[5] = {name = 'w', type = 'integer', is_nullable = true}
r = c[1]:call('tarn_alter_apply', {'test.alt', aargs(6, {format = afmt2,
    def = {name = 'test.alt', id = asid, format = afmt2, indexes = aidx, json = '{}', ver = 6}})})
local aver = c[1]:eval('return box.space._tarn_catalog:get{"test.alt"}.def.ver')
check('APPLY changes the format and stores definition version 6', r[1] == 'ok' and aver == 6, {r, aver})
r = c[1]:call('tarn_alter_apply', {'test.alt', aargs(6)})
check('... a repeated APPLY does nothing', r[1] == 'ok' and r[2] == 'applied', r)
r = aw(5, {'update', asid, {1}, {{'+', 2, 1}}})
check('the fence outlives the ALTER (stored with the definition)', r[1] == 'error' and r[2] == 1412, r)
r = aw(6, {'update', asid, {1}, {{'+', 5, 1}}, {}, {4, 41}})
check('an update of a row older than an added field starts from its fill value',
      r[1] == 'ok' and aget(1)[5] == 42 and aget(1)[2] == 13, {r, aget(1)})
aw(6, {'insert', asid, {3, 21, 'q', 1, box.NULL}})
local ux = {{8, 'uv', true, {{field = 2, type = 'integer'}}}}
r = c[1]:call('tarn_alter_prepare', {'test.alt', aargs(7, {builds = ux})})
check('a unique index the rows violate: 1062, and no index is left', r[2] == 1062 and aindex(8) == nil, {r, aindex(8)})
r = c[1]:call('tarn_alter_prepare', {'test.alt', aargs(7, {checks = {{5, 'w', 'notnull', box.NULL, box.NULL, box.NULL}}})})
check('a NOT NULL check counts a short row by its fill value (NULL here)', r[1] == 'error' and r[2] == 1265, r)
r = c[1]:call('tarn_alter_prepare', {'test.alt', aargs(7, {checks = {{5, 'w', 'range', 0, 100, 0}}})})
check('... and a range check by its fill value (0 here)', r[1] == 'ok', r)
c[1]:call('tarn_alter_abort', {'test.alt', aargs(7)})
r = c[1]:call('tarn_ddl_begin', {{'o1', 'test.alt', 7, aargs(7)}})
local r2 = c[1]:call('tarn_ddl_begin', {{'o2', 'test.alt', 8, aargs(8)}})
check('one schema change of a table at a time (job record)', r[1] == 'ok' and r2[1] == 'busy' and r2[2] == 'o1', {r, r2})
r = c[1]:call('tarn_ddl_job')
check('... the pending job can be read back', r[1] == 'ok' and r[2] == 'o1' and r[4] == 'test.alt' and r[6].ver == 7, r)
r = c[1]:call('tarn_ddl_touch', {'o2', 'o1'})
c[1]:call('tarn_ddl_end', {'o1'})
check('... taken over by another server', r[1] == 'ok' and c[1]:call('tarn_ddl_job')[2] == 'o2', r)
c[1]:call('tarn_ddl_end', {'o2'})
check('... and ended by its owner', #c[1]:call('tarn_ddl_job') == 1)
-- Two jobs beginning at once: the second must see the first even while its
-- insert waits for the WAL (a read-confirmed read would not).
local f1 = c[1]:call('tarn_ddl_begin', {{'o3', 'test.alt', 9, aargs(9)}}, {is_async = true})
local f2 = c[1]:call('tarn_ddl_begin', {{'o4', 'test.alt', 10, aargs(10)}}, {is_async = true})
local function status(f)
    -- wait_result returns the call's results packed in a table, or nil and
    -- the error
    local ok, r, e = pcall(f.wait_result, f, 5)
    if not ok then return tostring(r) end
    if r == nil then return tostring(e) end
    return type(r[1]) == 'table' and r[1][1] or tostring(r[1])
end
local st = {status(f1), status(f2)}
table.sort(st)
check('two schema changes beginning at once: one runs, the other is told busy', st[1] == 'busy' and st[2] == 'ok', st)
c[1]:call('tarn_ddl_end', {'o3'})
c[1]:call('tarn_ddl_end', {'o4'})

print('index builds ahead of the fence (BUILD, vinyl tables)')
r = c[1]:call('tarn_alter_build', {'test.alt', aargs(7, {builds = {{9, 'mv', false, {{field = 2, type = 'integer'}}}}})})
check('BUILD leaves a memtx table to PREPARE (MVCC aborts a build that writes overtake)',
      r[1] == 'ok' and aindex(9) == nil, {r, aindex(9)})
for i = 1, 2 do
    c[i]:call('tarn_create_table', {{name = 'test.bld', engine = 'vinyl', format = afmt, indexes = aidx}})
end
c[1]:reload_schema()
local bsid = c[1].space['test.bld'].id
c[1]:call('tarn_autocommit', {{{'insert', bsid, {1, 10, 'a', 1}}, {'insert', bsid, {2, 20, 'b', 1}}}})
local function bargs(ver, extra)
    local a = {ver = ver, timeout = 1, checks = {}, builds = {}, drop = {}, renames = {},
               def = {name = 'test.bld', id = bsid, format = afmt, indexes = aidx, json = '{}', ver = ver}}
    for key, val in pairs(extra or {}) do a[key] = val end
    return a
end
local function bw(op) return c[1]:call('tarn_autocommit', {{op}, {fence = {bsid, 0}}}) end
local function bindex(id) return c[1]:eval('local i = box.space["test.bld"].index[...] return i and i.name', {id}) end
local bu = {{5, 'bv', true, {{field = 2, type = 'integer'}}}}
r = c[1]:call('tarn_alter_build', {'test.bld', bargs(1, {builds = bu})})
check('BUILD makes the new index under a temporary name', r[1] == 'ok' and bindex(5) == '_tarn_tmp_5', {r, bindex(5)})
r = bw({'insert', bsid, {3, 30, 'c', 1}})
check('... without fencing writes made with the current definition', r[1] == 'ok', r)
check('... and keeps the index current', c[1]:eval('return box.space["test.bld"].index[5]:get{30} ~= nil'))
r = bw({'insert', bsid, {4, 30, 'd', 1}})
check('a write the new unique index refuses fails with 1062, naming the index as it will be named',
      r[1] == 'duplicate' and r[2] == 1062 and tostring(r[3]):find('"bv"') ~= nil, r)
c[1]:call('tarn_alter_abort', {'test.bld', bargs(1, {builds = bu})})
check('ABORT drops what BUILD made', bindex(5) == nil)
-- A transaction prepared before the index existed writes a value it will
-- refuse; decided, it must still land: the index goes instead.
r = prepare(1, 'BX1', {{'replace', bsid, {5, 30, 'e', 1}}})
check('a transaction prepared before BUILD, writing a value the new index will refuse', r[1] == 'yes', r)
r = c[1]:call('tarn_alter_build', {'test.bld', bargs(1, {builds = bu})})
check('... BUILD does not see that value yet', r[1] == 'ok' and bindex(5) == '_tarn_tmp_5', {r, bindex(5)})
r = c[1]:call('tarn_apply', {'BX1'})
check('... its APPLY lands, dropping the index under construction',
      r[1] == 'ok' and bindex(5) == nil and c[1]:eval('return box.space["test.bld"]:get{5} ~= nil'), {r, bindex(5)})
r = c[1]:call('tarn_alter_prepare', {'test.bld', bargs(1, {builds = bu})})
check('... and PREPARE, building it again behind the fence, fails the schema change (1062)',
      r[1] == 'duplicate' and r[2] == 1062 and bindex(5) == nil, {r, bindex(5)})
c[1]:call('tarn_alter_abort', {'test.bld', bargs(1, {builds = bu})})
c[1]:eval('tarn.cfg.alter_check_rows = 2')
r = c[1]:call('tarn_alter_prepare', {'test.bld', bargs(1, {checks = {{2, 'v', 'range', 0, 100, 0}}})})
check('checks of more existing rows than tarn.cfg.alter_check_rows are refused (1846)',
      r[1] == 'error' and r[2] == 1846, r)
check('... before anything is fenced', bw({'update', bsid, {1}, {{'+', 2, 1}}})[1] == 'ok')
c[1]:eval('tarn.cfg.alter_check_rows = 1000000')
c[1]:call('tarn_autocommit', {{{'delete', bsid, {5}}}})
local bidx = table.deepcopy(aidx)
bidx[3] = {name = 'bv', unique = true, parts = {{field = 'v', type = 'integer'}}}
local bdone = bargs(1, {builds = bu, def = {name = 'test.bld', id = bsid, format = afmt, indexes = bidx,
                                             json = '{}', ver = 1}})
r = {c[1]:call('tarn_alter_build', {'test.bld', bdone}), c[1]:call('tarn_alter_prepare', {'test.bld', bdone}),
     c[1]:call('tarn_alter_apply', {'test.bld', bdone})}
check('BUILD, PREPARE and APPLY: the index under its own name',
      r[1][1] == 'ok' and r[2][1] == 'ok' and r[3][1] == 'ok' and bindex(5) == 'bv', {r, bindex(5)})

print('indexes over instant-ADD fields (issue #70)')
do
    local fmt = table.deepcopy(afmt)
    fmt[5] = {name = 'added', type = 'integer', is_nullable = true}
    for _, engine in ipairs({'memtx', 'vinyl'}) do
        local name = 'test.fill_' .. engine
        for i = 1, 2 do
            c[i]:call('tarn_create_table', {{name = name, engine = engine, format = fmt, indexes = aidx}})
        end
        c[1]:reload_schema()
        local fsid = c[1].space[name].id
        local builds = {{5, 'added_idx', false, {{field = 5, type = 'integer', is_nullable = true}}},
                        {6, 'existing_idx', false, {{field = 2, type = 'integer'}}}}
        local indexes = table.deepcopy(aidx)
        indexes[3] = {name = 'added_idx', unique = false,
                      parts = {{field = 5, type = 'integer', is_nullable = true}}}
        indexes[4] = {name = 'existing_idx', unique = false, parts = {{field = 2, type = 'integer'}}}
        local a = {ver = 1, timeout = 1, checks = {{5, 'added', 'indexed', box.NULL, box.NULL, 3}},
                   builds = builds, drop = {}, renames = {},
                   def = {name = name, id = fsid, format = fmt, indexes = indexes, json = '{}', ver = 1}}
        local function fw(op) return c[1]:call('tarn_autocommit', {{op}, {fence = {fsid, 0}}}) end
        local function fi(id)
            return c[1]:eval('local n, id = ... local i = box.space[n].index[id] return i and i.name', {name, id})
        end
        local function label(s) return engine .. ': ' .. s end
        fw({'insert', fsid, {1, 10, 'short', 1}})
        fw({'insert', fsid, {2, 20, 'null', 1, box.NULL}})
        r = c[1]:call('tarn_alter_build', {name, a})
        check(label('BUILD defers every index when any field needs a presence check'),
              r[1] == 'ok' and fi(5) == nil and fi(6) == nil, r)
        for _ = 1, 2 do
            r = c[1]:call('tarn_alter_prepare', {name, a})
            check(label('short non-NULL fill fails PREPARE with 1846, repeatably'),
                  r[1] == 'error' and r[2] == 1846 and fi(5) == nil and fi(6) == nil, r)
        end
        check(label('failed check releases the fence'), fw({'update', fsid, {1}, {{'+', 2, 1}}})[1] == 'ok')
        local limit = c[1]:eval('return tarn.cfg.alter_check_rows')
        c[1]:eval('tarn.cfg.alter_check_rows = 1')
        r = c[1]:call('tarn_alter_prepare', {name, a})
        c[1]:eval('tarn.cfg.alter_check_rows = ...', {limit})
        check(label('presence checks retain the row-scan bound'), r[1] == 'error' and r[2] == 1846
              and tostring(r[3]):find('limit 1', 1, true) ~= nil, r)
        check(label('bounded refusal does not leave a fence'), fw({'update', fsid, {1}, {{'+', 2, 1}}})[1] == 'ok')
        local txid = 'FILL_' .. engine
        r = prepare(1, txid, {{'replace', fsid, {1, 12, 'full', 1, 3}}})
        check(label('old-layout writer can prepare before the DDL fence'), r[1] == 'yes', r)
        local short = table.deepcopy(a)
        short.timeout = 0.01
        r = c[1]:call('tarn_alter_prepare', {name, short})
        check(label('PREPARE drains writers before checking short tuples'), r[1] == 'timeout' and r[2] == 1205, r)
        local got
        fiber.create(function() got = c[1]:call('tarn_alter_prepare', {name, a}) end)
        fiber.sleep(0.05)
        check(label('presence check waits for the prepared writer'), got == nil, got)
        c[1]:call('tarn_apply', {txid})
        local deadline = fiber.time() + 5
        while got == nil and fiber.time() < deadline do fiber.sleep(0.01) end
        check(label('materialized value and explicit NULL both pass after drain'),
              got and got[1] == 'ok' and fi(5) == '_tarn_tmp_5' and fi(6) == '_tarn_tmp_6', got)
        r = c[1]:call('tarn_alter_prepare', {name, a})
        check(label('successful PREPARE is repeatable'), r[1] == 'ok', r)
        c[1]:call('tarn_alter_abort', {name, a})
        check(label('ABORT removes both temporary indexes'), fi(5) == nil and fi(6) == nil)
        fw({'insert', fsid, {3, 30, 'late short', 1}})
        -- No fence now: APPLY must redo PREPARE, as after a restart. It
        -- cannot publish an index if an old-layout writer left a short row.
        r = c[1]:call('tarn_alter_apply', {name, a})
        local ver = c[1]:eval('local r = box.space._tarn_catalog:get{...} return r.def.ver or 0', {name})
        check(label('APPLY without its fence rechecks and fails closed'),
              r[1] == 'error' and r[2] == 1846 and ver == 0 and fi(5) == nil and fi(6) == nil, {r, ver})
        check(label('failed APPLY releases the reconstructed fence'), fw({'delete', fsid, {3}})[1] == 'ok')
        c[1]:call('tarn_alter_prepare', {name, a})
        r = c[1]:call('tarn_alter_apply', {name, a})
        check(label('fully present fields can be indexed and published'),
              r[1] == 'ok' and fi(5) == 'added_idx' and fi(6) == 'existing_idx', r)
        r = c[1]:call('tarn_alter_apply', {name, a})
        check(label('published APPLY remains idempotent'), r[1] == 'ok' and r[2] == 'applied', r)
        local null_checks = table.deepcopy(a)
        null_checks.ver, null_checks.checks = 2, box.NULL
        null_checks.builds = {{7, 'null_checks', false, {{field = 2, type = 'integer'}}}}
        r = c[1]:call('tarn_alter_build', {name, null_checks})
        check(label('BUILD accepts explicit MessagePack NULL checks'),
              r[1] == 'ok' and (engine ~= 'vinyl' or fi(7) == '_tarn_tmp_7'), r)
        c[1]:call('tarn_alter_abort', {name, null_checks})
    end
end

print('DROP TABLE under a prepared transaction')
-- A table dropped between a transaction's PREPARE and its APPLY: the APPLY
-- still lands the transaction's other writes and releases its locks (it
-- used to fail on the missing space at every attempt, holding them).
local gdef = table.deepcopy(def)
gdef.name = 'test.gone'
c[1]:call('tarn_create_table', {gdef})
c[1]:reload_schema()
local gsid = c[1].space['test.gone'].id
r = prepare(1, 'XG', {{'replace', gsid, {1, 1, box.NULL, 1}}, {'replace', sid, {9001, 7, box.NULL, 1}}})
check('a transaction prepared with writes to two tables', r[1] == 'yes', r)
check('one of them dropped', c[1]:call('tarn_drop_table', {'test.gone'})[1] == 'ok')
r = c[1]:call('tarn_apply', {'XG'})
check('... APPLY lands the write to the other one', r[1] == 'ok' and bal(1, 9001) == 7, {r, bal(1, 9001)})
check('... and releases the locks', c[1]:eval("return box.space._tarn_locks.index.txid:count{'XG'}") == 0)

print('vinyl reader threads')
-- lua/test/run.sh sets TARN_VINYL_READ_THREADS=3 on shard 1, and on shard 2
-- only TARN_HOST_INSTANCES=2: the CPUs halved, 1 to 8.
local ffi = require('ffi')
pcall(ffi.cdef, 'long sysconf(int name);')
local cpus = tonumber(ffi.C.sysconf(jit.os == 'OSX' and 58 or 84))
local want = math.max(1, math.min(8, math.floor(cpus / 2)))
r = c[1]:eval('return box.cfg.vinyl_read_threads')
check('TARN_VINYL_READ_THREADS sets vinyl_read_threads', r == 3, r)
r = c[2]:eval('return box.cfg.vinyl_read_threads')
check('by default, the CPUs divided among the instances', r == want, {r, want, cpus})

print('vinyl tables: commit steps that yield')
-- A vinyl read can yield; tarn_test_hook holds a step at such a point
-- (lua/tarn.lua "Yields") while another step runs. The storage instances run
-- with TARN_CHECK_YIELDS: an atomic section that yields fails its step.
local vfmt = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
              {name = 'u', type = 'string', is_nullable = true}, {name = 'bucket_id', type = 'unsigned'}}
local vidx = {{name = 'primary', unique = true, parts = {{field = 'id', type = 'unsigned'}}},
              {name = 'u', unique = true, parts = {{field = 'u', type = 'string', is_nullable = true}}},
              {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}}}
for i = 1, 2 do
    c[i]:call('tarn_create_table', {{name = 'test.vy', engine = 'vinyl', format = vfmt, indexes = vidx}})
end
c[1]:reload_schema()
local vsid = c[1].space['test.vy'].id
check('a vinyl space', c[1]:eval('return box.space["test.vy"].engine') == 'vinyl')
r = c[1]:call('tarn_autocommit', {{{'insert', vsid, {1, 10, box.NULL, 1}}, {'insert', vsid, {2, 20, box.NULL, 1}},
                                   {'insert', vsid, {3, 30, box.NULL, 1}}}})
check('autocommit into a vinyl table', r[1] == 'ok' and r[2] == 3, r)
-- On disk, with no cache: reads now yield for real.
c[1]:eval('box.snapshot() box.cfg{vinyl_cache = 0}')
local function vget(id) local t = c[1]:eval('return box.space["test.vy"]:get{...}', {id}) return t and t[2] end
local function hook(name, action) return c[1]:call('tarn_test_hook', {name, action}) end
local function async(fn, args) return c[1]:call(fn, args, {is_async = true}) end
local function result(f)
    local ok, res, err = pcall(f.wait_result, f, 10)
    if not ok then return {'raised', tostring(res)} end
    if res == nil then return {'raised', tostring(err)} end
    return res[1]
end
local function vprepare(txid, writes, stream)
    return {'tarn_prepare', {txid, 1, {1}, writes, {}, {}, fiber.time()}}
end

-- Two PREPAREs of one row: the first, held after its read, finds the
-- second's W lock in its atomic section and votes NO.
hook('prepare', 'hold')
local fa = async(unpack(vprepare('VA', {{'replace', vsid, {1, 11, box.NULL, 1}}})))
hook('prepare', 'reached')
r = c[1]:call(unpack(vprepare('VB', {{'replace', vsid, {1, 12, box.NULL, 1}}})))
hook('prepare', 'release')
local ra = result(fa)
check('two PREPAREs of one vinyl row: the second votes yes ...', r[1] == 'yes', r)
check('... and the first, held after its read, votes no', ra[1] == 'no' and ra[2] == 1213, ra)
c[1]:call('tarn_apply', {'VB'})
check('... and the row holds the second one', vget(1) == 12, vget(1))

-- A local commit held after its read while a PREPARE locks the row: the
-- commit finds the W lock in its atomic section and fails.
hook('write', 'hold')
local fl = async('tarn_commit_local', {'VL1', {{'replace', vsid, {2, 21, box.NULL, 1}}}})
hook('write', 'reached')
r = c[1]:call(unpack(vprepare('VC', {{'replace', vsid, {2, 22, box.NULL, 1}}})))
hook('write', 'release')
local rl = result(fl)
check('a local commit that meets a W lock taken while it read fails', r[1] == 'yes' and rl[1] == 'conflict', {r, rl})
c[1]:call('tarn_apply', {'VC'})
check('... and the prepared write is applied', vget(2) == 22, vget(2))

-- An autocommit write held after its read while a row lock is requested on
-- the row: the lock grant waits for the commit in flight, which then finds
-- the row lock and waits for it in turn.
hook('write', 'hold')
local fw = async('tarn_autocommit', {{{'update', vsid, {3}, {{'+', 2, 1}}}}, {lock_timeout = 5}})
hook('write', 'reached')
local fk = async('tarn_lock', {'VK1', {{vsid, 0, {3}}}, 'X', {read = true, timeout = 5}})
fiber.sleep(0.2)
check('a row lock waits for an autocommit write in flight', not fk:is_ready())
hook('write', 'release')
local rk = result(fk)
check('... then reads the row as it was', rk[1] == 'ok' and rk[3][1][2] == 30, rk)
fiber.sleep(0.1)
check('... while the write waits for the row lock', not fw:is_ready())
c[1]:call('tarn_unlock', {'VK1'})
local rw = result(fw)
check('... and is applied once it is released', rw[1] == 'ok' and vget(3) == 31, {rw, vget(3)})

-- Once a commit in flight has done its writes, only lock grants on the keys
-- they touch wait for it: the primary key, or a unique value the old or the
-- new row holds.
r = c[1]:call('tarn_autocommit', {{{'update', vsid, {3}, {{'=', 3, 'u3'}}}}})
hook('written', 'hold')
fw = async('tarn_autocommit', {{{'update', vsid, {3}, {{'=', 3, 'u4'}, {'+', 2, 1}}}}, {lock_timeout = 5}})
hook('written', 'reached')
t0 = fiber.clock()
r = c[1]:call('tarn_lock', {'VK2', {{vsid, 0, {2}}, {vsid, 1, {'u5'}}}, 'X', {read = true, timeout = 5}})
check('a row lock on another row does not wait for a commit in flight', r[1] == 'ok' and r[3][1][2] == 22
      and r[3][2] == nil and fiber.clock() - t0 < 1, r)
local fks = {}
for i, k in ipairs({{0, {3}}, {1, {'u3'}}, {1, {'u4'}}}) do
    fks[i] = async('tarn_lock', {'VK' .. (2 + i), {{vsid, k[1], k[2]}}, 'X', {read = true, timeout = 5}})
end
fiber.sleep(0.2)
check('... but one on its row, its old or its new unique value does',
      not fks[1]:is_ready() and not fks[2]:is_ready() and not fks[3]:is_ready())
hook('written', 'release')
local rks = {result(fks[1]), result(fks[2]), result(fks[3])}
check('... then read the row as it was', rks[1][1] == 'ok' and rks[1][3][1][2] == 31
      and rks[2][3][1][2] == 31 and rks[3][3][1] == nil, rks)
for i = 2, 5 do c[1]:call('tarn_unlock', {'VK' .. i}) end
rw = result(fw)
check('... and the write is applied once they are released', rw[1] == 'ok' and vget(3) == 32, {rw, vget(3)})

-- A schema change while a local commit made with the old definition is held
-- after its read: PREPARE of the change waits for it, and the commit fails
-- its fence check in the atomic section (1412).
local function vargs(ver)
    return {ver = ver, timeout = 5, checks = {}, builds = {}, drop = {}, renames = {},
            def = {name = 'test.vy', id = vsid, format = vfmt, indexes = vidx, json = '{}', ver = ver}}
end
hook('write', 'hold')
local fc = async('tarn_commit_local', {'VL2', {{'replace', vsid, {1, 13, box.NULL, 1}}}, {}, {}, box.NULL, {vsid, 0}})
hook('write', 'reached')
local fp = async('tarn_alter_prepare', {'test.vy', vargs(3)})
fiber.sleep(0.2)
check('PREPARE of a schema change waits for a commit in flight', not fp:is_ready())
hook('write', 'release')
local rc = result(fc)
local rp = result(fp)
check('... which fails its fence check (1412)', rc[1] == 'error' and rc[2] == 1412, rc)
check('... and the change is prepared', rp[1] == 'ok', rp)
c[1]:call('tarn_alter_abort', {'test.vy', vargs(3)})
check('... and the row is unchanged', vget(1) == 12, vget(1))

-- Autocommit reads of vinyl tables (checked_read): a cross-shard
-- transaction that writes the row while the read is held after its scan --
-- prepared and applied in the meantime, or only prepared -- makes the read
-- run again (or wait), so it returns the applied row.
local function vread(id) return async('tarn_select', {vsid, 0, {id}, 'EQ', 1, box.NULL, box.NULL, true}) end
hook('scan', 'hold')
local fr = vread(2)
hook('scan', 'reached')
c[1]:call(unpack(vprepare('VD', {{'replace', vsid, {2, 23, box.NULL, 1}}})))
c[1]:call('tarn_apply', {'VD'})
hook('scan', 'release')
local rr = result(fr)
check('a read whose row was written by a cross-shard commit while it ran reads again',
      rr[1] == 'ok' and rr[2][1][2] == 23, rr)
hook('scan', 'hold')
fr = vread(2)
hook('scan', 'reached')
c[1]:call(unpack(vprepare('VE', {{'replace', vsid, {2, 24, box.NULL, 1}}})))
hook('scan', 'release')
fiber.sleep(0.1)
check('... and one that meets a W lock taken meanwhile waits for it', not fr:is_ready())
c[1]:call('tarn_apply', {'VE'})
rr = result(fr)
check('... then reads the applied row', rr[1] == 'ok' and rr[2][1][2] == 24, rr)
local function vcount(preds)
    return async('tarn_count', {vsid, 0, {}, 'ALL', box.NULL, true, preds or {}, {}, true, false})
end
local n0 = c[1]:call('tarn_count', {vsid, 0, {}, 'ALL', box.NULL, true, {}, {}, true, false})[2]
hook('scan', 'hold')
fr = vcount()
hook('scan', 'reached')
c[1]:call(unpack(vprepare('VF', {{'replace', vsid, {9, 90, box.NULL, 1}}})))
c[1]:call('tarn_apply', {'VF'})
hook('scan', 'release')
rr = result(fr)
check('a vinyl count whose range a cross-shard commit wrote while it ran counts again',
      rr[1] == 'ok' and rr[2] == n0 + 1, {rr, n0})
rr = c[1]:call('tarn_count', {vsid, 0, {}, 'ALL', box.NULL, true, {{2, 'ge', 20, box.NULL, true}}, {}, true, false})
local want = c[1]:eval('local n = 0 for _, t in box.space["test.vy"]:pairs() do if t[2] >= 20 then n = n + 1 end end return n')
check('a vinyl count with a residual', rr[1] == 'ok' and rr[2] == want, {rr, want})
rr = c[1]:call('tarn_estimate', {vsid, 2, {1}, 0, {}, 1})
local vn = c[1]:eval('return box.space["test.vy"].index[0]:len()')
local vb = c[1]:eval('return box.space["test.vy"].index[2]:count({1})')
check('vinyl counts a prefix up to where paging the other is sure to read fewer rows',
      rr[1] == 'ok' and rr[2] == math.min(vb, math.floor(math.sqrt(vn)) + 1) and rr[3] == vn, {rr, vn, vb})
rr = c[1]:call('tarn_estimate', {vsid, 2, {1}, 0, {}, 2^62})
check('... and not past one page: the plan stays', rr[1] == 'ok' and rr[2] == 0 and rr[3] == vn, rr)
-- issue #93: a sample of B's first page gives the cost of paging B. Space
-- est: ids 1..30, g = 1 for every third id, 3 for every tenth, else 2.
c[1]:eval([[
    local s = box.schema.space.create('test.est', {engine = 'vinyl'})
    s:create_index('pk', {parts = {{1, 'unsigned'}}})
    s:create_index('g', {parts = {{2, 'unsigned'}}, unique = false})
    for id = 1, 30 do s:insert{id, id % 10 == 0 and 3 or (id % 3 == 0 and 1 or 2)} end
]])
local esid = c[1]:eval('return box.space["test.est"].id')
local function est(...) return c[1]:call('tarn_estimate', {esid, 1, ...}) end
-- g = 1 holds ids 3, 6, 9, 12, 15, 18, 21, 24, 27 (30 is g = 3): 9 rows
check('vinyl: budget rows of A in the first page cost that page; A is counted one past it',
      table.concat(est({1}, 0, {}, 2, 'EQ'), ',') == 'ok,9,0,8', est({1}, 0, {}, 2, 'EQ'))
check('... read backwards for a backwards page',
      table.concat(est({1}, 0, {}, 2, 'REQ'), ',') == 'ok,9,0,8', est({1}, 0, {}, 2, 'REQ'))
-- g = 3: ids 10, 20, 30; one in the first 12 rows: about 36 rows to page
check('... fewer rows of A extrapolate from their share of the page',
      table.concat(est({3}, 0, {}, 3, 'EQ'), ',') == 'ok,3,0,36', est({3}, 0, {}, 3, 'EQ'))
check('... a B shorter than the page costs its rows',
      table.concat(est({3}, 0, {}, 8, 'EQ'), ',') == 'ok,3,0,30', est({3}, 0, {}, 8, 'EQ'))
rr = est({3}, 0, {}, 2, 'EQ')
check('... and with no row of A in the page, falls back to the index size',
      rr[1] == 'ok' and rr[2] == 3 and rr[3] == 30 and rr[4] == nil, rr)
rr = est({1}, 0, {}, 2, box.NULL)
-- (counted up to sqrt(2 * 30) + 1 = 8, where paging is sure to win)
check('... as it does without a sample', rr[1] == 'ok' and rr[2] == 8 and rr[3] == 30 and rr[4] == nil, rr)
c[1]:eval('box.space["test.est"]:drop()')
local svc = c[1]:new_stream()
rr = svc:call('tarn_count', {vsid, 0, {}, 'ALL', box.NULL, true, {}, {{9}, {2}, {777}}, false, true})
check('a vinyl count in a stream leaves out the rows the caller wrote', rr[1] == 'ok' and rr[2] == n0 - 1, {rr, n0})
svc:rollback()
check('no atomic section yielded on vinyl', c[1]:eval('return true'))

print('jobs: one record per table name')
local function jb(owner, names)
    return {owner = owner, kind = 'drop', space = names[1], names = names, ver = 1, a = {x = 1}}
end
r = {c[1]:call('tarn_job_begin', {jb('j1', {'t.a'}), 0}), c[1]:call('tarn_job_begin', {jb('j2', {'t.b'}), 0})}
check('jobs on two tables run at once', r[1][1] == 'ok' and r[2][1] == 'ok', r)
r = c[1]:call('tarn_job_begin', {jb('j3', {'t.c', 't.a'}), 0})
check('a job on a table that another holds is told busy', r[1] == 'busy' and r[2] == 'j1', r)
check('... and holds none of its names', #c[1]:call('tarn_jobs') == 2)
r = c[1]:call('tarn_job_begin', {jb('j1', {'t.a'}), 0})
check('... unless it is the same job (a repeated call)', r[1] == 'ok', r)
local fj = async('tarn_job_begin', {jb('j3', {'t.c', 't.a'}), 5})
fiber.sleep(0.1)
check('with a wait, it waits', not fj:is_ready())
c[1]:call('tarn_job_end', {'j1'})
local rj = result(fj)
check('... and begins once the other ends', rj[1] == 'ok', rj)
local function job_of(owner)
    for _, j in ipairs(c[1]:call('tarn_jobs')) do if j[1] == owner then return j end end
end
r = c[1]:call('tarn_job_phase', {'j2', 'apply'})
check('a job records its phase', r[1] == 'ok' and job_of('j2')[5] == 'apply' and job_of('j2')[7].x == 1, {r, job_of('j2')})
r = c[1]:call('tarn_job_touch', {'j4', 'j2'})
local r2 = c[1]:call('tarn_job_touch', {'j5', 'j2'})
check('an abandoned job is taken over by one server', r[1] == 'ok' and r2[1] == 'gone' and job_of('j4') ~= nil, {r, r2})
r = c[1]:call('tarn_job_touch', {'j4', 'j2'})
check('... a repeated takeover succeeds', r[1] == 'ok', r)
r = c[1]:call('tarn_job_phase', {'j2', 'abort'})
check('... and the old owner cannot change it any more', r[1] == 'gone' and job_of('j4')[5] == 'apply', r)
c[1]:call('tarn_job_end', {'j3'})
c[1]:call('tarn_job_end', {'j4'})
check('jobs end', #c[1]:call('tarn_jobs') == 0)

print('CREATE TABLE, DROP TABLE and TRUNCATE in steps (issue #30)')
local cdef = table.deepcopy(def)
cdef.name, cdef.id = 'test.cr', 20001
local ca = {def = cdef}
local function cexists(i) return c[i]:eval('return box.space["test.cr"] ~= nil') end
local function cnamed(i) return c[i]:eval('return box.space._tarn_catalog:get{"test.cr"} ~= nil') end
r = c[1]:call('tarn_create_prepare', {'test.cr', ca})
check('CREATE PREPARE makes the space', r[1] == 'ok' and cexists(1), r)
check('... which no catalog names yet', not cnamed(1))
check('... a repeated PREPARE does nothing', c[1]:call('tarn_create_prepare', {'test.cr', ca})[1] == 'ok')
r = c[1]:call('tarn_create_abort', {'test.cr', ca})
check('CREATE ABORT drops it', r[1] == 'ok' and not cexists(1), r)
for i = 1, 2 do c[i]:call('tarn_create_prepare', {'test.cr', ca}) end
r = {c[2]:call('tarn_create_apply', {'test.cr', ca}), c[1]:call('tarn_create_apply', {'test.cr', ca})}
check('CREATE APPLY names it in the catalog', r[1][1] == 'ok' and r[2][1] == 'ok' and cnamed(1) and cnamed(2), r)
r = c[1]:call('tarn_create_abort', {'test.cr', ca})
check('... after which ABORT leaves it', r[1] == 'ok' and r[2] == 'applied' and cexists(1), r)
local other = table.deepcopy(ca)
other.def.id = 20002
r = c[1]:call('tarn_create_prepare', {'test.cr', other})
check('a CREATE of a name another table has: 1050', r[1] == 'error' and r[2] == 1050, r)

local crsid = 20001
c[1]:call('tarn_autocommit', {{{'insert', crsid, {1, 10, box.NULL, 1}}, {'insert', crsid, {2, 20, box.NULL, 1}}}})
local function targs(ver)
    local d = table.deepcopy(cdef)
    d.json, d.ver = '{}', ver
    return {ver = ver, timeout = 5, id = crsid, def = d}
end
local function cw(ver, op) return c[1]:call('tarn_autocommit', {{op}, {fence = {crsid, ver}}}) end
local function clen() return c[1]:eval('return box.space["test.cr"]:len()') end
r = prepare(1, 'TX1', {{'replace', crsid, {3, 30, box.NULL, 1}}})
check('a transaction prepared with a write to the table', r[1] == 'yes', r)
got = nil
f = fiber.create(function() got = c[1]:call('tarn_truncate_prepare', {'test.cr', targs(3)}) end)
fiber.sleep(0.1)
check('TRUNCATE PREPARE waits for it', got == nil)
local t0 = fiber.clock()
r = c[1]:call('tarn_select', {crsid, 0, {1}, 'EQ', 1, box.NULL, box.NULL, true})
check('... meanwhile autocommit reads of the table wait, then fail (1213)',
      r[1] == 'conflict' and fiber.clock() - t0 >= 0.9, r)
r = c[1]:call('tarn_select', {sid, 0, {9001}, 'EQ', 1, box.NULL, box.NULL, true})
check('... those of other tables do not', r[1] == 'ok', r)
r = cw(0, {'update', crsid, {1}, {{'+', 2, 1}}})
check('... and writes made with the current definition get 1412', r[1] == 'error' and r[2] == 1412, r)
local fk2 = async('tarn_lock', {'TK1', {{crsid, 0, {1}}}, 'X', {read = true, timeout = 5}})
fiber.sleep(0.1)
check('... and a row lock with a read waits', not fk2:is_ready())
c[1]:call('tarn_apply', {'TX1'})
fiber.sleep(0.1)
check('... until the transaction is applied: then it prepares', got and got[1] == 'ok', got)
t0 = fiber.clock()
r = c[1]:call('tarn_count', {crsid, 0, {}, 'ALL', box.NULL, true, {}, {}, true, false})
check('an autocommit count of the held table waits (no row lock left), then fails',
      r[1] == 'conflict' and fiber.clock() - t0 >= 0.9, r)
local sr = c[1]:new_stream()
sr:call('tarn_select', {crsid, 0, {1}, 'EQ', 1, box.NULL, box.NULL, false, box.NULL, true})
r = sr:call('tarn_validate', {'TV1', {{crsid, 0, {1}}}, {}, true})
check('a transaction that read the held table fails validation', r[1] == 'conflict', r)
c[1]:eval('tarn.stale_fences()')
r = cw(0, {'update', crsid, {1}, {{'+', 2, 1}}})
check('the fence and the hold outlive a rebuild from storage (a restart, a new leader)',
      r[1] == 'error' and r[2] == 1412 and c[1]:call('tarn_info').ddl_held == 1, {r, c[1]:call('tarn_info').ddl_held})
r = c[1]:call('tarn_truncate_apply', {'test.cr', targs(3)})
check("TRUNCATE APPLY empties the table, the applied transaction's row too", r[1] == 'ok' and clen() == 0, {r, clen()})
local rk2 = result(fk2)
check('... the waiting row lock reads the empty table', rk2[1] == 'ok' and rk2[3][1] == nil, rk2)
c[1]:call('tarn_unlock', {'TK1'})
check('... the definition is stored under the new version',
      c[1]:eval('return box.space._tarn_catalog:get{"test.cr"}.def.ver') == 3)
r = cw(0, {'insert', crsid, {1, 1, box.NULL, 1}})
check('... writes encoded before it stay refused (1412)', r[1] == 'error' and r[2] == 1412, r)
r = cw(3, {'insert', crsid, {1, 1, box.NULL, 1}})
check('... those encoded after it are not', r[1] == 'ok', r)
r = c[1]:call('tarn_truncate_apply', {'test.cr', targs(3)})
check('... and a repeated APPLY does nothing', r[1] == 'ok' and r[2] == 'applied' and clen() == 1, {r, clen()})
check('... and nothing is held any more', c[1]:call('tarn_info').ddl_held == 0)
r = c[1]:call('tarn_truncate_prepare', {'test.cr', targs(4)})
check('TRUNCATE ABORT after PREPARE', r[1] == 'ok' and c[1]:call('tarn_info').ddl_held == 1, r)
-- A node that becomes the leader has not seen the holds its predecessor
-- took: an autocommit read with the lock check reads them first. (Here the
-- node reads the holds without this one, which is then stored again.)
do
    local hk = 'hold:' .. crsid
    local hv = c[1]:eval('return box.space._tarn_meta:get{...}.value', {hk})
    c[1]:eval('box.space._tarn_meta:delete{...} tarn.stale_fences()', {hk})
    r = c[1]:call('tarn_select', {crsid, 0, {1}, 'EQ', 1, box.NULL, box.NULL, true})
    check('(a node that knows of no hold reads the table)', r[1] == 'ok' and c[1]:call('tarn_info').ddl_held == 0, r)
    c[1]:eval('box.space._tarn_meta:replace{...} tarn.stale_fences()', {hk, hv})
    r = c[1]:call('tarn_select', {crsid, 0, {1}, 'EQ', 1, box.NULL, box.NULL, true})
    check('... once it is to read the holds again, an autocommit read reads them, then waits (1213)',
          r[1] == 'conflict' and c[1]:call('tarn_info').ddl_held == 1, r)
end
r = c[1]:call('tarn_truncate_abort', {'test.cr', targs(4)})
check('... releases the table and lowers the fence',
      r[1] == 'ok' and c[1]:call('tarn_info').ddl_held == 0 and cw(3, {'update', crsid, {1}, {{'+', 2, 1}}})[1] == 'ok', r)
local da = {ver = 5, timeout = 5, id = crsid}
r = {c[1]:call('tarn_drop_prepare', {'test.cr', da}), c[1]:call('tarn_drop_apply', {'test.cr', da})}
check('DROP PREPARE and APPLY drop the space and its catalog entry',
      r[1][1] == 'ok' and r[2][1] == 'ok' and not cexists(1) and not cnamed(1), r)
r = c[1]:call('tarn_drop_apply', {'test.cr', da})
check('... a repeated APPLY does nothing', r[1] == 'ok' and r[2] == 'applied', r)
check('... and nothing is held', c[1]:call('tarn_info').ddl_held == 0)

-- vinyl MVCC aborts the writers of a space a DDL changes, not its readers:
-- the TRUNCATE dooms them (the stream registers with tarn_select's begin).
local vtdef = {name = 'test.vt', id = 20003, engine = 'vinyl', format = vfmt, indexes = vidx}
c[1]:call('tarn_create_table', {vtdef})
c[1]:call('tarn_autocommit', {{{'insert', 20003, {1, 10, box.NULL, 1}}}})
local sv = c[1]:new_stream()
r = sv:call('tarn_select', {20003, 0, {1}, 'EQ', 1, box.NULL, box.NULL, false, box.NULL, true})
check('a stream reads a vinyl table', r[1] == 'ok' and #r[2] == 1, r)
local vt = {ver = 2, timeout = 5, id = 20003, def = {name = 'test.vt', id = 20003, engine = 'vinyl', format = vfmt,
                                                     indexes = vidx, json = '{}', ver = 2}}
r = {c[1]:call('tarn_truncate_prepare', {'test.vt', vt}), c[1]:call('tarn_truncate_apply', {'test.vt', vt})}
check('the table is truncated', r[1][1] == 'ok' and r[2][1] == 'ok', r)
r = sv:call('tarn_validate', {'TV2', {{20003, 0, {1}}}, {}, true})
check('... and the stream that read it before fails validation', r[1] == 'conflict', r)
local sw = c[1]:new_stream()
sw:call('tarn_select', {20003, 0, {1}, 'EQ', 1, box.NULL, box.NULL, false, box.NULL, true})
r = sw:call('tarn_validate', {'TV3', {{20003, 0, {1}}}, {}, true})
check('... one that began after it does not', r[1] == 'ok', r)

print('global indexes of unique keys without the shard key (issue #14)')
local gdef = {
    name = 'test.gt', id = 20010, engine = 'memtx',
    format = {{name = 'id', type = 'unsigned'}, {name = 'h', type = 'string'}, {name = 'bucket_id', type = 'unsigned'}},
    indexes = {{name = 'primary', unique = true, parts = {{field = 'id', type = 'unsigned'}}},
               {name = 'uh', unique = true, parts = {{field = 'h', type = 'string'}}},
               {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}}},
    uix = {{name = '_tarn_uix_20010_1', id = 20011, engine = 'memtx',
            format = {{name = 'h', type = 'string'}, {name = 'id', type = 'unsigned'},
                      {name = 'bucket_id', type = 'unsigned'}},
            indexes = {{name = 'uh', unique = true, parts = {{field = 'h', type = 'string'}}},
                       {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}}}}},
    json = '{}',
}
local ga = {def = gdef}
for i = 1, 2 do c[i]:call('tarn_create_prepare', {'test.gt', ga}) end
for i = 2, 1, -1 do c[i]:call('tarn_create_apply', {'test.gt', ga}) end
local function uexists(i) return c[i]:eval('return box.space._tarn_uix_20010_1 ~= nil') end
check('CREATE makes the global index space on every shard', uexists(1) and uexists(2))
check('... with no catalog entry of its own',
      c[1]:eval('return box.space._tarn_catalog:get{"_tarn_uix_20010_1"} == nil'))
r = c[1]:call('tarn_autocommit', {{{'insert', 20010, {1, 'hat', 1}}, {'insert', 20011, {'hat', 1, 7}}}})
check('a row and its entry commit together', r[1] == 'ok', r)
-- (a row on the other shard: only the entry collides)
r = c[2]:call('tarn_autocommit', {{{'insert', 20011, {'hat', 2, 7}}}})
check('... the other shard has no entry', r[1] == 'ok', r)
c[2]:call('tarn_autocommit', {{{'delete', 20011, {'hat'}}}})
r = c[1]:call('tarn_autocommit', {{{'insert', 20011, {'hat', 2, 7}}}})
check("an entry whose key is taken: 1062, named by the unique key",
      r[1] == 'duplicate' and r[2] == 1062 and r[3] == "Duplicate entry 'hat' for key 'uh'", r)
r = prepare(1, 'GX1', {{'insert', 20011, {'hat', 3, 7}}})
check('... also when PREPARE finds it', r[1] == 'no' and r[2] == 1062 and r[3] == "Duplicate entry 'hat' for key 'uh'", r)
r = prepare(1, 'GX2', {{'insert', 20011, {'cap', 4, 7}}})
check('a transaction prepared with an entry', r[1] == 'yes', r)
got = nil
f = fiber.create(function()
    got = c[1]:call('tarn_drop_prepare', {'test.gt', {ver = 9, timeout = 5, id = 20010}})
end)
fiber.sleep(0.1)
check("DROP PREPARE waits for the table's prepared entries", got == nil)
c[1]:call('tarn_apply', {'GX2'})
fiber.sleep(0.1)
check('... until they are applied', got and got[1] == 'ok', got)
c[1]:call('tarn_drop_abort', {'test.gt', {ver = 9, timeout = 5, id = 20010}})
local ta = {ver = 10, timeout = 5, id = 20010, def = table.deepcopy(gdef)}
ta.def.ver = 10
r = {c[1]:call('tarn_truncate_prepare', {'test.gt', ta}), c[1]:call('tarn_truncate_apply', {'test.gt', ta})}
check('TRUNCATE empties the global index too', r[2][1] == 'ok'
      and c[1]:eval('return box.space._tarn_uix_20010_1:len()') == 0, r)
r = prepare(1, 'GX3', {{'insert', 20010, {5, 'cap', 1}}, {'insert', 20010, {6, 'top', 1}}}, nil, nil, nil)
c[1]:call('tarn_apply', {'GX3'})
r = c[1]:call('tarn_autocommit', {{{'insert', 20011, {'old', 9, 7}}}, {fence = {20010, 9}}})
check('an entry written with the definition from before TRUNCATE gets 1412 (fenced by its table)',
      r[1] == 'error' and r[2] == 1412, r)
local sl = c[1]:new_stream(); sl:begin()
r = sl:call('tarn_commit_local', {'GL1', {{'replace', 20010, {6, 'cap', 1}}, {'replace', 20010, {5, 'pin', 1}}}})
check('a local commit moves a value from a later row to an earlier one', r[1] == 'ok', r)
sl = c[1]:new_stream(); sl:begin()
r = sl:call('tarn_commit_local', {'GL2', {{'replace', 20010, {5, 'cap', 1}}, {'replace', 20010, {6, 'pin', 1}}}})
check('... and two rows swap their values', r[1] == 'ok' and c[1]:eval('return box.space["test.gt"]:get{6}[2]') == 'pin', r)
r = prepare(1, 'GX5', {{'replace', 20010, {6, 'pin', 1}}, {'replace', 20010, {5, 'cap', 1}}})
check('PREPARE of a transaction that moves a value back: yes', r[1] == 'yes', r)
r = c[1]:call('tarn_apply', {'GX5'})
check('... and APPLY lands both rows', r[1] == 'ok' and c[1]:eval('return box.space["test.gt"]:get{5}[2]') == 'cap', r)
c[1]:call('tarn_autocommit', {{{'replace', 20010, {5, 'cap', 1}}, {'replace', 20010, {6, 'top', 1}}}})
r = prepare(1, 'GX4', {{'replace', 20010, {7, 'cap', 1}}, {'delete', 20010, {5}}})
check("PREPARE of a REPLACE that deletes the value's holder: no 1062", r[1] == 'yes', r)
c[1]:call('tarn_apply', {'GX4'})
r = {c[1]:call('tarn_drop_prepare', {'test.gt', {ver = 11, timeout = 5, id = 20010}}),
     c[1]:call('tarn_drop_apply', {'test.gt', {ver = 11, timeout = 5, id = 20010}})}
check('DROP drops the global index space', r[2][1] == 'ok' and not uexists(1), r)

print('ALTER TABLE adds a global index to a table with rows (issue #88)')
do   -- (a block: the main chunk is near Lua's limit of 200 locals)
local ufmt = {{name = 'id', type = 'unsigned'}, {name = 'h', type = 'string'}, {name = 'bucket_id', type = 'unsigned'}}
local uidx = {{name = 'primary', unique = true, parts = {{field = 'id', type = 'unsigned'}}},
              {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}}}
for i = 1, 2 do
    c[i]:call('tarn_create_table', {{name = 'test.ua', id = 20020, engine = 'memtx', format = ufmt, indexes = uidx}})
end
c[1]:call('tarn_autocommit', {{{'insert', 20020, {1, 'a', 1}}, {'insert', 20020, {2, 'b', 1}}}})
local uspace = {name = '_tarn_uix_20020_1', id = 20021, engine = 'memtx',
                format = {{name = 'h', type = 'string'}, {name = 'id', type = 'unsigned'}, {name = 'bucket_id', type = 'unsigned'}},
                indexes = {{name = 'uh', unique = true, parts = {{field = 'h', type = 'string'}}},
                           {name = 'bucket_id', unique = false, parts = {{field = 'bucket_id', type = 'unsigned'}}}}}
local function uargs()
    return {ver = 12, timeout = 1, checks = {}, builds = {}, drop = {}, renames = {}, uix = {uspace}, uix_drop = {},
            def = {name = 'test.ua', id = 20020, format = ufmt, indexes = uidx, json = '{}', ver = 12}}
end
local function uw(ver, op) return c[1]:call('tarn_autocommit', {{op}, {fence = {20020, ver}}}) end
r = c[1]:call('tarn_uix_fill', {20021, {{'a', 1, 7}}, 20020, 12, 2})
check('a fill before PREPARE is refused (1412)', r[1] == 'error' and r[2] == 1412, r)
c[1]:call('tarn_alter_prepare', {'test.ua', uargs(), 1})
r = c[1]:call('tarn_alter_prepare', {'test.ua', uargs(), 2})
check('PREPARE creates the empty global index space', r[1] == 'ok'
      and c[1]:eval('return box.space._tarn_uix_20020_1:len()') == 0, r)
r = c[1]:call('tarn_uix_fill', {20021, {{'z', 9, 7}}, 20020, 12, 1})
check('a fill of the run the job was taken from (generation 2 prepared since) is refused',
      r[1] == 'error' and r[2] == 1412, r)
r = c[1]:call('tarn_alter_prepare', {'test.ua', uargs(), 1})
check('... and so is a late PREPARE of that run', r[1] == 'error' and r[2] == 1412, r)
r = {c[1]:call('tarn_uix_fill', {20021, {{'a', 1, 7}}, 20020, 12, 2}),
     c[1]:call('tarn_uix_fill', {20021, {{'a', 1, 7}, {'b', 2, 7}}, 20020, 12, 2})}
check('the SQL server fills it; entries written again (another fill) count once',
      r[1][1] == 'ok' and r[2][1] == 'ok' and c[1]:eval('return box.space._tarn_uix_20020_1:len()') == 2, r)
r = c[1]:call('tarn_uix_fill', {20021, {{'b', 3, 7}}, 20020, 12, 2})
check('... a value another row holds: duplicate', r[1] == 'duplicate' and r[2] == 1, r)
c[1]:eval('tarn.stale_fences()')
r = uw(0, {'insert', 20020, {9, 'a', 1}})
check('the fence outlives a rebuild from storage (a restart, a new leader): 1412', r[1] == 'error' and r[2] == 1412, r)
r = c[1]:call('tarn_alter_apply', {'test.ua', uargs()})
check('APPLY keeps the filled entries', r[1] == 'ok'
      and c[1]:eval('return box.space._tarn_uix_20020_1:len()') == 2, r)
check('... and clears the stored fence', c[1]:eval('return box.space._tarn_meta:get{"fence:20020"} == nil'))
r = c[1]:call('tarn_uix_fill', {20021, {{'z', 5, 7}}, 20020, 12, 2})
check('a fill after APPLY (a server that lost the job) is refused', r[1] == 'error' and r[2] == 1412, r)
local a2 = uargs(); a2.ver = 13; a2.def.ver = 13; a2.uix = {table.deepcopy(uspace)}
a2.uix[1].name, a2.uix[1].id = '_tarn_uix_20020_2', 20022
r = c[1]:call('tarn_alter_prepare', {'test.ua', a2, 1})
c[1]:call('tarn_alter_abort', {'test.ua', a2})
check('ABORT drops the new space', r[1] == 'ok' and c[1]:eval('return box.space[20022] == nil'), r)
c[1]:eval('tarn.stale_fences()')
r = uw(12, {'insert', 20020, {9, 'q', 1}})
check('... and the table takes writes again, also after a rebuild from storage', r[1] == 'ok', r)
r = c[1]:call('tarn_alter_prepare', {'test.ua', a2, 1})
c[1]:eval('tarn.stale_fences()')
local rw = uw(12, {'replace', 20020, {9, 'r', 1}})
check('a late PREPARE after ABORT is refused and fences nothing', r[1] == 'error' and r[2] == 1412
      and rw[1] == 'ok' and c[1]:eval('return box.space[20022] == nil'), {r, rw})
-- ... also after a later change of the table aborted too (versions only grow)
local a3 = uargs(); a3.ver = 14; a3.def.ver = 14; a3.uix = {}
c[1]:call('tarn_alter_prepare', {'test.ua', a3, 1})
c[1]:call('tarn_alter_abort', {'test.ua', a3})
r = c[1]:call('tarn_alter_prepare', {'test.ua', a2, 1})
c[1]:eval('tarn.stale_fences()')
rw = uw(12, {'replace', 20020, {9, 's', 1}})
check('a late PREPARE of a change older than one begun since is refused', r[1] == 'error' and r[2] == 1412
      and rw[1] == 'ok' and c[1]:eval('return box.space[20022] == nil'), {r, rw})
-- A late PREPARE that fails (here: its checks) after a later run prepared:
-- its undo leaves that run's fence alone.
local a4 = uargs(); a4.ver = 15; a4.def.ver = 15; a4.uix = {table.deepcopy(uspace)}
a4.uix[1].name, a4.uix[1].id = '_tarn_uix_20020_3', 20023
r = c[1]:call('tarn_alter_prepare', {'test.ua', a4, 2})
local bad = table.deepcopy(a4); bad.checks = {{2, 'h', 'chars', 0, box.NULL, box.NULL}}
local rb = c[1]:call('tarn_alter_prepare', {'test.ua', bad, 2})
rw = uw(12, {'replace', 20020, {9, 't', 1}})
check("a failed PREPARE of the same run lifts the fence", r[1] == 'ok' and rb[1] == 'error'
      and rw[1] == 'ok', {r, rb, rw})
-- Run 2 prepares and waits for a prepared transaction; run 3 takes over
-- and prepares too; run 2 times out first: its undo leaves run 3's fence.
r = prepare(1, 'GU1', {{'replace', 20020, {9, 'v', 1}}})
local a4s = table.deepcopy(a4); a4s.timeout = 0.3
local f2 = async('tarn_alter_prepare', {'test.ua', a4s, 2})
fiber.sleep(0.1)
local f3 = async('tarn_alter_prepare', {'test.ua', a4, 3})
local r2 = result(f2)
c[1]:call('tarn_apply', {'GU1'})
local r3 = result(f3)
c[1]:eval('tarn.stale_fences()')
rw = uw(12, {'replace', 20020, {9, 'u', 1}})
check("a late PREPARE's undo leaves the fence of the run that took the job over",
      r[1] == 'yes' and r2[1] == 'timeout' and r3[1] == 'ok' and rw[1] == 'error' and rw[2] == 1412
      and c[1]:eval('return box.space._tarn_meta:get{"fence:20020"}.value.gen') == 3, {r, r2, r3, rw})
c[1]:call('tarn_alter_abort', {'test.ua', a4})
-- Run 4 is held after its checks and builds; run 5 prepares, and the
-- change is aborted; run 4 goes on: it creates nothing, and its undo
-- leaves the tombstone alone.
local a5 = table.deepcopy(a4); a5.ver = 16; a5.def.ver = 16
a5.builds = {{7, 'iv5', true, {{field = 2, type = 'string'}}}}
hook('alter_prepare', 'hold')
local f4 = async('tarn_alter_prepare', {'test.ua', a5, 4})
hook('alter_prepare', 'reached')
c[1]:call('tarn_alter_abort', {'test.ua', a5})
hook('alter_prepare', 'release')
local r4 = result(f4)
c[1]:eval('tarn.stale_fences()')
rw = uw(12, {'replace', 20020, {9, 'w', 1}})
check('a PREPARE of a run that finishes after ABORT creates nothing and fences nothing',
      r4[1] == 'error' and r4[2] == 1412 and rw[1] == 'ok'
      and c[1]:eval('return box.space[20023] == nil and box.space["test.ua"].index[7] == nil')
      and c[1]:eval('return box.space._tarn_meta:get{"fence:20020"}.value.aborted') == true, {r4, rw})
end

-- Issue #77: a read returns its rows a page at a time (at most
-- scan_page_rows rows, or scan_page_bytes bytes) with the position to go on
-- from, so the Lua heap of a storage node no longer grows with the range it
-- scans; a read that finds the heap short anyway fails with 1041 and the
-- node stays up.
print('pages')
local function cfg(k, v) return c[1]:eval('local t = package.loaded.tarn.cfg local o = t[...] t[...] = select(2, ...) return o', {k, v}) end
local function fill(name, engine, n)
    c[1]:call('tarn_create_table', {{name = name, engine = engine, format = vfmt,
        indexes = {vidx[1], {name = 'v', unique = false, parts = {{field = 'v', type = 'integer'}}}}}})
    c[1]:eval([[local s, n = box.space[...], select(2, ...)
        for i = 1, n, 1000 do box.begin()
            for j = i, math.min(i + 999, n) do s:replace{j, j % 7, string.rep('x', 40), 1} end
            box.commit() end]], {name, n})
    return c[1]:eval('return box.space[...].id', {name})
end
-- Every page of a tarn_select, as the SQL server reads them.
local function pages(sid_, conn, args)
    local rows, n, after = {}, 0, box.NULL
    local a = table.copy(args)
    while true do
        a[8], a[9], a[10] = box.NULL, false, after
        local r = conn:call('tarn_select', {sid_, unpack(a, 1, 10)}, {timeout = 60})
        if r[1] ~= 'ok' then return nil, r end
        n = n + 1
        for _, t in ipairs(r[2]) do rows[#rows + 1] = t[1] end
        if a[4] ~= box.NULL then a[4] = a[4] - #r[2] end   -- the limit left
        if r[3] == nil or a[4] == 0 then return rows, n end
        after = r[3]
    end
end
local function in_order(rows, n)
    if #rows ~= n then return false end
    for i = 1, n do if rows[i] ~= i then return false end end
    return true
end
for _, engine in ipairs({'memtx', 'vinyl'}) do
    local psid = fill('test.pg_' .. engine, engine, 10000)
    local r = c[1]:call('tarn_select', {psid, 0, {}, 'ALL', box.NULL, box.NULL, true, true})
    check(engine .. ': an unbounded autocommit read returns one page and where to go on',
          r[1] == 'ok' and #r[2] == 4096 and r[3] ~= nil, {r[1], #(r[2] or {})})
    local rows, n = pages(psid, c[1], {0, {}, 'ALL', box.NULL, box.NULL, true, true})
    check(engine .. ': its pages hold every row once, in order', in_order(rows or {}, 10000) and n == 3, n)
    rows, n = pages(psid, c[1], {0, {100}, 'GE', 5000, {9000}, true, true})
    check(engine .. ': a range with a limit and an end key, in pages',
          rows and #rows == 5000 and rows[1] == 100 and rows[5000] == 5099 and n == 2, {rows and #rows, n})
    local threes = c[1]:eval('return box.space[...].index[1]:count{3}', {'test.pg_' .. engine})
    rows, n = pages(psid, c[1], {1, {3}, 'EQ', box.NULL, box.NULL, true, false})
    check(engine .. ': a secondary-index prefix', rows and #rows == threes and n == 1, {rows and #rows, n, threes})
    local old = cfg('scan_page_bytes', 4096)
    rows, n = pages(psid, c[1], {0, {}, 'ALL', box.NULL, box.NULL, true, true})
    check(engine .. ': pages end at scan_page_bytes too', in_order(rows or {}, 10000) and n > 100, n)
    local st = c[1]:new_stream()
    local got, after, calls = 0, nil, 0
    repeat
        r = st:call('tarn_select', {psid, 0, {}, 'ALL', box.NULL, box.NULL, true, false, box.NULL, calls == 0, after})
        got, after, calls = got + #r[2], r[3], calls + 1
    until r[1] ~= 'ok' or after == nil
    st:rollback()
    check(engine .. ': and in a stream', got == 10000 and calls > 100, {got, calls})
    cfg('scan_page_bytes', old)
    old = cfg('scan_page_rows', 100)
    local keys = {{1}, {5}, {9999}, {20000}}
    local all, after2, calls2 = {}, nil, 0
    repeat
        r = c[1]:call('tarn_select_keys', {psid, 1, {{2}, {4}}, 150, true, false, after2})
        for _, t in ipairs(r[2] or {}) do all[#all + 1] = t[2] end
        after2, calls2 = r[3], calls2 + 1
    until r[1] ~= 'ok' or after2 == nil
    local twos = 0
    for _, v in ipairs(all) do if v == 2 then twos = twos + 1 end end
    check(engine .. ': multi-key reads in pages, each key\'s limit kept', #all == 300 and twos == 150 and calls2 == 3,
          {#all, twos, calls2})
    r = c[1]:call('tarn_select_keys', {psid, 0, keys, box.NULL, true, false})
    check(engine .. ': unique keys in one page', r[1] == 'ok' and #r[2] == 3 and r[3] == nil, r)
    -- Every iterator, in pages of 100 rows, against one read of the index:
    -- reverse ones, and a non-unique index whose pages end amid equal keys.
    local function unpaged(iid, key, it)
        return c[1]:eval([[local s, iid, key, it = ...
            local out = {}
            for _, t in box.space[s].index[iid]:pairs(key, {iterator = it}) do out[#out + 1] = t[1] end
            return out]], {'test.pg_' .. engine, iid, key, it})
    end
    for _, q in ipairs({{0, {9000}, 'LE'}, {0, {9000}, 'LT'}, {0, {5000}, 'GT'}, {0, {}, 'LE'},
                        {1, {3}, 'EQ'}, {1, {3}, 'REQ'}, {1, {3}, 'GE'}, {1, {4}, 'LT'}}) do
        local want = unpaged(q[1], q[2], q[3])
        rows, n = pages(psid, c[1], {q[1], q[2], q[3], box.NULL, box.NULL, true, true})
        local same = rows ~= nil and #rows == #want
        for i = 1, same and #want or 0 do if rows[i] ~= want[i] then same = false end end
        check(string.format('%s: %s on index %d in pages, as one read', engine, q[3], q[1]),
              same and n > 1, {rows and #rows, #want, n})
    end
    cfg('scan_page_rows', old)
    -- A page's lock check covers what it read: a W lock past its last row
    -- does not hold it up; one inside it does.
    local wl = function(txid, id)
        return c[1]:call('tarn_prepare', {txid, 1, {1}, {{'replace', psid, {id, 0, 'w', 1}}}, {}, {}, fiber.time()})
    end
    r = wl('PW_' .. engine, 6000)   -- in the second page (4097..8192)
    -- (Held up, it would fail with a conflict after lock_wait.)
    local p1 = c[1]:call('tarn_select', {psid, 0, {}, 'ALL', box.NULL, box.NULL, true, true})
    check(engine .. ': a W lock past the page does not hold it up',
          r[1] == 'yes' and p1[1] == 'ok' and #p1[2] == 4096, {r, p1[1]})
    local p2 = c[1]:call('tarn_select', {psid, 0, {}, 'ALL', box.NULL, box.NULL, true, true, box.NULL, false, p1[3]})
    check('... the page that reaches it does', p2[1] == 'conflict', p2[1])
    c[1]:call('tarn_abort', {'PW_' .. engine, 1, {1}})
    -- A stored tuple as the position (what the SQL server passes on after
    -- a plain IPROTO SELECT).
    local tup = c[1]:eval('return box.space[...]:get{4096}', {'test.pg_' .. engine})
    st = c[1]:new_stream()
    r = st:call('tarn_select', {psid, 0, {}, 'ALL', 3, box.NULL, true, false, box.NULL, true, tup})
    st:rollback()
    check(engine .. ': a tuple as the position', r[1] == 'ok' and #r[2] == 3 and r[2][1][1] == 4097, r)
end
-- A tarn_lock_range page cut short by its bytes is not the end of the range.
do
    local old = cfg('scan_page_bytes', 1)
    r = lrange('Q77', {{qsid, {1}, 5, {}}}, 'S', {skip = true})
    check('tarn_lock_range goes on after a page cut by its bytes', r[1] == 'ok' and #r[3][1] == 5, r)
    c[1]:call('tarn_unlock', {'Q77'})
    cfg('scan_page_bytes', old)
end
-- Reads that would together hold more rows than the Lua heap allows: they
-- used to kill the node (a memory error where nothing could catch it).
do
    local bsid = fill('test.big', 'memtx', 200000)
    local alloc = "require('internal.alloc')"
    c[1]:eval('collectgarbage() collectgarbage()')
    local limit0 = c[1]:eval('return ' .. alloc .. '.getlimit()')
    local used = c[1]:eval('return ' .. alloc .. '.used()')
    c[1]:eval(alloc .. '.setlimit(...)', {used + 40 * 1024 * 1024})
    local res, done = {}, 0
    for i = 1, 4 do
        fiber.create(function()
            local rows, n = pages(bsid, c[1], {0, {}, 'ALL', box.NULL, box.NULL, true, true})
            res[i] = rows and #rows or n
            done = done + 1
        end)
    end
    wait_for(function() return done == 4 end, 120)
    check('four unbounded reads of 200,000 rows each under a 40 MiB heap margin all complete',
          res[1] == 200000 and res[2] == 200000 and res[3] == 200000 and res[4] == 200000, res)
    -- Less free than the reserve reads keep (16 MiB): a read fails cleanly.
    c[1]:eval('collectgarbage() collectgarbage()')
    used = c[1]:eval('return ' .. alloc .. '.used()')
    c[1]:eval(alloc .. '.setlimit(...)', {used + 8 * 1024 * 1024})
    r = c[1]:call('tarn_select', {bsid, 0, {}, 'ALL', box.NULL, box.NULL, true, true})
    local r2 = c[1]:call('tarn_select', {bsid, 0, {10}, 'GE', box.NULL, {150000}, true, false})
    c[1]:eval(alloc .. '.setlimit(...)', {limit0})
    check('a read short of Lua memory fails with 1041', r[1] == 'error' and r[2] == 1041, r)
    check('... in a scan with an end key too', r2[1] == 'error' and r2[2] == 1041, r2)
    check('... and the node stays up', c[1]:ping() and c[1]:call('tarn_info').stats.read_oom >= 2)
    -- A read that starts with enough heap and runs short while it scans
    -- (pages made large enough to hold the whole table): memtx, vinyl
    -- (checked_read), and in a stream.
    local vbsid = fill('test.bigv', 'vinyl', 200000)
    local oldr, oldb = cfg('scan_page_rows', 10000000), cfg('scan_page_bytes', 1024 * 1024 * 1024)
    local function mid(name, fn)
        c[1]:eval('collectgarbage() collectgarbage()')
        local u = c[1]:eval('return ' .. alloc .. '.used()')
        -- 4 MiB above the reserve: about 50,000 of the 200,000 rows.
        local l = math.max(u + 20 * 1024 * 1024, math.ceil((u + 4 * 1024 * 1024) * 16 / 15))
        c[1]:eval(alloc .. '.setlimit(...)', {l})
        local res = fn()
        c[1]:eval(alloc .. '.setlimit(...)', {limit0})
        check(name .. ' fails with 1041 when the heap runs short as it scans',
              res[1] == 'error' and res[2] == 1041, {res[1], res[2], type(res[2]) == 'table' and #res[2]})
    end
    local oom0 = c[1]:call('tarn_info').stats.read_oom
    mid('memtx: a scan with an end key',
        function() return c[1]:call('tarn_select', {bsid, 0, {1}, 'GE', box.NULL, {200000}, true, true}) end)
    mid('vinyl: an autocommit read',
        function() return c[1]:call('tarn_select', {vbsid, 0, {}, 'ALL', box.NULL, box.NULL, true, true}) end)
    mid('a read in a stream', function()
        local s = c[1]:new_stream()
        local res = s:call('tarn_select', {bsid, 0, {}, 'ALL', box.NULL, box.NULL, true, false, box.NULL, true})
        s:rollback()
        return res
    end)
    cfg('scan_page_rows', oldr); cfg('scan_page_bytes', oldb)
    check('... the scans stopped where they ran short (3 more 1041s), and the node stays up',
          c[1]:ping() and c[1]:call('tarn_info').stats.read_oom == oom0 + 3)
end

-- A heap left full of garbage near the limit is collected by the node
-- (issue #73): LuaJIT's next cycle would start above the limit, so without
-- the heap watchdog every call failed with "not enough memory" until a
-- manual collect.
do
    local alloc = "require('internal.alloc')"
    local collects0 = c[1]:call('tarn_info').stats.heap_collects
    local limit0 = c[1]:eval('return ' .. alloc .. '.getlimit()')
    -- 40 MiB live through a full cycle puts the next threshold near 80 MiB
    -- over the base, above a limit 64 MiB over it; then the 40 MiB is garbage.
    c[1]:eval([[
        local a = require('internal.alloc')
        collectgarbage() collectgarbage()
        a.setlimit(a.used() + 64 * 1048576)
        _G.__t73 = {}
        for i = 1, 40 * 1024 do _G.__t73[i] = string.rep('l', 1000) .. i end
        collectgarbage()
        _G.__t73 = nil
    ]])
    -- garbage in small calls, as requests leave it, past the limit
    for _ = 1, 6000 do
        pcall(c[1].eval, c[1], "for i = 1, 100 do local _ = {i, string.rep('g', 300) .. i} end")
    end
    local ok = wait_for(function()
        return pcall(c[1].call, c[1], 'tarn_info') and pcall(c[1].eval, c[1], 'return 1')
    end, 5)
    local info = ok and c[1]:call('tarn_info')
    pcall(c[1].eval, c[1], 'collectgarbage() ' .. alloc .. '.setlimit(...)', {limit0})
    check('a heap of garbage at the limit is collected without a manual collect, and calls succeed',
          ok and info.stats.heap_collects > collects0, info and info.stats.heap_collects)
end

print(string.format('\n%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
