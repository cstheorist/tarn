-- Two real networked RF1 nodes: validates actual bounded public RPC clock
-- sampling and protocol hook placement. This is not RF3/initial-time proof.
local net, fiber, clock, json = require('net.box'), require('fiber'), require('clock'), require('json')
local c, checks = {}, 0
for i = 1, 2 do
    local until_ = clock.monotonic() + 10
    repeat
        local candidate = net.connect('127.0.0.1:' .. (3560 + i),
            {user = 'tarn', password = 'tarn', wait_connected = .2, connect_timeout = .2})
        local good, probe = pcall(candidate.call, candidate, 'tarn_clock_probe', {}, {timeout = .2})
        if good and type(probe) == 'table' then c[i] = candidate; break end
        candidate:close(); fiber.sleep(.02)
    until clock.monotonic() > until_
    assert(c[i], 'clock node did not initialize')
end
local function rpc(i, fn, args) return c[i]:call(fn, args or {}, {timeout = 2}) end
local function ev(i, body, args) return c[i]:eval(body, args or {}, {timeout = 2}) end
local function wait(fn, secs)
    local deadline = clock.monotonic() + (secs or 5)
    repeat local ok, r = pcall(fn); if ok and r then return r end; fiber.sleep(.02)
    until clock.monotonic() > deadline
    return false
end
local function check(yes, text, extra) assert(yes, text .. (extra and (' ' .. json.encode(extra)) or ''));
    checks = checks + 1; print('ok ' .. text) end
local function ok(r) assert(type(r) == 'table' and r[1] == 'ok', json.encode(r)); return r end
check(wait(function()
    for i = 1, 2 do
        local s = rpc(i, 'tarn_clock_status')
        if s.state ~= 'healthy' or s.certificate_kind ~= 'peer' or s.peer_count ~= 2 then return false end
    end
    return true
end), 'both actual RPC samplers certify both logical nodes')
local function args(tx, sid, key)
    return {tx, 1, {1}, {{'insert', sid, {key, 7, 1}}}, {}, {}, clock.realtime()}
end
for i, engine in ipairs({'memtx', 'vinyl'}) do
    local sid = 97000 + i
    ok(rpc(1, 'tarn_create_table', {{id = sid, name = 'test.clock_' .. engine, ver = 0, engine = engine,
        json = '{}', lazy = {}, uix = {},
        format = {{'id', 'unsigned'}, {'n', 'unsigned'}, {'bucket_id', 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}}}}}))
    ok(rpc(1, 'tarn_autocommit', {{{'insert', sid, {1, 5, 1}}}}))
    -- A declared write-only native stream has no first-read certificate.
    local st = c[1]:new_stream(); st:begin()
    local a = args('clock_write_only_' .. engine, sid, 2)
    check(st:call('tarn_prepare', a, {timeout = 2})[1] == 'yes', engine .. ' declared write-only PREPARE does not replace a read context')
    rpc(1, 'tarn_txn_cas', {a[1], 'staging', 'aborted', {1}}); ok(rpc(1, 'tarn_abort', {a[1], 1, {1}}))
    -- Delay after old-row data was observed, before final YES/NO.
    ok(rpc(1, 'tarn_test_hook', {'prepare', 'hold'}))
    a = args('clock_expired_' .. engine, sid, 1) -- duplicate -> fresh NO if guard is misplaced
    local result
    fiber.create(function() result = rpc(1, 'tarn_prepare', a) end)
    check(wait(function() return rpc(1, 'tarn_test_hook', {'prepare', 'reached'})[2] > 0 end), engine .. ' PREPARE held after planning read')
    fiber.sleep(.7)
    ok(rpc(1, 'tarn_test_hook', {'prepare', 'release'}))
    check(wait(function() return result end) and result[1] == 'expired', engine .. ' yielded duplicate refusal expires without a fresh NO')
    check(ev(1, [[local id=...;return box.space._tarn_part:get{id}==nil and box.space._tarn_txn:get{id}==nil]], {a[1]}),
          engine .. ' expired planning leaves no receipt or decision')
end
-- A legacy one-margin receipt must survive its old expiry, then collect
-- only under the new two-margin policy and atomic persisted watermark.
local created = ev(1, [[local t=require('clock').realtime();box.space._tarn_part:insert{
    'clock_legacy','aborted',1,{1},t,t+1};return t]])
fiber.sleep(1.1)
check(ev(1, [[return box.space._tarn_part:get{'clock_legacy'}~=nil]]), 'legacy one-margin expiry cannot authorize early deletion')
check(wait(function() return ev(1, [[return box.space._tarn_part:get{'clock_legacy'}==nil]]) end), 'healthy GC eventually collects the legacy receipt')
check(rpc(1, 'tarn_clock_status').high > created + 1.5, 'the same GC transaction persists a sufficient high-water mark')

ev(1, [[tarn.cfg.peers['3']={'127.0.0.1:3563'};tarn.clock.invalidate('owned unreachable-peer control')]])
check(wait(function() return rpc(1, 'tarn_clock_status').reason == 'required clock peer unreachable or expired' end),
      'unreachable required peer closes actual certification', rpc(1, 'tarn_clock_status'))
local local_args = args('clock_unrelated', 97001, 8)
check(rpc(1, 'tarn_prepare', local_args)[1] == 'yes', 'unrelated logical shard loss does not block relevant PREPARE')
rpc(1, 'tarn_txn_cas', {'clock_unrelated', 'staging', 'aborted', {1}})
ok(rpc(1, 'tarn_abort', {'clock_unrelated', 1, {1}}))
ev(1, [[local t=require('clock').realtime();box.space._tarn_part:insert{'clock_unrelated_gc','aborted',1,{1},t-10,t-9}]])
check(wait(function() return ev(1, [[return box.space._tarn_part:get{'clock_unrelated_gc'}==nil]]) end),
      'unrelated logical shard loss does not block relevant GC')
check(ok(rpc(1, 'tarn_autocommit', {{{'update', 97001, {999}, {{'=', 2, 7}}}}}))[2] == 0,
      'healthy local no-op DML retains progress while full unrelated coverage is UNKNOWN')
local missing_args = args('clock_unknown', 97001, 9); missing_args[3] = {1, 3}
local denied = rpc(1, 'tarn_prepare', missing_args)
check(denied[1] == 'expired' and ev(1, [[return box.space._tarn_part:get{'clock_unknown'}==nil]]),
      'unknown relevant health uses legacy-compatible no-vote without effects')
ev(1, [[tarn.cfg.peers['3']=nil;tarn.clock.invalidate('owned peer restored')]])
check(wait(function() return rpc(1, 'tarn_clock_status').certificate_kind == 'peer'
    and rpc(1, 'tarn_clock_status').state == 'healthy' end), 'complete real peer sampling recovers UNKNOWN')

for _, pair in ipairs({{'clock_apply', 20}, {'clock_abort', 21}, {'clock_revote', 22}}) do
    check(rpc(1, 'tarn_prepare', args(pair[1], 97001, pair[2]))[1] == 'yes', 'prepare durable completion fixture ' .. pair[1])
end
rpc(1, 'tarn_txn_cas', {'clock_apply', 'staging', 'committed', {1}})
rpc(1, 'tarn_txn_cas', {'clock_abort', 'staging', 'aborted', {1}})
ev(1, [[local c=require('clock');local t=c.realtime();box.space._tarn_part:insert{
 'clock_keep_fault','aborted',1,{1},t-10,t-9};assert(tarn.clock.test_time(t+5,c.monotonic())[1]=='ok');tarn.clock.check()]])
check(rpc(1, 'tarn_clock_probe').local_ok == false, 'raw public clock probe stays available on a latched fault')
local st = c[1]:new_stream(); st:begin()
check(st:call('tarn_prepare', {'clock_revote', 1, {1}, {}, {}, {}}, {timeout = 2})[1] == 'yes',
      'confirmed receipt revotes before stream/timestamp/new-admission checks')
check(ok(rpc(1, 'tarn_apply', {'clock_apply'}))[2] == 'applied'
      and ev(1, [[return box.space[97001]:get{20}~=nil]]), 'decided APPLY remains available during clock fault')
check(ok(rpc(1, 'tarn_abort', {'clock_abort', 1, {1}}))[2] == 'aborted'
      and ev(1, [[return box.space[97001]:get{21}==nil]]), 'decided ABORT remains available during clock fault')
local fresh = rpc(1, 'tarn_prepare', args('clock_after_fault', 97001, 30))
check(fresh[1] == 'expired', 'new admission stays closed using the legacy no-vote wire tag')
local no_op, no_op_error = pcall(rpc, 1, 'tarn_autocommit', {{{'update', 97001, {999}, {{'=', 2, 7}}}}})
check(not no_op and type(no_op_error) == 'cdata' and no_op_error.code == box.error.TRANSACTION_CONFLICT,
      'a no-op DML result cannot bypass local clock/quorum guards')
local cap = rpc(1, 'tarn_read_contract')
check(cap[1] == 'strong_reads' and cap[2] == 1, 'code capability stays available independent of clock readiness')
fiber.sleep(.3)
check(ev(1, [[return box.space._tarn_part:get{'clock_keep_fault'}~=nil]]), 'faulted GC retains an otherwise-expired refusal')
check(wait(function() return ev(1, [[return type(box.space._tarn_meta:get{'clock:policy:1'}.value.fault)=='string']]) end),
      'clock fault is durably latched independently of completion work')
-- No unrelated transactions are cleaned; these exact owned promises only.
rpc(1, 'tarn_txn_cas', {'clock_revote', 'staging', 'aborted', {1}})
ok(rpc(1, 'tarn_abort', {'clock_revote', 1, {1}}))
for _, conn in ipairs(c) do conn:close() end
print(string.format('%d passed, 0 failed', checks)); os.exit(0)
