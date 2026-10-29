-- A three-logical-shard removal dependency regression. The controller owns
-- the private nodes and stops node 2 ONLY after finish prints its zero-count
-- and removed-membership certificate. This mirrors Rust's unchanged removal
-- gates; CI growshrink separately exercises the actual SQL REMOVE command.
--   tarantool member_dependency_test.lua setup|finish|survivors URI1 URI2 URI3
local net, fiber, clock, json = require('net.box'), require('fiber'), require('clock'), require('json')
local stage = assert(arg[1], 'stage required')
assert(stage == 'setup' or stage == 'finish' or stage == 'survivors', 'unknown stage')
local uris = {assert(arg[2]), assert(arg[3]), assert(arg[4])}
local c, pass, fail = {}, 0, 0
local KEY, PREFIX = 'test:member-dependency:130', 'member-dependency-130-'
local members = stage == 'survivors' and {1, 3} or {1, 2, 3}
for _, i in ipairs(members) do
    -- A fresh storage process listens before its Tarn user/schema exist.
    -- Retry only establishing this connection, never a mutating RPC.
    c[i] = net.connect(uris[i], {user = 'tarn', password = 'tarn', wait_connected = .2,
                               connect_timeout = .2, reconnect_after = .05})
    local deadline = clock.monotonic() + 10
    while not c[i]:is_connected() and clock.monotonic() < deadline do fiber.sleep(.02) end
    assert(c[i]:is_connected(), 'private node unavailable: ' .. i .. ' ' .. tostring(c[i].error))
end
local function rpc(i, name, args) return c[i]:call(name, args or {}, {timeout = 5}) end
local function ev(i, body, args) return c[i]:eval(body, args or {}, {timeout = 5}) end
local function ok(r) assert(type(r) == 'table' and r[1] == 'ok', json.encode(r)); return r end
local function check(name, yes, extra)
    if yes then pass = pass + 1; print('  ok   ' .. stage .. ': ' .. name)
    else fail = fail + 1; print('  FAIL ' .. stage .. ': ' .. name .. ' ' .. json.encode(extra or false)) end
end
local function wait(label, fn, seconds)
    local last, deadline = nil, clock.monotonic() + (seconds or 5)
    repeat
        local good, r = pcall(fn); last = good and r or tostring(r)
        if good and r then return r end
        fiber.sleep(.02)
    until clock.monotonic() >= deadline
    error(label .. ' timed out: ' .. json.encode(last))
end
local function ready(ids, scope)
    for _, i in ipairs(ids) do
        wait('actual clock coverage on ' .. i, function()
            return ev(i, [[local scope=...;return tarn.clock.status().read_ready and tarn.clock.check(scope)==nil]], {scope})
        end)
    end
end
local function state(i, txid, sid, rowkey)
    return ev(i, [[local id,sid,key=...;box.begin{txn_isolation='read-confirmed'}
        local p,d=box.space._tarn_part:get{id},box.space._tarn_txn:get{id}
        local space=type(sid)=='number' and box.space[sid] or nil
        local row=space and space:get{key} or nil
        local result={part=p and p.state or 'none',dshard=p and p.dshard or box.NULL,
          parts=p and p.parts or {},decision=d and d.status or 'none',decision_parts=d and d.parts or {},
          locks=box.space._tarn_locks.index.txid:count{id},space_exists=space~=nil,row=row and row:totable() or false}
        box.rollback();local s=tarn.clock.status();result.clock={state=s.state,reason=s.reason or box.NULL,
          epoch=s.epoch,read_ready=s.read_ready,fault=s.fault or box.NULL}
        local m=box.space._tarn_meta:get{'members'};result.members={}
        for _,x in ipairs(m and m.value.shards or {}) do result.members[#result.members+1]={id=x.id,state=x.state} end
        return result]], {txid, sid or box.NULL, rowkey or 1})
end
local function diagnostic(label, txid, sid, rowkey)
    local states = {}
    for _, i in ipairs(members) do states[tostring(i)] = state(i, txid, sid, rowkey) end
    print('MEMBER_DEPENDENCY ' .. json.encode{event = label, stage = stage, txid = txid, states = states})
    return states
end
local function counts()
    local out = {}
    for _, i in ipairs(members) do out[#out + 1] = ok(rpc(i, 'tarn_shard_records', {2}))[2] end
    return out
end
local function zero(a) for _, n in ipairs(a) do if n ~= 0 then return false end end; return true end
local function fixture()
    return assert(ev(1, [[local k=...;local r=box.space._tarn_meta:get{k};return r and r.value]], {KEY}), 'missing owned fixture')
end
local function save(f)
    ev(1, [[local k,v=...;box.begin{txn_isolation='read-confirmed'};box.space._tarn_meta:replace{k,v};box.commit()]], {KEY, f})
end
local function publish(f, state2)
    f.ver = f.ver + 1
    local m = {ver = f.ver, shards = {}}
    for i = 1, 3 do m.shards[i] = {id = i, replicas = {uris[i]}, state = i == 2 and state2 or 'active'} end
    for i = 1, 3 do ok(rpc(i, 'tarn_members_set', {m})) end
    ready({1, 2, 3}, state2 == 'removed' and {1, 3} or {1, 2, 3})
    f.member_state = state2
end
local function bucket(i) return i == 1 and 1 or 25 end
local function args(f, txid, did, parts, sid, i, key, value)
    return {txid, did, parts, i == 2 and {} or {{'insert', sid, {key, value or 7, bucket(i)}}}, {}, {},
            clock.realtime(), box.NULL, {sid, 0}, i == 2 and {} or {bucket(i)}, f.ver}
end
local function promised(f, txid, did, parts, sid, i, key)
    local r = rpc(i, 'tarn_prepare', args(f, txid, did, parts, sid, i, key))
    assert(r[1] == 'yes', 'positive PREPARE failed: ' .. json.encode{reply = r, state = state(i, txid, sid, key)})
end

local good, error_message = xpcall(function()
    for _, i in ipairs(members) do
        wait('complete private schema ' .. i, function()
            return ev(i, [[return rawget(_G,'tarn')~=nil and box.space._tarn_meta and box.space._tarn_scratch
              and box.space._tarn_lazy_floor and box.space._tarn_lazy_floor.index.bucket_id
              and box.space._tarn_meta:get{'shard_id'}~=nil]])
        end)
        assert(ev(i, [[local id=...;return tarn.cfg.test_hooks==true and tarn.cfg.shard_id==id]], {i}),
               'requires private test-hook-enabled logical node ' .. i)
    end
    if stage == 'setup' then
        assert(ev(1, [[return box.space._tarn_meta:get{...}==nil]], {KEY}), 'owned fixture already exists')
        local f = {ver = 0, claims = {}, phase = 'setup', uris = uris}
        for i = 1, 3 do
            local m = rpc(i, 'tarn_members'); f.ver = math.max(f.ver, m[2])
            ev(i, [[tarn.cfg.recovery_after=1000000;tarn.cfg.recovery_interval=.05]])
            local ranges = i == 1 and {{1, 24}} or (i == 3 and {{25, 48}} or {})
            ok(rpc(i, 'tarn_bucket_init', {48, ranges}))
        end
        publish(f, 'active')
        for n, engine in ipairs({'memtx', 'vinyl'}) do
            local sid = 99400 + n
            local def = {id = sid, name = 'test.member_dependency_' .. engine, engine = engine, ver = 0, json = '{}',
                lazy = {}, uix = {}, format = {{'id', 'unsigned'}, {'v', 'integer'}, {'bucket_id', 'unsigned'}},
                indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                           {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}}}}
            for i = 1, 3 do
                ok(rpc(i, 'tarn_create_table', {def}))
                local actual = ev(i, [[local id=...;local s=box.space[id]
                    return s and {id=s.id,name=s.name,engine=s.engine} or false]], {sid})
                assert(actual and actual.id == sid and actual.name == def.name and actual.engine == engine,
                       'physical CREATE identity mismatch on ' .. i .. ': ' .. json.encode(actual))
            end
            for _, i in ipairs({1, 3}) do
                ok(rpc(i, 'tarn_autocommit', {{{'insert', sid, {1, 0, bucket(i)}}}, {fence = {sid, 0}, wb = {bucket(i)}}}))
            end
            -- Isolate the volatile count from all durable obligations. This
            -- read-bearing pause is outside the final atomic promise checks.
            if f.member_state ~= 'active' then publish(f, 'active') end
            ready({1, 2, 3}, {1, 2, 3})
            local tx = PREFIX .. engine .. '-inflight'
            check(engine .. ' starts with no dependency on departing shard', zero(counts()))
            ok(rpc(3, 'tarn_test_hook', {'prepare', 'hold'}))
            local result
            local task = fiber.new(function() result = rpc(3, 'tarn_prepare', args(f, tx, 1, {1, 2, 3}, sid, 3, 60)) end)
            task:set_joinable(true)
            wait('inflight PREPARE hook', function() return rpc(3, 'tarn_test_hook', {'prepare', 'reached'})[2] > 0 end)
            diagnostic(engine .. '-inflight-before-draining', tx, sid, 60)
            check(engine .. ' in-flight PREPARE names every participant, not just D',
                  ok(rpc(3, 'tarn_shard_records', {2}))[2] > 0)
            publish(f, 'draining')
            ok(rpc(3, 'tarn_test_hook', {'prepare', 'release'}))
            wait('inflight PREPARE result', function() return result end); assert(task:join())
            local s = state(3, tx, sid, 60)
            check(engine .. ' no new YES survives a draining publication', result[1] ~= 'yes'
                  and s.part ~= 'prepared' and s.decision ~= 'staging' and s.locks == 0 and not s.row, {result, s})
        end
        publish(f, 'active')
        -- Isolate decision-only dependencies before any remote PREPARED
        -- fixture can make the same shard's aggregate count nonzero.
        for n, engine in ipairs({'memtx', 'vinyl'}) do
            local sid, tx = 99400 + n, PREFIX .. engine .. '-staging-only'
            check(engine .. ' decision-only fixture starts without another dependency', zero(counts()))
            promised(f, tx, 1, {1, 2, 3}, sid, 1, 30)
            ok(rpc(1, 'tarn_abort', {tx, 1, {1, 2, 3}}))
            local d = diagnostic(engine .. '-decision-only-dependency', tx, sid, 30)
            check(engine .. ' local STAGING retains participant2 after local participant is terminal',
                  d['1'].part == 'aborted' and d['1'].decision == 'staging' and d['1'].locks == 0
                  and ok(rpc(1, 'tarn_shard_records', {2}))[2] > 0, d)
            assert(rpc(1, 'tarn_txn_cas', {tx, 'staging', 'aborted', {1, 2, 3}}) == 'aborted')
            check(engine .. ' authoritative decision completion releases its isolated dependency', zero(counts()))
            f.claims[#f.claims + 1] = {txid = tx, did = 1, parts = {1, 2, 3}, sid = sid, key = 30}
        end
        for n, engine in ipairs({'memtx', 'vinyl'}) do
            local sid = 99400 + n
            -- Critical baseline witness: D=3, P={2,3}, and node2 is already
            -- terminal with no lock. The transaction ID encodes neither D nor P.
            local tx = PREFIX .. engine .. '-d3-terminal2'
            promised(f, tx, 3, {2, 3}, sid, 3, 40)
            ok(rpc(2, 'tarn_abort', {tx, 3, {2, 3}}))
            local d = diagnostic(engine .. '-remote-participant-dependency', tx, sid, 40)
            check(engine .. ' terminal participant2 has no remaining row locks', d['2'].part == 'aborted' and d['2'].locks == 0)
            check(engine .. ' survivor PREPARED still prevents participant2 removal',
                  ok(rpc(3, 'tarn_shard_records', {2}))[2] > 0, d)
            f.claims[#f.claims + 1] = {txid = tx, did = 3, parts = {2, 3}, sid = sid, key = 40}
            tx = PREFIX .. engine .. '-d1-missing2'
            for _, i in ipairs({1, 3}) do promised(f, tx, 1, {1, 2, 3}, sid, i, 10) end
            d = diagnostic(engine .. '-missing-participant-dependency', tx, sid, 10)
            check(engine .. ' missing participant2 cannot make survivor dependencies zero', d['2'].part == 'none'
                  and ok(rpc(1, 'tarn_shard_records', {2}))[2] > 0 and ok(rpc(3, 'tarn_shard_records', {2}))[2] > 0, d)
            f.claims[#f.claims + 1] = {txid = tx, did = 1, parts = {1, 2, 3}, sid = sid, key = 10}
            tx = PREFIX .. engine .. '-d1-aborted2'
            for i = 1, 3 do promised(f, tx, 1, {1, 2, 3}, sid, i, 20) end
            assert(rpc(1, 'tarn_txn_cas', {tx, 'staging', 'aborted', {1, 2, 3}}) == 'aborted')
            ok(rpc(2, 'tarn_abort', {tx, 1, {1, 2, 3}}))
            d = diagnostic(engine .. '-aborted-participant-dependency', tx, sid, 20)
            check(engine .. ' decided ABORT still retains dependencies until survivor completion',
                  d['2'].part == 'aborted' and d['2'].locks == 0 and d['3'].part == 'prepared'
                  and ok(rpc(3, 'tarn_shard_records', {2}))[2] > 0, d)
            f.claims[#f.claims + 1] = {txid = tx, did = 1, parts = {1, 2, 3}, sid = sid, key = 20}
        end
        publish(f, 'draining')
        for n, engine in ipairs({'memtx', 'vinyl'}) do
            local sid, tx = 99400 + n, PREFIX .. engine .. '-after-draining'
            local r = rpc(3, 'tarn_prepare', args(f, tx, 1, {1, 2, 3}, sid, 3, 50))
            local s = state(3, tx, sid, 50)
            check(engine .. ' fresh admission refuses a draining participant even when D stays active',
                  r[1] == 'moved' and r[2] == 1213 and s.part ~= 'prepared' and s.decision ~= 'staging'
                  and s.locks == 0 and not s.row, {r, s})
            local cas_id = PREFIX .. engine .. '-new-staging'
            local accepted, rcas = pcall(rpc, 1, 'tarn_txn_cas', {cas_id, 'none', 'staging', {1, 2, 3}})
            s = state(1, cas_id, sid, 51)
            check(engine .. ' NONE to STAGING cannot add a draining dependency', not accepted
                  and type(rcas) == 'cdata' and rcas.code == box.error.TRANSACTION_CONFLICT
                  and s.decision == 'none' and s.part == 'none', {accepted, tostring(rcas), s})
            local abort_id = PREFIX .. engine .. '-new-abort-fence'
            check(engine .. ' a fresh recovery ABORT fence remains available while draining',
                  rpc(1, 'tarn_txn_cas', {abort_id, 'none', 'aborted', {1, 2, 3}}) == 'aborted')
            r = rpc(3, 'tarn_prepare', {PREFIX .. engine .. '-d1-missing2', 1, {1, 2, 3}, {}, {}, {}, 0})
            check(engine .. ' confirmed YES revotes before new dependency and age checks', r[1] == 'yes', r)
        end
        f.phase = 'barrier'; save(f)
        check('real all-shard removal predicate stays nonzero', not zero(counts()), counts())
        print('MEMBER_DEPENDENCY ' .. json.encode{event = 'barrier', counts = counts(), claims = f.claims})
    elseif stage == 'finish' then
        local f = fixture(); assert(f.phase == 'barrier')
        ready({1, 2, 3}, {1, 2, 3})
        -- Actual background recovery, not hand-written participant states:
        -- absent/aborted participants force an authoritative ABORT decision.
        for i = 1, 3 do ev(i, [[tarn.cfg.recovery_after=0;tarn.cfg.recovery_interval=.05]]) end
        wait('authoritative recovery finishes exact owned claims', function()
            for _, claim in ipairs(f.claims) do
                for _, i in ipairs(claim.parts) do
                    local s = state(i, claim.txid, claim.sid, claim.key)
                    if s.part == 'prepared' or s.decision == 'staging' or s.locks ~= 0 then return false end
                    if s.row then return false end
                end
            end
            return zero(counts())
        end, 10)
        check('authoritative recovery releases all departing-shard dependencies', zero(counts()), counts())
        local report = ok(rpc(2, 'tarn_buckets'))
        check('departing node owns no bucket before removal', #report[4] == 0 and #report[5] == 0, report)
        assert(fail == 0, 'refusing to publish removal after a failed drain check')
        publish(f, 'removed')
        for i = 1, 3 do
            local m = ok(rpc(i, 'tarn_members')); check('node ' .. i .. ' acknowledges removed membership',
                m[2] == f.ver and m[3][2].id == 2 and m[3][2].state == 'removed', m)
        end
        assert(fail == 0, 'refusing stop certificate without every membership ACK')
        f.phase = 'removed'; save(f)
        print('SAFE_TO_STOP_SHARD_2 ' .. json.encode{membership = f.ver, counts = counts(), bucket_count = #report[4]})
    else
        local f = fixture(); assert(f.phase == 'removed')
        for _, i in ipairs({1, 3}) do ev(i, [[tarn.cfg.recovery_after=1000000]]) end
        local stopped = net.connect(uris[2], {user = 'tarn', password = 'tarn', wait_connected = .2, connect_timeout = .2})
        check('controller stopped owned node2 only after zero-count removal', not stopped:is_connected()); stopped:close()
        ready({1, 3}, {1, 3})
        for n, engine in ipairs({'memtx', 'vinyl'}) do
            local sid = 99400 + n
            for _, i in ipairs({1, 3}) do
                local r = rpc(i, 'tarn_select', {sid, 0, {1}, 'EQ', 1, box.NULL, false, true, box.NULL, false, box.NULL, bucket(i)})
                check(engine .. ' strong read on survivor ' .. i .. ' is not stranded', r[1] == 'ok'
                      and #r[2] == 1 and r[2][1][2] == 0, r)
            end
        end
        for _, i in ipairs({1, 3}) do
            local move = PREFIX .. 'fence-' .. i
            ok(rpc(i, 'tarn_bucket_send_begin', {bucket(i), i == 1 and 3 or 1, move, 1}))
            local r = rpc(i, 'tarn_bucket_fence', {bucket(i), move, 1})
            check('survivor ' .. i .. ' bucket fence drains after authoritative recovery', r[1] == 'ok', r)
            ok(rpc(i, 'tarn_bucket_send_abort', {bucket(i), move}))
        end
        for n, engine in ipairs({'memtx', 'vinyl'}) do
            for k, action in ipairs({'apply', 'abort', 'revote'}) do
                local tx, sid = PREFIX .. engine .. '-fault-' .. action, 99400 + n
                for _, i in ipairs({1, 3}) do promised(f, tx, 1, {1, 3}, sid, i, 100 + k) end
                if action ~= 'revote' then
                    assert(rpc(1, 'tarn_txn_cas', {tx, 'staging', action == 'apply' and 'committed' or 'aborted', {1, 3}})
                           == (action == 'apply' and 'committed' or 'aborted'))
                end
            end
        end
        -- Last: a genuine module-local wall/monotonic divergence, never a
        -- host clock change, healthy-certificate substitute or fault reset.
        for _, i in ipairs({1, 3}) do
            local s = ev(i, [[assert(tarn.cfg.test_hooks);tarn.clock.stop();local s=tarn.clock.status()
                assert(tarn.clock.test_time(s.wall+100,s.mono)[1]=='ok');tarn.clock.observe();tarn.clock.check()
                local v=tarn.clock.status();return {fault=v.fault,read_ready=v.read_ready}]])
            check('survivor ' .. i .. ' has a genuine latched clock fault', type(s.fault) == 'string' and not s.read_ready, s)
        end
        for n, engine in ipairs({'memtx', 'vinyl'}) do
            local sid = 99400 + n
            local revote = PREFIX .. engine .. '-fault-revote'
            for _, i in ipairs({1, 3}) do
                check(engine .. ' confirmed receipt revotes under fault on ' .. i,
                      rpc(i, 'tarn_prepare', {revote, 1, {1, 3}, {}, {}, {}, 0})[1] == 'yes')
            end
            check(engine .. ' existing decision commits under fault',
                  rpc(1, 'tarn_txn_cas', {revote, 'staging', 'committed', {1, 3}}) == 'committed')
            for k, action in ipairs({'apply', 'abort', 'revote'}) do
                local tx = PREFIX .. engine .. '-fault-' .. action
                for _, i in ipairs({1, 3}) do
                    ok(rpc(i, action == 'abort' and 'tarn_abort' or 'tarn_apply', action == 'abort' and {tx, 1, {1, 3}} or {tx}))
                    local s = state(i, tx, sid, 100 + k)
                    check(engine .. ' decided ' .. action .. ' finishes exact work under fault on ' .. i,
                          s.part == (action == 'abort' and 'aborted' or 'applied') and s.locks == 0
                          and (action == 'abort' and not s.row or action ~= 'abort' and s.row and s.row[2] == 7), s)
                end
            end
            local tx = PREFIX .. engine .. '-fresh-after-fault'
            local r = rpc(1, 'tarn_prepare', args(f, tx, 1, {1, 3}, sid, 1, 199))
            local s = state(1, tx, sid, 199)
            check(engine .. ' clock UNKNOWN creates neither a fake NO nor a new promise',
                  r[1] == 'expired' and s.part == 'none' and s.decision == 'none' and s.locks == 0 and not s.row, {r, s})
        end
        f.phase = 'finished'; save(f)
    end
end, debug.traceback)
if not good then print('MEMBER_DEPENDENCY_ERROR ' .. tostring(error_message)); fail = fail + 1 end
for i, conn in pairs(c) do
    if stage == 'setup' then pcall(conn.call, conn, 'tarn_test_hook', {'prepare', 'release'}, {timeout = 1}) end
    conn:close()
end
print(string.format('%d passed, %d failed (%s)', pass, fail, stage))
os.exit(fail == 0 and 0 or 1)
