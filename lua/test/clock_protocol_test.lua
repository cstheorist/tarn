-- Actual RF3: durability/configuration is part of fresh refusal and GC
-- authorization. Test-only wrappers hold exact owned calls, never replay a
-- mutation, and are restored before fixture shutdown.
local net, fiber, fio, ffi, json, clock = require('net.box'), require('fiber'), require('fio'),
    require('ffi'), require('json'), require('clock')
io.stdout:setvbuf('no')
local work = assert(os.getenv('CLOCK_PROTOCOL_WORK'))
local base = tonumber(assert(os.getenv('CLOCK_PROTOCOL_BASE')))
local cs, passed, stopped = {}, 0, nil
local function event(name, value)
    local f = assert(io.open(work .. '/events.jsonl', 'a'))
    f:write(json.encode{event = name, value = value}, '\n'); f:close()
end
local function check(name, yes, value)
    event(name, value or {}); assert(yes, name .. ': ' .. json.encode(value))
    passed = passed + 1; print('  ok   ' .. name)
end
local function await(name, fn, secs)
    local deadline, last = fiber.clock() + (secs or 10)
    repeat local ok, x = pcall(fn); if ok and x then return x end
        last = x; fiber.sleep(.01)
    until fiber.clock() >= deadline
    error(name .. ': timed out: ' .. tostring(last))
end
for i = 1, 3 do
    cs[i] = net.connect('127.0.0.1:' .. (base + i), {user = 'tarn', password = 'tarn',
        wait_connected = false, connect_timeout = .2, reconnect_after = .05})
end
local function ev(i, code, args) return cs[i]:eval(code, args or {}, {timeout = 8}) end
local function rpc(i, proc, args) return cs[i]:call(proc, args or {}, {timeout = 8}) end
local function ok(r) assert(type(r) == 'table' and r[1] == 'ok', json.encode(r)); return r end
local function state(i)
    return ev(i, [[if not rawget(_G,'tarn') or not box.space._tarn_part
        or not box.space._tarn_scratch or not box.space._tarn_lazy_floor then return false end
        local s=box.space._tarn_meta:get{'shard_id'}
        return s and {ro=box.info.ro,queue=box.info.synchro.queue,clock=tarn.clock.status(),
            native=tarn_info().native,sync=tarn.cfg.sync,wal=box.cfg.wal_mode} or false]])
end
local leader = await('writable/loaded RF3 leader', function()
    local found
    for i = 1, 3 do
        local s = state(i)
        if s and not s.ro and s.queue.len == 0 and s.clock.read_ready then found = i end
    end
    return found
end, 40)
local c, follower = cs[leader], leader % 3 + 1
local function healthy()
    return ev(leader, 'return tarn.clock.read_check()==nil and tarn.clock.check({1})==nil')
end
await('relevant clock coverage', healthy)
for i = 1, 3 do
    await('all replicas initialized', function() return state(i) end)
    ev(i, 'tarn.cfg.read_freshness_timeout=.15;tarn.cfg.recovery_interval=.05;tarn.cfg.recovery_after=1000000')
end
check('actual RF3 Lua fsync mode', state(leader).sync and state(leader).wal == 'fsync'
    and state(leader).native == false, state(leader))
local function signal(i, sig)
    local f = assert(io.open(work .. '/s' .. i .. '.pid')); local pid = tonumber(f:read('*a')); f:close()
    f = assert(io.open(work .. '/s' .. i .. '.process-identity')); local identity = f:read('*a'):match('^%s*(.-)%s*$'); f:close()
    f = assert(io.open('/proc/' .. pid .. '/stat')); local line = f:read('*a'); f:close()
    local fields = {}; for v in line:gmatch('%S+') do fields[#fields+1] = v end
    assert(fields[4] .. ':' .. fields[22] == identity, 'owned signal identity changed')
    pcall(ffi.cdef, 'int kill(int,int); int getpid(void);')
    assert(pid ~= ffi.C.getpid() and ffi.C.kill(pid, sig) == 0)
    stopped = sig == 19 and i or nil; event('owned signal', {node = i, pid = pid, signal = sig})
end
local function absent(id)
    return ev(leader, [[local id=...;box.begin{txn_isolation='read-confirmed'}
        local r=box.space._tarn_part:get{id}==nil and box.space._tarn_txn:get{id}==nil
            and box.space._tarn_locks.index.txid:count{id}==0
        box.rollback();return r]], {id})
end
local function reset()
    if stopped then pcall(signal, stopped, 18) end
    pcall(ev, leader, [[
        if rawget(_G,'clock_original_admit') then tarn.clock.admit=clock_original_admit end
        clock_admit_hold=false
        if rawget(_G,'clock_original_statuses') then tarn_txn_statuses=clock_original_statuses end
        clock_gc_hold=false
        tarn_test_hook('prepare','release')
        box.cfg{replication_synchro_quorum=2,replication_linearizable_quorum='N - Q + 1'}
        for _,name in ipairs({'_tarn_part','_tarn_meta'}) do
            if box.space[name] and not box.space[name].is_sync then box.space[name]:alter{is_sync=true} end
        end]])
end
local success, failure = xpcall(function()
    local sids = {98101, 98102}
    for n, engine in ipairs({'memtx', 'vinyl'}) do
        local sid = sids[n]
        ok(rpc(leader, 'tarn_create_table', {{id = sid, name = 'test.clock_protocol_' .. engine,
            ver = 0, json = '{}', lazy = {}, uix = {}, engine = engine,
            format = {{'id','unsigned'},{'v','unsigned'},{'bucket_id','unsigned'}},
            indexes = {{name='primary',unique=true,parts={{1,'unsigned'}}},
                {name='bucket_id',unique=false,parts={{3,'unsigned'}}}}}}))
        ok(rpc(leader, 'tarn_autocommit', {{{'insert',sid,{1,5,1}}}}))
        for _, mutation in ipairs({'part', 'meta', 'quorum'}) do
            await('coverage before refusal control', healthy)
            local id = 'clock-no-' .. engine .. '-' .. mutation
            ok(rpc(leader, 'tarn_test_hook', {'prepare','hold'}))
            local a = {id,1,{1},{{'insert',sid,{1,9,1}}},{},{},clock.realtime()}
            local f = c:call('tarn_prepare', a, {is_async=true})
            await('PREPARE after planning', function()
                return rpc(leader,'tarn_test_hook',{'prepare','reached'})[2] > 0
            end)
            ev(leader, [[local kind=...
                if kind=='quorum' then box.cfg{replication_synchro_quorum=1,replication_linearizable_quorum=1}
                else box.space[kind=='part' and '_tarn_part' or '_tarn_meta']:alter{is_sync=false} end]], {mutation})
            ok(rpc(leader,'tarn_test_hook',{'prepare','release'}))
            local r, e = f:wait_result(3); assert(r,tostring(e)); r = r[1]
            check(engine .. ' post-yield ' .. mutation .. ' change gives no vote or refusal history',
                r[1] == 'expired' and r[2] == 1213 and absent(id), r)
            reset(); await('coverage restored', healthy)
        end
        -- Hold only the first admission after its authoritative receipt MISS.
        -- The identical duplicate can then install a real durable YES.
        local id = 'clock-concurrent-' .. engine
        ev(leader, [[assert(tarn.cfg.test_hooks);clock_original_admit=tarn.clock.admit
            clock_admit_once=true;clock_admit_hold=true;clock_admit_reached=false
            tarn.clock.admit=function(...)
                if clock_admit_once then clock_admit_once=false;clock_admit_reached=true
                    while clock_admit_hold do require('fiber').sleep(.005) end end
                return clock_original_admit(...) end]])
        local a = {id,1,{1},{{'insert',sid,{2,7,1}}},{},{},clock.realtime()}
        local first = c:call('tarn_prepare',a,{is_async=true})
        await('first admission held after receipt MISS',function() return ev(leader,'return clock_admit_reached') end)
        local second = rpc(leader,'tarn_prepare',a)
        check(engine .. ' concurrent duplicate installs confirmed YES',second[1]=='yes',second)
        ev(leader,'clock_admit_hold=false')
        local r,e=first:wait_result(3);assert(r,tostring(e));r=r[1]
        check(engine .. ' earlier MISS cannot contradict concurrent durable YES with NO',
            (r[1]=='yes' or r[1]=='expired') and ev(leader,
                'return box.space._tarn_part:get{...}.state', {id})=='prepared',r)
        ev(leader,'tarn.clock.admit=clock_original_admit;clock_original_admit=nil')
        rpc(leader,'tarn_txn_cas',{id,'staging','aborted',{1}})
        ok(rpc(leader,'tarn_abort',{id,1,{1}}))
    end

    -- Unsupported actual durability cannot authorize deletion or advance H.
    for _, mutation in ipairs({'part','quorum'}) do
        await('coverage before GC control',healthy)
        local id='clock-gc-'..mutation
        local before=ev(leader,[[local kind,id=...
            if kind=='quorum' then box.cfg{replication_synchro_quorum=1,replication_linearizable_quorum=1}
            else box.space._tarn_part:alter{is_sync=false} end
            local t=require('clock').realtime();box.space._tarn_part:insert{id,'aborted',1,{1},t-1000,t-900}
            return box.space._tarn_meta:get{'clock:policy:1'}.value.high]],{mutation,id})
        fiber.sleep(.65)
        local after=ev(leader,[[local id=...;return {kept=box.space._tarn_part:get{id}~=nil,
            high=box.space._tarn_meta:get{'clock:policy:1'}.value.high}]],{id})
        check('unsupported '..mutation..' GC retains receipt and durable H',after.kept and after.high==before,after)
        reset();await('healthy GC resumes',function()return ev(leader,'return box.space._tarn_part:get{...}==nil',{id})end)
    end
    -- A real terminal status was read before the hold; mutate controls only
    -- after that yielding proof, so the deletion-side guard is required.
    local id='clock-gc-after-status'
    ev(leader,[[assert(tarn.cfg.test_hooks);clock_original_statuses=tarn_txn_statuses
        clock_gc_hold=true;clock_gc_reached=false
        tarn_txn_statuses=function(ids)
            local r=clock_original_statuses(ids)
            for _,id in ipairs(ids) do if id=='clock-gc-after-status' then
                clock_gc_reached=true;while clock_gc_hold do require('fiber').sleep(.005) end
            end end;return r end
        local t=require('clock').realtime();box.begin{txn_isolation='read-confirmed'}
        box.space._tarn_part:insert{'clock-gc-after-status','applied',1,{1},t-1000,t-900}
        box.space._tarn_txn:insert{'clock-gc-after-status','committed',{1},t-1000,t-900};box.commit()]])
    await('GC held after actual status proof',function()return ev(leader,'return clock_gc_reached')end)
    local before=ev(leader,[[box.space._tarn_part:alter{is_sync=false};clock_gc_hold=false
        return box.space._tarn_meta:get{'clock:policy:1'}.value.high]])
    fiber.sleep(.15)
    local after=ev(leader,[[return {kept=box.space._tarn_part:get{'clock-gc-after-status'}~=nil,
        high=box.space._tarn_meta:get{'clock:policy:1'}.value.high}]])
    check('post-yield GC control change retains receipt and durable H',after.kept and after.high==before,after)
    reset();await('post-yield receipt eventually GCed',function()return ev(leader,'return box.space._tarn_part:get{...}==nil',{id})end)

    for n,engine in ipairs({'memtx','vinyl'}) do
        local sid=sids[n]
        -- The short deadline belongs to the unavailable-prefix controls.
        -- Restore it for each engine after the preceding positive recovery.
        ev(leader,'tarn.cfg.read_freshness_timeout=.15')
        local st=c:new_stream()
        ok(st:call('tarn_select',{sid,0,{1},0,1,box.NULL,true,false,box.NULL,true},{timeout=3}))
        local original=st:eval("return tostring(require('ffi').C.box_txn_id())")
        for i=1,3 do ev(i,'box.cfg{replication_synchro_quorum=3,replication_synchro_timeout=3}') end
        signal(follower,19)
        local writer=c:eval('local sid=...;box.space[sid]:replace{3,8,1};return true',{sid},{is_async=true})
        await('actual synchronous pending write',function()return ev(leader,'return box.info.synchro.queue.len')>0 end,1)
        local id='clock-borrowed-timeout-'..engine
        local validated,validation=pcall(st.call,st,'tarn_validate',
            {id,{{sid,0,{1},1}},{},true},{timeout=2})
        check(engine..' final validation prefix failure retains exact marked transaction',
            not validated and type(validation)=='cdata' and validation.code~=0
            and st:eval("return tostring(require('ffi').C.box_txn_id())")==original,
            {error=tostring(validation),before=original})
        local local_ok,local_error=pcall(st.call,st,'tarn_commit_local',
            {id,{},{{sid,0,{1},1}},{}},{timeout=2})
        check(engine..' zero-write local completion cannot bypass a failed final prefix',
            not local_ok and type(local_error)=='cdata' and local_error.code~=0
            and st:eval("return tostring(require('ffi').C.box_txn_id())")==original,
            {error=tostring(local_error),before=original})
        local r=st:call('tarn_prepare',{id,1,{1},{},{{sid,0,{1},1}},{},clock.realtime()},{timeout=2})
        local same=st:eval("return box.is_in_txn() and tostring(require('ffi').C.box_txn_id())")
        check(engine..' failed child receipt barrier preserves borrowed transaction and creates no vote',
            r[1]=='expired' and r[2]==1213 and same==original and absent(id),{reply=r,before=original,after=same})
        signal(follower,18)
        local wr,we=writer:wait_result(3);assert(wr,tostring(we))
        -- Use the production 5-second barrier deadline once the actual
        -- follower has resumed. Keep the single writer attempt and the
        -- original borrowed MVCC transaction; neither is replayed/refreshed.
        for i=1,3 do ev(i,"box.cfg{replication_synchro_quorum=2,replication_synchro_timeout=5};tarn.cfg.read_freshness_timeout=5") end
        await('confirmed queue drained after follower resumes',function()
            local s=state(leader);return s.queue.len==0 and not s.queue.busy
        end)
        await('coverage after follower resumes',healthy)
        ok(rpc(leader,'tarn_autocommit',{{{'replace',sid,{1,9,1}}}}))
        local v=st:call('tarn_validate',{id,{{sid,0,{1},1}},{}})
        check(engine..' original borrowed MVCC read set still detects a later overwrite',v[1]=='conflict' and v[2]==1213,v)
        pcall(st.rollback,st)
    end
end,debug.traceback)
reset()
for _,c in ipairs(cs)do c:close()end
if not success then error(failure)end
print(passed..' passed, 0 failed')
