-- RF1/fsync mechanism witness, not RF3: one BUILD loses its cut between
-- reset's confirmed copying receipt and its first shadow transaction, or
-- after the original confirmed source-page read returns before that write.
-- Only the actual initial box.status callback and the owned BUILD fiber
-- are scheduled. No invalidation, status payload, page or receipt is forged.
local fio, fiber, json, mp, digest = require('fio'), require('fiber'), require('json'),
    require('msgpack'), require('digest')
io.stdout:setvbuf('no')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local work = fio.abspath(assert(arg[1], 'NEW_WORKDIR memtx|vinyl baseline|desired [post-reset|after-baseline]'))
local engine, expected = assert(arg[2]), assert(arg[3])
local boundary = arg[4] or 'post-reset'
assert(engine == 'memtx' or engine == 'vinyl', 'unknown source engine')
assert(expected == 'baseline' or expected == 'desired', 'unknown expected outcome')
assert(boundary == 'post-reset' or boundary == 'after-baseline', 'unknown callback boundary')
assert(not fio.path.exists(work), 'private fixture directory must be new')
fio.mktree(work)
local native = os.getenv('TARN_NATIVE')
if native and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
box.cfg{work_dir=work,listen='unix/:'..fio.pathjoin(work,'iproto.sock'),
    memtx_use_mvcc_engine=true,memtx_memory=128*1048576,
    vinyl_memory=32*1048576,vinyl_cache=0,wal_mode='fsync',checkpoint_interval=0,
    txn_isolation='read-confirmed',election_mode='off',replication_synchro_quorum=1,
    replication_synchro_timeout=.5,log_level=3}
box.ctl.promote()
local tarn = require('tarn')
tarn.lazy.paused = true
local E, sid, target, name, count = tarn.engine, 90900, 90901, 'test.engine_cut', 260
local passed, worker = 0
local callback = {registered=0,delivered=false,done=false,released=false,resume=fiber.channel(1)}
local original_watch, original_fresh, original_commit, original_rollback = box.watch, E.fresh, box.commit, box.rollback
local function event(label, data)
    local f = assert(io.open(fio.pathjoin(work, 'events.jsonl'), 'a'))
    f:write(json.encode{event=label,engine=engine,expected=expected,boundary=boundary,data=data or {}}, '\n'); f:close()
end
local function check(label, yes, data)
    event(label, {ok=yes,witness=data or {}})
    assert(yes, label .. ': ' .. json.encode(data or {}))
    passed=passed+1; print('  ok   ' .. label)
end
local function wait(label, fn, diagnostic)
    local deadline=fiber.clock()+5
    repeat if fn() then return end; fiber.sleep(.005) until fiber.clock()>=deadline
    if diagnostic then
        local yes,data=pcall(diagnostic)
        if not yes then data={error=tostring(data)} end
        event('startup readiness timeout',data)
        io.stderr:write('ENGINE_CUT_STARTUP_DIAGNOSTIC ',json.encode(data),'\n')
    end
    error(label .. ' timed out')
end
local function ok(r) assert(type(r)=='table' and r[1]=='ok',json.encode(r)); return r end
local function confirmed(fn)
    box.begin{txn_isolation='read-confirmed'}
    local yes,r=pcall(fn)
    if box.is_in_txn() then box.rollback() end
    if not yes then error(r) end
    return r
end
local function receipt()
    return confirmed(function()
        local r=box.space._tarn_meta:get{'engine:'..sid}
        return r and table.deepcopy(r.value) or false
    end)
end
local function rows(id)
    return confirmed(function()
        local out={}
        if box.space[id] then
            for _,t in box.space[id].index[0]:pairs() do out[#out+1]=t:totable() end
        end
        return out
    end)
end
local function volatile()
    local t,q=E.tracks[sid],E.tickets
    return {ro=box.info.ro,term=box.info.election.term,clock_epoch=tarn.clock.epoch,
        epoch=E.epoch,capture_epoch=E.capture_epoch,stale=E.stale,
        track=t and {valid=t.valid,epoch=t.epoch,token=t.token,keys=#t.list} or false,
        tickets={epoch=q.epoch,ready=q.ready(),outstanding=q.outstanding,building=q.building},
        queue=box.info.synchro.queue}
end
local function startup_state()
    local schema={}
    for _,name in ipairs{'_tarn_meta','_tarn_buckets','_tarn_catalog','_tarn_part',
        '_tarn_txn','_tarn_scratch','_tarn_lazy_floor','_tarn_lazy_usage'} do
        local sp=box.space[name]
        schema[name]={exists=sp~=nil,primary=sp~=nil and sp.index[0]~=nil,
            state_index=sp~=nil and sp.index.state~=nil,
            bucket_index=sp~=nil and sp.index.bucket_id~=nil}
    end
    local function observed(fn)
        local yes,r=pcall(fn)
        return {ok=yes,allowed=yes and r==nil,result=r==nil and box.NULL
            or type(r)=='cdata' and tostring(r) or r}
    end
    local meta=confirmed(function()
        local sp=box.space._tarn_meta
        local function value(key)
            local t=sp and sp.index[0] and sp:get{key}
            return t and t.value or box.NULL
        end
        local usage=box.space._tarn_lazy_usage
        local row=usage and usage.index[0] and usage:get{0}
        return {shard_id=value('shard_id'),clock_policy=value('clock:policy:1'),
            usage0=row and row:totable() or box.NULL}
    end)
    local read=observed(tarn.clock.read_check)
    local admission=observed(function() return tarn.clock.check({1}) end)
    local quota=tarn.lazy.quota.ready()
    return {schema=schema,meta=meta,clock_status=tarn.clock.status(),read_check=read,clock_check=admission,
        quota_ready=quota,quota_snapshot=tarn.lazy.quota.snapshot() or box.NULL,
        predicates={initial_status_delivered=callback.delivered,scratch_primary=schema._tarn_scratch.primary,
            lazy_floor_bucket=schema._tarn_lazy_floor.bucket_index,meta_primary=schema._tarn_meta.primary,
            shard_id_present=meta.shard_id~=box.NULL,usage0_present=meta.usage0~=box.NULL,
            quota_ready=quota,clock_read_allowed=read.allowed,clock_admission_allowed=admission.allowed},
        callback={registered=callback.registered,delivered=callback.delivered,done=callback.done},
        ro=box.info.ro,term=box.info.election.term,queue=box.info.synchro.queue,
        listen=box.cfg.listen,replication=box.cfg.replication,peers=tarn.cfg.peers}
end
-- Register the real watcher normally. Hold only its actual first delivery;
-- later deliveries call the production callback without interference.
box.watch=function(key,fn,...)
    local info=debug.getinfo(fn,'S')
    if key~='box.status' or not info.source:match('[/\\]tarn%.lua$') then
        return original_watch(key,fn,...)
    end
    callback.registered=callback.registered+1
    callback.source,callback.line=info.source,info.linedefined
    return original_watch(key,function(...)
        if callback.delivered then return fn(...) end
        callback.delivered=true;callback.payload={...}
        callback.resume:get() -- released by this fixture or its cleanup
        callback.before=volatile()
        local yes,err=pcall(fn,...)
        callback.after=volatile();callback.ok=yes
        if not yes then callback.error=tostring(err) end
        callback.done=true
        if not yes then error(err) end
    end,...)
end

local function main()
    tarn.init{shard_id=1,peers={},sync=true,native=native and native~='0' and native or false,
        test_hooks=true,check_yields=true,check_locks=true,recovery_after=1000000,gc_interval=1000000}
    box.watch=original_watch
    wait('complete schema, clock policy and actual initial status delivery',function()
        if not callback.delivered or box.space._tarn_scratch==nil
            or box.space._tarn_scratch.index[0]==nil or box.space._tarn_lazy_floor==nil
            or box.space._tarn_lazy_floor.index.bucket_id==nil or box.space._tarn_lazy_usage==nil
            or box.space._tarn_meta==nil then return false end
        return box.space._tarn_meta:get{'shard_id'}~=nil
            and box.space._tarn_lazy_usage:get{0}~=nil and tarn.lazy.quota.ready()
            and tarn.clock.read_check()==nil and tarn.clock.check({1})==nil
    end,startup_state)
    wait('configured native readiness',function()
        return not native or native=='0' or (tarn_info().native or {}).ready==1
    end)
    local info=tarn_info()
    if native and native~='0' then
        local ffi=require('ffi')
        local ns=ffi.cast('struct tarn_ns *',box.func['tarn_storage.ts_state']:call())
        check('actual native module is ready',info.native and info.native.ready==1
            and ns.layout==14 and ffi.sizeof('struct tarn_ns')==392
            and ffi.offsetof('struct tarn_ns','fallbacks')==304,info.native)
    else check('explicit Lua-only mode',tarn.cfg.native==false and info.native==false) end
    check('RF1 fsync watcher is the actual held Tarn registration',callback.registered==1
        and callback.delivered and not callback.done and callback.source:match('[/\\]tarn%.lua$')~=nil
        and tarn.cfg.sync and box.cfg.wal_mode=='fsync' and box.cfg.replication_synchro_quorum==1,
        {source=callback.source,line=callback.line,payload=callback.payload})
    ok(tarn_bucket_init(16,{{1,16}}))
    local def={id=sid,name=name,engine=engine,ver=0,json='{}',lazy={},
        format={{'id','unsigned'},{'v','integer'},{'bucket_id','unsigned'}},
        indexes={{name='primary',unique=true,parts={{1,'unsigned'}}},
            {name='bucket_id',unique=false,parts={{3,'unsigned'}}}}}
    ok(tarn_create_table(def))
    local writes={}
    for n=1,count do writes[#writes+1]={'insert',sid,{n,n*10,1}} end
    ok(tarn_autocommit(writes,{fence={sid,0},wb={1}}))
    local source_bytes=mp.encode(rows(sid))
    local expected_source_bytes=source_bytes
    local nd=table.deepcopy(def);nd.id,nd.engine,nd.ver=target,engine=='memtx' and 'vinyl' or 'memtx',5
    local a={ver=5,timeout=5,engine_change={old_id=sid,old_ver=0,old_engine=engine,old_name=name},
        def=nd,uix={},uix_drop={},lazy_drop={}}
    local owner='engine-cut-'..engine
    ok(tarn_job_begin({owner=owner,kind='alter',space=name,names={name},ver=5,a=a},0))
    worker={calls=0,done=false,boundary=false,resume=fiber.channel(1),start=fiber.channel(1),commits={},read_returns={}}
    E.fresh=function(...)
        local result=original_fresh(...)
        if fiber.self()==worker.fiber and not worker.post_reset and not box.is_in_txn() then
            local r=receipt();local t=E.tracks[sid]
            if r and r.phase=='copying' and r.copied==0 and (r.cursor==nil or r.cursor==box.NULL)
                and t and t.valid and t.epoch==E.capture_epoch then
                worker.post_reset=true
                if boundary=='post-reset' then
                    worker.before=volatile();worker.receipt=r;worker.boundary=true
                    assert(worker.resume:get(5),'owned BUILD boundary was not released')
                end
            end
        end
        return result
    end
    box.rollback=function(...)
        local result=original_rollback(...)
        if boundary=='after-baseline' and fiber.self()==worker.fiber and worker.post_reset
            and not worker.boundary and not worker.inspect and not box.is_in_txn() then
            -- Read-only frame inspection identifies the completed source
            -- read, not the ordinal number of rollbacks. The caller must be
            -- E.read, its original fn must close over this exact descriptor,
            -- and its untouched result must be a full tuple page + PK cursor.
            local fn,page
            local caller=debug.getinfo(2,'S')
            for i=1,32 do
                local key,value=debug.getlocal(2,i)
                if key==nil then break end
                if key=='fn' then fn=value elseif key=='result' then page=value end
            end
            local binding=false
            if type(fn)=='function' then
                for i=1,32 do
                    local key,value=debug.getupvalue(fn,i)
                    if key==nil then break end
                    if key=='a' and value==a then binding=true end
                end
            end
            if #worker.read_returns<8 then
                local info=type(fn)=='function' and debug.getinfo(fn,'S') or nil
                worker.read_returns[#worker.read_returns+1]={caller=caller and caller.source,
                    fn_source=info and info.source,fn_line=info and info.linedefined,binding=binding,
                    result_type=type(page),result_length=type(page)=='table' and #page or false}
            end
            if caller and caller.source==debug.getinfo(original_fresh,'S').source
                and binding and type(page)=='table' and #page==2 and type(page[1])=='table'
                and #page[1]==128 and box.tuple.is(page[1][1]) and type(page[2])=='table' then
                worker.inspect=true
                local r=receipt();local t=E.tracks[sid]
                assert(r.phase=='copying' and r.copied==0 and (r.cursor==nil or r.cursor==box.NULL)
                    and t and t.valid and t.epoch==E.capture_epoch,'source page lost its original capture before hold')
                local captured={}
                for i,tuple in ipairs(page[1]) do captured[i]=tuple:totable() end
                local info=debug.getinfo(fn,'S')
                worker.page={source=info.source,line=info.linedefined,lastline=info.lastlinedefined,
                    old_id=a.engine_change.old_id,new_id=a.def.id,ver=a.ver,
                    rows=captured,cursor=table.deepcopy(page[2]),original_read_returned=true}
                worker.before=volatile();worker.receipt=r;worker.boundary=true
                assert(worker.resume:get(5),'owned source-page boundary was not released')
                worker.inspect=false
            end
        end
        return result
    end
    -- Read-only, owned-fiber terminal observation: call the original commit
    -- once, retaining the real native terminal callbacks, never an RPC retry.
    box.commit=function(...)
        if fiber.self()~=worker.fiber then return original_commit(...) end
        if E.tickets.outstanding==0 then return original_commit(...) end
        local c={txid=tostring(box.txn_id()),terminal='pending',before=E.tickets.outstanding}
        worker.commits[#worker.commits+1]=c
        box.on_commit(function() c.terminal='commit' end)
        box.on_rollback(function() c.terminal='rollback' end)
        local yes,result=pcall(original_commit,...)
        c.ok,c.after=yes,E.tickets.outstanding
        if not yes then error(result) end
        return result
    end
    if expected=='desired' then ok(tarn_test_hook('engine_copy','hold')) end
    worker.fiber=fiber.new(function()
        assert(worker.start:get(5)) -- publish the owned fiber before BUILD
        worker.calls=worker.calls+1
        worker.ok,worker.result=pcall(tarn_alter_build,name,a,1)
        if box.is_in_txn() then worker.left_transaction=true;box.rollback() end
        worker.done=true
    end)
    worker.fiber:set_joinable(true);assert(worker.start:put(true,5))
    wait('owned BUILD reaches '..boundary..' boundary',function() return worker.boundary or worker.done end)
    check('one BUILD is held at confirmed copying0 outside a transaction',worker.boundary and not worker.done
        and worker.calls==1 and worker.before.tickets.ready and worker.before.tickets.outstanding==0
        and mp.encode(rows(sid))==source_bytes and #rows(target)==0,
        {before=worker.before,result=worker.result,receipt=worker.receipt,page=worker.page})
    if boundary=='after-baseline' then
        local first={};for n=1,128 do first[n]={n,n*10,1} end
        check('held page is the original confirmed source read with its exact PK cursor',worker.page
            and worker.page.original_read_returned and worker.page.old_id==sid and worker.page.new_id==target
            and mp.encode(worker.page.rows)==mp.encode(first) and mp.encode(worker.page.cursor)==mp.encode({128}),
            worker.page)
    end
    callback.released=true;assert(callback.resume:put(true,0))
    wait('real status callback returns',function() return callback.done end)
    event('real status delivery',{source=callback.source,line=callback.line,payload=callback.payload,
        before=callback.before,after=callback.after,ok=callback.ok,error=callback.error})
    local before,after=callback.before,callback.after
    check('real callback invalidates capture and cut with unchanged observed role',callback.ok
        and not before.ro and after.ro==before.ro and after.term==before.term
        and after.clock_epoch==before.clock_epoch and after.capture_epoch==before.capture_epoch+1
        and after.track and not after.track.valid and after.track.epoch<after.capture_epoch
        and after.tickets.epoch==before.tickets.epoch+1 and not after.tickets.ready
        and after.tickets.outstanding==0,{before=before,after=after})
    check('callback preserves exact durable source and copying0 shadow image',mp.encode(rows(sid))==source_bytes
        and #rows(target)==0 and receipt().phase=='copying' and receipt().copied==0,
        {source_sha256=digest.sha256_hex(source_bytes),receipt=receipt()})
    if boundary=='after-baseline' then
        ok(tarn_autocommit({{'replace',sid,{1,801,1}},{'delete',sid,{2}}},{fence={sid,0},wb={1}}))
        expected_source_bytes=mp.encode(rows(sid))
        check('acknowledged gap writes change the source while old capture stays invalid',
            box.space[sid]:get{1}[2]==801 and box.space[sid]:get{2}==nil
            and not E.tracks[sid].valid and #rows(target)==0
            and worker.page.rows[1][2]==10 and worker.page.rows[2][1]==2
            and expected_source_bytes~=source_bytes,{source_sha256=digest.sha256_hex(expected_source_bytes)})
    end
    local first,current={},rows(sid)
    for n=1,math.min(128,#current) do first[n]=current[n] end
    assert(worker.resume:put(true,0))
    if expected=='desired' then
        wait('original BUILD reaches a real copied page or terminal outcome',function()
            return tarn_test_hook('engine_copy','reached')[2]>0 or worker.done
        end)
        local r,t=receipt(),E.tracks[sid]
        check('same BUILD recaptures before writing one exact first page',not worker.done and worker.calls==1
            and r.copied==128 and t and t.valid and t.epoch==E.capture_epoch
            and t.token~=worker.receipt.capture and mp.encode(rows(target))==mp.encode(first),
            {receipt=r,result=worker.result,commits=worker.commits})
        ok(tarn_autocommit({{'replace',sid,{1,901,1}},{'delete',sid,{2}},
            {'insert',sid,{0,777,1}}},{fence={sid,0},wb={1}}))
        box.begin{txn_isolation='read-confirmed'};box.space[sid]:insert{999,999,1};box.rollback()
        local final=mp.encode(rows(sid))
        ok(tarn_test_hook('engine_copy','release'))
        wait('one original BUILD terminates',function() return worker.done end)
        check('original BUILD succeeds once without trusting the lost capture',worker.ok and worker.result[1]=='ok'
            and worker.calls==1 and receipt().phase=='copied',worker.result)
        ok(tarn_alter_prepare(name,a,1))
        check('confirmed PREPARE reconciles acknowledged writes and rollback extras exactly',
            receipt().phase=='prepared' and mp.encode(rows(target))==final
            and box.space[target]:get{999}==nil and E.tickets.outstanding==0)
        ok(tarn_job_phase(owner,'apply'));ok(tarn_alter_apply(name,a,1));ok(tarn_job_end(owner))
        check('confirmed APPLY publishes the exact physical image and finishes cleanup',box.space[sid]==nil
            and box.space[name].id==target and box.space[name].engine==nd.engine
            and mp.encode(rows(target))==final and receipt().cleaned==true
            and box.space._tarn_meta:get{'hold:'..sid}==nil
            and box.space._tarn_meta:get{'hold:'..target}==nil and E.tickets.outstanding==0)
    else
        wait('baseline original BUILD returns',function() return worker.done end)
        check('baseline is the exact completed pre-effect cut refusal',worker.ok and worker.calls==1
            and worker.result[1]=='error' and worker.result[2]==1213
            and worker.result[3]=='Engine-change transaction cut is rebuilding',worker.result)
        check('baseline retains exact source and a known empty copying receipt',mp.encode(rows(sid))==expected_source_bytes
            and #rows(target)==0 and receipt().phase=='copying' and receipt().copied==0
            and box.space._tarn_catalog:get{name}.def.id==sid and not worker.left_transaction)
    end
    assert(worker.fiber:join());worker.joined=true
    local terminals=true
    for _,c in ipairs(worker.commits) do
        terminals=terminals and c.ok and c.terminal=='commit' and c.before>0 and c.after==0
    end
    check('all observed BUILD writes reached actual transaction terminals',#worker.commits>0 and terminals
        and E.tickets.outstanding==0 and box.info.synchro.queue.len==0 and not box.info.synchro.queue.busy,
        {calls=worker.calls,commits=worker.commits,volatile=volatile()})
end

local success,err=pcall(main)
box.watch=original_watch;E.fresh=original_fresh;box.commit=original_commit;box.rollback=original_rollback
if not callback.released then callback.released=true;callback.resume:put(true,0) end
if worker and worker.fiber then
    worker.resume:put(true,0)
    pcall(tarn_test_hook,'engine_copy','release')
    if not worker.done then worker.fiber:cancel() end
    if worker.done and not worker.joined then pcall(worker.fiber.join,worker.fiber) end
end
if not success then
    event('fixture failure',{error=tostring(err),callback_done=callback.done,
        calls=worker and worker.calls or 0,result=worker and worker.result or false,
        read_returns=worker and worker.read_returns or {}})
    io.stderr:write('FAIL engine cut ',engine,' ',expected,' ',boundary,': ',tostring(err),'\n')
end
print(string.format('%d passed, %d failed (RF1 %s %s %s)',passed,success and 0 or 1,engine,expected,boundary))
os.exit(success and 0 or 1)
