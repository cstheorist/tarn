-- Public CommitLocal eligibility and MVCC controls for PLAIN-source binding.
-- Only owned fixture metadata is changed. Competing metadata writes below
-- isolate storage-engine tracking; they are not complete lazy DDL protocols.
-- Production functions are observed, never bypassed or supplied forged plans.
local fio, fiber, json, mp, ffi = require('fio'), require('fiber'), require('json'), require('msgpack'), require('ffi')
io.stdout:setvbuf('no')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local work = fio.abspath(assert(arg[1], 'new private work directory required'))
assert(not fio.path.exists(work), 'fixture directory must be new')
fio.mktree(work)
local native = os.getenv('TARN_NATIVE')
if native and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
box.cfg{work_dir=work,memtx_use_mvcc_engine=true,memtx_memory=128*1024*1024,
    vinyl_memory=32*1024*1024,vinyl_cache=0,wal_mode='fsync',
    txn_isolation='read-confirmed',checkpoint_interval=0,log_level=3}
local tarn = require('tarn')
tarn.lazy.paused = true
tarn.init{shard_id=1,peers={},native=native and native ~= '0' and native or false,
    test_hooks=true,check_yields=true,recovery_after=1000000,gc_interval=1000000}
local pass, fail, serial, active, ns = 0, 0, 0
local actors = {}
local function event(label, data)
    local f = assert(io.open(fio.pathjoin(work, 'events.jsonl'), 'a'))
    f:write(json.encode{event=label,data=data}, '\n'); f:close()
end
local function check(label, yes, data)
    event(label, {ok=yes,witness=data or {}})
    if yes then pass=pass+1; print('  ok   '..label)
    else fail=fail+1; print('  FAIL '..label..' '..json.encode(data or {})) end
end
local function ok(r) assert(type(r)=='table' and r[1]=='ok',json.encode(r)); return r end
local function wait(label, fn)
    local deadline=fiber.clock()+5
    repeat if fn() then return end; fiber.sleep(.005) until fiber.clock()>=deadline
    error(label..' timed out')
end
local function ready()
    wait('clock and quota ready',function()
        return rawget(_G,'tarn')~=nil and box.space._tarn_lazy_floor~=nil
            and tarn.clock.read_check()==nil and tarn.clock.check({1})==nil and tarn.lazy.quota.ready()
    end)
    ok(tarn_autocommit({},{}))
    assert(not tarn.lazy.stale and not tarn.engine.stale)
end
local function current_id() return box.is_in_txn() and tostring(box.txn_id()) or false end
local function global_state()
    return {lazy_stale=tarn.lazy.stale,engine_stale=tarn.engine.stale,
        native_stale=ns and tonumber(ns.capture_stale) or false,
        native_excluded=ns and tostring(ns.write_excluded) or false}
end
local function definition(name, id, engine)
    return {name=name,id=id,ver=0,engine=engine,json='{}',lazy={},
        format={{name='id',type='unsigned'},{name='v',type='unsigned'},{name='bucket_id',type='unsigned'}},
        indexes={{name='primary',unique=true,parts={{1,'unsigned'}}},
                 {name='bucket_id',unique=false,parts={{3,'unsigned'}}},
                 {name='v',unique=false,parts={{2,'unsigned'}}}}}
end
local function spec(def, gen)
    return {gen=gen,base_space_id=def.id,local_index_id=2,route_parts=1,
        key_fields={2},key_types={'unsigned'},pk_fields={1},pk_native_fields={1},
        pk_types={'unsigned'},bucket_field=3,engine=def.engine}
end
local protocol={'_tarn_catalog','_tarn_part','_tarn_txn','_tarn_locks','_tarn_rlocks',
    '_tarn_lazy_capture','_tarn_lazy_defs','_tarn_lazy_outbox','_tarn_lazy_counter',
    '_tarn_lazy_version','_tarn_lazy_reservation','_tarn_lazy_slots','_tarn_lazy_floor','_tarn_lazy_usage'}
local function image(ctx)
    box.begin{txn_isolation='read-confirmed'}
    local yes, result=pcall(function()
        local side={}
        for _,d in ipairs(ctx.others) do side[#side+1]={d.id,box.space[d.id]:select{}} end
        for _,name in ipairs(protocol) do side[#side+1]={name,box.space[name]:select{}} end
        local rows=box.space[ctx.a.id]:select{}
        return {all=mp.encode({rows,side}),side=mp.encode(side),v=rows[1][2],count=#rows}
    end)
    box.rollback(); if not yes then error(result) end
    return result
end
local original={prove=assert(tarn.lazy.prove_plain),capture=tarn.lazy.capture,guard=tarn.lazy.guard}
local function observe(kind)
    if active and fiber.self()==active.fiber then
        active[kind..'_calls']=(active[kind..'_calls'] or 0)+1
        active[kind..'_id']=current_id()
        if not active.rollback_registered and box.is_in_txn() then
            active.rollback_registered=true
            local s=active
            box.on_rollback(function() s.rollbacks=s.rollbacks+1 end)
        end
    end
end
tarn.lazy.prove_plain=function(...)
    observe('proof')
    local r=original.prove(...)
    if active and fiber.self()==active.fiber then active.proof_returned=r~=nil end
    return r
end
tarn.lazy.capture=function(...) observe('capture'); return original.capture(...) end
tarn.lazy.guard=function(...) observe('guard'); return original.guard(...) end
local function start_writer(ctx, mode, second, before_proof)
    serial=serial+1
    local s={mode=mode,txid='plain-binding-'..serial,done=false,rollbacks=0,
        packet_sids={ctx.a.id},resume=fiber.channel(1),arrived=fiber.channel(1),start=fiber.channel(1)}
    if second then s.packet_sids[2]=second.id end
    s.fiber=fiber.new(function()
        local yes, err=pcall(function()
            assert(s.start:get(5)) -- publish the observation owner before any public call
            local points, fence, writes, wb={}, {}, {}, {}
            if mode=='marked' then
                local read=ok(tarn_select(ctx.history.id,0,{1},'EQ',1,box.NULL,true,false,box.NULL,true,box.NULL,{1}))
                assert(#read[2]==1)
                s.prior_value=read[2][1][2]; s.original_id=current_id()
                points={{ctx.history.id,0,{1},1}}
                s.rollback_registered=true
                box.on_rollback(function() s.rollbacks=s.rollbacks+1 end)
                if before_proof then assert(s.arrived:put(true,5)); assert(s.resume:get(5)) end
            elseif mode=='ambient' then
                box.begin{txn_isolation='read-committed'}
                s.original_id=current_id()
                box.space[ctx.history.id]:get{1}
                s.rollback_registered=true
                box.on_rollback(function() s.rollbacks=s.rollbacks+1 end)
            end
            local defs={ctx.a}; if second then defs[2]=second end
            for _,d in ipairs(defs) do
                writes[#writes+1]={'update',d.id,{1},{{'+',2,1}}}
                fence[#fence+1]=d.id; fence[#fence+1]=0; wb[#wb+1]=1
            end
            s.commit_entry_id=current_id()
            if mode=='autocommit' then s.result=tarn_autocommit(writes,{fence=fence,wb=wb})
            elseif mode=='rc' then
                local lock=ok(tarn_lock(s.txid,{{ctx.a.id,0,{1},1}},'X',{read=true,timeout=1}))
                s.result=tarn_commit_rc(s.txid,writes,lock[2],fence,wb)
            else s.result=tarn_commit_local(s.txid,writes,points,{},box.NULL,fence,wb) end
            s.transaction_remains=box.is_in_txn()
        end)
        if not yes then
            s.exception=tostring(err)
            if type(err)=='cdata' or type(err)=='table' then s.error_code=err.code end
        end
        if box.is_in_txn() then s.cleanup_rollback=true; box.rollback() end
        if mode=='rc' then pcall(tarn_unlock,s.txid) end
        s.done=true
    end)
    s.fiber:set_joinable(true); actors[#actors+1]=s; active=s
    assert(s.start:put(true,5)) -- keep the buffered channel until the child has consumed it
    return s
end
local function finish(s)
    wait('one original public commit finishes',function() return s.done end)
    assert(s.fiber:join()); s.fiber=nil; s.start=nil; s.resume=nil; s.arrived=nil; active=nil
    event('public commit outcome',s)
end
local function boundary(s, before_proof)
    local observed={done=false}
    local observer=fiber.new(function()
        local yes,value=pcall(function()
            if before_proof then return s.arrived:get(5) end
            return tarn_test_hook('write','reached')
        end)
        if not yes then value=tostring(value) end
        observed.ok,observed.value,observed.done=yes,value,true
    end)
    observer:set_joinable(true)
    local yes,err=pcall(wait,'public write boundary or actor termination',function()
        if s.done then error('public actor terminated before its requested boundary') end
        if not observed.done then return false end
        assert(observed.ok,'boundary observer failed: '..tostring(observed.value))
        if before_proof then assert(observed.value,'first read did not finish')
        else assert(type(observed.value)=='table' and observed.value[1]=='ok'
            and observed.value[2]>0,'public write hook not reached') end
        return true
    end)
    if not observed.done then observer:cancel() end
    local joined,join_error=observer:join()
    if not yes then
        local d={observer=observed,error=tostring(err),global=global_state()}
        for k,v in pairs(s) do
            if k~='fiber' and k~='start' and k~='resume' and k~='arrived' then d[k]=v end
        end
        d.fiber_status=fiber.status(s.fiber)
        event('public write boundary rejected',d)
        if s.done then finish(s) end
        error(tostring(err))
    end
    assert(joined,tostring(join_error))
end
local function aborted(s)
    return (s.result and (s.result[2]==1213 or s.result[2]==1412))
        or s.error_code==box.error.TRANSACTION_CONFLICT or s.error_code==box.error.READONLY
end
local function prepare_other(ctx)
    ctx.version=ctx.version+1
    ctx.b.ver=ctx.version
    ctx.b.format[#ctx.b.format+1]={name='c'..ctx.version,type='unsigned',is_nullable=true}
    local a={ver=ctx.version,def=ctx.b,format=ctx.b.format,builds={},drop={},renames={},
        uix={},uix_drop={},lazy_drop={},checks={},timeout=5}
    ok(tarn_alter_prepare(ctx.b.name,a,ctx.version)); ready()
    return function() ok(tarn_alter_apply(ctx.b.name,a,ctx.version)) end
end
local function exercise(ctx, label, options)
    options=options or {}
    local apply=prepare_other(ctx)
    local mode=options.mode or 'marked'
    local label=ctx.a.engine..'/'..label
    if not options.before_proof then ok(tarn_test_hook('write','hold')) end
    local s=start_writer(ctx,mode,options.second,options.before_proof)
    boundary(s,options.before_proof)
    assert(not s.done,'public commit terminated before competing action')
    if options.mutate then options.mutate() end
    apply() -- actual unrelated ALTER, never a synthetic stale flag
    assert(tarn.lazy.stale,'unrelated ALTER failed to invalidate registry')
    if options.read_only then box.cfg{read_only=true}; assert(box.info.ro) end
    local before, global_before=image(ctx),global_state()
    if options.before_proof then assert(s.resume:put(true,5))
    else ok(tarn_test_hook('write','release')) end
    finish(s)
    local after, global_after=image(ctx),global_state()
    if options.read_only then box.cfg{read_only=false}; assert(not box.info.ro) end
    if options.success then
        check(label..': one original public commit succeeds exactly once',s.result and s.result[1]=='ok'
            and s.result[2]==1 and s.rollbacks==0 and not s.transaction_remains
            and not s.cleanup_rollback and after.v==before.v+1 and after.count==1
            and after.side==before.side,s)
        check(label..': original first-read transaction reaches proof/capture/guard',
            s.original_id==s.commit_entry_id and s.proof_id==s.original_id
            and s.capture_id==s.original_id and s.guard_id==s.original_id
            and s.proof_calls==1 and s.capture_calls==1 and s.guard_calls==1,s)
        check(label..': global registry and native exclusion remain stale',
            global_before.lazy_stale and global_after.lazy_stale
            and (not ns or (global_before.native_stale==1 and global_after.native_stale==1
                and global_before.native_excluded==global_after.native_excluded)),
            {before=global_before,after=global_after})
    else
        check(label..': definite refusal, never an unknown or replayed success',aborted(s)
            and not (s.result and s.result[1]=='ok') and not s.transaction_remains,s)
        check(label..': rejected packet preserves exact selected durable image',before.all==after.all,s)
        if options.strict then
            check(label..': complete packet stays on old strict capture path',
                s.result and s.result[2]==1412 and s.result[3]=='Lazy capture registry is stale'
                and not s.proof_returned,s)
        end
        if options.before_proof then
            check(label..': overwritten earlier read cannot acquire a replacement view',
                s.original_id==s.commit_entry_id and s.rollbacks==1,s)
        end
    end
    if options.undo then options.undo() end
    ready()
end
local function deliver()
    local events=ok(tarn_lazy_control('pending'))[2]
    if #events==0 then return end
    local routes={}; for _,a in ipairs(events) do routes[#routes+1]=a[5] end
    ok(tarn_lazy_control('ack',ok(tarn_lazy_apply(events,routes))[2]))
end
local function main()
    ready(); ok(tarn_bucket_init(16,{{1,16}}))
    ready() -- bucket publication invalidates the native ownership mirror
    if tarn.cfg.native then
        ns=ffi.cast('struct tarn_ns *',box.func['tarn_storage.ts_state']:call({tarn.NS_LAYOUT}))
        assert(ns.layout==tarn.NS_LAYOUT and ns.ready==1,'requested native module must actually be ready')
    end
    event('identity',{version=box.info.version,wal=box.cfg.wal_mode,native=tarn.cfg.native~=false,
        layout=ns and tonumber(ns.layout) or false,read_path='public tarn_select/CommitLocal'})
    for n,engine in ipairs({'memtx','vinyl'}) do
        local base=98100+n*20
        local ctx={version=0,a=definition('test.plain_a_'..engine,base,engine),
            b=definition('test.plain_b_'..engine,base+1,engine),
            history=definition('test.plain_h_'..engine,base+2,engine),
            pending=definition('test.plain_p_'..engine,base+3,engine),
            captured=definition('test.plain_c_'..engine,base+4,engine)}
        ctx.others={ctx.b,ctx.history,ctx.pending,ctx.captured}
        for _,d in ipairs{ctx.a,ctx.b,ctx.history,ctx.pending} do ok(tarn_create_table(d)) end
        local active_spec=spec(ctx.captured,base+5)
        ctx.captured.lazy={active_spec}
        local ca={def=ctx.captured,timeout=5}
        ok(tarn_create_prepare(ctx.captured.name,ca,1))
        for _=1,5 do if ok(tarn_lazy_bootstrap(ctx.captured.name,ca,1))[2] then break end end
        ok(tarn_lazy_ready(ctx.captured.name,ca,1)); ok(tarn_create_apply(ctx.captured.name,ca))
        ready()
        for _,d in ipairs{ctx.a,ctx.b,ctx.history,ctx.pending,ctx.captured} do
            ok(tarn_autocommit({{'insert',d.id,{1,10,1}}},{fence={d.id,0},wb={1}}))
        end
        deliver(); ready()
        exercise(ctx,'eligible-original-stream',{success=true})
        exercise(ctx,'mixed-active-second-SID',{second=ctx.captured,strict=true})
        local pending_spec=spec(ctx.pending,base+6)
        tarn.lazy.target_space(pending_spec) -- before any proof/read set
        box.space._tarn_lazy_defs:insert{pending_spec.gen,ctx.pending.id,0,'building',1,pending_spec,box.NULL,false}
        exercise(ctx,'mixed-pending-second-SID',{second=ctx.pending,strict=true})
        box.space._tarn_lazy_defs:delete{pending_spec.gen}; box.space[pending_spec.gen]:drop(); ready()
        local catalog=box.space._tarn_catalog:get{ctx.a.name}:totable()
        exercise(ctx,'catalog-point-after-proof',{
            mutate=function()
                local changed=table.deepcopy(catalog); changed[2].ver=1
                box.space._tarn_catalog:replace(changed)
            end,
            undo=function() box.space._tarn_catalog:replace(catalog) end})
        exercise(ctx,'capture-hole-after-proof',{
            mutate=function() box.space._tarn_lazy_capture:insert{ctx.a.id,0,{}} end,
            undo=function() box.space._tarn_lazy_capture:delete{ctx.a.id} end})
        local later_spec=spec(ctx.a,base+7)
        tarn.lazy.target_space(later_spec) -- isolate descriptor insertion from native DDL
        exercise(ctx,'pending-phantom-after-proof',{
            mutate=function() box.space._tarn_lazy_defs:insert{later_spec.gen,ctx.a.id,0,'building',1,later_spec,box.NULL,false} end,
            undo=function() box.space._tarn_lazy_defs:delete{later_spec.gen}; box.space[later_spec.gen]:drop() end})
        exercise(ctx,'prior-user-read-before-proof',{before_proof=true,
            mutate=function()
                ok(tarn_autocommit({{'update',ctx.history.id,{1},{{'+',2,1}}}},
                    {fence={ctx.history.id,0},wb={1}}))
            end})
        for _,mode in ipairs{'local','autocommit','rc'} do exercise(ctx,mode..'-ineligible',{mode=mode,strict=true}) end
        -- A same-source fence remains authoritative even for a marked stream.
        local fenced=table.deepcopy(ctx.a); fenced.ver=1
        local fa={ver=1,def=fenced,builds={},drop={},renames={},uix={},uix_drop={},lazy_drop={},checks={},timeout=5}
        ok(tarn_alter_prepare(ctx.a.name,fa,1)); ready()
        local before=image(ctx); local s=start_writer(ctx,'marked'); finish(s)
        check(engine..'/same-source-fence: marked commit cannot bypass version refusal',
            s.result and s.result[2]==1412 and not s.proof_returned and before.all==image(ctx).all,s)
        ok(tarn_alter_abort(ctx.a.name,fa,1)); ready()
        -- Public CommitLocal must refuse an unmarked ambient RW transaction.
        before=image(ctx); s=start_writer(ctx,'ambient'); finish(s)
        check(engine..'/unmarked-ambient: no late mark or transaction replacement',
            aborted(s) and not s.proof_returned and before.all==image(ctx).all,s)
        exercise(ctx,'owned-RF1-read-only-fault',{read_only=true})
        exercise(ctx,'post-fault-original-stream',{success=true})
    end
end
local good,err=xpcall(main,debug.traceback)
pcall(tarn_test_hook,'write','release')
if box.info.ro then pcall(box.cfg,{read_only=false}) end
for _,s in ipairs(actors) do
    if s.resume then pcall(s.resume.put,s.resume,true,0) end
end
tarn.lazy.prove_plain,tarn.lazy.capture,tarn.lazy.guard=original.prove,original.capture,original.guard
if not good then fail=fail+1; event('fixture exception',tostring(err)); print('  FAIL fixture exception '..tostring(err)) end
print(string.format('%d passed, %d failed',pass,fail))
os.exit(fail==0 and 0 or 1)
