-- Primitive gate for a proposed PLAIN-source certificate, not a production
-- cache bypass. Metadata is read in the exact original public transaction,
-- before its initial row image; its actual memtx/Vinyl write must still
-- validate point holes, filtered full-scan phantoms and prior user reads.
-- Unrelated B uses real ALTER PREPARE/APPLY. No stale flag is cleared and
-- no caller transaction is restarted. A failed desired oracle rejects the
-- proposed primitive; it is not permission to weaken the production guard.
local fio, fiber, json, mp = require('fio'), require('fiber'), require('json'), require('msgpack')
io.stdout:setvbuf('no')
local source = fio.abspath(debug.getinfo(1, 'S').source:sub(2))
local here = assert(source:match('(.*/)'))
package.path = here .. '../?.lua;' .. package.path
local work = fio.abspath(assert(arg[1], 'new private work directory required'))
local window = arg[2] or 'after-publish'
assert(window=='after-publish' or window=='during-publish', 'unknown ALTER window')
local native = os.getenv('TARN_NATIVE')
if native and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
assert(fio.stat(work)==nil, 'primitive fixture must be new')
fio.mktree(work)
box.cfg{work_dir=work,memtx_use_mvcc_engine=true,memtx_memory=128*1024*1024,
    vinyl_memory=32*1024*1024,vinyl_cache=0,wal_mode='fsync',
    txn_isolation='read-confirmed',checkpoint_interval=0,log_level=3}
local tarn = require('tarn')
tarn.lazy.paused = true
tarn.init{shard_id=1,peers={},test_hooks=true,check_yields=true,recovery_after=1000000,
    gc_interval=1000000,native=native and native ~= '0' and native or false}
local pass, fail = 0, 0
local function event(label, data)
    local f = assert(io.open(fio.pathjoin(work, 'events.jsonl'), 'a'))
    f:write(json.encode{event=label,window=window,data=data}, '\n');f:close()
end
local function check(label, yes, data)
    event(label, {ok=yes,witness=data})
    if yes then pass=pass+1;print('  ok   '..label)
    else fail=fail+1;print('  FAIL '..label..' '..json.encode(data)) end
end
local function ok(r) assert(type(r)=='table' and r[1]=='ok',json.encode(r));return r end
local function wait(label, fn)
    local deadline=fiber.clock()+5
    repeat if fn() then return end;fiber.sleep(.005) until fiber.clock()>=deadline
    error(label..' timed out')
end
local function ready()
    wait('actual local readiness',function()
        return box.space._tarn_part~=nil and tarn.clock.read_check()==nil and tarn.lazy.quota.ready()
    end)
    ok(tarn_autocommit({},{}))
end
local function definition(name, id, engine)
    return {name=name,id=id,ver=0,engine=engine,json='{}',lazy={},
        format={{name='id',type='unsigned'},{name='v',type='unsigned'},{name='bucket_id',type='unsigned'}},
        indexes={{name='primary',unique=true,parts={{1,'unsigned'}}},
                 {name='bucket_id',unique=false,parts={{3,'unsigned'}}},
                 {name='by_v',unique=false,parts={{2,'unsigned'}}}}}
end
local function rows(sp)
    box.begin{txn_isolation='read-confirmed'}
    local result=mp.encode(sp:select{})
    box.rollback()
    return result
end
local function proof(def)
    assert(box.is_in_txn(), 'absence proof needs the original transaction')
    local before=tostring(box.txn_id())
    local catalog=box.space._tarn_catalog:get{def.name}
    assert(catalog and catalog.def.id==def.id and (catalog.def.ver or 0)==def.ver)
    assert(catalog.def.lazy==nil or next(catalog.def.lazy)==nil, 'source declares lazy capture')
    local capture=box.space._tarn_lazy_capture:get{def.id}
    assert(capture==nil or #capture.specs==0, 'source has active capture')
    local seen, relevant=0,0
    -- There is no base_id index. This FULL scan deliberately uses real
    -- Tarantool trackers; unrelated lazy metadata may conservatively abort
    -- the caller. Ordinary non-lazy ALTER B changes none of these rows.
    for _, d in box.space._tarn_lazy_defs:pairs() do
        seen=seen+1
        if d.base_id==def.id and d.phase~='retired' then relevant=relevant+1 end
    end
    assert(relevant==0, 'source has pending capture')
    assert(tostring(box.txn_id())==before)
    return {id=before,pending_rows_read=seen,relevant_pending=relevant,capture_absent=capture==nil}
end
local b=definition('test.metadata_other',97520,'vinyl')
local b_version=0
local function prepare_other()
    b_version=b_version+1;b.ver=b_version
    b.format[#b.format+1]={name='c'..b_version,type='unsigned',is_nullable=true}
    local a={ver=b_version,def=b,format=b.format,builds={},drop={},renames={},
        uix={},uix_drop={},lazy_drop={},checks={},timeout=5}
    ok(tarn_alter_prepare(b.name,a,b_version))
    ready()
    return function()
        if window=='after-publish' then
            ok(tarn_alter_apply(b.name,a,b_version))
            check('unrelated ALTER published before original writer resumes',
                box.space._tarn_catalog:get{b.name}.def.ver==b_version,{version=b_version})
            return
        end
        ok(tarn_test_hook('alter_schema','hold'))
        local ddl={done=false}
        local f=fiber.new(function()
            ddl.good,ddl.result=pcall(tarn_alter_apply,b.name,a,b_version)
            ddl.done=true
        end)
        f:set_joinable(true)
        assert(tarn_test_hook('alter_schema','reached')[2]==1, 'ALTER did not reach native-schema boundary')
        assert(not ddl.done and tarn.lazy.stale, 'ALTER was not held after actual cache invalidation')
        return function()
            check('same unrelated ALTER remains held through original writer termination',not ddl.done,{version=b_version})
            ok(tarn_test_hook('alter_schema','release'))
            wait('same unrelated ALTER publication',function() return ddl.done end)
            assert(f:join());assert(ddl.good,tostring(ddl.result));ok(ddl.result)
            check('same unrelated ALTER resumes and publishes without replay',
                box.space._tarn_catalog:get{b.name}.def.ver==b_version,{version=b_version})
        end
    end
end
local history
local function run_case(def, isolation, point, label, mutate, undo, want_commit)
    ready()
    local sp=assert(box.space[def.id])
    local original_space=sp
    local before=rows(sp)
    local before_v=sp:get{1}[2]
    local arrived, resume=fiber.channel(1),fiber.channel(1)
    local s={label=label,engine=def.engine,isolation=isolation,point=point}
    local f=fiber.new(function()
        local yes, err=pcall(function()
            box.begin{txn_isolation=isolation}
            s.original_id=tostring(box.txn_id())
            s.prior_history=history:get{1}[2]
            s.proof=proof(def)
            local initial=assert(sp:get{1})
            s.initial_id=tostring(box.txn_id());s.initial=initial:totable()
            if point=='after-write' then sp:replace{1,initial[2]+1,1} end
            assert(arrived:put(true,5));assert(resume:get(5))
            s.resumed_id=tostring(box.txn_id())
            assert(box.space[def.id]==original_space,'source space identity changed')
            if point=='before-write' then sp:replace{1,initial[2]+1,1} end
            s.commit_id=tostring(box.txn_id())
            box.commit();s.committed=true
        end)
        if not yes then
            s.error=tostring(err)
            if type(err)=='cdata' or type(err)=='table' then s.error_code=err.code end
        end
        if box.is_in_txn() then box.rollback();s.cleanup_rollback=true end
        s.done=true
    end)
    f:set_joinable(true)
    assert(arrived:get(5),'writer did not reach primitive boundary')
    local complete=mutate()
    s.other_ddl_pending=type(complete)=='function'
    s.capture_stale_while_held=tarn.lazy.stale
    s.engine_stale_while_held=tarn.engine.stale
    event('mutation completed while original transaction held',s)
    assert(resume:put(true,5))
    wait('primitive writer termination',function() return s.done end)
    assert(f:join())
    local after=rows(sp)
    local after_v=sp:get{1}[2]
    check(label..': original transaction/read history retained',
        s.proof.id==s.original_id and s.initial_id==s.original_id and s.resumed_id==s.original_id
        and s.prior_history~=nil,s)
    if want_commit then
        check(label..': real unrelated ALTER permits the original commit',s.committed==true,s)
        check(label..': exactly one acknowledged increment',s.committed==true
            and after_v==before_v+1 and sp:count()==1,s)
    else
        check(label..': relevant metadata/history forces a conflict',not s.committed
            and s.error_code==box.error.TRANSACTION_CONFLICT,s)
        check(label..': rejected write leaves exact source unchanged',not s.committed and before==after,s)
    end
    if complete then complete() end
    if undo then undo() end
end
local good, err=xpcall(function()
    ready();ok(tarn_bucket_init(16,{{1,16}}));ok(tarn_create_table(b))
    history=box.schema.space.create('metadata_prior_history')
    history:create_index('pk');history:insert{1,0}
    -- A stable, unrelated REAL pending descriptor ensures the full scan
    -- filters by source rather than assuming that the entire table is empty.
    local c=definition('test.metadata_stable_pending',97530,'memtx')
    ok(tarn_create_table(c))
    c.lazy={{gen=97630,base_space_id=c.id,local_index_id=2,route_parts=1,
        key_fields={2},key_types={'unsigned'},pk_fields={1},pk_native_fields={1},
        pk_types={'unsigned'},bucket_field=3}}
    tarn.lazy.prepare(c,1)
    for n,engine in ipairs({'memtx','vinyl'}) do
        local def=definition('test.metadata_'..engine,97500+n,engine)
        ok(tarn_create_table(def));ok(tarn_autocommit({{'insert',def.id,{1,10,1}}},{fence={def.id,0},wb={1}}))
        for _,isolation in ipairs({'read-confirmed','read-committed'}) do
            local prefix=engine..'/'..isolation
            for _,point in ipairs({'before-write','after-write'}) do
                local apply=prepare_other()
                run_case(def,isolation,point,prefix..'/'..point..'/unrelated-ALTER',apply,nil,true)
            end
            local catalog=box.space._tarn_catalog:get{def.name}:totable()
            run_case(def,isolation,'after-write',prefix..'/catalog-point',function()
                local changed=table.deepcopy(catalog);changed[2].ver=changed[2].ver+1
                box.space._tarn_catalog:replace(changed)
            end,function() box.space._tarn_catalog:replace(catalog) end,false)
            run_case(def,isolation,'after-write',prefix..'/capture-hole',function()
                box.space._tarn_lazy_capture:insert{def.id,def.ver,{}}
            end,function() box.space._tarn_lazy_capture:delete{def.id} end,false)
            local gen=97600+n
            local spec={gen=gen,base_space_id=def.id,local_index_id=2,route_parts=1,
                key_fields={2},key_types={'unsigned'},pk_fields={1},pk_native_fields={1},
                pk_types={'unsigned'},bucket_field=3,engine=engine}
            -- Create the owned target BEFORE binding. The only competing
            -- action afterwards is the actual pending-descriptor insertion,
            -- so an unrelated native-schema side effect cannot mask it.
            tarn.lazy.target_space(spec)
            run_case(def,isolation,'after-write',prefix..'/pending-phantom',function()
                box.space._tarn_lazy_defs:insert{gen,def.id,def.ver,'building',1,spec,box.NULL,false}
            end,function()
                box.space._tarn_lazy_defs:delete{gen};box.space[gen]:drop()
            end,false)
            run_case(def,isolation,'after-write',prefix..'/prior-user-read',function()
                history:update({1},{{'+',2,1}})
            end,nil,false)
        end
    end
end,debug.traceback)
pcall(tarn_test_hook,'alter_schema','release')
if not good then fail=fail+1;event('primitive exception',tostring(err));print('  FAIL primitive exception '..tostring(err)) end
print(string.format('%d passed, %d failed',pass,fail))
os.exit(fail==0 and 0 or 1)
