-- A real unrelated ALTER publication between a public CommitLocal's
-- yielding work and final checks. No synthetic stale flag or cache clear.
-- Default "success" is the desired regression oracle (fails on baseline).
-- "stale" records the baseline's definite 1412/rollback mechanism instead.
local fio, fiber, json, mp = require('fio'), require('fiber'), require('json'), require('msgpack')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local work = fio.abspath(assert(arg[1], 'private work directory required'))
local expected = arg[2] or 'success'
assert(expected == 'success' or expected == 'stale')
local window = arg[3] or 'after-publish'
assert(window == 'after-publish' or window == 'during-publish')
local native = os.getenv('TARN_NATIVE')
if native ~= nil and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
fio.mktree(work)
box.cfg{work_dir=work,memtx_use_mvcc_engine=true,memtx_memory=128*1024*1024,
    vinyl_memory=32*1024*1024,vinyl_cache=0,wal_mode='fsync',
    txn_isolation='read-confirmed',log_level=3}
local tarn = require('tarn').init{shard_id=1,peers={},test_hooks=true,check_yields=true,
    native=native ~= nil and native ~= '0' and native or false}
local pass, fail = 0, 0
local function event(label, value)
    local f=assert(io.open(fio.pathjoin(work,'events.jsonl'),'a'))
    f:write(json.encode{event=label,expected=expected,window=window,data=value},'\n');f:close()
end
local function check(label, yes, value)
    event(label,{ok=yes,witness=value or {}})
    if yes then pass=pass+1; print('  ok   '..label)
    else fail=fail+1; print('  FAIL '..label..' '..json.encode(value or {})) end
end
local function ok(value)
    assert(type(value)=='table' and value[1]=='ok',json.encode(value));return value
end
local function wait(label, fn, seconds)
    local until_time=fiber.clock()+(seconds or 5)
    repeat
        local value=fn()
        if value then return value end
        fiber.sleep(.01)
    until fiber.clock()>=until_time
    error(label..' deadline')
end
local a_id, b_id, a_name, b_name = 97300, 97301, 'test.registry_a', 'test.registry_b'
local function definition(name, id)
    return {name=name,id=id,ver=0,engine='vinyl',json='{}',lazy={},
        format={{name='id',type='unsigned'},{name='v',type='unsigned'},{name='bucket_id',type='unsigned'}},
        indexes={{name='primary',unique=true,parts={{1,'unsigned'}}},
                 {name='bucket_id',unique=false,parts={{3,'unsigned'}}}}}
end
local b_def=definition(b_name,b_id)
local function ready()
    wait('local storage clock/quota readiness',function()
        return box.space._tarn_part~=nil and tarn.clock.read_check()==nil
           and tarn.clock.check({1})==nil and tarn.lazy.quota.ready()
    end)
    ok(tarn_autocommit({},{}))
    assert(not tarn.lazy.stale)
end
local protocol_spaces={'_tarn_catalog','_tarn_meta','_tarn_part','_tarn_txn','_tarn_locks','_tarn_rlocks',
    '_tarn_lazy_capture','_tarn_lazy_defs','_tarn_lazy_outbox','_tarn_lazy_counter',
    '_tarn_lazy_version','_tarn_lazy_reservation','_tarn_lazy_slots','_tarn_lazy_floor','_tarn_lazy_usage'}
local function image()
    box.begin{txn_isolation='read-confirmed'}
    local good, value=pcall(function()
        local side={box.space[b_id]:select{}}
        for _,name in ipairs(protocol_spaces) do side[#side+1]={name,box.space[name]:select{}} end
        local rows=box.space[a_id]:select{}
        return {all=mp.encode({rows,side}),side=mp.encode(side),rows=mp.encode(rows),v=rows[1][2]}
    end)
    box.rollback();if not good then error(value) end
    return value
end
local active, original_capture, original_guard = nil, tarn.lazy.capture, tarn.lazy.guard
-- Observe the real production functions; call their unchanged bodies.
tarn.lazy.capture=function(...)
    if active and fiber.self()==active.fiber then
        active.capture_calls=active.capture_calls+1
        active.capture_id=box.txn_id(); active.capture_stale=tarn.lazy.stale
        active.capture_engine_stale=tarn.engine.stale
    end
    return original_capture(...)
end
tarn.lazy.guard=function(...)
    if active and fiber.self()==active.fiber then
        active.guard_calls=active.guard_calls+1
        active.guard_id=box.txn_id(); active.guard_stale=tarn.lazy.stale
        active.guard_engine_stale=tarn.engine.stale
    end
    return original_guard(...)
end
local function writer(label)
    local state={label=label,rollback_count=0,capture_calls=0,guard_calls=0,done=false}
    local f=fiber.new(function()
        local good, err=pcall(function()
            local response=ok(tarn_select(a_id,0,{1},'EQ',1,box.NULL,true,false,box.NULL,true,box.NULL,{1}))
            local rows=response[2]
            state.read_response,state.initial_id=response,box.txn_id()
            assert(#rows==1 and rows[1][1]==1,'stream first-read shape: '..json.encode{
                response=response,space_id=a_id,stream_id=state.initial_id,expected_pk=1})
            state.read_v=rows[1][2]
            box.on_rollback(function() state.rollback_count=state.rollback_count+1 end)
            state.result=tarn_commit_local('registry-'..label,
                {{'update',a_id,{1},{{'+',2,1}}}},{{a_id,0,{1},1}},{},box.NULL,{a_id,0},{1})
            state.transaction_remains=box.is_in_txn()
        end)
        state.exception=not good and tostring(err) or nil
        if box.is_in_txn() then state.cleanup_rollback=true;box.rollback() end
        state.done=true
    end)
    f:set_joinable(true);state.fiber=f;active=state
    return state
end
local function finish(state)
    wait('public CommitLocal termination',function() return state.done end)
    assert(state.fiber:join());active=nil
    -- Fiber handles are not JSON data; keep only observed primitive fields.
    state.fiber=nil
    if state.exception~=nil then event('writer control exception',state) end
    assert(state.exception==nil,json.encode(state))
    return state
end
local function quiescent(label)
    ready();local before=image()
    local state=finish(writer(label));local after=image()
    check(label..': ordinary marked stream commits exactly once',state.result[1]=='ok'
        and after.v==before.v+1 and after.side==before.side and state.rollback_count==0
        and not state.transaction_remains and not state.cleanup_rollback,state)
end
local function publication(hook, version)
    -- PREPARE's drain cut ends before A is admitted. A later unrelated
    -- writer must not be protected by pretending that older cut included it.
    b_def.ver=version
    b_def.format[#b_def.format+1]={name='c'..version,type='unsigned',is_nullable=true}
    local arg={ver=version,def=b_def,format=b_def.format,builds={},drop={},renames={},
        uix={},uix_drop={},lazy_drop={},checks={},timeout=5}
    ok(tarn_alter_prepare(b_name,arg,version))
    ready()
    ok(tarn_test_hook(hook,'hold'))
    local state=writer(hook)
    assert(tarn_test_hook(hook,'reached')[2]==1,'writer did not reach '..hook)
    assert(state.initial_id~=nil and not state.done,'writer did not retain its stream')
    assert(not tarn.lazy.stale,'registry was already stale before unrelated APPLY')
    local ddl
    if window=='during-publish' then
        ok(tarn_test_hook('alter_schema','hold'))
        ddl={done=false}
        ddl.fiber=fiber.new(function()
            local good,value=pcall(tarn_alter_apply,b_name,arg,version)
            ddl.good,ddl.result,ddl.done=good,value,true
        end)
        ddl.fiber:set_joinable(true)
        assert(tarn_test_hook('alter_schema','reached')[2]==1,'unrelated ALTER did not reach publication hook')
        assert(not ddl.done,'unrelated ALTER escaped its publication hook')
    else
        ok(tarn_alter_apply(b_name,arg,version))
    end
    assert(tarn.lazy.stale,'actual ALTER publication did not invalidate capture')
    local before=image()
    event('unrelated ALTER invalidated capture while real writer hook held',
        {hook=hook,source_id=a_id,other_id=b_id,other_version=version,stream_id=state.initial_id,
         capture_stale=tarn.lazy.stale,engine_stale=tarn.engine.stale,
         source_v=before.v,queue=box.info.synchro.queue,publication_pending=ddl~=nil and not ddl.done,
         confirmed_other_version=box.space._tarn_catalog:get{b_name}.def.ver})
    ok(tarn_test_hook(hook,'release'))
    finish(state)
    local after=image()
    check(hook..': unchanged transaction reaches original capture boundary',
        state.capture_calls==1 and state.capture_id==state.initial_id,state)
    if hook=='written' then
        check(hook..': unchanged transaction reaches original final guard',
            state.guard_calls==1 and state.guard_id==state.initial_id,state)
    end
    local stale=state.result[2]==1412 and state.result[3]=='Lazy capture registry is stale'
    local committed=state.result[1]=='ok'
    if expected=='stale' then
        check(hook..': baseline returns exact stale1412 with definite rollback',stale
            and state.rollback_count==1 and not state.transaction_remains and not state.cleanup_rollback,state)
        check(hook..': baseline rollback preserves full selected durable byte image',before.all==after.all,
            {before_v=before.v,after_v=after.v,all_equal=before.all==after.all})
    else
        check(hook..': unrelated DDL permits this one original commit',committed
            and state.rollback_count==0 and not state.transaction_remains and not state.cleanup_rollback,state)
        check(hook..': only the original increment becomes durable',after.v==before.v+1 and before.side==after.side,
            {before_v=before.v,after_v=after.v,side_equal=before.side==after.side})
    end
    if ddl~=nil then
        check(hook..': unrelated ALTER stays pending until writer terminates',not ddl.done)
        ok(tarn_test_hook('alter_schema','release'))
        wait('unrelated ALTER termination after writer',function() return ddl.done end)
        assert(ddl.fiber:join());assert(ddl.good,json.encode{error=tostring(ddl.result)})
        ok(ddl.result)
    end
    check(hook..': unrelated ALTER remains published without replay',
        box.space._tarn_catalog:get{b_name}.def.ver==version and #box.space[b_id]:format()==3+version)
    quiescent(hook..'-after')
end
local function main()
    ready()
    ok(tarn_bucket_init(16,{{1,16}}))
    ok(tarn_create_table(definition(a_name,a_id)));ok(tarn_create_table(b_def))
    ready()
    ok(tarn_autocommit({{'insert',a_id,{1,0,1}},{'insert',b_id,{1,0,1}}},
        {fence={a_id,0,b_id,0},wb={1,1}}))
    event('identity',{version=box.info.version,wal=box.cfg.wal_mode,engine=box.space[a_id].engine,
        native=tarn_info().native,native_layout=tarn.NS_LAYOUT,public_commit_path='Lua tarn_commit_local'})
    quiescent('before')
    publication('write',1)
    publication('written',2)
end
local good,err=pcall(main)
pcall(tarn_test_hook,'write','release');pcall(tarn_test_hook,'written','release')
pcall(tarn_test_hook,'alter_schema','release')
tarn.lazy.capture,tarn.lazy.guard=original_capture,original_guard
if not good then
    event('fixture setup/control exception',{error=tostring(err)})
    io.stderr:write('FAIL lazy registry fixture: '..tostring(err)..'\n');os.exit(1)
end
print(string.format('%d passed, %d failed (%s %s)',pass,fail,expected,window))
os.exit(fail==0 and 0 or 1)
