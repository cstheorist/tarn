-- Test-only stages on owned peers. `action` is supplied by the Python drill.
local net, fiber = require('net.box'), require('fiber')
local c = {net.connect(os.getenv('CB_NODE_0'), {wait_connected=5, reconnect_after=0}),
           net.connect(os.getenv('CB_NODE_3'), {wait_connected=5, reconnect_after=0})}
local function call(i, fn, ...) return c[i]:call(fn, {...}, {timeout=30}) end
local function ok(r) assert(r[1]=='ok', require('json').encode(r)); return r end
local function ev(i, code, args) return c[i]:eval(code, args or {}, {timeout=30}) end
local sid, gen, name = 88003, 88004, 'test.lazy_backup'
local spec = {gen=gen, base_space_id=sid, local_index_id=2, route_parts=1,
    key_fields={2}, key_types={'string'}, pk_fields={1}, pk_native_fields={1},
    pk_types={'unsigned'}, bucket_field=3}
local function await(fn)
    local deadline=fiber.clock()+20
    repeat if fn() then return end; fiber.sleep(.02) until fiber.clock()>deadline
    error('lazy backup fixture did not drain')
end
local function depth(i) return call(i,'tarn_lazy_stats')[2].depth end
if action == 'native_probe' then
    local writes, opts={{'replace',88001,{1,'native-probe',1}}},{fence={88001,0},wb={1}}
    if os.getenv('TARN_NATIVE')~='0' then
        assert(ev(1,"return box.func['tarn_storage.ts_autocommit']~=nil"))
        local r=call(1,'tarn_storage.ts_autocommit',writes,opts)
        if r[1]=='lua' then ok(call(1,'tarn_autocommit',writes,opts)) else ok(r) end
        local n=call(1,'tarn_info').native
        assert(n and (n.writes>0 or n.writes_to_lua>0))
        for _, connection in ipairs(c) do connection:close() end
        return n
    end
    assert(not call(1,'tarn_info').native)
    ok(call(1,'tarn_autocommit',writes,opts))
elseif action == 'native_fenced' then
    local writes,opts={{'replace',88001,{999,'forbidden',1}}},{fence={88001,0},wb={1}}
    if os.getenv('TARN_NATIVE')~='0' then
        local success,r=pcall(call,2,'tarn_storage.ts_autocommit',writes,opts)
        assert(not success or r[1]=='lua' or r[1]~='ok')
    end
    local success,r=pcall(call,2,'tarn_autocommit',writes,opts)
    assert(not success or r[1]~='ok')
    assert(ev(2,'return box.space[88001]:get{999}==nil'))
elseif action == 'lazy_create' then
    for i=1,2 do
        local def={id=sid,name=name,ver=0,engine=os.getenv('CB_ENGINE'),json='{}',lazy={spec},
            format={{name='id',type='unsigned'},{name='v',type='string'},{name='bucket_id',type='unsigned'}},
            indexes={{name='primary',unique=true,parts={{1,'unsigned'}}},
                     {name='bucket_id',unique=false,parts={{3,'unsigned'}}},
                     {name='v',unique=false,parts={{2,'string'}}}}}
        local a={def=def,timeout=5}
        ok(call(i,'tarn_create_prepare',name,a,1))
        repeat local r=ok(call(i,'tarn_lazy_bootstrap',name,a,1)); if r[2] then break end until false
        ok(call(i,'tarn_lazy_ready',name,a,1)); ok(call(i,'tarn_create_apply',name,a))
    end
elseif action == 'lazy_pending' then
    local keys={}
    for i=1,100 do
        local key='key-'..i
        if ev(1,'local s,k=...;return tarn.lazy.bucket(s,{k})',{spec,key})==2 then keys[#keys+1]=key end
        if #keys==2 then break end
    end
    assert(#keys==2)
    ok(call(1,'tarn_lazy_control','pause'))
    ok(call(1,'tarn_autocommit',{{'insert',sid,{1,keys[1],1}}},{fence={sid,0},wb={1}}))
    local old=call(1,'tarn_lazy_control','pending')[2]
    assert(#old==1 and depth(1)==1)
    ev(1,"local keys,old=...;box.space._tarn_meta:replace{'test.backup.lazy',{keys=keys,old=old}};return true",{keys,old})
elseif action == 'lazy_drain' then
    local saved=ev(1,"return box.space._tarn_meta:get{'test.backup.lazy'}[2]")
    ok(call(1,'tarn_lazy_control','resume')); await(function() return depth(1)==0 end)
    ok(call(1,'tarn_autocommit',{{'replace',sid,{1,saved.keys[2],1}}},{fence={sid,0},wb={1}}))
    await(function() return depth(1)==0 end)
    ok(call(2,'tarn_lazy_gc',gen,2,{2}))
    assert(ev(2,'local g=...;return box.space._tarn_lazy_floor:get{g,2,1}.seq',{gen})>=2)
elseif action == 'move_begin' then
    local r=ok(call(1,'tarn_bucket_send_begin',1,2,'backup-move',5))
    ev(1,'__backup_move_generation=...;return true',{r[2]})
    ok(call(2,'tarn_bucket_recv_begin',1,1,'backup-move'))
elseif action == 'move_finish' then
    local after=box.NULL
    repeat
        local r=ok(call(1,'tarn_bucket_rows',1,after))
        ok(call(2,'tarn_bucket_recv_rows',1,'backup-move',r[2])); after=r[3]
    until after==box.NULL
    ok(call(2,'tarn_bucket_hold',1,'backup-move'))
    ok(call(1,'tarn_bucket_fence',1,'backup-move',5))
    local generation=ev(1,'return __backup_move_generation')
    local at=1
    repeat
        local r=ok(call(1,'tarn_bucket_delta',1,'backup-move',generation,at))
        ok(call(2,'tarn_bucket_recv_rows',1,'backup-move',r[2])); at=r[3]
    until at==box.NULL
    ok(call(1,'tarn_bucket_send',1,'backup-move')); ok(call(2,'tarn_bucket_recv',1,'backup-move'))
elseif action == 'post_restore' then
    local saved=ev(1,"return box.space._tarn_meta:get{'test.backup.lazy'}[2]")
    -- Deliver the actual retained old frame after GC, movement and restore.
    ok(call(2,'tarn_lazy_apply',saved.old,{2}))
    local actual=ev(2,[[local s,g,keys=...;return {box.space[s]:get{1}[2],
        box.space[g].index.ordered:count{true,keys[1]},box.space[g].index.ordered:count{true,keys[2]},
        box.space._tarn_lazy_floor:get{g,2,1}.seq}]],{sid,gen,saved.keys})
    assert(actual[1]==saved.keys[2] and actual[2]==0 and actual[3]==1 and actual[4]>=2)
else error('unknown fixture stage') end
for _, connection in ipairs(c) do connection:close() end
return true
