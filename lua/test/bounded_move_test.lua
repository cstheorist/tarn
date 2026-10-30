-- Tiny fixed budgets exercise disk staging, durable receipts, frozen recopy,
-- partial visibility and cancellation without bucket-sized Lua containers.
local net, json = require('net.box'), require('json')
local c = {}
for i = 1, 2 do
    c[i] = net.connect(os.getenv('S' .. i), {user='tarn', password='tarn', wait_connected=5})
    assert(c[i]:is_connected())
end
local n = 0
local function check(name, condition, value)
    assert(condition, name .. ': ' .. json.encode(value))
    n = n + 1; print('ok ' .. name)
end
local function call(i, fn, args)
    local r = c[i]:call(fn, args or {})
    assert(r[1] == 'ok', fn .. ': ' .. json.encode(r)); return r
end
local function cfg(i, key, value)
    return c[i]:eval('local k,v=... local old=tarn.cfg[k] tarn.cfg[k]=v return old', {key,value})
end
local function progress(b, id) return call(2, 'tarn_bucket_progress', {b,id})[2] end
local function stage(b) return c[2]:eval('return box.space._tarn_move:get{...}.rows', {b}) end
local function batch(i, fn, b, id, seq)
    local count = 0
    repeat
        local r = c[i]:call(fn, {b,id,true,seq})
        assert(r[1]=='ok' or r[1]=='more', json.encode(r))
        count = count + 1
        if r[1]=='ok' then return count end
    until count > 100
    error('bounded operation did not finish')
end
local defs = {}
for j, engine in ipairs({'memtx','vinyl'}) do
    local d = {name='test.bounded_move_' .. engine, id=24170+j, engine=engine,
        format={{name='id',type='unsigned'},{name='payload',type='string'},{name='bucket_id',type='unsigned'}},
        indexes={{name='primary',unique=true,parts={{field='id',type='unsigned'}}},
                 {name='bucket_id',unique=false,parts={{field='bucket_id',type='unsigned'}}}}}
    defs[j]=d
    for i=1,2 do call(i,'tarn_create_table',{d}) end
end
for i=1,2 do
    local r=c[i]:call('tarn_bucket_init',{16,i==1 and {{1,8}} or {{9,16}}})
    assert(r[1]=='ok' or r[1]=='exists')
    cfg(i,'move_batch_rows',3);cfg(i,'move_batch_bytes',2048);cfg(i,'move_delta_keys',2)
end
local b,id=1,'bounded-176'
for _,d in ipairs(defs) do
    for k=1,13 do c[1]:eval('local s,k,b=... box.space[s]:replace{k,string.rep("a",100),b}',{d.id,k,b}) end
end
check('staging uses disk engine', c[2]:eval('return box.space._tarn_recv.engine')=='vinyl')
call(2,'tarn_bucket_recv_begin',{b,1,id,2})
local gen=call(1,'tarn_bucket_send_begin',{b,2,id,5})[2]
local page=call(1,'tarn_bucket_rows',{b})
local cp={seq=1,phase='copy',generation=gen,cursor=page[3],done=false}
local old=cfg(2,'move_staging_bytes',1)
local r=c[2]:call('tarn_bucket_recv_rows',{b,id,page[2],cp})
check('staging budget refusal rolls back rows and progress',r[1]=='error' and stage(b)==0 and progress(b,id).seq==0,r)
cfg(2,'move_staging_bytes',old)
old=cfg(2,'move_disk_reserve_bytes',9007199254740991)
local ok, result=pcall(c[2].call,c[2],'tarn_bucket_recv_rows',{b,id,page[2],cp})
check('disk headroom refusal preserves progress',(not ok or result[1]=='error') and stage(b)==0,result)
cfg(2,'move_disk_reserve_bytes',old)
call(2,'tarn_bucket_recv_rows',{b,id,page[2],cp})
check('duplicate receipt is idempotent',call(2,'tarn_bucket_recv_rows',{b,id,page[2],cp})[2]=='duplicate' and stage(b)==3)
local missing={seq=2,phase='copy',generation=gen,cursor=box.NULL,done=true}
r=c[2]:call('tarn_bucket_recv_rows',{b,id,{{999999,{1,'missing',b}}},missing})
check('missing target space refuses the whole page',r[1]=='error' and r[2]==1412 and stage(b)==3
      and progress(b,id).seq==1,r)
cp.done=true
r=c[2]:call('tarn_bucket_recv_rows',{b,id,page[2],cp})
check('same sequence with changed checkpoint is refused',r[1]=='error' and progress(b,id).done==false,r)
cp.done=false;cp.seq=3
r=c[2]:call('tarn_bucket_recv_rows',{b,id,page[2],cp})
check('out-of-order page refused',r[1]=='error' and stage(b)==3,r)
cp.seq=1
old=cfg(2,'move_slots',1)
r=c[2]:call('tarn_bucket_recv_begin',{4,1,'bounded-concurrent',2})
check('concurrent move admission refuses excess reservation',r[1]=='error',r)
cfg(2,'move_slots',old)
call(2,'tarn_bucket_recv_begin',{4,1,'bounded-concurrent',2})
local other={{defs[1].id,{100,string.rep('b',100),4}}}
local cp2={seq=1,phase='copy',generation='other',done=true,cursor=box.NULL}
local total=c[2]:call('tarn_info').movement.staged_bytes
old=cfg(2,'move_total_staging_bytes',total+1)
local per=cfg(2,'move_staging_bytes',total+1)
r=c[2]:call('tarn_bucket_recv_rows',{4,'bounded-concurrent',other,cp2})
check('aggregate staging admission refuses other move',r[1]=='error' and stage(4)==0,r)
cfg(2,'move_total_staging_bytes',old);cfg(2,'move_staging_bytes',per)
call(2,'tarn_bucket_recv_rows',{4,'bounded-concurrent',other,cp2})
check('cancellation reclaims reserved disk in batches',batch(2,'tarn_bucket_recv_abort',4,'bounded-concurrent')==2)
r=c[2]:call('tarn_bucket_recv_rows',{4,'bounded-concurrent',other,cp2})
check('late page cannot recreate cancelled resources',r[1]=='error',r)
-- Overflow dirty hints using actual checked writes, then force a full frozen
-- recopy. Deletion/reinsertion must not resurrect the old baseline payload.
for k=1,3 do call(1,'tarn_autocommit',{{{'replace',defs[1].id,{k,'changed'..k,b}}},{wb={b}}}) end
call(1,'tarn_autocommit',{{{'delete',defs[2].id,{2}}}},{wb={b}})
call(2,'tarn_bucket_hold',{b,id})
call(1,'tarn_bucket_fence',{b,id,5})
r=c[1]:call('tarn_bucket_delta',{b,id,gen,1})
check('bounded delta overflow requires frozen recopy',r[1]=='lost',r)
local seq=progress(b,id).seq+1
check('clear uses bounded commits',batch(2,'tarn_bucket_recv_clear',b,id,seq)==2)
check('repeated reset receipt cannot clear a later page',progress(b,id).phase=='frozen')
local copied=0
repeat
    local p=progress(b,id)
    page=call(1,'tarn_bucket_rows',{b,p.cursor or box.NULL})
    cp={seq=p.seq+1,phase='frozen',generation=id,cursor=page[3] or box.NULL,
        done=page[3]==nil or page[3]==box.NULL}
    call(2,'tarn_bucket_recv_rows',{b,id,page[2],cp})
    copied=copied+#page[2]
    call(2,'tarn_bucket_recv_clear',{b,id,true,seq})
    check('late reset retained staged page '..cp.seq,stage(b)==copied)
until cp.done
check('frozen recopy includes exact mixed-engine rows',copied==25)
local sid=defs[1].id
local args={sid,0,{},'ALL',box.NULL,box.NULL,true,true,box.NULL,false,box.NULL,box.NULL}
r=c[1]:call('tarn_bucket_send',{b,id,true})
check('source reclaim is bounded',r[1]=='more',r)
r=c[1]:call('tarn_select',args)
check('unscoped reads refuse partial source',r[1]=='moved',r)
r=c[1]:call('tarn_bucket_send_abort',{b,id})
check('reclaim cannot reverse ownership',r[1]=='error',r)
check('source reclamation needs multiple commits',batch(1,'tarn_bucket_send',b,id)>2)
r=c[2]:call('tarn_bucket_recv',{b,id,true})
check('receiver install is bounded',r[1]=='more',r)
r=c[2]:call('tarn_select',args)
check('unscoped reads refuse partial receiver',r[1]=='moved',r)
local st=c[2]:new_stream();st:eval('tarn.fresh.begin_stream()')
local native=c[2]:call('tarn_info').native
if os.getenv('TARN_NATIVE') and os.getenv('TARN_NATIVE') ~= '0' then
    check('explicit native module is loaded', type(native) == 'table')
    r=c[2]:call('tarn_storage.ts_select',args)
    check('native dispatch hands partial reads to Lua',r[1]=='lua',r)
end
check('receiver install needs multiple commits',batch(2,'tarn_bucket_recv',b,id)>2)
r=st:call('tarn_select',args)
check('snapshot opened during install stays refused after publication',r[1]=='moved',r)
pcall(st.rollback,st)
for _,d in ipairs(defs) do
    local a=c[1]:eval('return box.space[...]:count()', {d.id})
    local z=c[2]:eval('return box.space[...]:select{}', {d.id})
    check('no source tuples remain '..d.engine,a==0)
    check('target exact row count '..d.engine,#z==(d.engine=='memtx' and 13 or 12),#z)
    for _,t in ipairs(z) do
        local want=d.engine=='memtx' and t[1]<=3 and 'changed'..t[1] or string.rep('a',100)
        check('exact payload '..d.engine..' '..t[1],t[2]==want)
    end
end
check('all movement resources released',c[1]:call('tarn_info').movement.active==0 and c[2]:call('tarn_info').movement.active==0)
for i=1,2 do
    local stats=c[i]:call('tarn_info').movement.stats
    check('observed batches respect fixed limits '..i,stats.max_batch_rows<=3 and stats.max_batch_bytes<=2048,stats)
    cfg(i,'move_batch_rows',512);cfg(i,'move_batch_bytes',1048576);cfg(i,'move_delta_keys',16384)
end
print(n .. ' bounded movement checks passed')
os.exit(0)
