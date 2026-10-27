-- One refused destination must not starve healthy groups/pages.
local net,fiber=require('net.box'),require('fiber')
local c={}
for i=1,3 do c[i]=net.connect(assert(os.getenv('S'..i)),{user='tarn',password='tarn',wait_connected=5}) end
local function call(i,fn,...)return c[i]:call(fn,{...})end
local function ok(r)assert(r[1]=='ok',require('json').encode(r));return r end
local function wait(fn)
 local deadline=fiber.clock()+5 repeat if fn()then return true end fiber.sleep(.01) until fiber.clock()>deadline return false
end
local sid,gen,name=69001,69002,'test.lazy_shipper'
local spec={gen=gen,base_space_id=sid,local_index_id=2,route_parts=1,key_fields={2},key_types={'string'},
 pk_fields={1},pk_native_fields={1},pk_types={'unsigned'},bucket_field=3}
local def={id=sid,name=name,ver=0,engine='memtx',json='{}',lazy={spec},
 format={{name='id',type='unsigned'},{name='v',type='string'},{name='bucket_id',type='unsigned'}},
 indexes={{name='primary',unique=true,parts={{1,'unsigned'}}},
  {name='bucket_id',unique=false,parts={{3,'unsigned'}}},{name='v',unique=false,parts={{2,'string'}}}}}
for i=1,3 do
 ok(call(i,'tarn_lazy_control','pause'))
 ok(call(i,'tarn_create_prepare',name,{def=def},1))
 repeat local r=ok(call(i,'tarn_lazy_bootstrap',name,{def=def},1));if r[2]then break end until false
 ok(call(i,'tarn_lazy_ready',name,{def=def},1));ok(call(i,'tarn_create_apply',name,{def=def}))
end
local owners={}
for i=1,3 do for _,span in ipairs(call(i,'tarn_buckets')[4]) do for b=span[1],span[2]do owners[b]=i end end end
local good,bad
for n=1,1000 do
 local k='ship-'..n local b=c[1]:eval('local s,k=... return tarn.lazy.bucket(s,{k})',{spec,k})
 if owners[b]==1 then good=good or k elseif owners[b]==2 then bad=bad or k end
 if good and bad then break end
end
assert(good and bad)
c[2]:eval([[local g=... local original=tarn_lazy_apply rawset(_G,'__ship_original',original)
 tarn_lazy_apply=function(batch,route)for _,a in ipairs(batch)do if a[2]==g then return {'error',1412,'test refusal'}end end
 return original(batch,route)end]],{gen})
local writes,wb={},{}
for n=1,259 do writes[n],wb[n]={'insert',sid,{n,(n==2 or n==259)and good or bad,2}},2 end
ok(call(1,'tarn_autocommit',writes,{fence={sid,0},wb=wb}))
ok(call(1,'tarn_lazy_control','resume'))
assert(wait(function()return c[1]:eval('return box.space[...].index.ordered:count{true,select(2,...)}',{gen,good})==2
 and call(1,'tarn_lazy_stats')[2].depth==257 end),
 'failed group or first page starved a healthy later assignment')
local depth=call(1,'tarn_lazy_stats')[2].depth
assert(depth==257,'refused assignments were dropped or healthy ACKs not exact: '..depth)
local f=call(1,'tarn_lazy_frontiers',{2},{2})[2][1]
assert(f[2]==259 and f[3]==0,'a later ACK skipped the earliest pending hole')
assert(c[1]:eval('return tarn.lazy.owner_cache.n<=4096'))
print('  ok   all healthy owner groups and later pages progress past a refused destination')
ok(call(1,'tarn_lazy_control','pause'))
c[2]:eval('tarn_lazy_apply=__ship_original __ship_original=nil')
ok(call(1,'tarn_lazy_control','resume'))
assert(wait(function()return call(1,'tarn_lazy_stats')[2].depth==0 end),'refused exact frames did not recover')
assert(c[2]:eval('return box.space[...].index.ordered:count{true,select(2,...)}',{gen,bad})==257)
f=call(1,'tarn_lazy_frontiers',{2},{2})[2][1];assert(f[2]==259 and f[3]==259)
print('  ok   restored destination drains exact retained frames and closes the frontier hole')
for i=1,3 do ok(call(i,'tarn_lazy_control','resume'))end
print('2 passed, 0 failed')
os.exit(0)
