-- Reopen the joined/moved heterogeneous-ID candidate from real fsync WAL.
local net,fiber=require('net.box'),require('fiber')
local c=net.connect(assert(os.getenv('S3')),{user='tarn',password='tarn',wait_connected=10,reconnect_after=.05})
local deadline=fiber.clock()+5
while fiber.clock()<deadline do
    local ok,ready=pcall(c.eval,c,'return rawget(_G,"tarn")~=nil and tarn.lazy.quota.ready()')
    if ok and ready then break end
    fiber.sleep(.02)
end
local state=c:eval('return box.space._tarn_meta:get{"test.lazy.join_restart"}.value')
assert(c:eval([[local r=... return box.space._tarn_lazy_counter.id==r.aux[1]
    and box.space._tarn_lazy_version.id==r.aux[2] and box.space._tarn_lazy_floor.id==r.aux[3]
    and box.space._tarn_lazy_counter:get{1}.seq==r.counter and box.space[r.sid]:len()==0
    and box.space._tarn_lazy_floor:get{r.gen,r.target,1}.seq==r.counter]],{state}))
local ack=c:call('tarn_lazy_apply',{{state.replay},{state.target}});assert(ack[1]=='ok')
local w=c:call('tarn_autocommit',{{{'insert',state.sid,{1,'after-restart',1}}},{fence={state.sid,0},wb={1}}})
assert(w[1]=='ok',require('json').encode(w))
deadline=fiber.clock()+5
local complete=false
repeat
    local f=c:call('tarn_lazy_frontiers',{{1},{1}})
    complete=f[1]=='ok' and f[2][1][2]==6 and f[2][1][3]==6
    if complete then break end
    fiber.sleep(.02)
until fiber.clock()>deadline
assert(complete,'restarted joined source did not resume delivery at seq6')
print('  ok   heterogeneous auxiliary IDs, empty-bucket counter and floor survive restart')
print('1 passed, 0 failed')
os.exit(0)
