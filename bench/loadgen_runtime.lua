-- Qualification of the owned local smoke fixture only.
local json, net = require('json'), require('net.box')
local base, db, engine, native = tonumber(arg[1]), arg[2], arg[3], arg[4]
local rows, leaders = {}, 0
for shard=1,2 do
    local writable = 0
    for replica=1,3 do
        local address='127.0.0.1:'..tostring(base+10*shard+replica)
        local c=net.connect('tarn:tarn@'..address,{wait_connected=5})
        assert(c:is_connected(),address)
        local r=c:eval([[local db=...; return {info=tarn_info(),wal=box.cfg.wal_mode,
            quorum=box.cfg.replication_synchro_quorum,engine=box.space[db..'.t'].engine}]],{db})
        c:close()
        assert(r.wal=='fsync' and r.quorum=='N / 2 + 1' and r.engine==engine)
        if not r.info.ro then
            writable=writable+1
            if native=='enabled' then assert(r.info.native and r.info.native.ready==1)
            else assert(r.info.native==false) end
        end
        rows[#rows+1]={address=address,result=r}
    end
    assert(writable==1); leaders=leaders+writable
end
print(json.encode({scope='same-host RF3/fsync functional fixture',leaders=leaders,nodes=rows}))
os.exit(0)
