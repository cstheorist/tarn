-- Private JSON-lines bridge for backup.py. Never retry an administrative RPC.
local json, net = require('json'), require('net.box')
local uri = assert(os.getenv('TARN_BACKUP_URI'), 'TARN_BACKUP_URI is required')
local c = net.connect(uri, {wait_connected = 10, reconnect_after = 0})
local scripts = {
    cluster = [[
local op,args=...
local m=require('tarn_backup')
assert(op=='inspect' or op=='fence' or op=='audit' or op=='checkpoint' or op=='release',
       'unknown cluster backup operation')
return m[op](unpack(args or {}))
]],
    start = [[
local token,ttl,seed=...
assert(type(box.backup.info)=='function','file backups require Tarantool 3.9')
assert(box.backup.info()==nil,'another backup is active')
if seed~=nil then
    assert(seed~=tostring(box.info.uuid),'seed identity must differ from the source')
    assert(box.space._cluster.index.uuid:get{seed}~=nil,'register seed UUID before taking the backup')
end
local files=box.backup.start{ttl=ttl}
local info=box.backup.info()
_G.__tarn_file_backup={token=token,expires_at=info.expires_at}
return {files=files,backup=info,source_uuid=tostring(box.info.uuid),
    replicaset_uuid=tostring(box.info.replicaset.uuid),version=box.info.version,
    source_dir=require('fio').cwd(),seed_uuid=seed}
]],
    check = [[
local token=...
local held=rawget(_G,'__tarn_file_backup')
local info=box.backup.info()
assert(held~=nil and held.token==token and info~=nil and info.expires_at==held.expires_at,
       'backup lease expired or changed')
return true
]],
    stop = [[
local token=...
local held=rawget(_G,'__tarn_file_backup')
if held==nil or held.token~=token then return false end
local info=box.backup.info()
if info~=nil and info.expires_at==held.expires_at then box.backup.stop() end
_G.__tarn_file_backup=nil
return true
]],
}
io.stdout:setvbuf('no')
for line in io.lines() do
    local good, result = pcall(function()
        local request=json.decode(line)
        local timeout=request.timeout or 30
        assert(type(timeout)=='number' and timeout>0 and timeout<=3600, 'invalid RPC deadline')
        return c:eval(assert(scripts[request.op], 'unknown backup operation'), request.args or {}, {timeout=timeout})
    end)
    print(json.encode(good and {ok=true,result=result} or {ok=false,error=tostring(result)}))
end
c:close()
os.exit()
