-- Private test bridge. Each administrative call is sent once; its caller
-- must inspect durable state after an uncertain result, never replay writes.
local json, net = require('json'), require('net.box')
local uri=assert(arg[1])
local c = net.connect(uri, {wait_connected=2,reconnect_after=0})
io.stdout:setvbuf('no')
for line in io.lines() do
    local q=json.decode(line)
    if not c:is_connected() then
        c:close();c=net.connect(uri,{wait_connected=2,reconnect_after=0})
    end
    local ok,r=pcall(c.eval,c,q.code,q.args or {},{timeout=q.timeout or 30})
    print(json.encode(ok and {ok=true,result=r} or {ok=false,error=tostring(r)}))
end
c:close();os.exit()
