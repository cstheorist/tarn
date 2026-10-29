-- Test-only bounded administrative request; no automatic mutation retries.
local net,json=require('net.box'),require('json')
local c=net.connect(assert(arg[1]),{wait_connected=math.min(5,tonumber(arg[3]) or 30),reconnect_after=0})
local f=assert(io.open(assert(arg[2])));local code=f:read('*a');f:close()
local result=c:eval(code,{}, {timeout=tonumber(arg[3]) or 30})
print(json.encode(result));c:close();os.exit()
