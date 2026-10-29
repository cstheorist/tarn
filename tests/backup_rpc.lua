-- Test-only bounded administrative request; no automatic mutation retries.
local net,json=require('net.box'),require('json')
local c=net.connect(assert(arg[1]),{wait_connected=10,reconnect_after=0})
local f=assert(io.open(assert(arg[2])));local code=f:read('*a');f:close()
local result=c:eval(code,{}, {timeout=1800})
print(json.encode(result));c:close();os.exit()
