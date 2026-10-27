-- Tests only: one bounded net.box call/eval, then close every owned handle.
local nb = require('net.box')
local json = require('json')
local uri = arg[1]
if not uri:find('@', 1, true) then uri = 'tarn:tarn@' .. uri end
local c = nb.connect(uri, {wait_connected = 2})
local ok, result = pcall(function()
    local args = json.decode(arg[4])
    if arg[2] == 'call' then return c:call(arg[3], args, {timeout = 5}) end
    if arg[2] == 'eval' then return c:eval(arg[3], args, {timeout = 5}) end
    error('unknown test RPC mode')
end)
c:close()
if not ok then io.stderr:write(tostring(result) .. '\n'); os.exit(1) end
print(json.encode(result))
os.exit(0)
