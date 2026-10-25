-- kvget.lua NODES_JSON SPACE KEY_JSON: read one row by primary key straight
-- from Tarantool on every listed node and print the found tuples as JSON.
local nb = require('net.box')
local json = require('json')
local out = {}
for _, uri in ipairs(json.decode(arg[1])) do
    local c = nb.connect('tarn:tarn@' .. uri, {wait_connected = 2})
    local t = c.space[arg[2]].index.primary:get(json.decode(arg[3]))
    if t ~= nil then out[#out + 1] = t:totable() end
    c:close()
end
print(json.encode(out))
os.exit(0)
