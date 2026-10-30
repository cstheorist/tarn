-- probe.lua URI: exit 0 iff URI is a writable Tarn storage node with its schema.
local nb = require('net.box')
local ok, c = pcall(nb.connect, arg[1], {wait_connected = 1, connect_timeout = 1})
if not ok or not c:is_connected() then os.exit(1) end
local ok2, r = pcall(c.eval, c, [[
    if box.info.ro or box.space._tarn_meta == nil or box.space._tarn_meta:get{'shard_id'} == nil then
        return false
    end
    local compatibility = rawget(_G, 'tarn_compatibility')
    if compatibility ~= nil then
        local report = compatibility()
        return report[1] == 'ok' and report[7] ~= 'pending'
    end
    return true
]], {}, {timeout = 1})
os.exit((ok2 and r) and 0 or 1)
