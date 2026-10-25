-- joined.lua URI N: exit 0 iff the storage instance at URI has finished
-- bootstrapping and follows the other N - 1 instances of its shard.
local nb = require('net.box')
local ok, c = pcall(nb.connect, arg[1], {wait_connected = 1, connect_timeout = 1})
if not ok or not c:is_connected() then os.exit(1) end
local ok2, r = pcall(c.eval, c, [[
    local peers = ...
    if box.info.status ~= 'running' then return false end
    local n = 0
    for id, p in pairs(box.info.replication) do
        if id ~= box.info.id and p.upstream ~= nil and p.upstream.status == 'follow' then n = n + 1 end
    end
    return n >= peers
]], {tonumber(arg[2]) - 1}, {timeout = 1})
os.exit((ok2 and r) and 0 or 1)
