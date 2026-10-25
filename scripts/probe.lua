-- probe.lua URI: exit 0 iff URI is a writable Tarn storage node with its schema.
local nb = require('net.box')
local ok, c = pcall(nb.connect, arg[1], {wait_connected = 1, connect_timeout = 1})
if not ok or not c:is_connected() then os.exit(1) end
local ok2, r = pcall(c.eval, c, [[return (not box.info.ro) and box.space._tarn_meta ~= nil
    and box.space._tarn_meta:get{'shard_id'} ~= nil]], {}, {timeout = 1})
os.exit((ok2 and r) and 0 or 1)
