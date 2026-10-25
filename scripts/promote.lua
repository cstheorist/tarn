-- promote.lua URI: make the node at URI the leader of its replica set
-- (box.ctl.promote()) -- the previous leader is demoted while its client
-- connections stay open.
local nb = require('net.box')
local c = nb.connect(arg[1], {wait_connected = 2, connect_timeout = 2})
if not c:is_connected() then io.stderr:write('cannot connect to ' .. arg[1] .. '\n'); os.exit(1) end
local ok, err = pcall(c.eval, c, 'box.ctl.promote()', {}, {timeout = 10})
if not ok then io.stderr:write(tostring(err) .. '\n'); os.exit(1) end
os.exit(0)
