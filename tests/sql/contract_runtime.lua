-- Qualification of this isolated RF3 fixture, not production administration.
local json, net = require('json'), require('net.box')
local rows, leaders = {}, 0
for shard = 1, 2 do
    local writable = 0
    for replica = 1, 3 do
        local address = '127.0.0.1:' .. tostring(3300 + 10 * shard + replica)
        local c = net.connect(address, {wait_connected = 5})
        assert(c:is_connected(), address)
        local state = c:eval('return {ro=box.info.ro, version=_TARANTOOL, info=tarn_info()}')
        c:close()
        state.address = address
        rows[#rows + 1] = state
        if not state.ro then
            writable = writable + 1
            if arg[1] == 'enabled' then
                assert(type(state.info.native) == 'table' and state.info.native.ready == 1,
                       'native module not ready on ' .. address)
            else
                assert(state.info.native == false, 'native module unexpectedly enabled on ' .. address)
            end
        end
    end
    assert(writable == 1, 'expected one leader in shard ' .. tostring(shard))
    leaders = leaders + writable
end
local f = assert(io.open(arg[2], 'w'))
f:write(json.encode({leaders=leaders, nodes=rows}), '\n')
f:close()
os.exit(0)
