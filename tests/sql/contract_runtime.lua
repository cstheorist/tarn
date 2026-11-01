-- Qualification of this isolated RF3 fixture, not production administration.
local json, net = require('json'), require('net.box')
local rows, leaders, failures = {}, 0, {}
for shard = 1, 2 do
    local writable = 0
    for replica = 1, 3 do
        local address = '127.0.0.1:' .. tostring(3300 + 10 * shard + replica)
        local c = net.connect('tarn:tarn@' .. address, {wait_connected = 5})
        assert(c:is_connected(), address)
        local info = c:call('tarn_info')
        local durability = c:eval([[
local cfg=box.cfg
local sync={}
for _,name in ipairs{'_tarn_meta','_tarn_part','_tarn_txn','_tarn_locks','_tarn_catalog'} do
    local sp=box.space[name]
    sync[name]=sp and sp.is_sync or false
end
return {wal_mode=cfg.wal_mode,quorum_config=cfg.replication_synchro_quorum,
    quorum=box.info.synchro.quorum,
    replica_count=type(cfg.replication)=='table' and #cfg.replication or 0,
    sync=tarn.cfg.sync,spaces=sync}
]])
        local state = {ro=info.ro, info=info, durability=durability}
        c:close()
        state.address = address
        rows[#rows + 1] = state
        if durability.wal_mode ~= 'fsync' or durability.quorum ~= 2
           or durability.replica_count ~= 3 or durability.sync ~= true then
            failures[#failures + 1] = 'RF3/quorum2/fsync configuration not established on ' .. address
        end
        for name, synchronous in pairs(durability.spaces) do
            if not synchronous then failures[#failures + 1] = 'non-synchronous ' .. name .. ' on ' .. address end
        end
        if not state.ro then
            writable = writable + 1
            if arg[1] == 'enabled' then
                if not (type(state.info.native) == 'table' and state.info.native.ready == 1) then
                    failures[#failures + 1] = 'native module not ready on ' .. address
                end
            else
                if state.info.native ~= false then
                    failures[#failures + 1] = 'native module unexpectedly enabled on ' .. address
                end
            end
        end
    end
    if writable ~= 1 then failures[#failures + 1] = 'expected one leader in shard ' .. tostring(shard) end
    leaders = leaders + writable
end
local f = assert(io.open(arg[2], 'w'))
f:write(json.encode({leaders=leaders, nodes=rows, failures=failures}), '\n')
f:close()
assert(#failures == 0, table.concat(failures, '; '))
os.exit(0)
