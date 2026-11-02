-- Qualification of this isolated RF3 fixture, not production administration.
-- Arguments: enabled|disabled (the native module), the output file, and the
-- tables' engine.
local json, net = require('json'), require('net.box')
local rows, leaders, failures = {}, 0, {}
-- Which storage paths the histories took, summed over the leaders: requests
-- the native procedures served, and requests they handed to the Lua ones.
local paths = {native = 0, lua_handoff = 0}
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
                local n = state.info.native
                if not (type(n) == 'table' and n.ready == 1) then
                    failures[#failures + 1] = 'native module not ready on ' .. address
                else
                    paths.native = paths.native + n.served + n.writes + n.commits + n.validations
                    paths.lua_handoff = paths.lua_handoff + n.to_lua + n.writes_to_lua + n.commits_to_lua
                        + n.validations_to_lua
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
-- With the native module, the histories must take both paths on memtx
-- tables. Vinyl requests stay on the Lua path (docs/native-storage.md), so
-- there they must at least reach the native module and be handed back.
if arg[1] == 'enabled' then
    if arg[3] == 'memtx' and paths.native == 0 then
        failures[#failures + 1] = 'no request took the native path'
    end
    if paths.lua_handoff == 0 then
        failures[#failures + 1] = 'no request was handed from the native procedures to the Lua ones'
    end
end
local f = assert(io.open(arg[2], 'w'))
f:write(json.encode({leaders=leaders, nodes=rows, paths=paths, failures=failures}), '\n')
f:close()
assert(#failures == 0, table.concat(failures, '; '))
os.exit(0)
