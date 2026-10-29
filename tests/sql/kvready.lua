-- Raw readiness diagnostics for the fresh six-node tests/kv.sh fixture.
-- This is a test precondition, not a replacement for a request's strong barrier.
local json, net, fiber = require('json'), require('net.box'), require('fiber')
local f = assert(io.open(assert(arg[1]), 'r'))
local config = json.decode(f:read('*a')); f:close()
assert(config.version == 1 and type(config.native) == 'boolean'
    and type(config.replicas) == 'table' and #config.replicas == 2)
local conns, endpoints = {}, {}
for _, group in ipairs(config.replicas) do
    assert(type(group) == 'table' and #group == 3)
    for _, endpoint in ipairs(group) do
        assert(type(endpoint) == 'string' and endpoint:match('^127%.0%.0%.1:%d+$'))
        endpoints[#endpoints + 1] = endpoint
        conns[#conns + 1] = net.connect(endpoint, {user = 'tarn', password = 'tarn',
            wait_connected = false, connect_timeout = .3, reconnect_after = .05})
    end
end
local probe = [[
    local t=rawget(_G,'tarn'); if not t then return false end
    local p=tarn_clock_probe(); local i=tarn_info()
    local spaces,all_sync={},true
    for _,name in ipairs{'_tarn_meta','_tarn_part','_tarn_txn'} do
        local s=box.space[name]; spaces[name]=s~=nil and s.is_sync and s.state.is_sync or false
        all_sync=all_sync and spaces[name]
    end
    local layout,size
    if i.native then
        local ffi=require('ffi');local n=ffi.cast('struct tarn_ns*',box.func['tarn_storage.ts_state']:call())
        layout,size=tonumber(n.layout),ffi.sizeof('struct tarn_ns')
    end
    local replicas={}
    for id,r in pairs(box.info.replication) do replicas[#replicas+1]={id=id,uuid=tostring(r.uuid),
        upstream=r.upstream and r.upstream.status or false,downstream=r.downstream and r.downstream.status or false} end
    return {loaded=t.clock.loaded,local_ok=p.local_ok,clock=t.clock.status(),read_ready=t.clock.read_check()==nil,
        ro=box.info.ro,term=box.info.election.term,leader=box.info.election.leader,uuid=tostring(box.info.uuid),pid=box.info.pid,
        queue=box.info.synchro.queue,quorum=box.info.synchro.quorum,quorum_config=box.cfg.replication_synchro_quorum,
        read_quorum=box.cfg.replication_linearizable_quorum,replication_count=#box.cfg.replication,replicas=replicas,
        sync=t.cfg.sync,spaces_sync=spaces,all_sync=all_sync,wal=box.cfg.wal_mode,mvcc=box.cfg.memtx_use_mvcc_engine,
        isolation=box.cfg.txn_isolation,native=i.native,native_layout=layout,native_size=size,version=box.info.version,
        memtx_memory=box.cfg.memtx_memory,vinyl_memory=box.cfg.vinyl_memory,vinyl_cache=box.cfg.vinyl_cache}
]]
local deadline = fiber.clock() + 90
local facts, leaders, ready
repeat
    facts, leaders, ready = {}, {}, true
    local counts = {0, 0}
    for index, c in ipairs(conns) do
        local ok, state = pcall(c.eval, c, probe, {}, {timeout = .5})
        if not ok or type(state) ~= 'table' then
            facts[index] = {endpoint = endpoints[index], error = tostring(state)}; ready = false
        else
            state.endpoint = endpoints[index]; facts[index] = state
            local group = math.floor((index - 1) / 3) + 1
            if not state.loaded or not state.local_ok or state.quorum ~= 2 or state.replication_count ~= 3
                or not state.sync or not state.all_sync or state.wal ~= 'fsync' or not state.mvcc
                or not tostring(state.version):match('^3%.9%.') or state.queue.len ~= 0 then ready = false end
            if config.native and (type(state.native) ~= 'table' or state.native_layout ~= 14 or state.native_size ~= 392) then ready = false end
            if not state.ro then
                counts[group] = counts[group] + 1; leaders[group] = endpoints[index]
                if not state.read_ready or config.native and (type(state.native) ~= 'table' or state.native.ready ~= 1) then ready = false end
            end
        end
    end
    ready = ready and counts[1] == 1 and counts[2] == 1
    if not ready then fiber.sleep(.1) end
until ready or fiber.clock() >= deadline
for _, c in ipairs(conns) do c:close() end
print(json.encode{version = 1, status = ready and 'ok' or 'error', leaders = leaders,
    replicas = config.replicas, facts = facts, scope = 'fixture preflight; each read still requires its own strong guard'})
os.exit(ready and 0 or 1)
