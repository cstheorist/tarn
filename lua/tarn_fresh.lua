-- A request-specific quorum barrier before the first strong read. The
-- public isolation setter collects fresh peer vclocks, catches up local WAL
-- and settles synchronous transactions. We then use confirmed visibility
-- for either engine; direct LINEARIZABLE Vinyl access is unsupported.
local ffi = require('ffi')
local fiber = require('fiber')
local clock = require('clock')
local net_box = require('net.box')
ffi.cdef[[
int box_txn_set_isolation(uint32_t level);
int64_t box_txn_id(void);
typedef struct txn_savepoint box_txn_savepoint_t;
box_txn_savepoint_t *box_txn_savepoint(void);
]]
local M = {}
local MAX_STREAMS = 4096

function M.new(c)
    local F = {streams = {}, count = 0}
    local provider
    local function unavailable(message)
        error(box.error.new{code = box.error.TRANSACTION_CONFLICT,
                            reason = 'strong read unavailable: ' .. message})
    end
    local function key(id)
        return tostring(ffi.cast('uint64_t', id))
    end
    local function current()
        return ffi.C.box_txn_id()
    end
    function F.set_clock(p) provider = p end
    -- Cached-only: native State publication must never call a yielding
    -- certificate refresh. UNKNOWN remains unavailable until wired.
    function F.native_state()
        local read = provider and (provider.read_native_state or provider.native_state)
        if type(read) ~= 'function' then return false, 0, 0, 0, 0 end
        local ok, healthy, mono, wall, gap, drift = pcall(read)
        if not ok or healthy ~= true or not ffi.istype('uint64_t', mono) or not ffi.istype('uint64_t', wall)
           or not ffi.istype('uint64_t', gap) or not ffi.istype('uint64_t', drift) then
            return false, 0, 0, 0, 0
        end
        return true, mono, wall, gap, drift
    end
    function F.clock_check()
        c.writable() -- known demotion must retire the gateway's old connection
        local read = provider and (provider.read_check or provider.check)
        if type(read) ~= 'function' then unavailable('clock health is unknown') end
        local ok, status = pcall(read)
        if not ok then unavailable('clock health check failed') end
        if status ~= nil then
            unavailable(type(status) == 'table' and tostring(status[3] or status[1]) or tostring(status))
        end
    end
    -- Only public configuration/state is inspected here; no data read,
    -- yield or native State access. Anonymous replicas do not count.
    function F.mode()
        local n = 0
        for id in pairs(box.info.replication) do if type(id) == 'number' and id > 0 then n = n + 1 end end
        local replicas = box.cfg.replication
        local configured = type(replicas) == 'table' and next(replicas) ~= nil
                           or type(replicas) == 'string' and replicas ~= ''
        if n <= 1 and not configured then return 1 end
        if not c.cfg.sync or box.cfg.memtx_use_mvcc_engine ~= true then return 3 end
        local q = box.info.synchro.quorum
        local r = box.cfg.replication_linearizable_quorum
        if type(q) ~= 'number' or q <= n / 2 or q > math.max(n, 1) then return 3 end
        if r ~= 'N - Q + 1' and (type(r) ~= 'number' or r + q <= n or r > n) then return 3 end
        return 2
    end
    function F.controls()
        local mode = F.mode()
        if mode == 3 then unavailable('replicated asynchronous spaces or unsupported quorum configuration') end
        if mode == 1 then return mode end
        -- if_not_exists never repairs an old asynchronous control space.
        for _, name in ipairs({'_tarn_meta', '_tarn_catalog', '_tarn_locks', '_tarn_rlocks',
                               '_tarn_buckets', '_tarn_recv', '_tarn_part', '_tarn_txn', '_tarn_alter_undo'}) do
            local sp = box.space[name]
            if sp == nil or not sp.is_sync or not sp.state.is_sync then
                unavailable('control space ' .. name .. ' is not synchronous')
            end
        end
        local scratch = box.space._tarn_scratch
        if scratch == nil or scratch.type ~= 'data-temporary' then unavailable('scratch space is not data-temporary') end
        for id, sp in pairs(box.space) do
            if type(id) == 'number' and sp.name:sub(1, 6) == '_tarn_' and sp.name ~= '_tarn_scratch'
               and (not sp.is_sync or not sp.state.is_sync) then
                unavailable('control or auxiliary space ' .. sp.name .. ' is not synchronous')
            end
        end
        return mode
    end
    local function space_mode(sp, mode)
        if mode == 3 then unavailable('unsupported replicated configuration') end
        if mode == 2 and (not sp.is_sync or not sp.state.is_sync) then
            unavailable('space ' .. sp.name .. ' is not synchronous')
        end
    end
    function F.space(sp) return space_mode(sp, F.mode()) end
    -- F.space with the mode a caller read (F.mode) in the same check, with
    -- no yield since (BK.fresh_spaces).
    function F.space_in(sp, mode) return space_mode(sp, mode) end
    -- The mode belongs only to this nonyielding final check.
    -- Do not retain it across a barrier, data access, yield or invocation.
    function F.plans(plans, space_pos)
        local mode = F.controls()
        c.writable()
        for _, plan in ipairs(plans) do space_mode(plan[space_pos], mode) end
    end
    -- These ordinary Lua guards run through the public Lua C API before
    -- borrowing native State and after a quorum wait. Read current public
    -- configuration/space properties each time,
    -- including the immutable clock-policy match between sampler ticks.
    -- No observation, State publication, data access or yield is allowed.
    local function native_guard(fn, ...)
        local old = box.error.last()
        local ok, result = pcall(fn, ...)
        local restored = pcall(function()
            if old ~= nil then box.error.set(old) else box.error.clear() end
        end)
        return ok and restored and result or 0
    end
    local function controls()
        if provider ~= nil and type(provider.policy_ok) == 'function' and provider.policy_ok() ~= true then return 0 end
        local mode = F.controls()
        return (mode == 1 or mode == 2) and mode or 0
    end
    local function space(sid)
        local sp = box.space[tonumber(sid)]
        if sp == nil then return 0 end
        F.space(sp)
        return 1
    end
    function F.native_controls() return native_guard(controls) end
    function F.native_space(sid) return native_guard(space, sid) end
    -- Guards used by native procedures. They never yield, touch native
    -- State or refresh clock/membership. Both hooks bound registry lifetime
    -- to the exact transaction, including a raw IPROTO COMMIT/ROLLBACK.
    function F.mark(id)
        if not box.is_in_txn() or key(id) ~= key(current()) then return 0 end
        local k = key(id)
        if F.streams[k] then return 1 end
        if F.count >= MAX_STREAMS then return 0 end
        -- Allocate every closure before publishing the token. A protected
        -- native entry can turn a later allocation error into handback,
        -- but cannot reclaim a token whose terminal hooks never existed.
        local function clear()
            if F.streams[k] then
                F.streams[k] = nil
                F.count = F.count - 1
            end
        end
        local rollback_diag
        local function restore_diag()
            if rollback_diag ~= nil then box.error.set(rollback_diag) else box.error.clear() end
        end
        local function rollback_probe()
            -- Legacy rollback hooks also run for savepoints, including a
            -- savepoint that undoes every write. The public savepoint API
            -- refuses a completed rollback with TXN_ROLLBACK, but remains
            -- usable during a partial rollback. It never reads data or yields.
            local sp = ffi.C.box_txn_savepoint()
            local err = box.error.last()
            return sp == nil and err ~= nil and err.code == box.error.TXN_ROLLBACK
        end
        local function rolled_back()
            rollback_diag = box.error.last()
            local ok, terminal = pcall(rollback_probe)
            local restored = pcall(restore_diag)
            -- An uncertain classification must refuse future use of this
            -- stream, rather than retain an unowned certificate.
            if not ok or not restored or terminal then clear() end
        end
        local function rollback_hook()
            local ok = pcall(rolled_back)
            rollback_diag = nil
            if not ok then clear() end
        end
        local function install()
            -- Registration is nonyielding and cannot complete this TX.
            -- A partially registered hook is harmless without a token.
            box.on_commit(clear)
            box.on_rollback(rollback_hook)
            local gen, partial = 0, false
            if c.movement then gen, partial = c.movement() end
            F.streams[k] = {movement = gen, partial = partial}
            F.count = F.count + 1
        end
        if not pcall(install) then
            if F.streams[k] then
                F.streams[k] = nil
                F.count = F.count - 1
            end
            return 0
        end
        -- Reclamation still requires Tarantool to execute the registered
        -- terminal hook; the headroom check is not an allocator reservation.
        return 1
    end
    function F.ok(id)
        return box.is_in_txn() and key(id) == key(current()) and F.streams[key(id)] and 1 or 0
    end
    -- A snapshot opened during physical movement can contain a strict subset
    -- of the bucket even after publication. Its original quorum certificate
    -- remains valid, but it cannot be used as a complete movement view.
    function F.move_safe()
        if not c.movement or not box.is_in_txn() then return true end
        local token = F.streams[key(current())]
        if not token then return true end -- raw streams are refused by F.stream
        local gen = c.movement()
        return not token.partial and token.movement == gen
    end
    function F.native_mark(id) return native_guard(F.mark, id) end
    local function native_ok(id)
        if not F.move_safe() then return 0 end -- Lua returns the movement refusal
        return F.ok(id)
    end
    function F.native_ok(id) return native_guard(native_ok, id) end
    function F.stream()
        c.writable()
        if F.ok(current()) ~= 1 then unavailable('ambient stream has no first-read freshness proof') end
    end
    local function begin(timeout, recovery)
        c.writable()
        if box.is_in_txn() then unavailable('freshness barrier cannot replace an ambient transaction') end
        F.controls()
        box.begin{txn_isolation = 'linearizable', timeout = timeout}
        local ok, err = pcall(function()
            if ffi.C.box_txn_set_isolation(2) ~= 0 then error(box.error.last()) end
            if not recovery then F.clock_check() end
            F.controls() -- schema/configuration may have changed while waiting
            c.writable()
        end)
        if not ok then
            if box.is_in_txn() then box.rollback() end
            error(err)
        end
    end
    function F.barrier(timeout, recovery)
        c.writable()
        if box.is_in_txn() then unavailable('freshness barrier cannot replace an ambient transaction') end
        if not recovery then F.clock_check() end
        begin(timeout or c.cfg.read_freshness_timeout, recovery)
        -- No data was read, and confirmed prefixes survive this rollback.
        box.rollback()
    end
    -- The lock calls' barrier without the synchronous queue's wait (issue
    -- #314; tla/StrongReadFreshness.tla, OwnerBarrier with LockCall): the
    -- vclocks of a quorum of the shard's replicas, asked after the call
    -- began, joined, and the local WAL caught up to them. The queue's wait
    -- (txn_limbo_wait_last_txn) adds nothing on its writable owner: a new
    -- leader's PROMOTE settles the queue before the leader can write, and a
    -- newer term's PROMOTE makes an instance read-only before that term's
    -- rows reach its WAL (PromoteSettles, TermBeforeRows). The queue then
    -- holds only the owner's own transactions whose commits haven't
    -- returned. The caller checks that it is still writable afterwards
    -- (pl_begin, pl_intact).
    -- One round of calls serves every barrier that began before the round
    -- did, as the relays' vclock sync serves concurrent native barriers. It
    -- asks as many replicas as a quorum needs, those that answered the last
    -- round first, and the others once those haven't answered within a few
    -- round trips. A replica without tarn_peer_vclock (a rolling upgrade)
    -- sends the caller to the full barrier. (The state is in fields:
    -- jit_upvalue_test.lua.)
    local OB = {started = 0, done = 0, want = 0, result = nil, conns = {}, rounds = 0, fallbacks = 0,
                asked = 0, prefer = {}, rtt = nil, patience = 0.005, cond = fiber.cond(), wake = fiber.cond()}
    F.ob = OB
    -- The read quorum counting this instance (F.mode: N - Q + 1, or the
    -- configured number) and the replica set's registered members.
    local function read_quorum()
        local members, n = {}, 0
        for _, t in box.space._cluster:pairs() do members[t[2]] = true; n = n + 1 end
        local r = box.cfg.replication_linearizable_quorum
        if type(r) ~= 'number' then r = n - box.info.synchro.quorum + 1 end
        return r, members
    end
    local function connection(uri)
        local conn = OB.conns[uri]
        if conn ~= nil and (conn.state == 'error' or conn.state == 'closed') then
            pcall(conn.close, conn)
            conn = nil
        end
        if conn == nil then
            conn = net_box.connect(c.cfg.resolve_uri and c.cfg.resolve_uri(uri) or uri,
                                   {fetch_schema = false, user = c.cfg.user, password = c.cfg.password,
                                    wait_connected = false, reconnect_after = 0.5})
            OB.conns[uri] = conn
        end
        return conn
    end
    -- A reply's vclock without component 0, or nil if any entry is not a
    -- replica id and an LSN (the reply then doesn't count).
    local function vclock_of(v)
        if type(v) ~= 'table' then return nil end
        local out = {}
        for id, lsn in pairs(v) do
            if type(id) ~= 'number' then return nil end
            if type(lsn) == 'number' or ffi.istype('int64_t', lsn) or ffi.istype('uint64_t', lsn) then
                if id ~= 0 then out[id] = lsn end
            elseif lsn ~= box.NULL then   -- (box.NULL: a hole the encoder left in a sparse array)
                return nil
            end
        end
        return out
    end
    local function ask(uri, deadline, replies)
        local ok, res = pcall(function()
            local conn = connection(uri)
            return conn:call('tarn_peer_vclock', {}, {timeout = math.max(deadline - clock.monotonic(), 0.001)})
        end)
        replies:put({uri = uri, ok = ok, res = res}, 0)
    end
    -- One round: {joined = vclock} from a quorum, or {failed = reason}.
    local function round(deadline)
        local r, members = read_quorum()
        if r <= 1 then return {joined = {}} end
        OB.own = OB.own or box.info.uuid
        OB.rs = OB.rs or box.info.replicaset.uuid
        local listen = c.own_uris and c.own_uris() or {}
        local peers, later = {}, {}
        for _, uri in ipairs(c.peers and c.peers() or {}) do
            if not listen[(uri:gsub('^[^@]*@', ''))] then
                table.insert(OB.prefer[uri] and peers or later, uri)
            end
        end
        for _, uri in ipairs(later) do peers[#peers + 1] = uri end
        local replies = fiber.channel(#peers + 1)
        local asked = 0
        local function more(k)
            for _ = 1, k do
                if asked >= #peers then return end
                asked = asked + 1
                OB.asked = OB.asked + 1
                fiber.new(ask, peers[asked], deadline, replies)
            end
        end
        local start = clock.monotonic()
        more(r - 1)
        local seen, got, joined, unsupported, received, from = {}, 0, {}, false, 0, {}
        while received < asked do
            local now = clock.monotonic()
            local left = deadline - now
            if asked < #peers then left = math.min(left, start + OB.patience - now) end
            local m = left > 0 and replies:get(left) or nil
            if m == nil then
                if asked >= #peers or clock.monotonic() >= deadline then break end
                more(#peers)   -- those asked are slow: ask the rest
            else
                received = received + 1
                local v = m.ok and m.res
                local vclock = type(v) == 'table' and vclock_of(v.vclock)
                if vclock and v.replicaset == OB.rs and v.uuid ~= OB.own and members[v.uuid] and not seen[v.uuid] then
                    seen[v.uuid] = true
                    got = got + 1
                    from[m.uri] = true
                    for id, lsn in pairs(vclock) do
                        if (joined[id] or 0) < lsn then joined[id] = lsn end
                    end
                    if got + 1 >= r then
                        local rtt = clock.monotonic() - start
                        OB.rtt = OB.rtt and 0.8 * OB.rtt + 0.2 * rtt or rtt
                        OB.patience = math.min(math.max(4 * OB.rtt, 0.002), 0.02)
                        OB.prefer = from
                        return {joined = joined}
                    end
                else
                    if not m.ok and type(m.res) == 'cdata'
                       and (m.res.code == box.error.NO_SUCH_PROC or m.res.code == box.error.ACCESS_DENIED) then
                        unsupported = true
                    end
                    more(1)   -- in its place
                end
            end
        end
        return {failed = unsupported and 'unsupported' or 'timeout'}
    end
    local function rounds()
        while true do
            if OB.want > OB.done then
                local n = OB.done + 1
                OB.started = n
                local ok, res = pcall(round, clock.monotonic() + c.cfg.read_freshness_timeout)
                OB.result = ok and res or {failed = 'error'}
                OB.done = n
                OB.rounds = OB.rounds + 1
                OB.cond:broadcast()
            else
                OB.wake:wait()
            end
        end
    end
    local function covered(joined)
        local v = box.info.vclock
        for id, lsn in pairs(joined) do
            if (v[id] or 0) < lsn then return false end
        end
        return true
    end
    function F.owner_barrier(timeout, recovery)
        c.writable()
        if box.is_in_txn() then unavailable('freshness barrier cannot replace an ambient transaction') end
        if not recovery then F.clock_check() end
        local deadline = clock.monotonic() + (timeout or c.cfg.read_freshness_timeout)
        if F.controls() == 2 then
            local res
            repeat
                -- A round that starts after now (a failed one: the next).
                local need = OB.started + 1
                if OB.want < need then OB.want = need end
                if OB.worker == nil or OB.worker:status() == 'dead' then
                    OB.worker = fiber.new(rounds)
                    OB.worker:name('tarn_owner_barrier')
                end
                OB.wake:signal()
                while OB.done < need do
                    local left = deadline - clock.monotonic()
                    if left <= 0 then box.error(box.error.TIMEOUT) end
                    OB.cond:wait(left)
                end
                res = OB.result
                if res.failed == 'unsupported' then
                    OB.fallbacks = OB.fallbacks + 1
                    return F.barrier(math.max(deadline - clock.monotonic(), 0.001), recovery)
                end
                if res.joined == nil then
                    if clock.monotonic() + 0.01 >= deadline then box.error(box.error.TIMEOUT) end
                    fiber.sleep(0.01)
                end
            until res.joined ~= nil
            while not covered(res.joined) do
                if clock.monotonic() >= deadline then box.error(box.error.TIMEOUT) end
                fiber.sleep(0.001)
            end
        end
        if not recovery then F.clock_check() end
        F.controls() -- schema/configuration may have changed while waiting
        c.writable()
    end
    function F.begin_stream()
        c.writable()
        if box.is_in_txn() then unavailable('first read cannot replace an ambient transaction') end
        F.clock_check()
        begin(60) -- timeout cannot be extended after the quorum wait yields
        if F.mark(current()) ~= 1 then box.rollback(); unavailable('too many active fresh streams') end
    end
    function F.read(first, checked)
        c.writable()
        if box.is_in_txn() then
            F.stream()
            F.clock_check()
            F.controls()
            c.writable()
        elseif first == true then F.begin_stream()
        elseif checked == true then F.barrier()
        else unavailable('stream transaction no longer exists') end
    end
    -- Final validation needs a current prefix, independently of the first
    -- snapshot's proof: an isolated former leader may not yet know a newer
    -- term or the writes ACKed by its successor. Never refresh this caller's
    -- isolation/read view. A separate fiber owns only an empty barrier.
    function F.fence_stream()
        F.stream()
        F.clock_check()
        local mode, original = F.controls(), current()
        if mode ~= 2 then return end
        local child = fiber.new(function() F.barrier(); return true end)
        child:set_joinable(true)
        local ok, result = child:join()
        if not ok then error(result) end
        c.writable()
        if current() ~= original or F.ok(original) ~= 1 then
            unavailable('final prefix lost the original stream transaction')
        end
        F.clock_check()
        F.controls()
        c.writable()
    end
    return F
end
return M
