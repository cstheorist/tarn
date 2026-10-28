-- A request-specific quorum barrier before the first strong read. The
-- public isolation setter collects fresh peer vclocks, catches up local WAL
-- and settles synchronous transactions. We then use confirmed visibility
-- for either engine; direct LINEARIZABLE Vinyl access is unsupported.
local ffi = require('ffi')
local fiber = require('fiber')
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
    function F.space(sp)
        local mode = F.mode()
        if mode == 3 then unavailable('unsupported replicated configuration') end
        if mode == 2 and (not sp.is_sync or not sp.state.is_sync) then
            unavailable('space ' .. sp.name .. ' is not synchronous')
        end
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
            F.streams[k] = true
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
    function F.native_mark(id) return native_guard(F.mark, id) end
    function F.native_ok(id) return native_guard(F.ok, id) end
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
