-- Conservative admission cache for LazyMeter.tla. Durable outbox entries
-- and per-row PREPARE promises are the resource truth. A SQL transaction
-- never reads or rewrites a shared usage tuple: that would make disjoint
-- read-confirmed transactions conflict while another writer awaits WAL.
local M = {}
local MAX = 4503599627370495 -- exact arithmetic, including temporary APPLY debt
local MAX_LIMIT = 1125899906842623

local function fail(code, message)
    error(box.error.new{code = code, reason = message})
end

local function integer(n)
    return type(n) == 'number' and n >= -MAX and n <= MAX and n % 1 == 0
end

function M.new(c)
    assert(type(c.cut) == 'function', 'lazy meter requires a native quiescence cut')
    local q = {epoch = 0, outstanding = 0, tickets = {}, current = nil, building = false}

    function q.invalidate()
        q.epoch = q.epoch + 1
        if q.current ~= nil then q.current.ready = false end
    end

    function q.ready()
        local u = q.current
        return u ~= nil and u.ready and u.epoch == q.epoch and not box.info.ro
    end

    local function limit(name, default)
        local n = c.cfg[name]
        if n == nil then n = default end
        if not integer(n) or n < 0 or n > MAX_LIMIT then
            fail(1210, 'Invalid lazy capacity limit: ' .. name)
        end
        return n
    end

    local function finish(t, committed)
        if t.done or not t.armed then return end
        t.done = true
        local u = t.meter -- captured epoch: never debit a replacement meter
        for i = 1, 4 do
            if committed then
                u.committed[i] = u.committed[i] + t.net[i]
                u.charged[i] = u.charged[i] - t.negative[i]
            else
                u.charged[i] = u.charged[i] - t.positive[i]
            end
            if u.charged[i] < 0 or u.committed[i] < 0 or u.charged[i] < u.committed[i] then
                -- A broken refinement must stop admission before another
                -- fiber can observe the undercount. No space reads or yields
                -- are allowed in these terminal callbacks.
                u.ready = false
                q.invalidate()
            end
        end
        q.tickets[t.id] = nil
        q.outstanding = q.outstanding - 1
    end

    -- de/db: outbox entries/encoded bytes; dre/drb: reserved entries/bytes.
    -- Call BEFORE the corresponding durable mutation. An existing promise
    -- permits APPLY's positive allocation even if limits were lowered or
    -- its retained reservation temporarily makes charged usage exceed them.
    function q.change(de, db, dre, drb, promised)
        if not q.ready() then fail(1213, 'Lazy admission meter is rebuilding; retry') end
        if not box.is_in_txn() then fail(1210, 'Lazy capacity mutation requires a transaction') end
        local delta = {de, db, dre, drb}
        local u = q.current
        for i = 1, 4 do
            if not integer(delta[i]) or math.abs(delta[i]) > MAX_LIMIT
               or u.charged[i] + math.max(delta[i], 0) > MAX then
                fail(1041, 'Lazy capacity accounting exhausted')
            end
        end
        if not promised and (de > 0 or db > 0 or dre > 0 or drb > 0) then
            local entries = u.charged[1] + u.charged[3] + math.max(de, 0) + math.max(dre, 0)
            local bytes = u.charged[2] + u.charged[4] + math.max(db, 0) + math.max(drb, 0)
            if entries > limit('lazy_outbox_entries', 65536)
               or bytes > limit('lazy_outbox_bytes', 64 * 1024 * 1024) then
                fail(1041, 'Lazy outbox admission limit exceeded')
            end
        end
        local id = box.txn_id()
        if type(id) ~= 'number' then id = tostring(id) end
        local t = q.tickets[id]
        if t == nil then
            t = {id = id, meter = u, positive = {0, 0, 0, 0}, negative = {0, 0, 0, 0},
                 net = {0, 0, 0, 0}, done = false, armed = false, counted = false}
            -- Allocate/register BOTH terminal paths before charging. A
            -- detached sync waiter may exit before its actual transaction
            -- ends: this ticket survives until a terminal callback runs.
            local ok, err = pcall(function()
                box.on_commit(function() finish(t, true) end)
                box.on_rollback(function() finish(t, false) end)
                q.tickets[id] = t
                q.outstanding = q.outstanding + 1
                t.counted, t.armed = true, true
            end)
            if not ok then
                t.done = true
                q.tickets[id] = nil
                if t.counted then q.outstanding = q.outstanding - 1 end
                error(err)
            end
        elseif t.meter ~= u then
            fail(1213, 'Lazy admission epoch changed; retry')
        end
        for i = 1, 4 do
            local p, n = math.max(delta[i], 0), math.max(-delta[i], 0)
            t.positive[i] = t.positive[i] + p
            t.negative[i] = t.negative[i] + n
            t.net[i] = t.net[i] + delta[i]
            u.charged[i] = u.charged[i] + p
        end
    end

    function q.snapshot()
        if not q.ready() then return nil end
        local n = q.current.committed
        return {entries = n[1], bytes = n[2], reserved_entries = n[3], reserved_bytes = n[4]}
    end

    local function native_cut()
        if box.info.ro or q.outstanding ~= 0 or not c.cut() then return false end
        local queue = box.info.synchro.queue
        if queue.len ~= 0 or queue.busy then return false end
        if c.cfg.sync and queue.owner ~= box.info.id then return false end
        return true
    end

    local function scan()
        local n = {0, 0, 0, 0}
        box.begin{txn_isolation = 'read-confirmed'}
        local ok, err = pcall(function()
            for _, row in box.space._tarn_lazy_outbox:pairs() do
                n[1], n[2] = n[1] + 1, n[2] + row.bytes
            end
            for _, row in box.space._tarn_lazy_reservation:pairs() do
                n[3], n[4] = n[3] + row.entries, n[4] + row.bytes
            end
        end)
        if box.is_in_txn() then box.rollback() end
        if not ok then error(err) end
        return n
    end

    -- Separate background worker only. Never wait for the caller's own
    -- INFLIGHT marker inside a commit path. Admission remains UNKNOWN until
    -- the confirmed snapshot and the current native/leadership cut agree.
    function q.rebuild()
        if q.ready() then return true end
        if q.building or box.is_in_txn() or not native_cut() then return false end
        q.building = true
        local epoch = q.epoch
        local ok, result = pcall(function()
            local n = (c.scan or scan)()
            for i = 1, 4 do
                if not integer(n[i]) or n[i] < 0 then fail(1041, 'Invalid durable lazy capacity total') end
            end
            local next_meter = {epoch = epoch, ready = true, committed = n,
                                charged = {n[1], n[2], n[3], n[4]}}
            local function install()
                if epoch ~= q.epoch or not native_cut() then return false end
                q.current = next_meter
                return true
            end
            if c.atomic then return c.atomic('LazyMeterInstall', install) end
            return install()
        end)
        q.building = false
        if not ok then error(result) end
        return result
    end

    -- Trace compilation can allocate on a loop backedge. Once a ticket is
    -- registered, the paired arithmetic updates must finish without such
    -- an allocation failure between a refund record and its actual charge.
    if rawget(_G, 'jit') ~= nil then
        jit.off(q.change, true)
        jit.off(finish, true)
    end

    return q
end

return M
