-- Vinyl telemetry and storage-shared admission of fresh SQL writes.
-- Credits are an encoded-work estimate, never a native allocator promise.
local clock, ffi, mp = require('clock'), require('ffi'), require('msgpack')
local alloc_ok, alloc = pcall(require, 'internal.alloc')
local MAX = 9007199254740991
local UMAX, IMAX = ffi.new('uint64_t', MAX), ffi.new('int64_t', MAX)
local M = {}
local function number(v)
    if type(v) == 'cdata' then
        if ffi.istype('uint64_t', v) then if v > UMAX then return nil end
        elseif ffi.istype('int64_t', v) then if v < 0 or v > IMAX then return nil end
        else return nil end
        v = tonumber(v) -- only after an exact integer range check
    end
    if type(v) ~= 'number' or v ~= v or v < 0 or v > MAX or v % 1 ~= 0 then return nil end
    return v
end
local function headroom()
    return not alloc_ok or alloc.unused() >= math.max(alloc.getlimit() / 16, 16 * 1048576)
end
local FIELDS = {'dump_bandwidth', 'write_rate', 'rate_limit', 'blocked_writers', 'memory_level0',
    'memory_tuple', 'memory_tx', 'memory_limit', 'compaction_queue', 'run_count', 'range_count', 'runs_per_range'}
local UNKNOWN = {}
function M.new(c)
    local time = c.now or clock.monotonic
    local V = {admitted = 0, rejected = 0, quota_timeouts = 0, active = false, credit = 0, last = time()}
    local native_stats = c.native_stats or function() return box.stat.vinyl() end
    local spaces = c.spaces or function() return box.space end
    local memory_limit = c.memory_limit or function() return box.cfg.vinyl_memory end
    local can_sample = c.can_sample or headroom
    function V.reset()
        V.active, V.credit, V.last = false, 0, time()
        V.cached, V.cache_until = nil, 0
    end
    local function sample()
        local out = {}
        if not can_sample() then return out end
        local ok, s = pcall(native_stats)
        if not ok or type(s) ~= 'table' then return out end
        local r, m, q = s.regulator or {}, s.memory or {}, s.scheduler or {}
        for _, k in ipairs({'dump_bandwidth', 'write_rate', 'rate_limit', 'blocked_writers'}) do out[k] = number(r[k]) end
        out.memory_level0, out.memory_tuple, out.memory_tx = number(m.level0), number(m.tuple), number(m.tx)
        out.memory_limit, out.compaction_queue = number(memory_limit()), number(q.compaction_queue)
        out.dump_watermark = number(r.dump_watermark)
        return out
    end
    function V.stats()
        local s, out = sample(), {}
        for _, k in ipairs(FIELDS) do out[k] = s[k] or box.NULL end
        out.admitted, out.rejected, out.quota_timeouts = V.admitted, V.rejected, V.quota_timeouts
        local runs, ranges, valid = 0, 0, can_sample()
        if valid then
            for id, sp in pairs(spaces()) do
                if type(id) == 'number' and sp.engine == 'vinyl' then
                    for iid, idx in pairs(sp.index) do
                        if type(iid) == 'number' then
                            if not can_sample() then valid = false; break end
                            local ok, stat = pcall(idx.stat, idx)
                            local rn, rg
                            if ok and type(stat) == 'table' then rn, rg = number(stat.run_count), number(stat.range_count) end
                            if not rn or not rg or runs + rn > MAX or ranges + rg > MAX then valid = false; break end
                            runs, ranges = runs + rn, ranges + rg
                        end
                    end
                end
                if not valid then break end
            end
        end
        if valid then
            out.run_count, out.range_count = runs, ranges
            out.runs_per_range = ranges > 0 and runs / ranges or box.NULL
        end
        return out
    end
    -- Test-only leased fixture for the guard sampler. Public telemetry
    -- always reports genuine native statistics, including during a test.
    function V.test_pressure(owner, values, ttl)
        if not c.cfg.test_hooks or type(owner) ~= 'string' or #owner == 0 or #owner > 128 then
            return {'error', 1210, 'Vinyl pressure fixtures require test_hooks and an owner'}
        end
        local now = time()
        if V.fixture and V.fixture.expires > now and V.fixture.owner ~= owner then
            return {'error', 1210, 'Vinyl pressure fixture is owned by another test'}
        end
        if type(values) == 'nil' or values == box.NULL then V.fixture = nil
        else
            ttl = ttl or 5
            if type(values) ~= 'table' or type(ttl) ~= 'number' or ttl ~= ttl or ttl < 1 or ttl > 30 then
                return {'error', 1210, 'Invalid Vinyl pressure fixture'}
            end
            local copy = {}
            for _, k in ipairs({'rate_limit', 'dump_bandwidth', 'blocked_writers', 'memory_level0', 'memory_limit', 'compaction_queue'}) do
                copy[k] = number(values[k])
                if not copy[k] then return {'error', 1210, 'Incomplete Vinyl pressure fixture'} end
            end
            V.fixture = {owner = owner, expires = now + ttl, stats = copy}
        end
        V.reset()
        return {'ok'}
    end
    local function guard_sample()
        if not can_sample() then V.cached = nil; return UNKNOWN end
        if V.fixture then
            if c.cfg.test_hooks and V.fixture.expires > time() then return V.fixture.stats end
            V.fixture = nil; V.reset()
        end
        local now = time()
        if V.cached ~= nil and now < V.cache_until then return V.cached end
        -- Native introspection allocates several tables. Share one bounded
        -- 20ms snapshot across gateways; public telemetry still samples fresh.
        V.cached, V.cache_until = sample(), now + .02
        return V.cached
    end
    local function pressure(s)
        if s.rate_limit == nil or s.dump_bandwidth == nil or s.blocked_writers == nil
           or s.memory_level0 == nil or not s.memory_limit or s.memory_limit == 0 or s.compaction_queue == nil then
            return nil
        end
        if s.rate_limit == 0 then return true end
        local high = math.min(s.dump_watermark or s.memory_limit, s.memory_limit * .75)
        if V.active then
            return not (s.blocked_writers == 0 and s.memory_level0 <= s.memory_limit * .5
                        and s.compaction_queue <= s.memory_limit * .5)
        end
        return s.blocked_writers > 0 or s.memory_level0 >= high or s.compaction_queue >= s.memory_limit
    end
    local function state(s, now)
        local p = pressure(s)
        if p == nil then V.active, V.credit, V.last = false, 0, now; return nil end
        if not p then V.active, V.credit, V.last = false, 0, now; return false end
        if not V.active then V.active, V.credit, V.last = true, 0, now end
        local burst = math.min(s.rate_limit * .05, s.memory_limit / 16, 1048576)
        V.credit = math.min(burst, V.credit + s.rate_limit * math.max(0, now - V.last))
        V.last = now
        return true, burst
    end
    local function refuse(message, delay)
        V.rejected = V.rejected + 1
        return {'throttled', 1205, message, math.max(1, math.min(50, math.ceil(delay or 50)))}
    end
    -- Old-row reads happen only while pressure is engaged, inside the
    -- caller's native transaction. They may yield, before business effects.
    -- An UPDATE packet alone omits all unchanged wide fields.
    local function cost(writes)
        local total = 0
        for _, w in ipairs(writes) do
            local sp = box.space[w[2]]
            if sp and sp.engine == 'vinyl' then
                local packet, old, new = #mp.encode(w), 0, 0
                if w[1] ~= 'insert' then
                    local pk = w[3]
                    if w[1] == 'replace' then pk = c.key_def(sp.id, 0):extract_key(box.tuple.new(w[3])) end
                    local t = sp:get(pk); old = t and t:bsize() or 0
                end
                if w[1] == 'update' then new = old + packet
                elseif w[1] ~= 'delete' then new = #mp.encode(w[3]) end
                local n = 0; for id in pairs(sp.index) do if type(id) == 'number' then n = n + 1 end end
                total = total + (old + new + packet + 256) * math.max(1, n)
                if total > MAX then return MAX end
            end
        end
        return total
    end
    function V.guard(writes)
        local vinyl = false
        for _, w in ipairs(writes) do
            local sp = box.space[w[2]]
            if sp and sp.engine == 'vinyl' then vinyl = true; break end
        end
        if not vinyl then return nil, nil end
        local started = time()
        local deadline = started + c.cfg.vinyl_attempt_timeout
        local s = guard_sample()
        local p = state(s, started)
        if p == nil then return refuse('Vinyl pressure statistics are unavailable'), nil end
        if not p then V.admitted = V.admitted + 1; return nil, deadline end
        local bytes = cost(writes)
        local now = time()
        s = guard_sample() -- another request or a role change may have run while reading old rows
        local active, burst = state(s, now)
        if active == nil then return refuse('Vinyl pressure statistics are unavailable'), nil end
        if active then
            if bytes > burst then return refuse('Vinyl write exceeds the current pressure burst allowance'), nil end
            if V.credit < bytes then
                return refuse('Vinyl writes are throttled while dump or compaction catches up',
                              s.rate_limit > 0 and (bytes - V.credit) * 1000 / s.rate_limit or 50), nil
            end
            V.credit = V.credit - bytes -- no yield between eligibility and the shared debit
        end
        V.admitted = V.admitted + 1
        return nil, deadline
    end
    function V.check_deadline(deadline)
        if deadline and time() >= deadline then
            error(box.error.new{code = 1205, reason = 'Vinyl write attempt deadline exceeded; transaction rolled back'})
        end
    end
    return V
end
return M
