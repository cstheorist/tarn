-- RecordExpiryClock / RecordExpiryPolicy refinements (#130).
-- This detects local clock discontinuities; it does NOT establish an initial
-- clock bound, timestamp provenance, or historical cross-node rate bounds.
-- A trusted, bounded peer sampler supplies certify()'s interval evidence.
-- Call invalidate() before activating changed membership or a new local role.
local clock, fiber, ffi = require('clock'), require('fiber'), require('ffi')
local M = {}
local KEY = 'clock:policy:1'
local BILLION = 1000000000
-- Seconds stay within uint64 nanoseconds (and ordinary double arithmetic).
local MAX = 9007199254
local function finite(n)
    return type(n) == 'number' and n == n and n >= 0 and n <= MAX
end
local function unavailable(reason)
    -- Neither YES nor NO: in particular, never create a refusal tombstone.
    return {'clock_unknown', 1213, 'Clock safety unavailable: ' .. reason}
end
local function clone(p)
    return {version = 1, max_age = p.max_age, skew = p.skew,
            max_gap = p.max_gap, max_drift = p.max_drift,
            high = p.high, created_wall = p.created_wall, fault = p.fault}
end

function M.new(c)
    assert(type(c) == 'table' and type(c.cfg) == 'table', 'clock guard requires cfg')
    local C = {epoch = 0, high = 0, loaded = false, state = 'unknown',
               reason = 'policy not loaded', pending = 0, fault_pending = false}
    local policy, last_wall, last_mono, last_wall64, last_mono64, certificate
    local fake, worker
    local function meta() return c.meta or box.space._tarn_meta end
    local function cfg()
        local function setting(name, default)
            local v = c.cfg[name]
            return v == nil and default or v
        end
        local p = {max_age = setting('max_txn_age', 120), skew = setting('clock_skew', 60),
                   max_gap = setting('clock_max_gap', 2), max_drift = setting('clock_max_drift', .25)}
        if not finite(p.max_age) or p.max_age <= 0 or not finite(p.skew)
           or not finite(p.max_gap) or p.max_gap <= 0 or not finite(p.max_drift)
           or p.max_drift >= p.max_gap or p.max_age + 2 * p.skew > MAX then return nil end
        return p
    end
    local function matches(p, q)
        return type(p) == 'table' and q and p.version == 1
               and finite(p.max_age) and p.max_age > 0 and finite(p.skew)
               and finite(p.max_gap) and p.max_gap > 0 and finite(p.max_drift)
               and p.max_drift < p.max_gap and p.max_age + 2 * p.skew <= MAX
               and (p.fault == nil or type(p.fault) == 'string')
               and p.max_age == q.max_age and p.skew == q.skew
               and p.max_gap == q.max_gap and p.max_drift == q.max_drift
               and finite(p.high) and finite(p.created_wall)
    end
    local function publish()
        if c.publish then pcall(c.publish) end -- nonyielding mirror update only
    end
    local function unknown(reason)
        if C.state == 'healthy' then C.epoch = C.epoch + 1 end
        certificate = nil
        C.state, C.reason = C.fault and 'fault' or 'unknown', C.fault or reason
        publish()
        return unavailable(C.reason)
    end
    function C.fault_now(reason)
        if not C.fault then
            C.fault, C.fault_pending = tostring(reason), true
            C.epoch = C.epoch + 1
        end
        return unknown(C.fault)
    end
    function C.invalidate(reason)
        C.epoch = C.epoch + 1
        C.loaded = false
        return unknown(reason or 'role or membership changed')
    end
    local function sample()
        if fake then
            return fake.wall, fake.mono, ffi.new('uint64_t', math.floor(fake.wall * BILLION)),
                   ffi.new('uint64_t', math.floor(fake.mono * BILLION))
        end
        local m, w = clock.monotonic64(), clock.realtime64()
        return tonumber(w) / BILLION, tonumber(m) / BILLION, w, m
    end
    -- No space reads/writes, yields, or transaction ownership changes.
    function C.observe()
        local q = cfg()
        if not q then return C.fault_now('invalid timing configuration') end
        if policy and not matches(policy, q) then return C.fault_now('timing policy changed') end
        local w, m, w64, m64 = sample()
        if not finite(w) or not finite(m) then return C.fault_now('invalid clock reading') end
        if last_mono ~= nil then
            local dm, dw = m - last_mono, w - last_wall
            if dm < 0 or dw < 0 then return C.fault_now('clock moved backward') end
            if dm > q.max_gap then return C.fault_now('clock monitor gap exceeded') end
            if math.abs(dw - dm) > q.max_drift then return C.fault_now('wall/monotonic drift exceeded') end
        end
        if policy and w + q.max_drift < math.max(C.high, policy.created_wall) then
            return C.fault_now('clock precedes persisted high-water mark')
        end
        last_wall, last_mono, last_wall64, last_mono64 = w, m, w64, m64
        if C.fault then return unavailable(C.fault) end
        return nil
    end

    -- Own transaction ONLY; never read metadata through an ambient stream.
    -- A successful box.commit is the durability boundary, not replace().
    function C.load()
        if box.is_in_txn() or C.pending ~= 0 then return false, unavailable('policy load needs its own transaction') end
        local load_epoch = C.epoch
        local e = C.observe()
        local q = cfg()
        if not q then return false, e end
        local space = meta()
        if not space then return false, unknown('policy space unavailable') end
        local ok, p = pcall(function()
            box.begin{txn_isolation = 'read-confirmed'}
            local t = space:get{KEY}
            if not t then
                if box.info.ro then box.rollback(); return nil end
                local new = {version = 1, max_age = q.max_age, skew = q.skew,
                             max_gap = q.max_gap, max_drift = q.max_drift,
                             high = 0, created_wall = last_wall or 0, fault = C.fault}
                space:insert{KEY, new}
                box.commit()
                return new
            end
            local value = t.value or t[2]
            box.rollback()
            return value
        end)
        if not ok then
            if box.is_in_txn() then box.rollback() end
            return false, unknown('policy load failed')
        end
        if not p then return false, unknown('policy not installed') end
        if not matches(p, q) then
            if matches(p, p) then policy = clone(p); C.high = math.max(C.high, p.high) end
            return false, C.fault_now('persisted timing policy mismatch')
        end
        policy = clone(p)
        C.high = math.max(C.high, p.high)
        C.loaded = true
        if p.fault then C.fault, C.fault_pending = p.fault, false end
        if C.epoch ~= load_epoch then
            C.loaded = false
            return false, unknown('role or clock epoch changed during policy load')
        end
        if C.observe() then return false, unavailable(C.reason) end
        if C.fault then return false, unavailable(C.fault) end
        -- Self-certification is restricted to RF1. Its initial clock and
        -- every external timestamp issuer remain explicitly trusted.
        if not c.cfg.sync and not box.info.ro then
            certificate = {epoch = C.epoch, until_mono = last_mono + policy.max_gap}
            C.state, C.reason = 'healthy', nil
            publish()
        end
        return true
    end

    -- Persist a detected fault in a separate worker/call, never the user's
    -- transaction. If quorum/role prevents this, volatile fault stays closed;
    -- after restart the confirmed policy/H plus UNKNOWN bootstrap still apply.
    function C.flush_fault()
        if not C.fault_pending or not C.fault or box.is_in_txn() or box.info.ro or not meta() then return false end
        local ok = pcall(function()
            box.begin{txn_isolation = 'read-confirmed'}
            local t = meta():get{KEY}
            if not t then box.rollback(); return false end
            local current = t.value or t[2]
            if not matches(current, policy) then box.rollback(); return false end
            local p = clone(current)
            p.high, p.fault = math.max(p.high, C.high), p.fault or C.fault
            meta():replace{KEY, p}
            box.commit()
            C.fault_pending = false
            return true
        end)
        if box.is_in_txn() then box.rollback() end
        return ok and not C.fault_pending
    end

    -- An unguarded raw probe must remain callable while health is UNKNOWN.
    -- No policy installation, peer RPC or strong-read barrier is performed.
    function C.probe()
        local e = C.observe()
        local w, m = sample()
        return {wall = w, mono = m, epoch = C.epoch, high = C.high,
                max_age = policy and policy.max_age or nil, skew = policy and policy.skew or nil,
                -- Peer.state=healthy would circularly deadlock bootstrap.
                local_ok = e == nil and C.loaded and not C.fault,
                fault = C.fault, state = C.state}
    end
    -- Each interval bounds remote_wall - local_wall at the trusted sampler's
    -- bounded request/response observation. Coverage of ALL required peers,
    -- authentication, initial/issuer bounds and rate assumptions are sampler
    -- obligations, not established by this arithmetic check.
    function C.certify(proof)
        local e = C.observe()
        if e then return e end
        if box.info.ro then return unknown('clock owner is read-only') end
        if not C.loaded or not policy then return unknown('policy not loaded') end
        if type(proof) ~= 'table' or proof.epoch ~= C.epoch
           or not finite(proof.started) or proof.started > last_mono
           or last_mono - proof.started > policy.max_gap
           or type(proof.peers) ~= 'table' then return unknown('clock certificate unavailable or stale') end
        local expected = c.required_peers and c.required_peers() or nil
        if expected ~= nil and type(expected) ~= 'table' then return unknown('invalid required clock peers') end
        if c.cfg.sync and (type(expected) ~= 'table' or #expected == 0) then
            return unknown('required clock peers unavailable')
        end
        local seen = {}
        for _, p in ipairs(proof.peers) do
            if type(p) ~= 'table' or type(p.id) ~= 'string' or seen[p.id]
               or type(p.low) ~= 'number' or type(p.high) ~= 'number'
               or p.low ~= p.low or p.high ~= p.high or p.low > p.high
               or p.max_age ~= policy.max_age or p.skew ~= policy.skew then
                return unknown('invalid peer clock interval or policy')
            end
            if p.local_ok ~= true or p.fault ~= nil then return unknown('peer clock monitor not healthy') end
            if p.high < -policy.skew or p.low > policy.skew then
                return C.fault_now('peer clock bound violated')
            end
            if p.low < -policy.skew or p.high > policy.skew then
                return unknown('peer clock bound not certified')
            end
            seen[p.id] = true
        end
        for _, id in ipairs(expected or {}) do
            if not seen[id] then return unknown('required clock peer unreachable') end
        end
        certificate = {epoch = C.epoch, until_mono = proof.started + policy.max_gap}
        C.state, C.reason = 'healthy', nil
        publish()
        return nil
    end
    function C.check()
        local e = C.observe()
        if e then return e end
        if box.info.ro then return unknown('clock owner is read-only') end
        if not C.loaded or not certificate or certificate.epoch ~= C.epoch
           or last_mono > certificate.until_mono then return unknown('clock certification expired') end
        return nil
    end
    function C.admit(ts, epoch)
        local e = C.check()
        if e then return e end
        if epoch ~= nil and epoch ~= C.epoch then return unavailable('clock epoch changed during admission') end
        if not finite(ts) then return unavailable('finite coordinator timestamp required') end
        if ts > last_wall + policy.skew then return unavailable('coordinator timestamp exceeds clock bound') end
        if math.max(last_wall, C.high) - ts > policy.max_age then return {'expired', 1213, 'transaction too old'} end
        return nil, C.epoch
    end
    -- Finishing an already decided operation must never be gated on health.
    function C.now()
        local w = sample()
        return math.max(finite(w) and w or (last_wall or 0), C.high)
    end
    function C.expiry()
        local p = policy or cfg() or {max_age = 120, skew = 60}
        return C.now() + p.max_age + 2 * p.skew
    end
    function C.gc_enter()
        local e = C.check()
        if e then return nil, e end
        return {epoch = C.epoch}
    end
    -- Call immediately BEFORE deletes, inside the SAME transaction. Failure
    -- is returned to the owner, which must roll back the whole GC chunk.
    function C.gc_commit(ticket)
        if not box.is_in_txn() then return unavailable('GC requires its deletion transaction') end
        local e = C.check()
        if e then return e end
        if type(ticket) ~= 'table' or ticket.epoch ~= C.epoch then return unavailable('GC clock epoch changed') end
        local t = meta():get{KEY}
        local p = t and (t.value or t[2])
        if not matches(p, policy) or p.fault then return C.fault_now('GC timing policy changed') end
        local high = math.max(C.high, p.high, last_wall)
        local next_policy = clone(p); next_policy.high = high
        local armed = false
        local function finish(committed)
            if not armed then return end
            armed = false
            C.pending = C.pending - 1
            if committed then C.high = math.max(C.high, high) end
        end
        -- Both callbacks and their captures exist BEFORE metadata mutation.
        box.on_commit(function() finish(true) end)
        box.on_rollback(function() finish(false) end)
        C.pending, armed = C.pending + 1, true
        -- Conservative prepublication: rollback need not lower a rejection
        -- floor. Rebuild/reload never lowers this live instance's high-water.
        C.high = high
        meta():replace{KEY, next_policy}
        return nil
    end
    function C.native_state()
        local ok = C.state == 'healthy' and C.loaded and not C.fault and certificate
                   and certificate.epoch == C.epoch and last_mono ~= nil
                   and last_mono <= certificate.until_mono
        if not ok then return false, 0ULL, 0ULL, 0ULL, 0ULL end
        -- Sampling the local clock must NEVER renew a peer certificate.
        -- A sampler may be awaiting replies while native requests run.
        local gap = math.min(policy.max_gap, certificate.until_mono - last_mono)
        return true, last_mono64, last_wall64, ffi.new('uint64_t', math.floor(gap * BILLION)),
               ffi.new('uint64_t', math.floor(policy.max_drift * BILLION))
    end
    function C.status()
        return {state = C.state, reason = C.reason, fault = C.fault, fault_pending = C.fault_pending,
                epoch = C.epoch, high = C.high, loaded = C.loaded, pending = C.pending,
                wall = last_wall, mono = last_mono, policy = policy and clone(policy) or nil,
                certified_until = certificate and certificate.until_mono or nil}
    end
    function C.tick()
        C.observe()
        if not C.loaded then C.load() end
        if C.fault then C.flush_fault(); return false end
        if C.loaded then
            if box.info.ro then unknown('clock owner is read-only'); return false end
            if not c.cfg.sync then
                certificate = {epoch = C.epoch, until_mono = last_mono + policy.max_gap}
                C.state, C.reason = 'healthy', nil; publish()
            elseif c.certifier then
                local ok, proof = pcall(c.certifier, C.probe())
                if ok then C.certify(proof) else unknown('clock peer sampling failed') end
            end
        end
        return C.check() == nil
    end
    function C.start()
        if worker then return end
        worker = fiber.create(function()
            fiber.name('tarn_clock')
            while true do
                local ok = pcall(C.tick)
                if not ok then unknown('clock monitor failed') end
                fiber.sleep(.1)
            end
        end)
    end
    function C.stop()
        if worker then worker:cancel(); worker = nil end
    end
    function C.test_time(wall, mono)
        if c.cfg.test_hooks ~= true then return {'error', 1210, 'Clock injection requires test_hooks'} end
        if not finite(wall) or not finite(mono) then return {'error', 1210, 'Invalid test clock'} end
        fake = {wall = wall, mono = mono}
        return {'ok'}
    end
    if rawget(_G, 'jit') then jit.off(C.gc_commit, true) end
    return C
end

return M
