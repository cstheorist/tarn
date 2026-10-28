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
local function unsigned_clock(v)
    if ffi.istype('uint64_t', v) then return v end
    -- Tarantool3.9 public clock.*64 returns signed nanoseconds. Validate
    -- before casting; never reinterpret a negative clock as a huge uint64.
    if ffi.istype('int64_t', v) and v >= 0 then return ffi.cast('uint64_t', v) end
    return nil
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
    local fake, worker, attached, on_policy
    local certificates = {}
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
        C.local_ready = false
        certificates = {}
        return unknown(C.fault)
    end
    function C.invalidate(reason)
        C.epoch = C.epoch + 1
        C.loaded = false
        C.local_ready = false
        certificates = {}
        return unknown(reason or 'role or membership changed')
    end
    function C.attach()
        local space = meta()
        if not space or attached == space then return end
        if attached then pcall(attached.on_replace, attached, nil, on_policy) end
        on_policy = on_policy or function(_, new)
            if new == nil or new[1] ~= KEY then return end
            local p = new[2]
            -- A replica may receive/confirm GC after load() observed an
            -- older confirmed prefix. Conservatively mirror pending H as
            -- well: rollback may over-refuse, but can never lower this floor.
            if type(p) == 'table' and finite(p.high) then C.high = math.max(C.high, p.high) end
            if policy and not matches(p, policy) then C.fault_now('replicated timing policy changed')
            elseif type(p) == 'table' and p.fault then C.fault_now(p.fault) end
        end
        space:on_replace(on_policy)
        attached = space
    end
    local function sample()
        if fake then
            return fake.wall, fake.mono, ffi.new('uint64_t', math.floor(fake.wall * BILLION)),
                   ffi.new('uint64_t', math.floor(fake.mono * BILLION))
        end
        local m, w = unsigned_clock(clock.monotonic64()), unsigned_clock(clock.realtime64())
        if m == nil or w == nil then return -1, -1, 0ULL, 0ULL end
        return tonumber(w) / BILLION, tonumber(m) / BILLION, w, m
    end
    -- No space reads/writes, yields, or transaction ownership changes.
    function C.observe()
        if c.can_run and not c.can_run() then return unavailable('clock monitor heap pressure') end
        local q = cfg()
        if not q then return C.fault_now('invalid timing configuration') end
        if policy and not matches(policy, q) then return C.fault_now('timing policy changed') end
        local w, m, w64, m64 = sample()
        if not finite(w) or not finite(m) then return C.fault_now('invalid clock reading') end
        if last_mono ~= nil then
            local dm, dw = m - last_mono, w - last_wall
            if dm < 0 or dw < 0 then return C.fault_now('clock moved backward') end
            if math.abs(dw - dm) > q.max_drift then return C.fault_now('wall/monotonic drift exceeded') end
            if dm > q.max_gap then
                -- An ordinary process pause is uncertainty, not proof of a
                -- wall-clock fault. Drop every certificate and reset the
                -- observation; a subsequent read still needs fresh quorum.
                last_wall, last_mono, last_wall64, last_mono64 = w, m, w64, m64
                C.local_ready = false
                certificates = {}
                C.epoch = C.epoch + 1
                return unknown('clock monitor gap exceeded')
            end
        end
        -- The persisted value may have been written by another replica.
        -- A valid slower leader still uses H for admission; its local
        -- discontinuity detector must not misclassify inter-node skew.
        if policy and w + q.skew + q.max_drift < math.max(C.high, policy.created_wall) then
            return C.fault_now('clock precedes persisted high-water mark')
        end
        last_wall, last_mono, last_wall64, last_mono64 = w, m, w64, m64
        if C.fault then return unavailable(C.fault) end
        C.local_ready = true
        return nil
    end

    -- Own transaction ONLY; never read metadata through an ambient stream.
    -- A successful box.commit is the durability boundary, not replace().
    function C.load()
        if c.can_run and not c.can_run() then return false, unavailable('clock monitor heap pressure') end
        if box.is_in_txn() or C.pending ~= 0 then return false, unavailable('policy load needs its own transaction') end
        local load_epoch = C.epoch
        local e = C.observe()
        local q = cfg()
        if not q then return false, e end
        local space = meta()
        if not space then return false, unknown('policy space unavailable') end
        C.attach()
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
            certificate = {epoch = C.epoch, until_mono = last_mono + policy.max_gap, kind = 'self', peers = 0}
            if c.cfg.shard_id then certificates['shard:' .. c.cfg.shard_id] = certificate end
            C.state, C.reason = 'healthy', nil
            publish()
        end
        return true
    end

    -- Persist a detected fault in a separate worker/call, never the user's
    -- transaction. If quorum/role prevents this, volatile fault stays closed;
    -- after restart the confirmed policy/H plus UNKNOWN bootstrap still apply.
    function C.flush_fault()
        if c.can_run and not c.can_run() then return false end
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
    local function required_ids(required)
        local out = {}
        if required == nil then required = c.required_peers and c.required_peers() or {} end
        if type(required) ~= 'table' or #required > 64 then return nil end
        for _, id in ipairs(required) do
            if type(id) == 'number' then id = 'shard:' .. id end
            if type(id) ~= 'string' then return nil end
            out[#out + 1] = id
        end
        if c.cfg.sync and #out == 0 then return nil end
        return out
    end
    local function covered(ids, installing)
        if not ids then return false, 'required clock peers unavailable' end
        if #ids == 0 and not installing and (not certificate or certificate.epoch ~= C.epoch
           or last_mono > certificate.until_mono) then return false, 'clock certification expired' end
        for _, id in ipairs(ids) do
            local p = certificates[id]
            if not p or p.epoch ~= C.epoch or last_mono > p.until_mono then
                return false, 'required clock peer unreachable or expired'
            end
        end
        return true
    end
    function C.certify(proof)
        local e = C.observe()
        if e then return e end
        if box.info.ro then return unknown('clock owner is read-only') end
        if not C.loaded or not policy then return unknown('policy not loaded') end
        if type(proof) ~= 'table' or proof.epoch ~= C.epoch
           or not finite(proof.started) or proof.started > last_mono
           or last_mono - proof.started > policy.max_gap
           or type(proof.peers) ~= 'table' or #proof.peers > 64 then
            return unknown('clock certificate unavailable or stale')
        end
        local seen = {}
        for _, p in ipairs(proof.peers) do
            if type(p) ~= 'table' or type(p.id) ~= 'string' or seen[p.id]
               or type(p.low) ~= 'number' or type(p.high) ~= 'number'
               or p.low ~= p.low or p.high ~= p.high or p.low > p.high
               or p.max_age ~= policy.max_age or p.skew ~= policy.skew then
                return unknown('invalid peer clock interval or policy')
            end
            seen[p.id] = true
            certificates[p.id] = nil
            if p.local_ok == true and p.fault == nil then
                if p.high < -policy.skew or p.low > policy.skew then
                    return C.fault_now('peer clock bound violated')
                end
                if p.low >= -policy.skew and p.high <= policy.skew then
                    certificates[p.id] = {epoch = C.epoch, until_mono = proof.started + policy.max_gap}
                end
            end
        end
        local ids = required_ids()
        local good, why = covered(ids, true)
        if not good then return unknown(why) end
        local until_mono = proof.started + policy.max_gap
        for _, id in ipairs(ids) do until_mono = math.min(until_mono, certificates[id].until_mono) end
        certificate = {epoch = C.epoch, until_mono = until_mono, kind = 'peer', peers = #ids}
        C.state, C.reason = 'healthy', nil
        publish()
        return nil
    end
    function C.read_check()
        local e = C.observe()
        if e then return e end
        if box.info.ro then return unknown('clock owner is read-only') end
        if not C.loaded or not C.local_ready then return unavailable('local clock policy not ready') end
        return nil
    end
    function C.check(required)
        local e = C.read_check()
        if e then return e end
        local good, why = covered(required_ids(required))
        if not good then return unavailable(why) end
        return nil
    end
    function C.admit(ts, epoch, required)
        local e = C.check(required)
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
    function C.retained_until(row)
        local p = policy or cfg() or {max_age = 120, skew = 60}
        return math.max(row.expires, row.ts + p.max_age + 2 * p.skew)
    end
    function C.gc_enter(required)
        local e = C.check(required)
        if e then return nil, e end
        return {epoch = C.epoch, high = 0, required = required_ids(required)}
    end
    -- Call immediately BEFORE deletes, inside the SAME transaction. Failure
    -- is returned to the owner, which must roll back the whole GC chunk.
    function C.gc_commit(ticket)
        if not box.is_in_txn() then return unavailable('GC requires its deletion transaction') end
        local e = C.check(type(ticket) == 'table' and ticket.required or nil)
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
        ticket.high = high
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
    function C.read_native_state()
        if not C.read_eligible() then
            return false, 0ULL, 0ULL, 0ULL, 0ULL
        end
        return true, last_mono64, last_wall64, ffi.new('uint64_t', math.floor(policy.max_gap * BILLION)),
               ffi.new('uint64_t', math.floor(policy.max_drift * BILLION))
    end
    function C.read_eligible()
        return C.local_ready and C.policy_ok() and true or false
    end
    function C.policy_ok()
        -- Native pre-borrow callback: no sampling, State publication or I/O.
        if c.can_run and not c.can_run() then return false end
        return C.loaded and not C.fault and matches(policy, cfg()) and true or false
    end
    function C.status()
        local covered_n = 0
        for _, p in pairs(certificates) do
            if p.epoch == C.epoch and last_mono ~= nil and last_mono <= p.until_mono then covered_n = covered_n + 1 end
        end
        return {state = C.state, reason = C.reason, fault = C.fault, fault_pending = C.fault_pending,
                epoch = C.epoch, high = C.high, loaded = C.loaded, pending = C.pending,
                read_ready = C.loaded and C.local_ready and not C.fault or false,
                wall = last_wall, mono = last_mono, policy = policy and clone(policy) or nil,
                certificate_kind = certificate and certificate.kind or nil,
                peer_count = certificate and certificate.peers or 0,
                covered_peer_count = covered_n,
                certified_until = certificate and certificate.until_mono or nil}
    end
    function C.tick()
        -- The runtime's nonallocating heap preflight precedes cfg/box.info
        -- and peer work. Skipping never renews certificates or clears faults.
        if c.can_run and not c.can_run() then return false end
        C.observe()
        if not C.loaded then C.load() end
        if C.fault then C.flush_fault(); return false end
        if C.loaded then
            if box.info.ro then unknown('clock owner is read-only'); return false end
            -- RF1 has no remote replica that can certify its own clock.
            -- The initial-time premise plus each current local observation
            -- keeps only this writer's slot current; other shards still
            -- require their role-certified interval evidence.
            if not c.cfg.sync and c.cfg.shard_id and C.local_ready then
                certificates['shard:' .. c.cfg.shard_id] = {
                    epoch = C.epoch, until_mono = last_mono + policy.max_gap}
            end
            if c.certifier then
                local ok, proof = pcall(c.certifier, C.probe())
                if ok then C.certify(proof) else unknown('clock peer sampling failed') end
            elseif not c.cfg.sync then
                certificate = {epoch = C.epoch, until_mono = last_mono + policy.max_gap, kind = 'self', peers = 0}
            if c.cfg.shard_id then certificates['shard:' .. c.cfg.shard_id] = certificate end
                C.state, C.reason = 'healthy', nil; publish()
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

-- Background-only public RPC sampler. No native/private addresses, no retry,
-- and no error string containing a credential-bearing URI is published.
function M.sampler(c)
    local net = require('net.box')
    local conns = {}
    local function close()
        for uri, conn in pairs(conns) do pcall(conn.close, conn); conns[uri] = nil end
    end
    local hints = {}
    local function sample(probe)
        local groups = c.peers()
        if type(groups) ~= 'table' or #groups > 64 then return nil end
        local active, live_groups, total = {}, {}, 0
        for _, g in ipairs(groups) do
            live_groups[g.id] = true
            for _, uri in ipairs(g.uris) do active[uri] = true; total = total + 1 end
        end
        if total > 256 then return nil end
        for id in pairs(hints) do if not live_groups[id] then hints[id] = nil end end
        for uri, conn in pairs(conns) do
            if not active[uri] then pcall(conn.close, conn); conns[uri] = nil end
        end
        local deadline = clock.monotonic() + math.min(1, (c.cfg.clock_max_gap or 2) / 2)
        local results, workers, next_id = {}, {}, 0
        local done = fiber.channel(4)
        for _ = 1, math.min(4, #groups) do
            workers[#workers + 1] = fiber.create(function()
                while clock.monotonic() < deadline do
                    next_id = next_id + 1
                    local g = groups[next_id]
                    if g == nil then break end
                    local tried = {}
                    if hints[g.id] then
                        local found = false
                        for _, uri in ipairs(g.uris) do if uri == hints[g.id] then found = true; break end end
                        if not found then hints[g.id] = nil end
                    end
                    for j = 0, #g.uris do
                        local uri = j == 0 and hints[g.id] or g.uris[j]
                        if uri and not tried[uri] and clock.monotonic() < deadline then
                            tried[uri] = true
                            local before = c.probe()
                            local ok, remote = pcall(function()
                                local left = math.min(.25, deadline - clock.monotonic())
                                if left <= 0 then return nil end
                                local conn = conns[uri]
                                if conn == nil or not conn:is_connected() then
                                    if conn then pcall(conn.close, conn) end
                                    conn = net.connect(uri, {user = c.cfg.user, password = c.cfg.password,
                                        connect_timeout = math.min(.05, left), wait_connected = math.min(.05, left)})
                                    conns[uri] = conn
                                end
                                left = math.min(.25, deadline - clock.monotonic())
                                if left <= 0 or not conn:is_connected() then return nil end
                                return conn:call('tarn_clock_certify', {left}, {timeout = left})
                            end)
                            local after = c.probe()
                            if ok and type(remote) == 'table' and finite(remote.wall)
                               and remote.shard_id == g.shard and remote.ro == false and remote.role_proved == true then
                                results[#results + 1] = {id = g.id, low = remote.wall - after.wall,
                                    high = remote.wall - before.wall, max_age = remote.max_age,
                                    skew = remote.skew, local_ok = remote.local_ok, fault = remote.fault}
                                hints[g.id] = uri
                                break
                            end
                            local conn = conns[uri]
                            if conn then pcall(conn.close, conn); conns[uri] = nil end
                        end
                    end
                end
                done:put(true, 0)
            end)
        end
        for _ = 1, #workers do
            local left = deadline - clock.monotonic()
            if left <= 0 or not done:get(left) then break end
        end
        for _, f in ipairs(workers) do if f:status() ~= 'dead' then pcall(f.cancel, f) end end
        return {epoch = probe.epoch, started = probe.mono, peers = results}
    end
    return sample, close
end

return M
