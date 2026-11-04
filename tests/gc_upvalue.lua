-- Bounded functional experiment for #160. Production modules stay unchanged.
-- A passing run is a bounded negative result, not a GC cause/fix finding.
local fio, json, ffi = require('fio'), require('json'), require('ffi')
local jit = require('jit')
io.stdout:setvbuf('no')
package.path = fio.dirname(arg[0]) .. '/../lua/?.lua;' .. package.path

local profiles = {
    ordinary = {batch = 1},
    ['jit-off'] = {batch = 1, jit_off = true},
    ['incremental-gc'] = {batch = 1, gc_step = true},
    ['construction-warm'] = {batch = 64, construct = true},
    ['construction-warm-incremental-gc'] = {batch = 64, construct = true, gc_step = true},
    ['retained-construction-warm'] = {batch = 64, construct = true, retained = true},
    ['retained-construction-warm-incremental-gc'] = {
        batch = 64, construct = true, retained = true, gc_step = true,
    },
    ['retained-construction-warm-incremental-gc-jit-off'] = {
        batch = 64, construct = true, retained = true, gc_step = true, jit_off = true,
    },
}
local result = {
    schema_version = 1, fixture = 'tarn-engine-upvalue-functional', complete = false, passed = false,
    profile = arg[1] or 'ordinary',
    counts = {iterations = 0, constructors = 0, marks = 0, cuts = 0,
              invalidations = 0, rebuilds = 0, blocked_rebuilds = 0, gc_steps = 0},
    position = {phase = 'configuration', iteration = 0},
    runtime = {tarantool = tostring(_TARANTOOL), luajit = jit.version,
               arch = jit.arch, os = jit.os, gc64 = ffi.abi('gc64')},
}
local function jit_status()
    local flags = {jit.status()}
    local enabled = table.remove(flags, 1)
    return {enabled = enabled, flags = flags}
end
result.runtime.initial_jit = jit_status()

local function run()
    assert(arg[3] == nil, 'usage: gc_upvalue.lua PROFILE [ITERATIONS]')
    local profile = assert(profiles[result.profile], 'unknown profile')
    local iterations = arg[2] == nil and 20000 or tonumber(arg[2])
    assert(iterations and iterations % 1 == 0 and iterations >= 1 and iterations <= 1000000,
           'ITERATIONS must be an integer in [1, 1000000]')
    local identities = assert(os.getenv('TARN_GC_SOURCE_IDENTITIES_JSON'),
                              'runner must supply TARN_GC_SOURCE_IDENTITIES_JSON')
    result.source_identities = json.decode(identities)
    assert(type(result.source_identities) == 'table' and identities:match('^%s*{')
           and next(result.source_identities) ~= nil, 'source identities must be a nonempty JSON object')
    if profile.jit_off then jit.off() end
    result.runtime.effective_jit = jit_status()
    assert(result.runtime.effective_jit.enabled == (not profile.jit_off),
           'JIT must be enabled for this profile (or disabled for jit-off)')
    result.config = {
        iterations = iterations, max_iterations = 1000000, batch_size = profile.batch,
        constructor_warming = profile.construct == true,
        retained_engines = profile.retained == true,
        gc = profile.gc_step and 'explicit incremental step(1) each iteration' or 'default',
        native = false, sync = false, wal_mode = 'none',
        cut_sequence = 'false then true', transactions = false,
    }

    -- The real meter consults box.info and box.is_in_txn even without writes.
    -- Own a standalone, empty database; do not load tarn or a native module.
    result.position.phase = 'initialization'
    assert(type(box.cfg) == 'function', 'fixture requires a fresh, unconfigured process')
    local work = fio.abspath(assert(os.getenv('TARN_GC_WORK_DIR') or fio.tempdir(),
                                    'cannot create isolated work directory'))
    fio.mktree(work)
    result.work_dir = work
    box.cfg{memtx_memory = 64 * 1024 * 1024, wal_mode = 'none',
            memtx_dir = work, wal_dir = work, vinyl_dir = work,
            checkpoint_interval = 0, log = work .. '/tarantool.log'}
    assert(not box.info.ro and not box.is_in_txn(), 'standalone instance must be writable and idle')
    local engine = require('tarn_engine')
    local counts, sequence = result.counts, 0
    local function new_engine()
        -- Only scalar expectations survive the mark factory. Keeping a shadow
        -- reference to its table here could hide a lifetime failure.
        local state = {expected = 0, allow = true}
        local E = engine.new{
            cfg = {sync = false},
            atomic = function(label, fn)
                assert(label == 'LazyMeterInstall', 'unexpected atomic service')
                return fn()
            end,
            exclude = function(excluded, loss)
                assert(excluded == nil and loss == true, 'unexpected exclusion service')
            end,
            mark = function()
                sequence = sequence + 1
                state.expected = sequence
                counts.marks = counts.marks + 1
                return {lua = sequence, native = nil}
            end,
            cut = function(mark)
                counts.cuts = counts.cuts + 1
                assert(type(mark) == 'table', 'mark is not a table')
                assert(type(mark.lua) == 'number' and mark.lua == state.expected,
                       'mark sequence/content mismatch')
                assert(mark.native == nil and getmetatable(mark) == nil, 'mark shape mismatch')
                local fields = 0
                for k in pairs(mark) do
                    assert(k == 'lua', 'unexpected mark field')
                    fields = fields + 1
                end
                assert(fields == 1, 'mark field count mismatch')
                return state.allow
            end,
        }
        counts.constructors = counts.constructors + 1
        assert(state.expected > 0 and not E.tickets.ready(), 'constructor state mismatch')
        return {engine = E, state = state}
    end
    local function exercise(item, iteration)
        local E, state = item.engine, item.state
        result.position.phase, result.position.iteration = 'invalidate', iteration
        E.invalidate(true)
        counts.invalidations = counts.invalidations + 1
        assert(not E.tickets.ready(), 'invalidation did not retire READY')
        if profile.gc_step then
            -- Explicit scheduling perturbation: never part of ordinary/default GC.
            collectgarbage('step', 1)
            counts.gc_steps = counts.gc_steps + 1
        end
        result.position.phase = 'blocked-rebuild'
        state.allow = false
        assert(E.tickets.rebuild() == false and not E.tickets.ready(), 'blocked cut admitted rebuild')
        counts.blocked_rebuilds = counts.blocked_rebuilds + 1
        result.position.phase = 'rebuild'
        state.allow = true
        assert(E.tickets.rebuild() == true and E.tickets.ready(), 'valid cut failed to rebuild')
        counts.rebuilds = counts.rebuilds + 1
        local snapshot = E.tickets.snapshot()
        assert(snapshot and snapshot.entries == 0 and snapshot.bytes == 0
               and snapshot.reserved_entries == 0 and snapshot.reserved_bytes == 0,
               'zero-accounting snapshot mismatch')
        assert(E.tickets.outstanding == 0 and next(E.tickets.tickets) == nil,
               'unexpected terminal ticket')
        counts.iterations = counts.iterations + 1
    end

    if profile.retained then
        -- Keep the same closed upvalues live throughout repeated exercise.
        -- Constructor warming remains an attempt, not proof of a trace.
        result.position.phase = 'construction'
        local batch, n = {}, math.min(profile.batch, iterations)
        for j = 1, n do batch[j] = new_engine() end
        for i = 1, iterations do exercise(batch[(i - 1) % n + 1], i) end
    elseif profile.construct then
        -- Repeated real constructors can warm a shared prototype while their
        -- own mark upvalues are open. Exercise each instance only after return.
        -- This attempts that precondition; it does not certify a recorded trace.
        for first = 1, iterations, profile.batch do
            result.position.phase, result.position.iteration = 'construction', first
            local batch = {}
            local n = math.min(profile.batch, iterations - first + 1)
            for j = 1, n do batch[j] = new_engine() end
            for j = 1, n do exercise(batch[j], first + j - 1) end
        end
    else
        result.position.phase = 'construction'
        local item = new_engine()
        for i = 1, iterations do exercise(item, i) end
    end
    assert(counts.iterations == iterations and counts.invalidations == iterations
           and counts.rebuilds == iterations and counts.blocked_rebuilds == iterations,
           'incomplete iteration accounting')
    assert(counts.marks == counts.constructors + iterations and counts.cuts == 3 * iterations,
           'mark/cut callback accounting mismatch')
    local constructors = profile.retained and math.min(profile.batch, iterations)
                         or (profile.construct and iterations or 1)
    assert(counts.constructors == constructors, 'constructor accounting mismatch')
    result.position.phase = 'finished'
    result.complete, result.passed = true, true
    result.interpretation = 'bounded functional pass; no GC cause or fix established'
end

local ok, err = pcall(run)
if not ok then
    result.error = tostring(err)
    local phase = result.position.phase
    result.failure_kind = (phase == 'configuration' and 'configuration')
                          or (phase == 'initialization' and 'initialization') or 'invariant'
end
print(json.encode(result))
os.exit(ok and 0 or 1)
