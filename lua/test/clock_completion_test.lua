-- CompletionRetention: positive actual PREPARE/finish APIs and fsync-WAL
-- restart. Only exact owned receipt metadata represents a prior leader's
-- later clock; injection is module-local under actual test_hooks. No host
-- clock changes, live broken source, or admission with a fault is used.
local fio, fiber, clock, json = require('fio'), require('fiber'), require('clock'), require('json')
local here = fio.dirname(arg[0]); package.path = here .. '/../?.lua;' .. package.path
local work, stage = assert(arg[1]), assert(arg[2]); fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 96 * 1048576,
    vinyl_memory = 16 * 1048576, vinyl_cache = 0, wal_mode = 'fsync',
    txn_isolation = 'read-confirmed', checkpoint_interval = 0, log_level = 3}
local tarn = require('tarn')
tarn.init{shard_id = 1, peers = {}, native = false, test_hooks = true,
    max_txn_age = 10, clock_skew = 2, recovery_after = 1000000, gc_interval = .05}
local checks = 0
local function check(yes, name)
    assert(yes, name); checks = checks + 1; print('ok ' .. stage .. ': ' .. name)
end
local function ok(r) assert(type(r) == 'table' and r[1] == 'ok', json.encode(r)); return r end
local function wait(fn)
    local deadline = fiber.clock() + 5
    repeat if fn() then return true end; fiber.sleep(.01) until fiber.clock() > deadline
    return false
end
assert(wait(function() return tarn.clock.check({1}) == nil and box.space._tarn_scratch ~= nil end))
local function id(engine, action) return 'clock-completion-' .. engine .. '-' .. action end
local function records(tx)
    box.begin{txn_isolation = 'read-confirmed'}
    local p, r = box.space._tarn_part:get{tx}, box.space._tarn_txn:get{tx}
    local result = {part = p and {state = p.state, ts = p.ts, expires = p.expires},
                    decision = r and {state = r.status, ts = r.ts, expires = r.expires}}
    box.rollback(); return result
end
if stage == 'write' then
    local base = clock.realtime()
    local fixture = {base = base, records = {}}
    for n, engine in ipairs({'memtx', 'vinyl'}) do
        local sid = 98400 + n
        ok(tarn_create_table{id = sid, name = 'test.completion_' .. engine, engine = engine,
            ver = 0, json = '{}', lazy = {}, uix = {},
            format = {{'id', 'unsigned'}, {'v', 'unsigned'}, {'bucket_id', 'unsigned'}},
            indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                       {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}}}})
        for key, action in ipairs({'apply', 'abort', 'cas'}) do
            local tx = id(engine, action)
            local r = tarn_prepare(tx, 1, {1}, {{'insert', sid, {key, 7, 1}}}, {}, {}, clock.realtime())
            assert(r[1] == 'yes', json.encode(r))
            -- Historical state carried by a prior healthy leader, whose
            -- receipt timestamps exceed this holder's last local sample.
            box.begin{txn_isolation = 'read-confirmed'}
            box.space._tarn_part:update({tx}, {{'=', 5, base + 100}, {'=', 6, base + 114}})
            box.space._tarn_txn:update({tx}, {{'=', 4, base + 101}, {'=', 5, base + 130}})
            box.commit()
            fixture.records[tx] = records(tx)
        end
    end
    box.space._tarn_meta:replace{'clock:completion-test', fixture}
    -- Stop only this fixture's clock worker before inducing a true local
    -- backward fault. Its durable policy still has no fault, so the next
    -- process cannot rely on a persisted fault to retain the receipts.
    tarn.clock.stop()
    local observed = tarn.clock.status()
    ok(tarn_clock_test_time(base - 100, observed.mono + 1))
    check(tarn.clock.check({1}) ~= nil and tarn.clock.fault ~= nil,
          'backward test clock is unhealthy before decided completion')
    check(tarn.clock.now() >= observed.wall, 'completion clock floor includes the last valid sample')
    for n, engine in ipairs({'memtx', 'vinyl'}) do
        local sid = 98400 + n
        -- All participants (the one owned shard) promised YES: APPLY is a
        -- known COMMIT choice and marks its still-STAGING decision final.
        ok(tarn_apply(id(engine, 'apply')))
        check(tarn_txn_cas(id(engine, 'abort'), 'staging', 'aborted', {1}) == 'aborted',
              engine .. ' decided ABORT choice remains available on fault')
        ok(tarn_abort(id(engine, 'abort'), 1, {1}))
        check(tarn_txn_cas(id(engine, 'cas'), 'staging', 'committed', {1}) == 'committed',
              engine .. ' existing COMMIT decision remains available on fault')
        ok(tarn_apply(id(engine, 'cas')))
        for _, action in ipairs({'apply', 'abort', 'cas'}) do
            local tx = id(engine, action)
            local before, after = fixture.records[tx], records(tx)
            check(after.part.ts >= before.part.ts and after.part.expires >= before.part.expires
                  and after.decision.ts >= before.decision.ts and after.decision.expires >= before.decision.expires,
                  engine .. ' ' .. action .. ' preserves each prior timestamp and horizon')
        end
        check(box.space[sid]:get{1}[2] == 7 and box.space[sid]:get{2} == nil and box.space[sid]:get{3}[2] == 7,
              engine .. ' decided business effects are exact')
    end
    check(tarn.clock.fault_pending and box.space._tarn_meta:get{'clock:policy:1'}.value.fault == nil,
          'durable receipts cannot rely on a persisted fault latch')
elseif stage == 'restart' then
    local fixture = assert(box.space._tarn_meta:get{'clock:completion-test'}).value
    tarn.clock.stop()
    local observed = tarn.clock.status()
    local wall = fixture.base + 102
    ok(tarn_clock_test_time(wall, observed.mono + wall - observed.wall))
    tarn.clock.observe() -- a matched elapsed gap is UNKNOWN, not a fault
    assert(tarn.clock.observe() == nil); tarn.clock.tick()
    check(tarn.clock.check({1}) == nil and tarn.clock.fault == nil
          and box.space._tarn_meta:get{'clock:policy:1'}.value.fault == nil,
          'WAL restart loses volatile fault and establishes a healthy successor')
    check(tarn.clock.admit(fixture.base + 100) == nil,
          'the original prior-leader timestamp is still within the admission horizon')
    fiber.sleep(.2) -- actual independent GC worker gets several passes
    for n, engine in ipairs({'memtx', 'vinyl'}) do
        local sid = 98400 + n
        for _, action in ipairs({'apply', 'abort', 'cas'}) do
            local tx = id(engine, action)
            local before, after = fixture.records[tx], records(tx)
            check(after.part ~= nil and after.decision ~= nil
                  and after.part.ts >= before.part.ts and after.part.expires >= before.part.expires
                  and after.decision.ts >= before.decision.ts and after.decision.expires >= before.decision.expires,
                  engine .. ' ' .. action .. ' survives confirmed restart and healthy GC')
            local r = tarn_prepare(tx, 1, {1}, {}, {}, {}, fixture.base + 100)
            check(r[1] == (action == 'abort' and 'no' or 'yes'),
                  engine .. ' ' .. action .. ' revotes only its retained outcome')
        end
        check(box.space[sid]:get{1}[2] == 7 and box.space[sid]:get{2} == nil and box.space[sid]:get{3}[2] == 7,
              engine .. ' restart preserves exact committed data')
    end
else error('unknown stage') end
print(string.format('%d passed, 0 failed', checks)); os.exit(0)
