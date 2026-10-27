-- A duplicate participant APPLY must not discharge session catch-up while
-- the first APPLY's native synchronous commit can still roll back.
local fio, fiber, json, clock = require('fio'), require('fiber'), require('json'), require('clock')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local native = os.getenv('TARN_NATIVE')
if native ~= nil and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'fsync',
        txn_isolation = 'read-confirmed', replication_synchro_quorum = 1,
        replication_synchro_timeout = .5, election_mode = 'off', log_level = 3}
box.ctl.promote()
local tarn = require('tarn'); tarn.lazy.paused = true
tarn.init{shard_id = 1, peers = {}, sync = true,
          native = native ~= nil and native ~= '0' and native or false,
          test_hooks = true, check_yields = true, recovery_after = 1000000}
local passed, failed = 0, 0
local function check(name, yes, extra)
    if yes then passed = passed + 1; print('  ok   ' .. name)
    else failed = failed + 1; print('  FAIL ' .. name .. ' ' .. json.encode(extra)) end
end
local function ok(r) assert(r[1] == 'ok', json.encode(r)); return r end
local function wait(label, fn)
    local deadline = fiber.clock() + 4
    repeat if fn() then return end; fiber.sleep(.005) until fiber.clock() >= deadline
    error(label .. ' timed out')
end
wait('schema and quota ready', function()
    return box.space._tarn_part ~= nil and tarn.lazy.quota.ready()
        -- (and the native procedures, which the schema's setup makes last)
        and (native == nil or native == '0' or (tarn_info().native or {}).ready == 1)
end)
ok(tarn_bucket_init(16, {{1, 16}}))
ok(tarn_autocommit({}, {}))
local ni, layout = tarn_info().native
if native ~= nil and native ~= '0' then
    layout = tonumber(require('ffi').cast('struct tarn_ns *', box.func['tarn_storage.ts_state']:call()).layout)
    assert(ni and ni.ready == 1 and layout == 8, 'actual native ABI8 is required')
else
    assert(tarn.cfg.native == false and ni == false, 'explicit Lua mode is required')
end
print('IDENTITY ' .. json.encode{version = box.info.version, native = ni, layout = layout,
    sync = tarn.cfg.sync, wal = box.cfg.wal_mode, part_sync = box.space._tarn_part.is_sync})
local function state(sid, txid, bucket)
    box.begin{txn_isolation = 'read-confirmed'}
    local p = box.space._tarn_part:get{txid}
    local d = box.space._tarn_txn:get{txid}
    local c = box.space._tarn_lazy_counter:get{bucket}
    local row = box.space[sid]:get{1}
    local r = {part = p and p.state or false, decision = d and d.status or false,
        row = row and row:totable() or false, counter = c and tonumber(c.seq) or 0,
        outbox = box.space._tarn_lazy_outbox:count(),
        reservations = box.space._tarn_lazy_reservation:count()}
    box.rollback()
    return r
end
for n, engine in ipairs({'memtx', 'vinyl'}) do
    local sid, gen, txid = 73000 + n * 10, 73001 + n * 10, 'apply-confirm-' .. engine
    local spec = {gen = gen, base_space_id = sid, local_index_id = 2, route_parts = 1,
        key_fields = {2}, key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1},
        pk_types = {'unsigned'}, bucket_field = 3}
    local def = {id = sid, name = 'test.lazy_apply_confirm_' .. engine, ver = 0,
        engine = engine, json = '{}', lazy = {spec},
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'string'},
                  {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{2, 'string'}}}}}
    local a = {def = def, timeout = 5}
    ok(tarn_create_prepare(def.name, a, 1))
    local done = false
    for _ = 1, 5 do if ok(tarn_lazy_bootstrap(def.name, a, 1))[2] then done = true; break end end
    assert(done, 'empty bootstrap did not finish')
    ok(tarn_lazy_ready(def.name, a, 1)); ok(tarn_create_apply(def.name, a))
    ok(tarn_autocommit({}, {}))
    local yes = tarn_prepare(txid, 1, {1}, {{'insert', sid, {1, 'own', n}}}, {}, {},
                             clock.realtime(), box.NULL, {sid, 0}, {n})
    assert(yes[1] == 'yes', json.encode(yes))
    assert(tarn_txn_cas(txid, 'staging', 'committed', {1}) == 'committed')
    local before = state(sid, txid, n)
    check(engine .. ': durable PREPARE reserves without publishing row or sequence',
          before.part == 'prepared' and before.decision == 'committed' and not before.row
          and before.counter == 0 and before.outbox == 0 and before.reservations == 1
          and box.space[sid].engine == engine and box.space._tarn_part.is_sync, before)
    box.cfg{replication_synchro_quorum = 2}
    local first
    local writer = fiber.new(function() first = tarn_apply(txid) end)
    writer:set_joinable(true)
    wait(engine .. ' APPLY reaches real quorum wait', function()
        return first == nil and box.info.synchro.queue.len == 1
    end)
    local duplicate = tarn_apply(txid)
    -- Check the participant reply itself, before any frontier/barrier or
    -- source ACK could fail and accidentally mask an early terminal ACK.
    check(engine .. ': duplicate cannot ACK an unconfirmed participant APPLY',
          duplicate[1] ~= 'ok', duplicate)
    wait(engine .. ' first APPLY rolls back', function()
        return first ~= nil and box.info.synchro.queue.len == 0
    end)
    assert(writer:join())
    local after = state(sid, txid, n)
    check(engine .. ': failed first APPLY retains prepared promise and every effect rolls back',
          first[1] ~= 'ok' and after.part == 'prepared' and after.decision == 'committed'
          and not after.row and after.counter == 0 and after.outbox == 0
          and after.reservations == before.reservations, {first, after})
    box.cfg{replication_synchro_quorum = 1}
    -- An already-decided APPLY consumes its existing promise even when new
    -- admission is disabled. Confirmation must not add a fresh cap check.
    local old_entries, old_bytes = tarn.cfg.lazy_outbox_entries, tarn.cfg.lazy_outbox_bytes
    tarn.cfg.lazy_outbox_entries, tarn.cfg.lazy_outbox_bytes = 0, 0
    local retry = tarn_apply(txid)
    tarn.cfg.lazy_outbox_entries, tarn.cfg.lazy_outbox_bytes = old_entries, old_bytes
    local applied = state(sid, txid, n)
    check(engine .. ': confirmed retry consumes the durable promise despite lowered admission caps',
          retry[1] == 'ok' and retry[2] == 'applied' and applied.part == 'applied'
          and applied.row and applied.row[2] == 'own' and applied.counter == 1
          and applied.outbox == 1 and applied.reservations == 0, {retry, applied})
    local again = tarn_apply(txid)
    local events = ok(tarn_lazy_control('pending'))[2]
    check(engine .. ': confirmed duplicate is idempotent before delivery',
          again[1] == 'ok' and again[2] == 'applied' and #events == 1
          and state(sid, txid, n).counter == 1, again)
    local route = {}; for _, e in ipairs(events) do route[#route + 1] = e[5] end
    ok(tarn_lazy_control('ack', ok(tarn_lazy_apply(events, route))[2]))
    local f = ok(tarn_lazy_frontiers({n}, {n}))[2][1]
    check(engine .. ': durable APPLY and delivery permit catch-up with the base row present',
          #ok(tarn_lazy_control('pending'))[2] == 0 and f[2] == 1 and f[3] == 1
          and box.space[gen].index.ordered:count{true, 'own'} == 1
          and state(sid, txid, n).row[2] == 'own')
end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
