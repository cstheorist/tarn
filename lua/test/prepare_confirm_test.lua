-- Revote YES is evidence of a confirmed participant, never of another
-- PREPARE that is still waiting for its synchronous quorum.
local fio, fiber, clock, json, net = require('fio'), require('fiber'), require('clock'), require('json'), require('net.box')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local native = os.getenv('TARN_NATIVE')
if native ~= nil and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
local work = assert(arg[1]); fio.mktree(work)
local uri = 'unix/:' .. fio.pathjoin(work, 'iproto.sock')
box.cfg{work_dir = work, listen = uri, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1048576,
    vinyl_memory = 32 * 1048576, vinyl_cache = 0, vinyl_timeout = .25, wal_mode = 'fsync',
    checkpoint_interval = 0, txn_isolation = 'read-confirmed', replication_synchro_quorum = 1,
    replication_synchro_timeout = .5, election_mode = 'off', log_level = 3}
box.ctl.promote()
local tarn = require('tarn'); tarn.lazy.paused = true
tarn.init{shard_id = 1, peers = {}, sync = true, native = native ~= nil and native ~= '0' and native or false,
    test_hooks = true, check_yields = true, recovery_after = 1000000}
fiber.sleep(.03)
local function wait(label, fn)
    local until_ = fiber.clock() + 5
    repeat if fn() then return end; fiber.sleep(.005) until fiber.clock() >= until_
    error(label .. ' timed out')
end
wait('schema/quota ready', function() return box.space._tarn_part ~= nil and tarn.lazy.quota.ready() end)
local function ok(r) assert(r[1] == 'ok', json.encode(r)); return r end
ok(tarn_bucket_init(16, {{1, 16}})); ok(tarn_autocommit({}, {}))
local ni, layout = tarn_info().native
if native ~= nil and native ~= '0' then
    layout = tonumber(require('ffi').cast('struct tarn_ns *', box.func['tarn_storage.ts_state']:call()).layout)
    assert(ni and ni.ready == 1 and layout == 7, 'actual native ABI7 is required')
else assert(tarn.cfg.native == false and ni == false, 'explicit Lua mode is required') end
print('IDENTITY ' .. json.encode{version = box.info.version, native = ni, layout = layout,
    sync = tarn.cfg.sync, wal = box.cfg.wal_mode, part_sync = box.space._tarn_part.is_sync})
local c = net.connect(uri, {user = 'tarn', password = 'tarn', wait_connected = 3})
assert(c:is_connected(), 'private stream connection failed')
local pass, fail = 0, 0
local function check(name, yes, extra)
    if yes then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. ' ' .. json.encode(extra)) end
end
local function state(sid, txid, key)
    box.begin{txn_isolation = 'read-confirmed'}
    local p, d = box.space._tarn_part:get{txid}, box.space._tarn_txn:get{txid}
    local row = box.space[sid]:get{key or 1}
    local s = {part = p and p.state or 'none', decision = d and d.status or 'none', row = row and row:totable() or false,
        locks = box.space._tarn_locks.index.txid:count{txid},
        reservations = box.space._tarn_lazy_reservation.index.pk:count{txid}}
    box.rollback(); return s
end
for n, engine in ipairs({'memtx', 'vinyl'}) do
    local sid, gen = 78000 + n * 10, 78001 + n * 10
    local name, id = 'test.prepare_confirm_' .. engine, 'prepare-confirm-' .. engine
    local def = {id = sid, name = name, ver = 0, engine = engine, json = '{}',
        format = {{'id', 'unsigned'}, {'v', 'unsigned'}, {'bucket_id', 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{2, 'unsigned'}}}},
        lazy = {{gen = gen, base_space_id = sid, local_index_id = 2, route_parts = 1,
            key_fields = {2}, key_types = {'unsigned'}, pk_fields = {1}, pk_native_fields = {1},
            pk_types = {'unsigned'}, bucket_field = 3}}}
    local a = {def = def, timeout = 5}
    ok(tarn_create_prepare(name, a, 1))
    local done = false
    for _ = 1, 5 do if ok(tarn_lazy_bootstrap(name, a, 1))[2] then done = true; break end end
    assert(done); ok(tarn_lazy_ready(name, a, 1)); ok(tarn_create_apply(name, a))
    local writes = {{'insert', sid, {1, 7, n}}}
    local function args(txid, ws, reads)
        return {txid, 1, {1}, ws or writes, reads or {}, {}, clock.realtime(), box.NULL, {sid, 0}, {n}}
    end
    local function prepare(txid) return tarn_prepare(unpack(args(txid))) end
    box.cfg{replication_synchro_quorum = 2}
    local first
    local writer = fiber.new(function() first = prepare(id) end); writer:set_joinable(true)
    wait(engine .. ' first PREPARE reaches real quorum wait', function()
        return first == nil and box.info.synchro.queue.len == 1
    end)
    -- Show the exact dirty proof the regression excludes. This read-only
    -- inspection does not certify anything or mutate the pending transaction.
    box.begin{txn_isolation = 'read-committed'}
    local dirty = box.space._tarn_part:get{id}
    local promised = box.space._tarn_lazy_reservation.index.pk:count{id}
    box.rollback()
    check(engine .. ': first pending PREPARE really installed unconfirmed participant and promise',
          dirty and dirty.state == 'prepared' and promised == 1 and first == nil)
    local duplicate = prepare(id)
    check(engine .. ': duplicate cannot certify an unconfirmed YES', duplicate[1] ~= 'yes', duplicate)
    wait(engine .. ' original PREPARE and best-effort NO cleanup finish', function()
        return first ~= nil and box.info.synchro.queue.len == 0
    end)
    assert(writer:join())
    local after = state(sid, id)
    check(engine .. ': quorum rollback leaves no prepared claim, promise or business row',
          first[1] ~= 'yes' and after.part ~= 'prepared' and after.decision ~= 'staging'
          and after.locks == 0 and after.reservations == 0 and not after.row, {first, after})
    box.cfg{replication_synchro_quorum = 1}
    -- A NO may legitimately tombstone its old ID. Retry the logical work
    -- with a fresh transaction ID, then prove confirmed revotes work.
    local retry = id .. '-retry'
    local yes = prepare(retry); local confirmed = state(sid, retry)
    check(engine .. ': fresh retry after quorum release obtains a durable YES', yes[1] == 'yes'
          and confirmed.part == 'prepared' and confirmed.decision == 'staging'
          and confirmed.locks > 0 and confirmed.reservations == 1 and not confirmed.row, {yes, confirmed})
    yes = prepare(retry)
    check(engine .. ': repeated confirmed PREPARE remains YES', yes[1] == 'yes', yes)
    assert(tarn_txn_cas(retry, 'staging', 'committed', {1}) == 'committed'); ok(tarn_apply(retry))
    local s = c:new_stream()
    ok(s:call('tarn_select', {sid, 0, {1}, 'EQ', 1, box.NULL, false, false, box.NULL, true}))
    ok(tarn_autocommit({{'replace', sid, {1, 8, n}}}, {fence = {sid, 0}, wb = {n}}))
    local ws, reads = {{'insert', sid, {2, 9, n}}}, {{sid, 0, {1}, n}}
    local no = s:call('tarn_prepare', args(id .. '-stale-stream', ws, reads))
    check(engine .. ': existing stream retains its overwritten read tracker and votes NO',
          no[1] == 'no' and no[2] == 1213 and box.space[sid]:get{2} == nil, no)
    pcall(s.rollback, s)
    s = c:new_stream()
    ok(s:call('tarn_select', {sid, 0, {1}, 'EQ', 1, box.NULL, false, false, box.NULL, true}))
    local stream_id = id .. '-good-stream'
    yes = s:call('tarn_prepare', args(stream_id, ws, reads)); confirmed = state(sid, stream_id, 2)
    check(engine .. ': an unchanged existing stream prepares its original transaction', yes[1] == 'yes'
          and confirmed.part == 'prepared' and confirmed.reservations == 1 and not confirmed.row, {yes, confirmed})
    yes = tarn_prepare(unpack(args(stream_id, ws, reads)))
    check(engine .. ': confirmed stream PREPARE can be revoted outside its stream', yes[1] == 'yes', yes)
    assert(tarn_txn_cas(stream_id, 'staging', 'committed', {1}) == 'committed'); ok(tarn_apply(stream_id))
    local events, route = ok(tarn_lazy_control('pending'))[2], {}
    for _, e in ipairs(events) do route[#route + 1] = e[5] end
    ok(tarn_lazy_control('ack', ok(tarn_lazy_apply(events, route))[2]))
    check(engine .. ': confirmed prepared work applies and its reservations drain',
          box.space[sid]:get{1}[2] == 8 and box.space[sid]:get{2}[2] == 9
          and box.space._tarn_lazy_reservation:count() == 0 and #ok(tarn_lazy_control('pending'))[2] == 0)
    pcall(s.rollback, s)
end
c:close()
print(string.format('%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
