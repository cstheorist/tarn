-- Concurrent participants ensure one central, immutable counter alias.
-- Real quorum waits make creation overlap; confirmed replays need no WAL.
local fio, fiber, json, net = require('fio'), require('fiber'), require('json'), require('net.box')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local native = os.getenv('TARN_NATIVE')
if native and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, listen = '127.0.0.1:3897', memtx_use_mvcc_engine = true,
    memtx_memory = 128 * 1048576, vinyl_memory = 16 * 1048576, vinyl_cache = 0,
    wal_mode = 'fsync', txn_isolation = 'read-confirmed', election_mode = 'off',
    replication_synchro_quorum = 1, replication_synchro_timeout = 10, log_level = 3}
box.ctl.promote()
local tarn = require('tarn'); tarn.lazy.paused = true
local function wait(label, fn)
    local deadline = fiber.clock() + 5
    repeat if fn() then return end; fiber.sleep(.005) until fiber.clock() >= deadline
    error(label .. ' timed out')
end
tarn.init{shard_id = 1, peers = {}, sync = true, test_hooks = true, check_locks = true,
    recovery_after = 1000000, native = native and native ~= '0' and native or false}
wait('schema/native', function()
    for _, name in ipairs({'_tarn_meta', '_tarn_buckets', '_tarn_catalog', '_tarn_locks', '_tarn_part', '_tarn_txn'}) do
        if box.space[name] == nil or box.space[name].index[0] == nil then return false end
    end
    if box.space._tarn_part.index.state == nil or box.space._tarn_meta:get{'shard_id'} == nil then return false end
    return not native or native == '0' or (tarn_info().native or {}).ready == 1
end)
if native and native ~= '0' then
    local ffi = require('ffi')
    local ns = ffi.cast('struct tarn_ns *', box.func['tarn_storage.ts_state']:call())
    assert(ns.layout == 13 and ns.ready == 1 and ffi.sizeof('struct tarn_ns') == 392)
    assert(ffi.offsetof('struct tarn_ns', 'capture_stale') == 200
        and ffi.offsetof('struct tarn_ns', 'write_excluded') == 208
        and ffi.offsetof('struct tarn_ns', 'fallbacks') == 304)
else assert(tarn.cfg.native == false and tarn_info().native == false) end
print('IDENTITY ' .. json.encode{version = box.info.version, native = tarn_info().native,
    sync = tarn.cfg.sync, wal = box.cfg.wal_mode})
local clients = {net.connect('tarn:tarn@127.0.0.1:3897', {wait_connected = 5}),
                 net.connect('tarn:tarn@127.0.0.1:3897', {wait_connected = 5})}
local passed, failed, calls = 0, 0, {}
local function check(label, yes, extra)
    if yes then passed = passed + 1; print('  ok   ' .. label)
    else failed = failed + 1; print('  FAIL ' .. label .. ' ' .. json.encode(extra)) end
end
local function successful(s) return s.done and s.ok and type(s.reply) == 'table' and s.reply[1] == 'ok' end
local function start(c, old, new)
    local state = {}
    state.fiber = fiber.new(function()
        state.ok, state.reply = pcall(c.call, c, 'tarn_engine_counter_alias', {old, new})
        state.done = true
    end)
    state.fiber:set_joinable(true); calls[#calls + 1] = state
    return state
end
local function joined(s)
    wait('alias caller', function() return s.done end)
    local ok, err = s.fiber:join(); assert(ok, tostring(err))
end
local function confirmed(fn)
    box.begin{txn_isolation = 'read-confirmed'}
    local ok, result = pcall(fn)
    if box.is_in_txn() then box.rollback() end
    if not ok then error(result) end
    return result
end
local function alias(id)
    return confirmed(function() local t = box.space._tarn_meta:get{'counter_alias:' .. id}; return t and t.value end)
end
local original = tarn_engine_counter_alias
local watched, arrived = 120001, 0
-- Fixture-only arrival observation: the actual public RPC remains the body.
_G.tarn_engine_counter_alias = function(old, new)
    if new == watched then arrived = arrived + 1 end
    return original(old, new)
end
local ok, err = xpcall(function()
    box.cfg{replication_synchro_quorum = 2}
    local first = start(clients[1], 120000, watched)
    wait('first native quorum wait', function() return arrived == 1 and box.info.synchro.queue.len == 1 end)
    check('first alias creation is genuinely pending WAL/quorum, with no reply', not first.done)
    local second = start(clients[2], 120000, watched)
    wait('second RPC arrival', function() return arrived == 2 end)
    fiber.sleep(.03)
    check('the second participant waits instead of returning an MVCC conflict', not second.done,
        {done = second.done, reply = second.reply})
    box.cfg{replication_synchro_quorum = 1}
    joined(first); joined(second)
    check('both concurrent creators acknowledge one confirmed canonical binding',
        successful(first) and successful(second) and alias(watched) == 120000,
        {first = first.reply, second = second.reply})

    box.cfg{replication_synchro_quorum = 2}
    local a, b = start(clients[1], 120000, watched), start(clients[2], 120000, watched)
    local deadline = fiber.clock() + .3
    repeat if a.done and b.done then break end; fiber.sleep(.005) until fiber.clock() >= deadline
    check('confirmed identical replays both succeed without a new synchronous write',
        successful(a) and successful(b) and box.info.synchro.queue.len == 0,
        {first = a.reply, second = b.reply, queue = box.info.synchro.queue.len})
    box.cfg{replication_synchro_quorum = 1}; joined(a); joined(b)
    check('read-only replays preserve the confirmed alias', alias(watched) == 120000)

    local wrong = clients[1]:call('tarn_engine_counter_alias', {120099, watched})
    check('a different canonical identity is refused without changing the alias',
        wrong[1] == 'error' and wrong[2] == 1412 and alias(watched) == 120000, wrong)
    local chained = 120002
    local c, d = start(clients[1], watched, chained), start(clients[2], watched, chained)
    joined(c); joined(d)
    check('concurrent next-generation aliases retain the original counter identity',
        successful(c) and successful(d) and alias(chained) == 120000,
        {first = c.reply, second = d.reply})
    check('legacy AUTO_INCREMENT and hidden-row names share the same origin',
        confirmed(function() return tarn.engine.counter_name('ai:' .. chained) == 'ai:120000'
            and tarn.engine.counter_name('rid:' .. chained) == 'rid:120000' end))
    check('native lock-copy checks remain exact', tarn_info().stats.lock_copy_errors == 0)
end, debug.traceback)
box.cfg{replication_synchro_quorum = 1}
_G.tarn_engine_counter_alias = original
if box.is_in_txn() then box.rollback() end
for _, c in ipairs(clients) do c:close() end
if not ok then failed = failed + 1; print('  FAIL alias exception: ' .. tostring(err)) end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
