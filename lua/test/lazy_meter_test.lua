-- Standalone real-MVCC regression for LazyMeter's admission cache. No peers
-- or user data: each invocation owns a private temporary database.
local fiber, fio = require('fiber'), require('fio')
io.stdout:setvbuf('no')
local here = fio.dirname(arg[0])
package.path = here .. '/../?.lua;' .. package.path
local work = os.getenv('TARN_METER_DIR') or fio.tempdir()
fio.mktree(work)
box.cfg{memtx_memory = 64 * 1024 * 1024, memtx_use_mvcc_engine = true, wal_mode = 'fsync',
        memtx_dir = work, wal_dir = work, vinyl_dir = work, log = work .. '/tarantool.log'}
local function space(name, format)
    local s = box.schema.space.create(name, {format = format})
    s:create_index('pk')
    return s
end
local out = space('_tarn_lazy_outbox', {{'id', 'unsigned'}, {'bytes', 'unsigned'}})
local res = space('_tarn_lazy_reservation', {{'id', 'unsigned'}, {'entries', 'unsigned'}, {'bytes', 'unsigned'}})
local rows = space('t', {{'id', 'unsigned'}, {'n', 'unsigned'}})
rows:insert{1, 0}; rows:insert{2, 0}
local cfg, cut = {lazy_outbox_entries = 2, lazy_outbox_bytes = 100}, true
local q = require('tarn_lazy_meter').new{cfg = cfg, cut = function() return cut end}
local passed = 0
local function check(name, ok)
    assert(ok, name)
    passed = passed + 1
    print('  ok   ' .. name)
end
local function snapshot(e, b, re, rb)
    local u = q.snapshot()
    return u ~= nil and u.entries == e and u.bytes == b and u.reserved_entries == re and u.reserved_bytes == rb
end
local function txn(body)
    box.begin{txn_isolation = 'read-confirmed', timeout = 60}
    local ok, err = pcall(body)
    if ok then box.commit() else box.rollback(); error(err) end
end
local function clear()
    txn(function()
        local keys = {}
        for _, t in out:pairs() do keys[#keys + 1] = {t.id, t.bytes} end
        for _, t in ipairs(keys) do q.change(-1, -t[2], 0, 0); out:delete{t[1]} end
    end)
end

box.begin()
local ok, err = pcall(q.change, 1, 10, 0, 0)
box.rollback()
check('UNKNOWN refuses before allocating a terminal ticket', not ok and err.code == 1213 and q.outstanding == 0)
check('confirmed empty rebuild enables admission', q.rebuild() and snapshot(0, 0, 0, 0))

-- A has already read its own PK when B begins WAL commit. A shared usage
-- tuple makes A fail with TRANSACTION_CONFLICT in this exact schedule.
box.begin{txn_isolation = 'read-confirmed', timeout = 60}
rows:get{1}
local state = {done = false}
local b = fiber.create(function()
    state.ok, state.err = pcall(function()
        txn(function()
            rows:get{2}
            q.change(1, 10, 0, 0)
            out:insert{2, 10}
            rows:replace{2, 1}
        end)
    end)
    state.done = true
end)
b:set_joinable(true)
check('second writer is genuinely awaiting WAL', not state.done and q.outstanding == 1)
q.change(1, 10, 0, 0)
out:insert{1, 10}; rows:replace{1, 1}
box.commit()
check('second writer joins', b:join())
check('both disjoint writers commit across WAL overlap', state.ok and rows:get{1}.n == 1 and rows:get{2}.n == 1)
check('confirmed usage matches both durable entries', snapshot(2, 20, 0, 0) and q.outstanding == 0)

local acked = false
local ack = fiber.create(function()
    txn(function() q.change(-1, -10, 0, 0); out:delete{1} end)
    acked = true
end)
ack:set_joinable(true)
check('unconfirmed ACK keeps its admission charge', not acked and snapshot(2, 20, 0, 0))
box.begin()
ok, err = pcall(q.change, 1, 10, 0, 0)
box.rollback()
check('another writer cannot spend unconfirmed ACK credit', not ok and err.code == 1041)
check('ACK joins and releases only confirmed credit', ack:join() and snapshot(1, 10, 0, 0))

txn(function() q.change(0, 0, 1, 20); res:insert{1, 1, 20} end)
check('durable PREPARE retains a full promise', snapshot(1, 10, 1, 20))
cfg.lazy_outbox_entries, cfg.lazy_outbox_bytes = 0, 0
txn(function()
    q.change(0, 0, -1, -20); res:delete{1}
    q.change(1, 10, 0, 0, true); out:insert{3, 10}
end)
check('promised APPLY converts after limits lower to zero', snapshot(2, 20, 0, 0))
clear()
check('negative-only ACK remains possible with lowered limits', snapshot(0, 0, 0, 0))
cfg.lazy_outbox_entries, cfg.lazy_outbox_bytes = 2, 100
txn(function() q.change(0, 0, 1, 20); res:insert{2, 1, 20} end)
cfg.lazy_outbox_entries, cfg.lazy_outbox_bytes = 0, 0
txn(function() q.change(0, 0, -1, -20); res:delete{2} end)
check('ABORT releases its promise even with lowered limits', snapshot(0, 0, 0, 0))
cfg.lazy_outbox_entries, cfg.lazy_outbox_bytes = 2, 100

box.begin()
q.change(1, 10, 0, 0); out:insert{4, 10}
box.rollback()
check('rollback refunds positive precharges', snapshot(0, 0, 0, 0) and out:get{4} == nil and q.outstanding == 0)

-- A failed second callback registration must leave the first callback
-- harmless, even if a caller chooses to commit the otherwise empty txn.
box.begin()
local original = box.on_rollback
box.on_rollback = function() error('injected callback registration failure') end
ok = pcall(q.change, 1, 10, 0, 0)
box.on_rollback = original
box.commit()
check('partially registered ticket never charges or decrements twice', not ok and q.outstanding == 0 and snapshot(0, 0, 0, 0))

local done = false
local pending = fiber.create(function()
    txn(function() q.change(1, 10, 0, 0); out:insert{5, 10} end)
    done = true
end)
pending:set_joinable(true)
check('invalidation witness has an old terminal ticket', not done and q.outstanding == 1)
q.invalidate()
check('rebuild cannot pass an old pending terminal effect', not q.ready() and not q.rebuild())
check('old writer joins', pending:join())
check('rebuild includes confirmed old effect exactly once', q.rebuild() and snapshot(1, 10, 0, 0) and q.outstanding == 0)
q.invalidate(); cut = false
check('native quiescence cut gates rebuild', not q.rebuild())
cut = true
check('rebuild resumes after native quiescence', q.rebuild() and snapshot(1, 10, 0, 0))

local stale, q2 = true, nil
q2 = require('tarn_lazy_meter').new{cfg = cfg, cut = function() return true end, scan = function()
    if stale then stale = false; q2.invalidate() end
    return {1, 10, 0, 0}
end}
check('snapshot from an old epoch cannot install READY', not q2.rebuild() and not q2.ready())
check('current epoch snapshot installs READY', q2.rebuild() and q2.snapshot().entries == 1)

local sync = box.schema.space.create('sync_t', {is_sync = true, format = {{'id', 'unsigned'}}})
sync:create_index('pk')
box.ctl.promote()
cfg.sync = true
q.invalidate()
check('synchronous owner cut permits a confirmed rebuild', q.rebuild() and snapshot(1, 10, 0, 0))
box.cfg{replication_synchro_quorum = 2, replication_synchro_timeout = 1}
local sync_done = false
local waiting = fiber.create(function()
    txn(function() q.change(1, 10, 0, 0); out:insert{6, 10}; sync:insert{6} end)
    sync_done = true
end)
waiting:set_joinable(true)
local deadline = fiber.clock() + .5
while box.info.synchro.queue.len == 0 and not sync_done and fiber.clock() < deadline do fiber.sleep(.001) end
check('a real quorum wait retains a terminal ticket', not sync_done and box.info.synchro.queue.len > 0 and q.outstanding == 1)
q.invalidate()
check('rebuild refuses an unresolved synchronous queue', not q.rebuild())
box.cfg{replication_synchro_quorum = 1}
check('synchronous writer joins after confirmation', waiting:join())
check('confirmed queue and terminal ticket permit exact rebuild', q.rebuild() and snapshot(2, 20, 0, 0) and q.outstanding == 0)

cfg.lazy_outbox_entries = 3
local wake, notified, notify_yielded, notify_error = fiber.cond(), 0, false, nil
local q3 = require('tarn_lazy_meter').new{cfg = cfg, cut = function() return true end, notify = function()
    notified = notified + 1
    local worked, why = pcall(function()
        -- Commit callbacks may execute on a native WAL fiber, for which
        -- fiber.self() and the current fiber.info entry can be absent.
        local id = fiber.id()
        local info = fiber.info()
        local before = info and id and info[id] and info[id].csw
        wake:signal()
        info = fiber.info()
        local after = info and id and info[id] and info[id].csw
        notify_yielded = notify_yielded or (before ~= nil and after ~= nil and after ~= before)
        if notified == 2 then error('injected delivery wake failure') end
    end)
    if not worked then notify_error = tostring(why); error(why) end
end}
check('wake fixture rebuilds from confirmed state', q3.rebuild())
local woke = false
local sleeper = fiber.create(function() woke = wake:wait(1) end)
sleeper:set_joinable(true)
txn(function() q3.change(1, 10, 0, 0); out:insert{7, 10} end)
print('wake witness: notified=' .. notified .. ' yielded=' .. tostring(notify_yielded) .. ' error=' .. tostring(notify_error))
check('positive committed outbox wakes delivery from native callback', sleeper:join() and woke and notified == 1 and not notify_yielded)
txn(function() q3.change(-1, -10, 0, 0); out:delete{7} end)
check('ACK-only commit does not wake delivery', notified == 1)
txn(function() q3.change(1, 10, 0, 0); out:insert{8, 10} end)
check('wake failure cannot corrupt quota or commit outcome', notified == 2 and q3.outstanding == 0
      and q3.ready() and q3.snapshot().entries == 3 and out:get{8} ~= nil)

print(passed .. ' lazy meter checks passed; private database ' .. work)
os.exit(0)
