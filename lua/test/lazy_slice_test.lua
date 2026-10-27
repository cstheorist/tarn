-- First bounded native slice, no listener or external services.
local fio = require('fio')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local native = os.getenv('TARN_NATIVE')
if native ~= nil and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
local dir = assert(arg[1])
fio.mktree(dir)
box.cfg{work_dir = dir, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'write',
        txn_isolation = 'read-confirmed', log_level = 3}
local tarn = require('tarn').init{shard_id = 1, peers = {}, test_hooks = true,
                                check_yields = true, native = native ~= nil and native ~= '0' and native or false}
-- Let the initial status watch and independent quota-rebuild worker run;
-- a caller never rebuilds/waits on its own transaction INFLIGHT marker.
require('fiber').sleep(0.03)
local meter_deadline = require('fiber').clock() + 3
while not tarn.lazy.quota.ready() and require('fiber').clock() < meter_deadline do require('fiber').sleep(0.01) end
assert(tarn.lazy.quota.ready(), 'lazy admission meter did not reach READY')
local pass, fail = 0, 0
local function check(name, yes, extra)
    if yes then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. ' ' .. require('json').encode(extra)) end
end
assert(tarn_bucket_init(16, {{1, 16}})[1] == 'ok')
local sid, gen = 60001, 60002
local def = {name = 'test.lazy_slice', id = sid, ver = 0, engine = 'memtx',
    format = {{name = 'id', type = 'unsigned'}, {name = 'status', type = 'string', is_nullable = true},
              {name = 'bucket_id', type = 'unsigned'}},
    indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}},
               {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}},
               {name = 'status', unique = false, parts = {{field = 2, type = 'string', is_nullable = true}}}}, json = '{}'}
assert(tarn_create_table(def)[1] == 'ok')
def.lazy = {{gen = gen, base_space_id = sid, local_index_id = 2, route_parts = 1,
             key_fields = {2}, key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1},
             pk_types = {'unsigned'}, bucket_field = 3}}
assert(tarn_lazy_control('register', def)[1] == 'ok')
assert(tarn_lazy_control('pause')[1] == 'ok')
local function write(w, opts) return tarn_autocommit({w}, opts or {fence = {sid, 0}, wb = {1}}) end
local function pending() return tarn_lazy_control('pending')[2] end
local function frontier(b) b = b or 1; return tarn_lazy_frontiers({b}, {b})[2][1] end
local function deliver(events)
    local route = {}
    for _, a in ipairs(events) do route[#route + 1] = a[5] end
    local r = tarn_lazy_apply(events, route)
    assert(r[1] == 'ok', require('json').encode(r))
    assert(tarn_lazy_control('ack', r[2])[1] == 'ok')
    return r
end
local function lives(key)
    local n = 0
    for _, t in box.space[gen].index.ordered:pairs({true, key}, {iterator = 'EQ'}) do if t.live then n = n + 1 end end
    return n
end
local r = write({'insert', sid, {1, 'a', 1}}, {wb = {1}})
check('missing source version rejects without row/counter/outbox changes', r[1] == 'error' and r[2] == 1412
      and box.space[sid]:get{1} == nil and #pending() == 0 and frontier()[2] == 0, r)
r = write({'insert', sid, {1, 'a', 1}})
local first = pending()
check('row commit atomically appends one versioned membership', r[1] == 'ok' and #first == 1
      and first[1][4] == 1 and first[1][9] == true and frontier()[3] == 0, r)
deliver(first)
check('durable ACK advances the completed frontier', lives('a') == 1 and frontier()[3] == 1)
r = write({'update', sid, {1}, {{'=', 2, 'b'}}})
local changed = pending()
check('key change creates old-key delete and new-key put at one newer seq', r[1] == 'ok' and #changed == 2
      and changed[1][4] == 2 and changed[2][4] == 2, {r, changed})
deliver({changed[2]})
check('a higher ACK is not a hole-free frontier', frontier()[3] == 1)
deliver({changed[1]})
assert(tarn_lazy_apply(first, {first[1][5]})[1] == 'ok')
check('late old ADD cannot resurrect the old key', lives('a') == 0 and lives('b') == 1 and frontier()[3] == 2)
assert(write({'delete', sid, {1}})[1] == 'ok')
local deleted = pending()
deliver(deleted)
assert(write({'insert', sid, {1, 'c', 1}})[1] == 'ok')
local reinsert = pending()
deliver(reinsert)
assert(tarn_lazy_apply(changed, {changed[1][5], changed[2][5]})[1] == 'ok')
check('DELETE/reinsert never resets the sequence or old-key tombstones', reinsert[1][4] == 4
      and lives('b') == 0 and lives('c') == 1 and frontier()[3] == 4)
local old = tarn.cfg.lazy_outbox_entries
tarn.cfg.lazy_outbox_entries = 0
r = write({'insert', sid, {2, 'blocked', 1}})
tarn.cfg.lazy_outbox_entries = old
check('outbox admission failure rolls back row/version/counter/events', r[1] == 'error' and r[2] == 1041
      and box.space[sid]:get{2} == nil and frontier()[2] == 4 and #pending() == 0, r)
-- Packet repeats of one PK must publish initial→final, rather than
-- intermediate memberships or an old image from unique retry order.
r = tarn_autocommit({{'update', sid, {1}, {{'=', 2, 'intermediate'}}},
                     {'update', sid, {1}, {{'=', 2, 'final'}}}}, {fence = {sid, 0}, wb = {1, 1}})
local net = pending()
check('repeated-PK packet captures one original-to-final logical change', r[1] == 'ok' and #net == 2
      and net[1][6][1] == 'c' and net[2][6][1] == 'final' and net[1][4] == net[2][4], {r, net})
deliver(net)
local targetbucket = first[1][5]
local collected = tarn_lazy_gc(gen, targetbucket, {targetbucket})
check('GC persists a source acceptance floor before deleting old tombstones', collected[1] == 'ok'
      and box.space._tarn_lazy_floor:get{gen, targetbucket, 1} ~= nil, collected)
assert(tarn_lazy_apply(first, {targetbucket})[1] == 'ok')
check('delayed ADD at or below the durable GC floor cannot resurrect membership', lives('a') == 0)
local cap = box.space._tarn_lazy_capture:get{sid}:totable()
box.space._tarn_lazy_capture:delete{sid}; tarn.lazy.invalidate()
local rows_before, seq_before = box.space[sid]:len(), frontier()[2]
r = write({'insert', sid, {99, 'missing-capture', 1}})
check('missing durable capture cannot be inferred as an empty registry', r[1] ~= 'ok'
      and box.space[sid]:len() == rows_before and frontier()[2] == seq_before and #pending() == 0, r)
box.space._tarn_lazy_capture:replace(cap); tarn.lazy.invalidate(); tarn.lazy.fresh()
local function prepare(id, value, bucket)
    bucket = bucket or 1
    return tarn_prepare(id, 1, {1}, {{'insert', sid, {value, 'prepared', bucket}}}, {}, {},
                        require('clock').realtime(), nil, {sid, 0}, {bucket})
end
local before = frontier()[2]
r = prepare('lazy-reserved', 3)
local reserved = tarn_lazy_stats()[2]
check('PREPARE reserves bytes, entries and source sequence without publishing', r[1] == 'yes'
      and box.space[sid]:get{3} == nil and frontier()[2] == before and #pending() == 0
      and reserved.reserved_entries == 1 and reserved.reserved_bytes > 0
      and box.space._tarn_lazy_slots:get{1}.reserved == 1, r)
local limits = {tarn.cfg.lazy_outbox_entries, tarn.cfg.lazy_outbox_bytes}
tarn.cfg.lazy_outbox_entries, tarn.cfg.lazy_outbox_bytes = 0, 0
r = tarn_apply('lazy-reserved')
tarn.cfg.lazy_outbox_entries, tarn.cfg.lazy_outbox_bytes = limits[1], limits[2]
check('decided APPLY consumes its promise even after configured limits decrease', r[1] == 'ok'
      and box.space[sid]:get{3} ~= nil and frontier()[2] == before + 1 and #pending() == 1
      and tarn_lazy_stats()[2].reserved_entries == 0 and box.space._tarn_lazy_slots:get{1} == nil, r)
deliver(pending())
r = prepare('lazy-aborted', 4)
assert(r[1] == 'yes', require('json').encode(r))
assert(tarn_abort('lazy-aborted', 1, {1})[1] == 'ok')
check('ABORT releases the exact reservation and source sequence slot', box.space[sid]:get{4} == nil
      and #pending() == 0 and box.space._tarn_lazy_slots:get{1} == nil
      and tarn_lazy_stats()[2].reserved_entries == 0 and frontier()[2] == before + 1)
tarn.cfg.lazy_outbox_entries = 0
r = prepare('lazy-no-capacity', 5)
tarn.cfg.lazy_outbox_entries = limits[1]
check('failed PREPARE admission rolls back all promises, counters and data', r[1] == 'no' and r[2] == 1041
      and box.space[sid]:get{5} == nil and box.space._tarn_lazy_slots:get{1} == nil
      and tarn_lazy_stats()[2].reserved_entries == 0 and frontier()[2] == before + 1, r)
box.space._tarn_lazy_counter:replace{2, 9007199254740990}
r = prepare('lazy-last-sequence', 6, 2)
assert(r[1] == 'yes', require('json').encode(r))
r = write({'insert', sid, {7, 'cannot-steal-promise', 2}}, {fence = {sid, 0}, wb = {2}})
check('local admission cannot consume a promised final source sequence', r[1] == 'error' and r[2] == 1041
      and box.space[sid]:get{7} == nil and frontier(2)[2] == 9007199254740990, r)
assert(tarn_apply('lazy-last-sequence')[1] == 'ok')
check('the reserved final sequence remains exactly representable on APPLY', frontier(2)[2] == 9007199254740991
      and #pending() == 1 and pending()[1][4] == 9007199254740991)
deliver(pending())
local fiber = require('fiber')
assert(write({'insert', sid, {8, 'held', 1}})[1] == 'ok')
local held, answer = pending(), nil
assert(tarn_test_hook('lazy_apply', 'hold')[1] == 'ok')
local f = fiber.new(function() answer = tarn_lazy_apply(held, {held[1][5]}) end)
f:set_joinable(true)
assert(tarn_test_hook('lazy_apply', 'reached')[2] == 1)
assert(tarn_lazy_control('stale_routes')[1] == 'ok')
assert(tarn_test_hook('lazy_apply', 'release')[1] == 'ok')
assert(f:join())
check('target apply refuses stale routing after a yielding native step', answer[1] == 'moved'
      and lives('held') == 0 and #pending() == 1, answer)
deliver(pending())
check('the target apply retry actually installs the previously held membership', lives('held') == 1)
assert(write({'update', sid, {1}, {{'=', 2, 'gc-new'}}})[1] == 'ok')
local changes = pending()
deliver(changes)
local oldkeybucket = changes[1][5]
local floor_before = box.space._tarn_lazy_floor:get{gen, oldkeybucket, 1}
local before_encoded = require('msgpack').encode(floor_before or box.NULL)
assert(tarn_test_hook('lazy_gc', 'hold')[1] == 'ok')
answer = nil
f = fiber.new(function() answer = tarn_lazy_gc(gen, oldkeybucket, {oldkeybucket}) end)
f:set_joinable(true)
assert(tarn_test_hook('lazy_gc', 'reached')[2] == 1)
assert(tarn_lazy_control('stale_routes')[1] == 'ok')
assert(tarn_test_hook('lazy_gc', 'release')[1] == 'ok')
assert(f:join())
check('target GC rejects stale routing and rolls back its floor and deletions', answer[1] == 'moved'
      and require('msgpack').encode(box.space._tarn_lazy_floor:get{gen, oldkeybucket, 1} or box.NULL) == before_encoded, answer)
assert(tarn_lazy_gc(gen, oldkeybucket, {oldkeybucket})[1] == 'ok')
if native ~= nil and native ~= '0' then
    tarn.lazy.fresh()
    -- The native procedure must exclude the actual source even with no
    -- opts.fence. The Lua path can then return its precise 1412 error.
    local before = box.space[sid]:len()
    local nr = box.func['tarn_storage.ts_autocommit']:call({{{'insert', sid, {9, 'native', 1}}}, {wb = {1}}})
    check('native write on a lazy source falls back before writing', nr[1] == 'lua'
          and box.space[sid]:len() == before and #pending() == 0, nr)
    local ffi = require('ffi')
    local ns = ffi.cast('struct tarn_ns *', box.func['tarn_storage.ts_state']:call({6}))
    check('ABI6 has the integrated native layout and offsets', ffi.sizeof('struct tarn_ns') == 184
          and ffi.offsetof('struct tarn_ns', 'pl_spaces') == 160
          and ffi.offsetof('struct tarn_ns', 'capture_stale') == 168
          and ffi.offsetof('struct tarn_ns', 'write_excluded') == 176)
    local saved = ns.write_excluded
    ns.write_excluded = nil
    nr = box.func['tarn_storage.ts_autocommit']:call({{{'insert', sid, {9, 'null-array', 1}}}, {wb = {1}}})
    ns.write_excluded = saved
    check('missing native exclusion array fails closed before mutation', nr[1] == 'lua' and box.space[sid]:get{9} == nil)
    ns.capture_stale = 1
    nr = box.func['tarn_storage.ts_autocommit']:call({{{'insert', sid, {9, 'stale-registry', 1}}}, {wb = {1}}})
    ns.capture_stale = 0
    check('stale native capture registry fails closed', nr[1] == 'lua' and box.space[sid]:get{9} == nil)
    local safe = {name = 'test.lazy_native_control', id = 65001, ver = 0, engine = 'memtx',
                  format = def.format, indexes = def.indexes, json = '{}'}
    assert(tarn_create_table(safe)[1] == 'ok')
    tarn.lazy.fresh()
    nr = box.func['tarn_storage.ts_autocommit']:call({{{'insert', safe.id, {1, 'safe', 1}},
          {'insert', sid, {9, 'lazy-second', 1}}}, {wb = {1, 1}}})
    check('native mixed batch checks every source before its first write', nr[1] == 'lua'
          and box.space[safe.id]:get{1} == nil and box.space[sid]:get{9} == nil)
    nr = box.func['tarn_storage.ts_autocommit']:call({{{'insert', safe.id, {2, 'safe', 1}}}, {wb = {1}}})
    check('non-lazy control still executes natively', nr[1] == 'ok' and box.space[safe.id]:get{2} ~= nil, nr)
    box.func['tarn_storage.ts_state']:call({4})
    local valid, mismatch = pcall(box.func['tarn_storage.ts_autocommit'].call,
        box.func['tarn_storage.ts_autocommit'], {{{'insert', safe.id, {3, 'bad-layout', 1}}}, {wb = {1}}})
    check('already-ready ABI mismatch blocks the old native entrypoint', not valid
          and mismatch.code == box.error.LOAD_FUNCTION and box.space[safe.id]:get{3} == nil)
    box.func['tarn_storage.ts_state']:call({6})
    nr = box.func['tarn_storage.ts_autocommit']:call({{{'insert', safe.id, {4, 'restored', 1}}}, {wb = {1}}})
    check('matching ABI restores safe native execution', nr[1] == 'ok' and box.space[safe.id]:get{4} ~= nil)
    check('ABI6 exclusion is fresh and initialized', tarn_info().native.ready == 1)
end
print(string.format('%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
