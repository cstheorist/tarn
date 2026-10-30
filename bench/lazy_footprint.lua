-- Memory and disk per lazy-index structure (issue #177).
--
--   tarantool bench/lazy_footprint.lua WORK_DIR ENGINE ROWS [LUA_DIR]
--
-- One storage node owning every bucket (1,024) and a table t(id, k, bucket_id)
-- with a lazy index on k (1,000 distinct values). It loads ROWS rows and
-- drains their assignments, deletes a tenth of them (at most 20,000) and lets
-- garbage collection collect their tombstones, holds the assignments of
-- 1,000 key changes (2,000) in the outbox, and prepares then commits 100
-- cross-shard transactions. After
-- each phase it measures every space involved: tuples, and for memtx the
-- tuple and index bytes, for vinyl the bytes on disk and in memory. The last
-- line is a JSON object of the measurements; LUA_DIR selects the Lua module
-- to measure (another release's lua/ directory, for a before/after).
local fio, fiber, json, clock = require('fio'), require('fiber'), require('json'), require('clock')
local work, engine, rows = assert(arg[1]), assert(arg[2]), assert(tonumber(arg[3]))
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
local lua_dir = arg[4] or fio.abspath(here .. '../lua')
package.path = lua_dir .. '/?.lua;' .. package.path
fio.mktree(work)
box.cfg{work_dir = work, listen = 'unix/:' .. fio.pathjoin(work, 'iproto.sock'),
        memtx_use_mvcc_engine = true, memtx_memory = 3 * 1024 * 1024 * 1024,
        vinyl_memory = 128 * 1024 * 1024, vinyl_cache = 64 * 1024 * 1024, wal_mode = 'write',
        checkpoint_interval = 0, txn_isolation = 'read-confirmed', replication_synchro_quorum = 1,
        election_mode = 'off', log_level = 2}
box.ctl.promote()
local tarn = require('tarn').init{shard_id = 1, peers = {}, sync = true, native = false,
                                  test_hooks = true, recovery_after = 1000000}
local function ok(r) assert(r[1] == 'ok', json.encode(r)); return r end
local function wait(label, fn, secs)
    local deadline = fiber.clock() + (secs or 600)
    repeat if fn() then return end; fiber.sleep(.01) until fiber.clock() >= deadline
    error(label .. ' timed out')
end
wait('schema and quota ready', function()
    return box.space._tarn_part ~= nil and tarn.lazy.quota.ready() and tarn.clock.status().read_ready
end, 10)
local BUCKETS, KEYS = 1024, 1000
ok(tarn_bucket_init(BUCKETS, {{1, BUCKETS}}))
wait('writer clock', function() return tarn.clock.check({1}) == nil end, 10)
ok(tarn_autocommit({}, {}))

local sid, gen, name = 70001, 70002, 'test.lazy_footprint'
local def = {name = name, id = sid, ver = 0, engine = engine, json = '{}',
    format = {{name = 'id', type = 'unsigned'}, {name = 'k', type = 'string', is_nullable = true},
              {name = 'bucket_id', type = 'unsigned'}},
    indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}},
               {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}},
               {name = 'k', unique = false, parts = {{field = 2, type = 'string', is_nullable = true}}}},
    lazy = {{gen = gen, base_space_id = sid, local_index_id = 2, route_parts = 1,
             key_fields = {2}, key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1},
             pk_types = {'unsigned'}, bucket_field = 3}}}
local a = {def = def, timeout = 5}
ok(tarn_create_prepare(name, a, 1))
repeat local r = ok(tarn_lazy_bootstrap(name, a, 1)); if r[2] then break end until false
ok(tarn_lazy_ready(name, a, 1)); ok(tarn_create_apply(name, a))

local function depth() return tarn_lazy_stats()[2].depth end
local function bucket(id) return id % BUCKETS + 1 end
-- An autocommit of these writes; while the outbox is full (1041) or vinyl
-- admission refuses new writes under pressure (1205), wait and retry.
local function commit(writes, wb)
    while true do
        local r = tarn_autocommit(writes, {fence = {sid, 0}, wb = wb})
        if r[1] == 'ok' then return end
        if r[2] ~= 1041 and r[2] ~= 1205 then error(json.encode(r)) end
        fiber.sleep(.05)
    end
end
local function stat(sp)
    if sp == nil then return {tuples = 0} end
    local out = {tuples = 0, engine = sp.engine, data = 0, index = 0, disk = 0, memory = 0}
    for id, ix in pairs(sp.index) do
        if type(id) == 'number' then
            if sp.engine == 'memtx' then
                out.index = out.index + ix:bsize()
            else
                local s = ix:stat()
                out.disk = out.disk + s.disk.bytes
                out.memory = out.memory + s.memory.bytes
                if id == 0 then out.tuples = s.disk.rows + s.memory.rows end
            end
        end
    end
    if sp.engine == 'memtx' then out.tuples, out.data = sp:len(), sp:bsize() end
    return out
end
local function rss()
    for line in io.lines('/proc/self/status') do
        local kb = line:match('^VmRSS:%s+(%d+)')
        if kb then return tonumber(kb) * 1024 end
    end
end
local spaces = {'_tarn_lazy_version', '_tarn_lazy_counter', '_tarn_lazy_floor', '_tarn_lazy_outbox',
                '_tarn_lazy_reservation', '_tarn_lazy_slots', '_tarn_part', '_tarn_txn'}
local function snapshot(label)
    -- (vinyl: what the last dump left in memory goes to disk first)
    if engine == 'vinyl' then box.snapshot() end
    local slab = box.slab.info()
    local out = {phase = label, rss = rss(), memory = box.info.memory(), items_used = slab.items_used,
                 arena_used = slab.arena_used, base = stat(box.space[sid]), target = stat(box.space[gen])}
    local live, dead = 0, 0
    for _, t in box.space[gen].index.tombstones:pairs({false}, {iterator = 'EQ'}) do dead = dead + 1 end
    live = box.space[gen].index.ordered:count({true})
    out.target.live, out.target.tombstones = live, dead
    for _, n in ipairs(spaces) do out[n] = stat(box.space[n]) end
    return out
end

local result = {engine = engine, rows = rows, buckets = BUCKETS, keys = KEYS, lua = lua_dir, phases = {}}
local t0 = clock.monotonic()
local n = 0
while n < rows do
    local writes, wb = {}, {}
    for _ = 1, math.min(128, rows - n) do
        n = n + 1
        writes[#writes + 1] = {'insert', sid, {n, 'k' .. (n % KEYS), bucket(n)}}
        wb[#wb + 1] = bucket(n)
    end
    commit(writes, wb)
end
wait('load drained', function() return depth() == 0 end)
result.load_seconds = clock.monotonic() - t0
result.phases[#result.phases + 1] = snapshot('loaded')

-- Deletes: tombstones, then floors once they are collected.
local deletes, step = math.min(math.floor(rows / 10), 20000), 0
step = math.max(1, math.floor(rows / math.max(deletes, 1)))
local writes, wb, done = {}, {}, 0
for id = 1, rows, step do
    if done >= deletes then break end
    writes[#writes + 1], wb[#wb + 1] = {'delete', sid, {id}}, bucket(id)
    done = done + 1
    if #writes == 128 then commit(writes, wb); writes, wb = {}, {} end
end
if #writes > 0 then commit(writes, wb) end
wait('deletes drained', function() return depth() == 0 end)
tarn.lazy.paused = true   -- (collection holds off while measured)
fiber.sleep(.2)
result.deletes = done
result.phases[#result.phases + 1] = snapshot('tombstones')
tarn.lazy.paused = false
wait('tombstones collected', function()
    return box.space[gen].index.tombstones:min({false}) == nil
end, 1800)
result.phases[#result.phases + 1] = snapshot('collected')

-- The outbox: the assignments of 1,000 key changes (an old-key delete and a
-- new-key put each), held while delivery is paused.
ok(tarn_lazy_control('pause'))
writes, wb = {}, {}
for id = rows, 1, -1 do
    if box.space[sid]:get{id} ~= nil then
        writes[#writes + 1], wb[#wb + 1] = {'update', sid, {id}, {{'=', 2, 'moved'}}}, bucket(id)
        if #writes == 1000 then break end
    end
end
for i = 1, #writes, 100 do
    local w, b = {}, {}
    for j = i, math.min(i + 99, #writes) do w[#w + 1], b[#b + 1] = writes[j], wb[j] end
    commit(w, b)
end
local held = snapshot('outbox')
local encoded = 0
for _, o in box.space._tarn_lazy_outbox:pairs() do encoded = encoded + o.bytes end
held.outbox_encoded = encoded
result.phases[#result.phases + 1] = held
ok(tarn_lazy_control('resume'))
wait('outbox drained', function() return depth() == 0 end)

-- Reservations: 100 prepared cross-shard transactions, each replacing one
-- row; then committed, leaving their protocol records until expiry.
local txids, prepared_ids = {}, {}
for id = 1, rows do
    if box.space[sid]:get{id} ~= nil then
        prepared_ids[#prepared_ids + 1] = id
        if #prepared_ids == 100 then break end
    end
end
for i, id in ipairs(prepared_ids) do
    local txid = 'fp-' .. i
    txids[i] = txid
    local r = tarn_prepare(txid, 1, {1}, {{'replace', sid, {id, 'prepared', bucket(id)}}}, {}, {},
                           clock.realtime(), box.NULL, {sid, 0}, {bucket(id)})
    assert(r[1] == 'yes', json.encode(r))
end
result.phases[#result.phases + 1] = snapshot('reserved')
for _, txid in ipairs(txids) do
    assert(tarn_txn_cas(txid, 'staging', 'committed', {1}) == 'committed')
    ok(tarn_apply(txid))
end
wait('applied drained', function() return depth() == 0 end)
result.phases[#result.phases + 1] = snapshot('committed')
result.seconds = clock.monotonic() - t0
print(json.encode(result))
os.exit(0)
