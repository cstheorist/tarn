-- Lazy-index baselines take their bucket's counter, and writes keep no
-- per-row version ledger (issue #177, tla/LazyBaseline.tla). Entries an
-- older release left in _tarn_lazy_version are deleted in bounded batches.
local fio, fiber, mp, json = require('fio'), require('fiber'), require('msgpack'), require('json')
local vb = require('varbinary')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local dir = assert(arg[1]); fio.mktree(dir)
box.cfg{work_dir = dir, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'write',
        txn_isolation = 'read-confirmed', log_level = 3}
local tarn = require('tarn').init{shard_id = 1, peers = {}, test_hooks = true, check_yields = true, native = false}
-- Let the initial status watch and independent quota-rebuild worker run.
fiber.sleep(0.03)
local meter_deadline = fiber.clock() + 3
while not tarn.lazy.quota.ready() and fiber.clock() < meter_deadline do fiber.sleep(0.01) end
assert(tarn.lazy.quota.ready(), 'lazy admission meter did not reach READY')
local passed, failed = 0, 0
local function check(label, yes, extra)
    if yes then passed = passed + 1; print('  ok   ' .. label)
    else failed = failed + 1; print('  FAIL ' .. label .. ' ' .. json.encode(extra)) end
end
local function ok(r) assert(r[1] == 'ok', json.encode(r)); return r end
local function wait(fn, secs)
    local deadline = fiber.clock() + (secs or 5)
    repeat if fn() then return true end; fiber.sleep(0.01) until fiber.clock() > deadline
    return fn()
end
local function copy(t) local o = {}; for k, v in pairs(t) do o[k] = v end; return o end
local function depth() return tarn_lazy_stats()[2].depth end
ok(tarn_bucket_init(16, {{1, 16}}))

for i, engine in ipairs({'memtx', 'vinyl'}) do
    local sid, gen = 62000 + i * 10, 62001 + i * 10
    local name = 'test.lazy_baseline_' .. engine
    local def = {name = name, id = sid, ver = 0, engine = engine, json = '{}',
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'string', is_nullable = true},
                  {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{field = 2, type = 'string', is_nullable = true}}}},
        lazy = {{gen = gen, base_space_id = sid, local_index_id = 2, route_parts = 1,
                 key_fields = {2}, key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1},
                 pk_types = {'unsigned'}, bucket_field = 3}}}
    local a = {def = def, timeout = 5}
    ok(tarn_create_prepare(name, a, 1))
    repeat local r = ok(tarn_lazy_bootstrap(name, a, 1)); if r[2] then break end until false
    ok(tarn_lazy_ready(name, a, 1)); ok(tarn_create_apply(name, a))
    local function write(w, bucket) return ok(tarn_autocommit({w}, {fence = {sid, def.ver}, wb = {bucket}})) end
    local function counters()
        local out = {}
        for b = 1, 2 do local t = box.space._tarn_lazy_counter:get{b}; out[b] = t and t.seq or 0 end
        return out
    end
    -- Bucket 1: inserts, a key change, a delete and a reinsert, a delete;
    -- bucket 2: inserts only. Each write takes its bucket's next sequence
    -- (the counters are the buckets', across tables).
    local start = counters()
    for _, id in ipairs({1, 2, 3, 4}) do write({'insert', sid, {id, 'a', 1}}, 1) end
    for _, id in ipairs({11, 12, 13}) do write({'insert', sid, {id, 'a', 2}}, 2) end
    write({'update', sid, {2}, {{'=', 2, 'b'}}}, 1)
    write({'delete', sid, {3}}, 1)
    write({'insert', sid, {3, 'b', 1}}, 1)
    write({'delete', sid, {4}}, 1)
    check(engine .. ': writes keep no per-row version', box.space._tarn_lazy_version:count{sid} == 0,
          box.space._tarn_lazy_version:count{sid})
    check(engine .. ': the first incarnation drains', wait(function() return depth() == 0 end))
    local counter = counters()

    -- A second incarnation over the existing rows; delivery paused, so the
    -- baseline can be read from the outbox.
    ok(tarn_lazy_control('pause'))
    local more = copy(def)
    local second = copy(def.lazy[1]); second.gen = gen + 1
    more.ver, more.lazy = 1, {def.lazy[1], second}
    local alt = {ver = 1, timeout = 5, def = more, builds = {}, checks = {}, lazy_drop = {}}
    ok(tarn_alter_prepare(name, alt, 2))
    repeat local r = ok(tarn_lazy_bootstrap(name, alt, 2)); if r[2] then break end until false
    local baseline, wrong = {}, {}
    for _, o in box.space._tarn_lazy_outbox.index.generation:pairs({gen + 1}) do
        local x = o.assignment
        baseline[x[7][1]] = x
        if x[10] ~= true or x[4] ~= counter[x[3]] then wrong[#wrong + 1] = {x[7][1], x[3], x[4]} end
    end
    local ids = {}; for id in pairs(baseline) do ids[#ids + 1] = id end
    table.sort(ids)
    check(engine .. ': one baseline assignment per live row', json.encode(ids) == json.encode({1, 2, 3, 11, 12, 13}), ids)
    check(engine .. ': each takes its bucket counter, not the row\'s last sequence',
          #wrong == 0 and counter[1] == start[1] + 8 and counter[2] == start[2] + 3,
          {wrong = wrong, counter = counter, start = start})
    ok(tarn_lazy_control('resume'))
    check(engine .. ': the baseline is acknowledged before READY', wait(function()
        local r = ok(tarn_lazy_bootstrap_status(name, alt, 2)); return r[2] and r[3] == 0
    end))
    ok(tarn_lazy_ready(name, alt, 2)); ok(tarn_alter_apply(name, alt, 2))
    def = more
    local target = box.space[gen + 1]
    local function live(key)
        local out = {}
        for _, t in target.index.ordered:pairs({true, key}, {iterator = 'EQ'}) do out[#out + 1] = t.p1 end
        table.sort(out)
        return json.encode(out)
    end
    check(engine .. ': the new incarnation holds the rows\' current keys',
          live('a') == json.encode({1, 11, 12, 13}) and live('b') == json.encode({2, 3}), {live('a'), live('b')})

    -- Later writes take greater sequences and supersede the baseline.
    write({'update', sid, {1}, {{'=', 2, 'c'}}}, 1)
    write({'delete', sid, {11}}, 2)
    check(engine .. ': later writes supersede their rows\' baseline', wait(function()
        return depth() == 0 and live('a') == json.encode({12, 13}) and live('c') == json.encode({1})
    end), {live('a'), live('c')})
    -- A baseline frame delivered again, behind those writes: acknowledged
    -- as obsolete, it brings neither row back.
    for _, id in ipairs({1, 11}) do
        local x = baseline[id]
        local r = tarn_lazy_apply({x}, {x[5]})
        check(engine .. ': a late baseline for row ' .. id .. ' is acknowledged as obsolete',
              r[1] == 'ok' and #r[2] == 1 and live('a') == json.encode({12, 13}), {r, live('a')})
    end
    -- Once the tombstones are collected under floors, the same frames still
    -- stay out.
    local function tombstones()
        local n = 0
        for _ in target.index.tombstones:pairs({false}, {iterator = 'EQ'}) do n = n + 1 end
        return n
    end
    check(engine .. ': tombstones are collected under the floors', wait(function() return tombstones() == 0 end, 10),
          tombstones())
    for _, id in ipairs({1, 11}) do
        local x = baseline[id]
        local r = tarn_lazy_apply({x}, {x[5]})
        check(engine .. ': after collection, a late baseline for row ' .. id .. ' stays out',
              r[1] == 'ok' and live('a') == json.encode({12, 13}), {r, live('a')})
    end
end

-- Entries an older release left: deleted at most 128 in a transaction, and
-- only in buckets this shard may write.
ok(tarn_lazy_control('pause'))
local versions = box.space._tarn_lazy_version
box.begin()
for i = 1, 300 do versions:insert{63000, vb.new(mp.encode({i})), i % 16 + 1, i} end
versions:insert{63000, vb.new(mp.encode({1000})), 17, 1}   -- (no such bucket here)
box.commit()
local deleted = tarn.lazy.clear_legacy()
check('one cleanup pass deletes one batch', deleted and versions:len() == 301 - 128, versions:len())
ok(tarn_lazy_control('resume'))
check('the worker deletes the rest, except in a bucket this shard does not own',
      wait(function() return versions:len() == 1 end, 10) and versions:get{63000, vb.new(mp.encode({1000}))} ~= nil,
      versions:len())
check('a pass with nothing it may delete deletes nothing', tarn.lazy.clear_legacy() == false)

print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
