-- The floors of a retired lazy incarnation are deleted once its target is
-- dropped (issue #177, tla/LazyBaseline.tla ClearFloors): at most 128 in a
-- transaction, only in buckets this shard may write. A frame for the
-- retired incarnation that arrives afterwards is still acknowledged from
-- the retirement, and the floors of a live incarnation stay.
local fio, fiber, json = require('fio'), require('fiber'), require('json')
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
    local deadline = fiber.clock() + (secs or 10)
    repeat if fn() then return true end; fiber.sleep(0.01) until fiber.clock() > deadline
    return fn()
end
local function copy(t) local o = {}; for k, v in pairs(t) do o[k] = v end; return o end
local function depth() return tarn_lazy_stats()[2].depth end
local floors = box.space._tarn_lazy_floor
local function nfloors(gen) return floors:count{gen} end
-- This shard owns all 16 buckets; a bucket outside the map (20) is one
-- it may not write.
ok(tarn_bucket_init(16, {{1, 16}}))

for i, engine in ipairs({'memtx', 'vinyl'}) do
    local sid, gen = 64000 + i * 10, 64001 + i * 10
    local name = 'test.lazy_retired_' .. engine
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
    local function bucket(id) return id % 16 + 1 end
    local function rekey(round)
        for id = 1, 64 do
            ok(tarn_autocommit({{'replace', sid, {id, 'k' .. ((id + round) % 8), bucket(id)}}},
                               {fence = {sid, def.ver}, wb = {bucket(id)}}))
        end
    end
    local function collected(g)
        return wait(function()
            local sp = box.space[g]
            return depth() == 0 and sp ~= nil and sp.index.tombstones:min{false} == nil
        end)
    end
    -- Rows over buckets 1 to 16, then a key change of each: the old keys'
    -- tombstones are collected under floors.
    rekey(0); rekey(1)
    check(engine .. ': tombstones are collected under floors', collected(gen) and nfloors(gen) > 0, nfloors(gen))

    -- A second incarnation, which stays; its own floors after more changes.
    local more = copy(def)
    local second = copy(def.lazy[1]); second.gen = gen + 1
    more.ver, more.lazy = 1, {def.lazy[1], second}
    local alt = {ver = 1, timeout = 5, def = more, builds = {}, checks = {}, lazy_drop = {}}
    ok(tarn_alter_prepare(name, alt, 2))
    repeat local r = ok(tarn_lazy_bootstrap(name, alt, 2)); if r[2] then break end until false
    assert(wait(function() local r = ok(tarn_lazy_bootstrap_status(name, alt, 2)); return r[2] and r[3] == 0 end))
    ok(tarn_lazy_ready(name, alt, 2)); ok(tarn_alter_apply(name, alt, 2))
    def = more
    -- One change held back: its frame for the first incarnation is kept to
    -- deliver again after the retirement.
    ok(tarn_lazy_control('pause'))
    ok(tarn_autocommit({{'replace', sid, {1, 'late', bucket(1)}}}, {fence = {sid, def.ver}, wb = {bucket(1)}}))
    local late
    for _, o in box.space._tarn_lazy_outbox.index.generation:pairs({gen}) do late = o.assignment end
    assert(late ~= nil)
    ok(tarn_lazy_control('resume'))
    rekey(2)
    check(engine .. ': the second incarnation collects under its own floors',
          collected(gen) and collected(gen + 1) and nfloors(gen + 1) > 0, {nfloors(gen), nfloors(gen + 1)})
    local kept = nfloors(gen + 1)

    -- Retire the first incarnation.
    local nd = copy(def)
    nd.ver, nd.lazy = 2, {second}
    local drop = {ver = 2, timeout = 5, def = nd, builds = {}, checks = {}, lazy_drop = {gen}}
    ok(tarn_alter_prepare(name, drop, 3)); ok(tarn_alter_apply(name, drop, 3))
    def = nd
    check(engine .. ': the retired incarnation\'s target is dropped and its floors deleted',
          wait(function() return box.space[gen] == nil and nfloors(gen) == 0 end), nfloors(gen))
    check(engine .. ': the live incarnation keeps its floors', nfloors(gen + 1) == kept, {nfloors(gen + 1), kept})
    local r = tarn_lazy_apply({late}, {late[5]})
    check(engine .. ': a late frame for the retired incarnation is acknowledged, and writes nothing',
          r[1] == 'ok' and #r[2] == 1 and box.space[gen] == nil and nfloors(gen) == 0, r)

    -- Floors a move brings back from a node that kept them: 300 in this
    -- shard's buckets, one in a bucket it may not write.
    ok(tarn_lazy_control('pause'))
    box.begin()
    local n = 0
    for b = 1, 16 do
        for source = 1, 32 do
            if n < 300 then floors:insert{gen, b, source, 1}; n = n + 1 end
        end
    end
    floors:insert{gen, 20, 1, 1}
    box.commit()
    check(engine .. ': one pass deletes at most 128 floors',
          tarn.lazy.clear_retired() == true and nfloors(gen) == 301 - 128, nfloors(gen))
    ok(tarn_lazy_control('resume'))
    check(engine .. ': the worker deletes the rest in the buckets this shard may write',
          wait(function() return nfloors(gen) == 1 end) and floors:get{gen, 20, 1} ~= nil, nfloors(gen))
    ok(tarn_lazy_control('pause'))
    check(engine .. ': nothing is left to delete', tarn.lazy.clear_retired() == false and nfloors(gen) == 1)
    floors:delete{gen, 20, 1}
    ok(tarn_lazy_control('resume'))
    check(engine .. ': the live incarnation still collects', (function()
        rekey(3)
        return collected(gen + 1) and nfloors(gen + 1) >= kept
    end)(), nfloors(gen + 1))
end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
