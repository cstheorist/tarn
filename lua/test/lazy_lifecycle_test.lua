-- Isolated end-to-end lazy lifecycle, delivery and typed cursor tests.
local fio, fiber, mp = require('fio'), require('fiber'), require('msgpack')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local dir = assert(arg[1]); fio.mktree(dir)
box.cfg{work_dir = dir, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'write',
        txn_isolation = 'read-confirmed', log_level = 3}
local tarn = require('tarn').init{shard_id = 1, peers = {}, test_hooks = true, check_yields = true, native = false}
-- Let the initial status watch and independent quota-rebuild worker run;
-- a caller never rebuilds/waits on its own transaction INFLIGHT marker.
require('fiber').sleep(0.03)
local meter_deadline = require('fiber').clock() + 3
while not tarn.lazy.quota.ready() and require('fiber').clock() < meter_deadline do require('fiber').sleep(0.01) end
assert(tarn.lazy.quota.ready(), 'lazy admission meter did not reach READY')
local passed, failed = 0, 0
local function check(label, yes, extra)
    if yes then passed = passed + 1; print('  ok   ' .. label)
    else failed = failed + 1; print('  FAIL ' .. label .. ' ' .. require('json').encode(extra)) end
end
local function ok(r) assert(r[1] == 'ok', require('json').encode(r)); return r end
local function wait(fn)
    local deadline = fiber.clock() + 5
    repeat if fn() then return true end; fiber.sleep(0.01) until fiber.clock() > deadline
    return false
end
ok(tarn_bucket_init(16, {{1, 16}}))
for i, engine in ipairs({'memtx', 'vinyl'}) do
    local sid, gen = 61000 + i * 10, 61001 + i * 10
    local name = 'test.lazy_lifecycle_' .. engine
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
    local early = tarn_create_apply(name, a)
    check(engine .. ' cannot publish a building lazy descriptor', early[1] == 'error' and early[2] == 1412, early)
    repeat local r = ok(tarn_lazy_bootstrap(name, a, 1)); if r[2] then break end until false
    check(engine .. ' empty bootstrap reaches done with no pending', ok(tarn_lazy_bootstrap_status(name, a, 1))[2])
    ok(tarn_lazy_ready(name, a, 1)); ok(tarn_create_apply(name, a))
    local function write(w) return tarn_autocommit({w}, {fence = {sid, def.ver}, wb = {1}}) end
    for _, id in ipairs({256, 2, 128, 1, 255}) do ok(write({'insert', sid, {id, 'same', 1}})) end
    check(engine .. ' leader shipper delivers and durably drains local outbox', wait(function()
        return tarn_lazy_stats()[2].depth == 0
    end))
    local after, seen = box.NULL, {}
    repeat
        local r = ok(tarn_lazy_select(gen, {'same'}, box.index.EQ, 2, after, {tarn.lazy.bucket(def.lazy[1], {'same'})}))
        for _, row in ipairs(r[3]) do seen[#seen + 1] = row[2][1] end
        after = r[4]
    until after == box.NULL
    check(engine .. ' opaque cursor orders numeric PK ties by native type', mp.encode(seen) == mp.encode({1, 2, 128, 255, 256}), seen)
    ok(tarn_lazy_control('pause'))
    ok(write({'update', sid, {1}, {{'=', 2, 'old-pending'}}}))
    local nd = {}
    for k, v in pairs(def) do nd[k] = v end
    nd.ver = 1
    nd.lazy = {}
    for _, spec in ipairs(def.lazy) do local n = {}; for k, v in pairs(spec) do n[k] = v end; n.gen = gen + 1; nd.lazy[#nd.lazy + 1] = n end
    local clear = {id = sid, ver = 1, timeout = 5, def = nd, lazy_drop = {gen}, lazy_empty = true}
    ok(tarn_truncate_prepare(name, clear, 2))
    repeat local r = ok(tarn_lazy_bootstrap(name, clear, 2)); if r[2] then break end until false
    local status = ok(tarn_lazy_bootstrap_status(name, clear, 2))
    check(engine .. ' empty rotation ignores pending frames for the retiring generation', status[2] and status[3] == 0, status)
    ok(tarn_lazy_ready(name, clear, 2)); ok(tarn_truncate_apply(name, clear))
    def = nd
    ok(write({'insert', sid, {1, 'new', 1}}))
    ok(tarn_lazy_control('resume'))
    check(engine .. ' retired frames drain without repopulating the new generation', wait(function()
        return tarn_lazy_stats()[2].depth == 0
    end) and box.space[sid]:len() == 1 and box.space[gen + 1].index.ordered:count{true} == 1)
    -- Add another global copy over existing committed rows. The first
    -- generation's queue is independent of this new baseline's cursor.
    local more = {}
    for k, v in pairs(def) do more[k] = v end
    more.ver, more.lazy = 2, {def.lazy[1]}
    local second = {}; for k, v in pairs(def.lazy[1]) do second[k] = v end; second.gen = gen + 2
    more.lazy[2] = second
    local alt = {ver = 2, timeout = 5, def = more, builds = {}, checks = {}, lazy_drop = {}}
    ok(tarn_alter_prepare(name, alt, 3))
    local future = tarn_autocommit({{'replace', sid, {99, 'too-early', 1}}}, {fence = {sid, 2}, wb = {1}})
    check(engine .. ' forward-schema writes cannot pass a merely PREPARED capture registry',
          future[1] == 'error' and future[2] == 1412 and box.space[sid]:get{99} == nil, future)
    repeat local r = ok(tarn_lazy_bootstrap(name, alt, 3)); if r[2] then break end until false
    check(engine .. ' confirmed baseline is delivered before READY', wait(function()
        local r = ok(tarn_lazy_bootstrap_status(name, alt, 3)); return r[2] and r[3] == 0
    end))
    ok(tarn_lazy_ready(name, alt, 3)); ok(tarn_alter_apply(name, alt, 3))
    check(engine .. ' bootstrap indexes the existing physical row exactly once', box.space[gen + 2].index.ordered:count{true} == 1)
    def = more
    local clear_def = {}; for k, v in pairs(def) do clear_def[k] = v end
    clear_def.ver, clear_def.lazy = 3, {}
    local retired = {}
    for n, spec in ipairs(def.lazy) do
        local rotated = {}; for k, v in pairs(spec) do rotated[k] = v end
        rotated.gen = gen + 2 + n; clear_def.lazy[n] = rotated; retired[n] = spec.gen
    end
    local clear_args = {id = sid, ver = 3, timeout = 5, def = clear_def, lazy_drop = retired, lazy_empty = true}
    ok(tarn_truncate_prepare(name, clear_args, 4))
    repeat local r = ok(tarn_lazy_bootstrap(name, clear_args, 4)); if r[2] then break end until false
    ok(tarn_lazy_ready(name, clear_args, 4))
    ok(tarn_test_hook('lazy_clear_schema', 'hold'))
    local finished
    local held_clear = fiber.new(function() finished = tarn_truncate_apply(name, clear_args, 4) end)
    held_clear:set_joinable(true)
    assert(tarn_test_hook('lazy_clear_schema', 'reached')[2] == 1)
    local retry_args = {}; for k, v in pairs(clear_args) do retry_args[k] = v end; retry_args.timeout = .03
    local competing = tarn_truncate_apply(name, retry_args, 4)
    check(engine .. ' an overlapping clear finisher cannot pass native DDL publication',
          competing[1] == 'timeout' and box.space._tarn_catalog:get{name}.def.ver == 2, competing)
    ok(tarn_test_hook('lazy_clear_schema', 'release')); assert(held_clear:join()); ok(finished)
    def = clear_def
    ok(write({'insert', sid, {99, 'after-clear', 1}}))
    ok(tarn_truncate_apply(name, clear_args, 4))
    check(engine .. ' late clear replay does not erase post-publication writes', box.space[sid]:get{99} ~= nil)
    local legacy = {ver = 4, timeout = 5, def = {id = sid, ver = 4, name = name}}
    local r = tarn_alter_prepare(name, legacy, 3)
    check(engine .. ' old gateway DDL without lazy presence fails before effects', r[1] == 'error' and r[2] == 1412, r)
    -- The intended wire ID must survive DROP+CREATE of the same name.
    box.space[sid]:drop()
    local replacement = {name = name, id = sid + 200, ver = 0, engine = engine,
                         format = def.format, indexes = def.indexes, json = '{}'}
    ok(tarn_create_table(replacement)); box.space[replacement.id]:replace{7, 'replacement', 1}
    r = tarn_truncate_apply(name, clear_args, 4)
    local stale_drop = tarn_drop_apply(name, {id = sid, ver = 4, timeout = 1, lazy_drop = retired}, 5)
    check(engine .. ' stale clear arguments cannot mutate a replacement space', r[1] == 'error' and r[2] == 1412
          and stale_drop[1] == 'error' and stale_drop[2] == 1412
          and box.space[replacement.id]:get{7}[2] == 'replacement', {r, stale_drop})
end
print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
