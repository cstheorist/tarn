-- Online lazy builds (issue #184, tla/LazyBaseline.tla): an ALTER's new lazy
-- index starts capturing the table's writes behind a short fence
-- (tarn_lazy_start), its baseline scan then runs while writes go on, and
-- PREPARE finds it done. The new incarnation then holds exactly the rows'
-- current keys, however the writes interleaved with the scan.
local fio, fiber, json = require('fio'), require('fiber'), require('json')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local dir = assert(arg[1]); fio.mktree(dir)
box.cfg{work_dir = dir, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'write',
        txn_isolation = 'read-confirmed', log_level = 3}
local tarn = require('tarn').init{shard_id = 1, peers = {}, test_hooks = true, check_yields = true, native = false}
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
local BUCKETS, ROWS = 16, 600

for i, engine in ipairs({'memtx', 'vinyl'}) do
    local sid, gen = 64000 + i * 10, 64001 + i * 10
    local name = 'test.lazy_online_' .. engine
    local def = {name = name, id = sid, ver = 0, engine = engine, json = '{}',
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'string', is_nullable = true},
                  {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{field = 2, type = 'string', is_nullable = true}}}},
        lazy = {}}
    ok(tarn_create_prepare(name, {def = def, timeout = 5}, 1)); ok(tarn_create_apply(name, {def = def, timeout = 5}))
    local rows = {}   -- id -> v, what the table holds
    local function bucket(id) return id % BUCKETS + 1 end
    local function write(w, id, ver)
        return tarn_autocommit({w}, {fence = {sid, ver or def.ver}, wb = {bucket(id)}})
    end
    for id = 1, ROWS do
        ok(write({'insert', sid, {id, 'k' .. (id % 7), bucket(id)}}, id)); rows[id] = 'k' .. (id % 7)
    end

    local spec = {gen = gen, base_space_id = sid, local_index_id = 2, route_parts = 1,
                  key_fields = {2}, key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1},
                  pk_types = {'unsigned'}, bucket_field = 3}
    local more = copy(def); more.ver, more.lazy = 1, {spec}
    local alt = {ver = 1, timeout = 5, def = more, builds = {}, checks = {}, lazy_drop = {}}

    -- A change that adds the indexed column cannot capture before APPLY.
    local widened = copy(more)
    widened.format = {def.format[1], {name = 'w', type = 'string', is_nullable = true}, def.format[3]}
    local r = tarn_lazy_start(name, {ver = 1, timeout = 5, def = widened, builds = {}, checks = {}, lazy_drop = {}}, 2)
    check(engine .. ': an index on a changed column builds behind the fence', r[1] == 'error' and r[2] == 1846, r)
    check(engine .. ': that refusal leaves no descriptor', box.space._tarn_lazy_defs:get{gen} == nil)

    -- The fenced build refuses this table above alter_check_rows; the online
    -- one does not count against it.
    local limit = tarn.cfg.alter_check_rows
    tarn.cfg.alter_check_rows = math.floor(ROWS / 2)

    -- A writer changes keys, deletes and inserts while the build runs.
    local stop, paused, writes, failures, next_id = false, false, 0, {}, ROWS + 1
    local writer = fiber.new(function()
        local n = 0
        while not stop do
            while paused and not stop do fiber.sleep(0.01) end
            n = n + 1
            local id, res = (n * 37) % ROWS + 1, nil
            if n % 5 == 0 then
                res = write({'delete', sid, {id}}, id)
                if res[1] == 'ok' then rows[id] = nil end
            elseif n % 5 == 1 then
                local nid = next_id; next_id = next_id + 1
                res = write({'insert', sid, {nid, 'k' .. (n % 9), bucket(nid)}}, nid)
                if res[1] == 'ok' then rows[nid] = 'k' .. (n % 9) end
            elseif rows[id] ~= nil then
                local v = 'k' .. (n % 11)
                res = write({'update', sid, {id}, {{'=', 2, v}}}, id)
                if res[1] == 'ok' then rows[id] = v end
            end
            if res ~= nil then
                if res[1] == 'ok' then writes = writes + 1
                -- the start's fence refuses writes of the old definition
                -- for a moment (the gateway retries them)
                elseif res[2] ~= 1412 then failures[#failures + 1] = res end
            end
            fiber.sleep(0.001)
        end
    end)
    writer:set_joinable(true)

    ok(tarn_lazy_start(name, alt, 2))
    check(engine .. ': the start stores the descriptor and turns capture on',
          box.space._tarn_lazy_defs:get{gen}.phase == 'building' and box.space._tarn_lazy_online:get{gen} ~= nil)
    check(engine .. ': a second start finds capture on', tarn_lazy_start(name, alt, 2)[1] == 'ok')
    local moved = tarn_bucket_send_begin(1, 2, 'm-online', 1)
    check(engine .. ': a bucket move waits for the build', moved[1] == 'timeout' and moved[2] == 1205
          and tostring(moved[3]):find('lazy index build') ~= nil, moved)
    local before = writes
    check(engine .. ': writes go on after the start', wait(function() return writes > before + 20 end), writes)
    local captured = box.space._tarn_lazy_outbox.index.generation:count{gen}
        + (box.space[gen] and box.space[gen]:len() or 0)
    check(engine .. ': writes during the build are captured for it before its scan', captured > 0, captured)

    -- Hold one batch between its reads and its write while rows change, and
    -- let a takeover's start be refused nothing.
    ok(tarn_test_hook('lazy_bootstrap', 'hold'))
    local held = fiber.new(function() return tarn_lazy_bootstrap(name, alt, 2) end)
    held:set_joinable(true)
    ok(tarn_test_hook('lazy_bootstrap', 'reached'))
    local during = writes
    check(engine .. ': rows change while a batch is held', wait(function() return writes > during + 20 end), writes)
    ok(tarn_test_hook('lazy_bootstrap', 'release'))
    local _, res = held:join()
    check(engine .. ': the held batch is written', res[1] == 'ok', res)

    -- A batch whose replica stops leading while it is held writes nothing,
    -- and raises the ER_READONLY its write met. That sends it to the new
    -- leader, where an error status would fail the ALTER. (A trigger stands
    -- in for the replica's loss of leadership: toggling read_only would
    -- also rebuild the lazy admission meter, which refuses first.)
    paused = true
    ok(tarn_test_hook('lazy_bootstrap', 'hold'))
    held = fiber.new(function() return tarn_lazy_bootstrap(name, alt, 2) end)
    held:set_joinable(true)
    ok(tarn_test_hook('lazy_bootstrap', 'reached'))
    local cursor = require('msgpack').encode(box.space._tarn_lazy_defs:get{gen}.cursor)
    local function readonly() box.error(box.error.READONLY) end
    box.space._tarn_lazy_defs:before_replace(readonly)
    ok(tarn_test_hook('lazy_bootstrap', 'release'))
    local wrote, err = held:join()
    box.space._tarn_lazy_defs:before_replace(nil, readonly)
    paused = false
    check(engine .. ': a batch that meets ER_READONLY raises it',
          not wrote and type(err) == 'cdata' and err.code == box.error.READONLY, {wrote, tostring(err)})
    check(engine .. ': and leaves the durable cursor where it was',
          require('msgpack').encode(box.space._tarn_lazy_defs:get{gen}.cursor) == cursor)

    -- A takeover: the next owner continues from the durable cursor, and the
    -- previous owner's batches are refused.
    ok(tarn_lazy_start(name, alt, 3))
    local stale = tarn_lazy_bootstrap(name, alt, 2)
    check(engine .. ': the previous owner\'s batch is refused', stale[1] == 'error' and stale[2] == 1412, stale)
    local batches = 0
    repeat
        local b = ok(tarn_lazy_bootstrap(name, alt, 3)); batches = batches + 1
    until b[2] or batches > 1000
    check(engine .. ': the scan finishes in bounded batches', batches > 1 and batches <= 1000, batches)

    stop = true; writer:join()
    check(engine .. ': the writer met no other refusal', #failures == 0, failures)
    ok(tarn_alter_prepare(name, alt, 3))
    ok(tarn_lazy_bootstrap(name, alt, 3))
    check(engine .. ': its deliveries drain', wait(function()
        local s = ok(tarn_lazy_bootstrap_status(name, alt, 3)); return s[2] and s[3] == 0
    end))
    ok(tarn_lazy_ready(name, alt, 3)); ok(tarn_alter_apply(name, alt, 3))
    tarn.cfg.alter_check_rows = limit
    def = more
    check(engine .. ': publication moves the incarnation to the published capture',
          box.space._tarn_lazy_online:get{gen} == nil and box.space._tarn_lazy_capture:get{sid} ~= nil)
    check(engine .. ': moves go on after publication', tarn.lazy.building() == nil)

    local target = box.space[gen]
    local function index()
        local out = {}
        for _, t in target.index.ordered:pairs({true}, {iterator = 'GE'}) do
            if t.live then out[#out + 1] = t.k1 .. '/' .. t.p1 end
        end
        table.sort(out)
        return out
    end
    local function expected()
        local out = {}
        for id, v in pairs(rows) do out[#out + 1] = v .. '/' .. id end
        table.sort(out)
        return out
    end
    check(engine .. ': the new index holds exactly the rows\' current keys',
          json.encode(index()) == json.encode(expected()), {got = #index(), want = #expected()})

    -- Later writes are captured as published ones.
    local id = next(rows)
    ok(write({'update', sid, {id}, {{'=', 2, 'zz'}}}, id)); rows[id] = 'zz'
    check(engine .. ': later writes reach the index', wait(function()
        return depth() == 0 and json.encode(index()) == json.encode(expected())
    end))

    -- An aborted online build retires its incarnation and stops capturing.
    local spec2 = copy(spec); spec2.gen = gen + 1
    local third = copy(more); third.ver, third.lazy = 2, {spec, spec2}
    local alt2 = {ver = 2, timeout = 5, def = third, builds = {}, checks = {}, lazy_drop = {}}
    ok(tarn_lazy_start(name, alt2, 4))
    ok(tarn_lazy_bootstrap(name, alt2, 4))
    ok(tarn_alter_abort(name, alt2, 4))
    check(engine .. ': ABORT retires the online incarnation and ends its capture',
          box.space._tarn_lazy_defs:get{gen + 1}.phase == 'retired' and box.space._tarn_lazy_online:get{gen + 1} == nil
          and tarn.lazy.building() == nil)
    ok(write({'update', sid, {id}, {{'=', 2, 'yy'}}}, id)); rows[id] = 'yy'
    check(engine .. ': writes after ABORT capture only the published index', wait(function()
        return depth() == 0 and box.space._tarn_lazy_outbox.index.generation:count{gen + 1} == 0
            and json.encode(index()) == json.encode(expected())
    end))
end

print(string.format('%d passed, %d failed', passed, failed))
os.exit(failed == 0 and 0 or 1)
