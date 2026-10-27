-- Direct stale-gateway calls must not bypass lazy capture through the
-- native one-shard commit path, including after reads in an IPROTO stream.
local native = os.getenv('TARN_NATIVE')
if native == nil or native == '0' then
    print('native one-shard commits disabled (TARN_NATIVE): nothing to test')
    print('0 passed, 0 failed'); os.exit(0)
end
local fio, fiber, net, json = require('fio'), require('fiber'), require('net.box'), require('json')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath
local work = assert(arg[1]); fio.mktree(work)
local uri = 'unix/:' .. fio.pathjoin(work, 'iproto.sock')
box.cfg{work_dir = work, listen = uri, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'fsync',
        txn_isolation = 'read-confirmed', log_level = 3}
local tarn = require('tarn'); tarn.lazy.paused = true
tarn.init{shard_id = 1, peers = {}, native = native, test_hooks = true, check_yields = true}
fiber.sleep(.03) -- let the initial box.status watch invalidate before waiting for READY
local deadline = fiber.clock() + 3
while not tarn.lazy.quota.ready() and fiber.clock() < deadline do fiber.sleep(.01) end
assert(tarn.lazy.quota.ready(), 'quota did not reach READY')
local function ok(r) if r[1] ~= 'ok' then error(json.encode(r), 2) end; return r end
ok(tarn_bucket_init(16, {{1, 16}}))
local pass, fail = 0, 0
local function check(name, yes, extra)
    if yes then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. ' ' .. json.encode(extra)) end
end
local function def(name, sid)
    return {name = name, id = sid, ver = 0, engine = 'memtx', json = '{}',
        format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'unsigned'},
                  {name = 'bucket_id', type = 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}},
                   {name = 'v', unique = false, parts = {{2, 'unsigned'}}}}}
end
local source, safe, gen = def('test.lazy_native_commit', 74001), def('test.lazy_native_safe', 74010), 74002
ok(tarn_create_table(source)); ok(tarn_create_table(safe))
source.lazy = {{gen = gen, base_space_id = source.id, local_index_id = 2, route_parts = 1,
    key_fields = {2}, key_types = {'unsigned'}, pk_fields = {1}, pk_native_fields = {1},
    pk_types = {'unsigned'}, bucket_field = 3}}
ok(tarn_lazy_control('register', source))
deadline = fiber.clock() + 3
while not tarn.lazy.quota.ready() and fiber.clock() < deadline do fiber.sleep(.01) end
assert(tarn.lazy.quota.ready(), 'fixture quota did not remain READY')
ok(tarn_autocommit({{'insert', source.id, {1, 10, 1}}, {'insert', safe.id, {1, 100, 1}}},
                  {fence = {source.id, 0, safe.id, 0}, wb = {1, 1}}))
local function deliver()
    local events = ok(tarn_lazy_control('pending'))[2]
    local route = {}; for _, e in ipairs(events) do route[#route + 1] = e[5] end
    ok(tarn_lazy_control('ack', ok(tarn_lazy_apply(events, route))[2]))
end
deliver(); tarn.lazy.fresh()
ok(tarn_autocommit({}, {}))
local ns = require('ffi').cast('struct tarn_ns *', box.func['tarn_storage.ts_state']:call({8}))
assert(tarn.cfg.native and ns.layout == 8 and ns.ready == 1, 'actual native ABI8 is required')
print('IDENTITY ' .. json.encode{version = box.info.version, layout = tonumber(ns.layout), ready = tonumber(ns.ready)})
local c = net.connect(uri, {user = 'tarn', password = 'tarn', wait_connected = 3})
assert(c:is_connected(), 'private stream connection failed')
local serial = 0
local function args(writes, reads, fence)
    serial = serial + 1
    local wb = {}; for _ = 1, #writes do wb[#wb + 1] = 1 end
    return {'lazy-native-commit-' .. serial, writes, reads or {}, {}, box.NULL, fence or box.NULL, wb, box.NULL}
end
local function snapshot()
    box.begin{txn_isolation = 'read-confirmed'}
    local counter = box.space._tarn_lazy_counter:get{1}
    local r = require('msgpack').encode({box.space[source.id]:select{}, box.space[safe.id]:select{},
        counter and counter.seq or 0, box.space._tarn_lazy_outbox:select{}})
    box.rollback(); return r
end
local function refused(name, writes)
    local before, n0 = snapshot(), tonumber(ns.c_to_lua)
    local r = c:call('tarn_storage.ts_commit_local', args(writes))
    check(name, r[1] == 'lua' and snapshot() == before and tonumber(ns.c_to_lua) == n0 + 1, r)
end
-- No fence is supplied: a stale gateway cannot omit capture admission by
-- omitting the version list, and every source is checked before writes.
refused('native lazy-source commit without a fence falls back before mutation',
        {{'insert', source.id, {2, 20, 1}}})
refused('safe-first/lazy-second native commit cannot make its first write',
        {{'insert', safe.id, {2, 200, 1}}, {'insert', source.id, {2, 20, 1}}})
ns.capture_stale = 1
refused('unknown capture registry excludes even a non-lazy native commit', {{'insert', safe.id, {2, 200, 1}}})
ns.capture_stale = 0
local saved = ns.write_excluded
ns.write_excluded = nil
refused('missing capture exclusion array refuses before a native commit writes', {{'insert', safe.id, {2, 200, 1}}})
ns.write_excluded = saved
local served = tonumber(ns.c_served)
local r = c:call('tarn_storage.ts_commit_local', args({{'insert', safe.id, {2, 200, 1}}}))
check('non-lazy native commit still serves and commits', r[1] == 'ok' and r[2] == 1
      and box.space[safe.id]:get{2}[2] == 200 and tonumber(ns.c_served) == served + 1, r)
local function stream()
    local s = c:new_stream()
    ok(s:call('tarn_select', {safe.id, 0, {1}, 'EQ', 1, box.NULL, false, false, box.NULL, true}))
    return s
end
local s = stream()
local tid = s:eval('return box.txn_id()')
local writes = {{'insert', safe.id, {3, 300, 1}}, {'insert', source.id, {3, 30, 1}}}
local packet = args(writes, {{safe.id, 0, {1}, 1}}, {safe.id, 0, source.id, 0})
local before = snapshot()
r = s:call('tarn_storage.ts_commit_local', packet)
check('mixed native fallback preserves the original stream transaction and writes nothing',
      r[1] == 'lua' and s:eval('return box.txn_id()') == tid and snapshot() == before, r)
ok(tarn_autocommit({{'replace', safe.id, {1, 101, 1}}}, {fence = {safe.id, 0}, wb = {1}}))
r = s:call('tarn_commit_local', packet)
check('fallback keeps the stream read tracker: concurrent overwrite conflicts without capture',
      r[1] == 'conflict' and r[2] == 1213 and box.space[safe.id]:get{3} == nil
      and box.space[source.id]:get{3} == nil and box.space._tarn_lazy_counter:get{1}.seq == 1
      and #ok(tarn_lazy_control('pending'))[2] == 0, r)
pcall(s.rollback, s)
s = stream()
packet = args({{'insert', source.id, {4, 40, 1}}}, {{safe.id, 0, {1}, 1}}, {source.id, 0})
r = s:call('tarn_storage.ts_commit_local', packet)
check('an uncontended lazy-source stream also returns an unwritten fallback',
      r[1] == 'lua' and box.space[source.id]:get{4} == nil, r)
r = s:call('tarn_commit_local', packet)
local events = ok(tarn_lazy_control('pending'))[2]
check('Lua fallback can commit once in that stream and capture the membership',
      r[1] == 'ok' and r[2] == 1 and box.space[source.id]:get{4}[2] == 40
      and box.space._tarn_lazy_counter:get{1}.seq == 2 and #events == 1
      and events[1][4] == 2, r)
deliver()
check('the captured fallback write actually converges after delivery',
      #ok(tarn_lazy_control('pending'))[2] == 0 and box.space[gen].index.ordered:count{true, 40} == 1)
pcall(s.rollback, s); c:close()
print(string.format('%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
