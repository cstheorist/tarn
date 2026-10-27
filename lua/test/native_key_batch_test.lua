-- Native full-primary-key batches against the existing Lua operation.
-- No generic test hook is held: served counters prove eligible reads use C.
local fio, fiber, json, net = require('fio'), require('fiber'), require('json'), require('net.box')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local native = os.getenv('TARN_NATIVE')
if native and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, listen = '127.0.0.1:3898', memtx_use_mvcc_engine = true,
    memtx_memory = 128 * 1048576, vinyl_memory = 32 * 1048576, vinyl_cache = 0,
    wal_mode = 'fsync', txn_isolation = 'read-confirmed', log_level = 3}
local tarn = require('tarn').init{shard_id = 1, peers = {}, native = native and native ~= '0' and native or false,
    check_locks = true, check_yields = true, test_hooks = true, lock_wait = .03, recovery_after = 1000000}
tarn.lazy.paused = true
local c = net.connect('tarn:tarn@127.0.0.1:3898', {wait_connected = 5}); assert(c:is_connected())
local passed, failed = 0, 0
local function check(label, yes, extra)
    if yes then passed = passed + 1; print('  ok   ' .. label)
    else failed = failed + 1; print('  FAIL ' .. label .. ' ' .. tostring(extra)) end
end
local function canon(v)
    if box.tuple.is(v) then v = v:totable() end
    if type(v) ~= 'table' then return json.encode(v) end
    local out, keys, array = {}, {}, true
    for k in pairs(v) do keys[#keys + 1] = k; if type(k) ~= 'number' then array = false end end
    if array and #keys == #v then
        for i = 1, #v do out[i] = canon(v[i]) end
        return '[' .. table.concat(out, ',') .. ']'
    end
    table.sort(keys, function(a, b) return tostring(a) < tostring(b) end)
    for _, k in ipairs(keys) do out[#out + 1] = tostring(k) .. '=' .. canon(v[k]) end
    return '{' .. table.concat(out, ',') .. '}'
end
local function ok(r) assert(r[1] == 'ok', canon(r)); return r end
ok(c:call('tarn_bucket_init', {16, {{1, 8}}}))
local sid, vsid, csid, b = 96001, 96002, 96003, 1
local function def(id, name, engine, composite)
    return {id = id, name = name, engine = engine, ver = 0, json = '{}', lazy = {},
        format = {{'id', 'unsigned'}, {'v', 'integer'}, {'payload', 'string'}, {'bucket_id', 'unsigned'},
                  {'note', 'any', is_nullable = true}},
        indexes = {{name = 'primary', unique = true, parts = composite and {{1, 'unsigned'}, {2, 'integer'}} or {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{4, 'unsigned'}}}}}
end
local base = def(sid, 'test.native_key_batch', 'memtx')
ok(c:call('tarn_create_table', {base}))
ok(c:call('tarn_create_table', {def(vsid, 'test.native_key_batch_v', 'vinyl')}))
ok(c:call('tarn_create_table', {def(csid, 'test.native_key_batch_c', 'memtx', true)}))
for _, id in ipairs({sid, vsid}) do
    c:eval('local s=box.space[...] s:replace{1,0,"a",1,box.NULL};s:replace{2,0,"b",1};s:replace{3,0,string.rep("x",4096),1,false}', {id})
end
c:eval('local s=box.space[...] s:replace{1,1,"c",1};s:replace{1,2,"d",1}', {csid})
local function args(keys, id, route)
    return {id or sid, 0, keys, 1, true, false, box.NULL, type(route) == 'nil' and b or route}
end
local function lua_read(a) return c:call('tarn_select_keys', a) end
local first = lua_read(args({{1}, {99}, {2}, {1}}))
check('Lua baseline preserves order, duplicates, misses and physical tuple widths',
    first[1] == 'ok' and #first[2] == 3 and first[2][1][1] == 1 and first[2][2][1] == 2
    and first[2][3][1] == 1 and #first[2][1] == 5 and #first[2][2] == 4)
check('Lua baseline supports complete composite keys', #lua_read(args({{1, 2}, {1, 1}}, csid))[2] == 2)
if not native or native == '0' then
    check('explicit Lua mode has no native module state', tarn.cfg.native == false and tarn_info().native == false)
    print(string.format('%d passed, %d failed', passed, failed)); c:close(); os.exit(failed == 0 and 0 or 1)
end
local info = c:call('tarn_info'); assert(info.native and info.native.ready == 1)
local ns = require('ffi').cast('struct tarn_ns *', box.func['tarn_storage.ts_state']:call())
assert(ns.layout == 11 and require('ffi').sizeof('struct tarn_ns') == 312)
print('IDENTITY ' .. json.encode{version = box.info.version, layout = tonumber(ns.layout), native = info.native})
local function census()
    return c:eval([[local f=require('ffi');local n=f.cast('struct tarn_ns *',box.func['tarn_storage.ts_state']:call())
        return {served=tonumber(n.served),to_lua=tonumber(n.to_lua),hooks=tonumber(n.hooks)}]])
end
local function native_read(a) return c:call('tarn_storage.ts_select', a) end
local function same(label, a)
    local l = lua_read(a) -- also refreshes any ordinary caches
    local before = census(); local r = native_read(a); local after = census()
    check(label, r[1] == 'ok' and canon(r) == canon(l) and after.served == before.served + 1
        and after.to_lua == before.to_lua and after.hooks == 0, canon(r) .. ' / ' .. canon(l))
end
local function handback(label, a)
    local before = census(); local r = native_read(a); local after = census()
    check(label, canon(r) == '["lua"]' and after.served == before.served
        and after.to_lua == before.to_lua + 1 and before.hooks == 0 and after.hooks == 0, canon(r))
end
local saved = {rows = tarn.cfg.scan_page_rows, bytes = tarn.cfg.scan_page_bytes, window = tarn.cfg.load_window}
local yes, err = xpcall(function()
    local before = c:call('tarn_info').native
    local cap = native_read({})
    local after = c:call('tarn_info').native
    check('capability discovery is exact and has no served/fallback counter effects',
        canon(cap) == '["native_batch_keys",1]' and before.served == after.served
        and before.to_lua == after.to_lua and canon(before.fallbacks) == canon(after.fallbacks))
    c:eval([[local f=require('ffi');local n=f.cast('struct tarn_ns *',box.func['tarn_storage.ts_state']:call());n.ready=0]])
    local cap_unready = native_read({})
    c:eval([[local f=require('ffi');local n=f.cast('struct tarn_ns *',box.func['tarn_storage.ts_state']:call());n.ready=1]])
    check('capability describes code support while data readiness remains unknown', canon(cap_unready) == canon(cap))
    local ordinary = {sid, 0, {1}, 0, 1, box.NULL, true, true, box.NULL, false, box.NULL, b}
    local l12, n12 = c:call('tarn_select', ordinary), native_read(ordinary)
    check('the existing twelve-argument read still serves its Lua twin', n12[1] == 'ok' and canon(l12) == canon(n12))
    same('hits, duplicates and misses are served natively in input order', args({{1}, {99}, {2}, {1}}))
    same('every missing key costs work but returns no tuple', args({{90}, {91}, {92}}))
    same('composite full primary keys use the same native tuple bytes', args({{1, 2}, {1, 1}, {1, 2}}, csid))
    local many = {}; for i = 1, 128 do many[i] = {i + 100} end
    same('the exact128-key all-miss work boundary is supported', args(many))
    many[129] = {229}; handback('129 keys are handed back whole', args(many))
    handback('vinyl uses the existing yielding read', args({{1}, {2}}, vsid))
    handback('primary prefixes use the existing range read', args({{1}}, csid))
    local bad = args({{1}}); bad[2] = 1; handback('secondary indexes remain Lua', bad)
    bad = args({{1}}); bad[6] = true; handback('a request which begins a stream remains Lua', bad)
    bad = args({{1}}); bad[5] = false; handback('unchecked stream reads remain Lua', bad)
    bad = args({{1}}); bad[7] = {1, box.NULL, 0}; handback('continuations remain Lua', bad)
    bad = args({{1}}); bad[3] = {7}; handback('malformed eight-argument shapes remain Lua', bad)
    handback('a route to an unowned bucket remains Lua', args({{1}}, sid, 9))
    local epoch = c:call('tarn_buckets')[3]
    handback('a stale scatter epoch remains Lua', args({{1}}, sid, {e = epoch + 1}))

    local tx = 'native-batch-missing-w'
    local p = c:call('tarn_prepare', {tx, 1, {1, 2}, {{'insert', sid, {9, 99, 'pending', b}}}, {}, {},
        require('clock').realtime(), box.NULL, {sid, 0}, {b}})
    assert(p[1] == 'yes', canon(p))
    same('an unrelated prepared W claim does not disable native batches', args({{1}, {2}}))
    handback('a W claim on a missing requested key prevents any complete result', args({{1}, {9}}))
    handback('an all-miss request still checks the prepared insert claim', args({{9}}))
    c:call('tarn_txn_cas', {tx, 'staging', 'aborted', {1, 2}}); ok(c:call('tarn_abort', {tx, 1, {1, 2}}))
    local st = c:new_stream()
    local read = st:call('tarn_select', {sid, 0, {1}, 'EQ', 1, box.NULL, true, false, box.NULL, true})
    assert(read[1] == 'ok', canon(read))
    tx = 'native-batch-r'
    p = st:call('tarn_prepare', {tx, 1, {1, 2}, {}, {{sid, 0, {1}, b}}, {}, require('clock').realtime()})
    assert(p[1] == 'yes', canon(p))
    same('prepared R claims do not block snapshot batches', args({{1}, {2}}))
    c:call('tarn_txn_cas', {tx, 'staging', 'aborted', {1, 2}}); ok(c:call('tarn_abort', {tx, 1, {1, 2}}))

    local clear_def = {}; for k, v in pairs(base) do clear_def[k] = v end; clear_def.ver = 1
    local clear = {id = sid, ver = 1, timeout = .1, def = clear_def}
    ok(c:call('tarn_truncate_prepare', {base.name, clear}))
    handback('a real durable DDL hold prevents native reads', args({{1}, {2}}))
    ok(c:call('tarn_truncate_abort', {base.name, clear}))
    c:eval('tarn.stale_fences()')
    handback('stale definition/hold census is refreshed by Lua', args({{1}}))
    same('the normal Lua refresh restores native eligibility', args({{1}, {2}}))

    c:eval('tarn.cfg.scan_page_rows=2;tarn.ns_sync()')
    same('row-cap equality after the final key is complete', args({{1}, {2}}))
    handback('a later miss cannot bypass the pre-key row cap', args({{1}, {2}, {99}}))
    local size = c:eval('return box.space[...]:get{1}:bsize()', {sid})
    c:eval('local rows,bytes=...;tarn.cfg.scan_page_rows=rows;tarn.cfg.scan_page_bytes=bytes;tarn.ns_sync()', {saved.rows, size})
    same('byte-cap equality after the final tuple is complete', args({{1}}))
    handback('a later miss cannot bypass the pre-key byte cap', args({{1}, {99}}))
    c:eval('tarn.cfg.scan_page_bytes=...;tarn.ns_sync()', {size - 1})
    handback('an oversized final tuple is conservatively handed back without a prefix', args({{1}}))
    c:eval('local rows,bytes=...;tarn.cfg.scan_page_rows=rows;tarn.cfg.scan_page_bytes=bytes;tarn.ns_sync()', {saved.rows, saved.bytes})

    local old = c:new_stream()
    old:call('tarn_select', {sid, 0, {1}, 'EQ', 1, box.NULL, true, false, box.NULL, true})
    local r = old:call('tarn_storage.ts_select', args({{1}, {2}}))
    check('an ambient transaction is handed back unchanged', canon(r) == '["lua"]', canon(r))
    c:eval('local s=box.space[...];s:replace{1,1,"a",1,box.NULL}', {sid})
    local v = old:call('tarn_validate', {'native-batch-stream', {{sid, 0, {1}, b}}, {}, false})
    check('handback retained the original overwritten stream read set', v[1] == 'conflict' and v[2] == 1213, canon(v))

    -- Real concurrent atomic writes, with no held hooks. Every reader below
    -- must be counted as served, rather than silently tested through Lua.
    c:eval('local s=box.space[...];box.begin();s:replace{1,0,"a",1,box.NULL};s:replace{2,0,"b",1};box.commit()', {sid})
    lua_read(args({{1}, {2}}))
    c:eval([[local fiber=require('fiber');local s=box.space[...];_G.batch_stop=false;_G.batch_writes=0
        _G.batch_writer=fiber.new(function()
            while not _G.batch_stop do
                local n=_G.batch_writes+2;box.begin();s:replace{1,n,'a',1,box.NULL};s:replace{2,n,'b',1};box.commit()
                _G.batch_writes=n;fiber.sleep(0)
            end
        end);_G.batch_writer:set_joinable(true)]], {sid})
    local n, coherent, before_reads = 500, true, census()
    for _ = 1, n do
        local r = native_read(args({{1}, {2}, {1}}, sid, box.NULL))
        coherent = coherent and r[1] == 'ok' and #r[2] == 3 and r[2][1][1] == 1 and r[2][2][1] == 2
            and r[2][3][1] == 1 and r[2][1][2] == r[2][2][2] and r[2][1][2] == r[2][3][2]
    end
    local after_reads = census()
    local writes = c:eval('_G.batch_stop=true;local ok,err=_G.batch_writer:join();assert(ok,tostring(err));return _G.batch_writes')
    check('500 actually served batches retain one atomic world, input order and duplicates',
        coherent and writes > 2 and before_reads.hooks == 0 and after_reads.hooks == 0
        and after_reads.served == before_reads.served + n and after_reads.to_lua == before_reads.to_lua,
        'writes=' .. writes .. ' served=' .. (after_reads.served - before_reads.served))
    check('native lock-copy checks remain exact', c:call('tarn_info').stats.lock_copy_errors == 0)
    c:eval("box.func['tarn_storage.ts_state']:call({12})")
    local compatible = pcall(native_read, {})
    c:eval("box.func['tarn_storage.ts_state']:call({11})")
    check('capability discovery does not bypass the existing module-layout guard', not compatible)
end, debug.traceback)
-- Restore only this fixture's state; no shared services or hooks are used.
pcall(c.eval, c, '_G.batch_stop=true;tarn.cfg.scan_page_rows=...;tarn.cfg.scan_page_bytes=select(2,...);tarn.ns_sync()', {saved.rows, saved.bytes})
if not yes then failed = failed + 1; print('  FAIL native batch exception: ' .. tostring(err)) end
c:close(); print(string.format('%d passed, %d failed', passed, failed)); os.exit(failed == 0 and 0 or 1)
