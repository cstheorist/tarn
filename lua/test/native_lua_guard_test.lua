-- Ordinary IPROTO calls exercise the protected public Lua C API bridge.
-- JIT and GC stay enabled; the bridge itself is never called through FFI.
local fio, fiber, ffi, net, json = require('fio'), require('fiber'), require('ffi'), require('net.box'), require('json')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
local native = os.getenv('TARN_NATIVE')
if native == '0' then print('native Lua guard skipped: native disabled'); os.exit(0) end
native = native or here .. '../../target/release/libtarn_storage.so'
assert(fio.stat(native), 'native Lua guard module is missing: ' .. native)
package.path = here .. '../?.lua;' .. package.path
package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath
ffi.cdef[[
struct lua_State;
struct lua_State *luaT_state(void);
int lua_gettop(struct lua_State *L);
]]
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, listen = '127.0.0.1:3897', memtx_use_mvcc_engine = true,
    memtx_memory = 128 * 1048576, vinyl_memory = 16 * 1048576, vinyl_cache = 0,
    wal_mode = 'fsync', txn_isolation = 'read-confirmed', log_level = 3}
local tarn = require('tarn').init{shard_id = 1, peers = {}, native = native,
    check_locks = true, check_yields = true}
local F, passed = tarn.fresh, 0
local function check(yes, label, detail)
    assert(yes, label .. ': ' .. json.encode(detail or {}))
    passed = passed + 1; print('  ok   ' .. label)
end
local function ready()
    local scratch, info = box.space._tarn_scratch, tarn_info()
    return scratch and scratch.index[0] and tarn.clock.read_check() == nil and tarn.lazy.quota.ready()
       and info.native and info.native.ready == 1
end
local deadline = fiber.clock() + 5
repeat
    if ready() then break end
    fiber.sleep(.01)
until fiber.clock() >= deadline
assert(ready(), 'native clock startup unavailable')
local c = net.connect('tarn:tarn@127.0.0.1:3897', {wait_connected = 5})
assert(c:is_connected())
local idle, stream, committed = c:new_stream(), nil, nil
local saved = {controls = F.native_controls, space = F.native_space, mark = F.native_mark, ok = F.native_ok,
               move_safe = F.move_safe}
local hooks = {commit = box.on_commit, rollback = box.on_rollback}
local sid = 96601
local args = {sid, 0, {{1}}, 1, true, false, box.NULL, 1}
local first = {sid, 0, {1}, 0, 1, box.NULL, true, false, box.NULL, true, box.NULL, 1}
local next_read = table.deepcopy(first); next_read[10] = false
local function native_read(via, a) return via:call('tarn_storage.ts_select', a, {timeout = 5}) end
local function census(via)
    return via:eval([[local ffi=require('ffi');local sid=...
        local entries=0;for _ in pairs(tarn.fresh.streams) do entries=entries+1 end
        return {top=ffi.C.lua_gettop(ffi.C.luaT_state()),txn=box.is_in_txn(),
            id=tostring(ffi.cast('uint64_t',ffi.C.box_txn_id())),certificates=tarn.fresh.count,entries=entries,
            row=box.space[sid]:get{1}:totable(),rows=box.space[sid]:count(),
            parts=box.space._tarn_part:count(),decisions=box.space._tarn_txn:count(),
            locks=box.space._tarn_locks:count(),native=tarn_info().native}]], {sid}, {timeout = 5})
end
local function unchanged(a, b)
    return a.top == b.top and a.txn == b.txn and a.id == b.id
       and a.certificates == b.certificates and a.entries == b.entries
       and a.rows == b.rows and json.encode(a.row) == json.encode(b.row)
       and a.parts == b.parts and a.decisions == b.decisions and a.locks == b.locks
end
local function served(label, via, a, value)
    local before = census(via)
    local r = native_read(via, a)
    local after = census(via)
    check(r[1] == 'ok' and #r[2] == 1 and r[2][1][1] == 1 and r[2][1][2] == value
          and after.native.served == before.native.served + 1
          and after.native.to_lua == before.native.to_lua, label, r)
end
local ok, err = xpcall(function()
    assert(c:call('tarn_bucket_init', {16, {{1, 16}}})[1] == 'ok')
    assert(c:call('tarn_create_table', {{id = sid, name = 'test.native_lua_guard', engine = 'memtx',
        ver = 0, json = '{}', lazy = {}, format = {{'id', 'unsigned'}, {'v', 'integer'}, {'bucket_id', 'unsigned'}},
        indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
                   {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}}}}})[1] == 'ok')
    c:eval('box.space[...]:replace{1,10,1}', {sid})
    assert(c:call('tarn_select_keys', args)[1] == 'ok') -- ordinary cache refresh
    local ns = ffi.cast('struct tarn_ns *', box.func['tarn_storage.ts_state']:call())
    local zero = true; for i = 0, 3 do zero = zero and ns.fresh_guard_reserved[i] == 0 end
    check(jit.status() and ns.layout == 14 and ffi.sizeof('struct tarn_ns') == 392
          and ffi.offsetof('struct tarn_ns', 'fresh_guard_reserved') == 360 and zero and ns.hooks == 0,
          'actual ABI14 uses a reserved tail with normal JIT enabled')
    served('healthy native RPC serves the exact tuple', idle, args, 10)

    local cases = {
        {name = 'missing'}, {name = 'nonfunction', value = {}},
        {name = 'wrong numeric', value = function() return 3 end},
        {name = 'nonnumeric', value = function() return '1' end},
        {name = 'erroring', value = function() error('ordinary guard refusal') end},
    }
    for _, case in ipairs(cases) do
        F.native_controls = case.value
        local before = census(idle)
        local r = native_read(idle, args)
        local after = census(idle)
        check(#r == 1 and r[1] == 'lua' and unchanged(before, after) and not after.txn
              and after.native.served == before.native.served
              and after.native.to_lua == before.native.to_lua + 1,
              case.name .. ' guard hands back without effects or stack changes', r)
    end
    F.native_controls = function() error('ordinary repeated guard refusal') end
    local before, replies = census(idle), true
    for _ = 1, 200 do
        local r = native_read(idle, args); replies = replies and #r == 1 and r[1] == 'lua'
    end
    local after = census(idle)
    check(replies and unchanged(before, after) and not after.txn and after.certificates == 0
          and after.native.served == before.native.served and after.native.to_lua == before.native.to_lua + 200,
          '200 ordinary C API errors leave no transaction, certificate or stack leak')
    F.native_controls = saved.controls
    served('restoring the guard restores actual native service', idle, args, 10)

    for _, case in ipairs({
        {name = 'before the first hook', hook = 'commit', after = false},
        {name = 'after the first hook', hook = 'commit', after = true},
        {name = 'before the second hook', hook = 'rollback', after = false},
        {name = 'after the second hook', hook = 'rollback', after = true},
    }) do
        local injected, register = 0, hooks[case.hook]
        local function registration(fn, ...)
            -- Only this fixture's freshness hook is refused. Background
            -- workers keep their ordinary registration behavior.
            local source = type(fn) == 'function' and debug.getinfo(fn, 'S').source or ''
            if source:match('tarn_fresh%.lua$') then
                injected = injected + 1
                if case.after then register(fn, ...) end
                error('ordinary hook registration refusal')
            end
            return register(fn, ...)
        end
        if case.hook == 'commit' then box.on_commit = registration else box.on_rollback = registration end
        before = census(idle)
        local r = native_read(idle, first)
        box.on_commit, box.on_rollback = hooks.commit, hooks.rollback
        after = census(idle)
        check(injected == 1 and #r == 1 and r[1] == 'lua' and unchanged(before, after) and not after.txn
              and after.certificates == 0 and after.entries == 0
              and after.native.served == before.native.served and after.native.to_lua == before.native.to_lua + 1,
              'registration error ' .. case.name .. ' leaves no token, transaction or effects', r)
    end
    served('restoring hook registration restores actual native service', idle, args, 10)

    local marked
    F.native_mark = function(id) marked = tostring(ffi.cast('uint64_t', id)); return saved.mark(id) end
    stream = c:new_stream()
    served('native first read creates a certified stream', stream, first, 10)
    local original = census(stream)
    check(original.txn and original.id == marked and original.certificates == 1 and original.entries == 1,
          'the C bridge passes the exact current transaction identity to mark', original.id)
    F.native_mark, F.native_ok = saved.mark, nil
    before, replies = census(stream), true
    for _ = 1, 200 do
        local r = native_read(stream, next_read); replies = replies and #r == 1 and r[1] == 'lua'
    end
    after = census(stream)
    check(replies and unchanged(before, after) and after.txn and after.id == original.id
          and after.native.served == before.native.served and after.native.to_lua == before.native.to_lua + 200,
          'missing stream guard preserves the exact marked transaction across 200 handbacks')
    F.native_ok = saved.ok
    F.move_safe = function() error('injected movement guard refusal') end
    local guarded = stream:eval([[local previous=box.error.new{code=1213,reason='preserved error'}
        box.error.set(previous)
        local success,value=pcall(tarn.fresh.native_ok,box.txn_id())
        local preserved=box.error.last()==previous
        box.error.clear()
        return {success,value==0,preserved}]])
    check(guarded[1] and guarded[2] and guarded[3],
          'movement guard errors return refusal and preserve the existing error', guarded)
    before = census(stream)
    local movement_reply = native_read(stream, next_read)
    after = census(stream)
    check(movement_reply[1]=='lua' and unchanged(before,after),
          'movement guard error hands back without losing the marked snapshot', movement_reply)
    F.move_safe = saved.move_safe
    c:eval('box.space[...]:replace{1,11,1}', {sid})
    local validation = stream:call('tarn_validate', {'native-lua-guard-readset', {{sid, 0, {1}, 1}}, {}, false})
    after = census(stream)
    check(validation[1] == 'conflict' and validation[2] == 1213 and not after.txn
          and after.certificates == 0 and after.entries == 0,
          'handback retains the original MVCC read set and rollback clears its certificate', validation)

    committed = c:new_stream()
    served('restored guards serve a fresh native stream', committed, first, 11)
    before = census(committed)
    validation = committed:call('tarn_storage.ts_validate',
        {'native-lua-guard-commit', {{sid, 0, {1}, 1}}, {}, false, box.NULL, box.NULL})
    after = census(committed)
    check(validation[1] == 'ok' and not after.txn and after.certificates == 0 and after.entries == 0
          and after.native.validations == before.native.validations + 1,
          'actual native validation commits and reclaims the restored stream certificate', validation)
    check(tarn_info().stats.lock_copy_errors == 0, 'native lock mirrors remain exact')
end, debug.traceback)
F.native_controls, F.native_space, F.native_mark, F.native_ok = saved.controls, saved.space, saved.mark, saved.ok
F.move_safe = saved.move_safe
box.on_commit, box.on_rollback = hooks.commit, hooks.rollback
for _, s in pairs({idle, stream, committed}) do
    pcall(s.eval, s, 'if box.is_in_txn() then box.rollback() end', {}, {timeout = 5})
end
c:close()
if not ok then print('  FAIL ' .. tostring(err)) end
print(string.format('%d passed, %d failed', passed, ok and 0 or 1)); os.exit(ok and 0 or 1)
