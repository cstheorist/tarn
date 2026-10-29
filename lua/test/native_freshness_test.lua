-- A blocked synchronous entry guarantees that the native pre-data barrier
-- yields. Other large IPROTO CALLs then recycle argument ports before it
-- resumes. Results must retain exact keys/payloads and the stream read set.
local net, fiber, fio, ffi, json = require('net.box'), require('fiber'), require('fio'), require('ffi'), require('json')
io.stdout:setvbuf('no')
local work, base = assert(os.getenv('NATIVE_FRESHNESS_WORK')), tonumber(assert(os.getenv('NATIVE_FRESHNESS_BASE')))
local conns, passed = {}, 0
for i = 1, 3 do
    conns[i] = net.connect('127.0.0.1:' .. (base + i), {user = 'tarn', password = 'tarn',
        wait_connected = false, connect_timeout = .3, reconnect_after = .05})
end
local function event(label, value)
    local f = assert(io.open(work .. '/events.jsonl', 'a'))
    f:write(json.encode{event = label, value = value}, '\n'); f:close()
end
local function check(label, yes, value)
    event(label, value or {}); assert(yes, label .. ': ' .. json.encode(value)); passed = passed + 1
    print('  ok   ' .. label)
end
local function ev(i, code, args) return conns[i]:eval(code, args or {}, {timeout = 10}) end
local function await(label, fn, seconds)
    local deadline, last = fiber.clock() + (seconds or 30)
    repeat
        local ok, value = pcall(fn)
        if ok and value then return value end
        last = value; fiber.sleep(.01)
    until fiber.clock() >= deadline
    error(label .. ' timed out: ' .. tostring(last))
end
local function state(i)
    if not conns[i]:is_connected() then return false end
    return ev(i, [[if not rawget(_G,'tarn') then return false end
        local n=tarn_info().native
        return {pid=box.info.pid,workdir=require('fio').cwd(),ro=box.info.ro,term=box.info.election.term,
            queue=box.info.synchro.queue,native=n,clock=tarn.clock.status(),sync=tarn.cfg.sync,
            version=box.info.version,wal=box.cfg.wal_mode} ]])
end
for i = 1, 3 do await('replica startup', function() return state(i) end) end
local leader = await('healthy native leader', function()
    local found
    for i = 1, 3 do
        local s = state(i)
        if s and not s.ro and s.queue.len == 0 and s.native and s.native.ready == 1 and s.clock.read_ready then
            assert(not found, 'multiple writable leaders'); found = i
        end
    end
    return found
end)
local follower = leader % 3 + 1
local c = conns[leader]
local function signal(i, sig)
    local f = assert(io.open(work .. '/s' .. i .. '.pid')); local pid = tonumber(f:read('*a')); f:close()
    f = assert(io.open(work .. '/s' .. i .. '.process-identity')); local recorded = f:read('*a'):match('^%s*(.-)%s*$'); f:close()
    f = assert(io.open('/proc/' .. pid .. '/stat')); local stat = f:read('*a'); f:close()
    local fields = {}; for value in stat:gmatch('%S+') do fields[#fields + 1] = value end
    local s = sig == 19 and assert(state(i)) or nil
    assert(fields[4] .. ':' .. fields[22] == recorded and (not s or s.pid == pid and s.workdir == work .. '/s' .. i),
           'owned process identity changed')
    pcall(ffi.cdef, 'int kill(int,int); int getpid(void);')
    assert(pid ~= ffi.C.getpid()); assert(ffi.C.kill(pid, sig) == 0)
    event('owned process signal', {index = i, pid = pid, signal = sig, identity = recorded})
end
local sid, token = 96500, string.rep('key-input-', 4096)
local function ok(r) assert(type(r) == 'table' and r[1] == 'ok', json.encode(r)); return r end
ok(c:call('tarn_bucket_init', {16, {{1, 16}}}))
ok(c:call('tarn_create_table', {{id=sid,name='test.native_fresh',engine='memtx',ver=0,json='{}',lazy={},
    format={{'id','unsigned'},{'token','string'},{'v','integer'},{'bucket_id','unsigned'}},
    indexes={{name='primary',unique=true,parts={{1,'unsigned'},{2,'string'}}},
             {name='bucket_id',unique=false,parts={{4,'unsigned'}}}}}}))
ev(leader, [[local sid,token=...;box.space[sid]:replace{1,token,0,1}
    tarn.cfg.scan_page_bytes=4*1024*1024;tarn.cfg.scan_page_rows=1024;tarn.ns_sync()
    native_fresh_status_events={sequence=0,events={}}
    native_fresh_status_watch=box.watch('box.status',function(value)
        local s=native_fresh_status_events
        s.sequence=s.sequence+1
        if #s.events==8 then table.remove(s.events,1) end
        s.events[#s.events+1]={sequence=s.sequence,value=value,ro=box.info.ro,
            term=box.info.election.term,mono=require('clock').monotonic()}
    end)
    function native_fresh_cache()
        local ffi=require('ffi');local n=ffi.cast('struct tarn_ns*',box.func['tarn_storage.ts_state']:call())
        return {ready=tonumber(n.ready),bk_stale=tonumber(n.bk_stale),lk_stale=tonumber(n.lk_stale),
            fence_stale=tonumber(n.fence_stale),hooks=tonumber(n.hooks),count=tonumber(n.count),
            bucket1=n.count>0 and tonumber(n.serves[1]) or 0,
            ro=box.info.ro,term=box.info.election.term,queue=box.info.synchro.queue.len,
            status=native_fresh_status_events}
    end
    function native_fresh_recycle(payload) return #payload end
    box.schema.func.create('native_fresh_recycle',{if_not_exists=true})
    box.schema.user.grant('tarn','execute','function','native_fresh_recycle',{if_not_exists=true})]], {sid, token})
for i = 1, 3 do
    await('replicated source tuple', function() return ev(i, 'return box.space[...] and box.space[...]:get{1,select(2,...)}~=nil', {sid,token}) end)
    ev(i, 'box.cfg{replication_synchro_quorum=3,replication_synchro_timeout=10}')
end
local read_args = {sid,0,{1,token},0,1,box.NULL,true,true,box.NULL,false,box.NULL,1}
ok(c:call('tarn_select', read_args))
local n = ev(leader, [[local ffi=require('ffi');local n=ffi.cast('struct tarn_ns*',box.func['tarn_storage.ts_state']:call())
    local zero=true;for i=0,3 do zero=zero and n.fresh_guard_reserved[i]==0 end
    return {layout=tonumber(n.layout),size=ffi.sizeof('struct tarn_ns'),clock=tonumber(n.fresh_clock_ok),hooks=tonumber(n.hooks),
        byte_reserved_offset=ffi.offsetof('struct tarn_ns','fresh_reserved'),
        guard_reserved_offset=ffi.offsetof('struct tarn_ns','fresh_guard_reserved'),
        guard_reserved_size=ffi.sizeof(n.fresh_guard_reserved),guard_reserved_zero=zero}]])
check('actual ABI14 and production native clock mirror are ready', n.layout == 14 and n.size == 392 and n.clock == 1 and n.hooks == 0
    and n.byte_reserved_offset == 314 and n.guard_reserved_offset == 360 and n.guard_reserved_size == 32 and n.guard_reserved_zero, n)
check('RF3 fsync synchronous majority-compatible configuration', state(leader).sync and state(leader).wal == 'fsync', state(leader))
local round = 0
local function blocked(proc, args, via)
    round = round + 1
    ok(c:call('tarn_select', read_args)) -- refresh ordinary caches before blocking
    local after_read = ev(leader, 'return native_fresh_cache()')
    -- A read need not leave the native WRITE bucket cache ready. In
    -- particular, a box.status notification conservatively invalidates it
    -- even when the term/RO role did not change. Refresh through the public
    -- map reader while every quorum member is still running, never after
    -- the synchronous entry below is pending.
    ok(c:call('tarn_buckets'))
    local prepared = ev(leader, 'return native_fresh_cache()')
    check(proc .. ' native write/read caches are ready before replica pause',
        prepared.ready==1 and prepared.hooks==0 and prepared.bk_stale==0 and prepared.lk_stale==0
        and prepared.fence_stale==0 and prepared.count==16 and prepared.bucket1==2
        and not prepared.ro and prepared.queue==0, {after_read=after_read,prepared=prepared})
    signal(follower, 19)
    local yes, result = xpcall(function()
        local writer = c:eval('local sid,key,v=...;box.space[sid]:replace{1,key,v,1};return v', {sid,token,round}, {is_async=true})
        await('pending synchronous WAL entry', function() return ev(leader,'return box.info.synchro.queue.len') > 0 end, 2)
        local pending_cache = ev(leader, 'return native_fresh_cache()') -- diagnostics only; no refresh/barrier
        local before = c:call('tarn_info').native
        local future = (via or c):call(proc, args, {is_async=true})
        fiber.sleep(.03)
        check(proc .. ' waits for the pending entry before data', not future:is_ready(), {round=round,queue=state(leader).queue})
        local junk = string.rep('recycled-port-' .. round, 32768)
        for _ = 1, 24 do assert(c:call('native_fresh_recycle', {junk}) == #junk) end
        check(proc .. ' remains blocked while other large CALL ports are recycled', not future:is_ready(), {bytes=#junk*24})
        signal(follower, 18)
        local wr, we = writer:wait_result(5); assert(wr, tostring(we))
        local r, err = future:wait_result(5); assert(r, tostring(err))
        local cache_after = ev(leader, 'return native_fresh_cache()')
        event('native cache across yielded call', {proc=proc,round=round,prepared=prepared,
              pending=pending_cache,after=cache_after})
        return {reply=r, before=before, after=c:call('tarn_info').native,
                cache={prepared=prepared,pending=pending_cache,after=cache_after}}
    end, debug.traceback)
    if not yes then pcall(signal, follower, 18); error(result) end
    return result
end
local r = blocked('tarn_storage.ts_select', read_args)
check('twelve-argument native read retains its key after first yield', r.reply[1][1] == 'ok'
    and r.reply[1][2][1][2] == token and r.reply[1][2][1][3] == round and r.after.served == r.before.served+1, r.after)
local keys = {}; for _ = 1, 16 do keys[#keys+1] = {1,token} end
r = blocked('tarn_storage.ts_select', {sid,0,keys,1,true,false,box.NULL,1})
local exact = r.reply[1][1] == 'ok' and #r.reply[1][2] == 16
for _, row in ipairs(r.reply[1][2] or {}) do exact = exact and row[1] == 1 and row[2] == token and row[3] == round end
check('native key batch retains every duplicate key and exact tuple after first yield', exact and r.after.served == r.before.served+1, r.after)
local stream = c:new_stream(); local first = table.deepcopy(read_args); first[8], first[10] = false, true
r = blocked('tarn_storage.ts_select', first, stream)
check('first native stream read retains its owned transaction and key', r.reply[1][1] == 'ok' and r.reply[1][2][1][2] == token
    and r.after.served == r.before.served+1, r.after)
local stream_id = stream:eval("return tostring(require('ffi').C.box_txn_id())")
local validation_before = c:call('tarn_info').native
local handback = stream:call('tarn_storage.ts_validate', {'native-final-prefix',{{sid,0,{1,token},1}},{}})
local validation_after = c:call('tarn_info').native
check('replicated native validation hands back without consuming the stream or its read set', handback[1] == 'lua'
    and stream:eval("return tostring(require('ffi').C.box_txn_id())") == stream_id
    and validation_after.validations == validation_before.validations
    and validation_after.validations_to_lua == validation_before.validations_to_lua + 1, handback)
ev(leader, 'local sid,key=...;box.space[sid]:replace{1,key,100,1}', {sid,token})
local v = stream:call('tarn_validate', {'native-input-readset',{{sid,0,{1,token},1}},{}})
check('a later overwrite conflicts with the original yielded native stream read set', v[1] == 'conflict' and v[2] == 1213, v)
local payload = string.rep('write-input-',4096)
r = blocked('tarn_storage.ts_autocommit', {{{'replace',sid,{2,payload,200,1}}},{fence={sid,0},wb={1}}})
local row = ev(leader, 'return box.space[...]:get{2,select(2,...)}', {sid,payload})
check('native autocommit retains its mutation payload after the pre-data yield', r.reply[1][1] == 'ok' and row[2] == payload
    and row[3] == 200 and r.after.writes == r.before.writes+1, {native=r.after,cache=r.cache})
r = blocked('tarn_storage.ts_commit_local', {'native-input-local',{{'replace',sid,{3,payload,300,1}}},{},{},box.NULL,{sid,0},{1},box.NULL})
row = ev(leader, 'return box.space[...]:get{3,select(2,...)}', {sid,payload})
check('native write-only local commit retains its payload after the pre-data yield', r.reply[1][1] == 'ok' and row[2] == payload
    and row[3] == 300 and r.after.commits == r.before.commits+1, {native=r.after,cache=r.cache})
local no_op = c:new_stream(); ok(no_op:call('tarn_storage.ts_select', first))
local no_op_id = no_op:eval("return tostring(require('ffi').C.box_txn_id())")
local no_op_args = {'native-final-no-op',{{'delete',sid,{999,token}}},{{sid,0,{1,token},1}},{},box.NULL,{sid,0},{1},box.NULL}
local commit_before = c:call('tarn_info').native
handback = no_op:call('tarn_storage.ts_commit_local', no_op_args)
local commit_after = c:call('tarn_info').native
check('replicated native stream commit hands back before a possible zero-WAL result', handback[1] == 'lua'
    and no_op:eval("return tostring(require('ffi').C.box_txn_id())") == no_op_id
    and commit_after.commits == commit_before.commits
    and commit_after.commits_to_lua == commit_before.commits_to_lua + 1, handback)
local completed = no_op:call('tarn_commit_local', no_op_args)
check('Lua final-prefix fence completes an unchanged zero-row stream commit', completed[1] == 'ok' and completed[2] == 0, completed)
local ambient = c:new_stream(); ambient:begin()
local old_id = ambient:eval([[local ffi=require('ffi');box.space[...]:get{1,select(2,...)}
    return tostring(ffi.C.box_txn_id())]], {sid,token})
local untouched = ambient:call('tarn_storage.ts_select', read_args)
check('native refuses an unmarked ambient view without replacing its transaction', untouched[1] == 'lua'
    and ambient:eval("return tostring(require('ffi').C.box_txn_id())") == old_id, untouched)
ev(leader, 'local sid,key=...;box.space[sid]:replace{1,key,101,1}', {sid,token})
local committed = pcall(function() ambient.space[sid]:replace{4,token,400,1}; ambient:commit() end)
check('unmarked native handback retains the original overwritten MVCC read set', not committed)
pcall(ambient.rollback, ambient)

-- A real barrier failure is an error, never a successful handback that the
-- gateway could immediately retry through Lua. Only the owned empty TX ends.
ok(c:call('tarn_select', read_args))
ev(leader, 'tarn.cfg.read_freshness_timeout=.1;tarn.ns_sync()')
signal(follower, 19)
local timeout_ok, timeout_err = xpcall(function()
    local writer = c:eval('local sid,key=...;box.space[sid]:replace{1,key,102,1};return true', {sid,token}, {is_async=true})
    await('timeout fixture pending WAL', function() return ev(leader,'return box.info.synchro.queue.len') > 0 end, 2)
    local before = c:call('tarn_info').native
    local future = c:call('tarn_storage.ts_select', read_args, {is_async=true})
    local answer, err = future:wait_result(1)
    local after = c:call('tarn_info').native
    check('failed native quorum barrier propagates an error without a Lua retry envelope', answer == nil and err ~= nil
        and after.served == before.served and after.to_lua == before.to_lua, {error=tostring(err),before=before,after=after})
    signal(follower, 18)
    local wr, we = writer:wait_result(5); assert(wr, tostring(we))
end, debug.traceback)
if not timeout_ok then pcall(signal, follower, 18); error(timeout_err) end
ev(leader, 'tarn.cfg.read_freshness_timeout=5;tarn.ns_sync()')
ok(c:call('tarn_select', read_args))
local policy_guard = ev(leader, [[
    require('jit').off()
    local ffi=require('ffi');local n=ffi.cast('struct tarn_ns*',box.func['tarn_storage.ts_state']:call())
    local old=tarn.cfg.max_txn_age;local cached=tonumber(n.fresh_clock_ok)
    tarn.cfg.max_txn_age=old+1
    local csw=require('fiber').self():csw()
    local r=box.func['tarn_storage.ts_select']:call(...)
    local no_yield=require('fiber').self():csw()==csw
    tarn.cfg.max_txn_age=old
    return {cached=cached,reply=r,no_yield=no_yield,policy=tarn.clock.policy_ok()}
]], {read_args})
check('native rechecks immutable clock policy even while its cached observation is healthy', policy_guard.cached == 1
    and policy_guard.reply[1] == 'lua' and policy_guard.no_yield and policy_guard.policy, policy_guard)
ev(leader, 'box.space[...]:alter{is_sync=false}', {sid})
local asynchronous = c:call('tarn_storage.ts_select', read_args)
local lua_allowed = pcall(c.call, c, 'tarn_select', read_args)
check('actual asynchronous user spaces fail closed in native and Lua strong reads', asynchronous[1] == 'lua' and not lua_allowed, asynchronous)
ev(leader, 'box.space[...]:alter{is_sync=true}', {sid})
check('native lock-copy checks remain exact', ev(leader,'return tarn.stats.lock_copy_errors') == 0)
for _, conn in ipairs(conns) do conn:close() end
print(passed .. ' passed, 0 failed')
