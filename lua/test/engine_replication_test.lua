-- RF3 engine migration: only signals processes created by the companion
-- shell harness, after checking their PID, data directory and start time.
local net, fiber, fio, ffi, json = require('net.box'), require('fiber'), require('fio'), require('ffi'), require('json')
io.stdout:setvbuf('no')
local work = assert(os.getenv('ENGINE_RF3_WORK'))
local base = tonumber(assert(os.getenv('ENGINE_RF3_BASE')))
local engine, mode, stage = assert(os.getenv('ENGINE_RF3_ENGINE')), assert(os.getenv('ENGINE_RF3_EXPECT_MODE')), assert(arg[1])
local target_engine = engine == 'memtx' and 'vinyl' or 'memtx'
local old_id, new_id, name, count = 90800, 90801, 'test.engine_rf3', 300
local owner, conns, passed = 'engine-rf3-' .. engine, {}, 0
for i = 1, 3 do
    conns[i] = net.connect('127.0.0.1:' .. (base + i), {user = 'tarn', password = 'tarn',
        wait_connected = false, connect_timeout = .3, reconnect_after = .05})
end
local function event(label, data)
    local f = assert(io.open(work .. '/events.jsonl', 'a'))
    f:write(json.encode{stage = stage, engine = engine, mode = mode, event = label, data = data}, '\n'); f:close()
end
local function check(label, yes, data)
    event(label, data or {}); assert(yes, label .. ': ' .. json.encode(data)); passed = passed + 1
    print('  ok   ' .. engine .. ': ' .. label)
end
local function ev(i, script, args, timeout) return conns[i]:eval(script, args or {}, {timeout = timeout or 12}) end
local function call(i, proc, ...) return conns[i]:call(proc, {...}, {timeout = 12}) end
local function ok(r) assert(type(r) == 'table' and r[1] == 'ok', json.encode(r)); return r end
local function await(label, fn, seconds)
    local deadline, last = fiber.clock() + (seconds or 30)
    repeat
        local good, value = pcall(fn)
        if good and value then return value end
        last = value; fiber.sleep(.025)
    until fiber.clock() >= deadline
    error(label .. ' timed out: ' .. tostring(last))
end
local probe = [[
local old_id,new_id,name,full=...
if rawget(_G,'tarn')==nil then return false end
for _,key in ipairs{'_tarn_meta','_tarn_buckets','_tarn_catalog','_tarn_part','_tarn_locks','_tarn_txn'} do
    if box.space[key]==nil or box.space[key].index[0]==nil then return false end
end
local r={pid=box.info.pid,uuid=tostring(box.info.uuid),id=box.info.id,term=box.info.election.term,
    ro=box.info.ro,workdir=require('fio').cwd(),wal=box.cfg.wal_mode,sync=tarn.cfg.sync,
    queue=box.info.synchro.queue,version=box.info.version,check_locks=tarn.cfg.check_locks,
    lock_copy_errors=tarn.stats.lock_copy_errors,clock=tarn.clock.status()}
local e,t=tarn.engine,tarn.engine.tracks[old_id]
r.capture={stale=e.stale,epoch=e.epoch,capture_epoch=e.capture_epoch,
    track=t and {valid=t.valid,epoch=t.epoch,token=t.token,dirty_keys=#t.list,bytes=t.bytes} or false,
    tickets={ready=e.tickets.ready(),epoch=e.tickets.epoch,outstanding=e.tickets.outstanding,
             building=e.tickets.building}}
local b=rawget(_G,'engine_rf3_build_state')
if b then
    r.builder=table.deepcopy(b)
    local f=rawget(_G,'engine_rf3_build_fiber')
    if f then r.builder.fiber={id=f:id(),status=f:status(),csw=f:csw()} end
else r.builder=false end
if not r.ro then
    r.native=tarn_info().native; r.quota_ready=tarn.lazy.quota.ready()
    if r.native and r.native.ready==1 then
        r.layout=tonumber(require('ffi').cast('struct tarn_ns *',box.func['tarn_storage.ts_state']:call()).layout)
        r.state_size=require('ffi').sizeof('struct tarn_ns')
        r.fallback_offset=require('ffi').offsetof('struct tarn_ns','fallbacks')
        local ns=require('ffi').cast('struct tarn_ns *',box.func['tarn_storage.ts_state']:call())
        r.native_cache={capture_stale=ns.capture_stale~=0,bk_stale=ns.bk_stale~=0,hooks=tonumber(ns.hooks)}
    end
end
box.begin{txn_isolation='read-confirmed'}
local yes,err=pcall(function()
    local old,target=box.space[old_id],box.space[new_id]
    r.source_rows=old and old.index[0]:count() or 0
    r.source_zero=old and old:get{0}~=nil or false
    r.target_rows=target and target.index[0]:count() or 0
    r.target_zero=target and target:get{0}~=nil or false
    r.target_engine=target and target.engine or false
    local m=box.space._tarn_meta:get{'engine:'..old_id}; r.receipt=m and m.value or false
    local j=box.space._tarn_meta:get{'job:'..name};r.job=j and j.value or false
    local cat=box.space._tarn_catalog:get{name};r.catalog_id=cat and cat.def.id or 0
    r.held=box.space._tarn_meta:get{'hold:'..new_id}~=nil
    if full then
        local rows={}
        if old then for _,tuple in old.index[0]:pairs() do rows[#rows+1]=tuple:totable() end end
        r.source_sha256=require('digest').sha256_hex(require('msgpack').encode(rows))
    end
end)
box.rollback();if not yes then error(err) end
return r
]]
local function state(i, timeout, full)
    if not conns[i]:is_connected() then return false end
    return ev(i, probe, {old_id, new_id, name, full or false}, timeout)
end
local function copy_hook(i)
    local started = fiber.clock()
    local good, result = pcall(await, 'partial copy hook', function()
        return call(i, 'tarn_test_hook', 'engine_copy', 'reached')[2] == 1
    end)
    if good then return true end
    local wait_elapsed = fiber.clock() - started
    -- Do not re-send BUILD. Capture its one attempt and current durable/
    -- volatile progress before the controller stops these owned processes.
    -- Each diagnostic RPC is bounded separately; it cannot extend the
    -- original hook wait or the copy's own deadline.
    local nodes = {}
    for j = 1, 3 do
        local yes, s = pcall(state, j, 1, true)
        nodes[j] = {connected = conns[j]:is_connected(), response = yes and s or false,
            error = not yes and tostring(s) or (s == false and tostring(conns[j].error) or false),
            process_log = work .. '/s' .. j .. '.log'}
    end
    local data = {error = tostring(result), elapsed = wait_elapsed,
        diagnostic_elapsed = fiber.clock() - started - wait_elapsed, leader = i, nodes = nodes}
    event('partial copy hook timeout', data)
    io.stderr:write('ENGINE_RF3_COPY_DIAGNOSTIC ', json.encode(data), '\n')
    return false, result, data
end
local function same(a, b)
    if type(a) ~= type(b) then return false end
    if type(a) ~= 'table' then return a == b end
    for k, v in pairs(a) do if not same(v, b[k]) then return false end end
    for k in pairs(b) do if a[k] == nil then return false end end
    return true
end
local function mode_ok(s)
    return s.check_locks == true and s.lock_copy_errors == 0 and
        (mode == 'native' and s.native and s.native.ready == 1 and s.layout == 14 and s.state_size == 392
            and s.fallback_offset == 304
        or mode == 'lua' and s.native == false)
end
local function leader()
    local snapshots, first = {}, true
    local started = fiber.clock()
    local good, result = pcall(await, 'confirmed writable leader', function()
        local found, eligible = nil, 0
        for i = 1, 3 do
            local yes, s = pcall(state, i)
            snapshots[i] = {connected = conns[i]:is_connected(), response = yes and s or false,
                error = not yes and tostring(s) or (s == false and tostring(conns[i].error) or false)}
            if yes and s and not s.ro and s.quota_ready and s.clock.read_ready and s.queue.len == 0
               and not s.queue.busy and s.queue.owner == s.id and mode_ok(s) then
                eligible = eligible + 1; found = i
            end
        end
        if first then event('leader readiness first observation', snapshots); first = false end
        assert(eligible <= 1, 'multiple writable leaders')
        return found
    end)
    if not good then
        -- Capture every node's last response/error before the shell's
        -- finally cleanup. In particular a durable clock fault must stay
        -- closed; cache warming or a longer wait must not hide it.
        local data = {error = tostring(result), elapsed = fiber.clock() - started, nodes = snapshots}
        event('leader readiness timeout', data)
        io.stderr:write('ENGINE_RF3_LEADER_DIAGNOSTIC ', json.encode(data), '\n')
        error(result)
    end
    event('leader readiness accepted', {index = result, elapsed = fiber.clock() - started, nodes = snapshots})
    return result
end
local function args(i)
    return ev(i, "return assert(box.space._tarn_meta:get{'test:engine_rf3'}).value.a")
end
local function kill_owned(i, which)
    local s = assert(state(i))
    local f = assert(io.open(work .. '/s' .. i .. '.pid')); local pid = tonumber(f:read('*a')); f:close()
    f = assert(io.open(work .. '/s' .. i .. '.process-identity')); local recorded = f:read('*a'):match('^%s*(.-)%s*$'); f:close()
    f = assert(io.open('/proc/' .. pid .. '/stat')); local stat = f:read('*a'); f:close()
    local fields = {}; for value in stat:gmatch('%S+') do fields[#fields + 1] = value end
    assert(s.pid == pid and s.workdir == work .. '/s' .. i and fields[4] .. ':' .. fields[22] == recorded,
        'owned process identity changed')
    pcall(ffi.cdef, 'int kill(int,int); int getpid(void);')
    assert(pid ~= ffi.C.getpid())
    f = assert(fio.open(work .. '/killed-' .. which, {'O_WRONLY','O_CREAT','O_EXCL'}, 384))
    assert(f:write(tostring(i) .. '\n')); f:close()
    f = assert(io.open(work .. '/identity-' .. which .. '.json', 'w')); f:write(json.encode(s)); f:close()
    event('owned leader process loss', {index = i, identity = s})
    assert(ffi.C.kill(pid, 9) == 0)
end
local function main()
    if stage == 'setup' then
        for i = 1, 3 do
            await('replica schema', function() return state(i) end)
            ev(i, 'tarn.cfg.recovery_after=1000000;tarn.lazy.paused=true')
        end
        local l = leader()
        ok(call(l, 'tarn_bucket_init', 16, {{1, 16}}))
        ok(ev(l, [[
local old_id,new_id,name,engine,target,owner,count=...
local format={{'id','unsigned'},{'v','integer'},{'bucket_id','unsigned'}}
local indexes={{name='primary',unique=true,parts={{1,'unsigned'}}},
               {name='bucket_id',unique=false,parts={{3,'unsigned'}}}}
local def={id=old_id,name=name,engine=engine,ver=0,json='{}',format=format,indexes=indexes,lazy={}}
assert(tarn_create_table(def)[1]=='ok')
local writes={};for i=1,count do writes[#writes+1]={'insert',old_id,{i,i*10,1}} end
assert(tarn_autocommit(writes,{fence={old_id,0}})[1]=='ok')
local nd=table.deepcopy(def);nd.id,nd.engine,nd.ver=new_id,target,5
local a={ver=5,timeout=12,engine_change={old_id=old_id,old_ver=0,old_engine=engine,old_name=name},
    def=nd,uix={},uix_drop={},lazy_drop={}}
box.space._tarn_meta:replace{'test:engine_rf3',{a=a,owner=owner}}
return tarn_job_begin({owner=owner,kind='alter',space=name,names={name},ver=5,a=a},0)
]], {old_id, new_id, name, engine, target_engine, owner, count}))
        for i = 1, 3 do await('source replicated', function() local s = state(i); return s and s.source_rows == count end) end
        check('RF3 quorum2 fsync source and native mode', state(l).sync and state(l).wal == 'fsync' and mode_ok(state(l)))
    elseif stage == 'kill-first' or stage == 'copy-control' then
        local l = leader()
        local control = stage == 'copy-control' and assert(os.getenv('ENGINE_RF3_COPY_CONTROL')) or ''
        assert(control == '' or control == 'source-binding' or control == 'exception' or control == 'stall',
            'unknown private engine copy control')
        local before = control ~= '' and assert(state(l, 12, true)) or nil
        ev(l, [[
local name,control=...;local a=table.deepcopy(box.space._tarn_meta:get{'test:engine_rf3'}.value.a)
assert(rawget(_G,'engine_rf3_build_state')==nil,'engine BUILD was already attempted; refusing replay')
tarn_test_hook('engine_copy','hold')
local fiber=require('fiber')
local s={attempts=1,calls=0,done=false,started=fiber.clock(),control=control,
    synthetic=control=='exception' or control=='stall'}
_G.engine_rf3_build_state=s
local f=fiber.new(function()
    local yes,result=pcall(function()
        -- The binding control reaches the REAL production refusal before
        -- any shadow/cursor write. The other two controls exercise only
        -- exception/stall observation and are explicitly synthetic.
        if control=='source-binding' then a.engine_change.old_ver=a.engine_change.old_ver+1 end
        if control=='exception' then error('synthetic engine builder exception') end
        -- A host pause must not let the synthetic stall expire and start a
        -- real mutation after its diagnostic deadline. Only cancellation
        -- or the owning controller's process cleanup can end this branch.
        while control=='stall' do fiber.sleep(1) end
        s.calls=s.calls+1
        return tarn_alter_build(name,a,1)
    end)
    s.ok,s.finished=yes,fiber.clock()
    if yes then _G.engine_rf3_build=result;s.result=result
    else
        s.exception=tostring(result)
        if type(result)=='cdata' or type(result)=='table' then
            local valid,code=pcall(function() return result.code end)
            if valid then s.exception_code=code end
        end
    end
    s.done=true
end)
f:name('engine_rf3_build');f:set_joinable(true);_G.engine_rf3_build_fiber=f
]], {name, control})
        local reached, err, diagnostic = copy_hook(l)
        if control ~= '' then
            assert(not reached and diagnostic, 'control unexpectedly reached the copy hook')
            local after = assert(diagnostic.nodes[l].response)
            local b = assert(after.builder)
            check('single-attempt copy diagnostic retains the original failure',
                b.attempts == 1 and (control == 'source-binding' and b.calls == 1 and b.done and b.ok
                    and b.result[1] == 'error' and b.result[2] == 1412
                    and b.result[3]:find('Engine source definition changed', 1, true)
                or control == 'exception' and b.calls == 0 and b.done and b.ok == false
                    and b.exception:find('synthetic engine builder exception', 1, true)
                or control == 'stall' and b.calls == 0 and not b.done and b.fiber.status == 'suspended'), after)
            check('pre-copy control preserves exact source, job and empty shadow/receipt',
                before.source_rows == count and after.source_rows == count
                and before.source_sha256 == after.source_sha256 and same(before.job, after.job)
                and before.target_rows == 0 and after.target_rows == 0 and not before.receipt and not after.receipt
                and after.queue.len == 0 and not after.queue.busy and after.term == before.term and not after.ro, after)
            -- Only the synthetic sleeping worker needs cancellation. Never
            -- retry the mutation to repair a diagnostic or a failed build.
            ev(l, [[local f=assert(rawget(_G,'engine_rf3_build_fiber'))
                if not _G.engine_rf3_build_state.done then f:cancel() end
                f:join();tarn_test_hook('engine_copy','release')]])
            event('copy diagnostic control accepted', {control = control, synthetic = b.synthetic, diagnostic = diagnostic})
            return
        end
        if not reached then error(err) end
        ok(call(l, 'tarn_autocommit', {{'insert', old_id, {0, 777, 1}}}, {fence = {old_id, 0}}))
        for i = 1, 3 do await('partial copy and acknowledged dirty row replicated', function()
            local s = state(i)
            return s and s.receipt and s.receipt.copied == 128 and s.target_rows == 128
                and s.source_zero and not s.target_zero
        end) end
        check('copy is partial and an acknowledged behind-cursor row is not in the shadow', state(l).source_rows == count + 1)
        kill_owned(l, 'first')
    elseif stage == 'after-first' then
        local l = leader(); local a = args(l)
        local gen = ok(call(l, 'tarn_job_touch', owner .. '-resumed', owner))[2]; assert(gen == 2)
        ok(call(l, 'tarn_alter_build', name, a, gen)); ok(call(l, 'tarn_alter_prepare', name, a, gen))
        local s = state(l)
        check('promotion fully recopies lost volatile capture and persists PREPARED',
            s.receipt.phase == 'prepared' and s.target_zero and s.target_rows == count + 1 and s.held, s)
        ev(l, "box.space._tarn_meta:replace{'test:engine_rf3_token',...}", {s.receipt.capture})
    elseif stage == 'caught-up' then
        local f = assert(io.open(work .. '/killed-first')); local i = tonumber(f:read('*a')); f:close()
        f = assert(io.open(work .. '/identity-first.json')); local before = json.decode(f:read('*a')); f:close()
        for j = 1, 3 do await('all replicas have immutable PREPARED image', function()
            local s = state(j); return s and s.receipt and s.receipt.phase == 'prepared'
                and s.target_zero and s.target_rows == count + 1
        end) end
        check('restarted replica keeps UUID and receives prepared replacement', state(i).uuid == before.uuid and state(i).pid ~= before.pid)
    elseif stage == 'kill-second' then
        local l = leader(); local s = state(l)
        check('second leader has confirmed PREPARED image before process loss', s.receipt.phase == 'prepared' and s.target_zero)
        kill_owned(l, 'second')
    elseif stage == 'after-second' then
        local l = leader(); local a = args(l)
        local gen = ok(call(l, 'tarn_job_touch', owner .. '-final', owner .. '-resumed'))[2]; assert(gen == 3)
        local before = ev(l, "return box.space._tarn_meta:get{'test:engine_rf3_token'}.value")
        ok(call(l, 'tarn_alter_build', name, a, gen)); ok(call(l, 'tarn_alter_prepare', name, a, gen))
        check('second promotion never rebuilds a durable PREPARED replacement', state(l).receipt.capture == before)
        ok(call(l, 'tarn_job_phase', owner .. '-final', 'apply'))
        ok(call(l, 'tarn_alter_apply', name, a, gen)); ok(call(l, 'tarn_job_end', owner .. '-final'))
        ok(call(l, 'tarn_autocommit', {{'insert', new_id, {count + 1, 888, 1}}}, {fence = {new_id, 5}}))
        local s = state(l)
        check('promoted leader publishes, cleans and accepts a fresh write',
            s.catalog_id == new_id and s.target_engine == target_engine and s.source_rows == 0
            and s.target_rows == count + 2 and s.target_zero and not s.held and s.receipt.cleaned, s)
        local r = call(l, 'tarn_select', old_id, 0, {1}, 'EQ', 1, box.NULL, true, true)
        check('retired ID remains a schema-change error after both leader deaths', r[1] == 'error' and r[2] == 1412, r)
    else error('unknown engine replication stage ' .. stage) end
end
local good, err = pcall(main)
for _, c in ipairs(conns) do c:close() end
if not good then io.stderr:write('FAIL engine RF3 ', engine, ' ', stage, ': ', tostring(err), '\n'); os.exit(1) end
print(string.format('%d passed, 0 failed (%s %s)', passed, engine, stage))
os.exit(0)
