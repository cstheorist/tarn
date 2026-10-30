-- RF3 companion; only signals identity-checked processes owned by
-- lazy_replication.sh, which creates and cleans the entire fixture.
local net, fiber, json = require('net.box'), require('fiber'), require('json')
local fio, ffi, digest, mp = require('fio'), require('ffi'), require('digest'), require('msgpack')
local work, base, engine = assert(os.getenv('LAZY_RF3_WORK')), tonumber(assert(os.getenv('LAZY_RF3_BASE'))), assert(os.getenv('LAZY_RF3_ENGINE'))
local stage, pass = assert(arg[1]), 0
assert(engine == 'memtx' or engine == 'vinyl')
local expected_mode = assert(os.getenv('LAZY_RF3_EXPECT_MODE'))
assert(expected_mode == 'native' or expected_mode == 'lua')
local mode_label = expected_mode == 'native' and 'native ABI' or 'Lua'
local sid, gen, name, bucket = 64001, 64002, 'test.lazy_rf3', 1
local txid = 'lazy-rf3-' .. engine
local ids = {}
for i = 1, 1000 do
    if tonumber(digest.crc32(mp.encode(i))) % 16 + 1 == bucket then ids[#ids + 1] = i end
    if #ids == 3 then break end
end
assert(#ids == 3)
local c = {}
for i = 1, 3 do
    c[i] = net.connect('127.0.0.1:' .. (base + i), {user = 'tarn', password = 'tarn',
        wait_connected = false, connect_timeout = .3, reconnect_after = .05})
end
local function event(label, data)
    local f = assert(io.open(work .. '/events.jsonl', 'a'))
    f:write(json.encode{stage = stage, engine = engine, expected_mode = expected_mode, event = label, data = data}, '\n'); f:close()
end
local function check(label, yes, data)
    event(label, data or {})
    assert(yes, label .. ': ' .. json.encode(data))
    pass = pass + 1; print('  ok   ' .. engine .. ': ' .. label)
end
local function ev(i, script, args)
    return c[i]:eval(script, args or {}, {timeout = 8})
end
local function call(i, proc, ...)
    return c[i]:call(proc, {...}, {timeout = 8})
end
local function ok(r) assert(type(r) == 'table' and r[1] == 'ok', json.encode(r)); return r end
local function await(label, fn, seconds)
    local deadline, last = fiber.clock() + (seconds or 25)
    repeat
        local good, value = pcall(fn)
        if good and value then return value end
        last = value; fiber.sleep(.03)
    until fiber.clock() >= deadline
    error(label .. ' deadline: ' .. tostring(last))
end
local probe = [[
local sid,gen,bucket,ids,txid = ...
if rawget(_G,'tarn') == nil or box.space._tarn_lazy_outbox == nil then return false end
local x={pid=box.info.pid,uuid=tostring(box.info.uuid),id=box.info.id,term=box.info.election.term,
    ro=box.info.ro,workdir=fio and fio.cwd() or require('fio').cwd(),sync=tarn.cfg.sync,
    queue=box.info.synchro.queue,quota_ready=tarn.lazy.quota.ready(),capture_stale=tarn.lazy.stale,
    wal=box.cfg.wal_mode,version=box.info.version,configured_native=tarn.cfg.native~=false and tarn.cfg.native~=nil,
    want_layout=tarn.NS_LAYOUT}
if not x.ro then
    local n=tarn_info().native
    x.native=n
    if n and n.ready==1 then
        x.layout=tonumber(require('ffi').cast('struct tarn_ns *',box.func['tarn_storage.ts_state']:call()).layout)
    end
end
box.begin{txn_isolation='read-confirmed'}
local good,err=pcall(function()
    local sp,target=box.space[sid],box.space[gen]
    x.engine=sp and sp.engine or false; x.target_engine=target and target.engine or false
    x.rows={}; for j,id in ipairs(ids) do x.rows[j]=sp and sp:get{id}~=nil or false end
    local count=box.space._tarn_lazy_counter:get{bucket}; x.counter=count and tonumber(count.seq) or 0
    local slots=box.space._tarn_lazy_slots:get{bucket}; x.slots=slots and tonumber(slots.reserved) or 0
    x.depth,x.bytes,x.reserved,x.reserved_bytes=0,0,0,0
    for _,r in box.space._tarn_lazy_outbox:pairs() do x.depth=x.depth+1; x.bytes=x.bytes+r.bytes end
    for _,r in box.space._tarn_lazy_reservation:pairs() do x.reserved=x.reserved+r.entries; x.reserved_bytes=x.reserved_bytes+r.bytes end
    local p=box.space._tarn_part:get{txid}; x.part=p and p.state or false
    local old=box.space._tarn_meta:get{'test.lazy.rf3.old'}
    if old then
        local floor=box.space._tarn_lazy_floor:get{gen,old.value[5],bucket}; x.floor=floor and tonumber(floor.seq) or -1
    end
    x.live={}
    for _,key in ipairs{'old','prepared','new'} do x.live[key]=target and target.index.ordered:count{true,key} or 0 end
end)
box.rollback(); if not good then error(err) end
if x.quota_ready then x.quota=tarn.lazy.quota.snapshot() end
local source=tarn.lazy.sources[sid]; x.capture=source and source.ver==0 and #source.specs==1 and source.specs[1].gen==gen or false
return x
]]
local function state(i)
    if not c[i]:is_connected() then return false end
    return ev(i, probe, {sid, gen, bucket, ids, txid})
end
local function mode_ok(s)
    if expected_mode == 'native' then
        return s.configured_native and s.native and s.native.ready == 1 and s.layout == s.want_layout
    end
    return s.configured_native == false and s.native == false and s.layout == nil
end
local function leader()
    local seen = {}
    local success, result = pcall(await, 'writable ' .. expected_mode .. '/quota leader', function()
        local found
        for i = 1, 3 do
            local yes, s = pcall(state, i)
            seen[i] = yes and s or tostring(s)
            if yes and s and not s.ro and s.quota_ready
               and s.queue.len == 0 and not s.queue.busy and s.queue.owner == s.id
               and mode_ok(s) then
                assert(found == nil, 'multiple writable leaders'); found = i
            end
        end
        -- Paused shipping need not refresh an idle capture cache. The
        -- ordinary entry path refreshes it before admitting user writes.
        if found then
            local r = call(found, 'tarn_autocommit', {}, {})
            if r[1] == 'ok' then return found end
        end
    end)
    if not success then event('leader discovery rejected', seen); error(result) end
    return result
end
local function durable_image(s)
    return mp.encode({s.engine, s.target_engine, s.rows, s.counter, s.slots, s.depth, s.bytes,
        s.reserved, s.reserved_bytes, s.part, s.floor or box.NULL,
        s.live.old, s.live.prepared, s.live.new})
end
local function entry_ready(i, label)
    local before, last = assert(state(i))
    local success, result = pcall(await, 'ordinary entry readiness: ' .. label, function()
        -- A confirmed mutation does not pin volatile caches: a later
        -- box.status notification invalidates them again. Shipping is
        -- paused here, so use the ordinary no-write entry to refresh capture
        -- and let the separate quota worker establish its confirmed cut.
        local r = call(i, 'tarn_autocommit', {}, {})
        local s = state(i)
        last = {response = r, state = s}
        return r[1] == 'ok' and s and not s.ro and s.quota_ready and s.capture
           and not s.capture_stale and s.queue.len == 0 and not s.queue.busy
           and s.queue.owner == s.id and mode_ok(s) and s
    end)
    if not success then event('ordinary entry readiness rejected', last); error(result) end
    assert(durable_image(before) == durable_image(result), 'empty entry changed durable lazy state')
    event('ordinary entry refreshed caches without durable changes', {label = label, state = result})
    return result
end
local function release_status_notification(i)
    if os.getenv('LAZY_RF3_STATUS_CONTROL') ~= '1' then return end
    local notification = ev(i, [[return assert(rawget(_G,'lazy_rf3_status_control'),
        'status-control preload missing').release()]])
    assert(notification.callback_ok and notification.epoch_after > notification.epoch_before
           and notification.capture_stale and notification.ro_before == notification.ro_after,
           json.encode(notification))
    event('held initial box.status notification delivered', {index = i, notification = notification})
end
local function exact(i, label)
    local s = entry_ready(i, label)
    check(label, s.sync and s.wal == 'fsync' and s.engine == engine and s.target_engine == engine
          and s.quota_ready and s.capture and not s.capture_stale and s.queue.len == 0 and s.queue.owner == s.id
          and s.quota.entries == s.depth and s.quota.bytes == s.bytes
          and s.quota.reserved_entries == s.reserved and s.quota.reserved_bytes == s.reserved_bytes
          and mode_ok(s), s)
    return s
end
local function write(i, op, id, key)
    return ok(call(i, 'tarn_autocommit', {{op, sid, op == 'delete' and {id} or {id, key, bucket}}},
                   {fence = {sid, 0}, wb = {bucket}}))
end
local function deliver(i)
    local events = ok(call(i, 'tarn_lazy_control', 'pending'))[2]
    if #events == 0 then return end
    local route = {}; for _, a in ipairs(events) do route[#route + 1] = a[5] end
    ok(call(i, 'tarn_lazy_control', 'ack', ok(call(i, 'tarn_lazy_apply', events, route))[2]))
end
local function pause(i)
    local r = ok(call(i, 'tarn_lazy_control', 'pause'))
    assert(r[2] == true and r[3] == 0 and r[4] == 0, json.encode(r))
    ev(i, 'tarn.cfg.recovery_after=1000000')
end
local function kill_owned(i, which)
    local s = exact(i, 'identity before ' .. which .. ' leader death')
    local f = assert(io.open(work .. '/s' .. i .. '.pid')); local pid = tonumber(f:read('*a')); f:close()
    assert(s.pid == pid and s.workdir == work .. '/s' .. i, 'owned process identity mismatch')
    ffi.cdef('int kill(int,int); int getpid(void);')
    assert(pid ~= ffi.C.getpid())
    f = assert(fio.open(work .. '/killed-' .. which, {'O_WRONLY','O_CREAT','O_EXCL'}, 384))
    assert(f:write(tostring(i) .. '\n')); f:close()
    f = assert(fio.open(work .. '/identity-' .. which .. '.json', {'O_WRONLY','O_CREAT','O_EXCL'}, 384))
    assert(f:write(json.encode(s))); f:close()
    event('SIGKILL owned leader', {index = i, identity = s})
    assert(ffi.C.kill(pid, 9) == 0, 'SIGKILL failed')
end
local function main()
    if stage == 'setup' then
        for i = 1, 3 do await('replica module ready', function() return ev(i, 'return rawget(_G,"tarn")~=nil') end); pause(i) end
        local l = leader()
        ok(call(l, 'tarn_bucket_init', 16, {{1,16}}))
        local spec = {gen=gen,base_space_id=sid,local_index_id=2,route_parts=1,key_fields={2},key_types={'string'},
                      pk_fields={1},pk_native_fields={1},pk_types={'unsigned'},bucket_field=3}
        local def = {id=sid,name=name,ver=0,engine=engine,json='{}',lazy={spec},
            format={{name='id',type='unsigned'},{name='v',type='string'},{name='bucket_id',type='unsigned'}},
            indexes={{name='primary',unique=true,parts={{1,'unsigned'}}},
                     {name='bucket_id',unique=false,parts={{3,'unsigned'}}},
                     {name='v',unique=false,parts={{2,'string'}}}}}
        local a={def=def,timeout=5}
        ok(call(l,'tarn_create_prepare',name,a,1))
        for _=1,5 do local r=ok(call(l,'tarn_lazy_bootstrap',name,a,1)); if r[2] then break end end
        ok(call(l,'tarn_lazy_ready',name,a,1)); ok(call(l,'tarn_create_apply',name,a))
        l=leader(); exact(l,'RF3 fsync ' .. mode_label .. ' capture/quota initialized')
        write(l,'insert',ids[1],'old')
        -- The decision shard creates STAGING atomically with its YES. A
        -- pre-existing decision record correctly fences a fresh PREPARE.
        -- Empty/local entry readiness does not establish the distributed
        -- clock-peer certificate. Wait for that before the one-shot PREPARE.
        await('prepared transaction clock peer ready', function()
            return ev(l, 'return tarn.clock.check({1}) == nil')
        end)
        local r=call(l,'tarn_prepare',txid,1,{1},{{'insert',sid,{ids[2],'prepared',bucket}}},{},{},require('clock').realtime(),box.NULL,{sid,0},{bucket})
        assert(r[1]=='yes',json.encode(r))
        assert(call(l,'tarn_txn_cas',txid,'staging','committed',{1})=='committed')
        -- Release every held observer before any loss, so later real role
        -- notifications run normally on followers as well as the leader.
        for i=1,3 do release_status_notification(i) end
        local s=exact(l,'confirmed base/outbox and durable YES retain exact quota')
        check('PREPARE reserves without sequence allocation',s.rows[1] and not s.rows[2] and s.depth==1 and s.reserved==1 and s.slots==1 and s.counter==1 and s.part=='prepared',s)
        for i=1,3 do await('confirmed pending/promise reaches every replica',function() local x=state(i); return x and x.rows[1] and not x.rows[2] and x.depth==1 and x.reserved==1 and x.slots==1 and x.counter==1 end) end
    elseif stage=='kill-first' or stage=='kill-second' then
        kill_owned(leader(),stage=='kill-first' and 'first' or 'second')
    elseif stage=='after-first' then
        local l=leader(); local s=exact(l,'first promotion rebuilds capture and exact confirmed quota')
        check('promoted leader retains base/outbox/promise/counter',s.rows[1] and not s.rows[2] and s.depth==1 and s.reserved==1 and s.slots==1 and s.counter==1 and s.part=='prepared',s)
        ok(call(l,'tarn_autocommit',{},{})) -- refresh entry caches before intentional UNKNOWN probe
        local blocked=ev(l,[[
local sid,id,b=...
local q=tarn.lazy.quota
local rebuild,change=q.rebuild,q.change
local observations,attempts={},0
local function image()
    box.begin{txn_isolation='read-confirmed'}
    local ok,value=pcall(function() return require('msgpack').encode({box.space[sid]:select{},
        box.space._tarn_lazy_outbox:select{},box.space._tarn_lazy_reservation:select{},
        box.space._tarn_lazy_counter:select{},box.space._tarn_lazy_slots:select{},
        box.space._tarn_lazy_floor:select{}}) end)
    box.rollback(); if not ok then error(value) end
    return value
end
local before=image()
-- Hold background refresh, not the production admission guard. The
-- ordinary barrier may yield; UNKNOWN must still exist at q.change.
q.rebuild=function() attempts=attempts+1; return false end
q.change=function(...) observations[#observations+1]=q.ready(); return change(...) end
local good,r=pcall(function()
    tarn.lazy.quota_invalidate()
    assert(not q.ready())
    return tarn_autocommit({{'insert',sid,{id,'blocked',b}}},{fence={sid,0},wb={b}})
end)
q.rebuild,q.change=rebuild,change
if not good then error(r) end
local all_unknown=#observations>0
for _,ready in ipairs(observations) do all_unknown=all_unknown and not ready end
return {r,box.space[sid]:get{id}==nil,all_unknown,before==image(),
    tarn.lazy.worker~=nil and tarn.lazy.worker:status()~='dead',attempts,observations}
]],{sid,ids[3],bucket})
        check('UNKNOWN rejects new source publication without user tuple',blocked[1][2]==1213 and blocked[2]
              and blocked[3] and blocked[4] and blocked[5],blocked)
        l=leader(); ok(call(l,'tarn_apply',txid)); s=exact(l,'known committed APPLY converts durable promise after promotion')
        check('APPLY consumes promise once and advances sequence',s.rows[2] and s.counter==2 and s.reserved==0 and s.slots==0 and s.depth==2,s)
        ev(l,[[local old=tarn_lazy_control('pending')[2][1]; assert(old[4]==1 and old[6][1]=='old' and old[9]); box.space._tarn_meta:replace{'test.lazy.rf3.old',old}]])
        deliver(l); write(l,'delete',ids[1]); deliver(l)
        local r=ev(l,[[local g,b=...; local old=box.space._tarn_meta:get{'test.lazy.rf3.old'}.value; return tarn_lazy_gc(g,old[5],{old[5]})]],{gen,bucket}); ok(r)
        s=exact(l,'target floor confirmed before second promotion')
        check('delete/ACK/GC leaves floor3 and only prepared membership',s.floor==3 and s.counter==3 and s.depth==0 and s.reserved==0 and s.live.old==0 and s.live.prepared==1,s)
    elseif stage=='caught-up' then
        local f=assert(io.open(work..'/killed-first')); local i=tonumber(f:read('*a')); f:close()
        f=assert(io.open(work..'/identity-first.json')); local before=json.decode(f:read('*a')); f:close()
        await('restarted first node catches confirmed floor',function() local s=state(i); return s and s.floor==3 and s.counter==3 and s.live.old==0 and s.live.prepared==1 end)
        release_status_notification(i)
        pause(i)
        local s=state(i)
        check('first killed replica restarts same identity and catches up',s.uuid==before.uuid and s.pid~=before.pid and s.engine==engine,s)
        for j=1,3 do await('all three replicas caught up before second death',function() local x=state(j); return x and x.floor==3 and x.counter==3 and x.depth==0 and x.reserved==0 end); pause(j) end
        exact(leader(),'three-replica restored cluster ready for second death')
    elseif stage=='after-second' then
        local l=leader(); local s=exact(l,'second promotion rebuilds exact empty quota and capture')
        check('floor and sequence survive second leader death',s.floor==3 and s.counter==3 and s.depth==0 and s.reserved==0 and s.slots==0 and not s.rows[1] and s.rows[2],s)
        ok(ev(l,[[local old=box.space._tarn_meta:get{'test.lazy.rf3.old'}.value; return tarn_lazy_apply({old},{old[5]})]]))
        s=state(l); check('delayed old ADD cannot bypass promoted floor',s.live.old==0 and s.live.prepared==1 and s.counter==3,s)
        write(l,'insert',ids[1],'new'); s=state(l)
        check('reinsert uses next sequence and durable paused outbox',s.counter==4 and s.depth==1 and s.live.new==0,s)
        ok(call(l,'tarn_lazy_control','resume'))
        await('promoted shipper resumes delivery',function() local x=state(l); return x.depth==0 and x.live.new==1 end)
        s=exact(l,'resumed real shipper drains confirmed source quota')
        check('final native memberships and committed tuples match',s.rows[1] and s.rows[2] and not s.rows[3] and s.live.old==0 and s.live.prepared==1 and s.live.new==1 and s.depth==0 and s.reserved==0 and s.counter==4,s)
    else error('unknown stage '..stage) end
end
local success,err=pcall(main)
for _,conn in ipairs(c) do conn:close() end
if not success then io.stderr:write('FAIL lazy RF3 ',engine,' ',stage,': ',tostring(err),'\n'); os.exit(1) end
print(string.format('%d passed, 0 failed (%s %s)',pass,engine,stage))
os.exit(0)
