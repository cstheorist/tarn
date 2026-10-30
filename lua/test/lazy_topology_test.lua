-- Dedicated two-shard fixture; run only via lazy_faults.sh.
local net, fiber, json = require('net.box'), require('fiber'), require('json')
local opts = {user = 'tarn', password = 'tarn', wait_connected = 5}
local c = {net.connect(assert(os.getenv('S1')), opts), net.connect(assert(os.getenv('S2')), opts),
           net.connect(assert(os.getenv('S3')), opts)}
local pass, fail = 0, 0
local function ok(r) assert(r[1] == 'ok', debug.traceback(json.encode(r), 2)); return r end
local function check(name, yes, extra)
    if yes then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. ' ' .. json.encode(extra)) end
end
local function call(i, name, ...) return c[i]:call(name, {...}) end
local function wait(fn)
    local deadline = fiber.clock() + 5
    repeat if fn() then return true end; fiber.sleep(0.02) until fiber.clock() >= deadline
    return false
end
local sid, gen, name = 62001, 62002, 'test.lazy_topology'
local spec = {gen = gen, base_space_id = sid, local_index_id = 2, route_parts = 2,
              key_fields = {2,1}, key_types = {'string','unsigned'}, pk_fields = {1}, pk_native_fields = {1},
              pk_types = {'unsigned'}, bucket_field = 3}
local def = {id = sid, name = name, ver = 0, engine = 'memtx', json = '{}', lazy = {spec},
    format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'string'}, {name = 'bucket_id', type = 'unsigned'}},
    indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
               {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}},
               {name = 'v', unique = false, parts = {{2, 'string'},{1,'unsigned'}}}}}
local a = {def = def, timeout = 5}
for i = 1, 2 do
    ok(call(i, 'tarn_bucket_init', 16, {{(i - 1) * 8 + 1, i * 8}}))
    ok(call(i, 'tarn_create_prepare', name, a, 1))
    repeat local r = ok(call(i, 'tarn_lazy_bootstrap', name, a, 1)); if r[2] then break end until false
    ok(call(i, 'tarn_lazy_ready', name, a, 1)); ok(call(i, 'tarn_create_apply', name, a))
end
local key, target
for i = 1, 100 do
    local k = 'target-' .. i
    local b = c[1]:eval('local s,k=... return tarn.lazy.bucket(s,{k,1})', {spec, k})
    if b > 8 then key, target = k, b; break end
end
assert(key)
local function depth(i) return call(i, 'tarn_lazy_stats')[2].depth end
local function write(i, id, value) return call(i, 'tarn_autocommit', {{'replace', sid, {id, value, 1}}}, {fence = {sid, 0}, wb = {1}}) end
local function lives(i, value)
    return c[i]:eval('local g,k=... return box.space[g].index.ordered:count{true,k}', {gen, value})
end
local function copy_rows(from, to, bucket, move)
    local after = box.NULL
    repeat
        local r = ok(call(from, 'tarn_bucket_rows', bucket, after))
        ok(call(to, 'tarn_bucket_recv_rows', bucket, move, r[2]))
        after = r[3]
    until after == box.NULL
end
local function finish_move(from, to, bucket, move, generation)
    ok(call(from, 'tarn_bucket_fence', bucket, move, 5))
    local at = 1
    repeat
        local r = ok(call(from, 'tarn_bucket_delta', bucket, move, generation, at))
        ok(call(to, 'tarn_bucket_recv_rows', bucket, move, r[2]))
        at = r[3]
    until at == box.NULL
    ok(call(from, 'tarn_bucket_send', bucket, move))
    ok(call(to, 'tarn_bucket_recv', bucket, move))
end
ok(call(1, 'tarn_lazy_control', 'pause'))
ok(write(1, 1, key))
check('remote destination has no unshipped candidate', depth(1) == 1 and lives(2, key) == 0)
local r = ok(call(1, 'tarn_bucket_send_begin', 1, 2, 'lazy-source', 5))
local generation = r[2]
ok(call(2, 'tarn_bucket_recv_begin', 1, 1, 'lazy-source'))
copy_rows(1, 2, 1, 'lazy-source'); ok(call(2, 'tarn_bucket_hold', 1, 'lazy-source'))
r = call(1, 'tarn_bucket_fence', 1, 'lazy-source', 0.05)
check('source handoff cannot pass an unacknowledged outbox', r[1] == 'timeout' and depth(1) == 1, r)
check('removal counts pending source obligations even without prepared transactions', call(1, 'tarn_shard_records', 1)[2] > 0)
ok(call(1, 'tarn_lazy_control', 'resume'))
check('shipper discovers the remote index bucket owner', wait(function() return depth(1) == 0 end) and lives(2, key) == 1)
finish_move(1, 2, 1, 'lazy-source', generation)
local copied = ok(call(2, 'tarn_lazy_frontiers', {1}, {1}))[2][1]
check('source counter and completed prefix survive bucket handoff', copied[2] == 1 and copied[3] == 1, copied)
ok(write(2, 1, key))
check('new source owner increments rather than resets sequence', wait(function() return depth(2) == 0 end)
      and ok(call(2, 'tarn_lazy_frontiers', {1}, {1}))[2][1][2] == 2)
-- Create a tombstone on this target, then collect it and move the durable floor.
local other
for i = 101, 1000 do
    local k = 'target-' .. i
    if c[2]:eval('local s,k=... return tarn.lazy.bucket(s,{k,1})', {spec, k}) == target then other = k; break end
end
assert(other)
ok(call(2, 'tarn_lazy_control', 'pause'))
ok(write(2, 1, other))
local events = call(2, 'tarn_lazy_control', 'pending')[2]
local route = {}; for _, e in ipairs(events) do route[#route + 1] = e[5] end
local ack = ok(call(2, 'tarn_lazy_apply', events, route))
ok(call(2, 'tarn_lazy_control', 'ack', ack[2]))
ok(call(2, 'tarn_lazy_gc', gen, target, {target}))
local floor = c[2]:eval('local g,b=... return box.space._tarn_lazy_floor:get{g,b,1}.seq', {gen, target})
local moved = ok(call(2, 'tarn_bucket_send_begin', target, 1, 'lazy-target', 5))
ok(call(1, 'tarn_bucket_recv_begin', target, 2, 'lazy-target'))
copy_rows(2, 1, target, 'lazy-target'); ok(call(1, 'tarn_bucket_hold', target, 'lazy-target'))
local function prefix_count(i, value)
    local map = ok(call(i, 'tarn_buckets'))
    local after, count, pages = box.NULL, 0, 0
    repeat
        local page = ok(call(i, 'tarn_lazy_select', gen, {value}, 'EQ', 1, after, {e=map[3],m=map[6]}))
        count, pages, after = count + #page[3], pages + 1, page[4]
        assert(pages <= 10, 'prefix cursor failed to advance')
    until after == box.NULL
    return count
end
check('receiving copies are excluded from composite prefix pages', prefix_count(1,other)==0 and prefix_count(2,other)==1)
finish_move(2, 1, target, 'lazy-target', moved[2])
check('sent copies cannot duplicate composite prefix results', prefix_count(2,other)==0 and prefix_count(1,other)==1)
local imported = c[1]:eval('local g,b=... return box.space._tarn_lazy_floor:get{g,b,1}.seq', {gen, target})
check('target handoff copies typed live rows and durable acceptance floors', imported == floor and lives(1, other) == 1)
-- The old-key delete at seq3 is harmless after the floor moved, as is a
-- synthesized delayed seq1 ADD carrying the exact old membership identity.
local oldadd = {}; for i, v in ipairs(events[1]) do oldadd[i] = v end
oldadd[4], oldadd[9] = 1, true
ok(call(1, 'tarn_lazy_apply', {oldadd}, {target}))
check('a delayed frame cannot resurrect an old membership after target handoff', lives(1, key) == 0 and lives(1, other) == 1)
ok(call(2, 'tarn_lazy_control', 'resume'))
-- A joining shard needs active hidden schema/capture plus retired fences.
local retired_gen = gen + 1
for i = 1, 2 do
    c[i]:eval([[local g,s=... local copy={} for k,v in pairs(s) do copy[k]=v end copy.gen=g
        box.space._tarn_lazy_defs:insert{g,s.base_space_id,0,'retired',0,copy,box.NULL,true}]], {retired_gen, spec})
end
-- Native/raw schemas on an upgraded candidate can occupy different low
-- IDs. Move wire must use the whitelisted stable names, not these numbers.
c[3]:eval([[for i,name in ipairs{'_tarn_lazy_counter','_tarn_lazy_version','_tarn_lazy_floor'} do
    local old=box.space[name] local fmt=old:format() old:rename(name..'_old_empty')
    local s=box.schema.space.create(name,{id=9050+i,format=fmt})
    if name=='_tarn_lazy_counter' then s:create_index('pk',{parts={'bucket_id'}})
    elseif name=='_tarn_lazy_version' then s:create_index('pk',{parts={'base_id','pk'}})
    else s:create_index('pk',{parts={'gen','bucket_id','source'}}) end
    s:create_index('bucket_id',{parts={'bucket_id'},unique=false})
end]])
local export = ok(call(1, 'tarn_schema_export'))[2]
check('lazy schema export starts with the authoritative composite metadata tag', export[1].lazy_v1.version == 2
      and #export[1].lazy_v1.active == 1 and #export[1].lazy_v1.retired == 1)
local old_importer = pcall(function()
    local want = {}; for _, t in ipairs(export) do want[t.name] = true end
end)
check('the metadata-first envelope makes the old importer fail before native creation', not old_importer)
local rejected = call(3, 'tarn_schema_import', export)
check('an old coordinator without the V1 token cannot import lazy metadata', rejected[1] == 'error' and rejected[2] == 1412
      and c[3]:eval('return box.space[...] == nil', {sid}), rejected)
local mp = require('msgpack')
local malformed = mp.decode(mp.encode(export)); malformed[1].lazy_v1.version = 3
rejected = call(3, 'tarn_schema_import', malformed, 1)
check('unknown lazy envelope version fails before any imported native table', rejected[1] == 'error'
      and c[3]:eval('return box.space[...] == nil', {sid}), rejected)
ok(call(3, 'tarn_schema_import', export, 2)); ok(call(3, 'tarn_schema_import', export, 2))
check('candidate join retries install only committed active generations and retired fences',
    c[3]:eval([[local sid,g,ret=... return box.space[sid]:len()==0 and box.space[g].index.ordered~=nil
      and box.space._tarn_lazy_capture:get{sid}.specs[1].gen==g
      and box.space._tarn_lazy_defs:get{g}.phase=='ready'
      and box.space._tarn_lazy_defs:get{ret}.phase=='retired' and box.space[ret]==nil]], {sid,gen,retired_gen}))
ok(call(3, 'tarn_bucket_init', 16, {}))
local members = {ver = 1, shards = {}}
for i = 1, 3 do members.shards[i] = {id=i,replicas={assert(os.getenv('S'..i))},state='active'} end
for i = 1, 3 do ok(call(i, 'tarn_members_set', members)) end
assert(wait(function()
    for i = 1, 3 do
        if not c[i]:eval('return tarn.clock.status().read_ready') then return false end
    end
    return true
end), 'clock policy readiness after actual membership change')
local joined = ok(call(1, 'tarn_bucket_send_begin', target, 3, 'lazy-join', 5))
ok(call(3, 'tarn_bucket_recv_begin', target, 1, 'lazy-join'))
copy_rows(1, 3, target, 'lazy-join'); ok(call(3, 'tarn_bucket_hold', target, 'lazy-join'))
finish_move(1, 3, target, 'lazy-join', joined[2])
local joined_floor = c[3]:eval('local g,b=... return box.space._tarn_lazy_floor:get{g,b,1}.seq', {gen,target})
ok(call(3, 'tarn_lazy_apply', {oldadd}, {target}))
check('JOIN then target move preserves live memberships and anti-resurrection floors',
      joined_floor == floor and lives(3, other) == 1 and lives(3, key) == 0)
local obsolete = {}; for i,v in ipairs(oldadd) do obsolete[i]=v end; obsolete[2]=retired_gen
local retired_ack = call(3, 'tarn_lazy_apply', {obsolete}, {target})
check('a joined shard can authoritatively ACK old retired-generation frames', retired_ack[1]=='ok'
      and tostring(retired_ack[2][1])==tostring(obsolete[1]) and c[3]:eval('return box.space[...] == nil',{retired_gen}), retired_ack)
-- Old destinations must fail before the source starts noting a move.
c[3]:eval([[rawset(_G,'__lazy_saved_caps',tarn_lazy_capabilities)
    tarn_lazy_capabilities=function() return {'ok',1,{max_entries=128,max_bytes=1048576,max_seq=9007199254740991,aux_names=1}} end]])
local legacy_move=call(2,'tarn_bucket_send_begin',1,3,'old-aux-receiver',5)
c[3]:eval('tarn_lazy_capabilities=__lazy_saved_caps __lazy_saved_caps=nil')
local source_before=c[2]:eval('return box.space._tarn_buckets:get{1}.state')
check('an old receiver without composite routing fails before move effects',legacy_move[1]=='error' and legacy_move[2]==1412
      and source_before=='active',legacy_move)
local source_move=ok(call(2,'tarn_bucket_send_begin',1,3,'heterogeneous-source',5))
ok(call(3,'tarn_bucket_recv_begin',1,2,'heterogeneous-source'))
local unknown_ok,unknown_error=pcall(call,3,'tarn_bucket_recv_rows',1,'heterogeneous-source',{{'_tarn_lazy_unknown',{1}}})
check('unknown auxiliary names fail rather than silently discard bucket data',not unknown_ok
      and unknown_error.code==1210 and c[3]:eval('return box.space._tarn_recv:count{1}')==0)
copy_rows(2,3,1,'heterogeneous-source')
-- Keep the old owner's hint live across the entire move. Membership and
-- leadership stay fixed; the local bucket-map revision must invalidate it.
local cached_owner=c[3]:eval([[tarn_buckets()
    local owner=tarn.lazy.owners({1})[1]
    tarn.lazy.owner_cache.until_at=require('clock').monotonic()+60
    return owner]])
check('receiving shard caches the current source owner before handoff',cached_owner==2,cached_owner)
-- Change after initial copy while the source is SENDING. Counter/version
-- must come from the final delta, not remain at their first-copy values.
ok(write(2,1,other)); assert(wait(function() return depth(2)==0 end))
ok(call(2,'tarn_autocommit',{{'delete',sid,{1}}},{fence={sid,0},wb={1}}))
assert(wait(function() return depth(2)==0 end))
ok(call(3,'tarn_bucket_hold',1,'heterogeneous-source'))
finish_move(2,3,1,'heterogeneous-source',source_move[2])
local aux=c[3]:eval([[local sid=... return {box.space._tarn_lazy_counter.id,box.space._tarn_lazy_version.id,
    box.space._tarn_lazy_floor.id,box.space._tarn_lazy_counter:get{1}.seq,
    box.space._tarn_lazy_version.index.bucket_id:count{1}}]],{sid})
check('heterogeneous-ID copy, counter delta and version deletion use stable auxiliary names',
      aux[1]==9051 and aux[2]==9052 and aux[3]==9053 and aux[4]==5 and aux[5]==0,aux)
check('completed source handoff refreshes owner hints without waiting for TTL',
      c[3]:eval('return tarn.lazy.owners({1})[1]')==3)
ok(call(3,'tarn_lazy_gc',gen,target,{target}))
c[3]:eval([[box.space._tarn_meta:replace{'test.lazy.join_restart',{sid=...,gen=select(2,...),target=select(3,...),
    replay=select(4,...),counter=5,aux={9051,9052,9053}}}]],{sid,gen,target,oldadd})
print(string.format('%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
