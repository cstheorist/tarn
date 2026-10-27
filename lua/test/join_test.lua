-- Dependent co-located primary-prefix reads (issue #83). Run after the
-- storage tests and before bucket_test.lua, while the shards have no map.
local net = require('net.box')
local fiber = require('fiber')
local json = require('json')
local msgpack = require('msgpack')
local c = net.connect(os.getenv('S1'), {user = 'tarn', password = 'tarn', wait_connected = 5})
assert(c:is_connected(), 'cannot connect to shard')
local pass, fail, tables = 0, 0, {}
local function check(name, cond, extra)
    if cond then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. (extra and ('  -- ' .. json.encode(extra)) or '')) end
end
local function cfg(k, v)
    return c:eval('local k, v = ... local o = tarn.cfg[k] tarn.cfg[k] = v return o', {k, v})
end
local function hook(action) return c:call('tarn_test_hook', {'join_scan', action}) end
local function result(f)
    local ok, r, e = pcall(f.wait_result, f, 10)
    if not ok then return {'raised', tostring(r)} end
    return r and r[1] or {'raised', tostring(e)}
end
local function complete(r) return r[1] == 'ok' and r[3] == true end
local function declined(r) return r[1] == 'ok' and r[3] == false and #r[2] == 0 end
local function pair(tag, pe, ce)
    local p, d = 'test.join_' .. tag .. '_p', 'test.join_' .. tag .. '_c'
    local pf = {{name = 'r', type = 'unsigned'}, {name = 'id', type = 'unsigned'},
                {name = 'v', type = 'integer'}, {name = 'ref', type = 'unsigned', is_nullable = true},
                {name = 'bucket_id', type = 'unsigned'}, {name = 'added', type = 'unsigned', is_nullable = true}}
    local cf = {{name = 'r', type = 'unsigned'}, {name = 'pid', type = 'unsigned'},
                {name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
                {name = 'bucket_id', type = 'unsigned'}}
    local pi = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}, {field = 2, type = 'unsigned'}}},
                {name = 'bucket_id', unique = false, parts = {{field = 5, type = 'unsigned'}}}}
    local ci = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}, {field = 2, type = 'unsigned'},
                                                        {field = 3, type = 'unsigned'}}},
                {name = 'bucket_id', unique = false, parts = {{field = 5, type = 'unsigned'}}}}
    c:call('tarn_create_table', {{name = p, engine = pe, format = pf, indexes = pi}})
    c:call('tarn_create_table', {{name = d, engine = ce, format = cf, indexes = ci}})
    tables[#tables + 1], tables[#tables + 2] = p, d
    c:reload_schema()
    local ps, cs = c.space[p].id, c.space[d].id
    c:call('tarn_autocommit', {{{'insert', ps, {42, 1, 10, 7, 1}}, {'insert', ps, {42, 2, 20, 7, 1}},
                               {'insert', ps, {42, 3, 30, box.NULL, 1}}, {'insert', cs, {42, 1, 1, 100, 1}}}})
    return ps, cs
end
local function reads(ps, cs)
    return {{ps, 0, {{0, 42}}}, {cs, 1, {{1, 1}, {1, 2}}}}
end
local function read(rs, route) return c:call('tarn_select_join', {rs, true, false, route or box.NULL}) end
local function prepare(id, writes)
    local st = c:new_stream()
    st:begin()
    return st:call('tarn_prepare', {id, 1, {1}, writes, {}, {}, fiber.time()})
end

print('dependent primary prefixes: complete slots, misses and NULLs')
local ps, cs = pair('base', 'memtx', 'memtx')
local rs = reads(ps, cs)
local r = read(rs)
check('every table has its actual prefixes, including missing children', complete(r) and #r[2] == 2
      and #r[2][1][1] == 1 and #r[2][1][2] == 3 and #r[2][2][1] == 3 and #r[2][2][2] == 1, r)
local dedup = {{ps, 0, {{0, 42}}}, {cs, 1, {{1, 1}, {1, 4}}}}
r = read(dedup)
check('repeated parent fields derive one key, explicit NULL derives none', complete(r)
      and #r[2][2][1] == 1 and r[2][2][1][1][2] == 7 and #r[2][2][2] == 0, r)
r = read({{ps, 0, {{0, 99}}}, {cs, 1, {{1, 1}, {1, 2}}}, {cs, 2, {{0, 42}}}})
check('empty parents suppress every dependent descendant, even constant bindings', complete(r)
      and #r[2][1][1] == 1 and #r[2][1][2] == 0 and #r[2][2][1] == 0 and #r[2][3][1] == 0, r)
r = read({{ps, 0, {{0, 42}}}, {cs, 1, {{1, 1}, {1, 6}}}})
check('an absent physical source field declines instead of becoming NULL', declined(r), r)
for _, bad in ipairs({{}, {{ps, 1, {{0, 42}}}}, {{ps, 0, {{1, 1}}}},
                     {{ps, 0, {{0, 42}}}, {cs, 2, {{1, 1}}}},
                     {{ps, 0, {{0, 42}}}, {cs, 1, {{1, 99}}}}, {{ps, 0, {{2, 42}}}}}) do
    r = read(bad)
    check('malformed or forward descriptor fails with 1210', r[1] == 'error' and r[2] == 1210, r)
end
local many = {}
for i = 1, 17 do many[i] = {ps, 0, {{0, 42}}} end
r = read(many)
check('more than sixteen slots is refused', r[1] == 'error' and r[2] == 1210, r)

print('a first grouped read restores persisted DDL holds')
do
    -- A new leader may have DDL_N == 0 while a hold is durable. Storing
    -- it with stale_fences models that cache state before the first read.
    local hold = 'hold:' .. cs
    local lock_wait = cfg('lock_wait', 0.05)
    c:eval('local k = ... box.space._tarn_meta:replace{k, {ver = 1, label = "JOIN_PERSISTED_HOLD"}} '
           .. 'tarn.stale_fences()', {hold})
    r = read(rs)
    check('a persisted child-table hold is restored before the grouped read can reply',
          r[1] == 'conflict' and r[2] == 1213 and c:call('tarn_info').ddl_held == 1, r)
    c:eval('box.space._tarn_meta:delete{...} tarn.stale_fences()', {hold})
    -- The scalar API also refreshes the cache: cleanup stays valid when
    -- running this regression against the unfixed grouped procedure.
    c:call('tarn_select', {ps, 0, {42}, 'EQ', box.NULL, box.NULL, true, true})
    r = read(rs)
    check('removing the persisted hold restores normal grouped reads',
          complete(r) and c:call('tarn_info').ddl_held == 0, r)
    cfg('lock_wait', lock_wait)
end

print('one snapshot across memtx, vinyl and mixed-engine tables')
for _, engines in ipairs({{'mm', 'memtx', 'memtx'}, {'vv', 'vinyl', 'vinyl'},
                          {'mv', 'memtx', 'vinyl'}, {'vm', 'vinyl', 'memtx'}}) do
    local p, d = pair(engines[1], engines[2], engines[3])
    local spec = reads(p, d)
    hook('hold')
    local f = c:call('tarn_select_join', {spec, true, false, box.NULL}, {is_async = true})
    hook('reached')
    local w = c:call('tarn_autocommit', {{{'replace', p, {42, 1, 11, 7, 1}},
                                         {'replace', d, {42, 1, 1, 101, 1}}}})
    hook('release')
    r = result(f)
    local pv = complete(r) and r[2][1][2][1][3]
    local cv = complete(r) and r[2][2][2][1][4]
    check(engines[1] .. ': a local atomic write cannot fracture the table snapshots',
          w[1] == 'ok' and complete(r) and ((pv == 10 and cv == 100) or (pv == 11 and cv == 101)), {w, r})
end

print('the union of prepared-lock history covers dependent misses')
local vp, vc = pair('history', 'vinyl', 'vinyl')
local vr = reads(vp, vc)
local before = c:call('tarn_info').stats.vy_rereads
hook('hold')
local f = c:call('tarn_select_join', {vr, true, false, box.NULL}, {is_async = true})
hook('reached')
r = prepare('JOIN_HISTORY', {{'insert', vc, {42, 2, 1, 200, 1}}})
check('only the child insert prepares while the parent scan is held', r[1] == 'yes', r)
c:call('tarn_apply', {'JOIN_HISTORY'})
hook('release')
r = result(f)
check('a released W claim on a previously empty child prefix rereads the whole group', complete(r)
      and #r[2][2][2] == 2 and c:call('tarn_info').stats.vy_rereads > before, r)
r = prepare('JOIN_WAIT', {{'insert', vc, {42, 3, 1, 300, 1}}})
check('a second missing-child insert prepares', r[1] == 'yes', r)
f = c:call('tarn_select_join', {vr, true, false, box.NULL}, {is_async = true})
fiber.sleep(0.1)
check('a pending W claim on a miss prevents a reply', not f:is_ready())
c:call('tarn_apply', {'JOIN_WAIT'})
r = result(f)
check('... and the reply includes the inserted child after APPLY', complete(r) and #r[2][2][2] == 3, r)

print('a persisted DDL hold invalidated during a grouped snapshot is refreshed')
do
    local p, d = pair('ddl_stale', 'vinyl', 'vinyl')
    local hold = 'hold:' .. d
    local lock_wait = cfg('lock_wait', 0.05)
    hook('hold')
    local held = c:call('tarn_select_join', {reads(p, d), true, false, box.NULL}, {is_async = true})
    local reached = hook('reached')
    check('the grouped snapshot pauses before a child hold becomes durable',
          reached[2] == 1 and not held:is_ready(), reached)
    c:eval('local k = ... box.space._tarn_meta:replace{k, {ver = 1, label = "JOIN_STALE_HOLD"}} '
           .. 'tarn.stale_fences()', {hold})
    hook('release')
    r = result(held)
    check('a stale fence cache is refreshed before the group can accept its snapshot',
          r[1] == 'conflict' and r[2] == 1213 and c:call('tarn_info').ddl_held == 1, r)
    c:eval('box.space._tarn_meta:delete{...} tarn.stale_fences()', {hold})
    c:call('tarn_select', {ps, 0, {42}, 'EQ', box.NULL, box.NULL, true, true})
    check('cleaning the persisted child hold restores the fence cache',
          c:call('tarn_info').ddl_held == 0)
    cfg('lock_wait', lock_wait)
end

print('a DDL hold released during a grouped snapshot is still observed')
for _, refresh in ipairs({false, true}) do
    local label = refresh and 'ddl_rebuilt_history' or 'ddl_history'
    local p, d = pair(label, 'vinyl', 'vinyl')
    local name, def = c:eval('local s = box.space[...] return s.name, box.space._tarn_catalog:get{s.name}.def', {p})
    def.id, def.ver, def.json = p, 2, '{}'
    local a = {id = p, ver = 2, timeout = 5, def = def}
    r = c:call('tarn_truncate_prepare', {name, a})
    check('TRUNCATE holds the vinyl root before the grouped read begins',
          r[1] == 'ok' and c:call('tarn_info').ddl_held == 1, r)
    hook('hold')
    local held = c:call('tarn_select_join', {reads(p, d), true, false, box.NULL}, {is_async = true})
    local reached = hook('reached')
    check('the grouped snapshot is paused after capturing the held root rows',
          reached[2] == 1 and not held:is_ready(), reached)
    r = c:call('tarn_truncate_apply', {name, a})
    check('TRUNCATE APPLY empties the root and releases its hold before the read resumes',
          r[1] == 'ok' and c:call('tarn_info').ddl_held == 0
          and c:eval('return box.space[...]:len()', {p}) == 0, r)
    if refresh then
        c:eval('tarn.stale_fences()')
        -- Refresh in another request on an unrelated table. It clears the
        -- stale flag before the paused group performs its final check.
        local scalar = c:call('tarn_select', {ps, 0, {42}, 'EQ', box.NULL, box.NULL, true, true})
        check('an unrelated scalar read rebuilds the released-hold cache before the group resumes',
              scalar[1] == 'ok' and c:call('tarn_info').ddl_held == 0, scalar)
    end
    hook('release')
    r = result(held)
    check((refresh and 'a rebuilt cache cannot erase DDL history needed by the paused group'
           or 'a released DDL hold forces a whole-group reread or conflict, not the old held root rows'),
          (r[1] == 'conflict' and r[2] == 1213)
          or (complete(r) and #r[2][1][2] == 0 and #r[2][2][1] == 0 and #r[2][2][2] == 0), r)
end

print('stream view, read-set metadata and total response caps')
local st = c:new_stream()
r = st:call('tarn_select_join', {rs, false, true, box.NULL})
check('the first request begins a stream and returns missing-prefix metadata too', complete(r)
      and #r[2][2][1] == 3, r)
c:call('tarn_autocommit', {{{'insert', cs, {42, 2, 1, 200, 1}}}})
local rr = {{ps, 0, {42}, {42}, true, true}, {cs, 0, {42, 1}, {42, 1}, true, true},
            {cs, 0, {42, 2}, {42, 2}, true, true}, {cs, 0, {42, 3}, {42, 3}, true, true}}
r = st:call('tarn_validate', {'JOIN_STREAM', {}, rr, true, box.NULL})
check('recording the returned missing prefix fails validation after an insert', r[1] == 'conflict', r)
local rowcap = cfg('scan_page_rows', 5)
r = read(rs) -- three parents plus two children
check('an exact aggregate row cap succeeds', complete(r), r)
cfg('scan_page_rows', 4)
r = read(rs)
check('tables individually fit but their shared row total declines without partial slots', declined(r), r)
cfg('scan_page_rows', rowcap)
local bytes = c:eval([[
    local p, c = ... local n = 0
    for _, sid in ipairs({p, c}) do for _, t in box.space[sid]:pairs() do n = n + t:bsize() end end
    local mp = require('msgpack')
    return n + #mp.encode({42}) + #mp.encode({42, 1}) + #mp.encode({42, 2}) + #mp.encode({42, 3})
]], {ps, cs})
local bytecap = cfg('scan_page_bytes', bytes)
r = read(rs)
check('exact tuple plus key-metadata byte budget succeeds', complete(r), r)
cfg('scan_page_bytes', bytes - 1)
r = read(rs)
check('the shared byte cap includes derived-key metadata', declined(r), r)
cfg('scan_page_bytes', bytecap)
local kp, kc = pair('keycap', 'memtx', 'memtx')
c:eval([[
    local p, c = ... box.space[p]:truncate() box.space[c]:truncate()
    box.atomic(function() for i = 1, 4095 do box.space[p]:insert{42, i, 0, box.NULL, 1} end end)
]], {kp, kc})
rowcap = cfg('scan_page_rows', 5000)
r = read(reads(kp, kc))
check('4096 total actual prefixes fit exactly', complete(r) and #r[2][2][1] == 4095, r[1])
c:call('tarn_autocommit', {{{'insert', kp, {42, 4096, 0, box.NULL, 1}}}})
r = read(reads(kp, kc))
check('the next distinct prefix exceeds the shared key cap', declined(r), r[1])
cfg('scan_page_rows', rowcap)

print('the route is checked after a yielding multi-table read')
assert(c:call('tarn_buckets')[2] == 0, 'join test must run before bucket tests')
c:call('tarn_bucket_init', {1, {{1, 1}}})
local epoch = c:call('tarn_buckets')[3]
hook('hold')
f = c:call('tarn_select_join', {rs, true, false, {e = epoch}}, {is_async = true})
hook('reached')
c:eval([[local e = ... box.space._tarn_meta:replace{'buckets', {count = 1, epoch = e + 1}}
          box.cfg{read_only = true} box.cfg{read_only = false}]], {epoch})
hook('release')
r = result(f)
check('a changed route refuses the whole reply', r[1] == 'moved' and r[2] == 1213, r)
c:eval([[box.space._tarn_buckets:truncate() box.space._tarn_meta:delete{'buckets'}
          box.cfg{read_only = true} box.cfg{read_only = false}]])
check('the isolated test restores the absent map', c:call('tarn_buckets')[2] == 0)
for _, name in ipairs(tables) do c:call('tarn_drop_table', {name}) end
print(string.format('\n%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
