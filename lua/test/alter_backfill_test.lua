-- Automatic instant-fill index materialization (issue #70).
-- Run through lua/test/run.sh on fresh, isolated shards.
local net = require('net.box')
local fiber = require('fiber')
local json = require('json')
local c = net.connect(os.getenv('S1'), {user = 'tarn', password = 'tarn', wait_connected = 5})
assert(c:is_connected(), 'cannot connect to shard')
local pass, fail, names, supported = 0, 0, {}, true
local function check(name, cond, extra)
    if cond then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. (extra and ('  -- ' .. json.encode(extra)) or '')) end
end
local function create(tag, engine, rows, extra)
    local name = 'test.bf70_' .. engine .. '_' .. tag
    local format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
                    {name = 'bucket_id', type = 'unsigned'}}
    for _, f in ipairs(extra or {}) do format[#format + 1] = f end
    local indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}},
                     {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}}}
    c:call('tarn_create_table', {{name = name, engine = engine, format = format, indexes = indexes}})
    names[#names + 1] = name
    local sid = c:eval('return box.space[...].id', {name})
    local writes = {}
    for _, row in ipairs(rows or {{1, 10, 1}, {2, 20, 1}}) do
        writes[#writes + 1] = {'insert', sid, row}
    end
    assert(#writes == 0 or c:call('tarn_autocommit', {writes})[1] == 'ok')
    return {name = name, id = sid, format = format, indexes = indexes}
end
local function add(t, ver, fill, unique)
    local format = table.deepcopy(t.format)
    format[#format + 1] = {name = 'c', type = 'integer', is_nullable = true}
    local field = #format
    local parts = {{field = field, type = 'integer', is_nullable = true}}
    local indexes = table.deepcopy(t.indexes)
    indexes[#indexes + 1] = {name = 'c_idx', unique = unique, parts = parts}
    return {ver = ver, timeout = 5, materialize = true, old_width = #t.format,
            pad = {3, fill}, checks = {{field, 'c', 'indexed', box.NULL, box.NULL, fill}},
            builds = {{2, 'c_idx', unique, parts}}, drop = {}, renames = {}, format = format,
            def = {name = t.name, id = t.id, engine = c:eval('return box.space[...].engine', {t.id}),
                   format = format, indexes = indexes, json = '{}', ver = ver}}
end
local function call(proc, t, a, gen) return c:call('tarn_alter_' .. proc, {t.name, a, gen or 1}) end
local function tuples(t)
    return c:eval('local out = {} for _, r in box.space[...]:pairs() do out[#out + 1] = r:totable() end return out', {t.id})
end
local function indexed(t, key)
    return c:eval('local sid, key = ... return box.space[sid].index[2]:count{key}', {t.id, key})
end
local function undo_count(t, ver)
    return c:eval('local sid, ver = ... return box.space._tarn_alter_undo.index[0]:count{sid, ver}', {t.id, ver})
end
local function old_write(t)
    return c:call('tarn_autocommit', {{{'update', t.id, {1}, {{'+', 2, 1}}}}, {fence = {t.id, 0}}})
end
local function hook(name, action) return c:call('tarn_test_hook', {name, action}) end
local function async(proc, t, a, gen)
    return c:call('tarn_alter_' .. proc, {t.name, a, gen or 1}, {is_async = true})
end
local function result(f)
    local ok, r, err = pcall(f.wait_result, f, 10)
    return ok and r and r[1] or {'raised', tostring(ok and err or r)}
end

print('combined ADD COLUMN DEFAULT + ADD INDEX materializes old tuples')
for _, engine in ipairs({'memtx', 'vinyl'}) do
    local t = create('combined', engine)
    local a = add(t, 1, 3, false)
    local r = call('build', t, a)
    check(engine .. ': BUILD leaves the index for fenced materialization', r[1] == 'ok'
          and c:eval('return box.space[...].index[2] == nil', {t.id}), r)
    r = call('prepare', t, a)
    check(engine .. ': combined non-NULL fill and index PREPARE succeeds', r[1] == 'ok', r)
    -- Fail promptly against the old rejection guard; later cases must not
    -- wait for new hooks that an old storage module cannot reach.
    if r[1] == 'ok' then
        local rows = tuples(t)
        check(engine .. ': PREPARE physically appends the fill before building its index',
              #rows == 2 and #rows[1] == 4 and rows[1][4] == 3
              and #rows[2] == 4 and rows[2][4] == 3 and indexed(t, 3) == 2, rows)
        r = call('apply', t, a)
        check(engine .. ': APPLY publishes the indexed values', r[1] == 'ok' and indexed(t, 3) == 2, r)
        r = call('abort', t, a)
        check(engine .. ': a late ABORT does not trim a published column', r[1] == 'ok'
              and #tuples(t)[1] == 4 and indexed(t, 3) == 2, r)
    else
        supported = false
        call('abort', t, a)
    end
end

if supported then
for _, engine in ipairs({'memtx', 'vinyl'}) do
    local function label(s) return engine .. ': ' .. s end
    do
        print(label('published instant fills and physically present values'))
        local t = create('present', engine, {{1, 10, 1}, {2, 20, 1, box.NULL}, {3, 30, 1, 7}},
                         {{name = 'c', type = 'integer', is_nullable = true}})
        local a = add(t, 1, 3, false)
        -- Index a previously committed instant column; its frozen fill is
        -- 3 even if its current SQL DEFAULT has since become 9.
        a.pad, a.format, a.def.format = {3, 3}, box.NULL, t.format
        a.checks[1][1], a.builds[1][4][1].field = 4, 4
        a.def.indexes[3].parts = a.builds[1][4]
        local r = call('prepare', t, a)
        local rows = tuples(t)
        check(label('only missing frozen fills are appended; NULL and custom values remain'),
              r[1] == 'ok' and rows[1][4] == 3 and rows[2][4] == box.NULL and rows[3][4] == 7, {r, rows})
        check(label('only the short row needs undo'), undo_count(t, 1) == 1)
        r = call('apply', t, a)
        check(label('the native index agrees with frozen and present values'),
              r[1] == 'ok' and indexed(t, 3) == 1 and indexed(t, 7) == 1 and undo_count(t, 1) == 0, r)
    end
    do
        print(label('old padding survives a simultaneous DROP and indexed ADD'))
        local t = create('overlay', engine, {{1, 10, 1}},
                         {{name = 'old', type = 'integer', is_nullable = true}})
        local a = add(t, 1, 9, false)
        a.pad = {3, 3, 9}
        a.format[4] = {name = '_dropped_3', type = 'any', is_nullable = true}
        hook('alter_backfill_chunk', 'hold')
        local f = async('prepare', t, a)
        local reached = hook('alter_backfill_chunk', 'reached')
        local rows = tuples(t)
        check(label('old-catalog projection still sees its fill while the new column is unpublished'),
              reached[2] == 1 and #rows[1] == 5 and rows[1][4] == 3 and rows[1][5] == 9, rows)
        local w = old_write(t)
        check(label('an old-layout writer is refused throughout the backfill'), w[1] == 'error' and w[2] == 1412, w)
        hook('alter_backfill_chunk', 'release')
        local r = result(f)
        check(label('the overlaid row can be indexed and published'), r[1] == 'ok' and call('apply', t, a)[1] == 'ok'
              and indexed(t, 9) == 1, r)
    end
    do
        print(label('NULL padding holes are distinct from missing descriptors'))
        local t = create('null_pad', engine, {{1, 10, 1}},
                         {{name = '_dropped_3', type = 'any', is_nullable = true}})
        local a = add(t, 1, 9, false)
        a.pad = {3, box.NULL, 9}
        local r = call('prepare', t, a)
        check(label('explicit NULL in an intervening pad slot is accepted and preserved'),
              r[1] == 'ok' and tuples(t)[1][4] == box.NULL and tuples(t)[1][5] == 9, r)
        check(label('an indexed suffix after a NULL hole publishes correctly'),
              r[1] == 'ok' and call('apply', t, a)[1] == 'ok' and indexed(t, 9) == 1)
    end
    do
        print(label('UNIQUE failure undo and later field-slot reuse'))
        local t = create('duplicate', engine)
        local a = add(t, 1, 3, true)
        local r = call('prepare', t, a)
        check(label('duplicate frozen fills reject UNIQUE with 1062'), r[1] == 'duplicate' and r[2] == 1062, r)
        local w = old_write(t)
        check(label('a failed build retains its fence and original-length journal'),
              w[1] == 'error' and w[2] == 1412 and undo_count(t, 1) == 2, w)
        r = call('abort', t, a)
        local rows = tuples(t)
        check(label('ABORT restores exact original lengths before releasing old writers'), r[1] == 'ok'
              and #rows[1] == 3 and #rows[2] == 3 and undo_count(t, 1) == 0 and old_write(t)[1] == 'ok', {r, rows})
        r = call('prepare', t, a)
        check(label('a late PREPARE cannot recreate an aborted suffix'), r[1] == 'error' and r[2] == 1412, r)
        local b = add(t, 2, 9, false)
        r = call('prepare', t, b)
        local ordinary = table.deepcopy(a)
        ordinary.materialize = false
        local old_build, old_apply = call('build', t, ordinary), call('apply', t, ordinary)
        check(label('delayed ordinary BUILD/APPLY cannot touch a newer backfill index'),
              old_build[1] == 'error' and old_build[2] == 1412
              and old_apply[1] == 'error' and old_apply[2] == 1412, {old_build, old_apply})
        check(label('a later ADD reuses the slot with its own different fill'), r[1] == 'ok'
              and call('apply', t, b)[1] == 'ok' and indexed(t, 9) == 2 and indexed(t, 3) == 0, r)
    end
    do
        print(label('retry and takeover preserve the first original length'))
        local t = create('retry', engine)
        local a = add(t, 1, 3, false)
        local r1, r2 = call('prepare', t, a, 1), call('prepare', t, a, 1)
        local r3 = call('prepare', t, a, 2)
        check(label('repeated preparation and takeover retain one original undo per row'),
              r1[1] == 'ok' and r2[1] == 'ok' and r3[1] == 'ok' and undo_count(t, 1) == 2, {r1, r2, r3})
        local stale = call('apply', t, a, 1)
        check(label('an older APPLY cannot publish after a newer preparation'), stale[1] == 'error' and stale[2] == 1412, stale)
        local r = call('abort', t, a, 2)
        local rows = tuples(t)
        check(label('undo after retries restores the first length, not a padded retry length'),
              r[1] == 'ok' and #rows[1] == 3 and #rows[2] == 3, {r, rows})
    end
    do
        print(label('ABORT waits for outstanding native index work'))
        local t = create('quiesce', engine)
        local a = add(t, 1, 3, false)
        hook('alter_backfill_index', 'hold')
        local p = async('prepare', t, a, 1)
        local reached = hook('alter_backfill_index', 'reached')
        local abort = async('abort', t, a, 2)
        fiber.sleep(0.05)
        check(label('ABORT cannot finish while an earlier PREPARE can still build an index'),
              reached[2] == 1 and not abort:is_ready())
        hook('alter_backfill_index', 'release')
        local rp, ra = result(p), result(abort)
        check(label('the released builder quiesces before ABORT removes index and suffix'),
              ra[1] == 'ok' and #tuples(t)[1] == 3
              and c:eval('return box.space[...].index[2] == nil', {t.id}), {rp, ra})
        local b = add(t, 2, 9, false)
        b.builds[1][4] = {{field = 2, type = 'integer'}}
        b.def.indexes[3].parts = b.builds[1][4]
        local r = call('prepare', t, b, 3)
        check(label('the next ALTER can reuse the index ID with different parts'), r[1] == 'ok'
              and call('apply', t, b, 3)[1] == 'ok' and indexed(t, 10) == 1 and indexed(t, 3) == 0, r)
    end
    do
        print(label('global-fill batches retain generation and phase ownership'))
        for _, point in ipairs({'uix_fill', 'uix_fill_written'}) do
            local t = create('global_' .. point, engine, {{1, 10, 1}})
            local a = add(t, 1, 3, false)
            local uid = t.id + 100000
            a.uix = {{name = '_tarn_uix_' .. t.id .. '_2', id = uid, engine = engine,
                      format = {{name = 'k', type = 'integer'}, {name = 'owner', type = 'unsigned'},
                                {name = 'bucket_id', type = 'unsigned'}},
                      indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'integer'}}},
                                 {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}}}}}
            assert(call('prepare', t, a, 1)[1] == 'ok')
            hook(point, 'hold')
            local f = c:call('tarn_uix_fill', {uid, {{3, 1, 1}}, t.id, 1, 1}, {is_async = true})
            local reached = hook(point, 'reached')
            local changed
            if point == 'uix_fill' then
                changed = call('prepare', t, a, 2)
            else
                -- This is a direct storage protocol race: the eligible
                -- fill's transaction has written but has not committed.
                changed = call('apply', t, a, 1)
            end
            hook(point, 'release')
            local r = result(f)
            check(label(point .. ': a changed owner/phase prevents a late global-index commit'),
                  reached[2] == 1 and changed[1] == 'ok'
                  and ((r[1] == 'error' and r[2] == 1412) or (r[1] == 'conflict' and r[2] == 1213))
                  and c:eval('return box.space[...]:len()', {uid}) == 0, {changed, r})
            if point == 'uix_fill' then call('abort', t, a, 2) end
        end
    end
    do
        print(label('old ALTER arguments cannot bind to a replacement table'))
        local t = create('recreated', engine)
        local a = add(t, 1, 3, false)
        c:call('tarn_drop_table', {t.name})
        local replacement = t.id + 200000
        c:call('tarn_create_table', {{name = t.name, id = replacement, engine = engine,
                                    format = t.format, indexes = t.indexes}})
        c:call('tarn_autocommit', {{{'insert', replacement, {1, 88, 1}}}})
        for _, op in ipairs({'build', 'prepare', 'apply', 'abort'}) do
            local r = call(op, t, a)
            check(label('old ' .. op .. ' is fenced by its original space identity'),
                  r[1] == 'error' and r[2] == 1412, r)
        end
        check(label('the replacement tuple, indexes and fence metadata stay untouched'), c:eval([[
            local sid = ... local s = box.space[sid] local t = s:get{1}
            return #t == 3 and t[2] == 88 and s.index[2] == nil
                   and box.space._tarn_meta:get{'fence:' .. sid} == nil
        ]], {replacement}))
    end
    do
        print(label('native DDL exceptions release the ALTER mutex'))
        local t = create('ddl_error', engine)
        local a = add(t, 1, 3, false)
        assert(call('prepare', t, a)[1] == 'ok')
        local format = a.format
        a.format = {{name = 'id', type = 'not_a_native_type'}}
        local ok = pcall(call, 'apply', t, a)
        check(label('a native format error escapes without completing publication'), not ok)
        a.format = format
        local r = call('apply', t, a)
        check(label('the valid APPLY retry acquires the released mutex and finishes'),
              r[1] == 'ok' and indexed(t, 3) == 2 and undo_count(t, 1) == 0, r)
    end
    do
        print(label('published APPLY retries clean undo without trimming rows'))
        local t = create('apply_gc', engine)
        local a = add(t, 1, 3, false)
        assert(call('prepare', t, a)[1] == 'ok')
        hook('alter_backfill_apply', 'hold')
        local f = async('apply', t, a)
        local reached = hook('alter_backfill_apply', 'reached')
        c:eval('box.cfg{read_only = true}')
        hook('alter_backfill_apply', 'release')
        local first = result(f)
        c:eval('box.cfg{read_only = false}')
        check(label('a role change after publication can leave only journal cleanup pending'),
              reached[2] == 1 and first[1] ~= 'ok' and #tuples(t)[1] == 4 and undo_count(t, 1) == 2, first)
        local r = call('apply', t, a, 2)
        local abort = call('abort', t, a, 1)
        check(label('repeated APPLY drains undo and a stale ABORT keeps committed indexed rows'),
              r[1] == 'ok' and abort[1] == 'ok' and undo_count(t, 1) == 0
              and #tuples(t)[1] == 4 and indexed(t, 3) == 2, {r, abort})
    end
end
end

for _, name in ipairs(names) do c:call('tarn_drop_table', {name}) end
print(string.format('\n%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
