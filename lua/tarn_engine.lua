-- Online physical-engine replacement. EngineCopy/EngineCutover model the
-- protocol; the source remains authoritative until a confirmed job decision.
local fiber, clock, mp, uuid = require('fiber'), require('clock'), require('msgpack'), require('uuid')
local M = {}
local RC = {txn_isolation = 'read-confirmed'}
local function fail(code, text) error(box.error.new{code = code, reason = text}) end
local function nilish(x) return x == nil or x == box.NULL end
local function terminal(p) return p == 'published' or p == 'aborted' end
local function key(sid) return 'engine:' .. sid end

function M.new(c)
    local E = {stale = true, epoch = 0, capture_epoch = 0, tracks = {}, triggers = {}, records = {},
        excluded = {}, by_space = {}, missing_ids = {}}
    local mark
    local q = require('tarn_lazy_meter').new{cfg = c.cfg, atomic = c.atomic,
        scan = function() return {0, 0, 0, 0} end,
        cut = function() return mark ~= nil and c.cut(mark) end}
    E.tickets = q -- zero accounting, actual native transaction terminal tickets
    local function invalidate_tickets()
        q.invalidate()
        mark = nil -- allocation failure while taking the new cut stays UNKNOWN
        mark = c.mark()
    end
    invalidate_tickets()
    local function read(fn)
        box.begin(RC)
        local ok, result = pcall(fn)
        if box.is_in_txn() then box.rollback() end
        if not ok then error(result) end
        return result
    end
    local function record(sid)
        local t = box.space._tarn_meta:get{key(sid)}
        return t and t.value or nil
    end
    local function bound(a)
        local x = type(a) == 'table' and a.engine_change
        if type(x) ~= 'table' or type(a.def) ~= 'table'
           or type(x.old_id) ~= 'number' or type(a.def.id) ~= 'number'
           or x.old_id == a.def.id or type(x.old_name) ~= 'string'
           or (x.old_engine ~= 'memtx' and x.old_engine ~= 'vinyl')
           or (a.def.engine ~= 'memtx' and a.def.engine ~= 'vinyl')
           or x.old_engine == a.def.engine or a.def.name ~= x.old_name then
            fail(1210, 'Malformed engine-change descriptor')
        end
        return x
    end
    local function group(r, old)
        local out = {old and r.old_id or r.new_id}
        for _, sid in ipairs(old and r.old_aux or r.new_aux or {}) do out[#out + 1] = sid end
        return out
    end
    local function owned(a, gen, phases)
        local x = bound(a)
        local r = record(x.old_id)
        if r == nil or r.old_id ~= x.old_id or r.new_id ~= a.def.id or r.ver ~= a.ver
           or r.old_name ~= x.old_name or r.old_ver ~= x.old_ver or r.gen ~= gen
           or (phases and not phases[r.phase]) then fail(1412, 'Engine-change ownership changed') end
        return r
    end
    local function transaction(fn, check)
        if not q.ready() then fail(1213, 'Engine-change transaction cut is rebuilding') end
        local epoch = q.epoch
        box.begin(RC)
        local ok, result = pcall(function()
            -- Register before every shadow/cursor write, even though this
            -- instance of the shared meter accounts for zero resource bytes.
            q.change(0, 0, 0, 0, true)
            local answer = fn()
            c.atomic('EngineWrite', function()
                c.writable()
                if q.epoch ~= epoch or c.bk.stale then fail(1213, 'Engine-change epoch changed') end
                if check then check() end
            end)
            box.commit()
            return answer
        end)
        if box.is_in_txn() then box.rollback() end
        if not ok then error(result) end
        return result
    end
    local function cut(deadline)
        -- A caller exiting, or releasing its mutex after a detached sync
        -- wait, is not proof that its native transaction has terminated.
        invalidate_tickets()
        repeat
            c.writable(); c.bk.fresh()
            if q.rebuild() then return end
            if clock.monotonic() >= deadline then fail(1205, 'Engine-change transactions have not drained') end
            fiber.sleep(.01)
        until false
    end
    local function ready(deadline)
        if not q.ready() then cut(deadline) end
    end
    local function snapshot(sp)
        local indexes = {}
        for id, ix in pairs(sp.index) do
            if type(id) == 'number' then
                local parts = {}
                for _, p in ipairs(ix.parts) do
                    parts[#parts + 1] = {field = p.fieldno, type = p.type, is_nullable = p.is_nullable,
                        collation = p.collation, path = p.path}
                end
                if ix.type:lower() ~= 'tree' then fail(1235, 'Engine changes require TREE indexes') end
                indexes[#indexes + 1] = {id = ix.id, name = ix.name, type = ix.type:lower(),
                    unique = ix.unique, parts = parts}
            end
        end
        table.sort(indexes, function(a, b) return a.id < b.id end)
        if #indexes == 0 or indexes[1].id ~= 0 then fail(1412, 'Engine source has no primary index') end
        return {format = sp:format(), indexes = indexes}
    end
    local function capture_valid(t)
        return t ~= nil and t.valid and t.epoch == E.capture_epoch
    end
    local function capture_body(t, sid, old, new)
        local kd = c.key_def(sid, 0)
        for _, row in ipairs({old or false, new or false}) do
            if row then
                local pk = kd:extract_key(row):totable()
                local encoded = mp.encode(pk)
                if t.keys[encoded] == nil then
                    local limit = c.cfg.engine_dirty_keys or 65536
                    local bytes = c.cfg.engine_dirty_bytes or 8 * 1024 * 1024
                    if #t.list >= limit or t.bytes + #encoded > bytes then t.valid = false; return end
                    t.keys[encoded] = true
                    t.list[#t.list + 1] = pk
                    t.bytes = t.bytes + #encoded
                end
            end
        end
    end
    local function capture(sid, old, new)
        local t = E.tracks[sid]
        if not capture_valid(t) then return end
        -- A failing allocation may already have changed keys but not list
        -- (or vice versa). Aborting this source statement is insufficient:
        -- no later writer may trust that partially updated volatile set.
        local ok, err = pcall(capture_body, t, sid, old, new)
        if not ok then t.valid = false; error(err) end
    end
    local function attach(sid)
        local sp = box.space[sid]
        if sp ~= nil and E.triggers[sid] == nil then
            local fn = function(old, new) capture(sid, old, new) end
            sp:on_replace(fn)
            E.triggers[sid] = fn
        end
    end
    local function detach(sid)
        local fn, sp = E.triggers[sid], box.space[sid]
        if fn ~= nil and sp ~= nil then sp:on_replace(nil, fn) end
        E.triggers[sid], E.tracks[sid] = nil, nil
    end
    function E.invalidate(loss)
        E.epoch = E.epoch + 1
        E.stale = true
        c.exclude(nil, true)
        if loss then
            E.capture_epoch = E.capture_epoch + 1
            invalidate_tickets()
            for _, t in pairs(E.tracks) do t.valid = false end
        end
    end
    function E.fresh()
        if not E.stale then return end
        local epoch = E.epoch
        local f = fiber.new(function()
            local records = read(function()
                local out = {}
                for _, t in box.space._tarn_meta:pairs({'engine:'}, {iterator = 'GE'}) do
                    if t.key:sub(1, 7) ~= 'engine:' then break end
                    out[t.value.old_id] = t.value
                end
                return out
            end)
            local excluded, by_space, missing_ids = {}, {}, {}
            for sid, r in pairs(records) do
                if r.phase ~= 'aborted' then
                    missing_ids[sid] = true
                    for _, id in ipairs(r.old_aux or {}) do missing_ids[id] = true end
                end
                if not terminal(r.phase) then
                    excluded[sid], excluded[r.new_id] = true, true
                    by_space[sid], by_space[r.new_id] = {source = true, record = r}, {record = r}
                    for _, id in ipairs(r.old_aux or {}) do
                        excluded[id], by_space[id] = true, {source = true, record = r}
                    end
                    for _, id in ipairs(r.new_aux or {}) do
                        excluded[id], by_space[id] = true, {record = r}
                    end
                    attach(sid)
                end
            end
            c.writable()
            if epoch ~= E.epoch then fail(1213, 'Engine registry changed while rebuilding') end
            E.records, E.excluded, E.by_space, E.missing_ids, E.stale = records, excluded, by_space, missing_ids, false
            c.exclude(excluded, false)
        end)
        f:set_joinable(true)
        local ok, err = f:join()
        if not ok then error(err) end
    end
    function E.check(writes, plans)
        if E.stale then return {'error', 1412, 'Engine capture registry is stale'} end
        for _, w in ipairs(writes or {}) do
            local p = E.by_space[plans and w[1].id or w[2]]
            if p then
                if not p.source then return {'error', 1412, 'Engine replacement is not published'} end
                local phase = p.record.phase
                if phase ~= 'copying' and phase ~= 'copied' and phase ~= 'resetting' then
                    return {'error', 1412, 'Engine source is fenced'}
                end
            end
        end
    end
    function E.guard(writes) E.fresh(); return E.check(writes, false) end
    function E.missing_space(sid)
        -- Called after the ordinary entry freshness checks, including from
        -- paths which may not yield. Only our confirmed bound old IDs get
        -- a schema retry; a genuinely unknown table keeps its normal 1146.
        if E.missing_ids[sid] then
            return {'error', 1412, 'Table definition changed during engine replacement; retry transaction'}
        end
    end
    function E.translate_error(err)
        -- A vinyl reader can retain an index handle across a yield while
        -- cutover removes its space. That native error bypasses the entry
        -- lookup above. Use the typed diagnostic's identity, never its text
        -- or a generic MySQL 1146, and only IDs owned by this migration.
        if type(err) ~= 'cdata' or err.code ~= box.error.NO_SUCH_SPACE then return nil end
        local field = err.space
        local sid
        if type(field) == 'number' and field >= 0 and field % 1 == 0 then sid = field
        elseif type(field) == 'string' and field:match('^%d+$') then sid = tonumber(field) end
        if sid ~= nil and sid <= 4294967295 then return E.missing_space(sid) end
    end
    function E.rpc_errors(fn, failed)
        return function(first, ...)
            local ok, result = pcall(fn, first, ...)
            if ok then return result end
            local changed = E.translate_error(result)
            if changed then return failed and failed(first, changed) or changed end
            error(result)
        end
    end
    local function refresh() E.invalidate(false); c.refresh(); E.fresh() end
    local function replace_record(r) box.space._tarn_meta:replace{key(r.old_id), r} end
    local function schema_index(sp, ix)
        if sp.index[ix.id] == nil then
            sp:create_index(ix.name, {id = ix.id, type = ix.type, unique = ix.unique, parts = ix.parts})
        end
    end
    local function new_shadow(r)
        local sp = box.space[r.new_id]
        if sp ~= nil and (sp.name ~= r.shadow_name or sp.engine ~= r.engine) then
            fail(1412, 'Engine shadow identity changed')
        end
        if sp == nil then
            sp = box.schema.space.create(r.shadow_name, {id = r.new_id, engine = r.engine,
                is_sync = c.cfg.sync, format = r.schema.format})
        end
        require('tarn_production').readable(sp)
        schema_index(sp, r.schema.indexes[1])
        return sp
    end
    local function update(a, gen, phases, fn)
        return transaction(function()
            local r = owned(a, gen, phases)
            fn(r); replace_record(r)
            return r
        end)
    end
    local function central(name, a, gen, phase)
        local x = bound(a)
        local r = c.call(1, 'tarn_engine_decision', name, x.old_id, a.def.id, a.ver, gen)
        if type(r) ~= 'table' or r[1] ~= 'ok' or r[2] ~= phase then
            fail(1412, 'Engine change has no matching confirmed ' .. phase .. ' decision')
        end
    end
    function E.decision(name, old_id, new_id, ver, gen)
        c.writable()
        return read(function()
            local t = box.space._tarn_meta:get{'job:' .. name}
            local j = t and t.value
            local x = j and j.a and j.a.engine_change
            if j == nil or j.kind ~= 'alter' or j.space ~= name or j.ver ~= ver
               or (j.gen or 1) ~= gen or type(x) ~= 'table' or x.old_id ~= old_id
               or j.a.def.id ~= new_id then return {'error', 1412, 'Engine decision binding changed'} end
            return {'ok', j.phase}
        end)
    end
    local function check_source(a)
        local x = bound(a)
        local sp = box.space[x.old_id]
        local cat = box.space._tarn_catalog:get{x.old_name}
        if sp == nil or sp.name ~= x.old_name or sp.engine ~= x.old_engine
           or cat == nil or cat.def.id ~= x.old_id or (cat.def.ver or 0) ~= x.old_ver then
            fail(1412, 'Engine source definition changed')
        end
        return sp
    end
    local function start(name, a, gen, deadline)
        local x = bound(a)
        local r = read(function() return record(x.old_id) end)
        if r ~= nil and r.ver == a.ver and r.new_id == a.def.id then
            if r.gen > gen then fail(1412, 'Engine worker was superseded') end
            if r.phase == 'aborted' then fail(1412, 'Engine change was aborted') end
            if r.gen ~= gen then
                r = transaction(function()
                    local v = record(x.old_id)
                    if v.ver ~= a.ver or v.gen > gen then fail(1412, 'Engine worker was superseded') end
                    v.gen = gen; replace_record(v)
                    local fence = box.space._tarn_meta:get{'fence:' .. v.new_id}
                    if fence then
                        local value = fence.value; value.gen = gen
                        box.space._tarn_meta:replace{'fence:' .. v.new_id, value}
                    end
                    return v
                end)
            end
            return r
        end
        if r ~= nil and (r.ver >= a.ver or not terminal(r.phase)) then fail(1412, 'A different engine job owns the source') end
        local sp = check_source(a)
        local old_aux, new_aux, old_uix = {}, {}, {}
        for _, id in ipairs(a.uix_drop or {}) do old_aux[#old_aux + 1], old_uix[#old_uix + 1] = id, id end
        for _, id in ipairs(a.lazy_drop or {}) do old_aux[#old_aux + 1] = id end
        for _, d in ipairs(a.uix or {}) do new_aux[#new_aux + 1] = d.id end
        for _, d in ipairs(a.def.lazy or {}) do new_aux[#new_aux + 1] = d.gen end
        r = {old_id = x.old_id, old_ver = x.old_ver, old_engine = x.old_engine, old_name = name,
            new_id = a.def.id, engine = a.def.engine, ver = a.ver, gen = gen, phase = 'resetting',
            shadow_name = '_tarn_engine_' .. x.old_id .. '_' .. a.def.id,
            retired_name = '_tarn_engine_old_' .. x.old_id .. '_' .. a.def.id,
            schema = snapshot(sp), cursor = box.NULL, copied = 0, capture = '',
            old_aux = old_aux, new_aux = new_aux, old_uix = old_uix}
        transaction(function() replace_record(r) end)
        refresh(); attach(x.old_id)
        return r
    end
    local function reset(a, gen, deadline)
        local x = bound(a)
        cut(deadline)
        local t = {token = tostring(uuid.new()), valid = true, epoch = E.capture_epoch, keys = {}, list = {}, bytes = 0}
        E.tracks[x.old_id] = t; attach(x.old_id)
        local r = update(a, gen, nil, function(v)
            v.phase, v.cursor, v.copied, v.capture = 'resetting', box.NULL, 0, t.token
        end)
        local sp = box.space[r.new_id]
        if sp ~= nil then
            if sp.name ~= r.shadow_name or sp.engine ~= r.engine then fail(1412, 'Engine shadow identity changed') end
            sp:drop()
        end
        c.writable(); new_shadow(r)
        -- The pre-capture prefix may still be awaiting WAL. Those effects
        -- must settle before the first confirmed baseline page is taken.
        if not c.wait_commits(deadline) then fail(1205, 'Engine pre-capture writes have not drained') end
        update(a, gen, {resetting = true}, function(v) v.phase = 'copying' end)
        refresh()
        return t
    end
    function E.build(name, a, gen, deadline)
        -- E.body's registry refresh may have yielded since its entry cut.
        -- Reacquire only before entering a stage, never by retrying a
        -- transaction/commit that raised or may still have native effects.
        ready(deadline)
        local r = start(name, a, gen, deadline)
        local function certified(v)
            return v.phase == 'prepared' or v.phase == 'applying' or v.phase == 'published'
        end
        if certified(r) then return {'ok', 'applied'} end
        local x, restarts = bound(a), 0
        local function current(v, t, epoch)
            return q.ready() and (epoch == nil or q.epoch == epoch)
                   and capture_valid(t) and E.tracks[x.old_id] == t and t.token == v.capture
        end
        repeat
            -- A cut may be lost between successful stages. Nothing sampled
            -- below survives this wait: reread ownership, phase and capture
            -- only after the terminal-ticket/native prefix is current.
            ready(deadline)
            r = read(function() return owned(a, gen) end)
            if certified(r) then return {'ok', 'applied'} end
            if r.phase ~= 'resetting' and r.phase ~= 'copying' and r.phase ~= 'copied'
               and r.phase ~= 'preparing' then fail(1412, 'Engine copy phase changed') end
            local t = E.tracks[x.old_id]
            if r.phase == 'resetting' or not capture_valid(t) or t.token ~= r.capture then
                restarts = restarts + 1
                if restarts > 8 then fail(1205, 'Engine copy repeatedly lost its capture; retry the ALTER') end
                t = reset(a, gen, deadline)
                r = read(function() return owned(a, gen) end)
                if certified(r) then return {'ok', 'applied'} end
            end
            -- reset() can return a confirmed copying receipt after a
            -- status callback invalidated its volatile cut/capture. Do not
            -- enter even the first page transaction with that old state.
            if current(r, t) then
                if r.phase == 'copied' then return {'ok'} end
                if r.phase ~= 'copying' then fail(1412, 'Engine copy phase changed') end
                local epoch = q.epoch
                local rows, cursor = read(function()
                    local sp = check_source(a)
                    local out, bytes, last = {}, 0, r.cursor
                    local opts, from = {iterator = 'ALL'}, {}
                    if not nilish(last) then opts.iterator, from = 'GT', last end
                    for _, tuple in sp.index[0]:pairs(from, opts) do
                        if #out > 0 and (#out >= (c.cfg.engine_copy_rows or 128)
                           or bytes + tuple:bsize() > 1024 * 1024) then break end
                        out[#out + 1], bytes = tuple, bytes + tuple:bsize()
                        last = c.key_def(sp.id, 0):extract_key(tuple):totable()
                    end
                    return {out, last}
                end)
                cursor, rows = rows[2], rows[1]
                -- A confirmed Vinyl page read can yield too. If its
                -- lease/cut was lost, discard the page and cursor before
                -- re-entry; never relabel old payload under a new capture.
                if current(r, t, epoch) then
                    transaction(function()
                        local v = owned(a, gen, {copying = true})
                        if not capture_valid(t) or E.tracks[x.old_id] ~= t then fail(1213, 'Engine capture was lost') end
                        local shadow = box.space[v.new_id]
                        for _, tuple in ipairs(rows) do shadow:replace(tuple) end
                        v.cursor, v.copied = cursor, v.copied + #rows
                        if #rows == 0 then v.phase = 'copied' end
                        replace_record(v)
                    end, function() if not capture_valid(t) then fail(1213, 'Engine capture was lost') end end)
                    if #rows > 0 then c.hook('engine_copy') end
                end
            end
            fiber.sleep(0)
        until false
    end
    function E.prepare(name, a, gen, deadline)
        local r = start(name, a, gen, deadline)
        if r.phase == 'published' then return {'ok', 'applied'} end
        if r.phase == 'prepared' or r.phase == 'applying' then
            c.lazy.prepare(a.def, gen)
            return {'ok'}
        end
        if r.phase == 'preparing' and E.tracks[r.old_id] then E.tracks[r.old_id].valid = false end
        E.build(name, a, gen, deadline)
        deadline = clock.monotonic() + (a.timeout or 50)
        update(a, gen, {copied = true}, function(v)
            v.phase = 'preparing'
            for _, old in ipairs({true, false}) do
                for _, sid in ipairs(group(v, old)) do
                    box.space._tarn_meta:replace{'hold:' .. sid, {ver = a.ver, label = 'ENGINE ' .. name}}
                    box.space._tarn_meta:replace{'fence:' .. sid,
                        {ver = a.ver, gen = gen, phase = 'preparing', engine = true}}
                end
            end
        end)
        refresh()
        if not c.drain(r.old_id, deadline) then fail(1205, 'Engine source writes have not drained') end
        -- A source caller may detach while its write remains in the sync
        -- queue behind the fence. Confirm that prefix before reading payloads.
        cut(deadline)
        local t = E.tracks[r.old_id]
        if not capture_valid(t) then fail(1213, 'Engine capture was lost before the final tail') end
        for offset = 1, #t.list, 128 do
            local final = read(function()
                local out, source = {}, box.space[r.old_id]
                for i = offset, math.min(offset + 127, #t.list) do
                    local pk = t.list[i]
                    out[#out + 1] = {pk, source:get(pk) or false}
                end
                return out
            end)
            transaction(function()
                owned(a, gen, {preparing = true})
                local target = box.space[r.new_id]
                for _, entry in ipairs(final) do
                    if entry[2] then target:replace(entry[2]) else target:delete(entry[1]) end
                end
            end, function() if not capture_valid(t) then fail(1213, 'Engine capture was lost') end end)
            c.hook('engine_tail'); fiber.sleep(0)
        end
        cut(deadline)
        r = read(function() return owned(a, gen, {preparing = true}) end)
        for _, ix in ipairs(r.schema.indexes) do
            c.writable(); schema_index(box.space[r.new_id], ix)
        end
        for _, d in ipairs(a.uix or {}) do c.create_space(d) end
        c.lazy.prepare(a.def, gen)
        cut(deadline)
        update(a, gen, {preparing = true}, function(v)
            if not capture_valid(t) then fail(1213, 'Engine capture was lost') end
            v.phase = 'prepared'
            box.space._tarn_meta:replace{'fence:' .. v.new_id,
                {ver = a.ver, gen = gen, phase = 'prepared', engine = true}}
        end)
        c.hook('engine_prepared')
        return {'ok'}
    end
    local function clean_published(a, gen, r, deadline)
        if r.cleaned then return {'ok', 'applied'} end
        cut(deadline)
        for _, sid in ipairs(r.old_uix or {}) do if box.space[sid] ~= nil then box.space[sid]:drop() end end
        local old = box.space[r.old_id]
        if old ~= nil then
            if old.name ~= r.retired_name then fail(1412, 'Retired engine source identity changed') end
            old:drop()
        end
        c.lazy.clear_versions(r.old_id)
        transaction(function()
            local v = owned(a, gen, {published = true})
            for _, old in ipairs({true, false}) do
                for _, sid in ipairs(group(v, old)) do
                    local held = box.space._tarn_meta:get{'hold:' .. sid}
                    if held and held.value.ver == a.ver then box.space._tarn_meta:delete{'hold:' .. sid} end
                    if not old then
                        local f = box.space._tarn_meta:get{'fence:' .. sid}
                        if f and f.value.ver == a.ver then box.space._tarn_meta:delete{'fence:' .. sid} end
                    end
                end
            end
            v.cleaned = true; replace_record(v)
        end)
        detach(r.old_id)
        c.invalidate_defs(); refresh()
        return {'ok'}
    end
    function E.apply(name, a, gen, deadline)
        local x = bound(a)
        local r = read(function() return record(x.old_id) end)
        if r and r.ver == a.ver and r.new_id == a.def.id and r.phase == 'published' then
            -- The confirmed local publication remains a proof after the
            -- coordinator ended its job. It is not a cleanup certificate.
            return clean_published(a, r.gen, r, deadline)
        end
        central(name, a, gen, 'apply')
        r = start(name, a, gen, deadline)
        if r.phase ~= 'prepared' and r.phase ~= 'applying' then fail(1412, 'Engine replacement is not prepared') end
        local guard = c.lazy.ddl_guard('apply', name, a)
        if guard then return guard end
        update(a, gen, {prepared = true, applying = true}, function(v) v.phase = 'applying' end)
        cut(deadline)
        local alias = c.call(1, 'tarn_engine_counter_alias', x.old_id, a.def.id)
        if alias[1] ~= 'ok' then fail(1412, 'Engine counter alias is not confirmed') end
        local old, target = box.space[r.old_id], box.space[r.new_id]
        if target == nil or target.engine ~= r.engine then fail(1412, 'Engine replacement disappeared') end
        if old ~= nil and old.name == r.old_name then old:rename(r.retired_name) end
        if target.name == r.shadow_name then target:rename(r.old_name) end
        if target.name ~= r.old_name then fail(1412, 'Engine replacement name changed') end
        for _, sid in ipairs(group(r, true)) do c.doom(sid) end
        c.hook('engine_publish')
        transaction(function()
            local v = owned(a, gen, {applying = true})
            c.lazy.retire(a.lazy_drop)
            c.lazy.activate(a.def)
            box.space._tarn_lazy_capture:delete{v.old_id}
            box.space._tarn_catalog:replace{name, a.def}
            v.phase, v.cleaned = 'published', false; replace_record(v)
            for _, sid in ipairs(group(v, true)) do
                box.space._tarn_meta:replace{'fence:' .. sid,
                    {ver = a.ver, gen = gen, phase = 'migrated', engine_retired = true}}
            end
        end)
        c.hook('engine_published')
        r = read(function() return owned(a, gen, {published = true}) end)
        return clean_published(a, gen, r, deadline)
    end
    function E.abort(name, a, gen, deadline)
        local x = bound(a)
        local r = read(function() return record(x.old_id) end)
        if r and r.ver == a.ver and r.new_id == a.def.id and r.phase == 'published' then return {'ok', 'applied'} end
        if r and r.ver == a.ver and r.new_id == a.def.id and r.phase == 'aborted' then
            detach(r.old_id); refresh()
            return {'ok'}
        end
        central(name, a, gen, 'abort')
        r = start(name, a, gen, deadline)
        if r.phase == 'applying' then fail(1412, 'Engine publication already began') end
        update(a, gen, nil, function(v) v.phase = 'aborting' end)
        cut(deadline)
        local target = box.space[r.new_id]
        if target ~= nil then
            if target.name ~= r.shadow_name then fail(1412, 'Engine shadow identity changed') end
            target:drop()
        end
        for _, d in ipairs(a.uix or {}) do if box.space[d.id] ~= nil then box.space[d.id]:drop() end end
        transaction(function()
            local v = owned(a, gen, {aborting = true})
            c.lazy.abort(a.def)
            v.phase = 'aborted'; replace_record(v)
            for _, old in ipairs({true, false}) do
                for _, sid in ipairs(group(v, old)) do
                    local h = box.space._tarn_meta:get{'hold:' .. sid}
                    if h and h.value.ver == a.ver then box.space._tarn_meta:delete{'hold:' .. sid} end
                    local f = box.space._tarn_meta:get{'fence:' .. sid}
                    if f and f.value.ver == a.ver then box.space._tarn_meta:delete{'fence:' .. sid} end
                end
            end
        end)
        detach(r.old_id)
        refresh()
        return {'ok'}
    end
    function E.body(op, name, a, gen)
        c.writable(); c.bk.fresh()
        local deadline = clock.monotonic() + (a.timeout or 50)
        ready(deadline); E.fresh()
        if op == 'bootstrap' or op == 'ready' then
            read(function() owned(a, gen, {prepared = true}) end)
            return op == 'bootstrap' and c.lazy.bootstrap(name, a, gen) or c.lazy.ready(name, a, gen)
        end
        return assert(E[op])(name, a, gen, deadline)
    end
    function E.run(op, name, a, gen)
        local ok, result = pcall(function()
            if box.is_in_txn() then fail(1210, 'Engine DDL requires its own transaction') end
            local x = bound(a)
            if name ~= x.old_name then fail(1210, 'Engine request name does not match its source binding') end
            return c.serial(x.old_id, a.timeout or 50, E.body, op, name, a, gen or 0)
        end)
        return ok and result or c.failure(result)
    end
    function E.counter_name(name)
        local prefix, sid = name:match('^(%a+):(%d+)$')
        if prefix ~= 'ai' and prefix ~= 'rid' then return name end
        sid = tonumber(sid)
        local seen = {}
        for _ = 1, 32 do
            if seen[sid] then fail(1412, 'Engine counter alias cycle') end
            seen[sid] = true
            local t = box.space._tarn_meta:get{'counter_alias:' .. sid}
            if t == nil then return prefix .. ':' .. sid end
            if type(t.value) ~= 'number' or t.value < 10000 then fail(1412, 'Malformed engine counter alias') end
            sid = t.value
        end
        fail(1412, 'Engine counter alias chain is too long')
    end
    local function counter_alias_body(old_id, new_id)
        c.writable()
        box.begin(RC)
        local canonical = tonumber(E.counter_name('ai:' .. old_id):match(':(%d+)$'))
        local old = box.space._tarn_meta:get{'counter_alias:' .. new_id}
        if old then
            if old.value ~= canonical then fail(1412, 'Engine counter alias identity changed') end
            -- This immutable binding is already confirmed. Replays need no
            -- WAL write and cannot contend on an identical replacement.
            box.rollback()
            return {'ok'}
        end
        box.space._tarn_meta:replace{'counter_alias:' .. new_id, canonical}
        box.commit()
        return {'ok'}
    end
    function E.counter_alias(old_id, new_id)
        -- Every participant ensures the same central binding. Begin its
        -- confirmed view only after admission, and retain the mutex through
        -- WAL completion. A detached/uncertain outcome still needs RC proof.
        local ok, result = pcall(c.serial, new_id, 50, counter_alias_body, old_id, new_id)
        if box.is_in_txn() then box.rollback() end
        return ok and result or c.failure(result)
    end
    return E
end
return M
