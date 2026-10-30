-- Lazy-index storage. Logical publishers call capture inside their existing
-- row transaction; physical backfill/import never call it.
local fiber, clock = require('fiber'), require('clock')
local mp, vb, digest = require('msgpack'), require('varbinary'), require('digest')
local ffi = require('ffi')
local M = {}
local MAX_SEQ, BATCH, BYTES = 9007199254740991, 128, 1024 * 1024
local RC = {txn_isolation = 'read-confirmed'}
local MAX_U64 = ffi.new('uint64_t', MAX_SEQ)
local MAX_I64 = ffi.cast('int64_t', MAX_U64)
local function sequence(v)
    if type(v) == 'cdata' then
        if ffi.istype('uint64_t', v) then
            if v > MAX_U64 then return nil end
        elseif ffi.istype('int64_t', v) then
            if v < 0LL or v > MAX_I64 then return nil end
        else return nil end
        v = tonumber(v) -- only after the exact cdata range comparison
    end
    if type(v) ~= 'number' or v < 0 or v > MAX_SEQ or v % 1 ~= 0 then return nil end
    return v
end
local function bin(x) return vb.new(mp.encode(x)) end
local function isnull(x) return type(x) == 'nil' or x == box.NULL end
local function values(t, fields)
    local out = {}
    for i, f in ipairs(fields) do local v = t[f]; out[i] = type(v) == 'nil' and box.NULL or v end
    return out
end
local function bad(code, message) error(box.error.new{code = code, reason = message}) end

function M.new(c)
    local L = {stale = true, sources = {}, paused = false, inflight = 0, uncertain = 0, wake = fiber.cond()}
    local q = require('tarn_lazy_meter').new{cfg = c.cfg, atomic = c.atomic, cut = c.quota_cut,
        notify = function() L.wake:signal() end}
    L.quota = q -- native regression probes inspect readiness, never mutate totals
    function L.quota_invalidate()
        L.quota_marked = false
        q.invalidate()
        c.quota_mark()
        L.quota_marked = true
    end
    local function internal(name, format, indexes)
        local s = box.space[name]
        if s == nil then
            local id = box.schema.SYSTEM_ID_MAX + 1
            while id < 10000 and box.space[id] ~= nil do id = id + 1 end
            if id >= 10000 then error('tarn: no free internal lazy metadata ID') end
            s = box.schema.space.create(name, {id = id, is_sync = c.cfg.sync, format = format})
        end
        for _, ix in ipairs(indexes) do
            s:create_index(ix[1], {parts = ix[2], unique = ix[3] ~= false, if_not_exists = true})
        end
        return s
    end
    function L.schema()
        internal('_tarn_lazy_defs', {{'gen', 'unsigned'}, {'base_id', 'unsigned'}, {'ver', 'unsigned'},
                 {'phase', 'string'}, {'owner', 'unsigned'}, {'spec', 'map'},
                 {'cursor', 'any', is_nullable = true}, {'done', 'boolean'}}, {{'pk', {'gen'}}})
        internal('_tarn_lazy_capture', {{'base_id', 'unsigned'}, {'ver', 'unsigned'}, {'specs', 'array'}},
                 {{'pk', {'base_id'}}})
        internal('_tarn_lazy_counter', {{'bucket_id', 'unsigned'}, {'seq', 'unsigned'}},
                 {{'pk', {'bucket_id'}}, {'bucket_id', {'bucket_id'}, false}})
        internal('_tarn_lazy_version', {{'base_id', 'unsigned'}, {'pk', 'varbinary'},
                 {'bucket_id', 'unsigned'}, {'seq', 'unsigned'}},
                 {{'pk', {'base_id', 'pk'}}, {'bucket_id', {'bucket_id'}, false}})
        internal('_tarn_lazy_outbox', {{'id', 'varbinary'}, {'bucket_id', 'unsigned'}, {'seq', 'unsigned'},
                 {'gen', 'unsigned'}, {'assignment', 'array'}, {'ts', 'double'}, {'bytes', 'unsigned'}},
                 {{'pk', {'id'}}, {'pending', {'bucket_id', 'seq', 'id'}, false},
                  {'generation', {'gen'}, false}, {'oldest', {'ts', 'id'}, false}})
        internal('_tarn_lazy_usage', {{'id', 'unsigned'}, {'entries', 'unsigned'}, {'bytes', 'unsigned'},
                 {'reserved_entries', 'unsigned'}, {'reserved_bytes', 'unsigned'}}, {{'pk', {'id'}}})
        internal('_tarn_lazy_slots', {{'bucket_id', 'unsigned'}, {'reserved', 'unsigned'}},
                 {{'pk', {'bucket_id'}}, {'bucket_id', {'bucket_id'}, false}})
        internal('_tarn_lazy_reservation', {{'txid', 'string'}, {'n', 'unsigned'}, {'bucket_id', 'unsigned'},
                 {'base_id', 'unsigned'}, {'plan', 'array'}, {'entries', 'unsigned'}, {'bytes', 'unsigned'}},
                 {{'pk', {'txid', 'n'}}, {'bucket_id', {'bucket_id'}, false}})
        internal('_tarn_lazy_floor', {{'gen', 'unsigned'}, {'bucket_id', 'unsigned'},
                 {'source', 'unsigned'}, {'seq', 'unsigned'}},
                 {{'pk', {'gen', 'bucket_id', 'source'}}, {'bucket_id', {'bucket_id'}, false}})
        if box.space._tarn_lazy_usage:get{0} == nil then box.space._tarn_lazy_usage:insert{0, 0, 0, 0, 0} end
    end
    function L.invalidate()
        L.stale = true
        if c.exclude then c.exclude(nil, true) end
    end
    function L.fresh()
        if not L.stale then return end
        local f = fiber.new(function()
            local sources, excluded = {}, {}
            box.begin{txn_isolation = 'read-confirmed'}
            local ok, err = pcall(function()
            for _, t in box.space._tarn_catalog:pairs() do
                local sp = box.space[t.name]
                if sp ~= nil then
                    sources[sp.id] = {ver = t.def.ver or 0, specs = {}, declared = t.def.lazy or {}}
                    sources[sp.id].requires = #sources[sp.id].declared > 0
                end
            end
            for _, t in box.space._tarn_lazy_capture:pairs() do
                local source = sources[t.base_id]
                if source ~= nil then
                    if source.ver ~= t.ver and (#source.declared > 0 or #t.specs > 0) then
                        error('lazy capture/catalog version mismatch')
                    end
                    source.specs = t.specs
                    if #t.specs > 0 then excluded[t.base_id] = true end
                end
            end
            for _, source in pairs(sources) do
                if #source.declared > 0 then
                    if #source.specs ~= #source.declared then error('missing durable lazy capture descriptor') end
                    local gens = {}
                    for _, s in ipairs(source.specs) do gens[s.gen] = true end
                    for _, s in ipairs(source.declared) do
                        if not gens[s.gen] then error('stale durable lazy capture descriptor') end
                    end
                end
            end
            -- Pending descriptors exclude native writes before publication.
            for _, t in box.space._tarn_lazy_defs:pairs() do
                if t.phase ~= 'retired' then
                    excluded[t.base_id] = true
                    if sources[t.base_id] == nil then sources[t.base_id] = {ver = -1, specs = {}, declared = {}} end
                    sources[t.base_id].requires = true
                end
            end
            end)
            if box.is_in_txn() then box.rollback() end
            if not ok then error(err) end
            -- Publish the native exclusion before admitting Lua work. If its
            -- allocation fails, the old cache remains fail-closed.
            if c.exclude then c.exclude(excluded, false) end
            L.sources, L.stale = sources, false
        end)
        f:set_joinable(true)
        local ok, err = f:join()
        if not ok then error(err) end
    end
    -- A request-local negative proof, restricted by the caller to an
    -- already-marked READ_CONFIRMED CommitLocal stream. The original
    -- transaction tracks every catalog/capture lookup and the full defs
    -- scan: an applicable publication or phantom must still conflict with
    -- its user writes. This never makes the global registry/native bitmap
    -- fresh, and is not a certificate for any active or pending capture.
    local plain_tag = {}
    local function plain_check(proof, rows, plans)
        if type(proof) ~= 'table' or proof.tag ~= plain_tag or c.plain_stream == nil
           or c.plain_stream() ~= proof.txid or #rows ~= proof.count
           or (not plans and rows ~= proof.writes) then
            bad(1412, 'Lazy plain-source proof is no longer valid')
        end
        for i = 1, proof.count do
            local row = rows[i]
            local sid = proof.ids[i]
            local source = proof.sources[sid]
            if type(row) ~= 'table' then bad(1412, 'Lazy plain-source proof is no longer valid') end
            local actual
            if plans then actual = row[1] else actual = box.space[row[2]] end
            if source == nil or actual ~= source.space or box.space[sid] ~= source.space
               or source.space.name ~= source.name or (not plans and row[2] ~= sid) then
                bad(1412, 'Lazy plain-source proof is no longer valid')
            end
        end
    end
    function L.prove_plain(writes, fence, original)
        -- No RPC argument can opt into this path: CommitLocal captures the
        -- entry transaction, whose first-read mark implies confirmed
        -- visibility under the supported Lua/native stream constructors.
        if c.plain_stream == nil or c.plain_stream() ~= original or original == nil then
            bad(1412, 'Lazy plain-source proof lost its stream transaction')
        end
        if #writes == 0 or isnull(fence) then return nil end
        local versions = {}
        for i = 1, #fence, 2 do versions[fence[i]] = fence[i + 1] end
        local proof = {tag = plain_tag, txid = original, writes = writes, fence = fence,
                       count = #writes, ids = {}, sources = {}}
        for i, w in ipairs(writes) do
            local sid, sp = w[2], box.space[w[2]]
            if sp == nil then return nil end
            proof.ids[i] = sid
            if proof.sources[sid] == nil then
                local name = sp.name
                local catalog = box.space._tarn_catalog:get{name}
                if catalog == nil or catalog.def.id ~= sid or catalog.def.name ~= name
                   or (catalog.def.ver or 0) ~= versions[sid] then return nil end
                local declared = catalog.def.lazy
                if declared ~= nil and (type(declared) ~= 'table' or next(declared) ~= nil) then return nil end
                local capture = box.space._tarn_lazy_capture:get{sid}
                if capture ~= nil and next(capture.specs) ~= nil then return nil end
                proof.sources[sid] = {space = sp, name = name, ver = versions[sid]}
            end
        end
        -- There is no base_id index. Keep this full scan in the same
        -- transaction so inserts/replacements for any written SID are
        -- tracked. Unrelated lazy DDL can conservatively conflict too.
        for _, d in box.space._tarn_lazy_defs:pairs() do
            if proof.sources[d.base_id] ~= nil and d.phase ~= 'retired' then return nil end
        end
        plain_check(proof, writes, false)
        return proof
    end
    function L.guard(writes, fence, plans)
        if plans and writes.lazy_plain ~= nil then
            local proof = writes.lazy_plain
            plain_check(proof, writes, true)
            if fence ~= proof.fence then bad(1412, 'Lazy plain-source proof is no longer valid') end
            local versions = {}
            for i = 1, #fence, 2 do versions[fence[i]] = fence[i + 1] end
            for sid, source in pairs(proof.sources) do
                if versions[sid] ~= source.ver then bad(1412, 'Lazy plain-source proof is no longer valid') end
            end
            return nil
        end
        if L.stale then return {'error', 1412, 'Lazy capture registry is stale'} end
        if isnull(fence) then fence = {} end
        local versions = {}
        for i = 1, #(fence or {}), 2 do versions[fence[i]] = fence[i + 1] end
        for _, w in ipairs(writes) do
            local sid = plans and w[1].id or w[2]
            local source = L.sources[sid]
            if source ~= nil and (#source.specs > 0 or source.requires) and versions[sid] ~= source.ver then
                return {'error', 1412, 'Lazy capture requires the active local table definition'}
            end
        end
        return nil
    end
    function L.target_space(s)
        local sp = box.space[s.gen]
        if sp ~= nil and sp.name ~= '_tarn_lix_' .. s.gen then bad(1412, 'Lazy target ID is already in use') end
        local fmt = {{'identity', 'varbinary'}, {'pkid', 'varbinary'}, {'raw_pk', 'array'},
                     {'source', 'unsigned'}, {'seq', 'unsigned'}, {'live', 'boolean'}, {'bucket_id', 'unsigned'}}
        local parts = {{field = 6, type = 'boolean'}}
        for i, ty in ipairs(s.key_types) do
            fmt[#fmt + 1] = {name = 'k' .. i, type = ty, is_nullable = true}
            parts[#parts + 1] = {field = #fmt, type = ty, is_nullable = true}
        end
        for i, ty in ipairs(s.pk_types) do
            fmt[#fmt + 1] = {name = 'p' .. i, type = ty}
            parts[#parts + 1] = {field = #fmt, type = ty}
        end
        if sp == nil then
            sp = box.schema.space.create('_tarn_lix_' .. s.gen, {id = s.gen,
                engine = s.engine or 'memtx', is_sync = c.cfg.sync, format = fmt})
            c.hook('lazy_target_space')
        end
        -- Native schema creation is not one transaction. A retry repairs
        -- every index after a crash at any point before descriptor install.
        sp:create_index('primary', {parts = {{field = 1, type = 'varbinary'}}, if_not_exists = true})
        sp:create_index('ordered', {parts = parts, unique = false, if_not_exists = true})
        sp:create_index('bucket_id', {parts = {{field = 7, type = 'unsigned'}}, unique = false, if_not_exists = true})
        sp:create_index('source', {parts = {{field = 4, type = 'unsigned'}, {field = 7, type = 'unsigned'},
                                           {field = 5, type = 'unsigned'}}, unique = false, if_not_exists = true})
        sp:create_index('tombstones', {parts = {{6, 'boolean'}, {7, 'unsigned'}, {4, 'unsigned'},
            {5, 'unsigned'}, {1, 'varbinary'}}, unique = false, if_not_exists = true})
        return sp
    end
    function L.needs_bootstrap(def)
        for _, s in ipairs((def and def.lazy) or {}) do
            local d = box.space._tarn_lazy_defs:get{s.gen}
            if d == nil or d.phase ~= 'ready' then return true end
        end
        return false
    end
    local function valid_spec(s)
        if type(s) ~= 'table' or sequence(s.gen) == nil or s.gen < 10000
           or sequence(s.base_space_id) == nil or s.base_space_id < 10000
           or s.gen == s.base_space_id or sequence(s.route_parts) == nil or s.route_parts < 1 or s.route_parts > 16
           or sequence(s.local_index_id) == nil or sequence(s.bucket_field) == nil or s.bucket_field < 1 then return false end
        for _, fields in ipairs({s.key_fields, s.pk_fields, s.pk_native_fields}) do
            if type(fields) ~= 'table' or #fields == 0 then return false end
            for _, f in ipairs(fields) do if sequence(f) == nil or f < 1 then return false end end
        end
        if s.route_parts > #s.key_fields then return false end
        if type(s.key_types) ~= 'table' or type(s.pk_types) ~= 'table' or #s.key_fields ~= #s.key_types
           or #s.pk_fields ~= #s.pk_native_fields or #s.pk_fields ~= #s.pk_types then return false end
        for _, types in ipairs({s.key_types, s.pk_types}) do
            for _, ty in ipairs(types) do
                if type(ty) ~= 'string' or ty == 'number' or ty == 'double' or ty == 'decimal' then return false end
            end
        end
        return true
    end
    local function spec_identity(s)
        return mp.encode({s.gen, s.base_space_id, s.local_index_id, s.route_parts, s.key_fields,
                          s.key_types, s.pk_fields, s.pk_native_fields, s.pk_types, s.bucket_field})
    end
    function L.export_meta(tables, schema_version)
        local active, retired = {}, {}
        for _, t in ipairs(tables) do
            for _, spec in ipairs(t.def.lazy or {}) do
                local d = box.space._tarn_lazy_defs:get{spec.gen}
                if d == nil or d.base_id ~= t.def.id or d.phase ~= 'ready' or not d.done then
                    bad(1412, 'Committed lazy generation is not ready for schema export')
                end
                active[#active + 1] = spec
            end
        end
        for _, d in box.space._tarn_lazy_defs:pairs() do
            if d.phase == 'retired' then retired[#retired + 1] = d:totable() end
        end
        if #active == 0 and #retired == 0 then return nil end
        local version = 1
        for _, s in ipairs(active) do if s.route_parts > 1 then version = 2 end end
        for _, d in ipairs(retired) do if d[6].route_parts > 1 then version = 2 end end
        return {lazy_v1 = {version = version, active = active, retired = retired}, schema_version = schema_version}
    end
    function L.import_envelope(export, token)
        if type(export) ~= 'table' then bad(1210, 'Malformed schema import envelope') end
        local tagged = type(export[1]) == 'table' and export[1].lazy_v1 ~= nil
        local meta, tables, expected, seen, active = tagged and export[1].lazy_v1 or nil, {}, {}, {}, {}
        if tagged and (type(meta) ~= 'table' or (meta.version ~= 1 and meta.version ~= 2) or token ~= meta.version
           or type(meta.active) ~= 'table' or type(meta.retired) ~= 'table'
           or sequence(export[1].schema_version) == nil) then bad(1412, 'Lazy schema import requires matching coordinator capability') end
        -- Validate the entire envelope before creating any native space.
        for i, t in ipairs(export) do
            if not (tagged and i == 1) then
                if type(t) ~= 'table' or type(t.name) ~= 'string' or type(t.def) ~= 'table'
                   or type(t.space) ~= 'table' or (t.def.id ~= nil and t.space.id ~= t.def.id)
                   or (t.def.id == nil and #(t.def.lazy or {}) > 0) or t.space.name ~= t.name
                   or t.def.name ~= t.name or type(t.space.format) ~= 'table' or type(t.space.indexes) ~= 'table'
                   or type(t.uix or {}) ~= 'table' or seen[t.name] then bad(1210, 'Malformed imported table') end
                seen[t.name] = true
                for _, u in ipairs(t.uix or {}) do
                    if type(u) ~= 'table' or type(u.name) ~= 'string' or sequence(u.id) == nil
                       or type(u.format) ~= 'table' or type(u.indexes) ~= 'table' then bad(1210, 'Malformed imported global index') end
                end
                for _, spec in ipairs(t.def.lazy or {}) do
                    if not tagged or not valid_spec(spec) or (spec.route_parts > 1 and meta.version ~= 2) or spec.base_space_id ~= t.def.id or expected[spec.gen] then
                        bad(1412, 'Unbound lazy schema generation')
                    end
                    expected[spec.gen] = spec_identity(spec)
                end
                tables[#tables + 1] = t
            end
        end
        if tagged then
            for _, spec in ipairs(meta.active) do
                if not valid_spec(spec) or (spec.route_parts > 1 and meta.version ~= 2) or expected[spec.gen] ~= spec_identity(spec) or active[spec.gen] then
                    bad(1210, 'Malformed active lazy import descriptor')
                end
                local old = box.space._tarn_lazy_defs:get{spec.gen}
                local sp = box.space[spec.gen]
                if (old ~= nil and (old.phase == 'retired' or spec_identity(old.spec) ~= spec_identity(spec)))
                   or (sp ~= nil and sp.name ~= '_tarn_lix_' .. spec.gen) then
                    bad(1412, 'Imported lazy generation conflicts with local state')
                end
                active[spec.gen] = true
            end
            for gen in pairs(expected) do if not active[gen] then bad(1210, 'Missing active lazy import descriptor') end end
            local retired = {}
            for _, d in ipairs(meta.retired) do
                if type(d) ~= 'table' or #d ~= 8 or d[4] ~= 'retired' or not valid_spec(d[6])
                   or (d[6].route_parts > 1 and meta.version ~= 2) or d[1] ~= d[6].gen or d[2] ~= d[6].base_space_id or active[d[1]] or retired[d[1]]
                   or sequence(d[3]) == nil or sequence(d[5]) == nil or type(d[8]) ~= 'boolean' then
                    bad(1210, 'Malformed retired lazy import descriptor')
                end
                local old = box.space._tarn_lazy_defs:get{d[1]}
                if old ~= nil and old.phase ~= 'retired' then bad(1412, 'Imported retirement conflicts with live state') end
                retired[d[1]] = true
            end
        end
        return tables, meta
    end
    function L.import_retired(meta)
        if meta == nil then return end
        L.invalidate()
        for _, d in ipairs(meta.retired) do
            local old = box.space._tarn_lazy_defs:get{d[1]}
            if old ~= nil and old.phase ~= 'retired' then bad(1412, 'Imported retirement conflicts with a live generation') end
            box.space._tarn_lazy_defs:replace(d)
        end
    end
    function L.import_active(def)
        L.prepare(def, 0)
        box.begin(c.rw)
        for _, spec in ipairs(def.lazy or {}) do
            box.space._tarn_lazy_defs:update(spec.gen, {{'=', 4, 'ready'}, {'=', 8, true}})
        end
        L.activate(def)
        box.space._tarn_catalog:replace{def.name, def}
        box.commit()
    end
    function L.prepare(def, owner)
        L.invalidate()
        for _, original in ipairs(def.lazy or {}) do
            if not valid_spec(original) then bad(1210, 'Malformed lazy index descriptor') end
            local s = {}
            for k, v in pairs(original) do s[k] = v end
            s.engine = def.engine or 'memtx'
            if s.base_space_id ~= def.id or s.gen < 10000 then bad(1210, 'Invalid lazy source/generation') end
            local old = box.space._tarn_lazy_defs:get{s.gen}
            if old ~= nil and (old.base_id ~= def.id or old.phase == 'retired'
               or (old.phase ~= 'ready' and old.owner > (owner or 0))) then
                bad(1412, 'Stale lazy generation owner')
            end
            if old ~= nil and old.phase == 'ready' and box.space[s.gen] == nil then
                bad(1412, 'Ready lazy target is missing')
            end
            L.target_space(s)
            if old == nil then
                box.space._tarn_lazy_defs:insert{s.gen, def.id, def.ver or 0, 'building', owner or 0, s, box.NULL, false}
            elseif spec_identity(old.spec) ~= spec_identity(s) then
                bad(1412, 'Lazy generation descriptor changed without rotation')
            elseif old.phase ~= 'ready' then
                box.space._tarn_lazy_defs:update(s.gen, {{'=', 5, owner or 0}})
            end
        end
    end
    function L.ddl_guard(op, name, a)
        local sp = box.space[name]
        local sid = sp and sp.id or (a.def and a.def.id) or a.id
        local required, current = false, {}
        if sid ~= nil then
            local catalog = sp and box.space._tarn_catalog:get{sp.name}
            for _, spec in ipairs(catalog and catalog.def.lazy or {}) do required, current[spec.gen] = true, true end
            local capture = box.space._tarn_lazy_capture:get{sid}
            for _, s in ipairs(capture and capture.specs or {}) do required, current[s.gen] = true, true end
            for _, d in box.space._tarn_lazy_defs:pairs() do
                if d.base_id == sid and d.phase ~= 'retired' then required = true end
            end
        end
        if required then
            if op == 'drop' then
                if type(a.lazy_drop) ~= 'table' then return {'error', 1412, 'Lazy-aware DROP arguments required'} end
            elseif type(a.def) ~= 'table' or type(a.def.lazy) ~= 'table' then
                return {'error', 1412, 'Lazy-aware table definition required'}
            end
            local keep, dropped = {}, {}
            for _, s in ipairs(a.def and a.def.lazy or {}) do keep[s.gen] = true end
            for _, gen in ipairs(a.lazy_drop or {}) do dropped[gen] = true end
            for gen in pairs(current) do
                if not keep[gen] and not dropped[gen] then return {'error', 1412, 'Explicit lazy retirement required'} end
            end
        end
        if op == 'apply' or op == 'create_apply' or op == 'truncate_apply' then
            for _, s in ipairs(a.def and a.def.lazy or {}) do
                local d = box.space._tarn_lazy_defs:get{s.gen}
                if d == nil or d.phase ~= 'ready' or not d.done then return {'error', 1412, 'Lazy bootstrap is not ready'} end
            end
        end
        return nil
    end
    function L.retire(generations)
        L.invalidate()
        for _, gen in ipairs(generations or {}) do
            local d = box.space._tarn_lazy_defs:get{gen}
            if d ~= nil then box.space._tarn_lazy_defs:update(gen, {{'=', 4, 'retired'}}) end
        end
    end
    function L.abort(def)
        if def == nil then return end
        local published, retire = {}, {}
        local cap = box.space._tarn_lazy_capture:get{def.id}
        for _, spec in ipairs(cap and cap.specs or {}) do published[spec.gen] = true end
        for _, spec in ipairs(def.lazy or {}) do
            if not published[spec.gen] then
                if not valid_spec(spec) or spec.base_space_id ~= def.id then bad(1210, 'Invalid aborted lazy generation') end
                if box.space._tarn_lazy_defs:get{spec.gen} == nil then
                    -- Native target creation can crash before its descriptor
                    -- exists; ABORT still needs a durable retirement fence.
                    box.space._tarn_lazy_defs:insert{spec.gen, def.id, def.ver or 0, 'retired', 0, spec, box.NULL, false}
                end
                retire[#retire + 1] = spec.gen
            end
        end
        L.retire(retire)
    end
    function L.clear_versions(sid)
        -- Native clear has already run; its durable hold/fence stays live
        -- until this bounded forward-only cleanup and publication finish.
        while true do
            local ok, result = pcall(function()
                box.begin(c.rw)
                local versions, keys = box.space._tarn_lazy_version, {}
                for _, v in versions:pairs({sid}, {iterator = 'EQ'}) do
                    keys[#keys + 1] = v.pk
                    if #keys >= BATCH then break end
                end
                if #keys == 0 then box.rollback(); return false end
                for _, key in ipairs(keys) do
                    local old = versions:delete{sid, key}
                    c.bk.note(versions, old, nil)
                end
                c.writable()
                box.commit()
                return true
            end)
            if box.is_in_txn() then box.rollback() end
            if not ok then error(result) end
            if not result then return end
            c.hook('lazy_clear_versions')
        end
    end
    function L.activate(def)
        -- Caller publishes the catalog in THIS transaction.
        L.invalidate()
        if def.id == nil then return end -- legacy non-lazy native definitions
        if #(def.lazy or {}) == 0 then box.space._tarn_lazy_capture:delete{def.id}
        else box.space._tarn_lazy_capture:replace{def.id, def.ver or 0, def.lazy} end
    end
    function L.bucket(s, key)
        if c.bk.count == 0 then bad(1210, 'Lazy indexes require an initialized bucket map') end
        local crc = digest.crc32.new()
        for i = 1, s.route_parts do
            local v, ty = key[i], s.key_types[i]
            if isnull(v) then crc:update(mp.encode(box.NULL))
            elseif ty == 'string' or ty == 'varbinary' then crc:update(tostring(v))
            else crc:update(mp.encode(v)) end
        end
        return tonumber(crc:result()) % c.bk.count + 1
    end
    local function enqueue(a, promised)
        local out = box.space._tarn_lazy_outbox
        if out:get{a[1]} ~= nil then return end
        local bytes = #mp.encode(a)
        if bytes > BYTES - 1024 then bad(1041, 'Lazy assignment exceeds the packet bound') end
        q.change(1, bytes, 0, 0, promised)
        out:insert{a[1], a[3], a[4], a[2], a, clock.realtime(), bytes}
    end
    function L.bootstrap(name, a, owner)
        c.bk.fresh()
        local guard = L.ddl_guard('bootstrap', name, a)
        if guard then return guard end
        c.writable()
        local sp = box.space[a.def.id]
        if sp == nil then return {'error', 1412, 'Lazy source disappeared'} end
        local ok, result = pcall(function()
            -- Confirmed row/version view and durable cursor advance share
            -- one transaction; the DDL fence prevents new source writes.
            box.begin{txn_isolation = 'read-confirmed'}
            for _, spec in ipairs(a.def.lazy or {}) do
                local d = box.space._tarn_lazy_defs:get{spec.gen}
                if d == nil or d.phase == 'retired' or (d.phase ~= 'ready' and d.owner ~= (owner or 0)) then
                    bad(1412, 'Stale lazy bootstrap owner')
                end
                if not d.done then
                    if a.lazy_empty then
                        box.space._tarn_lazy_defs:update(spec.gen, {{'=', 8, true}})
                    else
                        local opts, n, bytes, cursor, complete = {iterator = 'ALL'}, 0, 0, d.cursor, true
                        if not isnull(cursor) then opts.after = cursor end
                        for _, row in sp.index[0]:pairs({}, opts) do
                            local pk, raw = values(row, spec.pk_native_fields), values(row, spec.pk_fields)
                            local version = box.space._tarn_lazy_version:get{sp.id, bin(pk)}
                            local seq = version and sequence(version.seq) or 0
                            local key, bucket = values(row, spec.key_fields), row[spec.bucket_field]
                            local event = {bin({0, spec.gen, key, pk}), spec.gen, bucket, seq,
                                           L.bucket(spec, key), key, pk, raw, true, true}
                            local size = #mp.encode(event)
                            if size > BYTES - 1024 then bad(1041, 'Lazy baseline exceeds packet bound') end
                            if n > 0 and (n >= BATCH or bytes + size > BYTES - 1024) then complete = false; break end
                            enqueue(event)
                            cursor, n, bytes = sp.index[0]:tuple_pos(row), n + 1, bytes + size
                        end
                        box.space._tarn_lazy_defs:update(spec.gen, {{'=', 7, cursor or box.NULL}, {'=', 8, complete}})
                    end
                    c.hook('lazy_bootstrap')
                    -- MVCC validates this owner read if takeover/retirement
                    -- committed while scanning vinyl or the hook yielded.
                    local current = box.space._tarn_lazy_defs:get{spec.gen}
                    if current.owner ~= (owner or 0) or current.phase ~= 'building' then
                        bad(1412, 'Lazy bootstrap ownership changed')
                    end
                    box.commit()
                    return {'ok', false}
                end
            end
            box.rollback()
            return {'ok', true}
        end)
        if box.is_in_txn() then box.rollback() end
        return ok and result or c.failure(result)
    end
    function L.bootstrap_status(_, a, owner)
        c.writable()
        box.begin{txn_isolation = 'read-confirmed'}
        local ok, result = pcall(function()
            local done, pending = true, 0
            for _, s in ipairs(a.def.lazy or {}) do
                local d = box.space._tarn_lazy_defs:get{s.gen}
                if d == nil or d.phase == 'retired' or (d.phase ~= 'ready' and d.owner ~= (owner or 0)) then
                    bad(1412, 'Stale lazy bootstrap status')
                end
                done = done and d.done
                pending = pending + box.space._tarn_lazy_outbox.index.generation:count{s.gen}
            end
            return {'ok', done, pending}
        end)
        if box.is_in_txn() then box.rollback() end
        return ok and result or c.failure(result)
    end
    function L.ready(name, a, owner)
        c.writable()
        local ok, result = pcall(function()
            local status = L.bootstrap_status(name, a, owner)
            -- status has its own read view; start the publication transaction
            -- only afterwards and re-read every durable descriptor.
            if status[1] ~= 'ok' or not status[2] or status[3] ~= 0 then return {'error', 1412, 'Lazy bootstrap is incomplete'} end
            box.begin(c.rw)
            for _, s in ipairs(a.def.lazy or {}) do
                local d = box.space._tarn_lazy_defs:get{s.gen}
                if d == nil or not d.done or d.phase == 'retired' or (d.phase ~= 'ready' and d.owner ~= (owner or 0))
                   or box.space._tarn_lazy_outbox.index.generation:count{s.gen} ~= 0 then
                    bad(1412, 'Lazy bootstrap ownership changed')
                end
                box.space._tarn_lazy_defs:update(s.gen, {{'=', 4, 'ready'}})
            end
            box.commit()
            return {'ok'}
        end)
        if box.is_in_txn() then box.rollback() end
        return ok and result or c.failure(result)
    end
    local function rowid(sid, pk) return tostring(bin({sid, pk})) end
    function L.initial(writes, target, plain)
        if plain ~= nil then plain_check(plain, writes, false); return {} end
        local out = {}
        for _, w in ipairs(writes) do
            local source, sp = L.sources[w[2]], box.space[w[2]]
            if source ~= nil and #source.specs > 0 and sp ~= nil then
                local pk = target(sp, w)
                local id = rowid(sp.id, pk)
                if out[id] == nil then out[id] = {sp:get(pk) or false} end
            end
        end
        return out
    end
    local function rowplans(plans, allow_plain)
        if plans.lazy_plain ~= nil then
            -- A CommitLocal proof cannot stand in for durable PREPARE
            -- reservations or be reinterpreted against a newer cache.
            if not allow_plain then bad(1412, 'Lazy plain-source proof is not a reservation') end
            plain_check(plans.lazy_plain, plans, true)
            return {}
        end
        if L.stale then bad(1412, 'Lazy capture registry is stale') end
        local rows, order, out = {}, {}, {}
        for _, p in ipairs(plans) do
            local source = L.sources[p[1].id]
            if source ~= nil and #source.specs > 0 and not p[6] then
                local id = rowid(p[1].id, p[2])
                if rows[id] == nil then
                    local initial = plans.lazy_initial and plans.lazy_initial[id]
                    rows[id] = {sp = p[1], pk = p[2], old = initial and initial[1] or p[3],
                                new = p[4], source = source}
                    if initial and type(initial[1]) == 'boolean' then rows[id].old = nil end
                    order[#order + 1] = id
                else rows[id].new = p[4] end
            end
        end
        for _, id in ipairs(order) do
            local r = rows[id]
            -- Local write_steps may retry unique conflicts out of packet order.
            -- The first image was read before any physical rewrite; read the
            -- final image after all writes. PREPARE has not changed user rows.
            if plans.lazy_initial then r.new = r.sp:get(r.pk) end
            local t, sample = r.new or r.old, r.source.specs[1]
            if t ~= nil then
                local memberships = {}
                for _, spec in ipairs(r.source.specs) do
                    local oldkey = r.old ~= nil and values(r.old, spec.key_fields) or nil
                    local newkey = r.new ~= nil and values(r.new, spec.key_fields) or nil
                    local same = oldkey ~= nil and newkey ~= nil and mp.encode(oldkey) == mp.encode(newkey)
                    local function member(key, row, live)
                        memberships[#memberships + 1] = {spec.gen, L.bucket(spec, key), key,
                            values(row, spec.pk_native_fields), values(row, spec.pk_fields), live}
                    end
                    if oldkey ~= nil and not same then member(oldkey, r.old, false) end
                    if newkey ~= nil then member(newkey, r.new, true) end
                end
                out[#out + 1] = {r.sp.id, t[sample.bucket_field], values(t, sample.pk_native_fields),
                                r.new ~= nil, memberships}
            end
        end
        return out
    end
    local function capacity(row)
        local counter = box.space._tarn_lazy_counter:get{row[2]}
        local slots = box.space._tarn_lazy_slots:get{row[2]}
        local n, reserved = 0, 0
        if counter ~= nil then n = sequence(counter.seq) end
        if slots ~= nil then reserved = sequence(slots.reserved) end
        if n == nil or reserved == nil or reserved >= MAX_SEQ - n then
            bad(1041, 'Lazy sequence capacity exhausted')
        end
        return n, reserved
    end
    local function assignment(row, m, seq, ordinal)
        return {bin({1, row[2], seq, ordinal}), m[1], row[2], seq, m[2], m[3], m[4], m[5], m[6], false}
    end
    local function publish(row, promised)
        local n = capacity(row)
        local seq, counters, versions = n + 1, box.space._tarn_lazy_counter, box.space._tarn_lazy_version
        local old = counters:get{row[2]}
        c.bk.note(counters, old, counters:replace{row[2], seq})
        local pk = bin(row[3])
        old = versions:get{row[1], pk}
        local new
        if row[4] then new = versions:replace{row[1], pk, row[2], seq}
        else versions:delete{row[1], pk} end
        c.bk.note(versions, old, new)
        for i, m in ipairs(row[5]) do enqueue(assignment(row, m, seq, i), promised) end
    end
    function L.capture(plans)
        for _, row in ipairs(rowplans(plans, true)) do publish(row, false) end
    end
    function L.reserve(txid, plans)
        local reservations = box.space._tarn_lazy_reservation
        if reservations:get{txid, 1} ~= nil then return nil end
        for i, row in ipairs(rowplans(plans)) do
            local _, slots = capacity(row)
            local entries, bytes = #row[5], 0
            for j, m in ipairs(row[5]) do
                local size = #mp.encode(assignment(row, m, MAX_SEQ, j))
                if size > BYTES - 1024 then bad(1041, 'Lazy assignment exceeds the packet bound') end
                bytes = bytes + size
            end
            if #mp.encode(row) > BYTES then bad(1041, 'Lazy prepared row plan exceeds the packet bound') end
            q.change(0, 0, entries, bytes)
            box.space._tarn_lazy_slots:replace{row[2], slots + 1}
            reservations:insert{txid, i, row[2], row[1], row, entries, bytes}
        end
        return nil
    end
    local function unreserve(r)
        q.change(0, 0, -r.entries, -r.bytes)
        local slots = assert(box.space._tarn_lazy_slots:get{r.bucket_id})
        if slots.reserved == 1 then box.space._tarn_lazy_slots:delete{r.bucket_id}
        else box.space._tarn_lazy_slots:update(r.bucket_id, {{'-', 2, 1}}) end
        box.space._tarn_lazy_reservation:delete{r.txid, r.n}
    end
    function L.release(txid, committed)
        local rows = box.space._tarn_lazy_reservation:select{txid}
        for _, r in ipairs(rows) do
            unreserve(r)
            -- A dropped base space orders this transaction before its DROP.
            if committed and box.space[r.base_id] ~= nil then publish(r.plan, true) end
        end
    end
    function L.frontiers(buckets, route)
        c.writable()
        local moved = c.bk.check_read(route)
        if moved then return moved end
        if type(buckets) ~= 'table' or #buckets > BATCH then return {'error', 1210, 'Invalid lazy frontier batch'} end
        local out = {}
        box.begin{txn_isolation = 'read-confirmed'}
        local ok, err = pcall(function()
            for _, b in ipairs(buckets) do
                if sequence(b) == nil or b < 1 or b > c.bk.count then bad(1210, 'Invalid lazy source bucket') end
                if c.bk.bad(b, false) then error('Lazy source bucket is not owned here') end
                local row = box.space._tarn_lazy_counter:get{b}
                local n = 0
                if row ~= nil then n = sequence(row.seq) end
                if n == nil then bad(1041, 'Lazy counter exceeds the exact supported range') end
                local first
                for _, p in box.space._tarn_lazy_outbox.index.pending:pairs({b}, {iterator = 'EQ'}) do first = p; break end
                out[#out + 1] = {b, n, first and assert(sequence(first.seq)) - 1 or n}
            end
        end)
        if box.is_in_txn() then box.rollback() end
        if not ok then return c.failure(err) end
        if c.bk.stale then return {'moved', 1213, 'Lazy source routing changed during read', box.NULL} end
        moved = c.bk.check_now(route)
        return moved or {'ok', out}
    end
    function L.apply(assignments, route)
        c.writable()
        c.bk.fresh()
        if type(assignments) ~= 'table' or #assignments == 0 or #assignments > BATCH
           or #mp.encode(assignments) > BYTES then return {'error', 1210, 'Invalid lazy assignment batch'} end
        local flight
        local ok, result = pcall(function()
            flight = c.inflight_begin()
            -- A no-write ACK must rely only on confirmed marker, floor or
            -- retirement state. RW visibility can expose a quorum-pending
            -- version that later rolls back after this read-only call ACKs.
            box.begin(RC)
            local ack, buckets, plans = {}, {}, {}
            for _, a in ipairs(assignments) do
                if type(a) ~= 'table' or #a ~= 10 or type(a[9]) ~= 'boolean' or type(a[10]) ~= 'boolean'
                   or type(a[6]) ~= 'table' or type(a[7]) ~= 'table' or type(a[8]) ~= 'table'
                   or sequence(a[4]) == nil or (not a[10] and sequence(a[4]) == 0)
                   or sequence(a[3]) == nil or a[3] < 1 or a[3] > c.bk.count
                   or sequence(a[5]) == nil or a[5] < 1 or a[5] > c.bk.count then
                    bad(1210, 'Malformed lazy assignment')
                end
                a[4] = sequence(a[4])
                local d = box.space._tarn_lazy_defs:get{a[2]}
                if d == nil then bad(1412, 'Unknown lazy generation') end
                if d.phase ~= 'retired' then
                    local s, sp = d.spec, box.space[a[2]]
                    if sp == nil or (d.phase ~= 'ready' and not a[10]) then bad(1412, 'Lazy target is not ready') end
                    if #a[6] ~= #s.key_fields or #a[7] ~= #s.pk_fields or #a[8] ~= #s.pk_fields
                       or a[5] ~= L.bucket(s, a[6]) then bad(1210, 'Lazy assignment identity/routing mismatch') end
                    buckets[#buckets + 1] = a[5]
                    local floor = box.space._tarn_lazy_floor:get{a[2], a[5], a[3]}
                    local identity = bin({a[6], a[7]})
                    local old = sp:get{identity}
                    if (floor == nil or a[4] > floor.seq) and (old == nil or a[4] > old.seq) then
                        if d.phase == 'ready' and a[10] then bad(1412, 'Late lazy baseline assignment') end
                        local row = {identity, bin(a[7]), a[8], a[3], a[4], a[9], a[5]}
                        for _, x in ipairs(a[6]) do row[#row + 1] = x end
                        for _, x in ipairs(a[7]) do row[#row + 1] = x end
                        local new = sp:replace(row)
                        c.bk.note(sp, old, new)
                        plans[#plans + 1] = {sp, {identity}, old, new, new, false}
                    end
                end
                ack[#ack + 1] = a[1]
            end
            c.inflight_writes(flight, plans)
            c.hook('lazy_apply')
            local refused = c.atomic('LazyApply', L.final_check, buckets, plans)
            if refused then box.rollback(); return refused end
            box.commit()
            return {'ok', ack}
        end)
        if box.is_in_txn() then box.rollback() end
        if flight ~= nil then c.inflight_end(flight) end
        return ok and result or c.failure(result)
    end
    local function confirmed(fn)
        box.begin{txn_isolation = 'read-confirmed'}
        local ok, result = pcall(fn)
        if box.is_in_txn() then box.rollback() end
        if not ok then error(result) end
        return result
    end
    function L.read_page(after)
        return confirmed(function()
            local out, bytes, last = {}, 0, nil
            local opts = {iterator = after ~= nil and 'GT' or 'ALL'}
            for _, p in box.space._tarn_lazy_outbox:pairs(after ~= nil and {after} or {}, opts) do
                if #out >= BATCH or bytes + p.bytes > BYTES - 1024 then break end
                out[#out + 1], bytes, last = p.assignment, bytes + p.bytes, p.id
            end
            return {rows = out, last = last}
        end)
    end
    function L.pending() return L.read_page(nil).rows end
    function L.ack(ids)
        local ok, result = pcall(function()
            box.begin(c.rw)
            for _, id in ipairs(ids) do
                local p = box.space._tarn_lazy_outbox:get{id}
                if p ~= nil then
                    q.change(-1, -p.bytes, 0, 0)
                    box.space._tarn_lazy_outbox:delete{id}
                end
            end
            c.writable()
            box.commit()
        end)
        if box.is_in_txn() then box.rollback() end
        if not ok then error(result) end
    end
    function L.shards()
        local ids = {[c.cfg.shard_id] = true}
        for id in pairs(c.cfg.peers) do ids[tonumber(id)] = true end
        for _, m in ipairs(c.bk.members.shards or {}) do
            if m.state ~= 'removed' then ids[m.id] = true else ids[m.id] = nil end
        end
        local out = {}
        for id in pairs(ids) do out[#out + 1] = id end
        table.sort(out)
        return out
    end
    function L.owners(buckets)
        local now, out, missing = clock.monotonic(), {}, {}
        local cache = L.owner_cache
        -- A local handoff advances ver without changing membership or the
        -- leadership generation. Discard hints as soon as that map changes.
        if cache == nil or cache.until_at <= now or cache.mver ~= c.bk.mver or cache.gen ~= c.bk.gen
           or cache.bucket_ver ~= c.bk.ver then
            cache = {rows = {}, n = 0, until_at = now + .25, mver = c.bk.mver, gen = c.bk.gen,
                     bucket_ver = c.bk.ver}
            L.owner_cache = cache
        end
        for _, bucket in ipairs(buckets) do
            local owner = cache.rows[bucket]
            if owner ~= nil then out[bucket] = owner else missing[bucket] = true end
        end
        if next(missing) == nil then return out end
        -- Discover only requested buckets, keeping both the request page
        -- and the retained cache bounded independently of cluster size.
        for _, id in ipairs(L.shards()) do
            local ok, r = pcall(c.call, id, 'tarn_buckets')
            if ok and type(r) == 'table' and r[1] == 'ok' and r[4] ~= nil then
                for _, span in ipairs(r[4]) do
                    for bucket in pairs(missing) do
                        if bucket >= span[1] and bucket <= span[2] then out[bucket] = id end
                    end
                end
            end
        end
        for bucket in pairs(missing) do
            if out[bucket] ~= nil then
                if cache.n >= 4096 then cache.rows, cache.n = {}, 0 end
                if cache.rows[bucket] == nil then cache.n = cache.n + 1 end
                cache.rows[bucket] = out[bucket]
            end
        end
        return out
    end
    function L.owner(bucket)
        local id = L.owners({bucket})[bucket]
        if id == nil then error('no current lazy bucket owner') end
        return id
    end
    function L.ship_once()
        c.writable()
        c.bk.fresh()
        local page = L.read_page(L.ship_cursor)
        if #page.rows == 0 and L.ship_cursor ~= nil then page = L.read_page(nil) end
        L.ship_cursor = page.last
        if #page.rows == 0 then return false end
        local wanted = {}
        for _, a in ipairs(page.rows) do wanted[#wanted + 1] = a[5] end
        local owners, groups, order = L.owners(wanted), {}, {}
        for _, a in ipairs(page.rows) do
            local owner = owners[a[5]]
            if owner ~= nil then
                if groups[owner] == nil then groups[owner] = {batch = {}, route = {}}; order[#order + 1] = owner end
                local g = groups[owner]
                g.batch[#g.batch + 1], g.route[#g.route + 1] = a, a[5]
            end
        end
        local progress, uncertain = false, false
        for _, bucket in ipairs(wanted) do if owners[bucket] == nil then uncertain = true end end
        for _, owner in ipairs(order) do
            if L.paused then break end
            local g = groups[owner]
            local function send()
                local r = c.call(owner, 'tarn_lazy_apply', g.batch, g.route)
                if type(r) ~= 'table' or r[1] ~= 'ok' or type(r[2]) ~= 'table' or #r[2] ~= #g.batch then
                    error('lazy target did not durably acknowledge the exact batch')
                end
                for i, id in ipairs(r[2]) do
                    if tostring(id) ~= tostring(g.batch[i][1]) then error('lazy acknowledgement identity mismatch') end
                end
                L.ack(r[2])
            end
            L.inflight = L.inflight + 1
            local ok = pcall(send)
            L.inflight = L.inflight - 1
            if ok then progress = true
            else
                uncertain = true
                local cache = L.owner_cache
                for _, bucket in ipairs(g.route) do
                    if cache ~= nil and cache.rows[bucket] ~= nil then cache.rows[bucket], cache.n = nil, cache.n - 1 end
                end
            end
        end
        L.uncertain = uncertain and 1 or 0
        return progress
    end
    require('jit').off(L.ship_once)
    local function worker_tick()
        if not L.quota_marked then c.quota_mark(); L.quota_marked = true end
        if box.space._tarn_lazy_outbox ~= nil and box.space._tarn_lazy_reservation ~= nil then q.rebuild() end
        if not L.paused and not box.info.ro and box.space._tarn_lazy_defs ~= nil
           and box.space._tarn_lazy_defs:len() > 0 then
            local ok, progress = pcall(L.ship_once) -- exact entries survive every failure
            if (L.next_gc or 0) <= clock.monotonic() then
                L.next_gc = clock.monotonic() + .1
                pcall(L.collect_once)
            end
            return ok and progress
        end
    end
    local function worker_loop()
        fiber.name('tarn_lazy')
        while true do
            -- Include idle metadata access in the protected body: the heap
            -- watchdog can recover after a transient allocation failure.
            local ok, progress = pcall(worker_tick)
            if ok and progress then fiber.yield() else L.wake:wait(0.02) end
        end
    end
    require('jit').off(worker_loop)
    function L.start()
        if L.worker ~= nil and L.worker:status() ~= 'dead' then return end
        L.worker = fiber.create(worker_loop)
    end
    function L.select(gen, key, iterator, limit, after, route)
        c.writable()
        local moved = c.bk.check_read(route)
        if moved then return moved end
        if type(key) ~= 'table' or (iterator ~= box.index.EQ and iterator ~= box.index.REQ
           and iterator ~= 'EQ' and iterator ~= 'REQ') or type(limit) ~= 'number'
           or limit < 1 or limit > BATCH or limit % 1 ~= 0 then
            return {'error', 1210, 'Malformed lazy index page'}
        end
        local ok, result = pcall(function()
            return confirmed(function()
                local d = box.space._tarn_lazy_defs:get{gen}
                if d == nil or d.phase ~= 'ready' then bad(1412, 'Lazy index is not ready') end
                if #key < 1 or #key > #d.spec.key_fields then bad(1210, 'Invalid lazy prefix') end
                if #key >= d.spec.route_parts then
                    local bucket = L.bucket(d.spec, key)
                    if c.bk.bad(bucket, false) then return c.bk.moved(bucket) end
                elseif type(route) ~= 'table' or type(route.e) ~= 'number' or type(route.m) ~= 'number' then
                    bad(1210, 'Composite lazy prefix requires an epoch and membership fence')
                end
                local sp, prefix, rows, bytes = box.space[gen], {true}, {}, 0
                for _, x in ipairs(key) do prefix[#prefix + 1] = x end
                local opts = {iterator = iterator}
                if not isnull(after) then opts.after = after end
                local next, cut, visited = box.NULL, false, 0
                for _, t in sp.index.ordered:pairs(prefix, opts) do
                    -- A receiving or previously sent copy is not an owner.
                    -- Bound inspected tuples too: stale copies may fill a page.
                    visited = visited + 1
                    if not c.bk.bad(t.bucket_id, false) then
                        local k = {}
                        for i = 1, #d.spec.key_fields do k[i] = t[7 + i] end
                        local candidate = {k, t.raw_pk, t.pkid, t.source, t.seq}
                        local n = #mp.encode(candidate)
                        if n > BYTES - 8192 then bad(1041, 'Lazy candidate exceeds page limit') end
                        if #rows > 0 and bytes + n > BYTES - 8192 then cut = true; break end
                        rows[#rows + 1], bytes = candidate, bytes + n
                    end
                    next = sp.index.ordered:tuple_pos(t)
                    if visited >= limit then cut = true; break end
                end
                return {'ok', gen, rows, cut and next or box.NULL}
            end)
        end)
        if not ok then return c.failure(result) end
        if c.bk.stale then return {'moved', 1213, 'Lazy routing changed during read', box.NULL} end
        moved = c.bk.check_now(route)
        return moved or result
    end
    function L.bucket_obligations(bucket)
        return confirmed(function()
            return box.space._tarn_lazy_outbox.index.pending:count{bucket}
                 + box.space._tarn_lazy_reservation.index.bucket_id:count{bucket}
        end)
    end
    function L.flush(bucket, deadline)
        while L.bucket_obligations(bucket) > 0 do
            if clock.monotonic() >= deadline then return false end
            if not L.paused then pcall(L.ship_once) end
            fiber.sleep(0.01)
        end
        return true
    end
    function L.obligations()
        return confirmed(function()
            return box.space._tarn_lazy_outbox:len() + box.space._tarn_lazy_reservation:len()
        end) + L.inflight
    end
    local auxiliary_names = {_tarn_lazy_counter = true, _tarn_lazy_version = true, _tarn_lazy_floor = true}
    function L.wire_space(sid)
        local sp = box.space[sid]
        return sp ~= nil and auxiliary_names[sp.name] and sp.name or sid
    end
    function L.resolve_space(wire)
        if type(wire) ~= 'string' then return wire end
        if not auxiliary_names[wire] then bad(1210, 'Unknown lazy auxiliary bucket space') end
        local sp = box.space[wire]
        if sp == nil then bad(1412, 'Missing lazy auxiliary bucket space') end
        return sp.id
    end
    function L.move_ready(to)
        if box.space._tarn_lazy_defs:len() == 0 and box.space._tarn_lazy_counter:len() == 0
           and box.space._tarn_lazy_floor:len() == 0 then return nil end
        local ok, cap = pcall(c.call, to, 'tarn_lazy_capabilities')
        if not ok or type(cap) ~= 'table' or cap[1] ~= 'ok' or cap[2] ~= 1
           or type(cap[3]) ~= 'table' or cap[3].aux_names ~= 1 then
            return {'error', 1412, 'Bucket destination lacks lazy auxiliary-name transport capability'}
        end
        for _, d in box.space._tarn_lazy_defs:pairs() do
            if d.spec.route_parts > 1 and (cap[3].route_parts or 1) < d.spec.route_parts then
                return {'error', 1412, 'Bucket destination lacks composite lazy routing capability'}
            end
        end
        return nil
    end
    function L.import(sp, tuple)
        if sp.name == '_tarn_lazy_floor' then
            local old = sp:get{tuple[1], tuple[2], tuple[3]}
            if old ~= nil and old.seq > tuple[4] then return old end
        end
        return sp:replace(tuple)
    end
    function L.spaces()
        local out = {}
        for _, name in ipairs({'_tarn_lazy_counter', '_tarn_lazy_version', '_tarn_lazy_floor'}) do
            if box.space[name] ~= nil then out[#out + 1] = box.space[name].id end
        end
        for _, d in box.space._tarn_lazy_defs:pairs() do
            if d.phase ~= 'retired' and box.space[d.gen] ~= nil then out[#out + 1] = d.gen end
        end
        return out
    end
    function L.gc(gen, bucket, route)
        c.writable()
        c.bk.fresh()
        local moved = c.bk.check_read(route)
        if moved then return moved end
        local ok, result = pcall(function()
            local sources = confirmed(function()
                local d = box.space._tarn_lazy_defs:get{gen}
                if d == nil or d.phase ~= 'ready' or box.space[gen] == nil then bad(1412, 'Lazy index is not ready') end
                local ids, out, n = {}, {}, 0
                for _, t in box.space[gen].index.tombstones:pairs({false, bucket}, {iterator = 'EQ'}) do
                    if not ids[t.source] then ids[t.source] = true; out[#out + 1] = t.source end
                    n = n + 1
                    if n >= BATCH then break end
                end
                return out
            end)
            local owners, proofs = L.owners(sources), {}
            for _, source in ipairs(sources) do
                local owner = owners[source]
                if owner == nil then error('no current lazy source owner') end
                local r = c.call(owner, 'tarn_lazy_frontiers', {source}, {source})
                if type(r) ~= 'table' or r[1] ~= 'ok' or type(r[2]) ~= 'table' or #r[2] ~= 1
                   or #r[2][1] ~= 3 or r[2][1][1] ~= source then error('invalid lazy completion proof') end
                local f = r[2][1][3]
                if f ~= -1 and sequence(f) == nil then error('invalid lazy completed frontier') end
                if f >= 0 then proofs[#proofs + 1] = {source, f} end
            end
            local flight
            local wrote, answer = pcall(function()
                flight = c.inflight_begin()
                -- Never skip persisting a proven floor because a higher
                -- but unconfirmed floor appears to cover these deletions.
                box.begin(RC)
                local d = box.space._tarn_lazy_defs:get{gen}
                if d == nil or d.phase ~= 'ready' then bad(1412, 'Lazy generation changed during collection') end
                local sp, plans, removed = box.space[gen], {}, 0
                for _, proof in ipairs(proofs) do
                    local source, f = proof[1], proof[2]
                    local floors = box.space._tarn_lazy_floor
                    local old = floors:get{gen, bucket, source}
                    if old == nil or f > old.seq then
                        local new = floors:replace{gen, bucket, source, f}
                        c.bk.note(floors, old, new)
                        plans[#plans + 1] = {floors, {gen, bucket, source}, old, new, new, false}
                    end
                    local keys = {}
                    for _, t in sp.index.tombstones:pairs({false, bucket, source}, {iterator = 'EQ'}) do
                        if t.seq > f or removed + #keys >= BATCH then break end
                        keys[#keys + 1] = t.identity
                    end
                    for _, key in ipairs(keys) do
                        local t = sp:delete{key}
                        c.bk.note(sp, t, nil)
                        plans[#plans + 1] = {sp, {key}, t, nil, nil, false}
                        removed = removed + 1
                    end
                end
                c.inflight_writes(flight, plans)
                c.hook('lazy_gc')
                local refused = c.atomic('LazyGC', L.final_check, {bucket}, plans)
                if refused then box.rollback(); return refused end
                box.commit()
                return {'ok', removed}
            end)
            if box.is_in_txn() then box.rollback() end
            if flight ~= nil then c.inflight_end(flight) end
            if not wrote then error(answer) end
            return answer
        end)
        return ok and result or c.failure(result)
    end
    function L.collect_once()
        for _, d in box.space._tarn_lazy_defs:pairs() do
            local sp = box.space[d.gen]
            if d.phase == 'retired' and sp ~= nil then sp:drop(); return end
            if d.phase == 'ready' and sp ~= nil then
                local t = sp.index.tombstones:min{false}
                if t ~= nil and not t.live and not c.bk.bad(t.bucket_id, true) then
                    L.gc(d.gen, t.bucket_id, {t.bucket_id})
                    return
                end
            end
        end
    end
    function L.stats()
        local u = q.snapshot()
        if u == nil then return {'error', 1213, 'Lazy capacity statistics are rebuilding'} end
        local first = box.space._tarn_lazy_outbox.index.oldest:min()
        return {'ok', {depth = u.entries, bytes = u.bytes, reserved_entries = u.reserved_entries,
                reserved_bytes = u.reserved_bytes, oldest_age = first and math.max(0, clock.realtime() - first.ts) or 0,
                inflight = L.inflight, paused = L.paused, uncertain = L.uncertain}}
    end
    function L.control(action, arg)
        if not c.cfg.test_hooks then return {'error', 1210, 'Lazy test controls are disabled'} end
        if action == 'pause' then
            L.paused = true
            local deadline = clock.monotonic() + 3
            while L.inflight > 0 and clock.monotonic() < deadline do fiber.sleep(0.01) end
        elseif action == 'resume' then L.paused = false; L.wake:signal()
        elseif action == 'stale_routes' then c.bk.reset(); return {'ok'}
        elseif action == 'pending' then return {'ok', L.pending()}
        elseif action == 'ack' then L.ack(arg); return {'ok'}
        elseif action == 'register' then
            if box.space[arg.id]:len() ~= 0 then return {'error', 1210, 'Test registration requires an empty base'} end
            L.prepare(arg, 0)
            box.begin(c.rw)
            for _, s in ipairs(arg.lazy) do box.space._tarn_lazy_defs:update(s.gen, {{'=', 4, 'ready'}, {'=', 8, true}}) end
            L.activate(arg)
            box.space._tarn_catalog:replace{arg.name, arg}
            box.commit()
        else return {'error', 1210, 'Unknown lazy test control'} end
        return {'ok', L.paused, L.inflight, L.uncertain}
    end
    L.capabilities = function() return {'ok', 1, {max_entries = BATCH, max_bytes = BYTES, max_seq = MAX_SEQ, aux_names = 1, route_parts = 16}} end
    function L.final_check(buckets, plans)
        c.writable()
        if c.bk.stale then return {'moved', 1213, 'Lazy ownership cache became stale'} end
        return c.bk.commit_check(buckets, nil, nil, false, plans)
    end
    return L
end
return M
