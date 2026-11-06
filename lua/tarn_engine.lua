-- Online physical-engine replacement. EngineCopy/EngineCutover model the
-- protocol; the source remains authoritative until a confirmed job decision.
-- The same copy rebuilds a table in its own engine when an ALTER builds
-- secondary indexes on a replicated cluster (issue #267): the replacement
-- gets every index while it is empty, so that followers apply only DDL on
-- an empty space and ordinary batches of rows, never a long index build
-- (tla/ShadowUnique.tla for its unique indexes). When the ALTER also adds,
-- renames or drops columns, the replacement takes the new format, and the
-- copy pads each row it writes with the fills its indexes need. A rebuild
-- keeps the table's global index spaces as they are: their entries hold
-- keys and primary keys, which the copy keeps, and both definitions write
-- them. Publication only renames them after the new space's id. It keeps
-- the table's lazy indexes too, whose targets name only their generation:
-- publication binds them to the new space (L.rebind in lua/tarn_lazy.lua).
-- The lazy indexes it makes (a.lazy_made) are built online against the old
-- space before the copy, as an ALTER in place builds them, and published
-- with the kept ones. Those the ALTER drops (a.uix_drop, a.lazy_drop) it
-- retires as an engine change retires all of them (issue #184): they take
-- the table's writes until PREPARE, which holds them with the table;
-- publication dooms their readers and retires the generations, and
-- cleanup drops the global index spaces; ABORT leaves them as they were.
local fiber, clock, mp, uuid = require('fiber'), require('clock'), require('msgpack'), require('uuid')
local M = {}
local RC = {txn_isolation = 'read-confirmed'}
local function fail(code, text) error(box.error.new{code = code, reason = text}) end
local function nilish(x) return x == nil or x == box.NULL end
local function terminal(p) return p == 'published' or p == 'aborted' end
local function key(sid) return 'engine:' .. sid end

function M.new(c)
    local E = {stale = true, epoch = 0, capture_epoch = 0, tracks = {}, triggers = {}, records = {},
        excluded = {}, by_space = {}, missing_ids = {}, durable = {}}
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
        -- A rebuild keeps the engine; an engine change changes it. A rebuild
        -- is `true`; 'layout' when it also changes the stored layout; 'aux'
        -- when it keeps the table's global index spaces; 'lazy' when it keeps
        -- its lazy indexes, and global ones if any ('aux' and 'lazy' may
        -- change the layout too). Storage from before one of them refuses
        -- such a job (every step, ABORT too) rather than copy the old layout,
        -- leave the kept spaces under names it no longer finds them by, or
        -- retire the kept lazy generations as an aborted engine change's.
        if type(x) ~= 'table' or type(a.def) ~= 'table'
           or type(x.old_id) ~= 'number' or type(a.def.id) ~= 'number'
           or x.old_id == a.def.id or type(x.old_name) ~= 'string'
           or (x.old_engine ~= 'memtx' and x.old_engine ~= 'vinyl')
           or (a.def.engine ~= 'memtx' and a.def.engine ~= 'vinyl')
           or (x.rebuild ~= nil and x.rebuild ~= true and x.rebuild ~= 'layout' and x.rebuild ~= 'aux'
               and x.rebuild ~= 'lazy')
           or (x.old_engine == a.def.engine) ~= (x.rebuild ~= nil) or a.def.name ~= x.old_name
           or (x.durable ~= nil and type(x.durable) ~= 'boolean')
           or (not nilish(a.lazy_made) and (x.rebuild ~= 'lazy' or type(a.lazy_made) ~= 'table'
                                             or a.materialize == true)) then
            fail(1210, 'Malformed engine-change descriptor')
        end
        for _, gen in ipairs(type(a.lazy_made) == 'table' and a.lazy_made or {}) do
            if type(gen) ~= 'number' or gen % 1 ~= 0 or gen < 1 then fail(1210, 'Malformed engine-change descriptor') end
        end
        -- (The spaces it retires join the source's group in its record, ABORT's
        -- too: every step refuses a list, or an id in one, that is not one.)
        for _, field in ipairs({'uix_drop', 'lazy_drop', 'uix_check'}) do
            local list = a[field]
            -- (type: box.NULL == nil holds, and it is no list)
            if type(list) ~= 'nil' and type(list) ~= 'table' then fail(1210, 'Malformed engine-change descriptor') end
            for _, sid in ipairs(type(list) == 'table' and list or {}) do
                if type(sid) ~= 'number' or sid % 1 ~= 0 or sid < 1 then
                    fail(1210, 'Malformed engine-change descriptor')
                end
            end
        end
        return x
    end
    -- The lazy generations a copy makes (a.lazy_made), as a set. Their
    -- build reads the old space's rows, so a copy that materializes fields
    -- missing from them makes none (above): the SQL server builds such a
    -- change in place, where PREPARE materializes the rows before the scan.
    local function made_set(a)
        local out = {}
        for _, gen in ipairs(type(a.lazy_made) == 'table' and a.lazy_made or {}) do out[gen] = true end
        return out
    end
    local function makes(a) return next(made_set(a)) ~= nil end
    -- The lazy generations a rebuild drops (a.lazy_drop), as a set: the
    -- table's, which publication retires (issue #184; L.keep checks them).
    local function dropped_set(a)
        local out = {}
        for _, gen in ipairs(type(a.lazy_drop) == 'table' and a.lazy_drop or {}) do out[gen] = true end
        return out
    end
    -- The generations a rebuild keeps and makes (L.keep), each live one of
    -- the table's being one of those or one it drops.
    local function keep_lazy(a, x) return c.lazy.keep(a.def, x.old_id, made_set(a), dropped_set(a)) end
    -- The ALTER's arguments as the online build of its made generations
    -- takes them: in place, on the old space (L.made_def).
    local function made_args(a, x)
        local o = {}
        for k, v in pairs(a) do o[k] = v end
        o.engine_change = nil
        o.def = c.lazy.made_def(a.def, x.old_id)
        return o
    end
    -- A made generation whose online scan has not ended (the shard did not
    -- start it: an ineligible change, or a replica that does not capture
    -- building incarnations) scans the old space behind the final fence, as
    -- an ALTER in place does: after PREPARE, in batches the coordinator
    -- sends (tarn_lazy_bootstrap), within tarn.cfg.alter_check_rows, which
    -- the copy checks when it begins and again in PREPARE.
    local function scan_bound(a, x)
        local sp = box.space[x.old_id]
        if sp ~= nil and c.lazy.needs_scan(made_args(a, x).def) and sp.index[0]:len() > c.cfg.alter_check_rows then
            fail(1846, string.format('A lazy index this ALTER builds behind its fence would scan %d rows, over '
                .. 'tarn.cfg.alter_check_rows (%d)', sp.index[0]:len(), c.cfg.alter_check_rows))
        end
    end
    local function none(v) return nilish(v) or (type(v) == 'table' and next(v) == nil) end
    local function keeps_aux(x) return x.rebuild == 'aux' or x.rebuild == 'lazy' end
    -- The row checks a rebuild applies (a.checks, issue #184): a narrower
    -- integer range, fewer characters or decimal digits, or NOT NULL, as
    -- the in-place change applies them (check_value in lua/tarn.lua), with
    -- the column's fill for a row that ends before its field. ('indexed'
    -- checks are for the in-place change: the copy pads such rows.)
    local CHECKS = {notnull = 3, chars = 4, digits = 4, range = 5}
    local function row_checks(a)
        local out = {}
        for _, ch in ipairs(type(a.checks) == 'table' and a.checks or {}) do
            if type(ch) ~= 'table' or type(ch[1]) ~= 'number' or ch[1] < 1 or ch[1] % 1 ~= 0
               or type(ch[2]) ~= 'string' or (ch[3] ~= 'indexed' and CHECKS[ch[3]] == nil) then
                fail(1210, 'Malformed engine-rebuild row check')
            end
            if ch[3] ~= 'indexed' then out[#out + 1] = ch end
        end
        return out
    end
    -- What a new job's descriptor may ask of the copy, checked when the job
    -- starts its copy (not at ABORT, which must end a job however it was
    -- refused): only a 'layout', 'aux' or 'lazy' rebuild changes the stored
    -- layout (format and padding). A rebuild may check rows (row_checks),
    -- renames no table, and makes no global index. Only an 'aux' or 'lazy'
    -- rebuild has global indexes, which it keeps or drops (kept_globals),
    -- and only a 'lazy' one lazy indexes, which it keeps, makes or drops
    -- (keep_lazy). (Storage from before drops refuses a rebuild that drops
    -- one here, and the SQL server asks it first.)
    local function admissible(x, a)
        if x.rebuild ~= 'layout' and not keeps_aux(x) and (not nilish(a.format) or not nilish(a.pad)) then
            fail(1210, x.rebuild and 'A rebuild that changes the stored layout must say so'
                or 'An engine change keeps the stored layout')
        end
        if x.rebuild ~= nil then
            row_checks(a)
            if not nilish(a.rename) then fail(1210, "An engine rebuild keeps the table's name") end
            if not none(a.uix) then fail(1210, 'An engine rebuild makes no global index') end
            if not keeps_aux(x) and (not none(a.def.uix) or not none(a.uix_drop) or #c.uix_spaces(x.old_id) > 0) then
                fail(1210, 'A rebuild that keeps global indexes must say so')
            end
            if x.rebuild ~= 'lazy' and (not none(a.def.lazy) or not none(a.lazy_drop) or #c.lazy.live(x.old_id) > 0) then
                fail(1210, 'A rebuild that keeps lazy indexes must say so')
            end
        end
        -- The conflicts spaces of building global indexes that PREPARE checks
        -- (issue #292): only a rebuild that keeps the indexes has them, each
        -- one it drops.
        local drops = {}
        for _, id in ipairs(type(a.uix_drop) == 'table' and a.uix_drop or {}) do drops[id] = true end
        for _, id in ipairs(type(a.uix_check) == 'table' and a.uix_check or {}) do
            if not drops[id] or not keeps_aux(x) then fail(1210, 'A rebuild checks only conflicts it drops') end
        end
    end
    -- The global index spaces an 'aux' rebuild keeps: every one of the
    -- source's but those it drops (a.uix_drop, each one of the source's),
    -- as the new definition names them, after the new space. {id,
    -- old_name, new_name} each, which APPLY renames. The index id in a name
    -- may change: an index on a column made nullable gets a new id
    -- (alter.rs), and such a change made in place left the space under the
    -- old one. (A dropped one joins the source's group: start, old_uix.)
    local function kept_globals(x, a)
        local have, n, out, taken, dropped = {}, 0, {}, {}, {}
        for _, sp in ipairs(c.uix_spaces(x.old_id)) do have[sp.id], n = sp.name, n + 1 end
        for _, id in ipairs(type(a.uix_drop) == 'table' and a.uix_drop or {}) do
            if have[id] == nil or dropped[id] then fail(1210, 'Malformed dropped global index') end
            dropped[id], n = true, n - 1
        end
        local old_tag, new_tag = '^' .. c.uix_prefix .. x.old_id .. '_%d+$', '^' .. c.uix_prefix .. a.def.id .. '_%d+$'
        for _, u in ipairs(type(a.def.uix) == 'table' and a.def.uix or {}) do
            local name = type(u) == 'table' and not dropped[u.id] and have[u.id]
            local new = name and type(u.name) == 'string' and u.name
            local other = new and box.space[new]
            if not name or not name:match(old_tag) or not new or not new:match(new_tag) or taken[new]
               or (other ~= nil and other.id ~= u.id) then
                fail(1210, 'Malformed kept global index')
            end
            out[#out + 1], have[u.id], taken[new] = {id = u.id, old_name = name, new_name = new}, nil, true
        end
        if #out ~= n then fail(1210, 'A rebuild keeps every global index of its table it does not drop') end
        return out
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
    -- A wait's deadline: the ALTER's lock wait timeout from now. The copy
    -- and the final tail take as long as the table needs, a page or a batch
    -- per transaction, so a cut or drain after them gets a deadline of its
    -- own rather than what is left of the call's: past that, a cut that
    -- meets one commit in flight on the node fails at once (1205).
    local function within(a) return clock.monotonic() + (a.timeout or 50) end
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
    -- A rebuild's native schema: the ALTER's format if it changes one, and
    -- the source's indexes, less the ones the ALTER drops and under the
    -- names it gives, plus the ones it adds. The primary index stays, so
    -- captured keys and copied rows share one key.
    local function rebuilt(sp, a)
        local s, drop, names, ids, out = snapshot(sp), {}, {}, {}, {}
        if not nilish(a.format) then
            if type(a.format) ~= 'table' or #a.format < #s.format then
                fail(1210, 'Malformed engine-rebuild format')
            end
            s.format = a.format
        end
        for _, id in ipairs(a.drop or {}) do drop[id] = true end
        for _, rn in ipairs(a.renames or {}) do names[rn[1]] = rn[2] end
        if drop[0] or type(a.builds) ~= 'table' or #a.builds == 0 then
            fail(1210, 'Malformed engine-rebuild descriptor')
        end
        for _, ix in ipairs(s.indexes) do
            if not drop[ix.id] then
                ix.name = names[ix.id] or ix.name
                out[#out + 1], ids[ix.id] = ix, true
            end
        end
        for _, b in ipairs(a.builds) do
            if type(b) ~= 'table' or type(b[1]) ~= 'number' or b[1] < 1 or ids[b[1]]
               or type(b[2]) ~= 'string' or type(b[4]) ~= 'table' then
                fail(1210, 'Malformed engine-rebuild index')
            end
            out[#out + 1] = {id = b[1], name = b[2], type = 'tree', unique = b[3] == true, parts = b[4]}
            ids[b[1]] = true
        end
        table.sort(out, function(p, q) return p.id < q.id end)
        s.indexes = out
        return s
    end
    -- A rebuild's padding (a.pad): the number of fields every stored row
    -- has, then the fills of the fields after it, up to the new width. The
    -- copy appends to each row it writes the fields the row lacks, as a
    -- materializing PREPARE does in place (UIX.mat_rows in lua/tarn.lua): an
    -- index on such a field would otherwise hold NULL where SQL reads the
    -- field's fill.
    local function padding(a)
        local p = a.pad
        if nilish(p) then return box.NULL end
        if type(p) ~= 'table' or type(p[1]) ~= 'number' or p[1] < 1 or p[1] % 1 ~= 0 or #p < 2
           or p[1] + #p - 1 < (tonumber(a.old_width) or 0) then
            fail(1210, 'Malformed engine-rebuild padding')
        end
        for i = 2, #p do
            if type(p[i]) == 'nil' then fail(1210, 'Malformed engine-rebuild padding') end
        end
        return p
    end
    -- What the copy writes for a source row: the row, padded to the record's
    -- padding if it has one.
    local function shaper(r)
        local p = r.pad
        if nilish(p) then return function(tuple) return tuple end end
        local base, width = p[1], p[1] + #p - 1
        return function(tuple)
            local n = #tuple
            if n >= width then return tuple end
            if n < base then fail(1210, 'Stored tuple is shorter than ALTER padding base') end
            local row = tuple:totable()
            for f = n + 1, width do row[f] = p[f - base + 1] end
            return box.tuple.new(row)
        end
    end
    -- What the copy checks a source row with (row_checks): a failing row
    -- fails the change with the error the in-place change gives it, naming
    -- the row by its position in the source's primary-key order, which
    -- position() gives (only then: it may count rows). Every row of a
    -- prepared copy was checked when it was last written to the
    -- replacement: by a page, a pass or the final pass.
    local function checker(a)
        local checks = row_checks(a)
        if #checks == 0 then return function() end end
        return function(row, position)
            for _, ch in ipairs(checks) do
                local v
                if #row < ch[1] then v = ch[6] else v = row[ch[1]] end
                local code = c.check_value(ch, v)
                if code == 1265 then
                    fail(1265, string.format("Data truncated for column '%s' at row %d", ch[2], position()))
                elseif code ~= nil then
                    fail(1264, string.format("Out of range value for column '%s' at row %d", ch[2], position()))
                end
            end
        end
    end
    -- The position of a source row in the source's primary-key order.
    local function position_in(sid, row)
        return function()
            return box.space[sid].index[0]:count(c.key_def(sid, 0):extract_key(row), {iterator = 'LT'}) + 1
        end
    end
    -- Unique secondary indexes of the replacement, by id.
    local function uniques(sp)
        local out = {}
        for id, ix in pairs(sp.index) do
            if type(id) == 'number' and id ~= 0 and ix.unique then out[#out + 1] = id end
        end
        table.sort(out)
        return out
    end
    -- The replacement's row, other than tuple's own, that holds one of its
    -- unique secondary keys, and that index. A key with a NULL part is no
    -- one's: Tarantool's nullable unique indexes take any number of them.
    local function holder(sp, unique, tuple)
        for _, id in ipairs(unique) do
            local key = c.key_def(sp.id, id):extract_key(tuple)
            local null = false
            for n = 1, #key do if nilish(key[n]) then null = true; break end end
            if not null then
                local other = sp.index[id]:get(key)
                if other ~= nil and c.key_def(sp.id, 0):compare(other, tuple) ~= 0 then
                    return other, sp.index[id]
                end
            end
        end
    end
    -- MySQL's error for row `tuple` that unique index `ix` of the
    -- replacement cannot take: with the key, unless a part is a weight
    -- field (varbinary), which is not the column's value.
    local function duplicate(ix, tuple)
        local parts = {}
        for i, p in ipairs(ix.parts) do
            local v = tuple[p.fieldno]
            if nilish(v) or p.type == 'varbinary' then fail(1062, "Duplicate entry for key '" .. ix.name .. "'") end
            parts[i] = type(v) == 'cdata' and (tostring(v):gsub('U?LL$', '')) or tostring(v)
        end
        fail(1062, string.format("Duplicate entry '%s' for key '%s'", table.concat(parts, '-'), ix.name))
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
    -- A copy's durable capture (issue #184, tla/EngineCopy.tla): every key
    -- written to the source during the copy, in a space of the source's
    -- engine (so in the write's own transaction), with the generation it
    -- was written in (E.durable[sid].gen). A key is in it exactly when a
    -- committed write touched it since the capture began: a write rolled
    -- back leaves nothing, and a write that can't note its key fails. A
    -- replica's applier notes nothing; the leader's rows arrive with the
    -- write. Its holes, in a space of the shadow's engine: the keys whose
    -- copies the copy took out of the shadow by the holder rule, noted in
    -- the same transaction, with the generation then. The in-memory capture
    -- below is its mirror on this leader: the keys of the writes since the
    -- last pass's swap, noted when the statement runs, rolled back or not.
    -- A pass rereads the keys up to its generation, then records it in the
    -- job's record (settled), so the keys above it are those written since.
    -- A pass, and the final pass, clear and reread only the keys the durable
    -- capture notes above that (noted): the others' writes rolled back, and
    -- their copies are the source's rows. A BUILD that finds the mirror
    -- lost, on a new leader or after a status change, rebuilds it from the
    -- keys and holes above the settled generation (resume) instead of
    -- copying the table again.
    local function capture(sid, old, new)
        local d = E.durable[sid]
        if d ~= nil and box.session.type() ~= 'applier' then
            local sp = box.space[d.id]
            if sp ~= nil and sp.name == d.name then
                local kd, last = c.key_def(sid, 0), nil
                for _, row in ipairs({old or false, new or false}) do
                    if row then
                        local encoded = mp.encode(kd:extract_key(row):totable())
                        if encoded ~= last then
                            sp:replace{encoded, d.gen}; last = encoded
                            d.noted = d.noted + 1
                        end
                    end
                end
            end
        end
        local t = E.tracks[sid]
        if not capture_valid(t) then return end
        -- A failing allocation may already have changed keys but not list
        -- (or vice versa). Aborting this source statement is insufficient:
        -- no later writer may trust that partially updated volatile set.
        local ok, err = pcall(capture_body, t, sid, old, new)
        if not ok then t.valid = false; error(err) end
    end
    -- Whether a write to row's key began since capture t started: else the
    -- row is the source's still.
    local function captured(t, sid, row)
        return t.keys[mp.encode(c.key_def(sid, 0):extract_key(row):totable())] ~= nil
    end
    -- The durable capture's spaces are named after the capture's token: a
    -- capture an older node started (it has no such spaces) is never taken
    -- for its own.
    local DURABLE, HOLES = '_tarn_engine_capture_', '_tarn_engine_holes_'
    local function durable_name(old_id, token) return DURABLE .. old_id .. '_' .. (token:gsub('-', '')) end
    local function holes_name(old_id, token) return HOLES .. old_id .. '_' .. (token:gsub('-', '')) end
    local function durable_of(r)
        if type(r.capture) ~= 'string' or r.capture == '' then return nil end
        return box.space[durable_name(r.old_id, r.capture)], box.space[holes_name(r.old_id, r.capture)]
    end
    -- E.durable[sid]: the durable capture the writes to sid note their keys
    -- in, {id, name, gen, holes = {id, name}}, while its space is there,
    -- with the number of keys it noted since it was bound or pruned (noted).
    local function bound_durable(sid)
        local d = E.durable[sid]
        local sp = d and box.space[d.id]
        return sp ~= nil and sp.name == d.name and d or nil
    end
    local function bind_durable(sid, sp, hp)
        local d = bound_durable(sid)
        if d == nil or d.id ~= sp.id then d = {id = sp.id, name = sp.name, gen = 1, noted = 0}; E.durable[sid] = d end
        if hp ~= nil then d.holes = {id = hp.id, name = hp.name} end
        return d
    end
    -- The durable capture of capture t, and its holes, or nil.
    local function durable_for(x, t)
        local d = bound_durable(x.old_id)
        if d == nil or t == nil or d.name ~= durable_name(x.old_id, t.token) or d.holes == nil then return nil end
        local h = box.space[d.holes.id]
        if h == nil or h.name ~= d.holes.name then return nil end
        return d, box.space[d.id], h
    end
    -- A copy the holder rule takes out of the shadow, in that transaction.
    local function hole(x, t, key)
        local d, _, h = durable_for(x, t)
        if d ~= nil then h:replace{mp.encode(key), d.gen}; d.noted = d.noted + 1 end
    end
    -- Each page of a durable capture's space ({key, generation} each), read
    -- confirmed a page per read, to fn(page), until it returns false.
    local function pages(space, fn)
        local after = nil
        repeat
            local page = read(function()
                local out = {}
                local from, opts = {}, {iterator = 'ALL'}
                if after ~= nil then from, opts.iterator = {after}, 'GT' end
                for _, e in space.index[0]:pairs(from, opts) do
                    out[#out + 1] = {e[1], e[2]}
                    if #out >= 1024 then break end
                end
                return out
            end)
            if #page > 0 then after = page[#page][1] end
            if fn(page) == false then return false end
            fiber.sleep(0)
        until #page < 1024
        return true
    end
    -- The keys of list the durable capture notes above the settled
    -- generation, a key in it or a hole (all of them without one).
    local function noted(x, t, list, settled)
        local d, w, h = durable_for(x, t)
        if d == nil then return list end
        local out = {}
        for offset = 1, #list, 128 do
            read(function()
                for i = offset, math.min(offset + 127, #list) do
                    local key = mp.encode(list[i])
                    local e, o = w:get{key}, h:get{key}
                    if (e ~= nil and e[2] > settled) or (o ~= nil and o[2] > settled) then
                        out[#out + 1] = list[i]
                    end
                end
            end)
            fiber.sleep(0)
        end
        return out
    end
    -- Its id is below the SQL server's, from 10001 up: an automatic one,
    -- past the largest there is, would take the next the SQL server gives a
    -- table (as for _tarn_alter_undo in lua/tarn.lua). Synchronous, as every
    -- _tarn_ space of a replicated shard is (lua/tarn_fresh.lua).
    local function create_durable(name, engine)
        for id = box.schema.SYSTEM_ID_MAX + 1, 9999 do
            if box.space[id] == nil then
                c.writable()
                local ok, sp = pcall(box.schema.space.create, name, {id = id, engine = engine,
                    is_sync = c.cfg.sync, format = {{name = 'key', type = 'string'}, {name = 'gen', type = 'unsigned'}}})
                -- (Another space may have taken the id meanwhile.)
                if ok then
                    sp:create_index('primary', {parts = {{1, 'string'}}})
                    return sp
                end
                if box.space[id] == nil then error(sp) end
            end
        end
        fail(1041, 'No free internal space id for a copy\'s durable capture')
    end
    local function drop_durables(old_id)
        local ids = {}
        for _, prefix in ipairs({DURABLE .. old_id .. '_', HOLES .. old_id .. '_'}) do
            for _, s in box.space._space.index.name:pairs({prefix}, {iterator = 'GE'}) do
                if s.name:sub(1, #prefix) ~= prefix then break end
                ids[#ids + 1] = s.id
            end
        end
        for _, id in ipairs(ids) do
            if box.space[id] ~= nil then c.writable(); box.space[id]:drop() end
        end
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
        E.triggers[sid], E.tracks[sid], E.durable[sid] = nil, nil, nil
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
                    -- The durable capture of the record's token, bound before
                    -- a write is admitted here, unless one is bound: a reset
                    -- binds its new one before it records its token, and the
                    -- record read above may be older. The writes from now on
                    -- take a generation above the settled ones. (Only while
                    -- the copy takes writes and may resume: a copy that starts
                    -- over or aborts drops its durable capture once the writes
                    -- noting their keys there have ended, and one behind its
                    -- fence takes no writes.)
                    if r.phase == 'copying' or r.phase == 'copied' then
                        local d = bound_durable(sid)
                        if d == nil then
                            local sp, hp = durable_of(r)
                            d = sp ~= nil and bind_durable(sid, sp, hp) or nil
                        end
                        if d ~= nil then d.gen = math.max(d.gen, (r.settled or 0) + 1) end
                    end
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
    -- (c.create_index: a vinyl build on a replicated shard needs care, issue #254.)
    local function schema_index(sp, ix)
        if sp.index[ix.id] == nil then
            c.create_index(sp, ix.name, {id = ix.id, type = ix.type, unique = ix.unique, parts = ix.parts})
        end
    end
    -- The replacement gets every index while it is empty (issue #267). A
    -- follower then applies DDL on an empty space only, and the rows as
    -- ordinary batches: it never builds an index in its applier, which would
    -- acknowledge nothing meanwhile and make a leader with strict fencing
    -- resign. The final fence builds nothing either.
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
        for _, ix in ipairs(r.schema.indexes) do c.writable(); schema_index(sp, ix) end
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
    -- The engine change that job record `j` runs, and its version: an
    -- ALTER's, or the step of an online build of global unique indexes
    -- that copies the table to make them enforcing (issue #292, the second
    -- of its three steps). nil for any other job.
    local function job_step(j)
        if type(j) ~= 'table' or type(j.a) ~= 'table' then return nil end
        local a, ver = j.a, j.ver
        if j.kind == 'unique_build' then
            a = type(j.a.uix_build) == 'table' and j.a.uix_build[2] or nil
            ver = type(a) == 'table' and a.ver or nil
        elseif j.kind ~= 'alter' then
            return nil
        end
        if type(a) ~= 'table' or type(a.engine_change) ~= 'table' or type(a.def) ~= 'table' then return nil end
        return a, ver
    end
    -- The decision for its engine change that job record `j` makes in
    -- phase `phase`: 'apply' or 'abort', or nil while it makes none. An
    -- ALTER's phase is its decision; an online build decides to apply its
    -- copy in 'enforcing:apply', and to abort it in 'enforcing:abort',
    -- which holds through the steps that then drop the index
    -- ('dropping:...'). Once made, a job's decision does not change
    -- (tarn_job_phase refuses a phase that would change it).
    function E.job_decision(j, phase)
        if job_step(j) == nil or type(phase) ~= 'string' then return nil end
        if j.kind == 'alter' then return (phase == 'apply' or phase == 'abort') and phase or nil end
        if phase == 'enforcing:apply' then return 'apply' end
        if phase == 'enforcing:abort' or phase:sub(1, 9) == 'dropping:' then return 'abort' end
        return nil
    end
    function E.decision(name, old_id, new_id, ver, gen)
        c.writable()
        return read(function()
            local t = box.space._tarn_meta:get{'job:' .. name}
            local j = t and t.value
            local a, jver = job_step(j)
            if a == nil or j.space ~= name or jver ~= ver or (j.gen or 1) ~= gen
               or a.engine_change.old_id ~= old_id or a.def.id ~= new_id then
                return {'error', 1412, 'Engine decision binding changed'}
            end
            return {'ok', E.job_decision(j, j.phase) or j.phase}
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
    -- (`aborting`: ABORT records a job it finds no record of, as a
    -- tombstone that stale BUILDs meet, without the copy's schema.)
    local function start(name, a, gen, deadline, aborting)
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
        if not aborting then admissible(x, a) end
        local schema, pad, kept, kept_lazy, made_lazy = box.NULL, box.NULL, {}, {}, {}
        if not aborting then
            schema = x.rebuild ~= nil and rebuilt(sp, a) or snapshot(sp)
            pad = x.rebuild ~= nil and padding(a) or box.NULL
            if keeps_aux(x) then kept = kept_globals(x, a) end
            if x.rebuild == 'lazy' then kept_lazy, made_lazy = keep_lazy(a, x) end
            if #made_lazy > 0 then scan_bound(a, x) end
        end
        local old_aux, new_aux, old_uix = {}, {}, {}
        for _, id in ipairs(a.uix_drop or {}) do old_aux[#old_aux + 1], old_uix[#old_uix + 1] = id, id end
        for _, id in ipairs(a.lazy_drop or {}) do old_aux[#old_aux + 1] = id end
        for _, d in ipairs(a.uix or {}) do new_aux[#new_aux + 1] = d.id end
        -- (A rebuild's lazy generations take deliveries throughout: those it
        -- keeps, and those it makes, built online before the copy.)
        if x.rebuild == nil then
            for _, d in ipairs(a.def.lazy or {}) do new_aux[#new_aux + 1] = d.gen end
        end
        r = {old_id = x.old_id, old_ver = x.old_ver, old_engine = x.old_engine, old_name = name,
            new_id = a.def.id, engine = a.def.engine, ver = a.ver, gen = gen, phase = 'resetting',
            shadow_name = '_tarn_engine_' .. x.old_id .. '_' .. a.def.id,
            retired_name = '_tarn_engine_old_' .. x.old_id .. '_' .. a.def.id,
            schema = schema, rebuild = x.rebuild ~= nil, pad = pad, kept_uix = kept, kept_lazy = kept_lazy,
            made_lazy = made_lazy,
            cursor = box.NULL, copied = 0, capture = '',
            old_aux = old_aux, new_aux = new_aux, old_uix = old_uix}
        transaction(function() replace_record(r) end)
        refresh(); attach(x.old_id)
        return r
    end
    local function reset(a, gen, deadline)
        local x = bound(a)
        -- The copy starts over. Once its record says so (no step resumes it
        -- then), the writes note nothing in its durable capture, and the cut
        -- waits for those that did: none meets the drop of its spaces below,
        -- which would abort it. (Not before: a reset that failed after it
        -- would leave writes that a resume of the same capture misses.)
        ready(deadline)
        update(a, gen, nil, function(v) v.phase = 'resetting' end)
        E.durable[x.old_id] = nil
        cut(deadline)
        -- The durable capture first, empty (an earlier one's keys concern a
        -- copy that starts over), so that a write the mirror notes is in it.
        local token = tostring(uuid.new())
        drop_durables(x.old_id)
        -- (Only when the SQL server found every replica able to note its
        -- writes there: engine_change.durable.)
        if x.durable == true then
            local d = create_durable(durable_name(x.old_id, token), x.old_engine)
            bind_durable(x.old_id, d, create_durable(holes_name(x.old_id, token), a.def.engine))
        end
        local t = {token = token, valid = true, epoch = E.capture_epoch, keys = {}, list = {}, bytes = 0}
        E.tracks[x.old_id] = t; attach(x.old_id)
        local r = update(a, gen, nil, function(v)
            v.phase, v.cursor, v.copied, v.capture = 'resetting', box.NULL, 0, t.token
            v.resets, v.settled = (v.resets or 0) + 1, 0
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
    -- Keys a catch-up pass took from capture t and did not settle go back
    -- into it (they may be stale or missing in the shadow), within the
    -- capture's limits: past them the capture is lost, and the copy starts
    -- over.
    local function restore(t, list)
        if not capture_valid(t) then return end
        local limit = c.cfg.engine_dirty_keys or 65536
        local bytes = c.cfg.engine_dirty_bytes or 8 * 1024 * 1024
        for _, pk in ipairs(list) do
            local encoded = mp.encode(pk)
            if t.keys[encoded] == nil then
                if #t.list >= limit or t.bytes + #encoded > bytes then t.valid = false; return end
                t.keys[encoded] = true
                t.list[#t.list + 1] = pk
                t.bytes = t.bytes + #encoded
            end
        end
    end
    -- The durable capture's entries and holes up to the settled generation
    -- are its no longer: a resume, and a pass, skip them. Once a binding
    -- has noted more than twice the mirror's limit of keys (d.noted), a pass
    -- that settled deletes them, so that the durable capture holds about the
    -- keys of the writes since the passes before, and a resume reads that
    -- much. Each delete reads its entry first, in the delete's transaction:
    -- a write that notes the key again commits before it, and its newer
    -- entry is the one read, which stays; or after it, and puts its entry
    -- back; or the delete's transaction fails with a conflict (memtx MVCC
    -- and vinyl alike). Best effort: an entry left is one a resume skips,
    -- and the spaces go when the copy ends.
    local function prune(a, gen, x, t, settled)
        local d, w, h = durable_for(x, t)
        if d == nil or d.noted <= 2 * (c.cfg.engine_dirty_keys or 65536) then return end
        local left, failures = 0, 0
        for _, space in ipairs({w, h}) do
            -- (A page's deletes in one transaction: on a replicated shard,
            -- each transaction waits for its quorum.)
            pages(space, function(page)
                local old = {}
                for _, e in ipairs(page) do
                    if e[2] <= settled then old[#old + 1] = e[1] else left = left + 1 end
                end
                if #old == 0 then return end
                local ok = pcall(transaction, function()
                    owned(a, gen, {copying = true, copied = true})
                    for _, key in ipairs(old) do
                        local e = space:get{key}
                        if e ~= nil and e[2] <= settled then space:delete{key} end
                    end
                end)
                if not ok then
                    left, failures = left + #old, failures + 1
                    if failures >= 3 then return false end
                end
            end)
            if failures >= 3 then return end
        end
        d.noted = left
        E.prunes = (E.prunes or 0) + 1
        c.hook('engine_pruned')
    end
    -- A catch-up pass (issue #184): the keys captured so far are copied
    -- again from the confirmed source, while writes go on, so that the
    -- capture needs room only for the writes since, and the final pass,
    -- behind the fence, rereads only those. A fresh set takes the writes
    -- from the swap on. The writes noted in the old set may still be in
    -- flight. A caller still in its commit is waited for (the commits in
    -- flight, Lua and native). A write whose caller left it detached is in
    -- the synchronous queue: the pass then writes a marker, the count of
    -- its passes in the job's record, which commits only once every entry
    -- ahead of it in the queue has. Confirmed reads after it see those
    -- writes. (Tarantool 3.9 also logs a delete of a missing key, so the
    -- first batch of deletes below would wait too; the marker doesn't rely
    -- on that. No transaction cut: that waits for an empty queue, which a
    -- table under steady writes may not have, and holds up the engine's
    -- other transactions meanwhile.) Every key of the old set leaves the
    -- shadow first: the rows left are ones no write touched since they were
    -- copied, or since an earlier pass reread them, and those keys'
    -- confirmed rows then go in, as the final pass puts them in. A copied
    -- row that holds one of their unique keys leaves if a write to it began
    -- since the swap (its key is in the new set), and is otherwise a
    -- duplicate in the source (1062). A pass that fails puts its keys back
    -- (restore).
    local function catch_up(a, gen, t, deadline)
        local x = bound(a)
        local list = t.list
        t.list, t.keys, t.bytes = {}, {}, 0
        -- The keys written from now on take the next generation: the pass
        -- settles this one (with the swap: no yield in between).
        local d = bound_durable(x.old_id)
        if d ~= nil and d.name ~= durable_name(x.old_id, t.token) then d = nil end
        local settles = d and d.gen
        if d then d.gen = d.gen + 1 end
        local ok, err = pcall(function()
            if not c.wait_commits(deadline) then fail(1205, 'Engine source writes have not drained') end
            local function held() if not capture_valid(t) or E.tracks[x.old_id] ~= t then fail(1213, 'Engine capture was lost') end end
            c.hook('engine_catch_up_swapped')
            local phases = {copying = true, copied = true}
            local settled = transaction(function()
                local v = owned(a, gen, phases)
                v.passes = (v.passes or 0) + 1
                replace_record(v)
                return v.settled or 0
            end, held)
            -- (Only the keys the durable capture notes: the others' writes
            -- rolled back, and their copies are the source's rows.)
            local keep = noted(x, t, list, settled)
            for offset = 1, #keep, 128 do
                transaction(function()
                    local v = owned(a, gen, phases)
                    local target = box.space[v.new_id]
                    for i = offset, math.min(offset + 127, #keep) do target:delete(keep[i]) end
                end, held)
                fiber.sleep(0)
            end
            for offset = 1, #keep, 128 do
                local rows = read(function()
                    local out, source = {}, box.space[x.old_id]
                    for i = offset, math.min(offset + 127, #keep) do
                        local row = source:get(keep[i])
                        if row ~= nil then out[#out + 1] = row end
                    end
                    return out
                end)
                transaction(function()
                    local v = owned(a, gen, phases)
                    local target = box.space[v.new_id]
                    local unique, pk, shape = uniques(target), c.key_def(target.id, 0), shaper(v)
                    local check_row = checker(a)
                    for _, confirmed in ipairs(rows) do
                        check_row(confirmed, position_in(x.old_id, confirmed))
                        local row = shape(confirmed)
                        local other, ix = holder(target, unique, row)
                        while other ~= nil do
                            if not captured(t, x.old_id, other) then duplicate(ix, row) end
                            local key = pk:extract_key(other)
                            target:delete(key); hole(x, t, key:totable())
                            other, ix = holder(target, unique, row)
                        end
                        target:replace(row)
                    end
                end, held)
                fiber.sleep(0)
            end
            -- Reread: the keys up to its generation are settled.
            if settles ~= nil then
                transaction(function()
                    local v = owned(a, gen, phases)
                    if (v.settled or 0) < settles then v.settled = settles; replace_record(v) end
                end, held)
            end
        end)
        if not ok then
            -- As in capture(): a restore that fails part way leaves a set
            -- no later writer may trust.
            if not pcall(restore, t, list) then t.valid = false end
            error(err)
        end
        if settles ~= nil then pcall(prune, a, gen, x, t, settles) end
        E.passes = (E.passes or 0) + 1
        c.hook('engine_catch_up')
    end
    -- Resume (issue #184): a BUILD or PREPARE that finds the in-memory
    -- capture lost, while the durable one of the record's token is there,
    -- rebuilds it and goes on from the cursor. The writes noted only in the
    -- durable capture may still be in flight: as a pass does, it waits for
    -- the commits in flight, then writes a marker (the count of resumes in
    -- the job's record), which commits after the synchronous queue's
    -- entries. Its keys and holes above the settled generation then join
    -- the new mirror, which notes the writes from its start. Over the
    -- capture's limits, it gives up: the copy starts over (nil). Until those
    -- keys are in it, the new mirror is not the copy's capture: a resume
    -- that fails part way leaves it lost, and so does one whose capture is
    -- lost meanwhile, to a status change for one (1213). The step sent again
    -- resumes again.
    local function resume(a, gen, r, deadline)
        local x = bound(a)
        local sp, hp = durable_of(r)
        if sp == nil or hp == nil then return nil end
        local d = bind_durable(x.old_id, sp, hp)
        local settled = r.settled or 0
        d.gen = math.max(d.gen, settled + 1)
        local t = {token = r.capture, valid = true, epoch = E.capture_epoch, keys = {}, list = {}, bytes = 0}
        -- (The budgets of passes go on with the capture's.)
        local lost = E.tracks[x.old_id]
        if lost ~= nil and lost.token == r.capture then t.fence_passes, t.behind = lost.fence_passes, lost.behind end
        E.tracks[x.old_id] = t; attach(x.old_id)
        local ok, resumed = pcall(function()
            local function held() if not capture_valid(t) or E.tracks[x.old_id] ~= t then fail(1213, 'Engine capture was lost') end end
            if not c.wait_commits(deadline) then fail(1205, 'Engine source writes have not drained') end
            transaction(function()
                local v = owned(a, gen, {copying = true, copied = true, preparing = true})
                v.resumes = (v.resumes or 0) + 1
                replace_record(v)
            end, held)
            c.hook('engine_resume_marked')
            -- The keys and holes above the settled generation, a page per
            -- read (a write meanwhile is in the mirror already): past the
            -- mirror's limits, the copy starts over.
            local limit = c.cfg.engine_dirty_keys or 65536
            local bytes = c.cfg.engine_dirty_bytes or 8 * 1024 * 1024
            local list, seen, size = {}, {}, 0
            for _, space in ipairs({sp, hp}) do
                local within_limits = pages(space, function(page)
                    for _, e in ipairs(page) do
                        if e[2] > settled and not seen[e[1]] then
                            if #list >= limit or size + #e[1] > bytes then return false end
                            seen[e[1]], list[#list + 1], size = true, mp.decode(e[1]), size + #e[1]
                        end
                    end
                end)
                if not within_limits then return false end
            end
            restore(t, list)
            if E.tracks[x.old_id] ~= t or t.epoch ~= E.capture_epoch then
                fail(1213, 'Engine capture was lost while it resumed')
            end
            -- (Lost to its limits, writes meanwhile included: false.)
            return capture_valid(t)
        end)
        if not ok or not resumed then t.valid = false end
        if not ok then error(resumed) end
        if not resumed then return nil end
        E.resumes = (E.resumes or 0) + 1
        c.hook('engine_resumed')
        return t
    end
    -- A slice (opts.slice, seconds, from an SQL server that sends the step
    -- again): once a call has run that long, from its start, the wait for
    -- the table's serial lock included, BUILD returns {'ok', 'more'} at the
    -- next page or pass, where everything it did is durable or in the
    -- capture, which the next call goes on with. A copy of a large table
    -- then takes many calls, each well within the SQL server's RPC deadline
    -- (issue #184); before, a call past it was sent again while the first
    -- still held the table's serial lock. (E.run turns the slice into the
    -- call's own deadline, opts.until_.) E.slices counts those returns.
    local function sliced(opts)
        local slice = type(opts) == 'table' and tonumber(opts.slice) or nil
        if slice == nil or slice ~= slice or slice < 0 then return nil end
        return {until_ = clock.monotonic() + slice}
    end
    local function more()
        E.slices = (E.slices or 0) + 1
        return {'ok', 'more'}
    end
    function E.build(name, a, gen, deadline, opts)
        -- E.body's registry refresh may have yielded since its entry cut.
        -- Reacquire only before entering a stage, never by retrying a
        -- transaction/commit that raised or may still have native effects.
        ready(deadline)
        local r = start(name, a, gen, deadline)
        local function certified(v)
            return v.phase == 'prepared' or v.phase == 'applying' or v.phase == 'published'
        end
        if certified(r) then return {'ok', 'applied'} end
        local x = bound(a)
        local until_ = type(opts) == 'table' and opts.until_ or nil
        local function current(v, t, epoch)
            return q.ready() and (epoch == nil or q.epoch == epoch)
                   and capture_valid(t) and E.tracks[x.old_id] == t and t.token == v.capture
        end
        repeat
            -- A cut may be lost between successful stages. Nothing sampled
            -- below survives this wait: reread ownership, phase and capture
            -- only after the terminal-ticket/native prefix is current. The
            -- copy may have run past the call's deadline: another job's cut
            -- or a capture lost to writes needs a wait of its own here.
            deadline = within(a)
            ready(deadline)
            r = read(function() return owned(a, gen) end)
            if certified(r) then return {'ok', 'applied'} end
            if r.phase ~= 'resetting' and r.phase ~= 'copying' and r.phase ~= 'copied'
               and r.phase ~= 'preparing' then fail(1412, 'Engine copy phase changed') end
            local t = E.tracks[x.old_id]
            if r.phase ~= 'resetting' and (not capture_valid(t) or t.token ~= r.capture) then
                t = resume(a, gen, r, deadline)
                r = read(function() return owned(a, gen) end)
                if certified(r) then return {'ok', 'applied'} end
            end
            if r.phase == 'resetting' or not capture_valid(t) or t.token ~= r.capture then
                -- (Counted in the record, until the copy reaches the end of
                -- the table: a BUILD in slices, or sent again, goes on
                -- counting.)
                if (r.resets or 0) >= 8 then fail(1205, 'Engine copy repeatedly lost its capture; retry the ALTER') end
                -- (A deadline of its own: a resume before may have taken
                -- the one above.)
                t = reset(a, gen, within(a))
                r = read(function() return owned(a, gen) end)
                if certified(r) then return {'ok', 'applied'} end
            end
            -- reset() can return a confirmed copying receipt after a
            -- status callback invalidated its volatile cut/capture. Do not
            -- enter even the first page transaction with that old state.
            if current(r, t) then
                -- (A PREPARE sent again behind its fence, its capture
                -- resumed: the copy is done, and PREPARE goes on.)
                if r.phase == 'preparing' then return {'ok'} end
                if r.phase == 'copied' then
                    -- The final pass rereads at most engine_fence_keys keys:
                    -- passes catch up to that first. Writes that outpace them
                    -- fail the change, rather than hold the fence longer.
                    -- (The budget below then starts again: PREPARE's passes
                    -- have one of their own, as BUILD's have.)
                    if #t.list <= (c.cfg.engine_fence_keys or 1024) then t.fence_passes = 0; return {'ok'} end
                    -- (Counted with the capture: across the calls of a BUILD
                    -- in slices, and of one sent again.)
                    t.fence_passes = (t.fence_passes or 0) + 1
                    if t.fence_passes > (c.cfg.engine_catchup_passes or 16) then
                        fail(1205, string.format('Writes to the table outpace the copy (%d keys written since its '
                            .. 'last catch-up pass, over tarn.cfg.engine_fence_keys); retry the ALTER when it is '
                            .. 'quieter', #t.list))
                    end
                    catch_up(a, gen, t, within(a))
                    if until_ ~= nil and clock.monotonic() >= until_ then return more() end
                    goto continue
                end
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
                        local unique, pk, shape = uniques(shadow), c.key_def(shadow.id, 0), shaper(v)
                        local check_row = checker(a)
                        for i, source_row in ipairs(rows) do
                            check_row(source_row, function() return v.copied + i end)
                            local tuple = shape(source_row)
                            -- A copied row that holds one of this row's
                            -- unique keys: if a write to it began since
                            -- capture did, its copy may be stale and leaves
                            -- the replacement (the row's key is captured, so
                            -- the final pass puts it back). Otherwise both
                            -- rows held the key in the confirmed source this
                            -- page was read from. (The durable capture
                            -- notes the copy taken out as a hole.)
                            local other, ix = holder(shadow, unique, tuple)
                            while other ~= nil do
                                if not captured(t, x.old_id, other) then duplicate(ix, tuple) end
                                local key = pk:extract_key(other)
                                shadow:delete(key); hole(x, t, key:totable())
                                other, ix = holder(shadow, unique, tuple)
                            end
                            shadow:replace(tuple)
                        end
                        v.cursor, v.copied = cursor, v.copied + #rows
                        if #rows == 0 then v.phase, v.resets = 'copied', 0 end
                        replace_record(v)
                    end, function() if not capture_valid(t) then fail(1213, 'Engine capture was lost') end end)
                    if #rows > 0 then c.hook('engine_copy') end
                    -- (Below the capture's limit, so that a pass comes first,
                    -- and never over an empty capture.)
                    local catchup = math.max(1, math.min(c.cfg.engine_catchup_keys or 16384,
                        math.floor((c.cfg.engine_dirty_keys or 65536) / 2)))
                    if #rows > 0 and #t.list >= catchup and capture_valid(t) then
                        catch_up(a, gen, t, within(a))
                        -- A pass whose writes refill the capture gains no
                        -- ground: the copy goes a page per pass. That many
                        -- in a row fail the change, as before the fence.
                        t.behind = #t.list >= catchup and (t.behind or 0) + 1 or 0
                        if t.behind > (c.cfg.engine_catchup_passes or 16) then
                            fail(1205, string.format('Writes to the table outpace the copy (%d keys written during '
                                .. 'its last catch-up pass, over tarn.cfg.engine_catchup_keys); retry the ALTER when '
                                .. 'it is quieter', #t.list))
                        end
                    end
                end
            end
            ::continue::
            if until_ ~= nil and clock.monotonic() >= until_ and r.phase == 'copying' then return more() end
            fiber.sleep(0)
        until false
    end
    -- The lazy generations the job makes, taken by its current worker, also
    -- in a PREPARE sent again after a takeover (READY takes only the
    -- worker's): an engine change's, bound to the new space, and a copy's,
    -- bound to the old space until publication (L.rebind). The others a
    -- copy names are the table's, kept as they are.
    local function new_lazy(a, gen)
        local x = bound(a)
        if x.rebuild == nil then c.lazy.prepare(a.def, gen)
        elseif makes(a) then c.writable(); c.lazy.prepare(made_args(a, x).def, gen) end
    end
    -- A batch refused before it wrote anything, which waits: a conflict,
    -- the outbox's admission limit (its deliveries go on), a busy shard.
    local function waits(r)
        return r[2] == 1213 or (r[2] == 1041 and tostring(r[3]):find('admission limit') ~= nil)
            or (r[2] == 1205 and tostring(r[3]):find('admission refused') ~= nil)
    end
    -- READY of the generations a copy makes, in APPLY, when no coordinator
    -- sent it: one from before such copies sends none, nor the batches of a
    -- scan behind the fence. This runs that scan, waits for the deliveries
    -- and marks them ready, as READY does, and otherwise refuses for now
    -- (1213), which APPLY's sender waits out: each APPLY goes on from the
    -- scan's durable cursor.
    local function made_ready(name, a, gen, deadline)
        local x = bound(a)
        local made, waiting = made_set(a), false
        for _, s in ipairs(a.def.lazy or {}) do
            local d = made[s.gen] and box.space._tarn_lazy_defs:get{s.gen}
            if made[s.gen] and (d == nil or d.phase ~= 'ready') then waiting = true end
        end
        if not waiting then return end
        local m = made_args(a, x)
        c.writable(); c.lazy.prepare(m.def, gen)
        repeat
            local r = c.lazy.bootstrap(name, m, gen)
            if r[1] == 'ok' and r[2] == true then break end
            if r[1] ~= 'ok' and not waits(r) then fail(r[2] or 1412, r[3] or 'Lazy scan failed') end
            if clock.monotonic() > deadline then fail(1213, 'A lazy scan behind the fence is under way; retry') end
            fiber.sleep(r[1] == 'ok' and 0 or 0.01)
        until false
        repeat
            local s = c.lazy.bootstrap_status(name, a, gen)
            if s[1] ~= 'ok' then fail(s[2] or 1412, s[3] or 'Lazy READY failed') end
            if s[2] and s[3] == 0 then break end
            if clock.monotonic() > deadline then fail(1213, 'Lazy deliveries are pending; retry') end
            fiber.sleep(0.01)
        until false
        local r = c.lazy.ready(name, a, gen)
        if r[1] ~= 'ok' then fail(r[2] or 1412, r[3] or 'Lazy READY failed') end
    end
    function E.prepare(name, a, gen, deadline, opts)
        local r = start(name, a, gen, deadline)
        if r.phase == 'published' then return {'ok', 'applied'} end
        if r.phase == 'prepared' or r.phase == 'applying' then
            new_lazy(a, gen)
            return {'ok'}
        end
        if r.phase == 'preparing' and E.tracks[r.old_id] then E.tracks[r.old_id].valid = false end
        -- (In slices too: the copy, and passes, before the fence.)
        local built = E.build(name, a, gen, deadline, opts)
        if built[1] == 'ok' and built[2] == 'more' then return built end
        deadline = within(a)
        update(a, gen, {copied = true, preparing = true}, function(v)
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
        local function held() if not capture_valid(t) then fail(1213, 'Engine capture was lost') end end
        -- (Only the keys the durable capture notes, as in a pass: a capture
        -- lost during the final pass is rebuilt with every key it clears.)
        local settled = read(function() return owned(a, gen, {preparing = true}).settled or 0 end)
        local list = noted(bound(a), t, t.list, settled)
        -- First every captured key leaves the replacement. The rows left
        -- are ones no committed write touched since they were copied, so
        -- they are the source's rows. Then the captured keys' confirmed rows
        -- go in: one whose unique key another row holds now is a duplicate
        -- in the fenced source itself (1062). Replacing the captured rows one
        -- by one instead would meet stale copies, as when two rows swapped
        -- keys.
        for offset = 1, #list, 128 do
            transaction(function()
                owned(a, gen, {preparing = true})
                local target = box.space[r.new_id]
                for i = offset, math.min(offset + 127, #list) do target:delete(list[i]) end
            end, held)
            c.hook('engine_tail'); fiber.sleep(0)
        end
        for offset = 1, #list, 128 do
            local final = read(function()
                local out, source = {}, box.space[r.old_id]
                for i = offset, math.min(offset + 127, #list) do
                    out[#out + 1] = source:get(list[i])
                end
                return out
            end)
            transaction(function()
                owned(a, gen, {preparing = true})
                local target = box.space[r.new_id]
                local unique, shape, check_row = uniques(target), shaper(r), checker(a)
                for _, confirmed in ipairs(final) do
                    check_row(confirmed, position_in(r.old_id, confirmed))
                    local row = shape(confirmed)
                    local other, ix = holder(target, unique, row)
                    if other ~= nil then duplicate(ix, row) end
                    target:replace(row)
                end
            end, held)
            c.hook('engine_tail'); fiber.sleep(0)
        end
        deadline = within(a)
        cut(deadline)
        r = read(function() return owned(a, gen, {preparing = true}) end)
        -- (Each exists: reset created them before the first row.)
        for _, ix in ipairs(r.schema.indexes) do
            c.writable(); schema_index(box.space[r.new_id], ix)
        end
        for _, d in ipairs(a.uix or {}) do c.create_space(d) end
        new_lazy(a, gen)
        if makes(a) then scan_bound(a, bound(a)) end
        cut(deadline)
        -- The copy that makes building global indexes enforcing (issue #292):
        -- behind the drained fence, the source's conflicts of them on this
        -- shard must be gone, or two rows share a key and the change fails.
        local dup = c.uix_conflicts(a.uix_check)
        if dup ~= nil then fail(1062, dup) end
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
        -- (A durable capture left by a node from before, which a cleanup
        -- did not know, goes with an APPLY sent again.)
        if r.cleaned then drop_durables(r.old_id); return {'ok', 'applied'} end
        E.durable[r.old_id] = nil
        cut(deadline)
        for _, sid in ipairs(r.old_uix or {}) do if box.space[sid] ~= nil then box.space[sid]:drop() end end
        local old = box.space[r.old_id]
        if old ~= nil then
            if old.name ~= r.retired_name then fail(1412, 'Retired engine source identity changed') end
            old:drop()
        end
        drop_durables(r.old_id)
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
        if makes(a) then made_ready(name, a, gen, deadline) end
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
        c.hook('engine_renamed')
        -- The global index spaces a rebuild keeps are named after the space
        -- that has the table's name, by which the storage finds them
        -- (UIX.spaces in lua/tarn.lua, which until the catalog below names
        -- that space finds them through the catalog too). Their contents
        -- stay, so their readers are not doomed: both definitions write them.
        for _, k in ipairs(r.kept_uix or {}) do
            local u = box.space[k.id]
            if u ~= nil and u.name == k.old_name then u:rename(k.new_name) end
            if u == nil or u.name ~= k.new_name then fail(1412, 'Kept global index identity changed') end
        end
        for _, sid in ipairs(group(r, true)) do c.doom(sid) end
        c.hook('engine_publish')
        transaction(function()
            local v = owned(a, gen, {applying = true})
            c.lazy.retire(a.lazy_drop)
            c.lazy.activate(a.def)
            local bind = {}
            for _, gen_ in ipairs(v.kept_lazy or {}) do bind[#bind + 1] = gen_ end
            for _, gen_ in ipairs(v.made_lazy or {}) do bind[#bind + 1] = gen_ end
            c.lazy.rebind(a.def, bind)
            box.space._tarn_lazy_capture:delete{v.old_id}
            box.space._tarn_catalog:replace{name, a.def}
            v.phase, v.cleaned = 'published', false; replace_record(v)
            for _, sid in ipairs(group(v, true)) do
                box.space._tarn_meta:replace{'fence:' .. sid,
                    {ver = a.ver, gen = gen, phase = 'migrated', engine_retired = true}}
            end
        end)
        -- A lazy registry read while that transaction waited for its quorum
        -- has the new space pending (L.fresh): read it again, now published.
        c.lazy.invalidate()
        c.hook('engine_published')
        r = read(function() return owned(a, gen, {published = true}) end)
        return clean_published(a, gen, r, deadline)
    end
    function E.abort(name, a, gen, deadline)
        local x = bound(a)
        local r = read(function() return record(x.old_id) end)
        if r and r.ver == a.ver and r.new_id == a.def.id and r.phase == 'published' then return {'ok', 'applied'} end
        if r and r.ver == a.ver and r.new_id == a.def.id and r.phase == 'aborted' then
            detach(r.old_id); drop_durables(r.old_id); refresh()
            return {'ok'}
        end
        central(name, a, gen, 'abort')
        r = start(name, a, gen, deadline, true)
        if r.phase == 'applying' then fail(1412, 'Engine publication already began') end
        update(a, gen, nil, function(v) v.phase = 'aborting' end)
        -- (As in reset: the writes from now on note nothing in the durable
        -- capture, and the cut waits for those that did.)
        E.durable[r.old_id] = nil
        cut(deadline)
        local target = box.space[r.new_id]
        if target ~= nil then
            if target.name ~= r.shadow_name then fail(1412, 'Engine shadow identity changed') end
            target:drop()
        end
        drop_durables(r.old_id)
        for _, d in ipairs(a.uix or {}) do if box.space[d.id] ~= nil then box.space[d.id]:drop() end end
        transaction(function()
            local v = owned(a, gen, {aborting = true})
            -- (A rebuild retires only the lazy generations it made: those it
            -- keeps are the table's.)
            if x.rebuild == nil then c.lazy.abort(a.def)
            elseif makes(a) then c.lazy.retire_made(a.def, x.old_id, made_set(a)) end
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
    function E.body(op, name, a, gen, opts)
        c.writable(); c.bk.fresh()
        local deadline = within(a)
        ready(deadline); E.fresh()
        -- The online build of a copy's made generations, against the old
        -- space, before the copy (tarn_lazy_start, tarn_lazy_bootstrap). An
        -- engine change builds its new generations behind its fence instead.
        if (op == 'lazy_start' or op == 'bootstrap') and makes(a) then
            local x = bound(a)
            local r = read(function() return record(x.old_id) end)
            if r and r.ver == a.ver and r.new_id == a.def.id then
                if r.phase == 'published' then return op == 'bootstrap' and {'ok', true} or {'ok', 'applied'} end
                if r.phase == 'aborted' then fail(1412, 'Engine replacement was aborted') end
            elseif r and not terminal(r.phase) then
                fail(1412, 'A different engine job owns the source')
            end
            -- (The descriptor as the copy will take it, before anything
            -- starts: a generation it would refuse is never built.)
            keep_lazy(a, x)
            local m = made_args(a, x)
            if op == 'bootstrap' then return c.lazy.bootstrap(name, m, gen) end
            return c.lazy_start(name, m, gen)
        elseif op == 'lazy_start' then
            return {'error', 1846, 'This change builds its lazy indexes behind the ALTER fence'}
        end
        if op == 'bootstrap' or op == 'ready' then
            read(function() owned(a, gen, {prepared = true}) end)
            return op == 'bootstrap' and c.lazy.bootstrap(name, a, gen) or c.lazy.ready(name, a, gen)
        end
        return assert(E[op])(name, a, gen, deadline, opts)
    end
    function E.run(op, name, a, gen, opts)
        local ok, result = pcall(function()
            if box.is_in_txn() then fail(1210, 'Engine DDL requires its own transaction') end
            local x = bound(a)
            if name ~= x.old_name then fail(1210, 'Engine request name does not match its source binding') end
            return c.serial(x.old_id, a.timeout or 50, E.body, op, name, a, gen or 0, sliced(opts))
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
