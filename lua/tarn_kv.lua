-- Supported read-only KV v1. Wire keys/tuples use the existing native
-- representation; definition_json supplies the SQL row/default codec.
-- Every point is independent. A complete batch is not an atomic snapshot.
local clock = require('clock')
local digest = require('digest')
local ffi = require('ffi')
local json = require('json')
local msgpack = require('msgpack')
local varbinary = require('varbinary')
local uri = require('uri')

local M = {}
local MAX_KEYS, KEY_BYTES, REPLY_BYTES = 128, 64 * 1024, 1024 * 1024
local CONSISTENCY = 'independent_points'
local FUNCTIONS = {'tarn_kv_v1.meta', 'tarn_kv_v1.get', 'tarn_kv_v1.get_many'}

local function fail(kind, message, code)
    error({kv_v1 = true, kind = kind, domain = 'contract', code = code or 0, message = message}, 0)
end
local function schema_changed()
    fail('schema_changed', 'Table definition changed; explicitly open a new table handle', 1412)
end
local function integer(n, min, max)
    return type(n) == 'number' and n == n and n % 1 == 0 and n >= min and n <= max
end
local function fields(value, required)
    if type(value) ~= 'table' then fail('bad_request', 'Expected a map') end
    for k in pairs(value) do if required[k] == nil then fail('bad_request', 'Unknown map field') end end
    for k in pairs(required) do if value[k] == nil then fail('bad_request', 'Missing map field: ' .. k) end end
end
local function array(value, max)
    if type(value) ~= 'table' then fail('bad_request', 'Expected an array') end
    local n = #value
    if n > max then fail('limit_exceeded', 'Array exceeds the KV v1 bound') end
    local count = 0
    for k in pairs(value) do
        if not integer(k, 1, n) then fail('bad_request', 'Sparse or non-array key') end
        count = count + 1
    end
    if count ~= n then fail('bad_request', 'Sparse or non-array key') end
    return n
end
local function identifier(s)
    if type(s) ~= 'string' or #s == 0 or #s > 64 or s:find('%z') then
        fail('bad_request', 'Invalid database or table identifier')
    end
    if s:find('.', 1, true) then fail('unsupported', 'KV v1 identifiers cannot contain a dot') end
end
local function table_token(t)
    fields(t, {db = true, name = true, sid = true, ver = true, definition = true, layout = true})
    identifier(t.db); identifier(t.name)
    if not integer(t.sid, 1, 4294967295) or not integer(t.ver, 0, 9007199254740991)
       or type(t.definition) ~= 'string' or not t.definition:match('^[0-9a-f]+$') or #t.definition ~= 64
       or type(t.layout) ~= 'string' or not t.layout:match('^[0-9a-f]+$') or #t.layout ~= 64 then
        fail('bad_request', 'Invalid table token')
    end
end
local function ownership_token(t)
    fields(t, {shard_id = true, bucket_count = true, bucket_epoch = true, membership_version = true})
    if not integer(t.shard_id, 1, 4294967295) or not integer(t.bucket_count, 1, 1000000)
       or not integer(t.bucket_epoch, 0, 9007199254740991)
       or not integer(t.membership_version, 0, 9007199254740991) then
        fail('bad_request', 'Invalid ownership token')
    end
end
local function preflight_keys(keys)
    -- Bound traversal and raw payload before any recursive serializer.
    -- Every array/scalar needs at least one encoded byte; exact headers
    -- and integer widths are checked after native-key validation below.
    local used = 1
    for _, value in ipairs(keys) do
        array(value, KEY_BYTES)
        used = used + 1
        if used > KEY_BYTES then fail('limit_exceeded', 'KV v1 encoded keys exceed 64 KiB') end
        for _, v in ipairs(value) do
            local ty, size = type(v), 1
            if ty == 'string' then size = #v + 1
            elseif varbinary.is(v) then size = #v + 1 -- public __len uses O(1) ffi.sizeof
            elseif not integer(v, -9007199254740991, 9007199254740991)
               and not ffi.istype('int64_t', v) and not ffi.istype('uint64_t', v) then
                fail('bad_request', 'Primary keys must be flat arrays of canonical scalar parts')
            end
            if size > KEY_BYTES - used then fail('limit_exceeded', 'KV v1 encoded keys exceed 64 KiB') end
            used = used + size
        end
    end
end
local function public_address(address)
    if type(address) ~= 'string' then fail('unavailable', 'Invalid public replica address') end
    local parsed = uri.parse(address)
    if parsed == nil then fail('unavailable', 'Invalid public replica address') end
    -- Never forward URI query/params, fragments or arbitrary path/scheme
    -- fields: they can contain transport secrets or local filenames.
    if parsed.query ~= nil or parsed.params ~= nil and next(parsed.params) ~= nil
       or parsed.fragment ~= nil or parsed.path ~= nil
       or parsed.scheme ~= nil and parsed.scheme ~= 'tcp' and parsed.scheme ~= 'unix' then
        fail('unsupported', 'KV v1 replica discovery supports plain TCP or Unix addresses only')
    end
    if parsed.unix ~= nil then
        if parsed.unix:sub(1, 1) ~= '/' then fail('unsupported', 'KV v1 requires absolute Unix socket paths') end
        return 'unix/:' .. parsed.unix
    end
    local port = tonumber(parsed.service)
    if type(parsed.host) ~= 'string' or parsed.host == '' or not integer(port, 1, 65535) then
        fail('unavailable', 'Invalid public TCP replica address')
    end
    return uri.format{host = parsed.host, service = tostring(port), ipv4 = parsed.ipv4, ipv6 = parsed.ipv6}
end
local function same_token(a, b)
    return a.db == b.db and a.name == b.name and a.sid == b.sid and a.ver == b.ver
           and a.definition == b.definition and a.layout == b.layout
end
local function bounded(reply)
    if #msgpack.encode(reply) > REPLY_BYTES then fail('limit_exceeded', 'KV v1 reply exceeds 1 MiB') end
    return reply
end
local function status_error(r)
    local code = tonumber(r[2]) or 0
    local kind = r[1] == 'moved' and 'moved' or code == 1412 and 'schema_changed' or 'unavailable'
    local e = {kv_v1 = true, kind = kind, domain = 'mysql', code = code,
               message = tostring(r[3] or 'Strong KV read refused')}
    if r[4] ~= nil and r[4] ~= box.NULL then e.peer = r[4] end
    error(e, 0)
end
local function error_reply(err)
    local e
    if type(err) == 'table' and err.kv_v1 then
        e = {kind = err.kind, domain = err.domain, code = err.code, message = err.message, peer = err.peer}
    elseif type(err) == 'cdata' and err.code ~= nil then
        local code = tonumber(err.code)
        local kind = code == box.error.ACCESS_DENIED and 'access_denied'
                     or code == box.error.READONLY and 'moved'
                     or code == box.error.NO_SUCH_SPACE and 'schema_changed' or 'unavailable'
        e = {kind = kind, domain = 'tarantool', code = code, message = tostring(err.message or err)}
    else
        e = {kind = 'read_error', domain = 'contract', code = 0, message = tostring(err)}
    end
    return {version = 1, status = 'error', error = e}
end

function M.new(c)
    local K = {}
    local function checkpoint(deadline)
        if clock.monotonic() >= deadline then fail('unavailable', 'KV v1 request deadline elapsed') end
    end
    local function acl(sid, uid)
        -- Public SELECT performs the real effective READ check even at
        -- limit zero. No tuple is fetched, and no caller view is borrowed.
        box.session.su(uid, function()
            local sp = box.space[sid]
            if sp == nil or sp.index[0] == nil then schema_changed() end
            sp.index[0]:select({}, {iterator = 'ALL', limit = 0})
        end)
        if box.is_in_txn() then fail('read_error', 'ACL probe left an unexpected transaction') end
    end
    local function definition(name)
        box.begin{txn_isolation = 'read-confirmed'}
        local ok, row = pcall(function() return box.space._tarn_catalog:get{name} end)
        box.rollback()
        if not ok then error(row) end
        if row == nil then schema_changed() end
        return row.def
    end
    local function describe(db, name, uid)
        local canonical = db .. '.' .. name
        local def = definition(canonical)
        local sp = box.space[def.id]
        if sp == nil or sp.name ~= canonical or def.name ~= canonical then schema_changed() end
        acl(sp.id, uid) -- Do not disclose a definition merely because CALL was granted.
        if type(def.json) ~= 'string' or #def.json == 0 then
            fail('unsupported', 'KV v1 requires a SQL table definition')
        end
        if #def.json > REPLY_BYTES then fail('limit_exceeded', 'KV v1 definition exceeds 1 MiB') end
        local d = json.decode(def.json)
        if type(d) ~= 'table' or d.db ~= db or d.name ~= name or d.space_id ~= def.id then schema_changed() end
        local ver = def.ver or 0
        -- ALTER's outer fence and the serialized SQL definition version
        -- are independent. Freeze both the outer version and exact JSON.
        if not integer(ver, 0, 9007199254740991) then schema_changed() end
        if sp.engine ~= (def.engine or 'memtx') then schema_changed() end
        c.fresh.space(sp)
        local format, by_name = {}, {}
        local actual = sp:format()
        if type(def.format) ~= 'table' or #actual ~= #def.format then schema_changed() end
        for i, f in ipairs(actual) do
            local declared = def.format[i]
            if f.name ~= declared.name or f.type ~= declared.type
               or (f.is_nullable == true) ~= (declared.is_nullable == true) then schema_changed() end
            format[i] = {f.name, f.type, f.is_nullable == true}
            by_name[f.name] = i
        end
        local idx, declared = sp.index[0], def.indexes and def.indexes[1]
        if idx == nil or not idx.unique or idx.type ~= 'TREE' or idx.func ~= nil or declared == nil or not declared.unique
           or #idx.parts == 0 or #idx.parts ~= #declared.parts then schema_changed() end
        local primary = {}
        for i, p in ipairs(idx.parts) do
            local dp = declared.parts[i]
            if p.fieldno ~= by_name[dp.field] or p.type ~= dp.type
               or (p.is_nullable == true) ~= (dp.is_nullable == true)
               or p.path ~= nil or p.collation ~= nil or p.is_nullable == true then
                fail('unsupported', 'KV v1 requires a non-null, non-functional native primary key')
            end
            if p.type ~= 'integer' and p.type ~= 'unsigned' and p.type ~= 'string' and p.type ~= 'varbinary' then
                fail('unsupported', 'KV v1 primary keys support only integer, unsigned, string and varbinary parts')
            end
            primary[i] = {p.fieldno, p.type, false, ''}
        end
        local pk = d.indexes and d.indexes[1]
        if type(d.columns) ~= 'table' or type(pk) ~= 'table' or pk.id ~= 0 or pk.name ~= 'PRIMARY'
           or type(pk.cols) ~= 'table' or #pk.cols ~= #primary or type(d.shard_key) ~= 'table'
           or #d.shard_key == 0 then fail('unsupported', 'KV v1 requires a routable declared primary key') end
        local positions, route = {}, {}
        for i, col in ipairs(pk.cols) do
            local column = integer(col, 0, #d.columns - 1) and d.columns[col + 1] or nil
            if column == nil or column.hidden or column.nullable then
                fail('unsupported', 'KV v1 requires a declared non-null primary key')
            end
            local field = column.weight or column.field or col
            if primary[i][1] ~= field + 1 or positions[col] ~= nil then schema_changed() end
            positions[col] = i
        end
        for i, col in ipairs(d.shard_key) do
            if positions[col] == nil then fail('unsupported', 'Partition columns must belong to the primary key') end
            route[i] = positions[col]
        end
        local token = {db = db, name = name, sid = sp.id, ver = ver,
                       definition = digest.sha256_hex(def.json),
                       layout = digest.sha256_hex(msgpack.encode({sp.engine, format, primary}))}
        return {token = token, json = def.json, format = format, primary = primary,
                route = route, space = sp, generation = c.guard(sp.id, ver)}
    end
    local function owner()
        local s = c.topology()
        if s.count == 0 then fail('unsupported', 'KV v1 requires an initialized bucket map') end
        return {shard_id = s.shard_id, bucket_count = s.count, bucket_epoch = s.epoch,
                membership_version = s.mver}, s
    end
    local function check_owner(want)
        local got = owner()
        if want.shard_id ~= got.shard_id or want.bucket_count ~= got.bucket_count
           or want.bucket_epoch ~= got.bucket_epoch or want.membership_version ~= got.membership_version then
            fail('moved', 'Ownership token changed; refresh routing under the same table handle')
        end
    end
    local function check_table(t, uid, deadline)
        checkpoint(deadline)
        local current = describe(t.token.db, t.token.name, uid)
        if current.space ~= t.space or current.generation ~= t.generation or not same_token(current.token, t.token) then
            schema_changed()
        end
        return current
    end
    local function key(value, t)
        if array(value, KEY_BYTES) ~= #t.primary then fail('bad_request', 'A complete primary key is required') end
        for i, v in ipairs(value) do
            local ty = t.primary[i][2]
            if ty == 'string' then
                if type(v) ~= 'string' then fail('bad_request', 'Expected a native string key part') end
            elseif ty == 'varbinary' then
                if not varbinary.is(v) then fail('bad_request', 'Expected a native binary key part') end
            else
                local signed, unsigned = ffi.istype('int64_t', v), ffi.istype('uint64_t', v)
                if not signed and not unsigned and not integer(v, -9007199254740991, 9007199254740991) then
                    fail('bad_request', 'Expected an exact native integer key part')
                end
                if ty == 'unsigned' and v < 0 or ty == 'integer' and unsigned and v > 9223372036854775807LL then
                    fail('bad_request', 'Primary key integer is out of range')
                end
            end
        end
    end
    local function bucket(value, t, count)
        local crc = digest.crc32.new()
        for _, p in ipairs(t.route) do
            local v, ty = value[p], t.primary[p][2]
            crc:update((ty == 'string' or ty == 'varbinary') and tostring(v) or msgpack.encode(v))
        end
        return tonumber(crc:result()) % count + 1
    end
    local function success(t)
        return {version = 1, status = 'ok', consistency = CONSISTENCY, table = t.token}
    end
    local function endpoint(fn, req, argc)
        -- This check precedes even validation/barriers. In particular a
        -- previously marked stream is not a fresh external point request.
        if box.is_in_txn() then return error_reply({kv_v1 = true, kind = 'ambient_transaction',
            domain = 'contract', code = 0, message = 'KV v1 refuses every ambient transaction'}) end
        if argc ~= 1 then return error_reply({kv_v1 = true, kind = 'bad_request',
            domain = 'contract', code = 0, message = 'KV v1 requires exactly one request map'}) end
        local uid = box.session.uid()
        local deadline = clock.monotonic() + c.cfg.read_freshness_timeout + c.cfg.lock_wait
        local ok, result = pcall(fn, req, uid, deadline)
        if box.is_in_txn() then
            local rolled, err = pcall(box.rollback)
            if not rolled then return error_reply(err) end
            if not ok then return error_reply(result) end
            return error_reply({kv_v1 = true, kind = 'read_error', domain = 'contract', code = 0,
                                message = 'KV v1 left an unexpected owned transaction'})
        end
        return ok and result or error_reply(result)
    end
    local function meta(req, uid, deadline)
        fields(req, {version = true, db = true, name = true})
        if req.version ~= 1 then fail('unsupported', 'Unsupported KV version') end
        identifier(req.db); identifier(req.name)
        c.prepare()
        local t = describe(req.db, req.name, uid)
        local ownership, topology = owner()
        local result = success(t)
        result.limits = {max_keys = MAX_KEYS, max_key_bytes = KEY_BYTES,
                         max_reply_bytes = REPLY_BYTES, max_metadata_bytes = REPLY_BYTES}
        result.definition_json, result.format, result.primary = t.json, t.format, t.primary
        result.shard_key_pk, result.ownership = t.route, ownership
        result.members = {}
        for _, member in ipairs(topology.members) do
            local replicas = {}
            for _, address in ipairs(member.replicas or {}) do
                replicas[#replicas + 1] = public_address(address)
            end
            result.members[#result.members + 1] = {id = member.id, replicas = replicas, state = member.state or 'active'}
        end
        result.buckets = {count = topology.count, epoch = topology.epoch, owned = topology.owned,
                          others = topology.others, membership_version = topology.mver}
        check_table(t, uid, deadline)
        check_owner(ownership)
        return bounded(result)
    end
    local function reads(req, uid, deadline, many)
        fields(req, many and {version = true, consistency = true, table = true, ownership = true, keys = true}
                        or {version = true, consistency = true, table = true, ownership = true, key = true})
        if req.version ~= 1 or req.consistency ~= CONSISTENCY then
            fail('unsupported', 'KV v1 supports only independent_points')
        end
        table_token(req.table); ownership_token(req.ownership)
        local keys = many and req.keys or {req.key}
        local n = array(keys, MAX_KEYS)
        if n == 0 then fail('bad_request', 'At least one primary key is required') end
        preflight_keys(keys)
        c.prepare()
        local t = describe(req.table.db, req.table.name, uid)
        if not same_token(t.token, req.table) then schema_changed() end
        check_owner(req.ownership)
        -- Validate every key before reading the first. A later malformed
        -- key cannot turn an already-read prefix into a successful reply.
        for _, value in ipairs(keys) do key(value, t) end
        if #msgpack.encode(keys) > KEY_BYTES then fail('limit_exceeded', 'KV v1 encoded keys exceed 64 KiB') end
        local result = success(t)
        result.slots = {}
        for i, value in ipairs(keys) do
            checkpoint(deadline)
            check_table(t, uid, deadline)
            check_owner(req.ownership)
            local b = bucket(value, t, req.ownership.bucket_count)
            -- Numeric bucket routing checks actual ownership. An epoch
            -- route alone would skip BK.bad even if it contained a list.
            local r = c.point(t.token.sid, value, b)
            if type(r) ~= 'table' or r[1] ~= 'ok' then
                if type(r) ~= 'table' then fail('read_error', 'Invalid guarded point response') end
                status_error(r)
            end
            if type(r[2]) ~= 'table' or #r[2] > 1 or r[3] ~= nil and r[3] ~= box.NULL then
                fail('read_error', 'Incomplete guarded point response')
            end
            check_table(t, uid, deadline)
            check_owner(req.ownership)
            local tuple = r[2][1]
            if tuple ~= nil and tuple:bsize() > REPLY_BYTES then
                fail('limit_exceeded', 'KV v1 stored tuple exceeds 1 MiB')
            end
            result.slots[i] = {position = i, key = value, status = tuple ~= nil and 'present' or 'missing', tuple = tuple}
            bounded(result) -- Includes key echoes, framing and the final tuple.
        end
        check_table(t, uid, deadline)
        check_owner(req.ownership)
        return bounded(result)
    end
    local function read_one(r, uid, deadline) return reads(r, uid, deadline, false) end
    local function read_many(r, uid, deadline) return reads(r, uid, deadline, true) end
    function K.meta(...) return endpoint(meta, select(1, ...), select('#', ...)) end
    function K.get(...) return endpoint(read_one, select(1, ...), select('#', ...)) end
    function K.get_many(...) return endpoint(read_many, select(1, ...), select('#', ...)) end
    function K.register()
        for _, name in ipairs(FUNCTIONS) do
            box.schema.func.create(name, {setuid = true, if_not_exists = true})
            local f = box.space._func.index.name:get{name}
            if f == nil or f.setuid ~= 1 or f.language ~= 'LUA' or f.body ~= ''
               or f.owner ~= box.session.euid() then
                error('KV v1 function registration is incompatible: ' .. name)
            end
        end
    end
    return K
end
return M
