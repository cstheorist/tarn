-- Actual registered KV v1 CALLs over net.box, under restricted credentials.
-- Standalone RF1 storage-unit fixtures for both engines; not SQL/RF3 history
-- or quorum/partition qualification. Schema/map fixtures below are labelled.
local fio, fiber, json, net = require('fio'), require('fiber'), require('json'), require('net.box')
local ffi, mp, binary, digest = require('ffi'), require('msgpack'), require('varbinary'), require('digest')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local native = os.getenv('TARN_NATIVE')
if native and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
local work = fio.abspath(assert(arg[1], 'new private work directory required'))
fio.mktree(work)
local port = tonumber(os.getenv('KV_API_TEST_PORT') or '3897')
box.cfg{work_dir = work, listen = '127.0.0.1:' .. port, memtx_use_mvcc_engine = true,
    memtx_memory = 256 * 1048576, vinyl_memory = 32 * 1048576, vinyl_cache = 0,
    -- Fixture-only headroom: store a row above the API's unchanged 1 MiB
    -- reply limit so the public oversized-final-tuple guard is exercised.
    memtx_max_tuple_size = 4 * 1048576, vinyl_max_tuple_size = 4 * 1048576,
    wal_mode = 'fsync', txn_isolation = 'read-confirmed', log_level = 3}
local address = '127.0.0.1:' .. port
local tarn = require('tarn').init{shard_id = 1, peers = {['1'] = {'tarn:tarn@' .. address}},
    native = native and native ~= '0' and native or false, test_hooks = true,
    check_locks = true, check_yields = true, lock_wait = .05, recovery_after = 1000000}
tarn.lazy.paused = true

local passed, failed, connections, held, prepared = 0, 0, {}, false, {}
local function event(label, value)
    local f = assert(io.open(work .. '/events.jsonl', 'a'))
    f:write(json.encode{event = label, data = value}, '\n'); f:close()
end
local function check(label, yes, value)
    event(label, value or {})
    assert(yes, label .. ': ' .. json.encode(value or {}))
    passed = passed + 1; print('  ok   ' .. label)
end
local function ok(r)
    assert(type(r) == 'table' and r[1] == 'ok', json.encode(r)); return r
end
local function copy(v)
    if type(v) ~= 'table' then return v end
    local out = {}; for k, x in pairs(v) do out[k] = copy(x) end; return out
end
local initial_peers = copy(tarn.cfg.peers['1'])
local function tuple(v) return box.tuple.is(v) and v:totable() or v end
local function connect(user, password)
    local c = net.connect(user .. ':' .. password .. '@' .. address, {wait_connected = 5})
    assert(c:is_connected(), 'connection failed for ' .. user)
    connections[#connections + 1] = c; return c
end
local admin = connect('tarn', 'tarn')
local function wait_for(fn, label)
    local deadline = fiber.clock() + 5
    repeat
        if fn() then return end
        fiber.sleep(.005)
    until fiber.clock() >= deadline
    error('timeout: ' .. label)
end
local function healthy()
    return not box.info.ro and tarn.clock.read_check() == nil and tarn.clock.check({1}) == nil
end
wait_for(healthy, 'confirmed local policy and own-writer clock readiness')
ok(admin:call('tarn_bucket_init', {1, {{1, 1}}}))
ok(admin:call('tarn_buckets'))

local names = {'kv_api_reader', 'kv_api_role', 'kv_api_writer', 'kv_api_none'}
local password, role = 'isolated-test-only', 'kv_api_read_role'
local functions = {'tarn_kv_v1.meta', 'tarn_kv_v1.get', 'tarn_kv_v1.get_many'}
box.schema.role.create(role)
for _, user in ipairs(names) do
    box.schema.user.create(user, {password = password})
    for _, fn in ipairs(functions) do box.schema.user.grant(user, 'execute', 'function', fn) end
end
box.schema.user.grant(names[2], role)
local reader, inherited, writer, none = connect(names[1], password), connect(names[2], password),
    connect(names[3], password), connect(names[4], password)

-- These named maps match alter::format_list/format_parts and a serialized
-- SQL TableDef. They are storage-unit metadata fixtures, not SQL execution.
local function column(name, ty, nullable, field)
    return {name = name, ty = ty, nullable = nullable, default = box.NULL,
            default_null = false, auto_increment = false, field = field}
end
local function definition(sid, name, engine, kind, outer, inner)
    local ty, sqlty = 'unsigned', 'BIGINT UNSIGNED'
    if kind == 'string' then ty, sqlty = 'string', 'VARCHAR(65535)' end
    if kind == 'binary' then ty, sqlty = 'varbinary', 'VARBINARY(64)' end
    if kind == 'number' then ty, sqlty = 'number', 'DOUBLE' end
    local cols = {column('id', sqlty, false, 0), column('v', 'BIGINT', true, 1),
                  column('payload', 'LONGTEXT', false, 2)}
    local format = {{name = 'id', type = ty, is_nullable = false},
                    {name = 'v', type = 'integer', is_nullable = true},
                    {name = 'payload', type = 'string', is_nullable = false},
                    {name = 'bucket_id', type = 'unsigned', is_nullable = false}}
    local pkcols, parts = {0}, {{field = 'id', type = ty, is_nullable = false}}
    if kind == 'composite' then
        cols[2].nullable, format[2].is_nullable = false, false
        pkcols, parts = {0, 1}, {parts[1], {field = 'v', type = 'integer', is_nullable = false}}
    end
    local doc = {db = 'kvunit', name = name, space_id = sid, columns = cols,
        indexes = {{name = kind == 'implicit' and 'unique_id' or 'PRIMARY', id = 0,
                    cols = pkcols, unique = true}},
        shard_key = {0}, auto_inc = box.NULL, comment = '', engine = engine,
        ver = inner or 0, bucket = 3}
    return {id = sid, name = 'kvunit.' .. name, engine = engine, ver = outer or 0,
        json = json.encode(doc), format = format, lazy = {},
        indexes = {{name = 'primary', unique = true, parts = parts},
                   {name = 'bucket_id', unique = false,
                    parts = {{field = 'bucket_id', type = 'unsigned', is_nullable = false}}}}}
end
local function create(def)
    ok(admin:call('tarn_create_table', {def}))
    box.schema.user.grant(names[1], 'read', 'space', def.id)
    box.schema.role.grant(role, 'read', 'space', def.id)
    return box.space[def.id]
end
local function meta(c, name)
    return c:call('tarn_kv_v1.meta', {{version = 1, db = 'kvunit', name = name}})
end
local function request(m, key, many)
    local r = {version = 1, consistency = 'independent_points',
               table = copy(m.table), ownership = copy(m.ownership)}
    r[many and 'keys' or 'key'] = copy(key); return r
end
local function error_result(label, r, kind, code)
    check(label, type(r) == 'table' and r.version == 1 and r.status == 'error'
        and type(r.error) == 'table' and r.error.kind == kind
        and (code == nil or r.error.code == code) and r.slots == nil
        and r.table == nil and r.definition_json == nil, r)
end
local function public(c, operation, r) return c:call('tarn_kv_v1.' .. operation, {r}) end
local function numeric_hole(label, first, second, fourth)
    -- Preserve a numeric-key MP_MAP through net.box; request()/copy() would
    -- discard its explicit serialization hint. This shape previously had
    -- #value == 4 while ipairs stopped after two supplied entries.
    local value = setmetatable({[1] = first, [2] = second, [4] = fourth}, {__serialize = 'map'})
    local encoded = mp.encode(value)
    local shape = admin:eval([[local v=...;local count,prefix=0,0
        for _ in pairs(v) do count=count+1 end
        for _ in ipairs(v) do prefix=prefix+1 end
        return {length=#v,count=count,prefix=prefix,hole=v[3]==nil}]], {value})
    check(label .. ' actual numeric-map hole spans Lua length without becoming a dense array',
        encoded:byte(1) == 0x83 and shape.length == 4 and shape.count == 3
        and shape.prefix == 2 and shape.hole, shape)
    return value
end
local function present(label, r, id, value)
    local s = r.slots and r.slots[1]
    check(label, r.status == 'ok' and r.consistency == 'independent_points' and #r.slots == 1
        and s.position == 1 and s.status == 'present' and s.key[1] == id
        and tuple(s.tuple)[1] == id and tuple(s.tuple)[2] == value, r)
end
local function future_result(f)
    local rows, err = f:wait_result(5); assert(rows, tostring(err))
    return rows.status and rows or rows[1]
end
local function held_scan(c, req)
    ok(admin:call('tarn_test_hook', {'scan', 'hold'})); held = true
    local f = c:call('tarn_kv_v1.get_many', {req}, {is_async = true})
    local reached = admin:call('tarn_test_hook', {'scan', 'reached'})
    assert(reached[1] == 'ok' and reached[2] > 0 and not f:is_ready(), json.encode(reached))
    return f
end
local function release_scan()
    ok(admin:call('tarn_test_hook', {'scan', 'release'})); held = false
end
local function acl_denied(yes, err) return not yes and tonumber(err.code) == box.error.ACCESS_DENIED end

local success, exception = xpcall(function()
    event('scope', {native = native or '0', version = box.info.version,
        scope = 'Actual restricted network API, RF1 both engines; no SQL/RF3 or barrier-partition qualification'})
    for engine_index, engine in ipairs({'memtx', 'vinyl'}) do
        local sid, name = 98200 + engine_index * 100, 't_' .. engine
        local def = definition(sid, name, engine)
        local sp = create(def)
        sp:replace{1, 11, 'one', 1}; sp:replace{2, box.NULL, 'null', 1}
        box.schema.user.grant(names[3], 'write', 'space', sid)
        wait_for(healthy, engine .. ' fixture clock readiness')
        local m = meta(reader, name)
        check(engine .. ' restricted metadata has actual definition/layout and limits',
            m.status == 'ok' and m.version == 1 and m.table.sid == sid and m.table.ver == 0
            and m.definition_json == def.json and m.consistency == 'independent_points'
            and m.limits.max_keys == 128 and m.limits.max_key_bytes == 65536
            and m.limits.max_reply_bytes == 1048576 and m.limits.max_metadata_bytes == 1048576
            and m.format[1][1] == 'id' and m.primary[1][1] == 1 and m.shard_key_pk[1] == 1
            and m.ownership.bucket_count == 1, m)
        present(engine .. ' get returns exact present key/value', public(reader, 'get', request(m, {1})), 1, 11)
        local r = public(reader, 'get_many', request(m, {{1}, {99}, {2}, {1}}, true))
        check(engine .. ' full ordered vector retains duplicate and missing positions and present SQL NULL',
            r.status == 'ok' and #r.slots == 4 and r.slots[1].position == 1
            and r.slots[2].position == 2 and r.slots[2].key[1] == 99 and r.slots[2].status == 'missing'
            and r.slots[2].tuple == nil and r.slots[3].position == 3 and r.slots[3].status == 'present'
            and tuple(r.slots[3].tuple)[2] == box.NULL and r.slots[4].position == 4
            and r.slots[4].key[1] == 1 and tuple(r.slots[4].tuple)[2] == 11, r)
        present(engine .. ' inherited table READ role works', public(inherited, 'get', request(m, {1})), 1, 11)
        for _, c in ipairs({writer, none}) do
            error_result(engine .. ' no READ metadata remains protected ' .. tostring(c), meta(c, name), 'access_denied')
            for _, key in ipairs({1, 99}) do
                error_result(engine .. ' unauthorized hit/miss same read guard ' .. tostring(key),
                    public(c, 'get', request(m, {key})), 'access_denied')
            end
        end
        local eval_ok, eval_error = pcall(reader.eval, reader, 'return 1')
        check(engine .. ' reader has no EVAL privilege', acl_denied(eval_ok, eval_error), tostring(eval_error))
        assert(reader.space[sid] ~= nil, 'raw DML check requires a real space descriptor')
        local raw_ok, raw_error = pcall(reader.space[sid].replace, reader.space[sid], {1, 99, 'forbidden', 1})
        check(engine .. ' raw native mutation is denied before effects',
            acl_denied(raw_ok, raw_error) and sp:get{1}[2] == 11, tostring(raw_error))
        local internal_ok, internal_error = pcall(reader.call, reader, 'tarn_autocommit',
            {{{'replace', sid, {1, 99, 'forbidden', 1}}}})
        check(engine .. ' no internal mutation CALL grant',
            acl_denied(internal_ok, internal_error) and sp:get{1}[2] == 11, tostring(internal_error))
        if native and native ~= '0' then
            local n_ok, n_error = pcall(reader.call, reader, 'tarn_storage.ts_autocommit',
                {{{'replace', sid, {1, 99, 'forbidden', 1}}}})
            check(engine .. ' no native mutation procedure grant',
                acl_denied(n_ok, n_error) and sp:get{1}[2] == 11, tostring(n_error))
        end

        for _, mode in ipairs({'atomic', 'same_shard_snapshot', 'cross_shard_snapshot'}) do
            local bad = request(m, {{1}, {2}}, true); bad.consistency = mode
            error_result(engine .. ' rejects unsupported consistency ' .. mode,
                public(reader, 'get_many', bad), 'unsupported')
        end
        for _, field in ipairs({'stream', 'begin', 'index', 'after', 'write'}) do
            local bad = request(m, {1}); bad[field] = true
            error_result(engine .. ' unknown public knob ' .. field, public(reader, 'get', bad), 'bad_request')
        end
        error_result(engine .. ' extra positional get arg is rejected',
            reader:call('tarn_kv_v1.get', {request(m, {1}), 7}), 'bad_request')
        error_result(engine .. ' extra positional meta arg is rejected',
            reader:call('tarn_kv_v1.meta', {{version = 1, db = 'kvunit', name = name}, 7}), 'bad_request')
        error_result(engine .. ' extra positional get_many arg is rejected',
            reader:call('tarn_kv_v1.get_many', {request(m, {{1}}, true), 7}), 'bad_request')
        error_result(engine .. ' missing CALL argument is rejected', reader:call('tarn_kv_v1.get', {}), 'bad_request')
        error_result(engine .. ' wrong metadata identifier type is rejected',
            meta(reader, {not_a_name = true}), 'bad_request')
        error_result(engine .. ' unknown metadata map field is rejected', reader:call('tarn_kv_v1.meta',
            {{version = 1, db = 'kvunit', name = name, extra = true}}), 'bad_request')
        for _, bad_key in ipairs({{}, {1, 2}, {'1'}, {1.5}, {false}, {box.NULL}, {{nested = true}}}) do
            error_result(engine .. ' invalid complete native key shape/type',
                public(reader, 'get', request(m, bad_key)), 'bad_request')
        end
        local nested = {{string.rep('n', 131072)}}
        error_result(engine .. ' giant nested later key is typed error with no prefix',
            public(reader, 'get_many', request(m, {{1}, {nested}}, true)), 'bad_request')
        local bad = request(m, {1}); bad.version = '1'
        error_result(engine .. ' wrong version type is unsupported', public(reader, 'get', bad), 'unsupported')
        bad = request(m, {1}); bad.table.ver = '0'
        error_result(engine .. ' wrong table token field type', public(reader, 'get', bad), 'bad_request')
        bad = request(m, {1}); bad.table.definition = string.rep('A', 64)
        error_result(engine .. ' noncanonical hash shape', public(reader, 'get', bad), 'bad_request')
        bad = request(m, {99}); bad.table.sid = bad.table.sid + 1
        error_result(engine .. ' forged physical SID cannot certify a missing row',
            public(reader, 'get', bad), 'schema_changed', 1412)
        bad = request(m, {1}); bad.ownership.shard_id = '1'
        error_result(engine .. ' wrong ownership field type is rejected', public(reader, 'get', bad), 'bad_request')
        for _, field in ipairs({'definition', 'layout'}) do
            bad = request(m, {1}); bad.table[field] = string.rep('0', 64)
            error_result(engine .. ' forged frozen ' .. field, public(reader, 'get', bad), 'schema_changed', 1412)
        end
        for _, field in ipairs({'shard_id', 'bucket_count', 'bucket_epoch', 'membership_version'}) do
            bad = request(m, {99}); bad.ownership[field] = bad.ownership[field] + 1
            error_result(engine .. ' forged ownership ' .. field .. ' cannot certify missing',
                public(reader, 'get', bad), 'moved')
        end
        error_result(engine .. ' empty get_many is not a complete batch',
            public(reader, 'get_many', request(m, {}, true)), 'bad_request')
        local sparse = request(m, {{1}}, true)
        sparse.keys = numeric_hole(engine .. ' outer batch', {1}, {2}, {99})
        error_result(engine .. ' sparse numeric-map batch rejects whole vector rather than returning a clipped prefix',
            public(reader, 'get_many', sparse), 'bad_request')
        present(engine .. ' dense point still succeeds after sparse batch refusal',
            public(reader, 'get', request(m, {1})), 1, 11)
        local keys = {}; for i = 1, 128 do keys[i] = {1000 + i} end
        r = public(reader, 'get_many', request(m, keys, true))
        local exact = r.status == 'ok' and #r.slots == 128
        if exact then for i, s in ipairs(r.slots) do
            exact = exact and s.position == i and s.status == 'missing' and s.key[1] == keys[i][1]
        end end
        check(engine .. ' exact 128 all-miss positions consume bounded work', exact, r)
        keys[129] = {1129}
        error_result(engine .. ' 129 all-miss keys reject whole request',
            public(reader, 'get_many', request(m, keys, true)), 'limit_exceeded')
        for i = 1, 129 do keys[i] = {1} end
        error_result(engine .. ' duplicate keys cannot evade 128 work cap',
            public(reader, 'get_many', request(m, keys, true)), 'limit_exceeded')

        local string_name = 's_' .. engine
        local strings = create(definition(sid + 1, string_name, engine, 'string'))
        strings:replace{'key', 21, 'string', 1}
        local sm = meta(reader, string_name)
        present(engine .. ' string primary key is supported', public(reader, 'get', request(sm, {'key'})), 'key', 21)
        local boundary = string.rep('k', 65531)
        assert(#mp.encode({{boundary}}) == 65536, 'wire fixture must equal the public key-byte limit')
        r = public(reader, 'get', request(sm, {boundary}))
        check(engine .. ' exact encoded 64KiB key boundary is a complete miss',
            r.status == 'ok' and r.slots[1].status == 'missing' and r.slots[1].key[1] == boundary)
        error_result(engine .. ' encoded key-byte overflow rejects whole batch',
            public(reader, 'get_many', request(sm, {{'key'}, {boundary .. 'k'}}, true)), 'limit_exceeded')
        error_result(engine .. ' giant native string is bounded before encoding',
            public(reader, 'get', request(sm, {string.rep('g', 131072)})), 'limit_exceeded')

        local bins = create(definition(sid + 2, 'b_' .. engine, engine, 'binary'))
        local bk = binary.new('a\0b'); bins:replace{bk, 22, 'binary', 1}
        local bm = meta(reader, 'b_' .. engine)
        r = public(reader, 'get', request(bm, {bk}))
        check(engine .. ' MP_BIN key and echo preserve binary identity',
            r.status == 'ok' and r.slots[1].status == 'present' and binary.is(r.slots[1].key[1])
            and tostring(r.slots[1].key[1]) == tostring(bk) and tuple(r.slots[1].tuple)[2] == 22)
        error_result(engine .. ' MP_STR is not silently reinterpreted as MP_BIN',
            public(reader, 'get', request(bm, {'a\0b'})), 'bad_request')
        error_result(engine .. ' giant native binary key is bounded',
            public(reader, 'get', request(bm, {binary.new(string.rep('g', 131072))})), 'limit_exceeded')
        local composite = create(definition(sid + 3, 'c_' .. engine, engine, 'composite'))
        composite:replace{7, 8, 'composite', 1}
        local cm = meta(reader, 'c_' .. engine)
        r = public(reader, 'get', request(cm, {7, 8}))
        check(engine .. ' complete composite primary key works', r.status == 'ok'
            and #r.slots[1].key == 2 and tuple(r.slots[1].tuple)[3] == 'composite', r)
        error_result(engine .. ' primary prefix is unsupported as an incomplete key',
            public(reader, 'get', request(cm, {7})), 'bad_request')
        -- Four-part PK makes the sparse inner map's Lua length match the
        -- declared arity, so an arity-only check cannot reject this case.
        local hole_def = definition(sid + 7, 'h_' .. engine, engine, 'composite')
        local hole_doc = json.decode(hole_def.json)
        hole_doc.columns[3].ty, hole_def.format[3].type = 'BIGINT', 'integer'
        hole_doc.columns[4] = column('tail', 'BIGINT', false, 4)
        hole_doc.indexes[1].cols = {0, 1, 2, 3}
        hole_def.format[5] = {name = 'tail', type = 'integer', is_nullable = false}
        hole_def.indexes[1].parts[3] = {field = 'payload', type = 'integer', is_nullable = false}
        hole_def.indexes[1].parts[4] = {field = 'tail', type = 'integer', is_nullable = false}
        hole_def.json = json.encode(hole_doc)
        local hole_space = create(hole_def); hole_space:replace{7, 8, 9, 1, 10}
        local hm = meta(reader, 'h_' .. engine)
        sparse = request(hm, {7, 8, 9, 10})
        sparse.key = numeric_hole(engine .. ' inner PK', 7, 8, 10)
        error_result(engine .. ' sparse numeric-map full-arity PK is bad_request without any slots',
            public(reader, 'get', sparse), 'bad_request')
        r = public(reader, 'get', request(hm, {7, 8, 9, 10}))
        check(engine .. ' complete four-part PK succeeds after sparse inner refusal',
            r.status == 'ok' and #r.slots == 1 and #r.slots[1].key == 4
            and r.slots[1].status == 'present' and tuple(r.slots[1].tuple)[5] == 10, r)
        create(definition(sid + 4, 'n_' .. engine, engine, 'number'))
        error_result(engine .. ' floating primary key metadata is explicitly unsupported',
            meta(reader, 'n_' .. engine), 'unsupported')
        create(definition(sid + 5, 'i_' .. engine, engine, 'implicit'))
        error_result(engine .. ' implicit non-PRIMARY key metadata is unsupported',
            meta(reader, 'i_' .. engine), 'unsupported')
        create(definition(sid + 6, 'v_' .. engine, engine, nil, 900, 7))
        local vm = meta(reader, 'v_' .. engine)
        check(engine .. ' outer catalog version and JSON provenance remain independently frozen',
            vm.status == 'ok' and vm.table.ver == 900 and json.decode(vm.definition_json).ver == 7, vm)

        -- These yields are the ACTUAL guarded scan hook, not a replacement
        -- freshness/ACL predicate. RF3 barrier yielding is tested separately.
        for _, who in ipairs({'direct', 'role'}) do
            local c = who == 'direct' and reader or inherited
            local f = held_scan(c, request(m, {{1}, {99}}, true))
            if who == 'direct' then box.schema.user.revoke(names[1], 'read', 'space', sid)
            else box.schema.role.revoke(role, 'read', 'space', sid) end
            release_scan()
            error_result(engine .. ' actual READ revocation after scan ' .. who,
                future_result(f), 'access_denied')
            if who == 'direct' then box.schema.user.grant(names[1], 'read', 'space', sid)
            else box.schema.role.grant(role, 'read', 'space', sid) end
            present(engine .. ' positive same handle after READ regrant ' .. who,
                public(c, 'get', request(m, {1})), 1, 11)
        end

        local tx = 'kv-unit-missing-' .. engine
        local vote = admin:call('tarn_prepare', {tx, 1, {1},
            {{'insert', sid, {90, 90, 'prepared', 1}}}, {}, {}, require('clock').realtime(),
            box.NULL, {sid, 0}, {1}})
        assert(vote[1] == 'yes', json.encode(vote)); prepared[tx] = true
        check(engine .. ' real durable PREPARE owns the currently missing key',
            box.space._tarn_part:get{tx}.state == 'prepared' and sp:get{90} == nil)
        r = public(reader, 'get_many', request(m, {{1}, {90}}, true))
        error_result(engine .. ' missing-key prepared claim prevents complete partial vector', r, 'unavailable', 1213)
        check(engine .. ' refusal reached the ordinary prepared-lock domain', r.error.domain == 'mysql', r)
        admin:call('tarn_txn_cas', {tx, 'staging', 'aborted', {1}})
        ok(admin:call('tarn_abort', {tx, 1, {1}})); prepared[tx] = nil
        r = public(reader, 'get_many', request(m, {{1}, {90}}, true))
        check(engine .. ' abort releases missing claim and normal complete vector succeeds',
            r.status == 'ok' and #r.slots == 2 and r.slots[1].status == 'present'
            and r.slots[2].status == 'missing', r)

        sp:replace{3, 33, string.rep('x', 1048576), 1}
        error_result(engine .. ' oversized final tuple exposes no successful prefix',
            public(reader, 'get_many', request(m, {{1}, {3}}, true)), 'limit_exceeded')
        error_result(engine .. ' single oversized tuple cannot bypass reply cap',
            public(reader, 'get', request(m, {3})), 'limit_exceeded')

        local stream = admin:new_stream(); stream:begin()
        local id = stream:eval('return tostring(box.txn_id())')
        error_result(engine .. ' ambient refusal precedes even malformed request',
            stream:call('tarn_kv_v1.get', {{invalid = true}}), 'ambient_transaction')
        check(engine .. ' ambient refusal preserves exact caller transaction',
            stream:eval('return tostring(box.txn_id())') == id)
        stream:rollback()
        stream:eval('tarn.fresh.begin_stream();box.space[...]:get{1}', {sid})
        id = stream:eval('return tostring(box.txn_id())')
        error_result(engine .. ' marked old stream is also refused without late refresh',
            stream:call('tarn_kv_v1.get', {request(m, {1})}), 'ambient_transaction')
        check(engine .. ' marked stream transaction identity survives refusal',
            stream:eval('return tostring(box.txn_id())') == id)
        sp:replace{1, 12, 'updated', 1}
        local intact = pcall(stream.eval, stream, 'box.space[...]:replace{91,91,"forbidden-old-view",1}', {sid})
        if intact then intact = pcall(stream.commit, stream) end
        check(engine .. ' original MVCC read history still rejects a later stale write', not intact and sp:get{91} == nil)
        pcall(stream.rollback, stream)
        present(engine .. ' fresh public request sees acknowledged replacement after old-stream refusal',
            public(reader, 'get', request(m, {1})), 1, 12)

        -- URI metadata fixtures do not connect to these configured strings.
        -- Plain authenticated TCP is sanitized; unsupported query/fragment
        -- forms must not disclose their test credentials through any reply.
        local peers = tarn.cfg.peers['1']
        tarn.cfg.peers['1'] = {'user-secret:password-secret@' .. address}
        r = meta(reader, name)
        local encoded = json.encode(r)
        check(engine .. ' metadata keeps TCP endpoint but removes URI credentials',
            r.status == 'ok' and encoded:find(address, 1, true) ~= nil
            and not encoded:find('user-secret', 1, true) and not encoded:find('password-secret', 1, true), r)
        for _, suffix in ipairs({'?ssl_password=query-secret', '#fragment-secret'}) do
            tarn.cfg.peers['1'] = {'user-secret:password-secret@' .. address .. suffix}
            r = meta(reader, name); encoded = json.encode(r)
            error_result(engine .. ' unsupported secret-bearing URI is not exported', r, 'unsupported')
            check(engine .. ' query/fragment and credentials absent even from error',
                not encoded:find('user-secret', 1, true) and not encoded:find('password-secret', 1, true)
                and not encoded:find('query-secret', 1, true) and not encoded:find('fragment-secret', 1, true), r)
        end
        tarn.cfg.peers['1'] = peers

        -- Local bucket-protocol roundtrip, not a full distributed move.
        -- The supported RPCs update both durable state and the live cache;
        -- raw _tarn_buckets replacement is not a cache invalidation API.
        -- One bucket avoids copying a CRC implementation into this oracle.
        local function bucket_rows()
            local rows, after = {}, box.NULL
            repeat
                local page = ok(admin:call('tarn_bucket_rows', {1, after}))
                for _, row in ipairs(page[2]) do rows[#rows + 1] = row end
                after = page[3]
            until after == nil or after == box.NULL
            return rows
        end
        local function image(rows)
            local encoded = {}
            for _, row in ipairs(rows) do encoded[#encoded + 1] = mp.encode(row) end
            table.sort(encoded)
            return digest.sha256_hex(mp.encode(encoded))
        end
        check(engine .. ' local roundtrip has no remote lazy-transport prerequisites',
            box.space._tarn_lazy_defs:len() == 0 and box.space._tarn_lazy_counter:len() == 0
            and box.space._tarn_lazy_floor:len() == 0)
        local old_report = ok(admin:call('tarn_buckets'))
        local rows = bucket_rows()
        local before_image, send = image(rows), 'kv-unit-send-' .. engine
        ok(admin:call('tarn_bucket_send_begin', {1, 2, send, 1}))
        ok(admin:call('tarn_bucket_fence', {1, send, 1}))
        ok(admin:call('tarn_bucket_send', {1, send}))
        local sent_report = ok(admin:call('tarn_buckets'))
        check(engine .. ' supported send publishes actual unowned state and newer epoch',
            sent_report[2] == 1 and sent_report[3] > old_report[3] and #sent_report[4] == 0
            and #sent_report[5] == 1 and sent_report[5][1][1] == 1
            and sent_report[5][1][2] == 'sent' and sent_report[5][1][3] == 2,
            {before = old_report, sent = sent_report})
        local moved = meta(reader, name); assert(moved.status == 'ok', json.encode(moved))
        check(engine .. ' new token observes the actual sent cache before the missing-key oracle',
            moved.ownership.bucket_epoch == sent_report[3] and #moved.buckets.owned == 0, moved)
        r = public(reader, 'get', request(moved, {99}))
        error_result(engine .. ' current token still rejects a key whose recomputed bucket is unowned', r, 'moved', 1213)
        check(engine .. ' unowned-key refusal is actual bucket guard, not token mismatch',
            r.error.domain == 'mysql' and r.error.message:find('bucket 1', 1, true) ~= nil, r)
        local receive = 'kv-unit-receive-' .. engine
        ok(admin:call('tarn_bucket_recv_begin', {1, 2, receive}))
        ok(admin:call('tarn_bucket_recv_rows', {1, receive, rows}))
        ok(admin:call('tarn_bucket_hold', {1, receive}))
        ok(admin:call('tarn_bucket_recv', {1, receive}))
        local active_report = ok(admin:call('tarn_buckets'))
        check(engine .. ' supported receive restores ownership and exact whole bucket image',
            active_report[3] > sent_report[3] and #active_report[4] == 1
            and active_report[4][1][1] == 1 and active_report[4][1][2] == 1
            and image(bucket_rows()) == before_image,
            {active = active_report, row_count = #rows, image_sha256 = before_image})
        m = meta(reader, name)
        r = public(reader, 'get', request(m, {99}))
        check(engine .. ' actual owned-map restore permits a normal missing result',
            r.status == 'ok' and r.slots[1].status == 'missing', r)

        local clear_def = copy(def); clear_def.ver = def.ver + 1
        local clear = {id = sid, ver = clear_def.ver, timeout = .2, def = clear_def}
        ok(admin:call('tarn_truncate_prepare', {def.name, clear}))
        error_result(engine .. ' real persisted DDL hold/fence makes frozen handle terminal',
            public(reader, 'get', request(m, {1})), 'schema_changed', 1412)
        ok(admin:call('tarn_truncate_abort', {def.name, clear}))
        present(engine .. ' aborted DDL restores ordinary guarded read',
            public(reader, 'get', request(m, {1})), 1, 12)

        -- STORAGE-UNIT catalog/format evolution with a valid next SQL JSON.
        local next_def, document = copy(def), json.decode(def.json)
        next_def.ver, document.ver, document.columns[2].name = 1, 1, 'amount'
        next_def.format[2].name = 'amount'; next_def.json = json.encode(document)
        sp:format(next_def.format)
        box.space._tarn_catalog:replace{def.name, next_def}; tarn.stale_fences()
        error_result(engine .. ' actual catalog/format change invalidates old frozen handle',
            public(reader, 'get_many', request(m, {{1}, {99}}, true)), 'schema_changed', 1412)
        local changed = meta(reader, name)
        check(engine .. ' explicit new handle exposes new field names and same stored row',
            changed.status == 'ok' and changed.table.ver == 1 and changed.format[2][1] == 'amount'
            and json.decode(changed.definition_json).columns[2].name == 'amount', changed)
        present(engine .. ' caller may explicitly resubmit compatible keys under new handle',
            public(reader, 'get', request(changed, {1})), 1, 12)

        -- STORAGE-UNIT drop/recreate ABA: identical public name/schema shape,
        -- a different physical identity, and different actual row data.
        box.space._tarn_catalog:delete{def.name}; sp:drop()
        local replacement = definition(sid + 20, name, engine)
        local recreated = create(replacement); recreated:replace{1, 77, 'new-incarnation', 1}
        tarn.stale_fences()
        error_result(engine .. ' old physical incarnation never reads recreated table rows',
            public(reader, 'get', request(changed, {1})), 'schema_changed', 1412)
        local fresh = meta(reader, name)
        check(engine .. ' explicit reopened table binds the new incarnation',
            fresh.status == 'ok' and fresh.table.sid == sid + 20 and fresh.table.sid ~= changed.table.sid, fresh)
        present(engine .. ' explicit new incarnation returns its own row data',
            public(reader, 'get', request(fresh, {1})), 1, 77)
    end
end, debug.traceback)

-- Only this isolated process's hooks, captured protocol receipts and sockets.
tarn.cfg.peers['1'] = initial_peers
if held then pcall(admin.call, admin, 'tarn_test_hook', {'scan', 'release'}) end
for tx in pairs(prepared) do
    pcall(admin.call, admin, 'tarn_txn_cas', {tx, 'staging', 'aborted', {1}})
    pcall(admin.call, admin, 'tarn_abort', {tx, 1, {1}})
end
for _, c in ipairs(connections) do pcall(c.close, c) end
if not success then failed = failed + 1; event('failed', {traceback = tostring(exception)}); print('  FAIL ' .. tostring(exception)) end
print(string.format('%d passed, %d failed (KV v1 actual restricted network storage-unit, memtx+vinyl)', passed, failed))
os.exit(failed == 0 and 0 or 1)
