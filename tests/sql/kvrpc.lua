-- Test-only JSON bridge: exactly one authenticated, versioned network CALL.
-- No EVAL, administrator fallback, stream or mutation mode is available.
local ffi, json, net, varbinary = require('ffi'), require('json'), require('net.box'), require('varbinary')
local function file(path)
    local f = assert(io.open(path, 'rb')); local data = f:read('*a'); f:close(); return data
end
local function decode(v)
    if type(v) ~= 'table' then return v end
    local fields = 0; for _ in pairs(v) do fields = fields + 1 end
    if v.binary_hex ~= nil then
        assert(fields == 1 and type(v.binary_hex) == 'string', 'invalid binary tag')
        local s = v.binary_hex
        assert(#s % 2 == 0 and not s:find('[^0-9a-fA-F]'), 'invalid binary hex')
        return varbinary.new((s:gsub('..', function(x) return string.char(tonumber(x, 16)) end)))
    end
    if v.int64 ~= nil or v.uint64 ~= nil then
        local k = v.int64 ~= nil and 'int64' or 'uint64'
        assert(fields == 1 and type(v[k]) == 'string', 'invalid integer tag')
        return ffi.cast(k == 'int64' and 'int64_t' or 'uint64_t', assert(tonumber64(v[k])))
    end
    local out = setmetatable({}, getmetatable(v))
    for k, value in pairs(v) do out[k] = decode(value) end
    return out
end
local function encode(v)
    if v == box.NULL then return box.NULL end
    if varbinary.is(v) then
        return {binary_hex = (tostring(v):gsub('.', function(x) return string.format('%02x', x:byte()) end))}
    end
    if ffi.istype('uint64_t', v) then return {uint64 = tostring(v):gsub('ULL$', '')} end
    if ffi.istype('int64_t', v) then return {int64 = tostring(v):gsub('LL$', '')} end
    if box.tuple.is(v) then return encode(v:totable()) end
    if type(v) ~= 'table' then return v end
    local out = setmetatable({}, getmetatable(v))
    for k, value in pairs(v) do out[k] = encode(value) end
    return out
end
local allowed = {['tarn_kv_v1.meta'] = true, ['tarn_kv_v1.get'] = true, ['tarn_kv_v1.get_many'] = true}
assert(#arg == 4 and allowed[arg[3]], 'endpoint credentials-file versioned-procedure request-file required')
local credentials = json.decode(file(arg[2]))
assert(type(credentials.user) == 'string' and type(credentials.password) == 'string', 'explicit credentials required')
local request = decode(json.decode(file(arg[4])))
local c = net.connect(arg[1], {user = credentials.user, password = credentials.password, wait_connected = 3})
local ok, result = pcall(c.call, c, arg[3], {request}, {timeout = 7})
c:close()
if not ok then
    local code = type(result) == 'cdata' and tonumber(result.code) or 0
    print(json.encode{bridge_error = {domain = 'tarantool', code = code, message = tostring(result)}})
    os.exit(1)
end
print(json.encode(encode(result)))
os.exit(0)
