-- Tarantool instance used by tarn-iproto's integration tests and bench.
-- Started with the working directory set to a fresh temp dir; writes the
-- bound address to listen.txt once everything below is ready.
local fiber = require('fiber')
local ffi = require('ffi')

box.cfg{
    listen = '127.0.0.1:0',
    memtx_use_mvcc_engine = true,
    memtx_memory = 256 * 1024 * 1024,
    memtx_max_tuple_size = 16 * 1024 * 1024,
    log = 'tarantool.log',
    checkpoint_interval = 0,
}

box.schema.user.create('tarn', {password = 'tarn', if_not_exists = true})
box.schema.user.grant('tarn', 'super', nil, nil, {if_not_exists = true})

-- Plain DML target.
local test = box.schema.space.create('test', {id = 512, if_not_exists = true})
test:create_index('pk', {parts = {{1, 'unsigned'}}, if_not_exists = true})

-- Iterator tests: {i, i % 3, 'v<i>'} for i = 1..10, non-unique index on field 2.
local iter = box.schema.space.create('iter', {id = 513, if_not_exists = true})
iter:create_index('pk', {parts = {{1, 'unsigned'}}, if_not_exists = true})
iter:create_index('sk', {parts = {{2, 'unsigned'}}, unique = false, if_not_exists = true})
for i = 1, 10 do iter:replace{i, i % 3, 'v' .. i} end

-- Value round trips: field 2 has no type constraint.
local vals = box.schema.space.create('vals', {id = 514, if_not_exists = true})
vals:create_index('pk', {parts = {{1, 'unsigned'}}, if_not_exists = true})

-- Pipelining / bench: {i, 2 * i, 'v<i>'} for i = 1..KV_ROWS.
local kv = box.schema.space.create('kv', {id = 515, if_not_exists = true})
kv:create_index('pk', {parts = {{1, 'unsigned'}}, if_not_exists = true})
local rows = tonumber(os.getenv('KV_ROWS') or '1000')
box.begin()
for i = 1, rows do
    kv:replace{i, 2 * i, 'v' .. i}
    if i % 10000 == 0 then box.commit() box.begin() end
end
box.commit()

-- Functions reachable through IPROTO_CALL (global Lua functions).
function multi() return 1, 'two', {3, 4}, box.tuple.new{5, 'six'} end
function echo(...) return ... end
function fail(msg) error(msg) end
function fail_box() box.error{code = 1234, reason = 'custom failure'} end
function nap(t) fiber.sleep(t) return t end
function iter_rows() return box.space.iter:select{} end
function vals_tostring(id) return tostring(box.space.vals:get(id)[2]) end
function vals_type(id)
    local v = box.space.vals:get(id)[2]
    if require('decimal').is_decimal(v) then return 'decimal' end
    if require('datetime').is_datetime(v) then return 'datetime' end
    return type(v)
end
function vals_decimal_eq(id, s) return box.space.vals:get(id)[2] == require('decimal').new(s) end

-- Exit when the test process that started us goes away.
ffi.cdef('int getppid(void);')
local parent = ffi.C.getppid()
fiber.create(function()
    while true do
        fiber.sleep(0.5)
        if ffi.C.getppid() ~= parent then os.exit(0) end
    end
end)

local f = io.open('listen.tmp', 'w')
f:write(tostring(box.info.listen))
f:close()
os.rename('listen.tmp', 'listen.txt')
