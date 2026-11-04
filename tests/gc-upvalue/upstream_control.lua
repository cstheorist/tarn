-- Ordinary functional reproducer by corsix, LuaJIT/LuaJIT#1085 (2023-09-18):
-- https://github.com/LuaJIT/LuaJIT/issues/1085
-- Keep the workload free of receipt accounting and threshold tuning.
local json, ffi, jit = require('json'), require('ffi'), require('jit')
io.stdout:setvbuf('no')
local function jit_status()
    local flags = {jit.status()}
    return {enabled = table.remove(flags, 1), flags = flags}
end
assert(arg[2] == nil and (arg[1] == 'on' or arg[1] == 'off'), 'expected on|off')
local identities = assert(os.getenv('TARN_GC_SOURCE_IDENTITIES_JSON'))
assert(identities:match('^%s*{'), 'expected identity object')
identities = json.decode(identities)
assert(type(identities) == 'table' and next(identities), 'empty identities')
local initial_jit = jit_status()
if arg[1] == 'on' then jit.on() else jit.off() end
local runtime = {tarantool = tostring(_TARANTOOL), luajit = jit.version,
    os = jit.os, arch = jit.arch, gc64 = ffi.abi('gc64'),
    initial_jit = initial_jit, effective_jit = jit_status()}
assert(runtime.effective_jit.enabled == (arg[1] == 'on'), 'JIT profile mismatch')
local config = {blocks = 3, stores_per_block = 100, total_stores = 300,
    gc = 'incremental step(1) after each store'}
local function emit(record)
    record.schema_version = 1
    record.fixture = 'luajit-1085-functional-control'
    record.profile, record.source_identities = arg[1], identities
    record.runtime, record.config = runtime, config
    print(json.encode(record))
end
-- Flushed preflight precedes the workload, including closure construction.
emit({kind = 'preflight'})
local nodes_checked = 0
local function workload()
    local set, get
    local function f()
        for i = 1, 100 do
            set({get()})
            collectgarbage('step', 1)
        end
    end
    do
        local uv
        set = function(v) uv = v end
        get = function() return uv end
        f() -- uv is open during trace recording
    end
    f();f() -- but now uv is closed

    -- Only after the published workload: exactly 300 plain one-link nodes,
    -- ending in the empty table created by the first {nil} store.
    local node = get()
    for i = 1, 300 do
        assert(type(node) == 'table' and getmetatable(node) == nil, 'invalid chain node')
        local fields = 0
        for key in pairs(node) do
            assert(key == 1, 'unexpected chain field')
            fields = fields + 1
        end
        assert(fields == (i == 300 and 0 or 1), 'chain node shape mismatch')
        node = node[1]
        nodes_checked = nodes_checked + 1
    end
    assert(node == nil, 'chain does not end at exactly 300 nodes')
end
local ok, err = pcall(workload)
local final_jit = jit_status()
emit({kind = 'completion', complete = true, passed = ok,
    nodes_checked = nodes_checked, final_jit = final_jit,
    error = not ok and tostring(err) or nil})
os.exit(ok and 0 or 1)
