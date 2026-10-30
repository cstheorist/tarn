-- A direct allocation census, not an RF3 or end-to-end performance claim.
-- tarantool bench/rc_alloc_probe.lua WORKDIR LUA_DIR [ITERATIONS]
local fio, fiber, clock, misc = require('fio'), require('fiber'), require('clock'), require('misc')
local work, source = assert(arg[1]), assert(arg[2])
local iterations = tonumber(arg[3]) or 10000
assert(iterations >= 1 and iterations <= 100000)
fio.mktree(work)
package.path = source .. '/?.lua;' .. package.path
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 64 * 1048576,
        wal_mode = 'fsync', checkpoint_interval = 0, log_level = 3}
local tarn = require('tarn')
tarn.init{shard_id = 1, peers = {}, native = false}
fiber.sleep(.05)
local s = box.schema.space.create('rc_probe')
s:create_index('primary')
s:insert{1, 0}
local function one(n)
    local txid = 'probe-' .. n
    local r = tarn_lock(txid, {{s.id, 0, {1}}}, 'X', {read = true})
    assert(r[1] == 'ok' and r[3][1][1] == 1, require('json').encode(r))
    assert(tarn_unlock(txid)[1] == 'ok')
end
for n = 1, 1000 do one(n) end
collectgarbage('collect')
local original_cond, conditions = fiber.cond, 0
fiber.cond = function(...)
    conditions = conditions + 1
    return original_cond(...)
end
local before, started = misc.getmetrics(), clock.monotonic()
for n = 1, iterations do one(n) end
local elapsed, after = clock.monotonic() - started, misc.getmetrics()
fiber.cond = original_cond
assert(tarn_info().row_locks == nil or tarn_info().row_locks == 0)
print(require('json').encode({scope = 'RF1 direct memtx Lua lock/unlock allocation census',
    iterations = iterations, condition_allocations = conditions,
    lua_allocated_bytes = after.gc_allocated - before.gc_allocated,
    lua_freed_bytes = after.gc_freed - before.gc_freed,
    elapsed_seconds_instrumented = elapsed, runtime = box.info.version}))
os.exit(0)
