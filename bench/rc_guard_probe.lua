-- Run with tarantool and RC186_ADDRESS=tarn:tarn@<owned fixture endpoint>.
-- Helper-only allocation census: no data writes, cached guards or barrier elision.
local fiber = require('fiber')
local json = require('json')
local conn = require('net.box').connect(assert(os.getenv('RC186_ADDRESS')), {wait_connected = 5})
assert(conn:is_connected())
local result = conn:eval([[
local fiber, clock, misc = require('fiber'), require('clock'), require('misc')
local fresh = require('tarn').fresh
assert(fresh.mode() == 2 and fresh.controls() == 2)
local cases = {
    {'empty', function() return 2 end},
    {'replication_info', function() return box.info.replication end},
    {'mode', fresh.mode},
    {'controls', fresh.controls},
    {'native_controls', fresh.native_controls},
}
local result = {version = box.info.version, wal = box.cfg.wal_mode,
    quorum = box.info.synchro.quorum, mode = fresh.mode(), native = tarn_info().native,
    samples = {}, iterations = 1000, yield_every = 100,
    scope = 'helper calls only; metrics include background work at cooperative yields; not end-to-end attribution'}
for repeat_id = 0, 4 do
    for offset = 0, #cases - 1 do
        local item = cases[(offset + repeat_id) % #cases + 1]
        for i = 1, 100 do assert(item[2]() ~= 0) end
        fiber.yield()
        local before = misc.getmetrics()
        local start = clock.monotonic()
        for i = 1, result.iterations do
            assert(item[2]() ~= 0)
            if i % result.yield_every == 0 then fiber.yield() end
        end
        local elapsed = clock.monotonic() - start
        local after = misc.getmetrics()
        result.samples[#result.samples + 1] = {repeat_id = repeat_id, name = item[1],
            elapsed_seconds = elapsed, lua_allocated = after.gc_allocated - before.gc_allocated,
            lua_freed = after.gc_freed - before.gc_freed}
    end
end
assert(fresh.controls() == 2)
return result
]])
conn:close()
print(json.encode(result))
os.exit(0)
