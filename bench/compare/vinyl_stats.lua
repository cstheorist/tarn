-- Vinyl's sizes on Tarn storage nodes, appended to a CSV file; with
-- snapshot = 1, each node checkpoints first, so that vinyl's in-memory level
-- goes to disk.
--
--   tarantool bench/compare/vinyl_stats.lua CSV LABEL SNAPSHOT HOST:PORT...
local fio = require('fio')
local netbox = require('net.box')

local csv, label, snapshot = arg[1], arg[2], arg[3] == '1'
local new = not fio.path.exists(csv)
local f = assert(io.open(csv, 'a'))
if new then
    f:write('label,node,data_mb,index_mb,cache_limit_mb,cache_mb,level0_mb,page_index_mb,bloom_mb\n')
end
local MB = 1024 * 1024
local status = 0
for i = 4, #arg do
    local c = netbox.connect('tarn:tarn@' .. arg[i], {wait_connected = 10})
    local ok, r = pcall(c.eval, c, [[
        if ... then box.snapshot() end
        local v = box.stat.vinyl()
        return {v.disk.data, v.disk.index, box.cfg.vinyl_cache, v.memory.tuple_cache,
                v.memory.level0, v.memory.page_index, v.memory.bloom_filter}
    ]], {snapshot}, {timeout = 600})
    if ok then
        f:write(string.format('%s,%s,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f\n', label, arg[i],
            r[1] / MB, r[2] / MB, r[3] / MB, r[4] / MB, r[5] / MB, r[6] / MB, r[7] / MB))
    else
        io.stderr:write(arg[i] .. ': ' .. tostring(r) .. '\n')
        status = 1
    end
    c:close()
end
f:close()
os.exit(status)
