-- What a storage node holds of a table, for the DDL fault test:
--   tarantool tests/sql/spaceinfo.lua HOST:PORT SPACE_NAME
-- prints {"space": bool, "named": bool, "ver": n, "rows": n, "jobs": n, "held": n}:
-- the space exists, the catalog names it (with definition version ver), its
-- row count, the schema change jobs in progress (first shard) and the tables
-- held by a TRUNCATE or DROP.
local json = require('json')
local c = require('net.box').connect('tarn:tarn@' .. arg[1], {wait_connected = 5})
local r = c:eval([[
    local name = ...
    local sp = box.space[name]
    local cat = box.space._tarn_catalog:get{name}
    local jobs = #tarn_jobs()
    return {space = sp ~= nil, named = cat ~= nil, ver = cat and cat.def.ver or 0,
            rows = sp and sp:len() or 0, jobs = jobs, held = tarn_info().ddl_held}
]], {arg[2]})
print(json.encode(r))
os.exit(0)
