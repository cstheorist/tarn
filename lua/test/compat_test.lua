-- Unknown disk formats must be refused before schema/recovery initialization.
local fio = require('fio')
local here = fio.abspath(debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './') .. '/'
package.path = here .. '../?.lua;' .. package.path
fio.mktree(assert(arg[1]))
box.cfg{work_dir = arg[1], memtx_use_mvcc_engine = true, log_level = 3}
local meta = box.schema.space.create('_tarn_meta', {format = {{'key', 'string'}, {'value', 'any'}}})
meta:create_index('pk', {parts = {'key'}})
meta:replace{'storage_format', 3}
local tarn = require('tarn')
local ok, err = pcall(tarn.init, {shard_id = 1})
assert(not ok and tostring(err):find('unsupported persisted storage_format', 1, true), tostring(err))
assert(box.space._tarn_catalog == nil and rawget(_G, 'tarn') == nil)
assert(meta:get{'storage_format'}.value == 3)
for _, value in ipairs({'1', false, 0, 4}) do
    meta:replace{'storage_format', value}
    assert(not pcall(tarn.init, {shard_id = 1}), 'unknown/coerced format must not be admitted')
end
meta:replace{'storage_format', 1}
meta:replace{'job:move:1', {phase='copy'}}
assert(not pcall(tarn.init, {shard_id=1}), 'legacy job must refuse migration')
assert(meta:get{'storage_format'}.value == 1 and box.space._tarn_catalog == nil)
meta:delete{'job:move:1'}
meta:delete{'storage_format'}
-- A crash after the marker but before DDL must resume safely. Refuse one
-- schema creation after observing the committed format marker.
local create = box.schema.space.create
box.schema.space.create = function(name, opts)
    if name == '_tarn_recv' then
        assert(meta:get{'storage_format'}.value == 2)
        error('injected migration interruption')
    end
    return create(name, opts)
end
assert(not pcall(tarn.init, {shard_id=1}))
assert(meta:get{'storage_format'}.value == 2 and box.space._tarn_move == nil)
box.schema.space.create = create
tarn.init{shard_id = 1}
assert(tarn_compatibility()[3] == 2 and meta:get{'storage_format'}.value == 2)
assert(tarn_compatibility()[7] == 'disabled')
print('compatibility startup/legacy-format tests passed')
os.exit(0)
