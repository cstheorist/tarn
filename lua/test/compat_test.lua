-- Unknown disk formats must be refused before schema/recovery initialization.
local fio = require('fio')
local here = fio.abspath(debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './') .. '/'
package.path = here .. '../?.lua;' .. package.path
fio.mktree(assert(arg[1]))
box.cfg{work_dir = arg[1], memtx_use_mvcc_engine = true, log_level = 3}
local meta = box.schema.space.create('_tarn_meta', {format = {{'key', 'string'}, {'value', 'any'}}})
meta:create_index('pk', {parts = {'key'}})
meta:replace{'storage_format', 2}
local tarn = require('tarn')
local ok, err = pcall(tarn.init, {shard_id = 1})
assert(not ok and tostring(err):find('unsupported persisted storage_format', 1, true), tostring(err))
assert(box.space._tarn_catalog == nil and rawget(_G, 'tarn') == nil)
assert(meta:get{'storage_format'}.value == 2)
meta:replace{'storage_format', '1'}
assert(not pcall(tarn.init, {shard_id = 1}), 'coerced format must not be admitted')
meta:delete{'storage_format'}
tarn.init{shard_id = 1}
assert(tarn_compatibility()[3] == 1 and meta:get{'storage_format'}.value == 1)
assert(tarn_compatibility()[7] == 'disabled')
print('compatibility startup/legacy-format tests passed')
os.exit(0)
