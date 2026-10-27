-- Native RENAME is durable before the catalog transaction; replay must
-- delete the ORIGINAL request-name key even when the space already moved.
local fio, fiber, ffi = require('fio'), require('fiber'), require('ffi')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local work, engine, stage = assert(arg[1]), assert(arg[2]), assert(arg[3])
fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'fsync',
        checkpoint_interval = 0, log_level = 3}
require('tarn').init{shard_id = 1, peers = {}, native = false, test_hooks = true, check_yields = true}
fiber.sleep(0.02)
local name, target, sid = 'test.bf70_rename_old', 'test.bf70_rename_new', 50120
local a
if stage == 'crash' then
    local format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
                    {name = 'bucket_id', type = 'unsigned'}}
    local indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}},
                     {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}}}
    assert(tarn_create_table{name = name, id = sid, engine = engine, format = format, indexes = indexes}[1] == 'ok')
    assert(tarn_autocommit({{'insert', sid, {1, 10, 1}}, {'insert', sid, {2, 20, 1}}})[1] == 'ok')
    format = table.deepcopy(format)
    format[4] = {name = 'c', type = 'integer', is_nullable = true}
    local parts = {{field = 4, type = 'integer', is_nullable = true}}
    indexes[3] = {name = 'c_idx', unique = false, parts = parts}
    a = {ver = 1, timeout = 5, materialize = true, old_width = 3, pad = {3, 3}, rename = target,
         checks = {{4, 'c', 'indexed', box.NULL, box.NULL, 3}}, builds = {{2, 'c_idx', false, parts}},
         drop = {}, renames = {}, format = format,
         def = {name = target, id = sid, engine = engine, format = format, indexes = indexes, json = '{}', ver = 1}}
    box.space._tarn_meta:replace{'test:rename_args', a}
    assert(tarn_alter_prepare(name, a, 1)[1] == 'ok')
    tarn_test_hook('alter_schema', 'hold')
    fiber.create(function() tarn_alter_apply(name, a, 1) end)
    assert(tarn_test_hook('alter_schema', 'reached')[2] == 1)
    assert(box.space[name] == nil and box.space[target].id == sid)
    assert(box.space._tarn_catalog:get{name} ~= nil and box.space._tarn_catalog:get{target} == nil)
    assert(box.space._tarn_alter_undo.index[0]:count{sid, 1} == 2)
    print('  ok   ' .. engine .. ': SIGKILL after native RENAME, before catalog publication')
    io.stdout:flush()
    pcall(ffi.cdef, 'int getpid(void);')
    pcall(ffi.cdef, 'int kill(int pid, int sig);')
    ffi.C.kill(ffi.C.getpid(), 9)
    error('SIGKILL unexpectedly returned')
elseif stage == 'finish' then
    a = assert(box.space._tarn_meta:get{'test:rename_args'}).value
    assert(box.space[name] == nil and box.space[target].id == sid)
    assert(box.space._tarn_catalog:get{name} ~= nil and box.space._tarn_catalog:get{target} == nil)
    local r = tarn_alter_apply(name, a, 2)
    assert(r[1] == 'ok')
    assert(box.space._tarn_catalog:get{name} == nil, 'replay left the original catalog key behind')
    assert(box.space._tarn_catalog:get{target}.def.id == sid)
    assert(box.space[target].index[2]:count{3} == 2)
    assert(box.space._tarn_alter_undo.index[0]:count{sid, 1} == 0)
    assert(tarn_alter_apply(name, a, 2)[1] == 'ok' and tarn_alter_abort(name, a, 1)[1] == 'ok')
    assert(box.space._tarn_catalog:get{name} == nil and #box.space[target]:get{1} == 4)
    print('  ok   ' .. engine .. ': APPLY replay publishes only the target catalog key and cleans undo')
else
    error('unknown rename restart stage ' .. stage)
end
os.exit(0)
