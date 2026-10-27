-- Actual SIGKILL/restart at committed backfill and undo chunk boundaries.
-- Invocation: tarantool FILE WORKDIR memtx|vinyl crash_pad|crash_undo|finish
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
local name, sid, count = 'test.bf70_restart', 50110, 260
local function journal() return box.space._tarn_alter_undo.index[0]:count{sid, 1} end
local function old_writer()
    return tarn_autocommit({{'update', sid, {1}, {{'+', 2, 1}}}}, {fence = {sid, 0}})
end
local function killed()
    io.stdout:flush()
    pcall(ffi.cdef, 'int getpid(void);')
    pcall(ffi.cdef, 'int kill(int pid, int sig);')
    ffi.C.kill(ffi.C.getpid(), 9)
    error('SIGKILL unexpectedly returned')
end
local a
if stage == 'crash_pad' then
    local format = {{name = 'id', type = 'unsigned'}, {name = 'v', type = 'integer'},
                    {name = 'bucket_id', type = 'unsigned'}}
    local indexes = {{name = 'primary', unique = true, parts = {{field = 1, type = 'unsigned'}}},
                     {name = 'bucket_id', unique = false, parts = {{field = 3, type = 'unsigned'}}}}
    assert(tarn_create_table{name = name, id = sid, engine = engine, format = format, indexes = indexes}[1] == 'ok')
    local writes = {}
    for i = 1, count do writes[#writes + 1] = {'insert', sid, {i, 10, 1}} end
    assert(tarn_autocommit(writes)[1] == 'ok')
    format = table.deepcopy(format)
    format[4] = {name = 'c', type = 'integer', is_nullable = true}
    local parts = {{field = 4, type = 'integer', is_nullable = true}}
    indexes[3] = {name = 'c_idx', unique = false, parts = parts}
    a = {ver = 1, timeout = 5, materialize = true, old_width = 3, pad = {3, 3},
         checks = {{4, 'c', 'indexed', box.NULL, box.NULL, 3}}, builds = {{2, 'c_idx', false, parts}},
         drop = {}, renames = {}, format = format,
         def = {name = name, id = sid, engine = engine, format = format, indexes = indexes, json = '{}', ver = 1}}
    box.space._tarn_meta:replace{'test:backfill_args', a}
    tarn_test_hook('alter_backfill_chunk', 'hold')
    fiber.create(function() tarn_alter_prepare(name, a, 1) end)
    assert(tarn_test_hook('alter_backfill_chunk', 'reached')[2] == 1)
    assert(journal() == 128 and #box.space[sid]:get{1} == 4 and #box.space[sid]:get{count} == 3)
    print('  ok   ' .. engine .. ': SIGKILL after a committed partial-padding chunk')
    killed()
else
    a = assert(box.space._tarn_meta:get{'test:backfill_args'}).value
    local w = old_writer()
    assert(w[1] == 'error' and w[2] == 1412, 'restart lost the durable ALTER write fence')
    if stage == 'crash_undo' then
        assert(journal() == 128 and #box.space[sid]:get{1} == 4 and #box.space[sid]:get{count} == 3)
        assert(tarn_alter_prepare(name, a, 2)[1] == 'ok')
        assert(journal() == count)
        tarn_test_hook('alter_backfill_undo', 'hold')
        fiber.create(function() tarn_alter_abort(name, a, 2) end)
        assert(tarn_test_hook('alter_backfill_undo', 'reached')[2] == 1)
        assert(journal() == count - 128 and #box.space[sid]:get{1} == 3
               and #box.space[sid]:get{count} == 4)
        w = old_writer()
        assert(w[1] == 'error' and w[2] == 1412, 'partial undo released the write fence')
        print('  ok   ' .. engine .. ': restart retains padding/fence; SIGKILL after a partial-undo chunk')
        killed()
    elseif stage == 'finish' then
        assert(journal() == count - 128 and #box.space[sid]:get{1} == 3
               and #box.space[sid]:get{count} == 4)
        assert(tarn_alter_abort(name, a, 3)[1] == 'ok')
        assert(journal() == 0)
        for _, t in box.space[sid]:pairs() do assert(#t == 3) end
        assert(old_writer()[1] == 'ok')
        local stale = tarn_alter_prepare(name, a, 1)
        assert(stale[1] == 'error' and stale[2] == 1412)
        a.ver, a.def.ver, a.pad[2], a.checks[1][6] = 2, 2, 9, 9
        assert(tarn_alter_prepare(name, a, 4)[1] == 'ok')
        assert(tarn_alter_apply(name, a, 4)[1] == 'ok')
        assert(box.space[sid].index[2]:count{9} == count and box.space[sid].index[2]:count{3} == 0)
        print('  ok   ' .. engine .. ': second restart finishes undo before a different-default ADD reuses the slot')
    else
        error('unknown backfill restart stage ' .. stage)
    end
end
os.exit(0)
