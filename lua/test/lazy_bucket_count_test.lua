-- Internal lazy metadata is bucket-move data, not evidence that a shard
-- without a map contains tables written with the legacy bucket count.
local fio, fiber, json = require('fio'), require('fiber'), require('json')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local native = os.getenv('TARN_NATIVE')
if native ~= nil and native ~= '0' then package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath end
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, wal_mode = 'fsync',
        txn_isolation = 'read-confirmed', log_level = 3}
local tarn = require('tarn'); tarn.lazy.paused = true
tarn.init{shard_id = 1, peers = {}, native = native ~= nil and native ~= '0' and native or false,
          test_hooks = true, check_yields = true}
fiber.sleep(.03)
local pass, fail = 0, 0
local function check(name, yes, extra)
    if yes then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. ' ' .. json.encode(extra)) end
end
local function ok(r) assert(r[1] == 'ok', json.encode(r)); return r end
local r = ok(tarn_buckets())
check('an internal-metadata-only shard without a map reports zero legacy tables',
      r[2] == 0 and r[3] == 0, r)
local sid, gen = 75001, 75002
local pk = require('varbinary').new(require('msgpack').encode({1}))
box.space._tarn_lazy_counter:insert{1, 7}
box.space._tarn_lazy_version:insert{sid, pk, 1, 7}
box.space._tarn_lazy_floor:insert{gen, 1, 2, 7}
r = ok(tarn_buckets())
check('populated counter/version/floor metadata still does not imply a legacy user table',
      r[2] == 0 and r[3] == 0, r)
ok(tarn_create_table{name = 'test.lazy_bucket_legacy', id = sid, ver = 0, engine = 'memtx', json = '{}',
    format = {{name = 'id', type = 'unsigned'}, {name = 'bucket_id', type = 'unsigned'}},
    indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
               {name = 'bucket_id', unique = false, parts = {{2, 'unsigned'}}}}})
box.space[sid]:insert{1, 1}
r = ok(tarn_buckets())
check('a committed catalog table without a map still reports the legacy table',
      r[2] == 0 and r[3] == 1, r)
local rows = ok(tarn_bucket_rows(1, box.NULL))[2]
local found = {}; for _, row in ipairs(rows) do found[row[1]] = row[2] end
check('default bucket-copy enumeration retains all lazy auxiliaries and the base row',
      #rows == 4 and found[sid] ~= nil and found._tarn_lazy_counter ~= nil
      and found._tarn_lazy_version ~= nil and found._tarn_lazy_floor ~= nil
      and found._tarn_lazy_counter[2] == 7 and found._tarn_lazy_version[4] == 7
      and found._tarn_lazy_floor[4] == 7, rows)
print(string.format('%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
