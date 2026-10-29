-- Private nodes for clock_integration.sh; no host clock changes.
local fio = require('fio')
local here = fio.dirname(arg[0]); package.path = here .. '/../?.lua;' .. package.path
local id, work = assert(tonumber(arg[1])), assert(arg[2]); fio.mktree(work)
box.cfg{listen = '127.0.0.1:' .. (3560 + id), work_dir = work,
        memtx_use_mvcc_engine = true, memtx_memory = 64 * 1048576,
        vinyl_memory = 16 * 1048576, vinyl_cache = 0, wal_mode = 'fsync',
        txn_isolation = 'read-confirmed', checkpoint_interval = 0, log_level = 3}
require('tarn').init{shard_id = id, native = false, test_hooks = true, check_yields = true,
    max_txn_age = .5, clock_skew = .5, recovery_interval = .05, recovery_after = 1000,
    peers = {['1'] = {'127.0.0.1:3561'}, ['2'] = {'127.0.0.1:3562'}}}
