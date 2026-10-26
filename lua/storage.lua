-- storage.lua -- entry point for a Tarn storage instance (one shard replica).
--
-- Configured from the environment so the same file serves every instance:
--   TARN_SHARD_ID      shard number (1..N)
--   TARN_LISTEN        IPROTO listen address, e.g. 127.0.0.1:3301
--   TARN_WORKDIR       data directory (WAL, snapshots)
--   TARN_REPLICAS      JSON list of all replica URIs of this shard ("" = single node)
--   TARN_PEERS         JSON map shard_id -> [replica URIs] of every shard (for recovery)
--   TARN_MEMORY_MB     memtx memory (default 1024)
--   TARN_VINYL_MEMORY_MB  vinyl in-memory level of the LSM trees (default 128)
--   TARN_VINYL_CACHE_MB   vinyl tuple cache (default 128)
--   TARN_VINYL_READ_THREADS  threads reading vinyl pages from disk (default:
--                      the CPUs divided among TARN_HOST_INSTANCES, 1 to 8)
--   TARN_HOST_INSTANCES storage instances sharing this machine (default 1)
--   TARN_VINYL_WRITE_THREADS, TARN_VINYL_PAGE_SIZE, TARN_VINYL_RANGE_SIZE,
--   TARN_VINYL_BLOOM_FPR   Tarantool's vinyl_* options of those names (its
--                      defaults when unset)
--   TARN_WAL_MODE      write | fsync | none (default write)
--   TARN_IPROTO_THREADS (default 1)
--   TARN_LISTEN_UNIX   optional Unix socket path to listen on as well
--   TARN_LOAD_WINDOW   seconds of requests a bucket's load counts (the
--                      rebalancer's TARN REBALANCE BY LOAD; default 60:
--                      the last one or two minutes)
--   TARN_CHECK_YIELDS, TARN_TEST_HOOKS  (tests) see lua/tarn.lua "Yields"
--   TARN_DUMP_EVERY    (tests) checkpoint every N seconds, so that vinyl rows
--                      go to disk and reads of them yield
local json = require('json')
local fio = require('fio')

local function env(name, default)
    local v = os.getenv(name)
    if v == nil or v == '' then return default end
    return v
end

local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '?.lua;' .. package.path

local shard_id = tonumber(env('TARN_SHARD_ID', '1'))
local listen = env('TARN_LISTEN', '127.0.0.1:3301')
local workdir = env('TARN_WORKDIR', 'data')
local replicas = json.decode(env('TARN_REPLICAS', '[]'))
local peers = json.decode(env('TARN_PEERS', '{}'))
fio.mktree(workdir)

-- Vinyl reads a page that misses its cache on one of vinyl_read_threads,
-- in turn; Tarantool's default of one thread queues every such read behind
-- the last. The readers wait on the disk and decompress pages: give them the
-- machine's CPUs, shared among the storage instances on it, at least 1 and at
-- most 8 (past that the transaction thread is the limit).
local function default_read_threads()
    local ffi = require('ffi')
    pcall(ffi.cdef, 'long sysconf(int name);')
    local cpus = tonumber(ffi.C.sysconf(jit.os == 'OSX' and 58 or 84)) -- _SC_NPROCESSORS_ONLN
    local per = math.floor(cpus / tonumber(env('TARN_HOST_INSTANCES', '1')))
    return math.max(1, math.min(8, per))
end

local replicated = #replicas > 1
-- Optionally also listen on a Unix socket (co-located SQL servers use it to
-- skip the TCP stack).
local listen_cfg = listen
if env('TARN_LISTEN_UNIX', '') ~= '' then
    listen_cfg = {listen, 'unix/:' .. env('TARN_LISTEN_UNIX', '')}
end
box.cfg{
    listen = listen_cfg,
    work_dir = workdir,
    memtx_memory = tonumber(env('TARN_MEMORY_MB', '1024')) * 1024 * 1024,
    memtx_use_mvcc_engine = true,
    vinyl_memory = tonumber(env('TARN_VINYL_MEMORY_MB', '128')) * 1024 * 1024,
    vinyl_cache = tonumber(env('TARN_VINYL_CACHE_MB', '128')) * 1024 * 1024,
    vinyl_read_threads = tonumber(env('TARN_VINYL_READ_THREADS', '')) or default_read_threads(),
    vinyl_write_threads = tonumber(env('TARN_VINYL_WRITE_THREADS', '')),
    vinyl_page_size = tonumber(env('TARN_VINYL_PAGE_SIZE', '')),
    vinyl_range_size = tonumber(env('TARN_VINYL_RANGE_SIZE', '')),
    vinyl_bloom_fpr = tonumber(env('TARN_VINYL_BLOOM_FPR', '')),
    -- Plain (direct) reads must never observe data that a failover could
    -- still roll back.
    txn_isolation = 'read-confirmed',
    wal_mode = env('TARN_WAL_MODE', 'write'),
    iproto_threads = tonumber(env('TARN_IPROTO_THREADS', '1')),
    net_msg_max = 8192,
    readahead = 1024 * 1024,
    log_level = tonumber(env('TARN_LOG_LEVEL', '5')),
    checkpoint_interval = 3600,
    feedback_enabled = false,
    replication = replicated and replicas or nil,
    replication_synchro_quorum = replicated and 'N / 2 + 1' or nil,
    replication_synchro_timeout = 5,
    election_mode = replicated and 'candidate' or 'off',
    election_fencing_mode = replicated and 'strict' or nil,
    replication_timeout = 0.5,
    bootstrap_strategy = replicated and 'config' or 'auto',
    bootstrap_leader = replicated and replicas[1] or nil,
    instance_name = 'shard' .. shard_id .. '_' .. (listen:gsub('[^%w]', '_')),
    replicaset_name = 'shard' .. shard_id,
}

-- The first instance bootstraps users; replicas receive them via replication.
if not replicated then
    box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
end

-- Wait until this instance knows whether it is writable before creating schema.
if replicated then
    local fiber = require('fiber')
    local deadline = os.time() + 30
    while box.info.ro and box.info.election.leader == 0 and os.time() < deadline do
        fiber.sleep(0.1)
    end
end

if env('TARN_DUMP_EVERY', '') ~= '' then
    local fiber = require('fiber')
    local every = tonumber(env('TARN_DUMP_EVERY', '2'))
    fiber.create(function()
        while true do
            fiber.sleep(every)
            pcall(box.snapshot)
        end
    end)
end

require('tarn').init{
    shard_id = shard_id,
    peers = peers,
    sync = replicated,
    -- Tests: fail a protocol step that yields inside its atomic section, and
    -- let tests hold a step at its yield points (lua/tarn.lua "Yields").
    check_yields = env('TARN_CHECK_YIELDS', '') ~= '',
    test_hooks = env('TARN_TEST_HOOKS', '') ~= '',
    load_window = tonumber(env('TARN_LOAD_WINDOW', '60')),
}
