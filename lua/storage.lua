-- storage.lua -- entry point for a Tarn storage instance (one shard replica).
--
-- Configured from the environment so the same file serves every instance:
--   TARN_SHARD_ID      shard number (1..N)
--   TARN_LISTEN        IPROTO listen address, e.g. 127.0.0.1:3301
--   TARN_WORKDIR       data directory (WAL, snapshots)
--   TARN_REPLICAS      JSON list of all replica URIs of this shard ("" = single node)
--   TARN_INSTANCE_UUID optional registered UUID when starting a backup-seeded replica
--   TARN_INSTANCE_NAME optional stable name (default: shard ID and listen address)
--   TARN_ELECTION_MODE candidate (default with replicas), voter, or off
--   TARN_PEERS         JSON map shard_id -> [replica URIs] of every shard (for recovery)
--   TARN_MEMORY_MB     memtx memory (default 1024)
--   TARN_VINYL_MEMORY_MB  vinyl in-memory level of the LSM trees (default 128)
--   TARN_VINYL_CACHE_MB   vinyl tuple cache (default 128)
--   TARN_VINYL_TIMEOUT    max seconds per native memory-quota wait (default .25)
--   TARN_VINYL_READ_THREADS  threads reading vinyl pages from disk (default:
--                      the CPUs divided among TARN_HOST_INSTANCES, 1 to 8)
--   TARN_HOST_INSTANCES storage instances sharing this machine (default 1)
--   TARN_VINYL_WRITE_THREADS, TARN_VINYL_PAGE_SIZE, TARN_VINYL_RANGE_SIZE,
--   TARN_VINYL_BLOOM_FPR   Tarantool's vinyl_* options of those names (its
--                      defaults when unset)
--   TARN_WAL_MODE      write | fsync | none (default write)
--   TARN_IPROTO_THREADS (default 1)
--   TARN_LISTEN_UNIX   optional Unix socket path to listen on as well
--   TARN_NATIVE        the native procedures' module, libtarn_storage.so
--                      (crates/tarn-storage; default: the release build of
--                      this checkout, when there is one; 0: none)
--   TARN_LOAD_WINDOW   seconds of requests a bucket's load counts (the
--                      rebalancer's TARN REBALANCE BY LOAD; default 60:
--                      the last one or two minutes)
--   TARN_MAX_TXN_AGE, TARN_CLOCK_SKEW, TARN_CLOCK_MAX_GAP,
--   TARN_CLOCK_MAX_DRIFT  immutable timing policy, in seconds; set on every
--                      replica before its first policy installation
--   TARN_CHECK_YIELDS, TARN_TEST_HOOKS  (tests) see lua/tarn.lua "Yields"
--   TARN_CHECK_LOCKS   (tests) see lua/tarn.lua "Native procedures"
--   TARN_DUMP_EVERY    (tests) checkpoint every N seconds, so that vinyl rows
--                      go to disk and reads of them yield
local json = require('json')
local fio = require('fio')
local log = require('log')

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
local function timing(name, default)
    local value = tonumber(env(name, tostring(default)))
    if value == nil or value ~= value or value < 0 or value > 9007199254 then
        error(name .. ' must be a finite nonnegative number within the clock policy bounds')
    end
    return value
end
local max_age = timing('TARN_MAX_TXN_AGE', 120)
local skew = timing('TARN_CLOCK_SKEW', 60)
local max_gap = timing('TARN_CLOCK_MAX_GAP', 2)
local max_drift = timing('TARN_CLOCK_MAX_DRIFT', .25)
if max_age <= 0 or max_gap <= 0 or max_drift >= max_gap or max_age + 2 * skew > 9007199254 then
    error('timing policy requires positive age/gap, drift < gap and age + 2*skew within the clock bounds')
end
assert(not fio.path.exists(fio.pathjoin(workdir, 'INCOMPLETE')),
       'refusing an incomplete backup/restore directory')
fio.mktree(workdir)
local maintenance = require('tarn_backup').read_marker(workdir)
local restore_parent = fio.dirname(fio.abspath(workdir))
local activation_uuid
if fio.path.exists(fio.pathjoin(restore_parent, 'cluster-source.json')) and not maintenance then
    local f = assert(io.open(fio.pathjoin(restore_parent, 'activated.json')),
                     'cluster restore activation is incomplete')
    local text = f:read(262145); f:close()
    assert(#text <= 262144, 'oversized cluster activation receipt')
    local activation = json.decode(text)
    assert(activation.format == require('tarn_backup').format and type(activation.instances) == 'table',
           'invalid cluster activation receipt')
    activation_uuid = activation.instances[fio.basename(fio.abspath(workdir))]
    assert(type(activation_uuid) == 'string' and require('uuid').fromstr(activation_uuid),
           'cluster activation receipt omits this replica')
end

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
local election_mode = env('TARN_ELECTION_MODE', replicated and 'candidate' or 'off')
assert(election_mode == 'candidate' or election_mode == 'voter' or election_mode == 'off',
       'TARN_ELECTION_MODE must be candidate, voter or off')
assert(maintenance or not replicated or election_mode ~= 'off',
       'replicated Tarn requires candidate or voter election mode')
local instance_uuid = env('TARN_INSTANCE_UUID', '')
if activation_uuid then
    assert(instance_uuid == '' or instance_uuid == activation_uuid, 'cluster activation identity mismatch')
    instance_uuid = activation_uuid
end
if instance_uuid ~= '' then
    assert(require('uuid').fromstr(instance_uuid) ~= nil, 'TARN_INSTANCE_UUID must be a UUID')
end
local seed_path = fio.pathjoin(workdir, 'seed.json')
if fio.path.exists(seed_path) then
    local f = assert(io.open(seed_path))
    local text = f:read(4097); f:close()
    assert(#text <= 4096, 'oversized seed identity marker')
    local seed = json.decode(text)
    assert(replicated and election_mode == 'voter' and seed.instance_uuid == instance_uuid,
           'backup seed requires its registered TARN_INSTANCE_UUID and TARN_ELECTION_MODE=voter')
end
local vinyl_timeout = tonumber(env('TARN_VINYL_TIMEOUT', '.25'))
if vinyl_timeout == nil or vinyl_timeout ~= vinyl_timeout or vinyl_timeout < 0 or vinyl_timeout > 60 then
    error('TARN_VINYL_TIMEOUT must be a finite number from 0 through 60')
end
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
    vinyl_timeout = vinyl_timeout,
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
    -- Restores are inspected in quarantine with no replication or elections.
    -- A source fence survives restart while retaining catch-up connectivity.
    replication = maintenance and maintenance.kind == 'restore' and {} or (replicated and replicas or nil),
    read_only = maintenance ~= nil,
    replication_synchro_quorum = replicated and 'N / 2 + 1' or nil,
    replication_synchro_timeout = 5,
    election_mode = maintenance and 'off' or election_mode,
    election_fencing_mode = replicated and 'strict' or nil,
    replication_timeout = 0.5,
    bootstrap_strategy = replicated and 'config' or 'auto',
    bootstrap_leader = replicated and replicas[1] or nil,
    instance_name = env('TARN_INSTANCE_NAME', 'shard' .. shard_id .. '_' .. (listen:gsub('[^%w]', '_'))),
    instance_uuid = instance_uuid ~= '' and instance_uuid or nil,
    replicaset_name = 'shard' .. shard_id,
}

if maintenance then
    assert(maintenance.uuid == tostring(box.info.uuid), 'maintenance identity mismatch')
    log.warn('tarn: maintenance fence %s; application initialization disabled', maintenance.token)
    return
end

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

-- The native procedures (lua/tarn.lua, "Native procedures"): Tarantool
-- finds the module 'tarn_storage' as lib?.so in its directory.
local native = env('TARN_NATIVE', '')
if native == '' then
    native = here .. '../target/release/libtarn_storage.so'
    if not fio.path.exists(native) then native = '0' end
end
if native ~= '0' then
    if fio.path.exists(native) and fio.basename(native) == 'libtarn_storage.so' then
        package.cpath = fio.dirname(native) .. '/lib?.so;' .. package.cpath
    else
        log.warn('tarn: TARN_NATIVE=%s is not a libtarn_storage.so: the Lua procedures serve every request', native)
        native = '0'
    end
end

require('tarn').init{
    shard_id = shard_id,
    peers = peers,
    sync = replicated,
    max_txn_age = max_age,
    clock_skew = skew,
    clock_max_gap = max_gap,
    clock_max_drift = max_drift,
    -- Tests: fail a protocol step that yields inside its atomic section, and
    -- let tests hold a step at its yield points (lua/tarn.lua "Yields").
    check_yields = env('TARN_CHECK_YIELDS', '') ~= '',
    test_hooks = env('TARN_TEST_HOOKS', '') ~= '',
    -- Tests: check the native module's copy of the lock index after each
    -- change (lua/tarn.lua "Native procedures").
    check_locks = env('TARN_CHECK_LOCKS', '') ~= '',
    load_window = tonumber(env('TARN_LOAD_WINDOW', '60')),
    move_batch_rows = tonumber(env('TARN_MOVE_BATCH_ROWS', '512')),
    move_batch_bytes = tonumber(env('TARN_MOVE_BATCH_BYTES', '1048576')),
    move_slots = tonumber(env('TARN_MOVE_SLOTS', '4')),
    move_staging_bytes = tonumber(env('TARN_MOVE_STAGING_BYTES', '17179869184')),
    move_total_staging_bytes = tonumber(env('TARN_MOVE_TOTAL_STAGING_BYTES', '34359738368')),
    move_delta_keys = tonumber(env('TARN_MOVE_DELTA_KEYS', '16384')),
    move_delta_bytes = tonumber(env('TARN_MOVE_DELTA_BYTES', '4194304')),
    move_total_delta_bytes = tonumber(env('TARN_MOVE_TOTAL_DELTA_BYTES', '8388608')),
    move_disk_reserve_bytes = tonumber(env('TARN_MOVE_DISK_RESERVE_BYTES', '1073741824')),
    native = native ~= '0' and native or false,
}
