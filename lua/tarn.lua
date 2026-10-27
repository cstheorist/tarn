-- tarn.lua -- Tarn storage-node module for Tarantool 3.x.
--
-- Every SQL table is a plain Tarantool space (named "<db>.<table>") with real
-- Tarantool indexes. Committed rows are ordinary tuples, so any Tarantool
-- client can read them directly (space.index.primary:get{...}). Uncommitted
-- writes never touch those spaces: single-shard transactions apply their
-- writes inside one local transaction, and cross-shard transactions park
-- their writes as intents in _tarn_locks until they are known to be
-- committed. The protocol is specified in tla/TarnCommit.tla; function
-- names below match the actions of that spec.
--
-- System spaces (all synchronous when the shard is replicated):
--   _tarn_meta     key -> value                 (shard id, counters, DDL jobs
--                                                and table holds)
--   _tarn_catalog  table name -> definition     (written by DDL)
--   _tarn_locks    R/W point locks and space locks of prepared txns;
--                  W locks carry the intent (op + new tuple)
--   _tarn_rlocks   R locks on primary-key ranges of prepared txns
--   _tarn_part     participant records: prepared / applied / aborted(tombstone)
--   _tarn_txn      transaction records on the decision shard:
--                  staging / committed / aborted
--
-- Record lifecycle (TarnCommit: GC configurations). A PREPARE is refused once
-- its transaction is older than max_txn_age, so every record carries an
-- expiry time beyond which no PREPARE of its transaction can be accepted any
-- more (expires = write time + max_txn_age + clock_skew). Records are only
-- garbage-collected after their expiry, and
--   * a prepared participant record is never collected (recovery resolves it);
--   * an applied participant record only once the decision record is final
--     (COMMITTED) or gone -- otherwise recovery could read "no record" as
--     "never prepared" and abort a committed transaction;
--   * a decision record only once no participant is still prepared.
-- The decision shard's APPLY marks the record COMMITTED (TarnCommit:
-- ApplyMarksCommitted), so committed records become final without recovery.

local fiber = require('fiber')
local log = require('log')
local msgpack = require('msgpack')
local key_def_lib = require('key_def')
local net_box = require('net.box')
local clock = require('clock')
local ffi = require('ffi')
local decimal = require('decimal')
local uuid = require('uuid')

local M = {
    cfg = {
        shard_id = 0,
        peers = {},              -- shard_id -> {uri, uri, ...} (all replicas)
        user = 'tarn', password = 'tarn',
        sync = false,            -- create spaces with is_sync (replicated shards)
        recovery_interval = 0.5, -- seconds between recovery scans
        recovery_after = 2.0,    -- age before an undecided txn is recovered
        lock_wait = 1.0,         -- max wait for a conflicting lock (autocommit)
        max_txn_age = 120.0,     -- PREPAREs of transactions older than this are refused
        clock_skew = 60.0,       -- assumed bound on clock differences between nodes
        gc_batch = 500,          -- records deleted per GC transaction
        lazy_outbox_entries = 65536, -- committed plus PREPARE-promised assignments
        lazy_outbox_bytes = 64 * 1024 * 1024,
        vinyl_attempt_timeout = .25, -- fresh Vinyl physical-write attempt; APPLY is already decided
        alter_check_rows = 1000000, -- ALTER TABLE refuses to check more existing rows than this per shard
        -- A read returns at most this many rows, or rows of this many bytes,
        -- per call; the SQL server continues it after the last one (issue #77).
        scan_page_rows = 4096,
        scan_page_bytes = 4 * 1024 * 1024,
        -- The rebalancer's load of each bucket covers the requests of the
        -- last one or two windows of this many seconds (tarn_bucket_stats).
        load_window = 60,
        -- The native procedures' module (lua/storage.lua: TARN_NATIVE), or
        -- false (see "Native procedures").
        native = false,
        check_yields = false,    -- tests: an atomic section that yields is an error
        test_hooks = false,      -- tests: tarn_test_hook may hold yield points
    },
    stats = {commit_local = 0, prepare_yes = 0, prepare_no = 0, apply = 0,
             abort = 0, conflicts = 0, recovered = 0, lock_waits = 0,
             gc_part = 0, gc_txn = 0, validate_changed = 0, protocol_errors = 0,
             plock_waits = 0, deadlocks = 0, lock_timeouts = 0, commit_rc = 0, commit_waits = 0,
             stale_defs = 0, alters = 0, vy_rereads = 0, apply_dropped = 0, alter_conflicts = 0,
             creates = 0, drops = 0, truncates = 0, ddl_doomed = 0, read_oom = 0, heap_collects = 0,
             moved = 0, move_broken_locks = 0, bucket_sends = 0, bucket_recvs = 0},
}

local RW = {txn_isolation = 'read-committed'}

-- Buckets (see "Buckets" below; one table, as the main chunk is at Lua's
-- limit of 200 locals). Declared here because the key definitions and the
-- lock index use it: the lock index counts the locks of prepared
-- transactions on each bucket's rows (nl[bucket] = n).
local BK = {nl = {}, state = {}, peer = {}, move = {}, held = 0, staged = {}, count = 0, epoch = 0,
            stale = true, track = {}, track_n = 0, fields = {}}

-------------------------------------------------------------------------------
-- Schema
-------------------------------------------------------------------------------

local function ensure_schema()
    local sync = M.cfg.sync
    local function space(name, format)
        local s = box.schema.space.create(name, {if_not_exists = true, is_sync = sync, format = format})
        return s
    end
    local meta = space('_tarn_meta', {{'key', 'string'}, {'value', 'any', is_nullable = true}})
    meta:create_index('pk', {parts = {'key'}, if_not_exists = true})

    local cat = space('_tarn_catalog', {{'name', 'string'}, {'def', 'map'}})
    cat:create_index('pk', {parts = {'name'}, if_not_exists = true})

    -- SQL allocates IDs from 10001 upward. On an upgraded, populated node
    -- auto-ID allocation would consume its next planned SQL ID instead.
    local undo = box.space._tarn_alter_undo
    if undo == nil then
        local id = box.schema.SYSTEM_ID_MAX + 1
        while id < 10000 and box.space[id] ~= nil do id = id + 1 end
        if id >= 10000 then error('tarn: no free internal space ID for ALTER undo') end
        undo = box.schema.space.create('_tarn_alter_undo', {id = id, is_sync = sync,
            format = {{'space_id', 'unsigned'}, {'ver', 'unsigned'}, {'key', 'string'}, {'oldlen', 'unsigned'}}})
    end
    undo:create_index('pk', {parts = {'space_id', 'ver', 'key'}, if_not_exists = true})

    -- A space made by an earlier version gets the fields added since (at
    -- the end, nullable: its tuples stay valid).
    local function extend(s, format)
        if #s:format() < #format then s:format(format) end
        return s
    end
    -- bucket: the bucket of the row a lock is on, when its request named
    -- one (a bucket move waits for these, see "Buckets").
    local lock_format = {
        {'space_id', 'unsigned'}, {'index_id', 'unsigned'}, {'key', 'string'},
        {'txid', 'string'}, {'mode', 'string'},
        {'op', 'string', is_nullable = true}, {'tuple', 'any', is_nullable = true},
        {'bucket', 'unsigned', is_nullable = true}}
    local locks = extend(space('_tarn_locks', lock_format), lock_format)
    locks:create_index('pk', {parts = {'space_id', 'index_id', 'key', 'txid'}, if_not_exists = true})
    locks:create_index('txid', {parts = {'txid'}, unique = false, if_not_exists = true})

    local rlock_format = {
        {'txid', 'string'}, {'n', 'unsigned'}, {'space_id', 'unsigned'}, {'index_id', 'unsigned'},
        {'lo', 'any', is_nullable = true}, {'hi', 'any', is_nullable = true},
        {'lo_incl', 'boolean'}, {'hi_incl', 'boolean'}, {'bucket', 'unsigned', is_nullable = true}}
    local rlocks = extend(space('_tarn_rlocks', rlock_format), rlock_format)
    rlocks:create_index('pk', {parts = {'txid', 'n'}, if_not_exists = true})
    rlocks:create_index('space', {parts = {'space_id', 'index_id'}, unique = false, if_not_exists = true})

    -- Buckets (see "Buckets"): the ones this shard has or is moving, and the
    -- rows of incoming ones, kept aside until they are taken over.
    local buckets = space('_tarn_buckets', {
        {'bucket', 'unsigned'}, {'state', 'string'}, {'peer', 'unsigned', is_nullable = true},
        {'move', 'string', is_nullable = true}})
    buckets:create_index('pk', {parts = {'bucket'}, if_not_exists = true})
    local recv = space('_tarn_recv', {
        {'bucket', 'unsigned'}, {'space_id', 'unsigned'}, {'key', 'string'}, {'tuple', 'array'}})
    recv:create_index('pk', {parts = {'bucket', 'space_id', 'key'}, if_not_exists = true})

    -- ts: time of the last state change; expires: see "Record lifecycle".
    local part = space('_tarn_part', {
        {'txid', 'string'}, {'state', 'string'}, {'dshard', 'unsigned'},
        {'parts', 'array'}, {'ts', 'double'}, {'expires', 'double'}})
    part:create_index('pk', {parts = {'txid'}, if_not_exists = true})
    part:create_index('state', {parts = {'state', 'ts'}, unique = false, if_not_exists = true})
    part:create_index('exp', {parts = {'expires'}, unique = false, if_not_exists = true})

    local txn = space('_tarn_txn', {
        {'txid', 'string'}, {'status', 'string'}, {'parts', 'array'}, {'ts', 'double'},
        {'expires', 'double'}})
    txn:create_index('pk', {parts = {'txid'}, if_not_exists = true})
    txn:create_index('status', {parts = {'status', 'ts'}, unique = false, if_not_exists = true})
    txn:create_index('exp', {parts = {'expires'}, unique = false, if_not_exists = true})

    -- Target of the probe write in tarn_validate (data-temporary: no WAL,
    -- not replicated; the definition reaches replicas).
    local scratch = box.schema.space.create('_tarn_scratch', {if_not_exists = true, type = 'data-temporary',
                                                              format = {{'k', 'unsigned'}}})
    scratch:create_index('pk', {parts = {'k'}, if_not_exists = true})
    if M.lazy then M.lazy.schema() end
end

-------------------------------------------------------------------------------
-- Lock table
-------------------------------------------------------------------------------

local kd_cache = {}  -- "space_id/index_id" -> key_def for comparisons

local function index_key_def(space_id, index_id)
    local ck = space_id * 1000 + index_id
    local kd = kd_cache[ck]
    if kd == nil then
        local idx = box.space[space_id].index[index_id]
        kd = key_def_lib.new(idx.parts)
        kd_cache[ck] = kd
    end
    return kd
end

local function invalidate_key_defs()
    kd_cache = {}; BK.fields = {}
    if M.lazy then M.lazy.invalidate() end
end

-- Canonical byte form of a key: the msgpack encoding of the key array.
local function kbin(key) return msgpack.encode(key) end

local function in_range(kd, key, lo, hi, lo_incl, hi_incl)
    if lo ~= nil then
        local c = kd:compare_keys(key, lo)
        if c < 0 or (c == 0 and not lo_incl) then return false end
    end
    if hi ~= nil then
        local c = kd:compare_keys(key, hi)
        if c > 0 or (c == 0 and not hi_incl) then return false end
    end
    return true
end

-- Lock records ---------------------------------------------------------------
-- A prepared transaction holds, on each participant:
--   W point locks  (space, 0, pk) carrying the intent (op + new tuple), and
--                  (space, idx, key) for every secondary-index key of the old
--                  and the new version of each row it writes;
--   R point locks  (space, idx, key) for every point lookup it made;
--   R range locks  (space, idx, lo, hi) in _tarn_rlocks for every scan.
-- Conflicts (TarnCommit Prepare.writesOK / readsOK, CommitLocal.noConf):
--   a writer conflicts with any lock on the primary key or on a unique key of
--   its new tuple, with R locks on any secondary key of its old/new tuple, and
--   with R range locks covering any of those keys;
--   a reader conflicts with W locks on the keys / within the ranges it read.

-- Index keys of a tuple for every secondary index: {{index_id, key, unique}}.
-- A unique key with a NULL part takes no value: MySQL lets any number of
-- rows have NULL there (and an index lookup with a NULL part is an error).
local function has_null(key)
    for i = 1, #key do
        if key[i] == nil then return true end   -- also box.NULL
    end
    return false
end

local function secondary_keys(space, tuple, out)
    if tuple == nil then return out end
    for id, idx in pairs(space.index) do
        if type(id) == 'number' and id ~= 0 then
            out[#out + 1] = {id, index_key_def(space.id, id):extract_key(tuple):totable(), idx.unique}
        end
    end
    return out
end

-- Volatile lock index ---------------------------------------------------------
-- Lock checks consult only this in-memory index of the lock records, never the
-- _tarn_locks/_tarn_rlocks spaces. Reading those spaces inside a transaction
-- creates MVCC read trackers, so two PREPAREs on one shard that locked
-- *different* keys of a table conflicted with each other (a range check
-- scanned every lock record of the table): correct, but it collapsed
-- throughput under concurrency. The index is
--   * updated in the same non-yielding step as the checks (check-and-lock is
--     atomic, as in TarnCommit!Prepare) and shows the locks of PREPAREs still
--     waiting for their WAL write or quorum (conservative);
--   * undone if a PREPARE's local transaction rolls back, released when the
--     APPLY/ABORT that deletes the durable records commits;
--   * rebuilt from the durable records at startup and after any election or
--     read-only state change, before the next check, and only while this
--     instance is writable (replicas never maintain it, and a new leader is
--     writable only once it owns the synchronous queue). The rebuild takes
--     the union of the read-confirmed and the read-committed view of the
--     lock spaces: it keeps the locks of PREPAREs still waiting for WAL or
--     quorum (visible only in the read-committed view; their on_rollback
--     undo removes them if they fail) and the locks that a pending
--     APPLY/ABORT is deleting (visible only in the read-confirmed view;
--     their on_commit release removes them).
-- Pessimistic row locks (see "Row locks" below), declared here because the
-- write checks consult them.
local PL = {}       -- PL[sid][iid][kbin] = {holders = {[txid] = 'S' | 'X'}, queue = {request, ...}}
local PL_N = 0      -- row locks currently held

local LK = {}       -- LK[sid][iid][kbin] = {[txid] = 'R' | 'W'}
local RL = {}       -- RL[sid][iid][txid] = {{lo, hi, lo_incl, hi_incl}, ...}
local BYTX = {}     -- BYTX[txid] = {{sid, iid, kbin, bucket} | {sid, iid, nil, bucket}, ...}
local NLOCK = 0     -- point + range locks currently held
local LK_STALE = true


-- W-lock history, for autocommit reads of vinyl tables (checked_read): while
-- one runs (VY_SCANS > 0), every W lock taken or released is logged, so that
-- a read whose scan yielded can tell whether a W lock covered what it read
-- at any moment of the scan. A ring of LOG_N entries; entries before
-- LOG_FLOOR are gone (overwritten, or the lock index was rebuilt).
local LOG_N = 8192
local LOG_SID, LOG_IID, LOG_KB = {}, {}, {}
local LOG_SEQ, LOG_FLOOR = 0, 1
local VY_SCANS = 0

local function lk_log(sid, iid, kb)
    LOG_SEQ = LOG_SEQ + 1
    local slot = LOG_SEQ % LOG_N
    LOG_SID[slot], LOG_IID[slot], LOG_KB[slot] = sid, iid, kb
    if LOG_SEQ - LOG_N >= LOG_FLOOR then LOG_FLOOR = LOG_SEQ - LOG_N + 1 end
end

-- A lock record names the bucket of its row when its request did (a read
-- or write routed to one bucket; nil for a read of every shard).
local function lk_tag(bucket, d)
    if bucket ~= nil and bucket ~= box.NULL then BK.nl[bucket] = (BK.nl[bucket] or 0) + d end
end

local function lk_point(txid, sid, iid, kb, mode, bucket)
    local s = LK[sid]; if s == nil then s = {}; LK[sid] = s end
    local i = s[iid]; if i == nil then i = {}; s[iid] = i end
    local e = i[kb]; if e == nil then e = {}; i[kb] = e end
    local new = e[txid] == nil
    if new then NLOCK = NLOCK + 1 end
    if e[txid] ~= 'W' then e[txid] = mode end
    if mode == 'W' and VY_SCANS > 0 then lk_log(sid, iid, kb) end
    local b = BYTX[txid]; if b == nil then b = {}; BYTX[txid] = b end
    b[#b + 1] = {sid, iid, kb, bucket}
    if new then BK.ns_lock(sid, 1) end
    lk_tag(bucket, 1)
end

local function lk_range(txid, sid, iid, lo, hi, lo_incl, hi_incl, bucket)
    local s = RL[sid]; if s == nil then s = {}; RL[sid] = s end
    local i = s[iid]; if i == nil then i = {}; s[iid] = i end
    local r = i[txid]; if r == nil then r = {}; i[txid] = r end
    r[#r + 1] = {lo, hi, lo_incl, hi_incl}
    NLOCK = NLOCK + 1
    local b = BYTX[txid]; if b == nil then b = {}; BYTX[txid] = b end
    b[#b + 1] = {sid, iid, nil, bucket}
    BK.ns_lock(sid, 1)
    lk_tag(bucket, 1)
end

local function lk_release(txid)
    local b = BYTX[txid]
    if b == nil then return end
    BYTX[txid] = nil
    for _, x in ipairs(b) do
        lk_tag(x[4], -1)
        if x[3] ~= nil then
            local i = LK[x[1]] and LK[x[1]][x[2]]
            local e = i and i[x[3]]
            if e ~= nil and e[txid] ~= nil then
                if e[txid] == 'W' and VY_SCANS > 0 then lk_log(x[1], x[2], x[3]) end
                e[txid] = nil
                NLOCK = NLOCK - 1
                BK.ns_lock(x[1], -1)
                if next(e) == nil then i[x[3]] = nil end
            end
        else
            local i = RL[x[1]] and RL[x[1]][x[2]]
            if i ~= nil and i[txid] ~= nil then
                NLOCK = NLOCK - #i[txid]
                BK.ns_lock(x[1], -#i[txid])
                i[txid] = nil
            end
        end
    end
end

-- Raise ER_READONLY unless this instance is writable. Every lock check and
-- every protocol step that relies on the lock index starts with it.
local function writable()
    if box.info.ro then box.error(box.error.READONLY) end
end

-- All lock records visible under one isolation level (no yields: memtx
-- scans and a read-only commit do not yield).
local function lk_scan(isolation)
    box.begin({txn_isolation = isolation})
    local pts, rgs = {}, {}
    for _, l in box.space._tarn_locks:pairs() do pts[#pts + 1] = l end
    for _, r in box.space._tarn_rlocks:pairs() do rgs[#rgs + 1] = r end
    box.commit()
    return pts, rgs
end

local function lk_reload()
    writable()
    -- Reads in progress cannot trust the history: it starts again here.
    LOG_SEQ = LOG_SEQ + 1
    LOG_SID[LOG_SEQ % LOG_N] = nil
    LOG_FLOOR = LOG_SEQ + 1
    local p1, r1 = lk_scan('read-confirmed')
    local p2, r2 = lk_scan('read-committed')
    LK, RL, BYTX, NLOCK = {}, {}, {}, 0
    BK.ns_lock(nil)
    BK.nl = {}
    local seen = {}
    for _, set in ipairs({p1, p2}) do
        for _, l in ipairs(set) do
            local id = l.space_id .. '\0' .. l.index_id .. '\0' .. l.key .. '\0' .. l.txid
            if not seen[id] then
                seen[id] = true
                lk_point(l.txid, l.space_id, l.index_id, l.key, l.mode, l.bucket)
            end
        end
    end
    for _, set in ipairs({r1, r2}) do
        for _, r in ipairs(set) do
            local id = 'r\0' .. r.txid .. '\0' .. r.n
            if not seen[id] then
                seen[id] = true
                lk_range(r.txid, r.space_id, r.index_id, r.lo, r.hi, r.lo_incl, r.hi_incl, r.bucket)
            end
        end
    end
    LK_STALE = false
    BK.ns_sync()
end

-- Make the index current; reads the spaces in a separate fiber so that a
-- caller inside a transaction gets no read trackers on them. Refuses to run
-- (ER_READONLY) on a read-only instance.
local function lk_fresh()
    writable()
    if not LK_STALE then return end
    local f = fiber.new(lk_reload)
    f:set_joinable(true)
    local ok, err = f:join()
    if not ok then error(err) end
end

-- Definition fences (online ALTER TABLE, see "Schema changes" below) ------
-- FENCE[space_id] is the oldest table definition version a write may have
-- been encoded with: the version of the table's last ALTER TABLE (`ver` in
-- its _tarn_catalog entry), raised in memory by an ALTER in progress.
local FENCE = {}
local FENCE_STALE = true   -- rebuilt from _tarn_catalog before the next check

-- Tables held by a TRUNCATE or DROP TABLE (see "CREATE TABLE, DROP TABLE and
-- TRUNCATE" below). From its PREPARE to its APPLY on this shard, a TRUNCATE
-- or DROP holds the whole table as a prepared transaction holds the rows it
-- writes: reads that check W locks wait for it, and transactions that read
-- the table fail validation. DDL_HELD[space_id] = what holds it (for
-- messages). The holds are also stored ('hold:<space_id>' in _tarn_meta), so
-- that they survive a restart or a leader change.
local DDL_HELD, DDL_N = {}, 0
-- A scan may span a hold and its release, or another fiber rebuilding the
-- cache. A fresh identity retains that history even when no hold remains;
-- unlike a numeric counter it cannot wrap while an older scan retains it.
local DDL_GEN = {}
-- A TRUNCATE or DROP applied here also fails the validation of the open
-- stream transactions that read the table before it: memtx MVCC aborts the
-- readers of a space that changes, vinyl only its writers.
-- DOOMED[txn id] = {space_id = true}, set when one is applied; DOOMED_N of
-- them (for the native procedures, which leave those transactions to Lua).
local DOOMED, DOOMED_N = {}, 0
-- Election term at the start of each open stream transaction (by txn id),
-- recorded by the request that begins it (tarn_select with begin).
local STREAM_TERM = {}

local function fence_rebuild()
    local f = {}
    for _, t in box.space._tarn_catalog:pairs() do
        local sp = box.space[t.name]
        if sp ~= nil and t.def.ver ~= nil then f[sp.id] = t.def.ver end
    end
    local held, n = {}, 0
    for _, t in box.space._tarn_meta:pairs({'hold:'}, {iterator = 'GE'}) do
        if t.key:sub(1, 5) ~= 'hold:' then break end
        local sid = tonumber(t.key:sub(6))
        if sid ~= nil and box.space[sid] ~= nil then
            held[sid], n = t.value.label, n + 1
            if (f[sid] or 0) < t.value.ver then f[sid] = t.value.ver end
        end
    end
    -- An ALTER TABLE that fills a new global index keeps its fence stored
    -- until APPLY or ABORT (tarn_alter_prepare): only the new definition
    -- writes the index's entries, so writes made with the old one must stay
    -- refused across a restart or a leader change.
    for _, t in box.space._tarn_meta:pairs({'fence:'}, {iterator = 'GE'}) do
        if t.key:sub(1, 6) ~= 'fence:' then break end
        local sid, v = tonumber(t.key:sub(7)), t.value
        if sid ~= nil and box.space[sid] ~= nil and type(v) == 'table' and not v.aborted
           and (f[sid] or 0) < v.ver then f[sid] = v.ver end
    end
    DDL_GEN = {}
    FENCE, FENCE_STALE = f, false
    DDL_HELD, DDL_N = held, n
    BK.ns_fence(nil)
    BK.ns_sync()
end

-- Rebuild the fences and the holds from storage before their next use (as
-- after a restart or a leader change; tests).
function M.stale_fences()
    FENCE_STALE = true
    if M.lazy then M.lazy.invalidate() end
    BK.ns_sync()
end

-- Like lk_fresh: in a separate fiber, so that a caller inside a transaction
-- gets no read trackers on _tarn_catalog.
local function fence_fresh()
    if not FENCE_STALE then
        if M.lazy then M.lazy.fresh() end
        return
    end
    local f = fiber.new(fence_rebuild)
    f:set_joinable(true)
    local ok, err = f:join()
    if not ok then error(err) end
    if M.lazy then M.lazy.fresh() end
end

local STALE_DEF_MSG = 'Table definition has changed, please retry transaction'

local function ddl_hold(sid, label)
    DDL_GEN = {}
    if DDL_HELD[sid] == nil then DDL_N = DDL_N + 1 end
    DDL_HELD[sid] = label
    BK.ns_sync()
end

local function ddl_release(sid)
    if DDL_HELD[sid] == nil then return end
    DDL_GEN = {}
    DDL_HELD[sid] = nil
    DDL_N = DDL_N - 1
    BK.ns_sync()
    M.unlock_cond:broadcast()
end

-- The open stream transactions may have read space `sid` before a TRUNCATE
-- or DROP that is applied now: their validation fails if they did.
local function doom_readers(sid)
    for id in pairs(STREAM_TERM) do
        local d = DOOMED[id]
        if d == nil then d = {}; DOOMED[id] = d; DOOMED_N = DOOMED_N + 1 end
        d[sid] = true
    end
    BK.ns_sync()
end

-- Why reads {points, ranges} of the current stream transaction fail
-- validation because of a TRUNCATE or DROP TABLE, or nil: one holds a table
-- they read, or one was applied to it since the stream began. Does not yield.
local function reads_ddl_conflict(points, ranges)
    if DDL_N == 0 and next(DOOMED) == nil then return nil end
    local doomed = DOOMED[box.txn_id() or -1]
    local function bad(sid)
        if DDL_HELD[sid] ~= nil then return 'read a table held by ' .. DDL_HELD[sid] end
        if doomed ~= nil and doomed[sid] then
            M.stats.ddl_doomed = M.stats.ddl_doomed + 1
            return 'read a table that a TRUNCATE or DROP TABLE changed since'
        end
        return nil
    end
    for _, r in ipairs(points or {}) do
        local h = bad(r[1])
        if h then return h end
    end
    for _, r in ipairs(ranges or {}) do
        local h = bad(r[1])
        if h then return h end
    end
    return nil
end

-- The writes of a transaction name, per table, the definition version they
-- were encoded with: fence = {space_id, ver, space_id, ver, ...}. Returns
-- the 1412 status if one of them is older than the table's fence. Call
-- fence_fresh() first: it may yield; this does not, so nothing can raise a
-- fence between this check and the writes that follow it.
local function stale_definition(fence)
    if fence == nil or fence == box.NULL then return nil end
    for i = 1, #fence, 2 do
        local cur = FENCE[fence[i]]
        if cur ~= nil and cur > fence[i + 1] then
            M.stats.stale_defs = M.stats.stale_defs + 1
            return {'error', 1412, STALE_DEF_MSG}
        end
    end
    return nil
end

-- Buckets (issue #24) --------------------------------------------------------
-- A row's bucket is crc32c(shard key) % count + 1, and buckets are what moves
-- between shards. _tarn_buckets holds the buckets this shard owns or is
-- moving, each in a state:
--   active     owned and served
--   sending    owned and served; a move to `peer` is copying it, and the
--              writes to its rows are noted (BK.track) for the last copy
--   fenced     owned, behind the move's fence: reads are served; writes, row
--              locks and PREPAREs that read or write its rows are refused
--   sent       moved to `peer`; its rows were deleted here
--   receiving  a move from `peer` copies its rows into _tarn_recv, which
--              nothing reads
--   held       receiving, and held: the validation of a read of every shard
--              that covers one of its rows fails (the row may already be gone
--              from the sender, and is not here yet)
-- (TarnCommit: place, Fenced, HoldOK, MoveFence, MoveDrained, MoveSendFlip,
-- MoveRecvFlip.) Requests name the buckets they were routed by, and a shard
-- refuses ('moved') one it does not own; the SQL server loads the map again
-- and routes the request anew. A read of every shard outside a transaction
-- names instead the epoch of the shard in its server's map (`_tarn_meta`
-- 'buckets'.epoch, which every send and receive bumps), and is refused when
-- it changed: its reads of different shards could otherwise straddle a move.
-- A shard without a map (made by an earlier version, before an SQL server
-- made the map) refuses nothing.

-- The stored state, read with the given isolation: {state, peer, move,
-- staged (held buckets' rows kept aside), count, epoch}.
function BK.read_view(isolation)
    local v = {state = {}, peer = {}, move = {}, staged = {}}
    box.begin({txn_isolation = isolation})
    for _, t in box.space._tarn_buckets:pairs() do
        v.state[t.bucket], v.peer[t.bucket], v.move[t.bucket] = t.state, t.peer, t.move
        if t.state == 'held' then v.staged[t.bucket] = BK.load_staged(t.bucket) end
    end
    local m = box.space._tarn_meta:get{'buckets'}
    box.commit()
    v.count = m ~= nil and m.value.count or 0
    v.epoch = m ~= nil and m.value.epoch or 0
    return v
end

-- How much of a bucket a state serves (a bucket in no state here: 2).
BK.SERVES = {active = 4, sending = 4, fenced = 3, receiving = 2, sent = 2, held = 1}

-- Rebuild the state in memory from the stored one. A step whose commit
-- waits for the WAL or the quorum may have changed the state in memory
-- already, in the direction that serves less (a send: 'sent' and the
-- epoch, see BK.early), or changes it only once its rows are seen (a
-- receive: 'active'). So the rebuild reads the state as confirmed and with
-- the commits in progress (read-committed), and takes for each bucket the
-- one that serves less, the newer epoch, and for a held bucket the rows
-- kept aside in either. (Either view alone would undo a send in progress,
-- or serve a bucket whose rows are not seen yet.) A step whose commit then
-- fails makes the state stale again (BK.commit), or undoes its change
-- (BK.early's on_rollback).
function BK.rebuild()
    local a, c = BK.read_view('read-confirmed'), BK.read_view('read-committed')
    local function serves(v, b) return BK.SERVES[v.state[b]] or 2 end
    local st, peer, mv, held, seen = {}, {}, {}, {}, {}
    for _, v in ipairs({a, c}) do
        for b in pairs(v.state) do
            if not seen[b] then
                seen[b] = true
                local from = serves(c, b) <= serves(a, b) and c or a
                st[b], peer[b], mv[b] = from.state[b], from.peer[b], from.move[b]
            end
        end
    end
    BK.held = 0
    for b, s in pairs(st) do
        if s == 'held' then
            local rows = {}
            for _, v in ipairs({a, c}) do
                for sid, l in pairs(v.staged[b] or {}) do
                    rows[sid] = rows[sid] or {}
                    for key, t in pairs(l) do rows[sid][key] = t end
                end
            end
            held[b] = rows
            BK.held = BK.held + 1
        end
    end
    BK.state, BK.peer, BK.move, BK.staged = st, peer, mv, held
    BK.count = math.max(a.count, c.count)
    BK.epoch = math.max(a.epoch, c.epoch)
    BK.load_members()
    BK.stale = false
    BK.ver = BK.ver + 1
    BK.ns_serves_all()
    BK.ns_sync()
end

-- Membership (issue #25): the shards of the cluster, kept on every shard
-- ('members' in _tarn_meta: {ver, shards = {{id, replicas, state}, ...}},
-- state 'active' or 'removed'), written on every shard by the SQL server
-- that adds or removes one (as a job). Its version only rises. A
-- transaction that read every shard it knows names the version it was
-- routed with in its commit requests (mver), as does a read of every shard
-- in its route; a shard with a newer version refuses them: they may have
-- missed a shard that joined since (TarnCommit: MemberOK, docs/design.md
-- §2.13). A shard without a membership record has version 0: the shards of
-- the SQL servers' configuration.
BK.members, BK.mver = {ver = 0, shards = {}}, 0
function BK.load_members()
    local m = box.space._tarn_meta:get{'members'}
    BK.members = m ~= nil and m.value or {ver = 0, shards = {}}
    BK.mver = BK.members.ver or 0
    BK.ns_sync()
end

-- The status refusing a request routed with membership version `mv`, or
-- nil.
function BK.member_refused(mv)
    if type(mv) ~= 'number' or mv >= BK.mver then return nil end
    M.stats.moved = M.stats.moved + 1
    return {'moved', 1213, string.format("the cluster's shards changed (version %d; the request's %d)",
                                         BK.mver, mv), box.NULL}
end

-- The state of shard `id` in this shard's membership: 'active', 'draining'
-- (it is leaving: its buckets move away) or 'removed'; nil when the
-- membership does not name it (version 0: the configuration's shards).
function BK.member_state(id)
    for _, m in ipairs(BK.members.shards or {}) do
        if m.id == id then return m.state or 'active' end
    end
    return nil
end

-- A shard that leaves must not be needed to finish a transaction: a
-- participant prepared with it as its decision shard asks it for the
-- outcome. So participants refuse a PREPARE whose decision shard is leaving
-- (prepare_locks), and the shard leaves once no shard counts one
-- (tarn_shard_records): BK.prep[dshard] counts the PREPAREs in progress by
-- decision shard, from before their checks until their records are written.
BK.prep = {}

-- Changes of the state in memory (states, epoch): tarn_buckets builds its
-- report once per change.
BK.ver = 0

-- Like lk_fresh: in a separate fiber, so that a caller inside a transaction
-- gets no read trackers on _tarn_buckets.
function BK.fresh()
    if not BK.stale then return end
    local f = fiber.new(BK.rebuild)
    f:set_joinable(true)
    local ok, err = f:join()
    if not ok then error(err) end
end

-- The writes to rows of the buckets being sent (BK.track[b] = {keys =
-- {[id] = true}, list = {{space_id, pk}, ...}}) are noted in memory only: a
-- leader change loses them, and the new generation tells the move to copy
-- the bucket again.
BK.gen = uuid.str()
function BK.reset()   -- (an election or read-only change)
    BK.stale = true
    BK.ns_sync()
    local st = tostring(box.info.election.term) .. '/' .. tostring(box.info.ro)
    if st == BK.lead then return end
    BK.lead = st
    BK.track, BK.track_n, BK.gen = {}, 0, uuid.str()
end

-- Whether this shard refuses bucket b: it does not own it, or (writes) it
-- takes no writes for it (fenced).
function BK.bad(b, writes)
    local s = BK.state[b]
    return not (s == 'active' or s == 'sending' or (s == 'fenced' and not writes))
end

-- The first bucket of the list that this shard refuses, or nil.
function BK.refused(buckets, writes)
    if BK.count == 0 or type(buckets) ~= 'table' then return nil end
    for _, b in pairs(buckets) do
        if type(b) == 'number' and BK.bad(b, writes) then return b end
    end
    return nil
end

-- The first bucket that the reads of a read set were routed by (point
-- reads: {space_id, index_id, key, bucket}; ranges: {space_id, index_id,
-- lo, hi, lo_incl, hi_incl, bucket}) that this shard refuses, or nil.
function BK.reads_refused(rpoints, rranges, writes)
    if BK.count == 0 then return nil end
    for _, r in ipairs(rpoints or {}) do
        if type(r[4]) == 'number' and BK.bad(r[4], writes) then return r[4] end
    end
    for _, r in ipairs(rranges or {}) do
        if type(r[7]) == 'number' and BK.bad(r[7], writes) then return r[7] end
    end
    return nil
end

-- The status refusing a request for bucket b.
function BK.moved(b)
    M.stats.moved = M.stats.moved + 1
    local s, peer = BK.state[b], BK.peer[b]
    local msg
    if s == 'fenced' then
        msg = string.format('bucket %d is moving to shard %s; try again', b, tostring(peer))
    elseif s == 'sent' then
        msg = string.format('bucket %d moved to shard %s', b, tostring(peer))
    else
        msg = string.format('bucket %d is not on shard %d', b, M.cfg.shard_id)
    end
    return {'moved', 1213, msg, peer or box.NULL}
end

-- route: the bucket a read was routed by, or a list of them, or {e =
-- epoch} for a read of every shard outside a transaction. The status that
-- refuses it, or nil.
function BK.check_read(route)
    if route == nil or route == box.NULL then return nil end
    BK.fresh()
    return BK.check_now(route)
end

-- BK.check_read without reading the map again (does not yield).
function BK.check_now(route)
    if type(route) == 'table' and route.m ~= nil then
        local r = BK.member_refused(route.m)
        if r then return r end
    end
    if BK.count == 0 then return nil end
    if type(route) == 'number' then
        return BK.bad(route, false) and BK.moved(route) or nil
    end
    if route.e ~= nil then
        if route.e == BK.epoch then return nil end
        M.stats.moved = M.stats.moved + 1
        return {'moved', 1213, string.format('the buckets of shard %d changed', M.cfg.shard_id), box.NULL}
    end
    local b = BK.refused(route, false)
    return b and BK.moved(b) or nil
end

-- A read checks its route before it reads (to fail fast) and again after,
-- with no yield in between: a read that waited -- for a prepared
-- transaction's lock, or for vinyl -- may have read after a move sent a
-- bucket away or brought one in. (BK.early changes the state and the epoch
-- before the rows do.) fn's argument `i` is the route; a reply that is not
-- 'ok' is returned as it is.
function BK.recheck(fn, i)
    return function(...)
        local r = fn(...)
        local route = select(i, ...)
        if type(r) == 'table' and r[1] == 'ok' and route ~= nil and route ~= box.NULL then
            if BK.stale then   -- (a leader change: the map is read again on the next request)
                M.stats.moved = M.stats.moved + 1
                return {'moved', 1213, 'shard leadership changed during the read', box.NULL}
            end
            local mv = BK.check_now(route)
            if mv then return mv end
            BK.count_reads(route)
        end
        return r
    end
end

-- Load (issue #25): the requests of each bucket, for the rebalancer
-- (tarn_bucket_stats): the reads routed by the bucket, and the rows written
-- to it and their bytes, counted in memory over windows of cfg.load_window
-- seconds, the current one and the one before. A read of several buckets
-- counts once for each. (Cheap: a read's own cost is about 2 us; the bytes
-- it returns would cost half as much again to count, and fiber.clock(),
-- the event loop's, a tenth of clock.monotonic().) A window is three FFI
-- arrays by bucket -- r: reads, w: rows written, b: their bytes -- where
-- the native procedures count their reads too (see "Native procedures"),
-- and which the GC does not traverse.
BK.load = {t0 = 0, n = 0, prev_secs = 0, cur = nil, prev = nil}
function BK.load_rotate()
    local L, t, w, n = BK.load, fiber.clock(), M.cfg.load_window, BK.count
    if L.n ~= n or t - L.t0 >= w then
        if L.n ~= n then   -- (a map of another count: nothing counted for it)
            L.prev, L.prev_secs = nil, 0
        elseif t - L.t0 < 2 * w then
            L.prev, L.prev_secs = L.cur, t - L.t0
        else
            L.prev, L.prev_secs = nil, 0
        end
        if n > 0 then
            L.cur = {r = ffi.new('uint32_t[?]', n + 1), w = ffi.new('uint32_t[?]', n + 1),
                     b = ffi.new('double[?]', n + 1)}
        else
            L.cur = nil
        end
        L.n, L.t0 = n, t
        BK.ns_window()
    end
    -- (the native procedures count reads in the window until it ends, then
    -- hand them to the Lua ones, whose count starts the next window)
    if BK.ns ~= nil then BK.ns.load_until = L.t0 + w end
end
-- The current window's arrays, where the native procedures count.
function BK.ns_window()
    local ns, c = BK.ns, BK.load.cur
    if ns == nil then return end
    ns.reads = c ~= nil and c.r or nil
    ns.writes = c ~= nil and c.w or nil
    ns.wbytes = c ~= nil and c.b or nil
end
function BK.load_add(b, reads, writes, bytes)
    local c = BK.load.cur
    if c ~= nil and type(b) == 'number' and b >= 1 and b <= BK.load.n then
        c.r[b], c.w[b], c.b[b] = c.r[b] + reads, c.w[b] + writes, c.b[b] + bytes
    end
end
function BK.count_reads(route)
    if type(route) == 'number' then
        BK.load_rotate()
        BK.load_add(route, 1, 0, 0)
    elseif type(route) == 'table' and route[1] ~= nil then
        BK.load_rotate()
        for _, b in ipairs(route) do BK.load_add(b, 1, 0, 0) end
    end
end

-- Native procedures (issue #28) -------------------------------------------
-- A C module (crates/tarn-storage; cfg.native: its .so) serves the most
-- frequent requests without running Lua, which costs a shard's
-- transaction thread more than their storage work: tarn_storage.ts_select
-- serves the autocommit reads of memtx tables that tarn_select serves with
-- nothing to check or wait for (crates/tarn-storage/src/select.rs). It
-- decides from the state below (struct tarn_ns, crates/tarn-storage/src/
-- state.rs), which this module keeps current: BK.ns_sync() after every
-- change of what it holds. A request it cannot decide as the Lua procedure
-- would, it hands to it: its reply ["lua"] makes the SQL server call the
-- Lua procedure. Both run in this thread, one at a time, and it does not
-- yield: it reads one state, as a Lua step that does not yield does.
ffi.cdef[[
struct tarn_ns {
    uint32_t layout;
    uint8_t ready, bk_stale, lk_stale, fence_stale;
    uint32_t count, ddl_n, page_rows, scan_chunk;
    uint64_t page_bytes, epoch, mver;
    double load_until;
    uint8_t *serves;
    uint32_t *reads;
    uint32_t *space_locks;
    uint64_t served, to_lua;
    uint32_t pl_n, hooks;
    uint64_t commit_seq, oldest_inflight;
    uint32_t *writes;
    double *wbytes;
    uint64_t *fences;
    uint64_t w_served, w_to_lua;
    uint32_t *pl_spaces;
    uint32_t held, doomed;
    uint64_t c_served, c_to_lua;
    uint64_t svp_txn;
    uint8_t capture_stale;
    uint8_t *write_excluded;
};
]]
BK.NS_LAYOUT = 7
BK.NS_SPACES = 65536
BK.ns = nil         -- the state (struct tarn_ns *), once the module is loaded
BK.ns_serves = nil  -- its `serves` array, kept alive here
-- The locks of prepared transactions by space (BK.NS_SPACES slots, a space
-- in slot id % BK.NS_SPACES: spaces that share a slot add up, so a count is
-- never below its space's): a read of a space without any is not checked
-- against them, as tarn_select would find none of them on what it read.
BK.ns_space_locks = nil

-- Count d more locks on space sid (nil: forget every count, as the lock
-- index is made again).
function BK.ns_lock(sid, d)
    local a = BK.ns_space_locks
    if a == nil then return end
    if sid == nil then
        ffi.fill(a, ffi.sizeof('uint32_t') * BK.NS_SPACES)
        return
    end
    if type(sid) ~= 'number' then return end   -- (not a space: no lock of one)
    local k = sid % BK.NS_SPACES
    a[k] = a[k] + d
end

-- The definition fences by slot (BK.NS_SPACES slots, a space in slot id %
-- BK.NS_SPACES): the highest FENCE of the spaces of each slot, as of space
-- sid's change (nil: of every change, as FENCE is read again). A write with
-- an older definition than its slot's goes to the Lua procedure, which
-- refuses it (1412) unless the fence is another space's.
BK.ns_fences = nil
function BK.ns_fence(sid)
    local a = BK.ns_fences
    if a == nil then return end
    local spaces = BK.NS_SPACES
    if sid == nil then
        ffi.fill(a, ffi.sizeof('uint64_t') * spaces)
        for s, v in pairs(FENCE) do
            local k = s % spaces
            if v > a[k] then a[k] = v end
        end
        return
    end
    local k, m = sid % spaces, 0
    for s, v in pairs(FENCE) do
        if s % spaces == k and v > m then m = v end
    end
    a[k] = m
end

-- PL_N, after each change.
-- The row locks: their number, and by space (BK.NS_SPACES slots, as for
-- the locks of prepared transactions): d more held on space sid, or (sid
-- nil) none.
function BK.ns_pl(sid, d)
    local ns = BK.ns
    if ns == nil then return end
    ns.pl_n = PL_N
    local a = BK.ns_pl_spaces
    if sid == nil then
        ffi.fill(a, ffi.sizeof('uint32_t') * BK.NS_SPACES)
    else
        local k = sid % BK.NS_SPACES
        a[k] = a[k] + d
    end
end

function BK.ns_sync()
    local ns = BK.ns
    if ns == nil then return end
    ns.bk_stale = BK.stale and 1 or 0
    ns.lk_stale = LK_STALE and 1 or 0
    ns.fence_stale = FENCE_STALE and 1 or 0
    ns.ddl_n = DDL_N
    ns.epoch, ns.mver = BK.epoch, BK.mver
    ns.page_rows, ns.page_bytes = M.cfg.scan_page_rows, M.cfg.scan_page_bytes
    ns.scan_chunk = BK.scan_chunk
    ns.load_until = BK.load.t0 + M.cfg.load_window
    ns.pl_n = PL_N
    ns.held = BK.held
    ns.doomed = DOOMED_N
    if ns.count ~= BK.count then BK.ns_serves_all() end
end

-- (tests: after changing cfg.scan_page_rows or scan_page_bytes)
function M.ns_sync() BK.ns_sync() end
-- ABI7: an unknown capture registry excludes every native write. Slots
-- collide conservatively; a false positive only hands a write back to Lua.
BK.lazy_stale, BK.lazy_excluded, BK.ns_write_excluded = true, {}, nil
function BK.ns_lazy(excluded, stale)
    if stale then
        BK.lazy_stale = true
        if BK.ns ~= nil then BK.ns.capture_stale = 1 end
        return
    end
    local a = ffi.new('uint8_t[?]', BK.NS_SPACES)
    for sid in pairs(excluded) do a[sid % BK.NS_SPACES] = 1 end
    BK.lazy_stale, BK.lazy_excluded, BK.ns_write_excluded = false, excluded, a
    if BK.ns ~= nil then
        BK.ns.write_excluded = a
        BK.ns.capture_stale = 0
    end
end
-- (tests: a native commit in flight, or none)
function M.ns_inflight(on)
    local ns = BK.ns
    if ns == nil then return end
    if on then
        ns.commit_seq = ns.commit_seq + 1
        ns.oldest_inflight = ns.commit_seq
    else
        ns.oldest_inflight = ffi.cast('uint64_t', -1)
    end
end

-- `serves` from the bucket states (BK.bad): 2 for an active bucket, whose
-- reads and writes the native procedures serve, 1 for one being sent or
-- fenced, whose reads they serve (a write to a bucket being sent is noted
-- for its move: BK.note), 0 for the others.
function BK.ns_serving(st)
    if st == 'active' then return 2 end
    return (st == 'sending' or st == 'fenced') and 1 or 0
end

function BK.ns_serves_all()
    local ns = BK.ns
    if ns == nil then return end
    local n = BK.count
    local a = n > 0 and ffi.new('uint8_t[?]', n + 1) or nil
    for b = 1, n do a[b] = BK.ns_serving(BK.state[b]) end
    -- (the new array and its count change together: nothing runs between)
    ns.serves, ns.count, BK.ns_serves = a, n, a
    BK.load_rotate()
end

function BK.ns_serve(b)
    local a = BK.ns_serves
    if a ~= nil and b >= 1 and b <= BK.ns.count then a[b] = BK.ns_serving(BK.state[b]) end
    BK.ns_sync()
end

-- Load the module and give it the state: on the leader (on a replica,
-- requests that need it are refused by the Lua procedures anyway). Its
-- functions are made by setup_schema; without the module (cfg.native
-- false, or its file missing) the SQL servers call the Lua procedures.
function BK.ns_init()
    if BK.ns ~= nil or not M.cfg.native then return end
    if M.lazy then M.lazy.fresh() end
    local f = box.func['tarn_storage.ts_state']
    if f == nil then return end
    local ok, addr = pcall(f.call, f, {BK.NS_LAYOUT})
    if not ok then
        log.warn('tarn: native procedures not loaded (%s): the Lua ones serve every request', tostring(addr))
        M.cfg.native = false
        return
    end
    local ns = ffi.cast('struct tarn_ns *', addr)
    if ns.layout ~= BK.NS_LAYOUT then
        log.warn('tarn: native procedures of state layout %d, %d expected: not used', ns.layout, BK.NS_LAYOUT)
        M.cfg.native = false
        return
    end
    -- (the counts of the lock index as it is: it is made again before its
    -- next use, as lk_stale says until then)
    BK.ns_space_locks = ffi.new('uint32_t[?]', BK.NS_SPACES)
    ns.space_locks = BK.ns_space_locks
    LK_STALE = true
    -- (the fences as FENCE has them, which fence_stale says when stale)
    BK.ns_fences = ffi.new('uint64_t[?]', BK.NS_SPACES)
    ns.fences = BK.ns_fences
    BK.ns_pl_spaces = ffi.new('uint32_t[?]', BK.NS_SPACES)
    ns.pl_spaces = BK.ns_pl_spaces
    BK.ns = ns
    BK.ns_pl_held()
    BK.ns_lazy(BK.lazy_excluded, BK.lazy_stale)
    BK.ns_fence(nil)
    BK.ns_serves_all()
    BK.ns_sync()
    BK.load_rotate()
    BK.ns_window()
    BK.ns_hooks()
    ns.ready = 1
    log.info('tarn: native procedures loaded')
end

-- The rows kept aside for held bucket b, by space: staged[sid] = {tuple, ...}
-- (for BK.held_conflict, which runs inside the checks of commit steps and
-- must not read _tarn_recv there).
function BK.load_staged(b)
    local out = {}
    for _, t in box.space._tarn_recv:pairs({b}, {iterator = 'EQ'}) do
        local l = out[t.space_id]
        if l == nil then l = {}; out[t.space_id] = l end
        l[t.key] = box.tuple.new(t.tuple)
    end
    return out
end

-- Whether a read of every shard (it names no bucket) in a read set covers
-- a row of a bucket held here, which the move will insert: the read may
-- have missed the row (TarnCommit: HoldOK). Once the sender has sent the
-- bucket, the rows kept aside are all of its rows. Does not yield.
function BK.held_conflict(rpoints, rranges)
    if BK.held == 0 then return nil end
    local function covers(staged, sid, iid, lo, hi, lo_incl, hi_incl)
        local rows = staged[sid]
        local sp = box.space[sid]
        if rows == nil or sp == nil or sp.index[iid] == nil then return false end
        local kd = index_key_def(sid, iid)
        if lo == box.NULL then lo = nil end
        if hi == box.NULL then hi = nil end
        for _, t in pairs(rows) do
            if in_range(kd, kd:extract_key(t):totable(), lo, hi, lo_incl, hi_incl) then return true end
        end
        return false
    end
    for b, staged in pairs(BK.staged) do
        for _, r in ipairs(rpoints or {}) do
            if type(r[4]) ~= 'number' and covers(staged, r[1], r[2], r[3], r[3], true, true) then return b end
        end
        for _, r in ipairs(rranges or {}) do
            if type(r[7]) ~= 'number' and covers(staged, r[1], r[2], r[3], r[4], r[5] ~= false, r[6] ~= false) then
                return b
            end
        end
    end
    return nil
end

-- The bucket_id field of a data space (nil if it has none).
function BK.field(sp)
    local f = BK.fields[sp.id]
    if f == nil then
        local idx = sp.index.bucket_id
        f = idx ~= nil and idx.parts[1].fieldno or false
        BK.fields[sp.id] = f
    end
    return f or nil
end

-- Note a write of a row of space sp (old -> new) if its bucket is being
-- sent. (A row's old and new version are in one bucket: the primary key
-- determines it.)
function BK.note(sp, old, new)
    if BK.track_n == 0 then return end
    local f = BK.field(sp)
    local t = new or old
    if f == nil or t == nil then return end
    local tr = BK.track[t[f]]
    if tr == nil then return end
    local pk = index_key_def(sp.id, 0):extract_key(t):totable()
    local id = sp.id .. '\0' .. kbin(pk)
    if not tr.keys[id] then
        tr.keys[id] = true
        tr.list[#tr.list + 1] = {sp.id, pk}
    end
end

local function range_locked(txid, sid, iid, key)
    local i = RL[sid] and RL[sid][iid]
    if i == nil then return nil end
    local kd
    for t, ranges in pairs(i) do
        if t ~= txid then
            kd = kd or index_key_def(sid, iid)
            for _, r in ipairs(ranges) do
                if in_range(kd, key, r[1], r[2], r[3], r[4]) then return t end
            end
        end
    end
    return nil
end

-- Conflicts of a write of `space` with primary key `pk`, old tuple `old`
-- (nil if none) and new tuple `new` (nil for delete). Returns holder or nil.
-- Another transaction's row lock on (sid, iid, key), if any.
local function plock_holder(txid, sid, iid, key)
    local i = PL[sid] and PL[sid][iid]
    local e = i and i[kbin(key)]
    if e == nil then return nil end
    for t in pairs(e.holders) do
        if t ~= txid then return t end
    end
    return nil
end

-- A write may not touch a row (or claim a unique value) that another
-- transaction holds a row lock on (TarnCommit: plocks in CommitLocal and
-- Prepare.writesOK).
local function write_plocked(txid, space, pk, new)
    if PL_N == 0 then return nil end
    local h = plock_holder(txid, space.id, 0, pk)
    if h then return h end
    if new ~= nil then
        for id, idx in pairs(space.index) do
            if type(id) == 'number' and id ~= 0 and idx.unique then
                h = plock_holder(txid, space.id, id, index_key_def(space.id, id):extract_key(new):totable())
                if h then return h end
            end
        end
    end
    return nil
end

local function write_conflict(txid, space, pk, old, new)
    local ph = write_plocked(txid, space, pk, new)
    if ph then return ph end
    if NLOCK == 0 then return nil end
    local sid = space.id
    local s = LK[sid]
    local e = s and s[0] and s[0][kbin(pk)]
    if e ~= nil then
        for t in pairs(e) do
            if t ~= txid then return t end
        end
    end
    local h = range_locked(txid, sid, 0, pk)
    if h then return h end
    local keys = secondary_keys(space, old, {})
    local nold = #keys
    secondary_keys(space, new, keys)
    for n, k in ipairs(keys) do
        -- a unique key the new tuple claims (a NULL part claims nothing)
        local any_mode = k[3] and n > nold and not has_null(k[2])
        local ee = s and s[k[1]] and s[k[1]][kbin(k[2])]
        if ee ~= nil then
            for t, m in pairs(ee) do
                if t ~= txid and (any_mode or m == 'R') then return t end
            end
        end
        h = range_locked(txid, sid, k[1], k[2])
        if h then return h end
    end
    return nil
end

-- Reads -----------------------------------------------------------------------
-- A read set from Tarn: points {{space_id, index_id, key}}, ranges
-- {{space_id, index_id, lo, hi, lo_incl, hi_incl}} (nil bound = unbounded).

local function point_w_locked(txid, space_id, index_id, key)
    local i = LK[space_id] and LK[space_id][index_id]
    local e = i and i[kbin(key)]
    if e ~= nil then
        for t, m in pairs(e) do
            if t ~= txid and m == 'W' then return t end
        end
    end
    return nil
end

local function range_w_locked(txid, space_id, index_id, lo, hi, lo_incl, hi_incl)
    local i = LK[space_id] and LK[space_id][index_id]
    if i == nil then return nil end
    local kd
    for kb, e in pairs(i) do
        for t, m in pairs(e) do
            if t ~= txid and m == 'W' then
                kd = kd or index_key_def(space_id, index_id)
                if in_range(kd, msgpack.decode(kb), lo, hi, lo_incl, hi_incl) then return t end
                break   -- other holders of the same key are equally out of range
            end
        end
    end
    return nil
end

-- TarnCommit NoWLocks: no other prepared txn holds a W lock on anything read.
local function reads_w_locked(txid, points, ranges)
    if NLOCK == 0 then return nil end
    for _, r in ipairs(points or {}) do
        local h = point_w_locked(txid, r[1], r[2], r[3])
        if h then return h end
    end
    for _, r in ipairs(ranges or {}) do
        local h = range_w_locked(txid, r[1], r[2], r[3], r[4], r[5], r[6])
        if h then return h end
    end
    return nil
end

-------------------------------------------------------------------------------
-- Errors returned to the Tarn server: {status, code, message}
-------------------------------------------------------------------------------

local OK = 'ok'
local function conflict(msg)
    M.stats.conflicts = M.stats.conflicts + 1
    return {'conflict', 1213, msg}
end
-- ER_READONLY always reaches the Tarn server as an IPROTO error (never as a
-- status table), so that it drops the connection to the stale leader.
local function reraise_readonly(e)
    if type(e) == 'cdata' and e.code == box.error.READONLY then error(e) end
end
-- Indexes that an ALTER TABLE is building, under temporary names (see
-- "Schema changes"): PENDING_NAME[space_id .. '/' .. index_id] = the name
-- they will get, for error messages.
local PENDING_NAME = {}
local TMP_PREFIX = '_tarn_tmp_'

local function pending_label(msg)
    return (msg:gsub('"(' .. TMP_PREFIX .. '(%d+))" in space "([^"]+)"', function(tmp, iid, space)
        local sp = box.space[space]
        local name = sp and PENDING_NAME[sp.id .. '/' .. iid]
        return '"' .. (name or tmp) .. '" in space "' .. space .. '"'
    end))
end

-- The index under construction (temporary name) that refused a write of
-- space sp with `err` (TUPLE_FOUND), if one did: its id.
local function refused_by_pending(sp, err)
    if type(err) ~= 'cdata' or err.code ~= box.error.TUPLE_FOUND then return nil end
    local iid = tonumber(tostring(err.message):match('"' .. TMP_PREFIX .. '(%d+)"'))
    local idx = iid and sp.index[iid]
    if idx == nil or idx.name ~= TMP_PREFIX .. iid then return nil end
    return iid
end

-- Global indexes of unique keys without the shard key (issue #14): hidden
-- spaces '_tarn_uix_<table space id>_<index id>', one entry per row
-- whose key has no NULL part (the key, then the row's primary key), written
-- in the same transaction as the row by the SQL servers. The entry's
-- primary key is the unique key, so an insert of a value another row holds
-- fails here like any duplicate key.
local UIX = {prefix = '_tarn_uix_'}   -- (one local: the main chunk is near Lua's limit)

function UIX.key_text(v)
    if type(v) == 'cdata' then return (tostring(v):gsub('U?LL$', '')) end
    return tostring(v)
end

-- MySQL's message for an entry of global index space `sp` that `tuple`
-- could not take, "Duplicate entry '<key>' for key '<unique key>'" (the
-- space's primary index has the unique key's name), or nil for other spaces.
function UIX.dup(sp, tuple)
    if sp == nil or tuple == nil or sp.name:sub(1, #UIX.prefix) ~= UIX.prefix then return nil end
    local parts = {}
    for i = 1, #sp.index[0].parts do parts[i] = UIX.key_text(tuple[i]) end
    return string.format("Duplicate entry '%s' for key '%s'", table.concat(parts, '-'), sp.index[0].name)
end

-- The global index spaces of table space `sid`.
function UIX.spaces(sid)
    local out, tag = {}, UIX.prefix .. sid .. '_'
    for _, s in box.space._space.index.name:pairs({tag}, {iterator = 'GE'}) do
        if s.name:sub(1, #tag) ~= tag then break end
        if s.name:sub(#tag + 1):match('^%d+$') then out[#out + 1] = box.space[s.id] end
    end
    return out
end

local function failure(e)
    if type(e) == 'cdata' or type(e) == 'table' then
        local code = e.code or 0
        if code == box.error.TRANSACTION_CONFLICT then return conflict(tostring(e.message)) end
        if code == box.error.TUPLE_FOUND then
            local msg = tostring(e.message)
            local sp = box.space[e.space or msg:match('in space "([^"]+)"') or '']
            return {'duplicate', 1062, UIX.dup(sp, e.new_tuple) or pending_label(msg)}
        end
        return {'error', code, tostring(e.message or e)}
    end
    return {'error', 0, tostring(e)}
end

-- failure() for a local transaction that is the whole commit (CommitLocal,
-- CommitRC, autocommit) or a PREPARE: ER_SYNC_ROLLBACK means the leader
-- changed while the transaction waited for its quorum and the new leader
-- rolled it back -- it took no effect anywhere, so the client may retry
-- (1213). Not for APPLY, whose transaction is already decided.
local function commit_failure(e)
    if (type(e) == 'cdata' or type(e) == 'table') and e.code == box.error.VY_QUOTA_TIMEOUT then
        M.vinyl.quota_timeouts = M.vinyl.quota_timeouts + 1
        return {'error', 1205, 'Timed out waiting for Vinyl memory quota; transaction rolled back'}
    end
    if type(e) == 'cdata' and e.code == box.error.SYNC_ROLLBACK then
        return conflict('shard leader changed during the commit, retry the transaction (' .. tostring(e.message) .. ')')
    end
    return failure(e)
end

-------------------------------------------------------------------------------
-- Row locks: READ COMMITTED writes and locking reads (TarnCommit: plocks)
-------------------------------------------------------------------------------
-- InnoDB-style pessimistic locks, kept in memory on the leader only. A lock
-- is named by (space_id, index_id, key): a row's primary key, or a value of
-- a unique key (so that concurrent inserts of one value wait for each
-- other). Modes: S (shared) and X (exclusive). A request that conflicts
-- with a holder, or with an earlier waiter, queues; it fails with 1205
-- after its timeout, or with 1213 at once if waiting would close a cycle in
-- the wait-for graph of this shard (cross-shard cycles are found by the SQL
-- servers' detector, which picks a victim through tarn_lock_victim).
-- Locks end with the transaction: its local commit, APPLY or ABORT, an
-- explicit tarn_unlock, or the end of the connection that took them.
--
-- The table is volatile: a leader change empties it (new generation), and
-- a transaction whose locks were dropped is BROKEN. Commits carry the
-- generation the transaction locked under, and are refused unless that
-- generation is current and the transaction is not broken, so a write is
-- never applied after its lock was lost (TarnCommit: BugRCNoLockCheck).
--
-- Reading under a lock. TarnCommit takes a local commit as one atomic step;
-- here box.commit() yields until the WAL (and the replication quorum) has
-- the transaction, and until then other readers still see the previous
-- versions of its rows. Writers that hold no row lock (autocommit
-- statements, serializable commits) check for row locks before they write,
-- so the only commits that can touch a row whose lock was just granted are
-- those already in flight. tarn_lock waits for them before it reads
-- (wait_commits): otherwise it would return a version about to be replaced,
-- and a write computed from it would erase that commit's update. Only the
-- commits that write the locked key matter (TarnCommit: InFlight(k)): once a
-- commit has done its writes it records them (inflight_writes), and a grant
-- waits only for the commits whose writes touch its key, or that have not
-- recorded theirs yet.

-- A write's plan: its space, primary key, old and new tuple, result, and
-- whether it is skipped (an UPDATE of a missing row).
local P_SPACE, P_PK, P_OLD, P_NEW, P_RES, P_SKIP = 1, 2, 3, 4, 5, 6

-- The bucket checks of a commit step (see "Buckets"): the buckets of its
-- writes (wb[i]: the bucket of writes[i]) must take writes here, and so
-- must those of the rows it writes (their bucket_id: this also refuses the
-- writes of SQL servers that name no buckets); the buckets its reads were
-- routed by must be owned -- for a PREPARE (fence_reads) also take writes:
-- its read locks would hold a move up. Its reads of every shard must not
-- cover rows of a bucket held here. A status, or nil. Does not yield; call
-- BK.fresh() first.
function BK.commit_check(wb, rpoints, rranges, fence_reads, plans, mver)
    local r = BK.member_refused(mver)
    if r then return r end
    if BK.count == 0 then return nil end
    local b = BK.refused(wb, true) or BK.reads_refused(rpoints, rranges, fence_reads) or BK.rows_refused(plans)
    if b then return BK.moved(b) end
    b = BK.held_conflict(rpoints, rranges)
    if b then return conflict(string.format('read the rows of bucket %d, which is moving to this shard', b)) end
    if plans ~= nil then BK.count_writes(plans) end
    return nil
end

-- (The writes of a commit step whose checks passed: see BK.load.)
function BK.count_writes(plans)
    BK.load_rotate()
    for _, p in ipairs(plans) do
        local b = BK.of_plan(p)
        if b ~= nil then
            local t = p[P_RES] or p[P_NEW]   -- (an update's row: its result, once applied)
            BK.load_add(b, 0, 1, type(t) == 'cdata' and t:bsize() or 0)
        end
    end
end

-- The bucket of the row a planned write writes (nil: none, or a space
-- without buckets).
function BK.of_plan(p)
    local t = p[P_NEW] or p[P_OLD]
    local f = t ~= nil and BK.field(p[P_SPACE]) or nil
    return f ~= nil and t[f] or nil
end

-- The first bucket of a row the planned writes write that takes no writes
-- here, or nil.
function BK.rows_refused(plans)
    for _, p in ipairs(plans or {}) do
        local b = BK.of_plan(p)
        if type(b) == 'number' and BK.bad(b, true) then return b end
    end
    return nil
end

-- INFLIGHT[seq], while that local commit is in flight: the plans of its
-- writes, or true while they are not known (it may write anything).
local INFLIGHT = {}
local INFLIGHT_SEQ = 0
local INFLIGHT_COND = fiber.cond()

-- A local commit that writes user rows is in flight from before its first
-- check to the end of its box.commit(): whatever starts meanwhile and must
-- not miss its writes (a row lock grant, a schema change) waits for it.
local function inflight_begin()
    INFLIGHT_SEQ = INFLIGHT_SEQ + 1
    INFLIGHT[INFLIGHT_SEQ] = true
    return INFLIGHT_SEQ
end

-- The commit `seq` has done all its writes (`plans`): from now on only lock
-- grants on what they write wait for it.
local yield_point
local function inflight_writes(seq, plans)
    if INFLIGHT[seq] then
        INFLIGHT[seq] = plans
        INFLIGHT_COND:broadcast()
    end
    yield_point('written')
end

local function inflight_end(seq)
    if INFLIGHT[seq] then
        INFLIGHT[seq] = nil
        INFLIGHT_COND:broadcast()
    end
end

-- box.commit() of a local transaction that wrote user rows and registered
-- nothing earlier.
local function tracked_commit()
    local seq = inflight_begin()
    local ok, err = pcall(box.commit)
    inflight_end(seq)
    if not ok then error(err) end
end

-- Yields ----------------------------------------------------------------------
-- A vinyl read that misses the cache yields while the page is read, and so
-- may a vinyl write (an insert's duplicate check, the old tuple a secondary
-- index needs). TarnCommit takes each commit step (CommitLocal, CommitRC,
-- autocommit writes, Prepare) as one atomic action, so each is split:
--   1. the storage reads and writes, which may yield -- inside the step's
--      local transaction, so MVCC aborts the commit if anything it read is
--      changed by another commit before this one is prepared;
--   2. the checks (definition fence, row locks, prepared transactions'
--      locks), the lock-index updates and the start of box.commit(), in one
--      section that does not yield (`atomic`): the checks see the state the
--      commit is prepared in, as in the model.
-- Local commits register as in flight before step 1 (inflight_begin).
-- `check_yields` makes a yield inside an atomic section an error (tests);
-- `yield_point` marks the places step 1 may yield, where tests can hold a
-- step (tarn_test_hook) to run another one in between.
local YIELD_HOOKS = {}   -- tests: name -> {cond = fiber.cond(), held = true}

function yield_point(name)
    local h = YIELD_HOOKS[name]
    if h == nil then return end
    h.reached = h.reached + 1
    h.cond:broadcast()
    if h.reached > 1 then return end   -- only the first step to get here is held
    local deadline = clock.monotonic() + 10
    while h.held and clock.monotonic() < deadline do h.cond:wait(0.05) end
end

local function atomic(what, fn, ...)
    if not M.cfg.check_yields then return fn(...) end
    local f = fiber.self()
    local c0 = f:csw()
    local r = fn(...)
    if f:csw() ~= c0 then error('tarn: ' .. what .. ' yielded inside its atomic section') end
    return r
end

-- tarn_test_hook(name, 'hold' | 'release' | 'reached'): tests only. 'hold'
-- holds the next step that reaches yield point `name` there (later ones pass)
-- until 'release'; 'reached' waits until one has.
function tarn_test_hook(name, action)
    if not M.cfg.test_hooks then return {'error', 0, 'test hooks are off'} end
    local h = YIELD_HOOKS[name]
    if action == 'hold' then
        YIELD_HOOKS[name] = {cond = fiber.cond(), held = true, reached = 0}
    elseif action == 'release' and h ~= nil then
        YIELD_HOOKS[name] = nil
        h.held = false
        h.cond:broadcast()
    elseif action == 'reached' then
        -- wait (up to 5 s) until a step reached the held point
        local deadline = clock.monotonic() + 5
        while h ~= nil and (h.reached or 0) == 0 and clock.monotonic() < deadline do h.cond:wait(0.05) end
        return {'ok', h ~= nil and h.reached or 0}
    end
    BK.ns_hooks()
    return {'ok'}
end

-- While a hook is held, the native procedures hand every request to the Lua
-- ones, whose steps it holds.
function BK.ns_hooks()
    if BK.ns == nil then return end
    local n = 0
    for _ in pairs(YIELD_HOOKS) do n = n + 1 end
    BK.ns.hooks = n
end

-- Does the in-flight commit `e` (INFLIGHT entry) write a row whose key in
-- index iid of space sid is `key` (before or after the write)? A tuple the
-- key definition cannot compare (written before ALTER TABLE added the
-- index's fields) counts as a match.
local function key_is(kd, t, key)
    if t == nil then return false end
    local ok, c = pcall(kd.compare_with_key, kd, t, key)
    return not ok or c == 0
end

local function writes_key(e, sid, iid, key)
    if e == true then return true end
    local kd
    for i = 1, #e do
        local p = e[i]
        if p[P_SPACE].id == sid then
            kd = kd or index_key_def(sid, iid)
            if key_is(kd, p[P_OLD], key) or key_is(kd, p[P_NEW], key) then return true end
        end
    end
    return false
end

-- Wait until every local commit in flight now has ended -- with sid, every
-- one of them that writes `key` of index iid of that space (false: deadline).
-- The native procedures' commits in flight now count as writing anything:
-- they end without a broadcast, so they are polled.
local function wait_commits(deadline, sid, iid, key)
    local upto = INFLIGHT_SEQ
    local ns = BK.ns
    local upto_native = ns ~= nil and ns.commit_seq or nil
    while true do
        local busy = false
        for seq, e in pairs(INFLIGHT) do
            if seq <= upto and (sid == nil or writes_key(e, sid, iid, key)) then
                busy = true
                break
            end
        end
        local native = upto_native ~= nil and ns.oldest_inflight <= upto_native
        if not busy and not native then return true end
        local left = deadline - clock.monotonic()
        if left <= 0 then return false end
        M.stats.commit_waits = M.stats.commit_waits + 1
        INFLIGHT_COND:wait(math.min(left, busy and 0.05 or 0.001))
    end
end

local PTX = {}        -- PTX[txid] = {keys = {[kid] = {sid, iid, kb}}, sessions = {}, waiting = request}
local PL_SESS = {}    -- PL_SESS[session id] = {[txid] = true}
local PL_BROKEN = {}  -- PL_BROKEN[txid] = time after which the entry can be forgotten
local PL_LIVE = {}    -- PL_LIVE[session id] = true: a connected session that took row locks
local PL_GEN = uuid.str()
local PL_STATE = nil  -- 'term/ro' the table belongs to
local PL_WAIT_SEQ = 0
M.pl_cond = fiber.cond()   -- broadcast whenever row locks are released

local LOCK_TIMEOUT_MSG = 'Lock wait timeout exceeded; try restarting transaction'
local DEADLOCK_MSG = 'Deadlock found when trying to get lock; try restarting transaction'
local NOWAIT_MSG = 'Statement aborted because lock(s) could not be acquired immediately and NOWAIT is set.'
local BROKEN_MSG = 'the transaction lost its row locks (leader change or lost connection); it was rolled back'

local function pl_conflict(a, b) return a == 'X' or b == 'X' end

local function pl_entry(sid, iid, kb, create)
    local s = PL[sid]
    if s == nil then
        if not create then return nil end
        s = {}; PL[sid] = s
    end
    local i = s[iid]
    if i == nil then
        if not create then return nil end
        i = {}; s[iid] = i
    end
    local e = i[kb]
    if e == nil and create then
        e = {holders = {}, queue = {}}
        i[kb] = e
    end
    return e
end

local function pl_drop_if_free(sid, iid, kb, e)
    if next(e.holders) == nil and #e.queue == 0 then
        local i = PL[sid] and PL[sid][iid]
        if i ~= nil and i[kb] == e then i[kb] = nil end
    end
end

local function pl_txn(txid)
    local p = PTX[txid]
    if p == nil then
        p = {keys = {}, sessions = {}}
        PTX[txid] = p
    end
    return p
end

-- Grantable now: compatible with every other holder and -- unless the
-- transaction already holds the lock (an upgrade jumps the queue) -- with
-- every earlier waiter among the first `upto` of the queue.
local function pl_grantable(e, req, upto)
    for t, m in pairs(e.holders) do
        if t ~= req.txid and pl_conflict(m, req.mode) then return false end
    end
    if e.holders[req.txid] == nil then
        for i = 1, upto do
            local q = e.queue[i]
            if q ~= req and q.txid ~= req.txid and pl_conflict(q.mode, req.mode) then return false end
        end
    end
    return true
end

local function pl_hold(txid, sid, iid, kb, e, mode)
    local cur = e.holders[txid]
    if cur == nil then
        PL_N = PL_N + 1
        BK.ns_pl(sid, 1)
        pl_txn(txid).keys[sid .. '\0' .. iid .. '\0' .. kb] = {sid, iid, kb}
    end
    if cur ~= 'X' then e.holders[txid] = mode end
end

local function pl_grant_waiters(sid, iid, kb, e)
    local i = 1
    while i <= #e.queue do
        local q = e.queue[i]
        if q.failed ~= nil then
            table.remove(e.queue, i)   -- (failed: its fiber reports it)
        elseif pl_grantable(e, q, i - 1) then
            table.remove(e.queue, i)
            pl_hold(q.txid, sid, iid, kb, e, q.mode)
            q.granted = true
            q.cond:signal()
        else
            i = i + 1
        end
    end
end

local function pl_unqueue(sid, iid, kb, e, req)
    for i, q in ipairs(e.queue) do
        if q == req then
            table.remove(e.queue, i)
            break
        end
    end
    pl_grant_waiters(sid, iid, kb, e)
    pl_drop_if_free(sid, iid, kb, e)
end

local function pl_release_key(txid, sid, iid, kb)
    local p = PTX[txid]
    if p ~= nil then p.keys[sid .. '\0' .. iid .. '\0' .. kb] = nil end
    local e = pl_entry(sid, iid, kb, false)
    if e == nil or e.holders[txid] == nil then return end
    e.holders[txid] = nil
    PL_N = PL_N - 1
    BK.ns_pl(sid, -1)
    pl_grant_waiters(sid, iid, kb, e)
    pl_drop_if_free(sid, iid, kb, e)
    M.pl_cond:broadcast()
end

local function pl_release_all(txid)
    local p = PTX[txid]
    if p == nil then return end
    PTX[txid] = nil
    for sess in pairs(p.sessions) do
        local m = PL_SESS[sess]
        if m ~= nil then
            m[txid] = nil
            if next(m) == nil then PL_SESS[sess] = nil end
        end
    end
    if p.waiting ~= nil then
        p.waiting.failed = 'released'
        p.waiting.cond:signal()
    end
    for _, k in pairs(p.keys) do
        local e = pl_entry(k[1], k[2], k[3], false)
        if e ~= nil and e.holders[txid] ~= nil then
            e.holders[txid] = nil
            PL_N = PL_N - 1
            BK.ns_pl(k[1], -1)
            pl_grant_waiters(k[1], k[2], k[3], e)
            pl_drop_if_free(k[1], k[2], k[3], e)
        end
    end
    M.pl_cond:broadcast()
end
M.pl_release_all = pl_release_all

-- A new leader term or a read-only change empties the table: its locks
-- belonged to the previous leadership (waiters fail, commits under the old
-- generation are refused).
local function pl_fresh()
    local st = tostring(box.info.election.term) .. '/' .. tostring(box.info.ro)
    if st == PL_STATE then return end
    for _, p in pairs(PTX) do
        if p.waiting ~= nil then
            p.waiting.failed = 'reset'
            p.waiting.cond:signal()
        end
    end
    PL, PTX, PL_SESS, PL_BROKEN, PL_LIVE, PL_N = {}, {}, {}, {}, {}, 0
    BK.ns_pl()
    PL_GEN = uuid.str()
    PL_STATE = st
    M.pl_cond:broadcast()
end
M.pl_fresh = pl_fresh

-- The row locks held now, by space, into the native procedures' state (as
-- it is made: BK.ns_init).
function BK.ns_pl_held()
    BK.ns_pl(nil)
    for _, p in pairs(PTX) do
        for _, k in pairs(p.keys) do BK.ns_pl(k[1], 1) end
    end
end

-- The transactions a waiting request waits for: conflicting holders and,
-- unless it is an upgrade, conflicting requests queued before it.
local function pl_waits_for(req)
    local out, e = {}, req.entry
    for t, m in pairs(e.holders) do
        if t ~= req.txid and pl_conflict(m, req.mode) then out[t] = true end
    end
    if e.holders[req.txid] == nil then
        for _, q in ipairs(e.queue) do
            if q == req then break end
            if q.txid ~= req.txid and pl_conflict(q.mode, req.mode) then out[q.txid] = true end
        end
    end
    return out
end

-- Would `txid`, waiting on `req`, close a cycle of waiting transactions?
local function pl_deadlock(txid, req)
    local seen, stack = {}, {}
    for t in pairs(pl_waits_for(req)) do stack[#stack + 1] = t end
    while #stack > 0 do
        local t = table.remove(stack)
        if t == txid then return true end
        if not seen[t] then
            seen[t] = true
            local p = PTX[t]
            if p ~= nil and p.waiting ~= nil then
                for u in pairs(pl_waits_for(p.waiting)) do stack[#stack + 1] = u end
            end
        end
    end
    return false
end

-- Take one lock: 'granted', 'skipped', or an error status table.
local function pl_acquire(txid, sid, iid, kb, mode, opts, deadline, bucket)
    local e = pl_entry(sid, iid, kb, true)
    -- (the bucket of the row: a move breaks the locks on its rows)
    if e.bucket == nil and type(bucket) == 'number' then e.bucket = bucket end
    local cur = e.holders[txid]
    if cur == 'X' or cur == mode then return 'granted' end
    -- held: the other shards where txid holds row locks, as its SQL server
    -- sent them; reported with the wait to the cross-shard detector.
    local held = opts.held
    if type(held) ~= 'table' then held = nil end
    local req = {txid = txid, mode = mode, entry = e, cond = fiber.cond(), held = held}
    if pl_grantable(e, req, #e.queue) then
        pl_hold(txid, sid, iid, kb, e, mode)
        return 'granted'
    end
    if opts.skip then
        pl_drop_if_free(sid, iid, kb, e)
        return 'skipped'
    end
    if opts.nowait then
        pl_drop_if_free(sid, iid, kb, e)
        return {'locked', 3572, NOWAIT_MSG}
    end
    if cur ~= nil then table.insert(e.queue, 1, req) else e.queue[#e.queue + 1] = req end
    local p = pl_txn(txid)
    if pl_deadlock(txid, req) then
        pl_unqueue(sid, iid, kb, e, req)
        M.stats.deadlocks = M.stats.deadlocks + 1
        return {'deadlock', 1213, DEADLOCK_MSG}
    end
    PL_WAIT_SEQ = PL_WAIT_SEQ + 1
    req.id = PL_WAIT_SEQ
    p.waiting = req
    M.stats.plock_waits = M.stats.plock_waits + 1
    while not req.granted and req.failed == nil do
        local left = deadline - clock.monotonic()
        if left <= 0 then break end
        req.cond:wait(left)
    end
    if p.waiting == req then p.waiting = nil end
    if req.granted then return 'granted' end
    if req.failed == 'reset' then error(box.error.new(box.error.READONLY)) end
    pl_unqueue(sid, iid, kb, e, req)
    if req.failed == 'deadlock' then
        M.stats.deadlocks = M.stats.deadlocks + 1
        return {'deadlock', 1213, DEADLOCK_MSG}
    end
    if req.failed == 'released' then return {'error', 1213, BROKEN_MSG} end
    M.stats.lock_timeouts = M.stats.lock_timeouts + 1
    return {'timeout', 1205, LOCK_TIMEOUT_MSG}
end

local function pl_register_session(txid)
    local sess = box.session.id()
    PL_LIVE[sess] = true
    local p = pl_txn(txid)
    if not p.sessions[sess] then
        p.sessions[sess] = true
        local m = PL_SESS[sess]
        if m == nil then m = {}; PL_SESS[sess] = m end
        m[txid] = true
    end
end

-- The generation of the locks a session takes: the lock table's generation
-- and the session id (session ids are never reused while the instance runs).
local function pl_session_gen()
    return PL_GEN .. '/' .. box.session.id()
end

-- Are the row locks `txid` took under generation `gen` all still held?
-- (nil gen: it took none on this shard.) They are lost when the table is
-- reset (new leader term, read-only change: PL_GEN changes) or when the
-- session that took them disconnects (PL_LIVE; PL_BROKEN also refuses the
-- transaction's further lock requests for a while).
local function pl_intact(txid, gen)
    if gen == nil or gen == box.NULL then return true end
    pl_fresh()
    if PL_BROKEN[txid] ~= nil then return false end
    local g, sess = string.match(gen, '^(.*)/(%d+)$')
    return g == PL_GEN and PL_LIVE[tonumber(sess)] == true
end

-- When the connection of a session that took row locks closes, they are
-- released and their transactions are broken (their commits are refused).
local function pl_on_disconnect()
    PL_LIVE[box.session.id()] = nil
    local m = PL_SESS[box.session.id()]
    if m == nil then return end
    PL_SESS[box.session.id()] = nil
    for txid in pairs(m) do
        if PTX[txid] ~= nil then
            PL_BROKEN[txid] = clock.monotonic() + M.cfg.max_txn_age + M.cfg.clock_skew
            pl_release_all(txid)
        end
    end
end

-- An error after locks were granted: they stay held until the transaction
-- ends (as in InnoDB), so the reply carries their generation as a fourth
-- element, and the SQL server releases them with the transaction's other
-- locks even when this was its first request here.
local function pl_failed(txid, r)
    local p = PTX[txid]
    if p ~= nil and next(p.keys) ~= nil then r[4] = pl_session_gen() end
    return r
end

-- tarn_lock(txid, keys, mode, opts): take row locks for txid, in order.
--   keys : {{space_id, index_id, key, bucket}, ...}   (index 0: primary key;
--          else a unique key value; bucket: of the row, see "Buckets")
--   mode : 'X' or 'S'
--   opts : {timeout = seconds (50), nowait = bool, skip = bool, read = bool,
--           held = {shard id, ...}: the other shards where txid holds row
--           locks, for the cross-shard deadlock detector (tarn_lock_waits)}
-- Returns {'ok', generation, results} (generation: see pl_session_gen; a
-- different one than before means the transaction's earlier locks on this
-- shard were lost); results[i] is, with read, the latest
-- committed tuple with that index key (box.NULL if none), else true; false
-- for a key skipped (skip: locked by another transaction). Before reading,
-- a granted request waits out prepared transactions' W locks on the key
-- (their value may already be committed: TarnCommit BugRCLockIgnoresWLocks).
-- Errors: {'locked', 3572} (nowait), {'timeout', 1205}, {'deadlock', 1213},
-- {'error', 1213} (the transaction's locks were lost). Locks granted before
-- an error stay held until the transaction ends (as in InnoDB); the error
-- then carries their generation (pl_failed).
-- The start of tarn_lock and tarn_lock_range: an error status, or nil and
-- the deadline of the request.
local function pl_begin(txid, opts)
    writable()
    lk_fresh()
    pl_fresh()
    BK.fresh()
    if PL_BROKEN[txid] ~= nil then return {'error', 1213, BROKEN_MSG} end
    local timeout = opts.timeout
    if timeout == nil or timeout == box.NULL then timeout = 50 end
    pl_register_session(txid)
    return nil, clock.monotonic() + timeout
end

-- Take one row lock of tarn_lock: false (skipped), true (granted; with
-- opts.read the latest committed tuple, or box.NULL if none), or an error
-- status table (pl_failed).
local function pl_take(txid, sid, iid, key, kb, mode, opts, deadline, bucket)
    -- A bucket this shard does not own, or that a move fences, takes no
    -- row locks (TarnCommit: Fenced in RCLock).
    if type(bucket) == 'number' and BK.count > 0 and BK.bad(bucket, true) then
        return pl_failed(txid, BK.moved(bucket))
    end
    local r = pl_acquire(txid, sid, iid, kb, mode, opts, deadline, bucket)
    if r == 'skipped' then return false end
    if r ~= 'granted' then return pl_failed(txid, r) end
    if not wait_commits(deadline, sid, iid, key) then
        M.stats.lock_timeouts = M.stats.lock_timeouts + 1
        return pl_failed(txid, {'timeout', 1205, LOCK_TIMEOUT_MSG})
    end
    -- (A table held by a TRUNCATE or DROP TABLE counts as W-locked.)
    if point_w_locked(txid, sid, iid, key) or DDL_HELD[sid] ~= nil then
        if opts.skip then
            pl_release_key(txid, sid, iid, kb)
            return false
        elseif opts.nowait then
            return pl_failed(txid, {'locked', 3572, NOWAIT_MSG})
        end
        while point_w_locked(txid, sid, iid, key) or DDL_HELD[sid] ~= nil do
            if clock.monotonic() > deadline then
                M.stats.lock_timeouts = M.stats.lock_timeouts + 1
                return pl_failed(txid, {'timeout', 1205, LOCK_TIMEOUT_MSG})
            end
            M.stats.lock_waits = M.stats.lock_waits + 1
            M.unlock_cond:wait(0.01)
        end
    end
    if box.space[sid] == nil then   -- dropped while it waited (the lock is held)
        return pl_failed(txid, {'error', 1146, 'no such space ' .. tostring(sid)})
    end
    if not opts.read then return true end
    local t = box.space[sid].index[iid]:get(key)
    return (t == nil) and box.NULL or t
end

function tarn_lock(txid, keys, mode, opts)
    opts = opts or {}
    local err, deadline = pl_begin(txid, opts)
    if err then return err end
    local results = {}
    for i, k in ipairs(keys) do
        local sid, iid, key = k[1], k[2], k[3]
        if box.space[sid] == nil then return pl_failed(txid, {'error', 1146, 'no such space ' .. tostring(sid)}) end
        local r = pl_take(txid, sid, iid, key, kbin(key), mode, opts, deadline, k[4])
        if type(r) == 'table' then return r end
        results[i] = r
    end
    return {OK, pl_session_gen(), results}
end

-- The scans of tarn_select and their pages (below, with tarn_select; one
-- table, as lua/tarn.lua is at Lua's limit of 200 locals in a chunk).
local PG = {OOM = {}}   -- PG.OOM: raised by a scan that stops for want of heap

-- tarn_lock_range(txid, ranges, mode, opts): the locking reads of READ
-- COMMITTED `SELECT ... WHERE <primary key prefix> [ORDER BY <primary key>]
-- LIMIT n FOR UPDATE | FOR SHARE [NOWAIT | SKIP LOCKED]` statements in one
-- call (issue #49): for each range in order, scan the primary key from the
-- start of the range and lock each row as tarn_lock does with read (FIFO
-- queues, NOWAIT, SKIP LOCKED, waiting out in-flight commits and prepared
-- transactions' W locks), until `n` rows are locked or the range ends.
--   ranges : {{space_id, prefix, n, skip_keys, bucket}, ...}: the rows whose
--            primary key starts with `prefix` (not empty, and fixing the
--            shard key: one bucket), leaving out the primary keys
--            `skip_keys` (rows the transaction itself deleted).
--   mode, opts : as tarn_lock's (read is implied).
-- The candidates are read a page at a time, each page as tarn_select reads
-- with its lock check (RCRead; see below for SKIP LOCKED); each row is read
-- again under its lock
-- (RCLock), and one deleted meanwhile has its lock released at once unless
-- the transaction held it already. The rows found and the order of the
-- lock requests are those of a locking read that pages through the range
-- from the SQL server.
-- Under SKIP LOCKED a page is read without the lock check: a row that a
-- prepared transaction W-locks is skipped under its lock anyway, where the
-- separate read and lock request used to wait for it (or fail with 1213).
-- Returns {'ok', generation, {rows, ...}}: for each range, the rows locked
-- (their latest committed versions). Errors as tarn_lock's.
function tarn_lock_range(txid, ranges, mode, opts)
    opts = opts or {}
    local err, deadline = pl_begin(txid, opts)
    if err then return err end
    local read = {timeout = opts.timeout, nowait = opts.nowait, skip = opts.skip, read = true, held = opts.held}
    local out = {}
    for i, rg in ipairs(ranges) do
        local sid, prefix, n, skip_keys, bucket = rg[1], rg[2], rg[3], rg[4], rg[5]
        local space = box.space[sid]
        if space == nil then return pl_failed(txid, {'error', 1146, 'no such space ' .. tostring(sid)}) end
        -- (the prefix fixes the shard key: one bucket)
        if type(bucket) == 'number' and BK.count > 0 and BK.bad(bucket, true) then
            return pl_failed(txid, BK.moved(bucket))
        end
        local idx = space.index[0]
        local kd = index_key_def(sid, 0)
        local skip = {}
        for _, k in ipairs(skip_keys or {}) do skip[kbin(k)] = true end
        local rows, last = {}, nil
        while #rows < n do
            local page = math.min(math.max(2 * (n - #rows), 16), 512)
            local key, it = prefix, 'GE'
            if last ~= nil then key, it = last, 'GT' end
            local r = PG.read(function()
                if opts.skip then
                    -- Every candidate is read again under its lock, and one a
                    -- prepared transaction W-locks is skipped: no need to wait
                    -- for those (a row such a transaction inserts would be).
                    return {OK, PG.scan(idx, kd, it, key, page, prefix, true, nil, false, nil)}
                end
                return PG.checked(space, idx, kd, it, key, page, prefix, true, nil, false, nil)
            end)
            if r[1] ~= OK then return pl_failed(txid, r) end
            local cands = r[2]
            for j = 1, #cands do
                local pk = kd:extract_key(cands[j]):totable()
                last = pk
                local kb = kbin(pk)
                if not skip[kb] then
                    local e = pl_entry(sid, 0, kb, false)
                    local had = e ~= nil and e.holders[txid] ~= nil
                    local t = pl_take(txid, sid, 0, pk, kb, mode, read, deadline, bucket)
                    if type(t) == 'table' then return t end   -- an error status
                    if t == box.NULL then
                        if not had then pl_release_key(txid, sid, 0, kb) end   -- deleted meanwhile
                    elseif t then
                        rows[#rows + 1] = t
                        if #rows >= n then break end
                    end
                end
            end
            -- The end of the range (a page cut short by its bytes says more).
            if #cands < page and r[3] == nil then break end
            -- Not in a transaction or an atomic step: let other requests run
            -- between pages (the SQL server asks for at most 256 rows a range).
            fiber.yield()
        end
        out[i] = rows
        fiber.yield()   -- and between ranges
    end
    return {OK, pl_session_gen(), out}
end

-- tarn_unlock(txid[, keys]): release some row locks of txid ({{space_id,
-- index_id, key}, ...}: READ COMMITTED drops the locks of rows that turned
-- out not to match), or all of them (rollback, end of a read-only
-- transaction).
function tarn_unlock(txid, keys)
    if keys == nil or keys == box.NULL then
        pl_release_all(txid)
        PL_BROKEN[txid] = nil
    else
        for _, k in ipairs(keys) do pl_release_key(txid, k[1], k[2], kbin(k[3])) end
    end
    return {OK}
end

-- Wait-for edges of this shard for the cross-shard deadlock detector:
-- {{waiter, wait id, {holder, ...}, {shard id, ...}}, ...}, the last list
-- being the other shards where the waiter holds row locks (opts.held of its
-- request): there the detector looks for the transactions waiting for it.
function tarn_lock_waits()
    local out = {}
    for txid, p in pairs(PTX) do
        local q = p.waiting
        if q ~= nil and not q.granted and q.failed == nil then
            local hs = {}
            for t in pairs(pl_waits_for(q)) do hs[#hs + 1] = t end
            out[#out + 1] = {txid, q.id, hs, q.held or {}}
        end
    end
    return out
end

-- Fail the wait `wait_id` of `txid` with 1213 if it is still pending (the
-- detector found it on a cycle). Returns true if it was.
function tarn_lock_victim(txid, wait_id)
    local p = PTX[txid]
    local q = p and p.waiting
    if q ~= nil and q.id == wait_id and not q.granted and q.failed == nil then
        q.failed = 'deadlock'
        q.cond:signal()
        return true
    end
    return false
end

-------------------------------------------------------------------------------
-- Applying writes to the plain spaces
-------------------------------------------------------------------------------

-- One write: {op, space_id, payload[, ops]}
--   insert  payload = full tuple (fails on duplicate key)
--   replace payload = full tuple
--   delete  payload = primary key
--   update  payload = primary key, ops = Tarantool update ops (autocommit only)
local function apply_write(w)
    local op, space = w[1], box.space[w[2]]
    if space == nil then error(box.error.new(box.error.NO_SUCH_SPACE, tostring(w[2]))) end
    if op == 'insert' then return space:insert(w[3])
    elseif op == 'replace' then return space:replace(w[3])
    elseif op == 'delete' then return space:delete(w[3])
    elseif op == 'update' then return space:update(w[3], w[4])
    end
    error('unknown write op ' .. tostring(op))
end

-- Primary key and new tuple (nil for delete) of a write {op, space_id, payload}.
local function write_target(space, w)
    if w[1] == 'delete' or w[1] == 'update' then return w[3], nil end
    local t = box.tuple.new(w[3])
    return index_key_def(space.id, 0):extract_key(t):totable(), t
end

-- Apply one write inside the current local transaction, refusing it if it
-- conflicts with a lock of another prepared transaction. Returns (result)
-- or (nil, holder); the caller rolls the local transaction back on conflict.
-- Tarantool update ops with MySQL semantics: '+' on a NULL field leaves it
-- NULL (NULL + d = NULL) instead of failing.
local function mysql_update_ops(old, ops)
    local out
    for i, op in ipairs(ops) do
        if op[1] == '+' and old[op[2]] == nil then
            if out == nil then
                out = {}
                for j = 1, i - 1 do out[j] = ops[j] end
            end
        elseif out ~= nil then
            out[#out + 1] = op
        end
    end
    return out or ops
end

-- Exact comparison of integers that may be Lua numbers or int64_t / uint64_t
-- cdata. LuaJIT compares a mix of signed and unsigned 64-bit values as
-- unsigned, so on its own -1 > 9223372036854775807ULL (issue #11).
local function int_negative(x)
    if type(x) == 'number' then return x < 0 end
    return ffi.istype('int64_t', x) and x < 0
end

local function int_lt(a, b)
    local na, nb = int_negative(a), int_negative(b)
    if na ~= nb then return na end
    if na then return ffi.cast('int64_t', a) < ffi.cast('int64_t', b) end
    return ffi.cast('uint64_t', a) < ffi.cast('uint64_t', b)
end
M.int_lt = int_lt

-- (check_arith's helpers are local to its block: the main chunk is at
-- Lua's limit of 200 locals)
local check_arith
do
local D_I64_MIN = decimal.new('-9223372036854775808')
local D_I64_MAX = decimal.new('9223372036854775807')
local D_U64_MAX = decimal.new('18446744073709551615')
local EXACT = 2 ^ 52

-- a + b without rounding: a Lua number when both are small, else a decimal.
local function exact_add(a, b)
    if type(a) == 'number' and type(b) == 'number' and a > -EXACT and a < EXACT and b > -EXACT and b < EXACT then
        return a + b
    end
    return decimal.new(a) + decimal.new(b)
end

-- MySQL's checks for an integer column changed by arithmetic (UPDATE ...
-- SET c = c + d): the arithmetic is BIGINT -- BIGINT UNSIGNED for an
-- unsigned column -- and fails with 1690 outside that domain; the result
-- must then fit the column type, else 1264. ranges: {{field, min, max,
-- column name}}.
function check_arith(old, ops, ranges)
    for _, r in ipairs(ranges) do
        local f, lo, hi = r[1], r[2], r[3]
        local name = r[4] or f
        local v = old[f]
        for _, op in ipairs(ops) do
            if op[2] == f and v ~= nil then
                if op[1] == '+' then
                    v = exact_add(v, op[3])
                    local unsigned = not int_negative(lo)
                    local bad
                    if type(v) == 'number' then
                        bad = unsigned and v < 0
                    elseif unsigned then
                        bad = v < 0 or v > D_U64_MAX
                    else
                        bad = v < D_I64_MIN or v > D_I64_MAX
                    end
                    if bad then
                        error(box.error.new({code = 1690, reason = string.format(
                            "BIGINT%s value is out of range in '`%s` + %s'",
                            unsigned and ' UNSIGNED' or '', tostring(name), tostring(op[3]))}))
                    end
                elseif op[1] == '=' then
                    v = op[3]
                end
            end
        end
        if v ~= nil then
            local out
            if type(v) == 'number' then
                out = int_lt(v, lo) or int_lt(hi, v)
            elseif decimal.is_decimal(v) then
                out = v < decimal.new(lo) or v > decimal.new(hi)
            end
            if out then
                error(box.error.new({code = 1264,
                    reason = "Out of range value for column '" .. tostring(name) .. "' at row 1"}))
            end
        end
    end
end
end   -- (check_arith)

-- Step 1 of one write (see "Yields"): read the row it replaces, work out the
-- tuple it leaves and apply it in the current transaction; may yield.
-- Returns its plan {space, pk, old, new, res, skip} (indexes P_*): `res` is
-- what the write returned, `skip` marks an update of a missing row (nothing
-- written).

-- (write_step is local to write_steps' block: the main chunk is near Lua's
-- limit of 200 locals)
local write_steps
do
local function write_step(w, deadline)
    local space = box.space[w[2]]
    if space == nil then error(box.error.new(box.error.NO_SUCH_SPACE, tostring(w[2]))) end
    local pk, new = write_target(space, w)
    local old = space:get(pk)
    yield_point('write')
    M.vinyl.check_deadline(deadline)
    if w[1] ~= 'update' then
        local res = apply_write(w)
        BK.note(space, old, new)
        return {space, pk, old, new, res, false}
    end
    if old == nil then return {space, pk, nil, nil, nil, true} end
    -- w[6]: {n, v1, v2, ...}: rows written before ALTER TABLE added the
    -- fields past the n-th end early; they hold the values v1, v2, ...
    local pad = w[6]
    if pad ~= nil and pad ~= box.NULL and #old < #pad - 1 + pad[1] then
        local t = old:totable()
        for f = math.max(#t + 1, pad[1] + 1), #pad - 1 + pad[1] do t[f] = pad[f - pad[1] + 1] end
        for f = 1, #t do if t[f] == nil then t[f] = box.NULL end end
        old = box.tuple.new(t)
    end
    -- w[5]: {{field, min, max}} for integer columns changed by arithmetic
    if w[5] ~= nil and #w[5] > 0 then check_arith(old, w[4], w[5]) end
    new = old:update(mysql_update_ops(old, w[4]))
    M.vinyl.check_deadline(deadline)
    local res = space:replace(new)
    BK.note(space, old, new)
    return {space, pk, old, new, res, false}
end

-- write_step for every write, in order, except that a write a unique index
-- refuses (TUPLE_FOUND) is tried again after the others: a transaction may
-- move a unique value from one row to another, and the row giving it up may
-- come later. Returns the plans in the order of `writes`.
function write_steps(writes, deadline)
    local plans, todo = {lazy_initial = M.lazy.initial(writes, write_target)}, {}
    for i = 1, #writes do todo[#todo + 1] = i end
    while true do
        local again, err = {}, nil
        for _, i in ipairs(todo) do
            local ok, p = pcall(write_step, writes[i], deadline)
            if ok then plans[i] = p
            elseif type(p) == 'cdata' and p.code == box.error.TUPLE_FOUND then
                again[#again + 1], err = i, p
            else error(p) end
        end
        if #again == 0 then return plans end
        if #again == #todo then
            -- Values that go round in a cycle (rows that swap them): take
            -- the rows out, then write them (as their old versions' plans).
            local old = {}
            for _, i in ipairs(again) do
                local w = writes[i]
                if w[1] ~= 'replace' then error(err) end
                local sp = box.space[w[2]]
                local pk = write_target(sp, w)
                old[i] = sp:get(pk)
                M.vinyl.check_deadline(deadline)
                sp:delete(pk)
            end
            for _, i in ipairs(again) do
                plans[i] = write_step(writes[i], deadline)
                plans[i][P_OLD] = old[i]
            end
            return plans
        end
        todo = again
    end
end
end

-- Step 2 (no yield): the holder of a lock that one of the writes conflicts
-- with -- a row lock, or a prepared transaction's lock (TarnCommit:
-- writesOK) -- or nil.
local function writes_locked(txid, plans)
    for i = 1, #plans do
        local p = plans[i]
        if not p[P_SKIP] then
            local h = write_conflict(txid, p[P_SPACE], p[P_PK], p[P_OLD], p[P_NEW])
            if h then return h end
        end
    end
    return nil
end

-- The lock index and the fences were current when the step began (lk_fresh,
-- fence_fresh); a leader change since then makes them stale.
local function still_fresh()
    writable()
    return not LK_STALE and not FENCE_STALE and not BK.stale
end
local FRESH_MSG = 'shard leadership changed during the commit'

-- MySQL affected-rows convention for one write: REPLACE of a different row
-- counts 2, an UPDATE or REPLACE that changes nothing counts 0 / 1. With
-- `found_rows` (the client's CLIENT_FOUND_ROWS), an UPDATE counts the row it
-- matched even when it changes nothing.
local function affected(op, old, new, found_rows)
    if op == 'insert' then return 1 end
    if op == 'delete' then return old ~= nil and 1 or 0 end
    local same = old ~= nil and new ~= nil and msgpack.encode(old) == msgpack.encode(new)
    if op == 'replace' then
        if old == nil or same then return 1 end
        return 2
    end
    -- update
    if old == nil or new == nil then return 0 end
    if same and not found_rows then return 0 end
    return 1
end

-- TarnCommit!CommitLocal: called inside the transaction's IPROTO stream after
-- its reads. Validates the read set against W locks (a W lock may belong to a
-- transaction that is committed but not yet applied here), applies the writes
-- with lock checks, and commits; Tarantool's MVCC validates the reads
-- themselves. Returns {'ok', n} or {status, code, message}; on failure the
-- stream's transaction is rolled back.
-- Step 2 of CommitLocal (see "Yields"): its checks, on the state the commit
-- is prepared in.
local function commit_local_checks(txid, lock_gen, fence, rpoints, rranges, plans, wb, mver)
    if not still_fresh() then return conflict(FRESH_MSG) end
    local mv = BK.commit_check(wb, rpoints, rranges, false, plans, mver)
    if mv then return mv end
    -- Rows the transaction locked (FOR UPDATE / FOR SHARE) were read under
    -- those locks, outside its stream: the locks must have survived.
    if not pl_intact(txid, lock_gen) then return {'error', 1213, BROKEN_MSG} end
    local stale = stale_definition(fence) or M.lazy.guard(plans, fence, true)
    if stale then return stale end
    local h = reads_w_locked(txid, rpoints, rranges)
    if h then return conflict('read row locked by prepared transaction ' .. h) end
    h = reads_ddl_conflict(rpoints, rranges)
    if h then return conflict(h) end
    h = writes_locked(txid, plans)
    if h then return conflict('row locked by prepared transaction ' .. h) end
    return nil
end

-- wb: the bucket of each write (see "Buckets"; nil from SQL servers that
-- predate them, which name none). mver: the membership version a
-- transaction that read every shard was routed with (see "Membership").
function tarn_commit_local(txid, writes, rpoints, rranges, lock_gen, fence, wb, mver)
    writable()
    local seq = inflight_begin()
    local ok, res = pcall(function()
        lk_fresh()
        fence_fresh()
        BK.fresh()
        -- Checked again below, where it counts; here to fail before writing.
        if not pl_intact(txid, lock_gen) then return {'error', 1213, BROKEN_MSG} end
        local stale = stale_definition(fence) or BK.commit_check(wb, rpoints, rranges, false, nil, mver)
        if stale then return stale end
        -- Without reads on this shard the coordinator sends no stream: the
        -- commit is then its own local transaction.
        if not box.is_in_txn() then box.begin(RW) end
        local throttle, write_deadline = M.vinyl.guard(writes)
        if throttle then return throttle end
        local plans, n = write_steps(writes, write_deadline), 0
        M.lazy.capture(plans)
        for i = 1, #plans do
            if plans[i][P_RES] ~= nil then n = n + 1 end
        end
        inflight_writes(seq, plans)
        local r = atomic('CommitLocal', commit_local_checks, txid, lock_gen, fence, rpoints, rranges, plans, wb,
                         mver)
        if r ~= nil then return r end
        box.on_commit(function() pl_release_all(txid) end)
        box.commit()
        M.stats.commit_local = M.stats.commit_local + 1
        return {OK, n}
    end)
    inflight_end(seq)
    if not ok then
        if box.is_in_txn() then box.rollback() end
        reraise_readonly(res)
        local f = commit_failure(res)
        if f[2] == 1205 then pl_release_all(txid) end
        return f
    end
    if res[1] ~= OK and box.is_in_txn() then box.rollback() end
    if res[2] == 1205 then pl_release_all(txid) end
    return res
end

-- (STREAM_TERM: see the definition fences above.)
local function note_stream_term()
    local id = box.txn_id()
    if id == nil then return end
    STREAM_TERM[id] = box.info.election.term
    local function forget()
        -- (a native procedure that takes its writes back to a savepoint
        -- runs the on_rollback triggers too: the transaction goes on)
        if BK.ns ~= nil and tonumber(BK.ns.svp_txn) == id then return end
        STREAM_TERM[id] = nil
        if DOOMED[id] ~= nil then
            DOOMED[id], DOOMED_N = nil, DOOMED_N - 1
            BK.ns_sync()
        end
    end
    box.on_commit(forget)
    box.on_rollback(forget)
end

-- Commit of a read-only single-shard transaction (TarnCommit!SnapshotRead):
-- the lock check runs in the same MVCC view as the reads.
-- TarnCommit!ValidateRO (cross = true) / the commit of a single-shard
-- read-only stream (cross = false). Runs inside the reader's stream.
-- Only a writable leader that has been the leader since the stream began
-- may validate: a demoted leader whose connection survived can still serve
-- the stream's reads, but it misses writes committed by its successor, and
-- the probe write below goes to a data-temporary space, which Tarantool
-- accepts even on read-only instances.
function tarn_validate(txid, rpoints, rranges, cross, lock_gen, mver)
    writable()
    if not pl_intact(txid, lock_gen) then
        if box.is_in_txn() then box.rollback() end
        pl_release_all(txid)
        return {'error', 1213, BROKEN_MSG}
    end
    if box.is_in_txn() then
        local t0 = STREAM_TERM[box.txn_id()]
        if t0 ~= nil and t0 ~= box.info.election.term then
            box.rollback()
            return conflict('shard leader changed during the transaction')
        end
    end
    lk_fresh()
    BK.fresh()
    if not box.is_in_txn() and (#(rpoints or {}) > 0 or #(rranges or {}) > 0) then
        return conflict('the stream transaction of these reads no longer exists')
    end
    if box.is_in_txn() then
        -- "The reads are unchanged": memtx MVCC moves a read-only transaction
        -- whose reads were overwritten into a read view (it keeps seeing the
        -- old values and could commit), but refuses any write from a read
        -- view. A probe write turns that state into a conflict. Needed for
        -- single-shard read-only transactions too: the lock check below sees
        -- the current lock index, not the reads' snapshot.
        local ok, err = pcall(function() box.space._tarn_scratch:replace{1} end)
        if not ok then
            if box.is_in_txn() then box.rollback() end
            M.stats.validate_changed = M.stats.validate_changed + 1
            return conflict('read set changed before commit: ' .. tostring(err))
        end
    end
    local h = reads_w_locked(txid, rpoints, rranges)
    if h then
        if box.is_in_txn() then box.rollback() end
        return conflict('read row locked by prepared transaction ' .. h)
    end
    h = reads_ddl_conflict(rpoints, rranges)
    if h then
        if box.is_in_txn() then box.rollback() end
        return conflict(h)
    end
    -- A read routed to a bucket this shard no longer owns, or a read of
    -- every shard that may have missed the rows of a bucket moving here.
    local mv = BK.commit_check(nil, rpoints, rranges, false, nil, mver)
    if mv then
        if box.is_in_txn() then box.rollback() end
        return mv
    end
    if box.is_in_txn() then box.commit() end
    pl_release_all(txid)
    return {OK}
end

M.unlock_cond = fiber.cond()

local ITER = {[0] = 'EQ', [1] = 'REQ', [2] = 'ALL', [3] = 'LT', [4] = 'LE', [5] = 'GE', [6] = 'GT'}

-- tarn_select: a bounded index scan, rows from `key` in `iterator` order,
-- stopping after `limit` rows or once past `end_key` (inclusive if
-- end_incl). Inside a stream the reads are tracked by MVCC. With lock_check
-- (autocommit reads, TarnCommit!SnapshotRead), the scan waits while another
-- transaction holds a W lock on anything it covers; on memtx, scan and lock
-- check run without a yield in between, so they observe one state (vinyl:
-- checked_read). Returns {'ok', tuples[, position]}: one page (see
-- "Pages").
local BEGIN_STREAM = {txn_isolation = 'read-confirmed', timeout = 60}
-- Scans longer than this many tuples yield inside stream transactions.
local SCAN_CHUNK = 1000
BK.scan_chunk = SCAN_CHUNK   -- (for the native procedures: BK.ns_sync)
-- Fiber slice of a long autocommit scan, which may not yield (seconds).
local LONG_SCAN_SLICE = {warn = 1.0, err = 30.0}

-- Pages (issue #77). A read returns at most cfg.scan_page_rows rows, or
-- rows of cfg.scan_page_bytes bytes, per call, and then the position of
-- its last row (`index:tuple_pos`); the caller continues after it. Every
-- row a call returns is held in the Lua heap until the reply is sent, so
-- without pages the heap grew with the range scanned, up to Tarantool's
-- Lua memory limit, where an allocation that failed outside a protected
-- call killed the instance. A read that finds the heap nearly full anyway
-- fails with 1041 instead (PG.read) and frees what it read.

-- Whether less than a reserve of the Lua heap is free (1/16 of the limit,
-- at least 16 MiB): what the replies, other requests and the node itself
-- need while reads run.
-- (A full collection runs at most every 100 ms: a heap that stays near the
-- reserve must not make every read collect it.)
--
-- A heap of garbage near the limit does not empty by itself (issue #73):
-- LuaJIT starts a collection cycle when the heap reaches a threshold, twice
-- what the last cycle left by default, and does not collect when an
-- allocation fails. With the threshold above the limit, every allocation
-- fails, the heap never reaches it, and every call (the recovery fiber's
-- too) fails with "not enough memory" until a manual collect. The heap
-- watchdog (PG.watch_heap) collects such a heap.
do
    local alloc_ok, alloc = pcall(require, 'internal.alloc')
    local collected = -math.huge
    local function reserve() return math.max(alloc.getlimit() / 16, 16 * 1024 * 1024) end
    -- A full collection when the heap is short and none ran for `every` s.
    local function collect(every)
        if clock.monotonic() - collected > every then
            collectgarbage()   -- (what is in use may be mostly garbage)
            collected = clock.monotonic()
        end
    end
    function PG.heap_low()
        if not alloc_ok then return false end
        if alloc.unused() >= reserve() then return false end
        collect(0.1)
        return alloc.unused() < reserve()
    end
    -- Check the heap every 100 ms (with no allocation while it is fine) and
    -- collect it when less than the reserve is free, at most every second
    -- (a heap that is full of live data is collected for nothing).
    function PG.watch_heap()
        if not alloc_ok then return end
        fiber.create(function()
            fiber.name('tarn_heap')
            while true do
                fiber.sleep(0.1)
                if alloc.unused() < reserve() then
                    local ok = pcall(collect, 1)
                    if ok and alloc.unused() >= reserve() then
                        M.stats.heap_collects = M.stats.heap_collects + 1
                    end
                end
            end
        end)
    end
end

function PG.full(n, bytes)
    return n >= M.cfg.scan_page_rows or bytes >= M.cfg.scan_page_bytes
end

-- Run read(...) and return its result, or the 1041 error status when it
-- ran out of heap (it stopped at heap_low, or an allocation failed).
function PG.read(read, ...)
    local ok, r
    if PG.heap_low() then ok, r = false, PG.OOM else ok, r = pcall(read, ...) end
    if ok then return r end
    if r == PG.OOM or tostring(r):find('not enough memory', 1, true) then
        M.stats.read_oom = M.stats.read_oom + 1
        return {'error', 1041, 'Out of memory: the storage node is short of Lua memory for this read'}
    end
    error(r)
end

-- The storage call `fn`, failing with 1041 instead when the heap runs out.
function PG.bounded(fn) return function(...) return PG.read(fn, ...) end end

-- Whether a W lock was taken or released on a key of [lo, hi] of the index
-- since history entry `since` (an autocommit read of a vinyl table): true,
-- or 'gone' when that history is no longer complete.
local function w_logged_since(since, sid, iid, kd, lo, hi, lo_incl, hi_incl)
    if since + 1 < LOG_FLOOR then return 'gone' end
    for seq = since + 1, LOG_SEQ do
        local slot = seq % LOG_N
        if LOG_SID[slot] == sid and LOG_IID[slot] == iid
                and in_range(kd, msgpack.decode(LOG_KB[slot]), lo, hi, lo_incl, hi_incl) then
            return true
        end
    end
    return false
end

-- An autocommit read of a vinyl table, with the lock check (TarnCommit!
-- SnapshotRead): the rows `scan()` returns, read in a state in which no
-- prepared transaction holds a W lock on anything they cover. A memtx scan
-- does not yield, so it and its check see one state (tarn_select). A vinyl
-- scan may (see "Yields"): it runs inside a read-only transaction, so MVCC
-- gives it one snapshot, taken at some moment while it ran, and
-- `locked(rows, since)` covers every W lock taken or released meanwhile (a
-- table) as well as those held now (the holder). When none covered what it
-- read at any moment of the scan, the snapshot is a state in which
-- SnapshotRead could have read the same rows. DDL cache invalidation or a
-- generation change also discards the snapshot: a hold may already have
-- been released by its final check. Otherwise the read waits for the lock
-- and runs again (bounded by lock_wait).
local function checked_read(scan, locked)
    local deadline = clock.monotonic() + M.cfg.lock_wait
    while true do
        VY_SCANS = VY_SCANS + 1
        local s0, d0 = LOG_SEQ, DDL_GEN
        local ok, rows, more = pcall(function()
            box.begin({txn_isolation = 'read-confirmed'})
            return scan()
        end)
        if box.is_in_txn() then box.rollback() end
        VY_SCANS = VY_SCANS - 1
        if not ok then error(rows) end
        local h = atomic('SnapshotRead', function()
            if LK_STALE or FENCE_STALE then return {'stale'} end
            if DDL_GEN ~= d0 then return {'ddl'} end
            if NLOCK == 0 and LOG_SEQ == s0 and DDL_N == 0 then return nil end
            return locked(rows, s0, more)
        end)
        if h == nil then return {OK, rows, more} end
        if type(h) == 'table' and h[1] == 'stale' then
            if clock.monotonic() > deadline then
                return conflict('lock or table definition cache changed during the read')
            end
            lk_fresh()   -- a leader change meanwhile (raises ER_READONLY if demoted)
            fence_fresh()
            if clock.monotonic() > deadline then
                return conflict('lock or table definition cache changed during the read')
            end
        elseif type(h) == 'table' then
            -- A cross-shard write or DDL hold changed while the scan ran:
            -- read again (locks still held are waited out).
            if clock.monotonic() > deadline then
                return conflict('rows or DDL claims changed during the read')
            end
            M.stats.vy_rereads = M.stats.vy_rereads + 1
        else
            if clock.monotonic() > deadline then
                return conflict('row locked by prepared transaction ' .. h)
            end
            M.stats.lock_waits = M.stats.lock_waits + 1
            M.unlock_cond:wait(0.01)
        end
    end
end

-- Bounded index scan of tarn_select: rows from `key` in `it` order (after
-- the position `after` if given), stopping after `limit` rows or once past
-- `end_key`, skipping `offset` rows. At most one page (see "Pages"): returns
-- the rows, and the position of the last one when the page ended the scan
-- before its end.
function PG.scan(idx, kd, it, key, limit, end_key, end_incl, offset, reverse, after)
    local in_txn = box.is_in_txn()
    local unbounded = limit == nil or limit > SCAN_CHUNK
    if unbounded and not in_txn then
        -- A long autocommit scan must be atomic, so it cannot yield: give it
        -- time instead of failing with "fiber slice is exceeded" (#10).
        fiber.set_slice(LONG_SCAN_SLICE)
    end
    local skip = (offset ~= nil) and offset or 0   -- msgpack nil arrives as box.NULL
    if after == box.NULL then after = nil end   -- (msgpack nil)
    if after ~= nil then skip = 0 end   -- (the first page skipped them)
    local max_bytes = M.cfg.scan_page_bytes
    local n = limit
    if n == nil or n > M.cfg.scan_page_rows then n = M.cfg.scan_page_rows end
    if end_key == nil and n > 0 and skip == 0 and not (unbounded and in_txn) then
        local rows = idx:select(key, {iterator = it, limit = n, after = after})
        yield_point('scan')
        local bytes = 0
        for i = 1, #rows do
            bytes = bytes + rows[i]:bsize()
            if bytes >= max_bytes and i < #rows then
                for j = #rows, i + 1, -1 do rows[j] = nil end
                return rows, idx:tuple_pos(rows[i])
            end
        end
        if #rows == n and n ~= limit then return rows, idx:tuple_pos(rows[n]) end
        return rows
    end
    local rows, seen, bytes = {}, 0, 0
    for _, t in idx:pairs(key, {iterator = it, after = after}) do
        seen = seen + 1
        -- Inside a transaction MVCC keeps the view consistent across yields:
        -- let other requests run during long scans.
        if in_txn and seen % SCAN_CHUNK == 0 then fiber.yield() end
        if seen % 256 == 0 and PG.heap_low() then error(PG.OOM) end
        if end_key ~= nil then
            local c = kd:compare_with_key(t, end_key)
            if reverse then
                if c < 0 or (c == 0 and not end_incl) then break end
            else
                if c > 0 or (c == 0 and not end_incl) then break end
            end
        end
        if skip > 0 then skip = skip - 1
        else
            rows[#rows + 1] = t
            if limit ~= nil and #rows >= limit then break end
            bytes = bytes + t:bsize()
            if PG.full(#rows, bytes) then return rows, idx:tuple_pos(t) end
        end
    end
    return rows
end

-- The holder of a W lock on what a tarn_select scan covered, a table when a
-- W lock was there at some moment since history entry `since` (vinyl), or
-- nil. Covered: from the start key to the end key, or to the last row
-- returned when the limit or the page (`more`) cut the scan short. (A page
-- that continues an earlier one is checked from the start key: more than
-- it covered, which can only make it wait more.)
local function select_locked(space_id, index_id, idx, kd, it, key, limit, end_key, reverse, rows, since, more)
    if DDL_HELD[space_id] ~= nil then return DDL_HELD[space_id] end
    local lo, hi, h
    if (it == 'EQ' or it == 'REQ') and idx.unique and #key == #idx.parts then
        lo, hi = key, key
        h = point_w_locked('', space_id, index_id, key)
    else
        local first = (#key > 0) and key or nil
        local last = end_key
        if ((limit ~= nil and #rows >= limit) or more ~= nil) and #rows > 0 then
            last = kd:extract_key(rows[#rows]):totable()
        end
        if it == 'EQ' or it == 'REQ' then lo, hi = first, first
        elseif reverse then lo, hi = last, first
        else lo, hi = first, last end
        h = range_w_locked('', space_id, index_id, lo, hi, true, true)
    end
    if h == nil and since ~= nil then
        local w = w_logged_since(since, space_id, index_id, kd, lo, hi, true, true)
        if w then return {w} end
    end
    return h
end

-- The lock-checked scan of tarn_select (TarnCommit!SnapshotRead, RCRead):
-- {OK, rows[, position]} read in a state in which no prepared transaction
-- W-locks what they cover, or the conflict after waiting lock_wait.
function PG.checked(space, idx, kd, it, key, limit, end_key, end_incl, offset, reverse, after)
    local space_id, index_id = space.id, idx.id
    if space.engine == 'vinyl' and not box.is_in_txn() then
        return checked_read(
            function() return PG.scan(idx, kd, it, key, limit, end_key, end_incl, offset, reverse, after) end,
            function(rows, since, more)
                return select_locked(space_id, index_id, idx, kd, it, key, limit, end_key, reverse, rows, since, more)
            end)
    end
    -- memtx: scan and check without a yield in between observe one state.
    local deadline = clock.monotonic() + M.cfg.lock_wait
    while true do
        local rows, more = PG.scan(idx, kd, it, key, limit, end_key, end_incl, offset, reverse, after)
        if NLOCK == 0 and DDL_N == 0 then return {OK, rows, more} end
        local h = select_locked(space_id, index_id, idx, kd, it, key, limit, end_key, reverse, rows, nil, more)
        if h == nil then return {OK, rows, more} end
        if clock.monotonic() > deadline then
            return conflict('row locked by prepared transaction ' .. h)
        end
        M.stats.lock_waits = M.stats.lock_waits + 1
        M.unlock_cond:wait(0.01)
    end
end

-- tarn_select(space_id, index_id, key, iterator, limit, end_key, end_incl,
-- lock_check, offset, begin, after, route): one page of the scan (see
-- "Pages"), {'ok', tuples[, position]}; with a position, call again with it
-- as `after` for the next page. route: the bucket the read was routed by,
-- or {e = epoch} (see "Buckets").
function tarn_select(space_id, index_id, key, iterator, limit, end_key, end_incl, lock_check, offset, begin, after,
                     route)
    local mv = BK.check_read(route)
    if mv then return mv end
    -- First request of a stream: start the stream's transaction here (it
    -- stays open after the call because the request carries a stream id).
    if begin == true and not box.is_in_txn() then
        box.begin(BEGIN_STREAM)
        note_stream_term()
    end
    if lock_check == true then
        -- An autocommit read is only linearizable on the current leader.
        lk_fresh()
        -- (and the tables held by a TRUNCATE or DROP TABLE, which the
        -- check below consults, are current: also for ts_select)
        fence_fresh()
    end
    local space = box.space[space_id]
    if space == nil then return {'error', 1146, 'no such space ' .. tostring(space_id)} end
    local idx = space.index[index_id]
    if idx == nil then return {'error', 1412, STALE_DEF_MSG} end   -- dropped by ALTER TABLE
    local it = type(iterator) == 'number' and ITER[iterator] or string.upper(tostring(iterator or 'EQ'))
    key = key or {}
    local kd = index_key_def(space_id, index_id)
    local reverse = (it == 'LT' or it == 'LE' or it == 'REQ')
    if lock_check ~= true then
        return {OK, PG.scan(idx, kd, it, key, limit, end_key, end_incl, offset, reverse, after)}
    end
    return PG.checked(space, idx, kd, it, key, limit, end_key, end_incl, offset, reverse, after)
end

tarn_select = PG.bounded(BK.recheck(tarn_select, 12))

-- Rows of several keys of one index in one call (issue #10: IN lists, ORs of
-- equalities, row-value IN). For each key: the row with that key when it is
-- a full key of a unique index, else every row whose index key starts with
-- it (at most `limit` per key). Rows come back key by key. lock_check,
-- begin and route as in tarn_select (route: the keys' buckets, when they
-- were routed). One page at a time (see "Pages"): the reply
-- {'ok', rows[, resume]} carries, when the page ended early, where the next
-- call starts: resume = {i, position, n} continues with keys[i], after the
-- row at `position` (box.NULL: from its first row), n of its rows returned
-- already; pass it as `after`.
local function keys_scan(idx, keys, limit, after)
    local in_txn = box.is_in_txn()
    local nparts = #idx.parts
    local rows, n, bytes = {}, 0, 0
    local first, pos, done = 1, nil, 0
    if after ~= nil and after ~= box.NULL then
        first, done = after[1], after[3]
        if after[2] ~= box.NULL then pos = after[2] end
    end
    for i = first, #keys do
        local key = keys[i]
        if PG.full(#rows, bytes) then return rows, {i, box.NULL, 0} end
        if idx.unique and #key == nparts then
            local t = idx:get(key)
            if t ~= nil then
                rows[#rows + 1] = t
                bytes = bytes + t:bsize()
            end
        else
            local got = (i == first) and done or 0
            for _, t in idx:pairs(key, {iterator = 'EQ', after = (i == first) and pos or nil}) do
                rows[#rows + 1] = t
                got = got + 1
                if limit ~= nil and got >= limit then break end
                bytes = bytes + t:bsize()
                if got % 256 == 0 and PG.heap_low() then error(PG.OOM) end
                if PG.full(#rows, bytes) then return rows, {i, idx:tuple_pos(t), got} end
            end
        end
        n = n + 1
        -- Inside a transaction MVCC keeps the view consistent across yields;
        -- let other requests run on long key lists.
        if in_txn and n % 256 == 0 then fiber.yield() end
    end
    return rows
end

local function keys_locked(space_id, index_id, idx, kd, keys, since)
    if DDL_HELD[space_id] ~= nil then return DDL_HELD[space_id] end
    local nparts = #idx.parts
    for _, key in ipairs(keys) do
        local h
        if idx.unique and #key == nparts then
            h = point_w_locked('', space_id, index_id, key)
        else
            h = range_w_locked('', space_id, index_id, key, key, true, true)
        end
        if h ~= nil then return h end
        if since ~= nil then
            local w = w_logged_since(since, space_id, index_id, kd, key, key, true, true)
            if w then return {w} end
        end
    end
    return nil
end

function tarn_select_keys(space_id, index_id, keys, limit, lock_check, begin, after, route)
    local mv = BK.check_read(route)
    if mv then return mv end
    if begin == true and not box.is_in_txn() then
        box.begin(BEGIN_STREAM)
        note_stream_term()
    end
    if lock_check == true then lk_fresh() end
    local space = box.space[space_id]
    if space == nil then return {'error', 1146, 'no such space ' .. tostring(space_id)} end
    local idx = space.index[index_id]
    if idx == nil then return {'error', 1412, STALE_DEF_MSG} end   -- dropped by ALTER TABLE
    if lock_check ~= true then return {OK, keys_scan(idx, keys, limit, after)} end
    -- (The lock check covers every key, also those of other pages.)
    if space.engine == 'vinyl' and not box.is_in_txn() then
        local kd = index_key_def(space_id, index_id)
        return checked_read(
            function() return keys_scan(idx, keys, limit, after) end,
            function(_, since) return keys_locked(space_id, index_id, idx, kd, keys, since) end)
    end
    local deadline = clock.monotonic() + M.cfg.lock_wait
    while true do
        local rows, more = keys_scan(idx, keys, limit, after)
        if NLOCK == 0 and DDL_N == 0 then return {OK, rows, more} end
        local h = keys_locked(space_id, index_id, idx, nil, keys, nil)
        if h == nil then return {OK, rows, more} end
        if clock.monotonic() > deadline then
            return conflict('row locked by prepared transaction ' .. h)
        end
        M.stats.lock_waits = M.stats.lock_waits + 1
        M.unlock_cond:wait(0.01)
    end
end

tarn_select_keys = PG.bounded(BK.recheck(tarn_select_keys, 8))

-- A co-located join (issue #83), read as dependent primary-key prefixes.
-- reads[i] = {space_id, parent, bindings}; parent is 0 (constants only),
-- or an earlier 1-based slot. A binding is {0, native_value} or
-- {1, native_field_of_parent}. Rust proves co-location, types/collations
-- and SQL eligibility; storage never evaluates SQL or joins tuples.
-- Reply slots[i] = {prefixes_actually_read, tuples}. All slots share the
-- same snapshot, W-lock-history interval, and row/key/byte budget. A cap
-- or an absent physical source field discards everything: {OK, {}, false}
-- asks Rust to use its existing executor, never a partial SQL result.
function PG.join_plan(reads)
    local bad = {'error', 1210, 'Incorrect arguments to tarn_select_join'}
    if type(reads) ~= 'table' or #reads == 0 or #reads > 16 then return nil, bad end
    local plan = {}
    for i, r in ipairs(reads) do
        if type(r) ~= 'table' or #r ~= 3 or type(r[1]) ~= 'number' or r[1] % 1 ~= 0
           or r[1] <= 0 or type(r[2]) ~= 'number' or r[2] % 1 ~= 0 or r[2] < 0 or r[2] >= i
           or type(r[3]) ~= 'table' or #r[3] == 0 then return nil, bad end
        local sp = box.space[r[1]]
        if sp == nil then return nil, {'error', 1146, 'no such space ' .. tostring(r[1])} end
        local idx = sp.index[0]
        if idx == nil then return nil, {'error', 1412, STALE_DEF_MSG} end
        if #r[3] > #idx.parts then return nil, bad end
        for _, b in ipairs(r[3]) do
            if type(b) ~= 'table' or #b ~= 2 then return nil, bad end
            if b[1] == 1 then
                if r[2] == 0 or type(b[2]) ~= 'number' or b[2] % 1 ~= 0 or b[2] < 1
                   or b[2] > plan[r[2]].fields then return nil, bad end
            elseif b[1] ~= 0 or b[2] == nil or type(b[2]) == 'table' then
                return nil, bad
            end
        end
        plan[i] = {space = sp, idx = idx, kd = index_key_def(sp.id, 0), parent = r[2], bindings = r[3],
                   fields = #sp:format()}
    end
    return plan
end

function PG.join_scan(plan)
    local slots, nrows, nkeys, bytes, work = {}, 0, 0, 0, 0
    local in_txn = box.is_in_txn()
    local function progress()
        work = work + 1
        if work % 256 == 0 and PG.heap_low() then error(PG.OOM) end
        if in_txn and work % SCAN_CHUNK == 0 then fiber.yield() end
    end
    for i, p in ipairs(plan) do
        local keys, rows, seen = {}, {}, {}
        slots[i] = {keys, rows}
        local parents = p.parent == 0 and {false} or slots[p.parent][2]
        for _, parent in ipairs(parents) do
            local key, null = {}, false
            for j, b in ipairs(p.bindings) do
                local v = b[2]
                if b[1] == 1 then
                    -- An instant-added field can be absent but hold a
                    -- non-NULL SQL fill: only Rust knows that fill.
                    if b[2] > #parent then return {}, false end
                    v = parent[b[2]]
                end
                if v == nil or v == box.NULL then null = true; break end
                key[j] = v
            end
            if not null then
                local kb = msgpack.encode(key)
                if not seen[kb] then
                    if nkeys >= 4096 or bytes + #kb > M.cfg.scan_page_bytes then return {}, false end
                    seen[kb] = true
                    nkeys, bytes = nkeys + 1, bytes + #kb
                    keys[#keys + 1] = key -- includes a probe that finds no row
                    for _, t in p.idx:pairs(key, {iterator = 'EQ'}) do
                        local size = t:bsize()
                        if nrows >= M.cfg.scan_page_rows or bytes + size > M.cfg.scan_page_bytes then
                            return {}, false
                        end
                        rows[#rows + 1] = t
                        nrows, bytes = nrows + 1, bytes + size
                        progress()
                    end
                end
            end
            progress() -- duplicate and NULL source keys must also yield fairly
        end
        yield_point('join_scan') -- tests: after one table, before the next
    end
    return slots, true
end

function tarn_select_join(reads, lock_check, begin, route)
    local mv = BK.check_read(route)
    if mv then return mv end
    if type(lock_check) ~= 'boolean' or type(begin) ~= 'boolean' or (lock_check and begin) then
        return {'error', 1210, 'Incorrect arguments to tarn_select_join'}
    end
    if lock_check then
        lk_fresh()
        -- A restart or new leader may know no cached DDL holds yet.
        -- Restore them before checked_read can use its no-lock fast path.
        fence_fresh()
    end
    local plan, bad = PG.join_plan(reads)
    if bad then return bad end
    if begin and not box.is_in_txn() then
        box.begin(BEGIN_STREAM)
        note_stream_term()
    end
    if not lock_check then
        if not box.is_in_txn() then return {'error', 1210, 'tarn_select_join needs a stream transaction'} end
        return {OK, PG.join_scan(plan)}
    end
    -- Every engine uses one read-confirmed transaction: memtx scans may
    -- yield too, and mixing individually checked tables would fracture it.
    if box.is_in_txn() then return {'error', 1210, 'tarn_select_join lock check needs no stream transaction'} end
    return checked_read(function() return PG.join_scan(plan) end, function(slots, since, complete)
        if not complete then return nil end -- discarded, no rows observed
        for i, p in ipairs(plan) do
            if #slots[i][1] > 0 then
                local h = keys_locked(p.space.id, 0, p.idx, p.kd, slots[i][1], since)
                if h ~= nil then return h end
            end
        end
        return nil
    end)
end

tarn_select_join = PG.bounded(BK.recheck(tarn_select_join, 4))

-- tarn_count: the number of rows a tarn_select scan (`key`, `iterator`,
-- `end_key`, `end_incl`) would return that satisfy every residual predicate
-- of `preds` and whose primary key is not in `exclude` (rows the calling
-- transaction has written, which it counts itself), without returning them
-- (issue #52). A predicate is {field, op, value, fill, int}: the tuple field
-- (1-based; `fill` when the tuple ends before it), 'eq' | 'ne' | 'lt' |
-- 'le' | 'gt' | 'ge' against `value`, 'in' against an array of values, or
-- 'null' | 'notnull'; `int` compares integers exactly, else strings by
-- bytes. A comparison with NULL never holds, as in SQL. An equality prefix
-- or a whole index without predicates is counted by the index. lock_check,
-- begin and route as in tarn_select, which it mirrors. Returns {'ok', n}.
local function pred_cmp(a, b, int)
    if int and not (type(a) == 'number' and type(b) == 'number') then
        if int_lt(a, b) then return -1 end
        if int_lt(b, a) then return 1 end
        return 0
    end
    if a < b then return -1 end
    if a > b then return 1 end
    return 0
end

local function preds_match(t, preds)
    local len = #t
    for i = 1, #preds do
        local p = preds[i]
        local op = p[2]
        local v
        if p[1] <= len then v = t[p[1]] else v = p[4] end
        if op == 'null' then
            if v ~= nil then return false end
        elseif v == nil then
            return false
        elseif op == 'in' then
            local any = false
            for _, x in ipairs(p[3]) do
                if pred_cmp(v, x, p[5]) == 0 then any = true; break end
            end
            if not any then return false end
        elseif op ~= 'notnull' then
            local c = pred_cmp(v, p[3], p[5])
            local ok
            if op == 'eq' then ok = c == 0
            elseif op == 'ne' then ok = c ~= 0
            elseif op == 'lt' then ok = c < 0
            elseif op == 'le' then ok = c <= 0
            elseif op == 'gt' then ok = c > 0
            else ok = c >= 0 end
            if not ok then return false end
        end
    end
    return true
end

-- Whether the scan (`key`, `it`, `end_key`, `end_incl`) covers tuple t.
local function scan_covers(kd, t, it, key, end_key, end_incl, reverse)
    if #key > 0 and it ~= 'ALL' then
        local c = kd:compare_with_key(t, key)
        if (it == 'EQ' or it == 'REQ') and c ~= 0 then return false end
        if (it == 'GE' and c < 0) or (it == 'GT' and c <= 0) then return false end
        if (it == 'LE' and c > 0) or (it == 'LT' and c >= 0) then return false end
    end
    if end_key ~= nil then
        local c = kd:compare_with_key(t, end_key)
        if reverse then
            if c < 0 or (c == 0 and not end_incl) then return false end
        else
            if c > 0 or (c == 0 and not end_incl) then return false end
        end
    end
    return true
end

local function count_scan(space_id, idx, kd, it, key, end_key, end_incl, reverse, preds, exclude)
    local in_txn = box.is_in_txn()
    if not in_txn then fiber.set_slice(LONG_SCAN_SLICE) end   -- see PG.scan
    local n = 0
    if #preds == 0 and end_key == nil and (it == 'EQ' or it == 'ALL') then
        n = idx:count(key, {iterator = it})
        yield_point('scan')
    else
        local seen = 0
        for _, t in idx:pairs(key, {iterator = it}) do
            seen = seen + 1
            -- Inside a transaction MVCC keeps the view consistent across
            -- yields: let other requests run during long scans.
            if in_txn and seen % SCAN_CHUNK == 0 then fiber.yield() end
            if end_key ~= nil then
                local c = kd:compare_with_key(t, end_key)
                if reverse then
                    if c < 0 or (c == 0 and not end_incl) then break end
                else
                    if c > 0 or (c == 0 and not end_incl) then break end
                end
            end
            if preds_match(t, preds) then n = n + 1 end
        end
    end
    -- The stored versions of rows the caller wrote are not its to count.
    -- (In a stream these gets are reads MVCC tracks: a concurrent commit to
    -- such a row can only abort the caller earlier, as it writes the row.)
    local pk = box.space[space_id].index[0]
    for _, k in ipairs(exclude) do
        local t = pk:get(k)
        if t ~= nil and scan_covers(kd, t, it, key, end_key, end_incl, reverse) and preds_match(t, preds) then
            n = n - 1
        end
    end
    return n
end

function tarn_count(space_id, index_id, key, iterator, end_key, end_incl, preds, exclude, lock_check, begin, route)
    local mv = BK.check_read(route)
    if mv then return mv end
    if begin == true and not box.is_in_txn() then
        box.begin(BEGIN_STREAM)
        note_stream_term()
    end
    if lock_check == true then lk_fresh() end
    local space = box.space[space_id]
    if space == nil then return {'error', 1146, 'no such space ' .. tostring(space_id)} end
    local idx = space.index[index_id]
    if idx == nil then return {'error', 1412, STALE_DEF_MSG} end   -- dropped by ALTER TABLE
    local it = type(iterator) == 'number' and ITER[iterator] or string.upper(tostring(iterator or 'EQ'))
    key = key or {}
    if end_key == box.NULL then end_key = nil end
    preds = preds or {}
    exclude = exclude or {}
    local kd = index_key_def(space_id, index_id)
    local reverse = (it == 'LT' or it == 'LE' or it == 'REQ')
    local function scan()
        return count_scan(space_id, idx, kd, it, key, end_key, end_incl, reverse, preds, exclude)
    end
    if lock_check ~= true then return {OK, scan()} end
    if space.engine == 'vinyl' and not box.is_in_txn() then
        return checked_read(scan, function(_, since)
            return select_locked(space_id, index_id, idx, kd, it, key, nil, end_key, reverse, nil, since)
        end)
    end
    -- memtx: count and check without a yield in between observe one state.
    local deadline = clock.monotonic() + M.cfg.lock_wait
    while true do
        local n = scan()
        if NLOCK == 0 and DDL_N == 0 then return {OK, n} end
        local h = select_locked(space_id, index_id, idx, kd, it, key, nil, end_key, reverse, nil, nil)
        if h == nil then return {OK, n} end
        if clock.monotonic() > deadline then
            return conflict('row locked by prepared transaction ' .. h)
        end
        M.stats.lock_waits = M.stats.lock_waits + 1
        M.unlock_cond:wait(0.01)
    end
end

-- (A count holds no rows; it fails with 1041 like a read if the heap runs
-- out anyway.)
tarn_count = PG.bounded(BK.recheck(tarn_count, 11))

-- tarn_estimate: row counts that choose the access path of an ordered read
-- with LIMIT (issue #81). Range A, the equality prefix `a_key` of index
-- `a_index`, would be read whole and sorted; range B, the prefix `b_key` of
-- `b_index`, follows ORDER BY and would be paged until enough rows qualify,
-- about budget/n_a * n_b rows when A's conditions are what B filters on.
-- Returns {'ok', n_a, n_b, nil}: memtx counts both in its index (no scan).
-- Vinyl, whose counts scan, measures instead what paging B would read
-- (issue #93), when `b_iter` ('EQ', or 'REQ' for a backwards page) says B
-- is its whole prefix: it reads B's first page (4 * budget rows, as the
-- paged read's first page): when it holds budget rows of A's prefix,
-- paging reads that page; with fewer, it reads about as many rows as their
-- share of the page says (the later pages, which double, may read up to
-- twice that); a B shorter than the page is read whole. Then it counts A
-- only up to that cost. Rows of A's prefix stand in for rows that satisfy
-- WHERE, which may be fewer: a selective condition outside A's index makes
-- paging read more than the cost, as with the counts of memtx. That returns
-- {'ok', n_a, 0, cost}: B wins when cost < n_a. Without a sample, or when
-- the page holds no row of A, it gives the index size as n_b (at least B's
-- rows) and counts A only up to the point where paging B is sure to read
-- fewer rows. Either count stops at one page (cfg.scan_page_rows, issue
-- #77): n_a is 0 when the answer lies past it, which keeps the plan.
-- Neither is a read of the transaction: the counts only pick between two
-- paths to the same rows.
function tarn_estimate(space_id, a_index, a_key, b_index, b_key, budget, b_iter)
    local space = box.space[space_id]
    if space == nil then return {'error', 1146, 'no such space ' .. tostring(space_id)} end
    local a, b = space.index[a_index], space.index[b_index]
    if a == nil or b == nil then return {'error', 1412, STALE_DEF_MSG} end
    if space.engine ~= 'vinyl' then
        return {OK, a:count(a_key, {iterator = 'EQ'}), b:count(b_key, {iterator = 'EQ'}), box.NULL}
    end
    local page = M.cfg.scan_page_rows
    fiber.set_slice(LONG_SCAN_SLICE)   -- see PG.scan
    if (b_iter == 'EQ' or b_iter == 'REQ') and budget <= page / 4 then
        local kd = index_key_def(space_id, a_index)
        local sample, seen, hits, cost = budget * 4, 0, 0, nil
        for _, t in b:pairs(b_key, {iterator = b_iter}) do
            seen = seen + 1
            if kd:compare_with_key(t, a_key) == 0 then
                hits = hits + 1
                -- (the paged read fetches its whole first page anyway)
                if hits == budget then cost = sample; break end
            end
            if seen >= sample then break end
        end
        if cost == nil and seen < sample then
            cost = seen                     -- B ends within its first page
        elseif cost == nil and hits > 0 then
            cost = math.ceil(sample * budget / hits)
        end
        if cost ~= nil then
            if cost >= page then return {OK, 0, 0, cost} end
            local na = 0
            for _ in a:pairs(a_key, {iterator = 'EQ'}) do
                na = na + 1
                if na > cost then break end
            end
            return {OK, na, 0, cost}
        end
    end
    local nb = b:len()
    -- (budget * nb > page^2 means cap > page; tested before the product,
    -- which a huge budget would overflow)
    if budget > page * page / math.max(nb, 1) then return {OK, 0, nb, box.NULL} end
    local cap = math.floor(math.sqrt(budget * nb)) + 1
    if cap > page then return {OK, 0, nb, box.NULL} end
    local na = 0
    for _ in a:pairs(a_key, {iterator = 'EQ'}) do
        na = na + 1
        if na >= cap then break end
    end
    return {OK, na, nb, box.NULL}
end

function tarn_read(space_id, index_id, key, iterator, limit, offset)
    return tarn_select(space_id, index_id, key, iterator, limit, nil, true, true, offset)
end

-- Step 2 of an autocommit write (see "Yields"). 'plocked': a row lock is in
-- the way (wait for it); 'locked': a prepared transaction's lock.
local function autocommit_checks(fence, plans, wb)
    if not still_fresh() then return conflict(FRESH_MSG) end
    local st = stale_definition(fence) or M.lazy.guard(plans, fence, true)
               or BK.commit_check(wb, nil, nil, false, plans)
    if st then return st end
    if PL_N > 0 then
        for i = 1, #plans do
            local p = plans[i]
            local ph = write_plocked('', p[P_SPACE], p[P_PK], p[P_NEW])
            if ph then return {'plocked', ph} end
        end
    end
    local h = writes_locked('', plans)
    if h then return {'locked', h} end
    return nil
end

-- Autocommit DML outside any stream: one local transaction. Waits (bounded)
-- for conflicting locks of prepared cross-shard txns to clear, then retries.
-- Returns {'ok', affected_rows, last_tuple}.
-- opts.wb: the bucket of each write (see "Buckets").
function tarn_autocommit(writes, opts)
    lk_fresh()   -- raises ER_READONLY before anything is written
    pl_fresh()
    local fence = opts and opts.fence
    local wb = opts and opts.wb
    local found_rows = opts and opts.found_rows == true
    local deadline = clock.monotonic() + M.cfg.lock_wait
    -- Rows held by another transaction's row lock: wait (holding nothing,
    -- so this cannot deadlock) up to the session's lock wait timeout, as an
    -- InnoDB autocommit statement waits for row locks.
    local lock_timeout = opts and opts.lock_timeout
    if lock_timeout == nil or lock_timeout == box.NULL then lock_timeout = 50 end
    local pl_deadline = clock.monotonic() + lock_timeout
    while true do
        local seq = inflight_begin()
        local ok, res = pcall(function()
            fence_fresh()
            BK.fresh()
            local stale = stale_definition(fence) or BK.commit_check(wb, nil, nil, false)
            if stale then return stale end
            box.begin(RW)
            local throttle, write_deadline = M.vinyl.guard(writes)
            if throttle then box.rollback(); return throttle end
            local plans, n, last = write_steps(writes, write_deadline), 0, nil
            M.lazy.capture(plans)
            for i = 1, #writes do
                local p = plans[i]
                n = n + affected(writes[i][1], p[P_OLD], p[P_RES], found_rows)
                if p[P_RES] ~= nil then last = p[P_RES] end
            end
            inflight_writes(seq, plans)
            local r = atomic('autocommit', autocommit_checks, fence, plans, wb)
            if r ~= nil then
                box.rollback()
                return r
            end
            box.commit()
            return {OK, n, last}
        end)
        inflight_end(seq)
        if not ok then
            if box.is_in_txn() then box.rollback() end
            reraise_readonly(res)
            return commit_failure(res)
        end
        if res[1] == 'plocked' then
            if clock.monotonic() > pl_deadline then
                M.stats.lock_timeouts = M.stats.lock_timeouts + 1
                return {'timeout', 1205, LOCK_TIMEOUT_MSG}
            end
            M.stats.plock_waits = M.stats.plock_waits + 1
            M.pl_cond:wait(math.min(0.1, math.max(0.001, pl_deadline - clock.monotonic())))
            deadline = clock.monotonic() + M.cfg.lock_wait
        elseif res[1] ~= 'locked' then
            return res
        else
            if clock.monotonic() > deadline then
                return conflict('row locked by prepared transaction ' .. res[2])
            end
            M.stats.lock_waits = M.stats.lock_waits + 1
            M.unlock_cond:wait(0.01)
        end
    end
end

-- Step 2 of CommitRC (see "Yields").
local function commit_rc_checks(txid, gen, fence, plans, wb)
    if not still_fresh() then return conflict(FRESH_MSG) end
    if not pl_intact(txid, gen) then return {'error', 1213, BROKEN_MSG} end
    local st = stale_definition(fence) or M.lazy.guard(plans, fence, true)
               or BK.commit_check(wb, nil, nil, false, plans)
    if st then return st end
    local h = writes_locked(txid, plans)
    if h then return {'locked', h} end
    return nil
end

-- TarnCommit!CommitRC: the commit of a READ COMMITTED transaction whose
-- writes are all on this shard. Its row locks must have survived (same
-- generation, not broken); prepared transactions' locks on what it writes
-- are waited out (bounded); the writes are applied in one local
-- transaction, and the row locks are released once it has committed.
-- Returns {'ok', n} or {status, code, message}.
-- wb: the bucket of each write (see "Buckets").
function tarn_commit_rc(txid, writes, gen, fence, wb)
    writable()
    local seq   -- the attempt in flight, if any
    local ok, res = pcall(function()
        lk_fresh()
        if not pl_intact(txid, gen) then
            pl_release_all(txid)
            return {'error', 1213, BROKEN_MSG}
        end
        local deadline = clock.monotonic() + M.cfg.lock_wait
        while true do
            fence_fresh()
            BK.fresh()
            local stale = stale_definition(fence) or BK.commit_check(wb, nil, nil, false)
            if stale then
                pl_release_all(txid)
                return stale
            end
            seq = inflight_begin()
            box.begin(RW)
            local throttle, write_deadline = M.vinyl.guard(writes)
            if throttle then box.rollback(); pl_release_all(txid); return throttle end
            local plans, n = write_steps(writes, write_deadline), 0
            M.lazy.capture(plans)
            for i = 1, #plans do
                if plans[i][P_RES] ~= nil then n = n + 1 end
            end
            inflight_writes(seq, plans)
            local r = atomic('CommitRC', commit_rc_checks, txid, gen, fence, plans, wb)
            if r == nil then
                box.on_commit(function() pl_release_all(txid) end)
                local okc, cerr = pcall(box.commit)
                inflight_end(seq)
                if not okc then error(cerr) end
                M.stats.commit_rc = M.stats.commit_rc + 1
                return {OK, n}
            end
            box.rollback()
            inflight_end(seq)
            if r[1] ~= 'locked' then
                if r[2] == 1213 or r[2] == 1412 then pl_release_all(txid) end
                return r
            end
            if clock.monotonic() > deadline then
                return conflict('row locked by prepared transaction ' .. r[2])
            end
            M.stats.lock_waits = M.stats.lock_waits + 1
            M.unlock_cond:wait(0.01)
        end
    end)
    if seq then inflight_end(seq) end
    if not ok then
        if box.is_in_txn() then box.rollback() end
        reraise_readonly(res)
        local f = commit_failure(res)
        if f[2] == 1205 then pl_release_all(txid) end
        return f
    end
    return res
end

-------------------------------------------------------------------------------
-- Cross-shard commit: participant side
-------------------------------------------------------------------------------

local function now() return clock.realtime() end

-- Expiry of a record written now: no PREPARE of its transaction can be
-- accepted after this time (the transaction started before now, by at most
-- clock_skew on another node's clock, and PREPAREs older than max_txn_age
-- are refused).
local function expiry() return now() + M.cfg.max_txn_age + M.cfg.clock_skew end

-- TarnCommit!Prepare. Runs inside the transaction's stream on this shard.
--   txid, dshard, parts : transaction id, decision shard, participant shards
--   writes  : {{op, space_id, tuple_or_key}, ...}  (op: insert/replace/delete)
--   rpoints : {{space_id, index_id, key}, ...}       point reads
--   rranges : {{space_id, index_id, lo, hi, lo_incl, hi_incl}, ...}  scans
--   txn_ts  : coordinator's start time (prepares older than max_txn_age refused)
-- Returns {'yes'}, {'no', code, message} (leaves a tombstone), or
-- {'expired', 1213, message} for a transaction older than max_txn_age: no
-- record is written and the reply is neither a YES nor a NO (TarnCommit:
-- Prepare requires t \notin expired), because a delayed or duplicated
-- PREPARE may meet a participant whose records were already collected after
-- the transaction committed. Raises ER_READONLY on a read-only instance.
-- Step 2 of Prepare (see "Yields"): readsOK, writesOK and recOK on the state
-- the PREPARE is prepared in, then the locks and the records. Returns
-- {code, message} for a NO vote, or nil.
local P_KEYS = 7
local function prepare_locks(txid, dshard, parts, writes, rpoints, rranges, lock_gen, fence, plans, wb, mver)
    if not still_fresh() then return {1213, FRESH_MSG} end
    if not pl_intact(txid, lock_gen) then return {1213, BROKEN_MSG} end
    if stale_definition(fence) then return {1412, STALE_DEF_MSG} end
    local capture = M.lazy.guard(plans, fence, true)
    if capture then return {capture[2], capture[3]} end
    capture = M.lazy.reserve(txid, plans)
    if capture then return capture end
    -- The buckets: owned, not fenced by a move (it would wait for these
    -- locks), and no read of every shard missing a moving bucket's rows.
    local mv = BK.commit_check(wb, rpoints, rranges, true, plans, mver)
    if mv then return {mv[2], mv[3], mv[1]} end
    -- A decision shard that is leaving the cluster (see BK.prep).
    local ds = BK.member_state(dshard)
    if ds ~= nil and ds ~= 'active' then
        M.stats.moved = M.stats.moved + 1
        return {1213, string.format('decision shard %d is leaving the cluster', dshard), 'moved'}
    end
    -- readsOK: no W lock of another prepared txn on anything we read.
    local h = reads_w_locked(txid, rpoints, rranges)
    if h then return {1213, 'read row locked by ' .. h} end
    h = reads_ddl_conflict(rpoints, rranges)
    if h then return {1213, h} end
    -- writesOK
    h = writes_locked(txid, plans)
    if h then return {1213, 'write row locked by ' .. h} end
    -- recOK: the decision shard must not have a record yet (fenced => abort).
    if dshard == M.cfg.shard_id then
        if box.space._tarn_txn:get{txid} ~= nil then
            return {1213, 'transaction already resolved by recovery'}
        end
        box.space._tarn_txn:insert{txid, 'staging', parts, now(), expiry()}
    end
    -- If this local transaction does not commit, its locks never existed.
    -- Registered before the first index entry: any error from here on rolls
    -- the transaction back (vote_no), which undoes the entries.
    box.on_rollback(function() lk_release(txid) end)
    local L = box.space._tarn_locks
    local wlocked = {}
    -- Each lock names the bucket of its row when the request did (a move
    -- waits for the locks on its rows), msgpack.NULL otherwise.
    local function tag(b) return type(b) == 'number' and b or msgpack.NULL end
    for i, w in ipairs(writes) do
        local p = plans[i]
        local sid = p[P_SPACE].id
        local pkb = kbin(p[P_PK])
        -- (the request's bucket, else the row's: an older SQL server names none)
        local b = tag(type(wb) == 'table' and wb[i] or BK.of_plan(p))
        wlocked[sid .. ':0:' .. pkb] = true
        lk_point(txid, sid, 0, pkb, 'W', b ~= msgpack.NULL and b or nil)
        L:replace{sid, 0, pkb, txid, 'W', w[1], w[1] ~= 'delete' and w[3] or msgpack.NULL, b}
        for _, k in ipairs(p[P_KEYS]) do
            local kb = kbin(k[2])
            wlocked[sid .. ':' .. k[1] .. ':' .. kb] = true
            lk_point(txid, sid, k[1], kb, 'W', b ~= msgpack.NULL and b or nil)
            L:replace{sid, k[1], kb, txid, 'W', msgpack.NULL, msgpack.NULL, b}
        end
    end
    for _, r in ipairs(rpoints) do
        local kb = kbin(r[3])
        if not wlocked[r[1] .. ':' .. r[2] .. ':' .. kb] then
            local b = tag(r[4])
            lk_point(txid, r[1], r[2], kb, 'R', b ~= msgpack.NULL and b or nil)
            L:replace{r[1], r[2], kb, txid, 'R', msgpack.NULL, msgpack.NULL, b}
        end
    end
    for i, r in ipairs(rranges) do
        local lo_incl, hi_incl = r[5] ~= false, r[6] ~= false
        local b = tag(r[7])
        lk_range(txid, r[1], r[2], r[3], r[4], lo_incl, hi_incl, b ~= msgpack.NULL and b or nil)
        box.space._tarn_rlocks:replace{txid, i, r[1], r[2], r[3], r[4], lo_incl, hi_incl, b}
    end
    box.space._tarn_part:insert{txid, 'prepared', dshard, parts, now(), expiry()}
    return nil
end

-- wb: the bucket of each write (see "Buckets"); a PREPARE refused for a
-- bucket answers 'moved' (a NO vote, with a tombstone like any).
function tarn_prepare(txid, dshard, parts, writes, rpoints, rranges, txn_ts, lock_gen, fence, wb, mver)
    writable()
    rpoints, rranges = rpoints or {}, rranges or {}
    local function vote_no(code, msg, status)
        if box.is_in_txn() then box.rollback() end
        -- Tombstone: a late duplicate of this PREPARE must also fail.
        pcall(function()
            box.begin(RW)
            if box.space._tarn_part:get{txid} == nil then
                box.space._tarn_part:insert{txid, 'aborted', dshard, parts, now(), expiry()}
            end
            if dshard == M.cfg.shard_id and box.space._tarn_txn:get{txid} == nil then
                box.space._tarn_txn:insert{txid, 'aborted', parts, now(), expiry()}
            end
            box.commit()
        end)
        if box.is_in_txn() then box.rollback() end
        pl_release_all(txid)   -- the transaction aborts: its row locks here can go
        M.stats.prepare_no = M.stats.prepare_no + 1
        return {status == 'moved' and 'moved' or 'no', code, msg}
    end
    local ok, res = pcall(function()
        -- Write-only participants are prepared outside any stream.
        if not box.is_in_txn() then box.begin(RW) end
        local existing = box.space._tarn_part:get{txid}
        if existing ~= nil then   -- idempotent re-delivery (TarnCommit!Revote)
            box.rollback()
            if existing.state == 'aborted' then return {'no', 1213, 'transaction was aborted'} end
            return {'yes'}
        end
        if txn_ts ~= nil and now() - txn_ts > M.cfg.max_txn_age then
            box.rollback()
            M.stats.prepare_no = M.stats.prepare_no + 1
            return {'expired', 1213, 'transaction too old'}
        end
        lk_fresh()
        fence_fresh()
        BK.fresh()
        -- The checks below run again in the atomic section, where they
        -- count; here they fail the PREPARE before it reads anything.
        -- RC transactions: their row locks must have survived (TarnCommit:
        -- Prepare.readsOK for t \in RC).
        if not pl_intact(txid, lock_gen) then return vote_no(1213, BROKEN_MSG) end
        -- Writes encoded with a table definition an ALTER TABLE replaced.
        if stale_definition(fence) then return vote_no(1412, STALE_DEF_MSG) end
        local mv = BK.commit_check(wb, rpoints, rranges, true, nil, mver)
        if mv then return vote_no(mv[2], mv[3], mv[1]) end
        local throttle = M.vinyl.guard(writes)
        if throttle then return vote_no(throttle[2], throttle[3]) end
        -- Reads (may yield, see "Yields"): the rows the writes replace and the
        -- holders of the unique values the new rows take (tracked by MVCC).
        local plans = {}
        -- Rows this transaction deletes or rewrites here (by space and
        -- primary key: false, or the new tuple). A unique value such a row
        -- holds now is free for the transaction's other rows unless the
        -- row's new version keeps it (REPLACE, a value moved between rows).
        local mine = {}
        for _, w in ipairs(writes) do
            local sp = box.space[w[2]]
            if sp ~= nil then
                local wpk, wnew = write_target(sp, w)
                mine[w[2] .. kbin(wpk)] = wnew or false
            end
        end
        local function keeps(space, iid, cpk, key)
            local m = mine[space.id .. kbin(cpk)]
            if m == nil then return true end
            if type(m) == 'boolean' then return false end   -- (a tuple's == cannot take a boolean)
            return index_key_def(space.id, iid):compare_with_key(m, key) == 0
        end
        for i, w in ipairs(writes) do
            local space = box.space[w[2]]
            if space == nil then return vote_no(1146, 'no such space ' .. tostring(w[2])) end
            local pk, new = write_target(space, w)
            local old = space:get(pk)
            yield_point('prepare')
            if w[1] == 'insert' and old ~= nil then
                return vote_no(1062, UIX.dup(space, new) or 'Duplicate entry for key PRIMARY')
            end
            local keys = secondary_keys(space, old, {})
            secondary_keys(space, new, keys)
            if new ~= nil then
                local pkd = index_key_def(space.id, 0)
                for _, k in ipairs(keys) do
                    if k[3] and not has_null(k[2]) then
                        local cur = space.index[k[1]]:get(k[2])
                        local cpk = cur ~= nil and pkd:extract_key(cur):totable()
                        if cur ~= nil and pkd:compare_keys(cpk, pk) ~= 0 and keeps(space, k[1], cpk, k[2]) then
                            local idx = space.index[k[1]]
                            return vote_no(1062, 'Duplicate entry for key '
                                .. (PENDING_NAME[space.id .. '/' .. k[1]] or idx.name))
                        end
                    end
                end
            end
            plans[i] = {space, pk, old, new, nil, false, keys}
        end
        -- TarnCommit!Prepare: the checks and the locks, then the commit, with
        -- no yield in between.
        local no = atomic('Prepare', prepare_locks, txid, dshard, parts, writes, rpoints, rranges,
                          lock_gen, fence, plans, wb, mver)
        if no ~= nil then return vote_no(no[1], no[2], no[3]) end
        -- MVCC validates every read this stream made; may raise conflict.
        -- Tracked: an ALTER TABLE waits for PREPAREs that passed its fence.
        tracked_commit()
        M.stats.prepare_yes = M.stats.prepare_yes + 1
        return {'yes'}
    end)
    if not ok then
        if box.is_in_txn() then box.rollback() end
        reraise_readonly(res)
        local f = commit_failure(res)
        return vote_no(f[2], f[3])
    end
    return res
end

-- (Counted by decision shard while in progress: see BK.prep.)
do
    local prepare = tarn_prepare
    function tarn_prepare(txid, dshard, ...)
        local d = dshard or 0
        BK.prep[d] = (BK.prep[d] or 0) + 1
        local ok, res = pcall(prepare, txid, dshard, ...)
        BK.prep[d] = BK.prep[d] - 1
        if not ok then error(res) end
        return res
    end
end

local function release_locks(txid)
    local L = box.space._tarn_locks
    for _, l in L.index.txid:pairs({txid}, {iterator = 'EQ'}) do
        L:delete{l.space_id, l.index_id, l.key, l.txid}
    end
    for _, r in box.space._tarn_rlocks:pairs({txid}, {iterator = 'EQ'}) do
        box.space._tarn_rlocks:delete{r.txid, r.n}
    end
    box.on_commit(function()
        lk_release(txid)
        pl_release_all(txid)   -- row locks end with the transaction (TarnCommit: RCApplied)
        M.unlock_cond:broadcast()
    end)
end

-- TarnCommit!ApplyAt: move intents into the plain spaces, release locks.
-- Only ever called once the transaction is known to be committed. On the
-- decision shard it also marks the record COMMITTED in the same local
-- transaction (TarnCommit: ApplyMarksCommitted), so that the record is final
-- before any participant record can be garbage-collected.
-- Returns {'ok', 'applied'} (now or earlier), {'ok', 'none'} (no record: it
-- was applied and then garbage-collected), or an error status.
function tarn_apply(txid, retries)
    writable()
    local refused   -- {space id, index id} of an index under construction that refused a row
    -- In flight from before its first write (until the commit ends): a move
    -- that starts noting the writes to a bucket waits for it (see "Buckets").
    local seq = inflight_begin()
    local ok, res = pcall(function()
        -- A duplicate must not acknowledge another APPLY's unconfirmed
        -- 'applied' record: its synchronous commit can still roll back.
        -- Keep terminal proofs and the decided write in one confirmed view.
        box.begin({txn_isolation = 'read-confirmed'})
        local p = box.space._tarn_part:get{txid}
        if p == nil then
            box.rollback()
            return {OK, 'none'}
        end
        if p.state == 'applied' then
            box.rollback()
            return {OK, 'applied'}
        end
        if p.state ~= 'prepared' then
            box.rollback()
            M.stats.protocol_errors = M.stats.protocol_errors + 1
            log.error('tarn: APPLY of %s, but the participant is %s', txid, p.state)
            return {'error', 0, 'APPLY of a transaction whose participant record is ' .. p.state}
        end
        if p.dshard == M.cfg.shard_id then
            local r = box.space._tarn_txn:get{txid}
            if r == nil or r.status == 'aborted' then
                box.rollback()
                M.stats.protocol_errors = M.stats.protocol_errors + 1
                log.error('tarn: APPLY of %s, but its record is %s', txid, r and r.status or 'missing')
                return {'error', 0, 'APPLY of a transaction whose record is ' .. (r and r.status or 'missing')}
            end
            if r.status == 'staging' then
                box.space._tarn_txn:replace{txid, 'committed', r.parts, now(), expiry()}
            end
        end
        -- Deletes first: a new row may reuse a deleted row's unique values.
        -- A table dropped since the PREPARE took its rows with it, and space
        -- ids are never reused: its writes have nothing left to change (the
        -- transaction is ordered before the DROP TABLE), while the others
        -- must still land.
        -- A replace that a unique index refuses is tried again after the
        -- others: a value may move from one row to another (see write_steps).
        local later = {}
        for _, pass in ipairs({'delete', 'write'}) do
            for _, l in box.space._tarn_locks.index.txid:pairs({txid}, {iterator = 'EQ'}) do
                if l.mode == 'W' and l.op ~= nil and (l.op == 'delete') == (pass == 'delete') then
                    local space = box.space[l.space_id]
                    if space == nil then
                        M.stats.apply_dropped = M.stats.apply_dropped + 1
                    elseif l.op == 'delete' then BK.note(space, space:delete(msgpack.decode(l.key)), nil)
                    else
                        local wok, err = pcall(space.replace, space, l.tuple)
                        if wok then BK.note(space, nil, err) end
                        if not wok then
                            local iid = refused_by_pending(space, err)
                            if iid ~= nil then refused = {space.id, iid} end
                            if iid ~= nil or type(err) ~= 'cdata' or err.code ~= box.error.TUPLE_FOUND then
                                error(err)
                            end
                            later[#later + 1] = {space, l.tuple, err}
                        end
                    end
                end
            end
        end
        while #later > 0 do
            local again = {}
            for _, x in ipairs(later) do
                local wok, err = pcall(x[1].replace, x[1], x[2])
                if wok then BK.note(x[1], nil, err) else again[#again + 1] = {x[1], x[2], err} end
            end
            if #again == #later then
                -- Values that go round in a cycle (rows that swap them):
                -- take the rows out, then write them.
                for _, x in ipairs(again) do
                    x[1]:delete(index_key_def(x[1].id, 0):extract_key(box.tuple.new(x[2])))
                end
                for _, x in ipairs(again) do BK.note(x[1], nil, x[1]:replace(x[2])) end
                break
            end
            later = again
        end
        M.lazy.release(txid, true)
        release_locks(txid)
        box.space._tarn_part:replace{txid, 'applied', p.dshard, p.parts, now(), expiry()}
        box.commit()
        M.stats.apply = M.stats.apply + 1
        return {OK, 'applied'}
    end)
    inflight_end(seq)
    if not ok then
        if box.is_in_txn() then box.rollback() end
        reraise_readonly(res)
        retries = retries or 0
        if refused ~= nil and retries < 8 then
            -- A decided transaction writes a row that a unique index an
            -- ALTER TABLE builds refuses: the schema change loses (see
            -- "Schema changes"). Drop the index, outside the transaction,
            -- and apply again.
            local sp = box.space[refused[1]]
            local idx = sp and sp.index[refused[2]]
            if idx ~= nil and idx.name == TMP_PREFIX .. refused[2] then
                log.warn('tarn: APPLY of %s writes a row that index %s of %s, which ALTER TABLE is building, '
                         .. 'refuses: dropping the index (the schema change will fail on the duplicate)',
                         txid, PENDING_NAME[sp.id .. '/' .. refused[2]] or idx.name, sp.name)
                idx:drop()
                M.stats.alter_conflicts = M.stats.alter_conflicts + 1
            end
            return tarn_apply(txid, retries + 1)
        end
        return failure(res)
    end
    return res
end

-- TarnCommit!AbortAt: release locks; with no participant record, leave a
-- tombstone so that a PREPARE still in flight is refused.
function tarn_abort(txid, dshard, parts)
    writable()
    local ok, res = pcall(function()
        box.begin(RW)
        local p = box.space._tarn_part:get{txid}
        if p ~= nil and p.state ~= 'prepared' then
            box.rollback()
            pl_release_all(txid)
            if p.state == 'applied' then
                M.stats.protocol_errors = M.stats.protocol_errors + 1
                log.error('tarn: ABORT of %s, but the participant applied it', txid)
            end
            return {OK, p.state}
        end
        M.lazy.release(txid, false)
        release_locks(txid)
        box.space._tarn_part:replace{txid, 'aborted', dshard or (p and p.dshard) or 0,
                                     parts or (p and p.parts) or {}, now(), expiry()}
        box.commit()
        M.stats.abort = M.stats.abort + 1
        return {OK, 'aborted'}
    end)
    if not ok then
        if box.is_in_txn() then box.rollback() end
        reraise_readonly(res)
        return failure(res)
    end
    return res
end

-- TarnCommit!RecoverFenceParticipant: tombstone if never prepared.
function tarn_fence(txid, dshard, parts)
    writable()
    local ok, res = pcall(function()
        box.begin(RW)
        local p = box.space._tarn_part:get{txid}
        if p == nil then
            box.space._tarn_part:insert{txid, 'aborted', dshard, parts, now(), expiry()}
            box.commit()
            return 'aborted'
        end
        box.rollback()
        return p.state
    end)
    if not ok then
        if box.is_in_txn() then box.rollback() end
        reraise_readonly(res)
        return nil, tostring(res)
    end
    return res
end

-- Status reads for recovery run on the leader only: a replica may lag.
function tarn_part_status(txid)
    writable()
    local p = box.space._tarn_part:get{txid}
    return p and p.state or 'none'
end

-- Batched form for garbage collection: statuses in the order of txids.
function tarn_part_statuses(txids)
    writable()
    local out = {}
    for i, id in ipairs(txids) do
        local p = box.space._tarn_part:get{id}
        out[i] = p and p.state or 'none'
    end
    return out
end

-- Decision-shard record operations (TarnCommit: rec[t]).
function tarn_txn_status(txid)
    writable()
    local r = box.space._tarn_txn:get{txid}
    return r and r.status or 'none', r and r.parts or nil
end

function tarn_txn_statuses(txids)
    writable()
    local out = {}
    for i, id in ipairs(txids) do
        local r = box.space._tarn_txn:get{id}
        out[i] = r and r.status or 'none'
    end
    return out
end

-- Compare-and-set of the record status; 'none' -> X creates it.
function tarn_txn_cas(txid, from, to, parts)
    writable()
    local ok, res = pcall(function()
        box.begin(RW)
        local r = box.space._tarn_txn:get{txid}
        local cur = r and r.status or 'none'
        if cur ~= from then box.rollback(); return cur end
        box.space._tarn_txn:replace{txid, to, parts or (r and r.parts) or {}, now(), expiry()}
        box.commit()
        return to
    end)
    if not ok then
        if box.is_in_txn() then box.rollback() end
        reraise_readonly(res)
        return nil, tostring(res)
    end
    return res
end

-------------------------------------------------------------------------------
-- Recovery (TarnCommit: Timeout, RecoverFenceRecord, RecoverFenceParticipant,
-- RecoverDecide). Runs on every writable node; idempotent and safe to run
-- concurrently with a live coordinator and with other recovery agents.
-------------------------------------------------------------------------------

local conns = {}

-- The replicas of shard `shard`: from the configuration, else from the
-- membership (a shard that joined since).
local function shard_uris(shard)
    local uris = M.cfg.peers[shard] or M.cfg.peers[tostring(shard)]
    if uris ~= nil then return uris end
    for _, m in ipairs(BK.members.shards or {}) do
        if m.id == shard then return m.replicas or {} end
    end
    return {}
end

local function shard_conn(shard)
    if shard == M.cfg.shard_id then return nil end  -- local calls
    local c = conns[shard]
    if c ~= nil and c:is_connected() then return c end
    for _, uri in ipairs(shard_uris(shard)) do
        local ok, cand = pcall(net_box.connect, uri, {user = M.cfg.user, password = M.cfg.password,
                                                      wait_connected = 1, connect_timeout = 1})
        if ok and cand:is_connected() then
            local ok2, ro = pcall(cand.eval, cand, 'return box.info.ro', {}, {timeout = 1})
            if ok2 and ro == false then
                conns[shard] = cand
                return cand
            end
            cand:close()
        end
    end
    error('no writable node for shard ' .. tostring(shard))
end

-- Call fn on the leader of `shard` (locally for this shard). A leader that
-- was demoted while the connection survived answers ER_READONLY: drop the
-- connection, find the new leader and retry once.
local function on_shard(shard, fn, ...)
    local c = shard_conn(shard)
    if c == nil then return _G[fn](...) end
    local args = {...}
    local ok, a, b = pcall(c.call, c, fn, args, {timeout = 2})
    if ok then return a, b end
    if type(a) == 'cdata' and a.code == box.error.READONLY then
        conns[shard] = nil
        c:close()
        c = shard_conn(shard)
        if c == nil then return _G[fn](unpack(args)) end
        return c:call(fn, args, {timeout = 2})
    end
    error(a)
end

-- One recovery agent's attempt to finish txid. Returns 'committed',
-- 'aborted', 'gone' (no record and no prepared participant: nothing left
-- to do) or 'unresolved'.
local function resolve(txid, dshard, parts)
    for _ = 1, 4 do
        local status = on_shard(dshard, 'tarn_txn_status', txid)
        if status == 'committed' then
            for _, p in ipairs(parts) do on_shard(p, 'tarn_apply', txid) end
            return 'committed'
        elseif status == 'aborted' then
            for _, p in ipairs(parts) do on_shard(p, 'tarn_abort', txid, dshard, parts) end
            return 'aborted'
        end
        local all_prepared, any_aborted, any_prepared, missing = true, false, false, {}
        for _, p in ipairs(parts) do
            local st = on_shard(p, 'tarn_part_status', txid)
            if st == 'aborted' then any_aborted = true; all_prepared = false
            elseif st == 'none' then all_prepared = false; missing[#missing + 1] = p
            elseif st == 'prepared' then any_prepared = true end
        end
        if status == 'none' then
            -- RecoverFenceRecord: block D's PREPARE, then the txn is aborted.
            -- Only while some participant is prepared: then the record cannot
            -- have been garbage-collected (TarnCommit: GCRec), so "no record"
            -- means D never prepared.
            if not any_prepared then return 'gone' end
            on_shard(dshard, 'tarn_txn_cas', txid, 'none', 'aborted', parts)
        elseif all_prepared then
            on_shard(dshard, 'tarn_txn_cas', txid, 'staging', 'committed')
        elseif any_aborted then
            on_shard(dshard, 'tarn_txn_cas', txid, 'staging', 'aborted')
        else
            -- STAGING: a participant without a record never prepared (applied
            -- records are kept until the record is final), so fence it.
            for _, p in ipairs(missing) do on_shard(p, 'tarn_fence', txid, dshard, parts) end
        end
    end
    return 'unresolved'
end
M.resolve = resolve

-- Delete records by primary key, gc_batch per local transaction.
local function gc_delete(space, ids)
    local i = 1
    while i <= #ids do
        local ok, err = pcall(function()
            box.begin()
            for j = i, math.min(i + M.cfg.gc_batch - 1, #ids) do space:delete{ids[j]} end
            box.commit()
        end)
        if not ok then
            -- e.g. ER_READONLY after a leader change: never leave this
            -- (long-lived) fiber inside a transaction.
            if box.is_in_txn() then box.rollback() end
            error(err)
        end
        i = i + M.cfg.gc_batch
    end
end

-- Statuses of txids on `shard` via fn (tarn_txn_statuses/tarn_part_statuses);
-- nil if the shard cannot be asked right now.
local function statuses_on(shard, fn, ids)
    -- A shard that left the cluster was needed by no transaction when it
    -- left (see BK.prep), and may be gone: none of its records count.
    if BK.member_state(shard) == 'removed' then
        local out = {}
        for i = 1, #ids do out[i] = 'none' end
        return out
    end
    local ok, res = pcall(on_shard, shard, fn, ids)
    if ok then return res end
    log.warn('tarn gc: %s on shard %s failed: %s', fn, tostring(shard), tostring(res))
    return nil
end

-- Garbage collection of expired records (see "Record lifecycle").
local function gc_pass(t)
    -- Forget broken transactions whose commits can no longer arrive.
    local mono = clock.monotonic()
    for txid, until_t in pairs(PL_BROKEN) do
        if until_t < mono then PL_BROKEN[txid] = nil end
    end
    local limit, scan = M.cfg.gc_batch * 4, M.cfg.gc_batch * 40
    -- Participant records.
    local victims, waiting, n = {}, {}, 0   -- waiting[dshard] = {txid, ...}
    for _, p in box.space._tarn_part.index.exp:pairs() do   -- ascending expiry
        n = n + 1
        if p.expires >= t or n > scan or #victims >= limit then break end
        if p.state == 'aborted' then
            victims[#victims + 1] = p.txid
        elseif p.state == 'applied' then
            local w = waiting[p.dshard]
            if w == nil then w = {}; waiting[p.dshard] = w end
            w[#w + 1] = p.txid
        end   -- prepared: never collected
    end
    for dshard, ids in pairs(waiting) do
        local st = statuses_on(dshard, 'tarn_txn_statuses', ids)
        for i, id in ipairs(st and ids or {}) do
            -- Applied => committed: delete once the record is final or gone.
            if st[i] == 'committed' or st[i] == 'none' then
                victims[#victims + 1] = id
            elseif st[i] == 'aborted' then
                M.stats.protocol_errors = M.stats.protocol_errors + 1
                log.error('tarn gc: %s applied here but its record is aborted', id)
            end
        end
    end
    gc_delete(box.space._tarn_part, victims)
    M.stats.gc_part = M.stats.gc_part + #victims
    -- Decision records: final ones, once no participant is still prepared.
    local cands, ask = {}, {}   -- ask[shard] = {txid, ...}
    n = 0
    for _, r in box.space._tarn_txn.index.exp:pairs() do
        n = n + 1
        if r.expires >= t or n > scan or #cands >= limit then break end
        if r.status == 'committed' or r.status == 'aborted' then
            cands[#cands + 1] = r
            for _, p in ipairs(r.parts) do
                local a = ask[p]
                if a == nil then a = {}; ask[p] = a end
                a[#a + 1] = r.txid
            end
        end
    end
    local prepared = {}   -- txid -> true if prepared (or unknown) somewhere
    for shard, ids in pairs(ask) do
        local st = statuses_on(shard, 'tarn_part_statuses', ids)
        for i, id in ipairs(ids) do
            if st == nil or st[i] == 'prepared' then prepared[id] = true end
        end
    end
    victims = {}
    for _, r in ipairs(cands) do
        if not prepared[r.txid] then victims[#victims + 1] = r.txid end
    end
    gc_delete(box.space._tarn_txn, victims)
    M.stats.gc_txn = M.stats.gc_txn + #victims
end

-- Candidates first, then the (yielding) resolution of each.
local function stuck(index, key, t)
    local out = {}
    for _, x in index:pairs({key}, {iterator = 'EQ'}) do
        if t - x.ts < M.cfg.recovery_after or #out >= 200 then break end
        out[#out + 1] = x
    end
    return out
end

local function recovery_pass()
    if box.info.ro or box.space._tarn_txn == nil then return end   -- (schema not made yet)
    local t = now()
    -- Participants stuck in 'prepared'.
    for _, p in ipairs(stuck(box.space._tarn_part.index.state, 'prepared', t)) do
        local ok, err = pcall(resolve, p.txid, p.dshard, p.parts)
        if ok then M.stats.recovered = M.stats.recovered + 1
        else log.warn('tarn recovery of %s failed: %s', p.txid, tostring(err)) end
    end
    -- Records stuck in 'staging' on this decision shard.
    for _, r in ipairs(stuck(box.space._tarn_txn.index.status, 'staging', t)) do
        local ok, err = pcall(resolve, r.txid, M.cfg.shard_id, r.parts)
        if not ok then log.warn('tarn recovery of %s failed: %s', r.txid, tostring(err)) end
    end
    gc_pass(t)
end

local recovery_fiber
function M.start_recovery()
    if recovery_fiber ~= nil then return end
    recovery_fiber = fiber.create(function()
        fiber.name('tarn_recovery')
        while true do
            fiber.sleep(M.cfg.recovery_interval)
            local ok, err = pcall(recovery_pass)
            if not ok then log.warn('tarn recovery pass: %s', tostring(err)) end
        end
    end)
end

-------------------------------------------------------------------------------
-- DDL and catalog
-------------------------------------------------------------------------------
-- SQL servers run CREATE TABLE, DROP TABLE, TRUNCATE and ALTER TABLE as jobs
-- in steps on every shard (see "CREATE TABLE, DROP TABLE and TRUNCATE" and
-- "Jobs" below). tarn_create_table, tarn_drop_table and tarn_truncate do it
-- in one step, for SQL servers from before those jobs.

-- def = {name=, engine=, format={{name=,type=,is_nullable=}}, indexes={{name=,
--        unique=, parts={{field=,type=,is_nullable=,collation=}}}}, ...}
-- def.uix: the table's global index spaces, in the same form (no catalog
-- entry of their own).
local function create_space(def)
    local s = box.schema.space.create(def.name, {
        id = def.id, engine = def.engine or 'memtx', format = def.format,
        is_sync = M.cfg.sync, if_not_exists = true})
    for i, idx in ipairs(def.indexes) do
        s:create_index(idx.name, {type = 'tree', unique = idx.unique, parts = idx.parts,
                                  id = i - 1, if_not_exists = true})
    end
    for _, u in ipairs(def.uix or {}) do create_space(u) end
    return s
end

function UIX.drop(sid)
    for _, u in ipairs(UIX.spaces(sid)) do u:drop() end
end

function tarn_create_table(def)
    if def.lazy ~= nil and #def.lazy > 0 then return {'error', 1412, 'Lazy indexes require the DDL job bootstrap'} end
    if box.space[def.name] ~= nil then
        return {'exists'}
    end
    local s = create_space(def)
    box.space._tarn_catalog:replace{def.name, def}
    invalidate_key_defs()
    return {OK, s.id}
end

function tarn_drop_table(name)
    local lazy = M.lazy.ddl_guard('legacy', name, {})
    if lazy then return lazy end
    if box.space[name] ~= nil then
        UIX.drop(box.space[name].id)
        box.space[name]:drop()
    end
    box.space._tarn_catalog:delete{name}
    invalidate_key_defs()
    return {OK}
end

-------------------------------------------------------------------------------
-- Schema changes (ALTER TABLE, RENAME TABLE, CREATE / DROP INDEX)
-------------------------------------------------------------------------------
-- Instant ADD normally leaves stored rows short: SQL supplies their frozen
-- fill values. An index that needs those values materializes missing suffixes
-- behind a durable write fence, with original-length undo until publication.
-- ALTER keeps dropped fields and changes names, types and indexes.
-- What makes it safe while transactions run is the table's definition
-- version (`ver`): writes carry the version of the definition they were
-- encoded with (FENCE above), and an ALTER TABLE to version V runs on every
-- shard in three steps, driven by one SQL server:
--   BUILD    on a vinyl table, build the new indexes under temporary names
--            while writes go on (vinyl keeps an index current while it
--            builds it): no write waits for it, however large the table. A
--            memtx table builds in PREPARE: with memtx MVCC, a write
--            committed while an index build yields aborts the build, and
--            memtx builds at memory speed anyway;
--   PREPARE  raise the fence to V (writes made with an older definition are
--            refused from now on), wait for the local commits already past
--            the check and for the prepared cross-shard transactions holding
--            locks on the table, check the existing rows against the new
--            column types (refused above alter_check_rows rows: writes wait
--            for the scan), and build what BUILD has not;
--   APPLY    once every shard has prepared: drop the replaced indexes,
--            change the format, rename indexes and the table, and store the
--            new definition (version V, so the fence survives restarts).
-- A failed step anywhere is undone everywhere (ABORT). Each step can be
-- repeated; an APPLY that finds its fence gone (restart, new leader)
-- prepares again first.
-- From BUILD on, a new unique index refuses rows that would break it
-- (1062), as TiDB's write-only index state does: a write that the schema
-- change would make fail fails now. A decided one cannot fail: an APPLY
-- that such an index refuses drops it and runs again (tarn_apply), and
-- PREPARE then builds it again under the fence, where the duplicate fails
-- the schema change instead.

local function catalog_ver(name)
    local r = box.space._tarn_catalog:get{name}
    return r and r.def.ver or 0
end

local function tmp_index_name(id) return TMP_PREFIX .. id end

local function has_locks(sid, uix)
    for _, i in pairs(LK[sid] or {}) do if next(i) ~= nil then return true end end
    for _, i in pairs(RL[sid] or {}) do if next(i) ~= nil then return true end end
    -- the table's global index entries on this shard, written by
    -- transactions whose rows may live on other shards
    for _, u in ipairs(uix or {}) do
        if has_locks(u.id) then return true end
    end
    return false
end

-- Wait until no commit that passed the fence check is in flight and no
-- prepared transaction holds locks on space sid.
local function drain(sid, deadline)
    if not wait_commits(deadline) then return false end
    local uix = UIX.spaces(sid)
    while true do
        lk_fresh()
        if not has_locks(sid, uix) then return true end
        local left = deadline - clock.monotonic()
        if left <= 0 then return false end
        M.unlock_cond:wait(math.min(left, 0.05))
    end
end

-- Digits before the decimal point of a decimal (0 for |d| < 1).
local function int_digits(d)
    local ip = tostring(decimal.abs(d)):match('^(%d+)') or ''
    return #(ip:gsub('^0+', ''))
end

-- One check of an existing value against a new column type:
--   {field, column, 'notnull'}         no NULL            (else 1265)
--   {field, column, 'chars', n}        <= n characters    (else 1265)
--   {field, column, 'range', lo, hi}   integer in range   (else 1264)
--   {field, column, 'digits', n}       decimal with <= n digits before the point (else 1264)
--   {field, column, 'indexed'}        field is physically present (else 1846)
-- with the column's fill value as the 6th element: the value of rows whose
-- tuple ends before the field.
local function check_value(c, v)
    local kind = c[3]
    if v == nil or v == box.NULL then
        return kind == 'notnull' and 1265 or nil
    end
    if kind == 'chars' then
        if type(v) == 'string' and (utf8.len(v) or #v) > c[4] then return 1265 end
    elseif kind == 'range' then
        if (type(v) == 'number' or type(v) == 'cdata') and not decimal.is_decimal(v)
            and (int_lt(v, c[4]) or int_lt(c[5], v)) then return 1264 end
    elseif kind == 'digits' then
        if decimal.is_decimal(v) and int_digits(v) > c[4] then return 1264 end
    end
    return nil
end

local function run_checks(sp, checks, before_materialize)
    if checks == nil or checks == box.NULL or #checks == 0 then return nil end
    local n = 0
    for _, t in sp.index[0]:pairs() do
        n = n + 1
        for _, c in ipairs(checks) do
            if not before_materialize and c[3] == 'indexed' and #t < c[1] then
                return {'error', 1846, string.format(
                    "Indexing column '%s' is not supported while existing rows omit its non-NULL fill "
                    .. "(row %d). The stored field must be materialized before adding the index", c[2], n)}
            end
            local v
            if #t < c[1] then v = c[6] else v = t[c[1]] end
            local code = check_value(c, v)
            if code == 1265 then
                return {'error', 1265, string.format("Data truncated for column '%s' at row %d", c[2], n)}
            elseif code then
                return {'error', 1264, string.format("Out of range value for column '%s' at row %d", c[2], n)}
            end
        end
        -- The table takes no writes meanwhile (fenced): yielding is safe.
        if n % 1000 == 0 then fiber.yield() end
    end
    return nil
end

-- New indexes {id, name, unique, parts}, built under temporary names; a
-- unique one fails on duplicates (1062). May take long and yields.
local function build_indexes(sp, builds)
    for _, b in ipairs(builds or {}) do
        PENDING_NAME[sp.id .. '/' .. b[1]] = b[2]
        if sp.index[b[1]] == nil then
            local ok, err = pcall(sp.create_index, sp, tmp_index_name(b[1]),
                {id = b[1], type = 'tree', unique = b[3], parts = b[4]})
            if not ok then
                if type(err) == 'cdata' and err.code == box.error.TUPLE_FOUND then
                    return {'duplicate', 1062, "Duplicate entry for key '" .. b[2] .. "'"}
                end
                return failure(err)
            end
        end
    end
    return nil
end

local function drop_built(sp, builds)
    for _, b in ipairs(builds or {}) do
        local idx = sp.index[b[1]]
        if idx ~= nil and idx.name == tmp_index_name(b[1]) then idx:drop() end
        PENDING_NAME[sp.id .. '/' .. b[1]] = nil
    end
end


local function alter_space(name, a)
    return box.space[name] or (a.rename ~= nil and a.rename ~= box.NULL and box.space[a.rename]) or nil
end

-- Automatic materialization only appends a missing suffix. The SQL server
-- preserves the old definition's padding for old slots (even dropped ones)
-- and supplies frozen fills for new slots. Existing values, including NULL,
-- are never rewritten. Undo therefore needs the original length, not a copy
-- of the row. Its identity deliberately excludes the job's takeover gen.
function UIX.mat_args(sp, a, gen)
    local p, width = a.pad, a.old_width
    if type(p) ~= 'table' or type(p[1]) ~= 'number' or p[1] < 1 or p[1] % 1 ~= 0
       or type(width) ~= 'number' or width % 1 ~= 0 or width < p[1]
       or p[1] + #p - 1 < width then
        return nil, {'error', 1210, 'Incorrect ALTER materialization padding'}
    end
    for i = 2, #p do
        if type(p[i]) == 'nil' then return nil, {'error', 1210, 'Incomplete ALTER materialization padding'} end
    end
    local r = UIX.record(sp.id)
    if gen == nil or gen == box.NULL then gen = r and r.ver == a.ver and r.gen or 0 end
    return gen
end

-- Called INSIDE the same RW transaction as its row/journal mutations.
-- Reading this durable owner also makes native MVCC abort a batch whose
-- generation/phase changes while a vinyl operation or WAL wait yields.
function UIX.mat_owned(sp, a, gen, phase)
    local r = UIX.record(sp.id)
    if catalog_ver(sp.name) >= a.ver or r == nil or r.ver ~= a.ver or r.gen ~= gen
       or r.aborted or r.phase ~= phase then return {'error', 1412, STALE_DEF_MSG} end
    return nil
end

function UIX.mat_tx(fn)
    local ok, res = pcall(function()
        box.begin(RW)
        local bad = fn()
        if bad then box.rollback(); return bad end
        box.commit()
        return {OK}
    end)
    if box.is_in_txn() then box.rollback() end
    return ok and res or failure(res)
end

-- Persist ownership before raising the in-memory fence or changing rows.
-- Aborting remains a LIVE fence; only the final aborted tombstone releases
-- it. Applying is irreversible, even before catalog publication finishes.
function UIX.mat_phase(sp, a, gen, phase)
    local r = UIX.mat_tx(function()
        if catalog_ver(sp.name) >= a.ver then return {OK, 'applied'} end
        local had = UIX.record(sp.id)
        if had ~= nil and (had.ver > a.ver or (had.ver == a.ver and (had.gen or 0) > gen)) then
            return {'error', 1412, STALE_DEF_MSG}
        end
        if had ~= nil and had.ver == a.ver then
            if had.aborted or had.phase == 'aborting' then
                if phase ~= 'aborting' then return {'error', 1412, STALE_DEF_MSG} end
                if had.aborted then return {OK, 'aborted'} end
            elseif had.phase == 'applying' and phase ~= 'applying' then
                return {'error', 1412, STALE_DEF_MSG}
            end
        end
        if phase == 'applying' and (had == nil or had.ver ~= a.ver
           or (had.phase ~= 'prepared' and had.phase ~= 'applying')) then
            return {'error', 1412, 'ALTER has not durably prepared on this shard'}
        end
        box.space._tarn_meta:replace{'fence:' .. sp.id,
            {ver = a.ver, gen = gen, phase = phase, materialize = true}}
        return nil
    end)
    if r[1] == OK and r[2] == nil and (FENCE[sp.id] or 0) < a.ver then
        FENCE[sp.id] = a.ver
        BK.ns_fence(sp.id)
    end
    return r
end

function UIX.mat_rows(sp, a, gen, deadline)
    local after, seen, target = nil, 0, a.pad[1] + #a.pad - 1
    local padding_bytes = #msgpack.encode(a.pad)
    while true do
        if clock.monotonic() > deadline then return {'timeout', 1205, LOCK_TIMEOUT_MSG} end
        local n, bytes = 0, 0
        local r = UIX.mat_tx(function()
            local bad = UIX.mat_owned(sp, a, gen, 'preparing')
            if bad then return bad end
            yield_point('alter_backfill')
            for _, t in sp.index[0]:pairs(after or {}, {iterator = after and 'GT' or 'ALL'}) do
                local pk = index_key_def(sp.id, 0):extract_key(t):totable()
                local kb = kbin(pk)
                n, seen, after = n + 1, seen + 1, pk
                if seen > M.cfg.alter_check_rows then
                    return {'error', 1846, 'ALTER materialization exceeds tarn.cfg.alter_check_rows'}
                end
                if #t < a.pad[1] then return {'error', 1210, 'Stored tuple is shorter than ALTER padding base'} end
                if #t < target then
                    local undo = box.space._tarn_alter_undo
                    if undo:get{sp.id, a.ver, kb} == nil then undo:insert{sp.id, a.ver, kb, #t} end
                    local row = t:totable()
                    for f = #row + 1, target do row[f] = a.pad[f - a.pad[1] + 1] end
                    sp:replace(row)
                end
                bytes = bytes + t:bsize() + #kb + padding_bytes
                if n >= 128 or bytes >= 1024 * 1024 then break end
            end
            yield_point('alter_backfill_written')
            return UIX.mat_owned(sp, a, gen, 'preparing')
        end)
        if r[1] ~= OK then return r end
        if n == 0 then return {OK} end
        yield_point('alter_backfill_chunk')
        if PG.heap_low() then return {'error', 1041, 'Out of memory during ALTER materialization'} end
        fiber.yield()
    end
end

-- Cleanup is bounded and restartable. ABORT trims and deletes each undo
-- entry atomically under the live aborting owner. After publication only
-- journal deletion is allowed; it cannot change rows or a newer fence.
function UIX.mat_clean(sp, a, gen, abort)
    while true do
        local n, bytes = 0, 0
        local r = UIX.mat_tx(function()
            if abort then
                local bad = UIX.mat_owned(sp, a, gen, 'aborting')
                if bad then return bad end
            elseif catalog_ver(sp.name) < a.ver then
                return {'error', 1412, STALE_DEF_MSG}
            end
            local undo = box.space._tarn_alter_undo
            for _, u in undo.index[0]:pairs({sp.id, a.ver}, {iterator = 'EQ'}) do
                if abort then
                    local key = msgpack.decode(u.key)
                    local t = sp:get(key)
                    if t == nil or #t < u.oldlen then
                        return {'error', 1412, 'ALTER undo no longer matches its fenced tuple'}
                    end
                    if #t > u.oldlen then sp:update(key, {{'#', u.oldlen + 1, #t - u.oldlen}}) end
                end
                undo:delete{sp.id, a.ver, u.key}
                n, bytes = n + 1, bytes + #u.key + 32
                if n >= 128 or bytes >= 1024 * 1024 then break end
            end
            if abort then return UIX.mat_owned(sp, a, gen, 'aborting') end
            return nil
        end)
        if r[1] ~= OK or n == 0 then return r end
        yield_point(abort and 'alter_backfill_undo' or 'alter_backfill_gc')
        fiber.yield()
    end
end

function UIX.mat_prepare(sp, a, gen)
    local bad
    gen, bad = UIX.mat_args(sp, a, gen)
    if bad then return bad end
    local r = UIX.mat_phase(sp, a, gen, 'preparing')
    if r[1] ~= OK or r[2] ~= nil then return r end
    local deadline = clock.monotonic() + (a.timeout or 50)
    if not drain(sp.id, deadline) then return {'timeout', 1205, LOCK_TIMEOUT_MSG} end
    bad = run_checks(sp, a.checks, true)
    if bad then return bad end
    r = UIX.mat_rows(sp, a, gen, deadline)
    if r[1] ~= OK then return r end
    bad = run_checks(sp, a.checks)
    if bad then return bad end
    -- A failure keeps the fence and journal until the coordinator's ABORT.
    yield_point('alter_backfill_index')
    bad = build_indexes(sp, a.builds)
    if bad then return bad end
    for _, u in ipairs(a.uix or {}) do
        if UIX.mat_owned(sp, a, gen, 'preparing') then return {'error', 1412, STALE_DEF_MSG} end
        create_space(u)
    end
    return UIX.mat_tx(function()
        local lost = UIX.mat_owned(sp, a, gen, 'preparing')
        if lost then return lost end
        box.space._tarn_meta:replace{'fence:' .. sp.id,
            {ver = a.ver, gen = gen, phase = 'prepared', materialize = true}}
        return nil
    end)
end

-- a: {ver, checks, builds, timeout, drop = {index ids}, renames = {{id, name}},
--     format = new format or nil, rename = new space name or nil, def = catalog entry,
--     uix = new global index spaces (as create_space takes them), uix_drop = {space ids}}
-- New global indexes (unique keys without the shard key, issue #88): PREPARE
-- creates their spaces, empty and named by no catalog; once every shard has
-- prepared -- the table's writes are fenced everywhere -- the SQL server
-- fills them from the rows (tarn_uix_fill), where a value held twice fails
-- the change (1062); APPLY puts them to use with the new definition, ABORT
-- drops them. APPLY drops the spaces of dropped ones.
function tarn_alter_build(name, a)
    writable()
    local sp = alter_space(name, a)
    if sp == nil then return {'error', 1146, "Table '" .. name .. "' doesn't exist"} end
    if catalog_ver(sp.name) >= a.ver or sp.engine ~= 'vinyl' then return {OK} end
    -- An instant-ADD field may be missing in old tuples. Check it only after
    -- PREPARE fences and drains old-layout writes, then build behind that fence.
    if a.checks ~= nil and a.checks ~= box.NULL then
        for _, c in ipairs(a.checks) do
            if c[3] == 'indexed' then return {OK} end
        end
    end
    local bad = build_indexes(sp, a.builds)
    if bad then
        drop_built(sp, a.builds)
        return bad
    end
    return {OK}
end

-- gen: the job's generation (see tarn_job_touch): the fill of a new global
-- index is accepted only from the run that prepared last (UIX.filling).
function tarn_alter_prepare(name, a, gen)
    writable()
    local sp = alter_space(name, a)
    if sp == nil then return {'error', 1146, "Table '" .. name .. "' doesn't exist"} end
    local cur = catalog_ver(sp.name)
    if cur >= a.ver then return {OK, 'applied'} end
    local fills = (a.uix ~= nil and a.uix ~= box.NULL and #a.uix > 0) or M.lazy.needs_bootstrap(a.def)
    if (a.checks ~= nil and a.checks ~= box.NULL and #a.checks > 0) or fills then
        -- Writes to the table wait while the rows are checked, or read to
        -- fill a new global index; vinyl's count is an estimate, which is
        -- enough for a limit.
        local rows = sp.index[0]:len()
        if rows > M.cfg.alter_check_rows then
            return {'error', 1846, string.format(
                "%s the existing rows of '%s' is not supported. Reason: a shard holds about %d rows "
                .. "of it (limit %d, tarn.cfg.alter_check_rows), and writes to the table would wait for "
                .. "the whole scan. %s", fills and 'Building a global unique index from' or 'Checking',
                sp.name, rows, M.cfg.alter_check_rows,
                fills and 'Add the unique key while the table is small, or include the shard key in it'
                    or 'Try a change that needs no check, such as a wider type or a nullable column')}
        end
    end
    fence_fresh()
    if a.materialize == true then return UIX.mat_prepare(sp, a, gen) end
    -- Stored first when a new global index is filled (see fence_rebuild).
    if fills then
        local had = UIX.record(sp.id)
        if gen == nil or gen == box.NULL then
            -- APPLY prepares again without one (the fill is done then).
            gen = had and had.ver == a.ver and had.gen or 0
        elseif had ~= nil and (had.ver > a.ver or (had.ver == a.ver and (had.aborted or (had.gen or 0) > gen))) then
            -- A run of the job that another one took over, a change that
            -- was aborted, or one older than a change begun since (catalog
            -- versions only grow): it may not fence the table again, nor fill.
            return {'error', 1412, STALE_DEF_MSG}
        end
        if had == nil or had.ver < a.ver or not had.aborted then
            box.space._tarn_meta:replace{'fence:' .. sp.id, {ver = a.ver, gen = gen}}
        end
    end
    if (FENCE[sp.id] or 0) < a.ver then FENCE[sp.id] = a.ver; BK.ns_fence(sp.id) end
    local function undo(res)
        drop_built(sp, a.builds)
        -- Only while the fence is still this run's: a later run of the job
        -- (a takeover) may have prepared on this shard meanwhile.
        local mine = not fills
        if fills then
            local r = UIX.record(sp.id)
            mine = r ~= nil and r.ver == a.ver and r.gen == gen and not r.aborted
            if mine then box.space._tarn_meta:delete{'fence:' .. sp.id} end
        end
        if mine then FENCE[sp.id] = cur > 0 and cur or nil; BK.ns_fence(sp.id) end
        return res
    end
    local deadline = clock.monotonic() + (a.timeout or 50)
    if not drain(sp.id, deadline) then
        M.stats.lock_timeouts = M.stats.lock_timeouts + 1
        return undo({'timeout', 1205, LOCK_TIMEOUT_MSG})
    end
    local bad = run_checks(sp, a.checks)
    if bad then return undo(bad) end
    bad = build_indexes(sp, a.builds)
    if bad then return undo(bad) end
    if fills then
        -- The steps above may yield for long: a later run of the job may have
        -- prepared meanwhile, or the change been aborted.
        yield_point('alter_prepare')
        local r = UIX.record(sp.id)
        if r == nil or r.ver ~= a.ver or r.aborted or r.gen ~= gen then
            return undo({'error', 1412, STALE_DEF_MSG})
        end
    end
    for _, u in ipairs(a.uix or {}) do create_space(u) end
    if fills then
        -- (Creating them yields on the WAL: once more.)
        local r = UIX.record(sp.id)
        if r == nil or r.ver ~= a.ver or r.aborted or r.gen ~= gen then
            for _, u in ipairs(a.uix or {}) do
                if box.space[u.id] ~= nil then box.space[u.id]:drop() end
            end
            return undo({'error', 1412, STALE_DEF_MSG})
        end
    end
    return {OK}
end

-- The stored fence of table space `sid` (see tarn_alter_prepare):
-- {ver, gen} or {ver, aborted = true}, or nil. Materialization also records
-- preparing/prepared/applying/aborting/aborted; only the final aborted state
-- sets aborted=true, so a restart retains the fence throughout undo.
function UIX.record(sid)
    local t = box.space._tarn_meta:get{'fence:' .. sid}
    return t ~= nil and type(t.value) == 'table' and t.value or nil
end

-- Whether generation `gen` of the job still fills the new global indexes of
-- the ALTER TABLE of table space `sid` to version `ver` here: the change is
-- prepared (its fence stored) by that run, and not applied. A run that took
-- the job over prepares again on every shard before it fills, so a late fill
-- of the run it took the job from is refused everywhere -- including after
-- some shard applied, when rows written with the new definition change again.
function UIX.filling(sid, ver, gen)
    local sp = box.space[sid]
    if sp == nil or catalog_ver(sp.name) >= ver then return false end
    local v = UIX.record(sid)
    return v ~= nil and v.ver == ver and not v.aborted and v.gen == gen
           and (v.phase == nil or v.phase == 'prepared')
end

-- Insert entries `tuples` into new global index space `id` of the ALTER
-- TABLE of table space `sid` to `ver`, in one local transaction: {'ok'},
-- or {'duplicate', i} when the i-th entry's key is held by another row, of
-- `owner` (see UIX.filling). An entry already there as it is counts as
-- written. Until a shard applies the change, its rows cannot change, so
-- every fill writes the same entries: a call sent again after a lost
-- connection, and the fill of a server that took the job over. A space is
-- never emptied for a new fill (ABORT drops it, and PREPARE makes a new one).
function tarn_uix_fill(id, tuples, sid, ver, gen)
    writable()
    local ok, res = pcall(function()
        box.begin(RW)
        -- The owner/phase/catalog read belongs to the same transaction as
        -- the entries. A takeover or APPLY cannot leave a late fill behind.
        if not UIX.filling(sid, ver, gen) then
            box.rollback()
            return {'error', 1412, STALE_DEF_MSG}
        end
        local sp = box.space[id]
        if sp == nil then
            box.rollback()
            return {'error', 1146, 'no such space ' .. tostring(id)}
        end
        yield_point('uix_fill')
        for i, t in ipairs(tuples) do
            local wrote, err = pcall(sp.insert, sp, t)
            if not wrote then
                local dup = type(err) == 'cdata' and err.code == box.error.TUPLE_FOUND
                if not (dup and msgpack.encode(err.old_tuple) == msgpack.encode(t)) then
                    box.rollback()
                    if dup then return {'duplicate', i} end
                    error(err)
                end
            end
        end
        yield_point('uix_fill_written')
        if not UIX.filling(sid, ver, gen) then
            box.rollback()
            return {'error', 1412, STALE_DEF_MSG}
        end
        box.commit()
        return {OK}
    end)
    if box.is_in_txn() then box.rollback() end
    return ok and res or failure(res)
end

function tarn_alter_apply(name, a, gen)
    writable()
    local sp = alter_space(name, a)
    if sp == nil then return {'error', 1146, "Table '" .. name .. "' doesn't exist"} end
    if catalog_ver(sp.name) >= a.ver then
        if a.materialize == true then
            local r = UIX.mat_clean(sp, a, gen, false)
            if r[1] ~= OK then return r end
        end
        return {OK, 'applied'}
    end
    fence_fresh()
    if a.materialize == true then
        local bad
        gen, bad = UIX.mat_args(sp, a, gen)
        if bad then return bad end
        local r = UIX.mat_phase(sp, a, gen, 'applying')
        if r[1] ~= OK or r[2] ~= nil then return r end
    elseif (FENCE[sp.id] or 0) < a.ver then
        -- The fence PREPARE raised is gone (restart, new leader): again.
        local r = tarn_alter_prepare(sp.name, a)
        if r[1] ~= OK then return r end
    end
    -- (PREPARE built them; an index dropped since is built again here,
    -- behind the fence.)
    local bad = build_indexes(sp, a.builds)
    if bad then return bad end
    for _, id in ipairs(a.drop or {}) do
        if sp.index[id] ~= nil then sp.index[id]:drop() end
    end
    for _, id in ipairs(a.uix_drop or {}) do
        if box.space[id] ~= nil then box.space[id]:drop() end
    end
    if a.format ~= nil and a.format ~= box.NULL then sp:format(a.format) end
    for _, r in ipairs(a.renames or {}) do
        local idx = sp.index[r[1]]
        if idx ~= nil and idx.name ~= r[2] then idx:rename(r[2]) end
    end
    for _, b in ipairs(a.builds or {}) do
        local idx = sp.index[b[1]]
        if idx ~= nil and idx.name ~= b[2] then idx:rename(b[2]) end
        PENDING_NAME[sp.id .. '/' .. b[1]] = nil
    end
    if a.rename ~= nil and a.rename ~= box.NULL and sp.name ~= a.rename then sp:rename(a.rename) end
    yield_point('alter_schema')
    box.begin(RW)
    if a.materialize == true then
        local lost = UIX.mat_owned(sp, a, gen, 'applying')
        if lost then box.rollback(); return lost end
    end
    -- Native RENAME may already have committed before a crash/retry. Its
    -- current name then is the target; the request still binds the old key.
    if name ~= sp.name then box.space._tarn_catalog:delete{name} end
    M.lazy.retire(a.lazy_drop)
    M.lazy.activate(a.def)
    box.space._tarn_catalog:replace{sp.name, a.def}
    box.space._tarn_meta:delete{'fence:' .. sp.id}
    box.commit()
    if (FENCE[sp.id] or 0) < a.ver then FENCE[sp.id] = a.ver end
    BK.ns_fence(sp.id)
    invalidate_key_defs()
    M.stats.alters = M.stats.alters + 1
    if a.materialize == true then
        yield_point('alter_backfill_apply')
        local r = UIX.mat_clean(sp, a, gen, false)
        if r[1] ~= OK then return r end
    end
    return {OK}
end

function tarn_alter_abort(name, a, gen)
    writable()
    local sp = alter_space(name, a)
    if sp == nil then return {OK} end
    local cur = catalog_ver(sp.name)
    if cur >= a.ver then return {OK, 'applied'} end
    if a.materialize == true then
        local bad
        gen, bad = UIX.mat_args(sp, a, gen)
        if bad then return bad end
        local r = UIX.mat_phase(sp, a, gen, 'aborting')
        if r[1] ~= OK or r[2] ~= nil then return r end
        drop_built(sp, a.builds)
        for _, u in ipairs(a.uix or {}) do
            if box.space[u.id] ~= nil then box.space[u.id]:drop() end
        end
        r = UIX.mat_clean(sp, a, gen, true)
        if r[1] ~= OK then return r end
        r = UIX.mat_tx(function()
            local lost = UIX.mat_owned(sp, a, gen, 'aborting')
            if lost then return lost end
            if box.space._tarn_alter_undo.index[0]:count{sp.id, a.ver} ~= 0 then
                return {'error', 1412, 'ALTER undo cleanup is not complete'}
            end
            box.space._tarn_meta:replace{'fence:' .. sp.id,
                {ver = a.ver, gen = gen, phase = 'aborted', materialize = true, aborted = true}}
            return nil
        end)
        if r[1] ~= OK then return r end
        M.stale_fences()
        fence_fresh()
        return {OK}
    end
    drop_built(sp, a.builds)
    for _, u in ipairs(a.uix or {}) do
        if box.space[u.id] ~= nil then box.space[u.id]:drop() end
    end
    -- The stored fence: a tombstone for this change (a late PREPARE of it,
    -- from a run of the job that lost it, may not fence the table again;
    -- the next ALTER TABLE of the table replaces it, or APPLY deletes it),
    -- unless a later change has begun since (a record of a later version
    -- stays as it is).
    local r = UIX.record(sp.id)
    local later = r ~= nil and r.ver > a.ver
    if not later and a.uix ~= nil and a.uix ~= box.NULL and #a.uix > 0 then
        box.space._tarn_meta:replace{'fence:' .. sp.id, {ver = a.ver, aborted = true}}
    elseif not later and r ~= nil and not r.aborted then
        box.space._tarn_meta:delete{'fence:' .. sp.id}
    end
    fence_fresh()
    if later and not r.aborted then
        FENCE[sp.id] = r.ver
    else
        FENCE[sp.id] = cur > 0 and cur or nil
    end
    BK.ns_fence(sp.id)
    return {OK}
end

-- Native index/format DDL may yield before an index becomes visible in
-- box.space. Serialize the whole ALTER RPC, not just its final phase check:
-- ABORT cannot miss an in-flight build, and an older finisher cannot resume
-- after a later ALTER reused an index ID. The lock is process-local: a
-- restart also kills all the native DDL fibers it protects. Cache/election
-- refreshes must NOT reset it. APPLY -> PREPARE is reentrant on one fiber.
UIX.alter_locks, UIX.alter_cond = {}, fiber.cond()
function UIX.alter_admit(fn, op, name, a, gen, sid)
    writable()
    local current = alter_space(name, a)
    if current == nil or current.id ~= sid or (a.id ~= nil and a.id ~= box.NULL and a.id ~= current.id)
       or (type(a.def) == 'table' and a.def.id ~= nil
       and a.def.id ~= box.NULL and a.def.id ~= current.id) then return {'error', 1412, STALE_DEF_MSG} end
    -- Admission is checked AFTER acquiring the lock. In particular, a
    -- delayed ordinary BUILD must not touch a newer job's reused IDs.
    if catalog_ver(current.name) < a.ver then
        local r = UIX.record(sid)
        if r ~= nil and (r.ver > a.ver or (r.ver == a.ver and
           ((gen ~= nil and gen ~= box.NULL and (r.gen or 0) > gen)
            or ((r.aborted or r.phase == 'aborting') and op ~= 'abort')
            or (r.phase == 'applying' and op ~= 'apply')
            or (r.materialize and a.materialize ~= true)))) then
            return {'error', 1412, STALE_DEF_MSG}
        end
    end
    local lazy = M.lazy.ddl_guard(op, name, a)
    if lazy then return lazy end
    if op == 'prepare' then M.lazy.prepare(a.def, gen) end
    local result = fn(name, a, gen)
    if op == 'abort' and result[1] == OK and result[2] ~= 'applied' then M.lazy.abort(a.def) end
    return result
end
function UIX.alter_serial(fn, op, name, a, gen)
    local sp = alter_space(name, a)
    if sp == nil then return fn(name, a, gen) end
    local sid, me = sp.id, fiber.self()
    local deadline = clock.monotonic() + (a.timeout or 50)
    while UIX.alter_locks[sid] ~= nil and UIX.alter_locks[sid].owner ~= me do
        local left = deadline - clock.monotonic()
        if left <= 0 then return {'timeout', 1205, LOCK_TIMEOUT_MSG} end
        UIX.alter_cond:wait(math.min(left, 0.05))
    end
    local lock = UIX.alter_locks[sid]
    if lock == nil then
        lock = {owner = me, depth = 0}
        UIX.alter_locks[sid] = lock
    end
    lock.depth = lock.depth + 1
    -- A named protected body avoids allocating a closure after acquisition.
    local ok, res = pcall(UIX.alter_admit, fn, op, name, a, gen, sid)
    local rollback_ok, rollback_error = true, nil
    if box.is_in_txn() then rollback_ok, rollback_error = pcall(box.rollback) end
    -- No result-table allocation between the protected call and release:
    -- even an allocation/native error must not leak the per-space mutex.
    lock.depth = lock.depth - 1
    if lock.depth == 0 then UIX.alter_locks[sid] = nil; UIX.alter_cond:broadcast() end
    if not rollback_ok then error(rollback_error) end
    if not ok then error(res) end
    return res
end
UIX.alter_build, UIX.alter_prepare = tarn_alter_build, tarn_alter_prepare
UIX.alter_apply, UIX.alter_abort = tarn_alter_apply, tarn_alter_abort
function tarn_alter_build(name, a, gen) return UIX.alter_serial(UIX.alter_build, 'build', name, a, gen) end
function tarn_alter_prepare(name, a, gen) return UIX.alter_serial(UIX.alter_prepare, 'prepare', name, a, gen) end
function tarn_alter_apply(name, a, gen) return UIX.alter_serial(UIX.alter_apply, 'apply', name, a, gen) end
function tarn_alter_abort(name, a, gen) return UIX.alter_serial(UIX.alter_abort, 'abort', name, a, gen) end

-------------------------------------------------------------------------------
-- CREATE TABLE, DROP TABLE and TRUNCATE (issue #30)
-------------------------------------------------------------------------------
-- Each runs as a job (see "Jobs" below) in steps on every shard, as ALTER
-- TABLE does, so that it takes effect on every shard or on none:
--   CREATE    PREPARE creates the space and its indexes, which no catalog
--             names yet (the SQL servers read the first shard's); APPLY
--             names it in _tarn_catalog; ABORT drops it.
--   DROP,     PREPARE raises the table's fence (writes made with its current
--   TRUNCATE  definition get 1412) and holds the table (DDL_HELD: reads that
--             check W locks wait, and validation of transactions that read
--             it fails), then waits for the local commits in flight and for
--             the prepared transactions holding locks on it (drain). APPLY
--             drops or truncates it, dooms the open streams that may have
--             read it before (DOOMED) and releases it; TRUNCATE also stores
--             the table's definition under a new version, so that writes
--             encoded before it stay refused. ABORT lowers the fence and
--             releases the table.
-- A TRUNCATE or DROP thus acts like a cross-shard transaction that writes
-- every row of the table: no transaction sees it applied on one shard and
-- not yet on another. Every step can be repeated.

-- a = {def = the catalog entry, as tarn_create_table takes it}
function tarn_create_prepare(name, a, gen)
    writable()
    local sp = box.space[name]
    if sp ~= nil and sp.id ~= a.def.id then
        return {'error', 1050, "Table '" .. name .. "' already exists"}
    end
    create_space(a.def)   -- (again: whatever an earlier attempt left out)
    M.lazy.prepare(a.def, gen)
    return {OK}
end

function tarn_create_apply(name, a)
    writable()
    local lazy = M.lazy.ddl_guard('create_apply', name, a)
    if lazy then return lazy end
    yield_point('ddl_apply')   -- tests: a shard that fails while it applies
    local sp = box.space[name]
    if sp ~= nil and sp.id ~= a.def.id then
        return {'error', 1050, "Table '" .. name .. "' already exists"}
    end
    create_space(a.def)   -- (if anything is missing)
    if box.space._tarn_catalog:get{name} == nil then
        box.begin(RW)
        M.lazy.activate(a.def)
        box.space._tarn_catalog:replace{name, a.def}
        box.commit()
        invalidate_key_defs()
        M.stats.creates = M.stats.creates + 1
    end
    return {OK}
end

function tarn_create_abort(name, a)
    writable()
    local lazy = M.lazy.ddl_guard('abort', name, a)
    if lazy then return lazy end
    local sp = box.space[name]
    if sp == nil or sp.id ~= a.def.id then return {OK} end
    if box.space._tarn_catalog:get{name} ~= nil then return {OK, 'applied'} end
    M.lazy.abort(a.def)
    UIX.drop(sp.id)
    sp:drop()
    return {OK}
end

local function hold_key(sid) return 'hold:' .. sid end
local NO_TABLE = "Table '%s' doesn't exist"

-- The table a DROP or TRUNCATE (op) with arguments a = {ver, timeout, id[,
-- def]} acts on, or nil when it is gone or (TRUNCATE) already applied.
local function clear_target(op, name, a)
    local sp = box.space[name]
    if sp == nil or sp.id ~= a.id then return nil end
    if op == 'TRUNCATE' and catalog_ver(name) >= a.ver then return nil end
    return sp
end

local function clear_prepare(op, name, a, gen)
    writable()
    local lazy = M.lazy.ddl_guard(op == 'DROP' and 'drop' or 'truncate', name, a)
    if lazy then return lazy end
    if op == 'TRUNCATE' then M.lazy.prepare(a.def, gen) end
    local sp = clear_target(op, name, a)
    if sp == nil then
        if op == 'TRUNCATE' and box.space[name] == nil then return {'error', 1146, string.format(NO_TABLE, name)} end
        return {OK, 'applied'}
    end
    local sid, label = sp.id, string.format('(%s TABLE %s)', op, name)
    -- Stored first, so that the fence and the hold survive a restart or a
    -- leader change (fence_rebuild).
    box.space._tarn_meta:replace{hold_key(sid), {op = op, ver = a.ver, label = label}}
    fence_fresh()
    if (FENCE[sid] or 0) < a.ver then FENCE[sid] = a.ver; BK.ns_fence(sid) end
    ddl_hold(sid, label)
    if not drain(sid, clock.monotonic() + (a.timeout or 50)) then
        box.space._tarn_meta:delete{hold_key(sid)}
        local cur = catalog_ver(name)
        FENCE[sid] = cur > 0 and cur or nil
        BK.ns_fence(sid)
        ddl_release(sid)
        M.stats.lock_timeouts = M.stats.lock_timeouts + 1
        return {'timeout', 1205, LOCK_TIMEOUT_MSG}
    end
    return {OK}
end

local function clear_apply(op, name, a)
    writable()
    local lazy = M.lazy.ddl_guard(op == 'DROP' and 'drop' or 'truncate_apply', name, a)
    if lazy then return lazy end
    yield_point('ddl_apply')   -- tests: a shard that fails while it applies
    local sp = box.space[name]
    if sp ~= nil and sp.id ~= a.id then sp = nil end
    if sp == nil then
        M.lazy.retire(a.lazy_drop)
        if op == 'TRUNCATE' then return {'error', 1146, string.format(NO_TABLE, name)} end
        -- Dropped by an earlier APPLY: finish what it may have left.
        UIX.drop(a.id)
        M.lazy.clear_versions(a.id)
        box.begin(RW)
        box.space._tarn_lazy_capture:delete{a.id}
        local c = box.space._tarn_catalog:get{name}
        if c ~= nil and c.def.id == a.id then box.space._tarn_catalog:delete{name} end
        box.space._tarn_meta:delete{hold_key(a.id)}
        box.commit()
        ddl_release(a.id)
        return {OK, 'applied'}
    end
    if op == 'TRUNCATE' and catalog_ver(name) >= a.ver then return {OK, 'applied'} end
    local sid = sp.id
    fence_fresh()
    if (FENCE[sid] or 0) < a.ver or DDL_HELD[sid] == nil then
        -- Fence or hold gone (a PREPARE that timed out, a lost one): again.
        local r = clear_prepare(op, name, a)
        if r[1] ~= OK or r[2] == 'applied' then return r end
    end
    if op == 'DROP' then
        M.lazy.retire(a.lazy_drop)
        UIX.drop(sid)
        sp:drop()
        yield_point('lazy_clear_schema')
        M.lazy.clear_versions(sid)
        box.begin(RW)
        box.space._tarn_lazy_capture:delete{sid}
        box.space._tarn_catalog:delete{name}
        box.space._tarn_meta:delete{hold_key(sid)}
        box.commit()
        -- From here on nothing yields: until the table is released below,
        -- the hold fails the validation of its readers; from then on, the
        -- streams that began before are doomed.
        doom_readers(sid)
        FENCE[sid] = nil
        BK.ns_fence(sid)
        M.stats.drops = M.stats.drops + 1
    else
        for _, u in ipairs(UIX.spaces(sid)) do u:truncate() end
        sp:truncate()
        yield_point('lazy_clear_schema')
        M.lazy.clear_versions(sid)
        box.begin(RW)
        M.lazy.retire(a.lazy_drop)
        M.lazy.activate(a.def)
        box.space._tarn_catalog:replace{name, a.def}
        box.space._tarn_meta:delete{hold_key(sid)}
        box.commit()
        doom_readers(sid)
        FENCE[sid] = a.ver
        BK.ns_fence(sid)
        M.stats.truncates = M.stats.truncates + 1
    end
    invalidate_key_defs()
    ddl_release(sid)
    return {OK}
end

local function clear_abort(op, name, a)
    writable()
    local lazy = M.lazy.ddl_guard(op == 'DROP' and 'drop' or 'abort', name, a)
    if lazy then return lazy end
    local sp = box.space[name]
    if sp ~= nil and sp.id == a.id and op == 'TRUNCATE' and catalog_ver(name) >= a.ver then
        return {OK, 'applied'}
    end
    if op == 'TRUNCATE' then M.lazy.abort(a.def) end
    box.space._tarn_meta:delete{hold_key(a.id)}
    if sp ~= nil and sp.id == a.id then
        fence_fresh()
        local cur = catalog_ver(name)
        FENCE[sp.id] = cur > 0 and cur or nil
        BK.ns_fence(sp.id)
    end
    ddl_release(a.id)
    return {OK}
end

-- Clear uses native DDL too. Keep retries/finishers and source bootstrap
-- under the same reentrant lock as ALTER until metadata publication/release.
UIX.drop_prepare = function(name, a, gen) return clear_prepare('DROP', name, a, gen) end
UIX.drop_apply = function(name, a) return clear_apply('DROP', name, a) end
UIX.drop_abort = function(name, a) return clear_abort('DROP', name, a) end
UIX.truncate_prepare = function(name, a, gen) return clear_prepare('TRUNCATE', name, a, gen) end
UIX.truncate_apply = function(name, a) return clear_apply('TRUNCATE', name, a) end
UIX.truncate_abort = function(name, a) return clear_abort('TRUNCATE', name, a) end
function tarn_drop_prepare(name, a, gen) return UIX.alter_serial(UIX.drop_prepare, 'drop', name, a, gen) end
function tarn_drop_apply(name, a, gen) return UIX.alter_serial(UIX.drop_apply, 'drop', name, a, gen) end
function tarn_drop_abort(name, a, gen) return UIX.alter_serial(UIX.drop_abort, 'drop', name, a, gen) end
function tarn_truncate_prepare(name, a, gen)
    return UIX.alter_serial(UIX.truncate_prepare, 'truncate', name, a, gen)
end
function tarn_truncate_apply(name, a, gen)
    return UIX.alter_serial(UIX.truncate_apply, 'truncate_apply', name, a, gen)
end
function tarn_truncate_abort(name, a, gen)
    return UIX.alter_serial(UIX.truncate_abort, 'truncate', name, a, gen)
end

-------------------------------------------------------------------------------
-- Jobs (on the first shard)
-------------------------------------------------------------------------------
-- A job is a CREATE, DROP, TRUNCATE or ALTER TABLE in progress, or a bucket
-- move. It has one record per name it touches ('job:<name>' in _tarn_meta:
-- a table name, two for an ALTER TABLE that renames; 'bucket:<n>' for a
-- move), so that statements on other tables do not wait for it -- an ALTER
-- TABLE may build an index for hours. Moves and schema changes wait for
-- each other. Its phase is 'prepare' until every shard has prepared (a
-- move: until its last copy), then 'apply' ('flip': the move sends the
-- bucket), or 'abort' if one could not ('run': an ALTER TABLE begun by a
-- SQL server from before phases). A SQL server that finds a job abandoned
-- finishes it: back before 'apply' (or 'flip'), forward after.
--   job = {owner, kind, space, names, ver, a, phase}
local JOB = 'job:'
local JOB_COND = fiber.cond()
-- Schema changes and changes of the cluster's shards that wait for bucket
-- moves to end, by owner: when they last found a move in their way
-- (BK.want). New moves wait for them (tarn_job_begin), or a stream of moves
-- -- a rebalance, a drain -- could keep them waiting for ever. They wait in
-- calls of at most a few seconds, one after the other: an entry not
-- refreshed for BK.WANT_FRESH seconds is forgotten.
BK.want = {}
BK.WANT_FRESH = 1.0

-- The owner of a job other than `owner`'s that waits for moves (see
-- BK.want), or nil.
function BK.wanted(owner)
    local now = clock.monotonic()
    for o, t in pairs(BK.want) do
        if now - t >= BK.WANT_FRESH then
            BK.want[o] = nil
        elseif o ~= owner then
            return o
        end
    end
    return nil
end

local function job_value(key)
    local t = box.space._tarn_meta:get{key}
    return t and t.value or nil
end

-- The record keys of the job that `owner` runs.
local function job_keys(owner)
    local out = {}
    for _, t in box.space._tarn_meta:pairs({JOB}, {iterator = 'GE'}) do
        if t.key:sub(1, #JOB) ~= JOB then break end
        if t.value.owner == owner then out[#out + 1] = t.key end
    end
    return out
end

-- Update (ops) or delete (ops == nil) every record of the job that `owner`
-- runs, in one transaction that reads them too: of two SQL servers taking
-- over the same abandoned job, one finds it gone or fails on the conflict.
-- Field updates, so that the owner's keep-alive and its phase change do not
-- undo each other.
local function job_change(owner, ops)
    local ok, res = pcall(function()
        box.begin(RW)
        local keys = job_keys(owner)
        for _, k in ipairs(keys) do
            if ops == nil then box.space._tarn_meta:delete{k} else box.space._tarn_meta:update(k, ops) end
        end
        box.commit()
        return #keys
    end)
    if box.is_in_txn() then box.rollback() end
    if not ok then return {'conflict', 1213, tostring(res)} end
    if res == 0 then return {'gone'} end
    return {OK}
end

-- Begin a job, waiting up to `wait` seconds while another holds one of its
-- names: {'ok', mver}, or {'busy', owner, age} of the job in the way. mver:
-- this shard's membership version (see "Membership"); a job acts on every
-- shard, and its server reads a newer membership first.
function tarn_job_begin(job, wait)
    writable()
    BK.fresh()
    local deadline = clock.monotonic() + (tonumber(wait) or 0)
    while true do
        local busy
        -- Read-committed: a job that another call inserted and that still
        -- waits for its WAL write is invisible to a read-confirmed read, and
        -- the insert below would then fail on the key instead of finding it.
        local ok = pcall(function()
            box.begin({txn_isolation = 'read-committed'})
            for _, n in ipairs(job.names) do
                busy = job_value(JOB .. n)
                if busy ~= nil then break end
            end
            -- Bucket moves and schema changes wait for each other: a move
            -- copies the rows of every table, and a schema change of a
            -- table acts on its rows on every shard. A change of the
            -- cluster's shards (kind 'members') waits for every other job,
            -- and every job for it: it copies every table to a shard that
            -- joins, and moves wait until every shard knows it.
            if busy == nil then
                for _, t in box.space._tarn_meta:pairs({JOB}, {iterator = 'GE'}) do
                    if t.key:sub(1, #JOB) ~= JOB then break end
                    local k = t.value.kind
                    if t.value.owner ~= job.owner and
                       ((k == 'move') ~= (job.kind == 'move') or k == 'members' or job.kind == 'members') then
                        busy = t.value
                        break
                    end
                end
            end
            -- And a move waits while one of them waits for moves.
            if busy == nil and job.kind == 'move' and BK.wanted(job.owner) ~= nil then
                busy = {owner = '', ts = clock.realtime()}
            end
            if busy == nil then
                local v = {owner = job.owner, ts = clock.realtime(), kind = job.kind, space = job.space,
                           names = job.names, ver = job.ver, a = job.a, phase = job.phase or 'prepare', gen = 1}
                for _, n in ipairs(job.names) do box.space._tarn_meta:insert{JOB .. n, v} end
            end
            box.commit()
        end)
        if box.is_in_txn() then box.rollback() end
        if ok and busy == nil then
            BK.want[job.owner] = nil
            return {OK, BK.mver}
        end
        if job.kind ~= 'move' and busy ~= nil then
            BK.want[job.owner] = busy.kind == 'move' and clock.monotonic() or nil
        end
        -- (A repeated call whose first one began the job: see Cluster::call.)
        if busy ~= nil and busy.owner == job.owner then return {OK, BK.mver} end
        local left = deadline - clock.monotonic()
        if left <= 0 then
            if busy == nil then return {'busy', '', 0} end   -- (one began at the same moment)
            return {'busy', busy.owner, clock.realtime() - busy.ts}
        end
        JOB_COND:wait(math.min(left, 0.25))
    end
end

-- Change the phase of the job (and keep it alive).
function tarn_job_phase(owner, phase)
    writable()
    return job_change(owner, {{'=', '[2].phase', phase}, {'=', '[2].ts', clock.realtime()}})
end
-- The job's arguments (a bucket move learns them once it holds its bucket).
function tarn_job_args(owner, a)
    writable()
    return job_change(owner, {{'=', '[2].a', a}, {'=', '[2].ts', clock.realtime()}})
end

-- Keep the job alive (owner), or take over an abandoned one (owner, from):
-- then {'ok', generation}. Each takeover counts one more generation of the
-- job, which its steps carry, so that the shards can tell a later run of the
-- job from an earlier one (see UIX.filling).
function tarn_job_touch(owner, from)
    writable()
    if from ~= nil and from ~= box.NULL then
        local keys = job_keys(from)
        local cur = keys[1] and job_value(keys[1])
        local gen = ((cur and cur.gen) or 1) + 1
        local r = job_change(from, {{'=', '[2].owner', owner}, {'=', '[2].ts', clock.realtime()},
                                    {'=', '[2].gen', gen}})
        if r[1] == OK then return {OK, gen} end
        -- (A repeated call whose first one took it over.)
        local mine = job_keys(owner)
        if r[1] == 'gone' and #mine > 0 then return {OK, job_value(mine[1]).gen or 1} end
        return r
    end
    return job_change(owner, {{'=', '[2].ts', clock.realtime()}})
end

function tarn_job_end(owner)
    writable()
    local r = job_change(owner, nil)
    JOB_COND:broadcast()
    if r[1] == 'gone' then return {OK} end
    return r
end

-- The jobs in progress: {{owner, age, kind, space, phase, ver, a, names}, ...}
function tarn_jobs()
    local out, seen = {}, {}
    for _, t in box.space._tarn_meta:pairs({JOB}, {iterator = 'GE'}) do
        if t.key:sub(1, #JOB) ~= JOB then break end
        local v = t.value
        if not seen[v.owner] then
            seen[v.owner] = true
            out[#out + 1] = {v.owner, clock.realtime() - v.ts, v.kind, v.space, v.phase, v.ver, v.a, v.names}
        end
    end
    return out
end

-- For SQL servers from before per-table jobs, which run ALTER TABLE only
-- this way: job = {owner, space, ver, a}.
function tarn_ddl_begin(job)
    return tarn_job_begin({owner = job[1], kind = 'alter', space = job[2], names = {job[2]},
                           ver = job[3], a = job[4], phase = 'run'}, 0)
end
function tarn_ddl_touch(owner, from) return tarn_job_touch(owner, from) end
function tarn_ddl_end(owner) return tarn_job_end(owner) end
-- {OK, owner, age, space, ver, a} of an ALTER TABLE in progress, or {OK}.
function tarn_ddl_job()
    for _, j in ipairs(tarn_jobs()) do
        if j[3] == 'alter' then return {OK, j[1], j[2], j[4], j[6], j[7]} end
    end
    return {OK}
end

-- A job record from before per-table jobs ('ddl:job'), made one.
local function migrate_legacy_job()
    local old = box.space._tarn_meta:get{'ddl:job'}
    if old == nil then return end
    local v = old.value
    box.begin(RW)
    if box.space._tarn_meta:get{JOB .. v.space} == nil then
        box.space._tarn_meta:insert{JOB .. v.space, {owner = v.owner, ts = v.ts, kind = 'alter', space = v.space,
                                                     names = {v.space}, ver = v.ver, a = v.a, phase = 'run'}}
    end
    box.space._tarn_meta:delete{'ddl:job'}
    box.commit()
end

function tarn_truncate(name)
    local lazy = M.lazy.ddl_guard('legacy', name, {})
    if lazy then return lazy end
    if box.space[name] ~= nil then
        for _, u in ipairs(UIX.spaces(box.space[name].id)) do u:truncate() end
        box.space[name]:truncate()
    end
    return {OK}
end

function tarn_catalog()
    local out = {}
    for _, t in box.space._tarn_catalog:pairs() do
        local s = box.space[t.name]
        local bix = s and s.index.bucket_id and s.index.bucket_id.id or box.NULL
        out[#out + 1] = {t.name, s and s.id or 0, t.def.json or '', bix}
    end
    return out
end

-- Allocate `n` consecutive values of counter `name` (AUTO_INCREMENT batches).
function tarn_counter_alloc(name, n)
    box.begin(RW)
    local r = box.space._tarn_meta:get{'ctr:' .. name}
    local start = (r and r.value or 0) + 1
    box.space._tarn_meta:replace{'ctr:' .. name, start + n - 1}
    box.commit()
    return start
end

-- Make sure a counter is at least `v` (explicit ids inserted by clients).
function tarn_counter_bump(name, v)
    box.begin(RW)
    local r = box.space._tarn_meta:get{'ctr:' .. name}
    if r == nil or r.value < v then box.space._tarn_meta:replace{'ctr:' .. name, v} end
    box.commit()
    return true
end

function tarn_db_create(name)
    box.space._tarn_meta:replace{'db:' .. name, true}
    return {OK}
end

function tarn_db_drop(name)
    box.space._tarn_meta:delete{'db:' .. name}
    return {OK}
end

function tarn_db_list()
    local out = {}
    for _, t in box.space._tarn_meta:pairs({'db:'}, {iterator = 'GE'}) do
        if t.key:sub(1, 3) ~= 'db:' then break end
        out[#out + 1] = t.key:sub(4)
    end
    return out
end

function tarn_catalog_version()
    local r = box.space._tarn_meta:get{'ctr:catalog_version'}
    return r and r.value or 0
end

-------------------------------------------------------------------------------
-- Bucket moves (issue #24; see "Buckets")
-------------------------------------------------------------------------------
-- One SQL server moves bucket b from shard S to shard R as a job (kind
-- 'move', see "Jobs"), in steps that may each be repeated:
--   R  tarn_bucket_recv_begin  receiving: the rows are kept aside (_tarn_recv)
--   S  tarn_bucket_send_begin  sending: the writes to its rows are noted
--                              from now on, once the commits in flight end
--      copy                    tarn_bucket_rows on S, tarn_bucket_recv_rows
--                              on R, a page at a time, while S serves it
--   R  tarn_bucket_hold        held
--   S  tarn_bucket_fence       fenced (writes, row locks and PREPAREs that
--                              read or write it refused; the row locks on
--                              its rows broken), then drained: no local
--                              commit in flight, no prepared transaction's
--                              lock on its rows
--      last copy               tarn_bucket_delta on S: the rows written
--                              since the copy began ('lost' after a leader
--                              change: then all of them again)
--   S  tarn_bucket_send        the rows deleted: sent (the job's phase is
--                              'flip' first: from here on only forward)
--   R  tarn_bucket_recv        the rows inserted: active
-- A move given up before the send releases both sides (tarn_bucket_send_abort,
-- tarn_bucket_recv_abort). (TarnCommit: MoveFence, MoveDrained,
-- MoveSendFlip, MoveRecvFlip, MoveAbort.)

-- The data spaces (with a bucket_id index): the tables, their global
-- indexes and lazy metadata, in space id order. Only the no-map legacy
-- table report excludes lazy spaces; moves include them by default.
function BK.spaces(include_lazy)
    local ids = {}
    for _, t in box.space._tarn_catalog:pairs() do
        local sp = box.space[t.name]
        if sp ~= nil and sp.index.bucket_id ~= nil then
            ids[#ids + 1] = sp.id
            for _, u in ipairs(UIX.spaces(sp.id)) do
                if u.index.bucket_id ~= nil then ids[#ids + 1] = u.id end
            end
        end
    end
    if include_lazy ~= false then
        for _, id in ipairs(M.lazy.spaces()) do ids[#ids + 1] = id end
    end
    table.sort(ids)
    return ids
end

-- Write the state of bucket b (nil: forget it) in the current transaction,
-- and with bump the epoch; then, once committed, BK.applied(b, ...) makes
-- it the state in memory.
function BK.store(b, state, peer, move, bump)
    if state == nil then
        box.space._tarn_buckets:delete{b}
    else
        box.space._tarn_buckets:replace{b, state, peer or box.NULL, move or box.NULL}
    end
    if bump then
        local m = box.space._tarn_meta:get{'buckets'}
        box.space._tarn_meta:replace{'buckets', {count = m.value.count, epoch = m.value.epoch + 1}}
        return m.value.epoch + 1
    end
    return nil
end

function BK.applied(b, state, peer, move, epoch)
    if BK.state[b] == 'held' and state ~= 'held' then
        BK.staged[b] = nil
        BK.held = BK.held - 1
    elseif state == 'held' and BK.state[b] ~= 'held' then
        BK.staged[b] = BK.load_staged(b)
        BK.held = BK.held + 1
    end
    BK.state[b], BK.peer[b], BK.move[b] = state, peer, move
    if epoch ~= nil then BK.epoch = epoch end
    BK.ns_serve(b)
    if state ~= 'sending' and state ~= 'fenced' and BK.track[b] ~= nil then
        BK.track[b] = nil
        BK.track_n = BK.track_n - 1
    end
    BK.ver = BK.ver + 1
end

-- Run fn(...) while no other step changes the state of a bucket here:
-- the steps that store a state run one at a time, and check the state
-- they expect under the latch (each bumps or reads the epoch, and the steps
-- of two moves, or of a move and its takeover, must not interleave).
BK.busy, BK.cond = nil, fiber.cond()
function BK.locked(fn, ...)
    local me = fiber.id()
    if BK.busy == me then return fn(...) end   -- (re-entered)
    while BK.busy ~= nil do BK.cond:wait() end
    BK.busy = me
    local ok, r = pcall(fn, ...)
    BK.busy = nil
    BK.cond:broadcast()
    if not ok then error(r) end
    return r
end

-- Store a state in a transaction of its own (under the latch).
function BK.set(b, state, peer, move, bump)
    BK.locked(function()
        box.begin(RW)
        local ok, r = pcall(BK.store, b, state, peer, move, bump)
        if not ok then
            box.rollback()
            error(r)
        end
        BK.commit()
        BK.applied(b, state, peer, move, r)
    end)
end

-- box.commit() of a step that stores a bucket state. If it fails, a
-- rebuild meanwhile may have read the state it stored (BK.rebuild reads the
-- commits in progress too): the state in memory is read again.
function BK.commit()
    local ok, err = pcall(box.commit)
    if not ok then
        if box.is_in_txn() then box.rollback() end
        BK.stale = true
        BK.ns_sync()
        error(err)
    end
end

-- Inside the transaction that stores it: make bucket b's state `state` in
-- memory now (and back if the transaction rolls back), before its writes
-- become visible -- as box.commit() waits for the WAL, other requests run,
-- and once the rows of a bucket sent are gone they must find it sent. With
-- state nil, bump the epoch alone: once the rows of a bucket received are
-- there, a read of every shard made with the old map must be refused,
-- while the bucket itself is served only once they are (BK.applied after
-- the commit).
function BK.early(b, state, peer, move, epoch)
    local old = {BK.state[b], BK.peer[b], BK.move[b], BK.epoch}
    if state ~= nil then
        BK.applied(b, state, peer, move, epoch)
    else
        BK.epoch, BK.ver = epoch, BK.ver + 1
        BK.ns_sync()
    end
    box.on_rollback(function()
        if state ~= nil then
            BK.applied(b, old[1], old[2], old[3], old[4])
        else
            BK.epoch, BK.ver = old[4], BK.ver + 1
            BK.ns_sync()
        end
    end)
end

-- Delete the rows of bucket b kept aside, in the current transaction.
function BK.clear_recv(b)
    local keys = {}
    for _, t in box.space._tarn_recv:pairs({b}, {iterator = 'EQ'}) do keys[#keys + 1] = {b, t.space_id, t.key} end
    for _, k in ipairs(keys) do box.space._tarn_recv:delete(k) end
end

-- Bucket b is not in the state a step expects (for move `move`).
function BK.unexpected(b, step)
    return {'error', 0, string.format('%s: bucket %d is %s on shard %d (move %s)', step, b,
                                      tostring(BK.state[b] or 'not here'), M.cfg.shard_id, tostring(BK.move[b]))}
end

-- Break the row locks on the rows of bucket b, and fail the requests that
-- wait for one: their transactions can no longer commit (TarnCommit:
-- MoveFence).
function BK.break_plocks(b)
    local victims = {}
    for txid, p in pairs(PTX) do
        local hit = p.waiting ~= nil and p.waiting.entry.bucket == b
        if not hit then
            for _, k in pairs(p.keys) do
                local e = pl_entry(k[1], k[2], k[3], false)
                if e ~= nil and e.bucket == b then
                    hit = true
                    break
                end
            end
        end
        if hit then victims[#victims + 1] = txid end
    end
    -- Their waiting requests fail first: the locks one of them releases
    -- must not be granted to another.
    for _, txid in ipairs(victims) do
        PL_BROKEN[txid] = clock.monotonic() + M.cfg.max_txn_age + M.cfg.clock_skew
        local q = PTX[txid].waiting
        if q ~= nil and not q.granted and q.failed == nil then
            q.failed = 'released'
            q.cond:signal()
        end
    end
    for _, txid in ipairs(victims) do pl_release_all(txid) end
    M.stats.move_broken_locks = M.stats.move_broken_locks + #victims
end

-- The map of a new cluster, made by the first SQL server: this shard's
-- buckets, as ranges {{lo, hi}, ...}, out of `count`. {'ok'}, or
-- {'exists', count} when the shard has a map (which stays as it is) -- also
-- when another server made it at the same moment, perhaps with another
-- count: the server then makes the other shards' maps with that one.
function BK.map_count()
    -- (read-committed: also a map whose WAL write is in progress)
    box.begin(RW)
    local m = box.space._tarn_meta:get{'buckets'}
    box.commit()
    return m and m.value.count
end

function tarn_bucket_init(count, ranges)
    writable()
    local ok, res = pcall(function()
        box.begin(RW)
        local m = box.space._tarn_meta:get{'buckets'}
        if m ~= nil then
            box.rollback()
            return {'exists', m.value.count}
        end
        for _, r in ipairs(ranges) do
            for b = r[1], r[2] do box.space._tarn_buckets:replace{b, 'active', box.NULL, box.NULL} end
        end
        -- (insert: of two servers making the map at once, one fails here or
        -- in its commit, and its bucket records go with it)
        box.space._tarn_meta:insert{'buckets', {count = count, epoch = 1}}
        box.commit()
        return {OK}
    end)
    if box.is_in_txn() then box.rollback() end
    if not ok then
        local c = BK.map_count()
        if c == nil then error(res) end
        return {'exists', c}
    end
    if res[1] == OK then BK.stale = true; BK.ns_sync() end
    return res
end

-- This shard's map: {'ok', count, epoch, owned, others, mver}: owned = the
-- buckets it owns, as ranges {{lo, hi}, ...}; others = {{bucket, state,
-- peer, move}, ...} for those it has in other states; mver = its
-- membership version (see "Membership"). Without a map {'ok',
-- 0, tables}: the number of Tarn tables on the shard, whose rows were
-- stored by a version from before the map (with the count of that time).
-- It reports the state in memory, which the shard's checks go by (a step's
-- change shows once the step made it, as BK.early does), and builds the
-- report once per change: reading 15,000 buckets from _tarn_buckets takes
-- about 11 ms, and SQL servers read the map after every move.
function tarn_buckets()
    writable()
    BK.fresh()
    if BK.count == 0 then return {OK, 0, #BK.spaces(false)} end
    local r = BK.report
    if r ~= nil and r.ver == BK.ver then return r.reply end
    local owned, others, lo, hi = {}, {}, nil, nil
    local st = BK.state
    for b = 1, BK.count do
        local s = st[b]
        if s == 'active' or s == 'sending' or s == 'fenced' then
            if hi ~= nil and b == hi + 1 then
                hi = b
            else
                if lo ~= nil then owned[#owned + 1] = {lo, hi} end
                lo, hi = b, b
            end
        end
        if s ~= nil and s ~= 'active' then
            others[#others + 1] = {b, s, BK.peer[b] or box.NULL, BK.move[b] or box.NULL}
        end
    end
    if lo ~= nil then owned[#owned + 1] = {lo, hi} end
    BK.report = {ver = BK.ver, reply = {OK, BK.count, BK.epoch, owned, others, BK.mver}}
    return BK.report.reply
end

-- R: receive bucket b from shard `from` in move `move`.
function tarn_bucket_recv_begin(b, from, move)
    writable()
    BK.fresh()
    return BK.locked(function()
        local s = BK.state[b]
        if (s == 'receiving' or s == 'held') and BK.move[b] == move then return {OK} end
        if BK.count == 0 or (s ~= nil and s ~= 'sent') then return BK.unexpected(b, 'receive') end
        box.begin(RW)
        BK.clear_recv(b)   -- (what a move given up may have left)
        BK.store(b, 'receiving', from, move)
        BK.commit()
        BK.applied(b, 'receiving', from, move)
        return {OK}
    end)
end

-- S: send bucket b to shard `to` in move `move`. The writes to its rows
-- are noted from now on; the local commits in flight now (whose writes may
-- not be) are waited for, so that the copy, which starts after this, sees
-- them. {'ok', generation}: a delta needs the notes of that generation.
function tarn_bucket_send_begin(b, to, move, timeout)
    writable()
    BK.fresh()
    local lazy = M.lazy.move_ready(to)
    if lazy then return lazy end
    -- Capability discovery may yield or refresh a remote leader.
    writable()
    BK.fresh()
    local r = BK.locked(function()
        local s = BK.state[b]
        if not ((s == 'sending' or s == 'fenced') and BK.move[b] == move) then
            if s ~= 'active' then return BK.unexpected(b, 'send') end
            BK.set(b, 'sending', to, move)
        end
        if BK.track[b] == nil then
            BK.track[b] = {keys = {}, list = {}}
            BK.track_n = BK.track_n + 1
        end
        return nil
    end)
    if r ~= nil then return r end
    if not wait_commits(clock.monotonic() + (tonumber(timeout) or 10)) then
        M.stats.lock_timeouts = M.stats.lock_timeouts + 1
        return {'timeout', 1205, LOCK_TIMEOUT_MSG}
    end
    return {OK, BK.gen}
end

-- S: a page of the rows of bucket b, from `after` ({space_id, position} of
-- the page before): {'ok', {{space_id, tuple}, ...}, next} (next: nil at
-- the end). Read-confirmed: only rows a quorum has.
function tarn_bucket_rows(b, after)
    writable()
    yield_point('bucket_rows')   -- (tests: a move held while it copies)
    local start, pos
    if after ~= nil and after ~= box.NULL then start, pos = after[1], after[2] end
    local out, n, bytes = {}, 0, 0
    box.begin({txn_isolation = 'read-confirmed'})
    local ok, nxt = pcall(function()
        for _, sid in ipairs(BK.spaces()) do
            if start == nil or sid >= start then
                local idx = box.space[sid].index.bucket_id
                local opts = {iterator = 'EQ'}
                if sid == start and pos ~= nil and pos ~= box.NULL then opts.after = pos end
                for _, t in idx:pairs({b}, opts) do
                    n = n + 1
                    out[n] = {M.lazy.wire_space(sid), t}
                    bytes = bytes + t:bsize()
                    if PG.full(n, bytes) then return {sid, idx:tuple_pos(t)} end
                end
            end
        end
        return box.NULL
    end)
    if box.is_in_txn() then box.commit() end
    if not ok then error(nxt) end
    return {OK, out, nxt}
end

-- R: rows of bucket b copied from the sender, kept aside: {space_id,
-- tuple}, or {space_id, nil, primary key} for a row deleted since.
function tarn_bucket_recv_rows(b, move, rows)
    writable()
    BK.fresh()
    return BK.locked(function()
        local s = BK.state[b]
        if (s ~= 'receiving' and s ~= 'held') or BK.move[b] ~= move then
            return BK.unexpected(b, 'receive rows')
        end
        box.begin(RW)
        local changed = {}   -- {space id, key, tuple or nil}
        local ok, err = pcall(function()
            for _, r in ipairs(rows) do
                local sid = M.lazy.resolve_space(r[1])
                if box.space[sid] ~= nil then
                    local tuple = r[2] ~= box.NULL and r[2] or nil
                    local key
                    if tuple ~= nil then
                        tuple = box.tuple.new(tuple)
                        key = kbin(index_key_def(sid, 0):extract_key(tuple):totable())
                        box.space._tarn_recv:replace{b, sid, key, tuple}
                    else
                        key = kbin(r[3])
                        box.space._tarn_recv:delete{b, sid, key}
                    end
                    changed[#changed + 1] = {sid, key, tuple}
                end
            end
        end)
        -- Held: the rows the checks use (BK.held_conflict) change before the
        -- commit, and again after it (a rebuild meanwhile may have read them
        -- before).
        local function stage()
            local staged = BK.state[b] == 'held' and BK.staged[b] or nil
            if staged == nil then return end
            for _, ch in ipairs(changed) do
                local l = staged[ch[1]]
                if l == nil then l = {}; staged[ch[1]] = l end
                l[ch[2]] = ch[3]
            end
        end
        if ok then
            stage()
            ok, err = pcall(BK.commit)
        else
            box.rollback()
        end
        if not ok then
            if BK.state[b] == 'held' then BK.staged[b] = BK.load_staged(b) end
            error(err)
        end
        stage()
        return {OK}
    end)
end

-- R: drop the rows of bucket b kept aside (the move copies them again).
function tarn_bucket_recv_clear(b, move)
    writable()
    BK.fresh()
    return BK.locked(function()
        local s = BK.state[b]
        if (s ~= 'receiving' and s ~= 'held') or BK.move[b] ~= move then return BK.unexpected(b, 'clear') end
        box.begin(RW)
        BK.clear_recv(b)
        BK.commit()
        if BK.staged[b] ~= nil then BK.staged[b] = {} end
        return {OK}
    end)
end

-- R: hold bucket b (see "Buckets").
function tarn_bucket_hold(b, move)
    writable()
    BK.fresh()
    return BK.locked(function()
        local s = BK.state[b]
        if s == 'held' and BK.move[b] == move then return {OK} end
        if s ~= 'receiving' or BK.move[b] ~= move then return BK.unexpected(b, 'hold') end
        BK.set(b, 'held', BK.peer[b], move)
        return {OK}
    end)
end

-- S: fence bucket b, break the row locks on its rows, and wait up to
-- `timeout` seconds until it is drained: no local commit in flight, and no
-- prepared transaction holding a lock on its rows ({'timeout', 1205} if
-- not: the move gives up). Writes that passed the fence's check before it
-- was raised are in flight; those after it are refused.
function tarn_bucket_fence(b, move, timeout)
    writable()
    lk_fresh()
    pl_fresh()
    BK.fresh()
    local r = BK.locked(function()
        local s = BK.state[b]
        if s == 'sent' and BK.move[b] == move then return {OK, 'sent'} end
        if (s ~= 'sending' and s ~= 'fenced') or BK.move[b] ~= move then return BK.unexpected(b, 'fence') end
        if s == 'sending' then BK.set(b, 'fenced', BK.peer[b], move) end
        BK.break_plocks(b)
        return nil
    end)
    if r ~= nil then return r end
    yield_point('bucket_fence')   -- (tests: a move held while its fence drains)
    local deadline = clock.monotonic() + (tonumber(timeout) or 10)
    if not wait_commits(deadline) then
        M.stats.lock_timeouts = M.stats.lock_timeouts + 1
        return {'timeout', 1205, LOCK_TIMEOUT_MSG}
    end
    while true do
        lk_fresh()
        if (BK.nl[b] or 0) == 0 then
            if M.lazy.flush(b, deadline) then return {OK} end
            return {'timeout', 1205, 'bucket lazy delivery has not drained'}
        end
        if clock.monotonic() > deadline then
            M.stats.lock_timeouts = M.stats.lock_timeouts + 1
            return {'timeout', 1205, string.format(
                'bucket %d: prepared transactions still hold locks on its rows', b)}
        end
        M.unlock_cond:wait(0.05)
    end
end

-- S: the rows of bucket b written since tarn_bucket_send_begin returned
-- generation `gen`, a page from the `from`-th noted row: {'ok', rows,
-- next} as tarn_bucket_recv_rows takes them (next: nil at the end), or
-- {'lost'} when the notes are not that generation's (a leader change):
-- the move copies the bucket again.
function tarn_bucket_delta(b, move, gen, from)
    writable()
    BK.fresh()
    local s = BK.state[b]
    if (s ~= 'sending' and s ~= 'fenced') or BK.move[b] ~= move then return BK.unexpected(b, 'delta') end
    local tr = BK.track[b]
    if tr == nil or gen ~= BK.gen then return {'lost'} end
    local i = tonumber(from) or 1
    local out, n, bytes = {}, 0, 0
    box.begin({txn_isolation = 'read-confirmed'})
    local ok, err = pcall(function()
        while i <= #tr.list and not PG.full(n, bytes) do
            local k = tr.list[i]
            local sp = box.space[k[1]]
            local t = sp ~= nil and sp:get(k[2]) or nil
            n = n + 1
            if t ~= nil then
                out[n] = {M.lazy.wire_space(k[1]), t}
                bytes = bytes + t:bsize()
            else
                out[n] = {M.lazy.wire_space(k[1]), box.NULL, k[2]}
            end
            i = i + 1
        end
    end)
    if box.is_in_txn() then box.commit() end
    if not ok then error(err) end
    return {OK, out, i <= #tr.list and i or box.NULL}
end

-- S: send bucket b: delete its rows, in one local transaction (MVCC fails
-- the validation of the streams that read them), and refuse it from now
-- on -- from before the deletes are visible (BK.early).
function tarn_bucket_send(b, move)
    writable()
    yield_point('bucket_send')   -- (tests: a move held after its flip, before its send)
    BK.fresh()
    return BK.locked(function()
        local s = BK.state[b]
        if s == 'sent' and BK.move[b] == move then return {OK, 'sent'} end
        if BK.move[b] ~= move then
            -- Not in this move: it was given up here (see Cluster::flip).
            local r = BK.unexpected(b, 'send')
            r[1] = 'not_moving'
            return r
        end
        if s ~= 'fenced' then return BK.unexpected(b, 'send') end
        if M.lazy.bucket_obligations(b) ~= 0 then return {'timeout', 1205, 'bucket lazy obligations remain'} end
        local peer = BK.peer[b]
        local epoch
        box.begin(RW)
        local ok, err = pcall(function()
            for _, sid in ipairs(BK.spaces()) do
                local sp = box.space[sid]
                local kd = index_key_def(sid, 0)
                local keys = {}
                for _, t in sp.index.bucket_id:pairs({b}, {iterator = 'EQ'}) do keys[#keys + 1] = kd:extract_key(t) end
                for _, k in ipairs(keys) do sp:delete(k) end
            end
            epoch = BK.store(b, 'sent', peer, move, true)
            BK.early(b, 'sent', peer, move, epoch)
        end)
        if not ok then
            box.rollback()
            error(err)
        end
        BK.commit()
        BK.applied(b, 'sent', peer, move, epoch)   -- (and after it: see BK.rebuild)
        M.stats.bucket_sends = M.stats.bucket_sends + 1
        return {OK}
    end)
end

-- R: take bucket b over: insert the rows kept aside, in one local
-- transaction (MVCC fails the validation of the streams whose reads they
-- fall in), own it and release the hold. The epoch changes before the rows
-- are visible, the state once they are (BK.early).
function tarn_bucket_recv(b, move)
    writable()
    yield_point('bucket_recv')   -- (tests: a move held between its send and its receive)
    BK.fresh()
    return BK.locked(function()
        local s = BK.state[b]
        if s == 'active' and BK.move[b] == move then return {OK, 'received'} end
        if s ~= 'held' or BK.move[b] ~= move then return BK.unexpected(b, 'take over') end
        local epoch
        box.begin(RW)
        local ok, err = pcall(function()
            for _, t in box.space._tarn_recv:pairs({b}, {iterator = 'EQ'}) do
                local sp = box.space[t.space_id]
                if sp ~= nil then M.lazy.import(sp, t.tuple) end
            end
            BK.clear_recv(b)
            epoch = BK.store(b, 'active', nil, move, true)
            BK.early(b, nil, nil, nil, epoch)
        end)
        if not ok then
            box.rollback()
            log.error('tarn: taking bucket %d over failed: %s', b, tostring(err))
            error(err)
        end
        BK.commit()
        BK.applied(b, 'active', nil, move, epoch)
        M.stats.bucket_recvs = M.stats.bucket_recvs + 1
        return {OK}
    end)
end

-- S: give the send of bucket b up (before tarn_bucket_send).
function tarn_bucket_send_abort(b, move)
    writable()
    BK.fresh()
    return BK.locked(function()
        local s = BK.state[b]
        if BK.move[b] ~= move then return {OK} end   -- (not this move's)
        if s == 'sent' then return {'error', 0, string.format('bucket %d was sent already', b)} end
        if s == 'sending' or s == 'fenced' then BK.set(b, 'active', nil, nil) end
        return {OK}
    end)
end

-- R: give the receive of bucket b up (before tarn_bucket_recv).
function tarn_bucket_recv_abort(b, move)
    writable()
    BK.fresh()
    return BK.locked(function()
        local s = BK.state[b]
        if BK.move[b] ~= move then return {OK} end
        if s == 'active' then return {'error', 0, string.format('bucket %d was taken over already', b)} end
        if s == 'receiving' or s == 'held' then
            box.begin(RW)
            BK.clear_recv(b)
            BK.store(b, nil)
            BK.commit()
            BK.applied(b, nil, nil, nil)
        end
        return {OK}
    end)
end

-- Membership (see BK.load_members) ------------------------------------------

-- {'ok', ver, shards}: this shard's record of the cluster's shards.
function tarn_members()
    writable()
    BK.fresh()
    return {OK, BK.members.ver or 0, BK.members.shards or {}}
end

-- Record membership `m` ({ver, shards}) if it is newer than this shard's:
-- {'ok', ver} with the version this shard has now. Run on every shard by
-- the SQL server that adds or removes a shard (a job).
function tarn_members_set(m)
    writable()
    yield_point('members_set')   -- (tests: a join held while it records the membership)
    BK.fresh()
    return BK.locked(function()
        local cur = box.space._tarn_meta:get{'members'}
        local v = cur ~= nil and cur.value.ver or 0
        if (m.ver or 0) <= v then return {OK, v} end
        box.begin(RW)
        box.space._tarn_meta:replace{'members', {ver = m.ver, shards = m.shards}}
        BK.commit()
        BK.load_members()
        BK.ver = BK.ver + 1
        return {OK, m.ver}
    end)
end

-- Whether shard `id` may still be needed to finish a transaction here:
-- {'ok', n} with n the PREPAREs in progress that name it as their decision
-- shard and the participants prepared with it as theirs, and on shard `id`
-- itself its decision records in progress. A shard leaves the cluster once
-- every shard reports 0 after it began to leave (see BK.prep).
function tarn_shard_records(id)
    writable()
    local n = BK.prep[id] or 0
    for _, p in box.space._tarn_part.index.state:pairs({'prepared'}, {iterator = 'EQ'}) do
        if p.dshard == id then n = n + 1 end
    end
    if id == M.cfg.shard_id then
        n = n + M.lazy.obligations()
        n = n + box.space._tarn_txn.index.status:count({'staging'})
    end
    return {OK, n}
end

-- Rebalance (issue #25) -------------------------------------------------------

-- The load of the buckets this shard owns (see BK.load): {'ok', secs,
-- {{bucket, reads, writes, bytes written}, ...}} over the last `secs`
-- seconds.
function tarn_bucket_stats()
    writable()
    BK.fresh()
    BK.load_rotate()
    local L, out = BK.load, {}
    local c, p = L.cur, L.prev
    for b = 1, L.n do
        local st = BK.state[b]
        if st == 'active' or st == 'sending' or st == 'fenced' then
            local r, w, by = c.r[b], c.w[b], c.b[b]
            if p ~= nil then r, w, by = r + p.r[b], w + p.w[b], by + p.b[b] end
            if r > 0 or w > 0 then out[#out + 1] = {b, r, w, by} end
        end
    end
    return {OK, L.prev_secs + (fiber.clock() - L.t0), out}
end

-- The size of each bucket this shard owns that has rows: {'ok', {{bucket,
-- rows, bytes}, ...}}, from a scan of the bucket_id index of every table
-- (it yields). The rebalancer reads it once, and again only after a while:
-- a bucket's size moves with it.
function BK.owns(st) return st == 'active' or st == 'sending' or st == 'fenced' end

function tarn_bucket_sizes()
    writable()
    BK.fresh()
    local sizes, n = {}, 0
    for _, sid in ipairs(BK.spaces()) do
        local sp = box.space[sid]
        local idx = sp ~= nil and sp.index.bucket_id
        if idx then
            local f = idx.parts[1].fieldno
            for _, t in idx:pairs() do
                local x = sizes[t[f]]
                if x == nil then
                    x = {0, 0}
                    sizes[t[f]] = x
                end
                x[1], x[2] = x[1] + 1, x[2] + t:bsize()
                n = n + 1
                if n % 1000 == 0 then fiber.yield() end
            end
        end
    end
    BK.fresh()
    local out = {}
    for b, x in pairs(sizes) do
        if BK.owns(BK.state[b]) then out[#out + 1] = {b, x[1], x[2]} end
    end
    table.sort(out, function(a, b) return a[1] < b[1] end)
    return {OK, out}
end

-- The rebalancer's record, on the first shard ('rebalance' in _tarn_meta;
-- the SQL servers keep its fields; `at` is when it was last written, by
-- this shard's clock): {'ok', record, age in seconds} or {'ok', nil}.
function tarn_rebalance_get()
    writable()
    local t = box.space._tarn_meta:get{'rebalance'}
    if t == nil then return {OK, box.NULL} end
    return {OK, t.value, clock.realtime() - (t.value.at or 0)}
end

-- Replace the record with `rec` if it is the one of owner `owner` at
-- version `ver` (as read), or, with owner nil, if there is none or it has
-- ended (status 'done', 'cancelled' or 'failed'): {'ok', new version}, or
-- {'busy', record}. Its owner, TARN REBALANCE PAUSE, RESUME and CANCEL, and
-- a server taking it over each read it, change it and write it back: the
-- version makes the one that wrote second read it again.
function tarn_rebalance_put(owner, ver, rec)
    writable()
    box.begin(RW)
    local t = box.space._tarn_meta:get{'rebalance'}
    local cur = t ~= nil and t.value or nil
    local mine
    if owner == nil or owner == box.NULL then
        mine = cur == nil or cur.status == 'done' or cur.status == 'cancelled' or cur.status == 'failed'
    else
        mine = cur ~= nil and cur.owner == owner and (cur.ver or 0) == ver
    end
    if not mine then
        box.rollback()
        return {'busy', cur or box.NULL}
    end
    rec.ver = (cur ~= nil and cur.ver or 0) + 1
    rec.at = clock.realtime()
    box.space._tarn_meta:replace{'rebalance', rec}
    box.commit()
    return {OK, rec.ver}
end

-- The definitions of this shard's tables, for a shard that joins: per table
-- its catalog entry and its space as it is (id, engine, format, indexes),
-- and the same of its global index spaces.
function tarn_schema_export()
    writable()
    local function space_of(sp)
        local idx = {}
        for id, x in pairs(sp.index) do
            if type(id) == 'number' then
                local parts = {}
                for _, pt in ipairs(x.parts) do
                    parts[#parts + 1] = {field = pt.fieldno, type = pt.type, is_nullable = pt.is_nullable,
                                         collation = pt.collation, path = pt.path}
                end
                idx[#idx + 1] = {id = x.id, name = x.name, unique = x.unique, parts = parts}
            end
        end
        -- (the primary index first)
        table.sort(idx, function(a, b) return a.id < b.id end)
        return {id = sp.id, name = sp.name, engine = sp.engine, format = sp:format(), indexes = idx}
    end
    BK.fresh()
    local version, membership = box.info.schema_version, BK.mver
    box.begin{txn_isolation = 'read-confirmed'}
    local ok, out = pcall(function()
        local tables = {}
        for _, t in box.space._tarn_catalog:pairs() do
            local sp = box.space[t.name]
            if sp ~= nil then
                local uix = {}
                for _, u in ipairs(UIX.spaces(sp.id)) do uix[#uix + 1] = space_of(u) end
                tables[#tables + 1] = {name = t.name, def = t.def, space = space_of(sp), uix = uix}
            end
        end
        local meta = M.lazy.export_meta(tables, version)
        if meta ~= nil then table.insert(tables, 1, meta) end
        return tables
    end)
    if box.is_in_txn() then box.rollback() end
    if not ok then return failure(out) end
    writable()
    if version ~= box.info.schema_version or BK.stale or membership ~= BK.mver then
        return {'error', 1412, 'Schema or membership changed during export'}
    end
    return {OK, out}
end

-- Make the tables of `export` (tarn_schema_export of another shard) here,
-- with the same space ids, formats and indexes: {'ok', n made}. Run again (a
-- join that stopped is run again), it makes what is missing. Refused when
-- this shard has other tables, or rows in these: it is no new shard.
function tarn_schema_import(export, token)
    writable()
    local valid, tables, lazy_meta = pcall(M.lazy.import_envelope, export, token)
    if not valid then return failure(tables) end
    export = tables
    BK.fresh()
    local membership = BK.mver
    yield_point('schema_import')   -- (tests: a join held before the shard has the tables)
    local want = {}
    for _, t in ipairs(export) do want[t.name] = true end
    for _, c in box.space._tarn_catalog:pairs() do
        if not want[c.name] then
            return {'error', 1210, string.format('the shard has table %s, which the cluster has not', c.name)}
        end
    end
    local function refused(d)
        local sp = box.space[d.name]
        if sp == nil then return nil end
        if sp.id ~= d.id then
            return string.format('the shard has space %s with id %d, not %d', d.name, sp.id, d.id)
        end
        if sp.index[0] ~= nil and sp.index[0]:min() ~= nil then
            return string.format('the shard has rows in %s', d.name)
        end
        return nil
    end
    for _, t in ipairs(export) do
        local e = refused(t.space)
        for _, u in ipairs(t.uix or {}) do e = e or refused(u) end
        if e ~= nil then return {'error', 1210, e} end
    end
    local n = 0
    local function make(d)
        if box.space[d.name] == nil then n = n + 1 end
        local sp = box.schema.space.create(d.name, {id = d.id, engine = d.engine, format = d.format,
                                                    is_sync = M.cfg.sync, if_not_exists = true})
        for _, x in ipairs(d.indexes) do
            sp:create_index(x.name, {id = x.id, type = 'tree', unique = x.unique, parts = x.parts,
                                     if_not_exists = true})
        end
    end
    for _, t in ipairs(export) do
        make(t.space)
        for _, u in ipairs(t.uix or {}) do make(u) end
        M.lazy.import_active(t.def)
    end
    M.lazy.import_retired(lazy_meta)
    if BK.stale or BK.mver ~= membership then return {'error', 1412, 'Membership changed during schema import'} end
    invalidate_key_defs()
    M.stale_fences()
    return {OK, n}
end

function tarn_info()
    BK.fresh()
    local ptx = 0   -- (transactions with row-lock state here)
    for _ in pairs(PTX) do ptx = ptx + 1 end
    local nb = 0
    for _, st in pairs(BK.state) do
        if st == 'active' or st == 'sending' or st == 'fenced' then nb = nb + 1 end
    end
    return {shard_id = M.cfg.shard_id, ro = box.info.ro, stats = M.stats, buckets = nb, bucket_epoch = BK.epoch,
            locks = box.space._tarn_locks:len(), lock_index = NLOCK, row_locks = PL_N, plock_txns = ptx,
            ddl_held = DDL_N, prepared = box.space._tarn_part.index.state:count({'prepared'}),
            part_records = box.space._tarn_part:len(), txn_records = box.space._tarn_txn:len(),
            schema_version = box.info.schema_version or 0, vinyl = M.vinyl.stats(),
            native = BK.ns ~= nil and {ready = BK.ns.ready, served = tonumber(BK.ns.served),
                                       to_lua = tonumber(BK.ns.to_lua), writes = tonumber(BK.ns.w_served),
                                       writes_to_lua = tonumber(BK.ns.w_to_lua),
                                       commits = tonumber(BK.ns.c_served),
                                       commits_to_lua = tonumber(BK.ns.c_to_lua)} or false}
end

-------------------------------------------------------------------------------

-- Every replica configured for this shard has joined and follows this node.
local function replicas_follow()
    local cfg = box.cfg.replication
    local want = type(cfg) == 'table' and #cfg - 1 or 0   -- the list names this node too
    local n = 0
    for id, r in pairs(box.info.replication) do
        if id ~= box.info.id and r.downstream ~= nil and r.downstream.status == 'follow' then
            n = n + 1
        end
    end
    return n >= want
end

-- Users and spaces, created by the writable node; replicas get them by
-- replication. Every step is idempotent and writes nothing when its object
-- exists.
local function setup_schema()
    box.schema.user.create(M.cfg.user, {password = M.cfg.password, if_not_exists = true})
    box.schema.user.grant(M.cfg.user, 'super', nil, nil, {if_not_exists = true})
    ensure_schema()
    if M.cfg.native then
        for _, f in ipairs({'tarn_storage.ts_state', 'tarn_storage.ts_select', 'tarn_storage.ts_autocommit',
                            'tarn_storage.ts_commit_local'}) do
            box.schema.func.create(f, {language = 'C', if_not_exists = true})
        end
    end
    if box.space._tarn_meta:get{'shard_id'} == nil then
        -- The new shard's first synchronous write. Made while the replicas
        -- are still joining, it cannot reach its quorum (they cannot
        -- acknowledge it yet): it times out after replication_synchro_timeout,
        -- and so does the first checkpoint of a joining replica that
        -- received it, which waits for its confirmation and stops the
        -- replica when it fails (Tarantool 3.9). So first let the replicas
        -- join (for up to a minute).
        local deadline = clock.monotonic() + 60
        while M.cfg.sync and not replicas_follow() and clock.monotonic() < deadline do
            fiber.sleep(0.1)
        end
        box.space._tarn_meta:replace{'shard_id', M.cfg.shard_id}
    end
    migrate_legacy_job()
    BK.ns_init()
end

-- Run setup_schema in the background whenever this node is writable, until
-- it succeeds: a leadership change, or a write that times out waiting for
-- its quorum while the shard bootstraps, must neither stop the process (an
-- error in init would) nor leave the schema half made (the next leader
-- finishes it).
local setup_fiber
local function setup_when_writable()
    if box.info.ro or setup_fiber ~= nil then return end
    setup_fiber = fiber.create(function()
        fiber.name('tarn_schema')
        while not box.info.ro do
            local ok, err = pcall(setup_schema)
            if ok then break end
            log.warn('tarn: creating the schema: %s; trying again', tostring(err))
            fiber.sleep(1)
        end
        setup_fiber = nil
    end)
end

function M.init(cfg)
    M.lazy.quota_invalidate()
    M.vinyl.reset()
    for k, v in pairs(cfg or {}) do M.cfg[k] = v end
    if not M.cfg.sync and not box.info.ro then
        setup_schema()          -- a single instance: before it serves requests
    else
        setup_when_writable()   -- a replicated shard: see above
    end
    box.space._space:on_replace(function() invalidate_key_defs() end)
    -- The lock index is maintained only while this instance is the leader;
    -- any election state change makes it stale (rebuilt before next use).
    box.ctl.on_election(function()
        LK_STALE = true; FENCE_STALE = true
        M.vinyl.reset()
        M.lazy.invalidate()
        M.lazy.quota_invalidate()
        pl_fresh(); BK.reset()
    end)
    box.watch('box.status', function()   -- read-only changes
        LK_STALE = true; FENCE_STALE = true
        M.vinyl.reset()
        M.lazy.invalidate()
        M.lazy.quota_invalidate()
        pl_fresh(); BK.reset()
        setup_when_writable()
    end)
    box.space._index:on_replace(function() invalidate_key_defs() end)
    box.session.on_disconnect(pl_on_disconnect)
    M.start_recovery()
    M.lazy.start()
    PG.watch_heap()
    rawset(_G, 'tarn', M)
    return M
end

M.vinyl = require('tarn_vinyl').new{cfg = M.cfg, key_def = index_key_def}
M.lazy = require('tarn_lazy').new{
    cfg = M.cfg, bk = BK, rw = RW, failure = failure, writable = writable,
    inflight_begin = inflight_begin, inflight_writes = inflight_writes, inflight_end = inflight_end,
    atomic = atomic, hook = yield_point, exclude = BK.ns_lazy, call = on_shard,
    quota_mark = function()
        BK.quota_cut_valid = false
        BK.quota_lua_cutoff = INFLIGHT_SEQ
        BK.quota_native_cutoff = BK.ns ~= nil and BK.ns.commit_seq or nil
        BK.quota_cut_valid = true
    end,
    quota_cut = function()
        if not BK.quota_cut_valid then return false end
        for seq in pairs(INFLIGHT) do if seq <= BK.quota_lua_cutoff then return false end end
        local mark = BK.quota_native_cutoff
        return mark == nil or BK.ns == nil or BK.ns.oldest_inflight > mark
    end,
}
function tarn_lazy_bootstrap(name, a, gen)
    a.ver = a.ver or a.def.ver or 0
    return UIX.alter_serial(M.lazy.bootstrap, 'bootstrap', name, a, gen)
end
function tarn_lazy_bootstrap_status(name, a, gen) return M.lazy.bootstrap_status(name, a, gen) end
function tarn_lazy_ready(name, a, gen)
    a.ver = a.ver or a.def.ver or 0
    return UIX.alter_serial(M.lazy.ready, 'ready', name, a, gen)
end
function tarn_lazy_capabilities() return M.lazy.capabilities() end
function tarn_lazy_apply(assignments, route) return M.lazy.apply(assignments, route) end
function tarn_lazy_frontiers(buckets, route) return M.lazy.frontiers(buckets, route) end
function tarn_lazy_select(gen, key, iterator, limit, after, route)
    return M.lazy.select(gen, key, iterator, limit, after, route)
end
function tarn_lazy_gc(gen, bucket, route) return M.lazy.gc(gen, bucket, route) end
function tarn_lazy_stats() return M.lazy.stats() end
function tarn_lazy_control(action, arg) return M.lazy.control(action, arg) end
function tarn_vinyl_stats() return {OK, M.vinyl.stats()} end

return M
