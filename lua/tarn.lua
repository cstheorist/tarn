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
        alter_check_rows = 1000000, -- ALTER TABLE refuses to check more existing rows than this per shard
        -- A read returns at most this many rows, or rows of this many bytes,
        -- per call; the SQL server continues it after the last one (issue #77).
        scan_page_rows = 4096,
        scan_page_bytes = 4 * 1024 * 1024,
        check_yields = false,    -- tests: an atomic section that yields is an error
        test_hooks = false,      -- tests: tarn_test_hook may hold yield points
    },
    stats = {commit_local = 0, prepare_yes = 0, prepare_no = 0, apply = 0,
             abort = 0, conflicts = 0, recovered = 0, lock_waits = 0,
             gc_part = 0, gc_txn = 0, validate_changed = 0, protocol_errors = 0,
             plock_waits = 0, deadlocks = 0, lock_timeouts = 0, commit_rc = 0, commit_waits = 0,
             stale_defs = 0, alters = 0, vy_rereads = 0, apply_dropped = 0, alter_conflicts = 0,
             creates = 0, drops = 0, truncates = 0, ddl_doomed = 0, read_oom = 0},
}

local RW = {txn_isolation = 'read-committed'}

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

    local locks = space('_tarn_locks', {
        {'space_id', 'unsigned'}, {'index_id', 'unsigned'}, {'key', 'string'},
        {'txid', 'string'}, {'mode', 'string'},
        {'op', 'string', is_nullable = true}, {'tuple', 'any', is_nullable = true}})
    locks:create_index('pk', {parts = {'space_id', 'index_id', 'key', 'txid'}, if_not_exists = true})
    locks:create_index('txid', {parts = {'txid'}, unique = false, if_not_exists = true})

    local rlocks = space('_tarn_rlocks', {
        {'txid', 'string'}, {'n', 'unsigned'}, {'space_id', 'unsigned'}, {'index_id', 'unsigned'},
        {'lo', 'any', is_nullable = true}, {'hi', 'any', is_nullable = true},
        {'lo_incl', 'boolean'}, {'hi_incl', 'boolean'}})
    rlocks:create_index('pk', {parts = {'txid', 'n'}, if_not_exists = true})
    rlocks:create_index('space', {parts = {'space_id', 'index_id'}, unique = false, if_not_exists = true})

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

local function invalidate_key_defs() kd_cache = {} end

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
local BYTX = {}     -- BYTX[txid] = {{sid, iid, kbin} | {sid, iid}, ...}
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

local function lk_point(txid, sid, iid, kb, mode)
    local s = LK[sid]; if s == nil then s = {}; LK[sid] = s end
    local i = s[iid]; if i == nil then i = {}; s[iid] = i end
    local e = i[kb]; if e == nil then e = {}; i[kb] = e end
    if e[txid] == nil then NLOCK = NLOCK + 1 end
    if e[txid] ~= 'W' then e[txid] = mode end
    if mode == 'W' and VY_SCANS > 0 then lk_log(sid, iid, kb) end
    local b = BYTX[txid]; if b == nil then b = {}; BYTX[txid] = b end
    b[#b + 1] = {sid, iid, kb}
end

local function lk_range(txid, sid, iid, lo, hi, lo_incl, hi_incl)
    local s = RL[sid]; if s == nil then s = {}; RL[sid] = s end
    local i = s[iid]; if i == nil then i = {}; s[iid] = i end
    local r = i[txid]; if r == nil then r = {}; i[txid] = r end
    r[#r + 1] = {lo, hi, lo_incl, hi_incl}
    NLOCK = NLOCK + 1
    local b = BYTX[txid]; if b == nil then b = {}; BYTX[txid] = b end
    b[#b + 1] = {sid, iid}
end

local function lk_release(txid)
    local b = BYTX[txid]
    if b == nil then return end
    BYTX[txid] = nil
    for _, x in ipairs(b) do
        if x[3] ~= nil then
            local i = LK[x[1]] and LK[x[1]][x[2]]
            local e = i and i[x[3]]
            if e ~= nil and e[txid] ~= nil then
                if e[txid] == 'W' and VY_SCANS > 0 then lk_log(x[1], x[2], x[3]) end
                e[txid] = nil
                NLOCK = NLOCK - 1
                if next(e) == nil then i[x[3]] = nil end
            end
        else
            local i = RL[x[1]] and RL[x[1]][x[2]]
            if i ~= nil and i[txid] ~= nil then
                NLOCK = NLOCK - #i[txid]
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
    local seen = {}
    for _, set in ipairs({p1, p2}) do
        for _, l in ipairs(set) do
            local id = l.space_id .. '\0' .. l.index_id .. '\0' .. l.key .. '\0' .. l.txid
            if not seen[id] then
                seen[id] = true
                lk_point(l.txid, l.space_id, l.index_id, l.key, l.mode)
            end
        end
    end
    for _, set in ipairs({r1, r2}) do
        for _, r in ipairs(set) do
            local id = 'r\0' .. r.txid .. '\0' .. r.n
            if not seen[id] then
                seen[id] = true
                lk_range(r.txid, r.space_id, r.index_id, r.lo, r.hi, r.lo_incl, r.hi_incl)
            end
        end
    end
    LK_STALE = false
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
-- A TRUNCATE or DROP applied here also fails the validation of the open
-- stream transactions that read the table before it: memtx MVCC aborts the
-- readers of a space that changes, vinyl only its writers.
-- DOOMED[txn id] = {space_id = true}, set when one is applied.
local DOOMED = {}
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
    FENCE, FENCE_STALE = f, false
    DDL_HELD, DDL_N = held, n
end

-- Rebuild the fences and the holds from storage before their next use (as
-- after a restart or a leader change; tests).
function M.stale_fences() FENCE_STALE = true end

-- Like lk_fresh: in a separate fiber, so that a caller inside a transaction
-- gets no read trackers on _tarn_catalog.
local function fence_fresh()
    if not FENCE_STALE then return end
    local f = fiber.new(fence_rebuild)
    f:set_joinable(true)
    local ok, err = f:join()
    if not ok then error(err) end
end

local STALE_DEF_MSG = 'Table definition has changed, please retry transaction'

local function ddl_hold(sid, label)
    if DDL_HELD[sid] == nil then DDL_N = DDL_N + 1 end
    DDL_HELD[sid] = label
end

local function ddl_release(sid)
    if DDL_HELD[sid] == nil then return end
    DDL_HELD[sid] = nil
    DDL_N = DDL_N - 1
    M.unlock_cond:broadcast()
end

-- The open stream transactions may have read space `sid` before a TRUNCATE
-- or DROP that is applied now: their validation fails if they did.
local function doom_readers(sid)
    for id in pairs(STREAM_TERM) do
        local d = DOOMED[id]
        if d == nil then d = {}; DOOMED[id] = d end
        d[sid] = true
    end
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
    return {'ok'}
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
local function wait_commits(deadline, sid, iid, key)
    local upto = INFLIGHT_SEQ
    while true do
        local busy = false
        for seq, e in pairs(INFLIGHT) do
            if seq <= upto and (sid == nil or writes_key(e, sid, iid, key)) then
                busy = true
                break
            end
        end
        if not busy then return true end
        local left = deadline - clock.monotonic()
        if left <= 0 then return false end
        M.stats.commit_waits = M.stats.commit_waits + 1
        INFLIGHT_COND:wait(math.min(left, 0.05))
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
        pl_txn(txid).keys[sid .. '\0' .. iid .. '\0' .. kb] = {sid, iid, kb}
    end
    if cur ~= 'X' then e.holders[txid] = mode end
end

local function pl_grant_waiters(sid, iid, kb, e)
    local i = 1
    while i <= #e.queue do
        local q = e.queue[i]
        if pl_grantable(e, q, i - 1) then
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
    PL_GEN = uuid.str()
    PL_STATE = st
    M.pl_cond:broadcast()
end
M.pl_fresh = pl_fresh

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
local function pl_acquire(txid, sid, iid, kb, mode, opts, deadline)
    local e = pl_entry(sid, iid, kb, true)
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
--   keys : {{space_id, index_id, key}, ...}   (index 0: primary key; else a unique key value)
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
    if PL_BROKEN[txid] ~= nil then return {'error', 1213, BROKEN_MSG} end
    local timeout = opts.timeout
    if timeout == nil or timeout == box.NULL then timeout = 50 end
    pl_register_session(txid)
    return nil, clock.monotonic() + timeout
end

-- Take one row lock of tarn_lock: false (skipped), true (granted; with
-- opts.read the latest committed tuple, or box.NULL if none), or an error
-- status table (pl_failed).
local function pl_take(txid, sid, iid, key, kb, mode, opts, deadline)
    local r = pl_acquire(txid, sid, iid, kb, mode, opts, deadline)
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
    if box.space[sid] == nil then   -- dropped while it waited
        return {'error', 1146, 'no such space ' .. tostring(sid)}
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
        local r = pl_take(txid, sid, iid, key, kbin(key), mode, opts, deadline)
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
--   ranges : {{space_id, prefix, n, skip_keys}, ...}: the rows whose primary
--            key starts with `prefix` (not empty), leaving out the primary
--            keys `skip_keys` (rows the transaction itself deleted).
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
        local sid, prefix, n, skip_keys = rg[1], rg[2], rg[3], rg[4]
        local space = box.space[sid]
        if space == nil then return pl_failed(txid, {'error', 1146, 'no such space ' .. tostring(sid)}) end
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
                    local t = pl_take(txid, sid, 0, pk, kb, mode, read, deadline)
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
local function check_arith(old, ops, ranges)
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

-- Step 1 of one write (see "Yields"): read the row it replaces, work out the
-- tuple it leaves and apply it in the current transaction; may yield.
-- Returns its plan {space, pk, old, new, res, skip} (indexes P_*): `res` is
-- what the write returned, `skip` marks an update of a missing row (nothing
-- written).

-- (write_step is local to write_steps' block: the main chunk is near Lua's
-- limit of 200 locals)
local write_steps
do
local function write_step(w)
    local space = box.space[w[2]]
    if space == nil then error(box.error.new(box.error.NO_SUCH_SPACE, tostring(w[2]))) end
    local pk, new = write_target(space, w)
    local old = space:get(pk)
    yield_point('write')
    if w[1] ~= 'update' then
        return {space, pk, old, new, apply_write(w), false}
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
    return {space, pk, old, new, space:replace(new), false}
end

-- write_step for every write, in order, except that a write a unique index
-- refuses (TUPLE_FOUND) is tried again after the others: a transaction may
-- move a unique value from one row to another, and the row giving it up may
-- come later. Returns the plans in the order of `writes`.
function write_steps(writes)
    local plans, todo = {}, {}
    for i = 1, #writes do todo[#todo + 1] = i end
    while true do
        local again, err = {}, nil
        for _, i in ipairs(todo) do
            local ok, p = pcall(write_step, writes[i])
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
                sp:delete(pk)
            end
            for _, i in ipairs(again) do
                plans[i] = write_step(writes[i])
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
    return not LK_STALE and not FENCE_STALE
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
local function commit_local_checks(txid, lock_gen, fence, rpoints, rranges, plans)
    if not still_fresh() then return conflict(FRESH_MSG) end
    -- Rows the transaction locked (FOR UPDATE / FOR SHARE) were read under
    -- those locks, outside its stream: the locks must have survived.
    if not pl_intact(txid, lock_gen) then return {'error', 1213, BROKEN_MSG} end
    local stale = stale_definition(fence)
    if stale then return stale end
    local h = reads_w_locked(txid, rpoints, rranges)
    if h then return conflict('read row locked by prepared transaction ' .. h) end
    h = reads_ddl_conflict(rpoints, rranges)
    if h then return conflict(h) end
    h = writes_locked(txid, plans)
    if h then return conflict('row locked by prepared transaction ' .. h) end
    return nil
end

function tarn_commit_local(txid, writes, rpoints, rranges, lock_gen, fence)
    writable()
    local seq = inflight_begin()
    local ok, res = pcall(function()
        lk_fresh()
        fence_fresh()
        -- Checked again below, where it counts; here to fail before writing.
        if not pl_intact(txid, lock_gen) then return {'error', 1213, BROKEN_MSG} end
        local stale = stale_definition(fence)
        if stale then return stale end
        -- Without reads on this shard the coordinator sends no stream: the
        -- commit is then its own local transaction.
        if not box.is_in_txn() then box.begin(RW) end
        local plans, n = write_steps(writes), 0
        for i = 1, #plans do
            if plans[i][P_RES] ~= nil then n = n + 1 end
        end
        inflight_writes(seq, plans)
        local r = atomic('CommitLocal', commit_local_checks, txid, lock_gen, fence, rpoints, rranges, plans)
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
        return commit_failure(res)
    end
    if res[1] ~= OK and box.is_in_txn() then box.rollback() end
    return res
end

-- (STREAM_TERM: see the definition fences above.)
local function note_stream_term()
    local id = box.txn_id()
    if id == nil then return end
    STREAM_TERM[id] = box.info.election.term
    local function forget() STREAM_TERM[id] = nil; DOOMED[id] = nil end
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
function tarn_validate(txid, rpoints, rranges, cross, lock_gen)
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
do
    local alloc_ok, alloc = pcall(require, 'internal.alloc')
    local collected = -math.huge
    function PG.heap_low()
        if not alloc_ok then return false end
        local reserve = math.max(alloc.getlimit() / 16, 16 * 1024 * 1024)
        if alloc.unused() >= reserve then return false end
        if clock.monotonic() - collected > 0.1 then
            collectgarbage()   -- (what is in use may be mostly garbage)
            collected = clock.monotonic()
        end
        return alloc.unused() < reserve
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
-- SnapshotRead could have read the same rows. Otherwise the read waits for
-- the lock and runs again (bounded by lock_wait).
local function checked_read(scan, locked)
    local deadline = clock.monotonic() + M.cfg.lock_wait
    while true do
        VY_SCANS = VY_SCANS + 1
        local s0 = LOG_SEQ
        local ok, rows, more = pcall(function()
            box.begin({txn_isolation = 'read-confirmed'})
            return scan()
        end)
        if box.is_in_txn() then box.rollback() end
        VY_SCANS = VY_SCANS - 1
        if not ok then error(rows) end
        local h = atomic('SnapshotRead', function()
            if LK_STALE then return {'stale'} end
            if NLOCK == 0 and LOG_SEQ == s0 and DDL_N == 0 then return nil end
            return locked(rows, s0, more)
        end)
        if h == nil then return {OK, rows, more} end
        if type(h) == 'table' and h[1] == 'stale' then
            lk_fresh()   -- a leader change meanwhile (raises ER_READONLY if demoted)
        elseif type(h) == 'table' then
            -- A cross-shard transaction wrote what the scan covers while it
            -- ran: read again (its locks, if still held, are waited out).
            if clock.monotonic() > deadline then
                return conflict('rows changed by a cross-shard transaction during the read')
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
-- lock_check, offset, begin, after): one page of the scan (see "Pages"),
-- {'ok', tuples[, position]}; with a position, call again with it as
-- `after` for the next page.
function tarn_select(space_id, index_id, key, iterator, limit, end_key, end_incl, lock_check, offset, begin, after)
    -- First request of a stream: start the stream's transaction here (it
    -- stays open after the call because the request carries a stream id).
    if begin == true and not box.is_in_txn() then
        box.begin(BEGIN_STREAM)
        note_stream_term()
    end
    if lock_check == true then
        -- An autocommit read is only linearizable on the current leader.
        lk_fresh()
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

tarn_select = PG.bounded(tarn_select)

-- Rows of several keys of one index in one call (issue #10: IN lists, ORs of
-- equalities, row-value IN). For each key: the row with that key when it is
-- a full key of a unique index, else every row whose index key starts with
-- it (at most `limit` per key). Rows come back key by key. lock_check and
-- begin as in tarn_select. One page at a time (see "Pages"): the reply
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

function tarn_select_keys(space_id, index_id, keys, limit, lock_check, begin, after)
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

tarn_select_keys = PG.bounded(tarn_select_keys)

-- tarn_count: the number of rows a tarn_select scan (`key`, `iterator`,
-- `end_key`, `end_incl`) would return that satisfy every residual predicate
-- of `preds` and whose primary key is not in `exclude` (rows the calling
-- transaction has written, which it counts itself), without returning them
-- (issue #52). A predicate is {field, op, value, fill, int}: the tuple field
-- (1-based; `fill` when the tuple ends before it), 'eq' | 'ne' | 'lt' |
-- 'le' | 'gt' | 'ge' against `value`, 'in' against an array of values, or
-- 'null' | 'notnull'; `int` compares integers exactly, else strings by
-- bytes. A comparison with NULL never holds, as in SQL. An equality prefix
-- or a whole index without predicates is counted by the index. lock_check
-- and begin as in tarn_select, which it mirrors. Returns {'ok', n}.
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

function tarn_count(space_id, index_id, key, iterator, end_key, end_incl, preds, exclude, lock_check, begin)
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
tarn_count = PG.bounded(tarn_count)

-- tarn_estimate: row counts that choose the access path of an ordered read
-- with LIMIT (issue #81). Range A, the equality prefix `a_key` of index
-- `a_index`, would be read whole and sorted; range B, the prefix `b_key` of
-- `b_index`, follows ORDER BY and would be paged until enough rows qualify,
-- about budget/n_a * n_b rows when A's conditions are what B filters on.
-- Returns {'ok', n_a, n_b}: memtx counts both in its index (no scan); vinyl,
-- whose counts scan, gives the index size as n_b (at least B's rows) and
-- counts A only up to the point where paging B is sure to read fewer rows,
-- and not at all when that point lies past one page (cfg.scan_page_rows,
-- issue #77): n_a is then 0, which keeps the plan. Neither is a read of the
-- transaction: the counts only pick between two paths to the same rows.
function tarn_estimate(space_id, a_index, a_key, b_index, b_key, budget)
    local space = box.space[space_id]
    if space == nil then return {'error', 1146, 'no such space ' .. tostring(space_id)} end
    local a, b = space.index[a_index], space.index[b_index]
    if a == nil or b == nil then return {'error', 1412, STALE_DEF_MSG} end
    if space.engine ~= 'vinyl' then
        return {OK, a:count(a_key, {iterator = 'EQ'}), b:count(b_key, {iterator = 'EQ'})}
    end
    local nb = b:len()
    local page = M.cfg.scan_page_rows
    -- (budget * nb > page^2 means cap > page; tested before the product,
    -- which a huge budget would overflow)
    if budget > page * page / math.max(nb, 1) then return {OK, 0, nb} end
    local cap = math.floor(math.sqrt(budget * nb)) + 1
    if cap > page then return {OK, 0, nb} end
    fiber.set_slice(LONG_SCAN_SLICE)   -- see PG.scan
    local na = 0
    for _ in a:pairs(a_key, {iterator = 'EQ'}) do
        na = na + 1
        if na >= cap then break end
    end
    return {OK, na, nb}
end

function tarn_read(space_id, index_id, key, iterator, limit, offset)
    return tarn_select(space_id, index_id, key, iterator, limit, nil, true, true, offset)
end

-- Step 2 of an autocommit write (see "Yields"). 'plocked': a row lock is in
-- the way (wait for it); 'locked': a prepared transaction's lock.
local function autocommit_checks(fence, plans)
    if not still_fresh() then return conflict(FRESH_MSG) end
    local st = stale_definition(fence)
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
function tarn_autocommit(writes, opts)
    lk_fresh()   -- raises ER_READONLY before anything is written
    pl_fresh()
    local fence = opts and opts.fence
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
            local stale = stale_definition(fence)
            if stale then return stale end
            box.begin(RW)
            local plans, n, last = write_steps(writes), 0, nil
            for i = 1, #writes do
                local p = plans[i]
                n = n + affected(writes[i][1], p[P_OLD], p[P_RES], found_rows)
                if p[P_RES] ~= nil then last = p[P_RES] end
            end
            inflight_writes(seq, plans)
            local r = atomic('autocommit', autocommit_checks, fence, plans)
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
local function commit_rc_checks(txid, gen, fence, plans)
    if not still_fresh() then return conflict(FRESH_MSG) end
    if not pl_intact(txid, gen) then return {'error', 1213, BROKEN_MSG} end
    local st = stale_definition(fence)
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
function tarn_commit_rc(txid, writes, gen, fence)
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
            local stale = stale_definition(fence)
            if stale then
                pl_release_all(txid)
                return stale
            end
            seq = inflight_begin()
            box.begin(RW)
            local plans, n = write_steps(writes), 0
            for i = 1, #plans do
                if plans[i][P_RES] ~= nil then n = n + 1 end
            end
            inflight_writes(seq, plans)
            local r = atomic('CommitRC', commit_rc_checks, txid, gen, fence, plans)
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
        return commit_failure(res)
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
local function prepare_locks(txid, dshard, parts, writes, rpoints, rranges, lock_gen, fence, plans)
    if not still_fresh() then return {1213, FRESH_MSG} end
    if not pl_intact(txid, lock_gen) then return {1213, BROKEN_MSG} end
    if stale_definition(fence) then return {1412, STALE_DEF_MSG} end
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
    for i, w in ipairs(writes) do
        local p = plans[i]
        local sid = p[P_SPACE].id
        local pkb = kbin(p[P_PK])
        wlocked[sid .. ':0:' .. pkb] = true
        lk_point(txid, sid, 0, pkb, 'W')
        L:replace{sid, 0, pkb, txid, 'W', w[1], w[1] ~= 'delete' and w[3] or msgpack.NULL}
        for _, k in ipairs(p[P_KEYS]) do
            local kb = kbin(k[2])
            wlocked[sid .. ':' .. k[1] .. ':' .. kb] = true
            lk_point(txid, sid, k[1], kb, 'W')
            L:replace{sid, k[1], kb, txid, 'W', msgpack.NULL, msgpack.NULL}
        end
    end
    for _, r in ipairs(rpoints) do
        local kb = kbin(r[3])
        if not wlocked[r[1] .. ':' .. r[2] .. ':' .. kb] then
            lk_point(txid, r[1], r[2], kb, 'R')
            L:replace{r[1], r[2], kb, txid, 'R', msgpack.NULL, msgpack.NULL}
        end
    end
    for i, r in ipairs(rranges) do
        local lo_incl, hi_incl = r[5] ~= false, r[6] ~= false
        lk_range(txid, r[1], r[2], r[3], r[4], lo_incl, hi_incl)
        box.space._tarn_rlocks:replace{txid, i, r[1], r[2], r[3], r[4], lo_incl, hi_incl}
    end
    box.space._tarn_part:insert{txid, 'prepared', dshard, parts, now(), expiry()}
    return nil
end

function tarn_prepare(txid, dshard, parts, writes, rpoints, rranges, txn_ts, lock_gen, fence)
    writable()
    rpoints, rranges = rpoints or {}, rranges or {}
    local function vote_no(code, msg)
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
        return {'no', code, msg}
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
        -- The checks below run again in the atomic section, where they
        -- count; here they fail the PREPARE before it reads anything.
        -- RC transactions: their row locks must have survived (TarnCommit:
        -- Prepare.readsOK for t \in RC).
        if not pl_intact(txid, lock_gen) then return vote_no(1213, BROKEN_MSG) end
        -- Writes encoded with a table definition an ALTER TABLE replaced.
        if stale_definition(fence) then return vote_no(1412, STALE_DEF_MSG) end
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
                          lock_gen, fence, plans)
        if no ~= nil then return vote_no(no[1], no[2]) end
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
    local ok, res = pcall(function()
        box.begin(RW)
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
                    elseif l.op == 'delete' then space:delete(msgpack.decode(l.key))
                    else
                        local wok, err = pcall(space.replace, space, l.tuple)
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
                if not wok then again[#again + 1] = {x[1], x[2], err} end
            end
            if #again == #later then
                -- Values that go round in a cycle (rows that swap them):
                -- take the rows out, then write them.
                for _, x in ipairs(again) do
                    x[1]:delete(index_key_def(x[1].id, 0):extract_key(box.tuple.new(x[2])))
                end
                for _, x in ipairs(again) do x[1]:replace(x[2]) end
                break
            end
            later = again
        end
        release_locks(txid)
        box.space._tarn_part:replace{txid, 'applied', p.dshard, p.parts, now(), expiry()}
        tracked_commit()
        M.stats.apply = M.stats.apply + 1
        return {OK, 'applied'}
    end)
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

local function shard_conn(shard)
    if shard == M.cfg.shard_id then return nil end  -- local calls
    local c = conns[shard]
    if c ~= nil and c:is_connected() then return c end
    for _, uri in ipairs(M.cfg.peers[shard] or M.cfg.peers[tostring(shard)] or {}) do
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
    if box.space[def.name] ~= nil then
        return {'exists'}
    end
    local s = create_space(def)
    box.space._tarn_catalog:replace{def.name, def}
    invalidate_key_defs()
    return {OK, s.id}
end

function tarn_drop_table(name)
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
-- No stored row changes: ALTER TABLE adds fields after the last one (rows
-- written before hold the column's fill value, which the SQL servers know),
-- keeps the fields of dropped columns, and changes names, types and indexes.
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

local function run_checks(sp, checks)
    if checks == nil or checks == box.NULL or #checks == 0 then return nil end
    local n = 0
    for _, t in sp.index[0]:pairs() do
        n = n + 1
        for _, c in ipairs(checks) do
            if c[3] == 'indexed' and #t < c[1] then
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

-- a: {ver, checks, builds, timeout, drop = {index ids}, renames = {{id, name}},
--     format = new format or nil, rename = new space name or nil, def = catalog entry}
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

function tarn_alter_prepare(name, a)
    writable()
    local sp = alter_space(name, a)
    if sp == nil then return {'error', 1146, "Table '" .. name .. "' doesn't exist"} end
    local cur = catalog_ver(sp.name)
    if cur >= a.ver then return {OK, 'applied'} end
    if a.checks ~= nil and a.checks ~= box.NULL and #a.checks > 0 then
        -- Writes to the table wait while the rows are checked; vinyl's
        -- count is an estimate, which is enough for a limit.
        local rows = sp.index[0]:len()
        if rows > M.cfg.alter_check_rows then
            return {'error', 1846, string.format(
                "Checking the existing rows of '%s' is not supported. Reason: a shard holds about %d rows "
                .. "of it (limit %d, tarn.cfg.alter_check_rows), and writes to the table would wait for "
                .. "the whole scan. Try a change that needs no check, such as a wider type or a "
                .. "nullable column", sp.name, rows, M.cfg.alter_check_rows)}
        end
    end
    fence_fresh()
    if (FENCE[sp.id] or 0) < a.ver then FENCE[sp.id] = a.ver end
    local function undo(res)
        drop_built(sp, a.builds)
        FENCE[sp.id] = cur > 0 and cur or nil
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
    return {OK}
end

function tarn_alter_apply(name, a)
    writable()
    local sp = alter_space(name, a)
    if sp == nil then return {'error', 1146, "Table '" .. name .. "' doesn't exist"} end
    if catalog_ver(sp.name) >= a.ver then return {OK, 'applied'} end
    fence_fresh()
    if (FENCE[sp.id] or 0) < a.ver then
        -- The fence PREPARE raised is gone (restart, new leader): again.
        local r = tarn_alter_prepare(sp.name, a)
        if r[1] ~= OK then return r end
    end
    -- (PREPARE built them; an index dropped since is built again here,
    -- behind the fence.)
    local bad = build_indexes(sp, a.builds)
    if bad then return bad end
    local old_name = sp.name
    for _, id in ipairs(a.drop or {}) do
        if sp.index[id] ~= nil then sp.index[id]:drop() end
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
    box.begin()
    if old_name ~= sp.name then box.space._tarn_catalog:delete{old_name} end
    box.space._tarn_catalog:replace{sp.name, a.def}
    box.commit()
    FENCE[sp.id] = a.ver
    invalidate_key_defs()
    M.stats.alters = M.stats.alters + 1
    return {OK}
end

function tarn_alter_abort(name, a)
    writable()
    local sp = alter_space(name, a)
    if sp == nil then return {OK} end
    local cur = catalog_ver(sp.name)
    if cur >= a.ver then return {OK, 'applied'} end
    drop_built(sp, a.builds)
    fence_fresh()
    FENCE[sp.id] = cur > 0 and cur or nil
    return {OK}
end

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
function tarn_create_prepare(name, a)
    writable()
    local sp = box.space[name]
    if sp ~= nil and sp.id ~= a.def.id then
        return {'error', 1050, "Table '" .. name .. "' already exists"}
    end
    create_space(a.def)   -- (again: whatever an earlier attempt left out)
    return {OK}
end

function tarn_create_apply(name, a)
    writable()
    yield_point('ddl_apply')   -- tests: a shard that fails while it applies
    local sp = box.space[name]
    if sp ~= nil and sp.id ~= a.def.id then
        return {'error', 1050, "Table '" .. name .. "' already exists"}
    end
    create_space(a.def)   -- (if anything is missing)
    if box.space._tarn_catalog:get{name} == nil then
        box.space._tarn_catalog:replace{name, a.def}
        invalidate_key_defs()
        M.stats.creates = M.stats.creates + 1
    end
    return {OK}
end

function tarn_create_abort(name, a)
    writable()
    local sp = box.space[name]
    if sp == nil or sp.id ~= a.def.id then return {OK} end
    if box.space._tarn_catalog:get{name} ~= nil then return {OK, 'applied'} end
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

local function clear_prepare(op, name, a)
    writable()
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
    if (FENCE[sid] or 0) < a.ver then FENCE[sid] = a.ver end
    ddl_hold(sid, label)
    if not drain(sid, clock.monotonic() + (a.timeout or 50)) then
        box.space._tarn_meta:delete{hold_key(sid)}
        local cur = catalog_ver(name)
        FENCE[sid] = cur > 0 and cur or nil
        ddl_release(sid)
        M.stats.lock_timeouts = M.stats.lock_timeouts + 1
        return {'timeout', 1205, LOCK_TIMEOUT_MSG}
    end
    return {OK}
end

local function clear_apply(op, name, a)
    writable()
    yield_point('ddl_apply')   -- tests: a shard that fails while it applies
    local sp = box.space[name]
    if sp ~= nil and sp.id ~= a.id then sp = nil end
    if sp == nil then
        if op == 'TRUNCATE' then return {'error', 1146, string.format(NO_TABLE, name)} end
        -- Dropped by an earlier APPLY: finish what it may have left.
        UIX.drop(a.id)
        box.begin(RW)
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
        UIX.drop(sid)
        sp:drop()
        box.begin(RW)
        box.space._tarn_catalog:delete{name}
        box.space._tarn_meta:delete{hold_key(sid)}
        box.commit()
        -- From here on nothing yields: until the table is released below,
        -- the hold fails the validation of its readers; from then on, the
        -- streams that began before are doomed.
        doom_readers(sid)
        FENCE[sid] = nil
        M.stats.drops = M.stats.drops + 1
    else
        for _, u in ipairs(UIX.spaces(sid)) do u:truncate() end
        sp:truncate()
        box.begin(RW)
        box.space._tarn_catalog:replace{name, a.def}
        box.space._tarn_meta:delete{hold_key(sid)}
        box.commit()
        doom_readers(sid)
        FENCE[sid] = a.ver
        M.stats.truncates = M.stats.truncates + 1
    end
    invalidate_key_defs()
    ddl_release(sid)
    return {OK}
end

local function clear_abort(op, name, a)
    writable()
    local sp = box.space[name]
    if sp ~= nil and sp.id == a.id and op == 'TRUNCATE' and catalog_ver(name) >= a.ver then
        return {OK, 'applied'}
    end
    box.space._tarn_meta:delete{hold_key(a.id)}
    if sp ~= nil and sp.id == a.id then
        fence_fresh()
        local cur = catalog_ver(name)
        FENCE[sp.id] = cur > 0 and cur or nil
    end
    ddl_release(a.id)
    return {OK}
end

function tarn_drop_prepare(name, a) return clear_prepare('DROP', name, a) end
function tarn_drop_apply(name, a) return clear_apply('DROP', name, a) end
function tarn_drop_abort(name, a) return clear_abort('DROP', name, a) end
function tarn_truncate_prepare(name, a) return clear_prepare('TRUNCATE', name, a) end
function tarn_truncate_apply(name, a) return clear_apply('TRUNCATE', name, a) end
function tarn_truncate_abort(name, a) return clear_abort('TRUNCATE', name, a) end

-------------------------------------------------------------------------------
-- Jobs (on the first shard)
-------------------------------------------------------------------------------
-- A job is a CREATE, DROP, TRUNCATE or ALTER TABLE in progress. It has one
-- record per table name it touches ('job:<name>' in _tarn_meta; an ALTER
-- TABLE that renames touches two), so that statements on other tables do
-- not wait for it -- an ALTER TABLE may build an index for hours. Its phase
-- is 'prepare' until every shard has prepared, then 'apply', or 'abort' if
-- one could not ('run': an ALTER TABLE begun by a SQL server from before
-- phases). A SQL server that finds a job abandoned finishes it: back before
-- 'apply', forward after.
--   job = {owner, kind, space, names, ver, a, phase}
local JOB = 'job:'
local JOB_COND = fiber.cond()

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
-- names. {OK}, or {'busy', owner, age} of the job in the way.
function tarn_job_begin(job, wait)
    writable()
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
            if busy == nil then
                local v = {owner = job.owner, ts = clock.realtime(), kind = job.kind, space = job.space,
                           names = job.names, ver = job.ver, a = job.a, phase = job.phase or 'prepare'}
                for _, n in ipairs(job.names) do box.space._tarn_meta:insert{JOB .. n, v} end
            end
            box.commit()
        end)
        if box.is_in_txn() then box.rollback() end
        if ok and busy == nil then return {OK} end
        -- (A repeated call whose first one began the job: see Cluster::call.)
        if busy ~= nil and busy.owner == job.owner then return {OK} end
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

-- Keep the job alive (owner), or take over an abandoned one (owner, from).
function tarn_job_touch(owner, from)
    writable()
    if from ~= nil and from ~= box.NULL then
        local r = job_change(from, {{'=', '[2].owner', owner}, {'=', '[2].ts', clock.realtime()}})
        -- (A repeated call whose first one took it over.)
        if r[1] == 'gone' and #job_keys(owner) > 0 then return {OK} end
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

function tarn_info()
    return {shard_id = M.cfg.shard_id, ro = box.info.ro, stats = M.stats,
            locks = box.space._tarn_locks:len(), lock_index = NLOCK, row_locks = PL_N, ddl_held = DDL_N, prepared = box.space._tarn_part.index.state:count({'prepared'}),
            part_records = box.space._tarn_part:len(), txn_records = box.space._tarn_txn:len(),
            schema_version = box.info.schema_version or 0}
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
    for k, v in pairs(cfg or {}) do M.cfg[k] = v end
    if not M.cfg.sync and not box.info.ro then
        setup_schema()          -- a single instance: before it serves requests
    else
        setup_when_writable()   -- a replicated shard: see above
    end
    box.space._space:on_replace(function() invalidate_key_defs() end)
    -- The lock index is maintained only while this instance is the leader;
    -- any election state change makes it stale (rebuilt before next use).
    box.ctl.on_election(function() LK_STALE = true; FENCE_STALE = true; pl_fresh() end)
    box.watch('box.status', function()   -- read-only changes
        LK_STALE = true; FENCE_STALE = true; pl_fresh()
        setup_when_writable()
    end)
    box.space._index:on_replace(function() invalidate_key_defs() end)
    box.session.on_disconnect(pl_on_disconnect)
    M.start_recovery()
    rawset(_G, 'tarn', M)
    return M
end

return M
