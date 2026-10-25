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
--   _tarn_meta     key -> value                 (shard id, counters)
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
    },
    stats = {commit_local = 0, prepare_yes = 0, prepare_no = 0, apply = 0,
             abort = 0, conflicts = 0, recovered = 0, lock_waits = 0,
             gc_part = 0, gc_txn = 0, validate_changed = 0, protocol_errors = 0},
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
local LK = {}       -- LK[sid][iid][kbin] = {[txid] = 'R' | 'W'}
local RL = {}       -- RL[sid][iid][txid] = {{lo, hi, lo_incl, hi_incl}, ...}
local BYTX = {}     -- BYTX[txid] = {{sid, iid, kbin} | {sid, iid}, ...}
local NLOCK = 0     -- point + range locks currently held
local LK_STALE = true

local function lk_point(txid, sid, iid, kb, mode)
    local s = LK[sid]; if s == nil then s = {}; LK[sid] = s end
    local i = s[iid]; if i == nil then i = {}; s[iid] = i end
    local e = i[kb]; if e == nil then e = {}; i[kb] = e end
    if e[txid] == nil then NLOCK = NLOCK + 1 end
    if e[txid] ~= 'W' then e[txid] = mode end
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
local function write_conflict(txid, space, pk, old, new)
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
        local any_mode = k[3] and n > nold     -- unique key claimed by the new tuple
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
local function failure(e)
    if type(e) == 'cdata' or type(e) == 'table' then
        local code = e.code or 0
        if code == box.error.TRANSACTION_CONFLICT then return conflict(tostring(e.message)) end
        if code == box.error.TUPLE_FOUND then return {'duplicate', 1062, tostring(e.message)} end
        return {'error', code, tostring(e.message or e)}
    end
    return {'error', 0, tostring(e)}
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

local function apply_checked(txid, w)
    local space = box.space[w[2]]
    if space == nil then error(box.error.new(box.error.NO_SUCH_SPACE, tostring(w[2]))) end
    local pk, new = write_target(space, w)
    local old = space:get(pk)
    if w[1] == 'update' then
        if old == nil then return nil end
        new = old:update(mysql_update_ops(old, w[4]))
        -- w[5]: {{field, min, max}} for columns changed by arithmetic
        for _, r in ipairs(w[5] or {}) do
            local v = new[r[1]]
            if v ~= nil and (v < r[2] or v > r[3]) then
                error(box.error.new({code = 1264,
                    reason = "Out of range value for column '" .. tostring(r[1]) .. "' at row 1"}))
            end
        end
        local holder = write_conflict(txid, space, pk, old, new)
        if holder then return nil, holder end
        return space:replace(new), nil, old
    end
    local holder = write_conflict(txid, space, pk, old, new)
    if holder then return nil, holder end
    return apply_write(w), nil, old
end

-- MySQL affected-rows convention for one write: REPLACE of a different row
-- counts 2, an UPDATE or REPLACE that changes nothing counts 0 / 1.
local function affected(op, old, new)
    if op == 'insert' then return 1 end
    if op == 'delete' then return old ~= nil and 1 or 0 end
    local same = old ~= nil and new ~= nil and msgpack.encode(old) == msgpack.encode(new)
    if op == 'replace' then
        if old == nil or same then return 1 end
        return 2
    end
    -- update
    if old == nil or new == nil or same then return 0 end
    return 1
end

-- TarnCommit!CommitLocal: called inside the transaction's IPROTO stream after
-- its reads. Validates the read set against W locks (a W lock may belong to a
-- transaction that is committed but not yet applied here), applies the writes
-- with lock checks, and commits; Tarantool's MVCC validates the reads
-- themselves. Returns {'ok', n} or {status, code, message}; on failure the
-- stream's transaction is rolled back.
function tarn_commit_local(txid, writes, rpoints, rranges)
    writable()
    local ok, res = pcall(function()
        lk_fresh()
        -- Without reads on this shard the coordinator sends no stream: the
        -- commit is then its own local transaction.
        if not box.is_in_txn() then box.begin(RW) end
        local h = reads_w_locked(txid, rpoints, rranges)
        if h then return conflict('read row locked by prepared transaction ' .. h) end
        local n = 0
        for _, w in ipairs(writes) do
            local res, holder = apply_checked(txid, w)
            if holder then return conflict('row locked by prepared transaction ' .. holder) end
            if res ~= nil then n = n + 1 end
        end
        box.commit()
        M.stats.commit_local = M.stats.commit_local + 1
        return {OK, n}
    end)
    if not ok then
        if box.is_in_txn() then box.rollback() end
        reraise_readonly(res)
        return failure(res)
    end
    if res[1] ~= OK and box.is_in_txn() then box.rollback() end
    return res
end

-- Election term at the start of each open stream transaction (by txn id),
-- recorded by the request that begins it (tarn_select with begin).
local STREAM_TERM = {}

local function note_stream_term()
    local id = box.txn_id()
    if id == nil then return end
    STREAM_TERM[id] = box.info.election.term
    local function forget() STREAM_TERM[id] = nil end
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
function tarn_validate(txid, rpoints, rranges, cross)
    writable()
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
    if box.is_in_txn() then box.commit() end
    return {OK}
end

M.unlock_cond = fiber.cond()

local ITER = {[0] = 'EQ', [1] = 'REQ', [2] = 'ALL', [3] = 'LT', [4] = 'LE', [5] = 'GE', [6] = 'GT'}

-- Bounded index scan: rows from `key` in `iterator` order, stopping after
-- `limit` rows or once past `end_key` (inclusive if end_incl). Inside a
-- stream the reads are tracked by MVCC. With lock_check (autocommit reads,
-- TarnCommit!SnapshotRead), the scan waits while another transaction holds a
-- W lock on anything it covers; scan and lock check run without a yield in
-- between, so they observe one state. Returns {'ok', tuples}.
local BEGIN_STREAM = {txn_isolation = 'read-confirmed', timeout = 60}

function tarn_select(space_id, index_id, key, iterator, limit, end_key, end_incl, lock_check, offset, begin)
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
    local it = type(iterator) == 'number' and ITER[iterator] or string.upper(tostring(iterator or 'EQ'))
    key = key or {}
    local kd = index_key_def(space_id, index_id)
    local reverse = (it == 'LT' or it == 'LE' or it == 'REQ')
    local deadline = clock.monotonic() + M.cfg.lock_wait
    while true do
        local rows = {}
        local skip = (offset ~= nil) and offset or 0   -- msgpack nil arrives as box.NULL
        if end_key == nil and (limit == nil or limit > 0) and skip == 0 then
            rows = idx:select(key, {iterator = it, limit = limit})
        else
            for _, t in idx:pairs(key, {iterator = it}) do
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
                end
            end
        end
        if lock_check ~= true or NLOCK == 0 then return {OK, rows} end
        local h
        if (it == 'EQ' or it == 'REQ') and idx.unique and #key == #idx.parts then
            h = point_w_locked('', space_id, index_id, key)
        else
            -- Covered range: from the start key to the end key, or to the last
            -- row returned when the limit cut the scan short.
            local lo, hi = nil, nil
            local first = (#key > 0) and key or nil
            local last = end_key
            if limit ~= nil and #rows >= limit and #rows > 0 then
                last = kd:extract_key(rows[#rows]):totable()
            end
            if it == 'EQ' or it == 'REQ' then lo, hi = first, first
            elseif reverse then lo, hi = last, first
            else lo, hi = first, last end
            h = range_w_locked('', space_id, index_id, lo, hi, true, true)
        end
        if h == nil then return {OK, rows} end
        if clock.monotonic() > deadline then
            return conflict('row locked by prepared transaction ' .. h)
        end
        M.stats.lock_waits = M.stats.lock_waits + 1
        M.unlock_cond:wait(0.01)
    end
end

-- Kept for the protocol tests: autocommit lock-aware select.
function tarn_read(space_id, index_id, key, iterator, limit, offset)
    return tarn_select(space_id, index_id, key, iterator, limit, nil, true, true, offset)
end

-- Autocommit DML outside any stream: one local transaction. Waits (bounded)
-- for conflicting locks of prepared cross-shard txns to clear, then retries.
-- Returns {'ok', affected_rows, last_tuple}.
function tarn_autocommit(writes)
    lk_fresh()   -- raises ER_READONLY before anything is written
    local deadline = clock.monotonic() + M.cfg.lock_wait
    while true do
        local ok, res = pcall(function()
            box.begin(RW)
            local n, last = 0, nil
            for _, w in ipairs(writes) do
                local t, holder, old = apply_checked('', w)
                if holder then box.rollback(); return {'locked', holder} end
                n = n + affected(w[1], old, t)
                if t ~= nil then last = t end
            end
            box.commit()
            return {OK, n, last}
        end)
        if not ok then
            if box.is_in_txn() then box.rollback() end
            reraise_readonly(res)
            return failure(res)
        end
        if res[1] ~= 'locked' then return res end
        if clock.monotonic() > deadline then
            return conflict('row locked by prepared transaction ' .. res[2])
        end
        M.stats.lock_waits = M.stats.lock_waits + 1
        M.unlock_cond:wait(0.01)
    end
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
function tarn_prepare(txid, dshard, parts, writes, rpoints, rranges, txn_ts)
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
        -- readsOK: no W lock of another prepared txn on anything we read.
        local h = reads_w_locked(txid, rpoints, rranges)
        if h then return vote_no(1213, 'read row locked by ' .. h) end
        -- writesOK, plus uniqueness of the new tuples (tracked reads).
        local plans = {}
        for i, w in ipairs(writes) do
            local space = box.space[w[2]]
            if space == nil then return vote_no(1146, 'no such space ' .. tostring(w[2])) end
            local pk, new = write_target(space, w)
            local old = space:get(pk)
            h = write_conflict(txid, space, pk, old, new)
            if h then return vote_no(1213, 'write row locked by ' .. h) end
            if w[1] == 'insert' and old ~= nil then
                return vote_no(1062, 'Duplicate entry for key PRIMARY')
            end
            local keys = secondary_keys(space, old, {})
            secondary_keys(space, new, keys)
            if new ~= nil then
                local pkd = index_key_def(space.id, 0)
                for _, k in ipairs(keys) do
                    if k[3] then
                        local cur = space.index[k[1]]:get(k[2])
                        if cur ~= nil and pkd:compare_keys(pkd:extract_key(cur):totable(), pk) ~= 0 then
                            return vote_no(1062, 'Duplicate entry for key ' .. space.index[k[1]].name)
                        end
                    end
                end
            end
            plans[i] = {space.id, pk, keys}
        end
        -- recOK: the decision shard must not have a record yet (fenced => abort).
        if dshard == M.cfg.shard_id then
            if box.space._tarn_txn:get{txid} ~= nil then
                return vote_no(1213, 'transaction already resolved by recovery')
            end
            box.space._tarn_txn:insert{txid, 'staging', parts, now(), expiry()}
        end
        -- If this local transaction does not commit, its locks never existed.
        -- Registered before the first index entry: any error from here on
        -- rolls the transaction back (vote_no), which undoes the entries.
        box.on_rollback(function() lk_release(txid) end)
        local L = box.space._tarn_locks
        local wlocked = {}
        for i, w in ipairs(writes) do
            local sid, pk, keys = plans[i][1], plans[i][2], plans[i][3]
            local pkb = kbin(pk)
            wlocked[sid .. ':0:' .. pkb] = true
            lk_point(txid, sid, 0, pkb, 'W')
            L:replace{sid, 0, pkb, txid, 'W', w[1], w[1] ~= 'delete' and w[3] or msgpack.NULL}
            for _, k in ipairs(keys) do
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
        box.commit()   -- MVCC validates every read this stream made; may raise conflict
        M.stats.prepare_yes = M.stats.prepare_yes + 1
        return {'yes'}
    end)
    if not ok then
        if box.is_in_txn() then box.rollback() end
        reraise_readonly(res)
        local f = failure(res)
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
function tarn_apply(txid)
    writable()
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
        for _, l in box.space._tarn_locks.index.txid:pairs({txid}, {iterator = 'EQ'}) do
            if l.mode == 'W' and l.op ~= nil then
                local space = box.space[l.space_id]
                if l.op == 'delete' then space:delete(msgpack.decode(l.key))
                else space:replace(l.tuple) end
            end
        end
        release_locks(txid)
        box.space._tarn_part:replace{txid, 'applied', p.dshard, p.parts, now(), expiry()}
        box.commit()
        M.stats.apply = M.stats.apply + 1
        return {OK, 'applied'}
    end)
    if not ok then
        if box.is_in_txn() then box.rollback() end
        reraise_readonly(res)
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
        box.begin()
        for j = i, math.min(i + M.cfg.gc_batch - 1, #ids) do space:delete{ids[j]} end
        box.commit()
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
    if box.info.ro then return end
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
-- DDL and catalog. Tarn broadcasts DDL to every shard.
-------------------------------------------------------------------------------

-- def = {name=, engine=, format={{name=,type=,is_nullable=}}, indexes={{name=,
--        unique=, parts={{field=,type=,is_nullable=,collation=}}}}, ...}
function tarn_create_table(def)
    if box.space[def.name] ~= nil then
        return {'exists'}
    end
    local s = box.schema.space.create(def.name, {
        id = def.id, engine = def.engine or 'memtx', format = def.format,
        is_sync = M.cfg.sync, if_not_exists = true})
    for i, idx in ipairs(def.indexes) do
        s:create_index(idx.name, {type = 'tree', unique = idx.unique, parts = idx.parts,
                                  id = i - 1, if_not_exists = true})
    end
    box.space._tarn_catalog:replace{def.name, def}
    invalidate_key_defs()
    return {OK, s.id}
end

function tarn_drop_table(name)
    if box.space[name] ~= nil then box.space[name]:drop() end
    box.space._tarn_catalog:delete{name}
    invalidate_key_defs()
    return {OK}
end

function tarn_create_index(name, idx)
    local s = box.space[name]
    if s == nil then return {'error', 1146, 'no such table ' .. name} end
    s:create_index(idx.name, {type = 'tree', unique = idx.unique, parts = idx.parts,
                              id = idx.id, if_not_exists = true})
    invalidate_key_defs()
    return {OK}
end

-- Replace the Tarn-side JSON definition stored with a table.
function tarn_set_def_json(name, json)
    local r = box.space._tarn_catalog:get{name}
    if r == nil then return {'error', 1146, 'no such table ' .. name} end
    local copy = table.deepcopy(r.def)
    copy.json = json
    box.space._tarn_catalog:replace{name, copy}
    return {OK}
end

function tarn_truncate(name)
    if box.space[name] ~= nil then box.space[name]:truncate() end
    return {OK}
end

function tarn_catalog()
    local out = {}
    for _, t in box.space._tarn_catalog:pairs() do
        out[#out + 1] = {t.name, box.space[t.name] and box.space[t.name].id or 0, t.def.json or ''}
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
            locks = box.space._tarn_locks:len(), lock_index = NLOCK, prepared = box.space._tarn_part.index.state:count({'prepared'}),
            part_records = box.space._tarn_part:len(), txn_records = box.space._tarn_txn:len(),
            schema_version = box.info.schema_version or 0}
end

-------------------------------------------------------------------------------

function M.init(cfg)
    for k, v in pairs(cfg or {}) do M.cfg[k] = v end
    -- Schema is created on the writable node and reaches replicas by replication.
    if not box.info.ro then
        box.schema.user.create(M.cfg.user, {password = M.cfg.password, if_not_exists = true})
        box.schema.user.grant(M.cfg.user, 'super', nil, nil, {if_not_exists = true})
        ensure_schema()
        box.space._tarn_meta:replace{'shard_id', M.cfg.shard_id}
    end
    box.space._space:on_replace(function() invalidate_key_defs() end)
    -- The lock index is maintained only while this instance is the leader;
    -- any election state change makes it stale (rebuilt before next use).
    box.ctl.on_election(function() LK_STALE = true end)
    box.watch('box.status', function() LK_STALE = true end)   -- read-only changes
    if not box.info.ro then lk_reload() end
    M.start_recovery()
    rawset(_G, 'tarn', M)
    return M
end

return M
