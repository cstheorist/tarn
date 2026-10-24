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
        record_ttl = 30.0,       -- age before decided txn records are GC'd
        tombstone_ttl = 3600.0,  -- age before tombstones are GC'd
        lock_wait = 1.0,         -- max wait for a conflicting lock (autocommit)
        max_txn_age = 1800.0,    -- prepares for older txns are refused (tombstone GC horizon)
    },
    stats = {commit_local = 0, prepare_yes = 0, prepare_no = 0, apply = 0,
             abort = 0, conflicts = 0, recovered = 0, lock_waits = 0},
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

    local part = space('_tarn_part', {
        {'txid', 'string'}, {'state', 'string'}, {'dshard', 'unsigned'},
        {'parts', 'array'}, {'ts', 'double'}})
    part:create_index('pk', {parts = {'txid'}, if_not_exists = true})
    part:create_index('state', {parts = {'state', 'ts'}, unique = false, if_not_exists = true})

    local txn = space('_tarn_txn', {
        {'txid', 'string'}, {'status', 'string'}, {'parts', 'array'}, {'ts', 'double'}})
    txn:create_index('pk', {parts = {'txid'}, if_not_exists = true})
    txn:create_index('status', {parts = {'status', 'ts'}, unique = false, if_not_exists = true})
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

local function range_locked(txid, space_id, index_id, key)
    local kd
    for _, r in box.space._tarn_rlocks.index.space:pairs({space_id, index_id}, {iterator = 'EQ'}) do
        if r.txid ~= txid then
            kd = kd or index_key_def(space_id, index_id)
            if in_range(kd, key, r.lo, r.hi, r.lo_incl, r.hi_incl) then return r.txid end
        end
    end
    return nil
end

-- Conflicts of a write of `space` with primary key `pk`, old tuple `old`
-- (nil if none) and new tuple `new` (nil for delete). Returns holder or nil.
local function write_conflict(txid, space, pk, old, new)
    local L = box.space._tarn_locks.index.pk
    local sid = space.id
    for _, l in L:pairs({sid, 0, kbin(pk)}, {iterator = 'EQ'}) do
        if l.txid ~= txid then return l.txid end
    end
    local h = range_locked(txid, sid, 0, pk)
    if h then return h end
    local keys = secondary_keys(space, old, {})
    local nold = #keys
    secondary_keys(space, new, keys)
    for i, k in ipairs(keys) do
        local any_mode = k[3] and i > nold     -- unique key claimed by the new tuple
        for _, l in L:pairs({sid, k[1], kbin(k[2])}, {iterator = 'EQ'}) do
            if l.txid ~= txid and (any_mode or l.mode == 'R') then return l.txid end
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
    for _, l in box.space._tarn_locks.index.pk:pairs({space_id, index_id, kbin(key)}, {iterator = 'EQ'}) do
        if l.txid ~= txid and l.mode == 'W' then return l.txid end
    end
    return nil
end

local function range_w_locked(txid, space_id, index_id, lo, hi, lo_incl, hi_incl)
    local kd
    for _, l in box.space._tarn_locks.index.pk:pairs({space_id, index_id}, {iterator = 'EQ'}) do
        if l.txid ~= txid and l.mode == 'W' then
            kd = kd or index_key_def(space_id, index_id)
            if in_range(kd, msgpack.decode(l.key), lo, hi, lo_incl, hi_incl) then return l.txid end
        end
    end
    return nil
end

-- TarnCommit NoWLocks: no other prepared txn holds a W lock on anything read.
local function reads_w_locked(txid, points, ranges)
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
local function apply_checked(txid, w)
    local space = box.space[w[2]]
    if space == nil then error(box.error.new(box.error.NO_SUCH_SPACE, tostring(w[2]))) end
    local pk, new = write_target(space, w)
    local old = space:get(pk)
    if w[1] == 'update' then
        if old == nil then return nil end
        new = old:update(w[4])
    end
    local holder = write_conflict(txid, space, pk, old, new)
    if holder then return nil, holder end
    return apply_write(w)
end

-- TarnCommit!CommitLocal: called inside the transaction's IPROTO stream after
-- its reads. Validates the read set against W locks (a W lock may belong to a
-- transaction that is committed but not yet applied here), applies the writes
-- with lock checks, and commits; Tarantool's MVCC validates the reads
-- themselves. Returns {'ok', n} or {status, code, message}; on failure the
-- stream's transaction is rolled back.
function tarn_commit_local(txid, writes, rpoints, rranges)
    local ok, res = pcall(function()
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
        return failure(res)
    end
    if res[1] ~= OK and box.is_in_txn() then box.rollback() end
    return res
end

-- Commit of a read-only single-shard transaction (TarnCommit!SnapshotRead):
-- the lock check runs in the same MVCC view as the reads.
function tarn_validate(txid, rpoints, rranges)
    local h = reads_w_locked(txid, rpoints, rranges)
    if h then
        if box.is_in_txn() then box.rollback() end
        return conflict('read row locked by prepared transaction ' .. h)
    end
    if box.is_in_txn() then box.commit() end
    return {OK}
end

M.unlock_cond = fiber.cond()

-- Autocommit read (TarnCommit!SnapshotRead with check-then-read): a select
-- on any index that waits while another transaction holds a W lock on what
-- the select covers. Lock check and select run without a yield in between,
-- so they observe one state. Returns {'ok', tuples}.
function tarn_read(space_id, index_id, key, iterator, limit, offset)
    local deadline = clock.monotonic() + M.cfg.lock_wait
    local idx = box.space[space_id].index[index_id]
    local unique_point = (iterator == nil or iterator == 'EQ' or iterator == 0)
                         and idx.unique and #key == #idx.parts
    while true do
        local rows = idx:select(key, {iterator = iterator, limit = limit, offset = offset})
        local h
        if box.space._tarn_locks:len() > 0 then
            if unique_point then
                h = point_w_locked('', space_id, index_id, key)
            else
                -- The rows returned cover [first, last]; an unlimited scan covers
                -- everything from the start key in the iterator's direction.
                local kd = index_key_def(space_id, index_id)
                local lo, hi = nil, nil
                local it = string.upper(tostring(iterator or 'EQ'))
                if #rows > 0 and limit ~= nil and #rows >= limit then
                    local a = kd:extract_key(rows[1]):totable()
                    local b = kd:extract_key(rows[#rows]):totable()
                    if it == 'LT' or it == 'LE' or it == 'REQ' then lo, hi = b, a else lo, hi = a, b end
                end
                if it == 'EQ' or it == 'REQ' then
                    lo = lo or key; hi = hi or key
                elseif it == 'GE' or it == 'GT' then
                    lo = lo or (#key > 0 and key or nil)
                elseif it == 'LE' or it == 'LT' then
                    hi = hi or (#key > 0 and key or nil)
                end
                h = range_w_locked('', space_id, index_id, lo, hi, true, true)
            end
        end
        if h == nil then return {OK, rows} end
        if clock.monotonic() > deadline then
            return conflict('row locked by prepared transaction ' .. h)
        end
        M.stats.lock_waits = M.stats.lock_waits + 1
        M.unlock_cond:wait(0.01)
    end
end

-- Autocommit DML outside any stream: one local transaction. Waits (bounded)
-- for conflicting locks of prepared cross-shard txns to clear, then retries.
-- Returns {'ok', affected_rows, last_tuple}.
function tarn_autocommit(writes)
    local deadline = clock.monotonic() + M.cfg.lock_wait
    while true do
        local ok, res = pcall(function()
            box.begin(RW)
            local n, last = 0, nil
            for _, w in ipairs(writes) do
                local t, holder = apply_checked('', w)
                if holder then box.rollback(); return {'locked', holder} end
                if t ~= nil then n = n + 1; last = t end
            end
            box.commit()
            return {OK, n, last}
        end)
        if not ok then
            if box.is_in_txn() then box.rollback() end
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

-- TarnCommit!Prepare. Runs inside the transaction's stream on this shard.
--   txid, dshard, parts : transaction id, decision shard, participant shards
--   writes  : {{op, space_id, tuple_or_key}, ...}  (op: insert/replace/delete)
--   rpoints : {{space_id, index_id, key}, ...}       point reads
--   rranges : {{space_id, index_id, lo, hi, lo_incl, hi_incl}, ...}  scans
--   txn_ts  : coordinator's start time (prepares older than max_txn_age refused)
-- Returns {'yes'} or {'no', code, message}. A NO leaves a tombstone.
function tarn_prepare(txid, dshard, parts, writes, rpoints, rranges, txn_ts)
    rpoints, rranges = rpoints or {}, rranges or {}
    local function vote_no(code, msg)
        if box.is_in_txn() then box.rollback() end
        -- Tombstone: a late duplicate of this PREPARE must also fail.
        pcall(function()
            box.begin(RW)
            if box.space._tarn_part:get{txid} == nil then
                box.space._tarn_part:insert{txid, 'aborted', dshard, parts, now()}
            end
            if dshard == M.cfg.shard_id and box.space._tarn_txn:get{txid} == nil then
                box.space._tarn_txn:insert{txid, 'aborted', parts, now()}
            end
            box.commit()
        end)
        if box.is_in_txn() then box.rollback() end
        M.stats.prepare_no = M.stats.prepare_no + 1
        return {'no', code, msg}
    end
    local ok, res = pcall(function()
        local existing = box.space._tarn_part:get{txid}
        if existing ~= nil then   -- idempotent re-delivery (TarnCommit!Revote)
            box.rollback()
            if existing.state == 'aborted' then return {'no', 1213, 'transaction was aborted'} end
            return {'yes'}
        end
        if txn_ts ~= nil and now() - txn_ts > M.cfg.max_txn_age then
            return vote_no(1213, 'transaction too old')
        end
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
            box.space._tarn_txn:insert{txid, 'staging', parts, now()}
        end
        local L = box.space._tarn_locks
        local wlocked = {}
        for i, w in ipairs(writes) do
            local sid, pk, keys = plans[i][1], plans[i][2], plans[i][3]
            local pkb = kbin(pk)
            wlocked[sid .. ':0:' .. pkb] = true
            L:replace{sid, 0, pkb, txid, 'W', w[1], w[1] ~= 'delete' and w[3] or msgpack.NULL}
            for _, k in ipairs(keys) do
                local kb = kbin(k[2])
                wlocked[sid .. ':' .. k[1] .. ':' .. kb] = true
                L:replace{sid, k[1], kb, txid, 'W', msgpack.NULL, msgpack.NULL}
            end
        end
        for _, r in ipairs(rpoints) do
            local kb = kbin(r[3])
            if not wlocked[r[1] .. ':' .. r[2] .. ':' .. kb] then
                L:replace{r[1], r[2], kb, txid, 'R', msgpack.NULL, msgpack.NULL}
            end
        end
        for i, r in ipairs(rranges) do
            box.space._tarn_rlocks:replace{txid, i, r[1], r[2], r[3], r[4], r[5] ~= false, r[6] ~= false}
        end
        box.space._tarn_part:insert{txid, 'prepared', dshard, parts, now()}
        box.commit()   -- MVCC validates every read this stream made; may raise conflict
        M.stats.prepare_yes = M.stats.prepare_yes + 1
        return {'yes'}
    end)
    if not ok then
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
    box.on_commit(function() M.unlock_cond:broadcast() end)
end

-- TarnCommit!ApplyAt: move intents into the plain spaces, release locks.
-- Only ever called once the transaction is known to be committed.
function tarn_apply(txid)
    local ok, res = pcall(function()
        box.begin(RW)
        local p = box.space._tarn_part:get{txid}
        if p == nil or p.state ~= 'prepared' then
            box.rollback()
            return {OK, p and p.state or 'none'}
        end
        for _, l in box.space._tarn_locks.index.txid:pairs({txid}, {iterator = 'EQ'}) do
            if l.mode == 'W' and l.op ~= nil then
                local space = box.space[l.space_id]
                if l.op == 'delete' then space:delete(msgpack.decode(l.key))
                else space:replace(l.tuple) end
            end
        end
        release_locks(txid)
        box.space._tarn_part:replace{txid, 'applied', p.dshard, p.parts, now()}
        box.commit()
        M.stats.apply = M.stats.apply + 1
        return {OK, 'applied'}
    end)
    if not ok then
        if box.is_in_txn() then box.rollback() end
        return failure(res)
    end
    return res
end

-- TarnCommit!AbortAt: release locks; with no participant record, leave a
-- tombstone so that a PREPARE still in flight is refused.
function tarn_abort(txid, dshard, parts)
    local ok, res = pcall(function()
        box.begin(RW)
        local p = box.space._tarn_part:get{txid}
        if p ~= nil and p.state ~= 'prepared' then
            box.rollback()
            return {OK, p.state}
        end
        release_locks(txid)
        box.space._tarn_part:replace{txid, 'aborted', dshard or (p and p.dshard) or 0,
                                     parts or (p and p.parts) or {}, now()}
        box.commit()
        M.stats.abort = M.stats.abort + 1
        return {OK, 'aborted'}
    end)
    if not ok then
        if box.is_in_txn() then box.rollback() end
        return failure(res)
    end
    return res
end

-- TarnCommit!RecoverFenceParticipant: tombstone if never prepared.
function tarn_fence(txid, dshard, parts)
    local ok, res = pcall(function()
        box.begin(RW)
        local p = box.space._tarn_part:get{txid}
        if p == nil then
            box.space._tarn_part:insert{txid, 'aborted', dshard, parts, now()}
            box.commit()
            return 'aborted'
        end
        box.rollback()
        return p.state
    end)
    if not ok then
        if box.is_in_txn() then box.rollback() end
        return nil, tostring(res)
    end
    return res
end

function tarn_part_status(txid)
    local p = box.space._tarn_part:get{txid}
    return p and p.state or 'none'
end

-- Decision-shard record operations (TarnCommit: rec[t]).
function tarn_txn_status(txid)
    local r = box.space._tarn_txn:get{txid}
    return r and r.status or 'none', r and r.parts or nil
end

-- Compare-and-set of the record status; 'none' -> X creates it.
function tarn_txn_cas(txid, from, to, parts)
    local ok, res = pcall(function()
        box.begin(RW)
        local r = box.space._tarn_txn:get{txid}
        local cur = r and r.status or 'none'
        if cur ~= from then box.rollback(); return cur end
        box.space._tarn_txn:replace{txid, to, parts or (r and r.parts) or {}, now()}
        box.commit()
        return to
    end)
    if not ok then
        if box.is_in_txn() then box.rollback() end
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

local function on_shard(shard, fn, ...)
    local c = shard_conn(shard)
    if c == nil then return _G[fn](...) end
    return c:call(fn, {...}, {timeout = 2})
end

local function resolve(txid, dshard, parts)
    for _ = 1, 4 do
        local status = on_shard(dshard, 'tarn_txn_status', txid)
        if status == 'none' then
            -- RecoverFenceRecord: block D's PREPARE, then the txn is aborted.
            on_shard(dshard, 'tarn_txn_cas', txid, 'none', 'aborted', parts)
        elseif status == 'committed' then
            for _, p in ipairs(parts) do on_shard(p, 'tarn_apply', txid) end
            return 'committed'
        elseif status == 'aborted' then
            for _, p in ipairs(parts) do on_shard(p, 'tarn_abort', txid, dshard, parts) end
            return 'aborted'
        elseif status == 'staging' then
            local all_prepared, any_aborted, missing = true, false, {}
            for _, p in ipairs(parts) do
                local st = on_shard(p, 'tarn_part_status', txid)
                if st == 'aborted' then any_aborted = true; all_prepared = false
                elseif st == 'none' then all_prepared = false; missing[#missing + 1] = p end
            end
            if all_prepared then
                on_shard(dshard, 'tarn_txn_cas', txid, 'staging', 'committed')
            elseif any_aborted then
                on_shard(dshard, 'tarn_txn_cas', txid, 'staging', 'aborted')
            else
                for _, p in ipairs(missing) do on_shard(p, 'tarn_fence', txid, dshard, parts) end
            end
        end
    end
    return 'unresolved'
end
M.resolve = resolve

local function recovery_pass()
    if box.info.ro then return end
    local t = now()
    -- Participants stuck in 'prepared'.
    for _, p in box.space._tarn_part.index.state:pairs({'prepared'}, {iterator = 'EQ'}) do
        if t - p.ts < M.cfg.recovery_after then break end
        local ok, err = pcall(resolve, p.txid, p.dshard, p.parts)
        if ok then M.stats.recovered = M.stats.recovered + 1
        else log.warn('tarn recovery of %s failed: %s', p.txid, tostring(err)) end
    end
    -- Records stuck in 'staging' on this decision shard.
    for _, r in box.space._tarn_txn.index.status:pairs({'staging'}, {iterator = 'EQ'}) do
        if t - r.ts < M.cfg.recovery_after then break end
        local ok, err = pcall(resolve, r.txid, M.cfg.shard_id, r.parts)
        if not ok then log.warn('tarn recovery of %s failed: %s', r.txid, tostring(err)) end
    end
    -- GC decided records once every participant has resolved.
    for _, status in ipairs({'committed', 'aborted'}) do
        local victims = {}
        for _, r in box.space._tarn_txn.index.status:pairs({status}, {iterator = 'EQ'}) do
            if t - r.ts < M.cfg.record_ttl then break end
            local done = true
            for _, p in ipairs(r.parts) do
                local ok, st = pcall(on_shard, p, 'tarn_part_status', r.txid)
                if not ok or st == 'prepared' then done = false; break end
            end
            if done then victims[#victims + 1] = r.txid end
        end
        for _, id in ipairs(victims) do box.space._tarn_txn:delete{id} end
    end
    -- GC participant records: applied ones after record_ttl, tombstones much later.
    for _, st in ipairs({{'applied', M.cfg.record_ttl}, {'aborted', M.cfg.tombstone_ttl}}) do
        local victims = {}
        for _, p in box.space._tarn_part.index.state:pairs({st[1]}, {iterator = 'EQ'}) do
            if t - p.ts < st[2] then break end
            victims[#victims + 1] = p.txid
        end
        for _, id in ipairs(victims) do box.space._tarn_part:delete{id} end
    end
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
        engine = def.engine or 'memtx', format = def.format,
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
    s:create_index(idx.name, {type = 'tree', unique = idx.unique, parts = idx.parts, if_not_exists = true})
    local def = box.space._tarn_catalog:get{name}.def
    local copy = table.deepcopy(def)
    copy.indexes[#copy.indexes + 1] = idx
    box.space._tarn_catalog:replace{name, copy}
    invalidate_key_defs()
    return {OK}
end

function tarn_truncate(name)
    if box.space[name] ~= nil then box.space[name]:truncate() end
    return {OK}
end

function tarn_catalog()
    local out = {}
    for _, t in box.space._tarn_catalog:pairs() do
        out[#out + 1] = {t.name, box.space[t.name] and box.space[t.name].id or 0, t.def}
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

function tarn_info()
    return {shard_id = M.cfg.shard_id, ro = box.info.ro, stats = M.stats,
            locks = box.space._tarn_locks:len(), prepared = box.space._tarn_part.index.state:count({'prepared'}),
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
    M.start_recovery()
    rawset(_G, 'tarn', M)
    return M
end

return M
