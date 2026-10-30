-- A lazy index under churn (issue #177); run by lazy_soak.sh. Two shards,
-- each the source of eight buckets and the target of eight; a table of 320
-- rows over all sixteen buckets with a lazy index on k (40 values). The
-- shards keep protocol records for about 4 s (TARN_MAX_TXN_AGE=2,
-- TARN_CLOCK_SKEW=1). One writer inserts, rekeys, rewrites and deletes rows
-- with single-shard writes and cross-shard transactions (shard 1 is the
-- decision shard). A coordinator commits most transactions, aborts some
-- and abandons others: after PREPARE on both shards (recovery commits
-- them) or on one (recovery aborts them). Phases:
--   1. churn with checkpoints, over several record lifetimes;
--   2. shard 2 stopped (SIGSTOP) while shard 1's rows change, with shard
--      1's lazy outbox capped at 300 entries: committed and promised
--      entries stay within the cap, single-shard writes beyond it are
--      refused (1041), PREPARE votes NO (1041) instead of YES, and every
--      YES completes;
--   3. shard 2 resumed, then killed (SIGKILL) and restarted from its
--      checkpoint and WAL while transactions are in flight.
-- Once the writes stop, every transaction resolves and the outboxes drain;
-- the index then holds exactly the rows' current keys, garbage collection
-- reclaims every tombstone and protocol record, and floors stay within one
-- per target and source bucket.
local net, fiber, json, fio = require('net.box'), require('fiber'), require('json'), require('fio')
local clock, NULL = require('clock'), require('msgpack').NULL
local opts = {user = 'tarn', password = 'tarn', wait_connected = 10}
local uri = {assert(os.getenv('S1')), assert(os.getenv('S2'))}
local c = {net.connect(uri[1], opts), net.connect(uri[2], opts)}
local work, pid2 = assert(os.getenv('WORK')), assert(os.getenv('S2_PID'))
local engine = os.getenv('ENGINE') or 'memtx'
local SCALE = tonumber(os.getenv('SOAK_SCALE') or '1')   -- phase lengths
math.randomseed(177)

local pass, fail = 0, 0
local function check(name, yes, extra)
    if yes then pass = pass + 1; print('  ok   ' .. name)
    else fail = fail + 1; print('  FAIL ' .. name .. ' ' .. json.encode(extra)) end
end
local function call(i, name, ...) return c[i]:call(name, {...}, {timeout = 10}) end
local function try(i, name, ...) return pcall(call, i, name, ...) end
local function look(i, name, ...) return pcall(c[i].call, c[i], name, {...}, {timeout = 2}) end
local function ok(r) assert(r[1] == 'ok', debug.traceback(json.encode(r), 2)); return r end
local function wait(fn, secs)
    local deadline = fiber.clock() + (secs or 30)
    repeat if fn() then return true end; fiber.sleep(.1) until fiber.clock() >= deadline
    return fn()
end

local sid, gen, name = 63101, 63102, 'test.lazy_soak'
local spec = {gen = gen, base_space_id = sid, local_index_id = 2, route_parts = 1,
              key_fields = {2}, key_types = {'string'}, pk_fields = {1}, pk_native_fields = {1},
              pk_types = {'unsigned'}, bucket_field = 3}
local def = {id = sid, name = name, ver = 0, engine = engine, json = '{}', lazy = {spec},
    format = {{name = 'id', type = 'unsigned'}, {name = 'k', type = 'string', is_nullable = true},
              {name = 'bucket_id', type = 'unsigned'}},
    indexes = {{name = 'primary', unique = true, parts = {{1, 'unsigned'}}},
               {name = 'bucket_id', unique = false, parts = {{3, 'unsigned'}}},
               {name = 'k', unique = false, parts = {{2, 'string', is_nullable = true}}}}}
local a = {def = def, timeout = 5}
for i = 1, 2 do
    ok(call(i, 'tarn_bucket_init', 16, {{(i - 1) * 8 + 1, i * 8}}))
    ok(call(i, 'tarn_create_prepare', name, a, 1))
    repeat local r = ok(call(i, 'tarn_lazy_bootstrap', name, a, 1)); if r[2] then break end until false
    ok(call(i, 'tarn_lazy_ready', name, a, 1)); ok(call(i, 'tarn_create_apply', name, a))
end

local ROWS, KEYS, CAP = 320, 40, 300
local function bucket(id) return id % 16 + 1 end
local function owner(id) return bucket(id) <= 8 and 1 or 2 end
local up = {true, true}  -- shards the writer may call
local want = {}          -- id -> k of the row, nil while there is none
local busy = {}          -- id -> transaction that holds it until resolved
local pending = {}       -- txid -> {parts, mode, plans, writes = {{id, k}}, decided, yes}
local stats = {writes = 0, refused = 0, throttled = 0, tx = 0, committed = 0, aborted = 0,
               recovered_commit = 0, recovered_abort = 0, prepare_no = 0, no = {}, local_yes = 0,
               local_no = 0, apply_retried = 0, yes_aborted = 0, unseen = 0, torn = 0, other = {}}
local function other(what)
    if #stats.other < 8 then stats.other[#stats.other + 1] = what end
end

-- A random change of row id, and the key it leaves (nil: no row).
local function plan(id)
    local cur = want[id]
    if cur == nil then return {'insert', sid, {id, 'k' .. math.random(KEYS), bucket(id)}} end
    local roll = math.random()
    if roll < .5 then return {'replace', sid, {id, 'k' .. math.random(KEYS), bucket(id)}}
    elseif roll < .8 then return {'delete', sid, {id}}
    else return {'replace', sid, {id, cur, bucket(id)}} end
end
local function result(w) if w[1] ~= 'delete' then return w[3][2] end end
local function pick(shard)
    for _ = 1, 20 do
        local id = math.random(ROWS)
        if owner(id) == shard and busy[id] == nil then return id end
    end
end
-- A refusal with a definite outcome: the write did not happen.
local function refused(r)
    if r[2] == 1041 then stats.refused = stats.refused + 1
    elseif r[2] == 1205 or r[2] == 1213 then stats.throttled = stats.throttled + 1
    else return false end
    return true
end

local function autocommit(shard)
    local id = pick(shard)
    if id == nil then return end
    local w = plan(id)
    local done, r = try(shard, 'tarn_autocommit', {w}, {fence = {sid, 0}, wb = {bucket(id)}})
    if not done then other({'autocommit', tostring(r)})
    elseif r[1] == 'ok' then stats.writes = stats.writes + 1; want[id] = result(w)
    elseif not refused(r) then other(r) end
end

local function resolve(txid, committed)
    local t = pending[txid]
    for _, w in ipairs(t.writes) do
        if committed then want[w[1]] = w[2] end
        busy[w[1]] = nil
    end
    if committed then
        stats.committed = stats.committed + 1
        if t.mode == 'abandon' then stats.recovered_commit = stats.recovered_commit + 1 end
    else
        stats.aborted = stats.aborted + 1
        if t.mode == 'half' then stats.recovered_abort = stats.recovered_abort + 1 end
        if t.mode == 'single' and t.yes then stats.yes_aborted = stats.yes_aborted + 1 end
    end
    pending[txid] = nil
end
local seq = 0
local function begin(mode, parts, ids)
    seq = seq + 1
    local txid, t = 'soak-' .. seq, {parts = parts, mode = mode, writes = {}, plans = {}}
    for i, id in ipairs(ids) do
        t.plans[i] = plan(id)
        t.writes[i] = {id, result(t.plans[i])}
        busy[id] = txid
    end
    pending[txid] = t
    stats.tx = stats.tx + 1
    return txid, t
end
-- PREPARE the i-th write of t on its owner. Answers true (YES), false (a
-- definite NO: the transaction aborts) or nil (unknown: recovery decides).
local function prepare(txid, t, i)
    local id = t.writes[i][1]
    local done, r = try(owner(id), 'tarn_prepare', txid, 1, t.parts, {t.plans[i]}, {}, {},
                        clock.realtime(), NULL, {sid, 0}, {bucket(id)})
    if not done then other({'prepare', tostring(r)}); return nil end
    if r[1] == 'yes' then return true end
    if r[1] ~= 'no' and r[1] ~= 'clock_unknown' then other(r) end
    if r[2] == 1041 then stats.prepare_no = stats.prepare_no + 1 end
    local why = r[1] .. ':' .. tostring(r[2])
    stats.no[why] = (stats.no[why] or 0) + 1
    return false
end
local function decide(txid, t, status)
    local done, st = try(1, 'tarn_txn_cas', txid, 'staging', status, t.parts)
    if not done then return end
    if st == 'committed' or st == 'aborted' then t.decided = st end
    for _, p in ipairs(t.parts) do
        if st == 'committed' then try(p, 'tarn_apply', txid)
        elseif st == 'aborted' then try(p, 'tarn_abort', txid, 1, t.parts) end
    end
end

-- One cross-shard transaction: a row of each shard.
local function cross()
    local x, y = pick(1), pick(2)
    if x == nil or y == nil then return end
    local roll = math.random()
    local mode = roll < .6 and 'commit' or roll < .8 and 'abandon' or roll < .9 and 'half' or 'abort'
    local txid, t = begin(mode, {1, 2}, {x, y})
    if mode == 'half' and math.random() < .5 then
        -- Only shard 2 prepares: recovery fences the record and aborts.
        if prepare(txid, t, 2) == false then resolve(txid, false) end
        return
    end
    local yes1 = prepare(txid, t, 1)
    if yes1 == false then resolve(txid, false); return end
    if mode == 'half' then return end   -- (recovery fences shard 2 and aborts)
    local yes2 = prepare(txid, t, 2)
    if mode == 'abandon' then return end
    decide(txid, t, (mode == 'commit' and yes1 and yes2) and 'committed' or 'aborted')
end

-- A transaction of shard 1 alone, committed after YES: the YES reserved
-- its outbox entries, so it completes even with the outbox at its cap.
local function single()
    local x = pick(1)
    if x == nil then return end
    local txid, t = begin('single', {1}, {x})
    local yes = prepare(txid, t, 1)
    if yes == false then
        stats.local_no = stats.local_no + 1
        resolve(txid, false)
        return
    end
    if yes == nil then return end
    t.yes, stats.local_yes = true, stats.local_yes + 1
    local done, st = try(1, 'tarn_txn_cas', txid, 'staging', 'committed', {1})
    if done and st == 'committed' then
        t.decided = st
        local applied, r = try(1, 'tarn_apply', txid)
        -- (otherwise recovery applies it)
        if not applied or r[1] ~= 'ok' then stats.apply_retried = stats.apply_retried + 1 end
    end
end

-- The outcome of a transaction whose records were collected before the
-- writer saw its decision: its rows, which nothing else wrote since. Every
-- row it changes must agree.
local function settle(txid, t)
    local outcome
    for _, w in ipairs(t.writes) do
        local id, new, old = w[1], w[2], want[w[1]]
        if new ~= old then
            local done, row = pcall(c[owner(id)].eval, c[owner(id)],
                                    'local s, id = ... local t = box.space[s]:get{id} return t and t[2]', {sid, id})
            if not done then return end
            if row == nil then row = nil end   -- (box.NULL)
            local committed = row == new
            if not committed and row ~= old then outcome = 'torn'
            elseif outcome == nil then outcome = committed
            elseif outcome ~= committed then outcome = 'torn' end
        end
    end
    stats.unseen = stats.unseen + 1
    if outcome == 'torn' then
        stats.torn = stats.torn + 1
        other({'torn', txid, t.writes})
        for _, w in ipairs(t.writes) do busy[w[1]] = nil; want[w[1]] = '?' end
        pending[txid] = nil
    else
        resolve(txid, outcome == true)
    end
end

-- Resolves the transactions whose decision is known and applied or
-- aborted on every participant the writer can reach.
local function poll()
    local ids = {}
    for txid in pairs(pending) do ids[#ids + 1] = txid end
    if #ids == 0 then return end
    local done, st = look(1, 'tarn_txn_statuses', ids)
    if not done then return end
    local parts = {}
    for i = 1, 2 do
        if up[i] then
            local got, p = look(i, 'tarn_part_statuses', ids)
            if got then parts[i] = p end
        end
    end
    for k, txid in ipairs(ids) do
        local t, s = pending[txid], st[k]
        if s == 'committed' or s == 'aborted' then t.decided = s end
        local finished = true
        for _, p in ipairs(t.parts) do
            if parts[p] == nil or parts[p][k] == 'prepared' then finished = false end
        end
        if finished and t.decided then resolve(txid, t.decided == 'committed')
        elseif finished and s == 'none' then settle(txid, t) end
    end
end
local function npending() local n = 0; for _ in pairs(pending) do n = n + 1 end; return n end
local function lazy(i)
    local done, r = try(i, 'tarn_lazy_stats')
    if done and r[1] == 'ok' then return r[2] end
end
local function depth(i) local s = lazy(i); return s and s.depth end

-- Writes for `secs` seconds. A checkpoint on each reachable shard every
-- `every` seconds. Answers the largest committed plus promised outbox
-- entries seen on shard 1.
local function churn(secs, every, mix)
    local deadline = fiber.clock() + secs * SCALE
    local next_cp, next_poll = fiber.clock() + (every or 1e9), 0
    local peak, n = 0, 0
    while fiber.clock() < deadline do
        n = n + 1
        local roll = math.random()
        if mix == 'single' then
            if roll < .2 then single() else autocommit(1) end
        elseif up[2] and roll < .04 and npending() < 24 then cross()
        else autocommit((up[2] and roll < .52) and 2 or 1) end
        if mix == 'single' or n % 16 == 0 then
            local s = lazy(1)
            if s then peak = math.max(peak, s.depth + s.reserved_entries) end
        end
        if fiber.clock() >= next_poll then poll(); next_poll = fiber.clock() + .05 end
        if fiber.clock() >= next_cp then
            for i = 1, 2 do
                if up[i] then pcall(function() c[i]:eval('box.snapshot()', {}, {timeout = 30}) end) end
            end
            next_cp = fiber.clock() + every
        end
    end
    return peak
end

print(string.format('churn over both shards, with checkpoints (%s)', engine))
churn(12, 3)
check('churn: writes and transactions go through on both shards',
      stats.writes > 1000 and stats.tx > 50 and #stats.other == 0, stats)

print('shard 2 stopped; shard 1 writes with its outbox capped')
assert(wait(function() local s = lazy(1); return s and s.depth + s.reserved_entries < CAP / 4 end, 30))
for i = 1, 2 do c[i]:eval('tarn.cfg.lazy_outbox_entries = ...', {CAP}) end
up[2] = false
os.execute('kill -STOP ' .. pid2)
local before = {refused = stats.refused, no = stats.prepare_no, yes = stats.local_yes}
local peak = churn(6, nil, 'single')
check('committed and promised outbox entries stay within the cap', peak <= CAP and peak > CAP / 2,
      {peak = peak, cap = CAP})
check('single-shard writes beyond the cap are refused (1041)', stats.refused > before.refused, stats)
check('PREPARE beyond the cap votes NO (1041) instead of YES',
      stats.prepare_no > before.no and stats.local_yes > before.yes and #stats.other == 0, stats)
os.execute('kill -CONT ' .. pid2)
up[2] = true
check('shard 1\'s backlog drains once shard 2 is back', wait(function() return (depth(1) or CAP) < 50 end, 60),
      depth(1))
for i = 1, 2 do c[i]:eval('tarn.cfg.lazy_outbox_entries = 65536') end
churn(5, 2)

print('shard 2 killed and restarted with transactions in flight')
c[2]:eval('box.snapshot()')
churn(2)
local in_flight = npending()
up[2] = false
local f = io.open(work .. '/restart', 'w'); f:close()
churn(3)                     -- (shard 1 alone while shard 2 restarts)
check('shard 2 restarts', wait(function() return fio.path.exists(work .. '/restarted') end, 60))
c[2] = net.connect(uri[2], opts)
assert(c[2]:is_connected(), 'cannot reconnect to shard 2')
up[2] = true
churn(8, 3)
check('churn after the restart: writes and transactions go through', #stats.other == 0,
      {in_flight = in_flight, stats = stats})

print('writes stopped')
check('every transaction resolves', wait(function() poll(); return npending() == 0 end, 60),
      {pending = npending(), stats = stats})
check('recovery committed abandoned transactions and aborted half-prepared ones',
      stats.recovered_commit > 0 and stats.recovered_abort > 0, stats)
check('every transaction is atomic: all its writes or none', stats.torn == 0, stats)
check('every YES at the cap committed', stats.local_yes > 0 and stats.yes_aborted == 0, stats)
check('both outboxes drain', wait(function() return depth(1) == 0 and depth(2) == 0 end, 90),
      {depth(1), depth(2)})
local function markers(i)
    return c[i]:eval([[local g = ... local out = {}
        for _, t in box.space[g].index.ordered:pairs({true}, {iterator = 'EQ'}) do out[#out + 1] = {t.k1, t.p1} end
        return out]], {gen})
end
local seen, extra, dup = {}, {}, {}
for i = 1, 2 do
    for _, m in ipairs(markers(i)) do
        local k, id = m[1], m[2]
        if want[id] ~= k then extra[#extra + 1] = {i, k, id}
        elseif seen[id] then dup[#dup + 1] = id
        else seen[id] = true end
    end
end
local missing, base, present = {}, {}, {}
for id = 1, ROWS do if want[id] ~= nil and not seen[id] then missing[#missing + 1] = {id, want[id]} end end
for i = 1, 2 do
    for _, t in ipairs(c[i]:eval('return box.space[...]:select()', {sid})) do
        present[t[1]] = true
        if want[t[1]] ~= t[2] then base[#base + 1] = {t[1], t[2], want[t[1]]} end
    end
end
for id = 1, ROWS do if want[id] ~= nil and not present[id] then base[#base + 1] = {id, nil, want[id]} end end
local live = 0; for _ in pairs(want) do live = live + 1 end
check('the rows and the index hold exactly the expected keys',
      #extra == 0 and #dup == 0 and #missing == 0 and #base == 0,
      {live = live, extra = extra, dup = dup, missing = missing, base = base})

print('garbage collection')
local function tombstones(i)
    return c[i]:eval([[local g = ... local n = 0
        for _ in box.space[g].index.tombstones:pairs({false}, {iterator = 'EQ'}) do n = n + 1 end return n]], {gen})
end
check('every tombstone is collected', wait(function() return tombstones(1) == 0 and tombstones(2) == 0 end, 90),
      {tombstones(1), tombstones(2)})
local function records(i)
    return c[i]:eval('return box.space._tarn_txn:len() + box.space._tarn_part:len()')
end
check('every protocol record is collected', wait(function() return records(1) == 0 and records(2) == 0 end, 60),
      {records(1), records(2)})
local sizes = {}
for i = 1, 2 do
    sizes[i] = c[i]:eval([[return {floors = box.space._tarn_lazy_floor:len(),
        counters = box.space._tarn_lazy_counter:len(), outbox = box.space._tarn_lazy_outbox:len(),
        reservations = box.space._tarn_lazy_reservation:len(), versions = box.space._tarn_lazy_version:len()}]])
end
check('at most one floor per target and source bucket, and one counter per source bucket',
      sizes[1].floors <= 8 * 16 and sizes[2].floors <= 8 * 16 and sizes[1].counters <= 8 and sizes[2].counters <= 8,
      sizes)
check('no outbox entries, reservations or per-row versions remain',
      sizes[1].outbox + sizes[1].reservations + sizes[1].versions
      + sizes[2].outbox + sizes[2].reservations + sizes[2].versions == 0, sizes)
print(string.format('%d writes; %d transactions (%d committed, %d by recovery; %d aborted, %d by recovery; '
                    .. '%d in flight at the kill); %d writes refused at the cap, %d PREPAREs voted NO there; '
                    .. '%d refused under contention; peak outbox %d of %d; %d live rows; floors %d and %d',
                    stats.writes, stats.tx, stats.committed, stats.recovered_commit, stats.aborted,
                    stats.recovered_abort, in_flight, stats.refused, stats.prepare_no, stats.throttled,
                    peak, CAP, live, sizes[1].floors, sizes[2].floors))
print(string.format('NO votes: %s; %d transactions settled from their rows (records collected before the '
                    .. 'writer saw the decision)', json.encode(stats.no), stats.unseen))
print(string.format('%d passed, %d failed', pass, fail))
os.exit(fail == 0 and 0 or 1)
