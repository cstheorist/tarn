-- Resource and progress accounting for bounded bucket movement.
-- No entry here may retain a bucket's complete rows or keys in the Lua heap.
local ffi, mp, digest = require('ffi'), require('msgpack'), require('digest')
local M = {}
ffi.cdef[[
struct tarn_move_vfs_linux {
 unsigned long bsize, frsize; uint64_t blocks, bfree, bavail, files, ffree, favail;
 unsigned long fsid, flag, namemax; int spare[6];
};
struct tarn_move_vfs_darwin {
 unsigned long bsize, frsize; unsigned int blocks, bfree, bavail, files, ffree, favail;
 unsigned long fsid, flag, namemax;
};
int statvfs(const char *path, void *buf);
]]

local function bad(code, text)
    error(box.error.new{code = code, reason = text})
end
local function internal(name, engine, format, parts, id)
    local s = box.space[name]
    if not s then
        if not id then
            id = box.schema.SYSTEM_ID_MAX + 1
            while id < 10000 and box.space[id] do id = id + 1 end
            assert(id < 10000, 'no free internal movement space ID')
        end
        s = box.schema.space.create(name, {id = id, engine = engine, format = format})
    end
    s:create_index('pk', {parts = parts, if_not_exists = true})
    return s
end

function M.new(c)
    local B = {stats = {batches = 0, max_batch_rows = 0, max_batch_bytes = 0,
        delta_overflows = 0, peak_delta_bytes = 0, peak_staged_bytes = 0}}
    local cfg, bk = c.cfg, c.bk
    local copying, copies = {}, 0
    function B.copy_batch(b, fn)
        if copying[b] or copies >= cfg.move_slots then bad(1041, 'Movement copy buffer budget exhausted') end
        copying[b], copies = true, copies + 1
        local ok, result = pcall(fn)
        copying[b], copies = nil, copies - 1
        if not ok then error(result) end
        return result
    end
    local names = {'move_batch_rows', 'move_batch_bytes', 'move_slots', 'move_staging_bytes',
        'move_total_staging_bytes', 'move_delta_keys', 'move_delta_bytes',
        'move_total_delta_bytes', 'move_disk_reserve_bytes'}
    function B.limits()
        for _, k in ipairs(names) do
            local v = cfg[k]
            if type(v) ~= 'number' or v ~= v or v < 1 or v > 9007199254740991 or v % 1 ~= 0 then
                bad(1210, 'Invalid movement limit: ' .. k)
            end
        end
        if cfg.move_slots > 64 or cfg.move_batch_rows > 4096 or cfg.move_batch_bytes > 4 * 1024 * 1024
           or cfg.move_delta_keys > 65536 or cfg.move_delta_bytes > cfg.move_total_delta_bytes
           or cfg.move_staging_bytes > cfg.move_total_staging_bytes then
            bad(1210, 'Movement limits exceed supported bounds')
        end
        return math.min(cfg.move_batch_rows, cfg.scan_page_rows), math.min(cfg.move_batch_bytes, cfg.scan_page_bytes)
    end
    function B.schema()
        B.limits()
        assert(require('tarn_compat').current() == 2, 'format marker must precede movement schema')
        local recv = box.space._tarn_recv
        local id = recv and recv.id
        if recv and recv.engine ~= 'vinyl' then
            if recv.index[0]:min() ~= nil then
                error('drain legacy bucket moves before migrating memtx staging')
            end
            for _, b in box.space._tarn_buckets:pairs() do
                if b.state ~= 'active' and b.state ~= 'sent' then
                    error('drain legacy bucket moves before migrating memtx staging')
                end
            end
            recv:drop()
        end
        recv = internal('_tarn_recv', 'vinyl',
            {{'bucket','unsigned'}, {'space_id','unsigned'}, {'key','string'}, {'tuple','array'}},
            {'bucket','space_id','key'}, id)
        local progress = internal('_tarn_move', 'memtx',
            {{'bucket','unsigned'}, {'move','string'}, {'side','string'}, {'bytes','unsigned'},
             {'rows','unsigned'}, {'progress','map'}}, {'bucket'})
        if recv.is_sync ~= cfg.sync then recv:alter{is_sync = cfg.sync} end
        if progress.is_sync ~= cfg.sync then progress:alter{is_sync = cfg.sync} end
    end
    function B.record(b, move)
        local r = box.space._tarn_move:get{b}
        if not r or r.move ~= move then bad(1412, 'Movement reservation or identity differs') end
        return r
    end
    function B.totals()
        local n, bytes = 0, 0
        if not box.space._tarn_move then return n, bytes end
        for _, r in box.space._tarn_move:pairs() do
            n, bytes = n + 1, bytes + r.bytes
            if n > 64 then bad(1041, 'Durable movement inventory exceeds the supported maximum') end
        end
        return n, bytes
    end
    function B.headroom(bytes)
        B.limits()
        if not ffi.abi('64bit') or (jit.os ~= 'Linux' and jit.os ~= 'OSX') then
            bad(1235, 'Bounded moves require the supported 64-bit Linux or Darwin statvfs ABI')
        end
        local kind = jit.os == 'Linux' and 'struct tarn_move_vfs_linux[1]' or 'struct tarn_move_vfs_darwin[1]'
        for _, path in ipairs({box.cfg.vinyl_dir, box.cfg.memtx_dir, box.cfg.wal_dir}) do
            local v = ffi.new(kind)
            if ffi.C.statvfs(path, v) ~= 0 then bad(1041, 'Cannot establish movement filesystem headroom') end
            local available = tonumber(v[0].bavail) * tonumber(v[0].frsize)
            -- Leave room for WAL, both copies, and engine amplification. This
            -- checks available space, not a guarantee against unrelated writers.
            if available < cfg.move_disk_reserve_bytes + 16 * bytes then
                bad(1041, 'Movement filesystem headroom exhausted; progress retained')
            end
        end
    end
    function B.capable(peer)
        -- Existing nodes learn joined shards through persisted membership;
        -- their startup peer inventory remains unchanged.

        local uris = c.shard_uris(peer)
        if type(uris) ~= 'table' or #uris == 0 or #uris > 64 then
            bad(1412, 'Bounded movement requires a complete supported replica inventory')
        end
        for _, uri in ipairs(uris) do
            local conn = require('net.box').connect(uri, {user = cfg.user, password = cfg.password,
                                                        wait_connected = 2, reconnect_after = 0})
            local ok, r = pcall(conn.call, conn, 'tarn_move_capabilities', {}, {timeout = 2})
            conn:close()
            if not ok or type(r) ~= 'table' or r[1] ~= 'ok' or r[2] ~= 2 then
                bad(1412, 'Every movement replica must advertise protocol 2 before copying')
            end
        end
    end
    -- Called in the same transaction that enters sending/receiving.
    function B.reserve(b, move, side, protocol)
        B.limits()
        if type(b) ~= 'number' or b % 1 ~= 0 or b < 1 or b > bk.count
           or type(move) ~= 'string' or #move == 0 or #move > 256 then
            bad(1210, 'Invalid movement bucket or identity')
        end
        local old = box.space._tarn_move:get{b}
        if old then
            if old.move ~= move or old.side ~= side then bad(1412, 'Another movement owns the reservation') end
            return old
        end
        local n = B.totals()
        if n >= cfg.move_slots then bad(1041, 'Concurrent movement limit exhausted') end
        return box.space._tarn_move:insert{b, move, side, 0, 0, {seq = 0, protocol = protocol or 1}}
    end
    function B.forget(b, move)
        local r = box.space._tarn_move:get{b}
        if r and r.move == move then box.space._tarn_move:delete{b} end
    end
    function B.page(rows, cp)
        local nr, nb = B.limits()
        if type(rows) ~= 'table' or #rows > nr then bad(1041, 'Movement batch row limit exceeded') end
        local encoded = mp.encode(rows)
        if #encoded > nb then bad(1041, 'Movement batch byte limit exceeded') end
        local receipt = cp and cp ~= box.NULL and
            {rows, cp.seq, cp.phase, cp.cursor or box.NULL, cp.generation, cp.done} or rows
        return #encoded, digest.sha256_hex(mp.encode(receipt))
    end
    function B.observed(n, bytes)
        B.stats.batches = B.stats.batches + 1
        B.stats.max_batch_rows = math.max(B.stats.max_batch_rows, n)
        B.stats.max_batch_bytes = math.max(B.stats.max_batch_bytes, bytes)
    end
    -- A checkpoint and its row effects commit together. Duplicate acceptance
    -- must name the same bytes; gaps/reordered pages never overwrite a new cut.
    function B.checkpoint(r, cp, hash)
        if cp == nil or cp == box.NULL then
            if r.progress.protocol == 2 then bad(1412, 'Movement page checkpoint required') end
            return false -- legacy bounded calls
        end
        if type(cp) ~= 'table' or #mp.encode(cp) > 16384 or type(cp.seq) ~= 'number'
           or cp.seq % 1 ~= 0 or cp.seq < 1 or cp.seq > 9007199254740991
           or (cp.phase ~= 'copy' and cp.phase ~= 'delta' and cp.phase ~= 'frozen')
           or type(cp.generation) ~= 'string' or #cp.generation > 256 or type(cp.done) ~= 'boolean' then
            bad(1210, 'Invalid movement page checkpoint')
        end
        local old = r.progress
        if cp.seq == old.seq and hash == old.hash then return true end
        if cp.seq ~= old.seq + 1 then bad(1412, 'Stale or out-of-order movement page') end
        if old.phase and not (old.phase == cp.phase and not old.done
           or old.phase == 'copy' and old.done and cp.phase == 'delta') then
            bad(1412, 'Movement page phase differs')
        end
        if old.generation and old.generation ~= cp.generation then bad(1412, 'Movement page generation differs') end
        return false
    end
    function B.account(r, delta_bytes, delta_rows, cp, hash)
        local bytes, rows = r.bytes + delta_bytes, r.rows + delta_rows
        local _, total = B.totals()
        if bytes < 0 or rows < 0 then bad(1412, 'Movement accounting underflow') end
        if delta_bytes > 0 and (bytes > cfg.move_staging_bytes
            or total + delta_bytes > cfg.move_total_staging_bytes) then
            bad(1041, 'Movement staging byte budget exhausted')
        end
        local p = r.progress
        if cp and cp ~= box.NULL then
            p = {seq = cp.seq, hash = hash, phase = cp.phase, cursor = cp.cursor,
                 generation = cp.generation, done = cp.done, protocol = p.protocol, reset_seq = p.reset_seq}
        end
        box.space._tarn_move:replace{r.bucket, r.move, r.side, bytes, rows, p}
        B.stats.peak_staged_bytes = math.max(B.stats.peak_staged_bytes, total + delta_bytes)
    end
    function B.reset_progress(b, move)
        local r = B.record(b, move)
        assert(r.rows == 0 and r.bytes == 0)
        local p = r.progress
        box.space._tarn_move:replace{b, move, r.side, 0, 0,
            {seq = p.seq, protocol = p.protocol, reset_seq = p.reset_seq,
             phase = p.reset_seq and 'frozen' or nil, generation = p.reset_seq and move or nil, done = false}}
    end
    function B.staged_page(b)
        local nr, nb = B.limits()
        local rows, bytes = {}, 0
        for _, t in box.space._tarn_recv:pairs({b}, {iterator = 'EQ'}) do
            local cost = t:bsize() + 32
            if cost > nb then bad(1041, 'A staged row exceeds the current movement batch limit') end
            if #rows >= nr or bytes + cost > nb then break end
            rows[#rows + 1], bytes = t, bytes + cost
        end
        return rows, bytes
    end
    function B.clear_batch(b, move)
        local r = B.record(b, move)
        local rows, bytes = B.staged_page(b)
        local removed = 0
        for _, t in ipairs(rows) do
            box.space._tarn_recv:delete{b, t.space_id, t.key}
            removed = removed + t:bsize()
        end
        B.account(r, -removed, -#rows)
        B.observed(#rows, bytes)
        return #rows == 0
    end
    -- This optimization runs inside non-yielding commit checks. It may discard
    -- only its own hints, never a user tuple, intent, vote or lazy obligation.
    function B.note(tr, sid, pk, key)
        if tr.lost or tr.keys[key] then return end
        local cost = #key + #mp.encode(pk) + 192
        local total = 0
        for _, t in pairs(bk.track) do total = total + (t.bytes or 0) end
        if #tr.list >= cfg.move_delta_keys or tr.bytes + cost > cfg.move_delta_bytes
           or total + cost > cfg.move_total_delta_bytes then
            tr.keys, tr.list, tr.bytes, tr.lost = {}, {}, 0, true
            B.stats.delta_overflows = B.stats.delta_overflows + 1
            return
        end
        tr.keys[key], tr.list[#tr.list + 1], tr.bytes = true, {sid, pk}, tr.bytes + cost
        B.stats.peak_delta_bytes = math.max(B.stats.peak_delta_bytes, total + cost)
    end
    function B.info()
        local n, bytes = B.totals()
        local limits = {}
        for _, name in ipairs(names) do limits[name] = cfg[name] end
        return {protocol = 2, active = n, copy_buffers = copies, staged_bytes = bytes, limits = limits, stats = B.stats,
                vinyl_memory = box.cfg.vinyl_memory, vinyl_cache = box.cfg.vinyl_cache}
    end
    return B
end
return M
