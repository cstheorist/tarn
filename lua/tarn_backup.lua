-- Administrative, fail-closed cluster maintenance. No SQL/commit retry path.
-- A fence is a local fsynced file as well as read_only/election_mode=off:
-- replicas must remain fenced after a restart, including an old leader.
local fio, json, mp = require('fio'), require('json'), require('msgpack')
local crypto, digest, fiber = require('crypto'), require('digest'), require('fiber')
local M = {marker = 'tarn-maintenance.json', format = 'tarn-cluster-backup-v1'}

local function sync_directory(path)
    local f = assert(fio.open(path, {'O_RDONLY'})); assert(f:fsync()); f:close()
end

function M.read_marker(root)
    local path = fio.pathjoin(root or '.', M.marker)
    if not fio.path.exists(path) then return nil end
    local f = assert(io.open(path)); local text = f:read(16385); f:close()
    assert(#text <= 16384, 'oversized maintenance marker')
    local m = json.decode(text)
    assert(m.format == M.format and type(m.token) == 'string' and #m.token == 36
           and (m.kind == 'source' or m.kind == 'restore'), 'invalid maintenance marker')
    return m
end

local function held(token)
    local m = M.read_marker()
    assert(m and m.token == token, 'maintenance token differs or is absent')
    assert(box.cfg.read_only and box.info.ro and box.cfg.election_mode == 'off', 'node is not fenced')
    return m
end

local function vector()
    local v = {}
    for id, lsn in pairs(box.info.vclock) do if id ~= 0 then v[tostring(id)] = tostring(lsn) end end
    return v
end

-- Credential-free endpoint identities for complete deployment inventory checks.
local function endpoint(uri)
    local p = assert(require('uri').parse(uri))
    return {host = p.host or '', service = p.service or ''}
end

local function canonical(v)
    if type(v) ~= 'table' then return mp.encode(v) end
    local keys, out = {}, {'T'}
    for k in pairs(v) do keys[#keys + 1] = k end
    table.sort(keys, function(a, b) return mp.encode(a) < mp.encode(b) end)
    for _, k in ipairs(keys) do
        local a, b = canonical(k), canonical(v[k])
        out[#out + 1] = #a .. ':' .. a .. #b .. ':' .. b
    end
    return table.concat(out) .. 'E'
end

local function rowhash(s)
    local d, n, bytes = crypto.digest.sha256.new(), 0, 0
    for _, t in s:pairs() do
        local b = mp.encode(t)
        d:update(#b .. ':' .. b); n, bytes = n + 1, bytes + #b
        if n % 1024 == 0 then fiber.yield() end
    end
    local hex = d:result():gsub('.', function(c) return string.format('%02x', string.byte(c)) end)
    return {rows = n, bytes = bytes, sha256 = hex}
end

local function readiness()
    local errors, buckets = {}, {}
    for id, s in pairs(box.space) do
        if type(id) == 'number' and id > box.schema.SYSTEM_ID_MAX then
            local opts = box.space._space:get{id}[6]
            if s.is_local and not opts.temporary and opts.type ~= 'data-temporary' then
                errors[#errors + 1] = 'persistent local space is unsupported: ' .. s.name
            end
        end
    end
    local function empty(name)
        local s = box.space[name]
        if s and s:len() ~= 0 then errors[#errors + 1] = name .. ' is not empty' end
    end
    for _, name in ipairs({'_tarn_locks', '_tarn_rlocks', '_tarn_recv', '_tarn_alter_undo',
                           '_tarn_lazy_reservation', '_tarn_lazy_outbox'}) do empty(name) end
    for _, p in box.space._tarn_part.index.state:pairs({'prepared'}, {iterator = 'EQ'}) do
        errors[#errors + 1] = 'prepared participant'; break
    end
    for _, t in box.space._tarn_txn.index.status:pairs({'staging'}, {iterator = 'EQ'}) do
        errors[#errors + 1] = 'staging decision'; break
    end
    local meta, members, count = box.space._tarn_meta, nil, nil
    for _, t in meta:pairs() do
        local k, v = t[1], t[2]
        if k:match('^job:') or k:match('^hold:') or k:match('^fence:') or k == 'ddl:job' then
            errors[#errors + 1] = 'unfinished metadata: ' .. k
        elseif k == 'rebalance' and v.status ~= 'done' and v.status ~= 'cancelled' and v.status ~= 'failed' then
            errors[#errors + 1] = 'unfinished rebalance'
        elseif k == 'members' then members = v
        elseif k == 'buckets' then count = v.count end
        assert(#errors <= 128, 'too many unfinished operations')
    end
    for _, b in box.space._tarn_buckets:pairs() do
        assert(#buckets < 1000000, 'bucket inventory limit exceeded')
        if b[2] == 'active' then buckets[#buckets + 1] = b[1]
        elseif b[2] ~= 'sent' then errors[#errors + 1] = 'unfinished bucket ' .. b[1]; break end
    end
    for _, d in box.space._tarn_lazy_defs:pairs() do
        if d[4] ~= 'ready' and d[4] ~= 'retired' then errors[#errors + 1] = 'unfinished lazy build'; break end
    end
    if box.info.synchro.queue.len ~= 0 then errors[#errors + 1] = 'unsettled synchronous queue' end
    return errors, buckets, members, count
end

function M.inspect()
    assert(box.space._tarn_meta and box.space._tarn_catalog, 'Tarn is not initialized')
    local errors, buckets, members, count = readiness()
    local registered, peers, replicas = {}, {}, {}
    for _, t in box.space._cluster:pairs() do registered[#registered + 1] = t[2] end
    table.sort(registered)
    local cfg = rawget(_G, 'tarn') and tarn.cfg
    if cfg then
        for id, uris in pairs(cfg.peers) do
            local endpoints = {}; for _, uri in ipairs(uris) do endpoints[#endpoints + 1] = endpoint(uri) end
            peers[tostring(id)] = endpoints
        end
    end
    for _, uri in ipairs(box.cfg.replication or {}) do replicas[#replicas + 1] = endpoint(uri) end
    local clean_members
    if members then
        clean_members = {ver = members.ver, shards = {}}
        for _, s in ipairs(members.shards) do
            local endpoints = {}; for _, uri in ipairs(s.replicas) do endpoints[#endpoints + 1] = endpoint(uri) end
            clean_members.shards[#clean_members.shards + 1] = {id = s.id, state = s.state or 'active', replicas = endpoints}
        end
    end
    local catalog = {}
    for _, t in box.space._tarn_catalog:pairs() do catalog[#catalog + 1] = t:totable() end
    return {uuid = tostring(box.info.uuid), replicaset_uuid = tostring(box.info.replicaset.uuid),
        shard = box.space._tarn_meta:get{'shard_id'}[2], version = box.info.version,
        source_dir = fio.cwd(), wal_mode = box.cfg.wal_mode, read_only = box.cfg.read_only,
        memtx_memory = box.cfg.memtx_memory, vinyl_memory = box.cfg.vinyl_memory,
        ro = box.info.ro, election_mode = box.cfg.election_mode, vclock = vector(),
        registered = registered, peers = peers, replicas = replicas, members = clean_members or box.NULL,
        errors = errors, buckets = buckets, bucket_count = count,
        catalog_sha256 = digest.sha256_hex(canonical(catalog)), marker = M.read_marker() or box.NULL}
end

function M.fence(token, plan_hash)
    assert(type(token) == 'string' and require('uuid').fromstr(token), 'invalid maintenance token')
    assert(type(plan_hash) == 'string' and #plan_hash == 64, 'invalid plan hash')
    assert(M.read_marker() == nil, 'another maintenance operation owns this node')
    local m = {format = M.format, token = token, plan_sha256 = plan_hash, kind = 'source',
               uuid = tostring(box.info.uuid)}
    local f = assert(fio.open(M.marker, {'O_WRONLY', 'O_CREAT', 'O_EXCL'}, tonumber('600', 8)))
    assert(f:write(json.encode(m))); assert(f:fsync()); f:close(); sync_directory('.')
    -- No success is returned until the storage engine has installed the fence.
    -- Failure at any boundary leaves the marker; the controller never retries.
    box.cfg{read_only = true, election_mode = 'off'}
    held(token)
    return M.inspect()
end

function M.audit(token)
    held(token)
    local before, spaces = vector(), {}
    for id, s in pairs(box.space) do
        if type(id) == 'number' and id > box.schema.SYSTEM_ID_MAX then
            local definition = box.space._space:get{id}
            local options = definition[6]
            if not options.temporary and options.type ~= 'data-temporary' then
                assert(s.index[0], 'persistent space without primary index')
                local indexes = {}
                for _, ix in box.space._index:pairs({id}, {iterator = 'EQ'}) do indexes[#indexes + 1] = ix:totable() end
                local h = rowhash(s)
                h.schema_sha256 = digest.sha256_hex(canonical({definition:totable(), indexes}))
                spaces[s.name] = h
            end
        end
    end
    held(token)
    assert(canonical(before) == canonical(vector()), 'WAL changed during frozen audit')
    return {info = M.inspect(), spaces = spaces}
end

function M.checkpoint(token)
    held(token)
    box.snapshot()
    held(token)
    return M.inspect()
end

function M.release(token)
    local m = held(token)
    assert(m.kind == 'source', 'restored nodes require whole-cluster verification before activation')
    assert(fio.unlink(M.marker)); sync_directory('.')
    -- Remain read-only. Restart every source replica using its original config.
    return {released = true, restart_required = true}
end

return M
