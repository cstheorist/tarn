-- Admission is checked before setup/recovery. Format 1 may migrate only at
-- a drained maintenance boundary; the format-2 marker precedes new metadata.
local M = {protocol = 1, format = 2}

function M.current()
    local meta = box.space._tarn_meta
    local marker = meta and meta:get{'storage_format'}
    if marker then return marker.value end
    return 1
end

function M.drained()
    local function require_empty(name)
        local s = box.space[name]
        assert(not s or s.index[0]:min() == nil,
               'tarn: drain legacy work before format migration: ' .. name)
    end
    for _, name in ipairs({'_tarn_locks', '_tarn_rlocks', '_tarn_recv', '_tarn_move',
                           '_tarn_alter_undo', '_tarn_lazy_reservation', '_tarn_lazy_outbox'}) do
        require_empty(name)
    end
    local part, txn = box.space._tarn_part, box.space._tarn_txn
    assert(not part or part.index.state:count({'prepared'}) == 0, 'tarn: drain legacy prepared work before format migration')
    assert(not txn or txn.index.status:count({'staging'}) == 0, 'tarn: drain legacy decisions before format migration')
    local meta = box.space._tarn_meta
    if meta then
        for _, t in meta:pairs() do
            local k, v = t[1], t[2]
            assert(not (k:match('^job:') or k:match('^hold:') or k:match('^fence:') or k == 'ddl:job'),
                   'tarn: drain legacy metadata before format migration: ' .. k)
            assert(k ~= 'rebalance' or v.status == 'done' or v.status == 'cancelled' or v.status == 'failed',
                   'tarn: drain legacy rebalance before format migration')
        end
    end
    local buckets = box.space._tarn_buckets
    if buckets then
        for _, b in buckets:pairs() do
            assert(b.state == 'active' or b.state == 'sent', 'tarn: drain legacy bucket moves before format migration')
        end
    end
    local defs = box.space._tarn_lazy_defs
    if defs then
        for _, d in defs:pairs() do
            assert(d[4] == 'ready' or d[4] == 'retired', 'tarn: drain legacy lazy builds before format migration')
        end
    end
end

function M.check()
    local runtime = box.info.version:match('^[^-]+')
    assert(runtime == '3.9.0', 'tarn: unsupported Tarantool ' .. tostring(runtime) .. '; see docs/upgrades.md')
    local format = M.current()
    assert(format == 1 or format == M.format,
           'tarn: unsupported persisted storage_format; restore compatible binaries; see docs/upgrades.md')
    if format == 1 then M.drained() end
end

function M.persist()
    M.check()
    if M.current() ~= M.format then
        box.space._tarn_meta:replace{'storage_format', M.format}
    end
end

function M.report(cfg, layout, native, ready)
    M.check()
    local format = M.current()
    assert(ready and format == M.format and box.space._tarn_meta:get{'shard_id'} and
           box.space._tarn_move and
           box.space._tarn_recv and box.space._tarn_recv.engine == 'vinyl',
           'tarn: storage format migration/initialization incomplete')
    return {'ok', M.protocol, format, cfg.shard_id, box.info.version, layout, native, cfg.max_txn_age}
end

return M
