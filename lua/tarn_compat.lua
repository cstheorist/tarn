-- Storage compatibility admission. No schema changes or protocol actions
-- belong here; unknown persisted formats must fail before setup/recovery.
local M = {protocol = 1, format = 1}

function M.check()
    local runtime = box.info.version:match('^[^-]+')
    assert(runtime == '3.9.0', 'tarn: unsupported Tarantool ' .. tostring(runtime) .. '; see docs/upgrades.md')
    local meta = box.space._tarn_meta
    local marker = meta and meta:get{'storage_format'}
    -- 21cd708 predates the marker and uses exactly format 1.
    assert(marker == nil or marker.value == M.format,
           'tarn: unsupported persisted storage_format; restore compatible binaries; see docs/upgrades.md')
end

function M.report(cfg, layout, native, ready)
    M.check()
    assert(ready, 'tarn: storage initialization incomplete')
    return {'ok', M.protocol, M.format, cfg.shard_id, box.info.version, layout, native, cfg.max_txn_age}
end

return M
