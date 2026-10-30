-- Real storage procedures, with observations and before-effect holds only.
local fiber = require('fiber')
box.cfg{
    listen = '127.0.0.1:0',
    memtx_use_mvcc_engine = true,
    memtx_memory = 256 * 1024 * 1024,
    log = 'tarantool.log',
    checkpoint_interval = 0,
}
package.path = os.getenv('TARN_TEST_LUA_DIR') .. '/?.lua;' .. package.path
require('tarn').init{shard_id = 1, sync = false, native = false, check_yields = true}

local original = tarn_autocommit
fallback_log = {native = {}, lua = {}, active = {}}
fallback_hold = false
fallback_mode = 'lua'
local function note(kind)
    table.insert(fallback_log[kind], tonumber(box.session.id()))
end
function tarn_autocommit(...)
    note('lua')
    if fallback_mode == 'readonly_lua' then
        fallback_mode = 'lua'
        box.error(box.error.READONLY)
    end
    local id = tonumber(box.session.id())
    fallback_log.active[tostring(id)] = true
    while fallback_hold do fiber.sleep(0.001) end
    fallback_log.active[tostring(id)] = nil
    return original(...)
end
function fallback_native(...)
    note('native')
    if fallback_mode == 'readonly_native' then
        fallback_mode = 'lua'
        box.error(box.error.READONLY)
    elseif fallback_mode == 'all' then
        return {'lua', 'all'}
    elseif fallback_mode == 'ok' then
        return original(...)
    elseif fallback_mode == 'conflict' then
        box.error(box.error.TRANSACTION_CONFLICT)
    elseif fallback_mode == 'hold_after_effect' then
        local result = original(...)
        fallback_log.committed = true
        while fallback_hold do fiber.sleep(0.001) end
        return result
    elseif fallback_mode:match('^malformed_') or fallback_mode == 'unknown' then
        local result = original(...)
        assert(result[1] == 'ok')
        fallback_log.committed = true
        if fallback_mode == 'malformed_error' then return {'error', 1213, 'bad envelope', 'extra'} end
        if fallback_mode == 'malformed_lua' then return {'lua', 'extra'} end
        if fallback_mode == 'malformed_returns' then return {'error', 1213, 'bad envelope'}, 'extra' end
        if fallback_mode == 'malformed_missing' then return end
        return {'error', 1180, 'native commit outcome unknown'}
    end
    return {'lua'}
end
tarn_storage = {ts_autocommit = fallback_native}

local f = assert(io.open('listen.tmp', 'w'))
f:write(tostring(box.info.listen))
f:close()
os.rename('listen.tmp', 'listen.txt')
