-- Test-only retry of two explicitly local transactions after a definite abort.
-- Their public wrappers roll back before returning this exact TT conflict.
-- This says nothing about volatile side effects, unknown commits or transport.
local R = {}
local ABORT = 'Transaction has been aborted by conflict'

function R.exact_abort(reply)
    if type(reply) ~= 'table' or reply[1] ~= 'conflict' or reply[2] ~= 1213 or reply[3] ~= ABORT then
        return false
    end
    local count = 0
    for key in pairs(reply) do
        if type(key) ~= 'number' or key % 1 ~= 0 or key < 1 or key > 3 then return false end
        count = count + 1
    end
    return count == 3
end

function R.describe_error(err)
    local ok, text = pcall(tostring, err)
    local out = {lua_type = type(err), text = ok and text or '<unprintable error>'}
    if type(err) == 'string' then out.message = err end
    if type(err) == 'table' or type(err) == 'cdata' or type(err) == 'userdata' then
        for _, key in ipairs({'type', 'code', 'message', 'sqlstate'}) do
            local got, value = pcall(function() return err[key] end)
            if got and (type(value) == 'number' or type(value) == 'string' or type(value) == 'boolean') then
                out[key] = value
            end
        end
    end
    return out
end

function R.call(procedure, invoke, attempts, now, sleep)
    assert(procedure == 'tarn_autocommit' or procedure == 'tarn_lazy_gc', 'unsupported fixture retry procedure')
    local started = now()
    local deadline = started + 1
    local last
    for attempt = 1, 3 do
        if attempt > 1 and now() >= deadline then
            return {status = 'failed', kind = 'retry_deadline', procedure = procedure, last_attempt = last}
        end
        local entry = {action = 'lazy_drain', procedure = procedure, attempt = attempt,
                       started_seconds = now(), retry_eligible = false}
        attempts[#attempts + 1] = entry
        local ok, reply = pcall(invoke)
        entry.finished_seconds = now()
        entry.elapsed_seconds = entry.finished_seconds - entry.started_seconds
        entry.elapsed_since_operation_seconds = entry.finished_seconds - started
        if not ok then
            entry.error = R.describe_error(reply)
            return {status = 'failed', kind = 'invocation_error', procedure = procedure, last_attempt = attempt,
                    error = entry.error}
        end
        entry.response_type, entry.response = type(reply), reply
        if type(reply) == 'table' and reply[1] == 'ok' then
            return {status = 'ok', procedure = procedure, last_attempt = attempt, response = reply}
        end
        entry.retry_eligible = R.exact_abort(reply)
        if not entry.retry_eligible then
            return {status = 'failed', kind = 'terminal_reply', procedure = procedure, last_attempt = attempt,
                    response_type = type(reply), response = reply}
        end
        last = attempt
        if attempt == 3 then
            return {status = 'failed', kind = 'attempt_limit', procedure = procedure, last_attempt = attempt,
                    response = reply}
        end
        if now() >= deadline then
            return {status = 'failed', kind = 'retry_deadline', procedure = procedure, last_attempt = attempt,
                    response = reply}
        end
        -- Only between definite returned aborts. Each RPC keeps its original
        -- timeout; the 1s deadline bounds starting another call, not its latency.
        sleep(.01)
    end
end

return R
