-- Pure injected controls for the same helper used by the RF3 backup drill.
-- No box.cfg, database, network, real conflicts or rollback proof here.
local source = debug.getinfo(1, 'S').source:sub(2)
local here = source:match('^(.*[/\\])') or './'
local R = dofile(here .. 'cluster_backup_retry.lua')
local ABORT = 'Transaction has been aborted by conflict'
local checks = 0
local function check(label, fn)
    fn()
    checks = checks + 1
    print('PASS ' .. label)
end
local function conflict() return {'conflict', 1213, ABORT} end
local function run(replies, procedure, advance, delay)
    local attempts, called, waits, at = {}, 0, {}, 0
    local function invoke()
        called = called + 1
        if advance then at = at + advance end
        local reply = replies[called]
        if type(reply) == 'function' then return reply() end
        return reply
    end
    local function sleep(n)
        waits[#waits + 1] = n
        at = at + (delay or n)
    end
    local result = R.call(procedure or 'tarn_autocommit', invoke, attempts, function() return at end, sleep)
    return result, attempts, called, waits
end

check('one successful local call preserves its full reply', function()
    local reply = {'ok', 1, {7, 'value'}}
    local result, attempts, called, waits = run({reply})
    assert(result.status == 'ok' and result.response == reply and called == 1 and #waits == 0)
    assert(#attempts == 1 and attempts[1].response == reply and attempts[1].response_type == 'table')
end)

check('only exact returned abort allows a subsequent call', function()
    local aborted, reply = conflict(), {'ok', 1, {7, 'value'}}
    local result, attempts, called, waits = run({aborted, reply})
    assert(result.status == 'ok' and called == 2 and #waits == 1 and waits[1] == .01)
    assert(attempts[1].response == aborted and attempts[1].retry_eligible == true)
    assert(attempts[2].response == reply and attempts[2].retry_eligible == false)
    assert(attempts[1].action == 'lazy_drain' and attempts[1].procedure == 'tarn_autocommit')
    assert(attempts[1].attempt == 1 and attempts[2].attempt == 2)
end)

check('foreground GC has the same explicitly whitelisted policy', function()
    local result, attempts, called = run({conflict(), {'ok', 0}}, 'tarn_lazy_gc')
    assert(result.status == 'ok' and called == 2 and attempts[1].procedure == 'tarn_lazy_gc')
end)

check('attempt cap retains all three actual aborts', function()
    local replies = {conflict(), conflict(), conflict(), {'ok'}}
    local result, attempts, called, waits = run(replies)
    assert(result.status == 'failed' and result.kind == 'attempt_limit' and called == 3 and #waits == 2)
    assert(result.response == replies[3] and #attempts == 3)
    for i = 1, 3 do assert(attempts[i].response == replies[i] and attempts[i].attempt == i) end
end)

check('a late definite abort cannot start another call past the deadline', function()
    local result, attempts, called, waits = run({conflict(), {'ok'}}, nil, 1)
    assert(result.status == 'failed' and result.kind == 'retry_deadline' and called == 1 and #waits == 0)
    assert(attempts[1].elapsed_seconds == 1 and attempts[1].response[3] == ABORT)
end)

check('the deadline is rechecked after the inter-attempt yield', function()
    local result, attempts, called, waits = run({conflict(), {'ok'}}, nil, .995, .01)
    assert(result.status == 'failed' and result.kind == 'retry_deadline' and called == 1 and #waits == 1)
    assert(#attempts == 1 and result.last_attempt == 1)
end)

check('unknown transport and other exceptions retain actual fields without retry', function()
    local err = {type = 'TimedOut', code = 78, message = 'timed out', sqlstate = '40003'}
    local result, attempts, called, waits = run({function() error(err, 0) end, {'ok'}})
    assert(result.status == 'failed' and result.kind == 'invocation_error' and called == 1 and #waits == 0)
    assert(result.error.lua_type == 'table' and result.error.type == err.type and result.error.code == err.code)
    assert(result.error.message == err.message and result.error.sqlstate == '40003')
    assert(attempts[1].error == result.error and attempts[1].retry_eligible == false)
end)

check('string errors are retained rather than parsed into retry permission', function()
    local result, attempts, called = run({function() error(ABORT, 0) end, {'ok'}})
    assert(result.kind == 'invocation_error' and called == 1)
    assert(result.error.lua_type == 'string' and result.error.message == ABORT)
    assert(attempts[1].response == nil)
end)

check('1180 unknowns 40003-shaped replies and unrelated1213 never retry', function()
    for _, reply in ipairs({{'error', 1180, 'uncertain'}, {'unknown', 1180, 'outcome unknown'},
                            {'error', 1213, ABORT}, {'conflict', 1213, 'other conflict'},
                            {'conflict', 1213, ABORT, '40003'}, {'conflict', '1213', ABORT},
                            {'conflict', 1213, ABORT, sqlstate = '40003'}}) do
        local result, attempts, called, waits = run({reply, {'ok'}})
        assert(result.kind == 'terminal_reply' and called == 1 and #waits == 0)
        assert(result.response == reply and attempts[1].response == reply and not attempts[1].retry_eligible)
    end
end)

check('sparse or extra-field replies do not match the exact dense tuple', function()
    for _, reply in ipairs({{[1] = 'conflict', [2] = 1213, [4] = ABORT},
                            {[1] = 'conflict', [2] = 1213, [3] = ABORT, [5] = true},
                            {'conflict', 1213, ABORT, note = 'extra'}}) do
        local result, _, called = run({reply, {'ok'}})
        assert(result.kind == 'terminal_reply' and called == 1)
    end
end)

check('malformed scalar and nil replies remain terminal with their actual type', function()
    for _, reply in ipairs({false, 1213, ABORT}) do
        local result, attempts, called = run({reply, {'ok'}})
        assert(result.kind == 'terminal_reply' and called == 1 and result.response == reply)
        assert(attempts[1].response_type == type(reply))
    end
    local result, attempts, called = run({})
    assert(result.kind == 'terminal_reply' and called == 1 and result.response_type == 'nil')
    assert(attempts[1].response_type == 'nil')
end)

check('unapproved procedures are rejected before even the first invocation', function()
    for _, name in ipairs({'tarn_lazy_control', 'tarn_apply', 'tarn_prepare', 'lazy_drain'}) do
        local called = false
        local ok = pcall(R.call, name, function() called = true end, {}, function() return 0 end, function() end)
        assert(not ok and not called)
    end
end)

check('clock observations are recorded without manufacturing SQLSTATE', function()
    local result, attempts = run({conflict(), {'ok'}}, nil, .1)
    assert(result.status == 'ok' and attempts[1].started_seconds == 0 and attempts[1].finished_seconds == .1)
    assert(attempts[2].started_seconds > attempts[1].finished_seconds and attempts[1].sqlstate == nil)
    assert(math.abs(attempts[2].elapsed_seconds - .1) < 1e-12)
    assert(math.abs(attempts[2].elapsed_since_operation_seconds - .21) < 1e-12)
    assert(attempts[2].elapsed_since_operation_seconds > attempts[2].elapsed_seconds)
end)

check('a failed wait preserves the prior actual response and never invokes again', function()
    local attempts, called, original = {}, 0, {message = 'wait failed'}
    local ok, err = pcall(R.call, 'tarn_autocommit', function() called = called + 1; return conflict() end,
        attempts, function() return 0 end, function() error(original, 0) end)
    assert(not ok and err == original and called == 1 and #attempts == 1 and attempts[1].retry_eligible)
end)

print(checks .. ' cluster backup retry controls, PASS')
