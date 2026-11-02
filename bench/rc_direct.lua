-- Client-side direct-storage worker for #186. No server-side workload EVAL.
-- Usage: tarantool bench/rc_direct.lua /absolute/path/to/config.json
-- stdout is a bounded readiness/final-summary protocol; every dispatched
-- attempt is first flushed to the separate append-only event journal.
local json, fiber, clock, net = require('json'), require('fiber'), require('clock'), require('net.box')
local config_file = assert(io.open(assert(arg[1], 'config path required'), 'r'))
local config = json.decode(config_file:read('*a')); config_file:close()
local journal = assert(io.open(config.events, 'a'))
local states, errors = {}, {}
local summary = {schema = 1, complete = false, clients = config.clients, workers_live = false, admitted = 0, finished = 0,
                 calls = {tarn_lock = 0, tarn_commit_rc = 0, tarn_unlock = 0}, errors = errors}
local stop, frozen, ready, started = false, false, 0, nil
local go = fiber.channel(config.clients)
local function save_summary()
    local file = assert(io.open(config.summary, 'w'))
    assert(file:write(json.encode(summary), '\n')); assert(file:flush()); file:close()
end
local function event(value)
    assert(journal:write(json.encode(value), '\n')); assert(journal:flush())
    if value.event == 'start' then summary.admitted = summary.admitted + 1 end
    if value.event == 'finish' then summary.finished = summary.finished + 1 end
end
local function array(value, size)
    if type(value) ~= 'table' or #value ~= size then return false end
    local count = 0
    for k in pairs(value) do
        if type(k) ~= 'number' or k < 1 or k > size or k % 1 ~= 0 then return false end
        count = count + 1
    end
    return count == size
end
local function refusal(reply, stage)
    if not (array(reply, 3) or stage ~= 'commit' and array(reply, 4)) then return false end
    if type(reply[3]) ~= 'string' then return false end
    if #reply == 4 and type(reply[4]) ~= 'string' then return false end
    return reply[1] == 'timeout' and reply[2] == 1205
        or reply[1] == 'deadlock' and reply[2] == 1213
        or reply[1] == 'conflict' and reply[2] == 1213
        or reply[1] == 'error' and (reply[2] == 1205 or reply[2] == 1213)
end
local function finish(state, outcome, reply, failure)
    if frozen or state.current == nil or state.current.finished then return end
    local a = state.current
    event({event = 'finish', token = a.token, outcome = outcome, stage = a.stage,
           latency_seconds = clock.monotonic() - a.began, reply = reply,
           failure = failure, calls = a.calls})
    a.finished, a.outcome = true, outcome
end
local function call(state, name, args, timeout)
    state.current.calls = state.current.calls + 1
    summary.calls[name] = summary.calls[name] + 1
    -- No reconnect option and no retry around CALL, including on timeout.
    return state.connection:call(name, args, {timeout = timeout or config.call_timeout})
end
local function lock_reply(reply, missing)
    assert(array(reply, 3) and reply[1] == 'ok' and type(reply[2]) == 'string'
           and #reply[2] > 0 and array(reply[3], 1), 'malformed lock success')
    local row = reply[3][1]
    if missing then
        assert(row == box.NULL, 'ledger token already exists')
    else
        -- net.box CALL can decode a native tuple as an ordinary array.
        if box.tuple.is(row) then row = row:totable() end
        assert(array(row, config.t.width), 'malformed locked row')
    end
    return reply[2], row
end
local function attempt(state, token, key, txid)
    local a = {token = token, key = key, txid = txid, began = clock.monotonic(),
               stage = 'lock_t', calls = 0, finished = false}
    state.current = a
    -- If this write/flush fails, no storage CALL for this identity is sent.
    event({event = 'start', token = token, key = key, txid = txid, worker = state.id,
           number = config.number, began = a.began})
    local options = {timeout = 1, nowait = false, skip = false, read = true, held = {}}
    local function refused(reply)
        if not refusal(reply, a.stage) then return false end
        local cleanup = call(state, 'tarn_unlock', {txid}, 5)
        assert(array(cleanup, 1) and cleanup[1] == 'ok', 'lock cleanup did not complete')
        finish(state, 'refused', reply)
        return true
    end
    local first = call(state, 'tarn_lock', {txid, {{config.t.sid, 0, {1, key}, config.bucket}}, 'X', options})
    if refused(first) then return end
    local generation, row = lock_reply(first, false)
    local f = config.t.fields
    assert(row[f.tenant] == 1 and row[f.id] == key and row[f.bucket_id] == config.bucket,
           'locked row has the wrong identity or bucket')
    assert(row[f.payload] == string.rep('x', 256), 'locked payload changed')
    local value = tonumber(row[f.v])
    assert(value ~= nil and value >= 0 and value % 1 == 0 and value < config.clients * config.max_attempts,
           'locked counter is outside the exact workload bounds')
    a.stage = 'lock_ledger'
    local second = call(state, 'tarn_lock', {txid, {{config.ledger.sid, 0, {1, token}, config.bucket}}, 'X', options})
    if refused(second) then return end
    local other_generation = lock_reply(second, true)
    assert(other_generation == generation, 'lock generation changed before commit')
    row[f.v] = value + 1
    local lf, ledger = config.ledger.fields, {}
    ledger[lf.tenant], ledger[lf.token], ledger[lf.k], ledger[lf.bucket_id] = 1, token, key, config.bucket
    assert(array(ledger, config.ledger.width), 'incomplete ledger tuple')
    a.stage = 'commit'
    event({event = 'commit', token = token})
    local committed = call(state, 'tarn_commit_rc', {txid,
        {{'replace', config.t.sid, row}, {'insert', config.ledger.sid, ledger}},
        generation, config.fence, {config.bucket, config.bucket}})
    if refused(committed) then return end
    assert(array(committed, 2) and committed[1] == 'ok' and committed[2] == 2,
           'malformed or unexpected commit success')
    finish(state, 'ok', committed)
end
local function close(state)
    if state.connection ~= nil then pcall(state.connection.close, state.connection) end
end
local run_ok, run_error = pcall(function()
    assert(config.clients == 1 or config.clients == 8)
    assert(config.seconds >= 1 and config.seconds <= 15 and config.max_attempts == 50000)
    assert(config.call_timeout == 10 and config.drain_seconds == 35)
    assert(config.shape == 'uncontended' or config.shape == 'unrelated' or config.shape == 'hot')
    assert(config.clients == (config.shape == 'uncontended' and 1 or 8))
    save_summary()
    for id = 0, config.clients - 1 do
        local state = {id = id, done = false}
        states[#states + 1] = state
        local worker = fiber.new(function()
            fiber.yield() -- publish the joinable handle before any startup failure
            local ok, err = pcall(function()
                state.connection = net.connect(config.address, {user = 'tarn', password = 'tarn', wait_connected = 5})
                assert(state.connection:is_connected(), 'direct worker could not connect')
                ready = ready + 1
                assert(go:get(30) == true, 'direct worker start gate expired')
                for sequence = 1, config.max_attempts do
                    if stop or clock.monotonic() >= started + config.seconds then return end
                    local token = config.number * 1000000 + id * 100000 + sequence
                    local key = config.shape == 'hot' and 0 or id
                    local txid = config.run_id .. '/' .. config.number .. '/' .. id .. '/' .. sequence
                    attempt(state, token, key, txid)
                    if sequence == config.max_attempts and clock.monotonic() < started + config.seconds then
                        error('bounded attempt cap reached before duration')
                    end
                end
            end)
            if not ok then
                stop = true
                local message = tostring(err):sub(1, 4096)
                errors[#errors + 1] = {worker = id, error = message}
                local saved, save_error = pcall(finish, state, 'unknown', nil, message)
                if not saved then errors[#errors + 1] = {worker = id, error = tostring(save_error):sub(1, 4096)} end
            end
            close(state)
            state.done = true
        end)
        worker:set_joinable(true); state.fiber = worker
    end
    local connection_deadline = clock.monotonic() + 10
    while ready < config.clients and not stop and clock.monotonic() < connection_deadline do fiber.sleep(.001) end
    assert(ready == config.clients and not stop, 'workers did not become ready')
    io.stdout:write(json.encode({ready = true, clients = ready}), '\n'); io.stdout:flush()
    assert(io.read('*l') == 'GO', 'missing collector start signal')
    started = clock.monotonic(); summary.started = started
    for _ = 1, config.clients do assert(go:put(true, 1)) end
    local drain_deadline = started + config.seconds + config.drain_seconds
    while true do
        local done = true
        for _, state in ipairs(states) do if not state.done then done = false end end
        if done then break end
        if clock.monotonic() >= drain_deadline then
            stop = true; summary.workers_live = true
            errors[#errors + 1] = {error = 'bounded worker drain expired'}
            for _, state in ipairs(states) do
                if not state.done then pcall(finish, state, 'unknown', nil, 'bounded worker drain expired') end
            end
            frozen = true
            break
        end
        fiber.sleep(.001)
    end
    summary.elapsed_seconds = clock.monotonic() - started
    summary.complete = not summary.workers_live and #errors == 0
end)
stop = true
if not run_ok then
    errors[#errors + 1] = {error = tostring(run_error):sub(1, 4096)}
    summary.complete = false
    for _, state in ipairs(states) do pcall(finish, state, 'unknown', nil, 'driver interrupted') end
end
frozen = true
go:close()
summary.states = {}
for _, state in ipairs(states) do
    summary.states[#summary.states + 1] = {worker = state.id, done_at_cut = state.done,
        current = state.current, connected = state.connection ~= nil and state.connection:is_connected()}
    if not state.done then summary.workers_live = true end
    close(state)
    if not state.done then pcall(state.fiber.cancel, state.fiber) end
    local joined, status = pcall(state.fiber.join, state.fiber, 1)
    if not joined or not status then
        summary.complete = false
        errors[#errors + 1] = {worker = state.id, error = 'worker did not join'}
    end
end
if summary.workers_live or #errors > 0 then summary.complete = false end
journal:flush(); journal:close()
save_summary()
io.stdout:write(json.encode(summary), '\n'); io.stdout:flush()
os.exit(summary.complete and 0 or 1)
