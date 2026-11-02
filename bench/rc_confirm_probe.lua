-- Deterministic confirmation-handoff probe. Run only on the runner's owned
-- fixture, before SQL setup/timing. Never use this injected delay as a timing
-- result: it exposes the interval after real confirmation but before the
-- caller of box.commit resumes. The rollback case is an injected control,
-- not a real WAL/quorum failure qualification.
-- Usage: tarantool bench/rc_confirm_probe.lua ADDRESS control|candidate
local net, json = require('net.box'), require('json')
local address, arm = arg[1], arg[2]
if not address or (arm ~= 'control' and arm ~= 'candidate') then
    print(json.encode({ok = false, error = 'usage: rc_confirm_probe.lua ADDRESS control|candidate'}))
    os.exit(1)
end
local connection = net.connect(address, {user = 'tarn', password = 'tarn',
                                        wait_connected = 5, connect_timeout = 5})
local ok, result = pcall(function()
    assert(connection:is_connected(), 'probe could not connect')
    return connection:eval([=[
local arm = ...
local fiber, uuid = require('fiber'), require('uuid')
local tarn = require('tarn')
assert(not box.info.ro and tarn.cfg.sync == true, 'probe needs the RF3 synchronous leader')
local original_commit = box.commit
local prefix = 'rc_confirm_probe_' .. uuid.str():gsub('-', '')
local spaces, txids, workers, gates, cases = {}, {}, {}, {}, {}
local active, watchdog_fired = true, false
local controller = fiber.self()
local target, inject_rollback, committed = nil, false, nil
local release, go, commit_done
local function await(predicate, label, seconds)
    local deadline = fiber.clock() + (seconds or 5)
    repeat
        if predicate() then return end
        fiber.sleep(0.001)
    until fiber.clock() >= deadline
    error('probe timeout: ' .. label)
end
local function spawn(fn)
    local state = {done = false}
    local worker = fiber.new(function()
        state.ok, state.result = pcall(fn)
        state.done = true
    end)
    worker:set_joinable(true)
    state.fiber = worker
    workers[#workers + 1] = state
    return state
end
local function channel()
    local ch = fiber.channel(1)
    gates[#gates + 1] = ch
    return ch
end
local function unlock_all()
    for _, txid in ipairs(txids) do tarn_unlock(txid) end
end
local function queued_behind(waiter_id, owner_id, wait_id)
    for _, edge in ipairs(tarn_lock_waits()) do
        if edge[1] == waiter_id and (wait_id == nil or edge[2] == wait_id) then
            for _, holder in ipairs(edge[3]) do
                if holder == owner_id then return edge[2] end
            end
        end
    end
    return nil
end
-- If the outer request is interrupted, a second fiber still restores the
-- wrapper and wakes held workers. A failed probe invalidates this fixture.
local watchdog = fiber.new(function()
    fiber.sleep(45)
    if active then
        watchdog_fired = true
        box.commit = original_commit
        for _, ch in ipairs(gates) do ch:close() end
        for _, w in ipairs(workers) do if not w.done then pcall(w.fiber.cancel, w.fiber) end end
        pcall(unlock_all)
        pcall(controller.cancel, controller)
    end
end)
watchdog:set_joinable(true)
local exercised, failure = pcall(function()
    await(function() return tarn.clock.read_check() == nil end, 'freshness clock ready', 10)
    for _, engine in ipairs({'memtx', 'vinyl'}) do
        local sp = box.schema.space.create(prefix .. '_' .. engine, {engine = engine, is_sync = true})
        spaces[#spaces + 1] = sp
        sp:format({{name = 'id', type = 'unsigned'}, {name = 'v', type = 'unsigned'}})
        sp:create_index('primary', {parts = {{field = 'id', type = 'unsigned'}}})
        for _, rollback in ipairs({false, true}) do
            box.commit = original_commit
            sp:replace{1, 0}
            local suffix = engine .. (rollback and '_rollback' or '_confirmed')
            local owner_id, waiter_id, nowait_id = prefix .. suffix .. '_owner',
                                                  prefix .. suffix .. '_waiter', prefix .. suffix .. '_nowait'
            txids[#txids + 1], txids[#txids + 2], txids[#txids + 3] = owner_id, waiter_id, nowait_id
            release, go, commit_done = channel(), channel(), channel()
            committed, inject_rollback, target = false, rollback, nil
            local owner_locked = false
            local owner = spawn(function()
                target = fiber.self():id()
                local locked = tarn_lock(owner_id, {{sp.id, 0, {1}}}, 'X', {read = true, timeout = 10})
                assert(locked[1] == 'ok', 'owner lock failed')
                owner_locked = true
                assert(go:get(10) == true, 'owner commit gate expired')
                return tarn_commit_rc(owner_id, {{'replace', sp.id, {1, 1}}}, locked[2])
            end)
            await(function() return owner_locked or owner.done end, 'owner lock')
            assert(owner_locked and not owner.done, 'owner failed before commit')
            local waiter = spawn(function()
                return tarn_lock(waiter_id, {{sp.id, 0, {1}}}, 'X', {read = true, timeout = 10})
            end)
            local waiter_wait_id
            await(function()
                waiter_wait_id = queued_behind(waiter_id, owner_id)
                return waiter_wait_id ~= nil or waiter.done
            end, 'waiter queue')
            assert(waiter_wait_id ~= nil and not waiter.done, 'waiter did not queue behind owner')
            local waits_before = tarn.stats.commit_waits
            box.commit = function(...)
                if fiber.self():id() ~= target then return original_commit(...) end
                if inject_rollback then
                    box.rollback()
                    error(box.error.new{code = box.error.TRANSACTION_CONFLICT,
                                        reason = 'rc confirmation probe rollback'})
                end
                local value = original_commit(...)
                committed = true
                assert(commit_done:put(true, 5), 'confirmation report gate expired')
                assert(release:get(5) == true, 'postconfirmation gate expired')
                return value
            end
            assert(go:put(true, 5), 'commit dispatch gate expired')
            if rollback then
                await(function() return owner.done end, 'rolled back owner')
                assert(owner.ok, tostring(owner.result))
                assert(owner.result[1] == 'conflict' and owner.result[2] == 1213
                       and owner.result[3] == 'rc confirmation probe rollback', 'rollback error changed')
                assert(not committed and sp:get{1}[2] == 0, 'rollback changed the committed row')
                assert(not waiter.done, 'rollback prematurely released the owner lock')
                local refused = tarn_lock(nowait_id, {{sp.id, 0, {1}}}, 'X', {nowait = true, timeout = 1})
                assert(refused[1] == 'locked' and refused[2] == 3572, 'failed commit lost its row lock')
                -- NOWAIT could conflict with an accidentally granted waiter.
                -- Require the original request to remain queued on this owner.
                assert(queued_behind(waiter_id, owner_id, waiter_wait_id) == waiter_wait_id,
                       'rollback did not retain the original waiter-to-owner edge')
                tarn_unlock(owner_id)
            else
                assert(commit_done:get(5) == true, 'real commit did not confirm')
                assert(committed and not owner.done and sp:get{1}[2] == 1, 'hold must follow real confirmation')
                if arm == 'candidate' then
                    await(function() return waiter.done end, 'candidate waiter before caller resumes', 2)
                    assert(waiter.ok and waiter.result[1] == 'ok' and waiter.result[3][1][2] == 1,
                           'candidate did not read confirmed row while caller was held')
                else
                    await(function() return tarn.stats.commit_waits > waits_before or waiter.done end,
                          'control waiter reaches inflight wait', 2)
                    assert(not waiter.done and tarn.stats.commit_waits > waits_before,
                           'control unexpectedly escaped the confirmed inflight marker')
                end
                assert(release:put(true, 5), 'caller resume gate expired')
            end
            await(function() return owner.done and waiter.done end, 'owner and waiter finish')
            assert(owner.ok and waiter.ok, tostring(owner.result) .. ' / ' .. tostring(waiter.result))
            assert(waiter.result[1] == 'ok' and waiter.result[3][1][2] == (rollback and 0 or 1),
                   'waiter returned an incorrect row or leaked inflight entry')
            if not rollback then assert(owner.result[1] == 'ok' and owner.result[2] == 1, 'commit result changed') end
            cases[#cases + 1] = {engine = engine, injected_rollback = rollback, owner_status = owner.result[1],
                owner_code = rollback and owner.result[2] or 0, row = waiter.result[3][1][2],
                commit_waits = tarn.stats.commit_waits - waits_before,
                waiter_finished_during_confirmed_hold = not rollback and arm == 'candidate'}
            box.commit = original_commit
            unlock_all()
        end
    end
end)
-- Restore before any cleanup can yield, including when an assertion fails.
box.commit = original_commit
for _, ch in ipairs(gates) do ch:close() end
pcall(unlock_all)
local cleanup_errors = {}
for _, worker in ipairs(workers) do
    if not worker.done then pcall(worker.fiber.cancel, worker.fiber) end
    local joined, status = pcall(worker.fiber.join, worker.fiber, 5)
    if not joined or not status then cleanup_errors[#cleanup_errors + 1] = 'worker did not join' end
end
pcall(unlock_all)
for _, sp in ipairs(spaces) do
    local dropped, err = pcall(sp.drop, sp)
    if not dropped then cleanup_errors[#cleanup_errors + 1] = tostring(err) end
end
active = false
pcall(watchdog.cancel, watchdog)
pcall(watchdog.join, watchdog, 5)
return {ok = exercised and not watchdog_fired and #cleanup_errors == 0,
        arm = arm, cases = cases, error = exercised and box.NULL or tostring(failure),
        watchdog_fired = watchdog_fired, cleanup_errors = cleanup_errors,
        wrapper_restored = box.commit == original_commit,
        space_removed = box.space[prefix .. '_memtx'] == nil and box.space[prefix .. '_vinyl'] == nil}
]=], {arm}, {timeout = 90})
end)
connection:close()
if not ok then result = {ok = false, arm = arm, error = tostring(result)} end
print(json.encode(result))
os.exit(result.ok and result.wrapper_restored and result.space_removed and 0 or 1)
