-- Preload only in lazy_replication.sh-owned storage processes. Hold the
-- initial real box.status notification before its production callback,
-- then let the RF3 client release it after confirmed PREPARE/CAS.
local fiber = require('fiber')
local json = require('json')
local pre_config_watch, original_require = box.watch, require
assert(rawget(_G, 'lazy_rf3_status_control') == nil, 'status control loaded twice')
local c = {pending = false, released = false, done = false, wake = fiber.cond(),
    phase = 'waiting for Tarn require after box.cfg', registered = 0, callbacks = 0}
rawset(_G, 'lazy_rf3_status_control', c)
local function install()
    -- storage.lua configures box before require('tarn').init{...}. Arm
    -- after that require returns, while init has not registered its watch.
    local watch = box.watch
    c.watch_changed_after_config = watch ~= pre_config_watch
    c.phase = 'armed after Tarn require, before init'
    box.watch = function(key, callback)
    if key ~= 'box.status' then return watch(key, callback) end
    local source = debug.getinfo(callback, 'S').source
    assert(source:match('/tarn%.lua$'), 'status control refused another callback: ' .. source)
    c.registered, c.callback_source = c.registered + 1, source
    box.watch = watch -- only the production registration, never other observers
    local first = true
    return watch(key, function(event_key, value)
        c.callbacks = c.callbacks + 1
        if not first then return callback(event_key, value) end
        first = false
        c.phase = 'holding initial real notification'
        c.pending, c.key, c.value, c.callback_source = true, event_key, value, source
        c.wake:broadcast()
        while not c.released do c.wake:wait() end
        local t = assert(rawget(_G, 'tarn'), 'Tarn init did not finish')
        c.epoch_before, c.ro_before = t.lazy.quota.epoch, box.info.ro
        c.term_before = box.info.election.term
        c.phase = 'calling production callback'
        local good, err = pcall(callback, event_key, value)
        c.callback_ok, c.callback_error = good, not good and tostring(err) or nil
        c.epoch_after, c.ro_after = t.lazy.quota.epoch, box.info.ro
        c.term_after, c.capture_stale = box.info.election.term, t.lazy.stale
        c.quota_ready = t.lazy.quota.ready()
        c.done = true
        c.phase = good and 'production callback complete' or 'production callback failed'
        c.wake:broadcast()
        if not good then error(err) end
    end)
    end
end
_G.require = function(name, ...)
    local result = original_require(name, ...)
    if name == 'tarn' then
        _G.require = original_require
        assert(type(result) == 'table' and type(result.init) == 'function', 'unexpected Tarn module')
        install()
    end
    return result
end
local function diagnostic()
    return json.encode{phase=c.phase,registered=c.registered,callbacks=c.callbacks,
        pending=c.pending,released=c.released,done=c.done,callback_source=c.callback_source,
        watch_changed_after_config=c.watch_changed_after_config,key=c.key,
        epoch_before=c.epoch_before,epoch_after=c.epoch_after,callback_error=c.callback_error}
end
function c.release()
    local deadline = fiber.clock() + 3
    while not c.pending and fiber.clock() < deadline do c.wake:wait(.01) end
    assert(c.pending and not c.released, 'no unique held initial notification: ' .. diagnostic())
    c.released = true; c.wake:broadcast()
    deadline = fiber.clock() + 3
    while not c.done and fiber.clock() < deadline do c.wake:wait(.01) end
    assert(c.done, 'production status callback did not finish: ' .. diagnostic())
    return {key=c.key,value=c.value,callback_source=c.callback_source,
        registered=c.registered,callbacks=c.callbacks,watch_changed_after_config=c.watch_changed_after_config,
        callback_ok=c.callback_ok,callback_error=c.callback_error,
        epoch_before=c.epoch_before,epoch_after=c.epoch_after,
        ro_before=c.ro_before,ro_after=c.ro_after,term_before=c.term_before,term_after=c.term_after,
        capture_stale=c.capture_stale,quota_ready=c.quota_ready}
end
