-- Test-only scheduling of the actual production box.status watch. No cache
-- bit, freshness guard, role, quorum or production function is replaced.
local fiber, json = require('fiber'), require('json')
local original_require = require
local mode = assert(os.getenv('NATIVE_FRESHNESS_CONTROL'))
assert(mode == 'native' or mode == 'handback', 'invalid native freshness control')
local c = {mode=mode,received=0,delivered=0,permits=0,pending=false,wake=fiber.cond()}
assert(rawget(_G,'native_fresh_control') == nil)
rawset(_G,'native_fresh_control',c)
local function snapshot()
    local t, ffi = rawget(_G,'tarn'), require('ffi')
    local r = {pid=box.info.pid,ro=box.info.ro,term=box.info.election.term,
        queue=box.info.synchro.queue.len,mono=require('clock').monotonic()}
    if t and box.func['tarn_storage.ts_state'] then
        local n=ffi.cast('struct tarn_ns*',box.func['tarn_storage.ts_state']:call())
        r.bk_stale,r.lk_stale,r.fence_stale=tonumber(n.bk_stale),tonumber(n.lk_stale),tonumber(n.fence_stale)
    end
    return r
end
local function record(kind, value)
    local f=assert(io.open(os.getenv('TARN_WORKDIR') .. '/status-events.jsonl','a'))
    f:write(json.encode{event=kind,value=value},'\n'); f:close()
end
_G.require = function(name, ...)
    local result=original_require(name,...)
    if name == 'tarn' then
        _G.require=original_require
        local watch=box.watch
        box.watch=function(key,callback)
            if key ~= 'box.status' then return watch(key,callback) end
            local source=debug.getinfo(callback,'S').source
            assert(source:match('/tarn%.lua$'),'unexpected status callback source: ' .. source)
            box.watch=watch -- intercept only the production registration
            c.source=source
            return watch(key,function(event_key,value)
                c.received=c.received+1
                local entry={sequence=c.received,key=event_key,value=value,received=snapshot(),source=source}
                c.pending=true; c.wake:broadcast(); record('received actual status notification',entry)
                while c.permits == 0 do c.wake:wait() end
                c.permits=c.permits-1
                entry.before=snapshot()
                local ok,err=pcall(callback,event_key,value)
                entry.after=snapshot(); entry.ok,entry.error=ok,not ok and tostring(err) or nil
                c.delivered=c.delivered+1; c.last=entry; c.pending=false
                record('delivered actual production status callback',entry); c.wake:broadcast()
                if not ok then error(err) end
            end)
        end
    end
    return result
end
function c.release()
    local deadline=fiber.clock()+3
    while not c.pending and fiber.clock()<deadline do c.wake:wait(.01) end
    assert(c.pending,'no actual pending status notification')
    local before=c.delivered
    c.permits=c.permits+1; c.wake:broadcast()
    while c.delivered == before and fiber.clock()<deadline do c.wake:wait(.01) end
    assert(c.delivered == before+1 and c.last.ok,'production status callback did not complete')
    return table.deepcopy(c.last)
end
