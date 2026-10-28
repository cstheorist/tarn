-- Targeted read-freshness integration and public callback lifetime checks.
-- A private standalone data directory is required; no services are started.
local ffi, fio, fiber = require('ffi'), require('fio'), require('fiber')
jit.off(true, true) -- this test also invokes native FFI callbacks from Lua
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 128 * 1024 * 1024,
        vinyl_memory = 32 * 1024 * 1024, vinyl_cache = 0, txn_isolation = 'read-confirmed',
        wal_mode = 'fsync', log_level = 3}
local tarn = require('tarn')
tarn.init{shard_id = 1, native = false, test_hooks = true}
local F = tarn.fresh
F.set_clock(nil) -- keep this helper test independent of the live clock worker
local checks = 0
local function check(yes, label)
    assert(yes, label); checks = checks + 1; print('ok ' .. label)
end
local function id() return ffi.C.box_txn_id() end
local function healthy()
    F.set_clock{check = function() return nil end,
                native_state = function() return true, 1ULL, 2ULL, 3ULL, 4ULL end}
end
check(tarn.NS_LAYOUT == 13 and ffi.sizeof('struct tarn_ns') == 392 and ffi.offsetof('struct tarn_ns', 'fresh_mode') == 312
      and ffi.offsetof('struct tarn_ns', 'read_timeout') == 320
      and ffi.offsetof('struct tarn_ns', 'clock_mono') == 328
      and ffi.offsetof('struct tarn_ns', 'fresh_mark') == 360
      and ffi.offsetof('struct tarn_ns', 'fresh_ok') == 368
      and ffi.offsetof('struct tarn_ns', 'fresh_controls') == 376
      and ffi.offsetof('struct tarn_ns', 'fresh_space') == 384, 'native ABI13 layout')
check(not pcall(F.barrier) and not box.is_in_txn(), 'unknown clock fails before owning a transaction')
check(tarn_txn_status('missing') == 'none' and tarn_part_status('missing') == 'none',
      'fresh recovery status does not require clock health')
healthy()
local native_ok, mono, wall, gap, drift = F.native_state()
check(native_ok and mono == 1ULL and wall == 2ULL and gap == 3ULL and drift == 4ULL,
      'clock mirror preserves exact native nanosecond fields')
F.set_clock{native_state = function() return true, 1, 2, 3, 4 end}
check(not F.native_state(), 'inexact clock mirror fields fail closed')
F.set_clock{native_state = function() error('provider unavailable') end}
check(not F.native_state(), 'clock provider errors fail closed')
local full_calls = 0
F.set_clock{
    read_check = function() return nil end,
    check = function() full_calls = full_calls + 1; return {'unavailable', 1213, 'peer certificate unknown'} end,
    read_native_state = function() return true, 5ULL, 6ULL, 7ULL, 8ULL end,
    native_state = function() full_calls = full_calls + 1; return false end,
    policy_ok = function() return true end,
}
check(pcall(F.barrier) and full_calls == 0, 'ordinary read uses local health while peer proof is unknown')
native_ok, mono, wall, gap, drift = F.native_state()
check(native_ok and mono == 5ULL and wall == 6ULL and gap == 7ULL and drift == 8ULL and full_calls == 0,
      'native read mirror prefers local health over full admission certificate')
local observed = 0
F.set_clock{policy_ok = function() return true end,
            read_check = function() observed = observed + 1; error('must not observe in a callback') end}
check(F.controls_callback() == 1 and observed == 0, 'native controls use pure policy eligibility without observing')
F.set_clock{policy_ok = function() return false end}
check(F.controls_callback() == 0, 'native controls reject changed clock policy between sampler ticks')
F.set_clock{policy_ok = function() error('policy unavailable') end}
check(F.controls_callback() == 0, 'native controls contain policy errors')
healthy()
for _, engine in ipairs({'memtx', 'vinyl'}) do
    local sp = box.schema.space.create('test.fresh_' .. engine, {engine = engine})
    sp:create_index('pk'); sp:replace{1, 10}; sp:replace{2, 20}
    local sid = sp.id
    local r = tarn_select(sid, 0, {1}, 'EQ', 1, nil, true, true)
    check(r[1] == 'ok' and r[2][1][2] == 10 and not box.is_in_txn(), engine .. ' autocommit point')
    r = tarn_select_keys(sid, 0, {{1}, {2}}, 1, true, false)
    check(r[1] == 'ok' and #r[2] == 2 and not box.is_in_txn(), engine .. ' key batch')
    r = tarn_select_join({{sid, 0, {{0, 1}}}}, true, false)
    check(r[1] == 'ok' and r[2][1][2][1][2] == 10 and not box.is_in_txn(), engine .. ' grouped read')
    r = tarn_count(sid, 0, {}, 'ALL', nil, true, {}, {}, true, false)
    check(r[1] == 'ok' and r[2] == 2 and not box.is_in_txn(), engine .. ' count')
    r = tarn_select(sid, 0, {1}, 'EQ', 1, nil, true, false, nil, true)
    local stream = id()
    check(r[1] == 'ok' and F.ok(stream) == 1 and F.count == 1, engine .. ' first stream read is certified')
    r = tarn_select(sid, 0, {2}, 'EQ', 1, nil, true, false, nil, false)
    check(r[1] == 'ok' and id() == stream, engine .. ' subsequent read retains exact transaction')
    F.fence_stream()
    check(id() == stream and F.ok(stream) == 1, engine .. ' RF1 final fence preserves original transaction')
    local savepoint = box.savepoint()
    sp:replace{3, 30}; box.rollback_to_savepoint(savepoint)
    check(F.ok(stream) == 1 and sp:get{3} == nil, engine .. ' savepoint handback retains certificate')
    box.rollback()
    check(F.count == 0, engine .. ' full rollback reclaims certificate')
    F.begin_stream(); sp:get{1}; box.commit()
    check(F.count == 0, engine .. ' commit reclaims certificate')
    box.begin{txn_isolation = 'read-confirmed'}; sp:get{1}; local ambient = id()
    check(not pcall(tarn_select, sid, 0, {1}, 'EQ', 1, nil, true, false, nil, false)
          and box.is_in_txn() and id() == ambient and F.count == 0,
          engine .. ' unmarked ambient stream refused without replacement')
    check(not pcall(tarn_validate, 'ambient', {{sid, 0, {1}}}, {}) and id() == ambient,
          engine .. ' late validation cannot certify ambient reads')
    check(not pcall(tarn_commit_local, 'ambient', {}, {{sid, 0, {1}}}, {}) and id() == ambient,
          engine .. ' local commit refuses unmarked ambient transaction')
    local txid = 'unsupported_' .. engine
    local prepared, refusal = pcall(tarn_prepare, txid, 1, {1}, {}, {{sid, 0, {1}}}, {}, require('clock').realtime())
    check((not prepared or type(refusal) == 'table' and refusal[1] == 'expired' and refusal[2] == 1213)
          and box.is_in_txn() and id() == ambient, engine .. ' PREPARE refuses unmarked ambient without a vote')
    local receipt = fiber.new(function()
        box.begin{txn_isolation = 'read-confirmed'}
        local missing = box.space._tarn_part:get{txid} == nil and box.space._tarn_txn:get{txid} == nil
        box.rollback(); return missing
    end)
    receipt:set_joinable(true)
    local inspected, missing = receipt:join()
    check(inspected and missing and id() == ambient, engine .. ' unsupported ambient PREPARE creates no record')
    check(type(tarn_catalog()) == 'table' and id() == ambient and F.ok(ambient) == 0,
          engine .. ' separate metadata snapshot preserves ambient transaction without certifying it')
    box.rollback()
    F.begin_stream(); sp:get{1}
    local writer = fiber.new(function() sp:replace{1, 11} end)
    writer:set_joinable(true); assert(writer:join())
    r = tarn_validate('changed', {{sid, 0, {1}}}, {})
    check(r[1] == 'conflict' and F.count == 0, engine .. ' conflict tracking survives freshness')
end
for _ = 1, 4200 do F.begin_stream(); box.rollback() end
check(F.count == 0, 'abort traffic beyond registry capacity does not leak certificates')
F.begin_stream()
check(F.mark_callback(ffi.cast('uint64_t', id())) == 1
      and F.ok_callback(ffi.cast('uint64_t', id())) == 1
      and F.mark_callback(9007199254740993ULL) == 0, 'native callbacks require the exact current transaction')
box.rollback()
check(F.count == 0, 'native callback certificate reclaimed on empty rollback')
F.begin_stream()
local callback_txn, switches = id(), fiber.self():csw()
check(F.controls_callback() == 1 and F.space_callback(box.space['test.fresh_memtx'].id) == 1
      and F.space_callback(4294967295) == 0 and id() == callback_txn and F.ok(callback_txn) == 1
      and fiber.self():csw() == switches, 'native controls and space callbacks preserve stream and never yield')
box.rollback()
local diagnostic = box.error.new{code = box.error.PROC_LUA, reason = 'preserve native fallback error'}
box.error.set(diagnostic)
local controls = F.controls
F.controls = function() error('control lookup unavailable') end
check(F.controls_callback() == 0 and box.error.last() == diagnostic, 'native callback failure preserves diagnostics')
F.controls = controls
box.error.clear()
-- Exercise actual asynchronous physical spaces under a synthetic RF-many
-- topology decision, without creating a live replication topology.
box.space._cluster:insert{2, tostring(require('uuid').new()), 'freshness_probe_peer'}
check(F.mode() == 3 and not pcall(F.barrier), 'registered RF-many asynchronous configuration fails closed')
box.space._cluster:delete{2}
local mode = F.mode; F.mode = function() return 2 end
check(not pcall(F.space, box.space['test.fresh_memtx']), 'replicated asynchronous user space rejected')
check(not pcall(F.controls), 'existing asynchronous metadata rejected despite configuration intent')
check(F.controls_callback() == 0 and F.space_callback(box.space['test.fresh_memtx'].id) == 0,
      'native callbacks recheck actual asynchronous controls and user spaces')
F.mode = mode
F.set_clock(nil)
check(not pcall(tarn_select, box.space['test.fresh_memtx'].id, 0, {1}, 'EQ', 1, nil, true, true)
      and not box.is_in_txn(), 'strong read denies unknown clock')
check(type(tarn_info()) == 'table', 'diagnostics remain available on clock fault')
healthy()
F.begin_stream()
local demoted = id()
F.set_clock(nil)
local changer = fiber.new(function() box.cfg{read_only = true} end)
changer:set_joinable(true); assert(changer:join())
for _, read in ipairs({F.barrier, F.begin_stream, F.clock_check, function() F.read(false, false) end}) do
    local ok, err = pcall(read)
    check(not ok and err.code == box.error.READONLY and box.is_in_txn() and id() == demoted,
          'known demotion takes priority over unknown clock without replacing ambient transaction')
end
changer = fiber.new(function() box.cfg{read_only = false} end)
changer:set_joinable(true); assert(changer:join())
healthy(); box.rollback()
-- A new generation during the first range request must not relabel its
-- rows as protected by locks from the replacement generation.
local sid = box.space['test.fresh_memtx'].id
for _, range in ipairs({false, true}) do
    tarn_test_hook('lock_reply', 'hold')
    local result
    local reader = fiber.new(function()
        if range then result = tarn_lock_range('lock-generation', {{sid, {1}, 1}}, 'X', {})
        else result = tarn_lock('lock-generation', {{sid, 0, {1}}}, 'X', {read = true}) end
    end)
    reader:set_joinable(true); reader:wakeup()
    check(tarn_test_hook('lock_reply', 'reached')[2] > 0, 'locking read reached reply boundary')
    box.cfg{read_only = true}; fiber.sleep(.01); box.cfg{read_only = false}; fiber.sleep(.01)
    tarn_test_hook('lock_reply', 'release')
    assert(reader:join())
    check(result[1] ~= 'ok' and result[2] == 1213, 'first locking reply refuses changed generation')
    tarn_unlock('lock-generation')
end
print(string.format('read freshness: %d checks passed', checks))
os.exit(0)
