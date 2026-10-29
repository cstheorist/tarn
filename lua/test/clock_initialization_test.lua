-- Actual box.status initial notifications must not revoke init's policy.
local fio, fiber = require('fio'), require('fiber')
local here = fio.dirname(arg[0]); package.path = here .. '/../?.lua;' .. package.path
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir = work, memtx_use_mvcc_engine = true, memtx_memory = 64 * 1048576,
        vinyl_memory = 16 * 1048576, wal_mode = 'fsync', checkpoint_interval = 0, log_level = 3}
local tarn = require('tarn')
local n = 0
local function check(yes, name) assert(yes, name); n = n + 1; print('ok ' .. name) end
local function wait(fn)
    local until_ = fiber.clock() + 3
    repeat if fn() then return true end; fiber.sleep(.01) until fiber.clock() > until_
    return false
end
tarn.init{shard_id = 1, peers = {}, native = false}
local epoch = tarn.clock.epoch
fiber.sleep(.02)
check(tarn.clock.loaded and tarn.clock.epoch == epoch and tarn.clock.read_check() == nil,
      'initial same-status notification preserves loaded policy and read health')
fiber.sleep(2.2)
check(tarn.clock.check({1}) == nil and tarn.clock.fault == nil,
      'a local RF1 writer without a listener retains its own observed clock coverage')
tarn.init{shard_id = 1, peers = {}, native = false}
epoch = tarn.clock.epoch
fiber.sleep(.02)
check(tarn.clock.loaded and tarn.clock.epoch == epoch and tarn.clock.read_check() == nil,
      'reinit same-status notifications do not repeatedly revoke policy')
box.cfg{read_only = true}; fiber.sleep(.02)
check(tarn.clock.epoch > epoch and tarn.clock.read_check() ~= nil,
      'a real read-only transition still invalidates clock admission')
box.cfg{read_only = false}
check(wait(function() return tarn.clock.read_check() == nil and tarn.clock.check({1}) == nil end),
      'real writable restoration re-establishes policy and relevant health')
print(string.format('%d passed, 0 failed', n)); os.exit(0)
