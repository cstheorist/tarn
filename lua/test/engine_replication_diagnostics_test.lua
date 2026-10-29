-- Live RF3 client control, run after the ordinary fixture's setup stage.
-- ENGINE_RF3_COPY_CONTROL=source-binding uses the actual engine admission
-- refusal; exception/stall are explicitly synthetic worker observations.
-- Every case retains the ordinary 30-second hook deadline and performs no
-- BUILD retry. The caller owns/retains all three processes and their WAL.
assert(os.getenv('ENGINE_RF3_COPY_CONTROL'), 'explicit copy control required')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
arg[1] = 'copy-control'
dofile(here .. 'engine_replication_test.lua')
