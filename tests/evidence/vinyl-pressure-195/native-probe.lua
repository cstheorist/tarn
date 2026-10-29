local fiber,json,digest=require('fiber'),require('json'),require('digest')
box.cfg{memtx_memory=64*1024*1024,vinyl_memory=8*1024*1024,vinyl_cache=0,vinyl_timeout=.25,vinyl_read_threads=1,vinyl_write_threads=2,wal_mode='write',log_level=2}
local s=box.schema.space.create('p',{engine='vinyl'});s:format{{'id','unsigned'},{'payload','string'}}
local pk=s:create_index('primary')
for i=1,512 do s:insert{i,digest.base64_encode(digest.urandom(6144),{urlsafe=true})} end
box.snapshot()
while box.stat.vinyl().scheduler.compaction_queue>0 do fiber.sleep(.01) end
for i=1,32 do s:insert{1000+i,digest.base64_encode(digest.urandom(6144),{urlsafe=true})} end
box.snapshot()
print(json.encode{stage='two_runs',scheduler=box.stat.vinyl().scheduler,index=pk:stat()})
box.cfg{snap_io_rate_limit=.5}
pk:compact()
for i=1,15 do print(json.encode{stage='limited',sample=i,scheduler=box.stat.vinyl().scheduler,index=pk:stat()});fiber.sleep(.2) end
box.cfg{snap_io_rate_limit=0}
os.exit()
