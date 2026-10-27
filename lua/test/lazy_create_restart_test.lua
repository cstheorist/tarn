-- A native target space can exist before its first index/descriptor.
local fio,fiber,ffi=require('fio'),require('fiber'),require('ffi')
local here=debug.getinfo(1,'S').source:sub(2):match('(.*/)') or './'
package.path=here..'../?.lua;'..package.path
local work,engine,stage=assert(arg[1]),assert(arg[2]),assert(arg[3]);fio.mktree(work)
box.cfg{work_dir=work,memtx_use_mvcc_engine=true,memtx_memory=128*1024*1024,
    vinyl_memory=32*1024*1024,vinyl_cache=0,wal_mode='fsync',checkpoint_interval=0,
    txn_isolation='read-confirmed',log_level=3}
local tarn=require('tarn');tarn.lazy.paused=true
tarn.init{shard_id=1,peers={},native=false,test_hooks=true,check_yields=true}
fiber.sleep(.03);assert(tarn.lazy.quota.ready())
local sid,gen,name=68001,68002,'test.lazy_create'
local function ok(r) assert(r[1]=='ok',require('json').encode(r));return r end
local a
if stage=='crash' then
    ok(tarn_bucket_init(16,{{1,16}}))
    a={timeout=5,def={id=sid,name=name,ver=0,engine=engine,json='{}',
        format={{name='id',type='unsigned'},{name='v',type='string'},{name='bucket_id',type='unsigned'}},
        indexes={{name='primary',unique=true,parts={{1,'unsigned'}}},
          {name='bucket_id',unique=false,parts={{3,'unsigned'}}},{name='v',unique=false,parts={{2,'string'}}}},
        lazy={{gen=gen,base_space_id=sid,local_index_id=2,route_parts=1,key_fields={2},key_types={'string'},
          pk_fields={1},pk_native_fields={1},pk_types={'unsigned'},bucket_field=3}}}}
    box.space._tarn_meta:replace{'test.lazy.create_args',a}
    ok(tarn_test_hook('lazy_target_space','hold'))
    fiber.create(function() tarn_create_prepare(name,a,1) end)
    assert(tarn_test_hook('lazy_target_space','reached')[2]==1)
    assert(box.space[gen]~=nil and box.space[gen].index[0]==nil and box.space._tarn_lazy_defs:get{gen}==nil)
    assert(box.space._tarn_catalog:get{name}==nil)
    print('  ok   '..engine..': SIGKILL after target space creation before indexes/descriptor');io.stdout:flush()
    pcall(ffi.cdef,'int getpid(void); int kill(int pid,int sig);');ffi.C.kill(ffi.C.getpid(),9);error('SIGKILL returned')
elseif stage=='finish' then
    a=box.space._tarn_meta:get{'test.lazy.create_args'}.value
    assert(box.space[gen].index[0]==nil)
    ok(tarn_create_prepare(name,a,1))
    repeat local r=ok(tarn_lazy_bootstrap(name,a,1));if r[2] then break end until false
    ok(tarn_lazy_ready(name,a,1));ok(tarn_create_apply(name,a))
    ok(tarn_autocommit({{'insert',sid,{1,'created',1}}},{fence={sid,0},wb={1}}))
    ok(tarn_lazy_control('resume'))
    local deadline=fiber.clock()+5
    while tarn_lazy_stats()[2].depth>0 and fiber.clock()<deadline do fiber.sleep(.01) end
    assert(tarn_lazy_stats()[2].depth==0 and box.space[gen].index.ordered:count{true,'created'}==1)
    print('  ok   '..engine..': retry repairs every native target index and resumes confirmed delivery')
else error('unknown stage') end
os.exit(0)
