#!/usr/bin/env python3
"""Owned real-Tarn file-backup/restore and replica-seed fixture, plus paired timings.

The large variant needs space for RF replicas, one backup and one candidate.
No process outside this fixture is signalled or data directory removed.
"""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import time
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--work', required=True)
    ap.add_argument('--rows', type=int, default=256)
    ap.add_argument('--payload', type=int, default=4096)
    ap.add_argument('--repetitions', type=int, default=3)
    ap.add_argument('--rate-mib', type=float, default=0)
    ap.add_argument('--replicas', type=int, choices=[3], default=3)
    ap.add_argument('--min-backup-bytes', type=int, default=0)
    ap.add_argument('--output', required=True)
    ap.add_argument('--functional', action='store_true')
    args = ap.parse_args()
    assert args.rows > 0 and args.payload >= 1024 and args.repetitions >= 1
    work = pathlib.Path(args.work).absolute();work.mkdir(mode=0o700)
    T = os.getenv('TARANTOOL','tarantool')
    processes=[];logs=[];ports=[]
    # Reserve a private port range chosen by the OS; refuse all collisions.
    sockets=[]
    for _ in range(args.replicas+1):
        s=socket.socket();s.bind(('127.0.0.1',0));ports.append(s.getsockname()[1]);sockets.append(s)
    for s in sockets:s.close()
    uris=['tarn:tarn@127.0.0.1:'+str(p) for p in ports]
    peers=json.dumps({'1':uris[:args.replicas]})
    revision=os.getenv('TARN_SOURCE_REVISION') or subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    record={'format':'tarn-backup-study-v1','source_revision':revision,
            'source_sha256':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in
                             [ROOT/'scripts/backup.py',ROOT/'scripts/backup_rpc.lua',ROOT/'tests/backup.py',*sorted((ROOT/'lua').glob('*.lua'))]},
            'settings':vars(args),'samples':[],'boundary':'owned same-host component; RF and resources explicit; no cross-shard snapshot or production-capacity claim'}
    def save():
        pathlib.Path(args.output).write_text(json.dumps(record,indent=2)+'\n')
    def run(cmd,env=None,okay=True):
        r=subprocess.run(cmd,cwd=ROOT,env=env,capture_output=True,text=True,timeout=1800)
        if okay and r.returncode:raise RuntimeError('command failed: '+str(cmd)+'\n'+r.stdout+'\n'+r.stderr[-6000:])
        return r
    def rpc(i,code):
        path=work/('request-'+uuid.uuid4().hex+'.lua');path.write_text(code)
        try:return json.loads(run([T,str(ROOT/'tests/backup_rpc.lua'),uris[i],str(path)]).stdout)
        finally:path.unlink()
    def await_rpc(i,code,seconds=60):
        deadline=time.monotonic()+seconds;last=None
        while time.monotonic()<deadline:
            try:
                last=rpc(i,code)
                if last:return last
            except (RuntimeError,json.JSONDecodeError) as e:last=str(e)[-1000:]
            time.sleep(.1)
        raise RuntimeError('fixture did not become ready: '+str(last))
    def start(i,directory,identity=None):
        env=dict(os.environ,TARN_SHARD_ID='1',TARN_LISTEN='127.0.0.1:'+str(ports[i]),TARN_WORKDIR=str(directory),
                 TARN_MEMORY_MB='128',TARN_VINYL_MEMORY_MB='64',TARN_VINYL_CACHE_MB='0',TARN_VINYL_TIMEOUT='5',
                 TARN_LOG_LEVEL='3',TARN_WAL_MODE='fsync',TARN_CHECK_YIELDS='1',TARN_CHECK_LOCKS='1',
                 TARN_HOST_INSTANCES=str(args.replicas+1),TARN_VINYL_READ_THREADS='1',TARN_VINYL_WRITE_THREADS='2',
                 TARN_REPLICAS=json.dumps(uris[:args.replicas] if i<args.replicas else uris),TARN_PEERS=peers,
                 TARN_ELECTION_MODE='candidate' if i==0 else 'voter')
        if identity:env['TARN_INSTANCE_UUID']=identity
        f=(work/('node'+str(i)+'-'+uuid.uuid4().hex+'.log')).open('w');logs.append(f)
        p=subprocess.Popen([T,str(ROOT/'lua/storage.lua')],env=env,stdout=f,stderr=subprocess.STDOUT)
        processes.append(p)
        return p
    def stop(p):
        if p.poll() is None:
            p.terminate()
            try:p.wait(timeout=10)
            except subprocess.TimeoutExpired:p.kill();p.wait()
    def probe_start(i):
        rpc(i,"""
assert(rawget(_G,'__backup_probe')==nil)
local p={active=true,samples={},errors={}};_G.__backup_probe=p
require('fiber').create(function()
 while p.active do
  local begin=require('clock').monotonic()
  local v=box.space[88002]:get{2}.v+1
  local good,r=pcall(tarn_autocommit,{{'replace',88002,{2,v,1}}},{fence={88002,0},wb={1}})
  p.samples[#p.samples+1]=(require('clock').monotonic()-begin)*1000
  if not good or r[1]~='ok' then p.errors[#p.errors+1]=tostring(r);p.active=false end
  if #p.samples>=120000 then p.errors[#p.errors+1]='sample bound reached';p.active=false end
  require('fiber').sleep(.02)
 end
 p.done=true
end)
return true
""")
    def probe_stop(i):
        samples=rpc(i,"local p=assert(__backup_probe);p.active=false;while not p.done do require('fiber').sleep(.001) end;__backup_probe=nil;return p")
        assert not samples['errors'],samples
        return samples
    try:
        base_processes=[start(i,work/('source'+str(i))) for i in range(args.replicas)]
        for i in range(args.replicas):await_rpc(i,'return rawget(_G,"tarn")~=nil')
        leader=None
        deadline=time.monotonic()+60
        while leader is None and time.monotonic()<deadline:
            for i in range(args.replicas):
                ready=rpc(i,"if box.info.ro or box.info.id==0 then return false end; "
                          +("if box.info.election.leader~=box.info.id or box.info.synchro.queue.owner~=box.info.id then return false end; " if args.replicas>1 else "")
                          +"local r=tarn_autocommit({},{});return r[1]=='ok'")
                if ready:leader=i;break
            if leader is None:time.sleep(.1)
        assert leader is not None,'no stable writable source'
        print('READY source RF'+str(args.replicas),flush=True)
        seeded_uuid=str(uuid.uuid4());candidate=args.replicas
        candidate_name='shard1_127_0_0_1_'+str(ports[candidate])
        assert leader==0,'fixture pins one leader; all other sources are voters'
        rpc(leader,"box.space._cluster:auto_increment{%s,%s};return true"%(json.dumps(seeded_uuid),json.dumps(candidate_name)))
        for i in range(args.replicas):rpc(i,'box.cfg{checkpoint_interval=2,checkpoint_count=1};return true')
        rpc(leader,"""
local function good(r) assert(r[1]=='ok',require('json').encode(r)) end
good(tarn_bucket_init(16,{{1,16}}))
local function create(id,name,engine,format)
 good(tarn_create_table{id=id,name=name,ver=0,engine=engine,json='{}',lazy={},format=format,
 indexes={{name='primary',unique=true,parts={{1,'unsigned'}}}, {name='bucket_id',unique=false,parts={{3,'unsigned'}}}}})
end
create(88001,'test.backup_data','vinyl',{{name='id',type='unsigned'},{name='v',type='string'},{name='bucket_id',type='unsigned'}})
create(88002,'test.backup_probe','memtx',{{name='id',type='unsigned'},{name='v',type='unsigned'},{name='bucket_id',type='unsigned'}})
good(tarn_autocommit({{'insert',88002,{1,0,1}},{'insert',88002,{2,0,1}}},{fence={88002,0},wb={1}}))
return true
""")
        print('LOAD',args.rows*args.payload,'payload bytes',flush=True)
        loaded=rpc(leader,"""
local digest,fiber=require('digest'),require('fiber')
local rows,payload=%d,%d
local batch={}
local throttled=0
local function publish(writes)
 local deadline=fiber.clock()+60
 repeat
  local r=tarn_autocommit(writes,{fence={88001,0},wb={1}})
  if r[1]=='ok' then return end
  -- This exact refusal certifies that admission precedes every effect.
  -- Split oversized bursts; never retry any transport/unknown outcome.
  assert(r[1]=='throttled' and r[2]==1205,require('json').encode(r))
  throttled=throttled+1
  if #writes>1 then
   local left,right={},{}
   for i,w in ipairs(writes) do if i<=math.floor(#writes/2) then left[#left+1]=w else right[#right+1]=w end end
   publish(left);publish(right);return
  end
  assert(fiber.clock()<deadline,'fixture write admission did not recover')
  fiber.sleep(.05)
 until false
end
for i=1,rows do
 batch[#batch+1]={'insert',88001,{i,digest.urandom(payload),1}}
 if #batch>=8 or i==rows then
  publish(batch);batch={};fiber.sleep(.001)
 end
end
box.snapshot();return {rows=box.space[88001].index[0]:count(),version=box.info.version,uuid=tostring(box.info.uuid),vclock=box.info.vclock,throttled=throttled}
"""%(args.rows,args.payload))
        record['load']=loaded;print('LOADED',loaded,flush=True)
        # Restore normal checkpoint policy after the bulk loader; checkpoint
        # every source to release its initial-load WAL before copying data.
        for i in range(args.replicas):rpc(i,'box.cfg{checkpoint_interval=3600,checkpoint_count=2};return true')
        for _ in range(3):
            rpc(leader,"box.space._tarn_meta:replace{'test.backup.checkpoint',require('clock').realtime()};return true")
            for i in range(args.replicas):rpc(i,'box.snapshot();return true')
        source=work/('source'+str(leader))
        for i in range(args.replicas):await_rpc(i,'return box.space[88001]~=nil and box.space[88001].index[0]:count()=='+str(args.rows))
        digest_code="""
local d=require('crypto').digest.sha256.new();local n=0
for _,t in box.space[88001]:pairs() do d:update(require('msgpack').encode(t:totable()));n=n+1; if n%128==0 then require('fiber').sleep(0);collectgarbage('step',256) end end
return {rows=n,sha256=require('digest').base64_encode(d:result()),uuid=tostring(box.info.uuid),vclock=box.info.vclock}
"""
        expected=rpc(leader,digest_code);record['fixture']=expected
        record['environment']={'platform':sys.platform,'uname':list(os.uname()),'native':os.getenv('TARN_NATIVE','default'),
                               'replication_contract':'three live synchronous source replicas; registered fourth candidate starts as a voter; no failover claim',
                               'foreground':'internal storage autocommit, one small row, 20ms think time; excludes client/network/SQL latency',
                               'cpu_limit':pathlib.Path('/sys/fs/cgroup/cpu.max').read_text().strip() if pathlib.Path('/sys/fs/cgroup/cpu.max').exists() else None,
                               'memory_limit':pathlib.Path('/sys/fs/cgroup/memory.max').read_text().strip() if pathlib.Path('/sys/fs/cgroup/memory.max').exists() else None}
        native=pathlib.Path(os.getenv('TARN_NATIVE','0'))
        if native.is_file():record['environment']['native_sha256']=hashlib.sha256(native.read_bytes()).hexdigest()
        probe_start(leader);time.sleep(3);record['foreground_baseline']=probe_stop(leader)
        print('DIGEST',expected['sha256'],flush=True)
        env=dict(os.environ,TARN_BACKUP_URI=uris[leader],TARANTOOL=T)
        backup=work/'backup'
        cmd=[sys.executable,str(ROOT/'scripts/backup.py'),'create','--source',str(source),'--output',str(backup),
             '--seed-uuid',seeded_uuid,'--rate-mib',str(args.rate_mib)]
        probe_start(leader)
        at=time.monotonic();manifest=json.loads(run(cmd,env=env).stdout);record['backup']=manifest
        record['foreground_backup']=probe_stop(leader)
        record['backup_wall_s']=time.monotonic()-at
        assert manifest['bytes']>=args.min_backup_bytes,(manifest['bytes'],args.min_backup_bytes)
        assert any(x['path'].endswith('.run') for x in manifest['files'])
        print('BACKUP',manifest['bytes'],'bytes',round(record['backup_wall_s'],3),'seconds',flush=True);save()
        record['backup_runs']=[dict(manifest=manifest,wall_s=record['backup_wall_s'],foreground=record['foreground_backup'])]
        for rep in range(1,args.repetitions):
            repeated=work/('backup-repeat-'+str(rep+1))
            repeated_cmd=list(cmd);repeated_cmd[repeated_cmd.index('--output')+1]=str(repeated)
            probe_start(leader);at=time.monotonic()
            repeated_manifest=json.loads(run(repeated_cmd,env=env).stdout)
            wall=time.monotonic()-at;foreground=probe_stop(leader)
            record['backup_runs'].append(dict(manifest=repeated_manifest,wall_s=wall,foreground=foreground))
            save();print('backup',rep+1,round(wall,3),'s',flush=True)
            shutil.rmtree(repeated)
        # Verify an isolated original-identity restore, including every tuple byte.
        restored=work/'isolated'
        run([sys.executable,str(ROOT/'scripts/backup.py'),'restore','--backup',str(backup),'--output',str(restored),'--rate-mib','0'])
        offline=work/'offline.lua'
        offline.write_text("box.cfg{work_dir="+json.dumps(str(restored))+",memtx_memory=134217728,vinyl_memory=67108864,vinyl_cache=0,read_only=true,log_level=2};print(require('json').encode((function() "+digest_code+" end)()));os.exit()")
        check=json.loads(run([T,str(offline)]).stdout);assert (check['rows'],check['sha256'])==(expected['rows'],expected['sha256'])
        record['isolated_restore']=check;shutil.rmtree(restored)
        if args.functional:
            # Refuse incomplete/corrupt/traversal manifests; never publish a restore marker.
            bad=work/'incomplete';bad.mkdir();(bad/'INCOMPLETE').touch()
            r=run([sys.executable,str(ROOT/'scripts/backup.py'),'restore','--backup',str(bad),'--output',str(work/'bad-out')],okay=False);assert r.returncode
            corrupt=backup/'files'/manifest['files'][-1]['path']
            with corrupt.open('r+b') as f:f.seek(-1,2);old=f.read(1);f.seek(-1,2);f.write(bytes([old[0]^1]))
            r=run([sys.executable,str(ROOT/'scripts/backup.py'),'restore','--backup',str(backup),'--output',str(work/'corrupt-out')],okay=False);assert r.returncode and not (work/'corrupt-out'/'restore.json').exists()
            with corrupt.open('r+b') as f:f.seek(-1,2);f.write(old)
            record['negative_controls']=['incomplete backup refused','corruption refused without complete marker']
            manifest_path=backup/'manifest.json';saved=manifest_path.read_text()
            unsafe=json.loads(saved);unsafe['files'][0]['path']='../outside.snap';manifest_path.write_text(json.dumps(unsafe))
            r=run([sys.executable,str(ROOT/'scripts/backup.py'),'restore','--backup',str(backup),'--output',str(work/'traversal-out')],okay=False)
            assert r.returncode and not (work/'traversal-out').exists();manifest_path.write_text(saved)
            record['negative_controls'].append('manifest traversal refused before creating output')
            # A second agent must not release the first agent's GC lease.
            sys.path.insert(0,str(ROOT/'scripts'));import backup as agent
            os.environ['TARN_BACKUP_URI']=uris[leader]
            bridge=agent.Bridge(T,'TARN_BACKUP_URI');token=str(uuid.uuid4())
            try:
                bridge.call('start',token,60,None)
                r=run(cmd[:cmd.index('--output')]+['--output',str(work/'competing-backup')],env=env,okay=False)
                assert r.returncode and bridge.call('check',token)
            finally:bridge.call('stop',token);bridge.close()
            expired=work/'expired-backup'
            r=run([sys.executable,str(ROOT/'scripts/backup.py'),'create','--source',str(source),'--output',str(expired),
                   '--ttl','1','--rate-mib','.25'],env=env,okay=False)
            assert r.returncode and (expired/'INCOMPLETE').exists() and not (expired/'manifest.json').exists()
            record['negative_controls']+=['competing agent preserves original lease','expired lease never publishes a manifest']
            interrupted=work/'interrupted-backup'
            p=subprocess.Popen([sys.executable,str(ROOT/'scripts/backup.py'),'create','--source',str(source),'--output',str(interrupted),
                                '--ttl','2','--rate-mib','.01'],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            processes.append(p)
            await_rpc(leader,'return box.backup.info()~=nil')
            p.kill();p.wait()
            await_rpc(leader,'return box.backup.info()==nil',seconds=10)
            assert (interrupted/'INCOMPLETE').exists() and not (interrupted/'manifest.json').exists()
            record['negative_controls'].append('killed copier leaves incomplete files and GC lease expires')
            denied=dict(os.environ,TARN_WORKDIR=str(interrupted),TARN_NATIVE='0')
            r=run([T,str(ROOT/'lua/storage.lua')],env=denied,okay=False)
            assert r.returncode and 'incomplete' in r.stderr
            record['negative_controls'].append('storage refuses incomplete data directory')
        # Each candidate uses the exact same address/registered identity; only owned stopped data is reset.
        for rep in range(args.repetitions):
            labels=['full_join','backup_seed'] if rep%2==0 else ['backup_seed','full_join']
            for label in labels:
                directory=work/('candidate-'+str(rep)+'-'+label)
                started=time.monotonic();copy_s=0
                probe_start(leader)
                if label=='backup_seed':
                    run([sys.executable,str(ROOT/'scripts/backup.py'),'restore','--backup',str(backup),'--output',str(directory),
                         '--seed-uuid',seeded_uuid,'--rate-mib',str(args.rate_mib)])
                    copy_s=time.monotonic()-started
                if args.functional and label=='backup_seed' and rep==0:
                    denied=dict(os.environ,TARN_WORKDIR=str(directory),TARN_REPLICAS=json.dumps(uris),
                                TARN_INSTANCE_UUID=seeded_uuid,TARN_ELECTION_MODE='candidate')
                    refusal=run([T,str(ROOT/'lua/storage.lua')],env=denied,okay=False)
                    assert refusal.returncode and 'requires' in refusal.stderr
                    record['negative_controls'].append('seed refuses candidate mode before operator catch-up approval')
                # A post-backup committed mutation must be fetched from WAL, not already in the files.
                tail=rep*2+(0 if label=='full_join' else 1)+1
                rpc(leader,"local r=tarn_autocommit({{'replace',88002,{1,%d,1}}},{fence={88002,0},wb={1}});assert(r[1]=='ok');return true"%tail)
                p=start(candidate,directory,seeded_uuid)
                try:
                    info=await_rpc(candidate,"return rawget(_G,'tarn')~=nil and box.space[88002]~=nil and box.space[88002]:get{1}.v==%d and {uuid=tostring(box.info.uuid),ro=box.info.ro,vclock=box.info.vclock,rows=box.space[88001].index[0]:count()}"%tail,seconds=1800)
                    elapsed=time.monotonic()-started
                    foreground=probe_stop(leader)
                    assert info['ro'] and info['uuid']==seeded_uuid and info['rows']==args.rows,info
                    check=rpc(candidate,digest_code);assert check['sha256']==expected['sha256'] and check['rows']==expected['rows']
                    sample={'repetition':rep+1,'label':label,'elapsed_s':elapsed,'seed_copy_s':copy_s,'start_follow_s':elapsed-copy_s,
                            'physical_backup_bytes':manifest['bytes'],'seconds_per_100GB_linear_normalization':elapsed*100000000000/manifest['bytes'],
                            'verified':check,'tail_value':tail,'state':info,'foreground':foreground}
                    record['samples'].append(sample);save();print(label,rep+1,round(elapsed,3),'s',flush=True)
                finally:stop(p)
                shutil.rmtree(directory)
        record['completed']=True;save();print('ALL BACKUP/RESTORE/SEED CHECKS PASSED',flush=True)
    except BaseException as error:
        record['error']=str(error);record['completed']=False;save();raise
    finally:
        for p in processes:stop(p)
        for f in logs:f.close()


if __name__=='__main__':main()
