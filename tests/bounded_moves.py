#!/usr/bin/env python3
"""Owned same-host RF3/fsync movement study; bounded staging is not an RSS bound."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import socket
import subprocess
import sys
import threading
import time
import traceback

import pymysql

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tests/sql'))
from movetest import bucket_of

PAYLOAD_LUA = """
local function payload(id)
 local out={} for j=1,128 do out[j]=require('digest').sha256_hex(tostring(id)..':'..j) end
 return table.concat(out)
end
"""


class RPC:
    def __init__(self, uri, log):
        self.p = subprocess.Popen([os.getenv('TARANTOOL', 'tarantool'), str(ROOT/'tests/bounded_move_rpc.lua'), uri],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log)
        self.pending = b''

    def call(self, code, args=(), timeout=30):
        self.p.stdin.write((json.dumps(dict(code=code, args=args, timeout=timeout))+'\n').encode())
        self.p.stdin.flush()
        end = time.monotonic()+timeout+10
        with selectors.DefaultSelector() as s:
            s.register(self.p.stdout, selectors.EVENT_READ)
            while b'\n' not in self.pending:
                if not s.select(max(0, end-time.monotonic())):
                    raise TimeoutError('administrative RPC outcome unknown')
                data = os.read(self.p.stdout.fileno(), 65536)
                if not data:
                    raise RuntimeError('administrative bridge closed; outcome unknown')
                self.pending += data
                assert len(self.pending) < 8*1024*1024
        line, self.pending = self.pending.split(b'\n', 1)
        r = json.loads(line)
        if not r['ok']:
            raise RuntimeError(r['error'])
        return r.get('result')

    def close(self):
        if self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(5)
            except subprocess.TimeoutExpired:
                self.p.kill(); self.p.wait()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--work', required=True)
    ap.add_argument('--engine', choices=['memtx', 'vinyl', 'mixed'], default='vinyl')
    ap.add_argument('--mib', type=int, default=32, help='payload MiB per moving bucket')
    ap.add_argument('--moves', type=int, choices=[1, 2], default=1)
    ap.add_argument('--fault', choices=['none', 'source', 'receiver', 'coordinator', 'disk', 'reply'], default='none')
    a = ap.parse_args()
    work = Path(a.work).absolute(); work.mkdir(mode=0o700)
    assert a.mib >= 1
    storage, sqlprocs, bridges, logs, threads, proxies = {}, {}, {}, [], [], []
    metrics = []; errors = []; checks = []
    result = dict(format='tarn-bounded-movement-v1', settings=dict(vars(a)), checks=checks, errors=errors,
        source_revision=os.getenv('TARN_SOURCE_REVISION', 'uncommitted'),
        source_sha256={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in
            [ROOT/'tests/bounded_moves.py', ROOT/'tests/bounded_move_rpc.lua', ROOT/'crates/tarn-sql/src/buckets.rs', ROOT/'crates/tarn-sql/src/cluster.rs', ROOT/'crates/tarn-sql/src/compat.rs',
             *sorted((ROOT/'lua').glob('*.lua'))]},
        limits=dict(vinyl_memory=16*1024*1024, vinyl_cache=0, batch_rows=64, batch_bytes=256*1024,
                    delta_keys=64, delta_bytes=64*1024, total_delta_bytes=128*1024,
                    slots=2, staging_bytes=256*1024*1024, total_staging_bytes=512*1024*1024),
        boundary='same-host RF3/fsync, no independent failure domains; RSS includes user data and engine overhead',
        environment=dict(uname=list(os.uname()), python=sys.version), samples=metrics)
    for name in ('cpu.max', 'memory.max', 'memory.events'):
        p = Path('/sys/fs/cgroup')/name
        if p.exists(): result['environment'][name] = p.read_text()
    binary = Path(os.environ['TARN_BIN'])
    native = os.getenv('TARN_NATIVE', '0')
    result['binaries'] = {str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in
                         [binary, Path(native)] if p.is_file()}
    sockets=[];ports=[]
    for _ in range(14 if a.fault == 'reply' else 8):
        s=socket.socket();s.bind(('127.0.0.1',0));ports.append(s.getsockname()[1]);sockets.append(s)
    for s in sockets:s.close()
    uris=['tarn:tarn@127.0.0.1:'+str(p) for p in ports[:6]]
    peers={str(i+1):uris[i*3:i*3+3] for i in range(2)}
    config=work/'cluster.json'
    config.write_text(json.dumps(dict(shards=[dict(id=i+1, replicas=[f'127.0.0.1:{p}' for p in (ports[8+i*3:8+i*3+3] if a.fault=='reply' else ports[i*3:i*3+3])])
        for i in range(2)],bucket_count=16,ddl_job_stale=2, pool_size=4)))

    def save():
        (work/'result.json').write_text(json.dumps(result,indent=2)+'\n')

    def checked(message):
        checks.append(message); print('PASS '+message,flush=True);save()

    def logfile(name):
        f=(work/name).open('a');logs.append(f);return f

    def start_storage(i):
        env=dict(os.environ,TARN_SHARD_ID=str(i//3+1),TARN_LISTEN=f'127.0.0.1:{ports[i]}',
            TARN_WORKDIR=str(work/f'node{i}'),TARN_PEERS=json.dumps(peers),TARN_REPLICAS=json.dumps(peers[str(i//3+1)]),
            TARN_MEMORY_MB='256',TARN_VINYL_MEMORY_MB='16',TARN_VINYL_CACHE_MB='0',TARN_WAL_MODE='fsync',
            TARN_HOST_INSTANCES='6',TARN_VINYL_READ_THREADS='1',TARN_LOG_LEVEL='3',TARN_CHECK_YIELDS='1',TARN_TEST_HOOKS='1',
            TARN_MOVE_BATCH_ROWS='64',TARN_MOVE_BATCH_BYTES='262144',TARN_MOVE_DELTA_KEYS='64',
            TARN_MOVE_DELTA_BYTES='65536',TARN_MOVE_TOTAL_DELTA_BYTES='131072',TARN_MOVE_SLOTS='2',
            TARN_MOVE_STAGING_BYTES='268435456',TARN_MOVE_TOTAL_STAGING_BYTES='536870912')
        f=logfile(f'node{i}.log')
        storage[i]=subprocess.Popen([os.getenv('TARANTOOL','tarantool'),str(ROOT/'lua/storage.lua')],env=env,stdout=f,stderr=f)

    def rpc(i):
        if i not in bridges: bridges[i]=RPC(uris[i],logfile(f'rpc{i}.log'))
        return bridges[i]

    def wait(fn, seconds=60):
        end=time.monotonic()+seconds;last=None
        while time.monotonic()<end:
            try:
                last=fn()
                if last:return last
            except (RuntimeError,OSError,TimeoutError,pymysql.Error) as e:last=repr(e)
            time.sleep(.1)
        raise AssertionError('readiness timeout: '+str(last))

    def start_sql(i):
        f=logfile(f'sql{i}.log')
        sqlprocs[i]=subprocess.Popen([str(binary),'--config',str(config),'--listen',f'127.0.0.1:{ports[6+i]}',
                                     '--threads','2','--node-id',str(1760+i)],stdout=f,stderr=f)

    def query(i, statement, db='bounded'):
        conn=pymysql.connect(host='127.0.0.1',port=ports[6+i],user='t',password='',database=db,autocommit=True,
                             connect_timeout=3,read_timeout=600,write_timeout=120)
        try:
            with conn.cursor() as cur:
                cur.execute(statement);return cur.fetchall() if cur.description else cur.rowcount
        finally:conn.close()

    def leader(shard):
        for i in range((shard-1)*3,shard*3):
            if storage[i].poll() is not None:continue
            try:
                if rpc(i).call("return rawget(_G,'tarn')~=nil and not box.info.ro and tarn.clock.check({1,2})==nil",timeout=2):return i
            except (RuntimeError,TimeoutError,OSError):pass
        return None

    def exact(i, tables, keys, count, expected):
        return rpc(i).call(PAYLOAD_LUA+'''local tables,keys,n,want=... local seen=0
for _,name in ipairs(tables) do local s=box.space[name]
 for _,t in s:pairs() do
  local k,id,value=t[1],t[2],t[3]
  assert(id>=1 and id<=n and value==payload(id),'payload differs')
  local yes=false for _,key in ipairs(keys) do if k==key then yes=true end end assert(yes)
  seen=seen+1
 end
end assert(seen==want,'row count '..seen..' expected '..want) return seen''',[tables,keys,count,expected],timeout=60)

    try:
        for i in range(6):start_storage(i)
        for i in range(6):wait(lambda i=i:rpc(i).call("return rawget(_G,'tarn')~=nil and box.space._tarn_move~=nil",timeout=2))
        ls=[wait(lambda:leader(1) is not None and leader(1)+1)-1,wait(lambda:leader(2) is not None and leader(2)+1)-1]
        for i in range(6):
            rpc(i).call('''__move_events={}
for _,name in ipairs({'tarn_bucket_fence','tarn_bucket_send','tarn_bucket_recv'}) do
 local original=_G[name]
 _G[name]=function(b,...)
  local key=tostring(b)..':'..name local e=__move_events[key] or {}
  e.started=e.started or require('clock').monotonic();__move_events[key]=e
  local r=original(b,...)
  if r[1]=='ok' then e.finished=e.finished or require('clock').monotonic() end
  return r
 end
end return true''')
        if a.fault=='reply':
            for i in range(6):
                f=logfile(f'proxy{i}.log')
                proxies.append(subprocess.Popen([sys.executable,str(ROOT/'tests/proxy.py'),str(ports[8+i]),str(ports[i]),
                    'tarn_bucket_recv_rows'],env=dict(os.environ,TARN_PROXY_ARM_FILE=str(work/'armed')),stdout=f,stderr=f))
            time.sleep(.2)
        for i in range(2):start_sql(i);wait(lambda i=i:query(i,'SELECT 1',None))
        query(0,'CREATE DATABASE bounded',None)
        engines=['memtx','vinyl'] if a.engine=='mixed' else [a.engine]
        tables=['bounded.t'+str(i) for i in range(len(engines))]
        for i,e in enumerate(engines):
            query(0,f'CREATE TABLE t{i} (k INT, id INT, payload TEXT, PRIMARY KEY(k,id)) ENGINE={e} PARTITION BY KEY(k)')
        keys=[];buckets=[]
        for k in range(1,1000):
            b=bucket_of(k,16)
            if b<=8 and b not in buckets:keys.append(k);buckets.append(b)
            if len(keys)==a.moves:break
        count=a.mib*128
        result['keys']=keys;result['buckets']=buckets;result['rows_per_bucket']=count
        for key,b in zip(keys,buckets):
            # Bounded, checked mutations. A failure is never replayed.
            rpc(ls[0]).call(PAYLOAD_LUA+'''local names,k,b,n=...
for first=1,n,4 do local writes={}
 for id=first,math.min(first+3,n) do local s=box.space[names[(id-1)%#names+1]]
  writes[#writes+1]={'insert',s.id,{k,id,payload(id),b}}
 end
 local deadline=require('fiber').clock()+30
 repeat
  local r=tarn_autocommit(writes,{wb={b}})
  if r[1]=='ok' then break end
  assert(r[1]=='throttled' and require('fiber').clock()<deadline,require('json').encode(r))
  -- Admission refused before mutation. This is not transport uncertainty.
  require('fiber').sleep(.05)
 until false
end return true''',[tables,key,b,count],timeout=600)
        checked('RF3/fsync payload loaded through checked mutations')
        for i in ls:rpc(i).call('box.snapshot();return true',timeout=60)
        result['initial_vclocks']={str(i):rpc(i).call('return box.info.vclock') for i in range(6)}
        # Hold at a real phase boundary when injecting a failure, so small
        # datasets cannot accidentally skip the intended fault.
        hook='bucket_rows' if a.fault in ('source','coordinator','disk') else 'move_install_batch'
        hi=ls[0] if hook=='bucket_rows' else ls[1]
        if a.fault not in ('none','reply'):rpc(hi).call('return tarn_test_hook(..., "hold")',[hook])
        if a.fault=='reply':(work/'armed').touch()
        moves={};t0=time.monotonic()
        def move(b):
            try:moves[str(b)]=dict(result=query(0,f'TARN MOVE BUCKET {b} TO SHARD 2'))
            except Exception as e:moves[str(b)]=dict(error=repr(e))
        for b in buckets:
            th=threading.Thread(target=move,args=(b,),daemon=True);threads.append(th);th.start()
        injected=a.fault=='reply';restored=False;restarted=None;disk_retried=False
        if injected:result['fault_at_seconds']=0
        finished=False;fence_start=None;unavailable_start=None;cleanup_start=None
        peak_rss={};peak_disk=0
        while time.monotonic()-t0<240:
            now=time.monotonic()-t0
            sample=dict(seconds=now,states={},staged_bytes=0,rss={},disk_bytes=0)
            for i,p in storage.items():
                if p.poll() is None:
                    try:
                        st=Path(f'/proc/{p.pid}/status').read_text()
                        fields={x.split(':')[0]:x.split(':')[1].strip() for x in st.splitlines() if ':' in x}
                        rss=int(fields['VmHWM'].split()[0])*1024
                        peak_rss[str(i)]=max(peak_rss.get(str(i),0),rss);sample['rss'][str(i)]=rss
                    except (OSError,KeyError):pass
                for file in (work/f'node{i}').rglob('*'):
                    if file.is_file():sample['disk_bytes']+=file.stat().st_blocks*512
            peak_disk=max(peak_disk,sample['disk_bytes'])
            for shard in (1,2):
                li=leader(shard)
                if li is None:continue
                ls[shard-1]=li
                state=rpc(li).call('''local bs=... local out={}
for _,b in ipairs(bs) do local r=box.space._tarn_buckets:get{b};out[tostring(b)]=r and r.state or 'absent' end
return {states=out,movement=tarn_info().movement,jobs=#tarn_jobs(),events=__move_events or {}}''',[buckets],timeout=3)
                sample['states'][str(shard)]=state
                sample['staged_bytes']+=state['movement']['staged_bytes']
            states=[v for s in sample['states'].values() for v in s['states'].values()]
            if 'fenced' in states and fence_start is None:fence_start=now
            if any(s in states for s in ('reclaiming','installing','sent')) and unavailable_start is None:unavailable_start=now
            if 'reclaiming' in states and cleanup_start is None:cleanup_start=now
            metrics.append(sample)
            if a.fault == 'none' and all(not t.is_alive() for t in threads):
                assert all('error' not in r for r in moves.values()), moves
            if a.fault!='none' and not injected:
                reached=rpc(hi).call('return tarn_test_hook(...,"reached")',[hook],timeout=6)
                assert reached[0]=='ok' and reached[1]>=1,reached
                if a.fault in ('source','receiver'):
                    storage[hi].kill();storage[hi].wait(10);bridges.pop(hi).close();restarted=hi
                elif a.fault=='coordinator':sqlprocs[0].kill();sqlprocs[0].wait(10)
                elif a.fault=='disk':
                    rpc(ls[1]).call('tarn.cfg.move_disk_reserve_bytes=9007199254740991;return true')
                injected=True;result['fault_at_seconds']=now
                if a.fault in ('coordinator','disk'):
                    rpc(hi).call('return tarn_test_hook(...,"release")',[hook])
                checked('injected '+a.fault+' at '+hook)
            if injected and not restored and now-result['fault_at_seconds']>3:
                if restarted is not None:start_storage(restarted)
                if a.fault=='coordinator':start_sql(0)
                if a.fault=='disk':rpc(ls[1]).call('tarn.cfg.move_disk_reserve_bytes=1073741824;return true')
                restored=True
            target=sample['states'].get('2',{});source=sample['states'].get('1',{})
            if all(target.get('states',{}).get(str(b))=='active' for b in buckets) and \
                all(source.get('states',{}).get(str(b))=='sent' for b in buckets) and \
                all(s['movement']['active']==0 and s['jobs']==0 for s in sample['states'].values()) and len(sample['states'])==2:
                finished=True;break
            # A pre-flip disk refusal cancels safely. After reclamation, retry a
            # new administrative move explicitly; never replay a user mutation.
            if a.fault=='disk' and restored and not disk_retried and all(not t.is_alive() for t in threads) and \
                all(s['movement']['active']==0 and s['jobs']==0 for s in sample['states'].values()):
                disk_retried=True
                for b in buckets:
                    th=threading.Thread(target=move,args=(b,),daemon=True);threads.append(th);th.start()
            time.sleep(.05)
        assert finished,'move did not complete'
        for th in threads:th.join(5)
        elapsed=time.monotonic()-t0
        events={}
        for sample in metrics:
            for state in sample['states'].values():
                for key, stamps in (state.get('events') or {}).items():
                    out=events.setdefault(key,{})
                    for name,stamp in stamps.items():out[name]=min(out.get(name,stamp),stamp)
        result['procedure_intervals_seconds']={}
        for b in buckets:
            fence=events.get(str(b)+':tarn_bucket_fence',{})
            send=events.get(str(b)+':tarn_bucket_send',{})
            recv=events.get(str(b)+':tarn_bucket_recv',{})
            def interval(end,start):return None if end is None or start is None else end-start
            result['procedure_intervals_seconds'][str(b)]=dict(
                write_fence=interval(recv.get('finished'),fence.get('started')),
                unavailable=interval(recv.get('finished'),send.get('started')),
                source_reclamation=interval(send.get('finished'),send.get('started')),
                target_install_and_cleanup=interval(recv.get('finished'),recv.get('started')))
        result.update(moves=moves,elapsed_seconds=elapsed,peak_rss_bytes=peak_rss,peak_allocated_disk_bytes=peak_disk,
            payload_bytes=a.moves*a.mib*1024*1024,payload_mib_per_second=a.moves*a.mib/elapsed,
            peak_encoded_staging_bytes=max(s['staged_bytes'] for s in metrics),
            sampled_write_fence_seconds=None if fence_start is None else elapsed-fence_start,
            sampled_unavailable_seconds=None if unavailable_start is None else elapsed-unavailable_start,
            sampled_cleanup_seconds=None if cleanup_start is None else elapsed-cleanup_start,
            sampling_note='phase boundaries sampled; durations are lower-bound intervals plus final completion latency; HWM includes load')
        exact(ls[0],tables,keys,count,0);exact(ls[1],tables,keys,count,count*a.moves)
        checked('exact acknowledged rows and payloads present once on destination')
        if a.fault=='reply':
            dropped=sum((work/f'proxy{i}.log').read_text().count('swallowed the reply') for i in range(6))
            assert dropped>=1,'proxy did not swallow an executed page reply'
            result['dropped_page_replies']=dropped
            checked('executed page reply lost; durable receipt recovered without duplicate effects')
        for i in ls:
            state=rpc(i).call('return {tarn_info().movement,box.space._tarn_recv:count(),#tarn_jobs()}')
            assert state[0]['active']==0 and state[0]['staged_bytes']==0 and state[1:]==[0,0],state
            assert state[0]['stats']['max_batch_rows']<=64 and state[0]['stats']['max_batch_bytes']<=262144,state
        checked('owned staging and progress reclaimed; observed batch bounds respected')
        # Re-encode decoded WAL rows, grouped by durable transaction sequence.
        # This measures transaction bytes, excluding physical xlog framing and
        # compression; it is not the WAL writer's combined syscall buffer size.
        result['wal_transactions']={}
        for i in ls:
            result['wal_transactions'][str(i)]=rpc(i).call('''local cut=... local mp=require('msgpack') local active={} local maxbytes,maxrows=0,0
for _,file in ipairs(require('fio').glob(box.cfg.wal_dir..'/*.xlog')) do
 for _,row in require('xlog').pairs(file) do local h=row.HEADER local body=row.BODY
  if h.lsn and h.replica_id and h.lsn>(cut[tostring(h.replica_id)] or cut[h.replica_id] or 0) then
   local id=tostring(h.replica_id)..':'..tostring(h.tsn or h.lsn)
   local tx=active[id] or {bytes=0,rows=0};tx.bytes=tx.bytes+#mp.encode(row);tx.rows=tx.rows+1
   if not h.tsn or h.commit then maxbytes=math.max(maxbytes,tx.bytes);maxrows=math.max(maxrows,tx.rows);active[id]=nil else active[id]=tx end
  end
 end
end return {max_reencoded_bytes=maxbytes,max_rows=maxrows}''',[result['initial_vclocks'][str(i)]],timeout=90)
        result['passed']=True;save();print(json.dumps({k:v for k,v in result.items() if k not in ('samples','source_sha256')},indent=2))
    except BaseException:
        result['passed']=False;errors.append(traceback.format_exc());save();raise
    finally:
        for p in [*sqlprocs.values(),*storage.values(),*proxies]:
            if p.poll() is None:p.terminate()
        for p in [*sqlprocs.values(),*storage.values(),*proxies]:
            try:p.wait(10)
            except subprocess.TimeoutExpired:p.kill();p.wait()
        for b in bridges.values():b.close()
        for f in logs:f.close()


if __name__=='__main__':main()
