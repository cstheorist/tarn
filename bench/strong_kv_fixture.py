#!/usr/bin/env python3
"""Prepare/read/audit one owned two-shard RF3 strong-KV measurement cohort.

Starts/signals no database services; tests/kv.sh or a private owner supplies
the eight existing children. Setup mutations are sent once. Any setup/study
failure preserves the database and users for the owner's offline evidence.
Known successful cleanup drops only the exact recorded reader UIDs.
"""
import argparse
import hashlib
import json
import os
import platform
import re
import stat
from pathlib import Path
import sys
import time
from types import SimpleNamespace

import strong_kv as controller
import strong_kv_cpu as cpu


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n')
    Path(path).chmod(0o600)


def owned(args):
    work=args.work.resolve()
    ws=work.stat()
    if work!=args.work.absolute() or not stat.S_ISDIR(ws.st_mode) or ws.st_uid!=os.getuid() or stat.S_IMODE(ws.st_mode)!=0o700 or not work.name.startswith('tarn-kv181-'):
        raise ValueError('invalid private original fixture directory')
    for filename in ('owner.json','manifest.json','processes.jsonl','source.json'):
        s=(work/filename).lstat()
        if not stat.S_ISREG(s.st_mode) or s.st_uid!=os.getuid() or stat.S_IMODE(s.st_mode)!=0o600:
            raise ValueError('fixture receipt is not owned mode0600 regular data')
    owner=json.loads((work/'owner.json').read_text())
    manifest=json.loads((work/'manifest.json').read_text())
    processes=[json.loads(line) for line in (work/'processes.jsonl').read_text().splitlines()]
    if owner['backend']!='host' or owner['owner']!='tarn-kv181-host-v1' or owner['workdir']!=str(work):
        raise ValueError('unsupported/mismatched fixture ownership')
    if os.getppid()!=owner['launcher_pid'] or not re.fullmatch('[0-9a-f]{32}',owner.get('ownerNonce','')):
        raise ValueError('measurement must be a direct child of its original launcher')
    if len(processes)!=8 or len({p['pid'] for p in processes})!=8 or {p['node_id'] for p in processes}!=({f's{s}r{r}' for s in (1,2) for r in (1,2,3)}|{'gw1','gw2'}):
        raise ValueError('exact six storage/two gateways required')
    if len(manifest['nodes'])!=6 or {n['pid'] for n in manifest['nodes']}!={p['pid'] for p in processes if p['node_id'].startswith('s')}:
        raise ValueError('six-node broker manifest disagrees with eight-child ledger')
    launcher=dict(pid=owner['launcher_pid'],starttime=owner['launcher_starttime'])
    config=dict(backend='host',launcher=launcher,nonce=owner['ownerNonce'])
    cpu.checked_launcher(config)
    for p in processes:
        if p['ownerNonce']!=owner['ownerNonce'] or p['ppid']!=launcher['pid'] or not Path(p['workdir']).is_relative_to(work):
            raise ValueError('process outside owned fixture')
        cpu.checked_host_process(p,owner['ownerNonce'])
    return owner,processes


PROBE=r'''
local t=rawget(_G,'tarn'); assert(t and box.space._tarn_meta:get{'shard_id'},'startup not complete')
local ffi=require('ffi');pcall(ffi.cdef,'int gettid(void);')
local tid=tonumber(ffi.C.gettid())
local native=tarn_info().native;local n=ffi.cast('struct tarn_ns*',box.func['tarn_storage.ts_state']:call())
local ok,running=pcall(collectgarbage,'isrunning')
local gc=box.NULL;if ok then gc=running end
local p=tarn_clock_probe();local replicas={}
for id,r in pairs(box.info.replication) do
 replicas[#replicas+1]={id=id,uuid=tostring(r.uuid),upstream=r.upstream and r.upstream.status or false,
 downstream=r.downstream and r.downstream.status or false}
end
return {pid=box.info.pid,tx_tid=tid,uuid=tostring(box.info.uuid),shard_id=t.cfg.shard_id,ro=box.info.ro,
 term=box.info.election.term,leader=box.info.election.leader,clock=t.clock.status(),local_ok=p.local_ok,
 read_ready=t.clock.read_check()==nil,read_contract=tarn_read_contract(),queue=box.info.synchro.queue,quorum=box.info.synchro.quorum,
 quorum_config=box.cfg.replication_synchro_quorum,replication_count=#box.cfg.replication,replicas=replicas,
 wal=box.cfg.wal_mode,mvcc=box.cfg.memtx_use_mvcc_engine,sync=t.cfg.sync,test_hooks=t.cfg.test_hooks,
 check_yields=t.cfg.check_yields,check_locks=t.cfg.check_locks,jit_on=jit.status(),gc_running=gc,
 native=native,native_layout=tonumber(n.layout),native_size=ffi.sizeof('struct tarn_ns'),vclock=box.info.vclock,
 memtx_memory=box.cfg.memtx_memory,vinyl_memory=box.cfg.vinyl_memory,vinyl_cache=box.cfg.vinyl_cache}
'''


def qualify(states, processes, leaders):
    if len(states)!=6:
        raise ValueError('incomplete storage observations')
    for p,s in zip(processes,states):
        if s['pid']!=p['pid'] or s['shard_id']!=p['shard_id'] or not s['local_ok']:
            raise ValueError('storage identity/clock not ready')
        if s['wal']!='fsync' or s['quorum']!=2 or s['replication_count']!=3 or not s['sync'] or not s['mvcc'] or s['queue']['len']!=0 or s['read_contract']!=['strong_reads',1]:
            raise ValueError('RF3/Q2/fsync/empty queue required')
        if s['test_hooks'] or s['check_yields'] or s['check_locks'] or not s['jit_on'] or s['gc_running'] is False:
            raise ValueError('normal JIT/GC, hooks/checks disabled required')
        if (s['native_layout'],s['native_size'])!=(14,392):
            raise ValueError('native ABI14/392 required')
        if not s['ro'] and (p['endpoint'] not in leaders or not s['read_ready'] or not isinstance(s['native'],dict) or s['native']['ready']!=1):
            raise ValueError('writable strong/native leader required')
    if {p['endpoint'] for p,s in zip(processes,states) if not s['ro']}!=set(leaders):
        raise ValueError('exactly two expected leaders required')


def complete_cpu(study):
    scopes={'storage_all_processes','storage_all_tx_threads','hot_owner_leader_tx','gateway_all_processes','client_process'}
    if len(study.get('phases',[]))!=72:raise ValueError('incomplete72phase study')
    for phase in study['phases']:
        values=(phase.get('cpu') or {}).get('cpu_counters_ns')
        if not isinstance(values,dict) or not scopes<=values.keys() or any(type(values[s]) is not int or values[s]<0 for s in scopes):
            raise ValueError('CPU publication requires all5 integer CPUscopes in every phase; raw retained')


def test_namespace(args,sdk,replicas,evidence):
    return SimpleNamespace(**{**{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},'sdk':str(sdk),'user':'sb','replica_groups':replicas,'evidence':str(evidence)})


def close_private(test):
    errors=[]
    for attribute,method in [('events','close'),('private','cleanup')]:
        value=getattr(test,attribute,None)
        if value is not None:
            try:getattr(value,method)()
            except Exception as e:errors.append(attribute+' cleanup: '+str(e))
    return errors


def source_after(source):
    rows={}
    for path,expected in source['files'].items():
        p=Path(path)
        try:actual={'sha256':sha(p),'bytes':p.stat().st_size}
        except OSError as e:actual={'error':type(e).__name__+': '+str(e)}
        rows[path]={'expected':expected,'actual':actual,'matched':actual.get('sha256')==expected['sha256'] and actual.get('bytes')==expected['bytes']}
    return {'head':source['head'],'all_matched':all(r['matched'] for r in rows.values()),'files':rows}


def image_engine(values, engine):
    if not values or any(value.get('engine') != engine for value in values):
        raise ValueError('actual corpus engine differs from requested cohort engine')


IMAGE=r'''
local name,tenant,n,payload,hot=...;local sp=assert(box.space[name]);local ffi=require('ffi')
local digest,msgpack=require('digest'),require('msgpack');local encoded={}
assert(sp.is_sync and sp.state.is_sync,'user table is not synchronous')
local format=sp:format();assert(#format==4 and format[1].name=='tenant_id' and format[2].name=='id' and format[3].name=='p' and format[4].name=='bucket_id','physical layout')
assert(#sp.index[0].parts==2 and sp.index[0].parts[1].fieldno==1 and sp.index[0].parts[2].fieldno==2,'native full PK')
box.begin{txn_isolation='read-confirmed'}
local ok,count=pcall(function()
 local count=0
 for _,t in sp.index[0]:pairs() do
  count=count+1;assert(hot and t[1]==tenant and t[2]==count and tostring(t[3])==payload,'exact committed corpus')
  encoded[#encoded+1]=msgpack.encode(t:totable())
 end
 assert(count==(hot and n or 0),'exact complete corpus count');return count
end)
box.rollback();if not ok then error(count) end
return {count=count,sha256=digest.sha256_hex(table.concat(encoded)),sid=sp.id,engine=sp.engine,is_sync=sp.is_sync,
 format=format,primary=sp.index[0].parts,lsn=box.info.lsn,vclock=box.info.vclock,instance_id=box.info.id}
'''


def main(args):
    if args.trials!=6:
        raise ValueError('full balanced cohort requires exactly six trials')
    args.work=args.work.absolute();args.output=args.output.resolve();args.driver=args.driver.resolve()
    owner,processes=owned(args)
    nodes=sorted((p for p in processes if p['node_id'].startswith('s')),key=lambda p:(p['shard_id'],p['replica']))
    gws=sorted((p for p in processes if p['node_id'].startswith('gw')),key=lambda p:p['node_id'])
    replicas=json.loads(args.replicas);leaders=args.shards.split(',');gateways=args.tarn.split(',')
    if len(replicas)!=2 or any(len(g)!=3 for g in replicas) or [p['endpoint'] for p in gws]!=gateways or {p['endpoint'] for p in nodes}!={e for g in replicas for e in g} or len(leaders)!=2:
        raise ValueError('endpoint lists differ from owned eight-child fixture')
    source=json.loads((args.work/'source.json').read_text())
    if not re.fullmatch('[0-9a-f]{40}',source.get('head','')) or not isinstance(source.get('files'),dict):
        raise ValueError('missing source checkout/file receipt')
    for path,record in source['files'].items():
        if sha(path)!=record['sha256']:
            raise ValueError('source/runtime bytes changed before cohort: '+path)
    sdk=args.sdk
    if sdk is None:
        candidates=[Path(p) for p in source['files'] if Path(p).name=='strong_kv']
        if len(candidates)!=1:raise ValueError('provide --sdk or one exact SDK source receipt entry')
        sdk=candidates[0]
    bound={str(Path(path).resolve()):record for path,record in source['files'].items()}
    required=[args.driver.resolve(),Path(sdk).resolve(),Path(__file__).resolve(),Path(cpu.__file__).resolve(),Path(controller.__file__).resolve()]
    required += [Path(p['exe']).resolve() for p in gws]
    native=[Path(p) for p in source['files'] if Path(p).name=='libtarn_storage.so']
    if len(native)!=1:raise ValueError('exact native module receipt required')
    required+=native
    for path in required:
        if str(path.resolve()) not in bound or sha(path)!=bound[str(path.resolve())]['sha256']:
            raise ValueError('unbound candidate artifact/helper: '+str(path))
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tests/sql'))
    from kvtest import Test,ev
    args.output.mkdir(mode=0o700,parents=False,exist_ok=False)
    namespace=test_namespace(args,sdk,replicas,args.output/'setup-events')
    t=None;users=[];removed_users=[];user_attempts=[];success=False;error=None;drop_outcome='not_attempted';summary={'version':1,'accepted':False,'engine':args.engine,'scope':'Endpoint-only matched study, no service lifecycle/signals. One hot tenant; not production/distributed OLTP capacity.','source':source,'candidate_artifacts':{str(p):{'bytes':p.stat().st_size,'sha256':sha(p)} for p in required},'environment':{'platform':platform.platform(),'cpu_count':os.cpu_count(),'affinity':sorted(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else None,'memory':Path('/proc/meminfo').read_text().splitlines()[0] if Path('/proc/meminfo').exists() else None},'files':{}}
    try:
        t=Test.__new__(Test)
        Test.__init__(t,namespace)
        summary.update(database=t.db,reader=t.user)
        for p in nodes:ev(p['endpoint'],"jit.on();collectgarbage('restart');return true",stage='normal_jit_gc_setup')
        states=[ev(p['endpoint'],PROBE,stage='study_preflight') for p in nodes]
        qualify(states,nodes,leaders);dump(args.output/'preflight.json',states)
        for p,s in zip(nodes,states):
            p['tx_tid']=s['tx_tid'];thread=cpu.parse_stat(Path('/proc',str(p['pid']),'task',str(p['tx_tid']),'stat').read_text());p['tx_starttime']=thread['starttime']
        with t.connect(False) as c:
            t.sql(c,'CREATE DATABASE '+t.db,mutation=True)
        for node in leaders:
            user_attempts.append(node);t.record('reader_create_invoke',node=node,reader=t.user)
            result=ev(node,"local u,p=...;assert(not box.schema.user.exists(u));box.schema.user.create(u,{password=p});for _,f in ipairs{'tarn_kv_v1.meta','tarn_kv_v1.get','tarn_kv_v1.get_many'} do box.schema.user.grant(u,'execute','function',f) end;return assert(box.space._user.index.name:get{u}).id",[t.user,t.password],stage='study_reader_create',sensitive=(t.password,))
            if type(result) is not int:raise ValueError('missing exact created reader UID')
            users.append((node,result));t.record('reader_created',node=node,uid=result)
        with t.connect() as c:
            t.sql(c,f'CREATE TABLE t (tenant_id BIGINT NOT NULL,id BIGINT NOT NULL,p VARBINARY(64) NOT NULL,PRIMARY KEY(tenant_id,id)) ENGINE={args.engine} PARTITION BY KEY(tenant_id)',mutation=True)
            for start in range(1,10001,1000):
                rows=[(7,i,'x'*64) for i in range(start,start+1000)]
                with c.cursor() as cur:
                    t.record('insert_batch_invoke',first=start,last=start+999)
                    try:cur.executemany('INSERT INTO t(tenant_id,id,p) VALUES(%s,%s,%s)',rows)
                    except Exception as e:
                        t.record('insert_batch_error',first=start,last=start+999,error={'type':type(e).__name__,'args':[controller.error_argument(v) for v in e.args],'outcome':'unknown_no_replay' if e.args and e.args[0]==1180 else 'error_no_replay'})
                        raise
                    if cur.rowcount!=1000:raise ValueError('unexpected inserted row count')
                    t.record('insert_batch_ack',first=start,last=start+999,affected=cur.rowcount)
        t.grant('t')
        metadata=[t.public(node,'meta',{'version':1,'db':t.db,'name':'t'}) for node in leaders]
        if any(m.get('status')!='ok' for m in metadata) or metadata[0]['table']!=metadata[1]['table']:
            raise ValueError('inconsistent public metadata')
        bucket=__import__('movetest').bucket_of(7,metadata[0]['buckets']['count'])
        ranges=[];hot_shard=None
        for m in metadata:
            sid=m['ownership']['shard_id']
            for lo,hi in m['buckets']['owned']:
                ranges.append([lo,hi,sid])
                if lo<=bucket<=hi:hot_shard=sid
        if hot_shard is None:raise ValueError('hot bucket owner absent')
        hot_leader=next(p['node_id'] for p in nodes if p['shard_id']==hot_shard and p['endpoint'] in leaders)
        def images(stage):
            deadline=time.monotonic()+90;last=None
            while time.monotonic()<deadline:
                try:
                    values=[ev(p['endpoint'],IMAGE,[t.db+'.t',7,10000,'x'*64,p['shard_id']==hot_shard],stage='corpus_'+stage) for p in nodes]
                    image_engine(values,args.engine)
                    for shard in (1,2):
                        group=[x for p,x in zip(nodes,values) if p['shard_id']==shard]
                        if len({(x['count'],x['sha256'],x['sid'],x['engine'],x['is_sync']) for x in group})!=1:raise ValueError('replicas have not caught up')
                        leader=next(x for p,x in zip(nodes,values) if p['shard_id']==shard and p['endpoint'] in leaders)
                        origin=str(leader['instance_id']);target=int(leader['vclock'].get(origin,0))
                        if target<=0 or any(int(x['vclock'].get(origin,0))<target for x in group):raise ValueError('replica vclock not at recorded leader prefix')
                    return values
                except Exception as e:
                    last=type(e).__name__+': '+str(e);time.sleep(.1)
            raise RuntimeError('read-only corpus/catchup deadline: '+str(last))
        before=images('before');dump(args.output/'corpus-before.json',before)
        config={'version':1,'backend':'host','root':str(args.output),'nonce':owner['ownerNonce'],'launcher':{'pid':owner['launcher_pid'],'starttime':owner['launcher_starttime']},'controller':cpu.host_identity(os.getpid()),'controller_path':str(Path(__file__).resolve()),'driver_path':str(args.driver),'nodes':nodes,'gateways':[{'process':g,'identity':cpu.host_identity(g['pid'])} for g in gws],'hot_leader':hot_leader}
        dump(args.output/'cpu-config.json',config)
        corpus={'version':1,'seeds':[{'id':m['ownership']['shard_id'],'replicas':replicas[i]} for i,m in enumerate(metadata)],'corpus':{'db':t.db,'table':'t','tenant':7,'rows':10000,'payload_hex':('x'*64).encode().hex(),'space_id':metadata[0]['table']['sid'],'physical_fields':{'tenant':0,'id':1,'payload':2,'bucket':3},'tuple_fields':4},'routing':{'bucket_count':metadata[0]['buckets']['count'],'ranges':ranges,'raw_endpoints':[{'shard_id':m['ownership']['shard_id'],'address':leaders[i]} for i,m in enumerate(metadata)]},'source':{'cohort_head':source['head'],'cohort_source_sha256':sha(args.work/'source.json'),'driver_sha256':sha(args.driver),'table_token':metadata[0]['table'],'engine':args.engine,'replicas':replicas}}
        dump(args.output/'benchmark-manifest.json',corpus)
        sql_credentials=Path(t.private.name)/'sql-credentials.json';dump(sql_credentials,{'user':'sb','password':''})
        run=SimpleNamespace(manifest=args.output/'benchmark-manifest.json',driver=args.driver,sql=gateways[0],sql_credentials_file=sql_credentials,credentials_file=t.credentials,python_path=None,output=args.output/'results',cpu_observer=json.dumps([sys.executable,str(Path(__file__).with_name('strong_kv_cpu.py').resolve()),str(args.output/'cpu-config.json')]),seconds=args.seconds,warmup=args.warmup,seed=181)
        study=controller.run(run)
        complete_cpu(study)
        after=images('after');dump(args.output/'corpus-after.json',after)
        if [(v['count'],v['sha256'],v['sid'],v['engine']) for v in before]!=[(v['count'],v['sha256'],v['sid'],v['engine']) for v in after]:raise ValueError('committed corpus changed')
        final=[ev(p['endpoint'],PROBE,stage='study_postflight') for p in nodes];qualify(final,nodes,leaders);dump(args.output/'postflight.json',final)
        for b,a in zip(states,final):
            if (b['uuid'],b['pid'],b['term'],b['leader'],b['ro'])!=(a['uuid'],a['pid'],a['term'],a['leader'],a['ro']):raise ValueError('replica leader/term identity changed')
        if not study['accepted']:raise ValueError('study did not complete')
        binding=source_after(source);dump(args.output/'source-after.json',binding)
        if not binding['all_matched']:raise ValueError('source/runtime/helper bytes changed across cohort')
        for node,uid in users:
            ev(node,"local u,id=...;local r=box.space._user.index.name:get{u};assert(r and r.id==id,'reader UID changed');box.schema.user.drop(u);return true",[t.user,uid],stage='study_exact_uid_cleanup')
            removed_users.append((node,uid))
        drop_outcome='unknown_no_replay'
        with t.connect(False) as c:t.sql(c,'DROP DATABASE '+t.db,mutation=True)
        drop_outcome='acknowledged'
        success=True;summary.update(accepted=True,cleanup='exact UID users and known database removed',phases=len(study['phases']))
    except Exception as e:
        error=type(e).__name__+': '+str(e);summary.update(error=error,database_drop_outcome=drop_outcome,user_create_attempts=user_attempts,known_created_users=users,confirmed_dropped_users=removed_users)
        raise
    finally:
        cleanup_errors=close_private(t)
        if cleanup_errors:success=False;summary['cleanup_errors']=cleanup_errors
        summary['files']={str(p.relative_to(args.output)):{'bytes':p.stat().st_size,'sha256':sha(p)} for p in sorted(args.output.rglob('*')) if p.is_file() and p.name!='fixture.json'}
        summary['accepted']=success;dump(args.output/'fixture.json',summary)
        if cleanup_errors and error is None:raise RuntimeError('private cleanup failed; original receipts retained')
    return summary


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('work','driver','output'):p.add_argument('--'+name,type=Path,required=True)
    for name in ('tarn','shards','replicas'):p.add_argument('--'+name,required=True)
    p.add_argument('--engine',choices=['memtx','vinyl'],required=True);p.add_argument('--sdk',type=Path)
    p.add_argument('--trials',type=int,default=6);p.add_argument('--seconds',type=float,default=5);p.add_argument('--warmup',type=float,default=1)
    main(p.parse_args())
