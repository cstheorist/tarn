#!/usr/bin/env python3
"""Matched RF3/fsync composite-routing study on an exclusively owned cluster.

Both arms use the same source, binary, schema and clients; only route_parts
changes. This isolates routing from unrelated gateway changes. Each write
sample includes commit, session-frontier catch-up, lazy lookup and base
recheck. No failed/uncertain mutation is replayed. Raw samples are retained.
One host measures contention and costs, not independent failure domains.
"""
import argparse
import concurrent.futures
import hashlib
import json
import math
import os
from pathlib import Path
import random
import subprocess
import threading
import time

from lazytest import connect, ev, q, wait_empty

INSTALL = r'''
local lease = ...
assert(tarn.cfg.test_hooks and not box.info.ro)
assert(rawget(_G,'__lazy185') == nil)
local fiber, clock, mp = require('fiber'), require('clock'), require('msgpack')
local s = {original={},calls={},seconds={},encoded_apply_bytes=0,assignments=0,samples={},reject=false}
local names={'initial','capture','apply','select','frontiers'}
for _,name in ipairs(names) do
    local original=tarn.lazy[name]; s.original[name]=original
    tarn.lazy[name]=function(...)
        s.calls[name]=(s.calls[name] or 0)+1
        if name=='apply' then
            local rows=...; s.assignments=s.assignments+#rows
            s.encoded_apply_bytes=s.encoded_apply_bytes+#mp.encode(rows)
            if s.reject then return {'error',1205,'owned routing study destination outage'} end
        end
        local started=clock.monotonic()
        local out={pcall(original,...)}
        s.seconds[name]=(s.seconds[name] or 0)+clock.monotonic()-started
        if not out[1] then error(out[2]) end
        return unpack(out,2)
    end
end
s.base=tarn_select_keys
rawset(_G,'tarn_select_keys',function(...)
    s.calls.base_fetch=(s.calls.base_fetch or 0)+1
    local started=clock.monotonic(); local out={pcall(s.base,...)}
    s.seconds.base_fetch=(s.seconds.base_fetch or 0)+clock.monotonic()-started
    if not out[1] then error(out[2]) end
    return unpack(out,2)
end)
function s.restore()
    if rawget(_G,'__lazy185')~=s then return end
    for name,original in pairs(s.original) do tarn.lazy[name]=original end
    rawset(_G,'tarn_select_keys',s.base)
    s.reject=false; rawset(_G,'__lazy185',nil)
end
rawset(_G,'__lazy185',s)
fiber.create(function()
    local deadline=clock.monotonic()+lease
    while rawget(_G,'__lazy185')==s and clock.monotonic()<deadline do
        local r=tarn_lazy_stats()
        if r[1]=='ok' and #s.samples<2000 then
            s.samples[#s.samples+1]={clock.monotonic(),r[2].depth,r[2].bytes,r[2].oldest_age}
        end
        fiber.sleep(.05)
    end
    s.restore()
end)
return {id=tarn.cfg.shard_id,uuid=tostring(box.info.uuid),version=box.info.version,
        wal_mode=box.cfg.wal_mode,quorum=box.info.synchro.quorum,configured_quorum=box.cfg.replication_synchro_quorum,
        replication=box.info.replication,native=tarn.cfg.native}
'''
SNAPSHOT = r'''
local s=assert(rawget(_G,'__lazy185'),'measurement lease expired')
local spaces={}
for _,name in ipairs({'_tarn_lazy_outbox','_tarn_lazy_version','_tarn_lazy_counter','_tarn_lazy_floor'}) do
 local sp=box.space[name]; spaces[name]={rows=sp:len(),bytes=sp:bsize()}
end
local r=tarn_lazy_stats()
return {calls=s.calls,seconds=s.seconds,assignments=s.assignments,
        encoded_apply_bytes=s.encoded_apply_bytes,samples=s.samples,
        backlog=r,spaces=spaces,net=box.stat.net()}
'''


def percentile(values, fraction):
    return sorted(values)[min(len(values)-1, math.ceil(len(values)*fraction)-1)] if values else None


def process_sample(directory):
    result = {}
    for f in sorted(Path(directory).glob('*.pid')):
        pid = int(f.read_text())
        fields = Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()
        io = {k: int(v) for k, v in (line.split(':') for line in Path(f'/proc/{pid}/io').read_text().splitlines())}
        result[f.stem] = dict(pid=pid, start_ticks=int(fields[19]),
                             cpu_seconds=(int(fields[11])+int(fields[12]))/os.sysconf('SC_CLK_TCK'),
                             rss_bytes=int(fields[21])*os.sysconf('SC_PAGE_SIZE'), io=io)
    return result


def status(c):
    return {k: int(v) for k, v in q(c, 'SHOW GLOBAL STATUS') if k.startswith('Tarn_') and str(v).isdigit()}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--tarn', default='127.0.0.1:4000')
    ap.add_argument('--nodes', required=True, help='current writable leader of each shard')
    ap.add_argument('--cluster-dir', required=True)
    ap.add_argument('--output', required=True)
    ap.add_argument('--pairs', type=int, default=5)
    ap.add_argument('--seconds', type=float, default=10)
    ap.add_argument('--threads', type=int, default=16)
    ap.add_argument('--batch', type=int, default=8)
    ap.add_argument('--write-every', type=int, default=1, help='write plus causal recheck every N requests; others recheck a previously written row')
    ap.add_argument('--prefix-every', type=int, default=10)
    ap.add_argument('--rate', type=float, default=0, help='scheduled complete operations/s; 0 measures saturation')
    ap.add_argument('--p99-ms', type=float, default=250)
    ap.add_argument('--lanes', default='1,4,16')
    ap.add_argument('--shapes', default='uniform,skewed')
    ap.add_argument('--source', required=True)
    ap.add_argument('--binary', required=True)
    ap.add_argument('--native', required=True)
    args = ap.parse_args()
    nodes = args.nodes.split(',')
    output = Path(args.output); output.parent.mkdir(parents=True, exist_ok=True)
    report = dict(arguments=vars(args), source=args.source,
                  binaries={p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in (args.binary,args.native)},
                  host=dict(uname=list(os.uname()), cpu_count=os.cpu_count()),
                  source_files={str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in
                                [Path(__file__),Path('lua/tarn_lazy.lua'),Path('crates/tarn-sql/src/lazy.rs'),
                                 Path(args.cluster_dir)/'cluster.json']},
                  comparison='same binary, route_parts=1 versus route_parts=2; explicit application lane values',
                  limitations=['one host; shared CPU, storage and failure domain',
                               'instrumentation costs included identically in both arms',
                               'physical I/O is process /proc io; network totals are Tarantool counters',
                               'no replay after mutation errors; all failures and missed offered slots retained'], samples=[])
    def save():
        output.write_text(json.dumps(report, indent=2, default=str)+'\n')
    save()
    c = connect(args.tarn, db=None)
    q(c,'CREATE DATABASE IF NOT EXISTS routing185'); q(c,'USE routing185')
    try:
        for shape in args.shapes.split(','):
            for lanes in map(int,args.lanes.split(',')):
                for pair in range(args.pairs):
                    for parts in ([1,2] if pair%2==0 else [2,1]):
                        sample = dict(shape=shape,lanes=lanes,pair=pair,route_parts=parts)
                        report['samples'].append(sample); save()
                        q(c,'DROP TABLE IF EXISTS t')
                        q(c,"CREATE TABLE t(tenant_id INT,id BIGINT,k INT,lane INT,n INT,v VARBINARY(64),"
                            "PRIMARY KEY(tenant_id,id),KEY lk(k,lane,n,id) COMMENT 'tarn:lazy=%d') PARTITION BY KEY(tenant_id)"%parts)
                        wait_empty(nodes)
                        installed=[]
                        try:
                            sample['identities']=[]
                            for node in nodes:
                                identity=ev(node,INSTALL,[max(120,args.seconds+60)])
                                sample['identities'].append(identity); installed.append(node)
                                assert identity['wal_mode']=='fsync' and identity['quorum']>=2,identity
                                assert len(identity['replication'])==3,identity
                            sample['process_before']=process_sample(args.cluster_dir)
                            sample['status_before']=status(c)
                            sample['native_before']=[ev(node,SNAPSHOT) for node in nodes]
                            ready=threading.Barrier(args.threads+1); start_event=threading.Event()
                            def worker(worker_id):
                                conn=connect(args.tarn,db='routing185'); lat=[]; errors=[]; skipped=0; op=0; written=0
                                rng=random.Random(185+worker_id)
                                try:
                                    q(conn,'SET tarn_lazy_read_your_writes=ON')
                                    ready.wait(timeout=20); start_event.wait(timeout=20)
                                    slot=worker_id
                                    while time.monotonic()<deadline:
                                        if args.rate:
                                            due=begin+slot/args.rate
                                            if due>=deadline: break
                                            time.sleep(max(0,due-time.monotonic())); slot+=args.threads
                                        started=time.monotonic(); op+=1
                                        write=op==1 or op%args.write_every==0
                                        if write:
                                            key=7 if shape=='skewed' and rng.random()<.95 else rng.randrange(128)
                                            lane=worker_id%lanes
                                            last=(worker_id+1)*1000000000+op*args.batch+args.batch-1
                                            number=op
                                            rows=[(worker_id+1,last-args.batch+1+j,key,lane,number,b'x'*64) for j in range(args.batch)]
                                        try:
                                            if write:
                                                with conn.cursor() as cur:
                                                    cur.executemany('INSERT INTO t VALUES(%s,%s,%s,%s,%s,%s)',rows)
                                                written+=args.batch
                                            got=q(conn,'SELECT id FROM t WHERE k=%s AND lane=%s AND n=%s AND id=%s LIMIT 1',(key,lane,number,last))
                                            assert got==((last,),),got
                                            kind='write_recheck' if write else 'read_recheck'
                                            if args.prefix_every and op%args.prefix_every==0:
                                                got=q(conn,'SELECT lane,n,id FROM t WHERE k=%s ORDER BY lane,n,id LIMIT 16',(key,))
                                                assert list(got)==sorted(got) and len(got)<=16,got
                                                kind+='_prefix'
                                            lat.append([started-begin,time.monotonic()-started,kind])
                                        except Exception as e:
                                            errors.append(dict(at=started-begin,error=repr(e),operation=op))
                                            break
                                        if args.rate:
                                            while begin+slot/args.rate<min(deadline,time.monotonic()):
                                                skipped+=1; slot+=args.threads
                                finally: conn.close()
                                return dict(latencies=lat,errors=errors,skipped=skipped,rows_inserted=written)
                            with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as pool:
                                tasks=[pool.submit(worker,i) for i in range(args.threads)]
                                ready.wait(timeout=20); begin=time.monotonic()+.05; deadline=begin+args.seconds
                                start_event.set(); runs=[t.result() for t in tasks]
                            elapsed=time.monotonic()-begin
                            wait_empty(nodes,timeout=30)
                            sample['process_after']=process_sample(args.cluster_dir)
                            sample['status_after']=status(c)
                            sample['native_after']=[ev(node,SNAPSHOT) for node in nodes]
                            values=[x[1] for r in runs for x in r['latencies']]
                            sample.update(workers=runs,elapsed=elapsed,completed=len(values),
                                          rows_inserted=sum(r["rows_inserted"] for r in runs),p50=percentile(values,.5),p99=percentile(values,.99),
                                          goodput=len(values)/elapsed,skipped=sum(r['skipped'] for r in runs))
                            observed=sample['status_after'].get('Tarn_lazy_reads',0)-sample['status_before'].get('Tarn_lazy_reads',0)
                            expected=len(values)+sum(x[2].endswith('_prefix') for r in runs for x in r['latencies'])
                            assert observed>=expected,(observed,expected)
                            sample['observed_lazy_reads']=observed
                            sample['goodput_at_p99_ceiling']=sample['goodput'] if values and sample['p99']*1000<=args.p99_ms and not any(r['errors'] for r in runs) else 0
                            # A controlled destination refusal retains exact durable
                            # assignments. It is separate from the timed workload.
                            for node in nodes: ev(node,"assert(__lazy185); __lazy185.reject=true; return true")
                            q(c,"INSERT INTO t VALUES(9999,99999999999,7,0,0,'outage')")
                            time.sleep(.2)
                            sample['outage_before']=[ev(node,SNAPSHOT) for node in nodes]
                            assert sum(x['backlog'][1]['depth'] for x in sample['outage_before'])>=1
                            resumed=time.monotonic()
                            for node in nodes: ev(node,"assert(__lazy185); __lazy185.reject=false; return true")
                            wait_empty(nodes,timeout=30)
                            sample['outage_catchup_seconds']=time.monotonic()-resumed
                            sample['outage_after']=[ev(node,SNAPSHOT) for node in nodes]
                            assert q(c,'SELECT id FROM t WHERE k=7 AND lane=0 AND n=0 AND id=99999999999')==((99999999999,),)
                            sample['complete']=True
                            print(json.dumps({k:sample[k] for k in ('shape','lanes','pair','route_parts','completed','p99','goodput','goodput_at_p99_ceiling')}),flush=True)
                        except Exception as e:
                            sample['failure']=repr(e); save(); raise
                        finally:
                            for node in installed:
                                ev(node,"if __lazy185 then __lazy185.restore() end return true")
                            save()
    finally: c.close()


if __name__=='__main__':
    main()
