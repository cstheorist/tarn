#!/usr/bin/env python3
"""Paired SQL dispatch benchmark against two already-running gateways.

Starts no services. Both gateways must share one immutable build and corpus,
with only native_batch_reads different. Raw u64 nanosecond samples include
SQL row validation. PyMySQL/GIL/network limits make rates end-to-end results,
not a claim about shard capacity. Storage CPU is gated total thread CPU.
"""
import argparse
from array import array
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import random
import sys
import threading
import time

from native_batch import observe, ticks, write


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered)-1, int((len(ordered)-1)*fraction))]


def keys(rng, batch, rows, shape):
    start = rng.randint(1, rows-batch+1)
    if shape == "hits":
        return list(range(start, start+batch))
    if shape == "duplicates":
        unique = max(1, batch//4)
        return [start+(i % unique) for i in range(batch)]
    if shape == "miss":
        return [rows+1+start+i for i in range(batch)]
    raise ValueError(shape)


def query(cursor, args, rng, batch, shape):
    ids = keys(rng, batch, args.rows, shape)
    expected = sorted({i for i in ids if i <= args.rows})
    sql = f"SELECT id,p FROM {args.database}.t WHERE tenant_id=%s AND id IN ("+",".join(["%s"]*batch)+") ORDER BY id"
    cursor.execute(sql, (args.tenant, *ids))
    rows = cursor.fetchall()
    if rows != tuple((i, "x"*args.payload) for i in expected):
        raise AssertionError(f"SQL set/order/payload mismatch: expected {expected}, got {rows[:3]}")
    return len(rows)


def connect(pymysql, endpoint):
    host, port = endpoint.rsplit(":",1)
    return pymysql.connect(host=host, port=int(port), user="sb", password="",
                           autocommit=True, charset="utf8mb4", connect_timeout=3,
                           read_timeout=15, write_timeout=15)


def phase(args, root, number, trial, mode, batch, shape, workers, pymysql):
    prefix = root/f"{number:03d}-{mode}-{shape}-b{batch}-w{workers}"
    record = {"trial":trial,"mode":mode,"batch":batch,"shape":shape,"workers":workers,
              "prefix":prefix.name,"started_wall":time.time(),"accepted":False,
              "scope":"end-to-end SQL including decoding and row validation; PyMySQL client"}
    conns = []
    traces = []
    seed = args.seed+trial*1_000_000+batch*100+("hits","duplicates","miss").index(shape)
    try:
        endpoint = args.lua_gateway if mode == "lua" else args.native_gateway
        conns = [connect(pymysql, endpoint) for _ in range(workers)]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            warm_end = time.monotonic()+args.warmup
            def warm(worker):
                rng = random.Random(seed+worker+33_000_000)
                with conns[worker].cursor() as cursor:
                    while time.monotonic() < warm_end:
                        query(cursor,args,rng,batch,shape)
            for f in [pool.submit(warm,w) for w in range(workers)]:
                f.result()
            go = threading.Event()
            deadline = [0.0]
            stop = threading.Event()
            ready = threading.Barrier(workers+1)
            def run(worker):
                rng = random.Random(seed+worker)
                latency = array('Q')
                trace = {"worker":worker,"latencies":latency,"rows":0,"error":None}
                traces.append(trace)
                ready.wait(timeout=10)
                go.wait(timeout=10)
                try:
                    with conns[worker].cursor() as cursor:
                        while time.monotonic() < deadline[0] and not stop.is_set():
                            started = time.perf_counter_ns()
                            trace["rows"] += query(cursor,args,rng,batch,shape)
                            latency.append(time.perf_counter_ns()-started)
                except BaseException as exc:
                    trace["error"] = f"{type(exc).__name__}: {exc}"
                    stop.set()
            futures = [pool.submit(run,w) for w in range(workers)]
            ready.wait(timeout=10)
            record["before"] = observe(args)
            cpu0 = time.process_time()
            start = time.monotonic()
            deadline[0] = start+args.seconds
            go.set()
            for f in futures:
                f.result(timeout=args.seconds+20)
            elapsed = time.monotonic()-start
            record["client_cpu_seconds"] = time.process_time()-cpu0
            record["after"] = observe(args,after=True)
        latency = [n for trace in traces for n in trace["latencies"]]
        count = len(latency)
        assert count and all(len(t["latencies"]) for t in traces), "empty worker"
        errors = [t["error"] for t in traces if t["error"]]
        assert not errors, errors
        b,a = record["before"],record["after"]
        assert b["uuid"]==a["uuid"] and b["pid"]==a["pid"], "storage restarted"
        assert b["native_layout"]==a["native_layout"]==11 and b["native_size"]==a["native_size"]==312
        assert b["test_hooks"] is False and a["test_hooks"] is False
        assert b["jit_on"] and a["jit_on"]
        assert b["gc_running"] is not False and a["gc_running"] is not False
        assert b["info"]["native"]["ready"]==a["info"]["native"]["ready"]==1
        record.update(operations=count,elapsed_seconds=elapsed,returned_rows=sum(t["rows"] for t in traces),
                      queries_per_second=count/elapsed,p50_ns=percentile(latency,.5),p99_ns=percentile(latency,.99),
                      min_ns=min(latency),max_ns=max(latency),client_cpu_cores=record["client_cpu_seconds"]/elapsed)
        record["cpu_thread_ns"] = a["cpu_thread_ns"]-b["cpu_thread_ns"]
        record["cpu_ns_per_query"] = record["cpu_thread_ns"]/count
        record["tx_cpu_cores"] = record["cpu_thread_ns"]/1e9/elapsed
        record["lua_allocated_bytes"] = a["gc"]["gc_allocated"]-b["gc"]["gc_allocated"]
        record["lua_bytes_per_query"] = record["lua_allocated_bytes"]/count
        record["gc_steps"] = {k:a["gc"][k]-b["gc"][k] for k in b["gc"] if k.startswith("gc_steps_")}
        record["rust_allocator_delta"] = {k:a["rust_allocs"][k]-b["rust_allocs"][k] for k in ("allocations","reallocations","deallocations","allocated_bytes","freed_bytes","live_bytes")}
        record["rust_requested_bytes_per_query"] = record["rust_allocator_delta"]["allocated_bytes"]/count
        record["native_served_delta"] = a["info"]["native"]["served"]-b["info"]["native"]["served"]
        record["native_handed_back_delta"] = a["info"]["native"]["to_lua"]-b["info"]["native"]["to_lua"]
        # No other clients run in this fixture. This distinguishes actual batch
        # dispatch from a planner path which never reaches the new procedure.
        served = record["native_served_delta"]
        handbacks = record["native_handed_back_delta"]
        if mode == "native":
            assert served > 0 and served+handbacks == count, (served,handbacks,count)
            rb = (b["info"]["native"]["fallbacks"] or {}).get("read") or {}
            ra = (a["info"]["native"]["fallbacks"] or {}).get("read") or {}
            reasons = {k:ra.get(k,0)-rb.get(k,0) for k in set(rb)|set(ra) if ra.get(k,0)!=rb.get(k,0)}
            record["fallback_reasons"] = reasons
            # Native code deliberately hands back an expired load-count window
            # so Lua can rotate its arrays. Keep that real overhead in results;
            # unexpected handbacks or a fully Lua run are never accepted.
            assert set(reasons) <= {"load_window"} and sum(reasons.values())==handbacks, reasons
            record["fallback_fraction"] = handbacks/count
        else:
            assert served == handbacks == 0, (served,handbacks)
        if ticks(a) is not None and ticks(b) is not None and args.clk_tck:
            record["proc_tx_cpu_seconds"] = (ticks(a)-ticks(b))/args.clk_tck
        record["accepted"] = True
    except BaseException as exc:
        record["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        raw = []
        for trace in sorted(traces,key=lambda t:t["worker"]):
            path = prefix.with_suffix(f".worker{trace['worker']}.latency.u64le")
            values = trace["latencies"]
            if sys.byteorder!="little":
                values.byteswap()
            path.write_bytes(values.tobytes())
            raw.append({"path":path.name,"samples":len(values),"sha256":hashlib.sha256(path.read_bytes()).hexdigest(),
                        "rows":trace["rows"],"error":trace["error"]})
        record["raw"] = raw
        write(prefix.with_suffix('.json'),record)
        for conn in conns:
            conn.close()
    return record


def run(args):
    if args.python_path:
        sys.path.insert(0,str(args.python_path.resolve()))
    import pymysql
    assert args.database.isidentifier() and args.rows>=max(args.batches)
    root=args.output.resolve()
    root.mkdir(parents=True,exist_ok=False)
    write(root/'manifest.json',{'arguments':{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
                             'client':'PyMySQL; Python GIL and SQL decoding included; not capacity evidence',
                             'cpu_scope':'TX-thread CPU over gated total, including observer/background overhead; no idle subtraction',
                             'gc_scope':'Normal Lua GC restarted by fixture before measurement; reject observed stopped GC, retain null if LuaJIT cannot introspect isrunning',
                             'allocation_scope':'Lua GC separate from native Rust requested allocator capacities; excludes C/tuples/RSS'})
    idle0=observe(args);time.sleep(.25);idle1=observe(args,after=True)
    write(root/'idle-control.json',{'before':idle0,'after':idle1,'subtracted':False})
    results=[]
    cases=[(b,s,w) for b in args.batches for s in args.shapes for w in args.workers]
    for trial in range(args.trials):
        order=cases.copy();random.Random(args.seed+trial).shuffle(order)
        for index,(batch,shape,workers) in enumerate(order):
            modes=['lua','native'] if (trial+index)%2==0 else ['native','lua']
            for mode in modes:
                row=phase(args,root,len(results),trial,mode,batch,shape,workers,pymysql)
                results.append(row);write(root/'results.json',results)
                print(f"SQL {mode} {shape} B{batch} W{workers}: accepted={row['accepted']}, ops={row.get('operations',0)}, error={row.get('error')}",flush=True)
                assert row['accepted'],row.get('error')
    pairs=[]
    for trial in range(args.trials):
        for batch,shape,workers in cases:
            pair=[r for r in results if (r['trial'],r['batch'],r['shape'],r['workers'])==(trial,batch,shape,workers)]
            l,n=({r['mode']:r for r in pair}[m] for m in ('lua','native'))
            pairs.append({'trial':trial,'batch':batch,'shape':shape,'workers':workers,'order':[r['mode'] for r in pair],
                          'native_over_lua_cpu':n['cpu_ns_per_query']/l['cpu_ns_per_query'],
                          'native_over_lua_rate':n['queries_per_second']/l['queries_per_second'],
                          'native_over_lua_p50':n['p50_ns']/l['p50_ns'],'native_over_lua_p99':n['p99_ns']/l['p99_ns']})
    write(root/'pairs.json',pairs)
    print(f"PASS {len(results)} SQL phases",flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--client',type=Path,required=True)
    p.add_argument('--shard',required=True)
    p.add_argument('--lua-gateway',required=True)
    p.add_argument('--native-gateway',required=True)
    p.add_argument('--database',required=True)
    p.add_argument('--tenant',type=int,default=7)
    p.add_argument('--rows',type=int,default=100_000)
    p.add_argument('--payload',type=int,default=64)
    p.add_argument('--batches',default='8,32,128')
    p.add_argument('--shapes',default='hits,duplicates,miss')
    p.add_argument('--workers',default='1')
    p.add_argument('--trials',type=int,default=5)
    p.add_argument('--warmup',type=float,default=1)
    p.add_argument('--seconds',type=float,default=5)
    p.add_argument('--seed',type=int,default=141)
    p.add_argument('--clk-tck',type=int)
    p.add_argument('--python-path',type=Path)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    args.batches=[int(n) for n in args.batches.split(',')]
    args.shapes=args.shapes.split(',')
    args.workers=[int(n) for n in args.workers.split(',')]
    args.native_allocs=True
    assert all(1<=b<=128 for b in args.batches) and all(1<=w<=64 for w in args.workers)
    assert set(args.shapes)<={'hits','duplicates','miss'} and args.trials>0 and args.warmup>0 and args.seconds>0
    run(args)


if __name__=='__main__':
    main()
