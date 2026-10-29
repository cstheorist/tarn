#!/usr/bin/env python3
"""Endpoint-driven guarded batch benchmark; starts no database services.

Build the native_batch Rust example with the pinned release toolchain.
native_batch_fixture.py supplies an owned reproducible local fixture. Raw
u64le samples include client row validation; Lua allocation counters exclude
Rust/CAPI/storage allocations. Every result is saved before acceptance checks.
"""
import argparse
import hashlib
import itertools
import json
import os
from pathlib import Path
import random
import select
import statistics
import subprocess
import time


SNAPSHOT = r"""
local clock,misc,ffi=require('clock'),require('misc'),require('ffi')
local function rust_snapshot()
 local r=box.func['tarn_storage.ts_select']:call({'native_batch_allocs'})
 assert(type(r)=='table' and r[1]=='native_batch_allocs','alloc-metrics feature unavailable')
 return r[2]
end
local rust
if __ALLOC__ and __AFTER__ then rust=rust_snapshot() end
local m=misc.getmetrics();local i=tarn_info();local b=tarn_buckets()
local pid=box.info.pid
local f=io.open('/proc/'..pid..'/task/'..pid..'/stat','r')
local proc=f and f:read('*a') or box.NULL;if f then f:close() end
local gc_ok,gc_running=pcall(collectgarbage,'isrunning')
local gc_state=box.NULL;if gc_ok then gc_state=gc_running end
local n=i.native and ffi.cast('struct tarn_ns *',box.func['tarn_storage.ts_state']:call()) or nil
if __ALLOC__ and not __AFTER__ then rust=rust_snapshot() end
return require('json').encode{
 cpu_thread_ns=tonumber(clock.thread64()),cpu_process_ns=tonumber(clock.proc64()),
 gc=m,call=box.stat().CALL.total,eval=box.stat().EVAL.total,
 info=i,pid=pid,uuid=tostring(box.info.uuid),version=box.info.version,
 read_contract=tarn_read_contract(),
 native_fresh_clock_ok=n and tonumber(n.fresh_clock_ok) or box.NULL,
 native_layout=n and tonumber(n.layout) or box.NULL,
 native_size=n and ffi.sizeof('struct tarn_ns') or box.NULL,
 route={epoch=b[3],membership=b[6],count=b[2]},proc_stat=proc,
 jit_on=jit.status(),gc_running=gc_state,
 runtime=box.runtime.info(),engine_options={memtx_memory=box.cfg.memtx_memory,
 vinyl_memory=box.cfg.vinyl_memory,vinyl_cache=box.cfg.vinyl_cache,
 wal_mode=box.cfg.wal_mode,quorum=box.cfg.replication_synchro_quorum},
 test_hooks=tarn.cfg.test_hooks,active_native_hooks=n and tonumber(n.hooks) or box.NULL,
 check_locks=tarn.cfg.check_locks,rust_allocs=rust or box.NULL}
"""


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def evaluate(args, code):
    command = [str(args.client), "--addr", args.shard, "--eval", code]
    result = subprocess.run(command, capture_output=True, text=True, timeout=10)
    if result.returncode:
        raise RuntimeError(f"observer failed: {result.stderr}")
    return json.loads(result.stdout)


def observe(args, after=False):
    code = SNAPSHOT.replace("__ALLOC__",str(args.native_allocs).lower()).replace("__AFTER__",str(after).lower())
    return evaluate(args,code)


def route_bytes(args, route, batch):
    def uint(n):
        if n < 128: return bytes([n])
        for width,marker in ((1,0xcc),(2,0xcd),(4,0xce),(8,0xcf)):
            if n < 1 << (8*width): return bytes([marker])+n.to_bytes(width,"big")
        raise ValueError("MP uint overflow")
    if route == "buckets":
        header=bytes([0x90+batch]) if batch<16 else b"\xdc"+batch.to_bytes(2,"big")
        return header+uint(args.bucket)*batch
    return b"\x82\xa1e"+uint(args.epoch)+b"\xa1m"+uint(args.membership)


def line_before(process, deadline):
    # Each Rust gate is one complete flushed line, with no other output.
    while time.monotonic() < deadline:
        if select.select([process.stdout], [], [], max(0, deadline-time.monotonic()))[0]:
            return process.stdout.readline()
    raise TimeoutError("bounded Rust client gate")


def ticks(snapshot):
    raw = snapshot.get("proc_stat")
    if not raw:
        return None
    fields = raw.rsplit(")", 1)[1].split()
    return int(fields[11]) + int(fields[12])


def measure(args, root, number, trial, mode, shape, batch, workers, seed, route):
    prefix = root / f"{number:03d}-{mode}-{shape}-{route}-b{batch}-w{workers}"
    command = [str(args.client), "--addr", args.shard, "--space", str(args.space),
               "--epoch", str(args.epoch), "--membership", str(args.membership),
               "--rows", str(args.rows), "--payload", str(args.payload),
               "--buckets", str(args.buckets), "--batch", str(batch),
               "--workers", str(workers), "--conns", str(args.conns),
               "--seed", str(seed), "--seconds", str(args.seconds),
               "--warmup", str(args.warmup), "--mode", mode, "--shape", shape,
               "--native-proc", args.native_proc, "--output", str(prefix), "--gate"]
    command += ["--route",route,"--bucket",str(args.bucket)]
    if args.tenant is not None:
        command += ["--tenant",str(args.tenant)]
    record = {"trial": trial, "mode": mode, "shape": shape, "batch": batch,
              "workers": workers, "seed": seed, "command": command,
              "started_wall": time.time(), "prefix": prefix.name,"route":route,
              "tenant":args.tenant,"bucket":args.bucket,
              "route_representation": {"e":args.epoch,"m":args.membership} if route=="epoch" else [args.bucket]*batch}
    process = None
    output = []
    collected = False
    try:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        ready = line_before(process, time.monotonic()+args.warmup+15)
        output.append(ready)
        if ready.strip() != "READY":
            raise RuntimeError("client failed before READY")
        record["before"] = observe(args)
        process.stdin.write("GO\n")
        process.stdin.flush()
        done = line_before(process, time.monotonic()+args.seconds+15)
        output.append(done)
        record["after"] = observe(args,after=True)
        process.stdin.write("FINISH\n")
        process.stdin.flush()
        process.stdin.close()
        process.stdin = None
        tail, errors = process.communicate(timeout=15)
        collected = True
        output.append(tail)
        prefix.with_suffix(".stdout").write_text("".join(output))
        prefix.with_suffix(".stderr").write_text(errors)
        record["exit_code"] = process.returncode
        result = json.loads(tail.strip()) if tail.strip() else None
        record["client"] = result
        for path in prefix.parent.glob(prefix.name+".*"):
            if path.suffix not in (".json",".stdout",".stderr"):
                record.setdefault("raw_sha256",{})[path.name]=digest(path)
        if done.strip() != "DONE" or process.returncode:
            raise RuntimeError("client failed (raw samples/error text retained): " + errors)
        before, after = record["before"], record["after"]
        assert before["uuid"] == after["uuid"] and before["pid"] == after["pid"], "storage restarted"
        for snapshot in (before, after):
            assert snapshot["read_contract"] == ["strong_reads", 1]
            assert snapshot["native_layout"] == 14 and snapshot["native_size"] == 392
            assert snapshot["native_fresh_clock_ok"] == 1 and snapshot["info"]["clock"]["read_ready"]
        assert result["operations"] > 0
        operations = result["operations"]
        requested = operations * batch
        record["cpu_thread_ns"] = after["cpu_thread_ns"]-before["cpu_thread_ns"]
        record["cpu_process_ns"] = after["cpu_process_ns"]-before["cpu_process_ns"]
        record["lua_allocated_bytes"] = after["gc"]["gc_allocated"]-before["gc"]["gc_allocated"]
        record["lua_freed_bytes"] = after["gc"]["gc_freed"]-before["gc"]["gc_freed"]
        record["gc_steps"] = {key: after["gc"][key]-before["gc"][key]
                              for key in before["gc"] if key.startswith("gc_steps_")}
        record["cpu_ns_per_batch"] = record["cpu_thread_ns"]/operations
        record["cpu_ns_per_key"] = record["cpu_thread_ns"]/requested
        record["lua_bytes_per_batch"] = record["lua_allocated_bytes"]/operations
        record["lua_bytes_per_key"] = record["lua_allocated_bytes"]/requested
        record["batches_per_second"] = operations/result["elapsed_seconds"]
        record["keys_per_second"] = requested/result["elapsed_seconds"]
        record["tx_cpu_cores"] = record["cpu_thread_ns"]/1e9/result["elapsed_seconds"]
        record["instance_cpu_cores"] = record["cpu_process_ns"]/1e9/result["elapsed_seconds"]
        record["client_cpu_cores"] = result["client_cpu_seconds"]/result["elapsed_seconds"]
        record["capacity_interpretation"] = "TX near saturation" if record["tx_cpu_cores"]>=.9 else "TX unsaturated; rate does not establish TX capacity"
        if record["client_cpu_cores"]>=1.8:
            record["capacity_interpretation"] += "; client near its two-worker CPU budget"
        for key in ("bytes_read","bytes_written"):
            if key in result:
                record[key+"_per_second"] = result[key]/result["elapsed_seconds"]
        if args.native_allocs:
            rb,ra=before["rust_allocs"],after["rust_allocs"]
            record["rust_allocator_delta"]={k:ra[k]-rb[k] for k in ("allocations","reallocations","deallocations","allocated_bytes","freed_bytes","live_bytes")}
            record["rust_requested_bytes_per_key"] = record["rust_allocator_delta"]["allocated_bytes"]/requested
            record["rust_requested_bytes_per_batch"] = record["rust_allocator_delta"]["allocated_bytes"]/operations
        record["native_before"] = before["info"]["native"]
        record["native_after"] = after["info"]["native"]
        nb,na=record["native_before"],record["native_after"]
        record["native_census_delta"]={k:na[k]-nb[k] for k in ("served","to_lua","writes","writes_to_lua","commits","commits_to_lua","validations","validations_to_lua")}
        old_reasons=nb.get("fallbacks",{});new_reasons=na.get("fallbacks",{})
        old_reasons=old_reasons.get("read",{}) if isinstance(old_reasons,dict) else {}
        new_reasons=new_reasons.get("read",{}) if isinstance(new_reasons,dict) else {}
        record["read_fallback_reasons_delta"]={k:new_reasons.get(k,0)-old_reasons.get(k,0) for k in set(old_reasons)|set(new_reasons)}
        outcomes=prefix.with_suffix(".outcome.u8").read_bytes()
        assert len(outcomes)==operations and set(outcomes)<={0,1,2},"unexpected failed/missing outcomes"
        assert outcomes.count(1)==result["native_served"] and outcomes.count(2)==result["fallbacks"]
        if mode=="lua":
            assert set(outcomes)<={0}
        else:
            assert outcomes.count(1)+outcomes.count(2)==operations
        if not args.allow_concurrent_native:
            census=record["native_census_delta"]
            assert census["served"]==result["native_served"] and census["to_lua"]==result["fallbacks"],"native census disagrees with actual client outcomes"
            assert all(census[k]==0 for k in census if k not in ("served","to_lua")),"unexpected other native activity"
            assert sum(record["read_fallback_reasons_delta"].values())==result["fallbacks"],"fallback reasons do not reconcile"
        if mode=="native":
            assert result["native_served"]>0 or args.allow_all_fallback,"positive native case served entirely by Lua"
            unexpected={k:v for k,v in record["read_fallback_reasons_delta"].items() if v and k not in args.allowed_fallback_reasons}
            assert not unexpected,"unexpected handback reasons: "+str(unexpected)
        record["native_fraction"] = result["native_served"]/operations
        if ticks(before) is not None and ticks(after) is not None:
            record["proc_tx_ticks"] = ticks(after)-ticks(before)
            if args.clk_tck:
                record["proc_tx_cpu_seconds"] = record["proc_tx_ticks"]/args.clk_tck
        for suffix, width in (("latency.u64le",8),("first.u64le",8),("outcome.u8",1)):
            path = prefix.with_suffix("."+suffix)
            assert path.stat().st_size == width*operations, path
            record.setdefault("raw_sha256", {})[path.name] = digest(path)
        route_file=prefix.with_suffix(".route.msgpack")
        assert route_file.read_bytes()==route_bytes(args,route,batch),"actual route wire bytes differ"
        record["route_wire_sha256"]=digest(route_file)
        record["accepted"] = True
    except BaseException as exc:
        record["error"] = f"{type(exc).__name__}: {exc}"
        record["accepted"] = False
        if process is not None and not collected:
            if process.poll() is None:
                process.terminate()
            try:
                tail, errors = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                tail, errors = process.communicate(timeout=5)
            prefix.with_suffix(".stdout").write_text("".join(output)+tail)
            prefix.with_suffix(".stderr").write_text(errors)
    finally:
        write(prefix.with_suffix(".json"), record)
    return record


def run(args):
    root = args.output.resolve()
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    manifest = {"arguments": {k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()}, "script_sha256": digest(__file__),
                "client_sha256": digest(args.client), "raw_format": "little-endian u64 ns, one u8 outcome",
                "allocation_scope": "LuaJIT managed bytes only; excludes Rust/CAPI/tuples",
                "native_allocation_audit": "Optional alloc-metrics feature implements module-wide AtomicU64 successful System allocator requests, enabled in the same artifact on BOTH paths. Realloc charges new requested bytes allocated and old requested bytes freed; failed realloc unchanged. Independent relaxed snapshots are not physical allocations or per-procedure counters.",
                "latency_scope": "guarded roundtrip plus exact response validation; first sample is first RPC only (zero when failed/outcome3)",
                "rust_counter_scope": "module-wide successful Rust System allocator requests; separate from Lua/C/tuples/RSS; instrumented BOTH paths",
                "rust_observer_order": "before counter snapshot AFTER tarn_info; after snapshot BEFORE tarn_info; stack-only counter reply",
                "cpu_scope": "TX/process CPU includes the observer/background interval between snapshots; idle/probe control retained, not subtracted",
                "jit_gc": "normal JIT and incremental GC; no stop/collect in timed phases",
                "read_guarantee": "strong_reads V1: fresh quorum barrier before data on both Lua/native paths; native ABI14/392 and owned CALL input copy"}
    manifest["arguments"]["client"] = str(args.client)
    manifest["arguments"]["output"] = str(args.output)
    write(root/"manifest.json", manifest)
    initial = observe(args)
    write(root/"initial.json", initial)
    assert initial["jit_on"] and initial["gc_running"] is not False
    assert initial["info"]["native"]["ready"] == 1, "other native procedures must stay enabled"
    assert initial["native_layout"] == 14 and initial["native_size"] == 392
    assert initial["test_hooks"] is False,"performance fixture must omit TARN_TEST_HOOKS rather than set string0"
    assert initial["info"]["locks"] == initial["info"]["prepared"] == initial["info"]["ddl_held"] == 0
    idle0 = observe(args)
    time.sleep(.25)
    idle1 = observe(args,after=True)
    write(root/"idle-probe-control.json", {"before": idle0, "after": idle1, "subtracted": False})
    results = []
    if "native" in args.modes:
        capability=evaluate(args,"return require('json').encode(box.func['tarn_storage.ts_select']:call())")
        write(root/"native-capability.json",capability)
        assert capability==["native_batch_keys",1],"new batch capability required"
    if args.cases_json:
        plan=json.loads(args.cases_json.read_text())
        cases=[(c["workers"],c["batch"],c["shape"],c["route"]) for c in plan]
        write(root/"case-plan.json",plan)
    else:
        cases = list(itertools.product(args.workers,args.batches,args.shapes,args.route_modes))
    assert cases and len(cases)<=64
    for workers,batch,shape,route in cases:
        assert 1<=workers<=64 and 1<=batch<=256 and shape in ("hits","duplicates","miss","mixed") and route in ("epoch","buckets")
        assert route!="buckets" or args.bucket>0
    for trial in range(args.trials):
        order = cases.copy()
        random.Random(args.seed+trial).shuffle(order)
        for index, (workers, batch, shape, route) in enumerate(order):
            # Alternate by stable case identity, not its shuffled position:
            # each case gets AB/BA/AB/BA/AB across five paired trials.
            stable_index=cases.index((workers,batch,shape,route))
            modes = args.modes if (trial+stable_index)%2 == 0 else list(reversed(args.modes))
            for mode in modes:
                # A matching seed on both dispatch paths reproduces the same
                # key stream; sample counts can differ with completed work.
                seed = args.seed + trial*1_000_000 + workers*10_000 + batch*10 + ("hits","duplicates","miss","mixed").index(shape)
                row = measure(args, root, len(results), trial, mode, shape, batch, workers, seed, route)
                results.append(row)
                write(root/"results.json", results)
                if not row["accepted"]:
                    raise AssertionError(row["error"])
                print(f"{mode} {shape} B{batch} W{workers}: {row['cpu_ns_per_key']:.1f} TXns/key, "
                      f"{row['lua_bytes_per_key']:.1f} LuaB/key, {row['batches_per_second']:.0f} batches/s, "
                      f"p50/p99 {row['client']['p50_ns']/1000:.1f}/{row['client']['p99_ns']/1000:.1f}us", flush=True)
    summary = []
    keys=sorted({(r["mode"],r["shape"],r["batch"],r["workers"],r["route"]) for r in results})
    for mode, shape, batch, workers, route in keys:
        group = [r for r in results if (r["mode"],r["shape"],r["batch"],r["workers"],r["route"]) == (mode,shape,batch,workers,route)]
        row = {"mode":mode,"shape":shape,"batch":batch,"workers":workers,"route":route,"trials":len(group)}
        for metric in ("cpu_ns_per_key","lua_bytes_per_key","batches_per_second"):
            values = [g[metric] for g in group]
            row[metric] = {"median":statistics.median(values),"min":min(values),"max":max(values),
                           "stdev":statistics.stdev(values) if len(values)>1 else 0,"raw":values}
        row["p50_ns"] = [g["client"]["p50_ns"] for g in group]
        row["p99_ns"] = [g["client"]["p99_ns"] for g in group]
        summary.append(row)
    write(root/"summary.json", summary)
    # Paired ratios retain trial identity/order rather than comparing pooled
    # samples or choosing the fastest run. Tail ratios use each trial's p99.
    pairs=[]
    for trial in range(args.trials):
        for workers,batch,shape,route in cases:
            pair=[r for r in results if (r["trial"],r["workers"],r["batch"],r["shape"],r["route"])==(trial,workers,batch,shape,route)]
            bymode={r["mode"]:r for r in pair}
            if set(bymode)=={"lua","native"}:
                l,n=bymode["lua"],bymode["native"]
                pairs.append({"trial":trial,"workers":workers,"batch":batch,"shape":shape,"route":route,
                              "order":[r["mode"] for r in pair],"native_over_lua_cpu":n["cpu_ns_per_key"]/l["cpu_ns_per_key"],
                              "native_over_lua_rate":n["batches_per_second"]/l["batches_per_second"],
                              "native_over_lua_p99":n["client"]["p99_ns"]/l["client"]["p99_ns"]})
    write(root/"pairs.json",pairs)
    write(root/"final.json", observe(args,after=True))



def control_call(args, name, arguments):
    """A genuine public CALL on the same owned node, never a synthetic lock."""
    code = """
local c=require('net.box').connect(__ADDR__,{user='tarn',password='tarn',wait_connected=3})
local ok,r=pcall(c.call,c,__NAME__,__ARGS__,{timeout=3});c:close()
if not ok then error(r) end
return require('json').encode(r)
""".replace('__ADDR__',json.dumps(args.shard)).replace('__NAME__',json.dumps(name)).replace('__ARGS__',arguments)
    return evaluate(args,code)


def claim_witness(args, tx, key):
    pk = '{'+','.join(str(n) for n in ([args.tenant,key] if args.tenant is not None else [key]))+'}'
    code = """
box.begin{txn_isolation='read-confirmed'}
local ok,r=pcall(function()
 local p=box.space._tarn_part:get{__TX__};local d=box.space._tarn_txn:get{__TX__}
 local row=box.space[__SID__]:get(__PK__);local locks={}
 for _,l in box.space._tarn_locks.index.txid:pairs{__TX__} do
  local image=l:totable();image[3]=require('digest').base64_encode(l.key);locks[#locks+1]=image
 end
 return {participant=p and p.state or 'none',decision=d and d.status or 'none',
         row=row and row:totable() or box.NULL,locks=locks,lock_key_encoding='base64',total_locks=box.space._tarn_locks:len(),
         total_prepared=box.space._tarn_part.index.state:count{'prepared'}}
end)
box.rollback();if not ok then error(r) end
return require('json').encode(r)
""".replace('__TX__',json.dumps(tx)).replace('__SID__',str(args.space)).replace('__PK__',pk)
    return evaluate(args,code)


def assert_claim(witness, expected_row):
    assert witness['participant']=='prepared' and witness['decision']=='staging',witness
    assert witness['row']==expected_row,witness
    assert len(witness['locks'])==witness['total_locks'] and witness['total_locks']>0,witness
    assert witness['total_prepared']==1 and all(l[4]=='W' for l in witness['locks']),witness


def prepare_claim(args, tx, key, operation):
    row = '{'+','.join(str(n) for n in ([args.tenant,key] if args.tenant is not None else [key]))+",string.rep('y',"+str(args.payload)+"),"+str(args.bucket)+'}'
    arguments='{'+json.dumps(tx)+',1,{1},{{'+json.dumps(operation)+','+str(args.space)+','+row+'}},{},{},require("clock").realtime(),box.NULL,{'+str(args.space)+',0},{'+str(args.bucket)+'}}'
    return control_call(args,'tarn_prepare',arguments)


def abort_claim(args, tx, key):
    before=claim_witness(args,tx,key)
    if before['decision']=='staging':
        assert control_call(args,'tarn_txn_cas','{'+json.dumps(tx)+',"staging","aborted",{1}}')=='aborted'
    elif before['decision'] not in ('aborted','none'):
        raise AssertionError('owned control has unexpected decision: '+str(before))
    # Do not replay uncertain PREPARE or change any other transaction.
    result=control_call(args,'tarn_abort','{'+json.dumps(tx)+',1,{1}}')
    assert result==['ok','aborted'],result
    after=claim_witness(args,tx,key)
    assert after['participant']=='aborted' and after['decision'] in ('aborted','none'),after
    assert not after['locks'] and after['total_locks']==after['total_prepared']==0,after
    return {'before':before,'abort':result,'after':after}


def refusal_call(args, key, mode):
    pk='{'+','.join(str(n) for n in ([args.tenant,key] if args.tenant is not None else [key]))+'}'
    code="""
local c=require('net.box').connect(__ADDR__,{user='tarn',password='tarn',wait_connected=3})
local a={__SID__,0,{__PK__},box.NULL,true,false,box.NULL,{__BUCKET__}}
local clock=require('clock');local out={mode=__MODE__}
local t=clock.monotonic64()
if out.mode=='native' then
 out.native=c:call('tarn_storage.ts_select',a,{timeout=3});out.first_ns=tonumber(clock.monotonic64()-t)
 assert(out.native[1]=='lua' and #out.native==1)
end
out.reply=c:call('tarn_select_keys',a,{timeout=3});out.total_ns=tonumber(clock.monotonic64()-t)
c:close();return require('json').encode(out)
""".replace('__ADDR__',json.dumps(args.shard)).replace('__SID__',str(args.space)).replace('__PK__',pk).replace('__BUCKET__',str(args.bucket)).replace('__MODE__',json.dumps(mode))
    return evaluate(args,code)


def run_prepared_controls(args):
    """A disjoint-claim timing control plus separate overlapping refusals.

    Core success acceptance is unchanged. Refusal timings use public loopback
    net.box CALLs inside EVAL, not external-client/network latency samples.
    """
    root=args.output.resolve();root.mkdir(mode=0o700,parents=False,exist_ok=False)
    assert args.tenant is not None and args.bucket>0 and args.modes==['lua','native']
    assert args.trials*2*(args.warmup+args.seconds)<=40,'prepared control must finish within60s'
    initial=observe(args);write(root/'initial.json',initial)
    assert initial['native_layout']==14 and initial['native_size']==392 and initial['info']['native']['ready']==1
    assert initial['test_hooks'] is False and initial['active_native_hooks']==0 and initial['jit_on'] and initial['gc_running'] is not False
    assert initial['info']['locks']==initial['info']['prepared']==initial['info']['ddl_held']==0
    write(root/'manifest.json',{'script_sha256':digest(__file__),'client_sha256':digest(args.client),
          'arguments':{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
          'positive_label':'disjoint prepared-W / busy lock index',
          'refusal_latency_scope':'public net.box loopback CALL inside EVAL, excluding external observer roundtrip; not core client tails',
          'cpu_scope':'TX/process CPU includes observer/background interval; not idle-subtracted',
          'allocation_scope':'Lua managed bytes and module-wide Rust requested bytes separate; excludes C/tuples/RSS'})
    cfg=evaluate(args,"return require('json').encode({recovery_after=tarn.cfg.recovery_after,max_txn_age=tarn.cfg.max_txn_age,lock_wait=tarn.cfg.lock_wait})")
    write(root/'saved-config.json',cfg)
    started=time.monotonic();active=None;results=[];clean=[]
    try:
        assert cfg['max_txn_age']==120
        assert evaluate(args,"tarn.cfg.recovery_after=90;return require('json').encode(tarn.cfg.recovery_after)")==90
        # Beyond both random hit and miss domains, so every read is disjoint.
        key=2*args.rows+1;tx=root.parent.name+'-disjoint';active=(tx,key)
        reply=prepare_claim(args,tx,key,'insert');write(root/'disjoint-prepare.json',reply);assert reply==['yes'],reply
        for trial in range(args.trials):
            for mode in (args.modes if trial%2==0 else list(reversed(args.modes))):
                assert time.monotonic()-started<60,'prepared control lifetime exceeded'
                before=claim_witness(args,tx,key);assert_claim(before,None)
                r=measure(args,root,len(results),trial,mode,'hits',32,1,args.seed+trial*1000000,'buckets')
                after=claim_witness(args,tx,key);r['prepared_w_before']=before;r['prepared_w_after']=after
                write(root/(r['prefix']+'.json'),r);results.append(r);write(root/'results.json',results)
                assert_claim(after,None);assert r['accepted'],r.get('error')
        clean.append(abort_claim(args,tx,key));active=None;write(root/'claim-cleanup.json',clean)
        assert clean[-1]['after']['row'] is None,'disjoint insert became visible'
        refusals=[]
        for label,key,operation in [('existing',1,'replace'),('missing',args.rows+1,'insert')]:
            tx=root.parent.name+'-'+label;active=(tx,key)
            expected=[args.tenant,key,'x'*args.payload,args.bucket] if label=='existing' else None
            reply=prepare_claim(args,tx,key,operation);write(root/(label+'-prepare.json'),reply);assert reply==['yes'],reply
            for mode in args.modes:
                assert time.monotonic()-started<60,'prepared control lifetime exceeded'
                before=claim_witness(args,tx,key);assert_claim(before,expected)
                census0=observe(args);receipt=refusal_call(args,key,mode);census1=observe(args,after=True)
                after=claim_witness(args,tx,key)
                record={'label':label,'mode':mode,'before':before,'after':after,'receipt':receipt,'census_before':census0,'census_after':census1}
                refusals.append(record);write(root/'refusals.json',refusals)
                # Full typed reply remains in evidence; no retry or generic error accepted.
                assert receipt['reply'][0]=='conflict' and receipt['reply'][1]==1213,receipt
                assert_claim(after,expected)
                nb,na=census0['info']['native'],census1['info']['native']
                assert na['served']==nb['served'] and na['to_lua']-nb['to_lua']==int(mode=='native')
                rb=nb.get('fallbacks',{}).get('read',{});ra=na.get('fallbacks',{}).get('read',{})
                assert ra.get('w_lock',0)-rb.get('w_lock',0)==int(mode=='native')
                assert all(ra.get(k,0)==rb.get(k,0) for k in set(rb)|set(ra) if k!='w_lock')
            clean.append(abort_claim(args,tx,key));active=None;write(root/'claim-cleanup.json',clean)
            assert clean[-1]['after']['row']==expected,'abort changed committed data'
        pairs=[]
        for trial in range(args.trials):
            pair=[r for r in results if r['trial']==trial];l=next(r for r in pair if r['mode']=='lua');n=next(r for r in pair if r['mode']=='native')
            pairs.append({'trial':trial,'order':[r['mode'] for r in pair],
                'native_over_lua_cpu':n['cpu_ns_per_key']/l['cpu_ns_per_key'],
                'native_over_lua_rate':n['batches_per_second']/l['batches_per_second'],
                'native_over_lua_p99':n['client']['p99_ns']/l['client']['p99_ns']})
        write(root/'pairs.json',pairs)
    finally:
        try:
            if active is not None:
                clean.append(abort_claim(args,*active));write(root/'claim-cleanup.json',clean)
        finally:
            restored=evaluate(args,'tarn.cfg.recovery_after='+str(cfg['recovery_after'])+';return require("json").encode({recovery_after=tarn.cfg.recovery_after,max_txn_age=tarn.cfg.max_txn_age,lock_wait=tarn.cfg.lock_wait})')
            write(root/'restored-config.json',restored);assert restored==cfg
            write(root/'final.json',observe(args,after=True))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--client", type=Path, required=True)
    p.add_argument("--shard", required=True)
    p.add_argument("--space", type=int, required=True)
    p.add_argument("--epoch", type=int, required=True)
    p.add_argument("--membership", type=int, required=True)
    p.add_argument("--tenant",type=int)
    p.add_argument("--bucket",type=int,default=0)
    p.add_argument("--route-modes",default="epoch")
    p.add_argument("--cases-json",type=Path)
    p.add_argument("--native-allocs",action="store_true")
    p.add_argument("--prepared-w-controls",action="store_true",help="bounded genuine PREPARE disjoint timing plus typed overlapping refusal controls")
    p.add_argument("--allow-concurrent-native",action="store_true",help="label intentionally mixed workloads; do not attribute module-wide counters to reads alone")
    p.add_argument("--allowed-fallback-reasons",default="load_window")
    p.add_argument("--allow-all-fallback",action="store_true",help="explicit Vinyl/unsupported control, never native performance evidence")
    p.add_argument("--rows", type=int, default=100_000)
    p.add_argument("--payload", type=int, default=64)
    p.add_argument("--buckets", type=int, default=480)
    p.add_argument("--workers", default="1")
    p.add_argument("--conns", type=int, default=1)
    p.add_argument("--batches", default="1,8,32,128")
    p.add_argument("--shapes", default="hits,duplicates,miss")
    p.add_argument("--modes", default="lua")
    p.add_argument("--native-proc", default="tarn_storage.ts_select")
    p.add_argument("--trials", type=int, default=3)
    p.add_argument("--warmup", type=float, default=1)
    p.add_argument("--seconds", type=float, default=3)
    p.add_argument("--clk-tck", type=int)
    p.add_argument("--seed", type=int, default=141)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    args.workers = [int(n) for n in args.workers.split(",")]
    args.batches = [int(n) for n in args.batches.split(",")]
    args.shapes = args.shapes.split(",")
    args.modes = args.modes.split(",")
    args.route_modes=args.route_modes.split(",")
    args.allowed_fallback_reasons=args.allowed_fallback_reasons.split(",")
    assert args.trials > 0 and all(1 <= n <= 256 for n in args.batches)
    assert set(args.modes) <= {"lua","native"} and set(args.shapes) <= {"hits","duplicates","miss","mixed"}
    if args.prepared_w_controls:
        run_prepared_controls(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
