#!/usr/bin/env python3
"""Own one private Docker storage node and one host gateway for native_batch.

Example (all binaries must already be built; no global build/stop scripts):
  python3 bench/native_batch_fixture.py --source-ref COMMIT --image DIGEST \
    --gateway /path/tarn-server --native /path/libtarn_storage.so \
    --native-manifest /path/build-manifest.json --client /path/native_batch

Build client: cargo +1.98.1 build --release -p tarn-iproto --example native_batch --locked
Requires Docker and PyMySQL (or --python-path DIR). All source, binaries,
arguments, raw samples and final WAL are retained in a new private directory;
only exact owner-labelled resources are stopped. Normal LuaJIT JIT/GC stay on.
The initial profile is a selection baseline, not a cross-build speed claim.
"""
import argparse
import datetime
import hashlib
import json
import os
import platform
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import traceback


TASK = "tarn-native-batch"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(root, manifest):
    (root/"manifest.json").write_text(json.dumps(manifest,indent=2)+"\n")


def call(command, timeout=10, check=True):
    r = subprocess.run(command,capture_output=True,text=True,timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f"{command}: {r.stderr}")
    return r


def observer(root, endpoint, code):
    return json.loads(call([str(root/"bin/native_batch"),"--addr",endpoint,"--eval",code],timeout=10).stdout)


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1",0))
        return s.getsockname()[1]


def freeze(args, root, manifest):
    source = args.source.resolve()
    ref = subprocess.check_output(["git","-C",str(source),"rev-parse",args.source_ref],text=True).strip()
    manifest["source_commit"] = ref
    paths = subprocess.check_output(["git","-C",str(source),"ls-tree","-r","--name-only",ref,"--","lua","crates","Cargo.toml","Cargo.lock","rust-toolchain.toml"],text=True).splitlines()
    hashes = {}
    for rel in paths:
        data = subprocess.check_output(["git","-C",str(source),"show",ref+":"+rel])
        dest = root/"repo"/rel
        dest.parent.mkdir(parents=True,exist_ok=True)
        dest.write_bytes(data)
        dest.chmod(0o444)
        hashes["repo/"+rel] = hashlib.sha256(data).hexdigest()
    build = json.loads(args.native_manifest.read_text())
    for rel,digest in build["runtime_sources"].items():
        assert hashes["repo/"+rel] == digest, "native source mismatch: "+rel
    assert sha(args.native) == build["artifact_sha256"], "native artifact mismatch"
    (root/"bin").mkdir()
    for source,name in ((args.gateway,"tarn-server"),(args.native,"libtarn_storage.so"),(args.client,"native_batch")):
        dest = root/"bin"/name
        shutil.copy2(source,dest)
        dest.chmod(0o444 if name.endswith(".so") else 0o555)
        hashes["bin/"+name] = sha(dest)
    (root/"harness").mkdir()
    for name in ("native_batch.py","native_batch_fixture.py"):
        dest = root/"harness"/name
        shutil.copy2(Path(__file__).parent/name,dest)
        hashes["harness/"+name] = sha(dest)
        compile(dest.read_text(),str(dest),"exec")
    if args.cases_json:
        shutil.copy2(args.cases_json,root/"harness/case-plan.json")
        hashes["harness/case-plan.json"] = sha(root/"harness/case-plan.json")
    client_source = args.client_source or source_dir(args)/"crates/tarn-iproto/examples/native_batch.rs"
    shutil.copy2(client_source,root/"harness/native_batch.rs")
    hashes["harness/native_batch.rs"] = sha(client_source)
    shutil.copy2(args.native_manifest,root/"native-build-manifest.json")
    # The gateway's custom CLI emits usage with exit2 rather than --help0.
    result = call([str(root/"bin/tarn-server"),"--help"],check=False)
    assert result.returncode == 2 and "usage: tarn-server" in result.stderr
    assert os.access(root/"bin/native_batch",os.X_OK)
    assert call([str(root/"bin/native_batch"),"--help"]).returncode == 0
    manifest["frozen_sha256"] = hashes
    manifest["native_runtime_source_files"] = len(build["runtime_sources"])
    manifest["lua_files"] = sum(p.startswith("lua/") for p in paths)
    manifest["packaging_preflight"] = {"gateway_executable":True,"client_executable":True,"gateway_usage_exit":2,"all_frozen_paths_readable":True}
    save(root,manifest)


def source_dir(args):
    return args.source.resolve()


def owned_group(root, pgid):
    listing=call(["ps","-ww","-axo","pid,pgid,stat,command"]).stdout
    members=[]
    for line in listing.splitlines():
        fields=line.split(None,3)
        if len(fields)<4 or not fields[0].isdigit() or not fields[1].isdigit():
            continue
        if int(fields[1])!=pgid or fields[2].startswith("Z"):
            continue
        command=fields[3]
        assert str(root/"harness/native_batch.py") in command or str(root/"bin/native_batch") in command,"unexpected member of owned measurement group"
        members.append(int(fields[0]))
    return members


def stop_measurement(root,m):
    process=m.get("measure_process")
    if not process:
        return
    pgid=process["pid"]
    if owned_group(root,pgid):
        try:
            os.killpg(pgid,signal.SIGTERM)
        except ProcessLookupError:
            pass
        until=time.monotonic()+5
        while owned_group(root,pgid) and time.monotonic()<until:
            time.sleep(.1)
        if owned_group(root,pgid):
            # Recheck every member's ownership before escalation, including
            # Rust clients left behind by a controller deadline.
            try:
                os.killpg(pgid,signal.SIGKILL)
            except ProcessLookupError:
                pass
    assert not owned_group(root,pgid),"owned measurement process group remains"
    m["measure_group_absent"]=True


def cleanup(root, m):
    errors = []
    try:
        stop_measurement(root,m)
    except BaseException as error:
        errors.append("measurement-group cleanup: "+str(error))
    process = m.get("gateway_process")
    if process:
        check = call(["ps","-ww","-p",str(process["pid"]),"-o","command="],check=False)
        if check.returncode == 0 and check.stdout.strip():
            if str(root/"bin/tarn-server") not in check.stdout or str(root/"cluster.json") not in check.stdout:
                errors.append("gateway PID ownership changed")
            else:
                os.kill(process["pid"],signal.SIGTERM)
                for _ in range(50):
                    state = call(["ps","-p",str(process["pid"]),"-o","stat="],check=False)
                    if state.returncode or not state.stdout.strip() or state.stdout.strip().startswith("Z"):
                        break
                    time.sleep(.1)
                else:
                    identity=call(["ps","-ww","-p",str(process["pid"]),"-o","command="],check=False)
                    if str(root/"bin/tarn-server") in identity.stdout and str(root/"cluster.json") in identity.stdout:
                        os.kill(process["pid"],signal.SIGKILL)
                    elif identity.returncode==0 and identity.stdout.strip():
                        errors.append("gateway PID changed before escalation; not killed")
    # Names are also reserved before creation: a timed-out CLI may create a
    # resource without delivering its ID. Verify both labels before touching it.
    result = call(["docker","inspect",m["container_name"]],check=False)
    if result.returncode == 0:
        info = json.loads(result.stdout)[0]
        assert info["Name"] == "/"+m["container_name"]
        assert info["Config"]["Labels"].get("task") == TASK and info["Config"]["Labels"].get("fixture") == root.name
        (root/"logs/final-container.json").write_text(result.stdout)
        m["oom_killed"] = info["State"]["OOMKilled"]
        m["container_id"] = info["Id"]
        logs = call(["docker","logs",info["Id"]],check=False)
        (root/"logs/container.log").write_text(logs.stdout+logs.stderr)
        call(["docker","rm","-fv",info["Id"]])
    elif "No such" not in result.stderr:
        errors.append("container audit: "+result.stderr)
    result = call(["docker","network","inspect",m["network_name"]],check=False)
    if result.returncode == 0:
        info = json.loads(result.stdout)[0]
        assert info["Name"] == m["network_name"] and info["Labels"].get("task") == TASK and info["Labels"].get("fixture") == root.name
        assert not info["Containers"], "owned network still has participants"
        m["network_id"] = info["Id"]
        call(["docker","network","rm",info["Id"]])
    elif "not found" not in result.stderr:
        errors.append("network audit: "+result.stderr)
    for rel,digest in m.get("frozen_sha256",{}).items():
        assert sha(root/rel) == digest, rel
    # Verify removal independently, then archive all offline owned data.
    for command in (["docker","inspect",m["container_name"]],["docker","network","inspect",m["network_name"]]):
        result = call(command,check=False)
        assert result.returncode != 0 and ("not found" in result.stderr or "No such" in result.stderr)
    if process:
        result = call(["ps","-ww","-p",str(process["pid"]),"-o","command="],check=False)
        assert str(root/"bin/tarn-server") not in result.stdout
    index = [{"path":str(p.relative_to(root)),"bytes":p.stat().st_size,"sha256":sha(p)} for p in sorted((root/"data").rglob("*")) if p.is_file()]
    (root/"logs/data-index.json").write_text(json.dumps(index,indent=2)+"\n")
    archive = root/"runtime-wal.tar"
    with tarfile.open(archive,"w") as t:
        t.add(root/"data",arcname="data")
    with tarfile.open(archive) as t:
        for item in index:
            assert hashlib.sha256(t.extractfile(item["path"]).read()).hexdigest() == item["sha256"]
    shutil.rmtree(root/"data")
    m["cleanup"] = {"errors":errors,"owned_services_absent":not errors,"data_removed":True,"archive_sha256":sha(archive),"files":len(index),"frozen_files_unchanged":len(m.get("frozen_sha256",{}))}
    m["stage"] = "cleaned" if not errors else "cleanup-incomplete"
    save(root,m)
    if errors:
        raise RuntimeError(errors)


def run(args):
    if args.python_path:
        sys.path.insert(0,str(args.python_path.resolve()))
    import pymysql
    root = Path(tempfile.mkdtemp(prefix="tarn-native-batch-",dir=args.parent))
    m = {"fixture":root.name,"task":TASK,"created_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"image":args.image,"stage":"preparing","arguments":{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},"container_name":root.name+"-storage","network_name":root.name+"-net"}
    (root/"logs").mkdir()
    (root/"data").mkdir()
    save(root,m)
    print("FIXTURE "+str(root),flush=True)
    failed = True
    try:
        freeze(args,root,m)
        m["ports"] = {"storage":port(),"gateway":port()}
        save(root,m)
        call(["docker","network","create","--label","task="+TASK,"--label","fixture="+root.name,m["network_name"]],timeout=5)
        storage = m["ports"]["storage"]
        command = ["docker","run","-d","--name",m["container_name"],"--label","task="+TASK,"--label","fixture="+root.name,"--network",m["network_name"],"--cpus",str(args.cpus),"--memory",args.memory,"--memory-swap",args.memory,"--pids-limit","128","--tmpfs","/tmp:rw,exec,size=134217728","-p",f"127.0.0.1:{storage}:{storage}","--mount",f"type=bind,src={root}/repo,dst=/source,readonly","--mount",f"type=bind,src={root}/bin,dst=/artifacts,readonly","--mount",f"type=bind,src={root}/data,dst=/data","--entrypoint","/bin/bash"]
        env = {"TARN_SHARD_ID":"1","TARN_LISTEN":f"0.0.0.0:{storage}","TARN_WORKDIR":"/data/storage","TARN_REPLICAS":"[]","TARN_PEERS":"{}","TARN_NATIVE":"/artifacts/libtarn_storage.so","TARN_MEMORY_MB":"256","TARN_VINYL_MEMORY_MB":"32","TARN_VINYL_CACHE_MB":"0","TARN_VINYL_READ_THREADS":"1","TARN_VINYL_WRITE_THREADS":"2","TARN_VINYL_TIMEOUT":"0.25","TARN_WAL_MODE":"write","TARN_CHECK_LOCKS":"1" if args.check_locks else "","TARN_TEST_HOOKS":""}
        for key,value in env.items():
            command += ["-e",key+"="+value]
        command += [args.image,"-c","unset TT_APP_NAME TT_INSTANCE_NAME; exec tarantool /source/lua/storage.lua > /data/storage.log 2>&1"]
        m["container_id"] = call(command,timeout=15).stdout.strip()
        save(root,m)
        endpoint = f"127.0.0.1:{storage}"
        deadline = time.monotonic()+30
        while time.monotonic() < deadline:
            try:
                ready = observer(root,endpoint,"return require('json').encode({ready=not box.info.ro and box.space._tarn_meta~=nil})")
                if ready["ready"]:
                    break
            except (RuntimeError,subprocess.TimeoutExpired):
                pass
            time.sleep(.2)
        else:
            raise RuntimeError("owned storage did not become writable")
        config = {"shards":[{"id":1,"replicas":[endpoint]}],"user":"tarn","password":"tarn","bucket_count":480,"ack_on_prepare":False,"native_batch_reads":args.native_batch_reads}
        (root/"cluster.json").write_text(json.dumps(config,indent=2)+"\n")
        argv = [str(root/"bin/tarn-server"),"--config",str(root/"cluster.json"),"--listen",f"127.0.0.1:{m['ports']['gateway']}","--threads","2","--pool","4","--node-id","14101","--default-engine",args.engine,"--user","sb:"]
        with (root/"logs/gateway.log").open("w") as log:
            proc = subprocess.Popen(argv,stdout=log,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL,start_new_session=True)
        m["gateway_process"] = {"pid":proc.pid,"argv":argv}
        save(root,m)
        for _ in range(50):
            try:
                c = pymysql.connect(host="127.0.0.1",port=m["ports"]["gateway"],user="sb",password="",autocommit=True,connect_timeout=1,read_timeout=15,charset="utf8mb4")
                break
            except pymysql.MySQLError:
                assert proc.poll() is None, "gateway exited"
                time.sleep(.1)
        else:
            raise RuntimeError("gateway not ready")
        database = "nb_"+root.name.rsplit("-",1)[1]
        with c.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {database}")
            if args.tenant is None:
                cursor.execute(f"CREATE TABLE {database}.t (id INT PRIMARY KEY,p VARCHAR({args.payload}) NOT NULL) ENGINE={args.engine}")
            else:
                cursor.execute(f"CREATE TABLE {database}.t (tenant_id INT,id INT,p VARCHAR({args.payload}) NOT NULL,PRIMARY KEY(tenant_id,id)) ENGINE={args.engine} PARTITION BY KEY(tenant_id)")
            for lo in range(1,args.rows+1,1000):
                values=[(i,"x"*args.payload) if args.tenant is None else (args.tenant,i,"x"*args.payload) for i in range(lo,min(args.rows+1,lo+1000))]
                placeholders="%s,%s" if args.tenant is None else "%s,%s,%s"
                cursor.executemany(f"INSERT INTO {database}.t VALUES ({placeholders})",values)
        c.close()
        tenant="nil" if args.tenant is None else str(args.tenant)
        code = """
local name,tenant,payload=__NAME__,__TENANT__,__PAYLOAD__
collectgarbage('restart');jit.on()
local b=tarn_buckets()
box.begin{txn_isolation='read-confirmed'}
local ok,result=pcall(function()
 local s=assert(box.space[name]);local n,sum=0,0;local image={}
 local bucket
 if tenant~=nil then bucket=require('digest').crc32(require('msgpack').encode(tenant))%b[2]+1 end
 for _,r in s:pairs() do
  local id,p,k=r[1],r[2],r[3]
  if tenant~=nil then assert(r[1]==tenant);id,p,k=r[2],r[3],r[4];assert(k==bucket) end
  assert(id>=1 and id<=__ROWS__ and id%1==0)
  assert(p==string.rep('x',payload));n=n+1;sum=sum+id
  image[#image+1]=require('msgpack').encode(r:totable())
 end
 local catalog=assert(box.space._tarn_catalog:get{name})
 return {space=s.id,engine=s.engine,rows=n,sum=sum,epoch=b[3],membership=b[6],count=b[2],
         version=box.info.version,tenant=tenant or box.NULL,physical_bucket=bucket or box.NULL,
         catalog_json=catalog.def.json,primary_parts=s.index.primary.parts,format=s:format(),
         canonical_tuple_image_sha256=require('digest').sha256_hex(table.concat(image)),
         bucket_source='SQL-routed inserts; every physical row audited against CRC32C(MessagePack(tenant))',
         index_stats=s.index.primary:stat()}
end)
if box.is_in_txn() then box.rollback() end
if not ok then error(result) end
return require('json').encode(result)
""".replace("__NAME__",json.dumps(database+".t")).replace("__TENANT__",tenant).replace("__PAYLOAD__",str(args.payload)).replace("__ROWS__",str(args.rows))
        dataset = observer(root,endpoint,code)
        assert dataset["rows"] == args.rows and dataset["sum"] == args.rows*(args.rows+1)//2 and dataset["engine"] == args.engine
        m["dataset"] = dataset
        if args.engine=="vinyl" and args.dump_before_measure:
            m["vinyl_dump_witness"] = observer(root,endpoint,"box.snapshot();return require('json').encode({index=box.space["+json.dumps(database+".t")+"].index.primary:stat(),vinyl=box.stat.vinyl()})")
        m["clk_tck"] = int(call(["docker","exec",m["container_id"],"getconf","CLK_TCK"]).stdout.strip())
        m["stage"] = "measuring"
        m["resource_context"] = {"host":platform.platform(),"machine":platform.machine(),
                                 "host_logical_cpus":os.cpu_count(),"host_load_before":os.getloadavg(),
                                 "storage_cpu_quota":args.cpus,"storage_memory":args.memory,
                                 "gateway_threads":2,"gateway_pool":4,"client_tokio_workers":2,
                                 "external_load_policy":"No global processes paused/stopped; record contextual load, not isolation proof"}
        save(root,m)
        measure = [sys.executable,str(root/"harness/native_batch.py"),"--client",str(root/"bin/native_batch"),"--shard",endpoint,"--space",str(dataset["space"]),"--epoch",str(dataset["epoch"]),"--membership",str(dataset["membership"]),"--rows",str(args.rows),"--payload",str(args.payload),"--buckets",str(dataset["count"]),"--batches",args.batches,"--shapes",args.shapes,"--workers",args.workers,"--trials",str(args.trials),"--warmup",str(args.warmup),"--seconds",str(args.seconds),"--clk-tck",str(m["clk_tck"]),"--modes",args.modes,"--output",str(root/"measurements")]
        measure += ["--route-modes",args.route_modes]
        if args.tenant is not None:
            measure += ["--tenant",str(args.tenant),"--bucket",str(dataset["physical_bucket"])]
        if args.cases_json:
            measure += ["--cases-json",str(root/"harness/case-plan.json")]
        if args.native_allocs:
            measure += ["--native-allocs"]
        if args.engine=="vinyl":
            measure += ["--allow-all-fallback","--allowed-fallback-reasons","vinyl,load_window"]
        m["measure_command"] = measure
        save(root,m)
        with (root/"logs/measure.log").open("w") as log:
            measured=subprocess.Popen(measure,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            m["measure_process"]={"pid":measured.pid,"argv":measure}
            save(root,m)
            try:
                status=measured.wait(timeout=args.deadline)
            except subprocess.TimeoutExpired:
                stop_measurement(root,m)
                measured.wait(timeout=5)
                raise
        m["measure_exit"] = status
        m["resource_context"]["host_load_after"] = os.getloadavg()
        save(root,m)
        print((root/"logs/measure.log").read_text(),flush=True)
        assert status == 0, "measurement failed; preserve evidence"
        failed = False
    except BaseException:
        (root/"logs/failure.txt").write_text(traceback.format_exc())
        raise
    finally:
        m["failed"] = failed
        save(root,m)
        try:
            cleanup(root,m)
        except BaseException:
            m["cleanup_failure"] = traceback.format_exc()
            save(root,m)
            raise
        print("CLEANUP "+str(root),flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source",type=Path,default=Path(__file__).resolve().parents[1])
    p.add_argument("--source-ref",required=True)
    p.add_argument("--image",required=True)
    p.add_argument("--gateway",type=Path,required=True)
    p.add_argument("--native",type=Path,required=True)
    p.add_argument("--native-manifest",type=Path,required=True)
    p.add_argument("--client",type=Path,required=True)
    p.add_argument("--client-source",type=Path,help="exact source used to build an already frozen client")
    p.add_argument("--python-path",type=Path)
    p.add_argument("--parent",default=tempfile.gettempdir())
    p.add_argument("--engine",choices=("memtx","vinyl"),default="memtx")
    p.add_argument("--tenant",type=int,help="fixed tenant of SQL-routed composite PRIMARY KEY(tenant_id,id)")
    p.add_argument("--route-modes",default="epoch")
    p.add_argument("--cases-json",type=Path)
    p.add_argument("--native-allocs",action="store_true")
    p.add_argument("--check-locks",action="store_true",help="enable identical optional lock-copy audit; default off for normal performance")
    p.add_argument("--native-batch-reads",action="store_true",help="candidate-only gateway flag; direct modes still choose endpoints independently")
    p.add_argument("--dump-before-measure",action="store_true",help="Vinyl control: create persistent runs first; does not drop OS page cache")
    p.add_argument("--rows",type=int,default=100_000)
    p.add_argument("--payload",type=int,default=64)
    p.add_argument("--batches",default="1,8,32,128")
    p.add_argument("--shapes",default="hits,duplicates,miss")
    p.add_argument("--workers",default="1")
    p.add_argument("--modes",default="lua")
    p.add_argument("--trials",type=int,default=3)
    p.add_argument("--warmup",type=float,default=1)
    p.add_argument("--seconds",type=float,default=3)
    p.add_argument("--deadline",type=float,default=360)
    p.add_argument("--cpus",type=float,default=2)
    p.add_argument("--memory",default="1g")
    args = p.parse_args()
    assert 0 < args.rows <= 1_000_000 and 0 < args.payload <= 4096
    for path in (args.gateway,args.native,args.native_manifest,args.client,args.client_source,args.cases_json):
        if path is not None:
            assert path.is_file() and os.access(path,os.R_OK), "missing readable input: "+str(path)
    assert os.access(args.gateway,os.X_OK) and os.access(args.client,os.X_OK)
    run(args)


if __name__ == "__main__":
    main()
