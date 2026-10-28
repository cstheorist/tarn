#!/usr/bin/env python3
"""Own matched SQL or direct batch reads, optionally with RF3/fsync storage.

Requires already-built binaries, Docker and PyMySQL. Only this fixture's exact
labelled containers/network and identity-checked child processes are stopped.
The same instrumented native module is loaded for both SQL gateway modes.
This is a read benchmark: RF3 verifies replication/durability configuration and
the committed corpus, rather than making a failover or write-throughput claim.
"""
import argparse
import datetime
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import traceback

from native_batch_fixture import call, freeze, observer, port, save, sha


TASK = "tarn-native-batch-sql"


def identity(pid):
    """Capture parent/start/argv, not merely a reusable numeric PID."""
    result = {}
    for field in ("ppid", "lstart", "command"):
        r = call(["ps", "-ww", "-p", str(pid), "-o", field+"="], check=False)
        if r.returncode or not r.stdout.strip():
            return None
        result[field] = r.stdout.strip()
    return result


def launch(root, m, name, argv, processes):
    with (root/"logs"/(name+".log")).open("w") as log:
        p = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=log,
                             stderr=subprocess.STDOUT, start_new_session=True)
    seen = identity(p.pid)
    assert seen and int(seen["ppid"]) == os.getpid(), "new child identity unavailable"
    assert os.getpgid(p.pid) == p.pid, "child did not receive its private process group"
    record = {"name": name, "pid": p.pid, "argv": argv, "identity": seen}
    m.setdefault("processes", []).append(record)
    processes.append((p, record))
    save(root, m)
    return p


def owned_members(root, p, record):
    """Include an observer child surviving its Python controller's exit."""
    listing = call(["ps", "-ww", "-axo", "pid,pgid,stat,command"]).stdout
    members = []
    for line in listing.splitlines():
        fields = line.split(None, 3)
        if len(fields) != 4 or not fields[0].isdigit() or not fields[1].isdigit():
            continue
        if int(fields[1]) != p.pid or fields[2].startswith("Z"):
            continue
        pid, command = int(fields[0]), fields[3]
        if pid == p.pid:
            current = identity(pid)
            if current is None:
                continue
            assert current == record["identity"], "child identity changed; not signalling"
        else:
            assert record["name"] == "measure" and str(root/"bin/native_batch") in command, \
                "unexpected member of owned process group"
        members.append(pid)
    return members


def stop_child(root, p, record):
    # Recheck parent/argv and every inherited session member immediately
    # before EACH group signal. A returned controller is not a child drain.
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if not owned_members(root, p, record):
            break
        try:
            os.killpg(p.pid, sig)
        except ProcessLookupError:
            continue
        deadline = time.monotonic()+5
        while owned_members(root, p, record) and time.monotonic() < deadline:
            p.poll()
            time.sleep(.1)
    assert not owned_members(root, p, record), "owned process group remains"
    p.wait(timeout=1)
    record["exit"] = p.returncode
    record["group_absent"] = True


def check_labels(info, root):
    labels = info["Config"]["Labels"]
    assert labels.get("task") == TASK and labels.get("fixture") == root.name


def cleanup(root, m, processes):
    errors = []
    for p, record in reversed(processes):
        try:
            stop_child(root, p, record)
        except BaseException:
            errors.append(traceback.format_exc())
    for node in m.get("nodes", []):
        try:
            result = call(["docker", "inspect", node["name"]], check=False)
            if result.returncode:
                assert "No such" in result.stderr, result.stderr
                continue
            info = json.loads(result.stdout)[0]
            assert info["Name"] == "/"+node["name"]
            check_labels(info, root)
            if node.get("id"):
                assert info["Id"] == node["id"]
            node["id"] = info["Id"]
            if info["State"]["Running"]:
                memory = call(["docker", "exec", info["Id"], "sh", "-c",
                               "cat /sys/fs/cgroup/memory.events; cat /sys/fs/cgroup/memory.peak"], check=False)
                (root/"logs"/(node["alias"]+"-memory.txt")).write_text(memory.stdout+memory.stderr)
                node["memory_events"] = {line.split()[0]: int(line.split()[1]) for line in memory.stdout.splitlines()
                                         if len(line.split()) == 2 and line.split()[1].isdigit()}
                call(["docker", "stop", "--time", "10", info["Id"]], timeout=15)
            final = json.loads(call(["docker", "inspect", info["Id"]]).stdout)[0]
            (root/"logs"/(node["alias"]+"-container.json")).write_text(json.dumps(final, indent=2)+"\n")
            node["oom_killed"] = final["State"]["OOMKilled"]
            logs = call(["docker", "logs", info["Id"]], check=False)
            (root/"logs"/(node["alias"]+"-docker.log")).write_text(logs.stdout+logs.stderr)
            call(["docker", "rm", "-v", info["Id"]])
            assert call(["docker", "inspect", info["Id"]], check=False).returncode != 0
        except BaseException:
            errors.append(traceback.format_exc())
    try:
        if m.get("network_attempted"):
            result = call(["docker", "network", "inspect", m["network_name"]], check=False)
            if result.returncode == 0:
                info = json.loads(result.stdout)[0]
                assert info["Name"] == m["network_name"]
                assert info["Labels"].get("task") == TASK and info["Labels"].get("fixture") == root.name
                assert not info["Containers"], "owned network still has participants"
                if m.get("network_id"):
                    assert info["Id"] == m["network_id"]
                call(["docker", "network", "rm", info["Id"]])
            else:
                assert "not found" in result.stderr, result.stderr
            assert call(["docker", "network", "inspect", m["network_name"]], check=False).returncode != 0
        for rel, digest in m.get("frozen_sha256", {}).items():
            assert sha(root/rel) == digest, "frozen file changed: "+rel
    except BaseException:
        errors.append(traceback.format_exc())
    # Archive only after every data-owning container was removed. On a cleanup
    # failure retain the original data rather than copying a live WAL image.
    if not errors:
        index = [{"path": str(p.relative_to(root)), "bytes": p.stat().st_size, "sha256": sha(p)}
                 for p in sorted((root/"data").rglob("*")) if p.is_file()]
        (root/"logs/data-index.json").write_text(json.dumps(index, indent=2)+"\n")
        archive = root/"runtime-wal.tar"
        with tarfile.open(archive, "w") as t:
            t.add(root/"data", arcname="data")
        with tarfile.open(archive) as t:
            for item in index:
                assert hashlib.sha256(t.extractfile(item["path"]).read()).hexdigest() == item["sha256"]
        shutil.rmtree(root/"data")
        m["archive"] = {"sha256": sha(archive), "files": len(index), "verified_offline": True}
    m["cleanup"] = {"errors": errors, "owned_services_absent": not errors,
                    "data_removed": not (root/"data").exists(),
                    "frozen_files_unchanged": len(m.get("frozen_sha256", {})) if not errors else None}
    m["stage"] = "cleaned" if not errors else "cleanup-incomplete"
    save(root, m)
    if errors:
        raise RuntimeError(errors)


READINESS = r"""
local meta=box.space._tarn_meta
local complete=meta and box.space._tarn_scratch and box.space._tarn_scratch.index.pk
 and box.space._tarn_lazy_floor and box.space._tarn_lazy_floor.index.bucket_id
 and meta:get{'shard_id'}~=nil
if not complete then return require('json').encode{ready=false} end
local ffi=require('ffi');local i=tarn_info()
local n=ffi.cast('struct tarn_ns *',box.func['tarn_storage.ts_state']:call())
local follow=0;local replication={}
for id,p in pairs(box.info.replication) do
 replication[tostring(id)]={uuid=tostring(p.uuid),upstream=p.upstream,downstream=p.downstream}
 if id~=box.info.id and p.upstream and p.upstream.status=='follow' then follow=follow+1 end
end
return require('json').encode{ready=true,id=box.info.id,ro=box.info.ro,
 uuid=tostring(box.info.uuid),status=box.info.status,follow=follow,
 clock=i.clock,native=i.native,layout=tonumber(n.layout),size=ffi.sizeof('struct tarn_ns'),
 lock_copy_errors=i.stats.lock_copy_errors,
 test_hooks=tarn.cfg.test_hooks,check_locks=tarn.cfg.check_locks,sync=tarn.cfg.sync,
 wal=box.cfg.wal_mode,quorum=box.cfg.replication_synchro_quorum,
 election=box.info.election,synchro=box.info.synchro,vclock=box.info.vclock,
 replication=replication,version=box.info.version}
"""


def wait_cluster(root, m, expected):
    deadline = time.monotonic()+60
    last = None
    while time.monotonic() < deadline:
        try:
            states = [observer(root, node["endpoint"], READINESS) for node in m["nodes"]]
            if all(s["ready"] and s["status"] == "running" and s["follow"] >= expected-1 for s in states):
                leaders = [i for i, s in enumerate(states) if not s["ro"]]
                if len(leaders) == 1:
                    for s in states:
                        assert s["layout"] == 14 and s["size"] == 392
                        assert s["test_hooks"] is False and s["check_locks"] is True
                        assert s["wal"] == ("fsync" if expected == 3 else "write")
                        assert s["sync"] == (expected == 3)
                        assert s["lock_copy_errors"] == 0
                    leader = leaders[0]
                    if (not states[leader]["native"] or states[leader]["native"]["ready"] != 1
                            or not states[leader]["clock"]["read_ready"] or states[leader]["clock"]["covered_peer_count"] < 1):
                        raise RuntimeError("leader native state is not ready yet")
                    if expected == 3:
                        assert states[leader]["synchro"]["queue"]["owner"] == states[leader]["id"]
                        assert states[leader]["quorum"] == "N / 2 + 1" or states[leader]["quorum"] == 2
                    return states, leader
            last = states
        except (RuntimeError, subprocess.TimeoutExpired, KeyError) as exc:
            last = str(exc)
        time.sleep(.2)
    raise RuntimeError("owned shard did not join before writes: "+str(last))


def corpus(root, endpoint, database, args):
    code = r"""
local s=assert(box.space[__TABLE__]);local d=require('digest');local mp=require('msgpack')
box.begin{txn_isolation='read-confirmed'}
local n,sum,h,chunk=0,0,'',{}
for _,r in s.index[0]:pairs() do
 n=n+1;assert(r[1]==__TENANT__ and r[2]==n and r[3]==string.rep('x',__PAYLOAD__))
 sum=sum+r[2];chunk[#chunk+1]=mp.encode(r)
 if #chunk==128 then h=d.sha256_hex(h..table.concat(chunk));chunk={} end
end
if #chunk>0 then h=d.sha256_hex(h..table.concat(chunk)) end
assert(n==__ROWS__);box.commit()
return require('json').encode{space=s.id,engine=s.engine,is_sync=s.is_sync,rows=n,sum=sum,hash=h,
 vclock=box.info.vclock,synchro=box.info.synchro,uuid=tostring(box.info.uuid),id=box.info.id}
"""
    for key, value in (("__TABLE__", json.dumps(database+".t")), ("__TENANT__", str(args.tenant)),
                       ("__PAYLOAD__", str(args.payload)), ("__ROWS__", str(args.rows))):
        code = code.replace(key, value)
    return observer(root, endpoint, code)


def confirm_corpus(root, m, database, args, leader, label):
    wanted = corpus(root, m["nodes"][leader]["endpoint"], database, args)
    assert wanted["is_sync"] == (args.replicas == 3)
    results = []
    for node in m["nodes"]:
        deadline = time.monotonic()+30
        while True:
            try:
                row = corpus(root, node["endpoint"], database, args)
                assert row["hash"] == wanted["hash"] and row["rows"] == wanted["rows"]
                assert row["is_sync"] == (args.replicas == 3)
                have = {int(k): v for k, v in row["vclock"].items()}
                assert all(int(k) == 0 or have.get(int(k), 0) >= lsn for k, lsn in wanted["vclock"].items())
                assert row["synchro"]["queue"]["len"] == 0
                results.append(row)
                break
            except (RuntimeError, AssertionError, subprocess.TimeoutExpired):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.1)
    m[label] = results
    save(root, m)
    return wanted


def measurement_command(root, m, args, database):
    common = ["--client", str(root/"bin/native_batch"), "--shard", m["leader_endpoint"],
              "--rows", str(args.rows), "--payload", str(args.payload), "--tenant", str(args.tenant),
              "--trials", str(args.trials), "--seconds", str(args.seconds), "--warmup", str(args.warmup),
              "--batches", args.batches, "--shapes", args.shapes, "--workers", args.workers,
              "--output", str(root/"measurements"), "--clk-tck", str(m["clk_tck"])]
    if args.direct_storage:
        route = m["direct_route"]
        command = [sys.executable, str(root/"harness/native_batch.py"), *common,
                   "--space", str(route["space"]), "--epoch", str(route["epoch"]),
                   "--membership", str(route["membership"]), "--buckets", str(route["count"]),
                   "--bucket", str(route["physical_bucket"]), "--route-modes", "buckets",
                   "--modes", "lua,native", "--conns", "1", "--native-allocs"]
        if args.cases_json:
            command += ["--cases-json", str(root/"harness/case-plan.json")]
        return command
    command = [sys.executable, str(root/"harness/native_batch_sql.py"), *common,
               "--lua-gateway", m["gateways"]["lua"]["endpoint"],
               "--native-gateway", m["gateways"]["native"]["endpoint"], "--database", database]
    if args.python_path:
        command += ["--python-path", str(args.python_path.resolve())]
    return command


def run(args):
    if args.python_path:
        sys.path.insert(0, str(args.python_path.resolve()))
    import pymysql
    root = Path(tempfile.mkdtemp(prefix="tarn-native-sql-rf"+str(args.replicas)+"-", dir=args.parent))
    m = {"task": TASK, "fixture": root.name, "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
         "stage": "preparing", "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
         "image": args.image, "network_name": root.name+"-net", "nodes": []}
    m["python_driver"] = {"package": "PyMySQL", "version": importlib.metadata.version("PyMySQL"),
                          "module": pymysql.__file__, "python": sys.version}
    (root/"data").mkdir(); (root/"logs").mkdir()
    save(root, m)
    processes = []
    print("FIXTURE "+str(root), flush=True)
    failed = True
    try:
        freeze(args, root, m)
        for name in ("native_batch_sql.py", "native_batch_sql_fixture.py"):
            dest = root/"harness"/name
            shutil.copy2(Path(__file__).parent/name, dest)
            compile(dest.read_text(), str(dest), "exec")
            m["frozen_sha256"]["harness/"+name] = sha(dest)
        save(root, m)
        if args.prepare_only:
            failed = False
            return
        m["network_attempted"] = True
        save(root, m)
        m["network_id"] = call(["docker", "network", "create", "--label", "task="+TASK,
                                 "--label", "fixture="+root.name, m["network_name"]]).stdout.strip()
        replicas = ["tarn:tarn@node"+str(i)+":3301" for i in range(1, args.replicas+1)]
        for i in range(1, args.replicas+1):
            node = {"name": root.name+"-node"+str(i), "alias": "node"+str(i), "port": port()}
            node["endpoint"] = "127.0.0.1:"+str(node["port"])
            m["nodes"].append(node)
        save(root, m)
        # Start ALL replicas before probing or allowing schema/user writes.
        for node in m["nodes"]:
            command = ["docker", "create", "--name", node["name"], "--label", "task="+TASK,
                       "--label", "fixture="+root.name, "--network", m["network_name"],
                       "--network-alias", node["alias"], "--cpus", str(args.cpus), "--memory", args.memory,
                       "--memory-swap", args.memory, "--pids-limit", "128", "--tmpfs", "/tmp:rw,exec,size=134217728",
                       "-p", "127.0.0.1:"+str(node["port"])+":3301",
                       "--mount", "type=bind,src="+str(root/"repo")+",dst=/source,readonly",
                       "--mount", "type=bind,src="+str(root/"bin")+",dst=/artifacts,readonly",
                       "--mount", "type=bind,src="+str(root/"data")+",dst=/data", "--entrypoint", "/bin/bash"]
            env = {"TARN_SHARD_ID": "1", "TARN_LISTEN": node["alias"]+":3301",
                   "TARN_WORKDIR": "/data/"+node["alias"], "TARN_REPLICAS": json.dumps(replicas if args.replicas == 3 else []),
                   "TARN_PEERS": json.dumps({"1": replicas}), "TARN_NATIVE": "/artifacts/libtarn_storage.so",
                   "TARN_MEMORY_MB": "256", "TARN_VINYL_MEMORY_MB": "32", "TARN_VINYL_CACHE_MB": "0",
                   "TARN_VINYL_READ_THREADS": "1", "TARN_VINYL_WRITE_THREADS": "2", "TARN_VINYL_TIMEOUT": "0.25",
                   "TARN_WAL_MODE": "fsync" if args.replicas == 3 else "write", "TARN_CHECK_LOCKS": "1",
                   "TARN_TEST_HOOKS": "", "TARN_LOG_LEVEL": "4"}
            for key, value in env.items():
                command += ["-e", key+"="+value]
            command += [args.image, "-c", "unset TT_APP_NAME TT_INSTANCE_NAME; exec tarantool /source/lua/storage.lua > /data/"+node["alias"]+".log 2>&1"]
            node["command"] = command
            node["id"] = call(command, timeout=15).stdout.strip()
            save(root, m)
        for node in m["nodes"]:
            call(["docker", "start", node["id"]], timeout=15)
        m["joined_before_writes"], leader = wait_cluster(root, m, args.replicas)
        m["leader_index"] = leader
        m["leader_endpoint"] = m["nodes"][leader]["endpoint"]
        gateways = {}
        for number, mode in enumerate(("lua", "native")):
            conf = {"shards": [{"id": 1, "replicas": [n["endpoint"] for n in m["nodes"]]}],
                    "user": "tarn", "password": "tarn", "bucket_count": 480, "ack_on_prepare": False,
                    "native_batch_reads": mode == "native"}
            config = root/(mode+"-cluster.json")
            config.write_text(json.dumps(conf, indent=2)+"\n")
            gp = port()
            argv = [str(root/"bin/tarn-server"), "--config", str(config), "--listen", "127.0.0.1:"+str(gp),
                    "--threads", "2", "--pool", "4", "--node-id", str(14111+number),
                    "--default-engine", "memtx", "--user", "sb:"]
            gateways[mode] = {"endpoint": "127.0.0.1:"+str(gp), "port": gp}
            proc = launch(root, m, mode+"-gateway", argv, processes)
            for _ in range(100):
                try:
                    c = pymysql.connect(host="127.0.0.1", port=gp, user="sb", password="", autocommit=True,
                                        charset="utf8mb4", connect_timeout=1, read_timeout=30, write_timeout=30)
                    c.close()
                    break
                except pymysql.MySQLError:
                    assert proc.poll() is None, "owned gateway exited"
                    time.sleep(.1)
            else:
                raise RuntimeError("owned gateway did not become ready")
        m["gateways"] = gateways
        save(root, m)
        database = "nbs_"+root.name.rsplit("-", 1)[1]
        c = pymysql.connect(host="127.0.0.1", port=gateways["lua"]["port"], user="sb", password="", autocommit=True,
                            charset="utf8mb4", connect_timeout=3, read_timeout=30, write_timeout=30)
        try:
            with c.cursor() as cursor:
                cursor.execute("CREATE DATABASE "+database)
                cursor.execute("CREATE TABLE "+database+".t (tenant_id INT NOT NULL,id INT NOT NULL,p VARCHAR("+
                               str(args.payload)+") NOT NULL,PRIMARY KEY(tenant_id,id)) PARTITION BY KEY(tenant_id) ENGINE=memtx")
                for lo in range(1, args.rows+1, 1000):
                    cursor.executemany("INSERT INTO "+database+".t VALUES (%s,%s,%s)",
                                       [(args.tenant, i, "x"*args.payload) for i in range(lo, min(args.rows+1, lo+1000))])
        finally:
            c.close()
        m["dataset"] = confirm_corpus(root, m, database, args, leader, "corpus_before")
        m["native_ready_before"], final_leader = wait_cluster(root, m, args.replicas)
        assert final_leader == leader, "leader changed before measurement"
        m["jit_gc_setup"] = observer(root, m["leader_endpoint"],
            "collectgarbage('restart');jit.on();return require('json').encode{jit=jit.status(),gc_restart=true}")
        assert m["jit_gc_setup"] == {"jit": True, "gc_restart": True}
        m["clk_tck"] = int(call(["docker", "exec", m["nodes"][leader]["id"], "getconf", "CLK_TCK"]).stdout.strip())
        m["resource_context"] = {"host": platform.platform(), "logical_cpus": os.cpu_count(),
                                 "load_before": os.getloadavg(), "per_replica_cpus": args.cpus,
                                 "per_replica_memory": args.memory, "gateway_threads": 2, "gateway_pool": 4,
                                 "read_claim": "matched end-to-end SQL; RF3 is a fsync read control, not a failover/write benchmark"}
        if args.direct_storage:
            code = r"""
local b=tarn_buckets();local s=assert(box.space[__TABLE__])
local bucket=require('digest').crc32(require('msgpack').encode(__TENANT__))%b[2]+1
return require('json').encode{space=s.id,epoch=b[3],membership=b[6],count=b[2],physical_bucket=bucket}
""".replace("__TABLE__", json.dumps(database+".t")).replace("__TENANT__", str(args.tenant))
            m["direct_route"] = observer(root, m["leader_endpoint"], code)
            assert m["direct_route"]["space"] == m["dataset"]["space"]
            m["resource_context"]["read_claim"] = (
                "matched strong Lua/native storage RPCs; RF3 fsync read control; no failover/write-throughput claim")
            m["resource_context"]["direct_client_connections"] = 1
            m["resource_context"]["idle_gateway_processes"] = 2
        command = measurement_command(root, m, args, database)
        m["measure_command"] = command
        m["stage"] = "measuring"
        save(root, m)
        measure = launch(root, m, "measure", command, processes)
        try:
            m["measure_exit"] = measure.wait(timeout=args.deadline)
        except subprocess.TimeoutExpired:
            stop_child(root, measure, processes[-1][1])
            raise
        print((root/"logs/measure.log").read_text(), flush=True)
        assert m["measure_exit"] == 0, "SQL measurement failed; raw evidence retained"
        confirm_corpus(root, m, database, args, leader, "corpus_after")
        m["native_ready_after"], final_leader = wait_cluster(root, m, args.replicas)
        assert final_leader == leader, "leader changed during measurement"
        assert all(s["native"]["ready"] == 1 for s in (m["native_ready_before"][leader], m["native_ready_after"][leader]))
        assert all(s["lock_copy_errors"] == 0 for s in m["native_ready_after"])
        m["resource_context"]["load_after"] = os.getloadavg()
        failed = False
    except BaseException:
        (root/"logs/failure.txt").write_text(traceback.format_exc())
        raise
    finally:
        m["failed"] = failed
        save(root, m)
        cleanup(root, m, processes)
        print("CLEANUP "+str(root), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, default=Path(__file__).resolve().parents[1])
    p.add_argument("--source-ref", required=True)
    p.add_argument("--image", required=True)
    for name in ("gateway", "native", "native-manifest", "client"):
        p.add_argument("--"+name, type=Path, required=True)
    p.add_argument("--client-source", type=Path)
    p.add_argument("--python-path", type=Path)
    p.add_argument("--direct-storage", action="store_true", help="measure strong Lua/native storage RPCs on the same owned replica set")
    p.add_argument("--cases-json", type=Path, help="direct-storage case list for native_batch.py; avoids a Cartesian matrix")
    p.add_argument("--parent", default=tempfile.gettempdir())
    p.add_argument("--replicas", type=int, choices=(1, 3), default=1)
    p.add_argument("--rows", type=int, default=100_000)
    p.add_argument("--payload", type=int, default=64)
    p.add_argument("--tenant", type=int, default=7)
    p.add_argument("--trials", type=int, default=5)
    p.add_argument("--seconds", type=float, default=5)
    p.add_argument("--warmup", type=float, default=1)
    p.add_argument("--batches", default="8,32,128")
    p.add_argument("--shapes", default="hits,duplicates,miss")
    p.add_argument("--workers", default="1")
    p.add_argument("--deadline", type=float, default=900)
    p.add_argument("--cpus", type=float, default=2)
    p.add_argument("--memory", default="1g")
    p.add_argument("--prepare-only", action="store_true", help="freeze and verify inputs without starting services")
    args = p.parse_args()
    assert args.direct_storage or args.cases_json is None, "--cases-json requires --direct-storage"
    assert 0 < args.rows <= 1_000_000 and 0 < args.payload <= 4096 and args.tenant >= 0
    assert args.trials > 0 and args.seconds > 0 and args.warmup > 0 and args.deadline > 0
    for path in (args.gateway, args.native, args.native_manifest, args.client, args.client_source, args.cases_json):
        if path is not None:
            assert path.is_file() and os.access(path, os.R_OK), "missing readable input: "+str(path)
    assert os.access(args.gateway, os.X_OK) and os.access(args.client, os.X_OK)
    run(args)


if __name__ == "__main__":
    main()
