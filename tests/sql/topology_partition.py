#!/usr/bin/env python3
"""Owned Docker RF3 partition: SQL reaches the isolated former leader.

Requires an image with Tarantool 3.9.0 and compatible Linux release artifacts
inside this checkout. Two private bridges separate replication from SQL.
Only this invocation's containers/networks are removed; all evidence survives.
"""
import argparse
import hashlib
import json
import pathlib
import subprocess
import time
import uuid

import pymysql
from topologytest import q, counters


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--docker", default="docker")
    ap.add_argument("--image", required=True)
    ap.add_argument("--gateway", required=True)
    ap.add_argument("--native", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--port", type=int, default=27150)
    a = ap.parse_args()
    root = pathlib.Path(__file__).resolve().parents[2]
    out = pathlib.Path(a.out).resolve(); out.mkdir(parents=True, exist_ok=False)
    def mounted(path): return "/work/" + str(pathlib.Path(path).resolve().relative_to(root))
    tag = "tarn188-partition-" + uuid.uuid4().hex[:10]
    containers = []; networks = []; clients = []
    report = dict(completed=False, events=[], cleanup=[],
                  source=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                  source_files={str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                                for sub in ("crates", "lua", "tests") for p in (root/sub).rglob("*")
                                if p.is_file() and p.suffix in (".rs", ".lua", ".py", ".sh", ".toml")},
                  binaries={str(p): hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
                            for p in (a.gateway, a.native)})
    def save(): (out/"report.json").write_text(json.dumps(report, indent=2))
    def event(phase, **data):
        report["events"].append(dict(phase=phase, at=time.time(), **data)); save()
    def d(*args, check=True):
        r = subprocess.run([a.docker, *map(str, args)], capture_output=True, text=True, timeout=30)
        with (out/"docker.log").open("a") as f:
            f.write(json.dumps(dict(args=args, rc=r.returncode, stdout=r.stdout, stderr=r.stderr))+"\n")
        if check and r.returncode: raise RuntimeError(r.stderr)
        return r.stdout.strip()
    def address(name, network):
        return json.loads(d("inspect", name))[0]["NetworkSettings"]["Networks"][network]["IPAddress"]
    def sql(port):
        deadline = time.monotonic()+30
        while True:
            try:
                c = pymysql.connect(host="127.0.0.1", port=port, user="t", password="",
                                    autocommit=True, read_timeout=8, write_timeout=8)
                clients.append(c); q(c, "SET innodb_lock_wait_timeout=2"); return c
            except pymysql.Error:
                if time.monotonic() >= deadline: raise
                time.sleep(.1)
    def control(node, body):
        code = ("local c=require('net.box').connect('127.0.0.1:"+str(3301+node)+
                "',{user='tarn',password='tarn',wait_connected=1}); "
                "local r=c:eval("+json.dumps(body)+
                "); print(require('json').encode(r)); c:close(); os.exit(0)")
        return json.loads(d("exec", nodes[node], "tarantool", "-e", code).splitlines()[-1])
    def leader(indices):
        deadline = time.monotonic()+30
        while True:
            for i in indices:
                try:
                    if control(i, "return not box.info.ro"):
                        return i
                except (RuntimeError, ValueError, IndexError): pass
            if time.monotonic() >= deadline: raise AssertionError("no confirmed writable leader")
            time.sleep(.1)
    def gateway(label, cfg, port, node):
        path = out/(label+".json"); path.write_text(json.dumps(cfg, indent=2))
        script = 'echo $$ > /data/'+label+'.pid; exec "$@" > /data/'+label+'.log 2>&1'
        d("exec", "-d", gw, "sh", "-c", script, "gateway", mounted(a.gateway), "--config", mounted(path),
          "--listen", "0.0.0.0:"+str(port), "--node-id", str(node), "--threads", "2", "--allow-all")
    try:
        report["image"] = json.loads(d("image", "inspect", a.image))[0]["Id"]
        for label in ("sql", "replication"):
            network = tag+"-"+label
            d("network", "create", "--internal", network); networks.append(network)
        nodes = []
        for i in range(3):
            name = tag+"-n"+str(i); nodes.append(name)
            d("run", "-d", "--name", name, "--network", networks[0], "--memory", "256m",
              "-v", str(root)+":/work:ro", a.image, "sh", "-c", "mkdir /data; exec sleep infinity")
            containers.append(name); d("network", "connect", networks[1], name)
        rep = [address(n, networks[1])+":"+str(3301+i) for i,n in enumerate(nodes)]
        sql_addrs = [address(n, networks[0])+":"+str(3301+i) for i,n in enumerate(nodes)]
        replicas = json.dumps(["tarn:tarn@"+x for x in rep])
        for i,name in enumerate(nodes):
            env = dict(TARN_LISTEN="0.0.0.0:"+str(3301+i), TARN_SHARD_ID=1, TARN_WORKDIR="/data",
                       TARN_REPLICAS=replicas, TARN_PEERS=json.dumps({"1":rep}), TARN_MEMORY_MB=32,
                       TARN_VINYL_MEMORY_MB=16, TARN_VINYL_CACHE_MB=0, TARN_WAL_MODE="fsync",
                       TARN_CHECK_LOCKS=1, TARN_CHECK_YIELDS=1, TARN_NATIVE=mounted(a.native))
            args = [item for k,v in env.items() for item in ("-e", k+"="+str(v))]
            d("exec", "-d", *args, name, "sh", "-c", "exec tarantool /work/lua/storage.lua > /data/node.log 2>&1")
        old = leader(range(3))
        for i in range(3): assert control(i, "return tarn_info().native"), "native module not active"
        gw = tag+"-gateways"
        d("run", "-d", "--name", gw, "--network", networks[0], "--memory", "256m",
          "-p", "127.0.0.1:"+str(a.port)+":4000", "-p", "127.0.0.1:"+str(a.port+1)+":4001",
          "-v", str(root)+":/work:ro", a.image, "sh", "-c", "mkdir /data; exec sleep infinity")
        containers.append(gw)
        cfg = dict(shards=[dict(id=1, replicas=sql_addrs)], pool_size=4,
                   rpc=dict(timeout_secs=2), topology=dict(max_connections=8, completion_connections=2,
                   idle_secs=1, fanout=2, active_branches=8, queued_branches=16), deadlock_interval=0)
        gateway("majority", cfg, 4000, 901)
        isolated_cfg = dict(cfg, shards=[dict(id=1, replicas=[sql_addrs[old]])])
        gateway("former", isolated_cfg, 4001, 902)
        good, former = sql(a.port), sql(a.port+1)
        q(good, "CREATE DATABASE partition188")
        q(good, "CREATE TABLE partition188.t(id INT PRIMARY KEY,v INT)")
        q(good, "INSERT INTO partition188.t VALUES(1,0)")
        assert q(former, "SELECT v FROM partition188.t WHERE id=1") == ((0,),)
        event("before_partition", old=old, counters=[counters(good),counters(former)], addresses=rep)
        old_ip = address(nodes[old], networks[1])
        d("network", "disconnect", networks[1], nodes[old])
        new = leader([i for i in range(3) if i != old])
        event("new_leader", old=old, new=new)
        # Read-only recovery can retry; submit the increment exactly once.
        deadline = time.monotonic()+20
        while True:
            try:
                assert q(good, "SELECT v FROM partition188.t WHERE id=1") == ((0,),); break
            except pymysql.Error as e:
                event("read_refusal", error=list(e.args))
                if time.monotonic() >= deadline: raise
                time.sleep(.1)
        assert q(good, "UPDATE partition188.t SET v=v+1 WHERE id=1") == 1
        assert q(good, "SELECT v FROM partition188.t WHERE id=1") == ((1,),)
        raw = control(old, "return box.space['partition188.t']:get{1}[2]")
        assert raw == 0, ("partition did not retain a stale raw tuple", raw)
        try:
            rows = q(former, "SELECT v FROM partition188.t WHERE id=1")
        except pymysql.Error as e:
            event("old_owner_refused", raw_value=raw, error=list(e.args))
        else: raise AssertionError(("isolated old-owner SQL acknowledged a stale read", rows))
        d("network", "connect", "--ip", old_ip, networks[1], nodes[old])
        deadline = time.monotonic()+20
        while control(old, "return box.space['partition188.t']:get{1}[2]") != 1:
            if time.monotonic() >= deadline: raise AssertionError("replication did not converge after heal")
            time.sleep(.1)
        for c in clients: c.close()
        clients.clear()
        for label in ("majority", "former"):
            pid = d("exec", gw, "cat", "/data/"+label+".pid")
            d("exec", gw, "kill", pid)
        gateway("healed0", cfg, 4000, 903); gateway("healed1", cfg, 4001, 904)
        for port in (a.port,a.port+1):
            c = sql(port); assert q(c, "SELECT v FROM partition188.t WHERE id=1") == ((1,),)
            status = counters(c); assert status["Tarn_storage_sockets_peak"] <= 8
            event("healed_restart", port=port, counters=status)
        report["completed"] = True
    finally:
        for c in clients: c.close()
        for name in containers:
            d("cp", name+":/data", str(out/name), check=False)
            d("rm", "-f", name, check=False)
            absent = not d("ps", "-aq", "--filter", "name=^/"+name+"$", check=False)
            report["cleanup"].append(dict(container=name, absent=absent))
        for network in reversed(networks): d("network", "rm", network, check=False)
        save()
    assert all(x["absent"] for x in report["cleanup"])
    print(json.dumps(dict(completed=report["completed"], report=str(out/"report.json"))))


if __name__ == "__main__": main()
