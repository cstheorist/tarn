#!/usr/bin/env python3
"""Run the multi-host comparison from the control node.

    orchestrate.py [--hosts FILE] [--results DIR] COMMAND [ARGS]

COMMAND:
  prepare             wait for the workers, install the bundle and the
                      runtime packages everywhere, fetch TiDB and YugabyteDB
                      (pinned, verified) and put them on their nodes
  deploy SYSTEM       start a fresh cluster (tarn | tidb | yb)
  stop SYSTEM         stop it and delete its data
  load SYSTEM         create the sysbench tables (from load1, one thread per table)
  bench SYSTEM [--variant V] [--tests T,..] [--threads N,..] [--time S]
                      the sysbench matrix, every load generator at once
  fault SYSTEM KIND   a failure under load: node-kill | zone-cut (region topology)
  system RUN          deploy, load, bench (default variant and the other one),
                      faults, stop: one system's whole share of a run
                      (RUN: tarn | tarn-fsync | tidb | yb)
  all                 prepare, then `system` for tarn, tarn-fsync, tidb and yb
  status              what runs where
  estimate            how long `all` takes with the current settings

Settings (environment): TABLES (16) SIZE (1000000) THREADS (16,64,256,512:
total over all load generators) TIME (60 s) TESTS (the seven OLTP workloads)
VARIANT_TESTS (oltp_read_write,oltp_write_only,oltp_update_index)
FSYNC_TESTS (oltp_update_index,oltp_insert,oltp_write_only,oltp_read_write) WARMUP (20 s)
TARN_WAL (write | fsync) TARN_IPROTO_THREADS (2) FAULT_TIME (120 s)
REHEARSAL (1: the local fake fleet; skips VM-only steps)

Results: RESULTS/<run>/<system>/{bench.csv, load.json, fault-*.csv, raw/}.
"""
import argparse
import concurrent.futures as cf
import csv
import json
import os
import re
import shlex
import subprocess
import sys
import threading
import time

TB = "/data/tb"                      # the bundle on every node
RUN = "/data/run"                    # services' data and logs on every node
PKG = "/data/pkg"                    # vendor packages, on the control node
SVC = f"{TB}/tarn/bench/cloud/node/svc.sh"
HERE = os.path.dirname(os.path.abspath(__file__))
SYSTEMS = ("tarn", "tidb", "yb")
# What `all` runs. tarn-fsync: Tarn again with fsync on every commit (its
# default writes the WAL without fsync; TiKV syncs its Raft log), so the
# durability difference is measured, not assumed. Results go under the label.
RUNS = ("tarn", "tidb", "yb", "tarn-fsync")   # the optional pass last
LABEL = {}                                # system -> results label, while running a label
SB_PASS = "sbpass-not-secret"
ALL_TESTS = ("oltp_point_select,oltp_read_only,oltp_read_write,oltp_update_index,"
             "oltp_update_non_index,oltp_insert,oltp_write_only")

def env(name, default):
    v = os.environ.get(name)
    return v if v not in (None, "") else default

S = {
    "tables": int(env("TABLES", "16")),
    "size": int(env("SIZE", "1000000")),
    "threads": [int(x) for x in env("THREADS", "16,64,256,512").split(",")],
    "time": int(env("TIME", "60")),
    "tests": env("TESTS", ALL_TESTS).split(","),
    "variant_tests": env("VARIANT_TESTS", "oltp_read_write,oltp_write_only,oltp_update_index").split(","),
    "fsync_tests": env("FSYNC_TESTS", "oltp_update_index,oltp_insert,oltp_write_only,oltp_read_write").split(","),
    "warmup": int(env("WARMUP", "20")),
    "tarn_wal": env("TARN_WAL", "write"),
    "tarn_iproto_threads": int(env("TARN_IPROTO_THREADS", "2")),
    "fault_time": int(env("FAULT_TIME", "120")),
    "rehearsal": env("REHEARSAL", "0") == "1",
}
# The isolation each system runs by default, and the one variant run
# beside it (READ COMMITTED for the two MySQL systems, SERIALIZABLE for
# YugabyteDB, whose default is snapshot isolation).
VARIANTS = {
    "tarn": ("serializable", "read-committed"),
    "tidb": ("default", "read-committed"),
    "yb": ("default", "serializable"),
}
# Errors sysbench retries (counted in the errors column).
MYSQL_IGNORE = {"tarn": "1213,1205,1020,1062,1180",
                "tidb": "1213,1205,1020,1062,8002,8022,8028,9007"}

LOCK = threading.Lock()
def log(msg):
    with LOCK:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Fleet:
    def __init__(self, path, key, results):
        self.p = json.load(open(path))
        self.key = key
        self.user = self.p.get("ssh_user", "bench")
        self.hosts = {h["name"]: h for h in self.p["hosts"]}
        self.run_id = self.p.get("run_id", "rehearsal")
        self.results = os.path.join(results, self.run_id)
        os.makedirs(self.results, exist_ok=True)
        self.known = os.path.join(os.path.dirname(os.path.abspath(path)), "known_hosts")

    def role(self, r):
        return [h for h in self.p["hosts"] if h["role"] == r]

    def storage(self, shard=None):
        return [h for h in self.role("storage") if shard is None or h["shard"] == shard]

    @property
    def rf(self):
        return self.p["rf"]

    @property
    def shards(self):
        return self.p["shards"]

    def ip(self, name):
        return self.hosts[name]["internal_ip"]

    # --- remote execution ------------------------------------------------
    def ssh_cmd(self, host):
        return ["ssh", "-i", self.key, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
                "-o", f"UserKnownHostsFile={self.known}", "-o", "ConnectTimeout=10",
                "-o", "ServerAliveInterval=15", "-o", "LogLevel=ERROR", f"{self.user}@{self.ip(host)}"]

    def sh(self, host, script, check=True, timeout=None, quiet=False, stdin=None):
        """Run a bash script on host; return stdout. With `stdin` (a file or
        pipe streamed to the script), the script travels as the command."""
        if stdin is None:
            p = subprocess.run(self.ssh_cmd(host) + ["bash -s"], input=script, capture_output=True,
                               text=True, timeout=timeout)
        else:
            p = subprocess.run(self.ssh_cmd(host) + ["bash -c " + shlex.quote(script)], stdin=stdin,
                               capture_output=True, timeout=timeout)
        out = p.stdout if isinstance(p.stdout, str) else p.stdout.decode(errors="replace")
        err = p.stderr if isinstance(p.stderr, str) else p.stderr.decode(errors="replace")
        if check and p.returncode != 0:
            raise RuntimeError(f"{host}: exit {p.returncode}\n--- script\n{script}\n--- stderr\n{err[-3000:]}\n--- stdout\n{out[-2000:]}")
        if not quiet and err.strip() and p.returncode != 0:
            log(f"{host}: {err.strip()[-500:]}")
        return out

    def par(self, hosts, fn, what=""):
        """fn(host) for every host in parallel; raise on the first failure."""
        names = [h["name"] if isinstance(h, dict) else h for h in hosts]
        if not names:
            return {}
        with cf.ThreadPoolExecutor(max_workers=min(32, len(names))) as ex:
            futs = {ex.submit(fn, n): n for n in names}
            res, errs = {}, []
            for f in cf.as_completed(futs):
                try:
                    res[futs[f]] = f.result()
                except Exception as e:  # noqa: BLE001 - report every failure
                    errs.append(f"{futs[f]}: {e}")
            if errs:
                raise RuntimeError(f"{what} failed on {len(errs)} host(s):\n" + "\n".join(errs))
            return res

    def push_dir(self, host, local_dir, remote_dir):
        """Copy a directory's contents to host (tar over SSH)."""
        tar = subprocess.Popen(["tar", "-C", local_dir, "-cf", "-", "."], stdout=subprocess.PIPE)
        self.sh(host, f"mkdir -p {remote_dir} && tar -C {remote_dir} -xf -", stdin=tar.stdout)
        tar.wait()

    def svc(self, host, op, name, *cmd, env_vars=None, check=True):
        script = ""
        if env_vars is not None:
            script += f"mkdir -p {RUN}/svc && cat > {RUN}/svc/{name}.env << 'EOF'\n"
            script += "".join(f"{k}={shlex.quote(str(v))}\n" for k, v in env_vars.items()) + "EOF\n"
        script += " ".join(shlex.quote(x) for x in [SVC, op, name, *cmd])
        return self.sh(host, script, check=check)

    def local(self, script, check=True, timeout=None):
        p = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=timeout)
        if check and p.returncode != 0:
            raise RuntimeError(f"local: exit {p.returncode}\n{script}\n{p.stderr[-3000:]}\n{p.stdout[-1000:]}")
        return p.stdout

    def label(self, system):
        return LABEL.get(system, system)

    def save(self, rel, text):
        head, _, tail = rel.partition("/")
        path = os.path.join(self.results, LABEL.get(head, head) + ("/" + tail if tail else ""))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a") as f:
            f.write(text)
        return path


def wait_until(what, fn, timeout, every=1.0):
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        try:
            if fn():
                return
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(every)
    raise RuntimeError(f"timed out after {timeout}s waiting for {what}" + (f": {last}" if last else ""))


def versions():
    v = {}
    for line in open(os.path.join(HERE, "versions.env")):
        m = re.match(r"^([A-Z_]+)=(.*)$", line.strip())
        if m:
            v[m.group(1)] = m.group(2).strip('"')
    return v


# --- prepare -----------------------------------------------------------------

def prepare(f):
    workers = [h for h in f.p["hosts"] if h["role"] != "control"]
    log(f"waiting for {len(workers)} workers")
    def ready(n):
        marker = "true" if S["rehearsal"] else "test -f /var/run/tarn-bench-ready"
        wait_until(f"{n} ready", lambda: f.sh(n, marker, check=True, quiet=True) is not None, 600, 5)
        f.sh(n, "sudo chown $(id -u):$(id -g) /data && mkdir -p /data/run /data/tb")
    f.par(workers, ready, "waiting for workers")

    log("bundle -> workers")
    f.par(workers, lambda n: f.push_dir(n, TB, TB), "copying the bundle")
    # dpkg waits for a lock an apt run at boot may still hold. (In the
    # rehearsal every host shares one package database: install once.)
    dpkg = (f"for i in $(seq 60); do sudo dpkg -i {TB}/debs/*.deb > /tmp/dpkg.log 2>&1 && exit 0; "
            f"grep -q 'lock' /tmp/dpkg.log || break; sleep 5; done; cat /tmp/dpkg.log; exit 1")
    f.par([{"name": "control"}] + ([] if S["rehearsal"] else workers), lambda n: f.sh(n, dpkg),
          "installing runtime packages")
    if not S["rehearsal"]:
        f.local("sudo apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "
                "mysql-client postgresql-client jq > /dev/null")

    v = versions()
    os.makedirs(PKG, exist_ok=True)
    log(f"TiDB {v['TIDB_VERSION']} (tiup, signed mirror)")
    f.local(f"""
set -e
export PATH=$HOME/.tiup/bin:$PATH
command -v tiup > /dev/null || curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh > /dev/null 2>&1
export PATH=$HOME/.tiup/bin:$PATH
tiup install pd:{v['TIDB_VERSION']} tikv:{v['TIDB_VERSION']} tidb:{v['TIDB_VERSION']} > /dev/null
mkdir -p {PKG}/tidb/bin
for c in pd tikv tidb; do
  b=$(find $HOME/.tiup/components/$c/{v['TIDB_VERSION']} -maxdepth 2 -type f -name "$c-server" | head -1)
  install -m 0755 "$b" {PKG}/tidb/bin/
done
{PKG}/tidb/bin/tidb-server -V | head -3 > {PKG}/tidb/VERSION
""", timeout=1800)
    f.par(f.storage(), lambda n: f.sh(n, f"mkdir -p {RUN}/../pkg/tidb/bin") and None, "")
    def push_tidb(n):
        role = f.hosts[n]["role"]
        bins = ["tikv-server"] if role == "storage" else ["pd-server", "tidb-server"]
        for b in bins:
            with open(f"{PKG}/tidb/bin/{b}", "rb") as src:
                f.sh(n, f"mkdir -p {PKG}/tidb/bin && cat > {PKG}/tidb/bin/{b} && chmod 755 {PKG}/tidb/bin/{b}",
                     stdin=src)
    f.par(f.storage() + f.role("sql"), push_tidb, "copying TiDB")

    log(f"YugabyteDB {v['YB_VERSION']}-{v['YB_BUILD']}")
    if S["rehearsal"] and not os.path.exists(f"{PKG}/yb/yugabyte.tar.gz"):
        log("  rehearsal: no YugabyteDB tarball in /data/pkg/yb; skipping YugabyteDB")
    else:
        f.local(f"""
set -e
mkdir -p {PKG}/yb && cd {PKG}/yb
if [ ! -f yugabyte.tar.gz ]; then
  curl -fsSL -o yugabyte.tar.gz {v['YB_URL']}
  sha=$(curl -fsSL {v['YB_SHA_URL']} | awk '{{print $1}}')
  echo "$sha  yugabyte.tar.gz" | sha256sum -c - > /dev/null
fi
""", timeout=1800)
        def push_yb(n):
            # YugabyteDB's initdb needs en_US.UTF-8 (node-init generates it too).
            f.sh(n, "locale -a | grep -qi '^en_US.utf8$' || sudo locale-gen en_US.UTF-8 > /dev/null")
            with open(f"{PKG}/yb/yugabyte.tar.gz", "rb") as src:
                f.sh(n, f"rm -rf /data/opt/yb && mkdir -p /data/opt/yb && tar -xzf - -C /data/opt/yb "
                        f"--strip-components=1 && /data/opt/yb/bin/post_install.sh > /dev/null 2>&1",
                     stdin=src, timeout=1800)
        f.par(f.storage(), push_yb, "installing YugabyteDB")

    log("environment")
    def envinfo(n):
        return f.sh(n, "echo \"== $(hostname)\"; nproc; free -g | head -2; df -h /data | tail -1; "
                       "uname -r; (chronyc tracking 2>/dev/null | head -4) || true", check=False)
    info = f.par(f.p["hosts"], envinfo, "environment")
    f.save("env/hosts.txt", "".join(info[k] for k in sorted(info)))
    f.save("env/MANIFEST", open(f"{TB}/MANIFEST").read())
    f.save("env/hosts.json", json.dumps(f.p, indent=1))
    log("prepared")


# --- Tarn ---------------------------------------------------------------------

def tarn_uri(f, host, cred=True):
    return f"{'tarn:tarn@' if cred else ''}{f.ip(host)}:3301"

def tarn_probe(f, script, host, *args):
    return subprocess.run([f"{TB}/bin/tarantool", f"{TB}/tarn/scripts/{script}", tarn_uri(f, host), *args],
                          capture_output=True, timeout=30).returncode == 0

def pin_tarn_leaders(f):
    """Leaders: replica 1 of each shard (in a zone with two replicas of it)."""
    if f.rf == 1:
        return
    for s in range(1, f.shards + 1):
        lead = f"s{s}r1"
        if not tarn_probe(f, "probe.lua", lead):
            subprocess.run([f"{TB}/bin/tarantool", f"{TB}/tarn/scripts/promote.lua", tarn_uri(f, lead)],
                           capture_output=True, timeout=30)
            wait_until(f"{lead} leading", lambda lead=lead: tarn_probe(f, "probe.lua", lead), 60, 0.5)

def deploy_tarn(f, isolation=None):
    peers = {str(s): [tarn_uri(f, h["name"], cred=False) for h in f.storage(s)] for s in range(1, f.shards + 1)}
    mem_mb = 20000 if not S["rehearsal"] else 512
    def start_storage(n):
        h = f.hosts[n]
        reps = [tarn_uri(f, x["name"]) for x in f.storage(h["shard"])] if f.rf > 1 else []
        e = {"TARN_SHARD_ID": h["shard"], "TARN_LISTEN": tarn_uri(f, n, cred=False),
             "TARN_WORKDIR": f"{RUN}/tarn/data", "TARN_REPLICAS": json.dumps(reps),
             "TARN_PEERS": json.dumps(peers), "TARN_MEMORY_MB": mem_mb, "TARN_WAL_MODE": S["tarn_wal"],
             "TARN_IPROTO_THREADS": S["tarn_iproto_threads"], "TARN_HOST_INSTANCES": 1,
             "TARN_NATIVE": f"{TB}/lib/libtarn_storage.so", "TARN_LOG_LEVEL": 5,
             "TARN_INSTANCE_NAME": f"shard{h['shard']}_{n}"}
        f.sh(n, f"mkdir -p {RUN}/tarn/data")
        f.svc(n, "start", "tarn-storage", "bash", "-c",
              f"cd {RUN}/tarn/data && exec {TB}/bin/tarantool {TB}/tarn/lua/storage.lua", env_vars=e)
    log("tarn: storage")
    f.par(f.storage(), start_storage, "starting Tarn storage")
    for s in range(1, f.shards + 1):
        wait_until(f"shard {s} leader",
                   lambda s=s: any(tarn_probe(f, "probe.lua", h["name"]) for h in f.storage(s)), 180, 0.5)
        if f.rf > 1:
            for h in f.storage(s):
                wait_until(f"{h['name']} joined", lambda h=h: tarn_probe(f, "joined.lua", h["name"], str(f.rf)), 180, 0.5)
    pin_tarn_leaders(f)
    # sysbench's prepare sends multi-row INSERTs of several MB, which the
    # default per-query admission budget (64 MiB, counted with allocation
    # multipliers) refuses; docs/resources.md says to raise query_bytes and
    # server_bytes for larger statements. The work limits are raised so the
    # loader's row counts (full scans) are admitted. Recorded in results.
    resources = {"query_bytes": 512 << 20, "server_bytes": (8 << 30) if not S["rehearsal"] else (1 << 30),
                 "query_work": 10 ** 11, "server_work": 10 ** 12}
    cluster = {"shards": [{"id": s, "replicas": [tarn_uri(f, h["name"], cred=False) for h in f.storage(s)]}
                          for s in range(1, f.shards + 1)], "query_pushdown": False, "resources": resources}
    f.save("tarn/cluster.json", json.dumps(cluster, indent=1))
    log("tarn: SQL servers")
    def start_sql(n):
        h = f.hosts[n]
        f.sh(n, f"mkdir -p {RUN}/tarn && cat > {RUN}/tarn/cluster.json << 'EOF'\n{json.dumps(cluster)}\nEOF")
        args = [f"{TB}/bin/tarn-server", "--config", f"{RUN}/tarn/cluster.json", "--listen", "0.0.0.0:4000",
                "--node-id", str(h["index"]), "--user", f"sb:{SB_PASS}"]
        if isolation:
            args += ["--isolation", isolation]
        f.svc(n, "start", "tarn-sql", *args, env_vars={"TARN_LOG": "info"})
    f.par(f.role("sql"), start_sql, "starting tarn-server")
    for h in f.role("sql"):
        wait_until(f"{h['name']} SQL", lambda h=h: mysql(f, "tarn", "SELECT 1", host=h["name"]) is not None, 60, 0.5)
    log("tarn: up")

def restart_tarn_sql(f, isolation):
    f.par(f.role("sql"), lambda n: f.svc(n, "stop", "tarn-sql"), "stopping tarn-server")
    def start(n):
        h = f.hosts[n]
        args = [f"{TB}/bin/tarn-server", "--config", f"{RUN}/tarn/cluster.json", "--listen", "0.0.0.0:4000",
                "--node-id", str(h["index"]), "--user", f"sb:{SB_PASS}"]
        if isolation:
            args += ["--isolation", isolation]
        f.svc(n, "start", "tarn-sql", *args)
    f.par(f.role("sql"), start, "starting tarn-server")
    for h in f.role("sql"):
        wait_until(f"{h['name']} SQL", lambda h=h: mysql(f, "tarn", "SELECT 1", host=h["name"]) is not None, 60, 0.5)


# --- TiDB ---------------------------------------------------------------------

def deploy_tidb(f):
    sql = f.role("sql")
    pd_peers = ",".join(f"pd{h['index']}=http://{f.ip(h['name'])}:2380" for h in sql)
    pd_clients = ",".join(f"{f.ip(h['name'])}:2379" for h in sql)
    # The rehearsal's sandbox holds only two TiKVs (about 2 GB each at
    # start): one replica there, on the first two storage hosts.
    kv_hosts = f.storage()[:2] if S["rehearsal"] else f.storage()
    replicas = 1 if S["rehearsal"] else f.rf
    pd_toml = (f"[replication]\nmax-replicas = {replicas}\nlocation-labels = [\"zone\", \"host\"]\n")
    log("tidb: PD")
    def start_pd(n):
        h = f.hosts[n]
        f.sh(n, f"mkdir -p {RUN}/tidb && printf %s {shlex.quote(pd_toml)} > {RUN}/tidb/pd.toml")
        f.svc(n, "start", "pd", f"{PKG}/tidb/bin/pd-server", f"--name=pd{h['index']}",
              f"--data-dir={RUN}/tidb/pd", f"--client-urls=http://{f.ip(n)}:2379",
              f"--peer-urls=http://{f.ip(n)}:2380", f"--initial-cluster={pd_peers}",
              f"--config={RUN}/tidb/pd.toml", f"--log-file={RUN}/logs/pd-server.log")
    f.par(sql, start_pd, "starting PD")
    wait_until("PD leader", lambda: '"name"' in f.local(
        f"curl -fsS http://{f.ip(sql[0]['name'])}:2379/pd/api/v1/leader", check=False), 120)
    log("tidb: TiKV")
    # (The rehearsal's sandbox caps open files at 20,000; TiKV requires
    # three times RocksDB's max-open-files and more, so shrink those there.)
    # Its memory cgroup also fits three TiKVs only with small write buffers.
    tikv_toml = "" if not S["rehearsal"] else (
        'memory-usage-limit = "900MB"\n'
        '[server]\ngrpc-memory-pool-quota = "128MB"\n'
        '[storage]\nreserve-space = "0MB"\ntxn-status-cache-capacity = 100000\n[storage.block-cache]\ncapacity = "192MB"\n'
        '[rocksdb]\nmax-open-files = 2048\n'
        '[rocksdb.defaultcf]\nwrite-buffer-size = "16MB"\nmax-write-buffer-number = 2\n'
        '[rocksdb.writecf]\nwrite-buffer-size = "16MB"\nmax-write-buffer-number = 2\n'
        '[rocksdb.lockcf]\nwrite-buffer-size = "8MB"\nmax-write-buffer-number = 2\n'
        '[raftdb]\nmax-open-files = 2048\n[raftdb.defaultcf]\nwrite-buffer-size = "16MB"\n')
    def start_tikv(n):
        h = f.hosts[n]
        f.sh(n, f"mkdir -p {RUN}/tidb && printf %s {shlex.quote(tikv_toml)} > {RUN}/tidb/tikv.toml")
        f.svc(n, "start", "tikv", f"{PKG}/tidb/bin/tikv-server", f"--addr={f.ip(n)}:20160",
              f"--advertise-addr={f.ip(n)}:20160", f"--status-addr={f.ip(n)}:20180",
              f"--advertise-status-addr={f.ip(n)}:20180", f"--pd={pd_clients}",
              f"--data-dir={RUN}/tidb/tikv", f"--labels=zone={h['zone']},host={n}",
              f"--config={RUN}/tidb/tikv.toml", f"--log-file={RUN}/logs/tikv-server.log")
    f.par(kv_hosts, start_tikv, "starting TiKV")
    log("tidb: TiDB servers")
    def start_tidb(n):
        # Temporary storage (spilled sorts) on /data, not the boot disk's /tmp.
        # Its DDL ingest (fast reorg: sysbench's CREATE INDEX after the load)
        # wants a large temp-dir: on /data (the SQL nodes' 200 GB boot disk).
        f.sh(n, f"mkdir -p {RUN}/tidb/tmp && printf 'tmp-storage-path = \"{RUN}/tidb/tmp\"\\n"
                f"temp-dir = \"{RUN}/tidb/tmp\"\\n' > {RUN}/tidb/tidb.toml")
        f.svc(n, "start", "tidb", f"{PKG}/tidb/bin/tidb-server", "-P", "4100", "--status=10080",
              "--store=tikv", f"--path={pd_clients}", f"--advertise-address={f.ip(n)}",
              f"--config={RUN}/tidb/tidb.toml", f"--log-file={RUN}/logs/tidb-server.log")
    f.par(sql, start_tidb, "starting tidb-server")
    n_kv = len(kv_hosts)
    wait_until("TiKV stores up", lambda: int(mysql(f, "tidb",
        "SELECT COUNT(*) FROM information_schema.tikv_store_status WHERE store_state_name = 'Up'", user="root") or 0) >= n_kv, 300, 2)
    mysql(f, "tidb", f"CREATE USER IF NOT EXISTS 'sb'@'%' IDENTIFIED BY '{SB_PASS}'; GRANT ALL ON *.* TO 'sb'@'%'", user="root")
    if S["rehearsal"]:   # (too little disk there for the ingest's temp-dir)
        mysql(f, "tidb", "SET GLOBAL tidb_enable_dist_task = OFF; SET GLOBAL tidb_ddl_enable_fast_reorg = OFF", user="root")
    f.save("tidb/settings.txt", mysql(f, "tidb", "SELECT @@version, @@transaction_isolation, @@tidb_txn_mode, "
                                      "@@tidb_ddl_enable_fast_reorg; SHOW CONFIG WHERE type = 'pd' AND "
                                      "name = 'replication.max-replicas'", user="root") + "\n")
    log("tidb: up")


# --- YugabyteDB ---------------------------------------------------------------

def yb_masters(f):
    """Shard 1's replica hosts: RF masters, spread over the zones like its replicas."""
    return [h["name"] for h in f.storage(1)]

def deploy_yb(f):
    region = f.p["region"]
    masters = yb_masters(f)
    maddrs = ",".join(f"{f.ip(m)}:7100" for m in masters)
    yb = "/data/opt/yb"
    log("yb: masters")
    def start_master(n):
        h = f.hosts[n]
        f.sh(n, f"mkdir -p {RUN}/yb/master")
        f.svc(n, "start", "yb-master", f"{yb}/bin/yb-master", f"--master_addresses={maddrs}",
              f"--rpc_bind_addresses={f.ip(n)}:7100", f"--webserver_interface={f.ip(n)}",
              f"--fs_data_dirs={RUN}/yb/master", "--placement_cloud=gcp", f"--placement_region={region}",
              f"--placement_zone={h['zone']}", f"--replication_factor={f.rf}",
              *(["--memory_limit_hard_bytes=268435456"] if S["rehearsal"] else
                ["--default_memory_limit_to_ram_ratio=0.1"]))
    f.par(masters, start_master, "starting yb-master")
    log("yb: tservers")
    def start_tserver(n):
        h = f.hosts[n]
        f.sh(n, f"mkdir -p {RUN}/yb/tserver")
        f.svc(n, "start", "yb-tserver", f"{yb}/bin/yb-tserver", f"--tserver_master_addrs={maddrs}",
              f"--rpc_bind_addresses={f.ip(n)}:9100", f"--webserver_interface={f.ip(n)}",
              "--enable_ysql", f"--pgsql_proxy_bind_address={f.ip(n)}:5433",
              f"--cql_proxy_bind_address={f.ip(n)}:9042", f"--fs_data_dirs={RUN}/yb/tserver",
              "--placement_cloud=gcp", f"--placement_region={region}", f"--placement_zone={h['zone']}",
              # (The rehearsal's memory cgroup holds three small tservers.)
              *(["--memory_limit_hard_bytes=1073741824", "--yb_num_shards_per_tserver=1",
                 "--ysql_num_shards_per_tserver=1"] if S["rehearsal"] else
                ["--default_memory_limit_to_ram_ratio=0.6"]))
    f.par(f.storage(), start_tserver, "starting yb-tserver")
    zones = ",".join(f"gcp.{region}.{z}" for z in f.p["zones"])
    admin = f"{yb}/bin/yb-admin -master_addresses {maddrs}"
    wait_until("yb placement", lambda: f.sh(masters[0], f"{admin} modify_placement_info {zones} {f.rf}",
                                            check=True, quiet=True) is not None, 180, 3)
    n_ts = len(f.storage())
    wait_until("YSQL on every tserver", lambda: psql(f, "SELECT COUNT(*) FROM yb_servers()") == str(n_ts), 300, 3)
    f.save("yb/universe.txt", f.sh(masters[0], f"{admin} get_universe_config", check=False))
    log("yb: up")


# --- SQL access -----------------------------------------------------------------

def endpoints(f, system):
    if system == "tarn":
        return [f.ip(h["name"]) for h in f.role("sql")], 4000
    if system == "tidb":
        return [f.ip(h["name"]) for h in f.role("sql")], 4100
    return [f.ip(h["name"]) for h in f.storage()], 5433

def mysql(f, system, q, host=None, user="sb", db=None):
    hosts, port = endpoints(f, system)
    h = f.ip(host) if host else hosts[0]
    pw = f"-p{SB_PASS}" if user == "sb" else ""
    p = subprocess.run(["mysql", "-h", h, "-P", str(port), "-u", user, *([pw] if pw else []),
                        *(["-D", db] if db else []), "-N", "-B", "-e", q],
                       capture_output=True, text=True, timeout=120)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip())
    return p.stdout.strip()

def psql(f, q, db="yugabyte"):
    hosts, port = endpoints(f, "yb")
    p = subprocess.run(["psql", "-h", hosts[0], "-p", str(port), "-U", "yugabyte", "-d", db, "-qAt",
                        "-v", "ON_ERROR_STOP=1", "-c", q], capture_output=True, text=True, timeout=120)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip())
    return p.stdout.strip()

def set_variant(f, system, variant):
    """Make new sessions use the variant's isolation level."""
    if system == "tarn":
        restart_tarn_sql(f, None if variant == "serializable" else "READ-COMMITTED")
    elif system == "tidb":
        level = "REPEATABLE-READ" if variant == "default" else "READ-COMMITTED"
        mysql(f, "tidb", f"SET GLOBAL transaction_isolation = '{level}'", user="root")
    else:
        level = "DEFAULT" if variant == "default" else "'serializable'"
        psql(f, f"ALTER DATABASE sbtest SET default_transaction_isolation TO {level}")


# --- sysbench ---------------------------------------------------------------------

def sb_args(f, system, threads, ignore_all=False, exclude=()):
    """sysbench's binary and options. ignore_all: no error stops a thread
    (fault runs, where the timeline must go on through the failure).
    exclude: SQL endpoints not to connect to."""
    hosts, port = endpoints(f, system)
    hosts = [h for h in hosts if h not in exclude]
    # Fault runs reconnect after lost connections, which loses server-side
    # prepared statements: they use plain statements, for every system alike.
    common = [f"--tables={S['tables']}", f"--table-size={S['size']}", f"--threads={threads}",
              "--rand-type=uniform", f"--db-ps-mode={'disable' if ignore_all else 'auto'}"]
    if system == "yb":
        # YugabyteDB's sysbench fork, patched (patches/) with
        # --pgsql-ignore-errors=all; it retries 40001 and 40P01 itself.
        return ([f"{TB}/opt/sysbench-yb/bin/sysbench"],
                common + ["--db-driver=pgsql", f"--pgsql-host={','.join(hosts)}", f"--pgsql-port={port}",
                          "--pgsql-user=yugabyte", "--pgsql-db=sbtest", "--range_key_partitioning=true",
                          "--serial_cache_size=1000", "--create_secondary=true"]
                + (["--pgsql-ignore-errors=all"] if ignore_all else []))
    return ([f"{TB}/opt/sysbench/bin/sysbench"],
            common + ["--db-driver=mysql", f"--mysql-host={','.join(hosts)}", f"--mysql-port={port}",
                      "--mysql-user=sb", f"--mysql-password={SB_PASS}", "--mysql-db=sbtest",
                      f"--mysql-ignore-errors={'all' if ignore_all else MYSQL_IGNORE[system]}"])

def load(f, system):
    log(f"{system}: load {S['tables']} x {S['size']} rows")
    if system == "yb":
        psql(f, "DROP DATABASE IF EXISTS sbtest")
        psql(f, "CREATE DATABASE sbtest")
    else:
        user = "root" if system == "tidb" else "sb"
        mysql(f, system, "DROP DATABASE IF EXISTS sbtest; CREATE DATABASE sbtest", user=user)
    loader = f.role("load")[0]["name"]
    # YugabyteDB loads with one thread: concurrent CREATE TABLE / INDEX from
    # several sysbench threads fail with 40001 on its catalog (as the earlier
    # single-host comparison found); the others load one table per thread.
    binary, args = sb_args(f, system, 1 if system == "yb" else S["tables"])
    t0 = time.time()
    out = f.sh(loader, " ".join(shlex.quote(x) for x in binary + ["oltp_read_write"] + args + ["prepare"]) + " 2>&1",
               timeout=6 * 3600)
    secs = time.time() - t0
    rows = S["tables"] * S["size"]
    f.save(f"{system}/raw/prepare.log", out)
    # sysbench exits 0 when a loader thread dies: trust the tables, not it.
    if "FATAL" in out:
        raise RuntimeError(f"{system}: load failed: " + next(l for l in out.splitlines() if "FATAL" in l)[:400])
    counts = []
    for t in range(1, S["tables"] + 1):
        q = f"SELECT COUNT(*) FROM sbtest{t}"
        counts.append(int(psql(f, q, db="sbtest") if system == "yb" else mysql(f, system, q, db="sbtest")))
    if any(c != S["size"] for c in counts):
        raise RuntimeError(f"{system}: load incomplete: rows per table {counts}, expected {S['size']}")
    res = {"system": f.label(system), "tables": S["tables"], "size": S["size"], "seconds": round(secs, 1),
           "rows_per_s": round(rows / secs)}
    f.save(f"{system}/load.json", json.dumps(res) + "\n")
    log(f"{system}: loaded {rows} rows in {secs:.0f} s ({rows / secs:.0f} rows/s)")
    return res


HIST = re.compile(r"^\s*([0-9.]+)\s*\|[* ]*\s([0-9]+)\s*$")

def parse_sb(out):
    def grab(pat, cast=float):
        m = re.search(pat, out)
        return cast(m.group(1)) if m else None
    hist = {}
    for line in out.splitlines():
        m = HIST.match(line)
        if m:
            hist[float(m.group(1))] = hist.get(float(m.group(1)), 0) + int(m.group(2))
    return {"tps": grab(r"transactions:\s+\d+\s+\(([0-9.]+) per sec"),
            "qps": grab(r"queries:\s+\d+\s+\(([0-9.]+) per sec"),
            "txns": grab(r"transactions:\s+(\d+)", int),
            "errors": grab(r"ignored errors:\s+(\d+)", int),
            "reconnects": grab(r"reconnects:\s+(\d+)", int),
            "avg": grab(r"avg:\s+([0-9.]+)"), "hist": hist}

def pct(hist, q):
    total = sum(hist.values())
    if not total:
        return None
    acc = 0
    for v in sorted(hist):
        acc += hist[v]
        if acc >= q * total:
            return v
    return max(hist)

def run_parallel(f, system, test, threads, secs, extra=(), label=None):
    """Run one workload from every load generator at once; merge the results."""
    gens = [h["name"] for h in f.role("load")]
    gens = gens[:max(1, min(len(gens), threads))]
    share = [threads // len(gens) + (1 if i < threads % len(gens) else 0) for i in range(len(gens))]
    start_at = time.time() + 3          # a common start, so the windows overlap
    def one(i):
        binary, args = sb_args(f, system, share[i])
        cmd = (f"sleep $(python3 -c 'import time; print(max(0, {start_at} - time.time()))'); "
               + " ".join(shlex.quote(x) for x in binary + [test] + args +
                          [f"--time={secs}", "--histogram=on", "--percentile=95", *extra, "run"]) + " 2>&1")
        return f.sh(gens[i], cmd, check=False, timeout=secs + 600)
    with cf.ThreadPoolExecutor(len(gens)) as ex:
        outs = list(ex.map(one, range(len(gens))))
    parts = [parse_sb(o) for o in outs]
    for g, o in zip(gens, outs):
        f.save(f"{system}/raw/{label or test}.{threads}.{g}.log", o)
    hist = {}
    for p in parts:
        for k, v in p["hist"].items():
            hist[k] = hist.get(k, 0) + v
    # A thread that met an error outside the ignore list stops, and the
    # run's totals then count fewer clients: such a row is marked not ok.
    ok = all(p["tps"] is not None for p in parts) and not any("FATAL" in o for o in outs)
    return {"tps": sum(p["tps"] or 0 for p in parts), "qps": sum(p["qps"] or 0 for p in parts),
            "errors": sum(p["errors"] or 0 for p in parts), "reconnects": sum(p["reconnects"] or 0 for p in parts),
            "p50": pct(hist, 0.50), "p95": pct(hist, 0.95), "p99": pct(hist, 0.99),
            "generators": len(gens), "ok": ok, "outs": outs}

BENCH_FIELDS = ["run", "topology", "system", "variant", "test", "threads", "tps", "qps", "p50_ms", "p95_ms",
                "p99_ms", "errors", "reconnects", "generators", "time_s", "tables", "size", "ok"]

def bench(f, system, variant=None, tests=None, threads=None, secs=None):
    variant = variant or VARIANTS[system][0]
    tests = tests or S["tests"]
    threads = threads or S["threads"]
    secs = secs or S["time"]
    set_variant(f, system, variant)
    if S["warmup"]:
        run_parallel(f, system, "oltp_point_select", max(threads), S["warmup"], label="warmup")
    path = os.path.join(f.results, f.label(system), "bench.csv")
    new = not os.path.exists(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="") as fh:
        w = csv.DictWriter(fh, BENCH_FIELDS)
        if new:
            w.writeheader()
        for test in tests:
            for th in threads:
                r = run_parallel(f, system, test, th, secs, label=f"{variant}.{test}")
                row = {"run": f.run_id, "topology": f.p["topology"], "system": f.label(system), "variant": variant,
                       "test": test, "threads": th, "tps": round(r["tps"], 2), "qps": round(r["qps"], 2),
                       "p50_ms": r["p50"], "p95_ms": r["p95"], "p99_ms": r["p99"], "errors": r["errors"],
                       "reconnects": r["reconnects"], "generators": r["generators"], "time_s": secs,
                       "tables": S["tables"], "size": S["size"], "ok": r["ok"]}
                w.writerow(row)
                fh.flush()
                flag = "" if r["ok"] else "  FAILED (see raw/)"
                log(f"{system} {variant:15s} {test:22s} t={th:<4d} tps={r['tps']:<10.1f} qps={r['qps']:<10.1f} "
                    f"p95={r['p95']} p99={r['p99']} err={r['errors']}{flag}")


# --- faults -----------------------------------------------------------------------

SERVICE = {"tarn": "tarn-storage", "tidb": "tikv", "yb": "yb-tserver"}

def tarn_leader(f, shard):
    for h in f.storage(shard):
        if tarn_probe(f, "probe.lua", h["name"]):
            return h["name"]
    return None

def fault(f, system, kind):
    secs = S["fault_time"]
    inject_at, heal_at = 30, 30 + secs // 3
    if kind == "zone-cut" and len(f.p["zones"]) < 3:
        raise SystemExit("zone-cut needs the region topology")
    threads = 64 * len(f.role("load"))
    if kind == "node-kill":
        victim = tarn_leader(f, 1) if system == "tarn" else "s1r1"
        desc = f"kill -9 {SERVICE[system]} on {victim}" + (" (shard 1's leader)" if system == "tarn" else "")
        def inject():
            f.svc(victim, "kill9", SERVICE[system])
        def heal():
            f.svc(victim, "restart", SERVICE[system], check=False)
        survivors = [h["name"] for h in f.role("load")]
        exclude = ()
    else:
        zone = f.p["zones"][0]
        inside = [h for h in f.p["hosts"] if h["zone"] == zone and h["role"] != "control"]
        outside_ips = [h["internal_ip"] for h in f.p["hosts"] if h["zone"] != zone and h["role"] != "control"]
        desc = f"cut zone {zone} off from the other zones ({len(inside)} hosts)"
        rules = "".join(f"sudo iptables -I INPUT -s {ip} -j DROP; sudo iptables -I OUTPUT -d {ip} -j DROP; "
                        for ip in outside_ips)
        undo = "".join(f"sudo iptables -D INPUT -s {ip} -j DROP; sudo iptables -D OUTPUT -d {ip} -j DROP; "
                       for ip in outside_ips)
        def inject():
            f.par(inside, lambda n: f.sh(n, rules), "zone cut")
        def heal():
            f.par(inside, lambda n: f.sh(n, undo, check=False), "zone heal")
        survivors = [h["name"] for h in f.role("load") if h["zone"] != zone]
        # The surviving clients use the endpoints outside the cut zone, as a
        # client with health-checked endpoints would: the timeline then shows
        # the database's failover, not connections hung on unreachable hosts.
        exclude = {h["internal_ip"] for h in inside}
    if wait_healthy(f, system, "before the fault") is None:
        raise RuntimeError(f"{system}: not healthy before the {kind} fault")
    log(f"{system}: fault {kind}: {desc}; inject at {inject_at}s, heal at {heal_at}s, {secs}s in all")
    gens = [h["name"] for h in f.role("load")]
    per = threads // len(gens)
    t0 = time.time() + 3
    def one(g):
        binary, args = sb_args(f, system, per, ignore_all=True, exclude=exclude if g in survivors else ())
        cmd = (f"sleep $(python3 -c 'import time; print(max(0, {t0} - time.time()))'); "
               + " ".join(shlex.quote(x) for x in binary + ["oltp_read_write"] + args +
                          [f"--time={secs}", "--report-interval=1", "run"]) + " 2>&1")
        return g, f.sh(g, cmd, check=False, timeout=secs + 600)
    timer = []
    def schedule():
        time.sleep(max(0, t0 + inject_at - time.time()))
        inject()
        timer.append(("inject", time.time() - t0))
        time.sleep(max(0, t0 + heal_at - time.time()))
        heal()
        timer.append(("heal", time.time() - t0))
    th = threading.Thread(target=schedule)
    th.start()
    with cf.ThreadPoolExecutor(len(gens)) as ex:
        outs = dict(ex.map(one, gens))
    th.join()
    series = {}
    for g in survivors:
        f.save(f"{system}/raw/fault-{kind}.{g}.log", outs[g])
        for m in re.finditer(r"\[\s*(\d+)s \].*?tps: ([0-9.]+).*?err/s: ([0-9.]+)", outs[g]):
            s, tps, err = int(m.group(1)), float(m.group(2)), float(m.group(3))
            a = series.setdefault(s, [0.0, 0.0])
            a[0] += tps
            a[1] += err
    path = f.save(f"{system}/fault-{kind}.csv", "")
    with open(path, "w") as fh:
        fh.write("second,tps,err_per_s\n")
        for s in sorted(series):
            fh.write(f"{s},{series[s][0]:.1f},{series[s][1]:.1f}\n")
    before = sorted(v[0] for s, v in series.items() if 5 <= s < inject_at)
    base = before[len(before) // 2] if before else 0
    after = [s for s in sorted(series) if s > inject_at]
    dip = next((s for s in after if series[s][0] < 0.5 * base), None)
    back = next((s for s in after if dip and s > dip and series[s][0] >= 0.5 * base), None)
    summary = {"system": f.label(system), "kind": kind, "what": desc, "baseline_tps": base, "events": timer,
               "first_low_second": dip, "recovered_second": back,
               "outage_s": (back - dip) if dip and back else (0 if dip is None else None),
               "valid": base > 0}
    summary["healthy_after_heal_s"] = wait_healthy(f, system, "after the fault")
    f.save(f"{system}/faults.jsonl", json.dumps(summary) + "\n")
    note = "" if base > 0 else "  INVALID: no throughput before the fault"
    log(f"{system}: {kind}: baseline {base:.0f} tps; below half from {dip}s to {back}s{note}")
    return summary


def wait_healthy(f, system, why, timeout=300):
    """Until a write to every table succeeds through every SQL endpoint (a
    read can succeed while some data is still unavailable). Returns the
    seconds it took, or None on timeout."""
    t0 = time.time()
    hosts, _ = endpoints(f, system)
    ids = sorted({1, max(1, S["size"] // 2), S["size"]})
    def healthy():
        for ip in hosts:
            name = next(h["name"] for h in f.p["hosts"] if h["internal_ip"] == ip)
            for t in range(1, S["tables"] + 1):
                q = f"UPDATE sbtest{t} SET k = k WHERE id IN ({', '.join(map(str, ids))})"
                if system == "yb":
                    psql(f, q, db="sbtest")
                else:
                    mysql(f, system, q, db="sbtest", host=name)
            if system == "yb":
                break           # psql() goes to the first tserver; the write still spans the tablets
        return True
    try:
        wait_until(f"{system} healthy {why}", healthy, timeout, 2)
        return round(time.time() - t0, 1)
    except RuntimeError as e:
        log(str(e))
        return None


# --- lifecycle -----------------------------------------------------------------------

def stop(f, system):
    names = {"tarn": ["tarn-sql", "tarn-storage"], "tidb": ["tidb", "tikv", "pd"], "yb": ["yb-tserver", "yb-master"]}[system]
    def down(n):
        for s in names:
            f.svc(n, "stop", s, check=False)
        f.sh(n, f"rm -rf {RUN}/{system}", check=False)
    f.par([h for h in f.p["hosts"] if h["role"] in ("storage", "sql")], down, f"stopping {system}")
    log(f"{system}: stopped, data deleted")

def collect_logs(f, system):
    def grab(n):
        out = f.sh(n, f"cd {RUN}/logs 2>/dev/null && tail -c 2000000 -q *.log 2>/dev/null | tail -n 20000", check=False)
        f.save(f"{system}/logs/{n}.log", out)
    f.par([h for h in f.p["hosts"] if h["role"] in ("storage", "sql")], grab, "collecting logs")

def deploy(f, system):
    {"tarn": deploy_tarn, "tidb": deploy_tidb, "yb": deploy_yb}[system](f)

def whole_system(f, run):
    """One entry of RUNS: a system's whole share of the run."""
    system = "tarn" if run.startswith("tarn") else run
    LABEL.clear()
    if run == "tarn-fsync":
        LABEL["tarn"] = run
        S["tarn_wal"] = "fsync"
    else:
        S["tarn_wal"] = env("TARN_WAL", "write")
    if system == "tarn":
        f.save("tarn/settings.txt", f"wal_mode={S['tarn_wal']} iproto_threads={S['tarn_iproto_threads']}\n")
    t0 = time.time()
    try:
        deploy(f, system)
        load(f, system)
        if run == "tarn-fsync":
            # Only what fsync changes: the writing workloads, default isolation.
            bench(f, system, VARIANTS[system][0], tests=S["fsync_tests"])
            return
        bench(f, system, VARIANTS[system][0])
        bench(f, system, VARIANTS[system][1], tests=S["variant_tests"])
        set_variant(f, system, VARIANTS[system][0])
        fault(f, system, "node-kill")
        if len(f.p["zones"]) >= 3:
            time.sleep(20)
            if system == "tarn":
                pin_tarn_leaders(f)     # the killed leader rejoined as a follower
            fault(f, system, "zone-cut")
    finally:
        collect_logs(f, system)
        stop(f, system)
        log(f"{run}: done in {(time.time() - t0) / 60:.0f} min")

def run_seconds(run):
    """Estimated seconds of one entry of RUNS (see `estimate`)."""
    n = len(S["threads"])
    load = S["tables"] * S["size"] / 20000
    if run == "tarn-fsync":
        return 180 + load + S["warmup"] + len(S["fsync_tests"]) * n * (S["time"] + 15)
    return (180 + load + 2 * S["warmup"] + (len(S["tests"]) + len(S["variant_tests"])) * n * (S["time"] + 15)
            + 2 * (S["fault_time"] + 40))

def estimate():
    n = len(S["threads"])
    matrix = len(S["tests"]) * n * (S["time"] + 15)
    variant = len(S["variant_tests"]) * n * (S["time"] + 15)
    rows = S["tables"] * S["size"]
    per = {"deploy": 180, "load (at 20k rows/s)": rows / 20000, "warmup x2": 2 * S["warmup"],
           "matrix": matrix, "variant": variant, "faults (region: 2)": 2 * (S["fault_time"] + 40)}
    tot = sum(per.values())
    for k, v in per.items():
        print(f"  {k:24s} {v / 60:6.1f} min")
    fsync = per["deploy"] + per["load (at 20k rows/s)"] + S["warmup"] + len(S["fsync_tests"]) * n * (S["time"] + 15)
    print(f"  per system               {tot / 60:6.1f} min (tarn-fsync: {fsync / 60:.1f} min)")
    print(f"  all: prepare (~20 min) + tarn, tidb, yb + tarn-fsync: {(3 * tot + fsync + 1200) / 3600:.1f} h")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hosts", default="/data/stage/hosts.json")
    ap.add_argument("--key", default="/data/stage/id_ed25519")
    ap.add_argument("--results", default="/data/results")
    ap.add_argument("cmd")
    ap.add_argument("args", nargs="*")
    ap.add_argument("--variant")
    ap.add_argument("--tests")
    ap.add_argument("--threads")
    ap.add_argument("--time", type=int)
    a = ap.parse_args()
    if a.cmd == "estimate":
        estimate()
        return
    f = Fleet(a.hosts, a.key, a.results)
    sysarg = lambda: a.args[0] if a.args and a.args[0] in SYSTEMS else sys.exit(f"system: one of {SYSTEMS}")
    runarg = lambda: a.args[0] if a.args and a.args[0] in RUNS else sys.exit(f"system: one of {RUNS}")
    if a.cmd == "prepare":
        prepare(f)
    elif a.cmd == "deploy":
        deploy(f, sysarg())
    elif a.cmd == "stop":
        stop(f, sysarg())
    elif a.cmd == "load":
        load(f, sysarg())
    elif a.cmd == "bench":
        bench(f, sysarg(), a.variant, a.tests.split(",") if a.tests else None,
              [int(x) for x in a.threads.split(",")] if a.threads else None, a.time)
    elif a.cmd == "fault":
        fault(f, sysarg(), a.args[1])
    elif a.cmd == "system":
        whole_system(f, runarg())
    elif a.cmd == "all":
        prepare(f)
        for r in RUNS:
            left = f.p.get("deadline", time.time() + 10 ** 6) - time.time()
            if left < run_seconds(r) + 1200:
                log(f"{r}: SKIPPED: {left / 60:.0f} min to the deadline, it needs about "
                    f"{run_seconds(r) / 60:.0f} min and 20 min of margin")
                continue
            try:
                whole_system(f, r)
            except Exception as e:  # noqa: BLE001 - one system's failure must not end the run
                log(f"{r}: FAILED: {e}")
    elif a.cmd == "status":
        for h in f.p["hosts"]:
            if h["role"] in ("storage", "sql"):
                out = f.sh(h["name"], f"for p in {RUN}/svc/*.pid; do [ -f $p ] && kill -0 $(cat $p) 2>/dev/null && "
                                      f"basename $p .pid; done", check=False)
                print(f"{h['name']:8s} {h['zone']:16s} {' '.join(out.split())}")
    else:
        sys.exit(f"unknown command {a.cmd}")


if __name__ == "__main__":
    main()
