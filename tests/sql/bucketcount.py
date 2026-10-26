#!/usr/bin/env python3
"""The number of buckets is chosen when a cluster is made (issue #24). Run by
tests/bucketcount.sh, which starts a 2-shard cluster whose configuration sets
bucket_count = 3000, as a cluster made by an earlier version may have.

1. The map is made with the configured count.
2. A cluster made by a version from before the map: its shards have tables
   but no map (the map is removed and the storage nodes restarted), and the
   configuration names no count. The SQL server makes the map with 3,000
   buckets, the count the rows were stored with: every row is found where
   it is, and takes writes.
3. A new cluster (no tables, no map): 30,000 buckets, the default.
4. A new cluster whose configuration names a count: that count.

Each case checks the count (SHOW STATUS Tarn_buckets) and that the shards
own equal, contiguous ranges that cover every bucket once.
"""
import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time

import pymysql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from movetest import bucket_of  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
T = os.environ.get("TARANTOOL", "tarantool")
DB = "bucketcount"


class Test:
    def __init__(self, args):
        self.args = args
        self.shards = args.shards.split(",")
        self.failures = 0

    def check(self, cond, what, extra=None):
        print(f"  {'ok  ' if cond else 'FAIL'} {what}" + ("" if cond or extra is None else f"  -- {extra}"))
        if not cond:
            self.failures += 1

    def sql(self, *stmts, db=DB):
        host, port = self.args.tarn.split(":")
        c = pymysql.connect(host=host, port=int(port), user="t", password="", database=db, autocommit=True)
        out = None
        with c.cursor() as cur:
            for s in stmts:
                cur.execute(s)
                out = cur.fetchall()
        c.close()
        return out

    def lua(self, shard, code):
        r = subprocess.run([T, "-e", f"local c = require('net.box').connect('tarn:tarn@{self.shards[shard]}') "
                            f"local r = c:eval([[{code}]]) print(require('json').encode(r)) os.exit(0)"],
                           capture_output=True, text=True, stdin=subprocess.DEVNULL, check=True)
        return r.stdout.strip().splitlines()[-1]

    # ---- the SQL server and the storage nodes ------------------------------
    def pidfile(self, name):
        return os.path.join(self.args.dir, f"{name}.pid")

    def stop(self, name):
        with open(self.pidfile(name)) as f:
            pid = int(f.read())
        os.kill(pid, signal.SIGTERM)
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.1)
        os.kill(pid, signal.SIGKILL)

    def start_server(self):
        """Start the SQL server again, as scripts/cluster.sh starts it."""
        tarn = os.environ.get("TARN_BIN", os.path.join(ROOT, "target", "release", "tarn-server"))
        log = open(os.path.join(self.args.dir, "tarn1.log"), "a")
        p = subprocess.Popen([tarn, "--config", os.path.join(self.args.dir, "cluster.json"),
                              "--listen", self.args.tarn, "--node-id", "1"],
                             stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                             env=dict(os.environ, TARN_LOG=os.environ.get("TARN_LOG", "info")),
                             start_new_session=True)
        with open(self.pidfile("tarn1"), "w") as f:
            f.write(str(p.pid))
        host, port = self.args.tarn.split(":")
        deadline = time.time() + 30
        while time.time() < deadline:
            if p.poll() is not None:
                raise RuntimeError(f"the SQL server stopped (exit code {p.returncode}); see tarn1.log")
            try:
                socket.create_connection((host, int(port)), timeout=1).close()
                return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError("the SQL server did not start")

    def configure(self, count):
        """Set (or, with None, remove) bucket_count in cluster.json."""
        path = os.path.join(self.args.dir, "cluster.json")
        with open(path) as f:
            cfg = json.load(f)
        cfg.pop("bucket_count", None)
        if count is not None:
            cfg["bucket_count"] = count
        with open(path, "w") as f:
            json.dump(cfg, f)

    def forget_map(self, count):
        """Stop the SQL server, remove the bucket map from every shard (as a
        version from before the map left them), restart the storage nodes,
        set bucket_count and start the SQL server, which makes a map."""
        self.stop("tarn1")
        for i in range(len(self.shards)):
            self.lua(i, "box.space._tarn_meta:delete{'buckets'} box.space._tarn_buckets:truncate() return true")
        for i in range(len(self.shards)):
            self.stop(f"s{i + 1}.1")
        subprocess.run([os.path.join(ROOT, "scripts", "cluster.sh"), "restart", "-d", self.args.dir]
                       + [f"s{i + 1}.1" for i in range(len(self.shards))], check=True,
                       stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        self.configure(count)
        self.start_server()

    # ---- checks -----------------------------------------------------------
    def buckets(self):
        return int(dict(self.sql("SHOW STATUS LIKE 'Tarn_buckets'", db=None))["Tarn_buckets"])

    def ranges(self):
        out = []
        for _, _, ranges, _, _ in self.sql("SHOW TARN BUCKETS", db=None):
            out.append([tuple(int(x) for x in r.split("-")) if "-" in r else (int(r), int(r))
                        for r in filter(None, ranges.split(", "))])
        return out

    def map_checks(self, label, want):
        n = self.buckets()
        self.check(n == want, f"{label}: {want} buckets", n)
        ranges = self.ranges()
        per = [sum(hi - lo + 1 for lo, hi in rs) for rs in ranges]
        whole = [len(rs) == 1 for rs in ranges]
        flat = sorted(r for rs in ranges for r in rs)
        covers = flat and flat[0][0] == 1 and flat[-1][1] == n and all(
            a[1] + 1 == b[0] for a, b in zip(flat, flat[1:]))
        self.check(all(whole) and max(per) - min(per) <= 1 and covers,
                   f"{label}: each shard owns one equal, contiguous range, every bucket once", ranges)

    def rows_checks(self, label, count):
        n = self.sql("SELECT COUNT(*), SUM(v) FROM t")[0]
        self.check(n[0] == 100 and int(n[1]) == sum(range(100)), f"{label}: the 100 rows, through SQL", n)
        got = [self.sql(f"SELECT v FROM t WHERE id = {i}")[0][0] for i in range(0, 100, 7)]
        self.check(got == list(range(0, 100, 7)), f"{label}: each found by its key", got)
        stored = json.loads(self.lua(0, f"local t = {{}} for _, r in box.space['{DB}.t']:pairs() do "
                                        f"t[#t + 1] = {{r.id, r.bucket_id}} end return t"))
        stored += json.loads(self.lua(1, f"local t = {{}} for _, r in box.space['{DB}.t']:pairs() do "
                                         f"t[#t + 1] = {{r.id, r.bucket_id}} end return t"))
        self.check(all(b == bucket_of(i, count) for i, b in stored),
                   f"{label}: their bucket ids are those of {count} buckets", stored[:3])
        self.sql("UPDATE t SET v = v + 1000 WHERE id = 5", "UPDATE t SET v = v - 1000 WHERE id = 5")
        self.sql("INSERT INTO t VALUES (100, 100)", "DELETE FROM t WHERE id = 100")
        self.check(self.sql("SELECT v FROM t WHERE id = 5")[0][0] == 5, f"{label}: they take writes")

    def run(self):
        print("1. a cluster whose configuration sets bucket_count = 3000")
        self.map_checks("configured", 3000)
        self.sql(f"DROP DATABASE IF EXISTS {DB}", f"CREATE DATABASE {DB}", db=None)
        self.sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        self.sql("INSERT INTO t VALUES " + ",".join(f"({i}, {i})" for i in range(100)))
        self.rows_checks("configured", 3000)

        print("2. made by a version from before the map: tables, no map, no bucket_count")
        self.forget_map(None)
        self.map_checks("from before the map", 3000)
        self.rows_checks("from before the map", 3000)

        print("3. a new cluster: no tables, no map, no bucket_count")
        self.sql(f"DROP DATABASE {DB}", db=None)
        self.forget_map(None)
        self.map_checks("new", 30000)

        print("4. a new cluster with bucket_count = 4096")
        self.forget_map(4096)
        self.map_checks("new, configured", 4096)
        self.sql(f"CREATE DATABASE {DB}", db=None)
        self.sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        self.sql("INSERT INTO t VALUES " + ",".join(f"({i}, {i})" for i in range(100)))
        self.rows_checks("new, configured", 4096)
        return self.failures


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True, help="the SQL server")
    ap.add_argument("--shards", required=True, help="the storage node of each shard")
    ap.add_argument("--dir", required=True, help="the cluster's directory")
    args = ap.parse_args()
    failures = Test(args).run()
    print("PASSED" if failures == 0 else f"{failures} FAILED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
