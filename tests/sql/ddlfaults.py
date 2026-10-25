#!/usr/bin/env python3
"""CREATE TABLE, DROP TABLE and TRUNCATE interrupted by a shard failure take
effect on every shard or on none (issue #30). Run by tests/ddlfaults.sh,
which starts a 2-shard cluster with test hooks and a short ddl_job_stale.

1. Shard 2 is down while the statement runs: it fails. Once shard 2 is
   back, the abandoned job is rolled back, so a CREATE leaves the table on
   no shard and a DROP or TRUNCATE leaves it as it was.
2. Shard 2 is killed after every shard prepared, while it applies (a test
   hook holds its APPLY): the statement does not succeed, and once shard 2
   is back the job is rolled forward on every shard.

Every check reads the shards directly (tests/sql/spaceinfo.lua).
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time

import pymysql

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
T = os.environ.get("TARANTOOL", "tarantool")
DB = "ddlfaults"


class Test:
    def __init__(self, args):
        self.args = args
        self.shards = args.shards.split(",")
        self.failures = 0

    def check(self, cond, what, extra=None):
        print(f"  {'ok  ' if cond else 'FAIL'} {what}" + ("" if cond or extra is None else f"  -- {extra}"))
        if not cond:
            self.failures += 1

    def sql(self, *stmts):
        c = pymysql.connect(host=self.args.host, port=self.args.port, user="t", password="",
                            database=DB, autocommit=True)
        out = None
        with c.cursor() as cur:
            for s in stmts:
                cur.execute(s)
                out = cur.fetchall()
        c.close()
        return out

    def sql_error(self, stmt):
        try:
            self.sql(stmt)
            return None
        except pymysql.MySQLError as e:
            return e.args[0]

    def info(self, shard, table):
        out = subprocess.run([T, os.path.join(ROOT, "tests/sql/spaceinfo.lua"), self.shards[shard],
                              f"{DB}.{table}"], capture_output=True, text=True, stdin=subprocess.DEVNULL)
        return json.loads(out.stdout.strip().splitlines()[-1])

    def both(self, table):
        return [self.info(0, table), self.info(1, table)]

    def hook(self, shard, name, action):
        subprocess.run([T, "-e", f"local c = require('net.box').connect('tarn:tarn@{self.shards[shard]}') "
                        f"c:call('tarn_test_hook', {{'{name}', '{action}'}}) os.exit(0)"],
                       capture_output=True, stdin=subprocess.DEVNULL, check=True)

    def kill_shard2(self):
        with open(os.path.join(self.args.dir, "s2.1.pid")) as f:
            pid = int(f.read())
        os.kill(pid, signal.SIGKILL)
        for _ in range(100):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.05)

    def restart_shard2(self):
        subprocess.run([self.args.cluster, "restart", "-d", self.args.dir, "s2.1"], check=True,
                       stdin=subprocess.DEVNULL)
        # until it serves requests as a writable leader with the Tarn schema
        for _ in range(200):
            r = subprocess.run([T, os.path.join(ROOT, "scripts/probe.lua"), "tarn:tarn@" + self.shards[1]],
                               capture_output=True, stdin=subprocess.DEVNULL)
            if r.returncode == 0:
                return
            time.sleep(0.1)
        raise RuntimeError("shard 2 did not come back")

    def wait_jobs_done(self, secs=60):
        """Until no schema change job is left (a server finishes it)."""
        deadline = time.time() + secs
        while time.time() < deadline:
            if self.info(0, "-")["jobs"] == 0:
                return True
            time.sleep(0.3)
        return False

    def table(self, name, rows=8):
        self.sql(f"DROP TABLE IF EXISTS {name}", f"CREATE TABLE {name} (id INT PRIMARY KEY, v INT)")
        if rows:
            self.sql(f"INSERT INTO {name} VALUES " + ", ".join(f"({i}, {i})" for i in range(rows)))

    def rows(self, info):
        return sum(i["rows"] for i in info)

    # ---- 1. a shard down while the statement runs: rolled back -------------

    def down_during(self, stmt, name, setup_rows):
        if setup_rows is not None:
            self.table(name, setup_rows)
        before = self.both(name)
        self.kill_shard2()
        t0 = time.time()
        err = self.sql_error(stmt)
        took = time.time() - t0
        self.check(err is not None, f"{stmt.split(' (')[0]} with shard 2 down fails ({err}, {took:.1f} s)")
        self.restart_shard2()
        self.check(self.wait_jobs_done(), "... once shard 2 is back, its job is finished")
        return before, self.both(name)

    def test_create_down(self):
        _, after = self.down_during("CREATE TABLE c1 (id INT PRIMARY KEY)", "c1", None)
        self.check(not any(i["space"] or i["named"] for i in after), "... and the table exists on no shard", after)
        self.check(self.sql_error("CREATE TABLE c1 (id INT PRIMARY KEY)") is None, "... and can be created again")
        after = self.both("c1")
        self.check(all(i["space"] and i["named"] for i in after), "... then on every shard", after)

    def test_drop_down(self):
        before, after = self.down_during("DROP TABLE d1", "d1", 8)
        self.check(all(i["space"] and i["named"] for i in after) and self.rows(after) == 8,
                   "... and the table is still on every shard, with its rows", after)
        self.check(self.sql_error("INSERT INTO d1 VALUES (100, 1)") is None, "... and takes writes again")

    def test_truncate_down(self):
        before, after = self.down_during("TRUNCATE TABLE t1", "t1", 8)
        self.check(self.rows(after) == 8 and [i["ver"] for i in after] == [i["ver"] for i in before],
                   "... and the table keeps its rows and its definition", after)
        self.check(self.sql_error("INSERT INTO t1 VALUES (100, 1)") is None, "... and takes writes again")

    # ---- 2. a shard killed while it applies: rolled forward ----------------

    def killed_applying(self, stmt):
        """Run stmt while a hook holds shard 2's APPLY; kill shard 2 there."""
        self.hook(1, "ddl_apply", "hold")
        res = {}

        def run():
            t0 = time.time()
            res["err"] = self.sql_error(stmt)
            res["took"] = time.time() - t0

        th = threading.Thread(target=run)
        th.start()
        self.hook(1, "ddl_apply", "reached")
        self.kill_shard2()
        th.join(60)
        self.check(res.get("err") is not None,
                   f"{stmt.split(' (')[0]}, shard 2 killed while it applies: not reported as done ({res})")

    def test_drop_killed(self):
        self.table("d2", 8)
        self.killed_applying("DROP TABLE d2")
        self.check(not self.info(0, "d2")["space"], "... shard 1 has applied it")
        self.restart_shard2()
        self.check(self.wait_jobs_done(), "... once shard 2 is back, the job is finished")
        after = self.both("d2")
        self.check(not any(i["space"] or i["named"] for i in after), "... and the table is gone from every shard", after)
        self.check(not any(i["held"] for i in after), "... and nothing is held", after)
        self.check(self.sql_error("CREATE TABLE d2 (id INT PRIMARY KEY)") is None, "... and the name is free again")

    def test_truncate_killed(self):
        self.table("t2", 8)
        before = self.both("t2")
        self.killed_applying("TRUNCATE TABLE t2")
        self.restart_shard2()
        self.check(self.wait_jobs_done(), "... once shard 2 is back, the job is finished")
        after = self.both("t2")
        self.check(self.rows(after) == 0, "... and every shard is empty", after)
        vers = {i["ver"] for i in after}
        self.check(len(vers) == 1 and vers != {before[0]["ver"]}, "... with the same new definition version", after)
        self.check(self.sql_error("INSERT INTO t2 VALUES (1, 1), (2, 2), (3, 3)") is None
                   and self.sql("SELECT COUNT(*) FROM t2")[0][0] == 3, "... and the table takes writes")

    def test_create_killed(self):
        self.sql("DROP TABLE IF EXISTS c2")
        self.killed_applying("CREATE TABLE c2 (id INT PRIMARY KEY, v INT)")
        self.restart_shard2()
        self.check(self.wait_jobs_done(), "... once shard 2 is back, the job is finished")
        after = self.both("c2")
        self.check(all(i["space"] and i["named"] for i in after), "... and the table is on every shard", after)
        self.check(self.sql_error("INSERT INTO c2 VALUES (1, 1), (2, 2)") is None
                   and self.sql("SELECT COUNT(*) FROM c2")[0][0] == 2, "... and usable")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True, help="host:port of a SQL server")
    ap.add_argument("--shards", required=True, help="host:port of shard 1's and shard 2's storage node")
    ap.add_argument("--dir", required=True, help="the cluster's directory (scripts/cluster.sh -d)")
    ap.add_argument("--cluster", default=os.path.join(ROOT, "scripts/cluster.sh"))
    ap.add_argument("--only", default="")
    args = ap.parse_args()
    args.host, port = args.tarn.split(":")
    args.port = int(port)
    c = pymysql.connect(host=args.host, port=args.port, user="t", password="", autocommit=True)
    with c.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {DB}")
        cur.execute(f"CREATE DATABASE {DB}")
    c.close()
    t = Test(args)
    names = [n for n in dir(t) if n.startswith("test_") and args.only in n]
    for n in sorted(names, key=lambda x: (not x.endswith("_down"), x)):
        before = t.failures
        t0 = time.time()
        print(n)
        try:
            getattr(t, n)()
        except Exception as e:  # noqa: BLE001
            t.failures += 1
            print(f"    EXCEPTION: {type(e).__name__}: {e}")
        print(f"{'ok  ' if t.failures == before else 'FAIL'} {n} ({time.time() - t0:.1f}s)")
    print(f"{len(names)} tests, {t.failures} failures")
    sys.exit(1 if t.failures else 0)


if __name__ == "__main__":
    main()
