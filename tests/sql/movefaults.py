#!/usr/bin/env python3
"""A bucket move whose SQL server dies (issue #24) is finished by another
server: given up when it died before the flip, finished when after. Run by
tests/movefaults.sh, which starts a 2-shard cluster with three SQL servers,
test hooks and ddl_job_stale = 2 s.

The server driving a move is killed while a test hook holds one of its
steps, and started again after each case:

1. while the move copies the bucket (on the sending shard): another server
   gives the move up, and the bucket stays where it was;
2. while the sending shard's fence waits for the transactions on the
   bucket's rows: given up, the fence and the receiving shard's hold lifted;
3. after the flip, before the send: another server finishes the move, and
   the bucket is on the new shard;
4. after the send, before the receive: finished;
5. after the flip, before the send, and the sending shard has given the
   move up (as a server does that gives a move up when the reply to its
   write of the flip is lost): another server gives it up on the receiving
   shard too, and ends the job.

Each case checks the rows (through SQL and on the shards, none left aside on
the receiving shard), that the bucket takes writes where it ends up, and
that no move or job is left.
"""
import argparse
import os
import signal
import socket
import subprocess
import sys
import threading
import time

import pymysql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from movetest import bucket_of  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
T = os.environ.get("TARANTOOL", "tarantool")
DB = "movefaults"


class Test:
    def __init__(self, args):
        self.args = args
        self.servers = args.tarn.split(",")
        self.shards = args.shards.split(",")
        self.failures = 0

    def check(self, cond, what, extra=None):
        print(f"  {'ok  ' if cond else 'FAIL'} {what}" + ("" if cond or extra is None else f"  -- {extra}"))
        if not cond:
            self.failures += 1

    def sql(self, k, *stmts, db=DB):
        host, port = self.servers[k].split(":")
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

    def hook(self, shard, name, action):
        return self.lua(shard, f"return tarn_test_hook('{name}', '{action}')")

    def kill_server(self, k):
        with open(os.path.join(self.args.dir, f"tarn{k + 1}.pid")) as f:
            pid = int(f.read())
        os.kill(pid, signal.SIGKILL)

    def start_server(self, k):
        """Start SQL server k again, as scripts/cluster.sh starts it."""
        tarn = os.environ.get("TARN_BIN", os.path.join(ROOT, "target", "release", "tarn-server"))
        log = open(os.path.join(self.args.dir, f"tarn{k + 1}.log"), "a")
        p = subprocess.Popen([tarn, "--config", os.path.join(self.args.dir, "cluster.json"),
                              "--listen", self.servers[k], "--node-id", str(k + 1)],
                             stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                             env=dict(os.environ, TARN_LOG=os.environ.get("TARN_LOG", "info")),
                             start_new_session=True)
        with open(os.path.join(self.args.dir, f"tarn{k + 1}.pid"), "w") as f:
            f.write(str(p.pid))
        host, port = self.servers[k].split(":")
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                socket.create_connection((host, int(port)), timeout=1).close()
                return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError(f"SQL server {k + 1} did not start again")

    def where(self, b):
        """(shard ids owning bucket b, buckets in a move) from SHOW TARN BUCKETS."""
        owners, moving = [], []
        for shard, _, ranges, _, mv in self.sql(0, "SHOW TARN BUCKETS"):
            for r in filter(None, ranges.split(", ")):
                lo, _, hi = r.partition("-")
                if int(lo) <= b <= int(hi or lo):
                    owners.append(int(shard))
            moving += [m for m in filter(None, mv.split(", ")) if " sent " not in m]
        return owners, moving

    def jobs(self):
        return int(self.lua(0, "return #tarn_jobs()"))

    def wait_settled(self, b, secs=30):
        deadline = time.time() + secs
        while time.time() < deadline:
            owners, moving = self.where(b)
            if len(owners) == 1 and not moving and self.jobs() == 0:
                return owners
            time.sleep(0.3)
        return self.where(b)[0]

    def rows_on(self, shard):
        return int(self.lua(shard, f"local s = box.space['{DB}.t'] return s and s:count() or 0"))

    def aside_on(self, shard):
        """Rows a receiving shard keeps aside for a move (_tarn_recv)."""
        return int(self.lua(shard, "return box.space._tarn_recv:count()"))

    def setup(self):
        self.sql(0, f"DROP DATABASE IF EXISTS {DB}", f"CREATE DATABASE {DB}", db=None)
        self.sql(0, "CREATE TABLE t (k INT, id INT, v INT, PRIMARY KEY (k, id)) PARTITION BY KEY (k)")
        # 200 rows under key 1, 50 under key 2
        self.sql(0, "INSERT INTO t VALUES " + ",".join(f"(1, {i}, {i})" for i in range(200)))
        self.sql(0, "INSERT INTO t VALUES " + ",".join(f"(2, {i}, {i})" for i in range(50)))
        self.count = sum(int(r[1]) for r in self.sql(0, "SHOW TARN BUCKETS"))
        self.b = bucket_of(1, self.count)

    def move_in_thread(self, k, to):
        res = {}

        def run():
            try:
                res["ok"] = self.sql(k, f"TARN MOVE BUCKET {self.b} TO SHARD {to}")
            except Exception as e:  # the server is killed: its connection breaks
                res["err"] = str(e)
        th = threading.Thread(target=run)
        th.start()
        return th, res

    def common_checks(self, label, want_shard):
        owners = self.wait_settled(self.b)
        self.check(owners == [want_shard], f"{label}: bucket {self.b} is on shard {want_shard} alone, no move or job left",
                   (owners, self.where(self.b), self.jobs()))
        n = self.sql(0, "SELECT COUNT(*), SUM(v) FROM t WHERE k = 1")[0]
        self.check(n[0] == 200 and int(n[1]) == sum(range(200)), f"{label}: its 200 rows, through SQL", n)
        on = [self.rows_on(0), self.rows_on(1)]
        self.check(sum(on) == 250, f"{label}: 250 rows on the shards, each once", on)
        aside = [self.aside_on(0), self.aside_on(1)]
        self.check(aside == [0, 0], f"{label}: no rows left aside", aside)
        self.sql(0, "UPDATE t SET v = v + 1 WHERE k = 1 AND id = 7")
        v = self.sql(0, "SELECT v FROM t WHERE k = 1 AND id = 7")[0][0]
        self.check(v == 8, f"{label}: it takes writes", v)
        self.sql(0, "UPDATE t SET v = v - 1 WHERE k = 1 AND id = 7")

    def move_id(self):
        """The id of the move of the bucket in progress (its job's arguments)."""
        return self.lua(0, f"for _, j in ipairs(tarn_jobs()) do if j[4] == 'bucket:{self.b}' then "
                           f"return j[7].move end end return nil").strip('"')

    def killed_at(self, label, hook_shard, hook, to, want_shard, before_takeover=None):
        """Move the bucket to shard `to` through SQL server 3, kill that
        server while `hook` holds the move on shard `hook_shard` (an index),
        let another server finish the job, release the hook, and check."""
        self.hook(hook_shard, hook, "hold")
        th, res = self.move_in_thread(2, to)
        reached = self.hook(hook_shard, hook, "reached")
        t0 = time.time()
        self.check('"ok",1' in reached.replace(" ", ""), f"{label}: the move reached the step", reached)
        self.kill_server(2)
        if before_takeover:
            before_takeover()
        # Another server takes the job over once it is stale and finishes it;
        # its own call of the held step passes (the hook holds only the
        # first, for at most 10 s). Then the dead server's call, still held,
        # goes on and finds the job finished.
        owners = self.wait_settled(self.b)
        took = time.time() - t0
        self.check(owners == [want_shard] and took < 9,
                   f"{label}: finished by another server while the step is held ({took:.1f} s)",
                   (owners, self.where(self.b)))
        self.hook(hook_shard, hook, "release")
        th.join(10)
        self.check("ok" not in res, f"{label}: the move statement did not return success", res)
        self.common_checks(label, want_shard)
        self.start_server(2)

    def run(self):
        self.setup()
        start, _ = self.where(self.b)
        s = start[0]
        other = 2 if s == 1 else 1
        si = s - 1
        print(f"bucket {self.b} (rows of k = 1) starts on shard {s}")
        print("1. the server driving the move dies while it copies")
        self.killed_at("given up while copying", si, "bucket_rows", other, s)
        print("2. ... while the fence drains")
        self.killed_at("given up while fenced", si, "bucket_fence", other, s)
        print("3. ... after the flip, before the send")
        self.killed_at("finished from the flip", si, "bucket_send", other, other)
        print("4. ... after the send, before the receive")
        # (back to shard s: the receiving shard is s now)
        self.killed_at("finished from the send", si, "bucket_recv", s, s)
        print("5. ... after the flip, and the sending shard has given the move up")

        def give_up_on_s():
            mid = self.move_id()
            r = self.lua(si, f"return tarn_bucket_send_abort({self.b}, '{mid}')")
            self.check(mid != "null" and '"ok"' in r, "the sending shard gives the move up", (mid, r))
        self.killed_at("given up after the flip", si, "bucket_send", other, s, give_up_on_s)
        return self.failures


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True, help="three SQL servers, comma-separated")
    ap.add_argument("--shards", required=True, help="the storage node of each shard")
    ap.add_argument("--dir", required=True, help="the cluster's directory (pid files)")
    args = ap.parse_args()
    failures = Test(args).run()
    print("PASSED" if failures == 0 else f"{failures} FAILED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
