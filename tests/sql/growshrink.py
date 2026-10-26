#!/usr/bin/env python3
"""Grow a cluster from two shards to four and shrink it to three (issue #25).
Run by tests/growshrink.sh on a cluster of 480 buckets and three SQL
servers whose shards 3 and 4 have storage but are not members yet:

    growshrink.py --phase grow --tarn A,B,C --add 3=REPLICAS --add 4=REPLICAS \\
        --remove 2 --dir DIR
    growshrink.py --phase control --tarn A,B,C --dir DIR --load-window SECS

The grow phase runs next to the bank workloads of txntest.py and rctest.py:
  1. shards 3 and 4 join (TARN ADD SHARD);
  2. TARN REBALANCE moves buckets to them, four at a time: SHOW TARN
     REBALANCE, SHOW TARN MOVES and information_schema.TARN_MOVES show it; a
     second one is refused while it runs; PAUSE stops new moves and RESUME
     (through another server) goes on; it ends 'done', every shard with an
     even share of the buckets and of a table's rows;
  3. shard 2 leaves (TARN REMOVE SHARD) and its storage stops; a rebalance
     evens the three shards left out.
The control phase runs once the workloads ended:
  4. after reads of the rows of a few buckets of one shard, TARN REBALANCE
     BY LOAD moves those buckets off that shard first;
  5. a rebalance cancelled at once ends 'cancelled', moves nothing more, and
     cannot be resumed;
  6. the SQL server running a rebalance is killed: another one takes it over
     and it ends 'done';
  7. a rebalance limited in bytes per second moves no faster.
After each step, every SQL server reads every row of the table.
"""
import argparse
import collections
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
DB = "growshrink"
ROWS = 4000
ENDED = ("done", "failed", "cancelled")
# SHOW TARN REBALANCE's columns
STATUS, BY, OWNER, STARTED, DONE, FAILED, PLANNED, MOVING, BYTES, MAX_MOVES, MAX_RATE, MESSAGE = range(12)


def connect(hp, db=DB):
    host, port = hp.split(":")
    return pymysql.connect(host=host, port=int(port), user="t", password="", database=db, autocommit=True)


def q(c, sql, args=None):
    cur = c.cursor()
    cur.execute(sql, args)
    rows = cur.fetchall() if cur.description else cur.rowcount
    cur.close()
    return rows


class Test:
    def __init__(self, args):
        self.args = args
        self.servers = args.tarn.split(",")
        self.add = [(int(a.partition("=")[0]), a.partition("=")[2]) for a in args.add]
        self.failures = 0
        self.t0 = time.time()

    def step(self, what):
        print(f"{what} (at {time.time() - self.t0:.0f} s)", flush=True)

    def check(self, cond, what, extra=None):
        print(f"  {'ok  ' if cond else 'FAIL'} {what}" + ("" if cond or extra is None else f"  -- {extra}"),
              flush=True)
        if not cond:
            self.failures += 1
        return cond

    def sql(self, k, s, args=None):
        c = connect(self.servers[k])
        try:
            return q(c, s, args)
        finally:
            c.close()

    def status(self, k=0):
        rows = self.sql(k, "SHOW TARN REBALANCE")
        return rows[0] if rows else None

    def wait_status(self, want, secs=180, k=0):
        deadline = time.time() + secs
        while time.time() < deadline:
            s = self.status(k)
            if s and s[STATUS] in want:
                return s
            time.sleep(0.3)
        return self.status(k)

    def wait_still(self, secs=30):
        """The status once no move is in flight."""
        deadline = time.time() + secs
        while True:
            s = self.status()
            if s[MOVING] == 0 or time.time() > deadline:
                return s
            time.sleep(0.2)

    def members(self):
        """{shard id: (state, replicas)} from SHOW TARN SHARDS."""
        return {int(r[0]): (r[2], r[1]) for r in self.sql(0, "SHOW TARN SHARDS")}

    def active(self):
        return sorted(s for s, (st, _) in self.members().items() if st == "active")

    def owners(self):
        """{bucket: shard id} from SHOW TARN BUCKETS."""
        out = {}
        for row in self.sql(0, "SHOW TARN BUCKETS"):
            for r in filter(None, str(row[2]).split(", ")):
                lo, _, hi = r.partition("-")
                for b in range(int(lo), int(hi or lo) + 1):
                    out[b] = int(row[0])
        return out

    def leader(self, replicas):
        """The writable replica of a shard."""
        for uri in replicas.split(","):
            r = subprocess.run([T, "-e", f"local c = require('net.box').connect('tarn:tarn@{uri}', "
                                f"{{wait_connected = 2}}) os.exit((c:is_connected() and not "
                                f"c:eval('return box.info.ro')) and 0 or 1)"],
                               capture_output=True, stdin=subprocess.DEVNULL)
            if r.returncode == 0:
                return uri
        return None

    def rows_on(self, replicas, table="t"):
        uri = self.leader(replicas)
        r = subprocess.run([T, "-e", f"local c = require('net.box').connect('tarn:tarn@{uri}') "
                            f"print(c:eval(\"local s = box.space['{DB}.{table}'] return s and s:count() or 0\")) "
                            f"os.exit(0)"], capture_output=True, text=True, stdin=subprocess.DEVNULL)
        return int(r.stdout.strip().splitlines()[-1])

    def balanced(self, label, tolerance=0.12):
        """Every active shard has its share of the buckets and of t's rows,
        within `tolerance` (rows: twice that)."""
        m = self.members()
        owners = self.owners()
        counts = {s: 0 for s, (st, _) in m.items() if st == "active"}
        for s in owners.values():
            counts[s] = counts.get(s, 0) + 1
        mean = sum(counts.values()) / len(counts)
        self.check(sum(counts.values()) == 480 and all(abs(n - mean) <= tolerance * mean for n in counts.values()),
                   f"{label}: the active shards have even shares of the buckets: {counts}")
        rows = {s: self.rows_on(m[s][1]) for s in counts}
        mean = sum(rows.values()) / len(rows)
        self.check(sum(rows.values()) == ROWS and all(abs(n - mean) <= 2 * tolerance * mean for n in rows.values()),
                   f"{label}: ... and of the table's rows: {rows}")

    def rows_ok(self, label, servers=None):
        for k in servers if servers is not None else range(len(self.servers)):
            n = self.sql(k, "SELECT COUNT(*), SUM(v) FROM t")[0]
            self.check(n[0] == ROWS and int(n[1]) == sum(range(ROWS)), f"{label}: SQL server {k + 1} reads every row",
                       n)

    def rebalance_ends(self, label, by="size"):
        s = self.wait_status(ENDED)
        self.check(s is not None and s[STATUS] == "done" and s[BY] == by and s[FAILED] == 0 and s[PLANNED] == 0,
                   f"{label}: it ends done ({s[DONE] if s else 0} moves, {s[BYTES] if s else 0} bytes)", s)
        return s

    # -- the grow phase -------------------------------------------------------

    def grow(self):
        a = self.args
        c = connect(self.servers[0], db=None)
        q(c, f"DROP DATABASE IF EXISTS {DB}")
        q(c, f"CREATE DATABASE {DB}")
        q(c, f"CREATE TABLE {DB}.t (id INT PRIMARY KEY, v INT NOT NULL, pad VARCHAR(400) NOT NULL)")
        pad = "x" * 300
        for lo in range(0, ROWS, 500):
            q(c, f"INSERT INTO {DB}.t VALUES " + ",".join(f"({i}, {i}, '{pad}')" for i in range(lo, lo + 500)))
        time.sleep(a.start_after)

        self.step("1. shards join")
        for sid, reps in self.add:
            self.sql(0, f"TARN ADD SHARD {sid} '{reps}'")
        m = self.members()
        owners = self.owners()
        self.check(all(m.get(s, ("",))[0] == "active" and s not in owners.values() for s, _ in self.add),
                   f"1: shards {[s for s, _ in self.add]} joined, with no buckets", m)

        self.step("2. TARN REBALANCE")
        r = self.sql(1, "TARN REBALANCE MAX MOVES 4")
        s = self.status(2)
        self.check(s is not None and s[STATUS] == "running" and s[BY] == "size" and s[MAX_MOVES] == 4,
                   f"2: it runs (SHOW TARN REBALANCE, through another server): {r}", s)
        try:
            self.sql(0, "TARN REBALANCE")
            self.check(False, "2: a second one is refused while it runs")
        except pymysql.MySQLError as e:
            self.check("in progress" in str(e), "2: a second one is refused while it runs", str(e))
        for _ in range(5):
            moves = self.sql(0, "SHOW TARN MOVES")
            info = self.sql(0, "SELECT BUCKET, FROM_SHARD, TO_SHARD, BYTES, STATE, ERROR "
                               "FROM information_schema.TARN_MOVES")
            if {m[0] for m in moves} == {m[0] for m in info}:
                break
        states = collections.Counter(m[4] for m in moves)
        self.check(len(moves) > 0 and {m[0] for m in moves} == {m[0] for m in info}
                   and set(states) <= {"planned", "moving", "copying", "fenced", "done", "failed"},
                   f"2: SHOW TARN MOVES and information_schema.TARN_MOVES list the round's moves: {dict(states)}",
                   (moves[:3], info[:3]))
        self.check(all(m[2] in (3, 4) and m[1] in (1, 2) for m in moves), "2: ... to the new shards", moves[:5])
        self.sql(0, "TARN REBALANCE PAUSE")
        d1 = self.wait_still()
        time.sleep(2)
        d2 = self.status()
        self.check(d1[STATUS] == "paused" and d1[DONE] == d2[DONE] and d2[MOVING] == 0 and d2[PLANNED] > 0,
                   f"2: paused, it starts no move (done {d1[DONE]}, then {d2[DONE]})", (d1, d2))
        self.sql(2, "TARN REBALANCE RESUME")
        self.rebalance_ends("2")
        self.balanced("2")
        self.rows_ok("2")

        if a.remove:
            self.step(f"3. shard {a.remove} leaves")
            gone = self.members()[a.remove][1]
            self.sql(0, f"TARN REMOVE SHARD {a.remove}")
            self.check(self.members()[a.remove][0] == "removed", f"3: shard {a.remove} is removed")
            self.check(self.rows_on(gone) == 0, "3: its storage keeps no row of the table")
            for f in sorted(os.listdir(a.dir)):
                if f.startswith(f"s{a.remove}.") and f.endswith(".pid"):
                    with open(os.path.join(a.dir, f)) as fh:
                        os.kill(int(fh.read()), signal.SIGKILL)
            r = self.sql(1, "TARN REBALANCE")
            self.check(True, f"3: TARN REBALANCE of the shards left: {r}")
            self.rebalance_ends("3")
            self.balanced("3")
            self.rows_ok("3")
        self.step("grown and shrunk")
        return self.failures

    # -- the control phase ----------------------------------------------------

    def hot_reads(self, ids, n_threads=8, per_thread=400):
        def run(k):
            c = connect(self.servers[k % 2])
            for i in range(per_thread):
                q(c, "SELECT v FROM t WHERE id = %s", (ids[(k * per_thread + i) % len(ids)],))
            c.close()
        ths = [threading.Thread(target=run, args=(k,)) for k in range(n_threads)]
        for t in ths:
            t.start()
        for t in ths:
            t.join()

    def control(self):
        a = self.args
        self.step("4. TARN REBALANCE BY LOAD")
        # (the workloads' requests are no longer counted)
        time.sleep(2 * a.load_window + 1)
        active = self.active()
        owners = self.owners()
        count = len(owners)
        hot_shard = active[0]
        hot, hot_buckets = [], set()
        for i in range(ROWS):
            b = bucket_of(i, count)
            if owners[b] == hot_shard and b not in hot_buckets:
                hot.append(i)
                hot_buckets.add(b)
            if len(hot) == 30:
                break
        t0 = time.time()
        self.hot_reads(hot)
        self.sql(0, "TARN REBALANCE BY LOAD MAX MOVES 2")
        moves = self.sql(0, "SHOW TARN MOVES")
        froms = collections.Counter(m[1] for m in moves)
        first = moves[:5]
        self.check(len(moves) > 0 and froms.most_common(1)[0][0] == hot_shard
                   and all(m[1] == hot_shard and m[0] in hot_buckets for m in first),
                   f"4: its first moves take the buckets that were read off their shard (shard {hot_shard}; "
                   f"reads took {time.time() - t0:.1f} s): {dict(froms)}", moves[:8])
        self.rebalance_ends("4", by="load")
        self.rows_ok("4")

        self.step("5. a rebalance cancelled at once")
        # (rows to move: in the buckets of one shard)
        self.sql(0, "CREATE TABLE skew (id INT PRIMARY KEY, pad VARCHAR(2000) NOT NULL)")
        owners = self.owners()
        heavy = self.active()[-1]
        ids = [i for i in range(100000) if owners[bucket_of(i, count)] == heavy][:600]
        for lo in range(0, len(ids), 100):
            self.sql(0, "INSERT INTO skew VALUES " + ",".join(f"({i}, '{'y' * 1500}')" for i in ids[lo:lo + 100]))
        self.sql(0, "TARN REBALANCE MAX MOVES 1")
        self.sql(1, "TARN REBALANCE CANCEL")
        s1 = self.wait_still()
        time.sleep(2)
        s2 = self.status()
        self.check(s1[STATUS] == "cancelled" and s1[DONE] == s2[DONE] and s2[MOVING] == 0 and s2[PLANNED] == 0,
                   f"5: cancelled, it moves nothing more ({s2[DONE]} moves)", (s1, s2))
        cancelled = [m for m in self.sql(0, "SHOW TARN MOVES") if m[4] == "cancelled"]
        info = self.sql(1, "SELECT BUCKET FROM information_schema.TARN_MOVES WHERE STATE = 'cancelled' "
                           "ORDER BY BUCKET")
        self.check(len(cancelled) > 0 and [r[0] for r in info] == sorted(m[0] for m in cancelled),
                   f"5: SHOW TARN MOVES and information_schema.TARN_MOVES show the moves it did not make "
                   f"({len(cancelled)})", (cancelled[:3], info[:3]))
        try:
            self.sql(0, "TARN REBALANCE RESUME")
            self.check(False, "5: ... and it cannot be resumed")
        except pymysql.MySQLError as e:
            self.check("ended" in str(e), "5: ... and it cannot be resumed", str(e))

        self.step("6. the SQL server running a rebalance is killed")
        self.sql(2, "TARN REBALANCE MAX MOVES 1")
        first = self.status()[OWNER]
        with open(os.path.join(a.dir, "tarn3.pid")) as f:
            os.kill(int(f.read()), signal.SIGKILL)
        t0 = time.time()
        s = self.rebalance_ends("6")
        self.check(s is not None and s[OWNER] != first and s[DONE] > 0,
                   f"6: another server took it over and finished it ({time.time() - t0:.1f} s; "
                   f"owner {first}, then {s[OWNER] if s else None})", s)
        self.start_server(2)
        self.rows_ok("6")
        n = self.sql(0, "SELECT COUNT(*) FROM skew")[0][0]
        self.check(n == len(ids), "6: the second table's rows are all there", n)

        self.step("7. a rebalance limited to 150,000 bytes a second")
        rate = 150_000
        owners = self.owners()
        heavy = self.active()[0]
        ids = [i for i in range(100000, 200000) if owners[bucket_of(i, count)] == heavy][:600]
        for lo in range(0, len(ids), 100):
            self.sql(0, "INSERT INTO skew VALUES " + ",".join(f"({i}, '{'z' * 1500}')" for i in ids[lo:lo + 100]))
        t0 = time.time()
        self.sql(0, f"TARN REBALANCE MAX MOVES 8 MAX RATE {rate}")
        s = self.rebalance_ends("7")
        secs = time.time() - t0
        biggest = max((m[3] for m in self.sql(0, "SHOW TARN MOVES")), default=0)
        self.check(s is not None and s[MAX_RATE] == rate and s[BYTES] > 2 * rate
                   and s[BYTES] <= rate * secs + 8 * biggest,
                   f"7: {s[BYTES] if s else 0} bytes in {secs:.1f} s: at most the rate (and the moves "
                   f"started at once)", s)
        return self.failures

    def start_server(self, k):
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=("grow", "control"), required=True)
    ap.add_argument("--tarn", required=True, help="three SQL servers, comma-separated (the third is killed)")
    ap.add_argument("--add", action="append", default=[], help="a shard that joins: id=replica[,replica...]")
    ap.add_argument("--remove", type=int, help="a shard that leaves afterwards")
    ap.add_argument("--dir", required=True, help="the cluster's directory (pid files)")
    ap.add_argument("--start-after", type=float, default=3)
    ap.add_argument("--load-window", type=float, default=5, help="the shards' TARN_LOAD_WINDOW")
    args = ap.parse_args()
    t = Test(args)
    failures = t.grow() if args.phase == "grow" else t.control()
    print("PASSED" if failures == 0 else f"{failures} FAILED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
