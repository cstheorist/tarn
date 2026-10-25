#!/usr/bin/env python3
"""Transaction tests for Tarn (serializable, cross-shard atomicity, direct KV
visibility) plus a concurrent bank-transfer invariant check.

    tests/sql/txntest.py --tarn 127.0.0.1:4000 [--kv 127.0.0.1:3311,127.0.0.1:3321]
                         [--bank-seconds 20] [--threads 8] [--only NAME]

--kv lists one storage node per shard (any replica) for the direct
Key/Value read checks, which use the Tarantool binary ($TARANTOOL).
"""
import argparse
import json
import os
import random
import subprocess
import sys
import threading
import time

import pymysql

TARANTOOL = os.environ.get("TARANTOOL", "/opt/src/tarantool/build/src/tarantool")
DB = "txntest"
CONFLICT = 1213


def connect(hp, db=DB, autocommit=True):
    host, port = hp.split(":")
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db,
                           autocommit=autocommit, charset="utf8mb4")


def q(c, sql, args=None):
    cur = c.cursor()
    cur.execute(sql, args)
    rows = cur.fetchall() if cur.description else cur.rowcount
    cur.close()
    return rows


def expect_conflict(fn):
    try:
        fn()
    except pymysql.MySQLError as e:
        if e.args[0] == CONFLICT:
            return True
        raise
    return False


def kv_get(nodes, space, key):
    """Read one row straight from Tarantool on every given node; return the
    row from whichever node holds it (rows live on exactly one shard)."""
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kvget.lua")
    p = subprocess.run([TARANTOOL, script, json.dumps(nodes), space, json.dumps(key)],
                       capture_output=True, text=True, timeout=20, stdin=subprocess.DEVNULL)
    if p.returncode != 0:
        raise RuntimeError(p.stderr)
    found = json.loads(p.stdout.strip().splitlines()[-1])
    if len(found) > 1:
        raise AssertionError(f"row {key} present on {len(found)} shards: {found}")
    return found[0] if found else None


class T:
    def __init__(self, args):
        self.args = args
        self.failures = 0

    def check(self, cond, what):
        if not cond:
            self.failures += 1
            print(f"    FAILED: {what}")
        return cond

    def fresh(self, name, ddl, rows):
        c = connect(self.args.tarn)
        q(c, f"DROP TABLE IF EXISTS {name}")
        q(c, ddl)
        if rows:
            q(c, f"INSERT INTO {name} VALUES " + ", ".join(rows))
        return c

    # ------------------------------------------------------------------
    def test_rollback(self):
        c = self.fresh("r", "CREATE TABLE r (id INT PRIMARY KEY, v INT)", ["(1, 10)", "(2, 20)"])
        q(c, "BEGIN")
        q(c, "INSERT INTO r VALUES (3, 30)")
        q(c, "UPDATE r SET v = 11 WHERE id = 1")
        q(c, "DELETE FROM r WHERE id = 2")
        self.check(q(c, "SELECT id, v FROM r ORDER BY id") == ((1, 11), (3, 30)), "read-your-writes inside txn")
        other = connect(self.args.tarn)
        self.check(q(other, "SELECT id, v FROM r ORDER BY id") == ((1, 10), (2, 20)), "uncommitted writes invisible")
        q(c, "ROLLBACK")
        self.check(q(c, "SELECT id, v FROM r ORDER BY id") == ((1, 10), (2, 20)), "rollback restores")

    def test_commit_visibility(self):
        c = self.fresh("cv", "CREATE TABLE cv (id INT PRIMARY KEY, v INT)", [f"({i}, 0)" for i in range(1, 21)])
        q(c, "BEGIN")
        q(c, "UPDATE cv SET v = 1")  # all shards
        other = connect(self.args.tarn)
        self.check(q(other, "SELECT SUM(v) FROM cv")[0][0] == 0, "invisible before commit")
        if self.args.kv:
            row = kv_get(self.args.kv, f"{DB}.cv", [7])
            self.check(row is not None and row[1] == 0, f"direct KV read sees committed value only: {row}")
        q(c, "COMMIT")
        self.check(q(other, "SELECT SUM(v) FROM cv")[0][0] == 20, "all visible after commit")
        if self.args.kv:
            for k in (1, 7, 13, 20):
                row = kv_get(self.args.kv, f"{DB}.cv", [k])
                self.check(row is not None and row[0] == k and row[1] == 1, f"direct KV read of {k}: {row}")

    def test_autocommit_kv(self):
        if not self.args.kv:
            return
        c = self.fresh("kv", "CREATE TABLE kv (k VARCHAR(20) PRIMARY KEY, v VARCHAR(20), n BIGINT)", [])
        q(c, "INSERT INTO kv VALUES ('alpha', 'one', 1), ('beta', NULL, -5)")
        a = kv_get(self.args.kv, f"{DB}.kv", ["alpha"])
        b = kv_get(self.args.kv, f"{DB}.kv", ["beta"])
        self.check(a is not None and a[:3] == ["alpha", "one", 1], f"KV alpha: {a}")
        self.check(b is not None and b[0] == "beta" and b[1] is None and b[2] == -5, f"KV beta: {b}")
        self.check(a is not None and isinstance(a[3], int) and 1 <= a[3] <= 3000, f"bucket_id field: {a}")

    def test_lost_update(self):
        self.fresh("lu", "CREATE TABLE lu (id INT PRIMARY KEY, v INT)", ["(1, 100)"])
        a, b = connect(self.args.tarn), connect(self.args.tarn)
        q(a, "BEGIN")
        q(b, "BEGIN")
        va = q(a, "SELECT v FROM lu WHERE id = 1")[0][0]
        vb = q(b, "SELECT v FROM lu WHERE id = 1")[0][0]
        q(a, f"UPDATE lu SET v = {va + 1} WHERE id = 1")
        q(b, f"UPDATE lu SET v = {vb + 1} WHERE id = 1")
        q(a, "COMMIT")
        conflicted = expect_conflict(lambda: q(b, "COMMIT"))
        self.check(conflicted, "second read-modify-write must abort")
        final = q(connect(self.args.tarn), "SELECT v FROM lu WHERE id = 1")[0][0]
        self.check(final == 101, f"final value {final} (expected 101)")

    def test_write_skew(self):
        # Doctors on call: each txn sees two on call and takes itself off.
        # Under snapshot isolation both commit (zero on call); serializable
        # must abort one. Rows are chosen to land on different shards.
        c = self.fresh("oncall", "CREATE TABLE oncall (id INT PRIMARY KEY, on_call INT)",
                       [f"({i}, 1)" for i in range(1, 9)])
        q(c, "UPDATE oncall SET on_call = 0 WHERE id > 2")
        a, b = connect(self.args.tarn), connect(self.args.tarn)
        q(a, "BEGIN")
        q(b, "BEGIN")
        na = q(a, "SELECT COUNT(*) FROM oncall WHERE on_call = 1")[0][0]
        nb = q(b, "SELECT COUNT(*) FROM oncall WHERE on_call = 1")[0][0]
        self.check(na == nb == 2, "both see two on call")
        q(a, "UPDATE oncall SET on_call = 0 WHERE id = 1")
        q(b, "UPDATE oncall SET on_call = 0 WHERE id = 2")
        ca = expect_conflict(lambda: q(a, "COMMIT"))
        cb = expect_conflict(lambda: q(b, "COMMIT"))
        self.check(ca or cb, "write skew: one of the two must abort")
        left = q(c, "SELECT COUNT(*) FROM oncall WHERE on_call = 1")[0][0]
        self.check(left >= 1, f"invariant: someone on call ({left})")

    def test_phantom(self):
        c = self.fresh("ph", "CREATE TABLE ph (id INT PRIMARY KEY, grp INT, KEY g (grp))",
                       ["(1, 1)", "(2, 1)", "(3, 2)"])
        a, b = connect(self.args.tarn), connect(self.args.tarn)
        q(a, "BEGIN")
        n1 = q(a, "SELECT COUNT(*) FROM ph WHERE grp = 1")[0][0]
        q(b, "INSERT INTO ph VALUES (10, 1)")  # autocommit, may land on any shard
        n2 = q(a, "SELECT COUNT(*) FROM ph WHERE grp = 1")[0][0]
        q(a, "INSERT INTO ph VALUES (20, 3)")  # make a a writer
        aborted = expect_conflict(lambda: q(a, "COMMIT"))
        # Serializable: either a saw a stable count and must abort (it read a
        # range that b changed before a committed), or it never saw b's row.
        self.check(aborted or n1 == n2 == 2, f"phantom handled (n1={n1} n2={n2} aborted={aborted})")
        self.check(q(c, "SELECT COUNT(*) FROM ph WHERE id = 10")[0][0] == 1, "b's insert committed")

    def test_fractured_read(self):
        # R reads y, then T updates x and y atomically (across shards for
        # most pairs) and commits, then R reads x. R must not commit having
        # seen T's write to x but not to y.
        c = self.fresh("fr", "CREATE TABLE fr (id INT PRIMARY KEY, v INT)", [f"({i}, 0)" for i in range(1, 13)])
        r, t = connect(self.args.tarn), connect(self.args.tarn)
        bad = []
        for y in range(2, 13):
            x = 1
            q(r, "BEGIN")
            vy = q(r, "SELECT v FROM fr WHERE id = %s", (y,))[0][0]
            q(t, "BEGIN")
            q(t, "UPDATE fr SET v = v + 1 WHERE id = %s", (x,))
            q(t, "UPDATE fr SET v = v + 1 WHERE id = %s", (y,))
            q(t, "COMMIT")
            vx = q(r, "SELECT v FROM fr WHERE id = %s", (x,))[0][0]
            committed = not expect_conflict(lambda: q(r, "COMMIT"))
            if not committed:
                try:
                    q(r, "ROLLBACK")
                except pymysql.MySQLError:
                    pass
            # before T: v(x) = y - 2, v(y) = 0; after T: v(x) = y - 1, v(y) = 1
            if committed and (vx == y - 1) != (vy == 1):
                bad.append((x, y, vx, vy))
        self.check(not bad, f"fractured reads committed: {bad}")

    def test_realtime_order(self):
        # Strict serializability across shards: R reads y; U updates y and
        # commits; then V updates x and commits (V starts after U finished);
        # then R reads x. If R saw V's x it must also see U's y, or abort.
        self.fresh("rt", "CREATE TABLE rt (id INT PRIMARY KEY, v INT)", [f"({i}, 0)" for i in range(1, 13)])
        r, u = connect(self.args.tarn), connect(self.args.tarn)
        bad = []
        for y in range(2, 13):
            x = 1
            vx0 = q(u, "SELECT v FROM rt WHERE id = %s", (x,))[0][0]
            q(r, "BEGIN")
            vy = q(r, "SELECT v FROM rt WHERE id = %s", (y,))[0][0]
            q(u, "UPDATE rt SET v = v + 1 WHERE id = %s", (y,))
            q(u, "UPDATE rt SET v = v + 1 WHERE id = %s", (x,))
            vx = q(r, "SELECT v FROM rt WHERE id = %s", (x,))[0][0]
            committed = not expect_conflict(lambda: q(r, "COMMIT"))
            if not committed:
                try:
                    q(r, "ROLLBACK")
                except pymysql.MySQLError:
                    pass
            if committed and vx == vx0 + 1 and vy == 0:
                bad.append((x, y))
        self.check(not bad, f"real-time order violated (saw V's x but not U's y) for {bad}")

    def test_cross_shard_atomic_dml(self):
        c = self.fresh("xs", "CREATE TABLE xs (id INT PRIMARY KEY, v INT)", [f"({i}, {i})" for i in range(1, 51)])
        q(c, "BEGIN")
        q(c, "UPDATE xs SET v = v + 1000 WHERE id <= 25")
        q(c, "DELETE FROM xs WHERE id > 45")
        q(c, "INSERT INTO xs VALUES (100, 100), (101, 101), (102, 102)")
        q(c, "COMMIT")
        rows = dict(q(c, "SELECT id, v FROM xs"))
        exp = {i: (i + 1000 if i <= 25 else i) for i in range(1, 46)}
        exp.update({100: 100, 101: 101, 102: 102})
        self.check(rows == exp, "cross-shard transaction applied exactly")

    def test_duplicate_in_txn(self):
        c = self.fresh("dup", "CREATE TABLE dup (id INT PRIMARY KEY, v INT)", ["(1, 1)"])
        q(c, "BEGIN")
        try:
            q(c, "INSERT INTO dup VALUES (1, 2)")
            got = None
        except pymysql.MySQLError as e:
            got = e.args[0]
        self.check(got == 1062, f"duplicate key inside txn -> 1062 (got {got})")
        q(c, "INSERT INTO dup VALUES (2, 2)")
        q(c, "COMMIT")
        self.check(q(c, "SELECT COUNT(*) FROM dup")[0][0] == 2, "txn continues after statement error")

    def test_select_for_update(self):
        self.fresh("sfu", "CREATE TABLE sfu (id INT PRIMARY KEY, v INT)", ["(1, 0)"])
        a, b = connect(self.args.tarn), connect(self.args.tarn)
        q(a, "BEGIN")
        q(a, "SELECT v FROM sfu WHERE id = 1 FOR UPDATE")
        q(b, "UPDATE sfu SET v = 5 WHERE id = 1")
        aborted = expect_conflict(lambda: (q(a, "UPDATE sfu SET v = v + 1 WHERE id = 1"), q(a, "COMMIT")))
        v = q(b, "SELECT v FROM sfu WHERE id = 1")[0][0]
        self.check((aborted and v == 5) or (not aborted and v == 6), f"FOR UPDATE + concurrent write (aborted={aborted} v={v})")

    # ------------------------------------------------------------------
    def test_bank(self):
        n_acc, init = 64, 1000
        total = n_acc * init
        self.fresh("bank", "CREATE TABLE bank (id INT PRIMARY KEY, bal BIGINT NOT NULL)",
                   [f"({i}, {init})" for i in range(n_acc)])
        stop = time.time() + self.args.bank_seconds
        stats = {"commit": 0, "conflict": 0, "audit": 0, "bad": [], "err": [], "times": []}
        t_start = time.time()
        lock = threading.Lock()

        def transfer(seed):
            rnd = random.Random(seed)
            c = connect(self.args.tarn)
            while time.time() < stop:
                x, y = rnd.sample(range(n_acc), 2)
                amt = rnd.randint(1, 50)
                try:
                    q(c, "BEGIN")
                    bx = q(c, "SELECT bal FROM bank WHERE id = %s", (x,))[0][0]
                    if bx >= amt:
                        q(c, "UPDATE bank SET bal = bal - %s WHERE id = %s", (amt, x))
                        q(c, "UPDATE bank SET bal = bal + %s WHERE id = %s", (amt, y))
                    q(c, "COMMIT")
                    with lock:
                        stats["commit"] += 1
                        stats["times"].append(time.time() - t_start)
                except pymysql.MySQLError as e:
                    try:
                        q(c, "ROLLBACK")
                    except pymysql.MySQLError:
                        c = connect(self.args.tarn)
                    with lock:
                        if e.args[0] == CONFLICT:
                            stats["conflict"] += 1
                        else:
                            stats["err"].append(str(e))
                            time.sleep(0.05)

        def audit():
            c = connect(self.args.tarn)
            while time.time() < stop:
                try:
                    q(c, "BEGIN")
                    s = q(c, "SELECT SUM(bal), COUNT(*), MIN(bal) FROM bank")[0]
                    q(c, "COMMIT")
                except pymysql.MySQLError as e:
                    try:
                        q(c, "ROLLBACK")
                    except pymysql.MySQLError:
                        c = connect(self.args.tarn)
                    if e.args[0] != CONFLICT:
                        with lock:
                            stats["err"].append("audit: " + str(e))
                    continue
                with lock:
                    stats["audit"] += 1
                    if int(s[0]) != total or s[1] != n_acc or s[2] < 0:
                        stats["bad"].append(s)
                # autocommit point reads of two accounts must also be sane
                try:
                    s2 = q(c, "SELECT SUM(bal) FROM bank")[0][0]
                    if int(s2) != total:
                        with lock:
                            stats["bad"].append(("autocommit", s2))
                except pymysql.MySQLError:
                    pass

        th = [threading.Thread(target=transfer, args=(i,)) for i in range(self.args.threads)]
        th += [threading.Thread(target=audit) for _ in range(2)]
        for t in th:
            t.start()
        for t in th:
            t.join()
        final = q(connect(self.args.tarn), "SELECT SUM(bal), COUNT(*) FROM bank")[0]
        print(f"    bank: {stats['commit']} commits, {stats['conflict']} conflicts, {stats['audit']} audits, "
              f"{len(stats['err'])} other errors, final sum {final[0]}")
        for e in stats["err"][:5]:
            print(f"      error: {e}")
        buckets = [0] * (int(self.args.bank_seconds) // 3 + 1)
        for t_ in stats["times"]:
            buckets[min(int(t_ // 3), len(buckets) - 1)] += 1
        print("    commits per 3 s:", " ".join(str(b) for b in buckets))
        self.check(not stats["bad"], f"audits saw a broken invariant: {stats['bad'][:5]}")
        self.check(int(final[0]) == total and final[1] == n_acc, "final balance sum")
        self.check(stats["commit"] > 0, "some transfers committed")
        if self.args.kv:
            s = 0
            for i in range(n_acc):
                row = kv_get(self.args.kv, f"{DB}.bank", [i])
                s += row[1]
            self.check(s == total, f"sum over direct KV reads = {s}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True)
    ap.add_argument("--kv", default="")
    ap.add_argument("--bank-seconds", type=float, default=15)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--only", default="")
    args = ap.parse_args()
    args.kv = [x for x in args.kv.split(",") if x]
    c = connect(args.tarn, db=None)
    q(c, f"DROP DATABASE IF EXISTS {DB}")
    q(c, f"CREATE DATABASE {DB}")
    t = T(args)
    names = [n for n in dir(t) if n.startswith("test_") and (not args.only or args.only in n)]
    order = sorted(names, key=lambda n: (n == "test_bank", n))
    for n in order:
        before = t.failures
        t0 = time.time()
        try:
            getattr(t, n)()
        except Exception as e:  # noqa: BLE001
            t.failures += 1
            print(f"    EXCEPTION: {type(e).__name__}: {e}")
        print(f"{'ok  ' if t.failures == before else 'FAIL'} {n} ({time.time() - t0:.1f}s)")
    print(f"{len(order)} tests, {t.failures} failures")
    sys.exit(1 if t.failures else 0)


if __name__ == "__main__":
    main()
