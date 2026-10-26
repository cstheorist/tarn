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

TARANTOOL = os.environ.get("TARANTOOL", "tarantool")
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

    def test_count_in_storage(self):
        # COUNTs the shards compute (issue #52) see the transaction's own
        # buffered writes, and a serializable count is validated at commit.
        c = self.fresh("cn", "CREATE TABLE cn (g INT NOT NULL, id INT NOT NULL, s VARCHAR(10), f TINYINT NOT NULL, "
                       "PRIMARY KEY (g, id), KEY gs (g, s))",
                       [f"({i % 3}, {i}, {'NULL' if i % 4 == 0 else repr('s' + str(i % 2))}, {i % 2})"
                        for i in range(60)])
        before = int(dict(q(c, "SHOW STATUS LIKE 'Tarn_storage_counts'"))["Tarn_storage_counts"])
        queries = ["SELECT COUNT(*) FROM cn WHERE g = 1", "SELECT COUNT(*) FROM cn WHERE g = 1 AND f = 0",
                   "SELECT COUNT(s) FROM cn WHERE g = 2", "SELECT COUNT(*) FROM cn WHERE g = 0 AND s IS NULL",
                   "SELECT COUNT(*) FROM cn", "SELECT COUNT(*) FROM cn WHERE id >= 10 AND s IN ('s0', 'x')"]

        def counts(conn):
            return [q(conn, s)[0][0] for s in queries]

        def by_rows(conn):
            # the same counts from the rows (no pushdown: GROUP BY)
            out = []
            for s in queries:
                r = q(conn, s.replace("COUNT(", "0 AS z, COUNT(") + " GROUP BY z")
                out.append(r[0][1] if r else 0)
            return out
        q(c, "BEGIN")
        q(c, "INSERT INTO cn VALUES (1, 100, NULL, 0), (2, 101, 's0', 1), (0, 102, NULL, 0)")
        q(c, "UPDATE cn SET f = 1 - f, s = NULL WHERE id IN (1, 4, 7, 11)")
        q(c, "DELETE FROM cn WHERE id IN (2, 13, 30)")
        q(c, "UPDATE cn SET g = 2 WHERE id = 10")
        mine = counts(c)
        self.check(mine == by_rows(c), f"counts with buffered writes {mine} vs {by_rows(c)}")
        other = connect(self.args.tarn)
        self.check(counts(other) == by_rows(other) and counts(other) != mine, "others count the committed rows")
        q(c, "COMMIT")
        self.check(counts(other) == mine, "committed counts")
        after = int(dict(q(c, "SHOW STATUS LIKE 'Tarn_storage_counts'"))["Tarn_storage_counts"])
        self.check(after > before, "the counts ran in storage")
        # Serializable: a count, then another transaction inserts into its
        # range, then the counting transaction writes: it must abort.
        a, b = connect(self.args.tarn), connect(self.args.tarn)
        q(a, "BEGIN")
        n1 = q(a, "SELECT COUNT(*) FROM cn WHERE g = 1 AND f = 0")[0][0]
        q(b, "INSERT INTO cn VALUES (1, 200, NULL, 0)")
        n2 = q(a, "SELECT COUNT(*) FROM cn WHERE g = 1 AND f = 0")[0][0]
        q(a, "INSERT INTO cn VALUES (2, 201, NULL, 0)")
        aborted = expect_conflict(lambda: q(a, "COMMIT"))
        self.check(aborted and n1 == n2, f"counted range validated (n1={n1} n2={n2} aborted={aborted})")

    def test_long_reads_in_pages(self):
        # Issue #77: storage returns a long read a page at a time (4096
        # rows or 4 MiB). Autocommit and inside a transaction, a read of
        # several pages returns what one read would, with a residual WHERE
        # applied page by page and the transaction's own writes overlaid;
        # the whole range is validated at commit.
        n = 13000
        c = self.fresh("lr", "CREATE TABLE lr (id INT PRIMARY KEY, g INT NOT NULL, pad VARCHAR(400))", [])
        for lo in range(0, n, 1000):
            q(c, "INSERT INTO lr VALUES " + ", ".join(f"({i}, {i % 10}, REPEAT('x', {i % 400}))"
                                                     for i in range(lo, lo + 1000)))
        total = sum(range(n))
        sum7 = sum(i for i in range(n) if i % 10 == 7)
        self.check(q(c, "SELECT COUNT(*), SUM(id) FROM lr") == ((n, total),), "autocommit, every row")
        self.check(q(c, "SELECT SUM(id) FROM lr WHERE g = 7")[0][0] == sum7, "autocommit, an unindexed WHERE")
        ids = [r[0] for r in q(c, "SELECT id FROM lr WHERE id >= 100 AND pad LIKE 'xx%' ORDER BY id LIMIT 9000")]
        want = [i for i in range(100, n) if i % 400 >= 2][:9000]
        self.check(ids == want, f"a range, a LIKE and a LIMIT across pages: {len(ids)} rows")
        self.check(len(q(c, "SELECT pad FROM lr")) == n, "every row with its text")
        q(c, "BEGIN")
        q(c, "UPDATE lr SET g = 7 WHERE id IN (1, 12999)")
        q(c, "DELETE FROM lr WHERE id = 17")
        q(c, "INSERT INTO lr VALUES (20000, 7, '')")
        mine = sum7 + 1 + 12999 - 17 + 20000
        self.check(q(c, "SELECT SUM(id) FROM lr WHERE g = 7")[0][0] == mine, "in a transaction, its writes overlaid")
        q(c, "ROLLBACK")
        a, b = connect(self.args.tarn), connect(self.args.tarn)
        q(a, "BEGIN")
        s1 = q(a, "SELECT SUM(id) FROM lr WHERE g = 7")[0][0]
        q(b, "UPDATE lr SET g = 7 WHERE id = 12998")   # in the last page of the scan
        s2 = q(a, "SELECT SUM(id) FROM lr WHERE g = 7")[0][0]
        q(a, "INSERT INTO lr VALUES (20001, 0, '')")
        aborted = expect_conflict(lambda: q(a, "COMMIT"))
        self.check(s1 == s2 == sum7 and aborted, f"a long read is validated as a whole ({s1} {s2} {aborted})")

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

    def test_case_insensitive_keys(self):
        """Case variants of a key under utf8mb4_general_ci are one key
        everywhere (issue #13): in the unique index, in the locks of
        concurrent transactions and in the shard the key routes to (by its
        weight string). Of two cross-shard transactions inserting 'userK'
        and 'USERK' at once, exactly one commits, serializable or READ
        COMMITTED."""
        c = self.fresh("cik", "CREATE TABLE cik (handle VARCHAR(20) PRIMARY KEY, v INT) COLLATE utf8mb4_general_ci",
                       [])
        self.fresh("cio", "CREATE TABLE cio (id INT PRIMARY KEY, v INT)", [f"({i}, 0)" for i in range(1, 9)])
        bad = []
        for iso, tag in (("REPEATABLE-READ", "s"), ("READ-COMMITTED", "c")):
            for k in range(10):
                key = f"user{tag}{k}"
                variants = [key.lower(), key.upper()]
                res = [None, None]
                barrier = threading.Barrier(2)

                def worker(i):
                    conn = connect(self.args.tarn)
                    q(conn, f"SET SESSION tx_isolation = '{iso}'")
                    try:
                        q(conn, "BEGIN")
                        # rows on every shard, apart from the other worker's
                        q(conn, f"UPDATE cio SET v = v + 1 WHERE id IN ({i + 1}, {i + 3}, {i + 5}, {i + 7})")
                        barrier.wait(20)
                        q(conn, "INSERT INTO cik VALUES (%s, %s)", (variants[i], i))
                        q(conn, "COMMIT")
                        res[i] = "ok"
                    except pymysql.MySQLError as e:
                        res[i] = e.args[0]
                        try:
                            q(conn, "ROLLBACK")
                        except pymysql.MySQLError:
                            pass
                    except threading.BrokenBarrierError:
                        res[i] = "barrier"

                ths = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
                for th in ths:
                    th.start()
                for th in ths:
                    th.join(60)
                rows = q(c, "SELECT handle, v FROM cik WHERE handle = %s", (key.upper(),))
                same = q(c, "SELECT handle, v FROM cik WHERE handle = %s", (key.lower(),))
                losers = [r for r in res if r != "ok"]
                if not (res.count("ok") == 1 and losers[0] in (1062, CONFLICT) and len(rows) == 1 and rows == same):
                    bad.append((iso, key, res, rows))
        self.check(not bad, f"one of two inserts of case variants commits, the other fails: {bad[:3]}")
        self.check(q(c, "SELECT COUNT(*) FROM cik")[0][0] == 20, "one row per key")
    def rows_read(self, c):
        return int(dict(q(c, "SHOW STATUS LIKE 'Tarn_rows_read'"))["Tarn_rows_read"])

    def test_ordered_limit_pages(self):
        # issue #41: an ordered page with LIMIT reads the index that follows
        # ORDER BY a page at a time and stops; a serializable read records
        # only what it read, so writes past it do not conflict
        c = self.fresh("ol", "CREATE TABLE ol (g INT, id INT, t INT, del TINYINT, PRIMARY KEY (g, id), "
                             "KEY bt (g, t, id)) PARTITION BY KEY(g)", [])
        for lo in range(0, 5000, 1000):
            q(c, "INSERT INTO ol VALUES " + ",".join(f"(1, {i}, {5000 - i}, {int(i % 4 == 0)})"
                                                   for i in range(lo, lo + 1000)))
        page = "SELECT id FROM ol WHERE g = 1 AND del = 0 ORDER BY t, id LIMIT 50"
        want = [i for i in range(4999, -1, -1) if i % 4][:50]
        # issue #50: ORDER BY ... DESC reads the index backwards
        rpage = "SELECT id FROM ol WHERE g = 1 AND del = 0 ORDER BY t DESC, id DESC LIMIT 50"
        rwant = [i for i in range(5000) if i % 4][:50]
        for iso in ("SERIALIZABLE", "READ COMMITTED"):
            q(c, f"SET SESSION TRANSACTION ISOLATION LEVEL {iso}")
            for txn in (False, True):
                for sql, exp, name in ((page, want, "first page"), (rpage, rwant, "last page")):
                    if txn:
                        q(c, "BEGIN")
                    before = self.rows_read(c)
                    got = [r[0] for r in q(c, sql)]
                    n = self.rows_read(c) - before
                    if txn:
                        q(c, "COMMIT")
                    what = f"{iso}, {'in a transaction' if txn else 'autocommit'}, {name}"
                    self.check(got == exp, f"{what}: {got[:5]}...")
                    self.check(n <= 400, f"{what}: {n} rows read for a page of 50 out of 5000")
        q(c, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        # the transaction's own writes are merged into the page
        q(c, "BEGIN")
        q(c, "INSERT INTO ol VALUES (1, 9001, 0, 0)")
        q(c, "UPDATE ol SET del = 1 WHERE g = 1 AND id = 4999")
        q(c, "UPDATE ol SET t = -1 WHERE g = 1 AND id = 2001")
        got = [r[0] for r in q(c, page)]
        self.check(got == [2001, 9001] + [i for i in want if i != 4999][:48], f"own writes merged ({got[:5]}...)")
        q(c, "ROLLBACK")
        # an insert into the unread rest of the range does not conflict, one
        # into the part read does
        a, b = connect(self.args.tarn), connect(self.args.tarn)
        for sql, t, conflict in ((page, 4000, False), (page, 10, True), (rpage, 20, False), (rpage, 4990, True)):
            q(a, "BEGIN")
            q(a, sql)
            q(a, "UPDATE ol SET del = 1 - del WHERE g = 1 AND id = 0")
            q(b, f"INSERT INTO ol VALUES (1, {7000 + t}, {t}, 0)")
            self.check(expect_conflict(lambda: q(a, "COMMIT")) == conflict,
                       f"insert at t = {t}: {'conflict' if conflict else 'no conflict'}")

    def test_ordered_limit_by_row_counts(self):
        # issue #81: with a longer equality prefix on another index, the read
        # pages the index that follows ORDER BY when the prefix holds many
        # rows, and reads the prefix when it holds few
        c = self.fresh("oc", "CREATE TABLE oc (g INT, id INT, t INT, closed INT NULL, del TINYINT, cust INT, "
                             "PRIMARY KEY (g, id), KEY bt (g, t, id), KEY bs (g, closed, del, id), "
                             "KEY bc (g, cust, id)) PARTITION BY KEY(g)", [])
        rows = [(i, (i * 7919) % 5000, None if i % 5 in (1, 3) else i, int(i % 10 == 0), i % 300)
                for i in range(5000)]
        for lo in range(0, 5000, 1000):
            q(c, "INSERT INTO oc VALUES " + ",".join(
                f"(1, {i}, {t}, {'NULL' if cl is None else cl}, {d}, {cu})" for i, t, cl, d, cu in rows[lo:lo + 1000]))
        by_t = sorted(rows, key=lambda r: (r[1], r[0]))
        estimates = lambda: int(dict(q(c, "SHOW STATUS LIKE 'Tarn_path_estimates'"))["Tarn_path_estimates"])
        cases = (
            ("SELECT id FROM oc WHERE g = 1 AND closed IS NULL AND del = 0 ORDER BY t, id LIMIT 50",
             [r[0] for r in by_t if r[2] is None and r[3] == 0][:50], 400),
            ("SELECT id FROM oc WHERE g = 1 AND closed IS NULL AND del = 0 ORDER BY t DESC, id DESC LIMIT 50",
             [r[0] for r in reversed(by_t) if r[2] is None and r[3] == 0][:50], 400),
            ("SELECT id FROM oc WHERE g = 1 AND cust = 7 ORDER BY t, id LIMIT 50",
             [r[0] for r in by_t if r[4] == 7][:50], 20),
        )
        for iso in ("SERIALIZABLE", "READ COMMITTED"):
            q(c, f"SET SESSION TRANSACTION ISOLATION LEVEL {iso}")
            for txn in (False, True):
                for sql, want, most in cases:
                    if txn:
                        q(c, "BEGIN")
                    before, est = self.rows_read(c), estimates()
                    got = [r[0] for r in q(c, sql)]
                    n, e = self.rows_read(c) - before, estimates() - est
                    if txn:
                        q(c, "COMMIT")
                    what = f"{iso}, {'in a transaction' if txn else 'autocommit'}: {sql}"
                    self.check(got == want, f"{what}: rows ({got[:5]}...)")
                    self.check(n <= most and e >= 1, f"{what}: {n} rows read, {e} estimates")
        q(c, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")

    def test_ordered_limit_case_insensitive(self):
        # issue #41: the part of a range a limited read records ends at its
        # last row as the index holds it (a case-insensitive key holds
        # weight strings): writes inside it conflict, writes past it do not
        self.fresh("oci", "CREATE TABLE oci (g INT, h VARCHAR(10), n INT, PRIMARY KEY (g, h)) "
                          "COLLATE=utf8mb4_general_ci PARTITION BY KEY(g)", [f"(1, 'k{i:02d}', {i})" for i in range(40)])
        a, b = connect(self.args.tarn), connect(self.args.tarn)
        for h, conflict in (("K35x", False), ("k01x", True)):
            q(a, "BEGIN")
            got = [r[0] for r in q(a, "SELECT h FROM oci WHERE g = 1 ORDER BY h LIMIT 3")]
            self.check(got == ["k00", "k01", "k02"], f"first page {got}")
            q(a, "UPDATE oci SET n = n + 1 WHERE g = 1 AND h = 'k39'")
            q(b, f"INSERT INTO oci VALUES (1, '{h}', 0)")
            self.check(expect_conflict(lambda: q(a, "COMMIT")) == conflict,
                       f"insert of {h}: {'conflict' if conflict else 'no conflict'}")

    def test_global_unique(self):
        # A unique key without the shard key (issue #14): sessions insert,
        # rename and delete rows with values from a small set, from rows on
        # every shard, in autocommit statements and transactions. No value
        # may ever be held twice, and the global index must name exactly the
        # rows that hold each value.
        c = self.fresh("gx", "CREATE TABLE gx (id INT PRIMARY KEY, h VARCHAR(10), UNIQUE KEY uh (h))", [])
        stop = time.time() + 4
        errors = []
        counts = {"ok": 0, "dup": 0, "conflict": 0}

        def worker(w):
            s = connect(self.args.tarn)
            rnd = random.Random(w)
            while time.time() < stop:
                k = rnd.randrange(60)
                v = f"v{rnd.randrange(12)}"
                try:
                    op = rnd.randrange(4)
                    if op == 0:
                        q(s, "INSERT INTO gx VALUES (%s, %s)", (k, v))
                    elif op == 1:
                        q(s, "UPDATE gx SET h = %s WHERE id = %s", (v, k))
                    elif op == 2:
                        q(s, "DELETE FROM gx WHERE id = %s", (k,))
                    else:
                        q(s, "BEGIN")
                        q(s, "INSERT INTO gx VALUES (%s, %s) ON DUPLICATE KEY UPDATE h = NULL", (k, v))
                        q(s, "UPDATE gx SET h = %s WHERE id = %s", (f"v{rnd.randrange(12)}", rnd.randrange(60)))
                        q(s, "COMMIT")
                    counts["ok"] += 1
                except pymysql.MySQLError as e:
                    code = e.args[0]
                    try:
                        q(s, "ROLLBACK")
                    except pymysql.MySQLError:
                        pass
                    if code == 1062:
                        counts["dup"] += 1
                    elif code == 1213:
                        counts["conflict"] += 1
                    else:
                        errors.append(e.args)

        ths = [threading.Thread(target=worker, args=(w,)) for w in range(8)]
        for t in ths:
            t.start()
        for t in ths:
            t.join()
        print(f"    global unique: {counts}")
        self.check(not errors, f"no other errors ({errors[:3]})")
        dups = q(c, "SELECT h, COUNT(*) FROM gx WHERE h IS NOT NULL GROUP BY h HAVING COUNT(*) > 1")
        self.check(not dups, f"no value held twice: {dups}")
        # every value is found through the index by exactly its row
        rows = q(c, "SELECT id, h FROM gx WHERE h IS NOT NULL")
        for k, h in rows:
            q(c, "INSERT INTO gx VALUES (1000, %s) ON DUPLICATE KEY UPDATE id = id", (h,))
            self.check(not q(c, "SELECT id FROM gx WHERE id = 1000"), f"the index finds the holder of {h}")
        free = {f"v{i}" for i in range(12)} - {h for _, h in rows}
        for h in free:
            q(c, "INSERT INTO gx VALUES (1000, %s)", (h,))
            q(c, "DELETE FROM gx WHERE id = 1000")

    def test_select_for_update(self):
        # FOR UPDATE takes a row lock (as in InnoDB): a concurrent writer
        # waits for the transaction instead of making it abort.
        self.fresh("sfu", "CREATE TABLE sfu (id INT PRIMARY KEY, v INT)", ["(1, 0)"])
        a, b = connect(self.args.tarn), connect(self.args.tarn)
        q(a, "BEGIN")
        q(a, "SELECT v FROM sfu WHERE id = 1 FOR UPDATE")
        res = {}
        th = threading.Thread(target=lambda: res.update(n=q(b, "UPDATE sfu SET v = v + 10 WHERE id = 1")))
        th.start()
        time.sleep(0.3)
        self.check(th.is_alive(), "a concurrent write waits for the row lock")
        q(a, "UPDATE sfu SET v = v + 1 WHERE id = 1")
        aborted = expect_conflict(lambda: q(a, "COMMIT"))
        th.join(30)
        v = q(b, "SELECT v FROM sfu WHERE id = 1")[0][0]
        self.check(not aborted and res.get("n") == 1 and v == 11, f"... and applies after the commit (aborted={aborted} v={v})")

    # ------------------------------------------------------------------
    def test_bank(self):
        """Concurrent transfers with serializable audits. Every transfer also
        inserts a ledger row (same transaction), so the final balances must
        equal what the ledger says: a transfer whose writes were lost on one
        shard, or applied twice, shows up there. Clients classify each COMMIT
        as acknowledged (must be in the ledger), aborted with 1213 (must not
        be), or ambiguous (lost connection / 1180: either). Autocommit
        increments of counters check that no single-row write is applied
        twice (ok <= n <= ok + ambiguous)."""
        n_acc, init, n_ctr = 64, 1000, 16
        total = n_acc * init
        self.fresh("bank", "CREATE TABLE bank (id INT PRIMARY KEY, bal BIGINT NOT NULL)",
                   [f"({i}, {init})" for i in range(n_acc)])
        self.fresh("ledger", "CREATE TABLE ledger (id BIGINT PRIMARY KEY, x INT NOT NULL, y INT NOT NULL, "
                   "amt INT NOT NULL)", [])
        self.fresh("counters", "CREATE TABLE counters (id INT PRIMARY KEY, n BIGINT NOT NULL)",
                   [f"({i}, 0)" for i in range(n_ctr)])
        stop = time.time() + self.args.bank_seconds
        stats = {"commit": 0, "conflict": 0, "audit": 0, "bad": [], "err": [], "times": [],
                 "acked": set(), "aborted": set(), "ambiguous": set(),
                 "ctr_ok": [0] * n_ctr, "ctr_amb": [0] * n_ctr, "ctr_err": 0}
        t_start = time.time()
        lock = threading.Lock()

        def transfer(seed):
            rnd = random.Random(seed)
            c = connect(self.args.tarn)
            seq = 0
            while time.time() < stop:
                x, y = rnd.sample(range(n_acc), 2)
                amt = rnd.randint(1, 50)
                seq += 1
                lid = seed * 1_000_000 + seq
                stage = "body"
                try:
                    q(c, "BEGIN")
                    bx = q(c, "SELECT bal FROM bank WHERE id = %s", (x,))[0][0]
                    if bx < amt:
                        q(c, "ROLLBACK")
                        continue
                    q(c, "UPDATE bank SET bal = bal - %s WHERE id = %s", (amt, x))
                    q(c, "UPDATE bank SET bal = bal + %s WHERE id = %s", (amt, y))
                    q(c, "INSERT INTO ledger VALUES (%s, %s, %s, %s)", (lid, x, y, amt))
                    stage = "commit"
                    q(c, "COMMIT")
                    with lock:
                        stats["commit"] += 1
                        stats["acked"].add(lid)
                        stats["times"].append(time.time() - t_start)
                except pymysql.MySQLError as e:
                    code = e.args[0] if e.args else 0
                    with lock:
                        if code == CONFLICT:
                            stats["conflict"] += 1
                            if stage == "commit":
                                stats["aborted"].add(lid)
                        else:
                            if stage == "commit":
                                stats["ambiguous"].add(lid)
                            stats["err"].append(str(e))
                    try:
                        q(c, "ROLLBACK")
                    except pymysql.MySQLError:
                        c = connect(self.args.tarn)
                    if code != CONFLICT:
                        time.sleep(0.05)

        def counter(seed):
            rnd = random.Random(1000 + seed)
            c = connect(self.args.tarn)
            while time.time() < stop:
                i = rnd.randrange(n_ctr)
                try:
                    q(c, "UPDATE counters SET n = n + 1 WHERE id = %s", (i,))
                    with lock:
                        stats["ctr_ok"][i] += 1
                except pymysql.MySQLError as e:
                    code = e.args[0] if e.args else 0
                    with lock:
                        if code == CONFLICT:
                            pass   # not applied
                        else:
                            stats["ctr_amb"][i] += 1
                            stats["ctr_err"] += 1
                    try:
                        c.ping()
                    except pymysql.MySQLError:
                        time.sleep(0.05)
                        try:
                            c = connect(self.args.tarn)
                        except pymysql.MySQLError:
                            pass
                    time.sleep(0.02)

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
                # autocommit reads must also be sane
                try:
                    s2 = q(c, "SELECT SUM(bal) FROM bank")[0][0]
                    if int(s2) != total:
                        with lock:
                            stats["bad"].append(("autocommit", s2))
                except pymysql.MySQLError:
                    pass

        th = [threading.Thread(target=transfer, args=(i,)) for i in range(self.args.threads)]
        th += [threading.Thread(target=counter, args=(i,)) for i in range(2)]
        th += [threading.Thread(target=audit) for _ in range(2)]
        for t in th:
            t.start()
        for t in th:
            t.join()
        # Recovery may still be finishing transactions whose APPLY was cut
        # off: wait until the ledger and the balances agree, or time out.
        c = connect(self.args.tarn)
        deadline = time.time() + 30
        while True:
            try:
                bal = dict(q(c, "SELECT id, bal FROM bank"))
                ledger = q(c, "SELECT id, x, y, amt FROM ledger")
                ctrs = dict(q(c, "SELECT id, n FROM counters"))
            except pymysql.MySQLError as e:
                if time.time() > deadline:
                    raise
                time.sleep(0.5)
                c = connect(self.args.tarn)
                continue
            want = {i: init for i in range(n_acc)}
            for _, x, y, amt in ledger:
                want[x] -= amt
                want[y] += amt
            if want == bal or time.time() > deadline:
                break
            time.sleep(0.5)
        in_ledger = {r[0] for r in ledger}
        print(f"    bank: {stats['commit']} commits, {stats['conflict']} conflicts, {stats['audit']} audits, "
              f"{len(stats['err'])} other errors ({len(stats['ambiguous'])} commits with unknown outcome, "
              f"{sum(1 for i in stats['ambiguous'] if i in in_ledger)} of them committed), "
              f"final sum {sum(bal.values())}")
        print(f"    counters: {sum(stats['ctr_ok'])} acknowledged increments, {stats['ctr_err']} with unknown outcome")
        for e in stats["err"][:5]:
            print(f"      error: {e}")
        buckets = [0] * (int(self.args.bank_seconds) // 3 + 1)
        for t_ in stats["times"]:
            buckets[min(int(t_ // 3), len(buckets) - 1)] += 1
        print("    commits per 3 s:", " ".join(str(b) for b in buckets))
        self.check(not stats["bad"], f"audits saw a broken invariant: {stats['bad'][:5]}")
        self.check(sum(bal.values()) == total and len(bal) == n_acc, "final balance sum")
        diff = {i: (bal.get(i), want[i]) for i in want if bal.get(i) != want[i]}
        self.check(not diff, f"balances match the ledger (lost or duplicated transfer writes): {list(diff.items())[:5]}")
        lost = stats["acked"] - in_ledger
        self.check(not lost, f"every acknowledged COMMIT is in the ledger: {len(lost)} missing")
        phantom = stats["aborted"] & in_ledger
        self.check(not phantom, f"no COMMIT reported as 1213 committed: {sorted(phantom)[:5]}")
        bad_ctr = [(i, ctrs.get(i), stats["ctr_ok"][i], stats["ctr_amb"][i]) for i in range(n_ctr)
                   if not (stats["ctr_ok"][i] <= ctrs.get(i, -1) <= stats["ctr_ok"][i] + stats["ctr_amb"][i])]
        self.check(not bad_ctr, f"autocommit increments applied exactly once (id, n, ok, unknown): {bad_ctr[:5]}")
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
