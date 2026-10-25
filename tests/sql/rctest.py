#!/usr/bin/env python3
"""READ COMMITTED tests for Tarn (issue #17): isolation variables, row locks
with waits, lock wait timeouts and deadlocks, semi-consistent writes, SKIP
LOCKED / NOWAIT, unique keys locked by value, savepoints and statement
rollback -- each checked against what InnoDB does at READ COMMITTED.

    tests/sql/rctest.py --tarn 127.0.0.1:4000[,127.0.0.1:4001...] [--only NAME] [--threads 8]

Needs a cluster with at least two shards (scripts/cluster.sh start -n 2).
The tests connect to the first server; list every SQL server of the cluster,
since any of them may break a cross-shard deadlock (and count it).
"""
import argparse
import random
import sys
import threading
import time

import pymysql

DB = "rctest"
DEADLOCK, LOCK_TIMEOUT, NOWAIT, DUP = 1213, 1205, 3572, 1062


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


def err_code(fn):
    """Run fn; return the MySQL error code it raised, or None."""
    try:
        fn()
    except pymysql.MySQLError as e:
        return e.args[0]
    return None


class Bg:
    """Run fn in a thread (it may block on a lock)."""

    def __init__(self, fn):
        self.res = self.exc = self.elapsed = None
        self.t0 = time.time()

        def run():
            try:
                self.res = fn()
            except Exception as e:  # noqa: BLE001
                self.exc = e
            self.elapsed = time.time() - self.t0

        self.th = threading.Thread(target=run, daemon=True)
        self.th.start()

    def running(self):
        return self.th.is_alive()

    def join(self, timeout=60):
        self.th.join(timeout)
        return self

    def code(self):
        return self.exc.args[0] if isinstance(self.exc, pymysql.MySQLError) else None


# Row placement (src/route.rs): CRC-32C over the shard key's msgpack bytes,
# initial value 0xFFFFFFFF and no final xor; bucket = crc % 3000 + 1.
def _crc32c(data, crc=0xFFFFFFFF):
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 & -(crc & 1))
    return crc


def _mp_uint(v):
    if v < 128:
        return bytes([v])
    if v < 256:
        return bytes([0xCC, v])
    if v < 65536:
        return b"\xcd" + v.to_bytes(2, "big")
    return b"\xce" + v.to_bytes(4, "big")


class T:
    def __init__(self, args):
        self.args = args
        self.failures = 0
        c = connect(args.tarn)
        self.nshards = int(dict(q(c, "SHOW STATUS LIKE 'Tarn_shards'"))["Tarn_shards"])

    def check(self, cond, what):
        if not cond:
            self.failures += 1
            print(f"    FAILED: {what}")
        return cond

    def shard(self, k):
        b = _crc32c(_mp_uint(k)) % 3000 + 1
        return (b - 1) * self.nshards // 3000

    def two_shards(self, ids):
        """Two ids from `ids` that live on different shards."""
        first = ids[0]
        for k in ids[1:]:
            if self.shard(k) != self.shard(first):
                return first, k
        raise RuntimeError("no two ids on different shards")

    def rc(self, autocommit=True, hp=None):
        c = connect(hp or self.args.tarn, autocommit=autocommit)
        q(c, "SET SESSION transaction_isolation = 'READ-COMMITTED'")
        return c

    def fresh(self, name, ddl, rows):
        c = connect(self.args.tarn)
        q(c, f"DROP TABLE IF EXISTS {name}")
        q(c, ddl)
        if rows:
            q(c, f"INSERT INTO {name} VALUES " + ", ".join(rows))
        return c

    def status(self, name):
        """A status counter summed over the cluster's SQL servers."""
        return sum(int(dict(q(connect(hp), f"SHOW STATUS LIKE '{name}'"))[name])
                   for hp in self.args.servers)

    # ------------------------------------------------------------------
    def test_00_shard_helper(self):
        # The placement helper agrees with the server: a transaction over two
        # rows it puts on one shard commits locally, over two on different
        # shards with 2PC.
        c = self.fresh("sh", "CREATE TABLE sh (id INT PRIMARY KEY, v INT)", [f"({i}, 0)" for i in range(1, 41)])
        same = [k for k in range(2, 41) if self.shard(k) == self.shard(1)][0]
        a, b = self.two_shards(list(range(1, 41)))
        for (x, y), counter in (((1, same), "Tarn_commits_local"), ((a, b), "Tarn_commits_2pc")):
            before = self.status(counter)
            q(c, "BEGIN")
            q(c, f"UPDATE sh SET v = v + 1 WHERE id IN ({x}, {y})")
            q(c, "COMMIT")
            self.check(self.status(counter) == before + 1, f"rows {x},{y} commit via {counter}")

    def test_isolation_variables(self):
        c = connect(self.args.tarn)
        self.check(q(c, "SELECT @@transaction_isolation")[0][0] == self.args.default_isolation,
                   "default isolation level")
        q(c, "SET SESSION transaction_isolation = 'READ-COMMITTED'")
        self.check(q(c, "SELECT @@transaction_isolation, @@session.tx_isolation, @@SESSION.transaction_isolation")
                   == (("READ-COMMITTED",) * 3,), "SET SESSION transaction_isolation")
        q(c, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        self.check(q(c, "SELECT @@transaction_isolation")[0][0] == "SERIALIZABLE", "SET SESSION TRANSACTION")
        q(c, "SET tx_isolation = 'read-committed'")
        self.check(q(c, "SELECT @@tx_isolation")[0][0] == "READ-COMMITTED", "tx_isolation, any case")
        self.check(q(c, "SHOW VARIABLES LIKE 'transaction_isolation'") == (("transaction_isolation", "READ-COMMITTED"),),
                   "SHOW VARIABLES")
        self.check(err_code(lambda: q(c, "SET transaction_isolation = 'SNAPSHOT'")) == 1231, "bad value: 1231")
        q(c, "SET innodb_lock_wait_timeout = 7")
        self.check(q(c, "SELECT @@innodb_lock_wait_timeout")[0][0] == 7, "innodb_lock_wait_timeout")
        q(c, "SET innodb_lock_wait_timeout = -5")
        self.check(q(c, "SELECT @@innodb_lock_wait_timeout")[0][0] == 0, "negative timeouts clamp to 0 (MariaDB)")
        self.check(err_code(lambda: q(c, "SET innodb_lock_wait_timeout = 1.5")) == 1232, "non-integer timeout: 1232")
        q(c, "SET innodb_lock_wait_timeout = DEFAULT")
        self.check(q(c, "SELECT @@innodb_lock_wait_timeout")[0][0] == 50, "DEFAULT: the global value")
        q(c, "BEGIN")
        self.check(err_code(lambda: q(c, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")) == 1568,
                   "SET TRANSACTION inside a transaction: 1568")
        q(c, "COMMIT")

        # SET TRANSACTION (no scope) applies to the next transaction only.
        self.fresh("iv", "CREATE TABLE iv (id INT PRIMARY KEY, v INT)", ["(1, 0)", "(2, 0)"])
        other = connect(self.args.tarn)

        def stale_read_then_write():
            q(c, "BEGIN")
            q(c, "SELECT v FROM iv WHERE id = 1")
            q(other, "UPDATE iv SET v = v + 1 WHERE id = 1")
            q(c, "UPDATE iv SET v = v + 1 WHERE id = 2")
            return err_code(lambda: q(c, "COMMIT"))

        for setter in ("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                       "SET @@transaction_isolation = 'SERIALIZABLE'"):
            q(c, setter)
            self.check(q(c, "SELECT @@transaction_isolation")[0][0] == "READ-COMMITTED",
                       f"{setter}: session level unchanged")
            self.check(stale_read_then_write() == DEADLOCK, f"{setter}: next transaction is serializable")
            self.check(stale_read_then_write() is None, f"{setter}: the one after is READ COMMITTED again")

    def test_global_isolation(self):
        c = connect(self.args.tarn)
        try:
            q(c, "SET GLOBAL transaction_isolation = 'READ-COMMITTED'")
            q(c, "SET GLOBAL innodb_lock_wait_timeout = 9")
            n = connect(self.args.tarn)
            self.check(q(n, "SELECT @@transaction_isolation, @@innodb_lock_wait_timeout") == (("READ-COMMITTED", 9),),
                       "new sessions take the global values")
            self.check(q(n, "SELECT @@global.transaction_isolation")[0][0] == "READ-COMMITTED", "@@global")
            self.check(q(c, "SELECT @@transaction_isolation")[0][0] == self.args.default_isolation,
                       "existing sessions keep theirs")
        finally:
            q(c, f"SET GLOBAL transaction_isolation = '{self.args.default_isolation}'")
            q(c, "SET GLOBAL innodb_lock_wait_timeout = 50")

    def test_plain_reads_never_abort(self):
        c = self.fresh("pr", "CREATE TABLE pr (id INT PRIMARY KEY, v INT)", [f"({i}, 0)" for i in range(1, 11)])
        a, other = self.rc(), connect(self.args.tarn)
        q(a, "BEGIN")
        self.check(q(a, "SELECT SUM(v) FROM pr")[0][0] == 0, "first read")
        q(other, "UPDATE pr SET v = 5 WHERE id = 3")
        self.check(q(a, "SELECT v FROM pr WHERE id = 3")[0][0] == 5, "each statement sees the latest commit")
        q(a, "UPDATE pr SET v = v + 1 WHERE id = 4")
        self.check(q(a, "SELECT SUM(v) FROM pr")[0][0] == 6, "... plus the transaction's own writes")
        q(other, "UPDATE pr SET v = 7 WHERE id = 5")
        self.check(err_code(lambda: q(a, "COMMIT")) is None, "commit after concurrent changes to what it read")
        self.check(q(c, "SELECT SUM(v) FROM pr")[0][0] == 13, "all three writes kept")
        # a row locked by an uncommitted writer is read at its committed value, without waiting
        q(other, "BEGIN")
        q(other, "UPDATE pr SET v = 100 WHERE id = 1")
        t0 = time.time()
        self.check(q(a, "SELECT v FROM pr WHERE id = 1")[0][0] == 0, "uncommitted writes are invisible")
        self.check(q(connect(self.args.tarn), "SELECT v FROM pr WHERE id = 1")[0][0] == 0,
                   "... to serializable readers too")
        self.check(time.time() - t0 < 1, "plain reads do not wait for row locks")
        q(other, "ROLLBACK")

    def test_counter_concurrent(self):
        n = 6
        self.fresh("ctr", "CREATE TABLE ctr (id INT PRIMARY KEY, n BIGINT)", [f"({i}, 10000)" for i in range(1, n + 1)])
        a, b = self.two_shards(list(range(1, n + 1)))
        errors, done = [], [0]
        lock = threading.Lock()
        per = self.args.rounds

        def worker(w):
            c = self.rc()
            for i in range(per):
                try:
                    if i % 3 == 0:
                        q(c, "UPDATE ctr SET n = n - 1 WHERE id = 1")  # autocommit
                    else:
                        q(c, "BEGIN")
                        q(c, "UPDATE ctr SET n = n - 1 WHERE id = 1")
                        if i % 3 == 2:  # cross-shard: both rows in one transaction
                            q(c, f"UPDATE ctr SET n = n - 1 WHERE id IN ({a}, {b})")
                        q(c, "COMMIT")
                    with lock:
                        done[0] += 1
                except pymysql.MySQLError as e:
                    errors.append(e.args)
                    try:
                        q(c, "ROLLBACK")
                    except pymysql.MySQLError:
                        pass

        ths = [threading.Thread(target=worker, args=(w,)) for w in range(self.args.threads)]
        for t in ths:
            t.start()
        for t in ths:
            t.join()
        c = connect(self.args.tarn)
        total = self.args.threads * per
        cross = self.args.threads * len([i for i in range(per) if i % 3 == 2])
        rows = dict(q(c, "SELECT id, n FROM ctr"))
        self.check(not errors, f"no errors at READ COMMITTED (got {errors[:3]})")
        exp = {k: 10000 for k in rows}
        exp[1] -= total
        exp[a] -= cross
        exp[b] -= cross
        self.check(rows == exp, f"no lost updates: {rows}, expected {exp}")

    def test_lock_wait_then_proceed(self):
        c = self.fresh("lw", "CREATE TABLE lw (id INT PRIMARY KEY, v INT)", ["(1, 0)", "(2, 0)"])
        a, b = self.rc(), self.rc()
        q(a, "BEGIN")
        q(a, "UPDATE lw SET v = v + 1 WHERE id = 1")
        q(b, "BEGIN")
        bg = Bg(lambda: q(b, "UPDATE lw SET v = v + 10 WHERE id = 1"))
        time.sleep(0.5)
        self.check(bg.running(), "a second writer waits for the row lock")
        q(a, "COMMIT")
        bg.join()
        self.check(bg.exc is None and bg.res == 1, f"... and proceeds once it is released ({bg.exc})")
        self.check(q(b, "SELECT v FROM lw WHERE id = 1")[0][0] == 11, "it updated the latest version")
        q(b, "COMMIT")
        self.check(q(c, "SELECT v FROM lw WHERE id = 1")[0][0] == 11, "no lost update")
        # autocommit writers wait too
        q(a, "BEGIN")
        q(a, "UPDATE lw SET v = 0 WHERE id = 2")
        bg = Bg(lambda: q(connect(self.args.tarn), "UPDATE lw SET v = v + 5 WHERE id = 2"))
        time.sleep(0.5)
        self.check(bg.running(), "an autocommit writer waits for the row lock")
        q(a, "ROLLBACK")
        bg.join()
        self.check(bg.exc is None and q(c, "SELECT v FROM lw WHERE id = 2")[0][0] == 5,
                   f"... and applies to the latest version ({bg.exc})")

    def test_lock_wait_timeout(self):
        c = self.fresh("lt", "CREATE TABLE lt (id INT PRIMARY KEY, v INT)", ["(1, 0)", "(2, 0)"])
        a, b = self.rc(), self.rc()
        q(b, "SET innodb_lock_wait_timeout = 1")
        q(a, "BEGIN")
        q(a, "UPDATE lt SET v = 1 WHERE id = 1")
        q(b, "BEGIN")
        q(b, "UPDATE lt SET v = 2 WHERE id = 2")
        t0 = time.time()
        code = err_code(lambda: q(b, "UPDATE lt SET v = 2 WHERE id = 1"))
        el = time.time() - t0
        self.check(code == LOCK_TIMEOUT, f"1205 after the lock wait timeout (got {code})")
        self.check(0.8 < el < 5, f"... after about a second ({el:.1f}s)")
        self.check(err_code(lambda: q(b, "SELECT 1 FROM lt FOR UPDATE NOWAIT")) == NOWAIT, "NOWAIT: 3572")
        self.check(q(b, "SELECT v FROM lt WHERE id = 2")[0][0] == 2, "the transaction survives both")
        q(b, "COMMIT")
        q(a, "COMMIT")
        self.check(q(c, "SELECT id, v FROM lt ORDER BY id") == ((1, 1), (2, 2)), "both transactions' writes")
        # autocommit statements time out too
        q(a, "BEGIN")
        q(a, "UPDATE lt SET v = 3 WHERE id = 1")
        x = connect(self.args.tarn)
        q(x, "SET innodb_lock_wait_timeout = 1")
        self.check(err_code(lambda: q(x, "UPDATE lt SET v = v + 1 WHERE id = 1")) == LOCK_TIMEOUT,
                   "an autocommit write times out with 1205")
        self.check(err_code(lambda: q(x, "DELETE FROM lt WHERE id = 1")) == LOCK_TIMEOUT, "... as does a delete")
        q(x, "SET innodb_lock_wait_timeout = 0")
        t0 = time.time()
        self.check(err_code(lambda: q(x, "UPDATE lt SET v = v + 1 WHERE id = 1")) == LOCK_TIMEOUT
                   and time.time() - t0 < 0.5, "a zero timeout fails at once")
        q(a, "ROLLBACK")

    def deadlock(self, x, y, table, servers=(None, None)):
        a, b = self.rc(hp=servers[0]), self.rc(hp=servers[1])
        q(a, "SET innodb_lock_wait_timeout = 30")
        q(b, "SET innodb_lock_wait_timeout = 30")
        q(a, "BEGIN")
        q(b, "BEGIN")
        q(a, f"UPDATE {table} SET v = v + 1 WHERE id = {x}")
        q(b, f"UPDATE {table} SET v = v + 10 WHERE id = {y}")
        ga = Bg(lambda: q(a, f"UPDATE {table} SET v = v + 1 WHERE id = {y}"))
        time.sleep(0.3)
        gb = Bg(lambda: q(b, f"UPDATE {table} SET v = v + 10 WHERE id = {x}"))
        ga.join(40)
        gb.join(40)
        codes = sorted([ga.code() or 0, gb.code() or 0])
        self.check(codes == [0, DEADLOCK], f"exactly one deadlock victim (got {ga.exc}, {gb.exc})")
        slowest = max(ga.elapsed or 99, gb.elapsed or 99)
        winner, loser = (a, b) if gb.code() == DEADLOCK else (b, a)
        self.check(err_code(lambda: q(winner, "COMMIT")) is None, "the survivor commits")
        # the victim's whole transaction was rolled back (as in InnoDB)
        self.check(q(loser, "SELECT @@autocommit")[0][0] == 1, "victim's session is usable")
        rows = dict(q(connect(self.args.tarn), f"SELECT id, v FROM {table} WHERE id IN ({x}, {y})"))
        exp = {x: 1, y: 1} if winner is a else {x: 10, y: 10}
        self.check(rows == exp, f"only the survivor's writes ({rows}, expected {exp})")
        return slowest

    def test_deadlock_same_shard(self):
        self.fresh("dl", "CREATE TABLE dl (g INT, id INT, v INT, PRIMARY KEY (g, id)) PARTITION BY KEY(g)", [])
        # one shard key value: both rows on one shard, so the shard sees the cycle
        q(connect(self.args.tarn), "INSERT INTO dl VALUES (1, 1, 0), (1, 2, 0)")
        self.fresh("dl1", "CREATE TABLE dl1 (id INT PRIMARY KEY, v INT)", ["(1, 0)"])
        before = self.status("Tarn_cross_shard_deadlocks")
        a, b = self.rc(), self.rc()
        for s in (a, b):
            q(s, "SET innodb_lock_wait_timeout = 30")
            q(s, "BEGIN")
        q(a, "UPDATE dl SET v = v + 1 WHERE g = 1 AND id = 1")
        q(b, "UPDATE dl SET v = v + 10 WHERE g = 1 AND id = 2")
        ga = Bg(lambda: q(a, "UPDATE dl SET v = v + 1 WHERE g = 1 AND id = 2"))
        time.sleep(0.3)
        t0 = time.time()
        code = err_code(lambda: q(b, "UPDATE dl SET v = v + 10 WHERE g = 1 AND id = 1"))
        self.check(code == DEADLOCK and time.time() - t0 < 1, f"the request closing the cycle fails at once ({code})")
        ga.join()
        self.check(ga.exc is None, f"the other transaction proceeds ({ga.exc})")
        q(a, "COMMIT")
        self.check(q(b, "SELECT COUNT(*) FROM dl1")[0][0] == 1, "victim's session is usable")
        self.check(q(connect(self.args.tarn), "SELECT id, v FROM dl ORDER BY id") == ((1, 1), (2, 1)),
                   "only the survivor's writes")
        self.check(self.status("Tarn_cross_shard_deadlocks") == before, "found by the shard itself")

    def test_deadlock_cross_shard(self):
        x, y = self.two_shards(list(range(1, 30)))
        self.fresh("dx", "CREATE TABLE dx (id INT PRIMARY KEY, v INT)", [f"({x}, 0)", f"({y}, 0)"])
        before = self.status("Tarn_cross_shard_deadlocks")
        slowest = self.deadlock(x, y, "dx")
        self.check(slowest < 10, f"broken by the cross-shard detector, not the 30 s timeout ({slowest:.1f}s)")
        self.check(self.status("Tarn_cross_shard_deadlocks") == before + 1, "counted in Tarn_cross_shard_deadlocks")

    def test_deadlock_cross_shard_servers(self):
        # Each SQL server starts its search from its own waiting transactions
        # and learns where the others' hold locks from their lock requests
        # (issue #26): cycles whose transactions are on different servers.
        if len(self.args.servers) < 2:
            print("    (one SQL server listed: skipped)")
            return
        s1, s2 = self.args.servers[0], self.args.servers[1]
        x, y = self.two_shards(list(range(1, 30)))
        self.fresh("dv", "CREATE TABLE dv (id INT PRIMARY KEY, v INT)", [f"({x}, 0)", f"({y}, 0)"])
        before = self.status("Tarn_cross_shard_deadlocks")
        slowest = self.deadlock(x, y, "dv", (s1, s2))
        self.check(slowest < 10, f"two servers: broken by the detector ({slowest:.1f}s)")
        self.check(self.status("Tarn_cross_shard_deadlocks") == before + 1, "two servers: counted once")
        # Three transactions on alternating servers, each holding one row and
        # asking for the next: rows 0 and 2 on one shard, row 1 on the other,
        # so no shard sees the whole cycle.
        x0, x1 = self.two_shards(list(range(1, 60)))
        x2 = [k for k in range(1, 60) if self.shard(k) == self.shard(x0) and k != x0][0]
        rows = [x0, x1, x2]
        self.fresh("d3", "CREATE TABLE d3 (id INT PRIMARY KEY, v INT)", [f"({k}, 0)" for k in rows])
        before = self.status("Tarn_cross_shard_deadlocks")
        ss = [self.rc(hp=self.args.servers[i % 2]) for i in range(3)]
        for i, s in enumerate(ss):
            q(s, "SET innodb_lock_wait_timeout = 30")
            q(s, "BEGIN")
            q(s, f"UPDATE d3 SET v = v + 1 WHERE id = {rows[i]}")
        def ask_then_commit(s, i):
            # Once the victim is gone, the others form a chain: each commits
            # at once so that the next one gets its row.
            q(s, f"UPDATE d3 SET v = v + 1 WHERE id = {rows[(i + 1) % 3]}")
            q(s, "COMMIT")

        gs = []
        for i, s in enumerate(ss):
            gs.append(Bg(lambda s=s, i=i: ask_then_commit(s, i)))
            time.sleep(0.2)
        for g in gs:
            g.join(40)
        codes = [g.code() or 0 for g in gs]
        self.check(sorted(codes) == [0, 0, DEADLOCK], f"three servers' cycle: exactly one victim ({codes})")
        slowest = max(g.elapsed or 99 for g in gs)
        self.check(slowest < 10, f"three transactions: broken by the detector ({slowest:.1f}s)")
        exp = {k: 0 for k in rows}
        for i in range(3):
            if codes[i] == 0:
                exp[rows[i]] += 1
                exp[rows[(i + 1) % 3]] += 1
        got = dict(q(connect(self.args.tarn), "SELECT id, v FROM d3"))
        self.check(got == exp, f"only the survivors' writes ({got}, expected {exp})")
        self.check(self.status("Tarn_cross_shard_deadlocks") == before + 1, "three transactions: counted once")

    def test_deadlock_detector_idle(self):
        # Without lock waits, no SQL server calls the shards to look for
        # deadlocks (issue #26: it used to call every shard every second).
        time.sleep(0.2)
        before = self.status("Tarn_deadlock_detector_calls")
        time.sleep(2.5)
        self.check(self.status("Tarn_deadlock_detector_calls") == before, "no detector calls without lock waits")

    def test_failed_lock_request_releases(self):
        # A lock request that is granted one row and then times out on the
        # next keeps the first lock until the transaction ends (as InnoDB);
        # it used to keep it for good when the request was the transaction's
        # first on that shard.
        x = 1
        y = [k for k in range(2, 40) if self.shard(k) == self.shard(x)][0]
        self.fresh("fl", "CREATE TABLE fl (id INT PRIMARY KEY, v INT)", [f"({x}, 0)", f"({y}, 0)"])
        h, t, u = self.rc(), self.rc(), self.rc()
        q(u, "SET innodb_lock_wait_timeout = 2")
        for ending in ("COMMIT", "ROLLBACK", "autocommit"):
            q(h, "BEGIN")
            q(h, f"UPDATE fl SET v = v + 1 WHERE id = {y}")
            q(t, "SET innodb_lock_wait_timeout = 1")
            if ending != "autocommit":
                q(t, "BEGIN")
            code = err_code(lambda: q(t, f"SELECT id FROM fl WHERE id IN ({x}, {y}) ORDER BY id FOR UPDATE"))
            self.check(code == LOCK_TIMEOUT, f"{ending}: 1205 on the second row ({code})")
            if ending != "autocommit":
                self.check(err_code(lambda: q(u, f"SELECT id FROM fl WHERE id = {x} FOR UPDATE NOWAIT")) == NOWAIT,
                           f"{ending}: the first row stays locked while the transaction lasts")
                q(t, ending)
            q(h, "ROLLBACK")
            q(u, "BEGIN")
            code = err_code(lambda: q(u, f"UPDATE fl SET v = v + 10 WHERE id = {x}"))
            self.check(code is None, f"{ending}: the lock is released with the transaction ({code})")
            q(u, "ROLLBACK")

    def test_skip_locked(self):
        c = self.fresh("jobs", "CREATE TABLE jobs (id INT PRIMARY KEY, state VARCHAR(10), worker INT)",
                       [f"({i}, 'new', NULL)" for i in range(1, 21)])
        a, b, d = self.rc(), self.rc(), self.rc()
        sql = "SELECT id FROM jobs WHERE state = 'new' ORDER BY id LIMIT 3 FOR UPDATE SKIP LOCKED"
        q(a, "BEGIN")
        q(b, "BEGIN")
        ra = [r[0] for r in q(a, sql)]
        rb = [r[0] for r in q(b, sql)]
        self.check(ra == [1, 2, 3], f"first worker takes the first three ({ra})")
        self.check(rb == [4, 5, 6], f"second worker skips them and still gets three ({rb})")
        q(d, "BEGIN")
        self.check(err_code(lambda: q(d, "SELECT id FROM jobs WHERE id = 2 FOR UPDATE NOWAIT")) == NOWAIT,
                   "NOWAIT on a held row: 3572")
        self.check([r[0] for r in q(d, "SELECT id FROM jobs WHERE id <= 4 FOR UPDATE SKIP LOCKED")] == [],
                   "SKIP LOCKED with every match held: empty")
        self.check([r[0] for r in q(d, "SELECT id FROM jobs WHERE id IN (6, 7) FOR SHARE SKIP LOCKED")] == [7],
                   "FOR SHARE SKIP LOCKED skips X-locked rows")
        q(d, "ROLLBACK")
        q(a, f"UPDATE jobs SET state = 'taken', worker = 1 WHERE id IN ({','.join(map(str, ra))})")
        q(a, "COMMIT")
        rc2 = [r[0] for r in q(b, sql)]
        self.check(rc2 == [4, 5, 6], f"a worker's own locks do not skip ({rc2})")
        q(b, "ROLLBACK")

        # SKIP LOCKED far down a queue: the scan goes on page by page
        q(c, "DROP TABLE IF EXISTS q2")
        q(c, "CREATE TABLE q2 (g INT, id INT, state VARCHAR(5), PRIMARY KEY (g, id)) PARTITION BY KEY(g)")
        for lo in range(0, 3000, 500):
            q(c, "INSERT INTO q2 VALUES " + ",".join(f"(1, {i}, '{'odd' if i % 2 else 'even'}')"
                                                   for i in range(lo, lo + 500)))
        q(a, "BEGIN")
        self.check(len(q(a, "SELECT id FROM q2 WHERE g = 1 AND id < 1700 FOR UPDATE")) == 1700, "lock 1700 rows")
        q(b, "BEGIN")
        got = [r[0] for r in q(b, "SELECT id FROM q2 WHERE g = 1 ORDER BY id LIMIT 3 FOR UPDATE SKIP LOCKED")]
        self.check(got == [1700, 1701, 1702], f"SKIP LOCKED past 1700 held rows ({got})")
        got = [r[0] for r in q(b, "SELECT id FROM q2 WHERE g = 1 AND state = 'odd' ORDER BY id LIMIT 2 "
                                  "FOR UPDATE SKIP LOCKED")]
        self.check(got == [1701, 1703], f"... with a filter the index does not cover ({got})")
        got = [r[0] for r in q(b, "SELECT id FROM q2 WHERE g = 1 ORDER BY id LIMIT 4 FOR UPDATE SKIP LOCKED")]
        self.check(got == [1700, 1701, 1702, 1703], f"... its own locks do not skip ({got})")
        q(b, "DELETE FROM q2 WHERE g = 1 AND id IN (1700, 1702)")
        got = [r[0] for r in q(b, "SELECT id FROM q2 WHERE g = 1 ORDER BY id LIMIT 2 FOR UPDATE SKIP LOCKED")]
        self.check(got == [1701, 1703], f"... nor return rows it deleted ({got})")
        q(b, "ROLLBACK")
        q(a, "ROLLBACK")

        # reservation stress: every job is claimed exactly once, without errors
        q(c, "UPDATE jobs SET state = 'new', worker = NULL")
        claims, errors = [], []
        lk = threading.Lock()

        def worker(w):
            s = self.rc(autocommit=False)
            while True:
                try:
                    ids = [r[0] for r in q(s, "SELECT id FROM jobs WHERE state = 'new' ORDER BY id LIMIT 2 "
                                              "FOR UPDATE SKIP LOCKED")]
                    if not ids:
                        s.commit()
                        if not q(s, "SELECT 1 FROM jobs WHERE state = 'new' LIMIT 1"):
                            s.commit()
                            return
                        continue
                    time.sleep(0.01)
                    q(s, f"UPDATE jobs SET state = 'taken', worker = {w} WHERE id IN ({','.join(map(str, ids))})")
                    s.commit()
                    with lk:
                        claims.extend(ids)
                except pymysql.MySQLError as e:
                    errors.append(e.args)
                    s.rollback()

        ths = [threading.Thread(target=worker, args=(w,)) for w in range(self.args.threads)]
        for t in ths:
            t.start()
        for t in ths:
            t.join(120)
        self.check(not errors, f"no errors ({errors[:3]})")
        self.check(sorted(claims) == list(range(1, 21)), f"each job claimed exactly once ({sorted(claims)})")
        self.check(q(c, "SELECT COUNT(*) FROM jobs WHERE state = 'taken'")[0][0] == 20, "all taken")

    def test_for_share(self):
        self.fresh("fs", "CREATE TABLE fs (id INT PRIMARY KEY, v INT)", ["(1, 0)"])
        a, b, w = self.rc(), self.rc(), self.rc()
        q(a, "BEGIN")
        q(b, "BEGIN")
        self.check(q(a, "SELECT v FROM fs WHERE id = 1 FOR SHARE") == ((0,),), "FOR SHARE")
        self.check(q(b, "SELECT v FROM fs WHERE id = 1 LOCK IN SHARE MODE") == ((0,),), "LOCK IN SHARE MODE: shared")
        bg = Bg(lambda: q(w, "UPDATE fs SET v = 1 WHERE id = 1"))
        time.sleep(0.4)
        self.check(bg.running(), "a writer waits for both share locks")
        q(a, "COMMIT")
        time.sleep(0.2)
        self.check(bg.running(), "... still held by one")
        q(b, "COMMIT")
        bg.join()
        self.check(bg.exc is None, f"... and writes once both are released ({bg.exc})")

    def test_semi_consistent(self):
        c = self.fresh("sc", "CREATE TABLE sc (id INT PRIMARY KEY, v VARCHAR(5), w INT)",
                       ["(1, 'a', 0)", "(2, 'a', 0)", "(3, 'c', 0)"])
        a, b = self.rc(), self.rc()
        q(a, "BEGIN")
        q(a, "UPDATE sc SET v = 'b' WHERE id = 1")   # row 1 leaves the match
        q(a, "UPDATE sc SET v = 'a' WHERE id = 3")   # row 3 joins it (uncommitted)
        bg = Bg(lambda: q(b, "UPDATE sc SET w = 1 WHERE v = 'a'"))
        time.sleep(0.5)
        self.check(bg.running(), "the writer waits on row 1 (committed 'a' matches)")
        q(a, "COMMIT")
        bg.join()
        self.check(bg.exc is None and bg.res == 1, f"WHERE re-checked on the latest version: 1 row ({bg.res}, {bg.exc})")
        self.check(q(c, "SELECT id, v, w FROM sc ORDER BY id") == ((1, 'b', 0), (2, 'a', 1), (3, 'a', 0)),
                   "row 1 left out after the re-check; row 3 was not a candidate")
        # SET x = x - 1 sees the newest committed row under the lock
        q(a, "BEGIN")
        q(a, "UPDATE sc SET w = w + 5 WHERE id = 2")
        bg = Bg(lambda: q(b, "UPDATE sc SET w = w - 1 WHERE id = 2 AND w > 0"))
        time.sleep(0.3)
        q(a, "COMMIT")
        bg.join()
        self.check(q(c, "SELECT w FROM sc WHERE id = 2")[0][0] == 5, "w = 1 + 5 - 1")
        # DELETE ... WHERE re-checks too
        q(a, "BEGIN")
        q(a, "UPDATE sc SET v = 'z' WHERE id = 2")
        bg = Bg(lambda: q(b, "DELETE FROM sc WHERE v = 'a'"))
        time.sleep(0.3)
        q(a, "COMMIT")
        bg.join()
        self.check(bg.exc is None and bg.res == 1 and q(c, "SELECT id FROM sc ORDER BY id") == ((1,), (2,)),
                   f"DELETE re-checks its WHERE under the lock ({bg.res}, {bg.exc})")

    def test_duplicate_insert_waits(self):
        c = self.fresh("ui", "CREATE TABLE ui (k VARCHAR(10) PRIMARY KEY, n INT)", [])
        a, b = self.rc(), self.rc()
        q(a, "BEGIN")
        q(a, "INSERT INTO ui VALUES ('x', 1)")
        bg = Bg(lambda: q(b, "INSERT INTO ui VALUES ('x', 2)"))
        time.sleep(0.4)
        self.check(bg.running(), "a duplicate insert waits for the uncommitted insert")
        q(a, "ROLLBACK")
        bg.join()
        self.check(bg.exc is None, f"... and succeeds when it rolls back ({bg.exc})")
        q(a, "BEGIN")
        q(a, "INSERT INTO ui VALUES ('y', 1)")
        bg = Bg(lambda: q(b, "INSERT INTO ui VALUES ('y', 2)"))
        bg2 = Bg(lambda: q(self.rc(), "INSERT IGNORE INTO ui VALUES ('y', 3)"))
        time.sleep(0.4)
        q(a, "COMMIT")
        bg.join()
        bg2.join()
        self.check(bg.code() == DUP, f"... or fails with 1062 when it commits ({bg.exc})")
        self.check(bg2.exc is None and bg2.res == 0, f"INSERT IGNORE: no row ({bg2.res}, {bg2.exc})")
        self.check(q(c, "SELECT k, n FROM ui ORDER BY k") == (("x", 2), ("y", 1)), "final rows")

    def test_unique_update_waits(self):
        c = self.fresh("uu", "CREATE TABLE uu (g INT, id INT, name VARCHAR(10), PRIMARY KEY (g, id), "
                             "UNIQUE KEY gn (g, name)) PARTITION BY KEY(g)",
                       ["(1, 1, 'a')", "(1, 2, 'b')", "(1, 3, 'c')"])
        a, b = self.rc(), self.rc()
        q(a, "BEGIN")
        q(a, "UPDATE uu SET name = 'z' WHERE g = 1 AND id = 1")
        bg = Bg(lambda: q(b, "UPDATE uu SET name = 'z' WHERE g = 1 AND id = 2"))
        time.sleep(0.4)
        self.check(bg.running(), "an UPDATE to a unique value another transaction just took waits")
        q(a, "ROLLBACK")
        bg.join()
        self.check(bg.exc is None and bg.res == 1, f"... and takes it when that one rolls back ({bg.exc})")
        q(a, "BEGIN")
        q(a, "UPDATE uu SET name = 'y' WHERE g = 1 AND id = 1")
        q(b, "BEGIN")
        bg = Bg(lambda: q(b, "UPDATE uu SET name = 'y' WHERE g = 1 AND id = 3"))
        time.sleep(0.4)
        q(a, "COMMIT")
        bg.join()
        self.check(bg.code() == DUP, f"... or fails with 1062 when it commits ({bg.exc})")
        self.check(q(b, "SELECT name FROM uu WHERE g = 1 ORDER BY id") == (("y",), ("z",), ("c",)),
                   "the failed statement left the transaction intact")
        q(b, "COMMIT")

    def test_upsert_row_alias(self):
        # MySQL 8.0.19's row alias, which Rails' upsert_all uses on MySQL 8
        # (MariaDB has no such syntax, so this is not in difftest.py).
        c = self.fresh("ra", "CREATE TABLE ra (id INT PRIMARY KEY, n INT, name VARCHAR(10))", ["(1, 10, 'a')"])
        for level in ("READ-COMMITTED", "SERIALIZABLE"):
            q(c, f"SET SESSION transaction_isolation = '{level}'")
            q(c, "UPDATE ra SET n = 10, name = 'a' WHERE id = 1")
            q(c, "INSERT INTO ra (id, n, name) VALUES (1, 5, 'b'), (2, 7, 'c') AS new "
                 "ON DUPLICATE KEY UPDATE n = ra.n + new.n, name = new.name")
            self.check(q(c, "SELECT id, n, name FROM ra ORDER BY id") == ((1, 15, "b"), (2, 7, "c")),
                       f"{level}: AS new ... new.col")
            q(c, "INSERT INTO ra VALUES (2, 1, 'z') AS v(i, m, nm) ON DUPLICATE KEY UPDATE n = n + m, name = nm")
            self.check(q(c, "SELECT n, name FROM ra WHERE id = 2") == ((8, "z"),), f"{level}: column aliases")
            self.check(err_code(lambda: q(c, "INSERT INTO ra VALUES (2, 1, 'z') AS v ON DUPLICATE KEY UPDATE n = v.nope"))
                       == 1054, f"{level}: unknown alias column")
            q(c, "DELETE FROM ra WHERE id = 2")

    def test_concurrent_upserts(self):
        c = self.fresh("up", "CREATE TABLE up (k VARCHAR(10) PRIMARY KEY, n INT)", [])
        q(c, "DROP TABLE IF EXISTS up2")
        # distinct primary keys colliding on a unique key that contains the shard key
        q(c, "CREATE TABLE up2 (t INT, id INT, name VARCHAR(10), n INT, "
             "PRIMARY KEY (t, id), UNIQUE KEY tn (t, name)) PARTITION BY KEY(t)")
        errors = []
        per = 25

        def worker(w):
            s = self.rc()
            for i in range(per):
                try:
                    if i % 2:
                        q(s, "BEGIN")
                    if i % 3:
                        q(s, "INSERT INTO up VALUES ('hot', 1) ON DUPLICATE KEY UPDATE n = n + 1")
                    else:
                        q(s, "INSERT INTO up VALUES ('hot', 1) AS new ON DUPLICATE KEY UPDATE n = up.n + new.n")
                    q(s, f"INSERT INTO up2 VALUES (7, {w * 1000 + i}, 'hot', 1) ON DUPLICATE KEY UPDATE n = n + 1")
                    q(s, f"INSERT IGNORE INTO up VALUES ('k{i}', {w})")
                    if i % 2:
                        q(s, "COMMIT")
                except pymysql.MySQLError as e:
                    errors.append(e.args)
                    try:
                        q(s, "ROLLBACK")
                    except pymysql.MySQLError:
                        pass

        ths = [threading.Thread(target=worker, args=(w,)) for w in range(self.args.threads)]
        for t in ths:
            t.start()
        for t in ths:
            t.join()
        total = self.args.threads * per
        self.check(not errors, f"no errors ({errors[:3]})")
        self.check(q(c, "SELECT n FROM up WHERE k = 'hot'") == ((total,),), "primary-key upserts all counted")
        self.check(q(c, "SELECT COUNT(*), SUM(n) FROM up2") == ((1, total),), "unique-key upserts all counted, one row")
        self.check(q(c, "SELECT COUNT(*) FROM up WHERE k LIKE 'k%'")[0][0] == per, "INSERT IGNORE: one row per key")

    def test_savepoints(self):
        for level in ("READ-COMMITTED", "SERIALIZABLE"):
            c = self.fresh("sp", "CREATE TABLE sp (id INT PRIMARY KEY, v INT)", ["(1, 0)"])
            q(c, f"SET SESSION transaction_isolation = '{level}'")
            q(c, "BEGIN")
            q(c, "INSERT INTO sp VALUES (2, 0)")
            q(c, "SAVEPOINT a")
            q(c, "INSERT INTO sp VALUES (3, 0)")
            q(c, "UPDATE sp SET v = 1 WHERE id = 1")
            q(c, "SAVEPOINT b")
            q(c, "DELETE FROM sp WHERE id = 2")
            self.check(q(c, "SELECT id, v FROM sp ORDER BY id") == ((1, 1), (3, 0)), f"{level}: before rollback")
            q(c, "ROLLBACK TO SAVEPOINT b")
            self.check(q(c, "SELECT id, v FROM sp ORDER BY id") == ((1, 1), (2, 0), (3, 0)), f"{level}: back to b")
            q(c, "ROLLBACK TO a")
            self.check(q(c, "SELECT id, v FROM sp ORDER BY id") == ((1, 0), (2, 0)), f"{level}: back to a")
            self.check(err_code(lambda: q(c, "ROLLBACK TO b")) == 1305, f"{level}: later savepoints are gone (1305)")
            q(c, "SAVEPOINT a")  # same name: replaced
            q(c, "INSERT INTO sp VALUES (4, 0)")
            q(c, "RELEASE SAVEPOINT a")
            self.check(err_code(lambda: q(c, "ROLLBACK TO a")) == 1305, f"{level}: released (1305)")
            q(c, "COMMIT")
            self.check(q(connect(self.args.tarn), "SELECT id FROM sp ORDER BY id") == ((1,), (2,), (4,)),
                       f"{level}: committed rows")
        # rolling back to a savepoint keeps the row locks (as InnoDB does)
        c = self.rc()
        q(c, "BEGIN")
        q(c, "SAVEPOINT s")
        q(c, "UPDATE sp SET v = 9 WHERE id = 1")
        q(c, "ROLLBACK TO SAVEPOINT s")
        o = self.rc()
        self.check(err_code(lambda: q(o, "SELECT 1 FROM sp WHERE id = 1 FOR UPDATE NOWAIT")) == NOWAIT,
                   "locks taken after the savepoint are kept")
        q(c, "COMMIT")
        self.check(q(o, "SELECT v FROM sp WHERE id = 1 FOR UPDATE NOWAIT") == ((0,),), "... until the end")

    def test_statement_rollback(self):
        for level in ("READ-COMMITTED", "SERIALIZABLE"):
            c = self.fresh("st", "CREATE TABLE st (id INT PRIMARY KEY, v TINYINT)", ["(1, 100)", "(2, 120)"])
            q(c, f"SET SESSION transaction_isolation = '{level}'")
            q(c, "BEGIN")
            q(c, "INSERT INTO st VALUES (3, 0)")
            self.check(err_code(lambda: q(c, "INSERT INTO st VALUES (4, 0), (1, 0)")) == DUP, f"{level}: duplicate")
            self.check(err_code(lambda: q(c, "UPDATE st SET v = v + 10 ORDER BY id")) == 1264, f"{level}: out of range")
            self.check(q(c, "SELECT id, v FROM st ORDER BY id") == ((1, 100), (2, 120), (3, 0)),
                       f"{level}: failed statements left nothing behind")
            q(c, "COMMIT")
            self.check(q(connect(self.args.tarn), "SELECT COUNT(*) FROM st")[0][0] == 3, f"{level}: committed")

    def test_disconnect_releases_locks(self):
        self.fresh("dc", "CREATE TABLE dc (id INT PRIMARY KEY, v INT)", ["(1, 0)"])
        a = self.rc()
        q(a, "BEGIN")
        q(a, "UPDATE dc SET v = 1 WHERE id = 1")
        a.close()
        b = self.rc()
        q(b, "SET innodb_lock_wait_timeout = 5")
        t0 = time.time()
        code = err_code(lambda: q(b, "UPDATE dc SET v = v + 10 WHERE id = 1"))
        self.check(code is None and time.time() - t0 < 3, f"a closed session's locks are released ({code})")
        self.check(q(b, "SELECT v FROM dc")[0][0] == 10, "its uncommitted write is gone")

    def test_serializable_meets_row_lock(self):
        c = self.fresh("sm", "CREATE TABLE sm (id INT PRIMARY KEY, v INT)", ["(1, 0)"])
        r, s = self.rc(), connect(self.args.tarn)  # s is serializable
        q(r, "BEGIN")
        q(r, "UPDATE sm SET v = v + 1 WHERE id = 1")
        q(s, "BEGIN")
        seen = q(s, "SELECT v FROM sm WHERE id = 1")[0][0]
        q(s, f"UPDATE sm SET v = {seen + 10} WHERE id = 1")
        q(r, "COMMIT")
        code = err_code(lambda: q(s, "COMMIT"))
        self.check(code == DEADLOCK, f"the serializable transaction's stale write aborts ({code})")
        self.check(q(c, "SELECT v FROM sm")[0][0] == 1, "no lost update")

    def test_cross_shard_rc_commit(self):
        x, y = self.two_shards(list(range(1, 30)))
        c = self.fresh("xs", "CREATE TABLE xs (id INT PRIMARY KEY, v INT)", [f"({x}, 0)", f"({y}, 0)"])
        a, o = self.rc(), self.rc()
        q(a, "BEGIN")
        q(a, f"UPDATE xs SET v = v + 1 WHERE id IN ({x}, {y})")
        self.check(q(a, f"SELECT id FROM xs WHERE id = {x} FOR UPDATE") == ((x,),), "re-locking an own row")
        q(a, "COMMIT")
        self.check(q(c, "SELECT SUM(v) FROM xs")[0][0] == 2, "both shards' writes committed")
        q(o, "BEGIN")
        self.check(len(q(o, "SELECT id FROM xs FOR UPDATE NOWAIT")) == 2, "and the locks were released")
        q(o, "COMMIT")

    def test_zz_bank(self):
        """Concurrent READ COMMITTED transfers (UPDATE ... SET bal = bal - x on
        locked rows, a ledger row in the same transaction, some across shards)
        for --bank-seconds; tests/failover.sh runs it during leader failovers.
        Each COMMIT is acknowledged (its ledger row must exist), refused
        (1213: must not), or of unknown outcome (1180, lost connection:
        either). The final balances must equal what the ledger says: a lost
        update or a half-applied transfer shows up there."""
        if self.args.bank_seconds <= 0:
            return
        n_acc, init = 32, 1000
        self.fresh("acct", "CREATE TABLE acct (id INT PRIMARY KEY, bal BIGINT NOT NULL)",
                   [f"({i}, {init})" for i in range(n_acc)])
        self.fresh("led", "CREATE TABLE led (id BIGINT PRIMARY KEY, x INT, y INT, amt INT)", [])
        stop = time.time() + self.args.bank_seconds
        st = {"ok": 0, "refused": 0, "timeouts": 0, "acked": set(), "refused_ids": set(), "unknown": set(),
              "other": []}
        lk = threading.Lock()

        def worker(seed):
            rnd = random.Random(seed)
            c, seq = None, 0
            while time.time() < stop:
                if c is None:
                    try:
                        c = self.rc()
                        q(c, "SET innodb_lock_wait_timeout = 5")
                    except pymysql.MySQLError:
                        time.sleep(0.2)
                        continue
                x, y = rnd.sample(range(n_acc), 2)
                amt = rnd.randint(1, 20)
                seq += 1
                lid = seed * 10_000_000 + seq
                stage = "body"
                try:
                    q(c, "BEGIN")
                    q(c, "UPDATE acct SET bal = bal - %s WHERE id = %s", (amt, x))
                    q(c, "UPDATE acct SET bal = bal + %s WHERE id = %s", (amt, y))
                    q(c, "INSERT INTO led VALUES (%s, %s, %s, %s)", (lid, x, y, amt))
                    stage = "commit"
                    q(c, "COMMIT")
                    with lk:
                        st["ok"] += 1
                        st["acked"].add(lid)
                except pymysql.MySQLError as e:
                    code = e.args[0] if e.args else 0
                    with lk:
                        if code in (DEADLOCK, LOCK_TIMEOUT):
                            st["refused" if code == DEADLOCK else "timeouts"] += 1
                            if stage == "commit":
                                st["refused_ids"].add(lid)
                        else:
                            if stage == "commit":
                                st["unknown"].add(lid)
                            if code not in (1180, 2003, 2006, 2013):
                                st["other"].append(e.args)
                    try:
                        q(c, "ROLLBACK")
                    except pymysql.MySQLError:
                        c = None

        ths = [threading.Thread(target=worker, args=(n,)) for n in range(self.args.threads)]
        for t in ths:
            t.start()
        for t in ths:
            t.join()
        # A shard may still be electing its new leader (tests/failover.sh), and
        # recovery may still be finishing transfers whose COMMIT was cut off:
        # retry the reads, and wait until the balances and the ledger agree,
        # or time out.
        deadline, c = time.time() + 30, None
        while True:
            try:
                c = c or connect(self.args.tarn)
                led = {r[0]: r[1:] for r in q(c, "SELECT id, x, y, amt FROM led")}
                bal = dict(q(c, "SELECT id, bal FROM acct"))
            except pymysql.MySQLError:
                if time.time() > deadline:
                    raise
                c = None
                time.sleep(0.5)
                continue
            exp = {i: init for i in range(n_acc)}
            for x, y, amt in led.values():
                exp[x] -= amt
                exp[y] += amt
            if bal == exp or time.time() > deadline:
                break
            time.sleep(0.5)
        print(f"    bank (READ COMMITTED): {st['ok']} commits, {st['refused']} deadlocks, {st['timeouts']} lock "
              f"timeouts, {len(st['unknown'])} unknown outcomes, {len(st['other'])} other errors")
        self.check(sum(bal.values()) == n_acc * init, f"total {sum(bal.values())}")
        self.check(bal == exp, "balances match the ledger (no lost update, no half-applied transfer)")
        self.check(st["acked"] <= set(led), f"{len(st['acked'] - set(led))} acknowledged transfers missing")
        self.check(not (st["refused_ids"] & set(led)), "a transfer reported as rolled back was committed")
        self.check(set(led) <= st["acked"] | st["unknown"], "unexplained ledger rows")
        self.check(not st["other"], f"unexpected errors: {st['other'][:3]}")
        self.check(st["ok"] > 0, "transfers committed")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True, help="host:port[,host:port...]: the cluster's SQL servers")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--rounds", type=int, default=60, help="transactions per thread in the counter test")
    ap.add_argument("--bank-seconds", type=float, default=8, help="length of the transfer workload (0: skip)")
    ap.add_argument("--only", default="")
    ap.add_argument("--default-isolation", default="SERIALIZABLE")
    args = ap.parse_args()
    args.servers = args.tarn.split(",")
    args.tarn = args.servers[0]
    c = connect(args.tarn, db=None)
    q(c, f"DROP DATABASE IF EXISTS {DB}")
    q(c, f"CREATE DATABASE {DB}")
    t = T(args)
    if t.nshards < 2:
        print("needs a cluster with at least two shards")
        sys.exit(2)
    names = sorted(n for n in dir(t) if n.startswith("test_") and (not args.only or args.only in n))
    for n in names:
        before = t.failures
        t0 = time.time()
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
