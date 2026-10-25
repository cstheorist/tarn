#!/usr/bin/env python3
"""Schema changes and dump loading (issue #16): MySQL dumps, multi-statement
batches, ALTER TABLE that Tarn refuses, and ALTER TABLE while transactions
run on several SQL servers -- no acknowledged write may be lost, and no
autocommit statement may fail because a table definition changed.

    tests/sql/ddltest.py --tarn 127.0.0.1:4000[,127.0.0.1:4001] [--seconds 20] [--threads 8]

With two SQL servers (scripts/cluster.sh start -s 2) the online test runs
writers on both while the schema changes go through the first: the second
server sees each new definition only after it reloads its catalog.
"""
import argparse
import os
import random
import shutil
import subprocess
import sys
import threading
import time

import pymysql
from pymysql.constants import CLIENT, COMMAND

DB = "ddltest"
HERE = os.path.dirname(os.path.abspath(__file__))
RETRY = (1213, 1412, 1205)
UNKNOWN = 1180


def connect(hp, db=DB, autocommit=True, multi=False):
    host, port = hp.split(":")
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db,
                           autocommit=autocommit, charset="utf8mb4",
                           client_flag=CLIENT.MULTI_STATEMENTS if multi else 0)


def q(c, sql, args=None):
    cur = c.cursor()
    cur.execute(sql, args)
    rows = cur.fetchall() if cur.description else cur.rowcount
    cur.close()
    return rows


def code(fn):
    try:
        fn()
    except pymysql.MySQLError as e:
        return e.args[0]
    return None


class T:
    def __init__(self, args):
        self.args = args
        self.servers = args.tarn.split(",")
        self.failures = 0

    def check(self, cond, what):
        if not cond:
            self.failures += 1
            print(f"    FAILED: {what}")
        return cond

    def c(self, i=0, **kw):
        return connect(self.servers[i % len(self.servers)], **kw)

    # ------------------------------------------------------------------
    def test_dump(self):
        """A mysqldump file (executable comments, SET with variables, LOCK
        TABLES, DISABLE KEYS, INVISIBLE indexes, foreign keys, table
        options) loads without errors."""
        c = self.c(db=None)
        q(c, "DROP DATABASE IF EXISTS ddl_dump")
        q(c, "CREATE DATABASE ddl_dump")
        path = os.path.join(HERE, "data", "mysql8_dump.sql")
        cli = shutil.which("mysql") or shutil.which("mariadb")
        host, port = self.servers[0].split(":")
        if cli:
            r = subprocess.run([cli, "-h", host, "-P", port, "-u", "sb", "ddl_dump"], stdin=open(path),
                               capture_output=True, text=True)
            self.check(r.returncode == 0 and not r.stderr.strip(), f"mysql client: {r.stderr.strip()[:400]}")
        else:
            m = self.c(db="ddl_dump", multi=True)
            cur = m.cursor()
            cur.execute(open(path).read())
            while cur.nextset():
                pass
        d = self.c(db="ddl_dump")
        self.check(q(d, "SELECT COUNT(*) FROM accounts")[0][0] == 3, "accounts rows")
        self.check(q(d, "SELECT email FROM users ORDER BY id") == (("a@acme.test",), ("b@globex.test",)), "users rows")
        create = q(d, "SHOW CREATE TABLE accounts")[0][1]
        self.check("INVISIBLE" in create and "COMMENT='customer accounts'" in create, f"SHOW CREATE TABLE: {create}")
        create = q(d, "SHOW CREATE TABLE users")[0][1]
        self.check("CONSTRAINT `fk_rails_61ac11da2b` FOREIGN KEY" in create, f"foreign key kept: {create}")
        # AUTO_INCREMENT=4 in the dump: the next generated id is 4
        q(d, "INSERT INTO accounts (name, created_at, updated_at) VALUES ('new', NOW(), NOW())")
        self.check(q(d, "SELECT LAST_INSERT_ID()")[0][0] == 4, "AUTO_INCREMENT table option")
        # the dump's session settings are back to their defaults afterwards
        self.check(q(d, "SELECT @@foreign_key_checks, @@unique_checks")[0] == (1, 1), "restored session variables")

    def test_multi_statements(self):
        """CLIENT_MULTI_STATEMENTS and COM_SET_OPTION: one result per
        statement, the first error ends the batch."""
        m = self.c(multi=True)
        cur = m.cursor()
        cur.execute("DROP TABLE IF EXISTS ms; CREATE TABLE ms (id INT PRIMARY KEY, v INT); "
                    "INSERT INTO ms VALUES (1, 10), (2, 20); SELECT SUM(v) FROM ms; UPDATE ms SET v = v + 1")
        results = []
        while True:
            results.append((cur.rowcount, cur.fetchall() if cur.description else None))
            if not cur.nextset():
                break
        self.check([r[0] for r in results] == [0, 0, 2, 1, 2] and results[3][1][0][0] == 30, f"results {results}")
        def batch():
            cur.execute("UPDATE ms SET v = 0 WHERE id = 1; SELECT * FROM nosuch; UPDATE ms SET v = 0")
            while cur.nextset():
                pass
        e = code(batch)
        self.check(e == 1146, f"error in a batch: {e}")
        self.check(q(m, "SELECT v FROM ms ORDER BY id") == ((0,), (21,)), "statements after the error did not run")
        # without the capability a batch is a syntax error ...
        plain = self.c()
        self.check(code(lambda: q(plain, "SELECT 1; SELECT 2")) == 1064, "batch refused without the capability")
        # ... until COM_SET_OPTION turns multi-statements on (as Rails does)
        def set_option(opt):
            plain._execute_command(COMMAND.COM_SET_OPTION, opt)
            pkt = plain._read_packet()   # EOF, as MySQL answers
            return pkt.is_eof_packet() or pkt.is_ok_packet()
        self.check(set_option(b"\x00\x00"), "COM_SET_OPTION answered")
        cur = plain.cursor()
        cur.execute("SELECT 1; SELECT 2")
        got = [cur.fetchall()]
        while cur.nextset():
            got.append(cur.fetchall())
        self.check(got == [((1,),), ((2,),)], f"COM_SET_OPTION on: {got}")
        set_option(b"\x01\x00")
        self.check(code(lambda: q(plain, "SELECT 1; SELECT 2")) == 1064, "COM_SET_OPTION off")

    def test_refusals(self):
        """What would need rows rewritten or resharded is refused (1235)."""
        c = self.c()
        q(c, "DROP TABLE IF EXISTS rf")
        q(c, "CREATE TABLE rf (id INT PRIMARY KEY, v INT, s VARCHAR(10), d DECIMAL(6,2))")
        q(c, "INSERT INTO rf VALUES (1, 1, 'a', 1.5)")
        for sql in ["ALTER TABLE rf MODIFY v VARCHAR(10)", "ALTER TABLE rf MODIFY s INT",
                    "ALTER TABLE rf MODIFY d DECIMAL(6,3)", "ALTER TABLE rf DROP PRIMARY KEY",
                    "ALTER TABLE rf ADD PRIMARY KEY (v)", "ALTER TABLE rf DROP COLUMN id",
                    "ALTER TABLE rf MODIFY id BIGINT UNSIGNED NOT NULL",
                    "ALTER TABLE rf ADD COLUMN k INT AUTO_INCREMENT", "ALTER TABLE rf ADD FULLTEXT INDEX f (s)"]:
            self.check(code(lambda: q(c, sql)) == 1235, sql)
        # a unique key must contain the shard key, as at CREATE TABLE
        self.check(code(lambda: q(c, "ALTER TABLE rf ADD UNIQUE KEY us (s)")) == 1503, "unique key without the shard key")
        self.check(q(c, "SELECT * FROM rf")[0][:3] == (1, 1, "a"), "table unchanged")

    def test_concurrent_ddl(self):
        """Schema changes from several connections at once (as sysbench's
        prepare creates its indexes): they queue, none fails."""
        errors = []

        def work(i):
            try:
                c = self.c(i)
                q(c, f"DROP TABLE IF EXISTS cd{i}")
                q(c, f"CREATE TABLE cd{i} (id INT PRIMARY KEY, v INT)")
                q(c, f"INSERT INTO cd{i} VALUES (1, 1), (2, 2)")
                q(c, f"CREATE INDEX k_{i} ON cd{i} (v)")
                q(c, f"ALTER TABLE cd{i} ADD COLUMN w INT DEFAULT 7")
            except pymysql.MySQLError as e:
                errors.append(f"cd{i}: {e}")

        threads = [threading.Thread(target=work, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(120)
        self.check(not errors, f"concurrent schema changes: {errors}")
        c = self.c()
        for i in range(6):
            keys = {r[2] for r in q(c, f"SHOW INDEX FROM cd{i}")}
            self.check(f"k_{i}" in keys, f"cd{i} has its index: {keys}")
            self.check(q(c, f"SELECT SUM(w) FROM cd{i}")[0][0] == 14, f"cd{i} has its column")

    def test_txn_across_alter(self):
        """A transaction that wrote a table cannot use a newer definition of
        it: rows in its write buffer are in the old column order. Its next
        statement on the table fails with 1412 and it is rolled back."""
        c = self.c()
        q(c, "DROP TABLE IF EXISTS tx")
        q(c, "CREATE TABLE tx (id INT PRIMARY KEY, n INT NOT NULL, s VARCHAR(10))")
        q(c, "INSERT INTO tx VALUES (1, 10, 'a'), (2, 20, 'b')")
        probe = self.c(1)   # the writer's SQL server (the second, if any)

        def wait_for(col, present):
            deadline = time.time() + 10
            while time.time() < deadline:
                if (f"`{col}`" in q(probe, "SHOW CREATE TABLE tx")[0][1]) == present:
                    return True
                time.sleep(0.05)
            return False

        # Under READ COMMITTED no stream transaction holds the reads, so
        # nothing else stops the old rows from being reused.
        cases = [("ADD COLUMN y VARCHAR(10) DEFAULT 'zz' AFTER id", "y", True, "UPDATE tx SET n = n + 1 WHERE id = 1"),
                 ("DROP COLUMN s", "s", False, "SELECT * FROM tx"),
                 ("ADD COLUMN z INT FIRST", "z", True, "INSERT INTO tx (id, n) VALUES (3, 30)"),
                 ("DROP COLUMN y", "y", False, "UPDATE tx SET n = n + 1 WHERE id = 1"),
                 ("ADD COLUMN s CHAR(2) AFTER id", "s", True, "SELECT * FROM tx WHERE id = 1"),
                 ("DROP COLUMN z", "z", False, "DELETE FROM tx WHERE id = 1")]
        for k, (alter, col, present, stmt) in enumerate(cases):
            iso = "SERIALIZABLE" if k % 2 == 0 else "READ COMMITTED"
            w = self.c(1, autocommit=False)
            q(w, f"SET SESSION TRANSACTION ISOLATION LEVEL {iso}")
            q(w, "UPDATE tx SET n = n + 1 WHERE id = 1")
            q(c, f"ALTER TABLE tx {alter}")
            self.check(wait_for(col, present), f"{alter}: the writer's server reloads its catalog")
            e = code(lambda: q(w, stmt))
            self.check(e == 1412, f"{iso}: {stmt} after {alter} in a transaction that wrote the table: {e}")
            q(w, "COMMIT")   # nothing left to commit
            self.check(q(c, "SELECT n FROM tx WHERE id = 1") == ((10,),), f"case {k}: the first UPDATE was rolled back")
            w.close()
        self.check(q(c, "SELECT id, n FROM tx ORDER BY id") == ((1, 10), (2, 20)), "table rows")

    def test_online(self):
        """Writers on every SQL server while one of them changes the table."""
        seconds, nthreads = self.args.seconds, self.args.threads
        c = self.c()
        q(c, "DROP TABLE IF EXISTS acc")
        extra = ", x INT NOT NULL DEFAULT 0" if self.args.no_ddl else ""
        q(c, f"CREATE TABLE acc (id INT PRIMARY KEY, n INT NOT NULL DEFAULT 0, tag VARCHAR(20){extra}, KEY kn (n))")
        q(c, "INSERT INTO acc (id, tag) VALUES " + ", ".join(f"({i}, 't{i}')" for i in range(1, 65)))
        lock = threading.Lock()
        st = {"n": 0, "x": 0, "n_unknown": 0, "x_unknown": 0, "ops": 0, "retries": 0, "errors": [], "ddl": 0}
        x_ready = threading.Event()
        if self.args.no_ddl:
            x_ready.set()
        stop = threading.Event()

        def bump(key, k=1):
            with lock:
                st[key] += k

        def autocommit(conn, sql, args, key):
            try:
                bump(key, q(conn, sql, args))   # rows changed (0 if the WHERE fails)
            except pymysql.MySQLError as e:
                if e.args[0] == UNKNOWN:
                    bump(key + "_unknown")
                else:
                    with lock:
                        st["errors"].append(f"autocommit {sql}: {e}")

        def txn(conn, body):
            """Run body in a transaction until it commits; returns its result."""
            for _ in range(200):
                try:
                    q(conn, "BEGIN")
                    r = body()
                    q(conn, "COMMIT")
                    return r
                except pymysql.MySQLError as e:
                    try:
                        q(conn, "ROLLBACK")
                    except pymysql.MySQLError:
                        pass
                    if e.args[0] in RETRY:
                        bump("retries")
                        time.sleep(random.random() * 0.01)
                        continue
                    with lock:
                        st["errors"].append(f"transaction: {e}")
                    return None
            return None

        def writer(w):
            conn = self.c(w)
            if w % 3 == 2:
                q(conn, "SET SESSION transaction_isolation = 'READ-COMMITTED'")
            rnd = random.Random(w)
            while not stop.is_set():
                a, b = rnd.randint(1, 64), rnd.randint(1, 64)
                kind = rnd.randint(0, 4)
                if kind == 0:  # fast path
                    autocommit(conn, "UPDATE acc SET n = n + 1 WHERE id = %s", (a,), "n")
                elif kind == 1:  # read-modify-write in an implicit transaction
                    autocommit(conn, "UPDATE acc SET n = n + 1 WHERE id = %s AND n >= 0", (a,), "n")
                elif kind == 2:  # move one unit between two rows (sum unchanged)
                    if a != b:
                        txn(conn, lambda: (q(conn, "UPDATE acc SET n = n - 1 WHERE id = %s", (a,)),
                                           q(conn, "UPDATE acc SET n = n + 1 WHERE id = %s", (b,))))
                elif kind == 3 and x_ready.is_set():
                    autocommit(conn, "UPDATE acc SET x = x + 1 WHERE id = %s", (a,), "x")
                else:  # an increment of both in one transaction, rows read first
                    def body():
                        q(conn, "SELECT n FROM acc WHERE id = %s", (a,))
                        q(conn, "UPDATE acc SET n = n + 1 WHERE id = %s", (a,))
                        if x_ready.is_set():
                            q(conn, "UPDATE acc SET x = x + 1 WHERE id = %s", (b,))
                            return 2
                        return 1
                    r = txn(conn, body)
                    if r:
                        bump("n")
                        if r == 2:
                            bump("x")
                bump("ops")

        def reader(w):
            conn = self.c(w)
            # READ COMMITTED: scans never abort (at SERIALIZABLE a scan of a
            # hot table may exhaust its retries, schema change or not)
            q(conn, "SET SESSION transaction_isolation = 'READ-COMMITTED'")
            while not stop.is_set():
                try:
                    q(conn, "SELECT * FROM acc WHERE id = %s", (random.randint(1, 64),))
                    q(conn, "SELECT COUNT(*) FROM acc WHERE n > 1")
                except pymysql.MySQLError as e:
                    with lock:
                        st["errors"].append(f"read: {e}")

        def ddl():
            conn = self.c(0)
            steps = ["ALTER TABLE acc ADD COLUMN x INT NOT NULL DEFAULT 0"]
            k = 0
            while len(steps) < 400:
                k += 1
                steps += [f"ALTER TABLE acc ADD COLUMN y{k} VARCHAR(10) DEFAULT 'y' AFTER id",
                          f"ALTER TABLE acc ADD INDEX ix{k} (x, n)",
                          f"ALTER TABLE acc MODIFY tag VARCHAR({30 + k})",
                          f"ALTER TABLE acc RENAME COLUMN tag TO tag{k}",
                          f"ALTER TABLE acc RENAME COLUMN tag{k} TO tag",
                          f"ALTER TABLE acc DROP COLUMN y{k}",
                          f"ALTER TABLE acc MODIFY n {'BIGINT' if k % 2 else 'INT'} NOT NULL DEFAULT 0",
                          f"ALTER TABLE acc DROP INDEX ix{k}, RENAME INDEX kn TO kn{k}",
                          f"ALTER TABLE acc RENAME INDEX kn{k} TO kn"]
            for s in steps:
                if stop.is_set() or self.args.no_ddl:
                    break
                try:
                    q(conn, s)
                    bump("ddl")
                    if s.startswith("ALTER TABLE acc ADD COLUMN x"):
                        x_ready.set()
                except pymysql.MySQLError as e:
                    with lock:
                        st["errors"].append(f"ddl {s}: {e}")
                time.sleep(0.05)

        threads = [threading.Thread(target=writer, args=(w,), daemon=True) for w in range(nthreads)]
        threads += [threading.Thread(target=reader, args=(w,), daemon=True) for w in range(2)]
        threads.append(threading.Thread(target=ddl, daemon=True))
        for t in threads:
            t.start()
        time.sleep(seconds)
        stop.set()
        for t in threads:
            t.join(60)
        sums = q(self.c(), "SELECT SUM(n), SUM(x), COUNT(*) FROM acc")[0]
        print(f"    {st['ops']} writes, {st['ddl']} schema changes, {st['retries']} transaction retries, "
              f"{st['n_unknown'] + st['x_unknown']} unknown outcomes; SUM(n) {sums[0]} (acknowledged {st['n']}), "
              f"SUM(x) {sums[1]} (acknowledged {st['x']})")
        kinds = {}
        for e in st["errors"]:
            kinds.setdefault(e[:90], 0)
            kinds[e[:90]] += 1
        for e, n in sorted(kinds.items(), key=lambda x: -x[1])[:10]:
            print(f"      error x{n}: {e}")
        self.check(not st["errors"], f"{len(st['errors'])} errors")
        self.check(st["ddl"] >= 8 or self.args.no_ddl, "schema changes ran")
        self.check(st["n"] <= sums[0] <= st["n"] + st["n_unknown"], "no increment of n lost or duplicated")
        self.check(st["x"] <= sums[1] <= st["x"] + st["x_unknown"], "no increment of x lost or duplicated")
        self.check(sums[2] == 64, "rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True, help="host:port[,host:port...]")
    ap.add_argument("--seconds", type=float, default=20)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--only", default="")
    ap.add_argument("--no-ddl", action="store_true", help="the online test without schema changes")
    args = ap.parse_args()
    c = connect(args.tarn.split(",")[0], db=None)
    q(c, f"DROP DATABASE IF EXISTS {DB}")
    q(c, f"CREATE DATABASE {DB}")
    t = T(args)
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
