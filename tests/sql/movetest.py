#!/usr/bin/env python3
"""Bucket moves under load (issue #24): transactions run through every SQL
server while one of them moves the buckets of the rows they use back and
forth between two shards.

    tests/sql/movetest.py --tarn 127.0.0.1:4000[,127.0.0.1:4001] [--seconds 30] [--threads 6]

The workload, on rows whose buckets keep moving:
  * serializable transfers between accounts, each with a ledger row, through
    explicit transactions: no transfer is lost or applied twice, and every
    acknowledged COMMIT is in the ledger;
  * READ COMMITTED read-modify-writes of counters (SELECT ... FOR UPDATE,
    then UPDATE) and autocommit increments: no update is lost;
  * serializable audits (the sum of the balances never changes) and READ
    COMMITTED reads of every shard, which must see each row exactly once:
    a read that met a bucket in two places, or in none, would not.
Moves that fail (a lock held too long, a statement refused) are counted and
must not break any of these. The table's shard key is its primary key, so
the test can compute each row's bucket (route.rs: CRC-32C of the key's
msgpack encoding, mod the bucket count, plus one).
"""
import argparse
import random
import sys
import threading
import time

import pymysql

DB = "movetest"
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


def crc32c(data, crc=0xFFFFFFFF):
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0x82F63B78 if crc & 1 else crc >> 1
    return crc


def mp_uint(i):
    if i < 128:
        return bytes([i])
    for tag, n in ((0xcc, 1), (0xcd, 2), (0xce, 4), (0xcf, 8)):
        if i < 1 << (8 * n):
            return bytes([tag]) + i.to_bytes(n, "big")
    raise ValueError(i)


def bucket_of(i, count):
    """The bucket of a row whose shard key is the integer i >= 0."""
    return crc32c(mp_uint(i)) % count + 1


class Test:
    def __init__(self, args):
        self.args = args
        self.servers = args.tarn.split(",")
        self.failures = 0
        self.lock = threading.Lock()
        self.stats = {"commit": 0, "conflict": 0, "acked": set(), "aborted": set(), "ambiguous": set(),
                      "err": [], "audits": 0, "rc_reads": 0, "bad": [], "moves": 0, "move_errs": [],
                      "rc_ok": 0, "rc_conflict": 0, "inc_ok": [], "inc_amb": []}

    def check(self, cond, what):
        print(f"  {'ok  ' if cond else 'FAIL'} {what}")
        if not cond:
            self.failures += 1
        return cond

    def server(self, k):
        return self.servers[k % len(self.servers)]

    def setup(self):
        c = connect(self.servers[0], db=None)
        q(c, f"DROP DATABASE IF EXISTS {DB}")
        q(c, f"CREATE DATABASE {DB}")
        c.select_db(DB)
        a = self.args
        q(c, "CREATE TABLE bank (id INT PRIMARY KEY, bal BIGINT NOT NULL)")
        q(c, "INSERT INTO bank VALUES " + ",".join(f"({i}, {a.init})" for i in range(a.accounts)))
        q(c, "CREATE TABLE ledger (id BIGINT PRIMARY KEY, x INT NOT NULL, y INT NOT NULL, amt INT NOT NULL)")
        q(c, "CREATE TABLE counters (id INT PRIMARY KEY, n BIGINT NOT NULL)")
        q(c, "INSERT INTO counters VALUES " + ",".join(f"({i}, 0)" for i in range(a.counters)))
        rows = q(c, "SHOW TARN BUCKETS")
        self.shard_ids = [int(r[0]) for r in rows]
        self.count = sum(int(r[1]) for r in rows)
        # the buckets of the rows the workload uses
        self.buckets = sorted({bucket_of(i, self.count) for i in range(max(a.accounts, a.counters))})
        self.stats["inc_ok"] = [0] * a.counters
        self.stats["inc_amb"] = [0] * a.counters

    # ---- workload -------------------------------------------------------------
    def transfer(self, k, stop):
        rnd = random.Random(k)
        c = connect(self.server(k))
        seq = 0
        a = self.args
        while time.time() < stop:
            x, y = rnd.sample(range(a.accounts), 2)
            amt = rnd.randint(1, 50)
            seq += 1
            lid = k * 1_000_000 + seq
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
                with self.lock:
                    self.stats["commit"] += 1
                    self.stats["acked"].add(lid)
            except pymysql.MySQLError as e:
                code = e.args[0] if e.args else 0
                with self.lock:
                    if code == CONFLICT:
                        self.stats["conflict"] += 1
                        if stage == "commit":
                            self.stats["aborted"].add(lid)
                    else:
                        if stage == "commit":
                            self.stats["ambiguous"].add(lid)
                        self.stats["err"].append(f"transfer: {e}")
                try:
                    q(c, "ROLLBACK")
                except pymysql.MySQLError:
                    c = connect(self.server(k))

    def rc_counter(self, k, stop):
        """READ COMMITTED read-modify-write: add one under the row lock."""
        rnd = random.Random(100 + k)
        c = connect(self.server(k))
        q(c, "SET SESSION transaction_isolation = 'READ-COMMITTED'")
        a = self.args
        while time.time() < stop:
            i = rnd.randrange(a.counters)
            try:
                q(c, "BEGIN")
                n = q(c, "SELECT n FROM counters WHERE id = %s FOR UPDATE", (i,))[0][0]
                q(c, "UPDATE counters SET n = %s WHERE id = %s", (n + 1, i))
                q(c, "COMMIT")
                with self.lock:
                    self.stats["rc_ok"] += 1
                    self.stats["inc_ok"][i] += 1
            except pymysql.MySQLError as e:
                code = e.args[0] if e.args else 0
                with self.lock:
                    if code == CONFLICT or code == 1205:
                        self.stats["rc_conflict"] += 1
                    else:
                        self.stats["inc_amb"][i] += 1
                        self.stats["err"].append(f"rc counter: {e}")
                try:
                    q(c, "ROLLBACK")
                except pymysql.MySQLError:
                    c = connect(self.server(k))
                    q(c, "SET SESSION transaction_isolation = 'READ-COMMITTED'")

    def increment(self, k, stop):
        """Autocommit increments (one storage call each when routed fresh)."""
        rnd = random.Random(200 + k)
        c = connect(self.server(k))
        a = self.args
        while time.time() < stop:
            i = rnd.randrange(a.counters)
            try:
                q(c, "UPDATE counters SET n = n + 1 WHERE id = %s", (i,))
                with self.lock:
                    self.stats["inc_ok"][i] += 1
            except pymysql.MySQLError as e:
                code = e.args[0] if e.args else 0
                with self.lock:
                    if code != CONFLICT:
                        self.stats["inc_amb"][i] += 1
                        self.stats["err"].append(f"increment: {e}")
                try:
                    c.ping()
                except pymysql.MySQLError:
                    c = connect(self.server(k))

    def audit(self, k, stop):
        a = self.args
        total = a.accounts * a.init
        c = connect(self.server(k))
        rc = connect(self.server(k + 1))
        q(rc, "SET SESSION transaction_isolation = 'READ-COMMITTED'")
        while time.time() < stop:
            try:
                q(c, "BEGIN")
                s = q(c, "SELECT SUM(bal), COUNT(*) FROM bank")[0]
                q(c, "COMMIT")
                with self.lock:
                    self.stats["audits"] += 1
                    if int(s[0]) != total or s[1] != a.accounts:
                        self.stats["bad"].append(("serializable audit", s))
            except pymysql.MySQLError as e:
                try:
                    q(c, "ROLLBACK")
                except pymysql.MySQLError:
                    c = connect(self.server(k))
                if e.args[0] != CONFLICT:
                    with self.lock:
                        self.stats["err"].append(f"audit: {e}")
            # READ COMMITTED reads of every shard: each row exactly once
            try:
                ids = [r[0] for r in q(rc, "SELECT id FROM bank")]
                n = q(rc, "SELECT COUNT(*) FROM bank")[0][0]
                with self.lock:
                    self.stats["rc_reads"] += 1
                    if sorted(ids) != list(range(a.accounts)) or n != a.accounts:
                        self.stats["bad"].append(("read committed", len(ids), len(set(ids)), n))
            except pymysql.MySQLError as e:
                if e.args[0] != CONFLICT:
                    with self.lock:
                        self.stats["err"].append(f"rc read: {e}")
                try:
                    rc.ping()
                except pymysql.MySQLError:
                    rc = connect(self.server(k + 1))
                    q(rc, "SET SESSION transaction_isolation = 'READ-COMMITTED'")

    def mover(self, stop):
        rnd = random.Random(7)
        c = connect(self.servers[-1])
        while time.time() < stop:
            b = rnd.choice(self.buckets)
            to = rnd.choice(self.shard_ids)
            try:
                r = q(c, f"TARN MOVE BUCKET {b} TO SHARD {to}")
                with self.lock:
                    self.stats["moves"] += r
            except pymysql.MySQLError as e:
                with self.lock:
                    self.stats["move_errs"].append(str(e))
                try:
                    c.ping()
                except pymysql.MySQLError:
                    c = connect(self.servers[-1])
            time.sleep(self.args.move_pause)

    # ---- checks -------------------------------------------------------------
    def workload(self, stop):
        """The threads of the workload, until `stop` (not started)."""
        a = self.args
        th = [threading.Thread(target=self.transfer, args=(k, stop)) for k in range(a.threads)]
        th += [threading.Thread(target=self.rc_counter, args=(k, stop)) for k in range(2)]
        th += [threading.Thread(target=self.increment, args=(k, stop)) for k in range(2)]
        th += [threading.Thread(target=self.audit, args=(k, stop)) for k in range(2)]
        if not a.no_moves:
            th += [threading.Thread(target=self.mover, args=(stop,))]
        return th

    def run(self):
        a = self.args
        self.setup()
        print(f"{len(self.servers)} SQL server(s), shards {self.shard_ids}, {self.count} buckets; "
              f"moving the {len(self.buckets)} buckets of the rows for {a.seconds:.0f} s")
        th = self.workload(time.time() + a.seconds)
        for t in th:
            t.start()
        for t in th:
            t.join()
        return self.finish()

    def finish(self):
        """The checks once the workload stopped: the number of failures."""
        a = self.args
        st = self.stats
        c = connect(self.servers[0])
        # (recovery may still be applying a transaction whose APPLY was cut
        # off: wait for the balances to match the ledger)
        deadline = time.time() + 30
        while True:
            bal = dict(q(c, "SELECT id, bal FROM bank"))
            ledger = q(c, "SELECT id, x, y, amt FROM ledger")
            want = {i: a.init for i in range(a.accounts)}
            for _, x, y, amt in ledger:
                want[x] -= amt
                want[y] += amt
            if want == bal or time.time() > deadline:
                break
            time.sleep(0.5)
        ctrs = dict(q(c, "SELECT id, n FROM counters"))
        in_ledger = {r[0] for r in ledger}
        # A move whose step failed after its send (a shard's leader died)
        # is finished by a server that finds its job abandoned.
        deadline = time.time() + a.settle
        while True:
            rows = q(c, "SHOW TARN BUCKETS")
            moving = [m for r in rows for m in filter(None, r[4].split(", ")) if " sent " not in m]
            if not moving or time.time() > deadline:
                break
            time.sleep(0.5)
        print(f"  {st['moves']} buckets moved ({len(st['move_errs'])} moves failed); {st['commit']} transfers, "
              f"{st['conflict']} conflicts; {st['rc_ok']} READ COMMITTED increments; {st['audits']} audits, "
              f"{st['rc_reads']} READ COMMITTED reads of every shard; {len(st['err'])} other errors")
        for e in st["move_errs"][:5]:
            print(f"    move failed: {e}")
        for e in st["err"][:5]:
            print(f"    error: {e}")
        if not a.no_moves:
            self.check(st["moves"] >= a.min_moves, f"buckets moved under load ({st['moves']})")
        self.check(not st["bad"], f"reads saw every row once, and the sum unchanged: {st['bad'][:5]}")
        self.check(sum(bal.values()) == a.accounts * a.init and len(bal) == a.accounts, "final balance sum")
        diff = {i: (bal.get(i), want[i]) for i in want if bal.get(i) != want[i]}
        self.check(not diff, f"balances match the ledger: {list(diff.items())[:5]}")
        lost = st["acked"] - in_ledger
        self.check(not lost, f"every acknowledged COMMIT is in the ledger ({len(lost)} missing)")
        phantom = st["aborted"] & in_ledger
        self.check(not phantom, f"no COMMIT that failed with 1213 committed: {sorted(phantom)[:5]}")
        bad = [(i, ctrs.get(i), st["inc_ok"][i], st["inc_amb"][i]) for i in range(a.counters)
               if not (st["inc_ok"][i] <= ctrs.get(i, -1) <= st["inc_ok"][i] + st["inc_amb"][i])]
        self.check(not bad, f"no increment lost or applied twice (id, n, ok, unknown): {bad[:5]}")
        self.check(st["commit"] > 0 and st["rc_ok"] > 0, "transfers and increments committed")
        self.check(not moving, f"no move left half done: {moving}")
        return self.failures


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True, help="SQL servers, comma-separated (the last one moves the buckets)")
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--threads", type=int, default=6)
    ap.add_argument("--accounts", type=int, default=24)
    ap.add_argument("--init", type=int, default=1000)
    ap.add_argument("--counters", type=int, default=8)
    ap.add_argument("--move-pause", type=float, default=0.05)
    ap.add_argument("--min-moves", type=int, default=10)
    ap.add_argument("--no-moves", action="store_true", help="the same workload without moves (a baseline)")
    ap.add_argument("--settle", type=float, default=90,
                    help="seconds to wait at the end for moves left half done to be finished")
    args = ap.parse_args()
    failures = Test(args).run()
    print("PASSED" if failures == 0 else f"{failures} FAILED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
