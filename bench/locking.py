#!/usr/bin/env python3
"""Contention benchmark for READ COMMITTED vs SERIALIZABLE (issues #12, #17).

Two workloads, each run at both isolation levels against any MySQL-protocol
server:

* reserve: issue #12's reservation transaction. Each cart takes one free
  unit per item with `SELECT ... ORDER BY unit LIMIT 1 FOR UPDATE SKIP
  LOCKED`, deletes it and records a hold, in one transaction. Carts draw
  their items from 4 items.
* counter: `UPDATE counters SET n = n - 1 WHERE id = ?` in an explicit
  transaction, on 4 hot rows.

Deadlocks / serialization failures (1213) are retried up to 50 times; a
transaction that still fails "gives up".

    bench/locking.py --host 127.0.0.1:4000 [--clients 32] [--seconds 15]
"""
import argparse
import random
import statistics
import threading
import time

import pymysql

DB = "lockbench"
RETRYABLE = (1213, 1205)


def connect(hp, db=DB):
    host, port = hp.split(":")
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db,
                           autocommit=True, charset="utf8mb4")


def q(c, sql, args=None):
    cur = c.cursor()
    cur.execute(sql, args)
    rows = cur.fetchall() if cur.description else cur.rowcount
    cur.close()
    return rows


def setup(hp, units):
    c = connect(hp, db=None)
    q(c, f"DROP DATABASE IF EXISTS {DB}")
    q(c, f"CREATE DATABASE {DB}")
    c.select_db(DB)
    q(c, "CREATE TABLE units (tenant INT, item INT, unit INT, PRIMARY KEY (tenant, item, unit)) PARTITION BY KEY(tenant)")
    q(c, "CREATE TABLE holds (tenant INT, id INT, item INT, unit INT, PRIMARY KEY (tenant, id)) PARTITION BY KEY(tenant)")
    q(c, "CREATE TABLE counters (id INT PRIMARY KEY, n BIGINT)")
    for item in range(4):
        for lo in range(0, units, 1000):
            q(c, "INSERT INTO units VALUES " + ",".join(f"(1, {item}, {u})" for u in range(lo, min(units, lo + 1000))))
    q(c, "INSERT INTO counters VALUES " + ",".join(f"({i}, 1000000000)" for i in range(4)))


def run(hp, level, workload, clients, seconds, items_per_cart):
    stop = time.time() + seconds
    lat, done, retries, gave_up, errors = [], [0], [0], [0], []
    lock = threading.Lock()
    hold_ids = iter(range(1, 1 << 40))
    ids_lock = threading.Lock()

    def one(c, rnd):
        if workload == "counter":
            q(c, "BEGIN")
            q(c, "UPDATE counters SET n = n - 1 WHERE id = %s", (rnd.randrange(4),))
            q(c, "COMMIT")
            return
        cart = rnd.sample(range(4), items_per_cart)
        q(c, "BEGIN")
        for item in cart:
            r = q(c, "SELECT unit FROM units WHERE tenant = 1 AND item = %s ORDER BY unit LIMIT 1 "
                     "FOR UPDATE SKIP LOCKED", (item,))
            if not r:
                continue
            q(c, "DELETE FROM units WHERE tenant = 1 AND item = %s AND unit = %s", (item, r[0][0]))
            with ids_lock:
                hid = next(hold_ids)
            q(c, "INSERT INTO holds VALUES (1, %s, %s, %s)", (hid, item, r[0][0]))
        q(c, "COMMIT")

    def client(n):
        c = connect(hp)
        q(c, f"SET SESSION tx_isolation = '{level}'")
        rnd = random.Random(n)
        while time.time() < stop:
            t0 = time.time()
            ok = False
            for attempt in range(51):
                try:
                    one(c, rnd)
                    ok = True
                    break
                except pymysql.MySQLError as e:
                    try:
                        q(c, "ROLLBACK")
                    except pymysql.MySQLError:
                        pass
                    with lock:
                        if e.args[0] not in RETRYABLE:
                            errors.append(e.args)
                            break
                        if attempt == 50:
                            gave_up[0] += 1
                            break
                        retries[0] += 1
            if ok:
                with lock:
                    done[0] += 1
                    lat.append(time.time() - t0)

    ths = [threading.Thread(target=client, args=(n,)) for n in range(clients)]
    t0 = time.time()
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    el = time.time() - t0
    lat.sort()
    p = (lambda f: lat[min(len(lat) - 1, int(f * len(lat)))] * 1000) if lat else (lambda f: 0)
    return {"tps": done[0] / el, "retries": retries[0], "gave_up": gave_up[0], "errors": errors[:3],
            "p50": p(0.5), "p99": p(0.99), "mean": statistics.mean(lat) * 1000 if lat else 0}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--clients", type=int, default=32)
    ap.add_argument("--seconds", type=float, default=15)
    ap.add_argument("--units", type=int, default=20000, help="units per item")
    ap.add_argument("--levels", default="SERIALIZABLE,READ-COMMITTED")
    args = ap.parse_args()
    print("| Workload | Isolation | Transactions/s | p50 ms | p99 ms | Retried 1213/1205 | Gave up | Other errors |")
    print("|---|---|---:|---:|---:|---:|---:|---|")
    for workload, ipc in (("reserve", 1), ("reserve", 2), ("counter", 0)):
        for level in args.levels.split(","):
            setup(args.host, args.units)
            r = run(args.host, level, workload, args.clients, args.seconds, ipc)
            name = f"reserve, {ipc} item{'s' if ipc > 1 else ''}/cart" if workload == "reserve" else "counter, 4 hot rows"
            print(f"| {name} | {level} | {r['tps']:,.0f} | {r['p50']:.0f} | {r['p99']:.0f} | {r['retries']:,} | "
                  f"{r['gave_up']} | {r['errors'] or ''} |", flush=True)


if __name__ == "__main__":
    main()
