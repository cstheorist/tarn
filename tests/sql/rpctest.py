#!/usr/bin/env python3
"""Many MySQL sessions, bounded RPC scopes, silent established connections.

Every mutation is submitted exactly once. Admission refusals must have no
row effect; enqueued calls with lost replies stay 1180/40003, never replayed.
"""
import argparse
import concurrent.futures
import json
import pathlib
import threading
import time

import pymysql


def q(c, sql):
    with c.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall() if cur.description else cur.rowcount


def rpc_rows(c):
    with c.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SHOW TARN RPCS")
        return cur.fetchall()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True)
    ap.add_argument("--config", required=True)
    ap.add_argument("--arm", required=True)
    ap.add_argument("--scope", choices=("connection", "shard", "global"), required=True)
    args = ap.parse_args()
    host, port = args.tarn.split(":")
    config = json.loads(pathlib.Path(args.config).read_text())["rpc"]

    def connect():
        return pymysql.connect(host=host, port=int(port), user="sb", password="", autocommit=True,
                               charset="utf8mb4", read_timeout=8, write_timeout=8)

    observer = connect()
    db = "rpctest_" + args.scope
    q(observer, "DROP DATABASE IF EXISTS " + db)
    q(observer, "CREATE DATABASE " + db)
    q(observer, f"CREATE TABLE {db}.t(id INT PRIMARY KEY,v INT)")
    count = 24
    q(observer, f"INSERT INTO {db}.t VALUES" + ",".join(f"({i},0)" for i in range(count)))
    clients = [connect() for _ in range(count)]
    barrier = threading.Barrier(count + 1)
    capture = threading.local()
    original_error = pymysql.err.raise_mysql_exception

    def error(data):
        capture.sqlstate = data[4:9].decode("ascii") if len(data) >= 9 and data[3:4] == b"#" else None
        original_error(data)

    def mutate(i):
        barrier.wait(timeout=8)
        start = time.monotonic()
        capture.sqlstate = None
        try:
            # Do not retry this statement on any error.
            q(clients[i], f"UPDATE {db}.t SET v=v+1 WHERE id={i}")
            return i, 0, None, time.monotonic() - start, ""
        except pymysql.MySQLError as e:
            return i, e.args[0], capture.sqlstate, time.monotonic() - start, str(e)

    samples = []
    arm = pathlib.Path(args.arm)
    try:
        pymysql.err.raise_mysql_exception = error
        arm.touch()
        with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
            futures = [pool.submit(mutate, i) for i in range(count)]
            barrier.wait(timeout=8)
            deadline = time.monotonic() + 6
            while not all(f.done() for f in futures):
                samples.extend(rpc_rows(observer))
                assert time.monotonic() < deadline, "connected blackhole did not retire by deadline"
                time.sleep(.01)
            outcomes = [f.result() for f in futures]
        arm.unlink()
        samples.extend(rpc_rows(observer))
        refused = [o for o in outcomes if o[1:3] == (1205, "HY000")]
        unknown = [o for o in outcomes if o[1:3] == (1180, "40003")]
        assert refused and unknown and len(refused) + len(unknown) == count, outcomes
        assert all("admission refused before enqueue" in o[4] for o in refused), refused
        assert any(f"({args.scope} limit)" in o[4] for o in refused), refused
        assert max(o[3] for o in outcomes) < 5, outcomes
        for row in samples:
            limit = config[row["Scope"]]
            assert row["Pending"] <= limit["requests"], row
            assert row["Charged_bytes"] <= limit["bytes"], row
            assert row["Peak_pending"] <= limit["requests"], row
            assert row["Peak_bytes"] <= limit["bytes"], row
            assert row["Normal_pending"] <= limit["requests"] - limit["reserved_requests"], row
            if row["Wire_bytes"] is not None:
                assert row["Wire_bytes"] <= row["Charged_bytes"], row
            assert row["Observation_drops"] == 0, row
        assert any(r["Refusals"] > 0 and r["Scope"] == args.scope for r in samples), samples
        assert any(r["Scope"] == "global" and r["Deadline_retirements"] > 0 for r in samples), samples
        assert any((r["Oldest_ms"] or 0) >= 100 for r in samples), samples
        values = dict(q(observer, f"SELECT id,v FROM {db}.t ORDER BY id"))
        for i, code, state, elapsed, message in outcomes:
            if code == 1205:
                assert values[i] == 0, (i, code, state, values[i])
            else:
                assert values[i] in (0, 1), (i, code, state, values[i])
        assert sum(values.values()) > 0, "no executed mutation witnessed before reply withholding"
        print(f"{args.scope}: {count} sessions, {len(refused)} before-enqueue refusals, {len(unknown)} uncertain; "
              f"{sum(values.values())} physical increments, max response {max(o[3] for o in outcomes):.3f}s")
        print("RPC PRESSURE TEST PASSED: scope bounds, deadline retirement, no replay, before-effect refusals")
    finally:
        arm.unlink(missing_ok=True)
        pymysql.err.raise_mysql_exception = original_error
        for c in clients:
            c.close()
        q(observer, "DROP DATABASE IF EXISTS " + db)
        observer.close()


if __name__ == "__main__":
    main()
