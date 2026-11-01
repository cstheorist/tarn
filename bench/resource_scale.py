#!/usr/bin/env python3
"""The same scan, high-fanout join and sort at 1x and 10x data, under fixed
SQL resource budgets, with one and several sessions (#134).

  python3 bench/resource_scale.py --tarn HOST:PORT --cluster-dir DIR
        [--rows 20000] [--scales 1,10] [--sessions 1,8] [--iterations 3]
        [--engine memtx] [--out RESULTS]

DIR is a scripts/cluster.sh cluster whose cluster.json holds the budgets
(TARN_RESOURCE_LIMITS when it started; the defaults of docs/resources.md
otherwise). Before each cell the script restarts the SQL server, so that the
cell's peak RSS (VmHWM) and its resource high-water marks
(Tarn_resource_peak_bytes, Tarn_resource_peak_work) are its own.

At scale S the table a holds S x ROWS rows, with a 180-character column, and b
S x ROWS / 20; a.g and b.g take 50 values and a.h 100; a.h and b.g are
indexed. The three statements:

  scan  SELECT id,v FROM a WHERE v % 10 = 3
        (WHERE evaluated by the SQL server on every row; keeps 1 in 10)
  join  SELECT a.id,b.id FROM a JOIN b ON a.g=b.g WHERE a.h = 1
        (1% of a through its index, each row matching 1 in 50 of b through
        b's index: the result grows with the square of the data, 100x at 10x)
  sort  SELECT id,v FROM a ORDER BY v,id
        (every row of a, sorted by an unindexed column)

Every success is checked against the data's own formula: the row count,
the sum of the ids and, for the sort, the order. A result that differs is a
clipped or wrong result, which fails the study. A refusal must be the
explicit resource error (1041, "Tarn resource limit:"); any other error fails
it too. The output records each cell's outcomes, the budget each refusal
named, latencies, high-water marks and peak RSS; and the budgets in force,
the source revision and the SQL server executable's SHA-256.
"""
import argparse
import collections
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import statistics
import subprocess
import sys
import threading
import time

import pymysql

GROUPS = 50
DB = "resource_scale"


def connect(endpoint, db=DB, read_timeout=600):
    host, port = endpoint.rsplit(":", 1)
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db, autocommit=True,
                           charset="utf8mb4", connect_timeout=5, read_timeout=read_timeout, write_timeout=60)


def q(c, sql, args=None):
    with c.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def v_of(i):
    return i * 7919 % 100003


def expected(op, n):
    """(rows, sum of ids, first row, last row) of the statement over n rows."""
    m = n // 20
    if op == "scan":
        ids = [i for i in range(1, n + 1) if v_of(i) % 10 == 3]
        return len(ids), sum(ids), None, None
    if op == "join":
        per = [0] * GROUPS
        per_sum = [0] * GROUPS
        for j in range(1, m + 1):
            per[j % GROUPS] += 1
            per_sum[j % GROUPS] += j
        rows = total = 0
        for i in range(1, n + 1, 100):  # h = i % 100 = 1
            g = i % GROUPS
            rows += per[g]
            total += i * per[g] + per_sum[g]
        return rows, total, None, None
    order = sorted(range(1, n + 1), key=lambda i: (v_of(i), i))
    return n, n * (n + 1) // 2, (order[0], v_of(order[0])), (order[-1], v_of(order[-1]))


SQL = {
    "scan": "SELECT id,v FROM a WHERE v % 10 = 3",
    "join": "SELECT a.id,b.id FROM a JOIN b ON a.g=b.g WHERE a.h = 1",
    "sort": "SELECT id,v FROM a ORDER BY v,id",
}


def check(op, rows, want):
    count, total, first, last = want
    if len(rows) != count:
        return f"{len(rows)} rows, expected {count}"
    got = sum(r[0] + r[1] for r in rows) if op == "join" else sum(r[0] for r in rows)
    if got != total:
        return f"id sum {got}, expected {total}"
    if op == "sort":
        if tuple(rows[0]) != first or tuple(rows[-1]) != last:
            return f"ends {rows[0]}..{rows[-1]}, expected {first}..{last}"
        if any((rows[k][1], rows[k][0]) > (rows[k + 1][1], rows[k + 1][0]) for k in range(len(rows) - 1)):
            return "out of order"
    return None


def load(tarn, n, engine):
    c = connect(tarn, db=None)
    q(c, f"DROP DATABASE IF EXISTS {DB}")
    q(c, f"CREATE DATABASE {DB}")
    q(c, f"USE {DB}")
    q(c, f"CREATE TABLE a (id INT PRIMARY KEY, g INT NOT NULL, h INT NOT NULL, v INT NOT NULL, "
         f"pad VARCHAR(200) NOT NULL, KEY (h)) ENGINE={engine}")
    q(c, f"CREATE TABLE b (id INT PRIMARY KEY, g INT NOT NULL, w INT NOT NULL, KEY (g)) ENGINE={engine}")

    def insert(sql, rows):
        with c.cursor() as cur:
            for start in range(0, len(rows), 500):
                for attempt in range(40):
                    try:
                        cur.executemany(sql, rows[start:start + 500])
                        break
                    except pymysql.MySQLError as e:  # vinyl admission, before any effect
                        if e.args[0] != 1205 or "Vinyl" not in str(e.args[1]) or attempt == 39:
                            raise
                        time.sleep(min(0.5, 0.05 * (attempt + 1)))
    insert("INSERT INTO a VALUES (%s,%s,%s,%s,%s)",
           [(i, i % GROUPS, i % 100, v_of(i), "x" * 180 + str(i)) for i in range(1, n + 1)])
    insert("INSERT INTO b VALUES (%s,%s,%s)", [(j, j % GROUPS, j) for j in range(1, n // 20 + 1)])
    assert q(c, "SELECT COUNT(*) FROM a")[0][0] == n
    c.close()


class Gateway:
    """The cluster's first SQL server, restarted before each cell."""

    def __init__(self, tarn, cluster):
        self.tarn, self.dir = tarn, Path(cluster)
        self.script = Path(__file__).resolve().parent.parent / "scripts" / "cluster.sh"

    def pid(self):
        # tarn1.sh execs the server, so the shell's pid is the server's.
        return int((self.dir / "tarn1.pid").read_text())

    def restart(self):
        pid = self.pid()
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        for _ in range(200):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            time.sleep(.05)
        else:
            os.kill(pid, signal.SIGKILL)
            time.sleep(.2)
        subprocess.run(["bash", str(self.script), "restart", "-d", str(self.dir), "tarn1"], check=True,
                       stdout=subprocess.DEVNULL)
        deadline = time.monotonic() + 30
        while True:
            try:
                connect(self.tarn).close()
                return
            except pymysql.MySQLError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.2)

    def memory(self):
        status = Path(f"/proc/{self.pid()}/status").read_text()
        fields = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
        return {k: int(fields[k].split()[0]) * 1024 for k in ("VmHWM", "VmRSS")}

    def counters(self):
        with connect(self.tarn) as c:
            return {k: int(v) for k, v in q(c, "SHOW GLOBAL STATUS LIKE 'Tarn_resource%'")}

    def executable(self):
        path = os.readlink(f"/proc/{self.pid()}/exe")
        return {"path": path, "sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest()}


def reason(message):
    """The budget a refusal names: "query memory budget (a + b > c)" -> "query memory budget"."""
    return message.removeprefix("Tarn resource limit:").strip().split(" (")[0]


def source_revision():
    root = Path(__file__).resolve().parent.parent
    run = lambda *a: subprocess.run(["git", "-C", str(root), *a], capture_output=True, text=True).stdout.strip()
    return {"revision": run("rev-parse", "HEAD") or None, "modified": bool(run("status", "--porcelain", "--untracked-files=no"))}


def run_cell(gw, op, n, sessions, iterations):
    gw.restart()
    want = expected(op, n)
    before = gw.counters()
    results, lock = [], threading.Lock()

    def session():
        with connect(gw.tarn) as c:
            for _ in range(iterations):
                start = time.perf_counter()
                try:
                    rows = q(c, SQL[op])
                    outcome = check(op, rows, want)
                    rec = {"outcome": "ok" if outcome is None else "wrong", "rows": len(rows), "detail": outcome}
                except pymysql.MySQLError as e:
                    explicit = e.args[0] == 1041 and str(e.args[1]).startswith("Tarn resource limit:")
                    rec = {"outcome": "refused" if explicit else "error", "error": list(e.args)}
                rec["seconds"] = time.perf_counter() - start
                with lock:
                    results.append(rec)

    started = time.monotonic()
    threads = [threading.Thread(target=session) for _ in range(sessions)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - started
    after, memory = gw.counters(), gw.memory()
    by = {k: sum(1 for r in results if r["outcome"] == k) for k in ("ok", "refused", "wrong", "error")}
    ok = sorted(r["seconds"] for r in results if r["outcome"] == "ok")
    reasons = collections.Counter(reason(r["error"][1]) for r in results if r["outcome"] == "refused")
    return {
        "op": op, "rows": n, "sessions": sessions, "iterations": iterations, "expected_rows": want[0],
        "outcomes": by, "refused_by": dict(sorted(reasons.items())), "elapsed_seconds": round(elapsed, 3),
        "ok_per_second": round(len(ok) / elapsed, 2) if elapsed else None,
        "ok_p50_seconds": round(statistics.median(ok), 4) if ok else None,
        "ok_max_seconds": round(ok[-1], 4) if ok else None,
        "peak_bytes": after.get("Tarn_resource_peak_bytes"), "peak_work": after.get("Tarn_resource_peak_work"),
        "refusals": after.get("Tarn_resource_refusals", 0) - before.get("Tarn_resource_refusals", 0),
        "read_deadlines": after.get("Tarn_resource_read_deadlines", 0) - before.get("Tarn_resource_read_deadlines", 0),
        "gateway_peak_rss_bytes": memory["VmHWM"], "gateway_rss_bytes": memory["VmRSS"],
        "failures": [r for r in results if r["outcome"] in ("wrong", "error")][:3],
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tarn", required=True)
    ap.add_argument("--cluster-dir", required=True)
    ap.add_argument("--rows", type=int, default=20000, help="rows of a at scale 1")
    ap.add_argument("--scales", default="1,10")
    ap.add_argument("--sessions", default="1,8")
    ap.add_argument("--iterations", type=int, default=3)
    ap.add_argument("--engine", choices=("memtx", "vinyl"), default="memtx")
    ap.add_argument("--out", help="results file (bench/results_resource_scale_ENGINE.json)")
    args = ap.parse_args()
    gw = Gateway(args.tarn, args.cluster_dir)
    config = json.loads((Path(args.cluster_dir) / "cluster.json").read_text())
    report = {
        "date": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "engine": args.engine, "rows_at_scale_1": args.rows, "groups": GROUPS, "sql": SQL,
        "resources": config.get("resources", "defaults (docs/resources.md)"),
        "host": {"platform": platform.platform(), "cpus": os.cpu_count()},
        "source": source_revision(), "gateway": gw.executable(), "cells": [],
    }
    failed = False
    for scale in (int(s) for s in args.scales.split(",")):
        n = args.rows * scale
        started = time.monotonic()
        load(args.tarn, n, args.engine)
        print(f"loaded {n} rows in {time.monotonic() - started:.0f} s", flush=True)
        for op in ("scan", "join", "sort"):
            for sessions in (int(s) for s in args.sessions.split(",")):
                cell = run_cell(gw, op, n, sessions, args.iterations)
                cell["scale"] = scale
                report["cells"].append(cell)
                failed |= cell["outcomes"]["wrong"] > 0 or cell["outcomes"]["error"] > 0
                print(json.dumps(cell), flush=True)
    out = args.out or str(Path(__file__).resolve().parent / f"results_resource_scale_{args.engine}.json")
    Path(out).write_text(json.dumps(report, indent=1) + "\n")
    print(f"wrote {out}", flush=True)
    if failed:
        print("FAIL a statement returned a wrong result or an error other than the resource refusal", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
