#!/usr/bin/env python3
"""Goodput, tail latency and memory of one workload with the SQL resource
budgets in force and with them out of reach, under identical admission
(#134's third scale criterion).

  python3 bench/resource_overhead.py --tarn HOST:PORT --cluster-dir DIR
        [--rows 100000] [--sessions 8] [--seconds 20] [--pairs 3]
        [--engine memtx] [--out RESULTS]
  python3 bench/resource_overhead.py --markdown RESULTS...

DIR is a scripts/cluster.sh cluster. The script loads a table, then runs the
same workload in alternating pairs of runs, after restarting the SQL server
with one of two `resources` objects in cluster.json, everything else (RPC
admission and topology settings included) unchanged:

  budgets    the defaults of docs/resources.md
  unbounded  limits no statement reaches (2^40 bytes, 2^60 work)

Each session runs a fixed mix, chosen at random with a seed per session:
50% point reads by primary key, 20% 100-row primary-key ranges, 10%
counts of a secondary-index prefix (about 1,000 rows) with a predicate the
SQL server evaluates, and 20% single-row updates. Every result is checked
(the point and range rows against the data's formula, the counts against
the data's formula, updates by their affected rows), and a refusal or any
other error fails the run.

Each run records completed operations per second (goodput), p50/p99/max
latency, the SQL server's peak RSS (VmHWM) and its idle RSS before the load,
and the budgets' high-water marks. The difference between the peak RSS and
the admitted peak is memory the managed budget does not cover.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import random
import statistics
import sys
import threading
import time

import pymysql

from resource_scale import Gateway, connect, source_revision

DB = "resource_overhead"
GROUPS = 100
UNBOUNDED = {"query_bytes": 1 << 40, "transaction_bytes": 1 << 40, "server_bytes": 1 << 41,
             "query_work": 1 << 60, "server_work": 1 << 61}


def q(c, sql, args=None):
    with c.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall() if cur.description else cur.rowcount


def v_of(i):
    return i * 7919 % 100003


def load(tarn, n, engine):
    c = connect(tarn, db=None)
    q(c, f"DROP DATABASE IF EXISTS {DB}")
    q(c, f"CREATE DATABASE {DB}")
    q(c, f"USE {DB}")
    q(c, f"CREATE TABLE t (id INT PRIMARY KEY, g INT NOT NULL, v INT NOT NULL, u INT NOT NULL, "
         f"pad VARCHAR(100) NOT NULL, KEY (g)) ENGINE={engine}")
    with c.cursor() as cur:
        for start in range(1, n + 1, 500):
            rows = [(i, i % GROUPS, v_of(i), 0, "p" * 90 + str(i)) for i in range(start, min(start + 500, n + 1))]
            for attempt in range(40):
                try:
                    cur.executemany("INSERT INTO t VALUES (%s,%s,%s,%s,%s)", rows)
                    break
                except pymysql.MySQLError as e:  # vinyl admission, before any effect
                    if e.args[0] != 1205 or "Vinyl" not in str(e.args[1]) or attempt == 39:
                        raise
                    time.sleep(min(0.5, 0.05 * (attempt + 1)))
    c.close()


def counts(n):
    """The count of rows of each group whose v % 7 = 3."""
    out = [0] * GROUPS
    for i in range(1, n + 1):
        if v_of(i) % 7 == 3:
            out[i % GROUPS] += 1
    return out


def session(tarn, n, seconds, seed, want, results):
    rng = random.Random(seed)
    lat, ok, errors = [], 0, []
    c = connect(tarn, db=DB)
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        r = rng.random()
        start = time.perf_counter()
        try:
            if r < 0.5:
                i = rng.randint(1, n)
                got = q(c, "SELECT v, g FROM t WHERE id = %s", (i,))
                assert got == ((v_of(i), i % GROUPS),), (i, got)
            elif r < 0.7:
                i = rng.randint(1, n - 99)
                got = q(c, "SELECT id, v FROM t WHERE id BETWEEN %s AND %s ORDER BY id", (i, i + 99))
                assert got == tuple((k, v_of(k)) for k in range(i, i + 100)), i
            elif r < 0.8:
                g = rng.randrange(GROUPS)
                got = q(c, "SELECT COUNT(*) FROM t WHERE g = %s AND v %% 7 = 3", (g,))
                assert got == ((want[g],),), (g, got)
            else:
                i = rng.randint(1, n)
                got = q(c, "UPDATE t SET u = u + 1 WHERE id = %s", (i,))
                assert got == 1, (i, got)
            ok += 1
            lat.append(time.perf_counter() - start)
        except (pymysql.MySQLError, AssertionError) as e:
            errors.append(repr(e)[:300])
            if len(errors) > 20:
                break
    c.close()
    results.append((ok, lat, errors))


def run(gw, cluster, label, resources, n, sessions, seconds, want, base_config):
    config = dict(base_config)
    if resources is not None:
        config["resources"] = resources
    else:
        config.pop("resources", None)
    (Path(cluster) / "cluster.json").write_text(json.dumps(config))
    gw.restart()
    time.sleep(1)
    idle = gw.memory()
    results = []
    threads = [threading.Thread(target=session, args=(gw.tarn, n, seconds, k, want, results)) for k in range(sessions)]
    started = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - started
    status = gw.counters()
    memory = gw.memory()
    lat = sorted(x for _, l, _ in results for x in l)
    ok = sum(o for o, _, _ in results)
    errors = [e for _, _, es in results for e in es]
    pick = lambda p: round(lat[min(len(lat) - 1, int(p * len(lat)))] * 1000, 3) if lat else None
    return {
        "config": label, "sessions": sessions, "seconds": seconds, "operations": ok,
        "goodput_per_second": round(ok / elapsed, 1), "p50_ms": pick(0.5), "p99_ms": pick(0.99),
        "max_ms": round(lat[-1] * 1000, 3) if lat else None, "errors": errors[:5], "error_count": len(errors),
        "gateway_idle_rss_bytes": idle["VmRSS"], "gateway_peak_rss_bytes": memory["VmHWM"],
        "peak_admitted_bytes": status.get("Tarn_resource_peak_bytes"), "peak_work": status.get("Tarn_resource_peak_work"),
        "refusals": status.get("Tarn_resource_refusals", 0),
    }


def markdown(reports):
    out = ["| Engine | Pair | Config | Goodput/s | p50 ms | p99 ms | Max ms | Idle RSS MiB | Peak RSS MiB "
           "| Admitted peak MiB | Refusals |", "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    mib = lambda b: "" if b is None else f"{b / 2**20:.0f}"
    for r in reports:
        for k, c in enumerate(r["runs"]):
            out.append(f"| {r['engine']} | {k // 2 + 1} | {c['config']} | {c['goodput_per_second']:,.1f} | {c['p50_ms']:.2f} "
                       f"| {c['p99_ms']:.2f} | {c['max_ms']:.1f} | {mib(c['gateway_idle_rss_bytes'])} "
                       f"| {mib(c['gateway_peak_rss_bytes'])} | {mib(c['peak_admitted_bytes'])} | {c['refusals']} |")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tarn")
    ap.add_argument("--cluster-dir")
    ap.add_argument("--rows", type=int, default=100000)
    ap.add_argument("--sessions", type=int, default=8)
    ap.add_argument("--seconds", type=int, default=20)
    ap.add_argument("--pairs", type=int, default=3)
    ap.add_argument("--engine", choices=("memtx", "vinyl"), default="memtx")
    ap.add_argument("--out", help="results file (bench/resources/overhead-ENGINE.json)")
    ap.add_argument("--markdown", nargs="+", metavar="RESULTS", help="print the runs of results files and exit")
    args = ap.parse_args()
    if args.markdown:
        print(markdown([json.loads(Path(f).read_text()) for f in args.markdown]))
        return
    if not args.tarn or not args.cluster_dir:
        ap.error("--tarn and --cluster-dir are required")
    cluster = Path(args.cluster_dir)
    base_config = json.loads((cluster / "cluster.json").read_text())
    if "resources" in base_config:
        sys.exit("cluster.json already sets resources: start the cluster without TARN_RESOURCE_LIMITS")
    gw = Gateway(args.tarn, args.cluster_dir)
    report = {
        "date": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "engine": args.engine, "rows": args.rows, "sessions": args.sessions, "seconds": args.seconds,
        "mix": {"point": 0.5, "range_100": 0.2, "index_prefix_count": 0.1, "update": 0.2},
        "configs": {"budgets": "defaults (docs/resources.md)", "unbounded": UNBOUNDED},
        "host": {"platform": platform.platform(), "cpus": os.cpu_count()},
        "source": source_revision(), "gateway": gw.executable(), "runs": [],
    }
    try:
        load(args.tarn, args.rows, args.engine)
        want = counts(args.rows)
        failed = False
        for pair in range(args.pairs):
            order = [("budgets", None), ("unbounded", UNBOUNDED)]
            if pair % 2:
                order.reverse()
            for label, resources in order:
                cell = run(gw, cluster, label, resources, args.rows, args.sessions, args.seconds, want, base_config)
                cell["pair"] = pair + 1
                report["runs"].append(cell)
                failed |= cell["error_count"] > 0
                print(json.dumps(cell), flush=True)
    finally:
        (cluster / "cluster.json").write_text(json.dumps(base_config))
        gw.restart()
    for label in ("budgets", "unbounded"):
        runs = [r for r in report["runs"] if r["config"] == label]
        report.setdefault("summary", {})[label] = {
            "goodput_median": statistics.median(r["goodput_per_second"] for r in runs),
            "p99_ms_median": statistics.median(r["p99_ms"] for r in runs),
            "peak_rss_bytes_max": max(r["gateway_peak_rss_bytes"] for r in runs),
            "peak_admitted_bytes_max": max(r["peak_admitted_bytes"] or 0 for r in runs),
        }
    out = args.out or str(Path(__file__).resolve().parent / "resources" / f"overhead-{args.engine}.json")
    Path(out).write_text(json.dumps(report, indent=1) + "\n")
    print(json.dumps(report["summary"]), flush=True)
    print(f"wrote {out}", flush=True)
    if failed:
        print("FAIL a run had errors", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
