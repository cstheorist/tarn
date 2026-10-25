#!/usr/bin/env python3
"""Render bench/results.csv as Markdown tables: one table per workload,
systems as rows, thread counts as columns (throughput and p95 latency)."""
import csv
import os
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.csv")
rows = list(csv.DictReader(open(path)))
# keep the last measurement for each (label, test, threads)
data = {}
for r in rows:
    data[(r["label"], r["test"], int(r["threads"]))] = r
tests = []
for r in rows:
    if r["test"] not in tests:
        tests.append(r["test"])
labels = []
for r in rows:
    if r["label"] not in labels:
        labels.append(r["label"])
threads = sorted({k[2] for k in data})

NAMES = {
    "mariadb": "MariaDB 10.11 (RR)",
    "mariadb_serializable": "MariaDB 10.11 (SERIALIZABLE)",
    "mysql": "MySQL 9.7 (RR)",
    "tarn_2x1": "Tarn 2 shards (strict serializable)",
    "tarn_2x1_ackprepare": "Tarn 2 shards, ack-on-prepare",
    "tarn_1x3sync": "Tarn 1 shard x 3 sync replicas",
}

def fmt(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return "-"
    return f"{v:,.0f}" if v >= 100 else f"{v:.1f}"

for t in tests:
    present = [l for l in labels if any((l, t, th) in data for th in threads)]
    if not present:
        continue
    metric = "ops/s" if t == "kv_point_read" else ("QPS" if t == "oltp_point_select" else "TPS")
    print(f"\n### {t}\n")
    print("| System | " + " | ".join(f"{th} thr {metric}" for th in threads) + " | " +
          " | ".join(f"{th} thr p95 ms" for th in threads) + " | errors (max) |")
    print("|---|" + "---:|" * (2 * len(threads) + 1))
    for l in present:
        vals, lats, errs = [], [], []
        for th in threads:
            r = data.get((l, t, th))
            if r is None:
                vals.append("-"); lats.append("-")
                continue
            vals.append(fmt(r["tps"]))
            try:
                lats.append(f"{float(r['lat_p95_ms']):.2f}")
            except (TypeError, ValueError):
                lats.append("-")
            try:
                errs.append(int(float(r["errors"] or 0)))
            except ValueError:
                pass
        print(f"| {NAMES.get(l, l)} | " + " | ".join(vals) + " | " + " | ".join(lats) + f" | {max(errs) if errs else 0} |")
