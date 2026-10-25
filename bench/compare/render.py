#!/usr/bin/env python3
"""Render the results of bench/compare/run.sh as markdown.

    bench/compare/render.py RESULTS_DIR > summary.md
"""
import csv
import os
import sys

res = sys.argv[1]


def rows(name):
    path = os.path.join(res, name)
    return list(csv.DictReader(open(path))) if os.path.exists(path) else []


def text(name):
    path = os.path.join(res, name)
    return open(path).read().strip() if os.path.exists(path) else ""


NAMES = {
    "mariadb": "MariaDB, 1 node (REPEATABLE READ)",
    "tarn_2x1": "Tarn, 2 shards x 1 (strict serializable)",
    "tarn_2x1_rc": "Tarn, 2 shards x 1 (READ COMMITTED)",
    "tarn_2x3": "Tarn, 2 shards x 3 sync replicas (strict serializable)",
    "tarn_2x1_vinyl": "Tarn, 2 shards x 1, vinyl, cache larger than the data",
    "tarn_2x1_vinyl_10x": "Tarn, 2 shards x 1, vinyl, data 10x the cache",
    "tidb_rf1": "TiDB, 1 TiKV (snapshot isolation)",
    "tidb_rf1_rc": "TiDB, 1 TiKV (READ COMMITTED)",
    "tidb_rf3": "TiDB, 3 TiKV, 3 replicas (snapshot isolation)",
    "tidb_rf1_10x": "TiDB, 1 TiKV, data 10x the block cache",
    "yb_rf1": "YugabyteDB, 1 node (default isolation)",
    "yb_rf1_serializable": "YugabyteDB, 1 node (SERIALIZABLE)",
    "yb_rf3": "YugabyteDB, 3 nodes, 3 replicas (default isolation)",
    "yb_rf1_colocated": "YugabyteDB, 1 node, colocated database",
}
ORDER = list(NAMES)


def name(label):
    return NAMES.get(label, label)


def num(v, digits=0):
    try:
        return f"{float(v):,.{digits}f}"
    except (TypeError, ValueError):
        return "–"


out = ["# Tarn vs TiDB vs YugabyteDB: same-host results", ""]
env = text("environment.txt")
ver = text("versions.txt")
if env or ver:
    out += ["```", env, ver, "```", ""]

configs = rows("configs.csv") if os.path.exists(os.path.join(res, "configs.csv")) else []
sb = rows("results.csv")
last = {}
for r in sb:
    last[(r["label"], r["test"], r["threads"])] = r
labels = sorted({k[0] for k in last}, key=lambda x: ORDER.index(x) if x in ORDER else 99)
tests = []
for k in last:
    if k[1] not in tests:
        tests.append(k[1])
threads = sorted({int(k[2]) for k in last})
# The latency column: lat_p95_ms, or lat_p99_ms with PERCENTILE=99.
pcol = next((c for c in (sb[0].keys() if sb else []) if c.startswith("lat_p") and c.endswith("_ms")), "lat_p95_ms")
pname = pcol[len("lat_"):-len("_ms")]

for t in tests:
    unit = "QPS" if t == "oltp_point_select" else "TPS"
    out.append(f"## {t}")
    out.append("")
    head = "| System | " + " | ".join(f"{th} thr {unit}" for th in threads) + " | " + \
        " | ".join(f"{th} thr {pname} ms" for th in threads) + " | errors (max) |"
    out.append(head)
    out.append("|---|" + "---:|" * (2 * len(threads) + 1))
    for lab in labels:
        cells, lat, errs = [], [], []
        if not any((lab, t, str(th)) in last for th in threads):
            continue
        for th in threads:
            r = last.get((lab, t, str(th)))
            v = (r["qps"] if unit == "QPS" else r["tps"]) if r else None
            cells.append(num(v))
            lat.append(num(r.get(pcol), 2) if r else "–")
            errs.append(int(float(r["errors"] or 0)) if r and r["errors"] else 0)
        out.append(f"| {name(lab)} | " + " | ".join(cells) + " | " + " | ".join(lat) + f" | {max(errs) if errs else 0:,} |")
    out.append("")

prep = rows("prepare.csv")
if prep:
    out += ["## Data load (sysbench prepare)", "", "| System | rows | seconds | rows/s |", "|---|---:|---:|---:|"]
    for r in prep:
        n = int(r["tables"]) * int(r["size"])
        s = float(r["seconds"])
        out.append(f"| {name(r['label'])} | {n:,} | {s:,.1f} | {n / s:,.0f} |")
    out.append("")

vy = rows("vinyl.csv")
if vy:
    out += ["## Vinyl sizes (MB, per storage node)", "",
            "after `.load`: loaded and checkpointed; after `.end`: after the workloads", "",
            "| When | node | data | indexes | cache limit | cache used | in-memory level | page index | bloom |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in vy:
        out.append(f"| {r['label']} | {r['node']} | {r['data_mb']} | {r['index_mb']} | {r['cache_limit_mb']} | "
                   f"{r['cache_mb']} | {r['level0_mb']} | {r['page_index_mb']} | {r['bloom_mb']} |")
    out.append("")

many = rows("many.csv")
if many:
    m = {}
    for r in many:
        m.setdefault(r["label"], {})[r["metric"]] = r["value"]
    out += ["## Many tables", "",
            "| System | tables created | DDL seconds | first 100 | last 100 | MB per 1000 tables | "
            "load s | 1 thr QPS | 16 thr QPS | 16 thr p95 ms |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for lab, d in m.items():
        try:
            created = int(d.get("tables_created", 0))
            mb = (int(d.get("rss_after_ddl_kb", 0)) - int(d.get("rss_before_kb", 0))) / 1024
            mb_per_k = f"{mb / created * 1000:,.0f}" if created else "–"
        except ValueError:
            mb_per_k = "–"
        out.append(f"| {name(lab)} | {d.get('tables_created', '–')} | {num(d.get('ddl_total_seconds'), 1)} | "
                   f"{num(d.get('ddl_first_100_seconds'), 1)} | {num(d.get('ddl_last_100_seconds'), 1)} | {mb_per_k} | "
                   f"{num(d.get('load_seconds'), 1)} | {num(d.get('select_1thr_qps'))} | "
                   f"{num(d.get('select_16thr_qps'))} | {num(d.get('select_16thr_p95_ms'), 2)} |")
    out.append("")

if configs:
    out += ["## Configurations", "", "| Configuration | status | seconds |", "|---|---|---:|"]
    for r in configs:
        out.append(f"| {r.get('config', '')} | {r.get('status', '')} | {r.get('seconds', '')} |")
print("\n".join(out))
