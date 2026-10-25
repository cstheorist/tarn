#!/usr/bin/env python3
"""Fill the <!-- TABLES --> and <!-- COMPARISON --> sections of
bench/RESULTS.md from bench/results.csv."""
import csv
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(HERE, "results.csv")
md_path = os.path.join(HERE, "RESULTS.md")

tables = subprocess.run([sys.executable, os.path.join(HERE, "summarize.py"), csv_path],
                        capture_output=True, text=True, check=True).stdout.strip()

rows = list(csv.DictReader(open(csv_path)))
last = {}
for r in rows:  # later runs of the same (label, test, threads) replace earlier ones
    last[(r["label"], r["test"], r["threads"])] = r
best = {}
for (label, test, _), r in last.items():
    try:
        v = float(r["tps"])
    except ValueError:
        continue
    best[(label, test)] = max(best.get((label, test), 0.0), v)

# Published results (TPS; point select: QPS) and their database vCPUs.
TIDB = {"vcpu": 80, "oltp_point_select": 118651, "oltp_read_write": 3169, "oltp_update_index": 20377,
        "oltp_update_non_index": 34011, "oltp_insert": 37247}
YB = {"vcpu": 24, "oltp_read_only": 4616.32, "oltp_read_write": 245.49, "oltp_update_index": 259.64}
TARN_VCPU = 2

def cell(v, vcpu):
    if v is None:
        return "–", "–"
    return f"{v:,.0f}", f"{v / vcpu:,.0f}"

lines = ["| Workload (best throughput) | TiDB 7.5 (80 vCPU) | per vCPU | YugabyteDB (24 vCPU) | per vCPU "
         "| Tarn 2 shards (2 vCPU) | per vCPU | Tarn 1×3 sync (2 vCPU) | per vCPU | MariaDB 1 node (2 vCPU) | per vCPU |",
         "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
for w in ["oltp_point_select", "oltp_read_only", "oltp_read_write", "oltp_update_index", "oltp_update_non_index", "oltp_insert"]:
    t = cell(TIDB.get(w), TIDB["vcpu"])
    y = cell(YB.get(w), YB["vcpu"])
    a = cell(best.get(("tarn_2x1", w)), TARN_VCPU)
    b = cell(best.get(("tarn_1x3sync", w)), TARN_VCPU)
    m = cell(best.get(("mariadb", w)), TARN_VCPU)
    lines.append(f"| {w} | {t[0]} | {t[1]} | {y[0]} | {y[1]} | {a[0]} | {a[1]} | {b[0]} | {b[1]} | {m[0]} | {m[1]} |")
kv = best.get(("tarn_2x1", "kv_point_read"))
if kv:
    lines.append(f"| direct KV point read | – | – | – | – | {kv:,.0f} | {kv / TARN_VCPU:,.0f} | – | – | – | – |")
comparison = "\n".join(lines)

md = open(md_path).read()
md = re.sub(r"<!-- TABLES -->.*?(?=\n## Reading the numbers)", "<!-- TABLES -->\n" + tables + "\n", md, flags=re.S)
md = re.sub(r"<!-- COMPARISON -->.*?(?=\nSources:)", "<!-- COMPARISON -->\n" + comparison + "\n", md, flags=re.S)
open(md_path, "w").write(md)
print("RESULTS.md updated")
