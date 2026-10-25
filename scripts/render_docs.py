#!/usr/bin/env python3
"""Fill generated sections of docs/design.md: <!-- TLC --> (model-checking table
from tla/out) and <!-- BENCH --> (headline benchmark numbers)."""
import csv
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
design = os.path.join(ROOT, "docs", "design.md")
tlc = subprocess.run([sys.executable, os.path.join(ROOT, "tla", "summarize.py")],
                     capture_output=True, text=True, check=True).stdout.strip()

last = {}
for r in csv.DictReader(open(os.path.join(ROOT, "bench", "results.csv"))):
    last[(r["label"], r["test"], r["threads"])] = r   # later runs replace earlier ones
best = {}
for (label, test, _), r in last.items():
    try:
        v = float(r["tps"])
    except ValueError:
        continue
    best[(label, test)] = max(best.get((label, test), 0.0), v)

def b(label, test):
    v = best.get((label, test))
    return f"{v:,.0f}" if v else "–"

rows = [
    ("oltp_point_select (QPS)", "oltp_point_select"),
    ("oltp_read_only (TPS)", "oltp_read_only"),
    ("oltp_read_write (TPS)", "oltp_read_write"),
    ("oltp_write_only (TPS)", "oltp_write_only"),
    ("oltp_update_index (TPS)", "oltp_update_index"),
    ("oltp_insert (TPS)", "oltp_insert"),
]
lines = ["| Best throughput over 1/16/64 threads | MariaDB 10.11 | MySQL 9.7 | Tarn 2 shards | Tarn 1×3 sync replicas |",
         "|---|---:|---:|---:|---:|"]
for name, t in rows:
    lines.append(f"| {name} | {b('mariadb', t)} | {b('mysql', t)} | {b('tarn_2x1', t)} | {b('tarn_1x3sync', t)} |")
lines.append(f"| direct KV point reads (ops/s) | – | – | {b('tarn_2x1', 'kv_point_read')} | – |")
bench = "\n".join(lines)

md = open(design).read()
md = re.sub(r"<!-- TLC -->.*?(?=\n### 5\.2)", "<!-- TLC -->\n" + tlc + "\n", md, flags=re.S)
md = re.sub(r"<!-- BENCH -->.*?(?=\n## 6\.)", "<!-- BENCH -->\n" + bench + "\n", md, flags=re.S)
open(design, "w").write(md)
print("docs/design.md updated")
