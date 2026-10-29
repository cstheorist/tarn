#!/usr/bin/env python3
"""Matched same-binary, same-data comparison: hints absent versus ANALYZE.

Run two fresh gateways over the same >=2-shard cluster. No other client may
ANALYZE the control. This is a small read-only join component experiment,
not a production, durable-write, RF3, isolation or competitor comparison.
"""
import argparse
import hashlib
import json
import pathlib
import platform
import secrets
import statistics
import subprocess
import sys
import time

import pymysql


def q(c, sql):
    with c.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall() if cur.description else cur.rowcount


def plan(c, sql):
    return json.loads(q(c, "EXPLAIN FORMAT=JSON " + sql)[0][0])


def counters(c, db):
    with c.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SHOW TARN QUERIES")
        rows = [r for r in cur.fetchall() if r["Database"] == db and r["Query"].startswith("SELECT")]
        assert len(rows) == 1, rows
        return rows[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True, help="fresh control,candidate gateways, same binary/settings/storage")
    ap.add_argument("--binary", required=True, help="the exact binary used by BOTH gateways")
    ap.add_argument("--environment", required=True, help="JSON with compiler/build/storage/resources/settings evidence")
    ap.add_argument("--output", required=True)
    ap.add_argument("--shape", choices=["inner_unique", "inner_prefix"], default="inner_unique")
    ap.add_argument("--binary-source-revision", help="defaults to HEAD; compiled Rust/Cargo inputs must match it")
    ap.add_argument("--rows", type=int, default=8192)
    ap.add_argument("--seconds", type=float, default=4)
    ap.add_argument("--repetitions", type=int, default=5)
    args = ap.parse_args()
    assert args.rows >= 1024 and args.seconds > 0 and args.repetitions >= 3
    binary = pathlib.Path(args.binary)
    root = pathlib.Path(__file__).resolve().parents[1]
    assert not subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=root).strip(), "commit measured source first"
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    binary_revision = args.binary_source_revision or revision
    subprocess.run(["git","diff","--exit-code",binary_revision,revision,"--","Cargo.toml","Cargo.lock","rust-toolchain.toml","crates"],cwd=root,check=True,stdout=subprocess.DEVNULL)
    record = {"format": "tarn-join-statistics-v1", "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "source_revision": binary_revision, "driver_source_revision": revision, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "driver_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
              "host": platform.platform(), "command": sys.argv, "environment": json.loads(pathlib.Path(args.environment).read_text()),
              "settings": vars(args), "samples": [],
              "boundary": "same binary/fixture; gateway-local hints absent vs present; RF1 read-only component; shared host uncontrolled; all samples retained"}
    out = pathlib.Path(args.output)
    def save():
        out.write_text(json.dumps(record, indent=2) + "\n")
    db = "join_stats_" + secrets.token_hex(6)
    connections = []
    for address in args.tarn.split(","):
        host, port = address.split(":")
        connections.append(pymysql.connect(host=host, port=int(port), user="sb", password="", autocommit=True))
    assert len(connections) == 2
    control, candidate = connections
    try:
        q(control, "CREATE DATABASE " + db)
        for c in connections:
            c.select_db(db)
        schema = ["CREATE TABLE a(id INT PRIMARY KEY,k INT,KEY k(k))",
                  "CREATE TABLE b(id INT PRIMARY KEY,a_id INT,KEY a_id(a_id))"]
        for sql in schema:
            q(control, sql)
        for start in range(1, args.rows + 1, 256):
            q(control, "INSERT INTO a VALUES" + ",".join(f"({i},{i%20})" for i in range(start, min(start+256,args.rows+1))))
        q(control, "INSERT INTO b VALUES" + ",".join(f"({i},777)" for i in range(1,21)))
        sql = "SELECT a.id,b.id FROM a JOIN b ON a.id=b.a_id WHERE b.id=7 ORDER BY a.id,b.id"
        if args.shape == "inner_prefix":
            sql = sql.replace("b.id=7", "b.a_id=777")
        expected = ((777,7),) if args.shape == "inner_unique" else tuple((777,i) for i in range(1,21))
        record["workload"] = {"schema": schema, "rows_a": args.rows, "rows_b":20, "query":sql,
                              "result": expected, "distribution":"unique outer PKs, 20 repeating outer integer prefix values; 20 inner rows refer to the same outer key"}
        q(candidate, "ANALYZE TABLE a")
        q(candidate, "ANALYZE TABLE b")
        record["plans"] = {"control":plan(control,sql), "candidate":plan(candidate,sql)}
        assert record["plans"]["control"]["plan"]["join_estimate"] is None
        assert record["plans"]["candidate"]["plan"]["join_estimate"]["reordered"] is True
        for c in connections:
            for _ in range(10):
                assert q(c,sql) == expected
        save()
        for rep in range(args.repetitions):
            order = [("control",control),("candidate",candidate)]
            if rep%2:
                order.reverse()
            for label,c in order:
                before = counters(c,db)
                times = []
                start = time.perf_counter()
                while time.perf_counter()-start < args.seconds:
                    begin = time.perf_counter_ns()
                    assert q(c,sql) == expected
                    times.append((time.perf_counter_ns()-begin)/1e6)
                elapsed = time.perf_counter()-start
                after = counters(c,db)
                delta = {k:after[k]-before[k] for k in ("Calls","Errors","Read_attempts","Storage_rows","Total_us")}
                assert delta["Calls"] == len(times) and delta["Errors"] == 0, delta
                sorted_times = sorted(times)
                sample = {"repetition":rep+1,"label":label,"calls":len(times),"elapsed_s":elapsed,
                          "qps":len(times)/elapsed,"latency_ms":times,"median_ms":statistics.median(times),
                          "p99_ms":sorted_times[min(len(times)-1,int(len(times)*.99))],"maximum_ms":max(times),
                          "foreground_deltas":delta}
                record["samples"].append(sample)
                save()
                print(label,rep+1,"qps",round(sample["qps"],2),"median_ms",round(sample["median_ms"],3),
                      "storage_rows/query",delta["Storage_rows"]/len(times),"read_attempts/query",delta["Read_attempts"]/len(times),flush=True)
        record["summary"] = {label:{"qps_median":statistics.median(s["qps"] for s in record["samples"] if s["label"]==label),
                                    "qps_range":[min(s["qps"] for s in record["samples"] if s["label"]==label),max(s["qps"] for s in record["samples"] if s["label"]==label)]}
                             for label in ("control","candidate")}
        record["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())
        save()
    finally:
        q(control,"DROP DATABASE " + db)
        for c in connections:
            c.close()


if __name__ == "__main__":
    main()
