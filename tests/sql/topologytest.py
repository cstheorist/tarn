#!/usr/bin/env python3
"""Source-bound topology resource fixture and matched gateway measurements.

Uses real storage groups from --config; does not simulate groups/replication.
All writes are submitted once. Exact point reads may retry known read errors.
Baseline measurements are optional; neither debug nor one-host runs establish
production throughput or independent failure-domain capacity.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
import pathlib
import subprocess
import time

import pymysql
from movetest import bucket_of


def q(c, sql):
    with c.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall() if cur.description else cur.rowcount


def connect(port):
    return pymysql.connect(host="127.0.0.1", port=port, user="sb", password="", autocommit=True,
                           read_timeout=30, write_timeout=30)


def counters(c):
    return {k: int(v) for k, v in q(c, "SHOW GLOBAL STATUS") if k.startswith((
        "Tarn_storage_", "Tarn_fanout_", "Tarn_response_", "Tarn_rpc_", "Tarn_topology_"))}


def rss(pid):
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True).strip()) * 1024


def percentile(v, fraction):
    return sorted(v)[min(len(v) - 1, int(len(v) * fraction))] if v else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--gateway", required=True)
    ap.add_argument("--baseline")
    ap.add_argument("--out", required=True)
    ap.add_argument("--pairs", type=int, default=5)
    ap.add_argument("--seconds", type=float, default=2)
    ap.add_argument("--port", type=int, default=26400)
    ap.add_argument("--rate", type=float, default=1000, help="total scheduled point reads/s across eight clients")
    ap.add_argument("--stack-bytes", help="explicit development-build stack override; release uses its default")
    ap.add_argument("--pool-size", type=int, default=4)
    ap.add_argument("--idle-seconds", type=float, default=2)
    args = ap.parse_args()
    assert args.pairs > 0 and args.seconds > 0 and args.rate > 0
    out = pathlib.Path(args.out); out.mkdir(parents=True, exist_ok=True)
    cfg = json.loads(pathlib.Path(args.config).read_text())
    groups = len(cfg["shards"])
    count = cfg.get("bucket_count", 2048)
    cfg.update(pool_size=args.pool_size, deadlock_interval=0)
    cfg["topology"] = dict(max_connections=20, completion_connections=4, idle_secs=1,
                           fanout=4, active_branches=16, queued_branches=32,
                           response_bytes=16 << 20, response_frame_bytes=8 << 20,
                           control_reply_bytes=8 << 20, stats_shards_per_tick=4)
    config = out / "cluster.json"; config.write_text(json.dumps(cfg, indent=2))
    keys = {}
    for key in range(1, 1000000):
        shard = (bucket_of(key, count) - 1) * groups // count
        keys.setdefault(shard, key)
        if len(keys) == groups: break
    assert len(keys) == groups
    hot = list(keys.values())[:2]
    initialized = False
    report = dict(groups=groups, replicas=[len(s["replicas"]) for s in cfg["shards"]],
                  config=cfg, samples=[], checks=[], read_refusals=[], completed=False,
                  scheduled_rate=args.rate, stack_override=args.stack_bytes,
                  source_head=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                  source_files={str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                                for root in ("crates", "tests", "lua")
                                for p in pathlib.Path(root).rglob("*") if p.is_file() and p.suffix in (".rs", ".py", ".sh", ".lua", ".toml")},
                  binaries={label: hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()
                            for label, path in [("candidate", args.gateway), ("baseline", args.baseline)] if path})
    report_path = out / "report.json"

    def save(): report_path.write_text(json.dumps(report, indent=2))

    def point(client, key):
        # Rollback is asynchronous. Retiring/pinned sockets can briefly refuse
        # new discovery after a rejected scatter. Only this read is retried;
        # retain each refusal and fail if cleanup does not restore capacity.
        deadline = time.monotonic() + 10
        while True:
            try:
                assert q(client, f"SELECT v FROM topology188.t WHERE id={key}") == ((key,),)
                return
            except pymysql.Error as e:
                report["read_refusals"].append(dict(key=key, error=list(e.args)))
                if e.args[0] != 1205 or time.monotonic() >= deadline: raise
                time.sleep(.02)

    try:
        for pair in range(args.pairs):
            modes = [("candidate", args.gateway)]
            if args.baseline:
                modes = [("baseline", args.baseline), *modes]
                if pair % 2: modes.reverse()
            for mode, binary in modes:
                processes = []; clients = []; logs = []
                try:
                    started = time.monotonic()
                    for gateway in range(2):
                        log = (out / f"{pair}-{mode}-{gateway}.log").open("w"); logs.append(log)
                        p = subprocess.Popen([binary, "--config", str(config), "--listen", f"127.0.0.1:{args.port+gateway}",
                                              "--node-id", str(gateway + 801), "--threads", "2", "--allow-all"],
                                             stdout=log, stderr=log,
                                             env={**os.environ, **({"RUST_MIN_STACK": args.stack_bytes} if args.stack_bytes else {})})
                        processes.append(p)
                    for gateway, p in enumerate(processes):
                        deadline = time.monotonic() + 90
                        while True:
                            assert p.poll() is None, f"gateway exited: {pair} {mode} {gateway}"
                            try:
                                c = connect(args.port + gateway); clients.append(c); break
                            except pymysql.Error:
                                if time.monotonic() > deadline: raise
                                time.sleep(.05)
                    startup = time.monotonic() - started
                    c = clients[0]
                    if not initialized:
                        q(c, "CREATE DATABASE topology188")
                        q(c, "CREATE TABLE topology188.t(id INT PRIMARY KEY,v INT)")
                        for key in keys.values(): q(c, f"INSERT INTO topology188.t VALUES({key},{key})")
                        initialized = True
                    # Exercise every configured group without retaining streams.
                    for client in clients:
                        for key in keys.values():
                            point(client, key)
                    before = [counters(x) for x in clients]
                    load_start = time.monotonic()
                    deadline = load_start + args.seconds

                    def worker(index):
                        c = connect(args.port + index % 2)
                        latencies = []; failures = []; skipped = 0; slot = index
                        try:
                            key = hot[index % len(hot)]
                            while time.monotonic() < deadline:
                                due = load_start + slot / args.rate
                                if due >= deadline: break
                                time.sleep(max(0, due - time.monotonic()))
                                slot += 8
                                t = time.perf_counter()
                                try:
                                    got = q(c, f"SELECT v FROM topology188.t WHERE id={key}")
                                    assert got == ((key,),), got
                                    latencies.append(time.perf_counter() - t)
                                except pymysql.Error as e:
                                    failures.append(list(e.args))
                                # No catch-up burst after a stall. Record missed
                                # scheduled slots instead of hiding lower load.
                                while load_start + slot / args.rate < min(deadline, time.monotonic()):
                                    skipped += 1
                                    slot += 8
                        finally: c.close()
                        return latencies, failures, skipped

                    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                        runs = list(pool.map(worker, range(8)))
                    elapsed = time.monotonic() - load_start
                    latency = [v for values, _, _ in runs for v in values]
                    failures = [e for _, errors, _ in runs for e in errors]
                    after = [counters(x) for x in clients]
                    physical = [sum(row["Closed"] == 0 for row in rpc_rows(x) if row["Scope"] == "connection") for x in clients]
                    sample = dict(pair=pair, mode=mode, startup_seconds=startup, successful_reads=len(latency),
                                  scheduled_seconds=args.seconds, elapsed_seconds=elapsed, goodput=len(latency)/elapsed,
                                  skipped_slots=sum(skipped for _, _, skipped in runs),
                                  p50=percentile(latency, .5), p99=percentile(latency, .99),
                                  errors=failures, rss_bytes=[rss(p.pid) for p in processes],
                                  observed_open_sockets=physical, before=before, after=after)
                    report["samples"].append(sample); save()
                    assert not failures, sample
                    if mode == "candidate":
                        idle_start = time.monotonic()
                        idle_before = [counters(x) for x in clients]
                        time.sleep(args.idle_seconds)
                        idle_after = [counters(x) for x in clients]
                        sample["idle"] = dict(seconds=time.monotonic()-idle_start, before=idle_before, after=idle_after)
                        for status in after:
                            assert status["Tarn_storage_sockets_peak"] <= 20, status
                            assert status["Tarn_response_bytes_peak"] <= 16 << 20, status
                            assert status["Tarn_fanout_peak"] <= 16, status
                        # A scan may be refused if its pinned stream set cannot
                        # fit. Successful results must be complete and exact.
                        try:
                            rows = q(c, "SELECT id,v FROM topology188.t ORDER BY id")
                            assert rows == tuple((k, k) for k in sorted(keys.values())), rows
                            report["checks"].append(dict(pair=pair, scan="complete", rows=len(rows)))
                        except pymysql.Error as e:
                            assert e.args[0] == 1205, e
                            report["checks"].append(dict(pair=pair, scan="explicit_refusal", error=list(e.args)))
                        for key in keys.values():
                            point(c, key)
                    save()
                finally:
                    for c in clients: c.close()
                    for p in processes:
                        if p.poll() is None: p.terminate()
                    for p in processes:
                        try: p.wait(timeout=10)
                        except subprocess.TimeoutExpired: p.kill(); p.wait()
                    for log in logs: log.close()
        report["completed"] = True
    finally:
        save()
    print(json.dumps(dict(report=str(report_path), groups=groups, completed=report["completed"])))


def rpc_rows(c):
    with c.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SHOW TARN RPCS")
        return cur.fetchall()


if __name__ == "__main__": main()
