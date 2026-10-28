#!/usr/bin/env python3
"""Repeated matched transport-only SELECT microbenchmark of RPC admission.

Build the existing IPROTO bench against exact control/candidate revisions
first. The candidate enables connection + shared shard/global limits; the
old control ignores the same environment flag. Both use the same phase,
init.lua fixture, task/connection counts, compiler and machine. Each sample
owns a fresh one-node Tarantool fixture. No SQL/HA/durability claim.
"""
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import re
import statistics
import subprocess


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--control", required=True)
    ap.add_argument("--candidate", required=True)
    ap.add_argument("--control-revision", required=True)
    ap.add_argument("--candidate-revision", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--seconds", type=int, default=8)
    ap.add_argument("--repetitions", type=int, default=5)
    args = ap.parse_args()
    doc = {"format": "tarn-rpc-admission-micro-v1", "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           "environment": {"rust": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                           "tarantool": subprocess.check_output(["tarantool", "--version"], text=True).splitlines()[0],
                           "kernel": subprocess.check_output(["uname", "-a"], text=True).strip(),
                           "cpu_limit": pathlib.Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
                           "memory_limit": pathlib.Path("/sys/fs/cgroup/memory.max").read_text().strip()},
           "profile": {"transport": "local TCP", "shards": 1, "replicas": 1, "engine": "memtx", "mvcc": True,
                       "mode": "select", "tasks": 64, "connections": 4, "rows": 1000, "seconds": args.seconds,
                       "fixture": "crates/tarn-iproto/tests/common/init.lua", "wal": "Tarantool default; read-only timed phase",
                       "fixture_sha256": sha("crates/tarn-iproto/tests/common/init.lua"),
                       "compiler_profile": "release, thin LTO, codegen-units=4, debug=1, locked dependencies",
                       "host_isolation": "shared Docker VM; no concurrent task-owned validation or builds during measurement"},
           "builds": {"before": {"revision": args.control_revision, "binary": args.control, "sha256": sha(args.control)},
                      "after": {"revision": args.candidate_revision, "binary": args.candidate, "sha256": sha(args.candidate)}},
           "limits": ["Component transport microbenchmark; no MySQL query, transaction, RF3 or durability comparison.",
                      "Fixture/driver check row count and decoding; key/value correctness has separate integration tests.",
                      "Shared host, rounded microsecond latencies, fixed sample duration and finite repetitions limit inference."],
           "samples": []}
    env = dict(os.environ, TARN_BENCH_SHARED_LIMITS="1")
    output = pathlib.Path(args.output)
    for rep in range(args.repetitions):
        order = ("before", "after") if rep % 2 == 0 else ("after", "before")
        for position, variant in enumerate(order):
            binary = args.control if variant == "before" else args.candidate
            cmd = [binary, "--secs", str(args.seconds), "--tasks", "64", "--conns", "4", "--rows", "1000", "--mode", "select"]
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True, timeout=args.seconds + 60)
            raw = result.stdout + result.stderr
            match = re.search(r"select:\s+(\d+) ops in ([\d.]+)s =\s+([\d.]+) ops/s \| latency us p50\s+(\d+) p99\s+(\d+) p99.9\s+(\d+) max\s+(\d+)", raw)
            assert match, raw
            cpu = re.search(r"cpu: client ([\d.]+) cores, ([\d.]+) us/op \| tarantool ([\d.]+) cores, ([\d.]+) us/op", raw)
            assert cpu, raw
            assert variant != "after" or "admission profile: connection + shared shard + shared global" in raw
            sample = {"rep": rep + 1, "position": position + 1, "variant": variant, "command": cmd,
                      "ops": int(match[1]), "elapsed_s_rounded": float(match[2]), "ops_s": float(match[3]),
                      "p50_us": int(match[4]), "p99_us": int(match[5]), "p999_us": int(match[6]), "max_us": int(match[7]),
                      "client_cores": float(cpu[1]), "client_us_op": float(cpu[2]),
                      "server_cores": float(cpu[3]), "server_us_op": float(cpu[4]), "raw_output": raw}
            doc["samples"].append(sample)
            output.write_text(json.dumps(doc, indent=2) + "\n")
            print(f"rep {rep + 1} {variant}: {sample['ops_s']:.0f} ops/s p99 {sample['p99_us']}us", flush=True)
    doc["summary"] = {variant: {"median_ops_s": statistics.median(s["ops_s"] for s in doc["samples"] if s["variant"] == variant),
                               "min_ops_s": min(s["ops_s"] for s in doc["samples"] if s["variant"] == variant),
                               "max_ops_s": max(s["ops_s"] for s in doc["samples"] if s["variant"] == variant)}
                      for variant in ("before", "after")}
    doc["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    output.write_text(json.dumps(doc, indent=2) + "\n")
    print(json.dumps(doc["summary"], indent=2))


if __name__ == "__main__":
    main()
