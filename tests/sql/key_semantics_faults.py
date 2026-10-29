#!/usr/bin/env python3
"""Owned RF3 leader loss, exact unique keys, move and restart; no mutation retry."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time

import pymysql
import tarantool

from key_semantics import Client, normalize
from movetest import bucket_of


def ev(address, code):
    host, port = address.rsplit(":", 1)
    c = tarantool.Connection(host, int(port), user="tarn", password="tarn", fetch_schema=False,
                             connection_timeout=3, socket_timeout=5)
    try:
        return c.eval(code).data
    finally:
        c.close()


def leaders(shards):
    result = []
    for shard in shards:
        found = []
        for address in shard["replicas"]:
            try:
                info = ev(address, "return {ro=box.info.ro,pid=box.info.pid,term=box.info.election.term,native=tarn_info().native}")[0]
                if not info["ro"]:
                    found.append((address, info))
            except (tarantool.Error, OSError):
                pass
        assert len(found) == 1, found
        result.append(found[0])
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tarn", required=True)
    p.add_argument("--cluster-dir", required=True)
    p.add_argument("--engine", choices=["memtx", "vinyl"], required=True)
    p.add_argument("--native", choices=["0", "1"], required=True)
    p.add_argument("--output", required=True)
    args = p.parse_args()
    directory = Path(args.cluster_dir).resolve()
    config = json.loads((directory / "cluster.json").read_text())
    assert len(config["shards"]) == 2 and all(len(s["replicas"]) == 3 for s in config["shards"])
    report = {"config": config, "engine": args.engine, "attempts": [], "events": []}
    lock = threading.Lock()
    threads = []
    try:
        initial = leaders(config["shards"])
        report["initial_leaders"] = initial
        assert all(bool(info["native"]) == (args.native == "1") for _, info in initial), initial
        c = Client(args.tarn, "tarn", "pymysql-text", args.engine)
        c.fresh("fault_keys", "r INT,id INT,d DECIMAL(30,2),k VARCHAR(32),PRIMARY KEY(r,id),UNIQUE KEY ud(d)", "PARTITION BY KEY(r)")

        def writer(worker):
            s = Client(args.tarn, "tarn", "pymysql-text", args.engine)
            try:
                for i in range(32):
                    ident = worker * 100 + i
                    key = f"900719925474{ident:04d}.01"
                    attempt = {"r": worker, "id": ident, "d": key, "k": "a " if i % 2 else "a"}
                    try:
                        s.q(f"INSERT INTO fault_keys VALUES ({worker},{ident},{key},%s)", (attempt["k"],))
                        attempt["outcome"] = "acknowledged"
                    except pymysql.MySQLError as e:
                        attempt.update(outcome="error", code=e.args[0], error=str(e))
                    with lock:
                        report["attempts"].append(attempt)
                    time.sleep(.005)
            except Exception as exc:
                with lock:
                    report.setdefault("worker_errors", []).append(repr(exc))
            finally:
                s.c.close()

        for worker in range(1, 5):
            thread = threading.Thread(target=writer, args=(worker,))
            threads.append(thread)
            thread.start()
        deadline = time.monotonic() + 20
        while sum(a["outcome"] == "acknowledged" for a in report["attempts"]) < 4:
            assert time.monotonic() < deadline, "writers did not start"
            time.sleep(.01)
        assert any(t.is_alive() for t in threads), "fault did not overlap workload"
        address, info = initial[0]
        pid = info["pid"]
        pidfiles = [f for f in directory.glob("s1.*.pid") if int(f.read_text()) == pid]
        assert len(pidfiles) == 1, (pid, pidfiles)
        # Never signal a PID based only on a remote reply or stale pidfile.
        assert Path(f"/proc/{pid}/cwd").resolve().is_relative_to(directory)
        assert b"storage.lua" in Path(f"/proc/{pid}/cmdline").read_bytes()
        report["events"].append({"kill": pid, "address": address, "completed_before": len(report["attempts"]),
                                 "acknowledged_before": sum(a["outcome"] == "acknowledged" for a in report["attempts"]), "time": time.time()})
        os.kill(pid, signal.SIGKILL)
        for thread in threads:
            thread.join(60)
            assert not thread.is_alive(), "writer exceeded deadline"
        assert not report.get("worker_errors"), report.get("worker_errors")
        assert len(report["attempts"]) == 128

        def read_all():
            deadline = time.monotonic() + 60
            while True:
                try:
                    return c.q("SELECT r,id,d,k FROM fault_keys ORDER BY r,id")
                except pymysql.MySQLError as e:
                    report["events"].append({"read_refusal": str(e)})
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(.1)  # only this read-only query is retried

        rows = read_all()
        actual = {(r, i): [d, k] for r, i, d, k in rows}
        assert len(actual) == len(rows)
        for a in report["attempts"]:
            row = actual.get((a["r"], a["id"]))
            expected = [a["d"], a["k"]]
            if a["outcome"] == "acknowledged":
                assert row == expected, (a, row)
            elif a["code"] in (1213, 1412):
                assert row is None, (a, row)
            elif a["code"] in (1180, 2003, 2006, 2013):
                assert row is None or row == expected, (a, row)
            else:
                raise AssertionError(("unexpected write outcome", a))
        assert len(actual) == sum((a["r"], a["id"]) in actual for a in report["attempts"])
        report["after_failover"] = rows

        # Recovered global uniqueness must distinguish adjacent values that
        # the original f64 comparator collapsed, while rejecting duplicates.
        for i, key in [(9001, "9007199254740992.01"), (9002, "9007199254740992.02")]:
            c.q(f"INSERT INTO fault_keys VALUES (1,{i},{key},'é')")
            assert c.error(f"INSERT INTO fault_keys VALUES (2,{i},{key},'e')") == 1062
        expected = read_all()
        count = config.get("bucket_count", 30000)
        bucket = bucket_of(1, count)
        # Discover current owner directly, then move that exact populated
        # bucket. Never infer ownership from the initial modulo placement.
        before = leaders(config["shards"])
        owner = None
        for shard, (node, _) in zip(config["shards"], before):
            if ev(node, f"return box.space._tarn_buckets:get{{{bucket}}} ~= nil")[0]:
                owner = shard["id"]
        assert owner in (1, 2)
        target = 3 - owner
        c.q(f"TARN MOVE BUCKET {bucket} TO SHARD {target}")
        assert read_all() == expected
        report["events"].append({"move": bucket, "from": owner, "to": target})

        instance = pidfiles[0].stem
        subprocess.run([str(Path(__file__).resolve().parents[2] / "scripts/cluster.sh"), "restart", "-d", str(directory), instance], check=True, timeout=45)
        assert read_all() == expected
        report["after_restart"] = expected
        report["final_leaders"] = leaders(config["shards"])
        report["raw_leaders"] = [ev(node, "local out={} for _,t in box.space['key_contracts.fault_keys']:pairs() do out[#out+1]={t[1],t[2],tostring(t[3]),t[4],t[5]} end return out")[0]
                                 for node, _ in report["final_leaders"]]
        raw = sorted([r[:4] for group in report["raw_leaders"] for r in group])
        assert raw == expected, (raw, expected)
        report["status"] = "passed"
        c.c.close()
    except Exception as exc:
        report.update(status="failed", error=repr(exc))
        raise
    finally:
        Path(args.output).write_text(json.dumps(normalize(report), indent=2, default=str) + "\n")


if __name__ == "__main__":
    main()
