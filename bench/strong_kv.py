#!/usr/bin/env python3
"""Endpoint-only SQL/strong-KV/raw study. Starts no database services.

SQL B32 is 32 independent autocommit PK reads, not one IN snapshot. The
strong SDK batches its points; raw B32 pipelines 32 IPROTO SELECTs. Protocol
packing and PyMySQL/Rust stacks differ. Raw has weaker guarantees and is a
descriptive reference, never evidence of a strong-read acceleration.

Optional --cpu-observer is a JSON argv array for an already-audited, read-only
observer. It receives one JSON object on stdin (stage/mode/cell/client_pid)
and returns {identity:...,cpu_counters_ns:{name:integer-or-null},...}. Identical
identity and monotonic integer counters permit a delta. Missing CPU is null.
The observer owns its bounded execution/children and must start no services.
"""
import argparse
from array import array
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.metadata
import itertools
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import select
import signal
import subprocess
import sys
import threading
import time

MASK = (1 << 64) - 1
CAP = 250_000


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def keys(state, rows, batch):
    state ^= (state << 13) & MASK
    state ^= state >> 7
    state ^= (state << 17) & MASK
    start = state % (rows - batch + 1) + 1
    return state, list(range(start, start + batch))


def plan(seed):
    orders = list(itertools.permutations(("sql", "strong", "raw")))
    random.Random(seed).shuffle(orders)
    result = []
    for trial, order in enumerate(orders):
        cells = list(itertools.product((1, 32), (1, 16)))
        random.Random(seed + trial * 1009).shuffle(cells)
        for batch, workers in cells:
            for mode in order:
                result.append(dict(trial=trial, mode=mode, batch=batch, workers=workers,
                                   seed=seed + trial * 1_000_000 + batch * 100 + workers))
    return result


def observe(args, cell, stage, pid):
    if args.cpu_observer is None:
        return None
    command = json.loads(args.cpu_observer)
    if not isinstance(command, list) or not command or not all(isinstance(x, str) for x in command):
        raise ValueError("cpu-observer must be a nonempty JSON argv array")
    request = dict(version=1, stage=stage, client_pid=pid, **cell)
    r = subprocess.run(command, input=json.dumps(request), text=True, capture_output=True, timeout=10)
    if r.returncode:
        raise RuntimeError("CPU observer failed: " + r.stderr[:4096])
    value = json.loads(r.stdout)
    if not isinstance(value, dict):
        raise ValueError("CPU observer must return one JSON object")
    return value


def cpu_delta(before, after):
    if before is None or after is None:
        return dict(available=False, cpu_counters_ns=None, reason="no observer")
    if "identity" not in before or before.get("identity") != after.get("identity"):
        raise ValueError("CPU observer identity changed or absent")
    b, a = before.get("cpu_counters_ns"), after.get("cpu_counters_ns")
    if not isinstance(b, dict) or not isinstance(a, dict) or b.keys() != a.keys():
        raise ValueError("CPU observer counter scope changed")
    out = {}
    for key in b:
        if b[key] is None or a[key] is None:
            out[key] = None
        elif type(b[key]) is not int or type(a[key]) is not int or a[key] < b[key]:
            raise ValueError("non-monotonic or non-integer CPU counter")
        else:
            out[key] = a[key] - b[key]
    return dict(available=any(v is not None for v in out.values()), cpu_counters_ns=out)


def line_before(process, deadline):
    left = deadline - time.monotonic()
    if left <= 0 or not select.select([process.stdout], [], [], left)[0]:
        raise TimeoutError("bounded Rust driver gate")
    line = process.stdout.readline()
    if not line:
        raise RuntimeError("Rust driver exited before gate")
    return json.loads(line)


def identity(pid):
    result = {}
    for field in ("ppid", "lstart", "command"):
        r = subprocess.run(["ps", "-ww", "-p", str(pid), "-o", field + "="],
                           capture_output=True, text=True, timeout=3)
        if r.returncode or not r.stdout.strip():
            return None
        result[field] = r.stdout.strip()
    return result


def stop_driver(process, captured):
    # This Rust driver starts no subprocesses. Parent/start/argv are rechecked
    # before every signal, rather than trusting a reusable numeric PID.
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if process.poll() is not None:
            break
        if identity(process.pid) != captured or os.getpgid(process.pid) != process.pid:
            raise RuntimeError("measurement child identity changed; not signalling")
        os.killpg(process.pid, sig)
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass
    process.wait(timeout=1)


def rust_phase(args, root, prefix, cell):
    command = [str(args.driver), "--manifest", str(args.manifest), "--mode", cell["mode"],
               "--batch", str(cell["batch"]), "--workers", str(cell["workers"]),
               "--seconds", str(args.seconds), "--warmup", str(args.warmup),
               "--seed", str(cell["seed"]), "--output", str(prefix)]
    if args.credentials_file:
        command += ["--credentials-file", str(args.credentials_file)]
    record = dict(cell, accepted=False, backend="Rust actual SDK" if cell["mode"] == "strong" else "Rust raw IPROTO",
                  arguments=command, cpu=None, errors=[])
    process = captured = None
    try:
        with prefix.with_suffix(".stderr.log").open("w") as log:
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=log, text=True, start_new_session=True)
            captured = identity(process.pid)
            if captured is None or int(captured["ppid"]) != os.getpid():
                raise RuntimeError("new driver identity unavailable")
            ready = line_before(process, time.monotonic() + args.warmup + 30)
            record["ready"] = ready
            if ready.get("event") != "READY" or ready.get("pid") != process.pid:
                raise RuntimeError("driver did not become READY")
            record["before"] = observe(args, cell, "before", process.pid)
            process.stdin.write("GO\n")
            process.stdin.flush()
            done = line_before(process, time.monotonic() + args.seconds + 30)
            record["done"] = done
            if done.get("event") != "DONE":
                raise RuntimeError("driver did not finish business requests")
            try:
                record["after"] = observe(args, cell, "after", process.pid)
                record["cpu"] = cpu_delta(record["before"], record["after"])
            except BaseException as e:
                # Business requests are finished. Release only the file-output
                # gate even when the observer fails, preserving measured raw
                # samples before rejecting this phase. This is no read retry.
                record["errors"].append("CPU observer: " + type(e).__name__ + ": " + str(e))
            process.stdin.write("SNAPSHOT_DONE\n")
            process.stdin.flush()
            result = line_before(process, time.monotonic() + 10)
            record["client"] = result
            code = process.wait(timeout=5)
            if result.get("event") != "RESULT" or not result.get("accepted") or code or record["errors"]:
                raise RuntimeError("Rust phase rejected; raw client result retained")
            record["accepted"] = True
    except BaseException as e:
        record["errors"].append(type(e).__name__ + ": " + str(e))
    finally:
        if process is not None and captured is not None:
            try:
                stop_driver(process, captured)
                record["driver_exit"] = process.returncode
            except BaseException as e:
                record["errors"].append("driver cleanup: " + str(e))
                record["accepted"] = False
        if process is not None:
            if process.poll() is not None:
                process.wait()
            for stream in (process.stdin, process.stdout):
                if stream is not None:
                    stream.close()
        write(prefix.with_suffix(".phase.json"), record)
    return record


class WireCounter:
    """Count consumed MySQL protocol bytes, excluding authentication/TCP headers.

    PyMySQL private hooks are version-recorded. A failing sendall/read has an
    incomplete byte census; it is never converted to a zero-byte success.
    """
    def __init__(self, connection):
        self.sent = self.received = 0
        self.complete = True
        read, write_bytes = connection._read_bytes, connection._write_bytes

        def counted_read(n):
            try:
                value = read(n)
            except BaseException:
                self.complete = False
                raise
            self.received += len(value)
            return value

        def counted_write(value):
            try:
                result = write_bytes(value)
            except BaseException:
                self.complete = False
                raise
            self.sent += len(value)
            return result

        connection._read_bytes, connection._write_bytes = counted_read, counted_write


def payload_bytes(value):
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise ValueError("unexpected SQL payload type")


def error_argument(value):
    if isinstance(value, bytes):
        return {"binary_hex": value.hex()}
    if isinstance(value, float) and not math.isfinite(value):
        return {"nonfinite": repr(value)}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return {"type": type(value).__name__, "repr": repr(value)}


def sql_phase(args, root, prefix, cell, manifest, pymysql, credentials):
    corpus = manifest["corpus"]
    db, table = corpus["db"], corpus["table"]
    if not all(re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", n) for n in (db, table)):
        raise ValueError("benchmark SQL identifiers require simple ASCII names")
    statement = f"SELECT tenant_id,id,p FROM `{db}`.`{table}` WHERE tenant_id=%s AND id=%s"
    expected = bytes.fromhex(corpus["payload_hex"])
    record = dict(cell, accepted=False, errors=[], cpu=None, backend="PyMySQL", sql_shape="B independent autocommit point statements per operation")
    conns, counters, traces = [], [], []
    query_count = [0] * cell["workers"]
    try:
        host, port = args.sql.rsplit(":", 1)
        for _ in range(cell["workers"]):
            c = pymysql.connect(host=host, port=int(port), user=credentials["user"], password=credentials["password"],
                                autocommit=True, charset="utf8mb4", connect_timeout=3, read_timeout=5, write_timeout=5)
            conns.append(c)
            counters.append(WireCounter(c))

        def one(cursor, worker, ids):
            deadline = time.monotonic() + 5
            for key in ids:
                if time.monotonic() >= deadline:
                    raise TimeoutError("independent SQL point operation exceeded its read budget")
                query_count[worker] += 1
                cursor.execute(statement, (corpus["tenant"], key))
                rows = cursor.fetchall()
                if len(rows) != 1 or rows[0][0] != corpus["tenant"] or rows[0][1] != key or payload_bytes(rows[0][2]) != expected:
                    raise AssertionError("SQL exact tenant/key/payload mismatch")

        with ThreadPoolExecutor(max_workers=cell["workers"]) as pool:
            end = time.monotonic() + args.warmup

            def warm(worker):
                state = (cell["seed"] ^ ((worker + 1) << 32)) or 1
                with conns[worker].cursor() as cursor:
                    while time.monotonic() < end:
                        state, ids = keys(state, corpus["rows"], cell["batch"])
                        one(cursor, worker, ids)

            for f in [pool.submit(warm, i) for i in range(cell["workers"])]:
                f.result()
            record["warmup"] = dict(statements=sum(query_count), bytes_written=sum(c.sent for c in counters),
                                     bytes_read=sum(c.received for c in counters))
            ready, go = threading.Barrier(cell["workers"] + 1), threading.Event()
            deadline = [0.0]

            def worker_run(worker):
                trace = dict(worker=worker, latency=array("Q"), outcomes=bytearray(), error=None)
                traces.append(trace)
                state = (cell["seed"] ^ ((worker + 1) << 32)) or 1
                ready.wait(timeout=10)
                go.wait(timeout=20)
                try:
                    with conns[worker].cursor() as cursor:
                        while time.monotonic() < deadline[0]:
                            if len(trace["latency"]) >= CAP:
                                raise RuntimeError("bounded latency histogram cap reached")
                            state, ids = keys(state, corpus["rows"], cell["batch"])
                            start = time.perf_counter_ns()
                            try:
                                one(cursor, worker, ids)
                            except BaseException:
                                trace["latency"].append(time.perf_counter_ns() - start)
                                trace["outcomes"].append(1)
                                raise
                            trace["latency"].append(time.perf_counter_ns() - start)
                            trace["outcomes"].append(0)
                except BaseException as e:
                    trace["error"] = dict(type=type(e).__name__, args=[error_argument(v) for v in e.args])

            futures = [pool.submit(worker_run, i) for i in range(cell["workers"])]
            ready.wait(timeout=10)
            record["before"] = observe(args, cell, "before", os.getpid())
            counts0 = (sum(c.sent for c in counters), sum(c.received for c in counters), sum(query_count))
            cpu0, start = time.process_time(), time.monotonic()
            deadline[0] = start + args.seconds
            go.set()
            for f in futures:
                f.result(timeout=args.seconds + 10)
            elapsed = time.monotonic() - start
            record["client_cpu_seconds"] = time.process_time() - cpu0
            record["after"] = observe(args, cell, "after", os.getpid())
            record["cpu"] = cpu_delta(record["before"], record["after"])
        lat = sorted(n for t in traces for n, outcome in zip(t["latency"], t["outcomes"]) if outcome == 0)
        attempted = sum(len(t["latency"]) for t in traces)
        q = lambda p: lat[int((len(lat) - 1) * p)] if lat else None
        record.update(attempted_operations=attempted, successful_operations=len(lat), attempted_keys=attempted * cell["batch"],
                      successful_keys=len(lat) * cell["batch"], returned_rows=len(lat) * cell["batch"], elapsed_seconds=elapsed,
                      operations_per_second=len(lat) / elapsed, p50_ns=q(.5), p99_ns=q(.99),
                      protocol=dict(complete=all(c.complete for c in counters), statements=sum(query_count) - counts0[2],
                                    bytes_written=sum(c.sent for c in counters) - counts0[0], bytes_read=sum(c.received for c in counters) - counts0[1],
                                    scope="consumed MySQL protocol bytes through version-recorded PyMySQL hooks; not TCP headers"))
        if any(t["error"] for t in traces) or any(not t["latency"] for t in traces) or not lat or not record["protocol"]["complete"]:
            raise AssertionError("SQL phase error/empty worker/incomplete bytes")
        if record["protocol"]["statements"] != len(lat) * cell["batch"]:
            raise AssertionError("SQL statement census differs from successful point count")
        if record["protocol"]["bytes_written"] <= 0 or record["protocol"]["bytes_read"] <= 0:
            raise AssertionError("SQL protocol byte hooks did not observe actual request/reply bytes")
        record["accepted"] = True
    except BaseException as e:
        record["errors"].append(type(e).__name__ + ": " + str(e))
    finally:
        record["raw"] = []
        for trace in sorted(traces, key=lambda t: t["worker"]):
            path = prefix.with_suffix(f".worker{trace['worker']}.latency.u64le")
            values = trace["latency"]
            if sys.byteorder != "little":
                values.byteswap()
            path.write_bytes(values.tobytes())
            outcome = prefix.with_suffix(f".worker{trace['worker']}.outcome.u8")
            outcome.write_bytes(trace["outcomes"])
            record["raw"].append(dict(worker=trace["worker"], samples=len(values), latency=path.name, outcome=outcome.name,
                                       latency_sha256=digest(path), outcome_sha256=digest(outcome), error=trace["error"]))
        for c in conns:
            try:
                c.close()
            except BaseException as e:
                record["errors"].append("connection cleanup: " + str(e))
                record["accepted"] = False
        write(prefix.with_suffix(".phase.json"), record)
    return record


def run(args):
    if args.python_path:
        sys.path.insert(0, str(args.python_path))
    import pymysql
    manifest = json.loads(args.manifest.read_text())
    if manifest.get("version") != 1:
        raise ValueError("unsupported corpus manifest")
    with args.sql_credentials_file.open() as f:
        credentials = json.load(f)
    if set(credentials) != {"user", "password"} or not all(isinstance(v, str) for v in credentials.values()):
        raise ValueError("invalid SQL credentials file")
    root = args.output
    root.mkdir(parents=True, exist_ok=False)
    study = dict(version=1, accepted=False, source=manifest["source"], manifest_sha256=digest(args.manifest),
                 controller_sha256=digest(__file__), driver_sha256=digest(args.driver), observer_argv=args.cpu_observer,
                 python=platform.python_version(), pymysql_distribution=importlib.metadata.version("PyMySQL"),
                 pymysql_module=pymysql.__version__, seconds=args.seconds, warmup=args.warmup,
                 load_model="closed-loop fixed concurrency; latency includes decoding/validation and excludes key generation",
                 plan=plan(args.seed), phases=[], limitations=[
                     "independent point operation; B32 SQL is32separate statements, strong batches, raw pipelines",
                     "PyMySQL and Rust client stacks differ; no isolated SQL parser/acceleration claim",
                     "raw local reads lack Tarn freshness/claims/owner protections and are a weaker descriptive reference",
                     "one unchanged hot tenant/owner/hits corpus, not distributed OLTP or production capacity",
                     "CPU absent without supplied identity-bound observer; no physical bandwidth claim"])
    write(root / "study.json", study)
    try:
        for number, cell in enumerate(study["plan"]):
            prefix = root / f"{number:03d}-{cell['mode']}-b{cell['batch']}-c{cell['workers']}-t{cell['trial']}"
            record = sql_phase(args, root, prefix, cell, manifest, pymysql, credentials) if cell["mode"] == "sql" else rust_phase(args, root, prefix, cell)
            study["phases"].append(record)
            if digest(args.manifest) != study["manifest_sha256"] or digest(args.driver) != study["driver_sha256"] or digest(__file__) != study["controller_sha256"]:
                record["accepted"] = False
                record["errors"].append("frozen input/driver/controller changed")
            write(root / "study.json", study)
            if not record["accepted"]:
                raise RuntimeError("phase rejected; original results/raw errors retained")
        study["accepted"] = len(study["phases"]) == 72
    finally:
        study["files"] = {p.name: dict(bytes=p.stat().st_size, sha256=digest(p)) for p in sorted(root.iterdir()) if p.is_file() and p.name != "study.json"}
        write(root / "study.json", study)
    return study


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--driver", type=Path, required=True)
    p.add_argument("--sql", required=True)
    p.add_argument("--sql-credentials-file", type=Path, required=True)
    p.add_argument("--credentials-file", type=Path)
    p.add_argument("--python-path", type=Path)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--cpu-observer")
    p.add_argument("--seconds", type=float, default=5)
    p.add_argument("--warmup", type=float, default=1)
    p.add_argument("--seed", type=int, default=181)
    args = p.parse_args()
    if not .01 <= args.seconds <= 30 or not .01 <= args.warmup <= 10 or not 0 <= args.seed < (1 << 63):
        p.error("invalid bounded phase duration/seed")
    run(args)


if __name__ == "__main__":
    main()
