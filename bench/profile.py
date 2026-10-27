#!/usr/bin/env python3
"""What the storage side of a request costs (issue #28): runs sysbench
workloads through a SQL server against one shard and measures, on the
shard's transaction thread (the thread that runs the storage procedures):

  * CPU per statement: the thread's user and system time (from /proc),
    divided by the statements sysbench ran;
  * bytes allocated by Lua per statement (LuaJIT's gc_allocated), and per
    storage call;
  * the share of storage calls the native procedures served, reads and
    writes (lua/tarn.lua, "Native procedures");
  * where the thread's time goes, from LuaJIT's sampling profiler (sysprof,
    default mode): Lua code (interpreted and JIT-compiled), the garbage
    collector, the JIT compiler, and C code called from Lua -- the
    storage engine's work and Tarantool's -- or no Lua at all.

    bench/profile.py --sql HOST:PORT --shard HOST:PORT --pid PID [--prepare]
        [--tests oltp_point_select,...] [--threads 32] [--time 20] [--json FILE]
    bench/profile.py --summarize DIR

bench/profile.sh starts a cluster for it and runs it. --json FILE writes the
numbers to FILE too; --summarize prints the means of the files in DIR,
grouped by the part of their names before the first '-' (native-1.json,
native-2.json, lua-1.json: native and lua).
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time

T = os.environ.get("TARANTOOL", "tarantool")
TESTS = "oltp_point_select,oltp_update_index,oltp_insert,oltp_read_write,oltp_read_only"
CLK_TCK = os.sysconf("SC_CLK_TCK")

# sysprof's states (LuaJIT's VM states, the interpreter's split by the kind
# of function it runs): grouped as Lua, GC, the JIT compiler and C
GROUPS = {
    "Lua": ("LFUNC", "FFUNC", "INTERP", "TRACE", "EXIT"),
    "GC": ("GC",),
    "JIT compiler": ("RECORD", "OPT", "ASM"),
    "C": ("CFUNC",),
}

STAT = """
local misc = require('misc')
local m = misc.getmetrics()
local n = tarn_info().native
return {calls = box.stat().CALL.total, eval = box.stat().EVAL.total, alloc = m.gc_allocated,
        freed = m.gc_freed, steps = m.gc_steps_propagate + m.gc_steps_atomic + m.gc_steps_sweepstring
        + m.gc_steps_sweep + m.gc_steps_finalize, traces = m.jit_trace_num, aborts = m.jit_trace_abort,
        served = n and n.served + (n.writes or 0) or box.NULL}
"""


def shard_eval(uri, code):
    """Evaluate Lua on the shard; its result as JSON."""
    script = ("local c = require('net.box').connect('tarn:tarn@" + uri + "', {wait_connected = 5}) "
              "print(require('json').encode(c:eval(" + json.dumps(code) + "))) os.exit(0)")
    p = subprocess.run([T, "-e", script], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if p.returncode != 0:
        raise RuntimeError(p.stderr)
    return json.loads(p.stdout.strip().splitlines()[-1])


def cpu_ticks(pid):
    """User and system time of the shard's transaction thread (its main
    thread), in clock ticks."""
    with open(f"/proc/{pid}/task/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return int(fields[11]), int(fields[12])


def sysbench(test, a, time_s, threads, extra=()):
    host, port = a.sql.split(":")
    cmd = ["sysbench", test, "--db-driver=mysql", f"--mysql-host={host}", f"--mysql-port={port}",
           "--mysql-user=root", "--mysql-password=", f"--mysql-db={a.db}", f"--tables={a.tables}",
           f"--table-size={a.size}", "--db-ps-mode=auto", "--mysql-ignore-errors=1213,1205,1020,1062",
           f"--threads={threads}", f"--time={time_s}", "--rand-type=uniform", *extra]
    p = subprocess.run(cmd + ["run"], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    out = p.stdout
    q = re.search(r"queries:\s+(\d+)\s+\(([\d.]+) per sec", out)
    t = re.search(r"transactions:\s+(\d+)\s+\(([\d.]+) per sec", out)
    if not q or not t:
        raise RuntimeError(f"sysbench {test} failed:\n{out[-2000:]}\n{p.stderr[-2000:]}")
    return {"queries": int(q.group(1)), "qps": float(q.group(2)), "tps": float(t.group(2))}


HEADER = (f"{'workload':<20} {'stmts/s':>9} {'CPU us/stmt':>12} {'alloc B/stmt':>13} {'calls/stmt':>11} "
          f"{'B/call':>7} {'native':>7}  {'Lua':>5} {'GC':>5} {'JIT':>5} {'C':>5}")


def line(row):
    share = row["share"]
    native = "-" if row.get("native_share") is None else f"{100 * row['native_share']:.0f}%"
    return (f"{row['test']:<20} {row['stmts_per_s']:>9.0f} {row['cpu_us_per_stmt']:>12.1f} "
            f"{row['alloc_per_stmt']:>13.0f} {row['calls_per_stmt']:>11.2f} {row['alloc_per_call']:>7.0f} "
            f"{native:>7}  {share['Lua']:>4.0f}% {share['GC']:>4.0f}% {share['JIT compiler']:>4.0f}% "
            f"{share['C']:>4.0f}%")


def summarize(d):
    """The means of the runs in directory d, by label (a file's name up to
    its first '-') and workload."""
    groups = {}
    for name in sorted(os.listdir(d)):
        if name.endswith(".json"):
            label = name[:-5].split("-", 1)[0]
            with open(os.path.join(d, name)) as f:
                groups.setdefault(label, []).extend(json.load(f)["results"])
    for label, results in groups.items():
        print(f"== {label}: the mean of each workload's runs")
        print(HEADER)
        tests = list(dict.fromkeys(r["test"] for r in results))
        for test in tests:
            rows = [r for r in results if r["test"] == test]
            mean = {"test": test, "share": {}, "runs": len(rows)}
            for k in ("stmts_per_s", "cpu_us_per_stmt", "alloc_per_stmt", "calls_per_stmt", "alloc_per_call"):
                mean[k] = sum(r[k] for r in rows) / len(rows)
            shares = [r.get("native_share") for r in rows]
            mean["native_share"] = None if None in shares else sum(shares) / len(shares)
            for g in GROUPS:
                mean["share"][g] = sum(r["share"][g] for r in rows) / len(rows)
            print(line(mean))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql")
    ap.add_argument("--shard", help="the shard's storage node, host:port")
    ap.add_argument("--pid", type=int, help="the shard's process id")
    ap.add_argument("--tests", default=TESTS)
    ap.add_argument("--threads", type=int, default=32)
    ap.add_argument("--time", type=int, default=20)
    ap.add_argument("--tables", type=int, default=4)
    ap.add_argument("--size", type=int, default=100000)
    ap.add_argument("--db", default="sbtest")
    ap.add_argument("--prepare", action="store_true")
    ap.add_argument("--interval", type=int, default=1, help="sysprof's sampling interval, ms")
    ap.add_argument("--json")
    ap.add_argument("--summarize", metavar="DIR")
    a = ap.parse_args()
    if a.summarize:
        return summarize(a.summarize)
    if not (a.sql and a.shard and a.pid):
        ap.error("--sql, --shard and --pid are required")
    if a.prepare:
        host, port = a.sql.split(":")
        subprocess.run(["mysql", "-h", host, "-P", port, "-u", "root", "-e",
                        f"DROP DATABASE IF EXISTS {a.db}; CREATE DATABASE {a.db}"], check=True)
        subprocess.run(["sysbench", "oltp_common", "--db-driver=mysql", f"--mysql-host={host}",
                        f"--mysql-port={port}", "--mysql-user=root", "--mysql-password=", f"--mysql-db={a.db}",
                        f"--tables={a.tables}", f"--table-size={a.size}", f"--threads={a.tables}", "prepare"],
                       check=True, capture_output=True)
    results = []
    tests = [t for t in a.tests.split(",") if t]
    if tests:
        print(HEADER, flush=True)
    for test in tests:
        # a short warm-up: the JIT compiles the hot paths
        sysbench(test, a, 3, a.threads)
        s0, c0 = shard_eval(a.shard, STAT), cpu_ticks(a.pid)
        shard_eval(a.shard, f"require('misc').sysprof.start({{mode = 'D', interval = {a.interval}}}) return true")
        r = sysbench(test, a, a.time, a.threads)
        prof = shard_eval(a.shard, "local misc = require('misc') local r = misc.sysprof.report() "
                                   "misc.sysprof.stop() return r")
        s1, c1 = shard_eval(a.shard, STAT), cpu_ticks(a.pid)
        calls = max(1, s1["calls"] - s0["calls"])
        stmts = max(1, r["queries"])
        ticks = (c1[0] - c0[0]) + (c1[1] - c0[1])
        vm = prof.get("vmstate", {})
        total = max(1, sum(vm.values()))
        share = {g: 100.0 * sum(vm.get(s, 0) for s in states) / total for g, states in GROUPS.items()}
        row = {
            "test": test, "stmts_per_s": r["qps"], "tps": r["tps"],
            "cpu_us_per_stmt": ticks / CLK_TCK * 1e6 / stmts,
            "alloc_per_stmt": (s1["alloc"] - s0["alloc"]) / stmts,
            "calls_per_stmt": calls / stmts,
            "alloc_per_call": (s1["alloc"] - s0["alloc"]) / calls,
            "gc_steps_per_call": (s1["steps"] - s0["steps"]) / calls,
            "native_share": None if s1["served"] is None or s0["served"] is None
            else (s1["served"] - s0["served"]) / calls,
            "trace_aborts": s1["aborts"] - s0["aborts"],
            "samples": prof.get("samples", 0), "vmstate": vm, "share": share,
        }
        results.append(row)
        print(line(row), flush=True)
    if a.json:
        with open(a.json, "w") as f:
            json.dump({"time": time.time(), "threads": a.threads, "results": results}, f, indent=1)


if __name__ == "__main__":
    sys.exit(main())
