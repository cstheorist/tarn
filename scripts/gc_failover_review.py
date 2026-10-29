#!/usr/bin/env python3
"""Source-bound RF3 crash investigation (#160), separate from performance CI.

Run only on an isolated Linux runner/container: the historical failover
fixture uses fixed local ports. Every trial has a fresh directory; KEEP=1
retains successful and failed logs. A failure is never erased by a repeat.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import time

OBSERVED = "420bde9fa9393c847cf022d4230b233c49b01fa7"


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for part in iter(lambda: f.read(1024 * 1024), b""):
            h.update(part)
    return h.hexdigest()


def command(argv, cwd=None):
    return subprocess.check_output(argv, cwd=cwd, text=True, stderr=subprocess.STDOUT).strip()


def sources(root):
    # Bind every compiled Rust/C/build input and resolved dependency version.
    paths = [root / name for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml")]
    paths += sorted(p for p in (root / "crates").rglob("*") if p.is_file())
    return {str(p.relative_to(root)): sha(p) for p in paths}


def bind(args):
    root, out = args.source.resolve(), args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    binaries = {name: sha(out / name) for name in ("tarn-server", "libtarn_storage.so")}
    receipt = {"schema": 1, "source_commit": command(["git", "rev-parse", "HEAD"], root),
               "sources": sources(root), "binaries": binaries,
               "rustc": command(["rustc", "-Vv"], root),
               "cargo": command(["cargo", "-V"], root),
               "profile": "unchanged source release profile; cargo build --release --locked -p tarn-server -p tarn-storage",
               "source_status": command(["git", "status", "--porcelain"], root)}
    (out / "build.json").write_text(json.dumps(receipt, indent=2) + "\n")


def classify(logs, returncode, timed_out=False):
    """A crash is an observation, not a causal diagnosis of native code."""
    signatures = []
    for name, text in logs.items():
        if re.search(r"Segmentation fault|SIGSEGV|SEGV_MAPERR|SEGV_ACCERR|AddressSanitizer|runtime error:.*out of bounds", text):
            signatures.append(name)
    output = logs.get("run.log", "")
    native = []
    for line in output.splitlines():
        if "leader, native procedures:" in line:
            try:
                native.append(json.loads(line.split("native procedures:", 1)[1]))
            except ValueError:
                native.append("invalid-report")
    return {"exit_code": returncode, "timed_out": timed_out,
            "crash_logs": signatures,
            "trace_gc_crash": any("gc_traverse_trace" in text for text in logs.values()),
            "native_reports": native,
            "fixture_passed": returncode == 0 and not timed_out and not signatures
                              and "FAILOVER TEST PASSED" in output,
            "accounting_output": [line for line in output.splitlines() if re.search(
                r"bank:|bank \(READ COMMITTED\)|counters:|final balance|ledger|unknown outcome|direct KV sum|FAIL|EXCEPTION", line)]}


def native_mode_confirmed(reports, mode):
    # tarn_info().native is JSON false when BK.ns is absent. Do not accept
    # missing/null reports: they do not establish that Lua served the trial.
    expected = (lambda n: isinstance(n, dict) and n.get("ready") == 1) if mode == "native" else (lambda n: n is False)
    return len(reports) == 2 and all(expected(n) for n in reports)


def run(args):
    source, binaries, out = args.source.resolve(), args.binaries.resolve(), args.output.resolve()
    # Refuse accidental reuse; each invocation retains all its trials.
    out.mkdir(parents=True, exist_ok=False)
    build = json.loads((binaries / "build.json").read_text())
    actual = command(["git", "rev-parse", "HEAD"], source)
    assert actual == build["source_commit"], "gateway/native artifact is from another source commit"
    assert sources(source) == build["sources"], "compiled source inputs changed"
    for name, digest in build["binaries"].items():
        assert sha(binaries / name) == digest, "artifact hash changed: " + name
    shutil.copy2(binaries / "build.json", out / "build.json")
    tarantool = Path(shutil.which(args.tarantool) or args.tarantool).resolve(strict=True)
    env = os.environ.copy()
    # A JIT-off run is a diagnostic control, never a production workaround.
    wrapper = out / "tarantool"
    import shlex
    wrapper.write_text("#!/bin/sh\nexec " + shlex.quote(str(tarantool))
                       + (" -joff" if args.jit == "off" else "") + ' "$@"\n')
    wrapper.chmod(0o755)
    env.update(TARANTOOL=str(wrapper), TARN_BIN=str(binaries / "tarn-server"),
               TARN_NATIVE=str(binaries / "libtarn_storage.so") if args.mode == "native" else "0",
               KEEP="1", ENGINE=args.engine, SECS=str(args.seconds), TARN_CHECK_LOCKS="1")
    probe = "print(require('json').encode{version=_TARANTOOL,jit=jit.version,arch=jit.arch,os=jit.os,gc64=require('ffi').abi('gc64'),status={jit.status()}});os.exit(0)"
    runtime = command([str(wrapper), "-e", probe])
    manifest = {"schema": 1, "source_commit": actual, "mode": args.mode, "engine": args.engine,
                "jit_control": args.jit, "seconds": args.seconds, "repetitions": args.repetitions,
                "tarantool_sha256": sha(tarantool), "runtime": runtime,
                "harness_sha256": sha(__file__), "build_receipt_sha256": sha(binaries / "build.json"),
                "fixture_sources": {str(p.relative_to(source)): sha(p) for directory in ("lua", "tests", "scripts")
                                    for p in sorted((source / directory).rglob("*")) if p.is_file()},
                "core_pattern": Path("/proc/sys/kernel/core_pattern").read_text().strip()
                                if Path("/proc/sys/kernel/core_pattern").exists() else None,
                "trials": [], "scope": "Functional investigation; no performance or causal-fix claim."}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for i in range(1, args.repetitions + 1):
        trial = out / f"trial-{i}"
        trial.mkdir()
        cluster = trial / "cluster"
        env["DIR"] = str(cluster)
        argv = ["bash", str(source / "tests/failover.sh")]
        start = time.monotonic()
        timed_out = False
        proc = None
        try:
            with (trial / "run.log").open("wb") as log:
                proc = subprocess.Popen(argv, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT,
                                        start_new_session=True)
                try:
                    rc = proc.wait(timeout=args.timeout)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    rc = 124
        finally:
            # The session is created above, exclusively for this trial. Drain
            # its descendants too: the fixture's shell can exit before a child.
            if proc is not None:
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                    time.sleep(.5)
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.wait()
        logs = {str(p.relative_to(trial)): p.read_text(errors="replace") for p in trial.rglob("*")
                if p.is_file() and p.suffix in (".log", ".out")}
        result = classify(logs, rc, timed_out)
        result["native_mode_confirmed"] = native_mode_confirmed(result["native_reports"], args.mode)
        result["fixture_passed"] = result["fixture_passed"] and result["native_mode_confirmed"]
        result.update(trial=i, elapsed_seconds=time.monotonic() - start,
                      log_sha256={name: sha(trial / name) for name in logs})
        # Failed storage/WAL state can be needed for recovery diagnosis.
        # Archive it unchanged before excluding the bulky directories from CI
        # upload. Healthy-trial logs and all accounting output are still kept.
        if not result["fixture_passed"] and cluster.exists():
            with tarfile.open(trial / "failed-runtime.tar.gz", "w:gz", compresslevel=1) as archive:
                archive.add(cluster, arcname="cluster")
            result["failed_runtime_sha256"] = sha(trial / "failed-runtime.tar.gz")
        (trial / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        manifest["trials"].append(result)
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(json.dumps({k: result[k] for k in ("trial", "exit_code", "timed_out", "fixture_passed", "trace_gc_crash", "crash_logs")}), flush=True)
    return 0 if all(t["fixture_passed"] for t in manifest["trials"]) else 1


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="command", required=True)
    build = sub.add_parser("bind")
    build.add_argument("--source", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)
    trial = sub.add_parser("run")
    trial.add_argument("--source", type=Path, required=True)
    trial.add_argument("--binaries", type=Path, required=True)
    trial.add_argument("--output", type=Path, required=True)
    trial.add_argument("--mode", choices=("native", "lua"), required=True)
    trial.add_argument("--engine", choices=("memtx", "vinyl"), required=True)
    trial.add_argument("--jit", choices=("on", "off"), default="on")
    trial.add_argument("--tarantool", default="tarantool")
    trial.add_argument("--repetitions", type=int, choices=range(1, 6), default=3)
    trial.add_argument("--seconds", type=int, choices=range(30, 121), default=45)
    trial.add_argument("--timeout", type=int, choices=range(180, 601), default=360)
    args = ap.parse_args()
    return bind(args) if args.command == "bind" else run(args)


if __name__ == "__main__":
    sys.exit(main())
