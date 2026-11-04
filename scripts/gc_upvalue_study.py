#!/usr/bin/env python3
"""Collect bounded functional observations; this is not a release qualification."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import signal
import subprocess
import time


TARANTOOL = "8b9a0f59a95bb6c4a491e716966d9df9d5a4f6bc"
LUAJIT = "8af934d93cf26fb1e9b08b28a8b51833d55fc8e8"
PROFILES = (
    "ordinary", "jit-off", "incremental-gc", "construction-warm",
    "construction-warm-incremental-gc",
)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def verify_runtime(directory, arm, patch):
    receipt = json.loads((directory / "runtime-receipt.json").read_text())
    expected = {
        "schema_version": 1, "arm": arm, "tarantool_commit": TARANTOOL,
        "luajit_commit": LUAJIT, "build_type": "RelWithDebInfo",
        "patch_sha256": sha(patch) if arm == "candidate" else None,
        "binary_sha256": sha(directory / "bin/tarantool"),
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise ValueError(f"runtime receipt mismatch: {key}")
    return receipt


def source_identity(root):
    def git(*args):
        return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()
    dirty = git("status", "--porcelain")
    if dirty:
        raise ValueError("source must be clean before the diagnostic run")
    paths = [*sorted((root / "lua").glob("*.lua")), root / "tests/gc_upvalue.lua",
             root / "scripts/gc_upvalue_study.py", root / "ci/install-gc-runtime.sh",
             *sorted((root / "ci/gc-runtime").glob("*")),
             root / ".github/workflows/gc-failover-review.yml"]
    return {"commit": git("rev-parse", "HEAD"), "tree": git("rev-parse", "HEAD^{tree}"),
            "dirty": False, "files": {str(p.relative_to(root)): sha(p) for p in paths if p.is_file()}}


def validate_receipt(record, profile, iterations, identities):
    """Reject false passes or measurements from a different configuration."""
    if not isinstance(record, dict) or record.get("schema_version") != 1:
        raise ValueError("missing fixture receipt/version")
    if record.get("profile") != profile or record.get("source_identities") != identities:
        raise ValueError("fixture profile/source identity mismatch")
    runtime = record.get("runtime", {})
    if runtime.get("os") != "Linux" or runtime.get("arch") != "x64" or runtime.get("gc64") is not False:
        raise ValueError("unqualified runtime architecture")
    if runtime.get("effective_jit", {}).get("enabled") is not (profile != "jit-off"):
        raise ValueError("fixture JIT configuration mismatch")
    config = record.get("config", {})
    expected_config = {"iterations": iterations, "max_iterations": 1_000_000,
                       "batch_size": 64 if profile.startswith("construction-warm") else 1,
                       "constructor_warming": profile.startswith("construction-warm"),
                       "gc": "explicit incremental step(1) each iteration" if "incremental-gc" in profile else "default",
                       "native": False, "sync": False, "transactions": False, "wal_mode": "none",
                       "cut_sequence": "false then true"}
    if any(type(config.get(k)) is not type(v) or config.get(k) != v for k, v in expected_config.items()):
        raise ValueError("fixture configuration mismatch")
    if record.get("passed") is not True:
        if record.get("complete") is not False or record.get("failure_kind") != "invariant":
            raise ValueError("fixture initialization/configuration error")
        return "invariant_failure"
    if record.get("complete") is not True:
        raise ValueError("incomplete receipt claims a pass")
    counts = record.get("counts", {})
    constructors = iterations if profile.startswith("construction-warm") else 1
    expected = {"iterations": iterations, "constructors": constructors,
                "marks": constructors + iterations, "cuts": 3 * iterations,
                "invalidations": iterations, "rebuilds": iterations,
                "blocked_rebuilds": iterations,
                "gc_steps": iterations if "incremental-gc" in profile else 0}
    if any(type(counts.get(key)) is not int or counts.get(key) != value for key, value in expected.items()):
        raise ValueError("incomplete or inconsistent functional counts")
    return "pass"


def collect_trial(binary, fixture, directory, profile, iterations, identities, timeout, core_limit):
    directory.mkdir()
    work = directory / "data"
    work.mkdir()
    env = dict(os.environ, TARN_GC_WORK_DIR=str(work),
               TARN_GC_SOURCE_IDENTITIES_JSON=json.dumps(identities, sort_keys=True))
    result = {"profile": profile, "iterations": iterations, "timeout_seconds": timeout,
              "core_limit_bytes": core_limit, "complete": False}
    save(directory / "invocation.json", result)

    def set_core_limit():
        _, hard = resource.getrlimit(resource.RLIMIT_CORE)
        limit = core_limit if hard == resource.RLIM_INFINITY else min(core_limit, hard)
        resource.setrlimit(resource.RLIMIT_CORE, (limit, hard))

    start = time.monotonic()
    with (directory / "stdout.log").open("wb") as out, (directory / "stderr.log").open("wb") as err:
        process = subprocess.Popen([str(binary), str(fixture), profile, str(iterations)],
                                   cwd=directory, env=env, stdout=out, stderr=err,
                                   preexec_fn=set_core_limit)
        result["pid"] = process.pid
        save(directory / "invocation.json", result)
        timed_out = False
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            # This unreaped child is owned by this invocation; its PID cannot be reused.
            os.kill(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.kill(process.pid, signal.SIGKILL)
                process.wait()
    result.update(returncode=process.returncode, elapsed_seconds=time.monotonic() - start,
                  timed_out=timed_out, stdout_sha256=sha(directory / "stdout.log"),
                  stderr_sha256=sha(directory / "stderr.log"))
    if timed_out:
        result["outcome"] = "timeout"
    elif process.returncode < 0:
        result.update(outcome="process_loss", signal=-process.returncode, complete=True)
    else:
        try:
            record = json.loads((directory / "stdout.log").read_text())
            outcome = validate_receipt(record, profile, iterations, identities)
            if (process.returncode == 0) != (outcome == "pass"):
                raise ValueError("exit status disagrees with functional receipt")
            result.update(outcome=outcome, fixture=record, complete=True)
        except (ValueError, TypeError) as error:
            result.update(outcome="invalid_receipt", error=str(error))
    save(directory / "result.json", result)
    return result


def run(args):
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise ValueError("this diagnostic is qualified only for Linux x86-64")
    if not 1 <= args.trials <= 10 or not 1 <= args.iterations <= 1_000_000 or not 1 <= args.timeout <= 120:
        raise ValueError("trials/iterations/timeout outside finite bounds")
    root, runtime, output = args.source.resolve(), args.runtime.resolve(), args.output.resolve()
    if output.exists():
        raise ValueError("output must be a fresh directory")
    cores = args.core_dir.resolve() if args.core_dir else None
    if cores and (not cores.is_dir() or any(cores.iterdir())):
        raise ValueError("core directory must already exist and be empty")
    receipt = verify_runtime(runtime, args.arm, root / "ci/gc-runtime/urefo-x86.patch")
    identity = source_identity(root)
    identities = {"tarn": identity, "runtime_receipt_sha256": sha(runtime / "runtime-receipt.json"),
                  "runtime_binary_sha256": receipt["binary_sha256"], "arm": args.arm}
    output.mkdir(parents=True)
    result = {"schema_version": 1, "arm": args.arm, "source_identities": identities,
              "runtime_receipt": receipt, "collection_complete": False, "qualification_pass": False,
              "interpretation": "diagnostic observations only; no GC cause or fix established",
              "trials": [], "profiles": list(PROFILES), "trials_per_profile": args.trials,
              "iterations_per_trial": args.iterations}
    save(output / "study.json", result)
    failed = False
    for profile in PROFILES:
        for trial in range(1, args.trials + 1):
            item = collect_trial(runtime / "bin/tarantool", root / "tests/gc_upvalue.lua",
                                 output / f"{profile}-{trial:02}", profile, args.iterations,
                                 identities, args.timeout, 1024 ** 3 if cores and not failed else 0)
            item["cores"] = [{"name": p.name, "bytes": p.stat().st_size, "sha256": sha(p)}
                             for p in sorted(cores.glob(f"core.*.{item['pid']}"))] if cores else []
            save(output / f"{profile}-{trial:02}" / "result.json", item)
            result["trials"].append(item)
            failed |= item["outcome"] != "pass"
            save(output / "study.json", result)
    result["collection_complete"] = all(t["complete"] for t in result["trials"])
    result["outcome_counts"] = {key: sum(t["outcome"] == key for t in result["trials"])
                                for key in ("pass", "invariant_failure", "process_loss", "timeout", "invalid_receipt")}
    result["conclusion"] = "observed_failure" if failed else "bounded_negative"
    # Bind the inputs again after execution. A changing binary/source is not a paired observation.
    if verify_runtime(runtime, args.arm, root / "ci/gc-runtime/urefo-x86.patch") != receipt or source_identity(root) != identity:
        raise ValueError("inputs changed during the run")
    save(output / "study.json", result)
    print(json.dumps({key: result[key] for key in ("arm", "collection_complete", "outcome_counts", "conclusion", "qualification_pass")}))
    return 0 if result["collection_complete"] and (args.arm == "baseline" or not failed) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--arm", choices=("baseline", "candidate"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--iterations", type=int, default=20000)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--core-dir", type=Path, help="empty directory configured in kernel.core_pattern by the isolated runner")
    args = parser.parse_args()
    try:
        return run(args)
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        parser.exit(2, f"diagnostic collector failed: {error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
