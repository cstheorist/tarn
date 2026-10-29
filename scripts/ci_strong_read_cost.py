#!/usr/bin/env python3
"""Manual matched Linux study, using the existing strict benchmark fixture.

Builds finish before any timing. A failed/interrupted cohort is retained and
stops the study; it is never replayed or pooled with a different platform.
Cleanup delegates to the fixture's identity/label checks and offline WAL
retention. No global Docker/process cleanup is permitted.
"""
import argparse
import collections
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback


ROOT = Path(__file__).resolve().parents[1]
TASK = "tarn-strong-read-cost-ci"
RUST = "rust:1.98.1-slim-bookworm@sha256:dacc9e51f252243eb59d2fb4cb4ad8b0d3f607b6a82c398cf8a321e59ff778a7"
TARANTOOL = "tarantool/tarantool@sha256:5cce83352071ee90732a7c2812e9c5e53e97e692a6e9d8eb60a6c4bf618be7e6"
CASES = [{"workers": 1, "batch": 1, "shape": "hits", "route": "buckets"},
         {"workers": 64, "batch": 128, "shape": "hits", "route": "buckets"}]


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for part in iter(lambda: f.read(1024 * 1024), b""):
            h.update(part)
    return h.hexdigest()


def save(path, value):
    pending = path.with_name(path.name + ".tmp")
    pending.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    pending.replace(path)


def read(path):
    return json.loads(path.read_text())


def command(argv, *, log=None, timeout=30, check=True):
    if log is None:
        r = subprocess.run(argv, text=True, capture_output=True, timeout=timeout)
    else:
        with log.open("w") as f:
            r = subprocess.run(argv, text=True, stdout=f, stderr=subprocess.STDOUT, timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f"command failed ({r.returncode}): {argv}; log={log}")
    return r


def git(*args):
    return command(["git", "-C", str(ROOT), *args]).stdout.strip()


def fixture_module():
    sys.path.insert(0, str(ROOT / "bench"))
    spec = importlib.util.spec_from_file_location("strong_cost_fixture", ROOT / "bench/native_batch_sql_fixture.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build(out):
    assert platform.system() == "Linux" and platform.machine() == "x86_64", "native x86_64 runner required; no QEMU"
    commit = git("rev-parse", "HEAD")
    assert os.environ.get("GITHUB_SHA", commit) == commit
    assert not git("status", "--porcelain"), "build only the exact clean dispatched checkout"
    (out / "build/bin").mkdir(parents=True)
    (out / "cohorts").mkdir()
    work = Path(tempfile.mkdtemp(prefix="strong-read-cost-build-", dir=out.parent))
    source = work / "source"
    source.mkdir()
    files = git("ls-tree", "-r", "--name-only", commit, "--", "crates", "Cargo.toml", "Cargo.lock",
                "rust-toolchain.toml", "rustfmt.toml").splitlines()
    inputs = {}
    for rel in files:
        data = subprocess.check_output(["git", "-C", str(ROOT), "show", commit + ":" + rel])
        dest = source / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        inputs[rel] = sha(dest)
    harness_inputs = {}
    for rel in (".github/workflows/ci.yml", ".github/workflows/strong-read-cost.yml",
                "scripts/ci_strong_read_cost.py", "bench/native_batch_sql_fixture.py",
                "bench/native_batch_fixture.py", "bench/native_batch.py", "bench/native_batch_sql.py"):
        data = subprocess.check_output(["git", "-C", str(ROOT), "show", commit + ":" + rel])
        assert data == (ROOT / rel).read_bytes()
        dest = out / "build/source-contracts" / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        harness_inputs[rel] = sha(dest)
    m = {"task": TASK, "source_commit": commit, "source_tree": git("rev-parse", "HEAD^{tree}"),
         "stage": "building", "source_sha256": inputs, "harness_sha256": harness_inputs, "build_directory": str(work),
         "rust_image": RUST, "tarantool_image": TARANTOOL,
         "runner": {"platform": platform.platform(), "machine": platform.machine(), "cpus": os.cpu_count(),
                    "affinity": sorted(os.sched_getaffinity(0)), "memory": Path("/proc/meminfo").read_text(),
                    "cgroups": {name: (Path("/sys/fs/cgroup") / name).read_text()
                                for name in ("cpu.max", "memory.max", "cpuset.cpus.effective")
                                if (Path("/sys/fs/cgroup") / name).exists()},
                    "load": os.getloadavg(), "github_run_id": os.environ.get("GITHUB_RUN_ID"),
                    "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT")},
         "resource_scope": "unchanged 2CPU/1GiB per storage replica; RF3 plus two gateways may oversubscribe runner; shared-host noise is not excluded"}
    save(out / "study.json", m)
    for name, image in (("rust", RUST), ("tarantool", TARANTOOL)):
        command(["docker", "pull", "--platform", "linux/amd64", image], log=out / (name + "-pull.log"), timeout=180)
        info = json.loads(command(["docker", "image", "inspect", image]).stdout)[0]
        assert info["Architecture"] == "amd64" and info["Os"] == "linux"
        save(out / (name + "-image.json"), info)
    script = """set -eu
rustc -Vv > /capture/toolchain.txt
cargo -V >> /capture/toolchain.txt
test "$(rustc --version | cut -d ' ' -f 2)" = 1.98.1
cargo test --locked -p tarn-storage --all-features --lib
cargo test --locked -p tarn-verified --test erased
cargo build --locked --release -p tarn-server --bin tarn-server
cargo build --locked --release -p tarn-storage --features alloc-metrics
cargo build --locked --release -p tarn-iproto --example native_batch
cargo tree --locked -p tarn-server --edges normal,build > /capture/gateway-tree.txt
cp /work/target/release/tarn-server /capture/bin/tarn-server
cp /work/target/release/libtarn_storage.so /capture/bin/libtarn_storage.so
cp /work/target/release/examples/native_batch /capture/bin/native_batch
"""
    (out / "build/build.sh").write_text(script)
    name = "tarn-cost-build-" + out.parent.name.replace("_", "-") + "-" + str(os.getpid())
    m["builder"] = {"name": name, "label": out.name}
    save(out / "study.json", m)
    argv = ["docker", "create", "--name", name, "--platform", "linux/amd64", "--label", "task=" + TASK,
            "--label", "fixture=" + out.name, "--user", f"{os.getuid()}:{os.getgid()}",
            "--mount", f"type=bind,src={source},dst=/source,readonly",
            "--mount", f"type=bind,src={work},dst=/work",
            "--mount", f"type=bind,src={out / 'build'},dst=/capture", "--workdir", "/source",
            "--env", "CARGO_HOME=/work/cargo", "--env", "CARGO_TARGET_DIR=/work/target",
            RUST, "sh", "/capture/build.sh"]
    m["builder"]["id"] = command(argv).stdout.strip()
    save(out / "study.json", m)
    command(["docker", "start", "--attach", m["builder"]["id"]], log=out / "build/build.log", timeout=1150)
    finished = json.loads(command(["docker", "inspect", m["builder"]["id"]]).stdout)[0]
    assert not finished["State"]["Running"] and finished["State"]["ExitCode"] == 0 and not finished["State"]["OOMKilled"]
    save(out / "builder-completed.json", finished)
    command(["docker", "rm", "-v", m["builder"]["id"]])
    m["builder"]["removed_before_timing"] = True
    assert all(sha(source / f) == d for f, d in inputs.items())
    toolchain = (out / "build/toolchain.txt").read_text()
    assert toolchain.startswith("rustc 1.98.1 ") and "host: x86_64-unknown-linux-gnu" in toolchain
    for binary in (out / "build/bin").iterdir():
        with binary.open("rb") as f:
            header = f.read(20)
        assert header[:6] == b"\x7fELF\x02\x01" and int.from_bytes(header[18:20], "little") == 62, binary.name
    runtime = {f: d for f, d in inputs.items() if f.startswith(("crates/tarn-storage/", "crates/tarn-verified/"))}
    artifacts = {f.name: sha(f) for f in (out / "build/bin").iterdir()}
    save(out / "build/native-binding.json", {"source_commit": commit, "source_tree": m["source_tree"],
         "artifact_sha256": artifacts["libtarn_storage.so"], "runtime_sources": runtime,
         "features": ["alloc-metrics"], "layout": 14, "size": 392, "build_image": RUST})
    shutil.copy2(ROOT / "crates/tarn-iproto/examples/native_batch.rs", out / "build/native_batch.rs")
    command([sys.executable, "-m", "venv", str(work / "python")], timeout=30)
    python = work / "python/bin/python"
    command([str(python), "-m", "pip", "--isolated", "install", "--index-url", "https://pypi.org/simple",
             "--target", str(out / "python-driver"), "--no-deps", "--disable-pip-version-check", "PyMySQL==1.2.3"],
            log=out / "python-install.log", timeout=120)
    assert "\nVersion: 1.2.3\n" in (out / "python-driver/pymysql-1.2.3.dist-info/METADATA").read_text()
    save(out / "python-driver-sha256.json", {str(p.relative_to(out / "python-driver")): sha(p)
         for p in sorted((out / "python-driver").rglob("*")) if p.is_file() and "__pycache__" not in p.parts})
    save(out / "direct-cases.json", CASES)
    m.update(stage="built", artifacts_sha256=artifacts, python=str(python))
    save(out / "study.json", m)


def measure(out):
    m = read(out / "study.json")
    assert m["stage"] == "built" and m["source_commit"] == git("rev-parse", "HEAD")
    assert not git("status", "--porcelain"), "measurement harness must match its committed source"
    m["cohorts"] = []
    try:
        for label, replicas, direct in (("direct-rf1", 1, True), ("direct-rf3", 3, True), ("sql-rf3", 3, False)):
            print("START " + label, flush=True)
            assert not git("status", "--porcelain")
            for rel, digest in m["harness_sha256"].items():
                assert sha(ROOT / rel) == digest, rel
            for name, digest in m["artifacts_sha256"].items():
                assert sha(out / "build/bin" / name) == digest, name
            argv = [m["python"], str(ROOT / "bench/native_batch_sql_fixture.py"), "--source", str(ROOT),
                    "--source-ref", m["source_commit"], "--image", TARANTOOL,
                    "--gateway", str(out / "build/bin/tarn-server"), "--native", str(out / "build/bin/libtarn_storage.so"),
                    "--native-manifest", str(out / "build/native-binding.json"),
                    "--client", str(out / "build/bin/native_batch"), "--client-source", str(out / "build/native_batch.rs"),
                    "--python-path", str(out / "python-driver"),
                    "--parent", str(out / "cohorts"), "--replicas", str(replicas), "--rows", "100000", "--payload", "64",
                    "--tenant", "7", "--trials", "6", "--seconds", "5", "--warmup", "1",
                    "--deadline", "900", "--cpus", "2", "--memory", "1g"]
            argv += ["--direct-storage", "--cases-json", str(out / "direct-cases.json")] if direct else [
                "--batches", "32", "--shapes", "hits,miss", "--workers", "1"]
            before = set((out / "cohorts").iterdir())
            item = {"label": label, "command": argv, "status": "running", "roots_before": [str(p) for p in before]}
            m["cohorts"].append(item)
            save(out / "study.json", m)
            # The fixture enforces its own 900s deadline and always drains its
            # exact children/containers. Workflow cleanup handles interruption.
            with (out / (label + ".log")).open("w") as log:
                process = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                item["controller"] = {"pid": process.pid, "identity": fixture_module().identity(process.pid)}
                assert item["controller"]["identity"] and os.getpgid(process.pid) == process.pid
                save(out / "study.json", m)
                process.wait()
            roots = set((out / "cohorts").iterdir()) - before
            item.update(exit=process.returncode, roots=[str(p) for p in roots], status="returned")
            save(out / "study.json", m)
            assert process.returncode == 0 and len(roots) == 1, "cohort failed; retained, no replay"
            root = roots.pop()
            fixture = read(root / "manifest.json")
            rows = read(root / "measurements/results.json")
            pairs = read(root / "measurements/pairs.json")
            assert fixture["source_commit"] == m["source_commit"] and fixture["stage"] == "cleaned"
            assert fixture["failed"] is False and fixture["cleanup"]["owned_services_absent"] is True
            assert fixture["cleanup"]["errors"] == [] and fixture["archive"]["verified_offline"] is True
            assert len(fixture["nodes"]) == replicas
            for node in fixture["nodes"]:
                assert node["oom_killed"] is False
                assert node["memory_events"]["oom"] == node["memory_events"]["oom_kill"] == 0
            for checkpoint in ("native_ready_before", "native_ready_after"):
                assert len(fixture[checkpoint]) == replicas
                for node in fixture[checkpoint]:
                    assert node["version"] == "3.9.0-0-g8b9a0f59a9"
                    assert node["layout"] == 14 and node["size"] == 392 and node["lock_copy_errors"] == 0
                    assert node["test_hooks"] is False and node["check_locks"] is True
                    assert node["sync"] == (replicas == 3) and node["wal"] == ("fsync" if replicas == 3 else "write")
            assert len(rows) == 24 and len(pairs) == 12 and all(r["accepted"] is True for r in rows)
            expected_cells = {(1, 1, "hits"), (128, 64, "hits")} if direct else {(32, 1, "hits"), (32, 1, "miss")}
            assert {(p["batch"], p["workers"], p["shape"]) for p in pairs} == expected_cells
            for cell in {(p["batch"], p["workers"], p["shape"]) for p in pairs}:
                six = [p for p in pairs if (p["batch"], p["workers"], p["shape"]) == cell]
                assert sorted(p["trial"] for p in six) == list(range(6))
                assert len(six) == 6 and collections.Counter(tuple(p["order"]) for p in six) == {
                    ("lua", "native"): 3, ("native", "lua"): 3}
            for row in rows:
                assert (row["client"]["elapsed_seconds"] if direct else row["elapsed_seconds"]) >= 5
                for snap in (row["before"], row["after"]):
                    assert snap["read_contract"] == ["strong_reads", 1]
                    assert snap["native_layout"] == 14 and snap["native_size"] == 392 and snap["native_fresh_clock_ok"] == 1
                    assert snap["info"]["clock"]["read_ready"] is True
                    policy = snap["info"]["clock"]["policy"]
                    assert {k: policy[k] for k in ("max_age", "skew", "max_gap", "max_drift")} == {
                        "max_age": 120, "skew": 60, "max_gap": 2, "max_drift": .25}
            for name, digest in m["artifacts_sha256"].items():
                assert fixture["frozen_sha256"]["bin/" + name] == digest
            item.update(status="accepted", phases=24, pairs=12, manifest_sha256=sha(root / "manifest.json"))
            save(out / "study.json", m)
            print("END " + label + " 24 accepted phases / 12 pairs", flush=True)
        m.update(stage="complete", accepted_phases=72, matched_pairs=36)
    except BaseException:
        m["stage"] = "failed-or-interrupted"
        m["failure"] = traceback.format_exc()
        raise
    finally:
        save(out / "study.json", m)


class ExistingChild:
    """Only used after the recorded controller is gone; never claims an exit code."""
    def __init__(self, pid):
        self.pid, self.returncode = pid, None

    def poll(self):
        return None

    def wait(self, timeout):
        return None


def live_group_members(pid):
    """Zombies cannot execute cleanup or retain a live process group."""
    listing = command(["ps", "-ww", "-axo", "pid,pgid,stat,command"]).stdout
    members = []
    for line in listing.splitlines():
        fields = line.split(None, 3)
        if len(fields) == 4 and fields[0].isdigit() and fields[1].isdigit():
            if int(fields[1]) == pid and not fields[2].startswith("Z"):
                members.append(int(fields[0]))
    return members


def drain_controller(module, pid, expected, grace=60):
    """Public-ps identity check; callable by an owned no-Docker control."""
    if not live_group_members(pid):
        return {"pid": pid, "drained": True, "scope": "no live private-group members"}
    current = module.identity(pid)
    assert current and all(current[k] == expected[k] for k in ("lstart", "command")), "controller identity changed"
    assert os.getpgid(pid) == pid, "controller no longer owns its private process group"
    os.killpg(pid, signal.SIGINT)
    deadline = time.monotonic() + grace
    while live_group_members(pid) and time.monotonic() < deadline:
        time.sleep(.2)
    assert not live_group_members(pid), "controller did not drain; retain resources for diagnosis"
    return {"pid": pid, "original": expected, "current": current, "drained": True,
            "reparented": current["ppid"] != expected["ppid"]}


def retain_offline_wal(out, root, fixture):
    """Only closed, verified fixture archives enter the uploadable WAL tree."""
    assert fixture["stage"] == "cleaned" and fixture["cleanup"]["owned_services_absent"] is True
    assert fixture["cleanup"]["errors"] == [] and fixture["cleanup"]["data_removed"] is True
    assert not (root / "data").exists() and fixture["archive"]["verified_offline"] is True
    archive, digest = root / "runtime-wal.tar", fixture["archive"]["sha256"]
    assert archive.is_file() and not archive.is_symlink() and sha(archive) == digest
    dest = out / "offline-wal" / root.name
    dest.mkdir(parents=True, exist_ok=True)
    pending = dest / "runtime-wal.tar.tmp"
    shutil.copyfile(archive, pending)
    assert sha(pending) == digest
    pending.replace(dest / "runtime-wal.tar")
    save(dest / "receipt.json", {"source_fixture": str(root), "sha256": digest,
         "files": fixture["archive"]["files"], "verified_offline": True,
         "source_manifest_sha256": sha(root / "manifest.json"),
         "scope": "copied after controller drain and confirmed owned-service cleanup"})


def cleanup(out):
    if not (out / "study.json").exists():
        return
    m, errors, module = read(out / "study.json"), [], fixture_module()
    blocked_roots, blocked_cohorts, reparented = set(), [], []
    # First stop each exact recorded fixture controller, allowing its own
    # finally block to drain and archive. No cohort is restarted here.
    for item in m.get("cohorts", []):
        controller = item.get("controller")
        if not controller:
            # Popen may have succeeded just before its identity receipt was
            # saved. Ownership is unknown: never race an unrecorded owner.
            errors.append("Cohort " + item["label"] + " has no recorded controller; cleanup ownership is unconfirmed")
            blocked_cohorts.append({"label": item["label"], "roots_before": item.get("roots_before", []),
                                    "reason": "controller identity was not recorded"})
            continue
        pid, expected = controller["pid"], controller["identity"]
        try:
            reparented.append(drain_controller(module, pid, expected))
        except BaseException:
            errors.append(traceback.format_exc())
            blocked_cohorts.append({"label": item["label"], "roots_before": item.get("roots_before", []),
                                    "reason": "controller drain was not confirmed"})
    for root in sorted((out / "cohorts").glob("tarn-native-sql-rf*")):
        # Evaluate the scope now, not at the failed-drain instant: an owner
        # that is still running may have created its root since that check.
        if any(str(root) not in item["roots_before"] for item in blocked_cohorts):
            blocked_roots.add(str(root))
            continue  # never race a still-running controller's teardown/WAL archive
        try:
            f = read(root / "manifest.json")
            assert f["task"] == module.TASK and f["fixture"] == root.name
            assert Path(f["arguments"]["parent"]).resolve() == (out / "cohorts").resolve()
            if f["stage"] == "cleaned":
                assert f["cleanup"]["owned_services_absent"] is True
                retain_offline_wal(out, root, f)
                continue
            processes = []
            for record in f.get("processes", []):
                current = module.identity(record["pid"])
                if current:
                    expected = record["identity"]
                    assert all(current[k] == expected[k] for k in ("lstart", "command")), "child identity changed"
                    # The now-dead controller can reparent a surviving child.
                    # Start time+exact argv must still match before signalling.
                    record["identity_before_cleanup_adoption"] = expected
                    record["identity"] = current
                processes.append((ExistingChild(record["pid"]), record))
            module.cleanup(root, f, processes)
            retain_offline_wal(out, root, read(root / "manifest.json"))
        except BaseException:
            errors.append(traceback.format_exc())
    builder = m.get("builder")
    if builder:
        try:
            r = command(["docker", "inspect", builder["name"]], check=False)
            if r.returncode == 0:
                info = json.loads(r.stdout)[0]
                assert info["Name"] == "/" + builder["name"]
                assert not builder.get("id") or info["Id"] == builder["id"]
                assert info["Config"]["Labels"]["task"] == TASK and info["Config"]["Labels"]["fixture"] == out.name
                if info["State"]["Running"]:
                    command(["docker", "stop", "--time", "10", info["Id"]], timeout=15)
                save(out / "builder-final.json", json.loads(command(["docker", "inspect", info["Id"]]).stdout)[0])
                command(["docker", "rm", "-v", info["Id"]])
                gone = command(["docker", "inspect", info["Id"]], check=False)
                assert gone.returncode != 0 and "no such" in gone.stderr.lower(), "builder absence unconfirmed"
            else:
                assert "no such" in r.stderr.lower(), "Docker unavailable; builder absence unconfirmed"
        except BaseException:
            errors.append(traceback.format_exc())
    save(out / "cleanup.json", {"errors": errors, "blocked_roots": sorted(blocked_roots),
         "blocked_cohorts": blocked_cohorts,
         "reparented_controllers": reparented, "scope": "only recorded identities and fixture-labelled resources"})
    if errors:
        m["stage"] = "failed-or-interrupted"
        m["cleanup_errors"] = errors
        save(out / "study.json", m)
        raise RuntimeError(errors)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("phase", choices=("build", "measure", "cleanup"))
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    save(out / (args.phase + "-invocation.json"), {"phase": args.phase, "python": sys.version,
         "platform": platform.platform(), "source": str(ROOT)})
    try:
        {"build": build, "measure": measure, "cleanup": cleanup}[args.phase](out)
    except BaseException:
        save(out / (args.phase + "-failure.json"), {"failure": traceback.format_exc()})
        raise


if __name__ == "__main__":
    main()
