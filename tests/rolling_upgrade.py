#!/usr/bin/env python3
"""Owned, isolated RF3/fsync rolling-upgrade regression; see docs/upgrades.md.

The old source/artifacts must be the baseline prepared by rolling_upgrade.sh.
All processes, data, logs, source/binary identities and events stay under a
fresh evidence directory. This never attaches to an existing cluster.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import traceback

from upgrade_prepare import prepare_witness

sys.path.insert(0, str(Path(__file__).resolve().parent / "sql"))
from lazytest import ev, rpc, q, wait_empty  # noqa: E402
from movetest import bucket_of  # noqa: E402
import pymysql  # noqa: E402
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from upgrade_build_receipt import verify as verify_build  # noqa: E402

BASELINE = "2e8aaeb772542968e503212a143b2e6f8d27b8ab"
ROOT = Path(__file__).resolve().parents[1]
TARANTOOL = os.environ.get("TARANTOOL", "tarantool")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def source(root):
    files = sorted(p for folder in ("crates", "lua", "tests", "scripts", "docs") for p in (root / folder).rglob("*")
                   if p.is_file() and p.suffix in (".rs", ".lua", ".toml", ".py", ".sh", ".md", ".c"))
    files += [root / "Cargo.toml", root / "Cargo.lock", root / "rust-toolchain.toml"]
    hashes = {str(p.relative_to(root)): sha(p) for p in files}
    return {"files": hashes, "sha256": hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()}


def wait(label, fn, seconds=45):
    deadline, last = time.monotonic() + seconds, None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except (RuntimeError, OSError, pymysql.MySQLError) as error:
            last = str(error)
        time.sleep(.1)
    raise AssertionError(f"{label} timed out: {last}")


def ports(n):
    sockets = [socket.socket() for _ in range(n)]
    try:
        for s in sockets:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


class Cluster:
    def __init__(self, args, name, shards=2, replicas=3):
        self.args, self.shards, self.replicas = args, shards, replicas
        self.work = args.evidence / name
        self.work.mkdir()
        allocated = ports(shards * replicas + 3)
        self.nodes = {f"{i}.{j}": f"127.0.0.1:{allocated.pop()}"
                      for i in range(1, shards + 1) for j in range(1, replicas + 1)}
        self.gateway_ports = allocated
        self.processes, self.logs, self.expected = {}, {}, {i: 0 for i in range(1, 17)}
        self.stage = 0
        self.gateway_versions = {}
        self.config = self.work / "cluster.json"
        self.config.write_text(json.dumps({"shards": [
            {"id": i, "replicas": [self.nodes[f"{i}.{j}"] for j in range(1, replicas + 1)]}
            for i in range(1, shards + 1)], "bucket_count": 64, "pool_size": 2}))

    def event(self, kind, **data):
        record = {"time": time.time(), "cluster": self.work.name, "event": kind, **data}
        with (self.args.evidence / "events.jsonl").open("a") as out:
            out.write(json.dumps(record, sort_keys=True) + "\n")
        print(f"  {self.work.name}: {kind}", flush=True)

    def spawn(self, key, cmd, env=None):
        assert key not in self.processes
        log = self.work / f"{key}-{time.time_ns()}.log"
        with log.open("wb") as out:
            self.processes[key] = subprocess.Popen(cmd, env=env, stdout=out, stderr=subprocess.STDOUT,
                                                   stdin=subprocess.DEVNULL, start_new_session=True)
        self.logs[key] = log

    def stop(self, key):
        proc = self.processes.pop(key, None)
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)

    def close(self):
        for key in list(self.processes):
            self.stop(key)
        for log in self.work.glob("s*.log"):
            text = log.read_text(errors="replace")
            assert "lock copy differs" not in text and "yielded inside" not in text, log

    def storage(self, key, version, native=None):
        root = self.args.old_root if version == "old" else ROOT
        native = native if native is not None else (self.args.old_native if version == "old" else self.args.new_native)
        shard = int(key.split(".")[0])
        env = dict(os.environ, TARN_SHARD_ID=str(shard), TARN_LISTEN=self.nodes[key],
                   TARN_WORKDIR=str(self.work / ("data-" + key)), TARN_MEMORY_MB="128",
                   TARN_VINYL_MEMORY_MB="16", TARN_VINYL_CACHE_MB="0", TARN_VINYL_READ_THREADS="1",
                   TARN_VINYL_WRITE_THREADS="2", TARN_WAL_MODE="fsync", TARN_LOG_LEVEL="4",
                   TARN_NATIVE=str(native), TARN_TEST_HOOKS="1", TARN_CHECK_LOCKS="1", TARN_CHECK_YIELDS="1",
                   TARN_REPLICAS=json.dumps(["tarn:tarn@" + self.nodes[f"{shard}.{j}"]
                                             for j in range(1, self.replicas + 1)] if self.replicas > 1 else []),
                   TARN_PEERS=json.dumps({str(i): ["tarn:tarn@" + self.nodes[f"{i}.{j}"]
                                                  for j in range(1, self.replicas + 1)]
                                          for i in range(1, self.shards + 1)}))
        self.spawn("s" + key, [TARANTOOL, str(root / "lua/storage.lua")], env)
        self.event("storage_started", node=key, source=version, native=str(native))

    def ready(self, key):
        node = self.nodes[key]
        # Readiness is observed before setup/canary mutations, never by replaying
        # one after a refusal or an uncertain reply. Preserve the clock policy.
        wait("schema/clock " + key, lambda: ev(node, """
if rawget(_G,'tarn') == nil or box.space._tarn_meta:get{'shard_id'} == nil then return false end
local clock=tarn.clock.status()
return clock.read_ready and (box.info.ro or clock.state=='healthy') and
    (box.info.ro or tarn_compatibility == nil or tarn_compatibility()[7] ~= 'pending')
"""))
        ev(node, "tarn.cfg.recovery_after=3600; return true")

    def leader(self, shard):
        def find():
            for key, node in self.nodes.items():
                if key.startswith(f"{shard}.") and "s" + key in self.processes:
                    try:
                        if ev(node, "return not box.info.ro"):
                            return key
                    except (RuntimeError, OSError):
                        pass
            return None
        return wait("leader " + str(shard), find)

    def leaders(self):
        return [self.nodes[self.leader(i)] for i in range(1, self.shards + 1)]

    def joined(self, shard):
        node = self.nodes[self.leader(shard)]
        wait("replica follow", lambda: ev(node, """
local n=0
for id,r in pairs(box.info.replication) do
    if id~=box.info.id and r.downstream and r.downstream.status=='follow' then n=n+1 end
end
return n >= ...
""", [self.replicas - 1]))

    def gateway(self, version, slot=0, refuse=False):
        binary = self.args.old_bin if version == "old" else self.args.new_bin
        key = "g" + str(slot)
        self.gateway_versions[slot] = version
        self.spawn(key, [str(binary), "--config", str(self.config), "--listen",
                         f"127.0.0.1:{self.gateway_ports[slot]}", "--allow-all", "--node-id", str(slot + 1),
                         "--default-engine", self.args.engine], dict(os.environ, TARN_LOG="info"))
        if refuse:
            proc = self.processes[key]
            assert proc.wait(timeout=20) != 0
            assert "incompatible Tarn storage" in self.logs[key].read_text()
            self.stop(key)
            self.event("incompatible_gateway_refused", source=version)
        else:
            def probe():
                with self.sql(slot):
                    return True
            wait("SQL gateway", probe)

    def sql(self, slot=0, db=None):
        return pymysql.connect(host="127.0.0.1", port=self.gateway_ports[slot], user="sb", password="",
                               database=db, autocommit=True, connect_timeout=1, read_timeout=20, write_timeout=5)

    def setup(self):
        with self.sql() as c:
            q(c, "CREATE DATABASE upgrade")
            q(c, "CREATE TABLE upgrade.t (id INT PRIMARY KEY, v INT, k INT, u INT, UNIQUE KEY uk(u),"
                 f"KEY lk(k) COMMENT 'tarn:lazy') ENGINE={self.args.engine}")
            q(c, "INSERT INTO upgrade.t VALUES " + ",".join(f"({i},0,7,{i*10})" for i in self.expected))
        self.check_rows()

    def check_rows(self, slots=(0,)):
        for slot in slots:
            with self.sql(slot, "upgrade") as c:
                assert q(c, "SELECT id,v FROM t ORDER BY id") == tuple(sorted(self.expected.items()))
        physical = []
        for node in self.leaders():
            physical.extend(ev(node, "local out={} for _,r in box.space['upgrade.t']:pairs() do "
                                     "out[#out+1]={r.id,r.v} end return out"))
        assert sorted(map(tuple, physical)) == sorted(self.expected.items()), physical

    def legacy_write_refusal(self, slot):
        # A predecessor-created catalog has unknown declaration provenance.
        # Check the new gateway's default before deliberately opting into the
        # old policy for this fixture's separately recorded mixed-version DML.
        with self.sql(slot, "upgrade") as c:
            assert q(c, "SELECT @@tarn_legacy_integrity") == (("REJECT",),)
            before = q(c, "SELECT id,v FROM t ORDER BY id")
            try:
                q(c, "UPDATE t SET v=v+9999 WHERE id=1")
            except pymysql.MySQLError as error:
                assert error.args[0] == 1235 and "legacy integrity" in error.args[1], error
            else:
                raise AssertionError("new gateway admitted legacy DML without acknowledgement")
            assert q(c, "SELECT id,v FROM t ORDER BY id") == before
            assert q(c, "SELECT @@tarn_legacy_integrity") == (("REJECT",),)
        self.event("legacy_integrity_default_refused_without_effect", slot=slot)

    def canary(self, slots=(0,)):
        self.stage += 1
        nodes = self.leaders()
        for slot in slots:
            with self.sql(slot, "upgrade") as c:
                if self.work.name == "rolling" and self.gateway_versions[slot] == "new":
                    # Explicit test-only acceptance of the known fixture's old
                    # declaration policy; never a gateway/pool-wide default.
                    assert q(c, "SELECT @@tarn_legacy_integrity") == (("REJECT",),)
                    q(c, "SET SESSION tarn_legacy_integrity='ALLOW'")
                    self.event("legacy_integrity_canary_explicit_allow", slot=slot)
                q(c, "BEGIN")
                q(c, "UPDATE t SET v=v+9999 WHERE id=1")
                assert q(c, "SELECT v FROM t WHERE id=1") == ((self.expected[1] + 9999,),)
                q(c, "ROLLBACK")
                assert q(c, "SELECT v FROM t WHERE id=1") == ((self.expected[1],),)
                q(c, "BEGIN")
                q(c, "UPDATE t SET v=v+1")
                q(c, "COMMIT")
                self.expected = {k: v + 1 for k, v in self.expected.items()}
                name = f"d{self.stage}_{slot}"
                q(c, f"CREATE TABLE {name} (id INT PRIMARY KEY,v INT)")
                q(c, f"INSERT INTO {name} VALUES (1,2)")
                q(c, f"ALTER TABLE {name} ADD COLUMN w INT DEFAULT 7")
                assert q(c, f"SELECT * FROM {name}") == ((1, 2, 7),)
                q(c, "SET tarn_lazy_read_your_writes=ON")
                q(c, "UPDATE t SET v=v+1 WHERE id=1")
                self.expected[1] += 1
                wait_empty(nodes)
                assert q(c, "SELECT id,v FROM t FORCE INDEX(lk) WHERE k=7 ORDER BY id") == tuple(sorted(self.expected.items()))
                if self.shards > 1:
                    bucket = bucket_of(1, 64)
                    owner = next(i for i, n in enumerate(nodes, 1)
                                 if ev(n, "local b=box.space._tarn_buckets:get{...}; return b and b.state=='active'", [bucket]))
                    q(c, f"TARN MOVE BUCKET {bucket} TO SHARD {3-owner}")
        self.check_rows(slots)
        self.event("SQL_DDL_move_lazy_canaries_passed", slots=list(slots), expected=self.expected)

    def prepare(self, txid, event=None):
        nodes = self.leaders()
        for shard, node in enumerate(nodes, 1):
            # Seed once, before any PREPARE. A lost setup reply fails the
            # fixture; it must never enter the admission-attempt loop.
            assert ev(node, """
local shard, engine = ...
local sid=99001
-- A fixture space outside the SQL catalog: incomplete native definitions
-- must never be inserted into the gateway's catalog as if they were SQL.
local sp=box.schema.space.create('upgrade.prepared',{id=sid,engine=engine,is_sync=tarn.cfg.sync,
    if_not_exists=true,format={{'id','unsigned'},{'v','integer'},{'bucket_id','unsigned'}}})
sp:create_index('primary',{unique=true,parts={{1,'unsigned'}},if_not_exists=true})
sp:create_index('bucket_id',{unique=false,parts={{3,'unsigned'}},if_not_exists=true})
local bucket
for _,b in box.space._tarn_buckets:pairs() do if b.state=='active' then bucket=b.bucket;break end end
assert(bucket)
box.space[sid]:replace{shard,0,bucket}
return true
""", [shard, self.args.engine]) is True

        def prepare(node, identity, timeout):
            return ev(node, """
local txid=...
local sid, shard=99001,tarn.cfg.shard_id
local bucket=box.space[sid]:get{shard}.bucket_id
local r=tarn_prepare(txid,1,{1,2},{{'replace',sid,{shard,1,bucket}}},{},{},require('fiber').time(),
                     nil,{sid,0},{bucket})
local p=box.space._tarn_part:get{txid}
return {reply=r,state=p and p.state or 'none'}
""", [identity], timeout=timeout)

        def abort_and_verify(identity, remaining):
            # A confirmed NO makes commit impossible. The decision must be
            # durably ABORT before releasing any earlier participant's YES.
            decision = rpc(nodes[0], "tarn_txn_cas", [identity, "staging", "aborted", [1, 2]],
                           timeout=remaining())
            assert decision == "aborted", decision
            evidence = []
            for node in nodes:
                result = rpc(node, "tarn_abort", [identity, 1, [1, 2]], timeout=remaining())
                assert result == ["ok", "aborted"], result
                state = ev(node, """
local txid=...
tarn.fresh.barrier(nil,true)
box.begin{txn_isolation='read-confirmed'}
local p=box.space._tarn_part:get{txid}
local d=box.space._tarn_txn:get{txid}
local result={participant=p and p.state or 'none',decision=d and d.status or 'none',
    value=box.space['upgrade.prepared']:get{tarn.cfg.shard_id}.v,
    locks=box.space._tarn_locks.index.txid:count{txid},
    range_locks=box.space._tarn_rlocks.index.pk:count{txid}}
box.rollback()
return result
""", [identity], timeout=remaining())
                assert state == dict(participant="aborted", decision="aborted" if node == nodes[0] else "none",
                                     value=0, locks=0, range_locks=0), state
                evidence.append(dict(node=node, abort=result, state=state))
            return evidence

        return prepare_witness(txid, nodes, prepare, abort_and_verify, event or self.event)

    def prepare_admission_controls(self):
        if self.args.engine != "vinyl":
            return
        # Leased pressure applies only to this owned control, and must yield
        # a real durable NO. Ordinary rolling stages use real pressure only.
        for participant, node in enumerate(self.leaders(), 1):
            owner = f"upgrade-admission-control-{participant}"
            observed = []
            pressure = dict(rate_limit=0, dump_bandwidth=1, blocked_writers=1,
                            memory_level0=0, memory_limit=16 << 20, compaction_queue=0)
            assert ev(node, "return tarn.vinyl.test_pressure(...)", [owner, pressure, 30]) == ["ok"]

            def record(kind, **data):
                self.event(kind, **data)
                if (kind == "prepare_attempt_reply" and data["node"] == node
                        and data["result"]["reply"][:2] == ["no", 1205]):
                    observed.append(data)
                    assert ev(node, "return tarn.vinyl.test_pressure(...)", [owner, None]) == ["ok"]

            try:
                base = f"upgrade-admission-{participant}"
                txid = self.prepare(base, event=record)
                assert observed and txid != base, (observed, txid)
                for shard in range(1, self.shards + 1):
                    leader = self.leader(shard)
                    follower = next(k for k in self.nodes if k.startswith(f"{shard}.") and k != leader)
                    self.stop("s" + follower)
                    self.storage(follower, "new")
                    self.ready(follower)
                    self.joined(shard)
                    assert ev(self.nodes[follower], "return box.space._tarn_part:get{...}.state", [txid]) == "prepared"
                self.finish(txid)
                self.event("admission_refusal_control_passed", participant=participant, txid=txid)
            finally:
                assert ev(node, "return tarn.vinyl.test_pressure(...)", [owner, None]) == ["ok"]

    def finish(self, txid):
        for node in self.leaders():
            assert rpc(node, "tarn_apply", [txid])[0] == "ok"
            assert rpc(node, "tarn_apply", [txid])[0] == "ok"
            assert ev(node, "return box.space['upgrade.prepared']:get{tarn.cfg.shard_id}.v") == 1
        self.event("prepared_applied_once_after_restart", txid=txid)

    def roll(self, version):
        for shard in range(1, self.shards + 1):
            leader = self.leader(shard)
            followers = [k for k in self.nodes if k.startswith(f"{shard}.") and k != leader]
            for key in followers:
                txid = f"upgrade-{version}-{key}"
                txid = self.prepare(txid)
                self.stop("s" + key)
                self.storage(key, version)
                self.ready(key)
                self.joined(shard)
                assert ev(self.nodes[key], "return box.space._tarn_part:get{...}.state", [txid]) == "prepared"
                self.finish(txid)
                self.canary()
            # A transaction opened on the old leader must not validate its
            # stale stream after a real handoff, even though that socket lives.
            with self.sql(0, "upgrade") as c:
                key = next(i for i in self.expected
                           if ev(self.nodes[leader], "return box.space['upgrade.t']:get{...} ~= nil", [i]))
                q(c, "BEGIN")
                q(c, "SELECT v FROM t WHERE id=%s", (key,))
                target = followers[0]
                ev(self.nodes[target], "box.ctl.promote(); return true")
                assert wait("handoff", lambda: self.leader(shard) == target)
                self.ready(target)
                try:
                    q(c, "COMMIT")
                except pymysql.MySQLError as error:
                    assert error.args[0] in (1213, 1180), error
                    self.event("old_stream_refused_after_handoff", code=error.args[0])
                else:
                    raise AssertionError("old leader validated a stream after handoff")
            self.stop("s" + leader)
            self.storage(leader, version)
            self.ready(leader)
            self.joined(shard)
            self.canary()
        self.event("storage_roll_complete", source=version)


def fresh_cluster(args):
    c = Cluster(args, "fresh-compatible")
    try:
        for key in c.nodes:
            c.storage(key, "new")
        for key in c.nodes:
            c.ready(key)
        for shard in range(1, c.shards + 1):
            c.joined(shard)
        c.gateway("new")
        c.setup()
        c.prepare_admission_controls()
        c.canary()
        c.event("fresh_RF3_bootstrap_passed")
    finally:
        c.close()


def rolling(args):
    c = Cluster(args, "rolling")
    try:
        for key in c.nodes:
            c.storage(key, "old")
        for key in c.nodes:
            c.ready(key)
        for shard in range(1, c.shards + 1):
            c.joined(shard)
        c.gateway("old")
        c.setup()
        c.gateway("new", slot=1, refuse=True)
        c.roll("new")
        c.gateway("new", slot=1)
        c.legacy_write_refusal(1)
        c.canary((0, 1))
        for shard in range(1, c.shards + 1):
            old = c.leader(shard)
            target = next(k for k in c.nodes if k.startswith(f"{shard}.") and k != old)
            ev(c.nodes[target], "box.ctl.promote(); return true")
            assert wait("new gateway handoff", lambda: c.leader(shard) == target)
            c.ready(target)
            c.check_rows((0, 1))
        c.event("both_gateways_follow_upgraded_leaders")
        # Swap gateway versions independently, leaving durable prepared state.
        txid = c.prepare("upgrade-gateway-restart")
        c.stop("g0")
        c.gateway("new", slot=0)
        c.legacy_write_refusal(0)
        c.finish(txid)
        c.canary((0, 1))
        c.stop("g0")
        c.gateway("old", slot=0)
        c.stop("g1")
        # Format 1 adds only an ignored meta key: the tested baseline can
        # be restored after all new gateways stop, with DDL/moves drained.
        c.roll("old")
        c.canary()
        c.event("baseline_rollback_passed")
    finally:
        c.close()


def native_modes(args):
    mismatch = args.evidence / "abi-mismatch"
    mismatch.mkdir()
    subprocess.run([os.environ.get("CC", "cc"), "-shared", "-fPIC", "-Wall", "-Wextra", "-Werror",
                    str(ROOT / "tests/fixtures/abi_mismatch.c"), "-o", str(mismatch / "libtarn_storage.so")], check=True)
    broken = args.evidence / "load-failure"
    broken.mkdir()
    (broken / "libtarn_storage.so").write_bytes(b"invalid shared object: owned upgrade fixture\n")
    for label, status, native in [("ready", "ready", args.new_native),
                                  ("predecessor", "ready", args.old_native),
                                  ("disabled", "disabled", "0"),
                                  ("load_failed", "load_failed", broken / "libtarn_storage.so"),
                                  ("abi_mismatch", "abi_mismatch", mismatch / "libtarn_storage.so")]:
        c = Cluster(args, "native-" + label, shards=1, replicas=1)
        try:
            c.storage("1.1", "new", native)
            c.ready("1.1")
            report = rpc(c.nodes["1.1"], "tarn_compatibility", [])
            assert report[6] == status, report
            c.gateway("new")
            c.setup()
            c.canary()
            c.event("native_contract_passed", report=report, module_sha256=None if native == "0" else sha(native))
            if label == "ready":
                # Replace a warm backend with actual legacy code. Discovery
                # must refuse it before forwarding a non-idempotent UPDATE.
                c.stop("s1.1")
                c.storage("1.1", "old")
                c.ready("1.1")
                with c.sql(0, "upgrade") as sql:
                    try:
                        q(sql, "UPDATE t SET v=v+1 WHERE id=1")
                    except pymysql.MySQLError as error:
                        assert error.args[0] == 1235, error
                    else:
                        raise AssertionError("new gateway used downgraded storage")
                assert ev(c.nodes["1.1"], "return box.space['upgrade.t']:get{1}.v") == c.expected[1]
                c.stop("s1.1")
                c.storage("1.1", "new")
                c.ready("1.1")
                c.check_rows()
                c.event("warm_gateway_reconnect_refused_legacy_without_effect")
        finally:
            c.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("old-root", "old-bin", "old-native", "new-bin", "new-native", "evidence"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--engine", choices=("memtx", "vinyl"), default="memtx")
    args = p.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=False)
    receipts = {}
    for version, root, gateway, native in [("old", args.old_root, args.old_bin, args.old_native),
                                          ("new", ROOT, args.new_bin, args.new_native)]:
        receipt = json.loads((gateway.parent / (version + "-build.json")).read_text())
        verify_build(receipt, root, gateway, native)
        receipts[version] = receipt
    assert receipts["old"]["revision"] == BASELINE, "unsupported predecessor build"
    assert receipts["new"]["revision"] == subprocess.check_output(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip(), "wrong new source revision"
    assert sha(args.old_bin) != sha(args.new_bin), "old/new gateways are identical; refusing invalid upgrade evidence"
    manifest = {"baseline_revision": BASELINE, "old_source": source(args.old_root), "new_source": source(ROOT),
                "build_receipts": receipts,
                "artifacts": {name: {"path": str(getattr(args, name)), "sha256": sha(getattr(args, name))}
                              for name in ("old_bin", "old_native", "new_bin", "new_native")},
                "engine": args.engine, "replicas": 3, "wal_mode": "fsync",
                "tarantool": subprocess.check_output([TARANTOOL, "--version"], text=True).splitlines()[0],
                "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                "build_flags": {k: v for k, v in os.environ.items() if k.startswith("CARGO_PROFILE_RELEASE_")},
                "result": "running"}
    (args.evidence / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    try:
        fresh_cluster(args)
        rolling(args)
        native_modes(args)
        assert source(ROOT) == manifest["new_source"], "source changed during validation"
        assert source(args.old_root) == manifest["old_source"], "baseline source changed during validation"
        manifest["result"] = "passed"
    except BaseException:
        manifest["result"], manifest["error"] = "failed", traceback.format_exc()
        raise
    finally:
        (args.evidence / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print("ROLLING UPGRADE TESTS PASSED", flush=True)


if __name__ == "__main__":
    main()
