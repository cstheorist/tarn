#!/usr/bin/env python3
"""Vinyl admission and real native-pressure checks on a dedicated cluster (#58).

This program starts or stops no services. Supply two writable shard leaders
in gateway participant order, idle gateways and TARN_TEST_HOOKS=1. Test controls
are owner-leased and restored in finally. Forced admission classifications are
reported separately from the optional real engine workload.

  python3 tests/sql/vinylpressure.py --tarn 127.0.0.1:4000 \
      --shards 127.0.0.1:3311,127.0.0.1:3321

--real-pressure additionally requires a dedicated small-memory fixture. It
uses the supported snap_io_rate_limit control, actual native engine statistics
and independent SQL writers. Its measured latency is a bounded test outcome,
not a universal latency or disk-performance claim. No mutation is replayed
after a connection error or ambiguous outcome.
"""

import argparse
import base64
import collections
import hashlib
import json
import math
import os
from pathlib import Path
import threading
import time
import traceback
import uuid

import pymysql

from lazyfaults import Lease as ApplyLease
from lazytest import ev, q, rpc
from movetest import bucket_of


FIELDS = (
    "dump_bandwidth", "write_rate", "rate_limit", "blocked_writers",
    "memory_level0", "memory_tuple", "memory_tx", "memory_limit",
    "compaction_queue", "run_count", "range_count", "runs_per_range",
    "admitted", "rejected", "quota_timeouts",
)


def wait_for(fn, seconds, what):
    deadline = time.monotonic() + seconds
    while True:
        result = fn()
        if result:
            return result
        if time.monotonic() >= deadline:
            raise AssertionError(f"timeout waiting for {what}")
        time.sleep(0.05)


def quantiles(values):
    values = sorted(values)
    if not values:
        return {}
    return {name: values[min(len(values) - 1, math.ceil(fraction * len(values)) - 1)]
            for name, fraction in (("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99), ("max_ms", 1))}


INSTALL = r"""
local owner, seconds = ...
assert(tarn.cfg.test_hooks == true and not box.info.ro, 'dedicated writable test_hooks shard required')
assert(rawget(_G, '__tarn_vinylpressure') == nil, 'another vinylpressure lease exists')
local fiber = require('fiber')
local s = {owner=owner, original_stats=assert(tarn_vinyl_stats), io_original=box.cfg.snap_io_rate_limit or 0}
function s.restore()
    assert(rawget(_G, '__tarn_vinylpressure') == s, 'vinylpressure ownership changed')
    local errors = {}
    if s.pressure_set then
        local ok,r=pcall(tarn.vinyl.test_pressure,s.owner,nil)
        if not ok or r[1]~='ok' then errors[#errors+1]='admission override restore: '..tostring(r) end
    end
    if s.stats_wrapper then
        if tarn_vinyl_stats == s.stats_wrapper then tarn_vinyl_stats=s.original_stats
        else errors[#errors+1]='statistics function ownership changed' end
    end
    if s.io_set ~= nil then
        if box.cfg.snap_io_rate_limit == s.io_set then box.cfg{snap_io_rate_limit=s.io_original}
        else errors[#errors+1]='I/O limit ownership changed' end
    end
    rawset(_G, '__tarn_vinylpressure', nil)
    return errors
end
rawset(_G, '__tarn_vinylpressure', s)
fiber.create(function()
    fiber.sleep(seconds)
    if rawget(_G, '__tarn_vinylpressure') == s then s.restore() end
end)
return {owner=owner,id=tarn.cfg.shard_id,uuid=tostring(box.info.uuid),version=box.info.version,
        vinyl_memory=box.cfg.vinyl_memory,vinyl_cache=box.cfg.vinyl_cache,
        vinyl_timeout=box.cfg.vinyl_timeout,native=tarn_info().native}
"""


class Controls:
    def __init__(self, test):
        self.test = test
        self.owner = uuid.uuid4().hex
        self.installed = []

    def __enter__(self):
        try:
            for index, node in enumerate(self.test.nodes):
                self.installed.append(index)  # the install receipt itself can be lost
                identity = ev(node, INSTALL, [self.owner, 120])
                assert identity["uuid"] == self.test.identities[index]["uuid"]
                self.test.record("control_lease", node=node, identity=identity)
            return self
        except BaseException:
            self.close()
            raise

    def io_limit(self, limit):
        for node in self.test.nodes:
            result = ev(node, """
local owner,limit=...
local s=assert(rawget(_G,'__tarn_vinylpressure'));assert(s.owner==owner)
box.cfg{snap_io_rate_limit=limit};s.io_set=limit
return {limit=box.cfg.snap_io_rate_limit,vinyl_timeout=box.cfg.vinyl_timeout,stats=tarn_vinyl_stats()}
""", [self.owner, limit])
            self.test.record("native_io_control", node=node, result=result)

    def omit_stat(self, field):
        node = self.test.nodes[0]
        return ev(node, """
local owner,field=...
local s=assert(rawget(_G,'__tarn_vinylpressure'));assert(s.owner==owner)
assert(not s.stats_wrapper)
s.stats_wrapper=function(...)
    local result=s.original_stats(...)
    if result[1]=='ok' then local copy={};for k,v in pairs(result[2]) do copy[k]=v end
        copy[field]=nil;return {'ok',copy} end
    return result
end
tarn_vinyl_stats=s.stats_wrapper
return true
""", [self.owner, field])

    def pressure(self, indices=None):
        """Explicitly forced classifier input, never a real-pressure witness."""
        for index in range(len(self.test.nodes)) if indices is None else indices:
            result = ev(self.test.nodes[index], """
local owner,limit=...
local s=assert(rawget(_G,'__tarn_vinylpressure'));assert(s.owner==owner)
local r=tarn.vinyl.test_pressure(owner,{rate_limit=0,dump_bandwidth=0,blocked_writers=1,
    memory_limit=limit,memory_level0=limit,compaction_queue=limit},30)
assert(r[1]=='ok',require('json').encode(r));s.pressure_set=true
return {result=r,actual=tarn_vinyl_stats()}
""", [self.owner, self.test.identities[index]["vinyl_memory"]])
            # The override belongs only to the guard sampler. It must never
            # misrepresent the public engine telemetry as native pressure.
            assert result["actual"][0] == "ok", result
            assert result["actual"][1]["blocked_writers"] == 0, result
            self.test.record("forced_guard_classification", node=self.test.nodes[index], result=result)

    def close(self):
        errors = []
        for index in self.installed:
            node = self.test.nodes[index]
            try:
                result = ev(node, """
local s=rawget(_G,'__tarn_vinylpressure')
if not s then return {expired=true} end
assert(s.owner==...,'vinylpressure cleanup owner mismatch')
return {errors=s.restore(),inactive=rawget(_G,'__tarn_vinylpressure')==nil}
""", [self.owner])
                self.test.record("control_restore", node=node, result=result)
                assert result.get("inactive") and not result.get("expired") and not result.get("errors"), result
            except Exception as error:
                errors.append(f"{node}: {error}")
        self.installed.clear()
        if errors:
            raise AssertionError("; ".join(errors))

    def __exit__(self, exc_type, exc, tb):
        self.close()


class Test:
    def __init__(self, args):
        self.args = args
        self.gateways = args.tarn.split(",")
        self.nodes = args.shards.split(",")
        assert len(self.nodes) == 2, "exactly two dedicated writable shards required"
        self.db = "vinylpressure_" + uuid.uuid4().hex[:12]
        self.space = self.db + ".t"
        self.created = False
        self.identities = []
        self.completed = 0
        self.evidence = Path(args.evidence or ("/tmp/tarn-vinylpressure-" + uuid.uuid4().hex))
        self.evidence.mkdir(mode=0o700, parents=False, exist_ok=False)
        self.events = (self.evidence / "events.jsonl").open("x", encoding="utf-8")
        os.chmod(self.events.name, 0o600)
        (self.evidence / "manifest.json").write_text(json.dumps({
            "database": self.db, "arguments": vars(args),
            "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "scope": "Dedicated functional checks; forced classification and real native pressure are distinct.",
        }, indent=2) + "\n")

    def record(self, event, **data):
        self.events.write(json.dumps({"event": event, "realtime": time.time(), **data}, default=str) + "\n")
        self.events.flush()

    def connect(self, gateway=None, database=True):
        host, port = (gateway or self.gateways[0]).rsplit(":", 1)
        return pymysql.connect(host=host, port=int(port), user=self.args.user, password="",
                               database=self.db if database else None, autocommit=True,
                               charset="utf8mb4", connect_timeout=3, read_timeout=15, write_timeout=5)

    def stats(self, node):
        result = rpc(node, "tarn_vinyl_stats", [])
        assert len(result) == 2 and result[0] == "ok" and isinstance(result[1], dict), result
        return result[1]

    def status(self, connection):
        return dict(q(connection, "SHOW TARN STATUS"))

    def setup(self):
        for node in self.nodes:
            identity = ev(node, """
assert(tarn.cfg.test_hooks==true and not box.info.ro,'dedicated writable test_hooks shard required')
return {id=tarn.cfg.shard_id,uuid=tostring(box.info.uuid),version=box.info.version,
        vinyl_memory=box.cfg.vinyl_memory,vinyl_cache=box.cfg.vinyl_cache,
        vinyl_timeout=box.cfg.vinyl_timeout,native=tarn_info().native}
""")
            assert identity["version"].startswith("3.9."), identity
            if self.args.require_native:
                assert identity["native"]["ready"] == 1, identity
            self.identities.append(identity)
        assert len({identity["id"] for identity in self.identities}) == 2
        self.record("preflight", identities=self.identities)
        with self.connect(database=False) as c:
            q(c, f"CREATE DATABASE {self.db}")
            self.created = True
            c.select_db(self.db)
            q(c, "CREATE TABLE t (id INT PRIMARY KEY,k INT,v INT) ENGINE=vinyl")
            q(c, "CREATE TABLE m (id INT PRIMARY KEY,v INT) ENGINE=memtx")
            q(c, "CREATE TABLE p (tenant_id INT NOT NULL,id INT NOT NULL,v VARBINARY(8192),"
                 "PRIMARY KEY(tenant_id,id)) PARTITION BY KEY(tenant_id) ENGINE=vinyl")
            ranges = q(c, "SHOW TARN BUCKETS")
            self.bucket_count = sum(int(row[1]) for row in ranges)
            owners = {}
            for row in ranges:
                owned = owners.setdefault(int(row[0]), set())
                for part in row[2].split(", ") if row[2] else []:
                    lo, _, hi = part.partition("-")
                    owned.update(range(int(lo), int(hi or lo) + 1))
            self.keys = []
            for identity in self.identities:
                keys = [key for key in range(1, 10000) if bucket_of(key, self.bucket_count) in owners[identity["id"]]]
                assert len(keys) >= 20, "insufficient routed fixture keys"
                self.keys.append(keys[:20])
        for node in self.nodes:
            for table in ("t", "p"):
                actual = ev(node, "local s=assert(box.space[...]);return {engine=s.engine,id=s.id}", [self.db + "." + table])
                assert actual["engine"] == "vinyl", actual
        # Every gateway must first observe the new catalog. A gateway whose
        # cache contains only memtx tables deliberately skips Vinyl polling.
        for gateway in self.gateways:
            with self.connect(gateway) as c:
                assert q(c, "SELECT COUNT(*) FROM t") == ((0,),)
        self.record("setup", bucket_count=self.bucket_count, keys=self.keys)

    def test_telemetry(self):
        for index, node in enumerate(self.nodes):
            native = self.stats(node)
            info = rpc(node, "tarn_info", [])
            assert set(FIELDS).issubset(native), native
            assert set(FIELDS).issubset(info["vinyl"]), info
            for field in FIELDS:
                value = native[field]
                assert value is None or isinstance(value, (int, float)) and math.isfinite(value) and value >= 0, (field, value)
            assert native["memory_limit"] == self.identities[index]["vinyl_memory"]
            for gateway in self.gateways:
                with self.connect(gateway) as c:
                    prefix = f"Tarn_vinyl_shard_{self.identities[index]['id']}_"
                    status = wait_for(lambda: (s if (s := self.status(c)).get(prefix + "sample") == "current" else None),
                                      6, "current SQL vinyl telemetry")
                    assert all(prefix + field in status for field in FIELDS), status
                    assert int(status[prefix + "memory_limit"]) == native["memory_limit"]
            self.record("native_telemetry", node=node, stats=native)

    def test_unknown_metric_is_null(self):
        # This is an explicit telemetry-omission control, not native pressure.
        prefix = f"Tarn_vinyl_shard_{self.identities[0]['id']}_"
        with Controls(self) as control:
            control.omit_stat("dump_bandwidth")
            for gateway in self.gateways:
                with self.connect(gateway) as c:
                    status = wait_for(lambda: (s if (s := self.status(c)).get(prefix + "dump_bandwidth") == "NULL" else None),
                                      6, "unknown metric remaining NULL")
                    assert status[prefix + "sample"] == "current", status
                    self.record("controlled_metric_omission", gateway=gateway, status=status)
        for gateway in self.gateways:
            with self.connect(gateway) as c:
                wait_for(lambda: self.status(c).get(prefix + "dump_bandwidth") not in (None, "NULL"),
                         6, "restored native telemetry")

    def refused(self, fn, label):
        started = time.monotonic()
        try:
            fn()
        except pymysql.MySQLError as error:
            elapsed_ms = (time.monotonic() - started) * 1000
            self.record("controlled_refusal", label=label, error=list(error.args), latency_ms=elapsed_ms)
            assert error.args[0] == 1205, (label, error.args)
            assert elapsed_ms < self.args.max_latency_ms, (label, elapsed_ms)
            return
        raise AssertionError(label + " unexpectedly succeeded under forced zero credit")

    def assert_absent(self, connection, keys, table="t"):
        placeholders = ",".join(["%s"] * len(keys))
        assert q(connection, f"SELECT id FROM {table} WHERE id IN ({placeholders})", tuple(keys)) == (), (table, keys)

    def test_autocommit_refusal_and_memtx_bypass(self):
        before = self.stats(self.nodes[0])["rejected"]
        keys = self.keys[0][:len(self.gateways)]
        with Controls(self) as control:
            control.pressure()
            for gateway, key in zip(self.gateways, keys):
                with self.connect(gateway) as c:
                    self.refused(lambda: q(c, "INSERT INTO t VALUES (%s,0,1)", (key,)), "autocommit " + gateway)
                    self.assert_absent(c, [key])
            with self.connect() as c:
                q(c, "INSERT INTO m VALUES (%s,1)", (self.keys[0][15],))
                assert q(c, "SELECT v FROM m WHERE id=%s", (self.keys[0][15],)) == ((1,),)
                q(c, "BEGIN")
                assert q(c, "SELECT id FROM t") == ()
                q(c, "COMMIT")
        assert self.stats(self.nodes[0])["rejected"] > before, "production admission guard was not reached"

    def test_local_and_rc_refusal_have_no_effects(self):
        for mode, index in (("SERIALIZABLE", 8), ("READ-COMMITTED", 9)):
            key = self.keys[0][index]
            with self.connect() as c:
                q(c, "SET SESSION transaction_isolation=%s", (mode,))
                q(c, "BEGIN")
                q(c, "INSERT INTO t VALUES (%s,0,1)", (key,))
                try:
                    with Controls(self) as control:
                        control.pressure()
                        self.refused(lambda: q(c, "COMMIT"), mode + " local COMMIT")
                finally:
                    q(c, "ROLLBACK")
                self.assert_absent(c, [key])

    def test_mixed_engine_commit_is_atomic_on_refusal(self):
        key = self.keys[0][10]
        with self.connect() as c:
            q(c, "BEGIN")
            q(c, "INSERT INTO m VALUES (%s,7)", (key,))
            q(c, "INSERT INTO t VALUES (%s,0,7)", (key,))
            try:
                with Controls(self) as control:
                    control.pressure()
                    self.refused(lambda: q(c, "COMMIT"), "mixed-engine local COMMIT")
            finally:
                q(c, "ROLLBACK")
            self.assert_absent(c, [key])
            self.assert_absent(c, [key], "m")

    def test_prepare_refusal_has_no_partial_commit(self):
        keys = [self.keys[0][11], self.keys[1][11]]
        with self.connect() as c:
            q(c, "BEGIN")
            for key in keys:
                q(c, "INSERT INTO t VALUES (%s,0,9)", (key,))
            try:
                with Controls(self) as control:
                    # The first participant may vote YES. A fresh PREPARE on
                    # the second must refuse and the coordinator must abort.
                    control.pressure([1])
                    self.refused(lambda: q(c, "COMMIT"), "cross-shard PREPARE")
            finally:
                q(c, "ROLLBACK")
            self.assert_absent(c, keys)

    def test_known_committed_apply_bypasses_admission(self):
        keys = [self.keys[0][12], self.keys[1][12]]
        with self.connect() as c:
            q(c, "BEGIN")
            for key in keys:
                q(c, "INSERT INTO t VALUES (%s,0,42)", (key,))
            # Existing participant-fault infrastructure retains a real YES
            # and COMMITTED decision while physical APPLY is still pending.
            with ApplyLease(self, keys[1]) as apply:
                q(c, "COMMIT")
                apply.known_committed = True
                state = apply.snapshot()
                assert state["state"] == "prepared" and state["row"] is None, state
                assert state["dshard"] == self.identities[0]["id"], state
                with Controls(self) as control:
                    control.pressure()
                    apply.release()
                    result = rpc(self.nodes[1], "tarn_apply", [state["txid"]])
                    assert result in (["ok", "applied"], ["ok", "none"]), result
                    rows = q(c, "SELECT id,v FROM t WHERE id IN (%s,%s) ORDER BY id", tuple(keys))
                    assert rows == tuple((key, 42) for key in sorted(keys)), rows
                    self.record("known_committed_apply_under_forced_admission", txid=state["txid"], result=result, rows=rows)

    def test_real_native_pressure(self):
        identity = self.identities[0]
        assert 1024 * 1024 <= identity["vinyl_memory"] <= 32 * 1024 * 1024, identity
        assert identity["vinyl_cache"] == 0, "real-pressure fixture requires cache zero"
        assert 0 < identity["vinyl_timeout"] <= .5, "real-pressure fixture requires the short native quota wait"
        tenant = self.keys[0][0]
        records, samples, errors = [], [], []
        lock = threading.Lock()
        stop = threading.Event()
        sampling_stop = threading.Event()
        ready = threading.Barrier(self.args.writers + 1)
        phase = ["native_io_uncapped"]
        attempts = [0]

        def sample():
            while not sampling_stop.is_set():
                try:
                    value = ev(self.nodes[0], """
local space=assert(box.space[...]);local indices={}
for iid,index in pairs(space.index) do if type(iid)=='number' then
    local s=index:stat();indices[#indices+1]={id=iid,run_count=s.run_count,range_count=s.range_count,
        run_avg=s.run_avg,run_histogram=s.run_histogram,compaction=s.disk.compaction} end end
return {realtime=require('clock').realtime(),native=box.stat.vinyl(),
    public=tarn_vinyl_stats(),indices=indices,vinyl_timeout=box.cfg.vinyl_timeout,
    vinyl_memory=box.cfg.vinyl_memory,vinyl_cache=box.cfg.vinyl_cache,
    snap_io_rate_limit=box.cfg.snap_io_rate_limit or 0,engine=space.engine}
""", [self.db + ".p"])
                    with lock:
                        samples.append(value)
                except Exception as error:
                    with lock:
                        errors.append("native sample: " + repr(error))
                    return
                sampling_stop.wait(.2)

        def writer(index):
            c = None
            try:
                c = self.connect(self.gateways[index % len(self.gateways)])
                ready.wait(timeout=10)
                sequence = 0
                while not stop.is_set():
                    with lock:
                        if attempts[0] >= 20000:
                            return
                        attempts[0] += 1
                    sequence += 1
                    key = (index + 1) * 1000000 + sequence
                    payload = base64.b64encode(os.urandom(6144))
                    entry = {"worker": index, "id": key, "started_phase": phase[0], "started_realtime": time.time()}
                    started = time.monotonic()
                    try:
                        q(c, "INSERT INTO p VALUES (%s,%s,%s)", (tenant, key, payload))
                        entry["result"] = "ok"
                    except pymysql.MySQLError as error:
                        entry["result"] = "error"
                        entry["error"] = list(error.args)
                    entry["latency_ms"] = (time.monotonic() - started) * 1000
                    entry["completed_phase"] = phase[0]
                    with lock:
                        records.append(entry)
                    if entry["result"] == "error" and entry["error"][0] != 1205:
                        # Never resend this potentially uncertain mutation.
                        stop.set()
                        return
            except BaseException as error:
                with lock:
                    errors.append("SQL writer: " + repr(error))
            finally:
                if c is not None:
                    c.close()

        sampler = threading.Thread(target=sample, daemon=True)
        writers = [threading.Thread(target=writer, args=(index,), daemon=True) for index in range(self.args.writers)]
        with Controls(self) as control:
            sampler.start()
            for thread in writers:
                thread.start()
            try:
                ready.wait(timeout=10)
                self.record("real_pressure_begin", source=self.nodes[0], tenant=tenant,
                            note="native I/O control; no forced classifier override", identity=identity)
                time.sleep(3)
                phase[0] = "native_io_limited"
                control.io_limit(.5)
                time.sleep(self.args.pressure_seconds)
            finally:
                # Release real engine I/O before draining any client call.
                control.io_limit(0)
                phase[0] = "native_io_released"
                time.sleep(3)
                stop.set()
                deadline = time.monotonic() + 10
                for thread in writers:
                    thread.join(max(0, deadline - time.monotonic()))
                sampling_stop.set()
                sampler.join(12)
                # Preserve a failed overload witness as well as a passing one.
                (self.evidence / "pressure-attempts.jsonl").write_text("".join(json.dumps(row) + "\n" for row in records))
                (self.evidence / "pressure-native-samples.jsonl").write_text("".join(json.dumps(row) + "\n" for row in samples))
        success = {row["id"] for row in records if row["result"] == "ok"}
        refused = {row["id"] for row in records if row["result"] == "error" and row["error"][0] == 1205}
        unexpected = [row for row in records if row["result"] == "error" and row["error"][0] != 1205]
        queue = max((row["native"]["scheduler"]["compaction_queue"] for row in samples), default=0)
        blocked = max((row["native"]["regulator"]["blocked_writers"] for row in samples), default=0)
        level0 = max((row["native"]["memory"]["level0"] for row in samples), default=0)
        outcomes = {}
        for result in ("ok", "error"):
            values = [row["latency_ms"] for row in records if row["result"] == result]
            outcomes[result] = {"count": len(values), **quantiles(values)}
        summary = {"attempts": attempts[0], "completed": len(records),
                   "acknowledged": len(success), "refused_1205": len(refused),
                   "stored_ids_match_acknowledgements": None, "refused_ids_absent": None,
                   "by_outcome": outcomes, "worker_errors": errors,
                   "compaction_queue_max_bytes": queue, "blocked_writers_max": blocked,
                   "memory_level0_max_bytes": level0, "unexpected_errors": unexpected,
                   "error_counts": dict(collections.Counter(str(row.get("error")) for row in records if row["result"] == "error"))}
        summary_path = self.evidence / "pressure-summary.json"
        summary_path.write_text(json.dumps(summary, indent=2) + "\n")
        assert not any(thread.is_alive() for thread in writers) and not sampler.is_alive(), "pressure worker exceeded drain cap"
        assert not errors, errors
        assert len(records) == attempts[0] and records and samples, (len(records), attempts, len(samples))
        assert attempts[0] < 20000, "attempt cap reached before sustained pressure completed"
        assert success and refused and not unexpected, (len(success), len(refused), unexpected)
        with self.connect() as c:
            actual = {row[0] for row in q(c, "SELECT id FROM p WHERE tenant_id=%s", (tenant,))}
        summary.update(stored_ids_match_acknowledgements=actual == success,
                       refused_ids_absent=not bool(actual & refused))
        summary_path.write_text(json.dumps(summary, indent=2) + "\n")
        assert actual == success and not (actual & refused), "acknowledged/refused key identities differ from stored rows"
        assert all(row["engine"] == "vinyl" and row["vinyl_memory"] == identity["vinyl_memory"]
                   and row["vinyl_cache"] == 0 and row["vinyl_timeout"] == identity["vinyl_timeout"] for row in samples)
        assert any(row["snap_io_rate_limit"] == .5 for row in samples), "native I/O control was not sampled"
        assert any(row["native"]["regulator"]["rate_limit"] <= 524288 for row in samples), "real regulator limit was not observed"
        assert queue > 0, "no actual queued compaction input; pressure fixture did not exercise compaction"
        assert blocked > 0 or level0 >= .75 * identity["vinyl_memory"] or queue >= identity["vinyl_memory"], "no actual native pressure witness"
        assert any(index["run_count"] > index["range_count"] > 0 for row in samples for index in row["indices"]), "no real run amplification"
        for result in ("ok", "error"):
            assert outcomes[result]["max_ms"] < self.args.max_latency_ms, (result, outcomes[result])
        self.record("real_pressure_completed", summary=summary)
        print("  pressure " + json.dumps({
            "attempts": summary["attempts"], "acknowledged": len(success), "refused_1205": len(refused),
            "success_latency_ms": outcomes["ok"], "refusal_latency_ms": outcomes["error"],
            "compaction_queue_bytes": queue, "blocked_writers": blocked,
            "acknowledged_and_refused_ids_verified": actual == success and not bool(actual & refused),
        }, sort_keys=True), flush=True)

    def cleanup(self):
        errors = []
        if self.created:
            try:
                with self.connect(database=False) as c:
                    q(c, f"DROP DATABASE {self.db}")
                self.created = False
            except Exception as error:
                errors.append(str(error))
        self.record("cleanup", errors=errors)
        self.events.close()
        if errors:
            raise AssertionError("; ".join(errors))

    def run(self):
        try:
            self.setup()
            for name in ("test_telemetry", "test_unknown_metric_is_null",
                         "test_autocommit_refusal_and_memtx_bypass",
                         "test_local_and_rc_refusal_have_no_effects",
                         "test_mixed_engine_commit_is_atomic_on_refusal",
                         "test_prepare_refusal_has_no_partial_commit",
                         "test_known_committed_apply_bypasses_admission"):
                print(name, flush=True)
                getattr(self, name)()
                self.completed += 1
                print("  ok ", name, flush=True)
            if self.args.real_pressure:
                print("test_real_native_pressure", flush=True)
                self.test_real_native_pressure()
                self.completed += 1
                print("  ok  test_real_native_pressure", flush=True)
        finally:
            self.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tarn", required=True, help="dedicated gateway endpoint(s)")
    parser.add_argument("--shards", required=True, help="two writable leaders in gateway order")
    parser.add_argument("--user", default="sb")
    parser.add_argument("--evidence", help="new private evidence directory")
    parser.add_argument("--require-native", action="store_true")
    parser.add_argument("--real-pressure", action="store_true", help="also run supported I/O-control overload workload")
    parser.add_argument("--writers", type=int, default=8)
    parser.add_argument("--pressure-seconds", type=int, default=10)
    parser.add_argument("--max-latency-ms", type=int, default=2000, help="observed single-row-call test ceiling")
    args = parser.parse_args()
    assert 1 <= len(args.tarn.split(",")) <= 8
    assert 2 <= args.writers <= 16 and 5 <= args.pressure_seconds <= 20
    assert 500 <= args.max_latency_ms <= 5000
    test = Test(args)
    try:
        test.run()
    except BaseException:
        traceback.print_exc()
        print(f"FAILED; evidence {test.evidence}", flush=True)
        return 1
    print(f"{test.completed} completed, 0 failures; evidence {test.evidence}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
