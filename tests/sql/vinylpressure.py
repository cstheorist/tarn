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
import copy
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import threading
import time
import traceback
import sys
import uuid

import pymysql

from lazyfaults import Lease as ApplyLease, decision_node
from lazytest import ev, q, rpc
from movetest import bucket_of


FIELDS = (
    "dump_bandwidth", "write_rate", "rate_limit", "blocked_writers",
    "memory_level0", "memory_tuple", "memory_tx", "memory_limit",
    "compaction_queue", "run_count", "range_count", "runs_per_range",
    "admitted", "rejected", "quota_timeouts",
)
PRESSURE_ATTEMPT_CAP = 20000
PRESSURE_PHASES = ("native_io_uncapped", "native_io_limited", "native_io_released")
COMPACTION_SEED_ROWS = 528


class PressureSchedule:
    """Bounded phase-local slots; a fast worker parks instead of ending early."""
    def __init__(self, writers, pressure_seconds):
        self.writers = writers
        durations = (3, pressure_seconds, 3)
        total = sum(durations)
        budgets = [PRESSURE_ATTEMPT_CAP * seconds // total for seconds in durations[:2]]
        budgets.append(PRESSURE_ATTEMPT_CAP - sum(budgets))
        self.phases = [
            {"name": name, "duration_seconds": seconds, "slot_budget": budget,
             "opened_monotonic": None, "deadline_monotonic": None, "closed_monotonic": None,
             "close_reason": None, "io_receipts": None,
             "workers": [{"reserved": 0, "skipped": 0, "expired": 0, "dispatched": 0, "completed": 0,
                          "inflight": 0, "first_dispatch_monotonic": None, "last_dispatch_monotonic": None,
                          "outcomes": {}} for _ in range(writers)]}
            for name, seconds, budget in zip(PRESSURE_PHASES, durations, budgets)]
        self.lock = threading.Lock()
        self.active = None
        self.next_phase = 0
        self.next_slots = list(range(writers))
        self.pending = {}
        self.inflight = {}
        self.dispatched = 0
        self.finished = False
        self.finish_reason = None

    def open_phase(self, name, now, receipts=None):
        with self.lock:
            assert not self.finished and self.active is None and self.next_phase < len(self.phases)
            phase = self.phases[self.next_phase]
            assert phase["name"] == name
            phase.update(opened_monotonic=now, deadline_monotonic=now + phase["duration_seconds"],
                         io_receipts=receipts)
            self.active = self.next_phase
            self.next_slots = list(range(self.writers))

    def close_phase(self, name, now, reason="completed"):
        with self.lock:
            assert self.active is not None and self.phases[self.active]["name"] == name
            phase = self.phases[self.active]
            phase.update(closed_monotonic=now, close_reason=reason, workers_at_close=copy.deepcopy(phase["workers"]))
            self.active = None
            self.next_phase += 1

    def claim(self, worker, now):
        with self.lock:
            if self.finished:
                return {"kind": "done"}
            if self.active is None:
                return {"kind": "wait", "seconds": .05}
            phase = self.phases[self.active]
            if now >= phase["deadline_monotonic"]:
                return {"kind": "wait", "seconds": .05}
            assert worker not in self.pending and worker not in self.inflight
            ordinal = self.next_slots[worker]
            if ordinal >= phase["slot_budget"]:
                return {"kind": "wait", "seconds": phase["deadline_monotonic"] - now}
            step = phase["duration_seconds"] / phase["slot_budget"]
            target = phase["opened_monotonic"] + ordinal * step
            if now < target:
                return {"kind": "wait", "seconds": target - now}
            # Permit one overdue slot, then skip missed successors. A slow
            # SQL call never earns a catch-up burst or another phase's slots.
            following = ordinal + self.writers
            first_future = math.floor((now - phase["opened_monotonic"]) / step) + 1
            first_future += (worker - first_future) % self.writers
            next_slot = max(following, first_future)
            remaining = lambda slot: max(0, (phase["slot_budget"] - 1 - slot) // self.writers + 1)
            stats = phase["workers"][worker]
            stats["skipped"] += remaining(following) - remaining(next_slot)
            stats["reserved"] += 1
            self.next_slots[worker] = next_slot
            ticket = {"kind": "permit", "worker": worker, "phase": phase["name"],
                      "phase_index": self.active, "ordinal": ordinal, "scheduled_monotonic": target,
                      "window_start_monotonic": phase["opened_monotonic"],
                      "window_deadline_monotonic": phase["deadline_monotonic"]}
            self.pending[worker] = ticket
            return ticket

    def cancel(self, ticket):
        with self.lock:
            assert self.pending.pop(ticket["worker"]) is ticket
            self.phases[ticket["phase_index"]]["workers"][ticket["worker"]]["expired"] += 1

    def dispatch(self, ticket, now):
        with self.lock:
            worker = ticket["worker"]
            assert self.pending.pop(worker) is ticket
            phase = self.phases[ticket["phase_index"]]
            stats = phase["workers"][worker]
            if (self.finished or self.active != ticket["phase_index"]
                    or not phase["opened_monotonic"] <= now < phase["deadline_monotonic"]):
                stats["expired"] += 1
                return None
            # Payload work may have taken time after claim(). Advance the
            # successor against actual dispatch too, so it cannot already
            # be overdue immediately after this call completes.
            step = phase["duration_seconds"] / phase["slot_budget"]
            first_future = math.floor((now - phase["opened_monotonic"]) / step) + 1
            first_future += (worker - first_future) % self.writers
            following = self.next_slots[worker]
            next_slot = max(following, first_future)
            remaining = lambda slot: max(0, (phase["slot_budget"] - 1 - slot) // self.writers + 1)
            stats["skipped"] += remaining(following) - remaining(next_slot)
            self.next_slots[worker] = next_slot
            self.dispatched += 1
            assert self.dispatched <= PRESSURE_ATTEMPT_CAP
            stats["dispatched"] += 1
            stats["inflight"] += 1
            if stats["first_dispatch_monotonic"] is None:
                stats["first_dispatch_monotonic"] = now
            stats["last_dispatch_monotonic"] = now
            dispatch = {**ticket, "attempt": self.dispatched, "started_monotonic": now}
            self.inflight[worker] = dispatch
            return dispatch

    def complete(self, dispatch, now, result):
        with self.lock:
            worker = dispatch["worker"]
            assert self.inflight.pop(worker) is dispatch
            stats = self.phases[dispatch["phase_index"]]["workers"][worker]
            stats["completed"] += 1
            stats["inflight"] -= 1
            stats["outcomes"][result] = stats["outcomes"].get(result, 0) + 1
            if self.active is not None and now < self.phases[self.active]["deadline_monotonic"]:
                return self.phases[self.active]["name"]
            return "between_phases"

    def finish(self, now, reason="completed"):
        with self.lock:
            if self.active is not None:
                phase = self.phases[self.active]
                phase.update(closed_monotonic=now, close_reason=reason, workers_at_close=copy.deepcopy(phase["workers"]))
                self.active = None
            self.finished, self.finish_reason = True, reason

    def snapshot(self):
        with self.lock:
            return copy.deepcopy({"attempt_cap": PRESSURE_ATTEMPT_CAP, "attempts": self.dispatched,
                                  "finished": self.finished, "finish_reason": self.finish_reason,
                                  "phases": self.phases, "pending_slots": list(self.pending.values()),
                                  "inflight_dispatches": list(self.inflight.values())})


def run_pressure_phases(schedule, io_limit, wait, now=time.monotonic, record=lambda *a, **kw: None):
    """Real controller path, with injectable time/I/O only for offline controls."""
    result = {"receipts": {}, "controls": [], "primary": None, "restore_errors": []}
    active = None
    stage = "uncapped I/O control"
    def apply(name, limit):
        timing = {"phase": name, "limit": limit, "started_monotonic": now(), "returned_monotonic": None}
        result["controls"].append(timing)
        try:
            receipt = io_limit(limit)
        except BaseException as error:
            timing["error"] = repr(error)
            raise
        timing["returned_monotonic"] = now()
        return receipt
    try:
        for name, limit in zip(PRESSURE_PHASES[:2], (0, .5)):
            stage = name + " I/O control"
            receipt = apply(name, limit)
            result["receipts"][name] = receipt
            schedule.open_phase(name, now(), receipt)
            active = name
            record("real_pressure_phase_open", phase=name, timing=schedule.snapshot())
            stage = name + " window"
            if wait(schedule.phases[schedule.active]["duration_seconds"]):
                raise AssertionError("pressure writers stopped during " + name)
            schedule.close_phase(name, now())
            active = None
            record("real_pressure_phase_close", phase=name, timing=schedule.snapshot())
    except BaseException as error:
        result["primary"] = (stage, error, error.__traceback__)
    finally:
        if active is not None:
            schedule.close_phase(active, now(), "aborted")
        # Release actual engine I/O before draining any client call, even
        # after a missing witness, worker error or control exception.
        try:
            receipt = apply(PRESSURE_PHASES[2], 0)
            result["receipts"][PRESSURE_PHASES[2]] = receipt
        except BaseException as error:
            result["restore_errors"].append(repr(error))
            if result["primary"] is None:
                result["primary"] = ("released I/O control", error, error.__traceback__)
        if result["primary"] is None:
            name = PRESSURE_PHASES[2]
            try:
                schedule.open_phase(name, now(), receipt)
                record("real_pressure_phase_open", phase=name, timing=schedule.snapshot())
                if wait(3):
                    raise AssertionError("pressure writers stopped during " + name)
                schedule.close_phase(name, now())
                record("real_pressure_phase_close", phase=name, timing=schedule.snapshot())
            except BaseException as error:
                result["primary"] = (name + " window", error, error.__traceback__)
        schedule.finish(now(), "completed" if result["primary"] is None else "aborted")
    return result


def audit_pressure_keys(connect, tenant, records, inflight, workers_live, query=q):
    """One bounded read-only observation; it never settles/replays unknown work."""
    success = {row["id"] for row in records if row["result"] == "ok"}
    refused = {row["id"] for row in records if row["result"] == "error" and row.get("error", [None])[0] == 1205}
    unknown = {row["id"] for row in records if row["id"] not in success | refused}
    pending = {row["id"] for row in inflight}
    result = {"status": "failed", "attempted": True, "row_limit": PRESSURE_ATTEMPT_CAP + 1,
              "started_monotonic": time.monotonic(),
              "unknown_ids": sorted(unknown), "inflight_ids": sorted(pending),
              "workers_live": workers_live, "stored_ids_match_acknowledgements": None, "refused_ids_absent": None}
    try:
        with connect() as connection:
            rows = query(connection, "SELECT id FROM p WHERE tenant_id=%s ORDER BY id LIMIT 20001", (tenant,))
        actual = {row[0] for row in rows}
        clipped = len(rows) > PRESSURE_ATTEMPT_CAP
        unsettled = bool(workers_live or pending or unknown or clipped)
        result.update(status="incomplete" if unsettled else "complete", observed_ids=sorted(actual),
                      row_count=len(rows), limit_reached=clipped,
                      acknowledged_ids_missing=None if clipped else sorted(success - actual),
                      refused_ids_present=sorted(actual & refused),
                      unexpected_ids=sorted(actual - success - unknown - pending),
                      unknown_ids_observed=sorted(actual & unknown),
                      inflight_ids_observed=sorted(actual & pending),
                      stored_ids_match_acknowledgements=actual == success if not unsettled else None,
                      refused_ids_absent=not bool(actual & refused) if not clipped else None)
    except BaseException as error:
        result["error"] = repr(error)
    result["finished_monotonic"] = time.monotonic()
    return result


def finalize_pressure(summary, *, audit, persist, check):
    """Read back and retain evidence before any sustained-pressure assertion."""
    try:
        summary["readback"] = audit()
    except BaseException as error:
        summary["readback"] = {"status": "failed", "attempted": True, "error": repr(error)}
    summary["stored_ids_match_acknowledgements"] = summary["readback"].get("stored_ids_match_acknowledgements")
    summary["refused_ids_absent"] = summary["readback"].get("refused_ids_absent")
    evidence_errors = summary.setdefault("evidence_errors", [])
    first_persist_error = None
    def save(attempt):
        nonlocal first_persist_error
        try:
            persist(summary)
        except BaseException as error:
            evidence_errors.append({"stage": "summary persist", "attempt": attempt,
                                    "type": type(error).__name__, "message": str(error)})
            if first_persist_error is None:
                first_persist_error = (error, error.__traceback__)
    save(1)
    checked, check_error = None, None
    try:
        checked = check(summary)
    except BaseException as error:
        summary["check_failure"] = {"type": type(error).__name__, "message": str(error)}
        if summary.get("primary_failure") is None:
            summary["primary_failure"] = {"stage": summary.get("check_stage", "witness"),
                                          **summary["check_failure"]}
        check_error = (error, error.__traceback__)
    if check_error is None and first_persist_error is not None and summary.get("primary_failure") is None:
        error = first_persist_error[0]
        summary["primary_failure"] = {"stage": "summary persist", "type": type(error).__name__, "message": str(error)}
    save(2)
    if check_error is not None:
        raise check_error[0].with_traceback(check_error[1])
    if first_persist_error is not None:
        raise first_persist_error[0].with_traceback(first_persist_error[1])
    return checked


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


def native_limit_witness(source_limit, samples):
    """Observe the actual regulator together with its .5 MiB/s setting.

    The caller selects the immediate receipt of the pressured shard, not an
    unloaded peer. A low rate before the control was set is not its witness.
    """
    control = (source_limit["limit"] == .5
               and source_limit["native"]["regulator"]["rate_limit"] <= 524288)
    periodic = any(row["snap_io_rate_limit"] == .5
                   and row["native"]["regulator"]["rate_limit"] <= 524288 for row in samples)
    return control, periodic


def compaction_witness(control, samples, audit):
    """Require the same real seed's queue and completed on-disk work."""
    assert audit['status'] == 'complete' and audit['matches'], audit
    assert control and control['after']['disk']['compaction']['queue']['bytes'] > 0, \
        'public compaction request did not observe real queued input'
    initial = control['before']['disk']['compaction']
    final = audit['index']['disk']['compaction']
    assert final['count'] > initial['count'] and final['input']['bytes'] > initial['input']['bytes'], \
        'seed compaction did not complete real disk work'
    assert any(row['snap_io_rate_limit'] == .5
               and row['compaction_fixture']['disk']['compaction']['queue']['bytes'] > 0 for row in samples), \
        'seed queue was not independently sampled during limited I/O'


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
        receipts = []
        for node in self.test.nodes:
            result = ev(node, """
local owner,limit,fixture=...
local s=assert(rawget(_G,'__tarn_vinylpressure'));assert(s.owner==owner)
box.cfg{snap_io_rate_limit=limit};s.io_set=limit
local compaction
if limit==.5 and fixture~=nil then
    assert(not s.compaction_requested,'compaction control must be submitted once')
    local space=assert(box.space[fixture]);assert(space.engine=='vinyl')
    local index=space.index[0]
    local before=index:stat()
    assert(before.run_count>before.range_count,'compaction seed lacks multiple runs')
    s.compaction_requested=true
    index:compact()
    -- The public API marks real on-disk runs for compaction. Observe its
    -- native queue in this same non-yielding entry, before a fast worker
    -- can complete it. Periodic observations remain independently required.
    compaction={space=fixture,before=before,after=index:stat(),api='index:compact()'}
end
-- Record the real regulator in this same call: a completed dump may
-- recalculate it before the independent periodic sampler next runs.
local native=box.stat.vinyl()
return {limit=box.cfg.snap_io_rate_limit,vinyl_timeout=box.cfg.vinyl_timeout,compaction=compaction,
    native=native,realtime=require('clock').realtime(),stats=tarn_vinyl_stats()}
""", [self.owner, limit, getattr(self.test, 'compaction_fixture', None) if node == self.test.nodes[0] else None])
            self.test.record("native_io_control", node=node, result=result)
            receipts.append({"node": node, "result": result})
        return receipts

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
            "client": {"distribution_version": importlib.metadata.version("PyMySQL"),
                       "module_version": pymysql.__version__, "module_path": pymysql.__file__,
                       "python_version": sys.version},
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
                decision = decision_node(self.nodes, self.identities, state)
                # If the pending participant is also the decision writer,
                # its APPLY has not yet turned STAGING into COMMITTED. The
                # successful SQL commit already proved unanimous YES;
                # establish the same confirmed decision witness for either
                # placement before forcing admission pressure.
                rpc(decision, "tarn_txn_cas", [state["txid"], "staging", "committed"])
                assert ev(decision, "local r=box.space._tarn_txn:get{...}; return r and r.status",
                          [state["txid"]]) == "committed", state
                with Controls(self) as control:
                    control.pressure()
                    apply.release()
                    result = rpc(self.nodes[1], "tarn_apply", [state["txid"]])
                    assert result in (["ok", "applied"], ["ok", "none"]), result
                    rows = q(c, "SELECT id,v FROM t WHERE id IN (%s,%s) ORDER BY id", tuple(keys))
                    assert rows == tuple((key, 42) for key in sorted(keys)), rows
                    self.record("known_committed_apply_under_forced_admission", txid=state["txid"], result=result, rows=rows)

    def prepare_compaction_fixture(self, tenant):
        """Two unequal immutable runs, bounded at 528 x 8 KiB, on the source.

        This is a separate native-work seed, not part of the 20,000 pressure
        attempts. SQL establishes it; no raw tuple or classifier mutation.
        Public checkpoints finish the two dumps before timed writer windows.
        """
        self.compaction_fixture = self.db + '.c'
        self.compaction_expected = {}
        with self.connect() as c:
            q(c, 'CREATE TABLE c (tenant_id INT NOT NULL,id INT NOT NULL,v VARBINARY(8192),'
                 'PRIMARY KEY(tenant_id,id)) PARTITION BY KEY(tenant_id) ENGINE=vinyl')
            for first, last in ((1, 512), (513, COMPACTION_SEED_ROWS)):
                for start in range(first, last + 1, 8):
                    values = [(tenant, key, base64.b64encode(os.urandom(6144)).decode('ascii'))
                              for key in range(start, min(start + 8, last + 1))]
                    # Preserve attempted identities before submission. No
                    # client exception causes these INSERTs to be replayed.
                    self.record('compaction_seed_submit', ids=[v[1] for v in values])
                    with c.cursor() as cur:
                        cur.executemany('INSERT INTO c VALUES (%s,%s,%s)', values)
                    self.compaction_expected.update((key, value.encode('ascii')) for _, key, value in values)
                    self.record('compaction_seed_ack', ids=[v[1] for v in values])
                receipt = ev(self.nodes[0], """
local name=...
box.snapshot()
local fiber=require('fiber');local deadline=fiber.time()+5
while box.stat.vinyl().scheduler.tasks_inprogress>0 and fiber.time()<deadline do fiber.sleep(.01) end
assert(box.stat.vinyl().scheduler.tasks_inprogress==0,'seed dump/compaction did not drain')
local s=assert(box.space[name]);return {engine=s.engine,index=s.index[0]:stat(),native=box.stat.vinyl()}
""", [self.compaction_fixture])
                self.record('compaction_seed_dump', last_id=last, result=receipt)
            assert receipt['engine'] == 'vinyl'
            assert receipt['index']['run_count'] > receipt['index']['range_count'] > 0, receipt
        audit = self.audit_compaction_fixture(tenant)
        self.record('compaction_seed_readback', result=audit)
        assert audit['status'] == 'complete' and audit['matches'], audit

    def audit_compaction_fixture(self, tenant):
        result = {'status': 'failed', 'matches': False, 'row_limit': COMPACTION_SEED_ROWS + 1}
        try:
            with self.connect() as c:
                rows = q(c, 'SELECT id,v FROM c WHERE tenant_id=%s ORDER BY id LIMIT 529', (tenant,))
            actual = dict(rows)
            result.update(status='complete', matches=actual == self.compaction_expected,
                          rows=len(rows), expected_rows=len(self.compaction_expected),
                          observed_sha256=hashlib.sha256(b''.join(str(key).encode()+b':'+value
                                                                 for key,value in rows)).hexdigest())
            result['index'] = ev(self.nodes[0], 'local s=assert(box.space[...]);return s.index[0]:stat()',
                                 [self.compaction_fixture])
        except BaseException as error:
            result['status'], result['error'] = 'failed', repr(error)
        return result

    def test_real_native_pressure(self):
        identity = self.identities[0]
        assert 1024 * 1024 <= identity["vinyl_memory"] <= 32 * 1024 * 1024, identity
        assert identity["vinyl_cache"] == 0, "real-pressure fixture requires cache zero"
        assert 0 < identity["vinyl_timeout"] <= .5, "real-pressure fixture requires the short native quota wait"
        tenant = self.keys[0][0]
        self.prepare_compaction_fixture(tenant)
        live_records, live_samples, live_errors = [], [], []
        inflight = {}
        lock = threading.Lock()
        stop = threading.Event()
        sampling_stop = threading.Event()
        ready = threading.Barrier(self.args.writers + 1)
        schedule = PressureSchedule(self.args.writers, self.args.pressure_seconds)

        def sample():
            while not sampling_stop.is_set():
                try:
                    value = ev(self.nodes[0], """
local name,fixture=...
local space=assert(box.space[name]);local indices={}
for iid,index in pairs(space.index) do if type(iid)=='number' then
    local s=index:stat();indices[#indices+1]={id=iid,run_count=s.run_count,range_count=s.range_count,
        run_avg=s.run_avg,run_histogram=s.run_histogram,compaction=s.disk.compaction} end end
return {realtime=require('clock').realtime(),native=box.stat.vinyl(),
    public=tarn_vinyl_stats(),indices=indices,vinyl_timeout=box.cfg.vinyl_timeout,
    vinyl_memory=box.cfg.vinyl_memory,vinyl_cache=box.cfg.vinyl_cache,
    snap_io_rate_limit=box.cfg.snap_io_rate_limit or 0,engine=space.engine,
    compaction_fixture=box.space[fixture].index[0]:stat()}
""", [self.db + ".p", self.compaction_fixture])
                    with lock:
                        live_samples.append(value)
                except Exception as error:
                    with lock:
                        live_errors.append("native sample: " + repr(error))
                    return
                sampling_stop.wait(.2)

        def writer(index):
            c = None
            try:
                c = self.connect(self.gateways[index % len(self.gateways)])
                ready.wait(timeout=10)
                sequence = 0
                while not stop.is_set():
                    ticket = schedule.claim(index, time.monotonic())
                    if ticket["kind"] == "done":
                        return
                    if ticket["kind"] == "wait":
                        stop.wait(min(.05, ticket["seconds"]))
                        continue
                    # ASCII text preserves the same 8192 payload bytes while
                    # avoiding driver-specific bytes-to-hex SQL formatting.
                    payload = base64.b64encode(os.urandom(6144)).decode("ascii")
                    if stop.is_set():
                        schedule.cancel(ticket)
                        return
                    # Payload generation can span a boundary. A late permit
                    # is discarded, never charged/transferred into a phase.
                    key = (index + 1) * 1000000 + sequence + 1
                    with lock:
                        # Same ledger→schedule order as the frozen snapshot.
                        # Counting a dispatch and registering its actual key
                        # are one observation; no drain snapshot can see a
                        # counted call with no identity in the ledger.
                        started = time.monotonic()
                        dispatch = schedule.dispatch(ticket, started)
                        if dispatch is not None:
                            sequence += 1
                            entry = {**dispatch, "id": key, "started_phase": dispatch["phase"],
                                     "started_realtime": time.time()}
                            inflight[key] = entry.copy()
                    if dispatch is None:
                        continue
                    try:
                        q(c, "INSERT INTO p VALUES (%s,%s,%s)", (tenant, key, payload))
                        entry["result"] = "ok"
                    except pymysql.MySQLError as error:
                        entry["result"] = "error"
                        entry["error"] = list(error.args) or [None, repr(error)]
                    except BaseException as error:
                        # The call may have sent bytes before a local/client
                        # exception. Retain it as unresolved; never resend.
                        entry["result"], entry["error"] = "error", [None, repr(error)]
                    finally:
                        finished = time.monotonic()
                        entry["completed_monotonic"] = finished
                        entry["latency_ms"] = (finished - started) * 1000
                        entry["completed_phase"] = schedule.complete(dispatch, finished, entry["result"])
                        with lock:
                            inflight.pop(key)
                            live_records.append(entry)
                    if entry["result"] == "error" and entry["error"][0] != 1205:
                        # Never resend this potentially uncertain mutation.
                        stop.set()
                        return
            except BaseException as error:
                with lock:
                    live_errors.append("SQL writer: " + repr(error))
                stop.set()
            finally:
                if c is not None:
                    try:
                        c.close()
                    except BaseException as error:
                        with lock:
                            live_errors.append("SQL writer close: " + repr(error))
                        stop.set()

        sampler = threading.Thread(target=sample, daemon=True)
        writers = [threading.Thread(target=writer, args=(index,), daemon=True) for index in range(self.args.writers)]
        primary, phase_result, restore_errors, drain_errors = None, None, [], []
        self.pressure_threads = writers
        try:
            with Controls(self) as control:
                try:
                    sampler.start()
                    for thread in writers:
                        thread.start()
                    ready.wait(timeout=10)
                    self.record("real_pressure_begin", source=self.nodes[0], tenant=tenant,
                                note="paced native I/O control; no forced classifier override; call-entry times "
                                     "are observations, not physical network-arrival promises", identity=identity)
                    phase_result = run_pressure_phases(schedule, control.io_limit, stop.wait, record=self.record)
                    primary = phase_result["primary"]
                    restore_errors.extend(phase_result["restore_errors"])
                except BaseException as error:
                    primary = ("pressure stage", error, error.__traceback__)
                finally:
                    if phase_result is None:
                        try:
                            control.io_limit(0)
                        except BaseException as error:
                            restore_errors.append(repr(error))
                            if primary is None:
                                primary = ("released I/O control", error, error.__traceback__)
                    stop.set()
                    ready.abort()
                    schedule.finish(time.monotonic(), "aborted" if primary else "completed")
                    deadline = time.monotonic() + 10
                    for thread in writers:
                        if thread.ident is not None:
                            thread.join(max(0, deadline - time.monotonic()))
                    sampling_stop.set()
                    if sampler.ident is not None:
                        sampler.join(12)
        except BaseException as error:
            restore_errors.append("control lease restore: " + repr(error))
            if primary is None:
                primary = ("control lease restore", error, error.__traceback__)
        workers_live = any(thread.is_alive() for thread in writers)
        self.pressure_preserve_database = workers_live
        if workers_live or sampler.is_alive():
            drain_errors.append("pressure worker exceeded drain cap")
        # Freeze under the ledger lock. A late result after the drain cap
        # cannot rewrite this observation or justify settled equality.
        with lock:
            records, samples, errors = copy.deepcopy(live_records), copy.deepcopy(live_samples), list(live_errors)
            pending = copy.deepcopy(list(inflight.values()))
            timing = schedule.snapshot()
            frozen_monotonic = time.monotonic()
        evidence_errors = []
        for filename, rows in (("pressure-attempts.jsonl", records), ("pressure-native-samples.jsonl", samples)):
            try:
                (self.evidence / filename).write_text("".join(json.dumps(row) + "\n" for row in rows))
            except BaseException as error:
                evidence_errors.append({"stage": "raw evidence write", "file": filename,
                                        "type": type(error).__name__, "message": str(error)})
                if primary is None:
                    primary = ("raw evidence write", error, error.__traceback__)
        success = {row["id"] for row in records if row["result"] == "ok"}
        refused = {row["id"] for row in records if row["result"] == "error" and row["error"][0] == 1205}
        unexpected = [row for row in records if row["result"] == "error" and row["error"][0] != 1205]
        def metric(row, *keys):
            try:
                for key in keys:
                    row = row[key]
                return row
            except (KeyError, TypeError):
                return None
        def maximum(*keys):
            values = [metric(row, *keys) for row in samples]
            return max((value for value in values if isinstance(value, (int, float))), default=None)
        queue = maximum("native", "scheduler", "compaction_queue")
        blocked = maximum("native", "regulator", "blocked_writers")
        level0 = maximum("native", "memory", "level0")
        receipts = phase_result["receipts"] if phase_result else {}
        source_limit = next((row["result"] for row in receipts.get(PRESSURE_PHASES[1], [])
                             if row["node"] == self.nodes[0]), None)
        compaction_control = source_limit.get('compaction') if source_limit else None
        rates = [metric(row, "native", "regulator", "rate_limit") for row in samples]
        rates = [value for value in rates if isinstance(value, (int, float))]
        control_rate = metric(source_limit, "native", "regulator", "rate_limit")
        control_limit, periodic_limit, metric_error = False, False, None
        if source_limit is not None:
            try:
                control_limit, periodic_limit = native_limit_witness(source_limit, samples)
            except (KeyError, TypeError) as error:
                metric_error = repr(error)
        outcomes = {}
        for result in ("ok", "error"):
            values = [row["latency_ms"] for row in records if row["result"] == result]
            outcomes[result] = {"count": len(values), **quantiles(values)}
        summary = {"attempts": timing["attempts"], "completed": len(records),
                   "acknowledged": len(success), "refused_1205": len(refused),
                   "stored_ids_match_acknowledgements": None, "refused_ids_absent": None,
                   "by_outcome": outcomes, "worker_errors": errors,
                   "compaction_queue_max_bytes": queue, "blocked_writers_max": blocked,
                   "compaction_control": compaction_control,
                   "memory_level0_max_bytes": level0, "unexpected_errors": unexpected,
                   "rate_limit_witness": {"control_node": self.nodes[0], "control_realtime": metric(source_limit, "realtime"),
                                          "configured_mib_per_second": metric(source_limit, "limit"),
                                          "control_native_rate_limit": control_rate,
                                          "control_observed_limit": control_limit,
                                          "periodic_min": min(rates, default=None),
                                          "periodic_max": max(rates, default=None),
                                          "periodic_observed_limit": periodic_limit},
                   "schedule": timing, "control_timings": phase_result["controls"] if phase_result else [],
                   "workers_live": workers_live, "sampler_live": sampler.is_alive(),
                   "inflight": pending, "diagnostics_frozen_monotonic": frozen_monotonic,
                   "restore_errors": restore_errors, "drain_errors": drain_errors, "metric_error": metric_error,
                   "evidence_errors": evidence_errors,
                   "primary_failure": {"stage": primary[0], "type": type(primary[1]).__name__,
                                       "message": str(primary[1])} if primary else None,
                   "error_counts": dict(collections.Counter(str(row.get("error")) for row in records if row["result"] == "error"))}
        summary_path = self.evidence / "pressure-summary.json"
        def persist(value):
            summary_path.write_text(json.dumps(value, indent=2) + "\n")
        def check(value):
            if primary:
                raise primary[1].with_traceback(primary[2])
            value["check_stage"] = "restore"
            assert not restore_errors, restore_errors
            value["check_stage"] = "drain"
            assert not drain_errors, drain_errors
            value["check_stage"] = "workers"
            assert not errors, errors
            assert len(records) == timing["attempts"] and records and samples, (len(records), timing["attempts"], len(samples))
            value["check_stage"] = "phase coverage"
            assert timing["attempts"] <= PRESSURE_ATTEMPT_CAP, "hard attempt cap exceeded"
            assert timing["finished"] and timing["finish_reason"] == "completed"
            for window in timing["phases"]:
                assert (window["close_reason"] == "completed" and window["opened_monotonic"] is not None
                        and window["closed_monotonic"] >= window["deadline_monotonic"]), window
                assert all(worker["dispatched"] > 0 for worker in window["workers"]), window
                assert sum(worker["reserved"] + worker["skipped"] for worker in window["workers"]) <= window["slot_budget"], window
            value["check_stage"] = "outcomes"
            assert success and refused and not unexpected, (len(success), len(refused), unexpected)
            value["check_stage"] = "readback"
            assert value["readback"]["status"] == "complete", value["readback"]
            assert (value["stored_ids_match_acknowledgements"] and value["refused_ids_absent"]), \
                "acknowledged/refused key identities differ from stored rows"
            seed = value['readback']['compaction_fixture']
            assert seed['status'] == 'complete' and seed['matches'], seed
            value["check_stage"] = "native pressure witness"
            assert metric_error is None, metric_error
            assert all(row["engine"] == "vinyl" and row["vinyl_memory"] == identity["vinyl_memory"]
                       and row["vinyl_cache"] == 0 and row["vinyl_timeout"] == identity["vinyl_timeout"] for row in samples)
            assert any(row["snap_io_rate_limit"] == .5 for row in samples), "native I/O control was not sampled"
            assert source_limit is not None and source_limit["limit"] == .5, "no confirmed limited-I/O receipt"
            # A completed dump may recompute the native regulator before
            # the periodic sampler; either genuine observation can witness it.
            assert control_limit or periodic_limit, "real regulator limit was not observed"
            assert queue is not None and queue > 0, "no actual queued compaction input; pressure fixture did not exercise compaction"
            compaction_witness(compaction_control, samples, seed)
            assert (blocked is not None and blocked > 0 or level0 is not None and level0 >= .75 * identity["vinyl_memory"]
                    or queue >= identity["vinyl_memory"]), "no actual native pressure witness"
            assert any(index["run_count"] > index["range_count"] > 0 for row in samples for index in row["indices"]), "no real run amplification"
            value["check_stage"] = "latency"
            for result in ("ok", "error"):
                assert outcomes[result]["max_ms"] < self.args.max_latency_ms, (result, outcomes[result])
            value["check_stage"] = "complete"
        def audit():
            result = audit_pressure_keys(self.connect, tenant, records, pending, workers_live)
            result['compaction_fixture'] = self.audit_compaction_fixture(tenant)
            return result
        finalize_pressure(summary,
                          audit=audit,
                          persist=persist, check=check)
        self.record("real_pressure_completed", summary=summary)
        print("  pressure " + json.dumps({
            "attempts": summary["attempts"], "acknowledged": len(success), "refused_1205": len(refused),
            "success_latency_ms": outcomes["ok"], "refusal_latency_ms": outcomes["error"],
            "compaction_queue_bytes": queue, "blocked_writers": blocked,
            "acknowledged_and_refused_ids_verified": summary["stored_ids_match_acknowledgements"] and summary["refused_ids_absent"],
        }, sort_keys=True), flush=True)

    def cleanup(self):
        errors = []
        if (getattr(self, "pressure_preserve_database", False)
                or any(thread.is_alive() for thread in getattr(self, "pressure_threads", []))):
            # Never DROP underneath a client whose outcome is still live.
            self.record("cleanup", errors=[], preserved_database=self.db, reason="pressure writers still live")
            self.events.close()
            return
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
        failed = False
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
        except BaseException:
            failed = True
            raise
        finally:
            try:
                self.cleanup()
            except BaseException:
                if not failed:
                    raise
                # cleanup() already retained its own error. Keep the
                # original stage/witness exception as the run's failure.
                traceback.print_exc()


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
