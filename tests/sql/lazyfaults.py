#!/usr/bin/env python3
"""Bounded own-commit/APPLY faults for lazy-index session barriers (#82).

Use a dedicated, idle two-shard cluster with TARN_TEST_HOOKS=1 and normal
ack_on_prepare=false. This program starts/stops no services. It creates a
unique database and leases only its own function wrappers; every wrapper
also restores itself after 60 seconds. Native probes use lazytest's bounded
Tarantool helper. SQL needs PyMySQL and TARANTOOL must name that helper's
Tarantool executable.

  python3 tests/sql/lazyfaults.py --tarn 127.0.0.1:4000 \
      --shards 127.0.0.1:3311,127.0.0.1:3321 --engine memtx

--shards lists writable leaders in gateway participant order. Run again
with --engine vinyl. Evidence is private under a new --evidence directory
(by default /tmp/tarn-lazyfaults-<unique id>); no unrelated transaction is
resolved, no process is killed, and no persistent service setting is changed.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time
import traceback
import uuid

import pymysql

from lazytest import ev, q, rpc, stat
from movetest import bucket_of, crc32c


def decision_node(nodes, identities, state):
    """Verify placement and locate the one recorded decision writer."""
    ids = sorted(x["id"] for x in identities)
    assert state["txid"] and sorted(state["parts"]) == ids, state
    expected = ids[crc32c(state["txid"].encode()) % len(ids)]
    assert state["dshard"] == expected, (state, expected)
    return next(node for node, identity in zip(nodes, identities) if identity["id"] == expected)


INSTALL = r"""
local owner, name, enabled, lose_reply, lease_seconds = ...
assert(tarn.cfg.test_hooks == true, 'lazyfaults requires test_hooks')
assert(rawget(_G, '__tarn_lazyfaults') == nil, 'another lazyfaults lease exists')
assert(not box.info.ro, 'lazyfaults requires a writable leader')
local sp = assert(box.space[name], 'lazyfaults table is absent')
local fiber, clock = require('fiber'), require('clock')
local s = {owner = owner, sid = sp.id, enabled = enabled,
           mode = 'error', hold_prepare = lose_reply, lose_reply = lose_reply,
           apply_calls = 0, silent_calls = 0, status_calls = 0,
           recovery_after = tarn.cfg.recovery_after,
           expires = clock.monotonic() + lease_seconds,
           original = {}, wrapped = {}, txid = false, prepared = false}
for _, key in ipairs({'tarn_apply', 'tarn_prepare', 'tarn_part_status'}) do
    s.original[key] = assert(rawget(_G, key), 'missing native protocol function')
end
local function selected(txid, writes)
    if not s.enabled then return false end
    if s.txid then return s.txid == txid end
    local match = false
    for _, w in ipairs(writes or {}) do
        if w[2] == s.sid then match = true; break end
    end
    if not writes then
        for _, l in box.space._tarn_locks.index.txid:pairs({txid}, {iterator = 'EQ'}) do
            if l.space_id == s.sid and l.mode == 'W' then match = true; break end
        end
    end
    if match then s.txid = txid end
    return match
end
function s.release()
    s.mode, s.hold_prepare = 'pass', false
end
function s.restore_sync()
    if s.sync == nil then return true end
    box.cfg{replication_synchro_quorum = 1}
    local deadline = clock.monotonic() + 5
    while box.info.synchro.queue.len > 0 and clock.monotonic() < deadline do fiber.sleep(.01) end
    assert(box.info.synchro.queue.len == 0, 'owned synchronous APPLY did not drain')
    local saved = s.sync
    assert(box.space[s.sid]):alter{is_sync = saved.is_sync}
    if saved.owner == 0 then box.ctl.demote() end
    box.cfg{replication_synchro_quorum = saved.quorum, replication_synchro_timeout = saved.timeout}
    s.sync = nil
    return true
end
function s.restore()
    if rawget(_G, '__tarn_lazyfaults') ~= s then return {'ownership changed'} end
    s.release()
    local errors = {}
    local sync_ok, sync_err = pcall(s.restore_sync)
    if not sync_ok then errors[#errors + 1] = 'sync restore: ' .. tostring(sync_err) end
    for key, original in pairs(s.original) do
        if rawget(_G, key) == s.wrapped[key] then rawset(_G, key, original)
        else errors[#errors + 1] = key .. ' changed outside this lease' end
    end
    if tarn.cfg.recovery_after == 3600 then tarn.cfg.recovery_after = s.recovery_after
    else errors[#errors + 1] = 'recovery_after changed outside this lease' end
    rawset(_G, '__tarn_lazyfaults', nil)
    return errors
end
s.wrapped.tarn_apply = function(txid, ...)
    if selected(txid) then
        s.apply_calls = s.apply_calls + 1
        if s.mode == 'error' then return {'error', 1213, 'lazyfaults injected APPLY refusal'} end
        if s.mode == 'silent' then
            s.silent_calls = s.silent_calls + 1
            while s.mode == 'silent' and clock.monotonic() < s.expires do fiber.sleep(0.01) end
        end
    end
    return s.original.tarn_apply(txid, ...)
end
s.wrapped.tarn_prepare = function(txid, dshard, parts, writes, ...)
    local mine = selected(txid, writes)
    if mine then s.dshard, s.parts = dshard, parts end
    local r = s.original.tarn_prepare(txid, dshard, parts, writes, ...)
    if mine and r[1] == 'yes' then
        s.prepared = true
        if s.lose_reply then
            while s.hold_prepare and clock.monotonic() < s.expires do fiber.sleep(0.01) end
            -- The durable YES already exists. A CALL error makes the
            -- coordinator's vote Unknown, as a lost PREPARE reply does.
            error('lazyfaults injected lost PREPARE reply after durable YES')
        end
    end
    return r
end
s.wrapped.tarn_part_status = function(txid)
    if s.enabled and s.lose_reply and s.txid == txid then
        s.status_calls = s.status_calls + 1
        error('lazyfaults participant status must not hide COMMITTED')
    end
    return s.original.tarn_part_status(txid)
end
tarn.cfg.recovery_after = 3600
rawset(_G, '__tarn_lazyfaults', s)
for key, fn in pairs(s.wrapped) do rawset(_G, key, fn) end
fiber.create(function()
    fiber.sleep(lease_seconds)
    if rawget(_G, '__tarn_lazyfaults') == s then s.restore() end
end)
return {owner = owner, id = tarn.cfg.shard_id, uuid = tostring(box.info.uuid),
        version = box.info.version, sid = sp.id, engine = sp.engine}
"""

SNAPSHOT = r"""
local owner, name, key, bucket = ...
local s = assert(rawget(_G, '__tarn_lazyfaults'), 'lazyfaults lease expired')
assert(s.owner == owner, 'wrong lazyfaults owner')
local p = s.txid and box.space._tarn_part:get{s.txid} or nil
local row = box.space[name]:get{key}
local counter = box.space._tarn_lazy_counter:get{bucket}
local pending = {}
for _,r in box.space._tarn_lazy_outbox.index.pending:pairs({bucket}, {iterator='EQ'}) do
    pending[#pending+1] = {seq=r.seq,gen=r.gen}
end
return {txid = s.txid, prepared = s.prepared, dshard = s.dshard or 0,
        parts = s.parts or {}, apply_calls = s.apply_calls,
        silent_calls = s.silent_calls, status_calls = s.status_calls,
        state = p and p.state or 'none', row = row and row:totable() or box.NULL,
        source_bucket=bucket, counter=counter and counter.seq or 0, outbox=pending}
"""

OVERLAP_STATE = r"""
local owner, key, tag = ...
local s = assert(rawget(_G, '__tarn_lazyfaults')); assert(s.owner == owner)
local sp = assert(box.space[s.sid])
local function view(isolation)
    box.begin{txn_isolation = isolation}
    local yes, result = pcall(function()
        local part = box.space._tarn_part:get{s.txid}
        local row = sp:get{key}
        return {state = part and part.state or 'none', row = row and row:totable() or box.NULL}
    end)
    if box.is_in_txn() then box.rollback() end
    if not yes then error(result) end
    return result
end
local dirty, confirmed = view('read-committed'), view('read-confirmed')
local queue = box.info.synchro.queue
local row = dirty.row
return {queue = queue, dirty = dirty, confirmed = confirmed,
    pending = sp.is_sync and queue.owner == box.info.id and queue.len >= 1
        and dirty.state == 'applied' and row ~= box.NULL
        and row[1] == key and row[2] == tag and row[3] == 42
        and confirmed.state == 'prepared' and confirmed.row == box.NULL}
"""


def connect(endpoint, db=None):
    host, port = endpoint.rsplit(":", 1)
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db,
                           autocommit=True, charset="utf8mb4", connect_timeout=3,
                           read_timeout=15, write_timeout=5)


def wait_for(fn, seconds, what):
    deadline = time.monotonic() + seconds
    while True:
        value = fn()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise AssertionError(f"timeout waiting for {what}")
        time.sleep(0.02)


class Worker:
    def __init__(self, fn):
        self.result = None
        self.error = None
        self.done = threading.Event()

        def run():
            try:
                self.result = fn()
            except Exception as exc:  # reported by finish, never treated as success
                self.error = exc
            finally:
                self.done.set()

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def finish(self, seconds=12):
        self.thread.join(seconds)
        if self.thread.is_alive():
            raise AssertionError("owned SQL worker did not finish after release")
        if self.error:
            raise self.error
        return self.result


class Lease:
    """Function identity + token ownership; cleanup touches only this lease."""

    def __init__(self, test, key, lose_reply=False):
        self.test, self.key = test, key
        self.owner = uuid.uuid4().hex
        self.installed = []
        self.lose_reply = lose_reply
        self.txid = None
        self.known_committed = False

    def __enter__(self):
        try:
            for i, node in enumerate(self.test.nodes):
                # An RPC can install the lease and lose its response.
                # Cleanup must inspect that node even without an ACK.
                self.installed.append(i)
                identity = ev(node, INSTALL, [self.owner, self.test.space, i == 1,
                                             self.lose_reply, 60])
                self.test.record("lease_install", owner=self.owner, node=node, identity=identity)
                assert identity["id"] == self.test.identities[i]["id"]
                assert identity["uuid"] == self.test.identities[i]["uuid"]
            return self
        except Exception:
            self.close()
            raise

    def snapshot(self):
        value = ev(self.test.nodes[1], SNAPSHOT, [self.owner, self.test.space, self.key,
                                                     bucket_of(self.key, self.test.bucket_count)])
        if value["txid"]:
            self.txid = value["txid"]
        return value

    def mode(self, value):
        return ev(self.test.nodes[1], """
local owner, mode = ...
local s = assert(rawget(_G, '__tarn_lazyfaults'))
assert(s.owner == owner)
s.mode = mode
return true
""", [self.owner, value])

    def release_prepare(self):
        return ev(self.test.nodes[1], """
local s = assert(rawget(_G, '__tarn_lazyfaults'))
assert(s.owner == ...)
s.hold_prepare = false
return true
""", [self.owner])

    def release(self):
        for i in self.installed:
            ev(self.test.nodes[i], """
local s = rawget(_G, '__tarn_lazyfaults')
if s then assert(s.owner == ...); s.release() end
return true
""", [self.owner])

    def close(self):
        errors = []
        for i in self.installed:
            try:
                result = ev(self.test.nodes[i], """
local owner = ...
local s = rawget(_G, '__tarn_lazyfaults')
if not s then return {inactive = true, expired = true} end
assert(s.owner == owner, 'cleanup ownership mismatch')
local txid = s.txid
local errors = s.restore()
return {inactive = rawget(_G, '__tarn_lazyfaults') == nil, errors = errors, txid = txid}
""", [self.owner])
                self.test.record("lease_restore", node=self.test.nodes[i], result=result)
                if result.get("txid"):
                    self.txid = result["txid"]
                assert result["inactive"] and not result.get("expired") and not result.get("errors"), result
            except Exception as exc:
                errors.append(f"{self.test.nodes[i]}: {exc}")
        self.installed.clear()
        # Only this captured transaction, and only after its commit is known.
        if self.txid and self.known_committed:
            for node in self.test.nodes:
                try:
                    # Another fiber of the node, such as its recovery, may
                    # be applying the same transaction: a conflict rolled
                    # this APPLY back, and the next one finds it applied.
                    for _ in range(50):
                        result = rpc(node, "tarn_apply", [self.txid])
                        if not result or result[0] != "conflict":
                            break
                        time.sleep(0.02)
                    assert list(result[:2]) in (["ok", "applied"], ["ok", "none"]), result
                except Exception as exc:
                    errors.append(f"owned APPLY cleanup {node}: {exc}")
        if errors:
            raise AssertionError("; ".join(errors))

    def __exit__(self, exc_type, exc, tb):
        self.close()


class Test:
    def __init__(self, args):
        self.args = args
        self.nodes = args.shards.split(",")
        assert len(self.nodes) == 2, "exactly two dedicated shard leaders required"
        self.db = "lazyfaults_" + uuid.uuid4().hex[:12]
        self.space = self.db + ".t"
        self.completed = 0
        self.created = False
        self.evidence = Path(args.evidence or ("/tmp/tarn-lazyfaults-" + uuid.uuid4().hex))
        self.evidence.mkdir(mode=0o700, parents=False, exist_ok=False)
        self.events = (self.evidence / "events.jsonl").open("x", encoding="utf-8")
        os.chmod(self.evidence / "events.jsonl", 0o600)
        self.identities = []
        manifest = {"database": self.db, "arguments": vars(self.args),
                    "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
        with (self.evidence / "manifest.json").open("x", encoding="utf-8") as f:
            os.chmod(f.name, 0o600)
            json.dump(manifest, f, indent=2, sort_keys=True)
            f.write("\n")

    def record(self, event, **values):
        value = {"event": event, "time": time.time(), **values}
        self.events.write(json.dumps(value, default=str, sort_keys=True) + "\n")
        self.events.flush()

    def setup(self):
        for node in self.nodes:
            self.identities.append(ev(node, """
assert(tarn.cfg.test_hooks == true and not box.info.ro, 'dedicated writable test_hooks shard required')
assert(rawget(_G, '__tarn_lazyfaults') == nil, 'another fault lease exists')
local stats = tarn_lazy_stats()
assert(stats[1] == 'ok', 'missing lazy storage')
return {id = tarn.cfg.shard_id, uuid = tostring(box.info.uuid), version = box.info.version}
"""))
        assert len({x["id"] for x in self.identities}) == 2
        self.record("preflight", identities=self.identities)
        c = connect(self.args.tarn)
        try:
            q(c, f"CREATE DATABASE {self.db}")
            self.created = True
            q(c, f"USE {self.db}")
            q(c, f"CREATE TABLE s (id INT PRIMARY KEY, v INT) ENGINE={self.args.engine}")
            q(c, f"CREATE TABLE t (id INT PRIMARY KEY, k INT, v INT, "
                 f"KEY by_k (k) COMMENT 'tarn:lazy') ENGINE={self.args.engine}")
            ranges = q(c, "SHOW TARN BUCKETS")
            count = sum(int(r[1]) for r in ranges)
            self.bucket_count = count
            owned = {}
            for row in ranges:
                ids = owned.setdefault(int(row[0]), set())
                for part in str(row[2]).split(", ") if row[2] else []:
                    lo, _, hi = part.partition("-")
                    ids.update(range(int(lo), int(hi or lo) + 1))
            self.keys = []
            for identity in self.identities:
                matches = [i for i in range(1, 10000) if bucket_of(i, count) in owned[identity["id"]]]
                assert len(matches) >= 4, "not enough routed fixture keys"
                self.keys.append(matches[:4])
            q(c, "INSERT INTO s VALUES (%s, 0)", (self.keys[0][0],))
            before, falls = stat(c, "Tarn_lazy_reads"), stat(c, "Tarn_lazy_fallbacks")
            assert q(c, "SELECT id, k, v FROM t FORCE INDEX(by_k) WHERE k = -999 ORDER BY id") == ()
            assert stat(c, "Tarn_lazy_reads") == before + 1, "fixture is not an actual lazy read"
            assert stat(c, "Tarn_lazy_fallbacks") == falls
            for node in self.nodes:
                info = ev(node, "local s = assert(box.space[...]); return {id=s.id,engine=s.engine}", [self.space])
                assert info["engine"] == self.args.engine, info
                stats = rpc(node, "tarn_lazy_stats", [])
                assert stats[0] == "ok" and stats[1]["depth"] == 0
                assert stats[1]["inflight"] == stats[1]["uncertain"] == 0 and not stats[1]["paused"], stats
            self.record("setup", engine=self.args.engine, shard_ids=[x["id"] for x in self.identities])
        finally:
            c.close()

    def begin(self, c, key, tag):
        q(c, "SET SESSION tarn_lazy_read_your_writes=OFF")
        q(c, "BEGIN")
        q(c, "UPDATE s SET v=v+1 WHERE id=%s", (self.keys[0][0],))
        q(c, "INSERT INTO t VALUES (%s,%s,42)", (key, tag))

    def pending_witness(self, c, lease):
        state = lease.snapshot()
        self.record("pending_witness", state=state)
        assert state["state"] == "prepared" and state["row"] is None, state
        decision_node(self.nodes, self.identities, state)
        assert state["apply_calls"] == 4, "normal ACK must follow exactly four returned-error APPLY rounds"
        assert stat(c, "Tarn_lazy_session_pending_apply") > 0
        assert stat(c, "Tarn_lazy_session_touched_buckets") > 0
        assert stat(c, "Tarn_lazy_session_strong_fallback") == 0
        stats = rpc(self.nodes[1], "tarn_lazy_stats", [])
        assert stats[0] == "ok" and stats[1]["depth"] == 0, stats

    def catch_up(self, c, lease, key, tag):
        q(c, "SET SESSION tarn_lazy_read_your_writes=ON")
        q(c, "SET SESSION tarn_lazy_wait_timeout=5")
        before, falls = stat(c, "Tarn_lazy_reads"), stat(c, "Tarn_lazy_fallbacks")
        worker = Worker(lambda: q(c, "SELECT id,k,v FROM t FORCE INDEX(by_k) WHERE k=%s ORDER BY id", (tag,)))
        try:
            def blocked():
                s = lease.snapshot()
                return s if s["apply_calls"] > 4 or worker.done.is_set() else None
            state = wait_for(blocked, 2, "lazy read's pending APPLY attempt")
            assert state["apply_calls"] > 4 and not worker.done.is_set(), "pending commit was forgotten"
            lease.mode("pass")
            assert worker.finish() == ((key, tag, 42),)
            assert stat(c, "Tarn_lazy_reads") == before + 1, "strong fallback masked the witness"
            assert stat(c, "Tarn_lazy_fallbacks") == falls
            assert stat(c, "Tarn_lazy_session_pending_apply") == 0
        finally:
            lease.release()
            worker.finish()

    def case(self, index, recovered=False, silent=False, overlap=False):
        key, tag = self.keys[1][index], 1000 + index
        c = connect(self.args.tarn, self.db)
        commit = None
        try:
            self.begin(c, key, tag)
            with Lease(self, key, lose_reply=recovered) as lease:
                try:
                    if recovered:
                        commit = Worker(lambda: q(c, "COMMIT"))
                        state = wait_for(lambda: (s if (s := lease.snapshot())["prepared"] else None),
                                         5, "durable target YES")
                        decision_node_ = decision_node(self.nodes, self.identities, state)
                        txid = state["txid"]
                        for node in self.nodes:
                            wait_for(lambda n=node: ev(n, "local p=box.space._tarn_part:get{...}; "
                                                      "return p and p.state or 'none'", [txid]) == "prepared",
                                     5, f"durable YES on {node}")
                        decision = rpc(decision_node_, "tarn_txn_cas", [txid, "staging", "committed"])
                        # Verify durable state, not merely a successful CALL.
                        assert ev(decision_node_, "local r=box.space._tarn_txn:get{...}; return r and r.status", [txid]) == "committed", decision
                        lease.known_committed = True
                        lease.release_prepare()
                        commit.finish()
                    else:
                        start = time.monotonic()
                        q(c, "COMMIT")
                        lease.known_committed = True
                        elapsed = time.monotonic() - start
                        assert 0.6 <= elapsed < 8, f"normal bounded APPLY exhaustion took {elapsed:.3f}s"
                    self.pending_witness(c, lease)
                    if recovered:
                        assert lease.snapshot()["status_calls"] == 0, "COMMITTED recovery queried unavailable status"
                    if overlap:
                        self.canceled_apply_overlap(c, lease, key, tag)
                    elif silent:
                        self.silent_deadline(c, lease, key, tag)
                    else:
                        self.catch_up(c, lease, key, tag)
                    self.completed += 1
                finally:
                    lease.release()
                    if commit:
                        commit.finish()
        finally:
            c.close()

    def silent_deadline(self, c, lease, key, tag):
        q(c, "SET SESSION tarn_lazy_read_your_writes=ON")
        q(c, "SET SESSION tarn_lazy_wait_timeout=1")
        before, falls = stat(c, "Tarn_lazy_reads"), stat(c, "Tarn_lazy_fallbacks")
        lease.mode("silent")
        started = time.monotonic()
        worker = Worker(lambda: q(c, "SELECT id,k,v FROM t FORCE INDEX(by_k) WHERE k=%s ORDER BY id", (tag,)))
        try:
            wait_for(lambda: lease.snapshot()["silent_calls"] > 0, 2, "silent APPLY entering")
            try:
                worker.finish(4)
            except pymysql.MySQLError as exc:
                assert exc.args[0] == 1205, f"unexpected catch-up error: {exc}"
            else:
                raise AssertionError("silent APPLY was bypassed with a stale result")
            elapsed = time.monotonic() - started
            assert elapsed < 4, f"catch-up ignored its absolute deadline: {elapsed:.3f}s"
            reads = stat(c, "Tarn_lazy_reads")
            pending = stat(c, "Tarn_lazy_session_pending_apply")
            strong = stat(c, "Tarn_lazy_session_strong_fallback")
            self.record("silent_timed_out", elapsed=elapsed, lazy_reads=reads, before=before,
                        pending_apply=pending, strong_fallback=strong)
            assert reads == before, f"timed-out read completed lazily: before={before}, after={reads}"
            assert pending > 0, f"cancellation erased pending obligations: pending={pending}"
            assert strong == 0, f"bounded catch-up timeout made history sticky: strong={strong}"
            lease.mode("pass")
            q(c, "SET SESSION tarn_lazy_wait_timeout=5")
            rows = q(c, "SELECT id,k,v FROM t FORCE INDEX(by_k) WHERE k=%s ORDER BY id", (tag,))
            retry_state = lease.snapshot()
            reads, fallbacks = stat(c, "Tarn_lazy_reads"), stat(c, "Tarn_lazy_fallbacks")
            pending = stat(c, "Tarn_lazy_session_pending_apply")
            self.record("silent_retry", rows=rows, expected=((key, tag, 42),), lazy_reads=reads,
                        before=before, fallbacks=fallbacks, before_fallbacks=falls, pending_apply=pending,
                        participant=retry_state)
            assert rows == ((key, tag, 42),), f"retry lost committed row: expected={((key, tag, 42),)}, actual={rows}"
            assert reads == before + 1, f"retry did not complete lazily: before={before}, after={reads}"
            assert fallbacks == falls, f"retry used strong fallback: before={falls}, after={fallbacks}"
            assert pending == 0, f"successful retry retained pending APPLY: pending={pending}"
        finally:
            lease.release()
            if not worker.done.is_set():
                worker.finish()

    def canceled_apply_overlap(self, c, lease, key, tag):
        """Hold a canceled APPLY in a real synchronous commit, not a sleep."""
        q(c, "SET SESSION tarn_lazy_read_your_writes=ON")
        q(c, "SET SESSION tarn_lazy_wait_timeout=1")
        before, falls = stat(c, "Tarn_lazy_reads"), stat(c, "Tarn_lazy_fallbacks")
        lease.mode("silent")
        workers = []

        def read():
            w = Worker(lambda: q(c, "SELECT id,k,v FROM t FORCE INDEX(by_k) WHERE k=%s ORDER BY id", (tag,)))
            workers.append(w)
            return w

        def timeout(w, label):
            started = time.monotonic()
            try:
                rows = w.finish(4)
            except pymysql.MySQLError as exc:
                assert exc.args[0] == 1205, f"{label}: unexpected error {exc}"
            else:
                state = lease.snapshot()
                self.record("overlap_early_result", rows=rows, participant=state,
                            pending_apply=stat(c, "Tarn_lazy_session_pending_apply"))
                raise AssertionError(f"{label}: returned before native quorum confirmation: rows={rows}, state={state}")
            assert time.monotonic() - started < 4, f"{label}: catch-up ignored its deadline"
            assert stat(c, "Tarn_lazy_session_pending_apply") > 0, f"{label}: pending APPLY was forgotten"
            assert stat(c, "Tarn_lazy_session_strong_fallback") == 0, f"{label}: history became sticky"
            assert stat(c, "Tarn_lazy_reads") == before, f"{label}: incomplete read counted as lazy success"
            assert stat(c, "Tarn_lazy_fallbacks") == falls, f"{label}: strong fallback masked the witness"

        try:
            first = read()
            wait_for(lambda: lease.snapshot()["silent_calls"] > 0, 2, "first canceled APPLY entering")
            timeout(first, "first silent read")
            saved = ev(self.nodes[1], """
local s = assert(rawget(_G, '__tarn_lazyfaults')); assert(s.owner == ...)
assert(s.enabled and s.mode == 'silent' and s.sync == nil)
local queue = box.info.synchro.queue
assert(queue.len == 0 and (queue.owner == 0 or queue.owner == box.info.id), 'fixture does not own sync queue')
local sp = assert(box.space[s.sid])
s.sync = {is_sync=sp.is_sync,quorum=box.cfg.replication_synchro_quorum,
          timeout=box.cfg.replication_synchro_timeout,owner=queue.owner}
box.cfg{replication_synchro_quorum=1,replication_synchro_timeout=30}
if queue.owner ~= box.info.id then box.ctl.promote() end
sp:alter{is_sync=true}
tarn.lazy.invalidate(); tarn.lazy.fresh()
return s.sync
""", [lease.owner])
            self.record("overlap_sync_saved", settings=saved)
            wait_for(lambda: ev(self.nodes[1], """
local s=assert(rawget(_G,'__tarn_lazyfaults')); assert(s.owner==...)
return tarn.lazy.quota.ready() and not tarn.lazy.stale and tarn.lazy.sources[s.sid] ~= nil
""", [lease.owner]), 5, "capture and quota rebuild before unavailable quorum")
            ev(self.nodes[1], """
local s=assert(rawget(_G,'__tarn_lazyfaults')); assert(s.owner==... and s.sync~=nil)
assert(box.info.synchro.queue.len==0); box.cfg{replication_synchro_quorum=2}; return true
""", [lease.owner])
            lease.mode("pass")
            last = None

            def pending():
                nonlocal last
                last = ev(self.nodes[1], OVERLAP_STATE, [lease.owner, key, tag])
                return last if last["pending"] else None

            try:
                held = wait_for(pending, 5, "canceled APPLY's real native quorum wait")
            except AssertionError:
                self.record("overlap_quorum_missing", last=last)
                raise
            # Background asynchronous writes can join the queue behind this
            # synchronous APPLY. Bind the witness to its unconfirmed row and
            # participant instead of assuming it is the queue's sole entry.
            self.record("overlap_quorum_pending", queue=held["queue"], owned=held)
            timeout(read(), "overlapping RYW read")
            held = ev(self.nodes[1], OVERLAP_STATE, [lease.owner, key, tag])
            assert held["pending"], f"owned native APPLY was not still held: {held}"
            self.record("overlap_timeout_retained", queue=held["queue"], owned=held, participant=lease.snapshot())
            ev(self.nodes[1], """
local s=assert(rawget(_G,'__tarn_lazyfaults')); assert(s.owner==... and s.sync~=nil)
box.cfg{replication_synchro_quorum=1}; return true
""", [lease.owner])
            wait_for(lambda: ev(self.nodes[1], "return box.info.synchro.queue.len") == 0,
                     5, "owned APPLY quorum completion")
            q(c, "SET SESSION tarn_lazy_wait_timeout=5")
            rows = q(c, "SELECT id,k,v FROM t FORCE INDEX(by_k) WHERE k=%s ORDER BY id", (tag,))
            assert rows == ((key, tag, 42),), f"confirmed retry lost own row: {rows}"
            assert stat(c, "Tarn_lazy_reads") == before + 1, "confirmed retry did not use lazy path"
            assert stat(c, "Tarn_lazy_fallbacks") == falls, "confirmed retry fell back strongly"
            assert stat(c, "Tarn_lazy_session_pending_apply") == 0, "confirmed retry kept pending APPLY"
            self.record("overlap_confirmed", rows=rows, participant=lease.snapshot())
        finally:
            # Restore the exact owned configuration before Lease invokes
            # ordinary APPLY cleanup; never leave that cleanup behind q=2.
            ev(self.nodes[1], """
local s=assert(rawget(_G,'__tarn_lazyfaults')); assert(s.owner==...)
s.release(); return s.restore_sync()
""", [lease.owner])
            for w in workers:
                if not w.done.is_set():
                    try:
                        w.finish()
                    except pymysql.MySQLError as exc:
                        assert exc.args[0] == 1205, exc

    def test_canceled_apply_overlap(self):
        self.case(3, overlap=True)

    def test_apply_exhaustion(self):
        self.case(0)

    def test_committed_recovery(self):
        self.case(1, recovered=True)

    def test_silent_apply_deadline(self):
        self.case(2, silent=True)

    def close(self, failed):
        # A failed assertion may leave a diagnostic DB; never run a generic
        # transaction resolver or delete another test's state to hide it.
        for node in self.nodes:
            active = ev(node, "return rawget(_G, '__tarn_lazyfaults') ~= nil")
            assert not active, f"owned injection lease still active on {node}"
        if self.created and not failed:
            c = connect(self.args.tarn)
            try:
                q(c, f"DROP DATABASE {self.db}")
            finally:
                c.close()
        self.record("complete", failed=failed, completed=self.completed, retained_database=bool(failed))
        self.events.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tarn", required=True)
    ap.add_argument("--shards", required=True)
    ap.add_argument("--engine", choices=("memtx", "vinyl"), default="memtx")
    ap.add_argument("--evidence", help="new private directory; must not already exist")
    ap.add_argument("--only", default="")
    args = ap.parse_args()
    t = Test(args)
    failed = 0
    try:
        t.setup()
        names = [n for n in dir(t) if n.startswith("test_") and args.only in n]
        assert names, "no fault method selected"
        for name in names:
            print(name, flush=True)
            try:
                getattr(t, name)()
                t.record("passed", method=name)
                print("  ok  " + name, flush=True)
            except Exception as exc:
                failed += 1
                t.record("failed", method=name, error=f"{type(exc).__name__}: {exc}", traceback=traceback.format_exc())
                traceback.print_exc()
                print(f"  FAIL {name}: {type(exc).__name__}: {exc}", flush=True)
                # Do not continue mutating a cluster after a fault fails.
                break
    except Exception as exc:
        failed += 1
        t.record("setup_failed", error=f"{type(exc).__name__}: {exc}", traceback=traceback.format_exc())
        traceback.print_exc()
        print(f"  FAIL setup: {type(exc).__name__}: {exc}", flush=True)
    finally:
        try:
            t.close(failed)
        except Exception as exc:
            failed += 1
            traceback.print_exc()
            print(f"  FAIL cleanup: {type(exc).__name__}: {exc}", flush=True)
    print(f"{t.completed} completed, {failed} failures; evidence {t.evidence}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
