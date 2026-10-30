#!/usr/bin/env python3
"""Disposable RF3 terminal clock-fault SQL/KV histories for issue #181.

Starts no services and uses no signals. One existing test_time injection
latches a real module-local drift fault; this driver never resets it. The
owned database, readers and policy are intentionally preserved for the
lifecycle controller's process snapshot and offline WAL capture.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time
import traceback

import pymysql

from kvtest import Test, ev
from movetest import bucket_of

FAULT_REASON = "wall/monotonic drift exceeded"
CLOCK_MESSAGE = "Clock safety unavailable: " + FAULT_REASON
ENGINES = ("memtx", "vinyl")

SNAPSHOT = """
local names,tenant,user=...; assert(rawget(_G,'tarn'))
assert(not box.is_in_txn(),'snapshot requires its own transaction')
box.begin{txn_isolation='read-confirmed'}
local ok,result=pcall(function()
 local meta=assert(box.space._tarn_meta); local row=meta:get{'clock:policy:1'}
 local corpus={}
 for _,name in ipairs(names) do
  local sp=assert(box.space[name]); local rows=sp:select{tenant}
  corpus[#corpus+1]={name=name,id=sp.id,engine=sp.engine,sync=sp.is_sync and sp.state.is_sync,
   rows=rows,sha256=require('digest').sha256_hex(require('msgpack').encode(rows))}
 end
 local u=box.space._user.index.name:get{user}
 return {uuid=tostring(box.info.uuid),shard_id=tarn.cfg.shard_id,ro=box.info.ro,
  term=box.info.election.term,leader=box.info.election.leader,queue=box.info.synchro.queue,
  q=box.info.synchro.quorum,r=box.cfg.replication_linearizable_quorum,
  wal=box.cfg.wal_mode,sync=tarn.cfg.sync,meta_sync=meta.is_sync and meta.state.is_sync,
  version=box.info.version,clock=tarn.clock.status(),policy=row and row.value or box.NULL,
  reader_uid=u and u.id or box.NULL,corpus=corpus,
  conflict_code=box.error.TRANSACTION_CONFLICT,test_hooks=tarn.cfg.test_hooks}
end)
if box.is_in_txn() then box.rollback() end
if not ok then error(result,0) end
return result
"""

INJECT = """
assert(tarn.cfg.test_hooks,'test_hooks required'); assert(not box.info.ro,'actual owner required')
local c=tarn.clock; local before=c.status()
assert(before.loaded and before.read_ready and before.fault==nil,'healthy starting clock required')
assert(type(before.wall)=='number' and type(before.mono)=='number' and before.policy)
-- This one module-local clock-source injection is the existing test seam.
-- No health flag, policy, threshold or original return value is replaced.
local injected=c.test_time(before.wall+before.policy.max_drift+1,before.mono)
assert(injected[1]=='ok')
local result=c.observe() -- Original detector, exactly once in this request.
return {before=before,injected=injected,result=result,after=c.status(),ro=box.info.ro,
 term=box.info.election.term,uuid=tostring(box.info.uuid)}
"""


def clock_refusal(reply, code):
    """Accept only the actual clock cause, never an arbitrary read error."""
    if not isinstance(reply, dict):
        return False
    error = reply.get("error")
    return (reply.get("version") == 1 and reply.get("status") == "error"
            and "rows" not in reply and "slots" not in reply and isinstance(error, dict)
            and error.get("kind") == "unavailable" and error.get("domain") == "tarantool"
            and type(error.get("code")) is int and error["code"] == code
            and isinstance(error.get("message"), str)
            and CLOCK_MESSAGE in error.get("message", ""))


def sql_clock_refusal(error):
    return (isinstance(error, pymysql.MySQLError) and len(error.args) >= 2
            and error.args[0] == 1213 and CLOCK_MESSAGE in str(error.args[1]))


class ClockFaultTest(Test):
    def __init__(self, args):
        super().__init__(args)
        self.tenant = None
        self.bucket = None
        self.tables = ["terminal_clock_" + engine for engine in ENGINES]
        self.before = {}
        self.frozen = {}
        self.injection_attempted = False
        self.injection_observed = False
        self.final_snapshot = None
        self.record("terminal_clock_manifest", source_sha256={
            Path(__file__).name: hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
            scope="terminal module-local hard drift; no reset or healthy-recovery claim")

    def snapshot(self, node):
        names = [self.db + "." + table for table in self.tables]
        return self.observe("terminal_clock_snapshot",
                            lambda: ev(node, SNAPSHOT, [names, self.tenant, self.user],
                                       stage="terminal_clock_readonly_snapshot"), node=node)[0]

    def configuration(self, node, state):
        self.check("RF3 synchronous fsync configuration is unchanged",
                   state["q"] == 2 and state["r"] in (2, "N - Q + 1") and state["wal"] == "fsync"
                   and state["sync"] and state["meta_sync"] and state["test_hooks"]
                   and all(c["sync"] for c in state["corpus"]), node=node, state=state)
        prior = self.before.get(node)
        if prior is not None:
            self.check("clock injection did not change identity or durability settings",
                       all(state[k] == prior[k] for k in ("uuid", "shard_id", "q", "r", "wal", "sync", "meta_sync")),
                       node=node, before=prior, after=state)

    def exact_corpus(self, state, populated):
        rows = [[self.tenant, 1, 10, self.bucket]] if populated else []
        return (len(state.get("corpus", [])) == len(self.tables)
                and all(c.get("rows") == rows for c in state["corpus"]))

    def setup_clock(self):
        self.setup()
        with self.connect() as c:
            ranges = self.sql(c, "SHOW TARN BUCKETS")
            count = sum(int(r[1]) for r in ranges)
            owned = set()
            for row in ranges:
                if int(row[0]) == self.identities[0]["id"]:
                    for part in str(row[2]).split(", ") if row[2] else []:
                        lo, _, hi = part.partition("-")
                        owned.update(range(int(lo), int(hi or lo) + 1))
            self.tenant = next(i for i in range(1, 10000) if bucket_of(i, count) in owned)
            self.bucket = bucket_of(self.tenant, count)
            for engine, table in zip(ENGINES, self.tables):
                self.sql(c, f"CREATE TABLE {table} (tenant INT,id INT,v INT,PRIMARY KEY(tenant,id)) "
                         f"PARTITION BY KEY(tenant) ENGINE={engine}", mutation=True)
                self.sql(c, f"INSERT INTO {table} VALUES (%s,1,10)", (self.tenant,), mutation=True)
                self.grant(table)
        deadline, last = time.monotonic() + 20, None
        while time.monotonic() < deadline:
            states = {node: self.snapshot(node) for group in self.args.replica_groups for node in group}
            last = states
            baseline_ready = all(
                self.exact_corpus(states[node], i == 0) and states[node]["clock"]["loaded"]
                and states[node]["clock"].get("fault") is None
                and isinstance(states[node]["policy"], dict) and states[node]["policy"].get("fault") is None
                for i, group in enumerate(self.args.replica_groups) for node in group)
            if baseline_ready and all(not states[node]["ro"] and states[node]["clock"]["read_ready"]
                                      for node in self.nodes):
                self.before = states
                break
            time.sleep(.05)
        else:
            raise AssertionError("confirmed baseline corpus did not reach every replica", last)
        for i, group in enumerate(self.args.replica_groups):
            for node in group:
                state = self.before[node]
                self.configuration(node, state)
                self.check("fresh disposable cohort has no clock fault", state["clock"].get("fault") is None
                           and isinstance(state["policy"], dict) and state["policy"].get("fault") is None
                           and state["clock"]["loaded"], node=node, state=state)
                self.check("replica belongs to its configured logical shard",
                           state["shard_id"] == self.identities[i]["id"], node=node, state=state)
        self.check("configured owners are actually writable before injection",
                   all(not self.before[node]["ro"] and self.before[node]["clock"]["read_ready"] for node in self.nodes))
        keys = [[self.tenant, 1], [self.tenant, 99]]
        with self.connect() as c:
            for table in self.tables:
                self.paired(c, table, keys, "tenant,id,v", "tenant=%s AND id=%s")
                meta = self.public(self.nodes[0], "meta", {"version": 1, "db": self.db, "name": table})
                self.check("healthy registered metadata before terminal fault", meta.get("status") == "ok", reply=meta)
                request = {"version": 1, "consistency": "independent_points", "table": meta["table"],
                           "ownership": meta["ownership"], "keys": keys}
                result = self.public(self.nodes[0], "get_many", request)
                self.check("healthy registered existing/missing vector before terminal fault",
                           result.get("status") == "ok" and result.get("table") == meta["table"]
                           and result.get("slots") == [
                               {"position": 1, "key": keys[0], "status": "present", "tuple": [self.tenant, 1, 10, self.bucket]},
                               {"position": 2, "key": keys[1], "status": "missing"}], reply=result)
                self.frozen[table] = request

    def inject_once(self):
        owner = self.nodes[0]
        self.injection_attempted = True  # A transport failure never permits a second injection.
        result = self.observe("terminal_clock_inject", lambda: ev(owner, INJECT, stage="terminal_clock_inject_once"),
                              node=owner)[0]
        after = result["after"]
        self.check("original detector latches the injected hard drift",
                   result["result"] == ["clock_unknown", 1213, CLOCK_MESSAGE]
                   and after.get("fault") == FAULT_REASON and after.get("state") == "fault"
                   and not after.get("read_ready") and not result["ro"]
                   and result["uuid"] == self.before[owner]["uuid"], result=result)
        self.injection_observed = True
        deadline, last = time.monotonic() + 15, None
        while time.monotonic() < deadline:
            last = {node: self.snapshot(node) for node in self.args.replica_groups[0]}
            if all(isinstance(s["policy"], dict) and s["policy"].get("fault") == FAULT_REASON
                   and s["clock"].get("fault") == FAULT_REASON for s in last.values()):
                break
            time.sleep(.05)
        else:
            raise AssertionError("fault was not confirmed in every affected replica's policy", last)
        for node, state in last.items():
            self.configuration(node, state)
            self.check("confirmed clock policy carries the actual terminal fault",
                       state["policy"]["fault"] == FAULT_REASON and state["clock"]["fault"] == FAULT_REASON,
                       node=node, state=state, visibility="own read-confirmed metadata transaction")

    def public_refusals(self):
        owner = self.nodes[0]
        code = self.before[owner]["conflict_code"]
        for table in self.tables:
            reply = self.public(owner, "get_many", self.frozen[table])
            self.check(table + " registered KV refuses for the actual clock fault", clock_refusal(reply, code), reply=reply)
            sdk = self.sdk(table, self.frozen[table]["keys"])
            self.check(table + " actual Rust SDK refuses for the actual clock fault", clock_refusal(sdk, code), reply=sdk)
            caught = None
            try:
                with self.connect() as c:
                    self.sql(c, f"SELECT tenant,id,v FROM {table} WHERE tenant=%s AND id IN (1,99)", (self.tenant,))
            except pymysql.MySQLError as error:
                caught = error
            self.check(table + " SQL refuses for the same actual clock fault", sql_clock_refusal(caught),
                       error=None if caught is None else {"type": type(caught).__name__, "args": list(caught.args),
                                                         "sqlstate": getattr(caught, "sqlstate", None)})
        final = {node: self.snapshot(node) for group in self.args.replica_groups for node in group}
        self.final_snapshot = final
        for i, group in enumerate(self.args.replica_groups):
            for node in group:
                state = final[node]
                self.configuration(node, state)
                self.check("raw confirmed corpus is unchanged despite public refusal", self.exact_corpus(state, i == 0),
                           node=node, before=self.before[node]["corpus"], after=state["corpus"],
                           scope="weaker raw local visibility, without the Tarn read-freshness/claim barrier")
                self.check("reader identity is preserved for offline inspection",
                           state["reader_uid"] == self.before[node]["reader_uid"], node=node, state=state)
        self.check("clock-refusal owner remains writable", not final[owner]["ro"], state=final[owner])
        self.record("terminal_clock_preserved_snapshot", nodes=final, database=self.db, reader=self.user,
                    acknowledged_reader_uids=self.users, injection_attempted=self.injection_attempted,
                    injection_observed=self.injection_observed,
                    scope="terminal fault; database/users/policy retained; no clock reset or healthy recovery")

    def cleanup(self, success):
        # The lifecycle controller, not this terminal-fault test, owns process
        # teardown and offline data capture. Do not DROP users or the database.
        errors = []
        for worker in self.workers:
            worker.join(13)
            if worker.is_alive():
                errors.append("owned read worker remains live: " + worker.name)
        if self.injection_attempted and self.final_snapshot is None:
            snapshot = {}
            for group in self.args.replica_groups:
                for node in group:
                    try:
                        snapshot[node] = self.snapshot(node)
                    except Exception as error:
                        errors.append("terminal snapshot unavailable at " + node + ": " + str(error))
            self.record("terminal_clock_failure_snapshot", nodes=snapshot,
                        scope="best-effort read-only observation; missing observations are not assumed healthy")
        try:
            self.private.cleanup()
        except Exception as error:
            errors.append("private credential cleanup: " + str(error))
        self.record("cleanup", database_preserved=self.created,
                    database_possibly_preserved=self.created or self.database_attempted and not self.database_ack,
                    readers_preserved=self.users, reader_creation_attempts=self.user_attempts,
                    success=success and not errors, errors=errors,
                    scope="intentional terminal fixture; only host credentials removed")
        return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("tarn", "shards", "replicas", "sdk", "evidence"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--user", default="sb")
    args = parser.parse_args()
    leaders = args.shards.split(",")
    try:
        groups = json.loads(args.replicas)
    except (TypeError, ValueError):
        parser.error("--replicas must be a JSON array of two RF3 groups")
    if (len(leaders) != 2 or not isinstance(groups, list) or len(groups) != 2
            or any(not isinstance(g, list) or len(g) != 3 or leader not in g for leader, g in zip(leaders, groups))):
        parser.error("two owners and matching RF3 triples are required")
    flat = [node for group in groups for node in group]
    if (any(not isinstance(node, str) or not node or '@' in node or any(c.isspace() for c in node) for node in flat)
            or len(set(flat)) != 6 or not Path(args.sdk).is_file()):
        parser.error("six distinct uncredentialed endpoints and an existing SDK executable are required")
    args.replica_groups = groups
    test = ClockFaultTest(args)
    success, failure = False, None
    try:
        test.setup_clock()
        test.inject_once()
        test.public_refusals()
        success = True
    except Exception:
        failure = traceback.format_exc()
        test.record("failure", traceback=failure)
        print(failure, file=sys.stderr)
    finally:
        errors = test.cleanup(success)
        summary = {"success": success and not errors, "completed_checks": test.completed,
                   "primary_error": failure, "cleanup_errors": errors, "database_preserved": test.created,
                   "reader_uids_preserved": test.users, "injection_attempted": test.injection_attempted,
                   "injection_observed": test.injection_observed,
                   "scope": "disposable RF3 terminal module-local hard clock fault; no recovery/reset claim"}
        (test.evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        test.events.close()
    print(f"{test.completed} checks, {'PASS' if summary['success'] else 'FAIL'}")
    return 0 if summary["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
