#!/usr/bin/env python3
"""Owned RF3 SQL/KV fault histories. Signals are delegated to a checked broker.

No service lifecycle, host PID signal, quorum change or mutation retry lives
here. --scenarios is explicit; an unimplemented requested case is an error.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
import traceback
import uuid

import pymysql

from kvtest import Test, ev
from movetest import bucket_of

IMPLEMENTED = {"leader", "quorum", "stall"}

STATE = """
assert(rawget(_G,'tarn')); local name=...; local s=assert(box.space[name])
return {shard_id=tarn.cfg.shard_id,uuid=tostring(box.info.uuid),ro=box.info.ro,
 term=box.info.election.term,leader=box.info.election.leader,queue=box.info.synchro.queue,
 q=box.info.synchro.quorum,r=box.cfg.replication_linearizable_quorum,
 wal=box.cfg.wal_mode,sync=tarn.cfg.sync,space_sync=s.is_sync and s.state.is_sync,
 clock=tarn.clock.status(),vclock=box.info.vclock}
"""

INSTALL_OBSERVER = """
local owner=...; assert(tarn.cfg.test_hooks,'test_hooks required')
assert(rawget(_G,'__tarn_kv_fault_observer')==nil,'another clock observer exists')
local fiber,clock=require('fiber'),require('clock'); local c=tarn.clock
local s={owner=owner,original=c.observe,events={},sequence=0,expires=clock.monotonic()+60,
 capture_failed=false,expired=false,restore_error=false}
local function capture(result)
 local state=c.status() -- No second observe or raw clock sample.
 s.sequence=s.sequence+1
 if #s.events==64 then table.remove(s.events,1) end
 s.events[#s.events+1]={sequence=s.sequence,result=result,state=state.state,reason=state.reason,
  epoch=state.epoch,read_ready=state.read_ready,fault=state.fault,wall_ns=state.wall_ns,mono_ns=state.mono_ns}
end
s.wrapped=function(...)
 -- Current C.observe returns one status (nil or a typed refusal). Errors
 -- keep their original identity; diagnostic failure changes no result.
 local ok,result=pcall(s.original,...)
 if not ok then error(result,0) end
 local captured=pcall(capture,result); if not captured then s.capture_failed=true end
 return result
end
rawset(_G,'__tarn_kv_fault_observer',s); c.observe=s.wrapped
fiber.create(function()
 fiber.sleep(60)
 if rawget(_G,'__tarn_kv_fault_observer')==s then
  s.expired=true
  if c.observe==s.wrapped then c.observe=s.original else s.restore_error='observer identity changed' end
 end
end)
return {owner=owner,gap=tarn.cfg.clock_max_gap,initial=c.status()}
"""

OBSERVER_STATE = """
local s=assert(rawget(_G,'__tarn_kv_fault_observer')); assert(s.owner==...,'observer owner changed')
return {owner=s.owner,events=s.events,capture_failed=s.capture_failed or false,expired=s.expired or false,
 restore_error=s.restore_error or false,clock=tarn.clock.status()}
"""

RESTORE_OBSERVER = """
local s=rawget(_G,'__tarn_kv_fault_observer'); assert(s and s.owner==...,'observer owner changed')
assert(not s.restore_error,s.restore_error)
assert(tarn.clock.observe==s.wrapped or s.expired and tarn.clock.observe==s.original,'observer function changed')
if tarn.clock.observe==s.wrapped then tarn.clock.observe=s.original end
rawset(_G,'__tarn_kv_fault_observer',nil)
return {restored=tarn.clock.observe==s.original,expired=s.expired or false,capture_failed=s.capture_failed or false}
"""


class FaultTest(Test):
    def __init__(self, args):
        super().__init__(args)
        self.broker_argv = json.loads(args.fault_control)
        self.stopped = set()
        self.broker_nodes = {}
        self.initial_endpoints = list(self.nodes)
        self.finished_scenarios = []
        self.fault_tables_created = False

    def control(self, request):
        def run():
            result = subprocess.run(self.broker_argv, input=json.dumps(request) + "\n", text=True,
                                    capture_output=True, stdin=None, timeout=15)
            if result.returncode:
                raise RuntimeError("fault broker failed", result.returncode, result.stderr[-2000:])
            reply = json.loads(result.stdout)
            if reply.get("version") != 1 or reply.get("status") != "ok":
                raise RuntimeError("fault broker refusal", reply)
            return reply
        return self.observe("fault_control", run, request=request)[0]

    def inspect(self):
        reply = self.control({"action": "inspect"})
        assert "signal" in reply.get("capabilities", []), reply
        found = {}
        for node in reply.get("nodes", []):
            assert all(key in node for key in ("node_id", "shard_id", "replica", "endpoint", "pid", "ppid", "starttime")), node
            assert node["node_id"] not in found and node["pid"] > 0 and node["ppid"] > 0, node
            found[node["node_id"]] = node
        expected = {address for group in self.args.replica_groups for address in group}
        assert len(found) == 6 and {n["endpoint"] for n in found.values()} == expected, reply
        for i, group in enumerate(self.args.replica_groups):
            nodes = [n for n in found.values() if n["endpoint"] in group]
            assert len(nodes) == 3 and {n["shard_id"] for n in nodes} == {self.identities[i]["id"]}, nodes
        self.broker_nodes = found
        return found

    def signal(self, node_id, signal):
        assert node_id in self.broker_nodes and signal in ("STOP", "CONT")
        if signal == "STOP":
            self.stopped.add(node_id)  # A lost reply still requires owned restoration.
        result = self.control({"action": "signal", "node": node_id, "signal": signal})
        deadline, last = time.monotonic() + 10, None
        while time.monotonic() < deadline:
            last = self.inspect()[node_id]
            stopped = last.get("state") in ("T", "t")
            if stopped == (signal == "STOP"):
                if signal == "CONT":
                    self.stopped.discard(node_id)
                self.record("fault_signal_transition", node=node_id, signal=signal, dispatch=result, observed=last)
                return result
            time.sleep(.02)
        raise AssertionError("owned signal state transition was not observed", node_id, signal, last)

    def restore(self):
        errors = []
        for node in list(self.stopped):
            try:
                self.signal(node, "CONT")
            except Exception as error:
                errors.append((node, str(error)))
        self.record("signal_restore", success=not errors, errors=errors)
        if errors:
            raise RuntimeError("owned signal restoration failed", errors)

    def state(self, endpoint, table):
        result = ev(endpoint, STATE, [self.db + "." + table])
        self.record("node_state", endpoint=endpoint, table=table, state=result)
        return result

    def configuration(self, endpoint, table):
        s = self.state(endpoint, table)
        self.check("RF3 durability/configuration remains unchanged", s["q"] == 2
                   and s["r"] in (2, "N - Q + 1") and s["wal"] == "fsync"
                   and s["sync"] and s["space_sync"] and not s["clock"].get("fault"), state=s)
        return s

    def group_nodes(self, index):
        return [n for n in self.broker_nodes.values() if n["endpoint"] in self.args.replica_groups[index]]

    def leader(self, index, table, exclude=(), seconds=35):
        deadline, last = time.monotonic() + seconds, []
        while time.monotonic() < deadline:
            for node in self.group_nodes(index):
                if node["node_id"] in self.stopped or node["node_id"] in exclude:
                    continue
                try:
                    state = self.state(node["endpoint"], table)
                    if not state["ro"] and state["queue"]["len"] == 0 and state["clock"].get("read_ready"):
                        meta = self.public(node["endpoint"], "meta", {"version": 1, "db": self.db, "name": table})
                        if meta.get("status") == "ok":
                            return node, meta
                    last = state
                except Exception as error:
                    last = str(error)
            time.sleep(.05)
        raise AssertionError("no actual fresh leader", index, last)

    def retarget(self, table):
        leaders = [self.leader(i, table)[0]["endpoint"] for i in range(2)]
        old_to_group = {endpoint: i for i, endpoint in enumerate(self.initial_endpoints)}
        for i, group in enumerate(self.args.replica_groups):
            for endpoint in group:
                old_to_group[endpoint] = i
        self.users = [(leaders[old_to_group[node]], uid) for node, uid in self.users]
        self.nodes = leaders
        self.record("current_leader_endpoints", leaders=leaders)

    def point_request(self, meta, keys):
        return {"version": 1, "consistency": "independent_points", "table": meta["table"],
                "ownership": meta["ownership"], "keys": keys}

    def paired_values(self, table, tenant, expected):
        keys = [[tenant, key] for key in expected]
        with self.connect() as c:
            self.paired(c, table, keys, "tenant,id,v", "tenant=%s AND id=%s")
            for key, value in expected.items():
                rows = self.sql(c, f"SELECT v FROM {table} WHERE tenant=%s AND id=%s", (tenant, key))
                self.check("exact SQL ACK-dependent value", rows == (() if value is None else ((value,),)),
                           key=[tenant, key], expected=value, rows=rows)

    def caught_up(self, table, tenant, expected, seconds=20):
        deadline, last = time.monotonic() + seconds, None
        while time.monotonic() < deadline:
            good = True
            for node in self.group_nodes(0):
                if node["node_id"] in self.stopped:
                    good = False; continue
                rows = ev(node["endpoint"], "local name,tenant=...; return box.space[name]:select{tenant}",
                          [self.db + "." + table, tenant])
                image = {r[1]: r[2] for r in rows}
                last = image
                good = good and all((key not in image if value is None else image.get(key) == value)
                                    for key, value in expected.items())
            if good:
                return
            time.sleep(.05)
        raise AssertionError("replicas did not retain exact acknowledged image", last, expected)

    def setup_faults(self):
        self.setup()
        self.inspect()
        with self.connect() as c:
            ranges = self.sql(c, "SHOW TARN BUCKETS")
            count = sum(int(r[1]) for r in ranges)
            first_id = self.identities[0]["id"]
            owned = set()
            for row in ranges:
                if int(row[0]) == first_id:
                    for part in str(row[2]).split(", ") if row[2] else []:
                        lo, _, hi = part.partition("-"); owned.update(range(int(lo), int(hi or lo) + 1))
            self.tenant = next(i for i in range(1, 10000) if bucket_of(i, count) in owned)
            for engine in ("memtx", "vinyl"):
                table = "fault_" + engine
                self.sql(c, f"CREATE TABLE {table} (tenant INT,id INT,v INT,PRIMARY KEY(tenant,id)) "
                         f"PARTITION BY KEY(tenant) ENGINE={engine}", mutation=True)
                self.sql(c, f"INSERT INTO {table} VALUES (%s,1,10)", (self.tenant,), mutation=True)
                self.grant(table)
        self.fault_tables_created = True
        for node in self.broker_nodes.values():
            self.configuration(node["endpoint"], "fault_memtx")
        self.values = {"memtx": {1: 10, 2: None, 99: None}, "vinyl": {1: 10, 2: None, 99: None}}

    def warm_route_read(self, c, table, tenant):
        # Only this independent read before BEGIN may be retried. Discovery
        # caches a no-leader refusal for 100 ms, so a fresh leader witness
        # does not guarantee that the gateway has discarded that refusal.
        assert c.get_autocommit() and not c.server_status & 1
        readonly_routes = {
            f"shard {self.identities[0]['id']} has no reachable leader ({node['endpoint']} is read-only)"
            for node in self.group_nodes(0)
        }
        for attempt in range(8):
            try:
                return self.sql(c, f"SELECT v FROM {table} WHERE tenant=%s AND id=1", (tenant,))
            except pymysql.OperationalError as error:
                code, message = error.args[:2]
                retryable = (code == 1213 and "shard leader changed" in message
                             or code == 2003 and message in readonly_routes)
                if not retryable or attempt == 7:
                    raise
                assert c.get_autocommit() and not c.server_status & 1
                self.record("known_ro_read_retry", attempt=attempt + 1, error=list(error.args),
                            scope="independent autocommit read before BEGIN; no mutation replay")
                time.sleep(.05)

    def scenario_leader(self, engine):
        table, tenant = "fault_" + engine, self.tenant
        self.paired_values(table, tenant, self.values[engine])
        old, meta = self.leader(0, table)
        before = self.configuration(old["endpoint"], table)
        request = self.point_request(meta, [[tenant, 1], [tenant, 2], [tenant, 99]])
        try:
            self.signal(old["node_id"], "STOP")
            new, _ = self.leader(0, table, exclude=(old["node_id"],))
            state = self.configuration(new["endpoint"], table)
            self.check("replacement has a newer actual term", state["term"] > before["term"], before=before, after=state)
            # Let the former leader answer RO7 on existing gateway TCP
            # connections before pausing it across the new SQL ACK. An
            # unresponsive cached socket otherwise waits for the gateway's
            # ordinary RPC deadline, without testing the KV read contract.
            self.signal(old["node_id"], "CONT")
            deadline, former = time.monotonic() + 20, None
            while time.monotonic() < deadline:
                former = self.state(old["endpoint"], table)
                if former["ro"] and former["term"] >= state["term"]:
                    break
                time.sleep(.02)
            else:
                raise AssertionError("former leader did not observe its new read-only term", former)
            self.record("known_ro_route_warmup", former=former, replacement=state,
                        scope="known-RO witness; no old-writable-role or A-B-A promotion claim")
            with self.connect() as c:
                rows = self.warm_route_read(c, table, tenant)
                self.check("SQL read warms the replacement route without a mutation",
                           rows == ((self.values[engine][1],),), rows=rows)
                self.signal(old["node_id"], "STOP")
                current, _ = self.leader(0, table, exclude=(old["node_id"],))
                self.check("replacement remains current while the former leader is paused",
                           current["node_id"] == new["node_id"], current=current, replacement=new)
                self.sql(c, "BEGIN")
                self.sql(c, f"UPDATE {table} SET v=11 WHERE tenant=%s AND id=1", (tenant,))
                self.sql(c, f"INSERT INTO {table} VALUES (%s,2,12)", (tenant,))
                self.sql(c, "COMMIT", mutation=True)
            self.values[engine] = {1: 11, 2: 12, 99: None}
            self.signal(old["node_id"], "CONT")
            result = self.public(old["endpoint"], "get_many", request)
            observed = self.state(old["endpoint"], table)
            if result.get("status") == "ok":
                slots = result.get("slots", [])
                exact = (len(slots) == 3 and [s["status"] for s in slots] == ["present", "present", "missing"]
                         and [s["tuple"][2] for s in slots[:2]] == [11, 12])
                self.check("resumed former leader serves only the replacement ACK image", exact, reply=result, state=observed)
            else:
                error = result.get("error", {})
                self.check("resumed former leader returns a typed refusal without a missing prefix",
                           "slots" not in result and error.get("kind") in ("moved", "unavailable")
                           and error.get("domain") in ("contract", "mysql", "tarantool"), reply=result, state=observed)
            self.record("former_leader_classification", observed_ro=observed["ro"], state=observed,
                        scope="resumed former leader; stale raw view or old writable role is not assumed")
        finally:
            self.restore()
        self.retarget(table)
        self.caught_up(table, tenant, self.values[engine])
        self.paired_values(table, tenant, self.values[engine])

    def scenario_stall(self, engine):
        table, tenant = "fault_" + engine, self.tenant
        self.paired_values(table, tenant, self.values[engine])
        leader, _ = self.leader(0, table)
        before = self.configuration(leader["endpoint"], table)
        owner = uuid.uuid4().hex
        installed = False
        try:
            installed = True  # Inspect the owned nonce even if installation loses its reply.
            observer = ev(leader["endpoint"], INSTALL_OBSERVER, [owner])
            self.record("clock_observer_install", endpoint=leader["endpoint"], result=observer)
            gap = observer["gap"]
            assert isinstance(gap, (int, float)) and 0 < gap <= 10, observer
            self.signal(leader["node_id"], "STOP")
            start = time.monotonic_ns()
            time.sleep(gap + .25)
            self.signal(leader["node_id"], "CONT")
            self.record("real_process_stall", node=leader["node_id"], start_ns=start,
                        end_ns=time.monotonic_ns(), configured_max_gap=gap)
            deadline, witness = time.monotonic() + 10, None
            while time.monotonic() < deadline:
                witness = ev(leader["endpoint"], OBSERVER_STATE, [owner])
                assert not witness["capture_failed"] and not witness["expired"] and not witness["restore_error"], witness
                if any(event.get("reason") == "clock monitor gap exceeded"
                       and event.get("state") == "unknown" and not event.get("read_ready")
                       and isinstance(event.get("result"), list) and event["result"][0] == "clock_unknown"
                       for event in witness["events"]):
                    break
                time.sleep(.02)
            else:
                raise AssertionError("actual monitor-gap UNKNOWN was not observed", witness)
            self.check("real process pause revokes local clock eligibility", not witness["clock"].get("fault"),
                       before=before, witness=witness,
                       scope="actual original C.observe result, no changed clock source or desired health flag")
        finally:
            active_error = sys.exc_info()[0] is not None
            try:
                self.restore()
                if installed:
                    restored = ev(leader["endpoint"], RESTORE_OBSERVER, [owner])
                    self.record("clock_observer_restore", result=restored)
                    assert restored["restored"] and not restored["expired"] and not restored["capture_failed"], restored
            except Exception:
                self.record("clock_observer_cleanup_error", traceback=traceback.format_exc())
                if not active_error:
                    raise
        self.retarget(table)
        self.caught_up(table, tenant, self.values[engine])
        self.paired_values(table, tenant, self.values[engine])
        self.configuration(self.nodes[0], table)

    def scenario_quorum(self, engine):
        table, tenant = "fault_" + engine, self.tenant
        self.paired_values(table, tenant, self.values[engine])
        leader, meta = self.leader(0, table)
        before = self.configuration(leader["endpoint"], table)
        request = self.point_request(meta, [[tenant, 1], [tenant, 99]])
        try:
            for node in self.group_nodes(0):
                if node["node_id"] != leader["node_id"]:
                    self.signal(node["node_id"], "STOP")
            result = self.public(leader["endpoint"], "get_many", request)
            observed = self.state(leader["endpoint"], table)
            error = result.get("error", {})
            self.check("no-quorum direct point refuses the whole existing/missing vector",
                       result.get("status") == "error" and "slots" not in result
                       and error.get("kind") in ("moved", "unavailable")
                       and error.get("domain") in ("contract", "mysql", "tarantool"), reply=result, state=observed)
            self.check("quorum cohort has no unrelated latched clock fault", not observed["clock"].get("fault"), state=observed)
            sdk = self.sdk(table, [[tenant, 1], [tenant, 99]])
            self.check("no-quorum actual SDK returns a typed whole error", sdk.get("status") == "error"
                       and "rows" not in sdk and sdk.get("error", {}).get("kind") in
                       ("moved", "unavailable", "deadline", "transport", "transport_server"), reply=sdk)
            sql_error = None
            try:
                with self.connect() as c:
                    self.sql(c, f"SELECT v FROM {table} WHERE tenant=%s AND id=1", (tenant,))
            except pymysql.MySQLError as caught:
                sql_error = list(caught.args)
            # The client's 15-second read budget can expire before the
            # gateway's 120-second RPC/discovery deadline. This read then
            # loses its connection without rows; it is not a server ERR or
            # a known transaction abort. Keep that outcome separate.
            client_timeout = (sql_error is not None and sql_error[0] == 2013
                              and sql_error[1] == "Lost connection to MySQL server during query (timed out)")
            no_leader = (sql_error is not None and sql_error[0] == 2003
                         and "has no reachable leader" in sql_error[1])
            self.check("no-quorum SQL refuses rows or loses its read connection", sql_error is not None
                       and (sql_error[0] in (7, 97, 1180, 1205, 1213) or no_leader or client_timeout),
                       error=sql_error, classification="client_connection_loss" if client_timeout else "server_refusal")
            self.record("no_quorum_classification", before=before, after=observed,
                        first_guard="role" if error.get("domain") == "tarantool" and error.get("code") == 7
                        else "clock" if "clock" in error.get("message", "").lower() else "barrier_or_transport",
                        direct_error=error,
                        scope="known-RO if role fencing precedes barrier; no arbitrary partition claim")
        finally:
            self.restore()
        self.retarget(table)
        self.caught_up(table, tenant, self.values[engine])
        self.paired_values(table, tenant, self.values[engine])

    def cleanup(self, success):
        errors = []
        try:
            self.restore()
        except Exception as error:
            errors.append(str(error)); success = False
        if self.created and self.fault_tables_created:
            try:
                self.retarget("fault_memtx")
            except Exception as error:
                errors.append("cleanup leader discovery: " + str(error)); success = False
        return errors + super().cleanup(success)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("tarn", "shards", "replicas", "sdk", "evidence", "fault-control", "scenarios"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--user", default="sb")
    args = parser.parse_args()
    scenarios = args.scenarios.split(",")
    if not scenarios or len(set(scenarios)) != len(scenarios) or set(scenarios) - IMPLEMENTED:
        parser.error("implemented explicit scenarios: " + ",".join(sorted(IMPLEMENTED)))
    try:
        groups, argv = json.loads(args.replicas), json.loads(args.fault_control)
    except (TypeError, ValueError):
        parser.error("--replicas and --fault-control must be JSON arrays")
    leaders = args.shards.split(",")
    if (len(leaders) != 2 or not isinstance(groups, list) or len(groups) != 2
            or any(not isinstance(g, list) or len(g) != 3 or leader not in g for leader, g in zip(leaders, groups))):
        parser.error("two leaders and matching RF3 replica triples are required")
    flat = [address for group in groups for address in group]
    if (any(not isinstance(a, str) or not a or '@' in a or any(c.isspace() for c in a) for a in flat)
            or len(set(flat)) != 6 or not isinstance(argv, list) or not argv
            or any(not isinstance(a, str) or not a for a in argv) or not Path(args.sdk).is_file()):
        parser.error("invalid replica endpoints, broker JSON argv or SDK path")
    args.replica_groups = groups
    test = FaultTest(args)
    success, failure = False, None
    try:
        test.setup_faults()
        for scenario in scenarios:
            for engine in ("memtx", "vinyl"):
                getattr(test, "scenario_" + scenario)(engine)
                test.finished_scenarios.append({"scenario": scenario, "engine": engine})
        success = True
    except Exception:
        failure = traceback.format_exc(); test.record("failure", traceback=failure); print(failure, file=sys.stderr)
    finally:
        errors = test.cleanup(success)
        summary = {"success": success and not errors, "completed_checks": test.completed,
                   "requested_scenarios": scenarios, "completed_scenarios": test.finished_scenarios,
                   "primary_error": failure, "cleanup_errors": errors, "database_preserved": test.created,
                   "scope": "RF3 leader, no-quorum and observed monitor-gap histories; no asymmetric partition or snapshot claim"}
        (test.evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n"); test.events.close()
    print(f"{test.completed} checks, {'PASS' if summary['success'] else 'FAIL'}")
    return 0 if summary["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
