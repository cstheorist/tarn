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
import threading
import time
import traceback
import uuid

import pymysql

from kvtest import Test, ev
from movetest import bucket_of

IMPLEMENTED = {"leader", "quorum", "stall", "move", "ddl", "partition"}


def known_ro_serialization(error):
    state = getattr(error, "sqlstate", None)
    if (not isinstance(error, pymysql.MySQLError) or len(error.args) < 2
            or error.args[0] != 1213 or state not in (None, "40001", b"40001")
            or not isinstance(error.args[1], str)):
        return False
    message = error.args[1]
    wrapper = "Deadlock found when trying to get lock; try restarting transaction ("
    if message.startswith(wrapper) and message.endswith(")"):
        message = message[len(wrapper):-1]  # SqlError::conflict's actual SQL wire wrapper.
    return message == ("shard leader changed, retry the transaction "
                       "(Can't modify data on a read-only instance)")


def quorum_sql_outcome(error):
    if not isinstance(error, pymysql.MySQLError) or len(error.args) < 2:
        return None
    state = getattr(error, "sqlstate", None)
    if (isinstance(error, pymysql.OperationalError) and error.args[0] == 2013 and state is None
            and error.args[1] == "Lost connection to MySQL server during query (timed out)"):
        return "client_read_timeout"
    if error.args[0] in (7, 97, 1180, 1205, 1213) and state not in ("40003", b"40003"):
        return "server_refusal"
    return None


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

INSTALL_SCAN = """
local owner=...; assert(tarn.cfg.test_hooks,'test_hooks required')
assert(rawget(_G,'__tarn_kv_scan_hook')==nil,'another owned scan hook exists')
local fiber,clock=require('fiber'),require('clock')
local s={owner=owner,original=yield_point,control=tarn_test_hook,entered=false,resumed=false,
 released=false,expired=false,restore_error=false,expires=clock.monotonic()+10}
s.wrapped=function(name,...)
 local first=name=='scan' and not s.entered
 if first then s.entered=true; s.entered_at=clock.monotonic() end
 local ok,result=pcall(s.original,name,...)
 if first then s.resumed=true; s.resumed_at=clock.monotonic() end
 if not ok then error(result,0) end
 return result
end
rawset(_G,'__tarn_kv_scan_hook',s); yield_point=s.wrapped
assert(s.control('scan','hold')[1]=='ok')
fiber.create(function()
 fiber.sleep(10)
 if rawget(_G,'__tarn_kv_scan_hook')==s and not s.released then
  s.expired=true
  if yield_point~=s.wrapped or tarn_test_hook~=s.control then s.restore_error='scan hook identity changed'; return end
  s.control('scan','release'); s.released=true; yield_point=s.original
 end
end)
return {owner=owner,expires=s.expires}
"""

SCAN_CONTROL = """
local owner,action=...; local s=assert(rawget(_G,'__tarn_kv_scan_hook')); assert(s.owner==owner,'scan owner changed')
assert(not s.restore_error,s.restore_error)
assert(tarn_test_hook==s.control,'scan control function changed')
assert(yield_point==s.wrapped or s.released and yield_point==s.original,'scan yield function changed')
if action=='release' or action=='close' then
 if action=='release' then assert(s.entered and not s.resumed and not s.expired,'scan no longer held') end
 if not s.released then s.released_at=require('clock').monotonic(); s.control('scan','release'); s.released=true end
 if yield_point==s.wrapped then yield_point=s.original end
end
local result={owner=s.owner,entered=s.entered,resumed=s.resumed,released=s.released,expired=s.expired,
 entered_at=s.entered_at or box.NULL,resumed_at=s.resumed_at or box.NULL,released_at=s.released_at or box.NULL}
if action=='close' then rawset(_G,'__tarn_kv_scan_hook',nil) end
return result
"""


class FaultTest(Test):
    def __init__(self, args):
        super().__init__(args)
        self.broker_argv = json.loads(args.fault_control)
        self.stopped = set()
        self.ingress_paused = set()
        self.capabilities = set()
        self.replication_snapshot = None
        self.broker_nodes = {}
        self.initial_endpoints = list(self.nodes)
        self.finished_scenarios = []
        self.fault_tables_created = False
        self.requested = set(args.scenarios.split(","))

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
        self.capabilities = set(reply["capabilities"])
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
        self.replication_snapshot = reply.get("replication_ingress")
        return found

    def relay_snapshot(self, snapshot):
        assert isinstance(snapshot, dict) and snapshot.get("version") == 1 and snapshot.get("status") == "ok", snapshot
        flows = snapshot.get("flows", [])
        assert len(flows) == 12 and len({f["id"] for f in flows}) == 12, snapshot
        pairs = set()
        for flow in flows:
            client, server = flow["client_node"], flow["server_node"]
            assert client in self.broker_nodes and server in self.broker_nodes and client != server, flow
            assert self.broker_nodes[client]["shard_id"] == self.broker_nodes[server]["shard_id"], flow
            assert flow["id"] == client + "<-" + server and (client, server) not in pairs, flow
            pairs.add((client, server))
            for name in ("queued_s2c", "queued_c2s", "drained_s2c", "drained_c2s", "connections", "accepted",
                         "refused", "errors", "connect_failures"):
                assert type(flow.get(name)) is int and flow[name] >= 0, flow
        return snapshot

    def ingress(self, node_id, pause):
        assert node_id in self.broker_nodes and type(pause) is bool
        assert "replication_ingress" in self.capabilities, "broker lacks replication ingress control"
        if pause:
            self.ingress_paused.add(node_id)  # A lost ACK does not remove the obligation to heal.
        reply = self.control({"action": "replication_ingress", "node": node_id, "pause": pause})
        snapshot = self.relay_snapshot(reply.get("replication_ingress"))
        selected = [f for f in snapshot["flows"] if f["client_node"] == node_id]
        assert len(selected) == 2 and all(f["paused"] is pause for f in selected), snapshot
        if not pause:
            self.ingress_paused.discard(node_id)
        self.record("replication_ingress_transition", node=node_id, pause=pause, snapshot=snapshot)
        return snapshot

    def frozen_ingress(self, node_id, before):
        self.inspect()
        after = self.relay_snapshot(self.replication_snapshot)
        earlier = {f["id"]: f for f in before["flows"] if f["client_node"] == node_id}
        current = {f["id"]: f for f in after["flows"] if f["client_node"] == node_id}
        self.check("both replication-ingress queued-byte counters remain frozen",
                   len(current) == 2 and current.keys() == earlier.keys()
                   and all(f["paused"] and f["queued_s2c"] == earlier[k]["queued_s2c"] for k, f in current.items()),
                   node=node_id, before=before, after=after,
                   scope="queued-before-drain; pre-pause drain completions may still increase")
        self.check("relay connection caps did not create another fault",
                   all(f["refused"] == earlier[k]["refused"] for k, f in current.items()),
                   before=before, after=after)
        return after

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
        for node in list(self.ingress_paused):
            try:
                self.ingress(node, False)
            except Exception as error:
                errors.append((node, "replication ingress heal: " + str(error)))
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

    def start_sdk(self, table, keys):
        state, lock = {}, threading.Lock()
        def run():
            try:
                outcome = {"result": self.sdk(table, keys)}
            except Exception as error:
                outcome = {"error": {"type": type(error).__name__, "args": list(error.args),
                                     "sqlstate": getattr(error, "sqlstate", None)}}
            with lock:
                state.update(outcome)
        worker = threading.Thread(target=run, name="kv181-held-sdk-" + table, daemon=True)
        self.workers.append(worker); worker.start()
        return worker, state, lock

    def await_scan(self, endpoint, owner, worker):
        deadline, last = time.monotonic() + 4, None
        while time.monotonic() < deadline:
            last = ev(endpoint, SCAN_CONTROL, [owner, "snapshot"])
            assert not last["expired"] and not last["resumed"], last
            if last["entered"]:
                self.record("sdk_second_group_scan_held", endpoint=endpoint, state=last)
                return last
            assert worker.is_alive(), "SDK finished before the selected scan was held"
            time.sleep(.01)
        raise AssertionError("SDK second group did not reach the real scan hook", last)

    def close_scan(self, endpoint, owner, worker):
        active_error = sys.exc_info()[0] is not None
        try:
            result = ev(endpoint, SCAN_CONTROL, [owner, "close"])
            self.record("scan_hook_restore", endpoint=endpoint, result=result)
            assert result["released"] and not result["expired"], result
        except Exception:
            self.record("scan_hook_cleanup_error", traceback=traceback.format_exc())
            if not active_error:
                raise
        finally:
            if worker is not None:
                worker.join(13)
                if worker.is_alive():
                    self.record("scan_worker_unsettled", name=worker.name)
                    if not active_error:
                        raise AssertionError("owned held SDK worker remains live")

    def move_report(self, endpoint, table, bucket):
        meta = self.public(endpoint, "meta", {"version": 1, "db": self.db, "name": table})
        assert meta.get("status") == "ok", meta
        report = meta["buckets"]
        owned = any(lo <= bucket <= hi for lo, hi in report["owned"])
        other = [entry for entry in report["others"] if entry[0] == bucket]
        return meta, owned, other

    def paired_values(self, table, tenant, expected):
        keys = [[tenant, key] for key in expected]
        with self.connect() as c:
            self.warm_readonly_route(c, table, tenant, expected[1], "healthy SQL route has the exact expected value")
            self.paired(c, table, keys, "tenant,id,v", "tenant=%s AND id=%s")
            for key, value in expected.items():
                rows = self.sql(c, f"SELECT v FROM {table} WHERE tenant=%s AND id=%s", (tenant, key))
                self.check("exact SQL ACK-dependent value", rows == (() if value is None else ((value,),)),
                           key=[tenant, key], expected=value, rows=rows)

    def warm_readonly_route(self, connection, table, tenant, expected, label):
        # Independent autocommit reads only, before either caller's BEGIN.
        # Each known RO reply retires its physical client. Allow the explicit
        # configured pool's stale clients plus one positive SELECT; individual
        # socket IDs are not inferred from a refusal. Never replay SQL history,
        # a mutation, or an uncertain transport result.
        deadline = time.monotonic() + 10
        pool_size = self.args.sql_pool_size
        max_attempts = pool_size + 1
        statement = f"SELECT v FROM {table} WHERE tenant=%s AND id=1"
        for attempt in range(1, max_attempts + 1):
            try:
                rows = self.sql(connection, statement, (tenant,))
            except pymysql.MySQLError as error:
                retry = known_ro_serialization(error)
                self.record("known_ro_readonly_warmup_refusal", attempt=attempt, table=table,
                            sql_pool_size=pool_size, max_attempts=max_attempts,
                            error={"type": type(error).__name__, "args": list(error.args),
                                   "sqlstate": getattr(error, "sqlstate", None)},
                            retry_eligible=retry, guard_coverage=False,
                            scope="independent read-only route warm-up; no mutation or transaction replay")
                if not retry or attempt == max_attempts or time.monotonic() >= deadline:
                    raise
                time.sleep(.05)
                if time.monotonic() >= deadline:
                    raise
            else:
                self.check(label, rows == ((expected,),), rows=rows, attempt=attempt, table=table,
                           sql_pool_size=pool_size, max_attempts=max_attempts,
                           scope="exact positive SELECT required before the subsequent once-only mutation")
                return rows

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
        if "partition" in self.requested:
            assert "replication_ingress" in self.capabilities, "partition requested without relay capability"
            self.relay_snapshot(self.replication_snapshot)
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
            second = set(range(1, count + 1)) - owned
            other_tenants = []
            for candidate in range(1, 10000):
                if bucket_of(candidate, count) in second and all(
                        bucket_of(candidate, count) != bucket_of(previous, count) for previous in other_tenants):
                    other_tenants.append(candidate)
                    if len(other_tenants) == 4:
                        break
            assert len(other_tenants) == 4 and self.identities[0]["id"] < self.identities[1]["id"]
            self.bucket_count = count
            self.move_tenants = dict(zip(("memtx", "vinyl"), other_tenants[:2]))
            self.ddl_tenants = dict(zip(("memtx", "vinyl"), other_tenants[2:]))
            for engine in ("memtx", "vinyl"):
                table = "fault_" + engine
                self.sql(c, f"CREATE TABLE {table} (tenant INT,id INT,v INT,PRIMARY KEY(tenant,id)) "
                         f"PARTITION BY KEY(tenant) ENGINE={engine}", mutation=True)
                self.sql(c, f"INSERT INTO {table} VALUES (%s,1,10)", (self.tenant,), mutation=True)
                self.grant(table)
                if "partition" in self.requested:
                    partition = "partition_" + engine
                    self.sql(c, f"CREATE TABLE {partition} (tenant INT,id INT,v INT,PRIMARY KEY(tenant,id)) "
                             f"PARTITION BY KEY(tenant) ENGINE={engine}", mutation=True)
                    self.sql(c, f"INSERT INTO {partition} VALUES (%s,1,10)", (self.tenant,), mutation=True)
                    self.grant(partition)
                for scenario, tenants in (("move", self.move_tenants), ("ddl", self.ddl_tenants)):
                    if scenario in self.requested:
                        table = scenario + "_" + engine
                        self.sql(c, f"CREATE TABLE {table} (tenant INT,id INT,v INT,PRIMARY KEY(tenant,id)) "
                                 f"PARTITION BY KEY(tenant) ENGINE={engine}", mutation=True)
                        self.sql(c, f"INSERT INTO {table} VALUES (%s,1,10),(%s,1,20)",
                                 (self.tenant, tenants[engine]), mutation=True)
                        self.grant(table)
        self.fault_tables_created = True
        for node in self.broker_nodes.values():
            self.configuration(node["endpoint"], "fault_memtx")
        self.values = {"memtx": {1: 10, 2: None, 99: None}, "vinyl": {1: 10, 2: None, 99: None}}

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
                self.warm_readonly_route(c, table, tenant, self.values[engine][1],
                                         "SQL read warms the replacement route without a mutation")
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
            sql_error, sql_kind, sql_result = None, None, None
            query_started, query_finished, result_returned = None, None, False
            try:
                with self.connect() as c:
                    query_started = time.monotonic_ns()
                    sql_result = self.sql(c, f"SELECT v FROM {table} WHERE tenant=%s AND id=1", (tenant,))
                    result_returned = True
            except pymysql.MySQLError as caught:
                sql_kind = quorum_sql_outcome(caught)
                sql_error = {"type": type(caught).__name__, "args": list(caught.args),
                             "errno": caught.args[0] if caught.args and isinstance(caught.args[0], int) else None,
                             "sqlstate": getattr(caught, "sqlstate", None)}
            finally:
                query_finished = time.monotonic_ns()
            outcome = {"kind": sql_kind, "error": sql_error, "row_result_returned": result_returned,
                       "configured_client_read_timeout_seconds": 15,
                       "elapsed_ns": query_finished - query_started if query_started is not None else None,
                       "guard_coverage": False}
            self.check("no-quorum SQL yields a classified no-row outcome",
                       query_started is not None and not result_returned and sql_kind is not None,
                       outcome=outcome, unexpected_result=sql_result)
            self.record("no_quorum_classification", before=before, after=observed,
                        first_guard="role" if error.get("domain") == "tarantool" and error.get("code") == 7
                        else "clock" if "clock" in error.get("message", "").lower() else "unspecified_api_error",
                        direct_error=error, direct_outcome_origin="versioned_api_error_envelope",
                        sdk_outcome_kind=sdk.get("error", {}).get("kind"), sql_outcome=outcome,
                        scope="known-RO if role fencing precedes barrier; no arbitrary partition claim")
        finally:
            self.restore()
        self.retarget(table)
        self.caught_up(table, tenant, self.values[engine])
        self.paired_values(table, tenant, self.values[engine])

    def scenario_move(self, engine):
        table = "move_" + engine
        first, second = self.tenant, self.move_tenants[engine]
        keys = [[first, 1], [second, 1], [second, 99]]
        bucket = bucket_of(second, self.bucket_count)
        with self.connect() as c:
            self.paired(c, table, keys, "tenant,id,v", "tenant=%s AND id=%s")
        source, meta = self.leader(1, table)
        target, _ = self.leader(0, table)
        before_meta, owned, _ = self.move_report(source["endpoint"], table, bucket)
        assert owned and before_meta["ownership"] == meta["ownership"], before_meta
        original = self.point_request(meta, [[second, 1], [second, 99]])
        owner, worker = uuid.uuid4().hex, None
        installed = False
        try:
            installed = True
            receipt = ev(source["endpoint"], INSTALL_SCAN, [owner])
            self.record("scan_hook_install", endpoint=source["endpoint"], receipt=receipt)
            worker, outcome, lock = self.start_sdk(table, keys)
            self.await_scan(source["endpoint"], owner, worker)
            with self.connect() as c:
                # The sequential SDK already completed its first shard
                # group before this second-shard point reached the hook.
                self.sql(c, f"UPDATE {table} SET v=11 WHERE tenant=%s AND id=1", (first,), mutation=True)
                value_ack = self.ack
                try:
                    self.sql(c, f"TARN MOVE BUCKET {bucket} TO SHARD {self.identities[0]['id']}", mutation=True)
                except Exception:
                    # A missing job reply never authorizes mutation replay.
                    try:
                        self.record("uncertain_move_inspection", rows=self.sql(c, "SHOW TARN MOVES"))
                    except Exception:
                        self.record("uncertain_move_inspection_error", traceback=traceback.format_exc())
                    raise
            release = ev(source["endpoint"], SCAN_CONTROL, [owner, "release"])
            self.record("scan_release_after_move_ack", result=release, value_ack_sequence=value_ack,
                        move_ack_sequence=self.ack, bucket=bucket)
            worker.join(13)
            self.check("held SDK worker completes after the real move", not worker.is_alive())
            with lock:
                observed = dict(outcome)
            result = observed.get("result", {})
            expected = [{"tenant": first, "id": 1, "v": 11}, {"tenant": second, "id": 1, "v": 20}, None]
            self.check(engine + " actual SDK retries the complete vector under the same schema",
                       result.get("status") == "ok" and result.get("rows") == expected
                       and result.get("schema", {}).get("token") == meta["table"],
                       outcome=observed, expected=expected, value_ack_sequence=value_ack)
        finally:
            if installed:
                self.close_scan(source["endpoint"], owner, worker)
        current, still_owned, sent = self.move_report(source["endpoint"], table, bucket)
        _, target_owned, target_other = self.move_report(target["endpoint"], table, bucket)
        self.check(engine + " real move publishes Sent and target ownership",
                   not still_owned and len(sent) == 1 and sent[0][1] == "sent"
                   and sent[0][2] == self.identities[0]["id"] and target_owned and not target_other
                   and current["ownership"]["bucket_epoch"] > meta["ownership"]["bucket_epoch"],
                   source=current, target_owned=target_owned, bucket=bucket)
        for label, request in (("old owner token", original),
                               ("current unowned token", self.point_request(current, [[second, 1], [second, 99]]))):
            reply = self.public(source["endpoint"], "get_many", request)
            error = reply.get("error", {})
            self.check(engine + " " + label + " refuses without a missing prefix",
                       reply.get("status") == "error" and "slots" not in reply and error.get("kind") == "moved",
                       reply=reply)
            if label == "current unowned token":
                self.check("current token reaches the actual numeric bucket guard",
                           error.get("domain") == "mysql" and error.get("code") == 1213
                           and ("bucket " + str(bucket)) in error.get("message", ""), reply=reply)
        with self.connect() as c:
            self.paired(c, table, keys, "tenant,id,v", "tenant=%s AND id=%s")
        self.configuration(source["endpoint"], table)
        self.configuration(target["endpoint"], table)

    def scenario_ddl(self, engine):
        table = "ddl_" + engine
        first, second = self.tenant, self.ddl_tenants[engine]
        keys = [[first, 1], [second, 1], [second, 99]]
        with self.connect() as c:
            self.paired(c, table, keys, "tenant,id,v", "tenant=%s AND id=%s")
        source, meta = self.leader(1, table)
        request = self.point_request(meta, [[second, 1], [second, 99]])
        owner, worker = uuid.uuid4().hex, None
        installed = False
        try:
            installed = True
            receipt = ev(source["endpoint"], INSTALL_SCAN, [owner])
            self.record("scan_hook_install", endpoint=source["endpoint"], receipt=receipt)
            worker, outcome, lock = self.start_sdk(table, keys)
            self.await_scan(source["endpoint"], owner, worker)
            with self.connect() as c:
                try:
                    self.sql(c, f"ALTER TABLE {table} ADD COLUMN extra INT NOT NULL DEFAULT 7 FIRST", mutation=True)
                except Exception:
                    try:
                        self.record("uncertain_ddl_inspection", endpoint=source["endpoint"],
                                    jobs=ev(source["endpoint"], "return tarn_jobs()"))
                    except Exception:
                        self.record("uncertain_ddl_inspection_error", traceback=traceback.format_exc())
                    raise
            release = ev(source["endpoint"], SCAN_CONTROL, [owner, "release"])
            self.record("scan_release_after_ddl_ack", result=release, ddl_ack_sequence=self.ack)
            worker.join(13)
            self.check("held SDK worker completes after the real DDL", not worker.is_alive())
            with lock:
                observed = dict(outcome)
            result = observed.get("result", {})
            self.check(engine + " actual SDK rejects a changed schema without its earlier prefix",
                       result.get("status") == "error" and "rows" not in result
                       and result.get("error", {}).get("kind") == "schema_changed",
                       outcome=observed, original_table_token=meta["table"])
        finally:
            if installed:
                self.close_scan(source["endpoint"], owner, worker)
        refused = self.public(source["endpoint"], "get_many", request)
        self.check(engine + " frozen pre-DDL RPC token is terminal",
                   refused.get("status") == "error" and "slots" not in refused
                   and refused.get("error", {}).get("kind") == "schema_changed"
                   and refused["error"].get("code") == 1412, reply=refused)
        self.record("explicit_schema_reopen", table=table,
                    scope="the next SDK invocation deliberately opens a new handle; no implicit reinterpretation")
        with self.connect() as c:
            self.paired(c, table, keys, "extra,tenant,id,v", "tenant=%s AND id=%s")
        fresh = self.public(source["endpoint"], "meta", {"version": 1, "db": self.db, "name": table})
        self.check(engine + " explicit reopen has the new descriptor",
                   fresh.get("status") == "ok" and fresh.get("table") != meta["table"], reply=fresh)
        self.configuration(source["endpoint"], table)

    def scenario_partition(self, engine):
        table, tenant = "partition_" + engine, self.tenant
        expected_before = {1: 10, 2: None, 99: None}
        self.paired_values(table, tenant, expected_before)
        self.caught_up(table, tenant, expected_before)
        old, meta = self.leader(0, table)
        before = self.configuration(old["endpoint"], table)
        keys = [[tenant, 1], [tenant, 2], [tenant, 99]]
        request = self.point_request(meta, keys)
        raw_code = """
local name,tenant=...; box.begin{txn_isolation='read-confirmed'}
local ok,result=pcall(function()
 local sp=assert(box.space[name]); local function get(id) local row=sp:get{tenant,id}; return row and row:totable() or box.NULL end
 return {existing=get(1),inserted=get(2),absent=get(99),ro=box.info.ro,term=box.info.election.term}
end)
if box.is_in_txn() then box.rollback() end
if not ok then error(result) end
return result
"""
        baseline_raw = []
        for node in self.group_nodes(0):
            raw = ev(node["endpoint"], raw_code, [self.db + "." + table, tenant])
            assert raw["existing"][:3] == [tenant, 1, 10] and raw["inserted"] is None and raw["absent"] is None, raw
            baseline_raw.append({"node": node["node_id"], "image": raw})
        self.record("partition_confirmed_baseline", images=baseline_raw, table=table)
        self.inspect()
        relay_before = self.relay_snapshot(self.replication_snapshot)
        self.check("all twelve fixed relay flows carried traffic before the cut",
                   all(not f["paused"] and f["accepted"] > 0 and f["queued_s2c"] > 0 and f["queued_c2s"] > 0
                       for f in relay_before["flows"]), snapshot=relay_before)
        codes = ev(old["endpoint"], """
local out={}; for _,name in ipairs({'READONLY','TRANSACTION_CONFLICT','TRANSACTION_TIMEOUT',
 'QUORUM_WAIT','SYNC_QUORUM_TIMEOUT','SYNC_ROLLBACK','INTERFERING_PROMOTE','TIMEOUT'}) do
 if type(box.error[name])=='number' then out[name]=box.error[name] end
end; return out
""")
        self.record("partition_read_refusal_codes", constants=codes)
        try:
            paused = self.ingress(old["node_id"], True)
            self.check("only the old leader's two incoming replication gates are paused",
                       all(f["paused"] == (f["client_node"] == old["node_id"]) for f in paused["flows"]), snapshot=paused)
            candidates = [n for n in self.group_nodes(0) if n["node_id"] != old["node_id"]]
            candidate = sorted(candidates, key=lambda n: n["replica"])[0]
            # One ordinary public promotion is allowed; it neither changes
            # the configured membership/quorum nor stops a process.
            promoted = ev(candidate["endpoint"], "box.ctl.promote(); return {ro=box.info.ro,term=box.info.election.term}")
            self.record("partition_public_promotion", node=candidate["node_id"], result=promoted)
            new, _ = self.leader(0, table, exclude=(old["node_id"],))
            replacement = self.configuration(new["endpoint"], table)
            self.check("relay cut replacement has an actual newer quorum term",
                       replacement["term"] > before["term"], before=before, replacement=replacement)
            # The SQL gateway may retain a responsive connection to A. It
            # must learn the replacement through A's actual RO response,
            # never a test-created routing flag or failed mutation replay.
            deadline, former = time.monotonic() + 15, None
            while time.monotonic() < deadline:
                former = self.state(old["endpoint"], table)
                if former["ro"] and former["term"] >= replacement["term"]:
                    break
                time.sleep(.02)
            else:
                raise AssertionError("cut former leader did not supply a real RO route fence", former)
            self.frozen_ingress(old["node_id"], paused)
            with self.connect() as c:
                self.warm_readonly_route(c, table, tenant, 10, "partition SQL route warms through real RO fencing")
                self.sql(c, "BEGIN")
                self.sql(c, f"UPDATE {table} SET v=11 WHERE tenant=%s AND id=1", (tenant,))
                self.sql(c, f"INSERT INTO {table} VALUES (%s,2,12)", (tenant,))
                self.sql(c, "COMMIT", mutation=True)  # One mutation attempt; any uncertainty fails.
            ack_sequence = self.ack
            raw = ev(old["endpoint"], raw_code, [self.db + "." + table, tenant])
            self.record("partition_raw_after_sql_ack", sql_ack_sequence=ack_sequence, image=raw,
                        scope="raw read-confirmed native reads omit the Tarn freshness/claim barrier")
            self.check(engine + " post-ACK raw stale existing/missing-new witness is observed",
                       raw["existing"] is not None and raw["existing"][:3] == [tenant, 1, 10]
                       and raw["inserted"] is None and raw["absent"] is None, raw=raw)
            self.frozen_ingress(old["node_id"], paused)
            result = self.public(old["endpoint"], "get_many", request)
            observed = self.configuration(old["endpoint"], table)
            if result.get("status") == "ok":
                slots = result.get("slots", [])
                exact = (len(slots) == 3 and [s.get("position") for s in slots] == [1, 2, 3]
                         and [s.get("key") for s in slots] == keys
                         and [s.get("status") for s in slots] == ["present", "present", "missing"]
                         and [s["tuple"][:3] for s in slots[:2]] == [[tenant, 1, 11], [tenant, 2, 12]]
                         and "tuple" not in slots[2])
                self.check(engine + " cut endpoint returns only the complete new ACK image", exact, reply=result)
            else:
                error = result.get("error", {})
                supported = (error.get("kind") == "moved" and
                             (error.get("domain") == "tarantool" and error.get("code") == codes.get("READONLY")
                              or error.get("domain") == "mysql" and error.get("code") == 1213))
                supported = supported or (error.get("kind") == "unavailable"
                                         and error.get("domain") == "tarantool" and error.get("code") in codes.values())
                self.check(engine + " cut endpoint gives a specific whole read refusal",
                           result.get("status") == "error" and "slots" not in result and supported,
                           reply=result, state=observed, sql_ack_sequence=ack_sequence)
            self.record("partition_endpoint_classification", actual_ro=observed["ro"], actual_term=observed["term"],
                        error=result.get("error"), scope="replication-ingress cut; no assumed stale writable role")
            self.paired_values(table, tenant, {1: 11, 2: 12, 99: None})
            self.frozen_ingress(old["node_id"], paused)
        finally:
            self.restore()
        self.retarget(table)
        self.caught_up(table, tenant, {1: 11, 2: 12, 99: None})
        self.paired_values(table, tenant, {1: 11, 2: 12, 99: None})
        self.configuration(self.nodes[0], table)
        self.record("partition_recovery_complete", table=table,
                    scope="two peer replication ingress flows only; direct clients/freshness/recovery RPCs remain allowed")

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
    parser.add_argument("--sql-pool-size", required=True, type=int, choices=(4,),
                        help="fixed physical pool configured by the owned RF3 launcher")
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
                   "sql_pool_size": args.sql_pool_size,
                   "requested_scenarios": scenarios, "completed_scenarios": test.finished_scenarios,
                   "primary_error": failure, "cleanup_errors": errors, "database_preserved": test.created,
                   "scope": "Requested RF3 histories include bounded replication-ingress cuts, not arbitrary networking or snapshots"}
        (test.evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n"); test.events.close()
    print(f"{test.completed} checks, {'PASS' if summary['success'] else 'FAIL'}")
    return 0 if summary["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
