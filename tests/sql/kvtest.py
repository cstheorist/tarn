#!/usr/bin/env python3
"""Paired SQL / registered KV v1 / actual Rust SDK checks on an owned cluster.

Starts no services and never retries mutations. Requires TARANTOOL, PyMySQL,
and --sdk pointing to the compiled tarn-sql strong_kv example. Administrative
EVAL is confined to fixture setup/inspection; reader calls use kvrpc.lua.
"""
import argparse
import copy
import decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid

import pymysql

from lazytest import ev as _admin_ev, q, rpc
from lazyfaults import Lease as ApplyLease, decision_node
from movetest import bucket_of


def ev(node, script, args=None, *, stage="administrative_eval", sensitive=()):
    """Keep admin transport uncertainty, without retaining its argv secrets.

    The test transport may carry credential-setup arguments in argv. Its
    outer timeout, or a timeout printed by an owned executable wrapper,
    must never reach a retained traceback as a raw command.
    """
    try:
        return _admin_ev(node, script, args)
    except Exception as error:
        transport = isinstance(error, (subprocess.TimeoutExpired, subprocess.CalledProcessError, OSError))
        message = "" if transport else str(error)
        nested = isinstance(error, RuntimeError) and any(
            marker in message for marker in ("TimeoutExpired", "CalledProcessError", "FileNotFoundError"))
        if transport or nested:
            details = {"stage": stage, "type": type(error).__name__, "outcome": "unknown_no_replay",
                       "script_sha256": hashlib.sha256(script.encode()).hexdigest()}
            for name in ("timeout", "returncode", "errno"):
                value = getattr(error, name, None)
                if isinstance(value, (int, float)):
                    details[name] = value
            command = getattr(error, "cmd", None)
            if command is not None:
                encoded = json.dumps(command, default=str, ensure_ascii=True, sort_keys=True).encode()
                details["command_sha256"] = hashlib.sha256(encoded).hexdigest()
            if nested:
                details["private_diagnostic_sha256"] = hashlib.sha256(message.encode()).hexdigest()
            raise RuntimeError("Administrative transport failure: " + json.dumps(details, sort_keys=True)) from None
        redacted = message
        for secret in sensitive:
            if isinstance(secret, str) and secret:
                for spelling in (secret, json.dumps(secret)[1:-1], repr(secret)[1:-1]):
                    redacted = redacted.replace(spelling, "<redacted>")
        if redacted != message:
            # Preserve the remaining diagnostic/code text; suppress the raw
            # original context even when a wrapper truncated its type tag.
            raise RuntimeError(redacted) from None
        raise  # Actual database diagnostics are not reclassified by text/code.


def typed(value):
    if isinstance(value, decimal.Decimal):
        return {"decimal": str(value)}
    if isinstance(value, bytes):
        return {"binary_hex": value.hex()}
    if isinstance(value, (list, tuple)):
        return [typed(v) for v in value]
    if isinstance(value, dict):
        return {k: typed(v) for k, v in value.items()}
    return value


def wire(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and abs(value) > 2**53 - 1:
        return {"uint64" if value >= 0 else "int64": str(value)}
    if isinstance(value, (list, tuple)):
        return [wire(v) for v in value]
    if isinstance(value, dict):
        return {k: wire(v) for k, v in value.items()}
    return typed(value)


def integers(value):
    if isinstance(value, dict) and len(value) == 1 and ("int64" in value or "uint64" in value):
        return int(next(iter(value.values())))
    if isinstance(value, dict):
        return {k: integers(v) for k, v in value.items()}
    if isinstance(value, list):
        return [integers(v) for v in value]
    return value


class Test:
    def __init__(self, args):
        self.args = args
        self.nodes = args.shards.split(",")
        self.gateways = args.tarn.split(",")
        self.db = "kv181_" + uuid.uuid4().hex[:12]
        self.user = "kv181_" + uuid.uuid4().hex[:12]
        self.password = uuid.uuid4().hex
        self.created = False
        self.database_attempted = False
        self.database_ack = False
        self.completed = 0
        self.seq = 0
        self.history_lock = threading.RLock()
        self.ack = None
        self.identities = []
        self.users = []
        self.user_attempts = []
        self.workers = []
        self.pending_completed = []
        self.evidence = Path(args.evidence)
        self.evidence.mkdir(mode=0o700, parents=False, exist_ok=False)
        self.events = (self.evidence / "history.jsonl").open("x", encoding="utf-8")
        os.chmod(self.events.name, 0o600)
        self.private = tempfile.TemporaryDirectory(prefix="tarn-kv181-credentials-")
        self.credentials = Path(self.private.name) / "credentials.json"
        self.credentials.write_text(json.dumps({"user": self.user, "password": self.password}) + "\n")
        os.chmod(self.credentials, 0o600)
        self.seeds = Path(self.private.name) / "seeds.json"
        self.record("manifest", arguments=vars(args), database=self.db, reader=self.user,
                    source_sha256={p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                   for p in (Path(__file__), Path(__file__).with_name("kvrpc.lua"))},
                    sdk_sha256=hashlib.sha256(Path(args.sdk).read_bytes()).hexdigest(),
                    scope="functional evidence, not throughput; each SDK invocation opens a fresh handle")

    def record(self, event, **values):
        with self.history_lock:
            self.seq += 1
            value = {"sequence": self.seq, "event": event, "monotonic_ns": time.monotonic_ns(), **typed(values)}
            self.events.write(json.dumps(value, sort_keys=True, ensure_ascii=True) + "\n")
            self.events.flush()
            return self.seq

    def observe(self, event, fn, **values):
        start = time.monotonic_ns()
        invocation = self.record(event + "_invoke", start_ns=start, depends_on_ack=self.ack, **values)
        try:
            result = fn()
        except Exception as error:
            errno = error.args[0] if error.args and isinstance(error.args[0], int) else None
            self.record(event, start_ns=start, end_ns=time.monotonic_ns(), depends_on_ack=self.ack,
                        invocation_sequence=invocation, error={"type": type(error).__name__, "args": list(error.args),
                                                               "errno": errno, "sqlstate": getattr(error, "sqlstate", None)})
            raise
        seq = self.record(event, start_ns=start, end_ns=time.monotonic_ns(), depends_on_ack=self.ack,
                          invocation_sequence=invocation, result=result)
        return result, seq

    def connect(self, database=True):
        host, port = self.gateways[0].rsplit(":", 1)
        return pymysql.connect(host=host, port=int(port), user=self.args.user, password="",
                               database=self.db if database else None, autocommit=True,
                               charset="utf8mb4", connect_timeout=3, read_timeout=15, write_timeout=5)

    def sql(self, c, statement, params=None, mutation=False):
        result, seq = self.observe("sql", lambda: q(c, statement, params), statement=statement, params=params)
        if mutation:
            self.ack = seq
        return result

    def check(self, label, condition, **values):
        self.record("check", label=label, success=bool(condition), **values)
        assert condition, (label, values)
        self.completed += 1
        print("  ok  " + label, flush=True)

    def public(self, node, name, request):
        request_file = Path(self.private.name) / ("request-" + uuid.uuid4().hex + ".json")
        request_file.write_text(json.dumps(wire(request), ensure_ascii=True) + "\n")
        os.chmod(request_file, 0o600)
        command = [os.environ.get("TARANTOOL", "tarantool"), str(Path(__file__).with_name("kvrpc.lua")),
                   node, str(self.credentials), "tarn_kv_v1." + name, str(request_file)]
        def run():
            result = subprocess.run(command, capture_output=True, text=True, timeout=12, stdin=subprocess.DEVNULL)
            lines = result.stdout.strip().splitlines()
            if not lines:
                raise RuntimeError("KV bridge produced no JSON", result.returncode, result.stderr[-2000:])
            reply = integers(json.loads(lines[-1]))
            if result.returncode:
                raise RuntimeError("KV bridge transport failure", reply, result.stderr[-2000:])
            return reply
        try:
            return self.observe("kv_rpc", run, node=node, procedure=name, request=request)[0]
        finally:
            request_file.unlink(missing_ok=True)

    def sdk(self, table, keys):
        command = [self.args.sdk, "--seeds-file", str(self.seeds), "--database", self.db,
                   "--table", table, "--keys", json.dumps(typed(keys)), "--credentials-file", str(self.credentials)]
        def run():
            result = subprocess.run(command, capture_output=True, text=True, timeout=12, stdin=subprocess.DEVNULL)
            lines = result.stdout.strip().splitlines()
            if not lines:
                raise RuntimeError("SDK produced no JSON", result.returncode, result.stderr[-2000:])
            reply = json.loads(lines[-1])
            assert result.returncode == (0 if reply.get("status") == "ok" else 1), reply
            return reply
        return self.observe("rust_sdk", run, table=table, keys=keys)[0]

    def grant(self, table):
        for node in self.nodes:
            result = ev(node, """
local user,name=...; assert(box.schema.user.exists(user)); assert(box.space[name])
box.schema.user.grant(user,'read','space',name,{if_not_exists=true}); return true
""", [self.user, self.db + "." + table])
            self.record("admin_table_grant", node=node, table=table, result=result)

    def setup(self):
        for node in self.nodes:
            self.identities.append(ev(node, """
assert(not box.info.ro,'writable shard leader required')
return {id=tarn.cfg.shard_id,uuid=tostring(box.info.uuid),version=box.info.version,native=tarn_info().native}
"""))
        assert len(set(x["id"] for x in self.identities)) == len(self.nodes)
        self.seeds.write_text(json.dumps([{"id": identity["id"], "replicas": group}
                                         for group, identity in zip(self.args.replica_groups, self.identities)]) + "\n")
        os.chmod(self.seeds, 0o600)
        self.record("storage_identity", nodes=self.nodes, identities=self.identities,
                    replica_groups=self.args.replica_groups)
        with self.connect(False) as c:
            self.database_attempted = True
            self.sql(c, "CREATE DATABASE " + self.db, mutation=True)
            self.database_ack = True
            self.created = True
        for node in self.nodes:
            self.user_attempts.append(node)
            result = ev(node, """
local user,password=...; assert(not box.schema.user.exists(user),'reader name already exists')
box.schema.user.create(user,{password=password})
for _,name in ipairs({'tarn_kv_v1.meta','tarn_kv_v1.get','tarn_kv_v1.get_many'}) do
 assert(box.func[name]); box.schema.user.grant(user,'execute','function',name)
end
return {uid=assert(box.space._user.index.name:get{user}).id}
""", [self.user, self.password], stage="create_reader", sensitive=(self.password,))
            assert isinstance(result, dict) and isinstance(result.get("uid"), int), result
            self.users.append((node, result["uid"]))  # Cleanup only a definite creation ACK.
            self.record("admin_reader_create", node=node, result=result)

    def paired(self, c, table, keys, columns, where, sql_keys=None):
        expected = []
        names = [x.strip() for x in columns.split(",")]
        sql_keys = keys if sql_keys is None else sql_keys
        assert len(sql_keys) == len(keys), (keys, sql_keys)
        for key in sql_keys:
            rows = self.sql(c, "SELECT " + columns + " FROM " + table + " WHERE " + where, tuple(key))
            assert len(rows) <= 1, rows
            if rows:
                expected.append(dict(zip(names, typed(rows[0]))))
            else:
                expected.append(None)
        actual = self.sdk(table, keys)
        self.check(table + " SDK rows equal SQL", actual.get("status") == "ok" and actual["rows"] == expected,
                   expected=expected, actual=actual)
        self.check(table + " schema accompanies rows", actual["schema"]["db"] == self.db
                   and actual["schema"]["name"] == table
                   and [x["name"] for x in actual["schema"]["columns"]] == names)

    def functional(self, c, engine):
        table = "points_" + engine
        self.sql(c, f"CREATE TABLE {table} (tenant BIGINT NOT NULL,id BIGINT NOT NULL,v VARCHAR(50),n INT,"
                 f"PRIMARY KEY(tenant,id)) PARTITION BY KEY(tenant) ENGINE={engine}", mutation=True)
        self.sql(c, f"INSERT INTO {table} VALUES (1,1,'one',NULL),(1,2,NULL,22),(2,3,'three',33)", mutation=True)
        self.grant(table)
        self.paired(c, table, [[1, 1], [1, 999], [1, 2], [1, 1], [2, 3]], "tenant,id,v,n", "tenant=%s AND id=%s")
        node = next(node for node in self.nodes if ev(node, "return box.space[...]:get{1,1}~=nil", [self.db + "." + table]))
        meta = self.public(node, "meta", {"version": 1, "db": self.db, "name": table})
        self.check(engine + " registered metadata", meta.get("status") == "ok")
        request = {"version": 1, "consistency": "independent_points", "table": meta["table"],
                   "ownership": meta["ownership"], "keys": [[1, 1], [1, 999], [1, 2], [1, 1]]}
        reply = self.public(node, "get_many", request)
        self.check(engine + " RPC duplicate/miss positions", reply.get("status") == "ok"
                   and [s["status"] for s in reply["slots"]] == ["present", "missing", "present", "present"]
                   and [s["position"] for s in reply["slots"]] == [1, 2, 3, 4]
                   and [s["key"] for s in reply["slots"]] == request["keys"]
                   and reply["slots"][2]["tuple"][2] is None, reply=reply)
        for label, keys, kind in [("prefix", [[1]], "bad_request"), ("wrong_type", [[True, 1]], "bad_request"),
                                  ("nested", [[[1], 1]], "bad_request"),
                                  ("count_cap", [[1, 1]] * 129, "limit_exceeded"),
                                  ("bytes_cap", [["x" * 65537, 1]], "limit_exceeded")]:
            bad = copy.deepcopy(request); bad["keys"] = keys
            result = self.public(node, "get_many", bad)
            self.check(engine + " " + label, result.get("status") == "error" and "slots" not in result
                       and result["error"]["kind"] == kind and result["error"]["domain"] == "contract", reply=result)
        bad = copy.deepcopy(request); bad["ownership"]["bucket_epoch"] += 1
        result = self.public(node, "get_many", bad)
        self.check(engine + " stale owner explicit", result.get("status") == "error"
                   and result["error"]["kind"] == "moved" and "slots" not in result, reply=result)
        renamed = "changed_" + engine
        self.sql(c, f"ALTER TABLE {table} CHANGE COLUMN v payload VARCHAR(50) AFTER n,"
                 " ADD COLUMN extra INT NOT NULL DEFAULT 7 FIRST", mutation=True)
        self.sql(c, f"ALTER TABLE {table} RENAME TO {renamed}", mutation=True)
        result = self.public(node, "get_many", request)
        self.check(engine + " old frozen token terminal", result.get("status") == "error"
                   and result["error"]["kind"] == "schema_changed" and result["error"]["code"] == 1412
                   and "slots" not in result, reply=result)
        self.grant(renamed)
        self.paired(c, renamed, [[1, 1], [1, 2], [1, 999]], "extra,tenant,id,n,payload", "tenant=%s AND id=%s")
        edge = "edges_" + engine
        self.sql(c, f"CREATE TABLE {edge} (id BIGINT PRIMARY KEY,v VARCHAR(30)) ENGINE={engine}", mutation=True)
        denied = self.public(self.nodes[0], "meta", {"version": 1, "db": self.db, "name": edge})
        self.check(engine + " table READ grant required", denied.get("status") == "error"
                   and denied["error"]["kind"] == "access_denied" and denied["error"]["domain"] == "tarantool"
                   and denied["error"]["code"] > 0 and "definition_json" not in denied, reply=denied)
        self.sql(c, f"INSERT INTO {edge} VALUES (%s,'low'),(%s,'high')", (-(2**63), 2**63 - 1), mutation=True)
        self.grant(edge)
        self.paired(c, edge, [[-(2**63)], [2**63 - 1], [-7]], "id,v", "id=%s")
        unsigned = "unsigned_" + engine
        self.sql(c, f"CREATE TABLE {unsigned} (id BIGINT UNSIGNED PRIMARY KEY,v VARCHAR(30)) ENGINE={engine}", mutation=True)
        self.sql(c, f"INSERT INTO {unsigned} VALUES (%s,'max')", (2**64 - 1,), mutation=True)
        self.grant(unsigned)
        self.paired(c, unsigned, [[2**64 - 1], [0]], "id,v", "id=%s")
        ci = "collated_" + engine
        self.sql(c, f"CREATE TABLE {ci} (tenant INT,id VARCHAR(32) COLLATE utf8mb4_general_ci,v VARBINARY(32),"
                 f"PRIMARY KEY(tenant,id)) PARTITION BY KEY(tenant) ENGINE={engine}", mutation=True)
        self.sql(c, f"INSERT INTO {ci} VALUES (1,'AbC',X'00ff'),(2,'É',NULL)", mutation=True)
        self.grant(ci)
        self.paired(c, ci, [[1, "abc"], [2, "e"], [1, "absent"]], "tenant,id,v", "tenant=%s AND id=%s")
        binary = "binary_" + engine
        self.sql(c, f"CREATE TABLE {binary} (id VARBINARY(16) PRIMARY KEY,v VARCHAR(30)) ENGINE={engine}", mutation=True)
        self.sql(c, f"INSERT INTO {binary} VALUES (X'00ff','bytes')", mutation=True)
        self.grant(binary)
        # COM_QUERY is UTF-8 SQL text. PyMySQL 1.1.2 embeds bytes as raw
        # quoted text; use exact MySQL hex literals for the binary oracle.
        binary_keys = [[b"\x00\xff"], [b"missing"]]
        self.paired(c, binary, binary_keys, "id,v", "id=X%s",
                    sql_keys=[[key[0].hex()] for key in binary_keys])
        # Use a separate session so the checked profile does not change the
        # legacy/refusal controls that follow. The SDK shares SQL key coercion
        # and row decoding, including native decimal values above 2^53.
        exact_table = "exact_keys_" + engine
        with self.connect() as exact:
            self.sql(exact, "SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin")
            self.sql(exact, "SET tarn_key_contract='MYSQL'")
            self.sql(exact, f"CREATE TABLE {exact_table} (tenant BIGINT UNSIGNED,id VARCHAR(32) COLLATE utf8mb4_0900_bin,"
                     "d DECIMAL(38,2),b VARBINARY(8),PRIMARY KEY(tenant,id)) "
                     f"PARTITION BY KEY(tenant) ENGINE={engine}", mutation=True)
            vectors = [(2**64 - 1, "a", "9007199254740992.01"),
                       (2**64 - 1, "a ", "9007199254740992.02"),
                       (2**53 + 1, "\u00e9", "18446744073709551615.00"),
                       (2**53 + 1, "e\u0301", "-9223372036854775808.00")]
            for tenant, key, value in vectors:
                self.sql(exact, f"INSERT INTO {exact_table} VALUES (%s,%s,%s,X'00ff')",
                         (tenant, key, decimal.Decimal(value)), mutation=True)
            self.grant(exact_table)
            exact_keys = [[tenant, key] for tenant, key, _ in vectors]
            exact_keys += [exact_keys[0], [2**64 - 1, "missing"]]
            self.paired(exact, exact_table, exact_keys, "tenant,id,d,b", "tenant=%s AND id=%s")
        cap = "reply_cap_" + engine
        self.sql(c, f"CREATE TABLE {cap} (tenant INT,id INT,v LONGBLOB,PRIMARY KEY(tenant,id)) "
                 f"PARTITION BY KEY(tenant) ENGINE={engine}", mutation=True)
        # Each stored row fits the engine's default 1 MiB tuple limit.
        # The shared tenant routes both keys to one server-side batch.
        # Keep the default SQL parser/query budgets: a megabyte of escaped
        # text is unnecessary to construct these two exact stored values.
        for row_id, letter in ((1, 'x'), (2, 'y')):
            self.sql(c, f"INSERT INTO {cap} VALUES (1,%s,REPEAT(%s,%s))",
                     (row_id, letter, 600 * 1024), mutation=True)
        lengths = self.sql(c, f"SELECT id,OCTET_LENGTH(v) FROM {cap} WHERE tenant=1 ORDER BY id")
        assert lengths == ((1, 600 * 1024), (2, 600 * 1024)), lengths
        self.grant(cap)
        oversized = self.sdk(cap, [[1, 1], [1, 2]])
        self.check(engine + " final tuple crosses whole batch cap", oversized.get("status") == "error"
                   and oversized["error"]["kind"] == "limit_exceeded" and "rows" not in oversized, reply=oversized)

    def cleanup(self, success):
        errors = []
        live = [t.name for t in self.workers if t.is_alive()]
        if live:
            success = False
            errors.append("owned read workers remain live: " + repr(live))
        if success and self.created:
            try:
                with self.connect(False) as c:
                    self.sql(c, "DROP DATABASE " + self.db, mutation=True)
                self.created = False
            except Exception as error:
                errors.append(str(error))
        for node, uid in self.users:
            try:
                result = ev(node, """
local name,uid=...; local current=box.space._user.index.name:get{name}
if current then assert(current.id==uid,'reader cleanup identity changed'); box.schema.user.drop(name) end
return {absent=box.space._user.index.name:get{name}==nil,uid=uid}
""", [self.user, uid])
                assert result.get("absent") and result.get("uid") == uid, result
                self.record("admin_reader_drop", node=node, result=result)
            except Exception as error:
                errors.append(str(error))
        try:
            self.private.cleanup()
        except Exception as error:
            errors.append("private credential cleanup: " + str(error))
        uncertain_users = [node for node in self.user_attempts if node not in {n for n, _ in self.users}]
        self.record("cleanup", database_preserved=self.created,
                    database_possibly_preserved=self.created or self.database_attempted and not self.database_ack,
                    uncertain_reader_attempts_preserved=uncertain_users, success=not errors, errors=errors)
        return errors

    def pending_apply(self, c, engine):
        assert len(self.nodes) == 2, "pending APPLY requires exactly two logical shard leaders"
        table = "pending_" + engine
        self.space = self.db + "." + table  # Existing ApplyLease inspects only this owned table.
        self.sql(c, f"CREATE TABLE {table} (id INT PRIMARY KEY,k INT,v INT) ENGINE={engine}", mutation=True)
        self.grant(table)
        ranges = self.sql(c, "SHOW TARN BUCKETS")
        self.bucket_count = sum(int(r[1]) for r in ranges)
        owned = {}
        for row in ranges:
            ids = owned.setdefault(int(row[0]), set())
            for part in str(row[2]).split(", ") if row[2] else []:
                lo, _, hi = part.partition("-")
                ids.update(range(int(lo), int(hi or lo) + 1))
        keys = [next(i for i in range(1, 10000) if bucket_of(i, self.bucket_count) in owned[identity["id"]])
                for identity in self.identities]
        meta = self.public(self.nodes[1], "meta", {"version": 1, "db": self.db, "name": table})
        assert meta.get("status") == "ok", meta
        request = {"version": 1, "consistency": "independent_points", "table": meta["table"],
                   "ownership": meta["ownership"], "key": [keys[1]]}
        lease = ApplyLease(self, keys[1])
        entered = False
        try:
            lease.__enter__()
            entered = True
            self.sql(c, "BEGIN")
            for key in keys:
                self.sql(c, f"INSERT INTO {table} VALUES (%s,9,42)", (key,))
            self.sql(c, "COMMIT", mutation=True)  # Exactly once. Unknown is never reclassified/replayed.
            lease.known_committed = True
            ack_sequence = self.ack
            state = lease.snapshot()
            self.check(engine + " SQL ACK has pending physical APPLY", state["prepared"]
                       and state["state"] == "prepared" and state["row"] is None and bool(state["txid"]), state=state)
            txid = state["txid"]
            decision = decision_node(self.nodes, self.identities, state)
            decision_status = rpc(decision, "tarn_txn_status", [txid])
            parts = [rpc(node, "tarn_part_status", [txid]) for node in self.nodes]
            self.check(engine + " ACK decision and participant witnesses", decision_status in ("staging", "committed")
                       and all(value in ("prepared", "applied") for value in parts),
                       txid=txid, decision_node=decision, decision_status=decision_status, participant_statuses=parts,
                       sql_ack_sequence=ack_sequence)
            raw = ev(self.nodes[1], "local name,key=...; local t=box.space[name]:get{key}; return t and t:totable() or box.NULL",
                     [self.space, keys[1]])
            self.record("raw_native_after_ack", txid=txid, sql_ack_sequence=ack_sequence, row=raw,
                        scope="weaker raw-native visibility; no Tarn claim/freshness barrier")
            self.check(engine + " raw absence is retained separately", raw is None)

            def sql_point():
                with self.connect() as reader:
                    return self.sql(reader, f"SELECT id,k,v FROM {table} WHERE id=%s", (keys[1],))

            outcomes = {}
            outcomes_lock = threading.Lock()
            def worker(name, fn):
                try:
                    outcome = {"result": fn()}
                except Exception as error:
                    outcome = {"error_type": type(error).__name__, "error_args": list(error.args)}
                with outcomes_lock:
                    outcomes[name] = outcome
            calls = [("sql", sql_point), ("sdk", lambda: self.sdk(table, [[keys[1]]])),
                     ("rpc", lambda: self.public(self.nodes[1], "get", request))]
            current = []
            for name, fn in calls:
                thread = threading.Thread(target=worker, args=(name, fn),
                                          name="kv181-pending-" + engine + "-" + name, daemon=True)
                self.workers.append(thread); current.append(thread); thread.start()
            deadline = time.monotonic() + 16
            for thread in current:
                thread.join(max(0, deadline - time.monotonic()))
            with outcomes_lock:
                observed = dict(outcomes)
            self.check(engine + " held reads finish within their bounded refusal budgets",
                       not any(t.is_alive() for t in current), outcomes=observed)
            still = lease.snapshot()
            self.check(engine + " physical APPLY remains held during refusals", still["txid"] == txid
                       and still["state"] == "prepared" and still["row"] is None, state=still)
            for name, _ in calls:
                outcome = observed[name]
                if name == "sql":
                    args = outcome.get("error_args", [])
                    refused = len(args) >= 2 and args[0] == 1213 and txid in str(args[1])
                else:
                    result = outcome.get("result", {})
                    error = result.get("error", {})
                    refused = (result.get("status") == "error" and "rows" not in result and "slots" not in result
                               and error.get("kind") == "unavailable" and error.get("domain") == "mysql"
                               and error.get("code") == 1213 and txid in error.get("message", ""))
                self.check(engine + " " + name + " refuses captured W claim after ACK", refused,
                           txid=txid, sql_ack_sequence=ack_sequence, outcome=outcome)
            self.record("owned_apply_release", txid=txid, sql_ack_sequence=ack_sequence)
            lease.release()
        finally:
            active_error = sys.exc_info()[0] is not None
            if entered:
                try:
                    lease.close()  # Only captured txid, and only after definite ACK, can APPLY.
                except Exception:
                    self.record("apply_cleanup_error", traceback=traceback.format_exc())
                    if not active_error:
                        raise
        self.paired(c, table, [[keys[0]], [keys[1]]], "id,k,v", "id=%s")
        after = self.public(self.nodes[1], "get", request)
        self.check(engine + " registered point after physical APPLY", after.get("status") == "ok"
                   and len(after["slots"]) == 1 and after["slots"][0]["status"] == "present"
                   and after["slots"][0]["tuple"][:3] == [keys[1], 9, 42], reply=after)
        self.pending_completed.append(engine)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tarn", required=True, help="SQL gateways, comma-separated")
    parser.add_argument("--shards", required=True, help="writable leaders in gateway participant order")
    parser.add_argument("--sdk", required=True, help="compiled strong_kv Rust example")
    parser.add_argument("--evidence", required=True, help="new private directory")
    parser.add_argument("--user", default="sb")
    parser.add_argument("--replicas", help="JSON list of replica groups in --shards order; default [[leader],...]")
    args = parser.parse_args()
    if not Path(args.sdk).is_file():
        parser.error("--sdk must name an existing compiled executable")
    leaders = args.shards.split(",")
    if len(leaders) != 2 or any(not node for node in leaders) or len(set(leaders)) != 2:
        parser.error("--shards must name exactly two distinct logical shard leaders")
    try:
        groups = json.loads(args.replicas) if args.replicas is not None else [[node] for node in leaders]
    except (TypeError, ValueError):
        parser.error("--replicas must be a JSON list of replica groups")
    if not isinstance(groups, list) or len(groups) != len(leaders):
        parser.error("--replicas must contain one group per --shards leader")
    for node, group in zip(leaders, groups):
        if (not isinstance(group, list) or not 1 <= len(group) <= 32
                or any(not isinstance(address, str) or not address or '@' in address
                       or any(c.isspace() for c in address) for address in group)
                or len(set(group)) != len(group) or node not in group):
            parser.error("each replica group must contain its leader and 1..32 distinct credential-free addresses")
    if len({address for group in groups for address in group}) != sum(map(len, groups)):
        parser.error("replica groups must not share endpoints")
    args.replica_groups = groups
    test = Test(args)
    success, failure = False, None
    try:
        test.setup()
        with test.connect() as connection:
            for engine in ("memtx", "vinyl"):
                test.functional(connection, engine)
                test.pending_apply(connection, engine)
        success = True
    except Exception:
        failure = traceback.format_exc()
        test.record("failure", traceback=failure)
        print(failure, file=sys.stderr)
    finally:
        cleanup = test.cleanup(success)
        summary = {"completed_checks": test.completed, "success": success and not cleanup,
                   "primary_error": failure, "cleanup_errors": cleanup, "database_preserved": test.created,
                   "database_possibly_preserved": test.created or test.database_attempted and not test.database_ack,
                   "uncertain_reader_attempts_preserved": [n for n in test.user_attempts if n not in {x for x, _ in test.users}],
                   "pending_apply_completed_engines": test.pending_completed,
                   "live_read_workers": [t.name for t in test.workers if t.is_alive()]}
        (test.evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        test.events.close()
    print(f"{test.completed} checks, {'PASS' if summary['success'] else 'FAIL'}")
    return 0 if summary["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
