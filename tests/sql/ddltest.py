#!/usr/bin/env python3
"""Schema changes and dump loading (issue #16): MySQL dumps, multi-statement
batches, ALTER TABLE that Tarn refuses, and ALTER TABLE while transactions
run on several SQL servers -- no acknowledged write may be lost, and no
autocommit statement may fail because a table definition changed.

    tests/sql/ddltest.py --tarn 127.0.0.1:4000[,127.0.0.1:4001] [--seconds 20] [--threads 8]
                       [--kv 127.0.0.1:3311,127.0.0.1:3321]

With two SQL servers (scripts/cluster.sh start -s 2) the online test runs
writers on both while the schema changes go through the first: the second
server sees each new definition only after it reloads its catalog.
Supply one writable leader per shard, in gateway shard order, for the
engine-change tests' independent native images and legacy counter RPCs.
"""
import argparse
import os
import random
import shutil
import subprocess
import sys
import threading
import time
import traceback

import pymysql
from pymysql.constants import CLIENT, COMMAND

DB = "ddltest"
HERE = os.path.dirname(os.path.abspath(__file__))
RETRY = (1213, 1412, 1205)
UNKNOWN = 1180


def connect(hp, db=DB, autocommit=True, multi=False, read_timeout=None):
    host, port = hp.split(":")
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db,
                           autocommit=autocommit, charset="utf8mb4",
                           read_timeout=read_timeout,
                           client_flag=CLIENT.MULTI_STATEMENTS if multi else 0)


def q(c, sql, args=None):
    cur = c.cursor()
    cur.execute(sql, args)
    rows = cur.fetchall() if cur.description else cur.rowcount
    cur.close()
    return rows


def code(fn):
    try:
        fn()
    except pymysql.MySQLError as e:
        return e.args[0]
    return None


ENGINE_IMAGE = r"""
local name = ...
box.begin{txn_isolation='read-confirmed'}
local ok, result = pcall(function()
local sp = assert(box.space[name], 'missing native table ' .. name)
local cat = assert(box.space._tarn_catalog:get{name}, 'missing native catalog ' .. name)
local function hex(s)
    return (s:gsub('.', function(c) return string.format('%02x', string.byte(c)) end))
end
local function image(index)
    local out = {}
    for _, row in index:pairs() do
        out[#out + 1] = hex(require('msgpack').encode(row:totable()))
    end
    table.sort(out)
    return out
end
local indexes = {}
for id, ix in pairs(sp.index) do
    if type(id) == 'number' then
        indexes[#indexes + 1] = {id=ix.id, name=ix.name, type=ix.type, unique=ix.unique, parts=ix.parts}
    end
end
table.sort(indexes, function(a, b) return a.id < b.id end)
local keys, kd = {}, require('key_def').new(sp.index[0].parts)
for _, row in sp:pairs() do keys[#keys + 1] = kd:extract_key(row):totable() end
local aux = {}
for _, u in ipairs(cat.def.uix or {}) do aux[#aux + 1] = u.id end
for _, l in ipairs(cat.def.lazy or {}) do aux[#aux + 1] = l.gen end
table.sort(aux)
return {id=sp.id, name=sp.name, engine=sp.engine, ver=cat.def.ver, format=sp:format(),
        indexes=indexes, rows=image(sp.index[0]), buckets=image(assert(sp.index.bucket_id)),
        keys=keys, aux=aux}
end)
if box.is_in_txn() then box.rollback() end
if not ok then error(result) end
return result
"""


class T:
    def __init__(self, args):
        self.args = args
        self.servers = args.tarn.split(",")
        self.failures = 0

    def check(self, cond, what):
        if not cond:
            self.failures += 1
            print(f"    FAILED: {what}")
        return cond

    def c(self, i=0, **kw):
        return connect(self.servers[i % len(self.servers)], **kw)

    def engine_image(self, table):
        """Confirmed native value/arity images, independent of SQL's decoder.

        MessagePack reencodes the complete tuple fields, including missing
        suffixes, NULLs and binary weights; it does not assert preservation
        of the original numeric MessagePack encoding.
        """
        if not self.args.kv:
            return []
        from lazytest import ev
        images = [ev(node, ENGINE_IMAGE, [f"{DB}.{table}"]) for node in self.args.kv.split(",")]
        for image in images:
            self.check(image["name"] == f"{DB}.{table}", "exact public native name")
            self.check(image["rows"] == image["buckets"], "native bucket and primary scans contain exactly the same tuples")
        return images

    def change_engine(self, c, table, engine):
        q(c, f"ALTER TABLE `{table}` ENGINE={engine}")
        for server in range(len(self.servers)):
            with self.c(server) as probe:
                deadline = time.monotonic() + 10
                status = []
                while time.monotonic() < deadline:
                    # #142: the existing admin lexer corrupts quoted UTF-8
                    # LIKE tokens. Keep the Unicode table and compare the
                    # actual unfiltered catalog name, without that parser.
                    status = [row for row in q(probe, "SHOW TABLE STATUS") if row[0] == table]
                    if len(status) == 1 and status[0][1].lower() == engine:
                        break
                    time.sleep(.01)
                self.check(len(status) == 1 and status[0][1].lower() == engine,
                           f"gateway {server}: {table} reports {engine}: {status}")

    def engine_noop(self, c, table, engine):
        before = self.engine_image(table)
        # These internal test names are valid unquoted identifiers. The
        # full schema oracle avoids #142's quoted UTF-8 admin token bug.
        create = q(c, f"SHOW CREATE TABLE {table}")
        self.change_engine(c, table, engine)
        self.check(q(c, f"SHOW CREATE TABLE {table}") == create, "same-engine ALTER preserves the SQL definition")
        self.check(self.engine_image(table) == before,
                   "same-engine ALTER preserves native IDs, version, indexes, auxiliaries and physical rows")

    def legacy_counter(self, table, prefix, allocated):
        """The exact old gateway RPC names must alias the stable counter."""
        from lazytest import rpc
        node = self.args.kv.split(",")[0]
        sid = self.engine_image(table)[0]["id"]
        start = rpc(node, "tarn_counter_alloc", [f"{prefix}:{sid}", 7])
        maximum = max(key[0] if isinstance(key, tuple) else key for key in allocated)
        self.check(isinstance(start, int) and start > maximum,
                   f"legacy {prefix}:new-SID allocation shares the original high-water mark")
        floor = maximum + 1_000_000
        self.check(rpc(node, "tarn_counter_bump", [f"{prefix}:{sid}", floor]) is True,
                   f"legacy {prefix}:new-SID bump is accepted")
        return start, floor

    # ------------------------------------------------------------------
    def test_engine_change_fidelity(self):
        """Engine replacement preserves physical layouts and nonconsecutive index IDs."""
        c = self.c()
        table = "ec_雪"
        q(c, f"DROP TABLE IF EXISTS `{table}`")
        q(c, f"CREATE TABLE `{table}` (tenant INT NOT NULL,id INT NOT NULL,v INT,old VARCHAR(20),"
             "name VARCHAR(20) COLLATE utf8mb4_unicode_ci,payload VARCHAR(64),"
             "PRIMARY KEY(tenant,id),KEY discard(v),KEY by_name(name)) PARTITION BY KEY(tenant) ENGINE=memtx")
        raw = "\0\n\r\t\\'雪"
        q(c, f"INSERT INTO `{table}` VALUES (1,1,10,'kept physically','Á ',%s),(2,2,20,NULL,'ß',NULL)", (raw,))
        q(c, f"ALTER TABLE `{table}` ADD COLUMN frozen INT DEFAULT 3")
        q(c, f"ALTER TABLE `{table}` ALTER COLUMN frozen SET DEFAULT NULL")
        q(c, f"INSERT INTO `{table}` (tenant,id,v,name,payload) VALUES (3,3,30,'三','third')")
        q(c, f"ALTER TABLE `{table}` ALTER COLUMN frozen SET DEFAULT 9")
        q(c, f"ALTER TABLE `{table}` DROP COLUMN old, DROP INDEX discard")
        q(c, f"ALTER TABLE `{table}` ADD INDEX by_v(v,tenant,id)")
        select = f"SELECT tenant,id,v,name,payload,frozen FROM `{table}` ORDER BY tenant,id"
        expected = ((1, 1, 10, "Á ", raw, 3), (2, 2, 20, "ß", None, 3), (3, 3, 30, "三", "third", None))
        self.check(q(c, select) == expected, "fidelity source has frozen fills, present NULLs and exact raw text")
        original = self.engine_image(table)
        for image in original:
            ids = [index["id"] for index in image["indexes"]]
            self.check(ids != list(range(max(ids) + 1)), f"source really has native index-ID gaps: {ids}")
        for engine in ("vinyl", "memtx"):
            previous = self.engine_image(table)
            self.change_engine(c, table, engine)
            self.check(q(c, select) == expected, f"{engine}: exact logical rows and frozen defaults")
            self.check(q(c, f"SELECT HEX(payload) FROM `{table}` WHERE tenant=1 AND id=1") == ((raw.encode("utf8").hex().upper(),),),
                       f"{engine}: raw text bytes survive exactly")
            self.check(q(c, f"SELECT id FROM `{table}` FORCE INDEX(by_name) WHERE name='a' ORDER BY id") == ((1,),),
                       f"{engine}: CI index preserves raw spelling and weight semantics")
            self.check(q(c, f"SELECT tenant,id FROM `{table}` FORCE INDEX(by_v) WHERE v>=20 ORDER BY v") == ((2, 2), (3, 3)),
                       f"{engine}: replacement secondary uses its retained native ID")
            self.check(q(c, f"SELECT id FROM `{table}` FORCE INDEX(PRIMARY) WHERE tenant=1 AND id=1") == ((1,),),
                       f"{engine}: native primary remains index zero")
            after = self.engine_image(table)
            for old, new, source in zip(previous, after, original):
                self.check(new["id"] != old["id"] and new["engine"] == engine,
                           f"{engine}: a new actual physical base was published")
                for field in ("name", "format", "indexes", "rows", "buckets", "keys"):
                    self.check(new[field] == source[field], f"{engine}: exact native {field} preserved")
            self.engine_noop(c, table, engine)
        q(c, f"INSERT INTO `{table}` (tenant,id,v) VALUES (4,4,40)")
        self.check(q(c, f"SELECT frozen FROM `{table}` WHERE tenant=4 AND id=4") == ((9,),),
                   "migration preserves the current default separately from old frozen fills")
        before = self.engine_image(table)
        self.check(code(lambda: q(c, f"ALTER TABLE `{table}` ENGINE=vinyl, ADD COLUMN refused INT")) == 1235,
                   "combined engine and schema change is explicitly refused")
        self.check(self.engine_image(table) == before and "`refused`" not in q(c, f"SHOW CREATE TABLE {table}")[0][1],
                   "refused combined ALTER has no schema or row effects")
        q(c, f"DROP TABLE `{table}`")

    def test_engine_change_counters(self):
        """Deleted high IDs and gateway-reserved blocks survive repeated replacements."""
        a, b = self.c(0), self.c(1)
        q(a, "DROP TABLE IF EXISTS ec_ai")
        q(a, "CREATE TABLE ec_ai (id BIGINT PRIMARY KEY AUTO_INCREMENT,n INT) ENGINE=memtx")
        q(a, "INSERT INTO ec_ai VALUES (100000,-1)")
        q(a, "DELETE FROM ec_ai")
        issued = {100000}
        expected = {}
        serial = 0

        def insert(connection, count=1, minimum=100000):
            nonlocal serial
            values = list(range(serial, serial + count))
            serial += count
            q(connection, "INSERT INTO ec_ai(n) VALUES " + ",".join("(%s)" for _ in values), values)
            rows = q(connection, "SELECT id,n FROM ec_ai WHERE n>=%s AND n<%s ORDER BY n", (values[0], serial))
            self.check(len(rows) == count, "every generated insert is retained")
            for identity, n in rows:
                self.check(identity > minimum and identity not in issued,
                           f"generated ID {identity} never reuses a deleted or previously allocated value")
                issued.add(identity)
                expected[identity] = n

        insert(a)
        insert(b)
        for engine in ("vinyl", "memtx"):
            self.change_engine(a, "ec_ai", engine)
            if self.args.kv:
                start, floor = self.legacy_counter("ec_ai", "ai", issued)
                issued.update(range(start, start + 7))
                insert(a, 1001, floor)  # fresh SQL block sees the legacy RPC's bump
            insert(a)
            insert(b)
            insert(a, 1001)  # requires a new block, beyond either cached remainder
            self.engine_noop(a, "ec_ai", engine)
            self.check(q(b, "SELECT id,n FROM ec_ai ORDER BY id") == tuple(sorted(expected.items())),
                       f"{engine}: exact generated IDs across all gateway allocations")
        print(f"    stable AUTO_INCREMENT: {len(expected)} inserts through {len(set(self.servers))} gateway(s)")
        a.close()
        b.close()

    def test_engine_change_auxiliaries(self):
        """New auxiliary IDs preserve global uniqueness and a ready lazy query path."""
        from lazytest import ev, rpc, stat
        c = self.c()
        q(c, "DROP TABLE IF EXISTS ec_aux")
        q(c, "CREATE TABLE ec_aux (tenant INT NOT NULL,id INT NOT NULL,h VARCHAR(20) COLLATE utf8mb4_unicode_ci,"
             "k INT,n INT,v VARCHAR(20),PRIMARY KEY(tenant,id),UNIQUE KEY by_h(h),"
             "KEY by_k(k,n,id) COMMENT 'tarn:lazy') PARTITION BY KEY(tenant) ENGINE=memtx")
        q(c, "INSERT INTO ec_aux VALUES (1,1,'Á ',7,10,'one'),(2,2,'b',7,20,'two'),(3,3,NULL,NULL,30,'null')")
        q(c, "SET SESSION tarn_lazy_read_your_writes=ON,tarn_lazy_wait_timeout=5")
        expected = ((1, 1, "one"), (2, 2, "two"))
        nodes = self.args.kv.split(",") if self.args.kv else []
        duplicate_tenant = 9
        if nodes:
            from movetest import bucket_of
            images = self.engine_image("ec_aux")
            holder = next(i for i, image in enumerate(images) if [1, 1] in image["keys"])
            holder_shard = rpc(nodes[holder], "tarn_info", [])["shard_id"]
            ranges = q(c, "SHOW TARN BUCKETS")
            count = sum(int(row[1]) for row in ranges)
            other_buckets = set()
            for row in ranges:
                if int(row[0]) != holder_shard:
                    text = row[2]
                    for part in text.split(", ") if text else []:
                        low, _, high = part.partition("-")
                        other_buckets.update(range(int(low), int(high or low) + 1))
            duplicate_tenant = next(key for key in range(1, 10000) if bucket_of(key, count) in other_buckets)

        def counters():
            return [ev(node, "local out={} box.begin{txn_isolation='read-confirmed'} "
                       "for _,t in box.space._tarn_lazy_counter:pairs() do out[#out+1]=t:totable() end "
                       "box.rollback() return out") for node in nodes]

        def lazy_query():
            before = stat(c, "Tarn_lazy_reads")
            self.check(q(c, "SELECT tenant,id,v FROM ec_aux WHERE k=7 ORDER BY n,id") == expected,
                       "ready lazy generation returns the exact public rows")
            self.check(stat(c, "Tarn_lazy_reads") == before + 1, "query actually used the lazy path")

        lazy_query()
        for engine in ("vinyl", "memtx"):
            old = self.engine_image("ec_aux")
            sequence = counters()
            self.change_engine(c, "ec_aux", engine)
            lazy_query()
            self.check(counters() == sequence, "physical copy/bootstrap emits no new logical lazy sequence")
            q(c, "BEGIN")
            self.check(q(c, "SELECT tenant,id,v FROM ec_aux WHERE k=7 ORDER BY n,id") == expected,
                       "strong transaction agrees with the fresh lazy generation")
            q(c, "ROLLBACK")
            self.check(q(c, "SELECT tenant,id FROM ec_aux WHERE h='a'") == ((1, 1),),
                       "fresh global unique target preserves CI lookup")
            self.check(code(lambda: q(c, "INSERT INTO ec_aux VALUES (%s,9,'a',9,90,'duplicate')", (duplicate_tenant,))) == 1062,
                       "global duplicate on another shard remains refused across the replacement")
            self.check(q(c, "SELECT id FROM ec_aux WHERE tenant=%s AND id=9", (duplicate_tenant,)) == (),
                       "refused global duplicate has no base effect")
            after = self.engine_image("ec_aux")
            for previous, current in zip(old, after):
                self.check(len(current["aux"]) == len(previous["aux"]) == 2
                           and not set(current["aux"]) & set(previous["aux"]),
                           "both global and lazy target IDs are fresh")
                self.check(current["rows"] == previous["rows"], "auxiliary migration preserves exact native base tuples")
            self.engine_noop(c, "ec_aux", engine)
        q(c, "UPDATE ec_aux SET k=8,v='changed' WHERE tenant=1 AND id=1")
        self.check(q(c, "SELECT id,v FROM ec_aux WHERE k=8 ORDER BY n,id") == ((1, "changed"),),
                   "new source capture remains writable after both migrations")
        q(c, "DELETE FROM ec_aux WHERE tenant=1 AND id=1")
        q(c, "INSERT INTO ec_aux VALUES (%s,9,'a',8,90,'reused')", (duplicate_tenant,))
        actual = q(c, "SELECT tenant,id,v FROM ec_aux WHERE k=8 ORDER BY n,id")
        self.check(actual == ((duplicate_tenant, 9, "reused"),),
                   f"released global value and lazy DELETE/reinsert stay correct after migration: {actual}")
        c.close()

    def test_engine_change_hidden_row_ids(self):
        """A no-PK table retains issued hidden IDs even after high rows are deleted."""
        a, b = self.c(0), self.c(1)
        q(a, "DROP TABLE IF EXISTS ec_rid")
        q(a, "CREATE TABLE ec_rid (n INT NOT NULL,tag VARCHAR(20),KEY by_n(n)) ENGINE=memtx")
        q(a, "INSERT INTO ec_rid VALUES " + ",".join(f"({n},'old')" for n in range(1001)))
        q(b, "INSERT INTO ec_rid VALUES (1001,'high')")
        before = self.engine_image("ec_rid")
        issued = {tuple(key) for image in before for key in image["keys"]}
        q(a, "DELETE FROM ec_rid WHERE n>0")
        retained = self.engine_image("ec_rid")
        expected = [(0, "old")]
        for turn, engine in enumerate(("vinyl", "memtx", "vinyl", "memtx")):
            self.change_engine(a, "ec_rid", engine)
            current = self.engine_image("ec_rid")
            for old, new in zip(retained, current):
                self.check(set(old["rows"]).issubset(new["rows"]), "preserved rows retain exact native hidden IDs")
            if self.args.kv:
                start, floor = self.legacy_counter("ec_rid", "rid", issued)
                issued.update((identity,) for identity in range(start, start + 7))
                begin = 10000 + 2000 * turn
                q(a, "INSERT INTO ec_rid VALUES " + ",".join(f"({n},'legacy')" for n in range(begin, begin + 1001)))
                expected.extend((n, "legacy") for n in range(begin, begin + 1001))
                following = self.engine_image("ec_rid")
                prior = {tuple(key) for image in current for key in image["keys"]}
                fresh = {tuple(key) for image in following for key in image["keys"]} - prior
                self.check(len(fresh) == 1001 and all(key[0] > floor and key not in issued for key in fresh),
                           "fresh SQL hidden-ID block shares the legacy RPC's stable-origin bump")
                issued.update(fresh)
                current = following
            for worker, connection in enumerate((a, b)):
                n = 2000 + 2 * turn + worker
                q(connection, "INSERT INTO ec_rid VALUES (%s,'new')", (n,))
                expected.append((n, "new"))
            after = self.engine_image("ec_rid")
            previous = {tuple(key) for image in current for key in image["keys"]}
            now = {tuple(key) for image in after for key in image["keys"]}
            if after:
                self.check(len(now - previous) == 2 and not (now - previous) & issued,
                           "both gateways allocate fresh hidden IDs, including values deleted before migration")
                issued.update(now)
            self.check(q(b, "SELECT n,tag FROM ec_rid FORCE INDEX(by_n) WHERE n>=0 ORDER BY n") == tuple(sorted(expected)),
                       f"{engine}: every no-PK row is retained and indexed")
            self.engine_noop(a, "ec_rid", engine)
        a.close()
        b.close()

    def test_engine_change_online(self):
        """Every writer class commits while a real partial baseline is held."""
        if not self.args.kv:
            print("    copy-window/native audits require --kv and test_hooks; SQL fidelity cases still run")
            return
        from lazytest import ev, rpc
        nodes = self.args.kv.split(",")
        assert len(nodes) == 2, "copy-window test requires two dedicated writable shard leaders"
        c = self.c(read_timeout=30)
        q(c, "DROP TABLE IF EXISTS ec_online")
        q(c, "CREATE TABLE ec_online (tenant INT NOT NULL,id INT NOT NULL,n INT,tag VARCHAR(20),"
             "PRIMARY KEY(tenant,id)) PARTITION BY KEY(tenant) ENGINE=memtx")
        # An earlier movement suite may leave the first fixed tenant keys
        # on one shard. Bind the seed to the current confirmed bucket map,
        # then independently check the actual native rows below.
        from movetest import bucket_of
        maps = [rpc(node, "tarn_buckets", []) for node in nodes]
        count = maps[0][1]
        assert count > 0 and all(m[0] == "ok" and m[1] == count
                                 and all(note[1] == "sent" for note in m[4]) for m in maps), maps
        owners = {}
        for shard, m in enumerate(maps):
            for lo, hi in m[3]:
                for bucket in range(lo, hi + 1):
                    assert bucket not in owners, (bucket, maps)
                    owners[bucket] = shard
        assert len(owners) == count, "copy fixture requires a complete stable bucket map"
        for shard, m in enumerate(maps):
            assert all(owners[note[0]] != shard for note in m[4]), maps
        selected = [[], []]
        for tenant in range(1, count * 64 + 1):
            shard = owners[bucket_of(tenant, count)]
            if len(selected[shard]) < 8:
                selected[shard].append(tenant)
            if all(len(keys) == 8 for keys in selected):
                break
        assert all(len(keys) == 8 for keys in selected), ("no bounded two-shard tenant seed", selected)
        tenants = sorted(key for keys in selected for key in keys)
        print(f"    copy fixture tenants by current shard: {selected}")
        expected = {(tenant, identity): [0, "seed"] for tenant in tenants for identity in range(128)}
        for tenant in tenants:
            q(c, "INSERT INTO ec_online VALUES " + ",".join(f"({tenant},{identity},0,'seed')" for identity in range(128)))

        for engine in ("vinyl", "memtx"):
            source = self.engine_image("ec_online")
            assert all(len(image["keys"]) > 256 for image in source), "fixture needs multiple baseline pages on each shard"
            left, right = [[tuple(key) for key in image["keys"]] for image in source]
            keys = [(left[0],), (left[1],), (left[2], right[2]), (right[3],)]
            # Exercise the current native schema cache without a generic hook.
            # A held tarn_test_hook disables native writes globally, so the
            # held-window work below is not a native-exclusion witness.
            before_native = [rpc(node, "tarn_info", [])["native"] for node in nodes]
            if all(before_native):
                key = keys[0][0]
                bucket = ev(nodes[0], "local sid,key=... return box.space[sid].index[0]:get(key)[5]", [source[0]["id"], key])
                packet = [[["update", source[0]["id"], key, [["+", 3, 1]]]],
                          {"fence": [source[0]["id"], source[0].get("ver", 0)], "wb": [bucket]}]
                before_value = q(c, "SELECT n FROM ec_online WHERE tenant=%s AND id=%s", key)
                answer = rpc(nodes[0], "tarn_storage.ts_autocommit", packet)
                if source[0]["engine"] == "memtx":
                    self.check(answer[0] == "ok" and answer[1] == 1, "valid direct native packet commits on actual memtx")
                    if answer[0] == "ok":
                        expected[key][0] += 1
                else:
                    self.check(answer == ["lua"] and q(c, "SELECT n FROM ec_online WHERE tenant=%s AND id=%s", key) == before_value,
                               "same valid native packet hands actual vinyl back without mutation")
                after_native = [rpc(node, "tarn_info", [])["native"] for node in nodes]
                field = "writes" if source[0]["engine"] == "memtx" else "writes_to_lua"
                self.check(sum(info[field] for info in after_native) > sum(info[field] for info in before_native),
                           f"actual {source[0]['engine']} exercises the native eligibility/handback path")
            # The gateway deliberately skips the native RPC for known vinyl;
            # an ordinary SQL warmup therefore has no handback-counter claim.
            q(c, "UPDATE ec_online SET n=n+1 WHERE tenant=%s AND id=%s", keys[0][0])
            expected[keys[0][0]][0] += 1
            stop, lock = threading.Event(), threading.Lock()
            acknowledged = [0] * 4
            deltas, errors, ddl_errors = {}, [], []
            ddl_finished = threading.Event()
            workers = []
            ddl_connection = self.c(read_timeout=30)

            def alter():
                try:
                    q(ddl_connection, f"ALTER TABLE ec_online ENGINE={engine}")
                except Exception as error:  # retain the original error before cleanup
                    ddl_errors.append(str(error))
                finally:
                    ddl_finished.set()

            def writer(kind):
                connection = self.c(kind, read_timeout=15)
                last = ["connect", None]

                def execute(sql, args=None):
                    last[:] = [sql, args]
                    return q(connection, sql, args)

                try:
                    execute("SET SESSION innodb_lock_wait_timeout=5")
                    if kind == 3:
                        execute("SET SESSION transaction_isolation='READ-COMMITTED'")
                    while not stop.is_set():
                        try:
                            if kind:
                                execute("BEGIN")
                                for key in keys[kind]:
                                    suffix = " FOR UPDATE" if kind == 3 else ""
                                    execute("SELECT n FROM ec_online WHERE tenant=%s AND id=%s" + suffix, key)
                            for key in keys[kind]:
                                execute("UPDATE ec_online SET n=n+1 WHERE tenant=%s AND id=%s", key)
                            if kind:
                                execute("COMMIT")
                            with lock:
                                acknowledged[kind] += 1
                                for key in keys[kind]:
                                    deltas[key] = deltas.get(key, 0) + 1
                        except pymysql.MySQLError as error:
                            code(lambda: q(connection, "ROLLBACK"))
                            if kind and error.args[0] in RETRY:
                                continue  # only a known aborted transaction is replayed
                            with lock:
                                errors.append(f"writer {kind}, statement {last}: {error}")
                            return  # never replay a mutation with an unknown outcome
                except Exception as error:
                    with lock:
                        errors.append(f"writer {kind}: {error}")
                finally:
                    connection.close()

            ddl_thread = threading.Thread(target=alter, daemon=True)
            try:
                for node in nodes:
                    assert rpc(node, "tarn_test_hook", ["engine_copy", "hold"]) == ["ok"]
                ddl_thread.start()
                for node in nodes:
                    reached = rpc(node, "tarn_test_hook", ["engine_copy", "reached"])
                    assert reached[0] == "ok" and reached[1] > 0, (node, reached, ddl_errors)
                witnesses = []
                for node, image in zip(nodes, source):
                    state = ev(node, "local sid=... local r=assert(box.space._tarn_meta:get{'engine:'..sid}).value "
                               "local old,new=assert(box.space[r.old_id]),assert(box.space[r.new_id]) "
                               "return {old_id=old.id,new_id=new.id,phase=r.phase,old_name=old.name,"
                               "old_rows=old.index[0]:count(),new_rows=new.index[0]:count()}", [image["id"]])
                    self.check(state["old_id"] == image["id"] and state["old_name"] == f"{DB}.ec_online"
                               and state["new_id"] != image["id"] and state["phase"] == "copying"
                               and 0 < state["new_rows"] < state["old_rows"],
                               f"{engine}: real partial shadow while source remains authoritative: {state}")
                    witnesses.append((state["old_id"], state["new_id"], state["old_rows"], state["new_rows"]))
                print(f"    {engine}: native copy witnesses (old ID,new ID,source rows,shadow rows) {witnesses}")
                workers = [threading.Thread(target=writer, args=(kind,), daemon=True) for kind in range(4)]
                for worker in workers:
                    worker.start()
                deadline = time.monotonic() + 3
                while min(acknowledged) < 2 and not errors and time.monotonic() < deadline:
                    time.sleep(.01)
                self.check(min(acknowledged) >= 2 and not ddl_finished.is_set(),
                           f"autocommit/local/distributed/RC commit during baseline copy: {acknowledged}")
                # Dirty keys include rollback-only writes and both sides of
                # a DELETE/reinsert. The final image must reread confirmed
                # source state rather than replay a captured payload.
                for key in (left[5], left[-1]):
                    q(c, "DELETE FROM ec_online WHERE tenant=%s AND id=%s", key)
                    del expected[key]
                key = left[6]
                q(c, "DELETE FROM ec_online WHERE tenant=%s AND id=%s", key)
                q(c, "INSERT INTO ec_online VALUES (%s,%s,91,'reinserted')", key)
                expected[key] = [91, "reinserted"]
                q(c, "BEGIN")
                q(c, "INSERT INTO ec_online VALUES (999,999,999,'rolled back')")
                q(c, "ROLLBACK")
            finally:
                for node in nodes:
                    try:
                        rpc(node, "tarn_test_hook", ["engine_copy", "release"])
                    except Exception as error:
                        errors.append(f"release {node}: {error}")
                if ddl_thread.ident is not None:
                    ddl_thread.join(30)
                stop.set()
                for worker in workers:
                    worker.join(20)
                ddl_connection.close()
            self.check(not ddl_thread.is_alive() and all(not worker.is_alive() for worker in workers),
                       "all owned migration/writer threads terminated")
            self.check(not errors and not ddl_errors, f"copy/cutover errors: {errors[:4]} {ddl_errors}")
            for key, count in deltas.items():
                expected[key][0] += count
            actual = q(c, "SELECT tenant,id,n,tag FROM ec_online ORDER BY tenant,id")
            wanted = tuple((key[0], key[1], value[0], value[1]) for key, value in sorted(expected.items()))
            self.check(actual == wanted, f"{engine}: every acknowledged per-key change retained exactly; no rolled-back row")
            self.check(sum(len(image["rows"]) for image in self.engine_image("ec_online")) == len(wanted),
                       "direct native primary/bucket images retain each public row once")
            native_rows = []
            for node in nodes:
                rows = ev(node, "local name=... local out={} box.begin{txn_isolation='read-confirmed'} "
                          "local ok,err=pcall(function() for _,r in box.space[name]:pairs() do "
                          "out[#out+1]={r[1],r[2],r[3],r[4]} end end) box.rollback() "
                          "if not ok then error(err) end return out", [f"{DB}.ec_online"])
                native_rows.extend(tuple(row) for row in rows)
            self.check(tuple(sorted(native_rows)) == wanted,
                       "independent native values equal every acknowledged per-key effect exactly")
            self.change_engine(c, "ec_online", engine)  # completed same-engine request remains usable
            print(f"    {engine}: copy-window writer commits {acknowledged}; exact {len(wanted)}-row audit")
        c.close()

    def test_engine_change_old_streams(self):
        """An old retained-space handle cannot validate after physical publication."""
        c = self.c(read_timeout=30)
        q(c, "DROP TABLE IF EXISTS ec_stream")
        q(c, "CREATE TABLE ec_stream (id INT PRIMARY KEY,n INT) ENGINE=memtx")
        q(c, "INSERT INTO ec_stream VALUES " + ",".join(f"({identity},10)" for identity in range(1, 9)))
        for engine in ("vinyl", "memtx"):
            source_image = self.engine_image("ec_stream")
            reader = self.c(1, read_timeout=15)
            writer = self.c(1, read_timeout=15)
            rc = self.c(0, read_timeout=15)
            late_writer = self.c(1, read_timeout=15)
            try:
                q(reader, "BEGIN")
                self.check(q(reader, "SELECT SUM(n) FROM ec_stream") == ((80,),), "old read-only stream reads the full committed image")
                q(writer, "BEGIN")
                q(writer, "UPDATE ec_stream SET n=n+100 WHERE id=2")
                q(rc, "SET SESSION transaction_isolation='READ-COMMITTED'")
                q(rc, "BEGIN")
                q(rc, "UPDATE ec_stream SET n=n+100 WHERE id=3")
                q(late_writer, "SET SESSION transaction_isolation='SERIALIZABLE'")
                q(late_writer, "BEGIN")
                self.check(q(late_writer, "SELECT n FROM ec_stream WHERE id=4") == ((10,),),
                           "late writer opens a read stream before the physical replacement")
                self.change_engine(c, "ec_stream", engine)
                published_image = self.engine_image("ec_stream")
                late_error = code(lambda: q(late_writer, "UPDATE ec_stream SET n=n+100 WHERE id=4"))
                commit_error = code(lambda: q(late_writer, "COMMIT")) if late_error is None else None
                print(f"    {engine}: late UPDATE/COMMIT errors {late_error}/{commit_error}; native old/new IDs "
                      f"{[image['id'] for image in source_image]}/{[image['id'] for image in published_image]}")
                self.check(late_error in (1213, 1412) or (late_error is None and commit_error in (1213, 1412)),
                           f"{engine}: old read context aborts at UPDATE or COMMIT, never 1146 or a successful commit")
                code(lambda: q(late_writer, "ROLLBACK"))
                self.check(code(lambda: q(reader, "COMMIT")) == 1213,
                           f"{engine}: retained original-space reader cannot validate after publication")
                for name, connection in (("buffered serializable", writer), ("READ COMMITTED", rc)):
                    error = code(lambda: q(connection, "COMMIT"))
                    self.check(error in (1213, 1412), f"{engine}: {name} old writer has a known abort: {error}")
                self.check(q(c, "SELECT id,n FROM ec_stream ORDER BY id") == tuple((identity, 10) for identity in range(1, 9)),
                           f"{engine}: neither old buffered writer reached the replacement")
                q(reader, "BEGIN")
                q(reader, "SELECT SUM(n) FROM ec_stream")
                self.change_engine(c, "ec_stream", engine)
                self.check(code(lambda: q(reader, "COMMIT")) is None, "same-engine no-op does not doom a current reader")
            finally:
                for connection in (reader, writer, rc, late_writer):
                    code(lambda: q(connection, "ROLLBACK"))
                    connection.close()
        c.close()

    def test_engine_change_global_miss_stream(self):
        """A global UNIQUE miss records only auxiliary reads and must be doomed too."""
        from lazytest import stat
        c = self.c(read_timeout=30)
        q(c, "DROP TABLE IF EXISTS ec_miss")
        q(c, "CREATE TABLE ec_miss (tenant INT NOT NULL,id INT NOT NULL,h VARCHAR(20),"
             "PRIMARY KEY(tenant,id),UNIQUE KEY by_h(h)) PARTITION BY KEY(tenant) ENGINE=memtx")
        q(c, "INSERT INTO ec_miss VALUES (1,1,'held')")
        for engine in ("vinyl", "memtx"):
            reader = self.c(1, read_timeout=15)
            try:
                q(reader, "SET SESSION transaction_isolation='SERIALIZABLE'")
                q(reader, "BEGIN")
                calls, rows = stat(reader, "Tarn_storage_reads"), stat(reader, "Tarn_rows_read")
                self.check(q(reader, "SELECT tenant,id FROM ec_miss FORCE INDEX(by_h) WHERE h='missing'") == (),
                           "global UNIQUE value is absent")
                self.check(stat(reader, "Tarn_storage_reads") == calls + 1 and stat(reader, "Tarn_rows_read") == rows,
                           "global miss reads one auxiliary entry and fetches no base row")
                self.change_engine(c, "ec_miss", engine)
                error = code(lambda: q(reader, "COMMIT"))
                self.check(error == 1213, f"{engine}: old global-only reader cannot validate after publication: {error}")
                self.check(q(c, "SELECT tenant,id,h FROM ec_miss") == ((1, 1, "held"),),
                           "rejecting the old auxiliary reader preserves the exact base row")
                q(reader, "BEGIN")
                q(reader, "SELECT tenant,id FROM ec_miss FORCE INDEX(by_h) WHERE h='missing'")
                self.change_engine(c, "ec_miss", engine)
                self.check(code(lambda: q(reader, "COMMIT")) is None,
                           "same-engine no-op leaves a current global-miss reader valid")
            finally:
                code(lambda: q(reader, "ROLLBACK"))
                reader.close()
        c.close()

    def test_dump(self):
        """A mysqldump file (executable comments, SET with variables, LOCK
        TABLES, DISABLE KEYS, INVISIBLE indexes and table options) loads
        after explicitly removing an unsupported foreign-key declaration."""
        c = self.c(db=None)
        q(c, "DROP DATABASE IF EXISTS ddl_dump")
        q(c, "CREATE DATABASE ddl_dump")
        path = os.path.join(HERE, "data", "mysql8_dump.sql")
        original = open(path).read()
        declaration = (",\n  CONSTRAINT `fk_rails_61ac11da2b` FOREIGN KEY (`account_id`) "
                       "REFERENCES `accounts` (`id`)")
        assert original.count(declaration) == 1
        # This known fixture is migrated deliberately; the server never strips
        # arbitrary constraints from a dump or accepts them as enforcement.
        migrated = original.replace(declaration, "")
        q(c, "USE ddl_dump")
        self.check(code(lambda: q(c, "CREATE TABLE unsupported_fk (id INT PRIMARY KEY, p INT, "
                                  "FOREIGN KEY(p) REFERENCES accounts(id))")) == 1235,
                   "unmodified foreign-key declarations are refused")
        cli = shutil.which("mysql") or shutil.which("mariadb")
        host, port = self.servers[0].split(":")
        if cli:
            r = subprocess.run([cli, "-h", host, "-P", port, "-u", "sb", "ddl_dump"], input=migrated,
                               capture_output=True, text=True)
            self.check(r.returncode == 0 and not r.stderr.strip(), f"mysql client: {r.stderr.strip()[:400]}")
        else:
            m = self.c(db="ddl_dump", multi=True)
            cur = m.cursor()
            cur.execute(migrated)
            while cur.nextset():
                pass
        d = self.c(db="ddl_dump")
        self.check(q(d, "SELECT COUNT(*) FROM accounts")[0][0] == 3, "accounts rows")
        self.check(q(d, "SELECT email FROM users ORDER BY id") == (("a@acme.test",), ("b@globex.test",)), "users rows")
        create = q(d, "SHOW CREATE TABLE accounts")[0][1]
        self.check("INVISIBLE" in create and "COMMENT='customer accounts'" in create, f"SHOW CREATE TABLE: {create}")
        self.check("KEY `index_accounts_on_created_at` (`created_at`) COMMENT 'for reports'" in create,
                   f"index comment kept: {create}")
        create = q(d, "SHOW CREATE TABLE users")[0][1]
        self.check("FOREIGN KEY" not in create, f"explicitly migrated schema: {create}")
        # AUTO_INCREMENT=4 in the dump: the next generated id is 4
        q(d, "INSERT INTO accounts (name, created_at, updated_at) VALUES ('new', NOW(), NOW())")
        self.check(q(d, "SELECT LAST_INSERT_ID()")[0][0] == 4, "AUTO_INCREMENT table option")
        # the dump's session settings are back to their defaults afterwards
        self.check(q(d, "SELECT @@foreign_key_checks, @@unique_checks")[0] == (1, 1), "restored session variables")
        # SHOW CREATE TABLE reloads to the same table: quotes and backslashes
        # in index and table comments and defaults (#80)
        q(d, "CREATE TABLE esc (id INT PRIMARY KEY, v VARCHAR(9) DEFAULT 'a\\\\b''c', "
             "KEY kv (v) COMMENT 'd\\\\e''f') COMMENT 'g\\\\h''i'")
        create = q(d, "SHOW CREATE TABLE esc")[0][1]
        q(d, "DROP TABLE esc")
        q(d, create)
        self.check(q(d, "SHOW CREATE TABLE esc")[0][1] == create, f"SHOW CREATE TABLE reloads: {create}")
        self.check(q(d, "SHOW INDEX FROM esc")[1][12] == "d\\e'f", "index comment with a backslash and a quote")
        # MariaDB escapes NUL/LF/CR in a dump's DDL, while TAB/BS/CTRL-Z
        # remain raw. Check exact literals and reload through the CLI too.
        controls = "\0\n\r\t\b\x1a\\'\"雪"
        literal = "'\\0\\n\\r\t\b\x1a\\\\''\"雪'"
        q(d, "CREATE TABLE esc_controls (id INT PRIMARY KEY, v VARCHAR(32) DEFAULT %s, "
             "KEY kv (v) COMMENT %s) COMMENT=%s", (controls, controls, controls))
        q(d, "CREATE INDEX kc ON esc_controls (v) COMMENT %s", (controls,))
        create = q(d, "SHOW CREATE TABLE esc_controls")[0][1]
        self.check(f"DEFAULT {literal}" in create and f"COMMENT={literal}" in create
                   and create.count(f"COMMENT {literal}") == 2,
                   f"control bytes in SHOW CREATE literals: {create!r}")
        q(d, "DROP TABLE esc_controls")
        if cli:
            r = subprocess.run([cli, "-h", host, "-P", port, "-u", "sb", "--default-character-set=utf8mb4", "ddl_dump"],
                               input=create + ";\n", capture_output=True, text=True)
            self.check(r.returncode == 0 and not r.stderr.strip(), f"control DDL reload: {r.stderr.strip()[:400]}")
        else:
            q(d, create)
        self.check(q(d, "SHOW CREATE TABLE esc_controls")[0][1] == create,
                   "control DDL reload is byte-identical")
        comments = {row[2]: row[12] for row in q(d, "SHOW INDEX FROM esc_controls") if row[2] in ("kv", "kc")}
        self.check(comments == {"kv": controls, "kc": controls},
                   "control index comments survive reload")
        q(d, "INSERT INTO esc_controls (id) VALUES (1)")
        self.check(q(d, "SELECT v FROM esc_controls")[0][0] == controls, "control default survives reload")

    def test_multi_statements(self):
        """CLIENT_MULTI_STATEMENTS and COM_SET_OPTION: one result per
        statement, the first error ends the batch."""
        m = self.c(multi=True)
        cur = m.cursor()
        cur.execute("DROP TABLE IF EXISTS ms; CREATE TABLE ms (id INT PRIMARY KEY, v INT); "
                    "INSERT INTO ms VALUES (1, 10), (2, 20); SELECT SUM(v) FROM ms; UPDATE ms SET v = v + 1")
        results = []
        while True:
            results.append((cur.rowcount, cur.fetchall() if cur.description else None))
            if not cur.nextset():
                break
        self.check([r[0] for r in results] == [0, 0, 2, 1, 2] and results[3][1][0][0] == 30, f"results {results}")
        def batch():
            cur.execute("UPDATE ms SET v = 0 WHERE id = 1; SELECT * FROM nosuch; UPDATE ms SET v = 0")
            while cur.nextset():
                pass
        e = code(batch)
        self.check(e == 1146, f"error in a batch: {e}")
        self.check(q(m, "SELECT v FROM ms ORDER BY id") == ((0,), (21,)), "statements after the error did not run")
        # without the capability a batch is a syntax error ...
        plain = self.c()
        self.check(code(lambda: q(plain, "SELECT 1; SELECT 2")) == 1064, "batch refused without the capability")
        # ... until COM_SET_OPTION turns multi-statements on (as Rails does)
        def set_option(opt):
            plain._execute_command(COMMAND.COM_SET_OPTION, opt)
            pkt = plain._read_packet()   # EOF, as MySQL answers
            return pkt.is_eof_packet() or pkt.is_ok_packet()
        self.check(set_option(b"\x00\x00"), "COM_SET_OPTION answered")
        cur = plain.cursor()
        cur.execute("SELECT 1; SELECT 2")
        got = [cur.fetchall()]
        while cur.nextset():
            got.append(cur.fetchall())
        self.check(got == [((1,),), ((2,),)], f"COM_SET_OPTION on: {got}")
        set_option(b"\x01\x00")
        self.check(code(lambda: q(plain, "SELECT 1; SELECT 2")) == 1064, "COM_SET_OPTION off")

    def test_refusals(self):
        """What would need rows rewritten or resharded is refused (1235)."""
        c = self.c()
        q(c, "DROP TABLE IF EXISTS rf")
        q(c, "CREATE TABLE rf (id INT PRIMARY KEY, v INT, s VARCHAR(10), d DECIMAL(6,2))")
        q(c, "INSERT INTO rf VALUES (1, 1, 'a', 1.5)")
        for sql in ["ALTER TABLE rf MODIFY v VARCHAR(10)", "ALTER TABLE rf MODIFY s INT",
                    "ALTER TABLE rf MODIFY d DECIMAL(6,3)", "ALTER TABLE rf DROP PRIMARY KEY",
                    "ALTER TABLE rf ADD PRIMARY KEY (v)", "ALTER TABLE rf DROP COLUMN id",
                    "ALTER TABLE rf MODIFY id BIGINT UNSIGNED NOT NULL",
                    "ALTER TABLE rf ADD COLUMN k INT AUTO_INCREMENT", "ALTER TABLE rf ADD FULLTEXT INDEX f (s)"]:
            self.check(code(lambda: q(c, sql)) == 1235, sql)
        self.check(q(c, "SELECT * FROM rf")[0][:3] == (1, 1, "a"), "table unchanged")

    def test_indexed_default_backfill(self):
        """An index materializes missing frozen fills before it becomes usable."""
        c = self.c()
        for change, value in [
                ("ADD COLUMN c INT DEFAULT 3, ADD INDEX ic (c)", 3),
                ("ADD COLUMN c INT DEFAULT 0, ADD INDEX ic (c)", 0),
                ("ADD COLUMN c INT NOT NULL, ADD INDEX ic (c)", 0),
                ("ADD COLUMN c VARCHAR(10) COLLATE utf8mb4_bin DEFAULT '', ADD INDEX ic (c)", ""),
                ("ADD COLUMN c INT DEFAULT 3 FIRST, ADD INDEX ic (v, c)", 3),
                ("ADD COLUMN c INT DEFAULT 3 AFTER id, ADD INDEX ic (c, v)", 3)]:
            q(c, "DROP TABLE IF EXISTS df_guard")
            q(c, "CREATE TABLE df_guard (id INT PRIMARY KEY, v INT)")
            q(c, "INSERT INTO df_guard VALUES (1, 10), (2, 20)")
            q(c, f"ALTER TABLE df_guard {change}")
            self.check(q(c, "SELECT id, v, c FROM df_guard ORDER BY id") == ((1, 10, value), (2, 20, value)),
                       f"combined ALTER preserves rows and supplies the frozen fill: {change}")
            self.check(q(c, "SELECT id FROM df_guard WHERE c = %s ORDER BY id", (value,)) == ((1,), (2,)),
                       f"the new index finds both previously short tuples: {change}")
            self.check(any(row[2] == "ic" for row in q(c, "SHOW INDEX FROM df_guard")),
                       f"the index is published: {change}")
            self.check(q(c, "UPDATE df_guard SET v = v + 1 WHERE id = 1") == 1,
                       f"successful ALTER releases its write fence: {change}")

        q(c, "DROP TABLE IF EXISTS df_frozen")
        q(c, "CREATE TABLE df_frozen (id INT PRIMARY KEY, v INT)")
        q(c, "INSERT INTO df_frozen VALUES (1, 10)")
        q(c, "ALTER TABLE df_frozen ADD COLUMN c INT DEFAULT 3")
        q(c, "ALTER TABLE df_frozen ALTER COLUMN c SET DEFAULT NULL")
        q(c, "INSERT INTO df_frozen (id, v) VALUES (2, 20)")
        q(c, "ALTER TABLE df_frozen ALTER COLUMN c SET DEFAULT 9")
        q(c, "INSERT INTO df_frozen (id, v) VALUES (3, 30)")
        q(c, "INSERT INTO df_frozen (id, v, c) VALUES (4, 40, 77)")
        q(c, "UPDATE df_frozen SET c = c")
        q(c, "ALTER TABLE df_frozen ADD INDEX ic (c)")
        self.check(q(c, "SELECT id, c FROM df_frozen ORDER BY id") == ((1, 3), (2, None), (3, 9), (4, 77)),
                   "backfill uses the frozen fill and preserves physically present NULL and custom values")
        indexed = q(c, "SELECT id FROM df_frozen WHERE c = 3 ORDER BY id")
        scanned = q(c, "SELECT id FROM df_frozen WHERE c + 0 = 3 ORDER BY id")
        self.check(indexed == scanned == ((1,),), "backfilled fill has matching indexed and scan results")
        self.check(q(c, "SELECT id FROM df_frozen WHERE c IS NULL") == ((2,),),
                   "physically present NULL is allowed despite a non-NULL frozen fill")
        q(c, "INSERT INTO df_frozen (id, v) VALUES (5, 50)")
        self.check(q(c, "SELECT id FROM df_frozen WHERE c = 9 ORDER BY id") == ((3,), (5,)), "new default is indexed")
        q(c, "UPDATE df_frozen SET c = 10 WHERE id = 5")
        q(c, "DELETE FROM df_frozen WHERE id = 1")
        self.check(q(c, "SELECT id FROM df_frozen WHERE c = 10") == ((5,),)
                   and q(c, "SELECT id FROM df_frozen WHERE c = 9") == ((3,),)
                   and q(c, "SELECT id FROM df_frozen WHERE c = 3") == (), "index stays current under DML")

        q(c, "DROP TABLE IF EXISTS df_ci")
        q(c, "CREATE TABLE df_ci (id INT PRIMARY KEY, v INT)")
        q(c, "INSERT INTO df_ci VALUES (1, 10), (2, 20)")
        q(c, "ALTER TABLE df_ci ADD COLUMN c VARCHAR(10) COLLATE utf8mb4_unicode_ci DEFAULT 'Á ', "
             "ADD INDEX ic (c)")
        self.check(q(c, "SELECT id FROM df_ci WHERE c = 'a' ORDER BY id") == ((1,), (2,)),
                   "same-statement CI column gets the frozen weight including Unicode and PAD SPACE")
        q(c, "INSERT INTO df_ci VALUES (3, 30, 'a')")
        self.check(q(c, "SELECT id FROM df_ci WHERE c = 'Á' ORDER BY id") == ((1,), (2,), (3,)),
                   "the backfilled CI index agrees with weights of future writes")
        q(c, "ALTER TABLE df_ci ADD COLUMN unweighted VARCHAR(10) COLLATE utf8mb4_unicode_ci DEFAULT 'x'")
        self.check(code(lambda: q(c, "ALTER TABLE df_ci ADD INDEX iu (unweighted)")) == 1235,
                   "retrofitting a weight field to an older unweighted CI column remains explicitly unsupported")

    def test_indexed_default_allowed(self):
        """Empty tables, NULL fills and indexes of existing fields remain supported."""
        c = self.c()
        q(c, "DROP TABLE IF EXISTS df_empty")
        q(c, "CREATE TABLE df_empty (id INT PRIMARY KEY, v INT)")
        q(c, "ALTER TABLE df_empty ADD COLUMN c INT DEFAULT 3, ADD INDEX ic (c)")
        q(c, "INSERT INTO df_empty (id, v) VALUES (1, 10)")
        self.check(q(c, "SELECT id FROM df_empty WHERE c = 3") == ((1,),),
                   "combined ADD and index is allowed on an empty table")

        q(c, "DROP TABLE IF EXISTS df_null")
        q(c, "CREATE TABLE df_null (id INT PRIMARY KEY, v INT)")
        q(c, "INSERT INTO df_null VALUES (1, 10), (2, 20)")
        q(c, "ALTER TABLE df_null ADD COLUMN c INT DEFAULT NULL, ADD INDEX ic (c)")
        self.check(q(c, "SELECT id FROM df_null WHERE c IS NULL ORDER BY id") == ((1,), (2,)),
                   "missing fields with NULL fill can be indexed directly")
        q(c, "ALTER TABLE df_null DROP INDEX ic, ALTER COLUMN c SET DEFAULT 3")
        q(c, "ALTER TABLE df_null ADD INDEX ic (c)")
        q(c, "INSERT INTO df_null (id, v) VALUES (3, 30)")
        self.check(q(c, "SELECT id FROM df_null WHERE c IS NULL ORDER BY id") == ((1,), (2,))
                   and q(c, "SELECT id FROM df_null WHERE c = 3") == ((3,),),
                   "a later non-NULL default does not change the original NULL fill")
        q(c, "ALTER TABLE df_null ADD COLUMN d INT DEFAULT 5, ADD INDEX id_d (d, c)")
        self.check(q(c, "SELECT id, c, d FROM df_null ORDER BY id") == ((1, None, 5), (2, None, 5), (3, 3, 5))
                   and q(c, "SELECT id FROM df_null WHERE d = 5 AND c IS NULL ORDER BY id") == ((1,), (2,)),
                   "materializing a later indexed fill accepts and preserves earlier NULL padding")

        q(c, "DROP TABLE IF EXISTS df_existing")
        q(c, "CREATE TABLE df_existing (id INT PRIMARY KEY, v INT)")
        q(c, "INSERT INTO df_existing VALUES (1, 10), (2, 20)")
        q(c, "ALTER TABLE df_existing ADD COLUMN c INT DEFAULT 3, ADD INDEX iv (v)")
        self.check(q(c, "SELECT c FROM df_existing WHERE v = 10") == ((3,),),
                   "a separate new non-NULL fill does not block an index of existing fields")
        q(c, "ALTER TABLE df_existing ADD INDEX ic (c)")
        self.check(q(c, "SELECT id FROM df_existing WHERE c = 3 ORDER BY id") == ((1,), (2,)),
                   "a previously added default column can be indexed without a manual row rewrite")

    def test_indexed_default_unique(self):
        """Duplicate fills fail with1062 and restore reusable physical slots."""
        c = self.c()
        q(c, "DROP TABLE IF EXISTS df_unique")
        q(c, "CREATE TABLE df_unique (tenant INT, id INT, v INT, PRIMARY KEY (tenant, id)) "
             "PARTITION BY KEY(tenant)")
        q(c, "INSERT INTO df_unique VALUES (1, 1, 10), (1, 2, 20)")
        before = q(c, "SHOW CREATE TABLE df_unique")
        e = code(lambda: q(c, "ALTER TABLE df_unique ADD COLUMN c INT DEFAULT 3, ADD UNIQUE KEY uc (c, tenant)"))
        self.check(e == 1062 and q(c, "SHOW CREATE TABLE df_unique") == before,
                   "duplicate frozen fills report1062 and publish neither column nor unique index")
        q(c, "ALTER TABLE df_unique ADD COLUMN d INT DEFAULT 9, ADD INDEX id_d (d, tenant, id)")
        self.check(q(c, "SELECT tenant, id, d FROM df_unique ORDER BY tenant, id") == ((1, 1, 9), (1, 2, 9))
                   and q(c, "SELECT id FROM df_unique WHERE d = 9 ORDER BY id") == ((1,), (2,)),
                   "a different default reuses an aborted slot without inheriting its failed fill")
        q(c, "ALTER TABLE df_unique ADD COLUMN c INT DEFAULT 3")
        e = code(lambda: q(c, "ALTER TABLE df_unique ADD UNIQUE KEY uc (c, tenant)"))
        self.check(e == 1062 and not any(row[2] == "uc" for row in q(c, "SHOW INDEX FROM df_unique")),
                   "duplicate fills from an older instant column report1062 and leave no index")
        q(c, "UPDATE df_unique SET c = 4 WHERE tenant = 1 AND id = 2")
        q(c, "INSERT INTO df_unique (tenant, id, v, c) VALUES (1, 3, 30, NULL), (1, 4, 40, NULL)")
        q(c, "ALTER TABLE df_unique ADD UNIQUE KEY uc (c, tenant)")
        self.check(q(c, "SELECT id FROM df_unique WHERE c IS NULL ORDER BY id") == ((3,), (4,)),
                   "explicit NULLs can coexist in a newly built unique index")
        e = code(lambda: q(c, "INSERT INTO df_unique (tenant, id, v, c) VALUES (1, 5, 50, 3)"))
        self.check(e == 1062, "the accepted unique index rejects future duplicates")
        q(c, "INSERT INTO df_unique (tenant, id, v, c) VALUES (2, 5, 50, 3)")
        self.check(q(c, "SELECT tenant, id FROM df_unique WHERE c = 3 ORDER BY tenant, id") == ((1, 1), (2, 5)),
                   "all composite key parts participate in uniqueness and indexed reads")

    def test_indexed_default_global_rollback(self):
        """A global unique fill failure also restores every prepared shard."""
        c = self.c()
        q(c, "DROP TABLE IF EXISTS df_global")
        q(c, "CREATE TABLE df_global (id INT PRIMARY KEY, v INT)")
        q(c, "INSERT INTO df_global VALUES (1, 10), (2, 20), (3, 30)")
        before = q(c, "SHOW CREATE TABLE df_global")
        e = code(lambda: q(c, "ALTER TABLE df_global ADD COLUMN c INT DEFAULT 3, ADD UNIQUE KEY uc (c)"))
        self.check(e == 1062 and q(c, "SHOW CREATE TABLE df_global") == before,
                   "global duplicate fills abort without publishing the new schema")
        q(c, "ALTER TABLE df_global ADD COLUMN d INT DEFAULT 9, ADD INDEX id_d (d)")
        self.check(q(c, "SELECT id, v, d FROM df_global ORDER BY id") == ((1, 10, 9), (2, 20, 9), (3, 30, 9))
                   and q(c, "SELECT id FROM df_global WHERE d = 9 ORDER BY id") == ((1,), (2,), (3,)),
                   "a global-fill abort cannot poison the next instant-column slot")
        q(c, "ALTER TABLE df_global ADD UNIQUE KEY udv (d, v)")
        self.check(q(c, "SELECT id FROM df_global WHERE d = 9 AND v = 20") == ((2,),),
                   "a later valid global index contains the restored and backfilled values")

    def test_global_unique(self):
        """ALTER TABLE ADD / DROP of unique keys without the shard key
        (issue #88): built from the existing rows across shards, a value held
        twice fails the change (1062), and the key works afterwards."""
        c = self.c()
        q(c, "DROP TABLE IF EXISTS gk")
        q(c, "CREATE TABLE gk (id INT PRIMARY KEY, t INT NOT NULL, h VARCHAR(10), n INT)")
        q(c, "INSERT INTO gk VALUES " + ", ".join(f"({i}, {i % 3}, 'h{i}', {i % 50})" for i in range(1, 2501)))
        q(c, "INSERT INTO gk VALUES (3001, 1, NULL, NULL), (3002, 1, NULL, NULL)")
        self.check(code(lambda: q(c, "ALTER TABLE gk ADD UNIQUE KEY un (n)")) == 1062, "a duplicate fails the change")
        self.check(code(lambda: q(c, "INSERT INTO gk VALUES (4000, 1, 'x', 7)")) is None, "... which leaves no key")
        q(c, "DELETE FROM gk WHERE id = 4000")
        q(c, "ALTER TABLE gk ADD UNIQUE KEY th (t, h)")
        self.check(code(lambda: q(c, "INSERT INTO gk VALUES (4001, 1, 'h1', 0)")) == 1062, "the new key holds across shards")
        self.check(code(lambda: q(c, "INSERT INTO gk VALUES (4002, 1, NULL, 0)")) is None, "... NULLs take no entry")
        self.check(q(c, "SELECT id FROM gk WHERE t = 2 AND h = 'h2000'") == ((2000,),), "lookups through it")
        self.check(code(lambda: q(c, "UPDATE gk SET h = 'h4' WHERE id = 7")) == 1062, "updates check it")
        q(c, "ALTER TABLE gk DROP INDEX th")
        self.check(code(lambda: q(c, "INSERT INTO gk VALUES (4003, 1, 'h1', 0)")) is None, "a dropped key holds no more")
        self.check(code(lambda: q(c, "ALTER TABLE gk ADD UNIQUE KEY th (t, h)")) == 1062,
                   "adding it again finds the duplicate it now has")
        q(c, "DELETE FROM gk WHERE id = 4003")
        q(c, "CREATE UNIQUE INDEX th ON gk (t, h)")
        self.check(code(lambda: q(c, "INSERT INTO gk VALUES (4004, 2, 'h2', 0)")) == 1062, "CREATE UNIQUE INDEX")
        self.check(code(lambda: q(c, "ALTER TABLE gk RENAME INDEX th TO th2")) == 1846, "renaming one is refused")
        q(c, "DROP TABLE IF EXISTS gk2")
        q(c, "ALTER TABLE gk RENAME TO gk2, ADD UNIQUE KEY un2 (h, n)")
        self.check(code(lambda: q(c, "INSERT INTO gk2 VALUES (4005, 2, 'h1', 1)")) == 1062,
                   "RENAME TO and ADD UNIQUE in one statement")
        q(c, "DROP TABLE gk2")

    def test_table_trees(self):
        """COMMENT 'tarn:parent=...' declares a tree of tables (issue #85):
        keys that do not line up are warnings (SHOW WARNINGS), or errors
        under tarn_tree_checks = ERROR; the parent's CREATE and DROP check the
        children that name it, also when another SQL server made them."""
        a, b = self.c(0), self.c(1)
        for t in ["tt_line", "tt_bad", "tt_bad2", "tt_orders", "tt_customer", "tt_off"]:
            q(a, f"DROP TABLE IF EXISTS {t}")

        def warnings(c):
            return [(r[0], r[1], r[2]) for r in q(c, "SHOW WARNINGS")]

        def created(c, sql):
            cur = c.cursor()
            cur.execute(sql)
            n = c._result.warning_count
            cur.close()
            return n

        line = ("CREATE TABLE tt_line (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, line_no INT NOT NULL, "
                "PRIMARY KEY (customer_id, order_id, line_no)) COMMENT 'tarn:parent=tt_orders' "
                "PARTITION BY KEY (customer_id)")
        # a dump may create a child before its parent: a warning, never an error
        q(a, "SET tarn_tree_checks = 'ERROR'")
        self.check(created(a, line) == 1, "warning count in the OK packet")
        w = warnings(a)
        self.check(w == [("Warning", 1105, f"{DB}.tt_line: tarn:parent=tt_orders: table {DB}.tt_orders does not exist (yet)")],
                   f"missing parent: {w}")
        self.check(warnings(a) == w, "SHOW WARNINGS keeps them")
        self.check(q(a, "SELECT @@warning_count")[0][0] == 1, "warning_count reads the previous total")
        self.check(warnings(a) == [], "SELECT warning_count clears diagnostics, as in MySQL")
        q(a, "SELECT 1")
        self.check(warnings(a) == [], "the next statement clears them")
        q(a, "SET tarn_tree_checks = DEFAULT")
        # the parent, through the other SQL server: no warning about the child
        self.check(created(b, "CREATE TABLE tt_customer (customer_id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(20))") == 0,
                   "a root")
        self.check(created(b, "CREATE TABLE tt_orders (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, "
                              "PRIMARY KEY (customer_id, order_id)) COMMENT 'tarn:parent=tt_customer' "
                              "PARTITION BY KEY (customer_id)") == 0, f"the parent: {warnings(b)}")
        # keys that do not line up
        bad = ("CREATE TABLE tt_bad (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, line_no INT NOT NULL, "
               "PRIMARY KEY (customer_id, line_no)) COMMENT 'tarn:parent=tt_orders' PARTITION BY KEY (customer_id)")
        self.check(created(a, bad) == 1 and "must start with the primary key of tt_orders" in warnings(a)[0][2],
                   f"a child keyed without its parent: {warnings(a)}")
        q(a, "SET tarn_tree_checks = 'error'")
        self.check(q(a, "SELECT @@tarn_tree_checks")[0][0] == "ERROR", "tarn_tree_checks")
        bad2 = ("CREATE TABLE tt_bad2 (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, line_no INT NOT NULL, "
                "PRIMARY KEY (customer_id, order_id, line_no)) COMMENT 'tarn:parent=tt_orders'")
        self.check(code(lambda: q(a, bad2)) == 1105, "ERROR refuses a child sharded by its own key")
        self.check(code(lambda: q(a, "SELECT 1 FROM tt_bad2")) == 1146, "the refused table does not exist")
        # an unrelated change to a table that was already off is allowed
        self.check(code(lambda: q(a, "ALTER TABLE tt_bad ADD COLUMN qty INT")) is None, "unrelated ALTER TABLE")
        self.check(code(lambda: q(a, "ALTER TABLE tt_customer COMMENT = 'tarn:parent=tt_line'")) == 1105,
                   "ERROR refuses a cycle")
        # dropping the parent: the children that name it are warned about
        q(a, "SET tarn_tree_checks = WARN")
        cur = a.cursor()
        cur.execute("DROP TABLE tt_orders")
        self.check(a._result.warning_count == 2, f"DROP TABLE: {a._result.warning_count} warnings")
        w = sorted(x[2] for x in warnings(a))
        self.check(w == [f"{DB}.tt_bad: tarn:parent=tt_orders: table {DB}.tt_orders does not exist (yet)",
                         f"{DB}.tt_line: tarn:parent=tt_orders: table {DB}.tt_orders does not exist (yet)"],
                   f"orphans: {w}")
        # off, and bad values
        q(a, "SET tarn_tree_checks = OFF")
        self.check(created(a, "CREATE TABLE tt_off (id INT PRIMARY KEY) COMMENT 'tarn:parent=nosuch'") == 0, "OFF")
        self.check(code(lambda: q(a, "SET tarn_tree_checks = 'sometimes'")) == 1231, "bad value")
        self.check(code(lambda: q(a, "SET GLOBAL tarn_tree_checks = 'ERROR'")) == 1228, "a session variable")

    def test_concurrent_ddl(self):
        """Schema changes from several connections at once (as sysbench's
        prepare creates its indexes): they queue, none fails."""
        errors = []

        def work(i):
            try:
                c = self.c(i)
                q(c, f"DROP TABLE IF EXISTS cd{i}")
                q(c, f"CREATE TABLE cd{i} (id INT PRIMARY KEY, v INT)")
                q(c, f"INSERT INTO cd{i} VALUES (1, 1), (2, 2)")
                q(c, f"CREATE INDEX k_{i} ON cd{i} (v)")
                q(c, f"ALTER TABLE cd{i} ADD COLUMN w INT DEFAULT 7")
            except pymysql.MySQLError as e:
                errors.append(f"cd{i}: {e}")

        threads = [threading.Thread(target=work, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(120)
        self.check(not errors, f"concurrent schema changes: {errors}")
        c = self.c()
        for i in range(6):
            keys = {r[2] for r in q(c, f"SHOW INDEX FROM cd{i}")}
            self.check(f"k_{i}" in keys, f"cd{i} has its index: {keys}")
            self.check(q(c, f"SELECT SUM(w) FROM cd{i}")[0][0] == 14, f"cd{i} has its column")

    def test_fresh_tables(self):
        """A table one server just created is usable through the other at
        once, while schema changes on another table keep bumping the catalog
        version (a statement meets the catalog it was planned with replaced
        by a newer one: it must run again, not fail)."""
        import json

        c = self.c()
        q(c, "DROP TABLE IF EXISTS fr_churn")
        q(c, "CREATE TABLE fr_churn (id INT PRIMARY KEY, v INT)")
        stop = threading.Event()
        churn_errors = []

        def churn():
            d = self.c()
            k = 0
            while not stop.is_set():
                k += 1
                try:
                    q(d, f"ALTER TABLE fr_churn ADD COLUMN c{k} INT")
                    q(d, f"ALTER TABLE fr_churn DROP COLUMN c{k}")
                except pymysql.MySQLError as e:
                    churn_errors.append(str(e))

        t = threading.Thread(target=churn, daemon=True)
        t.start()
        a, b = self.c(0), self.c(1)
        errors = []
        outcomes = {"committed_ack": 0, "known_abort": 0, "rolled_back": 0, "unknown": 0}

        def error_record(e):
            number = e.args[0] if e.args else None
            return {"code": number, "message": str(e), "type": type(e).__name__,
                    "status": "unknown" if number in (UNKNOWN, 0, 2006, 2013, 2055) else "definite_error"}

        try:
            for i in range(100):
                q(a, "DROP TABLE IF EXISTS fr")
                q(a, "CREATE TABLE fr (id INT PRIMARY KEY, v INT)")
                q(a, "INSERT INTO fr VALUES (1, 1)")
                trace = {"iteration": i, "gateway": self.servers[1 % len(self.servers)], "phases": []}
                phase = "SELECT"
                try:
                    for phase, sql in (("SELECT", "SELECT * FROM fr WHERE id = 1"),
                                       ("BEGIN", "BEGIN"),
                                       ("UPDATE", "UPDATE fr SET v = v + 1 WHERE id = 1"),
                                       ("COMMIT", "COMMIT")):
                        result = q(b, sql)
                        trace["phases"].append({"phase": phase, "status": "ack", "result": result})
                        if phase == "SELECT" and result != ((1, 1),):
                            errors.append(f"{i} SELECT returned {result}, expected ((1, 1),)")
                        if phase == "UPDATE" and result != 1:
                            errors.append(f"{i} UPDATE affected {result}, expected 1")
                    trace["outcome"] = "committed_ack"
                except pymysql.MySQLError as e:
                    failure = error_record(e)
                    trace["phases"].append({"phase": phase, **failure})
                    errors.append(f"{i} {phase} ({failure['status']}): {e}")
                    # Preserve the original failure even if cleanup or its
                    # independent audit fails. A lost COMMIT response remains
                    # unknown after ROLLBACK; no mutation is replayed here.
                    try:
                        trace["rollback"] = {"status": "ack", "result": q(b, "ROLLBACK")}
                    except pymysql.MySQLError as cleanup:
                        trace["rollback"] = error_record(cleanup)
                    if failure["status"] == "unknown":
                        trace["outcome"] = "unknown"
                    elif failure["code"] in (1213, 1412):
                        trace["outcome"] = "known_abort"
                    elif trace["rollback"]["status"] == "ack":
                        trace["outcome"] = "rolled_back"
                    else:
                        trace["outcome"] = "unknown"
                outcomes[trace["outcome"]] += 1
                try:
                    rows = q(a, "SELECT id, v FROM fr ORDER BY id")
                    trace["audit"] = {"status": "ack", "rows": rows}
                    if trace["outcome"] == "committed_ack":
                        allowed = (((1, 2),),)
                    elif trace["outcome"] in ("known_abort", "rolled_back"):
                        allowed = (((1, 1),),)
                    else:
                        # This is only an observation of an unresolved
                        # operation, not proof that its outcome has settled.
                        allowed = (((1, 1),), ((1, 2),))
                        trace["audit"]["settled"] = False
                    if rows not in allowed:
                        errors.append(f"{i} audit after {trace['outcome']}: {rows}, expected one of {allowed}")
                except pymysql.MySQLError as audit:
                    trace["audit"] = error_record(audit)
                    errors.append(f"{i} audit after {trace['outcome']}: {audit}")
                print("    FRESH_TABLE_TRACE " + json.dumps(trace, sort_keys=True, separators=(",", ":")), flush=True)
        finally:
            stop.set()
            t.join(30)
        print("    fresh table outcomes: " + json.dumps(outcomes, sort_keys=True)
              + f"; churn thread alive after join: {t.is_alive()}")
        self.check(not errors, f"statements on a fresh table: {errors[:5]}")
        self.check(not churn_errors, f"schema changes: {churn_errors[:5]}")

    def test_index_build_ahead(self):
        """ADD INDEX on a vinyl table completes BUILD before it fences writes.
        A held BUILD reply witnesses acknowledged writes on every shard.
        Memtx builds behind the fence; there only the outcome is checked."""
        c = self.c()
        q(c, "DROP TABLE IF EXISTS bigt")
        q(c, "CREATE TABLE bigt (id INT PRIMARY KEY, k INT, v INT, pad VARCHAR(100))")
        vinyl = q(c, "SHOW TABLE STATUS LIKE 'bigt'")[0][1].lower() == "vinyl"
        rows = 300_000 if vinyl else 100_000
        pad = "p" * 80
        for start in range(0, rows, 1000):
            q(c, "INSERT INTO bigt VALUES " + ", ".join(
                f"({i}, {(i * 7919) % rows}, 0, '{pad}')" for i in range(start, start + 1000)))
        done = threading.Event()
        lat, errors = [], []

        def writer():
            w = self.c(1)
            i = 0
            while not done.is_set():
                i += 1
                t0 = time.time()
                try:
                    q(w, f"UPDATE bigt SET v = v + 1 WHERE id = {(i * 104729) % rows}")
                except pymysql.MySQLError as e:
                    errors.append(str(e))
                lat.append((t0, time.time()))

        nodes = self.args.kv.split(",") if vinyl and self.args.kv else []
        installed, ddl_errors = [], []
        ddl_finished = threading.Event()

        def alter():
            connection = self.c(read_timeout=30)
            try:
                q(connection, "ALTER TABLE bigt ADD INDEX kk (k)")
            except Exception as error:
                ddl_errors.append(repr(error))
            finally:
                connection.close()
                ddl_finished.set()

        if nodes:
            from lazytest import ev
        t = threading.Thread(target=writer, daemon=True)
        ddl = threading.Thread(target=alter, daemon=True)
        a0, a1 = time.monotonic(), time.monotonic()
        try:
            for node in nodes:
                ev(node, """assert(tarn.cfg.test_hooks and not __index_build_review)
local h={old=tarn_alter_build, reached=false, release=false}; __index_build_review=h
function tarn_alter_build(name,a,gen)
 local r=h.old(name,a,gen)
 if name=='ddltest.bigt' and r[1]=='ok' and #(a.builds or {})>0 then
  local sp=assert(box.space[name]); h.built=true
  for _,b in ipairs(a.builds) do h.built=h.built and sp.index[b[1]]~=nil end
  h.unfenced=box.space._tarn_meta:get{'hold:'..sp.id}==nil
  h.key=require('key_def').new(sp.index[0].parts):extract_key(sp.index[0]:min())[1]
  h.reached=true
  local deadline=require('clock').monotonic()+15
  while not h.release and require('clock').monotonic()<deadline do require('fiber').sleep(.01) end
  h.timed_out=not h.release
 end
 return r
end
return true""")
                installed.append(node)
            t.start()
            time.sleep(0.5)
            a0 = time.time()
            ddl.start()
            if nodes:
                # Hold the real completed BUILD before its PREPARE reply.
                # No generic yield hook is held: native writes remain enabled.
                deadline = time.monotonic() + 10
                witnesses = []
                for node in nodes:
                    state = None
                    while time.monotonic() < deadline and not ddl_finished.is_set():
                        state = ev(node, "local h=assert(__index_build_review); return {reached=h.reached,built=h.built,unfenced=h.unfenced,key=h.key}")
                        if state['reached']:
                            break
                        time.sleep(.01)
                    assert state and state['reached'] and state['built'] and state['unfenced'], (node,state,ddl_errors)
                    witnesses.append(state)
                for state in witnesses:
                    for _ in range(10):
                        q(c, "UPDATE bigt SET v=v+1 WHERE id=%s", (state['key'],))
                    assert q(c, "SELECT v FROM bigt WHERE id=%s", (state['key'],))[0][0] >= 10
                assert not ddl_finished.is_set(), ddl_errors
                print(f"    completed unfenced BUILD and ten acknowledged writes on each of {len(nodes)} shards")
            for node in installed:
                ev(node, "__index_build_review.release=true; return true")
            ddl.join(30)
            a1 = time.time()
            assert not ddl.is_alive() and not ddl_errors, ddl_errors
        finally:
            for node in installed:
                ev(node, "local h=__index_build_review; if h then h.release=true; tarn_alter_build=h.old end; return true")
            if ddl.ident is not None:
                ddl.join(30)
            done.set()
            if t.ident is not None:
                t.join(30)
            for node in installed:
                state = ev(node, "local h=__index_build_review; __index_build_review=nil; return {timed_out=h and h.timed_out or false}")
                self.check(not state['timed_out'], "build witness released before its safety deadline")
        within = sum(1 for (s0, s1) in lat if a0 <= s0 and s1 <= a1)
        worst = max((s1 - s0 for (s0, s1) in lat if s0 <= a1 and s1 >= a0), default=0.0)
        print(f"    ADD INDEX on {rows} {'vinyl' if vinyl else 'memtx'} rows: {a1 - a0:.2f} s; "
              f"{within} writes during it, the slowest overlapping one {worst * 1000:.0f} ms "
              "(observational; includes the controlled BUILD hold when enabled)")
        self.check(not errors and not t.is_alive(), f"writes during the index build: {errors[:3]}")
        if vinyl:
            self.check(within >= 10, f"writes went on during the index build ({within})")
        self.check(q(c, "SELECT COUNT(*) FROM bigt WHERE k = 7919")[0][0] == 1, "the new index answers")
        q(c, "DROP TABLE bigt")

    def test_txn_across_alter(self):
        """A transaction that wrote a table cannot use a newer definition of
        it: rows in its write buffer are in the old column order. Its next
        statement on the table fails with 1412 and it is rolled back."""
        c = self.c()
        q(c, "DROP TABLE IF EXISTS tx")
        q(c, "CREATE TABLE tx (id INT PRIMARY KEY, n INT NOT NULL, s VARCHAR(10))")
        q(c, "INSERT INTO tx VALUES (1, 10, 'a'), (2, 20, 'b')")
        probe = self.c(1)   # the writer's SQL server (the second, if any)

        def wait_for(col, present):
            deadline = time.time() + 10
            while time.time() < deadline:
                if (f"`{col}`" in q(probe, "SHOW CREATE TABLE tx")[0][1]) == present:
                    return True
                time.sleep(0.05)
            return False

        # Under READ COMMITTED no stream transaction holds the reads, so
        # nothing else stops the old rows from being reused.
        cases = [("ADD COLUMN y VARCHAR(10) DEFAULT 'zz' AFTER id", "y", True, "UPDATE tx SET n = n + 1 WHERE id = 1"),
                 ("DROP COLUMN s", "s", False, "SELECT * FROM tx"),
                 ("ADD COLUMN z INT FIRST", "z", True, "INSERT INTO tx (id, n) VALUES (3, 30)"),
                 ("DROP COLUMN y", "y", False, "UPDATE tx SET n = n + 1 WHERE id = 1"),
                 ("ADD COLUMN s CHAR(2) AFTER id", "s", True, "SELECT * FROM tx WHERE id = 1"),
                 ("DROP COLUMN z", "z", False, "DELETE FROM tx WHERE id = 1")]
        for k, (alter, col, present, stmt) in enumerate(cases):
            iso = "SERIALIZABLE" if k % 2 == 0 else "READ COMMITTED"
            w = self.c(1, autocommit=False)
            q(w, f"SET SESSION TRANSACTION ISOLATION LEVEL {iso}")
            q(w, "UPDATE tx SET n = n + 1 WHERE id = 1")
            q(c, f"ALTER TABLE tx {alter}")
            self.check(wait_for(col, present), f"{alter}: the writer's server reloads its catalog")
            e = code(lambda: q(w, stmt))
            self.check(e == 1412, f"{iso}: {stmt} after {alter} in a transaction that wrote the table: {e}")
            q(w, "COMMIT")   # nothing left to commit
            self.check(q(c, "SELECT n FROM tx WHERE id = 1") == ((10,),), f"case {k}: the first UPDATE was rolled back")
            w.close()
        self.check(q(c, "SELECT id, n FROM tx ORDER BY id") == ((1, 10), (2, 20)), "table rows")

    def test_online(self):
        """Writers on every SQL server while one of them changes the table."""
        seconds, nthreads = self.args.seconds, self.args.threads
        c = self.c()
        q(c, "DROP TABLE IF EXISTS acc")
        extra = ", x INT NOT NULL DEFAULT 0" if self.args.no_ddl else ""
        q(c, f"CREATE TABLE acc (id INT PRIMARY KEY, n INT NOT NULL DEFAULT 0, tag VARCHAR(20){extra}, KEY kn (n))")
        q(c, "INSERT INTO acc (id, tag) VALUES " + ", ".join(f"({i}, 't{i}')" for i in range(1, 65)))
        lock = threading.Lock()
        st = {"n": 0, "x": 0, "n_unknown": 0, "x_unknown": 0, "ops": 0, "retries": 0, "errors": [], "ddl": 0}
        x_ready = threading.Event()
        if self.args.no_ddl:
            x_ready.set()
        stop = threading.Event()

        def bump(key, k=1):
            with lock:
                st[key] += k

        def autocommit(conn, sql, args, key):
            try:
                bump(key, q(conn, sql, args))   # rows changed (0 if the WHERE fails)
            except pymysql.MySQLError as e:
                if e.args[0] == UNKNOWN:
                    bump(key + "_unknown")
                else:
                    with lock:
                        st["errors"].append(f"autocommit {sql}: {e}")

        def txn(conn, body):
            """Run body in a transaction until it commits; returns its result."""
            for _ in range(200):
                try:
                    q(conn, "BEGIN")
                    r = body()
                    q(conn, "COMMIT")
                    return r
                except pymysql.MySQLError as e:
                    try:
                        q(conn, "ROLLBACK")
                    except pymysql.MySQLError:
                        pass
                    if e.args[0] in RETRY:
                        bump("retries")
                        time.sleep(random.random() * 0.01)
                        continue
                    with lock:
                        st["errors"].append(f"transaction: {e}")
                    return None
            return None

        def writer(w):
            conn = self.c(w)
            if w % 3 == 2:
                q(conn, "SET SESSION transaction_isolation = 'READ-COMMITTED'")
            rnd = random.Random(w)
            while not stop.is_set():
                a, b = rnd.randint(1, 64), rnd.randint(1, 64)
                kind = rnd.randint(0, 4)
                if kind == 0:  # fast path
                    autocommit(conn, "UPDATE acc SET n = n + 1 WHERE id = %s", (a,), "n")
                elif kind == 1:  # read-modify-write in an implicit transaction
                    autocommit(conn, "UPDATE acc SET n = n + 1 WHERE id = %s AND n >= 0", (a,), "n")
                elif kind == 2:  # move one unit between two rows (sum unchanged)
                    if a != b:
                        txn(conn, lambda: (q(conn, "UPDATE acc SET n = n - 1 WHERE id = %s", (a,)),
                                           q(conn, "UPDATE acc SET n = n + 1 WHERE id = %s", (b,))))
                elif kind == 3 and x_ready.is_set():
                    autocommit(conn, "UPDATE acc SET x = x + 1 WHERE id = %s", (a,), "x")
                else:  # an increment of both in one transaction, rows read first
                    def body():
                        q(conn, "SELECT n FROM acc WHERE id = %s", (a,))
                        q(conn, "UPDATE acc SET n = n + 1 WHERE id = %s", (a,))
                        if x_ready.is_set():
                            q(conn, "UPDATE acc SET x = x + 1 WHERE id = %s", (b,))
                            return 2
                        return 1
                    r = txn(conn, body)
                    if r:
                        bump("n")
                        if r == 2:
                            bump("x")
                bump("ops")

        def reader(w):
            conn = self.c(w)
            # READ COMMITTED: scans never abort (at SERIALIZABLE a scan of a
            # hot table may exhaust its retries, schema change or not)
            q(conn, "SET SESSION transaction_isolation = 'READ-COMMITTED'")
            while not stop.is_set():
                try:
                    q(conn, "SELECT * FROM acc WHERE id = %s", (random.randint(1, 64),))
                    q(conn, "SELECT COUNT(*) FROM acc WHERE n > 1")
                except pymysql.MySQLError as e:
                    with lock:
                        st["errors"].append(f"read: {e}")

        def ddl():
            conn = self.c(0)
            steps = ["ALTER TABLE acc ADD COLUMN x INT NOT NULL DEFAULT 0"]
            k = 0
            while len(steps) < 400:
                k += 1
                steps += [f"ALTER TABLE acc ADD COLUMN y{k} VARCHAR(10) DEFAULT 'y' AFTER id",
                          f"ALTER TABLE acc ADD INDEX ix{k} (x, n)",
                          f"ALTER TABLE acc MODIFY tag VARCHAR({30 + k})",
                          f"ALTER TABLE acc RENAME COLUMN tag TO tag{k}",
                          f"ALTER TABLE acc RENAME COLUMN tag{k} TO tag",
                          f"ALTER TABLE acc DROP COLUMN y{k}",
                          f"ALTER TABLE acc MODIFY n {'BIGINT' if k % 2 else 'INT'} NOT NULL DEFAULT 0",
                          f"ALTER TABLE acc DROP INDEX ix{k}, RENAME INDEX kn TO kn{k}",
                          f"ALTER TABLE acc RENAME INDEX kn{k} TO kn"]
            for s in steps:
                if stop.is_set() or self.args.no_ddl:
                    break
                try:
                    q(conn, s)
                    bump("ddl")
                    if s.startswith("ALTER TABLE acc ADD COLUMN x"):
                        x_ready.set()
                except pymysql.MySQLError as e:
                    with lock:
                        st["errors"].append(f"ddl {s}: {e}")
                time.sleep(0.05)

        threads = [threading.Thread(target=writer, args=(w,), daemon=True) for w in range(nthreads)]
        threads += [threading.Thread(target=reader, args=(w,), daemon=True) for w in range(2)]
        threads.append(threading.Thread(target=ddl, daemon=True))
        for t in threads:
            t.start()
        time.sleep(seconds)
        stop.set()
        for t in threads:
            t.join(60)
        sums = q(self.c(), "SELECT SUM(n), SUM(x), COUNT(*) FROM acc")[0]
        print(f"    {st['ops']} writes, {st['ddl']} schema changes, {st['retries']} transaction retries, "
              f"{st['n_unknown'] + st['x_unknown']} unknown outcomes; SUM(n) {sums[0]} (acknowledged {st['n']}), "
              f"SUM(x) {sums[1]} (acknowledged {st['x']})")
        kinds = {}
        for e in st["errors"]:
            kinds.setdefault(e, 0)
            kinds[e] += 1
        # Preserve the actual storage cause; an ALTER prefix alone cannot
        # distinguish a known refusal from an unresolved effect.
        for e, n in sorted(kinds.items(), key=lambda x: -x[1]):
            print(f"      error x{n}: {e}")
        self.check(not st["errors"], f"{len(st['errors'])} errors")
        self.check(st["ddl"] >= 8 or self.args.no_ddl, "schema changes ran")
        self.check(st["n"] <= sums[0] <= st["n"] + st["n_unknown"], "no increment of n lost or duplicated")
        self.check(st["x"] <= sums[1] <= st["x"] + st["x_unknown"], "no increment of x lost or duplicated")
        self.check(sums[2] == 64, "rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True, help="host:port[,host:port...]")
    ap.add_argument("--seconds", type=float, default=20)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--kv", default="", help="one writable leader per shard for independent engine-change native audits")
    ap.add_argument("--only", default="")
    ap.add_argument("--no-ddl", action="store_true", help="the online test without schema changes")
    args = ap.parse_args()
    c = connect(args.tarn.split(",")[0], db=None)
    q(c, f"DROP DATABASE IF EXISTS {DB}")
    q(c, f"CREATE DATABASE {DB}")
    t = T(args)
    names = sorted(n for n in dir(t) if n.startswith("test_") and (not args.only or args.only in n))
    for n in names:
        before = t.failures
        t0 = time.time()
        try:
            getattr(t, n)()
        except Exception as e:  # noqa: BLE001
            t.failures += 1
            print(f"    EXCEPTION: {type(e).__name__}: {e}")
            traceback.print_exc()
        print(f"{'ok  ' if t.failures == before else 'FAIL'} {n} ({time.time() - t0:.1f}s)")
    print(f"{len(names)} tests, {t.failures} failures")
    sys.exit(1 if t.failures else 0)


if __name__ == "__main__":
    main()
