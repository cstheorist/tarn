#!/usr/bin/env python3
"""Lazy secondary-index semantics (#82), with controlled delivery lag.

Requires an isolated cluster with test_hooks=true. Native controls are
bounded and restored in finally; they never substitute for a barrier.
    lazytest.py --tarn HOST:PORT --kv HOST:PORT,HOST:PORT [--ref HOST:PORT]
"""
import argparse
import json
import os
import pathlib
import subprocess
import sys
import time

import pymysql

from movetest import bucket_of

DB = "lazytest"
TARANTOOL = os.environ.get("TARANTOOL", "tarantool")


def connect(hp, db=DB, autocommit=True):
    host, port = hp.rsplit(":", 1)
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db,
                           autocommit=autocommit, charset="utf8mb4", connect_timeout=3,
                           read_timeout=15, write_timeout=5)


def q(c, sql, args=None):
    with c.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall() if cur.description else cur.rowcount


def stat(c, name):
    values = dict(q(c, "SHOW STATUS LIKE %s", (name,)))
    if name not in values:
        raise AssertionError(f"missing status witness {name}")
    return int(values[name])


def _native(node, mode, text, args, timeout=12):
    script = pathlib.Path(__file__).with_name("lazyrpc.lua")
    r = subprocess.run([TARANTOOL, str(script), node, mode, text, json.dumps(args)],
                       stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=timeout)
    if r.returncode:
        raise RuntimeError(f"storage {mode} on {node}: {r.stderr[-2000:]}")
    return json.loads(r.stdout.strip().splitlines()[-1])


def rpc(node, proc, args, timeout=12):
    return _native(node, "call", proc, args, timeout)


def ev(node, script, args=None, timeout=12):
    return _native(node, "eval", script, [] if args is None else args, timeout)


def wait_empty(nodes, timeout=10):
    deadline = time.monotonic() + timeout
    while True:
        samples = [rpc(n, "tarn_lazy_stats", []) for n in nodes]
        if all(s[0] == "ok" and s[1]["depth"] == 0 and s[1]["inflight"] == 0
               and s[1]["reserved_entries"] == 0 for s in samples):
            return
        if time.monotonic() >= deadline:
            raise AssertionError(f"outboxes did not drain: {samples}")
        time.sleep(0.02)


def pause(nodes):
    wait_empty(nodes)
    for n in nodes:
        r = rpc(n, "tarn_lazy_control", ["pause"])
        if r != ["ok", True, 0, 0]:
            raise AssertionError(f"delivery pause was not quiescent: {r}")


def resume(nodes):
    errors = []
    for n in nodes:
        try:
            r = rpc(n, "tarn_lazy_control", ["resume"])
            if r[0] != "ok":
                errors.append((n, r))
        except Exception as e:  # cleanup must attempt every owned node
            errors.append((n, str(e)))
    if errors:
        raise AssertionError(f"could not restore delivery: {errors}")


class T:
    def __init__(self, args):
        self.args = args
        self.nodes = args.kv.split(",")
        self.failures = 0
        self.skipped = 0

    def c(self, **kw):
        return connect(self.args.tarn, **kw)

    def table(self, name="t", lazy=True, route_parts=1):
        c = self.c()
        q(c, f"DROP TABLE IF EXISTS {name}")
        tag = (" COMMENT 'tarn:lazy'" if route_parts == 1 else f" COMMENT 'tarn:lazy={route_parts}'") if lazy else ""
        q(c, f"CREATE TABLE {name} (tenant_id INT NOT NULL,id INT NOT NULL,k INT,n INT,v VARCHAR(32),"
             f"PRIMARY KEY(tenant_id,id),KEY lk(k,n,id){tag}) PARTITION BY KEY(tenant_id) "
             f"ENGINE={self.args.engine}")
        return c

    def lazy(self, c, sql, args=None):
        before = stat(c, "Tarn_lazy_reads")
        rows = q(c, sql, args)
        assert stat(c, "Tarn_lazy_reads") == before + 1, "query did not complete on the lazy path"
        return rows

    def caught_up(self, c):
        q(c, "SET SESSION tarn_lazy_read_your_writes=ON")
        q(c, "SET SESSION tarn_lazy_wait_timeout=5")
        wait_empty(self.nodes)

    def consistency(self, c, sql, selected, reason=None):
        plan = json.loads(q(c, "EXPLAIN FORMAT=JSON " + sql)[0][0])["plan"]
        policy = plan["read_consistency"]
        assert policy["selected"] == selected, policy
        if reason is not None:
            assert policy["reason"] == reason, policy
        return policy

    def test_consistency_override_retains_history_and_prepared_plan(self):
        with self.table("t_policy") as c, self.c() as other:
            sql = "SELECT id,n FROM t_policy WHERE k=7 ORDER BY n,id"
            q(c, "INSERT INTO t_policy VALUES (1,1,7,1,'old'),(2,2,7,2,'kept')")
            self.caught_up(c)
            assert self.lazy(c, sql) == ((1, 1), (2, 2))
            create = q(c, "SHOW CREATE TABLE t_policy")
            q(c, "SET tarn_lazy_read_your_writes=OFF")
            q(c, "PREPARE policy_q FROM 'SELECT id,n FROM t_policy WHERE k=? ORDER BY n,id'")
            q(c, "SET @k=7")
            pause(self.nodes)
            try:
                q(c, "UPDATE t_policy SET k=8 WHERE tenant_id=1 AND id=1")
                q(c, "INSERT INTO t_policy VALUES (3,3,7,3,'new')")
                # The obsolete membership is rejected; the newly matching
                # row has no delivered candidate and is explicitly absent.
                assert self.lazy(c, "EXECUTE policy_q USING @k") == ((2, 2),)
                assert self.lazy(c, sql + " LIMIT 1 OFFSET 1") == ()
                self.consistency(c, sql, "eventual")
                touched = stat(c, "Tarn_lazy_session_touched_buckets")
                assert touched >= 2, "fixture needs cumulative source buckets"
                # Another session's causal barrier has no own writes to
                # wait for. It cannot turn an eventual query into strong SQL.
                q(other, "SET tarn_lazy_read_your_writes=ON")
                assert self.lazy(other, sql) == ((2, 2),)
                assert q(other, "SELECT @@tarn_lazy_consistency") == (("eventual",),)

                q(c, "SET SESSION tarn_lazy_consistency='STRONG'")
                q(c, "SET tarn_lazy_read_your_writes=ON,tarn_lazy_wait_timeout=0")
                self.consistency(c, sql, "strong", "session_strong_policy")
                before = stat(c, "Tarn_lazy_reads")
                # Same prepared statement and cached physical plan, new
                # execution-time policy. No outbox wait/rebuild is needed.
                assert q(c, "EXECUTE policy_q USING @k") == ((2, 2), (3, 3))
                assert q(c, sql + " LIMIT 1 OFFSET 1") == ((3, 3),)
                assert q(c, "SELECT id FROM t_policy WHERE k=8") == ((1,),)
                assert stat(c, "Tarn_lazy_reads") == before
                assert stat(c, "Tarn_lazy_session_touched_buckets") == touched
                assert q(c, "SHOW CREATE TABLE t_policy") == create
                # The override selects the ordinary isolation contract,
                # including READ COMMITTED, rather than a new snapshot mode.
                q(c, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                assert q(c, sql) == ((2, 2), (3, 3))
                q(c, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                q(c, "SET tarn_lazy_consistency='eventual'")
                self.consistency(c, sql, "eventual")
                try:
                    q(c, "EXECUTE policy_q USING @k")
                except pymysql.MySQLError as e:
                    assert e.args[0] == 1205, e
                else:
                    raise AssertionError("policy switch discarded unfulfilled catch-up history")
                assert stat(c, "Tarn_lazy_session_touched_buckets") == touched
                q(c, "BEGIN")
                self.consistency(c, sql, "strong", "transaction")
                assert q(c, sql) == ((2, 2), (3, 3))
                q(c, "ROLLBACK")
                self.consistency(c, sql + " FOR UPDATE", "strong", "locking_read")
                assert q(c, sql + " FOR UPDATE") == ((2, 2), (3, 3))
            finally:
                q(c, "ROLLBACK")
                resume(self.nodes)
            self.caught_up(c)
            assert self.lazy(c, "EXECUTE policy_q USING @k") == ((2, 2), (3, 3))
            assert stat(c, "Tarn_lazy_session_touched_buckets") == 0
            q(c, "DEALLOCATE PREPARE policy_q")

    def test_consistency_does_not_activate_ordinary_indexes(self):
        with self.table("t_plain_policy", lazy=False) as c:
            q(c, "INSERT INTO t_plain_policy VALUES (1,1,7,1,'plain')")
            sql = "SELECT id FROM t_plain_policy WHERE k=7"
            before = stat(c, "Tarn_lazy_reads")
            self.consistency(c, sql, "strong")
            assert q(c, sql) == ((1,),)
            assert stat(c, "Tarn_lazy_reads") == before
            q(c, "SET tarn_lazy_consistency='strong'")
            assert q(c, "SELECT @@tarn_lazy_consistency") == (("strong",),)
            q(c, "SET tarn_lazy_consistency=DEFAULT")
            assert q(c, "SHOW VARIABLES LIKE 'tarn_lazy_consistency'") == (("tarn_lazy_consistency", "eventual"),)

    def test_consistency_result_budget_restarts_complete_strong_read(self):
        with self.c() as c:
            q(c, "DROP TABLE IF EXISTS t_policy_budget")
            q(c, "CREATE TABLE t_policy_budget (tenant_id INT NOT NULL,id INT NOT NULL,k INT,n INT,v MEDIUMBLOB,"
                 "PRIMARY KEY(tenant_id,id),KEY lk(k,n,id) COMMENT 'tarn:lazy') PARTITION BY KEY(tenant_id) "
                 f"ENGINE={self.args.engine}")
            # The real lazy result budget counts fetched base tuples even
            # for a narrow projection: 10 MiB of them, over the default 8 MiB
            # (an eighth of the statement's 64 MiB memory budget), force the
            # actual strong restart, which fits the budget; a clipped
            # candidate prefix must never count as success. (One 64 KiB row
            # per statement: a text statement is charged 128 bytes per byte.)
            # Spread over 11 tenants, the rows usually span both shards, whose
            # pages the restart reads concurrently: a read whose reservation
            # does not fit waits for the other's reply (#238).
            payload = b'x' * (64 * 1024)
            with c.cursor() as cur:
                for i in range(1, 161):
                    cur.execute("INSERT INTO t_policy_budget VALUES (%s,%s,%s,%s,%s)", (i % 11, i, 7, i, payload))
            self.caught_up(c)
            sql = "SELECT id FROM t_policy_budget WHERE k=7 ORDER BY n,id"
            self.consistency(c, sql, "eventual")
            reads, falls = stat(c, "Tarn_lazy_reads"), stat(c, "Tarn_lazy_fallbacks")
            assert q(c, sql) == tuple((i,) for i in range(1, 161))
            assert stat(c, "Tarn_lazy_reads") == reads
            assert stat(c, "Tarn_lazy_fallbacks") == falls + 1
            q(c, "SET tarn_lazy_consistency='strong'")
            assert q(c, sql) == tuple((i,) for i in range(1, 161))

    def test_many_small_rows_complete_or_fall_back(self):
        # A lazy read of many small rows (#247). Its candidate pages and
        # rechecks are refunded once consumed, and its limit counts kept rows
        # as charged. So the read completes on the lazy path or falls back to
        # the strong one; it does not exhaust the statement's memory budget,
        # as 30,000 rows did when every page stayed charged.
        with self.table("t_small") as c:
            rows = [(i % 13, i, 3, i, None) for i in range(1, 30001)]
            with c.cursor() as cur:
                for first in range(0, len(rows), 500):
                    for attempt in range(40):
                        try:
                            cur.executemany("INSERT INTO t_small VALUES (%s,%s,%s,%s,%s)", rows[first:first + 500])
                            break
                        except pymysql.MySQLError as e:  # vinyl admission, before any effect
                            if e.args[0] != 1205 or "Vinyl" not in str(e.args[1]) or attempt == 39:
                                raise
                            time.sleep(min(0.5, 0.05 * (attempt + 1)))
            self.caught_up(c)
            sql = "SELECT tenant_id,id FROM t_small WHERE k=3 ORDER BY n,id"
            q(c, "SET tarn_lazy_consistency='strong'")
            strong = q(c, sql)
            assert len(strong) == 30000, len(strong)
            q(c, "SET tarn_lazy_consistency='eventual'")
            reads, falls = stat(c, "Tarn_lazy_reads"), stat(c, "Tarn_lazy_fallbacks")
            assert q(c, sql) == strong
            assert stat(c, "Tarn_lazy_reads") + stat(c, "Tarn_lazy_fallbacks") == reads + falls + 1
            # A page of them stays on the lazy path.
            assert self.lazy(c, sql + " LIMIT 500") == strong[:500]

    def test_bootstrap_and_next_writes(self):
        with self.table("t_boot", lazy=False) as c:
            q(c, "INSERT INTO t_boot VALUES (1,1,7,1,'a'),(2,2,7,2,'b'),(3,3,NULL,3,'c')")
            q(c, "ALTER TABLE t_boot DROP INDEX lk,ADD INDEX lazy_k(k,n,id) COMMENT 'tarn:lazy'")
            # Baseline is complete when ALTER returns, without relying on
            # a session RYW wait or a later sleep.
            assert self.lazy(c, "SELECT id,v FROM t_boot WHERE k=7 ORDER BY n,id") == ((1, "a"), (2, "b"))
            assert self.lazy(c, "SELECT id,v FROM t_boot WHERE k IS NULL ORDER BY n,id") == ((3, "c"),)
            q(c, "INSERT INTO t_boot VALUES (4,4,7,4,'d')")
            self.caught_up(c)
            assert self.lazy(c, "SELECT id FROM t_boot WHERE k=7 ORDER BY n,id") == ((1,), (2,), (4,))

    def test_composite_routing_merge_rechecks_and_migration(self):
        with self.table("t_composite", route_parts=2) as c:
            rows = [(i % 11, i, 7, i // 2, f"v{i}") for i in range(1, 641)]
            with c.cursor() as cur:
                cur.executemany("INSERT INTO t_composite VALUES (%s,%s,%s,%s,%s)", rows)
            self.caught_up(c)
            sql = "SELECT id,n FROM t_composite WHERE k=7 ORDER BY n,id"
            expected = tuple((r[1], r[3]) for r in rows)
            assert self.lazy(c, sql) == expected
            assert self.lazy(c, sql + " LIMIT 11 OFFSET 257") == expected[257:268]
            assert self.lazy(c, sql.replace("n,id", "n DESC,id DESC") + " LIMIT 9 OFFSET 258") == expected[::-1][258:267]
            plan = json.loads(q(c, "EXPLAIN FORMAT=JSON " + sql)[0][0])["plan"]["lazy_index_alternative"]
            assert plan["routing_parts"] == 2 and plan["routing"]["shard_count"] == len(self.nodes), plan
            exact = "SELECT id,n FROM t_composite WHERE k=7 AND n=128 ORDER BY id"
            assert self.lazy(c, exact) == ((256, 128), (257, 128))
            keyset = "SELECT id,n FROM t_composite WHERE k=7 AND (n>128 OR (n=128 AND id>257)) ORDER BY n,id LIMIT 7"
            assert self.lazy(c, keyset) == expected[257:264]
            # One failed owner must refuse the whole statement, even when
            # another owner has enough rows to satisfy LIMIT already.
            failing = self.nodes[-1]
            ev(failing, "assert(tarn.cfg.test_hooks and not __lazy185_select); __lazy185_select=tarn_lazy_select; "
                        "tarn_lazy_select=function() return {'error',1041,'owned incomplete-prefix control'} end; return true")
            try:
                try:
                    self.lazy(c, sql + " LIMIT 1")
                except pymysql.MySQLError as e:
                    assert e.args[0] == 1041, e
                else:
                    raise AssertionError("partial owner set returned a successful LIMIT")
            finally:
                ev(failing, "tarn_lazy_select=assert(__lazy185_select); __lazy185_select=nil; return true")
            plan = json.loads(q(c, "EXPLAIN FORMAT=JSON " + exact)[0][0])["plan"]["lazy_index_alternative"]
            assert plan["routing"]["status"] == "single_destination" and plan["routing"]["shard_count"] == 1, plan
            q(c, "SET tarn_lazy_read_your_writes=OFF")
            pause(self.nodes)
            try:
                q(c, "UPDATE t_composite SET n=n+1000 WHERE id<=500")
                q(c, "DELETE FROM t_composite WHERE id=501")
                assert self.lazy(c, sql + " LIMIT 5 OFFSET 3") == expected[504:509]
            finally:
                resume(self.nodes)
            self.caught_up(c)
            # A new incarnation backfills current committed rows. The old
            # generation is retained until the new one is ready.
            q(c, "ALTER TABLE t_composite ADD KEY replacement(k,n,id) COMMENT 'tarn:lazy=3'")
            q(c, "ALTER TABLE t_composite DROP KEY lk")
            actual = self.lazy(c, sql)
            wanted = sorted(((i, n + (1000 if i <= 500 else 0)) for i, n in expected if i != 501), key=lambda r: (r[1], r[0]))
            assert actual == tuple(wanted)

    def test_lagged_insert_strong_contexts_and_unbound_prefix(self):
        with self.table() as c:
            pause(self.nodes)
            try:
                q(c, "INSERT INTO t VALUES (1,1,7,1,'one'),(2,2,7,2,'two')")
                assert self.lazy(c, "SELECT id FROM t WHERE k=7 ORDER BY n,id") == ()
                before = stat(c, "Tarn_lazy_reads")
                q(c, "BEGIN")
                assert q(c, "SELECT id FROM t WHERE k=7 ORDER BY n,id") == ((1,), (2,))
                q(c, "ROLLBACK")
                assert q(c, "SELECT id FROM t WHERE k=7 ORDER BY n,id FOR UPDATE") == ((1,), (2,))
                # A tail predicate does not determine the lazy routing
                # bucket: this MUST retain the strong local/scatter path.
                assert q(c, "SELECT id FROM t WHERE n>=1 ORDER BY id") == ((1,), (2,))
                assert q(c, "SELECT COUNT(*) FROM t WHERE k=7") == ((2,),)
                q(c, "UPDATE t SET v='changed' WHERE k=7")
                assert q(c, "SELECT id,v FROM t WHERE id=1 AND tenant_id=1") == ((1, "changed"),)
                assert stat(c, "Tarn_lazy_reads") == before, "strong context escaped into lazy reads"
            finally:
                q(c, "ROLLBACK")
                resume(self.nodes)

    def test_catch_up_follows_a_moved_bucket(self):
        # A session's obligation in a bucket that moves. The move cannot
        # flip while the bucket's lazy delivery is pending, and the session
        # keeps waiting for the obligation meanwhile; once delivery resumes
        # the move goes through, and catch-up then follows the bucket to its
        # new owner for the session's next write too.
        with self.table("t_move") as c, self.c() as admin:
            sql = "SELECT id FROM t_move WHERE k=7 ORDER BY id"
            count = ev(self.nodes[0], "return box.space._tarn_meta:get{'buckets'}.value.count")
            bucket = bucket_of(1, count)
            shard = {n: ev(n, "return box.space._tarn_meta:get{'shard_id'}.value") for n in self.nodes}

            def owner():
                states = {shard[n]: ev(n, f"local t = box.space._tarn_buckets:get{{{bucket}}} return t and t.state")
                          for n in self.nodes}
                active = [s for s, state in states.items() if state == "active"]
                assert len(active) == 1, states
                return active[0]

            def catch_up_refused():
                try:
                    q(c, sql)
                except pymysql.MySQLError as e:
                    assert e.args[0] == 1205, e
                else:
                    raise AssertionError("catch-up returned before the session's obligation was delivered")

            q(c, "INSERT INTO t_move VALUES (1,1,7,1,'a')")
            self.caught_up(c)
            assert self.lazy(c, sql) == ((1,),)
            source = owner()
            target = next(s for s in shard.values() if s != source)
            pause(self.nodes)
            try:
                q(c, "INSERT INTO t_move VALUES (1,2,7,2,'b')")
                q(c, "SET SESSION tarn_lazy_wait_timeout=0")
                catch_up_refused()
                # The source drains the bucket's delivery before it flips.
                try:
                    q(admin, f"TARN MOVE BUCKET {bucket} TO SHARD {target}")
                except pymysql.MySQLError as e:
                    assert e.args[0] == 1205 and "lazy delivery has not drained" in e.args[1], e
                else:
                    raise AssertionError("a bucket moved with its lazy delivery pending")
                assert owner() == source
                catch_up_refused()
            finally:
                resume(self.nodes)
            q(admin, f"TARN MOVE BUCKET {bucket} TO SHARD {target}")
            assert owner() == target
            q(c, "SET SESSION tarn_lazy_wait_timeout=5")
            assert self.lazy(c, sql) == ((1,), (2,))
            pause(self.nodes)
            try:
                q(c, "INSERT INTO t_move VALUES (1,3,7,3,'c')")
                q(c, "SET SESSION tarn_lazy_wait_timeout=0")
                catch_up_refused()
            finally:
                resume(self.nodes)
            q(c, "SET SESSION tarn_lazy_wait_timeout=5")
            assert self.lazy(c, sql) == ((1,), (2,), (3,))
            q(admin, f"TARN MOVE BUCKET {bucket} TO SHARD {source}")

    def test_changed_key_still_matches_where(self):
        with self.table("t_key") as c:
            q(c, "INSERT INTO t_key VALUES (1,1,7,1,'a'),(2,2,7,3,'b')")
            self.caught_up(c)
            assert self.lazy(c, "SELECT id,n FROM t_key WHERE k=7 ORDER BY n,id LIMIT 1") == ((1, 1),)
            q(c, "SET SESSION tarn_lazy_read_your_writes=OFF")
            pause(self.nodes)
            try:
                q(c, "UPDATE t_key SET n=5 WHERE tenant_id=1 AND id=1")
                # The old candidate is n=1, but the current row n=5 STILL
                # satisfies WHERE. WHERE-only validation returns the wrong
                # first row if LIMIT is applied before candidate-key check.
                assert self.lazy(c, "SELECT id,n FROM t_key WHERE k=7 AND n>=0 ORDER BY n,id LIMIT 1") == ((2, 3),)
                q(c, "DELETE FROM t_key WHERE tenant_id=2 AND id=2")
                assert self.lazy(c, "SELECT id,n FROM t_key WHERE k=7 ORDER BY n,id") == ()
            finally:
                resume(self.nodes)
            self.caught_up(c)
            assert self.lazy(c, "SELECT id,n FROM t_key WHERE k=7 ORDER BY n,id") == ((1, 5),)

    def test_limit_offset_refills_more_than_two_pages(self):
        with self.table("t_pages") as c:
            rows = [(i % 7, i, 7, i, f"v{i}") for i in range(1, 281)]
            with c.cursor() as cur:
                cur.executemany("INSERT INTO t_pages VALUES (%s,%s,%s,%s,%s)", rows)
            self.caught_up(c)
            self.lazy(c, "SELECT id FROM t_pages WHERE k=7 ORDER BY n,id LIMIT 1")
            q(c, "SET SESSION tarn_lazy_read_your_writes=OFF")
            pause(self.nodes)
            try:
                q(c, "UPDATE t_pages SET n=n+1000 WHERE id<=260")
                expected = tuple((i, i) for i in range(265, 270))
                assert self.lazy(c, "SELECT id,n FROM t_pages WHERE k=7 AND n>=0 ORDER BY n,id LIMIT 5 OFFSET 4") == expected
                expected = tuple((i, i) for i in range(280, 275, -1))
                assert self.lazy(c, "SELECT id,n FROM t_pages WHERE k=7 AND n>=0 ORDER BY n DESC,id DESC LIMIT 5") == expected
            finally:
                resume(self.nodes)
            self.caught_up(c)
            got = self.lazy(c, "SELECT tenant_id,id FROM t_pages WHERE k=7 ORDER BY n,id")
            assert len(got) == 280 and len(set(got)) == 280, "old/new assignments duplicated a primary key"

    def test_nullable_ci_keys_raw_pk_and_payload(self):
        c = self.c()
        try:
            q(c, "DROP TABLE IF EXISTS t_ci")
            q(c, "CREATE TABLE t_ci (tenant_id INT,id VARCHAR(20) COLLATE utf8mb4_unicode_ci,"
                 "k VARCHAR(20) COLLATE utf8mb4_general_ci,n INT,v VARCHAR(32),PRIMARY KEY(tenant_id,id),"
                 "KEY lk(k,n) COMMENT 'tarn:lazy') PARTITION BY KEY(tenant_id) "
                 f"ENGINE={self.args.engine}")
            q(c, "INSERT INTO t_ci VALUES (1,'É ','Á ',1,'雪'),(2,'ß','a',2,NULL),(3,'三',NULL,3,'null key')")
            self.caught_up(c)
            assert self.lazy(c, "SELECT id,k,n,v FROM t_ci WHERE k='a' ORDER BY n") == (("É ", "Á ", 1, "雪"), ("ß", "a", 2, None))
            assert self.lazy(c, "SELECT id,v FROM t_ci WHERE k IS NULL ORDER BY n") == (("三", "null key"),)
            q(c, "SET SESSION tarn_lazy_read_your_writes=OFF")
            pause(self.nodes)
            try:
                # Equal canonical keys can change raw spelling and payload.
                # Base fetch must display the current SQL values.
                q(c, "UPDATE t_ci SET id='e',k='A',v='new 雪' WHERE tenant_id=1 AND id='é'")
                assert self.lazy(c, "SELECT id,k,v FROM t_ci WHERE k='a' ORDER BY n LIMIT 1") == (("e", "A", "new 雪"),)
            finally:
                resume(self.nodes)
        finally:
            c.close()

    def test_variables_and_prepared_session_state(self):
        with self.table("t_session") as c:
            assert q(c, "SELECT @@tarn_lazy_read_your_writes,@@tarn_lazy_wait_timeout") == ((0, 5),)
            for sql, expected in [("SET GLOBAL tarn_lazy_read_your_writes=ON", 1228),
                                  ("SET GLOBAL tarn_lazy_consistency='strong'", 1228),
                                  ("SET tarn_lazy_consistency='causal'", 1231),
                                  ("SET tarn_lazy_consistency=1", 1231),
                                  ("SET tarn_lazy_consistency=NULL", 1231),
                                  ("SET GLOBAL tarn_lazy_wait_timeout=5", 1228),
                                  ("SET tarn_lazy_wait_timeout=-1", 1231),
                                  ("SET tarn_lazy_wait_timeout=61", 1231),
                                  ("SET tarn_lazy_wait_timeout=0.5", 1232),
                                  ("SET tarn_lazy_read_your_writes=2", 1231)]:
                try:
                    q(c, sql)
                except pymysql.MySQLError as e:
                    assert e.args[0] == expected, (sql, e)
                else:
                    raise AssertionError(f"invalid variable value accepted: {sql}")
            q(c, "PREPARE lazy_q FROM 'SELECT id FROM t_session WHERE k=? ORDER BY n,id'")
            q(c, "SET @k=7")
            pause(self.nodes)
            try:
                # Writes while OFF must remain covered after enabling.
                q(c, "INSERT INTO t_session VALUES (1,1,7,1,'first')")
                q(c, "INSERT INTO t_session VALUES (2,2,7,2,'second')")
                assert stat(c, "Tarn_lazy_session_touched_buckets") >= 1
                q(c, "SET tarn_lazy_read_your_writes=ON,tarn_lazy_wait_timeout=0")
                before = stat(c, "Tarn_lazy_fallbacks")
                try:
                    q(c, "EXECUTE lazy_q USING @k")
                except pymysql.MySQLError as e:
                    assert e.args[0] == 1205, e
                else:
                    raise AssertionError("nonblocking catch-up served a row while delivery was paused")
                assert stat(c, "Tarn_lazy_fallbacks") == before
                assert stat(c, "Tarn_lazy_session_touched_buckets") >= 1
            finally:
                resume(self.nodes)
            q(c, "SET tarn_lazy_wait_timeout=DEFAULT")
            self.caught_up(c)
            assert self.lazy(c, "EXECUTE lazy_q USING @k") == ((1,), (2,))
            assert stat(c, "Tarn_lazy_session_pending_apply") == 0
            try:
                q(c, "INSERT INTO t_session VALUES (1,1,7,99,'duplicate')")
            except pymysql.MySQLError as e:
                assert e.args[0] == 1062, e
            else:
                raise AssertionError("duplicate primary key accepted")
            assert stat(c, "Tarn_lazy_session_strong_fallback") == 0
            assert self.lazy(c, "EXECUTE lazy_q USING @k") == ((1,), (2,))
            q(c, "DEALLOCATE PREPARE lazy_q")

    def test_metadata_refusals_and_drop_recreate(self):
        with self.table("t_meta") as c:
            create = q(c, "SHOW CREATE TABLE t_meta")[0][1]
            assert "COMMENT 'tarn:lazy'" in create
            assert any(r[12] == "tarn:lazy" for r in q(c, "SHOW INDEX FROM t_meta"))
            q(c, "INSERT INTO t_meta VALUES (1,1,7,1,'a')")
            self.caught_up(c)
            assert self.lazy(c, "SELECT id FROM t_meta WHERE k=7 ORDER BY n,id") == ((1,),)
            q(c, "ALTER TABLE t_meta DROP INDEX lk")
            q(c, "ALTER TABLE t_meta ADD INDEX lk(k,n,id) COMMENT 'tarn:lazy'")
            assert self.lazy(c, "SELECT id FROM t_meta WHERE k=7 ORDER BY n,id") == ((1,),)
            for sql in ["ALTER TABLE t_meta ADD UNIQUE INDEX bad(k) COMMENT 'tarn:lazy'",
                        "CREATE TABLE bad_lazy (id INT PRIMARY KEY,k DOUBLE,KEY lk(k) COMMENT 'tarn:lazy')",
                        "CREATE TABLE bad_lazy (id DECIMAL(20,0),k INT,PRIMARY KEY(id),KEY lk(k) COMMENT 'tarn:lazy')"]:
                try:
                    q(c, sql)
                except pymysql.MySQLError as e:
                    assert e.args[0] == 1235, (sql, e)
                else:
                    raise AssertionError(f"unsupported lazy identity accepted: {sql}")

    def test_outbox_admission_rolls_back_base_write(self):
        with self.table("t_budget") as c:
            pause(self.nodes)
            saved = {}
            try:
                for n in self.nodes:
                    saved[n] = ev(n, "assert(tarn.cfg.test_hooks); local old=tarn.cfg.lazy_outbox_entries; "
                                     "tarn.cfg.lazy_outbox_entries=0; return old")
                before = [rpc(n, "tarn_lazy_stats", [])[1] for n in self.nodes]
                try:
                    q(c, "INSERT INTO t_budget VALUES (1,1,7,1,'rejected')")
                except pymysql.MySQLError as e:
                    assert e.args[0] == 1041, e
                else:
                    raise AssertionError("zero outbox budget accepted a lazy source write")
                assert q(c, "SELECT id,v FROM t_budget WHERE tenant_id=1 AND id=1") == ()
                assert stat(c, "Tarn_lazy_session_strong_fallback") == 0
                # Exercise the tracked COMMIT path as well as the direct
                # autocommit CALL: the entire failed transaction rolls back.
                q(c, "BEGIN")
                q(c, "INSERT INTO t_budget VALUES (1,1,7,1,'first'),(2,2,7,2,'second')")
                try:
                    q(c, "COMMIT")
                except pymysql.MySQLError as e:
                    assert e.args[0] == 1041, e
                else:
                    raise AssertionError("zero outbox budget accepted a tracked commit")
                q(c, "ROLLBACK")
                assert q(c, "SELECT id FROM t_budget ORDER BY id") == ()
                assert stat(c, "Tarn_lazy_session_strong_fallback") == 0
                after = [rpc(n, "tarn_lazy_stats", [])[1] for n in self.nodes]
                for a, b in zip(before, after):
                    for key in ("depth", "bytes", "reserved_entries", "reserved_bytes"):
                        assert a[key] == b[key], (key, a, b)
            finally:
                try:
                    for n, old in saved.items():
                        ev(n, "assert(tarn.cfg.test_hooks); local v=...; if v==box.NULL then v=nil end; "
                              "tarn.cfg.lazy_outbox_entries=v; return true", [old])
                finally:
                    resume(self.nodes)
            # A routine rollback must preserve this SAME session's ability
            # to catch up and serve the next eligible read lazily.
            q(c, "INSERT INTO t_budget VALUES (1,1,7,1,'accepted')")
            self.caught_up(c)
            assert self.lazy(c, "SELECT id,v FROM t_budget WHERE k=7 ORDER BY n,id") == ((1, "accepted"),)

    def test_truncate_rotates_incarnation_and_reinsert(self):
        with self.table("t_clear") as c:
            q(c, "INSERT INTO t_clear VALUES (1,1,7,1,'old'),(2,2,7,2,'other')")
            self.caught_up(c)
            assert self.lazy(c, "SELECT id,v FROM t_clear WHERE k=7 ORDER BY n,id") == ((1, "old"), (2, "other"))
            q(c, "SET tarn_lazy_read_your_writes=OFF")
            pause(self.nodes)
            try:
                # Leave exact old-generation assignments pending while the
                # clear publishes a new empty ready target.
                q(c, "UPDATE t_clear SET n=11 WHERE tenant_id=1 AND id=1")
                q(c, "TRUNCATE TABLE t_clear")
                assert self.lazy(c, "SELECT id,v FROM t_clear WHERE k=7 ORDER BY n,id") == ()
                q(c, "INSERT INTO t_clear VALUES (1,1,7,5,'new')")
                assert q(c, "SELECT id,v FROM t_clear WHERE tenant_id=1 AND id=1") == ((1, "new"),)
            finally:
                resume(self.nodes)
            self.caught_up(c)
            assert self.lazy(c, "SELECT id,n,v FROM t_clear WHERE k=7 ORDER BY n,id") == ((1, 5, "new"),)

    def test_caught_up_mariadb(self):
        if not self.args.ref:
            self.skipped += 1
            print("    SKIP: --ref not supplied")
            return
        with self.table("t_diff", route_parts=2) as c, connect(self.args.ref, db=None) as ref:
            q(ref, f"CREATE DATABASE IF NOT EXISTS {DB}")
            q(ref, f"USE {DB}")
            q(ref, "DROP TABLE IF EXISTS t_diff")
            q(ref, "CREATE TABLE t_diff (tenant_id INT NOT NULL,id INT NOT NULL,k INT,n INT,v VARCHAR(32),"
                   "PRIMARY KEY(tenant_id,id),KEY lk(k,n,id) COMMENT 'tarn:lazy') PARTITION BY KEY(tenant_id)")
            rows = [(i % 11, i, None if i % 17 == 0 else i % 4, i % 9, None if i % 13 == 0 else f"雪{i}")
                    for i in range(1, 321)]
            for conn in (c, ref):
                with conn.cursor() as cur:
                    cur.executemany("INSERT INTO t_diff VALUES (%s,%s,%s,%s,%s)", rows)
                q(conn, "UPDATE t_diff SET n=n+100 WHERE id<=140")
                q(conn, "DELETE FROM t_diff WHERE id BETWEEN 200 AND 210")
            self.caught_up(c)
            for sql in ["SELECT tenant_id,id,k,n,v FROM t_diff WHERE k=1 ORDER BY n,id",
                        "SELECT id,v FROM t_diff WHERE k=2 AND n>=4 ORDER BY n DESC,id DESC LIMIT 13 OFFSET 7",
                        "SELECT id,k,n FROM t_diff WHERE k IS NULL ORDER BY n,id LIMIT 9",
                        "SELECT id,v FROM t_diff WHERE k=0 AND v IS NOT NULL ORDER BY n,id LIMIT 11"]:
                assert self.lazy(c, sql) == q(ref, sql), sql


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tarn", required=True)
    ap.add_argument("--kv", required=True, help="one leader per source shard, comma-separated")
    ap.add_argument("--ref")
    ap.add_argument("--engine", choices=("memtx", "vinyl"), default="memtx")
    ap.add_argument("--only", default="")
    args = ap.parse_args()
    with connect(args.tarn, db=None) as c:
        q(c, f"DROP DATABASE IF EXISTS {DB}")
        q(c, f"CREATE DATABASE {DB}")
    t = T(args)
    names = sorted(n for n in dir(t) if n.startswith("test_") and (not args.only or args.only in n))
    if not names:
        ap.error("--only selected no tests")
    try:
        for name in names:
            before = t.failures
            start = time.monotonic()
            try:
                getattr(t, name)()
            except Exception as e:
                t.failures += 1
                print(f"    EXCEPTION: {type(e).__name__}: {e}")
            print(f"{'ok  ' if t.failures == before else 'FAIL'} {name} ({time.monotonic()-start:.1f}s)")
    finally:
        resume(t.nodes)
    print(f"{len(names)} tests, {t.failures} failures, {t.skipped} skipped")
    return 1 if t.failures else 0


if __name__ == "__main__":
    sys.exit(main())
