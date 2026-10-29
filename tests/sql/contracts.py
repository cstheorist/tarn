#!/usr/bin/env python3
"""Executable, paired MySQL release contracts; see contracts/manifest.json.

Every registered case runs against both servers. Exceptions are retained as
failures, never skipped. Use an isolated test cluster: schema and GLOBAL
defaults are changed and restored. No user mutation is retried.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import threading
import time

import mysql.connector
from mysql.connector.constants import ClientFlag

ROOT = Path(__file__).resolve().parents[2]
DB = "release_contracts"
MODE = "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"


class Server:
    def __init__(self, address, kind, engine, prepared=False, found_rows=False):
        self.host, port = address.rsplit(":", 1)
        self.port, self.kind, self.engine = int(port), kind, engine
        self.prepared, self.found_rows = prepared, found_rows
        self.connections = []

    def connect(self, database=DB):
        c = mysql.connector.connect(host=self.host, port=self.port, user="sb", password="",
                                    database=database, autocommit=True, use_pure=True,
                                    charset="utf8mb4", collation="utf8mb4_bin", connection_timeout=10,
                                    client_flags=[ClientFlag.FOUND_ROWS] if self.found_rows else [-ClientFlag.FOUND_ROWS])
        self.connections.append(c)
        q(c, "SET SESSION sql_mode = '" + MODE + "'")
        q(c, "SET SESSION time_zone = '+00:00'")
        return c

    def fresh(self):
        c = self.connect()
        q(c, "DROP TABLE IF EXISTS t")
        engine = self.engine if self.kind == "tarn" else "InnoDB"
        q(c, "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL DEFAULT 0, u INT, UNIQUE KEY uq(u)) ENGINE=" + engine)
        q(c, "INSERT INTO t VALUES (1,10,11),(2,20,22)")
        return c

    def sql(self, c, text, params=None):
        return q(c, text, params, self.prepared)

    def close(self):
        for c in self.connections:
            try:
                c.rollback()
                c.close()
            except mysql.connector.Error:
                pass
        self.connections.clear()


def q(c, sql, params=None, prepared=False):
    cur = c.cursor(prepared=prepared)
    try:
        cur.execute(sql, params)
        return [list(r) for r in cur.fetchall()] if cur.with_rows else cur.rowcount
    finally:
        cur.close()


def error(fn):
    try:
        fn()
    except mysql.connector.Error as e:
        return [e.errno, e.sqlstate]
    return None


def case_session_scope(s):
    c = s.connect()
    old = q(c, "SELECT @@global.transaction_isolation, @@global.innodb_lock_wait_timeout")[0]
    try:
        q(c, "SET GLOBAL transaction_isolation='READ-COMMITTED'")
        q(c, "SET GLOBAL innodb_lock_wait_timeout=7")
        q(c, "SET SESSION transaction_isolation='SERIALIZABLE'")
        q(c, "SET SESSION innodb_lock_wait_timeout=2")
        a = q(c, "SELECT @@global.transaction_isolation, @@session.transaction_isolation, "
                 "@@global.innodb_lock_wait_timeout, @@session.innodb_lock_wait_timeout")
        b = q(s.connect(), "SELECT @@transaction_isolation, @@innodb_lock_wait_timeout")
        assert a == [["READ-COMMITTED", "SERIALIZABLE", 7, 2]], a
        assert b == [["READ-COMMITTED", 7]], b
        return [a, b]
    finally:
        q(c, "SET GLOBAL transaction_isolation=%s", (old[0],))
        q(c, "SET GLOBAL innodb_lock_wait_timeout=%s", (old[1],))


def case_rc_visibility(s):
    a = s.fresh()
    b = s.connect()
    q(a, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    q(a, "BEGIN")
    first = s.sql(a, "SELECT v FROM t WHERE id=1")
    s.sql(b, "UPDATE t SET v=30 WHERE id=1")
    second = s.sql(a, "SELECT v FROM t WHERE id=1")
    s.sql(a, "UPDATE t SET v=v+1 WHERE id=1")
    own = s.sql(a, "SELECT v FROM t WHERE id=1")
    q(a, "ROLLBACK")
    out = [first, second, own, s.sql(b, "SELECT v FROM t WHERE id=1")]
    assert out == [[[10]], [[30]], [[31]], [[30]]], out
    return out


def case_savepoint_statement_rollback(s):
    c = s.fresh()
    q(c, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    q(c, "BEGIN")
    s.sql(c, "UPDATE t SET v=12 WHERE id=1")
    q(c, "SAVEPOINT x")
    s.sql(c, "UPDATE t SET v=23 WHERE id=2")
    dup = error(lambda: s.sql(c, "INSERT INTO t VALUES (3,30,33),(4,40,11)"))
    after = s.sql(c, "SELECT id,v,u FROM t ORDER BY id")
    q(c, "ROLLBACK TO SAVEPOINT x")
    q(c, "COMMIT")
    final = s.sql(c, "SELECT id,v,u FROM t ORDER BY id")
    assert dup == [1062, "23000"] and after == [[1,12,11],[2,23,22]] and final == [[1,12,11],[2,20,22]], (dup,after,final)
    return [dup, after, final]


def case_lock_timeout(s):
    a = s.fresh()
    b = s.connect()
    for c in (a,b):
        q(c, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        q(c, "BEGIN")
    s.sql(a, "UPDATE t SET v=99 WHERE id=1")
    s.sql(b, "UPDATE t SET v=25 WHERE id=2")
    q(b, "SET innodb_lock_wait_timeout=1")
    err = error(lambda: s.sql(b, "UPDATE t SET v=98 WHERE id=1"))
    q(a, "ROLLBACK")
    q(b, "COMMIT")
    rows = s.sql(a, "SELECT id,v FROM t ORDER BY id")
    assert err == [1205,"HY000"] and rows == [[1,10],[2,25]], (err,rows)
    return [err,rows]


def case_lock_wait(s):
    a = s.fresh()
    b = s.connect()
    for c in (a,b):
        q(c, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    q(a, "BEGIN")
    s.sql(a, "UPDATE t SET v=v+1 WHERE id=1")
    started, done = threading.Event(), threading.Event()
    result = []
    def writer():
        started.set()
        try:
            result.append(s.sql(b, "UPDATE t SET v=v+1 WHERE id=1"))
        except Exception as e:
            result.append(str(e))
        finally:
            done.set()
    t = threading.Thread(target=writer, daemon=True)
    t.start()
    assert started.wait(5)
    try:
        assert not done.wait(.15), result
    finally:
        q(a, "COMMIT")
    assert done.wait(10), "writer did not finish after lock release"
    t.join()
    rows = s.sql(a, "SELECT v FROM t WHERE id=1")
    assert result == [1] and rows == [[12]], (result,rows)
    return [result,rows]


def case_nowait_skip_locked(s):
    a = s.fresh()
    b = s.connect()
    for c in (a,b):
        q(c, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        q(c, "BEGIN")
    s.sql(a, "SELECT id FROM t WHERE id=1 FOR UPDATE")
    err = error(lambda: s.sql(b, "SELECT id FROM t WHERE id=1 FOR UPDATE NOWAIT"))
    rows = s.sql(b, "SELECT id FROM t ORDER BY id FOR UPDATE SKIP LOCKED")
    q(a, "ROLLBACK")
    q(b, "ROLLBACK")
    expected = [1205,"HY000"] if s.kind == "mariadb" else [3572,"HY000"]
    assert err == expected and rows == [[2]], (err,rows)
    return [err,rows]


def case_ddl_implicit_commit(s):
    c = s.fresh()
    q(c, "BEGIN")
    s.sql(c, "UPDATE t SET v=50 WHERE id=1")
    q(c, "CREATE TABLE ddl_cut (id INT PRIMARY KEY)")
    q(c, "ROLLBACK")
    rows = s.sql(s.connect(), "SELECT v FROM t WHERE id=1")
    q(c, "DROP TABLE ddl_cut")
    assert rows == [[50]], rows
    return rows


def case_rc_phantom_current_read(s):
    a = s.fresh()
    b = s.connect()
    q(a, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    q(a, "BEGIN")
    first = s.sql(a, "SELECT COUNT(*) FROM t WHERE v>=10")
    s.sql(b, "INSERT INTO t VALUES (3,30,33)")
    s.sql(b, "UPDATE t SET v=40 WHERE id=1")
    second = s.sql(a, "SELECT COUNT(*) FROM t WHERE v>=10")
    current = s.sql(a, "SELECT v FROM t WHERE id=1 FOR UPDATE")
    q(a, "COMMIT")
    assert [first,second,current] == [[[2]],[[3]],[[40]]], (first,second,current)
    return [first,second,current]


def case_rr_phantom_difference(s):
    a = s.fresh()
    b = s.connect()
    q(a, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    q(a, "BEGIN")
    first = s.sql(a, "SELECT COUNT(*) FROM t WHERE v>=10")
    s.sql(b, "INSERT INTO t VALUES (3,30,33)")
    commit = error(lambda: q(a, "COMMIT"))
    assert first == [[2]] and commit == ([1213,"40001"] if s.kind == "tarn" else None), (first,commit)
    return [first,commit]


def case_noop_serializable_difference(s):
    a = s.fresh()
    b = s.connect()
    q(a, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
    q(a, "BEGIN")
    s.sql(a, "UPDATE t SET v=v WHERE id=1")
    q(b, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    q(b, "SET innodb_lock_wait_timeout=1")
    outcome = error(lambda: s.sql(b, "UPDATE t SET v=31 WHERE id=1"))
    q(a, "ROLLBACK")
    final = s.sql(b, "SELECT v FROM t WHERE id=1")
    assert outcome == (None if s.kind == "tarn" else [1205,"HY000"]), outcome
    assert final == ([[31]] if s.kind == "tarn" else [[10]]), final
    return [outcome,final]


def case_deadlock_rollback(s):
    a = s.fresh()
    b = s.connect()
    for c in (a,b):
        q(c, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        q(c, "SET innodb_lock_wait_timeout=15")
        q(c, "BEGIN")
    s.sql(a, "UPDATE t SET v=v+1 WHERE id=1")
    s.sql(b, "UPDATE t SET v=v+1 WHERE id=2")
    results=[]
    def finish(c, key):
        e=error(lambda: s.sql(c, f"UPDATE t SET v=v+1 WHERE id={key}"))
        # A deadlock rolls back the whole transaction. COMMIT after it is
        # harmless, and a surviving transaction commits both increments.
        q(c,"COMMIT")
        results.append(e)
    thread=threading.Thread(target=finish,args=(a,2),daemon=True)
    thread.start()
    finish(b,1)
    thread.join(20)
    assert not thread.is_alive(), "deadlock survivor did not finish"
    assert len(results)==2 and results.count(None)==1 and results.count([1213,"40001"])==1, results
    final=s.sql(s.connect(),"SELECT id,v FROM t ORDER BY id")
    assert final==[[1,11],[2,21]], final
    return [[None,[1213,"40001"]],final]


def case_autocommit_scope(s):
    c = s.fresh()
    q(c, "SET autocommit=0")
    s.sql(c, "UPDATE t SET v=80 WHERE id=1")
    q(c, "SET autocommit=1")
    q(c, "ROLLBACK")
    rows = s.sql(s.connect(), "SELECT v FROM t WHERE id=1")
    assert rows == [[80]], rows
    return rows


def case_affected_rows(s):
    c = s.fresh()
    first = s.sql(c, "UPDATE t SET v=10 WHERE id=1")
    second = s.sql(c, "UPDATE t SET v=11 WHERE id=1")
    assert [first, second] == [int(s.found_rows),1], (first,second)
    return [first,second]


def case_binary_parameters(s):
    c = s.fresh()
    out = s.sql(c, "SELECT v FROM t WHERE id=%s", (1,))
    changed = s.sql(c, "UPDATE t SET v=%s WHERE id=%s", (42,2))
    rows = s.sql(c, "SELECT v FROM t WHERE id=%s", (2,))
    assert out == [[10]] and changed == 1 and rows == [[42]], (out,changed,rows)
    return [out,changed,rows]


def case_rr_validation_difference(s):
    a = s.fresh()
    b = s.connect()
    q(a, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    q(a, "BEGIN")
    old = s.sql(a, "SELECT v FROM t WHERE id=1")
    s.sql(b, "UPDATE t SET v=60 WHERE id=1")
    commit = error(lambda: q(a, "COMMIT"))
    assert old == [[10]], old
    assert commit == ([1213,"40001"] if s.kind == "tarn" else None), commit
    return [old,commit]


def case_write_skew_difference(s):
    a = s.fresh()
    b = s.connect()
    for c in (a,b):
        q(c, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        q(c, "BEGIN")
        assert s.sql(c, "SELECT SUM(v) FROM t") == [[30]]
    s.sql(a, "UPDATE t SET v=0 WHERE id=1")
    s.sql(b, "UPDATE t SET v=0 WHERE id=2")
    q(a, "COMMIT")
    second = error(lambda: q(b, "COMMIT"))
    final = s.sql(a, "SELECT id,v FROM t ORDER BY id")
    expected = [[1,0],[2,20]] if s.kind == "tarn" else [[1,0],[2,0]]
    assert second == ([1213,"40001"] if s.kind == "tarn" else None) and final == expected, (second,final)
    return [second,final]


def case_isolation_reporting(s):
    c = s.connect()
    if s.kind != "tarn":
        return error(lambda: q(c, "SELECT @@tarn_effective_isolation"))
    q(c, "SET transaction_isolation='REPEATABLE-READ'")
    defaults = q(c, "SELECT @@transaction_isolation, @@tarn_effective_isolation, @@tarn_active_isolation")
    assert defaults == [["REPEATABLE-READ","STRICT-SERIALIZABLE",None]], defaults
    q(c, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
    assert q(c, "SELECT @@tarn_next_isolation") == [["READ-COMMITTED"]]
    q(c, "BEGIN")
    active = q(c, "SELECT @@tarn_active_isolation, @@tarn_next_isolation, @@tarn_effective_isolation")
    assert active == [["READ-COMMITTED",None,"STRICT-SERIALIZABLE"]], active
    q(c, "ROLLBACK")
    assert q(c, "SELECT @@tarn_active_isolation") == [[None]]
    ro = error(lambda: q(c, "SET tarn_effective_isolation='MYSQL'"))
    assert ro == [1238,"HY000"], ro
    return [defaults,active,ro]


def case_exact_mysql_refusal(s):
    c = s.fresh()
    q(c, "BEGIN")
    s.sql(c, "UPDATE t SET v=44 WHERE id=1")
    e = error(lambda: q(c, "SET tarn_isolation_contract='MYSQL'"))
    assert e == ([1235,"42000"] if s.kind == "tarn" else [1193,"HY000"]), e
    own = s.sql(c, "SELECT v FROM t WHERE id=1")
    q(c, "ROLLBACK")
    final = s.sql(c, "SELECT v FROM t WHERE id=1")
    assert own == [[44]] and final == [[10]], (own,final)
    return [e,own,final]


def receipt(s):
    c = s.connect(None)
    fields = ["version", "sql_mode", "character_set_connection", "collation_connection", "time_zone", "transaction_isolation", "autocommit"]
    values = q(c, "SELECT " + ",".join("@@"+f for f in fields))[0]
    return dict(zip(fields,values), client_flags=c.client_flags, connector=mysql.connector.__version__, prepared=s.prepared)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tarn", required=True)
    ap.add_argument("--ref", required=True)
    ap.add_argument("--oracle", choices=["mysql","mariadb"], default="mysql")
    ap.add_argument("--engine", choices=["memtx","vinyl"], default="memtx")
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--only")
    args = ap.parse_args()
    manifest_path = ROOT / "contracts/manifest.json"
    manifest = json.loads(manifest_path.read_text())
    assert {x["id"] for x in manifest["contracts"]} == {n[5:] for n in globals() if n.startswith("case_")}
    report = {"manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(), "engine":args.engine, "oracle":args.oracle, "cases":[]}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    failures = 0
    for prepared in (False,True):
        servers = [Server(args.tarn,"tarn",args.engine,prepared),Server(args.ref,args.oracle,args.engine,prepared)]
        for s in servers:
            c=s.connect(None)
            q(c,"CREATE DATABASE IF NOT EXISTS " + DB)
        report.setdefault("connections",[]).append([receipt(s) for s in servers])
        if args.oracle == "mysql":
            assert report["connections"][-1][1]["version"] == manifest["oracle"]["version"], "oracle build changed"
        for entry in manifest["contracts"]:
            if args.only and entry["id"] != args.only:
                continue
            r={"id":entry["id"],"prepared":prepared,"classification":entry["classification"],"results":[],"passed":False}
            try:
                for s in servers:
                    try:
                        r["results"].append(globals()["case_"+entry["id"]](s))
                    finally:
                        s.close()
                if entry["classification"] == "equivalent" and not (args.oracle=="mariadb" and entry["id"]=="nowait_skip_locked"):
                    assert r["results"][0] == r["results"][1], "paired results differ"
                r["passed"] = True
            except Exception as e:
                failures += 1
                r["error"] = repr(e)
            report["cases"].append(r)
            args.output.write_text(json.dumps(report,indent=2,default=str)+"\n")
            print(("PASS " if r["passed"] else "FAIL ")+entry["id"]+f" prepared={prepared} "+r.get("error",""),flush=True)
        # CLIENT_FOUND_ROWS changes the explicit affected-row contract.
        for s in servers:
            s.found_rows=True
            try:
                result=case_affected_rows(s)
                report["cases"].append({"id":"affected_rows","prepared":prepared,"found_rows":True,"server":s.kind,"passed":True,"results":result})
            except Exception as e:
                failures+=1
                report["cases"].append({"id":"affected_rows","prepared":prepared,"found_rows":True,"server":s.kind,"passed":False,"error":repr(e)})
            finally:
                s.close()
    report["failures"]=failures
    args.output.write_text(json.dumps(report,indent=2,default=str)+"\n")
    return bool(failures)


if __name__ == "__main__":
    sys.exit(main())
