#!/usr/bin/env python3
"""Compile-time EXPLAIN and bounded, completed-query routing observations.

Needs >=2 shards. Checks public text and binary MySQL protocol, exact join
results before/after colocation, no EXPLAIN effects, and digest eviction.
"""
import argparse
import json
import secrets
import struct

import pymysql
from pymysql.constants import COMMAND


def q(c, sql):
    with c.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall() if cur.description else cur.rowcount


def plan(c, sql):
    with c.cursor() as cur:
        cur.execute("EXPLAIN FORMAT=JSON " + sql)
        assert [d[0] for d in cur.description] == ["EXPLAIN"]
        rows = cur.fetchall()
        assert len(rows) == 1 and len(rows[0]) == 1
        result = json.loads(rows[0][0])
        assert result["executed"] is False and result["phase"] == "compile_time"
        return result


def digests(c):
    db = q(c, "SELECT DATABASE()")[0][0]
    with c.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SHOW TARN QUERIES")
        return [row for row in cur.fetchall() if row["Database"] == db]


def status(c):
    return {k: int(v) for k, v in q(c, "SHOW TARN STATUS") if str(v).isdigit()}


def expect_error(c, sql, codes):
    try:
        q(c, sql)
    except pymysql.MySQLError as e:
        assert e.args[0] in codes, (sql, e)
    else:
        raise AssertionError("accepted unsupported statement: " + sql)


def prepare(c, sql):
    c._execute_command(COMMAND.COM_STMT_PREPARE, sql.encode())
    packet = c._read_packet()
    packet.check_error()
    data = packet.get_all_data()
    assert data[0] == 0
    statement, columns, params = struct.unpack_from("<IHH", data, 1)
    for count in (params, columns):
        for _ in range(count):
            c._read_packet().check_error()
        if count:
            assert c._read_packet().is_eof_packet()
    return statement, columns, params


def binary_explain(c, statement, value):
    payload = struct.pack("<IBI", statement, 0, 1) + b"\x00\x01\x08\x00" + struct.pack("<q", value)
    c._execute_command(COMMAND.COM_STMT_EXECUTE, payload)
    assert c._read_packet().read_length_encoded_integer() == 1
    field = c._read_packet().get_all_data()
    assert b"EXPLAIN" in field
    assert c._read_packet().is_eof_packet()
    row = c._read_packet()
    assert row.read(2) == b"\x00\x00"
    result = json.loads(row.read_length_coded_string())
    assert c._read_packet().is_eof_packet()
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True)
    args = ap.parse_args()
    host, port = args.tarn.split(":")
    c = pymysql.connect(host=host, port=int(port), user="sb", password="", autocommit=True, charset="utf8mb4")
    db = "explaintest_" + secrets.token_hex(6)
    q(c, "CREATE DATABASE " + db)
    c.select_db(db)
    try:
        q(c, "CREATE TABLE t (id INT PRIMARY KEY, v INT, code VARCHAR(30), UNIQUE KEY uc(code))")
        q(c, "CREATE TABLE ai (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")
        q(c, "INSERT INTO t VALUES (1,10,'secret-one'),(2,20,'secret-two')")
        point = plan(c, "SELECT v FROM t WHERE id=1")
        n = len(point["live_shards"])
        assert n >= 2
        access = point["plan"]["accesses"][0]
        assert access["access_path"] == "unique_point" and access["index"] == "PRIMARY"
        assert access["routing"]["shard_count"] == access["routing"]["bucket_count"] == 1
        assert plan(c, "SELECT * FROM t")["plan"]["routing"]["shard_count"] == n
        assert plan(c, "SELECT 1")["plan"]["routing"]["shard_count"] == 0
        global_read = plan(c, "SELECT id FROM t WHERE code='secret-one'")["plan"]["accesses"][0]["routing"]
        assert global_read["status"] == "global_lookup_then_base_rows"
        assert global_read["shard_count"] is None and global_read["global_key_route"]["shard_count"] == 1
        q(c, "SET SESSION transaction_isolation='READ-COMMITTED'")
        assert plan(c, "SELECT id FROM t WHERE code='secret-one'")["plan"]["accesses"][0]["routing"]["status"] == "global_lookup_then_base_rows"
        for sql in ("SELECT id FROM t WHERE code='secret-one' FOR UPDATE", "UPDATE t SET v=11 WHERE code='secret-one'"):
            assert plan(c, sql)["plan"]["accesses"][0]["routing"]["shard_count"] == n
        q(c, "SET SESSION transaction_isolation='SERIALIZABLE'")
        write = plan(c, "INSERT INTO t VALUES(3,30,'secret-three')")["plan"]
        assert write["base_write_routing"]["shard_count"] == 1
        assert write["global_indexes"][0]["routing"]["shard_count"] == 1
        assert plan(c, "UPDATE t SET code='changed' WHERE id=1")["plan"]["global_indexes"][0]["routing"]["shard_count"] is None
        assert plan(c, "INSERT INTO ai(v) VALUES(1)")["plan"]["base_write_routing"]["shard_count"] is None
        before = status(c)
        for sql in ("SELECT * FROM t", "SELECT id FROM t WHERE code='secret-one'", "UPDATE t SET v=v+1",
                    "DELETE FROM t", "INSERT INTO ai(v) VALUES(1)", "SELECT * FROM t FOR UPDATE"):
            plan(c, sql)
        after = status(c)
        for key in ("Tarn_rows_read", "Tarn_storage_reads", "Tarn_commits_local", "Tarn_commits_2pc", "Tarn_commits_read_only"):
            assert before[key] == after[key], (key, before[key], after[key])
        assert q(c, "INSERT INTO ai(v) VALUES(7)") == 1
        assert q(c, "SELECT id,v FROM ai") == ((1, 7),)
        q(c, "BEGIN")
        q(c, "INSERT INTO t VALUES(4,40,'pending')")
        plan(c, "DELETE FROM t")
        q(c, "ROLLBACK")
        assert q(c, "SELECT id,v FROM t ORDER BY id") == ((1, 10), (2, 20))
        before = status(c)
        for sql in ("EXPLAIN FORMAT=JSON SELECT (SELECT v FROM t WHERE id=1)",
                    "/* prefix */ EXPLAIN FORMAT=JSON SELECT id FROM t WHERE EXISTS(SELECT id FROM t)",
                    "--\tprefix\nEXPLAIN FORMAT=JSON DELETE FROM t WHERE id IN(SELECT id FROM t)"):
            expect_error(c, sql, {1235})
            try:
                prepare(c, sql)
            except pymysql.MySQLError as e:
                assert e.args[0] == 1235, e
            else:
                raise AssertionError("prepared complex EXPLAIN accepted")
        after = status(c)
        for key in ("Tarn_rows_read", "Tarn_storage_reads"):
            assert before[key] == after[key], (key, before[key], after[key])
        print("PASS paths, global participants, no user reads/IDs/locks/implicit commits, complex rejection")

        q(c, "PREPARE p FROM 'SELECT v FROM t WHERE id=?'")
        unknown = json.loads(q(c, "SHOW TARN PLAN FOR p")[0][0])
        assert unknown["plan"]["accesses"][0]["routing"]["shard_count"] is None
        q(c, "SET @id=1")
        assert q(c, "EXECUTE p USING @id") == ((10,),)
        q(c, "DEALLOCATE PREPARE p")
        statement, columns, params = prepare(c, "EXPLAIN FORMAT=JSON SELECT v FROM t WHERE id=?")
        assert (columns, params) == (1, 1)
        bound = binary_explain(c, statement, 1)
        assert bound["plan"]["accesses"][0]["routing"]["shard_count"] == 1
        c._execute_command(COMMAND.COM_STMT_CLOSE, struct.pack("<I", statement))
        statement, columns, params = prepare(c, "SELECT v FROM t WHERE id=?")
        unbound = json.loads(q(c, "SHOW TARN PLAN FOR " + str(statement))[0][0])
        assert unbound["plan"]["accesses"][0]["routing"]["shard_count"] is None
        c._execute_command(COMMAND.COM_STMT_CLOSE, struct.pack("<I", statement))
        expect_error(c, "SHOW TARN PLAN FOR missing", {1243})
        expect_error(c, "SHOW TARN QUERIES garbage", {1064})
        expect_error(c, "SHOW TARN PLAN FOR missing garbage", {1064})
        for sql in ("EXPLAIN FORMAT=TREE SELECT * FROM t", "EXPLAIN ANALYZE SELECT * FROM t",
                    "EXPLAIN FORMAT=JSON CREATE TABLE forbidden(id INT)"):
            expect_error(c, sql, {1064, 1235})
        print("PASS named/binary prepared plans, unbound markers, explicit unsupported syntax")

        for suffix, partition in (("before", ""), ("after", " PARTITION BY KEY(tenant_id)")):
            q(c, f"CREATE TABLE p_{suffix}(tenant_id INT,parent_id INT,v INT,PRIMARY KEY(tenant_id,parent_id)){partition}")
            q(c, f"CREATE TABLE c_{suffix}(tenant_id INT,parent_id INT,id INT,v INT,PRIMARY KEY(tenant_id,parent_id,id)) COMMENT='tarn:parent=p_{suffix}'{partition}")
            q(c, f"INSERT INTO p_{suffix} VALUES(1,7,10)")
            q(c, f"INSERT INTO c_{suffix} VALUES" + ",".join(f"(1,7,{i},{i*10})" for i in range(1, 17)))
        queries = {s: f"SELECT p.v,c.id,c.v FROM p_{s} p JOIN c_{s} c ON c.tenant_id=p.tenant_id AND c.parent_id=p.parent_id WHERE p.tenant_id=1 AND p.parent_id=7 ORDER BY c.id" for s in ("before", "after")}
        pre, post = (plan(c, queries[s]) for s in ("before", "after"))
        assert pre["plan"]["colocation"]["eligible"] is False, pre
        assert post["plan"]["colocation"]["eligible"] is True, post
        assert any("PARTITION BY KEY" in f["message"] for f in pre["plan"]["accesses"][1]["schema_findings"]), pre
        assert not post["plan"]["accesses"][1]["schema_findings"], post
        assert post["plan"]["routing"]["shard_count"] == post["plan"]["routing"]["bucket_count"] == 1
        assert q(c, queries["before"]) == q(c, queries["after"]) == tuple((10, i, i*10) for i in range(1, 17))
        entries = digests(c)
        rows = {s: next(e for e in entries if "P_" + s.upper() in e["Query"] and "JOIN" in e["Query"]) for s in queries}
        assert rows["before"]["Distributed"] == 1 and rows["before"]["Max_shards"] == n, rows
        assert rows["after"]["Local"] == 1 and rows["after"]["Max_shards"] == 1, rows
        assert rows["after"]["Grouped_reads"] == 1 and rows["before"]["Grouped_reads"] == 0, rows
        q(c, "BEGIN")
        q(c, "UPDATE c_after SET v=99 WHERE tenant_id=1 AND parent_id=7 AND id=1")
        assert plan(c, queries["after"])["plan"]["colocation"]["eligible"] is False
        q(c, "ROLLBACK")
        print("PASS equal 16-row joins; shard-key alignment changes observed fanout from", n, "to 1")

        ai = next(e for e in digests(c) if e["Query"].startswith("INSERT INTO AI"))
        assert ai["Calls"] == ai["Local_commits"] == 1, ai
        expect_error(c, "INSERT INTO ai VALUES(1,99)", {1062})
        duplicate = next(e for e in digests(c) if e["Query"] == "INSERT INTO AI VALUES ( ? , ? )")
        assert duplicate["Calls"] == duplicate["Errors"] == 1, duplicate
        assert q(c, "SELECT id,v FROM ai") == ((1, 7),)
        q(c, "BEGIN")
        q(c, "UPDATE c_after SET v=v+1 WHERE tenant_id=1 AND parent_id=7 AND id=1")
        q(c, "COMMIT")
        commit = next(e for e in digests(c) if e["Query"] == "COMMIT")
        assert commit["Local_commits"] == 1 and commit["Commit_errors"] == 0, commit
        q(c, "SELECT v FROM t WHERE id=1")
        q(c, "SELECT v FROM t WHERE id=2")
        text = next(e for e in digests(c) if e["Query"] == "SELECT V FROM T WHERE ID = ?")
        assert text["Calls"] == 3 and text["Local"] == 3, text  # includes EXECUTE p
        q(c, "SELECT id FROM t WHERE code='secret-one'")
        assert not any("secret" in e["Query"].lower() or "pending" in e["Query"].lower() for e in digests(c))
        for i in range(270):
            q(c, f"SELECT 1 AS digest_{i}")
        entries = digests(c)
        counters = status(c)
        assert len(entries) == counters["Tarn_query_digest_entries"] == counters["Tarn_query_digest_capacity"] == 256
        assert counters["Tarn_query_digest_evictions"] >= 14
        assert all(len(e["Query"].encode()) <= 512 and len(e["Digest"]) == 40 for e in entries)
        assert counters["Tarn_query_digest_cancelled"] == 0
        print("PASS literal redaction, prepared/text aggregation, 256-entry eviction, no false cancellations")
    finally:
        q(c, "DROP DATABASE IF EXISTS " + db)
        c.close()
    print("EXPLAIN/routing diagnostics: all checks passed")


if __name__ == "__main__":
    main()
