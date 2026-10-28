#!/usr/bin/env python3
"""Bounded optimizer hints, exact join results, OCC, overlays and invalidation.

Needs two gateways over the same >=2-shard cluster. Hints are gateway-local:
the first remains a written-order control, the second explicitly ANALYZEs.
"""
import argparse
import json
import secrets
import struct
import threading

import pymysql

from pymysql.constants import COMMAND

from explaintest import q, plan, prepare


def connect(address, db=None):
    host, port = address.split(":")
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db,
                           autocommit=True, charset="utf8mb4")


def expect_error(c, sql, codes):
    try:
        q(c, sql)
    except pymysql.MySQLError as e:
        assert e.args[0] in codes, (sql, e)
    else:
        raise AssertionError("expected rejection: " + sql)


def stats(c):
    with c.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SHOW TARN STATISTICS")
        return cur.fetchall()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True, help="control,candidate gateway addresses")
    args = ap.parse_args()
    control, candidate = args.tarn.split(",")
    db = "statistics_" + secrets.token_hex(6)
    a, b = connect(control), connect(candidate)
    q(a, "CREATE DATABASE " + db)
    a.select_db(db)
    b.select_db(db)
    try:
        q(a, "CREATE TABLE a(id INT PRIMARY KEY,k INT,s VARCHAR(20) COLLATE utf8mb4_general_ci,n DECIMAL(8,2))")
        q(a, "CREATE TABLE b(id INT PRIMARY KEY,a_id INT,k INT,KEY a_id(a_id))")
        for start in range(1, 2049, 256):
            q(a, "INSERT INTO a VALUES" + ",".join(f"({i},{i},'Á',{i}.25)" for i in range(start, start + 256)))
        q(a, "INSERT INTO b VALUES" + ",".join(f"({i},{777 if i < 9 else 'NULL'},{777})" for i in range(1, 21)))
        sql = "SELECT a.*,b.* FROM a JOIN b ON a.id=b.a_id WHERE b.id=7 ORDER BY a.id,b.id"
        baseline = q(a, sql)
        assert len(baseline) == 1 and baseline[0][0] == 777 and baseline[0][4] == 7
        assert plan(b, sql)["plan"]["accesses"][0]["alias"] == "a"
        assert q(b, "ANALYZE TABLE a") == ((db + ".a", "analyze", "status", "OK"),)
        assert plan(b, sql)["plan"]["join_estimate"] is None
        # Prepare before publication; the subsequent stats epoch must make
        # the binary prepared EXPLAIN re-plan without changing its metadata.
        statement, columns, params = prepare(b, "EXPLAIN FORMAT=JSON " + sql)
        assert (columns, params) == (1, 0)
        analyze, ncols, nparams = prepare(b, "ANALYZE TABLE b")
        assert (ncols, nparams) == (4, 0)
        b._execute_command(COMMAND.COM_STMT_EXECUTE, struct.pack("<IBI", analyze, 0, 1))
        assert b._read_packet().read_length_encoded_integer() == 4
        for _ in range(4):
            b._read_packet().check_error()
        assert b._read_packet().is_eof_packet()
        row = b._read_packet()
        assert row.read(2) == b"\x00\x00"
        assert [row.read_length_coded_string().decode() for _ in range(4)] == [db + ".b", "analyze", "status", "OK"]
        assert b._read_packet().is_eof_packet()
        b._execute_command(COMMAND.COM_STMT_CLOSE, struct.pack("<I", analyze))
        p = plan(b, sql)["plan"]
        assert p["accesses"][0]["alias"] == "b" and p["join_estimate"]["reordered"] is True, p
        assert all(x["cardinality"]["sample_rows"] <= 512 for x in p["accesses"])
        assert q(b, sql) == baseline
        # COM_STMT_EXECUTE with no parameters; read the one binary JSON column.
        b._execute_command(COMMAND.COM_STMT_EXECUTE, struct.pack("<IBI", statement, 0, 1))
        assert b._read_packet().read_length_encoded_integer() == 1
        b._read_packet().check_error()
        assert b._read_packet().is_eof_packet()
        row = b._read_packet()
        assert row.read(2) == b"\x00\x00"
        binary = json.loads(row.read_length_coded_string())
        assert b._read_packet().is_eof_packet()
        assert binary["plan"]["join_estimate"]["reordered"] is True
        b._execute_command(COMMAND.COM_STMT_CLOSE, struct.pack("<I", statement))
        def storage_rows():
            with b.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute("SHOW TARN QUERIES")
                return sum(r["Storage_rows"] for r in cur.fetchall() if r["Database"] == db)
        prepared, columns, params = prepare(b, "SELECT a.id,b.id FROM a JOIN b ON a.id=b.a_id WHERE b.id=?")
        assert (columns,params) == (2,1)
        before_rows = storage_rows()
        for key in (7,8):
            payload = struct.pack("<IBI",prepared,0,1)+b"\x00\x01\x08\x00"+struct.pack("<q",key)
            b._execute_command(COMMAND.COM_STMT_EXECUTE,payload)
            assert b._read_packet().read_length_encoded_integer() == 2
            for _ in range(2):
                b._read_packet().check_error()
            assert b._read_packet().is_eof_packet()
            row = b._read_packet()
            assert row.read(2) == b"\x00\x00"
            assert struct.unpack("<ii",row.read(8)) == (777,key)
            assert b._read_packet().is_eof_packet()
        assert storage_rows()-before_rows <= 10, "bound binary joins did not use the selective order"
        b._execute_command(COMMAND.COM_STMT_CLOSE,struct.pack("<I",prepared))
        assert not [s for s in stats(a) if s["Table"].startswith(db + ".")]
        samples = [s for s in stats(b) if s["Table"].startswith(db + ".")]
        assert samples and all(s["Sample_rows"] <= 512 for s in samples)
        assert {s["Estimated_table_rows"] for s in samples} == {2048, 20}
        for query in (sql.replace("a.*,b.*", "*"), sql.replace("a.*,b.*", "b.*,a.*"),
                      sql.replace("b.id=7", "b.id=8"), sql.replace("b.id=7", "b.id=9"),
                      sql.replace("b.id=7", "b.id=999")):
            assert q(a, query) == q(b, query), query
        for query in (sql.replace(" JOIN ", " LEFT JOIN "), sql.replace("a.*,b.*", "COUNT(*)"),
                      sql + " LIMIT 1", sql + " FOR UPDATE", sql.replace("b.id=7", "a.n=b.k"),
                      sql.replace("b.id=7", "a.s='a'"), sql.replace("a.*,b.*", "RAND()")):
            assert plan(b, query)["plan"]["join_estimate"] is None, query
        print("PASS bounded/local samples, missing-hint fallback, physical reorder, star/metadata, binary cache epoch and unsafe-shape fallback")

        q(b, "BEGIN")
        assert q(b, sql) == baseline
        q(a, "UPDATE a SET k=k+1 WHERE id=777")
        expect_error(b, "COMMIT", {1213})
        q(a, "UPDATE a SET k=k-1 WHERE id=777")
        missing = sql.replace("b.id=7", "b.id=200")
        q(b, "BEGIN")
        assert q(b, missing) == ()
        q(a, "INSERT INTO b VALUES(200,777,777)")
        expect_error(b, "COMMIT", {1213})
        q(b, "BEGIN")
        q(b, "DELETE FROM b WHERE id=7")
        assert q(b, sql) == ()
        q(b, "ROLLBACK")
        assert q(b, sql) == baseline
        q(b, "BEGIN")
        q(b, "INSERT INTO b VALUES(201,777,777)")
        assert len(q(b, sql.replace("b.id=7", "b.id=201"))) == 1
        q(b, "ROLLBACK")
        assert q(a, sql.replace("b.id=7", "b.id=201")) == ()
        print("PASS read validation, phantom validation, own-write/delete overlays and rollback")

        errors = []
        stop = threading.Event()
        committed = [0]
        def writer():
            c = connect(control, db)
            try:
                while not stop.is_set():
                    try:
                        q(c, "BEGIN")
                        q(c, "UPDATE a SET k=k+1 WHERE id=777")
                        q(c, "UPDATE b SET k=k+1 WHERE id=7")
                        q(c, "COMMIT")
                        committed[0] += 1
                    except pymysql.MySQLError as e:
                        q(c, "ROLLBACK")
                        if e.args[0] != 1213:
                            raise
            except Exception as e:
                errors.append(repr(e))
            finally:
                c.close()
        thread = threading.Thread(target=writer)
        thread.start()
        read = "SELECT a.k,b.k FROM a JOIN b ON a.id=b.a_id WHERE b.id=7"
        successes = 0
        try:
            for _ in range(100):
                try:
                    rows = q(b, read)
                    assert len(rows) == 1 and rows[0][0] == rows[0][1], rows
                    successes += 1
                except pymysql.MySQLError as e:
                    assert e.args[0] == 1213, e
        finally:
            stop.set()
            thread.join(timeout=30)
        assert not thread.is_alive() and not errors and successes and committed[0], (successes, committed, errors)
        print(f"PASS coherent reordered join under writes: {successes} reads, {committed[0]} committed transfers")

        # ANALYZE performs the normal implicit commit; existing stale data
        # estimates never substitute for execution or buffered-write checks.
        q(b, "BEGIN")
        q(b, "UPDATE a SET k=123 WHERE id=1")
        q(b, "ANALYZE TABLE a")
        assert q(a, "SELECT k FROM a WHERE id=1") == ((123,),)
        # Syntax rejection does not implicitly commit pending writes.
        q(b, "BEGIN")
        q(b, "UPDATE a SET k=999 WHERE id=2")
        expect_error(b, "ANALYZE TABLE a NOSCAN", {1235})
        q(b, "ROLLBACK")
        assert q(a, "SELECT k FROM a WHERE id=2") == ((2,),)
        q(a, "ALTER TABLE a ADD COLUMN added INT DEFAULT 3")
        # EXPLAIN intentionally uses the cached catalog. Observe the remote
        # DDL through the existing fresh-metadata path before checking its
        # catalog-generation invalidation.
        q(b, "SHOW COLUMNS FROM a")
        assert plan(b, sql)["plan"]["join_estimate"] is None
        assert not [s for s in stats(b) if s["Table"].startswith(db + ".")]
        q(b, "ANALYZE TABLE a")
        q(b, "ANALYZE TABLE b")
        assert plan(b, sql)["plan"]["join_estimate"]["reordered"] is True
        assert q(a, sql) == q(b, sql)
        assert q(b, sql)[0][4] == 3
        for query in ("ANALYZE", "ANALYZE a", "ANALYZE TABLE a NOSCAN", "ANALYZE TABLE a FOR COLUMNS id",
                      "ANALYZE TABLE a,b", "SHOW TARN STATISTICS garbage"):
            expect_error(b, query, {1064, 1235})
        print("PASS implicit commit, schema-generation invalidation, altered layout and explicit unsupported syntax")
    finally:
        for c in (a, b):
            try:
                q(c, "ROLLBACK")
            except Exception:
                pass
        q(a, "DROP DATABASE " + db)
        a.close()
        b.close()
    print("ALL STATISTICS/JOIN TESTS PASSED")


if __name__ == "__main__":
    main()
