#!/usr/bin/env python3
"""Paired key histories; isolated schemas, exact traces, no mutation retry."""
import argparse
import decimal
import hashlib
import importlib.metadata
import json
from pathlib import Path
import time

import mysql.connector
import pymysql

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = json.loads((ROOT / "contracts/key-semantics.json").read_text())
ORACLE_VERSION = MANIFEST["oracle"]["version"]


def normalize(x):
    if isinstance(x, decimal.Decimal):
        return str(x)
    if isinstance(x, (bytes, bytearray)):
        return {"hex": bytes(x).hex()}
    if isinstance(x, (list, tuple)):
        return [normalize(y) for y in x]
    if isinstance(x, dict):
        return {k: normalize(v) for k, v in x.items()}
    return x


class Client:
    def __init__(self, address, kind, protocol, engine):
        host, port = address.rsplit(":", 1)
        self.kind, self.protocol, self.engine = kind, protocol, engine
        cfg = dict(host=host, port=int(port), user="root", password="", autocommit=True)
        if protocol == "pymysql-text":
            self.c = pymysql.connect(**cfg, charset="utf8mb4")
        else:
            self.c = mysql.connector.connect(**cfg, use_pure=True, charset="utf8mb4", collation="utf8mb4_bin")
        self.q("SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin")
        self.q("SET sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")
        self.q("SET time_zone='+00:00'")
        self.q("CREATE DATABASE IF NOT EXISTS key_contracts")
        self.q("USE key_contracts")
        self.version = self.q("SELECT VERSION()")
        if kind == "tarn":
            self.q("SET tarn_key_contract='MYSQL'")
        self.trace = []

    def q(self, sql, params=None, record=False):
        # MySQL rejects USE/SET NAMES through COM_STMT_PREPARE (1295).
        # The measured data queries and writes still use the binary path.
        prepared = isinstance(sql, str) and sql.lstrip().upper().startswith(("SELECT ", "INSERT ", "UPDATE ", "DELETE "))
        cur = self.c.cursor() if self.protocol == "pymysql-text" else self.c.cursor(prepared=prepared)
        try:
            cur.execute(sql, params)
            result = normalize(cur.fetchall()) if cur.description else cur.rowcount
            if record:
                self.trace.append([sql, normalize(params), result])
            return result
        finally:
            cur.close()

    def error(self, sql, params=None):
        try:
            self.q(sql, params)
        except (pymysql.MySQLError, mysql.connector.Error) as e:
            code = e.args[0] if isinstance(e, pymysql.MySQLError) else e.errno
            self.trace.append([sql, normalize(params), {"error": code}])
            return code
        raise AssertionError("expected refusal: " + sql)

    def fresh(self, name, columns, placement=""):
        self.q("DROP TABLE IF EXISTS " + name)
        engine = self.engine if self.kind == "tarn" else "InnoDB"
        suffix = placement if self.kind == "tarn" else ""
        self.q(f"CREATE TABLE {name} ({columns}) ENGINE={engine} DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin {suffix}")


def text_keys(s):
    s.fresh("text_keys", "id BIGINT PRIMARY KEY, k VARCHAR(64), UNIQUE KEY uk(k)")
    values = ["a", "a ", "a\t", "A", "é", "e\u0301", "e", "😀", "\x00", None]
    for i, value in enumerate(values):
        s.q("INSERT INTO text_keys VALUES (%s,%s)", (i, value))
    s.q("INSERT INTO text_keys VALUES (20,NULL)")
    assert s.error("INSERT INTO text_keys VALUES (30,'a')") == 1062
    s.q("SELECT id,k FROM text_keys ORDER BY k,id", record=True)
    for value in values[:-1]:
        s.q("SELECT id,k FROM text_keys WHERE k=%s ORDER BY id", (value,), record=True)
    s.q("SELECT COUNT(DISTINCT k) FROM text_keys", record=True)
    s.q("SELECT k,COUNT(*) FROM text_keys GROUP BY k ORDER BY k", record=True)
    # Keyset pagination includes an explicit stable primary-key tie breaker.
    rows = s.q("SELECT k,id FROM text_keys WHERE k IS NOT NULL ORDER BY k,id")
    seen = []
    page = s.q("SELECT k,id FROM text_keys WHERE k IS NOT NULL ORDER BY k,id LIMIT 2")
    while page:
        seen.extend(page)
        page = s.q("SELECT k,id FROM text_keys WHERE (k,id)>(%s,%s) ORDER BY k,id LIMIT 2", tuple(page[-1]))
        assert len(seen) <= len(rows)
    assert seen == rows, (seen, rows)
    s.trace.append(["keyset", seen])
    s.q("SELECT a.id,b.id FROM text_keys a JOIN text_keys b ON a.k=b.k ORDER BY a.id,b.id", record=True)


def decimal_keys(s):
    s.fresh("decimal_keys", "id DECIMAL(38,2), v BIGINT, r INT NOT NULL DEFAULT 1, PRIMARY KEY(r,id), UNIQUE KEY ud(id), KEY kv(v)", "PARTITION BY KEY(r)")
    for i, key in enumerate(["9007199254740992.01", "9007199254740992.02", "9007199254740993.00", "-9223372036854775808.00", "18446744073709551615.00"]):
        s.q("INSERT INTO decimal_keys(id,v) VALUES (%s,%s)", (decimal.Decimal(key), i))
    s.q("SELECT id,v FROM decimal_keys ORDER BY id", record=True)
    s.q("SELECT DISTINCT id FROM decimal_keys ORDER BY id", record=True)
    s.q("SELECT MIN(id),MAX(id),COUNT(DISTINCT id) FROM decimal_keys", record=True)
    for key in ["9007199254740992.0100", "9007199254740992.02", "9007199254740993", "18446744073709551615.000", "9007199254740992.011"]:
        s.q(f"SELECT id,v FROM decimal_keys WHERE id={key} ORDER BY id", record=True)
    s.q("SELECT id FROM decimal_keys WHERE id>9007199254740992.011 ORDER BY id LIMIT 2", record=True)
    s.q("SELECT id FROM decimal_keys WHERE id=100000000000000000000000000000000000000000 ORDER BY id", record=True)
    s.q("SELECT id FROM decimal_keys WHERE id<100000000000000000000000000000000000000000 ORDER BY id", record=True)
    s.q("SELECT a.id,b.v FROM decimal_keys a JOIN decimal_keys b ON a.id=b.id ORDER BY a.id", record=True)
    assert s.error("INSERT INTO decimal_keys(id,v) VALUES (9007199254740992.010,9)") == 1062
    s.fresh("integer_keys", "id BIGINT UNSIGNED PRIMARY KEY, v BIGINT")
    for sql in ["INSERT INTO integer_keys VALUES (9007199254740993.0,9007199254740993.0)",
                "INSERT INTO integer_keys VALUES (18446744073709551615,-9223372036854775808)"]:
        s.q(sql)
    s.q("SELECT id,v FROM integer_keys ORDER BY id", record=True)
    s.q("SELECT id FROM integer_keys WHERE id=9007199254740993.0", record=True)
    assert s.error("INSERT INTO integer_keys VALUES (18446744073709551616,0)") == 1264
    assert s.error("INSERT INTO integer_keys VALUES ('10000000000000000000000000000000000000000',0)") == 1264
    assert s.error("INSERT INTO integer_keys VALUES (-1,0)") == 1264
    s.q("SELECT id,v FROM integer_keys ORDER BY id", record=True)
    s.fresh("decimal_round", "id INT PRIMARY KEY,d DECIMAL(5,2) UNSIGNED,UNIQUE KEY ud(d)")
    s.q("INSERT INTO decimal_round VALUES (1,9.995)")
    s.q("SELECT id,d FROM decimal_round ORDER BY id", record=True)
    assert s.error("INSERT INTO decimal_round VALUES (2,10.000)") == 1062
    assert s.error("INSERT INTO decimal_round VALUES (3,999.995)") == 1264
    assert s.error("INSERT INTO decimal_round VALUES (4,-0.01)") == 1264
    s.q("SELECT id,d FROM decimal_round ORDER BY id", record=True)


def binary_keys(s):
    s.fresh("binary_keys", "id INT PRIMARY KEY, k VARBINARY(8), UNIQUE KEY uk(k)")
    for i, value in enumerate([b"a", b"a ", b"a\0", b"\0", b"\xff", b"A", None]):
        if s.protocol == "pymysql-text" and value is not None:
            s.q(f"INSERT INTO binary_keys VALUES ({i},X'{value.hex()}')")
        else:
            s.q("INSERT INTO binary_keys VALUES (%s,%s)", (i, value))
    s.q("SELECT id,k FROM binary_keys ORDER BY k,id", record=True)
    assert s.error("INSERT INTO binary_keys VALUES (10,X'6100')") == 1062
    assert s.error("INSERT INTO binary_keys VALUES (11,X'000102030405060708')") == 1406
    s.q("SELECT id,k FROM binary_keys ORDER BY id", record=True)


def lazy_keys(s):
    # This is a settled-index key encoding/order control, not a claim that
    # the explicit eventual-read extension matches ordinary MySQL reads.
    s.fresh("lazy_keys", "r INT,id INT,k VARCHAR(64),n BIGINT,PRIMARY KEY(r,id),KEY lk(k,n,id) COMMENT 'tarn:lazy'", "PARTITION BY KEY(r)")
    if s.kind == "tarn":
        s.q("SET tarn_lazy_read_your_writes=1")
    for r, i, k, n in [(1,1,"a",9007199254740993),(2,2,"a",9007199254740992),(1,3,"a ",4),(3,4,"é",5),(4,5,"e\u0301",6)]:
        s.q("INSERT INTO lazy_keys VALUES (%s,%s,%s,%s)", (r,i,k,n))
    for key in ["a", "a ", "é", "e\u0301"]:
        s.q("SELECT r,id,k,n FROM lazy_keys WHERE k=%s ORDER BY n,id", (key,), record=True)
    s.q("UPDATE lazy_keys SET k='a ' WHERE r=1 AND id=1")
    s.q("DELETE FROM lazy_keys WHERE r=2 AND id=2")
    for key in ["a", "a "]:
        s.q("SELECT r,id,k,n FROM lazy_keys WHERE k=%s ORDER BY n,id", (key,), record=True)


def schema_keys(s):
    s.fresh("schema_keys", "r INT,k VARCHAR(32),v DECIMAL(30,2),PRIMARY KEY(r,k),UNIQUE KEY uv(v)", "PARTITION BY KEY(r)")
    s.q("INSERT INTO schema_keys VALUES (1,'a ',9007199254740993.01)")
    s.q("ALTER TABLE schema_keys ADD COLUMN n INT NOT NULL DEFAULT 7 FIRST")
    s.q("ALTER TABLE schema_keys ADD INDEX kn(k,n)")
    s.q("SELECT n,r,k,v FROM schema_keys WHERE k='a ' ORDER BY r,k", record=True)
    s.q("SELECT n,r,k,v FROM schema_keys WHERE v=9007199254740993.0100 ORDER BY r,k", record=True)
    if s.kind == "tarn":
        before = s.q("SHOW CREATE TABLE schema_keys")
        for statement in ["ALTER TABLE schema_keys MODIFY k VARCHAR(32) COLLATE utf8mb4_bin", "ALTER TABLE schema_keys MODIFY v DECIMAL(30,3)"]:
            assert s.error(statement) == 1235
            assert s.q("SHOW CREATE TABLE schema_keys") == before
        # Refusals are deliberate differences; retain separately from the
        # shared successful trace rather than compare arbitrary errors.
        s.trace = [x for x in s.trace if not (len(x) == 3 and isinstance(x[2], dict) and "error" in x[2])]


def temporal_legacy(s):
    # Temporal keys remain explicitly outside MYSQL key mode. Retain the
    # independently supported normalization control in the legacy profile.
    if s.kind == "tarn":
        s.q("SET tarn_key_contract='LEGACY'")
    s.fresh("temporal_keys", "id INT PRIMARY KEY,d DATETIME(6),UNIQUE KEY ud(d)")
    s.q("INSERT INTO temporal_keys VALUES (1,'2024-1-2T03:04:05.12')")
    assert s.error("INSERT INTO temporal_keys VALUES (2,'20240102030405.120000')") == 1062
    s.q("SELECT id FROM temporal_keys WHERE d='2024-01-02 03:04:05.120000' ORDER BY id", record=True)


def pad_legacy(s):
    if s.kind == "tarn":
        s.q("SET tarn_key_contract='LEGACY'")
    # Intentionally different: retain the difference instead of calling
    # Tarn's existing byte encoding MySQL PAD SPACE compatibility.
    rows = s.q("SELECT 'a' COLLATE utf8mb4_bin = 'a ' COLLATE utf8mb4_bin", record=True)
    assert rows == [[0 if s.kind == "tarn" else 1]]
    if s.kind == "tarn":
        s.q("SET tarn_key_contract='MYSQL'")
        assert s.error("SELECT 'a' COLLATE utf8mb4_bin = 'a ' COLLATE utf8mb4_bin") == 1235


def refusals(s):
    if s.kind != "tarn":
        return []
    s.fresh("guard", "id INT PRIMARY KEY, v INT")
    s.q("BEGIN")
    s.q("INSERT INTO guard VALUES (1,10)")
    for sql in ["CREATE TABLE refused (id INT PRIMARY KEY,k VARCHAR(16) COLLATE utf8mb4_0900_ai_ci)",
                "CREATE TABLE refused (id INT PRIMARY KEY,k VARCHAR(16) COLLATE utf8mb4_bin)",
                "CREATE TABLE refused (id DECIMAL(65,30) PRIMARY KEY)",
                "CREATE TABLE refused (id DATETIME PRIMARY KEY)",
                "CREATE TABLE refused (id INT PRIMARY KEY,k BIGINT DEFAULT 1e3)",
                "CREATE TABLE refused (id INT PRIMARY KEY,k VARCHAR(32) DEFAULT CURRENT_TIMESTAMP)",
                "CREATE TABLE refused (id INT PRIMARY KEY,k VARBINARY(8) DEFAULT X'ff')",
                "ALTER TABLE guard ADD k VARCHAR(16) COLLATE utf8mb4_unicode_ci",
                "SELECT CAST(id AS DOUBLE) FROM guard",
                "SELECT id FROM guard WHERE id='1'",
                "SELECT SUM(v) FROM guard",
                "SET NAMES latin1",
                "SET collation_connection='utf8mb4_0900_ai_ci'"]:
        assert s.error(sql) == 1235
    assert s.q("SELECT id,v FROM guard") == [[1,10]]
    s.q("ROLLBACK")
    assert s.q("SELECT id,v FROM guard") == []
    assert s.q("SHOW TABLES LIKE 'refused'") == []
    plan = json.loads(s.q("EXPLAIN FORMAT=JSON SELECT id,v FROM guard")[0][0])
    assert plan["key_contract"]["requested"] == "MYSQL"
    s.q("SET tarn_key_contract='LEGACY'")
    s.q("DROP TABLE IF EXISTS legacy_keys")
    s.q("CREATE TABLE legacy_keys (id INT PRIMARY KEY,k VARCHAR(16) COLLATE utf8mb4_0900_ai_ci)")
    plan = json.loads(s.q("EXPLAIN FORMAT=JSON SELECT k FROM legacy_keys")[0][0])
    assert plan["key_contract"]["requested"] == "LEGACY"
    assert not plan["key_contract"]["tables"][0]["mysql_key_definition"]
    if s.protocol == "connector-binary":
        # Reuse the very same prepared handle across the profile transition.
        cur = s.c.cursor(prepared=True)
        cur.execute("SELECT k FROM legacy_keys")
        cur.fetchall()
        s.q("SET tarn_key_contract='MYSQL'")
        try:
            cur.execute("SELECT k FROM legacy_keys")
        except mysql.connector.Error as e:
            assert e.errno == 1235
            s.trace.append(["legacy prepared handle under MYSQL", {"error": e.errno}])
        else:
            raise AssertionError("cached legacy plan bypassed key contract")
        finally:
            cur.close()
    else:
        s.q("SET tarn_key_contract='MYSQL'")
    assert s.error("SELECT k FROM legacy_keys") == 1235
    if s.protocol == "pymysql-text":
        try:
            s.q(b"INSERT INTO guard VALUES (2,20); INSERT INTO guard VALUES (3,'\xff')")
        except pymysql.MySQLError as e:
            assert e.args[0] == 1300, e
        else:
            raise AssertionError("invalid UTF-8 SQL must be refused")
        assert s.q("SELECT id,v FROM guard") == []
    return s.trace


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tarn", required=True)
    p.add_argument("--ref", required=True)
    p.add_argument("--engine", choices=["memtx", "vinyl"], default="memtx")
    p.add_argument("--output", required=True)
    args = p.parse_args()
    report = {"schema": 1, "engine": args.engine, "oracle": MANIFEST["oracle"],
              "clients": {"pymysql": importlib.metadata.version("PyMySQL"), "mysql.connector": mysql.connector.__version__},
              "source_sha256": {str(f.relative_to(ROOT)): hashlib.sha256(f.read_bytes()).hexdigest() for directory in ["crates", "lua"] for f in sorted((ROOT / directory).rglob("*")) if f.is_file()}, "runs": [], "started": time.time()}
    try:
        for protocol in ["pymysql-text", "connector-binary"]:
            for case in [text_keys, decimal_keys, binary_keys, lazy_keys, schema_keys, temporal_legacy, pad_legacy, refusals]:
                results = []
                for kind, address in [("mysql", args.ref), ("tarn", args.tarn)]:
                    s = Client(address, kind, protocol, args.engine)
                    record = {"case": case.__name__, "kind": kind, "protocol": protocol, "trace": s.trace}
                    report["runs"].append(record)
                    try:
                        version = s.version
                        if kind == "mysql":
                            assert version == [[ORACLE_VERSION]], version
                        record["version"] = version
                        case(s)
                        record["status"] = "passed"
                        results.append(s.trace)
                    except Exception as exc:
                        record.update(status="failed", error=repr(exc))
                        raise
                    finally:
                        s.c.close()
                if case not in (refusals, pad_legacy):
                    assert results[0] == results[1], (case.__name__, protocol, results)
                print(protocol, case.__name__, "PASS", flush=True)
        report["status"] = "passed"
    except Exception as exc:
        report.update(status="failed", error=repr(exc))
        raise
    finally:
        Path(args.output).write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
