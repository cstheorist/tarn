#!/usr/bin/env python3
"""Resource refusals, bounded ORDER/LIMIT, and overload on owned gateways.

Run tests/resources.sh; this program never changes a gateway's limits or stops
services. JSONL stdout records every control and measurement. SQL successes are
compared with MariaDB; 1041/HY000 is Tarn's explicit resource-policy refusal.
"""
import argparse
import concurrent.futures
import contextlib
import json
import math
import os
import platform
import subprocess
import socket
import struct
import threading
import time

import pymysql

DB = "resource_test"


def record(kind, **fields):
    print(json.dumps(dict(kind=kind, **fields), sort_keys=True), flush=True)


def connect(address, database=DB):
    host, port = address.split(":")
    return pymysql.connect(host=host, port=int(port), user="sb", password="",
                           database=database, autocommit=True, charset="utf8mb4",
                           read_timeout=30, write_timeout=30)


def q(c, sql, args=None):
    with c.cursor() as cursor:
        cursor.execute(sql, args)
        return cursor.fetchall() if cursor.description else cursor.rowcount


def refuse(c, sql):
    try:
        q(c, sql)
    except pymysql.MySQLError as error:
        assert error.args[0] == 1041 and "Tarn resource limit:" in error.args[1], (sql, error.args)
        record("refusal", sql=sql, code=1041, message=error.args[1])
    else:
        raise AssertionError("expected resource refusal: " + sql)
    assert q(c, "SELECT 1") == ((1,),), "session unusable after refusal"


def status(c):
    return {k: int(v) for k, v in q(c, "SHOW STATUS") if k.startswith("Tarn_resource_") or k == "Tarn_topk_scans"}


@contextlib.contextmanager
def rss(pid):
    samples = []
    stop = threading.Event()
    def sample():
        while not stop.is_set():
            with open(f"/proc/{pid}/statm") as stream:
                samples.append(int(stream.read().split()[1]) * os.sysconf("SC_PAGE_SIZE"))
            stop.wait(.01)
    thread = None
    if pid:
        thread = threading.Thread(target=sample)
        thread.start()
    try:
        yield samples
    finally:
        stop.set()
        if thread:
            thread.join()


def percentiles(values):
    values = sorted(values)
    return {key: values[min(len(values)-1, math.ceil(p*len(values))-1)]
            for key, p in (("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99), ("max_ms", 1))}


def protocol_ownership(a):
    # Stop reading an eight-megabyte result. The server must hold the query's
    # ownership while blocked on delivery, then release it on disconnect.
    with connect(a.tarn) as monitor:
        slow = connect(a.tarn)
        slow._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        before = status(monitor)
        slow._execute_command(3, b"SELECT s FROM t")
        held = None
        for _ in range(100):
            time.sleep(.01)
            seen = status(monitor)
            if seen["Tarn_resource_queries"] >= 2 and seen["Tarn_resource_bytes"] > before["Tarn_resource_bytes"] + (8 << 20):
                held = seen
                break
        assert held, "blocked output released query ownership"
        slow.close()
        for _ in range(100):
            after = status(monitor)
            if after["Tarn_resource_queries"] <= 1:
                break
            time.sleep(.01)
        assert after["Tarn_resource_queries"] <= 1, after
        record("slow_client", before=before, held=held, after=after)
    # Long parameter chunks have no immediate response. Admission failure
    # must be remembered until EXECUTE, then RESET/close must free ownership.
    with connect(a.bounded) as c:
        c._execute_command(0x16, b"SELECT ?")
        prepared = c._read_packet()._data
        statement, columns, params = struct.unpack_from("<IHH", prepared, 1)
        assert params == 1
        for count in (params, columns):
            for _ in range(count):
                c._read_packet()
            if count:
                c._read_packet()  # EOF
        for _ in range(273):
            c._execute_command(0x18, struct.pack("<IH", statement, 0) + b"x"*65536)
        execute = struct.pack("<IBI", statement, 0, 1) + b"\0\1\xfd\0"
        c._execute_command(0x17, execute)
        try:
            c._read_packet()
        except pymysql.MySQLError as error:
            assert error.args[0] == 1041, error
        else:
            raise AssertionError("long-data admission error was forgotten")
        c._execute_command(0x1a, struct.pack("<I", statement))
        assert c._read_packet().is_ok_packet()
        c._execute_command(0x17, execute + b"\3abc")
        assert c._read_packet()._data == b"\1"
        c._read_packet()  # column
        c._read_packet()  # EOF
        assert c._read_packet()._data == b"\0\0\3abc"
        c._read_packet()  # EOF
        c._execute_command(0x19, struct.pack("<I", statement))
        assert q(c, "SELECT 1") == ((1,),)
        record("binary_long_data_refusal_and_reset", chunks=273, chunk_bytes=65536)


def run(a):
    with connect(a.tarn, None) as c, connect(a.ref, None) as reference:
        record("environment", platform=platform.platform(), python=platform.python_version(),
               tarantool=subprocess.run([os.environ.get("TARANTOOL", "tarantool"), "--version"],
                                       capture_output=True, text=True, check=True).stdout.splitlines()[0],
               gateway=q(c, "SELECT VERSION()")[0][0], reference=q(reference, "SELECT VERSION()")[0][0])
    # Generic, unindexed sort keys; a unique last key makes SQL ordering exact.
    for address in (a.tarn, a.ref):
        with connect(address, None) as c:
            q(c, f"DROP DATABASE IF EXISTS {DB}")
            q(c, f"CREATE DATABASE {DB}")
            q(c, f"USE {DB}")
            q(c, "CREATE TABLE t (id INT PRIMARY KEY, n INT, s VARCHAR(512) COLLATE utf8mb4_unicode_ci, v INT)")
            for start in range(0, a.rows, 100):
                values = [(i+1, None if i % 19 == 0 else i % 17,
                           ("É" if i % 5 == 0 else "z") + str(i % 7) + "x"*380, 0)
                          for i in range(start, min(start+100, a.rows))]
                with c.cursor() as cursor:
                    cursor.executemany("INSERT INTO t VALUES (%s,%s,%s,%s)", values)
            q(c, "CREATE TABLE copied (id INT PRIMARY KEY, n INT, s VARCHAR(512) COLLATE utf8mb4_unicode_ci, v INT)")
    record("dataset", rows=a.rows, payload_chars=382)
    with connect(a.tarn) as control, connect(a.bounded) as bounded, connect(a.ref) as reference:
        before = status(bounded)
        for order in ("n,id", "n DESC,id", "s,id DESC", "s COLLATE utf8mb4_bin DESC,id", "n DESC,s,id"):
            for offset in (0, 13, 300):
                sql = f"SELECT id,n,s FROM t WHERE v=0 ORDER BY {order} LIMIT 19 OFFSET {offset}"
                assert q(bounded, sql) == q(reference, sql), sql
                record("differential", sql=sql)
        assert status(bounded)["Tarn_topk_scans"] > before["Tarn_topk_scans"], "top-K path not exercised"
        # These shapes deliberately materialize, or exceed even bounded K.
        for sql in ("SELECT * FROM t ORDER BY n", "SELECT DISTINCT s FROM t",
                    "SELECT n,GROUP_CONCAT(s) FROM t GROUP BY n",
                    "SELECT a.id,b.id,CONCAT(a.s,b.s) FROM t a JOIN t b ON a.v=b.v WHERE a.id<=256 AND b.id<=256",
                    "SELECT id FROM t ORDER BY n LIMIT 1 OFFSET 9999999999",
                    "SELECT REPEAT('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',1000000)",
                    "SELECT LPAD('x',999999999,'y')"):
            refuse(bounded, sql)
        # Admission errors happen before commit, including ordered DML.
        for sql in ("UPDATE t SET v=v+1 ORDER BY n", "INSERT INTO copied SELECT * FROM t",
                    "UPDATE t SET s=REPEAT(s,1000000) WHERE id=1"):
            refuse(bounded, sql)
            assert q(control, "SELECT SUM(v) FROM t") == ((0,),)
            assert q(control, "SELECT COUNT(*) FROM copied") == ((0,),)
        q(bounded, "BEGIN")
        q(bounded, "UPDATE t SET v=7 WHERE id=1")
        refuse(bounded, "SELECT LPAD('x',999999999,'y')")
        q(bounded, "COMMIT")
        assert q(control, "SELECT v FROM t WHERE id=1") == ((0,),), "resource error kept staged writes"
        # Existing buffered writes disable the heap; small ordered overlay is exact.
        q(bounded, "BEGIN")
        q(bounded, "UPDATE t SET n=-9 WHERE id=1")
        assert q(bounded, "SELECT id,n FROM t WHERE id<=8 ORDER BY n,id LIMIT 4") == ((1,-9),(2,1),(3,2),(4,3))
        q(bounded, "ROLLBACK")
        # SQL prepared execution and session-owned strings use the same budget.
        q(bounded, "PREPARE p FROM 'SELECT id FROM t ORDER BY n,id LIMIT 3'")
        assert q(bounded, "EXECUTE p") == q(reference, "SELECT id FROM t ORDER BY n,id LIMIT 3")
        q(bounded, "DEALLOCATE PREPARE p")
        refuse(bounded, "SET @huge=REPEAT('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',1000000)")
        record("bounded_status", status=status(bounded))
    # Resource errors end the complete explicit transaction, freeing row locks.
    with connect(a.transaction) as txn, connect(a.tarn) as control:
        for kind in ("reads", "writes", "savepoints"):
            q(txn, "BEGIN")
            q(txn, "UPDATE t SET v=7 WHERE id=1")
            for i in range(2, 1002):
                sql = {"reads": f"SELECT id,s FROM t WHERE id={i}",
                       "writes": f"UPDATE t SET v=8 WHERE id={i}",
                       "savepoints": f"SAVEPOINT s{i}"}[kind]
                try:
                    q(txn, sql)
                except pymysql.MySQLError as error:
                    assert error.args[0] == 1041, error
                    record("transaction_refusal", operation=kind, statements=i-1, message=error.args[1])
                    break
            else:
                raise AssertionError("transaction state not bounded: " + kind)
            q(txn, "COMMIT")
            assert q(control, "SELECT SUM(v) FROM t") == ((0,),)
            assert q(control, "UPDATE t SET v=0 WHERE id=1") == 0
    with connect(a.work) as work:
        refuse(work, "SELECT id FROM t ORDER BY n,id LIMIT 20")
        refuse(work, "SELECT REPEAT('x',4096) LIKE REPEAT('x',4096)")
    protocol_ownership(a)
    # Same executable, data, caps, OCC, SQL and repeats: only heap eligibility
    # differs. Report all timings and RSS; shared-host runs imply no SLA claim.
    sql = "SELECT id,n,s FROM t ORDER BY n,id LIMIT 20 OFFSET 13"
    expected = None
    for label, address, pid in (("topk", a.matched, a.matched_pid), ("materialize", a.materialized, a.materialized_pid)):
        with connect(address) as c:
            times = []
            with rss(pid) as samples:
                for repeat in range(3):
                    start = time.monotonic()
                    result = q(c, sql)
                    times.append((time.monotonic()-start)*1000)
                    if expected is None:
                        expected = result
                    assert result == expected
            record("matched_order_limit", path=label, sql=sql, repeats_ms=times,
                   rss_first=samples[0] if samples else None, rss_peak=max(samples, default=None), status=status(c))
    # Repeated oversized work beside useful point reads: actual measured
    # success throughput and tails, including refusals and connection failures.
    durations, useful, refused, other = [], 0, 0, []
    ready = threading.Barrier(12)
    def worker(index):
        results = []
        try:
            with connect(a.bounded) as c:
                ready.wait(timeout=15)
                for i in range(24):
                    small = (index+i) % 2 == 0
                    sql = "SELECT v FROM t WHERE id=1" if small else "SELECT * FROM t ORDER BY n"
                    start = time.monotonic()
                    try:
                        q(c, sql)
                        outcome = "useful" if small else "unexpected_large_success"
                    except pymysql.MySQLError as error:
                        outcome = "refused" if error.args[0] == 1041 else str(error.args)
                    results.append((outcome, (time.monotonic()-start)*1000))
        except pymysql.MySQLError as error:
            results.append((str(error.args), 0))
        return results
    started = time.monotonic()
    with rss(a.bounded_pid) as samples:
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            for results in pool.map(worker, range(12)):
                record("overload_samples", samples=results)
                for outcome, elapsed in results:
                    durations.append(elapsed)
                    useful += outcome == "useful"
                    refused += outcome == "refused"
                    if outcome not in ("useful", "refused"):
                        other.append(outcome)
    seconds = time.monotonic()-started
    assert useful > 0 and refused > 0 and not other, (useful, refused, other)
    with connect(a.bounded) as c:
        after = status(c)
        assert after["Tarn_resource_peak_bytes"] <= a.server_bytes, after
        assert q(c, "SELECT v FROM t WHERE id=1") == ((0,),)
        # Query/stream/result ownership has drained. Session buffers and cache
        # entries remain charged, so a zero-byte assertion would be incorrect.
        assert after["Tarn_resource_queries"] <= 1, after
    record("overload", clients=12, operations=len(durations), useful=useful, refused=refused,
           elapsed_s=seconds, useful_per_s=useful/seconds, other=other,
           rss_first=samples[0] if samples else None, rss_peak=max(samples, default=None),
           status=after, **percentiles(durations))
    record("passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("tarn", "bounded", "transaction", "work", "materialized", "matched", "ref"):
        parser.add_argument("--"+name, required=True)
    for name in ("pid", "bounded-pid", "materialized-pid", "matched-pid"):
        parser.add_argument("--"+name, type=int)
    parser.add_argument("--rows", type=int, default=20000)
    parser.add_argument("--server-bytes", type=int, default=96 << 20)
    run(parser.parse_args())
