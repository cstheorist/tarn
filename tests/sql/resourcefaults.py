#!/usr/bin/env python3
"""Resource budgets alongside storage continuations, client cancellation and
slow readers, with exact readback after each failure (#134).

Run by tests/resources.sh after resourcetest.py, on its owned gateways:
`control` (generous budgets), `bounded` (64 MiB per query, 96 MiB for the SQL
server) and `pressure` (64 MiB for a query and for the SQL server). tests/resources.sh runs again with TARN_NATIVE=0 for the Lua
procedures. Each check reads back what a failure must have left: a refusal
or a cancellation never counts as a pass by itself.

  1. A scan refused at one of its later pages (a storage continuation) in an
     explicit transaction rolls the whole transaction back, releases the
     statement's charges, and the session's next transaction commits.
  2. A client that disconnects while its INSERT ... SELECT runs leaves all of
     its rows or none, and the statement's ownership drains.
  3. A reader that stops reading holds its result's ownership; another
     session's statement that would exceed the SQL server's budget meanwhile
     is refused, and succeeds with the exact rows once the reader is gone.
"""
import argparse
import json
import socket
import time

import pymysql

DB = "resource_faults"
WIDE = 30000


def record(kind, **fields):
    print(json.dumps(dict(kind=kind, **fields), sort_keys=True), flush=True)


def connect(address, database=DB):
    host, port = address.split(":")
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=database,
                           autocommit=True, charset="utf8mb4", read_timeout=60, write_timeout=60)


def q(c, sql, args=None):
    with c.cursor() as cursor:
        cursor.execute(sql, args)
        return cursor.fetchall() if cursor.description else cursor.rowcount


def status(c):
    return {k: int(v) for k, v in q(c, "SHOW STATUS")
            if k.startswith(("Tarn_resource_", "Tarn_storage_reads")) and v.lstrip("-").isdigit()}


def payload(i):
    return "w" * 1990 + str(i)


def refused(c, sql, kind):
    """`sql` on `c` fails with 1041 naming `kind` ("query memory budget")."""
    try:
        q(c, sql)
    except pymysql.MySQLError as error:
        assert error.args[0] == 1041 and f"Tarn resource limit: {kind}" in error.args[1], (sql, error.args)
        return error.args[1]
    raise AssertionError(f"expected a refusal by the {kind}: {sql}")


def drained(c, before, slack=1 << 20, timeout=10):
    """The SQL server's query ownership is back to what it was: no query
    but this one, and no more bytes charged than before (plus slack)."""
    deadline = time.monotonic() + timeout
    while True:
        now = status(c)
        if now["Tarn_resource_queries"] <= 1 and now["Tarn_resource_bytes"] <= before["Tarn_resource_bytes"] + slack:
            return now
        if time.monotonic() >= deadline:
            raise AssertionError(f"ownership did not drain: before {before}, now {now}")
        time.sleep(0.05)


def setup(control):
    with connect(control, None) as c:
        q(c, f"DROP DATABASE IF EXISTS {DB}")
        q(c, f"CREATE DATABASE {DB}")
        q(c, f"USE {DB}")
        q(c, "CREATE TABLE wide (id INT PRIMARY KEY, s VARCHAR(2000) NOT NULL)")
        q(c, "CREATE TABLE ledger (id INT PRIMARY KEY, v INT NOT NULL)")
        q(c, "CREATE TABLE copy (id INT PRIMARY KEY, s VARCHAR(2000) NOT NULL)")
        with c.cursor() as cursor:
            for start in range(1, WIDE + 1, 200):
                cursor.executemany("INSERT INTO wide VALUES (%s,%s)",
                                   [(i, payload(i)) for i in range(start, min(start + 200, WIDE + 1))])
            cursor.executemany("INSERT INTO ledger VALUES (%s,0)", [(i,) for i in range(1, 101)])
    record("dataset", wide_rows=WIDE, payload_chars=1995)


def continuation(control, bounded):
    with connect(bounded) as b, connect(control) as c:
        before = status(b)
        q(b, "BEGIN")
        q(b, "UPDATE ledger SET v = v + 1 WHERE id = 1")
        q(b, "INSERT INTO ledger VALUES (1001, 7)")
        # Keeps nearly every row of `wide`: refused once what it keeps no
        # longer fits, after several pages of each shard.
        message = refused(b, "SELECT id, s FROM wide WHERE id % 97 <> 5", "query memory budget")
        after = status(b)
        pages = after["Tarn_storage_reads"] - before["Tarn_storage_reads"]
        assert pages > 4, f"refused before any continuation: {pages} storage reads"
        assert q(c, "SELECT v FROM ledger WHERE id = 1") == ((0,),), "the refused transaction's update stayed"
        assert q(c, "SELECT COUNT(*) FROM ledger WHERE id = 1001") == ((0,),), "its insert stayed"
        drained(b, before)
        q(b, "COMMIT")   # (nothing left to commit)
        q(b, "BEGIN")
        q(b, "UPDATE ledger SET v = v + 1 WHERE id = 2")
        q(b, "COMMIT")
        assert q(c, "SELECT id, v FROM ledger WHERE id <= 2 ORDER BY id") == ((1, 0), (2, 1))
        record("continuation_refusal", storage_reads=pages, message=message)


def cancellation(control, rows=3000, delays=(0.02, 0.05, 0.1, 1, 3)):
    """A client disconnects while its INSERT ... SELECT runs, after each of
    `delays` seconds (the last ones after it finished): all of the rows or
    none."""
    want = (rows, rows * (rows + 1) // 2)
    outcomes = []
    with connect(control) as c:
        for delay in delays:
            before = status(c)
            gone = connect(control)
            gone._execute_command(3, f"INSERT INTO copy SELECT id, s FROM wide WHERE id <= {rows}".encode())
            time.sleep(delay)
            gone._sock.close()
            drained(c, before)
            # The statement is all or nothing; its commit may still finish
            # after the disconnect, so wait until the count settles.
            deadline, last = time.monotonic() + 10, None
            while True:
                got = q(c, "SELECT COUNT(*), COALESCE(SUM(id), 0) FROM copy")[0]
                got = (int(got[0]), int(got[1]))
                if got == last or time.monotonic() >= deadline:
                    break
                last = got
                time.sleep(0.2)
            assert got in ((0, 0), want), f"a cancelled INSERT ... SELECT left {got}, not all {want} or none"
            if got == want:
                bad = q(c, "SELECT COUNT(*) FROM copy c JOIN wide w ON c.id = w.id WHERE c.s <> w.s")
                assert bad == ((0,),), "copied rows differ from their source"
            outcomes.append("all" if got == want else "none")
            q(c, "DELETE FROM copy")
            assert q(c, "SELECT COUNT(*) FROM copy") == ((0,),)
        q(c, "INSERT INTO copy VALUES (1, 'after')")
        assert q(c, "SELECT id, s FROM copy") == ((1, "after"),)
        q(c, "DELETE FROM copy")
    assert "all" in outcomes, f"no INSERT ... SELECT finished, even after {delays[-1]} s: {outcomes}"
    record("cancelled_insert_select", rows=rows, delays=delays, outcomes=outcomes)


def slow_reader(pressure):
    """On a SQL server whose budget is no more than one query's (64 MiB), a
    reader that stops reading a 5,000-row result keeps its ownership.
    Another session's 6,000-row statement, which fits alone, is refused
    meanwhile, and succeeds with the exact rows once the reader is gone."""
    sql = "SELECT id, s FROM wide WHERE id BETWEEN 10001 AND 16000 ORDER BY id"
    want = tuple((i, payload(i)) for i in range(10001, 16001))
    with connect(pressure) as b, connect(pressure) as monitor:
        before = status(monitor)
        reader = connect(pressure)
        reader._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        reader._execute_command(3, b"SELECT id, s FROM wide WHERE id <= 5000")
        # Its statement has finished and waits for the reader once what it
        # holds has stayed the same for a while.
        held, same, last = None, 0, None
        for _ in range(500):
            time.sleep(0.02)
            now = status(monitor)
            charged = now["Tarn_resource_bytes"] - before["Tarn_resource_bytes"]
            same = same + 1 if charged == last else 0
            last = charged
            if now["Tarn_resource_queries"] >= 2 and charged > 24 << 20 and same >= 10:
                held = charged
                break
        assert held, f"a reader that stopped reading released its result's ownership: {now}"
        message = refused(b, sql, "gateway memory budget")
        assert q(b, "SELECT 1") == ((1,),), "the session is unusable after the refusal"
        reader.close()
        drained(monitor, before)
        assert q(b, sql) == want, "the statement after the reader left returned other rows"
        record("slow_reader", held_bytes=held, message=message)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--control", required=True)
    ap.add_argument("--bounded", required=True)
    ap.add_argument("--pressure", required=True, help="a gateway whose server budget is one query's")
    a = ap.parse_args()
    setup(a.control)
    continuation(a.control, a.bounded)
    cancellation(a.control)
    slow_reader(a.pressure)
    record("passed")


if __name__ == "__main__":
    main()
