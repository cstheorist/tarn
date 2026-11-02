#!/usr/bin/env python3
"""ALTER TABLE builds new secondary indexes on replicated shards by copying
the table into a new space whose indexes exist before its rows (#267).

Run by tests/indexcopy.sh on 2 shards x 3 replicas with fsync. For a memtx
and a vinyl table, while one session updates the table and another inserts
into a second table:

  * ADD INDEX with ADD UNIQUE INDEX succeeds and replaces the table's space,
    and no replica's election term changes meanwhile;
  * the writes go on during the change, and none fails;
  * every replica holds the same rows and the same indexes, and each index
    holds every row;
  * the new unique index refuses a duplicate;
  * a duplicate in a table fails ADD UNIQUE INDEX with 1062 and leaves that
    table as it was, with no copy left on any replica;
  * an engine change, whose copy now has its indexes from the start too,
    keeps the terms as they were.

    indexcopytest.py --tarn HOST:PORT --cluster-dir DIR [--rows N]
"""
import argparse
import json
from pathlib import Path
import random
import threading
import time

import pymysql

from lazytest import ev

DB = "index_copy"
ELECTION = "local e = box.info.election; return {term = e.term, state = e.state, ro = box.info.ro}"


def connect(endpoint, db=DB, read_timeout=300):
    host, port = endpoint.rsplit(":", 1)
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db, autocommit=True,
                           charset="utf8mb4", connect_timeout=3, read_timeout=read_timeout, write_timeout=10)


def q(c, sql, args=None):
    with c.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall() if cur.description else cur.rowcount


fails = 0


def check(name, cond, detail=None):
    global fails
    if cond:
        print(f"  ok   {name}", flush=True)
    else:
        fails += 1
        print(f"  FAIL {name}" + (f"  -- {json.dumps(detail, default=str)}" if detail is not None else ""),
              flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tarn", required=True)
    ap.add_argument("--cluster-dir", required=True)
    ap.add_argument("--rows", type=int, default=30000)
    args = ap.parse_args()
    groups = json.loads((Path(args.cluster_dir) / "cluster.json").read_text())["shards"]
    nodes = [n for g in groups for n in g["replicas"]]
    assert len(groups) == 2 and all(len(g["replicas"]) == 3 for g in groups), groups

    def leader(group):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            for node in group["replicas"]:
                try:
                    if not ev(node, "return box.info.ro"):
                        return node
                except Exception:  # a replica that does not answer
                    pass
            time.sleep(0.1)
        raise AssertionError(f"shard {group['id']} has no leader")

    def terms():
        return {node: ev(node, ELECTION)["term"] for node in nodes}

    def caught_up(timeout=60):
        """Every replica has applied what its shard's leader wrote."""
        for g in groups:
            lead = leader(g)
            want = ev(lead, "return {id = box.info.id, lsn = box.info.lsn}")
            for node in g["replicas"]:
                deadline = time.monotonic() + timeout
                while ev(node, "return box.info.vclock[...] or 0", [want["id"]]) < want["lsn"]:
                    if time.monotonic() > deadline:
                        raise AssertionError(f"{node} did not catch up with {lead}")
                    time.sleep(0.1)

    def shape(node, space):
        """The space's id, engine, row count and indexes with their counts."""
        return ev(node, """
            -- (a vinyl count of rows in memory does not yield)
            require('fiber').set_slice({warn = 30, err = 120})
            local sp = box.space[...]
            if sp == nil then return box.NULL end
            local out = {id = sp.id, engine = sp.engine, rows = sp.index[0]:count(), indexes = {}}
            for id, ix in pairs(sp.index) do
                if type(id) == 'number' then out.indexes[ix.name] = {unique = ix.unique, rows = ix:count()} end
            end
            return out
        """, [space], timeout=60)

    def leftovers(node):
        return ev(node, """
            local out = {}
            for _, s in box.space._space:pairs() do
                if s.name:find('^_tarn_engine_') then out[#out + 1] = s.name end
            end
            return out
        """)

    c = connect(args.tarn, db=None)
    q(c, f"DROP DATABASE IF EXISTS {DB}")
    q(c, f"CREATE DATABASE {DB}")
    c = connect(args.tarn)
    q(c, "CREATE TABLE w (id BIGINT PRIMARY KEY, v INT) ENGINE=memtx")

    for engine in ("memtx", "vinyl"):
        t, space = f"t_{engine}", f"{DB}.t_{engine}"
        print(f"{engine} table: {args.rows} rows", flush=True)
        q(c, f"CREATE TABLE {t} (tenant_id INT, id INT, k INT, c INT, PRIMARY KEY (tenant_id, id)) "
             f"ENGINE={engine} PARTITION BY KEY(tenant_id)")
        for first in range(0, args.rows, 1000):
            rows = range(first, min(first + 1000, args.rows))
            q(c, f"INSERT INTO {t} VALUES " + ",".join(f"({i % 64},{i},{i % 97},{i})" for i in rows))
        before = {g["id"]: shape(leader(g), space) for g in groups}
        before_terms = terms()

        stop, errors, latencies = threading.Event(), [], {"table": [], "other": []}
        progress = {"table": 0, "other": 0}

        def writer(kind):
            conn = connect(args.tarn, read_timeout=120)
            rnd = random.Random(kind)
            n = 0
            while not stop.is_set():
                n += 1
                began = time.monotonic()
                try:
                    if kind == "table":
                        i = rnd.randrange(args.rows)
                        q(conn, f"UPDATE {t} SET k = k + 1 WHERE tenant_id = %s AND id = %s", (i % 64, i))
                    else:
                        q(conn, "INSERT INTO w VALUES (%s, %s)", ((1 + (engine == "vinyl")) * 10**9 + n, n))
                    latencies[kind].append(time.monotonic() - began)
                    progress[kind] += 1
                except pymysql.MySQLError as e:
                    errors.append(f"{kind}: {e.args}")
                    time.sleep(0.05)
                time.sleep(0.005)

        threads = [threading.Thread(target=writer, args=(k,), daemon=True) for k in ("table", "other")]
        for th in threads:
            th.start()
        time.sleep(1)
        at = dict(progress)
        began = time.monotonic()
        alter_error = None
        try:
            q(c, f"ALTER TABLE {t} ADD INDEX kk (k), ADD UNIQUE INDEX uc (tenant_id, c)")
        except pymysql.MySQLError as e:
            alter_error = e.args
        took = time.monotonic() - began
        during = {k: progress[k] - at[k] for k in progress}
        time.sleep(1)
        stop.set()
        for th in threads:
            th.join(30)
        check(f"{engine}: ALTER TABLE adds both indexes", alter_error is None, alter_error)
        print(f"  ({engine}: ALTER took {took:.2f} s; writes during it: {during}; "
              f"longest write to the table {max(latencies['table'] or [0]):.3f} s, "
              f"to the other table {max(latencies['other'] or [0]):.3f} s)", flush=True)
        check(f"{engine}: no replica changed its election term", terms() == before_terms,
              {"before": before_terms, "after": terms()})
        check(f"{engine}: writes to both tables went on during the change",
              during["table"] > 0 and during["other"] > 0, during)
        check(f"{engine}: no write failed", not errors, errors[:5])

        caught_up()
        shapes = {node: shape(node, space) for node in nodes}
        for g in groups:
            first = shapes[g["replicas"][0]]
            check(f"{engine}: shard {g['id']}'s table was replaced by its copy",
                  first is not None and first["id"] != before[g["id"]]["id"] and first["engine"] == engine,
                  {"before": before[g["id"]], "after": first})
            check(f"{engine}: shard {g['id']}'s replicas hold the same rows and indexes",
                  all(shapes[n] == first for n in g["replicas"]), {n: shapes[n] for n in g["replicas"]})
            check(f"{engine}: each index of shard {g['id']} holds every row",
                  first is not None and set(first["indexes"]) >= {"kk", "uc"}
                  and all(ix["rows"] == first["rows"] for ix in first["indexes"].values())
                  and first["indexes"]["uc"]["unique"] and first["rows"] == before[g["id"]]["rows"],
                  first)
        total = q(c, f"SELECT COUNT(*) FROM {t}")[0][0]
        check(f"{engine}: the table has its {args.rows} rows", total == args.rows, total)
        dup = None
        try:
            q(c, f"INSERT INTO {t} VALUES (5, %s, 0, 5)", (args.rows + 1,))
        except pymysql.MySQLError as e:
            dup = e.args[0]
        check(f"{engine}: the new unique index refuses a duplicate", dup == 1062, dup)

        # A duplicate the table holds fails the change and leaves the table.
        d, dspace = f"d_{engine}", f"{DB}.d_{engine}"
        q(c, f"CREATE TABLE {d} (tenant_id INT, id INT, c INT, PRIMARY KEY (tenant_id, id)) "
             f"ENGINE={engine} PARTITION BY KEY(tenant_id)")
        q(c, f"INSERT INTO {d} VALUES " + ",".join(f"({i % 8},{i},{i})" for i in range(2000))
             + ",(3,5000,11)")
        d_before = {g["id"]: shape(leader(g), dspace) for g in groups}
        refused = None
        try:
            q(c, f"ALTER TABLE {d} ADD UNIQUE INDEX uc (tenant_id, c)")
        except pymysql.MySQLError as e:
            refused = e.args[0]
        check(f"{engine}: a duplicate in the table fails ADD UNIQUE INDEX with 1062", refused == 1062, refused)
        caught_up()
        d_after = {g["id"]: shape(leader(g), dspace) for g in groups}
        check(f"{engine}: that table is as it was", d_after == d_before, {"before": d_before, "after": d_after})
        left = {node: leftovers(node) for node in nodes}
        check(f"{engine}: no copy is left on any replica", all(not v for v in left.values()), left)
        q(c, f"INSERT INTO {d} VALUES (4, 6000, 6000)")
        check(f"{engine}: that table takes writes", q(c, f"SELECT COUNT(*) FROM {d}")[0][0] == 2002)

    # An engine change copies too, into a space that has its indexes first.
    before_terms = terms()
    try:
        q(c, "ALTER TABLE t_memtx ENGINE=vinyl")
        engine_error = None
    except pymysql.MySQLError as e:
        engine_error = e.args
    check("an engine change succeeds", engine_error is None, engine_error)
    check("no replica changed its election term in the engine change", terms() == before_terms,
          {"before": before_terms, "after": terms()})
    caught_up()
    shapes = {node: shape(node, f"{DB}.t_memtx") for node in nodes}
    for g in groups:
        first = shapes[g["replicas"][0]]
        check(f"shard {g['id']}'s replicas agree on the vinyl copy and its indexes",
              first is not None and first["engine"] == "vinyl" and set(first["indexes"]) >= {"kk", "uc"}
              and all(shapes[n] == first for n in g["replicas"])
              and all(ix["rows"] == first["rows"] for ix in first["indexes"].values()),
              {n: shapes[n] for n in g["replicas"]})

    print("index copy: all checks passed" if fails == 0 else f"index copy: {fails} FAILED", flush=True)
    return 0 if fails == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
