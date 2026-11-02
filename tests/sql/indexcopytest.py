#!/usr/bin/env python3
"""ALTER TABLE builds new secondary indexes on replicated shards by copying
the table into a new space whose indexes exist before its rows (#267).

Run by tests/indexcopy.sh on 2 shards x 3 replicas with fsync. For a memtx
and a vinyl table, while three sessions write:

  * one adds 1 to k in rows it alone touches;
  * one owns other rows of the table and keeps a model of them: it hands a
    row's c to another row of its tenant (so a later page can meet the first
    row's stale copy), swaps two rows' c by way of NULL, inserts, deletes
    and moves rows to a new primary key, keeping (tenant_id, c) unique;
  * one inserts into a second table;

  * ADD INDEX with ADD UNIQUE INDEX on (tenant_id, c) succeeds and replaces
    the table's space by way of the copy, and no replica's election term
    changes meanwhile;
  * the writes go on during the change, and none fails;
  * the table holds every increment of k, and the second session's rows as
    its model has them, also when read by the new unique index;
  * every replica holds the same rows and the same indexes (digests of each
    index's entries in order), and each index holds every row;
  * the new unique index refuses a duplicate;
  * a duplicate in a table fails ADD UNIQUE INDEX with 1062, from the copy,
    and leaves that table as it was, with no copy left on any replica;
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
TENANTS = 64
FRESH_ID = 10**6   # ids the model's writer inserts or moves rows to are above this
FRESH_C = 10**7    # and the c values it gives rows


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


def owned_by_model(i):
    """Rows the model's writer owns: the original rows of every fourth round
    of tenants (some in each tenant), and the rows it inserts or moves. The
    increments go to the others."""
    return i > FRESH_ID or (i // TENANTS) % 4 == 3


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
        """The space's id, engine and row count, and each index's uniqueness,
        count and a digest of its entries in its order."""
        return ev(node, """
            -- (a vinyl scan of rows in memory does not yield)
            require('fiber').set_slice({warn = 30, err = 120})
            local crc, mp = require('digest').crc32, require('msgpack')
            local sp = box.space[...]
            if sp == nil then return box.NULL end
            local out = {id = sp.id, engine = sp.engine, rows = sp.index[0]:count(), indexes = {}}
            for id, ix in pairs(sp.index) do
                if type(id) == 'number' then
                    local h, n = crc.new(), 0
                    for _, tuple in ix:pairs() do h:update(mp.encode(tuple:totable())); n = n + 1 end
                    out.indexes[ix.name] = {unique = ix.unique, rows = n, digest = h:result()}
                end
            end
            return out
        """, [space], timeout=60)

    def engine_record(node, space, sid=None):
        """The engine copy's record for the space (or the space id sid): its
        phase, and whether it was a rebuild (new indexes, same engine)."""
        return ev(node, """
            local name, sid = ...
            if sid == nil then sid = box.space[name] and box.space[name].id end
            local r = sid and box.space._tarn_meta:get{'engine:' .. sid}
            return r and {phase = r.value.phase, rebuild = r.value.rebuild == true} or box.NULL
        """, [space, sid])

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
            q(c, f"INSERT INTO {t} VALUES " + ",".join(f"({i % TENANTS},{i},{i % 97},{i})" for i in rows))
        before = {g["id"]: shape(leader(g), space) for g in groups}
        before_terms = terms()

        stop, errors = threading.Event(), []
        latencies = {"table": [], "model": [], "other": []}
        progress = {"table": 0, "model": 0, "other": 0}
        bumps = [0]
        # (tenant_id, id) -> [k, c] of the rows the model's writer owns
        model = {(i % TENANTS, i): [i % 97, i] for i in range(args.rows) if owned_by_model(i)}

        def timed(kind, conn, sql, params):
            began = time.monotonic()
            n = q(conn, sql, params)
            latencies[kind].append(time.monotonic() - began)
            progress[kind] += 1
            return n

        def bump_writer():
            conn = connect(args.tarn, read_timeout=120)
            rnd = random.Random(f"table-{engine}")
            while not stop.is_set():
                i = rnd.randrange(args.rows)
                if owned_by_model(i):
                    i -= TENANTS
                try:
                    bumps[0] += timed("table", conn, f"UPDATE {t} SET k = k + 1 WHERE tenant_id = %s AND id = %s",
                                      (i % TENANTS, i))
                except pymysql.MySQLError as e:
                    errors.append(f"table: {e.args}")
                    time.sleep(0.05)
                time.sleep(0.005)

        def model_writer():
            conn = connect(args.tarn, read_timeout=120)
            rnd = random.Random(f"model-{engine}")
            fresh = {"id": FRESH_ID, "c": FRESH_C}
            ids = {tn: set() for tn in range(TENANTS)}
            for tn, i in model:
                ids[tn].add(i)

            def new(what):
                fresh[what] += 1
                return fresh[what]

            def set_c(tn, i, value):
                timed("model", conn, f"UPDATE {t} SET c = %s WHERE tenant_id = %s AND id = %s", (value, tn, i))
                model[(tn, i)][1] = value

            while not stop.is_set():
                tn = rnd.randrange(TENANTS)
                mine, op = sorted(ids[tn]), rnd.random()
                try:
                    if op < 0.4 and len(mine) >= 2:
                        # a gives its c to b: a page with b may meet a's stale copy
                        a, b = rnd.sample(mine, 2)
                        x = model[(tn, a)][1]
                        set_c(tn, a, new("c"))
                        set_c(tn, b, x)
                    elif op < 0.6 and len(mine) >= 2:
                        # a and b swap their c by way of NULL
                        a, b = rnd.sample(mine, 2)
                        x, y = model[(tn, a)][1], model[(tn, b)][1]
                        set_c(tn, a, None)
                        set_c(tn, b, x)
                        set_c(tn, a, y)
                    elif op < 0.75:
                        i, value = new("id"), new("c")
                        timed("model", conn, f"INSERT INTO {t} VALUES (%s, %s, 0, %s)", (tn, i, value))
                        model[(tn, i)] = [0, value]
                        ids[tn].add(i)
                    elif op < 0.9 and mine:
                        i = rnd.choice(mine)
                        timed("model", conn, f"DELETE FROM {t} WHERE tenant_id = %s AND id = %s", (tn, i))
                        del model[(tn, i)]
                        ids[tn].discard(i)
                    elif mine:
                        # the row moves to a new primary key
                        i, j = rnd.choice(mine), new("id")
                        timed("model", conn, f"UPDATE {t} SET id = %s WHERE tenant_id = %s AND id = %s", (j, tn, i))
                        model[(tn, j)] = model.pop((tn, i))
                        ids[tn].discard(i)
                        ids[tn].add(j)
                except pymysql.MySQLError as e:
                    errors.append(f"model: {e.args}")
                    time.sleep(0.05)
                time.sleep(0.005)

        def other_writer():
            conn = connect(args.tarn, read_timeout=120)
            n = 0
            while not stop.is_set():
                n += 1
                try:
                    timed("other", conn, "INSERT INTO w VALUES (%s, %s)", ((1 + (engine == "vinyl")) * 10**9 + n, n))
                except pymysql.MySQLError as e:
                    errors.append(f"other: {e.args}")
                    time.sleep(0.05)
                time.sleep(0.005)

        threads = [threading.Thread(target=fn, daemon=True) for fn in (bump_writer, model_writer, other_writer)]
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
        print(f"  ({engine}: ALTER took {took:.2f} s; writes during it: {during}; longest write "
              + ", ".join(f"{k} {max(v or [0]):.3f} s" for k, v in latencies.items()) + ")", flush=True)
        check(f"{engine}: no replica changed its election term", terms() == before_terms,
              {"before": before_terms, "after": terms()})
        check(f"{engine}: writes went on during the change", all(n > 0 for n in during.values()), during)
        check(f"{engine}: no write failed", not errors, errors[:5])

        caught_up()
        shapes = {node: shape(node, space) for node in nodes}
        for g in groups:
            first = shapes[g["replicas"][0]]
            check(f"{engine}: shard {g['id']}'s table was replaced by its copy",
                  first is not None and first["id"] != before[g["id"]]["id"] and first["engine"] == engine
                  and engine_record(leader(g), space, before[g["id"]]["id"]) == {"phase": "published",
                                                                                  "rebuild": True},
                  {"before": before[g["id"]], "after": first})
            check(f"{engine}: shard {g['id']}'s replicas hold the same rows and index entries",
                  all(shapes[n] == first for n in g["replicas"]), {n: shapes[n] for n in g["replicas"]})
            check(f"{engine}: each index of shard {g['id']} holds every row",
                  first is not None and set(first["indexes"]) >= {"kk", "uc"}
                  and all(ix["rows"] == first["rows"] for ix in first["indexes"].values())
                  and first["indexes"]["uc"]["unique"], first)

        table = {(r[0], r[1]): [r[2], r[3]] for r in q(c, f"SELECT tenant_id, id, k, c FROM {t}")}
        mine = {key: v for key, v in table.items() if owned_by_model(key[1])}
        missing = [k for k in model if mine.get(k) != model[k]][:5]
        extra = [k for k in mine if k not in model][:5]
        check(f"{engine}: the model's rows are as its writer left them",
              mine == model, {"differ": [(k, mine.get(k), model[k]) for k in missing], "extra": extra})
        base = sum(i % 97 for i in range(args.rows) if not owned_by_model(i))
        got = sum(v[0] for key, v in table.items() if not owned_by_model(key[1]))
        check(f"{engine}: the table holds every increment of k", got == base + bumps[0],
              {"sum": got, "want": base + bumps[0], "increments": bumps[0]})
        others = sum(1 for i in range(args.rows) if not owned_by_model(i))
        check(f"{engine}: the table has every row", len(table) == others + len(model),
              {"rows": len(table), "want": others + len(model)})
        sample = random.Random(engine).sample(sorted(k for k, v in model.items() if v[1] is not None), 200)
        found = [q(c, f"SELECT id FROM {t} WHERE tenant_id = %s AND c = %s", (tn, model[(tn, i)][1]))
                 for tn, i in sample]
        check(f"{engine}: the new unique index finds the model's rows by their c",
              all(f == ((i,),) for f, (_, i) in zip(found, sample)),
              [(k, f) for k, f in zip(sample, found) if f != ((k[1],),)][:5])
        dup_key = next(k for k, v in model.items() if v[1] is not None)
        dup = None
        try:
            q(c, f"INSERT INTO {t} VALUES (%s, %s, 0, %s)", (dup_key[0], 3 * FRESH_ID, model[dup_key][1]))
        except pymysql.MySQLError as e:
            dup = e.args[0]
        check(f"{engine}: the new unique index refuses a duplicate", dup == 1062, dup)

        # A duplicate the table holds fails the copy and leaves the table.
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
        records = {g["id"]: engine_record(leader(g), dspace) for g in groups}
        check(f"{engine}: that change was a copy, aborted on every shard",
              all(r == {"phase": "aborted", "rebuild": True} for r in records.values()), records)
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
