#!/usr/bin/env python3
"""ALTER TABLE builds new secondary indexes on replicated shards by copying
the table into a new space whose indexes exist before its rows (#267), also
when the same statement adds a column (#184).

Run by tests/indexcopy.sh on 2 shards x 3 replicas with fsync. For a memtx
and a vinyl table, two ALTERs run while three sessions write:

  * one adds 1 to k in rows it alone touches;
  * one owns other rows of the table and keeps a model of them: it hands a
    row's c to another row of its tenant (so a later page can meet the first
    row's stale copy), swaps two rows' c by way of NULL, inserts, deletes
    and moves rows to a new primary key, keeping (tenant_id, c) unique;
  * one inserts into a second table.

The first ALTER adds an index and a unique index on (tenant_id, c), the
second an integer column x and a case-insensitive string column s, with
non-NULL defaults, and an index on each. For each:

  * the change succeeds and replaces the table's space by way of the copy,
    and no replica's election term changes meanwhile;
  * the writes go on during the change, and none fails;
  * the table holds every increment of k, and the second session's rows as
    its model has them (after the first, also when read by the new unique
    index; after the second, every row with the fills, found by the new
    indexes, s's by its collation weight, and every stored row on every
    replica holding every field);
  * every replica holds the same rows and the same indexes (digests of each
    index's entries in order), and each index holds every row.

Then the new unique index refuses a duplicate; a duplicate in a table, and
a new column's fill that rows share under a new unique index, each fail
the change with 1062, from the copy, and leave that table as it was, with
no copy left on any replica; a column renamed, then dropped, each with a
new index, copies that table too.

A table whose unique key lacks the shard key has a global index, a space
of its own sharded by the key (#14). An ALTER that adds a column with a
fill and two indexes copies it too, and so does one that makes the key's
column nullable (which gives its index a new id), while a session hands
unique values from row to row, inserts, deletes and moves rows to new
primary keys, and tries duplicates on the other shard (#184). After each,
the global index's space stays, by id, named after the copy and the
index's id, on every replica, with one entry per row; it finds every row
and refuses another shard's duplicate. A TRUNCATE empties it, and DROP
TABLE drops it.

Last, an engine change, whose copy now has its indexes from the start too,
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

    def short_rows(node, space):
        """How many stored rows end before the space's last field: a copy
        that pads must leave none, whatever SQL would read for them."""
        return ev(node, """
            require('fiber').set_slice({warn = 30, err = 120})
            local sp = box.space[...]
            local width, n = #sp:format(), 0
            for _, t in sp:pairs() do if #t < width then n = n + 1 end end
            return n
        """, [space], timeout=60)

    def leftovers(node):
        return ev(node, """
            local out = {}
            for _, s in box.space._space:pairs() do
                if s.name:find('^_tarn_engine_') then out[#out + 1] = s.name end
            end
            return out
        """)

    def globals_of(node, space):
        """The global index spaces named after the table's space on node:
        {name: {id, rows, digest of its entries in order}}."""
        return ev(node, """
            require('fiber').set_slice({warn = 30, err = 120})
            local crc, mp = require('digest').crc32, require('msgpack')
            local sp, out = box.space[...], {}
            if sp == nil then return out end
            local tag = '_tarn_uix_' .. sp.id .. '_'
            for _, s in box.space._space.index.name:pairs({tag}, {iterator = 'GE'}) do
                if s.name:sub(1, #tag) ~= tag then break end
                local h, n = crc.new(), 0
                for _, e in box.space[s.id]:pairs() do h:update(mp.encode(e:totable())); n = n + 1 end
                out[s.name] = {id = s.id, rows = n, digest = h:result()}
            end
            return out
        """, [space], timeout=60) or {}   # (none comes back as an empty list)

    def uix_name(node, space, index):
        """The name of the global index space of index `index` of the table:
        after its space and the index's id in it."""
        return ev(node, """
            local sp, name = box.space[select(1, ...)], select(2, ...)
            return '_tarn_uix_' .. sp.id .. '_' .. sp.index[name].id
        """, [space, index])

    def named_after(node, sid):
        """Global index spaces still named after space id sid."""
        return ev(node, """
            local tag, out = '_tarn_uix_' .. ... .. '_', {}
            for _, s in box.space._space.index.name:pairs({tag}, {iterator = 'GE'}) do
                if s.name:sub(1, #tag) ~= tag then break end
                out[#out + 1] = s.name
            end
            return out
        """, [sid])

    def globally_unique(engine):
        """A unique key without the shard key has a global index: a space of
        its own, sharded by the key (#14). An ALTER that adds a column with a
        fill and indexes copies the table, and keeps that space, its id and
        its entries, renamed after the copy (#184), while a session moves
        unique values between rows and rows to new primary keys. So does one
        that makes the key's column nullable, which gives its index a new
        id, after which the space is named too."""
        g, gspace = f"g_{engine}", f"{DB}.g_{engine}"
        n = 20000
        q(c, f"CREATE TABLE {g} (tenant_id INT, id INT, u INT NOT NULL, k INT, PRIMARY KEY (tenant_id, id), "
             f"UNIQUE KEY uu (u)) ENGINE={engine} PARTITION BY KEY(tenant_id)")
        for first in range(0, n, 1000):
            q(c, f"INSERT INTO {g} VALUES " + ",".join(f"({i % TENANTS},{i},{i},{i % 97})"
                                                     for i in range(first, first + 1000)))
        model = {(i % TENANTS, i): i for i in range(n)}
        fresh = {"id": 2 * FRESH_ID, "u": 3 * FRESH_C}
        # A tenant whose rows another shard holds: a duplicate there meets
        # no local unique index, only the global one.
        home = {}
        for gi, g_ in enumerate(groups):
            for tn in ev(leader(g_), """
                local sp, out = box.space[...], {}
                for tn = 0, 63 do
                    if sp.index[0]:select({tn}, {limit = 1})[1] ~= nil then out[#out + 1] = tn end
                end
                return out
            """, [gspace]):
                home[tn] = gi

        def other(tn):
            return next(x for x in range(TENANTS) if x in home and home[x] != home[tn])
        errors, refused, writes = [], [0], [0]

        def new(what):
            fresh[what] += 1
            return fresh[what]

        def writer(stop):
            conn = connect(args.tarn, read_timeout=120)
            rnd = random.Random(f"global-{engine}")
            while not stop.is_set():
                keys = sorted(model)
                op = rnd.random()
                try:
                    if op < 0.3:
                        # a gives its unique value to b, which may live on
                        # another shard than a's entry
                        a, b = rnd.sample(keys, 2)
                        x = model[a]
                        q(conn, f"UPDATE {g} SET u = %s WHERE tenant_id = %s AND id = %s", (new("u"), *a))
                        model[a] = fresh["u"]
                        q(conn, f"UPDATE {g} SET u = %s WHERE tenant_id = %s AND id = %s", (x, *b))
                        model[b] = x
                    elif op < 0.5:
                        tn, i = rnd.randrange(TENANTS), new("id")
                        q(conn, f"INSERT INTO {g} (tenant_id, id, u, k) VALUES (%s, %s, %s, 0)", (tn, i, new("u")))
                        model[(tn, i)] = fresh["u"]
                    elif op < 0.65:
                        a = rnd.choice(keys)
                        q(conn, f"DELETE FROM {g} WHERE tenant_id = %s AND id = %s", a)
                        del model[a]
                    elif op < 0.8:
                        # the row moves to a new primary key, which its entry holds
                        a, j = rnd.choice(keys), new("id")
                        q(conn, f"UPDATE {g} SET id = %s WHERE tenant_id = %s AND id = %s", (j, *a))
                        model[(a[0], j)] = model.pop(a)
                    else:
                        # a row on another shard with a value a row holds
                        a = rnd.choice(keys)
                        try:
                            q(conn, f"INSERT INTO {g} (tenant_id, id, u, k) VALUES (%s, %s, %s, 0)",
                              (other(a[0]), new("id"), model[a]))
                            errors.append(f"duplicate of {a}'s {model[a]} went in")
                        except pymysql.MySQLError as e:
                            if e.args[0] != 1062:
                                raise
                            refused[0] += 1
                    writes[0] += 1
                except pymysql.MySQLError as e:
                    errors.append(f"global: {e.args}")
                    time.sleep(0.05)
                time.sleep(0.003)

        kept = {g_["id"]: {v["id"] for v in globals_of(leader(g_), gspace).values()} for g_ in groups}
        check(f"{engine}: a unique key without the shard key has one global index space per shard",
              all(len(v) == 1 for v in kept.values()) and len({frozenset(v) for v in kept.values()}) == 1, kept)
        # The second ALTER makes the key's column nullable, which gives its
        # index a new id, after which the copy renames the kept space too.
        for round_, sql in enumerate((f"ALTER TABLE {g} ADD COLUMN x INT NOT NULL DEFAULT 5, ADD INDEX kk (k), "
                                      f"ADD INDEX kx (x)",
                                      f"ALTER TABLE {g} MODIFY u INT NULL")):
            before = {g_["id"]: shape(leader(g_), gspace)["id"] for g_ in groups}
            before_terms, refused_before = terms(), refused[0]
            stop = threading.Event()
            th = threading.Thread(target=writer, args=(stop,), daemon=True)
            th.start()
            time.sleep(1)
            at = writes[0]
            alter_error, began = None, time.monotonic()
            try:
                q(c, sql)
            except pymysql.MySQLError as e:
                alter_error = e.args
            during = writes[0] - at
            print(f"  ({engine}: ALTER took {time.monotonic() - began:.2f} s; writes during it: {during})",
                  flush=True)
            time.sleep(1)
            stop.set()
            th.join(30)
            check(f"{engine}: {sql}", alter_error is None, alter_error)
            check(f"{engine}: writes went on through the global index during it, and none failed",
                  during > 0 and not errors and refused[0] > refused_before,
                  {"during": during, "errors": errors[:5], "duplicates refused": refused[0] - refused_before})
            check(f"{engine}: no replica changed its election term", terms() == before_terms,
                  {"before": before_terms, "after": terms()})
            caught_up()
            after = {g_["id"]: shape(leader(g_), gspace)["id"] for g_ in groups}
            records = {g_["id"]: engine_record(leader(g_), gspace, before[g_["id"]]) for g_ in groups}
            check(f"{engine}: that change was a copy, published on every shard",
                  all(after[s] != before[s] for s in after)
                  and all(r == {"phase": "published", "rebuild": True} for r in records.values()), records)
            rows = {(r[0], r[1]): (r[2], r[3]) for r in q(c, f"SELECT tenant_id, id, u, x FROM {g}")}
            check(f"{engine}: the table holds the model's rows, each with the fill",
                  {k: v[0] for k, v in rows.items()} == model and all(v[1] == 5 for v in rows.values()),
                  {"rows": len(rows), "model": len(model),
                   "differ": [k for k in model if rows.get(k, (None,))[0] != model[k]][:5]})
            sample = random.Random(f"g-{engine}-{round_}").sample(sorted(model), 300)
            found = [q(c, f"SELECT tenant_id, id FROM {g} WHERE u = %s", (model[k],)) for k in sample]
            check(f"{engine}: the global index finds rows by their unique value",
                  all(f == (k,) for f, k in zip(found, sample)),
                  [(k, f) for k, f in zip(sample, found) if f != (k,)][:5])
            dup = None
            try:
                a = sample[0]
                q(c, f"INSERT INTO {g} (tenant_id, id, u, k) VALUES (%s, %s, %s, 0)",
                  (other(a[0]), 4 * FRESH_ID + round_, model[a]))
            except pymysql.MySQLError as e:
                dup = e.args[0]
            check(f"{engine}: the global index refuses another shard's duplicate", dup == 1062, dup)
            spaces = {node: globals_of(node, gspace) for node in nodes}
            names = {node: uix_name(node, gspace, "uu") for node in nodes}
            stale = {node: named_after(node, before[g_["id"]]) for g_ in groups for node in g_["replicas"]}
            check(f"{engine}: each replica keeps the global index's space, by id, named after the copy and the "
                  f"index's id there",
                  all({v["id"] for v in spaces[node].values()} == kept[g_["id"]] for g_ in groups
                      for node in g_["replicas"])
                  and all(set(spaces[node]) == {names[node]} for node in nodes) and not any(stale.values()),
                  {"spaces": spaces, "names": names, "named after the old space": stale})
            check(f"{engine}: replicas agree on its entries, which are one per row",
                  all(spaces[node] == spaces[g_["replicas"][0]] for g_ in groups for node in g_["replicas"])
                  and sum(v["rows"] for g_ in groups for v in spaces[g_["replicas"][0]].values()) == len(model),
                  {"entries": {g_["id"]: spaces[g_["replicas"][0]] for g_ in groups}, "rows": len(model)})
            left = {node: leftovers(node) for node in nodes}
            check(f"{engine}: no copy is left on any replica", all(not v for v in left.values()), left)
        q(c, f"TRUNCATE TABLE {g}")
        q(c, f"INSERT INTO {g} (tenant_id, id, u, k) VALUES (1, 1, %s, 0)", (model[sample[1]],))
        caught_up()
        emptied = {node: sum(v["rows"] for v in globals_of(node, gspace).values()) for node in nodes}
        check(f"{engine}: TRUNCATE empties the kept space too, which then takes a value it held",
              sum(emptied[g_["replicas"][0]] for g_ in groups) == 1, emptied)
        q(c, f"DROP TABLE {g}")
        caught_up()
        uid = next(iter(kept[groups[0]["id"]]))
        gone = {node: ev(node, "return box.space[...] ~= nil", [uid]) for node in nodes}
        check(f"{engine}: DROP TABLE drops the kept space with the table", not any(gone.values()), gone)

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

        errors, bumps = [], [0]
        latencies = {"table": [], "model": [], "other": []}
        progress = {"table": 0, "model": 0, "other": 0}
        # (tenant_id, id) -> [k, c] of the rows the model's writer owns; it
        # writes through both ALTERs below.
        model = {(i % TENANTS, i): [i % 97, i] for i in range(args.rows) if owned_by_model(i)}
        ids = {tn: set() for tn in range(TENANTS)}
        for tn, i in model:
            ids[tn].add(i)
        fresh = {"id": FRESH_ID, "c": FRESH_C, "other": (1 + (engine == "vinyl")) * 10**9}

        def timed(kind, conn, sql, params):
            began = time.monotonic()
            n = q(conn, sql, params)
            latencies[kind].append(time.monotonic() - began)
            progress[kind] += 1
            return n

        def new(what):
            fresh[what] += 1
            return fresh[what]

        def bump_writer(stop):
            conn = connect(args.tarn, read_timeout=120)
            rnd = random.Random(f"table-{engine}-{len(latencies['table'])}")
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

        def model_writer(stop):
            conn = connect(args.tarn, read_timeout=120)
            rnd = random.Random(f"model-{engine}-{len(latencies['model'])}")

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
                        timed("model", conn, f"INSERT INTO {t} (tenant_id, id, k, c) VALUES (%s, %s, 0, %s)",
                              (tn, i, value))
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

        def other_writer(stop):
            conn = connect(args.tarn, read_timeout=120)
            while not stop.is_set():
                n = new("other")
                try:
                    timed("other", conn, "INSERT INTO w VALUES (%s, %s)", (n, n % 1000))
                except pymysql.MySQLError as e:
                    errors.append(f"other: {e.args}")
                    time.sleep(0.05)
                time.sleep(0.005)

        def online(sql, indexes):
            """Run ALTER `sql` while the three sessions write, and check that it
            copied the table on every shard, kept every term, let the writes go
            on, and left replicas that agree, each index (among `indexes`)
            holding every row."""
            before = {g["id"]: shape(leader(g), space) for g in groups}
            before_terms = terms()
            stop, failed = threading.Event(), len(errors)
            threads = [threading.Thread(target=fn, args=(stop,), daemon=True)
                       for fn in (bump_writer, model_writer, other_writer)]
            for th in threads:
                th.start()
            time.sleep(1)
            at = dict(progress)
            for v in latencies.values():
                v.clear()
            began = time.monotonic()
            alter_error = None
            try:
                q(c, sql)
            except pymysql.MySQLError as e:
                alter_error = e.args
            took = time.monotonic() - began
            during = {k: progress[k] - at[k] for k in progress}
            time.sleep(1)
            stop.set()
            for th in threads:
                th.join(30)
            check(f"{engine}: {sql}", alter_error is None, alter_error)
            print(f"  ({engine}: ALTER took {took:.2f} s; writes during it: {during}; longest write "
                  + ", ".join(f"{k} {max(v or [0]):.3f} s" for k, v in latencies.items()) + ")", flush=True)
            check(f"{engine}: no replica changed its election term", terms() == before_terms,
                  {"before": before_terms, "after": terms()})
            check(f"{engine}: writes went on during the change", all(n > 0 for n in during.values()), during)
            check(f"{engine}: no write failed", len(errors) == failed, errors[failed:failed + 5])
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
                      first is not None and set(first["indexes"]) >= set(indexes)
                      and all(ix["rows"] == first["rows"] for ix in first["indexes"].values()), first)

        def contents(columns):
            """Check the table against the writers: the model's rows exactly,
            and every increment of k in the others. Returns the rows."""
            table = {(r[0], r[1]): list(r[2:]) for r in q(c, f"SELECT tenant_id, id, {columns} FROM {t}")}
            mine = {key: v[:2] for key, v in table.items() if owned_by_model(key[1])}
            differ = [(k, mine.get(k), model[k]) for k in model if mine.get(k) != model[k]][:5]
            check(f"{engine}: the model's rows are as its writer left them",
                  mine == model, {"differ": differ, "extra": [k for k in mine if k not in model][:5]})
            base = sum(i % 97 for i in range(args.rows) if not owned_by_model(i))
            got = sum(v[0] for key, v in table.items() if not owned_by_model(key[1]))
            check(f"{engine}: the table holds every increment of k", got == base + bumps[0],
                  {"sum": got, "want": base + bumps[0], "increments": bumps[0]})
            others = sum(1 for i in range(args.rows) if not owned_by_model(i))
            check(f"{engine}: the table has every row", len(table) == others + len(model),
                  {"rows": len(table), "want": others + len(model)})
            return table

        online(f"ALTER TABLE {t} ADD INDEX kk (k), ADD UNIQUE INDEX uc (tenant_id, c)", ("kk", "uc"))
        contents("k, c")
        sample = random.Random(engine).sample(sorted(k for k, v in model.items() if v[1] is not None), 200)
        found = [q(c, f"SELECT id FROM {t} WHERE tenant_id = %s AND c = %s", (tn, model[(tn, i)][1]))
                 for tn, i in sample]
        check(f"{engine}: the new unique index finds the model's rows by their c",
              all(f == ((i,),) for f, (_, i) in zip(found, sample)),
              [(k, f) for k, f in zip(sample, found) if f != ((k[1],),)][:5])
        dup_key = next(k for k, v in model.items() if v[1] is not None)
        dup = None
        try:
            q(c, f"INSERT INTO {t} (tenant_id, id, k, c) VALUES (%s, %s, 0, %s)",
              (dup_key[0], 3 * FRESH_ID, model[dup_key][1]))
        except pymysql.MySQLError as e:
            dup = e.args[0]
        check(f"{engine}: the new unique index refuses a duplicate", dup == 1062, dup)

        # Columns and indexes on them: the copy gives every row the columns'
        # fills (and s's collation weight), those written during the copy
        # too (in the table's old layout).
        online(f"ALTER TABLE {t} ADD COLUMN x INT NOT NULL DEFAULT 7, "
               f"ADD COLUMN s VARCHAR(10) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'Ab', "
               f"ADD INDEX kx (x, tenant_id), ADD INDEX ks (s)", ("kk", "uc", "kx", "ks"))
        table = contents("k, c, x, s")
        check(f"{engine}: every row has the new columns' fills",
              all(v[2] == 7 and v[3] == "Ab" for v in table.values()),
              [(k, v) for k, v in table.items() if v[2] != 7 or v[3] != "Ab"][:5])
        by_index = q(c, f"SELECT COUNT(*) FROM {t} WHERE x = 7")[0][0]
        check(f"{engine}: the index on the new column finds every row", by_index == len(table),
              {"found": by_index, "rows": len(table)})
        by_weight = q(c, f"SELECT COUNT(*) FROM {t} WHERE s = 'ab'")[0][0]
        check(f"{engine}: the case-insensitive index finds every row by its fill's weight",
              by_weight == len(table), {"found": by_weight, "rows": len(table)})
        unpadded = {node: short_rows(node, space) for node in nodes}
        check(f"{engine}: every stored row on every replica holds every field", all(n == 0 for n in unpadded.values()),
              unpadded)

        # A duplicate the table holds fails the copy and leaves the table.
        d, dspace = f"d_{engine}", f"{DB}.d_{engine}"
        q(c, f"CREATE TABLE {d} (tenant_id INT, id INT, c INT, PRIMARY KEY (tenant_id, id)) "
             f"ENGINE={engine} PARTITION BY KEY(tenant_id)")
        q(c, f"INSERT INTO {d} VALUES " + ",".join(f"({i % 8},{i},{i})" for i in range(2000))
             + ",(3,5000,11)")
        for alter, what in ((f"ALTER TABLE {d} ADD UNIQUE INDEX uc (tenant_id, c)", "a duplicate in the table"),
                            (f"ALTER TABLE {d} ADD COLUMN z INT DEFAULT 1, ADD UNIQUE INDEX uz (tenant_id, z)",
                             "a new column's fill shared by rows")):
            d_before = {g["id"]: shape(leader(g), dspace) for g in groups}
            refused = None
            try:
                q(c, alter)
            except pymysql.MySQLError as e:
                refused = e.args[0]
            check(f"{engine}: {what} fails the change with 1062", refused == 1062, refused)
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

        # A column renamed, then dropped, each with a new index: copies too.
        for alter, what in ((f"ALTER TABLE {d} RENAME COLUMN c TO c2, ADD INDEX kc (c2)", "a renamed column"),
                            (f"ALTER TABLE {d} DROP COLUMN c2, ADD INDEX kt (tenant_id, id)", "a dropped column")):
            ids = {g["id"]: shape(leader(g), dspace)["id"] for g in groups}
            failed = None
            try:
                q(c, alter)
            except pymysql.MySQLError as e:
                failed = e.args
            check(f"{engine}: {what} with a new index", failed is None, failed)
            caught_up()
            records = {g["id"]: engine_record(leader(g), dspace, ids[g["id"]]) for g in groups}
            check(f"{engine}: that change was a copy, published on every shard",
                  all(r == {"phase": "published", "rebuild": True} for r in records.values()), records)
            shapes = {node: shape(node, dspace) for node in nodes}
            check(f"{engine}: the replicas agree on the copy, each index holding every row",
                  all(shapes[n] == shapes[g["replicas"][0]] for g in groups for n in g["replicas"])
                  and all(ix["rows"] == sh["rows"] for sh in shapes.values() for ix in sh["indexes"].values()),
                  shapes)
        columns = [r[0] for r in q(c, f"SHOW COLUMNS FROM {d}")]
        check(f"{engine}: the table has the columns left, and its rows",
              columns == ["tenant_id", "id"] and q(c, f"SELECT COUNT(*) FROM {d}")[0][0] == 2002,
              {"columns": columns})

        globally_unique(engine)

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
