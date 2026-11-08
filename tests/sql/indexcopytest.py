#!/usr/bin/env python3
"""ALTER TABLE builds new secondary indexes on replicated shards by copying
the table into a new space whose indexes exist before its rows (#267), also
when the same statement adds a column (#184).

Run by tests/indexcopy.sh on 2 shards x 3 replicas with fsync, with the SQL
server sending a copy's BUILD and PREPARE in slices of 0.05 s, so that each
copy takes several calls (#184), which the first ALTER checks. For a memtx and a vinyl table, two ALTERs
run while three sessions write:

  * one adds 1 to k in rows it alone touches;
  * one owns other rows of the table and keeps a model of them: it hands a
    row's c to another row of its tenant (so a later page can meet the first
    row's stale copy), swaps two rows' c by way of NULL, inserts, deletes
    and moves rows to a new primary key, keeping (tenant_id, c) unique;
  * one inserts into a second table.

The first ALTER adds an index and a unique index on (tenant_id, c), the
second an integer column x and a case-insensitive string column s, with
non-NULL defaults, and an index on each. The first runs with small
catch-up bounds (#184), so its copy runs catch-up passes during the copy
and before its fence, which every shard's record counts, and settles them
in the copy's durable capture, which every replica can keep. For each:

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

A lazy index keeps its entries in a space of its own, its target, named
after the index's generation and sharded by the index's key (#82). An
ALTER that adds an index copies such a table too, here one with a global
index as well, and so does one that adds a column with a fill and an index
on it, while a session changes the key, inserts, deletes, moves rows to
new primary keys and changes another column (#184). After each, every
replica keeps the generation, ready, its descriptor and capture bound to
the copy and nothing bound to the old space; the replicas agree on the
target, whose live entries are the table's rows, one each; the lazy index
finds every row by its key; and the global index's space is kept as
above. A TRUNCATE then retires the generation and publishes a new one,
which finds the rows written since, and DROP TABLE retires that.

An ALTER that adds a lazy index copies the table too: the index's new
generation is built online against the table's space before the copy,
and published with the copy, keeping the table's other lazy indexes. Two
run on another table while a session changes keys, inserts, deletes and
moves rows to new primary keys: the table's first lazy index, then a
second one with an ordinary index. After each, every replica has each
generation ready and bound to the copy, and nothing bound to the old
space; the replicas agree on each target, whose live entries are the
table's rows, one each; and each lazy index finds rows by its key, on
the lazy path. DROP TABLE retires the generations.

An ALTER that narrows a column's type and makes it NOT NULL, and adds an
index, copies the table too, checking every row it writes: it succeeds
while a session writes, and one with a row out of the new range, or a NULL
for NOT NULL, fails with the in-place change's error (1264, 1265), leaving
the table as it was and no copy behind (#184).

An ALTER that drops a global unique index and a lazy index, and adds an
index, copies the table too, while a session changes both indexes' keys,
inserts, deletes and moves rows (#184). After it, every replica keeps the
table's other global and lazy indexes as above, has dropped the dropped
global index's space and retired the dropped generation, and binds nothing
else to the old space; the kept indexes find every row, and the dropped
unique key takes a duplicate. Before it, one whose new unique index meets a
duplicate fails from the copy, and leaves the table and its indexes as
they were.

An ALTER that renames a table with a global unique index and adds an index
copies it too, and publishes it under the new name (#292), while a session
moves values and rows and, once the old name is gone (1146), goes on under
the new one. It succeeds, no replica's term changes, the writes go on, none
failing but those the rename moved, and every replica holds the global
index's entries, one per row, in its space, named after the copy; the old
name is gone, and the key finds rows and refuses a duplicate under the new
one.

An ALTER that adds a unique key without the shard key to a populated table
builds its global index online (#292): the step that makes the index
enforcing copies the table too, rather than build the key's local index in
place, so a shard holding more rows than alter_check_rows takes it, while a
session hands values between rows, perhaps on the other shard, inserts,
deletes and moves rows to new primary keys. It succeeds, no replica's term
changes, the writes go on, none failing, and the table's space is replaced
on every shard; every replica holds the index's entries, one per row, in its
space named after the copy, and no conflicts space or copy; the key finds
rows by value and refuses a duplicate on the other shard. One that meets a
value two rows of different shards share fails with 1062, naming the value,
and leaves the table as it was, with no copy or index space left.

A shard whose BUILD ended first keeps noting the writes to its rows while
the other shard builds, here held after a page by a test hook, and these
writes outgrow its capture. The coordinator sends BUILD everywhere again
before PREPARE: that shard copies its table again then, and no shard fences
until it has (#184).

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

from lazytest import ev, stat

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


def vinyl_refusal(e):
    """Vinyl admission's documented refusal of a fresh write, 1205: throttled
    while dump or compaction catches up (docs/operations.md). Nothing was
    written, and the remedy is to retry."""
    return e.args[0] == 1205 and "Vinyl" in str(e.args[1])


def load(c, sql, args=None):
    """q() for the rows a scenario starts from, which no one else writes: a
    statement that vinyl admission refuses (it wrote nothing) is sent again,
    for up to two minutes, as a client would. So is one whose outcome is
    unknown (1180, as when a PREPARE could vote neither yes nor no on a busy
    host, its transaction left to recovery): the statement inserts fresh
    rows in one transaction, so if the next try meets a duplicate key
    (1062), the first one committed, all its rows. Until recovery has
    decided, the next tries may also wait for the rows' locks (1205) or
    conflict (1213)."""
    deadline = time.monotonic() + 120
    unknown = False
    while True:
        try:
            return q(c, sql, args)
        except pymysql.MySQLError as e:
            code = e.args[0]
            if unknown and code == 1062:
                return None
            if time.monotonic() > deadline:
                raise
            if code == 1180:
                unknown = True
            elif not (vinyl_refusal(e) or (unknown and code in (1205, 1213))):
                raise
            time.sleep(0.2)


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

    def passes_of(node, sid):
        """How many catch-up passes the copy of space id sid ran (#184)."""
        return ev(node, """
            local r = box.space._tarn_meta:get{'engine:' .. ...}
            return r and r.value.passes or 0
        """, [sid])

    def settled_of(node, sid):
        """The generation of the durable capture the copy of space id sid
        last settled: above 0 once a pass of a durable copy rereads its keys
        (#184)."""
        return ev(node, """
            local r = box.space._tarn_meta:get{'engine:' .. ...}
            return r and r.value.settled or 0
        """, [sid])

    def slices_of(group):
        """How many calls of a copy's BUILD or PREPARE returned for more on
        the shard's nodes since they started (#184): on all of them, whichever
        led meanwhile."""
        return sum(ev(node, "return tarn.engine.slices or 0") for node in group["replicas"])

    def catchup_bounds(small):
        """Catch-up passes at 16 captured keys, until 8 are left for the
        fence, on every replica; or the defaults."""
        for node in nodes:
            ev(node, "tarn.cfg.engine_catchup_keys, tarn.cfg.engine_fence_keys = 16, 8" if small else
                     "tarn.cfg.engine_catchup_keys, tarn.cfg.engine_fence_keys = nil, nil")

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
            load(c, f"INSERT INTO {g} VALUES " + ",".join(f"({i % TENANTS},{i},{i},{i % 97})"
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
                    # (vinyl admission's refusal wrote nothing: the next
                    # write comes after a pause)
                    if not vinyl_refusal(e):
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

    def space_id(node, space):
        return ev(node, "local sp = box.space[...]; return sp and sp.id or box.NULL", [space])

    def renamed(engine):
        """An ALTER that renames a table with a global unique index and adds
        an index copies it, and publishes it under the new name (#292), while
        a session writes, going on under the new name once the old one is
        gone."""
        r, to = f"rn_{engine}", f"rn2_{engine}"
        n = 20000
        q(c, f"CREATE TABLE {r} (tenant_id INT, id INT, u INT, k INT, PRIMARY KEY (tenant_id, id), "
             f"UNIQUE KEY ru (u)) ENGINE={engine} PARTITION BY KEY(tenant_id)")
        for first in range(0, n, 1000):
            load(c, f"INSERT INTO {r} VALUES " + ",".join(f"({i % TENANTS},{i},{i},{i % 97})"
                                                     for i in range(first, first + 1000)))
        model = {(i % TENANTS, i): i for i in range(n)}
        fresh = {"id": 8 * FRESH_ID, "u": 8 * FRESH_C}
        errors, writes, moved, name = [], [0], [0], [r]

        def new(what):
            fresh[what] += 1
            return fresh[what]

        def writer(stop):
            conn = connect(args.tarn, read_timeout=120)
            rnd = random.Random(f"renamed-{engine}")
            while not stop.is_set():
                keys = sorted(model)
                op = rnd.random()
                t = name[0]
                try:
                    if op < 0.4:
                        a, b = rnd.sample(keys, 2)
                        x = model[a]
                        q(conn, f"UPDATE {t} SET u = %s WHERE tenant_id = %s AND id = %s", (new("u"), *a))
                        model[a] = fresh["u"]
                        q(conn, f"UPDATE {t} SET u = %s WHERE tenant_id = %s AND id = %s", (x, *b))
                        model[b] = x
                    elif op < 0.6:
                        tn, i = rnd.randrange(TENANTS), new("id")
                        q(conn, f"INSERT INTO {t} VALUES (%s, %s, %s, 0)", (tn, i, new("u")))
                        model[(tn, i)] = fresh["u"]
                    elif op < 0.8:
                        a = rnd.choice(keys)
                        q(conn, f"DELETE FROM {t} WHERE tenant_id = %s AND id = %s", a)
                        del model[a]
                    else:
                        a, j = rnd.choice(keys), new("id")
                        q(conn, f"UPDATE {t} SET id = %s WHERE tenant_id = %s AND id = %s", (j, *a))
                        model[(a[0], j)] = model.pop(a)
                    writes[0] += 1
                except pymysql.MySQLError as e:
                    # (The table under its new name, once its old one is
                    # gone: the statement went nowhere, and goes on there.
                    # A statement that a hand-over split, its first half
                    # done, leaves the model as the table has it.)
                    if e.args[0] == 1146 and t == r:
                        name[0], moved[0] = to, moved[0] + 1
                    elif not vinyl_refusal(e):
                        errors.append(f"renamed: {e.args}")
                    time.sleep(0.05)
                time.sleep(0.003)

        caught_up()
        before = {g["id"]: shape(leader(g), f"{DB}.{r}")["id"] for g in groups}
        kept = {g["id"]: {v["id"] for v in globals_of(leader(g), f"{DB}.{r}").values()} for g in groups}
        before_terms = terms()
        stop = threading.Event()
        th = threading.Thread(target=writer, args=(stop,), daemon=True)
        th.start()
        time.sleep(1)
        at, began = writes[0], time.monotonic()
        try:
            q(c, f"ALTER TABLE {r} RENAME TO {to}, ADD INDEX rk (k)")
            alter_error = None
        except pymysql.MySQLError as e:
            alter_error = e.args
        during = writes[0] - at
        print(f"  ({engine}: ALTER took {time.monotonic() - began:.2f} s; writes during it: {during})", flush=True)
        time.sleep(1)
        stop.set()
        th.join(30)
        check(f"{engine}: ALTER TABLE ... RENAME TO ..., ADD INDEX", alter_error is None, alter_error)
        check(f"{engine}: writes went on during it, none failed, and the writer went on under the new name",
              during > 0 and not errors and moved[0] == 1 and name[0] == to,
              {"during": during, "errors": errors[:5], "moved": moved[0]})
        check(f"{engine}: no replica changed its election term", terms() == before_terms,
              {"before": before_terms, "after": terms()})
        caught_up()
        space = f"{DB}.{to}"
        records = {g["id"]: engine_record(leader(g), space, before[g["id"]]) for g in groups}
        after = {g["id"]: shape(leader(g), space)["id"] for g in groups}
        gone = {node: ev(node, "return box.space[...] == nil and box.space._tarn_catalog:get{...} == nil",
                         [f"{DB}.{r}"]) for node in nodes}
        check(f"{engine}: that change was a copy, published under the new name on every shard, and the old "
              f"name is gone from every replica",
              all(after[s] != before[s] for s in after) and all(gone.values())
              and all(r_ == {"phase": "published", "rebuild": True} for r_ in records.values()),
              {"records": records, "gone": gone})
        rows = {(x[0], x[1]): x[2] for x in q(c, f"SELECT tenant_id, id, u FROM {to}")}
        check(f"{engine}: the table holds the model's rows under the new name", rows == model,
              {"rows": len(rows), "model": len(model)})
        spaces = {node: globals_of(node, space) for node in nodes}
        names = {node: uix_name(node, space, "ru") for node in nodes}
        check(f"{engine}: every replica keeps the global index's space, by id, named after the copy, "
              f"one entry per row",
              all({v["id"] for v in spaces[node].values()} == kept[g["id"]] for g in groups for node in g["replicas"])
              and all(set(spaces[node]) == {names[node]} for node in nodes)
              and all(spaces[node] == spaces[g["replicas"][0]] for g in groups for node in g["replicas"])
              and sum(v["rows"] for g in groups for v in spaces[g["replicas"][0]].values()) == len(model),
              {"spaces": {g["id"]: spaces[g["replicas"][0]] for g in groups}, "names": names})
        left = {node: leftovers(node) for node in nodes}
        check(f"{engine}: no copy is left on any replica", all(not v for v in left.values()), left)
        sample = random.Random(f"rn-{engine}").sample(sorted(model), 200)
        found = [q(c, f"SELECT tenant_id, id FROM {to} WHERE u = %s", (model[k],)) for k in sample]
        old_name = None
        try:
            q(c, f"SELECT COUNT(*) FROM {r}")
        except pymysql.MySQLError as e:
            old_name = e.args[0]
        dup = None
        try:
            a = sample[0]
            q(c, f"INSERT INTO {to} VALUES (%s, %s, %s, 0)", ((a[0] + 1) % TENANTS, 9 * FRESH_ID, model[a]))
        except pymysql.MySQLError as e:
            dup = e.args[0]
        check(f"{engine}: under the new name the key finds rows and refuses a duplicate, and the old name is "
              f"gone (1146)",
              all(f == (k,) for f, k in zip(found, sample)) and dup == 1062 and old_name == 1146,
              {"dup": dup, "old": old_name})
        q(c, f"DROP TABLE {to}")

    def online_unique(engine):
        """An ALTER that adds a unique key without the shard key builds its
        global index online (#292), and copies the table for the step that
        makes it enforcing, past alter_check_rows rows per shard, while a
        session writes; one that meets a value two rows share fails with
        1062 and leaves the table as it was."""
        o, ospace = f"o_{engine}", f"{DB}.o_{engine}"
        n = 20000
        q(c, f"CREATE TABLE {o} (tenant_id INT, id INT, u INT, k INT, PRIMARY KEY (tenant_id, id)) "
             f"ENGINE={engine} PARTITION BY KEY(tenant_id)")
        for first in range(0, n, 1000):
            load(c, f"INSERT INTO {o} VALUES " + ",".join(f"({i % TENANTS},{i},{i},0)"
                                                     for i in range(first, first + 1000)))
        model = {(i % TENANTS, i): i for i in range(n)}
        fresh = {"id": 5 * FRESH_ID, "u": 5 * FRESH_C}
        errors, writes = [], [0]

        def new(what):
            fresh[what] += 1
            return fresh[what]

        def writer(stop):
            conn = connect(args.tarn, read_timeout=120)
            rnd = random.Random(f"online-{engine}")
            while not stop.is_set():
                keys = sorted(model)
                op = rnd.random()
                try:
                    if op < 0.4:
                        # a hands its value to b, perhaps on the other shard
                        a, b = rnd.sample(keys, 2)
                        x = model[a]
                        q(conn, f"UPDATE {o} SET u = %s WHERE tenant_id = %s AND id = %s", (new("u"), *a))
                        model[a] = fresh["u"]
                        q(conn, f"UPDATE {o} SET u = %s WHERE tenant_id = %s AND id = %s", (x, *b))
                        model[b] = x
                    elif op < 0.6:
                        tn, i = rnd.randrange(TENANTS), new("id")
                        q(conn, f"INSERT INTO {o} VALUES (%s, %s, %s, 0)", (tn, i, new("u")))
                        model[(tn, i)] = fresh["u"]
                    elif op < 0.8:
                        a = rnd.choice(keys)
                        q(conn, f"DELETE FROM {o} WHERE tenant_id = %s AND id = %s", a)
                        del model[a]
                    else:
                        # the row moves to a new primary key, with its value
                        a, j = rnd.choice(keys), new("id")
                        q(conn, f"UPDATE {o} SET id = %s WHERE tenant_id = %s AND id = %s", (j, *a))
                        model[(a[0], j)] = model.pop(a)
                    writes[0] += 1
                except pymysql.MySQLError as e:
                    if not vinyl_refusal(e):
                        errors.append(f"online unique: {e.args}")
                    time.sleep(0.05)
                time.sleep(0.003)

        limits = {node: ev(node, "local o = tarn.cfg.alter_check_rows; tarn.cfg.alter_check_rows = ...; return o",
                           [1000]) for node in nodes}
        try:
            caught_up()
            before = {g["id"]: shape(leader(g), ospace)["id"] for g in groups}
            before_terms = terms()
            stop = threading.Event()
            th = threading.Thread(target=writer, args=(stop,), daemon=True)
            th.start()
            time.sleep(1)
            at, began = writes[0], time.monotonic()
            try:
                # (with an index on another column, which the copy builds)
                q(c, f"ALTER TABLE {o} ADD UNIQUE KEY ou (u), ADD INDEX ok (k)")
                alter_error = None
            except pymysql.MySQLError as e:
                alter_error = e.args
            during = writes[0] - at
            print(f"  ({engine}: ALTER took {time.monotonic() - began:.2f} s; writes during it: {during})",
                  flush=True)
            time.sleep(1)
            stop.set()
            th.join(30)
            check(f"{engine}: ADD UNIQUE KEY without the shard key, and an index, with alter_check_rows 1000 below "
                  f"the rows per shard", alter_error is None, alter_error)
            check(f"{engine}: writes went on during it, and none failed", during > 0 and not errors,
                  {"during": during, "errors": errors[:5]})
            check(f"{engine}: no replica changed its election term", terms() == before_terms,
                  {"before": before_terms, "after": terms()})
            caught_up()
            records = {g["id"]: engine_record(leader(g), ospace, before[g["id"]]) for g in groups}
            after = {g["id"]: shape(leader(g), ospace)["id"] for g in groups}
            check(f"{engine}: its enforcing step was a copy, published on every shard",
                  all(after[s] != before[s] for s in after)
                  and all(r == {"phase": "published", "rebuild": True} for r in records.values()), records)
            rows = {(r[0], r[1]): r[2] for r in q(c, f"SELECT tenant_id, id, u FROM {o}")}
            check(f"{engine}: the table holds the model's rows", rows == model,
                  {"rows": len(rows), "model": len(model)})
            spaces = {node: globals_of(node, ospace) for node in nodes}
            names = {node: uix_name(node, ospace, "ou") for node in nodes}
            check(f"{engine}: every replica holds the index's entries in its space, named after the copy, "
                  f"and no conflicts space",
                  all(set(spaces[node]) == {names[node]} for node in nodes)
                  and all(spaces[node] == spaces[g["replicas"][0]] for g in groups for node in g["replicas"])
                  and sum(v["rows"] for g in groups for v in spaces[g["replicas"][0]].values()) == len(model),
                  {"spaces": {g["id"]: spaces[g["replicas"][0]] for g in groups}, "names": names})
            left = {node: leftovers(node) for node in nodes}
            check(f"{engine}: no copy is left on any replica", all(not v for v in left.values()), left)
            counts = {node: ev(node, "local sp = box.space[...] "
                                     "return {sp.index[0]:count(), sp.index.ok and sp.index.ok:count() or -1}",
                               [ospace]) for node in nodes}
            check(f"{engine}: every replica's copy has the statement's other index, with an entry per row",
                  all(v[1] == v[0] for v in counts.values()), counts)
            sample = random.Random(f"o-{engine}").sample(sorted(model), 300)
            found = [q(c, f"SELECT tenant_id, id FROM {o} WHERE u = %s", (model[k],)) for k in sample]
            check(f"{engine}: the key finds rows by their value", all(f == (k,) for f, k in zip(found, sample)),
                  [(k, f) for k, f in zip(sample, found) if f != (k,)][:5])
            dup = None
            try:
                a = sample[0]
                q(c, f"INSERT INTO {o} VALUES (%s, %s, %s, 0)", ((a[0] + 1) % TENANTS, 6 * FRESH_ID, model[a]))
            except pymysql.MySQLError as e:
                dup = e.args[0]
            check(f"{engine}: the key refuses a duplicate in another tenant", dup == 1062, dup)

            # A value two rows of different shards share: the change fails.
            q(c, f"ALTER TABLE {o} DROP INDEX ou")
            home = {}
            for gi, g in enumerate(groups):
                for tn in ev(leader(g), """
                    local sp, out = box.space[...], {}
                    for tn = 0, 63 do
                        if sp.index[0]:select({tn}, {limit = 1})[1] ~= nil then out[#out + 1] = tn end
                    end
                    return out
                """, [ospace]):
                    home[tn] = gi
            a = next(k for k in sorted(model) if k[0] in home)
            tn = next(x for x in range(TENANTS) if x in home and home[x] != home[a[0]])
            q(c, f"INSERT INTO {o} VALUES (%s, %s, %s, 0)", (tn, 7 * FRESH_ID, model[a]))
            caught_up()
            before = {g["id"]: shape(leader(g), ospace)["id"] for g in groups}
            try:
                q(c, f"ALTER TABLE {o} ADD UNIQUE KEY ou (u)")
                failed = None
            except pymysql.MySQLError as e:
                failed = e.args
            caught_up()
            after = {g["id"]: shape(leader(g), ospace)["id"] for g in groups}
            left = {node: leftovers(node) for node in nodes}
            spaces = {node: globals_of(node, ospace) for node in nodes}
            check(f"{engine}: one that meets a value two rows of different shards share fails with 1062, naming "
                  f"it, and leaves the table as it was, with no copy or index space left",
                  failed == (1062, f"Duplicate entry '{model[a]}' for key 'ou'") and after == before
                  and all(not v for v in left.values()) and not any(spaces.values()),
                  {"failed": failed, "before": before, "after": after, "left": left, "spaces": spaces})
        finally:
            for node, v in limits.items():
                ev(node, "tarn.cfg.alter_check_rows = ...", [v])
        q(c, f"DROP TABLE {o}")

    def checked_copy(engine):
        """An ALTER that narrows a column's type and makes it NOT NULL, and
        adds an index, copies the table too: the copy checks every row it
        writes (#184). One whose rows all pass succeeds while a session
        writes. One with a row out of the new range, or a NULL for NOT NULL,
        fails with the in-place change's error and leaves the table as it
        was, with no copy left on any replica."""
        t = f"ck_{engine}"
        space = f"{DB}.{t}"
        q(c, f"CREATE TABLE {t} (tenant_id INT, id INT, k INT, a INT, w INT, z INT, PRIMARY KEY (tenant_id, id)) "
             f"ENGINE={engine} PARTITION BY KEY(tenant_id)")
        for first in range(0, 10000, 1000):
            load(c, f"INSERT INTO {t} VALUES " + ",".join(f"({i % TENANTS},{i},0,{i % 100},{i},{i})"
                                                       for i in range(first, first + 1000)))
        q(c, f"UPDATE {t} SET w = 100000 WHERE tenant_id = 5 AND id = 5")
        q(c, f"UPDATE {t} SET z = NULL WHERE tenant_id = 7 AND id = 7")
        caught_up()
        stop, failures, bumps_ = threading.Event(), [], [0]

        def writer():
            conn = connect(args.tarn)
            rnd = random.Random(f"checked-{engine}")
            while not stop.is_set():
                i = rnd.randrange(10000)
                try:
                    bumps_[0] += q(conn, f"UPDATE {t} SET k = k + 1 WHERE tenant_id = %s AND id = %s",
                                   (i % TENANTS, i))
                except pymysql.MySQLError as e:
                    failures.append(e.args)
                time.sleep(0.005)

        th = threading.Thread(target=writer, daemon=True)
        th.start()
        time.sleep(0.5)
        ids = {g["id"]: space_id(leader(g), space) for g in groups}
        error = None
        try:
            q(c, f"ALTER TABLE {t} MODIFY a TINYINT NOT NULL, ADD INDEX ka (a)")
        except pymysql.MySQLError as e:
            error = e.args
        time.sleep(0.5)
        stop.set()
        th.join(30)
        check(f"{engine}: an ALTER that narrows a column, makes it NOT NULL and adds an index succeeds "
              "while a session writes", error is None and not failures and bumps_[0] > 0,
              {"error": error, "failures": failures[:3], "writes": bumps_[0]})
        caught_up()
        records = {g["id"]: engine_record(leader(g), space, ids[g["id"]]) for g in groups}
        check(f"{engine}: that change was a copy, published on every shard",
              all(r == {"phase": "published", "rebuild": True} for r in records.values()), records)
        shapes = {node: shape(node, space) for node in nodes}
        check(f"{engine}: the replicas agree on that copy, its new index holding every row",
              all(shapes[n] == shapes[g["replicas"][0]] for g in groups for n in g["replicas"])
              and all(sh is not None and "ka" in sh["indexes"] and sh["indexes"]["ka"]["rows"] == sh["rows"]
                      for sh in shapes.values()), shapes)
        total = q(c, f"SELECT SUM(k), COUNT(*) FROM {t}")[0]
        check(f"{engine}: the table took every write", int(total[0]) == bumps_[0] and total[1] == 10000,
              {"sum": total[0], "rows": total[1], "writes": bumps_[0]})
        for alter, code, what in ((f"ALTER TABLE {t} MODIFY w SMALLINT, ADD INDEX kw (w)", 1264,
                                   "a row out of the new range"),
                                  (f"ALTER TABLE {t} MODIFY z INT NOT NULL, ADD INDEX kz (z)", 1265,
                                   "a NULL for NOT NULL")):
            before = {g["id"]: shape(leader(g), space) for g in groups}
            ids = {g["id"]: space_id(leader(g), space) for g in groups}
            refused = None
            try:
                q(c, alter)
            except pymysql.MySQLError as e:
                refused = e.args[0]
            check(f"{engine}: {what} fails the change with {code}, from the copy", refused == code, refused)
            caught_up()
            records = {g["id"]: engine_record(leader(g), space, ids[g["id"]]) for g in groups}
            check(f"{engine}: that change was a copy, aborted on every shard",
                  all(r == {"phase": "aborted", "rebuild": True} for r in records.values()), records)
            after = {g["id"]: shape(leader(g), space) for g in groups}
            check(f"{engine}: that table is as it was", after == before, {"before": before, "after": after})
            left = {node: leftovers(node) for node in nodes}
            check(f"{engine}: no copy is left on any replica", all(not v for v in left.values()), left)
        q(c, f"DROP TABLE {t}")

    def catch_up_round(engine):
        """The first shard's BUILD ends while the second's is held after a
        page (test hooks), and writes to the first shard's rows outgrow its
        capture meanwhile. The coordinator sends BUILD everywhere again before
        PREPARE (#184): the first shard copies its table again there, and no
        shard fences until it has. (Each leader logs the phases its copy's
        record takes, with the host's monotonic clock, from a trigger.)"""
        t = f"cr_{engine}"
        space = f"{DB}.{t}"
        q(c, f"CREATE TABLE {t} (tenant_id INT, id INT, k INT, PRIMARY KEY (tenant_id, id)) "
             f"ENGINE={engine} PARTITION BY KEY(tenant_id)")
        for first in range(0, 20000, 1000):
            load(c, f"INSERT INTO {t} VALUES " + ",".join(f"({i % TENANTS},{i},0)" for i in range(first, first + 1000)))
        caught_up()
        first_g, held_g = groups
        lead1, lead2 = leader(first_g), leader(held_g)
        sid = space_id(lead1, space)
        # The tenants whose rows the first shard holds.
        tenants = ev(lead1, """
            local seen, out = {}, {}
            for _, row in box.space[...]:pairs() do
                if not seen[row[1]] then seen[row[1]] = true; out[#out + 1] = row[1] end
            end
            return out
        """, [space], timeout=60)
        for lead in (lead1, lead2):
            ev(lead, """
                local key, clock = 'engine:' .. ..., require('clock')
                rawset(_G, 'phases_log', {})
                rawset(_G, 'phases_trigger', function(_, new)
                    if new ~= nil and new[1] == key then table.insert(phases_log, {clock.monotonic(), new[2].phase}) end
                end)
                box.space._tarn_meta:on_replace(phases_trigger)
            """, [sid])
        for node in first_g["replicas"]:
            ev(node, "tarn.cfg.engine_dirty_keys = 64")
        ev(lead2, "return tarn_test_hook('engine_copy', 'hold')")
        result = {}

        def alter():
            conn = connect(args.tarn)
            try:
                q(conn, f"ALTER TABLE {t} ADD INDEX kk (k)")
                result["error"] = None
            except pymysql.MySQLError as e:
                result["error"] = e.args

        th = threading.Thread(target=alter, daemon=True)
        th.start()
        deadline = time.monotonic() + 30
        while not (ev(lead2, "return tarn_test_hook('engine_copy', 'reached')")[1] > 0
                   and engine_record(lead1, space)["phase"] == "copied") and time.monotonic() < deadline:
            time.sleep(0.05)
        built = engine_record(lead1, space)
        # (More keys than the first shard's capture holds: about 600.)
        written = q(c, f"UPDATE {t} SET k = k + 1 WHERE tenant_id IN ({','.join(map(str, tenants[:2]))})")
        ev(lead2, "return tarn_test_hook('engine_copy', 'release')")
        th.join(180)
        for node in first_g["replicas"]:
            ev(node, "tarn.cfg.engine_dirty_keys = nil")
        logs = [ev(lead, """
            box.space._tarn_meta:on_replace(nil, phases_trigger)
            local out = phases_log
            rawset(_G, 'phases_log', nil); rawset(_G, 'phases_trigger', nil)
            return out
        """) for lead in (lead1, lead2)]
        check(f"{engine}: an ALTER whose first shard's capture outgrew it while the other built succeeds",
              not th.is_alive() and result.get("error") is None and built["phase"] == "copied" and written > 64,
              {"result": result, "built": built, "written": written})

        def at(log, phase, after=None):
            return next((t_ for t_, p in log if p == phase and (after is None or t_ > after)), None)

        copied = at(logs[0], "copied")
        again = at(logs[0], "resetting", copied) if copied is not None else None
        recopied = at(logs[0], "copied", again) if again is not None else None
        fenced = min((t_ for log in logs for t_, p in log if p == "preparing"), default=None)
        check(f"{engine}: that shard copies its table again before any shard fences",
              None not in (copied, again, recopied, fenced) and recopied < fenced,
              {"phases": [[p for i, (_, p) in enumerate(log) if i == 0 or log[i - 1][1] != p] for log in logs],
               "copied": copied, "again": again,
               "recopied": recopied, "fenced": fenced})
        caught_up()
        shapes = {node: shape(node, space) for node in nodes}
        check(f"{engine}: the replicas agree on that copy, its new index holding every row",
              all(shapes[n] == shapes[g["replicas"][0]] for g in groups for n in g["replicas"])
              and all(sh is not None and "kk" in sh["indexes"] and sh["indexes"]["kk"]["rows"] == sh["rows"]
                      for sh in shapes.values()), shapes)
        q(c, f"DROP TABLE {t}")

    def lazy_gens(node, space):
        """The table's published lazy generations on node, by index name."""
        return ev(node, """
            local sp, out = box.space[...], {}
            local cap = sp and box.space._tarn_lazy_capture:get{sp.id}
            for _, s in ipairs(cap and cap.specs or {}) do
                for name, ix in pairs(sp.index) do
                    if type(name) == 'string' and ix.id == s.local_index_id then out[name] = s.gen end
                end
            end
            return out
        """, [space]) or {}

    def gen_phases(node, gens):
        """Each generation's phase and the space its descriptor names on node."""
        return ev(node, """
            local out = {}
            for i, gen in ipairs({...}) do
                local d = box.space._tarn_lazy_defs:get{gen}
                out[i] = d and {d.phase, d.base_id} or 'none'
            end
            return out
        """, gens)

    def dropped_indexes(engine):
        """An ALTER that drops a global unique index and a lazy index, and
        adds an index, copies the table too (#184), while a session changes
        both indexes' keys, inserts, deletes and moves rows: until PREPARE
        the dropped indexes take the table's writes, and publication retires
        them, dropping the global index's space on every replica. The table's
        other global and lazy indexes are kept. Before it, one whose new
        unique index meets a duplicate fails from the copy and leaves the
        table and its indexes as they were."""
        t, space = f"dx_{engine}", f"{DB}.dx_{engine}"
        n, keys = 10000, 40
        q(c, f"CREATE TABLE {t} (tenant_id INT NOT NULL, id INT NOT NULL, k INT, j INT, u INT NOT NULL, "
             f"w INT NOT NULL, b INT, PRIMARY KEY (tenant_id, id), KEY by_k (k, id) COMMENT 'tarn:lazy', "
             f"KEY by_j (j, id) COMMENT 'tarn:lazy', UNIQUE KEY uu (u), UNIQUE KEY uw (w)) "
             f"ENGINE={engine} PARTITION BY KEY(tenant_id)")
        for first in range(0, n, 1000):
            load(c, f"INSERT INTO {t} VALUES " + ",".join(f"({i % TENANTS},{i},{i % keys},{i % keys},{i},{i},{i % 2})"
                                                     for i in range(first, first + 1000)))
        # (tenant_id, id) -> [k, j, u, w]
        model = {(i % TENANTS, i): [i % keys, i % keys, i, i] for i in range(n)}
        fresh = {"id": 6 * FRESH_ID, "u": 6 * FRESH_C}
        errors, writes, throttled = [], [0], [0]
        shard_of = {node: g_["id"] for g_ in groups for node in g_["replicas"]}

        def new(what):
            fresh[what] += 1
            return fresh[what]

        def writer(stop):
            conn = connect(args.tarn, read_timeout=120)
            rnd = random.Random(f"dropped-{engine}")
            while not stop.is_set():
                rows, op = sorted(model), rnd.random()
                try:
                    if op < 0.35:
                        a, k, j = rnd.choice(rows), rnd.randrange(keys), rnd.randrange(keys)
                        q(conn, f"UPDATE {t} SET k = %s, j = %s WHERE tenant_id = %s AND id = %s", (k, j, *a))
                        model[a][0:2] = [k, j]
                    elif op < 0.5:
                        a, u, w = (rnd.randrange(TENANTS), new("id")), new("u"), new("u")
                        k = rnd.randrange(keys)
                        q(conn, f"INSERT INTO {t} (tenant_id, id, k, j, u, w, b) VALUES (%s, %s, %s, %s, %s, %s, 0)",
                          (*a, k, k, u, w))
                        model[a] = [k, k, u, w]
                    elif op < 0.65:
                        a = rnd.choice(rows)
                        q(conn, f"DELETE FROM {t} WHERE tenant_id = %s AND id = %s", a)
                        del model[a]
                    elif op < 0.8:
                        # the row moves to a new primary key, which its entries hold
                        a, i = rnd.choice(rows), new("id")
                        q(conn, f"UPDATE {t} SET id = %s WHERE tenant_id = %s AND id = %s", (i, *a))
                        model[(a[0], i)] = model.pop(a)
                    else:
                        # new unique values for both keys
                        a, u, w = rnd.choice(rows), new("u"), new("u")
                        q(conn, f"UPDATE {t} SET u = %s, w = %s WHERE tenant_id = %s AND id = %s", (u, w, *a))
                        model[a][2:4] = [u, w]
                    writes[0] += 1
                except pymysql.MySQLError as e:
                    # (vinyl admission's refusal wrote nothing: the model
                    # stays as it was, and the next write comes after a pause)
                    if vinyl_refusal(e):
                        throttled[0] += 1
                    else:
                        errors.append(f"dropped: {e.args}")
                    time.sleep(0.05)
                time.sleep(0.003)

        drained()
        gens = lazy_gens(leader(groups[0]), space)
        globals_before = {node: globals_of(node, space) for node in nodes}
        uw_ids = {g_["id"]: globals_of(leader(g_), space)[uix_name(leader(g_), space, "uw")]["id"] for g_ in groups}
        uu_ids = {g_["id"]: globals_of(leader(g_), space)[uix_name(leader(g_), space, "uu")]["id"] for g_ in groups}
        check(f"{engine}: the table has two lazy indexes and two global index spaces on every replica",
              set(gens) == {"by_k", "by_j"} and all(len(v) == 2 for v in globals_before.values()),
              {"gens": gens, "globals": globals_before})

        # A new unique index that meets a duplicate fails the change, from
        # the copy, and leaves the table and its indexes as they were.
        ids = {g_["id"]: space_id(leader(g_), space) for g_ in groups}
        shapes = {g_["id"]: shape(leader(g_), space) for g_ in groups}
        refused = None
        try:
            q(c, f"ALTER TABLE {t} DROP INDEX uw, DROP INDEX by_j, ADD UNIQUE KEY ub (tenant_id, b)")
        except pymysql.MySQLError as e:
            refused = e.args[0]
        check(f"{engine}: a duplicate under the new unique index fails the change with 1062", refused == 1062,
              refused)
        caught_up()
        records = {g_["id"]: engine_record(leader(g_), space, ids[g_["id"]]) for g_ in groups}
        check(f"{engine}: that change was a copy, aborted on every shard",
              all(r == {"phase": "aborted", "rebuild": True} for r in records.values()), records)
        phases = {node: gen_phases(node, [gens["by_k"], gens["by_j"]]) for node in nodes}
        check(f"{engine}: the table, its global index spaces and its lazy generations are as they were",
              {g_["id"]: shape(leader(g_), space) for g_ in groups} == shapes
              and {node: globals_of(node, space) for node in nodes} == globals_before
              and all(v == [["ready", ids[shard_of[node]]], ["ready", ids[shard_of[node]]]]
                      for node, v in phases.items()), phases)
        left = {node: leftovers(node) for node in nodes}
        check(f"{engine}: no copy is left on any replica", all(not v for v in left.values()), left)

        sql = f"ALTER TABLE {t} DROP INDEX uw, DROP INDEX by_j, ADD INDEX kb (b)"
        before_terms = terms()
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
        print(f"  ({engine}: ALTER took {time.monotonic() - began:.2f} s; writes during it: {during})", flush=True)
        time.sleep(1)
        stop.set()
        th.join(30)
        check(f"{engine}: {sql}", alter_error is None, alter_error)
        check(f"{engine}: writes went on through every index during it, and none failed (vinyl admission may "
              f"refuse one, which writes nothing)", during > 0 and not errors,
              {"during": during, "errors": errors[:5], "vinyl refusals": throttled[0]})
        if throttled[0]:
            print(f"  ({engine}: vinyl admission refused {throttled[0]} writes, which wrote nothing)", flush=True)
        check(f"{engine}: no replica changed its election term", terms() == before_terms,
              {"before": before_terms, "after": terms()})
        drained()
        after = {g_["id"]: space_id(leader(g_), space) for g_ in groups}
        records = {g_["id"]: engine_record(leader(g_), space, ids[g_["id"]]) for g_ in groups}
        check(f"{engine}: that change was a copy, published on every shard",
              all(after[s_] != ids[s_] for s_ in after)
              and all(r == {"phase": "published", "rebuild": True} for r in records.values()), records)
        spaces = {node: globals_of(node, space) for node in nodes}
        names = {node: uix_name(node, space, "uu") for node in nodes}
        stale = {node: named_after(node, ids[shard_of[node]]) for node in nodes}
        gone = {node: ev(node, "return box.space[...] ~= nil", [uw_ids[shard_of[node]]]) for node in nodes}
        check(f"{engine}: every replica keeps uu's space, by id, named after the copy, and has dropped uw's",
              all(set(spaces[node]) == {names[node]} and spaces[node][names[node]]["id"] == uu_ids[shard_of[node]]
                  for node in nodes) and not any(stale.values()) and not any(gone.values())
              and all(spaces[node] == spaces[g_["replicas"][0]] for g_ in groups for node in g_["replicas"])
              and sum(v["rows"] for g_ in groups for v in spaces[g_["replicas"][0]].values()) == len(model),
              {"spaces": spaces, "named after the old space": stale, "uw's space": gone})
        phases = {node: gen_phases(node, [gens["by_k"], gens["by_j"]]) for node in nodes}
        states = {node: lazy_state(node, after[shard_of[node]]) for node in nodes}
        olds = {node: lazy_state(node, ids[shard_of[node]]) for node in nodes}
        check(f"{engine}: every replica keeps by_k's generation bound to the copy, and has retired by_j's",
              all(v == [["ready", after[shard_of[node]]], ["retired", ids[shard_of[node]]]]
                  and states[node]["capture"] == [gens["by_k"]] and olds[node]["capture"] == []
                  and set(olds[node]["defs"]) == {str(gens["by_j"])}
                  for node, v in phases.items()), {"phases": phases, "capture": states, "old space": olds})
        live = [tuple(e) for g_ in groups for e in live_entries(g_["replicas"][0], gens["by_k"])]
        want = {(v[0], *key) for key, v in model.items()}
        check(f"{engine}: by_k's live entries are the table's rows, one each",
              len(live) == len(want) and set(live) == want,
              {"entries": len(live), "rows": len(want), "missing": sorted(want - set(live))[:5],
               "extra": sorted(set(live) - want)[:5]})
        rows = {(r[0], r[1]): list(r[2:]) for r in q(c, f"SELECT tenant_id, id, k, j, u, w FROM {t}")}
        check(f"{engine}: the table holds the model's rows", rows == model,
              {"rows": len(rows), "model": len(model), "differ": [key for key in model if rows.get(key) != model[key]][:5]})
        sample = random.Random(f"dx-{engine}").sample(sorted(model), 50)
        by_u = [q(c, f"SELECT tenant_id, id FROM {t} WHERE u = %s", (model[key][2],)) for key in sample]
        by_k = [q(c, f"SELECT tenant_id, id FROM {t} WHERE k = %s AND id = %s", (model[key][0], key[1]))
                for key in sample]
        by_w = [q(c, f"SELECT tenant_id, id FROM {t} WHERE w = %s", (model[key][3],)) for key in sample[:10]]
        check(f"{engine}: the kept indexes find rows by their keys, and the dropped keys still find them",
              all(f == (key,) for f, key in zip(by_u, sample)) and all(f == (key,) for f, key in zip(by_k, sample))
              and all(f == (key,) for f, key in zip(by_w, sample[:10])),
              {"u": [(key, f) for key, f in zip(sample, by_u) if f != (key,)][:3],
               "k": [(key, f) for key, f in zip(sample, by_k) if f != (key,)][:3],
               "w": [(key, f) for key, f in zip(sample[:10], by_w) if f != (key,)][:3]})
        a = sample[0]
        q(c, f"INSERT INTO {t} (tenant_id, id, k, j, u, w, b) VALUES (%s, %s, 0, 0, %s, %s, 0)",
          ((a[0] + 1) % TENANTS, 9 * FRESH_ID, new("u"), model[a][3]))
        twice = q(c, f"SELECT COUNT(*) FROM {t} WHERE w = %s", (model[a][3],))[0][0]
        check(f"{engine}: the dropped unique key takes a duplicate", twice == 2, twice)
        created = q(c, f"SHOW CREATE TABLE {t}")[0][1]
        check(f"{engine}: the table's definition has kb and neither dropped index",
              "KEY `kb`" in created and "`uw`" not in created and "`by_j`" not in created, created)
        left = {node: leftovers(node) for node in nodes}
        check(f"{engine}: no copy is left on any replica", all(not v for v in left.values()), left)
        q(c, f"DROP TABLE {t}")

    def drained(timeout=120):
        """Every shard's lazy outbox is empty, with nothing in flight, and
        every replica has what the deliveries wrote."""
        deadline = time.monotonic() + timeout
        while True:
            stats = [ev(leader(g_), "return tarn_lazy_stats()") for g_ in groups]
            if all(s[0] == "ok" and s[1]["depth"] == 0 and s[1]["inflight"] == 0 and s[1]["reserved_entries"] == 0
                   for s in stats):
                break
            if time.monotonic() > deadline:
                raise AssertionError(f"lazy outboxes did not drain: {stats}")
            time.sleep(0.1)
        caught_up()

    def lazy_state(node, sid):
        """What binds lazy generations to space id sid on node: the gens its
        capture publishes, and each descriptor that names sid (by base or in
        its spec), with its phase and target: the target's name, and the
        count and a digest of its live entries in the index's order."""
        state = ev(node, """
            require('fiber').set_slice({warn = 30, err = 120})
            local crc, mp = require('digest').crc32, require('msgpack')
            local sid = ...
            local out, cap = {capture = {}, defs = {}}, box.space._tarn_lazy_capture:get{sid}
            for _, s in ipairs(cap and cap.specs or {}) do out.capture[#out.capture + 1] = s.gen end
            for _, d in box.space._tarn_lazy_defs:pairs() do
                if d.base_id == sid or d.spec.base_space_id == sid then
                    local sp, h, n = box.space[d.gen], crc.new(), 0
                    if sp ~= nil then
                        for _, e in sp.index.ordered:pairs({true}, {iterator = 'EQ'}) do
                            h:update(mp.encode(e:totable())); n = n + 1
                        end
                    end
                    out.defs[tostring(d.gen)] = {base = d.base_id, spec_base = d.spec.base_space_id, phase = d.phase,
                                                 target = sp and sp.name or box.NULL, live = n, digest = h:result()}
                end
            end
            return out
        """, [sid], timeout=60)
        # (none comes back as an empty list)
        return {"capture": state["capture"] or [], "defs": state["defs"] or {}}

    def live_entries(node, gen):
        """(k, tenant_id, id) of each live entry of generation gen's target on
        node: those of an index on (k, id) of a table keyed (tenant_id, id)."""
        return ev(node, """
            require('fiber').set_slice({warn = 30, err = 120})
            local out = {}
            for _, e in box.space[...].index.ordered:pairs({true}, {iterator = 'EQ'}) do
                out[#out + 1] = {e.k1, e.p1, e.p2}
            end
            return out
        """, [gen], timeout=60)

    def lazily_indexed(engine):
        """A lazy index keeps its entries in a space of its own, its target,
        named after the index's generation and sharded by the index's key
        (#82). An ALTER that adds an index copies the table and keeps the
        generation, with its target and entries, bound to the copy (#184),
        and so the table's global index, while a session changes the key,
        inserts, deletes, moves rows to new primary keys and changes another
        column; so does one that adds a column with a fill and an index on
        it."""
        lt, lspace = f"l_{engine}", f"{DB}.l_{engine}"
        n, keys = 20000, 50
        q(c, f"CREATE TABLE {lt} (tenant_id INT NOT NULL, id INT NOT NULL, k INT, v INT, u INT NOT NULL, "
             f"PRIMARY KEY (tenant_id, id), KEY by_k (k, id) COMMENT 'tarn:lazy', UNIQUE KEY uu (u)) "
             f"ENGINE={engine} PARTITION BY KEY(tenant_id)")
        for first in range(0, n, 1000):
            load(c, f"INSERT INTO {lt} VALUES " + ",".join(f"({i % TENANTS},{i},{i % keys},{i},{i})"
                                                      for i in range(first, first + 1000)))
        # (tenant_id, id) -> [k, v, u]
        model = {(i % TENANTS, i): [i % keys, i, i] for i in range(n)}
        fresh = {"id": 5 * FRESH_ID}
        errors, writes = [], [0]
        # (a session of its own, with no writes for a lazy read to wait for)
        reader = connect(args.tarn)
        shard_of = {node: g_["id"] for g_ in groups for node in g_["replicas"]}

        def writer(stop):
            conn = connect(args.tarn, read_timeout=120)
            rnd = random.Random(f"lazy-{engine}-{writes[0]}")
            while not stop.is_set():
                rows, op = sorted(model), rnd.random()
                try:
                    if op < 0.4:
                        a, k = rnd.choice(rows), rnd.randrange(keys)
                        q(conn, f"UPDATE {lt} SET k = %s WHERE tenant_id = %s AND id = %s", (k, *a))
                        model[a][0] = k
                    elif op < 0.6:
                        fresh["id"] += 1
                        a, k = (rnd.randrange(TENANTS), fresh["id"]), rnd.randrange(keys)
                        q(conn, f"INSERT INTO {lt} (tenant_id, id, k, v, u) VALUES (%s, %s, %s, %s, %s)",
                          (*a, k, a[1], a[1]))
                        model[a] = [k, a[1], a[1]]
                    elif op < 0.75:
                        a = rnd.choice(rows)
                        q(conn, f"DELETE FROM {lt} WHERE tenant_id = %s AND id = %s", a)
                        del model[a]
                    elif op < 0.9:
                        # the row moves to a new primary key, which its entry holds
                        a = rnd.choice(rows)
                        fresh["id"] += 1
                        q(conn, f"UPDATE {lt} SET id = %s WHERE tenant_id = %s AND id = %s", (fresh["id"], *a))
                        model[(a[0], fresh["id"])] = model.pop(a)
                    else:
                        a, v = rnd.choice(rows), rnd.randrange(10**6)
                        q(conn, f"UPDATE {lt} SET v = %s WHERE tenant_id = %s AND id = %s", (v, *a))
                        model[a][1] = v
                    writes[0] += 1
                except pymysql.MySQLError as e:
                    # (vinyl admission's refusal wrote nothing: the next
                    # write comes after a pause)
                    if not vinyl_refusal(e):
                        errors.append(f"lazy: {e.args}")
                    time.sleep(0.05)
                time.sleep(0.003)

        def answers(label):
            """The lazy index finds every row by its key, on the lazy path."""
            reads, fallbacks = stat(reader, "Tarn_lazy_reads"), stat(reader, "Tarn_lazy_fallbacks")
            wrong = []
            for k in range(keys):
                got = q(reader, f"SELECT tenant_id, id FROM {lt} WHERE k = %s ORDER BY id", (k,))
                want = tuple(sorted((key for key, v in model.items() if v[0] == k), key=lambda r: r[1]))
                if tuple(tuple(r) for r in got) != want:
                    wrong.append({"k": k, "rows": len(got), "want": len(want),
                                  "missing": sorted(set(want) - set(got))[:3],
                                  "extra": sorted(set(got) - set(want))[:3]})
            reads = stat(reader, "Tarn_lazy_reads") - reads
            fallbacks = stat(reader, "Tarn_lazy_fallbacks") - fallbacks
            check(f"{engine}: {label}", not wrong and reads == keys and fallbacks == 0,
                  {"wrong": wrong[:5], "lazy reads": reads, "fallbacks": fallbacks})

        drained()
        sids = {g_["id"]: space_id(leader(g_), lspace) for g_ in groups}
        states = {node: lazy_state(node, sids[shard_of[node]]) for node in nodes}
        gen = states[nodes[0]]["capture"][0] if states[nodes[0]]["capture"] else None
        check(f"{engine}: a lazy index has one generation, the same on every replica, ready and bound to the table",
              gen is not None and all(s["capture"] == [gen] and list(s["defs"]) == [str(gen)]
                                      and s["defs"][str(gen)]["phase"] == "ready" for s in states.values()), states)
        kept = {g_["id"]: {v["id"] for v in globals_of(leader(g_), lspace).values()} for g_ in groups}
        check(f"{engine}: its unique key without the shard key has one global index space per shard",
              all(len(v) == 1 for v in kept.values()) and len({frozenset(v) for v in kept.values()}) == 1, kept)
        answers("the lazy index finds every row by its key")
        for round_, sql in enumerate((f"ALTER TABLE {lt} ADD INDEX kv (v)",
                                      f"ALTER TABLE {lt} ADD COLUMN x INT NOT NULL DEFAULT 3, ADD INDEX kx (x)")):
            before = {g_["id"]: space_id(leader(g_), lspace) for g_ in groups}
            before_terms = terms()
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
            # (and writes after it, which the copy's capture takes)
            time.sleep(1)
            stop.set()
            th.join(30)
            check(f"{engine}: {sql}", alter_error is None, alter_error)
            check(f"{engine}: writes went on during it, and none failed", during > 0 and not errors,
                  {"during": during, "errors": errors[:5]})
            check(f"{engine}: no replica changed its election term", terms() == before_terms,
                  {"before": before_terms, "after": terms()})
            drained()
            after = {g_["id"]: space_id(leader(g_), lspace) for g_ in groups}
            records = {g_["id"]: engine_record(leader(g_), lspace, before[g_["id"]]) for g_ in groups}
            check(f"{engine}: that change was a copy, published on every shard",
                  all(after[s] != before[s] for s in after)
                  and all(r == {"phase": "published", "rebuild": True} for r in records.values()), records)
            states = {node: lazy_state(node, after[shard_of[node]]) for node in nodes}
            olds = {node: lazy_state(node, before[shard_of[node]]) for node in nodes}
            check(f"{engine}: every replica keeps the generation, ready, its descriptor and capture bound to the copy",
                  all(s["capture"] == [gen] and list(s["defs"]) == [str(gen)]
                      and s["defs"][str(gen)] | {"live": 0, "digest": 0}
                      == {"base": after[shard_of[node]], "spec_base": after[shard_of[node]], "phase": "ready",
                          "target": f"_tarn_lix_{gen}", "live": 0, "digest": 0}
                      for node, s in states.items()), states)
            check(f"{engine}: nothing binds a generation to the old space any more",
                  all(o == {"capture": [], "defs": {}} for o in olds.values()), olds)
            check(f"{engine}: the replicas of each shard agree on the target's live entries",
                  all(states[node]["defs"].get(str(gen)) == states[g_["replicas"][0]]["defs"].get(str(gen))
                      for g_ in groups for node in g_["replicas"]),
                  {node: states[node]["defs"].get(str(gen)) for node in nodes})
            live = [tuple(e) for g_ in groups for e in live_entries(g_["replicas"][0], gen)]
            want = {(v[0], *key) for key, v in model.items()}
            check(f"{engine}: the target's live entries are the table's rows, one each",
                  len(live) == len(want) and set(live) == want,
                  {"entries": len(live), "rows": len(want), "missing": sorted(want - set(live))[:5],
                   "extra": sorted(set(live) - want)[:5]})
            rows = {(r[0], r[1]): list(r[2:]) for r in q(c, f"SELECT tenant_id, id, k, v, u FROM {lt}")}
            check(f"{engine}: the table holds the model's rows", rows == model,
                  {"rows": len(rows), "model": len(model),
                   "differ": [key for key in model if rows.get(key) != model[key]][:5]})
            answers("the lazy index finds every row by its key, from the kept target")
            spaces = {node: globals_of(node, lspace) for node in nodes}
            names = {node: uix_name(node, lspace, "uu") for node in nodes}
            stale = {node: named_after(node, before[shard_of[node]]) for node in nodes}
            check(f"{engine}: the copy keeps the global index's space too, by id, named after the copy, one "
                  f"entry per row on every replica",
                  all({v["id"] for v in spaces[node].values()} == kept[shard_of[node]] for node in nodes)
                  and all(set(spaces[node]) == {names[node]} for node in nodes) and not any(stale.values())
                  and all(spaces[node] == spaces[g_["replicas"][0]] for g_ in groups for node in g_["replicas"])
                  and sum(v["rows"] for g_ in groups for v in spaces[g_["replicas"][0]].values()) == len(model),
                  {"spaces": spaces, "names": names, "named after the old space": stale, "rows": len(model)})
            sample = random.Random(f"l-{engine}-{round_}").sample(sorted(model), 100)
            found = [q(c, f"SELECT tenant_id, id FROM {lt} WHERE u = %s", (model[key][2],)) for key in sample]
            check(f"{engine}: the global index finds rows by their unique value",
                  all(f == (key,) for f, key in zip(found, sample)),
                  [(key, f) for key, f in zip(sample, found) if f != (key,)][:5])
            left = {node: leftovers(node) for node in nodes}
            check(f"{engine}: no copy is left on any replica", all(not v for v in left.values()), left)
        filled = q(c, f"SELECT COUNT(*) FROM {lt} WHERE x = 3")[0][0]
        check(f"{engine}: the index on the new column finds every row", filled == len(model),
              {"found": filled, "rows": len(model)})

        # A TRUNCATE retires the kept generation and publishes a new one.
        q(c, f"TRUNCATE TABLE {lt}")
        model.clear()
        for key, k in (((1, 1), 7), ((2, 2), 7), ((3, 3), 8)):
            q(c, f"INSERT INTO {lt} (tenant_id, id, k, v, u) VALUES (%s, %s, %s, 0, %s)", (*key, k, key[1]))
            model[key] = [k, 0, key[1]]
        drained()
        sid = {g_["id"]: space_id(leader(g_), lspace) for g_ in groups}
        states = {node: lazy_state(node, sid[shard_of[node]]) for node in nodes}
        fresh_gen = states[nodes[0]]["capture"][0] if states[nodes[0]]["capture"] else None
        check(f"{engine}: TRUNCATE retires the kept generation and publishes a new one on every replica",
              fresh_gen not in (None, gen)
              and all(s["capture"] == [fresh_gen] and s["defs"].get(str(gen), {}).get("phase") == "retired"
                      and s["defs"].get(str(fresh_gen), {}).get("phase") == "ready" for s in states.values()),
              states)
        emptied = {node: sum(v["rows"] for v in globals_of(node, lspace).values()) for node in nodes}
        check(f"{engine}: TRUNCATE empties the kept global index too",
              sum(emptied[g_["replicas"][0]] for g_ in groups) == 3, emptied)
        answers("after TRUNCATE the lazy index finds the rows written since")
        q(c, f"DROP TABLE {lt}")
        caught_up()
        phases = {node: ev(node, """
            local out = {}
            for i, gen in ipairs({...}) do
                local d = box.space._tarn_lazy_defs:get{gen}
                out[i] = d and d.phase or 'none'
            end
            return out
        """, [gen, fresh_gen]) for node in nodes}
        check(f"{engine}: DROP TABLE retires its generations on every replica",
              all(p in ("retired", "none") for v in phases.values() for p in v), phases)
        uid = next(iter(kept[groups[0]["id"]]))
        gone = {node: ev(node, "return box.space[...] ~= nil", [uid]) for node in nodes}
        check(f"{engine}: and drops the kept global index", not any(gone.values()), gone)

    def lazily_made(engine):
        """An ALTER that adds a lazy index copies the table (#184): the new
        generation is built online against the table's space before the
        copy, its writes captured meanwhile, and published with the copy,
        which keeps the generations of the table's other lazy indexes. The
        table's first lazy index, then a second one along with an ordinary
        index, while a session changes the keys, inserts, deletes and moves
        rows to new primary keys."""
        mt, mspace = f"m_{engine}", f"{DB}.m_{engine}"
        n, keys = 20000, 40
        q(c, f"CREATE TABLE {mt} (tenant_id INT NOT NULL, id INT NOT NULL, k INT, w INT, v INT, "
             f"PRIMARY KEY (tenant_id, id)) ENGINE={engine} PARTITION BY KEY(tenant_id)")
        for first in range(0, n, 1000):
            load(c, f"INSERT INTO {mt} VALUES " + ",".join(f"({i % TENANTS},{i},{i % keys},{i % (keys + 7)},{i})"
                                                      for i in range(first, first + 1000)))
        # (tenant_id, id) -> {column: value}
        model = {(i % TENANTS, i): {"k": i % keys, "w": i % (keys + 7), "v": i} for i in range(n)}
        fresh = {"id": 7 * FRESH_ID}
        errors, writes = [], [0]
        reader = connect(args.tarn)
        shard_of = {node: g_["id"] for g_ in groups for node in g_["replicas"]}

        def writer(stop):
            conn = connect(args.tarn, read_timeout=120)
            rnd = random.Random(f"made-{engine}-{writes[0]}")
            while not stop.is_set():
                rows, op = sorted(model), rnd.random()
                try:
                    if op < 0.5:
                        a, col = rnd.choice(rows), rnd.choice(("k", "w", "v"))
                        value = rnd.randrange(keys + 7)
                        q(conn, f"UPDATE {mt} SET {col} = %s WHERE tenant_id = %s AND id = %s", (value, *a))
                        model[a][col] = value
                    elif op < 0.7:
                        fresh["id"] += 1
                        a = (rnd.randrange(TENANTS), fresh["id"])
                        row = {"k": rnd.randrange(keys), "w": rnd.randrange(keys + 7), "v": a[1]}
                        q(conn, f"INSERT INTO {mt} (tenant_id, id, k, w, v) VALUES (%s, %s, %s, %s, %s)",
                          (*a, row["k"], row["w"], row["v"]))
                        model[a] = row
                    elif op < 0.85:
                        a = rnd.choice(rows)
                        q(conn, f"DELETE FROM {mt} WHERE tenant_id = %s AND id = %s", a)
                        del model[a]
                    else:
                        a = rnd.choice(rows)
                        fresh["id"] += 1
                        q(conn, f"UPDATE {mt} SET id = %s WHERE tenant_id = %s AND id = %s", (fresh["id"], *a))
                        model[(a[0], fresh["id"])] = model.pop(a)
                    writes[0] += 1
                except pymysql.MySQLError as e:
                    # (vinyl admission's refusal wrote nothing: the next
                    # write comes after a pause)
                    if not vinyl_refusal(e):
                        errors.append(f"made: {e.args}")
                    time.sleep(0.05)
                time.sleep(0.003)

        def answers(col, label):
            """The lazy index on col finds the rows of a sample of its keys,
            on the lazy path."""
            reads, fallbacks = stat(reader, "Tarn_lazy_reads"), stat(reader, "Tarn_lazy_fallbacks")
            wrong, sample = [], range(0, keys + 7, 3)
            for key in sample:
                got = q(reader, f"SELECT tenant_id, id FROM {mt} WHERE {col} = %s ORDER BY id", (key,))
                want = tuple(sorted((r for r, row in model.items() if row[col] == key), key=lambda r: r[1]))
                if tuple(tuple(r) for r in got) != want:
                    wrong.append({"key": key, "rows": len(got), "want": len(want),
                                  "missing": sorted(set(want) - set(got))[:3],
                                  "extra": sorted(set(got) - set(want))[:3]})
            reads = stat(reader, "Tarn_lazy_reads") - reads
            fallbacks = stat(reader, "Tarn_lazy_fallbacks") - fallbacks
            check(f"{engine}: {label}", not wrong and reads == len(sample) and fallbacks == 0,
                  {"wrong": wrong[:5], "lazy reads": reads, "fallbacks": fallbacks})

        gens = []
        for sql, cols in ((f"ALTER TABLE {mt} ADD INDEX by_w (w, id) COMMENT 'tarn:lazy'", ("w",)),
                          (f"ALTER TABLE {mt} ADD INDEX by_k (k, id) COMMENT 'tarn:lazy', ADD INDEX kv (v)",
                           ("w", "k"))):
            before = {g_["id"]: space_id(leader(g_), mspace) for g_ in groups}
            before_terms = terms()
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
            check(f"{engine}: writes went on during it, and none failed", during > 0 and not errors,
                  {"during": during, "errors": errors[:5]})
            check(f"{engine}: no replica changed its election term", terms() == before_terms,
                  {"before": before_terms, "after": terms()})
            drained()
            after = {g_["id"]: space_id(leader(g_), mspace) for g_ in groups}
            records = {g_["id"]: engine_record(leader(g_), mspace, before[g_["id"]]) for g_ in groups}
            check(f"{engine}: that change was a copy, published on every shard",
                  all(after[s_] != before[s_] for s_ in after)
                  and all(r == {"phase": "published", "rebuild": True} for r in records.values()), records)
            states = {node: lazy_state(node, after[shard_of[node]]) for node in nodes}
            olds = {node: lazy_state(node, before[shard_of[node]]) for node in nodes}
            made = sorted(set(states[nodes[0]]["capture"]) - set(gens))
            check(f"{engine}: it made one generation, kept the table's others",
                  len(made) == 1 and set(gens) <= set(states[nodes[0]]["capture"]), {"gens": gens, "made": made})
            gens = gens + made
            check(f"{engine}: every replica has each generation ready, bound to the copy",
                  all(sorted(s_["capture"]) == sorted(gens) and sorted(s_["defs"]) == sorted(map(str, gens))
                      and all(d | {"live": 0, "digest": 0}
                              == {"base": after[shard_of[node]], "spec_base": after[shard_of[node]],
                                  "phase": "ready", "target": f"_tarn_lix_{g}", "live": 0, "digest": 0}
                              for g, d in ((int(g), d) for g, d in s_["defs"].items()))
                      for node, s_ in states.items()), states)
            check(f"{engine}: nothing binds a generation to the old space any more",
                  all(o == {"capture": [], "defs": {}} for o in olds.values()), olds)
            check(f"{engine}: the replicas of each shard agree on each target's live entries",
                  all(states[node]["defs"] == states[g_["replicas"][0]]["defs"]
                      for g_ in groups for node in g_["replicas"]), {node: states[node]["defs"] for node in nodes})
            for gen, col in zip(gens, cols):
                live = [tuple(e) for g_ in groups for e in live_entries(g_["replicas"][0], gen)]
                want = {(row[col], *key) for key, row in model.items()}
                check(f"{engine}: the live entries of the index on {col} are the table's rows, one each",
                      len(live) == len(want) and set(live) == want,
                      {"entries": len(live), "rows": len(want), "missing": sorted(want - set(live))[:5],
                       "extra": sorted(set(live) - want)[:5]})
                answers(col, f"the lazy index on {col} finds rows by its key")
            rows = {(r[0], r[1]): {"k": r[2], "w": r[3], "v": r[4]}
                    for r in q(c, f"SELECT tenant_id, id, k, w, v FROM {mt}")}
            check(f"{engine}: the table holds the model's rows", rows == model,
                  {"rows": len(rows), "model": len(model),
                   "differ": [key for key in model if rows.get(key) != model[key]][:5]})
            left = {node: leftovers(node) for node in nodes}
            check(f"{engine}: no copy is left on any replica", all(not v for v in left.values()), left)
        q(c, f"DROP TABLE {mt}")
        caught_up()
        phases = {node: ev(node, """
            local out = {}
            for i, gen in ipairs({...}) do
                local d = box.space._tarn_lazy_defs:get{gen}
                out[i] = d and d.phase or 'none'
            end
            return out
        """, gens) for node in nodes}
        check(f"{engine}: DROP TABLE retires its generations on every replica",
              all(p in ("retired", "none") for v in phases.values() for p in v), phases)

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
            load(c, f"INSERT INTO {t} VALUES " + ",".join(f"({i % TENANTS},{i},{i % 97},{i})" for i in rows))

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
                    # (vinyl admission's refusal wrote nothing: the next
                    # write comes after a pause)
                    if not vinyl_refusal(e):
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
                    # (vinyl admission's refusal wrote nothing: the next
                    # write comes after a pause)
                    if not vinyl_refusal(e):
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
                    # (vinyl admission's refusal wrote nothing: the next
                    # write comes after a pause)
                    if not vinyl_refusal(e):
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

        old_ids = {g["id"]: ev(leader(g), "return box.space[...].id", [space]) for g in groups}
        sliced = {g["id"]: slices_of(g) for g in groups}
        catchup_bounds(True)
        try:
            online(f"ALTER TABLE {t} ADD INDEX kk (k), ADD UNIQUE INDEX uc (tenant_id, c)", ("kk", "uc"))
        finally:
            catchup_bounds(False)
        passes = {g["id"]: passes_of(leader(g), old_ids[g["id"]]) for g in groups}
        settled = {g["id"]: settled_of(leader(g), old_ids[g["id"]]) for g in groups}
        sliced = {g["id"]: slices_of(g) - sliced[g["id"]] for g in groups}
        print(f"  ({engine}: catch-up passes by shard: {passes}; calls that returned for more: {sliced})",
              flush=True)
        check(f"{engine}: each shard's copy ran catch-up passes", all(n > 0 for n in passes.values()), passes)
        check(f"{engine}: each shard's copy kept its capture durably, and its passes settled it",
              all(n > 0 for n in settled.values()), settled)
        check(f"{engine}: each shard's copy took several calls, in slices", all(n > 0 for n in sliced.values()),
              sliced)
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
        load(c, f"INSERT INTO {d} VALUES " + ",".join(f"({i % 8},{i},{i})" for i in range(2000))
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
        online_unique(engine)
        renamed(engine)
        lazily_indexed(engine)
        lazily_made(engine)
        catch_up_round(engine)
        checked_copy(engine)
        dropped_indexes(engine)

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
