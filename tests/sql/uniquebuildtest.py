#!/usr/bin/env python3
"""ALTER TABLE adds a unique key without the shard key to a populated table:
its global index is built online (#292), with no row limit, while writes go
on, and a value two rows share fails the change with 1062 and leaves the
table as it was.

Every storage node first gets an alter_check_rows below the table's rows per
shard, above which filling the index behind the table's fence is refused
(1846).

  * Online, with writes: a table of `--rows` rows in 64 tenants. One session
    keeps a model of a quarter of them: it hands a row's value to another
    row, perhaps on the other shard, sets values to NULL and back, inserts,
    deletes and moves rows to new primary keys, keeping the values unique.
    Another adds 1 to k in the other rows. ALTER TABLE ... ADD UNIQUE KEY
    runs meanwhile. It succeeds, none of the writes fails, and afterwards:
      - the table holds the model's rows, and every increment;
      - SHOW CREATE TABLE shows the key;
      - the global index holds one entry per row with a value, the row's,
        and no conflicts space is left on any node;
      - every row is found by its value (a sample);
      - a duplicate in another tenant is refused (1062).
  * Duplicates from before: two rows in different tenants share a value.
    The change fails with MariaDB's 1062, the table keeps no key and no
    index space, and once the values differ, the same change succeeds.
  * A duplicate written during the change: a session inserts a row with a
    value another row holds while the change runs. The insert goes in (as
    in MariaDB, whose online ADD UNIQUE refuses no concurrent write) and
    the change fails with 1062, leaving the table as it was.
  * Duplicates resolved during the change: during the backfill, a row takes
    a new value, another is recorded as a conflict there, and the first
    moves on, so the conflict takes the entry over; under READ COMMITTED,
    a conflict's row is deleted. The change succeeds, and the key finds
    each value's row.
  * Taken over (with two SQL servers): the first case on another table, the
    change sent to the first server, which is killed once the backfill has
    passed some rows, and the writes to the second, which takes the change
    over after ddl_job_stale and finishes it.

    uniquebuildtest.py --tarn HOST:PORT[,HOST:PORT] --cluster-dir DIR [--rows N]
"""
import argparse
import json
import os
from pathlib import Path
import random
import signal
import subprocess
import threading
import time

import pymysql

from lazytest import ev

DB = "unique_build"
TENANTS = 64
LIMIT = 500        # alter_check_rows on every storage node during the test
FRESH_ID = 10**6   # ids the model's session inserts or moves rows to are above this
FRESH_U = 10**7    # and the values it gives rows
RETRY = (1205, 1213, 1412)
ROOT = Path(__file__).resolve().parents[2]


def connect(endpoint, db=DB, read_timeout=600):
    host, port = endpoint.rsplit(":", 1)
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db, autocommit=True,
                           charset="utf8mb4", connect_timeout=3, read_timeout=read_timeout, write_timeout=10)


def q(c, sql, args=None):
    with c.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall() if cur.description else cur.rowcount


def attempt(c, sql, args=None):
    """None, or the error's arguments."""
    try:
        q(c, sql, args)
        return None
    except pymysql.MySQLError as e:
        return e.args


fails = 0


def check(name, cond, detail=None):
    global fails
    if cond:
        print(f"  ok   {name}", flush=True)
    else:
        fails += 1
        print(f"  FAIL {name}" + (f"  -- {json.dumps(detail, default=str)}" if detail is not None else ""),
              flush=True)


def wait_for(probe, timeout, what):
    deadline = time.monotonic() + timeout
    while True:
        r = probe()
        if r:
            return r
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(0.05)


def owned(i):
    """The rows the model's session writes (the others get increments)."""
    return (i // TENANTS) % 4 == 3 or i >= FRESH_ID


class Cluster:
    def __init__(self, cluster_dir):
        self.dir = Path(cluster_dir)
        self.groups = json.loads((self.dir / "cluster.json").read_text())["shards"]
        self.nodes = [n for g in self.groups for n in g["replicas"]]
        assert len(self.groups) == 2, self.groups

    def leader(self, group):
        def probe():
            for node in group["replicas"]:
                try:
                    if not ev(node, "return box.info.ro"):
                        return node
                except Exception:  # a replica that does not answer
                    pass
            return None
        return wait_for(probe, 30, f"shard {group['id']}'s leader")

    def hidden(self, space):
        """On every node: the table's global index spaces, by name, with
        their tuples' first three fields (an entry's key, then the primary
        key's columns the key lacks)."""
        out = {}
        for node in self.nodes:
            r = ev(node, """
                local sp, out = box.space[...], {}
                if sp == nil then return out end
                local tag = '_tarn_uix_' .. sp.id .. '_'
                for _, s in box.space._space.index.name:pairs({tag}, {iterator = 'GE'}) do
                    if s.name:sub(1, #tag) ~= tag then break end
                    local rows = {}
                    for _, t in box.space[s.id]:pairs() do rows[#rows + 1] = {t[1], t[2], t[3]} end
                    out[s.name] = rows
                end
                return out
            """, [space])
            out[node] = r if isinstance(r, dict) else {}   # (an empty table encodes as [])
        return out

    def job(self, space):
        """The schema change of `space` in progress, from the first shard's
        leader: its phase and arguments (None: none)."""
        r = ev(self.leader(self.groups[0]), """
            local t = box.space._tarn_meta:get{'job:' .. ...}
            return t and {phase = t.value.phase, a = t.value.a, owner = t.value.owner} or box.NULL
        """, [space])
        return r or None


class Writers:
    """Sessions writing to table `name` (made by `populate`) while a schema
    change runs. One keeps a model of the rows it owns: it hands a row's
    value to another row, sets values to NULL and back, inserts, deletes and
    moves rows to new primary keys, keeping the values unique. Another adds
    1 to k in the other rows. A statement that fails with an error in
    `retry` (a known abort) took no effect: they go on, as a client would,
    counting it."""

    def __init__(self, endpoint, name, n, seed, retry=()):
        self.endpoint, self.name, self.seed, self.retry = endpoint, name, seed, retry
        self.model = {(i % TENANTS, i): (None if i % 50 == 0 else i) for i in range(n) if owned(i)}
        self.others = [(i % TENANTS, i) for i in range(n) if not owned(i)]
        self.incs, self.errors, self.writes, self.retried = {}, [], 0, 0
        self.fresh = {"id": FRESH_ID, "u": FRESH_U}
        self.stop = threading.Event()
        self.threads = [threading.Thread(target=self.modeler), threading.Thread(target=self.incrementer)]

    def __enter__(self):
        for t in self.threads:
            t.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        for t in self.threads:
            t.join()

    def new(self, what):
        self.fresh[what] += 1
        return self.fresh[what]

    def modeler(self):
        conn = connect(self.endpoint, read_timeout=120)
        rnd, model, t = random.Random(f"{self.seed} model"), self.model, self.name
        while not self.stop.is_set():
            keys = sorted(model)
            op = rnd.random()
            try:
                if op < 0.35:
                    # a hands its value to b, which may be on the other shard
                    a, b = rnd.sample(keys, 2)
                    x, y = model[a], self.new("u")
                    q(conn, f"UPDATE {t} SET u = %s WHERE tenant_id = %s AND id = %s", (y, *a))
                    model[a] = y
                    q(conn, f"UPDATE {t} SET u = %s WHERE tenant_id = %s AND id = %s", (x, *b))
                    model[b] = x
                elif op < 0.45:
                    a = rnd.choice(keys)
                    v = None if model[a] is not None else self.new("u")
                    q(conn, f"UPDATE {t} SET u = %s WHERE tenant_id = %s AND id = %s", (v, *a))
                    model[a] = v
                elif op < 0.65:
                    tn, i, v = rnd.randrange(TENANTS), self.new("id"), self.new("u")
                    q(conn, f"INSERT INTO {t} (tenant_id, id, u, k) VALUES (%s, %s, %s, 0)", (tn, i, v))
                    model[(tn, i)] = v
                elif op < 0.8:
                    a = rnd.choice(keys)
                    q(conn, f"DELETE FROM {t} WHERE tenant_id = %s AND id = %s", a)
                    del model[a]
                else:
                    # the row moves to a new primary key, with its value
                    a, j = rnd.choice(keys), self.new("id")
                    q(conn, f"UPDATE {t} SET id = %s WHERE tenant_id = %s AND id = %s", (j, *a))
                    model[(a[0], j)] = model.pop(a)
                self.writes += 1
            except pymysql.MySQLError as e:
                self.failed("model", e)
            time.sleep(0.002)

    def incrementer(self):
        conn = connect(self.endpoint, read_timeout=120)
        rnd = random.Random(f"{self.seed} increments")
        while not self.stop.is_set():
            a = rnd.choice(self.others)
            try:
                q(conn, f"UPDATE {self.name} SET k = k + 1 WHERE tenant_id = %s AND id = %s", a)
                self.incs[a] = self.incs.get(a, 0) + 1
                self.writes += 1
            except pymysql.MySQLError as e:
                self.failed("increment", e)

    def failed(self, what, e):
        if e.args[0] in self.retry:
            self.retried += 1
        else:
            self.errors.append(f"{what}: {e.args}")
        time.sleep(0.05)


def populate(c, name, n, k=True):
    """Table `name`: n rows in TENANTS tenants, row i with u = i (NULL for
    every 50th, when it has k)."""
    q(c, f"CREATE TABLE {name} (tenant_id INT, id INT, u INT, {'k INT NOT NULL, ' if k else ''}"
         "PRIMARY KEY (tenant_id, id)) PARTITION BY KEY(tenant_id)")
    for first in range(0, n, 1000):
        q(c, f"INSERT INTO {name} VALUES " + ",".join(
            f"({i % TENANTS},{i},{'NULL' if k and i % 50 == 0 else i}{',0' if k else ''})"
            for i in range(first, min(n, first + 1000))))


def built(cl, c, w, name, key):
    """The checks after a change that added unique key `key` on u to table
    `name` while writers `w` wrote to it."""
    check(f"the writes go on meanwhile ({w.writes} of them), and none fails"
          + (f" ({w.retried} sent again after a deadlock or lock wait timeout)" if w.retry else ""),
          w.writes > 0 and not w.errors, w.errors[:5])
    shown = q(c, f"SHOW CREATE TABLE {name}")[0][1]
    check("SHOW CREATE TABLE shows the key", f"UNIQUE KEY `{key}` (`u`)" in shown, shown)
    rows = {(tn, i): u for tn, i, u in q(c, f"SELECT tenant_id, id, u FROM {name}")}
    mine = {r: u for r, u in rows.items() if owned(r[1])}
    check("the table holds the model's rows", mine == w.model,
          {"missing": len(set(w.model) - set(mine)), "extra": len(set(mine) - set(w.model)),
           "wrong": sum(1 for r in w.model if r in mine and mine[r] != w.model[r])})
    ks = {(tn, i): k for tn, i, k in q(c, f"SELECT tenant_id, id, k FROM {name}")}
    check(f"and every increment ({sum(w.incs.values())})", all(ks.get(a) == v for a, v in w.incs.items()))
    spaces = cl.hidden(f"{DB}.{name}")
    names = sorted({s for node in cl.nodes for s in spaces[node]})
    check("the global index has its space on every node, and no conflicts space is left",
          len(names) == 1 and not names[0].endswith("c")
          and all(set(spaces[node]) == set(names) for node in cl.nodes), names)
    got = {}
    for node in {cl.leader(g) for g in cl.groups}:
        for u, tn, i in (spaces[node].get(names[0], []) if names else []):
            got[u] = (tn, i)
    want = {u: r for r, u in rows.items() if u is not None}
    check(f"it holds one entry per row with a value, the row's ({len(want)})", got == want,
          {"missing": len(set(want) - set(got)), "extra": len(set(got) - set(want)),
           "wrong": sum(1 for u in want if u in got and got[u] != want[u])})
    sample = random.Random(name).sample(sorted(want), min(500, len(want)))
    lost = [u for u in sample if q(c, f"SELECT tenant_id, id FROM {name} WHERE u = %s", (u,)) != (want[u],)]
    check("every row is found by its value (a sample of 500)", not lost, lost[:5])
    u0 = sample[0]
    dup = attempt(c, f"INSERT INTO {name} VALUES (%s, %s, %s, 0)", ((want[u0][0] + 1) % TENANTS, 9 * FRESH_ID, u0))
    check("the key refuses a duplicate in another tenant (1062)", dup is not None and dup[0] == 1062, dup)


def online(cl, c, endpoint, n):
    print("online, while writes go on")
    populate(c, "t", n)
    with Writers(endpoint, "t", n, "online") as w:
        time.sleep(1)
        t0 = time.monotonic()
        failed = attempt(c, "ALTER TABLE t ADD UNIQUE KEY tu (u)")
        took = time.monotonic() - t0
        time.sleep(1)
    check(f"the change succeeds, with alter_check_rows {LIMIT} below the rows per shard ({took:.1f} s)",
          failed is None, failed)
    built(cl, c, w, "t", "tu")


def duplicates_before(cl, c):
    print("a value two rows share before the change")
    populate(c, "d", 2000, k=False)
    q(c, "INSERT INTO d VALUES (1, 5000, 7), (2, 5001, 8)")
    failed = attempt(c, "ALTER TABLE d ADD UNIQUE KEY du (u)")
    check("the change fails with MariaDB's 1062", failed is not None and failed[0] == 1062
          and failed[1] in ("Duplicate entry '7' for key 'du'", "Duplicate entry '8' for key 'du'"), failed)
    shown = q(c, "SHOW CREATE TABLE d")[0][1]
    left = {node: s for node, s in cl.hidden(f"{DB}.d").items() if s}
    check("the table keeps no key, and no node an index space", "`du`" not in shown and not left, [shown, left])
    q(c, "INSERT INTO d VALUES (3, 5002, %s)", (FRESH_U,))
    q(c, "UPDATE d SET u = %s WHERE tenant_id = 1 AND id = 5000", (FRESH_U + 1,))
    q(c, "UPDATE d SET u = %s WHERE tenant_id = 2 AND id = 5001", (FRESH_U + 2,))
    again = attempt(c, "ALTER TABLE d ADD UNIQUE KEY du (u)")
    check("the table takes writes, and once the values differ the same change succeeds", again is None, again)


def duplicate_during(cl, c, endpoint, n):
    print("a value a row holds, written during the change")
    populate(c, "w", n, k=False)
    outcome = {}

    def change():
        outcome["alter"] = attempt(connect(endpoint), "ALTER TABLE w ADD UNIQUE KEY wu (u)")

    th = threading.Thread(target=change)
    th.start()
    wait_for(lambda: (cl.job(f"{DB}.w") or {}).get("phase") == "backfill", 60, "the backfill")
    conn = connect(endpoint)
    for _ in range(100):
        outcome["insert"] = attempt(conn, "INSERT INTO w VALUES (5, %s, 6)", (2 * FRESH_ID,))
        if outcome["insert"] is None or outcome["insert"][0] not in RETRY:
            break
        time.sleep(0.05)
    th.join()
    check("the insert goes in, and the change fails with MariaDB's 1062", outcome["insert"] is None
          and outcome["alter"] == (1062, "Duplicate entry '6' for key 'wu'"), outcome)
    shown = q(c, "SHOW CREATE TABLE w")[0][1]
    left = {node: s for node, s in cl.hidden(f"{DB}.w").items() if s}
    check("... leaving the table as it was", "`wu`" not in shown and not left, [shown, left])
    check("with both rows", q(c, "SELECT tenant_id, id FROM w WHERE u = 6 ORDER BY id")
          == ((6, 6), (5, 2 * FRESH_ID)))


def resolved_during(cl, c, endpoint, n):
    print("values rows share during the change, and no longer when it checks")
    populate(c, "r", n, k=False)
    outcome = {}

    def change():
        outcome["alter"] = attempt(connect(endpoint), "ALTER TABLE r ADD UNIQUE KEY ru (u)")

    th = threading.Thread(target=change)
    th.start()
    wait_for(lambda: (cl.job(f"{DB}.r") or {}).get("phase") == "backfill", 60, "the backfill")
    ser, rc = connect(endpoint), connect(endpoint)
    q(rc, "SET SESSION transaction_isolation = 'READ-COMMITTED'")
    v, i = FRESH_U, FRESH_ID
    steps = [
        # a row takes v's entry, another is recorded as a conflict there,
        # and the first leaves v: the conflict takes the entry over
        (ser, "INSERT INTO r VALUES (5, %s, %s)", (i + 1, v)),
        (ser, "INSERT INTO r VALUES (9, %s, %s)", (i + 2, v)),
        (ser, "UPDATE r SET u = %s WHERE tenant_id = 5 AND id = %s", (v + 1, i + 1)),
        # (READ COMMITTED) a conflict at v + 2, whose row goes
        (rc, "INSERT INTO r VALUES (7, %s, %s)", (i + 3, v + 2)),
        (rc, "INSERT INTO r VALUES (11, %s, %s)", (i + 4, v + 2)),
        (rc, "DELETE FROM r WHERE tenant_id = 11 AND id = %s", (i + 4,)),
    ]
    errors = [(sql, e) for conn, sql, args in steps if (e := attempt(conn, sql, args)) is not None]
    phase = (cl.job(f"{DB}.r") or {}).get("phase")
    th.join()
    check("the writes go in, all during the backfill", not errors and phase == "backfill", [errors, phase])
    check("the change succeeds", outcome["alter"] is None, outcome)
    found = [q(c, "SELECT tenant_id, id FROM r WHERE u = %s", (x,)) for x in (v, v + 1, v + 2)]
    check("the key finds each value's row: the conflict that took v's entry over, and the others",
          found == [((9, i + 2),), ((5, i + 1),), ((7, i + 3),)], found)
    left = sorted({s for node, spaces in cl.hidden(f"{DB}.r").items() for s in spaces})
    check("no conflicts space is left", len(left) == 1 and not left[0].endswith("c"), left)


def taken_over(cl, endpoints, n):
    print("taken over by the second SQL server when the first stops")
    pid = int((cl.dir / "tarn1.pid").read_text())
    c2 = connect(endpoints[1])
    populate(c2, "k", n)
    outcome = {}

    def change():
        try:
            outcome["alter"] = attempt(connect(endpoints[0]), "ALTER TABLE k ADD UNIQUE KEY ku (u)")
        except pymysql.MySQLError as e:  # (connecting)
            outcome["alter"] = e.args

    def passed():
        j = cl.job(f"{DB}.k")
        if j and j["phase"] == "backfill" and any(len(cur) > 2 and cur[2] is not None
                                                  for cur in j["a"].get("cursors", [])):
            return j
        return None

    # (A transaction of the backfill that the first server leaves prepared
    # holds its rows until the shards recover it: a statement that meets
    # it fails with 1213 once its own retries run out.)
    with Writers(endpoints[1], "k", n, "takeover", retry=(1205, 1213)) as w:
        time.sleep(1)
        th = threading.Thread(target=change)
        th.start()
        j = wait_for(passed, 120, "the backfill to pass some rows")
        os.kill(pid, signal.SIGKILL)
        th.join(30)
        check("the first server's session is cut off in the backfill", outcome.get("alter") is not None
              and outcome["alter"][0] in (2006, 2013), [outcome, j["a"]["cursors"]])
        t0 = time.monotonic()
        wait_for(lambda: cl.job(f"{DB}.k") is None, 180, "the second server to finish the change")
        took = time.monotonic() - t0
        time.sleep(1)
    check(f"the second server takes the change over and finishes it ({took:.1f} s)", True)
    built(cl, c2, w, "k", "ku")
    subprocess.run([str(ROOT / "scripts/cluster.sh"), "restart", "-d", str(cl.dir), "tarn1"], check=True,
                   stdin=subprocess.DEVNULL)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tarn", required=True, help="SQL servers, comma separated; with two, the takeover too")
    ap.add_argument("--cluster-dir", required=True)
    ap.add_argument("--rows", type=int, default=20000)
    args = ap.parse_args()
    endpoints = args.tarn.split(",")
    cl = Cluster(args.cluster_dir)
    c = connect(endpoints[0], db=None)
    q(c, f"DROP DATABASE IF EXISTS {DB}")
    q(c, f"CREATE DATABASE {DB}")
    c.select_db(DB)
    limits = {node: ev(node, "local o = tarn.cfg.alter_check_rows; tarn.cfg.alter_check_rows = ...; return o",
                       [LIMIT]) for node in cl.nodes}
    try:
        online(cl, c, endpoints[0], args.rows)
        duplicates_before(cl, c)
        duplicate_during(cl, c, endpoints[0], args.rows)
        resolved_during(cl, c, endpoints[0], args.rows)
        if len(endpoints) > 1:
            taken_over(cl, endpoints, args.rows)
    finally:
        for node, v in limits.items():
            ev(node, "tarn.cfg.alter_check_rows = ...", [v])
    logs = [line for log in sorted(cl.dir.glob("s*.log")) for line in log.read_text(errors="replace").splitlines()]
    # (Every SQL server here maintains building indexes: no shard refused
    # one's write for not saying so, which it would send again another way.)
    refused = [line for line in logs if "does not maintain building indexes" in line]
    check("no shard refused a write as from a server that does not maintain building indexes", not refused,
          refused[:3])
    # (with TARN_CHECK_YIELDS and TARN_CHECK_LOCKS, as tests/uniquebuild.sh starts the shards)
    bad = [line for line in logs if "yielded inside" in line or "lock copy differs" in line]
    check("no atomic step yielded, and the native module's copy of the lock index held what it held", not bad,
          bad[:3])
    q(connect(endpoints[0], db=None), f"DROP DATABASE {DB}")
    print("all ok" if fails == 0 else f"{fails} failed")
    raise SystemExit(1 if fails else 0)


if __name__ == "__main__":
    main()
