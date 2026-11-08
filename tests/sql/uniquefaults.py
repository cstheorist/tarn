#!/usr/bin/env python3
"""An online build of a global unique index on 2 shards x 3 replicas with
fsync, through faults (#292). Run by tests/uniquefaults.sh, on memtx and on
vinyl, with test hooks.

  * A leader crash during the copy: on replicated shards the step that
    makes the index enforcing copies the table. Its copy is held after its
    first page on whichever replica of the second shard leads (test hook
    engine_copy), and that leader then ends itself. The shard's new leader
    resumes the copy from its durable capture, the SQL server sends the
    step again, and the build finishes while a session writes. The replica
    that crashed is started again.
  * A lost reply during the backfill: the first of the backfill's
    transactions to PREPARE on a shard's leader votes yes there, durably,
    and the reply is lost (a wrapper of tarn_prepare raises once the vote
    is in). The transaction's outcome is unknown to the SQL server, which
    resolves it as recovery does; the backfill goes on, and the build
    finishes while a session writes.

After each: the ALTER succeeded; the table's space was replaced by the copy
on every shard; the index holds one entry per row with a value, the row's,
on every replica of the entry's shard, and no conflicts space is left; no
copy is left on any replica; the key finds rows by value and refuses a
duplicate in another tenant. The checks read the table as it is: a write
that a crash left with an unknown outcome is not replayed.

  * A leader crash during the backfill: once the job is in its backfill,
    the second shard's leader ends itself. The backfill's pages there fail
    for a transient reason until the shard's new leader takes them, and the
    build finishes while a session writes. The replica that crashed is
    started again, and the index is checked as above.
  * A duplicate written during the backfill: once the job is in its
    backfill, a row of the second shard takes the value of a row of the
    first. The ALTER fails with 1062, naming the value, and leaves the
    table as it was: its space on every shard, no index or conflicts space,
    no copy, both rows with the value.

    uniquefaults.py --tarn HOST:PORT --cluster-dir DIR --engine memtx|vinyl
"""
import argparse
import json
import os
from pathlib import Path
import random
import subprocess
import threading
import time

import pymysql

from lazytest import ev, rpc

DB = "unique_faults"
TENANTS = 64
FRESH = 10**6
ROOT = Path(__file__).resolve().parents[2]
ELECTION = ("local e = box.info.election; "
            "return {ro = box.info.ro, state = e.state, term = e.term, leader = e.leader}")
# Writes that a leader crash or a lost reply may leave refused or with an
# unknown outcome; the session goes on, as a client would.
FAULTS = (1180, 1205, 1213, 1412, 2003, 2006, 2013)

fails = 0


def check(name, cond, detail=None):
    global fails
    if cond:
        print(f"  ok   {name}", flush=True)
    else:
        fails += 1
        print(f"  FAIL {name}" + (f"  -- {json.dumps(detail, default=str)}" if detail is not None else ""),
              flush=True)


def connect(endpoint, db=DB, read_timeout=300):
    host, port = endpoint.rsplit(":", 1)
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db, autocommit=True,
                           charset="utf8mb4", connect_timeout=3, read_timeout=read_timeout, write_timeout=10)


def q(c, sql, args=None):
    """Run a statement, sent again while vinyl admission throttles it (1205,
    before any effect), for up to a minute."""
    deadline = time.monotonic() + 60
    while True:
        try:
            with c.cursor() as cur:
                cur.execute(sql, args)
                return cur.fetchall() if cur.description else cur.rowcount
        except pymysql.MySQLError as e:
            if not (e.args[0] == 1205 and "Vinyl" in str(e.args[1])) or time.monotonic() > deadline:
                raise
            time.sleep(0.1)


# A one-shot wrapper of this node's tarn_prepare, for table space `sid`: the
# first transaction that writes an entry of the table's global index (a
# space '_tarn_uix_<sid>_<index id>') and votes yes here loses its reply.
# It restores itself then, or after `seconds`.
LOSE_REPLY = r"""
local sid, seconds = ...
assert(tarn.cfg.test_hooks == true, 'uniquefaults requires test hooks')
assert(rawget(_G, '__tarn_uniquefaults') == nil, 'another uniquefaults wrapper is installed')
local clock = require('clock')
local s = {original = rawget(_G, 'tarn_prepare'), lost = 0, txid = box.NULL,
           expires = clock.monotonic() + seconds}
local tag = '^_tarn_uix_' .. sid .. '_%d+$'
function s.restore()
    if rawget(_G, 'tarn_prepare') == s.wrapped then rawset(_G, 'tarn_prepare', s.original) end
end
s.wrapped = function(txid, dshard, parts, writes, ...)
    if clock.monotonic() > s.expires then s.restore() end
    local mine = false
    for _, w in ipairs(s.lost == 0 and writes or {}) do
        local sp = type(w) == 'table' and box.space[w[2]]
        if sp and sp.name:match(tag) then mine = true; break end
    end
    local r = s.original(txid, dshard, parts, writes, ...)
    if mine and type(r) == 'table' and r[1] == 'yes' then
        s.lost, s.txid = s.lost + 1, txid
        s.restore()
        error('uniquefaults injected a lost PREPARE reply after a durable yes')
    end
    return r
end
rawset(_G, '__tarn_uniquefaults', s)
rawset(_G, 'tarn_prepare', s.wrapped)
return true
"""
LOST = r"""
local s = rawget(_G, '__tarn_uniquefaults')
if s == nil then return {lost = 0} end
s.restore()
rawset(_G, '__tarn_uniquefaults', nil)
return {lost = s.lost, txid = s.txid}
"""


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tarn", required=True)
    ap.add_argument("--cluster-dir", required=True)
    ap.add_argument("--engine", choices=("memtx", "vinyl"), required=True)
    ap.add_argument("--rows", type=int, default=6000)
    args = ap.parse_args()
    work = Path(args.cluster_dir)
    groups = json.loads((work / "cluster.json").read_text())["shards"]
    nodes = [n for g in groups for n in g["replicas"]]
    assert len(groups) == 2 and all(len(g["replicas"]) == 3 for g in groups), groups

    def leader(group, exclude=None):
        for node in group["replicas"]:
            if node == exclude:
                continue
            try:
                if not ev(node, "return box.info.ro"):
                    return node
            except Exception:  # a replica that is down
                continue
        return None

    def wait_leader(group, exclude=None, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            found = leader(group, exclude)
            if found:
                return found
            time.sleep(0.1)
        raise AssertionError(f"shard {group['id']} has no leader")

    def settle(seconds=3, timeout=90):
        """Every replica answers and agrees on its shard's leader and term,
        for `seconds` in a row."""
        deadline, since, last = time.monotonic() + timeout, None, None
        while time.monotonic() < deadline:
            view = []
            for g in groups:
                seen = set()
                for node in g["replicas"]:
                    try:
                        e = ev(node, ELECTION)
                        seen.add((e["leader"], e["term"]))
                    except Exception:  # down or starting
                        seen.add(None)
                view.append(sorted(seen, key=repr))
            agreed = all(len(v) == 1 and v[0] is not None and v[0][0] != 0 for v in view)
            now = time.monotonic()
            if not agreed or view != last:
                since = now if agreed else None
            elif now - since >= seconds:
                return view
            last = view
            time.sleep(0.2)
        raise AssertionError(f"the shards did not settle on their leaders: {last}")

    def hook(targets, action):
        done = []
        for node in targets:
            try:
                r = rpc(node, "tarn_test_hook", ["engine_copy", action])
                assert r[0] == "ok", r
                done.append(node)
            except RuntimeError:  # a replica that is down
                pass
        return done

    def holder(targets, deadline):
        """The replica whose copy reached the hook, if one does in time."""
        found = []

        def poll(node):
            while not found and time.monotonic() < deadline:
                try:
                    # (waits up to 5 s on the node for a step to reach it)
                    if rpc(node, "tarn_test_hook", ["engine_copy", "reached"])[1] > 0:
                        found.append(node)
                except Exception:  # timed out, or the replica is down
                    time.sleep(0.2)
        pollers = [threading.Thread(target=poll, args=(n,)) for n in targets]
        for p in pollers:
            p.start()
        for p in pollers:
            p.join()
        return found[0] if found else None

    def space_ids(space):
        return {g["id"]: ev(wait_leader(g), "return box.space[...].id", [space]) for g in groups}

    def entries(space):
        """Each replica's global index spaces of the table, by name, with
        their entries (key, then the primary key's columns)."""
        out = {}
        for node in nodes:
            r = ev(node, """
                require('fiber').set_slice({warn = 30, err = 120})
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
            """, [space], timeout=60)
            out[node] = r if isinstance(r, dict) else {}   # (an empty table encodes as [])
        return out

    def copies_left():
        out = {}
        for node in nodes:
            names = ev(node, """
                local out = {}
                for _, s in box.space._space:pairs() do
                    if s.name:find('^_tarn_engine_') then out[#out + 1] = s.name end
                end
                return out
            """)
            if names:
                out[node] = names
        return out

    def caught_up(timeout=60):
        """Every replica has applied what its shard's leader wrote."""
        for g in groups:
            lead = wait_leader(g)
            want = ev(lead, "return {id = box.info.id, lsn = box.info.lsn}")
            for node in g["replicas"]:
                deadline = time.monotonic() + timeout
                while ev(node, "return box.info.vclock[...] or 0", [want["id"]]) < want["lsn"]:
                    if time.monotonic() > deadline:
                        raise AssertionError(f"{node} did not catch up with {lead}")
                    time.sleep(0.1)

    c = connect(args.tarn, db=None)
    q(c, f"DROP DATABASE IF EXISTS {DB}")
    q(c, f"CREATE DATABASE {DB}")
    c.select_db(DB)

    def populate(name):
        q(c, f"CREATE TABLE {name} (tenant_id INT, id INT, u INT, k INT NOT NULL, PRIMARY KEY (tenant_id, id)) "
             f"ENGINE={args.engine} PARTITION BY KEY(tenant_id)")
        for first in range(0, args.rows, 500):
            q(c, f"INSERT INTO {name} VALUES " + ",".join(
                f"({i % TENANTS},{i},{i},0)" for i in range(first, min(args.rows, first + 500))))

    class Writer:
        """A session writing to table `name` while the change runs: it adds
        1 to k, and with `keys` also inserts and deletes rows, gives a row a
        new value and hands a row's value to another, keeping the values
        unique. A write a fault refused, or left with an unknown outcome,
        is counted, and the session goes on."""

        def __init__(self, name, keys, seed):
            self.name, self.keys, self.rnd = name, keys, random.Random(seed)
            self.writes, self.faults, self.errors = 0, 0, []
            self.fresh = FRESH
            self.stop = threading.Event()
            self.thread = threading.Thread(target=self.run)

        def new(self):
            self.fresh += 1
            return self.fresh

        def run(self):
            conn = connect(args.tarn, read_timeout=60)
            t = self.name
            while not self.stop.is_set():
                op, i = self.rnd.random(), self.rnd.randrange(args.rows)
                try:
                    if not self.keys or op < 0.4:
                        q(conn, f"UPDATE {t} SET k = k + 1 WHERE tenant_id = %s AND id = %s", (i % TENANTS, i))
                    elif op < 0.6:
                        j = self.new()
                        q(conn, f"INSERT INTO {t} VALUES (%s, %s, %s, 0)", (j % TENANTS, j, self.new()))
                    elif op < 0.75:
                        q(conn, f"DELETE FROM {t} WHERE tenant_id = %s AND id = %s", (i % TENANTS, i))
                    else:
                        # (a row's value moves to another row, the first
                        # taking a fresh one first)
                        j = self.rnd.randrange(args.rows)
                        got = q(conn, f"SELECT u FROM {t} WHERE tenant_id = %s AND id = %s", (i % TENANTS, i))
                        if got and got[0][0] is not None:
                            q(conn, f"UPDATE {t} SET u = %s WHERE tenant_id = %s AND id = %s",
                              (self.new(), i % TENANTS, i))
                            q(conn, f"UPDATE {t} SET u = %s WHERE tenant_id = %s AND id = %s",
                              (got[0][0], j % TENANTS, j))
                    self.writes += 1
                except pymysql.MySQLError as e:
                    if e.args[0] in FAULTS:
                        self.faults += 1
                    else:
                        self.errors.append(list(e.args))
                    time.sleep(0.05)
                    if e.args[0] in (2006, 2013):
                        try:
                            conn = connect(args.tarn, read_timeout=60)
                        except pymysql.MySQLError:
                            time.sleep(0.5)
                time.sleep(0.005)

        def __enter__(self):
            self.thread.start()
            return self

        def __exit__(self, *exc):
            self.stop.set()
            self.thread.join(60)

    def built(name, key, before):
        """The checks after a change that added unique key `key` on u."""
        caught_up()
        space = f"{DB}.{name}"
        after = space_ids(space)
        rows = {(tn, i): u for tn, i, u in q(c, f"SELECT tenant_id, id, u FROM {name}")}
        want = {u: [tn, i] for (tn, i), u in rows.items() if u is not None}
        spaces = entries(space)
        names = sorted({s for node in nodes for s in spaces[node]})
        got, agree = {}, True
        for g in groups:
            first = spaces[g["replicas"][0]]
            agree = agree and all(spaces[n] == first for n in g["replicas"])
            for u, tn, i in (first.get(names[0], []) if len(names) == 1 else []):
                got[u] = [tn, i]
        check("the table's space was replaced by the copy on every shard, and no copy is left on any replica",
              all(after[s] != before[s] for s in after) and not copies_left(),
              {"before": before, "after": after, "left": copies_left()})
        check("the index has one space, no conflicts space is left, and every replica of a shard holds the "
              "same entries", len(names) == 1 and not names[0].endswith("c") and agree, names)
        check(f"it holds one entry per row with a value, the row's ({len(want)})", got == want,
              {"missing": len(set(want) - set(got)), "extra": len(set(got) - set(want)),
               "wrong": sum(1 for u in want if u in got and got[u] != want[u])})
        sample = random.Random(name).sample(sorted(want), min(300, len(want)))
        lost = [u for u in sample if [list(r) for r in q(c, f"SELECT tenant_id, id FROM {name} WHERE u = %s",
                                                           (u,))] != [want[u]]]
        dup = None
        try:
            u0 = sample[0]
            q(c, f"INSERT INTO {name} VALUES (%s, %s, %s, 0)", ((want[u0][0] + 1) % TENANTS, 9 * FRESH, u0))
        except pymysql.MySQLError as e:
            dup = e.args[0]
        check("the key finds each row by its value (a sample), and refuses a duplicate in another tenant",
              not lost and dup == 1062, {"lost": lost[:5], "dup": dup})
        shown = q(c, f"SHOW CREATE TABLE {name}")[0][1]
        check("SHOW CREATE TABLE shows the key", f"UNIQUE KEY `{key}` (`u`)" in shown, shown)

    print(f"{args.engine}: settled on {settle()}", flush=True)

    # 1. A leader crash during the copy.
    print(f"{args.engine}: a leader crash during the copy", flush=True)
    populate("t")
    caught_up()
    before = space_ids(f"{DB}.t")
    second = groups[1]["replicas"]
    alter = {}

    def run_alter(name, key):
        try:
            q(connect(args.tarn), f"ALTER TABLE {name} ADD UNIQUE KEY {key} (u)")
            alter["ok"] = True
        except pymysql.MySQLError as e:
            alter["error"] = list(e.args)

    crashed = None
    with Writer("t", True, f"{args.engine} crash") as w:
        time.sleep(0.5)
        armed = hook(second, "hold")
        th = threading.Thread(target=run_alter, args=("t", "tu"))
        th.start()
        victim = holder(armed, time.monotonic() + 180)
        check("the copy reaches the hook on the second shard's leader", victim is not None, alter)
        if victim is not None:
            state = ev(victim, "return {pid = box.info.pid, cwd = require('fio').cwd(), ro = box.info.ro}")
            name = Path(state["cwd"]).name
            assert int((work / f"{name}.pid").read_text()) == state["pid"], state
            before_crash = w.writes
            ev(victim, "local pid = ...; assert(box.info.pid == pid); "
                       "require('fiber').create(function() require('fiber').sleep(.05); "
                       "local ffi = require('ffi'); ffi.cdef('void _exit(int);'); ffi.C._exit(86) end)",
               [state["pid"]])
            crashed = name
            hook([n for n in armed if n != victim], "release")
            replacement = wait_leader(groups[1], exclude=victim)
            print(f"  ({args.engine}: {victim} held the copy and ended itself; {replacement} leads now)",
                  flush=True)
        else:
            hook(armed, "release")
        th.join(300)
        time.sleep(1)
    check("the ALTER succeeds once the shard's new leader resumes the copy", alter.get("ok") is True
          and not th.is_alive(), alter)
    check(f"the writes went on through the crash ({w.writes}, {w.faults} refused or unknown), "
          f"none failing otherwise", w.writes > 0 and not w.errors and (victim is None or w.writes > before_crash),
          w.errors[:5])
    if crashed is not None:
        subprocess.run([str(ROOT / "scripts/cluster.sh"), "restart", "-d", str(work), crashed], check=True,
                       stdin=subprocess.DEVNULL)
    settle()
    built("t", "tu", before)

    # 2. A lost reply during the backfill.
    print(f"{args.engine}: a lost reply during the backfill", flush=True)
    populate("r")
    caught_up()
    before = space_ids(f"{DB}.r")
    sid = before[groups[0]["id"]]
    leaders = [wait_leader(g) for g in groups]
    for node in leaders:
        ev(node, LOSE_REPLY, [sid, 300])
    alter.clear()
    with Writer("r", False, f"{args.engine} lost") as w:
        time.sleep(0.5)
        run_alter("r", "ru")
        time.sleep(1)
    lost = [ev(node, LOST) for node in leaders]
    check("a backfill transaction's PREPARE lost its reply after a durable yes",
          sum(x["lost"] for x in lost) >= 1, lost)
    check("the ALTER succeeds", alter.get("ok") is True, alter)
    check(f"the writes went on ({w.writes}, {w.faults} refused or unknown), none failing otherwise",
          w.writes > 0 and not w.errors, w.errors[:5])
    built("r", "ru", before)

    # 3. A duplicate written during the backfill, by a row of the other shard.
    print(f"{args.engine}: a duplicate written during the backfill, on the other shard", flush=True)
    populate("d")
    caught_up()
    space = f"{DB}.d"
    before = space_ids(space)
    home = {}
    for gi, g in enumerate(groups):
        for tn in ev(wait_leader(g), """
            local sp, out = box.space[...], {}
            for tn = 0, 63 do
                if sp.index[0]:select({tn}, {limit = 1})[1] ~= nil then out[#out + 1] = tn end
            end
            return out
        """, [space]):
            home[tn] = gi
    a = next(i for i in range(args.rows) if home.get(i % TENANTS) == 0)
    other = next(x for x in range(TENANTS) if home.get(x) == 1)
    inserted = {}

    def duplicate():
        """Once the job is in its backfill, a row of the second shard takes
        the value of row `a`, of the first."""
        deadline, phase = time.monotonic() + 120, None
        while time.monotonic() < deadline and phase != "backfill":
            phase = ev(wait_leader(groups[0]), "local t = box.space._tarn_meta:get{'job:' .. ...} "
                                               "return t and t.value.phase or box.NULL", [space])
            time.sleep(0.02)
        inserted["phase"] = phase
        try:
            q(connect(args.tarn), "INSERT INTO d VALUES (%s, %s, %s, 0)", (other, 8 * FRESH, a))
            inserted["ok"] = True
        except pymysql.MySQLError as e:
            inserted["error"] = list(e.args)

    alter.clear()
    with Writer("d", False, f"{args.engine} duplicate") as w:
        time.sleep(0.5)
        th = threading.Thread(target=duplicate)
        th.start()
        run_alter("d", "du")
        th.join(130)
        time.sleep(1)
    check("a row of the second shard took a value of the first's during the backfill",
          inserted.get("ok") is True and inserted.get("phase") == "backfill", inserted)
    check("the ALTER fails with 1062, naming the value", alter.get("error") == [1062,
          f"Duplicate entry '{a}' for key 'du'"], alter)
    caught_up()
    after, spaces = space_ids(space), entries(space)
    check("it leaves the table as it was: its space on every shard, with no index or conflicts space, and no "
          "copy on any replica", after == before and not any(spaces.values()) and not copies_left(),
          {"before": before, "after": after, "spaces": {n: list(s) for n, s in spaces.items() if s},
           "left": copies_left()})
    check(f"the writes went on ({w.writes}, {w.faults} refused or unknown), none failing otherwise",
          w.writes > 0 and not w.errors, w.errors[:5])
    shown = q(c, "SHOW CREATE TABLE d")[0][1]
    both = sorted(tuple(r) for r in q(c, "SELECT tenant_id, id FROM d WHERE u = %s", (a,)))
    check("the two rows keep the value, and SHOW CREATE TABLE shows no such key",
          both == sorted([(a % TENANTS, a), (other, 8 * FRESH)]) and "`du`" not in shown, {"rows": both})

    # 4. A leader crash during the backfill.
    print(f"{args.engine}: a leader crash during the backfill", flush=True)
    populate("b")
    caught_up()
    space = f"{DB}.b"
    before = space_ids(space)
    crash = {}

    def crash_in_backfill():
        """Once the job is in its backfill, the second shard's leader ends
        itself; the backfill's pages there fail until a new leader takes
        them."""
        deadline, phase = time.monotonic() + 120, None
        while time.monotonic() < deadline and phase != "backfill":
            phase = ev(wait_leader(groups[0]), "local t = box.space._tarn_meta:get{'job:' .. ...} "
                                               "return t and t.value.phase or box.NULL", [space])
            time.sleep(0.02)
        crash["phase"] = phase
        if phase != "backfill":
            return
        victim = wait_leader(groups[1])
        state = ev(victim, "return {pid = box.info.pid, cwd = require('fio').cwd()}")
        assert int((work / f"{Path(state['cwd']).name}.pid").read_text()) == state["pid"], state
        ev(victim, "local pid = ...; assert(box.info.pid == pid); "
                   "require('fiber').create(function() require('fiber').sleep(.05); "
                   "local ffi = require('ffi'); ffi.cdef('void _exit(int);'); ffi.C._exit(86) end)",
           [state["pid"]])
        crash["victim"] = Path(state["cwd"]).name
        crash["replacement"] = wait_leader(groups[1], exclude=victim)
        # (still in the backfill once the shard had its new leader)
        crash["phase_after"] = ev(wait_leader(groups[0]), "local t = box.space._tarn_meta:get{'job:' .. ...} "
                                                          "return t and t.value.phase or box.NULL", [space])

    alter.clear()
    with Writer("b", True, f"{args.engine} backfill crash") as w:
        time.sleep(0.5)
        th = threading.Thread(target=crash_in_backfill)
        th.start()
        run_alter("b", "bu")
        th.join(130)
        time.sleep(1)
    print(f"  ({args.engine}: {crash.get('victim')} ended itself in the backfill; "
          f"{crash.get('replacement')} leads now)", flush=True)
    check("the second shard's leader ended itself during the backfill, which went on once another took over",
          crash.get("phase") == "backfill" and crash.get("replacement") is not None
          and crash.get("phase_after") == "backfill", crash)
    check("the ALTER succeeds, the backfill going on with the shard's new leader", alter.get("ok") is True, alter)
    check(f"the writes went on through the crash ({w.writes}, {w.faults} refused or unknown), "
          f"none failing otherwise", w.writes > 0 and not w.errors, w.errors[:5])
    if crash.get("victim"):
        subprocess.run([str(ROOT / "scripts/cluster.sh"), "restart", "-d", str(work), crash["victim"]], check=True,
                       stdin=subprocess.DEVNULL)
    settle()
    built("b", "bu", before)

    q(connect(args.tarn, db=None), f"DROP DATABASE {DB}")
    print("all ok" if fails == 0 else f"{fails} failed", flush=True)
    raise SystemExit(1 if fails else 0)


if __name__ == "__main__":
    main()
