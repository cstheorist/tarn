#!/usr/bin/env python3
"""An online lazy-index build across an owned RF3 leader crash (#184).

ALTER TABLE ... ADD INDEX ... COMMENT 'tarn:lazy' runs while a writer
changes rows. Its baseline scan is held between a batch's reads and its
write on the second shard's leader, which then terminates itself. A new
leader continues the scan from the durable cursor and captures the writes;
once the ALTER returns, the lazy index answers as the strong path does.
Writes the crash left with an unknown outcome are not replayed: the check
compares with the table as it is.
"""
import argparse
import json
import os
from pathlib import Path
import threading
import time

import pymysql

from lazytest import ev, q, rpc, stat


def connect(endpoint, db=None, read_timeout=120):
    host, port = endpoint.rsplit(":", 1)
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db, autocommit=True,
                           charset="utf8mb4", connect_timeout=3, read_timeout=read_timeout, write_timeout=5)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tarn", required=True)
    ap.add_argument("--cluster-dir", required=True)
    ap.add_argument("--engine", choices=("memtx", "vinyl"), required=True)
    args = ap.parse_args()
    work = Path(args.cluster_dir)
    groups = json.loads((work / "cluster.json").read_text())["shards"]
    assert len(groups) == 2 and all(len(s["replicas"]) == 3 for s in groups)

    def record(event, **values):
        line = json.dumps({"event": event, **values})
        print(line, flush=True)
        with (work / "online-events.jsonl").open("a") as f:
            f.write(line + "\n")

    def leader(group, exclude=None):
        for node in group["replicas"]:
            if node == exclude:
                continue
            try:
                state = ev(node, "return {pid=box.info.pid,cwd=require('fio').cwd(),ro=box.info.ro,"
                                 "wal=box.cfg.wal_mode,sync=tarn.cfg.sync}")
            except Exception:  # a replica that is down
                continue
            if not state["ro"]:
                assert state["sync"] and state["wal"] == "fsync", state
                return node, state
        return None

    def wait_leader(group, exclude=None):
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            found = leader(group, exclude)
            if found:
                return found
            time.sleep(.05)
        raise AssertionError(f"no writable leader: group={group['id']} excluded={exclude}")

    ELECTION = ("local e,q=box.info.election,box.info.synchro.queue;"
                "return {ro=box.info.ro,state=e.state,term=e.term,leader=e.leader,owner=q.owner,len=q.len}")

    def alive(node):
        # Each replica's pid file sits in the cluster directory, named for
        # its working directory (sI.J), whose port ends in IJ.
        port = node.rsplit(":", 1)[1]
        pid = work / f"s{port[-2]}.{port[-1]}.pid"
        try:
            os.kill(int(pid.read_text()), 0)
            return True
        except (OSError, ValueError):
            return False

    def cohort():
        """Each replica's process and election state, for a failure message."""
        lines = []
        for g in groups:
            for node in g["replicas"]:
                try:
                    lines.append(f"{node} {json.dumps(ev(node, ELECTION), sort_keys=True)}")
                except Exception as e:  # down or not answering
                    lines.append(f"{node} alive={alive(node)} {type(e).__name__}: {str(e)[:120]}")
        return lines

    def fail(message, detail):
        # Lines with FAIL reach the CI annotations (ci/annotate.sh).
        for line in cohort():
            print("FAIL lazy online cohort:", line, flush=True)
        raise AssertionError((message, detail))

    def settle(seconds=3, timeout=60):
        """Every replica answers and agrees on its shard's leader and term,
        for `seconds` in a row. A restarted former leader rejoins its shard
        and can start an election, which the ALTER must not run into."""
        deadline, since, last = time.monotonic() + timeout, None, None
        while time.monotonic() < deadline:
            view = []
            for g in groups:
                seen = set()
                for node in g["replicas"]:
                    try:
                        e = ev(node, ELECTION)
                        seen.add((e["leader"], e["term"]))
                    except Exception:
                        seen.add(None)
                view.append(sorted(seen, key=repr))
            agreed = all(len(v) == 1 and v[0] is not None and v[0][0] != 0 for v in view)
            now = time.monotonic()
            if not agreed or view != last:
                since = now if agreed else None
            elif now - since >= seconds:
                return view
            last = view
            time.sleep(.2)
        fail("the shards did not settle on their leaders", last)

    # Lazy DDL checks every replica's capabilities, so every replica must
    # answer, including a leader an earlier test crashed and restarted.
    deadline = time.monotonic() + 60
    for node in (n for g in groups for n in g["replicas"]):
        while True:
            try:
                reply = rpc(node, "tarn_lazy_capabilities", [])
                if reply[0] == "ok":
                    break
            except Exception as e:  # still starting
                reply = repr(e)
            if time.monotonic() >= deadline:
                raise AssertionError(f"replica {node} did not answer: {reply}")
            time.sleep(.2)
    # A shard whose election finds no leader for 8 s has its most advanced
    # replica promoted, as an operator would: a replica that refuses the
    # others' votes because its vclock is ahead of theirs does not stand
    # itself while their elections keep bumping the term (#254). The
    # online build is what this test checks, not that election.
    crashed, healed, healer_stop = None, [], threading.Event()

    def healer():
        leaderless = {}
        while not healer_stop.wait(.5):
            for g in groups:
                states = {}
                for node in g["replicas"]:
                    if node == crashed:
                        continue
                    try:
                        states[node] = ev(node, "return {leader=box.info.election.leader,vclock=box.info.vclock}")
                    except Exception:  # down or not answering
                        pass
                if not states or any(st["leader"] != 0 for st in states.values()):
                    leaderless.pop(g["id"], None)
                    continue
                since = leaderless.setdefault(g["id"], time.monotonic())
                if time.monotonic() - since < 8:
                    continue
                clocks = {n: {int(k): v for k, v in st["vclock"].items()} for n, st in states.items()}

                def ahead(n):
                    return all(clocks[n].get(i, 0) >= lsn for m in clocks for i, lsn in clocks[m].items())
                best = max(clocks, key=lambda n: (ahead(n), sum(clocks[n].values())))
                try:
                    ev(best, "require('fiber').create(function() pcall(box.ctl.promote) end)")
                except Exception as e:  # it may have gone down meanwhile
                    record("heal_failed", shard=g["id"], replica=best, error=repr(e))
                    continue
                healed.append({"shard": g["id"], "replica": best, "after": round(time.monotonic() - since, 1)})
                record("healed", **healed[-1], vclocks=clocks)
                print(f"WARN shard {g['id']} found no leader for 8 s; promoted {best}", flush=True)
                leaderless.pop(g["id"], None)

    threading.Thread(target=healer, daemon=True).start()
    record("settled", leaders=settle())
    # Shard 2: the policy fault test before this one crashed shard 1's leader.
    first, _ = wait_leader(groups[0])
    victim, state = wait_leader(groups[1])
    assert Path(state["cwd"]).parent.name == work.name
    owned_pid = int((work / (Path(state["cwd"]).name + ".pid")).read_text())
    assert owned_pid == state["pid"]
    c = connect(args.tarn)
    q(c, "DROP DATABASE IF EXISTS lazy_online_fault")
    q(c, "CREATE DATABASE lazy_online_fault")
    q(c, "USE lazy_online_fault")
    q(c, "CREATE TABLE t (tenant_id INT NOT NULL,id INT NOT NULL,k INT,n INT,PRIMARY KEY(tenant_id,id)) "
         f"PARTITION BY KEY(tenant_id) ENGINE={args.engine}")
    rows = [(i % 17, i, i % 7, i) for i in range(1, 3001)]
    with c.cursor() as cur:
        for start in range(0, len(rows), 250):
            for attempt in range(40):
                try:
                    cur.executemany("INSERT INTO t VALUES (%s,%s,%s,%s)", rows[start:start + 250])
                    break
                except pymysql.MySQLError as e:  # vinyl admission, before any effect
                    if e.args[0] != 1205 or "Vinyl" not in str(e.args[1]) or attempt == 39:
                        raise
                    time.sleep(min(0.5, 0.05 * (attempt + 1)))

    stop, writes, errors = threading.Event(), [0], []

    def writer():
        with connect(args.tarn, "lazy_online_fault", read_timeout=30) as w:
            i = 0
            while not stop.is_set():
                i += 1
                rid = i * 37 % 3000 + 1
                try:
                    if i % 3 == 0:
                        q(w, "UPDATE t SET k=%s,n=n+1 WHERE tenant_id=%s AND id=%s", (i % 9, rid % 17, rid))
                    elif i % 3 == 1:
                        q(w, "INSERT INTO t VALUES (%s,%s,%s,%s)", (i % 17, 100000 + i, i % 7, i))
                    else:
                        q(w, "DELETE FROM t WHERE tenant_id=%s AND id=%s", (rid % 17, rid))
                    writes[0] += 1
                except pymysql.MySQLError as e:  # the crash: 2003, 2013, 1180, ...
                    errors.append(list(e.args))
                    time.sleep(.05)

    alter = {}

    def run_alter():
        try:
            with connect(args.tarn, "lazy_online_fault") as a:
                q(a, "ALTER TABLE t ADD INDEX lk(k,n,id) COMMENT 'tarn:lazy'")
            alter["ok"] = True
        except pymysql.MySQLError as e:
            alter["error"] = list(e.args)

    # A leader change in another step of the ALTER (BUILD, PREPARE) fails
    # it with 2003 or 1213 for the client to retry, as documented; its job
    # aborts, which retires the incarnation. Such an attempt before the
    # scan reached the hook is retried, with the hook on shard 2's leader
    # of the time.
    TRANSIENT = (1213, 2003, 2013)
    threads = [threading.Thread(target=writer)]
    retried = []
    try:
        threads[0].start()
        time.sleep(.3)
        for attempt in range(4):
            if attempt:
                settle()
                victim, state = wait_leader(groups[1])
                assert Path(state["cwd"]).parent.name == work.name
                owned_pid = int((work / (Path(state["cwd"]).name + ".pid")).read_text())
                assert owned_pid == state["pid"]
            r = rpc(victim, "tarn_test_hook", ["lazy_bootstrap", "hold"])
            assert r[0] == "ok", r
            alter.clear()
            threads.append(threading.Thread(target=run_alter))
            threads[-1].start()
            deadline = time.monotonic() + 60
            reached = False
            while time.monotonic() < deadline and "error" not in alter:
                try:
                    # (waits up to 5 s on the node, as long as the call may take)
                    if rpc(victim, "tarn_test_hook", ["lazy_bootstrap", "reached"])[1] > 0:
                        reached = True
                        break
                except RuntimeError as e:
                    if "timed out" not in str(e):
                        raise
            if reached:
                break
            # (1105 "storage error 217": a step a new leader rolled back.)
            transient = "error" in alter and (alter["error"][0] in TRANSIENT or alter["error"][0] == 1105
                                              and str(alter["error"][1]).startswith("storage error 217:"))
            if not transient or attempt == 3:
                fail("the scan never reached the hook", {"alter": alter, "retried": retried})
            retried.append(alter["error"])
            record("alter_retried", error=alter["error"])
            try:
                rpc(victim, "tarn_test_hook", ["lazy_bootstrap", "release"])
            except Exception:  # the old leader may be down
                pass
            threads[-1].join(timeout=30)
        threads[1:] = [threads[-1]]
        before = writes[0]
        building = ev(victim, "return tarn.lazy.building()")
        assert building, "the scan is held, yet no online build is recorded"
        record("scan_held", leader=victim, writes=before, gen=building)
        receipt = ev(victim, "local pid,cwd=...;assert(tarn.cfg.test_hooks and not box.info.ro);"
                     "assert(box.info.pid==pid and require('fio').cwd()==cwd);"
                     "require('fiber').create(function() require('fiber').sleep(.05);"
                     "local ffi=require('ffi');ffi.cdef('void _exit(int);');ffi.C._exit(86) end);"
                     "return {pid=box.info.pid}", [owned_pid, state["cwd"]])
        crashed = victim
        (work / (Path(state["cwd"]).name + ".killed")).write_text(str(owned_pid))
        record("owned_crash_scheduled", receipt=receipt)
        replacement, _ = wait_leader(groups[1], exclude=crashed)
        record("replacement", leader=replacement)
        threads[1].join(timeout=150)
        if threads[1].is_alive():
            fail("the ALTER did not return", alter)
        if not alter.get("ok"):
            fail("the ALTER failed", alter)
        after = writes[0]
        time.sleep(.3)
    finally:
        stop.set()
        for t in threads:
            if t.is_alive():
                t.join(timeout=30)
    if any(t.is_alive() for t in threads):
        fail("the writer or the ALTER did not stop", {"alter": alter, "writes": writes[0]})
    assert writes[0] > before, "no write succeeded after the crash"

    def drained():
        """The shards' current leaders, once their outboxes are empty: found
        again on each try, since a leader may have changed during the build,
        and a follower's copy of an outbox can be empty while its leader
        still ships."""
        deadline = time.monotonic() + 30
        while True:
            leaders = [wait_leader(g, exclude=crashed)[0] for g in groups]
            samples = [rpc(n, "tarn_lazy_stats", []) for n in leaders]
            if all(r[0] == "ok" and r[1]["depth"] == 0 and r[1]["inflight"] == 0 and r[1]["reserved_entries"] == 0
                   for r in samples) and [wait_leader(g, exclude=crashed)[0] for g in groups] == leaders:
                return leaders
            if time.monotonic() >= deadline:
                fail("the leaders' outboxes did not drain", samples)
            time.sleep(.05)

    def compare(k):
        """A strong read on each side of the lazy one: None when the two
        strong reads differ, a write the crash left in flight still landing."""
        sql = f"SELECT tenant_id,id,n FROM t WHERE k={k} ORDER BY n,id"
        q(c, "SET tarn_lazy_consistency='strong'")
        first = q(c, sql)
        q(c, "SET tarn_lazy_consistency='eventual'")
        lazy_reads = stat(c, "Tarn_lazy_reads")
        got = q(c, sql)
        assert stat(c, "Tarn_lazy_reads") == lazy_reads + 1, "the read did not use the lazy index"
        q(c, "SET tarn_lazy_consistency='strong'")
        if q(c, sql) != first:
            return None
        if got == first:
            return {}
        return {"k": k, "strong": len(first), "lazy": len(got), "extra": sorted(set(got) - set(first))[:3],
                "missing": sorted(set(first) - set(got))[:3],
                "duplicated": sorted(r for r in set(got) if got.count(r) > 1)[:3]}

    q(c, "SET tarn_lazy_read_your_writes=ON")
    # A mismatch must hold with the table still and the outboxes empty, on
    # three tries in a row: a write left in flight can land after the
    # writer stops, and its delivery after the outboxes were seen empty.
    unstable, mismatches, tries = 0, [], 0
    while tries < 3:
        leaders = drained()
        results = [compare(k) for k in range(9)]
        if any(r is None for r in results):
            unstable += 1
            if unstable > 20:
                fail("the table kept changing after the writer stopped", unstable)
            time.sleep(.5)
            continue
        mismatches = [r for r in results if r]
        if not mismatches:
            break
        tries += 1
        time.sleep(.5)
    live = [n for g in groups for n in g["replicas"] if n != crashed and alive(n)]
    healer_stop.set()
    record("checked", writes=writes[0], during_alter=after - before, write_errors=len(errors),
           error_codes=sorted({e[0] for e in errors}), mismatches=mismatches, unstable_reads=unstable,
           mismatched_tries=tries, healed=healed, alter_retried=retried)
    if mismatches:
        for node in live:
            try:
                stats = json.dumps(rpc(node, "tarn_lazy_stats", []))
            except Exception as e:  # not answering
                stats = repr(e)
            print("FAIL lazy online outbox:", node, "leader" if node in leaders else "follower", stats, flush=True)
        fail("the lazy index differs from the table", mismatches)
    c.close()
    print("RF3/fsync online lazy build survives an owned leader crash mid-scan; lazy equals strong", flush=True)


if __name__ == "__main__":
    main()
