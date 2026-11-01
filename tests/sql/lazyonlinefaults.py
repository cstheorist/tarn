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
from pathlib import Path
import threading
import time

import pymysql

from lazytest import ev, q, rpc, stat, wait_empty


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
            cur.executemany("INSERT INTO t VALUES (%s,%s,%s,%s)", rows[start:start + 250])

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

    crashed = None
    threads = [threading.Thread(target=writer), threading.Thread(target=run_alter)]
    try:
        r = rpc(victim, "tarn_test_hook", ["lazy_bootstrap", "hold"])
        assert r[0] == "ok", r
        threads[0].start()
        time.sleep(.3)
        threads[1].start()
        deadline = time.monotonic() + 60
        while True:
            assert time.monotonic() < deadline and "error" not in alter, ("the scan never reached the hook", alter)
            try:
                # (waits up to 5 s on the node, as long as the call may take)
                if rpc(victim, "tarn_test_hook", ["lazy_bootstrap", "reached"])[1] > 0:
                    break
            except RuntimeError as e:
                if "timed out" not in str(e):
                    raise
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
        assert not threads[1].is_alive(), "the ALTER did not return"
        assert alter.get("ok"), alter
        after = writes[0]
        time.sleep(.3)
    finally:
        stop.set()
        for t in threads:
            if t.is_alive():
                t.join(timeout=30)
    assert writes[0] > before, "no write succeeded after the crash"
    survivors = [first, replacement]
    wait_empty(survivors, timeout=30)
    q(c, "SET tarn_lazy_read_your_writes=ON")
    mismatches = []
    for k in range(9):
        sql = f"SELECT tenant_id,id,n FROM t WHERE k={k} ORDER BY n,id"
        q(c, "SET tarn_lazy_consistency='strong'")
        strong = q(c, sql)
        q(c, "SET tarn_lazy_consistency='eventual'")
        lazy_reads = stat(c, "Tarn_lazy_reads")
        got = q(c, sql)
        assert stat(c, "Tarn_lazy_reads") == lazy_reads + 1, "the read did not use the lazy index"
        if got != strong:
            mismatches.append({"k": k, "strong": len(strong), "lazy": len(got)})
    record("checked", writes=writes[0], during_alter=after - before, write_errors=len(errors),
           error_codes=sorted({e[0] for e in errors}), mismatches=mismatches)
    assert not mismatches, mismatches
    c.close()
    print("RF3/fsync online lazy build survives an owned leader crash mid-scan; lazy equals strong", flush=True)


if __name__ == "__main__":
    main()
