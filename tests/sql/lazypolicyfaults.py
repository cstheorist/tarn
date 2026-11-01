#!/usr/bin/env python3
"""Strong lazy-index policy across one owned RF3 leader crash (#182).

All DML confirms before the fault. Only the known read-only prepared query
is retried after leader loss; no SQL mutation is replayed.
"""
import argparse
import json
from pathlib import Path
import time

import pymysql

from lazytest import connect, ev, pause, q, rpc, stat


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
        with (work / "policy-events.jsonl").open("a") as f:
            f.write(line + "\n")

    def leader(group, exclude=None):
        for node in group["replicas"]:
            if node == exclude:
                continue
            state = ev(node, "return {pid=box.info.pid,cwd=require('fio').cwd(),ro=box.info.ro,"
                             "term=box.info.election.term,wal=box.cfg.wal_mode,sync=tarn.cfg.sync}")
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

    first, state = wait_leader(groups[0])
    second, _ = wait_leader(groups[1])
    assert Path(state["cwd"]).parent.name == work.name
    owned_pid = int((work / (Path(state["cwd"]).name + ".pid")).read_text())
    assert owned_pid == state["pid"]
    crashed = None
    try:
        with connect(args.tarn, db=None) as c:
            q(c, "DROP DATABASE IF EXISTS lazy_policy_fault")
            q(c, "CREATE DATABASE lazy_policy_fault")
            q(c, "USE lazy_policy_fault")
            q(c, "CREATE TABLE t (tenant_id INT NOT NULL,id INT NOT NULL,k INT,n INT,"
                 "PRIMARY KEY(tenant_id,id),KEY lk(k,n,id) COMMENT 'tarn:lazy') "
                 f"PARTITION BY KEY(tenant_id) ENGINE={args.engine}")
            tenants = {}
            for tenant in range(1, 65):
                plan = json.loads(q(c, f"EXPLAIN FORMAT=JSON SELECT id FROM t WHERE tenant_id={tenant} AND id=1")[0][0])
                ids = plan["plan"]["accesses"][0]["routing"]["shard_ids"]
                assert len(ids) == 1
                tenants.setdefault(ids[0], tenant)
                if len(tenants) == 2:
                    break
            a, b = (tenants[s["id"]] for s in groups)
            q(c, "INSERT INTO t VALUES (%s,1,7,1),(%s,2,7,2)", (a, b))
            q(c, "SET tarn_lazy_read_your_writes=ON")
            assert q(c, "SELECT id,n FROM t WHERE k=7 ORDER BY n,id") == ((1, 1), (2, 2))
            q(c, "SET tarn_lazy_read_your_writes=OFF")
            pause([first, second])
            q(c, "UPDATE t SET k=8 WHERE tenant_id=%s AND id=1", (a,))
            q(c, "INSERT INTO t VALUES (%s,3,7,3)", (b,))
            before = stat(c, "Tarn_lazy_reads")
            assert q(c, "SELECT id,n FROM t WHERE k=7 ORDER BY n,id") == ((2, 2),)
            assert stat(c, "Tarn_lazy_reads") == before + 1
            touched = stat(c, "Tarn_lazy_session_touched_buckets")
            assert touched >= 2
            q(c, "SET tarn_lazy_consistency='strong',tarn_lazy_read_your_writes=ON,tarn_lazy_wait_timeout=0")
            q(c, "PREPARE read_q FROM 'SELECT id,n FROM t WHERE k=7 ORDER BY n,id'")
            assert q(c, "EXECUTE read_q") == ((2, 2), (3, 3))
            before = stat(c, "Tarn_lazy_reads")
            record("before_crash", leader=first, state=state, tenants=tenants, touched=touched)
            # The storage process terminates itself only after validating
            # this private cohort's PID/cwd and acknowledging the schedule.
            # This also works when the test client is on another machine.
            receipt = ev(first, "local pid,cwd=...;assert(tarn.cfg.test_hooks and not box.info.ro);"
                         "assert(box.info.pid==pid and require('fio').cwd()==cwd);"
                         "require('fiber').create(function() require('fiber').sleep(.05);"
                         "local ffi=require('ffi');ffi.cdef('void _exit(int);');ffi.C._exit(86) end);"
                         "return {pid=box.info.pid,term=box.info.election.term}", [owned_pid, state["cwd"]])
            crashed = first
            (work / (Path(state["cwd"]).name + ".killed")).write_text(str(owned_pid))
            record("owned_crash_scheduled", receipt=receipt)
            deadline = time.monotonic() + 25
            replacement = wait_leader(groups[0], exclude=crashed)
            assert replacement and replacement[0] != first, "no replacement leader"
            failures = []
            while True:
                try:
                    rows = q(c, "EXECUTE read_q")
                    break
                except pymysql.MySQLError as e:
                    failures.append(list(e.args))
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(.05)  # retry only this known read-only query
            assert rows == ((2, 2), (3, 3)), rows
            assert q(c, "SELECT id FROM t WHERE k=8") == ((1,),)
            assert stat(c, "Tarn_lazy_reads") == before
            assert stat(c, "Tarn_lazy_session_touched_buckets") == touched
            record("strong_after_crash", replacement=replacement, rows=rows, read_errors=failures,
                   touched=touched, lazy_reads=before)
    finally:
        # Attempt every surviving replica; the private cohort is stopped by
        # the shell owner even if a role transition makes cleanup fail.
        errors = []
        for group in groups:
            for node in group["replicas"]:
                if node != crashed:
                    try:
                        result = rpc(node, "tarn_lazy_control", ["resume"])
                        assert result[0] == "ok", (node, result)
                    except Exception as error:
                        errors.append([node, repr(error)])
        record("delivery_restored", crashed=crashed, errors=errors)
        assert not errors, errors
    print("RF3/fsync strong policy survives owned leader crash; exact read-only results", flush=True)


if __name__ == "__main__":
    main()
