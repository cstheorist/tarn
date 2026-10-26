#!/usr/bin/env python3
"""A shard joins the cluster under load, and another leaves (issue #25).

    tests/sql/jointest.py --tarn 127.0.0.1:4000,127.0.0.1:4001 --idle 127.0.0.1:4002 \\
        --add 3=127.0.0.1:3331[,127.0.0.1:3332] [--remove 2 --dir DIR] [--seconds 45]
    tests/sql/jointest.py --verify 127.0.0.1:4002 --add 3=... [--remove 2]   (a server started again since)

While the workload of movetest.py runs through the --tarn servers (each
row's bucket moving between the shards):
  * an ordered LIMIT read held across ADD SHARD fails validation with the
    membership-change diagnostic, even on an empty index range;
  * TARN ADD SHARD makes the new shard a member: every SQL server lists it
    in SHOW TARN SHARDS, and it owns no bucket;
  * buckets move to it: a range at once, then the buckets of the rows the
    workload uses, back and forth between all the shards; the checks of
    movetest.py hold (no transfer lost or applied twice, each read of every
    shard sees each row once, no increment lost);
  * with --remove, then TARN REMOVE SHARD moves every bucket of that shard
    to the others and the shard leaves; its storage is stopped (the pid
    files in --dir), and the workload goes on without it;
  * the rows of the buckets the new shard owns are in its storage (read from
    it directly), and nowhere else;
  * the --idle server ran nothing since before the join: its first reads of
    every shard, routed with the membership from before, are refused (or
    find the shard that left gone) and run again with the new one, and see
    every row once; it writes rows on the new shard too.
--verify runs the last checks alone, on a server started again since the
join with its configuration from before (it reads the membership when it
starts).
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time

import pymysql

import movetest
from movetest import CONFLICT, DB, bucket_of, connect, q

TARANTOOL = os.environ.get("TARANTOOL", "tarantool")


def owned(rows, shard):
    """The buckets shard `shard` owns, from SHOW TARN BUCKETS."""
    out = set()
    for r in rows:
        if int(r[0]) != shard or not r[2]:
            continue
        for part in r[2].split(", "):
            lo, _, hi = part.partition("-")
            out.update(range(int(lo), int(hi or lo) + 1))
    return out


def storage_ids(replicas, space):
    """The primary keys of `space` in the storage of the leader of a shard,
    read directly."""
    script = """
local net = require('net.box')
local json = require('json')
for _, uri in ipairs(json.decode(os.getenv('REPLICAS'))) do
    local c = net.connect(uri, {user = 'tarn', password = 'tarn', wait_connected = 3})
    if c:is_connected() and not c:eval('return box.info.ro') then
        print(json.encode(c:eval('local out = {} for _, t in box.space[...]:pairs() do out[#out + 1] = t[1] end '
                                 .. 'return out', {os.getenv('SPACE')})))
        os.exit(0)
    end
end
os.exit(1)
"""
    env = dict(os.environ, REPLICAS=json.dumps(replicas.split(",")), SPACE=space)
    p = subprocess.run([TARANTOOL, "-e", script], capture_output=True, text=True, timeout=30,
                       stdin=subprocess.DEVNULL, env=env)
    if p.returncode != 0:
        raise RuntimeError(f"no leader among {replicas}: {p.stdout} {p.stderr}")
    return sorted(json.loads(p.stdout.strip().splitlines()[-1]))


class JoinTest(movetest.Test):
    def __init__(self, args):
        super().__init__(args)
        sid, _, reps = args.add.partition("=")
        self.new_id = int(sid)
        self.new_replicas = reps
        self.stats.update({"join": None, "join_err": None, "range": 0, "to_new": 0, "removed": None,
                           "remove_err": None, "ordered_scan": False, "ordered_conflict": False,
                           "ordered_error": None})

    def joiner(self, stop):
        """Add the shard, then move buckets to it: a range, and half of the
        buckets of the workload's rows (the mover takes it from then on)."""
        c = connect(self.servers[0])
        time.sleep(self.args.join_after)
        t0 = time.time()
        probe = connect(self.servers[0])
        try:
            # Hold an ordered scatter read across the join. The dedicated
            # empty table has no concurrent writes, so only membership can
            # invalidate it; a positive LIMIT must still record every shard.
            q(probe, "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            q(probe, "BEGIN")
            rows = q(probe, "SELECT tenant_id, id FROM ordered_probe WHERE state = 'open' "
                            "ORDER BY sort_key, id LIMIT 1")
            self.stats["ordered_scan"] = not rows
            q(c, f"TARN ADD SHARD {self.new_id} '{self.new_replicas}'")
            self.stats["join"] = time.time() - t0
            try:
                q(probe, "COMMIT")
            except pymysql.MySQLError as e:
                self.stats["ordered_error"] = str(e)
                self.stats["ordered_conflict"] = (e.args[0] == CONFLICT
                                                  and "cluster's shards changed" in str(e))
        except pymysql.MySQLError as e:
            self.stats["join_err"] = str(e)
            return
        finally:
            try:
                q(probe, "ROLLBACK")
            except pymysql.MySQLError:
                pass
            probe.close()
        self.shard_ids.append(self.new_id)   # (the mover's choice of shards)
        lo, hi = 1, self.args.range
        try:
            self.stats["range"] = q(c, f"TARN MOVE BUCKETS BETWEEN {lo} AND {hi} TO SHARD {self.new_id}")
        except pymysql.MySQLError as e:
            with self.lock:
                self.stats["move_errs"].append(f"range: {e}")
        for b in self.buckets[::2]:
            if time.time() >= stop:
                break
            try:
                n = q(c, f"TARN MOVE BUCKET {b} TO SHARD {self.new_id}")
                with self.lock:
                    self.stats["to_new"] += n
            except pymysql.MySQLError as e:
                with self.lock:
                    self.stats["move_errs"].append(str(e))
        if self.args.remove:
            self.leaver(c)

    def leaver(self, c):
        """Remove shard --remove, then stop its storage."""
        gone = self.args.remove
        self.shard_ids.remove(gone)   # (the mover moves no bucket to it from now on)
        t0 = time.time()
        try:
            n = q(c, f"TARN REMOVE SHARD {gone}")
            self.stats["removed"] = (time.time() - t0, n)
        except pymysql.MySQLError as e:
            self.stats["remove_err"] = str(e)
            return
        got = storage_ids(self.removed_replicas(), f"{DB}.bank")
        self.check(got == [], f"shard {gone} keeps no row of the table when it has left: {got[:10]}")
        for f in sorted(os.listdir(self.args.dir)):
            if f.startswith(f"s{gone}.") and f.endswith(".pid"):
                with open(os.path.join(self.args.dir, f)) as fh:
                    os.kill(int(fh.read()), 9)
        self.stats["stopped"] = time.time()

    def removed_replicas(self):
        """The replicas of shard --remove, as SHOW TARN SHARDS has them."""
        c = connect(self.servers[0])
        return [r[1] for r in q(c, "SHOW TARN SHARDS") if int(r[0]) == self.args.remove][0]

    def run(self):
        a = self.args
        self.setup()
        c = connect(self.servers[0])
        try:
            q(c, "CREATE TABLE ordered_probe (tenant_id INT NOT NULL, id INT NOT NULL, "
                 "state VARCHAR(8) NOT NULL, sort_key INT NOT NULL, PRIMARY KEY (tenant_id, id), "
                 "KEY by_state (state, sort_key, id)) PARTITION BY KEY(tenant_id)")
        finally:
            c.close()
        print(f"{len(self.servers)} SQL server(s), shards {self.shard_ids}, {self.count} buckets; shard "
              f"{self.new_id} joins after {a.join_after:.0f} s; the workload runs for {a.seconds:.0f} s")
        stop_at = time.time() + a.seconds
        th = self.workload(stop_at)
        th.append(threading.Thread(target=self.joiner, args=(stop_at,)))
        for t in th:
            t.start()
        for t in th:
            t.join()
        st = self.stats
        print(f"  joined in {st['join'] or 0:.2f} s; {st['range']} buckets of the range and {st['to_new']} of "
              f"the rows' moved to it")
        if not self.check(st["join"] is not None, f"TARN ADD SHARD {self.new_id} under load ({st['join_err']})"):
            return self.failures
        self.check(st["ordered_scan"] and st["ordered_conflict"],
                   f"ordered LIMIT across ADD SHARD fails its membership check ({st['ordered_error']})")
        if a.remove:
            r = st["removed"]
            print(f"  shard {a.remove} left in {r[0] if r else 0:.2f} s, moving {r[1] if r else 0} buckets")
            self.check(r is not None and st.get("stopped", stop_at) < stop_at,
                       f"TARN REMOVE SHARD {a.remove} under load, its storage stopped before the workload ended "
                       f"({st['remove_err']})")
        self.finish()
        for s in self.servers:
            self.check_members(connect(s), s)
        # (the mover may have moved some of the range there first: those of
        # the rows' buckets)
        self.check(a.range - len(self.buckets) <= st["range"] <= a.range,
                   f"the range of {a.range} buckets moved to the new shard ({st['range']})")
        self.check_storage(connect(self.servers[0]))
        self.verify(self.args.idle)
        return self.failures

    def check_members(self, c, server):
        rows = q(c, "SHOW TARN SHARDS")
        mine = [r for r in rows if int(r[0]) == self.new_id]
        self.check(len(mine) == 1 and mine[0][2] == "active" and mine[0][1] == self.new_replicas,
                   f"{server} lists shard {self.new_id} as a member: {rows}")
        if self.args.remove:
            gone = [r for r in rows if int(r[0]) == self.args.remove]
            self.check(len(gone) == 1 and gone[0][2] == "removed", f"{server} lists shard {self.args.remove} as removed")
            shards = sorted(int(r[0]) for r in q(c, "SHOW TARN BUCKETS"))
            self.check(self.args.remove not in shards, f"{server}: SHOW TARN BUCKETS leaves it out: {shards}")

    def check_storage(self, c):
        """The rows of the buckets the new shard owns are in its storage."""
        a = self.args
        rows = q(c, "SHOW TARN BUCKETS")
        count = sum(int(r[1]) for r in rows)
        mine = owned(rows, self.new_id)
        want = sorted(i for i in range(a.accounts) if bucket_of(i, count) in mine)
        got = storage_ids(self.new_replicas, f"{DB}.bank")
        self.check(got == want, f"the new shard's storage has the rows of its buckets: {len(got)} rows, "
                                f"{len(want)} expected")
        self.check(len(mine) >= a.range, f"the new shard owns buckets ({len(mine)})")

    def verify(self, server):
        """Reads and writes through `server`, which did not run a statement
        since before the join (or was started again since)."""
        a = self.args
        total = a.accounts * a.init
        bal = dict(q(connect(self.servers[0] if self.servers else server), "SELECT id, bal FROM bank"))
        c = connect(server)
        s = q(c, "SELECT SUM(bal), COUNT(*) FROM bank")[0]
        self.check(int(s[0]) == total and s[1] == a.accounts, f"{server}: the sum of every row ({s})")
        q(c, "BEGIN")
        s = q(c, "SELECT SUM(bal), COUNT(*) FROM bank")[0]
        q(c, "COMMIT")
        self.check(int(s[0]) == total and s[1] == a.accounts, f"{server}: ... in a transaction ({s})")
        rc = connect(server)
        q(rc, "SET SESSION transaction_isolation = 'READ-COMMITTED'")
        ids = [r[0] for r in q(rc, "SELECT id FROM bank")]
        self.check(sorted(ids) == list(range(a.accounts)), f"{server}: READ COMMITTED reads each row once")
        got = {i: q(c, "SELECT bal FROM bank WHERE id = %s", (i,))[0][0] for i in range(a.accounts)}
        self.check(got == bal, f"{server}: each row read by its key")
        self.check_members(c, server)
        # A transfer from a row on the new shard to one elsewhere.
        rows = q(c, "SHOW TARN BUCKETS")
        count = sum(int(r[1]) for r in rows)
        mine = owned(rows, self.new_id)
        on_new = [i for i in range(a.accounts) if bucket_of(i, count) in mine]
        other = [i for i in range(a.accounts) if bucket_of(i, count) not in mine]
        if not self.check(on_new and other, f"{server}: rows on the new shard and elsewhere"):
            return
        x, y = on_new[0], other[0]
        for _ in range(20):
            try:
                q(c, "BEGIN")
                q(c, "UPDATE bank SET bal = bal - 1 WHERE id = %s", (x,))
                q(c, "UPDATE bank SET bal = bal + 1 WHERE id = %s", (y,))
                q(c, "COMMIT")
                break
            except pymysql.MySQLError as e:
                q(c, "ROLLBACK")
                if e.args[0] != CONFLICT:
                    raise
        after = dict(q(c, "SELECT id, bal FROM bank WHERE id IN (%s, %s)", (x, y)))
        self.check(after[x] == bal[x] - 1 and after[y] == bal[y] + 1,
                   f"{server}: a transfer from a row of the new shard: {after}")
        s = q(c, "SELECT SUM(bal) FROM bank")[0][0]
        self.check(int(s) == total, f"{server}: ... the sum unchanged ({s})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", default="", help="SQL servers of the workload, comma-separated")
    ap.add_argument("--idle", help="a SQL server the test uses only after the join")
    ap.add_argument("--add", required=True, help="the shard that joins: id=replica[,replica...]")
    ap.add_argument("--verify", help="only check that this SQL server sees the new shard's rows")
    ap.add_argument("--seconds", type=float, default=45)
    ap.add_argument("--join-after", type=float, default=3)
    ap.add_argument("--range", type=int, default=60, help="buckets moved to the new shard at once")
    ap.add_argument("--remove", type=int, help="a shard to remove once the new one joined")
    ap.add_argument("--dir", help="the cluster's directory (to stop the storage of the shard that left)")
    ap.add_argument("--threads", type=int, default=6)
    ap.add_argument("--accounts", type=int, default=24)
    ap.add_argument("--init", type=int, default=1000)
    ap.add_argument("--counters", type=int, default=8)
    ap.add_argument("--move-pause", type=float, default=0.05)
    ap.add_argument("--min-moves", type=int, default=5)
    ap.add_argument("--no-moves", action="store_true")
    ap.add_argument("--settle", type=float, default=90)
    args = ap.parse_args()
    if args.verify:
        args.tarn = args.verify
        t = JoinTest(args)
        t.servers = []
        t.verify(args.verify)
        failures = t.failures
    else:
        if not args.tarn or not args.idle:
            ap.error("--tarn and --idle are needed (or --verify)")
        failures = JoinTest(args).run()
    print("PASSED" if failures == 0 else f"{failures} FAILED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
