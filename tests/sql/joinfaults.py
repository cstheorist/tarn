#!/usr/bin/env python3
"""A TARN ADD SHARD whose SQL server dies (issue #25) is finished by another
server: given up when it died before the shard joined, finished when after;
a TARN REMOVE SHARD whose server dies goes on when run again. Run by
tests/joinfaults.sh, which starts a 2-shard cluster with three SQL
servers, test hooks, ddl_job_stale = 2 s and 480 buckets, and the storage
of shard 3.

The server driving the join is killed while a test hook holds one of its
steps, and started again after each case:

1. while the new shard makes the tables (phase 'prepare'): another server
   gives the job up; no shard has the new membership, and the statement
   runs again through another server;
2. while it records the new membership on the second shard (phase 'apply',
   the first shard has it): another server finishes it, and every shard
   has it.

Then every SQL server lists the new shard (the restarted one too), a bucket
moves to it, and its rows are read and written there. Then shard 2 leaves:

4. the server driving TARN REMOVE SHARD is killed while a move of the
   drain copies a bucket: other servers end the moves it left, and the
   shard stays draining on every shard, with buckets;
5. run again through another server, it finishes: the shard is removed on
   every shard, the SQL servers leave it out, and once its storage is
   stopped every server still reads and writes every row.
"""
import argparse
import json
import os
import sys
import threading
import time

import pymysql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from movefaults import Test as MoveFaults  # noqa: E402
from movetest import bucket_of  # noqa: E402

DB = "joinfaults"


class Test(MoveFaults):
    def __init__(self, args):
        super().__init__(args)
        self.new_id = int(args.add.partition("=")[0])
        self.new_replicas = args.add.partition("=")[2]
        self.shards.append(self.new_replicas.split(",")[0])

    def sql(self, k, *stmts, db=DB):
        return super().sql(k, *stmts, db=db)

    def members(self, shard):
        """(version, shard ids) of the membership shard `shard` (an index) keeps."""
        r = json.loads(self.lua(shard, "local r = tarn_members() local ids = {} "
                                       "for _, m in ipairs(r[3]) do ids[#ids + 1] = m.id end return {r[2], ids}"))
        return r[0], sorted(r[1])

    def listed(self, k):
        return sorted(int(r[0]) for r in self.sql(k, "SHOW TARN SHARDS"))

    def setup(self):
        self.sql(0, f"DROP DATABASE IF EXISTS {DB}", f"CREATE DATABASE {DB}", db=None)
        self.sql(0, "CREATE TABLE t (k INT, id INT, v INT, PRIMARY KEY (k, id)) PARTITION BY KEY (k)")
        self.sql(0, "INSERT INTO t VALUES " + ",".join(f"(1, {i}, {i})" for i in range(100)))
        self.count = sum(int(r[1]) for r in self.sql(0, "SHOW TARN BUCKETS"))
        self.b = bucket_of(1, self.count)

    def rows_on(self, shard):
        return int(self.lua(shard, f"local s = box.space['{DB}.t'] return s and s:count() or 0"))

    def state_of(self, shard, id):
        """The state of shard `id` in the membership shard `shard` (an index) keeps."""
        return json.loads(self.lua(shard, f"for _, m in ipairs(tarn_members()[3]) do if m.id == {id} then "
                                          f"return m.state end end return 'none'"))

    def owned(self, id):
        return sum(int(r[1]) for r in self.sql(0, "SHOW TARN BUCKETS") if int(r[0]) == id)

    def remove_in_thread(self, k, id):
        res = {}

        def run():
            try:
                res["ok"] = self.sql(k, f"TARN REMOVE SHARD {id}")
            except Exception as e:  # the server is killed: its connection breaks
                res["err"] = str(e)
        th = threading.Thread(target=run)
        th.start()
        return th, res

    def add_in_thread(self, k):
        res = {}

        def run():
            try:
                res["ok"] = self.sql(k, f"TARN ADD SHARD {self.new_id} '{self.new_replicas}'")
            except Exception as e:  # the server is killed: its connection breaks
                res["err"] = str(e)
        th = threading.Thread(target=run)
        th.start()
        return th, res

    def wait_no_job(self, secs=15):
        deadline = time.time() + secs
        while time.time() < deadline:
            if self.jobs() == 0:
                return True
            time.sleep(0.2)
        return False

    def killed_at(self, label, hook_shard, hook, while_held):
        """TARN ADD SHARD through SQL server 3, killed while `hook` holds
        the join on shard `hook_shard` (an index); another server takes the
        job over once it is stale. `while_held` checks the shards before
        the held step of the dead server goes on."""
        self.hook(hook_shard, hook, "hold")
        th, res = self.add_in_thread(2)
        reached = self.hook(hook_shard, hook, "reached")
        t0 = time.time()
        self.check('"ok",1' in reached.replace(" ", ""), f"{label}: the join reached the step", reached)
        self.kill_server(2)
        done = self.wait_no_job()
        took = time.time() - t0
        self.check(done and took < 9, f"{label}: another server ended the job while the step is held ({took:.1f} s)")
        while_held()
        self.hook(hook_shard, hook, "release")
        th.join(10)
        self.check("ok" not in res, f"{label}: the statement did not return success", res)
        self.start_server(2)

    def run(self):
        self.setup()
        new = len(self.shards) - 1
        want = sorted([1, 2, self.new_id])

        def not_joined():
            for i in range(2):
                v, ids = self.members(i)
                self.check(self.new_id not in ids, f"1: shard {i + 1} does not have the new shard in its membership",
                           (v, ids))
            self.check(self.new_id not in self.listed(0), "1: no SQL server lists it")

        def joined():
            for i in range(3):
                v, ids = self.members(i)
                self.check(ids == want and v >= 1, f"2: shard {i + 1} has the new membership", (v, ids))

        print("1. killed before the shard joined")
        self.killed_at("1", new, "schema_import", not_joined)
        print("2. killed while it records the membership, after the first shard")
        self.killed_at("2", 1, "members_set", joined)
        for k in range(3):
            self.check(self.listed(k) == want, f"2: SQL server {k + 1} lists every shard", self.listed(k))
        print("3. a bucket moves to it")
        self.sql(0, f"TARN MOVE BUCKET {self.b} TO SHARD {self.new_id}")
        owners, moving = self.where(self.b)
        self.check(owners == [self.new_id] and not moving, f"3: bucket {self.b} is on shard {self.new_id}",
                   (owners, moving))
        self.check(self.rows_on(new) == 100 and self.rows_on(0) + self.rows_on(1) == 0,
                   "3: its 100 rows are on the new shard alone", [self.rows_on(i) for i in range(3)])
        for k in range(3):
            n = self.sql(k, "SELECT COUNT(*), SUM(v) FROM t")[0]
            self.check(n[0] == 100 and int(n[1]) == sum(range(100)), f"3: SQL server {k + 1} reads them", n)
        self.sql(2, "UPDATE t SET v = v + 1 WHERE k = 1 AND id = 7")
        v = self.sql(1, "SELECT v FROM t WHERE k = 1 AND id = 7")[0][0]
        self.check(v == 8, "3: they take writes", v)
        try:
            self.sql(1, f"TARN ADD SHARD {self.new_id} '{self.new_replicas}'")
            self.check(False, "a second TARN ADD SHARD of it fails")
        except pymysql.MySQLError as e:
            self.check("already" in str(e), "a second TARN ADD SHARD of it fails", str(e))
        print("4. shard 2 leaves; the server driving it is killed while it drains")
        self.hook(1, "bucket_rows", "hold")
        th, res = self.remove_in_thread(2, 2)
        reached = self.hook(1, "bucket_rows", "reached")
        self.check('"ok",1' in reached.replace(" ", ""), "4: the drain reached a copy", reached)
        self.kill_server(2)
        self.check(self.wait_no_job(secs=20), "4: other servers ended the moves it left")
        states = [self.state_of(i, 2) for i in range(3)]
        self.check(states == ["draining"] * 3, "4: shard 2 is draining on every shard", states)
        self.check(self.owned(2) > 0, "4: ... with buckets", self.owned(2))
        self.hook(1, "bucket_rows", "release")
        th.join(10)
        self.check("ok" not in res, "4: the statement did not return success", res)
        self.start_server(2)
        print("5. run again, it finishes")
        try:
            r = self.sql(1, "TARN REMOVE SHARD 2")
            self.check(True, f"5: TARN REMOVE SHARD 2 ran to its end {r}")
        except pymysql.MySQLError as e:
            self.check(False, "5: TARN REMOVE SHARD 2 ran to its end", str(e))
        states = [self.state_of(i, 2) for i in range(3)]
        self.check(states == ["removed"] * 3, "5: shard 2 is removed on every shard", states)
        self.check(self.rows_on(1) == 0, "5: its storage keeps no row", self.rows_on(1))
        for k in range(3):
            listed = {int(r[0]): r[2] for r in self.sql(k, "SHOW TARN SHARDS")}
            shown = sorted(int(r[0]) for r in self.sql(k, "SHOW TARN BUCKETS"))
            self.check(listed.get(2) == "removed" and shown == [1, self.new_id],
                       f"5: SQL server {k + 1} lists it removed and shows no buckets of it", (listed, shown))
        with open(os.path.join(self.args.dir, "s2.1.pid")) as f:
            os.kill(int(f.read()), 9)
        for k in range(3):
            n = self.sql(k, "SELECT COUNT(*), SUM(v) FROM t")[0]
            self.check(n[0] == 100 and int(n[1]) == sum(range(100)) + 1,
                       f"5: with its storage stopped, SQL server {k + 1} reads every row", n)
        self.sql(1, "UPDATE t SET v = v - 1 WHERE k = 1 AND id = 7")
        v = self.sql(2, "SELECT v FROM t WHERE k = 1 AND id = 7")[0][0]
        self.check(v == 7, "5: ... and writes them", v)
        return self.failures


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True, help="three SQL servers, comma-separated")
    ap.add_argument("--shards", required=True, help="a storage node of shards 1 and 2")
    ap.add_argument("--add", required=True, help="the shard that joins: id=replica[,replica...]")
    ap.add_argument("--dir", required=True, help="the cluster's directory (pid files, logs)")
    args = ap.parse_args()
    failures = Test(args).run()
    print("PASSED" if failures == 0 else f"{failures} FAILED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
