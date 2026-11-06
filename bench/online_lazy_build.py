#!/usr/bin/env python3
"""An online lazy-index build above alter_check_rows, with a writer (#184).

  python3 bench/online_lazy_build.py --tarn HOST:PORT --kv LEADER,LEADER
        [--rows-per-shard 1200000] [--rate 200] [--engine vinyl] [--out RESULTS]
        [--index lazy|plain|unique] [--any-size] [--other-rate 0] [--settle NODE,...]

--index plain adds an ordinary secondary index instead (no lazy checks; each
shard's new index is counted against its rows). --index unique adds a
unique key on n without the shard key, whose global index is built online
(#292): the writer then moves each row's n up by the table's row count,
which keeps the values unique, and the script also polls the job's phase on
the first shard. Afterwards the index's entries are compared with the
table's rows (a count and two order-independent digests over every shard),
rows are looked up by their values, and a duplicate must be refused.
--any-size allows tables below alter_check_rows. --other-rate adds a second
session inserting into another table (u) at that rate: on a replicated
shard, a vinyl build behind the fence makes the shard's other synchronous
writes go one at a time (#264), and they wait while the followers apply the
build (#267). --settle (every storage node) checkpoints them after loading
and waits until vinyl dumps and compacts nothing on any of them: on one
host, the load's dumps would otherwise share the disk with the build.

Needs the Python `tarantool` connector for its storage probes.

Loads a table (t: tenant_id, id, k, n, v) with ROWS-PER-SHARD rows on each
shard, more than the fenced build's default limit (alter_check_rows, one
million), then runs ALTER TABLE t ADD INDEX lk(k,n,id) COMMENT 'tarn:lazy'
while one session updates random rows at RATE writes per second (each
changes the indexed key). It records:

- the ALTER's wall time and the build's rate (rows per second);
- every write's latency, before, during and after the ALTER; the longest
  write during it bounds the time writes waited behind its fences;
- each shard leader's RSS, its WAL bytes, its rows and the new index's
  entries (vinyl counts are estimates), and its election term: a shard
  whose leader resigned during the ALTER (#267) counts a leader change;
- an ALTER that returned 1180 and completed in the background (its error,
  and when SHOW INDEX first listed the index);
- for a copy, when each shard's copy entered each phase, from its record,
  polled every 0.1 s, with the polls that took over 0.2 s; and the writes
  during the ALTER that took over 0.2 s, by when they began;
- for a unique key, when its job entered each phase (its steps and the
  backfill), polled the same way, and the backfill's rate;
- after the deliveries drain, whether the index's live entries are exactly
  the table's rows (a count and two order-independent digests over every
  shard), and whether lazy reads return what strong reads return for the
  first page of every key value.

A write's latency includes the SQL server; a refused write counts as an
error and is not retried by this script.
"""
import argparse
import datetime
import hashlib
import json
import os
import platform
import random
import statistics
import subprocess
import sys
import threading
import time

import pymysql
import tarantool

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "tests", "sql"))
from lazytest import wait_empty  # noqa: E402

DB = "online_lazy_build"
KEYS = 64


def connect(endpoint, db=DB, read_timeout=3600):
    host, port = endpoint.rsplit(":", 1)
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db, autocommit=True,
                           charset="utf8mb4", connect_timeout=3, read_timeout=read_timeout, write_timeout=60)


def q(c, sql, args=None):
    with c.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall() if cur.description else cur.rowcount


NODE_STATE = """
local fio = require('fio')
local rss
local f = io.open('/proc/self/status')
if f then
    for line in f:lines() do
        local kb = line:match('^VmRSS:%s+(%d+)')
        if kb then rss = tonumber(kb) * 1024 end
    end
    f:close()
end
local wal = 0
for _, name in ipairs(fio.glob(fio.pathjoin(box.cfg.wal_dir, '*.xlog'))) do
    local st = fio.stat(name); if st then wal = wal + st.size end
end
local entries
for _, d in box.space._tarn_lazy_defs:pairs() do
    local sp = box.space[d.gen]
    if d.phase ~= 'retired' and sp ~= nil then entries = (entries or 0) + sp:len() end
end
local rows = 0
for _, t in box.space._tarn_catalog:pairs() do
    if t.name == ... and box.space[t.name] then rows = box.space[t.name]:len() end
end
-- The entries of the table's global indexes on this shard (named after its
-- space and the index's id; a building one's conflicts space ends in 'c').
local uix
local sp = box.space[...]
if sp ~= nil then
    local tag = '_tarn_uix_' .. sp.id .. '_'
    for _, s in box.space._space.index.name:pairs({tag}, {iterator = 'GE'}) do
        if s.name:sub(1, #tag) ~= tag then break end
        if s.name:sub(#tag + 1):match('^%d+$') then uix = (uix or 0) + box.space[s.id]:len() end
    end
end
return {rss = rss, wal_bytes = wal, lazy_entries = entries, uix_entries = uix, rows = rows,
        check_rows = tarn.cfg.alter_check_rows, engine = box.space[...] and box.space[...].engine,
        term = box.info.election.term, ro = box.info.ro}
"""


def ev(node, script, args):
    host, port = node.rsplit(":", 1)
    conn = tarantool.Connection(host, int(port), user="tarn", password="tarn", socket_timeout=1800,
                                connect_now=True)
    try:
        return conn.eval(script, args).data[0]
    finally:
        conn.close()


def nodes_state(nodes):
    return [ev(n, NODE_STATE, [f"{DB}.t"]) for n in nodes]


# Every row's index identity (key k, n, id and primary key tenant_id, id),
# hashed two ways and summed modulo 2^32, with the count: over the table's
# rows, and over the live entries of the published lazy index. Summed over
# the shards, the two must be equal.
DIGEST = """
local digest, fiber = require('digest'), require('fiber')
local name = ...
local sp = box.space[name]
local gen
for _, d in box.space._tarn_lazy_defs:pairs() do
    if d.base_id == sp.id and d.phase == 'ready' then gen = d.gen end
end
local M = 4294967296
local function add(acc, k, n, id, t, pid)
    local text = string.format('%s/%s/%s/%s/%s', tostring(k), tostring(n), tostring(id), tostring(t), tostring(pid))
    acc[1] = acc[1] + 1
    acc[2] = (acc[2] + digest.crc32(text)) % M
    acc[3] = (acc[3] + digest.murmur(text)) % M
    -- (memtx reads do not yield: stay within the fiber slice; nothing
    -- writes the table by now)
    if acc[1] % 10000 == 0 then fiber.yield() end
end
local rows, entries = {0, 0, 0}, {0, 0, 0}
for _, r in sp:pairs() do add(rows, r[3], r[4], r[2], r[1], r[2]) end
local target = gen and box.space[gen]
if target then
    for _, e in target.index.ordered:pairs({true}, {iterator = 'GE'}) do
        if e.live then add(entries, e.k1, e.k2, e.k3, e.p1, e.p2) end
    end
end
return {rows = rows, entries = entries, gen = gen}
"""

# The same for a global unique index on n: every row's (n, tenant_id, id),
# and every entry of the table's global index spaces on the shard (the key,
# then the rest of the row's primary key). The entries are sharded by n, so
# only the sums over every shard compare. Also the global index and
# conflicts spaces named after the table's space.
UNIQUE_DIGEST = """
require('fiber').set_slice({warn = 60, err = 600})
local digest, fiber = require('digest'), require('fiber')
local sp = box.space[...]
local M = 4294967296
local function add(acc, n, t, id)
    local text = string.format('%s/%s/%s', tostring(n), tostring(t), tostring(id))
    acc[1] = acc[1] + 1
    acc[2] = (acc[2] + digest.crc32(text)) % M
    acc[3] = (acc[3] + digest.murmur(text)) % M
    if acc[1] % 10000 == 0 then fiber.yield() end
end
local rows, entries, spaces, conflicts = {0, 0, 0}, {0, 0, 0}, {}, {}
for _, r in sp:pairs() do
    if r[4] ~= nil then add(rows, r[4], r[1], r[2]) end
end
local tag = '_tarn_uix_' .. sp.id .. '_'
for _, s in box.space._space.index.name:pairs({tag}, {iterator = 'GE'}) do
    if s.name:sub(1, #tag) ~= tag then break end
    if s.name:sub(#tag + 1):match('^%d+$') then
        spaces[#spaces + 1] = s.name
        for _, e in box.space[s.id]:pairs() do add(entries, e[1], e[2], e[3]) end
    else
        conflicts[#conflicts + 1] = s.name
    end
end
return {rows = rows, entries = entries, spaces = spaces, conflicts = conflicts}
"""

# The phase of the schema change of table space `...` (its job record, on
# the first shard), or nil once it ended.
JOB_PHASE = """
local t = box.space._tarn_meta:get{'job:' .. ...}
return t and t.value.phase or box.NULL
"""


def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    return s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))]


def summary(samples, since=None):
    """Latencies of samples (start, ms, ok); with since, also the writes over
    0.2 s, by when they began after it (s), at most 20."""
    lat = [x[1] for x in samples if x[2]]
    out = {"writes": len(lat), "errors": sum(1 for x in samples if not x[2]),
           "p50_ms": percentile(lat, 50), "p99_ms": percentile(lat, 99), "max_ms": max(lat) if lat else None}
    if since is not None:
        out["slow"] = [[round(x[0] - since, 2), round(x[1])] for x in samples if x[1] > 200][:20]
    return out


# A copy's phase on a shard, from its record (the space's id before the
# ALTER), and whether its cleanup is done.
PHASE = """
local r = box.space._tarn_meta:get{'engine:' .. ...}
if r == nil then return box.NULL end
return r.value.cleaned and 'cleaned' or r.value.phase
"""


def poller_conns(nodes):
    """Connections to each shard's leader, for phase_poller (opened before
    the ALTER begins, so that its first poll is at once)."""
    conns = []
    for n in nodes:
        host, port = n.rsplit(":", 1)
        conns.append(tarantool.Connection(host, int(port), user="tarn", password="tarn", socket_timeout=60,
                                          connect_now=True))
    return conns


def phase_poller(conns, ids, began, stop, timeline, stalls, job=None, jobs=None):
    """Polls each shard's copy phase every 0.1 s until stop, through conns
    (poller_conns, closed at the end): each phase change as [s after began,
    phase] in timeline[shard], and each poll over 0.2 s as [s after began,
    shard, ms] in stalls. With job (the table's space name), also the job's
    phase on the first shard, its changes in jobs the same way."""
    last, last_job = [None] * len(conns), None
    try:
        while not stop.is_set():
            if job is not None:
                t0 = time.monotonic()
                try:
                    phase = conns[0].eval(JOB_PHASE, [job]).data[0]
                except Exception as e:  # a leader that does not answer
                    phase = f"error: {e}"
                if phase != last_job:
                    jobs.append([round(t0 - began, 2), phase])
                    last_job = phase
            for i, conn in enumerate(conns):
                t0 = time.monotonic()
                try:
                    phase = conn.eval(PHASE, [ids[i]]).data[0]
                except Exception as e:
                    phase = f"error: {e}"
                t1 = time.monotonic()
                if phase != last[i]:
                    timeline[i].append([round(t0 - began, 2), phase])
                    last[i] = phase
                if t1 - t0 > 0.2:
                    stalls.append([round(t0 - began, 2), i + 1, round((t1 - t0) * 1000)])
            stop.wait(0.1)
    finally:
        for conn in conns:
            conn.close()


def job_steps(jobs, alter_s):
    """How long the job spent in each phase, from its timeline (to the ALTER's
    return for the last one): [[phase, s]], in order."""
    out = []
    for i, (at, phase) in enumerate(jobs):
        if phase is None:
            continue
        end = jobs[i + 1][0] if i + 1 < len(jobs) else alter_s
        out.append([phase, round(end - at, 2)])
    return out


# A checkpoint, which dumps vinyl's in-memory level to disk.
CHECKPOINT = "box.snapshot() return true"
# Whether vinyl is busy: tasks (dumps, compactions) running, or bytes waiting
# for compaction.
VINYL_BUSY = """
local s = box.stat.vinyl().scheduler
return s.tasks_inprogress > 0 or s.compaction_queue > 0
"""


def settle(all_nodes, quiet=5, timeout=900):
    """After loading: checkpoint every storage node, then wait until vinyl
    has run no dump or compaction on any of them for `quiet` seconds: the
    load's dumps would otherwise share the disk with the ALTER (all the
    nodes here share one). The seconds it took."""
    t0 = time.monotonic()
    for n in all_nodes:
        ev(n, CHECKPOINT, [])
    since = time.monotonic()
    while time.monotonic() - since < quiet:
        assert time.monotonic() - t0 < timeout, "the storage nodes did not settle"
        if any(ev(n, VINYL_BUSY, []) for n in all_nodes):
            since = time.monotonic()
        time.sleep(1)
    took = time.monotonic() - t0
    print(f"settled in {took:.0f} s", flush=True)
    return took


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


ALTERS = {
    "lazy": "ALTER TABLE t ADD INDEX lk(k,n,id) COMMENT 'tarn:lazy'",
    "plain": "ALTER TABLE t ADD INDEX lk(k,n,id)",
    "unique": "ALTER TABLE t ADD UNIQUE KEY un (n)",
}
INDEX_NAME = {"lazy": "lk", "plain": "lk", "unique": "un"}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tarn", required=True)
    ap.add_argument("--kv", required=True, help="one leader per shard, comma-separated")
    ap.add_argument("--rows-per-shard", type=int, default=1_200_000)
    ap.add_argument("--rate", type=float, default=200, help="writer target, writes per second")
    ap.add_argument("--engine", choices=("memtx", "vinyl"), default="vinyl")
    ap.add_argument("--before", type=float, default=10, help="seconds of writes before the ALTER")
    ap.add_argument("--out", help="results file (bench/results_online_lazy_build_ENGINE.json)")
    ap.add_argument("--index", choices=("lazy", "plain", "unique"), default="lazy")
    ap.add_argument("--any-size", action="store_true", help="allow shards below alter_check_rows")
    ap.add_argument("--other-rate", type=float, default=0, help="writes per second to another table, u")
    ap.add_argument("--settle", help="every storage node, comma-separated: after loading, checkpoint each and wait "
                                     "until no vinyl dump or compaction runs on any, before the writes begin")
    args = ap.parse_args()
    args.out = args.out or os.path.join(HERE, f"results_online_lazy_build_{args.engine}.json")
    nodes = args.kv.split(",")
    shards = len(nodes)

    with connect(args.tarn, db=None) as c:
        q(c, f"DROP DATABASE IF EXISTS {DB}")
        q(c, f"CREATE DATABASE {DB}")
    c = connect(args.tarn)
    q(c, "CREATE TABLE t (tenant_id INT NOT NULL,id INT NOT NULL,k INT,n INT,v VARCHAR(16),"
         f"PRIMARY KEY(tenant_id,id)) PARTITION BY KEY(tenant_id) ENGINE={args.engine}")
    if args.other_rate:
        q(c, "CREATE TABLE u (tenant_id INT NOT NULL,id INT NOT NULL,n INT,"
             f"PRIMARY KEY(tenant_id,id)) PARTITION BY KEY(tenant_id) ENGINE={args.engine}")
    total = args.rows_per_shard * shards
    started = time.monotonic()
    batch, refusals = 500, 0
    with c.cursor() as cur:
        for first in range(0, total, batch):
            ids = range(first + 1, min(total, first + batch) + 1)
            values = ",".join(f"({i % 997},{i},{i % KEYS},{i},'v{i % 1000}')" for i in ids)
            for attempt in range(200):
                try:
                    cur.execute(f"INSERT INTO t VALUES {values}")
                    break
                except pymysql.MySQLError as e:
                    # Vinyl admission refuses a fresh write before any effect
                    # while dump or compaction catches up: retry it later.
                    if e.args[0] != 1205 or "Vinyl" not in str(e.args[1]) or attempt == 199:
                        raise
                    refusals += 1
                    time.sleep(min(1.0, 0.05 * (attempt + 1)))
            if first and first % 200_000 == 0:
                print(f"loaded {first} rows ({time.monotonic() - started:.0f} s)", flush=True)
    load_s = time.monotonic() - started
    settle_s = settle(args.settle.split(",")) if args.settle else None
    before_state = nodes_state(nodes)
    print(f"loaded {total} rows in {load_s:.0f} s; per shard: {[s['rows'] for s in before_state]}", flush=True)
    assert args.any_size or all(s["rows"] > s["check_rows"] for s in before_state), (
        "each shard must exceed alter_check_rows", before_state)
    replicas = ev(nodes[0], "local n = 0 for _ in pairs(box.info.replication) do n = n + 1 end return n", [])

    samples, other, errors, stop = [], [], [], threading.Event()
    alter_error = None
    ids = [ev(n, "return box.space[...].id", [f"{DB}.t"]) for n in nodes]
    timeline, stalls, polled, jobs = [[] for _ in nodes], [], threading.Event(), []

    def other_writer():
        # Inserts into u, a table the ALTER doesn't touch.
        period, i = 1 / args.other_rate, 0
        with connect(args.tarn, read_timeout=120) as w:
            nxt = time.monotonic()
            while not stop.is_set():
                i += 1
                t0 = time.monotonic()
                try:
                    q(w, "INSERT INTO u VALUES (%s,%s,0)", (i % 997, i))
                    ok = True
                except pymysql.MySQLError:
                    ok = False
                t1 = time.monotonic()
                other.append((t0, (t1 - t0) * 1000, ok))
                nxt = max(nxt + period, t1)
                time.sleep(max(0, nxt - time.monotonic()))

    def writer():
        rnd = random.Random(184)
        period = 1 / args.rate
        # Under a unique key on n, n moves up by the row count: row i's
        # values are i plus multiples of it, so no two rows share one.
        step = total if args.index == "unique" else 1
        with connect(args.tarn, read_timeout=120) as w:
            nxt = time.monotonic()
            while not stop.is_set():
                rid = rnd.randrange(1, total + 1)
                t0 = time.monotonic()
                try:
                    q(w, "UPDATE t SET k=%s,n=n+%s WHERE tenant_id=%s AND id=%s",
                      (rnd.randrange(KEYS), step, rid % 997, rid))
                    ok = True
                except pymysql.MySQLError as e:
                    ok = False
                    errors.append(f"{e.args[0]}: {e.args[1]}"[:200])
                t1 = time.monotonic()
                samples.append((t0, (t1 - t0) * 1000, ok))
                nxt = max(nxt + period, t1)
                time.sleep(max(0, nxt - time.monotonic()))

    threads = [threading.Thread(target=writer)]
    if args.other_rate:
        threads.append(threading.Thread(target=other_writer))
    for th in threads:
        th.start()
    try:
        time.sleep(args.before)
        conns = poller_conns(nodes)
        alter_begin = time.monotonic()
        began_at = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")
        poller = threading.Thread(target=phase_poller, args=(conns, ids, alter_begin, polled, timeline, stalls),
                                  kwargs={"job": f"{DB}.t" if args.index == "unique" else None, "jobs": jobs})
        poller.start()
        try:
            q(c, ALTERS[args.index])
        except pymysql.MySQLError as e:
            # A replicated shard's leader can resign while its followers
            # apply the build (#267): the ALTER reports 1180, and a SQL
            # server finishes it in the background.
            if e.args[0] != 1180 or "completes in the background" not in str(e.args[1]):
                raise
            alter_error = f"{e.args[0]}: {e.args[1]}"
        alter_end = time.monotonic()
        time.sleep(5)
    finally:
        stop.set()
        polled.set()
        for th in threads:
            th.join()
        poller.join()
    alter_s = alter_end - alter_begin
    print(f"ALTER began at {began_at} and returned after {alter_s:.1f} s{': ' + alter_error if alter_error else ''}",
          flush=True)
    published_s = None
    if alter_error:
        published_s = wait_published(c, INDEX_NAME[args.index], timeout=600) - alter_begin
        print(f"the index was published {published_s:.1f} s after the ALTER began", flush=True)
    if args.index == "lazy":
        wait_empty(nodes, timeout=600)
    after_state = nodes_state(nodes)
    common = {"alter_error": alter_error, "published_s": published_s, "timeline": timeline, "stalls": stalls,
              "errors": errors, "settle_s": settle_s}

    if args.index == "plain":
        return finish(args, nodes, shards, before_state, after_state, load_s, refusals, alter_s, total,
                      samples, other, alter_begin, alter_end, replicas, **common)
    if args.index == "unique":
        checked, mismatched, extra = unique_checks(c, nodes, total, jobs, alter_s)
        return finish(args, nodes, shards, before_state, after_state, load_s, refusals, alter_s, total,
                      samples, other, alter_begin, alter_end, replicas, checked, mismatched, extra=extra,
                      **common)
    # Exact: the index's live entries are the table's rows, on all shards.
    digests = [ev(n, DIGEST, [f"{DB}.t"]) for n in nodes]
    rows = [sum(d["rows"][i] for d in digests) % (2 ** 32 if i else 2 ** 63) for i in range(3)]
    entries = [sum(d["entries"][i] for d in digests) % (2 ** 32 if i else 2 ** 63) for i in range(3)]
    # And through SQL: the first page of every key, eventual against strong.
    q(c, "SET tarn_lazy_read_your_writes=ON")
    mismatched, checked = [], rows[0]
    if rows != entries:
        mismatched.append({"rows": rows, "entries": entries})
    for k in range(KEYS):
        sql = f"SELECT tenant_id,id FROM t WHERE k={k} ORDER BY n,id LIMIT 100"
        q(c, "SET tarn_lazy_consistency='strong'")
        strong = q(c, sql)
        q(c, "SET tarn_lazy_consistency='eventual'")
        lazy = q(c, sql)
        if lazy != strong:
            mismatched.append({"k": k, "strong": len(strong), "lazy": len(lazy)})
    return finish(args, nodes, shards, before_state, after_state, load_s, refusals, alter_s, total,
                  samples, other, alter_begin, alter_end, replicas, checked, mismatched, **common)


def unique_checks(c, nodes, total, jobs, alter_s):
    """After a unique key's build: whether its entries are exactly the rows'
    over every shard, with no conflicts space left; whether a sample of rows
    is found by its values; whether a duplicate is refused. And the job's
    phases, with the backfill's rate. (rows checked, mismatches, extra)"""
    digests = [ev(n, UNIQUE_DIGEST, [f"{DB}.t"]) for n in nodes]
    rows = [sum(d["rows"][i] for d in digests) % (2 ** 32 if i else 2 ** 63) for i in range(3)]
    entries = [sum(d["entries"][i] for d in digests) % (2 ** 32 if i else 2 ** 63) for i in range(3)]
    mismatched = []
    if rows != entries:
        mismatched.append({"rows": rows, "entries": entries})
    spaces = [d["spaces"] or [] for d in digests]   # (an empty table encodes as [])
    conflicts = [d["conflicts"] or [] for d in digests]
    if any(len(s) != 1 for s in spaces) or any(conflicts):
        mismatched.append({"spaces": spaces, "conflicts": conflicts})
    rnd = random.Random(292)
    sample = sorted(rnd.sample(range(1, total + 1), 300))
    values = {}
    for rid in sample:
        values[rid] = q(c, "SELECT n FROM t WHERE tenant_id=%s AND id=%s", (rid % 997, rid))[0][0]
        by_value = q(c, "SELECT tenant_id,id FROM t WHERE n=%s", (values[rid],))
        if by_value != ((rid % 997, rid),):
            mismatched.append({"id": rid, "n": values[rid], "found": by_value})
    dup = None
    try:
        # (another tenant's row, with the first sampled row's value)
        q(c, "INSERT INTO t VALUES (%s,%s,0,%s,'dup')", ((sample[0] + 1) % 997, 10 * total, values[sample[0]]))
    except pymysql.MySQLError as e:
        dup = e.args[0]
    if dup != 1062:
        mismatched.append({"duplicate": dup})
    steps = job_steps(jobs, alter_s)
    fill_s = sum(s for phase, s in steps if phase == "backfill")
    extra = {
        "job_phases": jobs, "job_steps": steps, "fill_s": round(fill_s, 1),
        "fill_rows_per_s": round(total / fill_s) if fill_s else None,
        "index_spaces": spaces, "lookups_checked": len(values), "duplicate_refused": dup == 1062,
    }
    return rows[0], mismatched, extra


def wait_published(c, index, timeout):
    """When SHOW INDEX first lists the index (monotonic clock), polling each
    second."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            if any(row[2] == index for row in q(c, "SHOW INDEX FROM t")):
                return time.monotonic()
        except pymysql.MySQLError:
            pass
        assert time.monotonic() < deadline, "the ALTER did not complete in the background"
        time.sleep(1)


# Each shard's new plain index holds as many entries as the table has rows.
PLAIN_COUNT = """
-- A vinyl count scans; rows a copy just wrote are all in memory, where the
-- scan does not yield: allow it longer than a fiber slice.
require('fiber').set_slice({warn = 60, err = 600})
local sp = box.space[...]
local idx = sp.index.lk
return {rows = sp.index[0]:count(), entries = idx and idx:count() or box.NULL}
"""


def finish(args, nodes, shards, before_state, after_state, load_s, refusals, alter_s, total,
           samples, other, alter_begin, alter_end, replicas, checked=None, mismatched=None,
           alter_error=None, published_s=None, timeline=None, stalls=None, errors=None, extra=None,
           settle_s=None):
    if mismatched is None:
        counts = [ev(n, PLAIN_COUNT, [f"{DB}.t"]) for n in nodes]
        mismatched = [c for c in counts if c["entries"] != c["rows"]]
        checked = sum(c["rows"] for c in counts)

    def phases(xs):
        return {
            "before": summary([x for x in xs if x[0] < alter_begin]),
            "during": summary([x for x in xs if alter_begin <= x[0] < alter_end], alter_begin),
            "after": summary([x for x in xs if x[0] >= alter_end]),
        }
    server = os.environ.get("TARN_BIN", os.path.join(ROOT, "target", "release", "tarn-server"))
    result = {
        "issue": 292 if args.index == "unique" else 184,
        "date": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "source": subprocess.run(["git", "-C", ROOT, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
        "dirty": bool(subprocess.run(["git", "-C", ROOT, "status", "--porcelain", "--untracked-files=no"],
                                     capture_output=True, text=True).stdout.strip()),
        "tarn_server_sha256": sha256(server) if os.path.exists(server) else None,
        "tarantool": subprocess.run([os.environ.get("TARANTOOL", "tarantool"), "--version"],
                                    capture_output=True, text=True).stdout.splitlines()[0],
        "host": {"cpus": os.cpu_count(), "machine": platform.machine(), "kernel": platform.release()},
        "engine": args.engine, "index": args.index, "shards": shards, "replicas_per_shard": replicas,
        "rows_per_shard": args.rows_per_shard,
        "alter_check_rows": before_state[0]["check_rows"], "writer_rate": args.rate,
        "other_rate": args.other_rate,
        "load_s": round(load_s, 1), "load_vinyl_refusals": refusals,
        "settle_s": round(settle_s, 1) if settle_s is not None else None, "alter_s": round(alter_s, 2),
        "alter_error": alter_error, "published_s": round(published_s, 1) if published_s is not None else None,
        "leader_changes": [a["term"] - b["term"] for a, b in zip(after_state, before_state)],
        "build_rows_per_s": round(total / alter_s), "writes": phases(samples),
        "other_writes": phases(other) if args.other_rate else None,
        "nodes_before": before_state, "nodes_after": after_state,
        "copy_phases": timeline, "slow_probes": (stalls or [])[:40],
        "write_errors": (errors or [])[:20],
        "checked_rows": checked, "mismatched_keys": mismatched,
    }
    result.update(extra or {})
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
        f.write("\n")
    shown = ["engine", "index", "replicas_per_shard", "rows_per_shard", "alter_s", "alter_error", "published_s",
             "leader_changes", "build_rows_per_s", "writes", "other_writes", "checked_rows", "mismatched_keys"]
    shown += [k for k in ("job_steps", "fill_s", "fill_rows_per_s") if k in result]
    print(json.dumps({k: result[k] for k in shown}, indent=1), flush=True)
    assert not mismatched, mismatched
    assert result["writes"]["during"]["writes"] > 0, "no write completed during the ALTER"


if __name__ == "__main__":
    main()
