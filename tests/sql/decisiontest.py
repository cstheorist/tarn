#!/usr/bin/env python3
"""Decision placement on an isolated three-shard cluster (issue #132).

The fixture/record checks are also used by bench/decision_placement.py.
Native probes use the bounded lazy-index test RPC helper. Never run this on a shared cluster:
the fault case deliberately kills the gateway started by tests/decision.sh.
"""
import argparse
import os
from pathlib import Path
import signal
import threading
import time
import uuid

import pymysql

from lazytest import connect, ev, q
from movetest import bucket_of, crc32c


SNAPSHOT = r"""
local fio = require('fio')
local bytes = 0
for _, path in ipairs(fio.glob(fio.pathjoin(box.cfg.wal_dir or '.', '*.xlog'))) do
    bytes = bytes + fio.stat(path).size
end
local txns, parts = {}, {}
for _, r in box.space._tarn_txn:pairs() do
    txns[r.txid] = {status = r.status, parts = r.parts}
end
for _, p in box.space._tarn_part:pairs() do
    parts[p.txid] = {state = p.state, decision = p.dshard, parts = p.parts}
end
return {id = tarn.cfg.shard_id, version = box.info.version,
        txns = txns, parts = parts, lsn = box.info.lsn, wal_bytes = bytes,
        gc_txn = tarn.stats.gc_txn, gc_part = tarn.stats.gc_part}
"""


def snapshots(nodes):
    return [ev(node, SNAPSHOT) for node in nodes]


def wait_for(fn, seconds, what):
    deadline = time.monotonic() + seconds
    while True:
        result = fn()
        if result:
            return result
        if time.monotonic() >= deadline:
            raise AssertionError(f"timeout waiting for {what}")
        time.sleep(.05)


class Fixture:
    def __init__(self, endpoint, nodes, workers=1):
        self.endpoint, self.nodes = endpoint, nodes
        self.db = 'decision_' + uuid.uuid4().hex[:12]
        self.keys = [[] for _ in nodes]
        with connect(endpoint, db=None) as c:
            q(c, f'CREATE DATABASE {self.db}')
            c.select_db(self.db)
            q(c, 'CREATE TABLE t (id INT PRIMARY KEY, v BIGINT NOT NULL)')
            count = int(dict(q(c, "SHOW STATUS LIKE 'Tarn_buckets'"))['Tarn_buckets'])
            i = 0
            while min(map(len, self.keys)) < workers:
                owner = (bucket_of(i, count) - 1) * len(nodes) // count
                if len(self.keys[owner]) < workers:
                    self.keys[owner].append(i)
                    # Each setup insert stays on the atomic local path, so
                    # baseline protocol records do not pollute GC evidence.
                    q(c, 'INSERT INTO t VALUES (%s, 0)', (i,))
                i += 1
        self.ids = [s['id'] for s in snapshots(nodes)]
        assert self.ids == sorted(self.ids) and len(set(self.ids)) == len(nodes)
        for node, keys in zip(nodes, self.keys):
            assert ev(node, "local s=box.space[...]; return s:len()", [self.db + '.t']) == workers, keys

    def transaction(self, c, worker=0, writers=None):
        writers = set(range(len(self.nodes))) if writers is None else set(writers)
        q(c, 'BEGIN')
        for i, keys in enumerate(self.keys):
            if i in writers:
                q(c, 'UPDATE t SET v = v + 1 WHERE id = %s', (keys[worker],))
            else:
                q(c, 'SELECT v FROM t WHERE id = %s', (keys[worker],))
        q(c, 'COMMIT')

    def values(self, endpoint=None):
        with connect(endpoint or self.endpoint, self.db) as c:
            return dict(q(c, 'SELECT id, v FROM t'))

    def close(self, endpoint=None):
        with connect(endpoint or self.endpoint, db=None) as c:
            q(c, f'DROP DATABASE IF EXISTS {self.db}')


def check_records(before, after, transactions, writers=None, hashed=True):
    ids = [s['id'] for s in after]
    candidates = ids if writers is None else sorted(writers)
    decisions = {}
    counts = []
    for old, new in zip(before, after):
        added = set(new['txns']) - set(old['txns'])
        counts.append(len(added))
        for txid in added:
            assert txid not in decisions, ('duplicate decision record', txid)
            record = new['txns'][txid]
            assert record['status'] == 'committed' and sorted(record['parts']) == ids, record
            expected = candidates[crc32c(txid.encode()) % len(candidates)] if hashed else candidates[0]
            assert new['id'] == expected, (txid, new['id'], expected)
            decisions[txid] = new['id']
    assert len(decisions) == transactions, (len(decisions), transactions)
    for old, new in zip(before, after):
        added = set(new['parts']) - set(old['parts'])
        assert added == decisions.keys(), (new['id'], len(added), len(decisions))
        for txid in added:
            p = new['parts'][txid]
            assert p['state'] == 'applied' and p['decision'] == decisions[txid], (txid, p)
            assert sorted(p['parts']) == ids, p
    return counts, set(decisions)


# Hold the durable YES on its decision writer, only for this table and only
# when it is not the first shard. The bounded lease restores itself even if
# the test exits; recovery is paused until the coordinator has been killed.
INSTALL = r"""
local owner, name, first = ...
assert(tarn.cfg.test_hooks, 'decision faults require test_hooks')
assert(rawget(_G, '__tarn_decision_test') == nil, 'decision fault lease exists')
local fiber, clock = require('fiber'), require('clock')
local s = {owner = owner, sid = assert(box.space[name]).id,
           original = tarn_prepare, recovery = tarn.cfg.recovery_after,
           hold = true, txid = false, expires = clock.monotonic() + 45}
s.wrapper = function(txid, dshard, parts, writes, ...)
    local mine = false
    for _, w in ipairs(writes or {}) do if w[2] == s.sid then mine = true end end
    local r = s.original(txid, dshard, parts, writes, ...)
    if mine and r[1] == 'yes' and dshard == tarn.cfg.shard_id and dshard ~= first then
        s.txid, s.decision = txid, dshard
        while s.hold and clock.monotonic() < s.expires do fiber.sleep(.01) end
    end
    return r
end
function s.restore()
    s.hold = false
    if rawget(_G, '__tarn_decision_test') ~= s then return end
    assert(tarn_prepare == s.wrapper, 'decision wrapper changed')
    tarn_prepare = s.original
    tarn.cfg.recovery_after = s.recovery
    rawset(_G, '__tarn_decision_test', nil)
end
tarn_prepare = s.wrapper
tarn.cfg.recovery_after = 3600
rawset(_G, '__tarn_decision_test', s)
fiber.create(function() fiber.sleep(45); s.restore() end)
return true
"""


def coordinator_crash(f, other_endpoint, cluster_dir):
    owner = uuid.uuid4().hex
    installed, successes, errors = [], [], []
    worker = None
    pid = int((Path(cluster_dir) / 'tarn1.pid').read_text())
    assert pid > 1 and (Path(cluster_dir) / 'tarn1.sh').is_file()
    before = f.values()
    try:
        for node in f.nodes:
            installed.append(node)
            ev(node, INSTALL, [owner, f.db + '.t', f.ids[0]])

        def commit_until_held():
            try:
                with connect(f.endpoint, f.db) as c:
                    for _ in range(30):
                        f.transaction(c)
                        successes.append(True)
            except Exception as e:
                errors.append(e)

        worker = threading.Thread(target=commit_until_held, daemon=True)
        worker.start()

        def held():
            for node in f.nodes:
                s = ev(node, "local s=rawget(_G,'__tarn_decision_test'); "
                       "return s and s.txid and {id=s.txid, decision=s.decision} or false")
                if s:
                    return s
            return None

        h = wait_for(held, 12, 'durable YES on a non-first decision writer')
        assert h['decision'] != f.ids[0], h

        def all_prepared():
            state = snapshots(f.nodes)
            return state if all(s['parts'].get(h['id'], {}).get('state') == 'prepared' for s in state) else None

        state = wait_for(all_prepared, 8, 'every durable participant')
        assert all(s['parts'][h['id']]['decision'] == h['decision'] for s in state)
        expected = {key: value + len(successes) for key, value in before.items()}
        for node, keys in zip(f.nodes, f.keys):
            raw = ev(node, "local name, key = ...; return box.space[name]:get{key}[2]", [f.db + '.t', keys[0]])
            assert raw == expected[keys[0]], ('uncommitted tuple visible', raw, expected)
        os.kill(pid, signal.SIGKILL)
    finally:
        for node in installed:
            ev(node, "local owner=...; local s=rawget(_G,'__tarn_decision_test'); "
               "if s then assert(s.owner==owner); s.restore() end; return true", [owner])
        if worker:
            worker.join(12)
            assert not worker.is_alive(), 'SQL worker stayed blocked after gateway crash'
    assert (len(errors) == 1 and isinstance(errors[0], pymysql.MySQLError)
            and errors[0].args[0] in (2006, 2013)), errors

    def resolved():
        state = snapshots(f.nodes)
        return state if all(s['parts'].get(h['id'], {}).get('state') == 'applied' for s in state) else None

    state = wait_for(resolved, 12, 'recorded decision recovery after coordinator crash')
    d = next(s for s in state if s['id'] == h['decision'])
    assert d['txns'][h['id']]['status'] == 'committed', d
    assert f.values(other_endpoint) == {key: value + 1 for key, value in expected.items()}
    print('coordinator crash recovered exactly once at decision shard', h['decision'])


def lost_prepare_reply(f, arm_file):
    before, values = snapshots(f.nodes), f.values()
    Path(arm_file).touch()
    outcome = 'acknowledged'
    with connect(f.endpoint, f.db) as c:
        try:
            f.transaction(c)
        except pymysql.MySQLError as e:
            assert e.args[0] == 1180, e
            outcome = 'unknown'
    # There is one attempt. A later observation resolves an unknown outcome;
    # it is never permission to replay the mutation.
    wait_for(lambda: f.values() == {key: value + 1 for key, value in values.items()},
             12, 'one application after lost PREPARE reply')
    check_records(before, snapshots(f.nodes), 1)
    print('lost PREPARE reply:', outcome, ', applied exactly once')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--tarn', required=True, help='two owned gateways, comma separated')
    ap.add_argument('--shards', required=True, help='three leaders in ascending durable-ID order')
    ap.add_argument('--cluster-dir', required=True, help='owned cluster started by tests/decision.sh')
    ap.add_argument('--proxy-arm-file', required=True)
    args = ap.parse_args()
    endpoints, nodes = args.tarn.split(','), args.shards.split(',')
    assert len(endpoints) == 2 and len(nodes) == 3
    f = Fixture(endpoints[0], nodes)
    cleanup_endpoint = endpoints[0]
    try:
        before = snapshots(nodes)
        with connect(endpoints[0], f.db) as c:
            for _ in range(120):
                f.transaction(c)
        counts, _ = check_records(before, snapshots(nodes), 120)
        assert min(counts) > 15, counts
        assert set(f.values().values()) == {120}
        print('common writer decision counts:', counts)
        before = snapshots(nodes)
        with connect(endpoints[0], f.db) as c:
            for _ in range(12):
                f.transaction(c, writers={1})
        counts, _ = check_records(before, snapshots(nodes), 12, writers={f.ids[1]})
        assert counts == [0, 12, 0], counts
        print('read-only participants never hold decisions:', counts)
        before = snapshots(nodes)
        with connect(endpoints[0], f.db) as c:
            q(c, 'UPDATE t SET v = v + 1 WHERE id = %s', (f.keys[0][0],))
        after = snapshots(nodes)
        assert all(s['txns'] == b['txns'] and s['parts'] == b['parts'] for b, s in zip(before, after))
        print('atomic local write made no distributed records')
        lost_prepare_reply(f, args.proxy_arm_file)
        cleanup_endpoint = endpoints[1]
        coordinator_crash(f, endpoints[1], args.cluster_dir)
    finally:
        f.close(cleanup_endpoint)
    print('DECISION PLACEMENT TEST PASSED')


if __name__ == '__main__':
    main()
