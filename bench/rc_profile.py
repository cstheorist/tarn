#!/usr/bin/env python3
"""Bounded RF3/fsync READ COMMITTED baseline with exact per-key accounting.

This is a single-host closed-loop profile, not scale-out evidence. Sampling
profiles and ordinary timings are separate phases. Every attempt is retained;
uncertain mutations are never retried. A failed sample stays failed.
"""
import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import threading
import time

import pymysql

DB = 'rc186'
METRICS = """
local misc=require('misc')
return {info=tarn_info(), metrics=misc.getmetrics(), calls=box.stat().CALL.total,
 wal=box.cfg.wal_mode, quorum=box.cfg.replication_synchro_quorum,
 engine=box.space['rc186.t'].engine, pid=box.info.pid}
"""


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def query(c, sql, args=None):
    with c.cursor() as q:
        q.execute(sql, args)
        return q.fetchall()


def connect(address, database=DB):
    host, port = address.rsplit(':', 1)
    return pymysql.connect(host=host, port=int(port), user='root', password='',
                           database=database, autocommit=True, charset='utf8mb4',
                           connect_timeout=5, read_timeout=15, write_timeout=5)


def evaluate(endpoints, code):
    script = """
local out={}
for _,address in ipairs(require('json').decode(os.getenv('RC186_ENDPOINTS'))) do
 local c=require('net.box').connect('tarn:tarn@'..address,{wait_connected=5})
 assert(c:is_connected(),address)
 out[#out+1]={address=address,result=c:eval(os.getenv('RC186_CODE'))}
 c:close()
end
print(require('json').encode(out)); os.exit(0)
"""
    result = subprocess.run(['tarantool', '-e', script], text=True, capture_output=True,
                            timeout=30, stdin=subprocess.DEVNULL,
                            env=dict(os.environ, RC186_ENDPOINTS=json.dumps(endpoints), RC186_CODE=code))
    if result.returncode:
        raise RuntimeError(f'diagnostic RPC exited {result.returncode}: {result.stderr[-12000:]}')
    return json.loads(result.stdout.strip().splitlines()[-1])


def cpu(pid):
    # TX thread and whole process separately; Linux clock tick precision is retained.
    def ticks(path):
        fields = Path(path).read_text().rsplit(')', 1)[1].split()
        return int(fields[11]) + int(fields[12])
    return dict(tx_ticks=ticks(f'/proc/{pid}/task/{pid}/stat'),
                process_ticks=ticks(f'/proc/{pid}/stat'))


def percentile(values, fraction):
    values = sorted(values)
    return values[min(len(values) - 1, int(fraction * len(values)))] if values else None


def diagnostic(record, name, operation):
    """A failed diagnostic must not prevent independent effect accounting."""
    try:
        return operation()
    except Exception as exc:
        record.setdefault('diagnostic_errors', []).append(dict(stage=name, error=repr(exc)))
        return None


def reconcile(records, counters, ledger):
    """Exact identities matter; equal aggregate sums are insufficient."""
    acknowledged = {r['token']: r['key'] for r in records if r['outcome'] == 'ok'}
    assert len(acknowledged) == sum(r['outcome'] == 'ok' for r in records), 'duplicate acknowledgement'
    observed = dict(ledger)
    assert len(observed) == len(ledger), 'duplicate observed token'
    assert observed == acknowledged, 'ledger differs from acknowledged identities'
    expected = collections.Counter(acknowledged.values())
    assert dict(counters) == {k: expected[k] for k in range(8)}, 'per-key effects differ'
    assert all(r['outcome'] in ('ok', 'refused') for r in records), 'unknown outcome'
    return dict(acknowledged=len(acknowledged), refused=len(records) - len(acknowledged),
                exact_ledger=True, exact_counters=True)


def sample(args, number, shape, profiled, endpoints, output, evidence, warmup=False):
    sample_started = time.monotonic()
    admin = connect(args.sql)
    query(admin, 'TRUNCATE TABLE ledger')
    query(admin, 'UPDATE t SET v=0')
    before = evaluate(endpoints, METRICS)
    by_address = {row['address']: row['result'] for row in before}
    leaders = [row['address'] for row in before if not row['result']['info']['ro']]
    assert len(leaders) == 2
    for row in before:
        r = row['result']
        assert r['wal'] == 'fsync' and r['quorum'] == 'N / 2 + 1' and r['engine'] == args.engine, r
        if not r['info']['ro']:
            n = r['info']['native']
            assert (isinstance(n, dict) and n.get('ready') == 1) if args.native == 'enabled' else n is False
    pids = [r['result']['pid'] for r in before]
    if profiled:
        source = Path(__file__).with_name('rc_guest_profile.lua').read_text()
        profile_bounds = evaluate(leaders,
            'rc186_profile=assert(loadstring(' + json.dumps(source) + '))(); '
            'return rc186_profile.start(' + str(args.seconds) + ')')
    else:
        profile_bounds = []
    cpus_before = {str(pid): cpu(pid) for pid in pids}
    records, failures = [], []
    clients = 1 if shape == 'uncontended' else 8
    start = [None]
    barrier = threading.Barrier(clients + 1, timeout=20,
                                action=lambda: start.__setitem__(0, time.monotonic()))
    cancel = threading.Event()

    def worker(worker_id):
        c = None
        try:
            c = connect(args.sql)
            query(c, "SET SESSION tx_isolation='READ-COMMITTED'")
            query(c, 'SET SESSION innodb_lock_wait_timeout=1')
            barrier.wait()
            for seq in range(1, 50001):
                if cancel.is_set() or time.monotonic() >= start[0] + args.seconds:
                    break
                token = number * 1000000 + worker_id * 100000 + seq
                key = 0 if shape == 'hot' else worker_id
                attempt = dict(token=token, key=key, worker=worker_id, outcome='unknown')
                began = time.monotonic()
                try:
                    query(c, 'BEGIN')
                    query(c, 'UPDATE t SET v=v+1 WHERE tenant=1 AND id=%s', (key,))
                    query(c, 'INSERT INTO ledger VALUES (1,%s,%s)', (token, key))
                    query(c, 'COMMIT')
                    attempt['outcome'] = 'ok'
                except pymysql.MySQLError as exc:
                    attempt['error'] = list(exc.args)
                    if exc.args[0] in (1205, 1213):
                        # An explicit refusal is followed by rollback, never a retry.
                        query(c, 'ROLLBACK')
                        attempt['outcome'] = 'refused'
                    else:
                        cancel.set()
                finally:
                    attempt['latency_seconds'] = time.monotonic() - began
                    records.append(attempt)
            else:
                raise AssertionError('bounded per-worker attempt cap reached before duration')
        except Exception as exc:
            failures.append(repr(exc))
            cancel.set()
            barrier.abort()
        finally:
            if c is not None:
                c.close()

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(clients)]
    for thread in threads:
        thread.start()
    try:
        barrier.wait()
        join_deadline = start[0] + args.seconds + 35
        for thread in threads:
            thread.join(max(0, join_deadline - time.monotonic()))
    except Exception as exc:
        failures.append(repr(exc))
    finally:
        cancel.set()
    elapsed = time.monotonic() - (start[0] or sample_started)
    attempt_path = output.parent / f'{output.stem}-sample-{number}.jsonl'
    attempt_path.write_text(''.join(json.dumps(r, sort_keys=True) + '\n' for r in sorted(records, key=lambda r: r['token'])))
    record = dict(number=number, shape=shape, profiled=profiled, warmup=warmup, clients=clients,
                  scheduled_seconds=args.seconds, elapsed_seconds=elapsed,
                  before=before, cpu_before=cpus_before, profile_bounds=profile_bounds,
                  errors=failures, workers_live=any(t.is_alive() for t in threads),
                  attempts_file=attempt_path.name, attempts_sha256=sha(attempt_path), complete=False)
    evidence['samples'].append(record)
    # Persist completed attempts before any diagnostic that could fail after a
    # process loss. Preserve partial outcomes even when no metric/readback works.
    output.write_text(json.dumps(evidence, indent=2) + '\n')
    record['cpu_after'] = diagnostic(record, 'cpu', lambda: {str(pid): cpu(pid) for pid in pids})
    if profiled:
        record['profile'] = diagnostic(record, 'profile', lambda: evaluate(leaders,
            'return rc186_profile.stop()'))
    after = diagnostic(record, 'metrics', lambda: evaluate(endpoints, METRICS))
    record['after'] = after
    # Preserve readback even when reconciliation rejects it.
    record['readback'] = diagnostic(record, 'readback', lambda: dict(
        counters=query(admin, 'SELECT id,v FROM t WHERE tenant=1 ORDER BY id'),
        ledger=query(admin, 'SELECT token,k FROM ledger WHERE tenant=1 ORDER BY token')))
    admin.close()
    output.write_text(json.dumps(evidence, indent=2) + '\n')
    assert not record['workers_live'], 'workers did not settle; readback is not final'
    if record['readback'] is not None:
        record.update(reconcile(records, record['readback']['counters'], record['readback']['ledger']))
    assert not failures and not record.get('diagnostic_errors'), record
    if profiled:
        assert sum(r['result']['samples'] for r in record['profile']) > 0, 'no profiler samples'
        assert all(not r['result']['auto_stopped'] for r in record['profile']), 'profile exceeded its deadline'
    assert record['acknowledged'] > 0
    record['goodput_per_second'] = record['acknowledged'] / elapsed
    record['latency'] = {outcome: {f'p{int(p * 100)}': percentile(
        [r['latency_seconds'] for r in records if r['outcome'] == outcome], p) for p in (.5, .99)}
        for outcome in ('ok', 'refused', 'unknown')}
    record['deltas'] = []
    for row in after:
        a, b = row['result'], by_address[row['address']]
        assert a['info']['ro'] == b['info']['ro'], 'leader changed during baseline'
        record['deltas'].append(dict(address=row['address'], calls=a['calls'] - b['calls'],
            metrics={k: a['metrics'][k] - b['metrics'][k] for k in
                     ('gc_allocated', 'gc_freed', 'gc_steps_propagate', 'gc_steps_atomic',
                      'gc_steps_sweep', 'gc_steps_finalize')},
            stats={k: a['info']['stats'][k] - b['info']['stats'][k] for k in
                   ('plock_waits', 'deadlocks', 'lock_timeouts', 'commit_rc', 'commit_waits')}))
    record['complete'] = True
    output.write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps({k: record[k] for k in ('number', 'shape', 'profiled', 'acknowledged', 'refused', 'goodput_per_second')}), flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--sql', default='127.0.0.1:4000')
    ap.add_argument('--cluster-dir', required=True)
    ap.add_argument('--bin', required=True)
    ap.add_argument('--engine', choices=('memtx', 'vinyl'), required=True)
    ap.add_argument('--native', choices=('enabled', 'disabled'), required=True)
    ap.add_argument('--seconds', type=float, default=5)
    ap.add_argument('--repeats', type=int, default=5)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    assert 1 <= args.seconds <= 30 and 5 <= args.repeats <= 10
    config = json.loads((Path(args.cluster_dir) / 'cluster.json').read_text())
    assert len(config['shards']) == 2 and all(len(s['replicas']) == 3 for s in config['shards'])
    endpoints = [r for s in config['shards'] for r in s['replicas']]
    output = Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    evidence = dict(schema=1, complete=False, scope='single-host closed-loop RF3/fsync baseline',
                    source=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                    source_dirty=bool(subprocess.check_output(['git', 'status', '--porcelain'], text=True)),
                    binary_sha256=sha(args.bin), harness_sha256=sha(__file__),
                    profiler_sha256=sha(Path(__file__).with_name('rc_guest_profile.lua')),
                    native_sha256=sha(Path(args.bin).with_name('libtarn_storage.so')),
                    lua_sha256=sha('lua/tarn.lua'), host=platform.platform(),
                    cpu_tick_hz=os.sysconf('SC_CLK_TCK'), engine=args.engine, native=args.native,
                    configuration=config,
                    cluster_parameters=(Path(args.cluster_dir) / 'params').read_text(), samples=[])
    try:
        admin = connect(args.sql, None)
        query(admin, f'CREATE DATABASE {DB}')
        admin.select_db(DB)
        query(admin, f'CREATE TABLE t (tenant INT, id INT, v BIGINT, payload VARCHAR(256), PRIMARY KEY(tenant,id)) ENGINE={args.engine} PARTITION BY KEY(tenant)')
        query(admin, f'CREATE TABLE ledger (tenant INT, token BIGINT, k INT, PRIMARY KEY(tenant,token)) ENGINE={args.engine} PARTITION BY KEY(tenant)')
        for k in range(8):
            query(admin, 'INSERT INTO t VALUES (1,%s,0,%s)', (k, 'x' * 256))
        admin.close()
        number = 0
        warmup_args = argparse.Namespace(**vars(args))
        warmup_args.seconds = min(args.seconds, 3)
        for shape in ('uncontended', 'unrelated', 'hot'):
            number += 1
            sample(warmup_args, number, shape, False, endpoints, output, evidence, warmup=True)
        for profiled in (False, True):
            for repeat in range(args.repeats):
                shapes = ['uncontended', 'unrelated', 'hot']
                if repeat % 2:
                    shapes.reverse()
                for shape in shapes:
                    number += 1
                    sample(args, number, shape, profiled, endpoints, output, evidence)
        evidence['complete'] = True
    except BaseException as exc:
        evidence['failure'] = repr(exc)
        raise
    finally:
        output.write_text(json.dumps(evidence, indent=2) + '\n')


if __name__ == '__main__':
    main()
