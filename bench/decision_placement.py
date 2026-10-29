#!/usr/bin/env python3
"""Matched, repeated decision-placement measurements on owned RF1 clusters.

Both release builds must differ only in the placement change. Each sample
starts a fresh three-shard memtx cluster with fsync WAL, the same native
module, worker count, and transactions. Reports record counts, WAL rows and
file growth, GC deletions and SQL throughput; this is not an RF3 comparison.
The shorter, matched retention is ONLY for these owned single-clock fixtures.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tests' / 'sql'))
from decisiontest import Fixture, check_records, snapshots, wait_for
from lazytest import connect, ev, q


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def port_ready(port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=.2):
            return True
    except OSError:
        return False


def run_sample(args, label, repeat):
    nodes = ['127.0.0.1:' + str(args.base + 10 * i + 1) for i in range(1, 4)]
    endpoint = '127.0.0.1:' + str(args.port)
    env = dict(os.environ, TARN_BIN=getattr(args, label + '_bin'), TARN_NATIVE=args.native,
               TARN_MAX_TXN_AGE=str(args.expiry_seconds), TARN_CLOCK_SKEW=str(args.clock_skew_seconds))
    cluster = ROOT / 'scripts' / 'cluster.sh'
    with tempfile.TemporaryDirectory(prefix='tarn-placement-') as temp:
        work = Path(temp)
        log = work / 'driver.log'
        with log.open('w') as output:
            gateway = None
            try:
                subprocess.run([str(cluster), 'start', '-d', temp, '-n', '3', '-r', '1', '-s', '0',
                                '-b', str(args.base), '-p', str(args.port), '-m', '256', '-w', 'fsync'],
                               env=env, stdout=output, stderr=subprocess.STDOUT, check=True)
                for node in nodes:
                    ev(node, "tarn.cfg.recovery_interval=.1; return true")
                config = work / 'cluster.json'
                cfg = json.loads(config.read_text())
                cfg['max_txn_age'] = args.expiry_seconds
                config.write_text(json.dumps(cfg))
                gateway = subprocess.Popen([env['TARN_BIN'], '--config', str(config), '--listen', endpoint,
                                            '--threads', '2', '--pool', '2', '--node-id', '1', '--allow-all'],
                                           env=env, stdout=output, stderr=subprocess.STDOUT)
                (work / 'tarn1.pid').write_text(str(gateway.pid))
                wait_for(lambda: port_ready(args.port), 10, 'owned benchmark gateway')
                f = Fixture(endpoint, nodes, args.workers)
                with connect(endpoint, f.db) as c:
                    for _ in range(20):
                        q(c, 'UPDATE t SET v = v + 1 WHERE id = %s', (f.keys[0][0],))
                    for keys in f.keys:
                        for key in keys:
                            q(c, 'UPDATE t SET v = 0 WHERE id = %s', (key,))
                before = snapshots(nodes)
                assert not any(s['parts'] or s['txns'] for s in before), 'setup made distributed records'

                def worker(index):
                    latencies = []
                    with connect(endpoint, f.db) as c:
                        for _ in range(args.transactions // args.workers):
                            start = time.perf_counter()
                            f.transaction(c, worker=index)
                            latencies.append(time.perf_counter() - start)
                    return latencies

                start = time.perf_counter()
                with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
                    latencies = [v for batch in pool.map(worker, range(args.workers)) for v in batch]
                elapsed = time.perf_counter() - start
                # All records must be captured before any workload expiry.
                assert elapsed < args.expiry_seconds / 2, ('increase --expiry-seconds', elapsed)
                after = snapshots(nodes)
                counts, txids = check_records(before, after, args.transactions, hashed=label == 'after')
                assert set(f.values().values()) == {args.transactions // args.workers}
                assert all(len(s['parts']) == args.transactions for s in after)

                def collected():
                    state = snapshots(nodes)
                    return state if not any(txids.intersection(s['parts']) or txids.intersection(s['txns'])
                                            for s in state) else None

                final = wait_for(collected, args.expiry_seconds + 2 * args.clock_skew_seconds + 10,
                                 'complete eligible record GC')
                gc_txn = [s['gc_txn'] - b['gc_txn'] for b, s in zip(before, final)]
                gc_part = [s['gc_part'] - b['gc_part'] for b, s in zip(before, final)]
                assert gc_txn == counts and gc_part == [args.transactions] * 3, (gc_txn, gc_part, counts)
                latency = sorted(latencies)
                result = {
                    'label': label, 'repeat': repeat, 'transactions': len(latencies), 'workers': args.workers,
                    'elapsed_seconds': elapsed, 'transactions_per_second': len(latencies) / elapsed,
                    'transaction_p50_ms': latency[len(latency) // 2] * 1000,
                    'transaction_p99_ms': latency[min(len(latency) - 1, int(len(latency) * .99))] * 1000,
                    'decision_records': counts, 'participant_records': [len(s['parts']) for s in after],
                    'wal_rows_workload': [s['lsn'] - b['lsn'] for b, s in zip(before, after)],
                    'wal_bytes_workload': [s['wal_bytes'] - b['wal_bytes'] for b, s in zip(before, after)],
                    'gc_decision_deletes': gc_txn, 'gc_participant_deletes': gc_part,
                    'wal_rows_gc': [s['lsn'] - b['lsn'] for b, s in zip(after, final)],
                    'wal_bytes_gc': [s['wal_bytes'] - b['wal_bytes'] for b, s in zip(after, final)],
                    'tarantool_version': after[0]['version'],
                    'decision_id_digest': hashlib.sha256('\n'.join(sorted(txids)).encode()).hexdigest(),
                    'placement_and_tuple_checks': 'passed',
                }
                print(json.dumps(result), flush=True)
                return result
            except Exception:
                output.flush()
                evidence = Path(args.out).with_suffix('.failure.log')
                evidence.write_text(log.read_text())
                raise
            finally:
                subprocess.run([str(cluster), 'stop', '-d', temp], env=env,
                               stdout=output, stderr=subprocess.STDOUT, check=False)
                if gateway:
                    gateway.wait(timeout=10)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    for name in ['before-bin', 'after-bin', 'native', 'before-ref', 'after-ref', 'out']:
        ap.add_argument('--' + name, required=True)
    ap.add_argument('--transactions', type=int, default=600)
    ap.add_argument('--workers', type=int, default=4)
    ap.add_argument('--repeats', type=int, default=3)
    ap.add_argument('--expiry-seconds', type=float, default=15)
    ap.add_argument('--clock-skew-seconds', type=float, default=.25,
                    help='fixed startup bound; must also cover peer sampling uncertainty')
    ap.add_argument('--base', type=int, default=5700)
    ap.add_argument('--port', type=int, default=5800)
    args = ap.parse_args()
    assert args.workers > 0 and args.transactions > 0 and args.transactions % args.workers == 0
    assert args.repeats >= 3 and args.expiry_seconds >= 5 and args.clock_skew_seconds >= 0
    for name in ['before_bin', 'after_bin', 'native', 'out']:
        setattr(args, name, str(Path(getattr(args, name)).resolve()))
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    result = {
        'complete': False, 'platform': platform.platform(), 'cpu_count': os.cpu_count(),
        'before_ref': args.before_ref, 'after_ref': args.after_ref,
        'binary_sha256': {name: sha256(getattr(args, name + '_bin')) for name in ['before', 'after']},
        'native_sha256': sha256(args.native),
        'topology': '3 shards x 1 replica, memtx, WAL fsync, one two-thread gateway, pool 2',
        'expiry_seconds': args.expiry_seconds, 'clock_skew_seconds': args.clock_skew_seconds,
        'retention_seconds': args.expiry_seconds + 2 * args.clock_skew_seconds,
        'scope': 'single-host placement workload; no RF3 or end-to-end speedup claim',
        'samples': [],
    }
    try:
        for repeat in range(1, args.repeats + 1):
            # Alternate order to avoid always giving one build the warm host.
            for label in (['before', 'after'] if repeat % 2 else ['after', 'before']):
                result['samples'].append(run_sample(args, label, repeat))
                Path(args.out).write_text(json.dumps(result, indent=2) + '\n')
        result['complete'] = True
    finally:
        Path(args.out).write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
