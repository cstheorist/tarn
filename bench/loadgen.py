#!/usr/bin/env python3
"""Bounded scheduled-arrival SQL load with a disk-backed exact-effects audit.

Runs only against an explicitly prepared, exclusively owned benchmark database.
No mutation retries, automatic database deletion, or physical-host claims.
"""
import argparse
import collections
from contextlib import contextmanager
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import queue
import random
import re
import resource
import sqlite3
import threading
import time
import uuid

import pymysql


@contextmanager
def error_states():
    """Keep the wire SQLSTATE which PyMySQL otherwise drops from exceptions."""
    original = pymysql.err.raise_mysql_exception
    def capture(packet):
        try:
            original(packet)
        except pymysql.MySQLError as error:
            error.sqlstate = packet[4:9].decode('ascii',errors='replace') if len(packet)>=9 and packet[3:4]==b'#' else None
            raise
    pymysql.err.raise_mysql_exception = capture
    try:
        yield
    finally:
        pymysql.err.raise_mysql_exception = original


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def file_sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1048576), b''):
            digest.update(chunk)
    return digest.hexdigest()


def sql(connection, statement, parameters=None):
    with connection.cursor() as cursor:
        cursor.execute(statement, parameters)
        return cursor.fetchall() if cursor.description else cursor.rowcount


def connect(args):
    host, port = args.endpoint.rsplit(':', 1)
    settings = dict(host=host.strip('[]'), port=int(port), user=args.user,
                    password=os.environ.get(args.password_env, ''),
                    autocommit=True, charset='utf8mb4', connect_timeout=5,
                    read_timeout=args.rpc_timeout, write_timeout=args.rpc_timeout)
    if args.tls_ca:
        settings.update(ssl_ca=args.tls_ca, ssl_verify_cert=True, ssl_verify_identity=True)
    return pymysql.connect(**settings)


def fixture_keys(token, rows, width, distribution, hot_keys, seed):
    count = hot_keys if distribution == 'hot' else rows
    return sorted(random.Random(seed ^ token).sample(range(count), width))


class Journal:
    """Bound raw evidence bytes and serialize each record before writing it."""
    def __init__(self, root, limit):
        self.root, self.limit, self.used = root, limit, 0
        self.lock = threading.Lock()
        self.sealed = False

    def append(self, stream, value):
        raw = encoded(value)
        with self.lock:
            if self.sealed or self.used + len(raw) > self.limit:
                raise RuntimeError('raw evidence byte limit reached; run remains incomplete')
            self.used += len(raw)
            stream.write(raw)
            stream.flush()

    def seal(self):
        with self.lock:
            self.sealed = True


def planned(args):
    count = math.ceil((args.warmup + args.seconds) * args.rate)
    if count > args.max_operations:
        raise ValueError('offered schedule exceeds --max-operations')
    return count


def execute(connection, args, token, keys, run_id):
    """One attempt. An uncertain RPC is never followed by another mutation."""
    phase = 'begin'
    try:
        if args.workload == 'point':
            row = sql(connection, 'SELECT k,n,pad FROM t WHERE k=%s', (keys[0],))
            if len(row) != 1 or row[0][0] != keys[0] or row[0][1] != 0 or bytes(row[0][2]) != b'x' * args.row_bytes:
                raise AssertionError('point result differs from the prepared fixture')
            return dict(outcome='acknowledged')
        sql(connection, 'BEGIN')
        phase = 'mutations'
        for key in keys:
            if sql(connection, 'UPDATE t SET n=n+1 WHERE k=%s', (key,)) != 1:
                raise AssertionError('update did not affect exactly one fixture row')
            if sql(connection, 'INSERT INTO ledger VALUES (%s,%s,%s)', (run_id, token, key)) != 1:
                raise AssertionError('ledger insert did not affect exactly one row')
        phase = 'commit'
        sql(connection, 'COMMIT')
        return dict(outcome='acknowledged')
    except pymysql.MySQLError as error:
        code = error.args[0] if error.args else None
        state = getattr(error,'sqlstate',None)
        result = dict(outcome='unknown', error_code=code, sqlstate=state, phase=phase)
        # Only the explicitly confirmed conflict/timeout contract qualifies
        # as refused, and only after the rollback response is received.
        if (code==1213 and state=='40001') or (code==1205 and state in ('HY000','40001')):
            try:
                sql(connection, 'ROLLBACK')
                result['outcome'] = 'refused'
            except Exception:
                result['rollback_failed'] = True
        return result
    except Exception as error:
        # An assertion or unexpected driver error is not a successful sample.
        # Do not guess whether a previously issued mutation completed.
        return dict(outcome='unknown', phase=phase, error_type=type(error).__name__)


def drive(args, run_id, journal, connection_factory=connect, operation=execute):
    count = planned(args)
    pending = queue.Queue(maxsize=args.queue)
    stop = threading.Event()
    failures = queue.SimpleQueue()
    started = [None]
    ready = threading.Barrier(args.workers + 1, timeout=30,
                              action=lambda: started.__setitem__(0,time.monotonic()+.05))

    def worker(number):
        connection = None
        try:
            connection = connection_factory(args)
            sql(connection, 'USE ' + args.database)
            sql(connection, 'SET SESSION TRANSACTION ISOLATION LEVEL ' + args.isolation.replace('-', ' '))
            with (journal.root / f'worker-{number}.jsonl').open('xb') as output:
                ready.wait()
                while True:
                    item = pending.get()
                    if item is None:
                        pending.task_done()
                        break
                    token, scheduled = item
                    begin = time.monotonic() - started[0]
                    result = dict(outcome='cancelled') if stop.is_set() else operation(
                        connection, args, token, fixture_keys(token, args.rows, args.keys_per_transaction,
                        args.distribution, args.hot_keys, args.seed), run_id)
                    if result['outcome'] == 'unknown':
                        stop.set()
                    journal.append(output, dict(token=token, scheduled=scheduled, started=begin,
                                                finished=time.monotonic() - started[0], **result))
                    pending.task_done()
        except BaseException as error:
            stop.set()
            failures.put(type(error).__name__ + ': ' + str(error)[:200])
            ready.abort()
        finally:
            if connection is not None:
                connection.close()

    workers = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(args.workers)]
    for worker_thread in workers:
        worker_thread.start()
    try:
        # Future start gives every connected worker the same clock origin.
        ready.wait()
        with (journal.root / 'scheduler.jsonl').open('xb') as output:
            for token in range(1, count + 1):
                due = (token - 1) / args.rate
                if not stop.is_set():
                    stop.wait(max(0, started[0] + due - time.monotonic()))
                outcome = None
                if stop.is_set():
                    outcome = 'cancelled'
                else:
                    try:
                        pending.put_nowait((token, due))
                    except queue.Full:
                        outcome = 'queue_full'
                if outcome:
                    journal.append(output, dict(token=token, scheduled=due, started=None,
                                                finished=time.monotonic() - started[0], outcome=outcome))
        # Admission ends at the declared time. Acknowledged work may settle
        # later; the audit includes that drain in latency and useful goodput.
        deadline = time.monotonic() + args.drain_timeout
        for _ in workers:
            while True:
                if not any(t.is_alive() for t in workers) or time.monotonic() >= deadline:
                    break
                try:
                    pending.put(None, timeout=min(.1, max(.001, deadline - time.monotonic())))
                    break
                except queue.Full:
                    continue
        for worker_thread in workers:
            worker_thread.join(max(0, deadline - time.monotonic()))
        if any(t.is_alive() for t in workers):
            stop.set()
            raise RuntimeError('workers did not settle before the drain deadline; no final readback')
        if not failures.empty():
            raise RuntimeError(failures.get())
    except BaseException:
        stop.set()
        raise


def counter_pages(connection, args):
    last = -1
    while True:
        rows = sql(connection, 'SELECT k,n,pad FROM t WHERE k>%s ORDER BY k LIMIT 128', (last,))
        if not rows:
            break
        for key, count, payload in rows:
            if key <= last or bytes(payload) != b'x' * args.row_bytes:
                raise AssertionError('fixture key order or payload mismatch')
            if args.workload == 'point' and count != 0:
                raise AssertionError('point reads require a zero-counter fixture; prepare a new namespace')
            yield dict(key=key, value=count, payload_bytes=len(payload), payload_sha256=sha(bytes(payload)))
            last = key


def ledger_pages(connection, run_id):
    token, key = 0, -1
    while True:
        rows = sql(connection, 'SELECT token,k FROM ledger WHERE run_id=%s AND '
                   '(token>%s OR (token=%s AND k>%s)) ORDER BY run_id,token,k LIMIT 128',
                   (run_id, token, token, key))
        if not rows:
            break
        for next_token, next_key in rows:
            if (next_token, next_key) <= (token, key):
                raise AssertionError('ledger continuation did not advance')
            yield dict(token=next_token, key=next_key)
            token, key = next_token, next_key


def retain_rows(journal, name, rows):
    with (journal.root / name).open('xb') as output:
        for row in rows:
            journal.append(output, row)


def lines(path):
    with path.open() as stream:
        for line in stream:
            yield json.loads(line)


def audit(root, args):
    """Derive effects and latency from raw records with bounded SQLite cache."""
    path = root / 'audit.sqlite'
    db = sqlite3.connect(path)
    try:
        db.execute('PRAGMA cache_size=-8192')
        db.execute('PRAGMA temp_store=FILE')
        db.execute('PRAGMA max_page_count=' + str(args.audit_bytes // 4096))
        db.executescript('''CREATE TABLE attempts(token INTEGER PRIMARY KEY, outcome TEXT,
            scheduled REAL, started REAL, finished REAL, latency REAL, service REAL);
            CREATE TABLE counters(k INTEGER PRIMARY KEY, before_n INTEGER, delta INTEGER DEFAULT 0,
                after_n INTEGER);
            CREATE TABLE expected(token INTEGER, k INTEGER, seen INTEGER DEFAULT 0, PRIMARY KEY(token,k));''')
        for row in lines(root / 'baseline.jsonl'):
            if row['payload_bytes']!=args.row_bytes or row['payload_sha256']!=sha(b'x'*args.row_bytes):
                raise AssertionError('baseline payload mismatch')
            db.execute('INSERT INTO counters(k,before_n) VALUES (?,?)', (row['key'], row['value']))
        if db.execute('SELECT count(*),min(k),max(k) FROM counters').fetchone() != (args.rows, 0, args.rows-1):
            raise AssertionError('baseline does not contain every fixture key')
        for path in sorted(root.glob('worker-*.jsonl')) + [root / 'scheduler.jsonl']:
            for row in lines(path):
                token, outcome = row['token'], row['outcome']
                if not 1 <= token <= planned(args) or outcome not in ('acknowledged', 'refused', 'unknown', 'queue_full', 'cancelled'):
                    raise AssertionError('invalid attempt identity/outcome')
                if row['scheduled'] != (token - 1) / args.rate:
                    raise AssertionError('attempt differs from declared arrival schedule')
                begin, end, due = row['started'], row['finished'], row['scheduled']
                if begin is not None and (begin < due or end < begin):
                    raise AssertionError('attempt timing is inconsistent')
                latency = end - due if outcome in ('acknowledged', 'refused', 'unknown') else None
                service = end - begin if begin is not None else None
                db.execute('INSERT INTO attempts VALUES (?,?,?,?,?,?,?)', (token,outcome,due,begin,end,latency,service))
                if outcome == 'acknowledged' and args.workload == 'transaction':
                    for key in fixture_keys(token,args.rows,args.keys_per_transaction,args.distribution,args.hot_keys,args.seed):
                        db.execute('INSERT INTO expected(token,k) VALUES (?,?)', (token,key))
                        if db.execute('UPDATE counters SET delta=delta+1 WHERE k=?', (key,)).rowcount != 1:
                            raise AssertionError('acknowledged unknown fixture key')
        if db.execute('SELECT count(*) FROM attempts').fetchone()[0] != planned(args):
            raise AssertionError('missing scheduled attempts; no complete-run qualification')
        for row in lines(root / 'readback.jsonl'):
            if row['payload_bytes']!=args.row_bytes or row['payload_sha256']!=sha(b'x'*args.row_bytes):
                raise AssertionError('readback payload mismatch')
            if db.execute('UPDATE counters SET after_n=? WHERE k=? AND after_n IS NULL',
                          (row['value'],row['key'])).rowcount != 1:
                raise AssertionError('duplicate or unexpected readback key')
        for row in lines(root / 'ledger.jsonl'):
            if db.execute('UPDATE expected SET seen=1 WHERE token=? AND k=? AND seen=0',
                          (row['token'],row['key'])).rowcount != 1:
                raise AssertionError('ledger has a duplicate, refused, unknown or unexpected effect')
        if db.execute('SELECT count(*) FROM expected WHERE seen=0').fetchone()[0]:
            raise AssertionError('acknowledged ledger effect is absent')
        if db.execute('SELECT count(*) FROM counters WHERE after_n IS NULL OR after_n!=before_n+delta').fetchone()[0]:
            raise AssertionError('exact per-key effects differ from acknowledgements')
        counts = dict(db.execute('SELECT outcome,count(*) FROM attempts WHERE scheduled>=? GROUP BY outcome', (args.warmup,)))
        all_counts = dict(db.execute('SELECT outcome,count(*) FROM attempts GROUP BY outcome'))
        n = counts.get('acknowledged',0)

        def percentiles(column):
            values = {}
            for name, fraction in (('p50',.5),('p99',.99),('p999',.999),('max',1)):
                value = db.execute(f'SELECT {column} FROM attempts WHERE outcome="acknowledged" '
                    f'AND scheduled>=? ORDER BY {column} LIMIT 1 OFFSET ?',
                    (args.warmup,max(0,math.ceil(fraction*n)-1))).fetchone()
                values[name] = value[0] * 1000 if value else None
            return values

        latency, service = percentiles('latency'), percentiles('service')
        offered = sum(counts.values())
        bad = offered - n
        end = db.execute('SELECT max(finished) FROM attempts WHERE scheduled>=?', (args.warmup,)).fetchone()[0]
        window = max(args.seconds, (end or 0) - args.warmup)
        within = db.execute('SELECT count(*) FROM attempts WHERE scheduled>=? AND outcome="acknowledged" AND latency<=?',
                            (args.warmup,args.p99_ms/1000)).fetchone()[0]
        complete = not all_counts.get('unknown',0) and not all_counts.get('cancelled',0)
        db.commit()
        return dict(complete=complete, exact_effects=True, counts=counts, all_counts=all_counts,
                    scheduled_latency_ms=latency, service_latency_ms=service,
                    verified_goodput_per_second=n/window, goodput_within_slo_per_second=within/args.seconds,
                    measured_cohort_seconds_including_drain=window, error_fraction=bad/offered if offered else 1,
                    objectives_pass=bool(complete and n and latency['p99']<=args.p99_ms and bad/offered<=args.max_error_rate))
    finally:
        db.close()


def prepare(connection, args):
    # No IF NOT EXISTS, DROP, TRUNCATE or replay: an existing namespace fails.
    sql(connection, 'CREATE DATABASE ' + args.database)
    sql(connection, 'USE ' + args.database)
    sql(connection, 'CREATE TABLE fixture_meta (k VARCHAR(32) PRIMARY KEY,v TEXT) ENGINE=' + args.engine)
    sql(connection, "INSERT INTO fixture_meta VALUES ('state','building')")
    sql(connection, 'CREATE TABLE t (k INT PRIMARY KEY,n BIGINT NOT NULL,pad LONGBLOB NOT NULL) ENGINE='
        + args.engine + ' PARTITION BY KEY(k)')
    sql(connection, 'CREATE TABLE ledger (run_id VARCHAR(32),token BIGINT,k INT,PRIMARY KEY(run_id,token,k)) ENGINE='
        + args.engine + ' PARTITION BY KEY(k)')
    batch = max(1,min(128,1048576//args.row_bytes))
    for start in range(0,args.rows,batch):
        keys = range(start,min(args.rows,start+batch))
        params = [value for key in keys for value in (key,0,b'x'*args.row_bytes)]
        sql(connection, 'INSERT INTO t VALUES ' + ','.join(['(%s,%s,%s)']*len(keys)), params)
    sql(connection, 'INSERT INTO fixture_meta VALUES (%s,%s)', ('config',json.dumps(dict(rows=args.rows,row_bytes=args.row_bytes,engine=args.engine))))
    sql(connection, "UPDATE fixture_meta SET v='ready' WHERE k='state'")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('prepare','run','audit'))
    parser.add_argument('--endpoint', default='127.0.0.1:4000')
    parser.add_argument('--database', required=True)
    parser.add_argument('--user', default='root')
    parser.add_argument('--password-env', default='TARN_BENCH_PASSWORD')
    parser.add_argument('--tls-ca')
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--engine', choices=('memtx','vinyl','InnoDB'), default='memtx')
    parser.add_argument('--rows', type=int, default=128)
    parser.add_argument('--row-bytes', type=int, default=256)
    parser.add_argument('--workload', choices=('point','transaction'), default='transaction')
    parser.add_argument('--keys-per-transaction', type=int, default=1)
    parser.add_argument('--distribution', choices=('uniform','hot'), default='uniform')
    parser.add_argument('--hot-keys', type=int, default=8)
    parser.add_argument('--seed', type=int, default=1)
    parser.add_argument('--isolation', choices=('SERIALIZABLE','READ-COMMITTED'), default='SERIALIZABLE')
    parser.add_argument('--rate', type=float, default=100)
    parser.add_argument('--warmup', type=float, default=3)
    parser.add_argument('--seconds', type=float, default=10)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--queue', type=int, default=64)
    parser.add_argument('--rpc-timeout', type=int, default=10)
    parser.add_argument('--drain-timeout', type=float, default=30)
    parser.add_argument('--max-operations', type=int, default=1000000)
    parser.add_argument('--evidence-bytes', type=int, default=1073741824)
    parser.add_argument('--audit-bytes', type=int, default=1073741824)
    parser.add_argument('--p99-ms', type=float, default=100)
    parser.add_argument('--max-error-rate', type=float, default=.001)
    parser.add_argument('--target-receipt', type=Path, help='Declared deployment metadata; never treated as runtime verification')
    args = parser.parse_args()
    if not re.fullmatch(r'tarn_load_[a-z0-9_]{1,40}',args.database):
        parser.error('database must be a fresh tarn_load_ namespace with lowercase identifier characters')
    if not (1<=args.rows<=10000000 and 1<=args.row_bytes<=65536 and 1<=args.workers<=256 and 1<=args.queue<=100000):
        parser.error('row, payload, worker or queue bound is invalid')
    if not (1<=args.keys_per_transaction<=min(8,args.rows) and args.keys_per_transaction<=args.hot_keys<=args.rows):
        parser.error('key count/hot set is invalid')
    if not (0<args.rate<=100000 and 0<args.seconds<=86400 and 0<=args.warmup<=3600 and
            0<args.rpc_timeout<=60 and 0<args.drain_timeout<=300 and 0<args.max_operations<=50000000 and
            1048576<=args.evidence_bytes<=107374182400 and 1048576<=args.audit_bytes<=107374182400 and
            0<args.p99_ms and 0<=args.max_error_rate<=1):
        parser.error('schedule, timeout, evidence or objective bound is invalid')
    if args.workload=='point' and args.keys_per_transaction!=1:
        parser.error('point workload requires exactly one key')
    planned(args)
    if args.command=='audit':
        original=json.loads((args.out/'receipt.json').read_text())
        replay=argparse.Namespace(**original['parameters'])
        if replay.command!='run' or set(original['raw_files'])!={p.name for p in args.out.glob('*.jsonl')}:
            raise AssertionError('receipt is not a complete raw-file inventory for a load run')
        for name, identity in original['raw_files'].items():
            path=args.out/name
            if path.stat().st_size!=identity['bytes'] or file_sha(path)!=identity['sha256']:
                raise AssertionError('raw evidence differs from retained hashes')
        # Offline audits create a separate SQLite file, preserving the original.
        import tempfile
        with tempfile.TemporaryDirectory(prefix='tarn-load-audit-') as temp:
            target=Path(temp)
            for path in args.out.glob('*.jsonl'):
                (target/path.name).symlink_to(path.resolve())
            result=audit(target,replay)
            result['collector_complete']=original['complete']
            result['complete']=result['complete'] and original['complete']
            result['objectives_pass']=result['objectives_pass'] and original['complete']
            print(json.dumps(result,sort_keys=True))
            if not result['complete']:
                raise SystemExit(1)
            if not result['objectives_pass']:
                raise SystemExit(2)
        return
    args.out.mkdir(parents=True,exist_ok=False)
    record=dict(format='tarn-scheduled-load-v1',complete=False,run_id=uuid.uuid4().hex,
                parameters={k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
                generator=dict(source_sha256=sha(Path(__file__).read_bytes()),python=platform.python_version(),
                               platform=platform.platform(),driver_version=importlib.metadata.version('PyMySQL')),
                scope='SQL scheduled-arrival generator; target durability, physical topology and host resources require separate verified receipts')
    if args.target_receipt:
        if args.target_receipt.stat().st_size>65536:
            raise ValueError('target metadata exceeds 64 KiB')
        raw=args.target_receipt.read_bytes()
        record['declared_target']=dict(sha256=sha(raw),value=json.loads(raw),verification='declaration only')
    receipt=args.out/'receipt.json'
    def save(): receipt.write_bytes(encoded(record))
    save()
    connection=None
    journal=Journal(args.out,args.evidence_bytes)
    start_cpu=time.process_time()
    try:
        connection=connect(args)
        record['server_version']=sql(connection,'SELECT VERSION()')[0][0]
        if args.command=='prepare':
            prepare(connection,args)
            record['complete']=True
        else:
            sql(connection,'USE '+args.database)
            fixture=json.loads(sql(connection,"SELECT v FROM fixture_meta WHERE k='config'")[0][0])
            if fixture!=dict(rows=args.rows,row_bytes=args.row_bytes,engine=args.engine):
                raise AssertionError('prepared fixture differs from declared configuration')
            if sql(connection,"UPDATE fixture_meta SET v=%s WHERE k='state' AND v='ready'",(record['run_id'],))!=1:
                raise AssertionError('fixture is active or previously uncertain; use a new namespace')
            retain_rows(journal,'baseline.jsonl',counter_pages(connection,args))
            drive(args,record['run_id'],journal)
            retain_rows(journal,'readback.jsonl',counter_pages(connection,args))
            retain_rows(journal,'ledger.jsonl',ledger_pages(connection,record['run_id']))
            record['audit']=audit(args.out,args)
            record['complete']=record['audit']['complete']
            if record['complete']:
                if sql(connection,"UPDATE fixture_meta SET v='ready' WHERE k='state' AND v=%s",(record['run_id'],))!=1:
                    raise AssertionError('fixture ownership changed')
            else:
                raise RuntimeError('unknown/cancelled outcomes retain fixture ownership; no replay or reuse')
    except BaseException as error:
        record['complete']=False
        record['failure']=dict(type=type(error).__name__,detail=str(error)[:500])
        raise
    finally:
        journal.seal()
        if connection is not None: connection.close()
        record['generator']['cpu_seconds']=time.process_time()-start_cpu
        record['generator']['cpu_scope']='whole command, including setup/readback/audit; not target CPU'
        record['generator']['max_rss_native_units']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        record['generator']['rss_units']='bytes' if platform.system()=='Darwin' else 'KiB on Linux; platform dependent elsewhere'
        record['raw_evidence_bytes']=journal.used
        record['raw_files']={p.name:dict(bytes=p.stat().st_size,sha256=file_sha(p)) for p in args.out.glob('*.jsonl')}
        save()
    print(json.dumps(record.get('audit',dict(complete=record['complete'])),sort_keys=True))
    if args.command=='run' and not record['audit']['objectives_pass']:
        raise SystemExit(2)


if __name__=='__main__':
    with error_states():
        main()
