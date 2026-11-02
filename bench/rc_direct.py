"""Direct net.box adapter with the rc_profile.sample interface; no replay.

The SQL-created fixture supplies native layouts, routing and definition fences.
Client event journals survive diagnostic failure and process interruption.
Client journaling/scheduling differs from MySQL: compare candidate/control
within each driver, not their absolute throughput as gateway attribution.
"""
import collections
import json
import math
import os
from pathlib import Path
import selectors
import subprocess
import time
import uuid


METADATA = """
local function describe(name)
 local sp=assert(box.space[name]); local entry=assert(box.space._tarn_catalog:get{name})
 local catalog=require('json').decode(assert(entry.def.json))
 return {sid=sp.id,engine=sp.engine,format=sp:format(),parts=sp.index[0].parts,
  catalog=catalog,fence_version=entry.def.ver or 0,is_sync=sp.is_sync and sp.state.is_sync}
end
local t,ledger=describe('rc186.t'),describe('rc186.ledger')
return {t=t,ledger=ledger,rows=box.space['rc186.t'].index[0]:select({1},{iterator='EQ',limit=9}),
 ledger_count=box.space['rc186.ledger']:len(),shard_id=require('tarn').cfg.shard_id}
"""


def _integer(value):
    return type(value) is int and 0 <= value < 2 ** 53


def read_ready(process, timeout=20):
    """Bound the entire readiness line, including a child stuck mid-write."""
    deadline, data = time.monotonic() + timeout, b''
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        while b'\n' not in data:
            remaining = deadline - time.monotonic()
            assert remaining > 0 and selector.select(remaining), 'direct driver readiness deadline expired'
            chunk = os.read(process.stdout.fileno(), 4096)
            assert chunk, 'direct driver exited before readiness'
            data += chunk
            assert len(data) <= 4096, 'oversized direct readiness reply'
    return data.decode()


def metadata(rows, engine):
    """Refuse unsupported layouts instead of guessing physical tuple order."""
    owners = []
    for item in rows:
        r = item['result']
        for name, names, pk in (('t', {'tenant', 'id', 'v', 'payload', 'bucket_id'}, ('tenant', 'id')),
                               ('ledger', {'tenant', 'token', 'k', 'bucket_id'}, ('tenant', 'token'))):
            d = r[name]
            fields = {f['name']: i + 1 for i, f in enumerate(d['format'])}
            assert set(fields) == names and len(d['format']) == len(names), ('unsupported layout', name)
            assert [p['fieldno'] for p in d['parts']] == [fields[n] for n in pk], ('primary key', name)
            c = d['catalog']
            assert d['is_sync'] is True and d['engine'] == engine
            assert c['db'] == 'rc186' and c['name'] == name and c['space_id'] == d['sid']
            assert c['shard_key'] == [0] and c['columns'][0]['name'] == 'tenant'
            version = c.get('ver', 0)
            assert _integer(version) and version == d['fence_version'], 'catalog/fence version mismatch'
            assert _integer(d['sid']) and d['sid'] > 0
            d.update(fields=fields, width=len(fields), version=version)
        assert r['ledger_count'] == 0, 'ledger was not reset'
        if r['rows']:
            f = r['t']['fields']
            assert len(r['rows']) == 8
            buckets, keys = set(), set()
            for row in r['rows']:
                assert len(row) == r['t']['width'] and row[f['tenant'] - 1] == 1
                keys.add(row[f['id'] - 1]); buckets.add(row[f['bucket_id'] - 1])
                assert row[f['v'] - 1] == 0 and row[f['payload'] - 1] == 'x' * 256
            assert keys == set(range(8)) and len(buckets) == 1
            bucket = next(iter(buckets))
            assert _integer(bucket) and bucket > 0
            fence = []
            for d in sorted((r['t'], r['ledger']), key=lambda x: x['sid']):
                fence.extend((d['sid'], d['version']))
            owners.append(dict(address=item['address'], t=r['t'], ledger=r['ledger'],
                               bucket=bucket, fence=fence, shard_id=r['shard_id']))
    assert len(owners) == 1, 'tenant rows must have exactly one owning leader'
    return owners[0]


def fold_events(path, number, shape):
    """Retain started identities as unknown when terminal evidence is absent."""
    attempts, errors, txids = {}, [], set()
    clients = 1 if shape == 'uncontended' else 8
    with Path(path).open('rb') as source:
        for line_number, raw in enumerate(source, 1):
            try:
                assert raw.endswith(b'\n'), 'incomplete final journal line'
                e = json.loads(raw)
                token = e['token']
                assert _integer(token), 'invalid token'
                kind = e['event']
                if kind == 'start':
                    assert token not in attempts, 'duplicate attempt identity'
                    worker, key = e['worker'], e['key']
                    assert type(worker) is int and 0 <= worker < clients and e['number'] == number
                    assert key == (0 if shape == 'hot' else worker)
                    sequence = token - number * 1000000 - worker * 100000
                    assert 1 <= sequence <= 50000
                    assert isinstance(e['txid'], str) and e['txid'] and e['txid'] not in txids
                    txids.add(e['txid'])
                    attempts[token] = dict(token=token, key=key, worker=worker, txid=e['txid'],
                                           outcome='unknown', commit_dispatched=False,
                                           terminal=False, latency_seconds=None, sqlstate='40003')
                else:
                    assert token in attempts, 'event without a recorded start'
                    a = attempts[token]
                    assert not a['terminal'], 'event after terminal outcome'
                    if kind == 'commit':
                        assert not a['commit_dispatched'], 'duplicate commit dispatch'
                        a['commit_dispatched'] = True
                    else:
                        assert kind == 'finish', 'unknown event type'
                        outcome, stage, reply = e['outcome'], e['stage'], e.get('reply')
                        assert outcome in ('ok', 'refused', 'unknown')
                        assert stage in ('lock_t', 'lock_ledger', 'commit')
                        latency = e['latency_seconds']
                        assert type(latency) in (int, float) and math.isfinite(latency) and latency >= 0
                        if outcome == 'ok':
                            assert a['commit_dispatched'] and stage == 'commit' and e['calls'] == 3
                            assert type(reply) is list and reply == ['ok', 2] and type(reply[1]) is int
                        elif outcome == 'refused':
                            assert type(reply) is list and len(reply) in ((3,) if stage == 'commit' else (3, 4))
                            assert type(reply[1]) is int and isinstance(reply[2], str)
                            assert len(reply) == 3 or isinstance(reply[3], str)
                            assert (reply[0], reply[1]) in {('timeout', 1205), ('deadlock', 1213),
                                ('conflict', 1213), ('error', 1205), ('error', 1213)}
                            assert a['commit_dispatched'] == (stage == 'commit')
                            assert e['calls'] == {'lock_t': 2, 'lock_ledger': 3, 'commit': 4}[stage]
                        a.update(outcome=outcome, stage=stage, reply=reply, calls=e['calls'], terminal=True,
                                 latency_seconds=latency, failure=e.get('failure'))
                        a['sqlstate'] = ('00000' if outcome == 'ok' else '40003' if outcome == 'unknown'
                                         else '40001' if reply[1] == 1213 else 'HY000')
            except (AssertionError, KeyError, TypeError, ValueError, UnicodeDecodeError) as exc:
                errors.append(dict(line=line_number, error=str(exc) or type(exc).__name__))
    return sorted(attempts.values(), key=lambda r: r['token']), errors


def sample(args, number, shape, profiled, endpoints, output, evidence, warmup=False):
    import rc_profile as common
    assert not profiled, 'direct samples do not enable the guest profiler'
    assert shape in ('uncontended', 'unrelated', 'hot') and 1 <= args.seconds <= 15
    assert type(number) is int and 1 <= number <= 1000000
    output = Path(output)
    stem = output.parent / f'{output.stem}-sample-{number}-direct'
    paths = {name: Path(str(stem) + suffix) for name, suffix in {
        'config': '.config.json', 'events': '.events.jsonl', 'summary': '.driver.json',
        'stdout': '.stdout.log', 'stderr': '.stderr.log', 'attempts': '.attempts.jsonl'}.items()}
    assert not any(p.exists() for p in paths.values()), 'refusing to overwrite direct evidence'
    paths['events'].touch()
    record = dict(number=number, driver='direct', shape=shape, profiled=False, warmup=warmup,
                  clients=1 if shape == 'uncontended' else 8, scheduled_seconds=args.seconds,
                  complete=False, errors=[], workers_live=True,
                  raw_files={k: p.name for k, p in paths.items()},
                  limitations='Client-side Lua/net.box and flushed attempt journals; compare within driver only.')
    evidence['samples'].append(record)
    def persist():
        output.write_text(json.dumps(evidence, indent=2) + '\n')
    persist()
    admin, process, raw_stdout, before, pids = None, None, '', None, []
    metrics_code = ('local r=(function()\n' + common.METRICS + '\nend)(); '
                    'r.term=box.info.election.term; r.uuid=tostring(box.info.uuid); return r')
    try:
        admin = common.connect(args.sql)
        common.query(admin, 'TRUNCATE TABLE ledger')
        common.query(admin, 'UPDATE t SET v=0')
        topology = common.evaluate(endpoints, 'return {ro=box.info.ro}')
        leaders = [r['address'] for r in topology if not r['result']['ro']]
        assert len(leaders) == 2
        record['metadata'] = common.evaluate(leaders, METADATA)
        selected = metadata(record['metadata'], args.engine)
        config = dict(selected, schema=1, number=number, shape=shape, seconds=args.seconds,
                      clients=record['clients'], max_attempts=50000, call_timeout=10, drain_seconds=35,
                      run_id='rc186-direct-' + uuid.uuid4().hex,
                      events=str(paths['events'].resolve()), summary=str(paths['summary'].resolve()))
        paths['config'].write_text(json.dumps(config, indent=2) + '\n')
        record['driver_sha256'] = common.sha(Path(__file__).with_name('rc_direct.lua'))
        record['adapter_sha256'] = common.sha(__file__)
        persist()
        with paths['stderr'].open('w') as stderr:
            process = subprocess.Popen(['tarantool', str(Path(__file__).with_name('rc_direct.lua')),
                                        str(paths['config'].resolve())], stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=stderr, text=True, bufsize=1)
            raw_stdout = read_ready(process)
            assert json.loads(raw_stdout) == {'ready': True, 'clients': record['clients']}
            before = common.evaluate(endpoints, metrics_code)
            record['before'] = before
            for row in before:
                r = row['result']; common.require_production_flags(r)
                assert r['wal'] == 'fsync' and r['quorum'] == 'N / 2 + 1' and r['engine'] == args.engine
                assert r['info']['row_locks'] == 0 and r['info']['plock_txns'] == 0
                if not r['info']['ro']:
                    n = r['info']['native']
                    assert (isinstance(n, dict) and n.get('ready') == 1) if args.native == 'enabled' else n is False
            pids = [r['result']['pid'] for r in before]
            record['cpu_before'] = {str(pid): common.cpu(pid) for pid in pids}
            persist()
            tail, _ = process.communicate('GO\n', timeout=args.seconds + 50)
            raw_stdout += tail
            record['driver_exit_code'] = process.returncode
            assert process.returncode == 0, 'direct driver failed'
    except BaseException as exc:
        record['errors'].append(repr(exc))
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            try:
                tail, _ = process.communicate(timeout=10)
                raw_stdout += tail
            except BaseException as exc:
                record['errors'].append('driver retirement: ' + repr(exc))
        record['driver_exit_code'] = process.returncode if process else None
        paths['stdout'].write_text(raw_stdout)
        # Preserve the journal and identities before diagnostics/readback.
        records, event_errors = fold_events(paths['events'], number, shape)
        record['event_errors'] = event_errors
        paths['attempts'].write_text(''.join(json.dumps(r, sort_keys=True) + '\n' for r in records))
        record['attempts_file'] = paths['attempts'].name
        record['attempts_sha256'] = common.sha(paths['attempts'])
        record['events_sha256'] = common.sha(paths['events'])
        record['driver_summary'] = common.diagnostic(record, 'driver_summary',
            lambda: json.loads(paths['summary'].read_text()))
        summary = record['driver_summary'] or {}
        record['workers_live'] = summary.get('workers_live', True)
        record['elapsed_seconds'] = summary.get('elapsed_seconds')
        counts = collections.Counter(r['outcome'] for r in records)
        record.update(acknowledged=counts['ok'], refused=counts['refused'], unknown=counts['unknown'])
        record['latency'] = {outcome: {f'p{int(p * 100)}': common.percentile(
            [r['latency_seconds'] for r in records if r['outcome'] == outcome and r['latency_seconds'] is not None], p)
            for p in (.5, .99)} for outcome in ('ok', 'refused', 'unknown')}
        persist()
        record['cpu_after'] = common.diagnostic(record, 'cpu', lambda: {str(pid): common.cpu(pid) for pid in pids})
        record['after'] = common.diagnostic(record, 'metrics', lambda: common.evaluate(endpoints, metrics_code))
        def readback():
            # Use a separate connection even if the original setup connection failed.
            with common.connect(args.sql) as reader:
                return dict(counters=common.query(reader, 'SELECT id,v FROM t WHERE tenant=1 ORDER BY id'),
                            ledger=common.query(reader, 'SELECT token,k FROM ledger WHERE tenant=1 ORDER BY token'))
        record['readback'] = common.diagnostic(record, 'readback', readback)
        if admin is not None:
            admin.close()
        persist()
    assert not record['errors'] and not event_errors and not record.get('diagnostic_errors'), record
    assert summary.get('complete') is True and not record['workers_live']
    assert len(summary.get('states', [])) == record['clients']
    assert all(s['done_at_cut'] is True for s in summary['states'])
    assert all(r['terminal'] for r in records)
    assert summary.get('admitted') == summary.get('finished') == len(records)
    assert sum(summary['calls'].values()) == sum(r['calls'] for r in records)
    record.update(common.reconcile(records, record['readback']['counters'], record['readback']['ledger']))
    assert record['acknowledged'] > 0 and record['elapsed_seconds'] > 0
    record['goodput_per_second'] = record['acknowledged'] / record['elapsed_seconds']
    previous = {r['address']: r['result'] for r in before}
    record['deltas'] = []
    for row in record['after']:
        a, b = row['result'], previous[row['address']]
        common.require_production_flags(a)
        assert all(a[k] == b[k] for k in ('pid', 'term', 'uuid')), 'storage identity or term changed'
        assert a['info']['ro'] == b['info']['ro'], 'storage role changed'
        assert a['info']['row_locks'] == 0 and a['info']['plock_txns'] == 0, 'direct locks did not drain'
        record['deltas'].append(dict(address=row['address'], calls=a['calls'] - b['calls'],
            metrics={k: a['metrics'][k] - b['metrics'][k] for k in
                     ('gc_allocated', 'gc_freed', 'gc_steps_propagate', 'gc_steps_atomic',
                      'gc_steps_sweep', 'gc_steps_finalize')},
            stats={k: a['info']['stats'][k] - b['info']['stats'][k] for k in
                   ('plock_waits', 'deadlocks', 'lock_timeouts', 'commit_rc', 'commit_waits')}))
    record['complete'] = True
    persist()
    print(json.dumps({k: record[k] for k in ('number', 'driver', 'shape', 'acknowledged',
                                            'refused', 'goodput_per_second')}), flush=True)
