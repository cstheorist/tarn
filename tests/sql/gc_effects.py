#!/usr/bin/env python3
"""Fixed, single-attempt effect reconciliation; not an explanation of GC faults."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time
import traceback

from effect_journal import Journal, read


def operations(keys, database, engine):
    """Complete workload manifest; read-only reconciliation is separate."""
    a, b, c, u, x, y = keys
    rows = []

    def add(op, txn, role, sql, params=None):
        rows.append(dict(op_id=op, txn_id=txn, role=role, sql=sql, params=params))

    add('database', 'setup', 'setup', 'CREATE DATABASE ' + database)
    add('table', 'setup', 'setup', 'CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL) ENGINE=' + engine)
    for key in keys:
        add('seed-' + str(key), 'setup', 'setup', 'INSERT INTO t VALUES (%s,0)', [key])
    add('isolation', 'ack', 'statement', "SET SESSION transaction_isolation='SERIALIZABLE'")
    add('ack-begin', 'ack', 'statement', 'BEGIN')
    add('ack-a', 'ack', 'statement', 'UPDATE t SET v=v+1 WHERE id=%s', [a])
    add('ack-b', 'ack', 'statement', 'UPDATE t SET v=v+1 WHERE id=%s', [x])
    add('ack-commit', 'ack', 'commit', 'COMMIT')
    add('refuse-begin', 'refuse', 'statement', 'BEGIN')
    add('refuse-read', 'refuse', 'statement', 'SELECT v FROM t WHERE id=%s', [c])
    add('refuse-a', 'refuse', 'statement', 'UPDATE t SET v=v+1 WHERE id=%s', [b])
    add('refuse-b', 'refuse', 'statement', 'UPDATE t SET v=v+1 WHERE id=%s', [y])
    add('conflict', 'conflict', 'statement', 'UPDATE t SET v=v+1 WHERE id=%s', [c])
    add('refuse-commit', 'refuse', 'commit', 'COMMIT')
    add('unknown', 'unknown', 'statement', 'UPDATE t SET v=v+1 WHERE id=%s', [u])
    return rows


def expected_rows(keys):
    a, b, c, u, x, y = keys
    return sorted([[a, 1], [b, 0], [c, 1], [u, 1], [x, 1], [y, 0]])


def bucket(key):
    # This fixed fixture deliberately uses only positive fixint keys. Match
    # movetest.bucket_of's CRC32C on their single-byte MessagePack encoding.
    assert type(key) is int and 0 < key < 128
    crc = 0xffffffff ^ key
    for _ in range(8):
        crc = (crc >> 1) ^ 0x82f63b78 if crc & 1 else crc >> 1
    return crc % 32 + 1


def expected_raw(keys):
    return [row + [bucket(row[0])] for row in expected_rows(keys)]


def check_runtime(info, pid, native):
    assert info['pid'] == pid and info['wal_mode'] == 'fsync', info
    assert info['configured_replicas'] == info['observed_replicas'] == 3 and info['quorum'] == 2 and info['sync'] is True, info
    assert isinstance(info['native'], dict) if native == 'native' else info['native'] is False
    assert info['system_spaces'] == {name: {'is_sync': True, 'state_is_sync': True}
                                      for name in ('_tarn_txn', '_tarn_part')}, info


def reconcile(journal, evidence):
    """Fail closed on missing evidence or altered outcome classification."""
    assert not journal['incomplete'] and not journal['pending'], 'incomplete journal'
    records = journal['records']
    cfg = records[0]['config']
    owned_names = {f's{s}.{r}' for s in (1, 2) for r in (1, 2, 3)}
    assert set(cfg['owned_nodes']) == owned_names, 'exact RF3 two-shard ownership required'
    assert set(evidence['storage_runtime']) == owned_names, 'all six runtime observations required'
    for name, info in evidence['storage_runtime'].items():
        check_runtime(info, cfg['owned_nodes'][name]['pid'], cfg['native'])
    keys = cfg['keys']
    assert len(keys) == 6 and len(set(keys)) == 6, 'unique per-operation target keys required'
    manifest = operations(keys, cfg['database'], cfg['engine'])
    intents = [r for r in records[1:] if r['type'] == 'intent']
    assert len(intents) == len(manifest), 'missing or extra submitted operation'
    for actual, wanted in zip(intents, manifest):
        assert all(actual.get(k) == v for k, v in wanted.items()), (actual, wanted)
    completions = {r['op_id']: r for r in records[1:] if r['type'] == 'completion'}
    assert set(completions) == {r['op_id'] for r in manifest}, 'missing completion'
    for op, completion in completions.items():
        if op in ('refuse-commit', 'unknown'):
            wanted = [1213, '40001'] if op == 'refuse-commit' else [1180, '40003']
            assert completion['kind'] == 'error' and completion['raw']['error'] == wanted, completion
        else:
            assert completion['kind'] == 'response' and 'result' in completion['raw'], completion
    # A statement response does not prove commit. Both COMMIT and every target
    # statement must exist above before their effects may be reconciled here.
    for op in [r['op_id'] for r in manifest if r['op_id'].startswith('seed-')] + ['ack-a', 'ack-b', 'refuse-a', 'refuse-b', 'conflict']:
        assert completions[op]['raw']['result'] == 1, completions[op]
    assert completions['refuse-read']['raw']['result'] == [[0]]
    events = evidence['proxy_events']
    replies = [e for e in events if e.get('stage') == 'reply']
    assert len(replies) == 1 and replies[0].get('committed') is True, events
    assert 'autocommit' in replies[0]['procedure'], events
    assert sum(e.get('stage') == 'injected' and e.get('fault') == 'hold' for e in events) == 1, events
    assert sum(e.get('stage') == 'retired' for e in events) == 1, events
    assert all(e['token'] == evidence['token'] for e in events), events
    assert evidence['killed']['node'] in cfg['owned_nodes'], 'kill must name an owned original node'
    assert evidence['killed']['signal'] == 'SIGKILL'
    assert evidence['killed']['pid'] == cfg['owned_nodes'][evidence['killed']['node']]['pid']
    killed = evidence['killed']['node']
    for phase, names in [('survivor', owned_names - {killed}), ('restarted', owned_names)]:
        actual_runtime = evidence[phase]['storage_runtime']
        assert set(actual_runtime) == names
        for name, info in actual_runtime.items():
            pid = evidence['restarted']['restart_pid'] if name == killed else cfg['owned_nodes'][name]['pid']
            check_runtime(info, pid, cfg['native'])
    expected, raw_expected = expected_rows(keys), expected_raw(keys)
    for phase in ('survivor', 'restarted'):
        sample = evidence[phase]
        assert sample['sql'] == expected, (phase, sample)
        assert len(sample['table_runtime']) == 2
        for table in sample['table_runtime']:
            assert table == {'engine': cfg['engine'], 'is_sync': True, 'state_is_sync': True}, table
        assert sorted(sample['raw'][0] + sample['raw'][1]) == raw_expected, (phase, sample)
        assert sample['raw'][0] == sorted(r for r in raw_expected if r[0] in keys[:4]), sample
        assert sample['raw'][1] == sorted(r for r in raw_expected if r[0] in keys[4:]), sample
    assert evidence['restarted']['restarted_raw'] == sorted(r for r in raw_expected if r[0] in keys[:4])
    assert evidence['restarted']['caught_up'] is True
    target, actual = evidence['restarted']['catchup_target'], evidence['restarted']['catchup_actual']
    assert all(int(actual.get(str(k), actual.get(k, 0))) >= int(v) for k, v in target.items() if int(k) != 0)
    return {'submitted': len(manifest), 'target_mutations': ['ack-a', 'ack-b', 'refuse-a', 'refuse-b', 'conflict', 'unknown'],
            'result': 'finite induced-loss and one-node WAL restart reconciled'}


def wait(check, what, seconds=45):
    deadline, last = time.monotonic() + seconds, None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except Exception as error:
            last = repr(error)
        time.sleep(.1)
    raise AssertionError(f'timeout: {what}; last={last}')


def run(args):
    # Deliberately lazy imports: the pure oracle runs without SQL drivers or
    # Tarantool. outcome_client captures the actual PyMySQL ERR SQLSTATE.
    import outcome_client
    import pymysql
    from lazytest import ev
    from movetest import bucket_of
    from outcome_client import Client

    work = args.work
    cfg = json.loads((work / 'fixture.json').read_text())
    nodes = cfg['replicas']
    count = 32
    first = [[i for i in range(1, 10000) if (bucket_of(i, count)-1)*2//count == shard]
             for shard in range(2)]
    cfg['keys'] = first[0][:4] + first[1][:2]
    assert all(bucket(key) == bucket_of(key, count) for key in cfg['keys'])
    cfg['database'] = 'gc_effects'
    connections = []
    evidence = {}
    def checkpoint():
        tmp = work / 'effects.tmp'
        with tmp.open('w') as stream:
            json.dump(evidence, stream, indent=2)
            stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
        tmp.replace(work / 'effects.json')
    # Journal precedes the first connection/DDL, including constructor failure.
    journal = Journal(work / 'effects.jsonl', source=json.loads((work / 'source.json').read_text()), config=cfg)
    try:
        runtime_script = '''
local n=0; for _ in pairs(box.info.replication) do n=n+1 end
local spaces={}
for _,name in ipairs({'_tarn_txn','_tarn_part'}) do
 local s=box.space[name]; spaces[name]={is_sync=s.is_sync,state_is_sync=s.state.is_sync}
end
return {pid=box.info.pid,uuid=tostring(box.info.uuid),version=box.info.version,
 ro=box.info.ro,native=tarn_info().native,wal_mode=box.cfg.wal_mode,
 configured_replicas=#box.cfg.replication,observed_replicas=n,
 quorum=box.info.synchro.quorum,configured_quorum=box.cfg.replication_synchro_quorum,
 sync=tarn.cfg.sync,system_spaces=spaces}
'''
        clock_script = 'local out={}; for k,v in pairs(box.info.vclock) do out[tostring(k)]=v end; return out'
        def runtime(exclude=None):
            samples = {}
            for name, owned in cfg['owned_nodes'].items():
                if name == exclude:
                    continue
                info = ev(owned['endpoint'], runtime_script)
                samples[name] = info
                check_runtime(info, int((work / (name + '.pid')).read_text()), cfg['native'])
            return samples

        evidence['storage_runtime'] = runtime()
        for name, info in evidence['storage_runtime'].items():
            assert info['pid'] == cfg['owned_nodes'][name]['pid']

        def connect(endpoint, database=None):
            client = Client(endpoint, 'pymysql', database, timeout=15)
            connections.append(client)
            return client

        def submit(client, op):
            journal.intent(**op)
            try:
                result = client.query(op['sql'], op['params'])
            except pymysql.Error as error:
                pair = [error.args[0], getattr(outcome_client._capture, 'sqlstate', None)]
                kind = 'transport' if pair[1] is None else 'error'
                journal.complete(op['op_id'], kind=kind, raw={'error': pair, 'args': list(error.args)})
                wanted = {'refuse-commit': [1213, '40001'], 'unknown': [1180, '40003']}.get(op['op_id'])
                assert pair == wanted and kind == 'error', (op['op_id'], kind, pair)
                return pair
            except BaseException as error:
                journal.complete(op['op_id'], kind='transport', raw={'exception': repr(error)})
                raise
            journal.complete(op['op_id'], kind='response', raw={'result': result})
            assert op['op_id'] not in ('refuse-commit', 'unknown'), (op['op_id'], 'unexpected response', result)
            if op['sql'].startswith(('INSERT ', 'UPDATE ')):
                assert result == 1, (op['op_id'], result)
            return result

        manifest = operations(cfg['keys'], cfg['database'], cfg['engine'])
        client = connect(args.tarn)
        assert submit(client, manifest[0]) is not None
        client.close()
        client = connect(args.tarn, cfg['database'])
        other = connect(args.tarn, cfg['database'])
        for op in manifest[1:-1]:
            value = submit(other if op['op_id'] == 'conflict' else client, op)
            if op['op_id'] == 'refuse-commit':
                assert value == [1213, '40001'], value

        token = 'gc-effects-unknown'
        evidence['token'] = token
        plan = {'token': token, 'match': 'autocommit', 'fault': 'hold'}
        tmp = work / 'plan.tmp'
        tmp.write_text(json.dumps(plan)); tmp.replace(work / 'plan.json')

        def events():
            return [json.loads(line) for line in (work / 'proxy.jsonl').read_text().splitlines()
                    if line.strip() and json.loads(line)['token'] == token]

        result, failures = [], []

        def uncertain():
            try:
                result.append(submit(client, manifest[-1]))
            except BaseException as error:
                failures.append(error)

        worker = threading.Thread(target=uncertain)
        worker.start()
        try:
            wait(lambda: any(e['stage'] == 'injected' for e in events()), 'held successful mutation', 10)
            node = cfg['proxy_node']
            endpoint = cfg['owned_nodes'][node]['endpoint']
            pid = cfg['owned_nodes'][node]['pid']
            info = ev(endpoint, 'return {writable=not box.info.ro,pid=box.info.pid}')
            assert info['writable'] and info['pid'] == pid, 'proxy upstream identity/leader changed before kill'
            assert int((work / (node + '.pid')).read_text()) == pid
            evidence['held_proxy_events'] = events()
            evidence['process_loss_intent'] = {'node': node, 'pid': pid, 'signal': 'SIGKILL'}
            checkpoint()
            # Only a PID created in this fixture's fresh directory is eligible.
            os.kill(pid, signal.SIGKILL)
            evidence['killed'] = {'node': node, 'pid': pid, 'signal': 'SIGKILL'}
            checkpoint()
        finally:
            worker.join(20)
            (work / 'plan.json').unlink(missing_ok=True)
        assert not worker.is_alive() and not failures, failures
        assert result == [[1180, '40003']], result
        wait(lambda: any(e['stage'] == 'retired' for e in events()), 'held RPC retirement', 5)
        evidence['proxy_events'] = events()

        def leader(shard):
            for endpoint in nodes[shard]:
                if endpoint == cfg['owned_nodes'][evidence['killed']['node']]['endpoint']:
                    continue
                try:
                    if ev(endpoint, "return box.info.status == 'running' and not box.info.ro"):
                        return endpoint
                except Exception:
                    pass
            return None

        leaders = [wait(lambda s=s: leader(s), 'survivor leader ' + str(s)) for s in range(2)]
        recovery = connect(args.recovery, cfg['database'])
        raw_script = 'local name=...; local out={}; for _,r in box.space[name]:pairs() do out[#out+1]=r:totable() end; return out'

        def snapshot():
            # Only observational SELECTs retry. No submitted mutation is replayed.
            sql = wait(lambda: recovery.query('SELECT id,v FROM t ORDER BY id'), 'strong SQL read readiness')
            raw = [sorted(ev(n, raw_script, [cfg['database'] + '.t'])) for n in leaders]
            tables = [ev(n, 'local s=box.space[...]; return {engine=s.engine,is_sync=s.is_sync,state_is_sync=s.state.is_sync}',
                         [cfg['database'] + '.t']) for n in leaders]
            return {'sql': sql, 'raw': raw, 'table_runtime': tables}

        evidence['survivor'] = snapshot()
        evidence['survivor']['storage_runtime'] = runtime(exclude=evidence['killed']['node'])
        checkpoint()
        assert evidence['survivor']['sql'] == expected_rows(cfg['keys']), evidence
        killed = evidence['killed']['node']
        endpoint = cfg['owned_nodes'][killed]['endpoint']
        clock = ev(leaders[0], clock_script)
        with (work / 'restart.log').open('w') as log:
            subprocess.run([str(args.root / 'scripts/cluster.sh'), 'restart', '-d', str(work), killed],
                           stdout=log, stderr=subprocess.STDOUT, check=True)
        wait(lambda: subprocess.run([os.environ.get('TARANTOOL', 'tarantool'), str(args.root / 'scripts/joined.lua'),
                                     'tarn:tarn@' + endpoint, '3'], stdin=subprocess.DEVNULL,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0,
             'restarted original node joined')

        def caught_up():
            current = ev(endpoint, clock_script)
            return all(int(current.get(str(k), current.get(k, 0))) >= int(v)
                       for k, v in clock.items() if int(k) != 0)

        wait(caught_up, 'restarted original WAL node catches survivor vclock')
        evidence['restarted'] = snapshot()
        evidence['restarted'].update(restarted_raw=sorted(ev(endpoint, raw_script, [cfg['database'] + '.t'])),
                                     caught_up=True, catchup_target=clock, catchup_actual=ev(endpoint, clock_script),
                                     storage_runtime=runtime(), restart_pid=int((work / (killed + '.pid')).read_text()))
        report = reconcile(read(work / 'effects.jsonl'), evidence)
        (work / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    finally:
        checkpoint()
        journal.close()
        for client in connections:
            client.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--tarn', required=True)
    parser.add_argument('--recovery', required=True)
    args = parser.parse_args()
    try:
        run(args)
    except BaseException:
        (args.work / 'failure.txt').write_text(traceback.format_exc())
        raise
