#!/usr/bin/env python3
"""Executable outcome/retry contract on an isolated RF3/fsync cluster.

Run through PyMySQL text, Connector/Python text and binary prepared SQL.
Every mutation has one application attempt. Faults consume a real matching
storage success before changing/losing its reply. Receipts retain failures.
"""
import argparse
import importlib.metadata
import json
from pathlib import Path
import time
import uuid
import pymysql
import mysql.connector

from outcome_client import Client
from lazytest import ev, rpc
from lazyfaults import Lease, Worker, wait_for, decision_node
from movetest import bucket_of

MODES = ('pymysql', 'connector', 'prepared')
FAULTS = ('lost', 'malformed_error', 'malformed_handback', 'extra_return',
          'missing_data', 'invalid_code', 'unknown', 'before_send')


class Fixture:
    def __init__(self, args, mode):
        self.args, self.mode = args, mode
        self.db = 'outcomes_' + uuid.uuid4().hex[:12]
        self.connections = []
        self.nodes = args.shards.split(',')
        self.c = self.connect(database=None)
        self.c.query('CREATE DATABASE ' + self.db)
        self.c.query('USE ' + self.db)
        self.c.query('CREATE TABLE t (id INT PRIMARY KEY, k INT, v INT NOT NULL) ENGINE=' + args.engine)
        count = int(self.c.query("SHOW STATUS LIKE 'Tarn_buckets'")[0][1])
        self.keys = [[i for i in range(1, 10000) if (bucket_of(i, count)-1)*2//count == s][:8]
                     for s in range(2)]
        for key in self.keys[0] + self.keys[1]:
            self.c.query('INSERT INTO t VALUES (%s,0,0)', (key,))
        self.key, self.other = self.keys[0][:2]
        self.observer = self.connect()
        self.bucket_count = count
        self.space = self.db + '.t'
        self.identities = [ev(node, "return {id=tarn.cfg.shard_id,uuid=box.info.uuid}") for node in self.nodes]

    def connect(self, database=True, timeout=10):
        c = Client(self.args.tarn, self.mode, self.db if database is True else database, timeout)
        self.connections.append(c)
        return c

    def arm(self, fault, match):
        token = uuid.uuid4().hex
        data = {'token': token, 'fault': fault, 'match': match}
        tmp = self.args.plan.with_suffix('.tmp')
        tmp.write_text(json.dumps(data))
        tmp.replace(self.args.plan)
        return token

    def disarm(self):
        self.args.plan.unlink(missing_ok=True)

    def events(self, token):
        return [e for line in self.args.proxy_log.read_text().splitlines()
                if (e := json.loads(line))['token'] == token]

    def value(self, key=None):
        deadline = time.monotonic() + 5
        while True:
            try:
                return self.observer.query('SELECT v FROM t WHERE id=%s', (key or self.key,))[0][0]
            except (pymysql.Error, mysql.connector.Error) as e:
                code = e.errno if isinstance(e, mysql.connector.Error) else e.args[0]
                if code != 1213 or time.monotonic() >= deadline:
                    raise
                # The fault can retire a pooled connection shared with this
                # independent read. Its retry never includes a user mutation.
                self.record('read_verification_interrupted', code=code, message=str(e))
                time.sleep(.02)

    def reset(self):
        self.disarm()
        self.c.query('ROLLBACK')
        self.c.query("SET SESSION transaction_isolation='SERIALIZABLE'")
        for key in (self.key, self.other, self.keys[1][0]):
            self.c.query('UPDATE t SET v=0 WHERE id=%s', (key,))

    def record(self, event, **values):
        # Lease evidence joins the same durable event stream as SQL cases.
        with self.args.events.open('a') as f:
            f.write(json.dumps({'driver': self.mode, 'event': event, **values})+'\n')

    def close(self):
        self.disarm()
        for c in self.connections:
            c.close()


def case_statement_rollback(f):
    f.reset()
    c = f.c
    c.query('BEGIN')
    c.query('UPDATE t SET v=7 WHERE id=%s', (f.other,))
    error = c.error('INSERT INTO t VALUES (%s,0,99)', (f.key,))
    assert error == [1062, '23000'], error
    assert c.active(), 'duplicate key must preserve the preceding transaction'
    c.query('COMMIT')
    assert not c.active() and f.value() == 0 and f.value(f.other) == 7
    return {'error': error, 'preceding_write': 7, 'failed_statement': 0}


def case_known_abort(f):
    f.reset()
    c = f.c
    c.query('BEGIN')
    c.query('SELECT v FROM t WHERE id=%s', (f.key,))
    c.query('UPDATE t SET v=7 WHERE id=%s', (f.other,))
    f.observer.query('UPDATE t SET v=1 WHERE id=%s', (f.key,))
    error = c.error('COMMIT')
    assert error == [1213, '40001'], error
    observed = {'active': c.active(), 'other': f.value(f.other), 'target': f.value()}
    assert observed == {'active': False, 'other': 0, 'target': 1}, observed
    return {'error': error, 'whole_transaction_rolled_back': True}


def case_lock_timeout(f):
    f.reset()
    c, holder = f.c, f.connect()
    for x in (c, holder):
        x.query("SET SESSION transaction_isolation='READ-COMMITTED'")
    holder.query('BEGIN')
    holder.query('UPDATE t SET v=9 WHERE id=%s', (f.key,))
    try:
        c.query('BEGIN')
        c.query('UPDATE t SET v=7 WHERE id=%s', (f.other,))
        c.query('SET innodb_lock_wait_timeout=1')
        error = c.error('UPDATE t SET v=v+1 WHERE id=%s', (f.key,))
        assert error == [1205, 'HY000'], error
        assert c.active()
        c.query('COMMIT')
    finally:
        holder.query('ROLLBACK')
        holder.close()
    assert f.value() == 0 and f.value(f.other) == 7
    return {'error': error, 'preceding_write': 7, 'timed_out_statement': 0}


def case_mutation_replies(f):
    receipts = []
    for path in ('autocommit', 'local', 'rc'):
        for fault in FAULTS:
            f.reset()
            c = f.c
            if path != 'autocommit':
                if path == 'rc':
                    c.query("SET SESSION transaction_isolation='READ-COMMITTED'")
                c.query('BEGIN')
                c.query('UPDATE t SET v=v+1 WHERE id=%s', (f.key,))
            token = f.arm(fault, 'autocommit' if path == 'autocommit' else 'commit_' + ('rc' if path == 'rc' else 'local'))
            try:
                error = (c.error('UPDATE t SET v=v+1 WHERE id=%s', (f.key,)) if path == 'autocommit'
                         else c.error('COMMIT'))
            finally:
                f.disarm()
            assert error == [1180, '40003'], (path, fault, error)
            assert not c.active(), (path, fault, 'transaction retained after uncertain COMMIT')
            value = f.value()
            assert value == (0 if fault == 'before_send' else 1), (path, fault, value)
            events = f.events(token)
            completed = sum(e.get('committed', False) for e in events)
            assert completed == (0 if fault == 'before_send' else 1), events
            receipts.append({'path': path, 'fault': fault, 'error': error, 'value': value,
                             'completed_mutation_replies': completed, 'proxy_events': events})
    return receipts


def case_client_loss(f):
    f.reset()
    c = f.connect(timeout=1)
    token = f.arm('hold', 'autocommit')
    try:
        error = c.error('UPDATE t SET v=v+1 WHERE id=%s', (f.key,))
        # A lost SQL connection cannot deliver Tarn's ERR packet/SQLSTATE.
        assert error[0] in (2013, 3024), error
    finally:
        c.close()
        f.disarm()
    assert f.value() == 1
    events = f.events(token)
    assert sum(e.get('committed', False) for e in events) == 1, events
    return {'client_error': error, 'outcome': 'unknown', 'value': 1, 'proxy_events': events}


def case_pending_apply(f):
    f.reset()
    remote = f.keys[1][0]
    with Lease(f, remote) as lease:
        f.c.query('BEGIN')
        f.c.query('UPDATE t SET v=v+1 WHERE id=%s', (f.key,))
        f.c.query('UPDATE t SET v=v+1 WHERE id=%s', (remote,))
        f.c.query('COMMIT')
        lease.known_committed = True
        assert not f.c.active()
        state = lease.snapshot()
        assert state['state'] == 'prepared' and state['row'][2] == 0, state
        error = f.observer.error('SELECT v FROM t WHERE id=%s', (remote,))
        assert error == [1213, '40001'], error
        # The raw native tuple is still the previous committed row. This is
        # not a failed COMMIT and cannot justify re-executing either update.
        lease.release()
        for node in f.nodes:
            result = rpc(node, 'tarn_apply', [state['txid']])
            assert result[:2] in (['ok', 'applied'], ['ok', 'none']), result
        assert f.value() == f.value(remote) == 1
    return {'commit': 'ok', 'raw_before_apply': 0, 'strong_before_apply': error,
            'after_apply': [1, 1], 'txid': state['txid']}


def case_prepare_outcomes(f):
    receipts = []
    for known_decision in (False, True):
        f.reset()
        remote = f.keys[1][0]
        with Lease(f, remote, lose_reply=True) as lease:
            f.c.query('BEGIN')
            for key in (f.key, remote):
                f.c.query('UPDATE t SET v=v+1 WHERE id=%s', (key,))
            worker = Worker(lambda: f.c.query('COMMIT') if known_decision else f.c.error('COMMIT'))
            try:
                state = wait_for(lambda: (s if (s := lease.snapshot())['prepared'] else None),
                                 5, 'durable remote YES')
                txid = state['txid']
                for node in f.nodes:
                    wait_for(lambda n=node: ev(n, "local p=box.space._tarn_part:get{...}; "
                                              "return p and p.state or 'none'", [txid]) == 'prepared',
                             5, 'confirmed YES at every participant')
                home = decision_node(f.nodes, f.identities, state)

                def decide():
                    # This fault fixture advances only its owned unanimous
                    # transaction, using the ordinary idempotent decision API.
                    rpc(home, 'tarn_txn_cas', [txid, 'staging', 'committed'])
                    assert ev(home, "local r=box.space._tarn_txn:get{...}; return r and r.status",
                              [txid]) == 'committed'
                    lease.known_committed = True

                if known_decision:
                    decide()
                lease.release_prepare()
                outcome = worker.finish()
                if not known_decision:
                    assert outcome == [1180, '40003'], outcome
                    decide()
                assert not f.c.active()
                lease.release()
                for node in f.nodes:
                    assert rpc(node, 'tarn_apply', [txid])[:2] == ['ok', 'applied']
                assert f.value() == f.value(remote) == 1
                receipts.append({'decision_known_to_client': known_decision,
                                 'outcome': 'ok' if known_decision else outcome,
                                 'final_values': [1, 1], 'txid': txid})
            finally:
                lease.release_prepare()
                worker.finish()
    return receipts


def case_operation_id(f):
    f.reset()
    c = f.c
    c.query('CREATE TABLE operations (op_id INT PRIMARY KEY, target_id INT, delta INT, result INT) ENGINE=' + f.args.engine)
    c.query('BEGIN')
    c.query('INSERT INTO operations VALUES (%s,%s,1,1)', (f.key, f.key))
    c.query('UPDATE t SET v=v+1 WHERE id=%s', (f.key,))
    token = f.arm('lost', 'commit_local')
    try:
        error = c.error('COMMIT')
    finally:
        f.disarm()
    assert error == [1180, '40003'], error
    retry = f.connect()
    retry.query('BEGIN')
    duplicate = retry.error('INSERT INTO operations VALUES (%s,%s,1,1)', (f.key, f.key))
    assert duplicate == [1062, '23000'], duplicate
    retry.query('ROLLBACK')
    record = retry.query('SELECT target_id,delta,result FROM operations WHERE op_id=%s', (f.key,))
    assert record == [[f.key, 1, 1]] and f.value() == 1
    events = f.events(token)
    assert sum(e.get('committed', False) for e in events) == 1, events
    return {'initial_error': error, 'duplicate': duplicate, 'durable_result': record, 'value': 1}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--tarn', required=True)
    ap.add_argument('--shards', required=True)
    ap.add_argument('--engine', choices=('memtx', 'vinyl'), default='memtx')
    ap.add_argument('--plan', type=Path, required=True)
    ap.add_argument('--proxy-log', type=Path, required=True)
    ap.add_argument('--evidence', type=Path, required=True)
    ap.add_argument('--mode', choices=MODES)
    args = ap.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=True)
    args.events = args.evidence / 'events.jsonl'
    manifest = json.loads((Path(__file__).resolve().parents[2] / 'contracts/outcomes.json').read_text())
    result = {'schema': 1, 'arguments': {k: str(v) for k, v in vars(args).items()},
              'clients': {p: importlib.metadata.version(p) for p in ('PyMySQL', 'mysql-connector-python', 'msgpack')},
              'cases': [], 'complete': False}
    output = args.evidence / 'results.json'
    try:
        for mode in ([args.mode] if args.mode else MODES):
            f = Fixture(args, mode)
            try:
                for entry in manifest['contracts']:
                    name = entry['id']
                    event = {'driver': mode, 'case': name, 'passed': False}
                    result['cases'].append(event)
                    try:
                        event['result'] = globals()['case_' + name](f)
                        event['passed'] = True
                        print(mode, name, 'PASS', flush=True)
                    except Exception as e:
                        event['error'] = repr(e)
                        raise
                    finally:
                        output.write_text(json.dumps(result, indent=2) + '\n')
            finally:
                f.close()
        result['complete'] = True
    finally:
        output.write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
