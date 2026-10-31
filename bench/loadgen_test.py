#!/usr/bin/env python3
"""Negative controls for scheduling, exact effects and uncertain mutations."""
import argparse
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import pymysql
import loadgen as load


def arguments(**changes):
    values=dict(rows=4,row_bytes=4,keys_per_transaction=2,distribution='uniform',hot_keys=4,
                seed=7,workload='transaction',warmup=.1,seconds=.3,rate=10,
                max_operations=1000,audit_bytes=10485760,p99_ms=50,max_error_rate=.8,
                workers=1,queue=1,drain_timeout=2,database='tarn_load_control',isolation='SERIALIZABLE')
    values.update(changes)
    return argparse.Namespace(**values)


def write(path, rows):
    path.write_bytes(b''.join(load.encoded(row) for row in rows))


class Accounting(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='loadgen-control-')
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.args=arguments()
        payload=dict(payload_bytes=4,payload_sha256=load.sha(b'xxxx'))
        self.baseline=[dict(key=k,value=0,**payload) for k in range(4)]
        self.attempts=[]
        counts=[0]*4
        self.ledger=[]
        for token,outcome in enumerate(('acknowledged','queue_full','refused','acknowledged'),1):
            due=(token-1)/10
            self.attempts.append(dict(token=token,scheduled=due,started=due+.01 if outcome!='queue_full' else None,
                                     finished=due+.03,outcome=outcome))
            if outcome=='acknowledged':
                for key in load.fixture_keys(token,4,2,'uniform',4,7):
                    counts[key]+=1
                    self.ledger.append(dict(token=token,key=key))
        self.after=[dict(key=k,value=counts[k],**payload) for k in range(4)]

    def run_audit(self):
        write(self.root/'baseline.jsonl',self.baseline)
        write(self.root/'readback.jsonl',self.after)
        write(self.root/'ledger.jsonl',self.ledger)
        write(self.root/'worker-0.jsonl',self.attempts)
        write(self.root/'scheduler.jsonl',[])
        return load.audit(self.root,self.args)

    def test_exact_and_separate_warmup(self):
        result=self.run_audit()
        self.assertTrue(result['complete'])
        self.assertEqual(result['counts'],dict(acknowledged=1,queue_full=1,refused=1))
        self.assertAlmostEqual(result['scheduled_latency_ms']['p99'],30)
        self.assertAlmostEqual(result['service_latency_ms']['p99'],20)
        self.assertAlmostEqual(result['error_fraction'],2/3)

    def test_equal_sum_wrong_key(self):
        self.after[0]['value']+=1
        self.after[1]['value']-=1
        with self.assertRaisesRegex(AssertionError,'per-key'):
            self.run_audit()

    def test_refused_effect(self):
        self.ledger.append(dict(token=3,key=0))
        with self.assertRaisesRegex(AssertionError,'unexpected effect'):
            self.run_audit()

    def test_missing_acknowledged_effect(self):
        self.ledger.pop()
        with self.assertRaisesRegex(AssertionError,'absent'):
            self.run_audit()

    def test_duplicate_ledger_identity(self):
        self.ledger.append(self.ledger[0])
        with self.assertRaisesRegex(AssertionError,'duplicate'):
            self.run_audit()

    def test_duplicate_attempt(self):
        self.attempts.append(self.attempts[0])
        with self.assertRaises(load.sqlite3.IntegrityError):
            self.run_audit()

    def test_missing_arrival(self):
        self.attempts.pop(1)
        with self.assertRaisesRegex(AssertionError,'missing scheduled'):
            self.run_audit()

    def test_schedule_cannot_be_rebased_on_service_start(self):
        self.attempts[0]['scheduled']=.01
        with self.assertRaisesRegex(AssertionError,'arrival schedule'):
            self.run_audit()

    def test_payload_readback(self):
        self.after[0]['payload_sha256']='bad'
        with self.assertRaisesRegex(AssertionError,'payload'):
            self.run_audit()

    def test_unknown_is_never_qualified_even_without_effect(self):
        self.attempts[2]['outcome']='unknown'
        result=self.run_audit()
        self.assertFalse(result['complete'])
        self.assertFalse(result['objectives_pass'])

    def test_drain_is_counted_in_latency_and_goodput(self):
        self.attempts[-1]['finished']=1.1
        result=self.run_audit()
        self.assertAlmostEqual(result['verified_goodput_per_second'],1)
        self.assertAlmostEqual(result['scheduled_latency_ms']['p99'],800)
        self.assertFalse(result['objectives_pass'])

    def test_slo_goodput_counts_drain_even_when_latency_objective_allows_it(self):
        self.args.p99_ms=1000
        self.attempts[-1]['finished']=1.1
        result=self.run_audit()
        self.assertAlmostEqual(result['goodput_within_slo_per_second'],1)
        self.assertLessEqual(result['goodput_within_slo_per_second'],
                             result['verified_goodput_per_second'])


class Connection:
    def close(self):
        pass


class Scheduler(unittest.TestCase):
    def run_driver(self, operation, **changes):
        temp=tempfile.TemporaryDirectory(prefix='loadgen-driver-')
        self.addCleanup(temp.cleanup)
        root=Path(temp.name)
        args=arguments(warmup=0,seconds=.1,rate=500,**changes)
        journal=load.Journal(root,1048576)
        with patch.object(load,'sql',return_value=1):
            load.drive(args,'run',journal,connection_factory=lambda _:Connection(),operation=operation)
        rows=[r for p in root.glob('*.jsonl') for r in load.lines(p)]
        self.assertEqual(sorted(r['token'] for r in rows),list(range(1,51)))
        self.assertTrue(all(r['scheduled']==(r['token']-1)/500 for r in rows))
        return rows

    def test_full_queue_preserves_offered_arrivals(self):
        def slow(*_):
            time.sleep(.02)
            return dict(outcome='acknowledged')
        rows=self.run_driver(slow)
        self.assertTrue(any(r['outcome']=='queue_full' for r in rows))
        self.assertTrue(any(r['outcome']=='acknowledged' and r['started']-r['scheduled']>.01 for r in rows))

    def test_unknown_stops_new_mutations(self):
        calls=[]
        def uncertain(*_):
            calls.append(1)
            return dict(outcome='unknown')
        rows=self.run_driver(uncertain)
        self.assertEqual(len(calls),1)
        self.assertEqual(sum(r['outcome']=='unknown' for r in rows),1)
        # A full queue can reject a slot before the worker observes the lost
        # reply. Both classes are accounted; neither issues another mutation.
        self.assertEqual(sum(r['outcome'] in ('cancelled','queue_full') for r in rows),49)
        self.assertTrue(any(r['outcome']=='cancelled' for r in rows))

    def test_evidence_cap_and_seal(self):
        with tempfile.TemporaryDirectory() as temp:
            journal=load.Journal(Path(temp),10)
            with (Path(temp)/'out').open('xb') as output:
                with self.assertRaisesRegex(RuntimeError,'byte limit'):
                    journal.append(output,dict(long='more than ten bytes'))
                self.assertEqual(journal.used,0)
                journal.seal()
                with self.assertRaises(RuntimeError):
                    journal.append(output,{})


class Outcomes(unittest.TestCase):
    @staticmethod
    def refusal(code):
        error=pymysql.OperationalError(code,'confirmed refusal')
        error.sqlstate='40001' if code==1213 else 'HY000'
        return error

    def attempt(self, failure, rollback_failure=False):
        statements=[]
        def query(_,statement,parameters=None):
            statements.append(statement)
            if statement=='COMMIT':
                raise failure
            if statement=='ROLLBACK' and rollback_failure:
                raise pymysql.OperationalError(2013,'lost rollback reply')
            return 1
        with patch.object(load,'sql',side_effect=query):
            result=load.execute(Connection(),arguments(),1,[0,1],'run')
        self.assertEqual(statements.count('BEGIN'),1)
        self.assertEqual(statements.count('COMMIT'),1)
        return result,statements

    def test_commit_reply_loss_is_not_replayed_or_rolled_back(self):
        result,statements=self.attempt(pymysql.OperationalError(2013,'lost commit reply'))
        self.assertEqual(result['outcome'],'unknown')
        self.assertNotIn('ROLLBACK',statements)

    def test_confirmed_refusal_requires_confirmed_rollback(self):
        result,statements=self.attempt(self.refusal(1213))
        self.assertEqual(result['outcome'],'refused')
        self.assertEqual(statements.count('ROLLBACK'),1)

    def test_failed_refusal_cleanup_remains_unknown(self):
        result,_=self.attempt(self.refusal(1205),True)
        self.assertEqual(result['outcome'],'unknown')

    def test_unknown_sqlstate_never_uses_refusal_code_alone(self):
        error=self.refusal(1205)
        error.sqlstate='40003'
        result,statements=self.attempt(error)
        self.assertEqual(result['outcome'],'unknown')
        self.assertNotIn('ROLLBACK',statements)

    def test_wire_sqlstate_is_retained(self):
        with load.error_states():
            with self.assertRaises(pymysql.MySQLError) as caught:
                pymysql.err.raise_mysql_exception(b'\xff'+(1180).to_bytes(2,'little')+b'#40003outcome unknown')
            self.assertEqual(caught.exception.sqlstate,'40003')


if __name__=='__main__':
    unittest.main()
