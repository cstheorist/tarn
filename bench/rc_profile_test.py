#!/usr/bin/env python3
"""Negative controls for exact benchmark accounting, including equal sums."""
import unittest
import argparse
import json
from pathlib import Path
import tempfile
import threading
from unittest.mock import patch
import rc_profile
from rc_profile import diagnostic, reconcile, require_production_flags


class Accounting(unittest.TestCase):
    def setUp(self):
        self.records = [dict(token=1, key=0, outcome='ok'), dict(token=2, key=1, outcome='refused')]
        self.counters = [(k, int(k == 0)) for k in range(8)]

    def test_exact(self):
        self.assertTrue(reconcile(self.records, self.counters, [(1, 0)])['exact_counters'])

    def test_equal_sum_wrong_key(self):
        with self.assertRaisesRegex(AssertionError, 'per-key'):
            reconcile(self.records, [(k, int(k == 1)) for k in range(8)], [(1, 0)])

    def test_refused_effect(self):
        with self.assertRaisesRegex(AssertionError, 'ledger'):
            reconcile(self.records, self.counters, [(2, 0)])

    def test_unknown_is_never_success(self):
        with self.assertRaisesRegex(AssertionError, 'unknown'):
            reconcile(self.records + [dict(token=3, key=0, outcome='unknown')], self.counters, [(1, 0)])

    def test_diagnostic_failure_does_not_discard_effect_audit(self):
        record = {}
        def missing_process():
            raise FileNotFoundError('process exited')
        self.assertIsNone(diagnostic(record, 'cpu', missing_process))
        audit = diagnostic(record, 'readback', lambda: reconcile(self.records, self.counters, [(1, 0)]))
        self.assertTrue(audit['exact_ledger'])
        self.assertEqual(record['diagnostic_errors'][0]['stage'], 'cpu')


class RuntimeFlags(unittest.TestCase):
    def test_effective_production_flags(self):
        require_production_flags({'test_flags': {'check_locks': False, 'check_yields': False}})

    def test_instrumented_or_missing_runtime_is_rejected(self):
        for name in ('check_locks', 'check_yields'):
            for value in (True, '0', 0, None):
                with self.subTest(name=name, value=value), self.assertRaises(AssertionError):
                    flags = {'check_locks': False, 'check_yields': False}
                    flags[name] = value
                    require_production_flags({'test_flags': flags})
        with self.assertRaises(AssertionError):
            require_production_flags({})


class UnsettledAttempt(unittest.TestCase):
    def exercise(self, interrupted):
        """A COMMIT can take effect while its reply never reaches the worker."""
        dispatched, release = threading.Event(), threading.Event()
        workers, statements, applied = [], [], []
        real_thread = threading.Thread

        class BoundedThread(real_thread):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                workers.append(self)

            def join(self, timeout=None):
                if not dispatched.wait(2):
                    raise AssertionError('fake COMMIT was not dispatched')
                if interrupted:
                    raise KeyboardInterrupt('injected controller interruption')
                # Simulate expiry of the controller's bounded join budget.

        class Connection:
            def close(self):
                pass

        def query(connection, sql, args=None):
            if threading.current_thread() in workers:
                statements.append(sql)
                if sql.startswith('INSERT INTO ledger'):
                    applied.append((args[0], args[1]))
                if sql == 'COMMIT':
                    dispatched.set()
                    if not release.wait(3):
                        raise RuntimeError('fake reply watchdog expired')
            elif sql.startswith('SELECT id,v'):
                return [(key, sum(k == key for _, k in applied)) for key in range(8)]
            elif sql.startswith('SELECT token,k'):
                return list(applied)
            return []

        rows = [dict(address=str(i), result=dict(
            info=dict(ro=i >= 2, native=False), wal='fsync', quorum='N / 2 + 1',
            engine='memtx', pid=i,
            test_flags=dict(check_locks=False, check_yields=False))) for i in range(6)]
        args = argparse.Namespace(sql='fake:1', seconds=1, engine='memtx', native='disabled')
        evidence = dict(samples=[])
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'baseline.json'
            try:
                with patch.object(rc_profile, 'connect', return_value=Connection()), \
                     patch.object(rc_profile, 'query', side_effect=query), \
                     patch.object(rc_profile, 'evaluate', return_value=rows), \
                     patch.object(rc_profile, 'cpu', return_value={}), \
                     patch.object(rc_profile.threading, 'Thread', BoundedThread):
                    with self.assertRaisesRegex(AssertionError, 'workers did not settle'):
                        rc_profile.sample(args, 1, 'uncontended', False, [], output, evidence)
                saved = json.loads(output.read_text())['samples'][0]
                attempts = [json.loads(line) for line in
                            (output.parent / saved['attempts_file']).read_text().splitlines()]
                self.assertEqual(len(attempts), 1)
                self.assertEqual(attempts[0]['outcome'], 'unknown')
                self.assertEqual(saved['readback']['ledger'], [[attempts[0]['token'], 0]])
                self.assertFalse(saved['complete'])
                self.assertTrue(saved['workers_live'])
                self.assertEqual(statements.count('BEGIN'), 1)
                self.assertEqual(statements.count('COMMIT'), 1)
                self.assertNotIn('ROLLBACK', statements)
                self.assertEqual(bool(saved['errors']), interrupted)
            finally:
                release.set()
                for worker in workers:
                    real_thread.join(worker, 2)

    def test_lost_reply_after_apply_is_retained_as_unknown(self):
        self.exercise(interrupted=False)

    def test_controller_interrupt_preserves_attempt_and_readback(self):
        self.exercise(interrupted=True)


if __name__ == '__main__':
    unittest.main()
