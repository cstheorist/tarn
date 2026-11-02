#!/usr/bin/env python3
"""Negative controls for exact benchmark accounting, including equal sums."""
import unittest
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


if __name__ == '__main__':
    unittest.main()
