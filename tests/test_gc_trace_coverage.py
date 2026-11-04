#!/usr/bin/env python3
"""Offline receipt and process-limit controls; no Lua runtime is executed."""
import copy
import importlib.util
import json
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
SPEC = importlib.util.spec_from_file_location('gc_trace_coverage', ROOT / 'scripts/gc_trace_coverage.py')
COVERAGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COVERAGE)


def passing():
    n = COVERAGE.ITERATIONS
    return {'schema_version': 1, 'profile': 'ordinary', 'source_identities': {'test': 'fixture'},
            'complete': True, 'passed': True,
            'runtime': {'os': 'Linux', 'arch': 'x64', 'gc64': False,
                        'effective_jit': {'enabled': True}},
            'config': {'iterations': n, 'max_iterations': 1000000, 'batch_size': 1,
                       'constructor_warming': False, 'retained_engines': False,
                       'native': False, 'sync': False, 'transactions': False,
                       'wal_mode': 'none', 'cut_sequence': 'false then true', 'gc': 'default'},
            'counts': {'iterations': n, 'constructors': 1, 'marks': n + 1, 'cuts': n * 3,
                       'invalidations': n, 'rebuilds': n, 'blocked_rebuilds': n, 'gc_steps': 0}}


class TraceControls(unittest.TestCase):
    def test_fixed_bounds_and_audited_identities(self):
        self.assertEqual(COVERAGE.REUSE_RUN, 41764862465)
        self.assertEqual(COVERAGE.PROFILES,
                         ('ordinary', 'construction-warm', 'retained-construction-warm'))
        self.assertEqual((COVERAGE.ITERATIONS, COVERAGE.TIMEOUT, COVERAGE.FILE_LIMIT),
                         (20000, 30, 8 * 1024 * 1024))
        self.assertEqual(COVERAGE.AUDITED['baseline']['runtime_receipt_sha256'],
                         '427e6f03241c664e4c395dcaaec5e59c8ed7f0849d598afb5393ca1fbd981a43')
        self.assertEqual(COVERAGE.AUDITED['candidate']['binary_sha256'],
                         '631b4615920d01d1439fc74ed9825d7d4889384b6aac6f1b38fa8e73ff16fbbd')

    def test_rejects_self_consistent_unaudited_runtime_before_execution(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'runtime-receipt.json').write_text('{}')
            with patch.object(COVERAGE.subprocess, 'Popen') as spawn:
                with self.assertRaisesRegex(ValueError, 'audited reuse artifact'):
                    COVERAGE.verify_audited_runtime(root, 'baseline', root / 'patch')
                spawn.assert_not_called()

    def test_checks_binary_arm_and_every_runtime_evidence_hash(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'bin').mkdir()
            (root / 'bin/tarantool').write_bytes(b'controlled binary')
            evidence = root / 'evidence.txt'
            evidence.write_bytes(b'controlled evidence')
            patchfile = root / 'patch'
            patchfile.write_bytes(b'controlled patch')
            receipt = {'schema_version': 1, 'arm': 'baseline',
                       'tarantool_commit': COVERAGE.verify_runtime.__globals__['TARANTOOL'],
                       'luajit_commit': COVERAGE.verify_runtime.__globals__['LUAJIT'],
                       'build_type': 'RelWithDebInfo', 'patch_sha256': None,
                       'binary_sha256': COVERAGE.sha(root / 'bin/tarantool'),
                       'evidence_sha256': {'evidence.txt': COVERAGE.sha(evidence)}}
            receiptfile = root / 'runtime-receipt.json'
            receiptfile.write_text(json.dumps(receipt))
            expected = {'baseline': {'binary_sha256': receipt['binary_sha256'],
                                    'runtime_receipt_sha256': COVERAGE.sha(receiptfile)}}
            with patch.object(COVERAGE, 'AUDITED', expected):
                self.assertEqual(COVERAGE.verify_audited_runtime(root, 'baseline', patchfile), receipt)
                evidence.write_bytes(b'changed evidence')
                with self.assertRaisesRegex(ValueError, 'evidence mismatch'):
                    COVERAGE.verify_audited_runtime(root, 'baseline', patchfile)
                evidence.unlink()
                with self.assertRaises(OSError):
                    COVERAGE.verify_audited_runtime(root, 'baseline', patchfile)
                (root / 'bin/tarantool').write_bytes(b'changed binary')
                with self.assertRaisesRegex(ValueError, 'binary differs'):
                    COVERAGE.verify_audited_runtime(root, 'baseline', patchfile)

    def child(self, body, timeout=None):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            binary = root / 'fake-runtime'
            binary.write_text('#!' + sys.executable + '\n' + body)
            binary.chmod(0o700)
            with patch.object(COVERAGE, 'TIMEOUT', timeout or COVERAGE.TIMEOUT):
                result = COVERAGE.collect_trial(binary, root / 'fixture', root / 'wrapper',
                                                root / 'trial', 'ordinary', {'test': 'fixture'})
            result['saved_invocation'] = json.loads((root / 'trial/invocation.json').read_text())
            self.assertEqual(result['invocation_sha256'], COVERAGE.sha(root / 'trial/invocation.json'))
            for name, info in result['files'].items():
                self.assertEqual(info['sha256'], COVERAGE.sha(root / 'trial' / name))
            return result

    def test_functional_pass_and_dump_are_independent_of_target_coverage(self):
        body = ("import json, os, resource\n"
                "assert resource.getrlimit(resource.RLIMIT_CORE) == (0, 0)\n"
                "assert resource.getrlimit(resource.RLIMIT_FSIZE) == (8388608, 8388608)\n"
                "open(os.environ['TARN_GC_TRACE_DUMP'], 'w').write('controlled trace')\n"
                'print(json.dumps(' + repr(passing()) + '))\n')
        result = self.child(body)
        self.assertTrue(result['observer_complete'])
        self.assertEqual(result['functional']['outcome'], 'pass')
        self.assertIn('unassessed', result['target_coverage'])
        self.assertEqual(result['saved_invocation']['dump_mode'], 'tbiT')
        self.assertEqual(result['saved_invocation']['iterations'], 20000)

    def test_missing_dump_cannot_claim_observer_completion(self):
        result = self.child('import json\nprint(json.dumps(' + repr(passing()) + '))\n')
        self.assertEqual(result['observer_outcome'], 'incomplete_dump')
        self.assertEqual(result['functional']['outcome'], 'pass')
        self.assertFalse(result['observer_complete'])

    def test_nonzero_exit_or_wrong_identity_cannot_reuse_pass(self):
        bad = copy.deepcopy(passing())
        bad['source_identities'] = {'test': 'different'}
        for record, exitcode in ((passing(), 3), (bad, 0)):
            with self.subTest(exitcode=exitcode):
                result = self.child('import json\nprint(json.dumps(' + repr(record) +
                                    '))\nraise SystemExit(' + str(exitcode) + ')\n')
                self.assertEqual(result['functional']['outcome'], 'invalid_receipt')

    def test_file_limit_is_observer_incomplete_not_runtime_cause(self):
        result = self.child("import os, signal\nsignal.signal(signal.SIGXFSZ, signal.SIG_DFL)\n"
                            "f = os.open(os.environ['TARN_GC_TRACE_DUMP'], "
                            "os.O_WRONLY | os.O_CREAT, 0o600)\n"
                            "for i in range(10): os.write(f, b'x' * 1048576)\n")
        self.assertEqual(result['returncode'], -signal.SIGXFSZ)
        self.assertEqual(result['observer_outcome'], 'incomplete_file_limit')
        self.assertFalse(result['observer_complete'])
        self.assertEqual(result['files']['trace.dump']['bytes'], COVERAGE.FILE_LIMIT)

    def test_timeout_is_observer_incomplete(self):
        result = self.child('import time\ntime.sleep(5)\n', timeout=0.05)
        self.assertEqual(result['observer_outcome'], 'incomplete_timeout')
        self.assertTrue(result['timed_out'])
        self.assertFalse(result['observer_complete'])


if __name__ == '__main__':
    unittest.main()
