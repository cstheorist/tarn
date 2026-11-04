#!/usr/bin/env python3
"""Offline functional-control receipts, owned fake children and fixed-batch checks."""
import copy
import importlib.util
import json
from pathlib import Path
import signal
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
SPEC = importlib.util.spec_from_file_location('gc_upstream_control', ROOT / 'scripts/gc_upstream_control.py')
CONTROL = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONTROL)
IDENTITIES = {'test': 'fixture'}


def records(profile='on'):
    preflight = {'schema_version': 1, 'fixture': 'luajit-1085-functional-control',
                 'kind': 'preflight', 'profile': profile, 'source_identities': IDENTITIES,
                 'config': copy.deepcopy(CONTROL.CONFIG),
                 'runtime': {'tarantool': '3.9.0-test', 'luajit': 'LuaJIT 2.1', 'os': 'Linux',
                             'arch': 'x64', 'gc64': False,
                             'initial_jit': {'enabled': True, 'flags': ['SSE2']},
                             'effective_jit': {'enabled': profile == 'on', 'flags': ['SSE2']}}}
    completion = {**copy.deepcopy(preflight), 'kind': 'completion', 'complete': True,
                  'passed': True, 'nodes_checked': 300,
                  'final_jit': copy.deepcopy(preflight['runtime']['effective_jit'])}
    return preflight, completion


def emit(*receipts):
    return 'import json\n' + ''.join('print(json.dumps(' + repr(r) + '), flush=True)\n'
                                      for r in receipts)


class UpstreamControls(unittest.TestCase):
    def child(self, body, timeout=None):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            binary = root / 'fake-runtime'
            binary.write_text('#!' + sys.executable + '\n' + body)
            binary.chmod(0o700)
            with patch.object(CONTROL, 'TIMEOUT', timeout or CONTROL.TIMEOUT):
                result = CONTROL.collect_trial(binary, root / 'fixture', root / 'trial',
                                               'on', IDENTITIES)
            result['invocation'] = json.loads((root / 'trial/invocation.json').read_text())
            self.assertEqual(result['invocation_sha256'], CONTROL.sha(root / 'trial/invocation.json'))
            for name, info in result['files'].items():
                self.assertEqual(info['sha256'], CONTROL.sha(root / 'trial' / name))
            self.assertFalse(result['qualification_pass'])
            return result

    def test_fixed_batch_and_limits_with_complete_pass(self):
        self.assertEqual((CONTROL.PROFILES, CONTROL.TRIALS, CONTROL.TIMEOUT), (('on', 'off'), 3, 10))
        result = self.child('import resource\n'
                            'assert resource.getrlimit(resource.RLIMIT_CORE) == (0, 0)\n'
                            'assert resource.getrlimit(resource.RLIMIT_FSIZE) == (8388608, 8388608)\n'
                            + emit(*records()))
        self.assertTrue(result['collection_complete'])
        self.assertEqual(result['functional']['outcome'], 'pass')
        self.assertEqual(result['invocation']['config']['total_stores'], 300)
        self.assertEqual(result['invocation']['core_limit_bytes'], 0)

    def test_invariant_failure_is_collected_separately(self):
        pre, final = records()
        final.update(passed=False, nodes_checked=17, error='controlled chain mismatch')
        result = self.child(emit(pre, final) + 'raise SystemExit(1)\n')
        self.assertTrue(result['collection_complete'])
        self.assertEqual(result['functional']['outcome'], 'invariant_failure')

    def test_invalid_or_missing_completion_cannot_pass(self):
        pre, final = records()
        variants = []
        for field, value in (('complete', False), ('nodes_checked', 299),
                             ('source_identities', {'test': 'changed'}), ('final_jit', {})):
            bad = copy.deepcopy(final)
            bad[field] = value
            variants.append(emit(pre, bad))
        variants.extend((emit(pre), emit(pre, final) + 'raise SystemExit(3)\n',
                         emit(pre) + "print('partial JSON')\n"))
        for body in variants:
            with self.subTest(body=body):
                result = self.child(body)
                self.assertEqual(result['functional']['outcome'], 'invalid_receipt')
                self.assertFalse(result['collection_complete'])

    def test_preflight_configuration_and_identity_are_required_before_process_loss(self):
        pre, _ = records()
        for key, value in (('source_identities', {}), ('runtime', {}), ('config', {})):
            bad = copy.deepcopy(pre)
            bad[key] = value
            result = self.child(emit(bad))
            self.assertEqual(result['functional']['outcome'], 'setup_failure')
        loss = 'import os, signal\nos.kill(os.getpid(), signal.SIGTERM)\n'
        absent = self.child(loss)
        present = self.child(emit(pre) + "print('partial', flush=True)\n" + loss)
        self.assertEqual(absent['functional']['outcome'], 'setup_failure')
        self.assertFalse(absent['collection_complete'])
        self.assertEqual(present['functional']['outcome'], 'process_loss')
        self.assertTrue(present['collection_complete'])
        self.assertEqual(present['signal'], signal.SIGTERM)
        self.assertEqual(present['returncode'], -signal.SIGTERM)

    def test_timeout_and_file_cap_are_incomplete(self):
        pre, _ = records()
        timeout = self.child(emit(pre) + 'import time\ntime.sleep(5)\n', timeout=0.1)
        self.assertTrue(timeout['timed_out'])
        self.assertFalse(timeout['collection_complete'])
        self.assertEqual(timeout['functional']['outcome'], 'timeout')
        capped = self.child(emit(pre) + 'import os\n'
                            "for i in range(10): os.write(2, b'x' * 1048576)\n")
        self.assertEqual(capped['functional']['outcome'], 'file_limit')
        self.assertFalse(capped['collection_complete'])
        self.assertEqual(capped['files']['stderr.log']['bytes'], CONTROL.FILE_LIMIT)

    def test_launch_failure_retains_empty_raw_logs_and_invocation(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            result = CONTROL.collect_trial(root / 'missing-runtime', root / 'fixture',
                                           root / 'trial', 'on', IDENTITIES)
            self.assertEqual(result['functional']['outcome'], 'setup_failure')
            self.assertFalse(result['collection_complete'])
            self.assertIsNone(result['pid'])
            self.assertIsNone(result['returncode'])
            self.assertTrue(result['launch_error'])
            self.assertEqual(set(result['files']), {'stdout.log', 'stderr.log'})
            self.assertTrue(all(f['bytes'] == 0 for f in result['files'].values()))

    def batch(self, folder, arm, outcome='pass', changed=False, runtime_changed=False):
        root = Path(folder)
        args = SimpleNamespace(source=root, runtime=root / 'runtime', arm=arm,
                               output=root / 'output')
        calls = []
        def trial(binary, fixture, directory, profile, identities):
            calls.append(profile)
            directory.mkdir()
            result = {'collection_complete': True, 'functional': {'outcome': outcome}}
            CONTROL.save(directory / 'result.json', result)
            return result
        with patch.object(CONTROL.platform, 'system', return_value='Linux'), \
                patch.object(CONTROL.platform, 'machine', return_value='x86_64'), \
                patch.object(CONTROL, 'verify_audited_runtime',
                             side_effect=[{'audited': True},
                                          ValueError('runtime changed') if runtime_changed
                                          else {'audited': True}]) as verify, \
                patch.object(CONTROL, 'control_source_identity',
                             side_effect=[{'source': 'before'}, {'source': 'after' if changed else 'before'}]), \
                patch.object(CONTROL, 'collect_trial', side_effect=trial), \
                patch('builtins.print'):
            exitcode = CONTROL.run(args)
        self.assertEqual(calls, ['on'] * 3 + ['off'] * 3)
        self.assertEqual(verify.call_count, 2)
        return exitcode, json.loads((root / 'output/upstream.json').read_text())

    def test_stop_rule_and_collected_baseline_failure_are_explicit(self):
        for arm, outcome, expected in (('baseline', 'pass', 0),
                                       ('baseline', 'process_loss', 0),
                                       ('candidate', 'invariant_failure', 1)):
            with tempfile.TemporaryDirectory() as folder:
                exitcode, study = self.batch(folder, arm, outcome)
                self.assertEqual(exitcode, expected)
                self.assertTrue(study['collection_complete'])
                self.assertFalse(study['qualification_pass'])
                self.assertEqual(study['functional_outcome'], 'bounded_negative' if outcome == 'pass'
                                 else 'unsuccessful_observation')

    def test_changed_source_or_runtime_after_batch_invalidates_collection(self):
        for source_changed in (True, False):
            with tempfile.TemporaryDirectory() as folder:
                exitcode, study = self.batch(folder, 'baseline', changed=source_changed,
                                            runtime_changed=not source_changed)
                self.assertEqual(exitcode, 1)
                self.assertFalse(study['collection_complete'])
                self.assertFalse(study['inputs_verified_after'])
                self.assertEqual(study['functional_outcome'], 'unvalidated_inputs')
                self.assertIn('changed', study['identity_error'])


if __name__ == '__main__':
    unittest.main()
