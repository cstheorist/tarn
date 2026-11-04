"""Offline falsification controls for the fixed effect oracle (no services)."""
import copy
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

SQL = Path(__file__).parent / 'sql'
sys.path.insert(0, str(SQL))
spec = importlib.util.spec_from_file_location('gc_effects', SQL / 'gc_effects.py')
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)
from effect_journal import Journal, read


class EffectOracle(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'journal.jsonl'
        keys = [11, 12, 13, 14, 21, 22]
        owned = {f's{s}.{r}': {'pid': 123 + (s-1)*3 + r-1, 'endpoint': f'127.0.0.1:{7200+s*10+r}'}
                 for s in (1, 2) for r in (1, 2, 3)}
        config = {'keys': keys, 'database': 'gc_effects', 'engine': 'memtx', 'native': 'native',
                  'owned_nodes': owned}
        with_journal = Journal(self.path, source={'fixture': 'offline'}, config=config)
        for op in fixture.operations(keys, 'gc_effects', 'memtx'):
            with_journal.intent(**op)
            name = op['op_id']
            if name in ('unknown', 'refuse-commit'):
                raw = {'error': [1180, '40003'] if name == 'unknown' else [1213, '40001']}
                kind = 'error'
            else:
                result = [[0]] if name == 'refuse-read' else 1
                raw, kind = {'result': result}, 'response'
            with_journal.complete(name, kind=kind, raw=raw)
        with_journal.close()
        self.journal = read(self.path)
        rows = fixture.expected_rows(keys)
        raw = fixture.expected_raw(keys)
        sample = {'sql': rows, 'raw': [[r for r in raw if r[0] in keys[:4]],
                                      [r for r in raw if r[0] in keys[4:]]],
                  'table_runtime': [{'engine': 'memtx', 'is_sync': True, 'state_is_sync': True} for _ in range(2)]}
        runtime = {name: {'pid': node['pid'], 'wal_mode': 'fsync', 'configured_replicas': 3,
                          'observed_replicas': 3, 'quorum': 2, 'sync': True, 'native': {'ready': 1},
                          'system_spaces': {n: {'is_sync': True, 'state_is_sync': True}
                                            for n in ('_tarn_txn', '_tarn_part')}}
                   for name, node in owned.items()}
        self.evidence = {'token': 'unknown', 'storage_runtime': runtime,
                         'proxy_events': [{'token': 'unknown', 'stage': 'reply', 'procedure': 'tarn_autocommit', 'committed': True},
                                          {'token': 'unknown', 'stage': 'injected', 'fault': 'hold'},
                                          {'token': 'unknown', 'stage': 'retired'}],
                         'killed': {'node': 's1.1', 'pid': 123, 'signal': 'SIGKILL'},
                         'survivor': copy.deepcopy(sample), 'restarted': copy.deepcopy(sample)}
        self.evidence['restarted'].update(restarted_raw=copy.deepcopy(sample['raw'][0]), caught_up=True,
                                         catchup_target={'1': 10}, catchup_actual={'1': 11})
        self.evidence['survivor']['storage_runtime'] = {k: copy.deepcopy(v) for k, v in runtime.items() if k != 's1.1'}
        self.evidence['restarted']['storage_runtime'] = copy.deepcopy(runtime)
        self.evidence['restarted']['storage_runtime']['s1.1']['pid'] = 456
        self.evidence['restarted']['restart_pid'] = 456

    def check(self):
        return fixture.reconcile(self.journal, self.evidence)

    def change(self, phase, key, value):
        for row in self.evidence[phase]['sql']:
            if row[0] == key:
                row[1] = value

    def completion(self, op):
        return next(r for r in self.journal['records'] if r.get('type') == 'completion' and r['op_id'] == op)

    def test_accepts_complete_fixed_history(self):
        self.assertEqual(self.check()['target_mutations'], ['ack-a', 'ack-b', 'refuse-a', 'refuse-b', 'conflict', 'unknown'])

    def test_lost_acknowledged_effect(self):
        self.change('survivor', 11, 0)
        with self.assertRaises(AssertionError): self.check()

    def test_refused_transaction_has_no_effect(self):
        self.change('survivor', 12, 1)
        with self.assertRaises(AssertionError): self.check()

    def test_duplicated_unknown_unguarded_increment(self):
        self.change('restarted', 14, 2)
        with self.assertRaises(AssertionError): self.check()

    def test_partial_cross_shard_acknowledged_commit(self):
        self.change('survivor', 21, 0)
        with self.assertRaises(AssertionError): self.check()

    def test_missing_setup_intent(self):
        self.journal['records'] = [r for r in self.journal['records'] if not (r.get('type') == 'intent' and r['op_id'] == 'table')]
        with self.assertRaises(AssertionError): self.check()

    def test_missing_setup_ack(self):
        self.journal['records'].remove(self.completion('seed-11'))
        with self.assertRaises(AssertionError): self.check()

    def test_statement_ack_does_not_replace_commit_ack(self):
        self.journal['records'].remove(self.completion('ack-commit'))
        with self.assertRaises(AssertionError): self.check()

    def test_40003_cannot_be_relabeled_retryable(self):
        self.completion('unknown')['raw']['error'] = [1213, '40001']
        with self.assertRaises(AssertionError): self.check()

    def test_40001_cannot_be_relabeled_unknown(self):
        self.completion('refuse-commit')['raw']['error'] = [1180, '40003']
        with self.assertRaises(AssertionError): self.check()

    def test_sqlstate_not_inferred_from_errno(self):
        self.completion('unknown')['raw']['error'] = [1180, None]
        with self.assertRaises(AssertionError): self.check()

    def test_duplicate_completed_proxy_mutation(self):
        self.evidence['proxy_events'].append(copy.deepcopy(self.evidence['proxy_events'][0]))
        with self.assertRaises(AssertionError): self.check()

    def test_raw_committed_tuples_checked_independently(self):
        self.evidence['survivor']['raw'][0][0][1] = 2
        with self.assertRaises(AssertionError): self.check()

    def test_raw_native_trailing_bucket_must_match(self):
        self.evidence['survivor']['raw'][0][0][2] += 1
        with self.assertRaises(AssertionError): self.check()

    def test_restarted_original_wal_node_must_match(self):
        self.evidence['restarted']['restarted_raw'][0][1] = 0
        with self.assertRaises(AssertionError): self.check()

    def test_catchup_required(self):
        self.evidence['restarted']['caught_up'] = False
        with self.assertRaises(AssertionError): self.check()

    def test_catchup_flag_does_not_replace_vclock(self):
        self.evidence['restarted']['catchup_actual'] = {'1': 9}
        with self.assertRaises(AssertionError): self.check()

    def test_runtime_quorum_cannot_be_inferred_from_config(self):
        self.evidence['storage_runtime']['s2.3']['quorum'] = 1
        with self.assertRaises(AssertionError): self.check()

    def test_native_selection_checked_against_runtime(self):
        self.evidence['storage_runtime']['s2.3']['native'] = False
        with self.assertRaises(AssertionError): self.check()

    def test_restarted_quorum_rechecked(self):
        self.evidence['restarted']['storage_runtime']['s1.1']['quorum'] = 1
        with self.assertRaises(AssertionError): self.check()

    def test_actual_system_space_sync_required(self):
        self.evidence['storage_runtime']['s1.2']['system_spaces']['_tarn_txn']['is_sync'] = False
        with self.assertRaises(AssertionError): self.check()

    def test_kill_pid_must_be_owned_original(self):
        self.evidence['killed']['pid'] = 456
        with self.assertRaises(AssertionError): self.check()

    def test_extra_submission_rejected(self):
        extra = copy.deepcopy(next(r for r in self.journal['records'] if r.get('type') == 'intent'))
        extra['op_id'] = 'unlisted-write'
        self.journal['records'].append(extra)
        with self.assertRaises(AssertionError): self.check()


if __name__ == '__main__':
    unittest.main()
