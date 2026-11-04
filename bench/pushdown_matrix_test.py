#!/usr/bin/env python3
"""Negative controls for benchmark qualification and retained regressions."""
import json
import copy
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from pushdown_matrix import qualify, runtime, summarize
from stop_pushdown_fixture import stop


class EvidenceControls(unittest.TestCase):
    def setUp(self):
        self.zero = {'Tarn_storage_reductions': 0, 'Tarn_storage_projections': 0}

    def test_inactive_enabled_path_is_rejected(self):
        with self.assertRaises(AssertionError):
            qualify(self.zero, self.zero, True, 'reduce')

    def test_control_cannot_use_optimized_path(self):
        with self.assertRaises(AssertionError):
            qualify(self.zero, dict(self.zero, Tarn_storage_projections=1), False, 'project')

    def test_fallback_must_not_be_reported_as_optimization(self):
        qualify(self.zero, self.zero, True, 'fallback')
        with self.assertRaises(AssertionError):
            qualify(self.zero, dict(self.zero, Tarn_storage_reductions=1), True, 'fallback')

    def test_one_regressing_pair_is_retained(self):
        samples = [dict(phase='direct', query='reduce_low', pair=pair, pushdown=on,
                        latency_seconds=[1.1 if on and pair == 4 else .5 if on else 1])
                   for pair in range(5) for on in (False, True)]
        result = summarize(samples)[0]
        self.assertEqual(result['paired_elapsed_ratios'], [.5, .5, .5, .5, 1.1])
        self.assertEqual(result['elapsed_ratio_range'], [.5, 1.1])
        self.assertFalse(result['all_observed_pairs_faster'])

    def test_performance_runtime_rejects_checks_on_every_replica(self):
        config, rows = self.runtime_fixture()
        with patch('pushdown_matrix.subprocess.check_output', return_value=json.dumps(rows)):
            runtime(config, 'memtx', 'enabled')
        for replica in (0, 1, 5):
            for flag in ('check_locks', 'check_yields', 'test_hooks'):
                for invalid in (True, 0, '0', None):
                    with self.subTest(replica=replica, flag=flag, invalid=invalid):
                        rows[replica]['test_flags'][flag] = invalid
                        with patch('pushdown_matrix.subprocess.check_output', return_value=json.dumps(rows)):
                            with self.assertRaises(AssertionError):
                                runtime(config, 'memtx', 'enabled')
                        rows[replica]['test_flags'][flag] = False

    @staticmethod
    def runtime_fixture():
        config = {'shards': [{'replicas': [f'127.0.0.1:{3300+i*10+j}' for j in range(3)]}
                             for i in range(2)]}
        rows = []
        for i, shard in enumerate(config['shards']):
            peers = [dict(id=j+1, uuid=f'uuid-{i}-{j}', upstream='follow') for j in range(3)]
            for j, address in enumerate(shard['replicas']):
                rows.append(dict(address=address, info={'ro': j != 0, 'native': {'ready': 1}},
                                 pid=100+i*3+j, instance_id=j+1, uuid=peers[j]['uuid'],
                                 replicas=copy.deepcopy(peers), quorum=2, replication_count=3,
                                 sync=True, is_sync=True, effective_sync=True,
                                 engine='memtx', wal_mode='fsync', rows=1,
                                 test_flags=dict(check_locks=False, check_yields=False, test_hooks=False)))
        return config, rows

    def test_runtime_rejects_unqualified_followers_and_membership(self):
        config, baseline = self.runtime_fixture()
        for replica in range(6):
            for field, invalid in [('engine', 'vinyl'), ('wal_mode', 'write'), ('quorum', 1),
                                   ('replication_count', 2), ('sync', False), ('is_sync', False),
                                   ('effective_sync', False)]:
                with self.subTest(replica=replica, field=field):
                    rows = copy.deepcopy(baseline)
                    rows[replica][field] = invalid
                    with patch('pushdown_matrix.subprocess.check_output', return_value=json.dumps(rows)):
                        with self.assertRaises(AssertionError):
                            runtime(config, 'memtx', 'enabled')

            for change in ('missing', 'foreign', 'not_following'):
                rows = copy.deepcopy(baseline)
                peer = rows[replica]['instance_id'] % 3
                if change == 'missing':
                    rows[replica]['replicas'].pop()
                elif change == 'foreign':
                    rows[replica]['replicas'][peer]['uuid'] = 'foreign'
                else:
                    rows[replica]['replicas'][peer]['upstream'] = 'disconnected'
                with self.subTest(replica=replica, change=change):
                    with patch('pushdown_matrix.subprocess.check_output', return_value=json.dumps(rows)):
                        with self.assertRaises(AssertionError):
                            runtime(config, 'memtx', 'enabled')

    def test_runtime_rejects_another_process_at_the_expected_endpoint(self):
        config, rows = self.runtime_fixture()
        pids = {r['address']: r['pid'] for r in rows}
        rows[1]['pid'] += 1000
        with patch('pushdown_matrix.subprocess.check_output', return_value=json.dumps(rows)):
            with self.assertRaises(AssertionError):
                runtime(config, 'memtx', 'enabled', pids)


class FixtureStopHandleControls(unittest.TestCase):
    def test_escalates_through_the_same_handle_and_waits_before_unlink(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory).resolve()
            pidfile = fixture / 's1.1.pid'
            pidfile.write_text('999999')
            def send(fd, sig):
                self.assertTrue(pidfile.exists())
                self.assertEqual(fd, 42)
            with patch('stop_pushdown_fixture.os.pidfd_open', create=True, return_value=42), \
                    patch('stop_pushdown_fixture.fixture_process'), \
                    patch('stop_pushdown_fixture.os.close') as close, \
                    patch('stop_pushdown_fixture.signal.pidfd_send_signal', create=True, side_effect=send) as sent, \
                    patch('stop_pushdown_fixture.select.poll') as poll:
                # First poll checks liveness; TERM expires immediately, KILL
                # is followed by the pinned process's exit notification.
                poll.return_value.poll.side_effect = [[], [(42, 1)]]
                stop(fixture, timeout=0)
                self.assertEqual([call.args[1] for call in sent.call_args_list],
                                 [signal.SIGTERM, signal.SIGKILL])
                close.assert_called_once_with(42)
                self.assertFalse(pidfile.exists())


@unittest.skipUnless(hasattr(os, 'pidfd_open') and hasattr(signal, 'pidfd_send_signal'),
                     'fixture shutdown requires Linux pidfds')
class FixtureStopControls(unittest.TestCase):
    def test_stops_owned_process_and_waits_before_removing_pid(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory).resolve()
            script = fixture / 's1.1.sh'
            # Keep a shell as the fixture's original launch identity and
            # ignore TERM so the test exercises precise pidfd escalation.
            script.write_text('trap "" TERM\nwhile :; do :; done\n')
            process = subprocess.Popen(['bash', str(script)])
            pidfile = fixture / 's1.1.pid'
            pidfile.write_text(str(process.pid))
            try:
                stop(fixture, timeout=.05)
                process.wait(timeout=2)
                self.assertFalse(pidfile.exists())
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()

    def test_refuses_unrelated_pid_without_signaling_it(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory).resolve()
            process = subprocess.Popen(['sleep', '30'])
            pidfile = fixture / 's1.1.pid'
            pidfile.write_text(str(process.pid))
            try:
                with self.assertRaises(ValueError):
                    stop(fixture, timeout=.05)
                self.assertIsNone(process.poll())
                self.assertTrue(pidfile.exists())
            finally:
                process.terminate()
                process.wait(timeout=2)


if __name__ == '__main__':
    unittest.main()
