#!/usr/bin/env python3
"""Offline controls for direct-worker identity retention and real-layout admission."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
import subprocess
import sys

from rc_direct import fold_events, metadata, read_ready


class Journal(unittest.TestCase):
    def fold(self, events, suffix=b''):
        with tempfile.TemporaryDirectory() as work:
            path = Path(work) / 'events.jsonl'
            path.write_bytes(b''.join(json.dumps(e).encode() + b'\n' for e in events) + suffix)
            return fold_events(path, 1, 'hot')

    def start(self, worker=0, sequence=1):
        return dict(event='start', token=1000000 + worker * 100000 + sequence,
                    worker=worker, key=0, number=1, txid=f'run/1/{worker}/{sequence}', began=1)

    def success(self, start=None):
        start = start or self.start()
        token = start['token']
        return [start, dict(event='commit', token=token), dict(event='finish', token=token,
                outcome='ok', stage='commit', reply=['ok', 2], calls=3, latency_seconds=.01)]

    def test_complete_success(self):
        rows, errors = self.fold(self.success())
        self.assertEqual(errors, [])
        self.assertEqual((rows[0]['outcome'], rows[0]['sqlstate'], rows[0]['calls']), ('ok', '00000', 3))

    def test_lost_commit_reply_retains_identity_as_unknown(self):
        rows, errors = self.fold(self.success()[:2])
        self.assertEqual(errors, [])
        self.assertEqual((rows[0]['token'], rows[0]['txid'], rows[0]['outcome'], rows[0]['sqlstate']),
                         (1000001, 'run/1/0/1', 'unknown', '40003'))
        self.assertTrue(rows[0]['commit_dispatched'])
        self.assertFalse(rows[0]['terminal'])

    def test_interrupt_does_not_drop_another_worker(self):
        rows, errors = self.fold(self.success() + [self.start(worker=1)])
        self.assertEqual(errors, [])
        self.assertEqual([r['outcome'] for r in rows], ['ok', 'unknown'])

    def test_refusal_has_different_sqlstate_from_transport_uncertainty(self):
        events = self.success()[:2]
        events.append(dict(event='finish', token=1000001, outcome='refused', stage='commit',
                           reply=['conflict', 1213, 'conflict'], calls=4, latency_seconds=.01))
        rows, errors = self.fold(events)
        self.assertEqual(errors, [])
        self.assertEqual(rows[0]['sqlstate'], '40001')

    def test_lock_refusal_records_cleanup_call(self):
        events = [self.start(), dict(event='finish', token=1000001, outcome='refused', stage='lock_t',
                  reply=['timeout', 1205, 'timeout', 'g/1'], calls=2, latency_seconds=1)]
        rows, errors = self.fold(events)
        self.assertEqual(errors, [])
        self.assertFalse(rows[0]['commit_dispatched'])
        self.assertEqual(rows[0]['sqlstate'], 'HY000')

    def test_malformed_success_never_acknowledges(self):
        for reply in (['ok'], ['ok', 1], ['ok', 2, 'extra'], ['ok', True], {'1': 'ok', '2': 2}):
            with self.subTest(reply=reply):
                events = self.success(); events[-1]['reply'] = reply
                rows, errors = self.fold(events)
                self.assertTrue(errors)
                self.assertEqual(rows[0]['outcome'], 'unknown')

    def test_duplicate_dispatch_or_terminal_fails_audit(self):
        events = self.success()
        for broken in (events[:2] + [events[1]] + events[2:], events + [events[-1]], [events[0]] + events):
            with self.subTest(broken=broken):
                self.assertTrue(self.fold(broken)[1])

    def test_partial_tail_keeps_prior_inflight_identity(self):
        rows, errors = self.fold(self.success()[:2], b'{"event":"finish","token":1000001')
        self.assertTrue(errors)
        self.assertEqual(rows[0]['outcome'], 'unknown')

    def test_unknown_error_is_not_a_refusal(self):
        events = self.success(); events[-1].update(outcome='refused', reply=['error', 1180, 'unknown'], calls=4)
        rows, errors = self.fold(events)
        self.assertTrue(errors)
        self.assertEqual(rows[0]['outcome'], 'unknown')


class Metadata(unittest.TestCase):
    def fixture(self):
        def table(name, sid, fields, key):
            return dict(sid=sid, engine='memtx', is_sync=True, fence_version=7,
                        format=[dict(name=f) for f in fields],
                        parts=[dict(fieldno=fields.index(f) + 1) for f in key],
                        catalog=dict(db='rc186', name=name, space_id=sid, ver=7, shard_key=[0],
                                     columns=[dict(name='tenant')]))
        t = table('t', 600, ['payload', 'v', 'tenant', 'bucket_id', 'id'], ('tenant', 'id'))
        ledger = table('ledger', 601, ['k', 'tenant', 'bucket_id', 'token'], ('tenant', 'token'))
        return [dict(address='owned:1', result=dict(t=t, ledger=ledger, ledger_count=0, shard_id=1,
                    rows=[['x' * 256, 0, 1, 42, key] for key in range(8)]))]

    def test_uses_native_positions_ids_versions_and_bucket(self):
        d = metadata(self.fixture(), 'memtx')
        self.assertEqual(d['t']['fields']['v'], 2)
        self.assertEqual(d['ledger']['fields']['token'], 4)
        self.assertEqual(d['fence'], [600, 7, 601, 7])
        self.assertEqual(d['bucket'], 42)

    def test_ambiguous_owner_is_refused(self):
        fixture = self.fixture()
        with self.assertRaises(AssertionError):
            metadata(fixture + copy.deepcopy(fixture), 'memtx')

    def test_mismatched_fence_unknown_layout_and_wrong_key_are_refused(self):
        for mutate in (lambda r: r['t'].update(fence_version=6),
                       lambda r: r['t']['format'].append(dict(name='extra')),
                       lambda r: r['t']['parts'].reverse(),
                       lambda r: r.update(ledger_count=1),
                       lambda r: r['t']['catalog'].update(shard_key=[1])):
            fixture = self.fixture(); mutate(fixture[0]['result'])
            with self.assertRaises(AssertionError):
                metadata(fixture, 'memtx')


class Readiness(unittest.TestCase):
    def test_partial_line_has_a_walltime_bound(self):
        process = subprocess.Popen([sys.executable, '-c',
            'import sys,time; sys.stdout.write("partial"); sys.stdout.flush(); time.sleep(10)'],
            stdout=subprocess.PIPE)
        try:
            with self.assertRaisesRegex(AssertionError, 'readiness deadline'):
                read_ready(process, .1)
        finally:
            process.kill(); process.communicate(timeout=2)


if __name__ == '__main__':
    unittest.main()
