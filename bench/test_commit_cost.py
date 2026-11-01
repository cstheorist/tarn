#!/usr/bin/env python3
"""Exercise evidence loss/corruption, ownership and bounded ingestion."""
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import commit_cost as cost

# A 41-byte request, 9-byte CHUNK and 15-byte final frame. Times and socket
# totals are independent inputs, not reconstructed by the auditor under test.
REQUEST = 'request process=12 connection=7 owner=2 sync=90 stream=3 request_type=10 phase="prepare" transaction=' + 'a' * 40 + ' encoded_bytes=41 at_ns=100'
CHUNK = 'frame process=12 connection=7 sync=90 response_type=128 encoded_bytes=9 at_ns=140'
FRAME = 'frame process=12 connection=7 sync=90 response_type=0 encoded_bytes=15 at_ns=190'
TERMINAL = 'terminal process=12 connection=7 sync=90 delivered=false at_ns=200 elapsed_ns=100'
COUNTERS = [dict(process=12, connection=7, bytes_written=41, bytes_read=24)]


def line(value):
    kind, fields = value.split(' ', 1)
    return ('2026-10-30T01:02:03Z TRACE tarn.commit_cost: cost_event="' + kind + '" ' + fields + '\n').encode()


class Evidence(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def inspect(self, events=None, counters=COUNTERS):
        directory = self.root / str(len(list(self.root.iterdir())))
        cost.collect(io.BytesIO(b''.join(line(x) for x in (events or [REQUEST, CHUNK, FRAME, TERMINAL]))), directory)
        return cost.audit(directory, counters)

    def test_complete_chunk_and_cancelled_receiver_reconcile_actual_bytes(self):
        result = self.inspect()
        self.assertTrue(result['qualified'])
        self.assertEqual(result['event_counts'], {'request': 1, 'frame': 2, 'terminal': 1})
        self.assertEqual(result['receiver_cancelled'], 1)
        self.assertEqual(result['prepare_owner_distribution'], [{'owners': 1, 'correlations': 1}])
        self.assertEqual(result['phases'][0]['encoded_bytes'], 41)

    def test_emission_reordering_preserves_recorded_lifetimes(self):
        self.assertTrue(self.inspect([TERMINAL, FRAME, CHUNK, REQUEST])['qualified'])

    def test_same_sync_on_another_connection_is_distinct(self):
        second = [x.replace('connection=7', 'connection=8').replace('owner=2', 'owner=5')
                  for x in [REQUEST, CHUNK, FRAME, TERMINAL]]
        result = self.inspect([REQUEST, CHUNK, FRAME, TERMINAL] + second,
                              COUNTERS + [dict(COUNTERS[0], connection=8)])
        self.assertTrue(result['qualified'])
        self.assertEqual(result['prepare_owner_distribution'], [{'owners': 2, 'correlations': 1}])

    def test_lost_request_final_or_terminal_never_qualifies(self):
        for events, expected in [([CHUNK, FRAME, TERMINAL], 'response_without_request'),
                                 ([REQUEST, CHUNK, TERMINAL], 'terminal_frame_count'),
                                 ([REQUEST, CHUNK, FRAME], 'request_without_terminal')]:
            result = self.inspect(events)
            self.assertFalse(result['qualified'])
            self.assertIn(expected, result['errors'])

    def test_duplicate_sync_or_terminal_never_qualifies(self):
        for extra, expected in [(REQUEST, 'duplicate_request'), (TERMINAL, 'duplicate_terminal')]:
            self.assertIn(expected, self.inspect([REQUEST, CHUNK, FRAME, TERMINAL, extra])['errors'])

    def test_future_response_chunk_after_final_and_clock_corruption(self):
        for chunk, terminal, expected in [(CHUNK.replace('response_type=128', 'response_type=4096'), TERMINAL, 'invalid_response_type'),
                                         (CHUNK.replace('at_ns=140', 'at_ns=195'), TERMINAL, 'chunk_after_final'),
                                         (CHUNK, TERMINAL.replace('elapsed_ns=100', 'elapsed_ns=99'), None),
                                         (CHUNK, TERMINAL.replace('elapsed_ns=100', 'elapsed_ns=98'), 'elapsed_time_disagreement')]:
            result = self.inspect([REQUEST, chunk, FRAME, terminal])
            self.assertIn(expected, result['errors']) if expected else self.assertTrue(result['qualified'])

    def test_partial_socket_write_and_inventory_gaps_are_visible(self):
        self.assertIn('socket_byte_disagreement', self.inspect(counters=[dict(COUNTERS[0], bytes_written=40)])['errors'])
        self.assertIn('socket_inventory_disagreement', self.inspect(counters=[])['errors'])
        self.assertIn('socket_counters_missing', self.inspect(counters=None)['errors'])

    def test_unknown_response_or_malformed_fields_fail_closed(self):
        for request in [REQUEST + ' arbitrary=payload', REQUEST + ' sync=90',
                        REQUEST.replace('owner=2', 'owner=-2'), REQUEST.replace('phase="prepare"', 'phase="private"'),
                        REQUEST.replace('sync=90', 'sync=18446744073709551616')]:
            with self.assertRaises(ValueError):
                self.inspect([request, CHUNK, FRAME, TERMINAL])

    def test_ansi_default_formatter_and_unrelated_lines(self):
        plain = line(REQUEST)
        colored = plain.replace(b'cost_event', b'\x1b[3mcost_event\x1b[0m')
        self.assertEqual(cost.parse(colored), cost.parse(plain))
        self.assertIsNone(cost.parse(b'INFO other.target: unrelated diagnostic\n'))

    def test_byte_line_and_truncation_limits_preserve_only_bounded_prefix(self):
        for source, kwargs, size in [(b'a' * 1000, dict(max_bytes=16), 16),
                                     (b'a' * 1000, dict(max_line=12), 12),
                                     (b'partial', {}, 7)]:
            directory = self.root / str(len(list(self.root.iterdir())))
            receipt = cost.collect(io.BytesIO(source), directory, **kwargs)
            self.assertFalse(receipt['complete'])
            self.assertEqual((directory / 'trace.log').stat().st_size, size)
            self.assertEqual(receipt['bytes'], size)
        with self.assertRaises(ValueError):
            cost.collect(io.BytesIO(b''), self.root / 'excessive', max_bytes=cost.MAX_BYTES + 1)

    def test_capture_reuse_and_changed_raw_bytes_are_refused(self):
        directory = self.root / 'fixed'
        cost.collect(io.BytesIO(line(REQUEST)), directory)
        with self.assertRaises(FileExistsError):
            cost.collect(io.BytesIO(b''), directory)
        raw = (directory / 'trace.log').read_bytes()
        (directory / 'trace.log').write_bytes(raw.replace(b'owner=2', b'owner=3'))
        with self.assertRaisesRegex(ValueError, 'hash changed'):
            cost.audit(directory, COUNTERS)

    def test_retirement_does_not_certify_pending_request(self):
        result = self.inspect([REQUEST, 'retired process=12 connection=7 pending=1 deadline=true at_ns=200'])
        self.assertFalse(result['qualified'])
        self.assertIn('request_without_terminal', result['errors'])

    def test_cli_preserves_a_failed_audit_and_the_original_capture(self):
        directory, output = self.root / 'capture', self.root / 'failure.json'
        cost.collect(io.BytesIO(line(REQUEST + ' unknown=payload')), directory)
        original = (directory / 'trace.log').read_bytes()
        result = subprocess.run([sys.executable, str(Path(cost.__file__)), 'audit',
                                 '--input', str(directory), '--output', str(output)],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertEqual(result.returncode, 2)
        self.assertFalse(json.loads(output.read_text())['qualified'])
        self.assertEqual((directory / 'trace.log').read_bytes(), original)


if __name__ == '__main__':
    unittest.main()
