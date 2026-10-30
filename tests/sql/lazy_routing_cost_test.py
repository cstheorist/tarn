#!/usr/bin/env python3
"""Failure controls for preserving a routing study's completed measurements."""
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import lazy_routing_cost as study


class RetentionTest(unittest.TestCase):
    def check_cleanup(self, fail):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)/'study.json'
            latencies = [[0.01, 0.002, 'read'], [0.02, 0.003, 'write']]
            sample = dict(shape='skewed', lanes=4, pair=0, route_parts=2,
                          complete=True, workers=[dict(worker_id=0, latencies=latencies,
                                                      errors=[], skipped=0)])
            visited = []
            def restore(node, script):
                visited.append(node)
                if fail and node == 'first':
                    raise TimeoutError('restore timed out')
            def save():
                output.write_text(json.dumps(sample))
            if fail:
                sample['failure'] = 'original post-workload drain failure'
            with patch.object(study, 'ev', restore):
                if fail:
                    with self.assertRaisesRegex(RuntimeError, 'cleanup failed'):
                        study.finish_sample(sample, output, ['first', 'second'], save)
                else:
                    study.finish_sample(sample, output, ['first', 'second'], save)
            self.assertEqual(visited, ['first', 'second'])
            saved = json.loads(output.read_text())
            raw = output.parent/saved['raw_latencies']['path']
            self.assertEqual(hashlib.sha256(raw.read_bytes()).hexdigest(), saved['raw_latencies']['sha256'])
            with gzip.open(raw, 'rt') as stream:
                self.assertEqual(json.load(stream), [dict(worker_id=0, latencies=latencies)])
            self.assertEqual(saved['workers'][0]['completed'], 2)
            self.assertEqual(saved['complete'], not fail)
            if fail:
                self.assertEqual(saved['failure'], 'original post-workload drain failure')
                self.assertEqual(saved['cleanup_failures'][0]['node'], 'first')
            else:
                self.assertNotIn('failure', saved)

    def test_restore_failure_keeps_raw_samples_and_attempts_every_node(self):
        self.check_cleanup(True)

    def test_success_keeps_raw_samples_and_complete_status(self):
        self.check_cleanup(False)


if __name__ == '__main__':
    unittest.main()
