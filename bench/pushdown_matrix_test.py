#!/usr/bin/env python3
"""Negative controls for benchmark qualification and retained regressions."""
import unittest
from pushdown_matrix import qualify, summarize


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


if __name__ == '__main__':
    unittest.main()
