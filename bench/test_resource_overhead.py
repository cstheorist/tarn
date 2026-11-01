#!/usr/bin/env python3
"""bench/resource_overhead.py's oracle and summary (#134)."""
import unittest

import resource_overhead as ro


class Overhead(unittest.TestCase):
    def test_the_counts_are_the_statement_evaluated_row_by_row(self):
        n = 2003
        want = ro.counts(n)
        for g in (0, 1, 57, ro.GROUPS - 1):
            self.assertEqual(want[g], sum(1 for i in range(1, n + 1) if i % ro.GROUPS == g and ro.v_of(i) % 7 == 3))

    def test_the_summary_has_a_row_per_run(self):
        run = {"config": "budgets", "goodput_per_second": 1234.5, "p50_ms": 1.234, "p99_ms": 9.876, "max_ms": 20.04,
               "gateway_idle_rss_bytes": 30 << 20, "gateway_peak_rss_bytes": 50 << 20,
               "peak_admitted_bytes": 200 << 20, "refusals": 0}
        lines = ro.markdown([{"engine": "memtx", "runs": [run, dict(run, config="unbounded")]}]).splitlines()
        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[2], "| memtx | 1 | budgets | 1,234.5 | 1.23 | 9.88 | 20.0 | 30 | 50 | 200 | 0 |")
        self.assertTrue(lines[3].startswith("| memtx | 1 | unbounded |"))


if __name__ == "__main__":
    unittest.main()
