#!/usr/bin/env python3
"""The resource scale study's oracle (#134): each statement's result from the
data's formula, what counts as a wrong (clipped) result, and the summary."""
import unittest

import resource_scale as rs


def stored(n):
    a = [(i, i % rs.GROUPS, i % 100, rs.v_of(i)) for i in range(1, n + 1)]
    b = [(j, j % rs.GROUPS) for j in range(1, n // 20 + 1)]
    return a, b


def brute(op, n):
    """The statement's result, evaluated row by row."""
    a, b = stored(n)
    if op == "scan":
        return [(i, v) for i, _, _, v in a if v % 10 == 3]
    if op == "join":
        return [(i, j) for i, g, h, _ in a if h == 1 for j, bg in b if bg == g]
    return [(i, v) for i, _, _, v in sorted(a, key=lambda r: (r[3], r[0]))]


class Oracle(unittest.TestCase):
    def test_the_formula_is_the_statements_result(self):
        for n in (1, 99, 1000, 2003):
            for op in rs.SQL:
                with self.subTest(op=op, n=n):
                    self.assertIsNone(rs.check(op, brute(op, n), rs.expected(op, n)))

    def test_a_clipped_altered_or_reordered_result_is_wrong(self):
        n = 2000
        for op in rs.SQL:
            rows, want = brute(op, n), rs.expected(op, n)
            altered = list(rows)
            altered[0] = (altered[0][0] + 1, altered[0][1])
            with self.subTest(op=op):
                self.assertTrue(len(rows) > 2)
                self.assertIsNotNone(rs.check(op, rows[:-1], want))
                self.assertIsNotNone(rs.check(op, altered, want))
        rows = brute("sort", n)
        rows[1], rows[2] = rows[2], rows[1]
        self.assertEqual(rs.check("sort", rows, rs.expected("sort", n)), "out of order")

    def test_refusals_are_counted_by_the_budget_they_name(self):
        self.assertEqual(rs.reason("Tarn resource limit: query memory budget (1 + 2 > 3)"), "query memory budget")
        self.assertEqual(rs.reason("Tarn resource limit: query work budget"), "query work budget")

    def test_the_summary_has_a_row_per_cell(self):
        cell = {"op": "sort", "rows": 200000, "sessions": 8, "iterations": 3,
                "outcomes": {"ok": 5, "refused": 19, "wrong": 0, "error": 0},
                "refused_by": {"gateway memory budget": 4, "query memory budget": 15},
                "ok_p50_seconds": 1.23456, "ok_max_seconds": 2.5, "peak_bytes": 3 << 20,
                "gateway_peak_rss_bytes": 150 << 20}
        table = rs.markdown([{"engine": "vinyl", "cells": [cell, dict(cell, outcomes={"ok": 0}, refused_by={},
                                                                         ok_p50_seconds=None)]}])
        lines = table.splitlines()
        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[2], "| vinyl | 200,000 | sort | 8 | 5/24 | 4 gateway memory budget, "
                                   "15 query memory budget | 1.235 | 2.500 | 3 | 150 |")
        self.assertIn("| 0/24 | 0 |  | 2.500 |", lines[3])


if __name__ == "__main__":
    unittest.main()
