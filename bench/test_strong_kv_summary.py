#!/usr/bin/env python3
"""Summaries of bench/strong_kv.py studies: medians, pairing and CI notices."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from urllib.parse import unquote

import strong_kv
import strong_kv_summary as summary

CPU = ("storage_all_processes", "storage_all_tx_threads", "hot_owner_leader_tx", "gateway_all_processes",
       "client_process")


def phase(cell, keys_per_second):
    """A phase shaped like bench/strong_kv.py's records, 5 s long."""
    keys = int(keys_per_second * 5)
    timing = dict(successful_keys=keys, elapsed_seconds=5.0, p50_ns=500_000 * cell["batch"],
                  p99_ns=1_000_000 * cell["batch"] * (2 if cell["mode"] == "strong" else 1))
    cpu = dict(available=True, cpu_counters_ns=dict(zip(CPU, (keys * 30_000, keys * 20_000, keys * 10_000,
                                                              keys * 5_000 if cell["mode"] == "sql" else 0,
                                                              keys * 4_000))))
    record = dict(cell, accepted=True, cpu=cpu, errors=[])
    if cell["mode"] == "sql":
        record.update(timing, protocol=dict(complete=True, statements=keys, bytes_written=keys * 70, bytes_read=keys * 130),
                      gateway_storage=dict(requests=keys * 2, bytes_written=keys * 150, bytes_read=keys * 250))
    else:
        record["client"] = dict(timing, event="RESULT", accepted=True,
                                transport=dict(complete=True, requests=keys * (2 if cell["mode"] == "strong" else 1),
                                               replies=keys, bytes_written=keys * 60, bytes_read=keys * 140))
    return record


def study(rate=lambda cell: 1000 * (cell["trial"] + 1) * (2 if cell["mode"] == "strong" else 1)):
    plan = strong_kv.plan(181)
    return dict(version=1, accepted=True, seconds=5, warmup=1, plan=plan,
                source=dict(engine="memtx", cohort_head="a" * 40),
                phases=[phase(cell, rate(cell)) for cell in plan])


class Summary(unittest.TestCase):
    def test_medians_ranges_and_per_key_costs(self):
        s = summary.summarize(study(), dict(environment=dict(cpu_count=4)))
        self.assertEqual((s["engine"], s["cpu_count"]), ("memtx", 4))
        self.assertEqual([(c["batch"], c["workers"]) for c in s["cells"]], list(summary.CELLS))
        cell = s["cells"][2]
        sql, strong, raw = cell["paths"]
        self.assertEqual((sql["mode"], strong["mode"], raw["mode"], sql["trials"]), ("sql", "strong", "raw", 6))
        # Six trials at 1000..6000 keys/s.
        self.assertEqual(sql["keys_per_second"], dict(median=3500, minimum=1000, maximum=6000))
        self.assertEqual(sql["p99_ms"]["median"], 32)
        self.assertEqual(sql["client_requests_per_key"]["median"], 1)
        self.assertEqual(sql["storage_requests_per_key"]["median"], 2)
        self.assertEqual(sql["storage_bytes_per_key"]["median"], 400)
        self.assertEqual(sql["client_bytes_per_key"]["median"], 200)
        self.assertEqual(sql["server_cpu_us_per_key"]["median"], 35)
        self.assertEqual(sql["hot_tx_cpu_us_per_key"]["median"], 10)
        self.assertEqual(strong["storage_requests_per_key"]["median"], 2)
        self.assertEqual(raw["storage_bytes_per_key"]["median"], 200)
        self.assertEqual(strong["server_cpu_us_per_key"]["median"], 30)

    def test_strong_kv_is_paired_with_sql_in_the_same_trial(self):
        ratios = summary.summarize(study())["cells"][0]["strong_over_sql"]
        self.assertEqual(ratios["keys_per_second"], dict(median=2, minimum=2, maximum=2))
        self.assertEqual(ratios["p99_ms"]["median"], 2)
        self.assertEqual(ratios["hot_tx_cpu_us_per_key"]["median"], 1)

    def test_older_sql_phases_without_gateway_counters_show_no_storage_cost(self):
        s = study()
        for p in s["phases"]:
            p.pop("gateway_storage", None)
        out = summary.summarize(s)
        self.assertIsNone(out["cells"][0]["paths"][0]["storage_requests_per_key"])
        self.assertIn("| 1, 1 | SQL | 3,500 (1,000–6,000) | 1 | — | — | 200 | 35 | 10 | 4 |", summary.markdown(out))

    def test_refuses_rejected_or_incomplete_studies(self):
        s = study()
        s["accepted"] = False
        self.assertRaises(ValueError, summary.summarize, s)
        s = study()
        s["phases"] = [p for p in s["phases"] if p["mode"] != "raw" or p["batch"] != 32]
        self.assertRaises(ValueError, summary.summarize, s)

    def test_notices_carry_each_cell_whole(self):
        s = summary.summarize(study())
        lines = list(summary.notices(s, "strong-kv memtx"))
        self.assertEqual(len(lines), 4)
        for line, cell in zip(lines, s["cells"]):
            self.assertTrue(line.startswith("::notice title=strong-kv memtx B%d C%d::" % (cell["batch"], cell["workers"])))
            body = json.loads(unquote(line.split("::", 2)[2]))
            self.assertEqual(body["cells"], [cell])
            self.assertEqual(body["engine"], "memtx")

    def test_notices_fit_the_runners_annotation_limit_with_wide_numbers(self):
        s = study(rate=lambda cell: 123_456.789 / (cell["trial"] + 7) / 3 ** (cell["mode"] == "raw"))
        fixture = dict(environment=dict(cpu_count=4, platform="Linux-6.11.0-1018-azure-x86_64-with-glibc2.39"))
        for line in summary.notices(summary.summarize(s, fixture), "strong-kv vinyl"):
            self.assertLess(len(line.split("::", 2)[2]), summary.NOTICE_LIMIT - 1000)

    def test_cli_writes_json_markdown_and_renders_a_saved_summary(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "results").mkdir()
            (root / "results" / "study.json").write_text(json.dumps(study()))
            (root / "fixture.json").write_text(json.dumps(dict(environment=dict(cpu_count=4))))
            tool = str(Path(summary.__file__))
            out = subprocess.run([sys.executable, tool, str(root / "results" / "study.json"), "--json",
                                  str(root / "summary.json"), "--markdown", str(root / "step.md"), "--notice", "t"],
                                 check=True, capture_output=True, text=True).stdout
            self.assertIn("#### Matched reads (memtx, 4 CPUs, source aaaaaaaaaaaa)", out)
            self.assertEqual(out.count("::notice title=t B"), 4)
            self.assertEqual((root / "step.md").read_text(), out[:out.index("::notice")])
            again = subprocess.run([sys.executable, tool, str(root / "summary.json")],
                                   check=True, capture_output=True, text=True).stdout
            self.assertEqual(again, (root / "step.md").read_text())


if __name__ == "__main__":
    unittest.main()
