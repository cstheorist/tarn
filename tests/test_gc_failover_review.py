#!/usr/bin/env python3
"""Regression controls for crash evidence that previously disappeared."""
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("review", Path(__file__).resolve().parents[1] / "scripts/gc_failover_review.py")
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)


class EvidenceTests(unittest.TestCase):
    def test_original_crash_is_not_a_successful_repeat(self):
        original = review.classify({"run.log": "FAILOVER TEST FAILED\n",
                                    "cluster/s1.2.log": "Segmentation fault\naddr: 0x2f\ngc_traverse_trace.part.0+330\n"}, 1)
        repeat = review.classify({"run.log": "FAILOVER TEST PASSED\n"}, 0)
        self.assertTrue(original["trace_gc_crash"])
        self.assertEqual(original["crash_logs"], ["cluster/s1.2.log"])
        self.assertFalse(original["fixture_passed"])
        self.assertTrue(repeat["fixture_passed"])
        self.assertFalse(all(t["fixture_passed"] for t in [original, repeat]))

    def test_success_banner_cannot_hide_crash_or_timeout(self):
        logs = {"run.log": "FAILOVER TEST PASSED", "cluster/s1.2.log": "SEGV_MAPERR"}
        self.assertFalse(review.classify(logs, 0)["fixture_passed"])
        self.assertFalse(review.classify({"run.log": "FAILOVER TEST PASSED"}, 0, True)["fixture_passed"])
        self.assertFalse(review.classify({"run.log": "setup error"}, 0)["fixture_passed"])

    def test_accounting_and_native_reports_survive_classification(self):
        text = ('bank: 12 commits, 1 commits with unknown outcome, final sum 64000\n'
                'counters: 9 acknowledged increments, 1 with unknown outcome\n'
                'direct KV sum on new leaders: 64000\n'
                'shard 1 leader, native procedures:\t{"ready":1,"writes":4}\n'
                'shard 2 leader, native procedures:\tfalse\nFAILOVER TEST PASSED\n')
        result = review.classify({"run.log": text}, 0)
        self.assertEqual(len(result["accounting_output"]), 4)
        self.assertEqual(result["native_reports"], [{"ready": 1, "writes": 4}, False])

    def test_lua_control_requires_explicit_disabled_reports(self):
        # Run 40873456226 retained two false reports in every Lua trial.
        self.assertTrue(review.native_mode_confirmed([False, False], "lua"))
        for reports in ([], [False], [None, None], [0, 0], [False, {"ready": 1}], ["invalid-report", False]):
            self.assertFalse(review.native_mode_confirmed(reports, "lua"))
        self.assertFalse(review.native_mode_confirmed([False, False], "native"))
        self.assertTrue(review.native_mode_confirmed([{"ready": 1}, {"ready": 1}], "native"))

    def test_real_subprocess_trials_retain_success_failure_and_archived_state(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source, binaries, out = root / "source", root / "bin", root / "evidence"
            for name in ("crates", "lua", "scripts", "tests"):
                (source / name).mkdir(parents=True)
            for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml"):
                (source / name).write_text("fixture\n")
            (source / "tests/failover.sh").write_text('''set -eu
mkdir -p "$DIR"
printf 'unchanged storage witness' > "$DIR/wal-witness"
echo 'shard 1 leader, native procedures: {"ready":1}'
echo 'shard 2 leader, native procedures: {"ready":1}'
case "$DIR" in
 */trial-2/*) echo 'Segmentation fault gc_traverse_trace' > "$DIR/s1.2.log"; exit 1;;
esac
echo 'FAILOVER TEST PASSED'
''')
            binaries.mkdir()
            for name in ("tarn-server", "libtarn_storage.so", "tarantool"):
                (binaries / name).write_text("fixture\n")
            build = {"source_commit": "f" * 40, "sources": review.sources(source),
                     "binaries": {n: review.sha(binaries / n) for n in ("tarn-server", "libtarn_storage.so")}}
            (binaries / "build.json").write_text(json.dumps(build))
            args = SimpleNamespace(source=source, binaries=binaries, output=out, mode="native", engine="vinyl",
                                   jit="on", tarantool=str(binaries / "tarantool"), repetitions=3, seconds=45, timeout=10)
            def command(argv, cwd=None):
                return "f" * 40 if argv[0] == "git" else '{"fixture":true}'
            with patch.object(review, "command", command):
                self.assertEqual(review.run(args), 1)
            result = json.loads((out / "manifest.json").read_text())
            self.assertEqual([t["fixture_passed"] for t in result["trials"]], [True, False, True])
            self.assertTrue((out / "trial-1/run.log").is_file())
            self.assertTrue((out / "trial-3/run.log").is_file())
            with review.tarfile.open(out / "trial-2/failed-runtime.tar.gz") as archive:
                self.assertEqual(archive.extractfile("cluster/wal-witness").read(), b"unchanged storage witness")
            self.assertEqual(review.sha(out / "trial-2/failed-runtime.tar.gz"), result["trials"][1]["failed_runtime_sha256"])


if __name__ == "__main__":
    unittest.main()
