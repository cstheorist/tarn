#!/usr/bin/env python3
"""Negative controls for diagnostic evidence, not live Lua runtime tests."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("gc_upvalue_study", ROOT / "scripts/gc_upvalue_study.py")
STUDY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(STUDY)


def passing(profile="ordinary", n=7):
    blocked = profile.startswith("retained-blocked-rebuild")
    construction = "construction-warm" in profile or blocked
    retained = profile.startswith("retained-")
    incremental = "incremental-gc" in profile
    constructors = 64 if blocked else min(64, n) if retained else n if construction else 1
    rebuilds = 64 if blocked else n
    return {"schema_version": 1, "profile": profile, "source_identities": {"test": "fixture"},
            "complete": True, "passed": True,
            "runtime": {"os": "Linux", "arch": "x64", "gc64": False,
                        "effective_jit": {"enabled": not profile.endswith("jit-off")}},
            "config": {"iterations": n, "max_iterations": 1000000, "batch_size": 64 if construction else 1,
                       "constructor_warming": construction, "retained_engines": retained,
                       "native": False, "sync": False,
                       "transactions": False, "wal_mode": "none",
                       "cut_sequence": "false during loop, true after loop" if blocked else "false then true",
                       "rebuild_stage": "blocked-only iterations" if blocked else "per-iteration rebuild",
                       "final_rebuild_count": 64 if blocked else 0,
                       "gc": "explicit incremental step(1) each iteration" if incremental else "default"},
            "counts": {"iterations": n, "constructors": constructors, "marks": constructors + n,
                       "cuts": n + 2*rebuilds, "invalidations": n, "rebuilds": rebuilds, "blocked_rebuilds": n,
                       "final_rebuilds": 64 if blocked else 0,
                       "gc_steps": n if incremental else 0}}


class ReceiptControls(unittest.TestCase):
    def validate(self, record, profile="ordinary"):
        return STUDY.validate_receipt(record, profile, 7, {"test": "fixture"})

    def test_profile_selection_keeps_legacy_defaults_and_accepts_only_known_profiles(self):
        argv = ["study", "--source", ".", "--runtime", ".", "--arm", "baseline", "--output", "out"]
        for selected in ((), STUDY.PROFILES[-3:]):
            with patch.object(sys, "argv", argv + (["--profiles", *selected] if selected else [])), \
                 patch.object(STUDY, "run", return_value=0) as run:
                self.assertEqual(STUDY.main(), 0)
                self.assertEqual(tuple(run.call_args.args[0].profiles), selected or STUDY.LEGACY_PROFILES)
        for invalid in (("unknown",), ("ordinary", "ordinary")):
            with patch.object(sys, "argv", argv + ["--profiles", *invalid]), \
                 patch("sys.stderr"), self.assertRaises(SystemExit) as error:
                STUDY.main()
            self.assertEqual(error.exception.code, 2)

    def test_profiles_and_exact_counts(self):
        for profile in STUDY.PROFILES:
            with self.subTest(profile=profile):
                self.assertEqual(self.validate(passing(profile), profile), "pass")

    def test_legacy_receipts_may_omit_new_fields(self):
        for profile in STUDY.LEGACY_PROFILES:
            record = passing(profile)
            del record["config"]["rebuild_stage"], record["config"]["final_rebuild_count"]
            del record["counts"]["final_rebuilds"]
            with self.subTest(profile=profile):
                self.assertEqual(self.validate(record, profile), "pass")

    def test_legacy_present_new_fields_remain_strict(self):
        for profile in STUDY.LEGACY_PROFILES:
            for section, key, value in (("config", "rebuild_stage", "blocked-only iterations"),
                                        ("config", "final_rebuild_count", 64),
                                        ("config", "final_rebuild_count", False),
                                        ("counts", "final_rebuilds", 64),
                                        ("counts", "final_rebuilds", False)):
                record = passing(profile)
                record[section][key] = value
                with self.subTest(profile=profile, key=key, value=value), self.assertRaises(ValueError):
                    self.validate(record, profile)

    def test_incomplete_cannot_claim_pass(self):
        record = passing()
        record["complete"] = False
        with self.assertRaisesRegex(ValueError, "incomplete"):
            self.validate(record)

    def test_retained_profile_must_reuse_the_same_bounded_instances(self):
        profile = "retained-construction-warm-incremental-gc"
        record = passing(profile, 137)
        self.assertEqual(STUDY.validate_receipt(record, profile, 137, {"test": "fixture"}), "pass")
        record["counts"].update(constructors=137, marks=274)
        with self.assertRaisesRegex(ValueError, "counts"):
            STUDY.validate_receipt(record, profile, 137, {"test": "fixture"})

    def test_blocked_stage_and_final_rebuilds_are_required(self):
        for profile in STUDY.PROFILES[len(STUDY.LEGACY_PROFILES):]:
            record = passing(profile, 137)
            self.assertEqual(STUDY.validate_receipt(record, profile, 137, {"test": "fixture"}), "pass")
            for key in record["counts"]:
                bad = copy.deepcopy(record)
                bad["counts"][key] -= 1
                with self.subTest(profile=profile, key=key), self.assertRaisesRegex(ValueError, "counts"):
                    STUDY.validate_receipt(bad, profile, 137, {"test": "fixture"})
            bad = copy.deepcopy(record)
            del bad["counts"]["final_rebuilds"]
            with self.assertRaisesRegex(ValueError, "counts"):
                STUDY.validate_receipt(bad, profile, 137, {"test": "fixture"})
            for key, value in (("rebuild_stage", "per-iteration rebuild"), ("final_rebuild_count", 0),
                               ("cut_sequence", "false then true")):
                bad = copy.deepcopy(record)
                bad["config"][key] = value
                with self.subTest(profile=profile, key=key), self.assertRaisesRegex(ValueError, "configuration"):
                    STUDY.validate_receipt(bad, profile, 137, {"test": "fixture"})

    def test_omitted_iterations_and_cuts_fail(self):
        for key in ("iterations", "constructors", "marks", "cuts", "invalidations", "rebuilds", "blocked_rebuilds"):
            record = passing()
            record["counts"][key] -= 1
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "counts"):
                self.validate(record)

    def test_boolean_is_not_constructor_count(self):
        record = passing()
        record["counts"]["constructors"] = True
        with self.assertRaisesRegex(ValueError, "counts"):
            self.validate(record)

    def test_wrong_sources_or_profile_fail(self):
        for key, value in (("profile", "jit-off"), ("source_identities", {"test": "different"})):
            record = passing()
            record[key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "identity"):
                self.validate(record)

    def test_wrong_architecture_or_jit_fail(self):
        for key, value in (("os", "OSX"), ("arch", "arm64"), ("gc64", True), ("effective_jit", {"enabled": False})):
            record = passing()
            record["runtime"][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.validate(record)

    def test_hidden_gc_perturbation_fails(self):
        record = passing()
        record["config"]["gc"] = "forced collection"
        with self.assertRaisesRegex(ValueError, "configuration"):
            self.validate(record)

    def test_invariant_failure_is_retained(self):
        record = passing()
        record.update(complete=False, passed=False, failure_kind="invariant", error="mark mismatch")
        self.assertEqual(self.validate(record), "invariant_failure")

    def test_initialization_failure_is_not_runtime_evidence(self):
        record = passing()
        record.update(complete=False, passed=False, failure_kind="initialization", error="bad fixture")
        with self.assertRaisesRegex(ValueError, "initialization"):
            self.validate(record)

    def test_wrong_binary_arm_and_patch_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "bin").mkdir()
            (root / "bin/tarantool").write_bytes(b"controlled fake binary")
            patch = root / "patch"
            patch.write_bytes(b"controlled patch")
            record = {"schema_version": 1, "arm": "candidate", "tarantool_commit": STUDY.TARANTOOL,
                      "luajit_commit": STUDY.LUAJIT, "build_type": "RelWithDebInfo",
                      "patch_sha256": STUDY.sha(patch), "binary_sha256": STUDY.sha(root / "bin/tarantool")}
            def write(value):
                (root / "runtime-receipt.json").write_text(json.dumps(value))
            write(record)
            self.assertEqual(STUDY.verify_runtime(root, "candidate", patch), record)
            for key in ("binary_sha256", "patch_sha256", "arm", "luajit_commit", "tarantool_commit"):
                bad = copy.deepcopy(record)
                bad[key] = "different"
                write(bad)
                with self.subTest(key=key), self.assertRaisesRegex(ValueError, key):
                    STUDY.verify_runtime(root, "candidate", patch)

    def child(self, body, timeout=5):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            binary = root / "fake-runtime"
            binary.write_text("#!" + sys.executable + "\n" + body)
            binary.chmod(0o700)
            return STUDY.collect_trial(binary, root / "unused-fixture", root / "trial", "ordinary", 7,
                                       {"test": "fixture"}, timeout, 0)

    def test_nonzero_exit_cannot_reuse_a_pass_receipt(self):
        body = "import json\nprint(json.dumps(" + repr(passing()) + "))\nraise SystemExit(3)\n"
        result = self.child(body)
        self.assertEqual(result["outcome"], "invalid_receipt")
        self.assertFalse(result["complete"])

    def test_controlled_child_pass_is_collected(self):
        result = self.child("import json\nprint(json.dumps(" + repr(passing()) + "))\n")
        self.assertEqual(result["outcome"], "pass")
        self.assertTrue(result["complete"])

    def test_process_loss_is_preserved_without_a_receipt(self):
        result = self.child("import os, signal\nos.kill(os.getpid(), signal.SIGTERM)\n")
        self.assertEqual(result["outcome"], "process_loss")
        self.assertTrue(result["complete"])
        self.assertNotEqual(result["returncode"], 0)

    def test_collector_timeout_is_not_runtime_failure_evidence(self):
        result = self.child("import time\ntime.sleep(5)\n", timeout=0.05)
        self.assertEqual(result["outcome"], "timeout")
        self.assertFalse(result["complete"])
        self.assertTrue(result["timed_out"])


if __name__ == "__main__":
    unittest.main()
