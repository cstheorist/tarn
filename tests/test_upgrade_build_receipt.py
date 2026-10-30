#!/usr/bin/env python3
"""Reject changed source/artifact inputs before running an upgrade matrix."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("receipts", Path(__file__).resolve().parents[1] / "scripts/upgrade_build_receipt.py")
receipts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(receipts)


class BuildReceipts(unittest.TestCase):
    def test_changed_source_or_artifact_is_refused(self):
        for changed in ("crates/body.rs", "Cargo.lock", "gateway", "native"):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "crates").mkdir()
                for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "crates/body.rs", "gateway", "native"):
                    (root / name).write_text(name)
                record = {"inputs": receipts.build_inputs(root), "gateway_sha256": receipts.sha(root / "gateway"),
                          "native_sha256": receipts.sha(root / "native")}
                receipts.verify(record, root, root / "gateway", root / "native")
                (root / changed).write_text("changed")
                with self.assertRaises(AssertionError):
                    receipts.verify(record, root, root / "gateway", root / "native")


if __name__ == "__main__":
    unittest.main()
