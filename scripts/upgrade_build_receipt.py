#!/usr/bin/env python3
"""Bind isolated upgrade artifacts to all compiled inputs before the matrix."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def build_inputs(root):
    files = [root / name for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml")]
    files += sorted(p for p in (root / "crates").rglob("*") if p.is_file())
    return {str(p.relative_to(root)): sha(p) for p in files}


def verify(receipt, root, gateway, native):
    assert build_inputs(root) == receipt["inputs"], "upgrade compiled inputs changed"
    assert sha(gateway) == receipt["gateway_sha256"], "upgrade gateway artifact changed"
    assert sha(native) == receipt["native_sha256"], "upgrade native artifact changed"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "gateway", "native", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    root = args.source.resolve()
    record = {"revision": args.revision, "inputs": build_inputs(root),
              "gateway_sha256": sha(args.gateway), "native_sha256": sha(args.native),
              "rustc": subprocess.check_output(["rustc", "-Vv"], text=True),
              "profile_overrides": {k: v for k, v in os.environ.items() if k.startswith("CARGO_PROFILE_")},
              "build": "cargo build --release --workspace --locked; isolated target directory"}
    args.output.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
