#!/usr/bin/env python3
"""Reject duplicate/unclassified contract entries or stale regression links.

This checks manifest coverage links, not assertion strength or oracle
equivalence. The referenced suites remain independently required in CI.
"""
import ast
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]


def main():
    seen = set()
    for fragment in sorted(Path(__file__).parent.glob("*.json")):
        spec = json.loads(fragment.read_text())
        assert spec["format"] == "tarn-contract-fragment-v1", fragment
        assert (ROOT / spec["documentation"]).is_file(), fragment
        for entry in spec["contracts"]:
            assert entry["id"] not in seen, entry["id"]
            seen.add(entry["id"])
            assert entry["classification"] in {"equivalent", "intentional_extension", "rejected"}, entry
            assert entry["behavior"] and entry["tests"], entry
            for link in entry["tests"]:
                file, symbol = link.split("::", 1)
                text = (ROOT / file).read_text()
                if file.endswith(".py"):
                    node = ast.parse(text)
                    for name in symbol.split("."):
                        found = [x for x in node.body if isinstance(x, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and x.name == name]
                        assert len(found) == 1, link
                        node = found[0]
                elif file.endswith(".rs"):
                    assert re.search(r"\bfn\s+" + re.escape(symbol) + r"\s*\(", text), link
                else:
                    raise AssertionError("unsupported regression reference: " + link)
    assert seen, "no contract entries"
    print(f"{len(seen)} classified contract entries have documentation and regression links")


if __name__ == "__main__":
    main()
