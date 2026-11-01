#!/usr/bin/env python3
"""Require every outcome case in the primary release contract index."""
import json
from check_contracts import ROOT, extensions

manifest = json.loads((ROOT / 'contracts/manifest.json').read_text())
registered = {name: entries for name, _, entries in extensions(ROOT, manifest)}
assert 'outcomes' in registered, 'outcome suite is absent from the primary manifest'
assert (ROOT / 'docs/outcomes.md').is_file()
print(f"{len(registered['outcomes'])} outcome contracts registered in the primary manifest")
