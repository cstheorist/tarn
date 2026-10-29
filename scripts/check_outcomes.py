#!/usr/bin/env python3
"""Check that every outcome contract has an executable case and vice versa."""
import ast
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
m = json.loads((root / 'contracts/outcomes.json').read_text())
assert m['schema'] == 1
entries = m['contracts']
ids = [e['id'] for e in entries]
assert len(ids) == len(set(ids))
functions = {n.name[5:] for n in ast.parse((root / 'tests/sql/outcomes.py').read_text()).body
             if isinstance(n, ast.FunctionDef) and n.name.startswith('case_')}
assert set(ids) == functions, (set(ids), functions)
for e in entries:
    assert e['test'] == 'tests/sql/outcomes.py::case_' + e['id']
    assert e['classification'] in ('equivalent', 'difference', 'extension')
    assert e['description']
assert (root / 'docs/outcomes.md').is_file()
print(f'{len(entries)} outcome contracts registered')
