#!/usr/bin/env python3
"""Write a compact receipt after a successful isolated diagnostic build."""
import hashlib
import json
import os
import pathlib
import platform
import sys

out = pathlib.Path(sys.argv[1])
arm = sys.argv[2]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


cache = {}
for line in (out / 'CMakeCache.txt').read_text().splitlines():
    if line and not line.startswith(('#', '//')) and '=' in line and ':' in line.split('=', 1)[0]:
        key, value = line.split('=', 1)
        cache[key.split(':', 1)[0]] = value

receipt = {
    'schema_version': 1,
    'purpose': 'Tarn #160 defensive functional diagnostic; no causal attribution',
    'arm': arm,
    'tarantool_commit': (out / 'inputs/tarantool-commit.txt').read_text().strip(),
    'luajit_commit': (out / 'inputs/luajit-commit.txt').read_text().strip(),
    'luajit_describe': (out / 'inputs/luajit-describe.txt').read_text().strip(),
    'luajit_shallow': (out / 'inputs/luajit-shallow-after.txt').read_text().strip() == 'true',
    'upstream_fix_commit': '07b3cd3cf9b57a3801a1ebc48144767e31671f21' if arm == 'candidate' else None,
    'patch_sha256': sha(out / 'inputs/urefo-x86.patch') if arm == 'candidate' else None,
    'upstream_patch_sha256': sha(out / 'inputs/upstream-07b3cd3c.patch'),
    'source_dirty_diff_sha256': sha(out / 'inputs/source-dirty.diff'),
    'binary_sha256': sha(out / 'bin/tarantool'),
    'build_type': cache['CMAKE_BUILD_TYPE'],
    'architecture': platform.machine(),
    'os': platform.system(),
    'gc64': False,
    'runtime_probe': (out / 'logs/runtime-probe.log').read_text().strip(),
    'jobs': int(sys.argv[3]),
    'compiler': {key: value for key, value in cache.items() if key.startswith(('CMAKE_C_COMPILER', 'CMAKE_CXX_COMPILER'))},
    'config': {key: cache[key] for key in ('CMAKE_BUILD_TYPE', 'ENABLE_BUNDLED_MIMALLOC', 'MIMALLOC_OVERRIDE', 'LUAJIT_ENABLE_GC64')},
    'build_environment': {key: os.environ.get(key) for key in ('CC', 'CXX', 'CFLAGS', 'CXXFLAGS', 'LDFLAGS')},
    'submodules': (out / 'inputs/submodules.txt').read_text().splitlines(),
    'evidence_sha256': {},
}
for folder in ('inputs', 'logs'):
    for path in sorted((out / folder).iterdir()):
        if path.is_file():
            receipt['evidence_sha256'][str(path.relative_to(out))] = sha(path)
receipt['evidence_sha256']['CMakeCache.txt'] = sha(out / 'CMakeCache.txt')
(out / 'runtime-receipt.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
