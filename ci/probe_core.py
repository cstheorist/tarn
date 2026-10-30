#!/usr/bin/env python3
"""Verify core generation by one intentionally crashed, empty Tarantool child.

Use only on an isolated Linux test runner. No Tarn/database is loaded, and
the child receives an empty environment. The probe core stays separate from
real failover cores; only its identity/header and expected crash log are kept.
"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import resource
import signal
import subprocess


def identity(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1048576), b''):
            digest.update(chunk)
    return dict(bytes=path.stat().st_size, sha256=digest.hexdigest())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    runtime, output = args.runtime.resolve(strict=True), args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    result = dict(complete=False, scope='Intentional empty-runtime core-generation control; not a failover crash.',
                  runtime=identity(runtime), child_environment='empty',
                  architecture=platform.machine(), system=platform.system(),
                  requested_signal='SIGSEGV', expected_exit_signal='SIGABRT')
    try:
        pattern = Path('/proc/sys/kernel/core_pattern').read_text().strip()
        result['core_pattern'] = pattern
        # Never send a probe to a host collector or write outside this
        # owned directory. Relative `core` is used by the local container.
        if pattern != 'core' and not (Path(pattern).parent == output and Path(pattern).name.startswith('core.')):
            raise RuntimeError('core pattern is not confined to the owned probe directory')
        _, hard = resource.getrlimit(resource.RLIMIT_CORE)
        if hard != resource.RLIM_INFINITY:
            raise RuntimeError('unlimited hard core limit is required for this control')
        def enable_core():
            resource.setrlimit(resource.RLIMIT_CORE, (hard, hard))
        with (output / 'crash.txt').open('wb') as log:
            child = subprocess.run([str(runtime), '-e',
                                    "local f=require('ffi');f.cdef'int raise(int);';f.C.raise(11)"],
                                   cwd=output, env={}, stdin=subprocess.DEVNULL,
                                   stdout=log, stderr=subprocess.STDOUT, timeout=30,
                                   preexec_fn=enable_core)
        result['returncode'] = child.returncode
        result['crash_log'] = identity(output / 'crash.txt')
        if child.returncode != -signal.SIGABRT:
            raise RuntimeError('pinned Tarantool did not terminate through its expected crash handler')
        if b'Segmentation fault' not in (output / 'crash.txt').read_bytes():
            raise RuntimeError('probe did not report the requested SIGSEGV')
        cores = list(output.glob('core*'))
        if len(cores) != 1 or not cores[0].is_file() or cores[0].is_symlink():
            raise RuntimeError('expected exactly one regular probe core')
        core = cores[0]
        with core.open('rb') as source:
            header = source.read(64)
        if len(header) != 64 or header[:4] != b'\x7fELF' or header[5] != 1 or header[16:18] != b'\x04\x00':
            raise RuntimeError('probe output is not a little-endian ELF core')
        result['core'] = dict(name=core.name, header_hex=header.hex(), **identity(core))
        result['complete'] = True
    except Exception as error:
        result['error'] = str(error)
        raise
    finally:
        (output / 'probe.json').write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
