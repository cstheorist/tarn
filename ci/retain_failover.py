#!/usr/bin/env python3
"""Retain ordinary CI failover inputs and stopped fixture/core evidence (#160).

This collector never declares a test passed and never changes its assertions.
Run only on the isolated CI runner; fixture shutdown belongs to the workflow.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile


def identity(path):
    h = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1048576), b''):
            h.update(chunk)
    return dict(bytes=path.stat().st_size, sha256=h.hexdigest())


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def begin(source, output, artifacts, core_pattern):
    output.mkdir(parents=True, exist_ok=False)
    (output / 'binaries').mkdir()
    receipt = dict(schema=1, scope='Input identity and crash evidence; not a passing-test or causal-fix claim.',
                   source_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=source, text=True).strip(),
                   source_status=subprocess.check_output(['git', 'status', '--porcelain'], cwd=source, text=True),
                   core_pattern=core_pattern, binaries={}, sources={})
    tracked = subprocess.check_output(['git', 'ls-files', '-z'], cwd=source).decode().split('\0')
    for name in tracked:
        if name and (source / name).is_file() and (name.startswith(('crates/', 'lua/', 'scripts/', 'tests/'))
                                                 or name in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml')):
            receipt['sources'][name] = identity(source / name)
    # Copy before execution so an overwritten build cannot replace evidence.
    for name, path in artifacts.items():
        original = path.resolve(strict=True)
        target = output / 'binaries' / name
        shutil.copy2(original, target)
        copied = identity(target)
        if identity(original) != copied:
            raise RuntimeError('binary changed during capture: ' + name)
        receipt['binaries'][name] = dict(path=str(original), **copied)
    save(output / 'before.json', receipt)


def archive(root, output):
    inventory = {}
    # Do not follow symlinks out of the owned fixture. The archive preserves
    # symlinks themselves, while the inventory explicitly identifies them.
    for path in sorted(root.rglob('*')):
        name = str(path.relative_to(root))
        if path.is_symlink():
            inventory[name] = dict(symlink=str(path.readlink()))
        elif path.is_file():
            inventory[name] = identity(path)
    with tarfile.open(output, 'w:gz', compresslevel=1, dereference=False) as result:
        result.add(root, arcname=root.name)
    return dict(archive=identity(output), files=inventory)


def finish(output, fixtures, cores, job_status):
    before = json.loads((output / 'before.json').read_text())
    result = dict(schema=1, job_status=job_status, binaries={}, fixtures={}, cores=None,
                  complete=False, errors=[])
    # A partial collector failure still leaves the pre-run inputs and all
    # successfully archived evidence available to the always-upload step.
    for name, old in before['binaries'].items():
        try:
            actual = identity(Path(old['path']))
            actual['unchanged'] = all(actual[k] == old[k] for k in ('bytes', 'sha256'))
            result['binaries'][name] = actual
            if not actual['unchanged']:
                result['errors'].append('binary changed after capture: ' + name)
        except OSError as error:
            result['errors'].append(name + ': ' + str(error))
    for fixture in fixtures:
        if not fixture.exists():
            result['fixtures'][str(fixture)] = dict(present=False)
            continue
        try:
            result['fixtures'][str(fixture)] = dict(present=True, **archive(fixture, output / (fixture.name + '.tar.gz')))
        except Exception as error:
            result['errors'].append(str(fixture) + ': ' + str(error))
    try:
        result['cores'] = archive(cores, output / 'cores.tar.gz')
    except Exception as error:
        result['errors'].append('cores: ' + str(error))
    result['complete'] = not result['errors']
    save(output / 'after.json', result)
    return result['complete']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('begin', 'finish'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--cores', type=Path, required=True)
    parser.add_argument('--fixture', action='append', type=Path, default=[])
    parser.add_argument('--job-status', default='unknown')
    args = parser.parse_args()
    if args.command == 'begin':
        args.cores.mkdir(parents=True, exist_ok=False)
        begin(args.source, args.output, {
            'tarantool': Path(shutil.which('tarantool')),
            'tarn-server': args.source / 'target/release/tarn-server',
            'libtarn_storage.so': args.source / 'target/release/libtarn_storage.so'},
            Path('/proc/sys/kernel/core_pattern').read_text().strip())
    elif not finish(args.output, args.fixture, args.cores, args.job_status):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
