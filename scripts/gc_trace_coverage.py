#!/usr/bin/env python3
"""Bounded trace observation using only the audited runtimes from run 41764862465."""

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import platform
import resource
import signal
import subprocess
import time

from gc_upvalue_study import save, sha, source_identity, validate_receipt, verify_runtime


REUSE_RUN = 41764862465
PROFILES = ('ordinary', 'construction-warm', 'retained-construction-warm')
ITERATIONS, TIMEOUT, FILE_LIMIT = 20000, 30, 8 * 1024 * 1024
# Independently audited artifact identities; a self-consistent new receipt is insufficient.
AUDITED = {
    'baseline': {
        'binary_sha256': '359258b1f01dd814d0560cd4612307f8183563f2b51906c93205757d75a267b2',
        'runtime_receipt_sha256': '427e6f03241c664e4c395dcaaec5e59c8ed7f0849d598afb5393ca1fbd981a43',
    },
    'candidate': {
        'binary_sha256': '631b4615920d01d1439fc74ed9825d7d4889384b6aac6f1b38fa8e73ff16fbbd',
        'runtime_receipt_sha256': '24435b559f307829292340d5cf0c896aa5fa3613b0c2bc309091f3896273668e',
    },
}


def verify_audited_runtime(directory, arm, patch):
    expected = AUDITED[arm]
    if sha(directory / 'runtime-receipt.json') != expected['runtime_receipt_sha256']:
        raise ValueError('runtime receipt differs from audited reuse artifact')
    if sha(directory / 'bin/tarantool') != expected['binary_sha256']:
        raise ValueError('runtime binary differs from audited reuse artifact')
    receipt = verify_runtime(directory, arm, patch)
    evidence = receipt.get('evidence_sha256')
    if not isinstance(evidence, dict) or not evidence:
        raise ValueError('audited runtime lacks evidence hashes')
    for relative, digest in evidence.items():
        path = PurePosixPath(relative)
        if path.is_absolute() or '..' in path.parts or not path.parts:
            raise ValueError('unsafe runtime evidence path')
        local = directory / relative
        if not local.resolve().is_relative_to(directory.resolve()) or sha(local) != digest:
            raise ValueError(f'runtime evidence mismatch: {relative}')
    return receipt


def observer_source_identity(root):
    identity = source_identity(root)
    for relative in ('scripts/gc_trace_coverage.py', 'tests/gc-upvalue/observe.lua',
                     'tests/test_gc_trace_coverage.py', 'tests/gc-upvalue/README.md'):
        identity['files'][relative] = sha(root / relative)
    return identity


def child_limits():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    # Python ignores SIGXFSZ; reset it so a child reaching the bound is observable.
    signal.signal(signal.SIGXFSZ, signal.SIG_DFL)
    resource.setrlimit(resource.RLIMIT_FSIZE, (FILE_LIMIT, FILE_LIMIT))


def functional_observation(stdout, returncode, profile, identities):
    try:
        record = json.loads(stdout.read_text())
        outcome = validate_receipt(record, profile, ITERATIONS, identities)
        if (returncode == 0) != (outcome == 'pass'):
            raise ValueError('exit status disagrees with functional receipt')
        return {'outcome': outcome, 'receipt': record}
    except (ValueError, TypeError, AttributeError, KeyError) as error:
        return {'outcome': 'invalid_receipt', 'error': str(error)}


def collect_trial(binary, fixture, wrapper, directory, profile, identities):
    if profile not in PROFILES:
        raise ValueError('unsupported fixed diagnostic profile')
    directory.mkdir()
    work, dump = directory / 'data', directory / 'trace.dump'
    work.mkdir()
    env = dict(os.environ, TARN_GC_WORK_DIR=str(work), TARN_GC_TRACE_DUMP=str(dump),
               TARN_GC_SOURCE_IDENTITIES_JSON=json.dumps(identities, sort_keys=True))
    command = [str(binary), str(wrapper), str(fixture), profile, str(ITERATIONS)]
    invocation = {'schema_version': 1, 'reuse_run': REUSE_RUN, 'command': command,
                  'profile': profile, 'iterations': ITERATIONS, 'trials_per_profile': 1,
                  'timeout_seconds': TIMEOUT, 'core_limit_bytes': 0,
                  'file_limit_bytes': FILE_LIMIT, 'dump_mode': 'tbiT',
                  'source_identities': identities}
    save(directory / 'invocation.json', invocation)
    start, timed_out = time.monotonic(), False
    with (directory / 'stdout.log').open('wb') as out, (directory / 'stderr.log').open('wb') as err:
        process = subprocess.Popen(command, cwd=directory, env=env, stdout=out, stderr=err,
                                   preexec_fn=child_limits)
        invocation['pid'] = process.pid
        save(directory / 'invocation.json', invocation)
        try:
            process.wait(timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            timed_out = True
            # Only this invocation's owned, unreaped child; never a process search/group.
            os.kill(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.kill(process.pid, signal.SIGKILL)
                process.wait()
    files = {name: {'bytes': (directory / name).stat().st_size, 'sha256': sha(directory / name)}
             for name in ('stdout.log', 'stderr.log', 'trace.dump') if (directory / name).is_file()}
    file_cap = process.returncode == -signal.SIGXFSZ or any(
        entry['bytes'] >= FILE_LIMIT for entry in files.values())
    if timed_out:
        observation = 'incomplete_timeout'
    elif file_cap:
        observation = 'incomplete_file_limit'
    elif process.returncode < 0:
        observation = 'process_loss'
    elif 'trace.dump' not in files or files['trace.dump']['bytes'] == 0:
        observation = 'incomplete_dump'
    else:
        observation = 'collected'
    result = {'schema_version': 1, 'profile': profile, 'pid': process.pid,
              'returncode': process.returncode, 'timed_out': timed_out, 'file_cap_reached': file_cap,
              'signal': -process.returncode if process.returncode < 0 else None,
              'elapsed_seconds': time.monotonic() - start, 'observer_outcome': observation,
              'observer_complete': observation == 'collected',
              'functional': functional_observation(directory / 'stdout.log', process.returncode,
                                                   profile, identities),
              'target_coverage': 'unassessed; requires source/prototype/IR review',
              'interpretation': 'diagnostic observation only; no runtime cause or fix established',
              'files': files, 'invocation_sha256': sha(directory / 'invocation.json')}
    save(directory / 'result.json', result)
    return result


def run(args):
    if platform.system() != 'Linux' or platform.machine() != 'x86_64':
        raise ValueError('trace diagnostic requires Linux x86-64')
    root, runtime, output = args.source.resolve(), args.runtime.resolve(), args.output.resolve()
    if output.exists():
        raise ValueError('output must be a fresh directory')
    patch = root / 'ci/gc-runtime/urefo-x86.patch'
    receipt = verify_audited_runtime(runtime, args.arm, patch)
    identity = observer_source_identity(root)
    identities = {'tarn': identity, 'reuse_run': REUSE_RUN, 'arm': args.arm,
                  **AUDITED[args.arm]}
    output.mkdir(parents=True)
    study = {'schema_version': 1, 'reuse_run': REUSE_RUN, 'arm': args.arm,
             'source_identities': identities, 'runtime_receipt': receipt,
             'profiles': list(PROFILES), 'iterations_per_trial': ITERATIONS,
             'trials_per_profile': 1, 'timeout_seconds': TIMEOUT,
             'file_limit_bytes': FILE_LIMIT, 'core_limit_bytes': 0,
             'collection_complete': False, 'qualification_pass': False,
             'target_coverage': 'unassessed; requires source/prototype/IR review',
             'interpretation': 'observer changes allocation/I/O; no runtime cause or fix established',
             'trials': []}
    save(output / 'coverage.json', study)
    for profile in PROFILES:
        folder = output / (profile + '-01')
        result = collect_trial(runtime / 'bin/tarantool', root / 'tests/gc_upvalue.lua',
                               root / 'tests/gc-upvalue/observe.lua', folder, profile, identities)
        study['trials'].append({'profile': profile, 'result_sha256': sha(folder / 'result.json'),
                                'result': result})
        save(output / 'coverage.json', study)
    # Reject changing source, runtime or any runtime evidence after the observations.
    if verify_audited_runtime(runtime, args.arm, patch) != receipt or observer_source_identity(root) != identity:
        raise ValueError('inputs changed during trace collection')
    study['collection_complete'] = all(t['result']['observer_complete'] for t in study['trials'])
    save(output / 'coverage.json', study)
    print(json.dumps({'arm': args.arm, 'collection_complete': study['collection_complete'],
                      'target_coverage': study['target_coverage'], 'qualification_pass': False}))
    return 0 if study['collection_complete'] and all(
        t['result']['functional']['outcome'] == 'pass' for t in study['trials']) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--runtime', type=Path, required=True)
    parser.add_argument('--arm', choices=tuple(AUDITED), required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        return run(args)
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        parser.exit(2, f'trace observer failed: {error}\n')


if __name__ == '__main__':
    raise SystemExit(main())
