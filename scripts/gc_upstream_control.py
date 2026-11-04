#!/usr/bin/env python3
"""Fixed functional LuaJIT #1085 control; audited runtime reuse, no qualification."""
import argparse
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import time

from gc_trace_coverage import AUDITED, REUSE_RUN, FILE_LIMIT, child_limits, verify_audited_runtime
from gc_upvalue_study import save, sha, source_identity

PROFILES, TRIALS, TIMEOUT = ('on', 'off'), 3, 10
CONFIG = {'blocks': 3, 'stores_per_block': 100, 'total_stores': 300,
          'gc': 'incremental step(1) after each store'}
INTERPRETATION = 'bounded functional control only; no Tarn crash cause or fix established'


def control_source_identity(root):
    identity = source_identity(root)
    for relative in ('scripts/gc_upstream_control.py', 'scripts/gc_trace_coverage.py',
                     'tests/test_gc_upstream_control.py', 'tests/gc-upvalue/upstream_control.lua',
                     'tests/gc-upvalue/README.md'):
        identity['files'][relative] = sha(root / relative)
    return identity


def validate_common(record, kind, profile, identities):
    if not isinstance(record, dict) or record.get('schema_version') != 1:
        raise ValueError('missing receipt/version')
    if (record.get('kind') != kind or record.get('fixture') != 'luajit-1085-functional-control'
            or record.get('profile') != profile or record.get('source_identities') != identities):
        raise ValueError('receipt kind/profile/source identity mismatch')
    config = record.get('config')
    if not isinstance(config, dict) or set(config) != set(CONFIG) or any(
            type(config[k]) is not type(v) or config[k] != v for k, v in CONFIG.items()):
        raise ValueError('configuration mismatch')
    runtime = record.get('runtime', {})
    if (runtime.get('os') != 'Linux' or runtime.get('arch') != 'x64'
            or runtime.get('gc64') is not False):
        raise ValueError('runtime architecture mismatch')
    if not all(isinstance(runtime.get(k), str) and runtime[k] for k in ('tarantool', 'luajit')):
        raise ValueError('missing actual runtime versions')
    for key in ('initial_jit', 'effective_jit'):
        state = runtime.get(key, {})
        if type(state.get('enabled')) is not bool or not isinstance(state.get('flags'), list):
            raise ValueError('missing actual JIT status')
    if runtime['effective_jit']['enabled'] is not (profile == 'on'):
        raise ValueError('JIT configuration mismatch')


def functional_observation(stdout, returncode, timed_out, file_cap, profile, identities):
    preflight = None
    try:
        lines = stdout.read_text().splitlines()
        if not lines:
            raise ValueError('missing preflight')
        preflight = json.loads(lines[0])
        validate_common(preflight, 'preflight', profile, identities)
    except (ValueError, TypeError, AttributeError, KeyError) as error:
        return {'outcome': 'setup_failure', 'complete': False, 'error': str(error)}
    base = {'preflight': preflight}
    if timed_out or file_cap:
        return {**base, 'outcome': 'timeout' if timed_out else 'file_limit', 'complete': False}
    if returncode < 0:
        return {**base, 'outcome': 'process_loss', 'complete': True}
    try:
        if len(lines) != 2:
            raise ValueError('expected preflight and completion receipts')
        final = json.loads(lines[1])
        validate_common(final, 'completion', profile, identities)
        if final['runtime'] != preflight['runtime']:
            raise ValueError('runtime identity changed between receipts')
        if final.get('complete') is not True or type(final.get('passed')) is not bool:
            raise ValueError('missing completion')
        if final.get('final_jit') != preflight['runtime']['effective_jit']:
            raise ValueError('JIT state changed')
        nodes = final.get('nodes_checked')
        if type(nodes) is not int or not 0 <= nodes <= 300:
            raise ValueError('invalid node count')
        if final['passed']:
            if nodes != 300 or returncode != 0:
                raise ValueError('pass requires 300 nodes and exit zero')
            outcome = 'pass'
        else:
            if returncode != 1 or not isinstance(final.get('error'), str) or not final['error']:
                raise ValueError('invariant failure requires error and exit one')
            outcome = 'invariant_failure'
        return {**base, 'outcome': outcome, 'complete': True, 'receipt': final}
    except (ValueError, TypeError, AttributeError, KeyError) as error:
        return {**base, 'outcome': 'invalid_receipt', 'complete': False, 'error': str(error)}


def collect_trial(binary, fixture, directory, profile, identities):
    if profile not in PROFILES:
        raise ValueError('unsupported fixed profile')
    directory.mkdir()
    command = [str(binary), str(fixture), profile]
    invocation = {'schema_version': 1, 'reuse_run': REUSE_RUN, 'command': command,
                  'profile': profile, 'trials_per_profile': TRIALS, 'config': CONFIG,
                  'timeout_seconds': TIMEOUT, 'core_limit_bytes': 0,
                  'file_limit_bytes': FILE_LIMIT, 'source_identities': identities}
    save(directory / 'invocation.json', invocation)
    env = dict(os.environ, TARN_GC_SOURCE_IDENTITIES_JSON=json.dumps(identities, sort_keys=True))
    start, timed_out, process, launch_error = time.monotonic(), False, None, None
    with (directory / 'stdout.log').open('wb') as out, (directory / 'stderr.log').open('wb') as err:
        try:
            process = subprocess.Popen(command, cwd=directory, env=env, stdout=out, stderr=err,
                                       preexec_fn=child_limits)
        except (OSError, subprocess.SubprocessError) as error:
            launch_error = str(error)
        if process is not None:
            invocation['pid'] = process.pid
            save(directory / 'invocation.json', invocation)
            try:
                process.wait(timeout=TIMEOUT)
            except subprocess.TimeoutExpired:
                timed_out = True
                # Only the owned unreaped child, never process groups or a PID search.
                process.terminate()
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
    files = {name: {'bytes': (directory / name).stat().st_size, 'sha256': sha(directory / name)}
             for name in ('stdout.log', 'stderr.log')}
    returncode = process.returncode if process is not None else None
    file_cap = returncode == -signal.SIGXFSZ or any(
        f['bytes'] >= FILE_LIMIT for f in files.values())
    functional = ({'outcome': 'setup_failure', 'complete': False, 'error': launch_error}
                  if launch_error is not None else functional_observation(
                      directory / 'stdout.log', returncode, timed_out, file_cap, profile, identities))
    result = {'schema_version': 1, 'profile': profile, 'pid': process.pid if process else None,
              'returncode': returncode, 'timed_out': timed_out, 'launch_error': launch_error,
              'signal': -returncode if returncode is not None and returncode < 0 else None,
              'file_cap_reached': file_cap, 'elapsed_seconds': time.monotonic() - start,
              'collection_complete': functional['complete'], 'functional': functional,
              'qualification_pass': False, 'interpretation': INTERPRETATION,
              'files': files, 'invocation_sha256': sha(directory / 'invocation.json')}
    save(directory / 'result.json', result)
    return result


def run(args):
    if platform.system() != 'Linux' or platform.machine() != 'x86_64':
        raise ValueError('functional control requires Linux x86-64')
    root, runtime, output = args.source.resolve(), args.runtime.resolve(), args.output.resolve()
    if output.exists():
        raise ValueError('output must be a fresh directory')
    patch = root / 'ci/gc-runtime/urefo-x86.patch'
    receipt = verify_audited_runtime(runtime, args.arm, patch)
    identity = control_source_identity(root)
    identities = {'tarn': identity, 'reuse_run': REUSE_RUN, 'arm': args.arm, **AUDITED[args.arm]}
    output.mkdir(parents=True)
    study = {'schema_version': 1, 'reuse_run': REUSE_RUN, 'arm': args.arm,
             'source_identities': identities, 'runtime_receipt': receipt,
             'profiles': list(PROFILES), 'trials_per_profile': TRIALS, 'config': CONFIG,
             'timeout_seconds': TIMEOUT, 'core_limit_bytes': 0, 'file_limit_bytes': FILE_LIMIT,
             'collection_complete': False, 'qualification_pass': False,
             'interpretation': INTERPRETATION, 'trials': []}
    manifest = output / 'upstream.json'
    save(manifest, study)
    for profile in PROFILES:
        for trial in range(1, TRIALS + 1):
            folder = output / f'{profile}-{trial:02d}'
            result = collect_trial(runtime / 'bin/tarantool',
                                   root / 'tests/gc-upvalue/upstream_control.lua',
                                   folder, profile, identities)
            study['trials'].append({'profile': profile, 'trial': trial,
                                    'result_sha256': sha(folder / 'result.json'), 'result': result})
            save(manifest, study)
    try:
        if (verify_audited_runtime(runtime, args.arm, patch) != receipt
                or control_source_identity(root) != identity):
            raise ValueError('inputs changed during collection')
        study['inputs_verified_after'] = True
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        study['inputs_verified_after'] = False
        study['identity_error'] = str(error)
    study['collection_complete'] = study['inputs_verified_after'] and all(
        t['result']['collection_complete'] for t in study['trials'])
    outcomes = [t['result']['functional']['outcome'] for t in study['trials']]
    study['functional_outcome'] = (
        'unvalidated_inputs' if not study['inputs_verified_after'] else
        'bounded_negative' if all(o == 'pass' for o in outcomes) else 'unsuccessful_observation')
    save(manifest, study)
    print(json.dumps({k: study[k] for k in ('arm', 'collection_complete', 'functional_outcome',
                                          'qualification_pass')}))
    return 0 if study['collection_complete'] and (
        args.arm == 'baseline' or study['functional_outcome'] == 'bounded_negative') else 1


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
        parser.exit(2, f'upstream control collection failed: {error}\n')


if __name__ == '__main__':
    raise SystemExit(main())
