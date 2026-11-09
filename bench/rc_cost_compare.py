#!/usr/bin/env python3
"""Matched checks-off RF3/fsync pilots of a READ COMMITTED change.

--hypothesis guards: copy the fixture sources into an owned temporary
directory and apply only the guard batching change in the candidate copy.
--hypothesis lock-barrier: both copies are the same sources; the candidate's
storage nodes run TARN_LOCK_BARRIER=owner, the control's queue (#314);
lock-barrier-auto: the same with auto.
Normal release binaries, admission, durability and query histories remain
matched. Stop on any failed/unknown attempt; keep both original storage
state and partial measurements.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import time

from rc_profile import DB, connect, evaluate, query, sample as sql_sample, sha
import rc_guard_candidate
import rc_direct

ROOT = Path(__file__).resolve().parents[1]
SHAPES = ('uncontended', 'unrelated', 'hot')
VARIANTS = ('control', 'candidate')
# Each hypothesis's storage environment per variant.
VARIANT_ENV = {'guards': {'control': {}, 'candidate': {}},
               'lock-barrier': {'control': {'TARN_LOCK_BARRIER': 'queue'},
                                'candidate': {'TARN_LOCK_BARRIER': 'owner'}},
               'lock-barrier-auto': {'control': {'TARN_LOCK_BARRIER': 'queue'},
                                     'candidate': {'TARN_LOCK_BARRIER': 'auto'}}}


def sources(work, hypothesis):
    paths = {}
    for variant in VARIANTS:
        root = work / variant
        for folder in ('lua', 'scripts'):
            shutil.copytree(ROOT / folder, root / folder)
        paths[variant] = root
    if hypothesis == 'guards':
        for variant in VARIANTS:
            rc_guard_candidate.apply(paths[variant], variant)
    control = {str(p.relative_to(paths['control'])): sha(p)
               for p in paths['control'].rglob('*') if p.is_file()}
    candidate = {str(p.relative_to(paths['candidate'])): sha(p)
                 for p in paths['candidate'].rglob('*') if p.is_file()}
    assert control.keys() == candidate.keys()
    changed = {p for p in control if control[p] != candidate[p]}
    assert changed == ({'lua/tarn.lua', 'lua/tarn_fresh.lua'} if hypothesis == 'guards' else set())
    return paths


# The lock-barrier lane's mechanism, read after its timing: every storage
# node runs its variant's barrier, and only the candidate's leaders ran
# rounds of peer replies, none sent to the full barrier.
BARRIER = """local t = require('tarn')
return {ro = box.info.ro, barrier = t.cfg.lock_barrier, rounds = t.fresh.ob.rounds, fallbacks = t.fresh.ob.fallbacks}"""


def barrier_mechanism(endpoints, hypothesis, variant):
    nodes = evaluate(endpoints, BARRIER)
    want = VARIANT_ENV[hypothesis][variant]['TARN_LOCK_BARRIER']
    assert all(n['result']['barrier'] == want for n in nodes), nodes
    rounds = sum(n['result']['rounds'] for n in nodes)
    assert all(n['result']['fallbacks'] == 0 for n in nodes), nodes
    assert (rounds > 0) if variant == 'candidate' else (rounds == 0), nodes
    return nodes


def setup(args):
    with connect(args.sql, None) as c:
        query(c, f'CREATE DATABASE {DB}')
        c.select_db(DB)
        query(c, f'CREATE TABLE t (tenant INT, id INT, v BIGINT, payload VARCHAR(256), '
              f'PRIMARY KEY(tenant,id)) ENGINE={args.engine} PARTITION BY KEY(tenant)')
        query(c, f'CREATE TABLE ledger (tenant INT, token BIGINT, k INT, '
              f'PRIMARY KEY(tenant,token)) ENGINE={args.engine} PARTITION BY KEY(tenant)')
        for k in range(8):
            query(c, 'INSERT INTO t VALUES (1,%s,0,%s)', (k, 'x' * 256))


def stop(script, cluster, env):
    pids = [int(p.read_text()) for p in cluster.glob('*.pid')]
    subprocess.run(['bash', str(script), 'stop', '-d', str(cluster)],
                   env=dict(env, KEEP='1'), check=True, timeout=60)
    # The helper returns after a short grace period. Do not start a new lane
    # while its predecessor still owns the ports or consumes benchmark CPU.
    deadline = time.monotonic() + 15
    while True:
        alive = []
        for pid in pids:
            try:
                fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
                if fields[0] != 'Z':
                    alive.append(pid)
            except FileNotFoundError:
                pass
        if not alive:
            return
        assert time.monotonic() < deadline, ('owned fixture did not stop', alive)
        time.sleep(.05)


def summarize(sessions, variants=('control', 'candidate')):
    summaries = []
    for shape in SHAPES:
        pairs = []
        for pair in range(5):
            controls = {}
            for variant in variants:
                session = next(s for s in sessions if s['pair'] == pair and s['variant'] == variant)
                s = next(s for s in session['samples'] if s['shape'] == shape and not s['warmup'])
                assert s['complete'] and not s['profiled']
                controls[variant] = dict(
                    goodput=s['goodput_per_second'], latency=s['latency'],
                    acknowledged=s['acknowledged'], refused=s['refused'],
                    lua_allocated_per_ack=sum(r['metrics']['gc_allocated'] for r in s['deltas']) / s['acknowledged'],
                    row_waits_per_ack=sum(r['stats']['plock_waits'] for r in s['deltas']) / s['acknowledged'],
                    commit_waits_per_ack=sum(r['stats']['commit_waits'] for r in s['deltas']) / s['acknowledged'],
                    tx_cpu_seconds_per_ack=sum(s['cpu_after'][pid]['tx_ticks'] - before['tx_ticks']
                        for pid, before in s['cpu_before'].items()) / session['cpu_tick_hz'] / s['acknowledged'])
            e, d = (controls[variant] for variant in variants)
            pairs.append(dict(pair=pair, controls=controls, goodput_ratio=d['goodput']/e['goodput'],
                              lua_allocation_ratio=d['lua_allocated_per_ack']/e['lua_allocated_per_ack'],
                              tx_cpu_ratio=d['tx_cpu_seconds_per_ack']/e['tx_cpu_seconds_per_ack'],
                              p99_ratio=d['latency']['ok']['p99']/e['latency']['ok']['p99']))
        summaries.append(dict(shape=shape, pairs=pairs,
                              median_goodput_ratio=statistics.median(p['goodput_ratio'] for p in pairs),
                              goodput_ratio_range=[min(p['goodput_ratio'] for p in pairs),
                                                   max(p['goodput_ratio'] for p in pairs)]))
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bin', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--engine', choices=('memtx', 'vinyl'), required=True)
    parser.add_argument('--native', choices=('enabled', 'disabled'), required=True)
    parser.add_argument('--driver', choices=('sql', 'direct'), default='sql')
    parser.add_argument('--hypothesis', choices=tuple(VARIANT_ENV), default='guards')
    parser.add_argument('--seconds', type=float, default=15)
    parser.add_argument('--base-port', type=int, default=17300)
    parser.add_argument('--sql-port', type=int, default=17400)
    args = parser.parse_args()
    sample = sql_sample if args.driver == 'sql' else rc_direct.sample
    assert args.seconds == 15
    args.bin = args.bin.resolve()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    # Keep live fixtures under the artifact root even if the process is killed
    # before finally can archive them. On normal exit replace them with gzip.
    work = args.out / 'work'
    work.mkdir()
    variants = VARIANTS
    paths = sources(work, args.hypothesis)
    native = args.bin.with_name('libtarn_storage.so')
    env = dict(os.environ, TARN_BIN=str(args.bin), TARN_NATIVE=str(native) if args.native == 'enabled' else '0',
               TARN_VINYL_CACHE_MB='0', TARN_CHECK_LOCKS='', TARN_CHECK_YIELDS='')
    receipt = dict(schema=2, complete=False, sessions=[], scope='matched checks-off RF3/fsync cost pilot',
                   hypothesis=args.hypothesis, variants=variants,
                   variant_environment=VARIANT_ENV[args.hypothesis],
                   driver=args.driver,
                   measured_seconds=args.seconds, warmup_seconds=3,
                   source=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                   source_dirty=bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True)),
                   binary_sha256=sha(args.bin), native_sha256=sha(native), harness_sha256=sha(__file__),
                   worker_sha256=sha(ROOT / 'bench/rc_profile.py'),
                   transformer_sha256=sha(ROOT / 'bench/rc_guard_candidate.py'),
                   probe_sha256=sha(ROOT / 'bench/rc_guard_probe.lua'), work_directory=str(work),
                   direct_adapter_sha256=sha(ROOT / 'bench/rc_direct.py'),
                   direct_driver_sha256=sha(ROOT / 'bench/rc_direct.lua'),
                   host=platform.platform(), engine=args.engine, native=args.native,
                   environment={k: env[k] for k in ('TARN_NATIVE', 'TARN_VINYL_CACHE_MB',
                                                     'TARN_CHECK_LOCKS', 'TARN_CHECK_YIELDS')},
                   lua_sha256={variant: sha(path/'lua/tarn.lua') for variant, path in paths.items()},
                   lua_file_sha256={variant: {str(p.relative_to(path)): sha(p) for p in sorted((path/'lua').rglob('*.lua'))}
                                    for variant, path in paths.items()},
                   limitations=f'Single host, closed-loop, {args.seconds:g}-second samples. Fresh fixture/process/JIT per lane. '
                   'No engine/WAL CPU attribution or full concurrent/fault qualification. No production default change.')
    output = args.out / 'comparison.json'
    active = None
    try:
        for pair in range(5):
            for variant in (variants if pair % 2 == 0 else tuple(reversed(variants))):
                cluster = work / f'cluster-{pair}-{variant}'
                script = paths[variant] / 'scripts/cluster.sh'
                active = (script, cluster)
                stem = args.out / f'pair-{pair}-{variant}'
                with stem.with_suffix('.startup.log').open('w') as log:
                    subprocess.run(['bash', str(script), 'start', '-d', str(cluster), '-n', '2', '-r', '3',
                                    '-w', 'fsync', '-b', str(args.base_port), '-p', str(args.sql_port),
                                    '-s', '1', '-m', '256', '-e', args.engine],
                                   env=dict(env, **VARIANT_ENV[args.hypothesis][variant]),
                                   stdout=log, stderr=subprocess.STDOUT, check=True, timeout=180)
                config = json.loads((cluster/'cluster.json').read_text())
                endpoints = [r for s in config['shards'] for r in s['replicas']]
                session = dict(pair=pair, variant=variant, complete=False, samples=[],
                               cpu_tick_hz=os.sysconf('SC_CLK_TCK'), configuration=config,
                               parameters=(cluster/'params').read_text(), lua_sha256=receipt['lua_sha256'][variant])
                receipt['sessions'].append(session)
                job = argparse.Namespace(sql=f'127.0.0.1:{args.sql_port}', seconds=args.seconds,
                                         engine=args.engine, native=args.native)
                setup(job)
                session_output = stem.with_suffix('.json')
                for shape in SHAPES:
                    warm = argparse.Namespace(**vars(job))
                    warm.seconds = 3
                    sample(warm, len(session['samples']) + 1, shape, False, endpoints,
                           session_output, session, warmup=True)
                for shape in SHAPES if pair % 2 == 0 else reversed(SHAPES):
                    sample(job, len(session['samples']) + 1, shape, False, endpoints, session_output, session)
                # Mechanism probes run after this lane's timing, so the injected
                # hold and helper census cannot precondition a measured fixture.
                if args.hypothesis.startswith('lock-barrier'):
                    session['mechanism'] = barrier_mechanism(endpoints, args.hypothesis, variant)
                elif pair == 0:
                    probe_name = 'rc_guard_probe.lua'
                    probes = evaluate(config['shards'][0]['replicas'], 'return {ro=box.info.ro}')
                    leader = next(r['address'] for r in probes if not r['result']['ro'])
                    with stem.with_suffix('.mechanism.log').open('w') as log:
                        subprocess.run(['tarantool', str(ROOT/'bench'/probe_name), leader, variant],
                                       env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=120)
                session['complete'] = True
                session_output.write_text(json.dumps(session, indent=2) + '\n')
                output.write_text(json.dumps(receipt, indent=2) + '\n')
                stop(script, cluster, env)
                active = None
        receipt['summary'] = summarize(receipt['sessions'], variants)
        receipt['complete'] = True
        # (the job log carries the result as well as the artifact)
        for row in receipt['summary']:
            pairs = row['pairs']
            print(json.dumps(dict(shape=row['shape'], median_goodput_ratio=round(row['median_goodput_ratio'], 3),
                                  goodput_ratio_range=[round(x, 3) for x in row['goodput_ratio_range']],
                                  median_p99_ratio=round(statistics.median(p['p99_ratio'] for p in pairs), 3),
                                  median_tx_cpu_ratio=round(statistics.median(p['tx_cpu_ratio'] for p in pairs), 3),
                                  goodput={v: [round(p['controls'][v]['goodput'], 1) for p in pairs]
                                           for v in variants})), flush=True)
    except BaseException as error:
        receipt['failure'] = repr(error)
        raise
    finally:
        output.write_text(json.dumps(receipt, indent=2) + '\n')
        if active:
            try:
                stop(active[0], active[1], env)
            except Exception as error:
                receipt['cleanup_failure'] = repr(error)
                output.write_text(json.dumps(receipt, indent=2) + '\n')
        # Keep exact modified sources, startup commands and storage state even
        # when an attempt or diagnostic failed. Never restart/replay a sample.
        shutil.make_archive(str(args.out/'fixtures'), 'gztar', work)
        shutil.rmtree(work)


if __name__ == '__main__':
    main()
