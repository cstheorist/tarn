#!/usr/bin/env python3
"""Isolated RF3 pilot for a condition-allocation hypothesis, not production code.

Copy the fixture sources into an owned temporary directory and alter only
condition creation in the candidate copy. Normal release binaries, admission,
durability and query histories remain matched. Stop on any failed/unknown
attempt; keep both original storage state and partial measurements.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import tempfile
import time

from rc_profile import DB, connect, query, sample, sha

ROOT = Path(__file__).resolve().parents[1]
SHAPES = ('uncontended', 'unrelated', 'hot')


def sources(work):
    paths = {}
    for variant in ('eager', 'deferred'):
        root = work / variant
        for folder in ('lua', 'scripts'):
            shutil.copytree(ROOT / folder, root / folder)
        paths[variant] = root
    path = paths['deferred'] / 'lua/tarn.lua'
    original = path.read_text()
    eager = 'local req = {txid = txid, mode = mode, entry = e, cond = fiber.cond(), held = held}'
    queue = '    if cur ~= nil then table.insert(e.queue, 1, req) else e.queue[#e.queue + 1] = req end'
    assert original.count(eager) == original.count(queue) == 1, 'prototype no longer matches source'
    candidate = original.replace(eager, 'local req = {txid = txid, mode = mode, entry = e, held = held}')
    candidate = candidate.replace(queue, '    req.cond = fiber.cond()\n' + queue)
    path.write_text(candidate)
    return paths


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


def summarize(sessions):
    summaries = []
    for shape in SHAPES:
        pairs = []
        for pair in range(5):
            controls = {}
            for variant in ('eager', 'deferred'):
                session = next(s for s in sessions if s['pair'] == pair and s['variant'] == variant)
                s = next(s for s in session['samples'] if s['shape'] == shape and not s['warmup'])
                assert s['complete'] and not s['profiled']
                controls[variant] = dict(
                    goodput=s['goodput_per_second'], latency=s['latency'],
                    acknowledged=s['acknowledged'], refused=s['refused'],
                    lua_allocated_per_ack=sum(r['metrics']['gc_allocated'] for r in s['deltas']) / s['acknowledged'],
                    tx_cpu_seconds_per_ack=sum(s['cpu_after'][pid]['tx_ticks'] - before['tx_ticks']
                        for pid, before in s['cpu_before'].items()) / session['cpu_tick_hz'] / s['acknowledged'])
            e, d = controls['eager'], controls['deferred']
            pairs.append(dict(pair=pair, controls=controls, goodput_ratio=d['goodput']/e['goodput'],
                              lua_allocation_ratio=d['lua_allocated_per_ack']/e['lua_allocated_per_ack']))
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
    parser.add_argument('--seconds', type=float, default=5)
    args = parser.parse_args()
    assert 5 <= args.seconds <= 15
    args.bin = args.bin.resolve()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    work = Path(tempfile.mkdtemp(prefix='rc186-paired-'))
    paths = sources(work)
    native = args.bin.with_name('libtarn_storage.so')
    env = dict(os.environ, TARN_BIN=str(args.bin), TARN_NATIVE=str(native) if args.native == 'enabled' else '0',
               TARN_VINYL_CACHE_MB='0', TARN_CHECK_LOCKS='0', TARN_CHECK_YIELDS='0')
    receipt = dict(schema=1, complete=False, sessions=[], scope='isolated RF3/fsync allocation pilot',
                   source=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                   binary_sha256=sha(args.bin), native_sha256=sha(native), harness_sha256=sha(__file__),
                   worker_sha256=sha(ROOT / 'bench/rc_profile.py'), work_directory=str(work),
                   host=platform.platform(), engine=args.engine, native=args.native,
                   environment={k: env[k] for k in ('TARN_NATIVE', 'TARN_VINYL_CACHE_MB',
                                                     'TARN_CHECK_LOCKS', 'TARN_CHECK_YIELDS')},
                   lua_sha256={variant: sha(path/'lua/tarn.lua') for variant, path in paths.items()},
                   limitations='Single host, closed-loop, five-second samples. Fresh fixture/process/JIT per lane. '
                   'No engine/WAL CPU attribution or full concurrent/fault qualification. No production default change.')
    output = args.out / 'comparison.json'
    active = None
    try:
        for pair in range(5):
            for variant in (('eager', 'deferred') if pair % 2 == 0 else ('deferred', 'eager')):
                cluster = work / f'cluster-{pair}-{variant}'
                script = paths[variant] / 'scripts/cluster.sh'
                active = (script, cluster)
                stem = args.out / f'pair-{pair}-{variant}'
                with stem.with_suffix('.startup.log').open('w') as log:
                    subprocess.run(['bash', str(script), 'start', '-d', str(cluster), '-n', '2', '-r', '3',
                                    '-w', 'fsync', '-p', '4000', '-s', '1', '-m', '256', '-e', args.engine],
                                   env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=180)
                config = json.loads((cluster/'cluster.json').read_text())
                endpoints = [r for s in config['shards'] for r in s['replicas']]
                session = dict(pair=pair, variant=variant, complete=False, samples=[],
                               cpu_tick_hz=os.sysconf('SC_CLK_TCK'), configuration=config,
                               parameters=(cluster/'params').read_text(), lua_sha256=receipt['lua_sha256'][variant])
                receipt['sessions'].append(session)
                job = argparse.Namespace(sql='127.0.0.1:4000', seconds=args.seconds,
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
                session['complete'] = True
                session_output.write_text(json.dumps(session, indent=2) + '\n')
                output.write_text(json.dumps(receipt, indent=2) + '\n')
                stop(script, cluster, env)
                active = None
        receipt['summary'] = summarize(receipt['sessions'])
        receipt['complete'] = True
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


if __name__ == '__main__':
    main()
