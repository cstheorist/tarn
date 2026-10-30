#!/usr/bin/env python3
"""Source-bound RF3 study. Instrumented bytes and direct timing are separate.

Run on an isolated, idle RF3/fsync cluster. Never pool different widths,
engines, sources or measurement phases. A completed study need not establish
a speedup: every paired ratio, including regressions, is retained.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

import pymysql
from pushdown import Proxy, cpu, digest, sha


def query(c, sql):
    with c.cursor() as q:
        q.execute(sql)
        return q.fetchall()


def metrics(c):
    return {k: int(v) for k, v in query(c, "SHOW GLOBAL STATUS LIKE 'Tarn_storage_%'")
            if k in ('Tarn_storage_reductions', 'Tarn_storage_projections')}


def qualify(before, after, enabled, path):
    delta = {k: after[k] - before[k] for k in before}
    for metric, shape in [('Tarn_storage_reductions', 'reduce'),
                          ('Tarn_storage_projections', 'project')]:
        assert (delta[metric] > 0) == (enabled and path == shape), (path, enabled, delta)
    return delta


def summarize(samples):
    result = []
    for phase, name in sorted({(s['phase'], s['query']) for s in samples}):
        rows = [s for s in samples if s['phase'] == phase and s['query'] == name]
        pairs = sorted({s['pair'] for s in rows})
        ratios = []
        byte_ratios = []
        for pair in pairs:
            off, on = [next(s for s in rows if s['pair'] == pair and s['pushdown'] == enabled)
                       for enabled in (False, True)]
            ratios.append(sum(on['latency_seconds']) / sum(off['latency_seconds']))
            if phase == 'instrumented':
                byte_ratios.append(on['response_bytes'] / off['response_bytes'])
        result.append(dict(phase=phase, query=name, pairs=len(pairs),
                           paired_elapsed_ratios=ratios,
                           median_elapsed_ratio=statistics.median(ratios),
                           elapsed_ratio_range=[min(ratios), max(ratios)],
                           all_observed_pairs_faster=max(ratios) < 1,
                           paired_response_byte_ratios=byte_ratios))
    return result


def runtime(config, engine, native):
    """Inspect actual leaders, durability and native mode, through fixture auth."""
    endpoints = [r for s in config['shards'] for r in s['replicas']]
    code = """
local json, net=require('json'),require('net.box')
local out={}
for _,address in ipairs(json.decode(os.getenv('TARN_PD_ENDPOINTS'))) do
  local c=net.connect('tarn:tarn@'..address,{wait_connected=5})
  local r=c:eval([[return {info=tarn_info(),wal_mode=box.cfg.wal_mode,
    quorum=box.cfg.replication_synchro_quorum,engine=box.space['bench135.p'].engine,
    rows=box.space['bench135.p']:count()}]])
  r.address=address; out[#out+1]=r; c:close()
end
print(json.encode(out)); os.exit(0)
"""
    rows = json.loads(subprocess.check_output(['tarantool', '-e', code], text=True,
                                             env=dict(os.environ, TARN_PD_ENDPOINTS=json.dumps(endpoints))))
    for shard in config['shards']:
        leaders = [r for r in rows if r['address'] in shard['replicas'] and not r['info']['ro']]
        assert len(leaders) == 1, leaders
        for r in leaders:
            assert r['engine'] == engine and r['wal_mode'] == 'fsync', r
            assert r['rows'] > 0, r
            if native == 'enabled':
                assert isinstance(r['info']['native'], dict) and r['info']['native']['ready'] == 1, r
            else:
                assert r['info']['native'] is False, r
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cluster-dir', required=True)
    ap.add_argument('--bin', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--engine', choices=['memtx', 'vinyl'], required=True)
    ap.add_argument('--native', choices=['enabled', 'disabled'], required=True)
    ap.add_argument('--wide-bytes', type=int, choices=[256, 4096, 65536], required=True)
    ap.add_argument('--rows', type=int, default=1024)
    ap.add_argument('--pairs', type=int, default=5)
    ap.add_argument('--iterations', type=int, default=10)
    args = ap.parse_args()
    assert args.pairs >= 5 and 128 <= args.rows <= 1024 and args.iterations > 0
    cluster, output = Path(args.cluster_dir), Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    config = json.loads((cluster / 'cluster.json').read_text())
    assert len(config['shards']) == 2 and all(len(s['replicas']) == 3 for s in config['shards'])
    storage = [int(p.read_text()) for p in sorted(cluster.glob('s*.pid'))]
    assert len(storage) == 6
    evidence = dict(schema=1, complete=False, samples=[], configurations={},
                    source=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                    source_dirty=bool(subprocess.check_output(['git', 'status', '--porcelain'], text=True)),
                    binary_sha256=sha(args.bin), native_sha256=sha(str(Path(args.bin).with_name('libtarn_storage.so'))),
                    harness_sha256=sha(__file__), proxy_sha256=sha(Path(__file__).with_name('pushdown.py')),
                    cargo_lock_sha256=sha('Cargo.lock'), host=platform.platform(),
                    cpu_count=os.cpu_count(), cpu_tick_hz=os.sysconf('SC_CLK_TCK'),
                    engine=args.engine, native=args.native, replication_factor=3,
                    storage_parameters=(cluster / 'params').read_text(),
                    fixture=dict(rows=args.rows, payload_bytes=args.wide_bytes, tenants=16),
                    pairs=args.pairs, iterations=args.iterations,
                    limitations='Shared CI host; closed-loop single client. CPU tick quantization. Instrumented latency includes copying proxies. Direct timing is separate. Payload width excludes other columns/tuple framing. No disk-bound, throughput-capacity or general speedup claim.')
    def save():
        output.write_text(json.dumps(evidence, indent=2) + '\n')
    clients, processes = {}, []
    save()
    try:
        for phase in ('instrumented', 'direct'):
            lanes = {}
            for enabled in (False, True):
                cfg = dict(config, query_pushdown=enabled, node_id=1350 + int(enabled), pool_size=4)
                # Matched study policy admits the complete 64 KiB x 1024-row
                # ordinary control; neither path may silently clip or skip it.
                cfg['resources'] = dict(query_bytes=512 << 20, server_bytes=2 << 30,
                                        query_work=1_000_000_000, server_work=4_000_000_000)
                proxies = []
                if phase == 'instrumented':
                    shards = []
                    for i, shard in enumerate(config['shards']):
                        replicas = []
                        for j, upstream in enumerate(shard['replicas']):
                            port = 3800 + 100 * int(enabled) + 10 * i + j
                            proxies.append(Proxy(port, int(upstream.rsplit(':', 1)[1])))
                            replicas.append(f'127.0.0.1:{port}')
                        shards.append(dict(id=shard['id'], replicas=replicas))
                    cfg['shards'] = shards
                stem = output.with_suffix(f'.{phase}.{int(enabled)}')
                path = Path(str(stem) + '.config.json')
                path.write_text(json.dumps(cfg))
                evidence['configurations'][f'{phase}-{enabled}'] = cfg
                with open(str(stem) + '.gateway.log', 'w') as log:
                    p = subprocess.Popen([args.bin, '--config', str(path), '--listen', f'127.0.0.1:{4500+int(enabled)}',
                                          '--threads', '1', '--isolation', 'SERIALIZABLE', '--allow-all'],
                                         stdout=log, stderr=log, env=dict(os.environ, TARN_LOG='error'))
                processes.append(p)
                for _ in range(200):
                    if p.poll() is not None:
                        raise RuntimeError('study gateway exited during startup')
                    try:
                        clients[enabled] = pymysql.connect(host='127.0.0.1', port=4500+int(enabled), user='t',
                                                         password='', autocommit=True, charset='utf8mb4')
                        break
                    except pymysql.MySQLError:
                        time.sleep(.05)
                else:
                    raise RuntimeError('study gateway did not start')
                lanes[enabled] = proxies
            if phase == 'instrumented':
                c = clients[False]
                query(c, 'DROP DATABASE IF EXISTS bench135')
                query(c, 'CREATE DATABASE bench135')
                query(c, 'CREATE TABLE bench135.p (tenant INT, id INT, n BIGINT, u BIGINT UNSIGNED, '
                         'tag INT, wide MEDIUMTEXT, PRIMARY KEY(tenant,id), KEY tn(tenant,n)) '
                         f'ENGINE={args.engine} PARTITION BY KEY(tenant)')
                # At most ~1 MiB/INSERT, well below packet/admission limits.
                batch = max(1, min(100, 1024*1024 // args.wide_bytes))
                with c.cursor() as q:
                    for first in range(1, args.rows+1, batch):
                        q.executemany('INSERT INTO bench135.p VALUES (%s,%s,%s,%s,%s,%s)', [
                            (i % 16, i, None if i % 3 == 0 else i % 11 - 5,
                             9007199254740993 if i % 2 else 18446744073709551615,
                             int(i % 128 == 0), 'x' * args.wide_bytes)
                            for i in range(first, min(first+batch, args.rows+1))])
                evidence['runtime_before'] = runtime(config, args.engine, args.native)
            queries = {}
            agg = 'SUM(n), MIN(n), MAX(n), SUM(u), COUNT(n)'
            for selectivity, pred in [('low', 'tag=1'), ('high', 'tag=0')]:
                queries[f'reduce_{selectivity}'] = (f'SELECT {agg} FROM bench135.p WHERE {pred}', 'reduce')
                queries[f'narrow_{selectivity}'] = (f'SELECT tenant,id,n FROM bench135.p WHERE {pred}', 'project')
                queries[f'full_{selectivity}'] = (f'SELECT * FROM bench135.p WHERE {pred}', 'fallback')
            queries['range_reduce'] = (f'SELECT {agg} FROM bench135.p WHERE tenant=1 AND n BETWEEN -4 AND 4', 'reduce')
            queries['grouped_fallback'] = ('SELECT tag,SUM(n),MIN(n),MAX(n) FROM bench135.p GROUP BY tag', 'fallback')
            evidence['queries'] = queries
            expected = evidence.setdefault('expected_digests', {})
            for name, (sql, path) in queries.items():
                for enabled in (False, True):
                    for _ in range(3):
                        rows = query(clients[enabled], sql)
                        assert expected.setdefault(name, digest(rows)) == digest(rows), name
                for pair in range(args.pairs):
                    for enabled in ((False, True) if pair % 2 == 0 else (True, False)):
                        c = clients[enabled]
                        before = metrics(c)
                        counters = [p.snapshot() for p in lanes[enabled]]
                        scpu, gcpu = sum(cpu(p) for p in storage), cpu(processes[int(enabled)].pid)
                        elapsed = []
                        for _ in range(args.iterations):
                            start = time.perf_counter()
                            rows = query(c, sql)
                            elapsed.append(time.perf_counter() - start)
                            assert digest(rows) == expected[name], name
                        sample = dict(phase=phase, query=name, path=path, pair=pair, pushdown=enabled,
                                      iterations=args.iterations, latency_seconds=elapsed,
                                      gateway_cpu_seconds=cpu(processes[int(enabled)].pid)-gcpu,
                                      storage_cpu_seconds=sum(cpu(p) for p in storage)-scpu,
                                      result_digest=expected[name], returned_rows=len(rows),
                                      returned_value_bytes=sum(len(str(v).encode()) for row in rows for v in row if v is not None))
                        for key in counters[0] if counters else []:
                            sample[key] = sum(p.snapshot()[key]-b[key] for p,b in zip(lanes[enabled],counters))
                        # Preserve the raw sample even when qualification fails.
                        evidence['samples'].append(sample)
                        save()
                        sample['path_counters'] = qualify(before, metrics(c), enabled, path)
                        save()
                        print(phase, name, pair, enabled, round(sum(elapsed), 3), flush=True)
            for c in clients.values():
                c.close()
            clients.clear()
            for p in processes:
                p.terminate()
                p.wait(timeout=10)
            processes.clear()
            for proxies in lanes.values():
                for p in proxies:
                    p.socket.close()
        evidence['runtime_after'] = runtime(config, args.engine, args.native)
        evidence['summary'] = summarize(evidence['samples'])
        evidence['complete'] = True
    except BaseException as e:
        evidence['error'] = repr(e)
        raise
    finally:
        save()
        for c in clients.values():
            c.close()
        for p in processes:
            p.terminate()
        for p in processes:
            p.wait(timeout=10)


if __name__ == '__main__':
    main()
