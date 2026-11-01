#!/usr/bin/env python3
"""Matched read-path study, run inside Linux beside an idle RF1 cluster.
Two gateways use the same binary/storage/fixture/isolation, differing only in
query_pushdown. TCP proxies count actual IPROTO bytes/frames per gateway.
Linux process CPU is measured independently for gateway and storage.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import statistics
import subprocess
import threading
import time
import pymysql


class Proxy:
    def __init__(self, port, upstream):
        self.upstream = upstream
        self.lock = threading.Lock()
        self.counts = dict(request_bytes=0, response_bytes=0, requests=0, responses=0)
        for fn in ['tarn_reduce', 'tarn_select_projected', 'tarn_select', 'tarn_storage.ts_select']:
            self.counts['calls_' + fn] = 0
        self.socket = socket.create_server(('127.0.0.1', port))
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while True:
            try:
                c, _ = self.socket.accept()
            except OSError:
                return
            s = socket.create_connection(('127.0.0.1', self.upstream))
            for src, dst, sending in [(c, s, True), (s, c, False)]:
                threading.Thread(target=self.pump, args=(src, dst, sending), daemon=True).start()

    def pump(self, src, dst, sending):
        buf = bytearray()
        greeting = 0 if sending else 128
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                buf.extend(data)
                frames = 0
                if greeting:
                    n = min(greeting, len(buf))
                    del buf[:n]
                    greeting -= n
                while not greeting and buf:
                    tag = buf[0]
                    if tag <= 127:
                        prefix, length = 1, tag
                    else:
                        width = {0xcc: 1, 0xcd: 2, 0xce: 4, 0xcf: 8}[tag]
                        if len(buf) < width + 1:
                            break
                        prefix = width + 1
                        length = int.from_bytes(buf[1:prefix], 'big')
                    if len(buf) < prefix + length:
                        break
                    if sending:
                        frame = buf[prefix:prefix + length]
                        with self.lock:
                            for fn in ['tarn_reduce', 'tarn_select_projected', 'tarn_select', 'tarn_storage.ts_select']:
                                marker = bytes([0xa0 + len(fn)]) + fn.encode()
                                if marker in frame:
                                    self.counts['calls_' + fn] += 1
                    del buf[:prefix + length]
                    frames += 1
                with self.lock:
                    self.counts['request_bytes' if sending else 'response_bytes'] += len(data)
                    self.counts['requests' if sending else 'responses'] += frames
                dst.sendall(data)
        except (OSError, KeyError):
            pass
        finally:
            for s in (src, dst):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                s.close()

    def snapshot(self):
        with self.lock:
            return self.counts.copy()


def connect(port):
    return pymysql.connect(host='127.0.0.1', port=port, user='t', password='', autocommit=True)


def cpu(pid):
    stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return (int(stat[11]) + int(stat[12])) / os.sysconf('SC_CLK_TCK')


def digest(rows):
    return hashlib.sha256(json.dumps(sorted(rows), default=str).encode()).hexdigest()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cluster-dir', required=True)
    ap.add_argument('--bin', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--source-base', required=True)
    ap.add_argument('--pairs', type=int, default=5)
    ap.add_argument('--iterations', type=int, default=30)
    ap.add_argument('--rows', type=int, default=12000)
    args = ap.parse_args()
    cluster = Path(args.cluster_dir)
    config = json.loads((cluster / 'cluster.json').read_text())
    storage = [int(p.read_text()) for p in sorted(cluster.glob('s*.pid'))]
    servers, clients, lanes = [], {}, {}
    evidence = dict(samples=[], host=platform.platform(), fixture=dict(rows=args.rows, wide_bytes=1024),
                    source_base=args.source_base,
                    source_files={str(p): sha(p) for folder in ['crates', 'lua', 'scripts', 'bench']
                                  for p in Path(folder).rglob('*') if p.is_file() and p.suffix in ('.rs', '.lua', '.toml', '.py', '.sh')},
                    binary_sha256=sha(args.bin), lua_sha256=sha('lua/tarn.lua'),
                    rust=subprocess.check_output(['rustc', '--version'], text=True).strip(),
                    tarantool=subprocess.check_output(['tarantool', '--version'], text=True).splitlines()[0],
                    cluster_config=config, configs={}, isolation='SERIALIZABLE', rf=1,
                    build_profile='release opt-level=3 thin-LTO codegen-units=4 debug=0',
                    limits='Shared Docker ARM64 host, 3 CPU container quota; read-only RF1 component evidence, no RF3/durable OLTP speedup claim')
    evidence['cargo_lock_sha256'] = sha('Cargo.lock')
    evidence['workspace_toml_sha256'] = sha('Cargo.toml')
    evidence['storage_parameters'] = (cluster / 'params').read_text().strip()
    evidence['native_sha256'] = sha(str(Path(args.bin).with_name('libtarn_storage.so')))
    evidence['storage_startup_sha256'] = {p.name: sha(p) for p in cluster.glob('s*.sh')}
    try:
        for mode in (False, True):
            cfg = dict(config, query_pushdown=mode, node_id=1350 + int(mode))
            proxies = []
            shards = []
            for i, shard in enumerate(config['shards']):
                hp = shard['replicas'][0]
                port = 3800 + 20 * int(mode) + i
                proxies.append(Proxy(port, int(hp.rsplit(':', 1)[1])))
                shards.append(dict(id=shard['id'], replicas=[f'127.0.0.1:{port}']))
            cfg['shards'] = shards
            evidence['configs'][str(mode)] = cfg
            path = Path(args.out).with_suffix(f'.{int(mode)}.config.json')
            path.write_text(json.dumps(cfg))
            log = open(Path(args.out).with_suffix(f'.{int(mode)}.gateway.log'), 'w')
            p = subprocess.Popen([args.bin, '--config', str(path), '--listen', f'127.0.0.1:{4500+int(mode)}',
                                  '--threads', '1', '--allow-all'], stdout=log, stderr=log,
                                 env=dict(os.environ, TARN_LOG='error'))
            servers.append(p)
            for _ in range(200):
                try:
                    clients[mode] = connect(4500 + int(mode))
                    break
                except pymysql.MySQLError:
                    time.sleep(.05)
            else:
                raise RuntimeError('gateway did not start')
            lanes[mode] = proxies
        c = clients[False]
        with c.cursor() as q:
            q.execute('DROP DATABASE IF EXISTS bench135')
            q.execute('CREATE DATABASE bench135')
            q.execute('CREATE TABLE bench135.p (tenant INT, id INT, n BIGINT, u BIGINT UNSIGNED, tag INT, wide TEXT, '
                      'PRIMARY KEY(tenant,id)) PARTITION BY KEY(tenant)')
            for first in range(1, args.rows + 1, 200):
                q.executemany('INSERT INTO bench135.p VALUES (%s,%s,%s,%s,%s,%s)', [
                    (i % 16, i, None if i % 3 == 0 else i % 11 - 5,
                     9007199254740993 if i % 2 else 18446744073709551615,
                     int(i >= args.rows - 1), 'x' * 1024)
                    for i in range(first, min(first + 200, args.rows + 1))])
        queries = dict(reduce='SELECT SUM(n), MIN(n), MAX(n), SUM(u), COUNT(n) FROM bench135.p WHERE tag = 1',
                       project='SELECT tenant,id,n FROM bench135.p WHERE tag = 1')
        expected = {}
        for name, sql in queries.items():
            for mode in (False, True):
                with clients[mode].cursor() as q:
                    for _ in range(3):
                        q.execute(sql)
                        h = digest(q.fetchall())
                        assert expected.setdefault(name, h) == h
        evidence['queries'], evidence['expected_digests'] = queries, expected
        for name, sql in queries.items():
            for pair in range(args.pairs):
                for mode in ((False, True) if pair % 2 == 0 else (True, False)):
                    before = [p.snapshot() for p in lanes[mode]]
                    scpu = sum(cpu(p) for p in storage)
                    gcpu = cpu(servers[int(mode)].pid)
                    elapsed = []
                    start = time.perf_counter()
                    with clients[mode].cursor() as q:
                        for _ in range(args.iterations):
                            t = time.perf_counter()
                            q.execute(sql)
                            rows = q.fetchall()
                            elapsed.append(time.perf_counter() - t)
                            assert digest(rows) == expected[name]
                    wall = time.perf_counter() - start
                    sample = dict(query=name, pair=pair, pushdown=mode, iterations=args.iterations,
                                  wall_seconds=wall, latency_seconds=elapsed,
                                  gateway_cpu_seconds=cpu(servers[int(mode)].pid) - gcpu,
                                  storage_cpu_seconds=sum(cpu(p) for p in storage) - scpu,
                                  result_digest=expected[name])
                    for key in before[0]:
                        sample[key] = sum(p.snapshot()[key] - b[key] for p, b in zip(lanes[mode], before))
                    active = sample['calls_tarn_reduce' if name == 'reduce' else 'calls_tarn_select_projected']
                    assert bool(active) == mode, ('pushdown qualification failed', sample)
                    evidence['samples'].append(sample)
                    Path(args.out).write_text(json.dumps(evidence, indent=2))
                    print(name, pair, mode, round(wall, 3), sample['response_bytes'], flush=True)
        for name in queries:
            for mode in (False, True):
                xs = [s for s in evidence['samples'] if s['query'] == name and s['pushdown'] == mode]
                print(name, mode, 'median ms/query', statistics.median(s['wall_seconds']/s['iterations']*1000 for s in xs))
    finally:
        for c in clients.values():
            c.close()
        for p in servers:
            p.terminate()
        for p in servers:
            p.wait(timeout=10)


if __name__ == '__main__':
    main()
