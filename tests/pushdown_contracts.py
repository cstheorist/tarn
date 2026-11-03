#!/usr/bin/env python3
"""Run wire contracts with an owned ordinary gateway over the same fixture."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pymysql


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cluster-dir', type=Path, required=True)
    parser.add_argument('--bin', type=Path, required=True)
    parser.add_argument('--on', default='127.0.0.1:4000')
    parser.add_argument('--off-port', type=int, default=4502)
    parser.add_argument('--ref', default='127.0.0.1:3407')
    parser.add_argument('--mysql', default='127.0.0.1:3408')
    parser.add_argument('--engine', choices=['memtx', 'vinyl'], required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--observe-only', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    args.out.mkdir(parents=True, exist_ok=True)
    config = json.loads((args.cluster_dir / 'cluster.json').read_text())
    if config.get('query_pushdown') is not True:
        raise ValueError('the existing gateway fixture must enable query_pushdown')
    ordinary = dict(config, query_pushdown=False)
    config_path = args.out / 'ordinary-config.json'
    config_path.write_text(json.dumps(ordinary, indent=2) + '\n')
    source = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    receipt = {'source': source, 'source_dirty': bool(subprocess.check_output(
        ['git', 'status', '--porcelain'], cwd=root, text=True)),
        'binary_sha256': digest(args.bin), 'runner_sha256': digest(__file__),
        'probe_sha256': digest(root / 'tests/sql/pushdown_contracts.py'),
        'fixture_config': config, 'ordinary_config': ordinary,
        'on': args.on, 'off_port': args.off_port, 'engine': args.engine,
        'complete': False}
    process = None
    result = 1
    try:
        with (args.out / 'ordinary-gateway.log').open('w') as log:
            process = subprocess.Popen([
                str(args.bin.resolve()), '--config', str(config_path.resolve()),
                '--listen', f'127.0.0.1:{args.off_port}', '--node-id', '1352',
                '--default-engine', args.engine, '--isolation', 'SERIALIZABLE',
            ], stdout=log, stderr=subprocess.STDOUT,
                env=dict(os.environ, TARN_LOG='warn'))
            receipt['owned_pid'] = process.pid
            deadline = time.monotonic() + 30
            while True:
                if process.poll() is not None:
                    raise RuntimeError('ordinary gateway exited before readiness')
                try:
                    connection = pymysql.connect(host='127.0.0.1', port=args.off_port,
                                                 user='sb', password='', autocommit=True,
                                                 connect_timeout=1, read_timeout=2)
                    try:
                        with connection.cursor() as cursor:
                            cursor.execute('SELECT 1')
                            assert cursor.fetchone() == (1,)
                    finally:
                        connection.close()
                    break
                except pymysql.MySQLError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('ordinary gateway did not become ready')
                    time.sleep(.1)
            command = [sys.executable, str(root / 'tests/sql/pushdown_contracts.py'),
                       '--off', f'127.0.0.1:{args.off_port}', '--on', args.on,
                       '--ref', args.ref, '--mysql', args.mysql, '--mysql-profile',
                       '--engine', args.engine, '--out', str(args.out / 'wire.json')]
            if args.observe_only:
                command.append('--observe-only')
            result = subprocess.run(command, timeout=600, check=False).returncode
            receipt['probe_exit'] = result
            receipt['complete'] = True
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        (args.out / 'inputs.json').write_text(json.dumps(receipt, indent=2) + '\n')
    return result


if __name__ == '__main__':
    sys.exit(main())
