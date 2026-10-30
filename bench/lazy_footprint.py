#!/usr/bin/env python3
"""Run bench/lazy_footprint.lua and summarize it (issue #177).

  python3 bench/lazy_footprint.py run --before LUA_DIR [--rows 10000,100000,1000000]
  python3 bench/lazy_footprint.py table [RESULTS]

`run` measures this checkout's Lua module ("after") and, with --before, another
release's lua/ directory ("before"), on memtx and vinyl, at each row count;
it writes bench/results_lazy_footprint.json. `table` prints the Markdown
tables of docs/lazy-indexes.md, "Memory and disk", from those results.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, 'results_lazy_footprint.json')


def run(args):
    tarantool = os.environ.get('TARANTOOL', 'tarantool')
    codes = [('after', os.path.join(HERE, '..', 'lua'))]
    if args.before:
        codes.insert(0, ('before', os.path.abspath(args.before)))
    out = []
    for rows in [int(r) for r in args.rows.split(',')]:
        for engine in ('memtx', 'vinyl'):
            for code, lua in codes:
                work = tempfile.mkdtemp(prefix='tarn-footprint-')
                try:
                    p = subprocess.run([tarantool, os.path.join(HERE, 'lazy_footprint.lua'), work, engine,
                                        str(rows), os.path.abspath(lua)],
                                       stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=3600)
                finally:
                    shutil.rmtree(work, ignore_errors=True)
                if p.returncode != 0:
                    sys.exit(f'{code} {engine} {rows}: exit {p.returncode}\n{p.stdout[-2000:]}{p.stderr[-2000:]}')
                out.append({'code': code, 'result': json.loads(p.stdout.strip().splitlines()[-1])})
                print(code, engine, rows, 'done', file=sys.stderr)
    with open(RESULTS, 'w') as f:
        json.dump(out, f, indent=1)
        f.write('\n')


def load(path):
    with open(path) as f:
        text = f.read().strip()
    if text.startswith('['):
        return json.loads(text)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def phases(r):
    return {p['phase']: p for p in r['result']['phases']}


def mem(s):
    """Resident bytes of a measured space: memtx tuples and index, vinyl memory."""
    return s.get('data', 0) + s.get('index', 0) + (s.get('memory', 0) if s.get('engine') == 'vinyl' else 0)


def table(args):
    rs = load(args.results)
    by = {(r['code'], r['result']['engine'], r['result']['rows']): r for r in rs}
    sizes = sorted({r['result']['rows'] for r in rs})
    print('Per-row version ledger (`_tarn_lazy_version`, memtx), after loading the rows:\n')
    print('| Rows | Table | Entries before | Ledger bytes before | Bytes per row | Entries after '
          '| memtx data + index, before → after | Process RSS, before → after |')
    print('|---:|---|---:|---:|---:|---:|---|---|')
    for rows in sizes:
        for engine in ('memtx', 'vinyl'):
            b, a = by.get(('before', engine, rows)), by.get(('after', engine, rows))
            if b is None or a is None:
                continue
            lb, la = phases(b)['loaded'], phases(a)['loaded']
            v = lb['_tarn_lazy_version']
            # The ledger's whole cost: tuple allocator and index memory, before minus after.
            total = (lb['items_used'] - la['items_used']) + (lb['memory']['index'] - la['memory']['index'])
            before = lb['memory']['data'] + lb['memory']['index']
            after = la['memory']['data'] + la['memory']['index']
            print(f"| {rows:,} | {engine} | {v['tuples']:,} | {total:,} | {total / rows:.0f} | "
                  f"{la['_tarn_lazy_version']['tuples']:,} | {before / 2**20:.1f} MiB → {after / 2**20:.1f} MiB "
                  f"| {lb['rss'] / 2**20:.0f} MiB → {la['rss'] / 2**20:.0f} MiB |")
    top = max(sizes)
    print(f'\nWhat stays, per item, {top:,} rows (this branch; tuple payload and index, '
          'without the allocator\'s per-tuple overhead):\n')
    print('| Structure | Kept | memtx table | vinyl table |')
    print('|---|---|---:|---:|')
    rows_def = []
    for engine in ('memtx', 'vinyl'):
        a = by.get(('after', engine, top))
        if a is None:
            rows_def.append(None)
            continue
        p = phases(a)
        L, T, C, O, R, K = (p['loaded'], p['tombstones'], p['collected'], p['outbox'], p['reserved'], p['committed'])
        d = {}
        t = L['target']
        if engine == 'memtx':
            d['marker'] = f"{(t['data'] + t['index']) / t['live']:.0f} B"
            dead = T['target']['tombstones']
            d['tomb'] = f"{(T['target']['data'] - C['target']['data']) / max(dead, 1) + t['index'] / t['live']:.0f} B"
        else:
            d['marker'] = f"{t['disk'] / t['live']:.0f} B on disk"
            d['tomb'] = 'on disk'
        f = C['_tarn_lazy_floor']
        d['floor'] = f"{(f['data'] + f['index']) / max(f['tuples'], 1):.0f} B ({f['tuples']:,} after {a['result']['deletes']:,} deletes)"
        c = L['_tarn_lazy_counter']
        d['counter'] = f"{(c['data'] + c['index']) / max(c['tuples'], 1):.0f} B ({c['tuples']:,})"
        o = O['_tarn_lazy_outbox']
        d['outbox'] = f"{o['data'] / max(o['tuples'], 1):.0f} B, {O['outbox_encoded'] / max(o['tuples'], 1):.0f} B encoded"
        rv = R['_tarn_lazy_reservation']
        d['reservation'] = f"{rv['data'] / max(rv['tuples'], 1):.0f} B"
        part, txn = K['_tarn_part'], K['_tarn_txn']
        d['records'] = f"{(part['data'] + txn['data']) / max(part['tuples'], 1):.0f} B"
        d['rss'] = f"{L['rss'] / 2**20:.0f} MiB"
        rows_def.append(d)

    def row(label, kept, key):
        cells = [x[key] if x else '' for x in rows_def]
        print(f'| {label} | {kept} | {cells[0]} | {cells[1]} |')
    row('Target marker', 'one per live row and lazy index, in the table\'s engine', 'marker')
    row('Tombstone', 'one per removed membership, until its floor collects it', 'tomb')
    row('Floor (`_tarn_lazy_floor`)', 'one per incarnation, target bucket and source bucket that collected a tombstone; never deleted', 'floor')
    row('Counter (`_tarn_lazy_counter`)', 'one per source bucket with a captured write', 'counter')
    row('Outbox assignment', 'one per pending membership change, capped by `lazy_outbox_entries` and `lazy_outbox_bytes`', 'outbox')
    row('Reservation', 'one per row a prepared transaction writes', 'reservation')
    row('Protocol records (`_tarn_part`, `_tarn_txn`)', 'per cross-shard transaction, until expiry', 'records')
    row('Process RSS after loading', '', 'rss')


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    r = sub.add_parser('run')
    r.add_argument('--before', help="another release's lua/ directory")
    r.add_argument('--rows', default='10000,100000,1000000')
    t = sub.add_parser('table')
    t.add_argument('results', nargs='?', default=RESULTS)
    a = ap.parse_args()
    run(a) if a.cmd == 'run' else table(a)


if __name__ == '__main__':
    main()
