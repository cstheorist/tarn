#!/usr/bin/env python3
"""Run bench/lazy_footprint.lua and summarize it (issue #177).

  python3 bench/lazy_footprint.py study --before REV --after REV [--rows 10000,100000,1000000]
                                        [--reps 3] [--out RESULTS]
  python3 bench/lazy_footprint.py table [RESULTS]

`study` measures the Lua modules of two commits of this repository in
matched pairs: for each repetition, row count and engine it loads one
commit then the other (alternating which goes first), then restarts each
node from its files. Every run uses this checkout's bench files. It keeps
what it has measured in RESULTS.partial, and the same command resumes an
interrupted study from there. It writes
bench/results_lazy_study.json with receipts: the commits and a SHA-256 of
every module file measured, of the bench files (and whether they differ
from the checkout's commit), of the Tarantool binary with its version, and
the host's processor, memory and kernel.

`table` prints the Markdown tables of docs/lazy-indexes.md, "Memory and
disk": from the study, or from bench/results_lazy_footprint.json (archived:
single observations of an earlier version of the bench, without receipts).
"""
import argparse
import datetime
import hashlib
import io
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tarfile
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
ARCHIVED = os.path.join(HERE, 'results_lazy_footprint.json')
STUDY = os.path.join(HERE, 'results_lazy_study.json')
BENCH = [os.path.join(HERE, 'lazy_footprint.lua'), os.path.abspath(__file__)]


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def git(*args):
    return subprocess.run(['git', '-C', ROOT, *args], check=True, capture_output=True, text=True).stdout.strip()


def extract(rev, dest):
    """The lua/ directory of commit rev, and a receipt of what it holds."""
    commit = git('rev-parse', '--verify', rev + '^{commit}')
    data = subprocess.run(['git', '-C', ROOT, 'archive', '--format=tar', commit, 'lua'],
                          check=True, capture_output=True).stdout
    with tarfile.open(fileobj=io.BytesIO(data)) as t:
        t.extractall(dest, filter='data')
    lua = os.path.join(dest, 'lua')
    files = {}
    for base, _, names in os.walk(lua):
        for n in sorted(names):
            p = os.path.join(base, n)
            files[os.path.relpath(p, dest)] = sha256(p)
    digest = hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()
    return lua, {'rev': rev, 'commit': commit, 'files_sha256': digest, 'files': files}


def host():
    cpu = ''
    with open('/proc/cpuinfo') as f:
        for line in f:
            if line.startswith('model name'):
                cpu = line.split(':', 1)[1].strip()
                break
    mem = 0
    with open('/proc/meminfo') as f:
        for line in f:
            if line.startswith('MemTotal:'):
                mem = int(line.split()[1]) * 1024
    return {'cpu': cpu, 'logical_cpus': os.cpu_count(), 'memory_bytes': mem,
            'kernel': platform.system() + ' ' + platform.release(), 'machine': platform.machine()}


def tarantool_receipt(tarantool):
    path = shutil.which(tarantool) or tarantool
    version = subprocess.run([path, '--version'], check=True, capture_output=True, text=True).stdout
    return {'version': version.splitlines()[0], 'build': version.splitlines()[1:3],
            'binary_sha256': sha256(os.path.realpath(path))}


def bench_receipt():
    rel = [os.path.relpath(p, ROOT) for p in BENCH]
    dirty = git('status', '--porcelain', '--', *rel)
    return {'modified': bool(dirty), 'files': {r: sha256(p) for r, p in zip(rel, BENCH)}}


def node(tarantool, work, engine, rows, lua, mode):
    p = subprocess.run([tarantool, BENCH[0], work, engine, str(rows), lua, mode],
                       stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=4 * 3600)
    if p.returncode != 0:
        sys.exit(f'{mode} {engine} {rows} {lua}: exit {p.returncode}\n{p.stdout[-2000:]}{p.stderr[-2000:]}')
    return json.loads(p.stdout.strip().splitlines()[-1])


def study(args):
    tarantool = os.environ.get('TARANTOOL', 'tarantool')
    scratch = tempfile.mkdtemp(prefix='tarn-study-')
    try:
        sources = {}
        for code, rev in (('before', args.before), ('after', args.after)):
            lua, receipt = extract(rev, os.path.join(scratch, code))
            sources[code] = (lua, receipt)
        receipts = {'started': datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds'),
                    'sources': {c: r for c, (_, r) in sources.items()}, 'bench': bench_receipt(),
                    'tarantool': tarantool_receipt(tarantool), 'host': host(),
                    'budgets': {'memtx_memory': 3 << 30, 'vinyl_memory': 128 << 20, 'vinyl_cache': 64 << 20},
                    'reps': args.reps, 'rows': [int(r) for r in args.rows.split(',')]}
        runs = []
        partial = args.out + '.partial'
        if os.path.exists(partial):
            # Resume an interrupted study of the same sources, bench, binary
            # and host; a pair with one run only is measured again.
            with open(partial) as f:
                old = json.load(f)
            same = ('sources', 'bench', 'tarantool', 'host', 'budgets', 'reps', 'rows')
            if any(old['receipts'][k] != receipts[k] for k in same):
                sys.exit(f'{partial} is another study: remove it to start again')
            receipts['started'] = old['receipts']['started']
            done = {}
            for r in old['runs']:
                done.setdefault((r['rep'], r['rows'], r['engine']), []).append(r)
            runs = [r for pair in done.values() if len(pair) == 2 for r in pair]
        have = {(r['rep'], r['rows'], r['engine']) for r in runs}
        for rep in range(1, args.reps + 1):
            for rows in receipts['rows']:
                for engine in ('memtx', 'vinyl'):
                    if (rep, rows, engine) in have:
                        continue
                    order = ('before', 'after') if rep % 2 else ('after', 'before')
                    for i, code in enumerate(order):
                        lua = sources[code][0]
                        work = os.path.join(scratch, f'work-{code}')
                        shutil.rmtree(work, ignore_errors=True)
                        load = node(tarantool, work, engine, rows, lua, 'load')
                        restart = node(tarantool, work, engine, rows, lua, 'restart')
                        shutil.rmtree(work, ignore_errors=True)
                        for r in (load, restart):
                            r['lua'] = code   # (not the scratch path)
                        runs.append({'code': code, 'rep': rep, 'position': i + 1, 'engine': engine,
                                     'rows': rows, 'result': load, 'restart': restart})
                        print(f'rep {rep} {engine} {rows:,} {code}: {load["seconds"]:.0f} s', file=sys.stderr)
                        with open(args.out + '.partial', 'w') as f:
                            json.dump({'receipts': receipts, 'runs': runs}, f)
        receipts['finished'] = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')
        with open(args.out, 'w') as f:
            json.dump({'receipts': receipts, 'runs': runs}, f, indent=1)
            f.write('\n')
        os.remove(args.out + '.partial')
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def load(path):
    with open(path) as f:
        text = f.read().strip()
    if text.startswith('[') or text.startswith('{'):
        return json.loads(text)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def phases(r):
    return {p['phase']: p for p in r['result']['phases']}


def mem(s):
    """Resident bytes of a measured space: memtx tuples and index, vinyl memory."""
    return s.get('data', 0) + s.get('index', 0) + (s.get('memory', 0) if s.get('engine') == 'vinyl' else 0)


def mib(n):
    return f'{n / 2**20:.1f} MiB'


def spread(values, fmt):
    """Median, with the range when the repetitions differ."""
    values = sorted(values)
    m = fmt(statistics.median(values))
    lo, hi = fmt(values[0]), fmt(values[-1])
    return m if lo == hi else f'{m} ({lo} to {hi})'


def table_study(doc):
    rec, runs = doc['receipts'], doc['runs']
    src = rec['sources']
    print(f"Receipts: before `{src['before']['commit'][:12]}`, after `{src['after']['commit'][:12]}` "
          f"(module files SHA-256 `{src['before']['files_sha256'][:12]}` and "
          f"`{src['after']['files_sha256'][:12]}`); {rec['tarantool']['version']} "
          f"(binary SHA-256 `{rec['tarantool']['binary_sha256'][:12]}`); {rec['host']['cpu']}, "
          f"{rec['host']['logical_cpus']} logical CPUs, {rec['host']['memory_bytes'] / 2**30:.1f} GiB, "
          f"{rec['host']['kernel']}; {rec['reps']} matched pairs per row count and engine, "
          f"{rec['started']} to {rec['finished']}. Medians, with the range over the repetitions.\n")
    by = {}
    for r in runs:
        by.setdefault((r['engine'], r['rows'], r['code']), []).append(r)
    sizes = sorted({r['rows'] for r in runs})
    pairs = {}
    for r in runs:
        pairs.setdefault((r['engine'], r['rows'], r['rep']), {})[r['code']] = r

    print('The per-row version ledger, after loading: before minus after in matched pairs.\n')
    print('| Rows | Table | Ledger entries before → after | memtx tuple + index bytes saved | per row '
          '| Process RSS saved | WAL bytes per key change, before → after | Restart to serving, before → after '
          '| RSS after restart, before → after |')
    print('|---:|---|---|---:|---:|---:|---|---|---|')
    secs = lambda v: f'{v:.1f} s'
    for rows in sizes:
        for engine in ('memtx', 'vinyl'):
            ds, rs, entries, rb, ra, wb, wa, mb, ma = [], [], set(), [], [], [], [], [], []
            for (e, n, _), p in sorted(pairs.items()):
                if e != engine or n != rows or 'before' not in p or 'after' not in p:
                    continue
                b, a = phases(p['before'])['loaded'], phases(p['after'])['loaded']
                ds.append((b['items_used'] + b['memory']['index']) - (a['items_used'] + a['memory']['index']))
                rs.append(b['rss'] - a['rss'])
                entries.add((b['_tarn_lazy_version']['tuples'], a['_tarn_lazy_version']['tuples']))
                rb.append(p['before']['restart']['ready_seconds'])
                ra.append(p['after']['restart']['ready_seconds'])
                wb.append(p['before']['result']['writes']['wal_bytes'])
                wa.append(p['after']['result']['writes']['wal_bytes'])
                mb.append(p['before']['restart']['memory']['rss'])
                ma.append(p['after']['restart']['memory']['rss'])
            if not ds:
                continue
            e = ', '.join(f'{x:,} → {y:,}' for x, y in sorted(entries))
            print(f'| {rows:,} | {engine} | {e} | {spread(ds, mib)} | '
                  f'{spread([d / rows for d in ds], lambda v: f"{v:.0f} B")} | {spread(rs, mib)} '
                  f'| {spread(wb, lambda v: f"{v:,.0f}")} → {spread(wa, lambda v: f"{v:,.0f}")} '
                  f'| {spread(rb, secs)} → {spread(ra, secs)} | {spread(mb, mib)} → {spread(ma, mib)} |')

    print('\nPer replica, after loading (this design). memtx: tuple payload, the tuple allocator\'s '
          'overhead (headers and size classes), indexes. On disk after a checkpoint: snapshot, WAL kept '
          'for the last two checkpoints, vinyl files. After a restart, memtx MVCC holds stories for '
          'tuples recovered from the snapshot.\n')
    print('| Rows | Table | Process RSS | memtx tuples / allocator / indexes | vinyl page index + bloom '
          '| Snapshot / WAL / vinyl files | Restart to serving | RSS after restart | MVCC stories after restart |')
    print('|---:|---|---:|---|---:|---|---:|---:|---|')
    for rows in sizes:
        for engine in ('memtx', 'vinyl'):
            rs = by.get((engine, rows, 'after'))
            if not rs:
                continue
            L = [phases(r)['loaded'] for r in rs]
            F = [r['result']['final']['files'] for r in rs]
            vy = [x['vinyl']['memory']['page_index'] + x['vinyl']['memory']['bloom_filter'] for x in L]
            memtx = ' / '.join(spread(v, mib) for v in (
                [x['memtx_bsize'] for x in L],
                [x['items_used'] - x['memtx_bsize'] for x in L],
                [x['arena_used'] - x['items_used'] for x in L]))
            M = [r['restart']['memory']['mvcc_stories'] for r in rs]
            stories = spread([m['count'] for m in M], lambda v: f'{v:,.0f}') + ', ' + spread(
                [m['bytes'] for m in M], mib)
            disk = ' / '.join(spread(v, mib) for v in (
                [f.get('snap', 0) for f in F], [f.get('xlog', 0) for f in F],
                [f.get('run', 0) + f.get('index', 0) + f.get('vylog', 0) for f in F]))
            print(f"| {rows:,} | {engine} | {spread([x['rss'] for x in L], mib)} | {memtx} | {spread(vy, mib)} "
                  f"| {disk} | {spread([r['restart']['ready_seconds'] for r in rs], secs)} "
                  f"| {spread([r['restart']['memory']['rss'] for r in rs], mib)} | {stories} |")

    top = max(sizes)
    print(f'\nPer item, {top:,} rows (tuple payload and index, without the allocator\'s per-tuple overhead):\n')
    print('| Structure | Kept | memtx table | vinyl table |')
    print('|---|---|---:|---:|')
    cells = {}
    for engine in ('memtx', 'vinyl'):
        rs = by.get((engine, top, 'after'), [])
        if not rs:
            continue
        P = [phases(r) for r in rs]
        c = cells.setdefault(engine, {})
        if engine == 'memtx':
            c['base'] = spread([(p['loaded']['base']['data'] + p['loaded']['base']['index'])
                                / p['loaded']['base']['tuples'] for p in P], lambda v: f'{v:.0f} B')
        else:
            c['base'] = spread([p['loaded']['base']['disk'] / p['loaded']['base']['tuples'] for p in P],
                               lambda v: f'{v:.0f} B on disk') + ', ' + spread(
                [(p['loaded']['base']['page_index'] + p['loaded']['base']['bloom']) / p['loaded']['base']['tuples']
                 for p in P], lambda v: f'{v:.1f} B page index + bloom')
        if engine == 'memtx':
            c['marker'] = spread([(p['loaded']['target']['data'] + p['loaded']['target']['index'])
                                  / p['loaded']['target']['live'] for p in P], lambda v: f'{v:.0f} B')
            c['tomb'] = spread([(p['tombstones']['target']['data'] - p['collected']['target']['data'])
                                / max(p['tombstones']['target']['tombstones'], 1)
                                + p['loaded']['target']['index'] / p['loaded']['target']['live'] for p in P],
                               lambda v: f'{v:.0f} B')
        else:
            c['tomb'] = 'on disk, as a marker'
            c['marker'] = spread([p['loaded']['target']['disk'] / p['loaded']['target']['live'] for p in P],
                                 lambda v: f'{v:.0f} B on disk') + ', ' + spread(
                [(p['loaded']['target']['page_index'] + p['loaded']['target']['bloom']) / p['loaded']['target']['live']
                 for p in P], lambda v: f'{v:.1f} B page index + bloom')
        f = [p['collected']['_tarn_lazy_floor'] for p in P]
        c['floor'] = spread([(x['data'] + x['index']) / max(x['tuples'], 1) for x in f], lambda v: f'{v:.0f} B') \
            + f" ({spread([x['tuples'] for x in f], lambda v: f'{v:,.0f}')} after {rs[0]['result']['deletes']:,} deletes)"
        k = [p['loaded']['_tarn_lazy_counter'] for p in P]
        c['counter'] = spread([(x['data'] + x['index']) / max(x['tuples'], 1) for x in k], lambda v: f'{v:.0f} B')
        o = [p['outbox'] for p in P]
        c['outbox'] = spread([x['_tarn_lazy_outbox']['data'] / max(x['_tarn_lazy_outbox']['tuples'], 1) for x in o],
                             lambda v: f'{v:.0f} B') + ', ' + spread(
            [x['outbox_encoded'] / max(x['_tarn_lazy_outbox']['tuples'], 1) for x in o], lambda v: f'{v:.0f} B encoded')
        v = [p['reserved']['_tarn_lazy_reservation'] for p in P]
        c['reservation'] = spread([x['data'] / max(x['tuples'], 1) for x in v], lambda v: f'{v:.0f} B')
        rec_ = [p['committed'] for p in P]
        c['records'] = spread([(x['_tarn_part']['data'] + x['_tarn_txn']['data']) / max(x['_tarn_part']['tuples'], 1)
                               for x in rec_], lambda v: f'{v:.0f} B')
    for label, kept, key in (
            ('Base row, for comparison', 'the row of `t` and its three local indexes', 'base'),
            ('Target marker', 'one per live row and lazy index, in the table\'s engine', 'marker'),
            ('Tombstone', 'one per removed membership, until its floor collects it', 'tomb'),
            ('Floor (`_tarn_lazy_floor`)', 'one per incarnation, target bucket and source bucket that collected '
             'a tombstone', 'floor'),
            ('Counter (`_tarn_lazy_counter`)', 'one per source bucket with a captured write', 'counter'),
            ('Outbox assignment', 'one per pending membership change, capped by `lazy_outbox_entries` and '
             '`lazy_outbox_bytes`', 'outbox'),
            ('Reservation', 'one per row a prepared transaction writes', 'reservation'),
            ('Protocol records (`_tarn_part`, `_tarn_txn`)', 'per cross-shard transaction, until expiry', 'records')):
        print(f"| {label} | {kept} | {cells.get('memtx', {}).get(key, '')} | {cells.get('vinyl', {}).get(key, '')} |")

    print('\nLatency in microseconds (5,000 single-row key changes, one per autocommit, delivery running; '
          '5,000 reads of one key\'s first page of at most 10 rows through the lazy index):\n')
    print('| Rows | Table | Write p50 / p99 / p99.9 | WAL bytes per key change | Read p50 / p99 / p99.9 '
          '| Read after restart p50 / p99 / p99.9 |')
    print('|---:|---|---:|---:|---:|---:|')
    us = lambda v: f'{v:.0f}'
    for rows in sizes:
        for engine in ('memtx', 'vinyl'):
            rs = by.get((engine, rows, 'after'))
            if not rs:
                continue

            def pcts(get):
                return ' / '.join(spread([get(r)[q] for r in rs], us) for q in ('p50', 'p99', 'p999'))
            print(f"| {rows:,} | {engine} | {pcts(lambda r: r['result']['writes'])} "
                  f"| {spread([r['result']['writes']['wal_bytes'] for r in rs], lambda v: f'{v:,.0f}')} "
                  f"| {pcts(lambda r: r['result']['reads'])} | {pcts(lambda r: r['restart']['reads'])} |")


def table_archived(rs):
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


def table(args):
    doc = load(args.results)
    if isinstance(doc, dict):
        table_study(doc)
    else:
        table_archived(doc)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    s = sub.add_parser('study')
    s.add_argument('--before', required=True, help='commit whose lua/ is measured as "before"')
    s.add_argument('--after', required=True, help='commit whose lua/ is measured as "after"')
    s.add_argument('--rows', default='10000,100000,1000000')
    s.add_argument('--reps', type=int, default=3)
    s.add_argument('--out', default=STUDY)
    t = sub.add_parser('table')
    t.add_argument('results', nargs='?', default=STUDY)
    a = ap.parse_args()
    study(a) if a.cmd == 'study' else table(a)


if __name__ == '__main__':
    main()
