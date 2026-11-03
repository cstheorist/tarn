#!/usr/bin/env python3
"""Verify retained receipts/raw captures; emit reproducible per-cell metrics CSV."""
import csv
import hashlib
import io
import json
from pathlib import Path
import statistics
import sys
import tarfile
import zipfile

ROOT = Path(__file__).resolve().parent
RUN = '41729770781'
median = statistics.median
sha = lambda data: hashlib.sha256(data).hexdigest()
with tarfile.open(ROOT / 'source-receipts.tar.gz', 'r:gz') as archive:
    context = {m.name: archive.extractfile(m).read() for m in archive.getmembers()}
original_hashes = dict(line.split('  ', 1)[::-1] for line in
                       context['digest-manifest.txt'].decode().splitlines())
for name, data in context.items():
    if name != 'digest-manifest.txt':
        assert sha(data) == original_hashes[name], name
receipts = {name: json.loads(context[name + '.json']) for name in
            ['artifact-receipt', 'run-receipt', 'pr-receipt', 'head-commit', 'build-commit']}
head = receipts['head-commit']
build = receipts['build-commit']
assert head['commit']['tree']['sha'] == build['commit']['tree']['sha']
assert receipts['run-receipt']['headSha'] == head['sha']
assert receipts['run-receipt']['conclusion'] == 'success'
assert receipts['pr-receipt']['head']['sha'] == head['sha']
assert head['sha'] in [p['sha'] for p in build['parents']]
source = context[f'downloads/pushdown-build-{RUN}/source.txt'].decode().strip()
assert source == build['sha']
binary_hashes = dict(line.split()[::-1] for line in
                     context[f'downloads/pushdown-build-{RUN}/binaries.sha256'].decode().splitlines())
# Binary bytes are intentionally omitted: these are original receipt hashes,
# checked against the downloaded binary bytes during the original analysis.
binary = binary_hashes['target/release/tarn-server']
native = binary_hashes['target/release/libtarn_storage.so']
source_hashes = {k: sha(context['source/' + v]) for k, v in {
    'harness_sha256': 'bench/pushdown_matrix.py',
    'proxy_sha256': 'bench/pushdown.py', 'cargo_lock_sha256': 'Cargo.lock'}.items()}
rows = []
wire_count = check_count = sample_count = observations = 0
for receipt in receipts['artifact-receipt']['artifacts']:
    name = receipt['name']
    if name == f'pushdown-build-{RUN}':
        continue
    archive_bytes = (ROOT / 'archives' / (name + '.zip')).read_bytes()
    assert 'sha256:' + sha(archive_bytes) == receipt['digest']
    assert len(archive_bytes) == receipt['size_in_bytes']
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        raw = {n: archive.read(n) for n in archive.namelist() if not n.endswith('/')}
    for member, data in raw.items():
        assert sha(data) == original_hashes[f'downloads/{name}/{member}'], member
    wire = json.loads(raw['pushdown-results/wire/wire.json'])
    inputs = json.loads(raw['pushdown-results/wire/inputs.json'])
    assert wire['complete'] and wire['status'] == 'passed'
    assert not wire['observe_only'] and not wire['oracle_only']
    assert not wire['harness_errors'] and not wire['failures']
    assert len(wire['cases']) == 33
    assert inputs['source'] == source and inputs['source_dirty'] is False
    assert inputs['binary_sha256'] == binary
    assert inputs['runner_sha256'] == sha(context['source/tests/pushdown_contracts.py'])
    assert inputs['probe_sha256'] == sha(context['source/tests/sql/pushdown_contracts.py'])
    for case in wire['cases']:
        assert all(c['passed'] for c in case['checks'])
        assert next(c['passed'] for c in case['checks'] if c['check'] == 'off/on full wire')
    assert sum(c['reference_wire_equal'].get('ref') is True for c in wire['cases']) == 30
    assert sum(c['reference_wire_equal'].get('mysql') is True for c in wire['cases']) == 29
    wire_count += len(wire['cases'])
    check_count += sum(len(c['checks']) for c in wire['cases'])
    for member, data in sorted(raw.items()):
        if not member.startswith('pushdown-results/width-') or member.count('.') != 1:
            continue
        j = json.loads(data)
        assert j['complete'] and j['source_dirty'] is False and j['source'] == source
        assert j['binary_sha256'] == binary and j['native_sha256'] == native
        assert all(j[k] == v for k, v in source_hashes.items())
        assert j['native'] == 'enabled' and j['pairs'] == 5 and j['iterations'] == 10
        assert j['fixture']['rows'] == 1024 and len(j['samples']) == 160
        assert len(j['queries']) == 8 and j['cpu_tick_hz'] == 100
        for snapshot in ['runtime_before', 'runtime_after']:
            runtime = j[snapshot]
            assert len(runtime) == len({r['pid'] for r in runtime}) == 6
            for r in runtime:
                assert all(r['test_flags'][k] is False for k in ['check_locks', 'check_yields', 'test_hooks'])
                assert r['wal_mode'] == 'fsync' and r['quorum'] == 2 and r['replication_count'] == 3
                assert r['engine'] == j['engine']
                assert all(r[k] is True for k in ['sync', 'is_sync', 'effective_sync'])
            for shard in [1, 2]:
                group = [r for r in runtime if r['info']['shard_id'] == shard]
                uuids = {r['uuid'] for r in group}
                assert len(group) == len(uuids) == 3
                assert sum(not r['info']['ro'] for r in group) == 1
                for r in group:
                    assert len(r['replicas']) == 3 and {x['uuid'] for x in r['replicas']} == uuids
                    assert all(x['id'] == r['instance_id'] or x['upstream'] == 'follow' for x in r['replicas'])
                assert next(r for r in group if not r['info']['ro'])['info']['native']['ready'] == 1
            observations += len(runtime)
        assert {r['address']: r['pid'] for r in j['runtime_before']} == {r['address']: r['pid'] for r in j['runtime_after']}
        sample_count += len(j['samples'])
        for phase in ['direct', 'instrumented']:
            for query, (sql, path) in j['queries'].items():
                samples = [s for s in j['samples'] if s['phase'] == phase and s['query'] == query]
                assert len(samples) == 10
                pairs = [(next(s for s in samples if s['pair'] == p and s['pushdown'] is False),
                          next(s for s in samples if s['pair'] == p and s['pushdown'] is True)) for p in range(5)]
                for sample in samples:
                    assert sample['iterations'] == len(sample['latency_seconds']) == 10
                    assert sample['result_digest'] == j['expected_digests'][query]
                    for counter, expected in [('Tarn_storage_reductions', 'reduce'), ('Tarn_storage_projections', 'project')]:
                        assert (sample['path_counters'][counter] > 0) == (sample['pushdown'] and path == expected)
                ratios = [sum(on['latency_seconds']) / sum(off['latency_seconds']) for off, on in pairs]
                row = dict(engine=j['engine'], width=j['fixture']['payload_bytes'], phase=phase,
                           query=query, path=path, sql=sql,
                           classification='all_faster' if max(ratios) < 1 else 'all_slower' if min(ratios) > 1 else 'mixed')
                def stats(key, values):
                    row.update({key + '_median': median(values), key + '_min': min(values), key + '_max': max(values)})
                stats('paired_elapsed_on_off', ratios)
                for metric in ['gateway_cpu_seconds', 'storage_cpu_seconds']:
                    stats(metric + '_off', [off[metric] for off, on in pairs])
                    stats(metric + '_on', [on[metric] for off, on in pairs])
                    stats(metric + '_paired_delta', [on[metric] - off[metric] for off, on in pairs])
                if phase == 'instrumented':
                    for metric in ['response_bytes', 'request_bytes', 'requests', 'responses']:
                        for enabled, label in [(False, 'off'), (True, 'on')]:
                            stats(metric + '_per_query_' + label, [s[metric] / 10 for s in samples if s['pushdown'] is enabled])
                rows.append(row)
assert (wire_count, check_count, sample_count, observations, len(rows)) == (132, 920, 960, 72, 96)
print('Verified: 4 exact artifact digests, retained member/source hashes, 132 wire cases/920 checks, '
      '960 measurement samples/9600 queries, 72 runtime observations; 96 metric rows.', file=sys.stderr)
writer = csv.DictWriter(sys.stdout, fieldnames=sorted({key for row in rows for key in row}), lineterminator='\n')
writer.writeheader()
writer.writerows(rows)
