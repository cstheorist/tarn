"""Audit the retained manifest/archives and recompute the descriptive summary.

Run from the repository root with the manifest path. Requires only Python
and Git; it does not import the measured collector or its summary.
"""
import collections
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys
import tarfile
import tempfile

source = '088cdb15477e7646d5483561060fc92cc0548bd3'
argument = Path(sys.argv[1])
temporary = None
if argument.is_file():
    manifest = json.loads(argument.read_text())
    assert manifest['source'] == source and len(manifest['archives']) == 4
    temporary = tempfile.TemporaryDirectory(prefix='rc186-audit-')
    root = Path(temporary.name)
    for entry in manifest['archives']:
        archive_path = argument.parent / entry['path']
        raw_archive = archive_path.read_bytes()
        assert len(raw_archive) == entry['bytes']
        assert hashlib.sha256(raw_archive).hexdigest() == entry['sha256']
        expected = {member['path']: member for member in entry['members']}
        with tarfile.open(archive_path) as archive:
            members = archive.getmembers()
            assert len(members) == len(expected) == len({m.name for m in members})
            assert {m.name for m in members} == expected.keys()
            for member in members:
                assert member.isfile() and not Path(member.name).is_absolute()
                assert '..' not in Path(member.name).parts
                raw = archive.extractfile(member).read()
                assert len(raw) == expected[member.name]['bytes']
                assert hashlib.sha256(raw).hexdigest() == expected[member.name]['sha256']
                destination = root / entry['mode'] / member.name
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(raw)
else:
    root = argument
lua_sha = hashlib.sha256(subprocess.check_output(['git', 'show', source + ':lua/tarn.lua'])).hexdigest()
worker_sha = hashlib.sha256(subprocess.check_output(['git', 'show', source + ':bench/rc_profile.py'])).hexdigest()
profiler_sha = hashlib.sha256(subprocess.check_output(['git', 'show', source + ':bench/rc_guest_profile.lua'])).hexdigest()
summaries = []
all_totals = collections.Counter()
for path in sorted(root.rglob('baseline.json')):
    baseline = json.loads(path.read_text())
    assert baseline['complete'] and not baseline['source_dirty'], path
    assert baseline['source'] == source
    assert baseline['lua_sha256'] == lua_sha
    assert baseline['harness_sha256'] == worker_sha
    assert baseline['profiler_sha256'] == profiler_sha
    receipts = path.parent.parent / 'rc186-build'
    assert (receipts / 'source.txt').read_text().strip() == source
    binaries = {line.split()[1].split('/')[-1]: line.split()[0]
                for line in (receipts / 'binaries.sha256').read_text().splitlines()}
    assert baseline['binary_sha256'] == binaries['tarn-server']
    assert baseline['native_sha256'] == binaries['libtarn_storage.so']
    shape_counts = collections.Counter()
    totals = collections.Counter()
    profiles = collections.Counter()
    states = collections.Counter()
    groups = collections.defaultdict(list)
    expected_order = [(shape, False, True, 3) for shape in ('uncontended', 'unrelated', 'hot')]
    for profiled in (False, True):
        for repeat in range(5):
            shapes = ('uncontended', 'unrelated', 'hot')
            for shape in (shapes if repeat % 2 == 0 else tuple(reversed(shapes))):
                expected_order.append((shape, profiled, False, 5))
    assert [(s['shape'], s['profiled'], s['warmup'], s['scheduled_seconds'])
            for s in baseline['samples']] == expected_order
    assert [s['number'] for s in baseline['samples']] == list(range(1, 34))
    for sample in baseline['samples']:
        assert sample['complete'] and not sample['errors'] and not sample['workers_live']
        assert not sample.get('diagnostic_errors')
        raw = (path.parent / sample['attempts_file']).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == sample['attempts_sha256']
        attempts = [json.loads(line) for line in raw.splitlines()]
        assert len({a['token'] for a in attempts}) == len(attempts)
        assert all(a['outcome'] in ('ok', 'refused') for a in attempts)
        ack = {a['token']: a['key'] for a in attempts if a['outcome'] == 'ok'}
        assert len(ack) == sample['acknowledged'] > 0
        assert sample['refused'] == len(attempts) - len(ack)
        assert all(a['error'][0] in (1205, 1213) for a in attempts if a['outcome'] == 'refused')
        assert len(sample['readback']['ledger']) == len(ack)
        assert dict(sample['readback']['ledger']) == ack
        expected = collections.Counter(ack.values())
        assert dict(sample['readback']['counters']) == {k: expected[k] for k in range(8)}
        assert sample['goodput_per_second'] == len(ack) / sample['elapsed_seconds']
        for phase in ('before', 'after'):
            rows = sample[phase]
            assert len(rows) == len({r['address'] for r in rows}) == 6
            assert sum(not r['result']['info']['ro'] for r in rows) == 2
            for row in rows:
                r = row['result']
                assert r['wal'] == 'fsync' and r['quorum'] == 'N / 2 + 1'
                assert r['engine'] == baseline['engine']
                assert all(r['test_flags'].get(k) is False for k in ('check_locks', 'check_yields'))
                if not r['info']['ro']:
                    native = r['info']['native']
                    assert (isinstance(native, dict) and native['ready'] == 1) if baseline['native'] == 'enabled' else native is False
        before = {r['address']: r['result'] for r in sample['before']}
        after = {r['address']: r['result'] for r in sample['after']}
        assert before.keys() == after.keys()
        for address in before:
            assert (before[address]['pid'], before[address]['info']['ro']) == (after[address]['pid'], after[address]['info']['ro'])
        deltas = {row['address']: row for row in sample['deltas']}
        assert len(sample['deltas']) == len(deltas) == 6 and deltas.keys() == before.keys()
        for address, delta in deltas.items():
            b, a = before[address], after[address]
            assert delta['calls'] == a['calls'] - b['calls']
            assert delta['metrics'] == {k: a['metrics'][k] - b['metrics'][k] for k in
                                       ('gc_allocated', 'gc_freed', 'gc_steps_propagate',
                                        'gc_steps_atomic', 'gc_steps_sweep', 'gc_steps_finalize')}
            assert delta['stats'] == {k: a['info']['stats'][k] - b['info']['stats'][k] for k in
                                     ('plock_waits', 'deadlocks', 'lock_timeouts', 'commit_rc', 'commit_waits')}
        for outcome in ('ok', 'refused', 'unknown'):
            latencies = sorted(a['latency_seconds'] for a in attempts if a['outcome'] == outcome)
            for fraction, name in ((.5, 'p50'), (.99, 'p99')):
                expected_q = latencies[min(len(latencies)-1, int(fraction*len(latencies)))] if latencies else None
                assert sample['latency'][outcome][name] == expected_q
        if sample['profiled']:
            assert len(sample['profile']) == 2
            assert len(sample['profile_bounds']) == 2
            for bound in sample['profile_bounds']:
                assert bound['result'] == dict(max_stacks=256, max_depth=8, max_stack_bytes=2048,
                                               interval_ms=10, deadline_seconds=7)
            for row in sample['profile']:
                p = row['result']
                assert not p['auto_stopped'] and p['distinct'] == len(p['stacks']) <= 256
                assert (p['max_stacks'], p['max_depth'], p['max_stack_bytes'], p['interval_ms']) == (256, 8, 2048, 10)
                assert all(len(stack) <= 2050 for stack in p['stacks'])
                assert all(len([frame for frame in stack[2:].split(';') if frame]) <= 8 for stack in p['stacks'])
                assert sum(p['states'].values()) == p['samples']
                assert sum(p['stacks'].values()) + p['overflow_samples'] == p['samples']
                profiles.update(p['stacks'])
                states.update(p['states'])
        totals.update(samples=1, attempts=len(attempts), acknowledged=len(ack), refused=sample['refused'])
        shape_counts[sample['shape'], sample['profiled'], sample['warmup']] += 1
        if not sample['profiled'] and not sample['warmup']:
            assert sample['scheduled_seconds'] == 5
            assert set(sample['cpu_before']) == set(sample['cpu_after']) == {str(r['pid']) for r in before.values()}
            cpu = {kind: sum(sample['cpu_after'][pid][kind] - value[kind]
                            for pid, value in sample['cpu_before'].items()) / baseline['cpu_tick_hz'] / len(ack)
                   for kind in ('tx_ticks', 'process_ticks')}
            assert all(v >= 0 for v in cpu.values())
            delta_native = collections.Counter()
            for address in before:
                if isinstance(before[address]['info']['native'], dict):
                    for key in ('served', 'to_lua', 'writes', 'writes_to_lua', 'commits', 'commits_to_lua', 'validations', 'validations_to_lua'):
                        delta_native[key] += after[address]['info']['native'][key] - before[address]['info']['native'][key]
            groups[sample['shape']].append(dict(
                tps=sample['goodput_per_second'], p50_ms=sample['latency']['ok']['p50']*1000,
                p99_ms=sample['latency']['ok']['p99']*1000,
                tx_cpu_us=cpu['tx_ticks']*1e6, process_cpu_us=cpu['process_ticks']*1e6,
                lua_bytes=sum(r['metrics']['gc_allocated'] for r in sample['deltas']) / len(ack),
                gc_steps=sum(sum(v for k,v in r['metrics'].items() if k.startswith('gc_steps_')) for r in sample['deltas']) / len(ack),
                plock_waits=sum(r['stats']['plock_waits'] for r in sample['deltas']) / len(ack),
                commit_waits=sum(r['stats']['commit_waits'] for r in sample['deltas']) / len(ack),
                native_delta=dict(delta_native)))
    assert shape_counts == {(shape, profiled, warm): count for shape in ('uncontended','unrelated','hot')
                            for profiled, warm, count in ((False,True,1),(False,False,5),(True,False,5))}
    all_totals.update(totals)
    summary = dict(engine=baseline['engine'], native=baseline['native'], source=baseline['source'], totals=dict(totals), groups={},
                   vm_states=dict(states), guest_stacks=profiles.most_common(20))
    for shape, samples in groups.items():
        summary['groups'][shape] = {key: dict(median=statistics.median(s[key] for s in samples),
                                             minimum=min(s[key] for s in samples), maximum=max(s[key] for s in samples))
                                    for key in samples[0] if key != 'native_delta'}
        summary['groups'][shape]['native_counts'] = dict(sum((collections.Counter(s['native_delta']) for s in samples), collections.Counter()))
    summaries.append(summary)
assert len(summaries) == 4
assert {(m['engine'], m['native']) for m in summaries} == {
    (e, n) for e in ('memtx', 'vinyl') for n in ('enabled', 'disabled')}
print(json.dumps(dict(totals=dict(all_totals), modes=summaries), indent=2))
if temporary is not None:
    temporary.cleanup()
