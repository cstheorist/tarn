#!/usr/bin/env python3
"""Independently audit retained RC baseline archives without extracting them."""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import tarfile


def audit(manifest_path):
    manifest_path = Path(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    totals = collections.Counter()
    modes = set()
    for archive in manifest['archives']:
        path = manifest_path.parent / archive['path']
        assert path.stat().st_size == archive['bytes']
        assert hashlib.sha256(path.read_bytes()).hexdigest() == archive['sha256']
        with tarfile.open(path) as tar:
            members = {m.name: m for m in tar.getmembers()}
            expected = {m['path']: m for m in archive['members']}
            assert len(members) == len(tar.getmembers()) == len(expected)
            assert set(members) == set(expected)
            data = {}
            for name, member in members.items():
                assert member.isfile(), name
                raw = tar.extractfile(member).read()
                receipt = expected[name]
                assert len(raw) == receipt['bytes']
                assert hashlib.sha256(raw).hexdigest() == receipt['sha256']
                data[name] = raw
        baseline = json.loads(data['rc-results/baseline.json'])
        assert baseline['complete'] and not baseline['source_dirty']
        assert baseline['source'] == manifest['source']
        assert data['rc-build/source.txt'].decode().strip() == manifest['source']
        mode = baseline['engine'], baseline['native']
        assert mode not in modes
        modes.add(mode)
        shapes = collections.Counter()
        for sample in baseline['samples']:
            assert sample['complete'] and not sample['errors']
            assert not sample['workers_live'] and not sample.get('diagnostic_errors')
            raw = data['rc-results/' + sample['attempts_file']]
            assert hashlib.sha256(raw).hexdigest() == sample['attempts_sha256']
            attempts = [json.loads(line) for line in raw.splitlines()]
            assert all(r['outcome'] in ('ok', 'refused') for r in attempts)
            ack = {r['token']: r['key'] for r in attempts if r['outcome'] == 'ok'}
            assert len(ack) == sum(r['outcome'] == 'ok' for r in attempts) == sample['acknowledged']
            ledger = sample['readback']['ledger']
            assert len(ledger) == len(ack) and dict(ledger) == ack
            counters = collections.Counter(ack.values())
            assert dict(sample['readback']['counters']) == {k: counters[k] for k in range(8)}
            refused = len(attempts) - len(ack)
            assert refused == sample['refused']
            for phase in ('before', 'after'):
                assert len(sample[phase]) == 6
                assert len({row['address'] for row in sample[phase]}) == 6
                leaders = 0
                for row in sample[phase]:
                    r = row['result']
                    assert r['wal'] == 'fsync' and r['quorum'] == 'N / 2 + 1'
                    assert r['engine'] == baseline['engine']
                    if not r['info']['ro']:
                        leaders += 1
                        n = r['info']['native']
                        assert (isinstance(n, dict) and n['ready'] == 1) if mode[1] == 'enabled' else n is False
                assert leaders == 2
            if sample['profiled']:
                assert len(sample['profile']) == 2
                for row in sample['profile']:
                    profile = row['result']
                    assert not profile['auto_stopped']
                    assert profile['distinct'] == len(profile['stacks']) <= 256
                    assert max(map(len, profile['stacks']), default=0) <= 2050
                    assert sum(profile['states'].values()) == profile['samples']
                    assert sum(profile['stacks'].values()) + profile['overflow_samples'] == profile['samples']
            shapes[sample['shape'], sample['profiled'], sample['warmup']] += 1
            totals.update(samples=1, attempts=len(attempts), acknowledged=len(ack), refused=refused)
        expected_shapes = {}
        for shape in ('uncontended', 'unrelated', 'hot'):
            expected_shapes[shape, False, True] = 1
            expected_shapes[shape, False, False] = 5
            expected_shapes[shape, True, False] = 5
        assert shapes == expected_shapes
    assert modes == {(engine, native) for engine in ('memtx', 'vinyl') for native in ('enabled', 'disabled')}
    return dict(source=manifest['source'], modes=len(modes), **totals)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    print(json.dumps(audit(parser.parse_args().manifest), sort_keys=True))
