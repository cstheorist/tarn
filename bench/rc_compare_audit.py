#!/usr/bin/env python3
"""Audit the retained paired pilot independently of the collector's summaries."""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import statistics
import tarfile


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def audit_mode(data, source, legacy_session_flag=False):
    receipt = json.loads(data['rc-results/paired/comparison.json'])
    baseline = json.loads(data['rc-results/baseline.json'])
    assert receipt['complete'] and receipt['source'] == source == baseline['source']
    assert receipt['binary_sha256'] == baseline['binary_sha256']
    assert receipt['native_sha256'] == baseline['native_sha256']
    assert (receipt['engine'], receipt['native']) == (baseline['engine'], baseline['native'])
    assert receipt['environment']['TARN_CHECK_LOCKS'] == '0'
    assert receipt['environment']['TARN_CHECK_YIELDS'] == '0'
    assert receipt['environment']['TARN_VINYL_CACHE_MB'] == '0'
    hypothesis = receipt.get('hypothesis', 'condition')
    variants = {'condition': ('eager', 'deferred'), 'plan-guards': ('scalar', 'batched')}[hypothesis]
    assert tuple(receipt.get('variants', variants)) == variants
    copies = []
    for variant in variants:
        prefix = f'paired-fixture/{variant}/'
        files = {p[len(prefix):]: raw for p, raw in data.items() if p.startswith(prefix)}
        assert sha(files['lua/tarn.lua']) == receipt['lua_sha256'][variant]
        if 'lua_file_sha256' in receipt:
            assert {p: sha(raw) for p, raw in files.items() if p.startswith('lua/') and p.endswith('.lua')} == receipt['lua_file_sha256'][variant]
        copies.append(files)
    control, candidate = copies
    assert control.keys() == candidate.keys()
    eager, deferred = control['lua/tarn.lua'], candidate['lua/tarn.lua']
    if hypothesis == 'condition':
        constructor = b'local req = {txid = txid, mode = mode, entry = e, cond = fiber.cond(), held = held}'
        publish = b'    if cur ~= nil then table.insert(e.queue, 1, req) else e.queue[#e.queue + 1] = req end'
        assert eager.count(constructor) == eager.count(publish) == 1
        assert deferred == eager.replace(constructor, constructor.replace(b'cond = fiber.cond(), ', b'')).replace(
            publish, b'    req.cond = fiber.cond()\n' + publish)
        changed = {'lua/tarn.lua'}
    else:
        before = b'''function BK.fresh_plans(plans)
    M.fresh.clock_check(); M.fresh.controls(); writable()
    for _, p in ipairs(plans) do M.fresh.space(p[P_SPACE]) end
end'''
        after = b'''function BK.fresh_plans(plans)
    M.fresh.clock_check(); M.fresh.plans(plans)
end'''
        assert eager.count(before) == 1 and deferred == eager.replace(before, after)
        original = control['lua/tarn_fresh.lua']
        space = b'    function F.space(sp)\n        local mode = F.mode()\n'
        anchor = b'    -- These ordinary Lua guards run through the public Lua C API before\n'
        methods = b'''    function F.space(sp) return space_mode(sp, F.mode()) end
    -- Only used inside the existing nonyielding final plan check. This
    -- mode is local to one invocation and cannot survive a yield/barrier.
    function F.plans(plans)
        local mode = F.controls()
        c.writable()
        for _, plan in ipairs(plans) do space_mode(plan[1], mode) end
    end
'''
        assert original.count(space) == original.count(anchor) == 1
        assert candidate['lua/tarn_fresh.lua'] == original.replace(space, b'    local function space_mode(sp, mode)\n').replace(anchor, methods + anchor)
        changed = {'lua/tarn.lua', 'lua/tarn_fresh.lua'}
    assert {p for p in control if control[p] != candidate[p]} == changed
    sessions = receipt['sessions']
    expected_order = [(pair, variant) for pair in range(5)
                      for variant in (variants if pair % 2 == 0 else tuple(reversed(variants)))]
    assert [(s['pair'], s['variant']) for s in sessions] == expected_order
    totals = collections.Counter()
    measured = {}
    parameters = sessions[0]['parameters']
    for session in sessions:
        assert session['complete'] and session['parameters'] == parameters
        assert 'N=2 R=3 ' in parameters and 'WAL=fsync ' in parameters
        assert session['lua_sha256'] == receipt['lua_sha256'][session['variant']]
        assert session['cpu_tick_hz'] > 0
        stem = f"rc-results/paired/pair-{session['pair']}-{session['variant']}"
        separate = json.loads(data[stem + '.json'])
        if legacy_session_flag:
            # Run 41052637120 flushed the final samples before setting this
            # flag. Preserve its bytes; the enclosing receipt is final.
            assert separate['complete'] is False
            separate['complete'] = True
        assert separate == session
        shapes = collections.Counter()
        for sample in session['samples']:
            assert sample['complete'] and not sample['profiled']
            assert not sample['errors'] and not sample['workers_live'] and not sample.get('diagnostic_errors')
            assert sample['scheduled_seconds'] == (3 if sample['warmup'] else 5)
            raw = data['rc-results/paired/' + sample['attempts_file']]
            assert sha(raw) == sample['attempts_sha256']
            attempts = [json.loads(line) for line in raw.splitlines()]
            assert all(a['outcome'] in ('ok', 'refused') for a in attempts)
            assert len({a['token'] for a in attempts}) == len(attempts)
            ack = {a['token']: a['key'] for a in attempts if a['outcome'] == 'ok'}
            assert len(ack) == sample['acknowledged'] > 0
            assert len(attempts) - len(ack) == sample['refused']
            assert len(sample['readback']['ledger']) == len(ack)
            assert dict(sample['readback']['ledger']) == ack
            counters = collections.Counter(ack.values())
            assert dict(sample['readback']['counters']) == {k: counters[k] for k in range(8)}
            assert sample['goodput_per_second'] == len(ack) / sample['elapsed_seconds']
            for phase in ('before', 'after'):
                assert len(sample[phase]) == len({r['address'] for r in sample[phase]}) == 6
                leaders = 0
                for row in sample[phase]:
                    info = row['result']
                    assert info['wal'] == 'fsync' and info['quorum'] == 'N / 2 + 1'
                    assert info['engine'] == receipt['engine']
                    if not info['info']['ro']:
                        leaders += 1
                        native = info['info']['native']
                        assert (isinstance(native, dict) and native['ready'] == 1) if receipt['native'] == 'enabled' else native is False
                assert leaders == 2
            shapes[sample['shape'], sample['warmup']] += 1
            totals.update(samples=1, attempts=len(attempts), acknowledged=len(ack), refused=sample['refused'])
            if not sample['warmup']:
                ticks = sum(sample['cpu_after'][pid]['tx_ticks'] - before['tx_ticks']
                            for pid, before in sample['cpu_before'].items())
                measured[session['pair'], session['variant'], sample['shape']] = dict(
                    goodput=sample['goodput_per_second'], latency=sample['latency'],
                    lua_bytes_per_ack=sum(r['metrics']['gc_allocated'] for r in sample['deltas']) / len(ack),
                    tx_cpu_seconds_per_ack=ticks / session['cpu_tick_hz'] / len(ack))
        assert shapes == {(shape, warm): 1 for shape in ('uncontended', 'unrelated', 'hot') for warm in (True, False)}
    groups = []
    for shape in ('uncontended', 'unrelated', 'hot'):
        pairs = []
        for pair in range(5):
            e, d = (measured[pair, variant, shape] for variant in variants)
            pairs.append(dict(pair=pair, **{variants[0]: e, variants[1]: d}, goodput_ratio=d['goodput']/e['goodput'],
                              lua_allocation_ratio=d['lua_bytes_per_ack']/e['lua_bytes_per_ack'],
                              tx_cpu_ratio=d['tx_cpu_seconds_per_ack']/e['tx_cpu_seconds_per_ack']))
        original = next(s for s in receipt['summary'] if s['shape'] == shape)
        ratios = [p['goodput_ratio'] for p in pairs]
        assert original['median_goodput_ratio'] == statistics.median(ratios)
        assert original['goodput_ratio_range'] == [min(ratios), max(ratios)]
        groups.append(dict(shape=shape, pairs=pairs, median_goodput_ratio=statistics.median(ratios),
                           goodput_ratio_range=[min(ratios), max(ratios)]))
    return dict(engine=receipt['engine'], native=receipt['native'], totals=dict(totals), groups=groups)


def audit(manifest_path):
    manifest_path = Path(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    results = []
    for archive in manifest['archives']:
        path = manifest_path.parent / archive['path']
        assert path.stat().st_size == archive['bytes'] and sha(path.read_bytes()) == archive['sha256']
        data = {}
        with tarfile.open(path) as tar:
            assert len(tar.getmembers()) == len(archive['members'])
            for member in archive['members']:
                raw = tar.extractfile(member['path']).read()
                assert len(raw) == member['bytes'] and sha(raw) == member['sha256']
                assert member['path'] not in data
                data[member['path']] = raw
        results.append(audit_mode(data, manifest['source'], manifest.get('legacy_session_flag', False)))
    assert {(r['engine'], r['native']) for r in results} == {
        (e, n) for e in ('memtx', 'vinyl') for n in ('enabled', 'disabled')}
    assert len(results) == 4
    return dict(source=manifest['source'], modes=results)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    print(json.dumps(audit(parser.parse_args().manifest), sort_keys=True))
