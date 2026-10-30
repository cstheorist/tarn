#!/usr/bin/env python3
"""Summarize paired routing studies without pooling different fixture settings.

The interval is a two-sided Student-t interval on five paired log ratios.
It describes variation in this finite, single-host fixture, not independent
hardware replications or a production capacity guarantee. Failed arms stay in
the output and cannot contribute to a positive qualification claim.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics


def describe(values):
    return dict(median=statistics.median(values), minimum=min(values), maximum=max(values)) if values else None


def delta(before, after, key):
    # Tarantool's MessagePack encoder represents an empty Lua map as [].
    if before == []:
        before = {}
    if after == []:
        after = {}
    return after.get(key, 0) - before.get(key, 0)


def metrics(sample):
    count = sample['completed']
    before, after = sample['native_before'], sample['native_after']
    calls = {key: sum(delta(b['calls'], a['calls'], key) for b, a in zip(before, after))
             for key in ('initial', 'capture', 'apply', 'select', 'frontiers', 'base_fetch')}
    elapsed = {key: sum(delta(b['seconds'], a['seconds'], key) for b, a in zip(before, after))
               for key in calls}
    assignments = [delta(b, a, 'assignments') for b, a in zip(before, after)]
    reads = [delta(b['calls'], a['calls'], 'select') for b, a in zip(before, after)]
    cpu = dict(storage=0, gateway=0, clients=0)
    io = {key: 0 for key in ('read_bytes', 'write_bytes', 'rchar', 'wchar')}
    storage_cpu = {}
    for name, b in sample['process_before'].items():
        a = sample['process_after'][name]
        assert (a['pid'], a['start_ticks']) == (b['pid'], b['start_ticks']), name
        role = 'storage' if name.startswith('s') else 'gateway' if name.startswith('tarn') else 'clients'
        used = delta(b, a, 'cpu_seconds')
        cpu[role] += used
        if role == 'storage':
            storage_cpu[name] = used
        for key in io:
            io[key] += delta(b['io'], a['io'], key)
    out = dict(goodput=sample['goodput'], goodput_at_p99_ceiling=sample['goodput_at_p99_ceiling'],
               p50_ms=sample['p50']*1000, p99_ms=sample['p99']*1000,
               rows_inserted=sample['rows_inserted'], completed=count, skipped=sample['skipped'],
               outage_catchup_ms=sample['outage_catchup_seconds']*1000,
               gateway_rpc_per_op=delta(sample['status_before'],sample['status_after'],'Tarn_storage_rpc_enqueued')/count,
               gateway_wire_bytes_per_op=sum(delta(sample['status_before'],sample['status_after'],k)
                                            for k in ('Tarn_storage_bytes_written','Tarn_storage_bytes_read'))/count,
               leader_iproto_bytes_per_op=sum(a['net'][k]['total']-b['net'][k]['total']
                                             for b,a in zip(before,after) for k in ('SENT','RECEIVED'))/count,
               encoded_apply_bytes_per_op=sum(delta(b,a,'encoded_apply_bytes') for b,a in zip(before,after))/count,
               max_source_lag_seconds=max((row[3] for a in after for row in a['samples']),default=0),
               sum_source_peak_backlog_entries=sum(max((row[1] for row in a['samples']),default=0) for a in after),
               sum_source_peak_backlog_bytes=sum(max((row[2] for row in a['samples']),default=0) for a in after),
               target_assignment_counts=assignments, target_select_counts=reads,
               max_target_assignment_share=max(assignments)/sum(assignments) if sum(assignments) else None,
               max_target_select_share=max(reads)/sum(reads) if sum(reads) else None,
               storage_cpu_seconds=storage_cpu)
    out.update({f'{key}_calls_per_op': value/count for key,value in calls.items()})
    out.update({f'{key}_elapsed_ms_per_op': value*1000/count for key,value in elapsed.items()})
    out.update({f'{role}_cpu_ms_per_op': value*1000/count for role,value in cpu.items()})
    out.update({f'process_{key}_per_op': value/count for key,value in io.items()})
    for space in after[0]['spaces']:
        for key in ('rows','bytes'):
            out[f'{space}_{key}'] = sum(a['spaces'][space][key] for a in after)
    return out


def summarize(path):
    report = json.loads(path.read_text())
    args = report['arguments']
    arms = []
    for sample in report['samples']:
        errors = [w['errors'] for w in sample.get('workers',[]) if w['errors']]
        arm = {k: sample[k] for k in ('shape','lanes','pair','route_parts')}
        arm.update(complete=sample.get('complete',False), errors=errors, failure=sample.get('failure'))
        raw = sample.get('raw_latencies')
        if raw:
            file = path.parent/raw['path']
            digest = hashlib.sha256()
            with file.open('rb') as stream:
                for block in iter(lambda: stream.read(1024*1024),b''):
                    digest.update(block)
            arm['raw_sample_hash_valid'] = digest.hexdigest() == raw['sha256']
        else:
            arm['raw_sample_hash_valid'] = all('latencies' in w for w in sample.get('workers',[])) and bool(sample.get('workers'))
        if arm['complete']:
            arm['metrics'] = metrics(sample)
        arms.append(arm)
    results = []
    for shape,lanes in sorted({(s['shape'],s['lanes']) for s in arms}):
        group = [a for a in arms if (a['shape'],a['lanes']) == (shape,lanes)]
        valid = (args['pairs'] == 5 and len(group) == 10 and
                 all(a['complete'] and not a['errors'] and not a['failure'] and a['raw_sample_hash_valid'] and
                     a['metrics']['goodput_at_p99_ceiling'] > 0 for a in group))
        ratios = []
        for pair in range(args['pairs']):
            pair_arms = [a for a in group if a['pair'] == pair]
            if len(pair_arms) != 2 or {a['route_parts'] for a in pair_arms} != {1,2}:
                valid = False
                continue
            one,two = sorted(pair_arms,key=lambda a:a['route_parts'])
            if all('metrics' in a and a['metrics']['goodput_at_p99_ceiling'] > 0 for a in (one,two)):
                ratios.append(two['metrics']['goodput_at_p99_ceiling']/one['metrics']['goodput_at_p99_ceiling'])
        interval = None
        if valid:
            logs = [math.log(x) for x in ratios]
            center = statistics.mean(logs)
            margin = 2.7764451051977987*statistics.stdev(logs)/math.sqrt(5)
            interval = dict(geometric_mean=math.exp(center), lower95=math.exp(center-margin), upper95=math.exp(center+margin))
        summaries = {}
        for parts in (1,2):
            complete = [a['metrics'] for a in group if a['route_parts']==parts and 'metrics' in a]
            summaries[str(parts)] = {key: describe([m[key] for m in complete]) for key in complete[0]
                                     if isinstance(complete[0][key],(float,int))} if complete else {}
        results.append(dict(shape=shape,lanes=lanes,qualified=valid,paired_ratios=ratios,
                            paired_ratio_interval=interval,by_route_parts=summaries,arms=group))
    return dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),arguments=args,
                source=report['source'],binaries=report['binaries'],host=report['host'],results=results)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('reports',type=Path,nargs='+')
    ap.add_argument('--output',type=Path,required=True)
    args = ap.parse_args()
    result = dict(method=__doc__,reports=[summarize(path) for path in args.reports])
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    for report in result['reports']:
        for row in report['results']:
            interval = row['paired_ratio_interval']
            print(report['path'],row['shape'],row['lanes'],
                  'ratio %.4f [%.4f, %.4f]' % tuple(interval[k] for k in ('geometric_mean','lower95','upper95'))
                  if interval else 'UNQUALIFIED')


if __name__ == '__main__':
    main()
