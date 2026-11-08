#!/usr/bin/env python3
"""Where a READ COMMITTED transaction's storage time goes (issue #186).

  python3 bench/rc_phases.py [--json OUT] RESULTS.json [RESULTS.json ...]

Reads bench/rc_profile.py's results from a cluster started with
TARN_RC_PHASES (lua/tarn_phases.lua), and prints, for each workload shape,
each phase's time and allocation per acknowledged transaction, summed over
the shards' leaders and taken over the ordinary (unprofiled, not warmup)
windows. Phases that enclose others are split here:
  - lock_begin's other checks are lock_begin less its freshness barrier;
  - lock_acquire's bookkeeping is lock_acquire less lock_wait;
  - commit_wal is box.commit less the lock release its commit trigger runs;
  - each procedure's "other" is its total less the phases counted within it.
The share is of the two procedures' total time (tarn_lock and
tarn_commit_rc), summed over transactions: a transaction's storage-side
latency, not the shard's CPU. A phase's time includes its waits and the
garbage collector's steps its allocations set off. Bytes are shown only for
the parts that run without yielding, where LuaJIT's count of allocated
bytes is theirs alone (the probes' own left out); a part that waits counts
every fiber's allocations meanwhile.
"""
import json
import sys

LOCK = ('lock_begin', 'lock_acquire', 'lock_wait', 'lock_commit_wait', 'lock_read')
COMMIT = ('commit_fresh', 'commit_plan', 'commit_checks', 'commit_wal', 'commit_release', 'commit_lock_wait')


# The parts that never yield on memtx (on vinyl, reads and plans may).
OWN_BYTES = ('lock: lock bookkeeping', 'lock: reading the row', 'commit: write plans', 'commit: final checks',
             'commit: releasing the row locks')


def split(p):
    """The phase table as disjoint parts: {part: [seconds, bytes]}."""
    get = lambda k: p.get(k, [0, 0, 0])
    out = {}
    out['lock: freshness barrier'] = get('lock_barrier')[1:]
    out['lock: other checks before the first lock'] = [get('lock_begin')[i] - get('lock_barrier')[i] for i in (1, 2)]
    out['lock: lock bookkeeping'] = [get('lock_acquire')[i] - get('lock_wait')[i] for i in (1, 2)]
    out['lock: waiting for the row lock'] = get('lock_wait')[1:]
    out['lock: waiting for commits on the row'] = get('lock_commit_wait')[1:]
    out['lock: reading the row'] = get('lock_read')[1:]
    out['lock: other'] = [get('lock_total')[i] - sum(get(k)[i] for k in LOCK if k != 'lock_wait') for i in (1, 2)]
    out['commit: freshness and fence checks'] = get('commit_fresh')[1:]
    out['commit: write plans'] = get('commit_plan')[1:]
    out['commit: final checks'] = get('commit_checks')[1:]
    out['commit: WAL, fsync and quorum'] = [get('commit_wal')[i] - get('commit_release')[i] for i in (1, 2)]
    out['commit: releasing the row locks'] = get('commit_release')[1:]
    out['commit: waiting for a prepared lock'] = get('commit_lock_wait')[1:]
    out['commit: other'] = [get('commit_total')[i] - sum(get(k)[i] for k in COMMIT if k != 'commit_release')
                            for i in (1, 2)]
    return out


def main():
    args = sys.argv[1:]
    out = None
    if args[:1] == ['--json']:
        out, args = args[1], args[2:]
    shapes, meta = {}, {}
    for path in args:
        evidence = json.load(open(path))
        meta = {k: evidence.get(k) for k in ('source', 'source_dirty', 'lua_sha256', 'binary_sha256', 'engine',
                                              'native', 'host', 'scope')}
        for s in evidence['samples']:
            if s.get('warmup') or s.get('profiled') or not s.get('complete'):
                continue
            acc = shapes.setdefault(s['shape'], {'acks': 0, 'phases': {}, 'windows': 0, 'alloc': 0,
                                                 'engine': evidence.get('engine')})
            acc['acks'] += s['acknowledged']
            acc['windows'] += 1
            acc.setdefault('goodput', []).append(s['goodput_per_second'])
            acc.setdefault('p50', []).append(s['latency']['ok']['p50'])
            acc.setdefault('p99', []).append(s['latency']['ok']['p99'])
            leaders = {r['address'] for r in s['before'] if not r['result']['info']['ro']}
            for d in s['deltas']:
                if d['address'] in leaders:
                    acc['alloc'] += d['metrics']['gc_allocated']
                for k, v in (d.get('phases') or {}).items():
                    tot = acc['phases'].setdefault(k, [0, 0, 0])
                    for i in range(3):
                        tot[i] += v[i]
    for shape, acc in shapes.items():
        if not acc['phases']:
            print(f"{shape}: no phases (start the cluster with TARN_RC_PHASES=1)")
            continue
        n, parts = acc['acks'], split(acc['phases'])
        total = acc['phases']['lock_total'][1] + acc['phases']['commit_total'][1]
        print(f"\n{shape}: {acc['windows']} windows, {n} acknowledged; storage time {1e6 * total / n:,.0f} us/ack "
              f"(tarn_lock calls {acc['phases']['lock_total'][0] / n:.2f}/ack, "
              f"tarn_commit_rc {acc['phases']['commit_total'][0] / n:.2f}/ack)")
        print(f"leaders' Lua allocation {acc['alloc'] / n:,.0f} bytes/ack, every fiber included")
        print("| Part | us/ack | share | bytes/ack |")
        print("|---|---:|---:|---:|")
        own = OWN_BYTES if acc['engine'] != 'vinyl' else OWN_BYTES[:1] + OWN_BYTES[3:]
        for name, (sec, b) in parts.items():
            print(f"| {name} | {1e6 * sec / n:,.1f} | {100 * sec / total:.1f}% | "
                  f"{f'{b / n:,.0f}' if name in own else '-'} |")


    if out:
        # (the windows' totals, without bench/rc_profile.py's attempts and node metrics)
        compact = dict(meta, shapes={shape: dict(
            windows=acc['windows'], acknowledged=acc['acks'], goodput_per_second=acc.get('goodput'),
            p50_seconds=acc.get('p50'), p99_seconds=acc.get('p99'),
            leaders_allocated_bytes_per_ack=acc['alloc'] / acc['acks'],
            phases=acc['phases']) for shape, acc in shapes.items()})
        with open(out, 'w') as f:
            json.dump(compact, f, indent=1)
            f.write('\n')


if __name__ == '__main__':
    main()
