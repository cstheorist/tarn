# Where a READ COMMITTED transaction's storage time goes (#186)

[`RC_PROFILE_REFRESH.md`](RC_PROFILE_REFRESH.md) measured the RF3/fsync
baseline of READ COMMITTED transactions, but its guest stacks could not
separate "engine CPU, WAL fsync, quorum waiting, ownership bookkeeping and
final freshness validation". This study separates them, with an opt-in
profiler of the two storage procedures such a transaction calls:
`tarn_lock`, twice per transaction in this workload, and `tarn_commit_rc`,
once.

## Method

- **The profiler.** `lua/tarn_phases.lua`, switched on by `TARN_RC_PHASES`
  on the storage nodes (off by default). It brackets each phase of the two
  procedures and adds its wall time, and the bytes Lua allocated meanwhile
  (LuaJIT's `gc_allocated`), to the phase's counters.
  - A probe never yields. When the profiler is off, a probe does nothing.
  - The probes' own allocations are left out: one `misc.getmetrics` table,
    800 bytes, calibrated at each reset.
  - `tarn_rc_phases()` returns the counters, and `bench/rc_profile.py`
    takes their difference over each window.
- **The parts.** `bench/rc_phases.py` splits the phases into disjoint parts:
  - **Bookkeeping** is a lock's acquisition less its wait.
  - **WAL, fsync and quorum** is `box.commit()` less the lock release its
    commit trigger runs.
  - Each procedure's **other** is its total less the phases inside it.

  Shares are of the two procedures' summed wall time over all transactions,
  that is, of a transaction's storage-side latency, not the shard's CPU.
  A phase's time includes its waits, and the garbage collector's steps that
  its allocations trigger.
- **Bytes.** Shown only for the parts that never yield. While a part waits,
  other fibers' allocations land in its count too.
- **Setup.** One 2-CPU host, the same as `RC_PROFILE_REFRESH.md`'s workload
  and harness:
  - 2 shards × 3 replicas, WAL `fsync`, quorum `N / 2 + 1`, Lua procedures
    (`TARN_NATIVE=0`), debug checks off;
  - for each shape, five 5-second closed-loop windows after a warmup:
    no contention, unrelated keys, and one hot key.

  Results: `bench/rc-evidence/rc-phases-20261108/` (per run, per shape:
  windows, acknowledgements, goodput, latencies, allocation and each
  phase's calls, seconds and bytes). The source was main `1632947` plus
  this change.

## Results: memtx

µs and bytes are per acknowledged transaction, summed over the two shards'
leaders.

| Part | No contention | Unrelated keys | Hot key | Bytes, no contention |
|---|---:|---:|---:|---:|
| lock: freshness barrier | 937 (39.6%) | 3,691 (63.7%) | 1,763 (7.5%) | – |
| lock: other checks before the first lock | 25 (1.1%) | 22 (0.4%) | 25 (0.1%) | – |
| lock: bookkeeping | 17 (0.7%) | 16 (0.3%) | 36 (0.2%) | 1,314 |
| lock: waiting for the row lock | 0 | 0 | 20,005 (85.6%) | – |
| lock: waiting for commits on the row | 9 (0.4%) | 15 (0.3%) | 71 (0.3%) | – |
| lock: reading the row | 16 (0.7%) | 13 (0.2%) | 21 (0.1%) | 101 |
| lock: other | 65 (2.7%) | 61 (1.0%) | 96 (0.4%) | – |
| commit: freshness and fence checks | 21 (0.9%) | 19 (0.3%) | 21 (0.1%) | – |
| commit: write plans | 68 (2.9%) | 60 (1.0%) | 71 (0.3%) | 3,312 |
| commit: final checks | 57 (2.4%) | 54 (0.9%) | 59 (0.3%) | 3,596 |
| commit: WAL, fsync and quorum | 1,109 (46.9%) | 1,814 (31.3%) | 1,147 (4.9%) | – |
| commit: releasing the row locks | 18 (0.8%) | 14 (0.2%) | 25 (0.1%) | 769 |
| commit: other | 20 (0.9%) | 18 (0.3%) | 22 (0.1%) | – |
| **storage time** | **2,363** | **5,797** | **23,360** | |

## Results: vinyl

Same setup. A vinyl read or plan may yield, so their bytes aren't shown.

| Part | No contention | Unrelated keys | Hot key |
|---|---:|---:|---:|
| lock: freshness barrier | 788 (36.5%) | 3,608 (63.2%) | 1,558 (7.4%) |
| lock: waiting for the row lock | 0 | 0 | 18,102 (85.8%) |
| commit: WAL, fsync and quorum | 1,047 (48.4%) | 1,795 (31.4%) | 1,016 (4.8%) |
| everything else | 327 (15.1%) | 305 (5.3%) | 418 (2.0%) |
| **storage time** | **2,162** | **5,707** | **21,094** |

## What it shows

- **Waits are most of a transaction's storage time.** Without contention, a
  transaction spends 85–87% of it in two waits: the freshness barriers
  of its lock calls (37–40%), and its commit's WAL write, fsync and quorum
  (47–48%). All the Lua work together comes to about 310 µs (13%) on memtx: checks,
  ownership bookkeeping, plans, final checks, release and the rest. That
  figure includes the probes' own time.
- **The barrier waits longer under concurrency.**
  - Each `tarn_lock` starts with `M.fresh.barrier()`, an empty
    `linearizable` transaction that waits until the shard holds a quorum's
    confirmed prefix.
  - With unrelated keys and more transactions in flight, it rose to
    3.6–3.7 ms per transaction, 63% of the storage time, against 31% for
    the commit's WAL wait.
  - It is paid once per lock call, so twice per transaction here.
- **On a hot key, the row-lock wait is everything** (86%). The barrier and
  the commit are paid as before.
- **Ownership bookkeeping is small.** Acquiring a lock, aside from its
  wait, costs 16–36 µs per transaction, and releasing them 14–27 µs. The
  final guard batched in #276 now takes 51–61 µs.
- **Allocation.** The leaders allocate about 39–42 KB per transaction with
  the profiler off.
  - The parts that never yield account for about 9 KB of it (memtx): the
    plans 3.3 KB, the final checks 3.6 KB, lock bookkeeping 1.3 KB, the
    release 0.8 KB.
  - The rest is allocated while waiting, or outside the two procedures
    (request decoding, replication, other fibers).
  - Per-phase bytes vary between runs with what LuaJIT compiles and sinks.
    Lock bookkeeping was 658 bytes per call here and 2,242 in an earlier
    run.
- **What the profiler costs.**

  | | No contention | Unrelated keys | Hot key |
  |---|---:|---:|---:|
  | goodput, profiler off (median TPS) | 348.9 | 910.5 | 349.4 |
  | goodput, profiler on | 304.8 (0.87×) | 853.1 (0.94×) | 323.5 (0.93×) |
  | p50, off / on | 2.50 / 2.64 ms | 8.30 / 8.87 ms | 18.61 / 19.82 ms |
  | leaders' allocation per transaction, off / on | 39.9 / 70.5 KB | 38.7 / 69.1 KB | 42.2 / 74.2 KB |

  Most of the cost is the probes' `misc.getmetrics` tables. The shares
  above are of profiled time; the waits they show are unchanged by it.

## Not established

- This is one host, closed loop. The absolute waits depend on its disk and
  scheduling, not on a dedicated deployment (#211).
- Shares are of latency, not of CPU. CPU is in `RC_PROFILE_REFRESH.md`'s
  TX CPU per acknowledgement.
- Barrier amortization is only a candidate. Paying the barrier once per
  transaction and shard, rather than once per lock call, would save the
  second barrier's wait: up to a fifth of uncontended storage time here.
  But the barrier is what proves that a locked read sees the latest
  committed version on the current leader. Changing it needs a safety
  argument and a model, and nothing here measures it. (Issue #314 found it
  unsafe, and measured the barrier without the queue wait below instead.)

## Before and after the barrier without the queue wait (#314)

Lock calls now leave out the barrier's wait for the synchronous queue: a
round of vclock calls to the shard's other replicas, then the local
catch-up (design §2.9, `TARN_LOCK_BARRIER=owner`). Same harness, host and
setup as above, once per setting, with the profiler on. Results:
`bench/rc-evidence/lock-barrier-20261109/`, source `5cfe148`. (The memtx
runs record its parent with the change not yet committed. Their
`lua/tarn.lua` hash is `5cfe148`'s.)

The barrier column is µs per acknowledged transaction, summed over the
leaders, as is the commit's WAL, fsync and quorum. Goodput and p99 are
medians of the five windows. Values are full barrier → owner barrier.

memtx:

| Shape | Barrier | Commit's WAL wait | Storage time | Goodput, TPS | p99, ms |
|---|---:|---:|---:|---:|---:|
| No contention | 1,144 → 484 | 1,087 → 1,068 | 2,558 → 1,854 | 275 → 367 | 9.8 → 5.7 |
| Unrelated keys | 3,372 → 1,785 | 1,669 → 2,954 | 5,306 → 5,098 | 929 → 951 | 18.9 → 21.7 |
| Hot key | 2,417 → 677 | 1,166 → 1,114 | 27,036 → 19,150 | 267 → 374 | 121.1 → 39.0 |

vinyl:

| Shape | Barrier | Commit's WAL wait | Storage time | Goodput, TPS | p99, ms |
|---|---:|---:|---:|---:|---:|
| No contention | 1,118 → 543 | 1,131 → 1,196 | 2,588 → 2,121 | 288 → 303 | 17.8 → 6.5 |
| Unrelated keys | 3,494 → 1,944 | 1,766 → 3,082 | 5,524 → 5,426 | 857 → 839 | 20.0 → 23.0 |
| Hot key | 2,213 → 679 | 1,173 → 1,142 | 26,784 → 19,442 | 316 → 376 | 120.7 → 37.1 |

- **The barrier fell by half or more.** It is about 250 µs per lock call
  with one client, against 560 µs for Tarantool's barrier on this host. Its
  share of storage time fell from 44% to 26% with one client, from 64% to
  35% with unrelated keys, and from 9% to 3.5% on the hot key.
- **With unrelated keys, this host's WAL became the wait.** The commit's
  WAL, fsync and quorum grew by 1.3 ms at about 900 transactions a second,
  and goodput didn't move. On hosted runners the same shape committed
  20–35% more often (`bench/LOCK_BARRIER.md`).
- **On the hot key, the row lock is held for less time.** A transaction's
  second lock call, barrier included, runs while it holds the first row's
  lock, so the queue for that lock shortened with the barrier. On hosted
  runners, whose full barrier is cheaper, the hot key committed about as
  often as before.
- These are single runs on one host. `bench/LOCK_BARRIER.md` has the matched
  comparison: five alternating pairs per shape, with the profiler off.
