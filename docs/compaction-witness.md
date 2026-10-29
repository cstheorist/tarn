# Bounded Vinyl compaction witness (#195)

The [original failed run](https://github.com/cstheorist/tarn/actions/runs/40923920014/job/123624831307)
completed its 3/10/3-second writer windows and exactly reconciled 4,445
acknowledged IDs and 144 refused IDs. All 52 periodic queue readings were
zero. This correctly failed the queued-compaction assertion.

It did perform compaction: the native cumulative `compaction_input` rose
from zero to 57,761,682 bytes, primarily during warmup. The primary index
reported seven completed compactions and the secondary ten. During most
of the limited window, dump and compaction completion counters remained
unchanged while the L0 quota filled. Memory/dump pressure and sampled
queued compaction are distinct observations. The artifact does not show
the queue between samples, so it cannot establish that a queue never
existed. The [retained source-bound series and file hashes](../tests/evidence/compaction-witness/original-40923920014.json)
preserve that failed observation; a later pass does not rewrite it.

In the pinned Tarantool 3.9.0 source,
[`vy_range_update_compaction_priority`](https://github.com/tarantool/tarantool/blob/3.9.0/src/box/vy_range.c)
derives queued input from actual run shape and resets it when no work is
eligible. [`vy_lsm_force_compaction`](https://github.com/tarantool/tarantool/blob/3.9.0/src/box/vy_lsm.c)
marks existing runs and accounts their real bytes. The public
[`index:compact()` implementation](https://github.com/tarantool/tarantool/blob/3.9.0/src/box/vinyl.c)
uses this path. Completed input/time are cumulative counters, while queue
bytes describe currently accounted work. This fixture does not replace
those native metrics or the admission classifier.

The revised fixture prepares a separate SQL table on the pressured shard:
512 random 8 KiB payloads, a completed checkpoint, then 16 additional
payloads and another completed checkpoint. Each insert batch has at most
eight rows. The unequal runs give the test bounded real disk work without
depending on how many natural compactions the timed writers trigger.
Preparation is outside the measured pressure windows, capped at 528 rows
(4.125 MiB of payload), and checked by exact readback. Each checkpoint RPC
retains the existing 12-second client bound and a five-second drain bound.

At the existing 0.5 MiB/s control boundary, one public compaction request
queues those runs. Its immediate receipt records actual index/native
statistics. The independent periodic sampler must still observe positive
seed queue bytes during limited I/O, and the final seed index counters must
show completed compaction input. Exact seed readback is retained alongside
the pressure attempt reconciliation before checking any witness. Both the
old global queue assertion and actual writer-pressure/run-amplification
checks remain required. A historical completion counter alone fails.

The 20,000 timed-attempt cap, phase-local scheduling, all writer windows,
latency bound, owner-leased I/O restoration, bounded drain, uncertain
outcomes and failure-time readback remain in place. No mutation is replayed.
The seed is additional bounded setup, explicitly separate from that cap.
The production SQL/native/Lua protocols and formal inputs are unchanged.

Eighteen offline controls include missing/old queue observations, absent
real completion, wrong I/O phase and corrupted seed readback. A small local
native-engine mechanism probe observed two real runs and 4,338,872 queued
bytes under the limiter; it is not a completed SQL pressure test.
`compaction-witness.yml` runs three fresh fsync fixtures in each native/Lua
mode, with the second fixture pinned to one available CPU. This is RF1
mechanism qualification; ordinary full CI separately retains RF3 faults,
SQL/storage tests, TLA+ and Verus. Final release results remain pending.
