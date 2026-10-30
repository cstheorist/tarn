# READ COMMITTED allocation investigation (#186)

Production execution is unchanged. The baseline must identify a useful
bounded optimization before any default changes. A lower isolated allocation
count does not establish an end-to-end or resource-capacity improvement.

`rc_alloc_probe.lua` counts condition creation through a test-only wrapper
around `fiber.cond` during direct uncontended lock/unlock calls. On main
`f147887`, Tarantool `3.9.0-0-g8b9a0f59a9`, ARM64 Linux in the owned local
container, 10,000 calls allocated 10,000 conditions and 57,774,956 total Lua
bytes. The instrumented loop took 0.109591 seconds. These are RF1 memtx,
Lua-only diagnostic observations, not paired timing or RF3 performance
evidence. The byte count includes all lock/unlock work, not just conditions.

The `READ COMMITTED allocation profile` workflow builds normal Rust 1.98.1
release artifacts and runs two shards, each RF3/fsync, on one CI host.
Memtx/Vinyl and native enabled/disabled have separate results. Each fixture
retains three explicitly labeled warmups and five repeats of uncontended,
eight unrelated keys and eight clients sharing one hot key. A transaction
increments its key and inserts a unique ledger token on the same shard.
There is no retry. Every acknowledged token must appear exactly once with
the expected key, every refusal must be absent, and every per-key counter
must equal its acknowledged effects. Unknown outcomes fail qualification;
the database is retained rather than replayed or destroyed.

Normal timing and sampling are separate phases. Sampling uses the
[LuaJIT guest-stack API](https://luajit.org/ext_profiler.html), retaining at
most 256 distinct stacks, eight frames and 2,048 bytes per stack, plus an
overflow sample count. It samples every 10 ms and stops automatically two
seconds after the planned window if the controller disappears. It writes no
profile files. This does not resolve host C stacks or separate exact engine
and WAL costs; VM-state samples and guest frames are only a coarse profile.
JIT inlining and callback delay also limit source-line attribution.
The complete bounded histogram and exact runtime binary are retained.
TX-thread/process CPU ticks, allocation/GC counters, native/fallback census,
lock/commit waits and all attempts are retained. Metric intervals include
connection setup and diagnostic RPC overhead; they are not exact per-function
allocation measurements. Clock-tick quantization, background work, client
contention and shared-runner variance remain visible limitations. The small
fixture is not a disk-capacity or open-loop overload experiment.

## Completed baseline

[Run 40972150228](https://github.com/cstheorist/tarn/actions/runs/40972150228)
passes all four configurations at compiled merge source
`d4875d8442f0c7d1af698b8e8f746c240ef0ebe2` (PR head `7b1117c`).
The 132 samples include 12 warmups, 60 ordinary measurements and 60 profiled
measurements. An independent audit reconciles 357,376 acknowledged tokens
and five explicit refusals across 357,381 attempts, with exact per-key
effects and no unknown outcomes. All profiler histograms satisfy their
bounds and no watchdog expiry qualifies as a completed sample.

The following ordinary-sample goodput is median [minimum, maximum] of five
five-second repeats, in committed transactions per second. Each mode uses
its own runner; differences between columns or modes are not matched
native-versus-Lua comparisons.

| Engine / native mode | Uncontended | Eight unrelated keys | Eight clients / hot key |
|---|---:|---:|---:|
| memtx / disabled | 398 [255, 442] | 892 [526, 1171] | 405 [257, 503] |
| memtx / enabled | 367 [358, 369] | 895 [885, 900] | 421 [407, 432] |
| vinyl / disabled | 353 [340, 354] | 859 [844, 868] | 407 [390, 416] |
| vinyl / enabled | 322 [321, 357] | 982 [909, 1082] | 366 [310, 405] |

The hot-key fixture reports approximately two combined row-lock/commit
waits per acknowledged transaction, versus zero in the uncontended and
unrelated-key fixtures. Lua allocation medians range from about 40–45 KiB
per acknowledgement across these modes, including diagnostic/background
work. Guest stacks frequently include `box.commit` and freshness barriers;
these samples cannot distinguish C CPU from engine/quorum waiting or
attribute a precise allocation fraction to condition creation.

The [retained archives and manifest](rc-evidence/run-40972150228/manifest.json)
contain all attempts, exact readback, metrics/histograms, logs, configurations
and source/compiler/binary hashes. The original workflow artifacts also
contain the exact Tarantool executable; the repository archives omit that
executable and retain its hash. Reproduce the archive/effect audit with:

```sh
python3 bench/rc_profile_audit.py bench/rc-evidence/run-40972150228/manifest.json
```

The corresponding ordinary CI run `40972150184` passed format/clippy,
TLA+ and Verus, then failed the cluster-backup fixture's `lazy_drain` stage
with an explicit 1213 conflict. Later SQL/fault steps were skipped. The
successful profiling matrix does not override that separate failed gate.

## Condition-allocation hypothesis

An isolated two-line prototype creates the condition immediately before
queue publication instead of creating it for every immediate grant. It
does not modify repository production code. Five alternating direct RF1
memtx/Lua pairs each execute 100,000 checked lock/unlock operations in a
fresh process, yielding every 1,000 operations. Ordinary timings disable
the condition-counting wrapper.

Deferred/eager elapsed ratios are 0.937, 1.009, 0.942, 1.014 and 0.969;
the median is 0.969. Lua-allocation ratios range from 0.952 to 1.005.
Three timing pairs improve and two regress. This does not establish a
repeatable improvement beyond uncertainty or an SQL/RF3 benefit, so the
prototype is not enabled or shipped. The original unbounded loop exceeded
Tarantool's fiber slice; that failure and the bounded pairs remain in
[the direct evidence](rc-evidence/direct-20261030/summary.json), with the
probe, prototype diff and source hashes. This local experiment does not
qualify the issue's required durable paired measurements.

`rc_compare.py` extends this hypothesis to an isolated RF3/fsync pilot. It
copies the Lua/fixture scripts into an owned temporary directory, applies
only the two-line allocation change to the candidate copy, and retains both
source hashes. Five alternating pairs use the same release binaries,
configuration and exact-accounting worker, with separate ordinary warmups
and all three contention shapes. Each lane starts fresh and must fully
stop before the next starts. Attempts, partial failures and both fixture
states are retained; failed samples are never restarted. Timing disables
the condition wrapper and additional lock/yield debug checks in both lanes.
This pilot cannot qualify a production change without the separate
concurrent/differential and fault gates, and it does not alter production
code or enable an optimization.

[Run 41052637120](https://github.com/cstheorist/tarn/actions/runs/41052637120)
completed all four modes at compiled merge source
`fbff68976e97cc149db87088c648c0fbe1743b4a` (PR head `099ce1a`). Independent
audits reconcile all 528,527 paired-pilot attempts as acknowledged exactly
once, with no refusals or unknown outcomes. The 240 samples comprise 120
warmups and 120 ordinary measurements. The same run's separate baseline
contains 132 samples, 381,799 acknowledgements and four explicit refusals;
all ledger/per-key effects and bounded profiler histograms pass.

The table gives deferred/eager goodput ratios as median [minimum, maximum]
of five alternating pairs. Above one favors deferred allocation. Every
observed range crosses one; no consistent end-to-end improvement is
established. The native memtx and Lua Vinyl lanes are especially close to
parity. Keep production allocation unchanged.

| Engine / native mode | Uncontended | Eight unrelated keys | Eight clients / hot key |
|---|---:|---:|---:|
| memtx / disabled | 0.757 [0.484, 1.065] | 0.695 [0.499, 1.429] | 0.810 [0.695, 1.681] |
| memtx / enabled | 0.996 [0.981, 1.018] | 0.995 [0.973, 1.001] | 0.994 [0.991, 1.003] |
| vinyl / disabled | 1.000 [0.991, 1.029] | 0.991 [0.979, 1.018] | 1.014 [0.951, 1.020] |
| vinyl / enabled | 1.046 [0.855, 1.478] | 1.046 [0.843, 1.116] | 0.981 [0.779, 1.546] |

The [independent audit](rc-evidence/run-41052637120/paired-audit.json) retains
every pair's latency, TX CPU, Lua allocation and goodput, including all
regressions. Median TX CPU ratios range from 0.991 to 1.090 and median
Lua-allocation ratios from 0.990 to 1.015 across the twelve groups.
Measurements include diagnostic/background work and clock-tick quantization;
these short, closed-loop, fresh-process samples do not establish a fixed-p99
capacity gain or an independent-storage-host result.

[Archives and the source-bound manifest](rc-evidence/run-41052637120/manifest.json)
retain all attempts, metrics, exact readback, copied sources, logs and
configuration. The original workflow artifacts also contain the exact
Tarantool executable and complete paired fixture storage state; their hashes
and sizes remain in the manifest. The initial collector wrote each final
session's samples before flipping its `complete` flag. The enclosing final
receipt has the flag set. Both original files remain unchanged, and the
independent audit checks all samples plus their equivalence. The collector
now persists the flag in both files for future runs.

```sh
python3 bench/rc_profile_audit.py bench/rc-evidence/run-41052637120/manifest.json
python3 bench/rc_compare_audit.py bench/rc-evidence/run-41052637120/manifest.json
```

Ordinary CI `41052637085` separately passes format/clippy, TLA+, Verus and
memtx rolling upgrade, but Vinyl rolling upgrade fails while seeding a
prepared transaction with an explicit 1205 admission refusal. The profile
does not override this failure, certify the full release, or qualify a
production optimization.

The repeated [run 41060171963](https://github.com/cstheorist/tarn/actions/runs/41060171963)
also completes all four modes at compiled merge source
`3bbb3f893327cfa9930d9e018b17cdd59f7b3450` (PR head `88ac124`). Its independent
audits reconcile 388,946 baseline acknowledgements and six refusals, plus
635,610 paired-pilot acknowledgements with no refusals/unknowns. The 132
baseline and 240 paired samples remain separate from the earlier source.
Every one of its twelve goodput-ratio ranges also crosses one. An extreme
native-memtx unrelated-key pair falls to 0.053, with p99 increasing from
13.9 ms to 401.9 ms; that sample remains valid exact-accounting evidence
and is retained without trimming or attributing its cause to the host.
The [complete archives and audit](rc-evidence/run-41060171963/manifest.json)
preserve these regressions. This reinforces the decision to keep the
condition-allocation candidate experimental. It contains no plan-guard
batching measurements.

## Further allocation census

`rc_guard_probe.lua` measures unchanged public freshness helpers on one
RF3/fsync shard in the owned ARM64 fixture. Five rotated 1,000-call samples
yield every 100 calls. Median Lua bytes/call are 0.832 for an empty control,
2,664.832 for `box.info.replication`, 3,032.832 for `fresh.mode`, 3,232.832 for
`fresh.controls` and 3,328.856 for `fresh.native_controls`. These are helper
invocations with diagnostic/background overhead, not exact fractions of
transaction allocation or CPU. The [raw census and receipt](rc-evidence/guard-20261030/manifest.json)
identify a narrower investigation target. They justify neither caching
membership/space guards nor eliding any request-specific quorum barrier.
The owned fixture is stopped and production guards remain unchanged.

### Final plan-guard hypothesis

`BK.fresh_plans` runs inside the existing nonyielding final commit check.
It checks control spaces, then rebuilds the same replication-mode metadata
for every planned row. An isolated candidate keeps one mode value local to
that invocation while still checking every planned user space. It preserves
clock/prefix barriers, all control-space checks and the writable check;
it never carries a mode through a yield or into another invocation. No
production source is changed. `rc_compare.py --hypothesis plan-guards`
reproduces the two copied-source edits with scalar/batched lanes.

The model precedes this candidate. `PlanGuardScalar` and `PlanGuardBatch`
make identical decisions in 48 distinct states each. Broken controls that
reuse mode through a yield or omit a planned space violate
`NoUnsafeAcceptance` at depths four and two. This proves only a finite local
guard equivalence under the nonyielding premise, not whole-code refinement,
freshness, locking, durability or liveness. Those contracts remain unchanged
and require their existing gates before any production proposal.

Five alternating helper-only pairs on RF3/fsync give the following median
batched/scalar ratios. Each sample contains 1,000 calls; both paths use the
same process, data, pre-timing GC and cooperative yield every ten calls.
All individual pairs remain in [the raw receipt](rc-evidence/guard-batch-20261030/summary.json).

| Planned rows | Elapsed ratio | Lua allocation ratio |
|---:|---:|---:|
| 1 | 0.712 | 0.516 |
| 2 | 0.525 | 0.348 |
| 8 | 0.196 | 0.118 |
| 32 | 0.057 | 0.032 |

Both paths refuse a changed unsupported mode between invocations and an
asynchronous space at the first or last position. The isolated candidate
passes all 415 existing storage checks on three RF1 shards, native disabled,
including both engines and lock/yield assertions. These are helper costs
and storage controls, not transaction throughput, independent-host evidence
or complete SQL/RF3 fault qualification. The four-mode RF3/fsync end-to-end
pilot is the next gate; its result is pending. The condition-allocation
candidate and its negative results remain separate evidence.

## Retained collector failure

[The first run](https://github.com/cstheorist/tarn/actions/runs/40963530299)
used full platform call-chain profiling. Its memtx/Lua fixture completed
15 ordinary timing samples with exact accounting, but the first profiled
window emitted a 48,746,177,842-byte trace. The original leader became
unresponsive and a follower was elected. The diagnostic RPC then failed;
the original collector did not persist that unfinished sample's attempts.
Those effects remain unaccounted for in this receipt. Later repeats also
failed; no complete performance baseline or production-crash explanation is
claimed. The original evidence, compressed-artifact hash and successful
partial samples remain in [the failure receipt](rc-evidence/original-40963530299.json).

The collector now persists attempts before reading process/profiler metrics,
attempts independent SQL readback even when diagnostics fail, and retains
diagnostic stderr. Guest-stack profiling replaces the unbounded host trace;
it is a narrower measurement with explicit memory/time bounds. Neither this
change nor a subsequent successful profile would explain #160's unrelated
historical native GC crash.

The pinned Tarantool constructor allocates Lua userdata, initializes the
wait-list and attaches its metatable; the inspected constructor and
`fiber_cond_create` contain no fiber-yield call ([Lua binding](https://github.com/tarantool/tarantool/blob/8b9a0f59a9/src/lua/fiber_cond.c),
[core initialization](https://github.com/tarantool/tarantool/blob/8b9a0f59a9/src/lib/core/fiber_cond.c)).
Treating creation through queue publication as non-yielding remains an
implementation premise of the local model, including LuaJIT allocation/GC
behavior. The isolated candidate also passes 415 existing storage protocol
checks on three RF1 shards with native disabled, including both engines,
waits, upgrades, NOWAIT/SKIP LOCKED, deadlocks, timeouts and memory pressure.
The [source-bound receipt](rc-evidence/direct-20261030/summary.json) records
this control; it does not replace the required SQL/RF3 fault qualification.

The focused `WaiterAllocation` model precedes implementation. Eager and
deferred allocation both preserve a ready condition before a request is
published to callbacks. The broken control delays allocation until after
publication and exhibits a missed signal. This is a bounded local refinement
premise, not a new proof of FIFO, deadlocks, upgrades, storage freshness,
durability or liveness. Those remain covered by existing models and live
contracts when an implementation is proposed.

Pending work: separate remaining CPU/allocation and engine/WAL costs more
precisely, select a useful bounded optimization with an explicit matched
control, preserve all semantics and
failure boundaries, and run at least five alternating paired measurements
plus the required concurrent, model, Verus and RF3 fault gates. #186 remains
open until its acceptance is demonstrated.
