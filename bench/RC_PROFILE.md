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
code or enable an optimization. Its results remain pending.

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
