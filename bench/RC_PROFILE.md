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

Normal timing and sampling are separate phases. The latter uses full guest
and host call chains from Tarantool's
[platform profiler](https://www.tarantool.io/docs/tarantool/3_x/tooling/luajit_sysprof),
with raw profiles and the exact runtime binary retained for interpretation.
TX-thread/process CPU ticks, allocation/GC counters, native/fallback census,
lock/commit waits and all attempts are retained. Metric intervals include
connection setup and diagnostic RPC overhead; they are not exact per-function
allocation measurements. Clock-tick quantization, background work, client
contention and shared-runner variance remain visible limitations. The small
fixture is not a disk-capacity or open-loop overload experiment.

The focused `WaiterAllocation` model precedes implementation. Eager and
deferred allocation both preserve a ready condition before a request is
published to callbacks. The broken control delays allocation until after
publication and exhibits a missed signal. This is a bounded local refinement
premise, not a new proof of FIFO, deadlocks, upgrades, storage freshness,
durability or liveness. Those remain covered by existing models and live
contracts when an implementation is proposed.

Pending work: complete and interpret the release baseline, select a bounded
optimization with an explicit matched control, preserve all semantics and
failure boundaries, and run at least five alternating paired measurements
plus the required concurrent, model, Verus and RF3 fault gates. #186 remains
open until its acceptance is demonstrated.
