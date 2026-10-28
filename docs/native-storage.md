# Native storage procedures

Tarn uses a hybrid storage layer: native Rust procedures for eligible hot
operations, with Lua retaining transaction ownership, waiting, recovery and
administration. The native code calls Tarantool's module API and returns
stored tuples as MessagePack. It uses the same memtx or Vinyl engine,
replication and WAL as Lua; changing the procedure language does not
remove those costs.

[Issue #28](https://github.com/cstheorist/tarn/issues/28) is closed. It
introduced native reads, autocommit writes, one-shard commits and read-only
validation, followed by narrower key/range lock checks and stream guards.
Its historical profiling and matched results are described in
[design §1.5](design.md#15-native-procedures) and
[the benchmark results](../bench/RESULTS.md). Those measurements apply to
their recorded builds and workloads. They do not measure every remaining
Lua procedure or establish a benefit from rewriting the whole storage
module.

## Current execution paths

The exported procedures and their eligibility checks are in
[`procs.rs`](../crates/tarn-storage/src/procs.rs). Unsupported operations
return `['lua']` before changing the operation's transaction effects; the
gateway then calls the corresponding Lua procedure. Native writes stop
being eligible for handback once committing begins. A lost mutation reply
retains an unknown outcome and must not trigger mutation replay.

| Responsibility | Current implementation and boundary |
|---|---|
| Ordinary reads | `ts_select` serves eligible memtx points and bounded ranges, including supported stream reads/begins. Continuations, Vinyl and requests requiring refresh/waiting retain their Lua path. Existing in-stream IPROTO SELECTs already bypass Lua for suitable reads. |
| Full-primary-key batches | The optional eight-argument `ts_select` form performs up to 128 memtx index gets and copies their tuples directly. Its narrower contract is below. Other `tarn_select_keys` shapes remain Lua. |
| Autocommit and one-shard commit | `ts_autocommit` and `ts_commit_local` serve their supported memtx writes. Checks cover routes, definitions, claims, read sets and native exclusions. The Lua procedures retain unsupported shapes and operations with lazy capture, online-copy capture or row-lock ownership requirements. |
| Read-only validation | `ts_validate` checks eligible transactions. Stream identity, term/DDL watermarks and membership remain part of its contract; it is not a replacement for all transaction coordination. |
| Counts and predicates | `tarn_count` remains a Lua wrapper. A predicate-free equality/whole-index count already calls Tarantool's `idx:count`; predicates, exclusions for own writes and end bounds can require Lua tuple iteration. Porting the wrapper alone need not improve the underlying count. |
| Co-located joins | `tarn_select_join` gathers dependent primary-prefix rowsets under one checked read. Rust reconstructs joins and overlays eligible own writes. Union read sets, empty-prefix phantoms, DDL history and incomplete-result fallback remain relevant. |
| READ COMMITTED ownership | `tarn_lock`/`tarn_lock_range`, waiter queues, lock upgrades, session cleanup, deadlock handling and lock-generation checks remain Lua. The native lock copy stores counts/ranges used to decide eligibility; it does not replace ownership or the waiter protocol. `tarn_commit_rc` remains Lua. |
| Distributed writes | PREPARE/APPLY/ABORT, durable receipts, decision recovery and record GC remain Lua, with Rust coordinating participants. A duplicate terminal receipt must certify confirmed state. |
| Lifecycle and control work | Catalog/DDL jobs, materialization/undo, engine copies, bucket moves, membership/JOIN, lazy-index shipping/GC/capacity and startup/role refresh remain Lua. They supply state that native operations must trust only while current. |

The Lua entry points and shared read/commit checks are in
[`tarn.lua`](../lua/tarn.lua). The lifecycle modules are
[`tarn_engine.lua`](../lua/tarn_engine.lua),
[`tarn_lazy.lua`](../lua/tarn_lazy.lua),
[`tarn_lazy_meter.lua`](../lua/tarn_lazy_meter.lua) and
[`tarn_vinyl.lua`](../lua/tarn_vinyl.lua). The SQL dispatch and buffered
transaction behavior remain in
[`exec.rs`](../crates/tarn-sql/src/exec.rs) and
[`txn.rs`](../crates/tarn-sql/src/txn.rs).

## Bounded primary-key batch prototype

Set `native_batch_reads: true` in the SQL server's cluster configuration to
opt in. It defaults to `false`; ordinary native reads/writes remain enabled
independently. The shard still needs its native module loaded through the
existing `TARN_NATIVE` setup.

The supported storage operation has no ambient transaction, does not begin
a stream, checks locks, reads index 0 with complete primary keys and has no
continuation. It is memtx-only. Each requested key costs one lookup,
including misses and duplicates. Present tuples retain their input order
and multiplicity; missing keys contribute no row. Raw physical tuples keep
the existing SQL decoder for layouts and instant-column fills.

The whole batch must fit the work, row and stored-tuple-byte caps. Before
each lookup all budgets must have room; before appending a present tuple
its size must fit. Exact exhaustion on the final key is permitted. An
oversized tuple or a later key after exhaustion hands the **whole** request
to Lua; native code never certifies a clipped prefix as complete. The byte
budget counts stored tuple bytes, with bounded reply framing separately.

Before any get, the native path checks every requested key against prepared
write claims, including a prepared insert at a currently missing key.
Read-only claims do not block snapshot reads. Readiness, writable role,
route/membership, definition/DDL freshness, held tables and load-count
windows must also permit serving. The get/check interval does not yield;
all rows and claims therefore refer to one memtx state under the existing
native-read premises.

The existing twelve-argument `ts_select` operation and State ABI 11/312 are
unchanged. An empty argument list returns only the versioned
`['native_batch_keys', 1]` capability; it certifies code support, not
leadership, readiness or data. Older modules hand that shape back. The
gateway caches support or absence per connection, preserves existing
native paths, and uses the same Lua batch request for per-operation
handback. Transport errors do not become an automatic second read call,
transaction restart or mutation replay.

## Correctness and measurement scope

[`NativeKeyBatch.tla`](../tla/NativeKeyBatch.tla) checks ordered duplicate/
miss behavior, one-image results, pre-existing acknowledged writes and
whole-request fallback. Its three broken controls remove missing-key claim
checks, allow a writer between gets, or certify a truncated result. The
executable [`batch.rs`](../crates/tarn-verified/src/batch.rs) kernel is used
before lookups and appends; Verus proves its exact budget transitions and
overflow safety. The
[verification document](verification.md#bounded-native-primary-key-batches-issue-141)
records gates and scope.

These proofs compose with existing lock-copy, stream and commit contracts.
They do not prove Tarantool's C implementation, the unsafe FFI boundary,
MessagePack-to-index correspondence, absence of CAPI yields or the Lua
mirror's every update. Functional tests compare raw Lua/native results,
exercise claims and handbacks, retain an existing stream's read set and
require served counters for concurrent coherent-pair reads. Passing those
tests is bounded evidence for the implementation premises.

Performance comparisons must use the same source, native artifact,
gateway, data, resource limits and repeated workload windows, switching
only batch dispatch. Warm capability discovery outside the timed window.
Record served/fallback counts, CPU, latency, throughput and allocation
scope; a native path that silently falls back is a different experiment.
The [batch runner](../bench/native_batch.py) retains this experiment
separately from earlier native-procedure results. The [completed paired experiment](../bench/NATIVE_BATCH.md) publishes
CPU, allocation, latency and rate results, including SQL regressions,
all raw trials and immutable build receipts.

The optional `tarn-storage` Cargo feature `alloc-metrics` defaults off.
The one-argument `ts_select(["native_batch_allocs"])` diagnostic reports
six cumulative module-wide Rust allocator counters;
it covers all Rust paths in that module, not only batches. Successful
reallocation counts the new requested capacity as allocated and the old
capacity as freed, even when resized in place. Failed requests do not
change the counters. The observer encodes into a fixed stack buffer;
returning it may allocate in Tarantool. The independently relaxed counters
wrap at `u64` and are not one atomic snapshot. They exclude Lua allocations,
Tarantool C/tuple allocations and process RSS. Compare both modes using the
same instrumented build and sample between workload windows.

## Engineering recommendation

Keep the hybrid boundary while measuring complete workloads. Small native
kernels can remove Lua table/GC work on a demonstrated hot path without
duplicating the whole coordinator, waiter and lifecycle machinery. The
selected batch operation is a bounded addition to that boundary; it does
not establish that batches dominate a transaction-heavy workload.

A future native APPLY kernel could run inside a Lua-owned wrapper that
retains confirmed receipt/decision checks, definition and route fences,
lazy capture/capacity obligations and release after confirmed completion.
Unsupported cases must be recognized before effects, or rolled back while
preserving the original stream/read set. No fallback is allowed after
commit begins. Before implementing it, measure APPLY's actual share,
model its ownership and durability boundaries, verify the reusable kernel
and test cancellation, duplicate/late requests, quorum rollback, engine
changes and lazy-index effects. Moving READ COMMITTED ownership into native
code would additionally require modeling queues, wakeups, session loss,
upgrades and deadlocks; a copied lock count is insufficient for that job.

Maintain one semantic reference and explicit eligibility rules, shared
verified arithmetic/encoding bodies where practical, ABI compatibility
tests and paired Lua/native fault tests. Expand the native subset when
measured benefit and its validation cost support the change.
