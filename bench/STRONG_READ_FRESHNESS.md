# Strong-read freshness and matched cost (#130)

This study compares Lua and native read paths under the same strong-read
contract at revision `554ef08dee261e7e9a4e109a7fc299e7fa7ce0ed`. All 72 phases
and 36 balanced pairs completed in one Linux job. The earlier complete `62cf4c6`
study remains a separate checkpoint; its samples are not pooled here.

## Results

Each cell reports the median of six native/Lua ratios and the observed range.
Lower CPU/p99 and higher operation rate favor native. Every one of the 36 pairs
had lower native leader TX CPU and p99, and higher operation rate. These are
within-cell component comparisons.

| Cell | Leader TX CPU per operation ratio | Operation-rate ratio | p99 ratio |
| --- | ---: | ---: | ---: |
| direct-rf1 B1/C1 hits | 0.625 (0.608–0.651) | 1.146 (1.121–1.161) | 0.733 (0.708–0.770) |
| direct-rf1 B128/C64 hits | 0.387 (0.359–0.398) | 2.586 (2.515–2.783) | 0.350 (0.309–0.383) |
| direct-rf3 B1/C1 hits | 0.851 (0.844–0.853) | 1.083 (1.076–1.097) | 0.763 (0.759–0.853) |
| direct-rf3 B128/C64 hits | 0.409 (0.398–0.438) | 2.372 (2.219–2.438) | 0.472 (0.427–0.509) |
| sql-rf3 B32/C1 hits | 0.725 (0.713–0.735) | 1.098 (1.085–1.120) | 0.821 (0.804–0.877) |
| sql-rf3 B32/C1 miss | 0.844 (0.839–0.854) | 1.059 (1.018–1.081) | 0.873 (0.822–0.894) |

The separate per-mode native/Lua leader TX CPU medians, in microseconds per
operation, are 24.3/38.8, 107.7/285.5, 114.2/134.0, 142.4/346.8, 143.0/196.9,
131.5/156.3 in table order. Direct operations are storage RPC batches; SQL
operations are statements. They are different capacity units.

## Contract and proof boundary

Both paths establish a fresh quorum-confirmed prefix before first data access
using Tarantool's public linearizable isolation setter on an empty transaction.
It collects fresh peer vclocks, catches up WAL and settles the synchronous
queue. Autocommit discards only that empty transaction; a first managed stream
retains its exact transaction and MVCC read set. Replicated completion obtains
another prefix in a separate fiber before validating the original view. Unmarked
ambient transactions are refused unchanged. Native ABI14/392 owns CALL input
across waits and uses a protected public Lua C API bridge.

Memtx and Vinyl use the pinned empty-barrier/confirmed-view composition; direct
Vinyl LINEARIZABLE access is unsupported. Raw native reads retain local
committed visibility. Opt-in lazy indexes can omit recent matches.
[Design](../docs/design.md),
[operations](../docs/operations.md#strong-reads-and-storage-rollout) and
[verification](../docs/verification.md#strong-read-freshness-stream-provenance-and-locking-replies-issue-130)
describe engine restrictions, stable quorum administration and runtime trust
boundaries. Administrative changes must preserve guarantees through every
in-flight synchronous acknowledgement.

Immutable timing policy and relevant-writer certification fence uncertain new
votes and GC. High-water updates and deletions share a transaction. Existing
receipts and decided completion remain finishable, retaining inherited
timestamps/expiry horizons. Non-idempotent mutations are never automatically
replayed after outcome uncertainty; idempotent protocol recovery can redeliver
requests. Lost mutation replies preserve 40003 semantics.

Accepted clock corrections remain inside the unchanged elapsed-time drift
budget; monotonic reversals fault and logical wall time never decreases. The
expiry argument requires relevant storage and timestamp issuers to stay within
one fixed common reference band of width configured skew over history. Current
pairwise agreement cannot establish that external premise. Neither the models
nor the shared executable arithmetic prove clock quality, the C/Lua API mapping,
native MVCC/WAL implementation or complete runtime refinement.

## Method

B is keys per request and C is concurrent workers. Direct C64 uses one physical
IPROTO connection with concurrent requests. SQL C1 uses one workload client.
Every cell has six paired trials, three Lua-first and three native-first, with
case order shuffled per trial and pair order alternating by trial and stable
case index. Each phase has one warmup second and five timed seconds.

The fixture holds 100,000 committed memtx rows with composite primary key
`(tenant_id,id)`, tenant 7 and 64-byte payloads on one logical shard. Exact
tuple/corpus hashes and replica catch-up are checked before and after. Direct
RF1 uses WAL `write`; RF3 uses quorum 2, synchronous spaces and WAL `fsync`.
These are separate cohorts; subtracting their results would not isolate
replication or freshness overhead. SQL modes use the same release gateway with
only native batch selection changed. Direct fixtures retain two idle gateways.

Both paths load the same allocator-instrumented native module, enable lock-copy
auditing, retain normal LuaJIT JIT/GC and disable test hooks. Leader
transaction-thread CPU includes common observer/probe overhead; idle work is not
subtracted. It does not aggregate peers, WAL threads or gateway CPU. Direct
client CPU and IPROTO bytes are retained; SQL results do not establish gateway
CPU or wire bandwidth. Lua-managed and Rust allocator-request counters have
different scopes and exclude C/tuple allocation and RSS.

The runner reports four CPUs, affinity 0–3, 16,367,528 kB total memory and Linux
7.0.0-1017-azure x86_64/glibc2.39. Each storage replica is capped at two CPUs
and 1 GiB, with 256 MiB memtx and 32 MiB Vinyl memory. RF3, two gateways and
clients can oversubscribe that runner. The builder is gone before timings; all
three cohorts run sequentially. Physical-host isolation and production capacity
are not established.

## Scope

This short, instrumented hot-corpus read study does not establish sustained
production capacity, steady-state GC/compaction, overload behavior, SSD-scale
results, durable-write or distributed-transaction throughput, or a
TiDB/YugabyteDB ranking. The comparison keeps one freshness guarantee and one
binary/fixture per cell; it does not isolate the cost of one loop or of
replication alone.

## Functional and formal evidence

The [measured revision's
CI](https://github.com/cstheorist/tarn/actions/runs/40827360444) passed all four
jobs. Its actual checkout `5eeab61b3c6b724ed2c6bc5fe859ecb942a5cbde` has the
same Git tree as `554ef08`. The full registered matrix executed 252 cases: 67
positive checks and 185 expected sensitivity/tradeoff outcomes. All 317 proof
inputs are source-bound; Verus reports 140 verified, zero errors. These are
bounded contracts/shared executable-body proofs, not unique end-to-end
properties.

The current local native suite passed 1,806 printed checks across 61 sections,
with all 67 frozen Lua files matched to the committed source. The yielding-input
fixture passed 32 checks with exact payload/native-service assertions. Rust
units cover 38 default / 41 instrumented cases and 18 erased-body checks. The
Linux CI additionally exercises native/Lua storage, MariaDB differential tests,
transactions/READ COMMITTED, DDL/lazy delivery, lost replies and coordinator
crashes, moves, joins, growth/shrink and both-engine RF3 failover.

A withdrawal audit found that decision-only counters could report zero while a
survivor still needed a departing participant. The fix fences supported new
promises on every draining dependency, tracks their complete
local/decision/participant scope through confirmation, and counts all unresolved
PREPARED/STAGING references. Recovery still obtains authoritative
missing-participant fences. The `DepartureDependencies` positive and five
controls cover orphaned receipts, late admission and conservative
pending-confirmation accounting. Earlier pending promises may confirm after
draining acknowledgements while their counters keep removal blocked.

The immutable `6984b45` baseline independently reproduces the count gap in both
engines: shard 3 has a PREPARED/STAGING transaction with decision shard 3 and
participants `{2,3}`, shard 2 is aborted with no locks, and every removal
counter reports zero. It records 14 passing / 15 expected failed assertions
without a fixture exception. The reviewed fixed harness passes 63 checks in both
native-enabled and Lua-only modes, including actual recovery, zero-count/removed
acknowledgements, controlled node-2 loss, survivor strong reads/bucket fences
and decided completion after a final latched clock fault. PREPARE remains Lua
even with the native module enabled. The local harness uses Rust's public
removal gates; actual SQL withdrawal also passes in CI. An entry-held final
refusal combines membership and clock fencing; no forced runtime pending-WAL
fault witness or exact reconstruction of the historical CI transaction is
claimed.

Earlier source-bound both-engine RF3 pause/asymmetric-link/repeated-leadership
witnesses retain their original revisions and scope. They show raw stale values
after replacement ACKs and strong-path refusal on a known-read-only former
leader. Modeled pre-fencing controls and positive live fences are separate
evidence; no live fractured-commit reproduction is claimed.

## Retained checkpoints and failures

The earlier complete `62cf4c6` 72/36 study remains whole and byte-identical
inside the new archive; its samples are excluded from every current statistic.
Its scalar files remain under [the earlier
index](evidence/strong-read-freshness/index.json). Historical ABI11 results in
[NATIVE_BATCH.md](NATIVE_BATCH.md) retain their original unbarriered scope. The
whole ABI13 checkpoint also remains historical; the public C API correction is
not a demonstrated causal fix for the earlier LuaJIT SEGV.

Excluded local ARM studies include strict-clock refusals and incomplete cohorts.
One later RF3 attempt correctly latched closed after raw wall advanced
712,247,099,861 ns while monotonic advanced 76,999,708 ns. No platform clock
cause is inferred, and no timing bound or fault state was relaxed. Two earlier
Linux attempts remain separate: one completed RF1 but failed
container-owned-data cleanup with no uploaded verified WAL export; another
completed RF1/cleanup but used the ARM version abbreviation in an AMD64
qualification assertion. Neither supplies samples to this study.

Retained correctness failures include missing relevant-writer test readiness, a
stale native-write cache precondition, an alias replay test observing an
unlogged synchronous entry, and the `6984b45` withdrawal failure. A
delayed real clock-loader control reproduces the alias assertion while the
replays remain read-only; it does not identify the unlogged historical writer.
Its startup fix retains original deadlines, the live worker and a stricter
zero-metadata-write assertion. The withdrawal baseline/control source, three
nonqualifying startup/oracle discoveries and earlier controller versions remain
separately labelled. Direct balance totals in the failed grow/shrink run did not
establish settled protocol state. Later validation evidence preserves the alias
failure/controls separately from the measurement archive.

## Reproducibility and publication

[Summary](evidence/strong-read-freshness-v2/summary.json), [all 72
phases](evidence/strong-read-freshness-v2/phases.csv), [all 36
pairs](evidence/strong-read-freshness-v2/pairs.csv) and
[index](evidence/strong-read-freshness-v2/index.json) bind the current results
to exact source, hashes and scopes. The [manual
job](https://github.com/cstheorist/tarn/actions/runs/40827426428) built all
three release artifacts from the same checkout with pinned AMD64 Rust 1.98.1
Bookworm and Tarantool 3.9.0 images. Their exact image/binary pair was exercised
in every phase; no cross-build binary reproducibility is inferred.

The [current data-only
archive](https://github.com/cstheorist/tarn/releases/download/bench-strong-read-freshness-130-v1/strong-read-freshness-130-linux-v2.tar.gz)
is **58,955,738 bytes**, SHA-256
`3a9eb9d6ce65e828e7264386d8637a7f738d485c38898b293522b8fd0197c250`, with **621
regular outer members**. It contains current raw observations,
source/build/harness identities, scoped formal/runtime gates and failure
history, plus the whole earlier **48,388,800-byte** checkpoint archive, SHA-256
`a1aa3e0e8906831596097abc73c0f5da3156e6147c6280aebc96ec6a023e2f82`. No samples
are pooled. Original code/raw observations retain bytes; JSON metadata path
redactions preserve parsed types/numbers and record original/public hashes in
FILES.csv. Product binaries, vendor trees and WAL contents are omitted. Private
Actions artifacts retain verified closed WAL where available; the first Linux
permission failure is explicitly incomplete.

The immutable data packet captured successful Tests/Verus/lint jobs while its
model job was still running. It therefore preserves an explicitly scoped
246-CI-plus-6-local formal union and does not assert later all-four-job results.
The measured source subsequently passed all four jobs as linked above.
Publication and merge additionally require the final report head's exact tested
tree and complete all-four-job CI.
[validation-attestation.json](https://github.com/cstheorist/tarn/releases/download/bench-strong-read-freshness-130-v1/validation-attestation.json)
and [supporting validation
evidence](https://github.com/cstheorist/tarn/releases/download/bench-strong-read-freshness-130-v1/final-validation-evidence.tar.gz)
record that later gate; the measurement archive does not invent it.
