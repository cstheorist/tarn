# Bounded movement evidence

This directory records the bounded-movement implementation and its acceptance checks. It separates fixed-configuration measurements from development diagnostics. All data is synthetic and all clusters were owned by the fixture. These are same-host RF3/fsync runs, not independent failure domains or a production capacity estimate.

The final scale study uses identical Rust dev-profile binaries (optimization level 0, debug symbols disabled), deterministic 8 KiB payloads, a 16 MiB shared Vinyl memory pool, zero Vinyl cache, 512-row/1 MiB page limits, two movement slots, 64 KiB of retained dirty hints per move (128 KiB total), and 256 MiB/512 MiB per-move/aggregate disk staging limits. Each cohort runs twice in fresh directories. A 32 MiB moving bucket exceeds the staging pool; 128 MiB is four times that payload at unchanged limits. The concurrent cohort moves two separate 32 MiB buckets and records two simultaneous reservations.

RSS includes user tuples, indexes, Vinyl metadata, Lua/transport buffers, replication, WAL and allocator overhead. The pool and retained-page limits are not a whole-process RSS limit. Disk figures sum allocated blocks in all six owned data directories, including source data and retained WAL. Logical staging cleanup does not imply immediate LSM file deletion or WAL/snapshot garbage collection. WAL figures group decoded/re-encoded records by transaction identity; they exclude physical xlog framing/compression and are not the size of a combined WAL syscall.

Procedure wrappers record fence/send/receive boundaries without changing their implementation. Sampled boundaries are retained as a separate fallback, not substituted for exact procedure intervals. High-water RSS includes loading. Throughput covers the move's observed completion interval; loading and final exact-data inspection are outside that interval.

The development archive preserves results, configuration and logs from unsuccessful attempts and exploratory runs. They are excluded from the matched comparison: initial administrative bridge startup; load-pressure burst refusal; a backward-clock safety refusal; a report encoder bug; a late read-only metrics probe after successful movement; an invalid expectation that a no-op UPDATE creates a lazy event; warm-pool refusal after handoff; a one-shard prepared fixture incorrectly naming a second clock peer; a traffic fixture that did not classify a confirmed stale-schema refusal; and backup readiness using Vinyl's estimated length after staging deletion. It also retains earlier successful fault and migration drills, engine-completion diagnostics, and the earlier compressible-payload measurement. The formal `initial/` directory retains the model-edit and memory-exhaustion failures. No failed run is relabeled because a later attempt passed.

The final contract is in [bounded movement](../../docs/bounded-moves.md), with [maintenance migration](../../docs/upgrades.md) and [proof boundaries](../../docs/verification.md). Restore/downgrade tests use an exact separately built format-1 predecessor and immutable build receipts; no marker is edited to bypass admission.

## Matched fixed-memory results

Two fresh runs per cohort, native dispatch, Vinyl user data. Ranges are observed minima–maxima, not confidence intervals. Receiver RSS is the largest process high-water mark among its three replicas; it is not a sum of nonconcurrent peaks.

| Payload / active moves | Move seconds | Payload MiB/s | Receiver RSS MiB | Staged MiB | Allocated disk MiB | Largest WAL transaction MiB |
|---|---:|---:|---:|---:|---:|---:|
| 32 MiB / 1 | 6.17–6.42 | 4.99–5.19 | 130.48–134.82 | 32.08 | 343.00–361.68 | 1.017 |
| 128 MiB / 1 | 62.10–62.98 | 2.03–2.06 | 129.13–143.96 | 128.31 | 1270.12–1449.20 | 1.017 |
| 2 × 32 MiB / 2 | 12.08–12.35 | 5.18–5.30 | 142.39–162.96 | 64.15 | 632.47–692.96 | 1.017 |

| Payload / active moves | Longest write fence s | Longest unavailable interval s | Source reclamation s | Target install and logical cleanup s |
|---|---:|---:|---:|---:|
| 32 MiB / 1 | 4.64–4.78 | 4.64–4.78 | 0.91 | 3.73–3.87 |
| 128 MiB / 1 | 57.50–58.36 | 57.50–58.36 | 4.51–4.55 | 52.99–53.81 |
| 2 × 32 MiB / 2 | 9.94–10.13 | 9.94–10.12 | 1.80–1.98 | 8.14 |

All six runs verify every deterministic payload exactly once at the destination, no source payload remaining, empty owned staging/progress/job state, and observed batch limits. Both concurrent runs observed two active moves. The larger cohort completed at fixed limits, with a longer fence and lower throughput; bounded work does not imply a short cutover.

[Readable run index](fixed-memory-index.json) and [complete result/config/process-log archive](fixed-memory-runs.tar.gz) bind these observations to source `7d70930`, binary hashes and the unchanged limits. [Development index](development-index.json) and [development archive](development-runs.tar.gz) retain the earlier results and failures. [Formal registry](formal-registry.json) records exact regeneration of 296 configs plus ten maintained configs; the 293 default outcomes are checked separately.

## Concurrent traffic and faults

All 18 RF3/fsync cases passed on source `37ee46b`: native and Lua dispatch for
memtx, Vinyl and mixed user tables, plus each of six faults with mixed tables.
The faults kill/restart a source leader, receiver leader or coordinator; drop an
executed page reply; refuse movement for insufficient disk headroom; or make a
lazy target unavailable until its delivery obligation demonstrably blocks cutover.
Each case moves a 1 MiB deterministic payload while SERIALIZABLE and READ
COMMITTED writers maintain independent ledgers and counters on that bucket.

Across the 18 runs, all 4,446 acknowledged ledger effects were present exactly
once. None of 709 confirmed serialization aborts appeared. Two attempts returned
unknown outcomes; neither appeared in the final ledgers, and neither attempt was
replayed. These are observations of this matrix, not a promise that an unknown
outcome always aborts. The fixture accepts a committed unknown outcome only if
its unique attempt identifier and counter agree. Both writers must make progress
after movement. Lazy fault cases also verify transferred counters/frontiers and
exact membership after a subsequent update.

The [fault index](fault-matrix-index.json) includes per-case acknowledgements,
unknowns, confirmed aborts, error codes and checks. The [complete fault archive](fault-matrix-runs.tar.gz)
retains every attempt set, result, configuration and process log. Its manifest
hashes each file. This correctness matrix is separate from the matched scale
study; fault-case throughput is not presented as a performance comparison.

## Validation and source boundaries

The [build receipt](build-receipt.json) binds the scale/fault binaries to all Rust
inputs and verifies them through `cd1ad67`. A later catalog freshness correction
changes the gateway; it does not change the movement protocol or native module.
The original timings remain bound to their original binary, not the later build.
Later Lua changes protect native guard error state, reject invalid limits before
initialization, and replace the backup drain check's estimated Vinyl length with
a bounded logical-index probe. Their focused checks and the final restore drills
exercise those changes; the earlier scale timings are not relabeled as new runs.

| Check | Local result and retained evidence |
|---|---|
| Bounded-move model and eight broken controls | 6,230 states, depth 28; each broken control fails its intended invariant |
| Complete focused model set | All 19 requested bounded-move/movement/join positive/negative outcomes match; [summary](checks/affected-models.out), full traces in `tla/out` |
| Shared Verus bodies | [140 verified, zero errors](checks/verus.out) |
| Rust workspace and SQL clippy | [Passed](checks/workspace-and-sql-clippy.out); local ARM full-workspace clippy has pre-existing native cast warnings |
| Bounded storage controls | [66 native and 64 Lua checks passed](checks/bounded-controls.out) |
| Native guard errors and stream handback | [23 checks passed](checks/native-guard.out) |
| Invalid movement configuration | [Four startup refusals passed](checks/config-refusals.out), before creation of Tarn metadata |
| Format-1 to format-2 maintenance migration | [memtx and Vinyl matrices passed](checks/format-migration.out), including both mixed-component admission directions, interrupted/prepared refusal, old-binary downgrade refusal and reopen; full artifacts in development archive |
| Cluster backup and isolated restore | Four RF3/fsync combinations passed on `cd1ad67`: memtx/Vinyl × native/Lua; exact rows, schemas and protocol spaces, interrupted capture/restore, quarantine and late lazy-frame checks |
| Broad failover workload | Two 60-second RF3/fsync runs passed on `cd1ad67`; 39 memtx and 42 Vinyl moves under transfers, increments, predicate audits and RC locks; handoff plus both shard-leader crashes; every acknowledged commit retained, no duplicate increments or unfinished move |
| Complete storage suites | Native passed in the [combined initial output](checks/storage-matrix-before-clock-readiness.out.gz); the [complete Lua rerun on `474aaae`](checks/storage-lua-final.out.gz) passed after the fixture readiness correction below |

The [restore/failover index](restore-and-failover-index.json),
[complete result/config/process-log archive](restore-and-failover-runs.tar.gz)
and [combined output](checks/restore-and-failover.out) retain these results.
Failover runs intentionally contain availability refusals during elections;
passing means exact effect reconciliation and resumed progress, not zero errors
or uninterrupted service. Backup captures a frozen cut: later source writes are
outside its documented recovery point.

Final fixture corrections preserve the format and clock guards. The topology
CI originally tried its frozen format-1 gateway against format-2 storage and was
correctly refused ([original job output](checks/topology-first-ci-failure.out.gz)).
It now records that refusal explicitly and runs the candidate's full topology
checks. An eight-group/two-gateway functional control passed twice. It supplies
no new cross-format baseline performance claim; historical topology measurements
retain their original source boundary.

The Lua-only full storage run passed its other suites but the held-status lazy
fixture received a safe clock-peer refusal before its one-shot PREPARE. The
fixture now waits for that required peer certificate before submitting PREPARE;
it neither weakens clock checks nor retries the mutation. The corrected control
passed for both engines ([output](checks/lazy-clock-readiness.out)). A first
focused invocation used a relative controller path and was refused by the
fixture's ownership wrapper; those startup logs are retained too. The
[control index](final-fixture-controls-index.json) and [hashed archive](final-fixture-controls.tar.gz)
retain successful runs and the original diagnostics. Full CI remains the check
of the integrated final revision.

Hosted CI then observed a confirmed `conflict`/`1213` abort during the backup
fixture's lazy-drain transaction ([job output](checks/restore-ci-confirmed-conflict.out.gz),
[original artifact](checks/restore-ci-confirmed-conflict.zip)). That fixture now
retries only this explicit confirmed-abort response, for at most five seconds;
transport exceptions and unknown outcomes still propagate without retry.
All four RF3 restore combinations passed again ([index](restore-confirmed-conflict-index.json),
[hashed archive](restore-confirmed-conflict-runs.tar.gz),
[output](checks/restore-confirmed-conflict.out)). This changes fixture setup,
not production mutation retry behavior.

The migration predecessor is exactly `8b12c4525bd677499a91b973afbc86cbcbb0cafd`.
Migration run source `265ed5e` and its separately built binaries are recorded in
the archived manifests. Format 2 requires drained maintenance, not a rolling
upgrade or an in-place downgrade. Raw native reads still expose local physical
state and do not establish routed ownership or a distributed snapshot.

## Catalog freshness regression found during final integration

The first full SQL differential run observed a stale `SELECT *` column list
after an acknowledged `ALTER TABLE ... DROP COLUMN`. An isolated 50-sequence
repeat did not reproduce it; a second full differential run passed on both
engines. These passes do not erase the first failure. Code inspection identified
a catalog-reload coalescing path also present unchanged on main `dacd3cb`.

A deterministic real-Tarantool regression holds an old catalog read across
publication and queues both an ordinary refresh and a mandatory DDL reload.
The unchanged implementation returns the old catalog ([negative control](checks/catalog-refresh-before-fix.out)).
The fix keeps ordinary refreshes coalesced and serializes mandatory reloads
while requiring their own read. The regression then passes, as do all 201 SQL
unit tests (including the 1,024-group simulation) and SQL all-target clippy
([output](checks/catalog-refresh-after-fix.out)). It adds no mutation retry.

Additional local SQL diagnostics are retained in
[the pre-fix output archive](checks/sql-before-catalog-fix.out.gz): the original
differential mismatch, passing memtx suites, the MySQL reference container's
OOM interruption, an invocation with missing required test arguments, and a
Vinyl index-build setup refused by admission at the movement study's smaller
16 MiB setting. Standard SQL validation uses the documented 128 MiB Vinyl
setting; the fixed-memory movement cohorts retain their original 16 MiB limit.

The updated gateway's [build receipt](catalog-build-receipt.json) binds source
`8c47d63` to the final local binaries. Both engines pass all 1,083 differential
cases and 21 concurrent DDL tests; Vinyl also passes ten lazy semantics and
four lazy recovery cases. Four further RF3/fsync mixed-engine movement cases
(source/coordinator crash × native/Lua) pass with exact effect reconciliation
and no replay of unknown outcomes. The [result index](catalog-integration-index.json)
and [hashed process/config/output archive](catalog-integration-runs.tar.gz)
retain those checks and the initial fixed-build clock-drift refusal separately.
No timing from these correctness runs replaces the fixed-memory study.


## Independent review: joined-shard capability lookup

Review of `4054480` reproduced a movement refusal after a successful real
RF3/fsync `TARN ADD SHARD 3`. The original nodes still had startup peers 1/2,
while confirmed membership named all three replicas of shard 3. The new
movement capability lookup consulted only startup peers and refused the
move with storage 1412 / SQL 1105. The exact value stayed unchanged.

The correction reuses the existing `shard_uris` membership-aware resolver;
it still checks every replica in that authoritative inventory. A fresh
three-shard native-enabled RF3/fsync run acknowledged the same move,
returned the exact value through SQL, and independently found the tuple
only on the new physical owner. The retained [before](checks/joined-move-review-before.out)
and [after](checks/joined-move-review-after.out) logs preserve the actual
startup inventory, committed membership and effects. The Rust dev build
is used only for this functional witness; no timing or capacity claim is
made. Existing `tests/jointest.sh` and `tests/growshrink.sh` exercise this
join-then-move path in full release CI. The integrated current-source
release/model/format-migration gates remain required before merge.


The combined format-2/KV storage unit exposed a fixture dependency: the
intentional oversized reply-cap row exceeded the normal movement-row
budget, and the local roundtrip named a fictional second shard. The
fixture now retires that payload only after both public API limit guards
pass and uses its real local capability endpoint for both roundtrip
sides. It remains explicitly a local protocol test, with real RF3
join/movement checked separately. Production limits and guards are
unchanged. The [original failure](checks/kv-integration-before.out) and
[complete corrected 210-check run](checks/kv-integration-after.out) are
retained. All 27 lazy baseline checks and 66 bounded movement checks
also pass on the combined storage source.
