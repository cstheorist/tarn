# Verification

Tarn's correctness argument has three layers, each catching a different
kind of mistake:

| Layer | Checks | Where |
|---|---|---|
| TLA+ model, checked with TLC | the commit protocol as a design: strict serializability, atomicity, recovery and record garbage collection under crashes, lost and re-delivered messages, and leader failover; READ COMMITTED transactions with row locks (no lost updates, real-time visibility); bucket moves between shards while transactions run | `tla/` |
| Verus proofs | that specific Rust functions do what their specifications say, for every input | `crates/tarn-verified/` |
| Tests | that the implementation matches the model and MySQL: storage protocol, SQL semantics against MariaDB, transaction anomalies, failover, faults | `lua/test/`, `tests/`, `crates/*/tests` |

What each layer found, and how the findings were fixed, is described in the
[design document](design.md#23-what-tlc-found) (§2.3, §2.4, §2.5, §2.8 and §2.9).

## TLA+ model

`tla/TarnCommit.tla` specifies the protocol: shards, keys with owners, three
transactions with fixed read and write sets, direct (non-SQL) readers, and
the actions of execution, single-shard commit, read-only validation,
PREPARE, APPLY, ABORT, coordinator decisions, timeout-driven recovery,
record expiry and garbage collection, READ COMMITTED transactions:
plain reads of the latest committed value, pessimistic row locks with a read
under the lock, their commits, and the window in which a local commit is not
yet visible to readers outside it, and bucket moves: rows have a place that
a move changes in steps (fence and drain on the old shard, a hold on the new
one, the rows deleted there and inserted here), requests reach the shard
they were routed to, and scans read every shard (design §2.12). Faults are coordinator crashes, lost and
re-delivered messages, and shard leader failover (durable state survives,
the MVCC read tracking of open streams and the row-lock table do not).

Invariants checked in every configuration:

| Invariant | Meaning |
|---|---|
| `TypeOK` | well-formed state |
| `AppliedOnlyIfCommitted` | no participant applies a transaction that is not committed |
| `NoAbortAfterCommit` | a committed transaction is never aborted anywhere |
| `PlainOnlyCommitted` | the plain Tarantool spaces only ever hold committed values |
| `DirectReadsClean` | direct key/value readers never see uncommitted data |
| `DirectReadYourWrites` | a direct read after an acknowledged commit sees it |
| `StrictSerializable` | the committed transactions have a serial order, consistent with real time, that explains every value they read (for READ COMMITTED transactions: every value they read under a lock; a scan of every shard must have seen each row exactly once) |
| `NoLostUpdate` | every READ COMMITTED write lands on the value its transaction read under the lock |
| `RCReadsSeeAcked` | a READ COMMITTED read sees every transaction acknowledged before it started |
| `UniqueHolds` | no key is inserted by two committed transactions (global unique indexes) |

`StrictSerializable` is checked constructively: the spec records the order in
which writes reached each key and each transaction's real-time
predecessors, and the invariant searches for a witness order.

### Configurations

`tla/MC.tla` defines twelve workloads on two shards (`Skew`, `Local`,
`Frac`, `Recov` with serializable transactions; `Counter`, `Mixed`, `Local`
with READ COMMITTED ones; `Uniq`, inserts of one global unique index
entry; `MoveScan` and `MoveRC`, a row moved to the other shard while
transactions run; `JoinScan`, the same to a shard that joins, also on
three shards; `GroupedTables`, a read of two co-located tables while
cross-shard writers run), and `tla/gen_cfgs.sh` generates one `.cfg` per combination
of workload, fault budget and options. The names say what is checked:

| Name | Meaning |
|---|---|
| `Skew`, `Local`, `Frac`, `Recov` | the protocol with PREPARE-based read-only commits and records kept forever |
| `*Failover` | a shard leader fails over during execution (with a direct reader) |
| `*_AckOnPrepare` | clients are acknowledged after PREPARE instead of after APPLY |
| `*_ROValidate` | the implementation's commit paths: cross-shard read-only transactions validate instead of preparing, and the decision shard's APPLY records COMMITTED |
| `*_GC` | additionally, transactions expire and protocol records are garbage-collected |
| `RC_*` | READ COMMITTED read-modify-writes racing each other and serializable transactions (`RC_Counter`, `RC_Mixed`, `RC_Local`), with faults, failovers (which empty the row-lock table) or ack-on-prepare |
| `Uniq*` | two rows with one value of a unique key without the shard key insert one global index entry, across shards and locally (`Uniq_RC`: the local one at READ COMMITTED), with faults or a failover |
| `Move_*` | a bucket move while a scan of every shard, writers of the moving row and READ COMMITTED read-modify-writes run (`Move_ScanPrepare`: the scan commits through PREPARE), with faults or a failover |
| `Join*` | the same move to a shard that joins the cluster, while a scan routed before it joined runs: on two shards (one read of the other shard) and on three (`Join3_*`: reads in streams, validated or prepared) |
| `GroupedTables` | one snapshot reads keys of two tables on one shard; the union of both read sets is checked for prepared writers |
| `GroupedRead` | a separate, small refinement model of one snapshot across scans that yield between tables |
| `GroupedDDL` | a separate checked-read premise: a yielding read cannot accept a snapshot that existed only while a DDL hold covered its table |
| `StreamMarks` | a separate local refinement model of the transaction numbers by which a shard knows that a stream began in its current term and after any `TRUNCATE` of a table it read, and of the premises the native steps rely on |
| `Handback*` | a separate model of the contract between the SQL server and a native procedure that commits writes: served as the Lua procedure would, or handed back unchanged, never once its commit began, and never sent again after a lost connection (`*_Autocommit`: an autocommit write) |
| `NativeKeyBatch` | a bounded native full-primary-key batch returns the complete ordered filter-map of one memtx image, checking write claims even on missing keys, or hands the whole request back |
| `StrongReadFreshness` | a request-specific quorum observation, local WAL catch-up and synchronous-queue settlement precede the first strong snapshot |
| `StrongStreamCertificate` | an owned empty transaction is certified after its first barrier and before any read; later reads require that exact native transaction ID |
| `StrongStreamCompletion` | a separate final quorum prefix precedes validation of the original caller transaction/read set; neither skipping the prefix nor replacing the read set may commit a fractured view |
| `LockReplyOwnership` | a yielding locking RPC returns rows only under its original observed role/generation and live session |
| `RecordExpiryClock` / `RecordExpiryPolicy` | a two-skew retention margin under varying bounded clock offsets, plus local persisted-policy/high-water admission after GC |
| `CompletionRetention` | decided finishing preserves inherited receipt timestamps/horizons despite an unknown clock, preventing GC and a contradictory fresh abort after reboot |
| `LogicalClockExpiry` | bounded raw corrections produce monotone logical clocks under a fixed common reference band, preserving the two-skew expiry/admission contract |
| `DepartureDependencies` | publish draining everywhere, fence new admissions, and drain all prepared, staging and in-flight dependencies before stopping a shard |
| `Fairness` | bounded pass ordering reaches eligible records beyond a fixed stuck set; it does not change record lifecycle or timing guards |
| `DecisionPlacement` / `Placement_*` | stable decision identity among frozen writers, plus commit/GC checks with another transaction-specific placement |
| `PipelineAdmission` | connection/shard/gateway resource charges and uncertainty survive cancellation and physical retirement |
| `EngineCopy` / `EngineCutover` | local online copy, final fencing and prepared-image retention, followed by confirmed decision/publication, auxiliary readiness, identity and counter continuity |
| `Backfill` | a separate local refinement model of append-only instant-column materialization, persistent fencing, original-length undo, abort and slot reuse |
| `BackfillDDL` | a separate local refinement model of whole-RPC serialization around yielding native index builds and delayed ALTER cleanup |
| `Bug_*` | one protection switched off; TLC **must find a violation** |
| `Tradeoff_*` | a documented weaker guarantee; TLC **must find a violation** |

The full results table (states, depth, time, outcome) is in
[design §5.1](design.md#51-model-checking), generated from the TLC logs in
`tla/out/`.

### Running TLC

You need Java 11+ and `tla2tools.jar`:

```sh
curl -fsSL -o tla/tla2tools.jar https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar

tla/check.sh                                    # the fast configurations, outcomes checked (as in CI)
WORKERS=4 tla/run_all.sh Recov_GC Bug_NoTombstone   # any configurations; logs in tla/out/
python3 tla/summarize.py                        # table of every result in tla/out/
```

`run_all.sh` honours `TLA2TOOLS` (path to the jar), `WORKERS` (default 1)
and `TLC_HEAP` (default 3g). The large configurations (`Skew*`,
`Move_ScanPrepare`), which `check.sh` leaves out, take from minutes to an
hour (`Move_ScanPrepare`: 48 minutes, 20.6 million states);
`Skew_GC_ROValidate` did not finish within two hours (47.8 million states
explored without a violation).

To add a protection switch: add a `Bug*` constant to `TarnCommit.tla`, guard
the protection with it, add a flag to `gen_cfgs.sh`, and add a `Bug_*`
configuration that TLC must fail.

### Grouped local reads (issue #83)

The commit model's keys are global identifiers: `x` and `z` can name keys
of different tables on shard A, with `y` on B. `GroupedTables` has T1 write
`z` and `y`, T2 read `x` and `z` in one `SnapshotRead`, and T3 read `y` and
write `x`. The read must check `NoWLocksAt` for the union `{x, z}`. Otherwise,
partly applied cross-shard writes can produce a read with no serial order.
`Bug_GroupedTablesNoWLockCheck` reuses `BugLocalReadsIgnoreWLocks` to remove
that union guard; it removes the whole guard, rather than modeling a
particular table omitted by a parser. No commit-protocol action changes.

`GroupedRead.tla` checks a separate refinement premise: one local read-only
transaction supplies one snapshot for every constituent scan, including
across yields. A local writer atomically changes both tables. The
`CoherentSnapshot` invariant requires the completed result to equal a whole
database state that existed during the RPC. With `BugSeparateSnapshots`,
TLC reads outer value 0, runs the writer, then reads inner value 1: a result
that never existed. This model applies to a grouped READ COMMITTED
statement too; the main model's per-key `RCRead` and `RCReadsSeeAcked` do
not express statement-wide snapshot coherence, and `StrictSerializable`
intentionally excludes plain RC reads.

The focused checks use no faults and disable timeout recovery in the
three-key workload. The two models establish the protocol guard and the
snapshot premise, respectively. They do not prove Tarantool's MVCC
implementation, the Lua lock-history ring or its overflow handling,
per-table definition and bucket fences, completeness/page limits, gateway
read-set construction, buffered-write overlays, or SQL join semantics.
Those remain implementation and storage/SQL test obligations. The raw
native `DirectReadAtomic` trade-off is unchanged.

Run just these checks with the same outcome checking used by CI:

```sh
tla/check.sh GroupedTables Bug_GroupedTablesNoWLockCheck GroupedRead Bug_GroupedReadSeparateSnapshots
```

They are also included in the default `check.sh` list. `run_all.sh` selects
the corresponding local refinement module or `MC.tla` by configuration;
explicit configuration arguments still replace the default list.
The bounded results are:

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `GroupedTables` | 1,909 | 17 | passes all commit-model invariants |
| `Bug_GroupedTablesNoWLockCheck` | 1,194 | 13 | violates `StrictSerializable` |
| `GroupedRead` | 15 | 6 | passes `TypeOK` and `CoherentSnapshot` |
| `Bug_GroupedReadSeparateSnapshots` | 18 | 6 | violates `CoherentSnapshot` |

### DDL claims during a yielding read

A vinyl grouped read exposed a further refinement gap: with its root
already held by TRUNCATE, it captured the old root rows, yielded, and then
returned those rows after APPLY emptied the table and released the hold.
The final current-hold check saw no hold; the prepared-write history had
no DDL event. The failing of the stream transactions that read a table
before its `TRUNCATE` (then `DOOMED`, now `DDL_TXN`) did not cover this
autocommit read. Refreshing the DDL cache on entrance fixes a cold cache,
but cannot detect a claim released during the scan.

`GroupedDDL.tla` isolates that boundary: one read attempt, one prepare/apply
of TRUNCATE, and one cache invalidation/rebuild. `NoHeldSnapshot` requires
an accepted snapshot to equal a state that existed during the read while
its table was not held. A clean, unchanged read can succeed. A generation
captured before scanning must still match at the atomic final check, and
the DDL cache must be fresh. Taking/releasing a hold and rebuilding the
cache advance the generation. A stale flag need not itself advance it:
the flag prevents acceptance until a rebuild, whose increment preserves
evidence even if another fiber clears the flag during the scan.

The implementation contract checks both lock-index and DDL-cache staleness,
then the DDL generation, before any no-lock shortcut or result-specific
callback, including for empty or incomplete groups. A stale result
refreshes both caches outside the read transaction; a changed generation
discards the whole result. Both repeat under the original absolute
`lock_wait` deadline, including repeated stale-cache retries. Native
non-yielding memtx reads still use their published current state; their C
state layout is unchanged.

The model deliberately abstracts the native snapshot, published-cache
mechanism and retry implementation. It proves this bounded claim/history
premise, not full distributed DDL linearizability, liveness, deadline
arithmetic, or the Lua implementation. Those require the storage witnesses
and existing protocol/fault tests. In particular, the generation mutant
captures a held table, applies TRUNCATE, then accepts its old snapshot; the
stale-cache mutant captures a held table, invalidates its cache, then
accepts using the now-unknown cached hold.

```sh
tla/check.sh GroupedDDL Bug_GroupedDDLIgnoreGeneration Bug_GroupedDDLIgnoreStale
```

All three are also in the default checks, routed to `GroupedDDL.tla`:

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `GroupedDDL` | 77 | 7 | passes `TypeOK` and `NoHeldSnapshot` |
| `Bug_GroupedDDLIgnoreGeneration` | 36 | 5 | violates `NoHeldSnapshot` |
| `Bug_GroupedDDLIgnoreStale` | 37 | 5 | violates `NoHeldSnapshot` |

### Instant-column materialization (issue #70)

`InstantBackfill.tla` checks the required local storage contract for
automatic backfill. It has two rows on one shard, one
admitted old-layout writer, one crash/recovery and one takeover. One row
ends before a published instant column whose frozen fill is 2; the other
already stores value 5. A first ALTER appends a new column with fill 3. If
it aborts, a second ALTER reuses that slot with fill 7. Old-layout writes
can set the published column to 9, so cleanup must preserve acknowledged
business updates as well as frozen fills.

The modeled contract is:

* Persist the per-shard version/generation fence before changing a row,
  and drain old-layout local commits and prepared transactions before
  materialization. Recovery reloads that fence before admitting writers.
* Atomically save a row's **first** original length and append only its
  missing suffix. The undo key uses job version and row, not takeover
  generation. Repeating a chunk preserves the original length. Below the
  old committed layout width, use the old definition's padding, even if
  the new ALTER drops that column; only newly allocated slots use new
  fills. Existing physical values, including NULLs, remain unchanged.
* Each chunk validates the current job version, owner and phase at its
  atomic commit. `BeginBatch`/`FinishBatch` allow a request to pause across
  takeover or abort; the final guard rejects stale work. A delayed request
  after a crash represents redelivery, not a native transaction surviving
  that crash.
* Publish only after all rows and indexes are ready. Once the durable job
  chooses APPLY, it goes forward. ABORT keeps the fence while atomically
  trimming rows and deleting their undo entries; it records completion and
  releases the fence only when that version's journal is empty. Published
  catalog versions prevent late ABORT from trimming published data. Undo
  garbage collection after APPLY changes metadata only.

The runtime refinement must read/check ownership metadata **inside the
same native RW transaction** as journal and user-tuple changes, or provide
an equivalent current-state, non-yielding final guard. A check made only
before `box.begin()` cannot establish the modeled final eligibility.
Tarantool documents its `read-committed` setting as commit/WAL visibility
and retains serialization checks for transactions that both read and
write; this is not an assumption that the API label has ANSI READ
COMMITTED semantics. See the primary [transaction model](https://www.tarantool.io/en/doc/latest/platform/atomic/transaction_model/)
and [MVCC description](https://www.tarantool.io/docs/tarantool/en/3_x/platform/atomic/txn_mode_mvcc).
That documentation supports the premise but does not prove this code's
metadata read is tracked. A focused native probe passed all 12 cases with
memtx metadata/journal and either memtx or vinyl user rows. Generation or
phase changed after the first metadata read, either before user writes or
after tuple/journal writes; the stale transaction failed and left both
unchanged. Controls with unchanged metadata committed. The probe repeats
the metadata read before commit. This validates that primitive on the
tested build; the actual materialization/undo procedures must preserve that
transaction boundary and pass their own paused-chunk and crash tests.
The native probe used Tarantool `3.9.0-0-g8b9a0f59a9` from image
`sha256:1a64f298fd5494728c660d221c4c5331e994ac4c57082246ca7c87c6497b0873`.

Invariants preserve the old projection before publication, require the
published native index to contain the correct logical fill, forbid undo
entries reappearing after abort completion, and prevent aborted fill 3
from poisoning a later column with fill 7. The lost-undo control abstracts
the durable outcome of separately committed row/journal writes interrupted
between them. The stale-batch control removes the whole final
version/owner/phase guard; it does not establish that every guard component
is independently necessary for identical, idempotent work.

This bounded model is not a proof of native MVCC, WAL/replication
atomicity, Lua chunk construction, collation conversions, predicate/index
implementation, full distributed DDL linearizability, or recovery liveness.
Existing distributed job decisions and commit-protocol assumptions remain
separate. New CI fields may use their known frozen weight; retrofitting
an unweighted pre-existing CI column remains explicitly unsupported (1235).
Native fault and differential tests must verify the implementation's
refinement, including a crash after partial padding and after partial undo.

```sh
tla/check.sh Backfill Bug_BackfillNoPersistentFence Bug_BackfillSkipDrain Bug_BackfillLoseUndo \
  Bug_BackfillOverwriteUndo Bug_BackfillPublishEarly Bug_BackfillReleaseEarly \
  Bug_BackfillStaleBatch Bug_BackfillWrongOldPad Bug_BackfillAbortAfterApply
```

All ten configurations are generated by `gen_cfgs.sh`, included in the
default checks, and routed to `InstantBackfill.tla`:

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `Backfill` | 3,757 | 21 | passes all five invariants |
| `Bug_BackfillNoPersistentFence` | 1,004 | 10 | violates `OldProjectionPreserved` |
| `Bug_BackfillSkipDrain` | 631 | 8 | violates `OldProjectionPreserved` |
| `Bug_BackfillLoseUndo` | 2,584 | 12 | violates `PublishedIndexCorrect` |
| `Bug_BackfillOverwriteUndo` | 6,251 | 15 | violates `PublishedIndexCorrect` |
| `Bug_BackfillPublishEarly` | 22 | 4 | violates `PublishedIndexCorrect` |
| `Bug_BackfillReleaseEarly` | 80 | 6 | violates `AbortedUndoClean` |
| `Bug_BackfillStaleBatch` | 86 | 6 | violates `AbortedUndoClean` |
| `Bug_BackfillWrongOldPad` | 11 | 4 | violates `OldProjectionPreserved` |
| `Bug_BackfillAbortAfterApply` | 1,249 | 11 | violates `PublishedIndexCorrect` |

### Yielding native index effects during backfill

The row/journal transaction guard cannot undo a native index build that
finishes after ABORT. Tarantool 3.9 serializes issued DDL operations, but
builds an index on a replacement space before publishing that space. A
Tarn cleanup that sees no candidate index and therefore issues no native
drop does not join that serialization. See the native
[ALTER preparation lock](https://github.com/tarantool/tarantool/blob/3.9.0/src/box/alter.cc#L1032),
[index build](https://github.com/tarantool/tarantool/blob/3.9.0/src/box/alter.cc#L1508)
and [space publication](https://github.com/tarantool/tarantool/blob/3.9.0/src/box/alter.cc#L1110).

`BackfillDDL.tla` isolates this local boundary. One old PREPARE can pause
inside an invisible build; two ABORT requests can pause before cleanup;
a new job can reuse the same numeric index ID with a different definition.
One DROP+CREATE can also replace an idle space under the same name, reset
its catalog version, and receive an old request carrying the original ID.
One per-space mutex covers each **entire** ALTER BUILD/PREPARE/APPLY/ABORT
RPC, including native DDL, cleanup, fence release and return. Admission
rechecks current durable version/phase and the request's intended space ID
after acquiring it, before native schema effects. Comparing the current
ID only to one captured from that name before locking is insufficient:
both can identify the replacement rather than the request's original space.
The model's requests carry that ID, as SQL-generated ALTER arguments do;
legacy/manual RPC arguments that omit it are outside this identity claim.
Thus a native build completes or rolls back before ABORT
can inspect/drop its candidate, and a delayed finisher cannot survive into
a newer job's index-ID reuse. A published old APPLY must only collect its
own undo metadata; the mutex does not authorize stale schema mutations.

Two controls remove separate consequences of this same mutex. Without
build quiescence, ABORT skips a still-hidden candidate, the old build
finishes, and ABORT releases its fence with that index still present.
Without finisher serialization, one ABORT completes, a new job builds its
candidate, and the other ABORT drops that candidate before publication.
The identity control retains the mutex but omits intended-ID binding;
the old request builds its candidate in the replacement's fresh namespace.
The correct model admits one RPC at a time; same-fiber nested calls are
part of that RPC, without an intervening unlock. Reentrant ownership,
exception-safe `finally` release, bounded waiting, actual native index
definitions and process-crash recovery remain runtime refinement
obligations, not proved Lua or native-DDL properties. The row/undo model
and distributed commit protocol are unchanged.

```sh
tla/check.sh BackfillDDL Bug_BackfillDDLNoQuiescence Bug_BackfillDDLNoFinisherSerialization \
  Bug_BackfillDDLWrongSpace
```

These four generated configurations route to `BackfillDDL.tla`. Together
with the preceding ten, they bring the default check list from 57 to 71;
all previous configuration files are unchanged.

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `BackfillDDL` | 33 | 11 | passes all four invariants |
| `Bug_BackfillDDLNoQuiescence` | 33 | 6 | violates `NoLateAbortedIndex` |
| `Bug_BackfillDDLNoFinisherSerialization` | 65 | 9 | violates `PublishedIndexMatches` |
| `Bug_BackfillDDLWrongSpace` | 15 | 4 | violates `ReplacementIndexUnchanged` |

### Lazy secondary indexes (issue #82)

Four separate local refinement modules specify the eventual-index contract
before implementation. They leave `TarnCommit.tla` and its configurations
unchanged. They do **not** establish distributed serializability for lazy
reads: missing recently matching rows is allowed until propagation finishes.
Only single-table, nonlocking autocommit reads may use this path; explicit
transactions, locking reads and writes retain the existing strong paths.
The first implementation always fetches base rows, without a covering
index shortcut.

* `LazyIndex.tla`: one primary key, two canonical index keys/destinations,
  a key move, delete/reinsert, two index incarnations and a speculative
  write that never commits. The committed row and immutable assignments
  enter durable state atomically. Shipping sees only confirmed commits;
  lost frames, duplicated/reordered frames, destination apply, ACK and
  source deletion are separate actions. Target membership and tombstones
  keep the newest sequence per `(index incarnation, key, base PK)`.
  A same-key change has one final assignment, never contradictory DELETE
  and ADD assignments at the same version. Authoritative retirement permits
  obsolete ACKs; a missing/not-ready destination does not.
* `LazyHandoff.tla`: one base bucket moves from S to R, including when
  empty. S fences/drains and ACK-flushes before counter/live-version copy
  and transfer. The variable `oldSourceRemoved` removes **S**, not R:
  `pending2` belongs to S; a subsequent `pending3` belongs to R and remains
  deliverable after S is removed. Removing a current receiver requires
  another instance of the same source-drain protocol. Independently, one
  target bucket moves X to Y. In-flight admission lasts through the native
  commit interval, final ownership checks reject newly fenced/stale work,
  and copy notes preserve writes during transfer. Live markers, tombstones
  and acceptance floors travel with the target bucket.
* `LazyCapacity.tla`: two prepared transactions and a local commit compete
  for bounded entries, worst encoded bytes and remaining sequence numbers.
  YES reserves all three durably without allocating a delivery sequence;
  APPLY converts the reservation into row/outbox state, while ABORT
  releases it. In the correct model every promised APPLY fits; its action
  needs no capacity rejection. Controls vary the limits so each omitted
  reservation has its own witness rather than being masked by another cap.
* `LazyBarrier.tla`: two own acknowledged commits on different buckets,
  one of which may still await row APPLY, plus another writer that can
  expose a hole in ACK order. Own obligations accumulate even while the
  session flag is OFF; it is enabled only after those writes in this
  fixture. Resolve every pending APPLY first, then sample counters and
  wait for per-bucket completion frontiers, **before** the index snapshot.
  Forgetting unresolved/unknown/expired history cannot claim success: the
  implementation must keep a bounded obligation or use explicit strong
  fallback/error. A timeout does not justify replaying the original DML.

The source completion prefix is `counter` when its pending outbox is empty,
otherwise `min(pending sequence) - 1`, sampled together in one native
read-confirmed view. All assignments at a sequence remain pending until
ACKed. Sequence-zero baseline work can therefore report a signed completed
prefix of -1; counters stay unsigned. Creating another index incarnation
can enqueue baseline sequence zero even when the shared source counter is
already positive, so this global completion frontier is not monotonic
across bootstrap. It may conservatively delay another ready index's barrier;
per-incarnation target floors remain independently scoped. Tarantool distinguishes visibility at
commit start from visibility after commit returns; source shipping and
frontier reads require the latter ([transaction model](https://www.tarantool.io/en/doc/latest/platform/atomic/transaction_model/)).

Tombstone GC first persists an acceptance floor, then removes only covered
tombstones. Its exact key is `(index incarnation, target bucket, source
bucket)`. A source's **all-destination** confirmed completion prefix proves
that floor; a largest ACK observed at one destination does not. Incoming
assignments at or below the floor are obsolete ACKs before per-key lookup.
Live entries remain. A later duplicate cannot recreate a deleted entry,
even after its individual tombstone is gone; target transfer must copy the
floor even if no visible rows remain. `LazyIndex` checks safe floor/GC
ordering and rejects the max-ACK proof; `LazyHandoff` checks floor copying.
Each models one source-bucket scope. Correct exact-scope lookup when many
such machines coexist is an implementation obligation, not a proof that
arbitrary floor records may be interchanged.

The invariants cover committed-only delivery, exact index projection once
writes stop and the outbox is ACK-drained, deleted-row/key rechecks, durable
source obligations, counter/live-version continuity, capacity promises and
causal session catch-up. The recheck fixture uses row existence and exact
current candidate-key equality. Actual full `WHERE` evaluation, canonical
key/type/collation encoding, PK deduplication and refill after rejecting a
stale candidate require SQL/storage tests. A passed barrier establishes
coverage of the session's committed index effects, not a global snapshot
or completeness for concurrent other writers.

Refinement premises remain explicit: bootstrap enumerates the complete
baseline, reads each row and its version in one consistent view, ACKs every
baseline assignment before READY, and refuses late bootstrap enqueue after
READY. These enumeration/snapshot/late-request guards are not separately
mutated here; native witnesses must exercise them. Existing fence/drain
contracts (including the backfill models above) protect the bootstrap write
window. New/forward-schema writes wait until this shard's durable matching
capture/catalog activation; a stale/unknown registry fails closed. The
forward-admission and registry controls expose omissions of those guards.
Physical materialization, bucket copies and temporary unique-swap deletes
must not become logical outbox changes. Native commit atomicity and
confirmation, byte accounting, integer exhaustion checks, full bucket-copy
implementation and fair eventual delivery/recovery remain runtime premises.
Process/network failure may lose transient frames/receipts; durable
row/outbox/markers/floors survive. The finite checks prove safety and
conditional drained convergence, not that a partitioned queue will drain.

All 37 focused configurations are generated by `gen_cfgs.sh`, selected by
`run_all.sh` module routing and included in the default `check.sh` list:
5 positive cases and 32 expected counterexamples. Explicit configuration
arguments still override the default list. The default list grows from
71 to 108 before the capacity-meter refinements below; previous
configurations and the core commit module are unchanged.
To run just these refinements:

```sh
configs=(LazyIndex LazyCapacity LazyBarrier LazyHandoff LazyHandoffLive)
for f in tla/Bug_Lazy*.cfg; do
  name=${f#tla/}
  configs+=("${name%.cfg}")
done
tla/check.sh "${configs[@]}"
```

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `LazyIndex` | 475,615 | 38 | passes |
| `LazyCapacity` | 71 | 9 | passes |
| `LazyBarrier` | 80 | 13 | passes |
| `LazyHandoff` | 156 | 13 | passes |
| `LazyHandoffLive` | 76 | 12 | passes |
| `Bug_LazyBarrierForgetUnresolved` | 126 | 8 | violates `OwnWritesCovered` |
| `Bug_LazyBarrierLastOnly` | 72 | 9 | violates `OwnWritesCovered` |
| `Bug_LazyBarrierMaxAck` | 81 | 12 | violates `OwnWritesCovered` |
| `Bug_LazyBarrierSkipApplyWait` | 57 | 7 | violates `OwnWritesCovered` |
| `Bug_LazyBarrierSplitFrontierView` | 197 | 13 | violates `PrefixCovered` |
| `Bug_LazyBarrierTrackOnlyWhenEnabled` | 57 | 7 | violates `OwnWritesCovered` |
| `Bug_LazyBarrierWaitAfterSnapshot` | 106 | 11 | violates `OwnWritesCovered` |
| `Bug_LazyCapacityAbortLeaks` | 8 | 3 | violates `AbortedReleased` |
| `Bug_LazyCapacityNoByteReserve` | 7 | 3 | violates `BytePromisesFit` |
| `Bug_LazyCapacityNoEntryReserve` | 6 | 3 | violates `EntryPromisesFit` |
| `Bug_LazyCapacityNoSeqReserve` | 6 | 3 | violates `SequencePromisesFit` |
| `Bug_LazyHandoffDropFloors` | 56 | 7 | violates `DrainedProjection` |
| `Bug_LazyHandoffDropLiveVersion` | 9 | 4 | violates `LiveVersionSurvives` |
| `Bug_LazyHandoffDropMarkers` | 18 | 5 | violates `DrainedProjection` |
| `Bug_LazyHandoffNoTargetDrain` | 136 | 11 | violates `DrainedProjection` |
| `Bug_LazyHandoffNoTargetFence` | 136 | 11 | violates `DrainedProjection` |
| `Bug_LazyHandoffNoTargetNote` | 127 | 11 | violates `DrainedProjection` |
| `Bug_LazyHandoffRemovePending` | 4 | 2 | violates `SourceObligationsSurvive` |
| `Bug_LazyHandoffResetCounter` | 10 | 4 | violates `CounterContinues` |
| `Bug_LazyHandoffSourceNoFlush` | 8 | 3 | violates `SourceObligationsSurvive` |
| `Bug_LazyIndexAckMissingTarget` | 537,216 | 21 | violates `DrainedConverges` |
| `Bug_LazyIndexCaptureFailOpen` | 171 | 10 | violates `DrainedConverges` |
| `Bug_LazyIndexFloorFromMaxAck` | 775,423 | 22 | violates `DrainedConverges` |
| `Bug_LazyIndexForgetBeforeAck` | 58,065 | 17 | violates `DrainedConverges` |
| `Bug_LazyIndexForwardBeforeCapture` | 54 | 8 | violates `DrainedConverges` |
| `Bug_LazyIndexGcBeforeFloor` | 552,060 | 24 | violates `DrainedConverges` |
| `Bug_LazyIndexIgnoreGeneration` | 473,050 | 29 | violates `DrainedConverges` |
| `Bug_LazyIndexNoBaseRecheck` | 50 | 9 | violates `BaseReadSafe` |
| `Bug_LazyIndexNoKeyRecheck` | 31 | 8 | violates `CandidateKeySafe` |
| `Bug_LazyIndexShipBeforeCommit` | 560 | 12 | violates `CommittedOnly` |
| `Bug_LazyIndexSplitOutbox` | 44 | 9 | violates `DrainedConverges` |
| `Bug_LazyIndexStaleOverwrite` | 249,637 | 21 | violates `DrainedConverges` |

### Conservative capacity meter

`LazyMeter.tla` refines the entry/byte admission premise of `LazyCapacity`
without changing that module or the commit protocol. Durable outbox entries
and per-transaction reservations remain resource truth. A volatile meter
precharges positive allocations before publication, refunds them only on
definite rollback, and defers negative releases until confirmed commit.
It conservatively covers both durable obligations and positive operations
that may still commit. APPLY converts a reservation already promised at YES;
it cannot be refused by a fresh capacity check. This fixture permits a
temporary double charge for conversion, so the meter itself may exceed the
limit and delay new admission while durable resource usage stays bounded.
It is not a bound on native transaction memory, target-index memory, or
process RSS. Sequence-capacity reservations remain the separate
`LazyCapacity` obligation.

Startup or leadership invalidation makes the meter UNKNOWN. Every quota
mutator must fail transiently before charging or changing durable truth,
including ACK, reservation release/ABORT, bootstrap and retirement cleanup.
A separate worker drains old native effects and the old sync/applier queue,
then sums entries and reservations in a confirmed view while admission is
fenced. READY installation must still match the rebuild epoch and current
writable role, with the native cut still valid. Source callers already in
INFLIGHT must never wait for that worker to drain themselves. A caller
returning or being cancelled is not sufficient evidence that a native
transaction has ended: the pinned native implementation can detach a
synchronous waiter while its transaction is unresolved
([`txn_commit_impl`](https://github.com/tarantool/tarantool/blob/8b9a0f59a9/src/box/txn.c#L1188)).

Each callback carries its immutable meter epoch. Old callbacks cannot alter
a rebuilt meter. The model separates native completion from its callback,
allowing a delayed old callback after rebuild; this deliberately
overapproximates the actual nonyielding completion callback. The native
success path requires no outstanding sync wait, commits the engines, then
runs commit callbacks before waking the caller; rollback callbacks follow
engine rollback
([native completion](https://github.com/tarantool/tarantool/blob/8b9a0f59a9/src/box/txn.c#L755)).
Runtime refinement still requires exact-image callback and promotion/queue
witnesses, all quota-mutator coverage, and atomic final role/epoch
publication. Callback tickets must be allocated and registered before
charging, and their terminal arithmetic must not yield, fail, or query
spaces ([callback restrictions](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_txn_management/on_commit/)).

The terminal-callback premise applies to quota-bearing Lua transactions,
not every invocation of a native rollback callback. The pinned engine also
runs transaction `on_rollback` callbacks when rolling back to a savepoint,
without ending the transaction or removing its callback list
([savepoint rollback](https://github.com/tarantool/tarantool/blob/8b9a0f59a9/src/box/txn.c#L395)).
The native one-shard commit may use that operation before handing a request
back to Lua. Supported accepted native streams must therefore have no
pre-existing lazy quota ticket: SQL buffers user writes until commit, and
the shared native write-admission guard checks every actual source against
the capture registry **before** creating a transaction or savepoint. Lazy
writes defer to Lua before native mutation. A stream's transaction carries
no Lua callbacks before its commit (what the shard knows of when it began,
for elections and DDL, are transaction numbers: design §1.5), so a native
savepoint rollback keeps its read set and that tracking. These are
implementation refinement obligations; the model does not prove arbitrary
savepoint use safe for an armed meter ticket. The ordinary terminal-callback
runtime evidence still applies to the guarded quota-bearing Lua paths.

The bounded fixture contains one reservation that can APPLY or ABORT, a
disjoint local enqueue/ACK, two invalidations, and one old queued native
effect. Its queue-cut action is an explicit runtime premise, not a proof
that a particular `box.info` field establishes that cut. It proves no
delivery or recovery liveness. In particular, unknown admission, unresolved
old effects, an unresolved queue, stale snapshot installation and late
callbacks have independent reachable controls. The default list now has
118 configurations before the target-confirmation refinement below (108
plus these 10); explicit arguments still override it. To run only this
refinement:

```sh
tla/check.sh LazyMeter Bug_LazyMeter{NoPrecharge,EarlyRelease,RefundUncertain,MutateUnknown,NoDrain,NoQueueCut,StaleInstall,StaleCallback,ApplyCap}
```

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `LazyMeter` | 1,918,745 | 30 | passes |
| `Bug_LazyMeterNoPrecharge` | 26 | 5 | violates `ReadyConservative` |
| `Bug_LazyMeterEarlyRelease` | 91 | 7 | violates `ReadyConservative` |
| `Bug_LazyMeterRefundUncertain` | 45 | 6 | violates `ReadyConservative` |
| `Bug_LazyMeterMutateUnknown` | 559 | 6 | violates `ReadyConservative` |
| `Bug_LazyMeterNoDrain` | 200 | 8 | violates `ReadyConservative` |
| `Bug_LazyMeterNoQueueCut` | 27 | 4 | violates `ReadyConservative` |
| `Bug_LazyMeterStaleInstall` | 25 | 5 | violates `ReadyEpochCurrent` |
| `Bug_LazyMeterStaleCallback` | 1,035 | 10 | violates `ReadyConservative` |
| `Bug_LazyMeterApplyCap` | 90 | 7 | violates `NoCapacityRefusal` |

### Confirmed target acknowledgement

`LazyTargetConfirm.tla` refines the existing durable-target acknowledgement
premise. One committed source assignment is retained after the first
target CALL loses its receipt. That native target transaction may still be
waiting for its synchronous quorum when the source retries the **same**
assignment. Observing the first transaction's unconfirmed marker and doing
no writes does not justify acknowledging the duplicate: the first
transaction can still roll back. The source could otherwise delete its
only assignment and leave no durable target effect.

An exact Tarantool 3.9.0 primitive witness, with MVCC and fsync WAL, reached
this failure on both memtx and vinyl: the duplicate read-committed
transaction observed the pending sequence, performed no writes and
acknowledged while the sync queue still contained the first transaction.
That transaction later timed out with error 216 and rolled back. The
read-confirmed control returned conflict 97 while the first was pending,
so the source retained its assignment for a later idempotent retry.
The implementation must use confirmed target proofs, including no-op
duplicate, floor and retirement paths; a write-mode transaction alone is
not evidence that every read it uses for an ACK is confirmed
([native visibility levels](https://www.tarantool.io/en/doc/latest/platform/atomic/transaction_model/)).

The bounded model separates the first pending native effect, duplicate
reply, source deletion and first commit/rollback. Its proof tokens abstract
a committed marker or retirement; the primitive witness exercises the
marker case. The broken control reaches pending marker, duplicate ACK,
source deletion, then native rollback and violates
`DrainedObligationSafe`. Existing `LazyIndex` retains the broader
version/floor/delivery contract; this refinement does not prove native
batch parsing, replication, or all transaction scheduling. The core and
previous refinement modules/configurations are unchanged.

```sh
tla/check.sh LazyTargetConfirm Bug_UnconfirmedAck
```

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `LazyTargetConfirm` | 19 | 5 | passes |
| `Bug_UnconfirmedAck` | 17 | 5 | violates `DrainedObligationSafe` |

### Confirmed participant APPLY acknowledgement

`LazyApplyConfirm.tla` refines the participant-completion premise used by
`LazyBarrier`. A durable COMMITTED decision and YES already exist. The
first physical APPLY can be waiting for its synchronous quorum while a
duplicate reads its provisional `applied` participant record. Such a
record cannot discharge the session's own pending-APPLY obligation.
Otherwise a correctly confirmed source counter/outbox view can still be
zero/empty, so its frontier barrier passes and an own committed row is
missing from the lazy read **while the first APPLY is still pending**.
Native rollback makes that loss persistent until a later recovery APPLY.

An actual `tarn_apply` probe on Tarantool 3.9.0, with MVCC, fsync WAL and
`cfg.sync=true`, reproduced the boundary on both memtx and vinyl. A durable
PREPARE and COMMITTED decision preceded the probe. While the first APPLY
waited for quorum two, the duplicate returned `ok/applied`. The first then
timed out with error 216; a read-confirmed view still showed `prepared`,
one reservation, no base row, counter zero and no outbox entry. A private
control changing only that APPLY transaction to read-confirmed returned
conflict 1213 to the duplicate while the first was pending, retained the
obligation, and completed the known-committed idempotent retry with quorum
one. These are native-engine probes with the optional C module disabled;
they do not establish the cause of a separate SQL CI failure.

The model separates native completion, duplicate reply, pending-history
release, delivery and the lazy snapshot. Its confirmed counter/frontier
read and delivery rules reuse `LazyBarrier`/`LazyIndex` premises. The
positive model requires confirmed participant completion before release;
`Bug_UnconfirmedApply` reaches the missing-row read while the first APPLY
is pending. This local refinement does not change the commit protocol or
prove every participant procedure, native queue implementation, recovery
schedule, or SQL consumer. Production must justify every successful APPLY
completion proof, including no-write duplicate paths.

```sh
tla/check.sh LazyApplyConfirm Bug_UnconfirmedApply
```

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `LazyApplyConfirm` | 14 | 7 | passes |
| `Bug_UnconfirmedApply` | 10 | 5 | violates `OwnReadSafe` |

### Bootstrap count reports and auxiliary metadata

`BootstrapCount.tla` checks the boundary between a storage shard's no-map
report and the coordinator's later initialization decision. Internal lazy
counter/version/floor spaces participate in bucket transport, but their
presence does not mean a legacy user table exists. Counting those spaces
as user tables makes an otherwise new cluster choose the legacy 3,000
bucket default instead of 30,000.

Two shards report independently, in either order; a separate coordinator
action consumes the complete reports. The bounded fixture varies user
table presence, auxiliary metadata, legacy rows, coherent existing maps,
and explicit configuration. It checks existing-map precedence, configured
count precedence when no map exists, the legacy count for real user
tables, and the fresh default when only auxiliary metadata exists. A
stored legacy row's abstract routing hash also retains its bucket when
the legacy count is inferred. `Bug_CountsAuxMetadata` changes the storage
report, leaving the coordinator unchanged; the trace reports a false user
table on one auxiliary-only shard and then chooses 3,000.

The model assumes stable committed table presence during these reports
and mutually consistent existing map counts. It does not prove concurrent
DDL/bootstrap serialization, conflicting-map reconciliation, the CRC
implementation, or migration between configured counts. The existing
`tests/bucketcount.sh` integration test is the runtime gate: configured,
legacy with persisted rows, fresh empty, and fresh configured clusters,
including row lookup/write routing and complete ownership ranges. Source
refinement must exclude auxiliary metadata only from the no-map table
presence report; ordinary bucket copy/send enumeration must retain it.
The commit and previous refinement models/configurations are unchanged.

```sh
tla/check.sh BootstrapCount Bug_CountsAuxMetadata
```

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `BootstrapCount` | 2,160 | 4 | passes |
| `Bug_CountsAuxMetadata` | 1,741 | 4 | violates `NewClusterDefault` |

### Vinyl fresh-write admission (issue #58)

`VinylAdmission.tla` checks a storage-side admission guard as a local
refinement of the existing commit protocol. Each shard has one shared
weighted-credit budget, used by both gateways and all fresh local commits
and PREPAREs. Checking and debiting that budget must be one nonyielding
action before business-row effects. The bounded pressure interval has two
gateways, two local transactions, a two-shard transaction, three refill
steps, and one leadership reset. The controller starts with zero credit;
promotion discards unused credit rather than granting a new full burst.
An admitted operation can complete after that reset without being charged
again.

A fresh PREPARE refused for admission follows the existing durable NO,
ABORT and tombstone protocol. Other participants may already be prepared;
the invariant requires no surviving business-row changes after abort,
not an absence of protocol metadata or WAL work. Duplicate durable YES
records and decided/recovered APPLY bypass fresh admission. The model
checks that those operations cannot receive an admission refusal; it does
not claim that the native engine completes them within a fixed time.
Local admission, a nonidempotent effect and its reply are separate steps.
Only an explicit before-effect `throttled` result permits a bounded caller
retry. Replaying an unknown result after a lost receipt can perform the
same mutation twice and is forbidden.

The credit cost is an estimate, not a native allocator reservation.
Tarantool exposes the effective transaction rate limit alongside dump
bandwidth and blocked writers
([pinned introspection](https://github.com/tarantool/tarantool/blob/8b9a0f59a9/src/box/vinyl.c#L255)).
Its regulator adjusts memory and compaction limits from observed progress
([regulator](https://github.com/tarantool/tarantool/blob/8b9a0f59a9/src/box/vy_regulator.c#L413));
the engine's own quota reservation/actual-use adjustment remains separate
([native quota](https://github.com/tarantool/tarantool/blob/8b9a0f59a9/src/box/vy_quota.h#L281)).
`SharedBudget` bounds charged estimates by credits earned from elapsed
refill; it does not bound actual memory, physical write amplification,
compaction debt or old decided work. A healthy bypass is outside this
pressure-interval bound; this fixture does not model entry to or exit from
a healthy period in which no admission budget is active. Runtime
refinement must establish cost accounting,
numeric/sentinel handling, role and metric freshness, cold reset behavior,
guard coverage before effects, native rejection identity, and the absolute
caller deadline. The one retry in this fixture abstracts a bounded retry
budget; it does not prove the implementation's 250 ms wall-clock bound or
bound native vinyl/quorum latency after admission. The model assumes the
existing durable YES, decision and abort semantics of `TarnCommit`; that
module and all previous model/configuration bytes are unchanged.

```sh
tla/check.sh VinylAdmission Bug_VinylAdmissionPerGatewayBudget \
  Bug_VinylAdmissionGateAfterEffect Bug_VinylAdmissionNegativeCredit \
  Bug_VinylAdmissionThrottleExistingYes Bug_VinylAdmissionThrottleDecidedApply \
  Bug_VinylAdmissionReplayUnknown Bug_VinylAdmissionResetFullBurst
```

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `VinylAdmission` | 67,072 | 20 | passes |
| `Bug_VinylAdmissionPerGatewayBudget` | 50 | 4 | violates `SharedBudget` |
| `Bug_VinylAdmissionGateAfterEffect` | 4 | 2 | violates `NoEffectRefusal` |
| `Bug_VinylAdmissionNegativeCredit` | 4 | 2 | violates `NonnegativeCredit` |
| `Bug_VinylAdmissionThrottleExistingYes` | 54 | 4 | violates `ExistingYesStable` |
| `Bug_VinylAdmissionThrottleDecidedApply` | 593 | 6 | violates `DecidedApplyNotThrottled` |
| `Bug_VinylAdmissionReplayUnknown` | 5,536 | 9 | violates `NoBusinessReplay` |
| `Bug_VinylAdmissionResetFullBurst` | 15 | 3 | violates `SharedBudget` |

### Confirmed duplicate PREPARE receipts

`PrepareConfirm.tla` refines the native visibility premise of
`TarnCommit!Revote` and admission's duplicate durable-YES bypass. It
separates the first pending native PREPARE, its confirmation or rollback,
the duplicate receipt, and the coordinator's receipt-certification step.
An unconfirmed `prepared` tuple is not a durable YES: the first transaction
can still roll back after the duplicate responds. A later idempotent
PREPARE can establish a confirmed promise if no abort tombstone remains.
The core commit protocol and previous models are unchanged.

An actual Tarantool 3.9.0 probe on both memtx and vinyl reproduced the false
YES with fsync WAL, synchronous metadata, native ABI7 ready, and a real
quorum-two wait. The second `tarn_prepare` returned `yes` while the first
was pending; the first later returned NO/error 216. A read-confirmed final
view contained no participant, decision, intent locks or business row.
The isolation control called the same unmodified procedure inside an
already-open read-confirmed native transaction. It returned conservative
NO/1213 while the first was pending. After restoring quorum one, a later
PREPARE returned YES with a confirmed prepared participant, STAGING
decision and intent locks. This control checks the isolation choice; it is
not a separate network-stream/read-set preservation proof.

The implementation begins read-confirmed only when no stream transaction
already exists. It must preserve a pre-existing stream and its reads;
normal conflict cleanup may roll that transaction back, but inspecting a
duplicate must not restart it or silently discard its read set. Supported
gateway streams already use read-confirmed isolation; this does not
certify arbitrary external read-committed streams. The
current SQL coordinator sends normal PREPARE once per participant, so the
probe establishes the native duplicate-delivery boundary, not a claim
that an observed SQL or CI failure used that schedule. The model checks
only the confirmed-receipt premise, not all PREPARE scheduling, native
replication, admission timing, or distributed serializability.

Every fresh refusal that can create a NO/tombstone follows the expiry
guard, including the missing-stream check. `TarnCommit!Prepare` already
requires `t` outside `expired`: after committed records are collected, a
late PREPARE must not recreate an aborted decision. The existing
`Recov_GC` positive gate passes 1,136,892 states; `Bug_FenceAfterExpiry`
violates `NoAbortAfterCommit` in 114,735 states. That mutant changes the
recovery branch of the same post-GC obligation, rather than directly
modeling the missing-stream branch. A paired Tarantool-runtime control
(Lua mode)
closes that refinement: on both engines the old ordering creates aborted
records after an actual COMMITTED/APPLIED transaction's owned records
are removed, while the corrected order returns `expired` without records
or business changes. Confirmed existing-record revotes still precede
expiry, since they retain a durable promise instead of creating one.

```sh
tla/check.sh PrepareConfirm Bug_UnconfirmedPrepare
```

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `PrepareConfirm` | 9 | 5 | passes |
| `Bug_UnconfirmedPrepare` | 6 | 4 | violates `CertifiedYesDurable` |

### Stream watermarks and the native steps (issue #28)

`StreamMarks.tla` checks how a shard knows, when a step commits the reads
of a stream's transaction, that the stream began in the node's current
election term (TarnCommit: `s \notin lost[t]`, for `ValidateRO`,
`CommitLocal` and `Prepare`) and after any `TRUNCATE` or `DROP TABLE` of a
table it read (design §2.10). Tarantool numbers transactions as they
begin, so the Lua module keeps the number of a transaction begun once the
node saw its term (`TERM_TXN`) and once each `TRUNCATE` or `DROP` was
applied (`DDL_TXN`): a stream's transaction below such a number began
before (design §1.5).

One node of a replicated shard loses and regains the leadership (two
term changes); a `TRUNCATE` holds, clears and releases a table, on this
node or, while it is read-only, on another leader, whose term this node
cannot lead; two streams begin, read the table, may lose their
transaction, and have their reads committed by a Lua step
(`tarn_validate`, `tarn_commit_local`, `tarn_prepare`) or a native one
(`ts_validate`, `ts_commit_local`), which hands back what it cannot
decide. The Lua step may yield between its freshness steps (`lk_fresh`,
`fence_fresh`) and its checks, which run with its commit. The native step
reads neither the term nor the stored holds: it relies on a change of the
term making the node read-only at once, and on the election triggers
running before the node is writable in the new term. After a leader
change the cache of holds is anything until read from storage again.
`CommittedReadsCurrent` says the reads a step committed were made in the
term it committed them in, of the table as it still was then, with no
hold on it.

These are local observed-term/DDL marking premises. They do not establish
that an old leader has observed a replacement before final validation.
Current ABI 14/392 replicated-stream native validation/local commit hands
back before effects for Lua's separate final-prefix fence; RF1 kernels
remain native. `StrongStreamCompletion` below supplies the additional
prefix/original-read-set composition check without a yielding native
call. Historical ABI 13 checkpoint and ABI 11/experimental ABI 12
measurements retain their original source, ABI and guarantee scope.

Each protection has a switch, and TLC finds each `Bug_StreamMarks*`
configuration violating `CommittedReadsCurrent` within seconds:

| Switch | Removes |
|---|---|
| `BugRoAfterTermChange`, `BugWritableBeforeTrigger` | the two premises of the native step |
| `BugNativeNoTermCheck` | the native step's `TERM_TXN` check |
| `BugNativeIgnoresLkStale` | its wait for a fresh lock index, whose reload notes `TERM_TXN` when the election trigger could not |
| `BugLuaNoTermCheck` | the Lua step's term check (`BK.stream_lost`), which local commits and PREPAREs did not make before |
| `BugLuaTermCheckEarly` | ... made before the step's last yield, as `tarn_validate` did before part 4 of #28 |
| `BugValidateNoFenceFresh` | reading the holds from storage again after a leader change |
| `BugCommitInNewTxn` | refusing reads whose stream's transaction is gone: local commits and PREPAREs began a new one |
| `BugDoomBeforeClear` | noting `DDL_TXN` only once the table is cleared |
| `BugDoomAfterCommit` | noting it before the catalog's commit (after it, an APPLY run again after a failed commit noted none) |

The model treats every table as vinyl, whose readers stay open: memtx
MVCC, which aborts the readers of a truncated table, and the MVCC
validation of the rows read (TarnCommit's business) only refuse more.

```sh
tla/check.sh StreamMarks Bug_StreamMarksNativeNoTermCheck Bug_StreamMarksLuaNoTermCheck
```

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `StreamMarks` | 2,134,412 | 21 | passes |
| `Bug_StreamMarksRoAfterTermChange` | 165 | 5 | violates `CommittedReadsCurrent` |
| `Bug_StreamMarksWritableBeforeTrigger` | 462 | 6 | violates `CommittedReadsCurrent` |
| `Bug_StreamMarksNativeNoTermCheck` | 13,429 | 8 | violates `CommittedReadsCurrent` |
| `Bug_StreamMarksNativeIgnoresLkStale` | 6,394 | 7 | violates `CommittedReadsCurrent` |
| `Bug_StreamMarksLuaNoTermCheck` | 3,558 | 7 | violates `CommittedReadsCurrent` |
| `Bug_StreamMarksLuaTermCheckEarly` | 5,027 | 7 | violates `CommittedReadsCurrent` |
| `Bug_StreamMarksValidateNoFenceFresh` | 70,994 | 11 | violates `CommittedReadsCurrent` |
| `Bug_StreamMarksCommitInNewTxn` | 11,392 | 8 | violates `CommittedReadsCurrent` |
| `Bug_StreamMarksDoomBeforeClear` | 68,565 | 10 | violates `CommittedReadsCurrent` |
| `Bug_StreamMarksDoomAfterCommit` | 20,376 | 9 | violates `CommittedReadsCurrent` |
| `StreamMarks_TwoTables` | 27,772,812 | 23 | passes |

`StreamMarks_TwoTables` (two tables, the native step checking the last
`DDL_TXN` of either) takes six minutes, and is left out of the default
list.

### The hand-back contract of the native procedures

`NativeHandback.tla` checks the contract between the SQL server and a
shard's native procedure that commits writes (`ts_autocommit`,
`ts_commit_local`):
- the procedure makes the step as the Lua procedure would, or hands it
  back (`["lua"]`) having changed nothing, and never once its commit
  began;
- the SQL server calls the Lua procedure after a hand-back, in the same
  stream;
- it never sends a call again whose outcome it does not know: it reports
  "outcome unknown" (1180, `Cluster::call_once`).

One transaction's writes are committed on one shard: in its stream, whose
transaction holds its reads, or as an autocommit write (`*_Autocommit`).
The native procedure takes a savepoint (or begins a transaction of its
own), makes the writes, and commits. Or it gives up, takes them back to
the savepoint and hands the step back; should that rollback fail, it
rolls the whole transaction back and replies 1213 (`txn_lost`). Its
commit succeeds, or fails having made nothing, or ends so that the writes
may or may not be made (`outcome_unknown`). The connection may drop at
any moment: its replies are lost, the requests already sent may still
run, and a stream's transaction still open when its session ends is
rolled back.

Writes are counted, not valued. `AtMostOnce`: they were committed at most
once. `ReplyHolds`: the client's OK means once, its error never; only an
unknown outcome leaves both open.

| Switch | Removes |
|---|---|
| `BugKeepWritesOnHandback` | the rollback to the savepoint before a hand-back (in a stream: the Lua procedure makes the writes again in the same transaction) |
| `BugHandbackAfterCommitBegan` | the rule that a procedure whose commit began never hands back (`committed` in `write_step`) |
| `BugResendAfterLoss` | the rule that a call whose outcome is unknown is not sent again |

```sh
tla/check.sh Handback Handback_Autocommit Bug_HandbackKeepsWrites Bug_HandbackAfterCommitBegan \
  Bug_HandbackAfterCommitBegan_Autocommit Bug_HandbackResendAfterLoss Bug_HandbackResendAfterLoss_Autocommit
```

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `Handback` | 28 | 7 | passes |
| `Handback_Autocommit` | 23 | 6 | passes |
| `Bug_HandbackKeepsWrites` | 32 | 7 | violates `AtMostOnce` |
| `Bug_HandbackAfterCommitBegan` | 34 | 8 | violates `ReplyHolds` |
| `Bug_HandbackAfterCommitBegan_Autocommit` | 23 | 7 | violates `AtMostOnce` |
| `Bug_HandbackResendAfterLoss` | 35 | 7 | violates `ReplyHolds` |
| `Bug_HandbackResendAfterLoss_Autocommit` | 29 | 7 | violates `AtMostOnce` |

The model is small on purpose: it is the contract every native procedure
that writes must keep, whatever it checks before.

The gateway also retains the selected physical connection for autocommit
Lua fallback, including cached native unavailability (#155). Only an
explicit read-only refusal permits leader rediscovery. The model abstracts
pool selection; it does not prove connection distribution or capacity use.
`cluster::fallback_tests` runs real Session SQL and storage procedures,
records physical session IDs, and holds concurrent writes before effect.
With warmed missing/blanket-handback native procedures, pools of two and
four use every connection, with one held data call per connection and
exactly one increment per statement. Separate controls cover ordinary
handbacks, native success and native/Lua read-only refusals. The existing
lost-reply faults check unknown outcomes without replay. A real socket-loss
control after a committed native-call effect checks `1180/40003`, separately
from a refused native conflict's `1213/40001`; neither calls Lua. Pool
utilization is not evidence of a latency or throughput improvement.

### Bounded native primary-key batches (issue #141)

`NativeKeyBatch.tla` checks a local read refinement, not a new commit
protocol. The request `a, b, a` includes a duplicate and an initially
missing key. A writer can insert the missing key or atomically change
both keys, with a prepared write-claim interval before publication.
A completed batch must preserve request order and duplicates, omit only
missing rows, inspect every requested key, and equal the filter-map of
one confirmed image. A commit acknowledged before the batch starts must
be visible. Both admission and final validation include write claims on
**all requested keys**, including misses; read-only range claims do not
block snapshot reads.

The native subset is memtx, autocommit with no ambient transaction,
index 0 and full primary keys, with a complete first page under bounded
work, row and stored-tuple-byte caps. Every inspected key consumes work,
even a miss. Before each key, all three budgets must still have room;
a present tuple is checked against the byte cap before appending it.
Equality after the final key can complete, but a clipped batch hands
back the whole request rather than certifying a partial result. The
model includes both complete and clipped budgets. Streams, Vinyl,
prefixes and continuations retain the existing Lua path.
Capability negotiation only selects a dispatch path; its constant reply
does not certify data, readiness, leadership, or any read guard.

The one-image argument assumes the native memtx get/check interval does
not yield, as in `TarnCommit`'s `SnapshotRead`/`DirectReadAtomic` premise.
Actual C lookup/MessagePack correspondence, parser arity, no-yield CAPI
behavior and the freshness of routing, readiness, lock and DDL mirrors
remain runtime refinement obligations. This model does not prove those
primitives, arbitrary predicates, or full distributed serializability.

```sh
tla/check.sh NativeKeyBatch Bug_KeyBatchMissingClaim \
  Bug_KeyBatchMixedSnapshots Bug_KeyBatchTruncatedComplete
```

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `NativeKeyBatch` | 105 | 9 | passes |
| `Bug_KeyBatchMissingClaim` | 101 | 7 | violates `NoMissingClaim` |
| `Bug_KeyBatchMixedSnapshots` | 174 | 9 | violates `OneSnapshot` |
| `Bug_KeyBatchTruncatedComplete` | 46 | 4 | violates `FullBatch` |

### Online engine copying and publication (issue #57)

`EngineCopy.tla` checks a local copy certificate, composed with
`EngineCutover.tla` for its later publication on two shards. The engines
are abstract images: these models do not implement or prove native DDL,
replication, full distributed DDL linearizability, or migration latency.
The source remains authoritative while its primary-only shadow is copied.
Two writes can delete/reinsert a key or move a primary key. Each write has
an unconfirmed interval and can roll back; its old and new keys are noted
before it can commit. Extra dirty keys left by rollback are harmless:
the final payload is a confirmed source reread, including absence for
DELETE, after the persistent fence and all admitted writes have drained.

A baseline source read and its owner-checked target write are separate
steps. Capture and a drain of pre-capture/native inflight effects precede
that baseline. Capture loss, overflow or a term change before PREPARED
invalidates the entire copy; the model even permits an old pending write
to resolve afterward, so the new prefix drain must cover it. A delayed
old-generation page cannot overwrite the new image. Local secondary and
UNIQUE indexes are built only after the frozen tail, avoiding transient
unique-key cycles in an online baseline. PREPARED certifies that image and
all those indexes; a restart/APPLY must preserve it, its persistent fence
and its roll-forward state instead of rebuilding it.

`EngineCutover` separates all-shard preparation/auxiliary readiness, a
confirmed central decision, native rename, local catalog publication and
old-space cleanup. A crash after rename resumes from explicit original
and replacement IDs, not whichever space currently has the public name.
A separate catalog observation retains its own committed definition ID and
JSON version. It must resolve the physical SID by that catalog ID, even
while native renames leave the public name absent or bound to the new
space before catalog publication. Deriving SID from that name mixes old
JSON with SID zero or the replacement. Neither APPLY-to-ABORT nor
ABORT-to-APPLY is permitted. Publication retires
old auxiliary generations and dooms old streams; their pending assignments
remain until an explicit retirement/ACK proof permits removal. Allocation
continues through one stable logical counter identity, including IDs that
were allocated and then deleted, rather than restarting from row maxima.

The old-stream guard covers the entire strong object group, not just the
base. A missing global-unique-index key can leave a stream with only an
old auxiliary-space read tracker: no base row was fetched. The model has
a distinct auxiliary-only reader, and `BugAuxStreamOmitted` removes just
its dooming guard. PREPARE holds and publication watermarks must cover
all old strong auxiliary IDs as well as the old base ID.

Implementation refinement obligations remain explicit:

* Capture must cover every actual source mutation before its commit;
  allocation/error paths must abort that write or invalidate the copy.
  Native capture exclusion must precede mutations/savepoints. No copied
  physical row generates a new logical lazy-index assignment.
* The source, progress/owner and terminal-receipt views must be confirmed
  where they certify completion. In particular, the central phase proof
  validates exact old/new IDs, version and generation using read-confirmed
  visibility; a missing nonterminal decision fails closed. The model's
  decision is already durable. `LazyTargetConfirm`, `LazyApplyConfirm` and
  `PrepareConfirm` explain the native confirmation boundary; runtime
  overlap/fault tests must establish this new call site's refinement.
* Existing whole-RPC DDL serialization (`BackfillDDL`), read holds and
  final DDL/term watermarks (`GroupedDDL`, `StreamMarks`) supply the
  native-yield and old-stream premises. The two stream-order premises
  documented above still apply. No extra native correctness claim follows.
  `WritePage` is a confirmed atomic native effect: a detached shadow commit
  can outlive its RPC and mutex scope. Before resetting, clearing/dropping
  a shadow or certifying PREPARED, a separate terminal-ticket and native
  sync-queue cut must settle every old shadow effect. The ticket/drain,
  queue and epoch premises compose with `LazyMeter`; this model does not
  prove that returning from a copy RPC has established that cut.
* Existing schema-job exclusion covers the entire operation versus bucket
  moves and JOIN/removal. This is a composed job premise, not an additional
  topology proof in these models. Preserve the actual source format and
  explicit native index IDs/parts/collations, including gaps; only engine,
  owned physical ID and native name change.
* A running stream's raw IPROTO SELECT may meet a retired physical ID.
  Only the typed server error `ER_NO_SUCH_SPACE` (36) falls through to the
  existing Lua read on the same client and stream with the same arguments
  and `begin=false`, skipping `ts_select`. No transport error or mutation
  is replayed and no transaction is restarted. Lua maps a known retired
  identity to 1412 and a genuinely unknown one to 1146. Existing read sets
  and `ValidateRO`/local/PREPARE lost-stream checks remain the commit
  premise; the models do not prove Rust dispatch/error-code matching.
* Fresh global/lazy target identities must be complete and READY before
  publication. Existing global-fill/lazy bootstrap proofs are reused,
  not reimplemented here. Preserve source bucket counters, row-version and
  reservation/outbox obligations or explicitly rotate/retire them without
  losing a pending obligation. READY and retirement are durable facts,
  not an inference from a missing old space. The logical auto/row-ID
  counter binding must survive the physical ID change.

The positive schedules permit source writes during baseline copying;
TLC checks safety, not a throughput or progress guarantee. Actual writer
progress, memtx/vinyl behavior, capacity refusal, rename/restart recovery,
index gaps and auxiliary integrity require independent live tests. The
final write fence includes secondary-index construction and may be long.

The independent [protocol suite](../lua/test/engine_test.lua) exercises
both directions with actual native ABI 8 loaded and with native disabled:
75 and 74 checks respectively on the recorded revision. It verifies
acknowledged writes during a paused copy, a unique-key cycle, DELETE/PK
moves, rollback extras, sparse index IDs, unchanged physical tuple suffixes,
capture recopy, stale workers, both decision-flip refusals, counter aliases
across a second migration, owned-trigger cleanup and native eligibility.
Owned caller cancellation interrupts native rename and post-publication
cleanup; this is separate from actual process-loss tests.

Two before-fix witnesses also refine the new controls. During a held
rename, the old catalog procedure returned the replacement SID with the
original definition JSON on both engines; the corrected resolver returns
the original bound pair until publication. A stream that read only an old
global-index miss could validate after publication while that unchanged
index still existed; the corrected strong-group guard returns 1213 before
its removal. These are runtime boundary checks, not a proof that every
native execution refines the models. Terminal-ticket fault tests and the
separate process-loss/replicated-promotion suites cover other premises.

```sh
tla/check.sh EngineCopy Bug_EngineCopyBeforeCapture Bug_EngineCopySkipPrefixDrain \
  Bug_EngineCopyCaptureOnlyNewKeys Bug_EngineCopyUnconfirmedSource \
  Bug_EngineCopyTailIgnoresDeletes Bug_EngineCopySkipFinalDrain \
  Bug_EngineCopyLostCaptureContinues Bug_EngineCopyStaleBatch \
  Bug_EngineCopyRebuildPrepared Bug_EngineCopyPrepareWithoutIndexes \
  Bug_EngineCopyLosePersistentFence EngineCutover \
  Bug_EngineCutoverPublishBeforeDecision Bug_EngineCutoverPublishBeforeAuxReady \
  Bug_EngineCutoverDropBeforePublish Bug_EngineCutoverAbortAfterDecision \
  Bug_EngineCutoverApplyAfterAbort Bug_EngineCutoverOldStreamValidates \
  Bug_EngineCutoverForgetOldObligations Bug_EngineCutoverResetCounterIdentity \
  Bug_EngineCutoverRenameLosesOriginalIdentity Bug_EngineCutoverCatalogByName \
  Bug_EngineCutoverAuxStreamOmitted
```

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `EngineCopy` | 5,316 | 19 | passes |
| `Bug_EngineCopyBeforeCapture` | 6,274 | 11 | violates `PreparedImage` |
| `Bug_EngineCopyCaptureOnlyNewKeys` | 3,122 | 12 | violates `PreparedImage` |
| `Bug_EngineCopyLosePersistentFence` | 2,534 | 11 | violates `PreparedImage` |
| `Bug_EngineCopyLostCaptureContinues` | 6,447 | 15 | violates `PreparedImage` |
| `Bug_EngineCopyPrepareWithoutIndexes` | 1,354 | 9 | violates `PreparedImage` |
| `Bug_EngineCopyRebuildPrepared` | 2,534 | 11 | violates `PreparedImage` |
| `Bug_EngineCopySkipFinalDrain` | 4,006 | 13 | violates `PreparedImage` |
| `Bug_EngineCopySkipPrefixDrain` | 4,048 | 12 | violates `PreparedImage` |
| `Bug_EngineCopyStaleBatch` | 6,254 | 17 | violates `PreparedImage` |
| `Bug_EngineCopyTailIgnoresDeletes` | 3,881 | 13 | violates `PreparedImage` |
| `Bug_EngineCopyUnconfirmedSource` | 269 | 6 | violates `ConfirmedCopies` |
| `EngineCutover` | 856 | 20 | passes |
| `Bug_EngineCutoverAbortAfterDecision` | 235 | 10 | violates `NoAbortPublished` |
| `Bug_EngineCutoverApplyAfterAbort` | 322 | 11 | violates `CatalogHasSpace` |
| `Bug_EngineCutoverDropBeforePublish` | 3 | 2 | violates `CatalogHasSpace` |
| `Bug_EngineCutoverForgetOldObligations` | 4 | 2 | violates `OldObligationsRetained` |
| `Bug_EngineCutoverOldStreamValidates` | 214 | 10 | violates `OldStreamsRejected` |
| `Bug_EngineCutoverPublishBeforeAuxReady` | 198 | 7 | violates `PublishedReady` |
| `Bug_EngineCutoverPublishBeforeDecision` | 241 | 9 | violates `NoAbortPublished` |
| `Bug_EngineCutoverRenameLosesOriginalIdentity` | 318 | 12 | violates `CatalogHasSpace` |
| `Bug_EngineCutoverResetCounterIdentity` | 216 | 10 | violates `CounterNeverReuses` |
| `Bug_EngineCutoverCatalogByName` | 173 | 8 | violates `CatalogObservationBound` |
| `Bug_EngineCutoverAuxStreamOmitted` | 214 | 10 | violates `OldStreamsRejected` |

### Detached source transactions

`EngineSourceDrain.tla` refines the final source drain independently of the
copy model. A source transaction admitted while the synchronous fence is
pending may sit after that fence in the native queue. Detaching its caller
removes caller tracking without settling the transaction. The correct tail
waits for native termination before its confirmed reread; a later queue cut
alone cannot repair an old payload already copied into the replacement.
The positive model explores 18 states to depth 8. The caller-only control
explores 22 states to depth 9 and violates `PreparedImage`: after the stale
tail, the detached source commits, the post-tail cut passes, and PREPARED
certifies a missing row. Capture completeness, immutable ownership and the
final exclusion of new writes remain premises of the composed models.

The native mapping is explicit, rather than proved by TLC. Pinned
Tarantool 3.9 confirms only the acknowledged prefix and stops at a newer
synchronous entry ([queue confirmation](https://github.com/tarantool/tarantool/blob/8b9a0f59a9/src/box/txn_limbo_queue.c#L649)).
A failed/cancelled wait can detach its caller while the transaction remains
pending ([transaction completion](https://github.com/tarantool/tarantool/blob/8b9a0f59a9/src/box/txn.c#L1188)).
The runtime must therefore establish its source native/queue cut before
the first tail read, in addition to caller/claim draining. A test that
injects a replica ACK prefix schedules this native boundary; it does not
claim to test actual replication transport or prove full distributed DDL.
No completed native prefix-ACK old/fixed runtime comparison is claimed:
this particular ordering is established by the focused model and pinned
source mapping. The ordinary engine runtime gates cover other schedules;
the optional private ACK-input harness is not part of the default suite.

```sh
tla/check.sh EngineSourceDrain Bug_EngineSourceDrainCallerOnly
```

### The copy of the lock index

The native procedures check locks against a copy of the Lua module's lock
index, which the Lua module changes in the same step as the index
(`crates/tarn-storage/src/locks.rs`, design §1.5). Verus proves what the
copy counts and what the checks conclude from the counts. It does not
prove when the copy changes. `NativeLockCopy.tla` models that:
- the steps that take a PREPARE's locks, before its quorum confirms it;
- the steps that release them (APPLY, ABORT, a PREPARE whose quorum
  fails);
- an election, after which the index is made again from the lock records.

Native steps run between those steps and decide from the copy alone.
`NativeFindsLocks`: no native step goes ahead while the index holds a lock
that the Lua check of the same step would find. For a write of a key, that
is any lock on it; for a read, a W lock. `CopyIsIndex`: between steps, the
copy counts what the index holds.

Each switch moves one change of the copy to another step than the
index's, and TLC must find a native step in between:

| Switch | The copy changes |
|---|---|
| `BugCopyAfterYield` | a step after the PREPARE takes its lock (a yield between, as vinyl reads would make) |
| `BugCopyReleaseFirst` | a step before a release takes the lock out of the index |
| `BugCopyOnQuorum` | once the quorum confirms the PREPARE (the index has its lock before) |
| `BugReloadNoCopy` | not at all when the index is made again: the copy stays cleared |

```sh
tla/check.sh LockCopy Bug_LockCopyAfterYield Bug_LockCopyReleaseFirst Bug_LockCopyOnQuorum \
  Bug_LockReloadNoCopy
```

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `LockCopy` | 290 | 6 | passes |
| `Bug_LockCopyAfterYield` | 16 | 3 | violates `NativeFindsLocks` |
| `Bug_LockCopyReleaseFirst` | 162 | 5 | violates `NativeFindsLocks` |
| `Bug_LockCopyOnQuorum` | 17 | 3 | violates `NativeFindsLocks` |
| `Bug_LockReloadNoCopy` | 166 | 5 | violates `NativeFindsLocks` |

`TARN_CHECK_LOCKS` checks the same at run time: the Lua module compares the
copy with the index after each change.

### Counter aliases

`EngineAlias.tla` refines the central counter-alias ensure used by parallel
engine APPLYs. Two callers request the same immutable new-SID/canonical-ID
binding. A confirmed read can see absence while another caller waits for
WAL; an overlapping write then conflicts even though its intended bytes
are identical. Serialize the whole ensure by new SID, acquire before its
read-confirmed transaction, and return an existing matching alias without
another write. Both positive configurations check that all weakly fair
executions finish both callers successfully, that an OK certifies the
confirmed binding, and that an already-installed replay writes nothing.

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `EngineAlias` | 12 | 7 | passes, including `Completion` |
| `EngineAlias_Existing` | 8 | 5 | passes, including `Completion` |
| `Bug_EngineAliasNoMutex` | 25 | 8 | violates `BothCallersOK` |

The control preserves the correct canonical alias but returns a conflict
to one caller; it does not demonstrate data corruption. The native MVCC
conflict and healthy finite-WAL premises are runtime obligations. Before
the fix, sixteen actual concurrent RPC pairs (eight absent and eight
already-installed aliases) each returned one OK and one 1213 while the
confirmed binding stayed canonical. The model does not prove native
isolation, a global/stale-owner protocol, or success under cancellation,
transport loss or an unhealthy WAL. The existing DDL interlock supplies
the immutable old/new identity premise.

```sh
tla/check.sh EngineAlias EngineAlias_Existing Bug_EngineAliasNoMutex
```

### Strong-read freshness, stream provenance and locking replies (issue #130)

`StrongReadFreshness.tla` refines the replica-freshness premise that the
commit model's atomic failover does not express. Three replicas retain
their own observed term, writable role, WAL evidence and settled effects.
Two requests race two acknowledged writes across A → B → A leadership,
one pause/resume and one partition/heal. The bounded network choices are a
healthy mesh and three faults affecting A: isolation, outgoing links only,
or incoming links only. These are representative directed networks, not
exhaustive exploration of all 64 edge subsets.

An acknowledged write leaves immutable WAL evidence on a write quorum of
two. Each request collects fresh replies from a read quorum of two, joins
their observed WAL, waits for local catch-up and settles the local sync
queue before taking its first snapshot. Every prior write quorum intersects
that request's reply quorum, so `CollectedCoversPriorAcks` follows from the
reply observations and monotone WAL; no action tests the desired freshness
predicate or reads the client's ACK floor. Catch-up and settlement then
give `StrongReadsSeePriorAcks`. A later new snapshot, including one after
an empty barrier transaction was rolled back, remains valid under this
monotone confirmed-prefix premise.

The six read controls independently admit a role-only read on a former
leader, reuse an earlier barrier, retain a snapshot taken before the
barrier, skip catch-up, skip queue settlement, or use a read quorum that
does not intersect every write quorum. Each reaches an actual strong read
missing a prior acknowledged write. The nonintersection control omits the
intermediate coverage invariant so TLC reaches that final missing-data
result. Native quorum durability, log/term safety, unconfirmed rollback,
WAL catch-up and queue settlement remain trusted replication premises;
this model does not implement Tarantool or prove its public C API mapping.

`StrongStreamCertificate.tla` checks a separate provenance obligation.
Only an owned empty transaction is marked after its first barrier and
before any read. Later Lua/native reads require the exact current native
transaction ID; an arbitrary unmarked ambient transaction is refused,
including one whose old snapshot a late barrier cannot repair. The fixture
has two stream slots, three never-reused transaction IDs and registry
capacity one. Commit/rollback cleanup removes that ID. A savepoint rollback
may conservatively remove a live transaction's certificate too: its later
read refuses rather than refreshing the old transaction. Five controls
catch late-snapshot marking, marking before the barrier, another
transaction's certificate, missing cleanup and ignored capacity. Native ID
uniqueness, terminal-trigger execution and ordering, the actual 4096-entry
bound, FFI layout and nonyielding guard/read admission remain runtime duties.

ABI 14/392 replaces the four stored LuaJIT FFI guard callback pointers with
`fresh_guard_reserved: uint64[4]` at offset 360, all zero; the six reserved
bytes at offset 314 remain unchanged. A small C shim enters the ordinary
`F.native_controls`, `F.native_space`, `F.native_mark` and `F.native_ok`
helpers through the public Lua C API before any Rust State borrow and
again after a yielding freshness wait. It protects lookup, exact uint64
argument creation and invocation with `lua_cpcall`, restores the entry
stack and maps errors or invalid results to unavailable. The helpers keep
their exact current-transaction certificate and nonyielding/no-State-access
contracts. No guard address is retained from LuaJIT's cached `CTState.L`.

Neither the TLA+ models nor the Verus kernels prove this C entry shim,
Lua's allocation/error behavior, current-fiber transaction context or the
underlying Tarantool implementation. Current ABI 14 validation must be
bound to its own source and artifact. Earlier runtime counts and ABI 13
checkpoint timings are not evidence for the changed entrypoint. A later
CI storage-process SIGSEGV prompted this investigation. Its cause has not
been established; this public-API correction is not a demonstrated causal
fix for that failure.

`StrongStreamCompletion.tla` checks a different boundary: the first-read
certificate alone does not certify final validation while a former leader
has not observed its successor's writes or term. A reader observes `X=0`,
a replacement writer stores WAL on a write quorum and acknowledges its
`X=1, Y=1` transaction, and a healthy logical shard can return `Y=1`. The
old X leader's writable flag and observed term may still look unchanged.

A separate owned child transaction obtains fresh replies from two of
three X replicas. Its reply quorum intersects the writer's two-replica WAL
quorum; `CollectedCoversStartAck` follows from those observations. Local
catch-up and queue settlement apply that history before the original
caller's MVCC probe/observed-term validation. The caller retains its exact
transaction and read set. Applying the replacement write invalidates that
old read view, so `NoFracturedCommit` follows without replacing the values
the reader already returned. A healthy `X=0, Y=0` transaction can still
commit before the writer. An unavailable final quorum supplies no progress
guarantee.

The two controls reach actual commits of `X=0, Y=1`. Skipping the final
prefix leaves the isolated former leader's old view writable. Dropping the
caller's read set after a successful prefix creates a new transaction that
passes its probe while retaining the already-returned old X observation.
Neither control stops at an early identity assertion. The bounded fixture
has one writer and one reader, two logical shards, RF3 only for X, four
representative link patterns and one heal. First-read certification, native
MVCC invalidation/probe semantics, confirmed replication and current
clock/configuration eligibility remain trusted premises. It does not prove
the child-fiber API, native handback implementation, clock certification or
safe administrative reconfiguration.

`LockReplyOwnership.tla` checks the first point/range locking-RPC reply,
where the gateway has no earlier generation to compare. Capture the role
epoch and `PL_GEN` after the barrier and `pl_begin`; retain them across
grants, row reads and yields. A final comparison with the same observed
local role/generation and live session derives `ReturnedRowsLocked` from
the grant/invalidation history. It does not assume ownership as an extra
guard. Two rows race one demote/promote cycle and one disconnect. Relabeling
the reply with the new generation, or ignoring the disconnected session,
returns rows whose locks are gone in the two controls. The relabel control
checks actual lock ownership rather than stopping at the earlier epoch
mismatch. This local proof does not promise leadership persists after the
admission barrier; existing commit validation, stream term/read-view checks,
lock/DDL history, bucket fences, FIFO and native read semantics still apply.

These checks cover default strong procedures, not a stronger contract for
raw native reads or opted-in lazy indexes. Raw reads have no request-specific
quorum barrier. A fresh base-row view does not make a lazy index complete:
recent matching rows may still be absent from its candidates.

### Record-expiry clocks and persisted admission policy (issue #130)

`RecordExpiryClock.tla` refines the core model's irreversible `Expire`
action. The positive fixture permits offsets to rise or fall while elapsed
time advances, provided each actual clock is nondecreasing and every
pairwise difference stays within `S`. Separate forward steps are allowed;
a paused process delays its actions while wall time continues. The finite
fixture has elapsed time 0–7, offsets 0–2, age limit `A = 2` and `S = 1`.
The timestamp is finite, immutable and issued before the record was written;
the issuer is included in the clock-bound premise.

Retention needs **two** skew margins when offsets vary. At the write,
`timestamp <= holder_write + S`. If GC requires
`holder_gc > holder_write + A + 2*S`, the receiving participant's clock is
`peer_gc >= holder_gc - S > timestamp + A`. Its nondecreasing clock then
keeps the transaction expired. A single margin fails even with monotone
clocks whose current pairwise differences always satisfy the bound. The
other controls exceed the bound, move a clock backward after GC, raise the
age limit, or omit/provide a future timestamp. Each permits a fresh PREPARE
after its old record was forgotten. These are premise counterexamples, not
a proof that a runtime clock monitor detects every such violation.

`LogicalClockExpiry.tla` checks a sufficient clock refinement for accepting
small raw backward corrections. Monotonic time still cannot decrease;
the accepted raw displacement is compared with the **previous logical**
wall value using the unchanged drift and observation-gap bounds. Publication
uses `max(previous_logical, accepted_raw)`. Comparing only consecutive raw
samples would permit a sequence of individually small corrections to move
farther behind the retained logical floor.

The raw-clock premise is a **fixed common reference band of width S**:
for every relevant storage node and timestamp issuer, throughout the
issue/retention/admission history, `real + L <= raw <= real + U`, with
constant `U-L = S` and nondecreasing reference time. An absolute error bound
of `S/2` about one trusted reference is sufficient. Absolute error `S`
would imply only pairwise `2*S`, and a changing contemporaneous pairwise
raw bound alone does not establish the fixed-band premise. This is an
explicitly stronger external premise; quorum, current offsets and peer
agreement cannot prove common-mode or historical reference-clock bounds.

If the previous logical value was at most `previous_real + U`, it is still
at most `current_real + U`. The accepted current raw value supplies both
the current lower bound and its own upper bound. Their maximum therefore
stays in the same band. Fresh logical observations retain pairwise bound
`S`, so the existing issuer/holder/participant argument still uses `A+2*S`.
Neither the configured skew nor drift allowance is widened. A cached old
logical sample has no current lower-band guarantee until another accepted
observation; peer interval/TTL validity and historical issuer bounds remain
separate trusted obligations. Logical certificate checks retain their
existing `S` allowance and may refuse a pair after clamping.

The finite fixture has reference/monotonic time 0–8, issuer timestamp
0–2 issued before the record write, `A=2`, `S=2`, drift allowance 2 and
maximum observation gap 3 in symbolic units. Holder/peer observations are
asynchronous. GC and fresh admission require their respective current
accepted samples. An accepted path includes raw 2 at time 0, then raw 1
at time 1: logical time stays 2 while the raw/previous-logical drift is
exactly -2. The positive checks `LogicalBand` and `NoFalseNO` in 85,476
states, depth 19.

Two controls retain the same local drift/gap checks but add an ungrounded
post-check logical floor or violate the fixed raw reference band through
finite accepted steps. Both reach actual receipt GC followed by a new
abort/NO over committed data. They fail `NoFalseNO` in 134 states/depth 4
and 4,028 states/depth 7; the intermediate band assertion is omitted in
these controls so it does not stop their traces early. These are timing-
contract boundary consequences, not complete Lua/runtime counterexamples
or a claim that production peer certificates accept those clocks. The
refinement also requires publication of the same accepted logical-wall/
monotonic pair; a second unvalidated raw sample cannot certify it. Clock
source quality, certification, rebaseline availability, native API/WAL
semantics and runtime liveness are outside this bounded check.

`RecordExpiryPolicy.tla` checks local protection after local GC. Persist an
immutable policy and atomically advance a wall high-water mark with each
destructive GC chunk. Fresh absent-record state creation uses
`max(current_wall, persisted_high)`, the fixed age limit and current clock
health. Recheck at final admission after planning may have yielded; an
unknown or changed policy/health state cannot authorize a fresh YES or
NO/tombstone. Existing durable receipts and decided finishing are outside
fresh admission. The fixture retains the high-water mark across two
restart/promotion actions and permits clock rollback and configured-age
changes. Its four controls omit the watermark, accept a mutable age policy,
check age only before the yield, or ignore uncertain health. The entry-only
control retains final health/policy checks, forcing a trace that actually
crosses expiry and GC while planning is paused.

The runtime refines the model's trusted `healthy` guard with both the
required clock coverage and current supported synchronous-replication and
control-space settings. Fresh refusal records and every destructive GC
chunk recheck that conjunction at their final transaction boundary. The
model does not inspect Tarantool configuration or prove its eligibility
checks; confirmed receipt lookup separately requires the fresh-quorum
contract above and must preserve a borrowed ambient transaction if that
lookup fails.

These checks do not establish safe administrative reconfiguration. The
quorum model fixes membership and quorum sizes, and the policy model
assumes atomic native replication. Actual quorum intersection and selected
WAL/synchronous durability must hold throughout every in-flight commit:
weakening a write quorum while the native commit yields can confirm an
already-submitted receipt or GC transaction. An entry/precommit eligibility
check cannot undo that confirmation. Drain work or preserve the guarantees
throughout overlapping configuration changes; arbitrary privileged changes
are outside this refinement.

The policy model assumes a valid immutable past-issued timestamp. It proves
local rollback protection, not global clock certification, timestamp
provenance, initial legacy-policy adoption, cross-shard high-water
propagation or native atomic replication. The old-clock/future-input
controls explain why those premises cannot be inferred from a role check,
fresh read, local elapsed-time sample or per-call future-timestamp check.

`CompletionRetention.tla` checks a separate terminal-update obligation.
Finishing decided work remains allowed while clock health is unknown, but
its receipt may have been written by a later good-clock leader. A floor
from this node's finite raw sample, last valid observation, policy creation
time and persisted high-water mark can still be below that inherited row.
The terminal helper samples that floor once, uses
`t = max(sampled_floor, existing.ts)` and
`expires = max(existing.expires, t + A + 2*S)`. It never shortens either
retained field.

The fixture uses `A=10`, `S=2`, an immutable coordinator timestamp 1100,
inherited receipt timestamps 1100–1102, two local raw-clock samples and two
persisted high-water choices. It allows a crash before the volatile clock
fault is persisted, then healthy-successor GC, another reboot retaining H,
and a late conflicting PREPARE. The positive reaches 672 states, depth 25.
Its control keeps all local clock floors but forgets both existing receipt
fields. TLC reaches an actual contradictory reopen in 56 states, depth 6:
1100/1114 becomes 1000/1014; a healthy successor at 1101 collects it and
persists H=1101; after reboot, coordinator timestamp 1100 is still fresh,
so a new abort/NO receipt is created over committed data. The control fails
`NoContradictoryReopen`, rather than an early timestamp inequality. Keeping
either an adequate old horizon or its timestamp would block this shortening;
the implemented contract retains both.

This refinement assumes the inherited receipt and coordinator timestamp
were valid, the successor's clock/eligibility evidence is sound, and GC/H
and user/protocol writes are atomically durable. It does not establish
initial clock history, health certification, fault persistence before a
crash, or native WAL semantics.

The nine positive outcomes are:

| Configuration | Distinct states | Depth | Principal invariant |
|---|---:|---:|---|
| `StrongReadFreshness` | 2,443,260 | 31 | `StrongReadsSeePriorAcks` |
| `StrongStreamCertificate` | 24,180 | 19 | certificate provenance, exact ID, lifetime and capacity |
| `StrongStreamCompletion` | 3,369 | 19 | `NoFracturedCommit`, `CollectedCoversStartAck` |
| `LockReplyOwnership` | 184 | 12 | `ReturnedRowsLocked`, `ReplyKeepsEntryGeneration` |
| `RecordExpiryClock` | 1,092 | 9 | `NoResurrectedPrepare` |
| `RecordExpiryPolicy` | 1,368 | 23 | `NoUnsafeAdmission` |
| `CompletionRetention` | 672 | 25 | `NoContradictoryReopen` |
| `LogicalClockExpiry` | 85,476 | 19 | `LogicalBand`, `NoFalseNO` |
| `DepartureDependencies` | 564 | 17 | `NoOrphanPrepared`, `NoOrphanStaging`, `NoNewDependencyAfterDrainACK` |

All 42 focused configurations (nine positive and 33 expected controls) are
generated and included in the default list. The first 36 belong to the
validated 246-case union below; six departure checks passed separately
through the registered checker with those prior inputs unchanged. This is
not a claim that the entire runtime CI suite passed. Per-control states and violated invariants are
in the generated [design results table](design.md#51-model-checking).
Run only these checks with the same outcome checker as CI:

```sh
configs=(StrongReadFreshness StrongStreamCertificate StrongStreamCompletion LockReplyOwnership RecordExpiryClock RecordExpiryPolicy CompletionRetention LogicalClockExpiry DepartureDependencies)
for family in StrongRead StrongStream StreamCompletion LockReply RecordExpiry RecordPolicy CompletionRetention LogicalClock Departure; do
  for cfg in tla/Bug_${family}*.cfg; do
    name=${cfg#tla/}
    configs+=("${name%.cfg}")
  done
done
tla/check.sh "${configs[@]}"
```

### Recovery and garbage collection move on (issue #129)

Recovery and garbage collection go through records in order: undecided
transactions oldest first, expired records by expiry. Some of them cannot
be acted on while a shard is down: a transaction that needs it cannot be
resolved, and some records cannot be collected. `RecoveryFairness.tla`
models the order of the passes (`lua/tarn.lua`, "Scheduling"), not what
they do with a record, which `TarnCommit.tla` models:

- a pass looks at no more than `Scan` records, and acts on no more than
  `Batch` of those it may act on now (`recovery_batch`, and five times as
  many; `gc_batch * 4`, and `gc_batch * 40`);
- it continues after the last record the previous pass looked at, and a
  pass that reaches the last record starts the next lap from the first;
- a record it could not act on waits `Backoff` passes.

The stuck records are any set of them: TLC tries each. `BoundedWait`:
within ceil(N / Batch) + 1 passes, every record that is not stuck has been
acted on. `BugPrefix` starts every pass from the first record, as both did
before: TLC finds stuck records first that keep the others from being acted
on (as many as a pass looks at, and no pass looks past them).

The bound counts scheduled passes with a fixed stuck set, rather than wall
time or arbitrary changes in availability. It does not establish clock
certification, quorum availability or scheduler fairness. The freshness,
expiry and persisted-policy refinements above still supply those separate
admission and deletion obligations.

Runtime GC also filters each record by its relevant writer-clock evidence
before forming a batch's required-writer union. An unknown record remains
retained without poisoning an unrelated eligible record in the same scan
window. After remote status checks, the deletion transaction still rechecks
current coverage, epoch and supported controls, and advances the persisted
high-water mark atomically with deletion. Coverage lost after a status proof
cannot authorize that deletion. These eligibility and final-guard premises
compose with cursor ordering; `RecoveryFairness` does not prove their clock
soundness. The recovery fault fixture exercises both the small mixed-health
batch and coverage expiration after the status proof, followed by recovery
once the missing writer resumes.

```sh
tla/check.sh Fairness Bug_FairnessPrefix
```

| Configuration | Distinct states | Depth | Expected result |
|---|---:|---:|---|
| `Fairness` | 2048 | 8 | passes |
| `Bug_FairnessPrefix` | 1281 | 6 | violates `BoundedWait` |

Both have eight records, `Batch` = 2, `Scan` = 3 and `Backoff` = 2.
`lua/test/recovery_faults.sh` checks the same on three shards, one of them
stopped, with 250 transactions that need it on each of the other two
(`recovery_batch` is 200) and more records than a garbage-collection pass
looks at.

### Decision-record placement (issue #132)

`TarnCommit!DecisionParticipant` abstracts a stable choice from frozen writing
participants. `Placement_Recov`, `Placement_Failover` and `Placement_RC`
override it with a transaction-specific choice that places the common
two-shard writer's record on B. They check the existing commit invariants
with coordinator crashes/message loss and GC, leader failure, and READ
COMMITTED locks, respectively. The tombstone and expiry-fence controls are
also checked with that placement. This checks another placement of the
record without changing the commit or GC guards.

`DecisionPlacement.tla` checks separate local premises on three shards:
every nonempty writing set, read-only participants, every active membership,
and a membership change before or after PREPARE. Selection prefers an
active writer, falls back to a writer when none is active, and keeps that
identity in durable records and recovery lookups. A leaving candidate is
refused rather than redirected. With hashing abstracted by any eligible
choice, the positive configuration has 1,970 states; allowing a read-only
decision, ignoring active writers, and selecting again during recovery
violate their invariants in 162, 252 and 1,142 states, respectively.

These are bounded protocol/identity checks. They do not prove uniform hash
distribution, the membership service, Lua serialization, clocks, storage
durability, or behavior for arbitrary cluster sizes. Rust tests check
canonical durable-ID ordering, gateway position changes, active preference
and realistic transaction-ID spread. `tests/decision.sh` checks actual
participant/decision records on three shards, excludes read-only decision
participants, keeps the local path free of distributed records, drops a
PREPARE reply through a TCP proxy, and kills its owned coordinator after
all participants prepared at a non-first decision writer. Recovery must
apply exactly once using the persisted identity. CI runs this with native
memtx, Lua-only memtx and vinyl tables.

```sh
tla/check.sh DecisionPlacement Bug_DecisionReadOnly Bug_DecisionIgnoreLeaving Bug_DecisionReselect \
  Placement_Recov Placement_Failover Placement_RC Bug_PlacementNoTombstone Bug_PlacementFenceAfterExpiry
tests/decision.sh
TARN_NATIVE=0 tests/decision.sh
ENGINE=vinyl tests/decision.sh
```

The recorded `bench/decision_placement.py` results use repeated, matched placement measurements:
new RF1/fsync clusters for each build/sample, disjoint rows per client and
one write per shard per transaction. It checks all committed tuples and
recorded identities, captures per-shard participant/decision records and
WAL rows/file growth, then waits for the eligible records' GC and counts its
deletions and WAL. Their shorter retention and zero clock-skew allowance
were confined to owned single-host fixtures before the persisted timing
policy. The current driver supplies matching timing environment variables
before cluster startup and requires a matching gateway age limit. Adapting
that setup does not rerun the historical measurements. This is a placement workload;
it does not establish an RF3 performance or operational advantage.

### Drain every dependency before removing a shard (issue #130)

`DepartureDependencies.tla` checks the protocol boundary that permits a
logical shard's storage to stop. A survivor's PREPARED or STAGING record
can still name the retiring shard in its participant set even when another
shard holds the decision. Moving its buckets and counting only records
whose decision is the retiring shard does not establish that it is unneeded.
Recovery still needs the absent participant to obtain a safe fence; an
unreachable or removed peer is not a NO vote or an invented NONE receipt.

The finite model has decision shard 1, retiring participant 2 and survivor 3,
one delayed operation, and per-node draining publication. The operation
creates either a survivor PREPARED receipt or a decision STAGING receipt.
Participant 2 has no receipt. Every node must acknowledge draining before
any zero-dependency gate is read. The gates count all confirmed PREPARED
and STAGING references, including non-decision participants, and operation
dependencies registered before the first yield and held through commit or
rollback. Counts can conservatively overlap; only zero versus nonzero matters.

New admission checks both current membership and its transaction's member
read, including commit validation of that read. The network can deliver a
request after a gate already returned zero. Closing new admission prevents
that request from creating a dependency. An old already validated promise
can instead be pending WAL when draining is acknowledged and confirm later;
its in-flight registration must keep removal blocked until the receipt is
visible and resolved. `NoNewDependencyAfterDrainACK` concerns first
admission after all acknowledgements, rather than delayed confirmation or
YES replies from an earlier admission. It is checked at actual durable
receipt creation, not merely at an early boolean guard.

The positive checks `NoOrphanPrepared`, `NoOrphanStaging` and that admission
property in 564 distinct states, depth 17. With weak fairness of publication,
gate, request and recovery actions, `HealthyDrainCompletes` also checks
eventual removal when required clock health remains available.
`DecidedCompletion` checks that an existing decision eventually releases
its receipts even if the local clock faults. New absence-based fencing
still requires health; persistent unknown health can intentionally prevent
removal. Healthy needed peers, sound membership/MVCC/WAL semantics and
balanced runtime counters remain premises, not conclusions of this model.

The five controls reach these concrete consequences:

| Configuration | Distinct states | Depth | Expected violation |
|---|---:|---:|---|
| `Bug_DepartureDecisionOnly` | 301 | 14 | `NoOrphanPrepared`: a non-decision participant reference survives removal |
| `Bug_DepartureLateAdmission` | 490 | 15 | `NoOrphanPrepared`: a delayed request creates its receipt after the peer stops |
| `Bug_DepartureIgnoreStaging` | 301 | 14 | `NoOrphanStaging`: the unresolved decision still needs the stopped participant |
| `Bug_DepartureUntrackedWal` | 341 | 14 | `NoOrphanPrepared`: an old pending promise confirms after an untracked gate passes |
| `Bug_DepartureNoCommitCheck` | 208 | 9 | `NoNewDependencyAfterDrainACK`: an old transaction image creates a new durable promise after draining acknowledgements |

In the first four traces participant 2 is actually offline, the survivor's
receipt is durable, the missing participant has no receipt, and no decision
exists. Recovery cannot fabricate its missing fence. The late-admission
control omits the earlier admission assertion to reach this actual orphan.
The commit-validation control creates an actual late durable promise;
tracking can still prevent its removal, so it does not claim an orphan.

Confirmed-row visibility and pending WAL/RPC lifetime are deliberately
separate in this model. A runtime fresh-prefix wait may independently
settle pending WAL; the model does not rely on it as a substitute for
tracking the request. This conservative boundary is not a native WAL or
full-runtime refinement proof. It leaves the existing no-vote/expiry,
immutable decision identity and uncertain-outcome contracts unchanged.

```sh
tla/check.sh DepartureDependencies Bug_DepartureDecisionOnly Bug_DepartureLateAdmission \
  Bug_DepartureIgnoreStaging Bug_DepartureUntrackedWal Bug_DepartureNoCommitCheck
```

### Plain lazy-source binding in an existing stream (issue #161)

`PlainLazyBinding.tla` checks a narrow negative certificate for an existing
marked `READ_CONFIRMED` stream. Before any initial source image or physical
write, the original transaction observes every actual write source's catalog
identity/version and empty lazy declaration, absent/empty capture point,
and absence of nonretired pending definitions. These reads remain in the
same native transaction as its earlier user history. Coverage, transaction
and source identity must still match through capture and the final guard;
a mismatch refuses the request rather than rebinding it to a new registry.

The bounded model has two possible written sources, one original user read,
one metadata/history change and an unrelated nonlazy publisher. Publication
may still be pending or already complete. That unrelated change can dirty
the global cache while the original plain write commits its one increment.
Relevant point/range changes and previous user-history conflicts remain
sticky and prevent acknowledgement. A full pending-definition scan may
conservatively conflict with unrelated lazy DDL; progress for that case is
not promised. Global native exclusion stays unknown throughout the dirty
interval, and engine, role, fence, clock and lock guards remain mandatory.

The positive check explores 115 distinct states to depth 7. Six controls
reach data consequences rather than failing only a certificate flag:

| Configuration | Distinct states | Depth | Expected violation |
|---|---:|---:|---|
| `Bug_PlainLazyDeclared` | 10 | 7 | `NoMissingMembership`: an acknowledged row lacks required membership after publication/drain |
| `Bug_PlainLazyCapture` | 10 | 7 | `NoMissingMembership`: an omitted capture-point obligation leaves the same missing membership |
| `Bug_PlainLazyPending` | 10 | 7 | `NoMissingMembership`: a pending-generation obligation is incorrectly exempted |
| `Bug_PlainLazyFirstWriteOnly` | 10 | 7 | `NoMissingMembership`: the second actual write source was not proved plain |
| `Bug_PlainLazyReplaceReadset` | 9 | 6 | `NoStaleReadCommit`: a new transaction overwrites an acknowledged value using the original stale read |
| `Bug_PlainLazyGlobalStale` | 15 | 7 | `UnrelatedOneCommit`: unrelated nonlazy publication incorrectly prevents the original increment |

Metadata shapes are independent local admission inputs, including partial
or malformed state; the model does not derive their complete DDL lifecycle.
Its pending-forward case has already sampled a baseline, so an incorrectly
exempted write has no outbox obligation to repair the missing membership.
Native confirmed visibility, point/range tracking, sticky MVCC conflict and
ordinary commit validation remain runtime refinement premises. This is not
a proof of native MVCC or all DDL scheduling. It grants no exemption to
autocommit/RW, `CommitRC`, PREPARE, unmarked streams, active/pending lazy
sources or mixed packets. Ineligible packets retain the original strict
guards. In particular, read-committed prepared-delete/retirement visibility
and rollback coverage are outside this certificate's scope.

```sh
tla/check.sh PlainLazyBinding Bug_PlainLazyDeclared Bug_PlainLazyCapture \
  Bug_PlainLazyPending Bug_PlainLazyFirstWriteOnly \
  Bug_PlainLazyReplaceReadset Bug_PlainLazyGlobalStale
```

### Re-entering one engine BUILD after a lost cut (issue #165)

`EngineBuildReentry.tla` starts one original BUILD at a successful-return,
pre-effect checkpoint. A saved baseline page predates a known native cut
invalidation. Before further effects, BUILD discards the page, reacquires
current terminal-ticket and queue evidence, and rereads ownership, phase
and capture. Cut-only loss keeps the copied prefix and captured dirty keys;
capture loss resets the incomplete generation and recopies. An uncertified
`preparing` receipt with an invalid track follows that recopy path.
Certified `prepared`/`applying`/`published` images are handled before reset.
A newer cut epoch invalidates a saved page even if readiness has already
been rebuilt by the time the original request resumes.

The two-key positive model checks `PreparedImage`, `OwnerCertified`,
`CertifiedRetained` and `CutOnlyNoRecopy`, exploring 1,500 distinct states to
depth 14. Under one finite invalidation and weak fairness of native
terminal/queue completion and the original BUILD's successful actions,
`OriginalBuildCompletes` reaches certification or an ownership refusal.
Three sensitivity controls reach an actual wrong PREPARED shadow image:

| Configuration | Distinct states | Depth | Expected violation |
|---|---:|---:|---|
| `Bug_EngineBuildReusePage` | 747 | 10 | `PreparedImage`: an old payload is relabelled with a current capture/cut lease |
| `Bug_EngineBuildCallerOnlyCut` | 2,058 | 13 | `PreparedImage`: a submitted old effect outlives its caller and overwrites a new page before final queue settlement |
| `Bug_EngineBuildKeepPrefix` | 764 | 9 | `PreparedImage`: an incomplete prefix misses a source change during lost capture |

Every control retains the generic owner, current-cut and capture checks.
Their final states have settled old tickets/queue and current ownership,
but shadow data differs from the source. Old pending shadow effects and
their terminal mapping are conservative compositional inputs; the model
does not claim that the observed CI builder had such an effect or prove
its full native reachability. Confirmed page effects and final source-tail
ordering retain the `EngineCopy`/`EngineSourceDrain` premises.

There is no retry action for helper, page-transaction, final-guard or commit
exceptions, no second RPC, and no replay of an uncertain effect. The runtime
keeps its original deadline and eight-reset limit; this finite model does
not establish completion within either bound, under repeated invalidation,
or after arbitrary mid-helper failures. Native WAL, replication and the
full runtime remain outside this local contract.

```sh
tla/check.sh EngineBuildReentry Bug_EngineBuildReusePage \
  Bug_EngineBuildCallerOnlyCut Bug_EngineBuildKeepPrefix
```

### Configuration counts

The default list contains 268 unique configurations: 70 positive
outcomes and 198 expected broken/trade-off outcomes. The 275 generated
configurations reproduce from `gen_cfgs.sh`; six grouped-read and decision-
placement configurations are maintained directly, for 281 total. Explicit
check arguments override the default list.

The premerge issue #130 list completed all 219 expected outcomes (57
positive, 162 controls). A separate frozen merged 238-case run completed
63 positive outcomes and 175 expected violations after the `MC` and
decision-placement changes. The three additional stream-completion cases
then passed the registered checker with one positive and two expected
violations. The original 238 model/config inputs and module dispatch were
checked unchanged, forming a validated 241-case union. Two completion-
retention cases then passed separately with those 241 model/config inputs
and dispatch unchanged, forming 243 cases. Three logical-clock expiry
cases then passed through the current checker, with all 243 prior model/
config bytes and dispatch checked unchanged. Their exact argument union
was the 246-case default list. Six departure-dependency checks then passed
the registered checker with all prior model/config bytes and module
dispatch unchanged, forming the then-current 252-case input union. Its generator
reproduced twice without changes. CI run
[40827360444](https://github.com/cstheorist/tarn/actions/runs/40827360444)
then executed all 252 cases with expected outcomes, including 67 positive
checks and 185 sensitivity/tradeoff controls. All four jobs passed; Verus
verified 140 bodies with zero errors. The actual checkout
`5eeab61b3c6b724ed2c6bc5fe859ecb942a5cbde` has the same tree as runtime
revision `554ef08`. The 317 proof inputs retain exact source bindings.
The final report-head CI/validation attestation is a separate publication
gate; a previous run is never represented as execution of changed models.
The seven plain lazy-binding cases subsequently completed the registered
checker with the outcomes above. The original 252-case prefix, model/config
bytes and module dispatch remain unchanged. CI run
[40876376448](https://github.com/cstheorist/tarn/actions/runs/40876376448)
then completed all 259 cases with 68 positive and 191 expected control
outcomes; all four jobs passed, including 140 Verus bodies with zero errors.
The four BUILD re-entry cases subsequently completed separately, preserving
the original 259-case prefix, model/config bytes and module dispatch. This
formed the 263-case union. The five file-backup cases then completed
separately with one positive and four expected violations; the combined
registry contains all 268 inputs. This is an input union, not a claim that
a previous CI run executed changed models. The generator reproduces
without changes. The current proof inventory is 336 files: 320 TLA+
model/config files, 11 verified-crate source/test/manifest files and five
formal runner/inspection scripts. Verus bodies are unchanged.

## Verus proofs

`crates/tarn-verified` is ordinary Rust inside `verus!` blocks: `cargo`
builds it with the specifications and proofs erased, and the server uses the
result. Verus proves:

* **Native clock observation arithmetic** (`clock.rs`): `observation_ok`
  exactly decides monotonic nondecrease, a bounded observation gap and
  `abs((raw_wall - previous_logical_wall) - elapsed_mono) <= max_drift`,
  including bounded raw backward corrections. All subtractions are free of
  overflow/underflow for every `u64` input. `logical_wall` returns exactly
  the maximum of the previous logical and raw values. A lemma preserves
  one clock's absolute band around a nondecreasing reference; it does not
  derive a pairwise bound from that error parameter. The executable bodies
  are shared with native admission. These are arithmetic proofs, not clock
  source, publication, remote-offset, persisted-policy, quorum or GC proofs.
* **Native batch budgets** (`batch.rs`): `can_step` permits the next lookup
  exactly while work, rows and bytes each have room. `step` either rejects
  the whole native attempt or charges one work unit, one row for a present
  tuple and exactly its stored size, without overflow or exceeding any
  cap. Missing keys still charge work; a final tuple may exactly exhaust
  a cap, while another key must hand back. This proves budget arithmetic,
  not native tuple lookup, MessagePack framing or snapshot isolation.
* **Routing** (`route.rs`): the CRC-32C used for bucket hashing equals a
  bitwise specification for every input; bucket and shard numbers are in
  range; the bucket-to-shard map of a new cluster is monotone and onto; the
  map built from the buckets the shards report owning gives each bucket the
  shard of the report that holds it, or none, and refuses reports that
  claim a bucket twice.
* **Coordinator decisions** (`coord.rs`): `decide` commits exactly on
  unanimous YES and aborts exactly when some vote is NO; `resolve_step`
  (one step of recovery) decides COMMIT only when the states it is given
  mean "committed", decides ABORT only when a participant is tombstoned,
  fences only participants without a record while the record is STAGING,
  and fences a missing record only before expiry or while some participant
  is prepared.
* **Global unique indexes** (`uix.rs`): applying the entry writes that
  `index_delta` decides for a write (a row's old version replaced by a new
  one, or none) to an index that holds the old version's entry leaves the
  other rows' entries alone and holds the new version's instead; it writes
  nothing when the entry does not change, deletes only an entry that
  exists and inserts only one that should.
* **MySQL wire codec** (`wire.rs`): length-encoded integers and strings
  encode to the byte-level specification and decode back without reading
  out of bounds; packet framing produces exactly MySQL's multi-packet
  format (including the empty terminator after a payload that is a multiple
  of 16 MiB) and deframing inverts it. No function can panic.
* **MessagePack of the native procedures** (`mp.rs`): the writers emit
  Tarantool's encodings: an integer unsigned when it is not negative, and
  every integer and header in its fewest bytes (no encoding of the same
  integer, and no header of an array, a map, a string or a binary of the
  same length, is shorter). The readers read back what the writers emit.
  `skip` ends where a value ends, however nested, and refuses a value cut
  short or a byte no value starts with. No function can panic or read past
  its buffer, whatever bytes it is given.
* **Lock checks of the native procedures** (`locks.rs`): the counts that
  the native module's copy of the lock index keeps for each key (the
  transactions with an R lock, with a W lock, with a row lock) and for each
  index (its W locks) stay the census of the locks the Lua module holds,
  through every change the Lua module copies to it (a lock taken, raised
  from R to W, released). With counts that are the census, a key or an
  index the native checks pass has no lock that the Lua module's checks
  (`point_w_locked`, `range_w_locked`, `write_conflict`, `write_plocked`)
  would find, whoever reads or writes. A key that the comparisons with the
  bounds of a range put outside it (`within`, which takes the bounds as
  included) is outside it for the Lua module's `in_range`, whichever bounds
  that includes. That the Lua module copies every change is checked at run
  time instead (`TARN_CHECK_LOCKS`, below).

The proofs are about single functions given their inputs. Protocol-level
facts they rely on, such as "a missing participant record under a STAGING
decision record means the participant never prepared", are checked by TLC.

Running Verus needs the release the proofs were written for and the Rust
toolchain it is built with (1.98.1, the version in `rust-toolchain.toml`):

```sh
rustup toolchain install 1.98.1
curl -fsSL -o verus.zip https://github.com/verus-lang/verus/releases/download/release%2F0.2026.10.04.426d8b0/verus-0.2026.10.04.426d8b0-x86-linux.zip
unzip -q verus.zip -d ~/verus
cd crates/tarn-verified && ~/verus/verus-x86-linux/verus --crate-type=lib src/lib.rs
# verification results:: 140 verified, 0 errors
```

`crates/tarn-verified/tests/erased.rs` exercises the erased code with
concrete inputs (codec boundaries, MessagePack encodings at their
boundaries and values skipped however nested, CRC check value, decision
cases).

## Test suites

| Suite | Command | Needs | Checks |
|---|---|---|---|
| Unit tests | `cargo test --workspace` | Tarantool (IPROTO tests start their own) | codec, packets, SQL normalization, expressions, collation weights, coordinator decisions, the cross-shard deadlock search, IPROTO client against a real Tarantool |
| Storage protocol | `lua/test/run.sh` | Tarantool | every participant procedure on two real shards, recovery, record lifecycle and garbage collection (also past transactions that need a stopped shard: `recovery_faults.sh`), refusal on demoted nodes, lock-index rebuild while a PREPARE waits for a quorum, row locks, schema-change steps and definition fences; bucket moves step by step, and the refusals of requests for buckets a shard does not own (`bucket_test.lua`); the native read procedure against `tarn_select`, read by read, autocommit and in transactions, to end keys of plain, nullable and collated indexes, and the native autocommit writes, one-shard commits and validations against `tarn_autocommit`, `tarn_commit_local` and `tarn_validate` on twin tables and streams: the same replies, and rows of the same bytes, for what they serve, every other kind handed back having changed nothing, and the commits in flight waited for; the lock checks of each, key by key, with the keys in ranges and the ranges' bounds compared as the Lua checks compare them (collations, prefixes, NULLs; `native_test.lua`). `TARN_NATIVE=0` runs them without the native procedures. The shards run with `TARN_CHECK_LOCKS`: after each change of the lock index, the Lua module compares the native module's copy of it with it, and a difference fails the run |
| SQL differential | `python3 tests/sql/difftest.py --tarn HOST:PORT --ref HOST:PORT` | a Tarn cluster and MariaDB 10.11, PyMySQL | 967 statements compared exactly with MariaDB: column names, rows, error codes, affected rows, final table contents and indexes (also after `ALTER TABLE`), including collations and their weight strings |
| SQL fuzzing | `python3 tests/sql/fuzzdiff.py --tarn ... --ref ... -n 2000` | same | randomly generated queries compared with MariaDB |
| Transactions | `python3 tests/sql/txntest.py --tarn HOST:PORT --kv SHARD1,SHARD2` | a Tarn cluster | rollback, visibility, write skew, lost update, phantoms, fractured reads, real-time order, cross-shard atomicity, direct KV reads, concurrent inserts of case variants of a case-insensitive key, and a concurrent bank workload with a ledger that exposes lost or duplicated commits |
| READ COMMITTED | `python3 tests/sql/rctest.py --tarn HOST:PORT[,HOST:PORT...]` (every SQL server) | a Tarn cluster with two or more shards | isolation variables, lock waits, lock wait timeouts, local and cross-shard deadlocks (also across SQL servers), no deadlock-detector calls without lock waits, locks of failed lock requests released with their transaction, concurrent counters without lost updates, semi-consistent `UPDATE`/`DELETE`, `SKIP LOCKED` job queues, `NOWAIT`, `FOR SHARE`, duplicate inserts and concurrent upserts on unique keys, savepoints, statement rollback, locks released on disconnect |
| Schema changes | `python3 tests/sql/ddltest.py --tarn HOST:PORT[,HOST:PORT]` | a Tarn cluster, ideally with two SQL servers (`cluster.sh -s 2`); the `mysql` client | a mysqldump file, multi-statement batches and `COM_SET_OPTION`, refused alterations, `ALTER TABLE` about 300 times while both servers write the table: no autocommit statement fails, no acknowledged write is lost or duplicated; concurrent schema changes; transactions spanning an `ALTER` |
| Bucket moves | `python3 tests/sql/movetest.py --tarn HOST:PORT[,HOST:PORT...]` | a Tarn cluster with two or more shards | transfers with a ledger, READ COMMITTED read-modify-writes, audits and READ COMMITTED scans of every shard while the buckets of their rows keep moving: nothing lost or applied twice, every scan sees each row once |
| Move faults | `tests/movefaults.sh` | Tarantool | the SQL server driving a move killed while it copies, while its fence drains, between its flip and its send, and between its send and its receive, and after its flip with the move given up on the sending shard: another server gives the move up, or finishes it |
| Failover | `tests/failover.sh` (`ENGINE=vinyl`: vinyl tables) | Tarantool | 2 shards × 3 replicas under the bank workload, READ COMMITTED transfers and `movetest.py`: a live leader change, then two leader kills; every check must hold |
| Bucket count | `tests/bucketcount.sh` | Tarantool | the bucket count of a new map: the configured one, 3,000 on a cluster made before bucket moves (whose rows use it), 30,000 on a new cluster |
| DDL faults | `tests/ddlfaults.sh` | Tarantool | `CREATE TABLE`, `DROP TABLE` and `TRUNCATE` with a shard down, and with a shard killed while it applies: each takes effect on every shard or on none, checked on the shards directly |
| Faults | `tests/faults.sh` (`ENGINE=vinyl`: a vinyl table; `TARN_NATIVE=0`: a shard without the native procedures, whose absence the SQL server must notice) | Tarantool | a reply lost after an autocommit write was applied, by the native procedure or by `tarn_autocommit`: the client must get "outcome unknown" and the write must be applied exactly once |

`bench/baselines.sh start mariadb` starts a local MariaDB on port 3407 with
a `sb` user for the differential tests (or point `--ref` at any MariaDB
10.11). `scripts/cluster.sh start -n 2 -r 1 -p 4000` starts a cluster.

The heap-watchdog fixture retains the 40 MiB garbage/64 MiB margin setup,
then stops pressure near 2 MiB free, keeping at least 1 MiB for native-core
work. It requires restored heap headroom, successful calls and a live lazy
worker without a manual recovery collection. The final clock/native
eligibility preflight uses `PG.heap_available`, which observes free space
without requesting a collection. An earlier-build
isolated control that cancelled only the heap-watchdog fiber failed its
collection assertion under the same bounded pressure. That historical
control is not a sensitivity result for the current build. Earlier
unbounded pressure reproduced a
native `cfg_get('read_only')` panic in both native and Lua modes; active
and stopped lazy-worker controls did not establish worker causation.
Tarantool's pinned [`cfg_get` implementation](https://github.com/tarantool/tarantool/blob/8b9a0f59a9/src/cfg.c#L40)
evaluates a Lua chunk and panics on a Lua error. The fixture verifies Tarn's
near-limit garbage recovery with native-core headroom; it is not a proof
that arbitrary privileged EVAL allocations can safely exhaust the hard
Lua heap limit.

### RF3 fixture readiness and copy diagnostics (issue #165)

A confirmed mutation does not pin volatile lazy caches. The production
`box.status` callback invalidates capture and quota even when the current
role is unchanged. Capture is refreshed on entry; the separate quota worker
rebuilds its confirmed cut even while shipping is paused. The RF3 fixture
therefore drives an empty ordinary autocommit and waits under its original
25-second readiness bound before applying the original exact quota,
capture, tuple, reservation, sequence and floor assertions. Selected
durable summaries are equal before and after those empty entries; this is
not a byte comparison of every database payload.

The UNKNOWN probe holds background quota rebuild across the strong-read
barrier and observes `ready()` at the actual production `q.change` guard.
It requires a nonempty set of false observations, error 1213, a live worker,
and unchanged byte images of the selected base/outbox/reservation/counter/
slot/floor spaces. The original guard still decides admission. Both test
wrappers are restored before recovery continues.

Engine copying keeps one original BUILD attempt. On the original
30-second copy-hook deadline, diagnostics retain its return or exception,
fiber state, job, capture epoch/tickets, source hash, shadow/receipt, native
cache, queue, leader and clock state. Each diagnostic RPC has a separate
one-second bound, after the original wait; a failed controller prints the
last 80 lines from each of its three owned process logs before cleanup.
No diagnostic resends BUILD. A live source-definition mismatch proves the
real 1412 refusal is retained with unchanged source/job and no shadow or
receipt. Exception and sleeping-worker controls are explicitly synthetic;
they test observation, not a claimed reconstruction of the historical
unlogged timeout.

`lua/test/run.sh` runs the normal RF3 suites, all three copy diagnostics
controls and a held-notification lazy regression in native and Lua-only
modes. The latter delays the actual initial `box.status` callback until
after confirmed PREPARE/CAS, then releases all initial callbacks and the
restarted replica's callback before later process loss. Its test-only
preload arms after configuration and before Tarn initialization. It uses
the actual notification and invokes the original production callback,
with no general worker liveness claim. Production Lua, native code, protocol and model
inputs are unchanged. The original failed CI execution and every local
control, including nonqualifying setup failures, are retained separately
in the [evidence index](../tests/evidence/rf3-fixture-readiness/index.json).

### Plain-table commits during unrelated DDL (issue #161)

An unrelated ALTER can invalidate the global lazy registry while a yielding
Vinyl write runs in a serializable stream. A definite 1412 rollback is safe
but unnecessary when every written source has no lazy index. CommitLocal
now reads each source's catalog identity/version, declared lazy list and
capture row or absence, plus the pending-definition scan, in the original
marked READ_CONFIRMED transaction before its first row image. The private
request-local binding covers the complete write packet and exact native
transaction ID. Its later checks refuse any identity or coverage mismatch.
It never refreshes or clears global lazy/native exclusion state.

Applicable metadata publications and earlier user reads remain in that
transaction's MVCC read set through commit. This relies on Tarantool's
point-hole and iterator tracking, including conflicts before the first
write, across memtx and Vinyl. It is a storage-engine premise rather than
a Verus proof of Tarantool. The full pending-definition scan may conflict
conservatively with unrelated lazy DDL and has cost proportional to that
registry. Active or pending lazy sources, unmarked transactions,
autocommit, READ COMMITTED and PREPARE retain the existing strict path.
Engine, ownership, source-version, role, clock and lock checks still apply.
No caller transaction is replaced and no SQL COMMIT is replayed.

`lazy_registry_test.lua` uses real unrelated ALTER publication both before
and during the original writer's completion. `lazy_metadata_mvcc_test.lua`
checks the point, hole, pending-phantom and earlier-user-read premises in
both isolation levels and both engines; its READ COMMITTED primitive
results do not broaden production eligibility. `lazy_plain_binding_test.lua`
checks public CommitLocal packet eligibility, relevant metadata conflicts,
retained earlier reads, source fencing and an owned read-only fault. Direct
fixture metadata mutations isolate MVCC tracking and do not represent a
complete lazy DDL protocol. The original RF3 and live-fault suites remain
required. SQL fresh-table diagnostics retain the failed phase, commit ACK,
cleanup and independent row observation; an unknown outcome stays unknown.
These checks establish no benchmark improvement or general DDL liveness.

### Engine BUILD re-entry runtime boundary (reopened issue #165)

A status notification can revoke the engine cut after BUILD's initial
readiness check, including after a confirmed `copying` receipt and before
its first shadow page. The engine meter has no background rebuilder. Its
UNKNOWN state therefore remains a safe refusal until another engine entry
establishes a cut. BUILD now checks readiness before start and at loop
re-entry, rereads the owned receipt and capture, and discards local work
when a successful reset or confirmed page read outlives that evidence.
It compares the sampled ticket epoch even if another operation has already
rebuilt readiness. Lost capture resets the incomplete generation; cut-only
rebuilding retains a valid capture. Certified phases are handled before
any reset, and the existing uncertified `preparing` recovery remains valid.

This is pre-effect re-entry inside the same original BUILD. Generic
transaction/cut/invalidation helpers, final epoch/capture/role/ownership
checks and exception classification are unchanged. Any exception from
start, reset, refresh, a submitted page transaction or commit still escapes.
There is no catch-and-retry of a closure or RPC, and no progress promise
through those errors. The original deadline and eight-reset limit remain
runtime checks. A same-observed-role notification is not assumed harmless:
coalesced observations cannot exclude an intervening physical role change.

`engine_cut_test.lua` is an RF1/fsync mechanism witness, separate from RF3
process loss. It holds a real registered initial `box.status` callback and
releases it after reset or after the original confirmed source-page read.
The second schedule identifies the original read frame, descriptor, tuple
page and PK cursor without replacing its result. Real gap writes change
the source while capture is invalid. The same one BUILD must recapture
before writing a fresh page, reconcile later acknowledged mutations and a
rollback extra, and publish the exact image through PREPARE/APPLY. Its
wrappers observe only the owned worker and restore original functions.
Baseline controls retain the exact 1213 refusal and empty copying receipt.
Existing detached-shadow/terminal, source-drain, engine/cutover and RF3
leader-loss checks remain required.

The retained CI counters and exact-source call graph single out a
same-observed-RO status callback as the historical post-track hard loss,
conditional on the normal one-init fixture and no out-of-band invalidation.
They do not identify its payload, producer or exact delivery boundary.
The observation-only RF3 run establishes repeated same-role deliveries;
the scheduled RF1 witness establishes the refusal mechanism. A later
successful run is separate evidence, not a retrospective fault diagnosis
or a benchmark improvement.

### Vinyl pressure phase scheduling and failed-witness readback (issue #174)

`tests/sql/vinylpressure.py --real-pressure` keeps its total 20,000-attempt
bound, eight writers by default, 8 KiB random payloads and the real native
I/O, regulator and compaction checks. It reserves phase-local slots in
proportion to the three-second warmup, requested limited-I/O window and
three-second release window. Each writer owns interleaved slots spaced
across the actual window. Missed slots expire; they are never transferred
or sent in a catch-up burst. Workers park between slots and while the
controller changes I/O. The limited window opens after the 0.5 MiB/s control
receipt; the release window opens after restoring uncapped I/O. Every
writer must dispatch work in each complete window. Timestamps observe
client call entry and completion, rather than promising physical network
arrival at a particular instant.

The fixture freezes its attempt, native-sample and in-flight ledgers after
bounded drain. Before checking the pressure witness, it reads back at most
20,001 ordered IDs and saves exact missing acknowledged IDs, present refused
IDs, unexpected IDs and any unresolved IDs. An uncertain result is never
replayed or classified as settled merely because readback sees its key.
Incomplete drain or readback is explicit; a live writer at the drain cut
preserves the owned database for investigation. Stage, restore, drain,
readback and assertion failures remain distinguishable. CI retains the
manifest, events, summary and raw JSONL ledgers on success and failure.

```sh
python3 tests/sql/vinylpressure_phase_test.py
```

These offline controls use virtual monotonic time and the fixture's actual
scheduler, controller, audit and finalizer. They offer more than the entire
budget during warmup, check later phase activity and the total bound, and
exercise slow calls, expired permits, I/O-control errors and failed-witness
readback. A source-derived adapter for the former global-cap policy fails
the early-exit oracle; it does not execute the entire historical fixture.
The retained original CI failure identifies cap exhaustion, but did not
retain per-attempt phase records or perform exact readback. Its phase of
exhaustion and exact effects remain unobserved.

This is a test-driver change. Production admission, commit, locking and
recovery code, TLA+ inputs and Verus bodies are unchanged. These controls
prove neither native-engine latency nor a comparative performance result;
the complete release SQL and live-fault matrix remains required.

## Typed range reads (issue #135)

`TypedRead.tla` checks a local result refinement with two one-row pages,
finite values, NULL/absence, and buffered replacements/deletes. Successful
projection and COUNT/SUM/MIN/MAX folds must equal filtering the captured
snapshot with the own-write overlay; their coverage includes rejected rows.
An incomplete fold cannot publish success. The positive configuration visits
99,328 distinct states. Five isolated negative controls violate ExactFold
(stored-version overlay, early termination on an empty page), FullCoverage
(recording only qualifying rows), CompleteOnly (publishing an incomplete
fold), and ExactProjection (mixing snapshots).

This is a bounded refinement under the existing MVCC snapshot, full-range
lock/DDL checks, routing and commit-validation premises. It does not prove
Lua/Rust parsing, integer arithmetic, decimal precision, collations, native
storage, or liveness. `Local_ROValidate`, `Frac_ROValidate`,
`LocalFailover_ROValidate`, `GroupedRead`, `GroupedDDL` and their relevant
negative controls retain the read-contract checks. Protocol actions and
verified executable bodies are unchanged.

`lua/test/pushdown_test.lua` exercises the actual memtx/Vinyl procedures:
signed/unsigned 64-bit and small-number promotion boundaries, NULLs, fills,
excluded extrema, empty qualifying pages, stream invalidation, prepared
locks on rejected rows, and a captured Vinyl reduction crossed by a
prepared/applied write. The public SQL test `tests/sql/pushdowntest.py`
compares exact MariaDB results, including aggregates above 2^53/64-bit
sums, parameter changes, unsupported shapes, overlays at both isolation
levels, absent procedures, incomplete/malformed partials, and OCC conflicts
after a rejected row changes. Small multi-shard READ COMMITTED statements
retain ordinary reads without new validation; their reduction counter must
stay unchanged. The ordinary differential suite adds 30
cases. Bucket tests reject both new procedures on the wrong owner.

## Continuous integration

`.github/workflows/ci.yml` runs on every push and pull request:

* **lint**: `cargo fmt --check` and `cargo clippy -D warnings`;
* **test**: unit and integration tests, storage protocol tests (with the
  native procedures and without), the SQL suites against MariaDB 10.11 (on
  a cluster with two SQL servers, whose shards check the native module's
  copy of the lock index after each change, `TARN_CHECK_LOCKS`), the
  schema-change, bucket-move, fault-injection and failover tests;
* **tla**: `tla/check.sh` (every `Bug_*` and `Tradeoff_*` configuration must
  fail, the fast correct configurations must pass);
* **verus**: the Verus proofs.


Query routing diagnostics (`observe.rs`, `exec/diagnostics.rs`, issue #133)
are outside the TLA+ safety state: they neither change protocol messages nor
read/write user tuples. Coordinator hooks observe existing events only,
without an observer borrow across await. EXPLAIN shares the executor's
colocation guards and routing coercions; it supplies no values to execution.
Unit tests bound cardinality and cancellation/concurrent scopes. The public
MySQL suite `tests/sql/explaintest.py` checks prepared and text plans,
unknown parameters, no IDs/implicit commits/read effects, literal redaction,
eviction, and an exact-result join whose aligned shard keys reduce observed
fanout. Existing GroupedTables/GroupedRead positive and sensitivity models,
storage protocols, SQL, transaction and live-fault suites remain required;
these models do not prove observer accuracy or performance.

### IPROTO admission and retirement (issue #127)

`PipelineAdmission.tla` models three connections, two shard scopes and a
shared gateway scope, with independent count/byte limits and completion
reserves. Caller cancellation retains identity/charges. A terminal response
or both physical I/O owners dropping releases them; a logical close alone
does not. Timer ticks include abandoned requests. Before-enqueue refusal
certifies no RPC effect; closing an enqueued request reports uncertainty.
It checks resource/uncertainty contracts outside TarnCommit's state, rather
than changing commit/locking/recovery actions. Existing Handback and commit
models remain required for no replay and transaction safety.

The positive model checks 730,752 distinct states (depth 26). Removing any
of cancellation retention, physical retirement retention, shard/global
admission, ordinary completion reserves, abandoned deadlines or the
before-enqueue boundary gives the corresponding `Bug_Pipeline*` witness.
Config generation and the default check job include all eight profiles.
This bounded safety model assumes its timer action is serviced. It does not
prove wall-clock fairness, allocator bounds, query memory, wire correlation
(which has separate frame tests), arbitrary topology sizes or performance.

Executable tests cover 4097 refused admissions behind retained cancelled
callers, byte saturation before encoding, slow-peer blocked writes,
continued other replies behind an abandoned oldest request, out-of-order
queue compaction, shared scope refund, reserved control CALLs and physical
I/O drop ordering. `tests/rpcs.sh` checks real MySQL outcomes/effects under
24-session pressure at each scope; `rpc_reserve` checks cold completion
routing while ordinary capacity remains occupied. Native/Lua/vinyl modes,
malformed/lost-reply faults and failover preserve the existing uncertainty
and matched fencing behavior. Resource budgets do not certify whole-query
memory bounds or benchmark speedups.

### Physical file backup agent and seed identity (issue #56)

`FileBackup.tla` checks the agent's publication and seed-mode contract with
two abstract files: copier death/lease expiry, file reclamation, corruption,
checked restore, registration, distinct seed identity and read-only follow
before operator-authorized promotion. It checks complete-file publication,
a valid publication lease, verified restored contents and identity separation.
The four `Bug_FileBackup*` configurations deliberately publish partial data,
skip hash checks, reuse the source identity or ignore an expired lease;
each must violate its corresponding invariant.

Tarantool's coherent backup file list/WAL recovery, actual filesystem
`fsync`/rename behavior, SHA-256, fixed-width header format, administrative
registration and the operator's catch-up/promotion decision are trusted
premises. This bounded model does not prove file IO, a whole-cluster backup,
Tarantool replication, performance, or Python/Lua refinement. The existing
commit/locking/recovery models and 140 verified shared bodies are unchanged.
Live tests recover real vinyl files and compare every tuple byte, including
post-backup WAL and negative lease/copy/restore controls. The startup voter
marker prevents accidental candidate startup until an operator clears it;
it does not make the operator's catch-up decision automatically.
