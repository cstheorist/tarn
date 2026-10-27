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

### Configuration counts

The default list contains 143 configurations, including 100 expected
broken/trade-off outcomes. The 154 generated configurations reproduce from
`gen_cfgs.sh`; two additional grouped-read configurations are maintained
directly, for 156 total. Explicit check arguments override the default
list.

## Verus proofs

`crates/tarn-verified` is ordinary Rust inside `verus!` blocks: `cargo`
builds it with the specifications and proofs erased, and the server uses the
result. Verus proves:

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
# verification results:: 66 verified, 0 errors
```

`crates/tarn-verified/tests/erased.rs` exercises the erased code with
concrete inputs (codec boundaries, CRC check value, decision cases).

## Test suites

| Suite | Command | Needs | Checks |
|---|---|---|---|
| Unit tests | `cargo test --workspace` | Tarantool (IPROTO tests start their own) | codec, packets, SQL normalization, expressions, collation weights, coordinator decisions, the cross-shard deadlock search, IPROTO client against a real Tarantool |
| Storage protocol | `lua/test/run.sh` | Tarantool | every participant procedure on two real shards, recovery, record lifecycle and garbage collection, refusal on demoted nodes, lock-index rebuild while a PREPARE waits for a quorum, row locks, schema-change steps and definition fences; bucket moves step by step, and the refusals of requests for buckets a shard does not own (`bucket_test.lua`); the native read procedure against `tarn_select`, read by read, autocommit and in transactions, to end keys of plain, nullable and collated indexes, and the native autocommit writes, one-shard commits and validations against `tarn_autocommit`, `tarn_commit_local` and `tarn_validate` on twin tables and streams: the same replies, and rows of the same bytes, for what they serve, every other kind handed back having changed nothing, and the commits in flight waited for (`native_test.lua`). `TARN_NATIVE=0` runs them without the native procedures |
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
work. It requires a node-triggered collection, successful calls and a live
lazy worker without a manual recovery collection. An isolated control
that cancels only the heap-watchdog fiber fails the collection assertion
under the same bounded pressure. Earlier unbounded pressure reproduced a
native `cfg_get('read_only')` panic in both native and Lua modes; active
and stopped lazy-worker controls did not establish worker causation.
Tarantool's pinned [`cfg_get` implementation](https://github.com/tarantool/tarantool/blob/8b9a0f59a9/src/cfg.c#L40)
evaluates a Lua chunk and panics on a Lua error. The fixture verifies Tarn's
near-limit garbage recovery with native-core headroom; it is not a proof
that arbitrary privileged EVAL allocations can safely exhaust the hard
Lua heap limit.

## Continuous integration

`.github/workflows/ci.yml` runs on every push and pull request:

* **lint**: `cargo fmt --check` and `cargo clippy -D warnings`;
* **test**: unit and integration tests, storage protocol tests (with the
  native procedures and without), the SQL suites against MariaDB 10.11 (on
  a cluster with two SQL servers), the schema-change, bucket-move,
  fault-injection and failover tests;
* **tla**: `tla/check.sh` (every `Bug_*` and `Tradeoff_*` configuration must
  fail, the fast correct configurations must pass);
* **verus**: the Verus proofs.
