# Lazy secondary indexes

This is the implementation contract for issue [#82](https://github.com/cstheorist/tarn/issues/82).

A lazy index is an additional, asynchronously maintained global copy of a
non-unique secondary index. The ordinary local Tarantool index remains in
place. Transactions, locking reads, writes and unsupported query shapes
continue to use the strong paths described in [design.md](design.md).

```sql
CREATE TABLE t (
  tenant_id BIGINT NOT NULL,
  id BIGINT NOT NULL,
  status VARCHAR(16),
  created_at DATETIME NOT NULL,
  PRIMARY KEY (tenant_id, id),
  KEY by_status (status, created_at) COMMENT 'tarn:lazy'
) PARTITION BY KEY (tenant_id);
```

By default, the first normalized index column determines the index bucket.
An equality on `status` reads one index shard, then fetches base rows from
their owning shards. A later ordering column alone does not spread a busy
status across index shards.

Opt into composite routing with `COMMENT 'tarn:lazy=N'`, where `N` is the
number of leading index columns to hash together (1–16, at most the index
width). For example, `KEY by_status(status, created_at) COMMENT 'tarn:lazy=2'`
spreads one status across the distinct timestamps. Or choose an explicit
application lane: `KEY by_status(status, lane, created_at) COMMENT 'tarn:lazy=2'`.
The application assigns and persists `lane`; Tarn does not invent a salt or
change lane values automatically. Adding lanes distributes independent
rows; it cannot parallelize concurrent updates to one row.

A query binding every routing column reads one index destination. A shorter
nonempty equality prefix reads every live index shard, up to 16, and merges
pages by normalized typed index columns and typed primary keys. Each shard
retains one page (128 inspected entries, at most 1 MiB); the gateway caps
buffered page bytes at 16 MiB, total candidates at 65,536 and page calls at
1,024. Empty pages containing only obsolete ownership copies still advance
the cursor. Larger reads use the complete strong fallback or fail explicitly
under the statement resource budget. ORDER BY that does not follow the
index requires collecting and sorting the complete qualifying bounded result.
EXPLAIN FORMAT=JSON shows the routing columns, incarnation, destination or
fanout, merge order and separate base-row rechecks.

Routing metadata is immutable within an incarnation. To migrate, add a new
index with the desired annotation, wait for ALTER to return successfully,
then drop the old index. Bootstrap captures the current committed rows and
publishes only after delivery completes. This uses the existing bounded,
write-fenced bootstrap (including its row/time limits); it is not an
unbounded online build. Comment edits do not change existing routing.
Upgrade every gateway and replica before creating composite metadata.
DDL checks every configured replica's routing capability; movement checks
the destination. Schema exports with active **or retired** composite
incarnations use lazy envelope version 2 and require a version-2 coordinator
token. Older importers reject the envelope before native schema effects.
Legacy single-column declarations and version-1 exports remain supported.

The [paired routing cost study](lazy-routing-cost.md) describes the workload,
resource configuration, accounting and uncertainty for hotspot measurements.
Adding lanes can also add shipping work and prefix-read fanout; choose the
routing columns for the queries that actually bind them.

## Read contract

Only an actual autocommit, nonlocking, single-table SELECT can use the lazy
copy. Explicit transactions, locking reads and DML use the local index or
another existing strong plan. Joins, subqueries, aggregates and other
unsupported shapes fall back to the existing executor.

The first implementation fetches base rows for every candidate. It checks
both the candidate's complete canonical index key against the current base
row and the query's complete WHERE expression. Deleted rows and stale
memberships are discarded. Primary keys are deduplicated. LIMIT and OFFSET
apply after those checks, so the executor continues to later index pages
when earlier candidates fail them. A resource limit must cause a safe
fallback or an explicit error; it must not turn a partial scan into a
successful complete result.

A lazy query can miss a recently matching row. Each returned row must
match at its base-row read; the query does not establish a distributed
serializable snapshot. An index-only optimization is not part of the
initial implementation. The local index continues to provide a strong
fallback. Existing indexes that merely contain a comment are not activated
until their durable lazy configuration has completed bootstrap.

Routing, equality and identity must agree for supported SQL types. Nullable
non-unique keys remain eligible for storage. Case-insensitive keys use
their normalized weights for routing and comparison while retaining raw
primary-key values for base lookup. Encoded identity bytes are not SQL
ordering keys: pagination uses typed index and primary-key values.
Unsupported routing types must be rejected when declaring the lazy index,
rather than allowed to route SQL-equal values differently.
The initial implementation accepts integer, text, binary and normalized
temporal values in index and primary-key identities. FLOAT, DOUBLE, DECIMAL
and JSON columns in either identity are rejected.

## Commit and delivery

Each index has an immutable, globally allocated incarnation identifier.
Dropping and recreating an index creates a new incarnation. An ordinary
table definition version or a reusable native index number is not that
identifier.

The source assigns exact, monotonically increasing sequence numbers per
base bucket. Counters survive deletes, empty buckets, restarts and bucket
moves. A logical commit writes the base changes, the bucket counter and all
old/new membership assignments to a durable outbox in the same local
transaction. No per-row state is kept on the source (issue
[#177](https://github.com/cstheorist/tarn/issues/177)): see "Bootstrap". Initial images are captured before physical rewrite loops;
temporary deletes used for unique-value cycles must not emit independent
logical changes. Physical backfill and bucket import must not emit changes.

Autocommit, local commit, READ COMMITTED commit and cross-shard APPLY all
perform this capture. PREPARE does not publish an outbox entry. Instead,
before voting YES it durably reserves the entries, encoded bytes and
sequence capacity its eventual APPLY needs. ABORT releases the reservation;
APPLY atomically converts it into committed assignments. A decided
transaction must not discover an avoidable capacity or sequence-exhaustion
failure for the first time during APPLY.

These limits count encoded assignments and promises; they do not reserve
the native allocator's RAM. Admission uses a conservative in-memory meter
whose durable truth is the individual outbox and reservation records.
Positive charges precede mutation, rollback refunds them, and negative
credits become reusable only after confirmed commit. This avoids a shared
usage tuple that would create MVCC conflicts between independent writers.
After a leadership transition, admission waits for old terminal callbacks
and the native queue cut before a confirmed scan rebuilds the meter.

A leader fiber reads only confirmed outbox entries and sends bounded
batches to the current destination owners. A source retains each exact
assignment until a durable acknowledgement. An acknowledgement means that
the target durably applied it, already holds a superseding version, or has
an authoritative durable retirement for that incarnation. An unavailable,
missing or not-yet-ready target does not acknowledge it. A leader resumes
delivery from durable state after restart or failover.
Duplicate deliveries read confirmed target state: a marker, floor or
retirement still awaiting WAL or quorum confirmation cannot justify an
acknowledgement. A conflict leaves the source assignment available for retry.

The target keeps the newest membership marker for each tuple of index
incarnation, canonical index key and canonical base primary key. Old-key
deletes and new-key puts can go to different target buckets, so recording
only the latest primary-key version on the new target is insufficient.
Versioned markers make duplicate and reordered delivery harmless. Visible
entries retain raw primary-key values for base reads; tombstones are not
query candidates.

Normal base writes do not synchronously call the destination index shard.
They still pay local index maintenance, outbox work and capacity admission.
An unavailable destination can fill the bounded outbox and apply write
backpressure; asynchronous delivery cannot promise unlimited write
availability while retaining every update.

## Frontiers and tombstone collection

For one source bucket, the completed frontier is its counter when there
are no pending assignments, otherwise one less than the smallest pending
sequence. The counter and pending minimum are read from one confirmed
view. The maximum acknowledged sequence is not a frontier: an earlier
assignment may still be missing, including one for a different destination.

Tombstone collection uses a durable floor keyed by index incarnation,
target bucket and source bucket. Its value is a source bucket's completed
prefix across all destinations. The target persists the floor before
deleting matching tombstones at or below it. Later frames at or below the
floor can be acknowledged as obsolete; live entries remain. Floors move
with target buckets, including otherwise empty buckets. Trusted import of
live entries must not discard them merely because their versions are below
an imported floor. Collection is disabled until bootstrap is fully
acknowledged and the incarnation is ready.

A permanent old delivery hole can hold back this global source prefix
while later changes at other destinations are acknowledged. Their
tombstones remain until the hole is resolved. Source outbox limits alone
therefore do not give a finite target tombstone-retention bound. Target
capacity failures retain source assignments and eventually apply source
backpressure; they must not be acknowledged as successful delivery.

## Bootstrap, DDL and native writes

The initial bootstrap uses a bounded write-fenced ALTER operation. It can
pause writes while it scans existing rows and waits for their baseline
assignments to be durably acknowledged. The existing per-shard ALTER scan
limit applies; this is not an unbounded online backfill. An instant-added
column is first materialized using its committed fill/layout rules.

Every baseline row and its bucket's counter come from one consistent view,
and the baseline assignment takes that counter as its sequence. Any value
up to the counter would do: every later change of the row takes a greater
sequence and supersedes the baseline at the target, and the new
incarnation's floors begin only after READY, once every baseline
assignment is acknowledged (`tla/LazyBaseline.tla`; a baseline above the
counter fails it). The counter holds the bucket's completed prefix back the
least while the baseline is pending. Releases before #177 gave each row the
sequence of its last change from a per-row ledger, `_tarn_lazy_version`, and
0 to rows without an entry; those values are within the same bound. The
generation/phase check rejects late bootstrap enqueue after readiness, so a
baseline assignment cannot appear behind a previously reported frontier.
All baseline acknowledgements precede READY, catalog publication and
unfencing. Capability checks prevent older storage nodes from silently
ignoring lazy metadata.

Every write and PREPARE path requires a matching, current local capture
configuration. Unknown or stale configuration fails closed. In particular,
a shard still preparing an ALTER must reject a forward-version write from
a gateway that already observed publication on another shard. Merely
rejecting older definition versions does not provide that guard. Restart
and leader changes rebuild this state before admitting writes.

A native procedure that cannot capture the outbox must hand such writes
back to Lua before writing anything. The exclusion is enforced on the
storage node and follows local capture activation and invalidation. A
gateway-side eligibility check alone cannot protect against a stale
gateway or an older client.

DROP durably retires its lazy incarnations before hidden-space cleanup.
TRUNCATE rotates them to freshly allocated incarnations and publishes the
empty ready indexes with the cleared base table. It keeps the source
bucket counters, and clears what older releases left in the table's
per-row version ledger. It does not backfill the rows being truncated. Late assignments for the old
incarnations cannot populate the new ones.

## Bucket moves and shard removal

Lazy target writes participate in the existing commit-in-flight and bucket
ownership checks. They note hidden data, marker and floor mutations for
the final copy delta. Ownership is checked after any yielding reads or
writes and immediately before commit. A move must not acknowledge a batch
that is then stranded on its old owner.

A source bucket is fenced and drained, and every committed assignment is
durably acknowledged before the source flips ownership. Its counters are
copied even when it has no user rows, and so is what an older release left
in the per-row version ledger, until the storage node deletes it: a
background pass deletes those entries, at most 128 in a transaction, in
buckets the node may write, noted for the move like any write. Pending
assignments are not migrated in the initial design. Destination outage can
therefore delay a move. Shard removal must also wait for pending,
reserved and in-flight lazy obligations before decommissioning a source.

Deploy lazy-aware gateways and storage modules on every replica before
creating a lazy index. Schema export carries a versioned lazy envelope,
including retired incarnations, and JOIN requires explicit support from
the gateway and every candidate replica. Bucket transfer identifies the
internal lazy counters, versions and floors by stable names, since their
local native space numbers can differ between nodes. Capability checks
refuse incompatible receivers before a transfer begins.

## Reading a session's own writes

The session controls are:

```sql
SET SESSION tarn_lazy_read_your_writes = ON;
SET SESSION tarn_lazy_wait_timeout = 5;
```

They are session-only. The timeout uses integer seconds, zero through sixty,
with five seconds the default. Zero requests a nonblocking check and an
error if catch-up cannot complete immediately.

A session retains cumulative own committed-but-not-yet-applied participant
obligations and touched stable bucket identifiers, even while the flag is
OFF. Enabling it later must cover those earlier writes. A successful SQL
COMMIT, including the normal acknowledgement mode, is not by itself proof
that all participants have physically applied their rows and outboxes.

Before opening an index snapshot, the session resolves all its pending
APPLY obligations using idempotent transaction recovery, captures the
source counters in confirmed views and waits for the corresponding
completed frontiers. This can conservatively wait for unrelated writes in
the same bucket. It does not replay a SQL mutation. A later successful
commit on another bucket must not replace an unresolved earlier obligation.
An idempotent participant APPLY reply must certify confirmed completion:
another APPLY's unconfirmed `applied` record cannot discharge the session's
obligation. Otherwise a confirmed counter read could still see the old
counter and admit an index read before the session's row exists.

History is bounded. Overflow or an expired/ambiguous receipt requires
backpressure, a sticky strong fallback or an error; it cannot silently
forget an unresolved obligation and return a stale lazy result. An attempted
catch-up that cannot finish within its budget returns error 1205 and keeps
the outstanding history for a later read. It does not begin a second,
potentially longer lock wait as a fallback. Queries already using the
strong path retain their ordinary lock-wait behavior. The barrier
establishes causal index catch-up before the read, not a globally
serializable query snapshot.

## Memory and disk

New captured writes keep no per-row version state: the source keeps a bucket
counter per source bucket, the pending outbox, and prepared reservations.
Legacy version entries remain until bounded background cleanup removes them.
Before issue
[#177](https://github.com/cstheorist/tarn/issues/177) it also kept a
per-row version ledger, `_tarn_lazy_version`, in memtx: an entry for every
live row written since the table's first lazy index, also for vinyl
tables.

`bench/lazy_footprint.lua` measures one storage node owning 1,024 buckets,
with a table `t(id, k, bucket_id)` and a lazy index on `k` (1,000 distinct
values). The budgets are the same at every row count: `memtx_memory` 3 GiB,
`vinyl_memory` 128 MiB and `vinyl_cache` 64 MiB. It loads the rows, deletes
a tenth of them (at most 20,000) and lets garbage collection collect their
tombstones. It holds the assignments of 1,000 key changes in the outbox,
and prepares then commits 100 transactions. It then times 5,000 single-row
key changes and 5,000 reads through the lazy index, makes a checkpoint, and
restarts the node from its files. `python3 bench/lazy_footprint.py study
--before REV --after REV` runs it for two commits in matched pairs,
alternating which goes first. It records receipts: the commits, a SHA-256 of
every module file measured, of the bench files and of the Tarantool binary,
and the host. `python3 bench/lazy_footprint.py table` prints the tables
below from `bench/results_lazy_study.json`. The study below compares two
merged commits:
- before: `0b8afce`, #196's head, whose `lua/` is main's once #196 merged;
- after: `08af776`, #206's head, whose `lua/` is main's once #206 merged.

Their `lua/` trees differ only by #206. Later changes to the module, such
as composite routing (#185) and bounded moves (#204), are not measured
here. `study` repeats the comparison for any two commits.

Receipts: before `0b8afce0675d`, after `08af77636f85` (module files SHA-256 `afae6519f162` and `feeb8a0f3e25`); Tarantool 3.9.0-0-g8b9a0f5 (binary SHA-256 `f76fca762b6f`); Intel(R) Xeon(R) Processor @ 2.10GHz, 2 logical CPUs, 7.8 GiB, Linux 6.18.44-fc-v185; 3 matched pairs per row count and engine, 2026-10-30T17:17:37+00:00 to 2026-10-30T19:44:42+00:00. Medians, with the range over the repetitions.

The per-row version ledger, after loading: before minus after in matched pairs.

| Rows | Table | Ledger entries before → after | memtx tuple + index bytes saved | per row | Process RSS saved | WAL bytes per key change, before → after | Restart to serving, before → after | RSS after restart, before → after |
|---:|---|---|---:|---:|---:|---|---|---|
| 10,000 | memtx | 10,000 → 0 | 0.8 MiB (0.7 MiB to 0.9 MiB) | 84 B (75 B to 99 B) | 0.7 MiB (-0.2 MiB to 1.0 MiB) | 726 → 682 | 0.1 s → 0.1 s | 51.7 MiB (51.4 MiB to 52.2 MiB) → 50.8 MiB (50.1 MiB to 50.9 MiB) |
| 10,000 | vinyl | 10,000 → 0 | 0.9 MiB (0.8 MiB to 0.9 MiB) | 89 B (83 B to 93 B) | 1.3 MiB (0.7 MiB to 1.5 MiB) | 722 → 677 (673 to 677) | 0.1 s → 0.1 s | 47.0 MiB (46.4 MiB to 47.2 MiB) → 42.1 MiB (42.0 MiB to 43.3 MiB) |
| 100,000 | memtx | 100,000 → 0 | 7.2 MiB (7.2 MiB to 7.3 MiB) | 76 B | 7.2 MiB (6.7 MiB to 7.5 MiB) | 742 → 697 | 0.5 s (0.5 s to 0.6 s) → 0.5 s (0.4 s to 0.5 s) | 156.5 MiB (156.3 MiB to 157.2 MiB) → 128.4 MiB (128.4 MiB to 129.0 MiB) |
| 100,000 | vinyl | 100,000 → 0 | 7.1 MiB (7.1 MiB to 7.2 MiB) | 75 B | 7.4 MiB (7.1 MiB to 7.8 MiB) | 731 → 686 (685 to 686) | 0.2 s (0.2 s to 0.3 s) → 0.1 s | 94.4 MiB (94.3 MiB to 94.7 MiB) → 44.5 MiB (44.5 MiB to 45.1 MiB) |
| 1,000,000 | memtx | 1,000,000 → 0 | 78.6 MiB (78.6 MiB to 78.8 MiB) | 82 B (82 B to 83 B) | 79.7 MiB (79.3 MiB to 79.9 MiB) | 792 → 743 | 6.6 s (6.4 s to 7.4 s) → 4.7 s (4.4 s to 4.9 s) | 1310.3 MiB (1309.6 MiB to 1310.7 MiB) → 953.7 MiB (941.7 MiB to 962.8 MiB) |
| 1,000,000 | vinyl | 1,000,000 → 0 | 78.7 MiB (78.6 MiB to 78.7 MiB) | 83 B (82 B to 83 B) | 72.1 MiB (68.4 MiB to 74.5 MiB) | 684 (680 to 685) → 627 (624 to 628) | 1.6 s (1.6 s to 1.9 s) → 0.1 s | 595.5 MiB (595.3 MiB to 612.6 MiB) → 53.7 MiB (53.1 MiB to 53.8 MiB) |

Per replica, after loading (this design). memtx: tuple payload, the tuple allocator's overhead (headers and size classes), indexes. On disk after a checkpoint: snapshot, WAL kept for the last two checkpoints, vinyl files. After a restart, memtx MVCC holds stories for tuples recovered from the snapshot.

| Rows | Table | Process RSS | memtx tuples / allocator / indexes | vinyl page index + bloom | Snapshot / WAL / vinyl files | Restart to serving | RSS after restart | MVCC stories after restart |
|---:|---|---:|---|---:|---|---:|---:|---|
| 10,000 | memtx | 49.1 MiB (48.6 MiB to 49.2 MiB) | 0.5 MiB / 0.7 MiB (0.7 MiB to 0.8 MiB) / 3.7 MiB | 0.0 MiB | 0.3 MiB / 4.3 MiB / 0.0 MiB | 0.1 s | 50.8 MiB (50.1 MiB to 50.9 MiB) | 7,920, 2.9 MiB |
| 10,000 | vinyl | 51.9 MiB (51.8 MiB to 52.0 MiB) | 0.0 MiB / 0.1 MiB (0.1 MiB to 0.2 MiB) / 1.9 MiB | 0.1 MiB | 0.1 MiB / 3.2 MiB / 3.6 MiB (2.9 MiB to 3.9 MiB) | 0.1 s | 42.1 MiB (42.0 MiB to 43.3 MiB) | 0, 0.0 MiB |
| 100,000 | memtx | 78.6 MiB | 5.2 MiB / 5.7 MiB (5.7 MiB to 5.8 MiB) / 21.3 MiB | 0.0 MiB | 2.2 MiB / 10.6 MiB / 0.0 MiB | 0.5 s (0.4 s to 0.5 s) | 128.4 MiB (128.4 MiB to 129.0 MiB) | 88,920, 34.4 MiB |
| 100,000 | vinyl | 89.3 MiB (88.6 MiB to 89.6 MiB) | 0.0 MiB / 0.2 MiB / 5.4 MiB | 0.7 MiB | 0.2 MiB / 3.3 MiB / 35.4 MiB (35.3 MiB to 35.4 MiB) | 0.1 s | 44.5 MiB (44.5 MiB to 45.1 MiB) | 0, 0.0 MiB |
| 1,000,000 | memtx | 329.5 MiB (329.4 MiB to 329.8 MiB) | 58.7 MiB / 54.9 MiB / 164.2 MiB | 0.0 MiB | 21.2 MiB / 65.0 MiB / 0.0 MiB | 4.7 s (4.4 s to 4.9 s) | 953.7 MiB (941.7 MiB to 962.8 MiB) | 978,920, 380.7 MiB |
| 1,000,000 | vinyl | 251.7 MiB (250.4 MiB to 255.3 MiB) | 0.0 MiB / 0.1 MiB (0.1 MiB to 0.2 MiB) / 6.7 MiB | 6.7 MiB | 0.1 MiB / 3.0 MiB / 105.5 MiB (104.2 MiB to 112.3 MiB) | 0.1 s | 53.7 MiB (53.1 MiB to 53.8 MiB) | 0, 0.0 MiB |

Per item, 1,000,000 rows (tuple payload and index, without the allocator's per-tuple overhead):

| Structure | Kept | memtx table | vinyl table |
|---|---|---:|---:|
| Base row, for comparison | the row of `t` and its three local indexes | 80 B | 78 B on disk, 1.4 B page index + bloom |
| Target marker | one per live row and lazy index, in the table's engine | 147 B | 216 B on disk, 5.7 B page index + bloom |
| Tombstone | one per removed membership, until its floor collects it | 147 B | on disk, as a marker |
| Floor (`_tarn_lazy_floor`) | one per incarnation, target bucket and source bucket that collected a tombstone | 91 B (2,560 after 20,000 deletes) | 91 B (2,560 after 20,000 deletes) |
| Counter (`_tarn_lazy_counter`) | one per source bucket with a captured write | 135 B | 135 B |
| Outbox assignment | one per pending membership change, capped by `lazy_outbox_entries` and `lazy_outbox_bytes` | 77 B, 45 B encoded | 77 B, 45 B encoded |
| Reservation | one per row a prepared transaction writes | 70 B | 70 B |
| Protocol records (`_tarn_part`, `_tarn_txn`) | per cross-shard transaction, until expiry | 73 B | 73 B |

Latency in microseconds (5,000 single-row key changes, one per autocommit, delivery running; 5,000 reads of one key's first page of at most 10 rows through the lazy index):

| Rows | Table | Write p50 / p99 / p99.9 | WAL bytes per key change | Read p50 / p99 / p99.9 | Read after restart p50 / p99 / p99.9 |
|---:|---|---:|---:|---:|---:|
| 10,000 | memtx | 480 (452 to 485) / 1734 (1684 to 1972) / 3435 (2945 to 4453) | 682 | 118 (116 to 139) / 703 (647 to 729) / 1209 (1022 to 3696) | 94 (84 to 99) / 587 (516 to 631) / 3009 (2601 to 3013) |
| 10,000 | vinyl | 469 (439 to 607) / 1753 (1748 to 2051) / 3051 (2627 to 4365) | 677 (673 to 677) | 150 (142 to 176) / 1325 (1110 to 1363) / 7495 (7214 to 9732) | 203 (155 to 209) / 2321 (2006 to 2461) / 3899 (3194 to 5077) |
| 100,000 | memtx | 598 (569 to 678) / 2125 (2117 to 2324) / 4622 (4531 to 5051) | 697 | 252 (231 to 253) / 1048 (960 to 1092) / 2077 (1799 to 2376) | 130 (116 to 132) / 692 (567 to 699) / 3089 (1686 to 3111) |
| 100,000 | vinyl | 607 (505 to 609) / 2213 (1724 to 2372) / 3572 (2776 to 4053) | 686 (685 to 686) | 260 (225 to 272) / 4497 (4443 to 4567) / 6677 (6477 to 6970) | 197 (158 to 207) / 5521 (3066 to 6411) / 8324 (7920 to 10013) |
| 1,000,000 | memtx | 636 (591 to 732) / 2343 (2187 to 2666) / 4602 (4448 to 5301) | 743 | 223 (179 to 248) / 915 (847 to 1028) / 2226 (1639 to 2370) | 148 (124 to 168) / 694 (632 to 840) / 3587 (3079 to 3761) |
| 1,000,000 | vinyl | 520 (510 to 608) / 2092 (2029 to 2339) / 3495 (3243 to 3677) | 627 (624 to 628) | 331 (330 to 374) / 10070 (7708 to 10321) / 12524 (10476 to 16218) | 213 (200 to 224) / 7944 (6963 to 8533) / 9929 (8183 to 11024) |

These are one node's measurements on one host, not capacity guarantees.
Every replica of a shard holds all of it: an RF3 shard holds three copies.

Per shard replica, then:
- **Rows:** what grows with the rows is the table's own rows and indexes,
  and one marker per live row and lazy index, in the table's engine.
  - memtx holds them in memory. Here that is the row (80 bytes) plus a
    marker (147 bytes), each with about 29 bytes of allocator overhead: at
    a million rows, 278 MiB of tuples, allocator and indexes.
  - vinyl holds them on disk: 78 and 216 bytes here. In memory it keeps
    about 7 bytes per row of page index and bloom filters, plus its
    in-memory level and its cache, which `vinyl_memory` and `vinyl_cache`
    bound.
- **Source:** about 135 bytes per source bucket for counters. The outbox
  (about 77 bytes an assignment) and the reservations of prepared
  transactions (about 70 bytes a row) are capped together by
  `lazy_outbox_entries` and `lazy_outbox_bytes` (65,536 assignments and
  64 MiB encoded by default). A destination outage fills the outbox up to
  the cap. Past it, writes that would add to it are refused (1041) and
  PREPARE votes NO.
- **Tombstones:** one per removed membership, until its source bucket's
  completed prefix passes it and a floor collects it. A permanent delivery
  hole keeps them (see "Frontiers and tombstone collection").
- **Floors:** one per incarnation, target bucket and source bucket that has
  collected a tombstone, about 91 bytes each, in memtx. None is deleted,
  even after its incarnation is retired; only a bucket leaving the shard
  takes its floors along. With deletes spread over every bucket they
  approach incarnations × target buckets × source buckets. That is bounded
  by the bucket count, not by the rows, but large at high bucket counts.
- **Protocol records:** about 73 bytes per cross-shard transaction, until
  they expire and garbage collection removes them.
- **The process:** RSS also holds Lua, network buffers and allocator
  slack. The vinyl budgets bound vinyl's memory, not the process's.

Writes: a key change writes WAL for:
- the base row, the counter, and the outbox entry and its removal;
- at the target, the old key's tombstone and the new key's marker;
- later, the floor and the tombstone's deletion.

The latency table gives the bytes. vinyl also rewrites its runs when it
dumps and compacts.

Restarts:
- A restart reads memtx tables back from the snapshot and the WAL, so its
  time grows with the memtx data. vinyl reads its index metadata, and its
  pages on demand.
- After a restart, memtx MVCC (`memtx_use_mvcc_engine`, which storage nodes
  enable) kept a transaction story for nearly every row of one large memtx
  table:
  - the memtx table at a million rows: 978,920 stories, 381 MiB;
  - with a vinyl table before #206, the ledger: 977,896 stories, 291 MiB.

  Transactions release stories a few at a time; in a separate check of
  Tarantool alone, about two per transaction. The process keeps the freed
  memory. So the ledger also cost a vinyl node most of its RSS after a
  restart.
- The admission meter's rebuild after a restart or failover scans the
  outbox and reservations, which the cap bounds.

<details>
<summary>Archived single observations, before this study</summary>

These earlier measurements contain one observation per version, engine and
row count, without source/build receipts or matched pairs; they are
unqualified development evidence, retained as reported.
`python3 bench/lazy_footprint.py table bench/results_lazy_footprint.json`
prints them.

Per-row version ledger (`_tarn_lazy_version`, memtx), after loading the rows:

| Rows | Table | Entries before | Ledger bytes before | Bytes per row | Entries after | memtx data + index, before → after | Process RSS, before → after |
|---:|---|---:|---:|---:|---:|---|---|
| 10,000 | memtx | 10,000 | 840,192 | 84 | 0 | 5.6 MiB → 4.8 MiB | 50 MiB → 48 MiB |
| 10,000 | vinyl | 10,000 | 804,576 | 80 | 0 | 2.9 MiB → 2.1 MiB | 53 MiB → 52 MiB |
| 100,000 | memtx | 100,000 | 7,529,192 | 75 | 0 | 38.6 MiB → 31.6 MiB | 86 MiB → 78 MiB |
| 100,000 | vinyl | 100,000 | 7,455,073 | 75 | 0 | 13.2 MiB → 6.3 MiB | 96 MiB → 89 MiB |
| 1,000,000 | memtx | 1,000,000 | 82,512,752 | 83 | 0 | 346.7 MiB → 272.7 MiB | 410 MiB → 329 MiB |
| 1,000,000 | vinyl | 1,000,000 | 82,418,317 | 82 | 0 | 87.5 MiB → 13.6 MiB | 324 MiB → 244 MiB |

What stays, per item, 1,000,000 rows (this branch; tuple payload and index, without the allocator's per-tuple overhead):

| Structure | Kept | memtx table | vinyl table |
|---|---|---:|---:|
| Target marker | one per live row and lazy index, in the table's engine | 147 B | 216 B on disk |
| Tombstone | one per removed membership, until its floor collects it | 147 B | on disk |
| Floor (`_tarn_lazy_floor`) | one per incarnation, target bucket and source bucket that collected a tombstone; never deleted | 91 B (2,560 after 20,000 deletes) | 91 B (2,560 after 20,000 deletes) |
| Counter (`_tarn_lazy_counter`) | one per source bucket with a captured write | 135 B (1,024) | 135 B (1,024) |
| Outbox assignment | one per pending membership change, capped by `lazy_outbox_entries` and `lazy_outbox_bytes` | 77 B, 45 B encoded | 77 B, 45 B encoded |
| Reservation | one per row a prepared transaction writes | 70 B | 70 B |
| Protocol records (`_tarn_part`, `_tarn_txn`) | per cross-shard transaction, until expiry | 73 B | 73 B |
| Process RSS after loading |  | 329 MiB | 244 MiB |

</details>

## Observability and verification

SHOW STATUS reports per-shard outbox depth and oldest-entry age from
asynchronously collected samples. Missing or stale samples are exposed as
such. A cached depth of zero is not a read-your-writes barrier.

The bounded TLA+ refinements cover delivery and replay, capture admission,
capacity promises, confirmed target and participant acknowledgements, tombstone floors,
handoff and cumulative session barriers, with broken-control configurations
that violate the corresponding invariants. Complete bootstrap enumeration,
consistent baseline row/version reads and late-enqueue rejection are
runtime refinement premises exercised by native tests. The models refine the existing commit
protocol under explicit storage atomicity and durable-confirmation
premises; they do not prove every SQL expression or the entire native
implementation. [verification.md](verification.md) records the checked
configurations, outcomes and scope.

### Churn and outage

`lua/test/lazy_soak.sh` (memtx and vinyl, part of `lua/test/run.sh`) runs
one writer against two shards, each the source of half the buckets and the
target of the other half, keeping protocol records for about 4 s
(`TARN_MAX_TXN_AGE=2`, `TARN_CLOCK_SKEW=1`). For about 45 s it inserts,
rekeys, rewrites and deletes 320 rows with single-shard writes and
cross-shard transactions, and checkpoints every few seconds. The
coordinator commits most transactions, aborts some, and abandons others
after PREPARE on both shards (recovery commits them) or on one (recovery
aborts them). The test stops one shard (SIGSTOP) while the other's outbox
is capped at 300 entries, then kills that shard and restarts it from its
checkpoint and WAL with transactions in flight. It checks that:

- committed and promised outbox entries stay within the cap while the
  destination is stopped; writes that would add entries beyond it are
  refused (1041) before they change rows, PREPARE votes NO (1041) instead
  of YES, and every YES commits;
- every transaction resolves, all of its writes or none, the abandoned
  ones by recovery;
- once the writes stop, both outboxes drain and the rows and the index
  hold exactly the expected keys;
- garbage collection then reclaims every tombstone and protocol record,
  and floors stay within one per target and source bucket.

`SOAK_SCALE=n` makes each phase n times longer. The test assumes what
delivery and collection need: a stopped or killed shard returns and its
peers reach it again. Until then the source's backlog stays within the cap
because writes are refused, and the tombstones and records that depend on
the missing shard stay.
