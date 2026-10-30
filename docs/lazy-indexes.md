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
tables. `bench/lazy_footprint.lua` measures each structure on one storage
node owning 1,024 buckets, with a table `t(id, k, bucket_id)` and a lazy
index on `k` (1,000 distinct values). It loads the rows, deletes a tenth of
them (at most 20,000) and lets garbage collection collect their tombstones,
holds the assignments of 1,000 key changes in the outbox, and prepares then
commits 100 cross-shard transactions. `python3 bench/lazy_footprint.py run
--before OLD/lua` repeats it for an older release's Lua module, and
`python3 bench/lazy_footprint.py table` prints these tables from
`bench/results_lazy_footprint.json`.

The archived measurements below contain one observation per version, engine
and row count. They lack immutable source/build receipts and repeated matched
pairs. Their before/after differences are unqualified development evidence,
and do not establish capacity, repeatable RSS savings or completion of #177's
measurement criteria. The original reported values are retained here; a
source-bound repeated study remains required.

Archived per-row version ledger (`_tarn_lazy_version`, memtx), after loading:

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

Per shard, then:
- **Source:** about 135 bytes per source bucket for counters; the
  outbox, which `lazy_outbox_entries` and `lazy_outbox_bytes` cap
  (65,536 assignments and 64 MiB encoded by default); and the reservations
  of the transactions prepared on the shard.
- **Target:** one marker per live row and index, and one tombstone per
  removed membership until collection, in the base table's engine: memtx
  holds them in memory, vinyl on disk with only its cache in memory.
- **Floors:** one per incarnation, target bucket and source bucket that has
  collected a tombstone, in memtx. None is deleted, even after its
  incarnation is retired; only a bucket leaving the shard takes its floors
  along. With deletes spread over every bucket they approach incarnations
  × target buckets × source buckets: bounded by the bucket count, not by
  the rows, but large at high bucket counts. Bounding them is the next
  part of #177.

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
