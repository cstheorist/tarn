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

The first normalized index column determines the index bucket. An equality
on `status` can therefore read one index shard, then fetch base rows from
their owning shards. It can still require several base-shard calls. A
range or ordering column after `status` does not spread one busy status
across index shards. Prepending a tenant or explicit bucket column can
spread that load, provided queries bind that routing column.

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
moves. A logical commit writes the base changes, row-version state and all
old/new membership assignments to a durable outbox in the same local
transaction. Initial images are captured before physical rewrite loops;
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

Every baseline row and its version come from one consistent view. The
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
bucket counters and clears the table's live-row version ledger. It does
not backfill the rows being truncated. Late assignments for the old
incarnations cannot populate the new ones.

## Bucket moves and shard removal

Lazy target writes participate in the existing commit-in-flight and bucket
ownership checks. They note hidden data, marker and floor mutations for
the final copy delta. Ownership is checked after any yielding reads or
writes and immediately before commit. A move must not acknowledge a batch
that is then stranded on its old owner.

A source bucket is fenced and drained, and every committed assignment is
durably acknowledged before the source flips ownership. Its counters and
row-version state are copied even when it has no user rows. Pending
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
