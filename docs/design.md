# Tarn: a distributed MySQL on Tarantool

Tarn is a MySQL-protocol SQL server whose storage is a set of Tarantool
shards. Every SQL table is an ordinary Tarantool space, so the same rows that
SQL clients read and write can be read directly as Key/Value by any Tarantool
client (`box.space['db.t'].index.primary:get{...}`). SQL transactions are
**strictly serializable** across shards; the commit protocol is specified in
TLA+ and model-checked with TLC, and the routing, coordinator-decision and
MySQL wire-codec code is verified in Verus.

This document describes the architecture, the protocol and its verification,
the test suites, the benchmarks on this machine and the known limitations.
The [README](../README.md) and [getting started](getting-started.md) cover building and running.

## 1. Architecture

```
 MySQL clients (mysql CLI, sysbench, JDBC, PyMySQL, ...)       Tarantool / KV clients
        │  MySQL protocol                                        │ IPROTO (get/select)
 ┌──────▼─────────────────────────────┐                          │
 │ tarn-server (any number, stateless)│                          │
 │  tarn-mysql   wire protocol        │                          │
 │  tarn-sql     parse/plan/execute,  │                          │
 │               transactions, 2PC    │                          │
 │  tarn-iproto  pipelined IPROTO     │                          │
 └──────┬──────────────┬──────────────┘                          │
        │ IPROTO (TCP or Unix socket), streams                   │
 ┌──────▼──────┐ ┌─────▼───────┐                                 │
 │ shard 1     │ │ shard 2     │  ...   ◄────────────────────────┘
 │ leader +    │ │ leader +    │   each shard: 1 or more Tarantool 3.9 instances,
 │ replicas    │ │ replicas    │   synchronous replication + Raft leader election,
 │ (lua/tarn)  │ │ (lua/tarn)  │   memtx (RAM) or vinyl (disk) tables, MVCC
 └─────────────┘ └─────────────┘
```

| Component | What it does |
|---|---|
| `crates/tarn-server` | The binary: cluster config, tokio runtime, accept loop, glues the MySQL protocol to SQL sessions. |
| `crates/tarn-mysql` | Server side of the MySQL protocol: HandshakeV10, `mysql_native_password` (auth switch from `caching_sha2_password`), text and binary result sets, prepared statements (`COM_STMT_*`, long data), `CLIENT_DEPRECATE_EOF`. All length-encoded integers/strings and packet framing go through the Verus-verified codec. |
| `crates/tarn-sql` | Parsing (sqlparser, MySQL dialect), literal normalization and a shared plan cache, planning (access-path selection, joins, aggregation), execution, sessions, the transaction coordinator, DDL, catalog cache, AUTO_INCREMENT. |
| `crates/tarn-iproto` | Async pipelined Tarantool client: many requests in flight per connection (matched by sync id), coalesced writes, zero-copy decoding, streams (interactive transactions), MP_DECIMAL. |
| `crates/tarn-verified` | Verus-verified code: shard routing, coordinator decision/recovery logic, MySQL wire codec. Compiled into the server (proof code is erased). |
| `lua/tarn.lua` | Storage-node module: system spaces, lock table, the participant side of the commit protocol, lock-aware reads, recovery fiber, DDL, counters. |
| `tla/` | TLA+ specification `TarnCommit.tla`, model configurations, TLC results. |

### 1.1 Data layout and routing

* Table `db.t` is the Tarantool space `"db.t"` with one field per column (same
  order and names) followed by a `bucket_id` field. Index 0 (`primary`) is the
  primary key, secondary indexes follow, and a non-unique `bucket_id` index is
  last: the layout vshard and crud expect. `ALTER TABLE` keeps every stored
  row as it is, so columns it adds get fields after `bucket_id` and the
  fields of dropped columns stay, renamed `_dropped_<n>` (§2.10); readers
  should go by field names.
* Rows are placed by a shard key (default: the primary key; `PARTITION BY
  KEY(cols)` chooses others). `bucket_id = crc32c(key) % count + 1` using
  vshard's `mpcrc32` definition (Tarantool's `digest.crc32`, strings hashed as
  raw bytes, other values as their msgpack encoding). A case-insensitive
  text column is hashed by its weight string (§1.3), so `'a'` and `'A'` land
  on the same shard.
* A row's bucket never changes; which shard owns a bucket does. The shards
  keep the map (`_tarn_buckets`: the buckets each one owns or is moving).
  The first SQL server on a new cluster makes it, with `count` buckets
  (`bucket_count`, 30,000 by default; a cluster made by a version from
  before the map keeps the count its rows were stored with, 3,000), and
  gives each shard an equal contiguous range; buckets then move between
  shards online (§2.12). SQL servers route by a copy of the map, and every
  request names the buckets it was routed by, so a shard refuses one that
  has moved and the server reads the map again.
* A case-insensitive text column that is part of an index has a second
  field after `bucket_id`, `_weight_<n>` (varbinary), holding its weight
  string; its indexes are built on that field (§1.3).
* A unique key that contains the shard key is enforced by its index on one
  shard. One that does not has a *global index* as well (§1.4).
* The table definitions live in `_tarn_catalog` on every shard; SQL servers
  cache them and reload on a catalog version bump.

Direct reads: a Tarantool client computes the bucket from the key (or simply
asks each shard) and does `space.index.primary:get{key}`. The `kvbench`
example does exactly that with the same Verus-verified routing code (and
the layout of a new cluster: after a move, a client finds the bucket's
shard in `tarn_buckets()`). A
case-insensitive key is looked up (and hashed) by its weight string, which
`SELECT WEIGHT_STRING(...)` returns (without trailing spaces).

### 1.2 Request paths

**Autocommit point read** (e.g. `SELECT c FROM t WHERE id = ?`): one IPROTO
call to the owning shard's leader, `tarn_select` with a lock check. The scan
and the check run without yielding, so they observe one state; if a prepared
cross-shard transaction holds a W lock on the row, the read waits until it is
applied (this is what makes autocommit reads strictly serializable, see §2.3).

**Counts** (`SELECT COUNT(*) FROM t WHERE <index range> [AND <simple
conditions>]`): each shard the range touches counts it with `tarn_count`,
which takes the same scan as `tarn_select` (and the same lock check or
stream) but evaluates the remaining conditions on the stored tuples and
returns one number; the SQL server sums them. An equality prefix without
further conditions is counted by the index (`index:count`, which MVCC
tracks as a read of the range). Inside a transaction the shards leave out
the stored versions of the rows it has written, and the SQL server adds
those of its buffered rows that match; the counted ranges are recorded for
validation exactly as reading the rows would record them.

**Autocommit single-shard DML** (`UPDATE ... WHERE pk = ?`, single-shard
`INSERT`, `DELETE ... WHERE pk = ?`): one call to `tarn_autocommit`, which runs
one local Tarantool transaction (lock check + apply + commit).

**Transactions** (`BEGIN ... COMMIT`, or statements that touch several shards):

* *Execution.* Reads go to each shard inside an IPROTO stream; the first read
  on a shard starts the stream's transaction inside the same call (no separate
  BEGIN round trip), and memtx MVCC tracks every read (points and gaps).
  Writes are buffered in the SQL server; a statement's writes are visible to
  the transaction's own later reads (read-your-writes overlay). Nothing
  reaches the shards before COMMIT.
* *Commit, single shard:* `tarn_commit_local` in the stream validates the
  read set against W locks of prepared cross-shard transactions, applies the
  buffered writes (with lock checks) and commits; MVCC aborts the transaction
  if anything it read was overwritten.
* *Commit, read-only (one or several shards):* each participant validates in
  its stream that no read was overwritten and nothing read is W-locked
  (`tarn_validate`; see §2.4 for how "not overwritten" is enforced).
* *Commit, read-write across shards:* a "parallel commit" two-phase commit.
  PREPARE runs on every participant in parallel and, in one local
  transaction, validates the reads, takes durable R/W locks (W locks carry the
  new tuple as an intent), and on the decision shard writes a STAGING record.
  The transaction is committed exactly when the record is STAGING and every
  participant is prepared, so the decision needs no extra round trip. APPLY
  then moves the intents into the plain spaces and releases the locks; the
  decision shard's APPLY also records COMMITTED in the same local
  transaction. The client is acknowledged after every APPLY succeeded (or,
  with `--ack-on-prepare`, right after the PREPAREs; see §2.7). An APPLY that
  keeps failing (shard down) is retried a few times and then left to
  recovery: the commit is acknowledged anyway, as with ack-on-prepare.
* *Recovery:* any storage node's recovery fiber (and any SQL server) can
  resolve a transaction whose coordinator vanished: fence the record (no
  record yet → ABORTED, refusing a late PREPARE), fence participants that
  never prepared with tombstones, then decide from the durable state and
  apply or abort everywhere. Recovery may race a slow-but-alive coordinator.
* *Record lifecycle:* PREPAREs of transactions older than `max_txn_age`
  (120 s) are refused, and protocol records are garbage-collected only after
  that horizon, under the rules of §2.6 that keep "no record" meaningful for
  recovery.

**Pages.** A storage read returns at most 4096 rows, or rows of 4 MiB,
per call (`scan_page_rows`, `scan_page_bytes` of `lua/tarn.lua`) with the
position of its last row, and the SQL server continues after it, applying
a single-table WHERE to each page as it arrives (issue #77). Every row of
a reply is held in the storage node's Lua heap until the reply is sent:
without pages, a scan of millions of rows took that heap to Tarantool's
Lua memory limit, where an allocation that failed outside a protected call
killed the node. A read that finds less than a reserve of that heap free
(1/16 of the limit, at least 16 MiB) fails with 1041 instead; counts
(`tarn_count`) return no rows. A fiber checks the heap every 100 ms and
runs a full collection when less than that reserve is free (at most once a
second): LuaJIT starts a cycle only when the heap reaches twice what the
last one left, so a heap of garbage near the limit otherwise stays there,
and every call fails with "not enough memory" until something collects it
(issue #73). Inside a transaction the pages are reads of
its stream (one MVCC view), and the whole range is recorded once for
validation. An autocommit read that needs more than one page runs as an
implicit read-only transaction, as one that needs several shards does. A
READ COMMITTED SELECT that needs more than one page reads in a read-only
serializable transaction overlaid with its transaction's writes (§2.9).

**Ordered pages or a longer prefix.** When a single-table `SELECT ...
ORDER BY ... LIMIT n` could read either a range with a longer equality
prefix (read whole, then sorted) or a range of an index that follows
`ORDER BY` (paged until `n` + `OFFSET` rows qualify), the SQL server asks
the shard both prefixes route to for their row counts first
(`tarn_estimate`, issue #81) and pages when `want * n_ordered / n_prefix <
n_prefix`. Memtx counts both in its indexes. Vinyl, whose counts scan,
measures what paging would read instead (issue #93): it reads the ordered
range's first page (`4 * want` rows, in the page's direction): with
`want` rows of the prefix in it, paging reads that page; else it
extrapolates from
their share of the page, then counts the prefix only up to that cost. When
the page holds no row of the prefix it takes the index size for the
ordered range and counts the prefix only up to where the answer is
certain. No scan goes past one page. Both ways assume that the rows of the
prefix are what the paged read filters on: a selective condition outside
the prefix's index makes paging read more than estimated. The counts
are no reads of the transaction: WHERE filters either path, so they change
how many rows are read, never which rows qualify.

**Merged ordered pages.** An ordered `LIMIT` over an index whose equality
prefix does not fix the shard key (`WHERE state = ? ORDER BY created_at
LIMIT n` on a table sharded by `tenant_id`) pages the range on every shard
at once (issue #86): each shard's first page is `n` + `OFFSET` rows in
index order, and the SQL server merges the pages by index key, then
primary key (the order of ties within each shard's index). It takes the
next row only while every shard either has rows left in its page, has no
more, or has already read past that row (WHERE rejected the rest of its
page); otherwise it asks just those shards for their next page,
concurrently. Each shard's pages are what a single-shard page is: in a
serializable transaction, reads of its stream recorded up to the last row
that shard returned (rows returned but not used stay recorded, which only
widens a conflict); under READ COMMITTED, a shard whose page ran out reads
its range again from the start with a larger limit, so every shard's rows
are one committed state of its range. The planner prefers such an index
only when no equality in `WHERE` fixes the shard key, so it never displaces
a range on one shard; locking reads (`FOR UPDATE`) still read and sort.
The pager retains the same bucket map and shard set across rounds: refills
use the original bucket ownership, and reads outside transaction streams
keep that map's epoch and membership checks. A move or join therefore
refuses an outdated read instead of mixing pages from different maps.

**Co-located join reads.** A join whose tables have identical native shard
key types and collations, primary keys beginning with those shard keys, and
ordinary join equalities proving the same bound root key can use one
`tarn_select_join` request. Each descriptor reads a constant primary prefix
or derives primary prefixes from the native key fields of one earlier alias.
The shard returns complete per-alias candidate rowsets and the prefixes it
actually probed, including misses. Rust applies the original access keys,
INNER/LEFT padding, residual predicates and final SQL processing. Native CI
weight fields are copied directly; DECIMAL/float probes and representation
coercions retain the existing executor.

Autocommit and READ COMMITTED reads share one read-confirmed native
transaction and one `checked_read` lock-history interval across all aliases.
The final check covers the union of all probed table prefixes and DDL
claims; a conflict or incomplete history restarts the whole group.
Serializable streams retain their existing MVCC transaction and record each
returned point/range with its root bucket for commit validation. The bucket is checked before and after the RPC. This refines
`SnapshotRead` over the union of its abstract keys; the grouped-table and
shared-snapshot witnesses are described in [verification](verification.md).

The optimization requires at most 16 aliases, one earlier PK parent per
prefix, and no buffered writes to the participating tables. Locking reads
and unsupported paths use the ordinary executor. All slots share
`scan_page_rows` and `scan_page_bytes` limits (4096 rows and 4 MiB by
default), with at most 4096 generated prefixes and key metadata charged to
the byte budget. An exhausted budget discards the entire grouped result
and falls back; it never produces a truncated join. A small eligible
autocommit read therefore needs one lock-checked RPC and no coordinator
validation call. Transactional writes keep the established overlay path.

Conflicts surface as MySQL error 1213 (`ER_LOCK_DEADLOCK`, SQLSTATE 40001),
which clients already treat as "retry the transaction". Autocommit statements
are retried transparently (up to 20 times with backoff). A shard leader
change (the request reached a demoted node, which answers ER_READONLY) and a
connection lost during a read are reported the same way: nothing was
written. A connection lost while a commit or an autocommit write was in
flight is different: the write may or may not have happened, so Tarn reports
error 1180 with SQLSTATE 40003 ("statement completion unknown") and never
re-sends the request.

### 1.3 Collations

Text compares under its column's collation (issue #13): `utf8mb4_bin`, or
MariaDB's `utf8mb4_general_ci` and `utf8mb4_unicode_ci` and their NO PAD
variants, reproduced from MariaDB 10.11's own weight tables
(`crates/tarn-sql/src/collation_data.rs`, generated from `WEIGHT_STRING()`
by `scripts/gen_collations.py`: 1,108 general_ci code points whose weight is
not the code point itself, 12,060 unicode_ci code points with listed
weights, the rest computed). Other collation names map to the closest of
these ([SQL compatibility](sql-compatibility.md#collations)). Under a
`_ci` collation a string's *key* is its weight string, big-endian, without
trailing space weights under PAD SPACE: two strings are equal exactly when
their keys are, and order as their keys do.

**Where the storage needs the collation.** Equality decides three things
below the SQL layer: which values a unique index refuses, which keys a lock
names (the lock index, row locks and read sets compare msgpack-encoded index
keys), and which shard a shard-key value routes to. So a case-insensitive
column that is part of an index gets a *weight field*, `_weight_<n>`: a
varbinary field after `bucket_id` that holds the column's key, written with
every row (`TableDef::encode_tuple`, and the update operations of the
autocommit fast path). The column's indexes are built on that field, the
shard key hashes it, and every index key the SQL servers send -- lookups,
ranges, read sets, row locks, the identities of rows in the write buffer --
holds it (`TableDef::index_key`). Byte order on keys is the collation's
order, so `'a'` and `'A'` are one key in every index, lock and route by
construction, and the commit protocol, the lock index and the storage module
needed no change. `tests/sql/txntest.py` races two cross-shard transactions
that insert case variants of one key, at both isolation levels: one commits,
the other fails with 1062 or 1213.

**Why not Tarantool's ICU collations.** An index part can carry an ICU
collation (`unicode_ci`), which would keep the stored layout and let direct
readers find `'ALICE'` by `'alice'`. Routing is what rules it out: equal
strings must hash alike, so the bucket would have to be computed from a
canonical form of everything ICU considers equal. ICU's primary sort keys
change between ICU versions, so bucket ids (stored in every tuple) and the
values a unique index refuses would depend on the ICU library of whichever
machine computed them, and no fixed normalization captures ICU's equality
exactly (expansions, ignorable characters, Hiragana equal to Katakana, ...).
The SQL servers would also need ICU to sort, group and deduplicate in the
indexes' order, and the lock index a canonical form of its keys. A
collation defined by a fixed table, as MySQL's are (a collation's weights
never change; new behaviour gets a new collation name), keeps placement and
uniqueness stable, and matches MariaDB exactly instead of approximately.

**The cost.** A weight field roughly doubles the stored size of an indexed
case-insensitive column, and a direct reader looks such a key up by its
weight string. Adding an index on an existing case-insensitive column that
has no weight field yet would mean rewriting every row, which `ALTER TABLE`
does not do (§2.10), so it is refused (1235), and so is changing the
collation of an indexed column (its index order would change). A column
that the same statement adds gets its weight field at once: older rows end
before both fields.

**In the SQL layer.** The binder derives every text expression's collation
with MySQL's coercibility rules (an explicit `COLLATE` wins over a column,
a column over a literal; between two columns a `_bin` collation wins and
two other different ones are error 1267) and stores it in each comparison
(`=`, `IN`, `BETWEEN`, `LIKE`, `CASE`, `NULLIF`, `GREATEST`, `LEAST`,
`LOCATE`). `ORDER BY` items become collation keys (`Expr::Key`); `GROUP BY`
items, `DISTINCT` columns and aggregates record their collation, and `UNION`
takes it from the branches' result metadata. The planner uses an index for
a predicate only if the index orders by the predicate's collation -- or, for
an equality, if the index is case-insensitive and the comparison binary:
the index then finds a superset, which the statement's filter narrows.
Literals take `collation_connection` (from the client's handshake or `SET
NAMES`), and plans are cached per connection collation.

### 1.4 Global unique indexes

A unique key that does not contain the shard key (issue #14), such as
`UNIQUE (tenant_id, handle)` next to `PRIMARY KEY (id)`, cannot be enforced
by one shard: two rows with one value may live on different shards. Such a
key gets a global index, a hidden space `_tarn_uix_<table space id>_<index
id>` on every shard, created, truncated and dropped with the table. It holds
one entry per row whose key has no NULL part: the key's columns (as the
table's indexes hold them, so a case-insensitive column by its weight
string), then the row's primary key. The entry's primary key is the unique
key, and entries are sharded by it.

The SQL servers write the entries as ordinary buffered writes in the same
transaction as the rows (`Session::stage`): an insert adds the row's entry,
a delete removes it, an update that changes the key does both, and an update
that leaves it alone touches nothing (`tarn_verified::uix::index_delta`,
§3). The commit protocol then does the rest. An insert of an entry whose key
is held fails like any duplicate primary key (1062, under the unique key's
name), in a local commit or by a NO vote in PREPARE. Two transactions that
insert one value from rows on different shards write the same key on one
shard, where W locks and MVCC let only one of them commit. The entry and its
row are committed atomically, and recovery, fences and garbage collection
treat entries like any row.

Entry writes are fenced with their table's definition version (§2.10), so a
`TRUNCATE`, `DROP TABLE` or `ALTER TABLE` of the table refuses them as it
refuses the rows. A transaction may move a value from one row to another, or
swap two rows' values: the commit steps write a row that a unique index
refuses again after the others, and take rows that swap values out before
writing them.

Finding the row that holds a value (`INSERT IGNORE`, `ON DUPLICATE KEY
UPDATE`, `REPLACE`, an `UPDATE` to a new value) takes two reads: the entry
on its shard, then the row by primary key. So does a lookup by the full key
(`WHERE tenant_id = ? AND handle = ?`, also with `IN`): one call per shard
the entries live on, then per shard their rows live on, instead of one per
shard (issue #88). A READ COMMITTED SELECT that finds a row no longer
holding its entry's key (the value moved between the two reads) reads again
in a snapshot; READ COMMITTED UPDATE and DELETE candidates, read outside any
snapshot, still ask every shard. Serializable transactions read both through
their streams, so both are validated at commit. READ COMMITTED transactions
lock the entry's key (X) and the row and read both under the locks, so
concurrent writers of one value queue on the entry's shard as they queue on
a local unique key (§2.9); deleting an entry also locks its key.

What it costs: an insert, a delete, or an update of a key column of such a
table writes up to one more shard per global index, so it becomes a
cross-shard commit unless the entry happens to live on the row's shard, and
it leaves the single-call autocommit path. Reads and updates of other
columns cost nothing more. The table also keeps its own index of the key,
unique within each shard. Writes made directly through Tarantool bypass the
global index.

`ALTER TABLE ... ADD UNIQUE` (and `CREATE UNIQUE INDEX`) of such a key on a
table with rows builds the global index from them (issue #88). PREPARE
raises the table's fence on every shard (§2.10) and creates the empty space,
and stores the fence (`fence:<space id>` in `_tarn_meta`) so that it
outlives a restart or a leader change: only the new definition writes the
index's entries. Once every shard has prepared, the SQL server reads the
rows a page at a time and writes their entries in batches to the entries'
shards, where a value held by two rows fails the change with 1062 (and ABORT
drops the space). The shards accept those writes only while the change is
prepared and not applied, and only from the run of the job whose PREPARE
stored the fence: each takeover of the job counts a generation
(`tarn_job_touch`), which its steps carry, and a shard refuses the PREPARE
and the fill of an earlier generation than the one it holds, and any PREPARE
of a change it has aborted (ABORT leaves a tombstone). Until the first shard
applies, the rows cannot change, so every fill of the generation writes the
same entries, and an entry written again (a call sent again) counts once.
After that, rows written with the new definition change again, but by then
no earlier run of the job can fill: the later one prepared on every shard
before it filled. APPLY then installs the definition that maintains the
index and removes the stored fence. Like a row check, the fill is refused
(1846) when a shard holds more than `tarn.cfg.alter_check_rows` rows of the
table. Writes to the table wait meanwhile, as they do while a memtx index is
built; an online build, as #43 does for vinyl indexes, is still to come.
Dropping the key drops the space in APPLY. Renaming one, or changing its
columns or their types, is refused (1846).

## 2. Isolation and the commit protocol

### 2.1 Guarantees

* SQL transactions (explicit or implicit, single- or cross-shard) are
  **strictly serializable** by default: there is a serial order consistent
  with real time.
* Sessions may choose **READ COMMITTED** instead, with InnoDB's semantics
  (§2.9): plain reads see the latest committed data per statement and never
  abort; writes and locking reads take pessimistic row locks; no update is
  lost. Commits stay atomic across shards, durable on a quorum and visible in
  real time, and the writes of READ COMMITTED transactions are ordered as
  under strict two-phase locking, also against serializable transactions.
* Direct Key/Value readers see only committed data (`read-confirmed`
  isolation: in replicated shards, only data acknowledged by a quorum), and a
  direct read issued after an SQL COMMIT returned sees that commit (except
  when an APPLY failed and the commit was acknowledged without it, see §1.2).
* Documented trade-offs (checked by TLC as *expected* violations): a direct
  reader that reads two rows of a cross-shard transaction on two different
  shards may see one applied and the other not yet (`Tradeoff_DirectReadAtomic`;
  SQL readers never can), and with `--ack-on-prepare` a direct read right
  after COMMIT may not see the commit yet (`Tradeoff_AckOnPrepare_DirectRYW`).

### 2.2 The TLA+ model (`tla/TarnCommit.tla`)

The model has shards, keys with owners (which a bucket move changes,
§2.12), three transactions with fixed read/write sets, direct (non-SQL)
readers, and the actions of the protocol:
reads, single-shard commit, snapshot reads, read-only validation, PREPARE
(with the decision record), coordinator decision/finish/abort, APPLY, ABORT
(with tombstones), timeout-triggered recovery (fence record, fence
participants, decide), re-delivered PREPAREs, and faults: coordinator crashes,
message loss, and **shard leader failover**, which keeps durable state
(quorum-replicated) but destroys the MVCC read tracking of streams still
executing. Local Tarantool transactions are atomic steps (with one
exception that matters to row locks, §2.9). Two options model
the record lifecycle of the implementation: `ApplyMarksCommitted` (the
decision shard's APPLY records COMMITTED) and `GC` (transactions expire,
after which their PREPAREs are refused, and records are garbage-collected
under the rules of §2.6).

Invariants checked in every configuration:

| Invariant | Meaning |
|---|---|
| `TypeOK` | well-formed state |
| `AppliedOnlyIfCommitted` | no participant applies a transaction that is not committed |
| `NoAbortAfterCommit` | a committed transaction is never aborted anywhere |
| `PlainOnlyCommitted` | the plain spaces only ever contain committed values |
| `DirectReadsClean` | direct readers never observe uncommitted data |
| `DirectReadYourWrites` | a direct read after an acknowledged commit sees it |
| `StrictSerializable` | the acknowledged transactions have a serial order, consistent with real time, that explains every value they read (READ COMMITTED transactions: every value they read under a lock; a scan of every shard: each row exactly once) |
| `NoLostUpdate` | every READ COMMITTED write lands on the value its transaction read under the lock |
| `RCReadsSeeAcked` | a READ COMMITTED read sees every transaction acknowledged before it started |
| `UniqueHolds` | no key is inserted by two committed transactions (unique keys, §1.4) |

`StrictSerializable` is checked constructively: the spec records the order in
which writes reached each key and the real-time predecessors of each
transaction, and the invariant searches for a witness order.

Each workload (`MC.tla`) is three transactions over two shards: *Skew* (two
cross-shard read-modify-writes that read each other's write target, plus a
cross-shard reader), *Local* (single-shard blind write racing a cross-shard
transaction, observed by a cross-shard reader), *Frac* (cross-shard blind
writer, cross-shard reader, single-shard read-modify-write), *Recov* (cross-
shard read-modify-write under crashes and message loss, plus single-shard
writer and reader). Three more workloads exercise READ COMMITTED (§2.9).
*Uniq* models a unique key without the shard key (§1.4): an insert (`IKeys`,
refused when the key holds a value) of one global index entry by a
cross-shard transaction that also inserts its row, and by a local one on the
entry's shard, serializable or READ COMMITTED, observed by a cross-shard
reader of the entry and the row; `BugInsertNoExistCheck` drops the check and
TLC finds two committed holders. The model never deletes keys, so a transaction that
frees a value and takes another (REPLACE, an UPDATE of the key) is argued
rather than checked: its deletes and inserts on a shard are in one PREPARE
or local commit, which applies them together. *MoveScan* and *MoveRC* move a
row to the other shard while transactions run (§2.12): rows have a place
that the move changes, a request reaches the shard it was routed to, and
scans read every shard.

### 2.3 What TLC found

The first complete model found a **strict-serializability violation in the
design**: single-shard reads (autocommit SELECTs and single-shard read-only
transactions) did not look at the lock table. TLC's 10-step counterexample:
cross-shard T1 writes x (shard A) and y (shard B), is prepared everywhere
(hence committed) and applied on B only; single-shard T2 reads T1's y, writes
y and is acknowledged; T3 starts after that and reads x on A, still the old
value because T1's APPLY has not reached A. T2 follows T1 (it read T1's
write), T3 follows T2 in real time, yet T3 must precede T1: a cycle, even
though T1's own client had not been acknowledged yet. Fix: single-shard reads
and local commits check the W locks on what they read and wait for the APPLY
(the `NoWLocks` condition in `SnapshotRead`/`CommitLocal`, `lock_check` in
`tarn_select`). `Bug_LocalReadsIgnoreWLocks` keeps the old behaviour and TLC
still finds the counterexample.

Every protective mechanism has a "bug" switch, and TLC finds a violation for
each one (results in §5): no read locks in PREPARE, no W-lock check on reads
in PREPARE, recovery that aborts without tombstones (a late PREPARE then
resurrects an aborted transaction: `AppliedOnlyIfCommitted` fails), local
commits that ignore R locks, single-shard reads that ignore W locks,
read-only validation without the W-lock check or without the "reads
unchanged" check, garbage collection of applied records regardless of the
decision record, garbage collection without the expiry horizon, fencing
a missing decision record after expiry, and three for bucket moves (§2.12):
a move without the drain, a receiver that does not hold the bucket, and a
sender that serves requests for a bucket it has sent.

### 2.4 A model/implementation gap found by testing: memtx read views

`ValidateRO` (cross-shard read-only commit) requires that the transaction's
reads are unchanged. The first implementation relied on memtx MVCC for that,
as `tarn_commit_local` and `tarn_prepare` do. But memtx treats read-only
transactions differently: when a concurrent commit overwrites something a
read-only transaction read, the reader is not aborted; it is moved into a
*read view* where it keeps seeing the old data and **can still commit**
(`txn_send_to_read_view` in `src/box/txn.c`). Writes from a read view are
refused (`txn_begin_stmt` aborts the transaction with a conflict).

A targeted test (`tests/sql/txntest.py::test_realtime_order`) reproduced the
consequence deterministically: R reads y on shard B; U updates y (single
shard) and commits; V updates x on shard A and commits; R reads x and commits
having seen V but not U, although U finished before V started. That is
serializable but not *strictly* serializable. The fix makes the check
explicit: `tarn_validate` performs a probe write into a data-temporary space
(no WAL, not replicated) inside the stream, which fails exactly when the
stream is in a read view. The model got the corresponding switch
`BugROValidateNoReadCheck`, and TLC finds the violation in 5 seconds when the
check is removed. (Read-write paths were never affected: their writes, or
PREPARE's lock records, hit the same refusal.)

### 2.5 The lock index: correct but slow, found by benchmarking

Lock checks first read the `_tarn_locks`/`_tarn_rlocks` spaces from inside
the transaction doing the check. Under memtx MVCC every such read leaves a
read tracker, and a range check had to scan all lock records of a table, so
two concurrent PREPAREs on one shard that locked *different* rows of a table
conflicted with each other through the lock space itself. That is safe
(only extra aborts) but sysbench `oltp_read_write` collapsed under
concurrency: 63 transactions/s at 64 threads with 8,672 conflicts in 15 s.

Lock checks now consult an in-memory index of the lock records (`LK`/`RL` in
`tarn.lua`) that is never read through MVCC. It is updated in the same
non-yielding step as the checks (so check-and-lock stays atomic, as
TarnCommit's `Prepare` requires) and it shows the locks of PREPAREs still
waiting for their WAL write or quorum. A PREPARE whose local transaction
rolls back removes its entries (`box.on_rollback`); APPLY/ABORT remove them
when their transaction commits (`box.on_commit`). The durable records stay
in the spaces for recovery; the index is rebuilt from them at startup and
after any election or read-only state change, before the next check, and
only on a writable node (replicas do not maintain it). The rebuild reads the
lock spaces under two isolation levels and takes the union: the
read-committed view holds the locks of PREPAREs still waiting for WAL or a
quorum, the read-confirmed view holds locks that a pending APPLY/ABORT is
deleting; the undo/release hooks of those transactions correct the index
whichever way they end. (The first version rebuilt from the read-confirmed
view only and dropped the locks of PREPAREs waiting for a quorum; see §2.8.)
Because checks see current lock state rather than the reads' snapshot,
every read-only validation (single-shard too) does the probe write of §2.4,
so "reads unchanged" plus "no W lock now" together give the model's atomic
`SnapshotRead`/`ValidateRO`.

### 2.6 Record lifecycle and timing assumptions

Every protocol record carries an expiry time: `max_txn_age` (120 s) plus a
clock-skew allowance (60 s) after it was written. A participant refuses a
PREPARE whose transaction started more than `max_txn_age` ago (by the
coordinator's timestamp, on the participant's clock), so after a record's
expiry no PREPARE of its transaction can be accepted any more. Records are
garbage-collected only after expiry, and

* a *prepared* participant record never (recovery resolves it first);
* an *applied* participant record only once the decision record is final
  (COMMITTED) or gone -- otherwise recovery could read the missing record as
  "never prepared", fence it and abort a transaction that is committed and
  already applied on that shard;
* a *tombstone* (aborted participant) any time after expiry;
* a *decision record* only once it is final and no participant is still
  prepared.

Recovery fences a missing decision record only while the transaction has
not expired or some participant is still prepared (either rules out "the
record was collected"); otherwise it reports the outcome as unknown. An
expired PREPARE is refused without writing anything and without a vote: a
NO could contradict a commit whose records are already gone (TLC found this,
§2.8). The `*_GC` configurations check all of this.

Timing assumptions remain in two places. The expiry rules assume that clocks
differ by less than the 60 s allowance. And a leader that is partitioned
away but does not yet know it has been replaced can still answer autocommit
reads from its (possibly stale) data until Tarantool's
`election_fencing_mode = 'strict'` makes it step down, which bounds the
window by the election timeout; this is the usual leader-lease assumption
and is not part of the TLA+ model, which treats leader changes as atomic.
A leader that knows it was demoted refuses every lock check, validation and
commit with ER_READONLY (it may still serve plain reads of an open stream,
whose commit then fails), and a validation in a stream that began under an
earlier election term is refused.

### 2.7 Ack-on-prepare

With `--ack-on-prepare`, the client is acknowledged as soon as every PREPARE
succeeded (one round trip instead of two); APPLY happens in the background.
TLC shows SQL-level strict serializability is preserved
(`Skew_AckOnPrepare`, `Recov_AckOnPrepare`: SQL readers wait on the W locks),
while direct read-your-writes is lost (`Tradeoff_AckOnPrepare_DirectRYW`).
The same argument covers the default mode when an APPLY keeps failing and
the commit is acknowledged without it.

### 2.8 Findings of an independent review

After the first complete version, an independent code review (a separate
agent auditing the code against the spec, with its own experiments on real
Tarantool instances) found five real bugs, all reproduced and fixed:

1. **Committed writes lost after a long shard outage (critical).** The
   coordinator never recorded COMMITTED, and applied participant records were
   garbage-collected after 30 s without looking at the decision record. If a
   participant applied a transaction while another one was down for longer
   than that, recovery later read the collected record as "never prepared",
   fenced it and aborted the transaction on the other shard. Fixed by the
   record lifecycle of §2.6; the model now covers garbage collection, and
   TLC finds this bug again in `Bug_GCIgnoresDecision`.
2. **Read-only commits validated by a demoted leader.** `tarn_validate`'s
   probe write goes to a data-temporary space, which Tarantool accepts on
   read-only nodes, and SQL servers kept using a demoted leader whose
   connection survived. Fixed: validation, lock checks and commits refuse to
   run on a read-only node and validation checks the election term; an
   ER_READONLY reply marks the connection stale on every code path, so the
   next request finds the new leader.
3. **Autocommit DML applied twice.** A lost connection made the SQL server
   re-send any call once, including `UPDATE ... SET v = v + 1`. Fixed: only
   idempotent calls are re-sent; otherwise the outcome is reported as unknown
   (1180/40003). `tests/faults.sh` reproduces the scenario with a proxy that
   swallows the reply.
4. **Locks stuck in the lock index.** An error inside PREPARE after its
   locks entered the index but before the undo hook was registered left them
   there until a restart. Fixed by registering the undo first.
5. **Lock index rebuilt without pending PREPAREs.** See §2.5.

Extending the model for (1) also paid off directly: the first version of the
GC rules let an expired PREPARE vote NO. TLC found a 13-step trace in which
a re-delivered PREPARE meets a participant whose applied record was already
collected and votes NO, and a slow coordinator aborts the committed
transaction on that participant. Expired PREPAREs now get a third answer,
"expired", that the coordinator treats as unknown.

### 2.9 READ COMMITTED and row locks

Sessions pick their isolation level as in MySQL (`transaction_isolation`,
`SET [GLOBAL | SESSION] TRANSACTION ISOLATION LEVEL`, the next-transaction
forms; the server default is SERIALIZABLE). READ UNCOMMITTED and READ
COMMITTED run as READ COMMITTED; REPEATABLE READ and SERIALIZABLE run
strictly serializable. READ COMMITTED follows InnoDB:

* **Plain reads** are autocommit-style storage reads of the latest committed
  state, one per statement, overlaid with the transaction's own buffered
  writes (an ordered page with `LIMIT` reads its range again from the start
  with a larger limit when it needs more, so it sees one state of the
  range). They take no locks, record no read set and are never validated, so
  they never cause an abort. Like autocommit reads they wait while a
  prepared cross-shard transaction W-locks what they read, because that
  transaction may already be committed and acknowledged (`RCRead`). A
  `SELECT` whose reads need more than one storage page (§1.2) reads them
  in one state instead, as InnoDB's statement reads one view: in a
  read-only serializable transaction overlaid with the transaction's own
  writes, validated when the statement ends. Page by page from the latest
  state, a row moved within an index meanwhile could be missed or read
  twice. Such a statement may therefore fail with 1213 when writes keep
  changing what it reads (after 20 retries), like an autocommit read, and
  as with any 1213 the transaction is then rolled back; short ones never
  do. UPDATE and DELETE read their candidates page by
  page: each is read again under its lock.
* **Row locks** (`tarn_lock`) are taken by writes and locking reads and held
  until the transaction ends. A lock is named by (space, index, key): a row
  by its primary key, or a value of a unique key. Modes are S and X; a
  request that conflicts with a holder or an earlier waiter queues in FIFO
  order (an S→X upgrade goes first). It fails with 1205 after
  `innodb_lock_wait_timeout` (the statement is rolled back, the transaction
  kept, as with InnoDB's default `innodb_rollback_on_timeout=OFF`), at once
  with 3572 under NOWAIT, or is skipped under SKIP LOCKED. A granted request
  waits out prepared transactions' W locks on the key and then returns the
  latest committed row: the read under the lock (`RCLock`). A locking read
  with LIMIT over one index range, in index order, reads its candidates a
  page at a time and locks as it goes, like InnoDB, so `SKIP LOCKED` over a
  long queue stops at the first free rows (`bench/RESULTS.md` has the
  numbers). When that range is an equality prefix of the primary key that
  is all of the WHERE clause, the shard does the scan and the locking in
  one call (`tarn_lock_range`): each page is read as an RC read, each row
  locked as `tarn_lock` locks it and read again under the lock, so the
  statement makes one storage round trip instead of a read and a lock
  request per page (up to 256 rows; the call yields between pages). Under
  `SKIP LOCKED` the page is read without waiting for prepared transactions'
  W locks: a row one holds is skipped under its lock, as a row another
  transaction locks is. The parts of a `UNION ALL` of such reads in a READ
  COMMITTED transaction go out together, one call per shard and lock mode,
  each locking its parts in statement order; under `SKIP LOCKED` or
  `NOWAIT`, which never wait, the calls to different shards run in
  parallel (a request that may wait goes alone, so that the cross-shard
  deadlock detector sees it waiting on one shard).
* **Writes re-read under the lock.** UPDATE and DELETE take candidates from
  the latest committed state, lock each in order (in batches, as far as
  LIMIT needs) and re-check WHERE on the version read under the lock.
  Rows that no longer match are unlocked at once; the new values are
  computed from the locked version, so `SET n = n - 1` never loses a
  concurrent decrement. A row that starts to match after the candidate scan
  is not seen: like InnoDB at READ COMMITTED there are no gap locks. A
  statement that names its rows by full primary key (`WHERE id = ?`, `id IN
  (...)`, possibly with more conditions) needs no candidate scan: it locks
  the keys and reads the rows under the locks in one `tarn_lock` call per
  shard, then re-checks WHERE the same way. Like InnoDB, whose
  semi-consistent read does not apply to unique searches, it waits for a
  locked row even if that row's committed version does not match.
* **Unique keys are locked by value.** INSERT, REPLACE and upserts lock the
  primary key and every unique-key value of each new row, exclusively, and
  read the row holding that value under the lock. Concurrent inserts of one
  value queue; the loser sees the winner's row (1062, its ON DUPLICATE KEY
  UPDATE branch, or nothing with IGNORE), or goes ahead if the winner rolls
  back.
* **Deadlocks.** A shard fails a request with 1213 at once if waiting would
  close a cycle in its own wait-for graph. Cycles through several shards are
  found by the SQL servers, each starting from its own transactions' lock
  requests (issue #26). Every `deadlock_interval` (1 s), a server with
  requests that have waited for 50 ms or more follows the wait-for edges
  backwards from them: it asks the shards where they wait or hold row locks
  for their waits (`tarn_lock_waits`: waiter, wait id, the transactions it
  waits for, and the other shards where the waiter holds row locks, which
  its request named), then the shards where each transaction found waiting
  for one already reached holds row locks, until no new shard comes up. A
  transaction's waiters wait either where it holds the lock or behind it in
  the queue where it waits itself, so the walk finds every transaction that
  waits for the starting ones, directly or through others; and since a
  transaction makes one lock request at a time and takes no lock while it
  waits, the shards its request named are still exact. Every transaction
  of a cycle waits, and its server walks from it, so every cycle is found.
  Each shard is asked once per walk, so a server whose transactions do not
  wait makes no call, and with contention the calls grow with the number of
  waits, not with the number of shards (each server used to ask every
  shard every second). The server then asks the shards of each cycle's
  waits again, and keeps the cycle if each wait is still pending with the
  same wait id *and* still waits for the next transaction of the cycle.
  Each such edge was there at both observations and all the time between:
  a transaction that left a wait's waits-for set can only queue behind the
  waiter again. Every first observation came before every second one, so
  at some moment all the edges existed at once, and the cycle is a real
  deadlock, which lasts until it is broken. Comparing wait ids alone, as
  the first detector did, could keep a cycle through a holder that released
  the lock after its edge was seen and only then began to wait itself. The
  cycle's transaction with the largest id (the same choice on every SQL
  server) is failed with 1213 where it waits (`tarn_lock_victim`) and
  rolled back.
* **Commit.** An RC transaction that wrote on one shard commits with one
  local transaction (`tarn_commit_rc`, `CommitRC`); across shards it uses
  the same PREPARE/APPLY as serializable transactions. Either way the row
  locks are released only once the commit is confirmed (`box.on_commit`),
  and a read-only RC transaction just releases its locks. Its PREPAREs carry
  the time its commit started, so RC transactions have no `max_txn_age`
  limit (an RC transaction keeps no stream open).
* **Savepoints and statement rollback** (both levels): a savepoint is a
  copy of the transaction's write buffer, and so is the start of each
  statement; ROLLBACK TO restores the buffer and keeps the row locks taken
  since, as InnoDB does. 1205 and 3572 keep the transaction, 1213 ends it.
* **Serializable transactions** respect row locks: their commits refuse to
  write a row (or claim a unique value) that another transaction has locked
  (1213; `BugIgnorePLocks`), and autocommit writes wait for the lock. Their
  FOR UPDATE / FOR SHARE also take row locks, but read through the stream as
  before, so those reads stay validated at commit.

**The lock table is volatile.** It lives in the shard leader's memory: a
leader change empties it, and the connection between an SQL server and a
shard owns the locks taken over it, which are released when it closes. A
transaction's later lock requests and its commit on a shard go over the
connection its first lock request used there. Each commit carries the
*generation* of the locks it took on each shard (the lock table's id and
that connection's storage session), and the shard refuses the commit (1213)
if the table was reset or the session is gone; a lock request over a new
session returns a new generation, which the SQL server treats the same way.
A write is never applied after its lock was lost (`BugRCNoLockCheck`). A
request that fails (1205, 3572, 1213) after some of its keys were granted
keeps those locks until the transaction ends, as InnoDB does, and its reply
carries their generation, so that the transaction releases them with its
others even when it was its first request on that shard.

**A gap between the model and the implementation, found by testing.**
TarnCommit takes a local commit as one atomic step. In Tarantool,
`box.commit()` yields until the WAL (and, with synchronous replication, the
quorum) has the transaction, and until then readers outside it still see the
previous versions of its rows. The first implementation of `tarn_lock`
granted a lock and read the row at once; when an autocommit `UPDATE ... SET
n = n - 1` had checked for row locks, applied its change in memory and was
waiting for the WAL at that moment, the lock read the old value, and the
READ COMMITTED transaction's write then erased the autocommit update. The
concurrent counter test (`tests/sql/rctest.py`) lost 3 to 17 of 3,200
decrements per run. Writers that hold no row lock check for row locks before
they write, so only commits already in flight when a lock is granted can
still change the row: `tarn_lock` now waits for the local commits in
flight at the grant that write the row (`wait_commits`) before it reads,
and the counter test loses nothing. A commit records its writes once it has
done them (`inflight_writes`); until then it may write anything, and every
grant waits for it; afterwards only a grant on its primary key, or on a
unique value its old or new row holds, does. The model splits the local
commit's confirmation off (`infl`, `ConfirmLocal`: other readers still see
the old value until then) and blocks `RCLock` while a commit to the key is in flight; with
`BugRCLockReadsInFlight` TLC finds the lost update in a 5-step trace.

The model's READ COMMITTED transactions (the set `RC`) have a set `LSet` of
keys they lock and read under the lock (their writes are among them); their
plain reads are `RCRead`. A serial order must explain only the values they
read under locks, which orders their writes as under strict two-phase
locking. `NoLostUpdate` checks that every RC write lands on the value read
under its lock, and `RCReadsSeeAcked` that a plain read sees every
transaction acknowledged before it started. Workloads: `Counter` (an RC
read-modify-write of x on one shard, another across both shards, and a
serializable cross-shard read-modify-write of x and y), `Mixed` (an RC
writer of x that reads y, a serializable cross-shard read-modify-write of y,
and an RC transaction that locks y FOR UPDATE), and `Local` (two RC
read-modify-writes of x, one of them across shards, racing a serializable
single-shard one whose local commit may be in flight). Every protection has a switch that TLC must catch:
`BugRCNoLockCheck` (commit without checking that the locks survived: a
failover empties the table and another transaction's write is overwritten),
`BugRCLockIgnoresWLocks` (lock granted while a prepared transaction W-locks
the key), `BugRCReadBeforeLock` (write computed from the plain read made
before locking), `BugIgnorePLocks` (serializable commits ignore row locks)
and `BugRCLockReadsInFlight`. The model gives serializable transactions no
row locks. Those their locking reads take in the implementation only make
other transactions wait, fail or abort (which removes behaviours rather than
adding any), and the reads themselves are validated at commit as before; a
serializable commit that finds such a lock lost aborts, which is always
allowed.

### 2.10 Schema changes

`ALTER TABLE` (issue #16) changes a table while transactions read and write
it, on every shard and with SQL servers whose catalogs lag behind, without
rewriting rows.

**Layout.** A column has a field number that never changes. Fields of
columns created with the table are their positions; `bucket_id` follows;
a column `ALTER TABLE` adds gets the next field after the last one, and a
dropped column keeps its field (renamed `_dropped_<n>`, typed `any`, written
as nil from then on). Rows written before a column existed end before its
field and hold its *fill*: the default when it was added (MySQL fills
existing rows the same way, with the type's zero value for `NOT NULL`
without a default). A type change must keep the field's Tarantool type
(integer and unsigned may change into each other), so the stored values
stay valid; the SQL types may differ in ranges and lengths. Changes that
would need every value converted (`INT` to `VARCHAR`, a different
`DECIMAL` scale) are refused. The SQL servers map rows to fields
(`TableDef::encode_tuple`, `Rd::row_of`), and update operations address
fields by number, padding rows older than an added field with the fills.

**Fences.** Every table definition has a version (`ver`): the catalog
version of its last `ALTER TABLE`. Every commit request carries, for each
table it writes, the version of the definition its writes were encoded
with, and the shard refuses a request whose version is older than the
table's fence with 1412 (`ER_TABLE_DEF_CHANGED`) before it applies
anything. The check sits in every write path (CommitLocal, CommitRC,
autocommit, PREPARE) right before the writes, with no yield in between.
So a server that has not reloaded its catalog cannot write a row in an old
layout -- which matters: an old-layout full-row write would drop the value
of a newly added column. Its autocommit statements, and the first statement
of a transaction, are re-planned and run again with the new catalog. A
transaction that wrote a table cannot go on with a newer definition of it:
the rows in its write buffer are in the old column order, so its next
statement on that table fails with 1412 and the transaction rolls back (its
commit would meet the fence anyway). One that only read the table goes on
under READ COMMITTED, each statement decoding rows with its own definition;
under SERIALIZABLE the ALTER's DDL aborts the stream transactions that read
the table (memtx MVCC invalidates an altered space), which fail with 1213.

**Protocol.** One SQL server drives a change to version V through four
idempotent steps per shard (lua/tarn.lua "Schema changes"):

1. On a vinyl table, BUILD builds the new indexes under temporary names
   while writes go on: vinyl keeps an index current while it builds it, so
   no write waits for the build, however long it takes. A memtx table
   builds in PREPARE instead: with memtx MVCC, any write committed while an
   index build yields aborts the build.
   An index on an instant-added column with a non-NULL fill is deferred to
   PREPARE instead: its stored field (or collation weight field) must be
   physically present in every existing tuple. Otherwise the native index
   would hold NULL where SQL reads the fill, missing rows and failing to
   enforce logical uniqueness. PREPARE refuses that change with 1846;
   automatic backfill is not implemented. NULL fills need no such check.
2. PREPARE raises the table's fence to V in memory, waits until no local
   commit that passed the check is still in flight (`wait_commits`) and no
   prepared cross-shard transaction holds locks on the table (their writes
   were encoded with the old definition and are applied by APPLY of the
   commit protocol), checks the existing rows against the new types --
   rows that end early by the column's fill -- and builds the new indexes
   BUILD has not (a unique one fails on duplicates, 1062). Writes wait
   while it checks rows, so it refuses to check more rows than
   `tarn.cfg.alter_check_rows` (one million by default) on a shard, with
   1846, before raising the fence.
3. When every shard has prepared, APPLY drops the indexes that go, changes
   the space format, renames indexes and the space, and stores the new
   definition, with `ver` = V, in `_tarn_catalog` (where the fence survives
   restarts and leader changes: `FENCE` is rebuilt from it). An APPLY that
   finds the in-memory fence gone (a restart or a new leader since PREPARE)
   runs PREPARE again first.
4. The catalog version is bumped; the other SQL servers reload within their
   0.3 s poll, or at once when a statement meets the fence.

If a BUILD or a PREPARE fails anywhere (a row does not fit, a duplicate, a
timeout waiting for prepared transactions), every shard is ABORTed:
temporary indexes are dropped and the fence returns to the stored version.

From BUILD on, a new unique index refuses rows that would break it, so an
undecided write that the finished change would make fail fails at once
(1062), as in TiDB's write-only index state. A decided one cannot fail: a
cross-shard transaction prepared before BUILD may write such a row, and its
APPLY must land. That APPLY drops the index under construction and runs
again; PREPARE then builds the index behind the fence, where the duplicate
fails the schema change instead.

**Jobs.** A schema change -- `ALTER TABLE`, and `CREATE TABLE`, `DROP
TABLE` and `TRUNCATE` (below) -- is first recorded as a job on the first
shard, with one record per table name it touches (an `ALTER TABLE ...
RENAME` touches two). Schema changes of the same table run one at a time;
those of other tables do not wait, which matters since an `ALTER TABLE` of a
large vinyl table may build an index for hours. The job records its phase:
`prepare` until every shard has prepared, then `apply`, or `abort` if one
could not, written before the first APPLY or ABORT is sent. Its SQL server
touches it every few seconds; every SQL server looks for jobs untouched for
`ddl_job_stale` (30 s, `cluster.json`) at startup and every 10 s, takes one
over, and finishes it from its phase: back (ABORT everywhere) before
`apply`, forward (APPLY everywhere) after. A statement whose job is left
unfinished reports the outcome as unknown (1180).

Writes wait (in the SQL server: the statement is retried, up to the lock
wait timeout) from the moment PREPARE raises the fence until the servers
have reloaded the new definition; reads do not wait. Row checks, and index
builds on memtx tables, happen during that window. An `ADD INDEX` on a vinyl
table of 300,000 rows (`ddltest.py`, reading from disk) took 0.8 s, during
which another server's single-row updates went on (961 of them, none slower
than 39 ms); built behind the fence, the same index held them for the whole
build. The price of the short window is not keeping two layouts writable
at once (F1 and TiDB step through intermediate schema states with leases
instead).

**Why the commit protocol model is unchanged.** The fence only makes
participants refuse requests (a NO vote, a failed local commit) before
they write, which the model already allows at any time; the change itself
waits until no prepared transaction holds locks on the table, so no
prepared write is applied in an old layout after APPLY. An index under
construction likewise only refuses undecided writes; the APPLY it would
refuse runs again after dropping it, which the model sees as an APPLY
taken a little later. `tests/sql/
ddltest.py` alters a table about 300 times in 20 s while two SQL servers
write it (fast-path and read-modify-write autocommits, transfers,
READ COMMITTED transactions) and checks that no acknowledged increment is
lost or duplicated, and `lua/test/run.sh` checks the shard side (fences on
every write path, waiting for prepared transactions, checks with fill
values, temporary indexes, the job records).

**`CREATE TABLE`, `DROP TABLE` and `TRUNCATE`** (issue #30) run as jobs in
the same steps, so that each takes effect on every shard or on none:

* `CREATE TABLE`: PREPARE creates the space and its indexes, which no
  catalog names yet (the SQL servers read the first shard's), and APPLY
  names it in `_tarn_catalog`, on the first shard last. ABORT drops it.
* `DROP TABLE` and `TRUNCATE`: PREPARE raises the table's fence above every
  definition the SQL servers have, *holds* the table, and waits for the
  local commits in flight and the prepared transactions holding locks on it,
  as `ALTER TABLE`'s PREPARE does. The hold works like a W lock of a
  prepared transaction on every row: autocommit reads of the table, and
  READ COMMITTED row locks that read, wait for it; the validation of a
  transaction that read the table fails. APPLY drops or empties the space
  (`TRUNCATE` also stores the definition under a new version, so writes
  encoded before it stay refused) and releases the hold. The fence and the
  hold are stored (`hold:<space id>` in `_tarn_meta`), so they outlive a
  restart or a leader change between PREPARE and APPLY. ABORT releases the
  hold and lowers the fence.

A `TRUNCATE` or `DROP` thus behaves like a cross-shard transaction that
writes every row of the table: no transaction sees it applied on one shard
and not yet on another, a transaction prepared before it is applied before
it (its rows go with the `TRUNCATE`), and a transaction that read the
table before it cannot commit after it. memtx MVCC aborts the transactions
that read a space when DDL changes it; vinyl aborts only those that wrote
it, so APPLY also marks the stream transactions open on the shard as
having read a table that changed, and their validation fails if their
reads include it.

The commit protocol model is unchanged here too: a `TRUNCATE` is the
model's cross-shard transaction that deletes every row of the table, with
the hold as the W locks its PREPARE takes after the table's own drain, and
APPLY as its writes; the model's validation of reads (the probe write of
§2.4) is what the doomed streams restore for vinyl. A `DROP` is a
`TRUNCATE` after which the table is gone; a `CREATE` writes nothing a
transaction could have read.

`tests/ddlfaults.sh` checks the failures: with a shard down, each
statement fails and its job is rolled back once the shard is back (the
created table exists on no shard; the dropped or truncated one keeps its
rows); with a shard killed while it applies (a test hook holds its APPLY),
the statement reports an unknown outcome and the job is rolled forward on
every shard once the shard is back.

### 2.11 Tables on disk (vinyl)

A table is a memtx space (every row in RAM) or, with `ENGINE=vinyl`, a vinyl
space: an LSM tree on disk with a RAM cache, for data larger than RAM
(issue #23). `--default-engine` decides what `ENGINE=InnoDB` and tables
without the option get. The commit protocol's own spaces stay in memtx:
since Tarantool 3, one transaction may write memtx and vinyl spaces when
memtx MVCC is on, and checked on Tarn's build, a transaction that either
engine moves into a read view refuses writes in both, and its reads stay
consistent across them. So PREPARE, APPLY and the local commits write
their records and the rows in one local transaction whatever the engine,
and `tarn_validate`'s probe write detects overwritten vinyl reads as it
does memtx ones (§2.4).

**Yields.** What vinyl changes is that a read which misses the cache yields
while the page is read, and so may a write (an insert's duplicate check).
The model takes each commit step -- CommitLocal, CommitRC, autocommit
writes, Prepare -- as one atomic action, and on memtx the implementation
was atomic simply because nothing in it yielded before `box.commit()`. Each
step is now split (lua/tarn.lua "Yields"):

1. the storage reads and writes, inside the step's local transaction: they
   may yield, and MVCC aborts the commit if anything they read is changed
   by another commit before this one is prepared;
2. then, in one section that does not yield (`atomic`), the checks --
   definition fence, row locks, prepared transactions' locks, unique values
   -- the lock-index updates and the start of `box.commit()`, which
   prepares the transaction before it waits for the WAL.

The step's atomic action in the model is at the second part: the checks see
the state the commit is prepared in, and the reads are still current then
(else MVCC aborts it). Local commits also register as in flight before the
first part, so that a row-lock grant or a schema change that starts while
one runs waits for it. The storage tests run with `TARN_CHECK_YIELDS`, which
makes an atomic section that yields fail its step, and hold steps at their
yield points (`tarn_test_hook`) to run another step in between: two PREPAREs
of one row, a local commit and a PREPARE, an autocommit write and a row
lock, a local commit and a schema change.

**Autocommit reads.** A memtx scan and its W-lock check see one state
(SnapshotRead). A vinyl scan may yield, so it runs inside a read-only
transaction instead: MVCC gives it one snapshot, taken at some moment while
it ran. Its check then covers the W locks held now and every W lock taken or
released while it ran (a history the lock index keeps while such a read is
in progress). When none covered what the scan read, no prepared transaction
held a lock on those rows at any moment of the scan, so the snapshot is a
state in which SnapshotRead could have read the same rows; otherwise the
read waits for the lock and runs again. Checking the current locks only
would not do: a cross-shard transaction prepared and applied here during
the scan may have been read by another transaction that this read also
sees, while its write here is missing from the snapshot.

**Schema changes.** `ALTER TABLE` builds a vinyl table's new indexes before
it fences the table's writes (§2.10), so an `ADD INDEX` on a large table
does not hold its writes for hours. Checking existing rows (a narrower
type, `NOT NULL`) still happens behind the fence, and is refused above
`tarn.cfg.alter_check_rows` rows per shard.

**Performance.** Cached, vinyl tables served 73–89% of memtx's throughput
in sysbench. With data ten times the cache, point selects ran at 72% of the
cached rate and single-row writes kept theirs, at 2.9× and 4.4–6.2× TiDB's
with the same data ten times its block cache
([bench/RESULTS.md](../bench/RESULTS.md); data on tmpfs, so these leave out
disk latency). [Sizing](operations.md#sizing) gives the memory and disk a
vinyl shard needs.

**Not yet.** Replica bootstrap and backups copy a shard's whole data set
(issue #56). A table cannot move between engines (issue #57). Writes are
not slowed down when compaction falls behind (issue #58).

### 2.12 Bucket moves

A bucket is the unit that moves between shards (issue #24): to add a
shard, to remove one, or to take load off one. Every protocol above assumes
that a row never changes shards, so a move has to keep three things true. A
request routed with an old map must not be served by a shard the bucket has
left. A serializable transaction whose reads covered the bucket must not
commit across the move: a scan of every shard that read the old shard
before the move and the new one after would see the bucket's rows twice,
and in the other order not at all. And the locks on the bucket's rows --
prepared transactions' locks and READ COMMITTED row locks -- must not be
left behind on the old shard, where nothing would check them any more.

**The protocol.** One SQL server drives a move of bucket *b* from shard S
to shard R in steps:

1. *Copy*: the rows of *b* are copied to R while S keeps serving them; R
   keeps the copy aside, where nothing reads it.
2. *Fence*: from now on S refuses writes to rows of *b*, row locks on them,
   and PREPAREs that read or write them (read locks taken now would hold
   the move up); it breaks the row locks held on them, so that their
   transactions can no longer commit. R *holds* *b*: the validation of a
   read that could cover a row of *b* there fails. Then S waits until no
   prepared transaction holds a point lock on a row of *b* and no local
   commit that writes one is in flight, and the changes made since the
   copy are copied.
3. *Send*: S deletes the rows of *b*, and from then on refuses requests for
   it; their servers load the new map and route them again.
4. *Receive*: R inserts the copy, owns *b* and releases the hold.

Between the send and the receive the rows are on no shard: requests for
them are refused everywhere and retried until R has them. A move that
cannot drain in time is given up before the send, which releases the
fence and the hold.

**Why each step is needed.** Apart from where rows are, a move changes
nothing a transaction can see: it is a cross-shard transaction that
deletes the bucket's rows on S and inserts them on R. MVCC treats it as
one. A stream that read a row of *b* on S, or scanned a range of an index
there that held one, fails its validation once S deletes them, and one
that scanned a range of R that they fall in fails once R inserts them. The
hold covers the time in between: a transaction that scanned R before the
rows arrived and S after they left saw them nowhere, and its validation
on R comes after its scan of S, so after the send -- while R holds the
bucket, or after the insert. The fence and the drain keep the copy
current: a local commit in flight when the last copy is taken, or the
APPLY of a prepared transaction, would write a row on S after it was
copied, and the write would be lost. Point read locks are drained too: a
prepared transaction may already be committed, and its read locks protect
what it read until then, which after the move a writer on R would not
see. A scan's read locks (a range on every shard) are not waited for: the
scan read R as well, so it holds them there too, and both its PREPAREs
come before the send.

**The model.** TarnCommit.tla gives every row a place (`place`). A request
reaches the shard it was routed to, which refuses rows it does not own; a
read of a row where it no longer is finds nothing there (`Gone`), which
the read's validation catches as a changed read. Scans (`SSet`: a range
read on every shard, as a lookup by a non-unique secondary index or a full
scan) record what they saw on each shard, and `StrictSerializable`
requires a scan to have seen each row exactly once, with the value the
serial order gives it. The move's steps are `MoveFence` (the fence, the
broken row locks, the hold), `MoveDrained` (the drain and the last copy),
`MoveSendFlip`, `MoveRecvFlip` and `MoveAbort`; the copy before the fence
changes nothing a transaction can see. Two workloads move row x from shard
A to shard B: *MoveScan* (a scan of both shards; a blind write of x, a
local commit wherever x is, like an autocommit `UPDATE`; and a
read-modify-write of x and y, through PREPARE/APPLY before the move and a
local commit after it) and *MoveRC* (READ COMMITTED read-modify-writes of
x, one of them across shards, and a serializable one whose local commit may
be in flight when the move fences x). TLC finds a 9-step counterexample for
each switch:

| Switch | Counterexample |
|---|---|
| `BugMoveNoDrain` | a local commit that writes x is still in flight when the move takes its last copy: B gets the value before it, and a transaction that starts after the commit was acknowledged reads x without it |
| `BugMoveNoHold` | a scan reads B before x arrives and A after x left, validates both, and commits without x |
| `BugMoveServeStale` | a write of x routed with the old map reaches A after A sent the bucket; A applies it where no row lives any more and acknowledges it, and the write is lost |

Not in the model: READ COMMITTED reads of every shard, which are not
validated (they check instead that no shard has sent or received a bucket
since their server loaded its map), and direct key/value readers, who must
look a row up again where it went.

**The implementation** (`lua/tarn.lua` "Buckets" and "Bucket moves",
`crates/tarn-sql/src/buckets.rs`). A shard keeps each bucket it owns or is
moving in `_tarn_buckets`, in a state: *active*; *sending* (the copy runs
and the writes to its rows are noted); *fenced*; *sent*; *receiving* (the
copy goes to `_tarn_recv`, which nothing reads); *held*. An *epoch* in
`_tarn_meta` changes with every send and receive. A SQL server reads every
shard's part of the map (`tarn_buckets`: the state in memory that the
shard's checks go by, built once per change), checks with the verified
`build_owner` (§3) that no two shards claim a bucket, and routes by it.

Every request names what it was routed by: a write the bucket of each row
it writes (`wb`), a row lock the bucket of its row, a point or range read
the bucket its key fixes -- also in the read sets that the commit steps
check, since reads inside a stream are plain `SELECT`s -- and a read of
every shard outside a transaction (READ COMMITTED) the shard's epoch in
the server's map. A shard refuses (`moved`, 1213) a bucket it does not own,
writes and row locks of a fenced one, and a read of every shard whose epoch
is not its own; it also checks the `bucket_id` of every row it writes,
which refuses the writes of servers that name no buckets. The server reads
the map again, and a statement that can start over -- autocommit, or the
first statement of a transaction -- runs again with it (`StaleRetry`, as
for a stale table definition); a later statement fails with 1213 and the
transaction is rolled back. The checks run where the model puts them: in
each commit step's atomic section (§2.11), and for reads both before and
after the read, with no yield after it, since a read that waited -- for a
prepared transaction's lock, or for vinyl -- may have read after a move
went by. A send or a receive changes the state (and the epoch) in memory
inside its transaction, before box.commit() lets the deleted or inserted
rows be seen, and back if it rolls back; a receive owns the bucket only
once its rows are seen. The epochs of one scan of every shard come from one
copy of the map: each read passes only if its shard's buckets have not
changed since that copy was read, so together they read each bucket on the
one shard that owned it then (epochs from two copies could read a bucket
on the shard it left and, later, on the one it went to). While a bucket is
between its send and its receive, such scans are refused.

One SQL server drives a move (`TARN MOVE BUCKET`, operations guide) as a
job (§2.10), which schema changes and other moves of the bucket wait for:
`tarn_bucket_recv_begin` on R, `tarn_bucket_send_begin` on S (which waits
for the local commits in flight, whose writes the notes may have missed),
the copy a page at a time, `tarn_bucket_hold`, `tarn_bucket_fence` (which
breaks the row locks on the bucket's rows and waits for the drain), the
last copy (`tarn_bucket_delta`; after a leader change of S the notes are
gone, and the bucket is copied again), the job's phase `flip`, then
`tarn_bucket_send` and `tarn_bucket_recv`. Every step can be repeated, and
the steps that change a bucket's state run one at a time on a shard. A
server that finds the job abandoned finishes it (`ddl_job_stale`): forward
from `flip`, else back -- S first, which refuses to give the bucket back
once it has sent it (then R takes it over); only then R. A driver gives a
move up only when it knows the job is not at `flip`: when the reply to its
write of the phase is lost it reads the job again, and leaves it to a
takeover if it cannot. The job is kept alive through whichever node leads
the first shard.

A leader change (or a node that turns read-only and back) makes a shard
read its map again from `_tarn_buckets`. A step whose commit is still
waiting for the WAL or the quorum then has a state in memory that the
stored one does not show yet: a send has made the bucket 'sent' already
(`BK.early`), a last copy has added its rows to those the hold checks, and
a receive makes the bucket 'active' only once its rows are seen. So the
shard reads both the confirmed state and the one with the commits in
progress, and takes for each bucket the state that serves less, the newer
epoch, and the rows kept aside in either; steps also apply their state
again after their commit.

Testing found what the model could not. The recheck after a read compared
the reply's status with a name defined further down the module (a global,
nil), so it never refused, and READ COMMITTED scans that waited for a lock
while a bucket moved returned one of its rows twice, or none
(`movetest.py`, §4); the storage tests now hold such a scan through a move.
A review found two more: a shard that read its map again while a send or a
last copy waited for its quorum lost that step in memory (a read routed by
an old map then found the bucket's rows gone, and a scan of R missed a row
of the last copy), and a driver that gave a move up when the reply to its
write of `flip` was lost left a job that no takeover could finish. The
storage tests hold each step's commit at a quorum through a map read, and
`movefaults.sh` has a takeover find the move given up on S after `flip`.

### 2.13 Adding and removing shards

With buckets that move (§2.12), the size of a cluster is a matter of
membership: a shard that joins starts with no buckets and receives some by
moves; a shard that leaves first sends all of its buckets away. One more
thing has to hold (issue #25). A scan reads every shard its SQL server
knows, and §2.12 relies on that being every shard a bucket can be on: a
scan that reads the sender after a bucket left and never reads the
receiver misses the bucket's rows, and nothing it read changed. A
transaction routed before a shard joined does not know that shard.

**The protocol.** Every shard keeps a membership version, and adding a
shard raises it on every shard before any bucket may move to the new one.
A transaction that scanned names the version its server routed it with in
its commit requests -- a validation, a PREPARE, a local commit -- and in
its read when it reads a single shard in one call; a shard that has a newer
version refuses it ('moved', 1213), and its server loads the membership and
runs the statement again, or rolls the transaction back. A transaction
routed with the old membership that scanned the sender before the bucket
left fails its validation there (MVCC, §2.12); one that scanned it after
can commit only once every shard has the new version, which then refuses
it. Removing a shard needs no check: a shard leaves once it owns no bucket
and receives none, and a scan that still reads it finds nothing there.

**The model.** With `MJoin`, B joins the cluster -- `MemberJoin` raises
`mver` on each shard, in any order -- and x moves from A to B once every
shard has the new version. `OldTxns` are transactions routed before B
joined; their scans read every shard but B (`KnownShards`), and
`MemberOK` is the check above. *JoinScan* is MoveScan's workload with its
scan routed before B joined, on two shards (every row starts on A, so the
scan is one read of A: `Join_Scan`, `Join_ScanFailover`) and on three (y
starts on C, so the scan reads A and C in streams and validates or prepares
there: `Join3_Scan`, `Join3_ScanPrepare`). Without the check
(`BugJoinNoCheck`) TLC finds a scan that commits without x: in 7 steps on
two shards (B joins, x moves, the scan reads A after x left), in 13 on
three (the scan reads C, B joins, x moves, the scan reads A and validates
on both, or prepares on both: `Bug_JoinNoCheck3`,
`Bug_JoinNoCheck3Prepare`).

**In Tarn.** Every shard keeps the membership in `_tarn_meta` ('members':
a version and the shards with their replicas; without the record, version 0
and the shards of the SQL servers' configuration). `TARN ADD SHARD` is a
job of kind `members`, which waits for every other job, as they wait for it
(`tarn_job_begin`). It checks the new node -- its shard id, no buckets, no
tables but the cluster's and no rows in them, so that a run that stopped
can run again -- copies every table from the first shard as it is there
(`tarn_schema_export`, `tarn_schema_import`: space ids, formats, indexes,
global index spaces, and the catalog entries the definition fences come
from), gives it a bucket map with no buckets, records phase `apply` (from
there a server that finds the job abandoned finishes it forward), and
writes the new membership on every shard (`tarn_members_set` only raises
the version) before the job ends; a move to the new shard is a job too, so
it begins once every shard has the new version, as the model's moves do.
A transaction takes its server's version when it begins (`Txn::mver`): a
server adds a joining shard to its list before it takes the shard's
version, and a scan reads every shard on the list, so the transaction's
scans read at least that membership's shards. Its commit requests name the
version when it read every shard (a read routed by no bucket), and a read
of every shard outside a transaction names its map's version next to the
epoch (`{e, m}`, §2.12); `BK.member_refused` refuses an older one. A
server learns a newer membership from the version in each shard's part of
the bucket map, which it reads again after a refusal, from the first shard
when it begins a job (a job acts on every shard), and when it starts; the
shards find the replicas of a shard that joined in the membership too
(recovery). A shard's list position never changes while a server runs
(`ShardList`), so the positions in transactions, maps and lock requests
stay valid as shards join.

**Removing a shard.** The scans need no check (above), but a shard that
leaves must not be needed to finish a transaction: a participant prepared
with it as its decision shard asks it for the outcome when the coordinator
stops, and would wait forever for a shard that is gone. So `TARN REMOVE
SHARD` first marks the shard *draining* (a `members` job, as above), and
from then on a participant refuses a PREPARE whose decision shard is not
active (in the PREPARE's atomic section, with the 'moved' status that makes
its server read the membership again; a coordinator takes the lowest
active participant that writes as its decision shard). Its buckets then
move to the active shards (a move to a shard that is not active is refused
under the move's job), and it waits until no shard counts a PREPARE in
progress or a prepared participant naming the shard as its decision shard,
nor the shard a decision record in progress (`tarn_shard_records`;
`BK.prep` counts a PREPARE from before its check until its records are
written, so that none slips past the count). Then a `members` job marks it
*removed*. SQL servers leave removed shards out of reads of every shard,
schema changes and map reads; one that cannot reach a shard reads the
membership, and runs the statement again if it changed; the shards'
garbage collection takes a removed shard's records as gone.

**Rebalancing.** `TARN REBALANCE` makes moves of §2.12, so it needs no new
protocol: what it adds is the choice of moves and their pacing. One runs at
a time: its record on the first shard (`tarn_rebalance_get`/`_put`) holds
its status, its limits and the moves of its current round, and is written
back only at the version it was read at -- by the server running it, by
`PAUSE`, `RESUME` and `CANCEL`, and by a server taking it over once it was
not written for `ddl_job_stale` -- so that none of them undoes another's
change. A move starts once the record says so, and a move that a server
taken over from had started is made again (a no-op once the bucket is
there, and one that waits for the old move's job to be finished first).
Rounds of up to 64 moves are planned from each bucket's bytes (the shards
scan their tables' `bucket_id` indexes: `tarn_bucket_sizes`) or its
requests (reads routed by the bucket and rows written to it, counted in
memory over the last one to two windows: `tarn_bucket_stats`). The server
running the rebalance reads every shard's numbers together and keeps them,
a bucket's numbers moving with it, until it reads them all again: numbers
of different times would make the shards that received buckets look
heavier (the rows written since counted) or, by requests, lighter (a
shard counts a bucket's requests from when it arrived). Each bucket also
weighs the mean of all of them, and each move
takes, from the heaviest active shard to the lightest, the bucket closest
to that mean weight, or to half their difference once that is less: moving
the heaviest buckets first would leave the shards they go to with fewer of
them, and the rows written later -- spread over the buckets by their hash
-- would drift away from the balance. A schema change or a change of the
shards that waits for moves holds new moves back (`BK.want`); without that,
a stream of moves would keep it waiting.

## 3. Verus

`crates/tarn-verified` is checked with Verus (`verus --crate-type=lib
src/lib.rs`: **66 verified, 0 errors**) and compiled into the server as plain
Rust. What is proven:

* **Routing** (`route.rs`). `crc32c_update` equals a bitwise CRC-32C
  specification (`crc_bytes`) for any input; `bucket_from_crc` returns a
  bucket in `1..=bucket_count`; `shard_of_bucket` returns a shard in
  `0..nshards`, is monotone in the bucket, and every shard owns at least one
  bucket (the map is a total, onto partition): the layout of a new
  cluster. `build_owner` turns the bucket ranges the shards report owning
  into the owner of each bucket: it is proven to give every bucket a range
  holds that range's shard, and every other bucket `NO_SHARD`, or to refuse
  the reports when two ranges share a bucket or one is out of range. The
  SQL layer, the KV benchmark and the bucket ids stored in tuples all use
  these functions.
* **Coordinator logic** (`coord.rs`). `decide(votes)` returns COMMIT iff
  every vote is YES and ABORT iff some vote is NO (the TLA+ `CoordDecide`).
  `resolve_step(record, participant states, expired)` is one step of
  recovery (`RecoverFenceRecord`, `RecoverFenceParticipant`,
  `RecoverDecide`): it is proven to decide COMMIT only when the states it is
  given satisfy `CommittedNow`, to decide ABORT only when some participant is
  tombstoned, to fence only participants without a record while the record
  is STAGING, and to fence a missing record only before expiry or while some
  participant is prepared (otherwise it returns `Gone`). This is a case
  analysis of one snapshot of states. That "no participant record" means
  "never prepared" and "no record" means "never created" are properties of
  the whole protocol (the garbage-collection rules of §2.6), which TLC checks
  in the `*_GC` configurations, not Verus. The coordinator in `txn.rs` calls
  both functions; the storage-side recovery fiber in Lua implements the same
  TLA+ actions and is covered by tests, not by proof.
* **MySQL wire codec** (`wire.rs`). Length-encoded integers and strings:
  encoding matches a byte-level spec, decoding inverts it and never reads out
  of bounds; packet framing: `frame_packets` produces exactly the MySQL
  multi-packet format (payloads ≥ 16 MiB split, empty terminator packet when
  the length is a multiple of 0xFFFFFF, sequence ids wrapping), and
  `deframe` is proven to return precisely the payload that `frames_spec`
  framed, or `None` while incomplete. No overflow or panic on any input.
  `tarn-mysql::codec` delegates to these functions.
* **Global index maintenance** (`uix.rs`, §1.4). `index_delta` decides
  which entry writes a row write makes. It is proven that applying them to
  an index that holds the row's old entry leaves the other rows' entries
  alone and holds its new entry instead (none when the row is deleted or its
  key has a NULL part), and that a write that keeps the key writes no entry.
  That the new entry's key is free is the commit protocol's job, which TLC
  checks (`Uniq*`, §2.2).

Not covered by the proofs: how SQL values are turned into the bytes that
are hashed for routing (`tarn-sql/src/route.rs`, tested against Tarantool's
own `digest.crc32` instead), and packet sequence numbers, which `deframe`
does not check.

## 4. Tests

| Suite | Scope | Result |
|---|---|---|
| `lua/test/run.sh` | storage protocol on two real shards: commit paths, locks, PREPARE/APPLY/ABORT, tombstones, fencing, recovery, lock-aware reads; APPLY outcomes, expiry, garbage-collection rules, refusal on demoted nodes and after an election term change, undo of a failed PREPARE's locks, lock-index rebuild while a PREPARE waits for a quorum; multi-key reads, integer arithmetic limits; row locks (queues, upgrades, timeouts, NOWAIT, SKIP LOCKED, local deadlocks, the cross-shard detector's interface with the shards each waiter holds locks on, the generation a failed request reports for the locks it keeps, lost generations and connections, RC commits, unique-value locks, locking reads of a primary-key range in one call); schema changes (definition fences on every write path, waiting for prepared transactions, row checks with fill values, temporary indexes, the job records, two jobs beginning at once); APPLY after a table was dropped; vinyl tables: each commit step held at its yield points while another runs, autocommit reads that meet W locks taken while they scan; index builds ahead of the fence (refused writes, an APPLY that a new index refuses), the row-check limit; jobs (per table name, phases, takeovers); `CREATE TABLE`, `DROP TABLE` and `TRUNCATE` in steps: a `TRUNCATE` that waits for a prepared transaction and then removes its rows, reads and row locks waiting on a held table, validation failing on it, holds that outlive a rebuild from storage, a vinyl reader whose validation fails after a `TRUNCATE`; bucket moves (`bucket_test.lua`): the map, requests refused for buckets the shard does not own on every read, write, lock and commit path (also writes that name no bucket, by their rows' `bucket_id`), a move step by step (copy in pages, writes during the copy in the last copy, the hold failing scans of the receiving shard, the fence refusing writes, row locks and PREPAREs but serving reads, send and receive, repeated steps), the fence breaking row locks and waiting for prepared transactions (also after a leader change, from the lock records) and for a local commit in flight, notes lost to a leader change and the copy made again, giving a move up, an APPLY during a move, a read of every shard that waits for a lock while a bucket is sent, moves and schema changes waiting for each other, the map read again while a send, a last copy or a receive waits for its quorum, two servers making a shard's map at once with different counts | 392/392 and 138/138 |
| `cargo test` (tarn-verified, tarn-mysql, tarn-sql) | codec boundaries, framing at 16 MiB edges, packet size limits, CRC vs Tarantool, coordinator decisions, binary/text row encoding, parameter decoding (all types, truncated input), SQL normalization, expressions, collation weights against MariaDB's, weight fields and the `ALTER TABLE` rules for them, index use under a collation, the cross-shard deadlock search (walks over simulated shards, cycles, the second collection), the bucket map from the shards' reports (a bucket between send and receive, overlapping and inconsistent reports) and the bucket count of a new map; the rebalancer's plans (even weights, and even shares of buckets of uneven sizes) and its record | all pass |
| `crates/tarn-iproto/tests` | real Tarantool: auth, iterators, DML, call/eval, streams and conflicts, 20k pipelined requests, connection loss, stale-leader detection (ER_READONLY), MP_DECIMAL written by Tarantool | 30 tests pass |
| `tests/sql/difftest.py` | 465 SQL cases run on Tarn and MariaDB 10.11 and compared exactly: column names, rows (integers and DECIMALs by exact value, DOUBLEs within 1e-9), error codes, affected rows, final table contents and indexes; access paths, expressions, aggregates, joins, subqueries, UNION, DML, integer arithmetic, unique keys, isolation variables, locking reads, savepoints and statement rollback, ALTER TABLE (adds, drops, type changes and their checks, renames, defaults, indexes, foreign keys), RENAME TABLE, user and system variables, executable comments, errors; tables without a primary key, `information_schema.TABLES`, `SELECT ... INTO @var`, SQL-level `PREPARE` / `EXECUTE`, `CLIENT_FOUND_ROWS`, row-value comparisons, `DATETIME` spellings; collations (98 cases): `=`, `IN`, `LIKE`, ranges and lookups through case-insensitive indexes, `ORDER BY`, `GROUP BY`, `DISTINCT`, `UNION`, joins, unique and primary keys that refuse case and accent variants, upserts, coercibility and 1267, `WEIGHT_STRING()` of every kind of character, case-insensitive keys in transactions and at READ COMMITTED, `ALTER TABLE` with collations, case-insensitive keys of tables without a primary key, keyset cursors over case-insensitive keys | 465/465 identical |
| `tests/sql/fuzzdiff.py` | randomly generated SELECTs (predicates with NULLs/IN/BETWEEN/LIKE, arithmetic, aggregates, GROUP BY/HAVING, ORDER BY+LIMIT, joins, subqueries) on Tarn and MariaDB, rows compared as above | 2,000 queries, 0 mismatches |
| `tests/sql/txntest.py` | rollback, visibility, write skew, lost update, phantom, fractured read, real-time order, FOR UPDATE, duplicate keys in transactions, cross-shard atomic DML, direct KV reads; concurrent cross-shard inserts of case variants of one case-insensitive key (serializable and READ COMMITTED: exactly one commits); concurrent bank transfers with serializable audits, a ledger written in the same transactions (lost or duplicated transfer writes, acknowledged commits missing, commits reported as 1213 but applied) and autocommit counters (increments applied twice) | 13/13 |
| `tests/sql/rctest.py` | READ COMMITTED against InnoDB's behaviour: isolation variables, lock waits and timeouts, local and cross-shard deadlocks (also between transactions on two SQL servers, and a cycle of three), no deadlock-detector calls without lock waits, locks of a failed lock request released with its transaction, 8 threads decrementing counters through autocommit, local and cross-shard transactions (no lost update), semi-consistent UPDATE/DELETE, SKIP LOCKED job queue (every job claimed once), a UNION ALL of SKIP LOCKED reservations (every unit free or held once while carts reserve and release), NOWAIT, FOR SHARE, duplicate inserts, concurrent upserts (also with MySQL 8's row alias) and unique-key updates, savepoints, statement rollback, locks released on disconnect, serializable transactions meeting row locks, primary-key point writes locked and read in one step, and 8 s of concurrent transfers checked against a ledger (also run during leader failovers by `tests/failover.sh`) | 28/28 |
| `tests/sql/ddltest.py` | a mysqldump file loaded with the `mysql` client; multi-statement batches and `COM_SET_OPTION`; refused alterations; 20 s of writes on two SQL servers (fast-path and read-modify-write autocommits, transfers, READ COMMITTED transactions, readers) while the first server alters the table about 300 times: no error reaches an autocommit statement, no acknowledged increment is lost or duplicated; schema changes from six connections at once; transactions that wrote a table before an ALTER, at both isolation levels (1412, rolled back); tables created through one server used at once through the other while schema changes reload the catalog; writes going on while `ADD INDEX` builds on a vinyl table of 300,000 rows | 8/8 |
| `tests/sql/movetest.py` | bucket moves under load: two SQL servers run serializable transfers with a ledger, READ COMMITTED read-modify-writes and autocommit increments of counters, serializable audits and READ COMMITTED reads of every shard, while one of them moves the buckets of those rows back and forth: no transfer lost or applied twice, no increment lost, audits exact, every READ COMMITTED scan sees each row once | about 190 moves in 30 s, every check holds; also on vinyl tables read from disk |
| `tests/movefaults.sh` | the SQL server driving a move killed while it copies, while its fence drains, after its flip before the send, and after the send before the receive, and after its flip with the move given up on the sending shard (test hooks hold the steps; the server is started again after each); another server finishes the job (`ddl_job_stale` = 2 s) | 41/41: given up in the first two cases and the last, finished in the third and fourth, each within 3 s; rows checked through SQL and on the shards |
| `tests/failover.sh` | 2 shards × 3 synchronous replicas under the bank workload, READ COMMITTED transfers and `movetest.py` (buckets moving through the leader changes): leadership of shard 1 moved with `box.ctl.promote()` (the old leader keeps its connections), then kill -9 of the leader of shard 1, then of shard 2; with memtx tables, and with vinyl tables (`ENGINE=vinyl`) | passes on both: new leaders in ≈2.5 s, all workload checks hold, final sums exact via SQL and via direct KV reads on the new leaders |
| `tests/bucketcount.sh` | the number of buckets a SQL server gives the map it makes: the configured `bucket_count`; 3,000 on shards that have tables but no map (a cluster made before bucket moves, simulated by removing the map and restarting the nodes); 30,000 on a new cluster; each time the shards' ranges and, with rows, their bucket ids, reads by key and writes | 20/20 |
| `tests/ddlfaults.sh` | `CREATE TABLE`, `DROP TABLE` and `TRUNCATE` with shard 2 down, and with shard 2 killed while a test hook holds its APPLY; the abandoned jobs are finished by the SQL server (`ddl_job_stale` = 2 s) | 6/6: rolled back with the shard down (the table on no shard, or unchanged), rolled forward when killed while applying; checked on each shard directly |
| `tests/faults.sh` | a proxy delivers an autocommit `UPDATE ... SET v = v + 1` to the shard, swallows the reply and closes the connection; on a memtx and on a vinyl table | the client gets 1180 (outcome unknown), the row is incremented exactly once |

## 5. Results

### 5.1 Model checking

Every configuration of `tla/MC.tla` and its result (`python3 tla/summarize.py`
regenerates this table from `tla/out`). Configurations named `*_ROValidate`
follow the implementation's commit paths (cross-shard read-only transactions
commit by validation without PREPARE; the decision shard's APPLY records
COMMITTED); `*_GC` configurations add expiry and garbage collection of
records; the others model the PREPARE-based variant with the coordinator
recording COMMITTED, and keep records forever. `Bug_*` configurations switch
off one protection each and must fail; `Tradeoff_*` check documented weaker
guarantees and must fail. Each workload has three transactions on two
shards (three for `Join3_*`) with one or two faults, so these are bounded
checks: small, but exhaustive within those bounds. Two caveats. `Skew_GC_ROValidate`, the
largest workload with garbage collection, a crash and a lost message, did
not finish in the time available: TLC explored 47.8 million distinct states,
breadth-first into depth 23, without finding a violation;
`SkewTimeout_GC_ROValidate` checks the same workload exhaustively with
recovery racing the coordinator but without crashes or message loss. And
`Skew`, `SkewFailover` and `Skew_AckOnPrepare` (up to an hour each) were
last checked before the GC, ApplyMarksCommitted and READ COMMITTED
extensions of the spec existed, and the other configurations that
`tla/check.sh` leaves out (`Frac`, `Local`, their `Failover` variants and
the other `Skew*`) before bucket moves and joining shards: with those
options off and no READ COMMITTED transactions, moves or joins, the new
actions are disabled and the new variables stay constant, so the behaviour
is unchanged, as re-running the
serializable configurations of `tla/check.sh` with the final spec confirms
(the same numbers of distinct states as before each extension).

<!-- TLC -->
| Configuration | Result | Distinct states | States generated | Depth | Time | Trace length |
|---|---|---:|---:|---:|---|---:|
| Bug_FenceAfterExpiry | violates NoAbortAfterCommit | 113180 | 569134 | 14 | 14s | 14 |
| Bug_GCIgnoresDecision | violates NoAbortAfterCommit | 24653 | 94588 | 12 | 05s | 11 |
| Bug_GCNoExpiry | violates NoAbortAfterCommit | 2594 | 7212 | 9 | 02s | 9 |
| Bug_GroupedReadSeparateSnapshots | violates CoherentSnapshot | 18 | 18 | 6 | 00s | 6 |
| Bug_GroupedTablesNoWLockCheck | violates StrictSerializable | 1194 | 5093 | 13 | 00s | 13 |
| Bug_IgnorePLocks | violates StrictSerializable | 3213 | 10214 | 11 | 02s | 11 |
| Bug_InsertNoExistCheck | violates UniqueHolds | 119 | 196 | 6 | 01s | 6 |
| Bug_JoinNoCheck | violates StrictSerializable | 993 | 2673 | 8 | 01s | 8 |
| Bug_JoinNoCheck3 | violates StrictSerializable | 30142 | 129908 | 13 | 07s | 13 |
| Bug_JoinNoCheck3Prepare | violates StrictSerializable | 44422 | 199259 | 13 | 07s | 13 |
| Bug_LocalIgnoresRLocks | violates StrictSerializable | 32672 | 133596 | 13 | 06s | 13 |
| Bug_LocalReadsIgnoreWLocks | violates StrictSerializable | 3653 | 10348 | 10 | 02s | 10 |
| Bug_MoveNoDrain | violates StrictSerializable | 4650 | 13078 | 10 | 03s | 9 |
| Bug_MoveNoHold | violates StrictSerializable | 4619 | 12922 | 10 | 02s | 9 |
| Bug_MoveServeStale | violates StrictSerializable | 5787 | 15518 | 11 | 03s | 9 |
| Bug_NoReadLocks | violates StrictSerializable | 1893 | 6394 | 9 | 02s | 9 |
| Bug_NoTombstone | violates AppliedOnlyIfCommitted | 6397 | 17927 | 9 | 03s | 9 |
| Bug_NoWLockCheckOnRead | violates StrictSerializable | 12006 | 44352 | 11 | 04s | 11 |
| Bug_RCLockIgnoresWLocks | violates StrictSerializable | 3223 | 10869 | 11 | 02s | 11 |
| Bug_RCLockReadsInFlight | violates StrictSerializable | 84 | 160 | 5 | 01s | 5 |
| Bug_RCNoLockCheck | violates StrictSerializable | 27837 | 84090 | 11 | 05s | 10 |
| Bug_RCReadBeforeLock | violates StrictSerializable | 2543 | 7817 | 11 | 02s | 11 |
| Bug_ROValidateNoReadCheck | violates StrictSerializable | 18873 | 69552 | 12 | 05s | 12 |
| Bug_ROValidateNoWLockCheck | violates StrictSerializable | 10550 | 37003 | 11 | 04s | 11 |
| Frac | no errors | 1182650 | 9429589 | 23 | 02min 33s |  |
| FracFailover | no errors | 2715768 | 21085027 | 25 | 10min 51s |  |
| FracFailover_ROValidate | no errors | 518625 | 2830211 | 21 | 01min 08s |  |
| Frac_GC_ROValidate | no errors | 1846859 | 13211705 | 25 | 04min 10s |  |
| Frac_ROValidate | no errors | 95786 | 555473 | 19 | 16s |  |
| GroupedRead | no errors | 15 | 17 | 6 | 00s |  |
| GroupedTables | no errors | 1909 | 10272 | 17 | 00s |  |
| Join3_Scan | no errors | 3633874 | 25756506 | 30 | 12min 05s |  |
| Join3_ScanPrepare | no errors | 1975971 | 16369248 | 33 | 09min 14s |  |
| Join_Scan | no errors | 83565 | 431359 | 24 | 13s |  |
| Join_ScanFailover | no errors | 56937 | 238702 | 24 | 09s |  |
| Local | no errors | 710975 | 5545010 | 23 | 01min 30s |  |
| LocalFailover | no errors | 1881011 | 14437776 | 25 | 04min 26s |  |
| LocalFailover_ROValidate | no errors | 397911 | 2148183 | 21 | 56s |  |
| Local_GC_ROValidate | no errors | 1207771 | 8610883 | 25 | 02min 47s |  |
| Local_ROValidate | no errors | 66348 | 377760 | 19 | 12s |  |
| Move_RC | no errors | 1450427 | 10618984 | 29 | 03min 17s |  |
| Move_Scan | no errors | 1471152 | 9238538 | 25 | 03min 36s |  |
| Move_ScanFailover | no errors | 853724 | 4772626 | 25 | 01min 59s |  |
| Move_ScanPrepare | no errors | 20568816 | 174490000 | 29 | 48min 06s |  |
| RC_Counter | no errors | 2164653 | 17141464 | 26 | 05min 07s |  |
| RC_CounterFailover | no errors | 1153174 | 8224685 | 26 | 03min 15s |  |
| RC_Counter_AckOnPrepare | no errors | 1180127 | 8944009 | 26 | 04min 27s |  |
| RC_Local | no errors | 50606 | 277842 | 19 | 09s |  |
| RC_Mixed | no errors | 171352 | 1029336 | 20 | 27s |  |
| RC_MixedFailover | no errors | 1219288 | 6901169 | 22 | 03min 56s |  |
| Recov | no errors | 71927 | 412881 | 18 | 11s |  |
| RecovReaders | no errors | 28258 | 135451 | 17 | 06s |  |
| Recov_AckOnPrepare | no errors | 83195 | 475418 | 18 | 14s |  |
| Recov_GC | no errors | 1136892 | 8123071 | 23 | 02min 26s |  |
| Skew | no errors | 12526098 | 130350710 | 30 | 25min 47s |  |
| SkewFailover | no errors | 15518506 | 158998997 | 32 | 30min 08s |  |
| SkewFailover_ROValidate | no errors | 4325065 | 34755383 | 28 | 10min 18s |  |
| SkewTimeout_GC_ROValidate | no errors | 4509135 | 40415963 | 34 | 14min 39s |  |
| Skew_AckOnPrepare | no errors | 26198655 | 277233192 | 30 | 01h 03min |  |
| Skew_GC_ROValidate | stopped at the time limit, no violation so far | 47804036 | 411580777 | ≥23 | - |  |
| Skew_ROValidate | no errors | 1944298 | 16444418 | 26 | 04min 27s |  |
| Tradeoff_AckOnPrepare_DirectRYW | violates DirectReadYourWrites | 598 | 1108 | 7 | 01s | 7 |
| Tradeoff_DirectReadAtomic | violates DirectReadAtomic | 3110 | 9575 | 9 | 02s | 9 |
| Uniq | no errors | 657499 | 5210697 | 22 | 01min 29s |  |
| UniqFailover | no errors | 1444398 | 11344964 | 24 | 03min 55s |  |
| Uniq_RC | no errors | 966902 | 7892907 | 23 | 02min 16s |  |

### 5.2 Benchmarks

See [bench/RESULTS.md](../bench/RESULTS.md) for the full tables, setup and
discussion. In short, on this 2-vCPU machine (load generator included):

<!-- BENCH -->
| Best throughput over 1/16/64 threads | MariaDB 10.11 | MySQL 9.7 | Tarn 2 shards | Tarn 1×3 sync replicas |
|---|---:|---:|---:|---:|
| oltp_point_select (QPS) | 46,391 | 26,442 | 29,711 | 32,389 |
| oltp_read_only (TPS) | 1,981 | 1,000 | 619 | – |
| oltp_read_write (TPS) | 1,389 | 726 | 356 | 495 |
| oltp_write_only (TPS) | 6,557 | 3,459 | 1,507 | – |
| oltp_update_index (TPS) | 28,197 | 13,237 | 17,617 | 10,231 |
| oltp_insert (TPS) | 30,020 | 17,493 | 16,646 | 11,566 |
| direct KV point reads (ops/s) | – | – | 259,797 | – |

## 6. Limitations

* SQL coverage is "sysbench-level plus": uncorrelated subqueries (`IN`,
  `EXISTS`, scalar) and `UNION [ALL]` work (the inner SELECTs run first,
  inside the statement's transaction); not supported are correlated
  subqueries, derived tables, views, stored procedures, triggers, enforced
  foreign keys, window functions, RIGHT/FULL joins, `information_schema`.
  Joins are nested-loop with index lookups; the lookups of a batch of up
  to 256 outer rows are read together, with one call per shard, and each
  key enters the read set as a separate lookup's would. Lookup parts of
  type DECIMAL, and global lookups with DECIMAL primary keys, retain per-row
  native lookups until exact batch matching supports them.
* `ALTER TABLE` does not rewrite rows: conversions between type families
  and `DECIMAL` scale changes are refused, dropped columns keep their
  stored values (no space is reclaimed), and writes to the table wait while
  a change prepares (§2.10): on memtx tables also while their new indexes
  are built, at memory speed; vinyl tables build them before unless they
  need a physical-field presence check for a non-NULL fill. Checking the
  existing rows (a narrower type, `NOT NULL`, indexed field presence) is refused above
  `tarn.cfg.alter_check_rows` rows per shard. A table cannot move between
  memtx and vinyl.
* Vinyl tables (§2.11): an autocommit read whose range a cross-shard
  transaction writes again and again while it scans runs again each time,
  and fails with 1213 after `lock_wait` (1 s); benchmarks with data larger
  than the cache are still to come (#23).
* Collations (§1.3): the server's default is `utf8mb4_bin`, which counts
  trailing spaces, unless `--default-collation` names another; collations
  other than `utf8mb4_bin`, `utf8mb4_general_ci` and `utf8mb4_unicode_ci`
  (and their NO PAD forms) compare like the closest of those. An index
  cannot be added on an existing case-insensitive column that is in no
  index yet, nor an indexed column's collation changed (1235), and an
  indexed case-insensitive column is stored twice (text and weights).
* Hash sharding: range predicates on the shard key and lookups through
  non-unique secondary indexes go to every shard.
* Global unique indexes (§1.4): writes to their tables touch the entries'
  shards too; adding one to a table with rows holds up the table's writes
  while it is built, and one cannot be renamed or changed; direct Tarantool
  writes do not maintain them.
* Serializable OCC: long read-only transactions that span shards can starve
  under heavy concurrent writes to what they read (they abort and retry);
  explicit transactions surface 1213 to the client like any serializable
  database. Sessions that need InnoDB's behaviour instead can use READ
  COMMITTED.
* READ COMMITTED: row locks are kept in the shard leaders' memory, so a
  leader change, or a lost connection between an SQL server and a shard,
  rolls back the transactions holding locks there (1213). There are no gap
  or next-key locks (as in InnoDB at READ COMMITTED). A serializable
  autocommit statement that must read before writing does not wait for a
  row lock: it retries briefly and then fails with 1213. Waits on prepared
  cross-shard transactions' W locks are bounded by timeouts but are not part
  of the deadlock detector's graph. A row lock costs about 400 bytes of
  Lua memory on the shard leader (InnoDB keeps a bit per row in a page
  bitmap), so a transaction that locks millions of rows should be split, as
  its write buffer in the SQL server already requires.
* Serializable transactions must commit within `max_txn_age` (120 s) of
  their first statement (stream transactions already time out after 60 s;
  READ COMMITTED transactions have no limit); protocol
  records are kept for about 3 minutes, which at several thousand
  cross-shard commits per second costs on the order of 100 MB of memtx
  memory per shard.
* A commit or autocommit write interrupted by a lost connection is reported
  as "outcome unknown" (1180/40003), as in other distributed databases; the
  client has to check.
* Timing assumptions (§2.6): clocks within 60 s of each other, and a leader
  lease bounded by Tarantool's election timeout for autocommit reads on a
  partitioned old leader.
* Write paths read in `read-committed` isolation, so in a replicated shard a
  write can fail with a duplicate key (1062) because of an insert that is
  not yet confirmed by the quorum and is later rolled back by a failover (a
  spurious error, not an inconsistency).
* AUTO_INCREMENT values are allocated in blocks per SQL server: unique and
  increasing per server, with gaps across servers (as in TiDB). An explicit id
  inserted through one server inside another server's cached block is not
  detected until that server reaches it (duplicate-key error then).
* DDL is not transactional (as in MySQL). Each `CREATE TABLE`, `DROP
  TABLE` and `TRUNCATE` is atomic across shards (§2.10), but `DROP DATABASE`
  drops its tables one by one, and one that fails part way leaves the tables
  it had not reached. While a `DROP TABLE` or `TRUNCATE` waits for the
  prepared transactions holding locks on the table, autocommit reads of the
  table wait too, and fail with 1213 after `lock_wait` (1 s), where MySQL's
  metadata lock makes them wait up to `lock_wait_timeout`. `TRUNCATE` does
  not reset `AUTO_INCREMENT`.
* Bucket moves (§2.12): one bucket at a time per move, and writes to the
  bucket's rows wait from its fence to its send (refused and retried; the
  time to drain the prepared transactions and copy the last writes); a
  transaction that used the bucket while it moved is rolled back with 1213,
  as after a leader change. The number of buckets is fixed when a cluster
  is made. Every SQL server must run a version with bucket moves (an older
  one names no buckets: its writes are refused by their rows' `bucket_id`,
  but its reads are not checked) and have every shard in its
  configuration before buckets move. vshard compatibility is at the level
  of tuple layout and bucket hashing; the map is Tarn's own.
* No TLS, no `COM_CHANGE_USER`; users and passwords come from the command
  line.
