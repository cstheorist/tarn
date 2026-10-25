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
`README.md` covers building and running.

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
 │ (lua/tarn)  │ │ (lua/tarn)  │   memtx engine with MVCC
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
  last: the layout vshard and crud expect.
* Rows are placed by a shard key (default: the primary key; `PARTITION BY
  KEY(cols)` chooses others). `bucket_id = crc32c(key) % 3000 + 1` using
  vshard's `mpcrc32` definition (Tarantool's `digest.crc32`, strings hashed as
  raw bytes, other values as their msgpack encoding). Buckets map to shards in
  equal contiguous ranges.
* As in MySQL's partitioned tables, every unique index must contain the shard
  key, so uniqueness is enforced by one shard.
* The table definitions live in `_tarn_catalog` on every shard; SQL servers
  cache them and reload on a catalog version bump.

Direct reads: a Tarantool client computes the bucket from the key (or simply
asks each shard) and does `space.index.primary:get{key}`. The `kvbench`
example does exactly that with the same Verus-verified routing code.

### 1.2 Request paths

**Autocommit point read** (e.g. `SELECT c FROM t WHERE id = ?`): one IPROTO
call to the owning shard's leader, `tarn_select` with a lock check. The scan
and the check run without yielding, so they observe one state; if a prepared
cross-shard transaction holds a W lock on the row, the read waits until it is
applied (this is what makes autocommit reads strictly serializable, see §2.3).

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
  client is acknowledged after every APPLY (or, with `--ack-on-prepare`,
  right after the PREPAREs; see §2.7).
* *Recovery:* any storage node's recovery fiber (and any SQL server) can
  resolve a transaction whose coordinator vanished: fence the record (no
  record yet → ABORTED, refusing a late PREPARE), fence participants that
  never prepared with tombstones, then decide from the durable state and
  apply or abort everywhere. Recovery may race a slow-but-alive coordinator.

Conflicts surface as MySQL error 1213 (`ER_LOCK_DEADLOCK`, SQLSTATE 40001),
which clients already treat as "retry the transaction". Autocommit statements
are retried transparently (up to 20 times with backoff).

## 2. Isolation and the commit protocol

### 2.1 Guarantees

* SQL transactions (explicit or implicit, single- or cross-shard) are
  **strictly serializable**: there is a serial order consistent with real time.
* Direct Key/Value readers see only committed data (`read-confirmed`
  isolation: in replicated shards, only data acknowledged by a quorum), and a
  direct read issued after an SQL COMMIT returned sees that commit.
* Documented trade-offs (checked by TLC as *expected* violations): a direct
  reader that reads two rows of a cross-shard transaction on two different
  shards may see one applied and the other not yet (`Tradeoff_DirectReadAtomic`;
  SQL readers never can), and with `--ack-on-prepare` a direct read right
  after COMMIT may not see the commit yet (`Tradeoff_AckOnPrepare_DirectRYW`).

### 2.2 The TLA+ model (`tla/TarnCommit.tla`)

The model has shards, keys with owners, three transactions with fixed
read/write sets, direct (non-SQL) readers, and the actions of the protocol:
reads, single-shard commit, snapshot reads, read-only validation, PREPARE
(with the decision record), coordinator decision/finish/abort, APPLY, ABORT
(with tombstones), timeout-triggered recovery (fence record, fence
participants, decide), re-delivered PREPAREs, and faults: coordinator crashes,
message loss, and **shard leader failover**, which keeps durable state
(quorum-replicated) but destroys the MVCC read tracking of streams still
executing. Local Tarantool transactions are atomic steps.

Invariants checked in every configuration:

| Invariant | Meaning |
|---|---|
| `TypeOK` | well-formed state |
| `AppliedOnlyIfCommitted` | no participant applies a transaction that is not committed |
| `NoAbortAfterCommit` | a committed transaction is never aborted anywhere |
| `PlainOnlyCommitted` | the plain spaces only ever contain committed values |
| `DirectReadsClean` | direct readers never observe uncommitted data |
| `DirectReadYourWrites` | a direct read after an acknowledged commit sees it |
| `StrictSerializable` | the acknowledged transactions have a serial order, consistent with real time, that explains every value they read |

`StrictSerializable` is checked constructively: the spec records the order in
which writes reached each key and the real-time predecessors of each
transaction, and the invariant searches for a witness order.

Each workload (`MC.tla`) is three transactions over two shards: *Skew* (two
cross-shard read-modify-writes that read each other's write target, plus a
cross-shard reader), *Local* (single-shard blind write racing a cross-shard
transaction, observed by a cross-shard reader), *Frac* (cross-shard blind
writer, cross-shard reader, single-shard read-modify-write), *Recov* (cross-
shard read-modify-write under crashes and message loss, plus single-shard
writer and reader).

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
unchanged" check.

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
after any election state change, before the next check (replicas do not
maintain it). Because checks now see current lock state rather than the
reads' snapshot, every read-only validation (single-shard too) does the
probe write of §2.4, so "reads unchanged" plus "no W lock now" together
give the model's atomic `SnapshotRead`/`ValidateRO`.

### 2.6 Timing assumptions (outside the model)

Safety does not depend on timing, with one exception that the model does not
cover: garbage collection of protocol records. Tombstones (participants
fenced by recovery) are kept `tombstone_ttl` (3600 s), and PREPAREs for
transactions that started more than `max_txn_age` (1800 s) ago are refused,
judged by the coordinator's start timestamp. A delayed PREPARE therefore
always meets its tombstone, provided clocks agree to within the 1800 s
difference. PREPARE is never re-sent by a coordinator (a lost vote goes to
recovery, as in the model), so applied participant records and decided
transaction records can be dropped after `record_ttl` (30 s) once no
participant is still prepared.

### 2.7 Ack-on-prepare

With `--ack-on-prepare`, the client is acknowledged as soon as every PREPARE
succeeded (one round trip instead of two); APPLY happens in the background.
TLC shows SQL-level strict serializability is preserved
(`Skew_AckOnPrepare`, `Recov_AckOnPrepare`: SQL readers wait on the W locks),
while direct read-your-writes is lost (`Tradeoff_AckOnPrepare_DirectRYW`).

## 3. Verus

`crates/tarn-verified` is checked with Verus (`verus --crate-type=lib
src/lib.rs`: **58 verified, 0 errors**) and compiled into the server as plain
Rust. What is proven:

* **Routing** (`route.rs`). `crc32c_update` equals a bitwise CRC-32C
  specification (`crc_bytes`) for any input; `bucket_from_crc` returns a
  bucket in `1..=bucket_count`; `shard_of_bucket` returns a shard in
  `0..nshards`, is monotone in the bucket, and every shard owns at least one
  bucket (the map is a total, onto partition). The SQL layer, the KV
  benchmark and the bucket ids stored in tuples all use these functions.
* **Coordinator logic** (`coord.rs`). `decide(votes)` returns COMMIT iff
  every vote is YES and ABORT iff some vote is NO (the TLA+ `CoordDecide`);
  `resolve_step(record, participant states)` is one step of recovery
  (`RecoverFenceRecord`, `RecoverFenceParticipant`, `RecoverDecide`) and is
  proven to report/decide COMMIT only when the transaction is committed in the
  sense of `CommittedNow`, ABORT only when it can never commit, and to fence
  only participants that never prepared. The coordinator in `txn.rs` calls
  both. (The storage-side recovery fiber in Lua implements the same TLA+
  actions and is covered by tests, not by proof.)
* **MySQL wire codec** (`wire.rs`). Length-encoded integers and strings:
  encoding matches a byte-level spec, decoding inverts it and never reads out
  of bounds; packet framing: `frame_packets` produces exactly the MySQL
  multi-packet format (payloads ≥ 16 MiB split, empty terminator packet when
  the length is a multiple of 0xFFFFFF, sequence ids wrapping), and
  `deframe` is proven to return precisely the payload that `frames_spec`
  framed, or `None` while incomplete. No overflow or panic on any input.
  `tarn-mysql::codec` delegates to these functions.

## 4. Tests

| Suite | Scope | Result |
|---|---|---|
| `lua/test/run.sh` | storage protocol on two real shards: commit paths, locks, PREPARE/APPLY/ABORT, tombstones, fencing, recovery, lock-aware reads | 50/50 |
| `cargo test` (tarn-verified, tarn-mysql, tarn-sql) | codec boundaries, framing at 16 MiB edges, CRC vs Tarantool, coordinator decisions, binary/text row encoding, parameter decoding (all types, truncated input), SQL normalization, expressions | all pass |
| `crates/tarn-iproto/tests` | real Tarantool: auth, iterators, DML, call/eval, streams and conflicts, 20k pipelined requests, connection loss, MP_DECIMAL written by Tarantool | 29 tests pass |
| `tests/sql/difftest.py` | 99 SQL cases run on Tarn and MariaDB 10.11, results compared (incl. column names, affected rows, final table contents): access paths, expressions, aggregates, joins, subqueries, UNION, DML, errors | 99/99 identical |
| `tests/sql/fuzzdiff.py` | randomly generated SELECTs (predicates with NULLs/IN/BETWEEN/LIKE, arithmetic, aggregates, GROUP BY/HAVING, ORDER BY+LIMIT, joins, subqueries) on Tarn and MariaDB | 2,000 queries, 0 mismatches |
| `tests/sql/txntest.py` | rollback, visibility, write skew, lost update, phantom, fractured read, real-time order, FOR UPDATE, duplicate keys in transactions, cross-shard atomic DML, direct KV reads, concurrent bank transfers with serializable audits | 12/12 |
| `tests/failover.sh` | 2 shards × 3 synchronous replicas under the bank workload; kill -9 the leader of shard 1, then of shard 2 | passes: new leaders in ≈2 s, every audit exact, final sums exact via SQL and via direct KV reads on the new leaders |

## 5. Results

### 5.1 Model checking

Every configuration of `tla/MC.tla` and its result (`python3 tla/summarize.py`
regenerates this table from `tla/out`). Configurations named `*_ROValidate`
model the implementation exactly (cross-shard read-only transactions commit
by validation, no PREPARE); the others model the PREPARE-based variant.
`Bug_*` configurations switch off one protection each and must fail;
`Tradeoff_*` check documented weaker guarantees and must fail.

<!-- TLC -->
| Configuration | Result | Distinct states | States generated | Depth | Time | Trace length |
|---|---|---:|---:|---:|---|---:|
| Bug_LocalIgnoresRLocks | violates StrictSerializable | 33430 | 136338 | 13 | 03s | 13 |
| Bug_LocalReadsIgnoreWLocks | violates StrictSerializable | 3608 | 10065 | 10 | 01s | 10 |
| Bug_NoReadLocks | violates StrictSerializable | 2056 | 7030 | 9 | 01s | 9 |
| Bug_NoTombstone | violates AppliedOnlyIfCommitted | 6541 | 17971 | 9 | 01s | 9 |
| Bug_NoWLockCheckOnRead | violates StrictSerializable | 12218 | 45049 | 11 | 02s | 11 |
| Bug_ROValidateNoReadCheck | violates StrictSerializable | 19356 | 70996 | 12 | 04s | 12 |
| Bug_ROValidateNoWLockCheck | violates StrictSerializable | 10736 | 37525 | 11 | 04s | 11 |
| Frac | no errors | 1182650 | 9419404 | 23 | 01min 57s |  |
| FracFailover | no errors | 2715768 | 21065756 | 25 | 04min 39s |  |
| FracFailover_ROValidate | no errors | 544585 | 2974335 | 21 | 57s |  |
| Frac_ROValidate | no errors | 103162 | 596551 | 19 | 13s |  |
| Local | no errors | 710975 | 5538922 | 23 | 01min 08s |  |
| LocalFailover | no errors | 1881011 | 14423765 | 25 | 03min 14s |  |
| LocalFailover_ROValidate | no errors | 407273 | 2199390 | 21 | 39s |  |
| Local_ROValidate | no errors | 69500 | 394964 | 19 | 08s |  |
| Recov | no errors | 71927 | 412305 | 18 | 08s |  |
| RecovReaders | no errors | 28258 | 135187 | 17 | 04s |  |
| Recov_AckOnPrepare | no errors | 83195 | 474842 | 18 | 08s |  |
| Skew | no errors | 12526098 | 130350710 | 30 | 25min 47s |  |
| SkewFailover | incomplete | 137797 | 650139 | - | - |  |
| SkewFailover_ROValidate | no errors | 4519985 | 36314837 | 28 | 09min 57s |  |
| Skew_AckOnPrepare | no errors | 26198655 | 277233192 | 30 | 01h 03min |  |
| Skew_ROValidate | no errors | 2085494 | 17591232 | 26 | 23min 00s |  |
| Tradeoff_AckOnPrepare_DirectRYW | violates DirectReadYourWrites | 546 | 957 | 7 | 00s | 7 |
| Tradeoff_DirectReadAtomic | violates DirectReadAtomic | 2236 | 6582 | 8 | 01s | 8 |

### 5.2 Benchmarks

See [bench/RESULTS.md](bench/RESULTS.md) for the full tables, setup and
discussion. In short, on this 2-vCPU machine (load generator included):

<!-- BENCH -->
| Best throughput over 1/16/64 threads | MariaDB 10.11 | MySQL 9.7 | Tarn 2 shards | Tarn 1×3 sync replicas |
|---|---:|---:|---:|---:|
| oltp_point_select (QPS) | 84,134 | 62,481 | 42,490 | 54,648 |
| oltp_read_only (TPS) | 3,294 | 2,012 | 902 | – |
| oltp_read_write (TPS) | 2,250 | 1,112 | 549 | 669 |
| oltp_write_only (TPS) | 9,766 | 5,679 | 2,756 | – |
| oltp_update_index (TPS) | 43,426 | 24,907 | 26,056 | 14,651 |
| oltp_insert (TPS) | 45,461 | 29,632 | 24,255 | 14,560 |
| direct KV point reads (ops/s) | – | – | 366,436 | – |

## 6. Limitations

* SQL coverage is "sysbench-level plus": uncorrelated subqueries (`IN`,
  `EXISTS`, scalar) and `UNION [ALL]` work (the inner SELECTs run first,
  inside the statement's transaction); not supported are correlated
  subqueries, derived tables, views, stored procedures, triggers, foreign
  keys, savepoints, `ALTER TABLE`, window functions, RIGHT/FULL joins.
  Joins are nested-loop with index lookups.
* String comparison, ordering, grouping and unique indexes are binary (like
  `utf8mb4_bin`); MySQL's default collations are case-insensitive, so
  `WHERE name = 'ALICE'` does not match `'alice'` in Tarn.
* Hash sharding: range predicates on the shard key and lookups through
  non-unique secondary indexes go to every shard.
* Serializable OCC: long read-only transactions that span shards can starve
  under heavy concurrent writes to what they read (they abort and retry);
  explicit transactions surface 1213 to the client like any serializable
  database.
* AUTO_INCREMENT values are allocated in blocks per SQL server: unique and
  increasing per server, with gaps across servers (as in TiDB). An explicit id
  inserted through one server inside another server's cached block is not
  detected until that server reaches it (duplicate-key error then).
* DDL is not transactional and assumes no concurrent DDL on the same table.
* The bucket → shard map is static (no online resharding / bucket moves yet),
  so vshard compatibility is at the level of tuple layout and bucket hashing.
* No TLS, no `COM_CHANGE_USER`, no multi-statement packets, users and
  passwords come from the command line.
