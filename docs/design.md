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

Conflicts surface as MySQL error 1213 (`ER_LOCK_DEADLOCK`, SQLSTATE 40001),
which clients already treat as "retry the transaction". Autocommit statements
are retried transparently (up to 20 times with backoff). A shard leader
change (the request reached a demoted node, which answers ER_READONLY) and a
connection lost during a read are reported the same way: nothing was
written. A connection lost while a commit or an autocommit write was in
flight is different: the write may or may not have happened, so Tarn reports
error 1180 with SQLSTATE 40003 ("statement completion unknown") and never
re-sends the request.

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

The model has shards, keys with owners, three transactions with fixed
read/write sets, direct (non-SQL) readers, and the actions of the protocol:
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
| `StrictSerializable` | the acknowledged transactions have a serial order, consistent with real time, that explains every value they read (READ COMMITTED transactions: every value they read under a lock) |
| `NoLostUpdate` | every READ COMMITTED write lands on the value its transaction read under the lock |
| `RCReadsSeeAcked` | a READ COMMITTED read sees every transaction acknowledged before it started |

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
decision record, garbage collection without the expiry horizon, and fencing
a missing decision record after expiry.

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
  writes. They take no locks, record no read set and are never validated, so
  they never cause an abort. Like autocommit reads they wait while a
  prepared cross-shard transaction W-locks what they read, because that
  transaction may already be committed and acknowledged (`RCRead`).
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
  numbers).
* **Writes re-read under the lock.** UPDATE and DELETE take candidates from
  the latest committed state, lock each in order (in batches, as far as
  LIMIT needs) and re-check WHERE on the version read under the lock.
  Rows that no longer match are unlocked at once; the new values are
  computed from the locked version, so `SET n = n - 1` never loses a
  concurrent decrement. A row that starts to match after the candidate scan
  is not seen: like InnoDB at READ COMMITTED there are no gap locks.
* **Unique keys are locked by value.** INSERT, REPLACE and upserts lock the
  primary key and every unique-key value of each new row, exclusively, and
  read the row holding that value under the lock. Concurrent inserts of one
  value queue; the loser sees the winner's row (1062, its ON DUPLICATE KEY
  UPDATE branch, or nothing with IGNORE), or goes ahead if the winner rolls
  back.
* **Deadlocks.** A shard fails a request with 1213 at once if waiting would
  close a cycle in its own wait-for graph. Cycles through several shards are
  found by each SQL server every `deadlock_interval` (1 s): it collects the
  shards' wait-for edges (`tarn_lock_waits`: waiter, wait id, holders),
  finds the cycles, collects again and keeps the cycles whose waits are all
  still pending with the same wait ids. Each such wait was pending from its
  first observation to its second, so all of them were pending at the same
  time, and a transaction blocked in a lock wait releases nothing, so the
  cycle was real. Its transaction with the largest id (the same choice on
  every SQL server) is failed with 1213 where it waits (`tarn_lock_victim`)
  and rolled back.
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
A write is never applied after its lock was lost (`BugRCNoLockCheck`).

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
still change the row: `tarn_lock` now waits for every local commit that was
in flight at the grant (`wait_commits`) before it reads, and the counter
test loses nothing. The model splits the local commit's confirmation off
(`infl`, `ConfirmLocal`: other readers still see the old value until then)
and blocks `RCLock` while a commit to the key is in flight; with
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

## 3. Verus

`crates/tarn-verified` is checked with Verus (`verus --crate-type=lib
src/lib.rs`: **59 verified, 0 errors**) and compiled into the server as plain
Rust. What is proven:

* **Routing** (`route.rs`). `crc32c_update` equals a bitwise CRC-32C
  specification (`crc_bytes`) for any input; `bucket_from_crc` returns a
  bucket in `1..=bucket_count`; `shard_of_bucket` returns a shard in
  `0..nshards`, is monotone in the bucket, and every shard owns at least one
  bucket (the map is a total, onto partition). The SQL layer, the KV
  benchmark and the bucket ids stored in tuples all use these functions.
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

Not covered by the proofs: how SQL values are turned into the bytes that
are hashed for routing (`tarn-sql/src/route.rs`, tested against Tarantool's
own `digest.crc32` instead), and packet sequence numbers, which `deframe`
does not check.

## 4. Tests

| Suite | Scope | Result |
|---|---|---|
| `lua/test/run.sh` | storage protocol on two real shards: commit paths, locks, PREPARE/APPLY/ABORT, tombstones, fencing, recovery, lock-aware reads; APPLY outcomes, expiry, garbage-collection rules, refusal on demoted nodes and after an election term change, undo of a failed PREPARE's locks, lock-index rebuild while a PREPARE waits for a quorum; multi-key reads, integer arithmetic limits; row locks (queues, upgrades, timeouts, NOWAIT, SKIP LOCKED, local deadlocks, the cross-shard detector's interface, lost generations and connections, RC commits, unique-value locks) | 118/118 |
| `cargo test` (tarn-verified, tarn-mysql, tarn-sql) | codec boundaries, framing at 16 MiB edges, packet size limits, CRC vs Tarantool, coordinator decisions, binary/text row encoding, parameter decoding (all types, truncated input), SQL normalization, expressions | all pass |
| `crates/tarn-iproto/tests` | real Tarantool: auth, iterators, DML, call/eval, streams and conflicts, 20k pipelined requests, connection loss, stale-leader detection (ER_READONLY), MP_DECIMAL written by Tarantool | 30 tests pass |
| `tests/sql/difftest.py` | 174 SQL cases run on Tarn and MariaDB 10.11 and compared exactly: column names, rows (integers and DECIMALs by exact value, DOUBLEs within 1e-9), error codes, affected rows, final table contents; access paths, expressions, aggregates, joins, subqueries, UNION, DML, integer arithmetic, unique keys, isolation variables, locking reads, savepoints and statement rollback, errors | 174/174 identical |
| `tests/sql/fuzzdiff.py` | randomly generated SELECTs (predicates with NULLs/IN/BETWEEN/LIKE, arithmetic, aggregates, GROUP BY/HAVING, ORDER BY+LIMIT, joins, subqueries) on Tarn and MariaDB, rows compared as above | 2,000 queries, 0 mismatches |
| `tests/sql/txntest.py` | rollback, visibility, write skew, lost update, phantom, fractured read, real-time order, FOR UPDATE, duplicate keys in transactions, cross-shard atomic DML, direct KV reads; concurrent bank transfers with serializable audits, a ledger written in the same transactions (lost or duplicated transfer writes, acknowledged commits missing, commits reported as 1213 but applied) and autocommit counters (increments applied twice) | 12/12 |
| `tests/sql/rctest.py` | READ COMMITTED against InnoDB's behaviour: isolation variables, lock waits and timeouts, local and cross-shard deadlocks, 8 threads decrementing counters through autocommit, local and cross-shard transactions (no lost update), semi-consistent UPDATE/DELETE, SKIP LOCKED job queue (every job claimed once), NOWAIT, FOR SHARE, duplicate inserts, concurrent upserts (also with MySQL 8's row alias) and unique-key updates, savepoints, statement rollback, locks released on disconnect, serializable transactions meeting row locks, and 8 s of concurrent transfers checked against a ledger (also run during leader failovers by `tests/failover.sh`) | 22/22 |
| `tests/failover.sh` | 2 shards × 3 synchronous replicas under the bank workload and READ COMMITTED transfers: leadership of shard 1 moved with `box.ctl.promote()` (the old leader keeps its connections), then kill -9 of the leader of shard 1, then of shard 2 | passes: new leaders in ≈2.5 s, all workload checks hold, final sums exact via SQL and via direct KV reads on the new leaders |
| `tests/faults.sh` | a proxy delivers an autocommit `UPDATE ... SET v = v + 1` to the shard, swallows the reply and closes the connection | the client gets 1180 (outcome unknown), the row is incremented exactly once |

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
shards with one or two faults, so these are bounded checks: small, but
exhaustive within those bounds. Two caveats. `Skew_GC_ROValidate`, the
largest workload with garbage collection, a crash and a lost message, did
not finish in the time available: TLC explored 47.8 million distinct states,
breadth-first into depth 23, without finding a violation;
`SkewTimeout_GC_ROValidate` checks the same workload exhaustively with
recovery racing the coordinator but without crashes or message loss. And
`Skew`, `SkewFailover` and `Skew_AckOnPrepare` (up to an hour each) were
last checked before the GC, ApplyMarksCommitted and READ COMMITTED
extensions of the spec existed: with those options off and no READ
COMMITTED transactions, the new actions are disabled and the new variables
stay constant, so the behaviour is unchanged, as re-running every other
configuration with the final spec confirms (identical numbers of distinct
states).

<!-- TLC -->
| Configuration | Result | Distinct states | States generated | Depth | Time | Trace length |
|---|---|---:|---:|---:|---|---:|
| Bug_FenceAfterExpiry | violates NoAbortAfterCommit | 114736 | 576778 | 14 | 10s | 14 |
| Bug_GCIgnoresDecision | violates NoAbortAfterCommit | 22229 | 82897 | 11 | 04s | 11 |
| Bug_GCNoExpiry | violates NoAbortAfterCommit | 2742 | 7635 | 10 | 01s | 9 |
| Bug_LocalIgnoresRLocks | violates StrictSerializable | 33430 | 136338 | 13 | 03s | 13 |
| Bug_LocalReadsIgnoreWLocks | violates StrictSerializable | 3608 | 10065 | 10 | 01s | 10 |
| Bug_NoReadLocks | violates StrictSerializable | 2056 | 7030 | 9 | 01s | 9 |
| Bug_NoTombstone | violates AppliedOnlyIfCommitted | 6569 | 18301 | 9 | 02s | 9 |
| Bug_NoWLockCheckOnRead | violates StrictSerializable | 12218 | 45049 | 11 | 02s | 11 |
| Bug_ROValidateNoReadCheck | violates StrictSerializable | 19356 | 70996 | 12 | 04s | 12 |
| Bug_ROValidateNoWLockCheck | violates StrictSerializable | 10736 | 37525 | 11 | 04s | 11 |
| Frac | no errors | 1182650 | 9429589 | 23 | 03min 29s |  |
| FracFailover | no errors | 2715768 | 21065756 | 25 | 04min 39s |  |
| FracFailover_ROValidate | no errors | 518625 | 2830211 | 21 | 49s |  |
| Frac_GC_ROValidate | no errors | 1846859 | 13211705 | 25 | 02min 54s |  |
| Frac_ROValidate | no errors | 95786 | 555473 | 19 | 12s |  |
| Local | no errors | 710975 | 5545010 | 23 | 02min 04s |  |
| LocalFailover | no errors | 1881011 | 14423765 | 25 | 03min 14s |  |
| LocalFailover_ROValidate | no errors | 397911 | 2148183 | 21 | 38s |  |
| Local_GC_ROValidate | no errors | 1207771 | 8610883 | 25 | 01min 53s |  |
| Local_ROValidate | no errors | 66348 | 377760 | 19 | 09s |  |
| Recov | no errors | 71927 | 412881 | 18 | 09s |  |
| RecovReaders | no errors | 28258 | 135187 | 17 | 04s |  |
| Recov_AckOnPrepare | no errors | 83195 | 474842 | 18 | 08s |  |
| Recov_GC | no errors | 1136892 | 8123071 | 23 | 01min 36s |  |
| Skew | no errors | 12526098 | 130350710 | 30 | 25min 47s |  |
| SkewFailover | no errors | 15518506 | 158998997 | 32 | 30min 08s |  |
| SkewFailover_ROValidate | no errors | 4325065 | 34755383 | 28 | 07min 38s |  |
| SkewTimeout_GC_ROValidate | no errors | 4509135 | 40415963 | 34 | 08min 18s |  |
| Skew_AckOnPrepare | no errors | 26198655 | 277233192 | 30 | 01h 03min |  |
| Skew_GC_ROValidate | stopped at the time limit, no violation so far | 47804036 | 411580777 | ≥23 | - |  |
| Skew_ROValidate | no errors | 1944298 | 16444418 | 26 | 06min 00s |  |
| Tradeoff_AckOnPrepare_DirectRYW | violates DirectReadYourWrites | 546 | 957 | 7 | 00s | 7 |
| Tradeoff_DirectReadAtomic | violates DirectReadAtomic | 2236 | 6582 | 8 | 01s | 8 |

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
  subqueries, derived tables, views, stored procedures, triggers, foreign
  keys, `ALTER TABLE`, window functions, RIGHT/FULL joins.
  Joins are nested-loop with index lookups.
* String comparison, ordering, grouping and unique indexes are binary (like
  `utf8mb4_bin`); MySQL's default collations are case-insensitive, so
  `WHERE name = 'ALICE'` does not match `'alice'` in Tarn.
* Hash sharding: range predicates on the shard key and lookups through
  non-unique secondary indexes go to every shard.
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
* DDL is not transactional and assumes no concurrent DDL on the same table.
* The bucket → shard map is static (no online resharding / bucket moves yet),
  so vshard compatibility is at the level of tuple layout and bucket hashing.
* No TLS, no `COM_CHANGE_USER`, no multi-statement packets, users and
  passwords come from the command line.
