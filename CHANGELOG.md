# Changelog

All notable changes to Tarn are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `ALTER TABLE ... ADD UNIQUE` and `CREATE UNIQUE INDEX` of a unique key
  without the shard key on a table with rows, and dropping one
  ([#88](https://github.com/cstheorist/tarn/issues/88)): the global index is
  built from the existing rows while the table's writes wait, and a value
  two rows hold fails the change with 1062 (refused above
  `tarn.cfg.alter_check_rows` rows per shard, as row checks are). Lookups by
  the full key of such a unique key read its global index instead of asking
  every shard.
- An ordered read with `LIMIT` chooses between an index with a longer
  equality prefix, which it must read whole and sort, and the index that
  follows `ORDER BY`, which it pages until `LIMIT` (+ `OFFSET`) rows
  qualify, by row counts: the shard counts both ranges (`tarn_estimate`, an
  index count on memtx, a sample and a capped scan on vinyl) and the read
  pages the ordered index when about `want * n_ordered / n_prefix` rows are fewer than
  `n_prefix` ([#81](https://github.com/cstheorist/tarn/issues/81)). On an
  orders-list benchmark the open-orders page of a tenant with 300,000 rows
  reads 204 rows instead of 112,726. `SHOW STATUS` reports
  `Tarn_path_estimates`. On vinyl the estimate samples the ordered index's
  first page (`4 * want` rows) to measure what paging would read, and
  counts the prefix only up to that
  ([#93](https://github.com/cstheorist/tarn/issues/93)): the open-orders
  page takes about 2 ms there instead of 15-19 ms.

- Unique keys that do not contain the shard key
  ([#14](https://github.com/cstheorist/tarn/issues/14)), such as
  `PRIMARY KEY (id), UNIQUE KEY (tenant_id, handle)`, are accepted and
  enforced across shards: each gets a global index, a hidden space sharded
  by the key, whose entries are written in the same transaction as the rows.
  1062 names the unique key; `INSERT IGNORE`, `ON DUPLICATE KEY UPDATE`,
  `REPLACE` and updates find the holder of a value through it; NULLs take
  no entry. READ COMMITTED transactions lock the value on the entry's shard.
  Writes to such tables touch one more shard per such key. `ALTER TABLE`
  cannot add one to an existing table yet (1503), nor change one (1846). The
  TLA+ model gains inserts and the `UniqueHolds` invariant (`Uniq*`,
  `Bug_InsertNoExistCheck`); Verus proves the entry maintenance (61
  verified).
- `col IS NULL` and `col <=> x` on a nullable index column are used as an
  equality whose NULL selects the column's NULLs when choosing the access
  path (the index lookup reads them), so an index
  `(a, b, c)` serves `a = 1 AND b IS NULL AND c = 0` with one prefix
  instead of scanning all of `a = 1`
  ([#51](https://github.com/cstheorist/tarn/issues/51)). A unique key, which
  holds any number of NULLs, is then read as a range. `IS NOT NULL` and
  comparisons with NULL keep their semantics.
- `ORDER BY <index columns> DESC ... LIMIT n` reads the index backwards, from
  the upper bound of the range, a page at a time, and stops after `n`
  (+ `OFFSET`) qualifying rows instead of reading the whole range forwards
  and sorting it ([#50](https://github.com/cstheorist/tarn/issues/50)).
  Locking reads page backwards too.
- Ordered pages read only the page: a `SELECT ... WHERE <index prefix>
  [AND <other conditions>] ORDER BY <next index columns> LIMIT n` picks the
  index that returns rows in `ORDER BY` order, reads it a page at a time,
  filters each page and stops once `n` (+ `OFFSET`) rows qualify, in
  autocommit, in transactions (with their own writes merged in) and under
  both isolation levels ([#41](https://github.com/cstheorist/tarn/issues/41)).
  Serializable reads record only the part of the range they read. On an
  orders-list benchmark with 300,000 rows in one shop the first page used
  to read the whole shop. `SHOW STATUS` reports `Tarn_rows_read`.
- Collations ([#13](https://github.com/cstheorist/tarn/issues/13)): text
  columns compare, sort, group and are unique under the collation they
  declare (`COLLATE`, `CHARACTER SET`), the table's (`DEFAULT CHARSET` /
  `COLLATE`) or the server's (`--default-collation`, `utf8mb4_bin` unless
  set). `utf8mb4_general_ci` and `utf8mb4_unicode_ci` (and their NO PAD
  forms) behave exactly as in MariaDB 10.11, from its weight tables; other
  collation names map to the closest one. A unique key refuses `'a'` next
  to `'A'` under a `_ci` collation, `WHERE handle = 'Foo'` finds `'foo'`
  through the index, and a case-insensitive primary key routes both to the
  same shard: such a column, when indexed, gets a weight field that its
  indexes and the shard key use. Literals take `collation_connection` from
  the client's handshake or `SET NAMES`; `COLLATE`, MySQL's coercibility
  rules (1267 for an illegal mix), `COLLATION()`, `COERCIBILITY()`,
  `CHARSET()` and `WEIGHT_STRING()` are supported, and `SHOW CREATE TABLE`,
  `SHOW FULL COLUMNS` and `SHOW COLLATION` report collations. `ALTER TABLE`
  refuses (1235) an index on an existing case-insensitive column that is in
  no index yet and a collation change of an indexed column. Upgrade every
  SQL server before creating tables with indexed case-insensitive columns:
  older servers do not write weight fields.
- Row-value comparisons `(a, b) < | <= | > | >= | <=> (x, y)`, with MySQL's
  NULL semantics; a keyset cursor `(a, b) > (?, ?)` on an index reads from
  the cursor instead of the whole index prefix. They failed with 1235
  ([#40](https://github.com/cstheorist/tarn/issues/40)).
- SQL-level prepared statements: `PREPARE name FROM 'text' | @var`,
  `EXECUTE name [USING @a, ...]`, `DEALLOCATE | DROP PREPARE name`. Every
  MySQL 8 dump runs them around its MyRocks bulk-load probes; they failed
  with 1064 and 1235 ([#34](https://github.com/cstheorist/tarn/issues/34)).
- `information_schema.TABLES`, read from the catalog, and `SELECT ... INTO
  @var`: the query at the top of every MySQL 8 dump and the ones Rails' MySQL
  adapter sends (`data_source_exists?`, `tables`, `table_comment`) used to
  fail with 1146 ([#33](https://github.com/cstheorist/tarn/issues/33)).
- Tables without a `PRIMARY KEY`, keyed as InnoDB keys them: by the first
  `UNIQUE` key with only `NOT NULL` columns, or else by a hidden row id that
  statements do not see ([#32](https://github.com/cstheorist/tarn/issues/32)).
  Rails' `schema_migrations` used to fail with 1173.
- Vinyl settings and sizing: storage nodes take vinyl's read and write
  threads, page and range size and bloom filter false-positive rate from
  `TARN_VINYL_*` variables, and `docs/operations.md` gives the memory and
  disk a vinyl shard needs, measured
  ([#23](https://github.com/cstheorist/tarn/issues/23)). CI runs the
  fault-injection and failover tests with vinyl tables too.
- `bench/RESULTS.md`: sysbench on vinyl tables, cached and with data ten
  times the cache, against memtx and against TiDB on the same host
  ([#23](https://github.com/cstheorist/tarn/issues/23)).
- `ALTER TABLE ... ADD INDEX` on a vinyl table builds the index before it
  holds the table's writes, which then wait only for the switch at the end
  instead of the whole build ([#23](https://github.com/cstheorist/tarn/issues/23)).
  Meanwhile a new unique index already refuses rows that would break it
  (1062); a committed cross-shard transaction whose row it refuses drops it
  and lands, and the schema change then fails on the duplicate. Checking
  the existing rows of a table for a narrower type or `NOT NULL`, which
  holds its writes for the scan, is refused (1846) above
  `tarn.cfg.alter_check_rows` rows per shard (a million by default).
- Tables on disk: `CREATE TABLE ... ENGINE=vinyl` stores a table in
  Tarantool's vinyl engine (an LSM tree with a RAM cache) instead of memtx,
  for data larger than RAM; `--default-engine` sets what `ENGINE=InnoDB`
  and tables without the option get
  ([#23](https://github.com/cstheorist/tarn/issues/23)). One transaction may
  span both engines. The commit steps that the TLA+ model takes as atomic
  now read and write first and check and lock in one step that does not
  yield, since vinyl reads yield; autocommit reads of vinyl tables check the
  history of W locks taken while they ran. `SHOW CREATE TABLE`,
  `SHOW TABLE STATUS` and `SHOW ENGINES` report the engine.
  `TARN_VINYL_MEMORY_MB` and `TARN_VINYL_CACHE_MB` size vinyl's memory.

- READ COMMITTED with InnoDB's locking behaviour, chosen per session, per
  transaction or globally (`transaction_isolation` / `tx_isolation`,
  `SET [GLOBAL | SESSION] TRANSACTION ISOLATION LEVEL`, `--isolation`); strict
  serializability stays the default
  ([#17](https://github.com/cstheorist/tarn/issues/17)). Plain reads see the
  latest committed data per statement and never cause an abort. Writes and
  locking reads take row locks with FIFO wait queues; `innodb_lock_wait_timeout`
  (1205), deadlock detection on each shard and across shards (1213); `UPDATE`
  and `DELETE` re-read and re-check each row under its lock, so `SET n = n - 1`
  waits instead of aborting and loses nothing; `INSERT`, `REPLACE` and upserts
  lock unique-key values, so concurrent inserts of one value wait for each
  other. Commits stay atomic across shards, durable on a quorum and visible in
  real time. The TLA+ model covers it, with five new protection switches.
- `FOR UPDATE`, `FOR SHARE` and `LOCK IN SHARE MODE` lock rows, with `NOWAIT`
  (3572) and `SKIP LOCKED`, which skips rows other transactions hold and keeps
  scanning until `LIMIT` is filled
  ([#12](https://github.com/cstheorist/tarn/issues/12)). At SERIALIZABLE the
  rows read this way stay validated at commit.
- `SAVEPOINT`, `ROLLBACK TO [SAVEPOINT]` and `RELEASE SAVEPOINT`, and
  statement-level rollback inside transactions
  ([#15](https://github.com/cstheorist/tarn/issues/15)).
- MySQL 8.0.19's row alias in upserts, `INSERT ... VALUES (...) AS new [(cols)]
  ON DUPLICATE KEY UPDATE c = new.c`, which Rails' `upsert_all` sends to
  servers that report MySQL 8 (as Tarn does).
- `tests/sql/rctest.py`: concurrent tests of READ COMMITTED against InnoDB's
  behaviour; `bench/locking.py`, a contention benchmark;
  `Tarn_cross_shard_deadlocks` in `SHOW TARN STATUS`.
- Online `ALTER TABLE`: add, drop, modify, change and rename columns,
  defaults, add, drop and rename indexes, foreign keys, `RENAME TABLE`,
  `DROP INDEX ... ON`, table options
  ([#16](https://github.com/cstheorist/tarn/issues/16)). No row is
  rewritten (added columns get new fields after the existing ones, filled
  with their default in older rows); existing rows are checked against
  narrower types (1264 / 1265); new indexes are built on every shard before
  the change takes effect. Every write carries the version of its table's
  definition and the shards refuse writes made with a replaced one (1412),
  so SQL servers that have not reloaded their catalog cannot write in an
  old layout; their statements are run again with the new definition. A
  transaction that wrote a table and then meets a newer definition of it
  fails with 1412 and rolls back. Schema changes run one at a time; those
  sent from several connections at once wait for each other.
  Conversions that would need rows rewritten (`INT` to `VARCHAR`, a
  different `DECIMAL` scale) and primary key changes are refused (1235).
- Loading `mysqldump` files: MySQL's executable comments `/*!NNNNN ... */`,
  user-defined variables (`SET @v = expr`, `@v` in statements), session
  variables set and read back (`SET var = @saved`), `NO_AUTO_VALUE_ON_ZERO`,
  `LOCK TABLES`, `CREATE TABLE` options `AUTO_INCREMENT = n` and `COMMENT`,
  `INVISIBLE` indexes, and foreign keys (recorded, not enforced).
- Multi-statement batches (`CLIENT_MULTI_STATEMENTS`, `COM_SET_OPTION`).
- `tests/sql/ddltest.py`: dumps, batches, and `ALTER TABLE` while two SQL
  servers write the table.

### Changed

- A READ COMMITTED `SELECT ... WHERE <primary key prefix> [ORDER BY
  <primary key>] LIMIT n FOR UPDATE | FOR SHARE [NOWAIT | SKIP LOCKED]`
  makes one storage call: the shard scans the range and takes the locks
  itself (`tarn_lock_range`), where the SQL server used to read a page of
  candidates and then send a lock request for them. The parts of a `UNION
  ALL` of such reads go together, in one call per shard, and under `SKIP
  LOCKED` or `NOWAIT` the shards' calls run in parallel
  ([#49](https://github.com/cstheorist/tarn/issues/49)). A two-part
  reservation query went from 4 sequential storage calls to 1. Under `SKIP
  LOCKED` such a read now skips a row a prepared cross-shard transaction
  W-locks instead of waiting for it (or failing with 1213 after
  `lock_wait`). Reads of more than 256 rows keep the paged path.
- Vinyl reads from disk on several threads by default
  ([#74](https://github.com/cstheorist/tarn/issues/74)): a storage instance
  sets `vinyl_read_threads` to the machine's CPUs divided by
  `TARN_HOST_INSTANCES` (1 unless set; `scripts/cluster.sh` sets it), at
  least 1 and at most 8, instead of Tarantool's single thread.
  `TARN_VINYL_READ_THREADS` still overrides it. Point selects of a vinyl
  table holding 13 times its cache went from 8.6k/s to 25.3k/s at 8 clients.
- Schema changes run as jobs with one record per table on the first shard,
  instead of one at a time cluster-wide: an `ALTER TABLE` that builds an
  index on a large table no longer holds up DDL on other tables. A job
  records its phase, and a server that finds one abandoned
  (`ddl_job_stale`, 30 s, in `cluster.json`) rolls it back before every
  shard prepared and forward after; every SQL server looks for them every
  10 s ([#30](https://github.com/cstheorist/tarn/issues/30)).
- Under READ COMMITTED, a row lock grant waits only for the local commits
  in flight that write the locked row or unique value, not for every one on
  the shard: transactions writing different rows no longer queue behind
  each other's WAL writes ([#47](https://github.com/cstheorist/tarn/issues/47)).
- Under READ COMMITTED, `UPDATE` and `DELETE` by full primary key (`WHERE
  id = ?`, `id IN (...)`) lock and read their rows in one storage call per
  shard, without reading candidates first: a read-modify-write transaction
  of two such updates makes 3 storage calls instead of 5
  ([#48](https://github.com/cstheorist/tarn/issues/48)).
- `SELECT COUNT(*)` (and `COUNT(col)`, `COUNT(1)`) of one table without
  `GROUP BY` is counted by the shards, which return one number each instead
  of every matching row, when the `WHERE` clause is an index range plus
  simple comparisons, `IN` lists and `IS [NULL | NOT NULL]` tests of integer
  and character columns; an equality prefix alone is counted by the index
  ([#52](https://github.com/cstheorist/tarn/issues/52)). Counts see the
  transaction's own writes and are validated at commit as reads of their
  ranges. Counting a 300,000-row prefix of an orders-list benchmark takes
  85 ms instead of 305 ms with a residual condition, and 0.2 ms instead of
  330 ms without one. `SHOW STATUS` reports them as `Tarn_storage_counts`.
- `col IN (...)`, `OR`s of equalities and row-value `IN` are index lookups
  on any index, batched into one storage call per shard, and an `OR` across
  indexes is a union of lookups (MySQL's `index_merge`); they used to scan
  the whole table. Long scans inside transactions yield, and autocommit
  scans no longer fail with "fiber slice is exceeded"
  ([#10](https://github.com/cstheorist/tarn/issues/10)).
- The cross-shard deadlock detector follows the wait-for graph from each
  SQL server's own waiting transactions instead of asking every shard for
  its lock waits every second
  ([#26](https://github.com/cstheorist/tarn/issues/26)). Lock requests name
  the other shards where their transaction holds row locks, so a server
  asks only the shards where its waiting transactions wait or hold locks,
  then those where the transactions waiting for them hold locks. A server
  whose transactions do not wait makes no storage call (each one used to
  make one per shard per second: 50,000 calls/s for 50 servers and 1,000
  shards), and with contention the calls grow with the number of waits.
  A cycle is confirmed only if a second look finds each of its waits still
  pending and still waiting for the next transaction of the cycle; the
  same wait id alone no longer suffices. `Tarn_deadlock_detector_calls` in
  `SHOW TARN STATUS` counts the detector's storage calls.

### Fixed

- New indexes on instant-added columns could miss old rows or accept
  duplicate logical keys: short tuples omit the column's non-NULL fill,
  which a native index reads as NULL
  ([#70](https://github.com/cstheorist/tarn/issues/70)). Such builds now fail
  with 1846 before publishing the index; empty tables, physically present
  fields and NULL fills remain supported. Automatic backfill is not yet
  implemented. Vinyl builds that need this check run behind the write fence.
- A large scan no longer kills a storage node. A read returned its whole
  range in one reply, held in the node's Lua heap, so reads of millions of
  rows (an unindexed `WHERE` over a big table) took the heap to Tarantool's
  Lua memory limit, and the node then exited with a Lua panic. Reads now
  return at most 4096 rows or 4 MiB per call and the SQL server continues
  after the last one, applying a single-table `WHERE` page by page; a read
  that finds the node's Lua heap nearly full fails with 1041 and the node
  stays up ([#77](https://github.com/cstheorist/tarn/issues/77)).
  Autocommit reads that need more than one page run as implicit read-only
  transactions, and a READ COMMITTED `SELECT` that does reads in one
  read-only snapshot overlaid with its transaction's writes, so that rows
  moved within an index meanwhile are neither missed nor returned twice.
  Both may therefore fail with 1213 (after 20 retries) when writes keep
  changing what they read, which rolls back the READ COMMITTED transaction
  as any 1213 does; short reads are unchanged.
- `SET @var = literal` after a leading `--`, `#` or `/* ... */` comment
  failed with 1064 because literal normalization turned the value into an
  unbound placeholder ([#71](https://github.com/cstheorist/tarn/issues/71)).
  Administrative recognition now skips comments while preserving the
  original assignment expressions and quoted values.
- `col <=> NULL` (and `col <=> ?` bound to NULL) on an indexed nullable
  column found no rows: the index lookup it became drops a NULL key part.
  It now matches the rows whose column is NULL in `SELECT`, `UPDATE` and
  `DELETE` ([#72](https://github.com/cstheorist/tarn/issues/72)).
- `SELECT ... HAVING <condition> LIMIT n` without aggregation pushed the
  `LIMIT` down to storage before `HAVING` filtered, and returned too few
  rows, often none ([#38](https://github.com/cstheorist/tarn/issues/38)).
- `DATETIME` and `TIMESTAMP` values compared by their spelling: a stored
  `'2025-01-01 00:00:01.000000'` did not equal `'2025-01-01 00:00:01'`, so
  keyset pagination repeated rows ([#37](https://github.com/cstheorist/tarn/issues/37)).
  Values are now stored in one form, with exactly the column's fractional
  digits (`DATETIME(6)` keeps its precision, which the catalog used to drop),
  and constants compared with date and datetime columns, in `WHERE`, `IN`,
  `BETWEEN` and index lookups, compare by value. Rows written before this
  change keep their old spelling.
- `CLIENT_FOUND_ROWS` was ignored: an `UPDATE` that matched a row without
  changing it reported 0 affected rows to drivers that ask for matched rows
  (Rails, Go's `clientFoundRows=true`), which read it as a missing row. It now
  reports matched rows, in every `UPDATE` path, and 1 for an upsert that
  changes nothing ([#39](https://github.com/cstheorist/tarn/issues/39)).
- A READ COMMITTED lock request that failed (1205, 3572 or 1213) after some
  of its rows were locked kept those locks after its transaction ended when
  it was the transaction's first lock request on that shard: they were
  released only when the SQL server's connection to the shard closed or the
  leader changed, and until then other transactions waited for those rows
  and timed out. The failed request now reports the locks' generation and
  the transaction releases them when it ends.
- `CREATE TABLE`, `DROP TABLE` and `TRUNCATE` were sent to every shard at
  once: a shard that failed left the table on some shards only, a
  `TRUNCATE` between a cross-shard transaction's PREPARE and its APPLY left
  that transaction's rows, and a transaction that had read the table could
  still commit after it. They now run in steps like `ALTER TABLE` and take
  effect on every shard or on none. A `DROP TABLE` or `TRUNCATE` first waits
  for the prepared transactions holding locks on the table; until it is
  applied, reads of the table wait and transactions that read it fail
  validation; `TRUNCATE` refuses writes encoded before it (1412)
  ([#30](https://github.com/cstheorist/tarn/issues/30)).
  `tests/ddlfaults.sh` interrupts each with a shard failure.
- Bootstrapping a replicated shard could stop storage instances: the new
  leader's first synchronous write, made while its replicas were still
  joining, timed out (stopping the leader), or reached a joining replica,
  whose first checkpoint then timed out waiting for its confirmation
  (stopping the replica). The leader now waits for its replicas before that
  write, and creates the schema in the background, retrying, so that a
  leadership change or a timeout during bootstrap no longer stops it.
  `scripts/cluster.sh start` returns once every replica has joined.
- `DROP TABLE` between a cross-shard transaction's PREPARE and its APPLY left
  the transaction unfinished for good: APPLY failed on the missing table at
  every attempt, so the transaction's writes to other tables never landed
  and their row locks stayed. APPLY now skips the dropped table's writes. A
  write that meets a table dropped through another SQL server fails with
  1146 instead of "storage error 36".
- A cross-shard transaction that wrote a row with NULL in a nullable unique
  key failed its PREPARE ("Supplied key type of part 0 does not match index
  part type"), and two of them conflicted on the NULL value; NULLs now take
  no unique value, as in MySQL.
- `DECIMAL(p, s)` values with more than p - s digits before the point were
  stored; they now fail with 1264, as in MySQL.
- A statement on one SQL server right after another server created or
  recreated a table, a column or a database failed until the next catalog
  poll (up to 0.3 s) with "doesn't exist" errors; it is now re-planned with
  the newer catalog, also while other schema changes reload the catalog at
  the same moment, and `SHOW TABLES`, `SHOW CREATE TABLE`, `SHOW INDEX`,
  `SHOW COLUMNS` and `DESCRIBE` check for a newer catalog first.
- `REPLACE` into a table with a unique key besides the primary key failed
  with 1062 when the new row collided with another row on that key; it now
  deletes that row, as MySQL does. Commits apply deletes before inserts, so
  a new row can take over a deleted row's unique value.
- `UPDATE ... SET c = c - 1` failed with 1264 when a signed `BIGINT` result
  was negative. Arithmetic on an unsigned integer now fails with 1690 below
  zero and BIGINT overflow with 1690, as in MySQL
  ([#11](https://github.com/cstheorist/tarn/issues/11)).

## [0.1.0] - 2026-10-25

First public release: a research prototype.

### Added

- `tarn-server`: MySQL protocol front end (handshake, `mysql_native_password`,
  text and binary protocol, prepared statements) over Tarantool shards.
- SQL: sysbench-level statements plus joins, aggregates, `GROUP_CONCAT`,
  subqueries without outer references, `UNION [ALL]`, `INSERT ... SELECT`,
  `ON DUPLICATE KEY UPDATE`, `REPLACE`, `AUTO_INCREMENT`, `SHOW` and `SET`
  statements; checked against MariaDB 10.11 by differential and randomized
  tests.
- Storage: every table is a plain Tarantool space, hash-sharded with vshard's
  bucket function, readable directly as key/value. Replicated shards use
  synchronous replication and Raft leader election.
- Transactions: strictly serializable across shards. Autocommit reads wait
  for W-locks of committed but not yet applied transactions. Single-shard
  commits are validated by memtx MVCC. Read-only commits validate. Cross-shard
  commits use a parallel-commit two-phase commit with recovery, an
  optional `--ack-on-prepare` mode, and expiry-based garbage collection of
  protocol records.
- `tarn-iproto`: an async, pipelined Tarantool IPROTO client with streams,
  `MP_DECIMAL` and zero-copy decoding.
- TLA+ specification `tla/TarnCommit.tla` with 33 TLC configurations,
  including 10 protection switches that TLC must catch.
- Verus proofs for shard routing, coordinator decisions and recovery steps,
  and the MySQL wire codec (59 verified).
- Tests: storage protocol (74), IPROTO integration, SQL differential and
  fuzzing against MariaDB, transaction anomalies, a ledger-checked bank
  workload, failover (live leader change and leader kills), fault injection.
- sysbench harness and results against MariaDB 10.11 and MySQL 9.7.

### Fixed during development

Found by TLC, by tests, by benchmarking, and by an independent code review
before the release; see [docs/design.md](docs/design.md) §2.3–§2.8.

- Single-shard reads ignored W-locks of committed but unapplied cross-shard
  transactions, breaking strict serializability (found by TLC).
- Cross-shard read-only commits could validate stale reads that memtx had
  moved into a read view (found by a targeted test).
- Lock checks read the lock spaces through MVCC, so unrelated PREPAREs
  conflicted and `oltp_read_write` collapsed under concurrency (found by
  benchmarking).
- Recovery could abort a committed transaction already applied on one shard
  after a participant record was garbage-collected (critical; found by the
  review).
- Read-only commits could validate on a demoted leader whose connection
  survived (found by the review).
- Autocommit DML could be applied twice after a lost connection; such
  outcomes are now reported as unknown (1180 / SQLSTATE 40003) (found by the
  review).
- The in-memory lock index could keep locks after a failed PREPARE, and lost
  the locks of PREPAREs waiting for a quorum when rebuilt (found by the
  review).
- An expired PREPARE could vote NO and contradict a commit whose records were
  already collected (found by TLC while modelling the fix above).

[Unreleased]: https://github.com/cstheorist/tarn/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/cstheorist/tarn/releases/tag/v0.1.0
