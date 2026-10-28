# Changelog

All notable changes to Tarn are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Recovery and garbage collection could be held up by what they cannot
  resolve ([#129](https://github.com/cstheorist/tarn/issues/129)). While a
  shard was down, 200 undecided transactions that needed it, the first ones
  each recovery pass took, kept every newer one from being resolved; and
  records that could not be collected could fill garbage collection's scan
  window. Each pass now continues where the previous one stopped,
  `recovery_workers` fibers resolve transactions at once, and a
  transaction or a shard that failed waits before it is tried again (up to
  `recovery_backoff_max`). Garbage collection runs on its own fiber, with
  its own budget (`gc_interval`, `gc_time_budget`). What recovery does
  with a transaction, and what garbage collection may delete, are
  unchanged. `tarn_info().recovery` reports the backlog, the age of its
  oldest transaction, the shards in backoff and garbage collection's laps;
  `stats.recovered` counts the participants recovery resolved (it counted
  the attempts that did not raise). `tla/RecoveryFairness.tla` models the
  order of the passes, and `lua/test/recovery_faults.sh` stops one of
  three shards.

- Native-to-Lua autocommit fallback reuses the selected physical connection
  after a matched handback or cached native unavailability. It no longer
  advances an even-sized pool twice per write, leaving half its connections
  unused. Explicit read-only refusals still discover a leader; lost replies
  remain unknown outcomes and are never replayed
  ([#155](https://github.com/cstheorist/tarn/issues/155)).

- IPROTO reply integrity ([#128](https://github.com/cstheorist/tarn/issues/128)):
  malformed frames, unsupported response types and unknown/duplicate SYNCs
  retire the connection instead of certifying a result. Every length-prefix
  width is bounded before payload allocation. Pushes retain the pending
  request's identity, including after caller cancellation. A sent mutation
  whose reply is invalid remains 1180/40003 and is never replayed.

### Added

- Optional bounded cardinality hints and conservative two-table inner-join
  reordering ([#136](https://github.com/cstheorist/tarn/issues/136)).
  `ANALYZE TABLE` now collects gateway-local integer-prefix statistics,
  invalidates plans by generation and reports MariaDB analyze status.
  `SHOW TARN STATISTICS`/EXPLAIN expose uncertainty, and query diagnostics
  count actual decoded storage tuples separately from RPCs.

- Distributed transactions spread decision records across active writing
  participants using a stable hash of the transaction ID
  ([#132](https://github.com/cstheorist/tarn/issues/132)). Durable shard IDs
  determine the candidate order; every PREPARE and recovery operation uses
  the one selected identity. Atomic local commits keep their existing path.
  Placement models, lost-reply/coordinator-crash tests and a repeated
  record/WAL/GC benchmark cover the change.

- Query routing diagnostics ([#133](https://github.com/cstheorist/tarn/issues/133)):
  `EXPLAIN FORMAT=JSON` reports physical access paths, shard/bucket bounds,
  colocation eligibility and possible global-index participants without
  executing the query. `SHOW TARN PLAN FOR` diagnoses unbound prepared
  statements, and bounded `SHOW TARN QUERIES` observations distinguish
  completed runtime fanout from compile-time constraints.

- An opt-in native primary-key batch prototype
  ([#141](https://github.com/cstheorist/tarn/issues/141)) replaces the Lua
  lookup loop for complete memtx autocommit batches of up to 128 keys.
  `native_batch_reads` in the gateway configuration enables it; older
  modules and unsupported requests use the existing Lua path. The model
  checks missing-key locks, snapshot consistency and complete results;
  the executed budget checks are verified with Verus. See the
  [native storage inventory](docs/native-storage.md).

- Bounded storage RPCs ([#127](https://github.com/cstheorist/tarn/issues/127)):
  connection/shard/gateway count and byte admission, reserved completion
  capacity, and deadlines that retire stalled physical connections while
  preserving cancelled identities and 40001 versus 40003. `SHOW TARN RPCS`
  and cached status counters expose charges, age, refusals and retirements.

- Online engine replacement ([#57](https://github.com/cstheorist/tarn/issues/57)):
  `ALTER TABLE ... ENGINE=vinyl` or `memtx` copies physical tuples while
  writes continue, then fences writes for catch-up and secondary-index
  builds. The final pause depends on the table and indexes. The replacement
  preserves native index IDs, frozen defaults and counter identities, with
  fresh global and lazy index generations. Confirmed publication rolls
  forward after a failure; a predecision abort retains the original table.
  Request engine changes separately from other schema alterations; the
  existing engine remains a no-op. See
  [the operational procedure](docs/operations.md#online-engine-changes).

- Why the native storage procedures hand requests to Lua
  ([#131](https://github.com/cstheorist/tarn/issues/131)):
  `tarn_info().native.fallbacks` counts, for each procedure, the requests
  it handed back by reason. The reasons include a lock on what the request
  touches (by kind), state being read again (the lock index, the bucket
  map, the fences), a table that lazy capture or a DDL holds, a vinyl
  table, reads of more than a page, and request shapes left to Lua.
  See [operations](docs/operations.md).
- Vinyl write admission ([#58](https://github.com/cstheorist/tarn/issues/58)):
  storage shards share a pressure-sensitive admission budget across SQL
  servers, refusing new writes before user data changes and preserving
  decided APPLY and recovery. SQL servers briefly wait only after explicit
  before-effect refusals; overload and rolled-back native quota waits
  return 1205. `tarn_info()` and cached `SHOW TARN STATUS` expose regulator,
  compaction, run and admission statistics. See
  [the operational limits and measurements](docs/operations.md#vinyl-write-admission).

- `tla/StreamMarks.tla` models how a shard knows that a stream began in
  its current election term and after any `TRUNCATE` or `DROP TABLE` of a
  table it read (the transaction numbers `TERM_TXN` and `DDL_TXN`), for the
  Lua procedures that commit a stream's reads and for the native ones, with
  the premises of Tarantool's elections that the native ones rely on. TLC
  must fail each of its ten protection switches
  ([#120](https://github.com/cstheorist/tarn/issues/120)).
- `tla/NativeHandback.tla` models the contract between the SQL server and
  the native procedures that commit writes: a step served as the Lua
  procedure would make it, or handed back unchanged and never once its
  commit began, and no call sent again after a lost connection. TLC must
  fail each of its three protection switches
  ([#120](https://github.com/cstheorist/tarn/issues/120)).
- The native storage procedures read and write MessagePack with a codec
  verified with Verus (`crates/tarn-verified/src/mp.rs`). Its writers emit
  Tarantool's shortest encodings, its readers read them back, and its
  `skip` follows any nesting. No function can panic or read past its
  buffer, whatever bytes it is given
  ([#120](https://github.com/cstheorist/tarn/issues/120)).

- Lazy secondary indexes ([#82](https://github.com/cstheorist/tarn/issues/82)):
  `COMMENT 'tarn:lazy'` on a non-unique key creates an asynchronous global
  copy while retaining the strong local index. Eligible nonlocking
  autocommit SELECTs fetch and recheck current base rows, refill pages
  after stale candidates and can opt into bounded session catch-up.
  Durable commit-atomic outboxes, reserved PREPARE capacity, versioned
  delivery and collection floors survive retry, restart and bucket moves.
  Bootstrap uses a bounded write fence; TRUNCATE rotates index generations.
  See [the consistency and operational limits](docs/lazy-indexes.md).

- Native storage procedures
  ([#28](https://github.com/cstheorist/tarn/issues/28)): `crates/tarn-storage`
  builds a Tarantool C module, `libtarn_storage.so`, that the storage nodes
  load (`TARN_NATIVE`; by default the release build of the checkout). Its
  `tarn_storage.ts_select` serves the reads of memtx tables that have
  nothing to wait for -- point reads, the rows equal to a key, and ranges
  from a key, to an end key or not, autocommit or inside a transaction --
  without running Lua, and hands every other read back to `tarn_select`.
  Its `tarn_storage.ts_autocommit` makes the autocommit writes to memtx
  tables (`INSERT`, `REPLACE`, and `UPDATE` and `DELETE` by primary key),
  on shards with replicas or without, when nothing would make
  `tarn_autocommit` wait or refuse them, and hands the others to
  `tarn_autocommit` unwritten. Its `tarn_storage.ts_commit_local` commits
  the transactions that wrote memtx tables on one shard, in the
  transaction's stream (whose reads it commits, as `tarn_commit_local`
  does), and hands the others back with the stream's transaction as it
  was. Its `tarn_storage.ts_validate` validates and commits read-only
  transactions in their streams, and `ts_select` also serves the first read
  of a stream, which begins its transaction.
  On one shard, point selects reach a quarter to over a third more
  statements a second, the transactions of `oltp_read_only`,
  `oltp_read_write` and `oltp_write_only` half again as many or more, and
  autocommit writes 71% to 76% more (on a shard of three replicas, about a
  third more), while the shard's CPU per statement falls to 29% to 50% of
  what the Lua procedures take, and those transactions run no Lua. `bench/profile.py` (run by `bench/profile.sh`) measures
  what the storage side of a workload costs a shard's transaction thread:
  CPU and Lua allocations per statement, and where the time goes.

- Rebalancing ([#25](https://github.com/cstheorist/tarn/issues/25)): `TARN
  REBALANCE [BY SIZE | BY LOAD] [MAX MOVES n] [MAX RATE bytes]` moves
  buckets in the background, in rounds, from the heaviest active shard to
  the lightest until the shards are even -- in the bytes of their rows, or
  in the requests to their buckets, which the shards count -- and hold even
  shares of the buckets, with at most n moves at a time and a limit of
  bytes per second. One runs at a time; `TARN REBALANCE PAUSE`, `RESUME`
  and `CANCEL` control it, `SHOW TARN REBALANCE`, `SHOW TARN MOVES` and
  `information_schema.TARN_MOVES` show it, and another SQL server takes it
  over if its server stops. A schema change or a change of the shards that
  waits for moves now holds new moves back. `tests/growshrink.sh` grows a
  cluster from two shards to four and shrinks it to three under the bank
  workloads of `txntest.py` and `rctest.py`.

- Shards leave a running cluster
  ([#25](https://github.com/cstheorist/tarn/issues/25)): `TARN REMOVE SHARD
  id` marks the shard draining (no bucket moves to it, and participants
  refuse it as a decision shard), moves its buckets to the active shards
  eight at a time, waits until no participant needs it to learn an outcome,
  and marks it removed on every shard. SQL servers then leave it out, also
  when they learn of it by finding it unreachable, and start without it when
  their configuration still lists it. `tests/jointest.sh` removes a shard
  under load and stops its storage.

- Shards join a running cluster
  ([#25](https://github.com/cstheorist/tarn/issues/25)): `TARN ADD SHARD id
  'replica,...'` checks the new shard's storage, creates every table there,
  gives it a bucket map with no buckets and records the new membership on
  every shard; buckets then move to it with `TARN MOVE BUCKETS`, and `SHOW
  TARN SHARDS` lists the shards. SQL servers learn new shards from the
  shards, with no restart or change of configuration. Every shard keeps the
  membership's version: a transaction that read every shard names the
  version it was routed with when it commits, as does a read of every shard
  outside a transaction, and shards with a newer one refuse them (it may
  have missed a shard that joined since; model-checked, `Join*` and
  `Bug_JoinNoCheck*` configurations). `tests/jointest.sh` adds a shard
  under the workload of `tests/sql/movetest.py`, `tests/joinfaults.sh`
  kills the SQL server driving a join (another server gives it up or
  finishes it), and `scripts/cluster.sh add-shard` starts a new shard's
  storage.

- Online bucket moves ([#24](https://github.com/cstheorist/tarn/issues/24)):
  `TARN MOVE BUCKET n TO SHARD id` and `TARN MOVE BUCKETS BETWEEN n AND m TO
  SHARD id` move buckets between shards while transactions run, to add a
  shard, remove one or take load off one; `SHOW TARN BUCKETS` shows the map
  and the moves in progress. The shards keep the bucket map, made by the
  first SQL server on a new cluster (or on one from an earlier version);
  requests name the buckets they were routed by, a shard refuses those it
  does not own, and the server reads the map again and runs the statement
  again. A move copies the bucket while the old shard serves it, fences it,
  waits for the prepared transactions on its rows, copies the last writes
  and switches; another server finishes a move whose server stopped. The
  protocol is model-checked (`Move_*` configurations, three `Bug_Move*`
  ones that TLC must fail) and `tarn_verified::route::build_owner`, which
  checks the shards' reports, is proven in Verus. `tests/sql/movetest.py`
  moves buckets under transfers, READ COMMITTED read-modify-writes and
  scans of every shard (also during `tests/failover.sh`), and
  `tests/movefaults.sh` kills the SQL server driving a move.

- The number of buckets is chosen when a cluster is made
  ([#24](https://github.com/cstheorist/tarn/issues/24)): `bucket_count` in
  `cluster.json`, now 30,000 by default (it was 3,000), so that the shards
  of a large cluster can be balanced to about 1%. The first SQL server
  makes the map with it, and the map keeps it. A cluster made by an earlier
  version keeps the count its rows were stored with: 3,000, or the
  `bucket_count` it set. Two servers making the map at once agree on one
  count. `SHOW STATUS` reports it (`Tarn_buckets`), and the `kvbench`
  example routes by the shards' map.

- Table trees ([#85](https://github.com/cstheorist/tarn/issues/85)): a
  table's `COMMENT 'tarn:parent=<table>'` declares that its rows nest under
  the parent's. `CREATE TABLE`, `ALTER TABLE` and `DROP TABLE` check that
  each child's primary key starts with its parent's, with key types that
  hash alike, and that the tree is sharded by the root's key, and report
  what does not line up as warnings; `SET tarn_tree_checks = 'ERROR'` refuses
  such statements instead. `tarn-server --check-schema FILE` runs the checks
  over a schema file without a cluster. `SHOW WARNINGS`, `SHOW ERRORS`,
  `@@warning_count` and the OK packet's warning count report a statement's
  warnings.

- An ordered read with `LIMIT` over an index whose equality prefix does not
  fix the shard key pages that index on every shard and merges the shards'
  pages ([#86](https://github.com/cstheorist/tarn/issues/86)), instead of
  reading every matching row of every shard and sorting them: each shard
  reads `LIMIT` + `OFFSET` rows in index order (backwards for `DESC`), and
  only a shard whose page the merge used up is asked for more. `OFFSET`,
  residual filters, the transaction's own writes, READ COMMITTED's one
  committed state per range and serializable range recording (per shard, up
  to the last row it returned) work as on one shard. The pager retains one
  routing snapshot across refill rounds, so a bucket move or membership change cannot mix pages from different maps.
- A join reads its inner table for a batch of outer rows at once
  ([#84](https://github.com/cstheorist/tarn/issues/84)): when the inner side
  is reached by equality on an index key or key prefix (`Point`, `Prefixes`
  or an unbounded equality prefix), the distinct keys of up to 256 outer
  rows go to their shards in one `tarn_select_keys` call per shard, in
  parallel (to every shard for a key without the shard key; through the
  global index for a unique key without it), and the rows are matched back
  to their outer rows in memory. Join order, `LEFT JOIN` padding, NULL keys,
  duplicates and residual `ON` / `WHERE` predicates are as before; in a
  transaction each key enters the read set as its separate lookup did, and
  the transaction's own rows are merged into every outer row's lookup. A
  READ COMMITTED batch longer than one storage page reads its outer rows'
  lookups one by one, as before. DECIMAL lookup parts, and global lookups
  whose base primary key contains DECIMAL, keep per-row reads to preserve
  exact native key matching. All keys in a batch use one routing snapshot.
  `SHOW STATUS` reports `Tarn_storage_reads`, attempted index-scan and
  multi-key page reads, including failed or discarded attempts.

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
  `n_prefix` ([#81](https://github.com/cstheorist/tarn/issues/81)). With
  300,000 rows under one tenant, a page of its 112,726 open rows reads 204
  rows instead of all of them. `SHOW STATUS` reports
  `Tarn_path_estimates`. On vinyl the estimate samples the ordered index's
  first page (`4 * want` rows) to measure what paging would read, and
  counts the prefix only up to that
  ([#93](https://github.com/cstheorist/tarn/issues/93)): that page takes
  about 2 ms there instead of 15-19 ms.

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
  Serializable reads record only the part of the range they read. With
  300,000 rows under one key prefix, the first page used to read all of
  them. `SHOW STATUS` reports `Tarn_rows_read`.
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

- Co-located primary-prefix joins ([#83](https://github.com/cstheorist/tarn/issues/83))
  use one bounded storage call when their full shard key is bound and their
  join equalities prove identical native hash inputs. The shard reads child
  prefixes derived from earlier primary-key rows; Rust retains the original
  INNER/LEFT joins, filters and projection. Autocommit and READ COMMITTED
  reads share one snapshot and prepared-write/DDL lock-history check across
  all tables, including empty probes. Reads across yields also retry if DDL
  holds change or their cache is rebuilt, so a released hold cannot hide a
  TRUNCATE. Serializable streams retain their MVCC transaction and record
  every prefix. Buffered writes, locking reads,
  unsupported accesses and exhausted shared reply budgets use the existing
  executor. Focused TLA+ workloads and broken-guard controls check the
  grouped-read assumptions; native memtx, vinyl and mixed-engine tests cover
  snapshot consistency, locks, paging bounds and routing.

### Changed

- The native storage procedures check locks key by key
  ([#28](https://github.com/cstheorist/tarn/issues/28)): a read with the
  lock check, a commit or a validation goes to Lua only when a prepared
  transaction holds a W lock on what it read (a key it read, or a key in a
  range it read), and a write only when a lock is on a key it writes or a
  range lock's range holds one -- no longer whenever its table has any
  lock. The native module keeps a copy of the lock index, which the Lua
  module changes in the same step as the index; its counts, the checks on
  them and the decision whether a key lies in a range are verified with
  Verus (`crates/tarn-verified/src/locks.rs`). `TARN_CHECK_LOCKS` makes
  the Lua module compare the copy with the index after each change; the
  storage tests, the SQL suites and the failover tests in CI run with it.
  On two shards under `oltp_write_only` (16 threads), the native procedures
  now commit 99.9% of the one-shard transactions, where they handed 93% to
  Lua, and the transactions per second rise 10%. Next to cross-shard
  transactions that read ranges, one-shard autocommit writes go to Lua
  0.4% of the time, where 37% did (`oltp_update_non_index` next to
  `oltp_read_write`: 28% more transactions per second), and the validations
  of read-only transactions next to writers 0.4-1.7% of the time, where
  53-81% did.
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
  ranges. Counting a 300,000-row prefix takes 85 ms instead of 305 ms with
  a residual condition, and 0.2 ms instead of 330 ms without one.
  `SHOW STATUS` reports them as `Tarn_storage_counts`.
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

- Database administration matches MariaDB affected-row counts and result
  metadata ([#149](https://github.com/cstheorist/tarn/issues/149)):
  CREATE DATABASE reports one row for a new database and zero for an
  existing IF NOT EXISTS target. SHOW DATABASES LIKE filters
  `information_schema` too, and includes the pattern in its column name.

- Quoted administrative SQL tokens preserve UTF-8
  ([#142](https://github.com/cstheorist/tarn/issues/142)): SHOW, USE,
  savepoints and quoted variable names retain multibyte characters.
  Doubled quotes and string backslash escapes are decoded without changing
  the original byte offsets used for SET expressions. Unterminated quoted
  tokens return syntax error 1064.

- A duplicate PREPARE on a participant without an open stream could vote
  YES from an unconfirmed record, then lose that promise when the original
  PREPARE rolled back. It now reads confirmed participant state while
  retaining the read set of an existing stream. Quorum-overlap and stream
  continuity controls cover both engines and both native modes.

- An expired PREPARE whose stream is gone is rejected before a fresh
  NO/tombstone can be created. After a committed transaction's records
  were collected, that earlier refusal could resurrect an aborted decision
  over committed history. Runtime controls retain the committed row and
  verify both the absent-history and collected-history cases.
- Hexadecimal literals (`X'4142'`, the form in which PyMySQL 1.2.3 sends
  bytes), strings with the `_binary` or a UTF-8 introducer (`_binary 'ab'`,
  `_binary X'4142'`, `_utf8mb4 'ab'`) and national strings (`N'ab'`) failed
  in text queries with 1064 or 1235. They are binary strings, or text ones,
  as in MariaDB: typed as such, compared as such, and named by their text
  (`SELECT X'41'` is a column `X'41'`). An odd number of digits, or another
  character, is 1064, as in MariaDB. A binary string is coercible: a column's
  collation, or an explicit one, wins over it, so a case-insensitive column
  compared with `X'41'` compares as text; between two literals the binary
  one wins, also in `LIKE` and `LOCATE`. A prepared statement executed with
  a binary parameter is planned for it. `0x123` is `0x0123`
  ([#122](https://github.com/cstheorist/tarn/issues/122)).
- A cached plan served statements whose literals were of another kind: after
  `SELECT 1`, `SELECT 'abc'` came back as an integer column, which PyMySQL
  could not convert. Plans are cached per kind of each parameter too
  ([#122](https://github.com/cstheorist/tarn/issues/122)).
- A local commit or PREPARE of the reads of a stream that began before the
  shard's leader was demoted and elected again is refused, as a validation
  is: the stream may have missed writes of the leader in between. They
  relied on MVCC there, and on Tarantool electing no node that misses a
  confirmed write. Both also refuse reads whose stream's transaction no
  longer exists, which they committed in a new transaction
  ([#120](https://github.com/cstheorist/tarn/issues/120)).
- `tarn_validate` checks that the node has been the leader since the
  stream began right before its probe write. It checked before reading the
  lock index again, which may yield: a node demoted meanwhile could still
  validate ([#28](https://github.com/cstheorist/tarn/issues/28)).
- A transaction whose row lock request a shard refused without granting
  anything (`NOWAIT`, a lock wait timeout) left row-lock state there when
  it then rolled back, or committed without that shard: the SQL server now
  releases the row locks of every shard its lock requests went to
  ([#28](https://github.com/cstheorist/tarn/issues/28)).
- An `UPDATE` in a transaction that set a `FLOAT` or `DOUBLE` column to the
  value it held counted the row as changed when that value was an integer.
  The shards store such values as integers, which the SQL server now reads
  as doubles ([#28](https://github.com/cstheorist/tarn/issues/28)).
- An autocommit read on a shard's new leader (of a range, of several keys,
  or a count) now reads the tables held by a `TRUNCATE` or `DROP TABLE`
  from storage first, as writes do, and so does a validation. Until a write
  read them, such a read could return rows of a table that a prepared
  `TRUNCATE` or `DROP TABLE` held, instead of waiting for it, and a
  validation could accept reads of that table
  ([#28](https://github.com/cstheorist/tarn/issues/28)).
- A `DROP TABLE` whose APPLY failed on a shard after dropping the table, and
  ran again, did not fail the open transactions that had read the table
  there (vinyl, which keeps them open): now it does
  ([#28](https://github.com/cstheorist/tarn/issues/28)).
- An OK packet's info string, such as `PREPARE`'s "Statement prepared",
  is length-encoded, as MySQL and MariaDB write it. MariaDB's client
  checked the length and failed every `PREPARE` with 2027 (malformed
  packet) ([#61](https://github.com/cstheorist/tarn/issues/61)).
- Indexes on instant-added non-NULL fills now include existing rows
  ([#70](https://github.com/cstheorist/tarn/issues/70)). PREPARE appends missing
  suffixes behind a persistent write fence, preserving present values,
  explicit NULLs and older frozen fills. Durable original-length undo
  restores failed changes before releasing the fence, including duplicate
  UNIQUE builds; a later ALTER cannot inherit an aborted default. Recovery
  and generation checks protect retries and takeover, and serialized ALTER
  RPCs prevent late native index builds from escaping cleanup. The existing
  row-count limit and unweighted CI-column retrofit restriction remain.
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

- An index's `COMMENT` (in `CREATE TABLE`, `ALTER TABLE ... ADD INDEX` and
  `CREATE INDEX`) is stored, kept by `ALTER TABLE`, and shown by
  `SHOW CREATE TABLE` and `SHOW INDEX`, so dump/reload preserves it
  ([#80](https://github.com/cstheorist/tarn/issues/80)). `CREATE INDEX`
  decodes MySQL string escapes; `SHOW CREATE TABLE` escapes quotes,
  backslashes, NUL, newline and carriage return as MariaDB does.

- A storage node whose Lua heap was left full of garbage near the limit
  failed every call, its recovery fiber's too, with "not enough memory"
  until a manual `collectgarbage()`: LuaJIT starts a collection cycle only
  when the heap reaches twice what the last cycle left, which was above the
  limit. A fiber now checks the heap every 100 ms and collects it when less
  than the read reserve (1/16 of the limit, at least 16 MiB) is free, at
  most once a second; `tarn_info` counts these in `heap_collects`
  ([#73](https://github.com/cstheorist/tarn/issues/73)).

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
