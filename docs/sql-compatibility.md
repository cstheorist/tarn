# SQL compatibility

Tarn speaks the MySQL client/server protocol and parses SQL with
[sqlparser](https://crates.io/crates/sqlparser) in its MySQL dialect. It
covers what OLTP applications and sysbench use, plus joins, aggregates,
subqueries without outer references, and `UNION`, at two isolation levels:
strictly serializable (the default) and READ COMMITTED as InnoDB implements
it. Behaviour is checked against MariaDB 10.11 by a differential test (465
cases, compared exactly) and by 2,000 randomly generated queries, the
locking behaviour of READ COMMITTED by concurrent tests, and online schema
changes by a test that alters a table hundreds of times while two SQL
servers write to it (`tests/sql/`).

Non-unique indexes explicitly declared with `COMMENT 'tarn:lazy'` also
provide an eventual global copy for eligible nonlocking autocommit SELECTs.
Current base rows and the complete predicate are rechecked; recently
matching rows may be missing. Explicit transactions, locking reads, DML
and unsupported shapes retain strong execution. Session-only
`tarn_lazy_read_your_writes` (OFF by default) and
`tarn_lazy_wait_timeout` (integer seconds 0..60, default 5) control causal
catch-up; an incomplete attempted catch-up returns 1205 and retains its
history. See [the full contract and supported identities](lazy-indexes.md).

## Protocol

| Feature | Support |
|---|---|
| Handshake | HandshakeV10, `mysql_native_password` (clients asking for `caching_sha2_password` are switched), `CLIENT_DEPRECATE_EOF`, `CLIENT_FOUND_ROWS` |
| Users | from the `--user NAME:PASSWORD` flags, or any user with `--allow-all` / no `--user` |
| Text protocol | `COM_QUERY` with full result sets |
| Binary protocol | `COM_STMT_PREPARE`, `EXECUTE` (all parameter types), `SEND_LONG_DATA`, `CLOSE`, `RESET` |
| Multi-statements | `CLIENT_MULTI_STATEMENTS`, or `COM_SET_OPTION` (as Rails and other drivers turn it on): one result per statement, and the first error ends the batch |
| Other commands | `COM_PING`, `COM_INIT_DB`, `COM_FIELD_LIST`, `COM_STATISTICS`, `COM_RESET_CONNECTION`, `COM_QUIT` |
| Not supported | TLS, `COM_CHANGE_USER`, server-side cursors (`COM_STMT_FETCH`) |
| Limits | 64 MiB per packet and per prepared statement's long data (64 KiB before authentication); 16,382 prepared statements per connection |

## Statements

### Queries

* `SELECT` with `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT` /
  `OFFSET` (both syntaxes), `DISTINCT`, column aliases, ordinal `ORDER BY 2`.
* Joins: `[INNER] JOIN`, `LEFT [OUTER] JOIN`, comma joins, `ON` / `USING`.
  Joins are nested loops with index lookups on the inner table. When the
  inner table is reached by equality on an index key or key prefix, the
  lookups of up to 256 outer rows are read together: one storage call per
  shard they touch, in parallel, instead of one call per outer row.
  DECIMAL lookup parts and global lookups whose primary key contains
  DECIMAL retain per-row native lookups for exact key matching.
  Co-located primary-prefix INNER/LEFT joins with a fully bound common
  shard key can read their dependent table prefixes in one bounded request.
  This requires matching native key types/collations and no participating
  buffered writes; unsupported/locking paths or shared-budget overflow use
  the ordinary executor. Filters, projections and LEFT padding retain SQL
  semantics.
* Index access: equality (including `IS NULL` and `<=>`) and range
  predicates on an index prefix, and
  multi-key lookups for `col IN (...)`, `OR`s of equalities and row-value
  `IN` on any index (one storage call per shard, rows in index order). An
  `OR` whose branches use different indexes becomes a union of lookups,
  like MySQL's `index_merge`. With `LIMIT`, an index whose columns after
  the equality prefix match `ORDER BY` (all ascending, or all descending: the
  index is then read backwards) is preferred, and the single-table
  read then goes through it a page at a time, filtering as it goes, and
  stops once `LIMIT` (+ `OFFSET`) rows qualify. When another index fixes a
  longer equality prefix, the shard counts both ranges first (one call: an
  index count on memtx; on vinyl a sample of the ordered index's first page
  and a count of the prefix up to what paging would read) and the read
  pages the ordered index when that reads fewer rows than reading the
  longer prefix whole and sorting it. When `WHERE` fixes no shard key, the
  ordered index is paged on every shard and the shards' pages are merged,
  so each shard reads about `LIMIT` + `OFFSET` rows instead of every match.
* Aggregates: `COUNT(*)`, `COUNT([DISTINCT] x)`, `SUM`, `AVG`, `MIN`, `MAX`,
  `ANY_VALUE`, `GROUP_CONCAT([DISTINCT] ... [ORDER BY ...] [SEPARATOR s])`.
  `COUNT(*)`, `COUNT(constant)` and `COUNT(col)` of one table without
  `GROUP BY` are counted by the shards, which return one number each
  instead of the rows, when the `WHERE` clause is an index range plus
  comparisons, `IN` lists and `IS [NOT] NULL` tests of integer and
  character columns against constants, compared by bytes (an equality
  prefix with no other condition is counted by the index itself); other
  counts, including comparisons under a case-insensitive collation, read
  the rows.
* Subqueries without outer references: `x IN (SELECT ...)`, `EXISTS (SELECT ...)`, scalar
  `(SELECT ...)`, in `SELECT`, `UPDATE` and `DELETE`. The inner query runs
  first, inside the statement's transaction.
* `UNION` and `UNION ALL`, with `ORDER BY` / `LIMIT` on the result.
* Locking reads: `FOR UPDATE`, `FOR SHARE` / `LOCK IN SHARE MODE`, each with
  `NOWAIT` or `SKIP LOCKED` (see [Isolation and locking](#isolation-and-locking)).
* `SELECT @@variable`, `SELECT DATABASE()`, `SELECT VERSION()`, and other
  constant queries.

### Data modification

* `INSERT ... VALUES` (multi-row), `INSERT ... SELECT`, `INSERT IGNORE`,
  `INSERT ... ON DUPLICATE KEY UPDATE` (with `VALUES(col)`, or MySQL 8's row
  alias `VALUES (...) AS new [(c1, ...)] ... UPDATE c = new.c`), `REPLACE`; a
  row that collides on a unique key other than the primary key is the one
  `ON DUPLICATE KEY UPDATE` updates and `REPLACE` deletes.
* Single-table `UPDATE` and `DELETE`, with `WHERE`, `ORDER BY` and `LIMIT`.
* Affected-row counts follow MySQL (an `UPDATE` that changes nothing counts
  0, a `REPLACE` of an existing row counts 2). With `CLIENT_FOUND_ROWS`, an
  `UPDATE` counts the rows it matched and an upsert that changes nothing
  counts 1, as MySQL does for the drivers that set it (Rails, Go's
  `clientFoundRows`).
* `AUTO_INCREMENT`, `LAST_INSERT_ID()`.

### Definitions

* `CREATE DATABASE`, `DROP DATABASE`, `USE`.
* `CREATE TABLE` with column types (below), `NOT NULL`, `DEFAULT`,
  `AUTO_INCREMENT`, `PRIMARY KEY`, `UNIQUE [KEY]`, `KEY` / `INDEX` (with
  `VISIBLE` / `INVISIBLE` and `COMMENT`), `FOREIGN KEY` (recorded, not enforced, below),
  the table options `AUTO_INCREMENT = n`, `COMMENT` and `ENGINE`, and the
  shard key clause `PARTITION BY KEY(col, ...) [PARTITIONS n]` (the
  partition count is ignored: rows are spread over the cluster's buckets).
  `ENGINE = memtx` keeps the table in RAM, `ENGINE = vinyl` on disk (an LSM
  tree with a RAM cache, for data larger than RAM); `MEMORY` means memtx,
  and the engines MySQL names (`InnoDB`, `MyISAM`, ...) mean the cluster's
  default (`--default-engine`, memtx unless set); any other name fails with
  1286. One transaction may span tables of both engines. `[DEFAULT]
  CHARSET` and `COLLATE` set the table's default collation
  ([Collations](#collations)); other table options (`ROW_FORMAT`, ...) are
  accepted and ignored.
  Vinyl writes use shared storage admission during dump/compaction
  pressure; refusal, a fresh attempt deadline or a rolled-back native quota
  wait returns 1205. Large batches may need splitting. See
  [the limits and telemetry](operations.md#vinyl-write-admission).
* `ALTER TABLE`, online (below), `RENAME TABLE`, `CREATE [UNIQUE] INDEX`,
  `DROP INDEX ... ON`, `DROP TABLE`, `TRUNCATE TABLE`.
* A table without a `PRIMARY KEY` is keyed as InnoDB keys it: by its first
  `UNIQUE` key whose columns are all `NOT NULL` (which keeps its name), or
  else by a hidden row id, `DB_ROW_ID`, drawn from a counter, which `SELECT
  *`, `INSERT` without a column list, `DESCRIBE`, `SHOW INDEX` and `SHOW
  CREATE TABLE` leave out and statements cannot name. No column may be
  called `DB_ROW_ID` (1166).
* A unique key that does not include the shard key (by default the primary
  key) is enforced across shards through a global index, maintained in the
  same transaction as the rows: writes to such a table touch one more shard
  per such key. `ALTER TABLE ... ADD UNIQUE` builds one from the existing
  rows (writes to the table wait meanwhile; a value two rows hold fails it
  with 1062; above a million rows per shard it is refused, 1846) and `DROP INDEX` drops it; renaming one or changing its
  columns is refused (1846). Lookups by the full key read through it.
* `CREATE` / `DROP TABLE` and `CREATE` / `DROP DATABASE` are not
  transactional; another SQL server sees their result within its catalog
  poll (0.3 s), and a statement that meets an older catalog there is run
  again with the new one.

#### ALTER TABLE

Ordinary instant schema changes keep existing tuples and do not block reads.
Indexing an omitted non-NULL fill appends the missing tuple suffix. Writes
to the table wait while the change is prepared (the statement is retried
internally, up to the lock wait timeout, like a wait for MySQL's metadata
lock). Writes made with the old definition after that point are refused
by the shards and retried with the new one, so no SQL server can write in
an old layout. A transaction that wrote the table before the change fails
with 1412 (`ER_TABLE_DEF_CHANGED`, "Table definition has changed, please
retry transaction") at its next statement on the table, or at COMMIT; at
SERIALIZABLE, one that read the table is aborted by the change (1213 at its
next read or at COMMIT). One schema change runs at a time; one that a crashed server
left half done is finished by the next schema change or server start.

| Alteration | Support |
|---|---|
| `ADD [COLUMN]` (`FIRST`, `AFTER col`, several at once) | instant; existing rows get the default (or the type's zero value for `NOT NULL` without a default, as MySQL fills them); not `AUTO_INCREMENT` |
| `DROP [COLUMN]` | instant (the stored values stay, unused); not primary key or shard key columns, nor columns of a foreign key (1828 / 1553); indexes lose the column |
| `MODIFY`, `CHANGE` | type changes within one family (integers, `CHAR`/`VARCHAR`/`TEXT`/`JSON`, `DECIMAL` with the same scale, `DATETIME`/`TIMESTAMP`, binary strings, floats), `NULL` / `NOT NULL`, defaults, position, name. Existing rows are checked against the narrower type (1264 / 1265, as MariaDB reports them); writes wait while they are checked, so a change that needs the check is refused (1846) when a shard holds more than a million rows of the table (`tarn.cfg.alter_check_rows`) |
| `RENAME COLUMN`, `ALTER [COLUMN] SET DEFAULT / DROP DEFAULT` | instant |
| `ADD [UNIQUE] INDEX / KEY` (with `COMMENT`), `DROP INDEX`, `RENAME INDEX`, `ALTER INDEX ... VISIBLE / INVISIBLE` | indexes are built on every shard before the change takes effect; duplicates fail it with 1062. An index on an instant-added non-NULL fill materializes missing fields first; see below. Other vinyl builds run before writes are held, so they wait only for the switch at the end; meanwhile a new unique index already refuses rows that would break it (1062), as in TiDB |
| `ADD [CONSTRAINT] FOREIGN KEY`, `DROP FOREIGN KEY`, `DROP CONSTRAINT` | recorded and shown, not enforced; an index on the columns is added as MySQL does |
| `RENAME [TO]`, `AUTO_INCREMENT = n`, `COMMENT` | yes |
| `ENGINE` | `ENGINE=memtx` / `ENGINE=vinyl` moves the whole table online using a replacement space, with a final fence for catch-up and index construction; see [Online engine changes](operations.md#online-engine-changes). An actual engine change must run separately from other schema changes (1235). Requesting the current engine is a no-op; MySQL engine names such as `InnoDB` remain accepted without changing the engine |
| `[DEFAULT] CHARSET`, `COLLATE` | the default collation of columns added later |
| `CONVERT TO CHARACTER SET utf8mb4 [COLLATE c]`, `MODIFY` / `CHANGE` to another collation | for columns no index orders by; changing the collation of an indexed column would reorder its index: refused (1235) |
| `ADD INDEX` (or a foreign key's index) on a case-insensitive column | for a column that is already in an index (it has a weight field, see [Collations](#collations)) or that the same statement adds; on any other existing case-insensitive column its rows would have to be rewritten: refused (1235) |
| `ROW_FORMAT`, `ALGORITHM`, `LOCK`, `FORCE`, `DISABLE` / `ENABLE KEYS`, `CHECK` constraints | accepted and ignored |
| Not supported (1235) | conversions that need every row rewritten (`INT` to `VARCHAR`, a different `DECIMAL` scale, `VARCHAR` to `DATETIME`), changing or dropping the primary key, `FULLTEXT` / `SPATIAL` indexes, partition maintenance, invisible columns |

An instant-added column's **fill** is its value for rows written before the
column existed. It stays fixed when `SET DEFAULT` changes the default for new
rows. Before indexing a non-NULL fill, Tarn appends missing fields in bounded
transactions behind a persistent write fence. Existing physical values and
explicit NULLs stay unchanged. This also supports a column and its index in
one statement, composite indexes, implicit foreign-key indexes and known
collation weight fields. The existing restriction on retrofitting a weight
field to an older unweighted CI column remains 1235.

A duplicate logical key fails with 1062. Durable undo restores tentative
suffixes before releasing the fence, so an aborted ALTER cannot poison a
later column that uses the slot with another default. Recovery resumes the
recorded commit or abort. Empty tables and NULL fills need no rewriting;
checks and materialization retain the `tarn.cfg.alter_check_rows` limit
(one million rows per shard, otherwise 1846). Vinyl builds that need
materialization run behind the fence; ordinary vinyl index builds remain
online. Older storage still rejects missing indexed fields with 1846.

#### Table trees

A table can declare that its rows belong to a parent table's rows with
`COMMENT 'tarn:parent=<table>'` (or `<database>.<table>`; the token may
share the comment with other text). MySQL and MariaDB store the comment and
ignore it, so the schema stays portable; foreign keys cannot say the same
there, because neither accepts them on partitioned tables. A tree keeps a
root row and all of its descendants on one shard when every child's primary
key starts with its parent's whole primary key and every table is sharded
by the root's shard key:

```sql
CREATE TABLE customer (customer_id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(100));
CREATE TABLE orders (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL,
  PRIMARY KEY (customer_id, order_id)) COMMENT 'tarn:parent=customer' PARTITION BY KEY (customer_id);
CREATE TABLE order_line (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, line_no INT NOT NULL,
  PRIMARY KEY (customer_id, order_id, line_no)) COMMENT 'tarn:parent=orders' PARTITION BY KEY (customer_id);
```

`CREATE TABLE`, `ALTER TABLE` and `DROP TABLE` check the trees they touch
and report what does not line up as warnings (1105, `SHOW WARNINGS`):

* the parent does not exist, or the parent links form a cycle. A dump may
  create a child before its parent, so a missing parent is only a warning,
  and the parent's `CREATE TABLE` checks the children that name it;
* the child's primary key does not start with its parent's primary-key
  columns (the same names, in order), or one of them would not hash and
  compare like the parent's: integers need the same signedness, text the
  same collation, other types the same type (binary strings always match);
* the child's shard key is not the root's, usually a missing `PARTITION BY
  KEY`;
* a note for each unique key outside the shard key: its global index adds a
  shard to writes that change it.

An `ALTER TABLE` reports only what it makes worse. `SET tarn_tree_checks =
'ERROR'` makes the findings about the table a statement creates or alters
errors that refuse it (a missing parent stays a warning); `'OFF'` turns the
checks off; `'WARN'` is the default. `tarn-server --check-schema FILE
[--database DB]` runs the same checks over a schema file, such as a
`mysqldump --no-data` dump, without a cluster.

#### Loading dumps

Files written by `mysqldump` load with the `mysql` client or as one
multi-statement batch: executable comments `/*!NNNNN ... */` are run up to
MySQL 8.0.40's version and later ones and MariaDB's `/*M! ... */` are
skipped; `SET @saved = @@var` / `SET var = @saved`, `SET NAMES`,
`TIME_ZONE = '+00:00'`, `SQL_MODE` (with `NO_AUTO_VALUE_ON_ZERO`),
`FOREIGN_KEY_CHECKS`, `UNIQUE_CHECKS` and `SQL_NOTES` are accepted; `LOCK
TABLES` commits the open transaction and takes no lock, `UNLOCK TABLES` and
`ALTER TABLE ... DISABLE KEYS` do nothing.

### Transactions and sessions

* `BEGIN`, `START TRANSACTION`, `COMMIT`, `ROLLBACK`, `SET autocommit = 0|1`.
* `SAVEPOINT name`, `ROLLBACK [WORK] TO [SAVEPOINT] name`, `RELEASE SAVEPOINT
  name` (error 1305 for an unknown name). Rolling back to a savepoint undoes
  the writes made since and keeps the row locks taken since, as InnoDB does.
* Statement-level rollback: a statement that fails inside a transaction
  leaves none of its writes behind; the transaction goes on, except after a
  deadlock (1213), which rolls the whole transaction back.
* Isolation level and lock wait timeout, per session or globally: see
  [Isolation and locking](#isolation-and-locking). `sql_mode` honours
  `NO_AUTO_VALUE_ON_ZERO` (Tarn is always strict); `time_zone` may be set to
  UTC (`'+00:00'`, `'SYSTEM'`), and the connection character set to
  `utf8mb4` or a subset of it (`SET NAMES`). Other session variables are
  accepted and reported back by `SELECT @@var`.
* User-defined variables: `SET @v = expr` (also `:=`), `SELECT ... INTO @a,
  @b` (before `FROM` or at the end; more than one row is 1172, none leaves
  the variables as they were), and `@v` anywhere in a statement (not in
  prepared statements, nor assigned inside a `SELECT` with `:=`).
* SQL-level prepared statements: `PREPARE name FROM 'text'` or `FROM @var`,
  `EXECUTE name [USING @a, ...]` (the variables' values replace the `?`
  markers in order), `DEALLOCATE PREPARE name` and `DROP PREPARE name`.
  They belong to the session; an unknown name is 1243, a wrong number of
  variables 1210, and `PREPARE`, `EXECUTE` and `DEALLOCATE` cannot be
  prepared themselves (1295). The text is re-planned at each `EXECUTE`
  through the plan cache, as a text query would be.
* `SHOW DATABASES | TABLES | [FULL] COLUMNS | CREATE TABLE | INDEX | TABLE STATUS |
  VARIABLES | STATUS | WARNINGS | ERRORS | ENGINES | COLLATION | CHARACTER SET |
  PROCESSLIST`, `DESCRIBE t`, and `SHOW TARN STATUS` (Tarn's own counters) and `SHOW TARN RPCS`
  (cached bounded storage-request admission and connection observations).
  `SHOW WARNINGS` lists the last statement's warnings and notes, whose number
  the OK packet and `@@warning_count` report; the only ones Tarn raises are
  those of the [table tree](#table-trees) checks.
* `information_schema.TABLES`, with MySQL 8's columns: one row per table
  (`TABLE_TYPE` `BASE TABLE`, `ENGINE` memtx or vinyl, `TABLE_COMMENT`) and
  one for itself (`SYSTEM VIEW`). Sizes, row counts and times are NULL. It
  is read from the catalog, so it takes no locks and needs no transaction
  (`FOR UPDATE` on it alone is a plain read); writing it is 1044, and the
  other `information_schema` tables are 1109.
* `UNLOCK`, `FLUSH`, `ANALYZE`, `OPTIMIZE`, `CHECKSUM`, `DO` are accepted
  and do nothing; `LOCK TABLES` commits the open transaction.

## Isolation and locking

| Setting | Statements |
|---|---|
| Session level | `SET [SESSION] transaction_isolation = 'READ-COMMITTED'` (also `tx_isolation`, `@@SESSION.`), `SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| Next transaction only | `SET TRANSACTION ISOLATION LEVEL ...`, `SET @@transaction_isolation = ...` (error 1568 inside a transaction) |
| Default for new sessions | `SET GLOBAL ...` (same forms), `--isolation` / the `isolation` config key; initially `SERIALIZABLE` |
| Lock wait timeout | `innodb_lock_wait_timeout`: session or global, whole seconds, clamped to 0..1073741824 as in MariaDB (0: do not wait); `--lock-wait-timeout` / `lock_wait_timeout`; default 50 |

`READ UNCOMMITTED` and `READ COMMITTED` run as READ COMMITTED; `REPEATABLE
READ` and `SERIALIZABLE` run strictly serializable (stronger than asked).
`@@transaction_isolation` reports the level that was set.

**SERIALIZABLE** (strictly serializable): transactions are optimistic.
Reads are validated at commit; a transaction whose reads changed, or that
would overwrite a row another transaction has locked, aborts with 1213
(`ER_LOCK_DEADLOCK`, SQLSTATE 40001). Autocommit statements are retried
internally; explicit transactions should be retried by the client.
`FOR UPDATE` / `FOR SHARE` additionally take row locks (below), and the rows
read stay validated at commit.

**READ COMMITTED**, as in InnoDB:

* A plain `SELECT` sees the latest committed data as of the statement (and
  the transaction's own writes). It takes no locks and never causes an
  abort; it does not wait for row locks.
* Writes and locking reads take row locks, held until the transaction ends:
  exclusive for `UPDATE`, `DELETE`, `INSERT`, `REPLACE`, upserts and `FOR
  UPDATE`, shared for `FOR SHARE` / `LOCK IN SHARE MODE`. A row is locked by
  its primary key; `INSERT`, `REPLACE` and upserts also lock each unique-key
  value they insert, so concurrent inserts of one value wait for each other
  and the loser sees the winner's row (1062, the `ON DUPLICATE KEY UPDATE`
  branch, or nothing for `INSERT IGNORE`) or proceeds if the winner rolls
  back.
* A conflicting request waits in a queue. It fails with 1205
  (`ER_LOCK_WAIT_TIMEOUT`) after `innodb_lock_wait_timeout`, which rolls
  back the statement only; with `NOWAIT` it fails at once with 3572
  (`ER_LOCK_NOWAIT`), also keeping the transaction; with `SKIP LOCKED` the
  row is left out and the scan goes on until `LIMIT` rows are returned.
* Deadlocks are detected: a wait that would close a cycle on one shard
  fails at once, a cycle through several shards within about a second; the
  victim gets 1213 and its transaction is rolled back.
* `UPDATE` and `DELETE` read each row again under its lock and re-check
  `WHERE` on that version, so `SET n = n - 1` always starts from the newest
  committed value, and a row that stopped matching is left alone (InnoDB's
  "semi-consistent" read). There are no gap locks, as in InnoDB at READ
  COMMITTED: a row that starts to match while a statement runs may be missed.
* Autocommit statements wait for row locks too. (At SERIALIZABLE, an
  autocommit statement that has to read before it writes, such as `UPDATE
  ... SET a = a * 2` or one with a multi-row `WHERE`, retries for a fraction
  of a second and then fails with 1213 instead.)
* Commits stay atomic across shards and durable on a quorum, and a commit is
  visible to every statement that starts after it was acknowledged.
* Row locks live in the memory of each shard's leader. A leader change, or a
  lost connection between an SQL server and a shard, releases them; a
  transaction that lost locks it held is rolled back at its next statement
  or at commit (1213).

## Types

| SQL type | Stored in Tarantool as | Notes |
|---|---|---|
| `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT` (`UNSIGNED` too) | `integer` / `unsigned` | range-checked on write (error 1264) |
| `BOOL`, `BOOLEAN`, `TINYINT(1)` | `integer` | |
| `FLOAT`, `DOUBLE` | `double` | |
| `DECIMAL(p, s)` | `decimal` | exact; precision checked on write (1264) |
| `CHAR`, `VARCHAR`, `TEXT` | `string` | compared under the column's collation ([below](#collations)) |
| `BINARY`, `VARBINARY`, `BLOB` | `varbinary` | |
| `DATE`, `DATETIME[(fsp)]`, `TIMESTAMP[(fsp)]`, `TIME`, `YEAR` | `string` / `integer` | MySQL text formats; dates and datetimes are stored in one spelling per value (`YYYY-MM-DD HH:MM:SS` with exactly `fsp` fractional digits, rounded as MySQL 8 rounds), so they sort, group and index in time order, and a constant compared with such a column compares by value: `'2025-01-01 00:00:01'` equals a stored `'2025-01-01 00:00:01.000000'` |
| `JSON` | `string` | stored as text, not validated or indexed |

## Collations

Text compares, sorts, groups and is unique under its collation, as in
MariaDB 10.11. Tarn implements four collations exactly, from MariaDB's own
weight tables, and `utf8mb4_bin` with one difference:

| Collation | Compares |
|---|---|
| `utf8mb4_bin` | bytes; **trailing spaces count** (`'a' <> 'a '`), unlike MySQL's PAD SPACE `_bin` |
| `utf8mb4_general_ci` | one weight per character: case- and accent-insensitive (`'é' = 'E'`, `'ß' = 's'`), characters above U+FFFF all equal; trailing spaces ignored |
| `utf8mb4_unicode_ci` | UCA 4.0.0 primary weights (`'ß' = 'ss'`, `'œ' = 'oe'`, ignorable characters ignored); trailing spaces ignored |
| `utf8mb4_general_nopad_ci`, `utf8mb4_unicode_nopad_ci` | as above, trailing spaces count |

Other names map to the closest of these and are shown as declared:
`utf8_*` / `utf8mb3_*` to their utf8mb4 counterparts; `_bin`, `_cs` and
`_as_cs` collations to `utf8mb4_bin`; MySQL 8's `utf8mb4_0900_ai_ci` to
`utf8mb4_unicode_nopad_ci`; the other UCA-based `_ci` collations
(`unicode_520`, `uca1400`, language variants) to `utf8mb4_unicode_ci`; the
remaining `_ci` collations (`latin1_swedish_ci`, ...) to
`utf8mb4_general_ci`. These approximations differ from the originals on the
characters their tables weigh differently. An unknown name fails with 1273.

* **Columns** take the collation they declare (`COLLATE c`, or the default
  collation of `CHARACTER SET cs`: `utf8mb4_general_ci` for utf8mb4), else
  the table's (`DEFAULT CHARSET` / `COLLATE` options), else the server's:
  `--default-collation`, `utf8mb4_bin` unless set (`@@collation_server`).
  Tables created before collations existed are `utf8mb4_bin`.
  `CREATE DATABASE ... COLLATE` is accepted and ignored.
* **Literals** take `collation_connection`: the collation the client's
  handshake asks for (most drivers ask for `utf8mb4_general_ci`), changed by
  `SET NAMES cs [COLLATE c]` and `SET collation_connection`.
* **Comparisons** (`=`, `<`, `IN`, `BETWEEN`, `LIKE`, `CASE`, `NULLIF`,
  `GREATEST`, `LEAST`, `LOCATE`), `ORDER BY`, `GROUP BY`, `DISTINCT`,
  `MIN`/`MAX`, `COUNT(DISTINCT)`, `UNION` and joins use MySQL's coercibility
  rules: an explicit `COLLATE` wins over a column, a column over a literal;
  between two columns, a `_bin` one wins, and two other different
  collations fail with 1267 (Illegal mix of collations). `LIKE` matches
  character by character by weight, with no padding.
* **Indexes and unique keys** order and compare by the collation, so a
  unique key refuses `'a'` next to `'A'` under a `_ci` collation, lookups
  and ranges through it find every equal value, and a case-insensitive
  primary key routes `'a'` and `'A'` to the same shard (an indexed
  case-insensitive column is stored with its weight string,
  [design §1.3](design.md#13-collations)).
* `COLLATION()`, `COERCIBILITY()`, `CHARSET()` and `WEIGHT_STRING()` report
  what Tarn uses.
* Which of several equal values `DISTINCT`, `GROUP BY`, `MIN` and `MAX`
  return (`'Alice'` or `'ALICE'`) depends on the order rows are read in,
  which across shards is not MySQL's.
* A string ending in a character below the space (a tab, a newline) sorts
  after the same string without it; MySQL sorts it before.
* User variables are replaced by literals before a statement is planned:
  `@v` takes `collation_connection` and a literal's coercibility, where
  MySQL keeps the collation it was assigned with at a column's
  coercibility. Compared with a column of another case-insensitive
  collation, `@v` takes the column's, where MySQL fails with 1267.

## Expressions and functions

* Operators: arithmetic (`+ - * / DIV %`), comparison, `AND OR NOT XOR`,
  `BETWEEN`, `IN (...)`, row values `(a, b) IN ((1, 2), ...)` and
  `(a, b) = | <> | < | <= | > | >= | <=> (x, y)` (lexicographic; a keyset
  cursor `(a, b) > (?, ?)` on an index starts its range at `a`), `LIKE`,
  `IS [NOT] NULL`, `CASE`, `CAST(x AS SIGNED | UNSIGNED | CHAR | DECIMAL(p, s) | DOUBLE | DATE | DATETIME | BINARY)`.
  NULL semantics follow MySQL.
* Functions: `CONCAT`, `CONCAT_WS`, `LENGTH`, `CHAR_LENGTH`, `LOWER`,
  `UPPER`, `SUBSTRING`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `LEFT`, `RIGHT`,
  `LPAD`, `RPAD`, `REPEAT`, `LOCATE`, `ASCII`, `HEX`, `ABS`, `CEIL`,
  `FLOOR`, `ROUND`, `MOD`, `SIGN`, `SQRT`, `POW`, `RAND`, `GREATEST`,
  `LEAST`, `COALESCE`, `IFNULL`, `NULLIF`, `IF`, `NOW`, `CURDATE`,
  `UNIX_TIMESTAMP`, `DATABASE`, `VERSION`, `CONNECTION_ID`,
  `LAST_INSERT_ID`, `USER`, `FOUND_ROWS`, `ROW_COUNT`, `COLLATION`,
  `COERCIBILITY`, `CHARSET`, `WEIGHT_STRING`; `expr COLLATE name`.

## Differences from MySQL

* **Isolation:** the default is strict serializability (MySQL's default is
  REPEATABLE READ, which Tarn runs strictly serializable). Conflicting
  serializable transactions abort with 1213 (`ER_LOCK_DEADLOCK`, SQLSTATE
  40001); autocommit statements are retried internally, explicit
  transactions should be retried by the client, as with any serializable
  database. Long cross-shard read-only transactions can be starved by heavy
  concurrent writes to what they read. READ COMMITTED behaves as in InnoDB
  ([above](#isolation-and-locking)), without gap locks or next-key locks.
* **`UPDATE` that changes nothing:** at SERIALIZABLE it writes and locks
  nothing (InnoDB still locks the rows it matched).
* **Outcome unknown:** if the connection to a shard breaks while a commit is
  in flight, the statement fails with 1180 and SQLSTATE 40003: it may or may
  not have been applied. Tarn never re-sends such a write.
* **Collations** ([above](#collations)): the server's default is
  `utf8mb4_bin` (MySQL's is case-insensitive) unless `--default-collation`
  says otherwise; `utf8mb4_bin` counts trailing spaces; collations other
  than `utf8mb4_bin`, `utf8mb4_general_ci` and `utf8mb4_unicode_ci` (and
  their NO PAD forms) compare like the closest of those.
* **AUTO_INCREMENT:** values are allocated in blocks per SQL server; they are
  unique and increasing per server, with gaps across servers (as in TiDB).
* **Transaction lifetime:** a serializable transaction must commit within
  120 s (`max_txn_age`) of its first statement; READ COMMITTED transactions
  have no such limit.
* **Hash sharding:** range predicates on the shard key and lookups through
  non-unique secondary indexes are sent to every shard.
* **Foreign keys** are recorded (`SHOW CREATE TABLE`, `DROP FOREIGN KEY`)
  but not enforced, as in MyISAM or TiDB before 6.6: `ON DELETE CASCADE`
  does not cascade and a missing parent row is not an error. `CHECK`
  constraints are accepted and not enforced either.
* **ENUM and SET** columns are stored as text without checking the value
  list.

## Not supported

Subqueries that reference the outer query, derived tables (`FROM (SELECT
...)`), views, stored procedures and functions, triggers, events, enforced
foreign keys, `ALTER TABLE` changes that rewrite rows (above), window
functions, common table expressions, `RIGHT` / `FULL` / `NATURAL` joins,
multi-table `UPDATE` / `DELETE`, `CREATE TABLE ... AS` / `LIKE`,
`information_schema` other than `TABLES`, user management statements, and online resharding. Unsupported syntax fails with error 1235
(`ER_NOT_SUPPORTED_YET`) or a parse error.
