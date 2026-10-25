# SQL compatibility

Tarn speaks the MySQL client/server protocol and parses SQL with
[sqlparser](https://crates.io/crates/sqlparser) in its MySQL dialect. It
covers what OLTP applications and sysbench use, plus joins, aggregates,
subqueries without outer references, and `UNION`, at two isolation levels:
strictly serializable (the default) and READ COMMITTED as InnoDB implements
it. Behaviour is checked against MariaDB 10.11 by a differential test (236
cases, compared exactly) and by 2,000 randomly generated queries, the
locking behaviour of READ COMMITTED by concurrent tests, and online schema
changes by a test that alters a table hundreds of times while two SQL
servers write to it (`tests/sql/`).

## Protocol

| Feature | Support |
|---|---|
| Handshake | HandshakeV10, `mysql_native_password` (clients asking for `caching_sha2_password` are switched), `CLIENT_DEPRECATE_EOF` |
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
  Joins are nested loops with index lookups on the inner table.
* Index access: equality and range predicates on an index prefix, and
  multi-key lookups for `col IN (...)`, `OR`s of equalities and row-value
  `IN` on any index (one storage call per shard, rows in index order). An
  `OR` whose branches use different indexes becomes a union of lookups,
  like MySQL's `index_merge`.
* Aggregates: `COUNT(*)`, `COUNT([DISTINCT] x)`, `SUM`, `AVG`, `MIN`, `MAX`,
  `ANY_VALUE`, `GROUP_CONCAT([DISTINCT] ... [ORDER BY ...] [SEPARATOR s])`.
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
  0, a `REPLACE` of an existing row counts 2).
* `AUTO_INCREMENT`, `LAST_INSERT_ID()`.

### Definitions

* `CREATE DATABASE`, `DROP DATABASE`, `USE`.
* `CREATE TABLE` with column types (below), `NOT NULL`, `DEFAULT`,
  `AUTO_INCREMENT`, `PRIMARY KEY`, `UNIQUE [KEY]`, `KEY` / `INDEX` (with
  `VISIBLE` / `INVISIBLE`), `FOREIGN KEY` (recorded, not enforced, below),
  the table options `AUTO_INCREMENT = n`, `COMMENT` and `ENGINE`, and the
  shard key clause `PARTITION BY KEY(col, ...) [PARTITIONS n]` (the
  partition count is ignored: rows are spread over the cluster's buckets).
  `ENGINE = memtx` keeps the table in RAM, `ENGINE = vinyl` on disk (an LSM
  tree with a RAM cache, for data larger than RAM); `MEMORY` means memtx,
  and the engines MySQL names (`InnoDB`, `MyISAM`, ...) mean the cluster's
  default (`--default-engine`, memtx unless set); any other name fails with
  1286. One transaction may span tables of both engines. Other table
  options (`CHARSET`, `ROW_FORMAT`, ...) are accepted and ignored.
* `ALTER TABLE`, online (below), `RENAME TABLE`, `CREATE [UNIQUE] INDEX`,
  `DROP INDEX ... ON`, `DROP TABLE`, `TRUNCATE TABLE`.
* A table without a `PRIMARY KEY` is keyed as InnoDB keys it: by its first
  `UNIQUE` key whose columns are all `NOT NULL` (which keeps its name), or
  else by a hidden row id, `DB_ROW_ID`, drawn from a counter, which `SELECT
  *`, `INSERT` without a column list, `DESCRIBE`, `SHOW INDEX` and `SHOW
  CREATE TABLE` leave out and statements cannot name. No column may be
  called `DB_ROW_ID` (1166).
* Every unique key must include the shard key (by default the primary key),
  so that uniqueness is enforced on one shard.
* `CREATE` / `DROP TABLE` and `CREATE` / `DROP DATABASE` are not
  transactional; another SQL server sees their result within its catalog
  poll (0.3 s), and a statement that meets an older catalog there is run
  again with the new one.

#### ALTER TABLE

Schema changes do not rewrite rows and do not block reads; writes to the
table wait while the change is prepared (the statement is retried
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
| `ADD [UNIQUE] INDEX / KEY`, `DROP INDEX`, `RENAME INDEX`, `ALTER INDEX ... VISIBLE / INVISIBLE` | indexes are built on every shard before the change takes effect; duplicates fail it with 1062. On a vinyl table the build runs before writes are held, so they wait only for the switch at the end; meanwhile a new unique index already refuses rows that would break it (1062), as in TiDB |
| `ADD [CONSTRAINT] FOREIGN KEY`, `DROP FOREIGN KEY`, `DROP CONSTRAINT` | recorded and shown, not enforced; an index on the columns is added as MySQL does |
| `RENAME [TO]`, `AUTO_INCREMENT = n`, `COMMENT` | yes |
| `ENGINE` | the table's own engine, or one MySQL names (`InnoDB`, ...): accepted and ignored; moving a table between memtx and vinyl rewrites every row: refused (1235) |
| `[DEFAULT] CHARSET`, `ROW_FORMAT`, `ALGORITHM`, `LOCK`, `FORCE`, `DISABLE` / `ENABLE KEYS`, `CHECK` constraints | accepted and ignored |
| Not supported (1235) | conversions that need every row rewritten (`INT` to `VARCHAR`, a different `DECIMAL` scale, `VARCHAR` to `DATETIME`), changing or dropping the primary key, `FULLTEXT` / `SPATIAL` indexes, partition maintenance, invisible columns |

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
  VARIABLES | STATUS | WARNINGS | ENGINES | COLLATION | CHARACTER SET |
  PROCESSLIST`, `DESCRIBE t`, and `SHOW TARN STATUS` (Tarn's own counters).
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
| `CHAR`, `VARCHAR`, `TEXT` | `string` | binary comparison (see below) |
| `BINARY`, `VARBINARY`, `BLOB` | `varbinary` | |
| `DATE`, `DATETIME`, `TIMESTAMP`, `TIME`, `YEAR` | `string` / `integer` | MySQL text formats |
| `JSON` | `string` | stored as text, not validated or indexed |

## Expressions and functions

* Operators: arithmetic (`+ - * / DIV %`), comparison, `AND OR NOT XOR`,
  `BETWEEN`, `IN (...)`, row values `(a, b) IN ((1, 2), ...)`, `LIKE`,
  `IS [NOT] NULL`, `CASE`, `CAST(x AS SIGNED | UNSIGNED | CHAR | DECIMAL(p, s) | DOUBLE | DATE | DATETIME | BINARY)`.
  NULL semantics follow MySQL.
* Functions: `CONCAT`, `CONCAT_WS`, `LENGTH`, `CHAR_LENGTH`, `LOWER`,
  `UPPER`, `SUBSTRING`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `LEFT`, `RIGHT`,
  `LPAD`, `RPAD`, `REPEAT`, `LOCATE`, `ASCII`, `HEX`, `ABS`, `CEIL`,
  `FLOOR`, `ROUND`, `MOD`, `SIGN`, `SQRT`, `POW`, `RAND`, `GREATEST`,
  `LEAST`, `COALESCE`, `IFNULL`, `NULLIF`, `IF`, `NOW`, `CURDATE`,
  `UNIX_TIMESTAMP`, `DATABASE`, `VERSION`, `CONNECTION_ID`,
  `LAST_INSERT_ID`, `USER`, `FOUND_ROWS`, `ROW_COUNT`.

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
* **Collation:** string comparison, ordering, grouping and unique indexes
  are binary (like `utf8mb4_bin`), so `'ALICE' = 'alice'` is false.
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
