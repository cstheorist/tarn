# SQL compatibility

Tarn speaks the MySQL client/server protocol and parses SQL with
[sqlparser](https://crates.io/crates/sqlparser) in its MySQL dialect. It
covers what OLTP applications and sysbench use, plus joins, aggregates,
subqueries without outer references, and `UNION`. Behaviour is checked
against MariaDB 10.11 by a differential test (99 cases, compared exactly)
and by 2,000 randomly generated queries (`tests/sql/`).

## Protocol

| Feature | Support |
|---|---|
| Handshake | HandshakeV10, `mysql_native_password` (clients asking for `caching_sha2_password` are switched), `CLIENT_DEPRECATE_EOF` |
| Users | from the `--user NAME:PASSWORD` flags, or any user with `--allow-all` / no `--user` |
| Text protocol | `COM_QUERY` with full result sets |
| Binary protocol | `COM_STMT_PREPARE`, `EXECUTE` (all parameter types), `SEND_LONG_DATA`, `CLOSE`, `RESET` |
| Other commands | `COM_PING`, `COM_INIT_DB`, `COM_FIELD_LIST`, `COM_STATISTICS`, `COM_SET_OPTION`, `COM_RESET_CONNECTION`, `COM_QUIT` |
| Not supported | TLS, `COM_CHANGE_USER`, multi-statement packets, server-side cursors (`COM_STMT_FETCH`) |
| Limits | 64 MiB per packet and per prepared statement's long data (64 KiB before authentication); 16,382 prepared statements per connection |

## Statements

### Queries

* `SELECT` with `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT` /
  `OFFSET` (both syntaxes), `DISTINCT`, column aliases, ordinal `ORDER BY 2`.
* Joins: `[INNER] JOIN`, `LEFT [OUTER] JOIN`, comma joins, `ON` / `USING`.
  Joins are nested loops with index lookups on the inner table.
* Aggregates: `COUNT(*)`, `COUNT([DISTINCT] x)`, `SUM`, `AVG`, `MIN`, `MAX`,
  `ANY_VALUE`, `GROUP_CONCAT([DISTINCT] ... [ORDER BY ...] [SEPARATOR s])`.
* Subqueries without outer references: `x IN (SELECT ...)`, `EXISTS (SELECT ...)`, scalar
  `(SELECT ...)`, in `SELECT`, `UPDATE` and `DELETE`. The inner query runs
  first, inside the statement's transaction.
* `UNION` and `UNION ALL`, with `ORDER BY` / `LIMIT` on the result.
* `SELECT ... FOR UPDATE` (accepted; every transaction is serializable anyway).
* `SELECT @@variable`, `SELECT DATABASE()`, `SELECT VERSION()`, and other
  constant queries.

### Data modification

* `INSERT ... VALUES` (multi-row), `INSERT ... SELECT`, `INSERT IGNORE`,
  `INSERT ... ON DUPLICATE KEY UPDATE` (with `VALUES(col)`), `REPLACE`.
* Single-table `UPDATE` and `DELETE`, with `WHERE`, `ORDER BY` and `LIMIT`.
* Affected-row counts follow MySQL (an `UPDATE` that changes nothing counts
  0, a `REPLACE` of an existing row counts 2).
* `AUTO_INCREMENT`, `LAST_INSERT_ID()`.

### Definitions

* `CREATE DATABASE`, `DROP DATABASE`, `USE`.
* `CREATE TABLE` with column types (below), `NOT NULL`, `DEFAULT`,
  `AUTO_INCREMENT`, `PRIMARY KEY`, `UNIQUE [KEY]`, `KEY` / `INDEX`, and the
  shard key clause `PARTITION BY KEY(col, ...) [PARTITIONS n]` (the partition
  count is ignored: rows are spread over the cluster's buckets).
* `CREATE [UNIQUE] INDEX`, `DROP TABLE`, `TRUNCATE TABLE`.
* Every table needs a primary key, and every unique key must include the
  shard key (by default the primary key), so that uniqueness is enforced on
  one shard.
* DDL is not transactional and assumes no concurrent DDL on the same table.

### Transactions and sessions

* `BEGIN`, `START TRANSACTION`, `COMMIT`, `ROLLBACK`, `SET autocommit = 0|1`.
* `SET` of session variables is accepted (`SET NAMES`, `SET SESSION ...`);
  `SET TRANSACTION ISOLATION LEVEL ...` is accepted and ignored: every
  transaction is strictly serializable.
* `SHOW DATABASES | TABLES | [FULL] COLUMNS | CREATE TABLE | INDEX | TABLE STATUS |
  VARIABLES | STATUS | WARNINGS | ENGINES | COLLATION | CHARACTER SET |
  PROCESSLIST`, `DESCRIBE t`, and `SHOW TARN STATUS` (Tarn's own counters).
* `LOCK`, `UNLOCK`, `FLUSH`, `ANALYZE`, `OPTIMIZE`, `CHECKSUM`, `DO` are
  accepted and do nothing.

## Types

| SQL type | Stored in Tarantool as | Notes |
|---|---|---|
| `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT` (`UNSIGNED` too) | `integer` / `unsigned` | range-checked on write (error 1264) |
| `BOOL`, `BOOLEAN`, `TINYINT(1)` | `integer` | |
| `FLOAT`, `DOUBLE` | `double` | |
| `DECIMAL(p, s)` | `decimal` | exact |
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

* **Isolation:** strict serializability for every transaction (MySQL's
  default is REPEATABLE READ). Conflicting transactions abort with 1213
  (`ER_LOCK_DEADLOCK`, SQLSTATE 40001); autocommit statements are retried
  internally, explicit transactions should be retried by the client, as with
  any serializable database. Long cross-shard read-only transactions can be
  starved by heavy concurrent writes to what they read.
* **Outcome unknown:** if the connection to a shard breaks while a commit is
  in flight, the statement fails with 1180 and SQLSTATE 40003: it may or may
  not have been applied. Tarn never re-sends such a write.
* **Collation:** string comparison, ordering, grouping and unique indexes
  are binary (like `utf8mb4_bin`), so `'ALICE' = 'alice'` is false.
* **AUTO_INCREMENT:** values are allocated in blocks per SQL server; they are
  unique and increasing per server, with gaps across servers (as in TiDB).
* **Transaction lifetime:** a transaction must commit within 120 s
  (`max_txn_age`) of its first statement.
* **Hash sharding:** range predicates on the shard key and lookups through
  non-unique secondary indexes are sent to every shard.

## Not supported

Subqueries that reference the outer query, derived tables (`FROM (SELECT
...)`), views, stored procedures and functions, triggers, events, foreign
keys, savepoints, `ALTER TABLE`, window functions, common table expressions,
`RIGHT` / `FULL` / `NATURAL` joins, multi-table `UPDATE` / `DELETE`,
`CREATE TABLE ... AS` / `LIKE`, user management statements, and online
resharding. Unsupported syntax fails with error 1235
(`ER_NOT_SUPPORTED_YET`) or a parse error.
