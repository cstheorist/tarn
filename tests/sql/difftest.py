#!/usr/bin/env python3
"""Differential SQL tests: run the same statements on Tarn and on a reference
MySQL-compatible server and compare what comes back.

    tests/sql/difftest.py --tarn 127.0.0.1:4000 --ref 127.0.0.1:3407 [-v]

Every case is executed on both servers in a fresh database. Result sets are
compared as multisets unless the query has ORDER BY (then in order); numbers
are compared numerically (DECIMAL vs DOUBLE representation differences are
tolerated), strings exactly. DML is compared by affected-row counts and by a
dump of the table afterwards. Errors must occur on both sides (codes are
reported when they differ but only the presence of an error is required).
"""
import argparse
import datetime
import decimal
import re
import sys

import pymysql

SCHEMA = [
    "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b VARCHAR(20), c DECIMAL(10,2), d DOUBLE, e DATETIME, KEY ka (a))",
    "CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT, name VARCHAR(30), qty INT NOT NULL DEFAULT 0, KEY k1 (t1_id))",
    "CREATE TABLE t3 (k1 INT, k2 VARCHAR(10), v INT, PRIMARY KEY (k1, k2))",
    "CREATE TABLE ai (id BIGINT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(10))",
    "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(40), n INT, UNIQUE KEY ue (email, id))",
    "CREATE TABLE sgn (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, u BIGINT UNSIGNED NOT NULL, i INT NOT NULL, "
    "ui INT UNSIGNED NOT NULL DEFAULT 0)",
    # a unique key apart from the primary key (both contain the shard key t)
    "CREATE TABLE uk (t INT, id INT, name VARCHAR(10), n INT, PRIMARY KEY (t, id), UNIQUE KEY tn (t, name)) "
    "PARTITION BY KEY(t)",
    # schema changes (issue #16)
    "CREATE TABLE al (id INT PRIMARY KEY, a INT, b VARCHAR(10), c DECIMAL(6,2), KEY ka (a))",
    "CREATE TABLE al2 (t INT, id INT, name VARCHAR(10), PRIMARY KEY (t, id)) PARTITION BY KEY(t)",
    "CREATE TABLE fkc (id INT PRIMARY KEY, p INT)",
]


def seed():
    s = []
    rows = []
    for i in range(1, 41):
        a = "NULL" if i % 7 == 0 else str(i % 5)
        b = "NULL" if i % 11 == 0 else f"'name{i % 9}'"
        c = f"{(i * 37) % 1000 / 10:.2f}"
        d = f"{i * 1.5}"
        e = f"'2024-0{1 + i % 9}-{10 + i % 18} 1{i % 10}:0{i % 6}:00'"
        rows.append(f"({i}, {a}, {b}, {c}, {d}, {e})")
    s.append("INSERT INTO t1 (id, a, b, c, d, e) VALUES " + ", ".join(rows))
    rows = [f"({i}, {1 + (i * 3) % 45}, 'item{i}', {i % 4})" for i in range(1, 61)]
    s.append("INSERT INTO t2 (id, t1_id, name, qty) VALUES " + ", ".join(rows))
    rows = [f"({i % 6}, 'k{i}', {i * 10})" for i in range(1, 25)]
    s.append("INSERT INTO t3 VALUES " + ", ".join(rows))
    s.append("INSERT INTO sgn (id, n, u, i) VALUES (1, 0, 0, 0), (2, 5, 5, 2147483647), "
             "(3, -9223372036854775807, 18446744073709551615, -2147483648)")
    s.append("INSERT INTO al VALUES (1, 5, 'one', 1.5), (2, NULL, 'two-two', 22.25), (3, 300, NULL, -3), (4, 7, 'four', 0)")
    s.append("INSERT INTO al2 VALUES (1, 1, 'x'), (1, 2, 'x'), (2, 1, 'x')")
    return s


# (sql, kind) where kind is 'q' (query) or 'dml' (followed by table dumps) or 'err'
CASES = [
    # point and range access
    ("SELECT * FROM t1 WHERE id = 7", "q"),
    ("SELECT id, a, b FROM t1 WHERE id IN (1, 5, 9, 100)", "q"),
    ("SELECT id FROM t1 WHERE id BETWEEN 10 AND 20", "q"),
    ("SELECT id FROM t1 WHERE id > 35", "q"),
    ("SELECT id FROM t1 WHERE id >= 5 AND id < 9 ORDER BY id DESC", "q"),
    ("SELECT id, c FROM t1 WHERE id BETWEEN 3 AND 30 ORDER BY c LIMIT 5", "q"),
    ("SELECT id FROM t1 ORDER BY id LIMIT 5 OFFSET 10", "q"),
    ("SELECT id FROM t1 ORDER BY id LIMIT 10, 3", "q"),
    ("SELECT DISTINCT a FROM t1", "q"),
    ("SELECT DISTINCT b FROM t1 WHERE id < 20 ORDER BY b", "q"),
    ("SELECT id, a FROM t1 WHERE a = 3", "q"),
    ("SELECT id FROM t1 WHERE a IS NULL", "q"),
    ("SELECT id FROM t1 WHERE b IS NOT NULL AND a <> 2", "q"),
    ("SELECT id FROM t1 WHERE a IN (1, 2) AND id > 20", "q"),
    ("SELECT id FROM t1 WHERE a NOT IN (1, 2)", "q"),
    ("SELECT id FROM t1 WHERE b LIKE 'name1%'", "q"),
    ("SELECT id FROM t1 WHERE b NOT LIKE '%3'", "q"),
    ("SELECT id FROM t1 WHERE NOT (a > 2 OR b = 'name2')", "q"),
    ("SELECT k1, k2, v FROM t3 WHERE k1 = 2", "q"),
    ("SELECT k1, k2, v FROM t3 WHERE k1 = 3 AND k2 = 'k9'", "q"),
    ("SELECT k1, k2 FROM t3 WHERE k1 = 1 AND k2 > 'k2' ORDER BY k2", "q"),
    ("SELECT * FROM t3 WHERE (k1, k2) IN ((1, 'k1'), (2, 'k2'))", "q"),
    # IN lists, ORs and row-value IN on secondary and partial keys (issue #10)
    ("SELECT id, a FROM t1 WHERE a IN (1, 3, 3, NULL, 2.5)", "q"),
    ("SELECT id, a FROM t1 WHERE a = 1 OR a = 4 ORDER BY id", "q"),
    ("SELECT id FROM t1 WHERE (a = 1 OR a = 2) AND id > 20", "q"),
    ("SELECT k1, k2, v FROM t3 WHERE k1 IN (1, 3) ORDER BY k1, k2", "q"),
    ("SELECT k1, k2 FROM t3 WHERE (k1 = 1 AND k2 = 'k1') OR (k1 = 3 AND k2 = 'k9') OR k1 = 5", "q"),
    ("SELECT id FROM t2 WHERE t1_id IN (4, 7, 100) OR id = 1", "q"),
    ("SELECT id, a FROM t1 WHERE a IN (2, 3) OR b = 'name4'", "q"),
    ("SELECT t2.id, t1.a FROM t2 JOIN t1 ON t1.id = t2.t1_id WHERE t2.t1_id IN (4, 10) OR t2.id = 3", "q"),
    # expressions and functions
    ("SELECT id, a + 1, a * 2 - id, id / 4, id DIV 3, id % 4, -a FROM t1 WHERE id <= 8", "q"),
    ("SELECT id, c * 2, c + d, d / 3 FROM t1 WHERE id IN (2, 4, 6)", "q"),
    ("SELECT CONCAT(b, '-', id), CONCAT_WS(',', a, b), LENGTH(b), UPPER(b), LOWER('ABC') FROM t1 WHERE id < 5", "q"),
    ("SELECT SUBSTRING(b, 2, 3), LEFT(b, 2), RIGHT(b, 2), TRIM('  x  '), REPLACE(b, 'name', 'N') FROM t1 WHERE id < 4", "q"),
    ("SELECT COALESCE(a, -1), IFNULL(b, 'none'), NULLIF(a, 1), IF(a > 2, 'big', 'small') FROM t1 WHERE id BETWEEN 5 AND 12", "q"),
    ("SELECT id, CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'many' END FROM t1 WHERE id < 10", "q"),
    ("SELECT id, CASE a WHEN 0 THEN 'zero' WHEN 4 THEN 'four' END FROM t1 WHERE id < 10", "q"),
    ("SELECT ABS(-5), CEIL(1.2), FLOOR(-1.2), ROUND(2.567, 2), MOD(17, 5), GREATEST(1, 9, 3), LEAST(4, 2, 8)", "q"),
    ("SELECT 1 + 1, 'a' = 'a', 3 > 2, NULL IS NULL, 1 = NULL, 10 / 4, 7 DIV 2", "q"),
    ("SELECT CAST('42' AS SIGNED), CAST(3.7 AS SIGNED), CAST(5 AS CHAR), '5' + 3", "q"),
    ("SELECT HEX(255), LPAD('7', 3, '0'), RPAD('ab', 4, '*'), REPEAT('xy', 3)", "q"),
    ("SELECT id, e FROM t1 WHERE e > '2024-05-01' ORDER BY e, id LIMIT 6", "q"),
    # aggregates
    ("SELECT COUNT(*) FROM t1", "q"),
    ("SELECT COUNT(a), COUNT(b), COUNT(DISTINCT a) FROM t1", "q"),
    ("SELECT SUM(a), MIN(a), MAX(a), AVG(a) FROM t1", "q"),
    ("SELECT SUM(c), MIN(c), MAX(c) FROM t1", "q"),
    ("SELECT a, COUNT(*) FROM t1 GROUP BY a", "q"),
    ("SELECT a, COUNT(*) AS n, SUM(id) FROM t1 GROUP BY a HAVING n > 7", "q"),
    ("SELECT b, MAX(id) FROM t1 WHERE a IS NOT NULL GROUP BY b ORDER BY b", "q"),
    ("SELECT a, b, COUNT(*) FROM t1 GROUP BY a, b ORDER BY a, b LIMIT 8", "q"),
    ("SELECT COUNT(*) FROM t1 WHERE id BETWEEN 5 AND 25", "q"),
    ("SELECT SUM(qty) FROM t2 WHERE id BETWEEN 1 AND 30", "q"),
    ("SELECT MAX(id) FROM t1 WHERE a = 2", "q"),
    ("SELECT COUNT(*) FROM t1 WHERE id = 1000", "q"),
    ("SELECT SUM(a) FROM t1 WHERE id = 1000", "q"),
    ("SELECT a, GROUP_CONCAT(id ORDER BY id) FROM t1 WHERE id < 15 GROUP BY a", "q"),
    # joins
    ("SELECT t2.id, t2.name, t1.b FROM t2 JOIN t1 ON t1.id = t2.t1_id WHERE t2.id < 10", "q"),
    ("SELECT t1.id, t2.id FROM t1 LEFT JOIN t2 ON t2.t1_id = t1.id WHERE t1.id > 35", "q"),
    ("SELECT t1.a, COUNT(*), SUM(t2.qty) FROM t1 JOIN t2 ON t2.t1_id = t1.id GROUP BY t1.a", "q"),
    ("SELECT x.id, y.id FROM t1 x, t2 y WHERE x.id = y.t1_id AND y.qty = 3 AND x.a = 1", "q"),
    ("SELECT t1.id, t3.v FROM t1 JOIN t3 ON t3.k1 = t1.a WHERE t1.id < 4", "q"),
    # subqueries (uncorrelated) and UNION
    ("SELECT id FROM t1 WHERE id IN (SELECT t1_id FROM t2 WHERE qty = 3)", "q"),
    ("SELECT id FROM t1 WHERE id NOT IN (SELECT t1_id FROM t2) AND id < 25", "q"),
    ("SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE qty = 99)", "q"),
    ("SELECT COUNT(*) FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE qty = 99)", "q"),
    ("SELECT id, (SELECT MAX(qty) FROM t2) AS mq FROM t1 WHERE id < 4", "q"),
    ("SELECT id FROM t1 WHERE a = (SELECT MIN(a) FROM t1)", "q"),
    ("SELECT id FROM t1 WHERE id IN (SELECT t1_id FROM t2 WHERE t1_id IN (SELECT id FROM t1 WHERE a = 2))", "q"),
    ("SELECT id FROM t1 WHERE id < 5 UNION SELECT t1_id FROM t2 WHERE id < 5", "q"),
    ("SELECT a FROM t1 UNION ALL SELECT a FROM t1 WHERE id < 3", "q"),
    ("SELECT id FROM t1 WHERE id < 6 UNION SELECT id FROM t2 WHERE id < 4 ORDER BY id DESC LIMIT 3", "q"),
    ("SELECT id FROM t1 WHERE id = (SELECT id FROM t2)", "err"),
    # errors
    ("SELECT nope FROM t1", "err"),
    ("SELECT * FROM missing_table", "err"),
    ("SELEC 1", "err"),
    ("INSERT INTO t1 (id, a) VALUES (1, 1)", "err"),
    ("INSERT INTO t2 (id, qty) VALUES (500, NULL)", "err"),
    # DML
    ("INSERT INTO t1 (id, a, b) VALUES (100, 1, 'new'), (101, NULL, 'newer')", "dml:t1"),
    ("INSERT IGNORE INTO t1 (id, a, b) VALUES (100, 9, 'dup'), (102, 2, 'x')", "dml:t1"),
    ("REPLACE INTO t1 (id, a, b) VALUES (100, 7, 'replaced'), (103, 3, 'y')", "dml:t1"),
    ("INSERT INTO t1 (id, a, b) VALUES (101, 5, 'z') ON DUPLICATE KEY UPDATE a = a + 10, b = VALUES(b)", "dml:t1"),
    ("INSERT INTO t1 (id, a, b) VALUES (104, 5, 'z') ON DUPLICATE KEY UPDATE a = a + 10", "dml:t1"),
    ("UPDATE t1 SET a = a + 1 WHERE id = 3", "dml:t1"),
    ("UPDATE t1 SET a = 0 WHERE id = 3", "dml:t1"),
    ("UPDATE t1 SET a = a + 1 WHERE id = 7", "dml:t1"),
    ("UPDATE t1 SET a = a + 2147483647 WHERE id = 1", "err"),
    ("UPDATE t1 SET a = a - 1 WHERE id = 2", "dml:t1"),
    ("UPDATE t1 SET b = CONCAT(b, '!'), c = c * 2 WHERE id BETWEEN 10 AND 14", "dml:t1"),
    ("UPDATE t1 SET a = 4 WHERE a = 3", "dml:t1"),
    ("UPDATE t1 SET d = d + 1 WHERE b LIKE 'name5%'", "dml:t1"),
    ("UPDATE t1 SET a = 1 ORDER BY id DESC LIMIT 2", "dml:t1"),
    ("DELETE FROM t1 WHERE id = 5", "dml:t1"),
    ("DELETE FROM t1 WHERE id = 5", "dml:t1"),
    ("DELETE FROM t1 WHERE a IS NULL AND id > 20", "dml:t1"),
    ("DELETE FROM t1 WHERE id > 30 ORDER BY id LIMIT 2", "dml:t1"),
    ("UPDATE t1 SET b = 'sub' WHERE id IN (SELECT t1_id FROM t2 WHERE qty = 1)", "dml:t1"),
    ("DELETE FROM t3 WHERE k1 IN (SELECT a FROM t1 WHERE id = 2)", "dml:t3"),
    ("UPDATE t3 SET v = v + 1 WHERE k1 = 2", "dml:t3"),
    ("DELETE FROM t3 WHERE k1 = 0 AND k2 = 'k6'", "dml:t3"),
    ("DELETE FROM t3 WHERE (k1, k2) IN ((1, 'k7'), (4, 'k4'), (9, 'none'))", "dml:t3"),
    ("UPDATE t3 SET v = v + 1 WHERE k1 IN (2, 4) OR (k1 = 5 AND k2 = 'k5')", "dml:t3"),
    ("INSERT INTO t3 SELECT a, CONCAT('n', id), id FROM t1 WHERE id < 6 AND a IS NOT NULL", "dml:t3"),
    ("INSERT INTO ai (v) VALUES ('a'), ('b'), ('c')", "dml:ai"),
    ("INSERT INTO ai (id, v) VALUES (100, 'd')", "dml:ai"),
    ("INSERT INTO ai (v) VALUES ('e')", "dml:ai"),
    ("INSERT INTO u VALUES (1, 'a@x', 1), (2, 'b@x', 2)", "dml:u"),
    ("INSERT INTO u VALUES (1, 'c@x', 3)", "err"),
    ("UPDATE u SET n = n * 10", "dml:u"),
    ("TRUNCATE TABLE t2", "dml:t2"),
    # integer arithmetic in UPDATE (issue #11): BIGINT / BIGINT UNSIGNED
    # domains (1690) and column ranges (1264), single-row and multi-row
    ("UPDATE sgn SET n = n - 1 WHERE id = 1", "dml:sgn"),
    ("UPDATE sgn SET n = n - 10 WHERE id = 2", "dml:sgn"),
    ("UPDATE sgn SET i = i + 1 WHERE id = 2", "err"),
    ("UPDATE sgn SET i = i - 1 WHERE id = 3", "err"),
    ("UPDATE sgn SET u = u - 1 WHERE id = 1", "err"),
    ("UPDATE sgn SET ui = ui - 1 WHERE id = 1", "err"),
    ("UPDATE sgn SET u = u + 1 WHERE id = 3", "err"),
    ("UPDATE sgn SET n = n - 2 WHERE id = 3", "err"),
    ("UPDATE sgn SET n = n - 1 WHERE id = 3", "dml:sgn"),
    ("UPDATE sgn SET u = u - 5 WHERE id = 3", "dml:sgn"),
    ("UPDATE sgn SET u = u - 1 WHERE id > 0", "err"),
    ("UPDATE sgn SET ui = ui - 1 WHERE id > 1", "err"),
    ("UPDATE sgn SET n = n + 1 WHERE id > 0", "dml:sgn"),
    ("UPDATE sgn SET u = u + 10, ui = ui + 3 WHERE id = 2", "dml:sgn"),
    ("SELECT u - 1 FROM sgn WHERE id = 1", "err"),
    ("SELECT id, u + 0, u * 2, ui - 0, n - 1, i + 1 FROM sgn ORDER BY id", "q"),
    # unique keys other than the primary key: upserts, IGNORE, REPLACE
    ("INSERT INTO uk VALUES (1, 1, 'a', 1), (1, 2, 'b', 1), (2, 1, 'a', 1)", "dml:uk"),
    ("INSERT INTO uk VALUES (1, 3, 'a', 5) ON DUPLICATE KEY UPDATE n = n + VALUES(n)", "dml:uk"),
    ("INSERT IGNORE INTO uk VALUES (1, 4, 'b', 9), (1, 5, 'c', 9)", "dml:uk"),
    ("REPLACE INTO uk VALUES (1, 6, 'b', 7)", "dml:uk"),
    ("REPLACE INTO uk VALUES (1, 0, 'c', 3), (2, 1, 'c', 4)", "dml:uk"),
    ("INSERT INTO uk VALUES (1, 7, 'a', 1)", "err"),
    ("INSERT INTO uk VALUES (1, 1, 'z', 1) ON DUPLICATE KEY UPDATE name = 'b'", "err"),
    ("UPDATE uk SET name = 'a' WHERE t = 1 AND id = 6", "err"),
    # session isolation level, lock wait timeout, locking reads (issue #17);
    # the rest of the cases run at READ COMMITTED on both servers
    ("SET SESSION tx_isolation = 'READ-COMMITTED'", "q"),
    ("SELECT @@tx_isolation, @@session.tx_isolation", "q"),
    ("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", "q"),
    ("SELECT @@tx_isolation", "q"),
    ("SET @@tx_isolation = 'SERIALIZABLE'", "q"),
    ("SELECT @@tx_isolation", "q"),
    ("SET tx_isolation = 'SNAPSHOT'", "err"),
    ("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED", "q"),
    ("SELECT @@tx_isolation", "q"),
    ("SET innodb_lock_wait_timeout = 7", "q"),
    ("SELECT @@innodb_lock_wait_timeout", "q"),
    ("SET innodb_lock_wait_timeout = -1", "q"),
    ("SELECT @@innodb_lock_wait_timeout", "q"),
    ("SET innodb_lock_wait_timeout = 2.5", "err"),
    ("SET innodb_lock_wait_timeout = 50", "q"),
    ("SELECT id, a FROM t1 WHERE id < 4 ORDER BY id LOCK IN SHARE MODE", "q"),
    ("SELECT id FROM t1 WHERE id BETWEEN 3 AND 6 ORDER BY id FOR UPDATE", "q"),
    ("SELECT id FROM t1 WHERE a = 2 ORDER BY id LIMIT 2 FOR UPDATE SKIP LOCKED", "q"),
    ("SELECT id, b FROM t1 WHERE id = 9 FOR UPDATE NOWAIT", "q"),
    ("SELECT t1.id, t2.id FROM t1 JOIN t2 ON t2.t1_id = t1.id WHERE t1.id < 8 ORDER BY t2.id LOCK IN SHARE MODE", "q"),
    ("SELECT COUNT(*) FROM t1 WHERE a = 1 FOR UPDATE", "q"),
    ("UPDATE uk SET n = n * 2 WHERE name IN ('a', 'c')", "dml:uk"),
    ("DELETE FROM uk WHERE n > 10", "dml:uk"),
    # savepoints and statement-level rollback inside a transaction
    ("BEGIN", "q"),
    ("INSERT INTO uk VALUES (3, 1, 'x', 1), (3, 4, 'w', 1)", "dml:uk"),
    ("UPDATE uk SET name = 'x' WHERE t = 3 AND id = 4", "err"),
    ("UPDATE uk SET name = 'v' WHERE t = 3 AND id = 4", "dml:uk"),
    ("SAVEPOINT s1", "q"),
    ("UPDATE uk SET n = n + 100 WHERE t = 1", "dml:uk"),
    ("INSERT INTO uk VALUES (3, 2, 'y', 1), (3, 3, 'x', 1)", "err"),
    ("SELECT * FROM uk ORDER BY t, id", "q"),
    ("SAVEPOINT s2", "q"),
    ("DELETE FROM uk WHERE t = 2", "dml:uk"),
    ("ROLLBACK TO SAVEPOINT s1", "q"),
    ("SELECT * FROM uk ORDER BY t, id", "q"),
    ("ROLLBACK TO s2", "err"),
    ("RELEASE SAVEPOINT s1", "q"),
    ("RELEASE SAVEPOINT s1", "err"),
    ("COMMIT", "dml:uk"),
    ("ROLLBACK TO SAVEPOINT s1", "err"),
    ("SET SESSION tx_isolation = 'REPEATABLE-READ'", "q"),
    # ALTER TABLE, CREATE / DROP INDEX, RENAME TABLE (issue #16); "ddl:t"
    # compares the table's rows, column names and indexes afterwards
    ("ALTER TABLE al ADD COLUMN n INT NOT NULL", "ddl:al"),
    ("ALTER TABLE al ADD COLUMN m VARCHAR(5) DEFAULT 'zz' AFTER id, ADD d DATE, ADD e INT DEFAULT -2 FIRST", "ddl:al"),
    ("INSERT INTO al (id, a, b, c, n) VALUES (5, 1, 'five', 5, 50)", "dml:al"),
    ("UPDATE al SET n = n + 1 WHERE id = 1", "dml:al"),
    ("UPDATE al SET m = 'new', n = n + 2 WHERE a = 7", "dml:al"),
    ("SELECT id, m, n, e FROM al WHERE n > 0 ORDER BY id", "q"),
    ("ALTER TABLE al MODIFY a INT NOT NULL", "err"),
    ("ALTER TABLE al MODIFY b VARCHAR(3)", "err"),
    ("ALTER TABLE al MODIFY a TINYINT", "err"),
    ("ALTER TABLE al MODIFY c DECIMAL(3,2)", "err"),
    ("ALTER TABLE al MODIFY b VARCHAR(20) NOT NULL", "err"),
    ("ALTER TABLE al MODIFY m VARCHAR(2)", "err"),
    ("ALTER TABLE al MODIFY e INT UNSIGNED", "err"),
    ("ALTER TABLE al MODIFY b VARCHAR(20), MODIFY a BIGINT, MODIFY c DECIMAL(9,2)", "ddl:al"),
    ("ALTER TABLE al CHANGE b bb VARCHAR(20) DEFAULT 'x' FIRST", "ddl:al"),
    ("ALTER TABLE al RENAME COLUMN bb TO b2", "ddl:al"),
    ("ALTER TABLE al ALTER COLUMN n SET DEFAULT 9", "ddl:al"),
    ("INSERT INTO al (id, a, c) VALUES (6, 2, 1)", "dml:al"),
    ("ALTER TABLE al ALTER COLUMN n DROP DEFAULT", "ddl:al"),
    ("INSERT INTO al (id, a, c) VALUES (7, 2, 1)", "err"),
    ("ALTER TABLE al DROP COLUMN m, DROP COLUMN e", "ddl:al"),
    ("ALTER TABLE al DROP COLUMN nope", "err"),
    ("ALTER TABLE al ADD COLUMN a INT", "err"),
    ("ALTER TABLE al ADD UNIQUE KEY ua (a, id), ADD INDEX ic (c)", "ddl:al"),
    ("ALTER TABLE al DROP INDEX ka, RENAME INDEX ic TO ic2", "ddl:al"),
    ("ALTER TABLE al ADD INDEX ic2 (n)", "err"),
    ("CREATE INDEX ia ON al (a, n)", "ddl:al"),
    ("ALTER TABLE al DROP COLUMN n", "ddl:al"),
    ("DROP INDEX ia ON al", "ddl:al"),
    ("DROP INDEX ia ON al", "err"),
    ("ALTER TABLE al2 ADD UNIQUE KEY tn (t, name)", "err"),
    ("UPDATE al2 SET name = 'y' WHERE t = 1 AND id = 2", "dml:al2"),
    ("ALTER TABLE al2 ADD UNIQUE KEY tn (t, name)", "ddl:al2"),
    ("INSERT INTO al2 VALUES (1, 3, 'y')", "err"),
    ("ALTER TABLE al ENGINE = InnoDB, ALGORITHM = INPLACE, LOCK = NONE", "ddl:al"),
    ("ALTER TABLE al DISABLE KEYS", "q"),
    ("ALTER TABLE al ENABLE KEYS", "q"),
    ("ALTER TABLE fkc ADD CONSTRAINT fk1 FOREIGN KEY (p) REFERENCES t1 (id)", "ddl:fkc"),
    ("ALTER TABLE fkc DROP COLUMN p", "err"),
    ("ALTER TABLE fkc DROP FOREIGN KEY fk1", "ddl:fkc"),
    ("ALTER TABLE fkc DROP FOREIGN KEY fk1", "err"),
    ("RENAME TABLE fkc TO fkc2", "q"),
    ("SELECT * FROM fkc", "err"),
    ("ALTER TABLE fkc2 RENAME TO fkc3, ADD COLUMN z INT", "q"),
    ("SELECT * FROM fkc3", "q"),
    # user and system variables, executable comments, LOCK TABLES
    ("SET @v = 5, @s = 'it''s', @d = 1.50, @n = NULL", "q"),
    ("SELECT @v, @s, @d, @n, @never_set", "q"),
    ("SELECT id FROM t1 WHERE id < @v ORDER BY id", "q"),
    ("SET @v := @v * 3", "q"),
    ("SELECT @v AS tripled", "q"),
    ("SET @old_mode = @@sql_mode", "q"),
    ("SET sql_mode = CONCAT(@@sql_mode, ',NO_AUTO_VALUE_ON_ZERO')", "q"),
    ("INSERT INTO ai (id, v) VALUES (0, 'zero')", "dml:ai"),
    ("SET sql_mode = @old_mode", "q"),
    ("INSERT INTO ai (id, v) VALUES (0, 'gen')", "q"),
    ("SELECT COUNT(*) FROM ai WHERE id = 0", "q"),
    ("SELECT 1 /*!40101 + 1 */ AS x", "q"),
    ("/*!40101 SET @c = 7 */", "q"),
    ("SELECT @c", "q"),
    ("LOCK TABLES t1 WRITE", "q"),
    ("UNLOCK TABLES", "q"),
    ("INSERT INTO t1 (id, c) VALUES (500, 123456789.5)", "err"),
]


def connect(hp, db=None):
    host, port = hp.split(":")
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db,
                           autocommit=True, charset="utf8mb4")


def norm(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float, decimal.Decimal)):
        return v
    if isinstance(v, bytes):
        try:
            return v.decode()
        except UnicodeDecodeError:
            return v.hex()
    if isinstance(v, (datetime.datetime, datetime.date, datetime.timedelta)):
        return str(v)
    return v


def same(x, y):
    """Exact equality, except that a DOUBLE on either side is compared with
    a relative tolerance of 1e-9. Integers and DECIMALs compare exactly by
    value (5 == Decimal('5.00')); a number never equals a string."""
    num = (int, float, decimal.Decimal)
    if isinstance(x, num) and isinstance(y, num):
        if isinstance(x, float) or isinstance(y, float):
            fx, fy = float(x), float(y)
            return abs(fx - fy) <= 1e-9 * max(1.0, abs(fx), abs(fy))
        return decimal.Decimal(x) == decimal.Decimal(y)
    return type(x) is type(y) and x == y


def rows_equal(a, b, ordered):
    if len(a) != len(b):
        return False
    if not ordered:
        def key(r):
            return tuple((0, "") if v is None else
                         (1, "%.6g" % float(v)) if isinstance(v, (int, float, decimal.Decimal)) else (2, str(v))
                         for v in r)
        a = sorted(a, key=key)
        b = sorted(b, key=key)
    for ra, rb in zip(a, b):
        if len(ra) != len(rb) or not all(same(x, y) for x, y in zip(ra, rb)):
            return False
    return True


def run(conn, sql):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        if cur.description is None:
            return ("ok", cur.rowcount, None)
        cols = [d[0] for d in cur.description]
        rows = [tuple(norm(v) for v in r) for r in cur.fetchall()]
        return ("rows", rows, cols)
    except pymysql.MySQLError as e:
        return ("err", e.args[0] if e.args else None, str(e))
    finally:
        cur.close()


def setup(conn, db):
    run(conn, f"DROP DATABASE IF EXISTS {db}")
    r = run(conn, f"CREATE DATABASE {db}")
    if r[0] == "err":
        raise SystemExit(f"cannot create database: {r}")
    conn.select_db(db)
    for s in SCHEMA + seed():
        r = run(conn, s)
        if r[0] == "err":
            raise SystemExit(f"setup failed: {s}: {r}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True)
    ap.add_argument("--ref", required=True)
    ap.add_argument("--db", default="difftest")
    ap.add_argument("-v", action="store_true")
    args = ap.parse_args()
    a, b = connect(args.tarn), connect(args.ref)
    setup(a, args.db)
    setup(b, args.db)
    fails = warns = 0
    for sql, kind in CASES:
        ra, rb = run(a, sql), run(b, sql)
        ok = True
        msg = ""
        if kind == "err":
            # both must fail, with the same MySQL error code
            ok = ra[0] == "err" and rb[0] == "err" and ra[1] == rb[1]
        elif ra[0] != rb[0]:
            ok = False
        elif ra[0] == "rows":
            ordered = bool(re.search(r"\bORDER\s+BY\b", sql, re.I)) and "GROUP_CONCAT" not in sql
            # same column names (exactly) and the same rows
            ok = ra[2] == rb[2] and rows_equal(ra[1], rb[1], ordered)
        elif ra[0] == "ok":
            # ALTER TABLE reports rows copied (0 when done in place)
            ok = ra[1] == rb[1] or kind.startswith("ddl:")
        if ok and kind.startswith("ddl:"):
            # the same rows under the same column names, and the same indexes
            t = kind[4:]
            da, db_ = run(a, f"SELECT * FROM {t}"), run(b, f"SELECT * FROM {t}")
            ok = da[0] == db_[0] == "rows" and da[2] == db_[2] and rows_equal(da[1], db_[1], False)
            ia, ib = run(a, f"SHOW INDEX FROM {t}"), run(b, f"SHOW INDEX FROM {t}")

            def keys(r):
                if r[0] != "rows":
                    return r
                cols = r[2]
                pick = [cols.index(c) for c in ("Non_unique", "Key_name", "Seq_in_index", "Column_name")]
                return sorted(tuple(row[i] for i in pick) for row in r[1])
            if ok and keys(ia) != keys(ib):
                ok = False
                msg = f"\n    indexes differ:\n    tarn: {keys(ia)}\n    ref:  {keys(ib)}"
            elif not ok:
                msg = f"\n    table {t} differs:\n    tarn: {da[2] if da[0] == 'rows' else da}\n    ref:  {db_[2] if db_[0] == 'rows' else db_}"
        if ok and kind.startswith("dml:"):
            t = kind[4:]
            da, db_ = run(a, f"SELECT * FROM {t}"), run(b, f"SELECT * FROM {t}")
            ok = da[0] == db_[0] == "rows" and rows_equal(da[1], db_[1], False)
            if not ok:
                msg = f"\n    table {t} differs:\n    tarn: {sorted(map(str, da[1]))[:12]}\n    ref:  {sorted(map(str, db_[1]))[:12]}"
        if not ok:
            fails += 1
            print(f"FAIL {sql}\n    tarn: {str(ra)[:600]}\n    ref:  {str(rb)[:600]}{msg}")
        elif args.v or msg:
            print(f"ok   {sql}{msg}")
    print(f"{len(CASES)} cases, {fails} failed, {warns} warnings")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
