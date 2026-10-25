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
from pymysql.constants import CLIENT

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
    # collations (issue #13): general_ci, unicode_ci and bin columns, indexed
    # and not; a case-insensitive primary (and shard) key; a case-insensitive
    # unique key next to the shard key
    "CREATE TABLE ci (id INT PRIMARY KEY, handle VARCHAR(20) COLLATE utf8mb4_general_ci, "
    "name VARCHAR(30) COLLATE utf8mb4_unicode_ci, code VARCHAR(10) COLLATE utf8mb4_bin, note VARCHAR(10), n INT, "
    "UNIQUE KEY uh (handle, id), KEY kn (name)) DEFAULT CHARSET=utf8mb4",
    "CREATE TABLE cih (handle VARCHAR(20) PRIMARY KEY, n INT) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
    "CREATE TABLE ciu (t INT, id INT, email VARCHAR(40), PRIMARY KEY (t, id), UNIQUE KEY ue (t, email)) "
    "DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci PARTITION BY KEY(t)",
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
    s.append("INSERT INTO ci VALUES (1, 'Alice', 'Émile', 'A1', 'x', 10), (2, 'bob', 'emile', 'a1', 'X', 20), "
             "(3, 'ALICE', 'Zoë', 'b2', 'y', 30), (4, 'carol', 'zoe', 'B2', 'Y', 40), "
             "(5, 'Bob', 'Ångström', 'c3', 'z', 50), (6, 'dave', 'angstrom', 'C3', NULL, 60), "
             "(7, 'Eve ', 'straße', 'd4', 'x', 70), (8, 'a_b', 'Straße', 'D4', 'w', 80), "
             "(9, 'ab', 'Œuvre', 'e5', 'v', 90), (10, 'a-b', 'oeuvre', 'E5', 'u', 100)")
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
    # UPDATE / DELETE by full primary key in a READ COMMITTED transaction:
    # the rows are locked and read in one step (issue #48)
    ("BEGIN", "q"),
    ("UPDATE t1 SET a = a + 10 WHERE id = 3", "dml:t1"),
    ("UPDATE t1 SET a = a + 10 WHERE id = 3", "dml:t1"),
    ("UPDATE t1 SET a = 1 WHERE id = 999", "dml:t1"),
    ("UPDATE t1 SET b = 'x' WHERE id = 4 AND a = 12345", "dml:t1"),
    ("UPDATE t1 SET b = 'same' WHERE id = 5 AND b = 'name5'", "dml:t1"),
    ("UPDATE t1 SET b = 'same' WHERE id = 5", "dml:t1"),
    ("UPDATE t1 SET c = c + 1 WHERE id IN (6, 8, 999, 6)", "dml:t1"),
    ("UPDATE t1 SET c = c + 1 WHERE id IN (9, 10, 12) ORDER BY id DESC LIMIT 2", "dml:t1"),
    ("UPDATE t1 SET a = NULL WHERE id = NULL", "dml:t1"),
    ("UPDATE t1 SET a = 0 WHERE id = '13'", "dml:t1"),
    ("UPDATE t1 SET a = 0 WHERE id = 2.5", "dml:t1"),
    ("UPDATE t3 SET v = v + 1 WHERE k1 = 1 AND k2 = 'k7'", "dml:t3"),
    ("UPDATE t3 SET v = v + 1 WHERE (k1, k2) IN ((2, 'k8'), (3, 'k9'), (3, 'nope'))", "dml:t3"),
    ("UPDATE uk SET n = n + 1 WHERE t = 1 AND id = 1 LIMIT 0", "dml:uk"),
    ("SELECT id, a, b, c FROM t1 WHERE id BETWEEN 3 AND 13 ORDER BY id", "q"),
    ("DELETE FROM t1 WHERE id = 14", "dml:t1"),
    ("DELETE FROM t1 WHERE id = 14", "dml:t1"),
    ("DELETE FROM t1 WHERE id = 15 AND a = 12345", "dml:t1"),
    ("DELETE FROM t1 WHERE id IN (16, 17, 999)", "dml:t1"),
    ("DELETE FROM t3 WHERE k1 = 4 AND k2 = 'k10'", "dml:t3"),
    ("INSERT INTO t1 (id, a) VALUES (14, 1)", "dml:t1"),
    ("UPDATE t1 SET a = 2 WHERE id = 14", "dml:t1"),
    ("SELECT id, a FROM t1 WHERE id BETWEEN 13 AND 17 ORDER BY id", "q"),
    ("COMMIT", "dml:t1"),
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
    # LIMIT is not pushed below a HAVING without aggregation (issue #38)
    # (each result is every qualifying row, so it does not depend on row order)
    ("SELECT id FROM t1 HAVING id = 40 LIMIT 1", "q"),
    ("SELECT id FROM t1 HAVING id BETWEEN 38 AND 40 LIMIT 5", "q"),
    ("SELECT k1, v FROM t3 HAVING v >= 200 LIMIT 10", "q"),
    ("SELECT id FROM t1 HAVING id BETWEEN 38 AND 40 ORDER BY id LIMIT 1, 5", "q"),
    # tables without a PRIMARY KEY (issue #32): keyed by the first unique
    # NOT NULL key, as Rails' schema_migrations, or by a hidden row id
    ("CREATE TABLE sm (version VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, "
     "UNIQUE KEY unique_schema_migrations (version)) ENGINE=InnoDB ROW_FORMAT=DYNAMIC", "ddl:sm"),
    ("INSERT INTO sm (version) VALUES ('20261023145912'), ('20261023145858')", "dml:sm"),
    ("INSERT INTO sm (version) VALUES ('20261023145912')", "err"),
    ("INSERT IGNORE INTO sm (version) VALUES ('20261023145912'), ('20261023010002')", "dml:sm"),
    ("INSERT INTO sm VALUES ('20261023145858') ON DUPLICATE KEY UPDATE version = '20261022000000'", "dml:sm"),
    ("SELECT version FROM sm WHERE version = '20261023010002'", "q"),
    ("SELECT * FROM sm ORDER BY version", "q"),
    ("DESCRIBE sm", "q"),
    ("CREATE TABLE nk (msg VARCHAR(20), n INT, KEY kn (n))", "ddl:nk"),
    ("INSERT INTO nk VALUES ('a', 1), ('a', 1), ('b', 2), (NULL, NULL)", "dml:nk"),
    ("INSERT INTO nk SET msg = 'c'", "dml:nk"),
    ("SELECT * FROM nk WHERE n = 1", "q"),
    ("SELECT nk.* FROM nk ORDER BY msg, n", "q"),
    ("UPDATE nk SET n = 5 WHERE msg = 'b'", "dml:nk"),
    ("DELETE FROM nk WHERE n = 1 LIMIT 1", "dml:nk"),
    ("REPLACE INTO nk VALUES ('a', 1)", "dml:nk"),
    ("SELECT msg, COUNT(*) FROM nk GROUP BY msg", "q"),
    ("SELECT DB_ROW_ID FROM nk", "err"),
    ("ALTER TABLE nk ADD COLUMN z INT DEFAULT 3, ADD INDEX kz (z)", "ddl:nk"),
    ("ALTER TABLE nk DROP COLUMN msg", "ddl:nk"),
    ("ALTER TABLE nk DROP COLUMN n, DROP COLUMN z", "err"),
    ("CREATE TABLE rid (DB_ROW_ID INT)", "err"),
    # information_schema.TABLES and SELECT ... INTO @var (issue #33): what
    # mysqldump's header and Rails send
    ("SELECT table_name FROM information_schema.tables WHERE table_schema = database() ORDER BY table_name", "q"),
    ("SELECT table_name FROM information_schema.tables WHERE table_schema = database() AND table_name = 't1' "
     "AND table_name IN (SELECT table_name FROM information_schema.tables WHERE table_schema = database())", "q"),
    ("SELECT table_name FROM information_schema.tables WHERE table_schema = database() "
     "AND table_type = 'BASE TABLE' AND table_name LIKE 't%'", "q"),
    ("SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES "
     "WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = 'u'", "q"),
    ("SELECT table_comment FROM information_schema.tables WHERE table_schema = database() AND table_name = 'nope'", "q"),
    ("SELECT COUNT(*) FROM information_schema.tables t WHERE t.table_schema = database()", "q"),
    ("SELECT COUNT(*) INTO @rocksdb_has_p_s_session_variables FROM INFORMATION_SCHEMA.TABLES "
     "WHERE TABLE_SCHEMA = 'performance_schema' AND TABLE_NAME = 'session_variables'", "q"),
    ("SELECT @rocksdb_has_p_s_session_variables", "q"),
    ("SELECT COUNT(*) INTO @n FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = database()", "q"),
    ("SELECT @n", "q"),
    ("SELECT a, b INTO @ia, @ib FROM t1 WHERE id = 2", "q"),
    ("SELECT @ia, @ib", "q"),
    ("SELECT a INTO @ia FROM t1 WHERE id = 999", "q"),
    ("SELECT @ia", "q"),
    ("SELECT id FROM t1 WHERE id < 5 INTO @ia", "err"),
    ("SELECT 1, 2 INTO @ia", "err"),
    ("SELECT * FROM information_schema.no_such_table", "err"),
    ("INSERT INTO information_schema.tables (table_name) VALUES ('x')", "err"),
    ("DELETE FROM information_schema.tables", "err"),
    # SQL-level PREPARE / EXECUTE / DEALLOCATE PREPARE (issue #34); the
    # first four are the shape of mysqldump's MyRocks preamble (there inside
    # /*!50717 */, which MariaDB skips; ddltest.py loads it verbatim)
    ("SET @rocksdb_get_is_supported = IF (@rocksdb_has_p_s_session_variables, 'SELECT COUNT(*) INTO "
     "@rocksdb_is_supported FROM performance_schema.session_variables WHERE VARIABLE_NAME=\\'rocksdb_bulk_load\\'', "
     "'SELECT 0 AS probe')", "q"),
    ("PREPARE s FROM @rocksdb_get_is_supported", "q"),
    ("EXECUTE s", "q"),
    ("DEALLOCATE PREPARE s", "q"),
    ("SET @q = 'SELECT id, b FROM t1 WHERE id < ? AND a = ? ORDER BY id'", "q"),
    ("PREPARE p FROM @q", "q"),
    ("SET @lim = 12, @a = 3", "q"),
    ("EXECUTE p USING @lim, @a", "q"),
    ("SET @lim = 30", "q"),
    ("EXECUTE p USING @lim, @a", "q"),
    ("EXECUTE p USING @lim", "err"),
    ("PREPARE ins FROM 'INSERT INTO t2 (id, t1_id, name) VALUES (?, ?, ''prep'')'", "q"),
    ("SET @i = 900, @t = 4", "q"),
    ("EXECUTE ins USING @i, @t", "dml:t2"),
    ("EXECUTE ins USING @i, @t", "err"),
    ("PREPARE p FROM 'SELECT COUNT(*) FROM t2 WHERE name = ''prep'''", "q"),
    ("EXECUTE p", "q"),
    ("DROP PREPARE p", "q"),
    ("EXECUTE p", "err"),
    ("DEALLOCATE PREPARE p", "err"),
    ("PREPARE p2 FROM 'EXECUTE ins'", "err"),
    # row-value comparisons (issue #40): keyset cursors, NULLs where they decide
    ("SELECT k1, k2 FROM t3 WHERE (k1, k2) > (2, 'k20') ORDER BY k1, k2 LIMIT 5", "q"),
    ("SELECT k1, k2 FROM t3 WHERE (k1, k2) >= (2, 'k20') ORDER BY k1, k2 LIMIT 5", "q"),
    ("SELECT k1, k2 FROM t3 WHERE (k1, k2) < (2, 'k20') ORDER BY k1 DESC, k2 DESC LIMIT 5", "q"),
    ("SELECT k1, k2 FROM t3 WHERE (k1, k2) <= (1, 'k13') ORDER BY k1, k2", "q"),
    ("SELECT id FROM t1 WHERE (a, id) > (3, 20) ORDER BY id", "q"),
    ("SELECT id FROM t1 WHERE NOT ((a, id) > (3, 20)) ORDER BY id", "q"),
    ("SELECT id FROM t1 WHERE (a, b) <=> (NULL, 'name5') ORDER BY id", "q"),
    ("SELECT (1, NULL) < (2, 0), (1, NULL) < (1, 0), (NULL, 1) > (0, 0), (1, 2, 3) >= (1, 2, 3)", "q"),
    ("SELECT id FROM t1 WHERE (id, a) > (1, 2, 3)", "err"),
    # DATETIME spellings (issue #37): one stored form per value; constants
    # compare by value, not by spelling. (Extra fractional digits are rounded,
    # as MySQL 8 does; MariaDB truncates them, so no case stores any.)
    ("CREATE TABLE dtp (s INT NOT NULL, id INT NOT NULL, dt DATETIME(6), d0 DATETIME, dd DATE, "
     "PRIMARY KEY (s, id), KEY by_dt (s, dt, id), KEY by_d0 (d0))", "ddl:dtp"),
    ("INSERT INTO dtp VALUES (1, 1, '2025-01-01 00:00:00', '2025-01-01 00:00:00', '2025-01-01'), "
     "(1, 2, '2025-01-01 00:00:01.000000', '2025-01-01 00:00:01.000000', '2025-01-01 10:00:00'), "
     "(1, 3, '2025-01-01 00:00:02.5', '2025-01-01 00:00:02', '2025-01-02'), "
     "(1, 4, '2025-01-01T00:00:03', '2025-12-31 23:59:59', '20250103'), "
     "(2, 1, '2025-1-1 0:0:4', '20250101000004', '2025-01-04')", "dml:dtp"),
    ("SELECT * FROM dtp ORDER BY s, id", "q"),
    ("SELECT id FROM dtp WHERE s = 1 AND dt = '2025-01-01 00:00:01'", "q"),
    ("SELECT id FROM dtp WHERE s = 1 AND dt = '2025-01-01 00:00:01.000'", "q"),
    ("SELECT id FROM dtp WHERE s = 1 AND (dt > '2025-01-01 00:00:01' OR (dt = '2025-01-01 00:00:01' AND id > 2)) "
     "ORDER BY dt, id", "q"),
    ("SELECT id FROM dtp WHERE s = 1 AND (dt, id) > ('2025-01-01 00:00:01', 2) ORDER BY dt, id", "q"),
    ("SELECT id FROM dtp WHERE (s, dt) IN ((1, '2025-01-01 00:00:01'), (2, '2025-01-01 00:00:04')) ORDER BY id", "q"),
    ("SELECT id FROM dtp WHERE s = 1 AND dt >= '2025-01-01 00:00:02.5' ORDER BY id", "q"),
    ("SELECT id FROM dtp WHERE s = 1 AND dt > '2025-01-01 00:00:02.4999' ORDER BY id", "q"),
    ("SELECT id FROM dtp WHERE s = 1 AND dt BETWEEN '2025-01-01 00:00:01' AND '2025-01-01 00:00:02.5' ORDER BY id", "q"),
    ("SELECT id FROM dtp WHERE dt IN ('2025-01-01 00:00:01', '2025-01-01 00:00:04.000') ORDER BY id", "q"),
    ("SELECT id FROM dtp WHERE d0 = '2025-01-01 00:00:01.000000'", "q"),
    ("SELECT id FROM dtp WHERE d0 = '2025-01-01 00:00:02.000'", "q"),
    ("SELECT id FROM dtp WHERE d0 > '2025-01-01 00:00:02.5' ORDER BY id", "q"),
    ("SELECT id FROM dtp WHERE dd = '2025-01-01 00:00:00'", "q"),
    ("SELECT id FROM dtp WHERE dd = '2025-01-01 10:00:00'", "q"),
    ("SELECT id FROM dtp WHERE dd < '2025-01-02 00:00:01' ORDER BY id", "q"),
    ("SELECT d0, COUNT(*) FROM dtp GROUP BY d0 ORDER BY d0", "q"),
    ("UPDATE dtp SET dt = '2025-01-01 00:00:01' WHERE dt = '2025-01-01 00:00:01.000000'", "dml:dtp"),
    # collations (issue #13): comparisons, LIKE, ranges and lookups through
    # case-insensitive indexes, ordering, grouping, unique keys
    ("SELECT id FROM ci WHERE handle = 'alice'", "q"),
    ("SELECT id FROM ci WHERE handle IN ('BOB', 'Carol', 'nobody') ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE handle = 'eve'", "q"),
    ("SELECT id FROM ci WHERE name = 'EMILE' ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE name IN ('zoe', 'ANGSTROM') ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE name = 'STRASSE' ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE name = 'oeuvre' ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE code = 'a1'", "q"),
    ("SELECT id FROM ci WHERE note = 'X' ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE handle LIKE 'a%' ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE handle LIKE 'B_B' ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE name LIKE '%STROM' ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE name LIKE 'é%' ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE code LIKE 'a%' ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE handle > 'bob' ORDER BY handle, id", "q"),
    ("SELECT id FROM ci WHERE handle BETWEEN 'B' AND 'D' ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE name >= 'Z' ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE handle < 'AB' ORDER BY id", "q"),
    ("SELECT handle, id FROM ci ORDER BY handle, id", "q"),
    ("SELECT name, id FROM ci ORDER BY name DESC, id", "q"),
    ("SELECT code FROM ci ORDER BY code", "q"),
    ("SELECT id FROM ci ORDER BY handle, id LIMIT 3", "q"),
    ("SELECT LOWER(handle), COUNT(*), MIN(id), SUM(n) FROM ci GROUP BY handle ORDER BY 1", "q"),
    ("SELECT MIN(id) AS k, COUNT(*) AS c FROM ci GROUP BY name HAVING c > 1 ORDER BY k", "q"),
    ("SELECT COUNT(DISTINCT handle), COUNT(DISTINCT name), COUNT(DISTINCT code), COUNT(DISTINCT note) FROM ci", "q"),
    ("SELECT MAX(handle), MIN(code), MAX(code) FROM ci", "q"),
    ("SELECT GROUP_CONCAT(id ORDER BY handle, id) FROM ci", "q"),
    ("SELECT handle FROM ci WHERE id = 1 UNION SELECT handle FROM ci WHERE id = 3", "q"),
    ("SELECT code FROM ci WHERE id = 1 UNION SELECT code FROM ci WHERE id = 2 ORDER BY code", "q"),
    ("SELECT 'a' = 'A', 'a' = 'a ', 'a' < 'B', 'ß' = 's', 'abc' LIKE 'AB_', 'é' = 'E'", "q"),
    ("SELECT 'ß' COLLATE utf8mb4_unicode_ci = 'ss', 'Æ' COLLATE utf8mb4_unicode_ci = 'ae', "
     "'a' COLLATE utf8mb4_bin = 'A', 'a' COLLATE utf8mb4_general_nopad_ci = 'a '", "q"),
    ("SELECT COLLATION(handle), COLLATION(name), COLLATION(code), COLLATION(note), COLLATION('x'), "
     "COERCIBILITY(handle), COERCIBILITY('x'), COLLATION(handle COLLATE utf8mb4_bin) FROM ci WHERE id = 1", "q"),
    ("SELECT HEX(WEIGHT_STRING(handle)), HEX(WEIGHT_STRING(name)), HEX(WEIGHT_STRING(code)) FROM ci ORDER BY id", "q"),
    ("SELECT HEX(WEIGHT_STRING('AaÀàÆæŒœßẞÞþÐðŁłØøĲĳﬁﬂ ΑαΣσςЁёЙйЯя٣३अआ中日本あアｶﾞ가각😀𝐀  ' COLLATE utf8mb4_general_ci)) "
     "AS w", "q"),
    ("SELECT HEX(WEIGHT_STRING('AaÀàÆæŒœßẞÞþÐðŁłØøĲĳﬁﬂ ΑαΣσςЁёЙйЯя٣३अआ中日本あアｶﾞ가각😀𝐀  ' COLLATE utf8mb4_unicode_ci)) "
     "AS w", "q"),
    ("SELECT NULLIF(handle, 'ALICE'), GREATEST(handle, 'B'), LEAST(name, 'F'), LOCATE('LI', handle) FROM ci "
     "WHERE id IN (1, 2, 5) ORDER BY id", "q"),
    ("SELECT id, CASE handle WHEN 'ALICE' THEN 'a' WHEN 'BOB' THEN 'b' ELSE '-' END FROM ci ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE code = 'a1' COLLATE utf8mb4_general_ci ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE handle = 'alice' COLLATE utf8mb4_bin", "q"),
    ("SELECT id FROM ci WHERE handle = code ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE handle = name", "err"),
    ("SELECT id FROM ci WHERE handle = 'x' COLLATE utf8mb4_nonesuch_ci", "err"),
    ("SELECT id FROM ci WHERE handle = 'x' COLLATE latin1_swedish_ci", "err"),
    ("INSERT INTO cih VALUES ('Alice', 1), ('Straße', 2), ('Zoë', 3)", "dml:cih"),
    ("INSERT INTO cih VALUES ('ALICE', 4)", "err"),
    ("INSERT INTO cih VALUES ('alice  ', 4)", "err"),
    ("INSERT INTO cih VALUES ('strasse', 4)", "dml:cih"),
    ("INSERT INTO cih VALUES ('ZOE', 4)", "err"),
    ("INSERT IGNORE INTO cih VALUES ('aLiCe', 5), ('Bob', 6)", "dml:cih"),
    ("INSERT INTO cih VALUES ('BOB', 7) ON DUPLICATE KEY UPDATE n = n + 100", "dml:cih"),
    ("REPLACE INTO cih VALUES ('bob', 8)", "dml:cih"),
    ("SELECT handle, n FROM cih WHERE handle IN ('ALICE', 'alice', 'BOB', 'zoe') ORDER BY n", "q"),
    ("SELECT handle FROM cih ORDER BY handle LIMIT 2", "q"),
    ("UPDATE cih SET handle = 'ALICE' WHERE handle = 'alice'", "dml:cih"),
    ("UPDATE cih SET handle = 'bob' WHERE handle = 'alice'", "err"),
    ("UPDATE cih SET n = n + 1 WHERE handle = 'BOB'", "dml:cih"),
    ("DELETE FROM cih WHERE handle = 'STRASSE'", "dml:cih"),
    ("SELECT a.id, b.n FROM ci a JOIN cih b ON b.handle = a.handle ORDER BY a.id", "q"),
    ("INSERT INTO ciu VALUES (1, 1, 'A@x.org'), (1, 2, 'b@x.org'), (2, 1, 'a@x.org')", "dml:ciu"),
    ("INSERT INTO ciu VALUES (1, 3, 'a@X.ORG')", "err"),
    ("UPDATE ciu SET email = 'B@X.org' WHERE t = 1 AND id = 1", "err"),
    ("UPDATE ciu SET email = 'a@X.org' WHERE t = 1 AND id = 1", "dml:ciu"),
    ("SELECT t, id FROM ciu WHERE email = 'A@X.ORG' ORDER BY t, id", "q"),
    ("UPDATE ci SET n = n + 1 ORDER BY handle DESC, id LIMIT 3", "dml:ci"),
    ("DELETE FROM ci WHERE name = 'OEUVRE' ORDER BY id LIMIT 1", "dml:ci"),
    ("BEGIN", "q"),
    ("INSERT INTO cih VALUES ('Zed', 1)", "dml:cih"),
    ("INSERT INTO cih VALUES ('ZED', 2)", "err"),
    ("UPDATE cih SET n = 5 WHERE handle = 'zed'", "dml:cih"),
    ("COMMIT", "dml:cih"),
    ("SET SESSION tx_isolation = 'READ-COMMITTED'", "q"),
    ("INSERT INTO cih VALUES ('zed', 3)", "err"),
    ("UPDATE cih SET n = n * 2 WHERE handle IN ('ZED', 'bob')", "dml:cih"),
    ("SELECT handle, n FROM cih WHERE handle = 'ZeD' FOR UPDATE", "q"),
    ("DELETE FROM cih WHERE handle = 'ZED'", "dml:cih"),
    ("SET SESSION tx_isolation = 'REPEATABLE-READ'", "q"),
    ("ALTER TABLE ci ADD COLUMN tag VARCHAR(10) COLLATE utf8mb4_general_ci, ADD UNIQUE KEY ut (tag, id)", "ddl:ci"),
    ("UPDATE ci SET tag = 'Y' WHERE id = 1", "dml:ci"),
    ("UPDATE ci SET tag = 'y' WHERE id IN (2, 3)", "dml:ci"),
    ("SELECT id FROM ci WHERE tag = 'Y' ORDER BY id", "q"),
    ("SELECT id FROM ci WHERE tag IS NULL ORDER BY id", "q"),
    ("ALTER TABLE ci MODIFY note VARCHAR(10) COLLATE utf8mb4_general_ci", "ddl:ci"),
    ("SELECT id FROM ci WHERE note = 'Y' ORDER BY id", "q"),
    ("ALTER TABLE ci MODIFY code VARCHAR(12) COLLATE utf8mb4_bin", "ddl:ci"),
    ("SELECT id FROM ci WHERE code = 'b2'", "q"),
    ("SELECT handle, id FROM ci WHERE (handle, id) > ('BOB', 0) ORDER BY handle, id LIMIT 4", "q"),
    ("SELECT id FROM ci WHERE (handle, id) <= ('carol', 100) ORDER BY handle DESC, id DESC LIMIT 3", "q"),
    ("SELECT id FROM ci WHERE (name, id) >= ('ÉMILE', 0) ORDER BY name, id", "q"),
    ("SELECT id FROM ci WHERE (handle, id) > (name, 0)", "err"),
    # ... in tables without a PRIMARY KEY: keyed by a case-insensitive
    # unique key, or by the hidden row id with a case-insensitive index
    ("CREATE TABLE cik (email VARCHAR(40) COLLATE utf8mb4_general_ci NOT NULL, n INT, UNIQUE KEY ue (email))",
     "ddl:cik"),
    ("INSERT INTO cik VALUES ('A@x.org', 1), ('b@x.org', 2)", "dml:cik"),
    ("INSERT INTO cik VALUES ('a@X.ORG', 3)", "err"),
    ("INSERT INTO cik VALUES ('B@X.org', 4) ON DUPLICATE KEY UPDATE n = n + 10", "dml:cik"),
    ("SELECT email, n FROM cik WHERE email = 'B@x.ORG'", "q"),
    ("CREATE TABLE cin (tag VARCHAR(10) COLLATE utf8mb4_unicode_ci, n INT, KEY kt (tag))", "ddl:cin"),
    ("INSERT INTO cin VALUES ('a', 1), ('A', 2), ('b', 3), ('Ä', 4)", "dml:cin"),
    ("SELECT n FROM cin WHERE tag = 'A' ORDER BY n", "q"),
    ("SELECT MIN(n), COUNT(*) FROM cin GROUP BY tag ORDER BY 1", "q"),
]


# Run on connections with CLIENT_FOUND_ROWS (issue #39): UPDATE reports the
# rows it matched, and an upsert that changes nothing reports 1.
FOUND_ROWS_CASES = [
    ("UPDATE t1 SET a = a WHERE id = 3", "dml:t1"),
    ("UPDATE t1 SET a = 4 WHERE id BETWEEN 1 AND 6", "dml:t1"),
    ("UPDATE t1 SET a = a WHERE id = 9999", "dml:t1"),
    ("INSERT INTO t1 (id, a) VALUES (3, 4) ON DUPLICATE KEY UPDATE a = 4", "dml:t1"),
    ("INSERT INTO t1 (id, a) VALUES (3, 5) ON DUPLICATE KEY UPDATE a = VALUES(a)", "dml:t1"),
    ("BEGIN", "q"),
    ("UPDATE t1 SET b = b WHERE id < 10", "dml:t1"),
    ("COMMIT", "q"),
    ("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED", "q"),
    ("UPDATE t1 SET a = a WHERE id < 10", "dml:t1"),
    ("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", "q"),
]


def connect(hp, db=None, flags=0):
    host, port = hp.split(":")
    return pymysql.connect(host=host, port=int(port), user="sb", password="", database=db,
                           autocommit=True, charset="utf8mb4", client_flag=flags)


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
    fa, fb = (connect(hp, args.db, CLIENT.FOUND_ROWS) for hp in (args.tarn, args.ref))
    for (a, b), (sql, kind) in [((a, b), c) for c in CASES] + [((fa, fb), c) for c in FOUND_ROWS_CASES]:
        sql = sql.replace("{db}", args.db)  # the database the cases run in (--db)
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
    print(f"{len(CASES) + len(FOUND_ROWS_CASES)} cases, {fails} failed, {warns} warnings")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
