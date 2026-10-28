#!/usr/bin/env python3
"""Public SQL controls for typed reads. Compare exact values with MariaDB;
exercise overlays, empty pages and OCC coverage on the real gateway.
Run on native memtx, Lua-only memtx and uncached vinyl.
"""
import argparse
import json
import os
import subprocess
from collections import Counter
from decimal import Decimal
import pymysql
from difftest import connect


def query(c, sql):
    with c.cursor() as q:
        q.execute(sql)
        return tuple(d[0] for d in q.description) if q.description else (), q.fetchall()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--tarn', required=True)
    ap.add_argument('--ref', required=True)
    ap.add_argument('--kv', default='127.0.0.1:3311,127.0.0.1:3321')
    args = ap.parse_args()
    a, b = connect(args.tarn), connect(args.ref)
    db = 'pushdown135'
    for c in (a, b):
        query(c, f'DROP DATABASE IF EXISTS {db}')
        query(c, f'CREATE DATABASE {db}')
        c.select_db(db)
        query(c, 'CREATE TABLE p (tenant INT, id INT, n BIGINT, u BIGINT UNSIGNED, '
              'tag INT, d DOUBLE, price DECIMAL(10,2), name VARCHAR(30) COLLATE utf8mb4_unicode_ci, '
              'wide TEXT, PRIMARY KEY(tenant,id), KEY tn(tenant,n)) PARTITION BY KEY(tenant)')
        with c.cursor() as q:
            for first in range(1, 9001, 200):
                q.executemany('INSERT INTO p VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)', [
                    (1 if i <= 4500 else 2, i, None if i % 3 == 0 else (i % 11 - 5),
                     18446744073709551615 if i % 2 else 9007199254740993,
                     1 if i >= 8999 else 0, float(i % 5), Decimal('1.25'),
                     'Éclair' if i % 2 else 'eclair', 'x' * 512)
                    for i in range(first, min(9001, first + 200))])
    comparisons = 0

    def same(sql):
        nonlocal comparisons
        x, y = query(a, sql), query(b, sql)
        # Decimal/integer equality in Python is exact, including >2^53.
        assert x[0] == y[0] and Counter(x[1]) == Counter(y[1]), (sql, x, y)
        comparisons += 1

    def status(metric):
        return int(query(a, f"SHOW GLOBAL STATUS LIKE '{metric}'")[1][0][1])

    aggregate = 'COUNT(*), COUNT(n), SUM(n), MIN(n), MAX(n), SUM(u), MIN(u), MAX(u), COUNT(name)'
    before = status('Tarn_storage_reductions')
    same(f'SELECT {aggregate} FROM p WHERE tag = 1')
    assert status('Tarn_storage_reductions') > before, 'numeric residual did not reach storage'
    before = status('Tarn_storage_projections')
    same('SELECT tenant,id FROM p WHERE tag = 1')
    assert status('Tarn_storage_projections') > before, 'filtered projection did not execute'
    for where in ['', 'WHERE tenant = 1', 'WHERE tenant = 2 AND n BETWEEN -4 AND 4',
                  'WHERE n IS NULL', 'WHERE n IS NOT NULL', 'WHERE tag = 1',
                  'WHERE tag = 17', 'WHERE n IN (-5, 0, 5, NULL)',
                  'WHERE u > 9007199254740993', 'WHERE u < 18446744073709551615',
                  'WHERE u IN (9007199254740993, 18446744073709551615, NULL)',
                  'WHERE tenant = 1 AND n < 0', 'WHERE tenant = 2 AND n <= 0',
                  'WHERE name = \'ÉCLAIR\'', 'WHERE n + 0 >= 0']:
        same(f'SELECT {aggregate} FROM p {where}')
    for sql in ['SELECT MIN(name), MAX(name), SUM(n) FROM p WHERE tenant = 1 AND id = 1',
                'SELECT SUM(price), MIN(price), MAX(price), SUM(n) FROM p',
                'SELECT SUM(d), MIN(d), MAX(d), SUM(n) FROM p',
                'SELECT SUM(DISTINCT n), MIN(n), MAX(n) FROM p',
                'SELECT tag, SUM(n), MIN(n), MAX(n) FROM p GROUP BY tag',
                'SELECT SUM(n) + COUNT(*) AS total, COALESCE(MIN(n),0) AS lo FROM p HAVING COUNT(*) > 0',
                'SELECT SUM(n), MIN(n) FROM p LIMIT 0',
                'SELECT tenant, id FROM p WHERE tag = 1',  # empty qualifying pages
                'SELECT tenant, id, n, u, d, price, name FROM p WHERE tag = 1',
                'SELECT tenant, id FROM p WHERE tag = 17',
                'SELECT tenant, id FROM p WHERE tenant = 2 AND n < 0',
                'SELECT tenant, id FROM p WHERE n IS NULL',
                'SELECT tenant, id FROM p WHERE n IN (-5, 0, NULL)',
                'SELECT tenant, id FROM p WHERE name = \'ÉCLAIR\'']:
        same(sql)
    for c in (a, b):
        query(c, "PREPARE typed135 FROM 'SELECT SUM(n), MIN(n), MAX(n), COUNT(*) FROM p WHERE n >= ?'")
    for floor in [-2, 0, 6]:
        for c in (a, b):
            query(c, f'SET @floor135 = {floor}')
        same('EXECUTE typed135 USING @floor135')
    for c in (a, b):
        query(c, 'DEALLOCATE PREPARE typed135')
    for c in (a, b):
        query(c, 'BEGIN')
        query(c, 'UPDATE p SET n = 123, u = 18446744073709551615 WHERE tenant = 1 AND id = 1')
        query(c, 'DELETE FROM p WHERE tenant = 2 AND id = 9000')
        query(c, "INSERT INTO p VALUES (2,9001,-999,0,1,1,1.25,'雪','new')")
    same(f'SELECT {aggregate} FROM p')
    same(f'SELECT {aggregate} FROM p WHERE n < 0')
    same('SELECT tenant,id,n FROM p WHERE tag = 1')
    for c in (a, b):
        query(c, 'SAVEPOINT s')
        query(c, 'UPDATE p SET n = NULL WHERE tenant = 1 AND id = 1')
    same(f'SELECT {aggregate} FROM p')
    for c in (a, b):
        query(c, 'ROLLBACK TO s')
    same(f'SELECT {aggregate} FROM p')
    for c in (a, b):
        query(c, 'ROLLBACK')
        query(c, 'ALTER TABLE p ADD COLUMN filled INT NOT NULL DEFAULT 7')
    same('SELECT SUM(filled), MIN(filled), MAX(filled), COUNT(n) FROM p')
    same('SELECT tenant,id,filled FROM p WHERE tag = 1')

    for c in (a, b):
        query(c, 'SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED')
        query(c, 'BEGIN')
        query(c, 'UPDATE p SET n = 123 WHERE tenant = 1 AND id = 1')
    same(f'SELECT {aggregate} FROM p')
    same('SELECT tenant,id,n,filled FROM p WHERE tag = 1')
    for c in (a, b):
        query(c, 'ROLLBACK')
        query(c, 'SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE')

    # A mixed deployment can lack one optional procedure. A matched missing
    # procedure, or an explicitly incomplete fold, keeps complete fallback.
    # Other database/protocol failures must not be mistaken for capability.
    def storage(code):
        for hp in args.kv.split(',')[:1]:
            lua = ("local c=require('net.box').connect(" + json.dumps(hp) +
                   ",{user='tarn',password='tarn',wait_connected=5}) " +
                   "c:eval(" + json.dumps(code) + ") c:close() os.exit(0)")
            subprocess.run([os.getenv('TARANTOOL', 'tarantool'), '-e', lua], check=True)

    storage('_pd135_reduce=tarn_reduce _pd135_select=tarn_select_projected')
    try:
        storage('tarn_reduce=nil tarn_select_projected=nil')
        same(f'SELECT {aggregate} FROM p')
        same('SELECT tenant,id FROM p WHERE tag = 1')
        storage('tarn_select_projected=_pd135_select '
                "tarn_reduce=function(...) local r=_pd135_reduce(...) "
                "if r[1]=='ok' then return {'ok',{},false} end return r end")
        same(f'SELECT {aggregate} FROM p')
        storage("tarn_reduce=function(...) local r=_pd135_reduce(...) "
                "if r[1]=='ok' then return {'ok',r[2],false} end return r end")
        try:
            query(a, f'SELECT {aggregate} FROM p')
        except pymysql.MySQLError as e:
            assert e.args[0] == 1105, e
        else:
            raise AssertionError('malformed partial accepted or silently retried')
        storage('tarn_reduce=_pd135_reduce '
                "tarn_select_projected=function(...) local r=_pd135_select(...) "
                "if r[1]=='ok' then r[4]=-1 end return r end")
        try:
            query(a, 'SELECT tenant,id FROM p WHERE tag = 1')
        except pymysql.MySQLError as e:
            assert e.args[0] == 1105, e
        else:
            raise AssertionError('malformed projection coverage accepted or silently retried')
    finally:
        storage('tarn_reduce=_pd135_reduce tarn_select_projected=_pd135_select '
                '_pd135_reduce=nil _pd135_select=nil')

    # Rejected rows remain in the read footprint: a concurrent update of one
    # cannot silently validate an old full-range projection or aggregate.
    other = connect(args.tarn, db)
    for sql in ['SELECT SUM(n), MIN(n), MAX(n) FROM p WHERE tag = 17',
                'SELECT tenant,id FROM p WHERE tag = 17']:
        query(a, 'BEGIN')
        query(a, sql)
        query(other, 'UPDATE p SET n = COALESCE(n,0) + 1 WHERE tenant = 1 AND id = 1')
        try:
            query(a, 'COMMIT')
        except pymysql.MySQLError as e:
            assert e.args[0] == 1213, e
        else:
            raise AssertionError('stale typed read validated')
    other.close()
    # Independent feature counters prove that these public paths executed.
    for metric in ['Tarn_storage_reductions', 'Tarn_storage_projections']:
        rows = query(a, f"SHOW GLOBAL STATUS LIKE '{metric}'")[1]
        assert rows and int(rows[0][1]) > 0, (metric, rows)
    a.close()
    b.close()
    print(f'{comparisons} exact pushdown comparisons; overlays, pages, fills and OCC controls passed')


if __name__ == '__main__':
    main()
