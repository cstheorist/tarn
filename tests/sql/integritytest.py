#!/usr/bin/env python3
"""Executable integrity declaration/refusal contract and supported-write controls.

Runs against an isolated Tarn cluster and pinned MySQL or MariaDB reference.
No raw user-data writes are used. --kv optionally injects an old catalog only
into this suite's fresh tables to test legacy inspection and removal.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
import pathlib
import secrets
import struct
import subprocess
import threading

import pymysql
from pymysql.constants import COMMAND
from explaintest import prepare

CONTRACT = pathlib.Path(__file__).with_name('data') / 'integrity-contract.json'
_raise = pymysql.protocol.MysqlPacket.raise_for_error


def state_error(packet):
    data = packet.get_all_data()
    try:
        _raise(packet)
    except pymysql.MySQLError as error:
        error.sqlstate = data[4:9].decode() if data[3:4] == b'#' else None
        raise


pymysql.protocol.MysqlPacket.raise_for_error = state_error


def connect(hp, db=None):
    host, port = hp.rsplit(':', 1)
    return pymysql.connect(host=host, port=int(port), user='sb', password='', database=db,
                           autocommit=True, charset='utf8mb4', read_timeout=30)


def q(c, sql):
    with c.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall() if cur.description else cur.rowcount


def outcome(fn):
    try:
        return {'ok': fn()}
    except pymysql.MySQLError as e:
        return {'error': e.args[0], 'state': getattr(e, 'sqlstate', None)}


def snapshot(c):
    return {'ddl': q(c, 'SHOW CREATE TABLE t')[0][1], 'rows': q(c, 'SELECT * FROM t ORDER BY id')}


def binary_execute(c, statement, values=()):
    payload = struct.pack('<IBI', statement, 0, 1)
    if values:
        nulls = sum((v is None) << i for i, v in enumerate(values))
        payload += nulls.to_bytes((len(values)+7)//8, 'little') + b'\x01'
        payload += b'\x08\x00' * len(values)
        payload += b''.join(struct.pack('<q', v) for v in values if v is not None)
    c._execute_command(COMMAND.COM_STMT_EXECUTE, payload)
    c._read_query_result()
    return c._result.affected_rows


def binary_rejection(c, sql):
    statement = None
    try:
        statement, columns, params = prepare(c, sql)
        assert columns == params == 0, (columns, params)
        return binary_execute(c, statement)
    finally:
        if statement is not None:
            c._execute_command(COMMAND.COM_STMT_CLOSE, struct.pack('<I', statement))


def named_rejection(c, sql):
    stored = False
    try:
        q(c, 'PREPARE integrity_ddl FROM ' + c.escape(sql))
        stored = True
        return q(c, 'EXECUTE integrity_ddl')
    finally:
        if stored:
            q(c, 'DEALLOCATE PREPARE integrity_ddl')


def declarations(a, b, manifest, flavor):
    results = []
    for case in manifest['rejections']:
        # Each reference trial starts with a valid schema/data set, including
        # nullable CHECK/FK inputs. Refusals are not inferred from bad data.
        for c in (a, b):
            q(c, 'DROP TABLE IF EXISTS denied')
            q(c, 'DROP TABLE IF EXISTS t')
            q(c, 'CREATE TABLE t(id INT PRIMARY KEY,v INT)')
            q(c, 'INSERT INTO t VALUES(1,NULL),(2,NULL)')
        before = snapshot(a)
        states = []
        for label, fn in [('text', lambda: q(a, case['sql'])),
                          ('binary', lambda: binary_rejection(a, case['sql'])),
                          ('named', lambda: named_rejection(a, case['sql']))]:
            r = outcome(fn)
            assert r == {'error': 1235, 'state': '42000'}, (case, label, r)
            assert snapshot(a) == before, (case, label, 'partial schema/data effect')
            assert not q(a, "SHOW TABLES LIKE 'denied'"), (case, 'created unsupported table')
            states.append({'path': label, **r})
        expected = case.get(flavor+'_error', 0)
        reference = outcome(lambda: q(b, case['sql']))
        assert reference.get('error', 0) == expected, (case, flavor, reference, expected)
        results.append({'id': case['id'], 'tarn': states, 'reference': reference})
    return results


def writes(c):
    q(c, 'CREATE TABLE e(id INT PRIMARY KEY,u INT,n INT NOT NULL,UNIQUE KEY uq(u))')
    trace = []
    for sql in [
        'INSERT INTO e VALUES(1,NULL,1),(2,NULL,2),(3,7,3)',
        'INSERT INTO e VALUES(4,8,4),(5,7,5)',
        'UPDATE e SET u=7 WHERE id IN(1,2)',
        'INSERT INTO e VALUES(6,6,NULL)',
        'UPDATE e SET n=NULL WHERE id=1',
        'INSERT INTO e VALUES(8,7,80) ON DUPLICATE KEY UPDATE n=n+1',
        'INSERT IGNORE INTO e VALUES(8,7,8),(9,9,9)',
        'BEGIN', 'INSERT INTO e VALUES(11,11,11)', 'SAVEPOINT s',
        'INSERT INTO e VALUES(12,12,12)', 'INSERT INTO e VALUES(13,7,13)',
        'ROLLBACK TO SAVEPOINT s', 'COMMIT',
    ]:
        r = outcome(lambda: q(c, sql))
        trace.append({'sql': sql, 'outcome': r, 'rows': q(c, 'SELECT * FROM e ORDER BY id')})
    statement, columns, params = prepare(c, 'INSERT INTO e VALUES(?,?,?)')
    assert columns == 0 and params == 3
    try:
        for values in [(14,14,14), (15,7,15), (16,16,None)]:
            r = outcome(lambda: binary_execute(c, statement, values))
            trace.append({'binary_values': values, 'outcome': r, 'rows': q(c, 'SELECT * FROM e ORDER BY id')})
    finally:
        c._execute_command(COMMAND.COM_STMT_CLOSE, struct.pack('<I', statement))
    # A rejected NOT NULL or UNIQUE addition cannot publish a half-valid schema.
    q(c, 'CREATE TABLE validation_rows(id INT PRIMARY KEY,u INT,n INT)')
    q(c, 'INSERT INTO validation_rows VALUES(1,1,NULL),(2,1,2)')
    before = q(c, 'SHOW CREATE TABLE validation_rows')[0][1]
    for sql in ['ALTER TABLE validation_rows ADD UNIQUE KEY uq(u)',
                'ALTER TABLE validation_rows MODIFY n INT NOT NULL']:
        r = outcome(lambda: q(c, sql))
        assert 'error' in r, (sql, r)
        assert q(c, 'SHOW CREATE TABLE validation_rows')[0][1] == before
        rows = q(c, 'SELECT * FROM validation_rows ORDER BY id')
        assert rows == ((1,1,None),(2,1,2)), rows
        trace.append({'sql': sql, 'outcome': r, 'rows': rows})
    q(c, 'UPDATE validation_rows SET u=2,n=2 WHERE id=1')
    q(c, 'ALTER TABLE validation_rows ADD UNIQUE KEY uq(u)')
    q(c, 'ALTER TABLE validation_rows MODIFY n INT NOT NULL')
    assert outcome(lambda: q(c, 'INSERT INTO validation_rows VALUES(3,2,3)'))['error'] == 1062
    assert outcome(lambda: q(c, 'INSERT INTO validation_rows VALUES(3,3,NULL)'))['error'] == 1048
    return trace


def concurrent_unique(hp, db):
    barrier = threading.Barrier(2)
    def writer(i):
        c = connect(hp, db)
        try:
            barrier.wait(timeout=10)
            return outcome(lambda: q(c, f'INSERT INTO e VALUES({i},77,{i})'))
        finally:
            c.close()
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        trials = list(pool.map(writer, (21,22)))
    assert sum('ok' in r for r in trials) == 1, trials
    assert all(r.get('error', 1062) in (1062,1213) for r in trials), trials
    c = connect(hp, db)
    try:
        rows = q(c, 'SELECT id,u,n FROM e WHERE u=77')
        assert len(rows) == 1 and rows[0] in ((21,77,21),(22,77,22)), rows
        return {'trials': trials, 'exact_rows': rows}
    finally:
        c.close()


def legacy(c, nodes, db):
    q(c, 'CREATE TABLE legacy_rows(id INT PRIMARY KEY,v TEXT,KEY kv(v))')
    q(c, "INSERT INTO legacy_rows VALUES(1,'outside-old-list')")
    for node in nodes.split(','):
        code = '''local c=require('net.box').connect('tarn:tarn@NODE',{wait_connected=5})
local result=c:eval([==[
local json=require('json');local t=assert(box.space._tarn_catalog:get{'DB.legacy_rows'}).def
local d=json.decode(t.json);d.integrity_policy=nil
d.foreign_keys={{name='legacy_fk',cols={'v'},ref_db='DB',ref_table='p',ref_cols={'id'}}}
t.json=json.encode(d);box.space._tarn_catalog:replace{'DB.legacy_rows',t}
tarn_counter_alloc('catalog_version',1);return true
]==]);assert(result);c:close();os.exit(0)'''.replace('NODE',node).replace('DB',db)
        subprocess.run([os.getenv('TARANTOOL','tarantool'), '-e', code], check=True,
                       capture_output=True, text=True, timeout=30)
    # A new connection forces a fresh real catalog, not a mutated gateway cache.
    c2 = connect(c.host+':'+str(c.port), db)
    try:
        rows = q(c2, 'SHOW TARN INTEGRITY')
        row = next(r for r in rows if r[0] == 'legacy_rows')
        assert row[1:] == ('LEGACY_UNENFORCED',1,'UNKNOWN_CHECK_ENUM_SET','NOT_CERTIFIED'), row
        ddl = q(c2, 'SHOW CREATE TABLE legacy_rows')[0][1]
        assert 'LEGACY_UNENFORCED' in ddl and 'FOREIGN KEY' in ddl
        assert q(c2, 'SELECT @@tarn_legacy_integrity') == (('REJECT',),)
        original = q(c2, 'SELECT * FROM legacy_rows')
        for sql in ["INSERT INTO legacy_rows VALUES(2,'v')", "REPLACE INTO legacy_rows VALUES(1,'v')",
                    "UPDATE legacy_rows SET v='v'", 'DELETE FROM legacy_rows',
                    "INSERT INTO legacy_rows SELECT id+1,v FROM legacy_rows"]:
            assert outcome(lambda: q(c2,sql)) == {'error':1235,'state':'42000'}, sql
            assert q(c2, 'SELECT * FROM legacy_rows') == original
        q(c2, "SET SESSION tarn_legacy_integrity='ALLOW'")
        assert q(c2, 'SELECT @@tarn_legacy_integrity') == (('ALLOW',),)
        q(c2, "INSERT INTO legacy_rows VALUES(2,'still-unenforced')")
        q(c2, 'DELETE FROM legacy_rows WHERE id=2')
        q(c2, 'SET SESSION tarn_legacy_integrity=DEFAULT')
        q(c2, 'ALTER TABLE legacy_rows DROP FOREIGN KEY legacy_fk')
        assert q(c2, 'SELECT * FROM legacy_rows') == ((1,'outside-old-list'),)
        row = next(r for r in q(c2, 'SHOW TARN INTEGRITY') if r[0] == 'legacy_rows')
        assert row[1:] == ('LEGACY_UNENFORCED',0,'UNKNOWN_CHECK_ENUM_SET','NOT_CERTIFIED'), row
        return {'before': ddl, 'after': q(c2, 'SHOW CREATE TABLE legacy_rows')[0][1], 'inspection': row}
    finally:
        c2.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--tarn', required=True)
    ap.add_argument('--ref', required=True)
    ap.add_argument('--reference', choices=['mysql','mariadb'], required=True)
    ap.add_argument('--kv', help='one writable leader per shard, for owned legacy fixture injection')
    ap.add_argument('--output', required=True)
    args = ap.parse_args()
    manifest = json.loads(CONTRACT.read_text())
    root = pathlib.Path(__file__).resolve().parents[2]
    try:
        revision = subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        revision = os.getenv('TARN_SOURCE_REVISION','unavailable')
    sources = [*sorted((root/'crates/tarn-sql/src').glob('*.rs')), pathlib.Path(__file__).resolve(), CONTRACT.resolve()]
    report = {'format': 'tarn-integrity-results-v1', 'settings': vars(args),
              'source_revision': revision,
              'source_sha256': {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
              'manifest_sha256': hashlib.sha256(CONTRACT.read_bytes()).hexdigest(), 'completed': False}
    db = 'integrity_'+secrets.token_hex(6)
    a, b = connect(args.tarn), connect(args.ref)
    try:
        report['versions'] = {'tarn': q(a,'SELECT VERSION()'), 'reference': q(b,'SELECT VERSION()')}
        for c in (a,b):
            q(c, 'CREATE DATABASE '+db)
            c.select_db(db)
            q(c, "SET sql_mode='STRICT_ALL_TABLES'")
            q(c, 'CREATE TABLE p(id INT PRIMARY KEY)')
            q(c, 'INSERT INTO p VALUES(1),(2)')
        report['declarations'] = declarations(a,b,manifest,args.reference)
        first, second = writes(a), writes(b)
        report['tarn_writes'], report['reference_writes'] = first, second
        assert len(first) == len(second)
        for x,y in zip(first,second):
            difference = manifest.get('intentional_error_differences',{}).get(x.get('sql'))
            if difference:
                assert x['outcome'] == difference['tarn'], x
                assert y['outcome'] == difference[args.reference], y
                assert {k:v for k,v in x.items() if k != 'outcome'} == {k:v for k,v in y.items() if k != 'outcome'}
            else:
                assert x == y, (x,y)
        report['concurrent'] = {'tarn': concurrent_unique(args.tarn,db), 'reference': concurrent_unique(args.ref,db)}
        if args.kv:
            report['legacy'] = legacy(a,args.kv,db)
        else:
            report['legacy'] = 'not requested; legacy catalog units are separate'
        report['completed'] = True
        print('PASS exact declaration refusals (text/binary/named), unchanged schema/data, supported writes/rollback/validation/conflicts and legacy controls')
    except BaseException as e:
        report['error'] = repr(e)
        raise
    finally:
        pathlib.Path(args.output).write_text(json.dumps(report,indent=2)+'\n')
        for c in (a,b):
            try:
                q(c,'DROP DATABASE '+db)
            finally:
                c.close()


if __name__ == '__main__':
    main()
