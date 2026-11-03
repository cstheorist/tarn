#!/usr/bin/env python3
"""Bounded text-protocol controls for query_pushdown; isolated test clusters only.

--off and --on MUST address gateways over the SAME Tarn shards, configured with
query_pushdown false and true respectively. Setup writes only through --off;
per-session own-write histories roll back before the next connection runs.
No mutation is retried. Run this probe without other clients: path counters are
process-global. --oracle-only qualifies reference fixtures without Tarn.

All off/on observations compare full descriptors (including origin/raw flags),
typed values, affected rows, errno/SQLSTATE and warnings. Reference 'wire' cases
compare aliases, type, semantic flags, charset, length and decimals; origin/index
flags and NUM_FLAG describe provenance/driver hints, not this result contract.
NOT_NULL, UNSIGNED and BINARY are NEVER masked out. Raw descriptors are retained.
Every admitted reference shape requires this complete descriptor contract;
metadata differences fail instead of being waived as implementation gaps.
'diagnostic' cases admit no equivalence. --observe-only collects failures without
claiming a release pass, for initial qualification.
PyMySQL text only; no prepared/binary or transport/fault/recovery certification.
"""
import argparse
from decimal import Decimal
import importlib.metadata
import json
from pathlib import Path
import sys

import pymysql
from pymysql.constants import FLAG
from pymysql.protocol import MysqlPacket

DB = 'pushdown_wire_contracts'
METRICS = ('Tarn_storage_reductions', 'Tarn_storage_projections')
# Excludes PRI_KEY, UNIQUE_KEY, MULTIPLE_KEY, PART_KEY, NUM_FLAG and origin
# AUTO_INCREMENT. Keep all flags that change the interpretation of our values.
FLAG_MASK = (FLAG.NOT_NULL | FLAG.BLOB | FLAG.UNSIGNED | FLAG.ZEROFILL |
             FLAG.BINARY | FLAG.ENUM | FLAG.TIMESTAMP | FLAG.SET)
FIELD_KEYS = ('name', 'type_code', 'charsetnr', 'length', 'decimals')
ROOT = Path(__file__).resolve().parents[2]


class StatePacket(MysqlPacket):
    """Preserve SQLSTATE from ERR packets, discarded by PyMySQL's exception API."""
    def raise_for_error(self):
        state = self._data[4:9].decode('ascii') if self._data[3:4] == b'#' else None
        try:
            super().raise_for_error()
        except pymysql.MySQLError as exc:
            exc.wire_sqlstate = state
            raise


class Connection(pymysql.connections.Connection):
    def _read_packet(self, packet_type=MysqlPacket):
        return super()._read_packet(StatePacket if packet_type is MysqlPacket else packet_type)


def typed(value):
    # JSON tags prevent Decimal(1), int(1), float(1) and '1' comparing equal.
    if value is None:
        return {'type': 'null'}
    if isinstance(value, Decimal):
        return {'type': 'decimal', 'value': str(value)}
    if isinstance(value, bytes):
        return {'type': 'bytes', 'hex': value.hex()}
    if isinstance(value, float):
        return {'type': 'float', 'hex': value.hex()}
    if type(value) in (str, int, bool):
        return {'type': type(value).__name__, 'value': value}
    raise TypeError('unregistered result type: ' + type(value).__name__)


def descriptors(fields):
    return [dict(name=f.name, org_name=f.org_name, table=f.table_name,
                 org_table=f.org_table, catalog=typed(f.catalog), db=typed(f.db),
                 type_code=f.type_code, flags=f.flags, charsetnr=f.charsetnr,
                 length=f.length, decimals=f.scale) for f in fields]


def execute(c, sql, params=None):
    with c.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else cur.rowcount


def observe(c, sql):
    out = {'fields': [], 'rows': [], 'affected_rows': None, 'error': None,
           'warning_count': None, 'warnings': []}
    with c.cursor() as cur:
        try:
            cur.execute(sql)
            if cur.description:
                out['fields'] = descriptors(cur._result.fields)
                rows = cur.fetchall()
                if len(rows) > 600:
                    raise AssertionError('probe row bound exceeded')
                out['rows'] = [[typed(v) for v in row] for row in rows]
            else:
                out['affected_rows'] = cur.rowcount
            out['warning_count'] = cur._result.warning_count
        except pymysql.MySQLError as exc:
            out['error'] = {'errno': exc.args[0],
                            'sqlstate': getattr(exc, 'wire_sqlstate', None),
                            'message': str(exc.args[1])[:512]}
    # Must precede status reads or any other statement that clears diagnostics.
    try:
        warnings = execute(c, 'SHOW WARNINGS')
        out['warnings'] = [[typed(v) for v in row] for row in warnings[:64]]
        if len(warnings) > 64:
            raise AssertionError('probe warning bound exceeded')
    except pymysql.MySQLError as exc:
        out['warnings_unavailable'] = {'errno': exc.args[0],
                                       'sqlstate': getattr(exc, 'wire_sqlstate', None)}
    return out


def comparison_view(obs, scope='full', unordered=False):
    out = dict(obs)
    if unordered:
        out['rows'] = sorted(out['rows'], key=lambda r: json.dumps(r, sort_keys=True))
    if scope != 'full':
        # Error prose is implementation-specific. Retain it in the raw capture.
        if out['error']:
            out['error'] = {k: out['error'][k] for k in ('errno', 'sqlstate')}
        # Warning message text remains exact; no warning-code-only normalization.
        if scope == 'wire':
            out['fields'] = [{**{k: f[k] for k in FIELD_KEYS},
                              'flags': f['flags'] & FLAG_MASK} for f in obs['fields']]
        elif scope == 'typed_rows':
            out.pop('fields')
        else:
            raise ValueError(scope)
    return out


def equal(a, b, scope='full', unordered=False):
    return comparison_view(a, scope, unordered) == comparison_view(b, scope, unordered)


def paths_ok(delta, expected):
    if set(delta) != set(METRICS) or any(v < 0 for v in delta.values()):
        return False
    if expected == 'reduction':
        return delta[METRICS[0]] > 0 and delta[METRICS[1]] == 0
    if expected == 'projection':
        return delta[METRICS[1]] > 0 and delta[METRICS[0]] == 0
    return expected == 'none' and all(v == 0 for v in delta.values())


def counters(c):
    result = {}
    for metric in METRICS:
        rows = execute(c, "SHOW GLOBAL STATUS LIKE '" + metric + "'")
        if len(rows) != 1:
            raise AssertionError('missing path counter: ' + metric)
        result[metric] = int(rows[0][1])
    return result


def case(name, sql, path='none', reference='wire', unordered=False,
         before=(), after=(), error=None, profile='LEGACY'):
    return dict(id=name, sql=sql, path=path, reference=reference,
                unordered=unordered, before=list(before), after=list(after),
                expected_error=error, profile=profile)


def cases(mysql_profile=False):
    extrema = 'MIN(s) AS lo, MAX(s) AS hi, MIN(u) AS ulo, MAX(u) AS uhi'
    result = [
        case('integer_extrema', 'SELECT ' + extrema + ' FROM p', 'reduction', 'wire'),
        case('empty_nonnullable_extrema', 'SELECT ' + extrema + ' FROM p WHERE tag=99', 'reduction', 'wire'),
        case('all_null_extrema', 'SELECT MIN(n) AS lo,MAX(n) AS hi FROM p WHERE tag=2', 'reduction', 'wire'),
        case('nullable_extrema', 'SELECT MIN(n) AS lo,MAX(n) AS hi FROM p', 'reduction', 'wire'),
        case('integer_sums', 'SELECT SUM(n) AS total,SUM(s) AS stotal,SUM(u) AS utotal,COUNT(n) AS present FROM p', 'reduction'),
        case('empty_sums', 'SELECT SUM(n) AS total,COUNT(n) AS present FROM p WHERE tag=99', 'reduction'),
        case('all_null_sums', 'SELECT SUM(n) AS total,COUNT(n) AS present FROM p WHERE tag=2', 'reduction'),
        case('integer_projection', 'SELECT s,u FROM p WHERE tag=1', 'projection', 'wire', True),
        case('empty_projection', 'SELECT s,u FROM p WHERE tag=99', 'projection', 'wire', True),
        case('boundary_predicate', 'SELECT s,u FROM p WHERE u>9007199254740993', 'projection', 'wire', True),
        case('decimal_fallback', 'SELECT SUM(price) AS total,MIN(price) AS lo,MAX(price) AS hi FROM p'),
        case('float_fallback', 'SELECT SUM(d) AS total,MIN(d) AS lo,MAX(d) AS hi FROM p'),
        case('ci_fallback', "SELECT MIN(n) AS lo,MAX(n) AS hi FROM p WHERE name='ÉCLAIR'"),
        case('computed_fallback', 'SELECT MIN(n) AS lo,MAX(n) AS hi FROM p WHERE n+0>=0'),
        case('unknown_column', 'SELECT missing FROM p', error=[1054, '42S22']),
        case('duplicate_key', "INSERT INTO p(tenant,id,s,u) VALUES(1,1,0,0)", error=[1062, '23000']),
        # Diagnostic captures intentional/unsupported warning behavior. No
        # reference equivalence is asserted for this legacy expression shape.
        case('division_warning_diagnostic', 'SELECT 1/0 AS quotient', reference='diagnostic'),
    ]
    writes = ['BEGIN', 'UPDATE p SET n=123,u=18446744073709551615 WHERE tenant=1 AND id=1',
              'DELETE FROM p WHERE tenant=2 AND id=2',
              "INSERT INTO p(tenant,id,n,s,u,tag) VALUES(3,900,-999,-9,0,1)"]
    result.extend([
        case('own_writes', 'SELECT SUM(n) AS total,MIN(n) AS lo,MAX(n) AS hi FROM p',
             'reduction', before=writes, after=['ROLLBACK']),
        case('own_projection', 'SELECT tenant,id,n FROM p WHERE tag=1',
             'projection', unordered=True, before=writes, after=['ROLLBACK']),
        case('savepoint_rollback', 'SELECT SUM(n) AS total,MIN(n) AS lo,MAX(n) AS hi FROM p',
             'reduction', before=writes + ['SAVEPOINT s',
                 'UPDATE p SET n=NULL WHERE tenant=1 AND id=1', 'ROLLBACK TO s'], after=['ROLLBACK']),
    ])
    if mysql_profile:
        result += [
            case('mysql_extrema', 'SELECT MIN(s) AS lo,MAX(s) AS hi FROM exact_p', 'reduction', 'wire', profile='MYSQL'),
            case('mysql_sum_refusal', 'SELECT SUM(s) AS total FROM exact_p',
                 reference='diagnostic', error=[1235, '42000'], profile='MYSQL'),
        ]
    return result


def setup(c, engine, mysql_profile):
    execute(c, 'DROP DATABASE IF EXISTS ' + DB)
    execute(c, 'CREATE DATABASE ' + DB)
    c.select_db(DB)
    execute(c, 'CREATE TABLE p (tenant INT NOT NULL,id INT NOT NULL,n BIGINT,'
            's BIGINT NOT NULL,u BIGINT UNSIGNED NOT NULL,tag INT DEFAULT 0,'
            'price DECIMAL(10,2),d DOUBLE,name VARCHAR(30) COLLATE utf8mb4_unicode_ci,'
            'wide VARCHAR(200),PRIMARY KEY(tenant,id)) ENGINE=' + engine + ' PARTITION BY KEY(tenant)')
    rows = [(1, 1, -7, -9223372036854775808, 18446744073709551615, 1, Decimal('1.25'), 0.5, 'Éclair', 'x'*100),
            (2, 2, 7, 9223372036854775807, 9007199254740993, 1, Decimal('-1.25'), -0.5, 'eclair', 'y'*100),
            (1, 3, None, 0, 0, 2, None, None, None, 'z'*100)]
    rows += [(1+i % 2, i, i % 7-3, i, i, 0, Decimal('0.25'), 0.25, 'other', 'w'*100)
             for i in range(4, 521)]
    with c.cursor() as cur:
        for start in range(0, len(rows), 128):
            cur.executemany('INSERT INTO p VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', rows[start:start+128])
    if mysql_profile:
        execute(c, 'CREATE TABLE exact_p (id INT PRIMARY KEY,s BIGINT NOT NULL) ENGINE=' + engine)
        execute(c, 'INSERT INTO exact_p VALUES(1,-9223372036854775808),(2,9223372036854775807)')


def connect(address, args):
    host, port = address.rsplit(':', 1)
    c = Connection(host=host, port=int(port), user=args.user, password=args.password,
                   charset='utf8mb4', autocommit=True, connect_timeout=10,
                   read_timeout=30, write_timeout=30)
    execute(c, "SET NAMES utf8mb4 COLLATE utf8mb4_bin")
    execute(c, "SET sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")
    execute(c, "SET time_zone='+00:00'")
    return c


def run_case(c, entry, tarn):
    if tarn:
        execute(c, "SET tarn_key_contract='" + entry['profile'] + "'")
        if entry['profile'] == 'MYSQL':
            execute(c, 'SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin')
            plan = json.loads(execute(c, 'EXPLAIN FORMAT=JSON SELECT s FROM exact_p')[0][0])
            if plan['key_contract']['requested'] != 'MYSQL':
                raise AssertionError('MYSQL profile not active')
        else:
            execute(c, 'SET NAMES utf8mb4 COLLATE utf8mb4_bin')
    for sql in entry['before']:
        execute(c, sql)
    try:
        before = counters(c) if tarn else None
        observation = observe(c, entry['sql'])
        after = counters(c) if tarn else None
        delta = {k: after[k]-before[k] for k in METRICS} if tarn else None
        return {'observation': observation, 'path_delta': delta}
    finally:
        for sql in entry['after']:
            execute(c, sql)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--off', help='Tarn gateway with query_pushdown=false')
    ap.add_argument('--on', help='Tarn gateway with query_pushdown=true, same shards as --off')
    ap.add_argument('--ref', required=True, help='MariaDB 10.11 host:port')
    ap.add_argument('--mysql', help='optional pinned MySQL host:port (contracts/key-semantics.json)')
    ap.add_argument('--mysql-profile', action='store_true', help='also test MYSQL-profile extrema and explicit SUM refusal')
    ap.add_argument('--observe-only', action='store_true', help='capture all discrepancies; never label the run passed')
    ap.add_argument('--oracle-only', action='store_true', help='qualify reference fixture/captures without Tarn; no release pass')
    ap.add_argument('--engine', choices=['memtx', 'vinyl'], default='memtx')
    ap.add_argument('--user', default='sb')
    ap.add_argument('--password', default='')
    ap.add_argument('--out', type=Path, required=True)
    args = ap.parse_args()
    if not args.oracle_only and not (args.off and args.on):
        ap.error('--off and --on are required unless --oracle-only')
    report = {'schema': 1, 'driver': 'PyMySQL text', 'driver_version': importlib.metadata.version('PyMySQL'),
              'oracle_only': args.oracle_only, 'observe_only': args.observe_only, 'engine': args.engine,
              'reference_semantic_flag_mask': FLAG_MASK, 'servers': {}, 'cases': [],
              'complete': False, 'harness_errors': [], 'failures': []}
    clients = {}
    try:
        addresses = [('ref', args.ref)] + ([('mysql', args.mysql)] if args.mysql else [])
        if not args.oracle_only:
            addresses = [('off', args.off), ('on', args.on)] + addresses
        for name, address in addresses:
            c = connect(address, args)
            clients[name] = c
            version = execute(c, 'SELECT VERSION()')[0][0]
            report['servers'][name] = {'address': address, 'version': version}
            if name == 'mysql':
                pinned = json.loads((ROOT / 'contracts/key-semantics.json').read_text())['oracle']
                report['servers'][name]['pin'] = pinned
                if version != pinned['version']:
                    raise AssertionError('MySQL version does not match pinned oracle')
            if name == 'ref' and not ('10.11.' in version and 'MariaDB' in version):
                raise AssertionError('--ref must be MariaDB 10.11')
            if name != 'on':
                setup(c, args.engine if name == 'off' else 'InnoDB', args.mysql_profile)
            else:
                c.select_db(DB)
        if not args.oracle_only:
            # A full typed fixture fingerprint through both gateways detects
            # separately populated/different clusters before testing paths.
            fingerprint = 'SELECT tenant,id,n,s,u,tag,price,d,name,wide FROM p ORDER BY tenant,id'
            if not equal(observe(clients['off'], fingerprint), observe(clients['on'], fingerprint)):
                raise AssertionError('--off/--on fixture observations differ')
        for entry in cases(args.mysql_profile):
            record = {'contract': entry, 'results': {}, 'checks': []}
            report['cases'].append(record)
            for name, c in clients.items():
                try:
                    record['results'][name] = run_case(c, entry, name in ('off', 'on'))
                except Exception as exc:
                    record['results'][name] = {'harness_error': repr(exc)[:1024]}
                    report['harness_errors'].append(entry['id'] + ': ' + name + ' harness error')
                    report['failures'].append(entry['id'] + ': ' + name + ' harness error')
            def check(label, passed):
                record['checks'].append({'check': label, 'passed': passed})
                if not passed:
                    report['failures'].append(entry['id'] + ': ' + label)
            observations = {n: r['observation'] for n, r in record['results'].items() if 'observation' in r}
            for name in ('off', 'on'):
                if name not in observations:
                    continue
                obs = observations[name]
                expected = entry['expected_error']
                observed_error = [obs['error']['errno'], obs['error']['sqlstate']] if obs['error'] else None
                check(name + ' expected error/success', observed_error == expected)
                check(name + ' path', paths_ok(record['results'][name]['path_delta'], entry['path'] if name == 'on' else 'none'))
            if 'off' in observations and 'on' in observations:
                check('off/on full wire', equal(observations['off'], observations['on'], unordered=entry['unordered']))
            for name in ('ref', 'mysql'):
                if name not in observations:
                    continue
                # MYSQL-profile SUM refusal is deliberately Tarn-only.
                if entry['id'] != 'mysql_sum_refusal':
                    err = observations[name]['error']
                    check(name + ' expected error/success', ([err['errno'], err['sqlstate']] if err else None) == entry['expected_error'])
                if 'off' in observations:
                    record.setdefault('reference_wire_equal', {})[name] = equal(observations['off'], observations[name], 'wire', entry['unordered'])
                    if entry['reference'] != 'diagnostic':
                        check('off/' + name + ' ' + entry['reference'], equal(observations['off'], observations[name], entry['reference'], entry['unordered']))
            if 'ref' in observations and 'mysql' in observations:
                record['reference_pair_wire_equal'] = equal(observations['ref'], observations['mysql'], 'wire', entry['unordered'])
            print(entry['id'] + ': ' + ('FAIL' if any(not c['passed'] for c in record['checks']) else 'captured' if args.oracle_only else 'PASS'), flush=True)
        report['complete'] = not report['harness_errors']
        report['status'] = ('failed' if not report['complete'] else 'observed' if args.observe_only
                            else 'failed' if report['failures'] else 'oracle-captured' if args.oracle_only else 'passed')
    except Exception as exc:
        report['harness_errors'].append(repr(exc)[:1024])
        report['failures'].append(repr(exc)[:1024])
        report['status'] = 'failed'
    finally:
        for c in clients.values():
            c.close()
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
    return bool(report['harness_errors']) or (bool(report['failures']) and not args.observe_only)


if __name__ == '__main__':
    sys.exit(main())
