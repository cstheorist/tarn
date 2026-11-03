#!/usr/bin/env python3
"""Offline sensitivity controls for pushdown_contracts.py; no database needed."""
import copy
import json
from pathlib import Path
import tempfile
from decimal import Decimal
import struct
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pymysql
from pymysql.constants import COMMAND, FIELD_TYPE, FLAG

from pushdown_contracts import (FLAG_MASK, METRICS, StatePacket, cases,
                                decode_binary_row, difference_summary, equal,
                                case, main, observe, paths_ok, run_case, typed)


def observation():
    return dict(fields=[dict(name='lo', org_name='lo', table='', org_table='',
                catalog=typed(b'def'), db=typed(b''), type_code=8,
                flags=0, charsetnr=63, length=20, decimals=0)],
                rows=[[typed(1)]], affected_rows=None, error=None,
                warning_count=0, warnings=[])


class Sensitivity(unittest.TestCase):
    def test_exact_typed_values(self):
        base = observation()
        for value in [2, Decimal('1'), 1.0, '1', b'1', None]:
            changed = copy.deepcopy(base)
            changed['rows'] = [[typed(value)]]
            for scope in ('full', 'wire', 'typed_rows'):
                self.assertFalse(equal(base, changed, scope), (value, scope))
        self.assertNotEqual(typed(Decimal('1')), typed(Decimal('1.00')))

    def test_each_descriptor_component_matters(self):
        base = observation()
        for key, value in [('name', 'renamed'), ('type_code', 246), ('charsetnr', 45),
                           ('length', 21), ('decimals', 2),
                           ('flags', FLAG.NOT_NULL), ('flags', FLAG.UNSIGNED),
                           ('flags', FLAG.BINARY)]:
            changed = copy.deepcopy(base)
            changed['fields'][0][key] = value
            self.assertFalse(equal(base, changed, 'full'), key)
            self.assertFalse(equal(base, changed, 'wire'), key)
        for flag in (FLAG.NOT_NULL, FLAG.UNSIGNED, FLAG.BINARY):
            self.assertEqual(FLAG_MASK & flag, flag)

    def test_origin_hints_captured_but_not_reference_contract(self):
        base = observation()
        for key, value in [('org_name', 's'), ('table', 'p'), ('db', typed(b'db')),
                           ('flags', FLAG.PRI_KEY | 32768)]:
            changed = copy.deepcopy(base)
            changed['fields'][0][key] = value
            self.assertFalse(equal(base, changed, 'full'), key)
            self.assertTrue(equal(base, changed, 'wire'), key)

    def test_errors_are_not_success_and_state_is_not_errno(self):
        base = observation()
        base['error'] = dict(errno=1054, sqlstate='42S22', message='missing')
        for key, value in [('errno', 1064), ('sqlstate', '42000')]:
            changed = copy.deepcopy(base)
            changed['error'][key] = value
            self.assertFalse(equal(base, changed, 'wire'), key)
        self.assertFalse(equal(base, observation(), 'wire'))

    def test_warning_and_affected_row_changes_rejected(self):
        base = observation()
        for key, value in [('warning_count', 1), ('warnings', [[typed('Warning'), typed(1365), typed('Division by 0')]]),
                           ('affected_rows', 1), ('warnings_unavailable', {'errno': 1235})]:
            changed = copy.deepcopy(base)
            changed[key] = value
            self.assertFalse(equal(base, changed, 'wire'), key)

    def test_row_multiset_preserves_duplicates(self):
        base = observation()
        base['rows'] += [[typed(2)]]
        changed = copy.deepcopy(base)
        changed['rows'].reverse()
        self.assertFalse(equal(base, changed))
        self.assertTrue(equal(base, changed, unordered=True))
        changed['rows'].append([typed(1)])
        self.assertFalse(equal(base, changed, unordered=True))

    def test_path_controls_reject_swaps_absence_and_reset(self):
        zero = dict.fromkeys(METRICS, 0)
        reduce = {METRICS[0]: 1, METRICS[1]: 0}
        project = {METRICS[0]: 0, METRICS[1]: 1}
        self.assertTrue(paths_ok(zero, 'none'))
        self.assertTrue(paths_ok(reduce, 'reduction'))
        self.assertTrue(paths_ok(project, 'projection'))
        for expected in ('reduction', 'projection'):
            self.assertFalse(paths_ok(zero, expected))
            self.assertFalse(paths_ok({METRICS[0]: -1, METRICS[1]: 0}, expected))
        self.assertFalse(paths_ok(project, 'reduction'))
        self.assertFalse(paths_ok(reduce, 'projection'))
        self.assertFalse(paths_ok(reduce, 'none'))
        self.assertFalse(paths_ok({}, 'none'))
        self.assertFalse(paths_ok(zero, 'misspelled'))

    def test_packet_sqlstate_is_preserved(self):
        packet = StatePacket(b'\xff' + struct.pack('<H', 1235) + b'#42000unsupported', 'utf8')
        with self.assertRaises(pymysql.MySQLError) as ctx:
            packet.raise_for_error()
        self.assertEqual(ctx.exception.args[0], 1235)
        self.assertEqual(ctx.exception.wire_sqlstate, '42000')

    def test_reference_diagnostic_prose_does_not_hide_identity_changes(self):
        base = observation()
        base['error'] = dict(errno=1054, sqlstate='42S22', message='one engine')
        base['warnings'] = [[typed('Error'), typed(1054), typed('one engine')]]
        changed = copy.deepcopy(base)
        changed['error']['message'] = 'another engine'
        changed['warnings'][0][2] = typed('another engine')
        self.assertTrue(equal(base, changed, 'wire'))
        self.assertFalse(equal(base, changed, 'full'))
        self.assertIn('/warnings/0/2/value', difference_summary(base, changed)['paths'])
        for i, value in [(0, 'Warning'), (1, 1064)]:
            invalid = copy.deepcopy(changed)
            invalid['warnings'][0][i] = typed(value)
            self.assertFalse(equal(base, invalid, 'wire'))

    def test_prepared_metadata_has_independent_sensitivity(self):
        base = observation()
        base['prepare_fields'] = copy.deepcopy(base['fields'])
        base['parameter_fields'] = []
        base['prepare_warning_count'] = 0
        for key in ('type_code', 'length', 'decimals', 'flags', 'charsetnr'):
            changed = copy.deepcopy(base)
            changed['prepare_fields'][0][key] += 1
            self.assertFalse(equal(base, changed, 'wire'), key)
            self.assertFalse(equal(base, changed, 'full'), key)

    def test_binary_signed_unsigned_decimal_and_bitmap(self):
        def field(code, flags=0, charset=63):
            return SimpleNamespace(type_code=code, flags=flags, charsetnr=charset)
        fields = [field(FIELD_TYPE.LONGLONG), field(FIELD_TYPE.LONGLONG, FLAG.UNSIGNED),
                  field(FIELD_TYPE.NEWDECIMAL), field(FIELD_TYPE.DOUBLE)]
        decimal = b'9007199254740993.00'
        data = (b'\x00\x00' + struct.pack('<qQ', -9223372036854775808, 18446744073709551615)
                + bytes([len(decimal)]) + decimal + struct.pack('<d', 0.5))
        row = decode_binary_row(data, fields)
        self.assertEqual([typed(v) for v in row], [typed(-9223372036854775808),
                         typed(18446744073709551615), typed(Decimal('9007199254740993.00')), typed(0.5)])
        fields[1].flags = 0
        self.assertNotEqual(typed(decode_binary_row(data, fields)[1]), typed(row[1]))
        fields = [field(FIELD_TYPE.LONGLONG) for _ in range(8)]
        values = [None, 1, 2, 3, 4, 5, None, 7]
        # NULL bits have the protocol's two-bit offset and cross a byte here.
        nulls = (1 << 2) | (1 << 8)
        data = b'\x00' + nulls.to_bytes(2, 'little') + b''.join(struct.pack('<q', v) for v in values if v is not None)
        self.assertEqual(decode_binary_row(data, fields), tuple(values))
        for bad in [b'\x01' + data[1:], data[:-1], data+b'extra']:
            with self.assertRaises((AssertionError, struct.error)):
                decode_binary_row(bad, fields)

    def test_binary_prepare_execute_capture_and_close(self):
        def descriptor(name, flags):
            names = [b'def', b'', b'', b'', name.encode(), name.encode()]
            return (b''.join(bytes([len(v)])+v for v in names) + b'\x0c' +
                    struct.pack('<HIBHB', 63, 20, FIELD_TYPE.LONGLONG, flags, 0) + b'\x00\x00')
        signed, unsigned = descriptor('lo', FLAG.BINARY), descriptor('hi', FLAG.BINARY | FLAG.UNSIGNED)
        eof = b'\xfe\x00\x00\x02\x00'
        packets = [struct.pack('<BIHHBH', 0, 7, 2, 0, 0, 0), signed, unsigned, eof,
                   b'\x02', signed, unsigned, eof,
                   b'\x00\x00'+struct.pack('<qQ', -9223372036854775808, 18446744073709551615), eof]
        class FakeConnection:
            encoding = 'utf8'
            use_unicode = True
            decoders = pymysql.converters.decoders
            def __init__(self):
                self.commands = []
            def _execute_command(self, command, payload):
                self.commands.append((command, payload))
            def _read_packet(self, packet_type=StatePacket):
                return packet_type(packets.pop(0), self.encoding)
        conn = FakeConnection()
        with patch('pushdown_contracts.capture_warnings'):
            result = observe(conn, 'SELECT MIN(s),MAX(u) FROM p', 'binary')
        self.assertFalse(packets)
        self.assertEqual([c[0] for c in conn.commands],
                         [COMMAND.COM_STMT_PREPARE, COMMAND.COM_STMT_EXECUTE, COMMAND.COM_STMT_CLOSE])
        self.assertEqual(result['fields'], result['prepare_fields'])
        self.assertEqual(result['rows'], [[typed(-9223372036854775808), typed(18446744073709551615)]])
        self.assertEqual(result['parameter_fields'], [])

    def test_mysql_profile_order_and_exact_diagnostics_table(self):
        entry = case('mysql_extrema', 'SELECT MIN(s) FROM exact_p', profile='MYSQL')
        commands = []
        def execute_sql(_c, sql):
            if sql == "SET tarn_key_contract='MYSQL'":
                self.assertIn('SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin', commands)
            commands.append(sql)
            if sql.startswith('EXPLAIN'):
                return [[json.dumps({'key_contract': {'requested': 'MYSQL'}})]]
            return []
        with patch('pushdown_contracts.execute', side_effect=execute_sql), \
             patch('pushdown_contracts.counters', return_value=dict.fromkeys(METRICS, 0)), \
             patch('pushdown_contracts.observe', return_value=observation()):
            result = run_case(object(), entry, True)
        self.assertEqual(result['diagnostics_reset'], 'SELECT id FROM exact_p WHERE id=1')
        self.assertFalse(any(' FROM p ' in sql for sql in commands))

    def test_warning_history_resets_and_restores_explicit_mode(self):
        entry = next(e for e in cases() if e['id'] == 'division_warning_diagnostic')
        commands = []
        def observe_sql(_c, _sql, _protocol):
            self.assertEqual(commands[-1], 'SELECT id FROM p WHERE tenant=1 AND id=1')
            self.assertIn('ERROR_FOR_DIVISION_BY_ZERO', commands[0])
            return observation()
        with patch('pushdown_contracts.execute', side_effect=lambda _c, sql: commands.append(sql)), \
             patch('pushdown_contracts.observe', side_effect=observe_sql):
            run_case(object(), entry, False)
        self.assertNotIn('ERROR_FOR_DIVISION_BY_ZERO', commands[-1])

    def test_authority_gates_without_hiding_other_reference(self):
        # The real CLI comparison loop must accept only the selected authority,
        # retaining the other engine's genuine descriptor difference.
        class FakeClient:
            def __init__(self, name): self.name = name
            def close(self): pass
            def select_db(self, _db): pass
        def capture(authority, tarn_length):
            entry = case('authority_control', 'SELECT MIN(d) FROM p',
                         profile='MYSQL' if authority == 'mysql' else 'LEGACY')
            def connect_fake(address, _args): return FakeClient(address)
            def execute_fake(client, _sql):
                return [[{'ref': '10.11.19-MariaDB', 'mysql': '8.4.13'}.get(client.name, 'Tarn')]]
            def run_fake(client, _entry, _tarn):
                obs = observation()
                obs['fields'][0]['length'] = {'ref': 23, 'mysql': 22}.get(client.name, tarn_length)
                return dict(observation=obs, path_delta=dict.fromkeys(METRICS, 0))
            with tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / 'report.json'
                argv = ['probe', '--off', 'off', '--on', 'on', '--ref', 'ref', '--mysql', 'mysql', '--out', str(output)]
                with patch('sys.argv', argv), patch('builtins.print'), \
                     patch('pushdown_contracts.cases', return_value=[entry]), \
                     patch('pushdown_contracts.connect', side_effect=connect_fake), \
                     patch('pushdown_contracts.setup'), \
                     patch('pushdown_contracts.execute', side_effect=execute_fake), \
                     patch('pushdown_contracts.observe', return_value=observation()), \
                     patch('pushdown_contracts.run_case', side_effect=run_fake):
                    exit_code = main()
                return exit_code, json.loads(output.read_text())
        code, report = capture('ref', 23)
        self.assertFalse(code)
        self.assertFalse(report['cases'][0]['reference_wire_equal']['mysql'])
        self.assertTrue(report['cases'][0]['reference_pair_raw_differences']['total'])
        code, _ = capture('ref', 22)
        self.assertTrue(code)
        code, report = capture('mysql', 22)
        self.assertFalse(code)
        self.assertFalse(report['cases'][0]['reference_wire_equal']['ref'])

    def test_bounded_suite_admissions(self):
        suite = cases(True)
        self.assertLessEqual(len(suite), 34)
        self.assertEqual(len(suite), len({e['id'] for e in suite}))
        self.assertTrue(all(e['reference'] == 'wire' for e in suite
                            if not e['id'].endswith(('mysql_sum_refusal', 'division_warning_diagnostic'))))
        self.assertEqual(next(e for e in suite if e['id'] == 'mysql_sum_refusal')['expected_error'], [1235, '42000'])
        self.assertTrue(all(e['authority'] == ('mysql' if e['profile'] == 'MYSQL' else 'ref') for e in suite))
        self.assertEqual(next(e for e in suite if e['id'] == 'row_predicate_overflow')['expected_error'], 'authority')
        self.assertGreaterEqual(sum(e['protocol'] == 'binary' for e in suite), 6)


if __name__ == '__main__':
    unittest.main()
