#!/usr/bin/env python3
"""Offline sensitivity controls for pushdown_contracts.py; no database needed."""
import copy
from decimal import Decimal
import struct
import unittest

import pymysql
from pymysql.constants import FLAG

from pushdown_contracts import (FLAG_MASK, METRICS, StatePacket, cases,
                                equal, paths_ok, typed)


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

    def test_bounded_suite_admissions(self):
        suite = cases(True)
        self.assertLessEqual(len(suite), 24)
        self.assertEqual(len(suite), len({e['id'] for e in suite}))
        self.assertTrue(all(e['reference'] == 'wire' for e in suite
                            if e['id'] not in ('mysql_sum_refusal', 'division_warning_diagnostic')))
        self.assertEqual(next(e for e in suite if e['id'] == 'mysql_sum_refusal')['expected_error'], [1235, '42000'])


if __name__ == '__main__':
    unittest.main()
