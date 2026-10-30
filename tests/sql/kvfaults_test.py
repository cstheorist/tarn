#!/usr/bin/env python3
"""Fault-fixture discovery retries stay bounded and read-only."""
import unittest
from unittest.mock import Mock, call, patch

import pymysql
from kvfaults import FaultTest


class WarmRouteRead(unittest.TestCase):
    def setUp(self):
        self.test = FaultTest.__new__(FaultTest)
        self.test.identities = [{"id": 1}]
        self.test.group_nodes = Mock(return_value=[{"endpoint": "a"}])
        self.test.sql = Mock()
        self.test.record = Mock()
        self.connection = Mock(server_status=0)
        self.connection.get_autocommit.return_value = True
        self.sleep = patch("kvfaults.time.sleep").start()
        self.addCleanup(patch.stopall)

    def read(self):
        return self.test.warm_route_read(self.connection, "fault_vinyl", 7)

    def test_cached_discovery_and_changed_leader_retry_only_select(self):
        self.test.sql.side_effect = [
            pymysql.OperationalError(2003, "shard 1 has no reachable leader (a is read-only)"),
            pymysql.OperationalError(1213, "shard leader changed, retry the transaction (RO7)"),
            ((10,),),
        ]
        self.assertEqual(self.read(), ((10,),))
        self.assertEqual(self.test.sql.call_args_list, [call(
            self.connection, "SELECT v FROM fault_vinyl WHERE tenant=%s AND id=1", (7,))] * 3)
        self.assertEqual(self.test.record.call_count, 2)

    def test_repeated_refusal_is_terminal_after_eight_attempts(self):
        error = pymysql.OperationalError(2003, "shard 1 has no reachable leader (a is read-only)")
        self.test.sql.side_effect = error
        with self.assertRaises(pymysql.OperationalError) as caught:
            self.read()
        self.assertIs(caught.exception, error)
        self.assertEqual(self.test.sql.call_count, 8)
        self.assertEqual(self.sleep.call_count, 7)

    def test_other_errors_are_not_retried(self):
        for code, message in [(2013, "Lost connection during query"),
                              (2003, "Can't connect to MySQL server"),
                              (2003, "shard 2 has no reachable leader (a is read-only)"),
                              (2003, "shard 1 has no reachable leader (other is read-only)"),
                              (1213, "transaction outcome unknown"),
                              (1205, "clock safety unavailable")]:
            with self.subTest(code=code, message=message):
                self.test.sql.reset_mock()
                error = pymysql.OperationalError(code, message)
                self.test.sql.side_effect = error
                with self.assertRaises(pymysql.OperationalError) as caught:
                    self.read()
                self.assertIs(caught.exception, error)
                self.assertEqual(self.test.sql.call_count, 1)

    def test_active_transaction_or_disabled_autocommit_refuses_before_read(self):
        for autocommit, status in [(False, 0), (True, 1)]:
            with self.subTest(autocommit=autocommit, status=status):
                self.connection.get_autocommit.return_value = autocommit
                self.connection.server_status = status
                with self.assertRaises(AssertionError):
                    self.read()
        self.test.sql.assert_not_called()


if __name__ == "__main__":
    unittest.main()
