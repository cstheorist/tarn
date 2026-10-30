#!/usr/bin/env python3
"""Offline controls for the actual SQL fault driver's read-only warm-up.

The wrapped fixture is the SQL error observed in #181 run40978557088,
history sequence133. It includes SqlError::conflict's outer wire text.
These controls mock SQL execution, start no services and perform no writes.
"""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import kvfaults
import kvtest

INNER_RO = "shard leader changed, retry the transaction (Can't modify data on a read-only instance)"
WIRE_RO = "Deadlock found when trying to get lock; try restarting transaction (" + INNER_RO + ")"


def refusal(message=WIRE_RO, state="40001", code=1213):
    error = kvfaults.pymysql.OperationalError(code, message)
    if state is not None:
        error.sqlstate = state
    return error


def driver():
    test = object.__new__(kvfaults.FaultTest)
    test.events, test.completed, test.ack = [], 0, 42
    test.args = SimpleNamespace(sql_pool_size=4)

    def record(event, **values):
        test.events.append({"event": event, **values})
        return len(test.events)

    test.record = record
    return test


def quorum_driver(error=None, result=None):
    test = driver()
    test.tenant, test.values = 7, {"memtx": {1: 11, 2: 12, 99: None}}
    test.paired_values = Mock()
    test.leader = Mock(return_value=({"node_id": "s1r1", "endpoint": "owner"}, {"table": {}, "ownership": {}}))
    test.configuration = Mock(return_value={"clock": {"fault": None}})
    test.group_nodes = Mock(return_value=[{"node_id": "s1r" + str(i)} for i in (1, 2, 3)])
    test.signal, test.restore, test.retarget, test.caught_up = Mock(), Mock(), Mock(), Mock()
    test.public = Mock(return_value={"version": 1, "status": "error", "error": {
        "kind": "unavailable", "domain": "tarantool", "code": 0, "message": "timed out"}})
    test.state = Mock(return_value={"clock": {"fault": None}, "ro": True})
    test.sdk = Mock(return_value={"version": 1, "status": "error", "error": {"kind": "transport"}})
    test.connect = MagicMock()
    test.sql = Mock(side_effect=error, return_value=result)
    return test


class ReadonlyRouteControls(unittest.TestCase):
    def test_actual_wire_refusal_then_positive_select_keeps_ack_and_mutations_untouched(self):
        test, error = driver(), refusal()
        with patch.object(kvtest, "q", side_effect=[error, ((10,),)]) as query, \
                patch.object(kvfaults.time, "monotonic", return_value=0), patch.object(kvfaults.time, "sleep"):
            result = test.warm_readonly_route(object(), "owned_memtx", 7, 10, "exact positive")
        self.assertEqual(result, ((10,),))
        self.assertEqual(query.call_count, 2)
        self.assertEqual(test.ack, 42)
        self.assertEqual(test.completed, 1)
        for call in query.call_args_list:
            self.assertEqual(call.args[1], "SELECT v FROM owned_memtx WHERE tenant=%s AND id=1")
            self.assertEqual(call.args[2], (7,))
        event = next(e for e in test.events if e["event"] == "known_ro_readonly_warmup_refusal")
        self.assertEqual(event["error"]["args"], [1213, WIRE_RO])
        self.assertEqual(event["error"]["sqlstate"], "40001")
        self.assertFalse(event["guard_coverage"])
        self.assertEqual(sum(e["event"] == "sql_invoke" for e in test.events), 2)

    def test_inner_cause_without_provided_state_does_not_invent_state(self):
        test = driver()
        with patch.object(kvtest, "q", side_effect=[refusal(INNER_RO, None), ((10,),)]) as query, \
                patch.object(kvfaults.time, "monotonic", return_value=0), patch.object(kvfaults.time, "sleep"):
            test.warm_readonly_route(object(), "owned", 7, 10, "exact positive")
        event = next(e for e in test.events if e["event"] == "known_ro_readonly_warmup_refusal")
        self.assertIsNone(event["error"]["sqlstate"])
        self.assertEqual(query.call_count, 2)

    def test_all_configured_stale_clients_allow_one_exact_positive_attempt(self):
        test = driver()
        responses = [refusal() for _ in range(test.args.sql_pool_size)] + [((10,),)]
        with patch.object(kvtest, "q", side_effect=responses) as query, \
                patch.object(kvfaults.time, "monotonic", return_value=0), patch.object(kvfaults.time, "sleep"):
            result = test.warm_readonly_route(object(), "owned", 7, 10, "exact positive")
        self.assertEqual(result, ((10,),))
        self.assertEqual(query.call_count, 5)
        self.assertEqual(test.ack, 42)
        self.assertEqual(test.completed, 1)
        refusals = [e for e in test.events if e["event"] == "known_ro_readonly_warmup_refusal"]
        self.assertEqual([e["attempt"] for e in refusals], [1, 2, 3, 4])
        self.assertTrue(all(e["sql_pool_size"] == 4 and e["max_attempts"] == 5
                            and not e["guard_coverage"] for e in refusals))
        for call in query.call_args_list:
            self.assertTrue(call.args[1].startswith("SELECT "))

    def test_40003_and_other_provided_states_are_never_retried(self):
        for state in ("40003", b"40003", "HY000", "40P01"):
            with self.subTest(state=state):
                test, error = driver(), refusal(state=state)
                with patch.object(kvtest, "q", side_effect=error) as query:
                    with self.assertRaises(kvfaults.pymysql.OperationalError) as caught:
                        test.warm_readonly_route(object(), "owned", 7, 10, "positive")
                self.assertIs(caught.exception, error)
                self.assertEqual(query.call_count, 1)
                self.assertEqual(test.ack, 42)

    def test_unrelated_serialization_and_near_miss_wrappers_are_not_retried(self):
        messages = (
            "transaction conflict", "Deadlock found when trying to get lock; try restarting transaction (transaction conflict)",
            "unrelated prefix: " + WIRE_RO, WIRE_RO + " unrelated suffix", WIRE_RO[:-1],
            "Deadlock found when trying to get lock; try restarting transaction (" + WIRE_RO + ")",
            INNER_RO.replace("Can't modify data on a read-only instance", "a read-only instance timed out"),
        )
        for message in messages:
            with self.subTest(message=message):
                test, error = driver(), refusal(message)
                with patch.object(kvtest, "q", side_effect=error) as query:
                    with self.assertRaises(kvfaults.pymysql.OperationalError) as caught:
                        test.warm_readonly_route(object(), "owned", 7, 10, "positive")
                self.assertIs(caught.exception, error)
                self.assertEqual(query.call_count, 1)

    def test_unknown_transport_and_other_codes_are_never_retried(self):
        for error in (refusal(code=2013), refusal(code=7), TimeoutError("read timeout"), OSError("socket closed")):
            with self.subTest(error=error):
                test = driver()
                with patch.object(kvtest, "q", side_effect=error) as query:
                    with self.assertRaises(type(error)) as caught:
                        test.warm_readonly_route(object(), "owned", 7, 10, "positive")
                self.assertIs(caught.exception, error)
                self.assertEqual(query.call_count, 1)

    def test_attempt_limit_preserves_the_final_original_error(self):
        test, errors = driver(), [refusal() for _ in range(5)]
        with patch.object(kvtest, "q", side_effect=errors) as query, \
                patch.object(kvfaults.time, "monotonic", return_value=0), patch.object(kvfaults.time, "sleep"):
            with self.assertRaises(kvfaults.pymysql.OperationalError) as caught:
                test.warm_readonly_route(object(), "owned", 7, 10, "positive")
        self.assertIs(caught.exception, errors[-1])
        self.assertEqual(query.call_count, 5)
        self.assertEqual(test.completed, 0)

    def test_retry_deadline_preserves_the_original_error(self):
        test, error = driver(), refusal()
        with patch.object(kvtest, "q", side_effect=error) as query, \
                patch.object(kvfaults.time, "monotonic", side_effect=[0, 11]):
            with self.assertRaises(kvfaults.pymysql.OperationalError) as caught:
                test.warm_readonly_route(object(), "owned", 7, 10, "positive")
        self.assertIs(caught.exception, error)
        self.assertEqual(query.call_count, 1)

    def test_wrong_positive_image_is_never_retried_or_counted(self):
        test = driver()
        with patch.object(kvtest, "q", return_value=((99,),)) as query:
            with self.assertRaises(AssertionError):
                test.warm_readonly_route(object(), "owned", 7, 10, "positive")
        self.assertEqual(query.call_count, 1)
        self.assertEqual(test.completed, 0)
        self.assertEqual(test.ack, 42)

    def test_quorum_timeout_namespace_cause_and_state_are_exact(self):
        message = "Lost connection to MySQL server during query (timed out)"
        self.assertEqual(kvfaults.quorum_sql_outcome(refusal(message, None, 2013)), "client_read_timeout")
        self.assertEqual(kvfaults.quorum_sql_outcome(refusal()), "server_refusal")
        for error in (refusal(message, "40003", 2013), refusal(message, "HY000", 2013),
                      refusal(message + " unrelated", None, 2013), refusal("Lost connection", None, 2013),
                      refusal(message, None, 1041), refusal(state="40003"),
                      RuntimeError(2013, message), None):
            with self.subTest(error=error):
                self.assertIsNone(kvfaults.quorum_sql_outcome(error))

    def test_actual_quorum_controller_records_one_timeout_without_guard_claim(self):
        test = quorum_driver(refusal("Lost connection to MySQL server during query (timed out)", None, 2013))
        test.scenario_quorum("memtx")
        test.sql.assert_called_once()
        test.restore.assert_called_once()
        self.assertEqual(test.paired_values.call_count, 2)  # Mandatory before and healed positive paths.
        event = next(e for e in test.events if e["event"] == "no_quorum_classification")
        self.assertEqual(event["sql_outcome"]["kind"], "client_read_timeout")
        self.assertIsNone(event["sql_outcome"]["error"]["sqlstate"])
        self.assertFalse(event["sql_outcome"]["guard_coverage"])
        self.assertFalse(event["sql_outcome"]["row_result_returned"])
        self.assertEqual(event["sql_outcome"]["configured_client_read_timeout_seconds"], 15)
        self.assertEqual(event["direct_outcome_origin"], "versioned_api_error_envelope")

    def test_quorum_controller_rejects_successful_rows_and_always_restores(self):
        test = quorum_driver(result=((11,),))
        with self.assertRaises(AssertionError):
            test.scenario_quorum("memtx")
        test.sql.assert_called_once()
        test.restore.assert_called_once()
        self.assertEqual(test.paired_values.call_count, 1)
        self.assertFalse(any(e["event"] == "no_quorum_classification" for e in test.events))

    def test_shared_healed_path_requires_exact_sql_and_sdk_positives(self):
        test = driver()
        test.db, test.connect = "owned", MagicMock()
        test.sdk = Mock(return_value={"status": "ok", "rows": [{"tenant": 7, "id": 1, "v": 11}, None],
                                    "schema": {"db": "owned", "name": "t", "columns": [
                                        {"name": "tenant"}, {"name": "id"}, {"name": "v"}]}})
        with patch.object(kvtest, "q", side_effect=[refusal(), ((11,),), ((7, 1, 11),), (), ((11,),), ()]), \
                patch.object(kvfaults.time, "sleep"):
            test.paired_values("t", 7, {1: 11, 99: None})
        test.sdk.assert_called_once_with("t", [[7, 1], [7, 99]])
        self.assertEqual(test.completed, 5)
        self.assertEqual(test.ack, 42)

    def test_bad_healed_sql_image_cannot_proceed_to_sdk_or_count_success(self):
        test = driver()
        test.connect, test.sdk = MagicMock(), Mock()
        with patch.object(kvtest, "q", return_value=((999,),)) as query:
            with self.assertRaises(AssertionError):
                test.paired_values("t", 7, {1: 11, 99: None})
        query.assert_called_once()
        test.sdk.assert_not_called()
        self.assertEqual(test.completed, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
