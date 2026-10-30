#!/usr/bin/env python3
"""Offline controls for the actual SQL fault driver's read-only warm-up.

The wrapped fixture is the SQL error observed in #181 run40978557088,
history sequence133. It includes SqlError::conflict's outer wire text.
These controls mock SQL execution, start no services and perform no writes.
"""
import unittest
from types import SimpleNamespace
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
