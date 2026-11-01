#!/usr/bin/env python3
"""Offline controls for the actual SQL fault driver's route and scan controls.

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


def connection(autocommit=True, server_status=0):
    return SimpleNamespace(get_autocommit=Mock(return_value=autocommit), server_status=server_status)


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
    test.identities = [{"id": 1}, {"id": 2}]
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


def scan_state(**changes):
    state = {"owner": "owned-nonce", "observer_done": True, "observer_error": False,
             "reached": 1, "reached_result": ["ok", 1], "observed_at": 12.5,
             "released": False, "expired": False, "observer_joined": False}
    state.update(changes)
    return state


class ReadonlyRouteControls(unittest.TestCase):
    def test_actual_wire_refusal_then_positive_select_keeps_ack_and_mutations_untouched(self):
        test, error = driver(), refusal()
        with patch.object(kvtest, "q", side_effect=[error, ((10,),)]) as query, \
                patch.object(kvfaults.time, "monotonic", return_value=0), patch.object(kvfaults.time, "sleep"):
            result = test.warm_readonly_route(connection(), "owned_memtx", 7, 10, "exact positive")
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
            test.warm_readonly_route(connection(), "owned", 7, 10, "exact positive")
        event = next(e for e in test.events if e["event"] == "known_ro_readonly_warmup_refusal")
        self.assertIsNone(event["error"]["sqlstate"])
        self.assertEqual(query.call_count, 2)

    def test_all_configured_stale_clients_allow_one_exact_positive_attempt(self):
        test = driver()
        responses = [refusal() for _ in range(test.args.sql_pool_size)] + [((10,),)]
        with patch.object(kvtest, "q", side_effect=responses) as query, \
                patch.object(kvfaults.time, "monotonic", return_value=0), patch.object(kvfaults.time, "sleep"):
            result = test.warm_readonly_route(connection(), "owned", 7, 10, "exact positive")
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
                        test.warm_readonly_route(connection(), "owned", 7, 10, "positive")
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
                        test.warm_readonly_route(connection(), "owned", 7, 10, "positive")
                self.assertIs(caught.exception, error)
                self.assertEqual(query.call_count, 1)

    def test_unknown_transport_and_other_codes_are_never_retried(self):
        for error in (refusal(code=2013), refusal(code=7), TimeoutError("read timeout"), OSError("socket closed")):
            with self.subTest(error=error):
                test = driver()
                with patch.object(kvtest, "q", side_effect=error) as query:
                    with self.assertRaises(type(error)) as caught:
                        test.warm_readonly_route(connection(), "owned", 7, 10, "positive")
                self.assertIs(caught.exception, error)
                self.assertEqual(query.call_count, 1)

    def test_attempt_limit_preserves_the_final_original_error(self):
        test, errors = driver(), [refusal() for _ in range(5)]
        with patch.object(kvtest, "q", side_effect=errors) as query, \
                patch.object(kvfaults.time, "monotonic", return_value=0), patch.object(kvfaults.time, "sleep"):
            with self.assertRaises(kvfaults.pymysql.OperationalError) as caught:
                test.warm_readonly_route(connection(), "owned", 7, 10, "positive")
        self.assertIs(caught.exception, errors[-1])
        self.assertEqual(query.call_count, 5)
        self.assertEqual(test.completed, 0)

    def test_retry_deadline_preserves_the_original_error(self):
        test, error = driver(), refusal()
        with patch.object(kvtest, "q", side_effect=error) as query, \
                patch.object(kvfaults.time, "monotonic", side_effect=[0, 11]):
            with self.assertRaises(kvfaults.pymysql.OperationalError) as caught:
                test.warm_readonly_route(connection(), "owned", 7, 10, "positive")
        self.assertIs(caught.exception, error)
        self.assertEqual(query.call_count, 1)

    def test_wrong_positive_image_is_never_retried_or_counted(self):
        test = driver()
        with patch.object(kvtest, "q", return_value=((99,),)) as query:
            with self.assertRaises(AssertionError):
                test.warm_readonly_route(connection(), "owned", 7, 10, "positive")
        self.assertEqual(query.call_count, 1)
        self.assertEqual(test.completed, 0)
        self.assertEqual(test.ack, 42)

    def test_quorum_timeout_namespace_cause_and_state_are_exact(self):
        message = "Lost connection to MySQL server during query (timed out)"
        self.assertEqual(kvfaults.quorum_sql_outcome(refusal(message, None, 2013), 1), "client_read_timeout")
        self.assertEqual(kvfaults.quorum_sql_outcome(refusal(), 1), "server_refusal")
        for error in (refusal(message, "40003", 2013), refusal(message, "HY000", 2013),
                      refusal(message + " unrelated", None, 2013), refusal("Lost connection", None, 2013),
                      refusal(message, None, 1041), refusal(state="40003"),
                      RuntimeError(2013, message), None):
            with self.subTest(error=error):
                self.assertIsNone(kvfaults.quorum_sql_outcome(error, 1))

    def test_source_defined_discovery_unavailability_is_tied_to_the_expected_shard(self):
        # Source integration control for Cluster::discover's format string;
        # this does not claim a live occurrence in the retained histories.
        message = "shard 1 has no reachable leader (127.0.0.1:3301 is read-only)"
        for state in (None, "HY000"):
            self.assertEqual(kvfaults.quorum_sql_outcome(refusal(message, state, 2003), 1),
                             "leader_discovery_unavailable")
        cases = [refusal(message, "HY000", 2003),
                 refusal(message, "40003", 2003), refusal(message, "40001", 2003),
                 refusal(message, "HY001", 2003), refusal(message, b"HY000", 2003),
                 refusal(message, "HY000", 2013)]
        expected = [2, 1, 1, 1, 1, 1]
        for error, shard in zip(cases, expected):
            with self.subTest(error=error, expected_shard=shard):
                self.assertIsNone(kvfaults.quorum_sql_outcome(error, shard))
        for text in ("cannot reach shard 1", "shard 1 has no reachable leader ()",
                     "shard 1 has no reachable leader (   )", "prefix " + message,
                     message + " suffix", message[:-1], message.replace("shard 1", "shard 11")):
            with self.subTest(message=text):
                self.assertIsNone(kvfaults.quorum_sql_outcome(refusal(text, "HY000", 2003), 1))

    def test_quorum_controller_classifies_source_defined_discovery_without_retry_or_guard_claim(self):
        message = "shard 1 has no reachable leader (no replicas configured)"
        test = quorum_driver(refusal(message, "HY000", 2003))
        test.scenario_quorum("memtx")
        test.sql.assert_called_once()
        event = next(e for e in test.events if e["event"] == "no_quorum_classification")
        self.assertEqual(event["sql_outcome"]["kind"], "leader_discovery_unavailable")
        self.assertEqual(event["sql_outcome"]["expected_shard_id"], 1)
        self.assertEqual(event["sql_outcome"]["error"]["sqlstate"], "HY000")
        self.assertFalse(event["sql_outcome"]["guard_coverage"])
        self.assertEqual(test.paired_values.call_count, 2)
        wrong = quorum_driver(refusal(message, "HY000", 2003))
        wrong.identities[0]["id"] = 2
        with self.assertRaises(AssertionError):
            wrong.scenario_quorum("memtx")
        wrong.sql.assert_called_once()
        wrong.restore.assert_called_once()

    def test_actual_driver_packet_preserves_observed_state_without_retry_or_guard_claim(self):
        # Both pinned 1.1.2 (no SQLSTATE attribute) and newer clients decode
        # this exact ERR packet. Never manufacture the discarded field.
        message = "shard 1 has no reachable leader (127.0.0.1:50485: tarantool operation timed out)"
        packet = b"\xff" + (2003).to_bytes(2, "little") + b"#HY000" + message.encode()
        try:
            kvfaults.pymysql.err.raise_mysql_exception(packet)
        except kvfaults.pymysql.OperationalError as error:
            observed = getattr(error, "sqlstate", None)
            self.assertIn(observed, (None, "HY000"))
            test = quorum_driver(error)
            test.scenario_quorum("memtx")
            test.sql.assert_called_once()
            test.restore.assert_called_once()
            self.assertEqual(test.paired_values.call_count, 2)
            event = next(e for e in test.events if e["event"] == "no_quorum_classification")
            outcome = event["sql_outcome"]
            self.assertEqual(outcome["kind"], "leader_discovery_unavailable")
            self.assertEqual(outcome["error"]["sqlstate"], observed)
            self.assertFalse(outcome["guard_coverage"])
            self.assertFalse(outcome["row_result_returned"])
        else:
            self.fail("ERR packet did not raise its actual client error")

    def test_healed_route_still_rejects_discovery_unavailability_without_retry(self):
        test = driver()
        error = refusal("shard 1 has no reachable leader (no replicas configured)", "HY000", 2003)
        with patch.object(kvtest, "q", side_effect=error) as query:
            with self.assertRaises(kvfaults.pymysql.OperationalError) as caught:
                test.warm_readonly_route(connection(), "owned", 7, 10, "positive")
        self.assertIs(caught.exception, error)
        query.assert_called_once()
        self.assertEqual(test.completed, 0)
        self.assertEqual(test.ack, 42)

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
        test.connect.return_value.__enter__.return_value = connection()
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
        test.connect.return_value.__enter__.return_value = connection()
        with patch.object(kvtest, "q", return_value=((999,),)) as query:
            with self.assertRaises(AssertionError):
                test.paired_values("t", 7, {1: 11, 99: None})
        query.assert_called_once()
        test.sdk.assert_not_called()
        self.assertEqual(test.completed, 0)


class ReadonlyDiscoveryControls(unittest.TestCase):
    endpoints = ("127.0.0.1:3311", "127.0.0.1:3312", "127.0.0.1:3313")

    def message(self, endpoint=None):
        return "shard 1 has no reachable leader (" + (endpoint or self.endpoints[0]) + " is read-only)"

    def warm(self, test, c, **changes):
        options = {"readonly_shard": 1, "readonly_endpoints": self.endpoints}
        options.update(changes)
        return test.warm_readonly_route(c, "owned_memtx", 7, 10, "exact positive", **options)

    def test_exact_actual_group_endpoints_and_observed_states_allow_positive_select(self):
        for endpoint in self.endpoints:
            for state in (None, "HY000"):
                with self.subTest(endpoint=endpoint, state=state):
                    test, c = driver(), connection()
                    error = refusal(self.message(endpoint), state, 2003)
                    with patch.object(kvtest, "q", side_effect=[error, ((10,),)]) as query, \
                            patch.object(kvfaults.time, "monotonic", return_value=0), patch.object(kvfaults.time, "sleep"):
                        self.assertEqual(self.warm(test, c), ((10,),))
                    self.assertEqual(query.call_count, 2)
                    self.assertEqual(test.ack, 42)
                    self.assertEqual(test.completed, 1)
                    self.assertTrue(all(call.args[1] == "SELECT v FROM owned_memtx WHERE tenant=%s AND id=1"
                                        and call.args[2] == (7,) for call in query.call_args_list))
                    event = next(e for e in test.events if e["event"] == "known_ro_readonly_warmup_refusal")
                    self.assertEqual(event["error"]["args"], [2003, self.message(endpoint)])
                    self.assertEqual(event["error"]["sqlstate"], state)
                    self.assertEqual(event["readonly_discovery_shard"], 1)
                    self.assertEqual(event["readonly_discovery_endpoints"], list(self.endpoints))
                    self.assertTrue(event["retry_eligible"])
                    self.assertFalse(event["guard_coverage"])

    def test_wrong_near_shard_endpoint_generic_discovery_and_timeout_never_retry(self):
        messages = [self.message().replace("shard 1 ", "shard 2 "), self.message().replace("shard 1 ", "shard 11 "),
                    self.message().replace("shard 1 ", "shard 01 "), self.message("127.0.0.1:33110"),
                    self.message("127.0.0.1:3314"), self.message("127.0.0.1:3321"),
                    "shard 1 has no reachable leader (no replicas configured)",
                    "shard 1 has no reachable leader (127.0.0.1:3311: tarantool operation timed out)",
                    self.message()[:-1], "prefix " + self.message(), self.message() + " suffix",
                    self.message().replace(" is read-only)", " is unavailable)"),
                    self.message().replace("leader (", "leader("), "Lost connection to MySQL server during query (timed out)"]
        for message in messages:
            with self.subTest(message=message):
                test, error = driver(), refusal(message, None, 2003)
                with patch.object(kvtest, "q", side_effect=error) as query:
                    with self.assertRaises(kvfaults.pymysql.OperationalError) as caught:
                        self.warm(test, connection())
                self.assertIs(caught.exception, error)
                self.assertEqual(query.call_count, 1)
                self.assertEqual(test.completed, 0)
                self.assertEqual(test.ack, 42)

    def test_contradictory_states_wrong_namespace_or_code_never_retry(self):
        errors = [refusal(self.message(), state, 2003) for state in
                  ("40003", b"40003", "40001", "HY001", b"HY000", "", False)]
        errors += [refusal(self.message(), None, code) for code in (2013, 1041, "2003", 2003.0)]
        errors += [kvfaults.pymysql.MySQLError(2003, self.message()),
                   refusal({"message": self.message()}, None, 2003), TimeoutError("unknown transport")]
        for error in errors:
            with self.subTest(error=error):
                test = driver()
                with patch.object(kvtest, "q", side_effect=error) as query:
                    with self.assertRaises(type(error)) as caught:
                        self.warm(test, connection())
                self.assertIs(caught.exception, error)
                self.assertEqual(query.call_count, 1)
                self.assertEqual(test.ack, 42)

    def test_other_warmups_do_not_opt_into_discovery_retries(self):
        test, error = driver(), refusal(self.message(), None, 2003)
        with patch.object(kvtest, "q", side_effect=error) as query:
            with self.assertRaises(kvfaults.pymysql.OperationalError) as caught:
                test.warm_readonly_route(connection(), "owned", 7, 10, "positive")
        self.assertIs(caught.exception, error)
        query.assert_called_once()
        for shard in (2, 0, True, "1"):
            with self.subTest(shard=shard):
                self.assertFalse(kvfaults.known_ro_discovery(error, shard, self.endpoints))

    def test_serialization_and_discovery_share_pool_plus_one_bound(self):
        test = driver()
        responses = [refusal(), refusal(self.message(), None, 2003), refusal(),
                     refusal(self.message(self.endpoints[1]), "HY000", 2003), ((10,),)]
        with patch.object(kvtest, "q", side_effect=responses) as query, \
                patch.object(kvfaults.time, "monotonic", return_value=0), patch.object(kvfaults.time, "sleep"):
            self.assertEqual(self.warm(test, connection()), ((10,),))
        self.assertEqual(query.call_count, test.args.sql_pool_size + 1)
        self.assertEqual(test.completed, 1)
        self.assertEqual(test.ack, 42)

    def test_leader_controller_warms_owned_group_before_one_transaction(self):
        test, c = driver(), connection()
        old = {"node_id": "s1r1", "endpoint": self.endpoints[0]}
        new = {"node_id": "s1r2", "endpoint": self.endpoints[1]}
        test.identities = [{"id": 1}, {"id": 2}]
        test.tenant, test.values = 7, {"memtx": {1: 10, 2: None, 99: None}}
        test.leader = Mock(side_effect=[(old, {"table": {}, "ownership": {}}), (new, {}), (new, {})])
        test.group_nodes = Mock(return_value=[old, new, {"node_id": "s1r3", "endpoint": self.endpoints[2]}])
        test.configuration = Mock(side_effect=[{"term": 1}, {"term": 2}])
        test.state = Mock(return_value={"ro": True, "term": 2})
        test.signal, test.restore, test.retarget, test.caught_up, test.paired_values = Mock(), Mock(), Mock(), Mock(), Mock()
        test.public = Mock(return_value={"status": "error", "error": {"kind": "moved", "domain": "contract"}})
        test.connect = MagicMock()
        test.connect.return_value.__enter__.return_value = c
        reads = 0
        def query(_c, statement, params):
            nonlocal reads
            if statement.startswith("SELECT "):
                reads += 1
                if reads == 1:
                    raise refusal(self.message(self.endpoints[2]), None, 2003)
                return ((10,),)
            if statement == "BEGIN":
                c.server_status = 1
            elif statement == "COMMIT":
                c.server_status = 0
            return ()
        with patch.object(kvtest, "q", side_effect=query) as sql, patch.object(kvfaults.time, "sleep"):
            test.scenario_leader("memtx")
        self.assertEqual([call.args[1] for call in sql.call_args_list], [
            "SELECT v FROM fault_memtx WHERE tenant=%s AND id=1",
            "SELECT v FROM fault_memtx WHERE tenant=%s AND id=1", "BEGIN",
            "UPDATE fault_memtx SET v=11 WHERE tenant=%s AND id=1",
            "INSERT INTO fault_memtx VALUES (%s,2,12)", "COMMIT"])
        test.group_nodes.assert_called_once_with(0)
        test.restore.assert_called_once()
        self.assertEqual(test.values["memtx"], {1: 11, 2: 12, 99: None})

    def test_discovery_exhaustion_and_deadline_preserve_original_error(self):
        test = driver()
        errors = [refusal(self.message(), None, 2003) for _ in range(5)]
        with patch.object(kvtest, "q", side_effect=errors) as query, \
                patch.object(kvfaults.time, "monotonic", return_value=0), patch.object(kvfaults.time, "sleep"):
            with self.assertRaises(kvfaults.pymysql.OperationalError) as caught:
                self.warm(test, connection())
        self.assertIs(caught.exception, errors[-1])
        self.assertEqual(query.call_count, 5)
        self.assertEqual(test.completed, 0)
        test, error = driver(), errors[0]
        with patch.object(kvtest, "q", side_effect=error) as query, \
                patch.object(kvfaults.time, "monotonic", side_effect=[0, 11]):
            with self.assertRaises(kvfaults.pymysql.OperationalError) as caught:
                self.warm(test, connection())
        self.assertIs(caught.exception, error)
        query.assert_called_once()

    def test_active_or_non_autocommit_connection_rejected_before_first_query(self):
        for c in (connection(autocommit=False), connection(server_status=1)):
            with self.subTest(connection=c):
                test = driver()
                with patch.object(kvtest, "q") as query:
                    with self.assertRaisesRegex(AssertionError, "independent autocommit"):
                        self.warm(test, c)
                query.assert_not_called()
                self.assertEqual(test.completed, 0)
                self.assertEqual(test.ack, 42)

    def test_transaction_state_is_rechecked_before_sleep_and_before_retry(self):
        for when in ("reply", "sleep"):
            with self.subTest(when=when):
                test, c = driver(), connection()
                error = refusal(self.message(), None, 2003)
                def failed(*args):
                    if when == "reply":
                        c.server_status = 1
                    raise error
                def sleep(_):
                    c.get_autocommit.return_value = False
                with patch.object(kvtest, "q", side_effect=failed) as query, \
                        patch.object(kvfaults.time, "monotonic", return_value=0), \
                        patch.object(kvfaults.time, "sleep", side_effect=sleep) as waited:
                    with self.assertRaisesRegex(AssertionError, "independent autocommit"):
                        self.warm(test, c)
                query.assert_called_once()
                self.assertEqual(waited.call_count, 0 if when == "reply" else 1)
                self.assertEqual(test.completed, 0)
                self.assertEqual(test.ack, 42)


class ReaderCleanupControls(unittest.TestCase):
    def fixture(self):
        test = driver()
        test.initial_endpoints = ["s1old", "s2old"]
        test.args.replica_groups = [["s1old", "s1new", "s1other"], ["s2old", "s2new", "s2other"]]
        test.leader = Mock(side_effect=[({"endpoint": "s1new"}, {}), ({"endpoint": "s2new"}, {})])
        test.user_attempts = ["s1old", "s2old"]
        test.user_created_origins = {"s1old"}
        test.users = [("s1old", 33)]
        test.workers, test.created, test.database_attempted, test.database_ack = [], False, False, False
        test.user, test.private = "owned_reader", Mock()
        return test

    def test_retarget_preserves_definite_create_and_unknown_create_origins(self):
        test = self.fixture()
        test.retarget("owned_memtx")
        self.assertEqual(test.users, [("s1new", 33)])
        self.assertEqual(test.user_created_origins, {"s1old"})
        with patch.object(kvtest, "ev", return_value={"absent": True, "uid": 33}) as cleanup:
            self.assertEqual(kvtest.Test.cleanup(test, True), [])
        self.assertEqual(cleanup.call_args.args[0], "s1new")
        self.assertEqual(cleanup.call_args.args[2], ["owned_reader", 33])
        self.assertEqual(test.uncertain_reader_attempts(), ["s2old"])
        event = next(e for e in test.events if e["event"] == "cleanup")
        self.assertEqual(event["uncertain_reader_attempts_preserved"], ["s2old"])

    def test_definite_creation_does_not_hide_wrong_uid_or_drop_transport_failure(self):
        for outcome in ({"absent": True, "uid": 34}, OSError("drop transport outcome unknown")):
            with self.subTest(outcome=repr(outcome)):
                test = self.fixture()
                test.retarget("owned_memtx")
                arguments = {"side_effect": outcome} if isinstance(outcome, Exception) else {"return_value": outcome}
                with patch.object(kvtest, "ev", **arguments) as cleanup:
                    errors = kvtest.Test.cleanup(test, True)
                self.assertEqual(cleanup.call_count, 1)
                self.assertEqual(len(errors), 1)
                self.assertFalse(any(e["event"] == "admin_reader_drop" for e in test.events))
                event = next(e for e in test.events if e["event"] == "cleanup")
                self.assertFalse(event["success"])
                self.assertEqual(event["uncertain_reader_attempts_preserved"], ["s2old"])


class PublicScanControls(unittest.TestCase):
    """Exercise actual driver methods; these mocks do not qualify Lua fibers."""

    def test_actual_public_counter_is_required_and_retained(self):
        test, state, worker = driver(), scan_state(), Mock()
        worker.is_alive.return_value = True
        with patch.object(kvfaults, "ev", return_value=state) as call, \
                patch.object(kvfaults.time, "monotonic", return_value=0):
            self.assertIs(test.await_scan("owner", "owned-nonce", worker), state)
        call.assert_called_once_with("owner", kvfaults.SCAN_CONTROL, ["owned-nonce", "snapshot"])
        event = test.events[-1]
        self.assertEqual(event["event"], "sdk_second_group_scan_held")
        self.assertIs(event["state"], state)
        self.assertEqual(event["state"]["reached_result"], ["ok", 1])
        self.assertNotIn("entered_at", event["state"])
        self.assertNotIn("resumed_at", event["state"])

    def test_old_global_entered_flag_cannot_satisfy_the_four_second_wait(self):
        test, worker = driver(), Mock()
        worker.is_alive.return_value = True
        state = scan_state(entered=True, observer_done=False, reached=None, reached_result=None)
        with patch.object(kvfaults, "ev", return_value=state) as call, \
                patch.object(kvfaults.time, "monotonic", side_effect=[0, 0, 4]), \
                patch.object(kvfaults.time, "sleep"):
            with self.assertRaisesRegex(AssertionError, "did not reach the real scan hook"):
                test.await_scan("owner", "owned-nonce", worker)
        call.assert_called_once()
        self.assertEqual(test.events, [])

    def test_expired_released_faulted_or_finished_reads_never_qualify_as_held(self):
        for changes, alive in (({"expired": True}, True), ({"released": True}, True),
                               ({"observer_error": "public reached failed"}, True), ({}, False)):
            with self.subTest(changes=changes, alive=alive):
                test, worker = driver(), Mock()
                worker.is_alive.return_value = alive
                with patch.object(kvfaults, "ev", return_value=scan_state(**changes)), \
                        patch.object(kvfaults.time, "monotonic", return_value=0):
                    with self.assertRaises(AssertionError):
                        test.await_scan("owner", "owned-nonce", worker)
                self.assertEqual(test.events, [])

    def test_release_requires_a_live_worker_and_actual_public_release_counter(self):
        test, worker = driver(), Mock()
        worker.is_alive.return_value = False
        with patch.object(kvfaults, "ev") as call:
            with self.assertRaisesRegex(AssertionError, "finished before public scan release"):
                test.release_scan("owner", "owned-nonce", worker)
        call.assert_not_called()
        self.assertEqual(test.events, [])
        worker.is_alive.return_value = True
        for count in (None, ["ok", 0], ["ok", True], ["ok", "1"]):
            with self.subTest(count=count), patch.object(kvfaults, "ev", return_value=scan_state(
                    released=True, release_reached_result=count)) as call:
                with self.assertRaises(AssertionError):
                    test.release_scan("owner", "owned-nonce", worker)
                call.assert_called_once_with("owner", kvfaults.SCAN_CONTROL, ["owned-nonce", "release"])
        state = scan_state(released=True, release_reached_result=["ok", 1])
        with patch.object(kvfaults, "ev", return_value=state):
            self.assertIs(test.release_scan("owner", "owned-nonce", worker), state)

    def test_close_accepts_joined_prestart_cancellation_without_inventing_a_reach(self):
        test, worker = driver(), Mock()
        worker.is_alive.return_value = False
        state = scan_state(released=True, observer_done=False, observer_joined=True,
                           observer_cancel_expected=True, reached=None, reached_result=None)
        with patch.object(kvfaults, "ev", return_value=state) as call:
            test.close_scan("owner", "owned-nonce", worker)
        call.assert_called_once_with("owner", kvfaults.SCAN_CONTROL, ["owned-nonce", "close"])
        worker.join.assert_called_once_with(13)
        self.assertEqual(test.events[-1]["event"], "scan_hook_restore")
        self.assertIsNone(test.events[-1]["result"]["reached_result"])

    def test_close_requires_join_and_drains_only_the_supplied_worker(self):
        for state in (scan_state(released=True),
                      scan_state(released=True, observer_joined=True, observer_error="failure")):
            with self.subTest(state=state):
                test, worker = driver(), Mock()
                worker.is_alive.return_value = False
                with patch.object(kvfaults, "ev", return_value=state):
                    with self.assertRaises(AssertionError):
                        test.close_scan("owner", "owned-nonce", worker)
                worker.join.assert_called_once_with(13)
                self.assertEqual(test.events[-1]["event"], "scan_hook_cleanup_error")

    def test_cleanup_failure_preserves_the_original_exception(self):
        test, worker, original = driver(), Mock(), RuntimeError("original assertion failure")
        worker.is_alive.return_value = False
        with patch.object(kvfaults, "ev", side_effect=RuntimeError("cleanup failed")):
            with self.assertRaises(RuntimeError) as caught:
                try:
                    raise original
                finally:
                    test.close_scan("owner", "owned-nonce", worker)
        self.assertIs(caught.exception, original)
        worker.join.assert_called_once_with(13)
        self.assertEqual(test.events[-1]["event"], "scan_hook_cleanup_error")


if __name__ == "__main__":
    unittest.main(verbosity=2)
