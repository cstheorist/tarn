#!/usr/bin/env python3
"""Offline phase/identity controls for the real Vinyl-pressure fixture (#174).

These checks use the fixture's actual scheduling/controller/audit helpers,
virtual monotonic time and fake readback. They create no services or writes.
The old global-cap adapter is a source-derived policy control, not execution
of the complete historical driver or a claim about its original CI failure.
"""
import collections
import copy
import json
import unittest

import vinylpressure as pressure


class CompactionControls(unittest.TestCase):
    def setUp(self):
        def index(queue, count, input_bytes):
            return {'disk': {'compaction': {'queue': {'bytes': queue}, 'count': count,
                                             'input': {'bytes': input_bytes}}}}
        self.control = {'before': index(0, 2, 100), 'after': index(4096, 2, 100)}
        self.samples = [{'snap_io_rate_limit': .5, 'compaction_fixture': index(4096, 2, 100)}]
        self.audit = {'status': 'complete', 'matches': True, 'index': index(0, 3, 4196)}

    def test_queue_and_completed_seed_work_are_required(self):
        pressure.compaction_witness(self.control, self.samples, self.audit)
        for field, value in [('count', 2), ('input', {'bytes': 100})]:
            audit = copy.deepcopy(self.audit)
            audit['index']['disk']['compaction'][field] = value
            with self.assertRaises(AssertionError):
                pressure.compaction_witness(self.control, self.samples, audit)

    def test_historical_completed_work_without_sampled_queue_is_rejected(self):
        samples = copy.deepcopy(self.samples)
        samples[0]['compaction_fixture']['disk']['compaction']['queue']['bytes'] = 0
        with self.assertRaises(AssertionError):
            pressure.compaction_witness(self.control, samples, self.audit)

    def test_unlimited_or_missing_control_cannot_certify_pressure(self):
        with self.assertRaises(AssertionError):
            pressure.compaction_witness(None, self.samples, self.audit)
        samples = copy.deepcopy(self.samples)
        samples[0]['snap_io_rate_limit'] = 0
        with self.assertRaises(AssertionError):
            pressure.compaction_witness(self.control, samples, self.audit)

    def test_corrupt_seed_readback_fails_even_with_real_native_work(self):
        with self.assertRaises(AssertionError):
            pressure.compaction_witness(self.control, self.samples, dict(self.audit, matches=False))


class VirtualWriters:
    """Zero-latency clients driven by the real scheduler and controller."""
    def __init__(self, schedule, writers):
        self.schedule, self.writers = schedule, writers
        self.now = 100.0
        self.calls, self.receipts, self.events = [], [], []
        self.offers = 0
        self.greedy_done = False

    def serve(self, worker):
        self.offers += 1
        ticket = self.schedule.claim(worker, self.now)
        assert ticket['kind'] != 'done', 'writer exited before all phase windows completed'
        if ticket['kind'] == 'wait':
            assert ticket['seconds'] > 0, 'exhausted writer must park, not busy-spin'
            return ticket['seconds']
        assert ticket['kind'] == 'permit', ticket
        dispatched = self.schedule.dispatch(ticket, self.now)
        assert dispatched is not None, 'immediate in-window dispatch was lost'
        self.calls.append({'worker': worker, 'now': self.now, **dispatched})
        self.schedule.complete(dispatched, self.now, 'ok')
        return 0

    def wait(self, seconds):
        assert seconds >= 0
        until = self.now + seconds
        if not self.greedy_done:
            # More offered work than the entire finite budget at warmup's
            # initial instant. The scheduler must pace/park, not spend the
            # future pressure/release allocation or terminate the clients.
            for i in range(pressure.PRESSURE_ATTEMPT_CAP + self.writers):
                self.serve(i % self.writers)
            self.greedy_done = True
        while self.now < until:
            waits = [self.serve(i) for i in range(self.writers)]
            if 0 in waits:
                # A zero-latency completion may permit only one due slot.
                continue
            self.now = min(until, self.now + min(waits))
        return False

    def io_limit(self, limit):
        # A slow control call is outside the pressure windows. Every writer
        # must remain parked until its actual receipt has returned.
        for worker in range(self.writers):
            assert self.schedule.claim(worker, self.now)['kind'] == 'wait'
        self.now += .25
        receipt = [{'node': 'source', 'result': {'limit': limit, 'at': self.now}}]
        self.receipts.append((self.now, limit, receipt))
        return receipt

    def record(self, event, **data):
        self.events.append({'event': event, 'now': self.now, **data})


class LegacyGlobalCapPolicy:
    """Only the former shared-cap/return policy, intentionally unpaced.

    Derived from the historical writer's `attempts >= 20000: return` before
    each SQL call. It has no reserved later-phase allocation. It is not a
    substitute for the retained original live CI evidence.
    """
    def __init__(self):
        self.attempts = 0

    def claim(self, worker, now):
        if self.attempts >= pressure.PRESSURE_ATTEMPT_CAP:
            return {'kind': 'done'}
        self.attempts += 1
        return {'kind': 'permit', 'phase': pressure.PRESSURE_PHASES[0],
                'ordinal': self.attempts, 'window_start_monotonic': now,
                'window_deadline_monotonic': now + 3}

    def dispatch(self, ticket, now):
        return dict(ticket)

    def complete(self, dispatched, now, result):
        pass


class PhaseControls(unittest.TestCase):
    def test_fast_writers_survive_all_receipted_windows_with_finite_budget(self):
        self.assertEqual(pressure.PRESSURE_ATTEMPT_CAP, 20000)
        schedule = pressure.PressureSchedule(8, 5)
        clients = VirtualWriters(schedule, 8)
        result = pressure.run_pressure_phases(schedule, clients.io_limit, clients.wait,
                                              now=lambda: clients.now, record=clients.record)
        self.assertIsNone(result['primary'])
        self.assertEqual(result['restore_errors'], [])
        self.assertGreater(clients.offers, pressure.PRESSURE_ATTEMPT_CAP)
        self.assertLessEqual(len(clients.calls), pressure.PRESSURE_ATTEMPT_CAP)
        self.assertTrue(clients.calls)
        phases = collections.defaultdict(list)
        for row in clients.calls:
            phases[row['phase']].append(row)
            self.assertLessEqual(row['window_start_monotonic'], row['now'])
            self.assertLess(row['now'], row['window_deadline_monotonic'])
        self.assertEqual(set(phases), set(pressure.PRESSURE_PHASES))
        self.assertEqual([x[1] for x in clients.receipts], [0, .5, 0])
        for phase, duration, receipt in zip(pressure.PRESSURE_PHASES, (3, 5, 3), clients.receipts):
            rows = phases[phase]
            self.assertEqual({x['worker'] for x in rows}, set(range(8)))
            self.assertEqual(len({x['ordinal'] for x in rows}), len(rows))
            self.assertTrue(all(x['window_start_monotonic'] >= receipt[0] for x in rows))
            self.assertTrue(all(x['window_deadline_monotonic'] - x['window_start_monotonic'] == duration for x in rows))
        image = schedule.snapshot()
        self.assertEqual(image['pending_slots'], [])
        self.assertEqual(image['inflight_dispatches'], [])
        self.assertEqual(image['attempts'], len(clients.calls))
        for phase, receipt in zip(image['phases'], clients.receipts):
            self.assertEqual(phase['io_receipts'], receipt[2])
            self.assertEqual(phase['closed_monotonic'], phase['deadline_monotonic'])
            for worker in phase['workers']:
                self.assertGreater(worker['dispatched'], 0)
                self.assertEqual(worker['dispatched'], worker['completed'])
                self.assertLessEqual(phase['opened_monotonic'], worker['first_dispatch_monotonic'])
                self.assertLess(worker['last_dispatch_monotonic'], phase['deadline_monotonic'])
        self.assertTrue(all(schedule.claim(i, clients.now)['kind'] == 'done' for i in range(8)))

    def test_old_global_cap_policy_fails_the_same_no_early_exit_oracle(self):
        clients = VirtualWriters(LegacyGlobalCapPolicy(), 8)
        with self.assertRaisesRegex(AssertionError, 'writer exited before all phase windows'):
            clients.wait(3)
        self.assertEqual(len(clients.calls), pressure.PRESSURE_ATTEMPT_CAP)
        self.assertEqual({x['phase'] for x in clients.calls}, {pressure.PRESSURE_PHASES[0]})

    def test_missed_slots_do_not_create_catchup_burst(self):
        schedule = pressure.PressureSchedule(2, 20)
        schedule.open_phase(pressure.PRESSURE_PHASES[0], 0)
        ticket = schedule.claim(0, 1.5)
        self.assertEqual(ticket['kind'], 'permit')
        dispatched = schedule.dispatch(ticket, 1.5)
        self.assertIsNotNone(dispatched)
        schedule.complete(dispatched, 1.5, 'ok')
        next_ticket = schedule.claim(0, 1.5)
        self.assertEqual(next_ticket['kind'], 'wait')
        self.assertGreater(next_ticket['seconds'], 0)

    def test_payload_delay_does_not_leave_an_immediately_due_successor(self):
        schedule = pressure.PressureSchedule(2, 5)
        schedule.open_phase(pressure.PRESSURE_PHASES[0], 0)
        ticket = schedule.claim(0, 0)
        self.assertEqual(ticket['kind'], 'permit')
        dispatched = schedule.dispatch(ticket, 1.5)
        self.assertIsNotNone(dispatched)
        schedule.complete(dispatched, 1.5, 'ok')
        following = schedule.claim(0, 1.5)
        self.assertEqual(following['kind'], 'wait')
        self.assertGreater(following['seconds'], 0)

    def test_payload_crossing_deadline_is_not_dispatched_or_relabelled(self):
        schedule = pressure.PressureSchedule(2, 5)
        schedule.open_phase(pressure.PRESSURE_PHASES[0], 0)
        ticket = schedule.claim(0, 0)
        self.assertEqual(ticket['kind'], 'permit')
        self.assertIsNone(schedule.dispatch(ticket, 3))
        schedule.close_phase(pressure.PRESSURE_PHASES[0], 3)
        self.assertEqual(schedule.claim(0, 3)['kind'], 'wait')
        schedule = pressure.PressureSchedule(2, 5)
        schedule.open_phase(pressure.PRESSURE_PHASES[0], 0)
        ticket = schedule.claim(0, 0)
        schedule.close_phase(pressure.PRESSURE_PHASES[0], 3)
        schedule.open_phase(pressure.PRESSURE_PHASES[1], 4)
        self.assertIsNone(schedule.dispatch(ticket, 4))

    def test_failed_control_restores_io_and_does_not_start_later_windows(self):
        schedule = pressure.PressureSchedule(2, 5)
        clients = VirtualWriters(schedule, 2)
        failure = RuntimeError('limited control receipt unavailable')

        def io_limit(limit):
            if limit == .5:
                raise failure
            return clients.io_limit(limit)

        result = pressure.run_pressure_phases(schedule, io_limit, clients.wait,
                                              now=lambda: clients.now, record=clients.record)
        self.assertIs(result['primary'][1], failure)
        self.assertEqual(result['restore_errors'], [])
        self.assertEqual([x[1] for x in clients.receipts], [0, 0])
        self.assertEqual({x['phase'] for x in clients.calls}, {pressure.PRESSURE_PHASES[0]})
        self.assertEqual(schedule.snapshot()['finish_reason'], 'aborted')

    def test_late_completion_is_not_evidence_of_a_later_phase_admission(self):
        schedule = pressure.PressureSchedule(2, 5)
        schedule.open_phase(pressure.PRESSURE_PHASES[0], 0)
        dispatched = schedule.dispatch(schedule.claim(0, 0), 0)
        schedule.close_phase(pressure.PRESSURE_PHASES[0], 3)
        schedule.open_phase(pressure.PRESSURE_PHASES[1], 4)
        self.assertEqual(schedule.complete(dispatched, 4, 'ok'), pressure.PRESSURE_PHASES[1])
        phases = schedule.snapshot()['phases']
        self.assertEqual(phases[0]['workers'][0]['dispatched'], 1)
        self.assertEqual(phases[0]['workers'][0]['completed'], 1)
        self.assertEqual(sum(x['dispatched'] for x in phases[1]['workers']), 0)


class ReadbackControls(unittest.TestCase):
    records = [{'worker': 0, 'id': 1, 'result': 'ok'},
               {'worker': 1, 'id': 2, 'result': 'error', 'error': [1205, 'refused']}]

    def run_finalizer(self, actual, records=None, inflight=(), workers_live=(), query_error=None):
        events, saved = [], []

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        def query(connection, sql, args):
            self.assertIsInstance(connection, Connection)
            self.assertEqual(' '.join(sql.split()),
                             'SELECT id FROM p WHERE tenant_id=%s ORDER BY id LIMIT 20001')
            self.assertEqual(args, (77,))
            events.append('bounded readback')
            if query_error:
                raise query_error
            return tuple((key,) for key in actual)

        def persist(summary):
            events.append('persist')
            saved.append(json.loads(json.dumps(summary)))

        def check(summary):
            events.append('witness check')
            self.assertTrue(saved, 'readback must be retained before witness validation')
            self.assertEqual(saved[-1]['readback'], summary['readback'])
            # Deliberately missing real-pressure witness. The actual
            # finalizer must already have queried and retained identities.
            self.assertGreater(summary['compaction_queue_max_bytes'], 0,
                               'no actual queued compaction input')

        summary = {'compaction_queue_max_bytes': 0}
        with self.assertRaisesRegex(AssertionError, 'no actual queued compaction input'):
            pressure.finalize_pressure(summary,
                audit=lambda: pressure.audit_pressure_keys(Connection, 77,
                    self.records if records is None else records, inflight, list(workers_live), query=query),
                persist=persist, check=check)
        self.assertEqual(events, ['bounded readback', 'persist', 'witness check', 'persist'])
        self.assertIn('no actual queued compaction input', saved[-1]['check_failure']['message'])
        return saved[-1]

    def test_missing_pressure_witness_still_retains_exact_ack_refused_readback(self):
        summary = self.run_finalizer([1])
        self.assertTrue(summary['stored_ids_match_acknowledgements'])
        self.assertTrue(summary['refused_ids_absent'])
        self.assertEqual(summary['readback']['status'], 'complete')

    def test_corrupt_readback_survives_the_original_witness_failure(self):
        for actual, missing, refused, extra in [([], [1], [], []), ([1, 2, 9], [], [2], [2, 9])]:
            with self.subTest(actual=actual):
                summary = self.run_finalizer(actual)
                audit = summary['readback']
                self.assertFalse(summary['stored_ids_match_acknowledgements'])
                self.assertEqual(audit['acknowledged_ids_missing'], missing)
                self.assertEqual(audit['refused_ids_present'], refused)
                self.assertEqual(audit['unexpected_ids'], extra)

    def test_query_failure_is_retained_before_witness_failure(self):
        summary = self.run_finalizer([], query_error=RuntimeError('readback unavailable'))
        self.assertEqual(summary['readback']['status'], 'failed')
        self.assertIn('readback unavailable', summary['readback']['error'])
        self.assertIsNone(summary['stored_ids_match_acknowledgements'])
        self.assertIsNone(summary['refused_ids_absent'])

    def test_unknown_and_inflight_remain_unsettled_even_when_their_keys_are_observed(self):
        records = self.records + [{'worker': 0, 'id': 3, 'result': 'error',
                                   'error': [1180, 'outcome unknown']}]
        summary = self.run_finalizer([1, 3, 4], records, [{'worker': 1, 'id': 4}], [1])
        audit = summary['readback']
        self.assertEqual(audit['status'], 'incomplete')
        self.assertEqual(audit['unknown_ids'], [3])
        self.assertEqual(audit['inflight_ids'], [4])
        self.assertEqual(audit['unknown_ids_observed'], [3])
        self.assertEqual(audit['inflight_ids_observed'], [4])
        self.assertIsNone(summary['stored_ids_match_acknowledgements'])
        self.assertTrue(summary['refused_ids_absent'])

    def test_sentinel_limit_cannot_certify_an_unbounded_or_truncated_key_set(self):
        summary = self.run_finalizer(range(1, pressure.PRESSURE_ATTEMPT_CAP + 2))
        audit = summary['readback']
        self.assertEqual(audit['row_count'], 20001)
        self.assertTrue(audit['limit_reached'])
        self.assertEqual(audit['status'], 'incomplete')
        self.assertIsNone(summary['stored_ids_match_acknowledgements'])
        self.assertIsNone(audit['acknowledged_ids_missing'])

    def test_persist_errors_do_not_mask_original_witness_or_stage_exception(self):
        for failure_at in (1, 2):
            for original in (AssertionError('original pressure witness failure'),
                             RuntimeError('original I/O stage failure')):
                with self.subTest(failure_at=failure_at, original=type(original).__name__):
                    persist_error = OSError('evidence write unavailable')
                    calls = []
                    summary = {'primary_failure': {'stage': 'pressure window'}}

                    def persist(value):
                        calls.append('persist')
                        if calls.count('persist') == failure_at:
                            raise persist_error

                    def check(value):
                        calls.append('check')
                        raise original

                    try:
                        pressure.finalize_pressure(summary,
                            audit=lambda: {'status': 'complete', 'attempted': True},
                            persist=persist, check=check)
                    except BaseException as raised:
                        self.assertIs(raised, original, 'evidence failure replaced the original failure')
                    else:
                        self.fail('original failure was swallowed')
                    self.assertIn('check', calls)
                    self.assertIn('evidence write unavailable', json.dumps(summary['evidence_errors']))
                    self.assertEqual(summary['primary_failure']['stage'], 'pressure window')

    def test_persistence_failure_alone_still_fails(self):
        failure = OSError('only evidence write failed')
        summary, calls = {}, []

        def persist(value):
            raise failure

        def check(value):
            calls.append('check')

        try:
            pressure.finalize_pressure(summary,
                audit=lambda: {'status': 'complete', 'attempted': True},
                persist=persist, check=check)
        except BaseException as raised:
            self.assertIs(raised, failure)
        else:
            self.fail('persistence failure was accepted as success')
        self.assertEqual(calls, ['check'])
        self.assertIn('only evidence write failed', json.dumps(summary['evidence_errors']))


if __name__ == '__main__':
    unittest.main(verbosity=2)
