#!/usr/bin/env python3
"""Refusal/uncertainty controls for upgrade fixture admission, without servers."""
import unittest

from upgrade_prepare import prepare_witness


YES = dict(reply=["yes"], state="prepared")
NO = dict(reply=["no", 1205, "Vinyl writes are throttled while dump or compaction catches up"], state="aborted")


class Fixture:
    def __init__(self, replies, cleanup_error=None):
        self.replies = iter(replies)
        self.calls, self.events = [], []
        self.cleanup_error = cleanup_error
        self.clock = 0

    def prepare(self, node, txid, timeout):
        self.calls.append(("prepare", node, txid))
        assert 0 < timeout <= 12
        result = next(self.replies)
        if isinstance(result, BaseException):
            raise result
        return result

    def cleanup(self, txid, remaining):
        assert 0 < remaining() <= 12
        self.calls.append(("cleanup", txid))
        if self.cleanup_error:
            raise self.cleanup_error
        return [{"participant": "aborted", "value": 0, "locks": 0}]

    def event(self, name, **data):
        self.events.append((name, data))

    def sleep(self, seconds):
        self.clock += seconds

    def run(self, **kwargs):
        return prepare_witness("fixture", ["a", "b"], self.prepare, self.cleanup, self.event,
                               now=lambda: self.clock, sleep=self.sleep, **kwargs)


class AdmissionTests(unittest.TestCase):
    def test_first_participant_refusal_uses_new_identity_after_verified_abort(self):
        f = Fixture([NO, YES, YES])
        self.assertEqual(f.run(), "fixture-admission-1")
        self.assertEqual(f.calls, [("prepare", "a", "fixture"), ("cleanup", "fixture"),
                                  ("prepare", "a", "fixture-admission-1"),
                                  ("prepare", "b", "fixture-admission-1")])

    def test_second_participant_refusal_does_not_leave_an_earlier_yes(self):
        f = Fixture([YES, NO, YES, YES])
        self.assertEqual(f.run(), "fixture-admission-1")
        self.assertEqual(f.calls[2], ("cleanup", "fixture"))
        names = [name for name, _ in f.events]
        self.assertLess(names.index("prepare_refusal_cleanup_verified"), names.index("durable_YES_before_restart"))

    def test_lost_reply_is_never_replayed_or_cleaned_up_as_a_refusal(self):
        f = Fixture([YES, TimeoutError("lost reply")])
        with self.assertRaisesRegex(TimeoutError, "lost reply"):
            f.run()
        self.assertEqual(f.calls, [("prepare", "a", "fixture"), ("prepare", "b", "fixture")])
        self.assertEqual(f.events[-1][0], "prepare_attempt_failed")

    def test_unknown_expired_and_unexpected_refusals_stop(self):
        for result in (dict(reply=["expired", 1213, "freshness unavailable"], state="none"),
                       dict(reply=["no", 1205, "unrelated lock timeout"], state="aborted"),
                       dict(reply=NO["reply"], state="none"), dict(reply=["yes"], state="aborted")):
            with self.subTest(result=result):
                f = Fixture([result])
                with self.assertRaises(AssertionError):
                    f.run()
                self.assertEqual(len(f.calls), 1)

    def test_cleanup_failure_never_admits_another_identity(self):
        f = Fixture([YES, NO], cleanup_error=RuntimeError("abort reply lost"))
        with self.assertRaisesRegex(RuntimeError, "abort reply lost"):
            f.run()
        self.assertEqual(len(f.calls), 3)

    def test_attempt_limit_retains_all_refusals(self):
        f = Fixture([NO, NO])
        with self.assertRaisesRegex(TimeoutError, "attempt limit"):
            f.run(attempts=2)
        self.assertEqual(sum(c[0] == "prepare" for c in f.calls), 2)
        self.assertEqual(sum(c[0] == "cleanup" for c in f.calls), 2)

    def test_elapsed_limit_is_independent_of_attempt_limit(self):
        f = Fixture([NO])
        with self.assertRaisesRegex(TimeoutError, "deadline"):
            f.run(seconds=.01)
        self.assertEqual(sum(c[0] == "prepare" for c in f.calls), 1)

    def test_late_final_yes_does_not_qualify_or_start_another_attempt(self):
        f = Fixture([YES, YES])
        prepare = f.prepare

        def slow_prepare(*args):
            result = prepare(*args)
            f.clock += .6
            return result

        f.prepare = slow_prepare
        with self.assertRaisesRegex(TimeoutError, "deadline"):
            f.run(seconds=1)
        self.assertEqual(len(f.calls), 2)
        self.assertNotIn("durable_YES_before_restart", [name for name, _ in f.events])


if __name__ == "__main__":
    unittest.main()
