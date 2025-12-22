# Copyright 2024 Wolfgang Hoschek AT mac DOT com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Unit tests for call_with_retries() helper."""

from __future__ import (
    annotations,
)
import argparse
import logging
import random
import threading
import time
import unittest
from logging import (
    Logger,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from bzfs_main.util.retry import (
    AttemptOutcome,
    Retry,
    RetryableError,
    RetryConfig,
    RetryError,
    RetryOptions,
    RetryPolicy,
    _sleep,
    call_with_retries,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestSleep,
        TestCallWithRetries,
        TestRetryPolicyCopy,
        TestRetryConfigCopy,
        TestRetryOptionsCopy,
        TestRetryOptionsCall,
        TestAttemptOutcomeCopy,
        TestCallWithRetriesBenchmark,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class SequenceRandom:
    """Deterministic `randint()` provider for backoff strategy tests."""

    def __init__(self, values: list[int]) -> None:
        self._values = values
        self._idx = 0

    def randint(self, a: int, b: int) -> int:
        if a > b:
            raise AssertionError(f"Invalid randint range: [{a}, {b}]")
        if self._idx >= len(self._values):
            raise AssertionError("SequenceRandom exhausted")
        value = self._values[self._idx]
        self._idx += 1
        if not (a <= value <= b):
            raise AssertionError(f"SequenceRandom value {value} not in [{a}, {b}]")
        return value


#############################################################################
class TestSleep(unittest.TestCase):

    def test_sleep_calls_time_sleep_when_termination_event_is_none(self) -> None:
        sleep_nanos = 123_000_000
        expected_secs = sleep_nanos / 1_000_000_000
        with patch("bzfs_main.util.retry.time.sleep") as mock_sleep:
            _sleep(sleep_nanos, termination_event=None)
        mock_sleep.assert_called_once()
        self.assertAlmostEqual(expected_secs, mock_sleep.call_args[0][0])

    def test_sleep_does_not_call_time_sleep_when_duration_is_zero(self) -> None:
        with patch("bzfs_main.util.retry.time.sleep") as mock_sleep:
            _sleep(0, termination_event=None)
        mock_sleep.assert_not_called()

    def test_sleep_calls_event_wait_when_termination_event_is_not_none(self) -> None:
        sleep_nanos = 123_000_000
        expected_secs = sleep_nanos / 1_000_000_000
        termination_event = threading.Event()
        with patch("bzfs_main.util.retry.time.sleep") as mock_sleep:
            with patch.object(termination_event, "wait") as mock_wait:
                _sleep(sleep_nanos, termination_event=termination_event)
        mock_sleep.assert_not_called()
        mock_wait.assert_called_once()
        self.assertAlmostEqual(expected_secs, mock_wait.call_args[0][0])

    def test_sleep_does_not_call_event_wait_when_duration_is_zero(self) -> None:
        termination_event = threading.Event()
        with patch.object(termination_event, "wait") as mock_wait:
            _sleep(0, termination_event=termination_event)
        mock_wait.assert_not_called()


#############################################################################
def decorrelated_jitter_backoff_strategy(
    retry: Retry, curr_max_sleep_nanos: int, rng: random.Random, elapsed_nanos: int, retryable_error: RetryableError
) -> tuple[int, int]:
    """Decorrelated-jitter picks a random sleep_nanos duration from the range [base, prev*3] with cap and returns sleep_nanos
    as the next state."""
    policy: RetryPolicy = retry.policy
    prev_sleep_nanos: int = policy.min_sleep_nanos if retry.count <= 0 else curr_max_sleep_nanos
    upper_bound: int = max(prev_sleep_nanos * 3, policy.min_sleep_nanos)
    sample: int = rng.randint(policy.min_sleep_nanos, upper_bound)
    sleep_nanos: int = min(policy.max_sleep_nanos, sample)
    return sleep_nanos, sleep_nanos


#############################################################################
class TestCallWithRetries(unittest.TestCase):

    def test_retry_policy_repr(self) -> None:
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=2,
            initial_max_sleep_secs=3,
            max_sleep_secs=4,
            max_elapsed_secs=5,
            exponential_base=6,
        )
        expected = (
            "RetryPolicy(max_retries=1, min_sleep_secs=2, initial_max_sleep_secs=3, "
            "max_sleep_secs=4, max_elapsed_secs=5, exponential_base=6, reraise=True, max_previous_outcomes=0)"
        )
        self.assertEqual(expected, repr(retry_policy))

        args = argparse.Namespace(
            max_retries=1,
            retry_min_sleep_secs=2,
            retry_initial_max_sleep_secs=3,
            retry_max_sleep_secs=4,
            retry_max_elapsed_secs=5,
            retry_exponential_base=6,
        )
        retry_policy = RetryPolicy.from_namespace(args)
        self.assertEqual(expected, repr(retry_policy))

    def test_call_with_retries_success(self) -> None:
        calls: list[int] = []
        retry_policy = RetryPolicy(
            max_retries=2,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )

        def fn(retry: Retry) -> str:
            calls.append(retry.count)
            if retry.count < 2:
                raise RetryableError(
                    "fail", display_msg="connect", retry_immediately_once=(retry.count == 0)
                ) from ValueError("boom")
            return "ok"

        mock_log = MagicMock(spec=Logger)
        self.assertEqual(
            "ok", call_with_retries(fn, policy=retry_policy, config=RetryConfig(display_msg="foo"), log=mock_log)
        )
        self.assertEqual([0, 1, 2], calls)
        self.assertEqual(2, len(mock_log.log.call_args_list))

        calls.clear()
        mock_log = MagicMock(spec=Logger)
        mock_log.isEnabledFor = lambda level: level >= logging.ERROR
        self.assertEqual(
            "ok", call_with_retries(fn, policy=retry_policy, config=RetryConfig(display_msg="foo"), log=mock_log)
        )
        self.assertEqual([0, 1, 2], calls)
        self.assertEqual(0, len(mock_log.log.call_args_list))

    def test_call_with_retries_log_none(self) -> None:
        """Ensures call_with_retries works when log is None."""
        retry_policy = RetryPolicy(
            max_retries=2,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        calls: list[int] = []

        def fn(retry: Retry) -> str:
            calls.append(retry.count)
            if retry.count < 1:
                raise RetryableError("fail", display_msg="connect", retry_immediately_once=True) from ValueError("boom")
            return "ok"

        # log=None should skip all logging but still perform retries and return successfully
        self.assertEqual("ok", call_with_retries(fn, policy=retry_policy, config=RetryConfig(display_msg="foo"), log=None))
        self.assertEqual([0, 1], calls)

    def test_call_with_retries_gives_up(self) -> None:
        """Ensures retries are attempted until max retries are exhausted when giveup() is not used."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )

        def fn(retry: Retry) -> None:
            raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")

        with self.assertRaises(ValueError):
            call_with_retries(fn, policy=retry_policy, config=RetryConfig(), log=MagicMock(spec=Logger))

    def test_call_with_retries_retryable_without_cause_raises_retry_error(self) -> None:
        """Ensures that if RetryableError has no __cause__, exhaustion raises RetryError with the RetryableError as the
        chained cause."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )

        def fn(retry: Retry) -> None:
            # Intentionally do not use 'from exc' so __cause__ stays None.
            raise RetryableError("fail", retry_immediately_once=True)

        with self.assertRaises(RetryError) as cm:
            call_with_retries(fn, policy=retry_policy, config=RetryConfig(), log=None)
        retry_error = cm.exception
        cause = retry_error.__cause__
        self.assertIsNotNone(cause)
        assert cause is not None
        self.assertIsInstance(cause, RetryableError)
        assert isinstance(cause, RetryableError)
        self.assertIsNone(cause.__cause__)
        self.assertEqual("fail", str(cause))

    def test_call_with_retries_reraise_disabled_raises_retry_error_with_cause(self) -> None:
        """Ensures reraise=False raises RetryError even when RetryableError preserves __cause__."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
            reraise=False,
        )

        def fn(retry: Retry) -> None:
            raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")

        with self.assertRaises(RetryError) as cm:
            call_with_retries(fn, policy=retry_policy, config=RetryConfig(), log=None)
        retry_error = cm.exception
        self.assertIsInstance(retry_error.__cause__, RetryableError)
        assert isinstance(retry_error.__cause__, RetryableError)
        self.assertIsInstance(retry_error.__cause__.__cause__, ValueError)
        self.assertEqual("boom", str(retry_error.__cause__.__cause__))

    def test_call_with_retries_giveup_stops_retries(self) -> None:
        """Ensures giveup() stops retries immediately and logs a warning."""
        retry_policy = RetryPolicy(
            max_retries=5,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        mock_log = MagicMock(spec=Logger)
        calls: list[int] = []

        def fn(retry: Retry) -> None:
            calls.append(retry.count)
            raise RetryableError("fail") from ValueError("boom")

        def giveup(outcome: AttemptOutcome) -> str:
            self.assertEqual(0, outcome.retry.count)
            self.assertEqual(1234, outcome.elapsed_nanos)
            self.assertGreaterEqual(outcome.sleep_nanos, 0)
            self.assertIs(retry_policy, outcome.retry.policy)
            self.assertIsInstance(outcome.result, RetryableError)
            return "circuit breaker triggered"

        after_attempt_events: list[AttemptOutcome] = []

        def after_attempt(outcome: AttemptOutcome) -> None:
            self.assertEqual(0, outcome.retry.count)
            self.assertIsInstance(outcome.result, RetryableError)
            self.assertIs(mock_log, outcome.log)
            self.assertEqual(1234, outcome.elapsed_nanos)
            self.assertGreaterEqual(outcome.sleep_nanos, 0)
            after_attempt_events.append(outcome)

        with patch("time.monotonic_ns", side_effect=[0, 1234]):
            with self.assertRaises(ValueError):
                call_with_retries(
                    fn, policy=retry_policy, config=RetryConfig(), giveup=giveup, after_attempt=after_attempt, log=mock_log
                )

        # giveup() must prevent additional retries
        self.assertEqual([0], calls)
        mock_log.log.assert_called_once()
        log_call_args = mock_log.log.call_args[0]
        self.assertEqual(logging.WARNING, log_call_args[0])
        self.assertEqual(1, len(after_attempt_events))
        self.assertFalse(after_attempt_events[0].is_success)
        self.assertTrue(after_attempt_events[0].is_exhausted)
        self.assertFalse(after_attempt_events[0].is_terminated)
        self.assertEqual("circuit breaker triggered", after_attempt_events[0].giveup_reason)

    def test_call_with_retries_giveup_reason_in_retry_error(self) -> None:
        """Ensures giveup_reason is surfaced via RetryError when reraise is disabled."""
        retry_policy = RetryPolicy(
            max_retries=5, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0, max_elapsed_secs=1, reraise=False
        )
        mock_log = MagicMock(spec=Logger)

        def fn(retry: Retry) -> None:
            raise RetryableError("fail") from ValueError("boom")

        def giveup(outcome: AttemptOutcome) -> str:
            self.assertEqual(0, outcome.retry.count)
            self.assertEqual(1234, outcome.elapsed_nanos)
            self.assertGreaterEqual(outcome.sleep_nanos, 0)
            self.assertIs(retry_policy, outcome.retry.policy)
            self.assertIsInstance(outcome.result, RetryableError)
            return "circuit breaker triggered"

        with patch("time.monotonic_ns", side_effect=[0, 1234]):
            with self.assertRaises(RetryError) as cm:
                call_with_retries(fn, policy=retry_policy, config=RetryConfig(), giveup=giveup, log=mock_log)

        err = cm.exception
        self.assertEqual("circuit breaker triggered", err.outcome.giveup_reason)
        self.assertFalse(err.outcome.is_terminated)
        self.assertEqual(0, err.outcome.retry.count)
        self.assertEqual(1234, err.outcome.elapsed_nanos)

    def test_call_with_retries_no_retries(self) -> None:
        """Ensures no retries or warning logs are emitted when retries are disabled."""
        retry_policy = RetryPolicy.no_retries()
        mock_log = MagicMock(spec=Logger)

        def fn(retry: Retry) -> None:
            raise RetryableError("fail") from ValueError("boom")

        with self.assertRaises(ValueError):
            call_with_retries(fn, policy=retry_policy, config=RetryConfig(), log=mock_log)
        mock_log.log.assert_not_called()

    def test_call_with_retries_elapsed_time(self) -> None:
        """Ensures retries stop and a warning log is emitted when the max elapsed time is exceeded."""
        retry_policy = RetryPolicy(
            max_retries=5,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=0,
        )
        mock_log = MagicMock(spec=Logger)
        max_elapsed = retry_policy.max_elapsed_nanos

        with patch("time.monotonic_ns", side_effect=[0, max_elapsed + 1]):

            def fn(retry: Retry) -> None:
                raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")

            with self.assertRaises(ValueError):
                call_with_retries(
                    fn, policy=retry_policy, config=RetryConfig(termination_event=threading.Event()), log=mock_log
                )
        mock_log.log.assert_called_once()
        warning_call_args = mock_log.log.call_args[0]
        self.assertEqual(logging.WARNING, warning_call_args[0])

    def test_call_with_retries_empty_display_msg_uses_default(self) -> None:
        """Ensures default 'Retrying ' message is used when display_msg is empty."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        mock_log = MagicMock(spec=Logger)

        def fn(retry: Retry) -> None:
            raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")

        with self.assertRaises(ValueError):
            call_with_retries(fn, policy=retry_policy, config=RetryConfig(display_msg=""), log=mock_log)

        self.assertEqual(mock_log.log.call_count, 2)
        warning_call_args = mock_log.log.call_args_list[-1][0]
        self.assertEqual(logging.WARNING, warning_call_args[0])
        warning_msg = warning_call_args[2]
        self.assertTrue(warning_msg.startswith("Retrying exhausted; giving up because the last [1/1] retries"))

    def test_call_with_retries_custom_log_levels(self) -> None:
        """Ensures call_with_retries uses provided info and warning log levels."""
        retry_policy = RetryPolicy(
            max_retries=2,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        mock_log = MagicMock(spec=Logger)

        def fn(retry: Retry) -> None:
            raise RetryableError("fail", retry_immediately_once=(retry.count == 0)) from ValueError("boom")

        with self.assertRaises(ValueError):
            call_with_retries(
                fn,
                policy=retry_policy,
                config=RetryConfig(info_loglevel=logging.DEBUG, warning_loglevel=logging.ERROR),
                log=mock_log,
            )

        info_call_args = mock_log.log.call_args_list[0][0]
        warning_call_args = mock_log.log.call_args_list[-1][0]
        self.assertEqual(logging.DEBUG, info_call_args[0])
        self.assertEqual("Retrying [1/2] in 0ns ...", info_call_args[2])
        info_call_args = mock_log.log.call_args_list[1][0]
        self.assertEqual("Retrying [2/2] in 0ns ...", info_call_args[2])
        self.assertEqual(logging.ERROR, warning_call_args[0])
        self.assertTrue(warning_call_args[2].startswith("Retrying exhausted; giving up because the last [2/2] retries"))

    def test_call_with_retries_after_attempt_success(self) -> None:
        """Ensures after_attempt is invoked for both retries and final success with correct flags."""
        retry_policy = RetryPolicy(
            max_retries=3,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=10,
        )
        events: list[AttemptOutcome] = []

        def fn(retry: Retry) -> str:
            # Fail twice, then succeed.
            if retry.count < 2:
                raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")
            return "ok"

        def after_attempt(outcome: AttemptOutcome) -> None:
            events.append(outcome)
            self.assertFalse(outcome.is_terminated)
            self.assertEqual("", outcome.giveup_reason)
            self.assertIsNone(outcome.log)

        final_result = call_with_retries(
            fn, policy=retry_policy, config=RetryConfig(), after_attempt=after_attempt, log=None
        )
        self.assertEqual("ok", final_result)

        # We expect two failed attempts (counts 0 and 1) and one success (count 2).
        self.assertEqual(3, len(events))

        # All intermediate events must be failures (is_success=False, is_exhausted=False, error present).
        for i, outcome in enumerate(events[:-1]):
            self.assertEqual(i, outcome.retry.count)
            self.assertFalse(outcome.is_success)
            self.assertFalse(outcome.is_exhausted)
            self.assertIsInstance(outcome.result, RetryableError)
            self.assertGreaterEqual(outcome.elapsed_nanos, 0)
            self.assertGreaterEqual(outcome.sleep_nanos, 0)
            if i == 0:
                self.assertEqual(0, outcome.sleep_nanos)

        # Last event must be the success (is_success=True, error is None) for attempt index 2.
        outcome = events[-1]
        self.assertEqual(2, outcome.retry.count)
        self.assertTrue(outcome.is_success)
        self.assertFalse(outcome.is_exhausted)
        self.assertEqual("", outcome.giveup_reason)
        self.assertEqual("ok", outcome.result)
        self.assertGreaterEqual(outcome.elapsed_nanos, 0)
        self.assertEqual(0, outcome.sleep_nanos)

    def test_call_with_retries_after_attempt_exhausted(self) -> None:
        """Ensures after_attempt is invoked with is_exhausted when retries are exhausted."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        events: list[AttemptOutcome] = []

        def fn(retry: Retry) -> None:
            raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")

        def after_attempt(outcome: AttemptOutcome) -> None:
            events.append(outcome)
            self.assertEqual("", outcome.giveup_reason)
            self.assertIsNone(outcome.log)

        with self.assertRaises(ValueError):
            call_with_retries(fn, policy=retry_policy, config=RetryConfig(), after_attempt=after_attempt, log=None)

        # There must be at least one event and the last one must indicate exhaustion.
        self.assertGreaterEqual(len(events), 1)
        outcome = events[-1]
        self.assertFalse(outcome.is_success)
        self.assertTrue(outcome.is_exhausted)
        self.assertFalse(outcome.is_terminated)
        self.assertEqual("", outcome.giveup_reason)
        self.assertIsInstance(outcome.result, RetryableError)
        self.assertGreaterEqual(outcome.retry.count, 0)
        self.assertGreaterEqual(outcome.elapsed_nanos, 0)
        self.assertGreaterEqual(outcome.sleep_nanos, 0)

    def test_call_with_retries_using_decorrelated_jitter_as_custom_backoff_strategy(self) -> None:
        """Ensures decorrelated-jitter can be used as a custom backoff_strategy."""
        seen_state: list[int] = []

        def recording_strategy(
            retry: Retry, curr_max_sleep_nanos: int, rng: random.Random, elapsed_nanos: int, retryable_error: RetryableError
        ) -> tuple[int, int]:
            seen_state.append(curr_max_sleep_nanos)
            return decorrelated_jitter_backoff_strategy(retry, curr_max_sleep_nanos, rng, elapsed_nanos, retryable_error)

        retry_policy = RetryPolicy(
            max_retries=3,
            min_sleep_secs=4e-9,  # 4ns base
            initial_max_sleep_secs=8e-9,  # intentionally != base; impl should still start at base
            max_sleep_secs=16e-9,  # 16ns cap
            max_elapsed_secs=1,
            exponential_base=2,
            backoff_strategy=recording_strategy,
        )
        calls: list[int] = []
        retry_sleep_nanos: list[int] = []

        def fn(retry: Retry) -> str:
            calls.append(retry.count)
            if retry.count < 2:
                raise RetryableError("fail") from ValueError("boom")
            return "ok"

        def after_attempt(outcome: AttemptOutcome) -> None:
            self.assertFalse(outcome.is_terminated)
            self.assertEqual("", outcome.giveup_reason)
            if not outcome.is_success and not outcome.is_exhausted:
                retry_sleep_nanos.append(outcome.sleep_nanos)
                self.assertIsInstance(outcome.result, RetryableError)
                self.assertGreaterEqual(outcome.elapsed_nanos, 0)
            self.assertIsNone(outcome.log)

        rng = SequenceRandom([5, 6])
        with patch("bzfs_main.util.retry._thread_local_rng", return_value=rng):
            actual = call_with_retries(
                fn,
                policy=retry_policy,
                config=RetryConfig(termination_event=threading.Event()),
                after_attempt=after_attempt,
                log=None,
            )

        self.assertEqual("ok", actual)
        self.assertEqual([0, 1, 2], calls)
        self.assertEqual([retry_policy.initial_max_sleep_nanos, 5], seen_state)
        self.assertEqual([5, 6], retry_sleep_nanos)

    def test_call_with_retries_max_previous_outcomes_1(self) -> None:
        """Ensures Retry.previous_outcomes retains only the most recent AttemptOutcome object."""
        retry_policy = RetryPolicy(
            max_retries=3,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
            max_previous_outcomes=1,
        )
        history_counts_per_attempt: list[list[int]] = []
        nested_history_sizes_per_attempt: list[list[int]] = []

        def fn(retry: Retry) -> str:
            history_counts_per_attempt.append([outcome.retry.count for outcome in retry.previous_outcomes])
            nested_history_sizes_per_attempt.append(
                [len(outcome.retry.previous_outcomes) for outcome in retry.previous_outcomes]
            )
            for outcome in retry.previous_outcomes:
                self.assertEqual((), outcome.retry.previous_outcomes)
            if retry.count < 2:
                raise RetryableError("fail", retry_immediately_once=(retry.count == 0)) from ValueError("boom")
            return "ok"

        self.assertEqual("ok", call_with_retries(fn, policy=retry_policy, config=RetryConfig(), log=None))
        self.assertEqual([[], [0], [1]], history_counts_per_attempt)
        self.assertEqual([[], [0], [0]], nested_history_sizes_per_attempt)

    def test_call_with_retries_max_previous_outcomes_2(self) -> None:
        """Ensures Retry.previous_outcomes retains the last 2 AttemptOutcome objects."""
        retry_policy = RetryPolicy(
            max_retries=3,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
            max_previous_outcomes=2,
        )
        history_counts_per_attempt: list[list[int]] = []

        def fn(retry: Retry) -> str:
            history_counts_per_attempt.append([outcome.retry.count for outcome in retry.previous_outcomes])
            if retry.count < 3:
                raise RetryableError("fail", retry_immediately_once=(retry.count == 0)) from ValueError("boom")
            return "ok"

        self.assertEqual("ok", call_with_retries(fn, policy=retry_policy, config=RetryConfig(), log=None))
        self.assertEqual([[], [0], [0, 1], [1, 2]], history_counts_per_attempt)

    def test_call_with_retries_previous_outcomes_are_detached(self) -> None:
        """Ensures Retry.previous_outcomes entries do not retain their own previous_outcomes history."""
        retry_policy = RetryPolicy(
            max_retries=4,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
            max_previous_outcomes=2,
        )
        nested_history_sizes_per_attempt: list[list[int]] = []

        def fn(retry: Retry) -> str:
            nested_history_sizes_per_attempt.append(
                [len(outcome.retry.previous_outcomes) for outcome in retry.previous_outcomes]
            )
            for outcome in retry.previous_outcomes:
                self.assertEqual((), outcome.retry.previous_outcomes)
            if retry.count < 4:
                raise RetryableError("fail", retry_immediately_once=(retry.count == 0)) from ValueError("boom")
            return "ok"

        self.assertEqual("ok", call_with_retries(fn, policy=retry_policy, config=RetryConfig(), log=None))
        self.assertEqual([[], [0], [0, 0], [0, 0], [0, 0]], nested_history_sizes_per_attempt)

    def test_retry_policy_invalid_backoff_strategy_raises_type_error(self) -> None:
        """Ensures RetryPolicy rejects a non-callable backoff_strategy."""
        with self.assertRaises(TypeError) as cm:
            RetryPolicy(backoff_strategy=123)  # type: ignore[arg-type]
        self.assertIn("backoff_strategy", str(cm.exception))

    def test_retry_policy_invalid_reraise_raises_type_error(self) -> None:
        """Ensures RetryPolicy rejects a non-bool reraise value."""
        with self.assertRaises(TypeError) as cm:
            RetryPolicy(reraise="no")  # type: ignore[arg-type]
        self.assertIn("reraise", str(cm.exception))


#############################################################################
class TestRetryPolicyCopy(unittest.TestCase):

    def test_copy_returns_distinct_but_equal_policy(self) -> None:
        """Ensures copy() returns a new RetryPolicy instance with identical field values when no overrides are given."""
        original = RetryPolicy(
            max_retries=1, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0, max_elapsed_secs=1
        )
        copied = original.copy()

        self.assertIsNot(original, copied)
        self.assertEqual(original, copied)

    def test_copy_overrides_selected_fields(self) -> None:
        """Ensures copy() correctly overrides selected fields while preserving others."""
        original = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
            exponential_base=2,
        )

        copied = original.copy(max_retries=2, max_elapsed_secs=3)

        self.assertIsNot(original, copied)
        self.assertEqual(1, original.max_retries)
        self.assertEqual(2, copied.max_retries)
        self.assertEqual(1, original.max_elapsed_secs)
        self.assertEqual(3, copied.max_elapsed_secs)
        self.assertEqual(1_000_000_000, original.max_elapsed_nanos)
        self.assertEqual(3_000_000_000, copied.max_elapsed_nanos)
        self.assertEqual(original.exponential_base, copied.exponential_base)

    def test_copy_disallows_overriding_derived_values(self) -> None:
        """Ensures derived nano fields cannot be overridden via copy()."""
        original = RetryPolicy(
            max_retries=1, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0, max_elapsed_secs=1
        )
        # Python <= 3.12 raises ValueError; >= 3.13 raises TypeError.
        with self.assertRaises((TypeError, ValueError)):
            original.copy(max_elapsed_nanos=123)


#############################################################################
class TestRetryConfigCopy(unittest.TestCase):

    def test_copy_returns_distinct_but_equal_config(self) -> None:
        """Ensures copy() returns a new RetryConfig instance with identical field values when no overrides are given."""
        original = RetryConfig()
        copied = original.copy()

        self.assertIsNot(original, copied)
        self.assertEqual(original, copied)

    def test_copy_overrides_selected_fields(self) -> None:
        """Ensures copy() correctly overrides selected fields while preserving others."""
        termination_event = threading.Event()

        original = RetryConfig(
            termination_event=termination_event,
            display_msg="Retrying",
            dots=" ...",
            format_msg=lambda msg, _err: msg,
            format_duration=lambda nanos: f"{nanos}ns",
            info_loglevel=logging.INFO,
            warning_loglevel=logging.WARNING,
            exc_info=False,
            stack_info=False,
            extra={"foo": "bar"},
        )

        copied = original.copy(
            display_msg="Copy",
            info_loglevel=logging.DEBUG,
            warning_loglevel=logging.ERROR,
            exc_info=True,
            stack_info=True,
            extra={"baz": 1},
        )

        self.assertIsNot(original, copied)
        self.assertEqual(termination_event, copied.termination_event)
        self.assertEqual("Copy", copied.display_msg)
        self.assertEqual(" ...", copied.dots)
        self.assertEqual(logging.DEBUG, copied.info_loglevel)
        self.assertEqual(logging.ERROR, copied.warning_loglevel)
        self.assertTrue(copied.exc_info)
        self.assertTrue(copied.stack_info)
        self.assertEqual({"baz": 1}, copied.extra)

    def test_repr_hides_extra_and_context(self) -> None:
        """Ensures dataclass repr does not leak potentially sensitive context."""
        cfg = RetryConfig(extra={"secret": "x"}, context={"token": "y"})
        text = repr(cfg)
        self.assertIn("RetryConfig(", text)
        self.assertNotIn("extra=", text)
        self.assertNotIn("context=", text)


#############################################################################
class TestRetryOptionsCopy(unittest.TestCase):

    def test_copy_returns_distinct_but_equal_options(self) -> None:
        """Ensures copy() returns a new RetryOptions instance with identical field values when no overrides are given."""
        original: RetryOptions = RetryOptions()
        copied = original.copy()

        self.assertIsNot(original, copied)
        self.assertEqual(original, copied)

    def test_copy_overrides_selected_fields(self) -> None:
        """Ensures copy() correctly overrides selected fields while preserving others."""
        original_policy = RetryPolicy(max_retries=1)
        original_config = RetryConfig(display_msg="orig")

        def giveup(outcome: AttemptOutcome) -> str:
            return "circuit breaker triggered"

        def after_attempt(outcome: AttemptOutcome) -> None:
            _ = outcome.giveup_reason

        log = MagicMock(spec=Logger)
        original: RetryOptions = RetryOptions(
            policy=original_policy,
            config=original_config,
            giveup=giveup,
            after_attempt=after_attempt,
            log=log,
        )

        new_policy = RetryPolicy(max_retries=2)
        new_config = RetryConfig(display_msg="copy")
        copied = original.copy(policy=new_policy, config=new_config)

        self.assertIsNot(original, copied)
        self.assertEqual(1, original.policy.max_retries)
        self.assertEqual(2, copied.policy.max_retries)
        self.assertEqual("orig", original.config.display_msg)
        self.assertEqual("copy", copied.config.display_msg)
        self.assertIs(original.giveup, copied.giveup)
        self.assertIs(original.after_attempt, copied.after_attempt)
        self.assertIs(original.log, copied.log)


#############################################################################
class TestRetryOptionsCall(unittest.TestCase):

    def test_default_fn_raises_not_implemented_error(self) -> None:
        options: RetryOptions = RetryOptions()
        with self.assertRaises(NotImplementedError) as ctx:
            options()
        self.assertEqual("Provide fn when calling RetryOptions", str(ctx.exception))

    def test_retry_options_is_callable_and_runs(self) -> None:
        """Ensures RetryOptions instances are callable and execute call_with_retries() with their own parameters."""
        calls: list[int] = []

        def fn(retry: Retry) -> str:
            calls.append(retry.count)
            if retry.count == 0:
                raise RetryableError("transient")
            return "ok"

        retry_policy = RetryPolicy(
            max_retries=1, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0, max_elapsed_secs=1
        )
        after_attempts: list[bool] = []

        def after_attempt(outcome: AttemptOutcome) -> None:
            after_attempts.append(outcome.is_success)

        options: RetryOptions[str] = RetryOptions(fn=fn, policy=retry_policy, after_attempt=after_attempt, log=None)

        self.assertTrue(callable(options))
        self.assertEqual("ok", options())
        self.assertEqual([0, 1], calls)
        self.assertEqual([False, True], after_attempts)


#############################################################################
class TestAttemptOutcomeCopy(unittest.TestCase):

    def test_copy_overrides_selected_fields(self) -> None:
        """Ensures copy() correctly overrides selected fields while preserving others."""
        original_retry = Retry(
            count=1,
            start_time_nanos=100,
            policy=RetryPolicy(max_retries=1),
            config=RetryConfig(display_msg="orig"),
            previous_outcomes=(),
        )
        original_log = MagicMock(spec=Logger)
        original = AttemptOutcome(
            retry=original_retry,
            is_success=False,
            is_exhausted=False,
            is_terminated=False,
            giveup_reason="",
            elapsed_nanos=123,
            sleep_nanos=456,
            result=RetryableError("result"),
            log=original_log,
        )

        copied_retry = Retry(
            count=2,
            start_time_nanos=200,
            policy=RetryPolicy(max_retries=2),
            config=RetryConfig(display_msg="copy"),
            previous_outcomes=(),
        )
        copied = original.copy(
            retry=copied_retry,
            is_success=True,
            is_exhausted=True,
            is_terminated=True,
            giveup_reason="done",
            elapsed_nanos=999,
            sleep_nanos=0,
            result=object(),
            log=None,
        )

        self.assertIsNot(original, copied)
        self.assertIs(original_retry, original.retry)
        self.assertFalse(original.is_success)
        self.assertFalse(original.is_exhausted)
        self.assertFalse(original.is_terminated)
        self.assertEqual("", original.giveup_reason)
        self.assertEqual(123, original.elapsed_nanos)
        self.assertEqual(456, original.sleep_nanos)
        self.assertIs(original_log, original.log)

        self.assertIs(copied_retry, copied.retry)
        self.assertTrue(copied.is_success)
        self.assertTrue(copied.is_exhausted)
        self.assertTrue(copied.is_terminated)
        self.assertEqual("done", copied.giveup_reason)
        self.assertEqual(999, copied.elapsed_nanos)
        self.assertEqual(0, copied.sleep_nanos)
        self.assertIsNone(copied.log)


#############################################################################
class TestCallWithRetriesBenchmark(unittest.TestCase):

    def test_benchmark_exhausted_r1k_n0_p0_immediate_success_Yes(self) -> None:  # noqa: N802
        self.benchmark(runs=1000, max_retries=0, max_previous_outcomes=0, immediate_success=True)

    def test_benchmark_exhausted_r1k_n0_p0_immediate_success_no(self) -> None:
        self.benchmark(runs=1000, max_retries=0, max_previous_outcomes=0)

    def test_benchmark_exhausted_r1k_n1_p1(self) -> None:
        self.benchmark(runs=1000, max_retries=1, max_previous_outcomes=1)

    def test_benchmark_exhausted_r1k_n2_p1(self) -> None:
        self.benchmark(runs=1000, max_retries=2, max_previous_outcomes=1)

    def test_benchmark_exhausted_r1k_n2_p2(self) -> None:
        self.benchmark(runs=1000, max_retries=2, max_previous_outcomes=2)

    def test_benchmark_exhausted_r1k_n1_p0(self) -> None:
        self.benchmark(runs=1000, max_retries=1, max_previous_outcomes=0)

    def test_benchmark_exhausted_r1k_n5_p0(self) -> None:
        self.benchmark(runs=1000, max_retries=5, max_previous_outcomes=0)

    @unittest.skip("benchmark; enable for performance comparison")
    def test_benchmark_exhausted_r100k_n1_p1(self) -> None:
        self.benchmark(runs=100_000, max_retries=1, max_previous_outcomes=1)

    def benchmark(
        self, runs: int, max_retries: int, max_previous_outcomes: int = 0, immediate_success: bool = False
    ) -> None:
        retry_policy = RetryPolicy(
            max_retries=max_retries,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1000,
            max_previous_outcomes=max_previous_outcomes,
        )

        def fn(_retry: Retry) -> None:
            if not immediate_success:
                raise RetryableError("fail")

        config = RetryConfig()
        try:  # warmup
            call_with_retries(fn, policy=retry_policy.copy(max_retries=100), config=config, log=None)
        except RetryError as exc:
            if immediate_success:
                self.fail("success expected")
            else:
                self.assertEqual(100, exc.outcome.retry.count)
                self.assertFalse(exc.outcome.is_terminated)
                self.assertTrue(exc.outcome.is_exhausted)
        else:
            if not immediate_success:
                self.fail("failure expected")

        log = logging.getLogger("TestRunWithRetriesBenchmark")
        log.setLevel(logging.INFO)
        if not log.handlers:
            log.addHandler(logging.StreamHandler())

        import gc

        gc.collect()

        iters = 0
        start = time.perf_counter()
        for _ in range(runs):
            try:
                iters += max_retries + 1
                call_with_retries(fn, policy=retry_policy, config=config, log=None)
            except RetryError:
                pass

        elapsed_secs = time.perf_counter() - start

        iters_per_sec = float("inf") if elapsed_secs <= 0 else iters / elapsed_secs
        runs_per_sec = float("inf") if elapsed_secs <= 0 else runs / elapsed_secs
        log.info(
            f"call_with_retries benchmark: "
            f"runs={runs}, max_retries={max_retries}, max_previous_outcomes={max_previous_outcomes}, "
            f"runs/sec={runs_per_sec:.0f}, iters/sec={iters_per_sec:.0f}, elapsed={elapsed_secs:.3f}s"
        )
