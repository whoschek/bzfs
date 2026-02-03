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
import unittest
from logging import (
    Logger,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from bzfs_main.util.retry import (
    INFINITY_MAX_RETRIES,
    AttemptOutcome,
    BackoffContext,
    Retry,
    RetryableError,
    RetryConfig,
    RetryError,
    RetryPolicy,
    RetryTemplate,
    _sleep,
    after_attempt_log_failure,
    all_giveup,
    any_giveup,
    call_with_exception_handlers,
    call_with_retries,
    multi_after_attempt,
    no_giveup,
    raise_retryable_error_from,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestSleep,
        TestCallWithRetries,
        TestRetryPolicyCopy,
        TestRetryConfigCopy,
        TestRetryTemplateCopy,
        TestRetryTemplateCall,
        TestAttemptOutcomeCopy,
        TestCallWithExceptionHandlers,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestSleep(unittest.TestCase):

    def test_sleep_calls_time_sleep_when_termination_event_is_none(self) -> None:
        sleep_nanos = 123_000_000
        expected_secs = sleep_nanos / 1_000_000_000
        retry = Retry(
            count=0,
            start_time_nanos=0,
            attempt_start_time_nanos=0,
            policy=RetryPolicy.no_retries(),
            config=RetryConfig(termination_event=None),
            log=None,
            previous_outcomes=(),
        )
        with (
            patch("bzfs_main.util.retry.time.sleep") as mock_sleep,
            patch("bzfs_main.util.retry.threading.Event.wait") as mock_wait,
        ):
            _sleep(sleep_nanos, retry)
        mock_sleep.assert_called_once()
        mock_wait.assert_not_called()
        self.assertAlmostEqual(expected_secs, mock_sleep.call_args[0][0])

    def test_sleep_calls_event_wait_when_termination_event_is_not_none(self) -> None:
        sleep_nanos = 123_000_000
        expected_secs = sleep_nanos / 1_000_000_000
        termination_event = threading.Event()
        retry = Retry(
            count=0,
            start_time_nanos=0,
            attempt_start_time_nanos=0,
            policy=RetryPolicy.no_retries(),
            config=RetryConfig(termination_event=termination_event),
            log=None,
            previous_outcomes=(),
        )
        with patch("bzfs_main.util.retry.time.sleep") as mock_sleep:
            with patch.object(termination_event, "wait") as mock_wait:
                _sleep(sleep_nanos, retry)
        mock_sleep.assert_not_called()
        mock_wait.assert_called_once()
        self.assertAlmostEqual(expected_secs, mock_wait.call_args[0][0])


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

    def test_call_with_retries_retry_immediately_once_skips_backoff_and_sleep(self) -> None:
        """Ensures retry_immediately_once triggers an immediate retry without computing backoff or sleeping."""
        backoff_strategy = MagicMock(side_effect=AssertionError("backoff_strategy must not be called"))
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=1,
            initial_max_sleep_secs=1,
            max_sleep_secs=1,
            max_elapsed_secs=10,
            backoff_strategy=backoff_strategy,
        )
        calls: list[int] = []
        events: list[AttemptOutcome] = []

        def fn(retry: Retry) -> str:
            calls.append(retry.count)
            if retry.count == 0:
                raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")
            return "ok"

        def after_attempt(outcome: AttemptOutcome) -> None:
            events.append(outcome)

        with patch("bzfs_main.util.retry._sleep") as mock_sleep:
            actual = call_with_retries(
                fn,
                policy=retry_policy,
                config=RetryConfig(),
                after_attempt=after_attempt,
                log=None,
            )

        self.assertEqual("ok", actual)
        self.assertEqual([0, 1], calls)
        self.assertEqual(2, len(events))
        self.assertFalse(events[0].is_success)
        self.assertFalse(events[0].is_exhausted)
        self.assertEqual(0, events[0].retry.count)
        self.assertEqual(0, events[0].sleep_nanos)
        self.assertTrue(events[1].is_success)
        self.assertEqual(1, events[1].retry.count)
        self.assertEqual(0, events[1].sleep_nanos)
        mock_sleep.assert_not_called()
        backoff_strategy.assert_not_called()

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

    def test_call_with_retries_after_attempt_retryable_error_on_success_retries(self) -> None:
        """Ensures raising RetryableError from after_attempt() on a successful attempt triggers a retry."""
        retry_policy = RetryPolicy(max_retries=3, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0)
        fn_calls: list[int] = []
        after_attempt_events: list[tuple[bool, bool, int]] = []
        raised_once: bool = False

        def fn(retry: Retry) -> str:
            fn_calls.append(retry.count)
            return "ok"

        def after_attempt(outcome: AttemptOutcome) -> None:
            nonlocal raised_once
            after_attempt_events.append((outcome.is_success, outcome.is_exhausted, outcome.retry.count))
            if outcome.is_success and not raised_once:
                raised_once = True
                raise RetryableError("retry from after_attempt")

        actual = call_with_retries(fn, policy=retry_policy, config=RetryConfig(), after_attempt=after_attempt, log=None)
        self.assertTrue(raised_once)
        self.assertEqual("ok", actual)
        self.assertEqual([0, 1], fn_calls)
        self.assertEqual([(True, False, 0), (False, False, 0), (True, False, 1)], after_attempt_events)

    def test_call_with_retries_after_attempt_retryable_error_on_failure_aborts(self) -> None:
        """Ensures raising RetryableError from after_attempt() on failure aborts without additional retries."""
        retry_policy = RetryPolicy(max_retries=3, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0)
        fn_calls: list[int] = []
        after_attempt_calls: list[int] = []

        def fn(retry: Retry) -> None:
            fn_calls.append(retry.count)
            raise RetryableError("attempt failed")

        def after_attempt(outcome: AttemptOutcome) -> None:
            after_attempt_calls.append(outcome.retry.count)
            if not outcome.is_success and not outcome.is_exhausted:
                raise RetryableError("after_attempt abort")

        with patch("bzfs_main.util.retry._sleep") as mock_sleep:
            with self.assertRaises(RetryableError):
                call_with_retries(fn, policy=retry_policy, config=RetryConfig(), after_attempt=after_attempt, log=None)

        self.assertEqual([0], fn_calls)
        self.assertEqual([0], after_attempt_calls)
        mock_sleep.assert_not_called()

    def test_call_with_retries_giveup_stops_retries(self) -> None:
        """Ensures giveup() stops retries immediately and custom after_attempt disables default logging."""
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

        def giveup(outcome: AttemptOutcome) -> object | None:
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
            self.assertIs(mock_log, outcome.retry.log)
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
        mock_log.log.assert_not_called()
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

        def giveup(outcome: AttemptOutcome) -> object | None:
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

    def test_call_with_retries_on_exhaustion_returns_fallback_and_overrides_reraise(self) -> None:
        """Ensures on_exhaustion() can return a fallback value instead of raising on exhaustion."""
        retry_policy = RetryPolicy(
            max_retries=0, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0, max_elapsed_secs=1
        )
        events: list[str] = []

        def fn(retry: Retry) -> str:
            raise RetryableError("fail") from ValueError("boom")

        def after_attempt(outcome: AttemptOutcome) -> None:
            self.assertTrue(outcome.is_exhausted)
            events.append("after_attempt")

        def on_exhaustion(outcome: AttemptOutcome) -> str:
            self.assertTrue(outcome.is_exhausted)
            self.assertIsInstance(outcome.result, RetryableError)
            events.append("on_exhaustion")
            return "fallback"

        actual = call_with_retries(
            fn,
            policy=retry_policy,
            config=RetryConfig(),
            after_attempt=after_attempt,
            on_exhaustion=on_exhaustion,
            log=None,
        )
        self.assertEqual("fallback", actual)
        self.assertEqual(["after_attempt", "on_exhaustion"], events)

    def test_call_with_retries_on_exhaustion_called_exactly_once(self) -> None:
        """Ensures on_exhaustion() is called once (only) on the exhaustion path."""
        retry_policy = RetryPolicy(
            max_retries=2, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0, max_elapsed_secs=1
        )
        calls: list[int] = []
        after_attempt_calls: list[int] = []
        on_exhaustion_calls: list[int] = []

        def fn(retry: Retry) -> str:
            calls.append(retry.count)
            raise RetryableError("fail") from ValueError("boom")

        def after_attempt(outcome: AttemptOutcome) -> None:
            after_attempt_calls.append(outcome.retry.count)

        def on_exhaustion(outcome: AttemptOutcome) -> str:
            on_exhaustion_calls.append(outcome.retry.count)
            return "fallback"

        actual = call_with_retries(
            fn,
            policy=retry_policy,
            config=RetryConfig(),
            after_attempt=after_attempt,
            on_exhaustion=on_exhaustion,
            log=None,
        )
        self.assertEqual("fallback", actual)
        self.assertEqual([0, 1, 2], calls)
        self.assertEqual([0, 1, 2], after_attempt_calls)
        self.assertEqual([2], on_exhaustion_calls)

    def test_call_with_retries_on_exhaustion_called_on_giveup(self) -> None:
        """Ensures on_exhaustion() runs when giveup() stops retries early."""
        retry_policy = RetryPolicy(
            max_retries=5, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0, max_elapsed_secs=1
        )

        def fn(retry: Retry) -> str:
            raise RetryableError("fail") from ValueError("boom")

        def giveup(outcome: AttemptOutcome) -> object | None:
            return "circuit breaker triggered"

        def on_exhaustion(outcome: AttemptOutcome) -> str:
            self.assertTrue(outcome.is_exhausted)
            self.assertEqual("circuit breaker triggered", outcome.giveup_reason)
            return "fallback"

        actual = call_with_retries(
            fn,
            policy=retry_policy,
            config=RetryConfig(),
            giveup=giveup,
            on_exhaustion=on_exhaustion,
            log=None,
        )
        self.assertEqual("fallback", actual)

    def test_call_with_retries_on_exhaustion_not_called_on_success(self) -> None:
        """Ensures on_exhaustion() is not called if a later attempt succeeds."""
        retry_policy = RetryPolicy(
            max_retries=2, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0, max_elapsed_secs=1
        )
        calls: list[int] = []
        on_exhaustion_calls: list[AttemptOutcome] = []

        def fn(retry: Retry) -> str:
            calls.append(retry.count)
            if retry.count == 0:
                raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")
            return "ok"

        def on_exhaustion(outcome: AttemptOutcome) -> str:
            on_exhaustion_calls.append(outcome)
            return "fallback"

        actual = call_with_retries(fn, policy=retry_policy, config=RetryConfig(), on_exhaustion=on_exhaustion, log=None)
        self.assertEqual("ok", actual)
        self.assertEqual([0, 1], calls)
        self.assertEqual([], on_exhaustion_calls)

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

    def test_call_with_retries_infinity_max_retries_log_display(self) -> None:
        """Ensures INFINITY_MAX_RETRIES displays as '∞' in after_attempt_log_failure log output."""
        retry_policy = RetryPolicy(
            max_retries=INFINITY_MAX_RETRIES,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        mock_log = MagicMock(spec=Logger)
        mock_log.isEnabledFor.return_value = True

        def fn(retry: Retry) -> str:
            if retry.count == 0:
                raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")
            return "ok"

        self.assertEqual("ok", call_with_retries(fn, policy=retry_policy, config=RetryConfig(), log=mock_log))
        self.assertEqual(1, mock_log.log.call_count)
        info_call_args = mock_log.log.call_args_list[0][0]
        self.assertEqual(logging.INFO, info_call_args[0])
        self.assertEqual("Retrying [1/∞] in 0ns ...", info_call_args[2])

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
            self.assertIsNone(outcome.giveup_reason)
            self.assertIsNone(outcome.retry.log)

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
        self.assertIsNone(outcome.giveup_reason)
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
            self.assertIsNone(outcome.giveup_reason)
            self.assertIsNone(outcome.retry.log)

        with self.assertRaises(ValueError):
            call_with_retries(fn, policy=retry_policy, config=RetryConfig(), after_attempt=after_attempt, log=None)

        # There must be at least one event and the last one must indicate exhaustion.
        self.assertGreaterEqual(len(events), 1)
        outcome = events[-1]
        self.assertFalse(outcome.is_success)
        self.assertTrue(outcome.is_exhausted)
        self.assertFalse(outcome.is_terminated)
        self.assertIsNone(outcome.giveup_reason)
        self.assertIsInstance(outcome.result, RetryableError)
        self.assertGreaterEqual(outcome.retry.count, 0)
        self.assertGreaterEqual(outcome.elapsed_nanos, 0)
        self.assertGreaterEqual(outcome.sleep_nanos, 0)

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

    def test_retry_policy_invalid_max_retries_raises_value_error(self) -> None:
        """Ensures RetryPolicy rejects invalid values via _validate()."""
        with self.assertRaises(ValueError) as cm:
            RetryPolicy(max_retries=-1)
        self.assertIn("Invalid RetryPolicy.max_retries", str(cm.exception))

    def test_retry_policy_rejects_non_finite_durations(self) -> None:
        """Ensures RetryPolicy rejects NaN/inf values for duration fields."""
        with self.assertRaises(OverflowError):
            RetryPolicy(max_elapsed_secs=float("inf"))
        with self.assertRaises(ValueError):
            RetryPolicy(max_sleep_secs=float("nan"))

    def test_retry_policy_invalid_reraise_raises_type_error(self) -> None:
        """Ensures RetryPolicy rejects a non-bool reraise value."""
        with self.assertRaises(TypeError) as cm:
            RetryPolicy(reraise="no")  # type: ignore[arg-type]
        self.assertIn("reraise", str(cm.exception))

    def test_call_with_retries_termination_event_can_flip_between_checks(self) -> None:
        """Ensures termination_event can become set between two consecutive is_set() checks."""
        retry_policy = RetryPolicy(
            max_retries=3,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=10,
            max_previous_outcomes=1,
        )
        termination_event = MagicMock(spec=threading.Event)
        is_set_returns: list[bool] = []
        is_set_call_count: int = 0

        def is_set() -> bool:
            nonlocal is_set_call_count
            is_set_call_count += 1
            value = is_set_call_count >= 2
            is_set_returns.append(value)
            return value

        termination_event.is_set.side_effect = is_set
        calls: list[int] = []
        events: list[AttemptOutcome] = []

        def fn(retry: Retry) -> None:
            calls.append(retry.count)
            raise RetryableError("fail") from ValueError("boom")

        def after_attempt(outcome: AttemptOutcome) -> None:
            events.append(outcome)

        with self.assertRaises(ValueError):
            call_with_retries(
                fn,
                policy=retry_policy,
                config=RetryConfig(termination_event=termination_event),
                after_attempt=after_attempt,
                log=None,
            )

        self.assertEqual([0], calls)
        self.assertGreaterEqual(len(is_set_returns), 2)
        self.assertEqual([False, True], is_set_returns[:2])
        self.assertEqual(2, len(events))
        self.assertFalse(events[0].is_success)
        self.assertFalse(events[0].is_exhausted)
        self.assertFalse(events[0].is_terminated)
        self.assertFalse(events[1].is_success)
        self.assertTrue(events[1].is_exhausted)
        self.assertTrue(events[1].is_terminated)
        self.assertEqual((), events[1].retry.previous_outcomes)

    def test_full_jitter_backoff_uses_curr_max_when_min_equals_curr_max(self) -> None:
        """Ensures full-jitter uses curr_max_sleep_nanos directly when min==curr (perf path)."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0.001,
            initial_max_sleep_secs=0.001,
            max_sleep_secs=1,
            max_elapsed_secs=10,
        )
        rng = MagicMock(spec=random.Random)
        rng.randint.side_effect = AssertionError("randint must not be called")
        calls: list[int] = []
        events: list[AttemptOutcome] = []

        def fn(retry: Retry) -> str:
            calls.append(retry.count)
            if retry.count == 0:
                raise RetryableError("fail") from ValueError("boom")
            return "ok"

        def after_attempt(outcome: AttemptOutcome) -> None:
            events.append(outcome)

        config = RetryConfig()
        with (
            patch("bzfs_main.util.retry._thread_local_rng", return_value=rng),
            patch("bzfs_main.util.retry._sleep") as mock_sleep,
        ):
            actual = call_with_retries(
                fn,
                policy=retry_policy,
                config=config,
                after_attempt=after_attempt,
                log=None,
            )

        self.assertEqual("ok", actual)
        self.assertEqual([0, 1], calls)
        self.assertEqual(2, len(events))
        self.assertEqual(retry_policy.min_sleep_nanos, retry_policy.initial_max_sleep_nanos)
        self.assertFalse(events[0].is_success)
        self.assertEqual(0, events[0].retry.count)
        self.assertEqual(retry_policy.initial_max_sleep_nanos, events[0].sleep_nanos)
        self.assertTrue(events[1].is_success)
        self.assertEqual(1, events[1].retry.count)
        self.assertEqual(0, events[1].sleep_nanos)
        mock_sleep.assert_called_once()
        self.assertEqual(retry_policy.initial_max_sleep_nanos, mock_sleep.call_args.args[0])
        self.assertIs(events[0].retry, mock_sleep.call_args.args[1])
        self.assertIs(config, mock_sleep.call_args.args[1].config)
        rng.randint.assert_not_called()

    def test_thread_local_rng_is_cached_after_first_initialization(self) -> None:
        """Ensures _thread_local_rng caches its RNG so random.Random() is not called again in the same thread."""
        import bzfs_main.util.retry

        threadlocal = bzfs_main.util.retry._THREAD_LOCAL_RNG
        original_rng = threadlocal.rng
        threadlocal.rng = None
        try:
            seen_rngs: list[random.Random] = []

            def backoff_strategy(context: BackoffContext) -> tuple[int, int]:
                seen_rngs.append(context.rng)
                return 0, context.curr_max_sleep_nanos

            retry_policy = RetryPolicy(
                max_retries=1,
                min_sleep_secs=0,
                initial_max_sleep_secs=1e-9,
                max_sleep_secs=1e-9,
                max_elapsed_secs=10,
                backoff_strategy=backoff_strategy,
            )

            sentinel_rng = MagicMock(spec=random.Random)

            def run_once() -> str:
                calls: list[int] = []

                def fn(retry: Retry) -> str:
                    calls.append(retry.count)
                    if retry.count == 0:
                        raise RetryableError("fail") from ValueError("boom")
                    return "ok"

                result = call_with_retries(fn, policy=retry_policy, config=RetryConfig(), log=None)
                self.assertEqual([0, 1], calls)
                return result

            with patch("bzfs_main.util.retry.random.Random", return_value=sentinel_rng) as mock_random:
                self.assertEqual("ok", run_once())
                self.assertEqual("ok", run_once())
            self.assertEqual(1, mock_random.call_count)
            self.assertEqual([sentinel_rng, sentinel_rng], seen_rngs)
        finally:
            threadlocal.rng = original_rng

    def test_repr_eq_hash(self) -> None:
        """Validates Retry/AttemptOutcome semantics for __repr__, __eq__ and __hash__."""
        retry_a = Retry(
            count=0,
            start_time_nanos=123,
            attempt_start_time_nanos=123,
            policy=RetryPolicy(max_retries=1),
            config=RetryConfig(display_msg="a"),
            log=None,
            previous_outcomes=(),
        )
        retry_b = Retry(
            count=0,
            start_time_nanos=123,
            attempt_start_time_nanos=123,
            policy=RetryPolicy(max_retries=999),
            config=RetryConfig(display_msg="b"),
            log=MagicMock(spec=Logger),
            previous_outcomes=(MagicMock(spec=AttemptOutcome),),
        )
        retry_c = Retry(
            count=1,
            start_time_nanos=123,
            attempt_start_time_nanos=123,
            policy=RetryPolicy(max_retries=1),
            config=RetryConfig(display_msg="a"),
            log=None,
            previous_outcomes=(),
        )

        retry_repr = repr(retry_a)
        self.assertIn("Retry(", retry_repr)
        self.assertNotIn("policy", retry_repr)
        self.assertNotIn("config", retry_repr)
        self.assertEqual(retry_a, retry_a)
        self.assertNotEqual(retry_a, retry_b)
        self.assertNotEqual(retry_a, retry_c)
        self.assertEqual(3, len({retry_a, retry_b, retry_c}))
        self.assertNotEqual(retry_a, object())
        self.assertFalse(retry_a == (retry_a.count, retry_a.start_time_nanos))

        outcome_a = AttemptOutcome(
            retry=retry_a,
            is_success=False,
            is_exhausted=False,
            is_terminated=False,
            giveup_reason=None,
            elapsed_nanos=5,
            sleep_nanos=7,
            result="boom",
        )
        outcome_b = outcome_a.copy(result="different")
        outcome_c = outcome_a.copy(sleep_nanos=8)
        outcome_d = outcome_a.copy(retry=retry_b)

        outcome_repr = repr(outcome_a)
        self.assertIn("AttemptOutcome(", outcome_repr)
        self.assertNotIn("result", outcome_repr)
        self.assertNotIn("log", outcome_repr)
        self.assertEqual(outcome_a, outcome_a)
        self.assertNotEqual(outcome_a, outcome_b)
        self.assertNotEqual(outcome_a, outcome_c)
        self.assertNotEqual(outcome_a, outcome_d)
        self.assertEqual(4, len({outcome_a, outcome_b, outcome_c, outcome_d}))
        self.assertNotEqual(outcome_a, object())
        self.assertFalse(outcome_a == (outcome_a.retry, outcome_a.elapsed_nanos, outcome_a.sleep_nanos))

    def test_multi_after_attempt_can_be_passed_as_after_attempt(self) -> None:
        """Ensures multi_after_attempt() composes multiple after_attempt handlers for call_with_retries()."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        after_attempt1: list[bool] = []
        after_attempt2: list[bool] = []
        calls: list[int] = []

        def fn(retry: Retry) -> str:
            calls.append(retry.count)
            if retry.count == 0:
                raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")
            return "ok"

        def handler1(outcome: AttemptOutcome) -> None:
            after_attempt1.append(outcome.is_success)

        def handler2(outcome: AttemptOutcome) -> None:
            after_attempt2.append(outcome.is_success)

        self.assertEqual(
            "ok",
            call_with_retries(
                fn,
                policy=retry_policy,
                config=RetryConfig(),
                after_attempt=multi_after_attempt([handler1, handler2]),
                log=None,
            ),
        )
        self.assertEqual([0, 1], calls)
        self.assertEqual([False, True], after_attempt1)
        self.assertEqual([False, True], after_attempt2)

    def test_multi_after_attempt_returns_default_handler(self) -> None:
        """Ensures multi_after_attempt() returns after_attempt_log_failure without wrapper overhead."""
        self.assertIs(after_attempt_log_failure, multi_after_attempt([after_attempt_log_failure]))

    def test_any_giveup(self) -> None:
        retry = Retry(
            count=0,
            start_time_nanos=1,
            attempt_start_time_nanos=1,
            policy=RetryPolicy(max_retries=0),
            config=RetryConfig(),
            log=None,
            previous_outcomes=(),
        )
        outcome = AttemptOutcome(
            retry=retry,
            is_success=False,
            is_exhausted=False,
            is_terminated=False,
            giveup_reason=None,
            elapsed_nanos=0,
            sleep_nanos=0,
            result=RetryableError("boom"),
        )

        self.assertIs(no_giveup, any_giveup([no_giveup]))

        handler1 = MagicMock(return_value="first reason")
        handler2 = MagicMock(side_effect=AssertionError("Must short-circuit"))
        giveup = any_giveup([handler1, handler2])
        self.assertEqual("first reason", giveup(outcome))
        handler1.assert_called_once_with(outcome)
        handler2.assert_not_called()

        handler3 = MagicMock(return_value=None)
        handler4 = MagicMock(return_value="second reason")
        giveup = any_giveup([handler3, handler4])
        self.assertEqual("second reason", giveup(outcome))
        handler3.assert_called_once_with(outcome)
        handler4.assert_called_once_with(outcome)

        giveup = any_giveup([])
        self.assertIsNone(giveup(outcome))

        giveup = any_giveup([lambda _outcome: None, lambda _outcome: None])
        self.assertIsNone(giveup(outcome))

        giveup = any_giveup([lambda _outcome: "", lambda _outcome: "Must short-circuit"])
        self.assertEqual("", giveup(outcome))

    def test_all_giveup(self) -> None:
        retry = Retry(
            count=0,
            start_time_nanos=0,
            attempt_start_time_nanos=0,
            policy=RetryPolicy(max_retries=0),
            config=RetryConfig(),
            log=None,
            previous_outcomes=(),
        )
        outcome = AttemptOutcome(
            retry=retry,
            is_success=False,
            is_exhausted=False,
            is_terminated=False,
            giveup_reason=None,
            elapsed_nanos=0,
            sleep_nanos=0,
            result=RetryableError("fail"),
        )

        giveup = all_giveup([no_giveup])
        self.assertIs(no_giveup, giveup)

        giveup = all_giveup([])
        self.assertIsNone(giveup(outcome))

        calls: list[str] = []

        def handler_returns_none(_outcome: AttemptOutcome) -> object | None:
            calls.append("h1")
            return None

        handler2 = MagicMock(side_effect=AssertionError("handler2 must not be called"))
        giveup = all_giveup([handler_returns_none, handler2])
        self.assertIsNone(giveup(outcome))
        self.assertEqual(["h1"], calls)

        calls = []

        def handler_returns_r1_then_continue(_outcome: AttemptOutcome) -> object | None:
            calls.append("h1")
            return "r1"

        def handler_returns_none_then_stop(_outcome: AttemptOutcome) -> object | None:
            calls.append("h2")
            return None

        handler3 = MagicMock(side_effect=AssertionError("handler3 must not be called"))
        giveup = all_giveup([handler_returns_r1_then_continue, handler_returns_none_then_stop, handler3])
        self.assertIsNone(giveup(outcome))
        self.assertEqual(["h1", "h2"], calls)

        calls = []

        def handler_returns_r1(_outcome: AttemptOutcome) -> object | None:
            calls.append("h1")
            return "r1"

        def handler_returns_r2(_outcome: AttemptOutcome) -> object | None:
            calls.append("h2")
            return "r2"

        giveup = all_giveup([handler_returns_r1, handler_returns_r2])
        self.assertEqual("r2", giveup(outcome))
        self.assertEqual(["h1", "h2"], calls)

    def test_backoff_context_repr_eq_hash(self) -> None:
        """Validates BackoffContext semantics for __repr__, __eq__ and __hash__."""
        retry = Retry(
            count=0,
            start_time_nanos=123,
            attempt_start_time_nanos=123,
            policy=RetryPolicy(max_retries=1),
            config=RetryConfig(display_msg="a"),
            log=None,
            previous_outcomes=(),
        )
        err = RetryableError("boom")
        rng = random.Random(0)

        context_a = BackoffContext(retry, 123, rng, 5, err)
        context_b = context_a.copy(elapsed_nanos=6)
        context_c = BackoffContext(retry, 123, rng, 5, err)

        expected = (
            "BackoffContext(retry=Retry(count=0, start_time_nanos=123, attempt_start_time_nanos=123), "
            "curr_max_sleep_nanos=123, elapsed_nanos=5)"
        )
        self.assertEqual(expected, str(context_a))

        self.assertTrue(context_a is context_a)
        self.assertTrue(context_a == context_a)
        self.assertFalse(context_a == context_b)
        self.assertFalse(context_a == context_c)
        self.assertEqual(3, len({context_a, context_b, context_c}))
        self.assertNotEqual(context_a, object())
        self.assertFalse(context_a == (context_a.retry, context_a.curr_max_sleep_nanos, context_a.elapsed_nanos))
        self.assertEqual(object.__hash__(context_a), hash(context_a))


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
class TestRetryTemplateCopy(unittest.TestCase):

    def test_copy_returns_distinct_but_equal_template(self) -> None:
        """Ensures copy() returns a new RetryTemplate instance with identical field values when no overrides are given."""
        original: RetryTemplate = RetryTemplate()
        copied = original.copy()

        self.assertIsNot(original, copied)
        self.assertEqual(original, copied)

    def test_copy_overrides_selected_fields(self) -> None:
        """Ensures copy() correctly overrides selected fields while preserving others."""
        original_policy = RetryPolicy(max_retries=1)
        original_config = RetryConfig(display_msg="orig")

        def giveup(outcome: AttemptOutcome) -> object | None:
            return "circuit breaker triggered"

        def after_attempt(outcome: AttemptOutcome) -> None:
            _ = outcome.giveup_reason

        log = MagicMock(spec=Logger)
        original: RetryTemplate = RetryTemplate(
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
class TestRetryTemplateCall(unittest.TestCase):

    def test_default_fn_raises_not_implemented_error(self) -> None:
        template: RetryTemplate = RetryTemplate()
        with self.assertRaises(NotImplementedError) as ctx:
            template()
        self.assertEqual("Provide fn when calling RetryTemplate", str(ctx.exception))

    def test_retry_template_is_callable_and_runs(self) -> None:
        """Ensures RetryTemplate instances are callable and execute call_with_retries() with their own parameters."""
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

        template: RetryTemplate[str] = RetryTemplate(fn=fn, policy=retry_policy, after_attempt=after_attempt, log=None)

        self.assertTrue(callable(template))
        self.assertEqual("ok", template())
        self.assertEqual([0, 1], calls)
        self.assertEqual([False, True], after_attempts)


#############################################################################
class TestAttemptOutcomeCopy(unittest.TestCase):

    def test_copy_overrides_selected_fields(self) -> None:
        """Ensures copy() correctly overrides selected fields while preserving others."""
        original_log = MagicMock(spec=Logger)
        original_retry = Retry(
            count=1,
            start_time_nanos=100,
            attempt_start_time_nanos=100,
            policy=RetryPolicy(max_retries=1),
            config=RetryConfig(display_msg="orig"),
            log=original_log,
            previous_outcomes=(),
        )
        original = AttemptOutcome(
            retry=original_retry,
            is_success=False,
            is_exhausted=False,
            is_terminated=False,
            giveup_reason=None,
            elapsed_nanos=123,
            sleep_nanos=456,
            result=RetryableError("result"),
        )

        copied_retry = Retry(
            count=2,
            start_time_nanos=200,
            attempt_start_time_nanos=200,
            policy=RetryPolicy(max_retries=2),
            config=RetryConfig(display_msg="copy"),
            log=None,
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
        )

        self.assertIsNot(original, copied)
        self.assertIs(original_retry, original.retry)
        self.assertFalse(original.is_success)
        self.assertFalse(original.is_exhausted)
        self.assertFalse(original.is_terminated)
        self.assertIsNone(original.giveup_reason)
        self.assertEqual(123, original.elapsed_nanos)
        self.assertEqual(456, original.sleep_nanos)
        self.assertIs(original_log, original.retry.log)

        self.assertIs(copied_retry, copied.retry)
        self.assertTrue(copied.is_success)
        self.assertTrue(copied.is_exhausted)
        self.assertTrue(copied.is_terminated)
        self.assertEqual("done", copied.giveup_reason)
        self.assertEqual(999, copied.elapsed_nanos)
        self.assertEqual(0, copied.sleep_nanos)
        self.assertIsNone(copied.retry.log)

    def test_attempt_elapsed_nanos(self) -> None:
        """Computes per-attempt duration as attempt_end - attempt_start using nanosecond timestamps."""
        retry = Retry(
            count=0,
            start_time_nanos=100,
            attempt_start_time_nanos=150,
            policy=RetryPolicy(max_retries=0),
            config=RetryConfig(),
            log=None,
            previous_outcomes=(),
        )
        outcome = AttemptOutcome(
            retry=retry,
            is_success=True,
            is_exhausted=False,
            is_terminated=False,
            giveup_reason=None,
            elapsed_nanos=200,
            sleep_nanos=0,
            result=object(),
        )
        self.assertEqual(150, outcome.attempt_elapsed_nanos())


#############################################################################
class TestCallWithExceptionHandlers(unittest.TestCase):

    def test_call_with_exception_handlers_success_returns_value(self) -> None:
        self.assertEqual("ok", call_with_exception_handlers(lambda: "ok", handlers={}))

    def test_call_with_exception_handlers_no_match_reraises_original_exception(self) -> None:
        err = ValueError("x")

        def fn() -> None:
            raise err

        with self.assertRaises(ValueError) as cm:
            call_with_exception_handlers(fn, handlers={KeyError: [(True, raise_retryable_error_from)]})
        self.assertIs(err, cm.exception)

    def test_call_with_exception_handlers_uses_base_class_handler_if_needed(self) -> None:
        calls: list[str] = []

        def fn() -> str:
            raise KeyError("x")

        def handler(exc: BaseException) -> str:
            calls.append(type(exc).__name__)
            return "handled"

        self.assertEqual(
            "handled",
            call_with_exception_handlers(
                fn,
                handlers={
                    Exception: [(True, lambda exc: handler(exc))],
                    OSError: [(True, lambda exc: raise_retryable_error_from(exc, display_msg=f"OSError: {exc}"))],
                },
            ),
        )
        self.assertEqual(["KeyError"], calls)

    def test_call_with_exception_handlers_uses_most_specific_handler(self) -> None:
        calls: list[str] = []

        def fn() -> str:
            raise KeyError("x")

        def base_handler(_exc: BaseException) -> str:
            calls.append("base")
            return "base"

        def key_handler(_exc: BaseException) -> str:
            calls.append("key")
            return "key"

        self.assertEqual(
            "key",
            call_with_exception_handlers(
                fn,
                handlers={
                    Exception: [(True, base_handler)],
                    KeyError: [(True, key_handler)],
                },
            ),
        )
        self.assertEqual(["key"], calls)

    def test_call_with_exception_handlers_raise_retryable_error_from(self) -> None:
        err = KeyError("x")

        def fn() -> None:
            raise err

        with self.assertRaises(RetryableError) as cm:
            call_with_exception_handlers(fn, handlers={KeyError: [(True, raise_retryable_error_from)]})
        self.assertIs(err, cm.exception.__cause__)
        self.assertEqual((), cm.exception.args)
        self.assertEqual("KeyError", cm.exception.display_msg_str())

    def test_call_with_exception_handlers_propagates_handler_exception(self) -> None:
        err = KeyError("x")

        def fn() -> None:
            raise err

        def handler(exc: BaseException) -> None:
            self.assertIs(err, exc)
            raise RetryableError("boom") from exc

        with self.assertRaises(RetryableError) as cm:
            call_with_exception_handlers(fn, handlers={KeyError: [(True, handler)]})
        self.assertIs(err, cm.exception.__cause__)

    def test_call_with_exception_handlers_predicate_types_and_order(self) -> None:
        calls: list[str] = []
        err = OSError("network glitch")

        def fn() -> str:
            raise err

        def pred_false(_exc: BaseException) -> bool:
            calls.append("pred_false")
            return False

        def handler(_exc: BaseException) -> str:
            calls.append("handler")
            return "handled"

        def handler_disabled(_exc: BaseException) -> str:
            calls.append("handler_disabled")
            return "disabled"

        self.assertEqual(
            "handled",
            call_with_exception_handlers(
                fn,
                handlers={
                    OSError: [
                        (False, handler_disabled),
                        (pred_false, handler_disabled),
                        (True, handler),
                    ],
                },
            ),
        )
        self.assertEqual(["pred_false", "handler"], calls)

    def test_call_with_exception_handlers_callable_predicate_true_runs_handler(self) -> None:
        calls: list[str] = []
        err = OSError("network glitch")

        def fn() -> str:
            raise err

        def pred_true(_exc: BaseException) -> bool:
            calls.append("pred_true")
            return True

        def handler(_exc: BaseException) -> str:
            calls.append("handler")
            return "handled"

        def handler_never(_exc: BaseException) -> str:
            calls.append("handler_never")
            return "never"

        self.assertEqual(
            "handled",
            call_with_exception_handlers(
                fn,
                handlers={
                    OSError: [
                        (pred_true, handler),
                        (True, handler_never),
                    ],
                },
            ),
        )
        self.assertEqual(["pred_true", "handler"], calls)

    def test_call_with_exception_handlers_chain_present_but_no_predicate_matches_reraises(self) -> None:
        calls: list[str] = []
        err = KeyError("x")

        def fn() -> str:
            raise err

        def base_handler(_exc: BaseException) -> str:
            calls.append("base_handler")
            return "base"

        def pred_false(_exc: BaseException) -> bool:
            calls.append("pred_false")
            return False

        def never_called_handler(_exc: BaseException) -> str:
            calls.append("never_called_handler")
            return "nope"

        with self.assertRaises(KeyError) as cm:
            call_with_exception_handlers(
                fn,
                handlers={
                    KeyError: [
                        (False, never_called_handler),
                        (pred_false, never_called_handler),
                    ],
                    Exception: [(True, base_handler)],
                },
            )
        self.assertIs(err, cm.exception)
        self.assertEqual(["pred_false"], calls)

    def test_call_with_exception_handlers_empty_chain_reraises_and_shadows_base_chain(self) -> None:
        calls: list[str] = []
        err = KeyError("x")

        def fn() -> str:
            raise err

        def base_handler(_exc: BaseException) -> str:
            calls.append("base_handler")
            return "base"

        with self.assertRaises(KeyError) as cm:
            call_with_exception_handlers(
                fn,
                handlers={
                    KeyError: [],
                    Exception: [(True, base_handler)],
                },
            )
        self.assertIs(err, cm.exception)
        self.assertEqual([], calls)

    def test_call_with_exception_handlers_fall_through_to_base_class_chain_if_enabled(self) -> None:
        calls: list[str] = []
        err = KeyError("x")

        def fn() -> str:
            raise err

        def pred_false(_exc: BaseException) -> bool:
            calls.append("pred_false")
            return False

        def base_handler(_exc: BaseException) -> str:
            calls.append("base_handler")
            return "base"

        self.assertEqual(
            "base",
            call_with_exception_handlers(
                fn,
                continue_if_no_predicate_matches=True,
                handlers={
                    KeyError: [
                        (False, base_handler),
                        (pred_false, base_handler),
                    ],
                    Exception: [(True, base_handler)],
                },
            ),
        )
        self.assertEqual(["pred_false", "base_handler"], calls)

        calls = []
        self.assertEqual(
            "base",
            call_with_exception_handlers(
                fn,
                continue_if_no_predicate_matches=True,
                handlers={
                    KeyError: [],
                    Exception: [(True, base_handler)],
                },
            ),
        )
        self.assertEqual(["base_handler"], calls)
