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
"""Unit tests for retry helper functions; Exercise exponential backoff, logging and error propagation."""

from __future__ import (
    annotations,
)
import argparse
import logging
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
    Retry,
    RetryableError,
    RetryConfig,
    RetryOptions,
    RetryPolicy,
    run_with_retries,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestRunWithRetries,
        TestRetryConfigCopy,
        TestRetryOptionsCopy,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestRunWithRetries(unittest.TestCase):

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
            "max_sleep_secs=4, max_elapsed_secs=5, exponential_base=6)"
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

    def test_run_with_retries_success(self) -> None:
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
                raise RetryableError("fail", display_msg="connect", no_sleep=(retry.count == 0)) from ValueError("boom")
            return "ok"

        mock_log = MagicMock(spec=Logger)
        self.assertEqual(
            "ok", run_with_retries(fn, policy=retry_policy, config=RetryConfig(display_msg="foo"), log=mock_log)
        )
        self.assertEqual([0, 1, 2], calls)
        self.assertEqual(2, len(mock_log.log.call_args_list))

        calls.clear()
        mock_log = MagicMock(spec=Logger)
        mock_log.isEnabledFor = lambda level: level >= logging.ERROR
        self.assertEqual(
            "ok", run_with_retries(fn, policy=retry_policy, config=RetryConfig(display_msg="foo"), log=mock_log)
        )
        self.assertEqual([0, 1, 2], calls)
        self.assertEqual(0, len(mock_log.log.call_args_list))

    def test_run_with_retries_log_none(self) -> None:
        """Ensures run_with_retries works when log is None."""
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
                raise RetryableError("fail", display_msg="connect", no_sleep=True) from ValueError("boom")
            return "ok"

        # log=None should skip all logging but still perform retries and return successfully
        self.assertEqual("ok", run_with_retries(fn, policy=retry_policy, config=RetryConfig(display_msg="foo"), log=None))
        self.assertEqual([0, 1], calls)

    def test_run_with_retries_gives_up(self) -> None:
        """Ensures retries are attempted until max retries are exhausted when giveup() is not used."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )

        def fn(retry: Retry) -> None:
            raise RetryableError("fail", no_sleep=True) from ValueError("boom")

        with self.assertRaises(ValueError):
            run_with_retries(fn, policy=retry_policy, config=RetryConfig(), log=MagicMock(spec=Logger))

    def test_run_with_retries_retryable_without_cause_raises_retryable(self) -> None:
        """Ensures that if RetryableError has no __cause__, exhaustion re-raises RetryableError rather than failing
        internally."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )

        def fn(retry: Retry) -> None:
            # Intentionally do not use 'from exc' so __cause__ stays None.
            raise RetryableError("fail", no_sleep=True)

        with self.assertRaises(RetryableError) as cm:
            run_with_retries(fn, policy=retry_policy, config=RetryConfig(), log=None)
        self.assertEqual("fail", str(cm.exception))

    def test_run_with_retries_giveup_stops_retries(self) -> None:
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

        def giveup(retryable_error: RetryableError) -> bool:
            self.assertIsInstance(retryable_error, RetryableError)
            return True

        with self.assertRaises(ValueError):
            run_with_retries(fn, policy=retry_policy, config=RetryConfig(), giveup=giveup, log=mock_log)

        # giveup() must prevent additional retries
        self.assertEqual([0], calls)
        mock_log.log.assert_called_once()
        log_call_args = mock_log.log.call_args[0]
        self.assertEqual(logging.WARNING, log_call_args[0])

    def test_run_with_retries_no_retries(self) -> None:
        """Ensures no retries or warning logs are emitted when retries are disabled."""
        retry_policy = RetryPolicy.no_retries()
        mock_log = MagicMock(spec=Logger)

        def fn(retry: Retry) -> None:
            raise RetryableError("fail") from ValueError("boom")

        with self.assertRaises(ValueError):
            run_with_retries(fn, policy=retry_policy, config=RetryConfig(), log=mock_log)
        mock_log.log.assert_not_called()

    def test_run_with_retries_elapsed_time(self) -> None:
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
                raise RetryableError("fail", no_sleep=True) from ValueError("boom")

            with self.assertRaises(ValueError):
                run_with_retries(
                    fn, policy=retry_policy, config=RetryConfig(termination_event=threading.Event()), log=mock_log
                )
        mock_log.log.assert_called_once()
        warning_call_args = mock_log.log.call_args[0]
        self.assertEqual(logging.WARNING, warning_call_args[0])

    def test_run_with_retries_empty_display_msg_uses_default(self) -> None:
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
            raise RetryableError("fail", no_sleep=True) from ValueError("boom")

        with self.assertRaises(ValueError):
            run_with_retries(fn, policy=retry_policy, config=RetryConfig(display_msg=""), log=mock_log)

        self.assertEqual(mock_log.log.call_count, 2)
        warning_call_args = mock_log.log.call_args_list[-1][0]
        self.assertEqual(logging.WARNING, warning_call_args[0])
        warning_msg = warning_call_args[2]
        self.assertTrue(warning_msg.startswith("Retrying exhausted; giving up because the last [1/1] retries"))

    def test_run_with_retries_custom_log_levels(self) -> None:
        """Ensures run_with_retries uses provided info and warning log levels."""
        retry_policy = RetryPolicy(
            max_retries=2,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        mock_log = MagicMock(spec=Logger)

        def fn(retry: Retry) -> None:
            raise RetryableError("fail", no_sleep=(retry.count == 0)) from ValueError("boom")

        with self.assertRaises(ValueError):
            run_with_retries(
                fn,
                policy=retry_policy,
                config=RetryConfig(info_loglevel=logging.DEBUG, warning_loglevel=logging.ERROR),
                log=mock_log,
            )

        info_call_args = mock_log.log.call_args_list[0][0]
        warning_call_args = mock_log.log.call_args_list[-1][0]
        self.assertEqual(logging.DEBUG, info_call_args[0])
        self.assertEqual("Retrying [1/2] immediately ...", info_call_args[2])
        info_call_args = mock_log.log.call_args_list[1][0]
        self.assertEqual("Retrying [2/2] in 1ns ...", info_call_args[2])
        self.assertEqual(logging.ERROR, warning_call_args[0])
        self.assertTrue(warning_call_args[2].startswith("Retrying exhausted; giving up because the last [2/2] retries"))

    def test_run_with_retries_after_attempt_success(self) -> None:
        """Ensures after_attempt is invoked for both retries and final success with correct flags."""
        retry_policy = RetryPolicy(
            max_retries=3,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=10,
        )
        events: list[tuple[int, bool, bool, bool, int, object]] = []

        def fn(retry: Retry) -> str:
            # Fail twice, then succeed.
            if retry.count < 2:
                raise RetryableError("fail", no_sleep=True) from ValueError("boom")
            return "ok"

        def after_attempt(
            retry: Retry,
            is_success: bool,
            is_exhausted: bool,
            is_terminated: bool,
            duration_nanos: int,
            result: object,
            log: Logger | None,
        ) -> None:
            events.append((retry.count, is_success, is_exhausted, is_terminated, duration_nanos, result))
            self.assertFalse(is_terminated)
            self.assertIsNone(log)

        final_result = run_with_retries(fn, policy=retry_policy, config=RetryConfig(), after_attempt=after_attempt, log=None)
        self.assertEqual("ok", final_result)

        # We expect two failed attempts (counts 0 and 1) and one success (count 2).
        self.assertEqual(3, len(events))

        # All intermediate events must be failures (is_success=False, is_exhausted=False, error present).
        for idx, (retry_count, is_success, is_exhausted, _is_terminated, duration_nanos, event_result) in enumerate(
            events[:-1]
        ):
            self.assertEqual(idx, retry_count)
            self.assertFalse(is_success)
            self.assertFalse(is_exhausted)
            self.assertIsInstance(event_result, RetryableError)
            self.assertGreaterEqual(duration_nanos, 0)

        # Last event must be the success (is_success=True, error is None) for attempt index 2.
        last_retry_count, is_success, is_exhausted, _is_terminated, duration_nanos, event_result = events[-1]
        self.assertEqual(2, last_retry_count)
        self.assertTrue(is_success)
        self.assertFalse(is_exhausted)
        self.assertEqual("ok", event_result)
        self.assertGreaterEqual(duration_nanos, 0)

    def test_run_with_retries_after_attempt_exhausted(self) -> None:
        """Ensures after_attempt is invoked with is_exhausted when retries are exhausted."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        events: list[tuple[int, bool, bool, bool, int, object]] = []

        def fn(retry: Retry) -> None:
            raise RetryableError("fail", no_sleep=True) from ValueError("boom")

        def after_attempt(
            retry: Retry,
            is_success: bool,
            is_exhausted: bool,
            is_terminated: bool,
            duration_nanos: int,
            result: object,
            log: Logger | None,
        ) -> None:
            events.append((retry.count, is_success, is_exhausted, is_terminated, duration_nanos, result))
            self.assertIsNone(log)

        with self.assertRaises(ValueError):
            run_with_retries(fn, policy=retry_policy, config=RetryConfig(), after_attempt=after_attempt, log=None)

        # There must be at least one event and the last one must indicate exhaustion.
        self.assertGreaterEqual(len(events), 1)
        last_retry_count, is_success, is_exhausted, is_terminated, duration_nanos, result = events[-1]
        self.assertFalse(is_success)
        self.assertTrue(is_exhausted)
        self.assertFalse(is_terminated)
        self.assertIsInstance(result, RetryableError)
        self.assertGreaterEqual(last_retry_count, 0)
        self.assertGreaterEqual(duration_nanos, 0)


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


#############################################################################
class TestRetryOptionsCopy(unittest.TestCase):

    def test_copy_returns_distinct_but_equal_options(self) -> None:
        """Ensures copy() returns a new RetryOptions instance with identical field values when no overrides are given."""
        original = RetryOptions()
        copied = original.copy()

        self.assertIsNot(original, copied)
        self.assertEqual(original, copied)

    def test_copy_overrides_selected_fields(self) -> None:
        """Ensures copy() correctly overrides selected fields while preserving others."""
        original_policy = RetryPolicy(max_retries=1)
        original_config = RetryConfig(display_msg="orig")

        def giveup(retryable_error: RetryableError) -> bool:
            return True

        def after_attempt(
            retry: Retry,
            is_success: bool,
            is_exhausted: bool,
            is_terminated: bool,
            duration_nanos: int,
            result: object,
            log: Logger | None,
        ) -> None:
            return None

        log = MagicMock(spec=Logger)
        original = RetryOptions(
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
