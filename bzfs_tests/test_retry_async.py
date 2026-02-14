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
"""Unit tests for call_with_retries_async()."""

from __future__ import (
    annotations,
)
import asyncio
import functools
import logging
import pickle
import unittest
from collections.abc import (
    Awaitable,
)
from contextlib import (
    suppress,
)
from typing import (
    Callable,
    TypeVar,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    patch,
)

from bzfs_main.util.retry import (
    NO_LOGGER,
    AttemptOutcome,
    Retry,
    RetryableError,
    RetryConfig,
    RetryError,
    RetryPolicy,
    RetryTemplate,
    RetryTiming,
    before_attempt_noop,
    call_with_retries_async,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestCallWithRetriesAsync,
        TestRetryTemplateWrapsAsync,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
_T = TypeVar("_T")


async def asyncio_await_with_timeout(awaitable: Awaitable[_T], timeout_nanos: int, *, display_msg: object = "timeout") -> _T:
    """Convenience function that awaits an awaitable with a hard timeout; on timeout raises RetryableError.

    Assumes awaitable handles cancellation (CancelledError) correctly.
    """
    import asyncio

    if timeout_nanos < 0:
        raise ValueError(f"Invalid timeout_nanos: must be >= 0 but got {timeout_nanos}")
    if timeout_nanos == 0:
        raise RetryableError(display_msg=display_msg) from TimeoutError("Async operation timed out")

    task: asyncio.Future[_T] = asyncio.ensure_future(awaitable)
    try:
        done, pending = await asyncio.wait({task}, timeout=timeout_nanos / 1_000_000_000)
        if task in done:
            return task.result()
        assert task in pending
        task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await task
        raise RetryableError(display_msg=display_msg) from TimeoutError(
            f"Async operation timed out after {timeout_nanos / 1_000_000_000}s"
        )
    except BaseException:
        if not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task
        raise


#############################################################################
class TestCallWithRetriesAsync(unittest.IsolatedAsyncioTestCase):
    """Unit tests for call_with_retries_async()."""

    async def test_sleep_async_delegates_to_asyncio_sleep(self) -> None:

        sleep_nanos = 456_000_000
        expected_secs = sleep_nanos / 1_000_000_000
        timing = RetryTiming()
        retry = Retry(
            count=0,
            call_start_time_nanos=0,
            before_attempt_start_time_nanos=0,
            attempt_start_time_nanos=0,
            idle_nanos=0,
            policy=RetryPolicy.no_retries().copy(timing=timing),
            log=None,
            previous_outcomes=(),
        )
        with patch("asyncio.sleep", new=AsyncMock()) as mock_sleep:
            await timing.sleep_async(sleep_nanos, retry)
        mock_sleep.assert_awaited_once_with(expected_secs)

    async def test_call_with_retries_async_success_retries_and_sleeps(self) -> None:
        retry_policy = RetryPolicy(
            max_retries=2,
            min_sleep_secs=0.001,
            initial_max_sleep_secs=0.001,
            max_sleep_secs=0.001,
            max_elapsed_secs=1,
        )
        expected_sleep_nanos: int = 1_000_000
        calls: list[int] = []
        events: list[AttemptOutcome] = []
        sleep_calls: list[tuple[int, int]] = []

        async def fn(retry: Retry) -> str:
            calls.append(retry.count)
            if retry.count < 2:
                raise RetryableError("fail", display_msg="connect") from ValueError("boom")
            return "ok"

        def after_attempt(outcome: AttemptOutcome) -> None:
            events.append(outcome)

        async def fake_sleep_async(sleep_nanos: int, retry: Retry) -> None:
            sleep_calls.append((sleep_nanos, retry.count))

        mock_sleep_async = AsyncMock(side_effect=fake_sleep_async)
        retry_policy = retry_policy.copy(timing=RetryTiming().copy(sleep_async=mock_sleep_async))
        actual = await call_with_retries_async(
            fn,
            policy=retry_policy,
            after_attempt=after_attempt,
            log=None,
        )

        self.assertEqual("ok", actual)
        self.assertEqual([0, 1, 2], calls)
        self.assertEqual([(expected_sleep_nanos, 0), (expected_sleep_nanos, 1)], sleep_calls)
        self.assertEqual(2, mock_sleep_async.await_count)

        self.assertEqual(3, len(events))
        self.assertFalse(events[0].is_success)
        self.assertFalse(events[0].is_exhausted)
        self.assertEqual(0, events[0].retry.count)
        self.assertEqual(expected_sleep_nanos, events[0].sleep_nanos)

        self.assertFalse(events[1].is_success)
        self.assertFalse(events[1].is_exhausted)
        self.assertEqual(1, events[1].retry.count)
        self.assertEqual(expected_sleep_nanos, events[1].sleep_nanos)

        self.assertTrue(events[2].is_success)
        self.assertEqual(2, events[2].retry.count)
        self.assertEqual(0, events[2].sleep_nanos)

    async def test_call_with_retries_async_retry_immediately_once_skips_backoff_and_sleeps_zero(self) -> None:
        backoff_strategy = MagicMock(side_effect=AssertionError("backoff_strategy must not be called"))
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0.001,
            initial_max_sleep_secs=0.001,
            max_sleep_secs=0.001,
            max_elapsed_secs=10,
        )
        calls: list[int] = []
        events: list[AttemptOutcome] = []
        sleep_calls: list[tuple[int, int]] = []

        async def fn(retry: Retry) -> str:
            calls.append(retry.count)
            if retry.count == 0:
                raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")
            return "ok"

        def after_attempt(outcome: AttemptOutcome) -> None:
            events.append(outcome)

        async def fake_sleep_async(sleep_nanos: int, retry: Retry) -> None:
            sleep_calls.append((sleep_nanos, retry.count))

        mock_sleep_async = AsyncMock(side_effect=fake_sleep_async)
        retry_policy = retry_policy.copy(timing=RetryTiming().copy(sleep_async=mock_sleep_async))
        actual = await call_with_retries_async(
            fn,
            policy=retry_policy,
            backoff=backoff_strategy,
            after_attempt=after_attempt,
            log=None,
        )

        self.assertEqual("ok", actual)
        self.assertEqual([0, 1], calls)
        self.assertEqual([(0, 0)], sleep_calls)
        self.assertEqual(2, len(events))
        self.assertFalse(events[0].is_success)
        self.assertFalse(events[0].is_exhausted)
        self.assertEqual(0, events[0].sleep_nanos)
        self.assertTrue(events[1].is_success)
        self.assertEqual(0, events[1].sleep_nanos)
        self.assertEqual(1, mock_sleep_async.await_count)
        backoff_strategy.assert_not_called()

    async def test_call_with_retries_async_after_attempt_retryable_error_on_success_retries(self) -> None:
        """Ensures raising RetryableError from after_attempt() on a successful attempt triggers a retry."""
        retry_policy = RetryPolicy(max_retries=3, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0)
        fn_calls: list[int] = []
        after_attempt_events: list[tuple[bool, bool, int]] = []
        raised_once: bool = False

        async def fn(retry: Retry) -> str:
            fn_calls.append(retry.count)
            return "ok"

        def after_attempt(outcome: AttemptOutcome) -> None:
            nonlocal raised_once
            after_attempt_events.append((outcome.is_success, outcome.is_exhausted, outcome.retry.count))
            if outcome.is_success and not raised_once:
                raised_once = True
                raise RetryableError("retry from after_attempt")

        actual = await call_with_retries_async(fn, policy=retry_policy, after_attempt=after_attempt, log=None)
        self.assertTrue(raised_once)
        self.assertEqual("ok", actual)
        self.assertEqual([0, 1], fn_calls)
        self.assertEqual([(True, False, 0), (False, False, 0), (True, False, 1)], after_attempt_events)

    async def test_call_with_retries_async_after_attempt_retryable_error_on_failure_aborts(self) -> None:
        """Ensures raising RetryableError from after_attempt() on failure aborts without additional retries."""
        retry_policy = RetryPolicy(max_retries=3, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0)
        fn_calls: list[int] = []
        after_attempt_calls: list[int] = []

        async def fn(retry: Retry) -> None:
            fn_calls.append(retry.count)
            raise RetryableError("attempt failed")

        def after_attempt(outcome: AttemptOutcome) -> None:
            after_attempt_calls.append(outcome.retry.count)
            if not outcome.is_success and not outcome.is_exhausted:
                raise RetryableError("after_attempt abort")

        mock_sleep_async = AsyncMock()
        retry_policy = retry_policy.copy(timing=RetryTiming().copy(sleep_async=mock_sleep_async))
        with self.assertRaises(RetryableError):
            await call_with_retries_async(fn, policy=retry_policy, after_attempt=after_attempt, log=None)

        self.assertEqual([0], fn_calls)
        self.assertEqual([0], after_attempt_calls)
        mock_sleep_async.assert_not_awaited()

    async def test_call_with_retries_async_giveup_stops_retries(self) -> None:
        """Ensures giveup() stops retries immediately and custom after_attempt disables default logging."""
        retry_policy = RetryPolicy(
            max_retries=5,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        mock_log = MagicMock(spec=logging.Logger)
        calls: list[int] = []

        async def fn(retry: Retry) -> None:
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

        monotonic_ns = MagicMock(side_effect=[0, 1234])
        retry_policy = retry_policy.copy(timing=RetryTiming(monotonic_ns=monotonic_ns))
        with self.assertRaises(ValueError):
            await call_with_retries_async(
                fn,
                policy=retry_policy,
                giveup=giveup,
                after_attempt=after_attempt,
                log=mock_log,
            )

        # giveup() must prevent additional retries
        self.assertEqual([0], calls)
        mock_log.log.assert_not_called()
        self.assertEqual(1, len(after_attempt_events))
        self.assertFalse(after_attempt_events[0].is_success)
        self.assertTrue(after_attempt_events[0].is_exhausted)
        self.assertFalse(after_attempt_events[0].is_terminated)
        self.assertEqual("circuit breaker triggered", after_attempt_events[0].giveup_reason)

    async def test_call_with_retries_async_before_attempt_can_delay_without_consuming_retry_count(self) -> None:
        """Ensures before_attempt can delay an attempt without incrementing retry.count."""
        retry_policy = RetryPolicy(
            max_retries=0,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=10,
        )
        before_attempt_calls: list[int] = []
        sleeps: list[tuple[int, int]] = []
        fn_calls: list[int] = []

        def before_attempt(retry: Retry) -> int:
            before_attempt_calls.append(retry.count)
            return 123

        async def sleep_async(sleep_nanos: int, retry: Retry) -> None:
            sleeps.append((sleep_nanos, retry.count))

        async def fn(retry: Retry) -> str:
            fn_calls.append(retry.count)
            return "ok"

        retry_policy = retry_policy.copy(timing=RetryTiming().copy(sleep_async=sleep_async))
        self.assertEqual(
            "ok",
            await call_with_retries_async(
                fn,
                policy=retry_policy,
                before_attempt=before_attempt,
                log=None,
            ),
        )
        self.assertEqual([0], before_attempt_calls)
        self.assertEqual([0], fn_calls)
        self.assertEqual([(123, 0)], sleeps)

    async def test_call_with_retries_async_before_attempt_is_invoked_for_each_attempt(self) -> None:
        """Ensures before_attempt runs before each fn(retry) invocation, including retries."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=10,
        )
        before_attempt_calls: list[int] = []
        fn_calls: list[int] = []

        def before_attempt(retry: Retry) -> int:
            before_attempt_calls.append(retry.count)
            return 0 if retry.count == 0 else 7

        sleeps: list[int] = []

        async def sleep_async(sleep_nanos: int, _retry: Retry) -> None:
            if sleep_nanos != 0:
                sleeps.append(sleep_nanos)

        async def fn(retry: Retry) -> str:
            fn_calls.append(retry.count)
            if retry.count == 0:
                raise RetryableError("fail", retry_immediately_once=False)
            return "ok"

        retry_policy = retry_policy.copy(timing=RetryTiming().copy(sleep_async=sleep_async))
        self.assertEqual(
            "ok",
            await call_with_retries_async(
                fn,
                policy=retry_policy,
                backoff=lambda ctx: (0, ctx.curr_max_sleep_nanos),
                before_attempt=before_attempt,
                log=None,
            ),
        )
        self.assertEqual([0, 1], before_attempt_calls)
        self.assertEqual([0, 1], fn_calls)
        self.assertEqual([7], sleeps)

    async def test_call_with_retries_async_before_attempt_wrapper_around_noop_reaches_before_attempt_path(self) -> None:
        """Ensures wrapping before_attempt_noop still executes the before_attempt callback path."""
        retry_policy = RetryPolicy.no_retries()
        wrapper_calls: list[int] = []
        sleeps: list[int] = []

        def before_attempt_wrapper(retry: Retry) -> int:
            wrapper_calls.append(retry.count)
            return before_attempt_noop(retry)

        async def sleep_async(sleep_nanos: int, _retry: Retry) -> None:
            sleeps.append(sleep_nanos)

        monotonic_ns = MagicMock(side_effect=[1_000, 1_234])
        retry_policy = retry_policy.copy(timing=RetryTiming(monotonic_ns=monotonic_ns).copy(sleep_async=sleep_async))

        async def fn(retry: Retry) -> str:
            self.assertEqual(1_000, retry.call_start_time_nanos)
            self.assertEqual(1_000, retry.before_attempt_start_time_nanos)
            self.assertEqual(1_234, retry.attempt_start_time_nanos)
            self.assertEqual(234, retry.before_attempt_sleep_nanos())
            self.assertEqual(234, retry.idle_nanos)
            return "ok"

        self.assertEqual(
            "ok",
            await call_with_retries_async(
                fn,
                policy=retry_policy,
                before_attempt=before_attempt_wrapper,
                log=None,
            ),
        )
        self.assertEqual([0], wrapper_calls)
        self.assertEqual([], sleeps)
        self.assertEqual(2, monotonic_ns.call_count)

    async def test_call_with_retries_async_before_attempt_negative_sleep_is_an_error(self) -> None:
        """Ensures before_attempt returning a negative duration raises an error."""
        retry_policy = RetryPolicy.no_retries()
        sleeps: list[int] = []

        def before_attempt(_retry: Retry) -> int:
            return -1

        async def sleep_async(sleep_nanos: int, _retry: Retry) -> None:
            sleeps.append(sleep_nanos)

        retry_policy = retry_policy.copy(timing=RetryTiming().copy(sleep_async=sleep_async))
        with self.assertRaises(AssertionError):
            await call_with_retries_async(
                lambda _retry: asyncio.sleep(0, result="ok"),
                policy=retry_policy,
                before_attempt=before_attempt,
                log=None,
            )
        self.assertEqual([], sleeps)

    async def test_retry_async_before_attempt_time_nanos_matches_attempt_start_without_sleep(self) -> None:
        """Ensures Retry captures before_attempt_time_nanos when before_attempt returns 0."""
        retry_policy = RetryPolicy.no_retries()

        def before_attempt(_retry: Retry) -> int:
            return 0

        monotonic_ns = MagicMock(side_effect=[1_000, 1_000])
        retry_policy = retry_policy.copy(timing=RetryTiming(monotonic_ns=monotonic_ns).copy(sleep_async=AsyncMock()))

        async def fn(retry: Retry) -> int:
            self.assertEqual(1_000, retry.call_start_time_nanos)
            self.assertEqual(1_000, retry.before_attempt_start_time_nanos)
            self.assertEqual(1_000, retry.attempt_start_time_nanos)
            self.assertEqual(0, retry.before_attempt_sleep_nanos())
            self.assertEqual(0, retry.idle_nanos)
            return 123

        self.assertEqual(
            123,
            await call_with_retries_async(
                fn,
                policy=retry_policy,
                before_attempt=before_attempt,
                log=None,
            ),
        )
        self.assertEqual(2, monotonic_ns.call_count)

    async def test_retry_async_before_attempt_sleep_nanos_reflects_time_elapsed_until_fn(self) -> None:
        """Ensures before_attempt_sleep_nanos is attempt_start_time - before_attempt_time."""
        retry_policy = RetryPolicy.no_retries()

        def before_attempt(_retry: Retry) -> int:
            return 123

        monotonic_ns = MagicMock(side_effect=[1_000, 2_000])
        retry_policy = retry_policy.copy(timing=RetryTiming(monotonic_ns=monotonic_ns).copy(sleep_async=AsyncMock()))

        async def fn(retry: Retry) -> int:
            self.assertEqual(1_000, retry.call_start_time_nanos)
            self.assertEqual(1_000, retry.before_attempt_start_time_nanos)
            self.assertEqual(2_000, retry.attempt_start_time_nanos)
            self.assertEqual(1_000, retry.before_attempt_sleep_nanos())
            self.assertEqual(1_000, retry.idle_nanos)
            return 456

        self.assertEqual(
            456,
            await call_with_retries_async(
                fn,
                policy=retry_policy,
                before_attempt=before_attempt,
                log=None,
            ),
        )

    async def test_retry_async_idle_nanos_includes_retry_sleep_nanos(self) -> None:
        """Ensures Retry.idle_nanos includes retry-loop sleep_nanos on subsequent async attempts."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=10,
        )
        sleeps: list[tuple[int, int]] = []
        retry_idle_nanos: list[tuple[int, int]] = []

        async def sleep_async(sleep_nanos: int, retry: Retry) -> None:
            sleeps.append((sleep_nanos, retry.count))

        retry_policy = retry_policy.copy(timing=RetryTiming(monotonic_ns=lambda: 0).copy(sleep_async=sleep_async))

        async def fn(retry: Retry) -> str:
            retry_idle_nanos.append((retry.count, retry.idle_nanos))
            if retry.count == 0:
                raise RetryableError("fail") from ValueError("boom")
            return "ok"

        self.assertEqual(
            "ok",
            await call_with_retries_async(
                fn,
                policy=retry_policy,
                backoff=lambda ctx: (11, ctx.curr_max_sleep_nanos),
                log=None,
            ),
        )
        self.assertEqual([(11, 0)], sleeps)
        self.assertEqual([(0, 0), (1, 11)], retry_idle_nanos)

    async def test_call_with_retries_async_max_previous_outcomes_1(self) -> None:
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

        async def sleep_async(sleep_nanos: int, _retry: Retry) -> None:
            self.assertEqual(0, sleep_nanos)

        retry_policy = retry_policy.copy(timing=RetryTiming().copy(sleep_async=sleep_async))

        async def fn(retry: Retry) -> str:
            history_counts_per_attempt.append([outcome.retry.count for outcome in retry.previous_outcomes])
            nested_history_sizes_per_attempt.append(
                [len(outcome.retry.previous_outcomes) for outcome in retry.previous_outcomes]
            )
            for outcome in retry.previous_outcomes:
                self.assertEqual((), outcome.retry.previous_outcomes)
            if retry.count < 2:
                raise RetryableError("fail", retry_immediately_once=(retry.count == 0)) from ValueError("boom")
            return "ok"

        self.assertEqual("ok", await call_with_retries_async(fn, policy=retry_policy, log=None))
        self.assertEqual([[], [0], [1]], history_counts_per_attempt)
        self.assertEqual([[], [0], [0]], nested_history_sizes_per_attempt)

    async def test_call_with_retries_async_max_previous_outcomes_2(self) -> None:
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

        async def sleep_async(sleep_nanos: int, _retry: Retry) -> None:
            self.assertEqual(0, sleep_nanos)

        retry_policy = retry_policy.copy(timing=RetryTiming().copy(sleep_async=sleep_async))

        async def fn(retry: Retry) -> str:
            history_counts_per_attempt.append([outcome.retry.count for outcome in retry.previous_outcomes])
            if retry.count < 3:
                raise RetryableError("fail", retry_immediately_once=(retry.count == 0)) from ValueError("boom")
            return "ok"

        self.assertEqual("ok", await call_with_retries_async(fn, policy=retry_policy, log=None))
        self.assertEqual([[], [0], [0, 1], [1, 2]], history_counts_per_attempt)

    async def test_call_with_retries_async_previous_outcomes_are_detached(self) -> None:
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

        async def sleep_async(sleep_nanos: int, _retry: Retry) -> None:
            self.assertEqual(0, sleep_nanos)

        retry_policy = retry_policy.copy(timing=RetryTiming().copy(sleep_async=sleep_async))

        async def fn(retry: Retry) -> str:
            nested_history_sizes_per_attempt.append(
                [len(outcome.retry.previous_outcomes) for outcome in retry.previous_outcomes]
            )
            for outcome in retry.previous_outcomes:
                self.assertEqual((), outcome.retry.previous_outcomes)
            if retry.count < 4:
                raise RetryableError("fail", retry_immediately_once=(retry.count == 0)) from ValueError("boom")
            return "ok"

        self.assertEqual("ok", await call_with_retries_async(fn, policy=retry_policy, log=None))
        self.assertEqual([[], [0], [0, 0], [0, 0], [0, 0]], nested_history_sizes_per_attempt)

    async def test_call_with_retries_async_after_attempt_success(self) -> None:
        """Ensures after_attempt is invoked for both retries and final success with correct flags."""
        retry_policy = RetryPolicy(
            max_retries=3,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=10,
        )
        events: list[AttemptOutcome] = []

        async def fn(retry: Retry) -> str:
            # Fail twice, then succeed.
            if retry.count < 2:
                raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")
            return "ok"

        def after_attempt(outcome: AttemptOutcome) -> None:
            events.append(outcome)
            self.assertFalse(outcome.is_terminated)
            self.assertIsNone(outcome.giveup_reason)
            self.assertIsNone(outcome.retry.log)

        final_result = await call_with_retries_async(
            fn,
            policy=retry_policy,
            after_attempt=after_attempt,
            log=None,
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

    async def test_call_with_retries_async_after_attempt_exhausted(self) -> None:
        """Ensures after_attempt is invoked with is_exhausted when retries are exhausted."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        events: list[AttemptOutcome] = []

        async def fn(_retry: Retry) -> None:
            raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")

        def after_attempt(outcome: AttemptOutcome) -> None:
            events.append(outcome)
            self.assertIsNone(outcome.giveup_reason)
            self.assertIsNone(outcome.retry.log)

        with self.assertRaises(ValueError):
            await call_with_retries_async(fn, policy=retry_policy, after_attempt=after_attempt, log=None)

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

    async def test_call_with_retries_async_can_terminate_after_after_attempt_before_continue(self) -> None:
        """Ensures termination between after_attempt() and the next-attempt continue stops retries."""
        retry_policy = RetryPolicy(
            max_retries=3,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=10,
        )
        calls: list[int] = []
        events: list[AttemptOutcome] = []
        terminated: bool = False

        def is_terminated(_retry: Retry) -> bool:
            return terminated

        retry_policy = retry_policy.copy(timing=RetryTiming(is_terminated=is_terminated))

        async def fn(retry: Retry) -> str:
            calls.append(retry.count)
            raise RetryableError("fail") from ValueError("boom")

        def after_attempt(outcome: AttemptOutcome) -> None:
            nonlocal terminated
            events.append(outcome)
            if not outcome.is_exhausted:
                terminated = True

        actual = await call_with_retries_async(
            fn,
            policy=retry_policy,
            backoff=lambda ctx: (0, ctx.curr_max_sleep_nanos),
            after_attempt=after_attempt,
            on_exhaustion=lambda _outcome: "exhausted",
            log=None,
        )

        self.assertEqual("exhausted", actual)
        self.assertEqual([0], calls)
        self.assertEqual(2, len(events))
        self.assertFalse(events[0].is_exhausted)
        self.assertFalse(events[0].is_terminated)
        self.assertTrue(events[1].is_exhausted)
        self.assertTrue(events[1].is_terminated)
        self.assertEqual(0, events[1].retry.count)

    async def test_call_with_retries_async_on_exhaustion_reraises_cause(self) -> None:
        async def fn(_retry: Retry) -> None:
            raise RetryableError("fail") from ValueError("boom")

        with self.assertRaises(ValueError):
            await call_with_retries_async(fn, policy=RetryPolicy.no_retries(), log=None)

    async def test_call_with_retries_async_on_retryable_error_called_once_per_failure(self) -> None:
        """Ensures on_retryable_error runs once for each RetryableError raised by fn()."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=10,
            reraise=False,
        )
        seen_counts: list[int] = []
        seen_exhausted_flags: list[bool] = []

        def on_retryable_error(outcome: AttemptOutcome) -> None:
            seen_counts.append(outcome.retry.count)
            seen_exhausted_flags.append(outcome.is_exhausted)

        async def fn(_retry: Retry) -> str:
            raise RetryableError("fail") from ValueError("boom")

        with self.assertRaises(RetryError):
            await call_with_retries_async(
                fn,
                policy=retry_policy,
                on_retryable_error=on_retryable_error,
                log=None,
            )

        self.assertEqual([0, 1], seen_counts)
        self.assertEqual([False, False], seen_exhausted_flags)

    async def test_call_with_retries_async_does_not_swallow_cancelled_error(self) -> None:
        """Ensures asyncio.CancelledError propagates immediately and is not treated as a retryable failure.

        In async systems cancellation is used for timeouts and shutdown; swallowing it can hang tasks, block teardown, and
        cause retry loops to ignore cooperative cancellation.
        """
        calls: list[int] = []

        async def fn(retry: Retry) -> None:
            calls.append(retry.count)
            raise asyncio.CancelledError()

        after_attempt = MagicMock()
        on_retryable_error = MagicMock()
        backoff_strategy = MagicMock(side_effect=AssertionError("backoff_strategy must not be called"))
        mock_sleep_async = AsyncMock(side_effect=AssertionError("sleep_async must not be called"))
        retry_policy = RetryPolicy(
            max_retries=10,
            min_sleep_secs=0.001,
            initial_max_sleep_secs=0.001,
            max_sleep_secs=0.001,
            max_elapsed_secs=10,
        ).copy(timing=RetryTiming().copy(sleep_async=mock_sleep_async))

        with self.assertRaises(asyncio.CancelledError):
            await call_with_retries_async(
                fn,
                policy=retry_policy,
                backoff=backoff_strategy,
                after_attempt=after_attempt,
                on_retryable_error=on_retryable_error,
                log=None,
            )

        self.assertEqual([0], calls)
        after_attempt.assert_not_called()
        on_retryable_error.assert_not_called()
        backoff_strategy.assert_not_called()
        self.assertEqual(0, mock_sleep_async.await_count)

    async def test_retry_template_call_with_retries_async(self) -> None:
        """Ensures RetryTemplate.call_with_retries_async() runs with template defaults and log overrides."""
        calls: list[int] = []
        before_attempt_calls: list[int] = []
        outcomes: list[AttemptOutcome] = []
        retry_policy = RetryPolicy.no_retries()
        retry_config = RetryConfig(display_msg="template")
        retry_policy = retry_policy.copy(config=retry_config)
        template_log = MagicMock(spec=logging.Logger)

        def before_attempt(retry: Retry) -> int:
            before_attempt_calls.append(retry.count)
            return 0

        def after_attempt(outcome: AttemptOutcome) -> None:
            outcomes.append(outcome)

        template: RetryTemplate[str] = RetryTemplate(
            policy=retry_policy,
            before_attempt=before_attempt,
            after_attempt=after_attempt,
            log=template_log,
        )

        async def fn(retry: Retry) -> str:
            calls.append(retry.count)
            self.assertIs(retry_policy, retry.policy)
            self.assertIs(retry_config, retry.policy.config)
            return "hello"

        with self.subTest("default_logger_from_template"):
            calls.clear()
            before_attempt_calls.clear()
            outcomes.clear()
            actual = await template.call_with_retries_async(fn)
            self.assertEqual("hello", actual)
            self.assertEqual([0], calls)
            self.assertEqual([0], before_attempt_calls)
            self.assertEqual(1, len(outcomes))
            self.assertTrue(outcomes[0].is_success)
            self.assertIs(template_log, outcomes[0].retry.log)

        with self.subTest("no_logger_override"):
            calls.clear()
            before_attempt_calls.clear()
            outcomes.clear()
            actual = await template.call_with_retries_async(fn, log=NO_LOGGER)
            self.assertEqual("hello", actual)
            self.assertEqual([0], calls)
            self.assertEqual([0], before_attempt_calls)
            self.assertEqual(1, len(outcomes))
            self.assertTrue(outcomes[0].is_success)
            self.assertIsNone(outcomes[0].retry.log)

    async def test_await_with_timeout_success_and_timeout(self) -> None:
        loop = asyncio.get_running_loop()

        with self.subTest("success"):

            async def immediate() -> str:
                return "ok"

            actual = await asyncio_await_with_timeout(immediate(), timeout_nanos=1_000_000_000)
            self.assertEqual("ok", actual)

        with self.subTest("timeout"):
            cancelled = asyncio.Event()

            async def long_running() -> None:
                try:
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    cancelled.set()
                    raise

            with self.assertRaises(RetryableError) as exc:
                await asyncio_await_with_timeout(long_running(), timeout_nanos=50_000_000, display_msg="connect")
            self.assertEqual("connect", exc.exception.display_msg)
            self.assertIsInstance(exc.exception.__cause__, TimeoutError)
            self.assertTrue(cancelled.is_set())

        with self.subTest("invalid_timeout"):
            fut: asyncio.Future[None] = loop.create_future()
            fut.set_result(None)
            with self.assertRaises(ValueError):
                await asyncio_await_with_timeout(fut, timeout_nanos=-1)

        with self.subTest("zero_timeout"):
            fut2: asyncio.Future[str] = loop.create_future()
            fut2.set_result("ok")
            with self.assertRaises(RetryableError) as exc2:
                await asyncio_await_with_timeout(fut2, timeout_nanos=0, display_msg="connect")
            self.assertEqual("connect", exc2.exception.display_msg)
            self.assertIsInstance(exc2.exception.__cause__, TimeoutError)

    async def test_make_from_asyncio_none_returns_default_timing(self) -> None:
        sleep_nanos = 456_000_000
        expected_secs = sleep_nanos / 1_000_000_000
        timing = RetryTiming.make_from_asyncio(None)
        retry = Retry(
            count=0,
            call_start_time_nanos=0,
            before_attempt_start_time_nanos=0,
            attempt_start_time_nanos=0,
            idle_nanos=0,
            policy=RetryPolicy.no_retries().copy(timing=timing),
            log=None,
            previous_outcomes=(),
        )
        with patch("asyncio.sleep", new=AsyncMock()) as mock_sleep:
            await timing.sleep_async(sleep_nanos, retry)
        mock_sleep.assert_awaited_once_with(expected_secs)
        self.assertFalse(timing.is_terminated(retry))

    async def test_make_from_asyncio_zero_sleep_yields(self) -> None:
        termination_event = asyncio.Event()
        timing = RetryTiming.make_from_asyncio(termination_event)
        retry = Retry(
            count=0,
            call_start_time_nanos=0,
            before_attempt_start_time_nanos=0,
            attempt_start_time_nanos=0,
            idle_nanos=0,
            policy=RetryPolicy.no_retries().copy(timing=timing),
            log=None,
            previous_outcomes=(),
        )
        with (
            patch("asyncio.sleep", new=AsyncMock()) as mock_sleep,
            patch("asyncio.wait_for", new=AsyncMock()) as mock_wait_for,
        ):
            await timing.sleep_async(0, retry)
        mock_sleep.assert_awaited_once_with(0)
        mock_wait_for.assert_not_awaited()

    def test_make_from_asyncio_event_is_terminated_delegates_to_is_set(self) -> None:
        termination_event = MagicMock(spec=asyncio.Event)
        termination_event.is_set.return_value = True
        timing = RetryTiming.make_from_asyncio(termination_event)
        retry = Retry(
            count=0,
            call_start_time_nanos=0,
            before_attempt_start_time_nanos=0,
            attempt_start_time_nanos=0,
            idle_nanos=0,
            policy=RetryPolicy.no_retries().copy(timing=timing),
            log=None,
            previous_outcomes=(),
        )
        self.assertTrue(timing.is_terminated(retry))
        termination_event.is_set.assert_called_once()

    async def test_make_from_asyncio_positive_sleep_waits_for_termination_event(self) -> None:
        sleep_nanos = 456_000_000
        expected_secs = sleep_nanos / 1_000_000_000
        termination_event = MagicMock()
        termination_event.is_set.return_value = False
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        fut.set_result(None)
        termination_event.wait.return_value = fut
        timing = RetryTiming.make_from_asyncio(termination_event)
        retry = Retry(
            count=0,
            call_start_time_nanos=0,
            before_attempt_start_time_nanos=0,
            attempt_start_time_nanos=0,
            idle_nanos=0,
            policy=RetryPolicy.no_retries().copy(timing=timing),
            log=None,
            previous_outcomes=(),
        )

        async def fake_wait_for(awaitable: Awaitable[object], *, timeout: float) -> None:
            self.assertEqual(expected_secs, timeout)
            await awaitable

        with (
            patch("asyncio.wait_for", new=AsyncMock(side_effect=fake_wait_for)) as mock_wait_for,
            patch("asyncio.sleep", new=AsyncMock()) as mock_sleep,
        ):
            await timing.sleep_async(sleep_nanos, retry)

        termination_event.wait.assert_called_once_with()
        mock_wait_for.assert_awaited_once()
        self.assertIs(fut, mock_wait_for.call_args[0][0])
        self.assertEqual(expected_secs, mock_wait_for.call_args.kwargs["timeout"])
        mock_sleep.assert_not_awaited()

    async def test_make_from_asyncio_positive_sleep_timeout_is_swallowed(self) -> None:
        sleep_nanos = 456_000_000
        expected_secs = sleep_nanos / 1_000_000_000
        termination_event = MagicMock()
        termination_event.is_set.return_value = False
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        termination_event.wait.return_value = fut
        timing = RetryTiming.make_from_asyncio(termination_event)
        retry = Retry(
            count=0,
            call_start_time_nanos=0,
            before_attempt_start_time_nanos=0,
            attempt_start_time_nanos=0,
            idle_nanos=0,
            policy=RetryPolicy.no_retries().copy(timing=timing),
            log=None,
            previous_outcomes=(),
        )

        mock_wait_for = AsyncMock(side_effect=asyncio.TimeoutError)
        with (
            patch("asyncio.wait_for", new=mock_wait_for),
            patch("asyncio.sleep", new=AsyncMock()) as mock_sleep,
        ):
            await timing.sleep_async(sleep_nanos, retry)

        termination_event.wait.assert_called_once_with()
        mock_wait_for.assert_awaited_once()
        self.assertIs(fut, mock_wait_for.call_args[0][0])
        self.assertEqual(expected_secs, mock_wait_for.call_args.kwargs["timeout"])
        mock_sleep.assert_not_awaited()

    def test_make_from_asyncio_none_result_is_pickleable(self) -> None:
        """RetryTiming.make_from_asyncio(None) must remain pickleable."""
        timing = RetryTiming.make_from_asyncio(None)
        roundtripped = pickle.loads(pickle.dumps(timing))
        retry = Retry(
            count=0,
            call_start_time_nanos=0,
            before_attempt_start_time_nanos=0,
            attempt_start_time_nanos=0,
            idle_nanos=0,
            policy=RetryPolicy.no_retries().copy(timing=roundtripped),
            log=None,
            previous_outcomes=(),
        )
        self.assertFalse(roundtripped.is_terminated(retry))


#############################################################################
class TestRetryTemplateWrapsAsync(unittest.IsolatedAsyncioTestCase):

    async def test_wraps_async_function_retries_and_passes_args(self) -> None:
        calls: list[tuple[int, int]] = []
        retry_count = 0

        async def fn(x: int) -> int:
            nonlocal retry_count
            calls.append((retry_count, x))
            retry_count += 1
            if retry_count == 1:
                raise RetryableError("fail")
            return x * 2

        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        template: RetryTemplate = RetryTemplate(policy=retry_policy, log=None)
        wrapped: Callable[[int], Awaitable[int]] = template.wraps(fn)

        actual: int = await wrapped(5)
        self.assertEqual(10, actual)
        self.assertEqual([(0, 5), (1, 5)], calls)

    async def test_wraps_async_callable_object_retries_and_passes_args(self) -> None:
        """Ensures wraps() treats async __call__ objects as async retry targets."""
        calls: list[tuple[int, int]] = []

        class MyAsyncCallable:
            """Async callable that fails once, then succeeds."""

            retry_count = 0

            async def __call__(self, x: int) -> int:
                """Raises RetryableError on first call and returns x*2 thereafter."""
                calls.append((self.retry_count, x))
                self.retry_count += 1
                if self.retry_count == 1:
                    raise RetryableError("fail")
                return x * 2

        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        template: RetryTemplate[int] = RetryTemplate(policy=retry_policy, log=None)
        wrapped: Callable[[int], Awaitable[int]] = template.wraps(MyAsyncCallable())

        actual: int = await wrapped(5)
        self.assertEqual(10, actual)
        self.assertEqual([(0, 5), (1, 5)], calls)

    async def test_wraps_partial_of_async_callable_object_retries_and_passes_args(self) -> None:
        """Ensures wraps() handles functools.partial(async_callable_obj, ...) correctly."""
        calls: list[tuple[int, int]] = []

        class MyAsyncCallable:
            """Async callable that fails once, then succeeds."""

            retry_count = 0

            async def __call__(self, x: int) -> int:
                """Raises RetryableError on first call and returns x*2 thereafter."""
                calls.append((self.retry_count, x))
                self.retry_count += 1
                if self.retry_count == 1:
                    raise RetryableError("fail")
                return x * 2

        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        template: RetryTemplate = RetryTemplate(policy=retry_policy, log=None)
        wrapped: Callable[[], Awaitable[int]] = template.wraps(functools.partial(MyAsyncCallable(), 5))

        actual: int = await wrapped()
        self.assertEqual(10, actual)
        self.assertEqual([(0, 5), (1, 5)], calls)

    async def test_wraps_partial_of_async_function_retries_and_passes_args(self) -> None:
        """Ensures wraps() handles functools.partial(async_fn, ...) correctly."""
        calls: list[tuple[int, int]] = []
        retry_count = 0

        async def fn(x: int) -> int:
            nonlocal retry_count
            calls.append((retry_count, x))
            retry_count += 1
            if retry_count == 1:
                raise RetryableError("fail")
            return x * 2

        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        template: RetryTemplate = RetryTemplate(policy=retry_policy, log=None)
        wrapped: Callable[[], Awaitable[int]] = template.wraps(functools.partial(fn, 5))

        actual: int = await wrapped()
        self.assertEqual(10, actual)
        self.assertEqual([(0, 5), (1, 5)], calls)
