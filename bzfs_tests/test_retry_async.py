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
import sys
import unittest
from collections.abc import (
    Awaitable,
)
from typing import (
    Callable,
    NoReturn,
    TypeVar,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    patch,
)

from bzfs_main.util.retry import (
    NO_LOGGER,
    AsyncRetryTemplate,
    AttemptOutcome,
    Retry,
    RetryableError,
    RetryConfig,
    RetryError,
    RetryPolicy,
    RetryTiming,
    before_attempt_noop,
    call_with_retries_async,
    multi_after_attempt_async,
    raise_retryable_error_from,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestAsyncCallWithRetries,
        TestAsyncRetryTemplateCall,
        TestAsyncRetryTemplateWraps,
        TestAsyncioAwaitWithRetryableTimeout,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
_T = TypeVar("_T")


async def asyncio_await_with_retryable_timeout(
    awaitable: Awaitable[_T],
    timeout_nanos: int,
    *,
    reraise_timeout_error: bool = True,
    raise_retryable_error: Callable[[TimeoutError], NoReturn] = lambda exc: raise_retryable_error_from(
        exc, display_msg="timeout"
    ),
) -> _T:
    """Await an awaitable (for example a coroutine), translating successful cancellation on timeout into RetryableError while
    preserving the awaitable's results and errors, including its own TimeoutError if `reraise_timeout_error == True`;
    assumes cooperative cancellation and that retrying on timeout is appropriate.

    The semantics are intended for cooperative, per-attempt timeouts for work the caller knows is safe to retry. For example
    reads, idempotent writes, requests with deduplication keys, and operations with explicit reconciliation or recovery.

    Requires Python >= 3.11.
    Cancellation is cooperative; the awaitable may exceed the timeout while running or performing cleanup.
    Zero and negative `timeout_nanos` still permit work to start, consistent with asyncio's scheduling semantics.
    Direct coroutines run in the caller's task; supplied tasks may already be running and retain their own task identity.

    reraise_timeout_error=False treats all directly raised TimeoutErrors as retryable.
    For example, with timeout_nanos=2_000_000_000, if the `awaitable` internally raises TimeoutError after one second:
    - reraise_timeout_error=False: raises RetryableError to enable convenient automatic retry.
    - reraise_timeout_error=True: propagates the original TimeoutError so callers can handle it in custom ways.
    """
    reraise: bool = False
    try:
        async with asyncio.timeout(timeout_nanos / 1_000_000_000):  # type: ignore[attr-defined]  # requires Python >= 3.11
            try:
                return await awaitable
            except TimeoutError:
                reraise = reraise_timeout_error
                raise
    except TimeoutError as exc:
        if reraise:
            raise
        else:
            raise_retryable_error(exc)


#############################################################################
class TestAsyncCallWithRetries(unittest.IsolatedAsyncioTestCase):
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

    async def test_call_with_retries_async_awaits_async_after_attempt(self) -> None:
        """Ensures async after_attempt() is awaited for failed and successful attempts."""
        retry_policy = RetryPolicy(max_retries=1, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0)
        events: list[tuple[bool, bool, int]] = []

        async def fn(retry: Retry) -> str:
            if retry.count == 0:
                raise RetryableError("fail") from ValueError("boom")
            return "ok"

        async def after_attempt(outcome: AttemptOutcome) -> None:
            await asyncio.sleep(0)
            events.append((outcome.is_success, outcome.is_exhausted, outcome.retry.count))

        actual = await call_with_retries_async(fn, policy=retry_policy, after_attempt=after_attempt, log=None)

        self.assertEqual("ok", actual)
        self.assertEqual([(False, False, 0), (True, False, 1)], events)

    async def test_call_with_retries_async_awaits_async_after_attempt_on_exhaustion(self) -> None:
        """Ensures async after_attempt() is awaited for the terminal exhausted outcome."""
        retry_policy = RetryPolicy.no_retries().copy(reraise=False)
        events: list[tuple[bool, bool, int]] = []

        async def fn(_retry: Retry) -> None:
            raise RetryableError("fail")

        async def after_attempt(outcome: AttemptOutcome) -> None:
            await asyncio.sleep(0)
            events.append((outcome.is_success, outcome.is_exhausted, outcome.retry.count))

        with self.assertRaises(RetryError):
            await call_with_retries_async(fn, policy=retry_policy, after_attempt=after_attempt, log=None)

        self.assertEqual([(False, True, 0)], events)

    async def test_call_with_retries_async_async_after_attempt_cancelled_error_propagates(self) -> None:
        """Ensures asyncio.CancelledError from async after_attempt() propagates without retrying."""
        retry_policy = RetryPolicy(max_retries=3, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0)
        fn_calls: list[int] = []
        after_attempt_calls: list[int] = []

        async def fn(retry: Retry) -> str:
            fn_calls.append(retry.count)
            return "ok"

        async def after_attempt(outcome: AttemptOutcome) -> None:
            after_attempt_calls.append(outcome.retry.count)
            raise asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await call_with_retries_async(fn, policy=retry_policy, after_attempt=after_attempt, log=None)

        self.assertEqual([0], fn_calls)
        self.assertEqual([0], after_attempt_calls)

    async def test_multi_after_attempt_async_runs_sync_and_async_handlers_in_order(self) -> None:
        """Ensures multi_after_attempt_async() awaits mixed sync and async handlers in order."""
        retry_policy = RetryPolicy(max_retries=1, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=0)
        events: list[tuple[str, bool, int]] = []

        async def fn(retry: Retry) -> str:
            if retry.count == 0:
                raise RetryableError("fail") from ValueError("boom")
            return "ok"

        def handler1(outcome: AttemptOutcome) -> None:
            events.append(("sync", outcome.is_success, outcome.retry.count))

        async def handler2(outcome: AttemptOutcome) -> None:
            await asyncio.sleep(0)
            events.append(("async", outcome.is_success, outcome.retry.count))

        actual = await call_with_retries_async(
            fn,
            policy=retry_policy,
            after_attempt=multi_after_attempt_async([handler1, handler2]),
            log=None,
        )

        self.assertEqual("ok", actual)
        self.assertEqual(
            [
                ("sync", False, 0),
                ("async", False, 0),
                ("sync", True, 1),
                ("async", True, 1),
            ],
            events,
        )

    async def test_multi_after_attempt_async_cancelled_error_propagates(self) -> None:
        """Ensures multi_after_attempt_async() propagates cancellation and stops later handlers."""
        retry_policy = RetryPolicy.no_retries()
        events: list[str] = []

        async def fn(_retry: Retry) -> str:
            return "ok"

        async def handler1(_outcome: AttemptOutcome) -> None:
            events.append("before-cancel")
            raise asyncio.CancelledError()

        def handler2(_outcome: AttemptOutcome) -> None:
            events.append("must-not-run")

        with self.assertRaises(asyncio.CancelledError):
            await call_with_retries_async(
                fn,
                policy=retry_policy,
                after_attempt=multi_after_attempt_async([handler1, handler2]),
                log=None,
            )

        self.assertEqual(["before-cancel"], events)

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

    async def test_call_with_retries_async_awaits_async_before_attempt(self) -> None:
        """Ensures async before_attempt() is awaited and its returned sleep duration is used."""
        retry_policy = RetryPolicy.no_retries()
        before_attempt_calls: list[int] = []
        sleeps: list[tuple[int, int]] = []
        fn_calls: list[int] = []

        async def before_attempt(retry: Retry) -> int:
            await asyncio.sleep(0)
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

    async def test_call_with_retries_async_async_before_attempt_runs_for_each_attempt(self) -> None:
        """Ensures async before_attempt() runs before each fn(retry) invocation, including retries."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=10,
        )
        before_attempt_calls: list[int] = []
        sleeps: list[int] = []
        fn_calls: list[int] = []

        async def before_attempt(retry: Retry) -> int:
            await asyncio.sleep(0)
            before_attempt_calls.append(retry.count)
            return 0 if retry.count == 0 else 7

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

    async def test_call_with_retries_async_async_before_attempt_cancelled_error_propagates(self) -> None:
        """Ensures asyncio.CancelledError from async before_attempt() propagates without invoking fn."""
        retry_policy = RetryPolicy.no_retries()
        before_attempt_calls: list[int] = []
        fn_calls: list[int] = []

        async def before_attempt(retry: Retry) -> int:
            before_attempt_calls.append(retry.count)
            raise asyncio.CancelledError()

        async def fn(retry: Retry) -> str:
            fn_calls.append(retry.count)
            return "ok"

        with self.assertRaises(asyncio.CancelledError):
            await call_with_retries_async(fn, policy=retry_policy, before_attempt=before_attempt, log=None)

        self.assertEqual([0], before_attempt_calls)
        self.assertEqual([], fn_calls)

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

    async def test_call_with_retries_async_awaits_async_on_exhaustion(self) -> None:
        """Ensures async on_exhaustion() can return a fallback value."""
        calls: list[int] = []
        outcomes: list[AttemptOutcome] = []

        async def fn(retry: Retry) -> str:
            calls.append(retry.count)
            raise RetryableError("fail") from ValueError("boom")

        async def on_exhaustion(outcome: AttemptOutcome) -> str:
            await asyncio.sleep(0)
            outcomes.append(outcome)
            return "fallback"

        actual = await call_with_retries_async(
            fn,
            policy=RetryPolicy.no_retries(),
            on_exhaustion=on_exhaustion,
            log=None,
        )

        self.assertEqual("fallback", actual)
        self.assertEqual([0], calls)
        self.assertEqual(1, len(outcomes))
        self.assertTrue(outcomes[0].is_exhausted)
        self.assertEqual(0, outcomes[0].retry.count)
        self.assertIsInstance(outcomes[0].result, RetryableError)

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

    async def test_call_with_retries_async_awaits_async_on_retryable_error(self) -> None:
        """Ensures async on_retryable_error runs once for each RetryableError raised by fn()."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=10,
            reraise=False,
        )
        seen_counts: list[int] = []

        async def on_retryable_error(outcome: AttemptOutcome) -> None:
            await asyncio.sleep(0)
            seen_counts.append(outcome.retry.count)

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

        template: AsyncRetryTemplate[str] = AsyncRetryTemplate(
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
            actual = await template.call_with_retries(fn)
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
            actual = await template.call_with_retries(fn, log=NO_LOGGER)
            self.assertEqual("hello", actual)
            self.assertEqual([0], calls)
            self.assertEqual([0], before_attempt_calls)
            self.assertEqual(1, len(outcomes))
            self.assertTrue(outcomes[0].is_success)
            self.assertIsNone(outcomes[0].retry.log)

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
class TestAsyncRetryTemplateCall(unittest.IsolatedAsyncioTestCase):
    """Unit tests for the async callable facade of AsyncRetryTemplate."""

    async def test_default_fn_raises_not_implemented_error(self) -> None:
        """Ensures the default async template callable requires an explicit fn."""
        template: AsyncRetryTemplate = AsyncRetryTemplate()
        with self.assertRaises(NotImplementedError) as ctx:
            await template()
        self.assertEqual("Provide fn when calling RetryTemplate", str(ctx.exception))

    async def test_retry_template_is_callable_and_runs(self) -> None:
        """Ensures AsyncRetryTemplate.__call__() retries using its stored parameters."""
        calls: list[int] = []

        async def fn(retry: Retry) -> str:
            calls.append(retry.count)
            if retry.count == 0:
                raise RetryableError("transient")
            return "ok"

        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        after_attempts: list[bool] = []

        async def after_attempt(outcome: AttemptOutcome) -> None:
            after_attempts.append(outcome.is_success)

        template: AsyncRetryTemplate[str] = AsyncRetryTemplate(
            fn=fn,
            policy=retry_policy,
            after_attempt=after_attempt,
            log=None,
        )

        self.assertTrue(callable(template))
        self.assertEqual("ok", await template())
        self.assertEqual([0, 1], calls)
        self.assertEqual([False, True], after_attempts)

        retrying: AsyncRetryTemplate = AsyncRetryTemplate(policy=retry_policy, after_attempt=after_attempt, log=None)

        async def _fn(retry: Retry) -> str:
            return "hello"

        actual1: str = await retrying.copy(fn=_fn)()
        self.assertEqual("hello", actual1)

        actual2: str = await retrying.call_with_retries(_fn)
        self.assertEqual("hello", actual2)

        actual3: str = await retrying.call_with_retries(_fn, policy=retrying.policy)
        self.assertEqual("hello", actual3)

        actual4: str = await retrying.call_with_retries(_fn, policy=None)
        self.assertEqual("hello", actual4)

        actual5: str = await retrying.call_with_retries(lambda retry: _fn(retry), log=NO_LOGGER)
        self.assertEqual("hello", actual5)

        self.assertIsNotNone(str(retrying))
        self.assertNotEqual("", str(retrying))

    async def test_retry_template_awaits_async_on_exhaustion(self) -> None:
        """Ensures AsyncRetryTemplate awaits an async on_exhaustion callback."""
        calls: list[int] = []
        outcomes: list[AttemptOutcome] = []

        async def fn(retry: Retry) -> str:
            calls.append(retry.count)
            raise RetryableError("fail") from ValueError("boom")

        async def on_exhaustion(outcome: AttemptOutcome) -> str:
            await asyncio.sleep(0)
            outcomes.append(outcome)
            return "fallback"

        template: AsyncRetryTemplate[str] = AsyncRetryTemplate(
            fn=fn,
            policy=RetryPolicy.no_retries(),
            on_exhaustion=on_exhaustion,
            log=None,
        )

        self.assertEqual("fallback", await template())
        self.assertEqual("fallback", await template.call_with_retries(fn))
        self.assertEqual([0, 0], calls)
        self.assertEqual(2, len(outcomes))
        self.assertTrue(all(outcome.is_exhausted for outcome in outcomes))
        self.assertEqual([0, 0], [outcome.retry.count for outcome in outcomes])

    async def test_retry_template_awaits_async_on_retryable_error(self) -> None:
        """Ensures AsyncRetryTemplate awaits async on_retryable_error defaults and overrides."""
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=10,
            reraise=False,
        )
        template_counts: list[int] = []
        override_counts: list[int] = []

        async def fn(_retry: Retry) -> str:
            raise RetryableError("fail") from ValueError("boom")

        async def template_on_retryable_error(outcome: AttemptOutcome) -> None:
            await asyncio.sleep(0)
            template_counts.append(outcome.retry.count)

        async def override_on_retryable_error(outcome: AttemptOutcome) -> None:
            await asyncio.sleep(0)
            override_counts.append(outcome.retry.count)

        template: AsyncRetryTemplate[str] = AsyncRetryTemplate(
            fn=fn,
            policy=retry_policy,
            on_retryable_error=template_on_retryable_error,
            log=None,
        )

        with self.assertRaises(RetryError):
            await template()
        with self.assertRaises(RetryError):
            await template.call_with_retries(fn, on_retryable_error=override_on_retryable_error)

        self.assertEqual([0, 1], template_counts)
        self.assertEqual([0, 1], override_counts)


#############################################################################
class TestAsyncRetryTemplateWraps(unittest.IsolatedAsyncioTestCase):

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
        template: AsyncRetryTemplate = AsyncRetryTemplate(policy=retry_policy, log=None)
        wrapped: Callable[[int], Awaitable[int]] = template.wraps(fn)

        actual: int = await wrapped(5)
        self.assertEqual(10, actual)
        self.assertEqual([(0, 5), (1, 5)], calls)

    async def test_wraps_awaitable_returning_function_retries_and_passes_args(self) -> None:
        """Ensures wraps() accepts synchronous callables that return awaitables."""
        calls: list[tuple[int, int]] = []
        retry_count = 0

        async def async_impl(x: int) -> int:
            nonlocal retry_count
            calls.append((retry_count, x))
            retry_count += 1
            if retry_count == 1:
                raise RetryableError("fail")
            return x * 2

        def fn(x: int) -> Awaitable[int]:
            return async_impl(x)

        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1,
        )
        template: AsyncRetryTemplate = AsyncRetryTemplate(policy=retry_policy, log=None)
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
        template: AsyncRetryTemplate[int] = AsyncRetryTemplate(policy=retry_policy, log=None)
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
        template: AsyncRetryTemplate = AsyncRetryTemplate(policy=retry_policy, log=None)
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
        template: AsyncRetryTemplate = AsyncRetryTemplate(policy=retry_policy, log=None)
        wrapped: Callable[[], Awaitable[int]] = template.wraps(functools.partial(fn, 5))

        actual: int = await wrapped()
        self.assertEqual(10, actual)
        self.assertEqual([(0, 5), (1, 5)], calls)


#############################################################################
@unittest.skipIf(sys.version_info < (3, 11), "Requires asyncio.timeout() (Python >= 3.11)")
class TestAsyncioAwaitWithRetryableTimeout(unittest.IsolatedAsyncioTestCase):
    """Test retryable timeouts on Python 3.11+ with cooperative awaitables and isolated event loops, separating helper
    behavior from retry-loop coverage."""

    async def test_await_with_timeout_runs_in_caller_task(self) -> None:
        """Require a directly supplied coroutine to share the caller's task across suspension, avoiding child-task scheduling."""
        caller = asyncio.current_task()

        async def operation() -> str:
            self.assertIs(caller, asyncio.current_task())
            await asyncio.sleep(0)
            self.assertIs(caller, asyncio.current_task())
            return "ok"

        loop = asyncio.get_running_loop()
        with patch.object(loop, "time", return_value=loop.time()):
            actual = await asyncio_await_with_retryable_timeout(operation(), timeout_nanos=1_000_000_000)
        self.assertEqual("ok", actual)

    async def test_await_with_timeout_success_and_timeout(self) -> None:
        """Verify success, cooperative cancellation and fresh default errors for positive and nonpositive budgets. Freeze
        the loop clock and advance it inside the operation so deadlines do not race with startup."""
        loop = asyncio.get_running_loop()
        errors: list[RetryableError] = []

        async def immediate() -> str:
            return "ok"

        for timeout_nanos in (1_000_000_000, 0, -1):
            with (
                self.subTest(timeout_nanos=timeout_nanos),
                patch.object(loop, "time", return_value=loop.time()) as clock,
            ):
                with self.subTest("success"):
                    actual = await asyncio_await_with_retryable_timeout(immediate(), timeout_nanos=timeout_nanos)
                    self.assertEqual("ok", actual)

                with self.subTest("timeout"):
                    cancelled = asyncio.Event()

                    async def long_running(cancelled_event: asyncio.Event) -> None:
                        try:
                            clock.return_value += 2
                            await asyncio.Event().wait()  # wait forever until cancellation
                        except asyncio.CancelledError:
                            cancelled_event.set()
                            raise
                        raise AssertionError("unreachable")

                    with self.assertRaises(RetryableError) as exc:
                        await asyncio_await_with_retryable_timeout(long_running(cancelled), timeout_nanos=timeout_nanos)
                    errors.append(exc.exception)
                    self.assertEqual("timeout", exc.exception.display_msg)
                    self.assertIsInstance(exc.exception.__cause__, TimeoutError)
                    self.assertTrue(cancelled.is_set())
        self.assertEqual(3, len({id(error) for error in errors}))

    async def test_await_with_timeout_skips_handler_for_completed_future(self) -> None:
        """Avoid invoking the handler for completed results or propagated errors; completed futures isolate conversion
        from timeout scheduling without leaving unawaited coroutines on failure."""
        raise_retryable_error = MagicMock(side_effect=AssertionError("raise_retryable_error must not be called"))
        for error in (None, TimeoutError("operation"), ValueError("operation"), asyncio.CancelledError("operation")):
            with self.subTest(error=error):
                future: asyncio.Future[str] = asyncio.get_running_loop().create_future()
                if error is None:
                    future.set_result("ok")
                    actual = await asyncio_await_with_retryable_timeout(
                        future, timeout_nanos=0, raise_retryable_error=raise_retryable_error
                    )
                    self.assertEqual("ok", actual)
                else:
                    future.set_exception(error)
                    with self.assertRaises(type(error)) as exc:
                        await asyncio_await_with_retryable_timeout(
                            future, timeout_nanos=0, raise_retryable_error=raise_retryable_error
                        )
                    self.assertIs(error, exc.exception)
        raise_retryable_error.assert_not_called()

    async def test_await_with_timeout_preserves_completed_result_after_deadline(self) -> None:
        """Keep authoritative results when synchronous work exceeds the deadline, using a frozen loop clock for determinism."""
        loop = asyncio.get_running_loop()

        async def operation() -> str:
            clock.return_value += 2
            return "committed"

        with patch.object(loop, "time", return_value=loop.time()) as clock:
            actual = await asyncio_await_with_retryable_timeout(operation(), timeout_nanos=1_000_000_000)
        self.assertEqual("committed", actual)

    async def test_await_with_timeout_propagates_operation_errors(self) -> None:
        """Preserve operation exception identity; cancellation must propagate but asyncio may reconstruct its exception."""

        async def fail(error: BaseException) -> None:
            raise error

        for error in (
            TimeoutError("operation"),
            ValueError("operation"),
            asyncio.CancelledError("operation"),
        ):
            with self.subTest(error=error):
                with self.assertRaises(type(error)) as exc:
                    await asyncio_await_with_retryable_timeout(fail(error), timeout_nanos=1_000_000_000)
                if not isinstance(error, asyncio.CancelledError):
                    self.assertIs(error, exc.exception)

    async def test_await_with_timeout_wraps_operation_timeout_when_requested(self) -> None:
        """Verify opting in invokes the handler once for an operation's TimeoutError and preserves its cause; fail before
        suspension to isolate conversion from timeout scheduling."""
        error = TimeoutError("operation")
        expected_error = RetryableError(display_msg="connect")

        def raise_error(exc: TimeoutError) -> NoReturn:
            raise expected_error from exc

        raise_retryable_error = MagicMock(side_effect=raise_error)

        async def fail() -> None:
            raise error

        with self.assertRaises(RetryableError) as exc:
            await asyncio_await_with_retryable_timeout(
                fail(), timeout_nanos=1_000_000_000, reraise_timeout_error=False, raise_retryable_error=raise_retryable_error
            )
        raise_retryable_error.assert_called_once_with(error)
        self.assertIs(expected_error, exc.exception)
        self.assertIs(error, exc.exception.__cause__)

    async def test_await_with_timeout_uses_cancellation_cleanup_outcome(self) -> None:
        """Preserve cleanup outcomes using real cancellation; advance the frozen loop clock after startup to avoid timing races."""
        loop = asyncio.get_running_loop()
        start_time = loop.time()

        async def long_running(error: Exception | None) -> str:
            try:
                clock.return_value += 2
                await asyncio.Event().wait()  # wait forever until cancellation
            except asyncio.CancelledError:
                if error is not None:
                    raise error from None
            return "cleanup result"

        for error in (None, ValueError("cleanup"), TimeoutError("cleanup")):
            with (
                self.subTest(error=error),
                patch.object(loop, "time", return_value=start_time) as clock,
            ):
                coroutine = long_running(error)
                if error is None:
                    actual = await asyncio_await_with_retryable_timeout(coroutine, timeout_nanos=1_000_000_000)
                    self.assertEqual("cleanup result", actual)
                else:
                    with self.assertRaises(type(error)) as exc:
                        await asyncio_await_with_retryable_timeout(coroutine, timeout_nanos=1_000_000_000)
                    self.assertIs(error, exc.exception)

    async def test_await_with_timeout_propagates_caller_cancellation(self) -> None:
        """Freeze the loop clock so caller cancellation is the only cancellation source; require asynchronous
        cleanup before cancellation propagates."""
        loop = asyncio.get_running_loop()
        started = asyncio.Event()
        blocked = asyncio.Event()
        cleaned_up = asyncio.Event()

        async def long_running() -> None:
            started.set()
            try:
                await blocked.wait()
            finally:
                await asyncio.sleep(0)
                cleaned_up.set()

        with patch.object(loop, "time", return_value=loop.time()):
            task = asyncio.create_task(asyncio_await_with_retryable_timeout(long_running(), timeout_nanos=1_000_000_000))
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(cleaned_up.is_set())

    async def test_await_with_timeout_accepts_future(self) -> None:
        """Await a pending future through the keyword API; freeze the clock so scheduled completion wins deterministically."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        with patch.object(loop, "time", return_value=loop.time()):
            loop.call_soon(future.set_result, "ok")
            actual = await asyncio_await_with_retryable_timeout(awaitable=future, timeout_nanos=1_000_000_000)
        self.assertEqual("ok", actual)

    async def test_await_with_timeout_accepts_task(self) -> None:
        """Preserve a supplied task's identity across suspension; freeze the clock to isolate execution from timeout races."""

        async def operation() -> asyncio.Task | None:
            await asyncio.sleep(0)
            return asyncio.current_task()

        loop = asyncio.get_running_loop()
        with patch.object(loop, "time", return_value=loop.time()):
            task = asyncio.create_task(operation())
            actual = await asyncio_await_with_retryable_timeout(awaitable=task, timeout_nanos=1_000_000_000)
        self.assertIs(task, actual)
        self.assertIsNot(asyncio.current_task(), actual)

    async def test_await_with_timeout_cancels_future(self) -> None:
        """Require timeout cancellation to reach a pending future, using a zero budget to avoid timing races."""
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        with self.assertRaises(RetryableError) as exc:
            await asyncio_await_with_retryable_timeout(awaitable=future, timeout_nanos=0)
        self.assertTrue(future.cancelled())
        self.assertIsInstance(exc.exception.__cause__, TimeoutError)

    async def test_await_with_timeout_cancels_task_after_cleanup(self) -> None:
        """Build retry metadata after asynchronous cancellation cleanup, preserving configured metadata and cause;
        synchronize task startup before the zero-budget wait."""
        started = asyncio.Event()
        cleaned_up = asyncio.Event()

        async def operation() -> None:
            started.set()
            try:
                await asyncio.Event().wait()  # wait forever until cancellation
            finally:
                await asyncio.sleep(0)
                cleaned_up.set()

        task = asyncio.create_task(operation())
        await started.wait()
        with self.assertRaises(RetryableError) as exc:
            await asyncio_await_with_retryable_timeout(awaitable=task, timeout_nanos=0)
        self.assertTrue(task.cancelled())
        self.assertTrue(cleaned_up.is_set())
        self.assertIsInstance(exc.exception.__cause__, TimeoutError)
