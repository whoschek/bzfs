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
"""Examples demonstrating use of call_with_retries()."""

from __future__ import (
    annotations,
)
import logging
import random
import time
import unittest
from collections.abc import (
    Awaitable,
)
from contextlib import (
    suppress,
)
from typing import (
    Callable,
    Final,
    TypeVar,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    patch,
)

from bzfs_main.util.retry import (
    _DEFAULT_RETRY_CONFIG,
    AttemptOutcome,
    BackoffContext,
    Retry,
    RetryableError,
    RetryConfig,
    RetryPolicy,
    _is_terminated,
    _thread_local_rng,
    after_attempt_log_failure,
    full_jitter_backoff_strategy,
    no_giveup,
    on_exhaustion_raise,
)

_T = TypeVar("_T")


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestCallWithRetriesAsync,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
async def call_with_retries_async(
    fn: Callable[[Retry], Awaitable[_T]],  # wraps work and raises RetryableError for failures that shall be retried
    policy: RetryPolicy,  # specifies how ``RetryableError`` shall be retried
    *,
    config: RetryConfig | None = None,  # controls logging settings and async cancellation between attempts
    giveup: Callable[[AttemptOutcome], object | None] = no_giveup,  # stop retrying based on domain-specific logic
    after_attempt: Callable[[AttemptOutcome], None] = after_attempt_log_failure,  # e.g. record metrics and/or custom logging
    on_exhaustion: Callable[[AttemptOutcome], _T] = on_exhaustion_raise,  # raise error or return fallback value
    log: logging.Logger | None = None,
) -> _T:
    """Async version of call_with_retries(); awaits ``fn`` and uses non-blocking sleep."""

    config = _DEFAULT_RETRY_CONFIG if config is None else config
    rng: random.Random | None = None
    retry_count: int = 0
    curr_max_sleep_nanos: int = policy.initial_max_sleep_nanos
    previous_outcomes: tuple[AttemptOutcome, ...] = ()  # for safety pass *immutable* deque to callbacks
    start_time_nanos: Final[int] = time.monotonic_ns()
    while True:
        attempt_start_time_nanos: int = time.monotonic_ns() if retry_count != 0 else start_time_nanos
        retry: Retry = Retry(retry_count, start_time_nanos, attempt_start_time_nanos, policy, config, log, previous_outcomes)
        try:
            result: _T = await fn(retry)  # Call the target function and supply retry attempt number and other metadata
            if after_attempt is not after_attempt_log_failure:
                elapsed_nanos: int = time.monotonic_ns() - start_time_nanos
                outcome: AttemptOutcome = AttemptOutcome(retry, True, False, False, None, elapsed_nanos, 0, result)
                after_attempt(outcome)
            return result
        except RetryableError as retryable_error:
            elapsed_nanos = time.monotonic_ns() - start_time_nanos
            giveup_reason: object | None = None
            sleep_nanos: int = 0
            sleep: Callable[[int, Retry], Awaitable] = _sleep_async
            is_terminated: Callable[[Retry], bool] = _is_terminated
            if retry_count < policy.max_retries and elapsed_nanos < policy.max_elapsed_nanos:
                if policy.max_sleep_nanos == 0 and policy.backoff_strategy is full_jitter_backoff_strategy:
                    pass  # perf: e.g. spin-before-block
                elif retry_count == 0 and retryable_error.retry_immediately_once:
                    pass  # retry once immediately without backoff
                else:  # jitter: default backoff_strategy picks random sleep_nanos in [min_sleep_nanos, curr_max_sleep_nanos]
                    rng = _thread_local_rng() if rng is None else rng
                    sleep_nanos, curr_max_sleep_nanos = policy.backoff_strategy(
                        BackoffContext(retry, curr_max_sleep_nanos, rng, elapsed_nanos, retryable_error)
                    )
                    assert sleep_nanos >= 0 and curr_max_sleep_nanos >= 0, sleep_nanos

                outcome = AttemptOutcome(retry, False, False, False, None, elapsed_nanos, sleep_nanos, retryable_error)
                if (not is_terminated(retry)) and (giveup_reason := giveup(outcome)) is None:
                    after_attempt(outcome)
                    if sleep_nanos > 0:
                        await sleep(sleep_nanos, retry)
                    if not is_terminated(retry):
                        n: int = policy.max_previous_outcomes
                        if n > 0:  #  outcome will be passed to next attempt via Retry.previous_outcomes
                            if previous_outcomes:  # detach to reduce memory footprint
                                outcome = outcome.copy(retry=retry.copy(previous_outcomes=()))
                            previous_outcomes = previous_outcomes[len(previous_outcomes) - n + 1 :] + (outcome,)  # imm deque
                        del outcome  # help gc
                        retry_count += 1
                        continue  # continue retry loop with next attempt
                else:
                    sleep_nanos = 0
            outcome = AttemptOutcome(
                retry, False, True, is_terminated(retry), giveup_reason, elapsed_nanos, sleep_nanos, retryable_error
            )
            after_attempt(outcome)
            return on_exhaustion(outcome)  # raise error or return fallback value


async def _sleep_async(sleep_nanos: int, retry: Retry) -> None:
    import asyncio

    termination_event = retry.config.termination_event
    if termination_event is None:
        await asyncio.sleep(sleep_nanos / 1_000_000_000)
    else:
        assert isinstance(termination_event, asyncio.Event)
        try:
            await asyncio.wait_for(termination_event.wait(), timeout=sleep_nanos / 1_000_000_000)
        except asyncio.TimeoutError:
            pass  # expected


async def await_with_timeout(awaitable: Awaitable[_T], timeout_nanos: int, *, display_msg: object = "timeout") -> _T:
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

        with patch(
            "bzfs_tests.test_retry_examples._sleep_async", new=AsyncMock(side_effect=fake_sleep_async)
        ) as mock_sleep_async:
            actual = await call_with_retries_async(
                fn,
                policy=retry_policy,
                config=RetryConfig(display_msg="foo"),
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

    async def test_call_with_retries_async_retry_immediately_once_skips_backoff_and_sleep(self) -> None:
        backoff_strategy = MagicMock(side_effect=AssertionError("backoff_strategy must not be called"))
        retry_policy = RetryPolicy(
            max_retries=1,
            min_sleep_secs=0.001,
            initial_max_sleep_secs=0.001,
            max_sleep_secs=0.001,
            max_elapsed_secs=10,
            backoff_strategy=backoff_strategy,
        )
        calls: list[int] = []
        events: list[AttemptOutcome] = []

        async def fn(retry: Retry) -> str:
            calls.append(retry.count)
            if retry.count == 0:
                raise RetryableError("fail", retry_immediately_once=True) from ValueError("boom")
            return "ok"

        def after_attempt(outcome: AttemptOutcome) -> None:
            events.append(outcome)

        mock_sleep_async = AsyncMock(side_effect=AssertionError("_sleep_async must not be called"))
        with patch("bzfs_tests.test_retry_examples._sleep_async", new=mock_sleep_async):
            actual = await call_with_retries_async(
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
        self.assertEqual(0, events[0].sleep_nanos)
        self.assertTrue(events[1].is_success)
        self.assertEqual(0, events[1].sleep_nanos)
        self.assertEqual(0, mock_sleep_async.await_count)
        backoff_strategy.assert_not_called()

    async def test_call_with_retries_async_on_exhaustion_reraises_cause(self) -> None:
        async def fn(_retry: Retry) -> None:
            raise RetryableError("fail") from ValueError("boom")

        with self.assertRaises(ValueError):
            await call_with_retries_async(fn, policy=RetryPolicy.no_retries(), config=RetryConfig(), log=None)

    async def test_await_with_timeout_success_and_timeout(self) -> None:
        import asyncio

        loop = asyncio.get_running_loop()

        with self.subTest("success"):

            async def immediate() -> str:
                return "ok"

            actual = await await_with_timeout(immediate(), timeout_nanos=1_000_000_000)
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
                await await_with_timeout(long_running(), timeout_nanos=50_000_000, display_msg="connect")
            self.assertEqual("connect", exc.exception.display_msg)
            self.assertIsInstance(exc.exception.__cause__, TimeoutError)
            self.assertTrue(cancelled.is_set())

        with self.subTest("invalid_timeout"):
            fut: asyncio.Future[None] = loop.create_future()
            fut.set_result(None)
            with self.assertRaises(ValueError):
                await await_with_timeout(fut, timeout_nanos=-1)

        with self.subTest("zero_timeout"):
            fut2: asyncio.Future[str] = loop.create_future()
            fut2.set_result("ok")
            with self.assertRaises(RetryableError) as exc2:
                await await_with_timeout(fut2, timeout_nanos=0, display_msg="connect")
            self.assertEqual("connect", exc2.exception.display_msg)
            self.assertIsInstance(exc2.exception.__cause__, TimeoutError)
