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
"""Examples demonstrating use of call_with_retries(), including call_with_retries_async(), backoff strategies, integration
with circuit breakers, rate limiting and retry-after, Prometheus metrics, etc."""

from __future__ import (
    annotations,
)
import asyncio
import logging
import random
import time
import unittest
from collections.abc import (
    Awaitable,
    Iterable,
)
from contextlib import (
    suppress,
)
from typing import (
    Callable,
    Final,
    TypeVar,
    cast,
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
    BackoffStrategy,
    Retry,
    RetryableError,
    RetryConfig,
    RetryError,
    RetryPolicy,
    RetryTiming,
    _thread_local_rng,
    after_attempt_log_failure,
    before_attempt_noop,
    call_with_retries,
    full_jitter_backoff_strategy,
    no_giveup,
    noop,
    on_exhaustion_raise,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestCallWithRetriesAsync,
        TestMiscBackoffStrategies,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
_T = TypeVar("_T")


async def call_with_retries_async(
    fn: Callable[[Retry], Awaitable[_T]],  # wraps work and raises RetryableError for failures that shall be retried
    policy: RetryPolicy,  # specifies how ``RetryableError`` shall be retried
    *,
    config: RetryConfig | None = None,  # controls logging settings
    giveup: Callable[[AttemptOutcome], object | None] = no_giveup,  # stop retrying based on domain-specific logic
    before_attempt: Callable[[Retry], int] = before_attempt_noop,  # e.g. wait due to internal backpressure
    after_attempt: Callable[[AttemptOutcome], None] = after_attempt_log_failure,  # e.g. record metrics and/or custom logging
    on_retryable_error: Callable[[AttemptOutcome], None] = noop,  # e.g. count failures (RetryableError) caught by retry loop
    on_exhaustion: Callable[[AttemptOutcome], _T] = on_exhaustion_raise,  # raise error or return fallback value
    log: logging.Logger | None = None,  # set this to ``None`` to disable logging
) -> _T:
    """Async version of call_with_retries(); awaits ``fn`` and uses non-blocking sleep."""

    config = _DEFAULT_RETRY_CONFIG if config is None else config
    rng: random.Random | None = None
    retry_count: int = 0
    curr_max_sleep_nanos: int = policy.initial_max_sleep_nanos
    previous_outcomes: tuple[AttemptOutcome, ...] = ()  # for safety pass *immutable* deque to callbacks
    timing: RetryTiming = policy.timing
    sleep: Callable[[int, Retry], Awaitable] = timing.sleep_async
    is_terminated: Callable[[Retry], bool] = timing.is_terminated
    mono_nanos: Callable[[], int] = timing.monotonic_ns
    call_start_nanos: Final[int] = mono_nanos()
    while True:
        before_attempt_nanos: int = mono_nanos() if retry_count != 0 else call_start_nanos
        retry: Retry = Retry(
            retry_count, call_start_nanos, before_attempt_nanos, before_attempt_nanos, policy, config, log, previous_outcomes
        )
        try:
            if before_attempt is not before_attempt_noop:
                before_attempt_sleep_nanos: int = before_attempt(retry)
                assert before_attempt_sleep_nanos >= 0, before_attempt_sleep_nanos
                if before_attempt_sleep_nanos > 0:
                    await sleep(before_attempt_sleep_nanos, retry)
                retry = Retry(
                    retry_count, call_start_nanos, before_attempt_nanos, mono_nanos(), policy, config, log, previous_outcomes
                )
            timing.on_before_attempt(retry)
            result: _T = await fn(retry)  # Call the target function and supply retry attempt number and other metadata
            if after_attempt is not after_attempt_log_failure:
                elapsed_nanos: int = mono_nanos() - call_start_nanos
                outcome: AttemptOutcome = AttemptOutcome(retry, True, False, False, None, elapsed_nanos, 0, result)
                after_attempt(outcome)
            return result
        except RetryableError as retryable_error:
            elapsed_nanos = mono_nanos() - call_start_nanos
            giveup_reason: object | None = None
            sleep_nanos: int = 0
            if on_retryable_error is not noop:
                on_retryable_error(
                    AttemptOutcome(retry, False, False, False, None, elapsed_nanos, sleep_nanos, retryable_error)
                )
            if retry_count < policy.max_retries and elapsed_nanos < policy.max_elapsed_nanos and not is_terminated(retry):
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


def make_timing_from_async(termination_event: asyncio.Event | None) -> RetryTiming:
    """Convenience factory that creates a Timing that performs async termination when ``termination_event`` is set."""
    if termination_event is None:
        return RetryTiming()

    def _is_terminated(retry: Retry) -> bool:
        return termination_event.is_set()

    async def _sleep_async(sleep_nanos: int, retry: Retry) -> None:
        try:
            await asyncio.wait_for(termination_event.wait(), timeout=sleep_nanos / 1_000_000_000)
        except asyncio.TimeoutError:
            pass  # expected

    return RetryTiming(is_terminated=_is_terminated, sleep_async=_sleep_async)


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
def decorrelated_jitter_backoff_strategy(context: BackoffContext) -> tuple[int, int]:
    """Decorrelated-jitter picks random sleep in [base, prev*3] with cap; next state is sleep."""
    retry: Retry = context.retry
    policy: RetryPolicy = retry.policy
    prev_sleep_nanos: int = policy.min_sleep_nanos if retry.count <= 0 else context.curr_max_sleep_nanos
    upper_bound: int = max(prev_sleep_nanos * 3, policy.min_sleep_nanos)
    sample: int = context.rng.randint(policy.min_sleep_nanos, upper_bound)
    sleep_nanos: int = min(policy.max_sleep_nanos, sample)
    return sleep_nanos, sleep_nanos


def exception_driven_backoff_strategy(retry_after: Callable[[RetryableError], int]) -> BackoffStrategy:
    """Returns a strategy whose sleep duration is derived from RetryableError, for example RetryableError.attachment."""

    def _strategy(context: BackoffContext) -> tuple[int, int]:
        sleep_nanos: int = max(0, retry_after(context.retryable_error))
        return sleep_nanos, context.curr_max_sleep_nanos

    return _strategy


def retry_after_or_fallback_strategy(
    *,
    retry_after: Callable[[BackoffContext], int | None] = lambda backoff_context: getattr(
        backoff_context.retryable_error, "retry_after_nanos", None
    ),
    fallback: BackoffStrategy = full_jitter_backoff_strategy,
    max_jitter_nanos: int = 100 * 1_000_000,  # 100ms
    honor_max_elapsed_secs: bool = True,
) -> BackoffStrategy:
    """Returns a BackoffStrategy that honors RetryableError.retry_after_nanos if present (or the result of a custom
    ``retry_after`` callback), else delegates to fallback strategy. When retry_after_nanos is present, adds a random value in
    [0, max_jitter_nanos] as jitter. honor_max_elapsed_secs clamps the final result.

    Interprets RetryableError.retry_after_nanos as a relative sleep duration (in nanoseconds) before the next attempt.

    For example, this can be used to handle the Retry-After header of HTTP 429 "Too Many Requests" responses.
    """
    max_jitter_nanos = max(0, max_jitter_nanos)

    def _strategy(context: BackoffContext) -> tuple[int, int]:
        curr_max_sleep_nanos: int = context.curr_max_sleep_nanos
        retry_after_nanos: int | None = retry_after(context)
        if retry_after_nanos is None:
            sleep_nanos, curr_max_sleep_nanos = fallback(context)
        else:
            retry_after_nanos = max(0, retry_after_nanos)
            retry_after_nanos = max(retry_after_nanos, context.retry.policy.min_sleep_nanos)
            retry_after_nanos += context.rng.randint(0, max_jitter_nanos)
            sleep_nanos = retry_after_nanos
        if honor_max_elapsed_secs:
            remaining_nanos: int = max(0, context.retry.policy.max_elapsed_nanos - context.elapsed_nanos)
            sleep_nanos = min(sleep_nanos, remaining_nanos)
        return sleep_nanos, curr_max_sleep_nanos

    return _strategy


def retry_after_backoff_strategy(
    *,
    retry_after: Callable[[BackoffContext], int | None] = lambda backoff_context: getattr(
        backoff_context.retryable_error, "retry_after_nanos", None
    ),
    delegate: BackoffStrategy = full_jitter_backoff_strategy,
    max_jitter_nanos: int = 100 * 1_000_000,  # 100ms
    honor_max_elapsed_secs: bool = True,
) -> BackoffStrategy:
    """Returns a BackoffStrategy that combines RetryableError.retry_after_nanos if present (or the result of a custom
    ``retry_after`` callback), and a delegate strategy; the combined sleep duration is the maximum of the delegate's sleep
    duration and retry_after_nanos (after applying jitter). When retry_after_nanos is present, adds a random value in [0,
    max_jitter_nanos] as jitter. honor_max_elapsed_secs clamps the final result.

    Interprets RetryableError.retry_after_nanos as a relative sleep duration (in nanoseconds) before the next attempt.
    """
    max_jitter_nanos = max(0, max_jitter_nanos)

    def _strategy(context: BackoffContext) -> tuple[int, int]:
        sleep_nanos: int
        sleep_nanos, curr_max_sleep_nanos = delegate(context)
        retry_after_nanos: int | None = retry_after(context)
        if retry_after_nanos is not None:
            retry_after_nanos = max(0, retry_after_nanos)
            retry_after_nanos = max(retry_after_nanos, context.retry.policy.min_sleep_nanos)
            retry_after_nanos += context.rng.randint(0, max_jitter_nanos)
            sleep_nanos = max(sleep_nanos, retry_after_nanos)
        if honor_max_elapsed_secs:
            remaining_nanos: int = max(0, context.retry.policy.max_elapsed_nanos - context.elapsed_nanos)
            sleep_nanos = min(sleep_nanos, remaining_nanos)
        return sleep_nanos, curr_max_sleep_nanos

    return _strategy


def jitter_backoff_strategy(
    delegate: BackoffStrategy = full_jitter_backoff_strategy,
    *,
    max_jitter_nanos: int = 100 * 1_000_000,  # 100ms
) -> BackoffStrategy:
    """Returns a BackoffStrategy that adds random jitter to another strategy."""
    max_jitter_nanos = max(0, max_jitter_nanos)

    def _strategy(context: BackoffContext) -> tuple[int, int]:
        sleep_nanos, curr_max_sleep_nanos = delegate(context)
        sleep_nanos += context.rng.randint(0, max_jitter_nanos)
        return sleep_nanos, curr_max_sleep_nanos

    return _strategy


def max_elapsed_backoff_strategy(delegate: BackoffStrategy = full_jitter_backoff_strategy) -> BackoffStrategy:
    """Returns a BackoffStrategy that delegates to another strategy, and honors retry.policy.max_elapsed_secs."""

    def _strategy(context: BackoffContext) -> tuple[int, int]:
        sleep_nanos, curr_max_sleep_nanos = delegate(context)
        remaining_nanos: int = max(0, context.retry.policy.max_elapsed_nanos - context.elapsed_nanos)
        sleep_nanos = min(sleep_nanos, remaining_nanos)
        return sleep_nanos, curr_max_sleep_nanos

    return _strategy


def random_backoff_strategy(context: BackoffContext) -> tuple[int, int]:
    policy: RetryPolicy = context.retry.policy
    sleep_nanos: int = context.rng.randint(policy.min_sleep_nanos, policy.max_sleep_nanos)
    return sleep_nanos, context.curr_max_sleep_nanos


def no_jitter_exponential_backoff_strategy(context: BackoffContext) -> tuple[int, int]:
    policy: RetryPolicy = context.retry.policy
    curr_max_sleep_nanos = context.curr_max_sleep_nanos
    sleep_nanos: int = curr_max_sleep_nanos
    curr_max_sleep_nanos = round(curr_max_sleep_nanos * policy.exponential_base)  # exponential backoff
    curr_max_sleep_nanos = min(curr_max_sleep_nanos, policy.max_sleep_nanos)  # ... with cap for next attempt
    return sleep_nanos, curr_max_sleep_nanos


def fixed_backoff_strategy(sleep_nanos: int) -> BackoffStrategy:
    """Returns a strategy that always sleeps a fixed number of nanoseconds."""
    sleep_nanos = max(0, sleep_nanos)

    def _strategy(context: BackoffContext) -> tuple[int, int]:
        return sleep_nanos, context.curr_max_sleep_nanos

    return _strategy


def linear_backoff_strategy(
    *,
    start_sleep_nanos: int = 0,
    increment_sleep_nanos: int = 10 * 1_000_000_000,
    max_sleep_nanos: int = 60 * 1_000_000_000,
) -> BackoffStrategy:

    def _strategy(context: BackoffContext) -> tuple[int, int]:
        sleep_nanos = start_sleep_nanos + (increment_sleep_nanos * context.retry.count)
        sleep_nanos = max(0, min(sleep_nanos, max_sleep_nanos))
        return sleep_nanos, context.curr_max_sleep_nanos

    return _strategy


def chained_backoff_strategy(strategies: Iterable[BackoffStrategy]) -> BackoffStrategy:
    """Returns a BackoffStrategy that walks a list of other strategies by retry count, then sticks to the last strategy."""
    strategies = tuple(strategies)
    if not strategies:
        raise ValueError("chained_backoff_strategy requires at least one strategy")

    def _strategy(context: BackoffContext) -> tuple[int, int]:
        strategy = strategies[min(context.retry.count, len(strategies) - 1)]
        return strategy(context)

    return _strategy


def sum_backoff_strategy(strategies: Iterable[BackoffStrategy]) -> BackoffStrategy:
    """Returns a BackoffStrategy that sums the sleeps of multiple other strategies."""
    strategies = tuple(strategies)

    def _strategy(context: BackoffContext) -> tuple[int, int]:
        sleep_nanos_sum = 0
        curr_max_sleep_nanos = context.curr_max_sleep_nanos
        for strategy in strategies:
            sleep_nanos, curr_max_sleep_nanos = strategy(context.copy(curr_max_sleep_nanos=curr_max_sleep_nanos))
            sleep_nanos_sum += sleep_nanos
        return sleep_nanos_sum, curr_max_sleep_nanos

    return _strategy


#############################################################################
class RetryIntegrationExamples(unittest.TestCase):
    def demo_circuit_breaker(self) -> None:
        """Demonstrates how to integrate a circuit breaker."""

        import subprocess
        from typing import (
            TypeVar,
            cast,
        )

        import pybreaker  # optional third-party ``pybreaker`` circuit breakers; see https://github.com/danielfm/pybreaker

        from bzfs_main.util.retry import (
            RetryableError,
            RetryPolicy,
            call_with_retries,
        )

        breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)
        # pybreaker default semantics: Calls pass through while the circuit is "closed". After fail_max failures, the circuit
        # "opens" and subsequently fails fast with CircuitBreakerError until reset_timeout elapses, at which point a trial
        # call is allowed to either close or re-open the circuit.
        # Also see https://martinfowler.com/bliki/CircuitBreaker.html

        def unreliable_operation(retry: Retry) -> str:
            # return run_some_ssh_cmd(retry)  # may raise TimeoutError, CalledProcessError, OSError, etc.
            if retry.count < 100:
                raise OSError("temporary failure connecting to foo.example.com")
            return "ok"

        _T = TypeVar("_T")

        def circuit_breaker(fn: Callable[[Retry], _T], breaker: pybreaker.CircuitBreaker) -> Callable[[Retry], _T]:

            def _fn(retry: Retry) -> _T:
                try:
                    return cast(_T, breaker.call(fn, retry))
                except pybreaker.CircuitBreakerError as exc:
                    raise RetryableError(display_msg="circuit breaker open") from exc
                except (TimeoutError, subprocess.CalledProcessError, OSError) as exc:
                    raise RetryableError(display_msg=type(exc).__name__) from exc

            return _fn

        def retry_after_circuit_breaker(breaker: pybreaker.CircuitBreaker) -> Callable[[BackoffContext], int | None]:

            def _retry_after(backoff_context: BackoffContext) -> int | None:
                return (
                    int(breaker.reset_timeout * 1_000_000_000)
                    if isinstance(backoff_context.retryable_error.__cause__, pybreaker.CircuitBreakerError)
                    else None
                )

            return _retry_after

        retry_policy = RetryPolicy(
            max_sleep_secs=60,
            max_elapsed_secs=600,
            backoff_strategy=retry_after_or_fallback_strategy(retry_after=retry_after_circuit_breaker(breaker)),
        )
        log = logging.getLogger(__name__)
        _result: str = call_with_retries(
            fn=circuit_breaker(unreliable_operation, breaker),
            policy=retry_policy,
            log=log,
        )

    def demo_rate_limits_and_retry_after(self) -> None:
        """Demonstrates how to integrate rate limits and honoring a Retry-After delay."""
        import subprocess

        import limits  # optional third-party ``limits`` library for rate limiting; see https://github.com/alisaifee/limits

        from bzfs_main.util.retry import (
            RetryableError,
            RetryPolicy,
            call_with_retries,
        )

        limiter = limits.strategies.MovingWindowRateLimiter(limits.storage.MemoryStorage())
        limit = limits.parse("5/second")
        # ``limits`` library default semantics of moving-window algorithm: each key tracks timestamped hits and allows at
        # most 5 hits per 1-second window. The limiter rejects a hit when the Nth most-recent entry is still within the
        # window (i.e., it would exceed the limit).

        def unreliable_operation(retry: Retry) -> str:
            try:
                # return run_some_ssh_cmd(retry)  # may raise TimeoutError, CalledProcessError, etc.
                if retry.count == 0:
                    exc = OSError("HTTP 429 Too Many Requests")
                    setattr(exc, "retry_after_secs", 2.0)  # noqa: B010
                    raise exc
                if retry.count < 100:
                    raise OSError("temporary failure connecting to foo.example.com")
                return "ok"
            except (TimeoutError, subprocess.CalledProcessError, OSError) as exc:
                raise RetryableError(display_msg=type(exc).__name__) from exc

        def retry_after(backoff_context: BackoffContext) -> int | None:
            retry_after_secs: object = getattr(backoff_context.retryable_error.__cause__, "retry_after_secs", None)
            if isinstance(retry_after_secs, (int, float)):
                return max(0, int(retry_after_secs * 1_000_000_000))  # relative delay in nanoseconds
            return None

        def rate_limited(
            limiter: limits.strategies.RateLimiter, limit: limits.RateLimitItem, *identifiers: str
        ) -> Callable[[Retry], int]:

            def _before_attempt(retry: Retry) -> int:
                if limiter.test(limit, *identifiers) and limiter.hit(limit, *identifiers):
                    return 0  # local limiter grants one send token
                window = limiter.get_window_stats(limit, *identifiers)
                return max(0, int(1_000_000_000 * (window.reset_time - time.time())))

            return _before_attempt

        retry_policy = RetryPolicy(
            max_elapsed_secs=600,
            backoff_strategy=retry_after_or_fallback_strategy(retry_after=retry_after),
        )
        log = logging.getLogger(__name__)
        _result: str = call_with_retries(
            fn=unreliable_operation,
            policy=retry_policy,
            before_attempt=rate_limited(limiter, limit, "ssh", "host1.example.com"),
            log=log,
        )

    def demo_prometheus_metrics(self) -> None:
        """Demonstrates collecting retry latency/failure metrics via an `after_attempt` Prometheus textfile exporter."""

        import prometheus_client  # optional third-party library; see https://github.com/prometheus/client_python

        from bzfs_main.util.retry import (
            RetryableError,
            RetryPolicy,
            call_with_retries,
            multi_after_attempt,
        )

        # Production services typically expose metrics to Prometheus via either an HTTP /metrics endpoint, or by writing a
        # `.prom` file for the node_exporter textfile collector. This example uses the textfile approach.
        registry = prometheus_client.CollectorRegistry()
        service: Final[str] = "widgets-api"
        method: Final[str] = "GET"
        route: Final[str] = "/v1/widgets/{id}"  # keep low-cardinality labels; avoid full URLs / user ids

        call_outcomes_total = prometheus_client.Counter(
            "web_client_call_outcomes_total",
            "Number of calls, labeled by outcome (success/exhausted/giveup/terminated).",
            ["service", "method", "route", "outcome"],
            registry=registry,
        )
        call_duration_seconds = prometheus_client.Histogram(
            "web_client_call_duration_seconds",
            "Total time spent in call_with_retries() until final outcome.",
            ["service", "method", "route", "outcome"],
            # Typical production-ish latency buckets (seconds).
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
            registry=registry,
        )
        retries_per_call = prometheus_client.Histogram(
            "web_client_retries_per_call",
            "Retries per call_with_retries() invocation (retry.count on final attempt).",
            ["service", "method", "route", "outcome"],
            buckets=(0, 1, 2, 3, 4, 5, 8, 13),
            registry=registry,
        )

        attempt_outcomes_total = prometheus_client.Counter(
            "web_client_attempt_outcomes_total",
            "Number of attempts (fn(retry) calls), labeled by outcome.",
            ["service", "method", "route", "outcome"],
            registry=registry,
        )
        attempt_duration_seconds = prometheus_client.Histogram(
            "web_client_attempt_duration_seconds",
            "Time spent in a single fn(retry) attempt.",
            ["service", "method", "route", "outcome"],
            buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
            registry=registry,
        )
        backoff_sleep_seconds = prometheus_client.Histogram(
            "web_client_backoff_sleep_seconds",
            "Backoff sleep duration between attempts (0 on non-sleeping outcomes).",
            ["service", "method", "route"],
            buckets=(0.0, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0),
            registry=registry,
        )
        retryable_errors_total = prometheus_client.Counter(
            "web_client_retryable_errors_total",
            "Retryable failures by error type (keep low-cardinality).",
            ["service", "method", "route", "error_type"],
            registry=registry,
        )

        labels: dict[str, str] = {"service": service, "method": method, "route": route}

        # In production you'd usually export periodically from a background task/thread, but this example keeps everything
        # within `after_attempt` for simplicity.
        prom_path: str = "/var/lib/node_exporter/textfile_collector/web_client_retry.prom"
        export_interval_nanos: int = 5 * 1_000_000_000
        last_export_time_nanos: int = 0

        def after_attempt_prometheus(outcome: AttemptOutcome) -> None:
            nonlocal last_export_time_nanos

            # Outcome labels:
            # - per-attempt: success / retryable_error / exhausted / giveup / terminated
            # - per-call (final only): success / exhausted / giveup / terminated
            attempt_outcome_label: str
            if outcome.is_success:
                attempt_outcome_label = "success"
            elif outcome.is_exhausted:
                if outcome.is_terminated:
                    attempt_outcome_label = "terminated"
                elif outcome.giveup_reason is not None:
                    attempt_outcome_label = "giveup"
                else:
                    attempt_outcome_label = "exhausted"
            else:
                attempt_outcome_label = "retryable_error"

            attempt_outcomes_total.labels(**labels, outcome=attempt_outcome_label).inc()
            attempt_duration_seconds.labels(**labels, outcome=attempt_outcome_label).observe(
                outcome.attempt_elapsed_nanos() / 1_000_000_000
            )
            backoff_sleep_seconds.labels(**labels).observe(outcome.sleep_nanos / 1_000_000_000)

            if not outcome.is_success:
                assert isinstance(outcome.result, RetryableError)
                retryable_error: RetryableError = outcome.result
                cause: BaseException | None = retryable_error.__cause__
                error_type: str = type(cause).__name__ if cause is not None else type(retryable_error).__name__
                retryable_errors_total.labels(**labels, error_type=error_type).inc()

            if outcome.is_success or outcome.is_exhausted:
                call_outcome_label: str
                if outcome.is_success:
                    call_outcome_label = "success"
                elif outcome.is_terminated:
                    call_outcome_label = "terminated"
                elif outcome.giveup_reason is not None:
                    call_outcome_label = "giveup"
                else:
                    call_outcome_label = "exhausted"
                call_outcomes_total.labels(**labels, outcome=call_outcome_label).inc()
                call_duration_seconds.labels(**labels, outcome=call_outcome_label).observe(
                    outcome.elapsed_nanos / 1_000_000_000
                )
                retries_per_call.labels(**labels, outcome=call_outcome_label).observe(outcome.retry.count)

            now_nanos: int = time.monotonic_ns()
            if now_nanos - last_export_time_nanos >= export_interval_nanos:
                last_export_time_nanos = now_nanos
                # Export metrics in Prometheus text exposition format for the node_exporter textfile collector
                prometheus_client.exposition.write_to_textfile(prom_path, registry)

        def web_call(retry: Retry) -> str:
            """Executes a single outbound request attempt; maps transient errors into RetryableError for retry."""
            import http.client
            import socket

            try:
                # Example only:
                # status_code, body = http_get("https://widgets.example.com/v1/widgets/123", timeout=..., headers=...)
                if retry.count < 2:
                    raise socket.timeout("upstream timed out")
                return "ok"
            except (socket.timeout, TimeoutError, http.client.HTTPException, OSError) as exc:
                # Keep the display_msg low-cardinality; use it for logs, not as a metric label.
                raise RetryableError(display_msg=type(exc).__name__) from exc

        retry_policy = RetryPolicy(
            max_retries=5,
            min_sleep_secs=0.05,
            initial_max_sleep_secs=0.05,
            max_sleep_secs=1,
            max_elapsed_secs=10,
        )
        after_attempt = multi_after_attempt([after_attempt_log_failure, after_attempt_prometheus])
        log = logging.getLogger(__name__)
        _result: str = call_with_retries(fn=web_call, policy=retry_policy, after_attempt=after_attempt, log=log)


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

        mock_sleep_async = AsyncMock(side_effect=fake_sleep_async)
        retry_policy = retry_policy.copy(timing=RetryTiming().copy(sleep_async=mock_sleep_async))
        actual = await call_with_retries_async(
            fn,
            policy=retry_policy,
            config=RetryConfig(),
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
        retry_policy = retry_policy.copy(timing=RetryTiming().copy(sleep_async=mock_sleep_async))
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
                config=RetryConfig(),
                on_retryable_error=on_retryable_error,
                log=None,
            )

        self.assertEqual([0, 1], seen_counts)
        self.assertEqual([False, False], seen_exhausted_flags)

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


#############################################################################
class TestMiscBackoffStrategies(unittest.TestCase):

    def test_linear_backoff_strategy(self) -> None:
        strategy = linear_backoff_strategy(
            start_sleep_nanos=1_000_000_000, increment_sleep_nanos=2_000_000_000, max_sleep_nanos=5_000_000_000
        )
        sleeps, mock_sleep = self._run_and_collect_sleep_nanos(backoff_strategy=strategy, failures=4)
        self.assertEqual([1_000_000_000, 3_000_000_000, 5_000_000_000, 5_000_000_000], sleeps)
        self.assertEqual(4, mock_sleep.call_count)

    def test_exception_driven_backoff_strategy_receives_retryable_error_with_cause(self) -> None:
        strategy = exception_driven_backoff_strategy(
            lambda err: 7_000_000_000 if isinstance(err.__cause__, ValueError) else 11_000_000_000
        )
        sleeps, mock_sleep = self._run_and_collect_sleep_nanos(backoff_strategy=strategy, failures=1, with_cause=True)
        self.assertEqual([7_000_000_000], sleeps)
        self.assertEqual(1, mock_sleep.call_count)

    def test_exception_driven_backoff_strategy_receives_retryable_error_without_cause(self) -> None:
        strategy = exception_driven_backoff_strategy(
            lambda err: 7_000_000_000 if isinstance(err.__cause__, ValueError) else 11_000_000_000
        )
        sleeps, mock_sleep = self._run_and_collect_sleep_nanos(backoff_strategy=strategy, failures=1, with_cause=False)
        self.assertEqual([11_000_000_000], sleeps)
        self.assertEqual(1, mock_sleep.call_count)

    def test_retry_after_or_fallback_strategy_delegates_when_retry_after_is_none(self) -> None:
        policy = RetryPolicy(max_retries=0, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=10)
        retry = Retry(
            count=0,
            call_start_time_nanos=0,
            before_attempt_start_time_nanos=0,
            attempt_start_time_nanos=0,
            policy=policy,
            config=RetryConfig(),
            log=None,
            previous_outcomes=(),
        )
        err = RetryableError("fail")
        rng = random.Random(0)

        delegate = MagicMock(return_value=(111, 222))
        backoff_strategy: BackoffStrategy = retry_after_or_fallback_strategy(fallback=delegate)
        context = BackoffContext(retry, 123, rng, 0, err)
        sleep_nanos, next_curr_max = backoff_strategy(context)
        self.assertEqual(111, sleep_nanos)
        self.assertEqual(222, next_curr_max)
        delegate.assert_called_once()
        (delegate_context,) = delegate.call_args.args
        self.assertIs(retry, delegate_context.retry)
        self.assertEqual(123, delegate_context.curr_max_sleep_nanos)
        self.assertIs(rng, delegate_context.rng)
        self.assertEqual(0, delegate_context.elapsed_nanos)
        self.assertIs(err, delegate_context.retryable_error)

    def test_retry_after_or_fallback_strategy_does_not_delegate_when_retry_after_is_present(self) -> None:
        policy = RetryPolicy(max_retries=0, min_sleep_secs=10, initial_max_sleep_secs=0, max_sleep_secs=10)
        retry = Retry(
            count=0,
            call_start_time_nanos=0,
            before_attempt_start_time_nanos=0,
            attempt_start_time_nanos=0,
            policy=policy,
            config=RetryConfig(),
            log=None,
            previous_outcomes=(),
        )
        err = RetryableError("fail")
        setattr(err, "retry_after_nanos", 500)  # noqa: B010
        rng = random.Random(0)

        delegate = MagicMock(side_effect=AssertionError("delegate must not be called"))
        backoff_strategy: BackoffStrategy = retry_after_or_fallback_strategy(fallback=delegate)
        sleep_nanos, next_curr_max = backoff_strategy(BackoffContext(retry, 123, rng, 0, err))
        max_jitter_nanos: int = 100 * 1_000_000
        self.assertGreaterEqual(sleep_nanos, policy.min_sleep_nanos)
        self.assertLessEqual(sleep_nanos, policy.min_sleep_nanos + max_jitter_nanos)
        self.assertEqual(123, next_curr_max)
        delegate.assert_not_called()

    def test_retry_after_backoff_strategy_uses_max_of_delegate_and_retry_after_and_honors_max_elapsed(self) -> None:
        policy = RetryPolicy(
            max_retries=0,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=10,
            max_elapsed_secs=1,
        )
        retry = Retry(
            count=0,
            call_start_time_nanos=0,
            before_attempt_start_time_nanos=0,
            attempt_start_time_nanos=0,
            policy=policy,
            config=RetryConfig(),
            log=None,
            previous_outcomes=(),
        )
        elapsed_nanos: int = policy.max_elapsed_nanos - 300

        for retry_after_nanos, expected_sleep_nanos in [(500, 300), (50, 100)]:
            with self.subTest(retry_after_nanos=retry_after_nanos):
                err = RetryableError("fail")
                setattr(err, "retry_after_nanos", retry_after_nanos)  # noqa: B010
                rng = random.Random(0)
                delegate = MagicMock(return_value=(100, 222))

                backoff_strategy: BackoffStrategy = retry_after_backoff_strategy(
                    retry_after=lambda backoff_context: getattr(backoff_context.retryable_error, "retry_after_nanos", None),
                    delegate=delegate,
                    max_jitter_nanos=0,
                    honor_max_elapsed_secs=True,
                )
                context = BackoffContext(retry, 123, rng, elapsed_nanos, err)
                sleep_nanos, next_curr_max = backoff_strategy(context)
                self.assertEqual(expected_sleep_nanos, sleep_nanos)
                self.assertEqual(222, next_curr_max)
                delegate.assert_called_once()
                (delegate_context,) = delegate.call_args.args
                self.assertIs(retry, delegate_context.retry)
                self.assertEqual(123, delegate_context.curr_max_sleep_nanos)
                self.assertIs(rng, delegate_context.rng)
                self.assertEqual(elapsed_nanos, delegate_context.elapsed_nanos)
                self.assertIs(err, delegate_context.retryable_error)

    def test_jitter_backoff_strategy_adds_jitter_and_preserves_curr_max_sleep_nanos(self) -> None:
        policy = RetryPolicy(max_retries=0, min_sleep_secs=0, initial_max_sleep_secs=0, max_sleep_secs=10)
        retry = Retry(
            count=0,
            call_start_time_nanos=0,
            before_attempt_start_time_nanos=0,
            attempt_start_time_nanos=0,
            policy=policy,
            config=RetryConfig(),
            log=None,
            previous_outcomes=(),
        )
        err = RetryableError("fail")

        for max_jitter_nanos, expected_jitter_nanos in [(10, 7), (-5, 0)]:
            with self.subTest(max_jitter_nanos=max_jitter_nanos):
                rng = cast(random.Random, SequenceRandom([expected_jitter_nanos]))
                delegate = MagicMock(return_value=(100, 222))
                backoff_strategy: BackoffStrategy = jitter_backoff_strategy(
                    delegate=delegate, max_jitter_nanos=max_jitter_nanos
                )
                context = BackoffContext(retry, 123, rng, 0, err)
                sleep_nanos, next_curr_max = backoff_strategy(context)
                self.assertEqual(100 + expected_jitter_nanos, sleep_nanos)
                self.assertEqual(222, next_curr_max)
                delegate.assert_called_once_with(context)

    def test_honor_max_elapsed_backoff_strategy_clamps_sleep_to_remaining_time_budget(self) -> None:
        policy = RetryPolicy(
            max_retries=0,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=10,
            max_elapsed_secs=1,
        )
        retry = Retry(
            count=0,
            call_start_time_nanos=0,
            before_attempt_start_time_nanos=0,
            attempt_start_time_nanos=0,
            policy=policy,
            config=RetryConfig(),
            log=None,
            previous_outcomes=(),
        )
        err = RetryableError("fail")
        rng = random.Random(0)

        delegate = MagicMock(return_value=(500, 222))
        backoff_strategy: BackoffStrategy = max_elapsed_backoff_strategy(delegate=delegate)

        elapsed_nanos = policy.max_elapsed_nanos - 300
        context = BackoffContext(retry, 123, rng, elapsed_nanos, err)
        sleep_nanos, next_curr_max = backoff_strategy(context)
        self.assertEqual(300, sleep_nanos)
        self.assertEqual(222, next_curr_max)
        delegate.assert_called_once()
        (delegate_context,) = delegate.call_args.args
        self.assertIs(retry, delegate_context.retry)
        self.assertEqual(123, delegate_context.curr_max_sleep_nanos)
        self.assertIs(rng, delegate_context.rng)
        self.assertEqual(elapsed_nanos, delegate_context.elapsed_nanos)
        self.assertIs(err, delegate_context.retryable_error)

        delegate.reset_mock()
        delegate.return_value = (100, 333)
        context = BackoffContext(retry, 123, rng, 0, err)
        sleep_nanos, next_curr_max = backoff_strategy(context)
        self.assertEqual(100, sleep_nanos)
        self.assertEqual(333, next_curr_max)
        delegate.assert_called_once()
        (delegate_context,) = delegate.call_args.args
        self.assertIs(retry, delegate_context.retry)
        self.assertEqual(123, delegate_context.curr_max_sleep_nanos)
        self.assertIs(rng, delegate_context.rng)
        self.assertEqual(0, delegate_context.elapsed_nanos)
        self.assertIs(err, delegate_context.retryable_error)

        delegate.reset_mock()
        delegate.return_value = (100, 444)
        elapsed_nanos = policy.max_elapsed_nanos + 1
        context = BackoffContext(retry, 123, rng, elapsed_nanos, err)
        sleep_nanos, next_curr_max = backoff_strategy(context)
        self.assertEqual(0, sleep_nanos)
        self.assertEqual(444, next_curr_max)
        delegate.assert_called_once()
        (delegate_context,) = delegate.call_args.args
        self.assertIs(retry, delegate_context.retry)
        self.assertEqual(123, delegate_context.curr_max_sleep_nanos)
        self.assertIs(rng, delegate_context.rng)
        self.assertEqual(elapsed_nanos, delegate_context.elapsed_nanos)
        self.assertIs(err, delegate_context.retryable_error)

    def test_chained_backoff_strategy(self) -> None:
        chained = chained_backoff_strategy(
            [
                fixed_backoff_strategy(1_000_000_000),
                fixed_backoff_strategy(2_000_000_000),
                fixed_backoff_strategy(3_000_000_000),
            ]
        )
        sleeps, mock_sleep = self._run_and_collect_sleep_nanos(backoff_strategy=chained, failures=4)
        self.assertEqual([1_000_000_000, 2_000_000_000, 3_000_000_000, 3_000_000_000], sleeps)
        self.assertEqual(4, mock_sleep.call_count)

    def test_sum_backoff_strategy(self) -> None:
        combined = sum_backoff_strategy([fixed_backoff_strategy(1_000_000_000), fixed_backoff_strategy(2_000_000_000)])
        sleeps, mock_sleep = self._run_and_collect_sleep_nanos(backoff_strategy=combined, failures=2)
        self.assertEqual([3_000_000_000, 3_000_000_000], sleeps)
        self.assertEqual(2, mock_sleep.call_count)

    def _run_and_collect_sleep_nanos(
        self,
        *,
        backoff_strategy: BackoffStrategy,
        failures: int,
        rng: object | None = None,
        with_cause: bool = True,
    ) -> tuple[list[int], MagicMock]:
        """Runs a few retries and collects AttemptOutcome.sleep_nanos for each retry."""
        retry_policy = RetryPolicy(
            max_retries=failures,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=10,
            backoff_strategy=backoff_strategy,
        )
        sleep_nanos: list[int] = []

        def fn(retry: Retry) -> str:
            if retry.count < failures:
                if with_cause:
                    raise RetryableError("fail") from ValueError("boom")
                raise RetryableError("fail")
            return "ok"

        def after_attempt(outcome: AttemptOutcome) -> None:
            if not outcome.is_success and not outcome.is_exhausted:
                sleep_nanos.append(outcome.sleep_nanos)

        rng_patch = patch("bzfs_main.util.retry._thread_local_rng", return_value=rng) if rng is not None else None
        mock_sleep = MagicMock()
        retry_policy = retry_policy.copy(timing=RetryTiming().copy(sleep=mock_sleep))
        config = RetryConfig()
        if rng_patch is None:
            call_with_retries(fn, policy=retry_policy, config=config, after_attempt=after_attempt, log=None)
        else:
            with rng_patch:
                call_with_retries(fn, policy=retry_policy, config=config, after_attempt=after_attempt, log=None)
        return sleep_nanos, mock_sleep

    def test_call_with_retries_using_decorrelated_jitter_as_custom_backoff_strategy(self) -> None:
        """Ensures decorrelated-jitter can be used as a custom backoff_strategy."""
        seen_state: list[int] = []

        def recording_strategy(context: BackoffContext) -> tuple[int, int]:
            seen_state.append(context.curr_max_sleep_nanos)
            return decorrelated_jitter_backoff_strategy(context)

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
            self.assertIsNone(outcome.giveup_reason)
            if not outcome.is_success and not outcome.is_exhausted:
                retry_sleep_nanos.append(outcome.sleep_nanos)
                self.assertIsInstance(outcome.result, RetryableError)
                self.assertGreaterEqual(outcome.elapsed_nanos, 0)
            self.assertIsNone(outcome.retry.log)

        rng = SequenceRandom([5, 6])
        with patch("bzfs_main.util.retry._thread_local_rng", return_value=rng):
            actual = call_with_retries(
                fn,
                policy=retry_policy,
                config=RetryConfig(),
                after_attempt=after_attempt,
                log=None,
            )

        self.assertEqual("ok", actual)
        self.assertEqual([0, 1, 2], calls)
        self.assertEqual([retry_policy.initial_max_sleep_nanos, 5], seen_state)
        self.assertEqual([5, 6], retry_sleep_nanos)


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
