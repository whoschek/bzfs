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
"""Examples demonstrating use of call_with_retries(), backoff strategies, integration with circuit breakers, rate limiting
and retry-after, Prometheus metrics, etc."""

from __future__ import (
    annotations,
)
import logging
import random
import time
import unittest
from collections.abc import (
    Iterable,
)
from typing import (
    Callable,
    Final,
    TypeVar,
    cast,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from bzfs_main.util.retry import (
    AttemptOutcome,
    BackoffContext,
    BackoffStrategy,
    Retry,
    RetryableError,
    RetryPolicy,
    RetryTiming,
    after_attempt_log_failure,
    call_with_retries,
    full_jitter_backoff_strategy,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestMiscBackoffStrategies,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


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


def backoff_from_retryable_error(retry_after: Callable[[RetryableError], int]) -> BackoffStrategy:
    """Returns an immutable strategy whose sleep duration is derived from RetryableError, e.g. RetryableError.attachment."""

    def _strategy(context: BackoffContext) -> tuple[int, int]:
        sleep_nanos: int = retry_after(context.retryable_error)
        return max(0, sleep_nanos), context.curr_max_sleep_nanos

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
    """Returns an (immutable) BackoffStrategy that honors RetryableError.retry_after_nanos if present (or the result of a
    custom ``retry_after`` callback), else delegates to fallback strategy. When retry_after_nanos is present, adds a random
    value in [0, max_jitter_nanos] as jitter. honor_max_elapsed_secs clamps the final result.

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
    """Returns an (immutable) BackoffStrategy that combines RetryableError.retry_after_nanos if present (or the result of a
    custom ``retry_after`` callback), and a delegate strategy; the combined sleep duration is the maximum of the delegate's
    sleep duration and retry_after_nanos (after applying jitter). When retry_after_nanos is present, adds a random value in
    [0, max_jitter_nanos] as jitter. honor_max_elapsed_secs clamps the final result.

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
    """Returns an (immutable) BackoffStrategy that adds random jitter to another strategy."""
    max_jitter_nanos = max(0, max_jitter_nanos)

    def _strategy(context: BackoffContext) -> tuple[int, int]:
        sleep_nanos, curr_max_sleep_nanos = delegate(context)
        sleep_nanos += context.rng.randint(0, max_jitter_nanos)
        return sleep_nanos, curr_max_sleep_nanos

    return _strategy


def max_elapsed_backoff_strategy(delegate: BackoffStrategy = full_jitter_backoff_strategy) -> BackoffStrategy:
    """Returns an immutable BackoffStrategy that delegates to another strategy, and honors retry.policy.max_elapsed_secs."""

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
    curr_max_sleep_nanos: int = context.curr_max_sleep_nanos
    sleep_nanos: int = curr_max_sleep_nanos
    curr_max_sleep_nanos = round(curr_max_sleep_nanos * policy.exponential_base)  # exponential backoff
    curr_max_sleep_nanos = min(curr_max_sleep_nanos, policy.max_sleep_nanos)  # ... with cap for next attempt
    return sleep_nanos, curr_max_sleep_nanos


def fixed_backoff_strategy(sleep_nanos: int) -> BackoffStrategy:
    """Returns an (immutable) BackoffStrategy that always sleeps a fixed number of nanoseconds."""
    sleep_nanos = max(0, sleep_nanos)

    def _strategy(context: BackoffContext) -> tuple[int, int]:
        return sleep_nanos, context.curr_max_sleep_nanos

    return _strategy


def linear_backoff_strategy(
    *,
    start_sleep_nanos: int = 85 * 1_000_000,  # 85ms
    increment_sleep_nanos: int = 1 * 1_111_000_000,  # ~1.1s
    max_sleep_nanos: int = 11 * 1_000_000_000,  # 11s
) -> BackoffStrategy:
    start_sleep_nanos = max(0, start_sleep_nanos)
    increment_sleep_nanos = max(0, increment_sleep_nanos)
    max_sleep_nanos = max(0, max_sleep_nanos)

    def _strategy(context: BackoffContext) -> tuple[int, int]:
        sleep_nanos = start_sleep_nanos + increment_sleep_nanos * context.retry.count
        sleep_nanos = min(sleep_nanos, max_sleep_nanos)
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


#############################################################################
class RetryIntegrationExamples(unittest.TestCase):
    def demo_circuit_breaker(self) -> None:
        """Demonstrates how to integrate a circuit breaker."""

        import subprocess
        from typing import (
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
        )
        log = logging.getLogger(__name__)
        _result: str = call_with_retries(
            fn=circuit_breaker(unreliable_operation, breaker),
            policy=retry_policy,
            backoff=retry_after_or_fallback_strategy(retry_after=retry_after_circuit_breaker(breaker)),
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
        )
        log = logging.getLogger(__name__)
        _result: str = call_with_retries(
            fn=unreliable_operation,
            policy=retry_policy,
            backoff=retry_after_or_fallback_strategy(retry_after=retry_after),
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
class TestMiscBackoffStrategies(unittest.TestCase):

    def test_linear_backoff_strategy(self) -> None:
        strategy = linear_backoff_strategy(
            start_sleep_nanos=1_000_000_000, increment_sleep_nanos=2_000_000_000, max_sleep_nanos=5_000_000_000
        )
        sleeps, mock_sleep = self._run_and_collect_sleep_nanos(backoff_strategy=strategy, failures=4)
        self.assertEqual([1_000_000_000, 3_000_000_000, 5_000_000_000, 5_000_000_000], sleeps)
        self.assertEqual(4, mock_sleep.call_count)

    def test_backoff_from_retryable_error_receives_retryable_error_with_cause(self) -> None:
        strategy = backoff_from_retryable_error(
            lambda err: 7_000_000_000 if isinstance(err.__cause__, ValueError) else 11_000_000_000
        )
        sleeps, mock_sleep = self._run_and_collect_sleep_nanos(backoff_strategy=strategy, failures=1, with_cause=True)
        self.assertEqual([7_000_000_000], sleeps)
        self.assertEqual(1, mock_sleep.call_count)

    def test_backoff_from_retryable_error_receives_retryable_error_without_cause(self) -> None:
        strategy = backoff_from_retryable_error(
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
        if rng_patch is None:
            call_with_retries(fn, policy=retry_policy, backoff=backoff_strategy, after_attempt=after_attempt, log=None)
        else:
            with rng_patch:
                call_with_retries(fn, policy=retry_policy, backoff=backoff_strategy, after_attempt=after_attempt, log=None)
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
                backoff=recording_strategy,
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
