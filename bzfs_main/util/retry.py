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
"""Customizable generic retry framework using jittered exponential backoff with cap.

Purpose:
--------
- Provide a reusable retry helper for transient failures using customizable policy, config and callbacks.
- Centralize backoff, jitter, logging and metrics behavior while keeping call sites compact.
- Prevent accidental retries: the loop retries only when the developer explicitly raises a ``RetryableError``, which reduces
  the risk of retrying non-idempotent operations.
- Provide a thread-safe, fast implementation; avoid shared RNG contention.
- Avoid unnecessary complexity and add zero dependencies beyond the Python standard library. Everything you need is in this
  single Python file.

Usage:
------
- Wrap work in a callable ``fn(retry: Retry)`` and therein raise ``RetryableError`` for failures that should be retried.
- Construct a policy via ``RetryPolicy(...)`` that specifies how ``RetryableError`` shall be retried.
- Invoke ``call_with_retries(fn=fn, policy=policy, log=logger)`` with a standard logging.Logger
- On success, the result of calling ``fn`` is returned.
- By default on exhaustion, call_with_retries() either re-raises the last underlying ``RetryableError.__cause__``, or raises
  ``RetryError`` (wrapping the last ``RetryableError``), like so:
  - if ``RetryPolicy.reraise`` is True and the last ``RetryableError.__cause__`` is not None, re-raise the last
    ``RetryableError.__cause__`` with its original traceback.
  - Otherwise, raise ``RetryError`` (wrapping the last ``RetryableError``, preserving its ``__cause__`` chain).
  - The default is ``RetryPolicy.reraise=True``.

Advanced Configuration:
-----------------------
- Tune ``RetryPolicy`` parameters to control maximum retries, sleep bounds, and elapsed-time budget.
- Use ``RetryConfig`` to control logging settings and async termination events.
- Set ``log=None`` to disable logging, or customize ``info_loglevel`` / ``warning_loglevel`` for structured logs.
- Pass ``termination_event`` via ``RetryConfig`` to support async cancellation between attempts.
- Supply a ``giveup(AttemptOutcome)`` callback to stop retrying based on domain-specific logic (for example, error/status
  codes or parsing stderr), including time-aware decisions or decisions based on the previous N most recent AttemptOutcome
  objects (via AttemptOutcome.retry.previous_outcomes)
- Use the ``any_giveup()`` / ``all_giveup()`` helper to consult more than one callback handler in ``giveup(AttemptOutcome)``.
- Supply an ``on_exhaustion(AttemptOutcome)`` callback to customize behavior when giving up; it may raise an error or return
  a fallback value.

Observability:
--------------
- Supply an ``after_attempt(AttemptOutcome)`` callback to collect per-attempt metrics such as success flag,
  exhausted/terminated state, attempt number, total elapsed duration (in nanoseconds), sleep duration (in nanoseconds), etc.
- ``AttemptOutcome.result`` is either the successful result or the most recent ``RetryableError``, enabling integration with
  metrics and tracing systems without coupling the retry loop to any specific backend.
- Supply an ``after_attempt(AttemptOutcome)`` callback to customize logging 100%, if necessary.
- Use the ``multi_after_attempt()`` helper to invoke more than one callback handler in ``after_attempt(AttemptOutcome)``.

Expert Configuration:
---------------------
- Set ``RetryPolicy.backoff_strategy(BackoffContext)`` to plug in a custom backoff algorithm (e.g., decorrelated-jitter or
  retry-after). The default is full-jitter exponential backoff with cap (aka industry standard).
- Set ``RetryPolicy.max_previous_outcomes > 0`` to pass the N most recent AttemptOutcome objects to callbacks (default is 0).
- If ``RetryPolicy.max_previous_outcomes > 0``, you can use ``RetryableError(..., attachment=...)`` to carry domain-specific
  state from a failed attempt to the next attempt via ``retry.previous_outcomes``. This pattern helps if attempt N+1 is a
  function of attempt N or all prior attempts (e.g., switching endpoints or resuming from an offset).
- Use ``RetryTemplate`` as a 'bag of knobs' configuration template for functions that shall be retried in similar ways.
- Or package up all knobs plus a ``fn(retry: Retry)`` function into a self-contained auto-retrying higher level function by
  constructing a ``RetryTemplate`` object (which is a ``Callable`` function itself).
- To keep calling code retry-transparent, set ``RetryPolicy.reraise=True`` (the default) *and* raise retryable failures as
  ``raise RetryableError(...) from exc``. Client code now won't notice whether call_with_retries is used or not.
- To make exhaustion observable to calling code, set ``RetryPolicy.reraise=False``: by default call_with_retries() now always
  raises ``RetryError`` (wrapping the last ``RetryableError``) on exhaustion, so callers now catch ``RetryError`` and can
  inspect the last underlying exception via ``err.outcome``, ``err.__cause__``, and even ``err.__cause__.__cause__`` when
  present.

Example Usage:
--------------
    import logging
    from bzfs_main.util.retry import Retry, RetryPolicy, RetryableError, call_with_retries

    def unreliable_operation(retry: Retry) -> str:
        try:
            if retry.count < 3:
                raise ValueError("temporary failure connecting to foo.example.com")
            return "ok"
        except ValueError as exc:
            # Preserve the underlying cause for correct error propagation and logging
            raise RetryableError(display_msg="connect") from exc

    retry_policy = RetryPolicy(
        max_retries=10,
        min_sleep_secs=0,
        initial_max_sleep_secs=0.125,
        max_sleep_secs=10,
        max_elapsed_secs=60,
    )
    log = logging.getLogger(__name__)
    result: str = call_with_retries(fn=unreliable_operation, policy=retry_policy, log=log)
    print(result)

    # Sample log output:
    # INFO:Retrying connect [1/10] in 8.79ms ...
    # INFO:Retrying connect [2/10] in 60.1ms ...
    # INFO:Retrying connect [3/10] in 192ms ...
    # ok

Background:
-----------
For background on exponential backoff and jitter, see for example
https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter
"""

from __future__ import (
    annotations,
)
import argparse
import dataclasses
import logging
import random
import threading
import time
from collections.abc import (
    Iterable,
    Mapping,
    Sequence,
)
from dataclasses import (
    dataclass,
)
from typing import (
    Any,
    Callable,
    Final,
    Generic,
    NamedTuple,
    NoReturn,
    TypeVar,
    Union,
    final,
)

from bzfs_main.util.utils import (
    human_readable_duration,
)

# constants:
INFINITY_MAX_RETRIES: Final[int] = 2**90 - 1  # a number that's essentially infinity for all practical retry purposes


#############################################################################
def no_giveup(outcome: AttemptOutcome) -> object | None:
    """Default implementation of ``giveup`` callback for call_with_retries(); never gives up; returning anything other than
    ``None`` indicates to give up retrying; thread-safe."""
    return None  # don't give up retrying


def after_attempt_log_failure(outcome: AttemptOutcome) -> None:
    """Default implementation of ``after_attempt`` callback for call_with_retries(); performs simple logging of retry attempt
    failures; thread-safe."""
    retry: Retry = outcome.retry
    if outcome.is_success or retry.log is None or not retry.config.enable_logging:
        return
    policy: RetryPolicy = retry.policy
    config: RetryConfig = retry.config
    log: logging.Logger = retry.log
    assert isinstance(outcome.result, RetryableError)
    retryable_error: RetryableError = outcome.result
    if not outcome.is_exhausted:
        if log.isEnabledFor(config.info_loglevel):  # Retrying X in Y ms ...
            m1: str = config.format_msg(config.display_msg, retryable_error)
            m2: str = config.format_pair(retry.count + 1, policy.max_retries)
            m3: str = config.format_duration(outcome.sleep_nanos)
            log.log(config.info_loglevel, "%s", f"{m1}{m2} in {m3}{config.dots}", extra=config.extra)
    else:
        if policy.max_retries > 0 and log.isEnabledFor(config.warning_loglevel) and not outcome.is_terminated:
            reason: str = "" if outcome.giveup_reason is None else f"{outcome.giveup_reason}; "
            format_duration: Callable[[int], str] = config.format_duration  # lambda: nanos
            log.log(
                config.warning_loglevel,
                "%s",
                f"{config.format_msg(config.display_msg, retryable_error)}"
                f"exhausted; giving up because {reason}the last "
                f"{config.format_pair(retry.count, policy.max_retries)} retries across "
                f"{config.format_pair(format_duration(outcome.elapsed_nanos), format_duration(policy.max_elapsed_nanos))} "
                "failed",
                exc_info=retryable_error if config.exc_info else None,
                stack_info=config.stack_info,
                extra=config.extra,
            )


def on_exhaustion_raise(outcome: AttemptOutcome) -> NoReturn:
    """Default implementation of ``on_exhaustion`` callback for call_with_retries(); always raises; thread-safe."""
    assert outcome.is_exhausted
    assert isinstance(outcome.result, RetryableError)
    retryable_error: RetryableError = outcome.result
    policy: RetryPolicy = outcome.retry.policy
    cause: BaseException | None = retryable_error.__cause__
    if policy.reraise and cause is not None:
        raise cause.with_traceback(cause.__traceback__)
    raise RetryError(outcome=outcome) from retryable_error


#############################################################################
_T = TypeVar("_T")


def call_with_retries(
    fn: Callable[[Retry], _T],  # typically a lambda; wraps work and raises RetryableError for failures that shall be retried
    policy: RetryPolicy,  # specifies how ``RetryableError`` shall be retried
    *,
    config: RetryConfig | None = None,  # controls logging settings and async cancellation between attempts
    giveup: Callable[[AttemptOutcome], object | None] = no_giveup,  # stop retrying based on domain-specific logic
    after_attempt: Callable[[AttemptOutcome], None] = after_attempt_log_failure,  # e.g. record metrics and/or custom logging
    on_exhaustion: Callable[[AttemptOutcome], _T] = on_exhaustion_raise,  # raise error or return fallback value
    log: logging.Logger | None = None,
) -> _T:
    """Runs the function ``fn`` and returns its result; retries on failure as indicated by policy and config; thread-safe.

    By default on exhaustion, call_with_retries() either re-raises the last underlying ``RetryableError.__cause__``, or raises
    ``RetryError`` (wrapping the last ``RetryableError``), like so:
    - if ``RetryPolicy.reraise`` is True and the last ``RetryableError.__cause__`` is not None, re-raise the last
      ``RetryableError.__cause__`` with its original traceback.
    - Otherwise, raise ``RetryError`` (wrapping the last ``RetryableError``, preserving its ``__cause__`` chain).
    - The default is ``RetryPolicy.reraise=True``.

    On the exhaustion path, ``on_exhaustion`` will be called exactly once (after the final after_attempt). The default
    implementation raises as described above; custom ``on_exhaustion`` impls may return a fallback value instead of an error.
    """
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
            result: _T = fn(retry)  # Call the target function and supply retry attempt number and other metadata
            if after_attempt is not after_attempt_log_failure:
                elapsed_nanos: int = time.monotonic_ns() - start_time_nanos
                outcome: AttemptOutcome = AttemptOutcome(retry, True, False, False, None, elapsed_nanos, 0, result)
                after_attempt(outcome)
            return result
        except RetryableError as retryable_error:
            elapsed_nanos = time.monotonic_ns() - start_time_nanos
            giveup_reason: object | None = None
            sleep_nanos: int = 0
            sleep: Callable[[int, Retry], None] = _sleep
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
                        sleep(sleep_nanos, retry)
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


def _sleep(sleep_nanos: int, retry: Retry) -> None:
    termination_event: threading.Event | None = retry.config.termination_event
    if termination_event is None:
        time.sleep(sleep_nanos / 1_000_000_000)
    else:
        termination_event.wait(sleep_nanos / 1_000_000_000)  # allow early wakeup on async termination


def _is_terminated(retry: Retry) -> bool:
    termination_event: threading.Event | None = retry.config.termination_event
    return termination_event is not None and termination_event.is_set()


def multi_after_attempt(handlers: Iterable[Callable[[AttemptOutcome], None]]) -> Callable[[AttemptOutcome], None]:
    """Composes independent ``after_attempt`` handlers into one ``call_with_retries(after_attempt=...)`` callback that
    invokes each handler in order; thread-safe."""
    handlers = tuple(handlers)
    if len(handlers) == 1 and handlers[0] is after_attempt_log_failure:
        return after_attempt_log_failure  # perf

    def _after_attempt(outcome: AttemptOutcome) -> None:
        for handler in handlers:
            handler(outcome)

    return _after_attempt


def any_giveup(handlers: Iterable[Callable[[AttemptOutcome], object | None]]) -> Callable[[AttemptOutcome], object | None]:
    """Composes independent ``giveup`` handlers into one ``call_with_retries(giveup=...)`` callback that gives up retrying if
    *any* handler gives up; that is if any handler returns a non-``None`` reason; thread-safe.

    Handlers are evaluated in order and short-circuit: On giving up returns the first handler's reason for giving up.
    """
    handlers = tuple(handlers)
    if len(handlers) == 1 and handlers[0] is no_giveup:
        return no_giveup  # perf

    def _giveup(outcome: AttemptOutcome) -> object | None:
        for handler in handlers:
            giveup_reason: object | None = handler(outcome)
            if giveup_reason is not None:
                return giveup_reason
        return None  # don't give up retrying

    return _giveup


def all_giveup(handlers: Iterable[Callable[[AttemptOutcome], object | None]]) -> Callable[[AttemptOutcome], object | None]:
    """Composes independent ``giveup`` handlers into one ``call_with_retries(giveup=...)`` callback that gives up retrying if
    *all* handlers give up; that is if all handlers return a non-``None`` reason; thread-safe.

    Handlers are evaluated in order and short-circuit: stops at first ``None``; else returns the last non-``None`` reason.
    """
    handlers = tuple(handlers)
    if len(handlers) == 1 and handlers[0] is no_giveup:
        return no_giveup  # perf

    def _giveup(outcome: AttemptOutcome) -> object | None:
        giveup_reason: object | None = None
        for handler in handlers:
            giveup_reason = handler(outcome)
            if giveup_reason is None:
                return None  # don't give up retrying
        return giveup_reason

    return _giveup


#############################################################################
class RetryableError(Exception):
    """Indicates that the task that caused the underlying exception can be retried and might eventually succeed;
    ``call_with_retries()`` will pass this exception to callbacks via ``AttemptOutcome.result``; can be subclassed."""

    def __init__(
        self,
        *exc_args: object,  # optional args passed into super().__init__()
        display_msg: object = None,  # for logging
        retry_immediately_once: bool = False,  # retry once immediately without backoff?
        attachment: object = None,  # optional domain specific info passed to next attempt via Retry.previous_outcomes if
        # RetryPolicy.max_previous_outcomes > 0. This helps when retrying is not just 'try again later', but
        # 'try again differently based on what just happened'.
        # Examples: switching network endpoints, adjusting per-attempt timeouts, capping retries by error-class, resuming
        # with a token/offset, maintaining failure history for this invocation of call_with_retries().
        # Example: 'cap retries to 3 for ECONNREFUSED but 12 for ETIMEDOUT' via attachment=collections.Counter
    ) -> None:
        super().__init__(*exc_args)
        self.display_msg: object = display_msg
        self.retry_immediately_once: bool = retry_immediately_once
        self.attachment: object = attachment

    def display_msg_str(self) -> str:
        """Returns the display_msg as a str; for logging."""
        return "" if self.display_msg is None else str(self.display_msg)


#############################################################################
@final
class RetryError(Exception):
    """Indicates that retries have been exhausted; the last RetryableError is in RetryError.__cause__."""

    outcome: Final[AttemptOutcome]
    """Metadata that describes why and how call_with_retries() gave up."""

    def __init__(self, outcome: AttemptOutcome) -> None:
        super().__init__(outcome)
        self.outcome = outcome


#############################################################################
@final
class Retry(NamedTuple):
    """Attempt metadata provided to callback functions; includes the current retry attempt number; immutable."""

    count: int  # type: ignore[assignment]
    """Attempt number; count=0 is the first attempt, count=1 is the second attempt aka first retry."""

    start_time_nanos: int
    """Value of time.monotonic_ns() at start of call_with_retries() invocation."""

    attempt_start_time_nanos: int
    """Value of time.monotonic_ns() at start of fn() invocation."""

    policy: RetryPolicy
    """Policy that was passed into call_with_retries()."""

    config: RetryConfig
    """Config that is used by call_with_retries()."""

    log: logging.Logger | None
    """Logger that was passed into call_with_retries()."""

    previous_outcomes: Sequence[AttemptOutcome]
    """History/state of the N=max_previous_outcomes most recent outcomes for the current call_with_retries() invocation."""

    def copy(self, **override_kwargs: Any) -> Retry:
        """Creates a new object copying an existing one with the specified fields overridden for customization."""
        return self._replace(**override_kwargs)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(count={self.count!r}, start_time_nanos={self.start_time_nanos!r}, "
            f"attempt_start_time_nanos={self.attempt_start_time_nanos!r})"
        )

    def __eq__(self, other: object) -> bool:
        return self is other

    def __hash__(self) -> int:
        return object.__hash__(self)


#############################################################################
@final
class AttemptOutcome(NamedTuple):
    """Captures per-attempt state for ``after_attempt`` callbacks; immutable."""

    retry: Retry
    """Attempt metadata passed into fn(retry)."""

    is_success: bool
    """False if fn(retry) raised a RetryableError; True otherwise."""

    is_exhausted: bool
    """True if the loop is giving up retrying (possibly even due to is_terminated); False otherwise."""

    is_terminated: bool
    """True if termination_event has become set; False otherwise."""

    giveup_reason: object | None
    """Reason returned by giveup(); None means giveup() was not called or decided to not give up."""

    elapsed_nanos: int
    """Total duration between the start of call_with_retries() invocation and the end of this fn() attempt."""

    sleep_nanos: int
    """Duration of current sleep period."""

    result: RetryableError | object
    """Result of fn(retry); a RetryableError on retryable failure, or some other object on success."""

    def attempt_elapsed_nanos(self) -> int:
        """Returns duration between the start of this fn() attempt and the end of this fn() attempt."""
        return self.elapsed_nanos + self.retry.start_time_nanos - self.retry.attempt_start_time_nanos

    def copy(self, **override_kwargs: Any) -> AttemptOutcome:
        """Creates a new outcome copying an existing one with the specified fields overridden for customization."""
        return self._replace(**override_kwargs)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"retry={self.retry!r}, "
            f"is_success={self.is_success!r}, "
            f"is_exhausted={self.is_exhausted!r}, "
            f"is_terminated={self.is_terminated!r}, "
            f"giveup_reason={self.giveup_reason!r}, "
            f"elapsed_nanos={self.elapsed_nanos!r}, "
            f"sleep_nanos={self.sleep_nanos!r})"
        )

    def __eq__(self, other: object) -> bool:
        return self is other

    def __hash__(self) -> int:
        return object.__hash__(self)


#############################################################################
@final
class BackoffContext(NamedTuple):
    """Captures per-backoff state for ``backoff_strategy`` callbacks."""

    retry: Retry
    """Attempt metadata passed into fn(retry)."""

    curr_max_sleep_nanos: int
    """Current maximum duration (in nanoseconds) to sleep before the next retry attempt;
    Typically: ``RetryPolicy.initial_max_sleep_nanos <= curr_max_sleep_nanos <= RetryPolicy.max_sleep_nanos``."""

    rng: random.Random
    """Thread-local random number generator instance."""

    elapsed_nanos: int
    """Total duration between the start of call_with_retries() invocation and the end of this fn() attempt."""

    retryable_error: RetryableError
    """Result of failed fn(retry) attempt."""

    def copy(self, **override_kwargs: Any) -> BackoffContext:
        """Creates a new object copying an existing one with the specified fields overridden for customization."""
        return self._replace(**override_kwargs)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"retry={self.retry!r}, "
            f"curr_max_sleep_nanos={self.curr_max_sleep_nanos!r}, "
            f"elapsed_nanos={self.elapsed_nanos!r})"
        )

    def __eq__(self, other: object) -> bool:
        return self is other

    def __hash__(self) -> int:
        return object.__hash__(self)


BackoffStrategy = Callable[[BackoffContext], tuple[int, int]]  # typealias; returns sleep_nanos:int, curr_max_sleep_nanos:int


def full_jitter_backoff_strategy(context: BackoffContext) -> tuple[int, int]:
    """Default implementation of ``backoff_strategy`` callback for RetryPolicy.

    Full-jitter picks a random sleep_nanos duration from the range [min_sleep_nanos, curr_max_sleep_nanos] and applies
    exponential backoff with cap to the next attempt; thread-safe. Typically, min_sleep_nanos is 0 and exponential_base is 2.
    Example curr_max_sleep_nanos sequence: 125ms --> 250ms --> 500ms --> 1s --> 2s --> 4s --> 8s --> 10s --> 10s...
    Full-jitter provides optimal balance between reducing server load and minimizing retry latency.
    """
    policy: RetryPolicy = context.retry.policy
    curr_max_sleep_nanos: int = context.curr_max_sleep_nanos
    if policy.min_sleep_nanos == curr_max_sleep_nanos:
        sleep_nanos = curr_max_sleep_nanos  # perf
    else:
        sleep_nanos = context.rng.randint(policy.min_sleep_nanos, curr_max_sleep_nanos)  # nanos to delay until next attempt
    curr_max_sleep_nanos = round(curr_max_sleep_nanos * policy.exponential_base)  # exponential backoff
    curr_max_sleep_nanos = min(curr_max_sleep_nanos, policy.max_sleep_nanos)  # ... with cap for next attempt
    return sleep_nanos, curr_max_sleep_nanos


#############################################################################
@dataclass(frozen=True)
@final
class RetryPolicy:
    """Configuration controlling max retry counts and backoff delays for call_with_retries(); immutable.

    By default uses full jitter which works as follows: The maximum duration to sleep between attempts initially starts with
    ``initial_max_sleep_secs`` and doubles on each retry, up to the final maximum of ``max_sleep_secs``.
    Example: 125ms --> 250ms --> 500ms --> 1s --> 2s --> 4s --> 8s --> 10s --> 10s...
    On each retry a random sleep duration in the range ``[min_sleep_secs, current max]`` is picked.
    In a nutshell: ``0 <= min_sleep_secs <= initial_max_sleep_secs <= max_sleep_secs``. Typically, min_sleep_secs=0.
    """

    max_retries: int = INFINITY_MAX_RETRIES
    """The maximum number of times ``fn`` will be invoked additionally after the first attempt invocation; must be >= 0."""

    min_sleep_secs: float = 0
    """The minimum duration to sleep between any two attempts."""

    initial_max_sleep_secs: float = 0.125
    """The initial maximum duration to sleep between any two attempts."""

    max_sleep_secs: float = 10
    """The final max duration to sleep between any two attempts; 0 <= min_sleep_secs <= initial_max_sleep_secs <=
    max_sleep_secs."""

    max_elapsed_secs: float = 60
    """``fn`` will not be retried (or not retried anymore) once this much time has elapsed since the initial start of
    call_with_retries(); set this to 365 * 86400 seconds or similar to effectively disable the time limit."""

    exponential_base: float = 2
    """Growth factor (aka multiplier) for backoff algorithm to calculate sleep duration; must be >= 1."""

    max_elapsed_nanos: int = dataclasses.field(init=False, repr=False)  # derived value
    min_sleep_nanos: int = dataclasses.field(init=False, repr=False)  # derived value
    initial_max_sleep_nanos: int = dataclasses.field(init=False, repr=False)  # derived value
    max_sleep_nanos: int = dataclasses.field(init=False, repr=False)  # derived value

    backoff_strategy: BackoffStrategy = dataclasses.field(default=full_jitter_backoff_strategy, repr=False)
    """Strategy that implements a backoff algorithm that reduces server load while minimizing retry latency; default is full
    jitter; various other example backoff strategies can be found in test_retry_examples.py."""

    reraise: bool = True
    """On exhaustion, the default (``True``) is to re-raise the underlying exception when present."""

    max_previous_outcomes: int = 0
    """Pass the N=max_previous_outcomes most recent AttemptOutcome objects to callbacks."""

    context: object = dataclasses.field(default=None, repr=False, compare=False)
    """Optional domain specific info."""

    @classmethod
    def from_namespace(cls, args: argparse.Namespace) -> RetryPolicy:
        """Factory that reads the policy from argparse.ArgumentParser via args."""
        return RetryPolicy(
            max_retries=getattr(args, "max_retries", INFINITY_MAX_RETRIES),
            min_sleep_secs=getattr(args, "retry_min_sleep_secs", 0),
            initial_max_sleep_secs=getattr(args, "retry_initial_max_sleep_secs", 0.125),
            max_sleep_secs=getattr(args, "retry_max_sleep_secs", 10),
            max_elapsed_secs=getattr(args, "retry_max_elapsed_secs", 60),
            exponential_base=getattr(args, "retry_exponential_base", 2),
            backoff_strategy=getattr(args, "retry_backoff_strategy", full_jitter_backoff_strategy),
            reraise=getattr(args, "retry_reraise", True),
            max_previous_outcomes=getattr(args, "retry_max_previous_outcomes", 0),
            context=getattr(args, "retry_context", None),
        )

    @classmethod
    def no_retries(cls) -> RetryPolicy:
        """Returns a policy that never retries."""
        return RetryPolicy(
            max_retries=0,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=0,
        )

    def __post_init__(self) -> None:  # validate and compute derived values
        self._validate_min("max_retries", self.max_retries, 0)
        self._validate_min("exponential_base", self.exponential_base, 1)
        self._validate_min("min_sleep_secs", self.min_sleep_secs, 0)
        self._validate_min("initial_max_sleep_secs", self.initial_max_sleep_secs, 0)
        self._validate_min("max_sleep_secs", self.max_sleep_secs, 0)
        self._validate_min("max_elapsed_secs", self.max_elapsed_secs, 0)
        object.__setattr__(self, "max_elapsed_nanos", int(self.max_elapsed_secs * 1_000_000_000))  # derived value
        min_sleep_nanos: int = int(self.min_sleep_secs * 1_000_000_000)
        initial_max_sleep_nanos: int = int(self.initial_max_sleep_secs * 1_000_000_000)
        max_sleep_nanos: int = int(self.max_sleep_secs * 1_000_000_000)
        max_sleep_nanos = max(min_sleep_nanos, max_sleep_nanos)
        initial_max_sleep_nanos = min(max_sleep_nanos, max(min_sleep_nanos, initial_max_sleep_nanos))
        object.__setattr__(self, "min_sleep_nanos", min_sleep_nanos)  # derived value
        object.__setattr__(self, "initial_max_sleep_nanos", initial_max_sleep_nanos)  # derived value
        object.__setattr__(self, "max_sleep_nanos", max_sleep_nanos)  # derived value
        self._validate_min("max_previous_outcomes", self.max_previous_outcomes, 0)
        assert 0 <= self.min_sleep_nanos <= self.initial_max_sleep_nanos <= self.max_sleep_nanos
        if not callable(self.backoff_strategy):
            raise TypeError("RetryPolicy.backoff_strategy must be callable")
        if not isinstance(self.reraise, bool):
            raise TypeError("RetryPolicy.reraise must be bool")

    def _validate_min(self, name: str, value: float, minimum: float) -> None:
        if value < minimum:
            raise ValueError(f"Invalid RetryPolicy.{name}: must be >= {minimum} but got {value}")

    def copy(self, **override_kwargs: Any) -> RetryPolicy:
        """Creates a new policy copying an existing one with the specified fields overridden for customization; thread-safe.

        Example usage: policy = retry_policy.copy(max_sleep_secs=2, max_elapsed_secs=10)
        """
        return dataclasses.replace(self, **override_kwargs)


#############################################################################
def _format_msg(display_msg: str, retryable_error: RetryableError) -> str:
    """Default implementation of ``format_msg`` callback for RetryConfig; creates simple log message; thread-safe."""
    msg = display_msg + " " if display_msg else ""
    errmsg: str = retryable_error.display_msg_str()
    msg = msg + errmsg + " " if errmsg else msg
    msg = msg if msg else "Retrying "
    return msg


def _format_pair(first: object, second: object) -> str:
    """Default implementation of ``format_pair`` callback for RetryConfig; creates simple log message part; thread-safe."""
    second = "∞" if INFINITY_MAX_RETRIES == second else second  # noqa: SIM300
    return f"[{first}/{second}]"


@dataclass(frozen=True)
@final
class RetryConfig:
    """Configures termination behavior and logging for call_with_retries(); all defaults work out of the box; immutable."""

    termination_event: threading.Event | None = None  # optionally allows for async cancellation
    display_msg: str = "Retrying"  # message prefix for retry log messages
    dots: str = " ..."  # suffix appended to retry log messages
    format_msg: Callable[[str, RetryableError], str] = _format_msg  # lambda: display_msg, retryable_error
    format_pair: Callable[[object, object], str] = _format_pair  # lambda: first, second
    format_duration: Callable[[int], str] = human_readable_duration  # lambda: nanos
    info_loglevel: int = logging.INFO  # loglevel used when not giving up
    warning_loglevel: int = logging.WARNING  # loglevel used when giving up
    enable_logging: bool = True  # set to False to disable logging
    exc_info: bool = False  # passed into Logger.log()
    stack_info: bool = False  # passed into Logger.log()
    extra: Mapping[str, object] | None = dataclasses.field(default=None, repr=False, compare=False)  # passed to Logger.log()
    context: object = dataclasses.field(default=None, repr=False, compare=False)  # optional domain specific info

    def copy(self, **override_kwargs: Any) -> RetryConfig:
        """Creates a new config copying an existing one with the specified fields overridden for customization."""
        return dataclasses.replace(self, **override_kwargs)


_DEFAULT_RETRY_CONFIG: Final[RetryConfig] = RetryConfig()  # constant


#############################################################################
def _fn_not_implemented(_retry: Retry) -> NoReturn:
    """Default implementation of ``fn`` callback for RetryTemplate; always raises."""
    raise NotImplementedError("Provide fn when calling RetryTemplate")


@dataclass(frozen=True)
@final
class RetryTemplate(Generic[_T]):
    """Convenience class that aggregates all knobs for call_with_retries(); and is itself callable too; immutable."""

    fn: Callable[[Retry], _T] = _fn_not_implemented  # set this to make the RetryTemplate object itself callable
    policy: RetryPolicy = RetryPolicy()  # specifies how ``RetryableError`` shall be retried
    config: RetryConfig = RetryConfig()  # controls logging settings and async cancellation between attempts
    giveup: Callable[[AttemptOutcome], object | None] = no_giveup  # stop retrying based on domain-specific logic
    after_attempt: Callable[[AttemptOutcome], None] = after_attempt_log_failure  # e.g. record metrics and/or custom logging
    on_exhaustion: Callable[[AttemptOutcome], _T] = on_exhaustion_raise  # raise error or return fallback value
    log: logging.Logger | None = None

    def copy(self, **override_kwargs: Any) -> RetryTemplate[_T]:
        """Creates a new object copying an existing one with the specified fields overridden for customization; thread-safe.

        Example usage: retry_template.copy(policy=policy.copy(max_sleep_secs=2, max_elapsed_secs=10), log=None)
        """
        return dataclasses.replace(self, **override_kwargs)

    def __call__(self) -> _T:
        """Executes ``self.fn`` via the call_with_retries() retry loop using the stored parameters; thread-safe.

        Example Usage: result: str = retry_template.copy(fn=...)()
        """
        return call_with_retries(
            fn=self.fn,
            policy=self.policy,
            config=self.config,
            giveup=self.giveup,
            after_attempt=self.after_attempt,
            on_exhaustion=self.on_exhaustion,
            log=self.log,
        )

    def call_with_retries(
        self,
        fn: Callable[[Retry], _T],
        policy: RetryPolicy | None = None,
        *,
        config: RetryConfig | None = None,
        giveup: Callable[[AttemptOutcome], object | None] | None = None,
        after_attempt: Callable[[AttemptOutcome], None] | None = None,
        on_exhaustion: Callable[[AttemptOutcome], _T] | None = None,
        log: logging.Logger | None = None,
    ) -> _T:
        """Executes ``fn`` via the call_with_retries() retry loop using the stored or overridden parameters; thread-safe.

        Example Usage: result: str = retry_template.call_with_retries(fn=...)
        """
        return call_with_retries(
            fn=fn,
            policy=self.policy if policy is None else policy,
            config=self.config if config is None else config,
            giveup=self.giveup if giveup is None else giveup,
            after_attempt=self.after_attempt if after_attempt is None else after_attempt,
            on_exhaustion=self.on_exhaustion if on_exhaustion is None else on_exhaustion,
            log=self.log if log is None else log,
        )


#############################################################################
def raise_retryable_error_from(
    exc: BaseException,
    *,
    display_msg: object = None,
    retry_immediately_once: bool = False,
    attachment: object = None,
) -> NoReturn:
    """Convenience function that raises a generic RetryableError that wraps the given underlying exception."""
    raise RetryableError(
        display_msg=type(exc).__name__ if display_msg is None else display_msg,
        retry_immediately_once=retry_immediately_once,
        attachment=attachment,
    ) from exc


ExceptionPredicate = Union[bool, Callable[[BaseException], bool]]  # Type alias


def call_with_exception_handlers(
    fn: Callable[[], _T],  # typically a lambda
    *,
    continue_if_no_predicate_matches: bool = False,
    handlers: Mapping[type[BaseException], Sequence[tuple[ExceptionPredicate, Callable[[BaseException], _T]]]],
) -> _T:
    """Convenience function that calls ``fn`` and returns its result; on exception runs the first matching handler in a per-
    exception handler chain; composes independent handlers via predicates into one function, in Event-Predicate-Action style.

    Lookup uses the exception type's Method Resolution Order (most-specific class in the exception class hierarchy wins). For
    the first class that exists as a key in ``handlers``, its chain is scanned in order. Each chain element is
    ``(predicate, handler)`` where ``predicate`` is either ``True`` (always matches), ``False`` (disabled), or
    ``predicate(exc) -> bool``. The first matching handler is called with the exception and its return value is returned. If
    no predicate matches then, by default, the original exception is re-raised and no less-specific handler chains are
    consulted. Set ``continue_if_no_predicate_matches=True`` to continue scanning exception base classes instead.

    Typically (but not necessarily) the handler raises a ``RetryableError``, via ``raise_retryable_error_from`` or similar.
    Or it may raise another exception type (which will not be retried), or even return a fallback value instead of raising.

    Example: turn transient ssh/zfs command failures into RetryableError for call_with_retries(), including feature flags:

        def run_remote(retry: Retry) -> str:
            p = subprocess.run(["ssh", "foo.example.com", "zfs", "list", "-H"], text=True, capture_output=True, check=True)
            return p.stdout

        def fn(retry: Retry) -> str:
            return call_with_exception_handlers(
                fn=lambda: run_remote(retry),
                handlers={
                    TimeoutError: [(True, raise_retryable_error_from)],
                    ConnectionResetError: [(True, lambda exc: raise_retryable_error_from(exc, display_msg="ssh reset"))],
                    subprocess.CalledProcessError: [
                        (lambda exc: exc.returncode == 255, lambda exc: raise_retryable_error_from(exc, display_msg="ssh error")),
                        (lambda exc: "cannot receive" in (exc.stderr or ""), lambda exc: raise_retryable_error_from(exc, display_msg="zfs recv")),
                    ],
                    OSError: [
                        (lambda exc: getattr(exc, "errno", None) in {errno.ETIMEDOUT, errno.EHOSTUNREACH},
                         lambda exc: raise_retryable_error_from(exc, display_msg=f"network: {exc}")),
                        (False, lambda exc: raise_retryable_error_from(exc, display_msg="disabled handler example")),
                    ],
                },
            )

        stdout: str = call_with_retries(fn=fn, policy=RetryPolicy(max_retries=3))

    Example: return a fallback value (no retry loop required):

        def read_optional_file(path: str) -> str:
            return call_with_exception_handlers(
                fn=lambda: open(path, encoding="utf-8").read(),
                handlers={FileNotFoundError: [(True, lambda _exc: "")]},
            )
    """
    try:
        return fn()
    except BaseException as exc:
        for cls in type(exc).__mro__:
            handler_chain = handlers.get(cls)
            if handler_chain is not None:
                for predicate, handler in handler_chain:
                    if predicate is True or (predicate is not False and predicate(exc)):
                        return handler(exc)
                if not continue_if_no_predicate_matches:
                    raise
        raise


#############################################################################
@final
class _ThreadLocalRNG(threading.local):
    """Caches a per-thread random number generator."""

    def __init__(self) -> None:
        self.rng: random.Random | None = None


_THREAD_LOCAL_RNG: Final[_ThreadLocalRNG] = _ThreadLocalRNG()


def _thread_local_rng() -> random.Random:
    """Returns a per-thread RNG for backoff jitter; for perf avoids locking and initializing a new random.Random() at high
    frequency."""
    threadlocal: _ThreadLocalRNG = _THREAD_LOCAL_RNG
    rng: random.Random | None = threadlocal.rng
    if rng is None:
        rng = random.Random()  # noqa: S311 jitter isn't security sensitive, and random.SystemRandom.randint() is slow
        threadlocal.rng = rng
    return rng
