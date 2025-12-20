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
"""Customizable generic retry support using jittered exponential backoff with cap.

Purpose:
--------
- Provide a reusable retry helper for transient failures using customizable policy and config objects.
- Centralize backoff, jitter, logging and metrics behavior while keeping call sites compact.
- Prevent accidental retries: the loop retries only when the developer explicitly raises a ``RetryableError``, which reduces
  the risk of retrying non-idempotent operations.
- Avoid unnecessary complexity and add zero dependencies beyond the Python standard library. Everything you need is in this
  single Python file.

Usage:
------
- Wrap work in a callable ``fn(retry: Retry)`` and therein raise ``RetryableError`` for failures that should be retried.
- Construct a policy via ``RetryPolicy(...)``
- Call ``run_with_retries(fn=fn, policy=policy, log=logger)`` with a standard logging.Logger
- On success, the result of calling ``fn`` is returned.
- On exhaustion, run_with_retries() either re-raises the last underlying ``RetryableError.__cause__``, or raises
  ``RetryError`` (wrapping the last ``RetryableError``), like so:
  - if ``RetryPolicy.reraise`` is True and the last ``RetryableError.__cause__`` is not None, re-raise the last
    ``RetryableError.__cause__`` with its original traceback.
  - Otherwise, raise ``RetryError`` (wrapping the last ``RetryableError``, preserving its ``__cause__`` chain).
  - The default is ``RetryPolicy.reraise=True``.

Advanced Configuration:
-----------------------
- Tune ``RetryPolicy`` parameters to control maximum retries, sleep bounds, and elapsed-time budget.
- Use ``RetryConfig`` to control termination events, and logging settings.
- Set ``log=None`` to disable logging, or customize ``info_loglevel`` / ``warning_loglevel`` for structured logs.
- Pass ``termination_event`` via ``RetryConfig`` to support async cancellation between attempts.
- Use ``giveup(AttemptOutcome)`` to stop retrying based on domain-specific logic (for example, error/status codes or parsing
  stderr), including time-aware decisions or decisions based on the previous N most recent AttemptOutcome objects (via
  AttemptOutcome.retry.previous_outcomes)

Observability:
--------------
- Supply ``after_attempt(AttemptOutcome)`` to collect per-attempt metrics such as success flag, exhausted/terminated state,
  attempt number, total elapsed duration (in nanoseconds), sleep duration (in nanoseconds), etc.
- ``AttemptOutcome.result`` is either the successful result or the most recent ``RetryableError``, enabling integration with
  metrics and tracing systems without coupling the retry loop to any specific backend.
- Supply ``after_attempt`` to customize logging 100%, if necessary, in which case also set ``enable_logging=False``.

Expert Configuration:
---------------------
- Set ``RetryPolicy.max_previous_outcomes > 0`` to pass the N most recent AttemptOutcome objects to callbacks (default is 0).
- If ``RetryPolicy.max_previous_outcomes > 0``, you can use ``RetryableError(..., attachment=...)`` to carry domain-specific
  state from a failed attempt to the next attempt via ``retry.previous_outcomes``. This pattern helps if attempt N+1 is a
  function of attempt N or all prior attempts (e.g., switching endpoints or resuming from an offset).
- Set ``backoff_strategy(retry, curr_max_sleep_nanos, rng, elapsed_nanos, retryable_error)`` to plug in a custom backoff
  algorithm (e.g., decorrelated-jitter). The default is full-jitter exponential backoff with cap (aka industry standard).
- To keep calling code retry-transparent, set ``RetryPolicy.reraise=True`` (the default) *and* raise retryable failures as
  ``raise RetryableError(...) from exc``. Client code now won't notice whether run_with_retries is used or not.
- To make exhaustion observable to calling code, set ``RetryPolicy.reraise=False``: run_with_retries() now always raises
  ``RetryError`` (wrapping the last ``RetryableError``) on exhaustion, so callers now catch ``RetryError`` and can inspect
  the last underlying exception via ``err.outcome``, ``err.__cause__``, and even ``err.__cause__.__cause__`` when present.

Example Usage:
--------------
    import logging
    from bzfs_main.util.retry import Retry, RetryPolicy, RetryableError, run_with_retries

    def unreliable_operation(retry: Retry) -> str:
        try:
            if retry.count < 3:
                raise ValueError("temporary failure connecting to foo.example.com")
            return "ok"
        except ValueError as exc:
            # Preserve the underlying cause for correct error propagation and logging
            raise RetryableError("temporary failure", display_msg="connect") from exc

    retry_policy = RetryPolicy(
        max_retries=10,
        min_sleep_secs=0,
        initial_max_sleep_secs=0.125,
        max_sleep_secs=10,
        max_elapsed_secs=60,
    )
    log = logging.getLogger(__name__)
    result: str = run_with_retries(fn=unreliable_operation, policy=retry_policy, log=log)
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
    Mapping,
    Sequence,
)
from dataclasses import (
    dataclass,
)
from logging import (
    Logger,
)
from typing import (
    Any,
    Callable,
    Final,
    TypeVar,
    final,
)

from bzfs_main.util.utils import (
    human_readable_duration,
)

#############################################################################
_T = TypeVar("_T")


def run_with_retries(
    fn: Callable[[Retry], _T],  # typically a lambda
    policy: RetryPolicy,
    config: RetryConfig | None = None,
    giveup: Callable[[AttemptOutcome], str] = lambda outcome: "",  # stop retrying based on domain-specific logic
    after_attempt: Callable[[AttemptOutcome], None] = lambda outcome: None,  # e.g. record metrics
    log: Logger | None = None,
) -> _T:
    """Runs the function ``fn`` and returns its result; retries on failure as indicated by policy and config; thread-safe.

    On exhaustion, run_with_retries() either re-raises the last underlying ``RetryableError.__cause__``, or raises
    ``RetryError`` (wrapping the last ``RetryableError``), like so:
    - if ``RetryPolicy.reraise`` is True and the last ``RetryableError.__cause__`` is not None, re-raise the last
      ``RetryableError.__cause__`` with its original traceback.
    - Otherwise, raise ``RetryError`` (wrapping the last ``RetryableError``, preserving its ``__cause__`` chain).
    - The default is ``RetryPolicy.reraise=True``.
    """
    config = _DEFAULT_RETRY_CONFIG if config is None else config
    curr_max_sleep_nanos: int = policy.initial_max_sleep_nanos
    retry_count: int = 0
    elapsed_nanos: int = 0
    rng: random.Random | None = None
    previous_outcomes: tuple[AttemptOutcome, ...] = ()  # for safety pass *immutable* deque to callbacks
    start_time_nanos: Final[int] = time.monotonic_ns()
    while True:
        giveup_reason: str = ""
        retry: Retry = Retry(retry_count, start_time_nanos, policy, config, previous_outcomes)
        try:
            result: _T = fn(retry)  # Call the target function and supply retry attempt number and other metadata
            outcome: AttemptOutcome = AttemptOutcome(
                retry, True, False, False, giveup_reason, time.monotonic_ns() - start_time_nanos, 0, result, log
            )
            after_attempt(outcome)
            return result
        except RetryableError as retryable_error:
            elapsed_nanos = time.monotonic_ns() - start_time_nanos
            if retry_count <= 0 and retryable_error.retry_immediately_once:
                sleep_nanos: int = 0  # retry once immediately without backoff
            else:  # jitter: default backoff_strategy picks random sleep_nanos in [min_sleep_nanos, curr_max_sleep_nanos]
                rng = random.SystemRandom() if rng is None else rng
                sleep_nanos, curr_max_sleep_nanos = policy.backoff_strategy(
                    retry, curr_max_sleep_nanos, rng, elapsed_nanos, retryable_error
                )
                assert sleep_nanos >= 0
                assert curr_max_sleep_nanos >= 0

            format_duration: Callable[[int], str] = config.format_duration  # lambda: nanos
            termination_event: threading.Event | None = config.termination_event
            outcome = AttemptOutcome(
                retry, False, False, False, giveup_reason, elapsed_nanos, sleep_nanos, retryable_error, log
            )
            will_retry: bool = False
            if (
                retry_count < policy.max_retries
                and elapsed_nanos < policy.max_elapsed_nanos
                and (termination_event is None or not termination_event.is_set())
                and not (giveup_reason := giveup(outcome))
            ):
                will_retry = True
                retry_count += 1
                loglevel: int = config.info_loglevel
                if log is not None and config.enable_logging and log.isEnabledFor(loglevel):
                    msg = config.format_msg(config.display_msg, retryable_error) + config.format_pair(
                        retry_count, policy.max_retries
                    )
                    log.log(loglevel, "%s", f"{msg} in {format_duration(sleep_nanos)}{config.dots}", extra=config.extra)
                after_attempt(outcome)
                _sleep(sleep_nanos, termination_event)
            else:
                sleep_nanos = 0

            is_terminated: bool = termination_event is not None and termination_event.is_set()
            if is_terminated or not will_retry:
                if (
                    policy.max_retries > 0
                    and log is not None
                    and config.enable_logging
                    and log.isEnabledFor(config.warning_loglevel)
                    and not is_terminated
                ):
                    reason: str = f"{giveup_reason}; " if giveup_reason else ""
                    log.log(
                        config.warning_loglevel,
                        "%s",
                        f"{config.format_msg(config.display_msg, retryable_error)}"
                        f"exhausted; giving up because {reason}the last "
                        f"{config.format_pair(retry_count, policy.max_retries)} retries across "
                        f"{config.format_pair(format_duration(elapsed_nanos), format_duration(policy.max_elapsed_nanos))} "
                        "failed",
                        exc_info=retryable_error if config.exc_info else None,
                        stack_info=config.stack_info,
                        extra=config.extra,
                    )
                outcome = AttemptOutcome(
                    retry, False, True, is_terminated, giveup_reason, elapsed_nanos, sleep_nanos, retryable_error, log
                )
                after_attempt(outcome)
                cause: BaseException | None = retryable_error.__cause__
                if policy.reraise and cause is not None:
                    raise cause.with_traceback(cause.__traceback__)  # noqa: B904 intentional re-raise without chaining
                else:
                    raise RetryError(outcome) from retryable_error

            n = policy.max_previous_outcomes
            if n > 0:  #  outcome will be passed to next attempt via Retry.previous_outcomes
                if len(outcome.retry.previous_outcomes) > 0:
                    outcome = outcome.copy(retry=retry.copy(previous_outcomes=()))  # detach to reduce memory footprint
                previous_outcomes = previous_outcomes[len(previous_outcomes) - n + 1 :] + (outcome,)  # *immutable* deque
            del outcome  # help gc
            elapsed_nanos = time.monotonic_ns() - start_time_nanos


def _sleep(sleep_nanos: int, termination_event: threading.Event | None) -> None:
    if sleep_nanos > 0:
        if termination_event is None:
            time.sleep(sleep_nanos / 1_000_000_000)
        else:
            termination_event.wait(sleep_nanos / 1_000_000_000)  # allow early wakeup on async termination


#############################################################################
class RetryableError(Exception):
    """Indicates that the task that caused the underlying exception can be retried and might eventually succeed;
    ``run_with_retries()`` will pass this exception to callbacks via ``AttemptOutcome.result``."""

    def __init__(
        self, message: str, display_msg: object = None, retry_immediately_once: bool = False, attachment: object = None
    ) -> None:
        super().__init__(message)
        self.display_msg: object = display_msg  # for logging
        self.retry_immediately_once: bool = retry_immediately_once  # retry once immediately without backoff?

        self.attachment: object = attachment  # domain specific info passed to next attempt via Retry.previous_outcomes if
        # RetryPolicy.max_previous_outcomes > 0. This helps when retrying is not just 'try again later', but
        # 'try again differently based on what just happened'.
        # Examples: switching network endpoints, adjusting per-attempt timeouts, capping retries by error-class, resuming
        # with a token/offset, maintaining failure history for this invocation of run_with_retries().
        # Example: 'cap retries to 3 for ECONNREFUSED but 12 for ETIMEDOUT' via attachment=collections.Counter

    def display_msg_str(self) -> str:
        """Returns the display_msg as a str; for logging."""
        return "" if self.display_msg is None else str(self.display_msg)


#############################################################################
@dataclass
@final
class RetryError(Exception):
    """Indicates that retries have been exhausted; the last RetryableError is in RetryError.__cause__."""

    outcome: AttemptOutcome


#############################################################################
@dataclass(frozen=True)
@final
class Retry:
    """The current retry attempt number provided to callback functions; immutable."""

    count: int  # attempt number, count=0 is the fist attempt, count=1 is the second attempt aka first retry
    start_time_nanos: int  # value of time.monotonic_ns() at start of this run_with_retries() invocation
    policy: RetryPolicy = dataclasses.field(repr=False, compare=False)
    config: RetryConfig = dataclasses.field(repr=False, compare=False)
    previous_outcomes: Sequence[AttemptOutcome] = dataclasses.field(repr=False, compare=False)  # in curr run_with_retries()

    def copy(self, **override_kwargs: Any) -> Retry:
        """Creates a new object copying an existing one with the specified fields overridden for customization."""
        return dataclasses.replace(self, **override_kwargs)


#############################################################################
@dataclass(frozen=True)
@final
class AttemptOutcome:
    """Captures per-attempt state for ``after_attempt`` callbacks; immutable."""

    retry: Retry
    is_success: bool
    is_exhausted: bool
    is_terminated: bool
    giveup_reason: str  # empty string means giveup() was not called or giveup() decided to not give up
    elapsed_nanos: int  # total duration since the start of this run_with_retries() invocation and end of this fn() attempt
    sleep_nanos: int  # duration of current sleep period
    result: RetryableError | object = dataclasses.field(repr=False, compare=False)
    log: Logger | None = dataclasses.field(repr=False, compare=False)

    def copy(self, **override_kwargs: Any) -> AttemptOutcome:
        """Creates a new outcome copying an existing one with the specified fields overridden for customization."""
        return dataclasses.replace(self, **override_kwargs)


#############################################################################
def _full_jitter_backoff_strategy(
    retry: Retry, curr_max_sleep_nanos: int, rand: random.Random, elapsed_nanos: int, retryable_error: RetryableError
) -> tuple[int, int]:
    """Full-jitter picks a random sleep_nanos duration from the range [min_sleep_nanos, curr_max_sleep_nanos] and applies
    exponential backoff with cap to the next attempt."""
    policy: RetryPolicy = retry.policy
    sleep_nanos: int = rand.randint(policy.min_sleep_nanos, curr_max_sleep_nanos)  # nanos to delay until next attempt
    curr_max_sleep_nanos = round(curr_max_sleep_nanos * policy.exponential_base)  # exponential backoff
    curr_max_sleep_nanos = min(curr_max_sleep_nanos, policy.max_sleep_nanos)  # ... with cap for next attempt
    return sleep_nanos, curr_max_sleep_nanos


@dataclass(frozen=True)
@final
class RetryPolicy:
    """Configuration controlling max retry counts and backoff delays for run_with_retries(); immutable.

    By default works as follows: The maximum duration to sleep between retries initially starts with initial_max_sleep_secs
    and doubles on each retry, up to the final maximum of max_sleep_secs. On each retry a random sleep duration in the range
    [min_sleep_secs, current max] is picked. In a nutshell: 0 <= min_sleep_secs <= initial_max_sleep_secs <= max_sleep_secs
    """

    max_retries: int = 10
    min_sleep_secs: float = 0
    initial_max_sleep_secs: float = 0.125
    max_sleep_secs: float = 10
    max_elapsed_secs: float = 60
    exponential_base: float = 2

    max_elapsed_nanos: int = dataclasses.field(init=False, repr=False)  # derived value
    min_sleep_nanos: int = dataclasses.field(init=False, repr=False)  # derived value
    initial_max_sleep_nanos: int = dataclasses.field(init=False, repr=False)  # derived value
    max_sleep_nanos: int = dataclasses.field(init=False, repr=False)  # derived value

    backoff_strategy: Callable[[Retry, int, random.Random, int, RetryableError], tuple[int, int]] = dataclasses.field(
        default=_full_jitter_backoff_strategy, repr=False  # retry, curr_max_sleep_nanos, rng, elapsed_nanos, retryable_error
    )
    reraise: bool = True  # on exhaustion, the default (``True``) is to re-raise the underlying exception when present
    max_previous_outcomes: int = 0  # pass the N=max_previous_outcomes most recent AttemptOutcome objects to callbacks
    context: object = dataclasses.field(default=None, repr=False, compare=False)  # optional domain specific info

    @classmethod
    def from_namespace(cls, args: argparse.Namespace) -> RetryPolicy:
        """Factory that reads the policy from ArgumentParser via args."""
        return RetryPolicy(
            max_retries=getattr(args, "max_retries", 10),
            min_sleep_secs=getattr(args, "retry_min_sleep_secs", 0),
            initial_max_sleep_secs=getattr(args, "retry_initial_max_sleep_secs", 0.125),
            max_sleep_secs=getattr(args, "retry_max_sleep_secs", 10),
            max_elapsed_secs=getattr(args, "retry_max_elapsed_secs", 60),
            exponential_base=getattr(args, "retry_exponential_base", 2),
            backoff_strategy=getattr(args, "retry_backoff_strategy", _full_jitter_backoff_strategy),
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

    def __post_init__(self) -> None:  # compute derived values
        object.__setattr__(self, "max_retries", max(0, self.max_retries))
        object.__setattr__(self, "exponential_base", max(1, self.exponential_base))
        object.__setattr__(self, "min_sleep_secs", max(0, self.min_sleep_secs))
        object.__setattr__(self, "initial_max_sleep_secs", max(0, self.initial_max_sleep_secs))
        object.__setattr__(self, "max_sleep_secs", max(0, self.max_sleep_secs))
        object.__setattr__(self, "max_elapsed_secs", max(0, self.max_elapsed_secs))
        object.__setattr__(self, "max_elapsed_nanos", int(self.max_elapsed_secs * 1_000_000_000))
        min_sleep_nanos: int = int(self.min_sleep_secs * 1_000_000_000)
        initial_max_sleep_nanos: int = int(self.initial_max_sleep_secs * 1_000_000_000)
        max_sleep_nanos: int = int(self.max_sleep_secs * 1_000_000_000)
        max_sleep_nanos = max(min_sleep_nanos, max_sleep_nanos)
        initial_max_sleep_nanos = min(max_sleep_nanos, max(min_sleep_nanos, initial_max_sleep_nanos))
        object.__setattr__(self, "min_sleep_nanos", min_sleep_nanos)
        object.__setattr__(self, "initial_max_sleep_nanos", initial_max_sleep_nanos)
        object.__setattr__(self, "max_sleep_nanos", max_sleep_nanos)
        object.__setattr__(self, "max_previous_outcomes", max(0, self.max_previous_outcomes))
        assert 0 <= self.min_sleep_nanos <= self.initial_max_sleep_nanos <= self.max_sleep_nanos
        if not callable(self.backoff_strategy):
            raise TypeError("RetryPolicy.backoff_strategy must be callable")
        if not isinstance(self.reraise, bool):
            raise TypeError("RetryPolicy.reraise must be bool")

    def copy(self, **override_kwargs: Any) -> RetryPolicy:
        """Creates a new policy copying an existing one with the specified fields overridden for customization."""
        return dataclasses.replace(self, **override_kwargs)


#############################################################################
def _format_msg(display_msg: str, retryable_error: RetryableError) -> str:  # thread-safe
    msg = display_msg + " " if display_msg else ""
    errmsg: str = retryable_error.display_msg_str()
    msg = msg + errmsg + " " if errmsg else msg
    msg = msg if msg else "Retrying "
    return msg


def _format_pair(first: object, second: object) -> str:  # thread-safe
    return f"[{first}/{second}]"


@dataclass(frozen=True)
@final
class RetryConfig:
    """Configures termination behavior and logging for run_with_retries(); all defaults work out of the box; immutable."""

    termination_event: threading.Event | None = None  # optionally allows for async cancellation
    display_msg: str = "Retrying"
    dots: str = " ..."
    format_msg: Callable[[str, RetryableError], str] = _format_msg  # lambda: display_msg, retryable_error
    format_pair: Callable[[object, object], str] = _format_pair  # lambda: first, second
    format_duration: Callable[[int], str] = human_readable_duration  # lambda: nanos
    info_loglevel: int = logging.INFO
    warning_loglevel: int = logging.WARNING
    enable_logging: bool = True  # set to False to move logging aspect into after_attempt()
    exc_info: bool = False
    stack_info: bool = False
    extra: Mapping[str, object] | None = dataclasses.field(default=None, repr=False, compare=False)
    context: object = dataclasses.field(default=None, repr=False, compare=False)  # optional domain specific info

    def copy(self, **override_kwargs: Any) -> RetryConfig:
        """Creates a new config copying an existing one with the specified fields overridden for customization."""
        return dataclasses.replace(self, **override_kwargs)


_DEFAULT_RETRY_CONFIG: Final[RetryConfig] = RetryConfig()  # constant


#############################################################################
@dataclass(frozen=True)
@final
class RetryOptions:
    """Convenience class that aggregates all knobs for run_with_retries(); all defaults work out of the box; immutable."""

    policy: RetryPolicy = RetryPolicy()
    config: RetryConfig = RetryConfig()
    giveup: Callable[[AttemptOutcome], str] = lambda outcome: ""  # stop retrying based on domain-specific logic
    after_attempt: Callable[[AttemptOutcome], None] = lambda outcome: None  # e.g. record metrics
    log: Logger | None = None

    def copy(self, **override_kwargs: Any) -> RetryOptions:
        """Creates a new object copying an existing one with the specified fields overridden for customization."""
        return dataclasses.replace(self, **override_kwargs)
