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
- Provide a reusable retry helper for transient failures using configurable policy and config objects.
- Centralize backoff, jitter, logging and metrics behavior while keeping call sites compact.
- Avoid unnecessary complexity and add zero dependencies beyond the Python standard library.

Usage:
------
- Wrap work in a callable ``fn(retry: Retry)`` and raise ``RetryableError`` for failures that should be retried.
- Construct a policy via ``RetryPolicy(...)``
- Call ``run_with_retries(fn=fn, policy=policy, log=logger)`` with a standard logging.Logger
- On success, the result of ``fn`` is returned; on exhaustion, the original cause is re-raised with its traceback.

Advanced Configuration:
-----------------------
- Tune ``RetryPolicy`` parameters to control maximum retries, sleep bounds, and elapsed-time budget.
- Use ``RetryConfig`` to control termination events, and logging settings.
- Set ``log`` to ``None`` to disable logging, or customize ``info_loglevel`` / ``warning_loglevel`` for structured logs.
- Pass ``termination_event`` via ``RetryConfig`` to support async cancellation between attempts.
- Use ``giveup(retryable_error)`` to stop retrying early based on domain-specific logic (for example, parsing ZFS stderr).

Observability:
--------------
- Supply ``after_attempt`` to collect per-attempt metrics such as success flag, exhausted/terminated state, attempt number,
  and a duration in nanoseconds (elapsed or sleep time).
- The last parameter passed to ``after_attempt`` is either the successful result or the most recent ``RetryableError``,
  enabling integration with metrics and tracing systems without coupling the retry loop to any specific backend.

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
            # Preserve the original cause for correct error propagation and logging
            raise RetryableError("temporary failure", display_msg="connect") from exc

    retry_policy = RetryPolicy(
        max_retries=10,
        min_sleep_secs=0,
        initial_max_sleep_secs=0.125,
        max_sleep_secs=10,
        max_elapsed_secs=60,
    )
    log = logging.getLogger(__name__)
    result = run_with_retries(fn=unreliable_operation, policy=retry_policy, log=log)
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
)
from dataclasses import (
    dataclass,
)
from typing import (
    Any,
    Callable,
    Final,
    TypeVar,
)

from bzfs_main.util.utils import (
    human_readable_duration,
)

#############################################################################
_T = TypeVar("_T")
_NO_TERMINATION_EVENT: Final[threading.Event] = threading.Event()  # constant


def run_with_retries(
    fn: Callable[[Retry], _T],  # typically a lambda
    policy: RetryPolicy,
    config: RetryConfig | None = None,
    giveup: Callable[[RetryableError], bool] = lambda retryable_error: False,  # stop retrying early (domain-specific logic)
    after_attempt: Callable[[Retry, bool, bool, bool, int, RetryableError | object], None] = (
        lambda retry, is_success, is_exhausted, is_terminated, duration_nanos, result: None  # e.g. record metrics
    ),
    log: logging.Logger | None = None,
) -> _T:
    """Runs the given function and retries on failure as indicated by policy and config; Thread-safe."""
    config = _DEFAULT_RETRY_CONFIG if config is None else config
    termination_event: threading.Event | None = config.termination_event
    c_max_sleep_nanos: int = policy.initial_max_sleep_nanos
    retry_count: int = 0
    rand: random.Random | None = None
    start_time_nanos: int = time.monotonic_ns()
    while True:
        retry: Retry = Retry(retry_count, policy)
        try:
            result: _T = fn(retry)  # Call the target function and supply retry attempt number
            after_attempt(retry, True, False, False, time.monotonic_ns() - start_time_nanos, result)
            return result
        except RetryableError as retryable_error:
            elapsed_nanos: int = time.monotonic_ns() - start_time_nanos
            termination_event = _NO_TERMINATION_EVENT if termination_event is None else termination_event
            format_msg: Callable[[str, RetryableError], str] = config.format_msg  # lambda: display_msg, retryable_error
            format_duration: Callable[[int], str] = config.format_duration  # lambda: nanos
            will_retry: bool = False
            if (
                retry_count < policy.max_retries
                and elapsed_nanos < policy.max_elapsed_nanos
                and not giveup(retryable_error)
                and not termination_event.is_set()
            ):
                will_retry = True
                retry_count += 1
                loglevel: int = config.info_loglevel
                cfg = config
                if retryable_error.no_sleep and retry_count <= 1:  # retry once immediately without backoff
                    if log is not None and log.isEnabledFor(loglevel):
                        msg = format_msg(cfg.display_msg, retryable_error) + cfg.format_pair(retry_count, policy.max_retries)
                        log.log(loglevel, "%s", f"{msg} immediately{config.dots}", extra=config.extra)
                    after_attempt(retry, False, False, False, 0, retryable_error)
                else:  # jitter: pick a random sleep duration within the range [min_sleep_nanos, c_max_sleep_nanos] as delay
                    rand = random.SystemRandom() if rand is None else rand
                    sleep_nanos: int = rand.randint(policy.min_sleep_nanos, c_max_sleep_nanos)
                    if log is not None and log.isEnabledFor(loglevel):
                        msg = format_msg(cfg.display_msg, retryable_error) + cfg.format_pair(retry_count, policy.max_retries)
                        log.log(loglevel, "%s", f"{msg} in {format_duration(sleep_nanos)}{config.dots}", extra=config.extra)
                    after_attempt(retry, False, False, False, sleep_nanos, retryable_error)
                    termination_event.wait(sleep_nanos / 1_000_000_000)  # seconds; allow early wakeup on async termination
                    c_max_sleep_nanos = round(c_max_sleep_nanos * policy.exponential_base)  # exponential backoff
                    c_max_sleep_nanos = min(c_max_sleep_nanos, policy.max_sleep_nanos)  # ... with cap
            if termination_event.is_set() or not will_retry:
                if (
                    policy.max_retries > 0
                    and log is not None
                    and log.isEnabledFor(config.warning_loglevel)
                    and not termination_event.is_set()
                ):
                    log.log(
                        config.warning_loglevel,
                        "%s",
                        f"{format_msg(config.display_msg, retryable_error)}exhausted; giving up because the last "
                        f"{config.format_pair(retry_count, policy.max_retries)} retries across "
                        f"{config.format_pair(format_duration(elapsed_nanos), format_duration(policy.max_elapsed_nanos))} "
                        "failed",
                        exc_info=retryable_error if config.exc_info else None,
                        stack_info=config.stack_info,
                        extra=config.extra,
                    )
                after_attempt(retry, False, True, termination_event.is_set(), elapsed_nanos, retryable_error)
                cause: BaseException | None = retryable_error.__cause__
                if cause is None:
                    raise
                else:
                    raise cause.with_traceback(cause.__traceback__)  # noqa: B904 intentional re-raise without chaining


#############################################################################
class RetryableError(Exception):
    """Indicates that the task that caused the underlying exception can be retried and might eventually succeed."""

    def __init__(self, message: str, display_msg: object = None, no_sleep: bool = False) -> None:
        super().__init__(message)
        self.display_msg: object = display_msg
        self.no_sleep: bool = no_sleep

    def display_msg_str(self) -> str:
        """Returns the display_msg as a str; for logging."""
        return "" if self.display_msg is None else str(self.display_msg)


#############################################################################
@dataclass(frozen=True)
class Retry:
    """The current retry attempt number provided to callback functions; immutable."""

    count: int
    policy: RetryPolicy


#############################################################################
class RetryPolicy:
    """Configuration controlling retry counts and backoff delays for run_with_retries(); immutable."""

    @classmethod
    def no_retries(cls) -> RetryPolicy:
        """Returns a policy that never retries."""
        return cls(
            max_retries=0,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=0,
        )

    @classmethod
    def from_namespace(cls, args: argparse.Namespace) -> RetryPolicy:
        """Factory that reads the policy from ArgumentParser via args."""
        return cls(
            max_retries=getattr(args, "max_retries", 10),
            min_sleep_secs=getattr(args, "retry_min_sleep_secs", 0),
            initial_max_sleep_secs=getattr(args, "retry_initial_max_sleep_secs", 0.125),
            max_sleep_secs=getattr(args, "retry_max_sleep_secs", 10),
            max_elapsed_secs=getattr(args, "retry_max_elapsed_secs", 60),
            exponential_base=getattr(args, "retry_exponential_base", 2),
        )

    def __init__(
        self,
        max_retries: int = 10,
        min_sleep_secs: float = 0,
        initial_max_sleep_secs: float = 0.125,
        max_sleep_secs: float = 10,
        max_elapsed_secs: float = 60,
        exponential_base: float = 2,
    ) -> None:
        # immutable variables:
        self.max_retries: Final[int] = max(0, max_retries)
        self.exponential_base: Final[float] = max(1, exponential_base)
        self.min_sleep_secs: Final[float] = max(0, min_sleep_secs)
        self.initial_max_sleep_secs: Final[float] = max(0, initial_max_sleep_secs)
        self.max_sleep_secs: Final[float] = max(0, max_sleep_secs)
        self.max_elapsed_secs: Final[float] = max(0, max_elapsed_secs)
        self.max_elapsed_nanos: Final[int] = int(self.max_elapsed_secs * 1_000_000_000)
        min_sleep_nanos: int = int(self.min_sleep_secs * 1_000_000_000)
        initial_max_sleep_nanos: int = int(self.initial_max_sleep_secs * 1_000_000_000)
        max_sleep_nanos: int = int(self.max_sleep_secs * 1_000_000_000)
        min_sleep_nanos = max(1, min_sleep_nanos)
        max_sleep_nanos = max(min_sleep_nanos, max_sleep_nanos)
        initial_max_sleep_nanos = min(max_sleep_nanos, max(min_sleep_nanos, initial_max_sleep_nanos))
        self.min_sleep_nanos: Final[int] = min_sleep_nanos
        self.initial_max_sleep_nanos: Final[int] = initial_max_sleep_nanos
        self.max_sleep_nanos: Final[int] = max_sleep_nanos
        assert 1 <= self.min_sleep_nanos <= self.initial_max_sleep_nanos <= self.max_sleep_nanos

    def __repr__(self) -> str:
        return (
            f"RetryPolicy(max_retries={self.max_retries}, min_sleep_secs={self.min_sleep_secs}, "
            f"initial_max_sleep_secs={self.initial_max_sleep_secs}, max_sleep_secs={self.max_sleep_secs}, "
            f"max_elapsed_secs={self.max_elapsed_secs}, exponential_base={self.exponential_base})"
        )


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
    exc_info: bool = False
    stack_info: bool = False
    extra: Mapping[str, object] | None = None

    def copy(self, **override_kwargs: Any) -> RetryConfig:
        """Creates a new config copying an existing one with the specified fields overridden for customization."""
        return dataclasses.replace(self, **override_kwargs)


_DEFAULT_RETRY_CONFIG: Final[RetryConfig] = RetryConfig()  # constant


#############################################################################
@dataclass(frozen=True)
class RetryOptions:
    """Convenience class that aggregates all knobs for run_with_retries(); all defaults work out of the box; immutable."""

    policy: RetryPolicy = RetryPolicy()  # noqa: RUF009
    config: RetryConfig = RetryConfig()
    giveup: Callable[[RetryableError], bool] = lambda retryable_error: False  # stop retrying early (domain-specific logic)
    after_attempt: Callable[[Retry, bool, bool, bool, int, RetryableError | object], None] = (
        lambda retry, is_success, is_exhausted, is_terminated, duration_nanos, result: None  # e.g. record metrics
    )
    log: logging.Logger | None = None

    def copy(self, **override_kwargs: Any) -> RetryOptions:
        """Creates a new object copying an existing one with the specified fields overridden for customization."""
        return dataclasses.replace(self, **override_kwargs)
