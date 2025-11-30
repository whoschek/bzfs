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
"""Generic retry support using jittered exponential backoff with cap.

This module retries failing operations according to a configurable policy. It assumes transient errors may eventually succeed
and centralizes retry logic for consistency across callers.
"""

from __future__ import (
    annotations,
)
import argparse
import logging
import random
import threading
import time
from dataclasses import (
    dataclass,
)
from typing import (
    Callable,
    Final,
    TypeVar,
)

from bzfs_main.util.utils import (
    human_readable_duration,
)


#############################################################################
class RetryPolicy:
    """Configuration controlling retry counts and backoff delays; Immutable."""

    def __init__(self, args: argparse.Namespace) -> None:
        """Option values for retries; reads from ArgumentParser via args."""
        # immutable variables:
        self.retries: Final[int] = args.retries
        self.min_sleep_secs: Final[float] = args.retry_min_sleep_secs
        self.initial_max_sleep_secs: Final[float] = args.retry_initial_max_sleep_secs
        self.max_sleep_secs: Final[float] = args.retry_max_sleep_secs
        self.max_elapsed_secs: Final[float] = args.retry_max_elapsed_secs
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
        """Return debug representation of retry parameters."""
        return (
            f"retries: {self.retries}, min_sleep_secs: {self.min_sleep_secs}, "
            f"initial_max_sleep_secs: {self.initial_max_sleep_secs}, max_sleep_secs: {self.max_sleep_secs}, "
            f"max_elapsed_secs: {self.max_elapsed_secs}"
        )

    @classmethod
    def no_retries(cls) -> RetryPolicy:
        """Returns a policy that never retries."""
        return cls(
            argparse.Namespace(
                retries=0,
                retry_min_sleep_secs=0,
                retry_initial_max_sleep_secs=0,
                retry_max_sleep_secs=0,
                retry_max_elapsed_secs=0,
            )
        )


#############################################################################
_T = TypeVar("_T")
_NO_TERMINATION_EVENT: Final[threading.Event] = threading.Event()  # constant


def run_with_retries(
    fn: Callable[[Retry], _T],  # typically a lambda
    policy: RetryPolicy,
    log: logging.Logger,
    display_msg: str = "Retrying",
    termination_event: threading.Event | None = None,
) -> _T:
    """Runs the given function and retries on failure as indicated by policy; The optional termination_event allows for early
    async cancellation of the retry loop; Thread-safe."""
    c_max_sleep_nanos: int = policy.initial_max_sleep_nanos
    retry_count: int = 0
    sysrandom: random.SystemRandom | None = None
    start_time_nanos: int = time.monotonic_ns()
    while True:
        try:
            return fn(Retry(retry_count))  # Call the target function and supply retry attempt number
        except RetryableError as retryable_error:
            elapsed_nanos: int = time.monotonic_ns() - start_time_nanos
            termination_event = _NO_TERMINATION_EVENT if termination_event is None else termination_event
            msg: str = display_msg + " " if display_msg else ""
            msg = msg + retryable_error.display_msg + " " if retryable_error.display_msg else msg
            msg = msg if msg else "Retrying "
            will_retry: bool = False
            if retry_count < policy.retries and elapsed_nanos < policy.max_elapsed_nanos and not termination_event.is_set():
                will_retry = True
                retry_count += 1
                retry_msg: str = f"{msg}[{retry_count}/{policy.retries}]"
                if retryable_error.no_sleep and retry_count <= 1:
                    log.info("%s", f"{retry_msg} immediately ...")
                else:  # jitter: pick a random sleep duration within the range [min_sleep_nanos, c_max_sleep_nanos] as delay
                    sysrandom = random.SystemRandom() if sysrandom is None else sysrandom
                    sleep_nanos: int = sysrandom.randint(policy.min_sleep_nanos, c_max_sleep_nanos)
                    log.info("%s", f"{retry_msg} in {human_readable_duration(sleep_nanos)} ...")
                    termination_event.wait(sleep_nanos / 1_000_000_000)  # seconds
                    c_max_sleep_nanos = min(2 * c_max_sleep_nanos, policy.max_sleep_nanos)  # exponential backoff with cap
            if termination_event.is_set() or not will_retry:
                if policy.retries > 0 and not termination_event.is_set():
                    log.warning(
                        "%s",
                        f"{msg}exhausted; giving up because the last [{retry_count}/{policy.retries}] retries across "
                        f"[{human_readable_duration(elapsed_nanos)}/{human_readable_duration(policy.max_elapsed_nanos)}] "
                        "failed",
                    )
                cause: BaseException | None = retryable_error.__cause__
                assert cause is not None
                raise cause.with_traceback(cause.__traceback__)  # noqa: B904 intentional re-raise of cause without chaining


#############################################################################
class RetryableError(Exception):
    """Indicates that the task that caused the underlying exception can be retried and might eventually succeed."""

    def __init__(self, message: str, display_msg: str = "", no_sleep: bool = False) -> None:
        super().__init__(message)
        self.display_msg: str = display_msg
        self.no_sleep: bool = no_sleep


#############################################################################
@dataclass(frozen=True)
class Retry:
    """The current retry attempt number provided to the callable."""

    count: int
