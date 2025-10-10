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
import random
import time
from dataclasses import (
    dataclass,
)
from logging import (
    Logger,
)
from typing import (
    Any,
    Callable,
    TypeVar,
)

from bzfs_main.utils import (
    human_readable_duration,
)


#############################################################################
class RetryPolicy:
    """Configuration controlling retry counts and backoff delays."""

    def __init__(self, args: argparse.Namespace) -> None:
        """Option values for retries; reads from ArgumentParser via args."""
        # immutable variables:
        self.retries: int = args.retries
        self.min_sleep_secs: float = args.retry_min_sleep_secs
        self.initial_max_sleep_secs: float = args.retry_initial_max_sleep_secs
        self.max_sleep_secs: float = args.retry_max_sleep_secs
        self.max_elapsed_secs: float = args.retry_max_elapsed_secs
        self.min_sleep_nanos: int = int(self.min_sleep_secs * 1_000_000_000)
        self.initial_max_sleep_nanos: int = int(self.initial_max_sleep_secs * 1_000_000_000)
        self.max_sleep_nanos: int = int(self.max_sleep_secs * 1_000_000_000)
        self.max_elapsed_nanos: int = int(self.max_elapsed_secs * 1_000_000_000)
        self.min_sleep_nanos = max(1, self.min_sleep_nanos)
        self.max_sleep_nanos = max(self.min_sleep_nanos, self.max_sleep_nanos)
        self.initial_max_sleep_nanos = min(self.max_sleep_nanos, max(self.min_sleep_nanos, self.initial_max_sleep_nanos))
        assert self.min_sleep_nanos <= self.initial_max_sleep_nanos <= self.max_sleep_nanos

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
T = TypeVar("T")


def run_with_retries(log: Logger, policy: RetryPolicy, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Runs the given function with the given arguments, and retries on failure as indicated by policy."""
    c_max_sleep_nanos: int = policy.initial_max_sleep_nanos
    retry_count: int = 0
    sysrandom: random.SystemRandom | None = None
    start_time_nanos: int = time.monotonic_ns()
    while True:
        try:
            return fn(*args, **kwargs, retry=Retry(retry_count))  # Call the target function with provided args
        except RetryableError as retryable_error:
            elapsed_nanos: int = time.monotonic_ns() - start_time_nanos
            if retry_count < policy.retries and elapsed_nanos < policy.max_elapsed_nanos:
                retry_count += 1
                if retryable_error.no_sleep and retry_count <= 1:
                    log.info(f"Retrying [{retry_count}/{policy.retries}] immediately ...")
                else:  # jitter: pick a random sleep duration within the range [min_sleep_nanos, c_max_sleep_nanos] as delay
                    sysrandom = random.SystemRandom() if sysrandom is None else sysrandom
                    sleep_nanos: int = sysrandom.randint(policy.min_sleep_nanos, c_max_sleep_nanos)
                    log.info(f"Retrying [{retry_count}/{policy.retries}] in {human_readable_duration(sleep_nanos)} ...")
                    time.sleep(sleep_nanos / 1_000_000_000)
                    c_max_sleep_nanos = min(policy.max_sleep_nanos, 2 * c_max_sleep_nanos)  # exponential backoff with cap
            else:
                if policy.retries > 0:
                    log.warning(
                        f"Giving up because the last [{retry_count}/{policy.retries}] retries across "
                        f"[{human_readable_duration(elapsed_nanos)}/{human_readable_duration(policy.max_elapsed_nanos)}] "
                        "for the current request failed!"
                    )
                cause: BaseException | None = retryable_error.__cause__
                assert cause is not None
                raise cause.with_traceback(cause.__traceback__) from getattr(cause, "__cause__", None)


#############################################################################
class RetryableError(Exception):
    """Indicates that the task that caused the underlying exception can be retried and might eventually succeed."""

    def __init__(self, message: str, no_sleep: bool = False) -> None:
        super().__init__(message)
        # immutable variables:
        self.no_sleep: bool = no_sleep


#############################################################################
@dataclass(frozen=True)
class Retry:
    """The current retry attempt number provided to the callable."""

    count: int
