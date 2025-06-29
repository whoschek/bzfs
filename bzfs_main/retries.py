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

from __future__ import annotations
import argparse
import random
import time
from dataclasses import dataclass
from logging import Logger
from typing import (
    Any,
    Callable,
    TypeVar,
)

from bzfs_main.utils import human_readable_duration


#############################################################################
class RetryPolicy:
    def __init__(self, args: argparse.Namespace) -> None:
        """Option values for retries; reads from ArgumentParser via args."""
        # immutable variables:
        self.retries: int = args.retries
        self.min_sleep_secs: float = args.retry_min_sleep_secs
        self.max_sleep_secs: float = args.retry_max_sleep_secs
        self.max_elapsed_secs: float = args.retry_max_elapsed_secs
        self.min_sleep_nanos: int = int(self.min_sleep_secs * 1_000_000_000)
        self.max_sleep_nanos: int = int(self.max_sleep_secs * 1_000_000_000)
        self.max_elapsed_nanos: int = int(self.max_elapsed_secs * 1_000_000_000)
        self.min_sleep_nanos = max(1, self.min_sleep_nanos)
        self.max_sleep_nanos = max(self.min_sleep_nanos, self.max_sleep_nanos)

    def __repr__(self) -> str:
        return (
            f"retries: {self.retries}, min_sleep_secs: {self.min_sleep_secs}, "
            f"max_sleep_secs: {self.max_sleep_secs}, max_elapsed_secs: {self.max_elapsed_secs}"
        )


T = TypeVar("T")


def run_with_retries(log: Logger, policy: RetryPolicy, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Runs the given function with the given arguments, and retries on failure as indicated by policy."""
    max_sleep_mark = policy.min_sleep_nanos
    retry_count = 0
    sysrandom = None
    start_time_nanos = time.monotonic_ns()
    while True:
        try:
            return fn(*args, **kwargs, retry=Retry(retry_count))  # Call the target function with provided args
        except RetryableError as retryable_error:
            elapsed_nanos = time.monotonic_ns() - start_time_nanos
            if retry_count < policy.retries and elapsed_nanos < policy.max_elapsed_nanos:
                retry_count += 1
                if retryable_error.no_sleep and retry_count <= 1:
                    log.info(f"Retrying [{retry_count}/{policy.retries}] immediately ...")
                else:  # jitter: pick a random sleep duration within the range [min_sleep_nanos, max_sleep_mark] as delay
                    sysrandom = random.SystemRandom() if sysrandom is None else sysrandom
                    sleep_nanos = sysrandom.randint(policy.min_sleep_nanos, max_sleep_mark)
                    log.info(f"Retrying [{retry_count}/{policy.retries}] in {human_readable_duration(sleep_nanos)} ...")
                    time.sleep(sleep_nanos / 1_000_000_000)
                    max_sleep_mark = min(policy.max_sleep_nanos, 2 * max_sleep_mark)  # exponential backoff with cap
            else:
                if policy.retries > 0:
                    log.warning(
                        f"Giving up because the last [{retry_count}/{policy.retries}] retries across "
                        f"[{elapsed_nanos // 1_000_000_000}/{policy.max_elapsed_nanos // 1_000_000_000}] "
                        "seconds for the current request failed!"
                    )
                assert retryable_error.__cause__ is not None
                raise retryable_error.__cause__ from None


#############################################################################
class RetryableError(Exception):
    """Indicates that the task that caused the underlying exception can be retried and might eventually succeed."""

    def __init__(self, message: str, no_sleep: bool = False) -> None:
        super().__init__(message)
        self.no_sleep: bool = no_sleep


#############################################################################
@dataclass(frozen=True)
class Retry:
    count: int
