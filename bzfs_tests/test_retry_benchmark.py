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
"""Microbenchmarks for call_with_retries()."""

from __future__ import (
    annotations,
)
import importlib.util
import logging
import time
import unittest
from typing import (
    Any,
)

from bzfs_main.util.retry import (
    Retry,
    RetryableError,
    RetryConfig,
    RetryError,
    RetryPolicy,
    call_with_retries,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestCallWithRetriesBenchmark,
        TestTenacityBenchmark,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestCallWithRetriesBenchmark(unittest.TestCase):

    def test_benchmark_r1k_n0_p0_s0(self) -> None:
        self.benchmark(runs=1000, max_retries=0, max_previous_outcomes=0, success_on=0)

    def test_benchmark_r1k_n1_p0_s1(self) -> None:
        self.benchmark(runs=1000, max_retries=1, max_previous_outcomes=0, success_on=1)

    def test_benchmark_r1k_n2_p0_s2(self) -> None:
        self.benchmark(runs=1000, max_retries=2, max_previous_outcomes=0, success_on=2)

    def test_benchmark_r1k_n1_p1_s1(self) -> None:
        self.benchmark(runs=1000, max_retries=1, max_previous_outcomes=1, success_on=1)

    def test_benchmark_r1k_n0_p0_sinf(self) -> None:
        self.benchmark(runs=1000, max_retries=0, max_previous_outcomes=0)

    def test_benchmark_r1k_n1_p0_sinf(self) -> None:
        self.benchmark(runs=1000, max_retries=1, max_previous_outcomes=0)

    def test_benchmark_r1k_n2_p0_sinf(self) -> None:
        self.benchmark(runs=1000, max_retries=2, max_previous_outcomes=0)

    def test_benchmark_r1k_n5_p0_sinf(self) -> None:
        self.benchmark(runs=1000, max_retries=5, max_previous_outcomes=0)

    @unittest.skip("benchmark; enable for performance comparison")
    def test_xbenchmark_r100k_n1_p1_sinf(self) -> None:
        self.benchmark(runs=100_000, max_retries=1, max_previous_outcomes=1)

    def benchmark(
        self,
        runs: int,
        max_retries: int,
        max_previous_outcomes: int = 0,
        success_on: int = 1_000_000_000,
    ) -> None:
        retry_policy = RetryPolicy(
            max_retries=max_retries,
            min_sleep_secs=0,
            initial_max_sleep_secs=0,
            max_sleep_secs=0,
            max_elapsed_secs=1000,
            max_previous_outcomes=max_previous_outcomes,
            reraise=False,
        )
        retry_policy = retry_policy.copy(timing=retry_policy.timing.copy(on_before_attempt=lambda retry: None))
        iters = 0

        def fn(retry: Retry) -> Any:
            nonlocal iters
            iters += 1
            if retry.count >= success_on:
                return None
            raise RetryableError("fail")

        config = RetryConfig()
        try:  # warmup
            call_with_retries(fn, policy=retry_policy.copy(max_retries=100), config=config, log=None)
        except RetryError:
            pass

        log = logging.getLogger("TestCallWithRetriesBenchmark")
        log.setLevel(logging.INFO)
        if not log.handlers:
            log.addHandler(logging.StreamHandler())

        import gc

        gc.collect()

        iters = 0
        start = time.perf_counter()
        for _ in range(runs):
            try:
                call_with_retries(fn, policy=retry_policy, config=config, log=None)
            except RetryError:
                pass

        elapsed_secs = time.perf_counter() - start
        iters_per_sec = float("inf") if elapsed_secs <= 0 else iters / elapsed_secs
        runs_per_sec = float("inf") if elapsed_secs <= 0 else runs / elapsed_secs
        log.info(
            f"call_with_retries benchmark: "
            f"runs={runs}, "
            f"max_retries={max_retries}, "
            f"success_on={success_on}, "
            f"max_previous_outcomes={max_previous_outcomes}, "
            f"runs/sec={runs_per_sec:.0f}, "
            f"iters/sec={iters_per_sec:.0f}, "
            f"elapsed={elapsed_secs:.3f}s"
        )


#############################################################################
@unittest.skipIf(importlib.util.find_spec("tenacity") is None, "tenacity not installed")
class TestTenacityBenchmark(unittest.TestCase):
    """Compare `call_with_retries()` with a tenacity-based version.

    Purpose: Provide a baseline for microbenchmarking.
    Assumptions: tenacity is installed; otherwise the tests are skipped.
    Design: Mirror `TestCallWithRetriesBenchmark` scenarios for comparability.
    """

    def test_benchmark_r1k_n0_p0_s0(self) -> None:
        self.benchmark(runs=1000, max_retries=0, max_previous_outcomes=0, success_on=0)

    def test_benchmark_r1k_n1_p0_s1(self) -> None:
        self.benchmark(runs=1000, max_retries=1, max_previous_outcomes=0, success_on=1)

    def test_benchmark_r1k_n2_p0_s2(self) -> None:
        self.benchmark(runs=1000, max_retries=2, max_previous_outcomes=0, success_on=2)

    def test_benchmark_r1k_n1_p1_s1(self) -> None:
        self.benchmark(runs=1000, max_retries=1, max_previous_outcomes=1, success_on=1)

    def test_benchmark_r1k_n0_p0_sinf(self) -> None:
        self.benchmark(runs=1000, max_retries=0, max_previous_outcomes=0)

    def test_benchmark_r1k_n1_p0_sinf(self) -> None:
        self.benchmark(runs=1000, max_retries=1, max_previous_outcomes=0)

    def test_benchmark_r1k_n2_p0_sinf(self) -> None:
        self.benchmark(runs=1000, max_retries=2, max_previous_outcomes=0)

    def test_benchmark_r1k_n5_p0_sinf(self) -> None:
        self.benchmark(runs=1000, max_retries=5, max_previous_outcomes=0)

    @unittest.skip("benchmark; enable for performance comparison")
    def test_xbenchmark_r100k_n1_p1_sinf(self) -> None:
        self.benchmark(runs=100_000, max_retries=1, max_previous_outcomes=1)

    def benchmark(
        self,
        runs: int,
        max_retries: int,
        max_previous_outcomes: int = 0,
        success_on: int = 1_000_000_000,
    ) -> None:
        from tenacity import RetryError as TenacityRetryError
        from tenacity import (
            Retrying,
            retry_if_exception_type,
            stop_after_attempt,
            wait_none,
        )

        previous_outcomes: tuple[object, ...] = ()

        def before_sleep(retry_state: Any) -> None:
            nonlocal previous_outcomes
            previous_outcomes = previous_outcomes[len(previous_outcomes) - max_previous_outcomes + 1 :] + (
                retry_state.outcome,
            )

        retrying = Retrying(
            retry=retry_if_exception_type(RetryableError),
            stop=stop_after_attempt(101),
            wait=wait_none(),
            before_sleep=before_sleep if max_previous_outcomes > 0 else None,
            reraise=False,
        )
        iters = 0

        def fn() -> None:
            nonlocal iters
            iters += 1
            count = retrying.statistics["attempt_number"] - 1
            if count >= success_on:
                return
            raise RetryableError("fail")

        previous_outcomes = ()
        try:  # warmup
            retrying(fn)
        except TenacityRetryError:
            pass

        retrying = Retrying(
            retry=retry_if_exception_type(RetryableError),
            stop=stop_after_attempt(max_retries + 1),
            wait=wait_none(),
            before_sleep=before_sleep if max_previous_outcomes > 0 else None,
            reraise=False,
        )

        log = logging.getLogger("TestTenacityBenchmark")
        log.setLevel(logging.INFO)
        if not log.handlers:
            log.addHandler(logging.StreamHandler())

        import gc

        gc.collect()

        iters = 0
        start = time.perf_counter()
        for _ in range(runs):
            previous_outcomes = ()
            try:
                retrying(fn)
            except TenacityRetryError:
                pass

        elapsed_secs = time.perf_counter() - start
        iters_per_sec = float("inf") if elapsed_secs <= 0 else iters / elapsed_secs
        runs_per_sec = float("inf") if elapsed_secs <= 0 else runs / elapsed_secs
        log.info(
            f"tenacity benchmark: "
            f"runs={runs}, "
            f"max_retries={max_retries}, "
            f"success_on={success_on}, "
            f"max_previous_outcomes={max_previous_outcomes}, "
            f"runs/sec={runs_per_sec:.0f}, "
            f"iters/sec={iters_per_sec:.0f}, "
            f"elapsed={elapsed_secs:.3f}s"
        )
