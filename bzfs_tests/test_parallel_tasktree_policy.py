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
"""Unit tests for the parallel task tree scheduling policy; Confirms job execution honors concurrency limits and
synchronization."""

from __future__ import (
    annotations,
)
import logging
import subprocess
import threading
import time
import unittest
from logging import (
    Logger,
)
from typing import (
    TYPE_CHECKING,
    Any,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from bzfs_main.util.parallel_tasktree import (
    BARRIER_CHAR,
)
from bzfs_main.util.parallel_tasktree_policy import (
    process_datasets_in_parallel_and_fault_tolerant,
)
from bzfs_main.util.retry import (
    RetryPolicy,
    RetryTemplate,
)
from bzfs_tests.tools import (
    stop_on_failure_subtest,
)

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from bzfs_main.util.retry import (
        Retry,
    )


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestProcessDatasetsInParallel,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestProcessDatasetsInParallel(unittest.TestCase):

    def setUp(self) -> None:
        self.lock: threading.Lock = threading.Lock()
        self.log = MagicMock(spec=Logger)
        self.log.isEnabledFor.side_effect = lambda level: level >= logging.INFO
        self.default_kwargs: dict[str, Any] = {
            "log": self.log,
            "skip_on_error": "dataset",
            "dry_run": False,
            "is_test_mode": True,
        }
        self.submitted: list[str] = []

    def append_submission(self, dataset: str) -> None:
        with self.lock:
            self.submitted.append(dataset)

    def test_basic(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            return True

        failed = process_datasets_in_parallel_and_fault_tolerant(
            datasets=[],
            skip_tree_on_error=lambda dataset: False,
            process_dataset=submit_no_skiptree,  # lambda
            **self.default_kwargs,
        )
        self.assertFalse(failed)
        self.assertListEqual([], self.submitted)

    def test_submit_no_skiptree(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            return True

        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.setUp()
                src_datasets = ["a1", "a1/b1", "a2"]
                if i > 0:
                    self.log.isEnabledFor.side_effect = lambda level: level >= logging.DEBUG
                    self.default_kwargs["retry_template"] = RetryTemplate[bool]().copy(policy=RetryPolicy.no_retries())
                failed = process_datasets_in_parallel_and_fault_tolerant(
                    datasets=src_datasets,
                    process_dataset=submit_no_skiptree,  # lambda
                    skip_tree_on_error=lambda dataset: False,
                    max_workers=1 if i > 0 else 8,
                    interval_nanos=lambda last_update_nanos, dataset, submit_count: 10_000_000,
                    task_name="mytask",
                    enable_barriers=i > 0,
                    **self.default_kwargs,
                )
                self.assertFalse(failed)
                self.assertListEqual(["a1", "a1/b1", "a2"], sorted(self.submitted))

    def test_submit_skiptree(self) -> None:
        def submit_skiptree(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            return False

        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.setUp()
                src_datasets = ["a1", "a1/b1", "a2"]
                failed = process_datasets_in_parallel_and_fault_tolerant(
                    datasets=src_datasets,
                    process_dataset=submit_skiptree,  # lambda
                    skip_tree_on_error=lambda dataset: False,
                    max_workers=8,
                    enable_barriers=i > 0,
                    **self.default_kwargs,
                )
                self.assertFalse(failed)
                self.assertListEqual(["a1", "a2"], sorted(self.submitted))

    def test_submit_zero_datasets(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            return True

        src_datasets: list[str] = []
        failed = process_datasets_in_parallel_and_fault_tolerant(
            datasets=src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: False,
            max_workers=8,
            **self.default_kwargs,
        )
        self.assertFalse(failed)
        self.assertListEqual([], sorted(self.submitted))

    def test_submit_timeout_with_skip_on_error_is_fail(self) -> None:
        def submit_raise_timeout(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            raise subprocess.TimeoutExpired("submit_raise_timeout", 10)

        src_datasets = ["a1", "a1/b1", "a2"]
        kwargs = self.default_kwargs
        kwargs.update({"skip_on_error": "fail"})

        with self.assertRaises(subprocess.TimeoutExpired):
            process_datasets_in_parallel_and_fault_tolerant(
                datasets=src_datasets,
                process_dataset=submit_raise_timeout,  # lambda
                skip_tree_on_error=lambda dataset: True,
                max_workers=1,
                **kwargs,
            )
        self.assertListEqual(["a1"], sorted(self.submitted))

    def test_submit_timeout_with_skip_on_error_is_not_fail(self) -> None:
        def submit_raise_timeout(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            raise subprocess.TimeoutExpired("submit_raise_timeout", 10)

        src_datasets = ["a1", "a1/b1", "a2"]
        failed = process_datasets_in_parallel_and_fault_tolerant(
            datasets=src_datasets,
            process_dataset=submit_raise_timeout,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            **self.default_kwargs,
        )
        self.assertTrue(failed)
        self.assertListEqual(["a1", "a2"], sorted(self.submitted))

    def test_cancel_pending_futures_when_fail_mode(self) -> None:
        """Covers the branch that cancels pending futures when skip_on_error == 'fail'.

        We run two datasets with max_workers=2. The first dataset raises immediately to trigger the fail-fast path in the
        policy callback, while the second sleeps briefly to ensure it remains in the todo_futures set. This guarantees the
        cancellation loop iterates at least once.
        """

        def process(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            if dataset == "a":
                raise subprocess.CalledProcessError(1, "cmd")
            # Keep the other future pending while the callback of the failing one runs
            time.sleep(0.05)
            return True

        src_datasets = ["a", "b"]

        # Patch Future.cancel to confirm the cancel loop is executed at least once.
        # Patch termination_handler to assert we hit the 'fail' branch.
        mock_terminate = MagicMock()
        with patch("concurrent.futures.Future.cancel", return_value=False) as mock_cancel:
            kwargs = dict(self.default_kwargs)
            kwargs["skip_on_error"] = "fail"
            with self.assertRaises(subprocess.CalledProcessError):
                process_datasets_in_parallel_and_fault_tolerant(
                    datasets=src_datasets,
                    process_dataset=process,  # lambda
                    skip_tree_on_error=lambda dataset: False,
                    max_workers=2,
                    termination_handler=mock_terminate,
                    enable_barriers=False,
                    **kwargs,
                )

        # Both datasets were submitted; the failing one raised, and the sleeping one remained pending
        # long enough to be cancelled via the policy branch.
        self.assertEqual(sorted(["a", "b"]), sorted(self.submitted))
        mock_terminate.assert_called_once()
        self.assertTrue(mock_cancel.called, "Expected at least one Future.cancel() call")

    def submit_raise_error(self, dataset: str, tid: str, retry: Retry) -> bool:
        self.append_submission(dataset)
        raise subprocess.CalledProcessError(1, "foo_cmd")

    def test_submit_raise_error_with_skip_tree_on_error_is_false(self) -> None:
        src_datasets = ["a1", "a1/b1", "a2"]
        failed = process_datasets_in_parallel_and_fault_tolerant(
            datasets=src_datasets,
            process_dataset=self.submit_raise_error,  # lambda
            skip_tree_on_error=lambda dataset: False,
            max_workers=8,
            **self.default_kwargs,
        )
        self.assertTrue(failed)
        self.assertListEqual(["a1", "a1/b1", "a2"], sorted(self.submitted))

    def test_submit_raise_error_with_skip_tree_on_error_is_true(self) -> None:
        src_datasets = ["a1", "a1/b1", "a2"]
        failed = process_datasets_in_parallel_and_fault_tolerant(
            datasets=src_datasets,
            process_dataset=self.submit_raise_error,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            **self.default_kwargs,
        )
        self.assertTrue(failed)
        self.assertListEqual(["a1", "a2"], sorted(self.submitted))

    def test_submit_barriers0a_no_skiptree(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            return True

        br = BARRIER_CHAR
        src_datasets = ["a/b/c", "a/b/c/0d", "a/b/c/1d", f"a/b/c/{br}/prune", f"a/b/c/{br}/prune/monitor"]
        failed = process_datasets_in_parallel_and_fault_tolerant(
            datasets=src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=True,
            **self.default_kwargs,
        )
        self.assertFalse(failed)
        self.assertListEqual(src_datasets, sorted(self.submitted))

    def test_submit_barriers0a_with_skiptree(self) -> None:
        def submit_with_skiptree(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            return dataset != "a/b/c/0d"

        br = BARRIER_CHAR
        src_datasets = ["a/b/c", "a/b/c/0d", "a/b/c/1d", f"a/b/c/{br}/prune", f"a/b/c/{br}/prune/monitor"]
        failed = process_datasets_in_parallel_and_fault_tolerant(
            datasets=src_datasets,
            process_dataset=submit_with_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=True,
            **self.default_kwargs,
        )
        self.assertFalse(failed)
        self.assertListEqual(["a/b/c", "a/b/c/0d", "a/b/c/1d"], sorted(self.submitted))

    def test_submit_barriers0b(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            return True

        src_datasets = ["a/b/c", "a/b/c/d/e/f", "u/v/w"]
        failed = process_datasets_in_parallel_and_fault_tolerant(
            datasets=src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=True,
            **self.default_kwargs,
        )
        self.assertFalse(failed)
        self.assertListEqual(src_datasets, sorted(self.submitted))

    def test_submit_barriers0c(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            return True

        src_datasets = ["a/b/c", "a/b/c/d/e/f", "u/v/w"]
        failed = process_datasets_in_parallel_and_fault_tolerant(
            datasets=src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=False,
            **self.default_kwargs,
        )
        self.assertFalse(failed)
        self.assertListEqual(src_datasets, sorted(self.submitted))

    def test_submit_barriers1(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            return True

        br = BARRIER_CHAR
        src_datasets = [
            "a/b/c",
            "a/b/c/0d",
            "a/b/c/1d",
            f"a/b/c/{br}/prune",
            f"a/b/c/{br}/prune/monitor",
            f"a/b/c/{br}/{br}/done",
        ]
        failed = process_datasets_in_parallel_and_fault_tolerant(
            datasets=src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=True,
            **self.default_kwargs,
        )
        self.assertFalse(failed)
        self.assertListEqual(src_datasets, sorted(self.submitted))

    def test_submit_barriers2(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            return True

        br = BARRIER_CHAR
        src_datasets = [
            "a/b/c",
            "a/b/c/0d",
            "a/b/c/1d",
            f"a/b/c/{br}/prune",
            f"a/b/c/{br}/prune/monitor",
            f"a/b/c/{br}/{br}/{br}/{br}/done",
        ]
        failed = process_datasets_in_parallel_and_fault_tolerant(
            datasets=src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=True,
            **self.default_kwargs,
        )
        self.assertFalse(failed)
        self.assertListEqual(src_datasets, sorted(self.submitted))

    def test_submit_barriers3(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: Retry) -> bool:
            self.append_submission(dataset)
            return True

        br = BARRIER_CHAR
        src_datasets = [
            "a/b/c",
            "a/b/c/0d",
            "a/b/c/1d",
            f"a/b/c/{br}/prune",
            f"a/b/c/{br}/prune/monitor",
            f"a/b/c/{br}/{br}/{br}/{br}",
        ]
        failed = process_datasets_in_parallel_and_fault_tolerant(
            datasets=src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=True,
            **self.default_kwargs,
        )
        self.assertFalse(failed)
        self.assertListEqual(src_datasets[0:-1], sorted(self.submitted))
