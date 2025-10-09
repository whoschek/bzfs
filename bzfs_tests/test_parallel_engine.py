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
"""Unit tests for the parallel engine managing work tasks; Confirms job execution honors concurrency limits and
synchronization."""

from __future__ import (
    annotations,
)
import argparse
import logging
import os
import random
import string
import subprocess
import threading
import time
import unittest
from logging import (
    Logger,
)
from typing import (
    Any,
)
from unittest.mock import (
    MagicMock,
)

from bzfs_main.parallel_engine import (
    BARRIER_CHAR,
    Tree,
    _build_dataset_tree,
    _make_tree_node,
    process_datasets_in_parallel_and_fault_tolerant,
)
from bzfs_main.retry import (
    Retry,
    RetryPolicy,
)
from bzfs_tests.tools import (
    stop_on_failure_subtest,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestBuildTree,
        TestProcessDatasetsInParallel,
        TestParallelEngineBenchmark,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestBuildTree(unittest.TestCase):

    def assert_keys_sorted(self, tree: dict[str, Any]) -> None:
        keys = list(tree.keys())
        self.assertEqual(sorted(keys), keys, f"Keys are not sorted: {keys}")
        for value in tree.values():
            if isinstance(value, dict):
                self.assert_keys_sorted(value)

    def test_basic_tree(self) -> None:
        datasets = ["pool", "pool/dataset", "pool/dataset/sub", "pool/other", "pool/other/sub/child"]
        expected_tree = {"pool": {"dataset": {"sub": {}}, "other": {"sub": {"child": {}}}}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_empty_input(self) -> None:
        datasets: list[str] = []
        expected_tree: Tree = {}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)

    def test_single_root(self) -> None:
        datasets: list[str] = ["pool"]
        expected_tree: Tree = {"pool": {}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_single_branch(self) -> None:
        datasets: list[str] = ["pool/dataset/sub/child"]
        expected_tree: Tree = {"pool": {"dataset": {"sub": {"child": {}}}}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots(self) -> None:
        datasets: list[str] = ["pool", "otherpool", "anotherpool"]
        expected_tree: Tree = {"anotherpool": {}, "otherpool": {}, "pool": {}}
        tree = _build_dataset_tree(sorted(datasets))
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_large_dataset(self) -> None:
        datasets: list[str] = [f"pool/dataset{i}" for i in range(100)]
        tree = _build_dataset_tree(sorted(datasets))
        self.assertEqual(100, len(tree["pool"]))
        self.assert_keys_sorted(tree)

    def test_nested_structure(self) -> None:
        datasets: list[str] = [
            "pool/parent",
            "pool/parent/child1",
            "pool/parent/child2",
            "pool/parent/child2/grandchild",
            "pool/parent/child3",
        ]
        expected_tree: Tree = {"pool": {"parent": {"child1": {}, "child2": {"grandchild": {}}, "child3": {}}}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_no_children(self) -> None:
        datasets: list[str] = ["pool", "otherpool"]
        expected_tree: Tree = {"otherpool": {}, "pool": {}}
        tree = _build_dataset_tree(sorted(datasets))
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_single_level(self) -> None:
        datasets: list[str] = ["pool", "pool1", "pool2", "pool3"]
        expected_tree: Tree = {"pool": {}, "pool1": {}, "pool2": {}, "pool3": {}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots_with_hierarchy(self) -> None:
        datasets: list[str] = ["pool", "pool1", "pool1/dataset1", "pool2", "pool2/dataset2", "pool2/dataset2/sub", "pool3"]
        expected_tree: Tree = {"pool": {}, "pool1": {"dataset1": {}}, "pool2": {"dataset2": {"sub": {}}}, "pool3": {}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots_flat(self) -> None:
        datasets: list[str] = ["root1", "root2", "root3", "root4"]
        expected_tree: Tree = {"root1": {}, "root2": {}, "root3": {}, "root4": {}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots_mixed_depth(self) -> None:
        datasets: list[str] = ["a", "a/b", "a/b/c", "x", "x/y", "z", "z/1", "z/2", "z/2/3"]
        expected_tree: Tree = {"a": {"b": {"c": {}}}, "x": {"y": {}}, "z": {"1": {}, "2": {"3": {}}}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_tree_with_missing_intermediate_nodes(self) -> None:
        datasets: list[str] = ["a", "a/b/c", "z/2/3"]
        expected_tree: Tree = {"a": {"b": {"c": {}}}, "z": {"2": {"3": {}}}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_tree_with_barriers(self) -> None:
        br = BARRIER_CHAR
        datasets: list[str] = [
            "a/b/c",
            "a/b/c/0d",
            "a/b/c/1d",
            f"a/b/c/{br}/prune",
            f"a/b/c/{br}/prune/monitor",
            f"a/b/c/{br}/{br}/done",
        ]
        expected_tree: Tree = {"a": {"b": {"c": {"0d": {}, "1d": {}, br: {"prune": {"monitor": {}}, br: {"done": {}}}}}}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)


#############################################################################
class TestProcessDatasetsInParallel(unittest.TestCase):

    def setUp(self) -> None:
        self.lock: threading.Lock = threading.Lock()
        self.log = MagicMock(spec=Logger)
        self.log.isEnabledFor.side_effect = lambda level: level >= logging.INFO
        self.default_kwargs: dict[str, Any] = {
            "log": self.log,
            "skip_on_error": "dataset",
            "retry_policy": None,
            "dry_run": False,
            "is_test_mode": True,
        }
        self.submitted: list[str] = []

    def append_submission(self, dataset: str) -> None:
        with self.lock:
            self.submitted.append(dataset)

    def test_str_treenode(self) -> None:
        self.assertTrue(bool(str(_make_tree_node("foo", {}))))

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
                    self.default_kwargs["retry_policy"] = RetryPolicy(
                        argparse.Namespace(
                            retries=0,
                            retry_min_sleep_secs=0,
                            retry_max_sleep_secs=0,
                            retry_initial_max_sleep_secs=0,
                            retry_max_elapsed_secs=0,
                        )
                    )
                failed = process_datasets_in_parallel_and_fault_tolerant(
                    datasets=src_datasets,
                    process_dataset=submit_no_skiptree,  # lambda
                    skip_tree_on_error=lambda dataset: False,
                    max_workers=1 if i > 0 else 8,
                    interval_nanos=lambda dataset: time.monotonic_ns() + 10_000_000,
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


#############################################################################
class TestParallelEngineBenchmark(unittest.TestCase):

    @staticmethod
    def generate_unique_datasets(num_datasets: int, length: int) -> list[str]:
        # Create a realistic hierarchy to test tree-building performance
        # Example: tank/group_xxxx/host_yyyy/data_zzzzzzzzzzzz...
        datasets: set[str] = set()

        # Characters for random components
        chars = string.ascii_lowercase + string.digits

        while len(datasets) < num_datasets:
            # Generate components to ensure some path sharing
            l1 = "tank"
            l2 = f"group_{''.join(random.choices(chars, k=4))}"
            l3 = f"host_{''.join(random.choices(chars, k=4))}"

            # Ensure the final component is long enough to meet the total length
            prefix = f"{l1}/{l2}/{l3}/"
            remaining_len = length - len(prefix)
            if remaining_len <= 0:
                raise ValueError("Target length is too short for the fixed prefix.")

            leaf = "".join(random.choices(chars, k=remaining_len))
            dataset = prefix + leaf
            datasets.add(dataset)

        return sorted(datasets)

    def _run_benchmark(self, num_datasets: int, enable_barriers: bool, max_workers: int = 2 * (os.cpu_count() or 1)) -> None:
        def dummy_process_dataset(dataset: str, tid: str, retry: Retry) -> bool:
            """A dummy function that does nothing, to benchmark the framework overhead."""
            return True

        def dummy_skip_tree_on_error(dataset: str) -> bool:
            """A dummy function that never skips."""
            return False

        log = logging.getLogger("TestParallelEngineBenchmark")
        log.setLevel(logging.INFO)
        if not log.handlers:
            log.addHandler(logging.StreamHandler())

        datasets = self.generate_unique_datasets(num_datasets=num_datasets, length=100)

        import gc

        gc.collect()
        start_time = time.monotonic()

        failed = process_datasets_in_parallel_and_fault_tolerant(
            log=log,
            datasets=datasets,
            process_dataset=dummy_process_dataset,
            skip_tree_on_error=dummy_skip_tree_on_error,
            max_workers=max_workers,
            enable_barriers=enable_barriers,
            is_test_mode=False,
        )

        end_time = time.monotonic()
        elapsed_time = end_time - start_time
        throughput = round(num_datasets / elapsed_time)
        log.info("=================================================")
        log.info(f"Results for datasets={num_datasets}, enable_barriers={enable_barriers}, max_workers={max_workers} ...")
        log.info(f"Total elapsed time: {elapsed_time:.2f} seconds")
        log.info(f"Throughput: {throughput} datasets/second")
        self.assertFalse(failed, "The process should not report failure.")

    def test_benchmark_10k_datasets(self) -> None:
        for enable_barriers in [False, True]:
            self._run_benchmark(num_datasets=10_000, enable_barriers=enable_barriers)

    @unittest.skip("benchmark; enable for performance comparison")
    def test_benchmark_100k_datasets(self) -> None:
        for enable_barriers in [False, True]:
            self._run_benchmark(num_datasets=100_000, enable_barriers=enable_barriers)

    @unittest.skip("benchmark; enable for performance comparison")
    def test_benchmark_1m_datasets(self) -> None:
        for enable_barriers in [False, True]:
            self._run_benchmark(num_datasets=1_000_000, enable_barriers=enable_barriers)
