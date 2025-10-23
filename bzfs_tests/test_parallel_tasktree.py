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
"""Unit tests for parallel task tree algorithm."""

from __future__ import (
    annotations,
)
import logging
import os
import random
import string
import time
import unittest
from concurrent.futures import Future
from typing import (
    Any,
)

from bzfs_main.parallel_tasktree import (
    BARRIER_CHAR,
    CompletionCallback,
    _build_dataset_tree,
    _make_tree_node,
    _Tree,
    process_datasets_in_parallel,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestBuildTree,
        TestProcessDatasetsInParallel,
        TestParallelTaskTreeBenchmark,
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
        expected_tree: _Tree = {}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)

    def test_single_root(self) -> None:
        datasets: list[str] = ["pool"]
        expected_tree: _Tree = {"pool": {}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_single_branch(self) -> None:
        datasets: list[str] = ["pool/dataset/sub/child"]
        expected_tree: _Tree = {"pool": {"dataset": {"sub": {"child": {}}}}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots(self) -> None:
        datasets: list[str] = ["pool", "otherpool", "anotherpool"]
        expected_tree: _Tree = {"anotherpool": {}, "otherpool": {}, "pool": {}}
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
        expected_tree: _Tree = {"pool": {"parent": {"child1": {}, "child2": {"grandchild": {}}, "child3": {}}}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_no_children(self) -> None:
        datasets: list[str] = ["pool", "otherpool"]
        expected_tree: _Tree = {"otherpool": {}, "pool": {}}
        tree = _build_dataset_tree(sorted(datasets))
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_single_level(self) -> None:
        datasets: list[str] = ["pool", "pool1", "pool2", "pool3"]
        expected_tree: _Tree = {"pool": {}, "pool1": {}, "pool2": {}, "pool3": {}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots_with_hierarchy(self) -> None:
        datasets: list[str] = ["pool", "pool1", "pool1/dataset1", "pool2", "pool2/dataset2", "pool2/dataset2/sub", "pool3"]
        expected_tree: _Tree = {"pool": {}, "pool1": {"dataset1": {}}, "pool2": {"dataset2": {"sub": {}}}, "pool3": {}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots_flat(self) -> None:
        datasets: list[str] = ["root1", "root2", "root3", "root4"]
        expected_tree: _Tree = {"root1": {}, "root2": {}, "root3": {}, "root4": {}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots_mixed_depth(self) -> None:
        datasets: list[str] = ["a", "a/b", "a/b/c", "x", "x/y", "z", "z/1", "z/2", "z/2/3"]
        expected_tree: _Tree = {"a": {"b": {"c": {}}}, "x": {"y": {}}, "z": {"1": {}, "2": {"3": {}}}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)

    def test_tree_with_missing_intermediate_nodes(self) -> None:
        datasets: list[str] = ["a", "a/b/c", "z/2/3"]
        expected_tree: _Tree = {"a": {"b": {"c": {}}}, "z": {"2": {"3": {}}}}
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
        expected_tree: _Tree = {"a": {"b": {"c": {"0d": {}, "1d": {}, br: {"prune": {"monitor": {}}, br: {"done": {}}}}}}}
        tree = _build_dataset_tree(datasets)
        self.assertEqual(expected_tree, tree)
        self.assert_keys_sorted(tree)


#############################################################################
class TestProcessDatasetsInParallel(unittest.TestCase):

    def test_str_treenode(self) -> None:
        self.assertTrue(bool(str(_make_tree_node("foo", {}))))


#############################################################################
class TestParallelTaskTreeBenchmark(unittest.TestCase):

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

        def dummy_process_dataset(dataset: str, submitted_count: int) -> CompletionCallback:
            """A dummy function that does nothing, to benchmark the framework overhead."""

            def _completion_callback(todo_futures: set[Future[CompletionCallback]]) -> tuple[bool, bool]:
                """A dummy function that never skips."""
                no_skip = True
                fail = False
                return no_skip, fail

            return _completion_callback

        log = logging.getLogger("TestParallelTaskTreeBenchmark")
        log.setLevel(logging.INFO)
        if not log.handlers:
            log.addHandler(logging.StreamHandler())

        datasets = self.generate_unique_datasets(num_datasets=num_datasets, length=100)

        import gc

        gc.collect()
        start_time = time.monotonic()

        failed = process_datasets_in_parallel(
            log=log,
            datasets=datasets,
            process_dataset=dummy_process_dataset,
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
