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
import threading
import time
import unittest
from concurrent.futures import (
    Future,
    ProcessPoolExecutor,
)
from functools import (
    partial,
)
from typing import (
    Any,
    Callable,
)
from unittest.mock import (
    MagicMock,
)

from bzfs_main.util.parallel_tasktree import (
    BARRIER_CHAR,
    CompletionCallback,
    CompletionCallbackResult,
    ParallelTaskTree,
    _build_dataset_tree,
    _make_tree_node,
    _Tree,
    _TreeNode,
)
from bzfs_main.util.utils import (
    Comparable,
    SynchronousExecutor,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestBuildTree,
        TestProcessDatasetsInParallel,
        TestCustomPriorityOrder,
        TestBarriersCleared,
        TestProcessPoolExecutor,
        TestParallelTaskTreeBenchmark,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
def run_parallel_tasktree(**kwargs: Any) -> bool:
    return ParallelTaskTree(**kwargs).process_datasets_in_parallel()


def _run_complete_dataset_with_barriers(
    node: _TreeNode,
    no_skip: bool,
    priority: Callable[[str], Comparable],
) -> None:
    """Access the internal barrier handler without requiring full scheduler setup."""

    def _noop_process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
        return lambda todo_futures: CompletionCallbackResult(no_skip=True, fail=False)

    tasktree = ParallelTaskTree(
        log=MagicMock(logging.Logger),
        datasets=[],
        process_dataset=_noop_process_dataset,
        priority=priority,
        enable_barriers=True,
        is_test_mode=True,
    )
    tasktree._complete_dataset_with_barriers(node, no_skip)


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
        self.assertTrue(bool(str(_make_tree_node("foo", "foo", {}))))

    def test_termination_event_pre_set_stops_before_submission(self) -> None:
        """If termination_event is set before scheduling, no tasks are submitted and the run fails."""
        log = MagicMock(logging.Logger)
        datasets = ["a", "b", "c"]
        calls: list[str] = []

        def process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
            calls.append(dataset)

            def _completion_callback(todo_futures: set[Future[CompletionCallback]]) -> CompletionCallbackResult:
                return CompletionCallbackResult(no_skip=True, fail=False)

            return _completion_callback

        termination_event = threading.Event()
        termination_event.set()

        failed = run_parallel_tasktree(
            log=log,
            datasets=datasets,
            process_dataset=process_dataset,
            max_workers=2,
            termination_event=termination_event,
            enable_barriers=False,
            is_test_mode=True,
        )

        self.assertTrue(failed, "Termination should mark the run as failed")
        self.assertEqual(0, len(calls), "No dataset should be submitted when already terminated")

    def test_termination_event_set_during_sleep_stops_new_submissions(self) -> None:
        """When termination_event is set while the coordinator sleeps, it should wake early, stop submitting, and fail."""

        log = MagicMock(logging.Logger)
        datasets = ["a", "b", "c"]
        calls: list[str] = []
        termination_event = threading.Event()

        # Ensure the scheduler sleeps between submissions to hit the termination_event.wait() path
        def interval_nanos(last_update_nanos: int, dataset: str, submit_count: int) -> int:
            # Large enough to allow the background thread to set the event and wake the wait early
            return 500_000_000  # 0.5s

        def process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
            calls.append(dataset)

            def _completion_callback(todo_futures: set[Future[CompletionCallback]]) -> CompletionCallbackResult:
                return CompletionCallbackResult(no_skip=True, fail=False)

            return _completion_callback

        # Background thread that sets termination once the first task has been submitted
        def trigger_termination() -> None:
            # Busy-wait is fine here because it runs for a very short time and simplifies determinism
            while len(calls) < 1:
                time.sleep(0.005)
            # Give the coordinator a moment to enter wait(); then signal termination to wake it
            time.sleep(0.01)
            termination_event.set()

        t = threading.Thread(target=trigger_termination)
        t.start()

        failed = run_parallel_tasktree(
            log=log,
            datasets=datasets,
            process_dataset=process_dataset,
            max_workers=2,
            interval_nanos=interval_nanos,
            termination_event=termination_event,
            enable_barriers=False,
            is_test_mode=True,
        )

        t.join(timeout=2)

        self.assertTrue(failed, "Termination should mark the run as failed")
        # Exactly one submission is expected: first submitted before sleep, then termination prevents further submissions
        self.assertEqual(1, len(calls), f"Expected 1 submitted dataset, got {len(calls)}: {calls}")

    def test_rejects_empty_dataset_name(self) -> None:
        """An empty dataset string is invalid input."""

        log = MagicMock(logging.Logger)
        datasets = ["", "a"]

        def process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
            raise AssertionError("process_dataset should not be called for invalid input")

        with self.assertRaises(ValueError):
            run_parallel_tasktree(
                log=log,
                datasets=datasets,
                process_dataset=process_dataset,
                max_workers=2,
                is_test_mode=True,
            )

    def test_rejects_leading_slash_dataset_name(self) -> None:
        """A dataset name must not start with '/'."""

        log = MagicMock(logging.Logger)
        datasets = ["/a"]

        def process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
            raise AssertionError("process_dataset should not be called for invalid input")

        with self.assertRaisesRegex(ValueError, r"Invalid dataset name: /a"):
            run_parallel_tasktree(
                log=log,
                datasets=datasets,
                process_dataset=process_dataset,
                max_workers=2,
                is_test_mode=True,
            )

    def test_barrier_jobs_wait_for_missing_parent_node(self) -> None:
        """Barrier descendants must not start before sibling work finishes, even if their parent has no job."""

        log = MagicMock(logging.Logger)
        br = BARRIER_CHAR
        datasets = ["a/b/before", f"a/b/{br}/after"]

        started: list[str] = []
        started_too_early = threading.Event()
        parent_done = threading.Event()
        lock = threading.Lock()

        def process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
            with lock:
                started.append(dataset)

            if dataset == "a/b/before":
                time.sleep(0.05)  # keep the sibling runnable while this job executes

                def _completion_callback_before(
                    todo_futures: set[Future[CompletionCallback]],
                ) -> CompletionCallbackResult:
                    parent_done.set()
                    return CompletionCallbackResult(no_skip=True, fail=False)

                return _completion_callback_before

            if dataset == f"a/b/{br}/after" and not parent_done.is_set():
                started_too_early.set()

            def _completion_callback_after(
                todo_futures: set[Future[CompletionCallback]],
            ) -> CompletionCallbackResult:
                return CompletionCallbackResult(no_skip=True, fail=False)

            return _completion_callback_after

        failed = run_parallel_tasktree(
            log=log,
            datasets=datasets,
            process_dataset=process_dataset,
            max_workers=2,
            enable_barriers=True,
            is_test_mode=True,
        )

        self.assertFalse(failed)
        self.assertCountEqual(datasets, started)
        self.assertFalse(
            started_too_early.is_set(),
            msg=f"Barrier descendant started before parent branch completed: started={started}",
        )

    def test_root_level_barrier_waits_for_root(self) -> None:
        """A barrier child under the root must not start before the root completes."""

        log = MagicMock(logging.Logger)
        br = BARRIER_CHAR
        datasets = ["root", f"root/{br}/after"]

        started: list[str] = []
        root_done = threading.Event()
        started_too_early = threading.Event()
        lock = threading.Lock()

        def process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
            with lock:
                started.append(dataset)
            if dataset == "root":

                def _completion_callback(todo_futures: set[Future[CompletionCallback]]) -> CompletionCallbackResult:
                    root_done.set()
                    return CompletionCallbackResult(no_skip=True, fail=False)

                return _completion_callback

            if not root_done.is_set():
                started_too_early.set()

            def _completion_callback2(todo_futures: set[Future[CompletionCallback]]) -> CompletionCallbackResult:
                return CompletionCallbackResult(no_skip=True, fail=False)

            return _completion_callback2

        failed = run_parallel_tasktree(
            log=log,
            datasets=datasets,
            process_dataset=process_dataset,
            max_workers=2,
            is_test_mode=True,
        )

        self.assertFalse(failed)
        self.assertEqual(["root", f"root/{br}/after"], started)
        self.assertFalse(started_too_early.is_set(), msg=f"Barrier child ran before root: started={started}")

    def test_explicit_sync_executor_runs_inline_on_main_thread(self) -> None:
        """With an explicit SynchronousExecutor, tasks execute inline on the main thread.

        Uses a dataset list with siblings to prove that even with ``max_workers>1`` and parallelizable structure, an explicit
        synchronous executor forces in-thread execution and preserves deterministic order.
        """

        log = MagicMock(logging.Logger)
        datasets = ["a", "a/b", "a/c"]  # siblings under 'a'

        lock: threading.Lock = threading.Lock()
        main_ident: int = threading.get_ident()
        calls: list[tuple[str, int]] = []

        def process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
            with lock:
                calls.append((dataset, threading.get_ident()))

            def _completion_callback(todo_futures: set[Future[CompletionCallback]]) -> CompletionCallbackResult:
                return CompletionCallbackResult(no_skip=True, fail=False)

            return _completion_callback

        failed = run_parallel_tasktree(
            log=log,
            datasets=datasets,
            process_dataset=process_dataset,
            max_workers=4,
            executors=lambda: SynchronousExecutor(),
            enable_barriers=False,
            is_test_mode=True,
        )

        self.assertFalse(failed)
        # Expected order: root 'a' first, then its children in lexicographic order
        self.assertEqual([d for d, _ in calls], ["a", "a/b", "a/c"])
        self.assertTrue(all(tid == main_ident for _, tid in calls), msg=f"Calls not on main thread: {calls}")

    def test_parallel_tasktree_can_be_reused_across_runs(self) -> None:
        """A ParallelTaskTree instance can be used for multiple runs with fresh executors."""

        log = MagicMock(logging.Logger)
        datasets = ["a", "a/b", "a/c"]

        calls: list[tuple[int, str]] = []
        lock = threading.Lock()
        run_id = 0

        def process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
            nonlocal run_id
            with lock:
                calls.append((run_id, dataset))

            def _completion_callback(todo_futures: set[Future[CompletionCallback]]) -> CompletionCallbackResult:
                return CompletionCallbackResult(no_skip=True, fail=False)

            return _completion_callback

        tasktree = ParallelTaskTree(
            log=log,
            datasets=datasets,
            process_dataset=process_dataset,
            max_workers=2,
            executors=lambda: SynchronousExecutor(),
            enable_barriers=False,
            is_test_mode=True,
        )

        for i in range(2):
            run_id = i
            failed = tasktree.process_datasets_in_parallel()
            self.assertFalse(failed)

        # Expect each dataset to have been processed once per run
        self.assertEqual(2 * len(datasets), len(calls))
        for i in range(2):
            run_datasets = sorted(ds for rid, ds in calls if rid == i)
            self.assertEqual(sorted(datasets), run_datasets)


#############################################################################
class TestCustomPriorityOrder(unittest.TestCase):

    def test_custom_priority_orders_available_datasets(self) -> None:
        """Custom priority uses integer "size" (cost) per dataset to decide order among available datasets.

        Datasets with the largest size must be processed first, while always respecting the dependency rule that a parent
        must complete before any of its children can be processed. Two roots ("r" and "s") with children are used. With
        max_workers=1 the order is deterministic.
        """
        log = MagicMock(logging.Logger)

        # Sorted input list with two roots and children; no barriers involved
        datasets = ["r", "r/a", "r/b", "r/c", "s", "s/x", "s/y"]

        # Simulated dataset sizes (aka cost). Largest size should be picked first among available datasets.
        sizes: dict[str, int] = {
            "r": 10,
            "r/a": 30,
            "r/b": 20,
            "r/c": 40,
            "s": 50,
            "s/x": 5,
            "s/y": 60,
        }

        # Priority: smaller compares first; use negative size for max-heap behavior, add dataset for stable tie-break
        def priority(ds: str) -> tuple[int, str]:
            return (-sizes[ds], ds)

        calls: list[str] = []

        def process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
            calls.append(dataset)

            def _completion_callback(todo_futures: set[Future[CompletionCallback]]) -> CompletionCallbackResult:
                return CompletionCallbackResult(no_skip=True, fail=False)

            return _completion_callback

        failed = run_parallel_tasktree(
            log=log,
            datasets=datasets,
            process_dataset=process_dataset,
            priority=priority,
            max_workers=1,
            enable_barriers=False,
            is_test_mode=True,
        )

        self.assertFalse(failed)
        # Expected order determined by sizes while respecting dependencies:
        # Roots available initially: pick 's'(50) over 'r'(10); after 's', its children become available along with 'r'.
        # Pick 's/y'(60) over 'r'(10) and 's/x'(5); then 'r'(10) before the remaining 's/x'(5). After 'r', pick 'r/c'(40),
        # 'r/a'(30), 'r/b'(20), then last remaining 's/x'(5).
        expected = ["s", "s/y", "r", "r/c", "r/a", "r/b", "s/x"]
        self.assertEqual(expected, calls)


def make_tree_node(dataset: str, children: _Tree, parent: _TreeNode | None = None) -> _TreeNode:
    return _make_tree_node(priority=dataset, dataset=dataset, children=children, parent=parent)


#############################################################################
class TestBarriersCleared(unittest.TestCase):

    def test_failure_clears_ancestor_barriers_and_sets_empty_barrier(self) -> None:
        """Verifies that a first failure clears barriers for the node and all its ancestors and sets empty_barrier.

        This exercises the ancestor-walking guard by confirming barriers_cleared flags are set along the chain, and that
        barriers are set to the empty_barrier on each visited node when handling a failure without further propagation
        (pending > 0 suppresses the subsequent while-loop).
        """

        # Build a -> b -> c chain
        a = make_tree_node("a", {})
        b = make_tree_node("a/b", {}, parent=a)
        c = make_tree_node("a/b/c", {}, parent=b)

        # Prevent the completion-propagation while-loop from running to keep the test focused on the failure path only
        c.mut.pending = 1
        a.mut.pending = 0
        b.mut.pending = 0

        # First failure at deepest node
        _run_complete_dataset_with_barriers(c, no_skip=False, priority=lambda dataset: dataset)

        # Check that the node and its ancestors have barriers cleared and point to the empty_barrier
        self.assertTrue(c.mut.barriers_cleared)
        self.assertTrue(b.mut.barriers_cleared)
        self.assertTrue(a.mut.barriers_cleared)
        c_barrier = c.mut.barrier
        b_barrier = b.mut.barrier
        a_barrier = a.mut.barrier
        self.assertIsNotNone(c_barrier)
        self.assertIsNotNone(b_barrier)
        self.assertIsNotNone(a_barrier)
        assert c_barrier is not None and b_barrier is not None and a_barrier is not None
        self.assertEqual("empty_barrier", c_barrier.dataset)
        self.assertEqual("empty_barrier", b_barrier.dataset)
        self.assertEqual("empty_barrier", a_barrier.dataset)

    def test_ancestor_walking_stops_at_cleared_ancestor(self) -> None:
        """Second failure in the same subtree should stop clearing at the first ancestor with barriers_cleared set.

        We confirm by setting a custom barrier object on an ancestor and ensuring it remains unchanged after the second
        failure. We again keep pending > 0 to avoid the subsequent completion-propagation loop from running.
        """

        # Build a -> b -> c
        a = make_tree_node("a", {})
        b = make_tree_node("a/b", {}, parent=a)
        c = make_tree_node("a/b/c", {}, parent=b)

        # First failure clears barriers up to root
        c.mut.pending = 1  # suppress while-loop
        _run_complete_dataset_with_barriers(c, no_skip=False, priority=lambda dataset: dataset)

        # Verify barriers cleared
        self.assertTrue(a.mut.barriers_cleared)
        self.assertTrue(b.mut.barriers_cleared)
        self.assertTrue(c.mut.barriers_cleared)

        # Place a custom marker on ancestor 'a' to detect unwanted overwrites; ancestor walking must stop at 'b'.
        marker = make_tree_node("custom_marker", {})
        a.mut.barrier = marker

        # Now fail deeper sibling 'd' under 'b' and ensure 'a' stays untouched by the barrier-clearing loop
        d = make_tree_node("a/b/d", {}, parent=b)
        d.mut.pending = 1  # suppress while-loop
        _run_complete_dataset_with_barriers(d, no_skip=False, priority=lambda dataset: dataset)

        # 'd' gets barriers cleared; 'b' and 'a' remain with barriers cleared but 'a' barrier should still be the custom marker
        self.assertTrue(d.mut.barriers_cleared)
        self.assertTrue(b.mut.barriers_cleared)
        self.assertTrue(a.mut.barriers_cleared)
        self.assertIs(
            a.mut.barrier, marker, "Ancestor walking should stop at first cleared ancestor and not touch higher ancestors"
        )

    def test_early_break_can_open_ancestor_barrier_after_failure(self) -> None:
        """Demonstrate that breaking when encountering an empty_barrier on an intermediate ancestor would allow a higher
        ancestor barrier to open after a failure deeper in the tree; This exposes that an early-break optimization would be
        unsafe unless all higher ancestors are already set to the empty_barrier."""

        log = MagicMock(logging.Logger)
        br = BARRIER_CHAR

        # Tree under 'x':
        # - x/node/child -> success, which opens x/node barrier and enqueues x/node/~/bar/fail
        # - x/node/~/bar/fail -> failure (no_skip=False)
        # - x/other -> delayed completion to keep x.pending > 0 at the time of failure
        # - x/~/after -> barrier job at ancestor 'x' that must NOT start if any descendant fails
        datasets = [
            "x",
            "x/node",
            "x/node/child",
            f"x/node/{br}/bar/fail",
            "x/other",
            f"x/{br}/after",
        ]
        self.assertEqual(sorted(datasets), datasets)

        calls: list[str] = []
        lock = threading.Lock()
        failure_done = threading.Event()

        def record(dataset: str) -> None:
            with lock:
                calls.append(dataset)

        def process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
            # Simulate long-running sibling to keep ancestor 'x' pending > 0 during failure handling
            if dataset == "x/other":
                time.sleep(0.05)  # give others a head start
                failure_done.wait(timeout=2)  # finish only after failure has been handled

            record(dataset)

            def _completion_callback(todo_futures: set[Future[CompletionCallback]]) -> CompletionCallbackResult:
                if dataset == f"x/node/{br}/bar/fail":
                    # Signal that failure handling has run; return no_skip=False (skip subtree) but not fail the run
                    failure_done.set()
                    return CompletionCallbackResult(no_skip=False, fail=False)
                return CompletionCallbackResult(no_skip=True, fail=False)

            return _completion_callback

        failed = run_parallel_tasktree(
            log=log,
            datasets=datasets,
            process_dataset=process_dataset,
            max_workers=2,
            enable_barriers=True,
            is_test_mode=True,
        )

        # The run should not be marked failed since fail=False, but the ancestor barrier 'x/~/after' must NOT start.
        # If it did start, the early-break allowed opening of ancestor barrier after a failure.
        self.assertFalse(failed)
        self.assertNotIn(f"x/{br}/after", calls, msg=f"Ancestor barrier job should not start after failure, calls={calls}")


#############################################################################
# Top-level helpers used by TestProcessPoolExecutor pickling tests.
#
# Process pools require pickleable, importable callables. We return a functools.partial of a top-level function to carry
# simple picklable state.
PP_CALLS: list[tuple[str, int, int]] = []  # (dataset, worker_pid, main_pid_at_callback)


def pp_completion_callback(
    dataset: str, worker_pid: int, todo_futures: set[Future[CompletionCallback]]
) -> CompletionCallbackResult:
    """Top-level callback for ProcessPoolExecutor tests; appends a record and continues.

    Must remain at module scope to be pickleable for functools.partial().
    """
    PP_CALLS.append((dataset, worker_pid, os.getpid()))
    return CompletionCallbackResult(no_skip=True, fail=False)


def pp_process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
    """Top-level worker used by process pool tests; returns a pickleable callback.

    Binds only simple values (dataset, worker_pid) into the partial so unpickling is reliable across processes.
    """
    worker_pid: int = os.getpid()
    return partial(pp_completion_callback, dataset, worker_pid)


#############################################################################
class TestProcessPoolExecutor(unittest.TestCase):

    def test_process_pool_executor_with_picklable_callback_via_partial(self) -> None:
        """ProcessPoolExecutor works when process_dataset returns a pickleable callback (via partial).

        - Ensures the returned CompletionCallback is reconstructed in the coordinator process and can record results.
        - Confirms that workers ran in separate processes by comparing PIDs.
        - Validates dependency ordering: the root runs before its children; sibling completion order is not asserted.
        """

        # Reset global call log
        PP_CALLS.clear()

        log = MagicMock(logging.Logger)
        datasets = ["a", "a/b", "a/c"]  # siblings under 'a'
        main_pid = os.getpid()

        failed = run_parallel_tasktree(
            log=log,
            datasets=datasets,
            process_dataset=pp_process_dataset,
            max_workers=2,
            executors=lambda: ProcessPoolExecutor(max_workers=2),
            enable_barriers=False,
            is_test_mode=True,
        )

        self.assertFalse(failed)
        self.assertEqual(3, len(PP_CALLS), PP_CALLS)
        # Root must complete before children are scheduled
        self.assertEqual("a", PP_CALLS[0][0], PP_CALLS)
        # Sibling order may vary; assert set equality for the remaining two
        remaining = [rec[0] for rec in PP_CALLS[1:]]
        self.assertEqual({"a/b", "a/c"}, set(remaining), PP_CALLS)
        # At least one worker must be a separate process
        self.assertTrue(any(worker_pid != main_pid for _, worker_pid, _ in PP_CALLS), PP_CALLS)
        # Callback runs in the main process
        self.assertTrue(all(main_pid == cb_main_pid for _, _, cb_main_pid in PP_CALLS), PP_CALLS)


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

        def dummy_process_dataset(dataset: str, submit_count: int) -> CompletionCallback:
            """A dummy function that does nothing, to benchmark the framework overhead."""

            def _completion_callback(todo_futures: set[Future[CompletionCallback]]) -> CompletionCallbackResult:
                """A dummy function that never skips."""
                return CompletionCallbackResult(no_skip=True, fail=False)

            return _completion_callback

        log = logging.getLogger("TestParallelTaskTreeBenchmark")
        log.setLevel(logging.INFO)
        if not log.handlers:
            log.addHandler(logging.StreamHandler())

        datasets = self.generate_unique_datasets(num_datasets=num_datasets, length=100)

        import gc

        gc.collect()
        start_time = time.monotonic()

        failed = run_parallel_tasktree(
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
