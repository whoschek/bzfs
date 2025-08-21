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
"""Fault-tolerant, dependency-aware scheduling and execution of parallel operations, ensuring that ancestor datasets finish
before descendants start; The design maximizes throughput while preventing inconsistent dataset states during replication or
snapshot deletion."""

from __future__ import annotations
import argparse
import concurrent
import heapq
import logging
import os
import subprocess
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor
from logging import Logger
from typing import (
    Any,
    Callable,
    Dict,
    NamedTuple,
)

from bzfs_main.retry import (
    Retry,
    RetryPolicy,
    run_with_retries,
)
from bzfs_main.utils import (
    DONT_SKIP_DATASET,
    Interner,
    SortedInterner,
    dry,
    has_duplicates,
    human_readable_duration,
    is_descendant,
    terminate_process_subtree,
)

# constants:
BARRIER_CHAR: str = "~"


#############################################################################
Tree = Dict[str, "Tree"]  # Type alias


def _build_dataset_tree(sorted_datasets: list[str]) -> Tree:
    """Takes as input a sorted list of datasets and returns a sorted directory tree containing the same dataset names, in the
    form of nested dicts."""
    tree: Tree = {}
    interner: Interner[str] = Interner()  # reduces memory footprint
    for dataset in sorted_datasets:
        current: Tree = tree
        for component in dataset.split("/"):
            child: Tree | None = current.get(component)
            if child is None:
                child = {}
                component = interner.intern(component)
                current[component] = child
            current = child
    shared_empty_leaf: Tree = {}

    def compact(node: Tree) -> None:
        """Tree with shared empty leaf nodes has some 30% lower memory footprint than the non-compacted version."""
        for key, child_node in node.items():
            if len(child_node) == 0:
                node[key] = shared_empty_leaf  # sharing is safe because the tree is treated as immutable henceforth
            else:
                compact(child_node)

    compact(tree)
    return tree


def _build_dataset_tree_and_find_roots(sorted_datasets: list[str]) -> list[TreeNode]:
    """For consistency, processing of a dataset only starts after processing of its ancestors has completed."""
    tree: Tree = _build_dataset_tree(sorted_datasets)  # tree consists of nested dictionaries
    skip_dataset: str = DONT_SKIP_DATASET
    roots: list[TreeNode] = []
    for dataset in sorted_datasets:
        if is_descendant(dataset, of_root_dataset=skip_dataset):
            continue
        skip_dataset = dataset
        children: Tree = tree
        for component in dataset.split("/"):
            children = children[component]
        roots.append(_make_tree_node(dataset, children))
    return roots


class TreeNodeMutableAttributes:
    """Container for mutable attributes, stored space efficiently."""

    __slots__ = ("barrier", "pending")  # uses more compact memory layout than __dict__

    def __init__(self) -> None:
        self.barrier: TreeNode | None = None  # zero or one barrier TreeNode waiting for this node to complete
        self.pending: int = 0  # number of children added to priority queue that haven't completed their work yet


class TreeNode(NamedTuple):
    """Node in dataset dependency tree used by the scheduler; TreeNodes are ordered by dataset name within a priority queue
    via __lt__ comparisons."""

    dataset: str  # Each dataset name is unique, thus attributes other than `dataset` are never used for comparisons
    children: Tree  # dataset "directory" tree consists of nested dicts; aka Dict[str, Dict]
    parent: Any  # aka TreeNode
    mut: TreeNodeMutableAttributes

    def __repr__(self) -> str:
        dataset, pending, barrier, nchildren = self.dataset, self.mut.pending, self.mut.barrier, len(self.children)
        return str({"dataset": dataset, "pending": pending, "barrier": barrier is not None, "nchildren": nchildren})


def _make_tree_node(dataset: str, children: Tree, parent: TreeNode | None = None) -> TreeNode:
    """Creates a TreeNode with mutable state container."""
    return TreeNode(dataset, children, parent, TreeNodeMutableAttributes())


def process_datasets_in_parallel_and_fault_tolerant(
    log: Logger,
    datasets: list[str],
    process_dataset: Callable[[str, str, Retry], bool],  # lambda[dataset, tid, Retry]; must be thread-safe
    skip_tree_on_error: Callable[[str], bool],  # lambda[dataset]
    skip_on_error: str = "fail",
    max_workers: int = os.cpu_count() or 1,
    interval_nanos: Callable[[str], int] = lambda dataset: 0,  # optionally, spread tasks out over time; e.g. for jitter
    task_name: str = "Task",
    enable_barriers: bool | None = None,  # for testing only; None means 'auto-detect'
    append_exception: Callable[[BaseException, str, str], None] = lambda ex, task_name, task_description: None,
    retry_policy: RetryPolicy | None = None,
    dry_run: bool = False,
    is_test_mode: bool = False,
) -> bool:
    """Executes dataset processing operations in parallel with dependency-aware scheduling and fault tolerance.

    This function orchestrates parallel execution of dataset operations while maintaining strict hierarchical
    dependencies. Processing of a dataset only starts after processing of all its ancestor datasets has completed,
    ensuring data consistency during operations like ZFS replication or snapshot deletion.

    Purpose:
    --------
    - Process hierarchical datasets in parallel while respecting parent-child dependencies
    - Provide fault tolerance with configurable error handling and retry mechanisms
    - Maximize throughput by processing independent dataset subtrees in parallel
    - Support complex job scheduling patterns via optional barrier synchronization

    Assumptions:
    -----------------
    - Input `datasets` list is sorted in lexicographical order (enforced in test mode)
    - Input `datasets` list contains no duplicate entries (enforced in test mode)
    - Dataset hierarchy is determined by slash-separated path components
    - The `process_dataset` callable is thread-safe and can be executed in parallel

    Design Rationale:
    -----------------
    - The implementation uses a priority queue-based scheduler that maintains two key invariants:

    - Dependency Ordering: Children are only made available for start of processing after their parent completes,
        preventing inconsistent dataset states.

    - Lexicographical Priority: Among the datasets available for start of processing, the lexicographically smallest
        is always processed next, ensuring more deterministic execution order.

    Algorithm Selection:
    --------------------
    - Simple Algorithm (default): Used when no barriers ('~') are detected in dataset names. Provides efficient
        scheduling for standard parent-child dependencies via recursive child enqueueing after parent completion.

    - Barrier Algorithm (advanced): Activated when barriers are detected or explicitly enabled. Supports complex
        synchronization scenarios where jobs must wait for completion of entire subtrees before proceeding. Essential
        for advanced job scheduling patterns like "complete all parallel replications before starting pruning phase."

    - Both algorithms are CPU and memory efficient. They require main memory proportional to the number of dataset
        names (~400 bytes per dataset name), and easily scale to millions of datasets. Time complexity is O(N log N),
        where N is the number of datasets.

    Error Handling Strategy:
    ------------------------
    - "fail": Immediately terminate all processing on first error (fail-fast)
    - "dataset": Skip failed dataset but continue processing others
    - "tree": Skip entire subtree rooted at failed dataset, determined by `skip_tree_on_error`

    Concurrency Design:
    -------------------
    Uses ThreadPoolExecutor with configurable worker limits to balance parallelism against resource consumption. The
    single-threaded coordination loop prevents race conditions while worker threads execute dataset operations in parallel.

    Params:
    -------
    - datasets: Sorted list of dataset names to process (must not contain duplicates)
    - process_dataset: Thread-safe function to execute on each dataset
    - skip_tree_on_error: Function determining whether to skip subtree on error
    - max_workers: Maximum number of parallel worker threads
    - enable_barriers: Force enable/disable barrier algorithm (None = auto-detect)

    Returns:
    --------
        bool: True if any dataset processing failed, False if all succeeded

    Raises:
    -------
        AssertionError: If input validation fails (sorted order, no duplicates, etc.)
        Various exceptions: Propagated from process_dataset when skip_on_error="fail"
    """
    assert log is not None
    assert (not is_test_mode) or datasets == sorted(datasets), "List is not sorted"
    assert (not is_test_mode) or not has_duplicates(datasets), "List contains duplicates"
    assert callable(process_dataset)
    assert callable(skip_tree_on_error)
    assert max_workers > 0
    assert callable(interval_nanos)
    assert "%" not in task_name
    has_barrier: bool = any(BARRIER_CHAR in dataset.split("/") for dataset in datasets)
    assert (enable_barriers is not False) or not has_barrier
    barriers_enabled: bool = bool(has_barrier or enable_barriers)
    assert callable(append_exception)
    if retry_policy is None:
        retry_policy = RetryPolicy(  # no retries
            argparse.Namespace(retries=0, retry_min_sleep_secs=0, retry_max_sleep_secs=0, retry_max_elapsed_secs=0)
        )
    is_debug: bool = log.isEnabledFor(logging.DEBUG)

    def _process_dataset(dataset: str, tid: str) -> bool:
        """Runs ``process_dataset`` with retries and logs duration."""
        start_time_nanos: int = time.monotonic_ns()
        try:
            return run_with_retries(log, retry_policy, process_dataset, dataset, tid)
        finally:
            if is_debug:
                elapsed_duration: str = human_readable_duration(time.monotonic_ns() - start_time_nanos)
                log.debug(dry(f"{tid} {task_name} done: %s took %s", dry_run), dataset, elapsed_duration)

    immutable_empty_barrier: TreeNode = _make_tree_node("immutable_empty_barrier", {})
    len_datasets: int = len(datasets)
    datasets_set: SortedInterner[str] = SortedInterner(datasets)  # reduces memory footprint
    priority_queue: list[TreeNode] = _build_dataset_tree_and_find_roots(datasets)
    heapq.heapify(priority_queue)  # same order as sorted()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        todo_futures: set[Future[bool]] = set()
        future_to_node: dict[Future[bool], TreeNode] = {}
        sentinel: Future[bool] = Future()
        submitted: int = 0
        next_update_nanos: int = time.monotonic_ns()
        fw_timeout: float | None = None

        def submit_datasets() -> bool:
            """Submits available datasets to worker threads and returns False if all tasks have been completed."""
            nonlocal fw_timeout
            fw_timeout = None  # indicates to use blocking flavor of concurrent.futures.wait()
            while len(priority_queue) > 0 and len(todo_futures) < max_workers:
                # pick "smallest" dataset (wrt. sort order) available for start of processing; submit to thread pool
                nonlocal next_update_nanos
                sleep_nanos: int = next_update_nanos - time.monotonic_ns()
                if sleep_nanos > 0:
                    time.sleep(sleep_nanos / 1_000_000_000)  # seconds
                if sleep_nanos > 0 and len(todo_futures) > 0:
                    fw_timeout = 0  # indicates to use non-blocking flavor of concurrent.futures.wait()
                    # It's possible an even "smaller" dataset (wrt. sort order) has become available while we slept.
                    # If so it's preferable to submit to the thread pool the smaller one first.
                    break  # break out of loop to check if that's the case via non-blocking concurrent.futures.wait()
                node: TreeNode = heapq.heappop(priority_queue)  # pick "smallest" dataset (wrt. sort order)
                next_update_nanos += max(0, interval_nanos(node.dataset))
                nonlocal submitted
                submitted += 1
                future: Future[bool] = executor.submit(_process_dataset, node.dataset, tid=f"{submitted}/{len_datasets}")
                future_to_node[future] = node
                todo_futures.add(future)
            return len(todo_futures) > 0

        # coordination loop; runs in the (single) main thread; submits tasks to worker threads and handles their results
        failed: bool = False
        while submit_datasets():
            done_futures: set[Future[bool]]
            done_futures, todo_futures = concurrent.futures.wait(todo_futures, fw_timeout, return_when=FIRST_COMPLETED)
            for done_future in done_futures:
                done_node: TreeNode = future_to_node.pop(done_future)
                dataset: str = done_node.dataset
                try:
                    no_skip: bool = done_future.result()  # does not block as processing has already completed
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired, SystemExit, UnicodeDecodeError) as e:
                    failed = True
                    if skip_on_error == "fail":
                        [todo_future.cancel() for todo_future in todo_futures]
                        terminate_process_subtree(except_current_process=True)
                        raise
                    no_skip = not (skip_on_error == "tree" or skip_tree_on_error(dataset))
                    log.error("%s", e)
                    append_exception(e, task_name, dataset)
                if barriers_enabled:  # This barrier-based algorithm is for more general job scheduling, as in bzfs_jobrunner
                    _complete_job_with_barriers(done_node, no_skip, priority_queue, datasets_set, immutable_empty_barrier)
                elif no_skip:  # This simple algorithm is sufficient for almost all use cases
                    _simple_enqueue_children(done_node, priority_queue, datasets_set)
            done_futures.clear()  # help gc
            done_future = sentinel  # help gc
        assert len(priority_queue) == 0
        assert len(todo_futures) == 0
        assert len(future_to_node) == 0
        return failed


def _simple_enqueue_children(node: TreeNode, priority_queue: list[TreeNode], datasets_set: SortedInterner[str]) -> None:
    """Enqueues child nodes for start of processing."""
    for child, grandchildren in node.children.items():  # as processing of parent has now completed
        child_abs_dataset: str = datasets_set.interned(f"{node.dataset}/{child}")
        child_node: TreeNode = _make_tree_node(child_abs_dataset, grandchildren)
        if child_abs_dataset in datasets_set:
            heapq.heappush(priority_queue, child_node)  # make it available for start of processing
        else:  # it's an intermediate node that has no job attached; pass the enqueue operation
            _simple_enqueue_children(child_node, priority_queue, datasets_set)  # ... recursively down the tree


def _complete_job_with_barriers(
    node: TreeNode,
    no_skip: bool,
    priority_queue: list[TreeNode],
    datasets_set: SortedInterner[str],
    immutable_empty_barrier: TreeNode,
) -> None:
    """After successful completion, enqueues children, opens barriers, and propagates completion upwards.

    The (more complex) algorithm below is for more general job scheduling, as in bzfs_jobrunner. Here, a "dataset" string is
    treated as an identifier for any kind of job rather than a reference to a concrete ZFS object. An example "dataset" job
    string is "src_host1/createsnapshot/push/prune". Jobs can depend on another job via a parent/child relationship formed by
    '/' directory separators within the dataset string, and multiple "datasets" form a job dependency tree by way of common
    dataset directory prefixes. Jobs that do not depend on each other can be executed in parallel, and jobs can be told to
    first wait for other jobs to complete successfully. The algorithm is based on a barrier primitive and is typically
    disabled". It is only required for rare jobrunner configs.

    For example, a job scheduler can specify that all parallel push replications jobs to multiple destinations must succeed
    before the jobs of the pruning phase can start. More generally, with this algo, a job scheduler can specify that all jobs
    within a given job subtree (containing any nested combination of sequential and/or parallel jobs) must successfully
    complete before a certain other job within the job tree is started. This is specified via the barrier directory named
    '~'. An example is "src_host1/createsnapshot/~/prune".

    Note that '~' is unambiguous as it is not a valid ZFS dataset name component per the naming rules enforced by the 'zfs
    create', 'zfs snapshot' and 'zfs bookmark' CLIs.
    """

    def enqueue_children(node: TreeNode) -> int:
        """Returns number of jobs that were added to priority_queue for immediate start of processing."""
        n: int = 0
        children: Tree = node.children
        for child, grandchildren in children.items():
            child_abs_dataset: str = datasets_set.interned(f"{node.dataset}/{child}")
            child_node: TreeNode = _make_tree_node(child_abs_dataset, grandchildren, parent=node)
            k: int
            if child != BARRIER_CHAR:
                if child_abs_dataset in datasets_set:
                    # it's not a barrier; make job available for immediate start of processing
                    heapq.heappush(priority_queue, child_node)
                    k = 1
                else:  # it's an intermediate node that has no job attached; pass the enqueue operation
                    k = enqueue_children(child_node)  # ... recursively down the tree
            elif len(children) == 1:  # if the only child is a barrier then pass the enqueue operation
                k = enqueue_children(child_node)  # ... recursively down the tree
            else:  # park the barrier node within the (still closed) barrier for the time being
                assert node.mut.barrier is None
                node.mut.barrier = child_node
                k = 0
            node.mut.pending += min(1, k)
            n += k
        assert n >= 0
        return n

    if no_skip:
        enqueue_children(node)  # make child datasets available for start of processing
    else:  # job completed without success
        tmp: TreeNode | None = node  # ... thus, opening the barrier shall always do nothing in node and its ancestors
        while tmp is not None:
            tmp.mut.barrier = immutable_empty_barrier
            tmp = tmp.parent
    assert node.mut.pending >= 0
    while node.mut.pending == 0:  # have all jobs in subtree of current node completed?
        if no_skip:  # ... if so open the barrier, if it exists, and enqueue jobs waiting on it
            if not (node.mut.barrier is None or node.mut.barrier is immutable_empty_barrier):
                node.mut.pending += min(1, enqueue_children(node.mut.barrier))
        node.mut.barrier = immutable_empty_barrier
        if node.mut.pending > 0:  # did opening of barrier cause jobs to be enqueued in subtree?
            break  # ... if so we have not yet completed the subtree, so don't mark the subtree as completed yet
        if node.parent is None:
            break  # we've reached the root node
        node = node.parent  # recurse up the tree to propagate completion upward
        node.mut.pending -= 1  # mark subtree as completed
        assert node.mut.pending >= 0
