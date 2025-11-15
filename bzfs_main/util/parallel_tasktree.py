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
snapshot deletion.

This module contains only generic scheduling and coordination (the "algorithm"). Error handling, retries, and skip policies
are customizable and implemented by callers via the CompletionCallback API or wrappers such as ``parallel_tasktree_policy``.

Has zero dependencies beyond the Python standard library.
"""

from __future__ import (
    annotations,
)
import concurrent
import heapq
import os
import threading
import time
from concurrent.futures import (
    FIRST_COMPLETED,
    Executor,
    Future,
    ThreadPoolExecutor,
)
from logging import (
    Logger,
)
from typing import (
    Callable,
    Final,
    NamedTuple,
)

from bzfs_main.util.utils import (
    DONT_SKIP_DATASET,
    Comparable,
    Interner,
    SortedInterner,
    SynchronousExecutor,
    has_duplicates,
    has_siblings,
    is_descendant,
)

# constants:
BARRIER_CHAR: Final[str] = "~"


#############################################################################
_Tree = dict[str, "_Tree"]  # Type alias


def _build_dataset_tree(sorted_datasets: list[str]) -> _Tree:
    """Takes as input a sorted list of datasets and returns a sorted directory tree containing the same dataset names, in the
    form of nested dicts."""
    tree: _Tree = {}
    interner: Interner[str] = Interner()  # reduces memory footprint
    for dataset in sorted_datasets:
        current: _Tree = tree
        for component in dataset.split("/"):
            child: _Tree | None = current.get(component)
            if child is None:
                child = {}
                component = interner.intern(component)
                current[component] = child
            current = child
    shared_empty_leaf: _Tree = {}

    def compact(node: _Tree) -> None:
        """Tree with shared empty leaf nodes has some 30% lower memory footprint than the non-compacted version."""
        for key, child_node in node.items():
            if len(child_node) == 0:
                node[key] = shared_empty_leaf  # sharing is safe because the tree is treated as immutable henceforth
            else:
                compact(child_node)

    compact(tree)
    return tree


def _build_dataset_tree_and_find_roots(sorted_datasets: list[str], priority: Callable[[str], Comparable]) -> list[_TreeNode]:
    """For consistency, processing of a dataset only starts after processing of its ancestors has completed."""
    tree: _Tree = _build_dataset_tree(sorted_datasets)  # tree consists of nested dictionaries
    skip_dataset: str = DONT_SKIP_DATASET
    roots: list[_TreeNode] = []
    for dataset in sorted_datasets:
        if is_descendant(dataset, of_root_dataset=skip_dataset):
            continue
        skip_dataset = dataset
        children: _Tree = tree
        for component in dataset.split("/"):
            children = children[component]
        roots.append(_make_tree_node(priority(dataset), dataset, children))
    return roots


class _TreeNodeMutableAttributes:
    """Container for mutable attributes, stored space efficiently."""

    __slots__ = ("barrier", "barriers_cleared", "pending")  # uses more compact memory layout than __dict__

    def __init__(self) -> None:
        self.barrier: _TreeNode | None = None  # zero or one barrier TreeNode waiting for this node to complete
        self.pending: int = 0  # number of children added to priority queue that haven't completed their work yet
        self.barriers_cleared: bool = False  # irrevocably mark barriers of this node and all its ancestors as cleared?


class _TreeNode(NamedTuple):
    """Node in dataset dependency tree used by the scheduler; _TreeNodes are ordered by priority and dataset name within a
    priority queue, via __lt__ comparisons."""

    priority: Comparable  # determines the processing order once this dataset has become available for start of processing
    dataset: str  # each dataset name is unique; attribs other than `priority` and `dataset` are never used for comparisons
    children: _Tree  # dataset "directory" tree consists of nested dicts; aka Dict[str, Dict]
    parent: _TreeNode | None
    mut: _TreeNodeMutableAttributes

    def __repr__(self) -> str:
        priority, dataset, pending, barrier = self.priority, self.dataset, self.mut.pending, self.mut.barrier
        return str({"priority": priority, "dataset": dataset, "pending": pending, "barrier": barrier is not None})


def _make_tree_node(priority: Comparable, dataset: str, children: _Tree, parent: _TreeNode | None = None) -> _TreeNode:
    """Creates a TreeNode with mutable state container."""
    return _TreeNode(priority, dataset, children, parent, _TreeNodeMutableAttributes())


#############################################################################
class CompletionCallbackResult(NamedTuple):
    """Result of a CompletionCallback invocation."""

    no_skip: bool
    """True enqueues descendants, False skips the subtree."""

    fail: bool
    """True marks overall run as failed; exceptions may be raised here."""


CompletionCallback = Callable[[set[Future["CompletionCallback"]]], CompletionCallbackResult]  # Type alias
"""Callable that is run by the main coordination thread after a ``process_dataset()`` task finishes.

Purpose:
- Decide follow-up scheduling after a ``process_dataset()`` task finished.

Assumptions:
- Runs in the single coordination thread.
- May inspect and cancel in-flight futures to implement fail-fast semantics.
- If cancelling in-flight futures for tasks that spawn subprocesses (e.g. via subprocess.run()), callers should also
consider terminating the corresponding process subtree to avoid child processes lingering longer than desired. Skipping
termination will not hang the scheduler (workers will complete naturally), but those subprocesses may outlive cancellation
until they exit or time out.
"""


#############################################################################
def process_datasets_in_parallel(
    log: Logger,
    datasets: list[str],  # (sorted) list of datasets to process
    process_dataset: Callable[[str, int], CompletionCallback],  # lambda: dataset, tid; must be thread-safe
    priority: Callable[[str], Comparable] = lambda dataset: dataset,  # lexicographical order by default
    max_workers: int = os.cpu_count() or 1,
    executor: Executor | None = None,  # the Executor to submit tasks to; None means 'auto-choose'
    interval_nanos: Callable[
        [int, str, int], int
    ] = lambda last_update_nanos, dataset, submitted_count: 0,  # optionally spread tasks out over time; e.g. for jitter
    termination_event: threading.Event | None = None,  # optional event to request early async termination
    enable_barriers: bool | None = None,  # for testing only; None means 'auto-detect'
    is_test_mode: bool = False,
) -> bool:  # returns True if any dataset processing failed, False if all succeeded; thread-safe
    """Executes dataset processing operations in parallel with dependency-aware scheduling and fault tolerance.

    This function orchestrates parallel execution of dataset operations while maintaining strict hierarchical dependencies.
    Processing of a dataset only starts after processing of all its ancestor datasets has completed, ensuring data
    consistency during operations like ZFS replication or snapshot deletion.

    Purpose:
    --------
    - Process hierarchical datasets in parallel while respecting parent-child dependencies
    - Provide dependency-aware scheduling; error handling and retries are implemented by callers via ``CompletionCallback``
      or thin wrappers
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

    - Priority: Among the datasets available for start of processing, the "smallest" is always processed next, according to a
        customizable priority callback function, which by default sorts by lexicographical order (not dataset size), ensuring
        more deterministic execution order.

    Algorithm Selection:
    --------------------
    - Simple Algorithm (default): Used when no barriers ('~') are detected in dataset names. Provides efficient
        scheduling for standard parent-child dependencies via recursive child enqueueing after parent completion.

    - Barrier Algorithm (advanced): Activated when barriers are detected or explicitly enabled. Supports complex
        synchronization scenarios where jobs must wait for completion of entire subtrees before proceeding. Essential
        for advanced job scheduling patterns like "complete all parallel replications before starting pruning phase."

    - Both algorithms are CPU and memory efficient. They require main memory proportional to the number of datasets
        (~400 bytes per dataset), and easily scale to millions of datasets. Time complexity is O(N log N), where
        N is the number of datasets.

    Concurrency Design:
    -------------------
    By default uses ThreadPoolExecutor with configurable worker limits to balance parallelism against resource consumption.
    Optionally, plug in a custom Executor to submit tasks to scale-out clusters via frameworks like Ray Core or Dask, etc.
    The single-threaded coordination loop prevents race conditions while worker threads execute dataset operations in parallel.

    Params:
    -------
    - datasets: Sorted list of dataset names to process (must not contain duplicates)
    - process_dataset: Thread-safe Callback function to execute on each dataset; returns a CompletionCallback determining if
      to fail or skip subtree on error; CompletionCallback runs in the (single) main thread as part of the coordination loop.
    - priority: Callback function to determine dataset processing order; defaults to lexicographical order.
    - interval_nanos: Callback that returns a non-negative delay (ns) to add to ``next_update_nanos`` for
      jitter/back-pressure control; arguments are ``(last_update_nanos, dataset, submitted_count)``
    - max_workers: Maximum number of parallel worker threads
    - executor: the Executor to submit tasks to; None means 'auto-choose'
    - enable_barriers: Force enable/disable barrier algorithm (None = auto-detect)
    - termination_event: Optional event to request early async termination; stops new submissions and cancels in-flight tasks

    Returns:
    --------
        bool: True if any dataset processing failed, False if all succeeded

    Raises:
    -------
        AssertionError: If input validation fails (sorted order, no duplicates, etc.)
        Various exceptions: Propagated from process_dataset and its CompletionCallback
    """
    assert log is not None
    assert (not is_test_mode) or datasets == sorted(datasets), "List is not sorted"
    assert (not is_test_mode) or not has_duplicates(datasets), "List contains duplicates"
    assert callable(process_dataset)
    assert callable(priority)
    assert max_workers > 0
    assert callable(interval_nanos)
    termination_event = threading.Event() if termination_event is None else termination_event
    has_barrier: Final[bool] = any(BARRIER_CHAR in dataset.split("/") for dataset in datasets)
    assert (enable_barriers is not False) or not has_barrier, "Barriers seen in datasets but barriers explicitly disabled"
    barriers_enabled: Final[bool] = bool(has_barrier or enable_barriers)

    empty_barrier: Final[_TreeNode] = _make_tree_node("empty_barrier", "empty_barrier", {})  # immutable!
    datasets_set: Final[SortedInterner[str]] = SortedInterner(datasets)  # reduces memory footprint
    priority_queue: Final[list[_TreeNode]] = _build_dataset_tree_and_find_roots(datasets, priority)
    heapq.heapify(priority_queue)  # same order as sorted()
    if executor is None:
        is_parallel: bool = max_workers > 1 and len(datasets) > 1 and has_siblings(datasets)  # siblings can run in parallel
        executor = ThreadPoolExecutor(max_workers) if is_parallel else SynchronousExecutor()
    with executor:
        todo_futures: set[Future[CompletionCallback]] = set()
        future_to_node: dict[Future[CompletionCallback], _TreeNode] = {}
        submitted_count: int = 0
        next_update_nanos: int = time.monotonic_ns()
        wait_timeout: float | None = None
        failed: bool = False

        def submit_datasets() -> bool:
            """Submits available datasets to worker threads and returns False if all tasks have been completed."""
            nonlocal wait_timeout
            wait_timeout = None  # indicates to use blocking flavor of concurrent.futures.wait()
            while len(priority_queue) > 0 and len(todo_futures) < max_workers:
                # pick "smallest" dataset (wrt. sort order) available for start of processing; submit to thread pool
                nonlocal next_update_nanos
                sleep_nanos: int = next_update_nanos - time.monotonic_ns()
                if sleep_nanos > 0:
                    termination_event.wait(sleep_nanos / 1_000_000_000)  # allow early wakeup on async termination
                if termination_event.is_set():
                    break
                if sleep_nanos > 0 and len(todo_futures) > 0:
                    wait_timeout = 0  # indicates to use non-blocking flavor of concurrent.futures.wait()
                    # It's possible an even "smaller" dataset (wrt. sort order) has become available while we slept.
                    # If so it's preferable to submit to the thread pool the smaller one first.
                    break  # break out of loop to check if that's the case via non-blocking concurrent.futures.wait()
                node: _TreeNode = heapq.heappop(priority_queue)  # pick "smallest" dataset (wrt. sort order)
                nonlocal submitted_count
                submitted_count += 1
                next_update_nanos += max(0, interval_nanos(next_update_nanos, node.dataset, submitted_count))
                future: Future[CompletionCallback] = executor.submit(process_dataset, node.dataset, submitted_count)
                future_to_node[future] = node
                todo_futures.add(future)
            return len(todo_futures) > 0 and not termination_event.is_set()

        def complete_datasets() -> None:
            """Waits for completed futures, processes results and errors, then enqueues follow-up tasks per policy."""
            nonlocal failed
            nonlocal todo_futures
            done_futures: set[Future[CompletionCallback]]
            done_futures, todo_futures = concurrent.futures.wait(todo_futures, wait_timeout, return_when=FIRST_COMPLETED)
            for done_future in done_futures:
                done_node: _TreeNode = future_to_node.pop(done_future)
                c_callback: CompletionCallback = done_future.result()  # does not block as processing has already completed
                c_callback_result: CompletionCallbackResult = c_callback(todo_futures)
                no_skip: bool = c_callback_result.no_skip
                fail: bool = c_callback_result.fail
                failed = failed or fail
                if barriers_enabled:  # This barrier-based algorithm is for more general job scheduling, as in bzfs_jobrunner
                    _complete_job_with_barriers(done_node, no_skip, priority_queue, priority, datasets_set, empty_barrier)
                elif no_skip:  # This simple algorithm is sufficient for almost all use cases
                    _simple_enqueue_children(done_node, priority_queue, priority, datasets_set)

        # coordination loop; runs in the (single) main thread; submits tasks to worker threads and handles their results
        while submit_datasets():
            complete_datasets()

        if termination_event.is_set():
            for todo_future in todo_futures:
                todo_future.cancel()
            failed = failed or len(priority_queue) > 0 or len(todo_futures) > 0
            priority_queue.clear()
            todo_futures.clear()
            future_to_node.clear()
        assert len(priority_queue) == 0
        assert len(todo_futures) == 0
        assert len(future_to_node) == 0
        return failed


def _simple_enqueue_children(
    node: _TreeNode,
    priority_queue: list[_TreeNode],
    priority: Callable[[str], Comparable],
    datasets_set: SortedInterner[str],
) -> None:
    """Enqueues child nodes for start of processing."""
    for child, grandchildren in node.children.items():  # as processing of parent has now completed
        child_abs_dataset: str = datasets_set.interned(f"{node.dataset}/{child}")
        child_node: _TreeNode = _make_tree_node(priority(child_abs_dataset), child_abs_dataset, grandchildren)
        if child_abs_dataset in datasets_set:
            heapq.heappush(priority_queue, child_node)  # make it available for start of processing
        else:  # it's an intermediate node that has no job attached; pass the enqueue operation
            _simple_enqueue_children(child_node, priority_queue, priority, datasets_set)  # ... recursively down the tree


def _complete_job_with_barriers(
    node: _TreeNode,
    no_skip: bool,
    priority_queue: list[_TreeNode],
    priority: Callable[[str], Comparable],
    datasets_set: SortedInterner[str],
    empty_barrier: _TreeNode,
) -> None:
    """After successful completion, enqueues children, opens barriers, and propagates completion upwards.

    The (more complex) algorithm below is for more general job scheduling, as in bzfs_jobrunner. Here, a "dataset" string is
    treated as an identifier for any kind of job rather than a reference to a concrete ZFS object. An example "dataset" job
    string is "src_host1/createsnapshots/replicate_to_hostA". Jobs can depend on another job via a parent/child relationship
    formed by '/' directory separators within the dataset string, and multiple "datasets" form a job dependency tree by way
    of common dataset directory prefixes. Jobs that do not depend on each other can be executed in parallel, and jobs can be
    told to first wait for other jobs to complete successfully. The algorithm is based on a barrier primitive and is
    typically disabled. It is only required for rare jobrunner configs.

    For example, a job scheduler can specify that all parallel replications jobs to multiple destinations must succeed before
    the jobs of the pruning phase can start. More generally, with this algo, a job scheduler can specify that all jobs within
    a given job subtree (containing any nested combination of sequential and/or parallel jobs) must successfully complete
    before a certain other job within the job tree is started. This is specified via the barrier directory named '~'. An
    example is "src_host1/createsnapshots/~/prune".

    Note that '~' is unambiguous as it is not a valid ZFS dataset name component per the naming rules enforced by the 'zfs
    create', 'zfs snapshot' and 'zfs bookmark' CLIs.
    """

    def enqueue_children(node: _TreeNode) -> int:
        """Returns number of jobs that were added to priority_queue for immediate start of processing."""
        n: int = 0
        children: _Tree = node.children
        for child, grandchildren in children.items():
            abs_dataset: str = datasets_set.interned(f"{node.dataset}/{child}")
            child_node: _TreeNode = _make_tree_node(priority(abs_dataset), abs_dataset, grandchildren, parent=node)
            k: int
            if child != BARRIER_CHAR:
                if abs_dataset in datasets_set:
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
        # ... thus, opening the barrier shall always do nothing in node and its ancestors.
        # perf: Irrevocably mark (exactly once) barriers of this node and all its ancestors as cleared due to subtree skip,
        # via barriers_cleared=True. This enables to avoid redundant re-walking the entire ancestor chain on subsequent skip.
        tmp: _TreeNode | None = node
        while tmp is not None and not tmp.mut.barriers_cleared:
            tmp.mut.barriers_cleared = True
            tmp.mut.barrier = empty_barrier
            tmp = tmp.parent
    assert node.mut.pending >= 0
    while node.mut.pending == 0:  # have all jobs in subtree of current node completed?
        if no_skip:  # ... if so open the barrier, if it exists, and enqueue jobs waiting on it
            if not (node.mut.barrier is None or node.mut.barrier is empty_barrier):
                node.mut.pending += min(1, enqueue_children(node.mut.barrier))
        node.mut.barrier = empty_barrier
        if node.mut.pending > 0:  # did opening of barrier cause jobs to be enqueued in subtree?
            break  # ... if so we have not yet completed the subtree, so don't mark the subtree as completed yet
        if node.parent is None:
            break  # we've reached the root node
        node = node.parent  # recurse up the tree to propagate completion upward
        node.mut.pending -= 1  # mark subtree as completed
        assert node.mut.pending >= 0
