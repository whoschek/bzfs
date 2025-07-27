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
"""Parallel execution utilities for I/O-bound operations, with configurable result ordering."""

from __future__ import annotations
import concurrent
import itertools
import os
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor
from typing import (
    Any,
    Callable,
    Generator,
    Iterator,
)


def parallel_iterator(
    iterator_builder: Callable[[ThreadPoolExecutor], list[Iterator[Future[Any]]]],
    max_workers: int = os.cpu_count() or 1,
    ordered: bool = True,
) -> Generator[Any, None, Any]:
    """Executes multiple iterators in parallel/concurrently, with configurable result ordering.

    This function provides high-performance parallel execution of iterator-based tasks using a shared thread pool, with
    precise control over result delivery ordering and concurrency management through a sliding window buffer approach.

    Purpose:
    --------
    Enables parallel/concurrent execution of multiple iterator streams while providing either sequential (ordered) or
    optimized-latency (unordered) result delivery. Primarily designed for I/O-bound operations like ZFS/SSH command
    execution where parallelism significantly improves throughput.

    Assumptions:
    ------------
    - Iterator builder creates iterators that yield properly submitted Future objects
    - Tasks are primarily I/O-bound and benefit from parallel execution
    - Caller can handle potential exceptions propagated from Future.result()
    - Iterator builder properly handles ThreadPoolExecutor lifecycle

    Design Rationale:
    -----------------
    The design optimizes for bzfs's primary use case: executing similar ZFS/SSH commands across multiple remote systems
    where I/O or ZFS overhead dominates and parallel execution provides substantial performance improvements over
    sequential processing. The implementation addresses several key design challenges:

    - Sliding Window Buffer: Maintains at most max_workers futures in flight, preventing resource exhaustion and bounding
        memory consumption while maximizing thread utilization. This is crucial when processing large numbers of datasets
        typical in ZFS operation. New tasks are submitted as completed ones are consumed.

    - Ordered vs Unordered Execution:

       - Ordered mode uses FIFO queue (deque.popleft()) ensuring sequential delivery
       - Unordered mode uses concurrent.futures.wait(FIRST_COMPLETED) for minimal latency

    - Exception Propagation: Future.result() naturally propagates exceptions from worker threads, maintaining error
        visibility for debugging.

    Parameters:
    -----------
    iterator_builder : Callable[[ThreadPoolExecutor], list[Iterator[Future[Any]]]]
        Factory function that receives a ThreadPoolExecutor and returns a list of iterators. Each iterator should yield
        Future objects representing submitted tasks. The builder is called once with the managed thread pool.

    max_workers : int, default=os.cpu_count() or 1
        Maximum number of worker threads in the thread pool. Also determines the buffer size for the sliding window
        execution model. Should typically match available CPU cores for I/O-bound tasks.

    ordered : bool, default=True
        Controls result delivery mode:
        - True: Results yielded in same order as input iterators (FIFO)
        - False: Results yielded as soon as available (minimum latency)

    Yields:
    -------
    Results from completed Future objects, either in original order (ordered=True) or completion order (ordered=False).

    Raises:
    -------
    Any exception raised by the submitted tasks will be propagated when their results are consumed via Future.result().

    Example:
    --------
    # Parallel SSH command execution with ordered results
    def build_ssh_commands(executor):
        return [
            (executor.submit(run_ssh_cmd, cmd) for cmd in commands)
        ]

    for result in parallel_iterator(build_ssh_commands, max_workers=4, ordered=True):
        process_ssh_result(result)
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        iterators: list[Iterator[Future[Any]]] = iterator_builder(executor)
        assert isinstance(iterators, list)
        iterator: Iterator[Future[Any]] = itertools.chain(*iterators)
        iterators.clear()  # help gc
        # Materialize the next N futures into a buffer, causing submission + parallel execution of their CLI calls
        fifo_buffer: deque[Future[Any]] = deque(itertools.islice(iterator, max_workers))
        next_future: Future[Any] | None

        if ordered:
            while fifo_buffer:  # submit the next CLI call whenever the current CLI call returns
                curr_future: Future[Any] = fifo_buffer.popleft()
                next_future = next(iterator, None)  # causes the next CLI call to be submitted
                if next_future is not None:
                    fifo_buffer.append(next_future)
                yield curr_future.result()  # blocks until CLI returns
        else:
            todo_futures: set[Future[Any]] = set(fifo_buffer)
            fifo_buffer.clear()  # help gc
            done_futures: set[Future[Any]]
            while todo_futures:
                done_futures, todo_futures = concurrent.futures.wait(todo_futures, return_when=FIRST_COMPLETED)  # blocks
                for done_future in done_futures:  # submit the next CLI call whenever a CLI call returns
                    next_future = next(iterator, None)  # causes the next CLI call to be submitted
                    if next_future is not None:
                        todo_futures.add(next_future)
                    yield done_future.result()  # does not block as processing has already completed
        assert next(iterator, None) is None


def run_in_parallel(fn1: Callable[[], Any], fn2: Callable[[], Any]) -> tuple[Any, Any]:
    """perf: Runs both I/O functions in parallel/concurrently."""
    with ThreadPoolExecutor(max_workers=1) as executor:
        future: Future[Any] = executor.submit(fn2)  # async fn2
        result1 = fn1()  # blocks until fn1 call returns
        result2 = future.result()  # blocks until fn2 call returns
        return result1, result2
