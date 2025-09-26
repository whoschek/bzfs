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

from __future__ import (
    annotations,
)
import concurrent
import itertools
import os
import sys
from collections import (
    deque,
)
from collections.abc import (
    Iterable,
    Iterator,
)
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ThreadPoolExecutor,
)
from typing import (
    Callable,
    TypeVar,
)

T = TypeVar("T")


def parallel_iterator(
    iterator_builder: Callable[[ThreadPoolExecutor], Iterable[Iterable[Future[T]]]],
    max_workers: int = os.cpu_count() or 1,
    ordered: bool = True,
) -> Iterator[T]:
    """Executes multiple iterators in parallel/concurrently, with explicit backpressure and configurable result ordering;
    avoids pre-submitting the entire workload.

    This function provides high-performance parallel execution of iterator-based tasks using a shared thread pool, with
    precise control over result delivery ordering and concurrency management through a sliding window buffer approach.

    Purpose:
    --------
    Enables parallel/concurrent execution of multiple iterator streams while providing either sequential (ordered) or
    performance-optimized (unordered) result delivery. Primarily designed for I/O-bound operations like ZFS/SSH command
    execution where parallelism significantly improves throughput.

    Assumptions:
    ------------
    - The builder must submit tasks to the provided executor (e.g., via ``executor.submit(...)``) and yield an Iterator
      over the corresponding Future[T] objects.
    - Tasks are primarily I/O-bound and benefit from parallel execution
    - Caller can handle potential exceptions propagated from ``Future.result()``
    - The builder properly scopes any resources to the lifecycle of the provided ThreadPoolExecutor

    Design Rationale:
    -----------------
    The design optimizes for bzfs's primary use case: executing similar ZFS/SSH commands across multiple remote systems
    where I/O or ZFS overhead dominates and parallel execution provides substantial performance improvements over
    sequential processing. The implementation addresses several key design challenges:

    - Sliding Window Buffer: Maintains at most ``max_workers`` futures in flight, preventing resource exhaustion and
      bounding memory consumption while maximizing thread utilization. New tasks are submitted as completed ones are
      consumed. This is crucial when processing large numbers of datasets typical in ZFS operation.

    - Ordered vs Unordered Execution:

       - Ordered mode uses a FIFO queue (``deque.popleft()``) ensuring sequential delivery that preserves the order in
         which the builder's iterators yield Futures (i.e., the chain order), regardless of completion order.
       - Unordered mode uses ``concurrent.futures.wait(FIRST_COMPLETED)`` to yield results as soon as they complete for
         minimum end-to-end latency and maximum throughput.

    - Exception Propagation: ``Future.result()`` naturally propagates exceptions from worker threads, maintaining error
      visibility for debugging.

    Parameters:
    -----------
    iterator_builder : Callable[[ThreadPoolExecutor], Iterable[Iterable[Future[T]]]]
        Factory function that receives a ThreadPoolExecutor and returns a series of iterators. Each iterator must yield
        Future[T] objects representing tasks that have already been submitted to the executor. The builder is called once
        with the managed thread pool.

    max_workers : int, default=os.cpu_count() or 1
        Maximum number of worker threads in the thread pool. Also determines the buffer size for the sliding window
        execution model. Often higher than the number of available CPU cores for I/O-bound tasks.

    ordered : bool, default=True
        Controls result delivery mode:
        - True: Results are yielded in the same order as produced by the builder's iterators (FIFO across the chained
          iterators), not by task completion order.
        - False: Results are yielded as soon as available (completion order) for minimum latency and maximum throughput.

    Yields:
    -------
    Results from completed Future objects, either in iterator order (``ordered=True``) or completion order
    (``ordered=False``).

    Raises:
    -------
    Any exception raised by the submitted tasks will be propagated when their results are consumed via ``Future.result()``.

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
        iterator: Iterator[Future[T]] = itertools.chain.from_iterable(iterator_builder(executor))
        # Materialize the next N futures into a buffer, causing submission + parallel execution of their CLI calls
        fifo_buffer: deque[Future[T]] = deque(itertools.islice(iterator, max_workers))
        sentinel: Future[T] = Future()
        next_future: Future[T]

        if ordered:
            while fifo_buffer:  # submit the next CLI call whenever the current CLI call returns
                curr_future: Future[T] = fifo_buffer.popleft()
                next_future = next(iterator, sentinel)  # keep the buffer full; causes the next CLI call to be submitted
                if next_future is not sentinel:
                    fifo_buffer.append(next_future)
                yield curr_future.result()  # blocks until CLI returns
        else:
            todo_futures: set[Future[T]] = set(fifo_buffer)
            fifo_buffer.clear()  # help gc
            done_futures: set[Future[T]]
            while todo_futures:
                done_futures, todo_futures = concurrent.futures.wait(todo_futures, return_when=FIRST_COMPLETED)  # blocks
                while done_futures:  # submit the next CLI call whenever a CLI call returns
                    next_future = next(iterator, sentinel)  # keep the buffer full; causes the next CLI call to be submitted
                    if next_future is not sentinel:
                        todo_futures.add(next_future)
                    yield done_futures.pop().result()  # does not block as processing has already completed
        assert next(iterator, sentinel) is sentinel


K = TypeVar("K")
V = TypeVar("V")


def run_in_parallel(fn1: Callable[[], K], fn2: Callable[[], V]) -> tuple[K, V]:
    """perf: Runs both I/O functions in parallel/concurrently."""
    with ThreadPoolExecutor(max_workers=1) as executor:
        future: Future[V] = executor.submit(fn2)  # async fn2
        result1: K = fn1()  # blocks until fn1 call returns
        result2: V = future.result()  # blocks until fn2 call returns
        return result1, result2


def batch_cmd_iterator(
    cmd_args: Iterable[str],  # list of arguments to be split across one or more commands
    fn: Callable[[list[str]], T],  # callback that runs a CLI command with a single batch
    max_batch_items: int = 2**29,  # max number of args per batch
    max_batch_bytes: int = 127 * 1024,  # max number of bytes per batch
    sep: str = " ",  # separator between batch args
) -> Iterator[T]:
    """Returns an iterator that runs fn(cmd_args) in sequential batches, without creating a cmdline that's too big for the OS
    to handle; Can be seen as a Pythonic xargs -n / -s with OS-aware safety margin.

    Except for the max_batch_bytes logic, this is essentially the same as:
    >>>
    while batch := itertools.batched(cmd_args, max_batch_items):  # doctest: +SKIP
        yield fn(batch)
    """
    assert isinstance(sep, str)
    fsenc: str = sys.getfilesystemencoding()
    seplen: int = len(sep.encode(fsenc))
    batch: list[str]
    batch, total_bytes, total_items = [], 0, 0
    for cmd_arg in cmd_args:
        arg_bytes: int = seplen + len(cmd_arg.encode(fsenc))
        if (total_items >= max_batch_items or total_bytes + arg_bytes > max_batch_bytes) and len(batch) > 0:
            yield fn(batch)
            batch, total_bytes, total_items = [], 0, 0
        batch.append(cmd_arg)
        total_bytes += arg_bytes
        total_items += 1
    if len(batch) > 0:
        yield fn(batch)


def get_max_command_line_bytes(os_name: str) -> int:
    """Remote flavor of os.sysconf("SC_ARG_MAX") - size(os.environb) - safety margin"""
    arg_max = MAX_CMDLINE_BYTES.get(os_name, 256 * 1024)
    environ_size = 4 * 1024  # typically is 1-4 KB
    safety_margin = (8 * 2 * 4 + 4) * 1024 if arg_max >= 1 * 1024 * 1024 else 8 * 1024
    max_bytes = max(4 * 1024, arg_max - environ_size - safety_margin)
    return max_bytes


# constants:
MAX_CMDLINE_BYTES: dict[str, int] = {
    "Linux": 2 * 1024 * 1024,
    "FreeBSD": 256 * 1024,
    "Darwin": 1 * 1024 * 1024,
    "Windows": 32 * 1024,
}
