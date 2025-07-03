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
    """Returns output datasets in the same order as the input datasets (not in random order) if ordered == True."""
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
