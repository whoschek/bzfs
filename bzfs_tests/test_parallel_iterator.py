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
"""Unit tests for parallel_iterator."""

from __future__ import (
    annotations,
)
import threading
import unittest
from collections.abc import (
    Iterator,
)
from concurrent.futures import (
    Future,
)

from bzfs_main.util.parallel_iterator import (
    parallel_iterator,
    parallel_iterator_results,
)


def suite() -> unittest.TestSuite:
    test_cases = [
        TestParallelIterator,
        TestParallelIteratorResultsTerminationEvent,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestParallelIterator(unittest.TestCase):
    """Covers edge cases for parallel_iterator_results()."""

    def test_zero_workers_empty_iterator_returns_empty(self) -> None:
        """When max_workers==0, function should accept an empty iterator and yield nothing."""
        self.assertEqual([], list(parallel_iterator_results(iter([]), max_workers=0, ordered=True)))

    def test_parallel_iterator_zero_workers_empty_builder(self) -> None:
        """parallel_iterator with max_workers==0 yields nothing when builder produces no tasks."""

        def builder(_executor: object) -> list[Iterator[Future[int]]]:
            return []  # no task iterators

        result = list(parallel_iterator(builder, max_workers=0, ordered=True))
        self.assertEqual([], result)


#############################################################################
def _done_future(val: int) -> Future[int]:
    f: Future[int] = Future()
    f.set_result(val)
    return f


class _NoConsumeIterator(Iterator[Future[int]]):
    """Iterator that must not be consumed; raises if next() is called."""

    def __init__(self) -> None:
        self.consumed = False

    def __iter__(self) -> _NoConsumeIterator:
        return self

    def __next__(self) -> Future[int]:
        self.consumed = True
        raise AssertionError("Iterator must not be consumed when termination_event is pre-set")


class TestParallelIteratorResultsTerminationEvent(unittest.TestCase):
    """Unit tests for termination_event behavior in parallel_iterator_results().

    Covers all branches where termination_event impacts control flow:
    - Early return when termination_event is pre-set (before any materialization).
    - Early stop inside the ordered flow while iterating.
    - Early stop inside the unordered flow while iterating.
    """

    def test_pre_set_event_stops_before_materialization(self) -> None:
        """If termination_event is set prior to iteration, nothing is yielded and the iterator is not touched."""
        term = threading.Event()
        term.set()
        itr = _NoConsumeIterator()

        results = list(parallel_iterator_results(iterator=itr, max_workers=2, ordered=True, is_terminated=term.is_set))

        self.assertEqual([], results)
        self.assertFalse(itr.consumed)

    def test_ordered_midstream_event_stops_early(self) -> None:
        """When termination_event is set between yields in ordered mode, subsequent results are not produced."""
        term = threading.Event()

        # Pre-completed futures ensure deterministic, non-blocking behavior.
        vals = [1, 2, 3, 4]

        def gen() -> Iterator[Future[int]]:
            for v in vals:
                yield _done_future(v)

        iterator = gen()
        it = parallel_iterator_results(iterator=iterator, max_workers=2, ordered=True, is_terminated=term.is_set)

        first = next(it)
        term.set()  # trigger early termination before consuming the remainder
        rest = list(it)

        self.assertEqual(1, first)
        self.assertEqual([], rest)

    def test_unordered_midstream_event_stops_early(self) -> None:
        """When termination_event is set between yields in unordered mode, the inner loop returns before more yields."""
        term = threading.Event()
        vals = [10, 20, 30, 40]

        def gen() -> Iterator[Future[int]]:
            for v in vals:
                yield _done_future(v)

        iterator = gen()
        it = parallel_iterator_results(iterator=iterator, max_workers=3, ordered=False, is_terminated=term.is_set)

        first = next(it)  # some completed value (unordered)
        term.set()  # trigger early termination
        rest = list(it)

        # Do not assert exact value of `first` due to unordered semantics; only count.
        self.assertIn(first, set(vals))
        self.assertEqual([], rest)
