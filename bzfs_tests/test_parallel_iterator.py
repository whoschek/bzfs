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
"""Unit tests for the iterator driving parallel ``bzfs`` execution; Confirm tasks yield results and propagate failures
correctly."""

from __future__ import annotations
import time
import unittest
from typing import (
    Iterable,
)

from bzfs_main import bzfs
from bzfs_main.configuration import Remote
from bzfs_main.connection import (
    DEDICATED,
    SHARED,
    ConnectionPools,
)
from bzfs_main.parallel_batch_cmd import itr_ssh_cmd_parallel
from bzfs_tests.abstract_testcase import AbstractTestCase


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestParallelIterator,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


def dummy_fn_ordered(cmd: list[str], batch: list[str]) -> tuple[list[str], list[str]]:
    return cmd, batch


def dummy_fn_unordered(cmd: list[str], batch: list[str]) -> tuple[list[str], list[str]]:
    if cmd[0] == "zfslist1":
        time.sleep(0.2)
    elif cmd[0] == "zfslist2":
        time.sleep(0.1)
    return cmd, batch


def dummy_fn_raise(cmd: list[str], batch: list[str]) -> tuple[list[str], list[str]]:
    if cmd[0] == "fail":
        raise ValueError("Intentional failure")
    return cmd, batch


def dummy_fn_race(cmd: list[str], batch: list[str]) -> tuple[list[str], list[str]]:
    if cmd[0] == "zfslist1":
        time.sleep(0.3)
    elif cmd[0] == "zfslist2":
        time.sleep(0.2)
    elif cmd[0] == "zfslist3":
        time.sleep(0.1)
    return cmd, batch


class TestParallelIterator(AbstractTestCase):

    def setUp(self) -> None:
        args = self.argparser_parse_args(args=["src", "dst"])
        p = self.make_params(args=args)
        job = bzfs.Job()
        job.params = p
        p.src = Remote("src", args, p)
        job.params.connection_pools["src"] = ConnectionPools(
            p.src, {SHARED: p.src.max_concurrent_ssh_sessions_per_tcp_connection, DEDICATED: 1}
        )
        job.max_workers = {"src": 2}
        job.params.available_programs = {"src": {"os": "Linux"}, "local": {"os": "Linux"}}
        self.job = job
        self.r = p.src

        # Test data with max_batch_items=2
        self.cmd_args_list_2: list[tuple[list[str], Iterable[str]]] = [
            (["zfslist1"], ["d1", "d2", "d3", "d4"]),
            (["zfslist2"], ["d5", "d6", "d7", "d8"]),
        ]
        self.expected_ordered_2 = [
            (["zfslist1"], ["d1", "d2"]),
            (["zfslist1"], ["d3", "d4"]),
            (["zfslist2"], ["d5", "d6"]),
            (["zfslist2"], ["d7", "d8"]),
        ]

        # Test data with max_batch_items=3
        self.cmd_args_list_3: list[tuple[list[str], Iterable[str]]] = [
            (["zfslist1"], ["a1", "a2", "a3", "a4"]),
            (["zfslist2"], ["b1", "b2", "b3", "b4", "b5"]),
        ]
        self.expected_ordered_3 = [
            (["zfslist1"], ["a1", "a2", "a3"]),
            (["zfslist1"], ["a4"]),
            (["zfslist2"], ["b1", "b2", "b3"]),
            (["zfslist2"], ["b4", "b5"]),
        ]

    def test_ordered_with_max_batch_items_2(self) -> None:
        results = list(
            itr_ssh_cmd_parallel(self.job, self.r, self.cmd_args_list_2, dummy_fn_ordered, max_batch_items=2, ordered=True)
        )
        self.assertEqual(self.expected_ordered_2, results)

    def test_unordered_with_max_batch_items_2(self) -> None:
        results = list(
            itr_ssh_cmd_parallel(
                self.job, self.r, self.cmd_args_list_2, dummy_fn_unordered, max_batch_items=2, ordered=False
            )
        )
        self.assertEqual(sorted(self.expected_ordered_2), sorted(results))

    def test_ordered_with_max_batch_items_3(self) -> None:
        results = list(
            itr_ssh_cmd_parallel(self.job, self.r, self.cmd_args_list_3, dummy_fn_ordered, max_batch_items=3, ordered=True)
        )
        self.assertEqual(self.expected_ordered_3, results)

    def test_unordered_with_max_batch_items_3(self) -> None:
        results = list(
            itr_ssh_cmd_parallel(
                self.job, self.r, self.cmd_args_list_3, dummy_fn_unordered, max_batch_items=3, ordered=False
            )
        )
        self.assertEqual(sorted(self.expected_ordered_3), sorted(results))

    def test_exception_propagation_ordered(self) -> None:
        cmd_args_list: list[tuple[list[str], Iterable[str]]] = [(["ok"], ["a1", "a2"]), (["fail"], ["b1", "b2"])]
        gen = itr_ssh_cmd_parallel(self.job, self.r, cmd_args_list, dummy_fn_raise, max_batch_items=2, ordered=True)
        result = next(gen)
        self.assertEqual((["ok"], ["a1", "a2"]), result)
        with self.assertRaises(ValueError) as context:
            next(gen)
        self.assertEqual("Intentional failure", str(context.exception))

    def test_exception_propagation_unordered(self) -> None:
        cmd_args_list: list[tuple[list[str], Iterable[str]]] = [(["ok"], ["a1", "a2"]), (["fail"], ["b1", "b2"])]
        gen = itr_ssh_cmd_parallel(self.job, self.r, cmd_args_list, dummy_fn_raise, max_batch_items=2, ordered=False)
        caught_exception = False
        results = []
        try:
            for r in gen:
                results.append(r)
        except ValueError as e:
            caught_exception = True
            self.assertEqual("Intentional failure", str(e))
        self.assertTrue(caught_exception, "Expected exception was not raised in unordered mode..")

    def test_unordered_thread_scheduling(self) -> None:
        cmd_args_list: list[tuple[list[str], Iterable[str]]] = [
            (["zfslist1"], ["a1"]),
            (["zfslist2"], ["b1"]),
            (["zfslist3"], ["c1"]),
        ]
        expected_ordered = [
            (["zfslist1"], ["a1"]),
            (["zfslist2"], ["b1"]),
            (["zfslist3"], ["c1"]),
        ]
        unordered_results = list(
            itr_ssh_cmd_parallel(self.job, self.r, cmd_args_list, dummy_fn_race, max_batch_items=1, ordered=False)
        )
        self.assertEqual(sorted(expected_ordered), sorted(unordered_results))

    def test_empty_cmd_args_list_ordered(self) -> None:
        results = list(itr_ssh_cmd_parallel(self.job, self.r, [], dummy_fn_ordered, max_batch_items=2, ordered=True))
        self.assertEqual([], results)

    def test_empty_cmd_args_list_unordered(self) -> None:
        results = list(itr_ssh_cmd_parallel(self.job, self.r, [], dummy_fn_ordered, max_batch_items=2, ordered=False))
        self.assertEqual([], results)

    def test_cmd_with_empty_arguments_ordered(self) -> None:
        cmd_args_list: list[tuple[list[str], Iterable[str]]] = [(["zfslist1"], []), (["zfslist2"], ["d1", "d2"])]
        expected_ordered = [(["zfslist2"], ["d1", "d2"])]
        results = list(
            itr_ssh_cmd_parallel(self.job, self.r, cmd_args_list, dummy_fn_ordered, max_batch_items=2, ordered=True)
        )
        self.assertEqual(expected_ordered, results)

    def test_cmd_with_empty_arguments_unordered(self) -> None:
        cmd_args_list: list[tuple[list[str], Iterable[str]]] = [(["zfslist1"], []), (["zfslist2"], ["d1", "d2"])]
        expected_ordered = [(["zfslist2"], ["d1", "d2"])]
        results = list(
            itr_ssh_cmd_parallel(self.job, self.r, cmd_args_list, dummy_fn_ordered, max_batch_items=2, ordered=False)
        )
        self.assertEqual(expected_ordered, results)
