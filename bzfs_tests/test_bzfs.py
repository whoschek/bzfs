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
"""Unit tests for the ``bzfs`` CLI."""

from __future__ import (
    annotations,
)
import contextlib
import logging
import re
import subprocess
import time
import unittest
from collections.abc import (
    Iterator,
)
from datetime import (
    datetime,
    timezone,
)
from itertools import (
    combinations,
)
from typing import (
    TYPE_CHECKING,
    Callable,
    cast,
)
from unittest.mock import (
    MagicMock,
    patch,
)

import bzfs_main.detect
import bzfs_main.utils
from bzfs_main import (
    bzfs,
)
from bzfs_main.argparse_actions import (
    SnapshotFilter,
)
from bzfs_main.configuration import (
    LogParams,
    Remote,
    SnapshotLabel,
)
from bzfs_main.connection import (
    ConnectionPools,
)
from bzfs_main.detect import (
    ZFS_VERSION_IS_AT_LEAST_2_2_0,
    RemoteConfCacheItem,
    detect_available_programs,
)
from bzfs_main.filter import (
    SNAPSHOT_REGEX_FILTER_NAME,
)
from bzfs_main.progress_reporter import (
    ProgressReporter,
)
from bzfs_main.replication import (
    _add_recv_property_options,
    _is_zfs_dataset_busy,
    _pv_cmd,
    replicate_dataset,
)
from bzfs_main.retry import (
    Retry,
)
from bzfs_main.snapshot_cache import (
    SnapshotCache,
)
from bzfs_main.utils import (
    DIE_STATUS,
    is_descendant,
)
from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)
from bzfs_tests.tools import (
    stop_on_failure_subtest,
    suppress_output,
)

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from bzfs_main.bzfs import (
        Job,
    )


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestHelperFunctions,
        TestAdditionalHelpers,
        TestParseDatasetLocator,
        TestArgumentParser,
        TestAddRecvPropertyOptions,
        TestPreservePropertiesValidation,
        TestJobMethods,
        TestDeleteDstDatasetsTask,
        TestDeleteEmptyDstDatasetsTask,
        TestHandleMinMaxSnapshots,
        TestFindDatasetsToSnapshot,
        TestTerminationEventBehavior,
        TestPerJobTermination,
        TestPythonVersionCheck,
        TestPerformance,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestHelperFunctions(AbstractTestCase):

    def test_validate_port(self) -> None:
        bzfs.validate_port(47, "msg")
        bzfs.validate_port("47", "msg")
        bzfs.validate_port(0, "msg")
        bzfs.validate_port("", "msg")
        with self.assertRaises(SystemExit):
            bzfs.validate_port("xxx47", "msg")

    def test_run_main_with_unexpected_exception(self) -> None:
        args = self.argparser_parse_args(args=["src", "dst"])
        job = bzfs.Job()
        job.params = self.make_params(args=args)

        with patch("time.monotonic_ns", side_effect=ValueError("my value error")):
            with self.assertRaises(SystemExit), suppress_output():
                job.run_main(args)

    def test_recv_option_property_names(self) -> None:
        def names(lst: list[str]) -> set[str]:
            return bzfs.Job().recv_option_property_names(lst)

        self.assertSetEqual(set(), names([]))
        self.assertSetEqual(set(), names(["name1=value1"]))
        self.assertSetEqual(set(), names(["name1"]))
        self.assertSetEqual({"name1"}, names(["-o", "name1=value1"]))
        self.assertSetEqual({"name1"}, names(["-x", "name1"]))
        self.assertSetEqual({"name1"}, names(["-o", "name1=value1", "-o", "name1=value2", "-x", "name1"]))
        self.assertSetEqual({"name1", "name2"}, names(["-o", "name1=value1", "-o", "name2=value2"]))
        self.assertSetEqual({"name1", "name2"}, names(["-o", "name1=value1", "-x", "name2"]))
        self.assertSetEqual({"name1", "name2"}, names(["-v", "-o", "name1=value1", "-o", "name2=value2"]))
        self.assertSetEqual({"name1", "name2"}, names(["-v", "-o", "name1=value1", "-o", "name2=value2", "-F"]))
        self.assertSetEqual({"name1", "name2"}, names(["-v", "-o", "name1=value1", "-n", "-o", "name2=value2", "-F"]))
        with self.assertRaises(SystemExit):
            names(["-o", "name1"])
        with self.assertRaises(SystemExit):
            names(["-o", "=value1"])
        with self.assertRaises(SystemExit):
            names(["-o", ""])
        with self.assertRaises(SystemExit):
            names(["-x", "=value1"])
        with self.assertRaises(SystemExit):
            names(["-x", ""])
        with self.assertRaises(SystemExit):
            names(["-o"])
        with self.assertRaises(SystemExit):
            names(["-o", "name1=value1", "-o"])
        with self.assertRaises(SystemExit):
            names(["-o", "name1=value1", "-x"])
        with self.assertRaises(SystemExit):
            names(["-o", "-o", "name1=value1"])
        with self.assertRaises(SystemExit):
            names(["-x", "-x", "name1=value1"])
        with self.assertRaises(SystemExit):
            names([" -o ", " -o ", "name1=value1"])

    def test_is_zfs_dataset_busy_match(self) -> None:
        def is_busy(proc: str, dataset: str, busy_if_send: bool = True) -> bool:
            return _is_zfs_dataset_busy([proc], dataset, busy_if_send=busy_if_send)

        ds = "tank/foo/bar"
        self.assertTrue(is_busy("zfs receive " + ds, ds))
        self.assertTrue(is_busy("zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("sudo zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("/bin/sudo zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("/usr/bin/sudo zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("sudo /sbin/zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("sudo /usr/sbin/zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("/bin/sudo /sbin/zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("doas zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("doas zfs receive -u " + ds, ds, busy_if_send=False))
        self.assertTrue(is_busy("sudo zfs receive -u -o foo:bar=/baz " + ds, ds))
        self.assertFalse(is_busy("sudo zfs xxxxxx -u " + ds, ds))
        self.assertFalse(is_busy("sudo zfs xxxxxx -u " + ds, ds, busy_if_send=False))
        self.assertFalse(is_busy("xsudo zfs receive -u " + ds, ds))
        self.assertFalse(is_busy("sudo zfs receive -u", ds))
        self.assertFalse(is_busy("sudo receive -u " + ds, ds))
        self.assertFalse(is_busy("zfs send " + ds + "@snap", ds, busy_if_send=False))
        self.assertTrue(is_busy("zfs send " + ds + "@snap", ds))

    def test_pv_cmd(self) -> None:
        args = self.argparser_parse_args(args=["src", "dst"])
        job = bzfs.Job()
        job.params = self.make_params(args=args, log_params=LogParams(args))
        job.params.available_programs = {"src": {"pv": "pv"}}
        self.assertNotEqual("cat", _pv_cmd(job, "src", 1024 * 1024, "foo"))

    @staticmethod
    def root_datasets_if_recursive_zfs_snapshot_is_possible_slow_but_correct(  # compare faster algos to this baseline impl
        src_datasets: list[str], basis_src_datasets: list[str]
    ) -> list[str] | None:
        # Assumes that src_datasets and basis_src_datasets are both sorted (and thus root_datasets is sorted too)
        src_datasets_set = set(src_datasets)
        root_datasets = bzfs.Job().find_root_datasets(src_datasets)
        for basis_dataset in basis_src_datasets:
            for root_dataset in root_datasets:
                if bzfs_main.utils.is_descendant(basis_dataset, of_root_dataset=root_dataset):
                    if basis_dataset not in src_datasets_set:
                        return None
        return root_datasets

    @staticmethod
    def root_datasets_if_recursive_zfs_snapshot_is_possible_faster_but_not_ideal(  # compare faster algo to this ok impl
        datasets: list[str], basis_datasets: list[str]
    ) -> list[str] | None:
        # Assumes that src_datasets and basis_src_datasets are both sorted (and thus root_datasets is sorted too)
        root_datasets = bzfs.Job().find_root_datasets(datasets)
        datasets_set: set[str] = set(datasets)
        i = 0
        j = 0
        len_root_datasets = len(root_datasets)
        len_basis_datasets = len(basis_datasets)
        while i < len_root_datasets and j < len_basis_datasets:  # walk and "merge" both sorted lists, in sync
            if basis_datasets[j] < root_datasets[i]:  # irrelevant subtree?
                j += 1  # move to the next basis_src_dataset
            elif is_descendant(basis_datasets[j], of_root_dataset=root_datasets[i]):  # relevant subtree?
                if basis_datasets[j] not in datasets_set:  # was dataset chopped off by schedule or --incl/exclude-dataset*?
                    return None  # detected filter pruning that is incompatible with 'zfs snapshot -r'
                j += 1  # move to the next basis_src_dataset
            else:
                i += 1  # move to next root dataset; no need to check root_datasets that are nomore (or not yet) reachable
        return root_datasets

    def test_root_datasets_if_recursive_zfs_snapshot_is_possible(self) -> None:
        """Also exhaustively compares optimized root dataset detection against a baseline impl that is slow but correct."""

        def run_filter(src_datasets: list[str], basis_src_datasets: list[str]) -> list[str]:
            assert set(src_datasets).issubset(set(basis_src_datasets))
            src_datasets = sorted(src_datasets)
            basis_src_datasets = sorted(basis_src_datasets)
            expected = self.root_datasets_if_recursive_zfs_snapshot_is_possible_slow_but_correct(
                src_datasets, basis_src_datasets
            )
            job = bzfs.Job()
            job.is_test_mode = True
            actual = job.root_datasets_if_recursive_zfs_snapshot_is_possible(src_datasets, basis_src_datasets)
            if expected is not None:
                self.assertListEqual(expected, cast(list[str], actual))
            self.assertEqual(expected, actual)
            expected = self.root_datasets_if_recursive_zfs_snapshot_is_possible_faster_but_not_ideal(
                src_datasets, basis_src_datasets
            )
            if expected is not None:
                self.assertListEqual(expected, cast(list[str], actual))
            self.assertEqual(expected, actual)
            return cast(list[str], actual)

        basis_src_datasets = ["a", "a/b", "a/b/c", "a/d"]
        self.assertListEqual([], run_filter([], basis_src_datasets))
        self.assertListEqual(["a"], run_filter(basis_src_datasets, basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["a", "a/b", "a/b/c"], basis_src_datasets)))
        self.assertIsNone(cast(None, run_filter(["a/b", "a/d"], basis_src_datasets)))
        self.assertListEqual(["a/b"], run_filter(["a/b", "a/b/c"], basis_src_datasets))
        self.assertListEqual(["a/b", "a/d"], run_filter(["a/b", "a/b/c", "a/d"], basis_src_datasets))
        self.assertListEqual(["a/b", "a/d"], run_filter(["a/b", "a/b/c", "a/d"], basis_src_datasets))
        self.assertListEqual(["a/d"], run_filter(["a/d"], basis_src_datasets))

        basis_src_datasets = ["a", "a/b", "a/b/c", "a/d", "e", "e/f"]
        self.assertListEqual(["a", "e"], run_filter(basis_src_datasets, basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["e"], basis_src_datasets)))
        self.assertListEqual(["e/f"], run_filter(["e/f"], basis_src_datasets))
        self.assertListEqual(["a", "e/f"], run_filter(["a", "a/b", "a/b/c", "a/d", "e/f"], basis_src_datasets))
        self.assertListEqual(["a/b"], run_filter(["a/b", "a/b/c"], basis_src_datasets))
        self.assertListEqual(["a/b", "a/d"], run_filter(["a/b", "a/b/c", "a/d"], basis_src_datasets))
        self.assertListEqual(["a/d"], run_filter(["a/d"], basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["a/b", "a/d"], basis_src_datasets)))
        self.assertIsNone(cast(None, run_filter(["a", "a/b", "a/d"], basis_src_datasets)))

        basis_src_datasets = ["a", "e", "h"]
        self.assertListEqual([], run_filter([], basis_src_datasets))
        self.assertListEqual(["a", "e", "h"], run_filter(basis_src_datasets, basis_src_datasets))
        self.assertListEqual(["a", "e"], run_filter(["a", "e"], basis_src_datasets))
        self.assertListEqual(["e", "h"], run_filter(["e", "h"], basis_src_datasets))
        self.assertListEqual(["a", "h"], run_filter(["a", "h"], basis_src_datasets))
        self.assertListEqual(["a"], run_filter(["a"], basis_src_datasets))
        self.assertListEqual(["e"], run_filter(["e"], basis_src_datasets))
        self.assertListEqual(["h"], run_filter(["h"], basis_src_datasets))

        basis_src_datasets = ["a", "e", "e/f", "h", "h/g"]
        self.assertListEqual([], run_filter([], basis_src_datasets))
        self.assertListEqual(["a", "e", "h"], run_filter(basis_src_datasets, basis_src_datasets))

        self.assertIsNone(cast(None, run_filter(["a", "h"], basis_src_datasets)))
        self.assertListEqual(["a", "h/g"], run_filter(["a", "h/g"], basis_src_datasets))

        basis_src_datasets = ["a", "e", "e/f", "h", "h/g", "k", "k/l"]
        self.assertListEqual([], run_filter([], basis_src_datasets))
        self.assertListEqual(["a", "e", "h", "k"], run_filter(basis_src_datasets, basis_src_datasets))

        self.assertIsNone(cast(None, run_filter(["a", "h"], basis_src_datasets)))
        self.assertListEqual(["a", "h/g"], run_filter(["a", "h/g"], basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["a", "k"], basis_src_datasets)))
        self.assertListEqual(["a", "k/l"], run_filter(["a", "k/l"], basis_src_datasets))

        basis_src_datasets = ["a", "e", "e/f", "h", "h/g", "hh", "hh/g", "k", "kk", "k/l", "kk/l"]
        self.assertIsNone(cast(None, run_filter(["a", "hh"], basis_src_datasets)))
        self.assertListEqual(["a", "hh/g"], run_filter(["a", "hh/g"], basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["kk"], basis_src_datasets)))
        self.assertListEqual(["kk/l"], run_filter(["kk/l"], basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["k"], basis_src_datasets)))
        self.assertListEqual(["k/l"], run_filter(["k/l"], basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["h"], basis_src_datasets)))
        self.assertListEqual(["h/g"], run_filter(["h/g"], basis_src_datasets))

        basis_src_datasets = ["a", "e", "e/f", "h", "h/g", "hh", "hh/g", "kk", "kk/l"]
        self.assertIsNone(cast(None, run_filter(["kk"], basis_src_datasets)))
        self.assertListEqual(["kk/l"], run_filter(["kk/l"], basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["hh"], basis_src_datasets)))

        # sorted()
        basis_src_datasets = ["a", "a/b", "a/b/c", "a/B", "a/B/c", "a/D", "a/X", "a/X/c"]
        self.assertListEqual(["a/D", "a/b"], run_filter(["a/b", "a/b/c", "a/D"], basis_src_datasets))
        self.assertListEqual(["a/B", "a/D", "a/b"], run_filter(["a/B", "a/B/c", "a/b", "a/b/c", "a/D"], basis_src_datasets))
        self.assertListEqual(["a/B", "a/D", "a/X"], run_filter(["a/B", "a/B/c", "a/X", "a/X/c", "a/D"], basis_src_datasets))

        exhaustive_basis_sets: list[list[str]] = [
            ["a", "a/b", "a/b/c", "a/d"],
            ["a", "a/b", "a/b/c", "a/d", "e", "e/f"],
            ["a", "e", "h"],
            ["a", "e", "e/f", "h", "h/g"],
            ["a", "e", "e/f", "h", "h/g", "k", "k/l"],
            ["a", "e", "e/f", "h", "h/g", "hh", "hh/g", "k", "kk", "k/l", "kk/l"],
            ["a", "e", "e/f", "h", "h/g", "hh", "hh/g", "kk", "kk/l"],
            ["a", "a/b", "a/b/c", "a/B", "a/B/c", "a/D", "a/X", "a/X/c"],
            [],
            ["a"],
            ["a", "b"],
            ["a", "b", "c"],
            ["a", "a/b", "c"],
            ["a", "a/b", "a/b/c", "a/d"],  # branching tree
            ["a", "a/b", "a/b/c"],  # simple chain
            ["a", "b"],  # multiple roots
            ["a", "a/b", "d", "d/e"],
            ["a", "a/b", "a/c", "d", "d/e"],
            ["x", "x/y", "x/y/z", "x/q", "r", "r/s"],
            ["a", "ab", "ab/c"],
            ["a", "a/b1", "a/b1/c1", "a/b2", "a/b2/c2", "a/b2/c2/d2"],
            [],
            ["a1", "a1/b", "a2", "a2/b"],
            ["a-b", "a-b/c-d", "x_y", "x_y/z"],
            ["foo", "foo/bar", "foo/bar/baz", "foo2", "foo2/bar"],
            ["a", "a/b", "b", "b/c", "c", "c/d"],
            ["alpha", "alpha/beta", "alpha/beta/gamma", "alpha2", "alpha2/beta2", "alpha2/beta2/gamma2", "delta"],
            ["p", "p/q", "pq", "pq/r", "pqr", "pqr/s"],
            ["root", "root/a", "root/a/b", "root/a/b/c", "other", "other/a", "other/a/b"],
            ["a", "a/b", "a/bc", "a/bc/d", "ab", "ab/c"],
            [
                "deep",
                "deep/a",
                "deep/a/b",
                "deep/a/b/c",
                "deep/a/b/c/d",
                "deep/a/b/c/d/e",
                "deep/a/b/c/d/e/f",
                "deep/a/b/c/d/e/f/g",
                "deep/a/b/c/d/e/f/g/h",
                "deep/a/b/c/d/e/f/g/h/i",
            ],  # deep chain of 10
            [
                "main",
                "main/branch1",
                "main/branch1/leaf",
                "main/branch2",
                "main/branch2/leaf",
                "main/branch2/leaf2",
                "aux",
                "aux/branch",
                "aux/branch/leaf",
            ],  # root with multiple deep branches
            [
                "r1",
                "r1/a",
                "r1/a/b",
                "r2",
                "r2/a",
                "r2/a/b",
                "r3",
                "r3/a",
                "r3/a/b",
                "r4",
            ],  # four roots varying depths
            [
                "a",
                "ab",
                "abc",
                "abc/d",
                "ab/c",
                "abcd",
                "abcd/e",
                "b",
                "bc",
                "bc/d",
            ],  # prefix but not descendant
            [
                "1",
                "1/2",
                "1/2/3",
                "1/2/3/4",
                "2",
                "2/3",
                "10",
                "10/1",
                "20",
                "20/0",
            ],  # numeric names
            [
                "a-b",
                "a-b/c-d",
                "a-b/c-d/e-f",
                "a_b",
                "a_b/c_d",
                "a_b/c_d/e_f",
                "x-y",
                "x-y/z-w",
                "x_y",
                "x_y/z_w",
            ],  # hyphen and underscore combos
            [
                "A",
                "A/B",
                "A/B/c",
                "A/D",
                "A/D/e",
                "F",
                "F/g",
                "F/g/H",
                "i",
                "i/j",
            ],  # mixed case names
            [
                "a.b",
                "a.b/c.d",
                "a.b/c.d/e.f",
                "g.h",
                "g.h/i.j",
                "g.h/i.j/k.l",
                "m.n",
                "m.n/o.p",
                "q.r",
                "q.r/s.t",
            ],  # dotted names
            [
                "a1",
                "a1/b2",
                "a1/b2/c3",
                "a2",
                "a2/b3",
                "a2/b3/c4",
                "b1",
                "b1/c2",
                "b1/c2/d3",
                "b2",
            ],  # alphanumeric segments
            [
                "x",
                "x/y",
                "x/y/z1",
                "x/y/z1/w",
                "x/y/z2",
                "x/y/z2/w",
                "y",
                "y/x",
                "y/x/z",
                "y/x/z/w",
            ],  # cross-linked roots
            ["a", "a/b", "a/b/c", "a/b/c/d", "a/b/c/d/e", "a/b/c/d/e/f", "a/b/c/d/e/f/g"],
            ["a", "a/b", "a/b/c", "a/b/c/d", "a/b/e", "a/b/e/f", "a/g", "a/g/h", "a/g/h/i"],
            ["r1", "r1/a", "r1/a/b", "r2", "r2/a", "r2/a/b", "r3", "r3/a", "r3/a/b", "r4"],
            ["ab", "ab/c", "a/b", "a/b/c", "abc", "abc/d", "a/bcd", "a/bcd/e"],
            ["foo", "foo/bar", "foo/bar-baz", "foo/bar-baz/qux", "foo_bar", "foo_bar/baz", "foo_bar/baz/qux"],
            ["A", "A/B", "A/B/C", "a", "a/b", "a/B", "a/B/c", "a/b/c"],
            ["d1", "d1/e1", "d1/e1/f1", "d2", "d2/e2", "d2/e2/f2", "d3", "d3/e3"],
            ["x", "x/y", "x/y/z", "x/y/z/a", "x/y/z/a/b", "u", "u/v", "u/v/w", "u/v/w/x"],
            [
                "root1",
                "root1/child1",
                "root1/child1/grand",
                "root2",
                "root2/child2",
                "root2/child2/grand2",
                "root3",
                "root3/child3",
                "root3/child3/grand3",
                "root4",
            ],
            ["a1", "a1/b-1", "a1/b-1/c_1", "a2", "a2/b-2", "a2/b-2/c_2", "a2/b-2/c_2/d", "a3", "a3/b_3"],
            [
                "a",
                "a/b",
                "a/b/c",
                "a/b/c/d",
                "a/b/c/d/e",
                "a/b/c/d/e/f",
                "a/b/c/d/e/f/g",
                "a/b/c/d/e/f/g/h",
                "a/b/c/d/e/f/g/h/i",
                "a/b/c/d/e/f/g/h/i/j",
            ],
            ["root", "root/a", "root/b", "root/c", "root/d", "root/e", "root/f", "root/g", "root/h", "root/i"],
            ["r1", "r1/a", "r1/a/b", "r1/a/b/c", "r1/a/b/c/d", "r2", "r2/a", "r2/a/b", "r2/a/b/c", "r2/a/b/c/d"],
            ["a", "ab", "ab/c", "abc", "abc/d", "abcd", "abcd/e", "abcde", "abcde/f", "a/b"],
        ]
        if not self.is_unit_test:  # slow
            exhaustive_basis_sets += [
                [
                    "pkg-1",
                    "pkg-1/mod_1",
                    "pkg-1/mod_1/sub-1",
                    "pkg_2",
                    "pkg_2/mod-2",
                    "pkg_2/mod-2/sub_2",
                    "pkg3",
                    "pkg3/mod3",
                    "pkg3/mod3/sub-3",
                    "pkg3/mod3/sub-3/leaf_3",
                ],
                ["A", "A/B", "A/B/C", "a", "a/b", "a/b/c", "Alpha", "Alpha/Beta", "alpha", "alpha/beta"],
                [
                    "top",
                    "top/mid1",
                    "top/mid1/leaf1",
                    "top/mid2",
                    "top/mid2/leaf2",
                    "top/mid2/leaf2/subleaf",
                    "top2",
                    "top2/mid",
                    "top2/mid/leaf",
                    "top2/mid/leaf/sub",
                ],
                ["d0", "d0/e1", "d0/e1/f2", "d0/e1/f2/g3", "d1", "d1/e2", "d2", "d2/e3", "d2/e3/f4", "d2/e3/f4/g5"],
                ["x", "x/y", "x/y/z", "x/y1", "x/y1/z1", "x2", "x2/y", "x2/y/z", "x2/y/z/a", "x2/y1"],
                ["m_n", "m_n/o", "m_n/o/p", "m-n", "m-n/o", "m-n/o/p", "mn", "mn/o", "mn/o/p", "mn/op"],
                [
                    "a",
                    "a/b",
                    "a/b/c",
                    "a/b/c/d",
                    "a/b/c/d/e",
                    "a/b/c/d/e/f",
                    "a/b/c/d/e/f/g",
                    "a/b/c/d/e/f/g/h",
                    "a/b/c/d/e/f/g/h/i",
                    "a/b/c/d/e/f/g/h/i/j",
                ],
                [
                    "root",
                    "root/a",
                    "root/a/b",
                    "root/a/c",
                    "root/d",
                    "root/d/e",
                    "root/d/f",
                    "other",
                    "other/x",
                    "other/x/y",
                ],
                ["a", "A", "a/b", "A/B", "a/b/c", "A/B/C", "d", "D", "d/e", "D/E"],
                ["a1", "a1/b1", "a1/b1/c1", "a1/b2", "a1/b2/c2", "a2", "a2/b1", "a2/b1/c1", "a2/b2", "a2/b2/c2"],
                [
                    "ab-c",
                    "ab-c/d",
                    "ab-c/d/e",
                    "x_y",
                    "x_y/z",
                    "x_y/z/a",
                    "foo-bar",
                    "foo-bar/baz",
                    "foo_bar",
                    "foo_bar/qux",
                ],
                ["a", "ab", "ab/c", "abc", "abc/d", "b", "b/c", "bc", "bc/d", "c"],
                ["m/n/p/q", "m", "m/n", "m/n/p", "m/x", "m/x/y", "n", "n/o", "n/o/p", "n/o/p/q"],
                [
                    "tank",
                    "tank/a",
                    "tank/a/b",
                    "tank/aa",
                    "tank/aa/b",
                    "tank/aa/b/c",
                    "tanka",
                    "tanka/b",
                    "tanka/b/c",
                    "tankb",
                ],
                ["x", "x/a", "x/a/b", "x/a/b/c", "y", "y/a", "y/a/b", "z", "z/a", "z/a/b"],
                [
                    "data",
                    "data/child",
                    "data/child/grand",
                    "data0",
                    "data0/child",
                    "data1",
                    "data1/child",
                    "data2",
                    "data2/child",
                    "data2/child/grand",
                ],
            ]
        for ex_basis_set in exhaustive_basis_sets:
            if False:  # can take minutes; toggle this when you modify root_datasets_if_recursive_zfs_snapshot_is_possible()
                basis_sets = [list(subset) for r in range(len(ex_basis_set) + 1) for subset in combinations(ex_basis_set, r)]
            else:
                basis_sets = [ex_basis_set]
            for basis in basis_sets:
                for subset in [list(subset2) for s in range(len(basis) + 1) for subset2 in combinations(basis, s)]:
                    with stop_on_failure_subtest(basis=basis, subset=subset):
                        run_filter(subset, basis)


#############################################################################
class TestAdditionalHelpers(AbstractTestCase):

    def test_job_shutdown_calls_pool_shutdown(self) -> None:
        job = bzfs.Job()
        mock_pool = MagicMock()
        job.remote_conf_cache = {("k",): RemoteConfCacheItem(mock_pool, {}, {})}
        job.shutdown()
        mock_pool.shutdown.assert_called_once()

    @patch("bzfs_main.bzfs.argument_parser")
    @patch("bzfs_main.bzfs.run_main")
    def test_main_handles_calledprocesserror(self, mock_run_main: MagicMock, mock_arg_parser: MagicMock) -> None:
        mock_arg_parser.return_value.parse_args.return_value = self.argparser_parse_args(["src", "dst"])
        mock_run_main.side_effect = subprocess.CalledProcessError(returncode=5, cmd=["cmd"])
        with self.assertRaises(SystemExit) as ctx:
            bzfs.main()
        self.assertEqual(5, ctx.exception.code)

    @patch("bzfs_main.bzfs.argument_parser")
    @patch("bzfs_main.bzfs.run_main")
    def test_main_handles_calledprocesserror2(self, mock_run_main: MagicMock, mock_arg_parser: MagicMock) -> None:
        mock_arg_parser.return_value.parse_args.return_value = self.argparser_parse_args(["src", "dst"])
        mock_run_main.side_effect = subprocess.CalledProcessError(returncode=bzfs.WARNING_STATUS, cmd=["cmd"])
        with self.assertRaises(SystemExit) as ctx:
            bzfs.main()
        self.assertEqual(DIE_STATUS, ctx.exception.code)

    @patch("bzfs_main.bzfs.Job.run_main")
    def test_run_main_delegates_to_job(self, mock_run_main: MagicMock) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        bzfs.run_main(args=args, sys_argv=["p"])
        mock_run_main.assert_called_once_with(args, ["p"], None)


#############################################################################
class TestParseDatasetLocator(AbstractTestCase):

    def run_test(
        self,
        input_value: str,
        expected_user: str,
        expected_host: str,
        expected_dataset: str,
        expected_user_host: str,
        expected_error: bool,
    ) -> None:
        expected_status = 0 if not expected_error else 3
        passed = False

        # Run without validation
        user, host, user_host, _pool, dataset = bzfs.parse_dataset_locator(input_value, validate=False)

        if (
            user == expected_user
            and host == expected_host
            and dataset == expected_dataset
            and user_host == expected_user_host
        ):
            passed = True

        # Rerun with validation
        status = 0
        try:
            bzfs.parse_dataset_locator(input_value, validate=True)
        except (ValueError, SystemExit):
            status = 3

        if status != expected_status or (not passed):
            if status != expected_status:
                logging.getLogger(__name__).warning("Validation Test failed:")
            else:
                logging.getLogger(__name__).warning("Test failed:")
            logging.getLogger(__name__).warning(
                f"input: {input_value}\nuser exp: '{expected_user}' vs '{user}'\nhost exp: '{expected_host}' vs "
                f"'{host}'\nuserhost exp: '{expected_user_host}' vs '{user_host}'\ndataset exp: '{expected_dataset}' vs "
                f"'{dataset}'"
            )
            self.fail()

    def test_basic(self) -> None:
        # Input format is [[user@]host:]dataset
        # test columns indicate values for: input | user | host | dataset | userhost | validationError
        self.run_test(
            "user@host.example.com:tank1/foo/bar",
            "user",
            "host.example.com",
            "tank1/foo/bar",
            "user@host.example.com",
            False,
        )
        self.run_test(
            "joe@192.168.1.1:tank1/foo/bar:baz:boo",
            "joe",
            "192.168.1.1",
            "tank1/foo/bar:baz:boo",
            "joe@192.168.1.1",
            False,
        )
        self.run_test("tank1/foo/bar", "", "", "tank1/foo/bar", "", False)
        self.run_test("-:tank1/foo/bar:baz:boo", "", "", "tank1/foo/bar:baz:boo", "", False)
        self.run_test("host.example.com:tank1/foo/bar", "", "host.example.com", "tank1/foo/bar", "host.example.com", False)
        self.run_test("root@host.example.com:tank1", "root", "host.example.com", "tank1", "root@host.example.com", False)
        self.run_test("192.168.1.1:tank1/foo/bar", "", "192.168.1.1", "tank1/foo/bar", "192.168.1.1", False)
        self.run_test("user@192.168.1.1:tank1/foo/bar", "user", "192.168.1.1", "tank1/foo/bar", "user@192.168.1.1", False)
        self.run_test(
            "user@host_2024-01-02:a3:04:56:tank1/foo/bar",
            "user",
            "host_2024-01-02",
            "a3:04:56:tank1/foo/bar",
            "user@host_2024-01-02",
            False,
        )
        self.run_test(
            "user@host_2024-01-02:a3:04:56:tank1:/foo:/:bar",
            "user",
            "host_2024-01-02",
            "a3:04:56:tank1:/foo:/:bar",
            "user@host_2024-01-02",
            False,
        )
        self.run_test(
            "user@host_2024-01-02:03:04:56:tank1/foo/bar",
            "user",
            "host_2024-01-02",
            "03:04:56:tank1/foo/bar",
            "user@host_2024-01-02",
            True,
        )
        self.run_test("user@localhost:tank1/foo/bar", "user", "localhost", "tank1/foo/bar", "user@localhost", False)
        self.run_test("host.local:tank1/foo/bar", "", "host.local", "tank1/foo/bar", "host.local", False)
        self.run_test("host.local:tank1/foo/bar", "", "host.local", "tank1/foo/bar", "host.local", False)
        self.run_test("-host.local:tank1/foo/bar", "", "-host.local", "tank1/foo/bar", "-host.local", True)
        self.run_test("user@host:", "user", "host", "", "user@host", True)
        self.run_test("@host:tank1/foo/bar", "", "host", "tank1/foo/bar", "host", False)
        self.run_test("@host:tank1/foo/bar", "", "host", "tank1/foo/bar", "host", False)
        self.run_test("@host:", "", "host", "", "host", True)
        self.run_test("user@:tank1/foo/bar", "", "user@", "tank1/foo/bar", "user@", True)
        self.run_test("user@:", "", "user@", "", "user@", True)
        self.run_test("@", "", "", "@", "", True)
        self.run_test("@foo", "", "", "@foo", "", True)
        self.run_test("@@", "", "", "@@", "", True)
        self.run_test(":::tank1:foo:bar:", "", "", ":::tank1:foo:bar:", "", True)
        self.run_test(":::tank1/bar", "", "", ":::tank1/bar", "", True)
        self.run_test(":::", "", "", ":::", "", True)
        self.run_test("::tank1/bar", "", "", "::tank1/bar", "", True)
        self.run_test("::", "", "", "::", "", True)
        self.run_test(":tank1/bar", "", "", ":tank1/bar", "", True)
        self.run_test(":", "", "", ":", "", True)
        self.run_test("", "", "", "", "", True)
        self.run_test("/", "", "", "/", "", True)
        self.run_test("tank//foo", "", "", "tank//foo", "", True)
        self.run_test("/tank1", "", "", "/tank1", "", True)
        self.run_test("tank1/", "", "", "tank1/", "", True)
        self.run_test(".", "", "", ".", "", True)
        self.run_test("..", "", "", "..", "", True)
        self.run_test("./tank", "", "", "./tank", "", True)
        self.run_test("../tank", "", "", "../tank", "", True)
        self.run_test("tank/..", "", "", "tank/..", "", True)
        self.run_test("tank/.", "", "", "tank/.", "", True)
        self.run_test("tank/fo`o", "", "", "tank/fo`o", "", True)
        self.run_test("tank/fo$o", "", "", "tank/fo$o", "", True)
        self.run_test("tank/fo\\o", "", "", "tank/fo\\o", "", True)
        self.run_test("u`set@localhost:tank1/foo/bar", "u`set", "localhost", "tank1/foo/bar", "u`set@localhost", True)
        self.run_test("u'ser@localhost:tank1/foo/bar", "u'ser", "localhost", "tank1/foo/bar", "u'ser@localhost", True)
        self.run_test('u"set@localhost:tank1/foo/bar', 'u"set', "localhost", "tank1/foo/bar", 'u"set@localhost', True)
        self.run_test("user@l`ocalhost:tank1/foo/bar", "user", "l`ocalhost", "tank1/foo/bar", "user@l`ocalhost", True)
        self.run_test("user@l'ocalhost:tank1/foo/bar", "user", "l'ocalhost", "tank1/foo/bar", "user@l'ocalhost", True)
        self.run_test('user@l"ocalhost:tank1/foo/bar', "user", 'l"ocalhost', "tank1/foo/bar", 'user@l"ocalhost', True)
        self.run_test("user@host.ex.com:tank1/foo@bar", "user", "host.ex.com", "tank1/foo@bar", "user@host.ex.com", True)
        self.run_test("user@host.ex.com:tank1/foo#bar", "user", "host.ex.com", "tank1/foo#bar", "user@host.ex.com", True)
        self.run_test(
            "whitespace user@host.ex.com:tank1/foo/bar",
            "whitespace user",
            "host.ex.com",
            "tank1/foo/bar",
            "whitespace user@host.ex.com",
            True,
        )
        self.run_test(
            "user@whitespace\thost:tank1/foo/bar",
            "user",
            "whitespace\thost",
            "tank1/foo/bar",
            "user@whitespace\thost",
            True,
        )
        self.run_test("user@host:tank1/foo/whitespace\tbar", "user", "host", "tank1/foo/whitespace\tbar", "user@host", True)
        self.run_test("user@host:tank1/foo/whitespace\nbar", "user", "host", "tank1/foo/whitespace\nbar", "user@host", True)
        self.run_test("user@host:tank1/foo/whitespace\rbar", "user", "host", "tank1/foo/whitespace\rbar", "user@host", True)
        self.run_test("user@host:tank1/foo/space bar", "user", "host", "tank1/foo/space bar", "user@host", False)
        self.run_test("user@||1:tank1/foo/space bar", "user", "::1", "tank1/foo/space bar", "user@::1", False)
        self.run_test("user@::1:tank1/foo/space bar", "", "user@", ":1:tank1/foo/space bar", "user@", True)


#############################################################################
class TestArgumentParser(AbstractTestCase):

    def test_help(self) -> None:
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e, suppress_output():
            parser.parse_args(["--help"])
        self.assertEqual(0, e.exception.code)

    def test_version(self) -> None:
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e, suppress_output():
            parser.parse_args(["--version"])
        self.assertEqual(0, e.exception.code)

    def test_missing_datasets(self) -> None:
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e, suppress_output():
            parser.parse_args(["--retries=1"])
        self.assertEqual(2, e.exception.code)

    def test_missing_dst_dataset(self) -> None:
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e, suppress_output():
            parser.parse_args(["src_dataset"])  # Each SRC_DATASET must have a corresponding DST_DATASET
        self.assertEqual(2, e.exception.code)

    def test_program_must_not_be_empty_string(self) -> None:
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e, suppress_output():
            parser.parse_args(["src_dataset", "src_dataset", "--zfs-program="])
        self.assertEqual(2, e.exception.code)


###############################################################################
class TestAddRecvPropertyOptions(AbstractTestCase):

    def setUp(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        self.p = self.make_params(args=args)
        self.p.src = Remote("src", args, self.p)
        self.p.dst = Remote("dst", args, self.p)
        self.p.zfs_recv_x_names = ["xprop1", "xprop2"]
        self.p.zfs_recv_ox_names = {"existing"}
        self.job = bzfs.Job()
        self.job.params = self.p

    def test_appends_x_options_when_supported(self) -> None:
        recv_opts: list[str] = []
        with (
            patch.object(self.p, "is_program_available", return_value=True),
            patch("bzfs_main.replication._zfs_get", return_value={}),
        ):
            result_opts, set_opts = _add_recv_property_options(self.job, True, recv_opts, "ds", {})
        self.assertEqual(["-x", "xprop1", "-x", "xprop2"], result_opts)
        self.assertEqual([], set_opts)
        # original zfs_recv_ox_names remains unchanged
        self.assertEqual({"existing"}, self.p.zfs_recv_ox_names)

    def test_skips_x_options_when_not_supported(self) -> None:
        recv_opts: list[str] = []
        with (
            patch.object(self.p, "is_program_available", return_value=False),
            patch("bzfs_main.replication._zfs_get", return_value={}),
        ):
            result_opts, set_opts = _add_recv_property_options(self.job, True, recv_opts, "ds", {})
        self.assertEqual([], result_opts)
        self.assertEqual([], set_opts)
        self.assertEqual({"existing"}, self.p.zfs_recv_ox_names)

    def test_default_o_regex_contains_safe_props(self) -> None:
        """Ensure default zfs_recv_o regex whitelists safe properties."""
        pattern, _ = self.p.zfs_recv_o_config.include_regexes[0]
        self.assertIn("compression", pattern.pattern)
        self.assertNotIn("mountpoint", pattern.pattern)

    def test_appends_o_options_for_whitelisted_props(self) -> None:
        """Add '-o' options for properties allowed by default regex."""
        self.p.zfs_recv_x_names = []
        self.p.zfs_recv_ox_names = set()
        recv_opts: list[str] = []

        def fake_zfs_get(*args: object) -> dict[str, str | None]:
            propnames = args[5]
            if propnames == "all":
                return {"compression": None}
            if propnames == "compression":
                return {"compression": "on"}
            raise AssertionError(f"unexpected props {propnames}")

        with (
            patch.object(self.p, "is_program_available", return_value=True),
            patch("bzfs_main.replication._zfs_get", side_effect=fake_zfs_get),
        ):
            result_opts, set_opts = _add_recv_property_options(self.job, True, recv_opts, "ds", {})
        self.assertEqual(["-o", "compression=on"], result_opts)
        self.assertEqual([], set_opts)
        self.assertEqual(set(), self.p.zfs_recv_ox_names)

    def test_skips_o_options_for_non_whitelisted_props(self) -> None:
        """Ignore properties that are absent from default whitelist."""
        self.p.zfs_recv_x_names = []
        self.p.zfs_recv_ox_names = set()
        recv_opts: list[str] = []

        def fake_zfs_get(*args: object) -> dict[str, str | None]:
            propnames = args[5]
            if propnames == "all":
                return {"mountpoint": None}
            if propnames == "":
                return {}
            raise AssertionError(f"unexpected props {propnames}")

        with (
            patch.object(self.p, "is_program_available", return_value=True),
            patch("bzfs_main.replication._zfs_get", side_effect=fake_zfs_get),
        ):
            result_opts, set_opts = _add_recv_property_options(self.job, True, recv_opts, "ds", {})
        self.assertEqual([], result_opts)
        self.assertEqual([], set_opts)
        self.assertEqual(set(), self.p.zfs_recv_ox_names)


#############################################################################
class TestPreservePropertiesValidation(AbstractTestCase):

    def setUp(self) -> None:
        self.args = self.argparser_parse_args(
            [
                "src",
                "dst",
                "--preserve-properties",
                "recordsize",
                "--zfs-send-program-opts=--props --raw --compressed",
            ]
        )
        self.p = self.make_params(args=self.args)
        self.job = bzfs.Job()
        self.job.params = self.p

        # Setup minimal remote objects. The specific details don't matter much as they are mocked.
        self.p.src = Remote("src", self.args, self.p)
        self.p.dst = Remote("dst", self.args, self.p)
        self.p.src.ssh_user_host = "src_host"
        self.p.dst.ssh_user_host = "dst_host"
        self.p.connection_pools = {
            "local": MagicMock(spec=ConnectionPools),
            "src": MagicMock(spec=ConnectionPools),
            "dst": MagicMock(spec=ConnectionPools),
        }
        self.p.zpool_features = {"src": {}, "dst": {}}
        self.p.available_programs = {"local": {"ssh": ""}, "src": {}, "dst": {}}

    @patch.object(bzfs_main.detect, "_detect_zpool_features")
    @patch.object(bzfs_main.detect, "_detect_available_programs_remote")
    def test_preserve_properties_fails_on_old_zfs_with_props(
        self, mock_detect_zpool_features: MagicMock, mock_detect_available_programs_remote: MagicMock
    ) -> None:
        """Verify that using --preserve-properties with --props fails on ZFS < 2.2.0."""
        with patch.object(self.p, "is_program_available") as mock_is_available:
            # Make the mock specific to the zfs>=2.2.0 check on dst
            def side_effect(program: str, location: str) -> bool:
                if program == ZFS_VERSION_IS_AT_LEAST_2_2_0 and location == "dst":
                    return False
                return True  # Assume other programs are available

            mock_is_available.side_effect = side_effect
            with self.assertRaises(SystemExit) as cm:
                detect_available_programs(self.job)
            self.assertEqual(cm.exception.code, DIE_STATUS)
            self.assertIn("--preserve-properties is unreliable on destination ZFS < 2.2.0", str(cm.exception))

    @patch.object(bzfs_main.detect, "_detect_zpool_features")
    @patch.object(bzfs_main.detect, "_detect_available_programs_remote")
    def test_preserve_properties_succeeds_on_new_zfs_with_props(
        self, mock_detect_zpool_features: MagicMock, mock_detect_available_programs_remote: MagicMock
    ) -> None:
        """Verify that --preserve-properties is allowed on ZFS >= 2.2.0."""
        with patch.object(self.p, "is_program_available", return_value=True):
            # This should not raise an exception
            detect_available_programs(self.job)

    @patch.object(bzfs_main.detect, "_detect_zpool_features")
    @patch.object(bzfs_main.detect, "_detect_available_programs_remote")
    def test_preserve_properties_succeeds_on_old_zfs_without_props(
        self, mock_detect_zpool_features: MagicMock, mock_detect_available_programs_remote: MagicMock
    ) -> None:
        """Verify --preserve-properties is allowed on old ZFS if --props is not used."""
        self.p.zfs_send_program_opts.remove("--props")  # Modify the params for this test case
        with patch.object(self.p, "is_program_available") as mock_is_available:

            def side_effect(program: str, location: str) -> bool:
                # Simulate old ZFS by returning False for version checks, but True for other programs like ssh.
                if program == ZFS_VERSION_IS_AT_LEAST_2_2_0:
                    return False
                return True

            mock_is_available.side_effect = side_effect
            # This should not raise an exception
            detect_available_programs(self.job)


#############################################################################
class TestJobMethods(AbstractTestCase):
    """Tests Job helpers that don't require ZFS on the host."""

    def make_job(self, args: list[str]) -> bzfs.Job:
        """Creates a Job with parsed ``args`` for reuse across tests."""
        parsed = self.argparser_parse_args(args=args)
        job = bzfs.Job()
        job.params = self.make_params(args=parsed)
        job.is_test_mode = True
        src_ds, dst_ds = job.params.root_dataset_pairs[0]
        job.params.src.root_dataset = job.params.src.basis_root_dataset = src_ds
        job.params.dst.root_dataset = job.params.dst.basis_root_dataset = dst_ds
        return job

    def test_sudo_cmd_root_user(self) -> None:
        """Root user needs no sudo prefix nor delegation."""
        job = self.make_job(["src", "dst"])
        with patch("os.getuid", return_value=0):
            sudo, deleg = job.sudo_cmd("", "")
        self.assertEqual("", sudo)
        self.assertFalse(deleg)

    def test_sudo_cmd_nonroot_with_privilege_elevation(self) -> None:
        """Non-root user with elevation returns 'sudo -n'."""
        job = self.make_job(["src", "dst"])
        with patch("os.getuid", return_value=1):
            sudo, deleg = job.sudo_cmd("user@host", "user")
        self.assertEqual("sudo -n", sudo)
        self.assertFalse(deleg)

    def test_sudo_cmd_nonroot_no_privilege_elevation(self) -> None:
        """Non-root without elevation requests delegation instead."""
        job = self.make_job(["src", "dst"])
        job.params.enable_privilege_elevation = False
        with patch("os.getuid", return_value=1):
            sudo, deleg = job.sudo_cmd("user@host", "user")
        self.assertEqual("", sudo)
        self.assertTrue(deleg)

    def test_sudo_cmd_disabled_program_raises(self) -> None:
        """Disabled sudo program exits when elevation is required."""
        job = self.make_job(["src", "dst"])
        job.params.sudo_program = bzfs_main.detect.DISABLE_PRG
        with patch("os.getuid", return_value=1):
            with self.assertRaises(SystemExit) as cm:
                job.sudo_cmd("user@host", "user")
        self.assertIn("sudo CLI is not available on host: user@host", str(cm.exception))

    def test_validate_once_compiles_recv_and_regexes(self) -> None:
        """validate_once extracts property names and filters regexes."""
        job = self.make_job(
            [
                "src",
                "dst",
                "--zfs-recv-program-opts",
                "-o foo=1 -x bar",
                "--exclude-dataset-regex",
                "",
                "!.*",
                "foo.*",
            ]
        )
        job.validate_once()
        self.assertSetEqual({"foo", "bar"}, job.params.zfs_recv_ox_names)
        patterns = [r.pattern for r, _ in job.params.tmp_exclude_dataset_regexes]
        self.assertIn("foo.*(?:/.*)?", patterns)

    def test_validate_once_requires_bytes_option(self) -> None:
        """Missing --bytes or --bits in pv opts raises SystemExit."""
        job = self.make_job(
            [
                "src",
                "dst",
                "--pv-program-opts=--eta",
            ]
        )
        with self.assertRaises(SystemExit):
            job.validate_once()

    def test_validate_once_compiles_snapshot_regex_filters(self) -> None:
        """Snapshot regex filters are compiled into pattern tuples."""
        job = self.make_job(["src", "dst"])
        job.params.snapshot_filters = [
            [SnapshotFilter(SNAPSHOT_REGEX_FILTER_NAME, None, (["^foo$"], ["bar"]))],
        ]
        job.validate_once()
        filt = job.params.snapshot_filters[0][0]
        self.assertIsInstance(filt.options[0][0][0], re.Pattern)
        self.assertEqual("bar", filt.options[1][0][0].pattern)

    def test_validate_once_snapshot_regex_filter_defaults_to_match_all(self) -> None:
        """Empty include list defaults to a match-all regex."""
        job = self.make_job(["src", "dst"])
        job.params.snapshot_filters = [
            [SnapshotFilter(SNAPSHOT_REGEX_FILTER_NAME, None, (["baz"], []))],
        ]
        job.validate_once()
        regex_filter = job.params.snapshot_filters[0][0]
        self.assertEqual(".*", regex_filter.options[1][0][0].pattern)

    def test_validate_once_ignores_non_regex_snapshot_filter(self) -> None:
        """Non-regex snapshot filters remain untouched."""
        job = self.make_job(["src", "dst"])
        job.params.snapshot_filters = [[SnapshotFilter("include_snapshot_times", None, ["opt"])]]
        job.validate_once()
        self.assertEqual(["opt"], job.params.snapshot_filters[0][0].options)

    def test_validate_once_separates_abs_and_rel_exclude_datasets(self) -> None:
        """Absolute and relative exclude datasets are split correctly."""
        job = self.make_job(
            [
                "--exclude-dataset=/pool/src/abs",
                "--exclude-dataset=rel",
                "pool/src",
                "pool/dst",
            ]
        )
        job.validate_once()
        self.assertListEqual(["/pool/src/abs"], job.params.abs_exclude_datasets)
        patterns = [r.pattern for r, _ in job.params.tmp_exclude_dataset_regexes]
        self.assertIn("rel(?:/.*)?", patterns)

    def test_validate_once_separates_abs_and_rel_include_datasets(self) -> None:
        """Absolute and relative include datasets are split correctly."""
        job = self.make_job(
            [
                "--include-dataset=/pool/dst/abs",
                "--include-dataset=rel",
                "pool/src",
                "pool/dst",
            ]
        )
        job.validate_once()
        self.assertListEqual(["/pool/dst/abs"], job.params.abs_include_datasets)
        patterns = [r.pattern for r, _ in job.params.tmp_include_dataset_regexes]
        self.assertIn("rel(?:/.*)?", patterns)

    def test_validate_once_skips_pv_checks_when_disabled(self) -> None:
        """No pv option validation occurs when pv program is disabled."""
        job = self.make_job(
            [
                "--pv-program",
                bzfs_main.detect.DISABLE_PRG,
                "src",
                "dst",
            ]
        )
        job.validate_once()  # should not raise

    def test_validate_once_requires_eta_option(self) -> None:
        """Missing --eta or -e triggers validation error."""
        job = self.make_job(
            [
                "src",
                "dst",
                "--pv-program-opts=--bytes --fineta --average-rate",
            ]
        )
        job.isatty = True
        with self.assertRaises(SystemExit):
            job.validate_once()

    def test_validate_once_requires_fineta_option(self) -> None:
        """Missing --fineta or -I triggers validation error."""
        job = self.make_job(
            [
                "src",
                "dst",
                "--pv-program-opts=--bytes --eta --average-rate",
            ]
        )
        job.isatty = True
        with self.assertRaises(SystemExit):
            job.validate_once()

    def test_validate_once_requires_average_rate_option(self) -> None:
        """Missing --average-rate or -a triggers validation error."""
        job = self.make_job(
            [
                "src",
                "dst",
                "--pv-program-opts=--bytes --eta --fineta",
            ]
        )
        job.isatty = True
        with self.assertRaises(SystemExit):
            job.validate_once()

    def test_validate_once_accepts_pv_options_long_forms(self) -> None:
        """All long-form pv options pass validation."""
        job = self.make_job(
            [
                "src",
                "dst",
                "--pv-program-opts=--bytes --eta --fineta --average-rate",
            ]
        )
        job.isatty = True
        job.validate_once()

    def test_validate_once_accepts_pv_options_short_forms(self) -> None:
        """Short-form pv options also satisfy validation checks."""
        job = self.make_job(
            [
                "src",
                "dst",
                "--pv-program-opts=-b -e -I -a",
            ]
        )
        job.isatty = True
        job.validate_once()

    def test_latest_common_snapshot_considers_excluded_src_snapshots(self) -> None:
        """Purpose: Ensure incremental replication chooses the latest common base snapshot/bookmark by GUID among all
        source points, even when the replication policy filters out that snapshot name. This guards against false
        "no common snapshot" conflicts that would otherwise force destructive rollback or full sends.

        Scenario: The source contains three points in createtxg order: d1 (guid1), h1 (guid2), and d2 (guid3).
        The destination already has h1 (guid2). The policy under test includes dailies (matching d.*) and excludes
        hourlies (matching h.*). Even though h1 is excluded from replication output by policy, the algorithm must still
        use h1 as the incremental base because it is the true latest common ancestor by GUID.

        Design: The test synthesizes zfs list outputs and patches network/ZFS invocations to isolate the replication
        planning logic deterministically. It also patches _incremental_send_steps_wrapper to yield a single -i step
        from h1 to d2 if and only if the correct base is discovered. The assertion verifies that incremental planning
        is invoked once, proving the implementation selects the safe base and proceeds incrementally.

        Failure mode before fix: If base discovery mistakenly consults filtered snapshots, h1 is invisible on src,
        no latest common base is found, and the code escalates to conflict or full-send paths on a non-empty dst.
        """
        # Arrange a job with snapshot filters: include dailies, exclude hourlies
        job = self.make_job(["pool/src", "pool/dst", "--include-snapshot-regex", "d.*", "--exclude-snapshot-regex", "h.*"])
        job.validate_once()
        job.max_workers.setdefault("src", 1)
        job.max_workers.setdefault("dst", 1)

        # Craft synthetic zfs list outputs (guid\tname) for src and dst
        src_dataset = "pool/src"
        dst_dataset = "pool/dst"
        src_lines_all = ["guid1\tpool/src@d1", "guid2\tpool/src@h1", "guid3\tpool/src@d2"]
        dst_lines = ["guid2\tpool/dst@h1"]

        # Patch out remote calls and heavy operations to isolate the logic
        with (
            patch("bzfs_main.replication.try_ssh_command") as mock_try,
            patch("bzfs_main.replication._run_zfs_send_receive") as mock_run,
            patch("bzfs_main.replication._estimate_send_size", return_value=0),
            patch("bzfs_main.replication._check_zfs_dataset_busy", return_value=True),
            patch("bzfs_main.replication._create_zfs_filesystem"),
            patch("bzfs_main.replication._create_zfs_bookmarks"),
            patch("bzfs_main.replication._zfs_set"),
            patch("bzfs_main.replication.are_bookmarks_enabled", return_value=False),
            patch("bzfs_main.replication.is_zpool_feature_enabled_or_active", return_value=False),
            patch(
                "bzfs_main.replication._incremental_send_steps_wrapper",
                return_value=[("-i", "pool/src@h1", "pool/src@d2", ["pool/src@d2"])],
            ) as mock_steps,
        ):

            # Filtered listing on src will be built by the code; we just return the raw list and let filters apply.
            def side_effect(
                _job: Job,
                remote: Remote,
                level: int,
                is_dry: bool = False,
                print_stdout: bool = False,
                cmd: list[str] | None = None,
                exists: bool = True,
                error_trigger: str | None = None,
            ) -> str:
                assert cmd is not None
                cmd_str = " ".join(cmd)
                if " list -t snapshot -d 1 -s createtxg -Hp -o guid,name " in cmd_str and cmd[-1] == dst_dataset:
                    return "\n".join(dst_lines) + "\n"
                if " list -t snapshot" in cmd_str and cmd[-1] == src_dataset:
                    return "\n".join(src_lines_all) + "\n"
                return ""

            mock_try.side_effect = side_effect
            mock_run.return_value = None

            # Act: Should complete without conflict and run a single incremental send based on h1->d2.
            replicate_dataset(job, src_dataset, tid="1/1", retry=Retry(0))
            # Assert: incremental planning was invoked (found common base -> incremental)
            mock_steps.assert_called_once()
            # And exactly one zfs send/receive pipeline executed
            mock_run.assert_called_once()
            # Strengthen: verify that the send command uses incremental flag '-i'
            args, _ = mock_run.call_args
            send_cmd = args[3]
            self.assertIn("-i", send_cmd)

    @patch("bzfs_main.bzfs.detect_available_programs")
    @patch("bzfs_main.bzfs.is_zpool_feature_enabled_or_active", return_value=False)
    def test_validate_task_errors_on_same_dataset(self, _feature: MagicMock, detect: MagicMock) -> None:
        """validate_task aborts when source and destination are identical."""
        detect.side_effect = lambda job: None
        job = self.make_job(
            [
                "--ssh-src-host",
                "h",
                "--ssh-dst-host",
                "h",
                "pool/a",
                "pool/a",
            ]
        )
        job.validate_once()
        with self.assertRaises(SystemExit):
            job.validate_task()

    @patch("bzfs_main.bzfs.detect_available_programs")
    @patch("bzfs_main.bzfs.is_zpool_feature_enabled_or_active", return_value=False)
    def test_validate_task_errors_on_src_descendant_of_dst(self, _feature: MagicMock, detect: MagicMock) -> None:
        """Recursive replication fails when src is under dst."""
        detect.side_effect = lambda job: job.params.available_programs.update({"src": {}, "dst": {}, "local": {}})
        job = self.make_job(["--recursive", "pool/a/b", "pool/a"])
        job.validate_once()
        with self.assertRaises(SystemExit):
            job.validate_task()

    @patch("bzfs_main.bzfs.detect_available_programs")
    @patch("bzfs_main.bzfs.is_zpool_feature_enabled_or_active", return_value=False)
    def test_validate_task_errors_on_dst_descendant_of_src(self, _feature: MagicMock, detect: MagicMock) -> None:
        """Recursive replication fails when dst is under src."""
        detect.side_effect = lambda job: job.params.available_programs.update({"src": {}, "dst": {}, "local": {}})
        job = self.make_job(["--recursive", "pool/a", "pool/a/b"])
        job.validate_once()
        with self.assertRaises(SystemExit):
            job.validate_task()

    @patch("bzfs_main.bzfs.detect_available_programs")
    @patch("bzfs_main.bzfs.is_zpool_feature_enabled_or_active", return_value=False)
    def test_validate_task_allows_overlap_when_not_recursive(self, _feature: MagicMock, detect: MagicMock) -> None:
        """Non-recursive replication permits overlapping datasets."""
        detect.side_effect = lambda job: job.params.available_programs.update({"src": {}, "dst": {}, "local": {}})
        job = self.make_job(["pool/a", "pool/a/b"])
        job.validate_once()
        job.validate_task()  # should not raise

    @patch("bzfs_main.bzfs.detect_available_programs")
    @patch("bzfs_main.bzfs.is_zpool_feature_enabled_or_active", return_value=False)
    def test_validate_task_sets_remote_fields(self, _feature: MagicMock, detect: MagicMock) -> None:
        """validate_task populates sudo prefix and nonlocal flag."""
        detect.side_effect = lambda job: job.params.available_programs.update({"src": {}, "dst": {}, "local": {}})
        job = self.make_job(
            [
                "pool/src",
                "remote:pool/dst",
            ]
        )
        job.validate_once()
        with patch.object(job, "sudo_cmd", return_value=("sudo -n", False)) as sudo_mock:
            job.validate_task()
        sudo_mock.assert_any_call(job.params.dst.ssh_user_host, job.params.dst.ssh_user)
        self.assertEqual(2, sudo_mock.call_count)
        self.assertTrue(job.params.dst.is_nonlocal)
        self.assertEqual("sudo -n", job.params.dst.sudo)
        self.assertFalse(job.params.dst.use_zfs_delegation)

    def test_print_replication_stats_logs_bytes(self) -> None:
        """print_replication_stats logs byte counts when pv is present."""
        job = self.make_job(["src", "dst"])
        p = job.params
        p.available_programs = {"local": {"pv": "pv"}}
        p.log_params.pv_log_file = "pv.log"
        job.num_snapshots_replicated = 5
        with (
            patch("time.monotonic_ns", return_value=2_000_000_000),
            patch("bzfs_main.bzfs.count_num_bytes_transferred_by_zfs_send", return_value=1024 * 1024),
        ):
            job.print_replication_stats(0)
        msg = cast(MagicMock, p.log.info).call_args[0][1]
        self.assertIn("zfs sent 5 snapshots in 2s.", msg)
        self.assertIn("zfs sent 1 MiB [512 KiB/s].", msg)


#############################################################################
class TestPythonVersionCheck(AbstractTestCase):
    """Test version check near top of program:
    if sys.version_info < (3, 9):
        print(f"ERROR: {prog_name} requires Python version >= 3.9!", file=sys.stderr)
        sys.exit(die_status)
    """

    @patch("sys.exit")
    @patch("sys.version_info", new=(3, 8))
    def test_version_below_3_9(self, mock_exit: MagicMock) -> None:
        """Verifies exit on unsupported Python version while silencing output."""
        with patch("sys.stdout"), patch("sys.stderr"):
            import importlib

            importlib.reload(bzfs)  # Reload module to apply version patch
            mock_exit.assert_called_with(DIE_STATUS)

    @patch("sys.exit")
    @patch("sys.version_info", new=(3, 9))
    def test_version_3_9_or_higher(self, mock_exit: MagicMock) -> None:
        import importlib

        importlib.reload(bzfs)  # Reload module to apply version patch
        mock_exit.assert_not_called()


#############################################################################
class TestDeleteDstDatasetsTask(AbstractTestCase):
    """Ensures destination-only datasets and trees are pruned correctly."""

    def _job(self) -> bzfs.Job:
        """Creates a job with minimal params and root datasets."""
        args = self.argparser_parse_args(args=["src", "dst"])
        job = bzfs.Job()
        job.params = self.make_params(args=args)
        job.params.src.root_dataset = "s"
        job.params.dst.root_dataset = "d"
        return job

    def test_deletes_orphan_tree(self) -> None:
        """Deletes destination dataset trees absent from the source."""
        job = self._job()
        basis_src = ["s", "s/keep"]
        basis_dst = ["d", "d/keep", "d/orphan", "d/orphan/child"]
        dst = list(basis_dst)
        with patch("bzfs_main.bzfs.delete_datasets") as mock_del:
            basis_after, dst_after = job.delete_dst_datasets_task(basis_src, basis_dst, dst)
        mock_del.assert_called_once()
        self.assertSetEqual({"d/orphan", "d/orphan/child"}, set(mock_del.call_args[0][2]))
        self.assertListEqual(["d", "d/keep"], basis_after)
        self.assertListEqual(["d", "d/keep"], dst_after)

    def test_keeps_parent_with_excluded_child(self) -> None:
        """Preserves parent dataset when an excluded child exists."""
        job = self._job()
        basis_src = ["s", "s/keep"]
        basis_dst = ["d", "d/keep", "d/delete"]
        dst = ["d", "d/delete"]
        with patch("bzfs_main.bzfs.delete_datasets") as mock_del:
            basis_after, dst_after = job.delete_dst_datasets_task(basis_src, basis_dst, dst)
        mock_del.assert_called_once()
        self.assertSetEqual({"d/delete"}, set(mock_del.call_args[0][2]))
        self.assertListEqual(["d", "d/keep"], basis_after)
        self.assertListEqual(["d"], dst_after)

    def test_preserves_source_datasets(self) -> None:
        """Skips deletion when destination datasets exist on the source."""
        job = self._job()
        basis_src = ["s", "s/foo"]
        basis_dst = ["d", "d/foo"]
        dst = list(basis_dst)
        with patch("bzfs_main.bzfs.delete_datasets") as mock_del:
            basis_after, dst_after = job.delete_dst_datasets_task(basis_src, basis_dst, dst)
        mock_del.assert_called_once()
        self.assertSetEqual(set(), set(mock_del.call_args[0][2]))
        self.assertListEqual(basis_dst, basis_after)
        self.assertListEqual(dst, dst_after)


#############################################################################
class TestDeleteEmptyDstDatasetsTask(AbstractTestCase):
    """Tests deletion of empty destination datasets, including bookmark logic."""

    def _job(self, delete_snapshots_and_bookmarks: bool = False) -> bzfs.Job:
        """Creates a Job with params toggling bookmark-aware deletion."""
        args = self.argparser_parse_args(args=["src", "dst"])
        job = bzfs.Job()
        job.params = self.make_params(args=args)
        job.params.delete_empty_dst_datasets_if_no_bookmarks_and_no_snapshots = delete_snapshots_and_bookmarks
        return job

    def test_deletes_only_leaf_without_snapshots(self) -> None:
        """Removes only dataset subtrees without snapshots."""
        job = self._job()
        basis = ["a", "a/b", "a/b/c", "a/d"]
        datasets = list(basis)
        with (
            patch(
                "bzfs_main.bzfs.zfs_list_snapshots_in_parallel",
                return_value=iter([["a/b/c@s1"]]),
            ) as mock_list,
            patch("bzfs_main.bzfs.delete_datasets") as mock_del,
        ):
            basis_datasets_result, result = job.delete_empty_dst_datasets_task(basis, datasets)
        self.assertListEqual(["a", "a/b", "a/b/c"], result)
        self.assertListEqual(["a", "a/b", "a/b/c"], basis_datasets_result)
        mock_del.assert_called_once()
        self.assertSetEqual({"a/d"}, set(mock_del.call_args[0][2]))
        mock_list.assert_called_once()
        self.assertListEqual(sorted(basis), mock_list.call_args[0][3])

    def test_keeps_dataset_with_bookmark(self) -> None:
        """Keeps dataset when only bookmarks exist and bookmarks count."""
        job = self._job(delete_snapshots_and_bookmarks=True)
        basis = ["b"]
        datasets = list(basis)
        with (
            patch(
                "bzfs_main.bzfs.are_bookmarks_enabled",
                return_value=True,
            ),
            patch(
                "bzfs_main.bzfs.zfs_list_snapshots_in_parallel",
                return_value=iter([["b#bm1"]]),
            ) as mock_list,
            patch("bzfs_main.bzfs.delete_datasets") as mock_del,
        ):
            basis_datasets_result, result = job.delete_empty_dst_datasets_task(basis, datasets)
        self.assertListEqual(["b"], result)
        self.assertListEqual(["b"], basis_datasets_result)
        mock_del.assert_called_once()
        self.assertSetEqual(set(), set(mock_del.call_args[0][2]))
        mock_list.assert_called_once()
        cmd = mock_list.call_args[0][2]
        self.assertIn("bookmark,snapshot", cmd)

    def test_deletes_entire_empty_tree(self) -> None:
        """Deletes parent and child when all lack snapshots."""
        job = self._job()
        basis = ["a", "a/b"]
        datasets = list(basis)
        with (
            patch(
                "bzfs_main.bzfs.zfs_list_snapshots_in_parallel",
                return_value=iter([]),
            ) as mock_list,
            patch("bzfs_main.bzfs.delete_datasets") as mock_del,
        ):
            basis_datasets_result, result = job.delete_empty_dst_datasets_task(basis, datasets)
        self.assertListEqual([], result)
        self.assertListEqual([], basis_datasets_result)
        mock_del.assert_called_once()
        self.assertSetEqual({"a", "a/b"}, set(mock_del.call_args[0][2]))
        mock_list.assert_called_once()


#############################################################################
class TestHandleMinMaxSnapshots(AbstractTestCase):
    """Verifies snapshot selection logic in handle_minmax_snapshots."""

    def test_callbacks_and_missing_datasets(self) -> None:
        """Checks latest/oldest callbacks and reporting of datasets without snapshots."""

        args = self.argparser_parse_args(args=["src", "dst"])
        job = bzfs.Job()
        job.params = self.make_params(args=args)
        job.is_test_mode = True
        remote = MagicMock(spec=Remote)
        label1 = SnapshotLabel("bzfs_", "", "", "_hourly")
        label2 = SnapshotLabel("bzfs_", "us-west-1_", "", "_hourly")
        datasets = ["tank/ds1", "tank/ds2", "tank/ds3", "tank/ds4"]

        lines = [
            "1\t100\ttank/ds1@bzfs_2023-01-01_00:00:00_hourly",
            "2\t200\ttank/ds1@bzfs_2024-01-01_00:00:00_hourly",
            "1\t150\ttank/ds2@unrelated_snap",
            "1\t100\ttank/ds4@bzfs_us-west-1_2023-01-01_00:00:00_hourly",
            "2\t200\ttank/ds4@bzfs_us-west-1_2024-01-01_00:00:00_hourly",
        ]

        def fake_zfs_list(
            job_obj: bzfs.Job,
            remote_obj: Remote,
            cmd: list[str],
            datasets_arg: list[str],
            ordered: bool = False,
        ) -> Iterator[list[str]]:
            yield lines

        latest_calls: list[tuple[int, int, str, str]] = []
        oldest_calls: list[tuple[int, int, str, str]] = []

        def latest_cb(i: int, t: int, dataset: str, snap: str) -> None:
            latest_calls.append((i, t, dataset, snap))

        def oldest_cb(i: int, t: int, dataset: str, snap: str) -> None:
            oldest_calls.append((i, t, dataset, snap))

        with patch("bzfs_main.bzfs.zfs_list_snapshots_in_parallel", new=fake_zfs_list):
            missing = job.handle_minmax_snapshots(remote, datasets, [label1, label2], latest_cb, oldest_cb)

        self.assertListEqual(["tank/ds3"], missing)

        expected_latest = [
            (0, 200, "tank/ds1", "bzfs_2024-01-01_00:00:00_hourly"),
            (1, 0, "tank/ds1", ""),
            (0, 0, "tank/ds2", ""),
            (1, 0, "tank/ds2", ""),
            (0, 0, "tank/ds4", ""),
            (1, 200, "tank/ds4", "bzfs_us-west-1_2024-01-01_00:00:00_hourly"),
        ]
        self.assertListEqual(expected_latest, latest_calls)

        expected_oldest = [
            (0, 100, "tank/ds1", "bzfs_2023-01-01_00:00:00_hourly"),
            (1, 0, "tank/ds1", ""),
            (0, 0, "tank/ds2", ""),
            (1, 0, "tank/ds2", ""),
            (0, 0, "tank/ds4", ""),
            (1, 100, "tank/ds4", "bzfs_us-west-1_2023-01-01_00:00:00_hourly"),
        ]
        self.assertListEqual(expected_oldest, oldest_calls)

    def test_latest_only(self) -> None:
        """Ensures latest callback runs when oldest is omitted and reports missing datasets."""

        args = self.argparser_parse_args(args=["src", "dst"])
        job = bzfs.Job()
        job.params = self.make_params(args=args)
        job.is_test_mode = True
        remote = MagicMock(spec=Remote)

        label = SnapshotLabel("bzfs_", "", "", "_hourly")
        datasets = ["tank/ds1", "tank/ds2"]

        lines = [
            "1\t100\ttank/ds1@bzfs_2023-01-01_00:00:00_hourly",
            "2\t200\ttank/ds1@bzfs_2024-01-01_00:00:00_hourly",
        ]

        def fake_zfs_list(
            job_obj: bzfs.Job,
            remote_obj: Remote,
            cmd: list[str],
            datasets_arg: list[str],
            ordered: bool = False,
        ) -> Iterator[list[str]]:
            yield lines

        latest_calls: list[tuple[int, int, str, str]] = []

        def latest_cb(i: int, t: int, dataset: str, snap: str) -> None:
            latest_calls.append((i, t, dataset, snap))

        with patch("bzfs_main.bzfs.zfs_list_snapshots_in_parallel", new=fake_zfs_list):
            missing = job.handle_minmax_snapshots(remote, datasets, [label], latest_cb)

        self.assertListEqual(["tank/ds2"], missing)
        self.assertListEqual(
            [(0, 200, "tank/ds1", "bzfs_2024-01-01_00:00:00_hourly")],
            latest_calls,
        )


#############################################################################
class TestFindDatasetsToSnapshot(AbstractTestCase):
    """Tests scheduling logic of find_datasets_to_snapshot.

    Purpose: ensure Job correctly determines datasets needing snapshots.
    Assumes snapshots are scheduled hourly.
    Design: exercises both caching and non-caching code paths using mocked helpers.
    """

    def _job(self) -> bzfs.Job:
        """Constructs a Job configured for snapshot scheduling tests.

        Purpose: reuse consistent snapshot plan setup.
        Assumes '--create-src-snapshots' is enabled.
        Design: builds Job with hourly plan.
        """

        args = self.argparser_parse_args(
            args=[
                "--create-src-snapshots",
                "--create-src-snapshots-plan",
                str({"bzfs": {"onsite": {"hourly": 1}}}),
                "src",
                "dst",
            ]
        )
        job = bzfs.Job()
        job.params = self.make_params(args=args)
        job.is_test_mode = True
        return job

    def test_selects_due_datasets_without_cache(self) -> None:
        """Schedules only datasets with outdated snapshots when cache is disabled.

        Purpose: verify baseline scheduling without cache.
        Assumes hourly schedule and deterministic timestamps.
        Design: stub handle_minmax_snapshots to provide known snapshot ages.
        """

        job = self._job()
        config = job.params.create_src_snapshots_config
        config.current_datetime = datetime(2024, 1, 1, 5, 30, tzinfo=timezone.utc)
        config.tz = timezone.utc
        label = config.snapshot_labels()[0]

        def fake_handle(
            self: bzfs.Job,
            remote: Remote,
            datasets: list[str],
            labels: list[SnapshotLabel],
            fn_latest: Callable[[int, int, str, str], None],
            fn_oldest: Callable[[int, int, str, str], None] | None = None,
            fn_on_finish_dataset: Callable[[str], None] | None = None,
        ) -> list[str]:
            times = {
                "tank/a": datetime(2024, 1, 1, 4, tzinfo=timezone.utc),
                "tank/b": datetime(2024, 1, 1, 5, tzinfo=timezone.utc),
            }
            for ds in datasets:
                fn_latest(0, int(times[ds].timestamp()), ds, "")
            return []

        with (
            patch("bzfs_main.bzfs.is_caching_snapshots", return_value=False),
            patch.object(bzfs.Job, "handle_minmax_snapshots", new=fake_handle),
        ):
            result = job.find_datasets_to_snapshot(["tank/a", "tank/b"])

        self.assertDictEqual({label: ["tank/a"]}, result)

    def test_uses_cache_and_fallback(self) -> None:
        """Combines cached and probed datasets when caching is enabled.

        Purpose: ensure cache results merge with fallback queries.
        Assumes one dataset has valid cache and another lacks it.
        Design: fake cache supplies one creation time; stub handles remaining datasets.
        """

        job = self._job()
        config = job.params.create_src_snapshots_config

        config.current_datetime = datetime(2024, 1, 1, 5, 30, tzinfo=timezone.utc)
        config.tz = timezone.utc
        label = config.snapshot_labels()[0]
        creation_time_a = int(datetime(2024, 1, 1, 4, tzinfo=timezone.utc).timestamp())

        class FakeCache:
            def __init__(self) -> None:
                # Encode a realistic invariant: creation_atime <= snapshots_changed_mtime and
                # dataset-level '=' cache equals the per-label mtime and job.src_properties.
                self.mapping: dict[str, int] = {
                    "tank/a": creation_time_a,
                    f"tank/a@{label.suffix}": creation_time_a,
                }

            def last_modified_cache_file(self, remote: Remote, dataset: str, label_obj: str | None = None) -> str:
                if label_obj is not None:
                    return f"{dataset}@{label.suffix}"
                return dataset

            def get_snapshots_changed(self, cache_file: str) -> int:
                return self.mapping.get(cache_file, 0)

            # Emulate codepath that reads both atime and mtime, by returning (creation_time, snapshots_changed)
            # for per-label paths and (0, snapshots_changed) for dataset-level '=' paths
            def get_snapshots_changed2(self, cache_file: str) -> tuple[int, int]:
                if cache_file in self.mapping:
                    if "@" in cache_file:
                        ds = cache_file.split("@", 1)[0]
                        return (self.mapping[cache_file], self.mapping.get(ds, 0))
                    return (0, self.mapping[cache_file])
                return (0, 0)

            def invalidate_last_modified_cache_dataset(self, dataset: str) -> None:
                """Do nothing for tests."""

        job.cache = cast(SnapshotCache, FakeCache())
        job.src_properties = {
            "tank/a": bzfs.DatasetProperties(0, creation_time_a),
            "tank/b": bzfs.DatasetProperties(0, 456),
        }

        received: list[str] = []

        def fake_handle(
            self: bzfs.Job,
            remote: Remote,
            datasets: list[str],
            labels: list[SnapshotLabel],
            fn_latest: Callable[[int, int, str, str], None],
            fn_oldest: Callable[[int, int, str, str], None] | None = None,
            fn_on_finish_dataset: Callable[[str], None] | None = None,
        ) -> list[str]:
            received.extend(datasets)
            for ds in datasets:
                fn_latest(0, int(datetime(2024, 1, 1, 5, tzinfo=timezone.utc).timestamp()), ds, "")
                if fn_on_finish_dataset:
                    fn_on_finish_dataset(ds)
            return []

        with (
            patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
            patch.object(bzfs.Job, "handle_minmax_snapshots", new=fake_handle),
            patch("bzfs_main.bzfs.set_last_modification_time_safe"),
        ):
            result = job.find_datasets_to_snapshot(["tank/a", "tank/b"])

        self.assertListEqual(["tank/b"], received)
        self.assertDictEqual({label: ["tank/a"]}, result)


#############################################################################
class TestTerminationEventBehavior(AbstractTestCase):
    """Covers termination_event branches in bzfs.Job, including early loop break and daemon sleep wake-up."""

    class _DummyPR:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def reset(self) -> None:
            pass

        def pause(self) -> None:
            pass

        def stop(self) -> None:
            pass

    def make_job(self, args: list[str]) -> bzfs.Job:
        parsed = self.argparser_parse_args(args=args)
        job = bzfs.Job()
        job.params = self.make_params(args=parsed)
        job.is_test_mode = True
        return job

    def test_run_tasks_terminates_when_event_pre_set(self) -> None:
        """If termination_event is already set at task loop start, terminate() is called and no task runs."""
        job = self.make_job(["src1", "dst1", "src2", "dst2"])
        job.termination_event.set()

        with (
            patch.object(job, "validate_once", return_value=None),
            patch.object(bzfs, "ProgressReporter", self._DummyPR),
            patch.object(job, "validate_task", side_effect=AssertionError("validate_task must not be called")),
            patch.object(job, "run_task", side_effect=AssertionError("run_task must not be called")),
            patch.object(job, "sleep_until_next_daemon_iteration", return_value=False) as sleep_mock,
            patch.object(job, "terminate") as term_mock,
        ):
            job.run_tasks()
            term_mock.assert_called_once()
            # Ensure early break: sleep called exactly once after breaking out of inner loop
            sleep_mock.assert_called_once()

    def test_run_tasks_terminates_after_first_task_when_event_set_mid_loop(self) -> None:
        """When termination_event is set between task iterations, terminate() is called and subsequent tasks are skipped."""
        job = self.make_job(["s1", "d1", "s2", "d2", "s3", "d3"])
        run_calls: list[str] = []

        def run_task_side_effect() -> None:
            run_calls.append("run_task")
            if len(run_calls) == 1:
                job.termination_event.set()  # simulate external termination after first task

        with (
            patch.object(job, "validate_once", return_value=None),
            patch.object(bzfs, "ProgressReporter", self._DummyPR),
            patch.object(job, "validate_task", return_value=None),
            patch.object(job, "run_task", side_effect=run_task_side_effect),
            patch.object(job, "sleep_until_next_daemon_iteration", return_value=False),
            patch.object(job, "terminate") as term_mock,
        ):
            job.run_tasks()
            self.assertEqual(1, len(run_calls), f"Expected exactly one task to run, got {len(run_calls)}")
            term_mock.assert_called_once()

    def test_sleep_until_next_daemon_iteration_wakes_on_termination(self) -> None:
        """sleep_until_next_daemon_iteration returns early with False when termination_event is set during wait."""
        job = self.make_job(["src", "dst"])

        job.progress_reporter = cast(ProgressReporter, self._DummyPR())
        # Provide required log file attribute for logging in sleep_until_next_daemon_iteration
        job.params.log_params.log_file = "dummy.log"

        # Stop time far in the future to ensure we rely on termination_event for wake-up
        stoptime_nanos = time.monotonic_ns() + 2 * 1_000_000_000  # 2s

        job.termination_event.set()
        result: bool = job.sleep_until_next_daemon_iteration(stoptime_nanos)
        self.assertLess(time.monotonic_ns(), stoptime_nanos - 0.2 * 1_000_000_000)
        self.assertFalse(result, "Expected False when termination_event is set during daemon sleep")


#############################################################################
class TestPerJobTermination(AbstractTestCase):
    """Verifies that Job.terminate() can be scoped to only kill the Job's own child processes."""

    def test_scoped_termination_kills_only_registered_children(self) -> None:
        job = bzfs.Job()
        # Start two child processes; register only one with the job
        p1 = subprocess.Popen(["sleep", "1"])
        p2 = subprocess.Popen(["sleep", "1"])
        try:
            job.subprocesses.register_child_pid(p1.pid)
            self.assertIsNone(p1.poll())
            self.assertIsNone(p2.poll())

            # Enable job-scoped termination and invoke terminate()
            job.terminate()
            time.sleep(0.05)

            # Registered child should be terminated; unregistered should still be running
            self.assertIsNotNone(p1.poll(), "Registered child should be terminated")
            self.assertIsNone(p2.poll(), "Unregistered child should remain running")
        finally:
            with contextlib.suppress(Exception):
                p1.kill()
            with contextlib.suppress(Exception):
                p2.kill()


#############################################################################
@unittest.skip("benchmark; enable for performance comparison")
class TestPerformance(AbstractTestCase):

    def test_close_fds(self) -> None:
        """See https://bugs.python.org/issue42738
        and https://github.com/python/cpython/pull/113118/commits/654f63053aee162a6e59fa894b2cd8ee82a33a77"""
        iters = 2000
        runs = 3
        for close_fds in [False, True]:
            for _ in range(runs):
                start_time_nanos = time.time_ns()
                for _ in range(iters):
                    cmd = ["echo", "hello"]
                    stdout = subprocess.run(
                        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True, close_fds=close_fds
                    ).stdout
                    self.assertTrue(stdout)
                secs = (time.time_ns() - start_time_nanos) / 1000_000_000
                logging.getLogger(__name__).warning(
                    f"close_fds={close_fds}: Took {secs:.1f} seconds, iters/sec: {iters/secs:.1f}"
                )
