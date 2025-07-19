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
"""Unit tests for bzfs --compare-snapshot-lists."""

from __future__ import annotations
import unittest
from typing import (
    Any,
)

import bzfs_main.compare_snapshot_lists
from bzfs_main.compare_snapshot_lists import ComparableSnapshot
from bzfs_tests.abstract_testcase import AbstractTestCase


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestCompareSnapshotLists,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestCompareSnapshotLists(AbstractTestCase):
    s = "s"
    d = "d"
    a = "a"

    def merge_sorted_iterators(self, src: list[Any], dst: list[Any], choice: str) -> list[tuple[Any, ...]]:
        s, d, a = self.s, self.d, self.a
        return list(bzfs_main.compare_snapshot_lists.merge_sorted_iterators([s, d, a], choice, iter(src), iter(dst)))

    def assert_merge_sorted_iterators(
        self,
        expected: list[tuple[Any, ...]],
        src: list[Any],
        dst: list[Any],
        choice: str = f"{s}+{d}+{a}",
        invert: bool = True,
    ) -> None:
        s, d, a = self.s, self.d, self.a
        self.assertListEqual(expected, self.merge_sorted_iterators(src, dst, choice))
        if invert:
            inverted = [(s if item[0] == d else d if item[0] == s else a,) + item[1:] for item in expected]
            self.assertListEqual(inverted, self.merge_sorted_iterators(dst, src, choice))

    def test_merge_sorted_iterators(self) -> None:
        s, d, a = self.s, self.d, self.a
        self.assert_merge_sorted_iterators([], [], [])
        self.assert_merge_sorted_iterators([(s, "x")], ["x"], [])
        self.assert_merge_sorted_iterators([(d, "x")], [], ["x"])
        self.assert_merge_sorted_iterators([(a, "x", "x")], ["x"], ["x"])
        self.assert_merge_sorted_iterators([(d, "x"), (s, "y")], ["y"], ["x"])
        src = [10, 13, 16, 17, 18]
        dst = [11, 12, 14, 15, 16, 17]
        self.assert_merge_sorted_iterators(
            [(s, 10), (d, 11), (d, 12), (s, 13), (d, 14), (d, 15), (a, 16, 16), (a, 17, 17), (s, 18)], src, dst
        )
        self.assert_merge_sorted_iterators([(d, "x"), (d, "z")], ["y"], ["x", "z"], d, invert=False)
        self.assert_merge_sorted_iterators([(s, "y")], ["y"], ["x", "z"], s, invert=False)
        self.assert_merge_sorted_iterators([], ["x"], ["x", "z"], s, invert=False)
        self.assert_merge_sorted_iterators([], ["y"], ["x", "z"], a)

    def test_merge_sorted_iterators_with_comparable_snapshots(self) -> None:
        """Tests the merge logic with actual ComparableSnapshot objects, specifically covering the case where a source-only
        bookmark is correctly reported after the fix."""

        src_snap = ComparableSnapshot(key=("ds1", "guid1"), cols=["1", "guid1", "1", "1024", "tank/src/ds1@snap1"])
        self.assert_merge_sorted_iterators(expected=[(self.s, src_snap)], src=[src_snap], dst=[], choice="src", invert=False)

        src_bookmark = ComparableSnapshot(key=("ds1", "guid2"), cols=["2", "guid2", "2", "-", "tank/src/ds1#snap2"])
        self.assert_merge_sorted_iterators(
            expected=[(self.s, src_bookmark)],
            src=[src_bookmark],
            dst=[],
            choice="src",
            invert=False,
        )

        # Test mixed case: src has a bookmark, dst has a different snapshot.
        dst_snap = ComparableSnapshot(key=("ds1", "guid3"), cols=["3", "guid3", "3", "1024", "tank/dst/ds1@snap3"])
        self.assert_merge_sorted_iterators(
            expected=[(self.s, src_bookmark), (self.d, dst_snap)], src=[src_bookmark], dst=[dst_snap], choice="src+dst"
        )

        # Test common snapshot: both src and dst have the same snapshot.
        common_snap_src = ComparableSnapshot(key=("ds1", "guid4"), cols=["4", "guid4", "4", "1024", "tank/src/ds1@snap4"])
        common_snap_dst = ComparableSnapshot(key=("ds1", "guid4"), cols=["4", "guid4", "4", "2048", "tank/dst/ds1@snap4"])
        self.assert_merge_sorted_iterators(
            expected=[(self.a, common_snap_src, common_snap_dst)], src=[common_snap_src], dst=[common_snap_dst], choice="all"
        )
