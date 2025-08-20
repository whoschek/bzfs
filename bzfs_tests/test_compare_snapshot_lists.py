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
import itertools
import os
import tempfile
import unittest
from typing import Any, Iterable, Iterator
from unittest.mock import MagicMock, patch

import bzfs_main.compare_snapshot_lists
from bzfs_main import configuration
from bzfs_main.bzfs import Job
from bzfs_main.compare_snapshot_lists import (
    ComparableSnapshot,
    _print_datasets,
    run_compare_snapshot_lists,
)
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
        return list(bzfs_main.compare_snapshot_lists._merge_sorted_iterators([s, d, a], choice, iter(src), iter(dst)))

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

    def test_print_datasets_inserts_missing_and_sorts(self) -> None:
        """_print_datasets sorts rel_datasets and adds placeholders for missing datasets."""

        items = [("b", 1), ("b", 2)]
        group = itertools.groupby(items, key=lambda e: e[0])
        calls: list[tuple[str, list[int]]] = []

        def capture(rel_ds: str, entries: Iterable[tuple[str, int]]) -> None:
            calls.append((rel_ds, [val for _, val in entries]))

        _print_datasets(group, capture, ["c", "a", "b"])
        expected = [("a", []), ("b", [1, 2]), ("c", [])]
        self.assertEqual(expected, calls)

    def test_print_datasets_handles_empty_group(self) -> None:
        """Even with no group entries, _print_datasets emits all relative datasets."""

        items: list[tuple[str, int]] = []
        group = itertools.groupby(items, key=lambda e: e[0])
        calls: list[tuple[str, list[int]]] = []

        def capture(rel_ds: str, entries: Iterable[tuple[str, int]]) -> None:
            calls.append((rel_ds, [val for _, val in entries]))

        _print_datasets(group, capture, ["a", "b"])
        self.assertEqual([("a", []), ("b", [])], calls)

    def test_print_datasets_appends_unknown_dataset(self) -> None:
        """Datasets absent from rel_datasets are still forwarded after known ones."""

        items = [("z", 42)]
        group = itertools.groupby(items, key=lambda e: e[0])
        calls: list[tuple[str, list[int]]] = []

        def capture(rel_ds: str, entries: Iterable[tuple[str, int]]) -> None:
            calls.append((rel_ds, [val for _, val in entries]))

        _print_datasets(group, capture, ["a"])
        self.assertEqual([("a", []), ("z", [42])], calls)

    def _run_compare(
        self,
        src_lines: dict[str, list[str]],
        dst_lines: dict[str, list[str]],
        src_datasets: list[str],
        dst_datasets: list[str],
        compare_choice: str = "src+dst+all",
        is_solaris: bool = False,
    ) -> tuple[list[list[str]], list[list[str]]]:
        """Runs run_compare_snapshot_lists() with stubbed ZFS output and returns TSV rows.

        Assumes src_lines and dst_lines map dataset names to zfs list output lines. Uses patching to avoid the external zfs
        CLI and isolates snapshot comparison logic.
        """

        job = Job()
        job.is_test_mode = True
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "job.log")
            args = self.argparser_parse_args(["tank/src", "tank/dst", "--compare-snapshot-lists"])
            log_params = MagicMock(spec=configuration.LogParams)
            log_params.log_file = log_file
            p = self.make_params(args, log_params=log_params)
            p.src.root_dataset = "tank/src"
            p.dst.root_dataset = "tank/dst"
            p.compare_snapshot_lists = compare_choice
            job.params = p

            def fake_zfs_list(
                _job: Job, r: Any, _cmd: list[str], datasets: list[str], ordered: bool = True
            ) -> Iterator[list[str]]:
                mapping = src_lines if r.location == "src" else dst_lines
                for ds in datasets:
                    yield mapping.get(ds, [])

            with patch("bzfs_main.compare_snapshot_lists.zfs_list_snapshots_in_parallel", side_effect=fake_zfs_list), patch(
                "bzfs_main.compare_snapshot_lists.are_bookmarks_enabled", return_value=True
            ), patch("bzfs_main.compare_snapshot_lists.is_solaris_zfs", return_value=is_solaris):
                run_compare_snapshot_lists(job, src_datasets, dst_datasets)

            tsv_dir = log_file[:-4] + ".cmp"
            tsv_file = os.path.join(tsv_dir, "tank~src%tank~dst.tsv")
            rel_file = tsv_file[:-4] + ".rel_datasets_tsv"
            with open(tsv_file, "r", encoding="utf-8") as fd:
                rows = [line.rstrip("\n").split("\t") for line in fd if not line.startswith("location")]
            with open(rel_file, "r", encoding="utf-8") as fd:
                rel_rows = [line.rstrip("\n").split("\t") for line in fd if not line.startswith("location")]
        return rows, rel_rows

    def test_run_compare_snapshot_lists_generates_expected_tsv(self) -> None:
        """Runs run_compare_snapshot_lists() and verifies TSV rows for src-only, dst-only and shared snapshots."""

        src_lines = {
            "tank/src/bar": ["400\tg4\t4\t4000\ttank/src/bar@snapBar"],
            "tank/src/foo": [
                "100\tg1\t1\t1000\ttank/src/foo@snapA",
                "200\tg2\t2\t2000\ttank/src/foo@snapB",
                "200\tg2\t2\t-\ttank/src/foo#snapB",
            ],
        }
        dst_lines = {
            "tank/dst/baz": ["500\tg5\t5\t5000\ttank/dst/baz@snapBaz"],
            "tank/dst/foo": [
                "100\tg1\t1\t1500\ttank/dst/foo@snapA",
                "300\tg3\t3\t3000\ttank/dst/foo@snapC",
            ],
        }

        rows, rel_rows = self._run_compare(
            src_lines,
            dst_lines,
            ["tank/src/bar", "tank/src/foo"],
            ["tank/dst/baz", "tank/dst/foo"],
        )
        self.assertEqual(5, len(rows))
        self.assertTrue(any(r[0] == "src" and r[7] == "tank/src/bar@snapBar" for r in rows))
        self.assertTrue(any(r[0] == "dst" and r[7] == "tank/dst/baz@snapBaz" for r in rows))
        self.assertTrue(any(r[0] == "all" and r[7] == "tank/src/foo@snapA" for r in rows))
        self.assertTrue(any(r[0] == "src" and r[7] == "tank/src/foo@snapB" for r in rows))
        self.assertTrue(any(r[0] == "dst" and r[7] == "tank/dst/foo@snapC" for r in rows))

        expected_rel = [
            ["src", "/bar", "tank/src/bar", ""],
            ["dst", "/baz", "", "tank/dst/baz"],
            ["all", "/foo", "tank/src/foo", "tank/dst/foo"],
        ]
        self.assertEqual(expected_rel, rel_rows)

    def test_run_compare_snapshot_lists_respects_compare_choices(self) -> None:
        """Ensures shared snapshots are omitted when 'all' is excluded from compare choices."""

        src_lines = {
            "tank/src/foo": [
                "100\tg1\t1\t1000\ttank/src/foo@snapA",
                "200\tg2\t2\t2000\ttank/src/foo@snapB",
            ]
        }
        dst_lines = {
            "tank/dst/foo": [
                "100\tg1\t1\t1500\ttank/dst/foo@snapA",
                "300\tg3\t3\t3000\ttank/dst/foo@snapC",
            ]
        }
        rows, rel_rows = self._run_compare(
            src_lines, dst_lines, ["tank/src/foo"], ["tank/dst/foo"], compare_choice="src+dst"
        )
        self.assertEqual(2, len(rows))
        self.assertTrue(all(r[0] != "all" for r in rows))
        self.assertEqual([["all", "/foo", "tank/src/foo", "tank/dst/foo"]], rel_rows)

    def test_run_compare_snapshot_lists_handles_empty_snapshot_dataset(self) -> None:
        """Verifies datasets with no snapshots still appear in dataset summary TSV."""

        src_lines: dict[str, list[str]] = {"tank/src/empty": []}
        rows, rel_rows = self._run_compare(src_lines, {}, ["tank/src/empty"], [])
        self.assertEqual([], rows)
        self.assertEqual([["src", "/empty", "tank/src/empty", ""]], rel_rows)

    def test_run_compare_snapshot_lists_ignores_duplicate_bookmarks(self) -> None:
        """Bookmarks with a matching snapshot GUID must not produce extra TSV rows."""

        src_lines = {
            "tank/src/foo": [
                "100\tg1\t1\t1000\ttank/src/foo@snapA",
                "100\tg1\t1\t-\ttank/src/foo#snapA",
            ]
        }
        rows, rel_rows = self._run_compare(src_lines, {}, ["tank/src/foo"], [])
        self.assertEqual(1, len(rows))
        self.assertEqual("src", rows[0][0])
        self.assertEqual("tank/src/foo@snapA", rows[0][7])
        self.assertEqual([["src", "/foo", "tank/src/foo", ""]], rel_rows)

    def test_run_compare_snapshot_lists_matches_by_guid_even_if_names_differ(self) -> None:
        """Snapshots with identical GUIDs but different names are classified as shared."""

        src_lines = {"tank/src/foo": ["100\tg1\t1\t1000\ttank/src/foo@snap1"]}
        dst_lines = {"tank/dst/foo": ["100\tg1\t1\t1500\ttank/dst/foo@renamed"]}
        rows, rel_rows = self._run_compare(src_lines, dst_lines, ["tank/src/foo"], ["tank/dst/foo"])
        self.assertEqual(1, len(rows))
        self.assertEqual("all", rows[0][0])
        self.assertEqual("tank/src/foo@snap1", rows[0][7])
        self.assertEqual([["all", "/foo", "tank/src/foo", "tank/dst/foo"]], rel_rows)

    def test_run_compare_snapshot_lists_solaris_sanitizes_written_field(self) -> None:
        """On Solaris, the 'written' column value 'snapshot' is sanitized to '-'."""

        src_lines = {"tank/src/foo": ["100\tg1\t1\tsnapshot\ttank/src/foo@snapA"]}
        rows, rel_rows = self._run_compare(src_lines, {}, ["tank/src/foo"], [], is_solaris=True)
        self.assertEqual("-", rows[0][9])
        self.assertEqual([["src", "/foo", "tank/src/foo", ""]], rel_rows)
