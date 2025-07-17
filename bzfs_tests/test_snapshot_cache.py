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
"""Unit tests for SnapshotCache."""

from __future__ import annotations
import os
import tempfile
import unittest
from unittest.mock import (
    MagicMock,
    patch,
)

from bzfs_main import bzfs
from bzfs_main.configuration import Remote
from bzfs_main.snapshot_cache import (
    set_last_modification_time,
    set_last_modification_time_safe,
)
from bzfs_tests.abstract_testcase import AbstractTestCase


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestSnapshotCache,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestSnapshotCache(AbstractTestCase):

    def test_set_last_modification_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file = os.path.join(tmpdir, "foo")
            set_last_modification_time(file, unixtime_in_secs=0)
            self.assertEqual(0, round(os.stat(file).st_mtime))
            set_last_modification_time(file, unixtime_in_secs=0)
            self.assertEqual(0, round(os.stat(file).st_mtime))
            set_last_modification_time(file, unixtime_in_secs=1000, if_more_recent=True)
            self.assertEqual(1000, round(os.stat(file).st_mtime))
            set_last_modification_time(file, unixtime_in_secs=0, if_more_recent=True)
            self.assertEqual(1000, round(os.stat(file).st_mtime))
            set_last_modification_time_safe(file, unixtime_in_secs=1001, if_more_recent=True)
            self.assertEqual(1001, round(os.stat(file).st_mtime))

    def test_set_last_modification_time_with_file_not_found_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file = os.path.join(tmpdir, "foo")
            with patch("bzfs_main.snapshot_cache.os_utime", side_effect=FileNotFoundError):
                with self.assertRaises(FileNotFoundError):
                    set_last_modification_time(file + "nonexisting", unixtime_in_secs=1001, if_more_recent=False)
                file = os.path.join(file, "x", "nonexisting2")
                set_last_modification_time_safe(file, unixtime_in_secs=1001, if_more_recent=False)

    @patch("bzfs_main.snapshot_cache.itr_ssh_cmd_parallel")
    def test_zfs_get_snapshots_changed_parsing(self, mock_itr_parallel: MagicMock) -> None:
        job = bzfs.Job()
        job.params = self.make_params(args=self.argparser_parse_args(args=["src", "dst"]))
        self.mock_remote = MagicMock(spec=Remote)  # spec helps catch calls to non-existent attrs

        mock_itr_parallel.return_value = [  # normal input
            [
                "12345\tdataset/valid1",
                "789\tdataset/valid2",
            ]
        ]
        results = job.cache.zfs_get_snapshots_changed(self.mock_remote, ["d1", "d2", "d3", "d4"])
        self.assertDictEqual({"dataset/valid1": 12345, "dataset/valid2": 789}, results)

        # Simulate output from a failing 'zfs list' command captured on its stdout.
        # This could be partial output, or error messages if zfs wrote them to stdout.
        mock_itr_parallel.return_value = [
            [
                "12345\tdataset/valid1",
                "ERROR: zfs command failed for dataset/invalid2",  # Line without tab, from stdout
                "789\tdataset/valid2",
            ]
        ]
        results = job.cache.zfs_get_snapshots_changed(self.mock_remote, ["d1", "d2", "d3", "d4"])
        self.assertDictEqual({"dataset/valid1": 12345}, results)

        mock_itr_parallel.return_value = [
            [
                "12345\tdataset/valid1",
                "123\t",  # empty dataset
                "789\tdataset/valid2",
            ]
        ]
        results = job.cache.zfs_get_snapshots_changed(self.mock_remote, ["d1", "d2", "d3", "d4"])
        self.assertDictEqual({"dataset/valid1": 12345}, results)

        mock_itr_parallel.return_value = [
            [
                "12345\tdataset/valid1",
                "\tfoo",  # missing timestamp
                "789\tdataset/valid2",
            ]
        ]
        results = job.cache.zfs_get_snapshots_changed(self.mock_remote, ["d1", "d2", "d3", "d4"])
        self.assertDictEqual({"dataset/valid1": 12345, "foo": 0, "dataset/valid2": 789}, results)
