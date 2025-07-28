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
"""Unit tests for snapshot filtering utilities; Confirm date- and pattern-based selectors yield the expected snapshots."""

from __future__ import annotations
import argparse
import io
import logging
import sys
import time
import unittest
from datetime import datetime, timedelta
from typing import (
    Any,
    Tuple,
    Union,
    cast,
)
from unittest.mock import (
    MagicMock,
    patch,
)

import bzfs_main.argparse_actions
from bzfs_main import bzfs
from bzfs_main.argparse_actions import SnapshotFilter
from bzfs_main.bzfs import (
    Job,
)
from bzfs_main.configuration import (
    Params,
    Remote,
)
from bzfs_main.filter import (
    SNAPSHOT_REGEX_FILTER_NAME,
    _filter_datasets_by_exclude_property,
    _filter_snapshots_by_regex,
    dataset_regexes,
    filter_datasets,
    filter_lines,
    filter_lines_except,
    filter_properties,
    filter_snapshots,
)
from bzfs_main.utils import (
    LOG_DEBUG,
    UNIX_TIME_INFINITY_SECS,
    compile_regexes,
    unixtime_fromisoformat,
)
from bzfs_tests.abstract_testcase import AbstractTestCase


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestHelperFunctions,
        TestTimeRangeAction,
        TestRankRangeAction,
        TestFilterSnapshotsWithBookmarks,
        TestFilterDatasets,
        TestFilterDatasetsByExcludeProperty,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class CommonTest(AbstractTestCase):

    def __init__(self, methodName: str = "runTest") -> None:  # noqa: N803
        super().__init__(methodName)

    def filter_snapshots_by_times_and_rank(
        self, snapshots: list[str], timerange: str, ranks: list[str] = [], loglevel: int = logging.DEBUG  # noqa: B006
    ) -> list[str]:
        args = self.argparser_parse_args(args=["src", "dst", "--include-snapshot-times-and-ranks", timerange, *ranks])
        job = bzfs.Job()
        job.params = self.make_params(args=args)
        job.params.log.setLevel(loglevel)
        snapshots = [f"{i}\t" + snapshot for i, snapshot in enumerate(snapshots)]  # simulate creation time
        results = filter_snapshots(job, snapshots)
        results = [result.split("\t", 1)[1] for result in results]  # drop creation time
        return results


#############################################################################
class TestHelperFunctions(CommonTest):

    def test_filter_lines(self) -> None:
        input_list = ["apple\tred", "banana\tyellow", "cherry\tred", "date\tbrown"]

        # Empty input_set
        self.assertListEqual([], filter_lines(input_list, set()))

        # input_set with some matching elements
        self.assertListEqual(["apple\tred", "cherry\tred"], filter_lines(input_list, {"apple", "cherry"}))

        # input_set with no matching elements
        self.assertListEqual([], filter_lines(input_list, {"grape", "kiwi"}))

        # Empty input_list
        self.assertListEqual([], filter_lines([], {"apple"}))

        # input_set with all elements matching
        self.assertListEqual(input_list, filter_lines(input_list, {"apple", "banana", "cherry", "date"}))

    def test_filter_lines_except(self) -> None:
        input_list = ["apple\tred", "banana\tyellow", "cherry\tred", "date\tbrown"]

        # Empty input_set
        self.assertListEqual(input_list, filter_lines_except(input_list, set()))

        # input_set with some elements to exclude
        self.assertListEqual(["banana\tyellow", "date\tbrown"], filter_lines_except(input_list, {"apple", "cherry"}))

        # input_set with no elements present in the input_list (exclude nothing new)
        self.assertListEqual(input_list, filter_lines_except(input_list, {"grape", "kiwi"}))

        # Empty input_list
        self.assertListEqual([], filter_lines_except([], {"apple"}))

        # input_set with all elements from input_list (exclude all)
        self.assertListEqual([], filter_lines_except(input_list, {"apple", "banana", "cherry", "date"}))

    def test_filter_datasets_with_test_mode_is_false(self) -> None:
        # test with Job.is_test_mode == False for coverage with the assertion therein disabled
        args = self.argparser_parse_args(args=["src", "dst"])
        job = bzfs.Job()
        job.params = self.make_params(args=args)
        src = Remote("src", args, job.params)
        filter_datasets(job, src, ["dataset1"])

    def test_dataset_regexes(self) -> None:
        mock_src = MagicMock(spec=Remote, root_dataset="tank/src")
        mock_dst = MagicMock(spec=Remote, root_dataset="tank/dst")
        datasets = ["foo", "bar/", "/tank/src/a", "/tank/dst/b", "/ignorenonexistent", "baz", ""]
        result = dataset_regexes(mock_src, mock_dst, datasets)
        self.assertListEqual(["foo", "bar", "a", "b", "baz", ".*"], result)

    def test_dataset_regexes_all_branches(self) -> None:
        mock_src = MagicMock(spec=Remote, root_dataset="tank/src")
        mock_dst = MagicMock(spec=Remote, root_dataset="tank/dst")
        datasets = ["/tank/src", "/tank/dst/", "/nonexistent", "foo", "bar/", "/tank/src/a", "/tank/dst//c/", ""]
        result = dataset_regexes(mock_src, mock_dst, datasets)
        self.assertListEqual([".*", ".*", "foo", "bar", "a", "/c", ".*"], result)

    def test_filter_snapshots_by_regex(self) -> None:
        job = MagicMock(spec=Job, params=MagicMock(spec=Params, log=MagicMock()))
        snapshots = ["\tds@a1", "\tds@b2", "\tds@c3", "\tds@keep", "\tds#bookmark"]
        regexes = (
            compile_regexes([".*c.*"]),
            compile_regexes(["a.*", "b2"]),
        )
        self.assertEqual(["\tds@a1", "\tds@b2"], _filter_snapshots_by_regex(job, snapshots, regexes))

    def test_filter_snapshots_by_regex_debug(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        log = logging.getLogger("debug_snapshots")
        log.setLevel(LOG_DEBUG)
        stream = io.StringIO()
        log.addHandler(logging.StreamHandler(stream))
        job = bzfs.Job()
        job.params = self.make_params(args=args, log=log)
        snapshots = ["\tds@a1", "\tds@b2", "\tds@c3", "\tds@other", "\tds#bookmark"]
        regexes = (
            compile_regexes(["c3"]),
            compile_regexes(["a1", "c3"]),
        )
        result = _filter_snapshots_by_regex(job, snapshots, regexes)
        self.assertEqual(["\tds@a1"], result)
        log_output = stream.getvalue()
        self.assertIn("Including b/c snapshot regex", log_output)
        self.assertIn("Excluding b/c snapshot regex", log_output)

    def test_filter_properties(self) -> None:
        job = MagicMock(spec=Job, params=MagicMock(spec=Params, log=MagicMock()))
        props: dict[str, str | None] = {"p1": "v1", "skip": "v", "p3": "v3"}
        include_regexes = compile_regexes(["p.*"])
        exclude_regexes = compile_regexes([".*3"])
        self.assertEqual({"p1": "v1"}, filter_properties(job.params, props, include_regexes, exclude_regexes))

    def test_filter_properties_debug(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        log = logging.getLogger("debug_props")
        log.setLevel(LOG_DEBUG)
        stream = io.StringIO()
        log.addHandler(logging.StreamHandler(stream))
        job = bzfs.Job()
        job.params = self.make_params(args=args, log=log)
        props: dict[str, str | None] = {"a1": "v1", "a2": "v2", "skip": "v"}
        include_regexes = compile_regexes(["a.*"])
        exclude_regexes = compile_regexes(["a2"])
        result = filter_properties(job.params, props, include_regexes, exclude_regexes)
        self.assertEqual({"a1": "v1"}, result)
        log_output = stream.getvalue()
        self.assertIn("Including b/c property regex", log_output)
        self.assertIn("Excluding b/c property regex", log_output)

    def test_filter_snapshots_calls_regex_filter(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        log = logging.getLogger("snap_call")
        job = bzfs.Job()
        job.params = self.make_params(args=args, log=log)
        regexes = (compile_regexes([]), compile_regexes(["keep"]))
        job.params.snapshot_filters = [[SnapshotFilter(SNAPSHOT_REGEX_FILTER_NAME, None, regexes)]]
        snapshots = ["0\tds@keep", "0\tds@other"]
        with patch("bzfs_main.filter._filter_snapshots_by_regex", return_value=[snapshots[0]]) as mock_f:
            result = filter_snapshots(job, snapshots)
            mock_f.assert_called_once_with(job, snapshots, regexes=regexes, filter_bookmarks=False)
        self.assertEqual([snapshots[0]], result)


#############################################################################
class TestTimeRangeAction(CommonTest):
    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--time-n-ranks", action=bzfs_main.argparse_actions.TimeRangeAndRankRangeAction, nargs="+")

    def parse_duration(self, arg: str) -> argparse.Namespace:
        return self.parser.parse_args(["--time-n-ranks", arg + " ago..anytime"])

    def parse_timestamp(self, arg: str) -> argparse.Namespace:
        return self.parser.parse_args(["--time-n-ranks", arg + "..anytime"])

    def test_parse_unix_time(self) -> None:  # Test Unix time in integer seconds
        args = self.parse_timestamp("1696510080")
        self.assertEqual(1696510080, args.time_n_ranks[0][0])

        args = self.parse_timestamp("0")
        self.assertEqual(0, args.time_n_ranks[0][0])

    def test_valid_durations(self) -> None:
        args = self.parse_duration("5millis")
        self.assertEqual(timedelta(milliseconds=5), args.time_n_ranks[0][0])

        args = self.parse_duration("5milliseconds")
        self.assertEqual(timedelta(milliseconds=5), args.time_n_ranks[0][0])

        args = self.parse_duration("5secs")
        self.assertEqual(timedelta(seconds=5), args.time_n_ranks[0][0])

        args = self.parse_duration("5 seconds")
        self.assertEqual(timedelta(seconds=5), args.time_n_ranks[0][0])

        args = self.parse_duration("30mins")
        self.assertEqual(timedelta(seconds=1800), args.time_n_ranks[0][0])

        args = self.parse_duration("30minutes")
        self.assertEqual(timedelta(seconds=1800), args.time_n_ranks[0][0])

        args = self.parse_duration("2hours")
        self.assertEqual(timedelta(seconds=2 * 60 * 60), args.time_n_ranks[0][0])

        args = self.parse_duration("0days")
        self.assertEqual(timedelta(seconds=0), args.time_n_ranks[0][0])

        args = self.parse_duration("10weeks")
        self.assertEqual(timedelta(seconds=6048000), args.time_n_ranks[0][0])

        args = self.parse_duration("15secs")
        self.assertEqual(timedelta(seconds=15), args.time_n_ranks[0][0])

        args = self.parse_duration("60mins")
        self.assertEqual(timedelta(seconds=3600), args.time_n_ranks[0][0])
        self.assertEqual(3600, args.time_n_ranks[0][0].total_seconds())

        args = self.parse_duration("2hours")
        self.assertEqual(timedelta(seconds=2 * 60 * 60), args.time_n_ranks[0][0])

        args = self.parse_duration("3days")
        self.assertEqual(timedelta(seconds=259200), args.time_n_ranks[0][0])

        args = self.parse_duration("2weeks")
        self.assertEqual(timedelta(seconds=1209600), args.time_n_ranks[0][0])

        # Test with spaces
        args = self.parse_duration("  30mins")
        self.assertEqual(timedelta(seconds=1800), args.time_n_ranks[0][0])

        args = self.parse_duration("5secs")
        self.assertEqual(timedelta(seconds=5), args.time_n_ranks[0][0])

    def test_invalid_time_spec(self) -> None:
        with self.assertRaises(SystemExit):
            self.parse_duration("")  # Empty string

        with self.assertRaises(SystemExit):
            self.parse_duration("  ")  # Empty string

        with self.assertRaises(SystemExit):
            self.parse_duration("10x")  # Invalid unit

        with self.assertRaises(SystemExit):
            self.parse_duration("-5mins")  # Negative number

        with self.assertRaises(SystemExit):
            self.parse_duration("abcd")  # Completely invalid format

        with self.assertRaises(SystemExit):
            self.parse_timestamp("2024-1-1")  # must be 2024-01-01

        with self.assertRaises(SystemExit):
            self.parse_timestamp("2024-10-35")  # Month does not have 35 days

        with self.assertRaises(SystemExit):
            self.parser.parse_args(["--time-n-ranks", "60_mins..anytime"])  # Missing 'ago'

        with self.assertRaises(SystemExit):
            self.parser.parse_args(["--time-n-ranks", "60_mins_ago"])  # Missing ..

    def test_parse_datetime(self) -> None:
        # Test ISO 8601 datetime strings without timezone
        args = self.parse_timestamp("2024-01-01")
        self.assertEqual(int(datetime.fromisoformat("2024-01-01").timestamp()), args.time_n_ranks[0][0])

        args = self.parse_timestamp("2024-12-31")
        self.assertEqual(int(datetime.fromisoformat("2024-12-31").timestamp()), args.time_n_ranks[0][0])

        args = self.parse_timestamp("2024-10-05T14:48:55")
        self.assertEqual(int(datetime.fromisoformat("2024-10-05T14:48:55").timestamp()), args.time_n_ranks[0][0])
        self.assertNotEqual(int(datetime.fromisoformat("2024-10-05T14:48:01").timestamp()), args.time_n_ranks[0][0])

    def test_parse_datetime_with_timezone(self) -> None:
        tz_py_version = (3, 11)
        if sys.version_info < tz_py_version:
            self.skipTest("Timezone support in datetime.fromisoformat() requires python >= 3.11")

        # Test ISO 8601 datetime strings with timezone info
        args = self.parse_timestamp("2024-10-05T14:48:55+02")
        self.assertEqual(int(datetime.fromisoformat("2024-10-05T14:48:55+02:00").timestamp()), args.time_n_ranks[0][0])

        args = self.parse_timestamp("2024-10-05T14:48:55+00:00")
        self.assertEqual(int(datetime.fromisoformat("2024-10-05T14:48:55+00:00").timestamp()), args.time_n_ranks[0][0])

        args = self.parse_timestamp("2024-10-05T14:48:55-04:30")
        self.assertEqual(int(datetime.fromisoformat("2024-10-05T14:48:55-04:30").timestamp()), args.time_n_ranks[0][0])

        args = self.parse_timestamp("2024-10-05T14:48:55+02:00")
        self.assertEqual(int(datetime.fromisoformat("2024-10-05T14:48:55+02:00").timestamp()), args.time_n_ranks[0][0])

    def test_get_include_snapshot_times(self) -> None:
        times_and_ranks_opt = "--include-snapshot-times-and-ranks="
        wildcards = ["*", "anytime"]

        args = self.argparser_parse_args(args=["src", "dst"])
        p = self.make_params(args=args)
        self.assertListEqual([[]], p.snapshot_filters)

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "*..*"])
        p = self.make_params(args=args)
        self.assertListEqual([[]], p.snapshot_filters)

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "anytime..anytime"])
        p = self.make_params(args=args)
        self.assertListEqual([[]], p.snapshot_filters)

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "notime"])
        p = self.make_params(args=args)
        self.assertEqual((0, 0), p.snapshot_filters[0][0].timerange)

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "1700000000..1700000001"])
        p = self.make_params(args=args)
        self.assertEqual((1700000000, 1700000001), p.snapshot_filters[0][0].timerange)

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "1700000001..1700000000"])
        p = self.make_params(args=args)
        self.assertEqual((1700000000, 1700000001), p.snapshot_filters[0][0].timerange)

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "0secs ago..60secs ago"])
        p = self.make_params(args=args)
        timerange = cast(Tuple[Union[timedelta, int], Union[timedelta, int]], p.snapshot_filters[0][0].timerange)
        self.assertEqual(timedelta(seconds=0), timerange[0])
        self.assertEqual(timedelta(seconds=60), timerange[1])

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "1secs ago..60secs ago"])
        p = self.make_params(args=args)
        timerange = cast(Tuple[Union[timedelta, int], Union[timedelta, int]], p.snapshot_filters[0][0].timerange)
        self.assertEqual(timedelta(seconds=1), timerange[0])
        self.assertEqual(timedelta(seconds=60), timerange[1])

        for wildcard in wildcards:
            args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "2024-01-01.." + wildcard])
            p = self.make_params(args=args)
            timerange = cast(Tuple[Union[timedelta, int], Union[timedelta, int]], p.snapshot_filters[0][0].timerange)
            self.assertEqual(int(datetime.fromisoformat("2024-01-01").timestamp()), timerange[0])
            self.assertLess(int(time.time() + 86400 * 365 * 1000), cast(int, timerange[1]))

            args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + wildcard + "..2024-01-01"])
            p = self.make_params(args=args)
            timerange = cast(Tuple[Union[timedelta, int], Union[timedelta, int]], p.snapshot_filters[0][0].timerange)
            self.assertEqual(0, timerange[0])
            self.assertEqual(int(datetime.fromisoformat("2024-01-01").timestamp()), cast(int, timerange[1]))

    def test_filter_snapshots_by_times(self) -> None:
        lst1 = ["\t" + snapshot for snapshot in ["d@0", "d#1", "d@2", "d@3"]]
        self.assertListEqual(["\td#1"], self.filter_snapshots_by_times_and_rank1(lst1, "0..0"))
        self.assertListEqual(["\td#1"], self.filter_snapshots_by_times_and_rank1(lst1, "notime"))
        self.assertListEqual(["\td@0", "\td#1"], self.filter_snapshots_by_times_and_rank1(lst1, "0..1"))
        self.assertListEqual(["\td@0", "\td#1"], self.filter_snapshots_by_times_and_rank1(lst1, "0..2"))
        self.assertListEqual(["\td@0", "\td#1", "\td@2"], self.filter_snapshots_by_times_and_rank1(lst1, "0..3"))
        self.assertListEqual(lst1, self.filter_snapshots_by_times_and_rank1(lst1, "0..4"))
        self.assertListEqual(lst1, self.filter_snapshots_by_times_and_rank1(lst1, "0..5"))
        self.assertListEqual(["\td#1", "\td@2"], self.filter_snapshots_by_times_and_rank1(lst1, "1..3"))
        self.assertListEqual(lst1, self.filter_snapshots_by_times_and_rank1(lst1, "1000 years ago..0secondsago"))
        self.assertListEqual(
            ["\td#1", "\td@2"], self.filter_snapshots_by_times_and_rank1(lst1, "1..3", loglevel=logging.INFO)
        )

    def filter_snapshots_by_times_and_rank1(
        self, snapshots: list[str], timerange: str, ranks: list[str] = [], loglevel: int = logging.DEBUG  # noqa: B006
    ) -> list[str]:
        return self.filter_snapshots_by_times_and_rank(snapshots, timerange=timerange, ranks=ranks, loglevel=loglevel)


#############################################################################
class TestRankRangeAction(CommonTest):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--ranks", nargs="+", action=bzfs_main.argparse_actions.TimeRangeAndRankRangeAction)

    def parse_args(self, arg: str) -> argparse.Namespace:
        return self.parser.parse_args(["--ranks", "0..0", arg])

    def test_valid_ranks(self) -> None:
        self.assertEqual((("latest", 0, False), ("latest", 2, True)), self.parse_args("latest0..latest2%").ranks[1])
        self.assertEqual((("oldest", 5, True), ("oldest", 9, True)), self.parse_args("oldest5%..oldest9%").ranks[1])

    def test_invalid_ranks(self) -> None:
        with self.assertRaises(SystemExit):
            self.parse_args("all except oldest0..all except oldest10")  # Range partitioning not yet implemented

        with self.assertRaises(SystemExit):
            self.parse_args("all except oldest0..oldest10")  # Range partitioning not yet implemented

        with self.assertRaises(SystemExit):
            self.parse_args("oldest0..10")  # missing kind

        with self.assertRaises(SystemExit):
            self.parse_args("oldest0oldest1")  # missing .. separator

        with self.assertRaises(SystemExit):
            self.parse_args("oldestt0..oldest1")  # misspelling

        with self.assertRaises(SystemExit):
            self.parse_args("oldest..oldest1")  # missing digits

        with self.assertRaises(SystemExit):
            self.parse_args("1..2")  # missing oldest|latest|spread

        with self.assertRaises(SystemExit):
            self.parse_args("oldest1..oldest2p")  # non-digits

        with self.assertRaises(SystemExit):
            self.parse_args("oldest1..oldestx2%")  # non-digits

        with self.assertRaises(SystemExit):
            self.parse_args("oldest1..oldest101%")  # percent > 100

        with self.assertRaises(SystemExit):
            self.parse_args("oldest1..xxxx100%")  # unknown param

    def test_ambigous_rankrange(self) -> None:
        with self.assertRaises(SystemExit):
            self.parse_args("latest0..oldest0")
        with self.assertRaises(SystemExit):
            self.parse_args("oldest0..latest0")
        with self.assertRaises(SystemExit):
            self.parse_args("oldest1..latest1")
        with self.assertRaises(SystemExit):
            self.parse_args("latest1..oldest1")
        with self.assertRaises(SystemExit):
            self.parse_args("oldest99%..latest100%")

    def filter_snapshots_by_rank(
        self, snapshots: list[str], ranks: list[str], timerange: str = "0..0", loglevel: int = logging.DEBUG
    ) -> list[str]:
        return self.filter_snapshots_by_times_and_rank(snapshots, timerange=timerange, ranks=ranks, loglevel=loglevel)

    def test_filter_snapshots_by_rank(self) -> None:
        lst1 = ["\t" + snapshot for snapshot in ["d@0", "d@1", "d@2", "d@3"]]
        self.assertListEqual([], self.filter_snapshots_by_rank(lst1, ["latest0..latest0"]))
        self.assertListEqual(["\td@3"], self.filter_snapshots_by_rank(lst1, ["latest0..latest1"]))
        self.assertListEqual(["\td@2", "\td@3"], self.filter_snapshots_by_rank(lst1, ["latest0..latest2"]))
        self.assertListEqual(["\td@1", "\td@2", "\td@3"], self.filter_snapshots_by_rank(lst1, ["latest0..latest3"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["latest0..latest4"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["latest0..latest5"]))
        self.assertListEqual(["\td@0"], self.filter_snapshots_by_rank(lst1, ["latest3..latest4"]))
        self.assertListEqual([], self.filter_snapshots_by_rank(lst1, ["latest4..latest4"]))
        self.assertListEqual(["\td@2", "\td@3"], self.filter_snapshots_by_rank(lst1, ["latest2..latest0"]))
        self.assertListEqual(
            ["\td@2", "\td@3"], self.filter_snapshots_by_rank(lst1, ["latest2..latest0"], loglevel=logging.INFO)
        )

        self.assertListEqual([], self.filter_snapshots_by_rank(lst1, ["oldest 0"]))
        self.assertListEqual([], self.filter_snapshots_by_rank(lst1, ["latest 0"]))
        self.assertListEqual(["\td@0"], self.filter_snapshots_by_rank(lst1, ["oldest 1"]))
        self.assertListEqual(["\td@3"], self.filter_snapshots_by_rank(lst1, ["latest 1"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest 4"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["latest 4"]))
        self.assertListEqual(["\td@0", "\td@1", "\td@2"], self.filter_snapshots_by_rank(lst1, ["oldest 3"]))
        self.assertListEqual(["\td@1", "\td@2", "\td@3"], self.filter_snapshots_by_rank(lst1, ["latest 3"]))
        self.assertListEqual(["\td@0", "\td@1"], self.filter_snapshots_by_rank(lst1, ["oldest 2"]))
        self.assertListEqual(["\td@2", "\td@3"], self.filter_snapshots_by_rank(lst1, ["latest 2"]))
        self.assertListEqual(["\td@0"], self.filter_snapshots_by_rank(lst1, ["oldest 25%"]))
        self.assertListEqual(["\td@3"], self.filter_snapshots_by_rank(lst1, ["latest 25%"]))
        self.assertListEqual(["\td@0", "\td@1", "\td@2"], self.filter_snapshots_by_rank(lst1, ["oldest 75%"]))
        self.assertListEqual(["\td@1", "\td@2", "\td@3"], self.filter_snapshots_by_rank(lst1, ["latest 75%"]))
        self.assertListEqual(["\td@0", "\td@1"], self.filter_snapshots_by_rank(lst1, ["oldest 50%"]))
        self.assertListEqual(["\td@2", "\td@3"], self.filter_snapshots_by_rank(lst1, ["latest 50%"]))
        self.assertListEqual(["\td@0", "\td@1"], self.filter_snapshots_by_rank(lst1, ["oldest 51%"]))
        self.assertListEqual(["\td@2", "\td@3"], self.filter_snapshots_by_rank(lst1, ["latest 51%"]))
        self.assertListEqual(["\td@0", "\td@1"], self.filter_snapshots_by_rank(lst1, ["oldest 49%"]))
        self.assertListEqual(["\td@2", "\td@3"], self.filter_snapshots_by_rank(lst1, ["latest 49%"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest 100%"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["latest 100%"]))
        self.assertListEqual([], self.filter_snapshots_by_rank(lst1, ["oldest 0%"]))
        self.assertListEqual([], self.filter_snapshots_by_rank(lst1, ["latest 0%"]))

        self.assertListEqual(["\td@0", "\td@1", "\td@2"], self.filter_snapshots_by_rank(lst1, ["latest1..latest100%"]))
        self.assertListEqual(["\td@0", "\td@1", "\td@2"], self.filter_snapshots_by_rank(lst1, ["all except latest1"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["latest0%..latest100%"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["all except latest0%"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest0%..oldest100%"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["all except oldest0%"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest100%..oldest0%"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["latest100%..latest0%"]))

        self.assertListEqual(["\td@2"], self.filter_snapshots_by_rank(lst1, ["oldest2..oldest3"]))
        self.assertListEqual(["\td@3"], self.filter_snapshots_by_rank(lst1, ["oldest3..oldest4"]))
        self.assertListEqual([], self.filter_snapshots_by_rank(lst1, ["oldest4..oldest5"]))
        self.assertListEqual([], self.filter_snapshots_by_rank(lst1, ["oldest5..oldest6"]))

        lst2 = ["\t" + snapshot for snapshot in ["d@0", "d@1", "d@2"]]
        self.assertListEqual(["\td@0", "\td@1"], self.filter_snapshots_by_rank(lst2, ["oldest 51%"]))
        self.assertListEqual(["\td@0", "\td@1"], self.filter_snapshots_by_rank(lst2, ["all except latest 49%"]))
        self.assertListEqual(["\td@0"], self.filter_snapshots_by_rank(lst2, ["oldest 49%"]))
        self.assertListEqual(["\td@0"], self.filter_snapshots_by_rank(lst2, ["all except latest 51%"]))
        self.assertListEqual(["\td@1", "\td@2"], self.filter_snapshots_by_rank(lst2, ["latest 51%"]))
        self.assertListEqual(["\td@1", "\td@2"], self.filter_snapshots_by_rank(lst2, ["all except oldest 49%"]))
        self.assertListEqual(["\td@2"], self.filter_snapshots_by_rank(lst2, ["latest 49%"]))
        self.assertListEqual(["\td@2"], self.filter_snapshots_by_rank(lst2, ["all except oldest 51%"]))
        self.assertListEqual([], self.filter_snapshots_by_rank(lst2, ["latest 0%"]))
        self.assertListEqual(lst2, self.filter_snapshots_by_rank(lst2, ["latest 100%"]))
        self.assertListEqual([], self.filter_snapshots_by_rank(lst2, ["all except oldest 100%"]))

        self.assertListEqual(["\td@0"], self.filter_snapshots_by_rank(lst1, ["oldest0..oldest1"]))
        self.assertListEqual(["\td@0", "\td@1"], self.filter_snapshots_by_rank(lst2, ["latest100%..latest1"]))

    def test_filter_snapshots_by_rank_with_bookmarks(self) -> None:
        lst1 = ["\t" + snapshot for snapshot in ["d@0", "d#1", "d@2", "d@3"]]
        self.assertListEqual(["\td#1"], self.filter_snapshots_by_rank(lst1, ["oldest0..oldest0"]))
        self.assertListEqual(["\td@0", "\td#1"], self.filter_snapshots_by_rank(lst1, ["oldest0..oldest1"]))
        self.assertListEqual(["\td@0", "\td#1", "\td@2"], self.filter_snapshots_by_rank(lst1, ["oldest0..oldest2"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest0..oldest3"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest0..oldest4"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest0..oldest5"]))
        self.assertListEqual(["\td@0", "\td#1", "\td@2"], self.filter_snapshots_by_rank(lst1, ["latest1..latest100%"]))

    def test_filter_snapshots_by_rank_with_chain(self) -> None:
        lst1 = ["\t" + snapshot for snapshot in ["d@0", "d#1", "d@2", "d@3"]]
        results = self.filter_snapshots_by_rank(lst1, ["latest1..latest100%", "latest1..latest100%"])
        self.assertListEqual(["\td@0", "\td#1"], results)

        results = self.filter_snapshots_by_rank(lst1, ["latest1..latest100%", "oldest1..oldest100%"])
        self.assertListEqual(["\td#1", "\td@2"], results)

    def test_filter_snapshots_by_rank_with_times(self) -> None:
        lst1 = ["\t" + snapshot for snapshot in ["d@0", "d#1", "d@2", "d@3"]]
        self.assertListEqual(["\td@0", "\td#1"], self.filter_snapshots_by_rank(lst1, ["oldest 1"], timerange="0..0"))
        self.assertListEqual(["\td@0", "\td#1"], self.filter_snapshots_by_rank(lst1, ["oldest 1"], timerange="notime"))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest 1"], timerange="0..11"))
        results = self.filter_snapshots_by_rank(lst1, ["oldest 1"], timerange="3..11")
        self.assertListEqual(["\td@0", "\td#1", "\td@3"], results)
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest 1"], timerange="2..11"))
        results = self.filter_snapshots_by_rank(lst1, ["oldest1..oldest2", "latest 1"], timerange="3..11")
        self.assertListEqual(["\td#1", "\td@2", "\td@3"], results)

        lst1 = ["\t" + snapshot for snapshot in ["d@0", "d#1", "d@2", "d@3", "d@4"]]
        results = self.filter_snapshots_by_rank(lst1, ["oldest1..oldest2", "latest 1"], timerange="4..11")
        self.assertListEqual(["\td#1", "\td@2", "\td@4"], results)

    def get_snapshot_filters(self, cli: list[str]) -> Any:
        args = self.argparser_parse_args(args=["src", "dst", *cli])
        return self.make_params(args=args).snapshot_filters[0]

    def test_merge_adjacent_snapshot_regexes_and_filters0(self) -> None:
        ranks = "--include-snapshot-times-and-ranks"
        cli = [ranks, "0..1", "oldest 50%", ranks, "0..0", "oldest 10%"]
        ranks_filter = self.get_snapshot_filters(cli)
        self.assertEqual((0, 1), ranks_filter[0].timerange)
        self.assertEqual([(("oldest", 0, False), ("oldest", 50, True))], ranks_filter[0].options)
        self.assertEqual((0, 0), ranks_filter[1].timerange)
        self.assertEqual([(("oldest", 0, False), ("oldest", 10, True))], ranks_filter[1].options)

    def test_merge_adjacent_snapshot_regexes_and_filters1(self) -> None:
        ranks = "--include-snapshot-times-and-ranks"
        cli = [ranks, "0..0", "oldest 50%", ranks, "0..0", "oldest 10%"]
        ranks_filter = self.get_snapshot_filters(cli)[0]
        self.assertEqual((0, 0), ranks_filter.timerange)
        self.assertEqual(
            [
                (("oldest", 0, False), ("oldest", 50, True)),
                (("oldest", 0, False), ("oldest", 10, True)),
            ],
            ranks_filter.options,
        )

    def test_merge_adjacent_snapshot_regexes_and_filters2(self) -> None:
        ranks = "--include-snapshot-times-and-ranks"
        cli = [ranks, "0..0", "oldest 50%", ranks, "0..0", "oldest 10%", ranks, "0..0", "oldest 20%"]
        ranks_filter = self.get_snapshot_filters(cli)[0]
        self.assertEqual((0, 0), ranks_filter.timerange)
        self.assertEqual(
            [
                (("oldest", 0, False), ("oldest", 50, True)),
                (("oldest", 0, False), ("oldest", 10, True)),
                (("oldest", 0, False), ("oldest", 20, True)),
            ],
            ranks_filter.options,
        )

    def test_merge_adjacent_snapshot_regexes_and_filters3(self) -> None:
        include = "--include-snapshot-regex"
        exclude = "--exclude-snapshot-regex"
        times = "--include-snapshot-times-and-ranks"
        cli = [times, "*..*", times, "0..9", include, "f", include, "d", exclude, "w", include, "h", exclude, "m"]
        times_filter, regex_filter = self.get_snapshot_filters(cli)
        self.assertEqual("include_snapshot_times", times_filter.name)
        self.assertEqual((0, 9), times_filter.timerange)
        self.assertEqual(SNAPSHOT_REGEX_FILTER_NAME, regex_filter.name)
        self.assertEqual((["w", "m"], ["f", "d", "h"]), regex_filter.options)

    def test_merge_adjacent_snapshot_regexes_doesnt_merge_across_groups(self) -> None:
        include = "--include-snapshot-regex"
        exclude = "--exclude-snapshot-regex"
        ranks = "--include-snapshot-times-and-ranks"
        cli = [include, ".*daily", exclude, ".*weekly", include, ".*hourly", ranks, "0..0", "oldest 5%", exclude, ".*m"]
        regex_filter1, ranks_filter, regex_filter2 = self.get_snapshot_filters(cli)
        self.assertEqual(SNAPSHOT_REGEX_FILTER_NAME, regex_filter1.name)
        self.assertEqual(([".*weekly"], [".*daily", ".*hourly"]), regex_filter1.options)
        self.assertEqual((0, 0), ranks_filter.timerange)
        self.assertEqual("include_snapshot_times_and_ranks", ranks_filter.name)
        self.assertEqual((("oldest", 0, False), ("oldest", 5, True)), ranks_filter.options[0])
        self.assertEqual(SNAPSHOT_REGEX_FILTER_NAME, regex_filter2.name)
        self.assertEqual(([".*m"], []), regex_filter2.options)

    def test_reorder_snapshot_times_simple(self) -> None:
        include = "--include-snapshot-regex"
        times = "--include-snapshot-times-and-ranks"
        cli = [include, ".*daily", times, "0..9"]
        times_filter, regex_filter = self.get_snapshot_filters(cli)
        self.assertEqual("include_snapshot_times", times_filter.name)
        self.assertEqual((0, 9), times_filter.timerange)
        self.assertEqual(SNAPSHOT_REGEX_FILTER_NAME, regex_filter.name)
        self.assertEqual(([], [".*daily"]), regex_filter.options)

    def test_reorder_snapshot_times_complex(self) -> None:
        include = "--include-snapshot-regex"
        exclude = "--exclude-snapshot-regex"
        times = "--include-snapshot-times-and-ranks"
        ranks = "--include-snapshot-times-and-ranks"
        cli = [include, ".*daily", exclude, ".*weekly", include, ".*hourly", times, "0..9", ranks, "0..0", "oldest1"]
        times_filter, regex_filter, ranks_filter = self.get_snapshot_filters(cli)
        self.assertEqual("include_snapshot_times", times_filter.name)
        self.assertEqual((0, 9), times_filter.timerange)
        self.assertEqual(SNAPSHOT_REGEX_FILTER_NAME, regex_filter.name)
        self.assertEqual(([".*weekly"], [".*daily", ".*hourly"]), regex_filter.options)
        self.assertEqual("include_snapshot_times_and_ranks", ranks_filter.name)
        self.assertEqual((("oldest", 0, False), ("oldest", 1, False)), ranks_filter.options[0])
        self.assertEqual((0, 0), ranks_filter.timerange)


#############################################################################
class TestFilterSnapshotsWithBookmarks(CommonTest):

    def setUp(self) -> None:
        args = self.argparser_parse_args(args=["src", "dst"])
        self.job = bzfs.Job()
        self.job.params = self.make_params(args=args)
        self.job.params.log.setLevel(logging.INFO)  # Set to INFO to avoid verbose output during tests

        self.basis_snapshots = [
            f"{int(unixtime_fromisoformat('2024-01-01'))}\tguid1\tds@daily_2024-01-01",
            f"{int(unixtime_fromisoformat('2024-01-02'))}\tguid2\tds#daily_2024-01-02",  # Bookmark
            f"{int(unixtime_fromisoformat('2024-01-03'))}\tguid3\tds@hourly_2024-01-03",
            f"{int(unixtime_fromisoformat('2024-01-04'))}\tguid4\tds#hourly_2024-01-04",  # Bookmark
            f"{int(unixtime_fromisoformat('2024-01-05'))}\tguid5\tds@keep_this_one",
        ]
        self.daily_snap = self.basis_snapshots[0]
        self.daily_bookmark = self.basis_snapshots[1]
        self.hourly_snap = self.basis_snapshots[2]
        self.hourly_bookmark = self.basis_snapshots[3]
        self.keep_snap = self.basis_snapshots[4]

    def test_filter_bookmarks_false_preserves_all_bookmarks(self) -> None:
        """Verify default behavior (filter_bookmarks=False): all bookmarks are unconditionally preserved, while snapshots are
        correctly filtered."""
        # A filter that should match only the 'keep_this_one' snapshot.
        regexes = (compile_regexes([]), compile_regexes([".*keep_this_one"]))
        self.job.params.snapshot_filters = [[SnapshotFilter(SNAPSHOT_REGEX_FILTER_NAME, None, regexes)]]

        result = filter_snapshots(self.job, self.basis_snapshots, filter_bookmarks=False)

        # The final list must contain ALL original bookmarks plus ONLY the filtered snapshots.
        self.assertIn(self.daily_bookmark, result, "Bookmarks must be preserved by default")
        self.assertIn(self.hourly_bookmark, result, "Bookmarks must be preserved by default")
        self.assertIn(self.keep_snap, result, "Matching snapshot should be present")

        # Snapshots that did not match the filter must be excluded.
        self.assertNotIn(self.daily_snap, result, "Non-matching snapshots should be filtered out")
        self.assertNotIn(self.hourly_snap, result, "Non-matching snapshots should be filtered out")
        self.assertEqual(3, len(result))

    def test_filter_bookmarks_true_with_regex(self) -> None:
        """Verify filter_bookmarks=True: regex filters apply to both bookmarks and snapshots."""
        # Filter for items containing 'daily'.
        regexes = (compile_regexes([]), compile_regexes([".*daily.*"]))
        self.job.params.snapshot_filters = [[SnapshotFilter(SNAPSHOT_REGEX_FILTER_NAME, None, regexes)]]

        result = filter_snapshots(self.job, self.basis_snapshots, filter_bookmarks=True)

        self.assertIn(self.daily_snap, result)
        self.assertIn(self.daily_bookmark, result)
        self.assertNotIn(self.hourly_snap, result)
        self.assertNotIn(self.hourly_bookmark, result)
        self.assertNotIn(self.keep_snap, result)
        self.assertEqual(2, len(result))

    def test_filter_bookmarks_true_with_time_range(self) -> None:
        """Verify filter_bookmarks=True: time filters apply to both bookmarks and snapshots."""
        # Filter for everything on or after 2024-01-03.
        timerange = (int(unixtime_fromisoformat("2024-01-03")), UNIX_TIME_INFINITY_SECS)
        self.job.params.snapshot_filters = [[SnapshotFilter("include_snapshot_times", timerange, None)]]

        result = filter_snapshots(self.job, self.basis_snapshots, filter_bookmarks=True)

        self.assertNotIn(self.daily_snap, result)
        self.assertNotIn(self.daily_bookmark, result)
        self.assertIn(self.hourly_snap, result)
        self.assertIn(self.hourly_bookmark, result)
        self.assertIn(self.keep_snap, result)
        self.assertEqual(3, len(result))

    def test_filter_bookmarks_true_with_rank(self) -> None:
        """Verify filter_bookmarks=True: rank and time filters are correctly unioned; The rank filter applies only to
        snapshots, while the time filter applies to all items."""
        # Rank filter for the latest 2 snapshots. Time range includes everything.
        ranks = [(("latest", 0, False), ("latest", 2, False))]
        timerange = (0, UNIX_TIME_INFINITY_SECS)
        self.job.params.snapshot_filters = [[SnapshotFilter("include_snapshot_times_and_ranks", timerange, ranks)]]

        result = filter_snapshots(self.job, self.basis_snapshots, filter_bookmarks=True)

        # The filter logic is a UNION:
        # 1. Rank filter selects the latest 2 snapshots: `hourly_snap` and `keep_snap`.
        # 2. Time filter selects all items (snapshots and bookmarks).
        # The union of these two sets is all items.
        self.assertIn(self.hourly_snap, result)
        self.assertIn(self.keep_snap, result)
        self.assertIn(self.daily_bookmark, result)
        self.assertIn(self.hourly_bookmark, result)
        self.assertIn(self.daily_snap, result)
        self.assertEqual(5, len(result), "The union of 'latest 2' and 'all time' should be all items")

    def test_filter_bookmarks_true_with_all_except(self) -> None:
        """Verify interaction with all_except=True when filtering bookmarks."""
        # This filter selects daily snapshots and bookmarks to be RETAINED.
        regexes = (compile_regexes([]), compile_regexes([".*daily.*"]))
        self.job.params.snapshot_filters = [[SnapshotFilter(SNAPSHOT_REGEX_FILTER_NAME, None, regexes)]]

        # `all_except=True` inverts the selection, so we get everything EXCEPT the daily items.
        result = filter_snapshots(self.job, self.basis_snapshots, all_except=True, filter_bookmarks=True)

        self.assertNotIn(self.daily_snap, result)
        self.assertNotIn(self.daily_bookmark, result)
        self.assertIn(self.hourly_snap, result)
        self.assertIn(self.hourly_bookmark, result)
        self.assertIn(self.keep_snap, result)
        self.assertEqual(3, len(result))


#############################################################################
class TestFilterDatasets(CommonTest):
    def make_job(
        self,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
        skip_parent: bool = False,
        exclude_property: str | None = None,
        debug: bool = False,
        test_mode: bool = False,
    ) -> tuple[Job, Remote]:
        args = self.argparser_parse_args(["src", "dst"])
        log = logging.getLogger(f"datasets_{id(self)}_{debug}")
        log.setLevel(LOG_DEBUG if debug else logging.INFO)
        job = bzfs.Job()
        job.params = self.make_params(args=args, log=log)
        job.is_test_mode = test_mode
        p = job.params
        p.include_dataset_regexes = compile_regexes(include or [".*"])
        p.exclude_dataset_regexes = compile_regexes(exclude or [])
        p.skip_parent = skip_parent
        p.exclude_dataset_property = exclude_property
        remote = MagicMock(spec=Remote, root_dataset="src", location="src")
        return job, remote

    def test_include_all(self) -> None:
        job, remote = self.make_job()
        datasets = ["src/a", "src/b"]
        self.assertListEqual(datasets, filter_datasets(job, remote, datasets))

    def test_exclude_matching(self) -> None:
        job, remote = self.make_job(exclude=["foo"])
        datasets = ["src/foo", "src/bar"]
        self.assertListEqual(["src/bar"], filter_datasets(job, remote, datasets))

    def test_include_specific(self) -> None:
        job, remote = self.make_job(include=["bar$"])
        datasets = ["src/bar", "src/foo"]
        self.assertListEqual(["src/bar"], filter_datasets(job, remote, datasets))

    def test_skip_parent(self) -> None:
        job, remote = self.make_job(skip_parent=True)
        datasets = ["src/a", "src/b"]
        self.assertListEqual(["src/b"], filter_datasets(job, remote, datasets))

    def test_property_filter_called(self) -> None:
        job, remote = self.make_job(exclude_property="skip")
        with patch("bzfs_main.filter._filter_datasets_by_exclude_property", return_value=["src/a"]) as m:
            result = filter_datasets(job, remote, ["src/a", "src/b"])
            m.assert_called_once()
            self.assertEqual(["src/a"], result)

    def test_debug_logging_off(self) -> None:
        job, remote = self.make_job(debug=False)
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        job.params.log.addHandler(handler)
        filter_datasets(job, remote, ["src/a"])
        self.assertNotIn("Finally included", stream.getvalue())

    def test_debug_logging_on(self) -> None:
        job, remote = self.make_job(debug=True)
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        job.params.log.addHandler(handler)
        filter_datasets(job, remote, ["src/a"])
        self.assertIn("Finally included", stream.getvalue())

    def test_test_mode_asserts(self) -> None:
        job, remote = self.make_job(test_mode=True)
        datasets = ["src/a", "src/a/b", "src/c"]
        self.assertListEqual(datasets, filter_datasets(job, remote, datasets))

    def test_relative_dataset_strip(self) -> None:
        job, remote = self.make_job(include=["a/b"])
        datasets = ["src/a/b"]
        self.assertListEqual(["src/a/b"], filter_datasets(job, remote, datasets))

    def test_dataset_not_included(self) -> None:
        job, remote = self.make_job(include=["foo$"])
        self.assertEqual([], filter_datasets(job, remote, ["src/bar"]))


#############################################################################
class TestFilterDatasetsByExcludeProperty(CommonTest):
    def make_job(self, debug: bool = False) -> tuple[Job, Remote]:
        args = self.argparser_parse_args(["src", "dst", "--exclude-dataset-property", "skip"])
        log = logging.getLogger(f"prop_{id(self)}_{debug}")
        log.setLevel(LOG_DEBUG if debug else logging.INFO)
        job = bzfs.Job()
        job.params = self.make_params(args=args, log=log)
        remote = MagicMock(spec=Remote, location="src")
        return job, remote

    def run_filter(self, mapping: dict[str, str | None], debug: bool = False) -> list[str]:
        job, remote = self.make_job(debug=debug)

        def fake_try(job_: Job, remote_: Remote, level: int, cmd: list[str]) -> str | None:
            return mapping.get(cmd[-1])

        with patch("bzfs_main.filter.try_ssh_command", side_effect=fake_try):
            with patch.object(job, "maybe_inject_delete"):
                with patch("socket.gethostname", return_value="host1"):
                    result = _filter_datasets_by_exclude_property(job, remote, list(mapping.keys()))
        return result

    def test_include_empty_value(self) -> None:
        mapping: dict[str, str | None] = {"a": "", "b": "-", "c": "true"}
        self.assertListEqual(["a", "b", "c"], self.run_filter(mapping))

    def test_exclude_false(self) -> None:
        mapping: dict[str, str | None] = {"a": "false", "b": "true"}
        self.assertListEqual(["b"], self.run_filter(mapping))

    def test_include_host_match(self) -> None:
        mapping: dict[str, str | None] = {"a": "host1"}
        self.assertListEqual(["a"], self.run_filter(mapping))

    def test_exclude_host_mismatch(self) -> None:
        mapping: dict[str, str | None] = {"a": "host2"}
        self.assertEqual([], self.run_filter(mapping))

    def test_host_value_with_spaces(self) -> None:
        mapping: dict[str, str | None] = {"a": "host1, host2"}
        self.assertListEqual(["a"], self.run_filter(mapping))

    def test_skip_descendant_after_exclude(self) -> None:
        job, remote = self.make_job()
        mapping: dict[str, str | None] = {"a": "false", "a/b": "true", "c": "true"}

        def fake_try(job_: Job, remote_: Remote, level: int, cmd: list[str]) -> str | None:
            return mapping.get(cmd[-1])

        with patch("bzfs_main.filter.try_ssh_command", side_effect=fake_try) as mock_try:
            with patch.object(job, "maybe_inject_delete"):
                with patch("socket.gethostname", return_value="host1"):
                    result = _filter_datasets_by_exclude_property(job, remote, ["a", "a/b", "c"])
        self.assertListEqual(["c"], result)
        self.assertEqual(2, mock_try.call_count)

    def test_property_none(self) -> None:
        job, remote = self.make_job()
        mapping: dict[str, str | None] = {"a": None, "b": "true"}

        def fake_try(job_: Job, remote_: Remote, level: int, cmd: list[str]) -> str | None:
            return mapping.get(cmd[-1])

        with patch("bzfs_main.filter.try_ssh_command", side_effect=fake_try):
            with patch.object(job, "maybe_inject_delete"):
                with patch("socket.gethostname", return_value="host1"):
                    result = _filter_datasets_by_exclude_property(job, remote, ["a", "b"])
        self.assertListEqual(["b"], result)

    def test_debug_logging(self) -> None:
        mapping: dict[str, str | None] = {"a": "host2"}
        job, remote = self.make_job(debug=True)
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        job.params.log.addHandler(handler)

        def fake_try(job_: Job, remote_: Remote, level: int, cmd: list[str]) -> str | None:
            return mapping.get(cmd[-1])

        with patch("bzfs_main.filter.try_ssh_command", side_effect=fake_try):
            with patch.object(job, "maybe_inject_delete"):
                with patch("socket.gethostname", return_value="host1"):
                    _filter_datasets_by_exclude_property(job, remote, ["a"])
        log_output = stream.getvalue()
        self.assertIn("Excluding b/c dataset prop", log_output)

    def test_case_insensitive_values(self) -> None:
        mapping: dict[str, str | None] = {"a": "TrUe", "b": "FaLsE"}
        self.assertListEqual(["a"], self.run_filter(mapping))
