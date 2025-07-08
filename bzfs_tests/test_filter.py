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
)

from bzfs_main import bzfs
from bzfs_main.bzfs import (
    Job,
    Params,
    Remote,
)
from bzfs_main.filter import (
    dataset_regexes,
    filter_datasets,
    filter_lines,
    filter_lines_except,
    filter_properties,
    filter_snapshots,
    filter_snapshots_by_regex,
    snapshot_regex_filter_name,
)
from bzfs_main.loggers import get_logger, reset_logger
from bzfs_main.utils import (
    compile_regexes,
    log_debug,
)
from bzfs_tests.abstract_testcase import AbstractTestCase


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestHelperFunctions,
        TestTimeRangeAction,
        TestRankRangeAction,
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
        log_params = bzfs.LogParams(args)
        try:
            job = bzfs.Job()
            job.params = Params(args, log_params=log_params, log=get_logger(log_params, args))
            job.params.log.setLevel(loglevel)
            snapshots = [f"{i}\t" + snapshot for i, snapshot in enumerate(snapshots)]  # simulate creation time
            results = filter_snapshots(job, snapshots)
            results = [result.split("\t", 1)[1] for result in results]  # drop creation time
            return results
        finally:
            reset_logger()


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
        p = Params(args)
        log_params = bzfs.LogParams(args)
        try:
            job = bzfs.Job()
            job.params = Params(args, log_params=log_params, log=get_logger(log_params, args))
            src = bzfs.Remote("src", args, p)
            filter_datasets(job, src, ["dataset1"])
        finally:
            reset_logger()

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
        self.assertEqual(["\tds@a1", "\tds@b2"], filter_snapshots_by_regex(job, snapshots, regexes))

    def test_filter_snapshots_by_regex_debug(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        log = logging.getLogger("debug_snapshots")
        log.setLevel(log_debug)
        stream = io.StringIO()
        log.addHandler(logging.StreamHandler(stream))
        job = bzfs.Job()
        job.params = Params(args, log=log)
        snapshots = ["\tds@a1", "\tds@b2", "\tds@c3", "\tds@other", "\tds#bookmark"]
        regexes = (
            compile_regexes(["c3"]),
            compile_regexes(["a1", "c3"]),
        )
        result = filter_snapshots_by_regex(job, snapshots, regexes)
        self.assertEqual(["\tds@a1"], result)
        log_output = stream.getvalue()
        self.assertIn("Including b/c snapshot regex", log_output)
        self.assertIn("Excluding b/c snapshot regex", log_output)

    def test_filter_properties(self) -> None:
        job = MagicMock(spec=Job, params=MagicMock(spec=Params, log=MagicMock()))
        props: dict[str, str | None] = {"p1": "v1", "skip": "v", "p3": "v3"}
        include_regexes = compile_regexes(["p.*"])
        exclude_regexes = compile_regexes([".*3"])
        self.assertEqual({"p1": "v1"}, filter_properties(job, props, include_regexes, exclude_regexes))

    def test_filter_properties_debug(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        log = logging.getLogger("debug_props")
        log.setLevel(log_debug)
        stream = io.StringIO()
        log.addHandler(logging.StreamHandler(stream))
        job = bzfs.Job()
        job.params = Params(args, log=log)
        props: dict[str, str | None] = {"a1": "v1", "a2": "v2", "skip": "v"}
        include_regexes = compile_regexes(["a.*"])
        exclude_regexes = compile_regexes(["a2"])
        result = filter_properties(job, props, include_regexes, exclude_regexes)
        self.assertEqual({"a1": "v1"}, result)
        log_output = stream.getvalue()
        self.assertIn("Including b/c property regex", log_output)
        self.assertIn("Excluding b/c property regex", log_output)


#############################################################################
class TestTimeRangeAction(CommonTest):
    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--time-n-ranks", action=bzfs.TimeRangeAndRankRangeAction, nargs="+")

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
        p = Params(args)
        self.assertListEqual([[]], p.snapshot_filters)

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "*..*"])
        p = Params(args)
        self.assertListEqual([[]], p.snapshot_filters)

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "anytime..anytime"])
        p = Params(args)
        self.assertListEqual([[]], p.snapshot_filters)

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "notime"])
        p = Params(args)
        self.assertEqual((0, 0), p.snapshot_filters[0][0].timerange)

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "1700000000..1700000001"])
        p = Params(args)
        self.assertEqual((1700000000, 1700000001), p.snapshot_filters[0][0].timerange)

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "1700000001..1700000000"])
        p = Params(args)
        self.assertEqual((1700000000, 1700000001), p.snapshot_filters[0][0].timerange)

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "0secs ago..60secs ago"])
        p = Params(args)
        timerange = cast(Tuple[Union[timedelta, int], Union[timedelta, int]], p.snapshot_filters[0][0].timerange)
        self.assertEqual(timedelta(seconds=0), timerange[0])
        self.assertEqual(timedelta(seconds=60), timerange[1])

        args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "1secs ago..60secs ago"])
        p = Params(args)
        timerange = cast(Tuple[Union[timedelta, int], Union[timedelta, int]], p.snapshot_filters[0][0].timerange)
        self.assertEqual(timedelta(seconds=1), timerange[0])
        self.assertEqual(timedelta(seconds=60), timerange[1])

        for wildcard in wildcards:
            args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "2024-01-01.." + wildcard])
            p = Params(args)
            timerange = cast(Tuple[Union[timedelta, int], Union[timedelta, int]], p.snapshot_filters[0][0].timerange)
            self.assertEqual(int(datetime.fromisoformat("2024-01-01").timestamp()), timerange[0])
            self.assertLess(int(time.time() + 86400 * 365 * 1000), cast(int, timerange[1]))

            args = self.argparser_parse_args(args=["src", "dst", times_and_ranks_opt + wildcard + "..2024-01-01"])
            p = Params(args)
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
        self.parser.add_argument("--ranks", nargs="+", action=bzfs.TimeRangeAndRankRangeAction)

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
        return Params(args).snapshot_filters[0]

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
        self.assertEqual(snapshot_regex_filter_name, regex_filter.name)
        self.assertEqual((["w", "m"], ["f", "d", "h"]), regex_filter.options)

    def test_merge_adjacent_snapshot_regexes_doesnt_merge_across_groups(self) -> None:
        include = "--include-snapshot-regex"
        exclude = "--exclude-snapshot-regex"
        ranks = "--include-snapshot-times-and-ranks"
        cli = [include, ".*daily", exclude, ".*weekly", include, ".*hourly", ranks, "0..0", "oldest 5%", exclude, ".*m"]
        regex_filter1, ranks_filter, regex_filter2 = self.get_snapshot_filters(cli)
        self.assertEqual(snapshot_regex_filter_name, regex_filter1.name)
        self.assertEqual(([".*weekly"], [".*daily", ".*hourly"]), regex_filter1.options)
        self.assertEqual((0, 0), ranks_filter.timerange)
        self.assertEqual("include_snapshot_times_and_ranks", ranks_filter.name)
        self.assertEqual((("oldest", 0, False), ("oldest", 5, True)), ranks_filter.options[0])
        self.assertEqual(snapshot_regex_filter_name, regex_filter2.name)
        self.assertEqual(([".*m"], []), regex_filter2.options)

    def test_reorder_snapshot_times_simple(self) -> None:
        include = "--include-snapshot-regex"
        times = "--include-snapshot-times-and-ranks"
        cli = [include, ".*daily", times, "0..9"]
        times_filter, regex_filter = self.get_snapshot_filters(cli)
        self.assertEqual("include_snapshot_times", times_filter.name)
        self.assertEqual((0, 9), times_filter.timerange)
        self.assertEqual(snapshot_regex_filter_name, regex_filter.name)
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
        self.assertEqual(snapshot_regex_filter_name, regex_filter.name)
        self.assertEqual(([".*weekly"], [".*daily", ".*hourly"]), regex_filter.options)
        self.assertEqual("include_snapshot_times_and_ranks", ranks_filter.name)
        self.assertEqual((("oldest", 0, False), ("oldest", 1, False)), ranks_filter.options[0])
        self.assertEqual((0, 0), ranks_filter.timerange)
