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
"""Unit tests for various small utility helpers."""

from __future__ import annotations
import argparse
import contextlib
import errno
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone, tzinfo
from logging import Logger
from subprocess import PIPE
from typing import (
    Any,
    Callable,
    Iterator,
    Sequence,
    Union,
    cast,
)
from unittest import mock
from unittest.mock import (
    MagicMock,
    patch,
)

from bzfs_main.utils import (
    DESCENDANTS_RE_SUFFIX,
    SmallPriorityQueue,
    SnapshotPeriods,
    SynchronizedBool,
    SynchronizedDict,
    append_if_absent,
    compile_regexes,
    current_datetime,
    cut,
    die,
    drain,
    find_match,
    format_dict,
    get_descendant_processes,
    get_home_directory,
    get_timezone,
    has_duplicates,
    human_readable_bytes,
    human_readable_duration,
    human_readable_float,
    is_descendant,
    isotime_from_unixtime,
    open_nofollow,
    parse_duration_to_milliseconds,
    percent,
    pid_exists,
    pretty_print_formatter,
    replace_capturing_groups_with_non_capturing_groups,
    shuffle_dict,
    sorted_dict,
    subprocess_run,
    tail,
    terminate_process_subtree,
    unixtime_fromisoformat,
    xfinally,
    xprint,
)
from bzfs_tests.abstract_testcase import AbstractTestCase


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestHelperFunctions,
        TestCut,
        TestDrain,
        TestShuffleDict,
        TestSortedDict,
        TestTail,
        TestGetHomeDirectory,
        TestHumanReadable,
        TestOpenNoFollow,
        TestFindMatch,
        TestSubprocessRun,
        TestPIDExists,
        TestTerminateProcessSubtree,
        TestSmallPriorityQueue,
        TestSynchronizedBool,
        TestSynchronizedDict,
        TestSnapshotPeriods,
        TestXFinally,
        TestCurrentDateTime,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestHelperFunctions(AbstractTestCase):
    def test_die_with_parser(self) -> None:
        parser = argparse.ArgumentParser()
        with self.assertRaises(SystemExit):
            die("boom", parser=parser)

    def test_has_duplicates(self) -> None:
        self.assertFalse(has_duplicates([]))
        self.assertFalse(has_duplicates([42]))
        self.assertFalse(has_duplicates([1, 2, 3, 4, 5]))
        self.assertTrue(has_duplicates([2, 2, 3, 4, 5]))
        self.assertTrue(has_duplicates([1, 2, 3, 3, 4, 5]))
        self.assertTrue(has_duplicates([1, 2, 3, 4, 5, 5]))
        self.assertTrue(has_duplicates([1, 1, 2, 3, 3, 4, 4, 5]))
        self.assertTrue(has_duplicates(["a", "b", "b", "c"]))
        self.assertFalse(has_duplicates(["ant", "bee", "cat"]))

    def test_is_descendant(self) -> None:
        self.assertTrue(is_descendant("pool/fs/child", "pool/fs"))
        self.assertTrue(is_descendant("pool/fs/child/grandchild", "pool/fs"))
        self.assertTrue(is_descendant("a/b/c/d", "a/b"))
        self.assertTrue(is_descendant("pool/fs", "pool/fs"))
        self.assertFalse(is_descendant("pool/otherfs", "pool/fs"))
        self.assertFalse(is_descendant("a/c", "a/b"))
        self.assertFalse(is_descendant("pool/fs", "pool/fs/child"))
        self.assertFalse(is_descendant("a/b", "a/b/c"))
        self.assertFalse(is_descendant("tank1/data", "tank2/backup"))
        self.assertFalse(is_descendant("a", ""))
        self.assertFalse(is_descendant("", "a"))
        self.assertTrue(is_descendant("", ""))
        self.assertFalse(is_descendant("pool/fs-backup", "pool/fs"))
        self.assertTrue(is_descendant("pool/fs", "pool"))

    def test_append_if_absent(self) -> None:
        self.assertListEqual([], append_if_absent([]))
        self.assertListEqual(["a"], append_if_absent([], "a"))
        self.assertListEqual(["a"], append_if_absent([], "a", "a"))

    def test_compile_regexes(self) -> None:
        def _assert_full_match(text: str, regex: str, re_suffix: str = "", expected: bool = True) -> None:
            match = compile_regexes([regex], suffix=re_suffix)[0][0].fullmatch(text)
            if expected:
                self.assertTrue(match)
            else:
                self.assertFalse(match)

        def assert_full_match(text: str, regex: str, re_suffix: str = "") -> None:
            _assert_full_match(text=text, regex=regex, re_suffix=re_suffix, expected=True)

        def assert_not_full_match(text: str, regex: str, re_suffix: str = "") -> None:
            _assert_full_match(text=text, regex=regex, re_suffix=re_suffix, expected=False)

        re_suffix = DESCENDANTS_RE_SUFFIX
        assert_full_match("foo", "foo")
        assert_not_full_match("xfoo", "foo")
        assert_not_full_match("fooy", "foo")
        assert_not_full_match("foo/bar", "foo")
        assert_full_match("foo", "foo$")
        assert_full_match("foo", ".*")
        assert_full_match("foo/bar", ".*")
        assert_full_match("foo", ".*", re_suffix)
        assert_full_match("foo/bar", ".*", re_suffix)
        assert_full_match("foo", "foo", re_suffix)
        assert_full_match("foo/bar", "foo", re_suffix)
        assert_full_match("foo/bar/baz", "foo", re_suffix)
        assert_full_match("foo", "foo$", re_suffix)
        assert_full_match("foo$", "foo\\$", re_suffix)
        assert_full_match("foo", "!foo", re_suffix)
        assert_full_match("foo", "!foo")
        with self.assertRaises(re.error):
            compile_regexes(["fo$o"], re_suffix)

    def test_parse_duration_to_milliseconds(self) -> None:
        self.assertEqual(5000, parse_duration_to_milliseconds("5 seconds"))
        self.assertEqual(
            300000,
            parse_duration_to_milliseconds("5 minutes ago", regex_suffix=r"\s*ago"),
        )
        with self.assertRaises(ValueError):
            parse_duration_to_milliseconds("foo")
        with self.assertRaises(SystemExit):
            parse_duration_to_milliseconds("foo", context="ctx")

    def test_format_dict(self) -> None:
        self.assertEqual("\"{'a': 1}\"", format_dict({"a": 1}))

    def test_pretty_print_formatter(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        params = self.make_params(args=args)
        self.assertIsNotNone(str(pretty_print_formatter(params)))

    def test_xprint(self) -> None:
        log = MagicMock(spec=Logger)
        xprint(log, "foo")
        xprint(log, "foo", run=True)
        xprint(log, "foo", run=False)
        xprint(log, "foo", file=sys.stdout)
        xprint(log, "")
        xprint(log, "", run=True)
        xprint(log, "", run=False)
        xprint(log, None)


###############################################################################
class TestSnapshotPeriods(AbstractTestCase):

    def test_label_milliseconds(self) -> None:
        xperiods = SnapshotPeriods()
        self.assertEqual(xperiods.suffix_milliseconds["yearly"] * 1, xperiods.label_milliseconds("foo_yearly"))
        self.assertEqual(xperiods.suffix_milliseconds["yearly"] * 2, xperiods.label_milliseconds("foo_2yearly"))
        self.assertEqual(xperiods.suffix_milliseconds["yearly"] * 2, xperiods.label_milliseconds("_2yearly"))
        self.assertEqual(0, xperiods.label_milliseconds("_0yearly"))
        self.assertEqual(0, xperiods.label_milliseconds("2yearly"))
        self.assertEqual(0, xperiods.label_milliseconds("x"))
        self.assertEqual(0, xperiods.label_milliseconds(""))
        self.assertEqual(xperiods.suffix_milliseconds["monthly"], xperiods.label_milliseconds("foo_monthly"))
        self.assertEqual(xperiods.suffix_milliseconds["weekly"], xperiods.label_milliseconds("foo_weekly"))
        self.assertEqual(xperiods.suffix_milliseconds["daily"], xperiods.label_milliseconds("foo_daily"))
        self.assertEqual(xperiods.suffix_milliseconds["hourly"], xperiods.label_milliseconds("foo_hourly"))
        self.assertEqual(xperiods.suffix_milliseconds["minutely"] * 60, xperiods.label_milliseconds("foo_60minutely"))
        self.assertEqual(xperiods.suffix_milliseconds["minutely"] * 59, xperiods.label_milliseconds("foo_59minutely"))
        self.assertEqual(xperiods.suffix_milliseconds["secondly"], xperiods.label_milliseconds("foo_secondly"))
        self.assertEqual(xperiods.suffix_milliseconds["millisecondly"], xperiods.label_milliseconds("foo_millisecondly"))


#############################################################################
class TestCut(AbstractTestCase):

    def test_cut(self) -> None:
        lines = ["34\td1@s1", "56\td2@s2"]
        self.assertListEqual(["34", "56"], cut(1, lines=lines))
        self.assertListEqual(["d1@s1", "d2@s2"], cut(2, lines=lines))
        self.assertListEqual([], cut(1, lines=[]))
        self.assertListEqual([], cut(2, lines=[]))
        with self.assertRaises(ValueError):
            cut(0, lines=lines)

    def test_cut_field1(self) -> None:
        lines = ["a\tb\tc", "d\te\tf"]
        expected = ["a", "d"]
        self.assertEqual(expected, cut(field=1, lines=lines))

    def test_cut_field2(self) -> None:
        lines = ["a\tb\tc", "d\te\tf"]
        expected = ["b\tc", "e\tf"]
        self.assertEqual(expected, cut(field=2, lines=lines))

    def test_cut_invalid_field(self) -> None:
        lines = ["a\tb\tc"]
        with self.assertRaises(ValueError):
            cut(field=3, lines=lines)

    def test_cut_empty_lines(self) -> None:
        self.assertEqual([], cut(field=1, lines=[]))

    def test_cut_different_separator(self) -> None:
        lines = ["a,b,c", "d,e,f"]
        expected = ["a", "d"]
        self.assertEqual(expected, cut(field=1, separator=",", lines=lines))


#############################################################################
class TestDrain(AbstractTestCase):

    def test_drain(self) -> None:
        itr = iter(["foo", "bar"])
        drain(itr)
        with self.assertRaises(StopIteration):
            next(itr)


#############################################################################
class TestShuffleDict(AbstractTestCase):

    def test_shuffle_dict_preserves_items(self) -> None:
        d = {"a": 1, "b": 2, "c": 3}

        def fake_shuffle(lst: list) -> None:
            lst.reverse()

        with patch("random.shuffle", side_effect=fake_shuffle) as mock_shuffle:
            result = shuffle_dict(d)
            self.assertEqual({"c": 3, "b": 2, "a": 1}, result)
            mock_shuffle.assert_called_once()

    def test_shuffle_dict_empty(self) -> None:
        self.assertEqual({}, shuffle_dict({}))


#############################################################################
class TestSortedDict(AbstractTestCase):

    def test_sorted_dict_empty_dictionary_returns_empty(self) -> None:
        result: dict[str, int] = sorted_dict({})
        self.assertEqual({}, result)

    def test_sorted_dict_single_key_value_pair_is_sorted_correctly(self) -> None:
        result: dict[str, int] = sorted_dict({"a": 1})
        self.assertEqual({"a": 1}, result)

    def test_sorted_dict_multiple_key_value_pairs_are_sorted_by_keys(self) -> None:
        result: dict[str, int] = sorted_dict({"b": 2, "a": 1, "c": 3})
        self.assertEqual({"a": 1, "b": 2, "c": 3}, result)

    def test_sorted_dict_with_numeric_keys_is_sorted_correctly(self) -> None:
        result: dict[int, str] = sorted_dict({3: "three", 1: "one", 2: "two"})
        self.assertEqual({1: "one", 2: "two", 3: "three"}, result)

    def test_sorted_dict_with_mixed_key_types_raises_error(self) -> None:
        with self.assertRaises(TypeError):
            sorted_dict({"a": 1, 2: "two"})


#############################################################################
class TestTail(AbstractTestCase):

    def test_tail(self) -> None:
        fd, file = tempfile.mkstemp(prefix="test_bzfs.tail_")
        os.write(fd, "line1\nline2\n".encode())
        os.close(fd)
        self.assertEqual(["line1\n", "line2\n"], list(tail(file, n=10)))
        self.assertEqual(["line1\n", "line2\n"], list(tail(file, n=2)))
        self.assertEqual(["line2\n"], list(tail(file, n=1)))
        self.assertEqual([], list(tail(file, n=0)))
        os.remove(file)
        self.assertEqual([], list(tail(file, n=2)))


#############################################################################
class TestGetHomeDirectory(AbstractTestCase):
    def test_get_home_directory(self) -> None:
        old_home = os.environ.get("HOME")
        if old_home is not None:
            self.assertEqual(old_home, get_home_directory())
            os.environ.pop("HOME")
            try:
                self.assertEqual(old_home, get_home_directory())
            finally:
                os.environ["HOME"] = old_home


#############################################################################
class TestHumanReadable(AbstractTestCase):

    def assert_human_readable_float(self, actual: float, expected: str) -> None:
        self.assertEqual(expected, human_readable_float(actual))
        self.assertEqual("-" + expected, human_readable_float(-actual))

    def test_human_readable_float_with_one_digit_before_decimal(self) -> None:
        self.assert_human_readable_float(3.14159, "3.14")
        self.assert_human_readable_float(5.0, "5")
        self.assert_human_readable_float(0.5, "0.5")
        self.assert_human_readable_float(0.499999, "0.5")
        self.assert_human_readable_float(3.1477, "3.15")
        self.assert_human_readable_float(1.999999, "2")
        self.assert_human_readable_float(2.5, "2.5")
        self.assert_human_readable_float(3.5, "3.5")

    def test_human_readable_float_with_two_digits_before_decimal(self) -> None:
        self.assert_human_readable_float(12.34, "12.3")
        self.assert_human_readable_float(12.0, "12")
        self.assert_human_readable_float(12.54, "12.5")
        self.assert_human_readable_float(12.56, "12.6")

    def test_human_readable_float_with_three_or_more_digits_before_decimal(self) -> None:
        self.assert_human_readable_float(123.456, "123")
        self.assert_human_readable_float(123.516, "124")
        self.assert_human_readable_float(1234.4678, "1234")
        self.assert_human_readable_float(1234.5678, "1235")
        self.assert_human_readable_float(12345.078, "12345")
        self.assert_human_readable_float(12345.678, "12346")
        self.assert_human_readable_float(999.99, "1000")

    def test_human_readable_float_with_zero(self) -> None:
        self.assertEqual("0", human_readable_float(0.0))
        self.assertEqual("0", human_readable_float(-0.0))
        self.assertEqual("0", human_readable_float(0.001))
        self.assertEqual("0", human_readable_float(-0.001))

    def test_human_readable_float_with_halfway_rounding_behavior(self) -> None:
        # For |n| < 10 => 2 decimals
        self.assert_human_readable_float(1.15, "1.15")
        self.assert_human_readable_float(1.25, "1.25")
        self.assert_human_readable_float(1.35, "1.35")
        self.assert_human_readable_float(1.45, "1.45")

        # 10.xx => one decimal
        eps = 1.0e-15
        self.assert_human_readable_float(10.15, "10.2")
        self.assert_human_readable_float(10.25, "10.2")
        self.assert_human_readable_float(10.35 + eps, "10.4")
        self.assert_human_readable_float(10.45, "10.4")

    def test_human_readable_bytes(self) -> None:
        self.assertEqual("0 B", human_readable_bytes(0))
        self.assertEqual("581 B", human_readable_bytes(0.567 * 1024**1))
        self.assertEqual("2 KiB", human_readable_bytes(2 * 1024**1))
        self.assertEqual("1 MiB", human_readable_bytes(1 * 1024**2))
        self.assertEqual("1 GiB", human_readable_bytes(1 * 1024**3))
        self.assertEqual("1 TiB", human_readable_bytes(1 * 1024**4))
        self.assertEqual("1 PiB", human_readable_bytes(1 * 1024**5))
        self.assertEqual("1 EiB", human_readable_bytes(1 * 1024**6))
        self.assertEqual("1 ZiB", human_readable_bytes(1 * 1024**7))
        self.assertEqual("1 YiB", human_readable_bytes(1 * 1024**8))
        self.assertEqual("1 RiB", human_readable_bytes(1 * 1024**9))
        self.assertEqual("1 QiB", human_readable_bytes(1 * 1024**10))
        self.assertEqual("1024 QiB", human_readable_bytes(1 * 1024**11))
        self.assertEqual("3 B", human_readable_bytes(2.567, precision=0))
        self.assertEqual("2.6 B", human_readable_bytes(2.567, precision=1))
        self.assertEqual("2.57 B", human_readable_bytes(2.567, precision=2))
        self.assertEqual("2.57 B", human_readable_bytes(2.567, precision=None))

    def test_human_readable_duration(self) -> None:
        ms = 1000_000
        self.assertEqual("0ns", human_readable_duration(0))
        self.assertEqual("3ns", human_readable_duration(3))
        self.assertEqual("3μs", human_readable_duration(3 * 1000))
        self.assertEqual("3ms", human_readable_duration(3 * ms))
        self.assertEqual("1s", human_readable_duration(1000 * ms))
        self.assertEqual("3s", human_readable_duration(3000 * ms))
        self.assertEqual("3m", human_readable_duration(3000 * 60 * ms))
        self.assertEqual("3h", human_readable_duration(3000 * 60 * 60 * ms))
        self.assertEqual("1.25d", human_readable_duration(3000 * 60 * 60 * 10 * ms))
        self.assertEqual("12.5d", human_readable_duration(3000 * 60 * 60 * 100 * ms))
        self.assertEqual("1250d", human_readable_duration(3000 * 60 * 60 * 10000 * ms))
        self.assertEqual("125ns", human_readable_duration(125))
        self.assertEqual("125μs", human_readable_duration(125 * 1000))
        self.assertEqual("125ms", human_readable_duration(125 * 1000 * 1000))
        self.assertEqual("2.08m", human_readable_duration(125 * 1000 * 1000 * 1000))

        self.assertEqual("0s", human_readable_duration(0, unit="s"))
        self.assertEqual("3s", human_readable_duration(3, unit="s"))
        self.assertEqual("3m", human_readable_duration(3 * 60, unit="s"))
        self.assertEqual("0h", human_readable_duration(0, unit="h"))
        self.assertEqual("3h", human_readable_duration(3, unit="h"))
        self.assertEqual("7.5d", human_readable_duration(3 * 60, unit="h"))
        self.assertEqual("0.3ns", human_readable_duration(0.3))
        self.assertEqual("300ns", human_readable_duration(0.3, unit="μs"))
        self.assertEqual("300μs", human_readable_duration(0.3, unit="ms"))
        self.assertEqual("300ms", human_readable_duration(0.3, unit="s"))
        self.assertEqual("18s", human_readable_duration(0.3, unit="m"))
        self.assertEqual("18m", human_readable_duration(0.3, unit="h"))
        self.assertEqual("7.2h", human_readable_duration(0.3, unit="d"))
        self.assertEqual("259ns", human_readable_duration(3 / 1000_000_000_000, unit="d"))
        self.assertEqual("0.26ns", human_readable_duration(3 / 1000_000_000_000_000, unit="d"))
        with self.assertRaises(ValueError):
            human_readable_duration(3, unit="hhh")  # invalid unit

        self.assertEqual("3ns", human_readable_duration(2.567, precision=0))
        self.assertEqual("2.6ns", human_readable_duration(2.567, precision=1))
        self.assertEqual("2.57ns", human_readable_duration(2.567, precision=2))
        self.assertEqual("2.57ns", human_readable_duration(2.567, precision=None))

    def test_percent(self) -> None:
        self.assertEqual("3=30%", percent(3, 10))
        self.assertEqual("0=NaN%", percent(0, 0))


#############################################################################
class TestOpenNoFollow(AbstractTestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.real_path = os.path.join(self.tmpdir, "file.txt")
        with open(self.real_path, "w", encoding="utf-8") as f:
            f.write("hello")
        self.symlink_path = os.path.join(self.tmpdir, "link.txt")
        os.symlink(self.real_path, self.symlink_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)

    def test_read_text(self) -> None:
        with open_nofollow(self.real_path, "r", encoding="utf-8") as f:
            data = f.read()
        self.assertEqual("hello", data)

    def test_read_binary(self) -> None:
        with open(self.real_path, "rb") as f:
            raw = f.read()
        with open_nofollow(self.real_path, "rb") as f:
            data = f.read()
        self.assertEqual(raw, data)

    def test_write_truncate(self) -> None:
        with open_nofollow(self.real_path, "w", encoding="utf-8") as f:
            f.write("world")
        with open(self.real_path, "r", encoding="utf-8") as f:
            self.assertEqual("world", f.read())

    def test_append(self) -> None:
        with open_nofollow(self.real_path, "a", encoding="utf-8") as f:
            f.write(" world")
        with open(self.real_path, "r", encoding="utf-8") as f:
            self.assertEqual("hello world", f.read())

    def test_exclusive_create(self) -> None:
        new_path = os.path.join(self.tmpdir, "new.txt")
        f = open_nofollow(new_path, "x", encoding="utf-8")
        f.write("new")
        f.close()
        # second open should fail
        with self.assertRaises(FileExistsError):
            open_nofollow(new_path, "x")

    def test_plus_mode(self) -> None:
        with open_nofollow(self.real_path, "r+") as f:
            content = f.read()
            self.assertEqual("hello", content)
            f.seek(0)
            f.write("HELLO")
        with open(self.real_path, "r", encoding="utf-8") as f:
            self.assertEqual("HELLO", f.read())

    def test_symlink_blocked(self) -> None:
        with self.assertRaises(OSError) as cm:
            open_nofollow(self.symlink_path, "r")
        self.assertIn(cm.exception.errno, (errno.ELOOP, errno.EMLINK))

    def test_nonexistent_read(self) -> None:
        missing = os.path.join(self.tmpdir, "missing.txt")
        with self.assertRaises(FileNotFoundError):
            open_nofollow(missing, "r")

    def test_permission_bits(self) -> None:
        # set umask to zero temporarily so we can observe raw permission bits
        old_umask = os.umask(0)
        try:
            new_path = os.path.join(self.tmpdir, "perm.txt")
            open_nofollow(new_path, "w", perm=0o600).close()
            mode = stat.S_IMODE(os.stat(new_path).st_mode)
            self.assertEqual(0o600, mode)
        finally:
            os.umask(old_umask)

    def test_invalid_empty_mode(self) -> None:
        with self.assertRaises(ValueError):
            open_nofollow(self.real_path, "")

    def test_invalid_mode(self) -> None:
        with self.assertRaises(ValueError):
            open_nofollow(self.real_path, "z")

    def test_fdopen_failure_closes_fd(self) -> None:
        err = RuntimeError("fdopen boom")
        orig_close = os.close
        with mock.patch("os.fdopen", side_effect=err) as m_fdopen, mock.patch("os.close", side_effect=orig_close) as m_close:
            with self.assertRaises(RuntimeError):
                open_nofollow(self.real_path, "r")
        m_fdopen.assert_called_once()
        m_close.assert_called_once()

    def test_check_owner_skipped(self) -> None:
        """check_owner=False should skip ownership verification."""
        with mock.patch("os.fstat", side_effect=AssertionError("should not call")) as m_fstat:
            with open_nofollow(self.real_path, "r", check_owner=False, encoding="utf-8") as f:
                self.assertEqual("hello", f.read())
        m_fstat.assert_not_called()

    def test_owner_mismatch_raises_and_closes_fd(self) -> None:
        class Stat:
            st_uid = os.geteuid() + 1

        orig_close = os.close
        with mock.patch("os.fstat", return_value=Stat()), mock.patch("os.close", side_effect=orig_close) as m_close:
            with self.assertRaises(PermissionError):
                open_nofollow(self.real_path, "r")
        m_close.assert_called_once()

    def test_close_error_ignored(self) -> None:
        err = RuntimeError("boom")
        orig_close = os.close

        def failing_close(fd: int) -> None:
            orig_close(fd)
            raise OSError("close fail")

        with mock.patch("os.fdopen", side_effect=err), mock.patch("os.close", side_effect=failing_close) as m_close:
            with self.assertRaises(RuntimeError):
                open_nofollow(self.real_path, "r")
        m_close.assert_called_once()


#############################################################################
class TestFindMatch(AbstractTestCase):

    def test_basic(self) -> None:
        def condition(arg: str) -> bool:
            return arg.startswith("-")

        lst = ["a", "b", "-c", "d"]
        self.assert_find_match(2, lst, condition)

        self.assert_find_match(2, lst, condition, -3)
        self.assert_find_match(2, lst, condition, -2)
        self.assert_find_match(-1, lst, condition, -1)
        self.assert_find_match(2, lst, condition, 0)
        self.assert_find_match(2, lst, condition, 1)
        self.assert_find_match(2, lst, condition, 2)
        self.assert_find_match(-1, lst, condition, 3)
        self.assert_find_match(-1, lst, condition, 4)
        self.assert_find_match(-1, lst, condition, 5)

        self.assert_find_match(-1, lst, condition, end=-3)
        self.assert_find_match(-1, lst, condition, end=-2)
        self.assert_find_match(2, lst, condition, end=-1)
        self.assert_find_match(-1, lst, condition, end=0)
        self.assert_find_match(-1, lst, condition, end=1)
        self.assert_find_match(-1, lst, condition, end=2)
        self.assert_find_match(2, lst, condition, end=3)
        self.assert_find_match(2, lst, condition, end=4)
        self.assert_find_match(2, lst, condition, end=5)
        self.assert_find_match(2, lst, condition, end=6)

        self.assert_find_match(2, lst, condition, start=2, end=-1)
        self.assert_find_match(-1, lst, condition, start=2, end=-2)
        self.assert_find_match(-1, lst, condition, start=3, end=-1)
        self.assert_find_match(-1, lst, condition, start=3, end=-1, raises=None)
        self.assert_find_match(-1, lst, condition, start=3, end=-1, raises=False)

        self.assert_find_match(2, lst, condition, raises=None)
        self.assert_find_match(2, lst, condition, raises=False)
        self.assert_find_match(2, lst, condition, raises=True)
        with self.assertRaises(ValueError):
            find_match(lst, condition, start=0, end=2, raises=True)
        x = 2
        with self.assertRaises(ValueError) as e:
            find_match(lst, condition, start=0, end=2, raises=f"foo: {x}")
        self.assertEqual(f"foo: {x}", str(e.exception))
        with self.assertRaises(ValueError) as e:
            find_match(lst, condition, start=0, end=2, raises=lambda: f"foo: {x}")
        self.assertEqual(f"foo: {x}", str(e.exception))
        with self.assertRaises(ValueError) as e:
            find_match(lst, condition, start=0, end=2, raises="")
        self.assertEqual("", str(e.exception))

        lst = ["-c"]
        self.assert_find_match(0, lst, condition)
        self.assert_find_match(0, lst, condition, -1)
        self.assert_find_match(0, lst, condition, 0)
        self.assert_find_match(-1, lst, condition, 1)

        self.assert_find_match(-1, lst, condition, end=-1)
        self.assert_find_match(-1, lst, condition, end=0)
        self.assert_find_match(0, lst, condition, end=1)

        self.assert_find_match(-1, lst, condition, start=2, end=-1)
        self.assert_find_match(-1, lst, condition, start=2, end=-2)
        self.assert_find_match(-1, lst, condition, start=3, end=-1)

        lst = []
        self.assert_find_match(-1, lst, condition)
        self.assert_find_match(-1, lst, condition, -1)
        self.assert_find_match(-1, lst, condition, 0)
        self.assert_find_match(-1, lst, condition, 1)

        self.assert_find_match(-1, lst, condition, end=-1)
        self.assert_find_match(-1, lst, condition, end=0)
        self.assert_find_match(-1, lst, condition, end=1)

        self.assert_find_match(-1, lst, condition, start=2, end=-1)
        self.assert_find_match(-1, lst, condition, start=2, end=-2)
        self.assert_find_match(-1, lst, condition, start=3, end=-1)

        lst = ["a", "b", "-c", "-d"]
        self.assertEqual(2, find_match(lst, condition, start=None, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=None, end=None, reverse=True))
        self.assertEqual(2, find_match(lst, condition, start=2, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=2, end=None, reverse=True))
        self.assertEqual(3, find_match(lst, condition, start=3, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=3, end=None, reverse=True))

        self.assertEqual(2, find_match(lst, condition, start=0, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=0, end=None, reverse=True))
        self.assertEqual(3, find_match(lst, condition, start=-1, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=-1, end=None, reverse=True))
        self.assertEqual(2, find_match(lst, condition, start=-2, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=-2, end=None, reverse=True))
        self.assertEqual(2, find_match(lst, condition, start=-3, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=-3, end=None, reverse=True))

        lst = ["-a", "-b", "c", "d"]
        self.assertEqual(0, find_match(lst, condition, end=-1, reverse=False))
        self.assertEqual(1, find_match(lst, condition, end=-1, reverse=True))
        self.assertEqual(0, find_match(lst, condition, end=-2, reverse=False))
        self.assertEqual(1, find_match(lst, condition, end=-2, reverse=True))
        self.assertEqual(0, find_match(lst, condition, end=-3, reverse=False))
        self.assertEqual(0, find_match(lst, condition, end=-3, reverse=True))
        self.assertEqual(-1, find_match(lst, condition, end=-4, reverse=False))
        self.assertEqual(-1, find_match(lst, condition, end=-4, reverse=True))

        lst = ["a", "-b", "-c", "d"]
        self.assertEqual(1, find_match(lst, condition, start=1, end=-1, reverse=False))
        self.assertEqual(2, find_match(lst, condition, start=1, end=-1, reverse=True))
        self.assertEqual(1, find_match(lst, condition, start=1, end=-2, reverse=False))
        self.assertEqual(1, find_match(lst, condition, start=1, end=-2, reverse=True))

    def assert_find_match(
        self,
        expected: int,
        lst: Sequence[str],
        condition: Callable[[str], bool],
        start: int | None = None,
        end: int | None = None,
        raises: bool | str | Callable[[], str] | None = False,
    ) -> None:
        raise_arg = cast(Union[bool, str, Callable[[], str]], raises)
        self.assertEqual(expected, find_match(lst, condition, start=start, end=end, reverse=False, raises=raise_arg))
        self.assertEqual(expected, find_match(lst, condition, start=start, end=end, reverse=True, raises=raise_arg))


#############################################################################
class TestReplaceCapturingGroups(AbstractTestCase):
    @staticmethod
    def replace_capturing_group(regex: str) -> str:
        return replace_capturing_groups_with_non_capturing_groups(regex)

    def test_basic_case(self) -> None:
        self.assertEqual("(?:abc)", self.replace_capturing_group("(abc)"))

    def test_nested_groups(self) -> None:
        self.assertEqual("(?:a(?:bc)d)", self.replace_capturing_group("(a(bc)d)"))

    def test_preceding_backslash(self) -> None:
        self.assertEqual("\\(abc)", self.replace_capturing_group("\\(abc)"))

    def test_group_starting_with_question_mark(self) -> None:
        self.assertEqual("(?abc)", self.replace_capturing_group("(?abc)"))

    def test_multiple_groups(self) -> None:
        self.assertEqual("(?:abc)(?:def)", self.replace_capturing_group("(abc)(def)"))

    def test_mixed_cases(self) -> None:
        self.assertEqual(
            "a(?:bc\\(de)f(?:gh)?i",
            self.replace_capturing_group("a(bc\\(de)f(gh)?i"),
        )

    def test_empty_group(self) -> None:
        self.assertEqual("(?:)", self.replace_capturing_group("()"))

    def test_group_with_named_group(self) -> None:
        self.assertEqual("(?P<name>abc)", self.replace_capturing_group("(?P<name>abc)"))

    def test_group_with_non_capturing_group(self) -> None:
        self.assertEqual("(?:a(?:bc)d)", self.replace_capturing_group("(a(?:bc)d)"))

    def test_group_with_lookahead(self) -> None:
        self.assertEqual("(?:abc)(?=def)", self.replace_capturing_group("(abc)(?=def)"))

    def test_group_with_lookbehind(self) -> None:
        self.assertEqual("(?<=abc)(?:def)", self.replace_capturing_group("(?<=abc)(def)"))

    def test_escaped_characters(self) -> None:
        pattern = re.escape("(abc)")
        self.assertEqual(pattern, self.replace_capturing_group(pattern))

    def test_complex_pattern_with_escape(self) -> None:
        complex_pattern = re.escape("(a[b]c{d}e|f.g)")
        self.assertEqual(complex_pattern, self.replace_capturing_group(complex_pattern))

    def test_complex_pattern(self) -> None:
        complex_pattern = "(a[b]c{d}e|f.g)(h(i|j)k)?(\\(l\\))"
        expected_result = "(?:a[b]c{d}e|f.g)(?:h(?:i|j)k)?(?:\\(l\\))"
        self.assertEqual(expected_result, self.replace_capturing_group(complex_pattern))


#############################################################################
class TestSubprocessRun(AbstractTestCase):
    def test_successful_command(self) -> None:
        result = subprocess_run(["true"], stdout=PIPE, stderr=subprocess.PIPE)
        self.assertEqual(0, result.returncode)
        self.assertEqual(b"", result.stdout)
        self.assertEqual(b"", result.stderr)

    def test_failing_command_no_check(self) -> None:
        result = subprocess_run(["false"], stdout=PIPE, stderr=PIPE)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(b"", result.stdout)
        self.assertEqual(b"", result.stderr)

    def test_failing_command_with_check(self) -> None:
        with self.assertRaises(subprocess.CalledProcessError) as context:
            subprocess_run(["false"], stdout=PIPE, stderr=PIPE, check=True)
        self.assertIsInstance(context.exception, subprocess.CalledProcessError)
        self.assertIsInstance(context.exception.returncode, int)
        self.assertTrue(context.exception.returncode != 0)

    def test_input_bytes(self) -> None:
        result = subprocess_run(["cat"], input=b"hello", stdout=PIPE, stderr=PIPE)
        self.assertEqual(b"hello", result.stdout)

    def test_valueerror_input_and_stdin(self) -> None:
        with self.assertRaises(ValueError):
            subprocess_run(["cat"], input=b"hello", stdin=PIPE, stdout=PIPE, stderr=PIPE)

    def test_timeout_expired(self) -> None:
        with self.assertRaises(subprocess.TimeoutExpired) as context:
            subprocess_run(["sleep", "1"], timeout=0.01, stdout=PIPE, stderr=PIPE)
        self.assertIsInstance(context.exception, subprocess.TimeoutExpired)
        self.assertEqual(["sleep", "1"], context.exception.cmd)

    def test_keyboardinterrupt_signal(self) -> None:
        old_handler = signal.signal(signal.SIGINT, signal.default_int_handler)  # install a handler that ignores SIGINT
        try:

            def send_sigint_to_sleep_cli() -> None:
                time.sleep(0.1)  # ensure sleep CLI has started before we kill it
                os.kill(os.getpid(), signal.SIGINT)  # send SIGINT to all members of the process group, including `sleep` CLI

            thread = threading.Thread(target=send_sigint_to_sleep_cli)
            thread.start()
            with self.assertRaises(KeyboardInterrupt):
                subprocess_run(["sleep", "1"])
            thread.join()
        finally:
            signal.signal(signal.SIGINT, old_handler)  # restore original signal handler


#############################################################################
class TestPIDExists(AbstractTestCase):

    def test_pid_exists(self) -> None:
        self.assertTrue(pid_exists(os.getpid()))
        self.assertFalse(pid_exists(-1))
        self.assertFalse(pid_exists(0))
        # This fake PID is extremely unlikely to be alive because it is orders of magnitude higher than the typical
        # range of process IDs on Unix-like systems:
        fake_pid = 2**31 - 1  # 2147483647
        self.assertFalse(pid_exists(fake_pid))

        # Process 1 is typically owned by root; if not running as root, this should return True because it raises EPERM
        if os.getuid() != 0:
            self.assertTrue(pid_exists(1))

    @patch("os.kill")
    def test_pid_exists_with_unexpected_oserror_returns_none(self, mock_kill: MagicMock) -> None:
        # Simulate an unexpected OSError (e.g., EINVAL) and verify that pid_exists returns None.
        err = OSError()
        err.errno = errno.EINVAL
        mock_kill.side_effect = err
        self.assertIsNone(pid_exists(1234))


#############################################################################
class TestTerminateProcessSubtree(AbstractTestCase):
    def setUp(self) -> None:
        self.children: list[subprocess.Popen[Any]] = []

    def tearDown(self) -> None:
        for child in self.children:
            try:
                child.kill()
            except OSError:
                pass
        self.children = []

    def test_get_descendant_processes(self) -> None:
        child = subprocess.Popen(["sleep", "1"])
        self.children.append(child)
        time.sleep(0.1)
        descendants = get_descendant_processes(os.getpid())
        self.assertIn(child.pid, descendants, "Child PID not found in descendants")

    def test_terminate_process_subtree_excluding_current(self) -> None:
        child = subprocess.Popen(["sleep", "1"])
        self.children.append(child)
        time.sleep(0.1)
        self.assertIsNone(child.poll(), "Child process should be running before termination")
        terminate_process_subtree(except_current_process=True)
        time.sleep(0.1)
        self.assertIsNotNone(child.poll(), "Child process should be terminated")


#############################################################################
class TestSmallPriorityQueue(AbstractTestCase):
    def setUp(self) -> None:
        self.pq: SmallPriorityQueue[int] = SmallPriorityQueue()
        self.pq_reverse: SmallPriorityQueue[int] = SmallPriorityQueue(reverse=True)

    def test_basic(self) -> None:
        self.assertEqual(0, len(self.pq))
        self.assertTrue(len(str(self.pq)) > 0)
        self.pq.push(2)
        self.assertEqual(1, len(self.pq))
        self.pq.push(1)
        self.assertEqual(2, len(self.pq))
        self.assertTrue(2 in self.pq)
        self.assertTrue(1 in self.pq)
        self.assertFalse(0 in self.pq)
        self.pq.clear()
        self.assertEqual(0, len(self.pq))

    def test_push_and_pop(self) -> None:
        self.pq.push(3)
        self.pq.push(1)
        self.pq.push(2)
        self.assertEqual([1, 2, 3], self.pq._lst)
        self.assertEqual(1, self.pq.pop())
        self.assertEqual([2, 3], self.pq._lst)

    def test_pop_empty(self) -> None:
        with self.assertRaises(IndexError):  # Generic IndexError from list.pop()
            self.pq.pop()

    def test_remove(self) -> None:
        self.pq.push(3)
        self.pq.push(1)
        self.pq.push(2)
        self.pq.remove(2)
        self.assertEqual([1, 3], self.pq._lst)
        self.assertFalse(self.pq.remove(0))
        self.assertTrue(self.pq.remove(1))
        self.assertEqual([3], self.pq._lst)

    def test_remove_nonexistent_element(self) -> None:
        self.pq.push(1)
        self.pq.push(3)
        self.pq.push(2)

        # Attempt to remove an element that doesn't exist (should raise IndexError)
        self.assertFalse(self.pq.remove(4))

    def test_peek(self) -> None:
        self.pq.push(3)
        self.pq.push(1)
        self.pq.push(2)
        self.assertEqual(1, self.pq.peek())
        self.assertEqual([1, 2, 3], self.pq._lst)
        self.pq_reverse.push(3)
        self.pq_reverse.push(1)
        self.pq_reverse.push(2)
        self.assertEqual(3, self.pq_reverse.peek())
        self.assertEqual([1, 2, 3], self.pq_reverse._lst)

    def test_peek_empty(self) -> None:
        with self.assertRaises(IndexError):  # Generic IndexError from list indexing
            self.pq.peek()

        with self.assertRaises(IndexError):  # Generic IndexError from list indexing
            self.pq_reverse.peek()

    def test_reverse_order(self) -> None:
        self.pq_reverse.push(1)
        self.pq_reverse.push(3)
        self.pq_reverse.push(2)
        self.assertEqual(3, self.pq_reverse.pop())
        self.assertEqual([1, 2], self.pq_reverse._lst)

    def test_iter(self) -> None:
        self.pq.push(3)
        self.pq.push(1)
        self.pq.push(2)
        self.assertListEqual([1, 2, 3], list(iter(self.pq)))
        self.assertEqual("[1, 2, 3]", str(self.pq))
        self.pq_reverse.push(1)
        self.pq_reverse.push(3)
        self.pq_reverse.push(2)
        self.assertListEqual([3, 2, 1], list(iter(self.pq_reverse)))
        self.assertEqual("[3, 2, 1]", str(self.pq_reverse))

    def test_duplicates(self) -> None:
        self.pq.push(2)
        self.pq.push(2)
        self.pq.push(1)
        self.assertEqual([1, 2, 2], self.pq._lst)

        # Pop should remove the smallest duplicate first
        self.assertEqual(1, self.pq.pop())
        self.assertEqual([2, 2], self.pq._lst)

        # Remove one duplicate, leaving another
        self.pq.remove(2)
        self.assertEqual([2], self.pq._lst)

        # Peek and pop should now work on the remaining duplicate
        self.assertEqual(2, self.pq.peek())
        self.assertEqual(2, self.pq.pop())
        self.assertEqual(0, len(self.pq))

    def test_reverse_with_duplicates(self) -> None:
        self.pq_reverse.push(2)
        self.pq_reverse.push(2)
        self.pq_reverse.push(3)
        self.pq_reverse.push(1)
        self.assertEqual([1, 2, 2, 3], self.pq_reverse._lst)

        # Pop the largest first in reverse order
        self.assertEqual(3, self.pq_reverse.pop())
        self.assertEqual([1, 2, 2], self.pq_reverse._lst)

        # Remove a duplicate
        self.pq_reverse.remove(2)
        self.assertEqual([1, 2], self.pq_reverse._lst)

        # Peek and pop the remaining elements
        self.assertEqual(2, self.pq_reverse.peek())
        self.assertEqual(2, self.pq_reverse.pop())
        self.assertEqual(1, self.pq_reverse.pop())
        self.assertEqual(0, len(self.pq_reverse))


#############################################################################
class TestSynchronizedBool(AbstractTestCase):
    def test_initialization(self) -> None:
        b = SynchronizedBool(True)
        self.assertTrue(b.value)

        b = SynchronizedBool(False)
        self.assertFalse(b.value)

        with self.assertRaises(AssertionError):
            SynchronizedBool(cast(Any, "not_a_bool"))

    def test_value_property(self) -> None:
        b = SynchronizedBool(True)
        self.assertTrue(b.value)

        b.value = False
        self.assertFalse(b.value)

    def test_loop(self) -> None:
        b = SynchronizedBool(False)
        for _ in range(3):
            b.value = not b.value
        self.assertIsInstance(b.value, bool)

    def test_bool_conversion(self) -> None:
        b = SynchronizedBool(True)
        self.assertTrue(bool(b))

        b.value = False
        self.assertFalse(bool(b))

    def test_get_and_set(self) -> None:
        b = SynchronizedBool(True)
        self.assertTrue(b.get_and_set(False))
        self.assertFalse(b.value)

    def test_compare_and_set(self) -> None:
        b = SynchronizedBool(True)
        self.assertTrue(b.compare_and_set(True, False))
        self.assertFalse(b.value)

        b = SynchronizedBool(True)
        self.assertFalse(b.compare_and_set(False, False))
        self.assertTrue(b.value)

    def test_str_and_repr(self) -> None:
        b = SynchronizedBool(True)
        self.assertEqual("True", str(b))
        self.assertEqual("True", repr(b))

        b.value = False
        self.assertEqual("False", str(b))
        self.assertEqual("False", repr(b))


#############################################################################
class TestSynchronizedDict(AbstractTestCase):
    def setUp(self) -> None:
        self.sync_dict: SynchronizedDict = SynchronizedDict({"a": 1, "b": 2, "c": 3})

    def test_getitem(self) -> None:
        self.assertEqual(1, self.sync_dict["a"])
        self.assertEqual(2, self.sync_dict["b"])

    def test_setitem(self) -> None:
        self.sync_dict["d"] = 4
        self.assertEqual(4, self.sync_dict["d"])

    def test_delitem(self) -> None:
        del self.sync_dict["a"]
        self.assertNotIn("a", self.sync_dict)

    def test_contains(self) -> None:
        self.assertTrue("a" in self.sync_dict)
        self.assertFalse("z" in self.sync_dict)

    def test_len(self) -> None:
        self.assertEqual(3, len(self.sync_dict))
        del self.sync_dict["a"]
        self.assertEqual(2, len(self.sync_dict))

    def test_repr(self) -> None:
        self.assertEqual(repr({"a": 1, "b": 2, "c": 3}), repr(self.sync_dict))

    def test_str(self) -> None:
        self.assertEqual(str({"a": 1, "b": 2, "c": 3}), str(self.sync_dict))

    def test_get(self) -> None:
        self.assertEqual(1, self.sync_dict.get("a"))
        self.assertEqual(42, self.sync_dict.get("z", 42))

    def test_pop(self) -> None:
        value = self.sync_dict.pop("b")
        self.assertEqual(2, value)
        self.assertNotIn("b", self.sync_dict)

    def test_clear(self) -> None:
        self.sync_dict.clear()
        self.assertEqual(0, len(self.sync_dict))

    def test_items(self) -> None:
        items = self.sync_dict.items()
        self.assertEqual({("a", 1), ("b", 2), ("c", 3)}, set(items))

    def test_loop(self) -> None:
        self.sync_dict["key"] = 1
        self.assertIn("key", self.sync_dict)


#############################################################################
class TestXFinally(AbstractTestCase):

    def test_xfinally_executes_cleanup_on_success(self) -> None:
        cleanup = MagicMock()
        with xfinally(cleanup):
            pass
        cleanup.assert_called_once()

    def test_xfinally_executes_cleanup_on_exception(self) -> None:
        cleanup = MagicMock()
        with self.assertRaises(ValueError):
            with xfinally(cleanup):
                raise ValueError("Body error")
        cleanup.assert_called_once()

    def test_xfinally_propagates_cleanup_exception(self) -> None:
        cleanup = MagicMock(side_effect=RuntimeError("Cleanup error"))
        with self.assertRaises(RuntimeError) as cm:
            with xfinally(cleanup):
                pass
        self.assertEqual("Cleanup error", str(cm.exception))
        cleanup.assert_called_once()

    # @unittest.skipIf(sys.version_info != (3, 10), "Requires Python <= 3.10")
    @unittest.skipIf(sys.version_info < (3, 10), "Requires Python >= 3.10")
    def test_xfinally_handles_cleanup_exception_python_3_10_or_lower(self) -> None:
        cleanup = MagicMock(side_effect=RuntimeError("Cleanup error"))
        with self.assertRaises(ValueError) as cm:
            with xfinally(cleanup):
                raise ValueError("Body error")
        self.assertIsInstance(cm.exception.__context__, RuntimeError)
        self.assertEqual("Cleanup error", str(cm.exception.__context__))
        cleanup.assert_called_once()

    @unittest.skipIf(not sys.version_info >= (3, 11), "Requires Python >= 3.11")
    def test_xfinally_handles_cleanup_exception_python_3_11_or_higher(self) -> None:
        self.skipTest("disabled until python 3.11 is the minimum supported")
        cleanup = MagicMock(side_effect=RuntimeError("Cleanup error"))
        with self.assertRaises(ExceptionGroup) as cm:  # type: ignore  # noqa: F821
            with xfinally(cleanup):
                raise ValueError("Body error")
        self.assertEqual(2, len(cm.exception.exceptions))
        self.assertIsInstance(cm.exception.exceptions[0], ValueError)
        self.assertIsInstance(cm.exception.exceptions[1], RuntimeError)
        cleanup.assert_called_once()


###############################################################################
class TestCurrentDateTime(AbstractTestCase):

    def setUp(self) -> None:
        self.fixed_datetime = datetime(2024, 1, 1, 12, 0, 0)

    def test_now(self) -> None:
        self.assertIsNotNone(current_datetime(tz_spec=None, now_fn=None))
        self.assertIsNotNone(current_datetime(tz_spec="UTC", now_fn=None))
        self.assertIsNotNone(current_datetime(tz_spec="+0530", now_fn=None))
        self.assertIsNotNone(current_datetime(tz_spec="+05:30", now_fn=None))
        self.assertIsNotNone(current_datetime(tz_spec="-0430", now_fn=None))
        self.assertIsNotNone(current_datetime(tz_spec="-04:30", now_fn=None))

    def test_local_timezone(self) -> None:
        expected = self.fixed_datetime.astimezone(tz=None)
        actual = current_datetime(tz_spec=None, now_fn=lambda tz=None: self.fixed_datetime.astimezone(tz=tz))  # type: ignore
        self.assertEqual(expected, actual)

        fmt = "%Y-%m-%dT%H:%M:%S"
        self.assertEqual("2024-01-01T12:00:00", actual.strftime(fmt))
        fmt = "%Y-%m-%d_%H:%M:%S"
        self.assertEqual("2024-01-01_12:00:00", actual.strftime(fmt))
        fmt = "%Y-%m-%d_%H:%M:%S_%Z"
        expected_prefix = "2024-01-01_12:00:00_"
        self.assertTrue(actual.strftime(fmt).startswith(expected_prefix))
        suffix = actual.strftime(fmt)[len(expected_prefix) :]
        self.assertGreater(len(suffix), 0)

        fmt = "%Y-%m-%d_%H:%M:%S%z"
        expected_prefix = "2024-01-01_12:00:00"
        self.assertTrue(actual.strftime(fmt).startswith(expected_prefix))
        suffix = actual.strftime(fmt)[len(expected_prefix) :]
        self.assertIn(suffix[0], ["+", "-"])
        self.assertTrue(suffix[1:].isdigit())

    def test_utc_timezone(self) -> None:
        expected = self.fixed_datetime.astimezone(tz=timezone.utc)

        def now_fn(tz: tzinfo | None = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = current_datetime(tz_spec="UTC", now_fn=now_fn)
        self.assertEqual(expected, actual)

    def test_tzoffset_positive(self) -> None:
        tz_spec = "+0530"
        tz = timezone(timedelta(hours=5, minutes=30))
        expected = self.fixed_datetime.astimezone(tz=tz)

        def now_fn(_: tzinfo | None = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = current_datetime(tz_spec=tz_spec, now_fn=now_fn)
        self.assertEqual(expected, actual)

    def test_tzoffset_negative(self) -> None:
        tz_spec = "-0430"
        tz = timezone(timedelta(hours=-4, minutes=-30))
        expected = self.fixed_datetime.astimezone(tz=tz)

        def now_fn(_: tzinfo | None = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = current_datetime(tz_spec=tz_spec, now_fn=now_fn)
        self.assertEqual(expected, actual)

    def test_iana_timezone(self) -> None:
        if sys.version_info < (3, 9):
            self.skipTest("ZoneInfo requires python >= 3.9")
        from zoneinfo import ZoneInfo

        tz_spec = "Asia/Tokyo"
        self.assertIsNotNone(current_datetime(tz_spec=tz_spec, now_fn=None))
        tz = ZoneInfo(tz_spec)
        expected = self.fixed_datetime.astimezone(tz=tz)

        def now_fn(_: tzinfo | None = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = current_datetime(tz_spec=tz_spec, now_fn=now_fn)
        self.assertEqual(expected, actual)

        fmt = "%Y-%m-%dT%H:%M:%S%z"
        expected_prefix = "2024-01-01T20:00:00"
        suffix = actual.strftime(fmt)[len(expected_prefix) :]
        self.assertIn(suffix[0], ["+", "-"])
        self.assertTrue(suffix[1:].isdigit())

    def test_invalid_timezone(self) -> None:
        with self.assertRaises(ValueError) as context:
            current_datetime(tz_spec="INVALID")
        self.assertIn("Invalid timezone specification", str(context.exception))

    def test_invalid_timezone_format_missing_sign(self) -> None:
        with self.assertRaises(ValueError):
            current_datetime(tz_spec="0400")

    def test_unixtime_conversion(self) -> None:
        iso = "2024-01-02T03:04:05+00:00"
        unix = unixtime_fromisoformat(iso)
        expected = "2024-01-02_03:04:05+00:00"
        self.assertEqual(expected, isotime_from_unixtime(unix))

    def test_get_timezone_variants(self) -> None:
        if sys.version_info < (3, 9):
            self.skipTest("ZoneInfo requires python >= 3.9")
        self.assertIsNone(get_timezone())
        self.assertEqual(timezone.utc, get_timezone("UTC"))
        tz = get_timezone("+0130")
        assert tz is not None
        self.assertEqual(90 * 60, cast(timedelta, tz.utcoffset(None)).total_seconds())
        zone = get_timezone("Europe/Vienna")
        self.assertEqual("Europe/Vienna", getattr(zone, "key", None))
        with self.assertRaises(ValueError):
            get_timezone("bad-tz")


@contextlib.contextmanager
def stop_on_failure_subtest(**params: Any) -> Iterator[None]:
    """Context manager to mimic UnitTest.subTest() but stop on first failure."""
    try:
        yield
    except AssertionError as e:
        raise AssertionError(f"SubTest failed with parameters: {params}") from e
