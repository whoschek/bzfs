#
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

import argparse
import errno
import itertools
import logging
import os
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone, tzinfo
from logging import Logger
from pathlib import Path
from subprocess import PIPE
from typing import Any, Callable, DefaultDict, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Union, cast
from unittest.mock import patch, mock_open, MagicMock

from bzfs_main import bzfs
from bzfs_main.bzfs import find_match, getenv_any, log_trace, Remote, PeriodAnchors
from bzfs_tests.zfs_util import is_solaris_zfs

# constants:
test_mode = getenv_any("test_mode", "")  # Consider toggling this when testing
is_unit_test = test_mode == "unit"  # run only unit tests aka skip integration tests
is_smoke_test = test_mode == "smoke"  # run only a small subset of tests
is_functional_test = test_mode == "functional"  # run most tests but only in a single local config combination
is_adhoc_test = test_mode == "adhoc"  # run only a few isolated changes


def suite() -> unittest.TestSuite:
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestXFinally))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestHelperFunctions))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestTerminateProcessSubtree))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSubprocessRun))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestParseDatasetLocator))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestReplaceCapturingGroups))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestFindMatch))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestBuildTree))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCurrentDateTime))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRoundDatetimeUpToDurationMultiple))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSmallPriorityQueue))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSynchronizedBool))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSynchronizedDict))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestArgumentParser))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestDatasetPairsAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestFileOrLiteralAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestNewSnapshotFilterGroupAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestTimeRangeAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestLogConfigVariablesAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSafeFileNameAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCheckRange))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCheckPercentRange))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestPythonVersionCheck))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRankRangeAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestConnectionPool))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestItrSSHCmdParallel))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestProcessDatasetsInParallel))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestIncrementalSendSteps))
    # suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestPerformance))
    return suite


def argparser_parse_args(args: List[str]) -> argparse.Namespace:
    return bzfs.argument_parser().parse_args(args + ["--log-dir", os.path.join(bzfs.get_home_directory(), "bzfs-logs-test")])


#############################################################################
class TestXFinally(unittest.TestCase):

    def test_xfinally_executes_cleanup_on_success(self) -> None:
        cleanup = MagicMock()
        with bzfs.xfinally(cleanup):
            pass
        cleanup.assert_called_once()

    def test_xfinally_executes_cleanup_on_exception(self) -> None:
        cleanup = MagicMock()
        with self.assertRaises(ValueError):
            with bzfs.xfinally(cleanup):
                raise ValueError("Body error")
        cleanup.assert_called_once()

    def test_xfinally_propagates_cleanup_exception(self) -> None:
        cleanup = MagicMock(side_effect=RuntimeError("Cleanup error"))
        with self.assertRaises(RuntimeError) as cm:
            with bzfs.xfinally(cleanup):
                pass
        self.assertEqual("Cleanup error", str(cm.exception))
        cleanup.assert_called_once()

    # @unittest.skipIf(sys.version_info != (3, 10), "Requires Python <= 3.10")
    @unittest.skipIf(sys.version_info < (3, 10), "Requires Python >= 3.10")
    def test_xfinally_handles_cleanup_exception_python_3_10_or_lower(self) -> None:
        cleanup = MagicMock(side_effect=RuntimeError("Cleanup error"))
        with self.assertRaises(ValueError) as cm:
            with bzfs.xfinally(cleanup):
                raise ValueError("Body error")
        self.assertIsInstance(cm.exception.__context__, RuntimeError)
        self.assertEqual("Cleanup error", str(cm.exception.__context__))
        cleanup.assert_called_once()

    @unittest.skipIf(not sys.version_info >= (3, 11), "Requires Python >= 3.11")
    def test_xfinally_handles_cleanup_exception_python_3_11_or_higher(self) -> None:
        self.skipTest("disabled until python 3.11 is the minimum supported")
        cleanup = MagicMock(side_effect=RuntimeError("Cleanup error"))
        with self.assertRaises(ExceptionGroup) as cm:  # type: ignore  # noqa: F821
            with bzfs.xfinally(cleanup):
                raise ValueError("Body error")
        self.assertEqual(2, len(cm.exception.exceptions))
        self.assertIsInstance(cm.exception.exceptions[0], ValueError)
        self.assertIsInstance(cm.exception.exceptions[1], RuntimeError)
        cleanup.assert_called_once()


#############################################################################
class TestHelperFunctions(unittest.TestCase):
    s = "s"
    d = "d"
    a = "a"

    def merge_sorted_iterators(self, src: List[Any], dst: List[Any], choice: str) -> List[Tuple[Any, ...]]:
        s, d, a = self.s, self.d, self.a
        return [item for item in bzfs.Job().merge_sorted_iterators([s, d, a], choice, iter(src), iter(dst))]

    def assert_merge_sorted_iterators(
        self,
        expected: List[Tuple[Any, ...]],
        src: List[Any],
        dst: List[Any],
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

    def test_append_if_absent(self) -> None:
        self.assertListEqual([], bzfs.append_if_absent([]))
        self.assertListEqual(["a"], bzfs.append_if_absent([], "a"))
        self.assertListEqual(["a"], bzfs.append_if_absent([], "a", "a"))

    def test_cut(self) -> None:
        lines = ["34\td1@s1", "56\td2@s2"]
        self.assertListEqual(["34", "56"], bzfs.cut(1, lines=lines))
        self.assertListEqual(["d1@s1", "d2@s2"], bzfs.cut(2, lines=lines))
        self.assertListEqual([], bzfs.cut(1, lines=[]))
        self.assertListEqual([], bzfs.cut(2, lines=[]))
        with self.assertRaises(ValueError):
            bzfs.cut(0, lines=lines)

    def test_get_home_directory(self) -> None:
        old_home = os.environ.get("HOME")
        if old_home is not None:
            self.assertEqual(old_home, bzfs.get_home_directory())
            os.environ.pop("HOME")
            try:
                self.assertEqual(old_home, bzfs.get_home_directory())
            finally:
                os.environ["HOME"] = old_home

    def test_tail(self) -> None:
        fd, file = tempfile.mkstemp(prefix="test_bzfs.tail_")
        os.write(fd, "line1\nline2\n".encode())
        os.close(fd)
        self.assertEqual(["line1\n", "line2\n"], list(bzfs.tail(file, n=10)))
        self.assertEqual(["line1\n", "line2\n"], list(bzfs.tail(file, n=2)))
        self.assertEqual(["line2\n"], list(bzfs.tail(file, n=1)))
        self.assertEqual([], list(bzfs.tail(file, n=0)))
        os.remove(file)
        self.assertEqual([], list(bzfs.tail(file, n=2)))

    def test_validate_port(self) -> None:
        bzfs.validate_port(47, "msg")
        bzfs.validate_port("47", "msg")
        bzfs.validate_port(0, "msg")
        bzfs.validate_port("", "msg")
        with self.assertRaises(SystemExit):
            bzfs.validate_port("xxx47", "msg")

    def test_validate_quoting(self) -> None:
        params = bzfs.Params(argparser_parse_args(args=["src", "dst"]))
        params.validate_quoting([""])
        params.validate_quoting(["foo"])
        with self.assertRaises(SystemExit):
            params.validate_quoting(['foo"'])
        with self.assertRaises(SystemExit):
            params.validate_quoting(["foo'"])
        with self.assertRaises(SystemExit):
            params.validate_quoting(["foo`"])

    def test_validate_arg(self) -> None:
        params = bzfs.Params(argparser_parse_args(args=["src", "dst"]))
        params.validate_arg("")
        params.validate_arg("foo")
        with self.assertRaises(SystemExit):
            params.validate_arg("foo ")
        with self.assertRaises(SystemExit):
            params.validate_arg("foo" + "\t")
        with self.assertRaises(SystemExit):
            params.validate_arg("foo" + "\t", allow_spaces=True)
        params.validate_arg("foo bar", allow_spaces=True)
        with self.assertRaises(SystemExit):
            params.validate_arg(" foo  bar ", allow_spaces=False)
        params.validate_arg(" foo  bar ", allow_spaces=True)
        with self.assertRaises(SystemExit):
            params.validate_arg("foo'bar")
        params.validate_arg("foo'bar", allow_all=True)
        with self.assertRaises(SystemExit):
            params.validate_arg('foo"bar')
        params.validate_arg('foo"bar', allow_all=True)
        with self.assertRaises(SystemExit):
            params.validate_arg("foo\tbar")
        params.validate_arg("foo\tbar", allow_all=True)
        with self.assertRaises(SystemExit):
            params.validate_arg("foo`bar")
        params.validate_arg("foo`bar", allow_all=True)
        with self.assertRaises(SystemExit):
            params.validate_arg("foo\nbar")
        params.validate_arg("foo\nbar", allow_all=True)
        with self.assertRaises(SystemExit):
            params.validate_arg("foo\rbar")
        params.validate_arg("foo\rbar", allow_all=True)
        params.validate_arg(" foo  bar ", allow_all=True)

    def test_validate_program_name(self) -> None:
        bzfs.ProgramValidator().validate_program("/foo/bar.bak")
        with self.assertRaises(SystemExit):
            bzfs.ProgramValidator().validate_program("/foo/bar.bak", extra_invalid_chars=",.")
        with self.assertRaises(SystemExit):
            bzfs.ProgramValidator().validate_program("awk")
        with self.assertRaises(SystemExit):
            bzfs.ProgramValidator().validate_program("/usr/bin/awk")
        bzfs.ProgramValidator().validate_program("/foo/bar")
        with self.assertRaises(SystemExit):
            bzfs.ProgramValidator().validate_program("/foo/bar/")
        with self.assertRaises(SystemExit):
            bzfs.ProgramValidator().validate_program("/")
        with self.assertRaises(SystemExit):
            bzfs.ProgramValidator().validate_program("")

    def test_validate_program_name_must_not_be_empty(self) -> None:
        args = argparser_parse_args(args=["src", "dst"])
        args.zfs_program = ""
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zfs_program = None
        with self.assertRaises(SystemExit):
            bzfs.Params(args)

    def test_validate_program_name_must_not_contain_special_chars(self) -> None:
        args = argparser_parse_args(args=["src", "dst"])
        args.zfs_program = "true;false"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zfs_program = "echo foo|cat"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zfs_program = "foo>bar"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zfs_program = "foo\nbar"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zfs_program = "foo\\bar"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)

    def test_validate_program_name_must_not_be_sudo(self) -> None:
        args = argparser_parse_args(args=["src", "dst"])
        args.zfs_program = "zfs"
        bzfs.Params(args)
        args.zfs_program = "sudo"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zfs_program = "doas"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zfs_program = "exec"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zfs_program = "time"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zfs_program = "zstd"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.sudo_program = "sudo"
        args.shell_program = "bash"
        args.ssh_program = "ssh"
        args.zfs_program = "zfs"
        args.zpool_program = "zpool"
        args.compression_program = "zstd"
        args.pv_program = "pv"
        args.ps_program = "ps"
        bzfs.Params(args)
        args.pv_program = "sudo"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.pv_program = "bash"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.pv_program = "ssh"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.pv_program = "zfs"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.pv_program = "zpool"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.compression_program = "rm"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.pv_program = "awk"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)

    def test_split_args(self) -> None:
        params = bzfs.Params(argparser_parse_args(args=["src", "dst"]))
        self.assertEqual([], params.split_args(""))
        self.assertEqual([], params.split_args("  "))
        self.assertEqual(["foo", "bar", "baz"], params.split_args("foo  bar baz"))
        self.assertEqual(["foo", "bar", "baz"], params.split_args(" foo  bar\tbaz "))
        self.assertEqual(["foo", "bar", "baz"], params.split_args("foo", "bar", "baz"))
        self.assertEqual(["foo", "baz"], params.split_args("foo", "", "baz"))
        self.assertEqual(["foo", "bar", "baz"], params.split_args("foo", ["bar", "", "baz"]))
        self.assertEqual(["foo"], params.split_args("foo", []))
        with self.assertRaises(SystemExit):
            params.split_args("'foo'")
        with self.assertRaises(SystemExit):
            params.split_args("`foo`")
        with self.assertRaises(SystemExit):
            params.split_args("$foo")
        self.assertEqual(["'foo'"], params.split_args("'foo'", allow_all=True))
        with self.assertRaises(SystemExit):
            params.split_args('"foo"')
        self.assertEqual(['"foo"'], params.split_args('"foo"', allow_all=True))
        self.assertEqual(["foo", "bar baz"], params.split_args("foo", "bar baz"))
        self.assertEqual(["foo", "bar\tbaz"], params.split_args("foo", "bar\tbaz"))
        self.assertEqual(["foo", "bar\nbaz"], params.split_args("foo", "bar\nbaz"))
        self.assertEqual(["foo", "bar\rbaz"], params.split_args("foo", "bar\rbaz"))

    def test_compile_regexes(self) -> None:
        def _assertFullMatch(text: str, regex: str, re_suffix: str = "", expected: bool = True) -> None:
            match = bzfs.compile_regexes([regex], suffix=re_suffix)[0][0].fullmatch(text)
            if expected:
                self.assertTrue(match)
            else:
                self.assertFalse(match)

        def assertFullMatch(text: str, regex: str, re_suffix: str = "") -> None:
            _assertFullMatch(text=text, regex=regex, re_suffix=re_suffix, expected=True)

        def assertNotFullMatch(text: str, regex: str, re_suffix: str = "") -> None:
            _assertFullMatch(text=text, regex=regex, re_suffix=re_suffix, expected=False)

        re_suffix = bzfs.Job().re_suffix
        assertFullMatch("foo", "foo")
        assertNotFullMatch("xfoo", "foo")
        assertNotFullMatch("fooy", "foo")
        assertNotFullMatch("foo/bar", "foo")
        assertFullMatch("foo", "foo$")
        assertFullMatch("foo", ".*")
        assertFullMatch("foo/bar", ".*")
        assertFullMatch("foo", ".*", re_suffix)
        assertFullMatch("foo/bar", ".*", re_suffix)
        assertFullMatch("foo", "foo", re_suffix)
        assertFullMatch("foo/bar", "foo", re_suffix)
        assertFullMatch("foo/bar/baz", "foo", re_suffix)
        assertFullMatch("foo", "foo$", re_suffix)
        assertFullMatch("foo$", "foo\\$", re_suffix)
        assertFullMatch("foo", "!foo", re_suffix)
        assertFullMatch("foo", "!foo")
        with self.assertRaises(re.error):
            bzfs.compile_regexes(["fo$o"], re_suffix)

    def test_filter_lines(self) -> None:
        input_list = ["apple\tred", "banana\tyellow", "cherry\tred", "date\tbrown"]

        # Empty input_set
        self.assertListEqual([], bzfs.filter_lines(input_list, set()))

        # input_set with some matching elements
        self.assertListEqual(["apple\tred", "cherry\tred"], bzfs.filter_lines(input_list, {"apple", "cherry"}))

        # input_set with no matching elements
        self.assertListEqual([], bzfs.filter_lines(input_list, {"grape", "kiwi"}))

        # Empty input_list
        self.assertListEqual([], bzfs.filter_lines([], {"apple"}))

        # input_set with all elements matching
        self.assertListEqual(input_list, bzfs.filter_lines(input_list, {"apple", "banana", "cherry", "date"}))

    def test_filter_lines_except(self) -> None:
        input_list = ["apple\tred", "banana\tyellow", "cherry\tred", "date\tbrown"]

        # Empty input_set
        self.assertListEqual(input_list, bzfs.filter_lines_except(input_list, set()))

        # input_set with some elements to exclude
        self.assertListEqual(["banana\tyellow", "date\tbrown"], bzfs.filter_lines_except(input_list, {"apple", "cherry"}))

        # input_set with no elements present in the input_list (exclude nothing new)
        self.assertListEqual(input_list, bzfs.filter_lines_except(input_list, {"grape", "kiwi"}))

        # Empty input_list
        self.assertListEqual([], bzfs.filter_lines_except([], {"apple"}))

        # input_set with all elements from input_list (exclude all)
        self.assertListEqual([], bzfs.filter_lines_except(input_list, {"apple", "banana", "cherry", "date"}))

    def test_fix_send_recv_opts(self) -> None:
        params = bzfs.Params(argparser_parse_args(args=["src", "dst"]))
        self.assertEqual([], params.fix_recv_opts(["-n"]))
        self.assertEqual([], params.fix_recv_opts(["--dryrun", "-n"]))
        self.assertEqual([""], params.fix_recv_opts([""]))
        self.assertEqual([], params.fix_recv_opts([]))
        self.assertEqual(["-"], params.fix_recv_opts(["-"]))
        self.assertEqual(["-h"], params.fix_recv_opts(["-hn"]))
        self.assertEqual(["-h"], params.fix_recv_opts(["-nh"]))
        self.assertEqual(["--Fvhn"], params.fix_recv_opts(["--Fvhn"]))
        self.assertEqual(["foo"], params.fix_recv_opts(["foo"]))
        self.assertEqual(["v", "n", "F"], params.fix_recv_opts(["v", "n", "F"]))
        self.assertEqual(["-o", "-n"], params.fix_recv_opts(["-o", "-n"]))
        self.assertEqual(["-o", "-n"], params.fix_recv_opts(["-o", "-n", "-n"]))
        self.assertEqual(["-x", "--dryrun"], params.fix_recv_opts(["-x", "--dryrun"]))
        self.assertEqual(["-x", "--dryrun"], params.fix_recv_opts(["-x", "--dryrun", "-n"]))
        self.assertEqual(["-x"], params.fix_recv_opts(["-x"]))

        self.assertEqual([], params.fix_send_opts(["-n"]))
        self.assertEqual([], params.fix_send_opts(["--dryrun", "-n", "-ed"]))
        self.assertEqual([], params.fix_send_opts(["-I", "s1"]))
        self.assertEqual(["--raw"], params.fix_send_opts(["-i", "s1", "--raw"]))
        self.assertEqual(["-X", "d1,d2"], params.fix_send_opts(["-X", "d1,d2"]))
        self.assertEqual(
            ["--exclude", "d1,d2", "--redact", "b1"], params.fix_send_opts(["--exclude", "d1,d2", "--redact", "b1"])
        )

    def test_xprint(self) -> None:
        log = logging.getLogger()
        bzfs.xprint(log, "foo")
        bzfs.xprint(log, "foo", run=True)
        bzfs.xprint(log, "foo", run=False)
        bzfs.xprint(log, "foo", file=sys.stdout)
        bzfs.xprint(log, "")
        bzfs.xprint(log, "", run=True)
        bzfs.xprint(log, "", run=False)
        bzfs.xprint(log, None)

    def test_pid_exists(self) -> None:
        self.assertTrue(bzfs.pid_exists(os.getpid()))
        self.assertFalse(bzfs.pid_exists(-1))
        self.assertFalse(bzfs.pid_exists(0))
        # This fake PID is extremely unlikely to be alive because it is orders of magnitude higher than the typical
        # range of process IDs on Unix-like systems:
        fake_pid = 2**31 - 1  # 2147483647
        self.assertFalse(bzfs.pid_exists(fake_pid))

        # Process 1 is typically owned by root; if not running as root, this should return True because it raises EPERM
        if os.getuid() != 0:
            self.assertTrue(bzfs.pid_exists(1))

    @patch("os.kill")
    def test_pid_exists_with_unexpected_oserror_returns_none(self, mock_kill: MagicMock) -> None:
        # Simulate an unexpected OSError (e.g., EINVAL) and verify that pid_exists returns None.
        err = OSError()
        err.errno = errno.EINVAL
        mock_kill.side_effect = err
        self.assertIsNone(bzfs.pid_exists(1234))

    def test_delete_stale_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            new_socket_file = os.path.join(tmpdir, "s_new_socket_file")
            Path(new_socket_file).touch()
            stale_socket_file = os.path.join(tmpdir, "s_stale_socket_file")
            Path(stale_socket_file).touch()
            one_hundred_days_ago = time.time() - 100 * 24 * 60 * 60
            os.utime(stale_socket_file, (one_hundred_days_ago, one_hundred_days_ago))
            sdir = os.path.join(tmpdir, "s_dir")
            os.mkdir(sdir)
            non_socket_file = os.path.join(tmpdir, "f")
            Path(non_socket_file).touch()

            bzfs.delete_stale_files(tmpdir, "s", millis=31 * 24 * 60 * 60 * 1000)

            self.assertTrue(os.path.exists(new_socket_file))
            self.assertFalse(os.path.exists(stale_socket_file))
            self.assertTrue(os.path.exists(sdir))
            self.assertTrue(os.path.exists(non_socket_file))

    def test_delete_stale_files_ssh_alive(self) -> None:
        """Socket file with a live pid (current process) should NOT be removed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            socket_name = f"s{os.getpid()}foo"
            socket_path = os.path.join(tmpdir, socket_name)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)  # create a real UNIX domain socket
            try:
                sock.bind(socket_path)
                time.sleep(0.001)  # sleep to ensure the file's mtime is in the past
                # Call delete_stale_files with millis=0 to mark all files as stale.
                bzfs.delete_stale_files(tmpdir, "s", millis=0, ssh=True)
                # The file should still exist because the current process is alive.
                self.assertTrue(os.path.exists(socket_path))
            finally:
                sock.close()
                if os.path.exists(socket_path):
                    os.remove(socket_path)

    def test_delete_stale_files_ssh_stale(self) -> None:
        """Socket file with a non-existent pid should be removed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # This fake PID is extremely unlikely to be alive because it is orders of magnitude higher than the typical
            # range of process IDs on Unix-like systems:
            fake_pid = 2**31 - 1  # 2147483647

            socket_name = f"s{fake_pid}foo"
            socket_path = os.path.join(tmpdir, socket_name)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)  # create a real UNIX domain socket
            try:
                sock.bind(socket_path)
                time.sleep(0.001)  # sleep to ensure the file's mtime is in the past
                bzfs.delete_stale_files(tmpdir, "s", millis=0, ssh=True)
                # The file should be removed because the fake pid is not alive.
                self.assertFalse(os.path.exists(socket_path))
            finally:
                sock.close()
                if os.path.exists(socket_path):
                    os.remove(socket_path)

    def test_delete_stale_files_ssh_regular_file(self) -> None:
        """A regular file should be removed even when ssh=True."""
        with tempfile.TemporaryDirectory() as tmpdir:
            regular_file = os.path.join(tmpdir, "s_regular_file")
            Path(regular_file).touch()
            bzfs.delete_stale_files(tmpdir, "s", millis=0, ssh=True)
            self.assertFalse(os.path.exists(regular_file))

    def test_set_last_modification_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file = os.path.join(tmpdir, "foo")
            bzfs.set_last_modification_time(file, unixtime_in_secs=0)
            self.assertEqual(0, round(os.stat(file).st_mtime))
            bzfs.set_last_modification_time(file, unixtime_in_secs=0)
            self.assertEqual(0, round(os.stat(file).st_mtime))
            bzfs.set_last_modification_time(file, unixtime_in_secs=1000, if_more_recent=True)
            self.assertEqual(1000, round(os.stat(file).st_mtime))
            bzfs.set_last_modification_time(file, unixtime_in_secs=0, if_more_recent=True)
            self.assertEqual(1000, round(os.stat(file).st_mtime))
            bzfs.set_last_modification_time_safe(file, unixtime_in_secs=1001, if_more_recent=True)
            self.assertEqual(1001, round(os.stat(file).st_mtime))

    def test_set_last_modification_time_with_FileNotFoundError(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file = os.path.join(tmpdir, "foo")
            with patch("bzfs_main.bzfs.os_utime", side_effect=FileNotFoundError):
                with self.assertRaises(FileNotFoundError):
                    bzfs.set_last_modification_time(file + "nonexisting", unixtime_in_secs=1001, if_more_recent=False)
                file = os.path.join(file, "x", "nonexisting2")
                bzfs.set_last_modification_time_safe(file, unixtime_in_secs=1001, if_more_recent=False)

    def test_recv_option_property_names(self) -> None:
        def names(lst: List[str]) -> Set[str]:
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
        self.assertSetEqual({"name1"}, names(["-o", "name1"]))
        self.assertSetEqual({""}, names(["-o", "=value1"]))
        self.assertSetEqual({""}, names(["-o", ""]))
        self.assertSetEqual({"=value1"}, names(["-x", "=value1"]))
        self.assertSetEqual({""}, names(["-x", ""]))
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

    def test_fix_solaris_raw_mode(self) -> None:
        self.assertListEqual(["-w", "none"], bzfs.fix_solaris_raw_mode(["-w"]))
        self.assertListEqual(["-w", "none"], bzfs.fix_solaris_raw_mode(["--raw"]))
        self.assertListEqual(["-w", "none"], bzfs.fix_solaris_raw_mode(["-w", "none"]))
        self.assertListEqual(["-w", "compress"], bzfs.fix_solaris_raw_mode(["-w", "compress"]))
        self.assertListEqual(["-w", "compress"], bzfs.fix_solaris_raw_mode(["-w", "--compressed"]))
        self.assertListEqual(["-w", "crypto"], bzfs.fix_solaris_raw_mode(["-w", "crypto"]))
        self.assertListEqual(["-w", "none", "foo"], bzfs.fix_solaris_raw_mode(["-w", "foo"]))
        self.assertListEqual(["-F"], bzfs.fix_solaris_raw_mode(["-F"]))

    def test_logdir_basename_prefix(self) -> None:
        """Basename of --log-dir must start with prefix 'bzfs-logs'"""
        logdir = os.path.join(bzfs.get_home_directory(), "bzfs-logs-tmp")
        try:
            bzfs.LogParams(bzfs.argument_parser().parse_args(args=["src", "dst", "--log-dir=" + logdir]))
            self.assertTrue(os.path.exists(logdir))
        finally:
            shutil.rmtree(logdir, ignore_errors=True)

        logdir = os.path.join(bzfs.get_home_directory(), "bzfs-tmp")
        with self.assertRaises(SystemExit):
            bzfs.LogParams(bzfs.argument_parser().parse_args(args=["src", "dst", "--log-dir=" + logdir]))
        self.assertFalse(os.path.exists(logdir))

    def test_get_logger_with_cleanup(self) -> None:
        def check(log: Logger, files: Set[str]) -> None:
            files_todo = files.copy()
            for handler in log.handlers:
                if isinstance(handler, logging.FileHandler):
                    self.assertIn(handler.baseFilename, files_todo)
                    files_todo.remove(handler.baseFilename)
            self.assertEqual(0, len(files_todo))

        prefix = "test_get_logger:"
        args = argparser_parse_args(args=["src", "dst"])
        root_logger = logging.getLogger()
        log_params = bzfs.LogParams(args)
        log = bzfs.get_logger(log_params, args, root_logger)
        self.assertTrue(log is root_logger)
        log.info(prefix + "aaa1")

        args = argparser_parse_args(args=["src", "dst"])
        log_params = bzfs.LogParams(args)
        log = bzfs.get_logger(log_params, args)
        log.log(bzfs.log_stderr, "%s", prefix + "bbbe1")
        log.log(bzfs.log_stdout, "%s", prefix + "bbbo1")
        log.info("%s", prefix + "bbb3")
        log.setLevel(logging.WARNING)
        log.log(bzfs.log_stderr, "%s", prefix + "bbbe2")
        log.log(bzfs.log_stdout, "%s", prefix + "bbbo2")
        log.info("%s", prefix + "bbb4")
        log.log(log_trace, "%s", prefix + "bbb5")
        log.setLevel(log_trace)
        log.log(log_trace, "%s", prefix + "bbb6")
        files = {os.path.abspath(log_params.log_file)}
        check(log, files)

        args = argparser_parse_args(args=["src", "dst", "-v"])
        log_params = bzfs.LogParams(args)
        log = bzfs.get_logger(log_params, args)
        self.assertIsNotNone(log)
        files.add(os.path.abspath(log_params.log_file))
        check(log, files)

        log.addFilter(lambda record: True)  # dummy
        bzfs.reset_logger()
        files.clear()
        check(log, files)

        args = argparser_parse_args(args=["src", "dst", "-v", "-v"])
        log_params = bzfs.LogParams(args)
        log = bzfs.get_logger(log_params, args)
        self.assertIsNotNone(log)
        files.add(os.path.abspath(log_params.log_file))
        check(log, files)

        args = argparser_parse_args(args=["src", "dst", "--quiet"])
        log_params = bzfs.LogParams(args)
        log = bzfs.get_logger(log_params, args)
        self.assertIsNotNone(log)
        files.add(os.path.abspath(log_params.log_file))
        check(log, files)

        bzfs.reset_logger()

    def test_get_syslog_address(self) -> None:
        udp = socket.SOCK_DGRAM
        tcp = socket.SOCK_STREAM
        self.assertEqual((("localhost", 514), udp), bzfs.get_syslog_address("localhost:514", "UDP"))
        self.assertEqual((("localhost", 514), tcp), bzfs.get_syslog_address("localhost:514", "TCP"))
        self.assertEqual(("/dev/log", None), bzfs.get_syslog_address("/dev/log", "UDP"))
        self.assertEqual(("/dev/log", None), bzfs.get_syslog_address("/dev/log", "TCP"))

    def test_validate_log_config_variable(self) -> None:
        self.assertIsNone(bzfs.validate_log_config_variable("name:value"))
        for var in ["noColon", ":noName", "$n:v", "{:v", "}:v", "", "  ", "\t", "a\tb:v", "spa ce:v", '"k":v', "'k':v"]:
            self.assertIsNotNone(bzfs.validate_log_config_variable(var))

    def test_has_siblings(self) -> None:
        self.assertFalse(bzfs.has_siblings([]))
        self.assertFalse(bzfs.has_siblings(["a"]))
        self.assertFalse(bzfs.has_siblings(["a", "a/b"]))
        self.assertFalse(bzfs.has_siblings(["a", "a/b", "a/b/c"]))
        self.assertTrue(bzfs.has_siblings(["a", "b"]))
        self.assertTrue(bzfs.has_siblings(["a", "a/b", "a/d"]))
        self.assertTrue(bzfs.has_siblings(["a", "a/b", "a/b/c", "a/b/d"]))
        self.assertTrue(bzfs.has_siblings(["a/b/c", "d/e/f"]))  # multiple root datasets can be processed in parallel
        self.assertFalse(bzfs.has_siblings(["a", "a/b/c"]))
        self.assertFalse(bzfs.has_siblings(["a", "a/b/c/d"]))
        self.assertTrue(bzfs.has_siblings(["a", "a/b/c", "a/b/d"]))
        self.assertTrue(bzfs.has_siblings(["a", "a/b/c/d", "a/b/c/e"]))

    def test_is_descendant(self) -> None:
        self.assertTrue(bzfs.is_descendant("pool/fs/child", "pool/fs"))
        self.assertTrue(bzfs.is_descendant("pool/fs/child/grandchild", "pool/fs"))
        self.assertTrue(bzfs.is_descendant("a/b/c/d", "a/b"))
        self.assertTrue(bzfs.is_descendant("pool/fs", "pool/fs"))
        self.assertFalse(bzfs.is_descendant("pool/otherfs", "pool/fs"))
        self.assertFalse(bzfs.is_descendant("a/c", "a/b"))
        self.assertFalse(bzfs.is_descendant("pool/fs", "pool/fs/child"))
        self.assertFalse(bzfs.is_descendant("a/b", "a/b/c"))
        self.assertFalse(bzfs.is_descendant("tank1/data", "tank2/backup"))
        self.assertFalse(bzfs.is_descendant("a", ""))
        self.assertFalse(bzfs.is_descendant("", "a"))
        self.assertTrue(bzfs.is_descendant("", ""))
        self.assertFalse(bzfs.is_descendant("pool/fs-backup", "pool/fs"))
        self.assertTrue(bzfs.is_descendant("pool/fs", "pool"))

    def test_validate_default_shell(self) -> None:
        args = argparser_parse_args(args=["src", "dst"])
        p = bzfs.Params(args)
        remote = bzfs.Remote("src", args, p)
        bzfs.validate_default_shell("/bin/sh", remote)
        bzfs.validate_default_shell("/bin/bash", remote)
        with self.assertRaises(SystemExit):
            bzfs.validate_default_shell("/bin/csh", remote)
        with self.assertRaises(SystemExit):
            bzfs.validate_default_shell("/bin/tcsh", remote)

    def test_filter_datasets_with_test_mode_is_false(self) -> None:
        # test with Job.is_test_mode == False for coverage with the assertion therein disabled
        args = argparser_parse_args(args=["src", "dst"])
        p = bzfs.Params(args)
        log_params = bzfs.LogParams(args)
        try:
            job = bzfs.Job()
            job.params = bzfs.Params(args, log_params=log_params, log=bzfs.get_logger(log_params, args))
            src = bzfs.Remote("src", args, p)
            job.filter_datasets(src, ["dataset1"])
        finally:
            bzfs.reset_logger()

    def test_is_zfs_dataset_busy_match(self) -> None:
        def is_busy(proc: str, dataset: str, busy_if_send: bool = True) -> bool:
            return bzfs.Job().is_zfs_dataset_busy([proc], dataset, busy_if_send=busy_if_send)

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

    def assert_human_readable_float(self, actual: float, expected: str) -> None:
        self.assertEqual(bzfs.human_readable_float(actual), expected)
        self.assertEqual(bzfs.human_readable_float(-actual), "-" + expected)

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
        self.assertEqual(bzfs.human_readable_float(0.0), "0")
        self.assertEqual(bzfs.human_readable_float(-0.0), "0")
        self.assertEqual(bzfs.human_readable_float(0.001), "0")
        self.assertEqual(bzfs.human_readable_float(-0.001), "0")

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
        self.assertEqual("0 B", bzfs.human_readable_bytes(0))
        self.assertEqual("581 B", bzfs.human_readable_bytes(0.567 * 1024**1))
        self.assertEqual("2 KiB", bzfs.human_readable_bytes(2 * 1024**1))
        self.assertEqual("1 MiB", bzfs.human_readable_bytes(1 * 1024**2))
        self.assertEqual("1 GiB", bzfs.human_readable_bytes(1 * 1024**3))
        self.assertEqual("1 TiB", bzfs.human_readable_bytes(1 * 1024**4))
        self.assertEqual("1 PiB", bzfs.human_readable_bytes(1 * 1024**5))
        self.assertEqual("1 EiB", bzfs.human_readable_bytes(1 * 1024**6))
        self.assertEqual("1 ZiB", bzfs.human_readable_bytes(1 * 1024**7))
        self.assertEqual("1 YiB", bzfs.human_readable_bytes(1 * 1024**8))
        self.assertEqual("1 RiB", bzfs.human_readable_bytes(1 * 1024**9))
        self.assertEqual("1 QiB", bzfs.human_readable_bytes(1 * 1024**10))
        self.assertEqual("1024 QiB", bzfs.human_readable_bytes(1 * 1024**11))
        self.assertEqual("3 B", bzfs.human_readable_bytes(2.567, precision=0))
        self.assertEqual("2.6 B", bzfs.human_readable_bytes(2.567, precision=1))
        self.assertEqual("2.57 B", bzfs.human_readable_bytes(2.567, precision=2))
        self.assertEqual("2.57 B", bzfs.human_readable_bytes(2.567, precision=None))

    def test_human_readable_duration(self) -> None:
        ms = 1000_000
        self.assertEqual("0ns", bzfs.human_readable_duration(0, long=False))
        self.assertEqual("3ns", bzfs.human_readable_duration(3, long=False))
        self.assertEqual("3μs", bzfs.human_readable_duration(3 * 1000, long=False))
        self.assertEqual("3ms", bzfs.human_readable_duration(3 * ms, long=False))
        self.assertEqual("1s", bzfs.human_readable_duration(1000 * ms, long=False))
        self.assertEqual("3s", bzfs.human_readable_duration(3000 * ms, long=False))
        self.assertEqual("3m", bzfs.human_readable_duration(3000 * 60 * ms, long=False))
        self.assertEqual("3h", bzfs.human_readable_duration(3000 * 60 * 60 * ms, long=False))
        self.assertEqual("1.25d", bzfs.human_readable_duration(3000 * 60 * 60 * 10 * ms, long=False))
        self.assertEqual("12.5d", bzfs.human_readable_duration(3000 * 60 * 60 * 100 * ms, long=False))
        self.assertEqual("1250d", bzfs.human_readable_duration(3000 * 60 * 60 * 10000 * ms, long=False))
        self.assertEqual("125ns", bzfs.human_readable_duration(125, long=False))
        self.assertEqual("125μs", bzfs.human_readable_duration(125 * 1000, long=False))
        self.assertEqual("125ms", bzfs.human_readable_duration(125 * 1000 * 1000, long=False))
        self.assertEqual("2.08m", bzfs.human_readable_duration(125 * 1000 * 1000 * 1000, long=False))

        self.assertEqual("0s", bzfs.human_readable_duration(0, unit="s", long=False))
        self.assertEqual("3s", bzfs.human_readable_duration(3, unit="s", long=False))
        self.assertEqual("3m", bzfs.human_readable_duration(3 * 60, unit="s", long=False))
        self.assertEqual("0h", bzfs.human_readable_duration(0, unit="h", long=False))
        self.assertEqual("3h", bzfs.human_readable_duration(3, unit="h", long=False))
        self.assertEqual("7.5d", bzfs.human_readable_duration(3 * 60, unit="h", long=False))
        self.assertEqual("0.3ns", bzfs.human_readable_duration(0.3, long=False))
        self.assertEqual("300ns", bzfs.human_readable_duration(0.3, unit="μs", long=False))
        self.assertEqual("300μs", bzfs.human_readable_duration(0.3, unit="ms", long=False))
        self.assertEqual("300ms", bzfs.human_readable_duration(0.3, unit="s", long=False))
        self.assertEqual("18s", bzfs.human_readable_duration(0.3, unit="m", long=False))
        self.assertEqual("18m", bzfs.human_readable_duration(0.3, unit="h", long=False))
        self.assertEqual("7.2h", bzfs.human_readable_duration(0.3, unit="d", long=False))
        self.assertEqual("259ns", bzfs.human_readable_duration(3 / 1000_000_000_000, unit="d", long=False))
        self.assertEqual("0.26ns", bzfs.human_readable_duration(3 / 1000_000_000_000_000, unit="d", long=False))
        with self.assertRaises(ValueError):
            bzfs.human_readable_duration(3, unit="hhh", long=False)  # invalid unit

        self.assertEqual("0s (0 seconds)", bzfs.human_readable_duration(0, unit="s", long=True))
        self.assertEqual("3m (180 seconds)", bzfs.human_readable_duration(3 * 60, unit="s", long=True))
        self.assertEqual("3h (10800 seconds)", bzfs.human_readable_duration(3 * 60, unit="m", long=True))
        self.assertEqual("3ms (0 seconds)", bzfs.human_readable_duration(3, unit="ms", long=True))
        self.assertEqual("3ms (0 seconds)", bzfs.human_readable_duration(3 * 1000 * 1000, long=True))

        self.assertEqual("0s (0 seconds)", bzfs.human_readable_duration(-0, unit="s", long=True))
        self.assertEqual("-3m (-180 seconds)", bzfs.human_readable_duration(-3 * 60, unit="s", long=True))
        self.assertEqual("-3h (-10800 seconds)", bzfs.human_readable_duration(-3 * 60, unit="m", long=True))
        self.assertEqual("-3ms (0 seconds)", bzfs.human_readable_duration(-3, unit="ms", long=True))
        self.assertEqual("-3ms (0 seconds)", bzfs.human_readable_duration(-3 * 1000 * 1000, long=True))

        self.assertEqual("3ns", bzfs.human_readable_duration(2.567, precision=0))
        self.assertEqual("2.6ns", bzfs.human_readable_duration(2.567, precision=1))
        self.assertEqual("2.57ns", bzfs.human_readable_duration(2.567, precision=2))
        self.assertEqual("2.57ns", bzfs.human_readable_duration(2.567, precision=None))

    def test_pv_cmd(self) -> None:
        args = argparser_parse_args(args=["src", "dst"])
        log_params = bzfs.LogParams(args)
        try:
            job = bzfs.Job()
            job.params = bzfs.Params(args, log_params=log_params, log=bzfs.get_logger(log_params, args))
            job.params.available_programs = {"src": {"pv": "pv"}}
            self.assertNotEqual("cat", job.pv_cmd("src", 1024 * 1024, "foo"))
        finally:
            bzfs.reset_logger()

    def test_pv_size_to_bytes(self) -> None:
        def pv_size_to_bytes(line: str) -> int:
            num_bytes, _ = bzfs.pv_size_to_bytes(line)
            return num_bytes

        self.assertEqual(800, pv_size_to_bytes("800B foo"))
        self.assertEqual(1, pv_size_to_bytes("8b"))
        self.assertEqual(round(4.12 * 1024), pv_size_to_bytes("4.12 KiB"))
        self.assertEqual(round(46.2 * 1024**3), pv_size_to_bytes("46,2GiB"))
        self.assertEqual(round(46.2 * 1024**3), pv_size_to_bytes("46.2GiB"))
        self.assertEqual(round(46.2 * 1024**3), pv_size_to_bytes("46" + bzfs.arabic_decimal_separator + "2GiB"))
        self.assertEqual(2 * 1024**2, pv_size_to_bytes("2 MiB"))
        self.assertEqual(1000**2, pv_size_to_bytes("1 MB"))
        self.assertEqual(1024**3, pv_size_to_bytes("1 GiB"))
        self.assertEqual(1024**3, pv_size_to_bytes("8 Gib"))
        self.assertEqual(1000**3, pv_size_to_bytes("8 Gb"))
        self.assertEqual(1024**4, pv_size_to_bytes("1 TiB"))
        self.assertEqual(1024**5, pv_size_to_bytes("1 PiB"))
        self.assertEqual(1024**6, pv_size_to_bytes("1 EiB"))
        self.assertEqual(1024**7, pv_size_to_bytes("1 ZiB"))
        self.assertEqual(1024**8, pv_size_to_bytes("1 YiB"))
        self.assertEqual(1024**9, pv_size_to_bytes("1 RiB"))
        self.assertEqual(1024**10, pv_size_to_bytes("1 QiB"))

        self.assertEqual(0, pv_size_to_bytes("foo"))
        self.assertEqual(0, pv_size_to_bytes("46-2GiB"))
        with self.assertRaises(ValueError):
            pv_size_to_bytes("4.12 KiB/s")

    def test_count_num_bytes_transferred_by_pv(self) -> None:
        default_opts = bzfs.argument_parser().get_default("pv_program_opts")
        self.assertTrue(isinstance(default_opts, str) and default_opts)
        pv_fd, pv_file = tempfile.mkstemp(prefix="test_bzfs.test_count_num_byte", suffix=".pv")
        os.close(pv_fd)
        cmd = (
            "dd if=/dev/zero bs=1k count=16 |"
            f" LC_ALL=C pv {default_opts} --size 16K --name=275GiB --force 2> {pv_file} >/dev/null"
        )
        try:
            subprocess.run(cmd, shell=True, check=True)
            num_bytes = bzfs.count_num_bytes_transferred_by_zfs_send(pv_file)
            print("pv_log_file: " + "\n".join(bzfs.tail(pv_file, 10)))
            self.assertEqual(16 * 1024, num_bytes)
        finally:
            Path(pv_file).unlink(missing_ok=True)

    def test_count_num_bytes_transferred_by_zfs_send(self) -> None:
        self.assertEqual(0, bzfs.count_num_bytes_transferred_by_zfs_send("/tmp/nonexisting_bzfs_test_file"))
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                pv1_fd, pv1 = tempfile.mkstemp(prefix="test_bzfs.test_count_num_byte", suffix=".pv")
                os.write(pv1_fd, ": 800B foo".encode("utf-8"))
                pv2 = pv1 + bzfs.pv_file_thread_separator + "001"
                with open(pv2, "w", encoding="utf-8") as fd:
                    fd.write("1 KB\r\n")
                    fd.write("2 KiB:20 KB\r")
                    fd.write("3 KiB:300 KB\r\n")
                    fd.write("4 KiB:4000 KB\r")
                    fd.write("4 KiB:50000 KB\r")
                    fd.write("4 KiB:600000 KB\r" + ("\n" if i == 0 else ""))
                pv3 = pv1 + bzfs.pv_file_thread_separator + "002"
                os.makedirs(pv3, exist_ok=True)
                try:
                    num_bytes = bzfs.count_num_bytes_transferred_by_zfs_send(pv1)
                    self.assertEqual(800 + 300 * 1000 + 600000 * 1000, num_bytes)
                finally:
                    os.remove(pv1)
                    os.remove(pv2)
                    shutil.rmtree(pv3)

    def test_progress_reporter_parse_pv_line(self) -> None:
        args = argparser_parse_args(args=["src", "dst"])
        p = bzfs.Params(args)
        reporter = bzfs.ProgressReporter(p, use_select=False, progress_update_intervals=None)
        curr_time_nanos = 123
        eols = ["", "\n", "\r", "\r\n"]
        for eol in eols:
            with stop_on_failure_subtest(i=eols.index(eol)):
                # normal intermediate line
                line = "125 GiB: 2,71GiB 0:00:08 [98,8MiB/s] [ 341MiB/s] [>                ]  2% ETA 0:06:03 ETA 17:27:49"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(2.71 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), eta_timestamp_nanos)
                self.assertEqual("[>                ]  2% ETA 0:06:03 ETA 17:27:49", line_tail)

                # intermediate line with duration ETA that contains days
                line = "98 GiB/ 0 B/  98 GiB: 93.1GiB 0:12:12 [ 185MiB/s] [ 130MiB/s] [==>  ] 94% ETA 2+0:00:39 ETA 17:55:48"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(93.1 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (2 * 86400 + 39), eta_timestamp_nanos)
                self.assertEqual("[==>  ] 94% ETA 2+0:00:39 ETA 17:55:48", line_tail)

                # intermediate line with duration ETA that contains other days syntax and timestamp ETA that contains days
                line = "98 GiB/ 0 B/  98 GiB: 93.1GiB 0:12:12 ( 185MiB/s) ( 130MiB/s) [==>  ] ETA 1:00:07:16 ETA 2025-01-23 14:06:02"  # noqa: E501
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(93.1 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (1 * 86400 + 7 * 60 + 16), eta_timestamp_nanos)
                self.assertEqual("[==>  ] ETA 1:00:07:16 ETA 2025-01-23 14:06:02", line_tail)

                # intermediate line with duration ETA that contains other days syntax and timestamp ETA using FIN marker
                line = "98 GiB/ 0 B/  98 GiB: 93.1GiB 9:0:12:12 [ 185MiB/s] [ 130MiB/s] [==>  ] ETA 1:00:07:16 FIN 2025-01-23 14:06:02"  # noqa: E501
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(93.1 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (1 * 86400 + 7 * 60 + 16), eta_timestamp_nanos)
                self.assertEqual("[==>  ] ETA 1:00:07:16 FIN 2025-01-23 14:06:02", line_tail)

                # final line on transfer completion does not contain duration ETA
                line = "98 GiB/ 0 B/  98 GiB: 98,1GiB 0:12:39 [ 132MiB/s] [ 132MiB/s] [=====>] 100%             ETA 17:55:37"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(98.1 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos, eta_timestamp_nanos)
                self.assertEqual("[=====>] 100%             ETA 17:55:37", line_tail)

                # final line on transfer completion does not contain duration ETA
                line = "12.6KiB: 44.2KiB 0:00:00 [3.14MiB/s] [3.14MiB/s] [===================] 350%             ETA 14:48:27"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(44.2 * 1024), num_bytes)
                self.assertEqual(curr_time_nanos, eta_timestamp_nanos)
                self.assertEqual("[===================] 350%             ETA 14:48:27", line_tail)

                # missing from --pv--program-opts: --timer, --rate, --average-rate
                line = "125 GiB: 2.71GiB[ >                    ]  2% ETA 0:06:03 ETA 17:27:49"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(2.71 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), eta_timestamp_nanos)
                self.assertEqual("[ >                    ]  2% ETA 0:06:03 ETA 17:27:49", line_tail)

                # missing from --pv--program-opts: --rate, --average-rate
                line = "125 GiB: 2.71GiB 0:00:08 [ >                    ]  2% ETA 0:06:03 ETA 17:27:49"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(2.71 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), eta_timestamp_nanos)
                self.assertEqual("0:00:08 [ >                    ]  2% ETA 0:06:03 ETA 17:27:49", line_tail)

                # intermediate line with square brackets after the first ETA (not sure if this actually occurs in the wild)
                line = "125 GiB: 2,71GiB 0:00:08 [98,8MiB/s] [ 341MiB/s] [>            ]  2% ETA 0:06:03 ] [ ETA 17:27:49]"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(2.71 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), eta_timestamp_nanos)
                self.assertEqual("[>            ]  2% ETA 0:06:03 ] [ ETA 17:27:49]", line_tail)

                # zero line with final line on transfer completion does not contain duration ETA
                line = "275GiB: 0.00 B 0:00:00 [0.00 B/s] [0.00 B/s] [>                   ]  0%             ETA 03:24:32"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(0, num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * 0, eta_timestamp_nanos)
                self.assertEqual("[>                   ]  0%             ETA 03:24:32", line_tail)

                for line in eols:
                    num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                    self.assertEqual(0, num_bytes)
                    self.assertEqual(curr_time_nanos, eta_timestamp_nanos)
                    self.assertEqual("", line_tail)

    def test_progress_reporter_update_transfer_stat(self) -> None:
        args = argparser_parse_args(args=["src", "dst"])
        p = bzfs.Params(args)
        curr_time_nanos = 123
        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                reporter = bzfs.ProgressReporter(p, use_select=False, progress_update_intervals=None)
                eta = bzfs.ProgressReporter.TransferStat.ETA(timestamp_nanos=0, seq_nr=0, line_tail="")
                bytes_in_flight = 789
                stat = bzfs.ProgressReporter.TransferStat(bytes_in_flight=bytes_in_flight, eta=eta)
                line = "125 GiB: 2,71GiB 0:00:08 [98,8MiB/s] [ 341MiB/s] [>                   ]  2% ETA 0:06:03 ETA 17:27:49"
                expected_bytes = round(2.71 * 1024**3)
                if i > 0:
                    line = line + "\r"
                num_bytes = reporter.update_transfer_stat(line, stat, curr_time_nanos)
                if i == 0:
                    self.assertEqual(expected_bytes - bytes_in_flight, num_bytes)
                    self.assertEqual(0, stat.bytes_in_flight)
                else:
                    self.assertEqual(expected_bytes - bytes_in_flight, num_bytes)
                    self.assertEqual(expected_bytes, stat.bytes_in_flight)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), stat.eta.timestamp_nanos)
                self.assertEqual("[>                   ]  2% ETA 0:06:03 ETA 17:27:49", stat.eta.line_tail)

    def test_progress_reporter_stop(self) -> None:
        args = argparser_parse_args(args=["src", "dst"])
        log_params = bzfs.LogParams(args)
        try:
            p = bzfs.Params(args, log_params=log_params, log=bzfs.get_logger(log_params, args))
            reporter = bzfs.ProgressReporter(p, use_select=False, progress_update_intervals=None)
            reporter.stop()
            reporter.stop()  # test stopping more than once is ok
            reporter.exception = ValueError()
            with self.assertRaises(ValueError):
                reporter.stop()
            self.assertIsInstance(reporter.exception, ValueError)
            with self.assertRaises(ValueError):
                reporter.stop()
            self.assertIsInstance(reporter.exception, ValueError)

            reporter = bzfs.ProgressReporter(p, use_select=False, progress_update_intervals=None, fail=True)
            reporter._run()
            self.assertIsInstance(reporter.exception, ValueError)
            with self.assertRaises(ValueError):
                reporter.stop()
            self.assertIsInstance(reporter.exception, ValueError)
            with self.assertRaises(ValueError):
                reporter.stop()
            self.assertIsInstance(reporter.exception, ValueError)
        finally:
            bzfs.reset_logger()

    def test_progress_reporter_exhausted_iterator_sees_appended_data(self) -> None:
        # Create a temporary file. We will keep the file descriptor from mkstemp open for the read iterator
        temp_file_fd_write, temp_file_path = tempfile.mkstemp(prefix="test_iterator_behavior_")
        try:
            # Write initial lines to the file using the write file descriptor
            initial_lines = ["line1\n", "line2\n"]
            with open(temp_file_fd_write, "w") as fd_write:
                for line in initial_lines:
                    fd_write.write(line)
                # fd_write is closed here, but temp_file_path is still valid.

            # Open the file for reading and keep this file descriptor (fd_read) open for the duration of the iterator's life.
            with open(temp_file_path, "r") as fd_read:  # fd_read will remain open
                iter_fd = iter(fd_read)  # Create the iterator on the open fd_read

                # Consume all lines from iter_fd
                read_lines_initial = []
                for line in iter_fd:
                    read_lines_initial.append(line)
                self.assertEqual(read_lines_initial, initial_lines, "Initial lines not read correctly.")

                # Verify that the iterator is indeed exhausted (while fd_read is still open)
                with self.assertRaises(StopIteration, msg="Iterator should be exhausted after reading all initial lines."):
                    next(iter_fd)

                # Append new lines to the *same underlying file path*.
                # The original fd_read is still open and iter_fd is tied to it.
                appended_lines = ["line3_appended\n", "line4_appended\n"]
                # Open for append - this is a *different* file descriptor instance, but it operates on the same file on disk.
                with open(temp_file_path, "a") as f_append:
                    for line in appended_lines:
                        f_append.write(line)

                # Attempt to read from the original, exhausted iter_fd again.
                # fd_read (to which iter_fd is bound) is still open.
                read_lines_after_append_from_original_iterator = []
                # This loop should yield additional lines even though 'iter_fd' was previously exhausted.
                for line in iter_fd:  # Attempting to iterate over the *exhausted* iterator
                    read_lines_after_append_from_original_iterator.append(line)

                self.assertEqual(
                    read_lines_after_append_from_original_iterator,
                    ["line3_appended\n", "line4_appended\n"],
                    "Exhausted iterator (on an still-open file) should yiels new lines even if the underlying file "
                    "was appended to.",
                )
        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    def test_progress_reporter_state_with_reused_pv_file(self) -> None:
        # We will manually call the reporter's update method to simulate its internal loop.
        mock_params = MagicMock(spec=bzfs.Params)
        reporter = bzfs.ProgressReporter(mock_params, use_select=False, progress_update_intervals=(0.01, 0.02))

        # Simulate ProgressReporter opening the file (it does this internally based on enqueue), For this test, we'll
        # manually manage a TransferStat object, as the reporter would. The key is that if the filename is the same,
        # the *same* TransferStat object associated with that selector key would be reused.

        # Create a TransferStat as the reporter would associate with pv_log_file_base
        eta_dummy = reporter.TransferStat.ETA(timestamp_nanos=0, seq_nr=0, line_tail="")
        stat_obj_for_pv_log_file = reporter.TransferStat(bytes_in_flight=0, eta=eta_dummy)
        current_time_ns = time.monotonic_ns()

        # Simulate First PV Operation
        # Format: "TOTAL_KNOWN_SIZE: CURRENT_BYTES OTHER_PV_STATS\r"
        op1_lines = [
            "100KiB: 10KiB [=>   ] 10%\r",  # 10KB transferred
            "100KiB: 50KiB [==>  ] 50%\r",  # 50KB transferred
            "100KiB: 100KiB [====>] 100%\n",  # 100KB transferred (final line)
        ]
        total_bytes_op1 = 0
        for line in op1_lines:
            delta_bytes = reporter.update_transfer_stat(line, stat_obj_for_pv_log_file, current_time_ns)
            total_bytes_op1 += delta_bytes
            current_time_ns += 1000  # Advance time slightly
        self.assertEqual(total_bytes_op1, 100 * 1024, "Total bytes for Op1 incorrect")
        # After op1 final line (ends with \n), bytes_in_flight in stat_obj_for_pv_log_file should be 0
        self.assertEqual(stat_obj_for_pv_log_file.bytes_in_flight, 0, "bytes_in_flight should be 0 after final line of Op1")

        # Simulate Second PV Operation (reusing the same pv_log_file_base name)
        # If the pv_log_file name is not unique per op, the reporter continues to use the *same* stat_obj_for_pv_log_file
        op2_lines = [
            "200KiB: 20KiB [=>   ] 10%\r",  # 20KB transferred for THIS operation
            "200KiB: 100KiB [==>  ] 50%\r",  # 100KB transferred for THIS operation
            "200KiB: 200KiB [====>] 100%\n",  # 200KB transferred for THIS operation (final line)
        ]
        total_bytes_op2 = 0
        for line in op2_lines:
            # CRITICAL: We are passing the SAME stat_obj_for_pv_log_file
            delta_bytes = reporter.update_transfer_stat(line, stat_obj_for_pv_log_file, current_time_ns)
            total_bytes_op2 += delta_bytes
            current_time_ns += 1000
        self.assertEqual(total_bytes_op2, 200 * 1024, "Total bytes for Op2 incorrect")
        self.assertEqual(stat_obj_for_pv_log_file.bytes_in_flight, 0, "bytes_in_flight should be 0 after final line of Op2")

    def test_reporter_handles_pv_log_file_disappearing_before_initial_open(self) -> None:
        # Test if ProgressReporter handles FileNotFoundError during the initial open of a pv_log_file.
        mock_params = MagicMock(spec=bzfs.Params)
        mock_params.log = MagicMock(spec=logging.Logger)
        mock_params.pv_program_opts = bzfs.argument_parser().get_default("pv_program_opts").split()

        temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, temp_dir)
        pv_log_file = os.path.join(temp_dir, "test.pv")
        reporter = bzfs.ProgressReporter(mock_params, use_select=False, progress_update_intervals=None)
        self.assertSetEqual(set(), reporter.file_name_set)
        self.assertSetEqual(set(), reporter.file_name_queue)
        reporter.enqueue_pv_log_file(pv_log_file)
        self.assertSetEqual(set(), reporter.file_name_set)
        self.assertSetEqual({pv_log_file}, reporter.file_name_queue)

        def mock_open(*args: Any, **kwargs: Any) -> Any:
            if args[0] == pv_log_file:
                raise FileNotFoundError(f"File {pv_log_file} disappeared before opening.")
            return open(*args, **kwargs)

        with patch("builtins.open", mock_open):
            reporter.start()
            time.sleep(0.1)  # Give the reporter thread a moment to attempt to open the file
            reporter.stop()

        mock_params.log.warning.assert_called_with(
            "ProgressReporter: pv log file disappeared before initial open, skipping: %s", pv_log_file
        )
        self.assertSetEqual(set(), reporter.file_name_set)
        self.assertSetEqual(set(), reporter.file_name_queue)
        self.assertIsNone(reporter.exception)

    def test_has_duplicates(self) -> None:
        self.assertFalse(bzfs.has_duplicates([]))
        self.assertFalse(bzfs.has_duplicates([42]))
        self.assertFalse(bzfs.has_duplicates([1, 2, 3, 4, 5]))
        self.assertTrue(bzfs.has_duplicates([2, 2, 3, 4, 5]))
        self.assertTrue(bzfs.has_duplicates([1, 2, 3, 3, 4, 5]))
        self.assertTrue(bzfs.has_duplicates([1, 2, 3, 4, 5, 5]))
        self.assertTrue(bzfs.has_duplicates([1, 1, 2, 3, 3, 4, 4, 5]))
        self.assertTrue(bzfs.has_duplicates(["a", "b", "b", "c"]))
        self.assertFalse(bzfs.has_duplicates(["ant", "bee", "cat"]))

    @staticmethod
    def root_datasets_if_recursive_zfs_snapshot_is_possible_slow_but_correct(  # compare faster algos to this baseline impl
        src_datasets: List[str], basis_src_datasets: List[str]
    ) -> Optional[List[str]]:
        # Assumes that src_datasets and basis_src_datasets are both sorted (and thus root_datasets is sorted too)
        src_datasets_set = set(src_datasets)
        root_datasets = bzfs.Job().find_root_datasets(src_datasets)
        for basis_dataset in basis_src_datasets:
            for root_dataset in root_datasets:
                if bzfs.is_descendant(basis_dataset, of_root_dataset=root_dataset):
                    if basis_dataset not in src_datasets_set:
                        return None
        return root_datasets

    def test_root_datasets_if_recursive_zfs_snapshot_is_possible(self) -> None:
        def run_filter(src_datasets: List[str], basis_src_datasets: List[str]) -> List[str]:
            assert set(src_datasets).issubset(set(basis_src_datasets))
            src_datasets = list(sorted(src_datasets))
            basis_src_datasets = list(sorted(basis_src_datasets))
            expected = self.root_datasets_if_recursive_zfs_snapshot_is_possible_slow_but_correct(
                src_datasets, basis_src_datasets
            )
            actual = bzfs.Job().root_datasets_if_recursive_zfs_snapshot_is_possible(src_datasets, basis_src_datasets)
            if expected is not None:
                self.assertListEqual(expected, cast(List[str], actual))
            self.assertEqual(expected, actual)
            return cast(List[str], actual)

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

    def test_validate_snapshot_name(self) -> None:
        bzfs.SnapshotLabel("foo_", "", "", "").validate_label("")
        bzfs.SnapshotLabel("foo_", "", "", "_foo").validate_label("")
        bzfs.SnapshotLabel("foo_", "foo_", "", "_foo").validate_label("")
        with self.assertRaises(SystemExit):
            bzfs.SnapshotLabel("", "", "", "").validate_label("")
        with self.assertRaises(SystemExit):
            bzfs.SnapshotLabel("foo__", "", "", "").validate_label("")
        with self.assertRaises(SystemExit):
            bzfs.SnapshotLabel("foo_", "foo__", "", "_foo").validate_label("")
        with self.assertRaises(SystemExit):
            bzfs.SnapshotLabel("foo_", "foo_", "", "__foo").validate_label("")
        with self.assertRaises(SystemExit):
            bzfs.SnapshotLabel("foo", "", "", "").validate_label("")
        with self.assertRaises(SystemExit):
            bzfs.SnapshotLabel("foo_", "foo", "", "").validate_label("")
        with self.assertRaises(SystemExit):
            bzfs.SnapshotLabel("foo_", "", "", "foo").validate_label("")
        with self.assertRaises(SystemExit):
            bzfs.SnapshotLabel("foo@bar_", "", "", "").validate_label("")
        with self.assertRaises(SystemExit):
            bzfs.SnapshotLabel("foo/bar_", "", "", "").validate_label("")
        self.assertTrue(str(bzfs.SnapshotLabel("foo_", "", "", "")))

    def test_label_milliseconds(self) -> None:
        xperiods = bzfs.SnapshotPeriods()
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

    def test_CreateSrcSnapshotConfig(self) -> None:
        params = bzfs.Params(argparser_parse_args(args=["src", "dst"]))
        good_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"foo": {"onsite": {"adhoc": 1}}}),
                "--create-src-snapshots-timeformat=xxx",
            ]
        )
        config = bzfs.CreateSrcSnapshotConfig(good_args, params)
        self.assertTrue(str(config))
        self.assertListEqual(["foo_onsite_xxx_adhoc"], [str(label) for label in config.snapshot_labels()])

        foo_bar_periods = {"us-west-1": {"hourly": 1, "weekly": 1, "daily": 1, "baz": 1}}
        periods = {"foo": foo_bar_periods, "bar": foo_bar_periods}
        good_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str(periods),
                "--create-src-snapshots-timeformat=xxx",
            ]
        )
        config = bzfs.CreateSrcSnapshotConfig(good_args, params)
        self.assertListEqual(
            [
                "bar_us-west-1_xxx_weekly",
                "foo_us-west-1_xxx_weekly",
                "bar_us-west-1_xxx_daily",
                "foo_us-west-1_xxx_daily",
                "bar_us-west-1_xxx_hourly",
                "foo_us-west-1_xxx_hourly",
                "bar_us-west-1_xxx_baz",
                "foo_us-west-1_xxx_baz",
            ],
            [str(label) for label in config.snapshot_labels()],
        )

        good_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"prod": {"onsite": {"daily": 0}}}),  # retention_amount == 0
                "--create-src-snapshots-timeformat=xxx",
            ]
        )
        config = bzfs.CreateSrcSnapshotConfig(good_args, params)
        self.assertListEqual([], [str(label) for label in config.snapshot_labels()])

        good_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"prod": {"": {"": 1}}}),  # empty infix and suffix
                "--create-src-snapshots-timeformat=xxx",
            ]
        )
        config = bzfs.CreateSrcSnapshotConfig(good_args, params)
        self.assertListEqual(["prod_xxx"], [str(label) for label in config.snapshot_labels()])

        good_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots",
                "--create-src-snapshots-timeformat=%Y%m%d_%H%M%S_%F",  # %F is a non-standard hack to support milliseconds
            ]
        )
        config = bzfs.CreateSrcSnapshotConfig(good_args, params)
        self.assertEqual(1, len(config.snapshot_labels()))
        label = config.snapshot_labels()[0]
        self.assertTrue(bool(str(label)))
        self.assertEqual("bzfs_", label.prefix)
        self.assertEqual("onsite_", label.infix)
        self.assertEqual("_adhoc", label.suffix)
        self.assertEqual(len("20250301_220830_222"), len(label.timestamp))

        # Snapshot name generated by --create-src-snapshots-* options must be a valid ZFS snapshot name
        bad_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"..": {"onsite": {"adhoc": 1}}}),
                "--create-src-snapshots-timeformat=",
            ]
        )
        with self.assertRaises(SystemExit):
            bzfs.CreateSrcSnapshotConfig(bad_args, params)

        # Snapshot name generated by --create-src-snapshots-* options must not be empty or begin with underscore
        bad_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"": {"onsite": {"adhoc": 1}}}),
                "--create-src-snapshots-timeformat=",
            ]
        )
        with self.assertRaises(SystemExit):
            bzfs.CreateSrcSnapshotConfig(bad_args, params)

        # Period amount must not be negative
        bad_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"prod": {"onsite": {"hourly": -1}}}),
            ]
        )
        with self.assertRaises(SystemExit):
            bzfs.CreateSrcSnapshotConfig(bad_args, params)

        # Period duration should be a divisor of 86400 seconds without remainder so snapshot timestamps repeat every day
        bad_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"prod": {"onsite": {"7hourly": 1}}}),
            ]
        )
        with self.assertRaises(SystemExit):
            bzfs.CreateSrcSnapshotConfig(bad_args, params)

        # Period duration should be a divisor of 86400 seconds without remainder so snapshot timestamps repeat every day
        bad_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"prod": {"onsite": {"7minutely": 1}}}),
            ]
        )
        with self.assertRaises(SystemExit):
            bzfs.CreateSrcSnapshotConfig(bad_args, params)

        # Period duration should be a divisor of 86400 seconds without remainder so snapshot timestamps repeat every day
        bad_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"prod": {"onsite": {"7secondly": 1}}}),
            ]
        )
        with self.assertRaises(SystemExit):
            bzfs.CreateSrcSnapshotConfig(bad_args, params)

        # Period duration should be a divisor of 86400 seconds without remainder so snapshot timestamps repeat every day
        bad_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"prod": {"onsite": {"86401secondly": 1}}}),
            ]
        )
        with self.assertRaises(SystemExit):
            bzfs.CreateSrcSnapshotConfig(bad_args, params)

        # Period duration should be a divisor of 12 months without remainder so that snapshots will be created at the same
        # time every year
        bad_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"prod": {"onsite": {"5monthly": 1}}}),
            ]
        )
        with self.assertRaises(SystemExit):
            bzfs.CreateSrcSnapshotConfig(bad_args, params)

        good_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"prod": {"onsite": {"6monthly": 1}}}),
            ]
        )
        bzfs.CreateSrcSnapshotConfig(good_args, params)

        args = bzfs.argument_parser().parse_args(["src", "dst", "--daemon-frequency=2secondly"])
        config = bzfs.CreateSrcSnapshotConfig(args, bzfs.Params(args))
        self.assertDictEqual({"_2secondly": (2, "secondly")}, config.suffix_durations)

        args = bzfs.argument_parser().parse_args(["src", "dst", "--daemon-frequency=2seconds"])
        with self.assertRaises(SystemExit):
            bzfs.CreateSrcSnapshotConfig(args, bzfs.Params(args))

        args = bzfs.argument_parser().parse_args(["src", "dst", "--daemon-frequency=-2secondly"])
        with self.assertRaises(SystemExit):
            bzfs.CreateSrcSnapshotConfig(args, bzfs.Params(args))

        args = bzfs.argument_parser().parse_args(["src", "dst", "--daemon-frequency=2adhoc"])
        with self.assertRaises(SystemExit):
            bzfs.CreateSrcSnapshotConfig(args, bzfs.Params(args))

    def test_MonitorSnapshotsConfig(self) -> None:
        def plan(alerts: Dict[str, Any]) -> str:
            return str({"z": {"onsite": {"100millisecondly": alerts}}})

        params = bzfs.Params(argparser_parse_args(args=["src", "dst"]))
        args = bzfs.argument_parser().parse_args(
            ["src", "dst", "--monitor-snapshots=" + plan({"latest": {"warning": "1 millis", "critical": "2 millis"}})]
        )
        config = bzfs.MonitorSnapshotsConfig(args, params)
        self.assertTrue(str(config))
        self.assertListEqual(
            [(100 + 1, 100 + 2)], [(alert.latest.warning_millis, alert.latest.critical_millis) for alert in config.alerts]  # type: ignore
        )
        self.assertListEqual([None], [alert.oldest for alert in config.alerts])
        self.assertListEqual(["z_onsite__100millisecondly"], [str(alert.label) for alert in config.alerts])
        self.assertTrue(config.enable_monitor_snapshots)
        self.assertFalse(config.dont_warn)
        self.assertFalse(config.dont_crit)

        args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--monitor-snapshots=" + plan({"oldest": {"warning": "1 millis", "critical": "2 millis", "cycles": 3}}),
            ]
        )
        config = bzfs.MonitorSnapshotsConfig(args, params)
        self.assertTrue(str(config))
        self.assertListEqual([None], [alert.latest for alert in config.alerts])
        self.assertListEqual(
            [(3 * 100 + 1, 3 * 100 + 2)],
            [(alert.oldest.warning_millis, alert.oldest.critical_millis) for alert in config.alerts],  # type: ignore
        )
        self.assertListEqual(["z_onsite__100millisecondly"], [str(alert.label) for alert in config.alerts])
        self.assertTrue(config.enable_monitor_snapshots)
        self.assertFalse(config.dont_warn)
        self.assertFalse(config.dont_crit)

        args = bzfs.argument_parser().parse_args(
            ["src", "dst", "--monitor-snapshots=" + plan({"latest": {"warning": "2 millis"}})]
        )
        config = bzfs.MonitorSnapshotsConfig(args, params)
        self.assertTrue(str(config))
        self.assertListEqual(
            [(100 + 2, bzfs.unixtime_infinity_secs)],
            [(alert.latest.warning_millis, alert.latest.critical_millis) for alert in config.alerts],  # type: ignore
        )
        self.assertListEqual(["z_onsite__100millisecondly"], [str(alert.label) for alert in config.alerts])

        args = bzfs.argument_parser().parse_args(
            ["src", "dst", "--monitor-snapshots=" + plan({"latest": {"critical": "2 millis"}})]
        )
        config = bzfs.MonitorSnapshotsConfig(args, params)
        self.assertTrue(str(config))
        self.assertListEqual(
            [(bzfs.unixtime_infinity_secs, 100 + 2)],
            [(alert.latest.warning_millis, alert.latest.critical_millis) for alert in config.alerts],  # type: ignore
        )
        self.assertListEqual(["z_onsite__100millisecondly"], [str(alert.label) for alert in config.alerts])

        args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--monitor-snapshots-no-latest-check",
                "--monitor-snapshots-no-oldest-check",
                "--monitor-snapshots=" + plan({"latest": {"critical": "2 millis"}, "oldest": {"critical": "2 millis"}}),
            ]
        )
        config = bzfs.MonitorSnapshotsConfig(args, params)
        self.assertListEqual([], config.alerts)
        self.assertFalse(config.enable_monitor_snapshots)

        args = bzfs.argument_parser().parse_args(["src", "dst", "--monitor-snapshots=" + plan({"latest": {}})])
        config = bzfs.MonitorSnapshotsConfig(args, params)
        self.assertListEqual([], config.alerts)
        self.assertFalse(config.enable_monitor_snapshots)

        args = bzfs.argument_parser().parse_args(["src", "dst", "--monitor-snapshots=" + plan({})])
        config = bzfs.MonitorSnapshotsConfig(args, params)
        self.assertListEqual([], config.alerts)
        self.assertFalse(config.enable_monitor_snapshots)

        args = bzfs.argument_parser().parse_args(["src", "dst", "--monitor-snapshots={}"])
        config = bzfs.MonitorSnapshotsConfig(args, params)
        self.assertListEqual([], config.alerts)
        self.assertFalse(config.enable_monitor_snapshots)

        args = bzfs.argument_parser().parse_args(
            ["src", "dst", "--monitor-snapshots=" + plan({"latest": {"badalert": "2 millis"}})]
        )
        with self.assertRaises(SystemExit):
            bzfs.MonitorSnapshotsConfig(args, params)

        args = bzfs.argument_parser().parse_args(
            ["src", "dst", "--monitor-snapshots=" + plan({"badlatest": {"warning": "2 millis"}})]
        )
        with self.assertRaises(SystemExit):
            bzfs.MonitorSnapshotsConfig(args, params)

        args = bzfs.argument_parser().parse_args(
            ["src", "dst", "--monitor-snapshots=" + plan({"latest": {"warning": "2 millisxxxxx"}})]
        )
        with self.assertRaises(SystemExit):
            bzfs.MonitorSnapshotsConfig(args, params)

        args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--monitor-snapshots-dont-warn",
                "--monitor-snapshots-dont-crit",
                "--monitor-snapshots=" + plan({"latest": {"warning": "2 millis"}}),
            ]
        )
        config = bzfs.MonitorSnapshotsConfig(args, params)
        self.assertTrue(config.dont_warn)
        self.assertTrue(config.dont_crit)

    @patch("bzfs_main.bzfs.Job.itr_ssh_cmd_parallel")
    def test_zfs_get_snapshots_changed_parsing(self, mock_itr_parallel: MagicMock) -> None:
        job = bzfs.Job()
        job.params = bzfs.Params(argparser_parse_args(args=["src", "dst"]))
        self.mock_remote = MagicMock(spec=bzfs.Remote)  # spec helps catch calls to non-existent attrs

        mock_itr_parallel.return_value = [  # normal input
            [
                "12345\tdataset/valid1",
                "789\tdataset/valid2",
            ]
        ]
        results = job.zfs_get_snapshots_changed(self.mock_remote, ["d1", "d2", "d3", "d4"])
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
        results = job.zfs_get_snapshots_changed(self.mock_remote, ["d1", "d2", "d3", "d4"])
        self.assertDictEqual({"dataset/valid1": 12345}, results)

        mock_itr_parallel.return_value = [
            [
                "12345\tdataset/valid1",
                "123\t",  # empty dataset
                "789\tdataset/valid2",
            ]
        ]
        results = job.zfs_get_snapshots_changed(self.mock_remote, ["d1", "d2", "d3", "d4"])
        self.assertDictEqual({"dataset/valid1": 12345}, results)

        mock_itr_parallel.return_value = [
            [
                "12345\tdataset/valid1",
                "\tfoo",  # missing timestamp
                "789\tdataset/valid2",
            ]
        ]
        results = job.zfs_get_snapshots_changed(self.mock_remote, ["d1", "d2", "d3", "d4"])
        self.assertDictEqual({"dataset/valid1": 12345, "foo": 0, "dataset/valid2": 789}, results)


#############################################################################
class TestTerminateProcessSubtree(unittest.TestCase):
    def setUp(self) -> None:
        self.children: List[subprocess.Popen[Any]] = []

    def tearDown(self) -> None:
        for child in self.children:
            try:
                child.kill()
            except Exception:
                pass
        self.children = []

    def test_get_descendant_processes(self) -> None:
        child = subprocess.Popen(["sleep", "1"])
        self.children.append(child)
        time.sleep(0.1)
        descendants = bzfs.get_descendant_processes(os.getpid())
        self.assertIn(child.pid, descendants, "Child PID not found in descendants")

    def test_terminate_process_subtree_excluding_current(self) -> None:
        child = subprocess.Popen(["sleep", "1"])
        self.children.append(child)
        time.sleep(0.1)
        self.assertIsNone(child.poll(), "Child process should be running before termination")
        bzfs.terminate_process_subtree(except_current_process=True)
        time.sleep(0.1)
        self.assertIsNotNone(child.poll(), "Child process should be terminated")


#############################################################################
class TestSubprocessRun(unittest.TestCase):
    def test_successful_command(self) -> None:
        result = bzfs.subprocess_run(["true"], stdout=PIPE, stderr=PIPE)
        self.assertEqual(0, result.returncode)
        self.assertEqual(b"", result.stdout)
        self.assertEqual(b"", result.stderr)

    def test_failing_command_no_check(self) -> None:
        result = bzfs.subprocess_run(["false"], stdout=PIPE, stderr=PIPE)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(b"", result.stdout)
        self.assertEqual(b"", result.stderr)

    def test_failing_command_with_check(self) -> None:
        with self.assertRaises(subprocess.CalledProcessError) as context:
            bzfs.subprocess_run(["false"], stdout=PIPE, stderr=PIPE, check=True)
        self.assertIsInstance(context.exception, subprocess.CalledProcessError)
        self.assertIsInstance(context.exception.returncode, int)
        self.assertTrue(context.exception.returncode != 0)

    def test_input_bytes(self) -> None:
        result = bzfs.subprocess_run(["cat"], input=b"hello", stdout=PIPE, stderr=PIPE)
        self.assertEqual(b"hello", result.stdout)

    def test_valueerror_input_and_stdin(self) -> None:
        with self.assertRaises(ValueError):
            bzfs.subprocess_run(["cat"], input=b"hello", stdin=PIPE, stdout=PIPE, stderr=PIPE)

    def test_timeout_expired(self) -> None:
        with self.assertRaises(subprocess.TimeoutExpired) as context:
            bzfs.subprocess_run(["sleep", "1"], timeout=0.01, stdout=PIPE, stderr=PIPE)
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
                bzfs.subprocess_run(["sleep", "1"])
            thread.join()
        finally:
            signal.signal(signal.SIGINT, old_handler)  # restore original signal handler


#############################################################################
class TestParseDatasetLocator(unittest.TestCase):
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
        user, host, user_host, pool, dataset = bzfs.parse_dataset_locator(input_value, validate=False)

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
                print("Validation Test failed:")
            else:
                print("Test failed:")
            print(
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
        self.run_test("u`ser@localhost:tank1/foo/bar", "u`ser", "localhost", "tank1/foo/bar", "u`ser@localhost", True)
        self.run_test("u'ser@localhost:tank1/foo/bar", "u'ser", "localhost", "tank1/foo/bar", "u'ser@localhost", True)
        self.run_test('u"ser@localhost:tank1/foo/bar', 'u"ser', "localhost", "tank1/foo/bar", 'u"ser@localhost', True)
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
class TestReplaceCapturingGroups(unittest.TestCase):
    def replace_capturing_group(self, regex: str) -> str:
        return bzfs.replace_capturing_groups_with_non_capturing_groups(regex)

    def test_basic_case(self) -> None:
        self.assertEqual(self.replace_capturing_group("(abc)"), "(?:abc)")

    def test_nested_groups(self) -> None:
        self.assertEqual(self.replace_capturing_group("(a(bc)d)"), "(?:a(?:bc)d)")

    def test_preceding_backslash(self) -> None:
        self.assertEqual(self.replace_capturing_group("\\(abc)"), "\\(abc)")

    def test_group_starting_with_question_mark(self) -> None:
        self.assertEqual(self.replace_capturing_group("(?abc)"), "(?abc)")

    def test_multiple_groups(self) -> None:
        self.assertEqual(self.replace_capturing_group("(abc)(def)"), "(?:abc)(?:def)")

    def test_mixed_cases(self) -> None:
        self.assertEqual(self.replace_capturing_group("a(bc\\(de)f(gh)?i"), "a(?:bc\\(de)f(?:gh)?i")

    def test_empty_group(self) -> None:
        self.assertEqual(self.replace_capturing_group("()"), "(?:)")

    def test_group_with_named_group(self) -> None:
        self.assertEqual(self.replace_capturing_group("(?P<name>abc)"), "(?P<name>abc)")

    def test_group_with_non_capturing_group(self) -> None:
        self.assertEqual(self.replace_capturing_group("(a(?:bc)d)"), "(?:a(?:bc)d)")

    def test_group_with_lookahead(self) -> None:
        self.assertEqual(self.replace_capturing_group("(abc)(?=def)"), "(?:abc)(?=def)")

    def test_group_with_lookbehind(self) -> None:
        self.assertEqual(self.replace_capturing_group("(?<=abc)(def)"), "(?<=abc)(?:def)")

    def test_escaped_characters(self) -> None:
        pattern = re.escape("(abc)")
        self.assertEqual(self.replace_capturing_group(pattern), pattern)

    def test_complex_pattern_with_escape(self) -> None:
        complex_pattern = re.escape("(a[b]c{d}e|f.g)")
        self.assertEqual(self.replace_capturing_group(complex_pattern), complex_pattern)

    def test_complex_pattern(self) -> None:
        complex_pattern = "(a[b]c{d}e|f.g)(h(i|j)k)?(\\(l\\))"
        expected_result = "(?:a[b]c{d}e|f.g)(?:h(?:i|j)k)?(?:\\(l\\))"
        self.assertEqual(self.replace_capturing_group(complex_pattern), expected_result)


#############################################################################
class TestBuildTree(unittest.TestCase):
    def assert_keys_sorted(self, tree: Dict[str, Any]) -> None:
        keys = list(tree.keys())
        self.assertEqual(keys, sorted(keys), f"Keys are not sorted: {keys}")
        for value in tree.values():
            if isinstance(value, dict):
                self.assert_keys_sorted(value)

    def test_basic_tree(self) -> None:
        datasets = ["pool", "pool/dataset", "pool/dataset/sub", "pool/other", "pool/other/sub/child"]
        expected_tree = {"pool": {"dataset": {"sub": {}}, "other": {"sub": {"child": {}}}}}
        tree = bzfs.Job().build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_empty_input(self) -> None:
        datasets: List[str] = []
        expected_tree: bzfs.Tree = {}
        tree = bzfs.Job().build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)

    def test_single_root(self) -> None:
        datasets: List[str] = ["pool"]
        expected_tree: bzfs.Tree = {"pool": {}}
        tree = bzfs.Job().build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_single_branch(self) -> None:
        datasets: List[str] = ["pool/dataset/sub/child"]
        expected_tree: bzfs.Tree = {"pool": {"dataset": {"sub": {"child": {}}}}}
        tree = bzfs.Job().build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots(self) -> None:
        datasets: List[str] = ["pool", "otherpool", "anotherpool"]
        expected_tree: bzfs.Tree = {"anotherpool": {}, "otherpool": {}, "pool": {}}
        tree = bzfs.Job().build_dataset_tree(sorted(datasets))
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_large_dataset(self) -> None:
        datasets: List[str] = [f"pool/dataset{i}" for i in range(100)]
        tree = bzfs.Job().build_dataset_tree(sorted(datasets))
        self.assertEqual(len(tree["pool"]), 100)
        self.assert_keys_sorted(tree)

    def test_nested_structure(self) -> None:
        datasets: List[str] = [
            "pool/parent",
            "pool/parent/child1",
            "pool/parent/child2",
            "pool/parent/child2/grandchild",
            "pool/parent/child3",
        ]
        expected_tree: bzfs.Tree = {"pool": {"parent": {"child1": {}, "child2": {"grandchild": {}}, "child3": {}}}}
        tree = bzfs.Job().build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_no_children(self) -> None:
        datasets: List[str] = ["pool", "otherpool"]
        expected_tree: bzfs.Tree = {"otherpool": {}, "pool": {}}
        tree = bzfs.Job().build_dataset_tree(sorted(datasets))
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_single_level(self) -> None:
        datasets: List[str] = ["pool", "pool1", "pool2", "pool3"]
        expected_tree: bzfs.Tree = {"pool": {}, "pool1": {}, "pool2": {}, "pool3": {}}
        tree = bzfs.Job().build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots_with_hierarchy(self) -> None:
        datasets: List[str] = ["pool", "pool1", "pool1/dataset1", "pool2", "pool2/dataset2", "pool2/dataset2/sub", "pool3"]
        expected_tree: bzfs.Tree = {"pool": {}, "pool1": {"dataset1": {}}, "pool2": {"dataset2": {"sub": {}}}, "pool3": {}}
        tree = bzfs.Job().build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots_flat(self) -> None:
        datasets: List[str] = ["root1", "root2", "root3", "root4"]
        expected_tree: bzfs.Tree = {"root1": {}, "root2": {}, "root3": {}, "root4": {}}
        tree = bzfs.Job().build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots_mixed_depth(self) -> None:
        datasets: List[str] = ["a", "a/b", "a/b/c", "x", "x/y", "z", "z/1", "z/2", "z/2/3"]
        expected_tree: bzfs.Tree = {"a": {"b": {"c": {}}}, "x": {"y": {}}, "z": {"1": {}, "2": {"3": {}}}}
        tree = bzfs.Job().build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_tree_with_missing_intermediate_nodes(self) -> None:
        datasets: List[str] = ["a", "a/b/c", "z/2/3"]
        expected_tree: bzfs.Tree = {"a": {"b": {"c": {}}}, "z": {"2": {"3": {}}}}
        tree = bzfs.Job().build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_tree_with_barriers(self) -> None:
        BR = bzfs.BARRIER_CHAR
        datasets: List[str] = [
            "a/b/c",
            "a/b/c/0d",
            "a/b/c/1d",
            f"a/b/c/{BR}/prune",
            f"a/b/c/{BR}/prune/monitor",
            f"a/b/c/{BR}/{BR}/done",
        ]
        expected_tree: bzfs.Tree = {
            "a": {"b": {"c": {"0d": {}, "1d": {}, BR: {"prune": {"monitor": {}}, BR: {"done": {}}}}}}
        }
        tree = bzfs.Job().build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)


#############################################################################
class TestCurrentDateTime(unittest.TestCase):

    def setUp(self) -> None:
        self.fixed_datetime = datetime(2024, 1, 1, 12, 0, 0)  # in no timezone

    def test_now(self) -> None:
        self.assertIsNotNone(bzfs.current_datetime(tz_spec=None, now_fn=None))
        self.assertIsNotNone(bzfs.current_datetime(tz_spec="UTC", now_fn=None))
        self.assertIsNotNone(bzfs.current_datetime(tz_spec="+0530", now_fn=None))
        self.assertIsNotNone(bzfs.current_datetime(tz_spec="+05:30", now_fn=None))
        self.assertIsNotNone(bzfs.current_datetime(tz_spec="-0430", now_fn=None))
        self.assertIsNotNone(bzfs.current_datetime(tz_spec="-04:30", now_fn=None))

    def test_local_timezone(self) -> None:
        expected = self.fixed_datetime.astimezone(tz=None)
        actual = bzfs.current_datetime(tz_spec=None, now_fn=lambda tz=None: self.fixed_datetime.astimezone(tz=tz))  # type: ignore
        self.assertEqual(expected, actual)

        # For the strftime format, see https://docs.python.org/3.12/library/datetime.html#strftime-strptime-behavior.
        fmt = "%Y-%m-%dT%H:%M:%S"
        self.assertEqual("2024-01-01T12:00:00", actual.strftime(fmt))
        fmt = "%Y-%m-%d_%H:%M:%S"
        self.assertEqual("2024-01-01_12:00:00", actual.strftime(fmt))
        fmt = "%Y-%m-%d_%H:%M:%S_%Z"
        expected_prefix = "2024-01-01_12:00:00_"
        self.assertTrue(actual.strftime(fmt).startswith(expected_prefix))
        suffix = actual.strftime(fmt)[len(expected_prefix) :]
        self.assertGreater(len(suffix), 0)  # CET

        fmt = "%Y-%m-%d_%H:%M:%S%z"
        expected_prefix = "2024-01-01_12:00:00"
        self.assertTrue(actual.strftime(fmt).startswith(expected_prefix))
        suffix = actual.strftime(fmt)[len(expected_prefix) :]  # "+0100"
        self.assertIn(suffix[0], ["+", "-"])
        self.assertTrue(suffix[1:].isdigit())

    def test_utc_timezone(self) -> None:
        expected = self.fixed_datetime.astimezone(tz=timezone.utc)

        def now_fn(tz: Optional[tzinfo] = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = bzfs.current_datetime(tz_spec="UTC", now_fn=now_fn)
        self.assertEqual(expected, actual)

    def test_tzoffset_positive(self) -> None:
        tz_spec = "+0530"
        tz = timezone(timedelta(hours=5, minutes=30))
        expected = self.fixed_datetime.astimezone(tz=tz)

        def now_fn(_: Optional[tzinfo] = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = bzfs.current_datetime(tz_spec=tz_spec, now_fn=now_fn)
        self.assertEqual(expected, actual)

    def test_tzoffset_negative(self) -> None:
        tz_spec = "-0430"
        tz = timezone(timedelta(hours=-4, minutes=-30))
        expected = self.fixed_datetime.astimezone(tz=tz)

        def now_fn(_: Optional[tzinfo] = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = bzfs.current_datetime(tz_spec=tz_spec, now_fn=now_fn)
        self.assertEqual(expected, actual)

    def test_iana_timezone(self) -> None:
        if sys.version_info < (3, 9):
            self.skipTest("ZoneInfo requires python >= 3.9")
        from zoneinfo import ZoneInfo  # requires python >= 3.9

        tz_spec = "Asia/Tokyo"
        self.assertIsNotNone(bzfs.current_datetime(tz_spec=tz_spec, now_fn=None))
        tz = ZoneInfo(tz_spec)  # Standard IANA timezone. Example: "Europe/Vienna"
        expected = self.fixed_datetime.astimezone(tz=tz)
        actual = bzfs.current_datetime(tz_spec=tz_spec, now_fn=lambda _=None: self.fixed_datetime.astimezone(tz=tz))
        self.assertEqual(expected, actual)

        fmt = "%Y-%m-%dT%H:%M:%S%z"
        expected_prefix = "2024-01-01T20:00:00"
        suffix = actual.strftime(fmt)[len(expected_prefix) :]  # "+0100"
        self.assertIn(suffix[0], ["+", "-"])
        self.assertTrue(suffix[1:].isdigit())

    def test_invalid_timezone(self) -> None:
        with self.assertRaises(ValueError) as context:
            bzfs.current_datetime(tz_spec="INVALID")
        self.assertIn("Invalid timezone specification", str(context.exception))

    def test_invalid_timezone_format_missing_sign(self) -> None:
        with self.assertRaises(ValueError):
            bzfs.current_datetime(tz_spec="0400")


def round_datetime_up_to_duration_multiple(
    dt: datetime, duration_amount: int, duration_unit: str, anchors: Optional[PeriodAnchors] = None
) -> datetime:
    anchors = PeriodAnchors() if anchors is None else anchors
    return bzfs.round_datetime_up_to_duration_multiple(dt, duration_amount, duration_unit, anchors=anchors)


class TestRoundDatetimeUpToDurationMultiple(unittest.TestCase):
    def setUp(self) -> None:
        # Use a fixed timezone (e.g. Eastern Standard Time, UTC-5) for all tests.
        self.tz = timezone(timedelta(hours=-5))

    def test_examples(self) -> None:
        def make_dt(hour: int, minute: int, second: int) -> datetime:
            return datetime(2024, 11, 29, hour, minute, second, 0, tzinfo=self.tz)

        def round_up(dt: datetime, duration_amount: int) -> datetime:
            return round_datetime_up_to_duration_multiple(dt, duration_amount, "hourly")

        """
        Using default anchors (e.g. hourly snapshots at minute=0, second=0)
        14:00:00, 1 hours --> 14:00:00
        14:05:01, 1 hours --> 15:00:00
        15:05:01, 1 hours --> 16:00:00
        16:05:01, 1 hours --> 17:00:00
        23:55:01, 1 hours --> 00:00:00 on the next day
        14:05:01, 2 hours --> 16:00:00
        15:00:00, 2 hours --> 16:00:00
        15:05:01, 2 hours --> 16:00:00
        16:00:00, 2 hours --> 16:00:00
        16:05:01, 2 hours --> 18:00:00
        23:55:01, 2 hours --> 00:00:00 on the next day
        """

        dt = make_dt(hour=14, minute=0, second=0)
        self.assertEqual(dt, round_up(dt, duration_amount=1))

        dt = make_dt(hour=14, minute=5, second=1)
        self.assertEqual(dt.replace(hour=15, minute=0, second=0), round_up(dt, duration_amount=1))

        dt = make_dt(hour=15, minute=5, second=1)
        self.assertEqual(dt.replace(hour=16, minute=0, second=0), round_up(dt, duration_amount=1))

        dt = make_dt(hour=16, minute=5, second=1)
        self.assertEqual(dt.replace(hour=17, minute=0, second=0), round_up(dt, duration_amount=1))

        dt = make_dt(hour=23, minute=55, second=1)
        self.assertEqual(dt.replace(day=dt.day + 1, hour=0, minute=0, second=0), round_up(dt, duration_amount=1))

        dt = make_dt(hour=14, minute=5, second=1)
        self.assertEqual(dt.replace(hour=16, minute=0, second=0), round_up(dt, duration_amount=2))

        dt = make_dt(hour=15, minute=0, second=0)
        self.assertEqual(dt.replace(hour=16, minute=0, second=0), round_up(dt, duration_amount=2))

        dt = make_dt(hour=15, minute=5, second=1)
        self.assertEqual(dt.replace(hour=16, minute=0, second=0), round_up(dt, duration_amount=2))

        dt = make_dt(hour=16, minute=0, second=0)
        self.assertEqual(dt, round_up(dt, duration_amount=2))

        dt = make_dt(hour=16, minute=5, second=1)
        self.assertEqual(dt.replace(hour=18, minute=0, second=0), round_up(dt, duration_amount=2))

        dt = make_dt(hour=23, minute=55, second=1)
        self.assertEqual(dt.replace(day=dt.day + 1, hour=0, minute=0, second=0), round_up(dt, duration_amount=2))

    def test_zero_unixtime(self) -> None:
        dt = datetime.fromtimestamp(0, tz=timezone.utc)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        expected = dt
        self.assertEqual(expected, result)
        self.assertEqual(timezone.utc, result.tzinfo)

        tz = timezone(timedelta(hours=-7))
        dt = datetime.fromtimestamp(0, tz=tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        expected = dt
        self.assertEqual(expected, result)
        self.assertEqual(tz, result.tzinfo)

    def test_zero_unixtime_plus_one_microsecond(self) -> None:
        dt = datetime.fromtimestamp(1 / 1_000_000, tz=timezone.utc)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        expected = dt.replace(microsecond=0) + timedelta(hours=1)
        self.assertEqual(expected, result)
        self.assertEqual(timezone.utc, result.tzinfo)

        tz = timezone(timedelta(hours=-7))
        dt = datetime.fromtimestamp(1 / 1_000_000, tz=tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        expected = dt.replace(microsecond=0) + timedelta(hours=1)
        self.assertEqual(expected, result)
        self.assertEqual(tz, result.tzinfo)

    def test_zero_duration_amount(self) -> None:
        dt = datetime(2025, 2, 11, 14, 5, 1, 123456, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 0, "hourly")
        expected = dt
        self.assertEqual(expected, result)
        self.assertEqual(self.tz, result.tzinfo)

    def test_milliseconds_non_boundary(self) -> None:
        """Rounding up to the next millisecond when dt is not on a millisecond boundary."""
        dt = datetime(2025, 2, 11, 14, 5, 1, 123456, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "millisecondly")
        expected = dt.replace(microsecond=0) + timedelta(milliseconds=123 + 1)
        self.assertEqual(expected, result)
        self.assertEqual(self.tz, result.tzinfo)

    def test_milliseconds_boundary(self) -> None:
        """Rounding up to the next second when dt is exactly on a second boundary returns dt."""
        dt = datetime(2025, 2, 11, 14, 5, 1, 123000, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "millisecondly")
        self.assertEqual(dt, result)

    def test_seconds_non_boundary(self) -> None:
        """Rounding up to the next second when dt is not on a second boundary."""
        dt = datetime(2025, 2, 11, 14, 5, 1, 123456, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "secondly")
        expected = dt.replace(microsecond=0) + timedelta(seconds=1)
        self.assertEqual(expected, result)
        self.assertEqual(self.tz, result.tzinfo)

    def test_seconds_boundary(self) -> None:
        """Rounding up to the next second when dt is exactly on a second boundary returns dt."""
        dt = datetime(2025, 2, 11, 14, 5, 1, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "secondly")
        self.assertEqual(dt, result)

    def test_minutes_non_boundary(self) -> None:
        """Rounding up to the next minute when dt is not on a minute boundary."""
        dt = datetime(2025, 2, 11, 14, 5, 1, 500000, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "minutely")
        expected = dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
        self.assertEqual(expected, result)

    def test_minutes_boundary(self) -> None:
        """Rounding up to the next minute when dt is exactly on a minute boundary returns dt."""
        dt = datetime(2025, 2, 11, 14, 5, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "minutely")
        self.assertEqual(dt, result)

    def test_hours_non_boundary(self) -> None:
        """Rounding up to the next hour when dt is not on an hour boundary."""
        dt = datetime(2025, 2, 11, 14, 5, 1, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        expected = dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        self.assertEqual(expected, result)

    def test_hours_non_boundary2(self) -> None:
        """Rounding up to the next hour when dt is not on an hour boundary."""
        dt = datetime(2025, 2, 11, 0, 5, 1, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly", PeriodAnchors(**{"hourly_minute": 59}))
        expected = dt.replace(minute=59, second=0, microsecond=0) + timedelta(hours=0)
        self.assertEqual(expected, result)

    def test_hours_non_boundary3(self) -> None:
        """Rounding up to the next hour when dt is not on an hour boundary."""
        dt = datetime(2025, 2, 11, 2, 5, 1, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly", PeriodAnchors(**{"hourly_minute": 59}))
        expected = dt.replace(minute=59, second=0, microsecond=0) + timedelta(hours=0)
        self.assertEqual(expected, result)

    def test_hours_boundary(self) -> None:
        """Rounding up to the next hour when dt is exactly on an hour boundary returns dt."""
        dt = datetime(2025, 2, 11, 14, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        self.assertEqual(dt, result)

    def test_days_non_boundary(self) -> None:
        """Rounding up to the next day when dt is not on a day boundary."""
        dt = datetime(2025, 2, 11, 14, 5, 1, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "daily")
        expected = dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        self.assertEqual(expected, result)

    def test_days_non_boundary2(self) -> None:
        """Rounding up to the next day when dt is not on a day boundary."""
        dt = datetime(2025, 2, 11, 1, 59, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "daily", PeriodAnchors(**{"daily_hour": 2}))
        expected = dt.replace(hour=2, minute=0, second=0, microsecond=0) + timedelta(days=0)
        self.assertEqual(expected, result)

    def test_days_boundary(self) -> None:
        """Rounding up to the next day when dt is exactly at midnight returns dt."""
        dt = datetime(2025, 2, 11, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "daily")
        self.assertEqual(dt, result)

    def test_weeks_non_boundary_saturday(self) -> None:
        # anchor is the most recent between Friday and Saturday
        dt = datetime(2025, 2, 11, 14, 5, 1, tzinfo=self.tz)  # Tuesday
        expected = datetime(2025, 2, 15, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=bzfs.PeriodAnchors(weekly_weekday=6))
        self.assertEqual(expected, result)

    def test_weeks_non_boundary_sunday(self) -> None:
        # anchor is the most recent midnight between Saturday and Sunday
        dt = datetime(2025, 2, 11, 14, 5, 1, tzinfo=self.tz)  # Tuesday
        expected = datetime(2025, 2, 16, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=bzfs.PeriodAnchors(weekly_weekday=0))
        self.assertEqual(expected, result)

    def test_weeks_non_boundary_monday(self) -> None:
        # anchor is the most recent midnight between Sunday and Monday
        dt = datetime(2025, 2, 11, 14, 5, 1, tzinfo=self.tz)  # Tuesday
        expected = datetime(2025, 2, 17, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=bzfs.PeriodAnchors(weekly_weekday=1))
        self.assertEqual(expected, result)

    def test_weeks_boundary_saturday(self) -> None:
        # dt is exactly at midnight between Friday and Saturday
        dt = datetime(2025, 2, 15, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=bzfs.PeriodAnchors(weekly_weekday=6))
        self.assertEqual(dt, result)

    def test_weeks_boundary_sunday(self) -> None:
        # dt is exactly at midnight between Saturday and Sunday
        dt = datetime(2025, 2, 16, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=bzfs.PeriodAnchors(weekly_weekday=0))
        self.assertEqual(dt, result)

    def test_weeks_boundary_monday(self) -> None:
        # dt is exactly at midnight between Sunday and Monday
        dt = datetime(2025, 2, 17, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=bzfs.PeriodAnchors(weekly_weekday=1))
        self.assertEqual(dt, result)

    def test_months_non_boundary2a(self) -> None:
        """Rounding up to the next multiple of months when dt is not on a boundary."""
        # For a 2-month step, using dt = March 15, 2025 should round up to May 1, 2025.
        dt = datetime(2025, 3, 15, 10, 30, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "monthly")
        expected = datetime(2025, 5, 1, 0, 0, 0, tzinfo=self.tz)
        self.assertEqual(expected, result)

    def test_months_non_boundary2b(self) -> None:
        """Rounding up to the next multiple of months when dt is not on a boundary."""
        # For a 2-month step (Jan, Mar, May...), using dt = April 15, 2025 should round up to May 1, 2025.
        dt = datetime(2025, 4, 15, 10, 30, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "monthly")
        expected = datetime(2025, 5, 1, 0, 0, 0, tzinfo=self.tz)
        self.assertEqual(expected, result)

    def test_months_boundary(self) -> None:
        """When dt is exactly on a month boundary that is a multiple, dt is returned unchanged."""
        dt = datetime(2025, 3, 1, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "monthly")
        self.assertEqual(dt, result)

    def test_years_non_boundary2a(self) -> None:
        """Rounding up to the next multiple of years when dt is not on a boundary."""
        # For a 2-year step with an anchor phase starting in 2025 (e.g. 2025, 2027...),
        # using dt = Feb 11, 2024 should round up to Jan 1, 2025.
        dt = datetime(2024, 2, 11, 14, 5, tzinfo=self.tz)
        anchors = PeriodAnchors(yearly_year=2025)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly", anchors=anchors)
        expected = datetime(2025, 1, 1, 0, 0, 0, tzinfo=self.tz)
        self.assertEqual(expected, result)

    def test_years_non_boundary2b(self) -> None:
        """Rounding up to the next multiple of years when dt is not on a boundary."""
        # For a 2-year step, using dt = Feb 11, 2025 should round up to Jan 1, 2027.
        dt = datetime(2025, 2, 11, 14, 5, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly")
        expected = datetime(2027, 1, 1, 0, 0, 0, tzinfo=self.tz)
        self.assertEqual(expected, result)

    def test_years_boundary(self) -> None:
        """When dt is exactly on a year boundary that is a multiple, dt is returned unchanged."""
        # January 1, 2025 is on a valid boundary if (2025-1) % 2 == 0.
        dt = datetime(2025, 1, 1, 0, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly")
        self.assertEqual(dt, result)

    def test_invalid_unit(self) -> None:
        """Passing an unsupported time unit should raise a ValueError."""
        dt = datetime(2025, 2, 11, 14, 5, tzinfo=self.tz)
        with self.assertRaises(ValueError):
            round_datetime_up_to_duration_multiple(dt, 2, "fortnights")

    def test_preserves_timezone(self) -> None:
        """The returned datetime must have the same timezone as the input."""
        dt = datetime(2025, 2, 11, 14, 5, 1, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "hourly")
        self.assertEqual(dt.tzinfo, result.tzinfo)

    def test_custom_hourly_anchor(self) -> None:
        # Custom hourly: snapshots occur at :15:30 each hour.
        dt = datetime(2025, 2, 11, 14, 20, 0, tzinfo=self.tz)
        # Global base (daily) is 00:00:00, so effective hourly base = 00:15:30.
        # dt is 14:20:00; offset = 14h04m30s → next multiple with 1-hour step: 15:15:30.
        expected = datetime(2025, 2, 11, 15, 15, 30, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(
            dt, 1, "hourly", anchors=bzfs.PeriodAnchors(hourly_minute=15, hourly_second=30)
        )
        self.assertEqual(expected, result)

    def test_custom_hourly_anchor2a(self) -> None:
        # Custom hourly: snapshots occur at :15:30 every other hour.
        dt = datetime(2025, 2, 11, 14, 20, 0, tzinfo=self.tz)
        # Global base (daily) is 00:00:00, so effective hourly base = 00:15:30.
        # dt is 14:20:00; offset = 14h04m30s → next multiple with 1-hour step: 15:15:30.
        expected = datetime(2025, 2, 11, 16, 15, 30, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(
            dt, 2, "hourly", anchors=bzfs.PeriodAnchors(hourly_minute=15, hourly_second=30)
        )
        self.assertEqual(expected, result)

    def test_custom_hourly_anchor2b(self) -> None:
        # Custom hourly: snapshots occur at :15:30 every other hour.
        dt = datetime(2025, 2, 11, 15, 20, 0, tzinfo=self.tz)
        # Global base (daily) is 00:00:00, so effective hourly base = 00:15:30.
        # dt is 14:20:00; offset = 14h04m30s → next multiple with 1-hour step: 15:15:30.
        expected = datetime(2025, 2, 11, 16, 15, 30, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(
            dt, 2, "hourly", anchors=bzfs.PeriodAnchors(hourly_minute=15, hourly_second=30)
        )
        self.assertEqual(expected, result)

    def test_custom_daily_anchor(self) -> None:
        # Custom daily: snapshots occur at 01:30:00.
        custom = bzfs.PeriodAnchors(daily_hour=1, daily_minute=30, daily_second=0)
        dt = datetime(2025, 2, 11, 0, 45, 0, tzinfo=self.tz)
        # Global base = previous day at 01:30:00, so next boundary = that + 1 day.
        yesterday = dt.replace(hour=custom.daily_hour, minute=custom.daily_minute, second=custom.daily_second)
        if yesterday > dt:
            yesterday -= timedelta(days=1)
        expected = yesterday + timedelta(days=1)
        result = round_datetime_up_to_duration_multiple(dt, 1, "daily", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_weekly_anchor1a(self) -> None:
        # Custom weekly: every week, weekly_weekday=2, weekly time = 03:00:00.
        custom = bzfs.PeriodAnchors(weekly_weekday=2, weekly_hour=3, weekly_minute=0, weekly_second=0)
        dt = datetime(2025, 2, 13, 5, 0, 0, tzinfo=self.tz)  # Thursday
        anchor = (dt - timedelta(days=2)).replace(hour=3, minute=0, second=0, microsecond=0)
        expected = anchor + timedelta(weeks=1)
        result = round_datetime_up_to_duration_multiple(dt, 1, "weekly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_monthly_anchor1a(self) -> None:
        # Custom monthly: snapshots every month on the 15th at 12:00:00.
        custom = bzfs.PeriodAnchors(monthly_monthday=15, monthly_hour=12, monthly_minute=0, monthly_second=0)
        dt = datetime(2025, 4, 20, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 5, 15, 12, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_monthly_anchor1b(self) -> None:
        # Custom monthly: snapshots every month on the 15th at 12:00:00.
        custom = bzfs.PeriodAnchors(monthly_monthday=15, monthly_hour=12, monthly_minute=0, monthly_second=0)
        dt = datetime(2025, 5, 12, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 5, 15, 12, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_monthly_anchor2a(self) -> None:
        # Custom monthly: snapshots every other month on the 15th at 12:00:00.
        custom = bzfs.PeriodAnchors(monthly_monthday=15, monthly_hour=12, monthly_minute=0, monthly_second=0)
        dt = datetime(2025, 4, 20, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 5, 15, 12, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "monthly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_monthly_anchor2b(self) -> None:
        # Custom monthly: snapshots every other month on the 15th at 12:00:00.
        custom = bzfs.PeriodAnchors(monthly_monthday=15, monthly_hour=12, monthly_minute=0, monthly_second=0)
        dt = datetime(2025, 5, 12, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 5, 15, 12, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "monthly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_monthly_anchor2c(self) -> None:
        # Custom monthly: snapshots every other month on the 15th at 12:00:00.
        custom = bzfs.PeriodAnchors(monthly_monthday=15, monthly_hour=12, monthly_minute=0, monthly_second=0)
        dt = datetime(2025, 5, 17, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 7, 15, 12, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "monthly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_yearly_anchor1a(self) -> None:
        # Custom yearly: snapshots on June 30 at 02:00:00.
        custom = bzfs.PeriodAnchors(yearly_month=6, yearly_monthday=30, yearly_hour=2, yearly_minute=0, yearly_second=0)
        dt = datetime(2024, 3, 15, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2024, 6, 30, 2, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "yearly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_yearly_anchor1b(self) -> None:
        # Custom yearly: snapshots on June 30 at 02:00:00.
        custom = bzfs.PeriodAnchors(yearly_month=6, yearly_monthday=30, yearly_hour=2, yearly_minute=0, yearly_second=0)
        dt = datetime(2025, 3, 15, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 6, 30, 2, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "yearly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_yearly_anchor2a(self) -> None:
        # Custom yearly: snapshots every other year on June 30 at 02:00:00.
        custom = bzfs.PeriodAnchors(yearly_month=6, yearly_monthday=30, yearly_hour=2, yearly_minute=0, yearly_second=0)
        dt = datetime(2024, 3, 15, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 6, 30, 2, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_yearly_anchor2b(self) -> None:
        # Custom yearly: snapshots every other year on June 30 at 02:00:00.
        custom = bzfs.PeriodAnchors(yearly_month=6, yearly_monthday=30, yearly_hour=2, yearly_minute=0, yearly_second=0)
        dt = datetime(2025, 3, 15, 10, 0, 0, tzinfo=self.tz)
        expected = datetime(2025, 6, 30, 2, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly", anchors=custom)
        self.assertEqual(expected, result)

    def test_custom_yearly_anchor2c(self) -> None:
        # Schedule: Every 2 years, starting in 2025.
        # dt: 2025-02-11 (which is AFTER the default anchor of 2025-01-01)
        # Expected result: 2027-01-01
        dt = datetime(2025, 2, 11, 14, 5, tzinfo=self.tz)
        anchors = PeriodAnchors(yearly_year=2025)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly", anchors=anchors)
        expected = datetime(2027, 1, 1, 0, 0, 0, tzinfo=self.tz)
        self.assertEqual(expected, result)

    def test_timezone_preservation(self) -> None:
        if sys.version_info < (3, 9):
            self.skipTest("ZoneInfo requires python >= 3.9")
        from zoneinfo import ZoneInfo  # requires python >= 3.9

        # Create a timezone with DST
        tz = ZoneInfo("America/New_York")

        # Create a timestamp just before DST transition
        dt_before_dst = datetime(2023, 3, 12, 1, 30, 0, tzinfo=tz)  # Just before "spring forward"

        # Create custom anchors
        custom = bzfs.PeriodAnchors(daily_hour=3, daily_minute=0, daily_second=0)

        # Round to next daily anchor - should be 3:00 same day, respecting DST change
        result = round_datetime_up_to_duration_multiple(dt_before_dst, 1, "daily", anchors=custom)

        # Expected result should be 3:00 AM after DST shift (which is actually 4:00 AM in wall clock time)
        # If timezone is handled incorrectly, this will be off by an hour
        expected = datetime(2023, 3, 12, 3, 0, 0, tzinfo=tz)

        # This will fail if the timezone handling is incorrect
        self.assertEqual(expected.hour, result.hour)
        self.assertEqual(expected.tzinfo, result.tzinfo)
        self.assertEqual(expected.utcoffset(), result.utcoffset())

    def test_monthly_anchor_invalid_day(self) -> None:
        # Custom monthly: snapshots every month on the 31st
        custom = bzfs.PeriodAnchors(monthly_monthday=31, monthly_hour=12, monthly_minute=0, monthly_second=0)

        # February case - should round to Feb 28th (or 29th in leap year) but instead will incorrectly handle this
        dt = datetime(2025, 2, 15, 10, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=custom)

        # This will fail if the function tries to create an invalid date (Feb 31st)
        # or incorrectly jumps to March 31st instead of using Feb 28th
        expected = datetime(2025, 2, 28, 12, 0, 0, tzinfo=self.tz)
        self.assertEqual(expected, result)

    def test_monthly_with_custom_month_anchor(self) -> None:
        """Tests that the `monthly_month` anchor correctly sets the cycle phase."""
        # Anchor: every 1 month, starting in March (month=3), on the 1st day.
        custom_anchors = PeriodAnchors(monthly_month=3, monthly_monthday=1)

        # If it's Jan 15, the next anchor month is February.
        dt = datetime(2025, 1, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2025, 2, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=custom_anchors)
        self.assertEqual(expected, result)

        # If it's March 15, the next anchor month is April.
        dt2 = datetime(2025, 3, 15, 10, 0, tzinfo=self.tz)
        expected2 = datetime(2025, 4, 1, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 1, "monthly", anchors=custom_anchors)
        self.assertEqual(expected2, result2)

    def test_multi_month_with_custom_month_anchor(self) -> None:
        """Tests multi-month logic with a custom `monthly_month` anchor."""
        # Anchor: every 2 months (bi-monthly), starting in April (month=4).
        # Schedule: Apr 1, Jun 1, Aug 1, Oct 1, Dec 1, Feb 1 (+1 year), etc.
        custom_anchors = PeriodAnchors(monthly_month=4, monthly_monthday=2)

        # Test Case 1: Current date is Mar 15. Next snapshot should be Apr 1.
        dt1 = datetime(2025, 3, 15, 10, 0, tzinfo=self.tz)
        expected1 = datetime(2025, 4, 2, 0, 0, tzinfo=self.tz)
        result1 = round_datetime_up_to_duration_multiple(dt1, 2, "monthly", anchors=custom_anchors)
        self.assertEqual(expected1, result1)

        # Test Case 2: Current date is April 15. Next snapshot should be June 1.
        dt2 = datetime(2025, 4, 15, 10, 0, tzinfo=self.tz)
        expected2 = datetime(2025, 6, 2, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 2, "monthly", anchors=custom_anchors)
        self.assertEqual(expected2, result2)

        # Test Case 3: Current date is Dec 15. Next snapshot is Feb 1 of next year.
        dt3 = datetime(2025, 12, 15, 10, 0, tzinfo=self.tz)
        expected3 = datetime(2026, 2, 2, 0, 0, tzinfo=self.tz)
        result3 = round_datetime_up_to_duration_multiple(dt3, 2, "monthly", anchors=custom_anchors)
        self.assertEqual(expected3, result3)

    def test_quarterly_scheduling(self) -> None:
        """Tests a common use case: quarterly snapshots."""
        # Anchor: every 3 months, starting in January (month=1).
        # Schedule: Jan 1, Apr 1, Jul 1, Oct 1.
        custom_anchors = PeriodAnchors(monthly_month=1, monthly_monthday=1)

        # Test Case 1: In February. Next is Apr 1.
        dt1 = datetime(2025, 2, 20, 10, 0, tzinfo=self.tz)
        expected1 = datetime(2025, 4, 1, 0, 0, tzinfo=self.tz)
        result1 = round_datetime_up_to_duration_multiple(dt1, 3, "monthly", anchors=custom_anchors)
        self.assertEqual(expected1, result1)

        # Test Case 2: In June. Next is Jul 1.
        dt2 = datetime(2025, 6, 1, 12, 0, tzinfo=self.tz)
        expected2 = datetime(2025, 7, 1, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 3, "monthly", anchors=custom_anchors)
        self.assertEqual(expected2, result2)

        # Test Case 3: In November. Next is Jan 1 of next year.
        dt3 = datetime(2025, 11, 5, 10, 0, tzinfo=self.tz)
        expected3 = datetime(2026, 1, 1, 0, 0, tzinfo=self.tz)
        result3 = round_datetime_up_to_duration_multiple(dt3, 3, "monthly", anchors=custom_anchors)
        self.assertEqual(expected3, result3)

    def test_multi_year_scheduling_is_correct(self) -> None:
        """Verifies multi-year (3-year) scheduling logic with default anchors."""
        # Schedule: Jan 1 of 2025, 2028, 2031, etc.
        anchors = PeriodAnchors(yearly_year=2025)

        # Case 1: In 2026. Next is 2028.
        dt1 = datetime(2026, 6, 15, 10, 0, tzinfo=self.tz)
        expected1 = datetime(2028, 1, 1, 0, 0, tzinfo=self.tz)
        result1 = round_datetime_up_to_duration_multiple(dt1, 3, "yearly", anchors=anchors)
        self.assertEqual(expected1, result1)

        # Case 2: In 2028, but after the anchor date. Next is 2031.
        dt2 = datetime(2028, 2, 1, 12, 0, tzinfo=self.tz)
        expected2 = datetime(2031, 1, 1, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 3, "yearly", anchors=anchors)
        self.assertEqual(expected2, result2)

    def test_multi_year_with_custom_anchor_properties(self) -> None:
        """Verifies multi-year scheduling with custom month and day anchors."""
        # Schedule: Every 2 years, on July 4th, starting in year 2024.
        # Valid snapshots: 2024-07-04, 2026-07-04, 2028-07-04...
        anchors = PeriodAnchors(yearly_year=2024, yearly_month=7, yearly_monthday=4)

        # Case 1: In 2025. Next boundary is 2026-07-04.
        dt1 = datetime(2025, 8, 1, 10, 0, tzinfo=self.tz)
        expected1 = datetime(2026, 7, 4, 0, 0, tzinfo=self.tz)
        result1 = round_datetime_up_to_duration_multiple(dt1, 2, "yearly", anchors=anchors)
        self.assertEqual(expected1, result1)

        # Case 2: In 2026, but before the anchor date. Next is 2026-07-04.
        dt2 = datetime(2026, 3, 10, 10, 0, tzinfo=self.tz)
        expected2 = datetime(2026, 7, 4, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 2, "yearly", anchors=anchors)
        self.assertEqual(expected2, result2)

        # Case 3 (Boundary): Exactly on the anchor date.
        dt3 = datetime(2026, 7, 4, 0, 0, tzinfo=self.tz)
        result3 = round_datetime_up_to_duration_multiple(dt3, 2, "yearly", anchors=anchors)
        self.assertEqual(dt3, result3)

        # Case 4 (Just after Boundary): One second after the anchor date.
        dt4 = datetime(2026, 7, 4, 0, 0, 1, tzinfo=self.tz)
        expected4 = datetime(2028, 7, 4, 0, 0, tzinfo=self.tz)
        result4 = round_datetime_up_to_duration_multiple(dt4, 2, "yearly", anchors=anchors)
        self.assertEqual(expected4, result4)

    def test_monthly_boundary_exact_time(self) -> None:
        """Tests that a dt exactly on a monthly anchor is returned unchanged."""
        anchors = PeriodAnchors(monthly_monthday=15, monthly_hour=10)
        dt = datetime(2025, 4, 15, 10, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(dt, result)

    def test_monthly_just_after_boundary(self) -> None:
        """Tests that a dt just after a monthly anchor correctly advances to the next month."""
        anchors = PeriodAnchors(monthly_monthday=15, monthly_hour=10)
        dt = datetime(2025, 4, 15, 10, 0, 1, tzinfo=self.tz)  # 1 microsecond after
        expected = datetime(2025, 5, 15, 10, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_before_anchor_day_in_same_month(self) -> None:
        """Tests when dt is in the correct month but before the anchor day."""
        anchors = PeriodAnchors(monthly_monthday=25)
        dt = datetime(2025, 4, 10, 10, 0, tzinfo=self.tz)
        expected = datetime(2025, 4, 25, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_end_of_month_rollover(self) -> None:
        """Tests monthly scheduling at the end of a year."""
        anchors = PeriodAnchors(monthly_monthday=1)
        dt = datetime(2025, 12, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2026, 1, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_anchor_on_31st_for_short_month(self) -> None:
        """Tests that an anchor on day 31 correctly resolves to the last day of a shorter month."""
        anchors = PeriodAnchors(monthly_monthday=31)
        # Next boundary after April 15 should be April 30th
        dt = datetime(2025, 4, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2025, 4, 30, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "monthly", anchors=anchors)
        self.assertEqual(expected, result)
        # Next boundary after Feb 15 (non-leap) should be Feb 28th
        dt2 = datetime(2025, 2, 15, 10, 0, tzinfo=self.tz)
        expected2 = datetime(2025, 2, 28, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 1, "monthly", anchors=anchors)
        self.assertEqual(expected2, result2)

    def test_multi_month_with_phase_wrapping_year(self) -> None:
        """Tests a multi-month schedule where the anchor phase causes cycles to straddle year boundaries."""
        # Schedule: Every 4 months, starting in November.
        # Cycles: ..., Jul 2024, Nov 2024, Mar 2025, Jul 2025, ...
        anchors = PeriodAnchors(monthly_month=11)
        # dt is Jan 2025. The last anchor was Nov 2024. Next is Mar 2025.
        dt = datetime(2025, 1, 20, 10, 0, tzinfo=self.tz)
        expected = datetime(2025, 3, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 4, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_monthly_cycle_with_future_anchor_month(self) -> None:
        """Tests that the monthly cycle works correctly when the anchor month is after the current month.
        This is already covered by other tests, but this one makes it explicit."""
        # Schedule: Every 3 months (quarterly). Anchor phase is set to start in November (month 11).
        # Schedule is Feb, May, Aug, Nov.
        anchors = PeriodAnchors(monthly_month=11)

        # Current date is March 2025. The last anchor was Nov 2024. The one before that was Aug 2024.
        # The period `dt` falls into started in Feb 2025. The next boundary is May 2025.
        dt = datetime(2025, 3, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2025, 5, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 3, "monthly", anchors=anchors)
        self.assertEqual(expected, result)

    # --- Comprehensive Tests for Yearly Scheduling ---

    def test_yearly_just_before_boundary(self) -> None:
        """Tests that a dt just before a yearly anchor correctly snaps to that anchor."""
        anchors = PeriodAnchors(yearly_month=7, yearly_monthday=4)
        dt = datetime(2025, 7, 3, 23, 59, 59, tzinfo=self.tz)
        expected = datetime(2025, 7, 4, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "yearly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_yearly_on_exact_boundary_with_offset_cycle(self) -> None:
        """Tests a dt exactly on a multi-year anchor boundary."""
        # Schedule: Every 3 years, phase starting 2022. (2022, 2025, 2028...)
        anchors = PeriodAnchors(yearly_year=2022)
        dt = datetime(2025, 1, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 3, "yearly", anchors=anchors)
        self.assertEqual(dt, result)

    def test_yearly_just_after_boundary_with_offset_cycle(self) -> None:
        """Tests a dt just after a multi-year anchor boundary."""
        # Schedule: Every 3 years, phase starting 2022. (2022, 2025, 2028...)
        anchors = PeriodAnchors(yearly_year=2022)
        dt = datetime(2025, 1, 1, 0, 0, 1, tzinfo=self.tz)  # 1 microsecond after
        expected = datetime(2028, 1, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 3, "yearly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_yearly_leap_year_anchor_from_non_leap(self) -> None:
        """Tests scheduling for a Feb 29 anchor when the current year is not a leap year."""
        # Schedule: Every year on Feb 29.
        anchors = PeriodAnchors(yearly_month=2, yearly_monthday=29)
        # dt is in 2025 (non-leap). The next valid Feb 29 is in 2028.
        # The anchor for 2025 is 2025-02-28. Since dt is past it, the next is 2026-02-28.
        dt = datetime(2025, 3, 1, 10, 0, tzinfo=self.tz)
        expected = datetime(2026, 2, 28, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "yearly", anchors=anchors)
        self.assertEqual(expected, result)

    def test_yearly_leap_year_anchor_from_leap(self) -> None:
        """Tests scheduling for a Feb 29 anchor when the current year is a leap year."""
        # Schedule: Every year on Feb 29.
        anchors = PeriodAnchors(yearly_month=2, yearly_monthday=29)
        # dt is Jan 2028 (leap). The next boundary is Feb 29, 2028.
        dt = datetime(2028, 1, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2028, 2, 29, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 1, "yearly", anchors=anchors)
        self.assertEqual(expected, result)
        # dt is Mar 2028 (leap), after the anchor. Next boundary is Feb 28, 2029.
        dt2 = datetime(2028, 3, 1, 10, 0, tzinfo=self.tz)
        expected2 = datetime(2029, 2, 28, 0, 0, tzinfo=self.tz)
        result2 = round_datetime_up_to_duration_multiple(dt2, 1, "yearly", anchors=anchors)
        self.assertEqual(expected2, result2)

    def test_yearly_cycle_with_future_anchor_year(self) -> None:
        """Tests that the cycle phase works correctly even if the anchor year is in the future.
        The anchor year should only define the phase (e.g., odd/even years), not a starting point."""
        # Schedule: Every 2 years. The anchor phase is defined by year 2051 (an odd year).
        # This means snapshots should occur in ..., 2023, 2025, 2027, ...
        anchors = PeriodAnchors(yearly_year=2051)

        # Current date is in 2024. The next snapshot should be in 2025.
        dt = datetime(2024, 5, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2025, 1, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly", anchors=anchors)
        self.assertEqual(expected, result)

        anchors = PeriodAnchors(yearly_year=2050)

        # Current date is in 2024. The next snapshot should be in 2026.
        dt = datetime(2024, 5, 15, 10, 0, tzinfo=self.tz)
        expected = datetime(2026, 1, 1, 0, 0, tzinfo=self.tz)
        result = round_datetime_up_to_duration_multiple(dt, 2, "yearly", anchors=anchors)
        self.assertEqual(expected, result)


#############################################################################
class TestFindMatch(unittest.TestCase):

    def test_basic(self) -> None:
        condition = lambda arg: arg.startswith("-")  # noqa: E731

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
        start: Optional[int] = None,
        end: Optional[int] = None,
        raises: Optional[Union[bool, str, Callable[[], str]]] = False,
    ) -> None:
        raise_arg = cast(Union[bool, str, Callable[[], str]], raises)
        self.assertEqual(expected, find_match(lst, condition, start=start, end=end, reverse=False, raises=raise_arg))
        self.assertEqual(expected, find_match(lst, condition, start=start, end=end, reverse=True, raises=raise_arg))


#############################################################################
class TestArgumentParser(unittest.TestCase):

    def test_help(self) -> None:
        if is_solaris_zfs():
            self.skipTest("FIXME: BlockingIOError: [Errno 11] write could not complete without blocking")
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["--help"])
        self.assertEqual(0, e.exception.code)

    def test_version(self) -> None:
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["--version"])
        self.assertEqual(0, e.exception.code)

    def test_missing_datasets(self) -> None:
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["--retries=1"])
        self.assertEqual(2, e.exception.code)

    def test_missing_dst_dataset(self) -> None:
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["src_dataset"])  # Each SRC_DATASET must have a corresponding DST_DATASET
        self.assertEqual(2, e.exception.code)

    def test_program_must_not_be_empty_string(self) -> None:
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["src_dataset", "src_dataset", "--zfs-program="])
        self.assertEqual(2, e.exception.code)


#############################################################################
class TestDatasetPairsAction(unittest.TestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--input", nargs="+", action=bzfs.DatasetPairsAction)

    def test_direct_value(self) -> None:
        args = self.parser.parse_args(["--input", "src1", "dst1"])
        self.assertEqual(args.input, [("src1", "dst1")])

    def test_direct_value_without_corresponding_dst(self) -> None:
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["--input", "src1"])

    def test_file_input(self) -> None:
        with patch("builtins.open", mock_open(read_data="src1\tdst1\nsrc2\tdst2\n")):
            args = self.parser.parse_args(["--input", "+testfile"])
            self.assertEqual(args.input, [("src1", "dst1"), ("src2", "dst2")])

    def test_file_input_without_trailing_newline(self) -> None:
        with patch("builtins.open", mock_open(read_data="src1\tdst1\nsrc2\tdst2")):
            args = self.parser.parse_args(["--input", "+testfile"])
            self.assertEqual(args.input, [("src1", "dst1"), ("src2", "dst2")])

    def test_mixed_input(self) -> None:
        with patch("builtins.open", mock_open(read_data="src1\tdst1\nsrc2\tdst2\n")):
            args = self.parser.parse_args(["--input", "src0", "dst0", "+testfile"])
            self.assertEqual(args.input, [("src0", "dst0"), ("src1", "dst1"), ("src2", "dst2")])

    def test_file_skip_comments_and_empty_lines(self) -> None:
        with patch("builtins.open", mock_open(read_data="\n\n#comment\nsrc1\tdst1\nsrc2\tdst2\n")):
            args = self.parser.parse_args(["--input", "+testfile"])
            self.assertEqual(args.input, [("src1", "dst1"), ("src2", "dst2")])

    def test_file_skip_stripped_empty_lines(self) -> None:
        with patch("builtins.open", mock_open(read_data=" \t \nsrc1\tdst1")):
            args = self.parser.parse_args(["--input", "+testfile"])
            self.assertEqual(args.input, [("src1", "dst1")])

    def test_file_missing_tab(self) -> None:
        with patch("builtins.open", mock_open(read_data="src1\nsrc2")):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+testfile"])

    def test_file_whitespace_only(self) -> None:
        with patch("builtins.open", mock_open(read_data=" \tdst1")):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+testfile"])

        with patch("builtins.open", mock_open(read_data="src1\t ")):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+testfile"])

        with patch("builtins.open", side_effect=FileNotFoundError):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+nonexistentfile"])

    def test_option_not_specified(self) -> None:
        args = self.parser.parse_args([])
        self.assertIsNone(args.input)


#############################################################################
class TestFileOrLiteralAction(unittest.TestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--input", nargs="+", action=bzfs.FileOrLiteralAction)

    def test_direct_value(self) -> None:
        args = self.parser.parse_args(["--input", "literalvalue"])
        self.assertEqual(args.input, ["literalvalue"])

    def test_file_input(self) -> None:
        with patch("builtins.open", mock_open(read_data="line 1\nline 2  \n")):
            args = self.parser.parse_args(["--input", "+testfile"])
            self.assertEqual(args.input, ["line 1", "line 2  "])

    def test_mixed_input(self) -> None:
        with patch("builtins.open", mock_open(read_data="line 1\nline 2")):
            args = self.parser.parse_args(["--input", "literalvalue", "+testfile"])
            self.assertEqual(args.input, ["literalvalue", "line 1", "line 2"])

    def test_skip_comments_and_empty_lines(self) -> None:
        with patch("builtins.open", mock_open(read_data="\n\n#comment\nline 1\n\n\nline 2\n")):
            args = self.parser.parse_args(["--input", "+testfile"])
            self.assertEqual(args.input, ["line 1", "line 2"])

    def test_file_not_found(self) -> None:
        with patch("builtins.open", side_effect=FileNotFoundError):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+nonexistentfile"])

    def test_option_not_specified(self) -> None:
        args = self.parser.parse_args([])
        self.assertIsNone(args.input)


#############################################################################
class TestNewSnapshotFilterGroupAction(unittest.TestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--new-snapshot-filter-group", action=bzfs.NewSnapshotFilterGroupAction, nargs=0)

    def test_basic0(self) -> None:
        args = self.parser.parse_args(["--new-snapshot-filter-group"])
        self.assertListEqual([[]], getattr(args, bzfs.snapshot_filters_var))

    def test_basic1(self) -> None:
        args = self.parser.parse_args(["--new-snapshot-filter-group", "--new-snapshot-filter-group"])
        self.assertListEqual([[]], getattr(args, bzfs.snapshot_filters_var))


#############################################################################
class TestTimeRangeAction(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--time-n-ranks", action=bzfs.TimeRangeAndRankRangeAction, nargs="+")

    def parse_duration(self, arg: str) -> argparse.Namespace:
        return self.parser.parse_args(["--time-n-ranks", arg + " ago..anytime"])

    def parse_timestamp(self, arg: str) -> argparse.Namespace:
        return self.parser.parse_args(["--time-n-ranks", arg + "..anytime"])

    def test_parse_unix_time(self) -> None:  # Test Unix time in integer seconds
        args = self.parse_timestamp("1696510080")
        self.assertEqual(args.time_n_ranks[0][0], 1696510080)

        args = self.parse_timestamp("0")
        self.assertEqual(args.time_n_ranks[0][0], 0)

    def test_valid_durations(self) -> None:
        args = self.parse_duration("5millis")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(milliseconds=5))

        args = self.parse_duration("5milliseconds")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(milliseconds=5))

        args = self.parse_duration("5secs")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=5))

        args = self.parse_duration("5 seconds")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=5))

        args = self.parse_duration("30mins")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=1800))

        args = self.parse_duration("30minutes")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=1800))

        args = self.parse_duration("2hours")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=2 * 60 * 60))

        args = self.parse_duration("0days")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=0))

        args = self.parse_duration("10weeks")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=6048000))

        args = self.parse_duration("15secs")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=15))

        args = self.parse_duration("60mins")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=3600))
        self.assertEqual(args.time_n_ranks[0][0].total_seconds(), 3600)

        args = self.parse_duration("2hours")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=2 * 60 * 60))

        args = self.parse_duration("3days")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=259200))

        args = self.parse_duration("2weeks")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=1209600))

        # Test with spaces
        args = self.parse_duration("  30mins")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=1800))

        args = self.parse_duration("5secs")
        self.assertEqual(args.time_n_ranks[0][0], timedelta(seconds=5))

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
        self.assertEqual(args.time_n_ranks[0][0], int(datetime.fromisoformat("2024-01-01").timestamp()))

        args = self.parse_timestamp("2024-12-31")
        self.assertEqual(args.time_n_ranks[0][0], int(datetime.fromisoformat("2024-12-31").timestamp()))

        args = self.parse_timestamp("2024-10-05T14:48:55")
        self.assertEqual(args.time_n_ranks[0][0], int(datetime.fromisoformat("2024-10-05T14:48:55").timestamp()))
        self.assertNotEqual(args.time_n_ranks[0][0], int(datetime.fromisoformat("2024-10-05T14:48:01").timestamp()))

    def test_parse_datetime_with_timezone(self) -> None:
        tz_py_version = (3, 11)
        if sys.version_info < tz_py_version:
            self.skipTest("Timezone support in datetime.fromisoformat() requires python >= 3.11")

        # Test ISO 8601 datetime strings with timezone info
        args = self.parse_timestamp("2024-10-05T14:48:55+02")
        self.assertEqual(args.time_n_ranks[0][0], int(datetime.fromisoformat("2024-10-05T14:48:55+02:00").timestamp()))

        args = self.parse_timestamp("2024-10-05T14:48:55+00:00")
        self.assertEqual(args.time_n_ranks[0][0], int(datetime.fromisoformat("2024-10-05T14:48:55+00:00").timestamp()))

        args = self.parse_timestamp("2024-10-05T14:48:55-04:30")
        self.assertEqual(args.time_n_ranks[0][0], int(datetime.fromisoformat("2024-10-05T14:48:55-04:30").timestamp()))

        args = self.parse_timestamp("2024-10-05T14:48:55+02:00")
        self.assertEqual(args.time_n_ranks[0][0], int(datetime.fromisoformat("2024-10-05T14:48:55+02:00").timestamp()))

    def test_get_include_snapshot_times(self) -> None:
        times_and_ranks_opt = "--include-snapshot-times-and-ranks="
        wildcards = ["*", "anytime"]

        args = argparser_parse_args(args=["src", "dst"])
        p = bzfs.Params(args)
        self.assertListEqual([[]], p.snapshot_filters)

        args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "*..*"])
        p = bzfs.Params(args)
        self.assertListEqual([[]], p.snapshot_filters)

        args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "anytime..anytime"])
        p = bzfs.Params(args)
        self.assertListEqual([[]], p.snapshot_filters)

        args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "notime"])
        p = bzfs.Params(args)
        self.assertEqual((0, 0), p.snapshot_filters[0][0].timerange)

        args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "1700000000..1700000001"])
        p = bzfs.Params(args)
        self.assertEqual((1700000000, 1700000001), p.snapshot_filters[0][0].timerange)

        args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "1700000001..1700000000"])
        p = bzfs.Params(args)
        self.assertEqual((1700000000, 1700000001), p.snapshot_filters[0][0].timerange)

        args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "0secs ago..60secs ago"])
        p = bzfs.Params(args)
        timerange = cast(Tuple[Union[timedelta, int], Union[timedelta, int]], p.snapshot_filters[0][0].timerange)
        self.assertEqual(timedelta(seconds=0), timerange[0])
        self.assertEqual(timedelta(seconds=60), timerange[1])

        args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "1secs ago..60secs ago"])
        p = bzfs.Params(args)
        timerange = cast(Tuple[Union[timedelta, int], Union[timedelta, int]], p.snapshot_filters[0][0].timerange)
        self.assertEqual(timedelta(seconds=1), timerange[0])
        self.assertEqual(timedelta(seconds=60), timerange[1])

        for wildcard in wildcards:
            args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "2024-01-01.." + wildcard])
            p = bzfs.Params(args)
            timerange = cast(Tuple[Union[timedelta, int], Union[timedelta, int]], p.snapshot_filters[0][0].timerange)
            self.assertEqual(int(datetime.fromisoformat("2024-01-01").timestamp()), timerange[0])
            self.assertLess(int(time.time() + 86400 * 365 * 1000), cast(int, timerange[1]))

            args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + wildcard + "..2024-01-01"])
            p = bzfs.Params(args)
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

    @staticmethod
    def filter_snapshots_by_times_and_rank1(
        snapshots: List[str], timerange: str, ranks: List[str] = [], loglevel: int = logging.DEBUG  # noqa: B006
    ) -> List[str]:
        return filter_snapshots_by_times_and_rank(snapshots, timerange=timerange, ranks=ranks, loglevel=loglevel)


def filter_snapshots_by_times_and_rank(
    snapshots: List[str], timerange: str, ranks: List[str] = [], loglevel: int = logging.DEBUG  # noqa: B006
) -> List[str]:
    args = argparser_parse_args(args=["src", "dst", "--include-snapshot-times-and-ranks", timerange, *ranks])
    log_params = bzfs.LogParams(args)
    try:
        job = bzfs.Job()
        job.params = bzfs.Params(args, log_params=log_params, log=bzfs.get_logger(log_params, args))
        job.params.log.setLevel(loglevel)
        snapshots = [f"{i}\t" + snapshot for i, snapshot in enumerate(snapshots)]  # simulate creation time
        results = job.filter_snapshots(snapshots)
        results = [result.split("\t", 1)[1] for result in results]  # drop creation time
        return results
    finally:
        bzfs.reset_logger()


#############################################################################
class TestRankRangeAction(unittest.TestCase):

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
        self, snapshots: List[str], ranks: List[str], timerange: str = "0..0", loglevel: int = logging.DEBUG
    ) -> List[str]:
        return filter_snapshots_by_times_and_rank(snapshots, timerange=timerange, ranks=ranks, loglevel=loglevel)

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

    @staticmethod
    def get_snapshot_filters(cli: List[str]) -> Any:
        args = argparser_parse_args(args=["src", "dst", *cli])
        return bzfs.Params(args).snapshot_filters[0]

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
        self.assertEqual(bzfs.snapshot_regex_filter_name, regex_filter.name)
        self.assertEqual((["w", "m"], ["f", "d", "h"]), regex_filter.options)

    def test_merge_adjacent_snapshot_regexes_doesnt_merge_across_groups(self) -> None:
        include = "--include-snapshot-regex"
        exclude = "--exclude-snapshot-regex"
        ranks = "--include-snapshot-times-and-ranks"
        cli = [include, ".*daily", exclude, ".*weekly", include, ".*hourly", ranks, "0..0", "oldest 5%", exclude, ".*m"]
        regex_filter1, ranks_filter, regex_filter2 = self.get_snapshot_filters(cli)
        self.assertEqual(bzfs.snapshot_regex_filter_name, regex_filter1.name)
        self.assertEqual(([".*weekly"], [".*daily", ".*hourly"]), regex_filter1.options)
        self.assertEqual((0, 0), ranks_filter.timerange)
        self.assertEqual("include_snapshot_times_and_ranks", ranks_filter.name)
        self.assertEqual((("oldest", 0, False), ("oldest", 5, True)), ranks_filter.options[0])
        self.assertEqual(bzfs.snapshot_regex_filter_name, regex_filter2.name)
        self.assertEqual(([".*m"], []), regex_filter2.options)

    def test_reorder_snapshot_times_simple(self) -> None:
        include = "--include-snapshot-regex"
        times = "--include-snapshot-times-and-ranks"
        cli = [include, ".*daily", times, "0..9"]
        times_filter, regex_filter = self.get_snapshot_filters(cli)
        self.assertEqual("include_snapshot_times", times_filter.name)
        self.assertEqual((0, 9), times_filter.timerange)
        self.assertEqual(bzfs.snapshot_regex_filter_name, regex_filter.name)
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
        self.assertEqual(bzfs.snapshot_regex_filter_name, regex_filter.name)
        self.assertEqual(([".*weekly"], [".*daily", ".*hourly"]), regex_filter.options)
        self.assertEqual("include_snapshot_times_and_ranks", ranks_filter.name)
        self.assertEqual((("oldest", 0, False), ("oldest", 1, False)), ranks_filter.options[0])
        self.assertEqual((0, 0), ranks_filter.timerange)


#############################################################################
class TestLogConfigVariablesAction(unittest.TestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--log-config-var", nargs="+", action=bzfs.LogConfigVariablesAction)

    def test_basic(self) -> None:
        args = self.parser.parse_args(["--log-config-var", "name1:val1", "name2:val2"])
        self.assertEqual(args.log_config_var, ["name1:val1", "name2:val2"])

        for var in ["", "  ", "varWithoutColon", ":valueWithoutName", " nameWithWhitespace:value"]:
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--log-config-var", var])


#############################################################################
class TestSafeFileNameAction(unittest.TestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("filename", action=bzfs.SafeFileNameAction)

    def test_safe_filename(self) -> None:
        args = self.parser.parse_args(["file1.txt"])
        self.assertEqual(args.filename, "file1.txt")

    def test_empty_filename(self) -> None:
        args = self.parser.parse_args([""])
        self.assertEqual(args.filename, "")

    def test_filename_in_subdirectory(self) -> None:
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["subdir/safe_file.txt"])

    def test_unsafe_filename_with_parent_directory_reference(self) -> None:
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["../escape.txt"])

    def test_unsafe_filename_with_absolute_path(self) -> None:
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["/unsafe_file.txt"])

    def test_unsafe_nested_parent_directory(self) -> None:
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["../../another_escape.txt"])

    def test_filename_with_single_dot_slash(self) -> None:
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["./file.txt"])


#############################################################################
class TestCheckRange(unittest.TestCase):

    def test_valid_range_min_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange, min=0, max=100)
        args = parser.parse_args(["--age", "50"])
        self.assertEqual(args.age, 50)

    def test_valid_range_inf_sup(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange, inf=0, sup=100)
        args = parser.parse_args(["--age", "50"])
        self.assertEqual(args.age, 50)

    def test_invalid_range_min_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange, min=0, max=100)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "-1"])

    def test_invalid_range_inf_sup(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange, inf=0, sup=100)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "101"])

    def test_invalid_combination_min_inf(self) -> None:
        with self.assertRaises(ValueError):
            parser = argparse.ArgumentParser()
            parser.add_argument("--age", type=int, action=bzfs.CheckRange, min=0, inf=100)

    def test_invalid_combination_max_sup(self) -> None:
        with self.assertRaises(ValueError):
            parser = argparse.ArgumentParser()
            parser.add_argument("--age", type=int, action=bzfs.CheckRange, max=0, sup=100)

    def test_valid_float_range_min_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(["--age", "50.5"])
        self.assertEqual(args.age, 50.5)

    def test_invalid_float_range_min_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange, min=0.0, max=100.0)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "-0.1"])

    def test_valid_edge_case_min(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(["--age", "0.0"])
        self.assertEqual(args.age, 0.0)

    def test_valid_edge_case_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(["--age", "100.0"])
        self.assertEqual(args.age, 100.0)

    def test_invalid_edge_case_sup(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange, inf=0.0, sup=100.0)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "100.0"])

    def test_invalid_edge_case_inf(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange, inf=0.0, sup=100.0)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "0.0"])

    def test_no_range_constraints(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange)
        args = parser.parse_args(["--age", "150"])
        self.assertEqual(args.age, 150)

    def test_no_range_constraints_float(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange)
        args = parser.parse_args(["--age", "150.5"])
        self.assertEqual(args.age, 150.5)

    def test_very_large_value(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange, max=10**18)
        args = parser.parse_args(["--age", "999999999999999999"])
        self.assertEqual(args.age, 999999999999999999)

    def test_very_small_value(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange, min=-(10**18))
        args = parser.parse_args(["--age", "-999999999999999999"])
        self.assertEqual(args.age, -999999999999999999)

    def test_default_interval(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange)
        action = bzfs.CheckRange(option_strings=["--age"], dest="age")
        self.assertEqual(action.interval(), "valid range: (-infinity, +infinity)")

    def test_interval_with_inf_sup(self) -> None:
        action = bzfs.CheckRange(option_strings=["--age"], dest="age", inf=0, sup=100)
        self.assertEqual(action.interval(), "valid range: (0, 100)")

    def test_interval_with_min_max(self) -> None:
        action = bzfs.CheckRange(option_strings=["--age"], dest="age", min=0, max=100)
        self.assertEqual(action.interval(), "valid range: [0, 100]")

    def test_interval_with_min(self) -> None:
        action = bzfs.CheckRange(option_strings=["--age"], dest="age", min=0)
        self.assertEqual(action.interval(), "valid range: [0, +infinity)")

    def test_interval_with_max(self) -> None:
        action = bzfs.CheckRange(option_strings=["--age"], dest="age", max=100)
        self.assertEqual(action.interval(), "valid range: (-infinity, 100]")

    def test_call_without_range_constraints(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange)
        args = parser.parse_args(["--age", "50"])
        self.assertEqual(args.age, 50)


#############################################################################
class TestCheckPercentRange(unittest.TestCase):

    def test_valid_range_min(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--threads", action=bzfs.CheckPercentRange, min=1)
        args = parser.parse_args(["--threads", "1"])
        threads, is_percent = args.threads
        self.assertEqual(threads, 1.0)
        self.assertFalse(is_percent)

    def test_valid_range_percent(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--threads", action=bzfs.CheckPercentRange, min=1)
        args = parser.parse_args(["--threads", "5.2%"])
        threads, is_percent = args.threads
        self.assertEqual(threads, 5.2)
        self.assertTrue(is_percent)

    def test_invalid(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--threads", action=bzfs.CheckPercentRange, min=1)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--threads", "0"])
        with self.assertRaises(SystemExit):
            parser.parse_args(["--threads", "0%"])
        with self.assertRaises(SystemExit):
            parser.parse_args(["--threads", "abc"])


#############################################################################
class TestPythonVersionCheck(unittest.TestCase):
    """Test version check near top of program:
    if sys.version_info < (3, 8):
        print(f"ERROR: {prog_name} requires Python version >= 3.8!", file=sys.stderr)
        sys.exit(die_status)
    """

    @patch("sys.exit")
    @patch("sys.version_info", new=(3, 6))
    def test_version_below_3_8(self, mock_exit: MagicMock) -> None:
        with patch("sys.stderr"):
            import importlib
            from bzfs_main import bzfs

            importlib.reload(bzfs)  # Reload module to apply version patch
            mock_exit.assert_called_with(bzfs.die_status)

    @patch("sys.exit")
    @patch("sys.version_info", new=(3, 8))
    def test_version_3_8_or_higher(self, mock_exit: MagicMock) -> None:
        import importlib
        from bzfs_main import bzfs

        importlib.reload(bzfs)  # Reload module to apply version patch
        mock_exit.assert_not_called()


#############################################################################
class TestConnectionPool(unittest.TestCase):
    def setUp(self) -> None:
        args = argparser_parse_args(args=["src", "dst", "-v"])
        p = bzfs.Params(args, log=bzfs.get_logger(bzfs.LogParams(args), args))
        self.src = p.src
        self.dst = p.dst
        self.dst.ssh_user_host = "127.0.0.1"
        self.remote = bzfs.Remote("src", args, p)
        self.src2 = bzfs.Remote("src", args, p)

    def assert_priority_queue(self, cpool: bzfs.ConnectionPool, queuelen: int) -> None:
        self.assertEqual(len(cpool.priority_queue), queuelen)

    def assert_equal_connections(self, conn: bzfs.Connection, donn: bzfs.Connection) -> None:
        self.assertTupleEqual((conn.cid, conn.ssh_cmd), (donn.cid, donn.ssh_cmd))
        self.assertEqual(conn.free, donn.free)
        self.assertEqual(conn.last_modified, donn.last_modified)

    def get_connection(
        self, cpool: bzfs.ConnectionPool, dpool: "SlowButCorrectConnectionPool"
    ) -> Tuple[bzfs.Connection, bzfs.Connection]:
        conn = cpool.get_connection()
        donn = dpool.get_connection()
        self.assert_equal_connections(conn, donn)
        return conn, donn

    def return_connection(
        self,
        cpool: bzfs.ConnectionPool,
        conn: bzfs.Connection,
        dpool: "SlowButCorrectConnectionPool",
        donn: bzfs.Connection,
    ) -> None:
        self.assertTupleEqual((conn.cid, conn.ssh_cmd), (donn.cid, donn.ssh_cmd))
        cpool.return_connection(conn)
        dpool.return_connection(donn)

    def test_basic(self) -> None:
        counter1a = itertools.count()
        counter2a = itertools.count()
        counter1b = itertools.count()
        self.src.local_ssh_command = lambda counter=counter1a: [str(next(counter))]  # type: ignore
        self.src2.local_ssh_command = lambda counter=counter1b: [str(next(counter))]  # type: ignore

        with self.assertRaises(AssertionError):
            bzfs.ConnectionPool(self.src, 0)

        capacity = 2
        cpool = bzfs.ConnectionPool(self.src, capacity)
        dpool = SlowButCorrectConnectionPool(self.src2, capacity)
        self.assert_priority_queue(cpool, 0)
        self.assertIsNotNone(repr(cpool))
        self.assertIsNotNone(str(cpool))
        cpool.shutdown("foo")

        conn1, donn1 = self.get_connection(cpool, dpool)
        self.assert_priority_queue(cpool, 1)
        self.assertEqual(conn1.free, (capacity - 1) * 1)
        i = [str(next(counter2a))]
        self.assertEqual(conn1.ssh_cmd, i)
        self.assertEqual(conn1.ssh_cmd_quoted, i)
        self.assertIsNotNone(repr(conn1))
        self.assertIsNotNone(str(conn1))

        self.return_connection(cpool, conn1, dpool, donn1)
        self.assert_priority_queue(cpool, 1)

        conn2, donn2 = self.get_connection(cpool, dpool)
        self.assert_priority_queue(cpool, 1)
        self.assertIs(conn1, conn2)
        self.assertEqual(conn2.free, (capacity - 1) * 1)

        conn3, donn3 = self.get_connection(cpool, dpool)
        self.assert_priority_queue(cpool, 1)
        self.assertIs(conn2, conn3)

        self.return_connection(cpool, conn2, dpool, donn2)
        self.assert_priority_queue(cpool, 1)

        self.return_connection(cpool, conn3, dpool, donn3)
        self.assert_priority_queue(cpool, 1)

        with self.assertRaises(AssertionError):
            cpool.return_connection(cast(Any, None))
        with self.assertRaises(AssertionError):
            cpool.return_connection(conn3)
        cpool.shutdown("bar")
        cpool.return_connection(conn3)

    def test_multiple_TCP_connections(self) -> None:
        capacity = 2
        cpool = bzfs.ConnectionPool(self.remote, capacity)

        conn1 = cpool.get_connection()
        self.assertEqual(conn1.free, (capacity - 1) * 1)
        conn2 = cpool.get_connection()
        self.assertIs(conn1, conn2)
        self.assertEqual(conn2.free, (capacity - 2) * 1)
        self.assert_priority_queue(cpool, 1)

        conn3 = cpool.get_connection()
        self.assertIsNot(conn2, conn3)
        self.assertEqual(conn3.free, (capacity - 1) * 1)
        self.assertEqual(conn2.free, (capacity - 2) * 1)
        self.assert_priority_queue(cpool, 2)
        conn4 = cpool.get_connection()
        self.assertIs(conn3, conn4)
        self.assertEqual(conn4.free, (capacity - 2) * 1)
        self.assert_priority_queue(cpool, 2)

        conn5 = cpool.get_connection()
        self.assertIsNot(conn4, conn5)
        self.assertEqual(conn5.free, (capacity - 1) * 1)
        self.assertEqual(conn4.free, (capacity - 2) * 1)
        self.assert_priority_queue(cpool, 3)

        cpool.return_connection(conn3)
        self.assert_priority_queue(cpool, 3)
        cpool.return_connection(conn4)
        t4 = cpool.last_modified
        self.assert_priority_queue(cpool, 3)

        cpool.return_connection(conn2)
        self.assert_priority_queue(cpool, 3)
        cpool.return_connection(conn1)
        t2 = cpool.last_modified
        self.assert_priority_queue(cpool, 3)

        cpool.return_connection(conn5)
        t5 = cpool.last_modified
        self.assert_priority_queue(cpool, 3)

        # assert sort order evens out the number of concurrent sessions among the TCP connections
        conn5a = cpool.get_connection()
        self.assertEqual(conn5a.free, (capacity - 1) * 1)

        conn2a = cpool.get_connection()
        self.assertEqual(conn2a.free, (capacity - 1) * 1)

        conn4a = cpool.get_connection()
        self.assertEqual(conn4a.free, (capacity - 1) * 1)

        # assert tie-breaker in favor of most recently returned TCP connection
        conn6 = cpool.get_connection()
        self.assertEqual(conn6.free, (capacity - 2) * 1)
        self.assertEqual(abs(conn6.last_modified), t5)

        conn1a = cpool.get_connection()
        self.assertEqual(conn1a.free, (capacity - 2) * 1)
        self.assertEqual(abs(conn1a.last_modified), t2)

        conn4a = cpool.get_connection()
        self.assertEqual(conn4a.free, (capacity - 2) * 1)
        self.assertEqual(abs(conn4a.last_modified), t4)

        cpool.shutdown("foo")

    def test_pools(self) -> None:
        with self.assertRaises(AssertionError):
            bzfs.ConnectionPools(self.dst, {"shared": 0})

        bzfs.ConnectionPools(self.dst, {}).shutdown("foo")

        pools = bzfs.ConnectionPools(self.dst, {"shared": 8, "dedicated": 1})
        self.assertIsNotNone(pools.pool("shared"))
        self.assertIsNotNone(repr(pools))
        self.assertIsNotNone(str(pools))
        pools.shutdown("foo")
        pools.pool("shared").get_connection()
        pools.shutdown("foo")

    def test_return_sequence(self) -> None:
        maxsessions = 10
        items = 10
        for j in range(0, 3):
            cpool = bzfs.ConnectionPool(self.src, maxsessions)
            dpool = SlowButCorrectConnectionPool(self.src2, maxsessions)
            rng = random.Random(12345)
            conns = [self.get_connection(cpool, dpool) for _ in range(0, items)]
            while conns:
                i = rng.randint(0, len(conns) - 1) if j == 0 else 0 if j == 1 else len(conns) - 1
                conn, donn = conns.pop(i)
                self.return_connection(cpool, conn, dpool, donn)
            print(f"cpool: {cpool}")

    def test_long_random_walk(self) -> None:
        log = logging.getLogger(bzfs.__name__)
        # loglevel = logging.DEBUG
        loglevel = log_trace
        is_logging = log.isEnabledFor(loglevel)
        num_steps = 75 if is_unit_test or is_smoke_test or is_functional_test or is_adhoc_test else 1000
        log.info(f"num_random_steps: {num_steps}")
        start_time_nanos = time.time_ns()
        for maxsessions in range(1, 10 + 1):
            for items in range(0, 64 + 1):
                counter1a = itertools.count()
                counter1b = itertools.count()
                self.src.local_ssh_command = lambda counter=counter1a: [str(next(counter))]  # type: ignore
                self.src2.local_ssh_command = lambda counter=counter1b: [str(next(counter))]  # type: ignore
                cpool = bzfs.ConnectionPool(self.src, maxsessions)
                dpool = SlowButCorrectConnectionPool(self.src2, maxsessions)
                # dpool = bzfs.ConnectionPool(self.src2, maxsessions)
                rng = random.Random(12345)
                conns = []
                try:
                    for _ in range(0, items):
                        conns.append(self.get_connection(cpool, dpool))
                    item = -1
                    for step in range(0, num_steps):
                        if is_logging:
                            log.log(loglevel, f"itr maxsessions: {maxsessions}, items: {items}, step: {step}")
                            log.log(loglevel, f"clen: {len(cpool.priority_queue)}, cpool: {cpool.priority_queue}")
                            log.log(loglevel, f"dlen: {len(dpool.priority_queue)}, dpool: {dpool.priority_queue}")
                        if not conns or rng.randint(0, 1):
                            log.log(loglevel, "get")
                            conns.append(self.get_connection(cpool, dpool))
                        else:
                            # k = rng.randint(0, 2)
                            k = 0
                            if k == 0:
                                i = rng.randint(0, len(conns) - 1)
                            elif k == 1:
                                i = 0
                            else:
                                i = len(conns) - 1
                            conn, donn = conns.pop(i)
                            if is_logging:
                                log.log(loglevel, f"return {i}/{len(conns)+1}: conn: {conn}, donn: {donn} ")
                            self.return_connection(cpool, conn, dpool, donn)
                except Exception:
                    print("Ooops!")
                    print(f"maxsessions: {maxsessions}, items: {items}, step: {step}, item: {item}")
                    print(f"clen: {len(cpool.priority_queue)}, cpool: {cpool.priority_queue}")
                    print(f"dlen: {len(dpool.priority_queue)}, dpool: {dpool.priority_queue}")
                    raise
                log.log(loglevel, "cpool: %s", cpool)
                # log.log(bzfs.log_debug, "cpool: %s", cpool)
        elapsed_secs = (time.time_ns() - start_time_nanos) / 1000_000_000
        log.info("random_walk took %s secs", elapsed_secs)


#############################################################################
class SlowButCorrectConnectionPool(bzfs.ConnectionPool):  # validate a better implementation against this baseline

    def __init__(self, remote: Remote, max_concurrent_ssh_sessions_per_tcp_connection: int) -> None:
        super().__init__(remote, max_concurrent_ssh_sessions_per_tcp_connection)
        self.priority_queue: List[bzfs.Connection] = []  # type: ignore

    def get_connection(self) -> bzfs.Connection:
        with self._lock:
            self.priority_queue.sort()
            conn = self.priority_queue[-1] if self.priority_queue else None
            if conn is None or conn.is_full():
                conn = bzfs.Connection(self.remote, self.capacity, self.cid)
                self.last_modified += 1
                conn.update_last_modified(self.last_modified)  # LIFO tiebreaker favors latest conn as that's most alive
                self.cid += 1
                self.priority_queue.append(conn)
            conn.increment_free(-1)
            return conn

    def return_connection(self, old_conn: bzfs.Connection) -> None:
        assert old_conn is not None
        with self._lock:
            assert any(old_conn is c for c in self.priority_queue)
            old_conn.increment_free(1)
            self.last_modified += 1
            old_conn.update_last_modified(self.last_modified)

    def __repr__(self) -> str:
        with self._lock:
            return str({"capacity": self.capacity, "queue_len": len(self.priority_queue), "queue": self.priority_queue})


#############################################################################
class TestIncrementalSendSteps(unittest.TestCase):

    def test_basic1(self) -> None:
        input_snapshots = ["d1", "h1", "d2", "d3", "d4"]
        expected_results = ["d1", "d2", "d3", "d4"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic2(self) -> None:
        input_snapshots = ["d1", "d2", "h1", "d3", "d4"]
        expected_results = ["d1", "d2", "d3", "d4"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic3(self) -> None:
        input_snapshots: List[str] = ["h0", "h1", "d1", "d2", "h2", "d3", "d4"]
        expected_results: List[str] = ["d1", "d2", "d3", "d4"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic4(self) -> None:
        input_snapshots: List[str] = ["d1"]
        expected_results: List[str] = ["d1"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic5(self) -> None:
        input_snapshots: List[str] = []
        expected_results: List[str] = []
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_validate_snapshot_series_excluding_hourlies_with_permutations(self) -> None:
        for i, testcase in enumerate(self.permute_snapshot_series()):
            with stop_on_failure_subtest(i=i):
                self.validate_incremental_send_steps(testcase[None], testcase["d"])

    def test_send_step_to_str(self) -> None:
        bzfs.Job().send_step_to_str(("-I", "d@s1", "d@s3"))

    def permute_snapshot_series(self, max_length: int = 9) -> List[DefaultDict[Optional[str], List[str]]]:
        """
        Simulates a series of hourly and daily snapshots. At the end, makes a backup while excluding hourly
        snapshots from replication. The expectation is that after replication dst contains all daily snapshots
        and no hourly snapshots.
        Example snapshot series: d1, h1, d2, d3, d4 --> expected dst output: d1, d2, d3, d4
        where
        d1 = first daily snapshot,  dN = n-th daily snapshot
        h1 = first hourly snapshot, hN = n-th hourly snapshot

        We test all possible permutations of series of length L=[0..max_length] snapshots
        """
        assert max_length >= 0
        testcases = []
        for L in range(0, max_length + 1):
            for N in range(0, L + 1):
                steps = "d" * N + "h" * (L - N)
                # compute a permutation of several 'd' and 'h' chars that represents the snapshot series
                for permutation in sorted(set(itertools.permutations(steps, len(steps)))):
                    snaps: DefaultDict[Optional[str], List[str]] = defaultdict(list)
                    count: DefaultDict[str, int] = defaultdict(int)
                    for char in permutation:
                        count[char] += 1  # tag snapshots with a monotonically increasing number within each category
                        char_count = f"{count[char]:01}" if max_length < 10 else f"{count[char]:02}"  # zero pad number
                        snapshot = f"{char}{char_count}"
                        snaps[None].append(snapshot)
                        snaps[char].append(snapshot)  # represents expected results for test verification
                    testcases.append(snaps)
        return testcases

    def validate_incremental_send_steps(self, input_snapshots: List[str], expected_results: List[str]) -> None:
        """Computes steps to incrementally replicate the daily snapshots of the given daily and/or hourly input
        snapshots. Applies the steps and compares the resulting destination snapshots with the expected results."""
        for is_resume in [False, True]:  # via --no-resume-recv
            for src_dataset in ["", "s@"]:
                for force_convert_I_to_i in [False, True]:
                    steps = self.incremental_send_steps1(
                        input_snapshots,
                        src_dataset=src_dataset,
                        is_resume=is_resume,
                        force_convert_I_to_i=force_convert_I_to_i,
                    )
                    # print(f"input_snapshots:" + ",".join(input_snapshots))
                    # print("steps: " + ",".join([self.send_step_to_str(step) for step in steps]))
                    output_snapshots = [] if len(expected_results) == 0 else [expected_results[0]]
                    output_snapshots += self.apply_incremental_send_steps(steps, input_snapshots)
                    # print(f"output_snapshots:" + ','.join(output_snapshots))
                    self.assertListEqual(expected_results, output_snapshots)
                    all_to_snapshots = []
                    for incr_flag, start_snapshot, end_snapshot, to_snapshots in steps:  # noqa: B007
                        self.assertIn(incr_flag, ["-I", "-i"])
                        self.assertGreaterEqual(len(to_snapshots), 1)
                        all_to_snapshots += [snapshot[snapshot.find("@") + 1 :] for snapshot in to_snapshots]
                    self.assertListEqual(expected_results[1:], all_to_snapshots)

    def send_step_to_str(self, step: Tuple) -> str:
        # return str(step)
        return str(step[1]) + ("-" if step[0] == "-I" else ":") + str(step[2])

    def apply_incremental_send_steps(self, steps: List[Tuple], input_snapshots: List[str]) -> List[str]:
        """Simulates replicating (a subset of) the given input_snapshots to a destination, according to the given steps.
        Returns the subset of snapshots that have actually been replicated to the destination."""
        output_snapshots = []
        for incr_flag, start_snapshot, end_snapshot, to_snapshots in steps:  # noqa: B007
            start_snapshot = start_snapshot[start_snapshot.find("@") + 1 :]
            end_snapshot = end_snapshot[end_snapshot.find("@") + 1 :]
            start = input_snapshots.index(start_snapshot)
            end = input_snapshots.index(end_snapshot)
            if incr_flag == "-I":
                for j in range(start + 1, end + 1):
                    output_snapshots.append(input_snapshots[j])
            else:
                output_snapshots.append(input_snapshots[end])
        return output_snapshots

    def incremental_send_steps1(
        self,
        input_snapshots: List[str],
        src_dataset: str,
        is_resume: bool = False,
        force_convert_I_to_i: bool = False,
    ) -> List[Tuple]:
        origin_src_snapshots_with_guids = []
        guid = 1
        for snapshot in input_snapshots:
            origin_src_snapshots_with_guids.append(f"{guid}\t{src_dataset}{snapshot}")
            guid += 1
        return self.incremental_send_steps2(
            origin_src_snapshots_with_guids, is_resume=is_resume, force_convert_I_to_i=force_convert_I_to_i
        )

    def incremental_send_steps2(
        self,
        origin_src_snapshots_with_guids: List[str],
        is_resume: bool = False,
        force_convert_I_to_i: bool = False,
    ) -> List[Tuple]:
        guids = []
        input_snapshots = []
        included_guids = set()
        for line in origin_src_snapshots_with_guids:
            guid, snapshot = line.split("\t", 1)
            guids.append(guid)
            input_snapshots.append(snapshot)
            i = snapshot.find("@")
            snapshot = snapshot[i + 1 :]
            if snapshot[0:1] == "d":
                included_guids.add(guid)
        return bzfs.Job().incremental_send_steps(
            input_snapshots,
            guids,
            included_guids=included_guids,
            is_resume=is_resume,
            force_convert_I_to_i=force_convert_I_to_i,
        )


#############################################################################
class TestSmallPriorityQueue(unittest.TestCase):
    def setUp(self) -> None:
        self.pq: bzfs.SmallPriorityQueue[int] = bzfs.SmallPriorityQueue()
        self.pq_reverse: bzfs.SmallPriorityQueue[int] = bzfs.SmallPriorityQueue(reverse=True)

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
        self.assertEqual(len(self.pq), 0)

    def test_push_and_pop(self) -> None:
        self.pq.push(3)
        self.pq.push(1)
        self.pq.push(2)
        self.assertEqual(self.pq._lst, [1, 2, 3])
        self.assertEqual(self.pq.pop(), 1)
        self.assertEqual(self.pq._lst, [2, 3])

    def test_pop_empty(self) -> None:
        with self.assertRaises(IndexError):  # Generic IndexError from list.pop()
            self.pq.pop()

    def test_remove(self) -> None:
        self.pq.push(3)
        self.pq.push(1)
        self.pq.push(2)
        self.pq.remove(2)
        self.assertEqual(self.pq._lst, [1, 3])
        self.assertFalse(self.pq.remove(0))
        self.assertTrue(self.pq.remove(1))
        self.assertEqual(self.pq._lst, [3])

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
        self.assertEqual(self.pq.peek(), 1)
        self.assertEqual(self.pq._lst, [1, 2, 3])
        self.pq_reverse.push(3)
        self.pq_reverse.push(1)
        self.pq_reverse.push(2)
        self.assertEqual(self.pq_reverse.peek(), 3)
        self.assertEqual(self.pq_reverse._lst, [1, 2, 3])

    def test_peek_empty(self) -> None:
        with self.assertRaises(IndexError):  # Generic IndexError from list indexing
            self.pq.peek()

        with self.assertRaises(IndexError):  # Generic IndexError from list indexing
            self.pq_reverse.peek()

    def test_reverse_order(self) -> None:
        self.pq_reverse.push(1)
        self.pq_reverse.push(3)
        self.pq_reverse.push(2)
        self.assertEqual(self.pq_reverse.pop(), 3)
        self.assertEqual(self.pq_reverse._lst, [1, 2])

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
        self.assertEqual(self.pq._lst, [1, 2, 2])

        # Pop should remove the smallest duplicate first
        self.assertEqual(self.pq.pop(), 1)
        self.assertEqual(self.pq._lst, [2, 2])

        # Remove one duplicate, leaving another
        self.pq.remove(2)
        self.assertEqual(self.pq._lst, [2])

        # Peek and pop should now work on the remaining duplicate
        self.assertEqual(self.pq.peek(), 2)
        self.assertEqual(self.pq.pop(), 2)
        self.assertEqual(len(self.pq), 0)

    def test_reverse_with_duplicates(self) -> None:
        self.pq_reverse.push(2)
        self.pq_reverse.push(2)
        self.pq_reverse.push(3)
        self.pq_reverse.push(1)
        self.assertEqual(self.pq_reverse._lst, [1, 2, 2, 3])

        # Pop the largest first in reverse order
        self.assertEqual(self.pq_reverse.pop(), 3)
        self.assertEqual(self.pq_reverse._lst, [1, 2, 2])

        # Remove a duplicate
        self.pq_reverse.remove(2)
        self.assertEqual(self.pq_reverse._lst, [1, 2])

        # Peek and pop the remaining elements
        self.assertEqual(self.pq_reverse.peek(), 2)
        self.assertEqual(self.pq_reverse.pop(), 2)
        self.assertEqual(self.pq_reverse.pop(), 1)
        self.assertEqual(len(self.pq_reverse), 0)


#############################################################################
class TestSynchronizedBool(unittest.TestCase):
    def test_initialization(self) -> None:
        b = bzfs.SynchronizedBool(True)
        self.assertTrue(b.value)

        b = bzfs.SynchronizedBool(False)
        self.assertFalse(b.value)

        with self.assertRaises(AssertionError):
            bzfs.SynchronizedBool(cast(Any, "not_a_bool"))

    def test_value_property(self) -> None:
        b = bzfs.SynchronizedBool(True)
        self.assertTrue(b.value)

        b.value = False
        self.assertFalse(b.value)

    def test_loop(self) -> None:
        b = bzfs.SynchronizedBool(False)
        for _ in range(3):
            b.value = not b.value
        self.assertIsInstance(b.value, bool)

    def test_bool_conversion(self) -> None:
        b = bzfs.SynchronizedBool(True)
        self.assertTrue(bool(b))

        b.value = False
        self.assertFalse(bool(b))

    def test_get_and_set(self) -> None:
        b = bzfs.SynchronizedBool(True)
        self.assertTrue(b.get_and_set(False))
        self.assertFalse(b.value)

    def test_compare_and_set(self) -> None:
        b = bzfs.SynchronizedBool(True)
        self.assertTrue(b.compare_and_set(True, False))
        self.assertFalse(b.value)

        b = bzfs.SynchronizedBool(True)
        self.assertFalse(b.compare_and_set(False, False))
        self.assertTrue(b.value)

    def test_str_and_repr(self) -> None:
        b = bzfs.SynchronizedBool(True)
        self.assertEqual(str(b), "True")
        self.assertEqual(repr(b), "True")

        b.value = False
        self.assertEqual(str(b), "False")
        self.assertEqual(repr(b), "False")


#############################################################################
class TestSynchronizedDict(unittest.TestCase):
    def setUp(self) -> None:
        self.sync_dict: bzfs.SynchronizedDict = bzfs.SynchronizedDict({"a": 1, "b": 2, "c": 3})

    def test_getitem(self) -> None:
        self.assertEqual(self.sync_dict["a"], 1)
        self.assertEqual(self.sync_dict["b"], 2)

    def test_setitem(self) -> None:
        self.sync_dict["d"] = 4
        self.assertEqual(self.sync_dict["d"], 4)

    def test_delitem(self) -> None:
        del self.sync_dict["a"]
        self.assertNotIn("a", self.sync_dict)

    def test_contains(self) -> None:
        self.assertTrue("a" in self.sync_dict)
        self.assertFalse("z" in self.sync_dict)

    def test_len(self) -> None:
        self.assertEqual(len(self.sync_dict), 3)
        del self.sync_dict["a"]
        self.assertEqual(len(self.sync_dict), 2)

    def test_repr(self) -> None:
        self.assertEqual(repr(self.sync_dict), repr({"a": 1, "b": 2, "c": 3}))

    def test_str(self) -> None:
        self.assertEqual(str(self.sync_dict), str({"a": 1, "b": 2, "c": 3}))

    def test_get(self) -> None:
        self.assertEqual(self.sync_dict.get("a"), 1)
        self.assertEqual(self.sync_dict.get("z", 42), 42)

    def test_pop(self) -> None:
        value = self.sync_dict.pop("b")
        self.assertEqual(value, 2)
        self.assertNotIn("b", self.sync_dict)

    def test_clear(self) -> None:
        self.sync_dict.clear()
        self.assertEqual(len(self.sync_dict), 0)

    def test_items(self) -> None:
        items = self.sync_dict.items()
        self.assertEqual(set(items), {("a", 1), ("b", 2), ("c", 3)})

    def test_loop(self) -> None:
        self.sync_dict["key"] = 1
        self.assertIn("key", self.sync_dict)


#############################################################################
# class TestItrSSHCmdParallel(unittest.TestCase)
def dummy_fn_ordered(cmd: List[str], batch: List[str]) -> Tuple[List[str], List[str]]:
    return cmd, batch


def dummy_fn_unordered(cmd: List[str], batch: List[str]) -> Tuple[List[str], List[str]]:
    if cmd[0] == "zfslist1":
        time.sleep(0.2)
    elif cmd[0] == "zfslist2":
        time.sleep(0.1)
    return cmd, batch


def dummy_fn_raise(cmd: List[str], batch: List[str]) -> Tuple[List[str], List[str]]:
    if cmd[0] == "fail":
        raise ValueError("Intentional failure")
    return cmd, batch


def dummy_fn_race(cmd: List[str], batch: List[str]) -> Tuple[List[str], List[str]]:
    if cmd[0] == "zfslist1":
        time.sleep(0.3)
    elif cmd[0] == "zfslist2":
        time.sleep(0.2)
    elif cmd[0] == "zfslist3":
        time.sleep(0.1)
    return cmd, batch


class TestItrSSHCmdParallel(unittest.TestCase):
    def setUp(self) -> None:
        args = argparser_parse_args(args=["src", "dst"])
        p = bzfs.Params(args)
        job = bzfs.Job()
        job.params = p
        p.src = bzfs.Remote("src", args, p)
        job.params.connection_pools["src"] = bzfs.ConnectionPools(
            p.src, {bzfs.SHARED: p.src.max_concurrent_ssh_sessions_per_tcp_connection, bzfs.DEDICATED: 1}
        )
        job.max_workers = {"src": 2}
        job.params.available_programs = {"src": {"os": "Linux"}, "local": {"os": "Linux"}}
        self.job = job
        self.r = p.src

        # Test data with max_batch_items=2
        self.cmd_args_list_2 = [(["zfslist1"], ["d1", "d2", "d3", "d4"]), (["zfslist2"], ["d5", "d6", "d7", "d8"])]
        self.expected_ordered_2 = [
            (["zfslist1"], ["d1", "d2"]),
            (["zfslist1"], ["d3", "d4"]),
            (["zfslist2"], ["d5", "d6"]),
            (["zfslist2"], ["d7", "d8"]),
        ]

        # Test data with max_batch_items=3
        self.cmd_args_list_3 = [(["zfslist1"], ["a1", "a2", "a3", "a4"]), (["zfslist2"], ["b1", "b2", "b3", "b4", "b5"])]
        self.expected_ordered_3 = [
            (["zfslist1"], ["a1", "a2", "a3"]),
            (["zfslist1"], ["a4"]),
            (["zfslist2"], ["b1", "b2", "b3"]),
            (["zfslist2"], ["b4", "b5"]),
        ]

    def tearDown(self) -> None:
        bzfs.reset_logger()

    def test_ordered_with_max_batch_items_2(self) -> None:
        results = list(
            self.job.itr_ssh_cmd_parallel(self.r, self.cmd_args_list_2, dummy_fn_ordered, max_batch_items=2, ordered=True)
        )
        self.assertEqual(results, self.expected_ordered_2)

    def test_unordered_with_max_batch_items_2(self) -> None:
        results = list(
            self.job.itr_ssh_cmd_parallel(self.r, self.cmd_args_list_2, dummy_fn_unordered, max_batch_items=2, ordered=False)
        )
        self.assertEqual(sorted(results), sorted(self.expected_ordered_2))

    def test_ordered_with_max_batch_items_3(self) -> None:
        results = list(
            self.job.itr_ssh_cmd_parallel(self.r, self.cmd_args_list_3, dummy_fn_ordered, max_batch_items=3, ordered=True)
        )
        self.assertEqual(results, self.expected_ordered_3)

    def test_unordered_with_max_batch_items_3(self) -> None:
        results = list(
            self.job.itr_ssh_cmd_parallel(self.r, self.cmd_args_list_3, dummy_fn_unordered, max_batch_items=3, ordered=False)
        )
        self.assertEqual(sorted(results), sorted(self.expected_ordered_3))

    def test_exception_propagation_ordered(self) -> None:
        cmd_args_list = [(["ok"], ["a1", "a2"]), (["fail"], ["b1", "b2"])]
        gen = self.job.itr_ssh_cmd_parallel(self.r, cmd_args_list, dummy_fn_raise, max_batch_items=2, ordered=True)
        result = next(gen)
        self.assertEqual(result, (["ok"], ["a1", "a2"]))
        with self.assertRaises(ValueError) as context:
            next(gen)
        self.assertEqual(str(context.exception), "Intentional failure")

    def test_exception_propagation_unordered(self) -> None:
        cmd_args_list = [(["ok"], ["a1", "a2"]), (["fail"], ["b1", "b2"])]
        gen = self.job.itr_ssh_cmd_parallel(self.r, cmd_args_list, dummy_fn_raise, max_batch_items=2, ordered=False)
        caught_exception = False
        results = []
        try:
            for r in gen:
                results.append(r)
        except ValueError as e:
            caught_exception = True
            self.assertEqual(str(e), "Intentional failure")
        self.assertTrue(caught_exception, "Expected exception was not raised in unordered mode..")

    def test_unordered_thread_scheduling(self) -> None:
        cmd_args_list = [
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
            self.job.itr_ssh_cmd_parallel(self.r, cmd_args_list, dummy_fn_race, max_batch_items=1, ordered=False)
        )
        self.assertEqual(sorted(unordered_results), sorted(expected_ordered))

    def test_empty_cmd_args_list_ordered(self) -> None:
        results = list(self.job.itr_ssh_cmd_parallel(self.r, [], dummy_fn_ordered, max_batch_items=2, ordered=True))
        self.assertEqual(results, [])

    def test_empty_cmd_args_list_unordered(self) -> None:
        results = list(self.job.itr_ssh_cmd_parallel(self.r, [], dummy_fn_ordered, max_batch_items=2, ordered=False))
        self.assertEqual(results, [])

    def test_cmd_with_empty_arguments_ordered(self) -> None:
        cmd_args_list = [(["zfslist1"], []), (["zfslist2"], ["d1", "d2"])]
        expected_ordered = [(["zfslist2"], ["d1", "d2"])]
        results = list(
            self.job.itr_ssh_cmd_parallel(self.r, cmd_args_list, dummy_fn_ordered, max_batch_items=2, ordered=True)
        )
        self.assertEqual(results, expected_ordered)

    def test_cmd_with_empty_arguments_unordered(self) -> None:
        cmd_args_list = [(["zfslist1"], []), (["zfslist2"], ["d1", "d2"])]
        expected_ordered = [(["zfslist2"], ["d1", "d2"])]
        results = list(
            self.job.itr_ssh_cmd_parallel(self.r, cmd_args_list, dummy_fn_ordered, max_batch_items=2, ordered=False)
        )
        self.assertEqual(results, expected_ordered)


#############################################################################
class TestProcessDatasetsInParallel(unittest.TestCase):
    def setUp(self) -> None:
        args = argparser_parse_args(args=["src", "dst"])
        p = bzfs.Params(args, log=bzfs.get_simple_logger("myprogram"))
        self.job = bzfs.Job()
        self.job.params = p
        self.lock = threading.Lock()
        self.submitted: List[str] = []

    def append_submission(self, dataset: str) -> None:
        with self.lock:
            self.submitted.append(dataset)

    def test_submit_no_skiptree(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: bzfs.Retry) -> bool:
            self.append_submission(dataset)
            return True

        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.setUp()
                src_datasets = ["a1", "a1/b1", "a2"]
                failed = self.job.process_datasets_in_parallel_and_fault_tolerant(
                    src_datasets,
                    process_dataset=submit_no_skiptree,  # lambda
                    skip_tree_on_error=lambda dataset: False,
                    max_workers=8,
                    interval_nanos=lambda dataset: 10_000_000,
                    task_name="mytask",
                    enable_barriers=i > 0,
                )
                self.assertFalse(failed)
                self.assertListEqual(["a1", "a1/b1", "a2"], sorted(self.submitted))

    def test_submit_skiptree(self) -> None:
        def submit_skiptree(dataset: str, tid: str, retry: bzfs.Retry) -> bool:
            self.append_submission(dataset)
            return False

        for i in range(0, 2):
            with stop_on_failure_subtest(i=i):
                self.setUp()
                src_datasets = ["a1", "a1/b1", "a2"]
                failed = self.job.process_datasets_in_parallel_and_fault_tolerant(
                    src_datasets,
                    process_dataset=submit_skiptree,  # lambda
                    skip_tree_on_error=lambda dataset: False,
                    max_workers=8,
                    enable_barriers=i > 0,
                )
                self.assertFalse(failed)
                self.assertListEqual(["a1", "a2"], sorted(self.submitted))

    def test_submit_zero_datasets(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: bzfs.Retry) -> bool:
            self.append_submission(dataset)
            return True

        src_datasets: List[str] = []
        failed = self.job.process_datasets_in_parallel_and_fault_tolerant(
            src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: False,
            max_workers=8,
        )
        self.assertFalse(failed)
        self.assertListEqual([], sorted(self.submitted))

    def test_submit_timeout_with_skip_on_error_is_fail(self) -> None:
        def submit_raise_timeout(dataset: str, tid: str, retry: bzfs.Retry) -> bool:
            self.append_submission(dataset)
            raise subprocess.TimeoutExpired("submit_raise_timeout", 10)

        src_datasets = ["a1", "a1/b1", "a2"]
        self.job.params.skip_on_error = "fail"
        with self.assertRaises(subprocess.TimeoutExpired):
            self.job.process_datasets_in_parallel_and_fault_tolerant(
                src_datasets,
                process_dataset=submit_raise_timeout,  # lambda
                skip_tree_on_error=lambda dataset: True,
                max_workers=1,
            )
        self.assertListEqual(["a1"], sorted(self.submitted))

    def test_submit_timeout_with_skip_on_error_is_not_fail(self) -> None:
        def submit_raise_timeout(dataset: str, tid: str, retry: bzfs.Retry) -> bool:
            self.append_submission(dataset)
            raise subprocess.TimeoutExpired("submit_raise_timeout", 10)

        src_datasets = ["a1", "a1/b1", "a2"]
        failed = self.job.process_datasets_in_parallel_and_fault_tolerant(
            src_datasets,
            process_dataset=submit_raise_timeout,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
        )
        self.assertTrue(failed)
        self.assertListEqual(["a1", "a2"], sorted(self.submitted))

    def submit_raise_error(self, dataset: str, tid: str, retry: bzfs.Retry) -> bool:
        self.append_submission(dataset)
        raise subprocess.CalledProcessError(1, "foo_cmd")

    def test_submit_raise_error_with_skip_tree_on_error_is_false(self) -> None:
        src_datasets = ["a1", "a1/b1", "a2"]
        failed = self.job.process_datasets_in_parallel_and_fault_tolerant(
            src_datasets,
            process_dataset=self.submit_raise_error,  # lambda
            skip_tree_on_error=lambda dataset: False,
            max_workers=8,
        )
        self.assertTrue(failed)
        self.assertListEqual(["a1", "a1/b1", "a2"], sorted(self.submitted))

    def test_submit_raise_error_with_skip_tree_on_error_is_true(self) -> None:
        src_datasets = ["a1", "a1/b1", "a2"]
        failed = self.job.process_datasets_in_parallel_and_fault_tolerant(
            src_datasets,
            process_dataset=self.submit_raise_error,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
        )
        self.assertTrue(failed)
        self.assertListEqual(["a1", "a2"], sorted(self.submitted))

    def test_submit_barriers0a_no_skiptree(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: bzfs.Retry) -> bool:
            self.append_submission(dataset)
            return True

        BR = bzfs.BARRIER_CHAR
        src_datasets = ["a/b/c", "a/b/c/0d", "a/b/c/1d", f"a/b/c/{BR}/prune", f"a/b/c/{BR}/prune/monitor"]
        failed = self.job.process_datasets_in_parallel_and_fault_tolerant(
            src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=True,
        )
        self.assertFalse(failed)
        self.assertListEqual(src_datasets, sorted(self.submitted))

    def test_submit_barriers0a_with_skiptree(self) -> None:
        def submit_with_skiptree(dataset: str, tid: str, retry: bzfs.Retry) -> bool:
            self.append_submission(dataset)
            return dataset != "a/b/c/0d"

        BR = bzfs.BARRIER_CHAR
        src_datasets = ["a/b/c", "a/b/c/0d", "a/b/c/1d", f"a/b/c/{BR}/prune", f"a/b/c/{BR}/prune/monitor"]
        failed = self.job.process_datasets_in_parallel_and_fault_tolerant(
            src_datasets,
            process_dataset=submit_with_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=True,
        )
        self.assertFalse(failed)
        self.assertListEqual(["a/b/c", "a/b/c/0d", "a/b/c/1d"], sorted(self.submitted))

    def test_submit_barriers0b(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: bzfs.Retry) -> bool:
            self.append_submission(dataset)
            return True

        src_datasets = ["a/b/c", "a/b/c/d/e/f", "u/v/w"]
        failed = self.job.process_datasets_in_parallel_and_fault_tolerant(
            src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=True,
        )
        self.assertFalse(failed)
        self.assertListEqual(src_datasets, sorted(self.submitted))

    def test_submit_barriers0c(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: bzfs.Retry) -> bool:
            self.append_submission(dataset)
            return True

        src_datasets = ["a/b/c", "a/b/c/d/e/f", "u/v/w"]
        failed = self.job.process_datasets_in_parallel_and_fault_tolerant(
            src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=False,
        )
        self.assertFalse(failed)
        self.assertListEqual(src_datasets, sorted(self.submitted))

    def test_submit_barriers1(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: bzfs.Retry) -> bool:
            self.append_submission(dataset)
            return True

        BR = bzfs.BARRIER_CHAR
        src_datasets = [
            "a/b/c",
            "a/b/c/0d",
            "a/b/c/1d",
            f"a/b/c/{BR}/prune",
            f"a/b/c/{BR}/prune/monitor",
            f"a/b/c/{BR}/{BR}/done",
        ]
        failed = self.job.process_datasets_in_parallel_and_fault_tolerant(
            src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=True,
        )
        self.assertFalse(failed)
        self.assertListEqual(src_datasets, sorted(self.submitted))

    def test_submit_barriers2(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: bzfs.Retry) -> bool:
            self.append_submission(dataset)
            return True

        BR = bzfs.BARRIER_CHAR
        src_datasets = [
            "a/b/c",
            "a/b/c/0d",
            "a/b/c/1d",
            f"a/b/c/{BR}/prune",
            f"a/b/c/{BR}/prune/monitor",
            f"a/b/c/{BR}/{BR}/{BR}/{BR}/done",
        ]
        failed = self.job.process_datasets_in_parallel_and_fault_tolerant(
            src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=True,
        )
        self.assertFalse(failed)
        self.assertListEqual(src_datasets, sorted(self.submitted))

    def test_submit_barriers3(self) -> None:
        def submit_no_skiptree(dataset: str, tid: str, retry: bzfs.Retry) -> bool:
            self.append_submission(dataset)
            return True

        BR = bzfs.BARRIER_CHAR
        src_datasets = [
            "a/b/c",
            "a/b/c/0d",
            "a/b/c/1d",
            f"a/b/c/{BR}/prune",
            f"a/b/c/{BR}/prune/monitor",
            f"a/b/c/{BR}/{BR}/{BR}/{BR}",
        ]
        failed = self.job.process_datasets_in_parallel_and_fault_tolerant(
            src_datasets,
            process_dataset=submit_no_skiptree,  # lambda
            skip_tree_on_error=lambda dataset: True,
            max_workers=8,
            enable_barriers=True,
        )
        self.assertFalse(failed)
        self.assertListEqual(src_datasets[0:-1], sorted(self.submitted))


#############################################################################
class TestPerformance(unittest.TestCase):

    def test_close_fds(self) -> None:
        """see https://bugs.python.org/issue42738
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
                print(f"close_fds={close_fds}: Took {secs:.1f} seconds, iters/sec: {iters/secs:.1f}")


@contextmanager
def stop_on_failure_subtest(**params: Any) -> Iterator[None]:
    """Context manager to mimic UnitTest.subTest() but stop on first failure"""
    try:
        yield
    except AssertionError as e:
        raise AssertionError(f"SubTest failed with parameters: {params}") from e
