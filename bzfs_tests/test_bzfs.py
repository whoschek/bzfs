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
import contextlib
import io
import itertools
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from collections import defaultdict
from datetime import datetime, timedelta, timezone, tzinfo
from logging import Logger
from pathlib import Path
from typing import (
    Any,
    List,
    cast,
)
from unittest.mock import (
    MagicMock,
    mock_open,
    patch,
)

from bzfs_main import bzfs
from bzfs_main.bzfs import (
    log_trace,
)
from bzfs_main.check_range import (
    CheckRange,
)
from bzfs_main.utils import (
    getenv_any,
)
from bzfs_tests.test_utils import (
    stop_on_failure_subtest,
)
from bzfs_tests.zfs_util import (
    is_solaris_zfs,
)

# constants:
test_mode = getenv_any("test_mode", "")  # Consider toggling this when testing
is_unit_test = test_mode == "unit"  # run only unit tests aka skip integration tests
is_smoke_test = test_mode == "smoke"  # run only a small subset of tests
is_functional_test = test_mode == "functional"  # run most tests but only in a single local config combination
is_adhoc_test = test_mode == "adhoc"  # run only a few isolated changes


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestHelperFunctions,
        TestAdditionalHelpers,
        TestParseDatasetLocator,
        TestCurrentDateTime,
        TestArgumentParser,
        TestAddRecvPropertyOptions,
        TestPreservePropertiesValidation,
        TestDatasetPairsAction,
        TestFileOrLiteralAction,
        TestNewSnapshotFilterGroupAction,
        TestNonEmptyStringAction,
        TestLogConfigVariablesAction,
        SSHConfigFileNameAction,
        TestSafeFileNameAction,
        TestSafeDirectoryNameAction,
        TestCheckRange,
        TestCheckPercentRange,
        TestPythonVersionCheck,
        TestItrSSHCmdParallel,
        TestRemoteConfCache,
        TestIncrementalSendSteps,
        TestLogging,
        # TestPerformance,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


def argparser_parse_args(args: list[str]) -> argparse.Namespace:
    return bzfs.argument_parser().parse_args(
        args + ["--log-dir", os.path.join(bzfs.get_home_directory(), bzfs.log_dir_default + "-test")]
    )


#############################################################################
class TestHelperFunctions(unittest.TestCase):
    s = "s"
    d = "d"
    a = "a"

    def merge_sorted_iterators(self, src: list[Any], dst: list[Any], choice: str) -> list[tuple[Any, ...]]:
        s, d, a = self.s, self.d, self.a
        return [item for item in bzfs.Job().merge_sorted_iterators([s, d, a], choice, iter(src), iter(dst))]

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

    def test_append_if_absent(self) -> None:
        self.assertListEqual([], bzfs.append_if_absent([]))
        self.assertListEqual(["a"], bzfs.append_if_absent([], "a"))
        self.assertListEqual(["a"], bzfs.append_if_absent([], "a", "a"))

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

    def test_validate_program_name_must_not_be_empty(self) -> None:
        args = argparser_parse_args(args=["src", "dst"])
        args.zpool_program = ""
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zpool_program = None
        with self.assertRaises(SystemExit):
            bzfs.Params(args)

    def test_validate_program_name_must_not_contain_special_chars(self) -> None:
        args = argparser_parse_args(args=["src", "dst"])
        args.zpool_program = "true;false"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zpool_program = "echo foo|cat"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zpool_program = "foo>bar"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zpool_program = "foo\nbar"
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zpool_program = "foo\\bar"
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

    def test_format_dict(self) -> None:
        self.assertEqual("\"{'a': 1}\"", bzfs.format_dict({"a": 1}))

    def test_pretty_print_formatter(self) -> None:
        args = argparser_parse_args(["src", "dst"])
        params = bzfs.Params(args, log_params=bzfs.LogParams(args), log=logging.getLogger())
        self.assertIsNotNone(str(bzfs.pretty_print_formatter(params)))

    def test_parse_duration_to_milliseconds(self) -> None:
        self.assertEqual(5000, bzfs.parse_duration_to_milliseconds("5 seconds"))
        self.assertEqual(
            300000,
            bzfs.parse_duration_to_milliseconds("5 minutes ago", regex_suffix=r"\s*ago"),
        )
        with self.assertRaises(ValueError):
            bzfs.parse_duration_to_milliseconds("foo")
        with self.assertRaises(SystemExit):
            bzfs.parse_duration_to_milliseconds("foo", context="ctx")

    def test_fix_send_recv_opts(self) -> None:
        params = bzfs.Params(argparser_parse_args(args=["src", "dst"]))
        self.assertEqual([], params.fix_recv_opts(["-n"], frozenset())[0])
        self.assertEqual([], params.fix_recv_opts(["--dryrun", "-n"], frozenset())[0])
        self.assertEqual([""], params.fix_recv_opts([""], frozenset())[0])
        self.assertEqual([], params.fix_recv_opts([], frozenset())[0])
        self.assertEqual(["-"], params.fix_recv_opts(["-"], frozenset())[0])
        self.assertEqual(["-h"], params.fix_recv_opts(["-hn"], frozenset())[0])
        self.assertEqual(["-h"], params.fix_recv_opts(["-nh"], frozenset())[0])
        self.assertEqual(["--Fvhn"], params.fix_recv_opts(["--Fvhn"], frozenset())[0])
        self.assertEqual(["foo"], params.fix_recv_opts(["foo"], frozenset())[0])
        self.assertEqual(["v", "n", "F"], params.fix_recv_opts(["v", "n", "F"], frozenset())[0])
        self.assertEqual(["-o", "-n"], params.fix_recv_opts(["-o", "-n"], frozenset())[0])
        self.assertEqual(["-o", "-n"], params.fix_recv_opts(["-o", "-n", "-n"], frozenset())[0])
        self.assertEqual(["-x", "--dryrun"], params.fix_recv_opts(["-x", "--dryrun"], frozenset())[0])
        self.assertEqual(["-x", "--dryrun"], params.fix_recv_opts(["-x", "--dryrun", "-n"], frozenset())[0])
        self.assertEqual(["-x"], params.fix_recv_opts(["-x"], frozenset())[0])

        self.assertEqual([], params.fix_send_opts(["-n"]))
        self.assertEqual([], params.fix_send_opts(["--dryrun", "-n", "-ed"]))
        self.assertEqual([], params.fix_send_opts(["-I", "s1"]))
        self.assertEqual(["--raw"], params.fix_send_opts(["-i", "s1", "--raw"]))
        self.assertEqual(["-X", "d1,d2"], params.fix_send_opts(["-X", "d1,d2"]))
        self.assertEqual(
            ["--exclude", "d1,d2", "--redact", "b1"], params.fix_send_opts(["--exclude", "d1,d2", "--redact", "b1"])
        )

    def test_fix_recv_opts_with_preserve_properties(self) -> None:
        mp = "mountpoint"
        cr = "createtxg"
        params = bzfs.Params(argparser_parse_args(args=["src", "dst"]))
        with self.assertRaises(SystemExit):
            params.fix_recv_opts(["-n", "-o", f"{mp}=foo"], frozenset([mp]))
        self.assertEqual(([], [mp]), params.fix_recv_opts(["-n"], frozenset([mp])))
        self.assertEqual((["-u", mp], [mp]), params.fix_recv_opts(["-n", "-u", mp], frozenset([mp])))
        self.assertEqual((["-x", mp], []), params.fix_recv_opts(["-n", "-x", mp], frozenset([mp])))
        self.assertEqual((["-x", mp, "-x", cr], []), params.fix_recv_opts(["-n", "-x", mp, "-x", cr], frozenset([mp])))
        self.assertEqual(([], [cr, mp]), params.fix_recv_opts([], frozenset([mp, cr])))

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

    def test_set_last_modification_time_with_file_not_found_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file = os.path.join(tmpdir, "foo")
            with patch("bzfs_main.bzfs.os_utime", side_effect=FileNotFoundError):
                with self.assertRaises(FileNotFoundError):
                    bzfs.set_last_modification_time(file + "nonexisting", unixtime_in_secs=1001, if_more_recent=False)
                file = os.path.join(file, "x", "nonexisting2")
                bzfs.set_last_modification_time_safe(file, unixtime_in_secs=1001, if_more_recent=False)

    def test_run_main_with_unexpected_exception(self) -> None:
        try:
            args = argparser_parse_args(args=["src", "dst"])
            log_params = bzfs.LogParams(args)
            job = bzfs.Job()
            job.params = bzfs.Params(args, log_params=log_params)

            with patch("time.monotonic_ns", side_effect=ValueError("my value error")):
                with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit):
                    job.run_main(args)
        finally:
            bzfs.reset_logger()

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
        logdir = os.path.join(bzfs.get_home_directory(), bzfs.log_dir_default + "-tmp")
        try:
            bzfs.LogParams(bzfs.argument_parser().parse_args(args=["src", "dst", "--log-dir=" + logdir]))
            self.assertTrue(os.path.exists(logdir))
        finally:
            shutil.rmtree(logdir, ignore_errors=True)

        logdir = os.path.join(bzfs.get_home_directory(), "bzfs-tmp")
        with self.assertRaises(SystemExit):
            bzfs.LogParams(bzfs.argument_parser().parse_args(args=["src", "dst", "--log-dir=" + logdir]))
        self.assertFalse(os.path.exists(logdir))

    def test_logdir_must_not_be_symlink(self) -> None:
        with tempfile.TemporaryDirectory(prefix="logdir_symlink_test") as tmpdir:
            target = os.path.join(tmpdir, "target")
            os.mkdir(target)
            link_path = os.path.join(tmpdir, bzfs.log_dir_default + "-link")
            os.symlink(target, link_path)
            with self.assertRaises(SystemExit) as cm:
                bzfs.LogParams(bzfs.argument_parser().parse_args(args=["src", "dst", "--log-dir=" + link_path]))
            self.assertEqual(bzfs.die_status, cm.exception.code)
            self.assertIn("--log-dir must not be a symlink", str(cm.exception))

    def test_get_logger_with_cleanup(self) -> None:
        def check(log: Logger, files: set[str]) -> None:
            files_todo = files.copy()
            for handler in log.handlers:
                if isinstance(handler, logging.FileHandler):
                    self.assertIn(handler.baseFilename, files_todo)
                    files_todo.remove(handler.baseFilename)
            self.assertEqual(0, len(files_todo))

        bzfs.reset_logger()
        prefix = "test_get_logger:"
        args = argparser_parse_args(args=["src", "dst"])
        root_logger = logging.getLogger()
        log_params = bzfs.LogParams(args)
        log = bzfs.get_logger(log_params, args, root_logger)
        self.assertTrue(log is root_logger)
        log.info(f"{prefix}aaa1")

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

    def test_log_config_file_validation(self) -> None:
        args = argparser_parse_args(["src", "dst", "--log-config-file", "+bad_file_name.txt"])
        log_params = bzfs.LogParams(args)
        with self.assertRaises(SystemExit):
            bzfs.get_dict_config_logger(log_params, args)

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

    def test_custom_ssh_config_file_must_match_file_name_pattern(self) -> None:
        args = argparser_parse_args(["src", "dst", "--ssh-src-config-file", "bad_file_name.cfg"])
        with self.assertRaises(SystemExit):
            bzfs.Params(args)

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

    @staticmethod
    def root_datasets_if_recursive_zfs_snapshot_is_possible_slow_but_correct(  # compare faster algos to this baseline impl
        src_datasets: list[str], basis_src_datasets: list[str]
    ) -> list[str] | None:
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
        def run_filter(src_datasets: list[str], basis_src_datasets: list[str]) -> list[str]:
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

    def test_CreateSrcSnapshotConfig(self) -> None:  # noqa: N802
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

    def test_MonitorSnapshotsConfig(self) -> None:  # noqa: N802
        def plan(alerts: dict[str, Any]) -> str:
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

    def test_validate_no_argument_file_raises(self) -> None:
        parser = argparse.ArgumentParser()
        ns = argparse.Namespace(no_argument_file=True)
        with self.assertRaises(SystemExit):
            bzfs.validate_no_argument_file("afile", ns, err_prefix="e", parser=parser)

    def test_die_with_parser(self) -> None:
        parser = argparse.ArgumentParser()
        with self.assertRaises(SystemExit):
            bzfs.die("boom", parser=parser)


#############################################################################
class TestAdditionalHelpers(unittest.TestCase):

    def test_params_verbose_zfs_and_bwlimit(self) -> None:
        args = argparser_parse_args(["src", "dst", "-v", "-v", "--bwlimit", "20m"])
        params = bzfs.Params(args)
        self.assertIn("-v", params.zfs_send_program_opts)
        self.assertIn("-v", params.zfs_recv_program_opts)
        self.assertIn("--rate-limit=20m", params.pv_program_opts)

    def test_program_name_injections(self) -> None:
        args = argparser_parse_args(["src", "dst"])
        p1 = bzfs.Params(args, inject_params={"inject_unavailable_ssh": True})
        self.assertEqual("ssh-xxx", p1.program_name("ssh"))
        p2 = bzfs.Params(args, inject_params={"inject_failing_ssh": True})
        self.assertEqual("false", p2.program_name("ssh"))

    def test_unset_matching_env_vars(self) -> None:
        with patch.dict(os.environ, {"FOO_BAR": "x"}):
            args = argparser_parse_args(["src", "dst", "--exclude-envvar-regex", "FOO.*"])
            params = bzfs.Params(args, log_params=bzfs.LogParams(args), log=logging.getLogger())
            params.unset_matching_env_vars(args)
            self.assertNotIn("FOO_BAR", os.environ)

    def test_local_ssh_command_variants(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = os.path.join(tmpdir, "bzfs_ssh_config")
            Path(cfg).touch()
            args = argparser_parse_args(["src", "dst", "--ssh-src-config-file", cfg])
            args.ssh_src_port = 2222
            p = bzfs.Params(args)
            r = bzfs.Remote("src", args, p)
            r.ssh_user_host = "user@host"
            r.ssh_user = "user"
            r.ssh_host = "host"
            cmd = r.local_ssh_command()
            self.assertEqual(cmd[0], p.ssh_program)
            self.assertIn("-F", cmd)
            self.assertIn(cfg, cmd)
            self.assertIn("-p", cmd)
            self.assertIn("2222", cmd)
            self.assertIn("-S", cmd)
            self.assertEqual("user@host", cmd[-1])

            r.reuse_ssh_connection = False
            cmd = r.local_ssh_command()
            self.assertNotIn("-S", cmd)

            args = argparser_parse_args(["src", "dst", "--ssh-program", "-"])
            p = bzfs.Params(args)
            r = bzfs.Remote("src", args, p)
            r.ssh_user_host = "u@h"
            with self.assertRaises(SystemExit):
                r.local_ssh_command()  # Cannot talk to remote host because ssh CLI is disabled
            r.ssh_user_host = ""
            self.assertEqual([], r.local_ssh_command())

    def test_params_zfs_recv_program_opt(self) -> None:
        args = argparser_parse_args(["src", "dst", "--zfs-recv-program-opt=-o", "--zfs-recv-program-opt=org.test=value"])
        params = bzfs.Params(args)
        self.assertIn("-o", params.zfs_recv_program_opts)
        self.assertIn("org.test=value", params.zfs_recv_program_opts)

    def test_copy_properties_config_repr(self) -> None:
        args = argparser_parse_args(["src", "dst"])
        params = bzfs.Params(args)
        conf = bzfs.CopyPropertiesConfig("zfs_recv_o", "-o", args, params)
        rep = repr(conf)
        self.assertIn("sources", rep)
        self.assertIn("targets", rep)

    def test_job_shutdown_calls_pool_shutdown(self) -> None:
        job = bzfs.Job()
        mock_pool = MagicMock()
        job.remote_conf_cache = {("k",): bzfs.RemoteConfCacheItem(mock_pool, {}, {})}
        job.shutdown()
        mock_pool.shutdown.assert_called_once()

    @patch("bzfs_main.bzfs.argument_parser")
    @patch("bzfs_main.bzfs.run_main")
    def test_main_handles_calledprocesserror(self, mock_run_main: MagicMock, mock_arg_parser: MagicMock) -> None:
        mock_arg_parser.return_value.parse_args.return_value = argparser_parse_args(["src", "dst"])
        mock_run_main.side_effect = subprocess.CalledProcessError(returncode=5, cmd=["cmd"])
        with self.assertRaises(SystemExit) as ctx:
            bzfs.main()
        self.assertEqual(5, ctx.exception.code)

    @patch("bzfs_main.bzfs.Job.run_main")
    def test_run_main_delegates_to_job(self, mock_run_main: MagicMock) -> None:
        args = argparser_parse_args(["src", "dst"])
        bzfs.run_main(args=args, sys_argv=["p"])
        mock_run_main.assert_called_once_with(args, ["p"], None)

    def test_pv_program_opts_disallows_dangerous_options(self) -> None:
        """Confirms that initialization fails if --pv-program-opts contains the forbidden -f or --log-file options."""
        # Test Case 1: The short-form option '-f' should be rejected.
        malicious_opts_short = "--bytes -f /etc/hosts"
        args_short = argparser_parse_args(["src", "dst", f"--pv-program-opts={malicious_opts_short}"])
        with self.assertRaises(SystemExit):
            bzfs.Params(args_short)

        # Test Case 2: The long-form option '--log-file' should be rejected.
        malicious_opts_long = "--progress --log-file /etc/shadow"
        args_long = argparser_parse_args(["src", "dst", f"--pv-program-opts={malicious_opts_long}"])
        with self.assertRaises(SystemExit):
            bzfs.Params(args_long)

        # Test Case 3: A valid set of options should instantiate successfully.
        valid_opts = "--bytes --progress --rate"
        args_valid = argparser_parse_args(["src", "dst", f"--pv-program-opts={valid_opts}"])

        # This should not raise an exception.
        params_valid = bzfs.Params(args_valid)
        self.assertIn("--rate", params_valid.pv_program_opts)

    def test_compression_program_opts_disallows_dangerous_options(self) -> None:
        malicious_opts_short = "-o /etc/hosts"
        args_short = argparser_parse_args(["src", "dst", f"--compression-program-opts={malicious_opts_short}"])
        with self.assertRaises(SystemExit):
            bzfs.Params(args_short)

        malicious_opts_long = "--output-file /etc/hosts"
        args_long = argparser_parse_args(["src", "dst", f"--compression-program-opts={malicious_opts_long}"])
        with self.assertRaises(SystemExit):
            bzfs.Params(args_long)

        valid_opts = "-9"
        args_valid = argparser_parse_args(["src", "dst", f"--compression-program-opts={valid_opts}"])
        params_valid = bzfs.Params(args_valid)
        self.assertIn("-9", params_valid.compression_program_opts)

    def test_mbuffer_program_opts_disallows_dangerous_options(self) -> None:
        malicious_opts = "-o /etc/hosts"
        args_short = argparser_parse_args(["src", "dst", f"--mbuffer-program-opts={malicious_opts}"])
        with self.assertRaises(SystemExit):
            bzfs.Params(args_short)

        valid_opts = "-q"
        args_valid = argparser_parse_args(["src", "dst", f"--mbuffer-program-opts={valid_opts}"])
        params_valid = bzfs.Params(args_valid)
        self.assertIn("-q", params_valid.mbuffer_program_opts)


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

        def now_fn(tz: tzinfo | None = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = bzfs.current_datetime(tz_spec="UTC", now_fn=now_fn)
        self.assertEqual(expected, actual)

    def test_tzoffset_positive(self) -> None:
        tz_spec = "+0530"
        tz = timezone(timedelta(hours=5, minutes=30))
        expected = self.fixed_datetime.astimezone(tz=tz)

        def now_fn(_: tzinfo | None = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = bzfs.current_datetime(tz_spec=tz_spec, now_fn=now_fn)
        self.assertEqual(expected, actual)

    def test_tzoffset_negative(self) -> None:
        tz_spec = "-0430"
        tz = timezone(timedelta(hours=-4, minutes=-30))
        expected = self.fixed_datetime.astimezone(tz=tz)

        def now_fn(_: tzinfo | None = None) -> datetime:
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

        def now_fn(_: tzinfo | None = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = bzfs.current_datetime(tz_spec=tz_spec, now_fn=now_fn)
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

    def test_unixtime_conversion(self) -> None:
        iso = "2024-01-02T03:04:05+00:00"
        unix = bzfs.unixtime_fromisoformat(iso)
        expected = "2024-01-02_03:04:05+00:00"
        self.assertEqual(expected, bzfs.isotime_from_unixtime(unix))

    def test_get_timezone_variants(self) -> None:
        if sys.version_info < (3, 9):
            self.skipTest("ZoneInfo requires python >= 3.9")
        self.assertIsNone(bzfs.get_timezone())
        self.assertEqual(timezone.utc, bzfs.get_timezone("UTC"))
        tz = bzfs.get_timezone("+0130")
        assert tz is not None
        self.assertEqual(90 * 60, cast(timedelta, tz.utcoffset(None)).total_seconds())
        zone = bzfs.get_timezone("Europe/Vienna")
        self.assertEqual("Europe/Vienna", getattr(zone, "key", None))
        with self.assertRaises(ValueError):
            bzfs.get_timezone("bad-tz")


#############################################################################
class TestArgumentParser(unittest.TestCase):

    def test_help(self) -> None:
        if is_solaris_zfs():
            self.skipTest("FIXME: BlockingIOError: [Errno 11] write could not complete without blocking")
        parser = bzfs.argument_parser()
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as e:
            parser.parse_args(["--help"])
        self.assertEqual(0, e.exception.code)

    def test_version(self) -> None:
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["--version"])
        self.assertEqual(0, e.exception.code)

    def test_missing_datasets(self) -> None:
        parser = bzfs.argument_parser()
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as e:
            parser.parse_args(["--retries=1"])
        self.assertEqual(2, e.exception.code)

    def test_missing_dst_dataset(self) -> None:
        parser = bzfs.argument_parser()
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as e:
            parser.parse_args(["src_dataset"])  # Each SRC_DATASET must have a corresponding DST_DATASET
        self.assertEqual(2, e.exception.code)

    def test_program_must_not_be_empty_string(self) -> None:
        parser = bzfs.argument_parser()
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as e:
            parser.parse_args(["src_dataset", "src_dataset", "--zfs-program="])
        self.assertEqual(2, e.exception.code)


###############################################################################
class TestAddRecvPropertyOptions(unittest.TestCase):

    def setUp(self) -> None:
        args = argparser_parse_args(["src", "dst"])
        self.p = bzfs.Params(args)
        self.p.src = bzfs.Remote("src", args, self.p)
        self.p.dst = bzfs.Remote("dst", args, self.p)
        self.p.zfs_recv_x_names = ["xprop1", "xprop2"]
        self.p.zfs_recv_ox_names = {"existing"}
        self.job = bzfs.Job()
        self.job.params = self.p

    def test_appends_x_options_when_supported(self) -> None:
        recv_opts: list[str] = []
        with patch.object(self.job, "is_program_available", return_value=True):
            result_opts, set_opts = self.job.add_recv_property_options(True, recv_opts, "ds", {})
        self.assertEqual(["-x", "xprop1", "-x", "xprop2"], result_opts)
        self.assertEqual([], set_opts)
        # original zfs_recv_ox_names remains unchanged
        self.assertEqual({"existing"}, self.p.zfs_recv_ox_names)

    def test_skips_x_options_when_not_supported(self) -> None:
        recv_opts: list[str] = []
        with patch.object(self.job, "is_program_available", return_value=False):
            result_opts, set_opts = self.job.add_recv_property_options(True, recv_opts, "ds", {})
        self.assertEqual([], result_opts)
        self.assertEqual([], set_opts)
        self.assertEqual({"existing"}, self.p.zfs_recv_ox_names)


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
        with patch("bzfs_main.bzfs.open_nofollow", mock_open(read_data="src1\tdst1\nsrc2\tdst2\n")):
            args = self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
            self.assertEqual(args.input, [("src1", "dst1"), ("src2", "dst2")])

    def test_file_input_without_trailing_newline(self) -> None:
        with patch("bzfs_main.bzfs.open_nofollow", mock_open(read_data="src1\tdst1\nsrc2\tdst2")):
            args = self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
            self.assertEqual(args.input, [("src1", "dst1"), ("src2", "dst2")])

    def test_mixed_input(self) -> None:
        with patch("bzfs_main.bzfs.open_nofollow", mock_open(read_data="src1\tdst1\nsrc2\tdst2\n")):
            args = self.parser.parse_args(["--input", "src0", "dst0", "+test_bzfs_argument_file"])
            self.assertEqual(args.input, [("src0", "dst0"), ("src1", "dst1"), ("src2", "dst2")])

    def test_file_skip_comments_and_empty_lines(self) -> None:
        with patch("bzfs_main.bzfs.open_nofollow", mock_open(read_data="\n\n#comment\nsrc1\tdst1\nsrc2\tdst2\n")):
            args = self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
            self.assertEqual(args.input, [("src1", "dst1"), ("src2", "dst2")])

    def test_file_skip_stripped_empty_lines(self) -> None:
        with patch("bzfs_main.bzfs.open_nofollow", mock_open(read_data=" \t \nsrc1\tdst1")):
            args = self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
            self.assertEqual(args.input, [("src1", "dst1")])

    def test_file_missing_tab(self) -> None:
        with patch("bzfs_main.bzfs.open_nofollow", mock_open(read_data="src1\nsrc2")):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+test_bzfs_argument_file"])

    def test_file_whitespace_only(self) -> None:
        with patch("bzfs_main.bzfs.open_nofollow", mock_open(read_data=" \tdst1")):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+test_bzfs_argument_file"])

        with patch("bzfs_main.bzfs.open_nofollow", mock_open(read_data="src1\t ")):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+test_bzfs_argument_file"])

        with patch("bzfs_main.bzfs.open_nofollow", side_effect=FileNotFoundError):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+nonexistent_test_bzfs_argument_file"])

    def test_option_not_specified(self) -> None:
        args = self.parser.parse_args([])
        self.assertIsNone(args.input)

    def test_dataset_pairs_action_invalid_basename(self) -> None:
        with patch("bzfs_main.bzfs.open_nofollow", mock_open(read_data="src\tdst\n")):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+bad_file_name"])


#############################################################################
class TestPreservePropertiesValidation(unittest.TestCase):
    def setUp(self) -> None:
        self.args = argparser_parse_args(
            [
                "src",
                "dst",
                "--preserve-properties",
                "recordsize",
                "--zfs-send-program-opts=--props --raw --compressed",
            ]
        )
        log_params = bzfs.LogParams(self.args)
        self.p = bzfs.Params(self.args, log_params=log_params, log=bzfs.get_logger(log_params, self.args))
        self.job = bzfs.Job()
        self.job.params = self.p

        # Setup minimal remote objects. The specific details don't matter much as they are mocked.
        self.p.src = bzfs.Remote("src", self.args, self.p)
        self.p.dst = bzfs.Remote("dst", self.args, self.p)
        self.p.src.ssh_user_host = "src_host"
        self.p.dst.ssh_user_host = "dst_host"
        self.p.connection_pools = {
            "local": MagicMock(spec=bzfs.ConnectionPools),
            "src": MagicMock(spec=bzfs.ConnectionPools),
            "dst": MagicMock(spec=bzfs.ConnectionPools),
        }
        self.p.zpool_features = {"src": {}, "dst": {}}
        self.p.available_programs = {"local": {"ssh": ""}, "src": {}, "dst": {}}

    def tearDown(self) -> None:
        bzfs.reset_logger()

    @patch.object(bzfs.Job, "detect_zpool_features")
    @patch.object(bzfs.Job, "detect_available_programs_remote")
    def test_preserve_properties_fails_on_old_zfs_with_props(
        self, mock_detect_progs: MagicMock, mock_detect_features: MagicMock
    ) -> None:
        """Verify that using --preserve-properties with --props fails on ZFS < 2.2.0."""
        with patch.object(self.job, "is_program_available") as mock_is_available:

            # Make the mock specific to the zfs>=2.2.0 check on dst
            def side_effect(program: str, location: str) -> bool:
                if program == bzfs.zfs_version_is_at_least_2_2_0 and location == "dst":
                    return False
                return True  # Assume other programs are available

            mock_is_available.side_effect = side_effect
            with self.assertRaises(SystemExit) as cm:
                self.job.detect_available_programs()
            self.assertEqual(cm.exception.code, bzfs.die_status)
            self.assertIn("--preserve-properties is unreliable on destination ZFS < 2.2.0", str(cm.exception))

    @patch.object(bzfs.Job, "detect_zpool_features")
    @patch.object(bzfs.Job, "detect_available_programs_remote")
    def test_preserve_properties_succeeds_on_new_zfs_with_props(
        self, mock_detect_progs: MagicMock, mock_detect_features: MagicMock
    ) -> None:
        """Verify that --preserve-properties is allowed on ZFS >= 2.2.0."""
        with patch.object(self.job, "is_program_available", return_value=True):
            # This should not raise an exception
            self.job.detect_available_programs()

    @patch.object(bzfs.Job, "detect_zpool_features")
    @patch.object(bzfs.Job, "detect_available_programs_remote")
    def test_preserve_properties_succeeds_on_old_zfs_without_props(
        self, mock_detect_progs: MagicMock, mock_detect_features: MagicMock
    ) -> None:
        """Verify --preserve-properties is allowed on old ZFS if --props is not used."""
        self.p.zfs_send_program_opts.remove("--props")  # Modify the params for this test case
        with patch.object(self.job, "is_program_available") as mock_is_available:
            mock_is_available.return_value = False  # Simulate old ZFS
            # This should not raise an exception
            self.job.detect_available_programs()


#############################################################################
class TestFileOrLiteralAction(unittest.TestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--input", nargs="+", action=bzfs.FileOrLiteralAction)

    def test_direct_value(self) -> None:
        args = self.parser.parse_args(["--input", "literalvalue"])
        self.assertEqual(args.input, ["literalvalue"])

    def test_file_input(self) -> None:
        with patch("bzfs_main.bzfs.open_nofollow", mock_open(read_data="line 1\nline 2  \n")):
            args = self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
            self.assertEqual(args.input, ["line 1", "line 2  "])

    def test_mixed_input(self) -> None:
        with patch("bzfs_main.bzfs.open_nofollow", mock_open(read_data="line 1\nline 2")):
            args = self.parser.parse_args(["--input", "literalvalue", "+test_bzfs_argument_file"])
            self.assertEqual(args.input, ["literalvalue", "line 1", "line 2"])

    def test_skip_comments_and_empty_lines(self) -> None:
        with patch("bzfs_main.bzfs.open_nofollow", mock_open(read_data="\n\n#comment\nline 1\n\n\nline 2\n")):
            args = self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
            self.assertEqual(args.input, ["line 1", "line 2"])

    def test_file_not_found(self) -> None:
        with patch("bzfs_main.bzfs.open_nofollow", side_effect=FileNotFoundError):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+nonexistent_test_bzfs_argument_file"])

    def test_option_not_specified(self) -> None:
        args = self.parser.parse_args([])
        self.assertIsNone(args.input)

    def test_file_or_literal_action_invalid_basename(self) -> None:
        with patch("bzfs_main.bzfs.open_nofollow", mock_open(read_data="line")):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+bad_file_name"])


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
class TestNonEmptyStringAction(unittest.TestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--name", action=bzfs.NonEmptyStringAction)

    def test_non_empty_string_action_empty(self) -> None:
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["--name", " "])


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
class SSHConfigFileNameAction(unittest.TestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("filename", action=bzfs.SSHConfigFileNameAction)

    def test_safe_filename(self) -> None:
        args = self.parser.parse_args(["file1.txt"])
        self.assertEqual(args.filename, "file1.txt")

    def test_empty_filename(self) -> None:
        with self.assertRaises(SystemExit):
            self.parser.parse_args([""])

    def test_filename_in_subdirectory(self) -> None:
        self.parser.parse_args(["subdir/safe_file.txt"])

    def test_filename_with_single_dot_slash(self) -> None:
        self.parser.parse_args(["./file.txt"])

    def test_ssh_config_filename_action_invalid_chars(self) -> None:
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["foo bar"])


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

    def test_filename_with_tab(self) -> None:
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["foo\nbar.txt"])


#############################################################################
class TestSafeDirectoryNameAction(unittest.TestCase):
    def test_valid_directory_name_is_accepted(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--dir", action=bzfs.SafeDirectoryNameAction)
        args = parser.parse_args(["--dir", "valid_directory"])
        assert args.dir == "valid_directory"

    def test_empty_directory_name_raises_error(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--dir", action=bzfs.SafeDirectoryNameAction)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--dir", ""])

    def test_directory_name_with_invalid_whitespace_raises_error(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--dir", action=bzfs.SafeDirectoryNameAction)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--dir", "invalid\nname"])

    def test_directory_name_with_leading_or_trailing_spaces_is_trimmed(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--dir", action=bzfs.SafeDirectoryNameAction)
        args = parser.parse_args(["--dir", "  valid_directory  "])
        assert args.dir == "valid_directory"


#############################################################################
class TestCheckRange(unittest.TestCase):

    def test_valid_range_min_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange, min=0, max=100)
        args = parser.parse_args(["--age", "50"])
        self.assertEqual(args.age, 50)

    def test_valid_range_inf_sup(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange, inf=0, sup=100)
        args = parser.parse_args(["--age", "50"])
        self.assertEqual(args.age, 50)

    def test_invalid_range_min_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange, min=0, max=100)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "-1"])

    def test_invalid_range_inf_sup(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange, inf=0, sup=100)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "101"])

    def test_invalid_combination_min_inf(self) -> None:
        with self.assertRaises(ValueError):
            parser = argparse.ArgumentParser()
            parser.add_argument("--age", type=int, action=CheckRange, min=0, inf=100)

    def test_invalid_combination_max_sup(self) -> None:
        with self.assertRaises(ValueError):
            parser = argparse.ArgumentParser()
            parser.add_argument("--age", type=int, action=CheckRange, max=0, sup=100)

    def test_valid_float_range_min_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(["--age", "50.5"])
        self.assertEqual(args.age, 50.5)

    def test_invalid_float_range_min_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange, min=0.0, max=100.0)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "-0.1"])

    def test_valid_edge_case_min(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(["--age", "0.0"])
        self.assertEqual(args.age, 0.0)

    def test_valid_edge_case_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(["--age", "100.0"])
        self.assertEqual(args.age, 100.0)

    def test_invalid_edge_case_sup(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange, inf=0.0, sup=100.0)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "100.0"])

    def test_invalid_edge_case_inf(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange, inf=0.0, sup=100.0)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "0.0"])

    def test_no_range_constraints(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange)
        args = parser.parse_args(["--age", "150"])
        self.assertEqual(args.age, 150)

    def test_no_range_constraints_float(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange)
        args = parser.parse_args(["--age", "150.5"])
        self.assertEqual(args.age, 150.5)

    def test_very_large_value(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange, max=10**18)
        args = parser.parse_args(["--age", "999999999999999999"])
        self.assertEqual(args.age, 999999999999999999)

    def test_very_small_value(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange, min=-(10**18))
        args = parser.parse_args(["--age", "-999999999999999999"])
        self.assertEqual(args.age, -999999999999999999)

    def test_default_interval(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange)
        action = CheckRange(option_strings=["--age"], dest="age")
        self.assertEqual(action.interval(), "valid range: (-infinity, +infinity)")

    def test_interval_with_inf_sup(self) -> None:
        action = CheckRange(option_strings=["--age"], dest="age", inf=0, sup=100)
        self.assertEqual(action.interval(), "valid range: (0, 100)")

    def test_interval_with_min_max(self) -> None:
        action = CheckRange(option_strings=["--age"], dest="age", min=0, max=100)
        self.assertEqual(action.interval(), "valid range: [0, 100]")

    def test_interval_with_min(self) -> None:
        action = CheckRange(option_strings=["--age"], dest="age", min=0)
        self.assertEqual(action.interval(), "valid range: [0, +infinity)")

    def test_interval_with_max(self) -> None:
        action = CheckRange(option_strings=["--age"], dest="age", max=100)
        self.assertEqual(action.interval(), "valid range: (-infinity, 100]")

    def test_call_without_range_constraints(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange)
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
        input_snapshots: list[str] = ["h0", "h1", "d1", "d2", "h2", "d3", "d4"]
        expected_results: list[str] = ["d1", "d2", "d3", "d4"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic4(self) -> None:
        input_snapshots: list[str] = ["d1"]
        expected_results: list[str] = ["d1"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic5(self) -> None:
        input_snapshots: list[str] = []
        expected_results: list[str] = []
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_validate_snapshot_series_excluding_hourlies_with_permutations(self) -> None:
        for i, testcase in enumerate(self.permute_snapshot_series()):
            with stop_on_failure_subtest(i=i):
                self.validate_incremental_send_steps(testcase[None], testcase["d"])

    def test_send_step_to_str(self) -> None:
        bzfs.Job().send_step_to_str(("-I", "d@s1", "d@s3"))

    def permute_snapshot_series(self, max_length: int = 9) -> list[defaultdict[str | None, list[str]]]:
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
        for L in range(max_length + 1):  # noqa: N806
            for N in range(L + 1):  # noqa: N806
                steps = "d" * N + "h" * (L - N)
                # compute a permutation of several 'd' and 'h' chars that represents the snapshot series
                for permutation in sorted(set(itertools.permutations(steps, len(steps)))):
                    snaps: defaultdict[str | None, list[str]] = defaultdict(list)
                    count: defaultdict[str, int] = defaultdict(int)
                    for char in permutation:
                        count[char] += 1  # tag snapshots with a monotonically increasing number within each category
                        char_count = f"{count[char]:01}" if max_length < 10 else f"{count[char]:02}"  # zero pad number
                        snapshot = f"{char}{char_count}"
                        snaps[None].append(snapshot)
                        snaps[char].append(snapshot)  # represents expected results for test verification
                    testcases.append(snaps)
        return testcases

    def validate_incremental_send_steps(self, input_snapshots: list[str], expected_results: list[str]) -> None:
        """Computes steps to incrementally replicate the daily snapshots of the given daily and/or hourly input
        snapshots. Applies the steps and compares the resulting destination snapshots with the expected results."""
        for is_resume in [False, True]:  # via --no-resume-recv
            for src_dataset in ["", "s@"]:
                for force_convert_I_to_i in [False, True]:  # noqa: N806
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

    def send_step_to_str(self, step: tuple) -> str:
        # return str(step)
        return str(step[1]) + ("-" if step[0] == "-I" else ":") + str(step[2])

    def apply_incremental_send_steps(self, steps: list[tuple], input_snapshots: list[str]) -> list[str]:
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
        input_snapshots: list[str],
        src_dataset: str,
        is_resume: bool = False,
        force_convert_I_to_i: bool = False,  # noqa: N803
    ) -> list[tuple]:
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
        origin_src_snapshots_with_guids: list[str],
        is_resume: bool = False,
        force_convert_I_to_i: bool = False,  # noqa: N803
    ) -> list[tuple]:
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
# class TestItrSSHCmdParallel(unittest.TestCase)
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
class TestRemoteConfCache(unittest.TestCase):

    def test_remote_conf_cache_hit_skips_detection(self) -> None:
        args = argparser_parse_args(["src", "dst"])
        p = bzfs.Params(args)
        p.log = MagicMock()
        job = bzfs.Job()
        job.params = p
        p.src = bzfs.Remote("src", args, p)
        p.dst = bzfs.Remote("dst", args, p)
        p.src.ssh_host = "host"
        p.src.ssh_user_host = "host"
        p.dst.ssh_host = "host2"
        p.dst.ssh_user_host = "host2"
        job.params.available_programs["local"] = {"ssh": ""}
        pools = bzfs.ConnectionPools(p.src, {bzfs.SHARED: 1, bzfs.DEDICATED: 1})
        item = bzfs.RemoteConfCacheItem(pools, {"os": "Linux"}, {"feat": "on"}, time.monotonic_ns())
        job.remote_conf_cache[p.src.cache_key()] = item
        job.remote_conf_cache[p.dst.cache_key()] = item
        with patch.object(job, "detect_available_programs_remote") as d1, patch.object(job, "detect_zpool_features") as d2:
            job.detect_available_programs()
            d1.assert_not_called()
            d2.assert_not_called()

    def test_remote_conf_cache_miss_runs_detection(self) -> None:
        args = argparser_parse_args(["src", "dst", "--daemon-remote-conf-cache-ttl", "10 milliseconds"])
        p = bzfs.Params(args)
        p.log = MagicMock()
        job = bzfs.Job()
        job.params = p
        p.src = bzfs.Remote("src", args, p)
        p.dst = bzfs.Remote("dst", args, p)
        p.src.ssh_host = "host"
        p.src.ssh_user_host = "host"
        p.dst.ssh_host = "host2"
        p.dst.ssh_user_host = "host2"
        job.params.available_programs["local"] = {"ssh": ""}
        pools = bzfs.ConnectionPools(p.src, {bzfs.SHARED: 1, bzfs.DEDICATED: 1})
        expired_ts = time.monotonic_ns() - p.remote_conf_cache_ttl_nanos - 1
        item = bzfs.RemoteConfCacheItem(pools, {"os": "Linux"}, {"feat": "on"}, expired_ts)
        job.remote_conf_cache[p.src.cache_key()] = item
        job.remote_conf_cache[p.dst.cache_key()] = item
        with patch.object(job, "detect_available_programs_remote") as d1, patch.object(job, "detect_zpool_features") as d2:
            d1.side_effect = lambda r, programs, host: programs.__setitem__(r.location, {"ssh": ""})
            d2.side_effect = lambda r: job.params.zpool_features.__setitem__(r.location, {"feat": "on"})
            job.detect_available_programs()
            self.assertEqual(d1.call_count, 2)
            self.assertEqual(d2.call_count, 2)


#############################################################################
class TestLogging(unittest.TestCase):

    def test_get_default_logger(self) -> None:
        args = argparser_parse_args(["src", "dst"])
        lp = bzfs.LogParams(args)
        logging.getLogger(bzfs.get_logger_subname()).handlers.clear()
        logging.getLogger(bzfs.__name__).handlers.clear()
        log = bzfs.get_default_logger(lp, args)
        self.assertTrue(any(isinstance(h, logging.StreamHandler) for h in log.handlers))
        self.assertTrue(any(isinstance(h, logging.FileHandler) for h in log.handlers))

    def test_get_default_logger_considers_existing_sublog_handlers(self) -> None:
        args = argparser_parse_args(["src", "dst"])
        lp = bzfs.LogParams(args)
        sublog = logging.getLogger(bzfs.get_logger_subname())
        sublog.handlers.clear()
        log = logging.getLogger(bzfs.__name__)
        log.handlers.clear()
        stream_h: logging.StreamHandler | None = None
        file_h: logging.FileHandler | None = None
        try:
            stream_h = logging.StreamHandler(stream=sys.stdout)
            file_h = logging.FileHandler(lp.log_file, encoding="utf-8")
            sublog.addHandler(stream_h)
            sublog.addHandler(file_h)
            log_result = bzfs.get_default_logger(lp, args)
            self.assertEqual([], log_result.handlers)
        finally:
            if stream_h is not None:
                stream_h.close()
            if file_h is not None:
                file_h.close()
            sublog.handlers.clear()
            bzfs.reset_logger()

    @patch("logging.handlers.SysLogHandler")
    def test_get_default_logger_syslog_warning(self, mock_syslog: MagicMock) -> None:
        args = argparser_parse_args(
            [
                "src",
                "dst",
                "--log-syslog-address=127.0.0.1:514",
                "--log-syslog-socktype=UDP",
                "--log-syslog-facility=1",
                "--log-syslog-level=DEBUG",
            ]
        )
        lp = bzfs.LogParams(args)
        self.assertEqual("127.0.0.1:514", args.log_syslog_address)
        self.assertEqual("UDP", args.log_syslog_socktype)
        self.assertEqual(1, args.log_syslog_facility)
        self.assertEqual("DEBUG", args.log_syslog_level)
        logging.getLogger(bzfs.get_logger_subname()).handlers.clear()
        logger = logging.getLogger(bzfs.__name__)
        logger.handlers.clear()
        handler = logging.Handler()
        mock_syslog.return_value = handler
        try:
            with patch.object(logger, "warning") as mock_warning:
                log = bzfs.get_default_logger(lp, args)
                mock_syslog.assert_called_once()
                self.assertIn(handler, log.handlers)
                mock_warning.assert_called_once()
        finally:
            bzfs.reset_logger()

    def test_remove_json_comments(self) -> None:
        config_str = (
            "#c1\n" "line_without_comment\n" "line_with_trailing_hash_only #\n" "line_with_trailing_comment##tail#\n"
        )
        expected = "\nline_without_comment\nline_with_trailing_hash_only #\n" "line_with_trailing_comment#"
        self.assertEqual(expected, bzfs.remove_json_comments(config_str))

    def test_get_dict_config_logger(self) -> None:
        with tempfile.NamedTemporaryFile("w", prefix="bzfs_log_config", suffix=".json", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "version": 1,
                    "handlers": {"h": {"class": "logging.StreamHandler"}},
                    "root": {"level": "${bzfs.log_level}", "handlers": ["h"]},
                },
                f,
            )
            path = f.name
        try:
            args = argparser_parse_args(["src", "dst", f"--log-config-file=+{path}"])
            lp = bzfs.LogParams(args)
            with patch("logging.config.dictConfig") as m:
                bzfs.get_dict_config_logger(lp, args)
                self.assertEqual(lp.log_level, m.call_args[0][0]["root"]["level"])
        finally:
            os.remove(path)

    def test_get_dict_config_logger_inline_string_with_defaults(self) -> None:
        config = (
            '{"version": 1, "handlers": {"h": {"class": "logging.StreamHandler"}}, '
            '"root": {"level": "${missing:DEBUG}", "handlers": ["h"]}}'
        )
        args = argparser_parse_args(["src", "dst", "--log-config-file", config])
        lp = bzfs.LogParams(args)
        with patch("logging.config.dictConfig") as m:
            bzfs.get_dict_config_logger(lp, args)
            self.assertEqual("DEBUG", m.call_args[0][0]["root"]["level"])

    def test_get_dict_config_logger_missing_default_raises(self) -> None:
        config = '{"version": 1, "root": {"level": "${missing}"}}'
        args = argparser_parse_args(["src", "dst", "--log-config-file", config])
        lp = bzfs.LogParams(args)
        with self.assertRaises(ValueError):
            bzfs.get_dict_config_logger(lp, args)

    def test_get_dict_config_logger_invalid_name_raises(self) -> None:
        config = '{"version": 1, "root": {"level": "${bad name:INFO}"}}'
        args = argparser_parse_args(["src", "dst", "--log-config-file", config])
        lp = bzfs.LogParams(args)
        with self.assertRaises(ValueError):
            bzfs.get_dict_config_logger(lp, args)

    def test_get_dict_config_logger_invalid_path(self) -> None:
        with tempfile.NamedTemporaryFile("w", prefix="badfilename", suffix=".json", delete=False, encoding="utf-8") as f:
            f.write("{}")
            path = f.name
        try:
            args = argparser_parse_args(["src", "dst", f"--log-config-file=+{path}"])
            lp = bzfs.LogParams(args)
            with self.assertRaises(SystemExit):
                bzfs.get_dict_config_logger(lp, args)
        finally:
            os.remove(path)

    def test_log_config_file_with_validation(self) -> None:
        """Tests that validate_log_config_dict() would prevent an arbitrary code execution vulnerability."""
        malicious_config = {
            "version": 1,
            "handlers": {"safe_handler": {"class": "logging.StreamHandler", "stream": "ext://sys.stdout"}},
            "root": {"handlers": ["safe_handler", {"()": "os.system", "command": "echo pwned > /dev/null"}]},
        }
        malicious_config_str = json.dumps(malicious_config)
        args = argparser_parse_args(["src", "dst", "--skip-replication", "--log-config-file", malicious_config_str])
        lp = bzfs.LogParams(args)
        with patch("os.system") as mock_system:
            # The fixed code should detect the disallowed callable and exit.
            with self.assertRaises(SystemExit) as cm:  # get_dict_config_logger() calls die(), which raises SystemExit.
                bzfs.get_dict_config_logger(lp, args)
            self.assertEqual(cm.exception.code, bzfs.die_status)
            self.assertIn("Disallowed callable 'os.system'", str(cm.exception))
            mock_system.assert_not_called()

        bzfs.validate_log_config_dict(None)  # type: ignore[arg-type]


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
