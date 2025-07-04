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
import logging
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone, tzinfo
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

import bzfs_main.detect
from bzfs_main import bzfs
from bzfs_main.check_range import (
    CheckRange,
)
from bzfs_main.connection import (
    ConnectionPools,
)
from bzfs_main.detect import (
    RemoteConfCacheItem,
    detect_available_programs,
    zfs_version_is_at_least_2_2_0,
)
from bzfs_main.utils import (
    die_status,
)
from bzfs_tests.abstract_testcase import AbstractTestCase
from bzfs_tests.zfs_util import (
    is_solaris_zfs,
)


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
        # TestPerformance,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestHelperFunctions(AbstractTestCase):
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
        params = bzfs.Params(self.argparser_parse_args(args=["src", "dst"]))
        params.validate_quoting([""])
        params.validate_quoting(["foo"])
        with self.assertRaises(SystemExit):
            params.validate_quoting(['foo"'])
        with self.assertRaises(SystemExit):
            params.validate_quoting(["foo'"])
        with self.assertRaises(SystemExit):
            params.validate_quoting(["foo`"])

    def test_validate_arg(self) -> None:
        params = bzfs.Params(self.argparser_parse_args(args=["src", "dst"]))
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
        args = self.argparser_parse_args(args=["src", "dst"])
        args.zpool_program = ""
        with self.assertRaises(SystemExit):
            bzfs.Params(args)
        args.zpool_program = None
        with self.assertRaises(SystemExit):
            bzfs.Params(args)

    def test_validate_program_name_must_not_contain_special_chars(self) -> None:
        args = self.argparser_parse_args(args=["src", "dst"])
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
        params = bzfs.Params(self.argparser_parse_args(args=["src", "dst"]))
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
        args = self.argparser_parse_args(["src", "dst"])
        params = bzfs.Params(args, log_params=bzfs.LogParams(args), log=logging.getLogger())
        self.assertIsNotNone(str(bzfs_main.utils.pretty_print_formatter(params)))

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
        params = bzfs.Params(self.argparser_parse_args(args=["src", "dst"]))
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
        params = bzfs.Params(self.argparser_parse_args(args=["src", "dst"]))
        with self.assertRaises(SystemExit):
            params.fix_recv_opts(["-n", "-o", f"{mp}=foo"], frozenset([mp]))
        self.assertEqual(([], [mp]), params.fix_recv_opts(["-n"], frozenset([mp])))
        self.assertEqual((["-u", mp], [mp]), params.fix_recv_opts(["-n", "-u", mp], frozenset([mp])))
        self.assertEqual((["-x", mp], []), params.fix_recv_opts(["-n", "-x", mp], frozenset([mp])))
        self.assertEqual((["-x", mp, "-x", cr], []), params.fix_recv_opts(["-n", "-x", mp, "-x", cr], frozenset([mp])))
        self.assertEqual(([], [cr, mp]), params.fix_recv_opts([], frozenset([mp, cr])))

    def test_xprint(self) -> None:
        log = logging.getLogger()
        bzfs_main.utils.xprint(log, "foo")
        bzfs_main.utils.xprint(log, "foo", run=True)
        bzfs_main.utils.xprint(log, "foo", run=False)
        bzfs_main.utils.xprint(log, "foo", file=sys.stdout)
        bzfs_main.utils.xprint(log, "")
        bzfs_main.utils.xprint(log, "", run=True)
        bzfs_main.utils.xprint(log, "", run=False)
        bzfs_main.utils.xprint(log, None)

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
            args = self.argparser_parse_args(args=["src", "dst"])
            log_params = bzfs.LogParams(args)
            job = bzfs.Job()
            job.params = bzfs.Params(args, log_params=log_params)

            with patch("time.monotonic_ns", side_effect=ValueError("my value error")):
                with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit):
                    job.run_main(args)
        finally:
            bzfs_main.loggers.reset_logger()

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
        args = self.argparser_parse_args(args=["src", "dst"])
        p = bzfs.Params(args)
        remote = bzfs.Remote("src", args, p)
        bzfs.validate_default_shell("/bin/sh", remote)
        bzfs.validate_default_shell("/bin/bash", remote)
        with self.assertRaises(SystemExit):
            bzfs.validate_default_shell("/bin/csh", remote)
        with self.assertRaises(SystemExit):
            bzfs.validate_default_shell("/bin/tcsh", remote)

    def test_custom_ssh_config_file_must_match_file_name_pattern(self) -> None:
        args = self.argparser_parse_args(["src", "dst", "--ssh-src-config-file", "bad_file_name.cfg"])
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
        args = self.argparser_parse_args(args=["src", "dst"])
        log_params = bzfs.LogParams(args)
        try:
            job = bzfs.Job()
            job.params = bzfs.Params(args, log_params=log_params, log=bzfs_main.loggers.get_logger(log_params, args))
            job.params.available_programs = {"src": {"pv": "pv"}}
            self.assertNotEqual("cat", job.pv_cmd("src", 1024 * 1024, "foo"))
        finally:
            bzfs_main.loggers.reset_logger()

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
        params = bzfs.Params(self.argparser_parse_args(args=["src", "dst"]))
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

        params = bzfs.Params(self.argparser_parse_args(args=["src", "dst"]))
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
            [(100 + 2, bzfs_main.utils.unixtime_infinity_secs)],
            [(alert.latest.warning_millis, alert.latest.critical_millis) for alert in config.alerts],  # type: ignore
        )
        self.assertListEqual(["z_onsite__100millisecondly"], [str(alert.label) for alert in config.alerts])

        args = bzfs.argument_parser().parse_args(
            ["src", "dst", "--monitor-snapshots=" + plan({"latest": {"critical": "2 millis"}})]
        )
        config = bzfs.MonitorSnapshotsConfig(args, params)
        self.assertTrue(str(config))
        self.assertListEqual(
            [(bzfs_main.utils.unixtime_infinity_secs, 100 + 2)],
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
        job.params = bzfs.Params(self.argparser_parse_args(args=["src", "dst"]))
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
            bzfs_main.utils.die("boom", parser=parser)


#############################################################################
class TestAdditionalHelpers(AbstractTestCase):

    def test_params_verbose_zfs_and_bwlimit(self) -> None:
        args = self.argparser_parse_args(["src", "dst", "-v", "-v", "--bwlimit", "20m"])
        params = bzfs.Params(args)
        self.assertIn("-v", params.zfs_send_program_opts)
        self.assertIn("-v", params.zfs_recv_program_opts)
        self.assertIn("--rate-limit=20m", params.pv_program_opts)

    def test_program_name_injections(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        p1 = bzfs.Params(args, inject_params={"inject_unavailable_ssh": True})
        self.assertEqual("ssh-xxx", p1.program_name("ssh"))
        p2 = bzfs.Params(args, inject_params={"inject_failing_ssh": True})
        self.assertEqual("false", p2.program_name("ssh"))

    def test_unset_matching_env_vars(self) -> None:
        with patch.dict(os.environ, {"FOO_BAR": "x"}):
            args = self.argparser_parse_args(["src", "dst", "--exclude-envvar-regex", "FOO.*"])
            params = bzfs.Params(args, log_params=bzfs.LogParams(args), log=logging.getLogger())
            params.unset_matching_env_vars(args)
            self.assertNotIn("FOO_BAR", os.environ)

    def test_local_ssh_command_variants(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = os.path.join(tmpdir, "bzfs_ssh_config")
            Path(cfg).touch()
            args = self.argparser_parse_args(["src", "dst", "--ssh-src-config-file", cfg])
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

            args = self.argparser_parse_args(["src", "dst", "--ssh-program", "-"])
            p = bzfs.Params(args)
            r = bzfs.Remote("src", args, p)
            r.ssh_user_host = "u@h"
            with self.assertRaises(SystemExit):
                r.local_ssh_command()  # Cannot talk to remote host because ssh CLI is disabled
            r.ssh_user_host = ""
            self.assertEqual([], r.local_ssh_command())

    def test_params_zfs_recv_program_opt(self) -> None:
        args = self.argparser_parse_args(
            ["src", "dst", "--zfs-recv-program-opt=-o", "--zfs-recv-program-opt=org.test=value"]
        )
        params = bzfs.Params(args)
        self.assertIn("-o", params.zfs_recv_program_opts)
        self.assertIn("org.test=value", params.zfs_recv_program_opts)

    def test_copy_properties_config_repr(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        params = bzfs.Params(args)
        conf = bzfs.CopyPropertiesConfig("zfs_recv_o", "-o", args, params)
        rep = repr(conf)
        self.assertIn("sources", rep)
        self.assertIn("targets", rep)

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

    @patch("bzfs_main.bzfs.Job.run_main")
    def test_run_main_delegates_to_job(self, mock_run_main: MagicMock) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        bzfs.run_main(args=args, sys_argv=["p"])
        mock_run_main.assert_called_once_with(args, ["p"], None)

    def test_pv_program_opts_disallows_dangerous_options(self) -> None:
        """Confirms that initialization fails if --pv-program-opts contains the forbidden -f or --log-file options."""
        # Test Case 1: The short-form option '-f' should be rejected.
        malicious_opts_short = "--bytes -f /etc/hosts"
        args_short = self.argparser_parse_args(["src", "dst", f"--pv-program-opts={malicious_opts_short}"])
        with self.assertRaises(SystemExit):
            bzfs.Params(args_short)

        # Test Case 2: The long-form option '--log-file' should be rejected.
        malicious_opts_long = "--progress --log-file /etc/shadow"
        args_long = self.argparser_parse_args(["src", "dst", f"--pv-program-opts={malicious_opts_long}"])
        with self.assertRaises(SystemExit):
            bzfs.Params(args_long)

        # Test Case 3: A valid set of options should instantiate successfully.
        valid_opts = "--bytes --progress --rate"
        args_valid = self.argparser_parse_args(["src", "dst", f"--pv-program-opts={valid_opts}"])

        # This should not raise an exception.
        params_valid = bzfs.Params(args_valid)
        self.assertIn("--rate", params_valid.pv_program_opts)

    def test_compression_program_opts_disallows_dangerous_options(self) -> None:
        malicious_opts_short = "-o /etc/hosts"
        args_short = self.argparser_parse_args(["src", "dst", f"--compression-program-opts={malicious_opts_short}"])
        with self.assertRaises(SystemExit):
            bzfs.Params(args_short)

        malicious_opts_long = "--output-file /etc/hosts"
        args_long = self.argparser_parse_args(["src", "dst", f"--compression-program-opts={malicious_opts_long}"])
        with self.assertRaises(SystemExit):
            bzfs.Params(args_long)

        valid_opts = "-9"
        args_valid = self.argparser_parse_args(["src", "dst", f"--compression-program-opts={valid_opts}"])
        params_valid = bzfs.Params(args_valid)
        self.assertIn("-9", params_valid.compression_program_opts)

    def test_mbuffer_program_opts_disallows_dangerous_options(self) -> None:
        malicious_opts = "-o /etc/hosts"
        args_short = self.argparser_parse_args(["src", "dst", f"--mbuffer-program-opts={malicious_opts}"])
        with self.assertRaises(SystemExit):
            bzfs.Params(args_short)

        valid_opts = "-q"
        args_valid = self.argparser_parse_args(["src", "dst", f"--mbuffer-program-opts={valid_opts}"])
        params_valid = bzfs.Params(args_valid)
        self.assertIn("-q", params_valid.mbuffer_program_opts)


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
class TestCurrentDateTime(AbstractTestCase):

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
class TestArgumentParser(AbstractTestCase):

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
class TestAddRecvPropertyOptions(AbstractTestCase):

    def setUp(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        self.p = bzfs.Params(args)
        self.p.src = bzfs.Remote("src", args, self.p)
        self.p.dst = bzfs.Remote("dst", args, self.p)
        self.p.zfs_recv_x_names = ["xprop1", "xprop2"]
        self.p.zfs_recv_ox_names = {"existing"}
        self.job = bzfs.Job()
        self.job.params = self.p

    def test_appends_x_options_when_supported(self) -> None:
        recv_opts: list[str] = []
        with patch.object(self.p, "is_program_available", return_value=True):
            result_opts, set_opts = self.job.add_recv_property_options(True, recv_opts, "ds", {})
        self.assertEqual(["-x", "xprop1", "-x", "xprop2"], result_opts)
        self.assertEqual([], set_opts)
        # original zfs_recv_ox_names remains unchanged
        self.assertEqual({"existing"}, self.p.zfs_recv_ox_names)

    def test_skips_x_options_when_not_supported(self) -> None:
        recv_opts: list[str] = []
        with patch.object(self.p, "is_program_available", return_value=False):
            result_opts, set_opts = self.job.add_recv_property_options(True, recv_opts, "ds", {})
        self.assertEqual([], result_opts)
        self.assertEqual([], set_opts)
        self.assertEqual({"existing"}, self.p.zfs_recv_ox_names)


#############################################################################
class TestDatasetPairsAction(AbstractTestCase):

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
        log_params = bzfs.LogParams(self.args)
        self.p = bzfs.Params(self.args, log_params=log_params, log=bzfs_main.loggers.get_logger(log_params, self.args))
        self.job = bzfs.Job()
        self.job.params = self.p

        # Setup minimal remote objects. The specific details don't matter much as they are mocked.
        self.p.src = bzfs.Remote("src", self.args, self.p)
        self.p.dst = bzfs.Remote("dst", self.args, self.p)
        self.p.src.ssh_user_host = "src_host"
        self.p.dst.ssh_user_host = "dst_host"
        self.p.connection_pools = {
            "local": MagicMock(spec=ConnectionPools),
            "src": MagicMock(spec=ConnectionPools),
            "dst": MagicMock(spec=ConnectionPools),
        }
        self.p.zpool_features = {"src": {}, "dst": {}}
        self.p.available_programs = {"local": {"ssh": ""}, "src": {}, "dst": {}}

    def tearDown(self) -> None:
        bzfs_main.loggers.reset_logger()

    @patch.object(bzfs_main.detect, "detect_zpool_features")
    @patch.object(bzfs_main.detect, "detect_available_programs_remote")
    def test_preserve_properties_fails_on_old_zfs_with_props(
        self, mock_detect_zpool_features: MagicMock, mock_detect_available_programs_remote: MagicMock
    ) -> None:
        """Verify that using --preserve-properties with --props fails on ZFS < 2.2.0."""
        with patch.object(self.p, "is_program_available") as mock_is_available:
            # Make the mock specific to the zfs>=2.2.0 check on dst
            def side_effect(program: str, location: str) -> bool:
                if program == zfs_version_is_at_least_2_2_0 and location == "dst":
                    return False
                return True  # Assume other programs are available

            mock_is_available.side_effect = side_effect
            with self.assertRaises(SystemExit) as cm:
                detect_available_programs(self.job)
            self.assertEqual(cm.exception.code, die_status)
            self.assertIn("--preserve-properties is unreliable on destination ZFS < 2.2.0", str(cm.exception))

    @patch.object(bzfs_main.detect, "detect_zpool_features")
    @patch.object(bzfs_main.detect, "detect_available_programs_remote")
    def test_preserve_properties_succeeds_on_new_zfs_with_props(
        self, mock_detect_zpool_features: MagicMock, mock_detect_available_programs_remote: MagicMock
    ) -> None:
        """Verify that --preserve-properties is allowed on ZFS >= 2.2.0."""
        with patch.object(self.p, "is_program_available", return_value=True):
            # This should not raise an exception
            detect_available_programs(self.job)

    @patch.object(bzfs_main.detect, "detect_zpool_features")
    @patch.object(bzfs_main.detect, "detect_available_programs_remote")
    def test_preserve_properties_succeeds_on_old_zfs_without_props(
        self, mock_detect_zpool_features: MagicMock, mock_detect_available_programs_remote: MagicMock
    ) -> None:
        """Verify --preserve-properties is allowed on old ZFS if --props is not used."""
        self.p.zfs_send_program_opts.remove("--props")  # Modify the params for this test case
        with patch.object(self.p, "is_program_available") as mock_is_available:

            def side_effect(program: str, location: str) -> bool:
                # Simulate old ZFS by returning False for version checks, but True for other programs like ssh.
                if program == zfs_version_is_at_least_2_2_0:
                    return False
                return True

            mock_is_available.side_effect = side_effect
            # This should not raise an exception
            detect_available_programs(self.job)


#############################################################################
class TestFileOrLiteralAction(AbstractTestCase):

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
class TestNewSnapshotFilterGroupAction(AbstractTestCase):

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
class TestNonEmptyStringAction(AbstractTestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--name", action=bzfs.NonEmptyStringAction)

    def test_non_empty_string_action_empty(self) -> None:
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["--name", " "])


#############################################################################
class TestLogConfigVariablesAction(AbstractTestCase):

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
class SSHConfigFileNameAction(AbstractTestCase):

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
class TestSafeFileNameAction(AbstractTestCase):

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
class TestSafeDirectoryNameAction(AbstractTestCase):
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
class TestCheckRange(AbstractTestCase):

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
class TestCheckPercentRange(AbstractTestCase):

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
class TestPythonVersionCheck(AbstractTestCase):
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
            mock_exit.assert_called_with(die_status)

    @patch("sys.exit")
    @patch("sys.version_info", new=(3, 8))
    def test_version_3_8_or_higher(self, mock_exit: MagicMock) -> None:
        import importlib

        from bzfs_main import bzfs

        importlib.reload(bzfs)  # Reload module to apply version patch
        mock_exit.assert_not_called()


#############################################################################
class TestPerformance(AbstractTestCase):

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
