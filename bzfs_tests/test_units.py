#!/usr/bin/env python3
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
import itertools
import logging
import os
import platform
import random
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from collections import defaultdict, Counter
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Sequence, Callable, Optional, TypeVar, Union, Dict
from unittest.mock import patch, mock_open

from bzfs import bzfs
from bzfs.bzfs import getenv_any
from bzfs_tests.zfs_util import is_solaris_zfs

test_mode = getenv_any("test_mode", "")  # Consider toggling this when testing isolated code changes
is_adhoc_test = test_mode == "adhoc"  # run only a few isolated changes
is_functional_test = test_mode == "functional"  # run most tests but only in a single local config combination


def suite():
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestHelperFunctions))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestParseDatasetLocator))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestReplaceCapturingGroups))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestFindMatch))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestBuildTree))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSynchronizedBool))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSynchronizedDict))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestArgumentParser))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestDatasetPairsAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestFileOrLiteralAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestTimeRangeAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestLogConfigVariablesAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSafeFileNameAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCheckRange))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCheckPercentRange))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestPythonVersionCheck))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRankRangeAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestConnectionPool))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestIncrementalSendSteps))
    # suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestPerformance))
    return suite


def argparser_parse_args(args):
    return bzfs.argument_parser().parse_args(
        args + ["--log-dir", os.path.join(bzfs.get_home_directory(), "bzfs-logs-test")]
    )


#############################################################################
class TestHelperFunctions(unittest.TestCase):
    s = "s"
    d = "d"
    a = "a"

    def merge_sorted_iterators(self, src, dst, choice):
        s, d, a = self.s, self.d, self.a
        return [item for item in bzfs.Job().merge_sorted_iterators([s, d, a], choice, iter(src), iter(dst))]

    def assert_merge_sorted_iterators(self, expected, src, dst, choice="+".join([s, d, a]), invert=True):
        s, d, a = self.s, self.d, self.a
        self.assertListEqual(expected, self.merge_sorted_iterators(src, dst, choice))
        if invert:
            inverted = [(s if item[0] == d else d if item[0] == s else a,) + item[1:] for item in expected]
            self.assertListEqual(inverted, self.merge_sorted_iterators(dst, src, choice))

    def test_merge_sorted_iterators(self):
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

    def test_append_if_absent(self):
        self.assertListEqual([], bzfs.append_if_absent([]))
        self.assertListEqual(["a"], bzfs.append_if_absent([], "a"))
        self.assertListEqual(["a"], bzfs.append_if_absent([], "a", "a"))

    def test_cut(self):
        lines = ["34\td1@s1", "56\td2@s2"]
        self.assertListEqual(["34", "56"], bzfs.cut(1, lines=lines))
        self.assertListEqual(["d1@s1", "d2@s2"], bzfs.cut(2, lines=lines))
        self.assertListEqual([], bzfs.cut(1, lines=[]))
        self.assertListEqual([], bzfs.cut(2, lines=[]))
        with self.assertRaises(ValueError):
            bzfs.cut(0, lines=lines)

    def test_get_home_directory(self):
        old_home = os.environ.get("HOME")
        if old_home is not None:
            self.assertEqual(old_home, bzfs.get_home_directory())
            os.environ.pop("HOME")
            try:
                self.assertEqual(old_home, bzfs.get_home_directory())
            finally:
                os.environ["HOME"] = old_home

    def test_tail(self):
        fd, file = tempfile.mkstemp(prefix="test_bzfs.tail_")
        os.write(fd, "line1\nline2\n".encode())
        os.close(fd)
        self.assertEqual(["line1\n", "line2\n"], list(bzfs.tail(file, n=10)))
        self.assertEqual(["line1\n", "line2\n"], list(bzfs.tail(file, n=2)))
        self.assertEqual(["line2\n"], list(bzfs.tail(file, n=1)))
        self.assertEqual([], list(bzfs.tail(file, n=0)))
        os.remove(file)
        self.assertEqual([], list(bzfs.tail(file, n=2)))

    def test_validate_port(self):
        bzfs.validate_port(47, "msg")
        bzfs.validate_port("47", "msg")
        bzfs.validate_port(0, "msg")
        bzfs.validate_port("", "msg")
        with self.assertRaises(SystemExit):
            bzfs.validate_port("xxx47", "msg")

    def test_validate_quoting(self):
        params = bzfs.Params(argparser_parse_args(args=["src", "dst"]))
        params.validate_quoting([""])
        params.validate_quoting(["foo"])
        with self.assertRaises(SystemExit):
            params.validate_quoting(['foo"'])
        with self.assertRaises(SystemExit):
            params.validate_quoting(["foo'"])
        with self.assertRaises(SystemExit):
            params.validate_quoting(["foo`"])

    def test_validate_arg(self):
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

    def test_validate_program_name_must_not_be_empty(self):
        args = argparser_parse_args(args=["src", "dst"])
        setattr(args, "zfs_program", "")
        with self.assertRaises(SystemExit):
            bzfs.Params(args)

    def test_split_args(self):
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
        self.assertEqual(["'foo'"], params.split_args("'foo'", allow_all=True))
        with self.assertRaises(SystemExit):
            params.split_args('"foo"')
        self.assertEqual(['"foo"'], params.split_args('"foo"', allow_all=True))
        self.assertEqual(["foo", "bar baz"], params.split_args("foo", "bar baz"))
        self.assertEqual(["foo", "bar\tbaz"], params.split_args("foo", "bar\tbaz"))
        self.assertEqual(["foo", "bar\nbaz"], params.split_args("foo", "bar\nbaz"))
        self.assertEqual(["foo", "bar\rbaz"], params.split_args("foo", "bar\rbaz"))

    def test_fix_send_recv_opts(self):
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

    def test_xprint(self):
        log = logging.getLogger()
        bzfs.xprint(log, "foo")
        bzfs.xprint(log, "foo", run=True)
        bzfs.xprint(log, "foo", run=False)
        bzfs.xprint(log, "foo", file=sys.stdout)
        bzfs.xprint(log, "")
        bzfs.xprint(log, "", run=True)
        bzfs.xprint(log, "", run=False)
        bzfs.xprint(log, None)

    def test_delete_stale_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            new_socket_file = os.path.join(tmpdir, "s_new_socket_file")
            Path(new_socket_file).touch()
            stale_socket_file = os.path.join(tmpdir, "s_stale_socket_file")
            Path(stale_socket_file).touch()
            one_hundred_days_ago = time.time() - 100 * 24 * 60 * 60
            os.utime(stale_socket_file, (one_hundred_days_ago, one_hundred_days_ago))
            dir = os.path.join(tmpdir, "s_dir")
            os.mkdir(dir)
            non_socket_file = os.path.join(tmpdir, "f")
            Path(non_socket_file).touch()

            bzfs.delete_stale_files(tmpdir, "s")

            self.assertTrue(os.path.exists(new_socket_file))
            self.assertFalse(os.path.exists(stale_socket_file))
            self.assertTrue(os.path.exists(dir))
            self.assertTrue(os.path.exists(non_socket_file))

    def test_recv_option_property_names(self):
        def names(lst):
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

    def test_fix_solaris_raw_mode(self):
        self.assertListEqual(["-w", "none"], bzfs.fix_solaris_raw_mode(["-w"]))
        self.assertListEqual(["-w", "none"], bzfs.fix_solaris_raw_mode(["--raw"]))
        self.assertListEqual(["-w", "none"], bzfs.fix_solaris_raw_mode(["-w", "none"]))
        self.assertListEqual(["-w", "compress"], bzfs.fix_solaris_raw_mode(["-w", "compress"]))
        self.assertListEqual(["-w", "compress"], bzfs.fix_solaris_raw_mode(["-w", "--compressed"]))
        self.assertListEqual(["-w", "none", "foo"], bzfs.fix_solaris_raw_mode(["-w", "foo"]))
        self.assertListEqual(["-F"], bzfs.fix_solaris_raw_mode(["-F"]))

    def test_get_logger_with_cleanup(self):
        def check(log, files):
            files_todo = files.copy()
            for handler in log.handlers:
                if isinstance(handler, logging.FileHandler):
                    self.assertIn(handler.baseFilename, files_todo)
                    files_todo.remove(handler.baseFilename)
            self.assertEqual(0, len(files_todo))

        prefix = "test_get_logger:"
        args = argparser_parse_args(args=["src", "dst"])
        root_logger = logging.getLogger()
        log_params = None
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
        log.trace("%s", prefix + "bbb5")
        log.setLevel(bzfs.log_trace)
        log.trace("%s", prefix + "bbb6")
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

    def test_get_syslog_address(self):
        udp = socket.SOCK_DGRAM
        tcp = socket.SOCK_STREAM
        self.assertEqual((("localhost", 514), udp), bzfs.get_syslog_address("localhost:514", "UDP"))
        self.assertEqual((("localhost", 514), tcp), bzfs.get_syslog_address("localhost:514", "TCP"))
        self.assertEqual(("/dev/log", None), bzfs.get_syslog_address("/dev/log", "UDP"))
        self.assertEqual(("/dev/log", None), bzfs.get_syslog_address("/dev/log", "TCP"))

    def test_validate_log_config_variable(self):
        self.assertIsNone(bzfs.validate_log_config_variable("name:value"))
        for var in ["noColon", ":noName", "$n:v", "{:v", "}:v", "", "  ", "\t", "a\tb:v", "spa ce:v", '"k":v', "'k':v"]:
            self.assertIsNotNone(bzfs.validate_log_config_variable(var))

    def test_unlink_missing_ok(self):
        tmp_file_fd, tmp_file = tempfile.mkstemp(prefix="test_bzfs.tmpfile_")
        os.close(tmp_file_fd)
        self.assertTrue(os.path.exists(tmp_file))
        bzfs.unlink_missing_ok(tmp_file)
        self.assertFalse(os.path.exists(tmp_file))
        bzfs.unlink_missing_ok(tmp_file)
        self.assertFalse(os.path.exists(tmp_file))

    def test_is_zfs_dataset_busy_match(self):
        def is_busy(proc, dataset, busy_if_send: bool = True):
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

    def assert_human_readable_float(self, actual, expected):
        self.assertEqual(bzfs.human_readable_float(actual), expected)
        self.assertEqual(bzfs.human_readable_float(-actual), "-" + expected)

    def test_human_readable_float_with_one_digit_before_decimal(self):
        self.assert_human_readable_float(3.14159, "3.14")
        self.assert_human_readable_float(5.0, "5")
        self.assert_human_readable_float(0.5, "0.5")
        self.assert_human_readable_float(0.499999, "0.5")
        self.assert_human_readable_float(3.1477, "3.15")
        self.assert_human_readable_float(1.999999, "2")
        self.assert_human_readable_float(2.5, "2.5")
        self.assert_human_readable_float(3.5, "3.5")

    def test_human_readable_float_with_two_digits_before_decimal(self):
        self.assert_human_readable_float(12.34, "12.3")
        self.assert_human_readable_float(12.0, "12")
        self.assert_human_readable_float(12.54, "12.5")
        self.assert_human_readable_float(12.56, "12.6")

    def test_human_readable_float_with_three_or_more_digits_before_decimal(self):
        self.assert_human_readable_float(123.456, "123")
        self.assert_human_readable_float(123.516, "124")
        self.assert_human_readable_float(1234.4678, "1234")
        self.assert_human_readable_float(1234.5678, "1235")
        self.assert_human_readable_float(12345.078, "12345")
        self.assert_human_readable_float(12345.678, "12346")
        self.assert_human_readable_float(999.99, "1000")

    def test_human_readable_float_with_zero(self):
        self.assertEqual(bzfs.human_readable_float(0.0), "0")
        self.assertEqual(bzfs.human_readable_float(-0.0), "0")
        self.assertEqual(bzfs.human_readable_float(0.001), "0")
        self.assertEqual(bzfs.human_readable_float(-0.001), "0")

    def test_human_readable_float_with_halfway_rounding_behavior(self):
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

    def test_human_readable_duration(self):
        ms = 1000_000
        self.assertEqual("0 ns", bzfs.human_readable_duration(0, long=False))
        self.assertEqual("3 ns", bzfs.human_readable_duration(3, long=False))
        self.assertEqual("3 μs", bzfs.human_readable_duration(3 * 1000, long=False))
        self.assertEqual("3 ms", bzfs.human_readable_duration(3 * ms, long=False))
        self.assertEqual("1 s", bzfs.human_readable_duration(1000 * ms, long=False))
        self.assertEqual("3 s", bzfs.human_readable_duration(3000 * ms, long=False))
        self.assertEqual("3 m", bzfs.human_readable_duration(3000 * 60 * ms, long=False))
        self.assertEqual("3 h", bzfs.human_readable_duration(3000 * 60 * 60 * ms, long=False))
        self.assertEqual("1.25 d", bzfs.human_readable_duration(3000 * 60 * 60 * 10 * ms, long=False))
        self.assertEqual("12.5 d", bzfs.human_readable_duration(3000 * 60 * 60 * 100 * ms, long=False))
        self.assertEqual("125 ns", bzfs.human_readable_duration(125, long=False))
        self.assertEqual("125 μs", bzfs.human_readable_duration(125 * 1000, long=False))
        self.assertEqual("125 ms", bzfs.human_readable_duration(125 * 1000 * 1000, long=False))
        self.assertEqual("2.08 m", bzfs.human_readable_duration(125 * 1000 * 1000 * 1000, long=False))

        self.assertEqual("0 s", bzfs.human_readable_duration(0, unit="s", long=False))
        self.assertEqual("3 s", bzfs.human_readable_duration(3, unit="s", long=False))
        self.assertEqual("3 m", bzfs.human_readable_duration(3 * 60, unit="s", long=False))
        self.assertEqual("0 h", bzfs.human_readable_duration(0, unit="h", long=False))
        self.assertEqual("3 h", bzfs.human_readable_duration(3, unit="h", long=False))
        self.assertEqual("7.5 d", bzfs.human_readable_duration(3 * 60, unit="h", long=False))
        with self.assertRaises(ValueError):
            bzfs.human_readable_duration(3, unit="hhh", long=False)  # invalid unit

        self.assertEqual("0 s (0 seconds)", bzfs.human_readable_duration(0, unit="s", long=True))
        self.assertEqual("3 m (180 seconds)", bzfs.human_readable_duration(3 * 60, unit="s", long=True))
        self.assertEqual("3 h (10800 seconds)", bzfs.human_readable_duration(3 * 60, unit="m", long=True))
        self.assertEqual("3 ms (0 seconds)", bzfs.human_readable_duration(3, unit="ms", long=True))
        self.assertEqual("3 ms (0 seconds)", bzfs.human_readable_duration(3 * 1000 * 1000, long=True))

        self.assertEqual("0 s (0 seconds)", bzfs.human_readable_duration(-0, unit="s", long=True))
        self.assertEqual("-3 m (-180 seconds)", bzfs.human_readable_duration(-3 * 60, unit="s", long=True))
        self.assertEqual("-3 h (-10800 seconds)", bzfs.human_readable_duration(-3 * 60, unit="m", long=True))
        self.assertEqual("-3 ms (0 seconds)", bzfs.human_readable_duration(-3, unit="ms", long=True))
        self.assertEqual("-3 ms (0 seconds)", bzfs.human_readable_duration(-3 * 1000 * 1000, long=True))

    def test_pv_cmd(self):
        args = argparser_parse_args(args=["src", "dst"])
        log_params = bzfs.LogParams(args)
        try:
            job = bzfs.Job()
            job.params = bzfs.Params(args, log_params=log_params, log=bzfs.get_logger(log_params, args))
            job.params.available_programs = {"src": {"pv": "pv"}}
            self.assertNotEqual("cat", job.pv_cmd("src", 1024 * 1024, "foo"))
        finally:
            bzfs.reset_logger()

    def test_pv_size_to_bytes(self):
        self.assertEqual(800, bzfs.pv_size_to_bytes("800B foo"))
        self.assertEqual(1, bzfs.pv_size_to_bytes("8b"))
        self.assertEqual(round(4.12 * 1024), bzfs.pv_size_to_bytes("4.12 KiB"))
        self.assertEqual(round(46.2 * 1024**3), bzfs.pv_size_to_bytes("46,2GiB"))
        self.assertEqual(round(46.2 * 1024**3), bzfs.pv_size_to_bytes("46.2GiB"))
        self.assertEqual(round(46.2 * 1024**3), bzfs.pv_size_to_bytes("46" + bzfs.arabic_decimal_separator + "2GiB"))
        self.assertEqual(2 * 1024**2, bzfs.pv_size_to_bytes("2 MiB"))
        self.assertEqual(1000**2, bzfs.pv_size_to_bytes("1 MB"))
        self.assertEqual(1024**3, bzfs.pv_size_to_bytes("1 GiB"))
        self.assertEqual(1024**3, bzfs.pv_size_to_bytes("8 Gib"))
        self.assertEqual(1000**3, bzfs.pv_size_to_bytes("8 Gb"))
        self.assertEqual(1024**4, bzfs.pv_size_to_bytes("1 TiB"))
        self.assertEqual(1024**5, bzfs.pv_size_to_bytes("1 EiB"))
        self.assertEqual(1024**6, bzfs.pv_size_to_bytes("1 ZiB"))
        self.assertEqual(1024**7, bzfs.pv_size_to_bytes("1 YiB"))
        with self.assertRaises(ValueError):
            bzfs.pv_size_to_bytes("foo")
        with self.assertRaises(ValueError):
            bzfs.pv_size_to_bytes("46-2GiB")

    def test_count_num_bytes_transferred_by_zfs_send(self):
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
                    num_bytes, tails = bzfs.count_num_bytes_transferred_by_zfs_send(pv1, 10)
                    self.assertEqual(800 + 300 * 1000 + 600000 * 1000, num_bytes)
                finally:
                    os.remove(pv1)
                    os.remove(pv2)
                    shutil.rmtree(pv3)


#############################################################################
class TestParseDatasetLocator(unittest.TestCase):
    def run_test(self, input, expected_user, expected_host, expected_dataset, expected_user_host, expected_error):
        expected_status = 0 if not expected_error else 3
        passed = False

        # Run without validation
        user, host, user_host, pool, dataset = bzfs.parse_dataset_locator(input, validate=False)

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
            bzfs.parse_dataset_locator(input, validate=True)
        except (ValueError, SystemExit):
            status = 3

        if status != expected_status or (not passed):
            if status != expected_status:
                print("Validation Test failed:")
            else:
                print("Test failed:")
            print(
                f"input: {input}\nuser exp: '{expected_user}' vs '{user}'\nhost exp: '{expected_host}' vs '{host}'\nuserhost exp: '{expected_user_host}' vs '{user_host}'\ndataset exp: '{expected_dataset}' vs '{dataset}'"
            )
            self.fail()

    def test_basic(self):
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
        self.run_test(
            "host.example.com:tank1/foo/bar", "", "host.example.com", "tank1/foo/bar", "host.example.com", False
        )
        self.run_test(
            "root@host.example.com:tank1", "root", "host.example.com", "tank1", "root@host.example.com", False
        )
        self.run_test("192.168.1.1:tank1/foo/bar", "", "192.168.1.1", "tank1/foo/bar", "192.168.1.1", False)
        self.run_test(
            "user@192.168.1.1:tank1/foo/bar", "user", "192.168.1.1", "tank1/foo/bar", "user@192.168.1.1", False
        )
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
        self.run_test(
            "user@host.ex.com:tank1/foo@bar", "user", "host.ex.com", "tank1/foo@bar", "user@host.ex.com", True
        )
        self.run_test(
            "user@host.ex.com:tank1/foo#bar", "user", "host.ex.com", "tank1/foo#bar", "user@host.ex.com", True
        )
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
        self.run_test(
            "user@host:tank1/foo/whitespace\tbar", "user", "host", "tank1/foo/whitespace\tbar", "user@host", True
        )
        self.run_test(
            "user@host:tank1/foo/whitespace\nbar", "user", "host", "tank1/foo/whitespace\nbar", "user@host", True
        )
        self.run_test(
            "user@host:tank1/foo/whitespace\rbar", "user", "host", "tank1/foo/whitespace\rbar", "user@host", True
        )
        self.run_test("user@host:tank1/foo/space bar", "user", "host", "tank1/foo/space bar", "user@host", False)
        self.run_test("user@||1:tank1/foo/space bar", "user", "::1", "tank1/foo/space bar", "user@::1", False)
        self.run_test("user@::1:tank1/foo/space bar", "", "user@", ":1:tank1/foo/space bar", "user@", True)


#############################################################################
class TestReplaceCapturingGroups(unittest.TestCase):
    def replace_capturing_group(self, regex):
        return bzfs.replace_capturing_groups_with_non_capturing_groups(regex)

    def test_basic_case(self):
        self.assertEqual(self.replace_capturing_group("(abc)"), "(?:abc)")

    def test_nested_groups(self):
        self.assertEqual(self.replace_capturing_group("(a(bc)d)"), "(?:a(?:bc)d)")

    def test_preceding_backslash(self):
        self.assertEqual(self.replace_capturing_group("\\(abc)"), "\\(abc)")

    def test_group_starting_with_question_mark(self):
        self.assertEqual(self.replace_capturing_group("(?abc)"), "(?abc)")

    def test_multiple_groups(self):
        self.assertEqual(self.replace_capturing_group("(abc)(def)"), "(?:abc)(?:def)")

    def test_mixed_cases(self):
        self.assertEqual(self.replace_capturing_group("a(bc\\(de)f(gh)?i"), "a(?:bc\\(de)f(?:gh)?i")

    def test_empty_group(self):
        self.assertEqual(self.replace_capturing_group("()"), "(?:)")

    def test_group_with_named_group(self):
        self.assertEqual(self.replace_capturing_group("(?P<name>abc)"), "(?P<name>abc)")

    def test_group_with_non_capturing_group(self):
        self.assertEqual(self.replace_capturing_group("(a(?:bc)d)"), "(?:a(?:bc)d)")

    def test_group_with_lookahead(self):
        self.assertEqual(self.replace_capturing_group("(abc)(?=def)"), "(?:abc)(?=def)")

    def test_group_with_lookbehind(self):
        self.assertEqual(self.replace_capturing_group("(?<=abc)(def)"), "(?<=abc)(?:def)")

    def test_escaped_characters(self):
        pattern = re.escape("(abc)")
        self.assertEqual(self.replace_capturing_group(pattern), pattern)

    def test_complex_pattern_with_escape(self):
        complex_pattern = re.escape("(a[b]c{d}e|f.g)")
        self.assertEqual(self.replace_capturing_group(complex_pattern), complex_pattern)

    def test_complex_pattern(self):
        complex_pattern = "(a[b]c{d}e|f.g)(h(i|j)k)?(\\(l\\))"
        expected_result = "(?:a[b]c{d}e|f.g)(?:h(?:i|j)k)?(?:\\(l\\))"
        self.assertEqual(self.replace_capturing_group(complex_pattern), expected_result)


#############################################################################
class TestBuildTree(unittest.TestCase):
    def assert_keys_sorted(self, tree: Dict[str, Optional[Dict]]):
        keys = list(tree.keys())
        self.assertEqual(keys, bzfs.isorted(keys), f"Keys are not sorted: {keys}")
        for value in tree.values():
            if isinstance(value, dict):
                self.assert_keys_sorted(value)

    def test_basic_tree(self):
        datasets = ["pool", "pool/dataset", "pool/dataset/sub", "pool/other", "pool/other/sub/child"]
        expected_tree = {"pool": {"dataset": {"sub": None}, "other": {"sub": {"child": None}}}}
        tree = bzfs.build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_empty_input(self):
        datasets = []
        expected_tree = {}
        tree = bzfs.build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)

    def test_single_root(self):
        datasets = ["pool"]
        expected_tree = {"pool": None}
        tree = bzfs.build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_single_branch(self):
        datasets = ["pool/dataset/sub/child"]
        expected_tree = {"pool": {"dataset": {"sub": {"child": None}}}}
        tree = bzfs.build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots(self):
        datasets = ["pool", "otherpool", "anotherpool"]
        expected_tree = {"anotherpool": None, "otherpool": None, "pool": None}
        tree = bzfs.build_dataset_tree(sorted(datasets))
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_large_dataset(self):
        datasets = [f"pool/dataset{i}" for i in range(100)]
        tree = bzfs.build_dataset_tree(sorted(datasets))
        self.assertEqual(len(tree["pool"]), 100)
        self.assert_keys_sorted(tree)

    def test_nested_structure(self):
        datasets = [
            "pool/parent",
            "pool/parent/child1",
            "pool/parent/child2",
            "pool/parent/child2/grandchild",
            "pool/parent/child3",
        ]
        expected_tree = {"pool": {"parent": {"child1": None, "child2": {"grandchild": None}, "child3": None}}}
        tree = bzfs.build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_no_children(self):
        datasets = ["pool", "otherpool"]
        expected_tree = {"otherpool": None, "pool": None}
        tree = bzfs.build_dataset_tree(sorted(datasets))
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_single_level(self):
        datasets = ["pool", "pool1", "pool2", "pool3"]
        expected_tree = {"pool": None, "pool1": None, "pool2": None, "pool3": None}
        tree = bzfs.build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots_with_hierarchy(self):
        datasets = ["pool", "pool1", "pool1/dataset1", "pool2", "pool2/dataset2", "pool2/dataset2/sub", "pool3"]
        expected_tree = {"pool": None, "pool1": {"dataset1": None}, "pool2": {"dataset2": {"sub": None}}, "pool3": None}
        tree = bzfs.build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots_flat(self):
        datasets = ["root1", "root2", "root3", "root4"]
        expected_tree = {"root1": None, "root2": None, "root3": None, "root4": None}
        tree = bzfs.build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)

    def test_multiple_roots_mixed_depth(self):
        datasets = ["a", "a/b", "a/b/c", "x", "x/y", "z", "z/1", "z/2", "z/2/3"]
        expected_tree = {"a": {"b": {"c": None}}, "x": {"y": None}, "z": {"1": None, "2": {"3": None}}}
        tree = bzfs.build_dataset_tree(datasets)
        self.assertEqual(tree, expected_tree)
        self.assert_keys_sorted(tree)


#############################################################################
class TestFindMatch(unittest.TestCase):

    def test_basic(self):
        condition = lambda arg: arg.startswith("-")

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

    def assert_find_match(self, expected, lst, condition, start=None, end=None, raises=False):
        self.assertEqual(expected, find_match(lst, condition, start=start, end=end, reverse=False, raises=raises))
        self.assertEqual(expected, find_match(lst, condition, start=start, end=end, reverse=True, raises=raises))


#############################################################################
class TestArgumentParser(unittest.TestCase):

    def test_help(self):
        if is_solaris_zfs():
            self.skipTest("FIXME: BlockingIOError: [Errno 11] write could not complete without blocking")
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["--help"])
        self.assertEqual(0, e.exception.code)

    def test_version(self):
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["--version"])
        self.assertEqual(0, e.exception.code)

    def test_missing_datasets(self):
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["--retries=1"])
        self.assertEqual(2, e.exception.code)

    def test_missing_dst_dataset(self):
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["src_dataset"])  # Each SRC_DATASET must have a corresponding DST_DATASET
        self.assertEqual(2, e.exception.code)

    def test_program_must_not_be_empty_string(self):
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["src_dataset", "src_dataset", "--zfs-program="])
        self.assertEqual(2, e.exception.code)


#############################################################################
class TestDatasetPairsAction(unittest.TestCase):

    def setUp(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--input", nargs="+", action=bzfs.DatasetPairsAction)

    def test_direct_value(self):
        args = self.parser.parse_args(["--input", "src1", "dst1"])
        self.assertEqual(args.input, [("src1", "dst1")])

    def test_direct_value_without_corresponding_dst(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["--input", "src1"])

    def test_file_input(self):
        with patch("builtins.open", mock_open(read_data="src1\tdst1\nsrc2\tdst2\n")):
            args = self.parser.parse_args(["--input", "+testfile"])
            self.assertEqual(args.input, [("src1", "dst1"), ("src2", "dst2")])

    def test_file_input_without_trailing_newline(self):
        with patch("builtins.open", mock_open(read_data="src1\tdst1\nsrc2\tdst2")):
            args = self.parser.parse_args(["--input", "+testfile"])
            self.assertEqual(args.input, [("src1", "dst1"), ("src2", "dst2")])

    def test_mixed_input(self):
        with patch("builtins.open", mock_open(read_data="src1\tdst1\nsrc2\tdst2\n")):
            args = self.parser.parse_args(["--input", "src0", "dst0", "+testfile"])
            self.assertEqual(args.input, [("src0", "dst0"), ("src1", "dst1"), ("src2", "dst2")])

    def test_file_skip_comments_and_empty_lines(self):
        with patch("builtins.open", mock_open(read_data="\n\n#comment\nsrc1\tdst1\nsrc2\tdst2\n")):
            args = self.parser.parse_args(["--input", "+testfile"])
            self.assertEqual(args.input, [("src1", "dst1"), ("src2", "dst2")])

    def test_file_skip_stripped_empty_lines(self):
        with patch("builtins.open", mock_open(read_data=" \t \nsrc1\tdst1")):
            args = self.parser.parse_args(["--input", "+testfile"])
            self.assertEqual(args.input, [("src1", "dst1")])

    def test_file_missing_tab(self):
        with patch("builtins.open", mock_open(read_data="src1\nsrc2")):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+testfile"])

    def test_file_whitespace_only(self):
        with patch("builtins.open", mock_open(read_data=" \tdst1")):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+testfile"])

        with patch("builtins.open", mock_open(read_data="src1\t ")):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+testfile"])

        with patch("builtins.open", side_effect=FileNotFoundError):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+nonexistentfile"])

    def test_option_not_specified(self):
        args = self.parser.parse_args([])
        self.assertIsNone(args.input)


#############################################################################
class TestFileOrLiteralAction(unittest.TestCase):

    def setUp(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--input", nargs="+", action=bzfs.FileOrLiteralAction)

    def test_direct_value(self):
        args = self.parser.parse_args(["--input", "literalvalue"])
        self.assertEqual(args.input, ["literalvalue"])

    def test_file_input(self):
        with patch("builtins.open", mock_open(read_data="line 1\nline 2  \n")):
            args = self.parser.parse_args(["--input", "+testfile"])
            self.assertEqual(args.input, ["line 1", "line 2  "])

    def test_mixed_input(self):
        with patch("builtins.open", mock_open(read_data="line 1\nline 2")):
            args = self.parser.parse_args(["--input", "literalvalue", "+testfile"])
            self.assertEqual(args.input, ["literalvalue", "line 1", "line 2"])

    def test_skip_comments_and_empty_lines(self):
        with patch("builtins.open", mock_open(read_data="\n\n#comment\nline 1\n\n\nline 2\n")):
            args = self.parser.parse_args(["--input", "+testfile"])
            self.assertEqual(args.input, ["line 1", "line 2"])

    def test_file_not_found(self):
        with patch("builtins.open", side_effect=FileNotFoundError):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--input", "+nonexistentfile"])

    def test_option_not_specified(self):
        args = self.parser.parse_args([])
        self.assertIsNone(args.input)


#############################################################################
class TestTimeRangeAction(unittest.TestCase):
    def setUp(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--time-n-ranks", action=bzfs.TimeRangeAndRankRangeAction, nargs="+")

    def parse_duration(self, arg):
        return self.parser.parse_args(["--time-n-ranks", arg + " ago..*"])

    def parse_timestamp(self, arg):
        return self.parser.parse_args(["--time-n-ranks", arg + "..*"])

    def test_parse_unix_time(self):  # Test Unix time in integer seconds
        args = self.parse_timestamp("1696510080")
        self.assertEqual(args.time_n_ranks[0][0], 1696510080)

        args = self.parse_timestamp("0")
        self.assertEqual(args.time_n_ranks[0][0], 0)

    def test_valid_durations(self):
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

    def test_invalid_time_spec(self):
        with self.assertRaises(SystemExit):
            self.parse_duration("")  # Empty string

        with self.assertRaises(SystemExit):
            self.parse_duration("  ")  # Empty string

        with self.assertRaises(SystemExit):
            self.parse_duration("10x")  # Invalid unit

        with self.assertRaises(SystemExit):
            self.parse_duration("10   weeks")  # spaces

        with self.assertRaises(SystemExit):
            self.parse_duration("-5mins")  # Negative number

        with self.assertRaises(SystemExit):
            self.parse_duration("abcd")  # Completely invalid format

        with self.assertRaises(SystemExit):
            self.parse_timestamp("2024-1-1")  # must be 2024-01-01

        with self.assertRaises(SystemExit):
            self.parse_timestamp("2024-10-35")  # Month does not have 35 days

        with self.assertRaises(SystemExit):
            self.parser.parse_args(["--time-n-ranks", "60_mins..*"])  # Missing 'ago'

        with self.assertRaises(SystemExit):
            self.parser.parse_args(["--time-n-ranks", "60_mins_ago"])  # Missing ..

    def test_parse_datetime(self):
        # Test ISO 8601 datetime strings without timezone
        args = self.parse_timestamp("2024-01-01")
        self.assertEqual(args.time_n_ranks[0][0], int(datetime.fromisoformat("2024-01-01").timestamp()))

        args = self.parse_timestamp("2024-12-31")
        self.assertEqual(args.time_n_ranks[0][0], int(datetime.fromisoformat("2024-12-31").timestamp()))

        args = self.parse_timestamp("2024-10-05T14:48:55")
        self.assertEqual(args.time_n_ranks[0][0], int(datetime.fromisoformat("2024-10-05T14:48:55").timestamp()))
        self.assertNotEqual(args.time_n_ranks[0][0], int(datetime.fromisoformat("2024-10-05T14:48:01").timestamp()))

    def test_parse_datetime_with_timezone(self):
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

    def test_get_include_snapshot_times(self):
        times_and_ranks_opt = "--include-snapshot-times-and-ranks="

        args = argparser_parse_args(args=["src", "dst"])
        p = bzfs.Params(args)
        self.assertListEqual([], p.snapshot_filters)

        args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "*..*"])
        p = bzfs.Params(args)
        self.assertListEqual([], p.snapshot_filters)

        args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "1700000000..1700000001"])
        p = bzfs.Params(args)
        self.assertEqual((1700000000, 1700000001), p.snapshot_filters[0].timerange)

        args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "1700000001..1700000000"])
        p = bzfs.Params(args)
        self.assertEqual((1700000000, 1700000001), p.snapshot_filters[0].timerange)

        args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "0secs ago..60secs ago"])
        p = bzfs.Params(args)
        self.assertAlmostEqual(int(time.time() - 60), p.snapshot_filters[0].timerange[0], delta=2)
        self.assertAlmostEqual(int(time.time()), p.snapshot_filters[0].timerange[1], delta=2)

        args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "2024-01-01..*"])
        p = bzfs.Params(args)
        self.assertEqual(int(datetime.fromisoformat("2024-01-01").timestamp()), p.snapshot_filters[0].timerange[0])
        self.assertLess(int(time.time() + 86400 * 365 * 1000), p.snapshot_filters[0].timerange[1])

        args = argparser_parse_args(args=["src", "dst", times_and_ranks_opt + "*..2024-01-01"])
        p = bzfs.Params(args)
        self.assertEqual(0, p.snapshot_filters[0].timerange[0])
        self.assertEqual(int(datetime.fromisoformat("2024-01-01").timestamp()), p.snapshot_filters[0].timerange[1])

    def test_filter_snapshots_by_times(self):
        lst1 = ["\t" + snapshot for snapshot in ["d@0", "d#1", "d@2", "d@3"]]
        self.assertListEqual(["\td#1"], self.filter_snapshots_by_times_and_rank1(lst1, "0..0"))
        self.assertListEqual(["\td@0", "\td#1"], self.filter_snapshots_by_times_and_rank1(lst1, "0..1"))
        self.assertListEqual(["\td@0", "\td#1"], self.filter_snapshots_by_times_and_rank1(lst1, "0..2"))
        self.assertListEqual(["\td@0", "\td#1", "\td@2"], self.filter_snapshots_by_times_and_rank1(lst1, "0..3"))
        self.assertListEqual(lst1, self.filter_snapshots_by_times_and_rank1(lst1, "0..4"))
        self.assertListEqual(lst1, self.filter_snapshots_by_times_and_rank1(lst1, "0..5"))
        self.assertListEqual(["\td#1", "\td@2"], self.filter_snapshots_by_times_and_rank1(lst1, "1..3"))

    @staticmethod
    def filter_snapshots_by_times_and_rank1(snapshots, timerange, ranks=[]):
        return filter_snapshots_by_times_and_rank(snapshots, timerange=timerange, ranks=ranks)


def filter_snapshots_by_times_and_rank(snapshots, timerange, ranks=[]):
    args = argparser_parse_args(
        args=["src", "dst", "--include-snapshot-times-and-ranks", timerange, *ranks, "--verbose"]
    )
    log_params = bzfs.LogParams(args)
    try:
        job = bzfs.Job()
        job.params = bzfs.Params(args, log_params=log_params, log=bzfs.get_logger(log_params, args))
        snapshots = [f"{i}\t" + snapshot for i, snapshot in enumerate(snapshots)]  # simulate creation time
        results = job.filter_snapshots(snapshots)
        results = [result.split("\t", 1)[1] for result in results]  # drop creation time
        return results
    finally:
        bzfs.reset_logger()


#############################################################################
class TestRankRangeAction(unittest.TestCase):

    def setUp(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--ranks", nargs="+", action=bzfs.TimeRangeAndRankRangeAction)

    def parse_args(self, arg):
        return self.parser.parse_args(["--ranks", "0..0", arg])

    def test_valid_ranks(self):
        self.assertEqual((("latest", 0, False), ("latest", 2, True)), self.parse_args("latest0..latest2%").ranks[1])
        self.assertEqual((("oldest", 5, True), ("oldest", 9, True)), self.parse_args("oldest5%..oldest9%").ranks[1])

    def test_invalid_ranks(self):
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

    def test_ambigous_rankrange(self):
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

    def filter_snapshots_by_rank(self, snapshots, ranks, timerange="0..0"):
        return filter_snapshots_by_times_and_rank(snapshots, timerange=timerange, ranks=ranks)

    def test_filter_snapshots_by_rank(self):
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
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["latest0%..latest100%"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest0%..oldest100%"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest100%..oldest0%"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["latest100%..latest0%"]))

        self.assertListEqual(["\td@2"], self.filter_snapshots_by_rank(lst1, ["oldest2..oldest3"]))
        self.assertListEqual(["\td@3"], self.filter_snapshots_by_rank(lst1, ["oldest3..oldest4"]))
        self.assertListEqual([], self.filter_snapshots_by_rank(lst1, ["oldest4..oldest5"]))
        self.assertListEqual([], self.filter_snapshots_by_rank(lst1, ["oldest5..oldest6"]))

        lst2 = ["\t" + snapshot for snapshot in ["d@0", "d@1", "d@2"]]
        self.assertListEqual(["\td@0", "\td@1"], self.filter_snapshots_by_rank(lst2, ["oldest 51%"]))
        self.assertListEqual(["\td@0"], self.filter_snapshots_by_rank(lst2, ["oldest 49%"]))
        self.assertListEqual(["\td@1", "\td@2"], self.filter_snapshots_by_rank(lst2, ["latest 51%"]))
        self.assertListEqual(["\td@2"], self.filter_snapshots_by_rank(lst2, ["latest 49%"]))
        self.assertListEqual([], self.filter_snapshots_by_rank(lst2, ["latest 0%"]))
        self.assertListEqual(lst2, self.filter_snapshots_by_rank(lst2, ["latest 100%"]))

        self.assertListEqual(["\td@0"], self.filter_snapshots_by_rank(lst1, ["oldest0..oldest1"]))
        self.assertListEqual(["\td@0", "\td@1"], self.filter_snapshots_by_rank(lst2, ["latest100%..latest1"]))

    def test_filter_snapshots_by_rank_with_bookmarks(self):
        lst1 = ["\t" + snapshot for snapshot in ["d@0", "d#1", "d@2", "d@3"]]
        self.assertListEqual(["\td#1"], self.filter_snapshots_by_rank(lst1, ["oldest0..oldest0"]))
        self.assertListEqual(["\td@0", "\td#1"], self.filter_snapshots_by_rank(lst1, ["oldest0..oldest1"]))
        self.assertListEqual(["\td@0", "\td#1", "\td@2"], self.filter_snapshots_by_rank(lst1, ["oldest0..oldest2"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest0..oldest3"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest0..oldest4"]))
        self.assertListEqual(lst1, self.filter_snapshots_by_rank(lst1, ["oldest0..oldest5"]))
        self.assertListEqual(["\td@0", "\td#1", "\td@2"], self.filter_snapshots_by_rank(lst1, ["latest1..latest100%"]))

    def test_filter_snapshots_by_rank_with_chain(self):
        lst1 = ["\t" + snapshot for snapshot in ["d@0", "d#1", "d@2", "d@3"]]
        results = self.filter_snapshots_by_rank(lst1, ["latest1..latest100%", "latest1..latest100%"])
        self.assertListEqual(["\td@0", "\td#1"], results)

        results = self.filter_snapshots_by_rank(lst1, ["latest1..latest100%", "oldest1..oldest100%"])
        self.assertListEqual(["\td#1", "\td@2"], results)

    def test_filter_snapshots_by_rank_with_times(self):
        lst1 = ["\t" + snapshot for snapshot in ["d@0", "d#1", "d@2", "d@3"]]
        self.assertListEqual(["\td@0", "\td#1"], self.filter_snapshots_by_rank(lst1, ["oldest 1"], timerange="0..0"))
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
    def get_snapshot_filters(cli):
        args = argparser_parse_args(args=["src", "dst", *cli])
        return bzfs.Params(args).snapshot_filters

    def test_merge_adjacent_snapshot_regexes_and_filters0(self):
        ranks = "--include-snapshot-times-and-ranks"
        cli = [ranks, "0..1", "oldest 50%", ranks, "0..0", "oldest 10%"]
        ranks_filter = self.get_snapshot_filters(cli)
        self.assertEqual((0, 1), ranks_filter[0].timerange)
        self.assertEqual([(("oldest", 0, False), ("oldest", 50, True))], ranks_filter[0].options)
        self.assertEqual((0, 0), ranks_filter[1].timerange)
        self.assertEqual([(("oldest", 0, False), ("oldest", 10, True))], ranks_filter[1].options)

    def test_merge_adjacent_snapshot_regexes_and_filters1(self):
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

    def test_merge_adjacent_snapshot_regexes_and_filters2(self):
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

    def test_merge_adjacent_snapshot_regexes_and_filters3(self):
        include = "--include-snapshot-regex"
        exclude = "--exclude-snapshot-regex"
        times = "--include-snapshot-times-and-ranks"
        cli = [times, "*..*", times, "0..9", include, "f", include, "d", exclude, "w", include, "h", exclude, "m"]
        times_filter, regex_filter = self.get_snapshot_filters(cli)
        self.assertEqual("include_snapshot_times", times_filter.name)
        self.assertEqual((0, 9), times_filter.timerange)
        self.assertEqual(bzfs.snapshot_regex_filter_name, regex_filter.name)
        self.assertEqual((["w", "m"], ["f", "d", "h"]), regex_filter.options)

    def test_merge_adjacent_snapshot_regexes_doesnt_merge_across_groups(self):
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

    def test_reorder_snapshot_times_simple(self):
        include = "--include-snapshot-regex"
        times = "--include-snapshot-times-and-ranks"
        cli = [include, ".*daily", times, "0..9"]
        times_filter, regex_filter = self.get_snapshot_filters(cli)
        self.assertEqual("include_snapshot_times", times_filter.name)
        self.assertEqual((0, 9), times_filter.timerange)
        self.assertEqual(bzfs.snapshot_regex_filter_name, regex_filter.name)
        self.assertEqual(([], [".*daily"]), regex_filter.options)

    def test_reorder_snapshot_times_complex(self):
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

    def setUp(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--log-config-var", nargs="+", action=bzfs.LogConfigVariablesAction)

    def test_basic(self):
        args = self.parser.parse_args(["--log-config-var", "name1:val1", "name2:val2"])
        self.assertEqual(args.log_config_var, ["name1:val1", "name2:val2"])

        for var in ["", "  ", "varWithoutColon", ":valueWithoutName", " nameWithWhitespace:value"]:
            with self.assertRaises(SystemExit):
                self.parser.parse_args(["--log-config-var", var])


#############################################################################
class TestSafeFileNameAction(unittest.TestCase):

    def setUp(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("filename", action=bzfs.SafeFileNameAction)

    def test_safe_filename(self):
        args = self.parser.parse_args(["file1.txt"])
        self.assertEqual(args.filename, "file1.txt")

    def test_empty_filename(self):
        args = self.parser.parse_args([""])
        self.assertEqual(args.filename, "")

    def test_filename_in_subdirectory(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["subdir/safe_file.txt"])

    def test_unsafe_filename_with_parent_directory_reference(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["../escape.txt"])

    def test_unsafe_filename_with_absolute_path(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["/unsafe_file.txt"])

    def test_unsafe_nested_parent_directory(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["../../another_escape.txt"])

    def test_filename_with_single_dot_slash(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["./file.txt"])


#############################################################################
class TestCheckRange(unittest.TestCase):

    def test_valid_range_min_max(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange, min=0, max=100)
        args = parser.parse_args(["--age", "50"])
        self.assertEqual(args.age, 50)

    def test_valid_range_inf_sup(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange, inf=0, sup=100)
        args = parser.parse_args(["--age", "50"])
        self.assertEqual(args.age, 50)

    def test_invalid_range_min_max(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange, min=0, max=100)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "-1"])

    def test_invalid_range_inf_sup(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange, inf=0, sup=100)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "101"])

    def test_invalid_combination_min_inf(self):
        with self.assertRaises(ValueError):
            parser = argparse.ArgumentParser()
            parser.add_argument("--age", type=int, action=bzfs.CheckRange, min=0, inf=100)

    def test_invalid_combination_max_sup(self):
        with self.assertRaises(ValueError):
            parser = argparse.ArgumentParser()
            parser.add_argument("--age", type=int, action=bzfs.CheckRange, max=0, sup=100)

    def test_valid_float_range_min_max(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(["--age", "50.5"])
        self.assertEqual(args.age, 50.5)

    def test_invalid_float_range_min_max(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange, min=0.0, max=100.0)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "-0.1"])

    def test_valid_edge_case_min(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(["--age", "0.0"])
        self.assertEqual(args.age, 0.0)

    def test_valid_edge_case_max(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(["--age", "100.0"])
        self.assertEqual(args.age, 100.0)

    def test_invalid_edge_case_sup(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange, inf=0.0, sup=100.0)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "100.0"])

    def test_invalid_edge_case_inf(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange, inf=0.0, sup=100.0)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--age", "0.0"])

    def test_no_range_constraints(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange)
        args = parser.parse_args(["--age", "150"])
        self.assertEqual(args.age, 150)

    def test_no_range_constraints_float(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=bzfs.CheckRange)
        args = parser.parse_args(["--age", "150.5"])
        self.assertEqual(args.age, 150.5)

    def test_very_large_value(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange, max=10**18)
        args = parser.parse_args(["--age", "999999999999999999"])
        self.assertEqual(args.age, 999999999999999999)

    def test_very_small_value(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange, min=-(10**18))
        args = parser.parse_args(["--age", "-999999999999999999"])
        self.assertEqual(args.age, -999999999999999999)

    def test_default_interval(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange)
        action = bzfs.CheckRange(option_strings=["--age"], dest="age")
        self.assertEqual(action.interval(), "valid range: (-infinity, +infinity)")

    def test_interval_with_inf_sup(self):
        action = bzfs.CheckRange(option_strings=["--age"], dest="age", inf=0, sup=100)
        self.assertEqual(action.interval(), "valid range: (0, 100)")

    def test_interval_with_min_max(self):
        action = bzfs.CheckRange(option_strings=["--age"], dest="age", min=0, max=100)
        self.assertEqual(action.interval(), "valid range: [0, 100]")

    def test_interval_with_min(self):
        action = bzfs.CheckRange(option_strings=["--age"], dest="age", min=0)
        self.assertEqual(action.interval(), "valid range: [0, +infinity)")

    def test_interval_with_max(self):
        action = bzfs.CheckRange(option_strings=["--age"], dest="age", max=100)
        self.assertEqual(action.interval(), "valid range: (-infinity, 100]")

    def test_call_without_range_constraints(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=bzfs.CheckRange)
        args = parser.parse_args(["--age", "50"])
        self.assertEqual(args.age, 50)


#############################################################################
class TestCheckPercentRange(unittest.TestCase):

    def test_valid_range_min(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--threads", action=bzfs.CheckPercentRange, min=1)
        args = parser.parse_args(["--threads", "1"])
        threads, is_percent = args.threads
        self.assertEqual(threads, 1.0)
        self.assertFalse(is_percent)

    def test_valid_range_percent(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--threads", action=bzfs.CheckPercentRange, min=1)
        args = parser.parse_args(["--threads", "5.2%"])
        threads, is_percent = args.threads
        self.assertEqual(threads, 5.2)
        self.assertTrue(is_percent)

    def test_invalid(self):
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
    if sys.version_info < (3, 7):
        print(f"ERROR: {prog_name} requires Python version >= 3.7!", file=sys.stderr)
        sys.exit(die_status)
    """

    @patch("sys.exit")
    @patch("sys.version_info", new=(3, 6))
    def test_version_below_3_7(self, mock_exit):
        with patch("sys.stderr"):
            import importlib
            from bzfs import bzfs

            importlib.reload(bzfs)  # Reload module to apply version patch
            mock_exit.assert_called_with(bzfs.die_status)

    @patch("sys.exit")
    @patch("sys.version_info", new=(3, 7))
    def test_version_3_7_or_higher(self, mock_exit):
        import importlib
        from bzfs import bzfs

        importlib.reload(bzfs)  # Reload module to apply version patch
        mock_exit.assert_not_called()


#############################################################################
class TestConnectionPool(unittest.TestCase):
    def setUp(self):
        args = argparser_parse_args(args=["src", "dst", "-v"])
        p = bzfs.Params(args, log=bzfs.get_logger(bzfs.LogParams(args), args))
        self.src = p.src
        self.dst = p.dst
        self.dst.ssh_user_host = "127.0.0.1"
        self.remote = bzfs.Remote("src", args, p)
        self.src2 = bzfs.Remote("src", args, p)

    def assert_priority_queue(self, cpool, queuelen, replaced, dirty=None):
        if dirty is None:
            self.assertTupleEqual((len(cpool.priority_queue), cpool.replaced), (queuelen, replaced))
        else:
            self.assertTupleEqual((len(cpool.priority_queue), cpool.replaced, cpool.dirty), (queuelen, replaced, dirty))
        pass

    def assert_equal_connections(self, conn, donn):
        self.assertTupleEqual((conn.cid, conn.ssh_cmd), (donn.cid, donn.ssh_cmd))
        self.assertEqual(conn.free, donn.free)

    def get_connection(self, cpool, dpool):
        conn = cpool.get_connection()
        donn = dpool.get_connection()
        self.assert_equal_connections(conn, donn)
        return conn, donn

    def return_connection(self, cpool, conn, dpool, donn):
        self.assertTupleEqual((conn.cid, conn.ssh_cmd), (donn.cid, donn.ssh_cmd))
        cpool.return_connection(conn)
        dpool.return_connection(donn)

    def test_basic(self):
        counter1a = itertools.count()
        counter2a = itertools.count()
        counter1b = itertools.count()
        self.src.local_ssh_command = lambda: [str(next(counter1a))]
        self.src2.local_ssh_command = lambda: [str(next(counter1b))]
        initial_last_modified = 0

        with self.assertRaises(AssertionError):
            bzfs.ConnectionPool(self.src, 0)

        capacity = 2
        cpool = bzfs.ConnectionPool(self.src, capacity)
        dpool = SlowButCorrectConnectionPool(self.src2, capacity)
        self.assert_priority_queue(cpool, 0, 0, 0)
        self.assertIsNotNone(repr(cpool))
        self.assertIsNotNone(str(cpool))
        cpool.shutdown("foo")

        conn1, donn1 = self.get_connection(cpool, dpool)
        self.assert_priority_queue(cpool, 1, 0, 0)
        self.assertEqual(conn1.free, (capacity - 1) * -1)
        self.assertEqual(conn1.last_modified, initial_last_modified)
        self.assertIsNone(conn1.replacement)
        i = [str(next(counter2a))]
        self.assertEqual(conn1.ssh_cmd, i)
        self.assertEqual(conn1.ssh_cmd_quoted, i)
        self.assertIsNotNone(repr(conn1))
        self.assertIsNotNone(str(conn1))

        self.return_connection(cpool, conn1, dpool, donn1)
        self.assert_priority_queue(cpool, 2, 1, 1)
        self.assertIsNotNone(conn1.replacement)
        self.assertEqual(conn1.last_modified, initial_last_modified)

        conn2, donn2 = self.get_connection(cpool, dpool)
        self.assert_priority_queue(cpool, 2, 1, 1)
        self.assertIsNot(conn1, conn2)
        self.assertEqual(conn2.free, (capacity - 1) * -1)
        self.assertNotEqual(conn2.last_modified, initial_last_modified)
        self.assertIsNone(conn2.replacement)
        self.assertIsNotNone(conn1.replacement)

        conn3, donn3 = self.get_connection(cpool, dpool)
        self.assert_priority_queue(cpool, 2, 1, 1)
        self.assertIs(conn2, conn3)
        self.assertEqual(conn3.free, (capacity - 2) * -1)
        self.assertNotEqual(conn3.last_modified, initial_last_modified)
        self.assertIsNone(conn3.replacement)
        self.assertIsNotNone(conn1.replacement)
        self.assertIsNone(conn2.replacement)

        self.return_connection(cpool, conn2, dpool, donn2)
        self.assert_priority_queue(cpool, 1, 0, 0)
        self.assertIsNotNone(conn2.replacement)
        self.assertIsNotNone(conn1.replacement)
        self.assertIsNotNone(conn3.replacement)
        self.assertNotEqual(conn2.last_modified, initial_last_modified)

        self.return_connection(cpool, conn3, dpool, donn3)
        self.assert_priority_queue(cpool, 2, 1, 1)
        self.assertIsNotNone(conn1.replacement)
        self.assertIsNotNone(conn2.replacement)
        self.assertIsNotNone(conn3.replacement)
        self.assertNotEqual(conn3.last_modified, initial_last_modified)

        with self.assertRaises(AssertionError):
            cpool.return_connection(None)
        with self.assertRaises(AssertionError):
            cpool.return_connection(conn3)
        with self.assertRaises(AssertionError):
            cpool.return_connection(conn3)

    def test_multiple_TCP_connections(self):
        capacity = 2
        cpool = bzfs.ConnectionPool(self.remote, capacity)

        conn1 = cpool.get_connection()
        self.assertEqual(conn1.free, (capacity - 1) * -1)
        conn2 = cpool.get_connection()
        self.assertIs(conn1, conn2)
        self.assertEqual(conn2.free, (capacity - 2) * -1)
        self.assert_priority_queue(cpool, 1, 0, 0)

        conn3 = cpool.get_connection()
        self.assertIsNot(conn2, conn3)
        self.assertEqual(conn3.free, (capacity - 1) * -1)
        self.assertEqual(conn2.free, (capacity - 2) * -1)
        self.assert_priority_queue(cpool, 2, 0, 0)
        conn4 = cpool.get_connection()
        self.assertIs(conn3, conn4)
        self.assertEqual(conn4.free, (capacity - 2) * -1)
        self.assert_priority_queue(cpool, 2, 0, 0)

        conn5 = cpool.get_connection()
        self.assertIsNot(conn4, conn5)
        self.assertEqual(conn5.free, (capacity - 1) * -1)
        self.assertEqual(conn4.free, (capacity - 2) * -1)
        self.assert_priority_queue(cpool, 3, 0, 0)

        cpool.return_connection(conn3)
        self.assert_priority_queue(cpool, 4, 1, 1)
        cpool.return_connection(conn4)
        t4 = cpool.last_modified
        self.assert_priority_queue(cpool, 5, 2, 2)

        cpool.return_connection(conn2)
        self.assert_priority_queue(cpool, 6, 3, 3)
        cpool.return_connection(conn1)
        t2 = cpool.last_modified
        self.assert_priority_queue(cpool, 3, 0, 0)

        cpool.return_connection(conn5)
        t5 = cpool.last_modified
        self.assert_priority_queue(cpool, 4, 1, 1)

        # assert sort order evens out the number of concurrent sessions among the TCP connections
        conn5a = cpool.get_connection()
        self.assertEqual(conn5a.free, (capacity - 1) * -1)

        conn2a = cpool.get_connection()
        self.assertEqual(conn2a.free, (capacity - 1) * -1)

        conn4a = cpool.get_connection()
        self.assertEqual(conn4a.free, (capacity - 1) * -1)

        # assert tie-breaker in favor of most recently returned TCP connection
        conn6 = cpool.get_connection()
        self.assertEqual(conn6.free, (capacity - 2) * -1)
        self.assertEqual(abs(conn6.last_modified), t5)

        conn1a = cpool.get_connection()
        self.assertEqual(conn1a.free, (capacity - 2) * -1)
        self.assertEqual(abs(conn1a.last_modified), t2)

        conn4a = cpool.get_connection()
        self.assertEqual(conn4a.free, (capacity - 2) * -1)
        self.assertEqual(abs(conn4a.last_modified), t4)

        cpool.shutdown("foo")

    def test_pools(self):
        with self.assertRaises(AssertionError):
            bzfs.ConnectionPools(self.dst, {"shared": 0})

        bzfs.ConnectionPools(self.dst, {}).shutdown("foo")

        pools = bzfs.ConnectionPools(self.dst, {"shared": 8, "dedicated": 1})
        self.assertIsNotNone(pools.pool("shared"))
        self.assertIsNotNone(repr(pools))
        self.assertIsNotNone(str(pools))
        pools.shutdown("foo")
        conn = pools.pool("shared").get_connection()
        pools.shutdown("foo")

    def test_return_sequence(self):
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

    def test_long_random_walk(self):
        log = logging.getLogger(bzfs.__name__)
        # loglevel = logging.DEBUG
        loglevel = bzfs.log_trace
        is_logging = log.isEnabledFor(loglevel)
        num_steps = 1000 if not is_adhoc_test and not is_functional_test else 75
        log.info(f"num_random_steps: {num_steps}")
        start_time_nanos = time.time_ns()
        total_returns = 0
        total_return_iters_sum = 0
        total_return_iters = Counter()
        for maxsessions in range(1, 10 + 1):
            for items in range(0, 64 + 1):
                counter1a = itertools.count()
                counter1b = itertools.count()
                self.src.local_ssh_command = lambda: [str(next(counter1a))]
                self.src2.local_ssh_command = lambda: [str(next(counter1b))]
                cpool = bzfs.ConnectionPool(self.src, maxsessions, is_trace=True)
                dpool = SlowButCorrectConnectionPool(self.src2, maxsessions)
                # dpool = bzfs.ConnectionPool(self.src2, maxsessions)
                rng = random.Random(12345)
                conns = []
                try:
                    for item in range(0, items):
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
                total_returns += cpool.returns
                total_return_iters_sum += cpool.return_iters_sum
                total_return_iters.update(cpool.return_iters)
        elapsed_secs = (time.time_ns() - start_time_nanos) / 1000_000_000
        log.info("random_walk took %s secs", elapsed_secs)
        s = sum(total_return_iters.values())
        total_return_iters = {key: value for key, value in sorted(total_return_iters.items())}
        percentage_iters = {key: 100 * value / s for key, value in total_return_iters.items()}
        log.info(
            "%stotal_returns: %s, total_return_iters_sum: %s, total_return_iters: %s, %s",
            "",
            total_returns,
            total_return_iters_sum,
            total_return_iters,
            percentage_iters,
        )


#############################################################################
class SlowButCorrectConnectionPool(bzfs.ConnectionPool):  # validate a better implementation against this baseline
    def get_connection(self) -> bzfs.Connection:
        with self._lock:
            self.priority_queue.sort()
            conn = self.priority_queue[0] if self.priority_queue else None
            if conn is None or conn.is_full():
                conn = bzfs.Connection(self.remote, self.capacity, self.cid)
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

    def test_basic1(self):
        input_snapshots = ["d1", "h1", "d2", "d3", "d4"]
        expected_results = ["d1", "d2", "d3", "d4"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic2(self):
        input_snapshots = ["d1", "d2", "h1", "d3", "d4"]
        expected_results = ["d1", "d2", "d3", "d4"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic3(self):
        input_snapshots = ["h0", "h1", "d1", "d2", "h2", "d3", "d4"]
        expected_results = ["d1", "d2", "d3", "d4"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic4(self):
        input_snapshots = ["d1"]
        expected_results = ["d1"]
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_basic5(self):
        input_snapshots = []
        expected_results = []
        self.validate_incremental_send_steps(input_snapshots, expected_results)

    def test_validate_snapshot_series_excluding_hourlies_with_permutations(self):
        for i, testcase in enumerate(self.permute_snapshot_series()):
            with stop_on_failure_subtest(i=i):
                self.validate_incremental_send_steps(testcase[None], testcase["d"])

    def test_send_step_to_str(self):
        bzfs.Job().send_step_to_str(("-I", "d@s1", "d@s3"))

    def permute_snapshot_series(self, max_length=9):
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
                    snaps = defaultdict(list)
                    count = defaultdict(int)
                    for char in permutation:
                        count[char] += 1  # tag snapshots with a monotonically increasing number within each category
                        char_count = f"{count[char]:01}" if max_length < 10 else f"{count[char]:02}"  # zero pad number
                        snapshot = f"{char}{char_count}"
                        snaps[None].append(snapshot)
                        snaps[char].append(snapshot)  # represents expected results for test verification
                    testcases.append(snaps)
        return testcases

    def validate_incremental_send_steps(self, input_snapshots, expected_results):
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

    def send_step_to_str(self, step):
        # return str(step)
        return str(step[1]) + ("-" if step[0] == "-I" else ":") + str(step[2])

    def apply_incremental_send_steps(self, steps, input_snapshots):
        """Simulates replicating (a subset of) the given input_snapshots to a destination, according to the given steps.
        Returns the subset of snapshots that have actually been replicated to the destination."""
        output_snapshots = []
        for incr_flag, start_snapshot, end_snapshot, num_snapshots in steps:
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

    def incremental_send_steps1(self, input_snapshots, src_dataset=None, is_resume=False, force_convert_I_to_i=False):
        origin_src_snapshots_with_guids = []
        guid = 1
        for snapshot in input_snapshots:
            origin_src_snapshots_with_guids.append(f"{guid}\t{src_dataset}{snapshot}")
            guid += 1
        return self.incremental_send_steps2(
            origin_src_snapshots_with_guids, is_resume=is_resume, force_convert_I_to_i=force_convert_I_to_i
        )

    def incremental_send_steps2(self, origin_src_snapshots_with_guids, is_resume=False, force_convert_I_to_i=False):
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
class TestSynchronizedBool(unittest.TestCase):
    def test_initialization(self):
        b = bzfs.SynchronizedBool(True)
        self.assertTrue(b.value)

        b = bzfs.SynchronizedBool(False)
        self.assertFalse(b.value)

        with self.assertRaises(AssertionError):
            bzfs.SynchronizedBool("not_a_bool")

    def test_value_property(self):
        b = bzfs.SynchronizedBool(True)
        self.assertTrue(b.value)

        b.value = False
        self.assertFalse(b.value)

    def test_loop(self):
        b = bzfs.SynchronizedBool(False)
        for _ in range(3):
            b.value = not b.value
        self.assertIsInstance(b.value, bool)

    def test_bool_conversion(self):
        b = bzfs.SynchronizedBool(True)
        self.assertTrue(bool(b))

        b.value = False
        self.assertFalse(bool(b))

    def test_str_and_repr(self):
        b = bzfs.SynchronizedBool(True)
        self.assertEqual(str(b), "True")
        self.assertEqual(repr(b), "True")

        b.value = False
        self.assertEqual(str(b), "False")
        self.assertEqual(repr(b), "False")


#############################################################################
class TestSynchronizedDict(unittest.TestCase):
    def setUp(self):
        self.sync_dict = bzfs.SynchronizedDict({"a": 1, "b": 2, "c": 3})

    def test_getitem(self):
        self.assertEqual(self.sync_dict["a"], 1)
        self.assertEqual(self.sync_dict["b"], 2)

    def test_setitem(self):
        self.sync_dict["d"] = 4
        self.assertEqual(self.sync_dict["d"], 4)

    def test_delitem(self):
        del self.sync_dict["a"]
        self.assertNotIn("a", self.sync_dict)

    def test_contains(self):
        self.assertTrue("a" in self.sync_dict)
        self.assertFalse("z" in self.sync_dict)

    def test_len(self):
        self.assertEqual(len(self.sync_dict), 3)
        del self.sync_dict["a"]
        self.assertEqual(len(self.sync_dict), 2)

    def test_repr(self):
        self.assertEqual(repr(self.sync_dict), repr({"a": 1, "b": 2, "c": 3}))

    def test_str(self):
        self.assertEqual(str(self.sync_dict), str({"a": 1, "b": 2, "c": 3}))

    def test_get(self):
        self.assertEqual(self.sync_dict.get("a"), 1)
        self.assertEqual(self.sync_dict.get("z", 42), 42)

    def test_pop(self):
        value = self.sync_dict.pop("b")
        self.assertEqual(value, 2)
        self.assertNotIn("b", self.sync_dict)

    def test_clear(self):
        self.sync_dict.clear()
        self.assertEqual(len(self.sync_dict), 0)

    def test_items(self):
        items = self.sync_dict.items()
        self.assertEqual(set(items), {("a", 1), ("b", 2), ("c", 3)})

    def test_loop(self):
        self.sync_dict["key"] = 1
        self.assertIn("key", self.sync_dict)


#############################################################################
class TestPerformance(unittest.TestCase):

    def test_close_fds(self):
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
                secs = (time.time_ns() - start_time_nanos) / 1000_000_000
                print(f"close_fds={close_fds}: Took {secs:.1f} seconds, iters/sec: {iters/secs:.1f}")


#############################################################################
T = TypeVar("T")


def find_match(
    seq: Sequence[T],
    predicate: Callable[[T], bool],
    start: Optional[int] = None,
    end: Optional[int] = None,
    reverse: bool = False,
    raises: Union[bool, str, Callable[[], str]] = False,  # raises: bool | str | Callable = False,  # python >= 3.10
) -> int:
    """Returns the integer index within seq of the first item (or last item if reverse==True) that matches the given
    predicate condition. If no matching item is found returns -1 or ValueError, depending on the raises parameter,
    which is a bool indicating whether to raise an error, or a string containing the error message, but can also be a
    Callable/lambda in order to support efficient deferred generation of error messages.
    Analog to str.find(), including slicing semantics with parameters start and end.
    For example, seq can be a list, tuple or str.

    Example usage:
        lst = ["a", "b", "-c", "d"]
        i = find_match(lst, lambda arg: arg.startswith("-"), start=1, end=3, reverse=True)
        if i >= 0:
            ...
        i = find_match(lst, lambda arg: arg.startswith("-"), raises=f"Tag {tag} not found in {file}")
        i = find_match(lst, lambda arg: arg.startswith("-"), raises=lambda: f"Tag {tag} not found in {file}")
    """
    offset = 0 if start is None else start if start >= 0 else len(seq) + start
    if start is not None or end is not None:
        seq = seq[start:end]
    for i, item in enumerate(reversed(seq) if reverse else seq):
        if predicate(item):
            if reverse:
                return len(seq) - i - 1 + offset
            else:
                return i + offset
    if raises is False or raises is None:
        return -1
    if raises is True:
        raise ValueError("No matching item found in sequence")
    if callable(raises):
        raises = raises()
    raise ValueError(raises)


@contextmanager
def stop_on_failure_subtest(**params):
    """Context manager to mimic UnitTest.subTest() but stop on first failure"""
    try:
        yield
    except AssertionError:
        raise AssertionError(f"SubTest failed with parameters: {params}")
