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
import re
import socket
import sys
import tempfile
import time
import unittest
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Sequence, Callable, Optional, TypeVar, Union
from unittest.mock import patch, mock_open

from bzfs import bzfs
from tests.zfs_util import is_solaris_zfs


def suite():
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestHelperFunctions))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestParseDatasetLocator))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestReplaceCapturingGroups))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestFindMatch))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestArgumentParser))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestDatasetPairsAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestFileOrLiteralAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestLogConfigVariablesAction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCheckRange))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestPythonVersionCheck))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(ExcludeSnapshotRegexValidationCase))
    return suite


#############################################################################
class TestHelperFunctions(unittest.TestCase):

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
        params = bzfs.Params(bzfs.argument_parser().parse_args(args=["src", "dst"]))
        params.validate_quoting([""])
        params.validate_quoting(["foo"])
        with self.assertRaises(SystemExit):
            params.validate_quoting(['foo"'])
        with self.assertRaises(SystemExit):
            params.validate_quoting(["foo'"])
        with self.assertRaises(SystemExit):
            params.validate_quoting(["foo`"])

    def test_validate_arg(self):
        params = bzfs.Params(bzfs.argument_parser().parse_args(args=["src", "dst"]))
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
        args = bzfs.argument_parser().parse_args(args=["src", "dst"])
        setattr(args, "zfs_program", "")
        with self.assertRaises(SystemExit):
            bzfs.Params(args)

    def test_split_args(self):
        params = bzfs.Params(bzfs.argument_parser().parse_args(args=["src", "dst"]))
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
        params = bzfs.Params(bzfs.argument_parser().parse_args(args=["src", "dst"]))
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

    def test_delete_stale_ssh_socket_files(self):
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

            bzfs.delete_stale_ssh_socket_files(tmpdir, "s")

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

    def recv_option_property_names_old(self):
        def names(lst):
            return bzfs.Job().recv_option_property_names(lst)

        self.assertSetEqual(set(), names([]))
        self.assertSetEqual(set(), names(["name1=value1"]))
        self.assertSetEqual({"name1"}, names(["-o", "name1=value1"]))
        self.assertSetEqual({"name1", "name2"}, names(["-o", "name1=value1", "-o", "name2=value2"]))
        self.assertSetEqual({"name1", "name2"}, names(["-v", "-o", "name1=value1", "-o", "name2=value2"]))
        self.assertSetEqual({"name1", "name2"}, names(["-v", "-o", "name1=value1", "-o", "name2=value2", "-F"]))
        self.assertSetEqual({"name1", "name2"}, names(["-v", "-o", "name1=value1", "-n", "-o", "name2=value2", "-F"]))
        self.assertSetEqual({"name1"}, names(["-o", "name1"]))
        self.assertSetEqual({""}, names(["-o", "=value1"]))
        with self.assertRaises(SystemExit):
            names(["-o"])
        with self.assertRaises(SystemExit):
            names(["-o", "name1=value1", "-o"])

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
        args = bzfs.argument_parser().parse_args(args=["src", "dst"])
        root_logger = logging.getLogger()
        log_params = None
        log = bzfs.get_logger(log_params, args, root_logger)
        self.assertTrue(log is root_logger)
        log.info(prefix + "aaa1")

        args = bzfs.argument_parser().parse_args(args=["src", "dst"])
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

        args = bzfs.argument_parser().parse_args(args=["src", "dst", "-v"])
        log_params = bzfs.LogParams(args)
        log = bzfs.get_logger(log_params, args)
        self.assertIsNotNone(log)
        files.add(os.path.abspath(log_params.log_file))
        check(log, files)

        log.addFilter(lambda record: True)  # dummy
        bzfs.reset_logger()
        files.clear()
        check(log, files)

        args = bzfs.argument_parser().parse_args(args=["src", "dst", "-v", "-v"])
        log_params = bzfs.LogParams(args)
        log = bzfs.get_logger(log_params, args)
        self.assertIsNotNone(log)
        files.add(os.path.abspath(log_params.log_file))
        check(log, files)

        args = bzfs.argument_parser().parse_args(args=["src", "dst", "--quiet"])
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
class ExcludeSnapshotRegexValidationCase(unittest.TestCase):

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
        # src_dataset = "s@"
        src_dataset = ""
        for force_convert_I_to_i in [False, True]:
            steps = self.incremental_send_steps1(
                input_snapshots, src_dataset=src_dataset, force_convert_I_to_i=force_convert_I_to_i
            )
            # print(f"input_snapshots:" + ','.join(input_snapshots))
            # print("steps: " + ','.join([self.send_step_to_str(step) for step in steps]))
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
        for incr_flag, start_snapshot, end_snapshot in steps:
            start = input_snapshots.index(start_snapshot)
            end = input_snapshots.index(end_snapshot)
            if incr_flag == "-I":
                for j in range(start + 1, end + 1):
                    output_snapshots.append(input_snapshots[j])
            else:
                output_snapshots.append(input_snapshots[end])
        return output_snapshots

    def incremental_send_steps1(self, input_snapshots, src_dataset=None, force_convert_I_to_i=False):
        origin_src_snapshots_with_guids = []
        guid = 1
        for snapshot in input_snapshots:
            origin_src_snapshots_with_guids.append(f"{guid}\t{src_dataset}{snapshot}")
            guid += 1
        return self.incremental_send_steps2(origin_src_snapshots_with_guids, force_convert_I_to_i=force_convert_I_to_i)

    def incremental_send_steps2(self, origin_src_snapshots_with_guids, force_convert_I_to_i=False):
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
            input_snapshots, guids, included_guids=included_guids, force_convert_I_to_i=force_convert_I_to_i
        )


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