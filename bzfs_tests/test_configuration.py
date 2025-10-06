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
"""Unit tests for the ``bzfs`` CLI."""

from __future__ import (
    annotations,
)
import os
import shutil
import socket
import stat
import tempfile
import time
import unittest
from pathlib import (
    Path,
)
from typing import (
    Any,
)
from unittest.mock import (
    patch,
)

from bzfs_main import (
    bzfs,
    configuration,
)
from bzfs_main.configuration import (
    LogParams,
    Remote,
    SnapshotLabel,
)
from bzfs_main.utils import (
    UNIX_TIME_INFINITY_SECS,
)
from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestHelperFunctions,
        TestAdditionalHelpers,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestHelperFunctions(AbstractTestCase):

    def test_validate_quoting(self) -> None:
        params = self.make_params(args=self.argparser_parse_args(args=["src", "dst"]))
        params._validate_quoting([""])
        params._validate_quoting(["foo"])
        with self.assertRaises(SystemExit):
            params._validate_quoting(['foo"'])
        with self.assertRaises(SystemExit):
            params._validate_quoting(["foo'"])
        with self.assertRaises(SystemExit):
            params._validate_quoting(["foo`"])

    def test_validate_arg(self) -> None:
        params = self.make_params(args=self.argparser_parse_args(args=["src", "dst"]))
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
            self.make_params(args=args)
        args.zpool_program = None
        with self.assertRaises(SystemExit):
            self.make_params(args=args)

    def test_validate_program_name_must_not_contain_special_chars(self) -> None:
        args = self.argparser_parse_args(args=["src", "dst"])
        args.zpool_program = "true;false"
        with self.assertRaises(SystemExit):
            self.make_params(args=args)
        args.zpool_program = "echo foo|cat"
        with self.assertRaises(SystemExit):
            self.make_params(args=args)
        args.zpool_program = "foo>bar"
        with self.assertRaises(SystemExit):
            self.make_params(args=args)
        args.zpool_program = "foo\nbar"
        with self.assertRaises(SystemExit):
            self.make_params(args=args)
        args.zpool_program = "foo\\bar"
        with self.assertRaises(SystemExit):
            self.make_params(args=args)

    def test_split_args(self) -> None:
        params = self.make_params(args=self.argparser_parse_args(args=["src", "dst"]))
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

    def test_fix_send_recv_opts(self) -> None:
        params = self.make_params(args=self.argparser_parse_args(args=["src", "dst"]))
        self.assertEqual([], params._fix_recv_opts(["-n"], frozenset())[0])
        self.assertEqual([], params._fix_recv_opts(["--dryrun", "-n"], frozenset())[0])
        self.assertEqual([""], params._fix_recv_opts([""], frozenset())[0])
        self.assertEqual([], params._fix_recv_opts([], frozenset())[0])
        self.assertEqual(["-"], params._fix_recv_opts(["-"], frozenset())[0])
        self.assertEqual(["-h"], params._fix_recv_opts(["-hn"], frozenset())[0])
        self.assertEqual(["-h"], params._fix_recv_opts(["-nh"], frozenset())[0])
        self.assertEqual(["--Fvhn"], params._fix_recv_opts(["--Fvhn"], frozenset())[0])
        self.assertEqual(["foo"], params._fix_recv_opts(["foo"], frozenset())[0])
        self.assertEqual(["v", "n", "F"], params._fix_recv_opts(["v", "n", "F"], frozenset())[0])
        self.assertEqual(["-o", "-n"], params._fix_recv_opts(["-o", "-n"], frozenset())[0])
        self.assertEqual(["-o", "-n"], params._fix_recv_opts(["-o", "-n", "-n"], frozenset())[0])
        self.assertEqual(["-x", "--dryrun"], params._fix_recv_opts(["-x", "--dryrun"], frozenset())[0])
        self.assertEqual(["-x", "--dryrun"], params._fix_recv_opts(["-x", "--dryrun", "-n"], frozenset())[0])
        self.assertEqual(["-x"], params._fix_recv_opts(["-x"], frozenset())[0])

        self.assertEqual([], params._fix_send_opts(["-n"]))
        self.assertEqual([], params._fix_send_opts(["--dryrun", "-n", "-ed"]))
        self.assertEqual([], params._fix_send_opts(["-I", "s1"]))
        self.assertEqual(["--raw"], params._fix_send_opts(["-i", "s1", "--raw"]))
        self.assertEqual(["-X", "d1,d2"], params._fix_send_opts(["-X", "d1,d2"]))
        self.assertEqual(
            ["--exclude", "d1,d2", "--redact", "b1"], params._fix_send_opts(["--exclude", "d1,d2", "--redact", "b1"])
        )

    def test_fix_recv_opts_with_preserve_properties(self) -> None:
        mp = "mountpoint"
        cr = "createtxg"
        params = self.make_params(args=self.argparser_parse_args(args=["src", "dst"]))
        with self.assertRaises(SystemExit):
            params._fix_recv_opts(["-n", "-o", f"{mp}=foo"], frozenset([mp]))
        self.assertEqual(([], [mp]), params._fix_recv_opts(["-n"], frozenset([mp])))
        self.assertEqual((["-u", mp], [mp]), params._fix_recv_opts(["-n", "-u", mp], frozenset([mp])))
        self.assertEqual((["-x", mp], []), params._fix_recv_opts(["-n", "-x", mp], frozenset([mp])))
        self.assertEqual((["-x", mp, "-x", cr], []), params._fix_recv_opts(["-n", "-x", mp, "-x", cr], frozenset([mp])))
        self.assertEqual(([], [cr, mp]), params._fix_recv_opts([], frozenset([mp, cr])))

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

            configuration._delete_stale_files(tmpdir, "s", millis=31 * 24 * 60 * 60 * 1000)

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
                configuration._delete_stale_files(tmpdir, "s", millis=0, ssh=True)
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
                configuration._delete_stale_files(tmpdir, "s", millis=0, ssh=True)
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
            configuration._delete_stale_files(tmpdir, "s", millis=0, ssh=True)
            self.assertFalse(os.path.exists(regular_file))

    def test_lock_file_name_secure_directory(self) -> None:
        """Lock file path should live in a private, non-world-writable directory."""
        args = self.argparser_parse_args(args=["src", "dst"])  # ensure a concrete log dir is configured
        log_params = LogParams(args)
        params = self.make_params(args=args, log_params=log_params)

        lock_file = params.lock_file_name()

        # The lock should be placed under a dedicated per-user locks directory next to the log parent dir
        log_parent_dir = os.path.dirname(log_params.log_dir)
        expected_locks_dir = os.path.join(log_parent_dir, ".locks")
        self.assertTrue(lock_file.startswith(expected_locks_dir + os.sep))

        # The locks directory should exist and be private to the current user (rwx------)
        st_mode = os.stat(expected_locks_dir).st_mode
        self.assertEqual(stat.S_IMODE(st_mode), stat.S_IRWXU)

    def test_custom_ssh_config_file_must_match_file_name_pattern(self) -> None:
        args = self.argparser_parse_args(["src", "dst", "--ssh-src-config-file", "bzfs_ssh_config.cfg"])
        self.make_params(args=args)

        args = self.argparser_parse_args(["src", "dst", "--ssh-src-config-file", "bad_file_name.cfg"])
        with self.assertRaises(SystemExit):
            self.make_params(args=args)

    def test_validate_snapshot_name(self) -> None:
        SnapshotLabel("foo_", "", "", "").validate_label("")
        SnapshotLabel("foo_", "", "", "_foo").validate_label("")
        SnapshotLabel("foo_", "foo_", "", "_foo").validate_label("")
        with self.assertRaises(SystemExit):
            SnapshotLabel("", "", "", "").validate_label("")
        with self.assertRaises(SystemExit):
            SnapshotLabel("foo__", "", "", "").validate_label("")
        with self.assertRaises(SystemExit):
            SnapshotLabel("foo_", "foo__", "", "_foo").validate_label("")
        with self.assertRaises(SystemExit):
            SnapshotLabel("foo_", "foo_", "", "__foo").validate_label("")
        with self.assertRaises(SystemExit):
            SnapshotLabel("foo", "", "", "").validate_label("")
        with self.assertRaises(SystemExit):
            SnapshotLabel("foo_", "foo", "", "").validate_label("")
        with self.assertRaises(SystemExit):
            SnapshotLabel("foo_", "", "", "foo").validate_label("")
        with self.assertRaises(SystemExit):
            SnapshotLabel("foo@bar_", "", "", "").validate_label("")
        with self.assertRaises(SystemExit):
            SnapshotLabel("foo/bar_", "", "", "").validate_label("")
        self.assertTrue(str(SnapshotLabel("foo_", "", "", "")))

    def test_CreateSrcSnapshotConfig(self) -> None:  # noqa: N802
        params = self.make_params(args=bzfs.argument_parser().parse_args(["src", "dst"]))

        good_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"foo": {"onsite": {"adhoc": 1}}}),
                "--create-src-snapshots-timeformat=xxx",
            ]
        )
        config = configuration.CreateSrcSnapshotConfig(good_args, params)
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
        config = configuration.CreateSrcSnapshotConfig(good_args, params)
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
        config = configuration.CreateSrcSnapshotConfig(good_args, params)
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
        config = configuration.CreateSrcSnapshotConfig(good_args, params)
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
        config = configuration.CreateSrcSnapshotConfig(good_args, params)
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
            configuration.CreateSrcSnapshotConfig(bad_args, params)

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
            configuration.CreateSrcSnapshotConfig(bad_args, params)

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
            configuration.CreateSrcSnapshotConfig(bad_args, params)

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
            configuration.CreateSrcSnapshotConfig(bad_args, params)

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
            configuration.CreateSrcSnapshotConfig(bad_args, params)

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
            configuration.CreateSrcSnapshotConfig(bad_args, params)

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
            configuration.CreateSrcSnapshotConfig(bad_args, params)

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
            configuration.CreateSrcSnapshotConfig(bad_args, params)

        good_args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--create-src-snapshots",
                "--create-src-snapshots-plan=" + str({"prod": {"onsite": {"6monthly": 1}}}),
            ]
        )
        configuration.CreateSrcSnapshotConfig(good_args, params)

        args = bzfs.argument_parser().parse_args(["src", "dst"])
        params.daemon_frequency = "2secondly"
        config = configuration.CreateSrcSnapshotConfig(args, params)
        self.assertDictEqual({"_2secondly": (2, "secondly")}, config.suffix_durations)

        args = bzfs.argument_parser().parse_args(["src", "dst"])
        params.daemon_frequency = "2seconds"
        with self.assertRaises(SystemExit):
            configuration.CreateSrcSnapshotConfig(args, params)

        args = bzfs.argument_parser().parse_args(["src", "dst"])
        params.daemon_frequency = "-2secondly"
        with self.assertRaises(SystemExit):
            configuration.CreateSrcSnapshotConfig(args, params)

        args = bzfs.argument_parser().parse_args(["src", "dst"])
        params.daemon_frequency = "2adhoc"
        with self.assertRaises(SystemExit):
            configuration.CreateSrcSnapshotConfig(args, params)

    def test_MonitorSnapshotsConfig(self) -> None:  # noqa: N802
        def plan(alerts: dict[str, Any]) -> str:
            return str({"z": {"onsite": {"100millisecondly": alerts}}})

        params = self.make_params(args=bzfs.argument_parser().parse_args(["src", "dst"]))

        args = bzfs.argument_parser().parse_args(
            ["src", "dst", "--monitor-snapshots=" + plan({"latest": {"warning": "1 millis", "critical": "2 millis"}})]
        )
        config = configuration.MonitorSnapshotsConfig(args, params)
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
        config = configuration.MonitorSnapshotsConfig(args, params)
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
        config = configuration.MonitorSnapshotsConfig(args, params)
        self.assertTrue(str(config))
        self.assertListEqual(
            [(100 + 2, UNIX_TIME_INFINITY_SECS)],
            [(alert.latest.warning_millis, alert.latest.critical_millis) for alert in config.alerts],  # type: ignore
        )
        self.assertListEqual(["z_onsite__100millisecondly"], [str(alert.label) for alert in config.alerts])

        args = bzfs.argument_parser().parse_args(
            ["src", "dst", "--monitor-snapshots=" + plan({"latest": {"critical": "2 millis"}})]
        )
        config = configuration.MonitorSnapshotsConfig(args, params)
        self.assertTrue(str(config))
        self.assertListEqual(
            [(UNIX_TIME_INFINITY_SECS, 100 + 2)],
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
        config = configuration.MonitorSnapshotsConfig(args, params)
        self.assertListEqual([], config.alerts)
        self.assertFalse(config.enable_monitor_snapshots)

        args = bzfs.argument_parser().parse_args(["src", "dst", "--monitor-snapshots=" + plan({"latest": {}})])
        config = configuration.MonitorSnapshotsConfig(args, params)
        self.assertListEqual([], config.alerts)
        self.assertFalse(config.enable_monitor_snapshots)

        args = bzfs.argument_parser().parse_args(["src", "dst", "--monitor-snapshots=" + plan({})])
        config = configuration.MonitorSnapshotsConfig(args, params)
        self.assertListEqual([], config.alerts)
        self.assertFalse(config.enable_monitor_snapshots)

        args = bzfs.argument_parser().parse_args(["src", "dst", "--monitor-snapshots={}"])
        config = configuration.MonitorSnapshotsConfig(args, params)
        self.assertListEqual([], config.alerts)
        self.assertFalse(config.enable_monitor_snapshots)

        args = bzfs.argument_parser().parse_args(
            ["src", "dst", "--monitor-snapshots=" + plan({"latest": {"badalert": "2 millis"}})]
        )
        with self.assertRaises(SystemExit):
            configuration.MonitorSnapshotsConfig(args, params)

        args = bzfs.argument_parser().parse_args(
            ["src", "dst", "--monitor-snapshots=" + plan({"badlatest": {"warning": "2 millis"}})]
        )
        with self.assertRaises(SystemExit):
            configuration.MonitorSnapshotsConfig(args, params)

        args = bzfs.argument_parser().parse_args(
            ["src", "dst", "--monitor-snapshots=" + plan({"latest": {"warning": "2 millisxxxxx"}})]
        )
        with self.assertRaises(SystemExit):
            configuration.MonitorSnapshotsConfig(args, params)

        args = bzfs.argument_parser().parse_args(
            [
                "src",
                "dst",
                "--monitor-snapshots-dont-warn",
                "--monitor-snapshots-dont-crit",
                "--monitor-snapshots=" + plan({"latest": {"warning": "2 millis"}}),
            ]
        )
        config = configuration.MonitorSnapshotsConfig(args, params)
        self.assertTrue(config.dont_warn)
        self.assertTrue(config.dont_crit)


#############################################################################
class TestAdditionalHelpers(AbstractTestCase):

    def test_params_verbose_zfs_and_bwlimit(self) -> None:
        args = self.argparser_parse_args(["src", "dst", "-v", "-v", "--bwlimit", "20m"])
        params = self.make_params(args=args)
        self.assertIn("-v", params.zfs_send_program_opts)
        self.assertIn("-v", params.zfs_recv_program_opts)
        self.assertIn("--rate-limit=20m", params.pv_program_opts)

    def test_program_name_injections(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        p1 = self.make_params(args=args, inject_params={"inject_unavailable_ssh": True})
        self.assertEqual("ssh-xxx", p1._program_name("ssh"))
        p2 = self.make_params(args=args, inject_params={"inject_failing_ssh": True})
        self.assertEqual("false", p2._program_name("ssh"))

    def test_unset_matching_env_vars(self) -> None:
        with patch.dict(os.environ, {"FOO_BAR": "x"}):
            args = self.argparser_parse_args(["src", "dst", "--exclude-envvar-regex", "FOO.*"])
            params = self.make_params(args=args)
            params._unset_matching_env_vars(args)
            self.assertNotIn("FOO_BAR", os.environ)

    def test_local_ssh_command_variants(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = os.path.join(tmpdir, "bzfs_ssh_config")
            Path(cfg).touch()
            args = self.argparser_parse_args(["src", "dst", "--ssh-src-config-file", cfg])
            args.ssh_src_port = 2222
            p = self.make_params(args=args)
            r = Remote("src", args, p)
            r.ssh_user_host = "user@host"
            r.ssh_user = "user"
            r.ssh_host = "host"
            cmd = r.local_ssh_command(None)
            self.assertEqual(cmd[0], p.ssh_program)
            self.assertIn("-F", cmd)
            self.assertIn(cfg, cmd)
            self.assertIn("-p", cmd)
            self.assertIn("2222", cmd)
            self.assertIn("-S", cmd)
            self.assertEqual("user@host", cmd[-1])

            r.reuse_ssh_connection = False
            cmd = r.local_ssh_command(None)
            self.assertNotIn("-S", cmd)

            args = self.argparser_parse_args(["src", "dst", "--ssh-program", "-"])
            p = self.make_params(args=args)
            r = Remote("src", args, p)
            r.ssh_user_host = "u@h"
            with self.assertRaises(SystemExit):
                r.local_ssh_command(None)  # Cannot talk to remote host because ssh CLI is disabled
            r.ssh_user_host = ""
            self.assertEqual([], r.local_ssh_command(None))

    def test_params_zfs_recv_program_opt(self) -> None:
        args = self.argparser_parse_args(
            ["src", "dst", "--zfs-recv-program-opt=-o", "--zfs-recv-program-opt=org.test=value"]
        )
        params = self.make_params(args=args)
        self.assertIn("-o", params.zfs_recv_program_opts)
        self.assertIn("org.test=value", params.zfs_recv_program_opts)

    def test_copy_properties_config_repr(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        params = self.make_params(args=args)
        config = configuration.CopyPropertiesConfig("zfs_recv_o", "-o", args, params)
        rep = repr(config)
        self.assertIn("sources", rep)
        self.assertIn("targets", rep)

    def test_pv_program_opts_disallows_dangerous_options(self) -> None:
        """Confirms that initialization fails if --pv-program-opts contains the forbidden -f or --log-file options."""
        # Test Case 1: The short-form option '-f' should be rejected.
        malicious_opts_short = "--bytes -f /etc/hosts"
        args_short = self.argparser_parse_args(["src", "dst", f"--pv-program-opts={malicious_opts_short}"])
        with self.assertRaises(SystemExit):
            self.make_params(args=args_short)

        # Test Case 2: The long-form option '--log-file' should be rejected.
        malicious_opts_long = "--progress --log-file /etc/shadow"
        args_long = self.argparser_parse_args(["src", "dst", f"--pv-program-opts={malicious_opts_long}"])
        with self.assertRaises(SystemExit):
            self.make_params(args=args_long)

        # Test Case 3: A valid set of options should instantiate successfully.
        valid_opts = "--bytes --progress --rate"
        args_valid = self.argparser_parse_args(["src", "dst", f"--pv-program-opts={valid_opts}"])

        # This should not raise an exception.
        params_valid = self.make_params(args=args_valid)
        self.assertIn("--rate", params_valid.pv_program_opts)

    def test_compression_program_opts_disallows_dangerous_options(self) -> None:
        malicious_opts_short = "-o /etc/hosts"
        args_short = self.argparser_parse_args(["src", "dst", f"--compression-program-opts={malicious_opts_short}"])
        with self.assertRaises(SystemExit):
            self.make_params(args=args_short)

        malicious_opts_long = "--output-file /etc/hosts"
        args_long = self.argparser_parse_args(["src", "dst", f"--compression-program-opts={malicious_opts_long}"])
        with self.assertRaises(SystemExit):
            self.make_params(args=args_long)

        valid_opts = "-9"
        args_valid = self.argparser_parse_args(["src", "dst", f"--compression-program-opts={valid_opts}"])
        params_valid = self.make_params(args=args_valid)
        self.assertIn("-9", params_valid.compression_program_opts)

    def test_mbuffer_program_opts_disallows_dangerous_options(self) -> None:
        malicious_opts = "-o /etc/hosts"
        args_short = self.argparser_parse_args(["src", "dst", f"--mbuffer-program-opts={malicious_opts}"])
        with self.assertRaises(SystemExit):
            self.make_params(args=args_short)

        valid_opts = "-q"
        args_valid = self.argparser_parse_args(["src", "dst", f"--mbuffer-program-opts={valid_opts}"])
        params_valid = self.make_params(args=args_valid)
        self.assertIn("-q", params_valid.mbuffer_program_opts)

    def test_logparams_handles_setup_cleanup_race(self) -> None:
        """Simulates a race where another bzfs instance deletes a sibling '.current/<run-id>/' directory while this process
        is still assembling its symlink set.

        That can surface as FileNotFoundError during the temporary 'current' symlink setup. The expected behavior is robust
        tolerance: no crash, proceed without updating the convenience 'current' pointer, and still produce a usable log file.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            log_parent_dir = os.path.join(tmpdir, "bzfs-logs")
            os.makedirs(log_parent_dir, exist_ok=True)

            def raise_and_remove_dir(src: str, dst_dir: str, dst: str) -> None:  # matches _create_symlink signature
                # Simulate a race: directory was removed between creation and symlink placement
                shutil.rmtree(dst_dir, ignore_errors=True)
                raise FileNotFoundError("simulated race: directory removed")

            args = self.argparser_parse_args(["src", "dst", "--log-dir", log_parent_dir])
            with patch("bzfs_main.configuration._create_symlink", side_effect=raise_and_remove_dir):
                lp = configuration.LogParams(args)  # must not raise

            # The specific 'current' symlink may or may not exist; key expectation is no crash and log file created
            self.assertTrue(os.path.isfile(lp.log_file))
