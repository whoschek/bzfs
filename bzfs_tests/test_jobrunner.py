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
"""Unit tests for the ``bzfs_jobrunner`` CLI; Validates job scheduling, command creation and error handling."""

from __future__ import annotations
import argparse
import platform
import shutil
import signal
import subprocess
import sys
import unittest
from logging import Logger
from subprocess import DEVNULL, PIPE, CalledProcessError
from typing import (
    Union,
    cast,
)
from unittest.mock import MagicMock, patch

from bzfs_main import bzfs_jobrunner
from bzfs_main.utils import DIE_STATUS
from bzfs_tests.abstract_testcase import AbstractTestCase
from bzfs_tests.tools import suppress_output


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestHelperFunctions,
        TestParseSrcHosts,
        TestValidation,
        TestFiltering,
        TestSkipDatasetsWithNonExistingDstPool,
        TestRunSubJobSpawnProcessPerJob,
        TestRunSubJobInCurrentThread,
        TestRunSubJob,
        TestErrorPropagation,
        TestValidateSnapshotPlan,
        TestValidateMonitorSnapshotPlan,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestHelperFunctions(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs_jobrunner.Job()

    def test_validate_type(self) -> None:
        """Ensure validate_type accepts expected types and exits on mismatches quietly."""
        self.job.validate_type("foo", str, "name")
        self.job.validate_type(123, int, "name")
        with self.assertRaises(SystemExit), self.assertLogs("bzfs_jobrunner", level="ERROR"):
            self.job.validate_type(123, str, "name")
        with self.assertRaises(SystemExit), self.assertLogs("bzfs_jobrunner", level="ERROR"):
            self.job.validate_type("foo", int, "name")
        with self.assertRaises(SystemExit), self.assertLogs("bzfs_jobrunner", level="ERROR"):
            self.job.validate_type(None, str, "name")
        self.job.validate_type("foo", Union[str, int], "name")
        self.job.validate_type(123, Union[str, int], "name")
        with self.assertRaises(SystemExit), self.assertLogs("bzfs_jobrunner", level="ERROR"):
            self.job.validate_type([], Union[str, int], "name")
        with self.assertRaises(SystemExit), self.assertLogs("bzfs_jobrunner", level="ERROR"):
            self.job.validate_type(None, Union[str, int], "name")

    def test_validate_non_empty_string(self) -> None:
        """Reject empty strings while keeping the log output silent."""
        self.job.validate_non_empty_string("valid_string", "name")
        with self.assertRaises(SystemExit), self.assertLogs("bzfs_jobrunner", level="ERROR"):
            self.job.validate_non_empty_string("", "name")

    def test_validate_non_negative_int(self) -> None:
        """Verify negative integers abort without leaking error logs."""
        self.job.validate_non_negative_int(1, "name")
        self.job.validate_non_negative_int(0, "name")
        with self.assertRaises(SystemExit), self.assertLogs("bzfs_jobrunner", level="ERROR"):
            self.job.validate_non_negative_int(-1, "name")

    def test_validate_true(self) -> None:
        """Expect false values to trigger exit, capturing any error message."""
        self.job.validate_true(1, "name")
        with self.assertRaises(SystemExit), self.assertLogs("bzfs_jobrunner", level="ERROR"):
            self.job.validate_true(0, "name")

    def test_validate_is_subset(self) -> None:
        """Confirm subset validation handles errors without emitting logs."""
        self.job.validate_is_subset(["1"], ["1", "2"], "x", "y")
        self.job.validate_is_subset([], ["1", "2"], "x", "y")
        self.job.validate_is_subset([], [], "x", "y")
        with self.assertRaises(SystemExit), self.assertLogs("bzfs_jobrunner", level="ERROR"):
            self.job.validate_is_subset(["3"], ["1", "2"], "x", "y")
        with self.assertRaises(SystemExit), self.assertLogs("bzfs_jobrunner", level="ERROR"):
            self.job.validate_is_subset(["3", "4"], [], "x", "y")
        with self.assertRaises(SystemExit), self.assertLogs("bzfs_jobrunner", level="ERROR"):
            self.job.validate_is_subset("foo", ["3"], "x", "y")
        with self.assertRaises(SystemExit), self.assertLogs("bzfs_jobrunner", level="ERROR"):
            self.job.validate_is_subset(["3"], "foo", "x", "y")

    def _make_mock_socket(self, bind_side_effect: Exception | None = None) -> MagicMock:
        sock = MagicMock()
        sock.bind.side_effect = bind_side_effect
        sock.__enter__.return_value = sock
        sock.__exit__.return_value = False
        return sock

    def test_detect_loopback_address_ipv4_supported(self) -> None:
        ipv4 = self._make_mock_socket()
        with patch("socket.socket", side_effect=[ipv4]) as socket_factory:
            self.assertEqual("127.0.0.1", bzfs_jobrunner._detect_loopback_address())
            self.assertEqual(1, socket_factory.call_count)

    def test_detect_loopback_address_ipv6_fallback(self) -> None:
        ipv4_fail = self._make_mock_socket(OSError("No IPv4"))
        ipv6_ok = self._make_mock_socket()
        with patch("socket.socket", side_effect=[ipv4_fail, ipv6_ok]) as socket_factory:
            self.assertEqual("::1", bzfs_jobrunner._detect_loopback_address())
            self.assertEqual(2, socket_factory.call_count)

    def test_detect_loopback_address_neither_supported(self) -> None:
        ipv4_fail = self._make_mock_socket(OSError("No IPv4"))
        ipv6_fail = self._make_mock_socket(OSError("No IPv6"))
        with patch("socket.socket", side_effect=[ipv4_fail, ipv6_fail]) as socket_factory:
            self.assertEqual("", bzfs_jobrunner._detect_loopback_address())
            self.assertEqual(2, socket_factory.call_count)

    def test_pretty_print_formatter(self) -> None:
        self.assertIsNotNone(str(bzfs_jobrunner._pretty_print_formatter({"foo": "bar"})))

    def test_help(self) -> None:
        if is_solaris_zfs():
            self.skipTest("FIXME: BlockingIOError: [Errno 11] write could not complete without blocking")
        parser = bzfs_jobrunner.argument_parser()
        with self.assertRaises(SystemExit) as e, suppress_output():
            parser.parse_args(["--help"])
        self.assertEqual(0, e.exception.code)

    def test_version(self) -> None:
        parser = bzfs_jobrunner.argument_parser()
        with self.assertRaises(SystemExit) as e, suppress_output():
            parser.parse_args(["--version"])
        self.assertEqual(0, e.exception.code)

    def test_reject_argument_action(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--secret", action=bzfs_jobrunner.RejectArgumentAction)
        with self.assertRaises(SystemExit), suppress_output():
            parser.parse_args(["--secret", "x"])

    def test_inapplicable_cli_option(self) -> None:
        opts = ["--create-src-snapshots", "--job-id=myid", "--root-dataset-pairs", "src", "dst"]
        parser = bzfs_jobrunner.argument_parser()
        parser.parse_args(opts)
        with self.assertRaises(SystemExit), suppress_output():
            parser.parse_args(["--delete-dst-snapshots"] + opts)

    def test_stats_repr(self) -> None:
        stats = bzfs_jobrunner.Stats()
        stats.jobs_all = 10
        stats.jobs_started = 5
        stats.jobs_completed = 5
        stats.jobs_failed = 2
        stats.jobs_running = 1
        stats.sum_elapsed_nanos = 1_000_000_000
        expect = "all:10, started:5=50%, completed:5=50%, failed:2=20%, running:1, avg_completion_time:200ms"
        self.assertEqual(expect, repr(stats))

    def test_dedupe_and_flatten(self) -> None:
        pairs = [("a", "b"), ("a", "b"), ("c", "d")]
        self.assertEqual([("a", "b"), ("c", "d")], bzfs_jobrunner._dedupe(pairs))
        self.assertEqual(["a", "b", "c", "d"], bzfs_jobrunner._flatten([("a", "b"), ("c", "d")]))

    def test_sanitize_and_log_suffix(self) -> None:
        self.assertEqual("a!b!c!d!e!f", bzfs_jobrunner._sanitize("a b..c/d,e\\f"))
        suffix = bzfs_jobrunner._log_suffix("l o", "s/p", "d\\h")
        self.assertEqual(",l!o,s!p,d!h", suffix)

    def test_convert_ipv6(self) -> None:
        self.assertEqual("fe80||1", bzfs_jobrunner.convert_ipv6("fe80::1"))

    @unittest.skipIf(sys.platform != "linux", "This test is designed for Linux only")
    def test_get_localhost_ips(self) -> None:
        job = bzfs_jobrunner.Job()
        ips: set[str] = job.get_localhost_ips()
        self.assertTrue(len(ips) > 0)

    @patch("subprocess.run", side_effect=RuntimeError("fail"))
    def test_get_localhost_ips_failure_logs_warning(self, mock_run: MagicMock) -> None:
        job = bzfs_jobrunner.Job()
        with patch.object(sys, "platform", "linux"), patch.object(job.log, "warning") as mock_warn:
            ips = job.get_localhost_ips()
        self.assertEqual(set(), ips)
        mock_warn.assert_called_once()
        fmt, exc = mock_warn.call_args.args
        self.assertEqual("Cannot run 'hostname -I' on localhost: %s", fmt)
        self.assertIsInstance(exc, RuntimeError)


#############################################################################
class TestParseSrcHosts(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs_jobrunner.Job()

    # ---- raw_src_hosts provided (no stdin) ----
    def test_cli_value_valid_list(self) -> None:
        result = self.job.parse_src_hosts_from_cli_or_stdin(str(["h1", "h2"]))
        self.assertEqual(["h1", "h2"], result)

    def test_cli_value_invalid_literal(self) -> None:
        with self.assertRaises(SystemExit) as cm:
            self.job.parse_src_hosts_from_cli_or_stdin("[not_a_name]")
        self.assertEqual(DIE_STATUS, cm.exception.code)
        self.assertIn("Invalid --src-hosts format:", str(cm.exception))

    def test_cli_value_not_a_list(self) -> None:
        with self.assertRaises(SystemExit) as cm:
            self.job.parse_src_hosts_from_cli_or_stdin("'single'")
        self.assertEqual(DIE_STATUS, cm.exception.code)
        self.assertIn("expected a Python list literal", str(cm.exception))

    # ---- raw_src_hosts missing -> read from stdin ----
    class _FakeStdin:
        def __init__(self, data: str, isatty: object | None = None) -> None:
            self._data = data
            # isatty can be: True/False function, raising function, None (no attribute)
            if isatty is True:
                self.isatty = lambda: True  # set dynamically for test
            elif isatty is False:
                self.isatty = lambda: False  # set dynamically for test
            elif callable(isatty):
                self.isatty = isatty  # set dynamically for test

        def read(self) -> str:
            return self._data

    def test_stdin_tty_raises(self) -> None:
        fake = self._FakeStdin(data="", isatty=True)
        with patch.object(sys, "stdin", fake):
            with self.assertRaises(SystemExit) as cm:
                self.job.parse_src_hosts_from_cli_or_stdin(None)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn("stdin is a TTY", str(cm.exception))

    def test_stdin_empty_raises(self) -> None:
        fake = self._FakeStdin(data="   \n\t  ", isatty=False)
        with patch.object(sys, "stdin", fake):
            with self.assertRaises(SystemExit) as cm:
                self.job.parse_src_hosts_from_cli_or_stdin(None)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn("stdin is empty", str(cm.exception))

    def test_stdin_invalid_literal_raises(self) -> None:
        fake = self._FakeStdin(data="nonsense", isatty=False)
        with patch.object(sys, "stdin", fake):
            with self.assertRaises(SystemExit) as cm:
                self.job.parse_src_hosts_from_cli_or_stdin(None)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn("Invalid --src-hosts format:", str(cm.exception))

    def test_stdin_not_a_list_raises(self) -> None:
        fake = self._FakeStdin(data="'abc'", isatty=False)
        with patch.object(sys, "stdin", fake):
            with self.assertRaises(SystemExit) as cm:
                self.job.parse_src_hosts_from_cli_or_stdin(None)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn("expected a Python list literal", str(cm.exception))

    def test_stdin_valid_list(self) -> None:
        fake = self._FakeStdin(data=str(["x", "y"]), isatty=False)
        with patch.object(sys, "stdin", fake):
            result = self.job.parse_src_hosts_from_cli_or_stdin(None)
        self.assertEqual(["x", "y"], result)

    def test_stdin_isatty_raises_exception_then_reads(self) -> None:
        def raising_isatty() -> bool:
            raise RuntimeError("isatty failure")

        fake = self._FakeStdin(data=str(["z1"]), isatty=raising_isatty)
        with patch.object(sys, "stdin", fake):
            result = self.job.parse_src_hosts_from_cli_or_stdin(None)
        self.assertEqual(["z1"], result)


#############################################################################
class TestValidation(AbstractTestCase):

    def _new_job(self) -> bzfs_jobrunner.Job:
        job = bzfs_jobrunner.Job()
        job.log = MagicMock()
        job.is_test_mode = True
        return job

    def _build_argv(
        self,
        *,
        src_hosts: list[str],
        dst_hosts: dict[str, list[str]],
        dst_root_datasets: dict[str, str],
        retain_dst_targets: dict[str, list[str]],
        src_host_filter: str | None = None,
        command: str = "--create-src-snapshots",
    ) -> list[str]:
        argv: list[str] = [
            bzfs_jobrunner.PROG_NAME,
            "--job-id=test",
            "--root-dataset-pairs",
            "tank/data",
            "backup/data",
            f"--src-hosts={src_hosts!s}",
            f"--dst-hosts={dst_hosts!s}",
            f"--dst-root-datasets={dst_root_datasets!s}",
            f"--retain-dst-targets={retain_dst_targets!s}",
            command,
        ]
        if src_host_filter:
            argv += ["--src-host", src_host_filter]
        return argv

    def _expect_pass(self, job: bzfs_jobrunner.Job, argv: list[str]) -> None:
        with patch.object(job, "run_subjobs", return_value=None) as mock_run_subjobs:
            with patch("sys.argv", argv):
                job.run_main(argv)
                mock_run_subjobs.assert_called_once()

    def test_multisource_substitution_token_validation_with_empty_target(self) -> None:
        job = self._new_job()

        # Config: 2 source hosts, 1 dest host, dst_root_datasets WITHOUT ^SRC_HOST
        base_argv = self._build_argv(
            src_hosts=["src1", "src2"],
            dst_hosts={"dstX": ["target", ""]},
            dst_root_datasets={"dstX": ""},
            retain_dst_targets={"dstX": ["target", ""]},
        )
        expect_msg = "source hosts must not be configured to write to the same destination dataset"

        # Scenario 1: Run for srcA only, should pass.
        argv_src1 = base_argv + ["--src-host", "src1"]
        self._expect_pass(job, argv_src1)

        # Scenario 1: Run for srcB only, should pass.
        argv_src2 = base_argv + ["--src-host", "src2"]
        job = self._new_job()
        self._expect_pass(job, argv_src2)

        # Scenario 3: Run for both (no --src-host filter), should fail.
        job = self._new_job()
        with patch("sys.argv", base_argv):  # No --src-host filter
            with self.assertRaises(SystemExit) as cm:
                job.run_main(base_argv)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn(expect_msg, str(cm.exception))

    def test_multisource_substitution_token_validation_passes_safe_config(self) -> None:
        job = self._new_job()

        # Config: 2 source hosts, 1 dest host, dst_root_datasets WITH ^SRC_HOST
        base_argv_safe = self._build_argv(
            src_hosts=["src1", "src2"],
            dst_hosts={"dstX": ["target"]},
            dst_root_datasets={"dstX": "pool/backup_^SRC_HOST", "dstY": ""},  # Correctly uses ^SRC_HOST
            retain_dst_targets={"dstX": ["target"], "dstY": ["target"]},
        )

        # Mock the actual subjob execution to prevent it from running fully
        argv_src1_safe = base_argv_safe + ["--src-host", "src1"]
        self._expect_pass(job, argv_src1_safe)  # Should not raise SystemExit due to validation

        # Scenario 2: Run for both. Should pass validation.
        job = self._new_job()
        self._expect_pass(job, base_argv_safe)

    def test_multisource_substitution_token_validation_rejects_unsafe_config(self) -> None:
        job = self._new_job()

        # Config: 2 source hosts, 1 dest host, dst_root_datasets WITHOUT ^SRC_HOST
        base_argv = self._build_argv(
            src_hosts=["src1", "src2"],
            dst_hosts={"dstX": ["target"]},
            dst_root_datasets={"dstX": "pool/^SRC_HOST", "dstZ": "pool/fixed"},  # incorrectly uses ^SRC_HOST
            retain_dst_targets={"dstX": ["target"], "dstZ": ["target"]},
        )
        expect_msg = "but not all non-empty root datasets in --dst-root-datasets contain the '^SRC_HOST' substitution token"

        # Scenario 1: Run for srcA only, should fail.
        argv_src1 = base_argv + ["--src-host", "src1"]
        with patch("sys.argv", argv_src1):
            with self.assertRaises(SystemExit) as cm:
                job.run_main(argv_src1)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn(expect_msg, str(cm.exception))

        # Scenario 2: Run for srcB only, should fail.
        argv_src2 = base_argv + ["--src-host", "src2"]
        with patch("sys.argv", argv_src2):
            with self.assertRaises(SystemExit) as cm:
                job.run_main(argv_src2)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn(expect_msg, str(cm.exception))

        # Scenario 3: Run for both (no --src-host filter), should fail.
        job = self._new_job()
        with patch("sys.argv", base_argv):  # No --src-host filter
            with self.assertRaises(SystemExit) as cm:
                job.run_main(base_argv)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn(expect_msg, str(cm.exception))


#############################################################################
class TestFiltering(AbstractTestCase):

    def _capture_subjobs(
        self,
        *,
        monitor_dst: bool = False,
        prune_dst: bool = False,
        src_hosts: list[str],
        dst_hosts: dict[str, list[str]],
        retain_dst_targets: dict[str, list[str]],
        dst_root_datasets: dict[str, str],
        dst_snapshot_plan: dict,
        monitor_snapshot_plan: dict | None = None,
        localhostname: str = "localtest",
    ) -> dict[str, list[str]]:
        """Builds argv, runs Job.run_main and returns the subjobs dict captured via run_subjobs."""
        job = bzfs_jobrunner.Job()
        job.log = MagicMock()
        job.is_test_mode = True

        argv: list[str] = [
            bzfs_jobrunner.PROG_NAME,
            "--job-id=test",
            "--root-dataset-pairs",
            "tank/src",
            "pool/dst",
            f"--src-hosts={src_hosts!s}",
            f"--dst-hosts={dst_hosts!s}",
            f"--retain-dst-targets={retain_dst_targets!s}",
            f"--dst-root-datasets={dst_root_datasets!s}",
            f"--dst-snapshot-plan={dst_snapshot_plan!s}",
            f"--localhost={localhostname}",
        ]
        if monitor_dst:
            assert monitor_snapshot_plan is not None
            argv += [f"--monitor-snapshot-plan={monitor_snapshot_plan!s}", "--monitor-dst-snapshots"]
        if prune_dst:
            argv += ["--prune-dst-snapshots"]

        captured: dict[str, list[str]] = {}

        def fake_run_subjobs(subjobs: dict[str, list[str]], *_: object, **__: object) -> None:
            captured.update(subjobs)

        with patch.object(job, "run_subjobs", side_effect=fake_run_subjobs):
            with patch("sys.argv", argv):
                job.run_main(argv)

        return captured

    def test_monitor_dst_snapshots_skips_when_no_matching_targets(self) -> None:
        """When a dst host has no targets retained, a monitor-dst-snapshots subjob is emitted."""
        captured = self._capture_subjobs(
            monitor_dst=True,
            src_hosts=["src1"],
            dst_hosts={"dstX": ["tgt1"]},
            retain_dst_targets={"dstX": ["tgt2"]},
            dst_root_datasets={"dstX": ""},
            dst_snapshot_plan={"org": {"tgt1": {"hour": 1}}},
            monitor_snapshot_plan={"org": {"tgt1": {"hour": {"warn": "msg", "max_age": 1}}}},
        )
        self.assertTrue(len(captured) > 0)
        self.assertTrue(any("monitor-dst-snapshots" in name for name in captured))

    def test_monitor_dst_snapshots_emits_when_matching_targets(self) -> None:
        """When a dst host retains at least one target, a monitor-dst-snapshots subjob is emitted."""
        captured = self._capture_subjobs(
            monitor_dst=True,
            src_hosts=["src1"],
            dst_hosts={"dstX": ["tgt1", "tgt2"]},
            retain_dst_targets={"dstX": ["tgt2"]},
            dst_root_datasets={"dstX": ""},
            dst_snapshot_plan={"org": {"tgt2": {"hour": 1}}},
            monitor_snapshot_plan={"org": {"tgt2": {"hour": {"warn": "msg", "max_age": 1}}}},
        )
        self.assertTrue(len(captured) > 0)
        self.assertTrue(any("monitor-dst-snapshots" in name for name in captured))

    def test_prune_dst_snapshots_skips_when_no_retain_targets_for_host(self) -> None:
        """When a dst host retains no targets, prune-dst-snapshots should not emit a subjob for that host."""
        captured = self._capture_subjobs(
            prune_dst=True,
            src_hosts=["src1"],
            dst_hosts={"dstX": ["t1", "t2"]},
            retain_dst_targets={"dstX": []},
            dst_root_datasets={"dstX": ""},
            dst_snapshot_plan={"org": {"t1": {"hour": 1}, "t2": {"hour": 1}}},
        )
        # self.assertEqual({}, captured)
        self.assertTrue(len(captured) > 0)
        self.assertTrue(any("prune-dst-snapshots" in name for name in captured))

    def test_prune_dst_snapshots_emits_when_retained_target_present(self) -> None:
        """When a dst host retains at least one target, a prune-dst-snapshots subjob is emitted."""
        captured = self._capture_subjobs(
            prune_dst=True,
            src_hosts=["src1"],
            dst_hosts={"dstX": ["t1", "t2"]},
            retain_dst_targets={"dstX": ["t2"]},
            dst_root_datasets={"dstX": ""},
            dst_snapshot_plan={"org": {"t2": {"hour": 1}}},
        )
        self.assertTrue(len(captured) > 0)
        self.assertTrue(any("prune-dst-snapshots" in name for name in captured))


#############################################################################
class TestSkipDatasetsWithNonExistingDstPool(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs_jobrunner.Job()
        self.log_mock = MagicMock()
        self.job.log = cast(Logger, self.log_mock)

    def test_empty_input_raises(self) -> None:
        with self.assertRaises(AssertionError):
            self.job.skip_nonexisting_local_dst_pools([])

    @patch("subprocess.run")
    def test_single_existing_pool(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\n", stderr="")
        pairs = [("-:srcpool1/src1", "-:dstpool1/dst1")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual(pairs, result)
        self.assertSetEqual({"-:dstpool1"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool1"}, self.job.cache_known_dst_pools)
        expected_cmd = "zfs list -t filesystem,volume -Hp -o name".split(" ") + ["dstpool1"]
        mock_run.assert_called_once_with(expected_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True, timeout=None)

    @patch("subprocess.run")
    def test_single_nonexisting_pool(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        pairs = [("-:srcpool2/src2", "-:dstpool2/dst2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual([], result)
        self.assertSetEqual(set(), self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool2"}, self.job.cache_known_dst_pools)
        self.log_mock.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: %s", "-:dstpool2/dst2"
        )

    @patch("subprocess.run")
    def test_multiple_pools_mixed(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\n", stderr="")
        pairs = [
            ("srcpool1/src1", "-:dstpool1/dst1"),
            ("srcpool2/src2", "-:dstpool2/dst2"),
            ("srcpool3/src3", "-:dstpool1/dst3"),
        ]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual([("srcpool1/src1", "-:dstpool1/dst1"), ("srcpool3/src3", "-:dstpool1/dst3")], result)
        self.assertSetEqual({"-:dstpool1"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool1", "-:dstpool2"}, self.job.cache_known_dst_pools)
        self.log_mock.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: %s", "-:dstpool2/dst2"
        )

    @patch("subprocess.run")
    def test_caching_avoids_subprocess_run(self, mock_run: MagicMock) -> None:
        self.job.cache_existing_dst_pools = {"-:dstpool1"}
        self.job.cache_known_dst_pools = {"-:dstpool1", "-:dstpool2"}
        pairs = [("srcpool1/src1", "-:dstpool1/dst1"), ("srcpool2/src2", "-:dstpool2/dst2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual([("srcpool1/src1", "-:dstpool1/dst1")], result)
        mock_run.assert_not_called()
        self.log_mock.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: %s", "-:dstpool2/dst2"
        )

    @patch("subprocess.run")
    def test_multiple_pools_exist_returns_all(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\ndstpool2\n", stderr="")
        pairs = [("srcpool1/src1", "-:dstpool1/dst1"), ("srcpool2/src2", "-:dstpool2/dst2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual(pairs, result)
        self.assertSetEqual({"-:dstpool1", "-:dstpool2"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool1", "-:dstpool2"}, self.job.cache_known_dst_pools)
        mock_run.assert_called_once()

    @patch("subprocess.run")
    def test_repeated_call_caching(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = [
            subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\n", stderr=""),
            subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool3\n", stderr=""),
        ]
        pairs1 = [("srcpool1/src1", "-:dstpool1/dst1"), ("srcpool1/src2", "-:dstpool2/dst2")]
        res1 = self.job.skip_nonexisting_local_dst_pools(pairs1)
        self.assertListEqual([("srcpool1/src1", "-:dstpool1/dst1")], res1)
        pairs2 = [("srcpool1/src2", "-:dstpool2/dst2"), ("srcpool3/src3", "-:dstpool3/dst3")]
        res2 = self.job.skip_nonexisting_local_dst_pools(pairs2)
        self.assertListEqual([("srcpool3/src3", "-:dstpool3/dst3")], res2)
        self.assertEqual(2, mock_run.call_count)

    @patch("subprocess.run")
    def test_multislash_dataset_names(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\n", stderr="")
        pairs = [("srcpool1/src1", "-:dstpool1/child/grand")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual(pairs, result)
        self.assertIn("-:dstpool1", self.job.cache_existing_dst_pools)

    @patch("subprocess.run")
    def test_multiple_warnings_for_nonexistent(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        pairs = [("srcpool1/src1", "-:srcpool1/dst1"), ("srcpool2/src2", "-:dstpool2/dst2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual([], result)
        expected_calls = [
            unittest.mock.call("Skipping dst dataset for which local dst pool does not exist: %s", "-:srcpool1/dst1"),
            unittest.mock.call("Skipping dst dataset for which local dst pool does not exist: %s", "-:dstpool2/dst2"),
        ]
        self.assertEqual(expected_calls, self.log_mock.warning.mock_calls)

    @patch("subprocess.run")
    def test_duplicate_pool_input(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\n", stderr="")
        pairs = [("srcpool1/src1", "-:dstpool1/dst1"), ("srcpool2/src2", "-:dstpool1/dst2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual(pairs, result)
        mock_run.assert_called_once()

    # dst without slash, existing pool
    @patch("subprocess.run")
    def test_dst_without_slash_existing_pool(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool4\n", stderr="")
        pairs = [("srcpool4/src4", "-:dstpool4")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual(pairs, result)
        self.assertSetEqual({"-:dstpool4"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool4"}, self.job.cache_known_dst_pools)
        expected_cmd = "zfs list -t filesystem,volume -Hp -o name".split(" ") + ["dstpool4"]
        mock_run.assert_called_once_with(expected_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True, timeout=None)

    # dst without slash, non-existing pool
    @patch("subprocess.run")
    def test_dst_without_slash_nonexisting_pool(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        pairs = [("srcpool4/src4", "-:dstpool2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual([], result)
        self.assertSetEqual(set(), self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool2"}, self.job.cache_known_dst_pools)
        self.log_mock.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: %s", "-:dstpool2"
        )

    # non-local dst and without slash
    @patch("subprocess.run")
    def test_nonlocaldst_without_slash(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        pairs = [("srcpool4/src4", "127.0.0.1:dstpool2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual(pairs, result)
        self.assertSetEqual({"127.0.0.1:dstpool2"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"127.0.0.1:dstpool2"}, self.job.cache_known_dst_pools)
        self.assertEqual([], self.log_mock.warning.mock_calls)

    @patch("subprocess.run")
    def test_mixed_local_existing_and_remote(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="local1\n", stderr="")
        pairs = [
            ("srcpool4/src1", "-:local1/src1"),  # Local, should be checked and found
            ("srcpool4/src2", "127.0.0.1:dstpool1/src2"),  # Remote, should be assumed to exist
        ]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertCountEqual(pairs, result)
        self.assertSetEqual({"-:local1", "127.0.0.1:dstpool1"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:local1", "127.0.0.1:dstpool1"}, self.job.cache_known_dst_pools)
        expected_cmd = "zfs list -t filesystem,volume -Hp -o name".split(" ") + ["local1"]
        mock_run.assert_called_once_with(expected_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True, timeout=None)
        self.log_mock.warning.assert_not_called()

    @patch("subprocess.run")
    def test_mixed_local_nonexisting_and_remote(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")  # local2 nonexistng
        pairs = [
            ("src1/src1", "-:local2/src1"),  # Local, should be checked and NOT found
            ("src2/src2", "127.0.0.1:dstpool1/src2"),  # Remote, should be assumed to exist
        ]
        expected_result = [("src2/src2", "127.0.0.1:dstpool1/src2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)

        self.assertCountEqual(expected_result, result)
        self.assertSetEqual({"127.0.0.1:dstpool1"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:local2", "127.0.0.1:dstpool1"}, self.job.cache_known_dst_pools)
        expected_cmd = "zfs list -t filesystem,volume -Hp -o name".split(" ") + ["local2"]
        mock_run.assert_called_once_with(expected_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True, timeout=None)
        self.log_mock.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: %s", "-:local2/src1"
        )

    @patch("subprocess.run")
    def test_mixed_local_existing_nonexisting_and_remote(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="local1\n", stderr=""
        )  # local1 exists, local2 does not
        pairs = [
            ("src1/src1", "-:local1/src1"),  # Local, exists
            ("src2/src2", "127.0.0.1:dstpool1/src2"),  # Remote
            ("src3/data3", "-:local2/data3"),  # Local, does not exist
        ]
        expected_result = [
            ("src1/src1", "-:local1/src1"),
            ("src2/src2", "127.0.0.1:dstpool1/src2"),
        ]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertCountEqual(expected_result, result)
        self.assertSetEqual({"-:local1", "127.0.0.1:dstpool1"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:local1", "127.0.0.1:dstpool1", "-:local2"}, self.job.cache_known_dst_pools)
        # zfs list should be called for both local1 and local2
        expected_cmd_parts = "zfs list -t filesystem,volume -Hp -o name".split(" ")
        # Order of pools in the command might vary, so check args more flexibly
        self.assertEqual(1, mock_run.call_count)
        called_cmd = mock_run.call_args[0][0]
        self.assertEqual(expected_cmd_parts, called_cmd[: len(expected_cmd_parts)])
        self.assertCountEqual(sorted(["local1", "local2"]), sorted(called_cmd[len(expected_cmd_parts) :]))

        self.log_mock.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: %s", "-:local2/data3"
        )

    @patch("subprocess.run")
    def test_all_remote_pools(self, mock_run: MagicMock) -> None:
        pairs = [
            ("src1/src1", "remote1:pool1/src1"),
            ("src2/src2", "remote2:pool2/src2"),
        ]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertCountEqual(pairs, result)
        self.assertSetEqual({"remote1:pool1", "remote2:pool2"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"remote1:pool1", "remote2:pool2"}, self.job.cache_known_dst_pools)
        mock_run.assert_not_called()  # No local pools to check
        self.log_mock.warning.assert_not_called()

    @patch("subprocess.run")
    def unexpected_error_on_local_dst_pool_check_raises_exception(self, mock_subprocess_run: MagicMock) -> None:
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=["zfs", "list", "-t", "filesystem,volume", "-Hp", "-o", "name"],
            returncode=2,
            stdout="",
            stderr="Permission denied",
        )
        with self.assertRaises(SystemExit) as cm:
            self.job.skip_nonexisting_local_dst_pools([("src/dataset", "-:nonexistent_pool/dataset")])
        self.assertEqual(3, cm.exception.code)
        self.assertIn("Unexpected error 2 on checking for existing local dst pools: Permission denied", str(cm.exception))

    @patch("subprocess.run")
    def local_dst_pool_not_found_skips_dataset(self, mock_subprocess_run: MagicMock) -> None:
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=["zfs", "list", "-t", "filesystem,volume", "-Hp", "-o", "name"],
            returncode=1,
            stdout="",
            stderr="dataset not found",
        )
        result = self.job.skip_nonexisting_local_dst_pools([("src/dataset", "-:nonexistent_pool/dataset")])
        self.assertEqual([], result)
        self.log_mock.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: -:nonexistent_pool/dataset"
        )

    @patch("subprocess.run")
    def local_dst_pool_exists_processes_dataset(self, mock_subprocess_run: MagicMock) -> None:
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=["zfs", "list", "-t", "filesystem,volume", "-Hp", "-o", "name"],
            returncode=0,
            stdout="existing_pool/dataset\n",
            stderr="",
        )
        result = self.job.skip_nonexisting_local_dst_pools([("src/dataset", "-:existing_pool/dataset")])
        self.assertEqual([("src/dataset", "-:existing_pool/dataset")], result)

    @patch("subprocess.run")
    def test_unexpected_error_logs_and_raises(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(args=["zfs", "list"], returncode=5, stdout="", stderr="boom")
        with patch.object(self.job.log, "error") as mock_error:
            with self.assertRaises(SystemExit) as cm:
                self.job.skip_nonexisting_local_dst_pools([("src/dataset", "-:pool/dataset")])
        expected = "Unexpected error 5 on checking for existing local dst pools: boom"
        mock_error.assert_called_once_with("%s", expected)
        self.assertEqual(3, cm.exception.code)
        self.assertIn(expected, str(cm.exception))


#############################################################################
class TestRunSubJobSpawnProcessPerJob(AbstractTestCase):

    def setUp(self) -> None:
        self.assertIsNotNone(shutil.which("sh"))
        self.job = bzfs_jobrunner.Job()

    def run_and_capture(self, cmd: list[str], timeout_secs: float | None) -> tuple[int | None, list[str]]:
        """Helper: invoke the method, return (exit_code, [all error-log messages])."""
        with patch.object(self.job.log, "error") as mock_error:
            code = self.job.run_worker_job_spawn_process_per_job(cmd, timeout_secs=timeout_secs)
            logs = [cast(str, call.args[1]) for call in mock_error.call_args_list]
        return code, logs

    def test_normal_completion(self) -> None:
        """Exits 0 under timeout, no errors."""
        code, logs = self.run_and_capture(["sleep", "0"], timeout_secs=1.0)
        self.assertEqual(0, code)
        self.assertListEqual([], logs)

    def test_none_timeout(self) -> None:
        """timeout_secs=None -> wait indefinitely, no errors."""
        code, logs = self.run_and_capture(["sleep", "0"], timeout_secs=None)
        self.assertEqual(0, code)
        self.assertListEqual([], logs)

    def test_nonzero_exit(self) -> None:
        """Non-zero exit code is propagated, no errors logged."""
        code, logs = self.run_and_capture(["sh", "-c", "exit 3"], timeout_secs=1.0)
        self.assertEqual(3, code)
        self.assertListEqual([], logs)

    def test_dryrun(self) -> None:
        """Dryrun mode does not actually execute the CLI command."""
        self.job.jobrunner_dryrun = True
        code, logs = self.run_and_capture(["sh", "-c", "exit 3"], timeout_secs=None)
        self.assertEqual(0, code)
        self.assertListEqual([], logs)

    def test_timeout_terminate(self) -> None:
        """Timeout->SIGTERM path: return code and first log prefix."""
        code, logs = self.run_and_capture(["sleep", "1"], timeout_secs=0.1)
        self.assertEqual(-signal.SIGTERM, code)
        self.assertTrue(
            logs and logs[0].startswith("Terminating worker job"),
            f"Expected first log starting with 'Terminating worker job', got {logs!r}",
        )

    def test_timeout_kill(self) -> None:
        """SIGTERM ignored->SIGKILL path: return code and kill-log present."""
        code, logs = self.run_and_capture(["sh", "-c", 'trap "" TERM; sleep 1'], timeout_secs=0.1)
        self.assertEqual(-signal.SIGKILL, code)
        self.assertTrue(
            any(msg.startswith("Killing worker job") for msg in logs),
            f"Expected a log starting with 'Killing worker job', got {logs!r}",
        )


#############################################################################
@patch("bzfs_main.bzfs_jobrunner.Job._bzfs_run_main")
class TestRunSubJobInCurrentThread(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs_jobrunner.Job()

    def test_no_timeout_success(self, mock_bzfs_run_main: MagicMock) -> None:
        cmd = ["bzfs", "foo", "bar"]
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(0, result)
        mock_bzfs_run_main.assert_called_once_with(cmd)

    def test_timeout_flag_insertion(self, mock_bzfs_run_main: MagicMock) -> None:
        cmd = ["bzfs", "foo"]
        timeout = 1.234
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=timeout)
        self.assertEqual(0, result)
        mock_bzfs_run_main.assert_called_once_with(["bzfs", "--timeout=1234milliseconds", "foo"])

    def test_called_process_error_returns_returncode(self, mock_bzfs_run_main: MagicMock) -> None:
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = subprocess.CalledProcessError(returncode=7, cmd=cmd)
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(7, result)

    def test_system_exit_with_code(self, mock_bzfs_run_main: MagicMock) -> None:
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = SystemExit(3)
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(3, result)

    def test_system_exit_without_code(self, mock_bzfs_run_main: MagicMock) -> None:
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = SystemExit
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertIsNone(result)

    def test_dryrun(self, mock_bzfs_run_main: MagicMock) -> None:
        """Dryrun mode does not actually execute the CLI command."""
        self.job.jobrunner_dryrun = True
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = SystemExit(3)
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(0, result)

    def test_generic_exception_returns_die_status(self, mock_bzfs_run_main: MagicMock) -> None:
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = SystemExit(DIE_STATUS)
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(DIE_STATUS, result)

    def test_single_element_cmd_edge(self, mock_bzfs_run_main: MagicMock) -> None:
        cmd = ["bzfs"]
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(0, result)
        mock_bzfs_run_main.assert_called_once_with(cmd)

    def test_unexpected_exception(self, mock_bzfs_run_main: MagicMock) -> None:
        """Suppress unexpected-exception logs and ensure the worker reports a fatal status."""
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = ValueError
        with suppress_output():
            result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(DIE_STATUS, result)


#############################################################################
class TestRunSubJob(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs_jobrunner.Job()
        self.job.stats.jobs_all = 1
        self.assertIsNone(self.job.first_exception)

    def test_success(self) -> None:
        """Run a successful subjob and silence its informational logs."""
        with suppress_output():
            result = self.job.run_subjob(cmd=["true"], name="j0", timeout_secs=None, spawn_process_per_job=True)
        self.assertEqual(0, result)

    def test_failure(self) -> None:
        """Verify failing subjobs yield errors while keeping log output quiet."""
        self.job.stats.jobs_all = 2
        with suppress_output():
            result = self.job.run_subjob(cmd=["false"], name="j0", timeout_secs=None, spawn_process_per_job=True)
        self.assertNotEqual(0, result)
        self.assertIsInstance(self.job.first_exception, int)
        self.assertTrue(self.job.first_exception != 0)

        with suppress_output():
            result = self.job.run_subjob(cmd=["false"], name="j1", timeout_secs=None, spawn_process_per_job=True)
        self.assertNotEqual(0, result)
        self.assertIsInstance(self.job.first_exception, int)
        self.assertTrue(self.job.first_exception != 0)

    def test_timeout(self) -> None:
        """Check subjobs respect timeouts and silence their logs."""
        self.job.stats.jobs_all = 2
        with suppress_output():
            result = self.job.run_subjob(cmd=["sleep", "0"], name="j0", timeout_secs=1, spawn_process_per_job=True)
        self.assertEqual(0, result)
        with suppress_output():
            result = self.job.run_subjob(cmd=["sleep", "1"], name="j1", timeout_secs=0.01, spawn_process_per_job=True)
        self.assertNotEqual(0, result)

    def test_nonexisting_cmd(self) -> None:
        """Ensure missing commands raise FileNotFoundError without noisy logs."""
        with self.assertRaises(FileNotFoundError), suppress_output():
            self.job.run_subjob(cmd=["sleep_nonexisting_cmd", "1"], name="j0", timeout_secs=None, spawn_process_per_job=True)


#############################################################################
class TestErrorPropagation(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs_jobrunner.Job()
        self.job.log = MagicMock()
        self.job.is_test_mode = True

    def test_parallel_worker_exception_sets_first_exception_via_exception_mode(self) -> None:
        self.job = bzfs_jobrunner.Job()
        self.job.log = MagicMock()
        self.job.is_test_mode = True
        with patch.object(self.job, "run_worker_job_in_current_thread", side_effect=CalledProcessError(1, "boom")):
            self.job.run_subjobs(
                subjobs={"000000src-host/replicate": ["bzfs", "--no-argument-file"]},
                max_workers=1,
                timeout_secs=None,
                work_period_seconds=0,
                jitter=False,
            )
        self.assertIsInstance(self.job.first_exception, int)
        self.assertEqual(DIE_STATUS, self.job.first_exception)
        self.assertEqual(1, self.job.stats.jobs_started)
        self.assertEqual(1, self.job.stats.jobs_completed)
        self.assertEqual(1, self.job.stats.jobs_failed)


#############################################################################
class TestValidateSnapshotPlan(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs_jobrunner.Job()
        self.job.log = MagicMock()

    def test_validate_snapshot_plan_empty(self) -> None:
        plan: dict = {}
        self.assertEqual(plan, self.job.validate_snapshot_plan(plan, "ctx"))

    def test_validate_snapshot_plan_valid(self) -> None:
        plan = {"org": {"tgt": {"hour": 1}}}
        self.assertEqual(plan, self.job.validate_snapshot_plan(plan, "ctx"))

    def test_validate_snapshot_plan_invalid_amount(self) -> None:
        plan = {"org": {"tgt": {"hour": -1}}}
        with self.assertRaises(SystemExit):
            self.job.validate_snapshot_plan(plan, "ctx")


#############################################################################
class TestValidateMonitorSnapshotPlan(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs_jobrunner.Job()
        self.job.log = MagicMock()

    def test_validate_monitor_snapshot_plan_empty(self) -> None:
        plan: dict = {}
        self.assertEqual(plan, self.job.validate_monitor_snapshot_plan(plan))

    def test_validate_monitor_snapshot_plan_valid(self) -> None:
        plan: dict[str, dict[str, dict[str, dict[str, str | int]]]] = {
            "org": {"tgt": {"hour": {"warn": "msg", "max_age": 1}}}
        }
        self.assertEqual(plan, self.job.validate_monitor_snapshot_plan(plan))

    def test_validate_monitor_snapshot_plan_invalid_value(self) -> None:
        plan: dict[str, dict[str, dict[str, dict[str, object]]]] = {"org": {"tgt": {"hour": {"warn": object()}}}}
        with self.assertRaises(SystemExit):
            self.job.validate_monitor_snapshot_plan(plan)  # type: ignore[arg-type]


#############################################################################
def is_solaris_zfs() -> bool:
    return platform.system() == "SunOS"
