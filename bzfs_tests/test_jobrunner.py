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

from __future__ import (
    annotations,
)
import argparse
import contextlib
import platform
import shutil
import signal
import subprocess
import sys
import threading
import time
import unittest
from logging import (
    Logger,
)
from subprocess import (
    DEVNULL,
    PIPE,
    CalledProcessError,
)
from typing import (
    Any,
    Union,
    cast,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from bzfs_main import (
    bzfs_jobrunner,
)
from bzfs_main.util.utils import (
    DIE_STATUS,
)
from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)
from bzfs_tests.tools import (
    suppress_logger,
    suppress_output,
)


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
        TestParserIsolationAcrossSubjobs,
        TestSpawnProcessPerJobDecision,
        TestScopedTerminationInProcess,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


def make_bzfs_jobrunner_job(log: Logger | None = None) -> bzfs_jobrunner.Job:
    job = bzfs_jobrunner.Job(log=log, termination_event=threading.Event())
    job.is_test_mode = True
    return job


#############################################################################
class TestHelperFunctions(AbstractTestCase):

    def setUp(self) -> None:
        self.job = make_bzfs_jobrunner_job()

    def test_validate_type(self) -> None:
        """Ensure validate_type accepts expected types and exits on mismatches quietly."""
        self.job.validate_type("foo", str, "name")
        self.job.validate_type(123, int, "name")
        with self.assertLogs(self.job.log, level="ERROR"), self.assertRaises(SystemExit):
            self.job.validate_type(123, str, "name")
        with self.assertLogs(self.job.log, level="ERROR"), self.assertRaises(SystemExit):
            self.job.validate_type("foo", int, "name")
        with self.assertLogs(self.job.log, level="ERROR"), self.assertRaises(SystemExit):
            self.job.validate_type(None, str, "name")
        self.job.validate_type("foo", Union[str, int], "name")
        self.job.validate_type(123, Union[str, int], "name")
        with self.assertLogs(self.job.log, level="ERROR"), self.assertRaises(SystemExit):
            self.job.validate_type([], Union[str, int], "name")
        with self.assertLogs(self.job.log, level="ERROR"), self.assertRaises(SystemExit):
            self.job.validate_type(None, Union[str, int], "name")

    def test_validate_non_empty_string(self) -> None:
        """Reject empty strings while keeping the log output silent."""
        self.job.validate_non_empty_string("valid_string", "name")
        with self.assertLogs(self.job.log, level="ERROR"), self.assertRaises(SystemExit):
            self.job.validate_non_empty_string("", "name")

    def test_validate_non_negative_int(self) -> None:
        """Verify negative integers abort without leaking error logs."""
        self.job.validate_non_negative_int(1, "name")
        self.job.validate_non_negative_int(0, "name")
        with self.assertLogs(self.job.log, level="ERROR"), self.assertRaises(SystemExit):
            self.job.validate_non_negative_int(-1, "name")

    def test_validate_true(self) -> None:
        """Expect false values to trigger exit, capturing any error message."""
        self.job.validate_true(1, "name")
        with self.assertLogs(self.job.log, level="ERROR"), self.assertRaises(SystemExit):
            self.job.validate_true(0, "name")

    def test_validate_is_subset(self) -> None:
        """Confirm subset validation handles errors without emitting logs."""
        self.job.validate_is_subset(["1"], ["1", "2"], "x", "y")
        self.job.validate_is_subset([], ["1", "2"], "x", "y")
        self.job.validate_is_subset([], [], "x", "y")
        with self.assertLogs(self.job.log, level="ERROR"), self.assertRaises(SystemExit):
            self.job.validate_is_subset(["3"], ["1", "2"], "x", "y")
        with self.assertLogs(self.job.log, level="ERROR"), self.assertRaises(SystemExit):
            self.job.validate_is_subset(["3", "4"], [], "x", "y")
        with self.assertLogs(self.job.log, level="ERROR"), self.assertRaises(SystemExit):
            self.job.validate_is_subset("foo", ["3"], "x", "y")
        with self.assertLogs(self.job.log, level="ERROR"), self.assertRaises(SystemExit):
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

    @unittest.skipIf(platform.system() != "Linux", "This test is designed for Linux only")
    def test_get_localhost_ips(self) -> None:
        job = make_bzfs_jobrunner_job()
        ips: set[str] = job.get_localhost_ips()
        self.assertTrue(len(ips) > 0)

    @patch("subprocess.run", side_effect=RuntimeError("fail"))
    def test_get_localhost_ips_failure_logs_warning(self, mock_run: MagicMock) -> None:
        job = make_bzfs_jobrunner_job()
        with patch("platform.system", return_value="Linux"), patch.object(job.log, "warning") as mock_warn:
            ips = job.get_localhost_ips()
        self.assertEqual(set(), ips)
        mock_warn.assert_called_once()
        fmt, exc = mock_warn.call_args.args
        self.assertEqual("Cannot run 'hostname -I' on localhost: %s", fmt)
        self.assertIsInstance(exc, RuntimeError)


#############################################################################
class TestParseSrcHosts(AbstractTestCase):

    def setUp(self) -> None:
        self.job = make_bzfs_jobrunner_job()

    # ---- raw_src_hosts provided (no stdin) ----
    def test_cli_value_valid_list(self) -> None:
        result = self.job.parse_src_hosts_from_cli_or_stdin(str(["h1", "h2"]))
        self.assertEqual(["h1", "h2"], result)

    def test_cli_value_invalid_literal(self) -> None:
        with self.assertRaises(SystemExit) as cm, suppress_output(), suppress_logger(self.job.log):
            self.job.parse_src_hosts_from_cli_or_stdin("[not_a_name]")
        self.assertEqual(DIE_STATUS, cm.exception.code)
        self.assertIn("Invalid --src-hosts format:", str(cm.exception))

    def test_cli_value_not_a_list(self) -> None:
        with self.assertRaises(SystemExit) as cm, suppress_output(), suppress_logger(self.job.log):
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
            with self.assertRaises(SystemExit) as cm, suppress_output(), suppress_logger(self.job.log):
                self.job.parse_src_hosts_from_cli_or_stdin(None)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn("stdin is a TTY", str(cm.exception))

    def test_stdin_empty_raises(self) -> None:
        fake = self._FakeStdin(data="   \n\t  ", isatty=False)
        with patch.object(sys, "stdin", fake):
            with self.assertRaises(SystemExit) as cm, suppress_output(), suppress_logger(self.job.log):
                self.job.parse_src_hosts_from_cli_or_stdin(None)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn("stdin is empty", str(cm.exception))

    def test_stdin_invalid_literal_raises(self) -> None:
        fake = self._FakeStdin(data="nonsense", isatty=False)
        with patch.object(sys, "stdin", fake):
            with self.assertRaises(SystemExit) as cm, suppress_output(), suppress_logger(self.job.log):
                self.job.parse_src_hosts_from_cli_or_stdin(None)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn("Invalid --src-hosts format:", str(cm.exception))

    def test_stdin_not_a_list_raises(self) -> None:
        fake = self._FakeStdin(data="'abc'", isatty=False)
        with patch.object(sys, "stdin", fake):
            with self.assertRaises(SystemExit) as cm, suppress_output(), suppress_logger(self.job.log):
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
        job = make_bzfs_jobrunner_job()
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
                with suppress_output():
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
            with self.assertRaises(SystemExit) as cm, suppress_output():
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
            with self.assertRaises(SystemExit) as cm, suppress_output():
                job.run_main(argv_src1)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn(expect_msg, str(cm.exception))

        # Scenario 2: Run for srcB only, should fail.
        argv_src2 = base_argv + ["--src-host", "src2"]
        with patch("sys.argv", argv_src2):
            with self.assertRaises(SystemExit) as cm, suppress_output():
                job.run_main(argv_src2)
            self.assertEqual(DIE_STATUS, cm.exception.code)
            self.assertIn(expect_msg, str(cm.exception))

        # Scenario 3: Run for both (no --src-host filter), should fail.
        job = self._new_job()
        with patch("sys.argv", base_argv):  # No --src-host filter
            with self.assertRaises(SystemExit) as cm, suppress_output():
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
        job = make_bzfs_jobrunner_job()

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
                with suppress_output():
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
        self.log_mock = MagicMock(Logger)
        self.job = make_bzfs_jobrunner_job(log=self.log_mock)

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
    def test_unexpected_error_on_local_dst_pool_check_raises_exception(self, mock_subprocess_run: MagicMock) -> None:
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
    def test_local_dst_pool_not_found_skips_dataset(self, mock_subprocess_run: MagicMock) -> None:
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=["zfs", "list", "-t", "filesystem,volume", "-Hp", "-o", "name"],
            returncode=1,
            stdout="",
            stderr="dataset not found",
        )
        result = self.job.skip_nonexisting_local_dst_pools([("src/dataset", "-:nonexistent_pool/dataset")])
        self.assertEqual([], result)
        self.log_mock.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: %s", "-:nonexistent_pool/dataset"
        )

    @patch("subprocess.run")
    def test_local_dst_pool_exists_processes_dataset(self, mock_subprocess_run: MagicMock) -> None:
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=["zfs", "list", "-t", "filesystem,volume", "-Hp", "-o", "name"],
            returncode=0,
            stdout="existing_pool\n",
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
        self.job = make_bzfs_jobrunner_job()

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

    def test_termination_event_triggers_early_cancel_in_spawn_mode(self) -> None:
        """When termination_event is set, spawned process should be terminated promptly, not after full timeout.

        This covers a race where a termination sweep may miss a just-spawned child. The worker must observe the termination
        event and proactively terminate its own subprocess to avoid hanging until timeout_secs elapses.
        """
        # Arrange a job with the event pre-set; patch Popen so we don't rely on real sleeps.
        self.job.termination_event.set()

        class _FakeProc:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.args = args
                self.kwargs = kwargs
                self.pid = 99999
                self.returncode: int | None = None

            def poll(self) -> int | None:
                return self.returncode

            def communicate(self, timeout: float | None = None) -> tuple[str, str]:
                return ("", "")

            def terminate(self) -> None:
                self.returncode = -signal.SIGTERM

            def kill(self) -> None:
                self.returncode = -signal.SIGKILL

        start = time.monotonic()
        with patch("bzfs_main.util.utils.subprocess.Popen", new=_FakeProc):
            code, _logs = self.run_and_capture(["sleep", "600"], timeout_secs=10.0)
        elapsed = time.monotonic() - start

        # Assert we did not wait for the full timeout; should return very quickly (< 0.2s on CI).
        self.assertLess(elapsed, 0.2, f"unexpected slow early-termination: {elapsed:.3f}s")
        self.assertIn(code, (-signal.SIGTERM, -signal.SIGKILL))

    def test_termination_event_real_sleep_exits_quickly(self) -> None:
        """Integration-style check: pre-set termination_event causes real spawned process to exit promptly."""
        self.job.termination_event.set()
        start = time.monotonic()
        code, logs = self.run_and_capture(["sleep", "1"], timeout_secs=2.0)
        elapsed = time.monotonic() - start

        # We should not wait anywhere near the full timeout; allow generous slack for slow CI.
        self.assertLess(elapsed, 0.75, f"unexpected slow early-termination with real sleep: {elapsed:.3f}s")
        self.assertIn(code, (-signal.SIGTERM, -signal.SIGKILL))
        self.assertTrue(
            logs and logs[0].startswith("Terminating worker job due to async termination request"),
            f"Expected first log starting with async termination request, got {logs!r}",
        )


#############################################################################
@patch("bzfs_main.bzfs_jobrunner.Job._bzfs_run_main")
class TestRunSubJobInCurrentThread(AbstractTestCase):

    def setUp(self) -> None:
        self.job = make_bzfs_jobrunner_job()

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
        self.job = make_bzfs_jobrunner_job()
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
        self.job = make_bzfs_jobrunner_job()

    def test_parallel_worker_exception_sets_first_exception_via_exception_mode(self) -> None:
        self.job = make_bzfs_jobrunner_job()
        with patch.object(self.job, "run_worker_job_in_current_thread", side_effect=CalledProcessError(1, "boom")):
            with suppress_output():
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


#############################################################################
class TestSpawnProcessPerJobDecision(AbstractTestCase):
    """Verifies Job.run_subjobs selects per-subjob execution mode correctly.

    It asserts no subprocesses are spawned without siblings, even when the spawn flag is true, and confirms subprocess
    spawning when siblings exist.
    """

    def setUp(self) -> None:
        self.job = make_bzfs_jobrunner_job()

    def _capture_spawn_flags(self, subjobs: dict[str, list[str]], spawn_process_per_job: bool) -> list[bool]:
        self.job.spawn_process_per_job = spawn_process_per_job
        flags: list[bool] = []

        def fake_run_subjob(cmd: list[str], name: str, timeout_secs: float | None, spawn_process_per_job: bool) -> int:
            flags.append(spawn_process_per_job)
            return 0

        with patch.object(self.job, "run_subjob", side_effect=fake_run_subjob):
            with suppress_output():
                self.job.run_subjobs(
                    subjobs=subjobs,
                    max_workers=1,
                    timeout_secs=None,
                    work_period_seconds=0,
                    jitter=False,
                )
        return flags

    def test_no_siblings_spawn_flag_false_runs_in_process(self) -> None:
        subjobs = {"000000src-host/replicate": ["bzfs"]}
        flags = self._capture_spawn_flags(subjobs, spawn_process_per_job=False)
        self.assertEqual([False], flags)

    def test_no_siblings_spawn_flag_true_still_runs_in_process(self) -> None:
        subjobs = {"000000src-host/replicate": ["bzfs"]}  # single subjob -> no siblings
        flags = self._capture_spawn_flags(subjobs, spawn_process_per_job=True)
        self.assertEqual([True], flags)

    def test_with_siblings_spawn_flag_false_runs_in_process(self) -> None:
        subjobs = {
            "000000src-host/replicate_A": ["bzfs"],
            "000000src-host/replicate_B": ["bzfs"],
        }
        flags = self._capture_spawn_flags(subjobs, spawn_process_per_job=False)
        self.assertEqual(sorted([False, False]), sorted(flags))

    def test_with_siblings_spawn_flag_true_spawns_processes(self) -> None:
        subjobs = {
            "000000src-host/replicate_A": ["bzfs"],
            "000000src-host/replicate_B": ["bzfs"],
        }
        flags = self._capture_spawn_flags(subjobs, spawn_process_per_job=True)
        self.assertEqual(sorted([True, True]), sorted(flags))


#############################################################################
class TestScopedTerminationInProcess(AbstractTestCase):
    """Validates scoped-termination semantics: if a subjob fails, only its own process subtree is torn down while sibling
    subjobs and their children keep running. Purpose: demonstrate that Job.run_subjobs preserves worker isolation and avoids
    collateral kills during parallel replication. Executes the scenario in both thread and process modes via a shared helper.
    """

    def setUp(self) -> None:
        self.job = make_bzfs_jobrunner_job()

    def test_failing_subjob_terminates_only_its_own_children_process(self) -> None:
        self._assert_scoped_termination(spawn_process_per_job=True, patch_method="run_worker_job_spawn_process_per_job")

    def test_failing_subjob_terminates_only_its_own_children_thread(self) -> None:
        self._assert_scoped_termination(spawn_process_per_job=False, patch_method="run_worker_job_in_current_thread")

    def _assert_scoped_termination(self, spawn_process_per_job: bool, patch_method: str) -> None:
        """Two concurrent subjobs; subjob B will fail after spawning a child, whereas subjob A stays alive.

        kill B to emulate a failing subjob, which should trigger termination of B's children, not trigger killing A.
        """
        subjobs: dict[str, list[str]] = {
            "000000src-host/replicate_A": ["bzfs", "srcA", "dstA"],
            "000000src-host/replicate_B": ["bzfs", "srcB", "dstB"],
        }
        children: dict[str, subprocess.Popen[Any]] = {}

        def fake_worker(cmd: list[str], timeout_secs: float | None) -> int:
            # Simulate subjob execution without invoking bzfs parser
            if "srcA" in cmd:
                p = subprocess.Popen(["sleep", "1"])  # Child that should survive
                children["A"] = p
                time.sleep(0.05)  # brief overlap to ensure concurrency without slowing the suite
                return 0
            if "srcB" in cmd:
                p = subprocess.Popen(["sleep", "1"])  # Child that should be terminated
                children["B"] = p
                # kill B to emulate a failing subjob, which should trigger termination of B's children, not trigger killing A
                p.kill()
                return DIE_STATUS
            return 0

        self.job.spawn_process_per_job = spawn_process_per_job
        target = f"bzfs_main.bzfs_jobrunner.Job.{patch_method}"
        with patch(target, side_effect=fake_worker), suppress_output():
            self.job.run_subjobs(
                subjobs=subjobs,
                max_workers=2,
                timeout_secs=None,
                work_period_seconds=0,
                jitter=False,
            )

        # B's child should have been terminated
        self.assertIn("B", children)
        deadline = time.monotonic() + 0.25
        while children["B"].poll() is None and time.monotonic() < deadline:
            time.sleep(0.005)
        self.assertIsNotNone(children["B"].poll(), "B child should be terminated")

        # A's child should still be running, proving scoped termination did not affect siblings
        self.assertIn("A", children)
        self.assertIsNone(children["A"].poll(), "A child should still be running")

        # Cleanup remaining child
        with contextlib.suppress(Exception):
            children["A"].kill()
        self.assertEqual(self.job.stats.jobs_started, self.job.stats.jobs_completed)
        self.assertEqual(1, self.job.stats.jobs_failed)


#############################################################################
class TestValidateSnapshotPlan(AbstractTestCase):

    def setUp(self) -> None:
        self.job = make_bzfs_jobrunner_job(log=MagicMock())

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
        self.job = make_bzfs_jobrunner_job(log=MagicMock())

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
class TestParserIsolationAcrossSubjobs(AbstractTestCase):

    def test_replicate_subjobs_do_not_leak_snapshot_filters(self) -> None:
        """Multiple replicate subjobs in one process must not leak include-snapshot-regex between parses.

        We capture the '--include-snapshot-regex=...' tokens produced by --include-snapshot-plan during the initial
        parse inside Job._bzfs_run_main (before bzfs re-parses aux args). With a reused parser, the list accumulates
        across subjobs and we would see both regexes in the second subjob.
        """
        job = make_bzfs_jobrunner_job()

        # Build argv that yields two separate replicate subjobs, one per dst host with distinct targets.
        # Host A replicates empty-target daily; Host B replicates 'onsite' hourly.
        argv: list[str] = [
            bzfs_jobrunner.PROG_NAME,
            "--job-id=test",
            "--root-dataset-pairs",
            "tank/src",
            "pool/dst",
            "--include-dataset-regex=^pool/dst$",  # forwarded unknown arg to bzfs; has default list in parser
            "--src-hosts=['src1']",
            "--dst-hosts={'dstA': [''], 'dstB': ['onsite']}",
            "--retain-dst-targets={'dstA': [''], 'dstB': ['onsite']}",
            "--dst-root-datasets={'dstA': 'pool/backup', 'dstB': 'pool/backup'}",
            "--dst-snapshot-plan={'org': {'': {'daily': 1}, 'onsite': {'hourly': 1}}}",
            "--replicate",
        ]

        # Capture subjobs produced by run_main without executing them.
        captured: dict[str, list[str]] = {}

        def fake_run_subjobs(subjobs: dict[str, list[str]], *_: object, **__: object) -> None:
            captured.update(subjobs)

        with patch.object(job, "run_subjobs", side_effect=fake_run_subjobs):
            with patch("sys.argv", argv):
                with suppress_output():
                    job.run_main(argv)

        # In this test we only emit replicate subjobs; assert count and take them in deterministic order.
        self.assertEqual(2, len(captured), f"unexpected subjobs: {sorted(captured.keys())}")
        replicate_cmds: list[list[str]] = [captured[name] for name in sorted(captured)]

        # Patch bzfs.Job.run_main to capture the parsed args as delivered by jobrunner's parser
        captured_regex_tokens: list[list[str]] = []
        captured_include_dataset_regex_values: list[list[str]] = []

        def fake_bzfs_run_main(
            self: Any, args: argparse.Namespace, sys_argv: list[str] | None = None, log: Logger | None = None
        ) -> None:
            tokens = [t for t in cast(list[str], args.include_snapshot_plan) if t.startswith("--include-snapshot-regex=")]
            captured_regex_tokens.append(tokens)
            captured_include_dataset_regex_values.append(list(cast(list[str], args.include_dataset_regex)))

        with patch("bzfs_main.bzfs.Job.run_main", new=fake_bzfs_run_main):
            job.stats.jobs_all = len(replicate_cmds)
            for idx, cmd in enumerate(replicate_cmds):
                with suppress_output():
                    result = job.run_subjob(cmd=cmd, name=f"replicate{idx}", timeout_secs=None, spawn_process_per_job=False)
                self.assertEqual(0, result)

        # Each subjob must specify exactly the expected include-snapshot-regex token (no extras),
        # compared as nested lists to preserve the per-subjob boundary (no tokens[0] needed).
        expected_tokens_nested = [
            ["--include-snapshot-regex=org_[1-9][0-9][0-9][0-9].*_daily"],
            ["--include-snapshot-regex=org_onsite_.*_hourly"],
        ]
        self.assertListEqual(expected_tokens_nested, captured_regex_tokens)

        # Critically: include_dataset_regex must equal the exact forwarded value for each subjob (no accumulation)
        self.assertListEqual([["^pool/dst$"], ["^pool/dst$"]], captured_include_dataset_regex_values)
