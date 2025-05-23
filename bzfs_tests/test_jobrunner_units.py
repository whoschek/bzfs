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

import importlib
import os
import platform
import shutil
import signal
import subprocess
import sys
import tempfile
import types
import unittest
from subprocess import DEVNULL, PIPE
from typing import Union
from unittest.mock import patch, MagicMock

from bzfs.bzfs import die_status

bzfs_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + os.sep + "bzfs"
if bzfs_dir not in os.environ["PATH"]:
    os.environ["PATH"] = bzfs_dir + os.pathsep + os.environ["PATH"]
from bzfs import bzfs_jobrunner


def suite():
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestHelperFunctions))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSkipDatasetsWithNonExistingDstPool))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRunSubJobSpawnProcessPerJob))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRunSubJobInCurrentThread))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRunSubJob))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestXLoadModule))
    return suite


#############################################################################
class TestHelperFunctions(unittest.TestCase):
    def setUp(self):
        self.job = bzfs_jobrunner.Job()

    def test_validate_type(self):
        self.job.validate_type("foo", str, "name")
        self.job.validate_type(123, int, "name")
        with self.assertRaises(SystemExit):
            self.job.validate_type(123, str, "name")
        with self.assertRaises(SystemExit):
            self.job.validate_type("foo", int, "name")
        with self.assertRaises(SystemExit):
            self.job.validate_type(None, str, "name")
        self.job.validate_type("foo", Union[str, int], "name")
        self.job.validate_type(123, Union[str, int], "name")
        with self.assertRaises(SystemExit):
            self.job.validate_type([], Union[str, int], "name")
        with self.assertRaises(SystemExit):
            self.job.validate_type(None, Union[str, int], "name")

    def test_validate_non_empty_string(self):
        self.job.validate_non_empty_string("valid_string", "name")
        with self.assertRaises(SystemExit):
            self.job.validate_non_empty_string("", "name")

    def test_validate_non_negative_int(self):
        self.job.validate_non_negative_int(1, "name")
        self.job.validate_non_negative_int(0, "name")
        with self.assertRaises(SystemExit):
            self.job.validate_non_negative_int(-1, "name")

    def test_validate_true(self):
        self.job.validate_true(1, "name")
        with self.assertRaises(SystemExit):
            self.job.validate_true(0, "name")

    def test_validate_is_subset(self):
        self.job.validate_is_subset([1], [1, 2], "x", "y")
        self.job.validate_is_subset([], [1, 2], "x", "y")
        self.job.validate_is_subset([], [], "x", "y")
        with self.assertRaises(SystemExit):
            self.job.validate_is_subset([3], [1, 2], "x", "y")
        with self.assertRaises(SystemExit):
            self.job.validate_is_subset([3, 4], [], "x", "y")
        with self.assertRaises(SystemExit):
            self.job.validate_is_subset("foo", [3], "x", "y")
        with self.assertRaises(SystemExit):
            self.job.validate_is_subset([3], "foo", "x", "y")

    def _make_mock_socket(self, bind_side_effect=None):
        sock = MagicMock()
        sock.bind.side_effect = bind_side_effect
        sock.__enter__.return_value = sock
        sock.__exit__.return_value = False
        return sock

    def test_detect_loopback_address_ipv4_supported(self):
        ipv4 = self._make_mock_socket()
        with patch("socket.socket", side_effect=[ipv4]) as socket_factory:
            self.assertEqual("127.0.0.1", bzfs_jobrunner.detect_loopback_address())
            self.assertEqual(1, socket_factory.call_count)

    def test_detect_loopback_address_ipv6_fallback(self):
        ipv4_fail = self._make_mock_socket(OSError("No IPv4"))
        ipv6_ok = self._make_mock_socket()
        with patch("socket.socket", side_effect=[ipv4_fail, ipv6_ok]) as socket_factory:
            self.assertEqual("::1", bzfs_jobrunner.detect_loopback_address())
            self.assertEqual(2, socket_factory.call_count)

    def test_detect_loopback_address_neither_supported(self):
        ipv4_fail = self._make_mock_socket(OSError("No IPv4"))
        ipv6_fail = self._make_mock_socket(OSError("No IPv6"))
        with patch("socket.socket", side_effect=[ipv4_fail, ipv6_fail]) as socket_factory:
            self.assertEqual("", bzfs_jobrunner.detect_loopback_address())
            self.assertEqual(2, socket_factory.call_count)

    def test_pretty_print_formatter(self):
        self.assertIsNotNone(str(bzfs_jobrunner.pretty_print_formatter({"foo": "bar"})))

    def test_help(self):
        if is_solaris_zfs():
            self.skipTest("FIXME: BlockingIOError: [Errno 11] write could not complete without blocking")
        parser = bzfs_jobrunner.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["--help"])
        self.assertEqual(0, e.exception.code)

    def test_version(self):
        parser = bzfs_jobrunner.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["--version"])
        self.assertEqual(0, e.exception.code)


#############################################################################
class TestSkipDatasetsWithNonExistingDstPool(unittest.TestCase):
    def setUp(self):
        self.job = bzfs_jobrunner.Job()
        self.job.log = MagicMock()

    def test_empty_input_raises(self):
        with self.assertRaises(AssertionError):
            self.job.skip_nonexisting_local_dst_pools([])

    @patch("subprocess.run")
    def test_single_existing_pool(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\n", stderr="")
        pairs = [("-:srcpool1/src1", "-:dstpool1/dst1")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual(pairs, result)
        self.assertSetEqual({"-:dstpool1"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool1"}, self.job.cache_known_dst_pools)
        expected_cmd = "zfs list -t filesystem,volume -Hp -o name".split(" ") + ["dstpool1"]
        mock_run.assert_called_once_with(expected_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True)

    @patch("subprocess.run")
    def test_single_nonexisting_pool(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        pairs = [("-:srcpool2/src2", "-:dstpool2/dst2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual([], result)
        self.assertSetEqual(set(), self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool2"}, self.job.cache_known_dst_pools)
        self.job.log.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: %s", "-:dstpool2/dst2"
        )

    @patch("subprocess.run")
    def test_multiple_pools_mixed(self, mock_run):
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
        self.job.log.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: %s", "-:dstpool2/dst2"
        )

    @patch("subprocess.run")
    def test_caching_avoids_subprocess_run(self, mock_run):
        self.job.cache_existing_dst_pools = {"-:dstpool1"}
        self.job.cache_known_dst_pools = {"-:dstpool1", "-:dstpool2"}
        pairs = [("srcpool1/src1", "-:dstpool1/dst1"), ("srcpool2/src2", "-:dstpool2/dst2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual([("srcpool1/src1", "-:dstpool1/dst1")], result)
        mock_run.assert_not_called()
        self.job.log.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: %s", "-:dstpool2/dst2"
        )

    @patch("subprocess.run")
    def test_multiple_pools_exist_returns_all(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\ndstpool2\n", stderr="")
        pairs = [("srcpool1/src1", "-:dstpool1/dst1"), ("srcpool2/src2", "-:dstpool2/dst2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual(pairs, result)
        self.assertSetEqual({"-:dstpool1", "-:dstpool2"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool1", "-:dstpool2"}, self.job.cache_known_dst_pools)
        mock_run.assert_called_once()

    @patch("subprocess.run")
    def test_repeated_call_caching(self, mock_run):
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
    def test_multislash_dataset_names(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\n", stderr="")
        pairs = [("srcpool1/src1", "-:dstpool1/child/grand")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual(pairs, result)
        self.assertIn("-:dstpool1", self.job.cache_existing_dst_pools)

    @patch("subprocess.run")
    def test_multiple_warnings_for_nonexistent(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        pairs = [("srcpool1/src1", "-:srcpool1/dst1"), ("srcpool2/src2", "-:dstpool2/dst2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual([], result)
        expected_calls = [
            unittest.mock.call("Skipping dst dataset for which local dst pool does not exist: %s", "-:srcpool1/dst1"),
            unittest.mock.call("Skipping dst dataset for which local dst pool does not exist: %s", "-:dstpool2/dst2"),
        ]
        self.assertEqual(expected_calls, self.job.log.warning.mock_calls)

    @patch("subprocess.run")
    def test_duplicate_pool_input(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\n", stderr="")
        pairs = [("srcpool1/src1", "-:dstpool1/dst1"), ("srcpool2/src2", "-:dstpool1/dst2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual(pairs, result)
        mock_run.assert_called_once()

    # dst without slash, existing pool
    @patch("subprocess.run")
    def test_dst_without_slash_existing_pool(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool4\n", stderr="")
        pairs = [("srcpool4/src4", "-:dstpool4")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual(pairs, result)
        self.assertSetEqual({"-:dstpool4"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool4"}, self.job.cache_known_dst_pools)
        expected_cmd = "zfs list -t filesystem,volume -Hp -o name".split(" ") + ["dstpool4"]
        mock_run.assert_called_once_with(expected_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True)

    # dst without slash, non-existing pool
    @patch("subprocess.run")
    def test_dst_without_slash_nonexisting_pool(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        pairs = [("srcpool4/src4", "-:dstpool2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual([], result)
        self.assertSetEqual(set(), self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool2"}, self.job.cache_known_dst_pools)
        self.job.log.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: %s", "-:dstpool2"
        )

    # non-local dst and without slash
    @patch("subprocess.run")
    def test_nonlocaldst_without_slash(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        pairs = [("srcpool4/src4", "127.0.0.1:dstpool2")]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertListEqual(pairs, result)
        self.assertSetEqual({"127.0.0.1:dstpool2"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"127.0.0.1:dstpool2"}, self.job.cache_known_dst_pools)
        self.assertEqual([], self.job.log.warning.mock_calls)

    @patch("subprocess.run")
    def test_mixed_local_existing_and_remote(self, mock_run):
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
        mock_run.assert_called_once_with(expected_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True)
        self.job.log.warning.assert_not_called()

    @patch("subprocess.run")
    def test_mixed_local_nonexisting_and_remote(self, mock_run):
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
        mock_run.assert_called_once_with(expected_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True)
        self.job.log.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: %s", "-:local2/src1"
        )

    @patch("subprocess.run")
    def test_mixed_local_existing_nonexisting_and_remote(self, mock_run):
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
        self.assertEqual(mock_run.call_count, 1)
        called_cmd = mock_run.call_args[0][0]
        self.assertEqual(called_cmd[: len(expected_cmd_parts)], expected_cmd_parts)
        self.assertCountEqual(sorted(["local1", "local2"]), sorted(called_cmd[len(expected_cmd_parts) :]))

        self.job.log.warning.assert_called_once_with(
            "Skipping dst dataset for which local dst pool does not exist: %s", "-:local2/data3"
        )

    @patch("subprocess.run")
    def test_all_remote_pools(self, mock_run):
        pairs = [
            ("src1/src1", "remote1:pool1/src1"),
            ("src2/src2", "remote2:pool2/src2"),
        ]
        result = self.job.skip_nonexisting_local_dst_pools(pairs)
        self.assertCountEqual(pairs, result)
        self.assertSetEqual({"remote1:pool1", "remote2:pool2"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"remote1:pool1", "remote2:pool2"}, self.job.cache_known_dst_pools)
        mock_run.assert_not_called()  # No local pools to check
        self.job.log.warning.assert_not_called()


#############################################################################
class TestRunSubJobSpawnProcessPerJob(unittest.TestCase):

    def setUp(self):
        self.assertIsNotNone(shutil.which("sh"))
        self.job = bzfs_jobrunner.Job()

    def run_and_capture(self, cmd, timeout_secs):
        """
        Helper: invoke the method, return (exit_code, [all error‑log messages]).
        """
        with patch.object(self.job.log, "error") as mock_error:
            code = self.job.run_worker_job_spawn_process_per_job(cmd, timeout_secs=timeout_secs)
            logs = [call.args[1] for call in mock_error.call_args_list]
        return code, logs

    def test_normal_completion(self):
        """Exits 0 under timeout, no errors."""
        code, logs = self.run_and_capture(["sleep", "0"], timeout_secs=1.0)
        self.assertEqual(0, code)
        self.assertListEqual([], logs)

    def test_none_timeout(self):
        """timeout_secs=None -> wait indefinitely, no errors."""
        code, logs = self.run_and_capture(["sleep", "0"], timeout_secs=None)
        self.assertEqual(0, code)
        self.assertListEqual([], logs)

    def test_nonzero_exit(self):
        """Non‑zero exit code is propagated, no errors logged."""
        code, logs = self.run_and_capture(["sh", "-c", "exit 3"], timeout_secs=1.0)
        self.assertEqual(3, code)
        self.assertListEqual([], logs)

    def test_dryrun(self):
        """dryrun mode does not actually execute the CLI command."""
        self.job.jobrunner_dryrun = True
        code, logs = self.run_and_capture(["sh", "-c", "exit 3"], timeout_secs=None)
        self.assertEqual(0, code)
        self.assertListEqual([], logs)

    def test_timeout_terminate(self):
        """Timeout->SIGTERM path: return code and first log prefix."""
        code, logs = self.run_and_capture(["sleep", "1"], timeout_secs=0.1)
        self.assertEqual(-signal.SIGTERM, code)
        self.assertTrue(
            logs and logs[0].startswith("Terminating worker job"),
            f"Expected first log starting with 'Terminating worker job', got {logs!r}",
        )

    def test_timeout_kill(self):
        """SIGTERM ignored→SIGKILL path: return code and kill‐log present."""
        code, logs = self.run_and_capture(["sh", "-c", 'trap "" TERM; sleep 1'], timeout_secs=0.1)
        self.assertEqual(-signal.SIGKILL, code)
        self.assertTrue(
            any(msg.startswith("Killing worker job") for msg in logs),
            f"Expected a log starting with 'Killing worker job', got {logs!r}",
        )


#############################################################################
@patch("bzfs.bzfs_jobrunner.Job._bzfs_run_main")
class TestRunSubJobInCurrentThread(unittest.TestCase):
    def setUp(self):
        self.job = bzfs_jobrunner.Job()

    def test_no_timeout_success(self, mock_bzfs_run_main):
        cmd = ["bzfs", "foo", "bar"]
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(0, result)
        mock_bzfs_run_main.assert_called_once_with(cmd)

    def test_timeout_flag_insertion(self, mock_bzfs_run_main):
        cmd = ["bzfs", "foo"]
        timeout = 1.234
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=timeout)
        self.assertEqual(0, result)
        mock_bzfs_run_main.assert_called_once_with(["bzfs", "--timeout=1234milliseconds", "foo"])

    def test_called_process_error_returns_returncode(self, mock_bzfs_run_main):
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = subprocess.CalledProcessError(returncode=7, cmd=cmd)
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(7, result)

    def test_system_exit_with_code(self, mock_bzfs_run_main):
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = SystemExit(3)
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(3, result)

    def test_system_exit_without_code(self, mock_bzfs_run_main):
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = SystemExit
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertIsNone(result)

    def test_dryrun(self, mock_bzfs_run_main):
        """dryrun mode does not actually execute the CLI command."""
        self.job.jobrunner_dryrun = True
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = SystemExit(3)
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(0, result)

    def test_generic_exception_returns_die_status(self, mock_bzfs_run_main):
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = SystemExit(die_status)
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(bzfs_jobrunner.die_status, result)

    def test_single_element_cmd_edge(self, mock_bzfs_run_main):
        cmd = ["bzfs"]
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(0, result)
        mock_bzfs_run_main.assert_called_once_with(cmd)

    def test_unexpected_exception(self, mock_bzfs_run_main):
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = ValueError
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(bzfs_jobrunner.die_status, result)


#############################################################################
class TestRunSubJob(unittest.TestCase):
    def setUp(self):
        self.job = bzfs_jobrunner.Job()
        self.job.stats.jobs_all = 1
        self.assertIsNone(self.job.first_exception)

    def test_success(self):
        result = self.job.run_subjob(cmd=["true"], name="j0", timeout_secs=None, spawn_process_per_job=True)
        self.assertEqual(0, result)

    def test_failure(self):
        self.job.stats.jobs_all = 2
        result = self.job.run_subjob(cmd=["false"], name="j0", timeout_secs=None, spawn_process_per_job=True)
        self.assertNotEqual(0, result)
        self.assertIsInstance(self.job.first_exception, int)
        self.assertTrue(self.job.first_exception != 0)

        result = self.job.run_subjob(cmd=["false"], name="j1", timeout_secs=None, spawn_process_per_job=True)
        self.assertNotEqual(0, result)
        self.assertIsInstance(self.job.first_exception, int)
        self.assertTrue(self.job.first_exception != 0)

    def test_timeout(self):
        self.job.stats.jobs_all = 2
        result = self.job.run_subjob(cmd=["sleep", "0"], name="j0", timeout_secs=1, spawn_process_per_job=True)
        self.assertEqual(0, result)
        result = self.job.run_subjob(cmd=["sleep", "1"], name="j1", timeout_secs=0.01, spawn_process_per_job=True)
        self.assertNotEqual(0, result)

    def test_nonexisting_cmd(self):
        with self.assertRaises(FileNotFoundError):
            self.job.run_subjob(cmd=["sleep_nonexisting_cmd", "1"], name="j0", timeout_secs=None, spawn_process_per_job=True)


#############################################################################
class TestXLoadModule(unittest.TestCase):
    # build a process-lifetime console-script stub
    _STUB_DIR = tempfile.mkdtemp(prefix="bzfs_stub_")
    _STUB_WRAPPER = os.path.join(_STUB_DIR, "bzfs")
    with open(_STUB_WRAPPER, "w") as fp:
        fp.write("import importlib as _il\n" "_il.import_module('bzfs.bzfs')\n")
    os.chmod(_STUB_WRAPPER, 0o755)

    def test_command_not_found_on_path_raises_error(self):
        with self.assertRaises(SystemExit) as context:
            bzfs_jobrunner.load_module("nonexistent_prog")
        self.assertEqual(die_status, context.exception.code)

    def test_a_wrapper_script_triggers_package_import(self):
        progname = "bzfs"
        sentinel = types.ModuleType("sentinel")

        patched_path = f"{self._STUB_DIR}{os.pathsep}{os.environ.get('PATH', '')}"
        with patch.dict(os.environ, {"PATH": patched_path}, clear=False):
            real_import = importlib.import_module

            def fake_import(name, *args, **kw):
                if name in (f"{progname}.{progname}", "bzfs.bzfs"):
                    return sentinel
                return real_import(name, *args, **kw)

            with patch("importlib.import_module", side_effect=fake_import):
                result = bzfs_jobrunner.load_module(progname)
                self.assertIs(result, sentinel)

        # cleanup sys.modules
        for key in ["sentinel"]:
            sys.modules.pop(key, None)

    def test_load_module_directly_without_wrapper_script(self):
        progname = "bzfs"
        # sys.modules.pop(progname, None)
        bzfs = bzfs_jobrunner.load_module(progname)
        self.assertIsNotNone(bzfs.get_simple_logger(progname))
        bzfs_jobrunner.load_module(progname)
        self.assertIsNotNone(bzfs.get_simple_logger(progname))

    def test_load_module_directly_withsibling_and_without_wrapper_script(self):
        progname = "bzfs"
        with patch("shutil.which", return_value=None), patch.object(sys, "argv", new=["./bzfs/bzfs_jobrunner"]):
            bzfs = bzfs_jobrunner.load_module(progname)
        self.assertIsNotNone(bzfs.get_simple_logger(progname))


#############################################################################
def is_solaris_zfs() -> bool:
    return platform.system() == "SunOS"
