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
import shutil
import signal
import subprocess
import sys
import tempfile
import types
import unittest
from subprocess import DEVNULL, PIPE
from unittest.mock import patch, MagicMock

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
    def test_validate_is_subset(self):
        bzfs_jobrunner.validate_is_subset([1], [1, 2], "x", "y")
        bzfs_jobrunner.validate_is_subset([], [1, 2], "x", "y")
        bzfs_jobrunner.validate_is_subset([], [], "x", "y")
        with self.assertRaises(AssertionError):
            bzfs_jobrunner.validate_is_subset([3], [1, 2], "x", "y")
        with self.assertRaises(AssertionError):
            bzfs_jobrunner.validate_is_subset([3, 4], [], "x", "y")

    def test_pretty_print_formatter(self):
        self.assertIsNotNone(str(bzfs_jobrunner.pretty_print_formatter({"foo": "bar"})))


#############################################################################
class TestSkipDatasetsWithNonExistingDstPool(unittest.TestCase):
    def setUp(self):
        self.job = bzfs_jobrunner.Job()
        self.job.log = MagicMock()

    def test_empty_input_raises(self):
        with self.assertRaises(AssertionError):
            self.job.skip_datasets_with_nonexisting_dst_pool([])

    @patch("subprocess.run")
    def test_single_existing_pool(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\n", stderr="")
        pairs = [("-:srcpool1/src1", "-:dstpool1/dst1")]
        result = self.job.skip_datasets_with_nonexisting_dst_pool(pairs)
        self.assertListEqual(pairs, result)
        self.assertSetEqual({"-:dstpool1"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool1"}, self.job.cache_known_dst_pools)
        expected_cmd = "zfs list -t filesystem,volume -Hp -o name".split(" ") + ["dstpool1"]
        mock_run.assert_called_once_with(expected_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True)

    @patch("subprocess.run")
    def test_single_nonexisting_pool(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        pairs = [("-:srcpool2/src2", "-:dstpool2/dst2")]
        result = self.job.skip_datasets_with_nonexisting_dst_pool(pairs)
        self.assertListEqual([], result)
        self.assertSetEqual(set(), self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool2"}, self.job.cache_known_dst_pools)
        self.job.log.warning.assert_called_once_with(
            "Skipping dst dataset for which dst pool does not exist: %s", "-:dstpool2/dst2"
        )

    @patch("subprocess.run")
    def test_multiple_pools_mixed(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\n", stderr="")
        pairs = [
            ("srcpool1/src1", "-:dstpool1/dst1"),
            ("srcpool2/src2", "-:dstpool2/dst2"),
            ("srcpool3/src3", "-:dstpool1/dst3"),
        ]
        result = self.job.skip_datasets_with_nonexisting_dst_pool(pairs)
        self.assertListEqual([("srcpool1/src1", "-:dstpool1/dst1"), ("srcpool3/src3", "-:dstpool1/dst3")], result)
        self.assertSetEqual({"-:dstpool1"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool1", "-:dstpool2"}, self.job.cache_known_dst_pools)
        self.job.log.warning.assert_called_once_with(
            "Skipping dst dataset for which dst pool does not exist: %s", "-:dstpool2/dst2"
        )

    @patch("subprocess.run")
    def test_caching_avoids_subprocess_run(self, mock_run):
        self.job.cache_existing_dst_pools = {"-:dstpool1"}
        self.job.cache_known_dst_pools = {"-:dstpool1", "-:dstpool2"}
        pairs = [("srcpool1/src1", "-:dstpool1/dst1"), ("srcpool2/src2", "-:dstpool2/dst2")]
        result = self.job.skip_datasets_with_nonexisting_dst_pool(pairs)
        self.assertListEqual([("srcpool1/src1", "-:dstpool1/dst1")], result)
        mock_run.assert_not_called()
        self.job.log.warning.assert_called_once_with(
            "Skipping dst dataset for which dst pool does not exist: %s", "-:dstpool2/dst2"
        )

    @patch("subprocess.run")
    def test_multiple_pools_exist_returns_all(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\ndstpool2\n", stderr="")
        pairs = [("srcpool1/src1", "-:dstpool1/dst1"), ("srcpool2/src2", "-:dstpool2/dst2")]
        result = self.job.skip_datasets_with_nonexisting_dst_pool(pairs)
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
        res1 = self.job.skip_datasets_with_nonexisting_dst_pool(pairs1)
        self.assertListEqual([("srcpool1/src1", "-:dstpool1/dst1")], res1)
        pairs2 = [("srcpool1/src2", "-:dstpool2/dst2"), ("srcpool3/src3", "-:dstpool3/dst3")]
        res2 = self.job.skip_datasets_with_nonexisting_dst_pool(pairs2)
        self.assertListEqual([("srcpool3/src3", "-:dstpool3/dst3")], res2)
        self.assertEqual(2, mock_run.call_count)

    @patch("subprocess.run")
    def test_multislash_dataset_names(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\n", stderr="")
        pairs = [("srcpool1/src1", "-:dstpool1/child/grand")]
        result = self.job.skip_datasets_with_nonexisting_dst_pool(pairs)
        self.assertListEqual(pairs, result)
        self.assertIn("-:dstpool1", self.job.cache_existing_dst_pools)

    @patch("subprocess.run")
    def test_multiple_warnings_for_nonexistent(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        pairs = [("srcpool1/src1", "-:srcpool1/dst1"), ("srcpool2/src2", "-:dstpool2/dst2")]
        result = self.job.skip_datasets_with_nonexisting_dst_pool(pairs)
        self.assertListEqual([], result)
        expected_calls = [
            unittest.mock.call("Skipping dst dataset for which dst pool does not exist: %s", "-:srcpool1/dst1"),
            unittest.mock.call("Skipping dst dataset for which dst pool does not exist: %s", "-:dstpool2/dst2"),
        ]
        self.assertEqual(expected_calls, self.job.log.warning.mock_calls)

    @patch("subprocess.run")
    def test_duplicate_pool_input(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool1\n", stderr="")
        pairs = [("srcpool1/src1", "-:dstpool1/dst1"), ("srcpool2/src2", "-:dstpool1/dst2")]
        result = self.job.skip_datasets_with_nonexisting_dst_pool(pairs)
        self.assertListEqual(pairs, result)
        mock_run.assert_called_once()

    # dst without slash, existing pool
    @patch("subprocess.run")
    def test_dst_without_slash_existing_pool(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="dstpool4\n", stderr="")
        pairs = [("srcpool4/src4", "-:dstpool4")]
        result = self.job.skip_datasets_with_nonexisting_dst_pool(pairs)
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
        result = self.job.skip_datasets_with_nonexisting_dst_pool(pairs)
        self.assertListEqual([], result)
        self.assertSetEqual(set(), self.job.cache_existing_dst_pools)
        self.assertSetEqual({"-:dstpool2"}, self.job.cache_known_dst_pools)
        self.job.log.warning.assert_called_once_with(
            "Skipping dst dataset for which dst pool does not exist: %s", "-:dstpool2"
        )

    # non-local dst and without slash
    @patch("subprocess.run")
    def test_nonlocaldst_without_slash(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        pairs = [("srcpool4/src4", "127.0.0.1:dstpool2")]
        result = self.job.skip_datasets_with_nonexisting_dst_pool(pairs)
        self.assertListEqual(pairs, result)
        self.assertSetEqual({"127.0.0.1:dstpool2"}, self.job.cache_existing_dst_pools)
        self.assertSetEqual({"127.0.0.1:dstpool2"}, self.job.cache_known_dst_pools)
        self.assertEqual([], self.job.log.warning.mock_calls)


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
        mock_bzfs_run_main.side_effect = ValueError("unexpected")
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(bzfs_jobrunner.die_status, result)

    def test_single_element_cmd_edge(self, mock_bzfs_run_main):
        cmd = ["bzfs"]
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(0, result)
        mock_bzfs_run_main.assert_called_once_with(cmd)


#############################################################################
class TestRunSubJob(unittest.TestCase):
    def setUp(self):
        self.job = bzfs_jobrunner.Job()
        self.job.stats.jobs_all = 1
        self.assertIsNone(self.job.first_exception)

    def test_success(self):
        result = self.job.run_subjob(cmd=["true"], timeout_secs=None, spawn_process_per_job=True)
        self.assertTrue(result)

    def test_failure(self):
        result = self.job.run_subjob(cmd=["false"], timeout_secs=None, spawn_process_per_job=True)
        self.assertFalse(result)
        self.assertIsInstance(self.job.first_exception, int)
        self.assertTrue(self.job.first_exception != 0)

        result = self.job.run_subjob(cmd=["false"], timeout_secs=None, spawn_process_per_job=True)
        self.assertFalse(result)
        self.assertIsInstance(self.job.first_exception, int)
        self.assertTrue(self.job.first_exception != 0)

    def test_timeout(self):
        result = self.job.run_subjob(cmd=["sleep", "0"], timeout_secs=1, spawn_process_per_job=True)
        self.assertTrue(result)
        result = self.job.run_subjob(cmd=["sleep", "1"], timeout_secs=0.01, spawn_process_per_job=True)
        self.assertFalse(result)

    def test_nonexisting_cmd(self):
        with self.assertRaises(FileNotFoundError):
            self.job.run_subjob(cmd=["sleep_nonexisting_cmd", "1"], timeout_secs=None, spawn_process_per_job=True)


#############################################################################
class TestXLoadModule(unittest.TestCase):
    # build a process-lifetime console-script stub
    _STUB_DIR = tempfile.mkdtemp(prefix="bzfs_stub_")
    _STUB_WRAPPER = os.path.join(_STUB_DIR, "bzfs")
    with open(_STUB_WRAPPER, "w") as fp:
        fp.write("import importlib as _il\n" "_il.import_module('bzfs.bzfs')\n")
    os.chmod(_STUB_WRAPPER, 0o755)

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
