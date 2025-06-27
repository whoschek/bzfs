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

from __future__ import annotations
import argparse
import contextlib
import io
import platform
import shutil
import signal
import subprocess
import sys
import unittest
from logging import Logger
from subprocess import DEVNULL, PIPE
from typing import Union, cast
from unittest.mock import patch, MagicMock

from bzfs_main import bzfs_jobrunner
from bzfs_main.bzfs_jobrunner import die_status


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestHelperFunctions,
        TestValidation,
        TestSkipDatasetsWithNonExistingDstPool,
        TestRunSubJobSpawnProcessPerJob,
        TestRunSubJobInCurrentThread,
        TestRunSubJob,
        TestValidateSnapshotPlan,
        TestValidateMonitorSnapshotPlan,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestHelperFunctions(unittest.TestCase):
    def setUp(self) -> None:
        self.job = bzfs_jobrunner.Job()

    def test_validate_type(self) -> None:
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

    def test_validate_non_empty_string(self) -> None:
        self.job.validate_non_empty_string("valid_string", "name")
        with self.assertRaises(SystemExit):
            self.job.validate_non_empty_string("", "name")

    def test_validate_non_negative_int(self) -> None:
        self.job.validate_non_negative_int(1, "name")
        self.job.validate_non_negative_int(0, "name")
        with self.assertRaises(SystemExit):
            self.job.validate_non_negative_int(-1, "name")

    def test_validate_true(self) -> None:
        self.job.validate_true(1, "name")
        with self.assertRaises(SystemExit):
            self.job.validate_true(0, "name")

    def test_validate_is_subset(self) -> None:
        self.job.validate_is_subset(["1"], ["1", "2"], "x", "y")
        self.job.validate_is_subset([], ["1", "2"], "x", "y")
        self.job.validate_is_subset([], [], "x", "y")
        with self.assertRaises(SystemExit):
            self.job.validate_is_subset(["3"], ["1", "2"], "x", "y")
        with self.assertRaises(SystemExit):
            self.job.validate_is_subset(["3", "4"], [], "x", "y")
        with self.assertRaises(SystemExit):
            self.job.validate_is_subset("foo", ["3"], "x", "y")
        with self.assertRaises(SystemExit):
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
            self.assertEqual("127.0.0.1", bzfs_jobrunner.detect_loopback_address())
            self.assertEqual(1, socket_factory.call_count)

    def test_detect_loopback_address_ipv6_fallback(self) -> None:
        ipv4_fail = self._make_mock_socket(OSError("No IPv4"))
        ipv6_ok = self._make_mock_socket()
        with patch("socket.socket", side_effect=[ipv4_fail, ipv6_ok]) as socket_factory:
            self.assertEqual("::1", bzfs_jobrunner.detect_loopback_address())
            self.assertEqual(2, socket_factory.call_count)

    def test_detect_loopback_address_neither_supported(self) -> None:
        ipv4_fail = self._make_mock_socket(OSError("No IPv4"))
        ipv6_fail = self._make_mock_socket(OSError("No IPv6"))
        with patch("socket.socket", side_effect=[ipv4_fail, ipv6_fail]) as socket_factory:
            self.assertEqual("", bzfs_jobrunner.detect_loopback_address())
            self.assertEqual(2, socket_factory.call_count)

    def test_pretty_print_formatter(self) -> None:
        self.assertIsNotNone(str(bzfs_jobrunner.pretty_print_formatter({"foo": "bar"})))

    def test_help(self) -> None:
        if is_solaris_zfs():
            self.skipTest("FIXME: BlockingIOError: [Errno 11] write could not complete without blocking")
        parser = bzfs_jobrunner.argument_parser()
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as e:
            parser.parse_args(["--help"])
        self.assertEqual(0, e.exception.code)

    def test_version(self) -> None:
        parser = bzfs_jobrunner.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["--version"])
        self.assertEqual(0, e.exception.code)

    def test_sorted_dict_empty_dictionary_returns_empty(self) -> None:
        result: dict[str, int] = bzfs_jobrunner.sorted_dict({})
        self.assertEqual(result, {})

    def test_sorted_dict_single_key_value_pair_is_sorted_correctly(self) -> None:
        result: dict[str, int] = bzfs_jobrunner.sorted_dict({"a": 1})
        self.assertEqual(result, {"a": 1})

    def test_sorted_dict_multiple_key_value_pairs_are_sorted_by_keys(self) -> None:
        result: dict[str, int] = bzfs_jobrunner.sorted_dict({"b": 2, "a": 1, "c": 3})
        self.assertEqual(result, {"a": 1, "b": 2, "c": 3})

    def test_sorted_dict_with_numeric_keys_is_sorted_correctly(self) -> None:
        result: dict[int, str] = bzfs_jobrunner.sorted_dict({3: "three", 1: "one", 2: "two"})
        self.assertEqual(result, {1: "one", 2: "two", 3: "three"})

    def test_sorted_dict_with_mixed_key_types_raises_error(self) -> None:
        with self.assertRaises(TypeError):
            bzfs_jobrunner.sorted_dict({"a": 1, 2: "two"})

    def test_reject_argument_action(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--secret", action=bzfs_jobrunner.RejectArgumentAction)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--secret", "x"])

    def test_inapplicable_cli_option(self) -> None:
        opts = ["--create-src-snapshots", "--job-id=myid", "--root-dataset-pairs", "src", "dst"]
        parser = bzfs_jobrunner.argument_parser()
        parser.parse_args(opts)
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["--delete-dst-snapshots"] + opts)

    def test_shuffle_dict_preserves_items(self) -> None:
        d = {"a": 1, "b": 2, "c": 3}

        def fake_shuffle(lst: list) -> None:
            lst.reverse()

        with patch("random.shuffle", side_effect=fake_shuffle) as mock_shuffle:
            result = bzfs_jobrunner.shuffle_dict(d)
            self.assertEqual({"c": 3, "b": 2, "a": 1}, result)
            mock_shuffle.assert_called_once()

    def test_shuffle_dict_empty(self) -> None:
        self.assertEqual({}, bzfs_jobrunner.shuffle_dict({}))

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
        self.assertEqual([("a", "b"), ("c", "d")], bzfs_jobrunner.dedupe(pairs))
        self.assertEqual(["a", "b", "c", "d"], bzfs_jobrunner.flatten([("a", "b"), ("c", "d")]))

    def test_sanitize_and_log_suffix(self) -> None:
        self.assertEqual("a!b!c!d!e!f", bzfs_jobrunner.sanitize("a b..c/d,e\\f"))
        suffix = bzfs_jobrunner.log_suffix("l o", "s/p", "d\\h")
        self.assertEqual(",l!o,s!p,d!h", suffix)

    def test_convert_ipv6(self) -> None:
        self.assertEqual("fe80||1", bzfs_jobrunner.convert_ipv6("fe80::1"))

    @unittest.skipIf(sys.platform != "linux", "This test is designed for Linux only")
    def test_get_localhost_ips(self) -> None:
        job = bzfs_jobrunner.Job()
        ips: set[str] = job.get_localhost_ips()
        self.assertTrue(len(ips) > 0)


#############################################################################
class TestValidation(unittest.TestCase):
    def test_multisource_substitution_token_validation_with_empty_target(self) -> None:
        job = bzfs_jobrunner.Job()
        job.log = MagicMock()
        job.is_test_mode = True

        # Config: 2 source hosts, 1 dest host, dst_root_datasets WITHOUT ^SRC_HOST
        base_argv = [
            bzfs_jobrunner.prog_name,
            "--job-id=test",
            "--root-dataset-pairs",
            "tank/data",
            "backup/data",
            "--src-hosts=" + str(["src1", "src2"]),
            "--dst-hosts=" + str({"dstX": ["target", ""]}),
            "--dst-root-datasets=" + str({"dstX": ""}),
            "--retain-dst-targets=" + str({"dstX": ["target", ""]}),
            "--create-src-snapshots",  # Dummy command to make arg parsing happy
        ]
        expect_msg = "source hosts must not be configured to write to the same destination dataset"

        # Scenario 1: Run for srcA only, should pass.
        argv_src1 = base_argv + ["--src-host", "src1"]
        with patch("sys.argv", argv_src1):
            with patch.object(job, "run_subjobs", return_value=None) as mock_run_subjobs:
                job.run_main(argv_src1)  # Should not raise SystemExit
                mock_run_subjobs.assert_called_once()

        # Scenario 1: Run for srcB only, should pass.
        argv_src2 = base_argv + ["--src-host", "src2"]
        with patch("sys.argv", argv_src1):
            with patch.object(job, "run_subjobs", return_value=None) as mock_run_subjobs:
                job.run_main(argv_src2)  # Should not raise SystemExit
                mock_run_subjobs.assert_called_once()

        # Scenario 3: Run for both (no --src-host filter), should fail.
        job = bzfs_jobrunner.Job()
        job.log = MagicMock()
        with patch("sys.argv", base_argv):  # No --src-host filter
            with self.assertRaises(SystemExit) as cm:
                job.run_main(base_argv)
            self.assertEqual(die_status, cm.exception.code)
            self.assertIn(expect_msg, str(cm.exception))

    def test_multisource_substitution_token_validation_passes_safe_config(self) -> None:
        job = bzfs_jobrunner.Job()
        job.log = MagicMock()
        job.is_test_mode = True

        # Config: 2 source hosts, 1 dest host, dst_root_datasets WITH ^SRC_HOST
        base_argv_safe = [
            bzfs_jobrunner.prog_name,
            "--job-id=test",
            "--root-dataset-pairs",
            "tank/data",
            "backup/data",
            "--src-hosts=" + str(["src1", "src2"]),
            "--dst-hosts=" + str({"dstX": ["target"]}),
            "--dst-root-datasets=" + str({"dstX": "pool/backup_^SRC_HOST", "dstY": ""}),  # Correctly uses ^SRC_HOST
            "--retain-dst-targets=" + str({"dstX": ["target"], "dstY": ["target"]}),
            "--create-src-snapshots",  # Dummy command to make arg parsing happy
        ]

        # Mock the actual subjob execution to prevent it from running fully
        with patch.object(job, "run_subjobs", return_value=None) as mock_run_subjobs:
            # Scenario 1: Run for srcA only. Should pass validation.
            argv_src1_safe = base_argv_safe + ["--src-host", "src1"]
            with patch("sys.argv", argv_src1_safe):
                job.run_main(argv_src1_safe)  # Should not raise SystemExit due to validation
                mock_run_subjobs.assert_called_once()  # Ensure it proceeded past validation

            mock_run_subjobs.reset_mock()
            job = bzfs_jobrunner.Job()
            job.log = MagicMock()
            job.is_test_mode = True
            with patch.object(job, "run_subjobs", return_value=None) as mock_run_subjobs_2:
                # Scenario 2: Run for both. Should pass validation.
                with patch("sys.argv", base_argv_safe):
                    job.run_main(base_argv_safe)  # Should not raise SystemExit
                    mock_run_subjobs_2.assert_called_once()

    def test_multisource_substitution_token_validation_rejects_unsafe_config(self) -> None:
        job = bzfs_jobrunner.Job()
        job.log = MagicMock()
        job.is_test_mode = True

        # Config: 2 source hosts, 1 dest host, dst_root_datasets WITHOUT ^SRC_HOST
        base_argv = [
            bzfs_jobrunner.prog_name,
            "--job-id=test",
            "--root-dataset-pairs",
            "tank/data",
            "backup/data",
            "--src-hosts=" + str(["src1", "src2"]),
            "--dst-hosts=" + str({"dstX": ["target"]}),
            "--dst-root-datasets=" + str({"dstX": "pool/^SRC_HOST", "dstZ": "pool/fixed"}),  # incorrectly uses ^SRC_HOST
            "--retain-dst-targets=" + str({"dstX": ["target"], "dstZ": ["target"]}),
            "--create-src-snapshots",  # Dummy command to make arg parsing happy
        ]
        expect_msg = "but not all non-empty root datasets in --dst-root-datasets contain the '^SRC_HOST' substitution token"

        # Scenario 1: Run for srcA only, should fail.
        argv_src1 = base_argv + ["--src-host", "src1"]
        with patch("sys.argv", argv_src1):
            with self.assertRaises(SystemExit) as cm:
                job.run_main(argv_src1)
            self.assertEqual(die_status, cm.exception.code)
            self.assertIn(expect_msg, str(cm.exception))

        # Scenario 2: Run for srcB only, should fail.
        argv_src2 = base_argv + ["--src-host", "src2"]
        with patch("sys.argv", argv_src2):
            with self.assertRaises(SystemExit) as cm:
                job.run_main(argv_src2)
            self.assertEqual(die_status, cm.exception.code)
            self.assertIn(expect_msg, str(cm.exception))

        # Scenario 3: Run for both (no --src-host filter), should fail.
        job = bzfs_jobrunner.Job()
        job.log = MagicMock()
        with patch("sys.argv", base_argv):  # No --src-host filter
            with self.assertRaises(SystemExit) as cm:
                job.run_main(base_argv)
            self.assertEqual(die_status, cm.exception.code)
            self.assertIn(expect_msg, str(cm.exception))


#############################################################################
class TestSkipDatasetsWithNonExistingDstPool(unittest.TestCase):
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
        mock_run.assert_called_once_with(expected_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True)

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
        mock_run.assert_called_once_with(expected_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True)

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
        mock_run.assert_called_once_with(expected_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True)
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
        mock_run.assert_called_once_with(expected_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True)
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
        self.assertEqual(mock_run.call_count, 1)
        called_cmd = mock_run.call_args[0][0]
        self.assertEqual(called_cmd[: len(expected_cmd_parts)], expected_cmd_parts)
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


#############################################################################
class TestRunSubJobSpawnProcessPerJob(unittest.TestCase):

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
        """dryrun mode does not actually execute the CLI command."""
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
class TestRunSubJobInCurrentThread(unittest.TestCase):
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
        """dryrun mode does not actually execute the CLI command."""
        self.job.jobrunner_dryrun = True
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = SystemExit(3)
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(0, result)

    def test_generic_exception_returns_die_status(self, mock_bzfs_run_main: MagicMock) -> None:
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = SystemExit(die_status)
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(die_status, result)

    def test_single_element_cmd_edge(self, mock_bzfs_run_main: MagicMock) -> None:
        cmd = ["bzfs"]
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(0, result)
        mock_bzfs_run_main.assert_called_once_with(cmd)

    def test_unexpected_exception(self, mock_bzfs_run_main: MagicMock) -> None:
        cmd = ["bzfs", "foo"]
        mock_bzfs_run_main.side_effect = ValueError
        result = self.job.run_worker_job_in_current_thread(cmd.copy(), timeout_secs=None)
        self.assertEqual(die_status, result)


#############################################################################
class TestRunSubJob(unittest.TestCase):
    def setUp(self) -> None:
        self.job = bzfs_jobrunner.Job()
        self.job.stats.jobs_all = 1
        self.assertIsNone(self.job.first_exception)

    def test_success(self) -> None:
        result = self.job.run_subjob(cmd=["true"], name="j0", timeout_secs=None, spawn_process_per_job=True)
        self.assertEqual(0, result)

    def test_failure(self) -> None:
        self.job.stats.jobs_all = 2
        result = self.job.run_subjob(cmd=["false"], name="j0", timeout_secs=None, spawn_process_per_job=True)
        self.assertNotEqual(0, result)
        self.assertIsInstance(self.job.first_exception, int)
        self.assertTrue(self.job.first_exception != 0)

        result = self.job.run_subjob(cmd=["false"], name="j1", timeout_secs=None, spawn_process_per_job=True)
        self.assertNotEqual(0, result)
        self.assertIsInstance(self.job.first_exception, int)
        self.assertTrue(self.job.first_exception != 0)

    def test_timeout(self) -> None:
        self.job.stats.jobs_all = 2
        result = self.job.run_subjob(cmd=["sleep", "0"], name="j0", timeout_secs=1, spawn_process_per_job=True)
        self.assertEqual(0, result)
        result = self.job.run_subjob(cmd=["sleep", "1"], name="j1", timeout_secs=0.01, spawn_process_per_job=True)
        self.assertNotEqual(0, result)

    def test_nonexisting_cmd(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self.job.run_subjob(cmd=["sleep_nonexisting_cmd", "1"], name="j0", timeout_secs=None, spawn_process_per_job=True)


#############################################################################
class TestValidateSnapshotPlan(unittest.TestCase):
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
class TestValidateMonitorSnapshotPlan(unittest.TestCase):
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
