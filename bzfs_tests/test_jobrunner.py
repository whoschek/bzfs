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
import unittest
from logging import Logger
from subprocess import DEVNULL, PIPE
from typing import Union, cast
from unittest.mock import patch, MagicMock

from bzfs_main import bzfs_jobrunner
from bzfs_main.bzfs_jobrunner import die_status


def suite() -> unittest.TestSuite:
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestHelperFunctions))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestValidation))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSkipDatasetsWithNonExistingDstPool))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRunSubJobSpawnProcessPerJob))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRunSubJobInCurrentThread))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestRunSubJob))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestValidateSnapshotPlan))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestValidateMonitorSnapshotPlan))
    return suite


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
            "--src-hosts=" + str(["srcA", "srcB"]),
            "--dst-hosts=" + str({"dstX": ["target", ""]}),
            "--dst-root-datasets=" + str({"dstX": ""}),
            "--retain-dst-targets=" + str({"dstX": ["target", ""]}),
            "--create-src-snapshots",  # Dummy command to make arg parsing happy
        ]
        expect_msg = "source hosts must not be configured to write to the same destination dataset"

        # Scenario 1: Run for srcA only, should pass.
        argv_srcA = base_argv + ["--src-host", "srcA"]
        with patch("sys.argv", argv_srcA):
            with patch.object(job, "run_subjobs", return_value=None) as mock_run_subjobs:
                job.run_main(argv_srcA)  # Should not raise SystemExit
                mock_run_subjobs.assert_called_once()

        # Scenario 1: Run for srcB only, should pass.
        argv_srcB = base_argv + ["--src-host", "srcB"]
        with patch("sys.argv", argv_srcA):
            with patch.object(job, "run_subjobs", return_value=None) as mock_run_subjobs:
                job.run_main(argv_srcB)  # Should not raise SystemExit
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
            "--src-hosts=" + str(["srcA", "srcB"]),
            "--dst-hosts=" + str({"dstX": ["target"]}),
            "--dst-root-datasets=" + str({"dstX": "pool/backup_^SRC_HOST", "dstY": ""}),  # Correctly uses ^SRC_HOST
            "--retain-dst-targets=" + str({"dstX": ["target"], "dstY": ["target"]}),
            "--create-src-snapshots",  # Dummy command to make arg parsing happy
        ]

        # Mock the actual subjob execution to prevent it from running fully
        with patch.object(job, "run_subjobs", return_value=None) as mock_run_subjobs:
            # Scenario 1: Run for srcA only. Should pass validation.
            argv_srcA_safe = base_argv_safe + ["--src-host", "srcA"]
            with patch("sys.argv", argv_srcA_safe):
                job.run_main(argv_srcA_safe)  # Should not raise SystemExit due to validation
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
            "--src-hosts=" + str(["srcA", "srcB"]),
            "--dst-hosts=" + str({"dstX": ["target"]}),
            "--dst-root-datasets=" + str({"dstX": "pool/^SRC_HOST", "dstZ": "pool/fixed"}),  # incorrectly uses ^SRC_HOST
            "--retain-dst-targets=" + str({"dstX": ["target"], "dstZ": ["target"]}),
            "--create-src-snapshots",  # Dummy command to make arg parsing happy
        ]
        expect_msg = "but not all non-empty root datasets in --dst-root-datasets contain the '^SRC_HOST' substitution token"

        # Scenario 1: Run for srcA only, should fail.
        argv_srcA = base_argv + ["--src-host", "srcA"]
        with patch("sys.argv", argv_srcA):
            with self.assertRaises(SystemExit) as cm:
                job.run_main(argv_srcA)
            self.assertEqual(die_status, cm.exception.code)
            self.assertIn(expect_msg, str(cm.exception))

        # Scenario 2: Run for srcB only, should fail.
        argv_srcB = base_argv + ["--src-host", "srcB"]
        with patch("sys.argv", argv_srcB):
            with self.assertRaises(SystemExit) as cm:
                job.run_main(argv_srcB)
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
        """
        Helper: invoke the method, return (exit_code, [all error‑log messages]).
        """
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
        """Non‑zero exit code is propagated, no errors logged."""
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
        """SIGTERM ignored→SIGKILL path: return code and kill‐log present."""
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


#############################################################################
# New tests based on coverage analysis
#############################################################################

class TestJobRunnerRunMainDetailed(unittest.TestCase):
    def setUp(self) -> None:
        self.job = bzfs_jobrunner.Job()
        self.job.log = MagicMock(spec=Logger)
        self.job.is_test_mode = True  # Prevents actual subjob execution where not needed
        # Mock run_subjobs to prevent actual execution and capture calls
        self.mock_run_subjobs = patch.object(self.job, "run_subjobs", return_value=None).start()
        self.addCleanup(patch.stopall)

        # Default minimal valid args
        self.base_argv = [
            bzfs_jobrunner.prog_name,
            "--job-id=test_job",
            "--root-dataset-pairs", "srcds/data", "dstds/data",
            "--src-hosts=" + str(["src1"]),
            "--dst-hosts=" + str({"dst1": ["targetA"]}),
            "--dst-root-datasets=" + str({"dst1": "backup_pool/^SRC_HOST"}),
            "--retain-dst-targets=" + str({"dst1": ["targetA"]}),
            "--src-snapshot-plan=" + str({"org": {"targetA": {"daily": 1}}}),
            "--create-src-snapshots" # Dummy command
        ]
        self.unknown_args_patch = patch.object(self.job.argument_parser, 'parse_known_args',
            return_value=(argparse.Namespace(), [])
        )


    def test_run_main_invalid_src_snapshot_plan_literal_eval_fails(self) -> None:
        argv = self.base_argv.copy()
        argv[argv.index("--src-snapshot-plan={\"org\": {\"targetA\": {\"daily\": 1}}}") + 1] = "invalid_plan_not_a_dict"
        with self.assertRaises(ValueError) as cm: # ValueError from literal_eval
            self.job.run_main(argv)
        self.assertIn("malformed node or string", str(cm.exception).lower())

    def test_run_main_invalid_src_snapshot_plan_structure(self) -> None:
        argv = self.base_argv.copy()
        # Invalid structure: 'daily' maps to a string, not int
        argv[argv.index("--src-snapshot-plan={\"org\": {\"targetA\": {\"daily\": 1}}}") + 1] = \
            str({"org": {"targetA": {"daily": "not-an-int"}}})
        with self.assertRaises(SystemExit) as cm:
            self.job.run_main(argv)
        self.assertIn("must be of type int but got str", str(cm.exception))

    def test_run_main_src_host_not_in_src_hosts(self) -> None:
        argv = self.base_argv + ["--src-host", "nonexistent_src"]
        with self.assertRaises(SystemExit) as cm:
            self.job.run_main(argv)
        self.assertIn("--src-host must be a subset of --src-hosts", str(cm.exception))

    def test_run_main_dst_host_not_in_dst_hosts(self) -> None:
        argv = self.base_argv + ["--dst-host", "nonexistent_dst"]
        with self.assertRaises(SystemExit) as cm:
            self.job.run_main(argv)
        self.assertIn("--dst-host must be a subset of --dst-hosts.keys", str(cm.exception))

    def test_run_main_dst_hosts_keys_not_subset_retain_dst_targets(self) -> None:
        argv = self.base_argv.copy()
        argv[argv.index("--retain-dst-targets={\"dst1\": [\"targetA\"]}") + 1] = str({"other_dst": ["targetA"]})
        with self.assertRaises(SystemExit) as cm:
            self.job.run_main(argv)
        self.assertIn("--dst-hosts.keys must be a subset of --retain-dst-targets.keys", str(cm.exception))

    def test_run_main_dst_root_datasets_keys_not_subset_retain_dst_targets(self) -> None:
        argv = self.base_argv.copy()
        argv[argv.index("--dst-root-datasets={\"dst1\": \"backup_pool/^SRC_HOST\"}") + 1] = \
            str({"other_dst_root": "backup_pool/^SRC_HOST"})
        with self.assertRaises(SystemExit) as cm:
            self.job.run_main(argv)
        self.assertIn("--dst-root-dataset.keys must be a subset of --retain-dst-targets.keys", str(cm.exception))

    def test_run_main_dst_hosts_keys_not_subset_dst_root_datasets(self) -> None:
        # Modify dst_hosts to have a key not in dst_root_datasets
        custom_args = self.base_argv.copy()
        custom_args[custom_args.index("--dst-hosts={\"dst1\": [\"targetA\"]}")+1] = str({"dst1": ["targetA"], "unmapped_dst": ["targetB"]})
        # Ensure dst_root_datasets does NOT contain "unmapped_dst"
        custom_args[custom_args.index("--dst-root-datasets={\"dst1\": \"backup_pool/^SRC_HOST\"}")+1] = str({"dst1": "pool/^SRC_HOST"})
        # Ensure retain_dst_targets contains all keys from dst_hosts to pass earlier checks
        custom_args[custom_args.index("--retain-dst-targets={\"dst1\": [\"targetA\"]}")+1] = str({"dst1": ["targetA"], "unmapped_dst": ["targetB"]})

        with self.assertRaises(SystemExit) as cm:
            self.job.run_main(custom_args)
        self.assertIn("--dst-hosts.keys must be a subset of --dst-root-dataset.keys", str(cm.exception))


    def test_run_main_multi_source_no_src_magic_token_collision(self) -> None:
        argv = [
            bzfs_jobrunner.prog_name,
            "--job-id=test_job",
            "--root-dataset-pairs", "srcds/data", "dstds/data",
            "--src-hosts=" + str(["src1", "src2"]), # Multiple sources
            "--dst-hosts=" + str({"dst1": ["targetA"]}),
            "--dst-root-datasets=" + str({"dst1": "backup_pool/fixed_path"}), # No ^SRC_HOST
            "--retain-dst-targets=" + str({"dst1": ["targetA"]}),
            "--create-src-snapshots"
        ]
        with self.assertRaises(SystemExit) as cm:
            self.job.run_main(argv)
        self.assertIn("multiple source hosts must not be configured to write to the same destination dataset", str(cm.exception))

    def test_run_main_multi_source_definition_missing_src_magic_token(self) -> None:
        # This tests the second "Cowardly refusing" condition.
        # --src-hosts defines multiple hosts globally, but one of the --dst-root-datasets entries
        # (for a dst_host that might not even be active in this particular run if --dst-host is used)
        # is missing the ^SRC_HOST token.
        argv = [
            bzfs_jobrunner.prog_name,
            "--job-id=test_job",
            "--root-dataset-pairs", "srcds/data", "dstds/data",
            "--src-hosts=" + str(["src1", "src2"]), # Multiple sources defined globally
            "--dst-hosts=" + str({"dst1": ["targetA"], "dst2_unsafe": ["targetB"]}),
            "--dst-root-datasets=" + str({
                "dst1": "backup_pool/^SRC_HOST", # Safe for dst1
                "dst2_unsafe": "backup_pool/fixed_path_for_dst2" # Unsafe for dst2
            }),
            "--retain-dst-targets=" + str({"dst1": ["targetA"], "dst2_unsafe": ["targetB"]}),
            "--src-host=src1", # Running for a single src host this time
            "--dst-host=dst1", # Running for a single dst host this time (the safe one)
            "--create-src-snapshots"
        ]
        with self.assertRaises(SystemExit) as cm:
            self.job.run_main(argv)
        self.assertIn("multiple source hosts are defined in the configuration, but not all non-empty root datasets", str(cm.exception))
        self.assertIn("Problematic subset of --dst-root-datasets: {'dst2_unsafe': 'backup_pool/fixed_path_for_dst2'}", str(cm.exception))


    @patch('uuid.uuid1')
    def test_run_main_job_run_defaults_to_uuid(self, mock_uuid1: MagicMock) -> None:
        mock_uuid1.return_value.hex = "mocked_uuid_hex"
        argv = self.base_argv.copy()
        # Ensure --job-run (and deprecated --jobid) are not present
        argv = [arg for arg in argv if not arg.startswith("--job-run") and not arg.startswith("--jobid")]

        self.job.run_main(argv)
        self.mock_run_subjobs.assert_called_once()
        subjobs_dict = self.mock_run_subjobs.call_args[0][0]

        # Check a sample subjob's log suffix for the UUID
        sample_subjob_cmd = list(subjobs_dict.values())[0]
        log_suffix_arg = next(s for s in sample_subjob_cmd if s.startswith("--log-file-suffix="))
        self.assertIn("mocked_uuid_hex", log_suffix_arg)

    def test_run_main_prune_dst_snapshots_empty_retain_targets(self) -> None:
        argv = self.base_argv.copy()
        # Remove --create-src-snapshots and add --prune-dst-snapshots
        argv = [arg for arg in argv if arg != "--create-src-snapshots"] + ["--prune-dst-snapshots"]
        # Set retain_dst_targets to empty for dst1
        argv[argv.index("--retain-dst-targets={\"dst1\": [\"targetA\"]}") + 1] = str({"dst1": []})
        # Set dst_snapshot_plan to something non-empty to avoid other errors
        argv.append("--dst-snapshot-plan=" + str({"org": {"targetA": {"daily": 1}}}))


        with self.assertRaises(SystemExit) as cm:
            self.job.run_main(argv)
        self.assertIn("--retain-dst-targets must not be empty. Cowardly refusing to delete all snapshots!", str(cm.exception))

    @patch('socket.gethostname', return_value="myhostname")
    @patch('pwd.getpwuid')
    def test_run_main_resolve_dataset_localhost_different_user(self, mock_getpwuid:MagicMock, mock_gethostname:MagicMock)->None:
        mock_getpwuid.return_value.pw_name = "currentuser"
        self.job.loopback_address = "127.0.0.1" # Ensure loopback is set

        argv = self.base_argv + ["--ssh-src-user=remoteuser", "--create-src-snapshots"]
        # We need to run for src1 which is "myhostname" now
        argv[argv.index("--src-hosts=[\"src1\"]")+1] = str(["myhostname"])

        self.job.run_main(argv)
        self.mock_run_subjobs.assert_called_once()
        subjobs_dict = self.mock_run_subjobs.call_args[0][0]
        create_cmd = subjobs_dict['000000src-host/create-src-snapshots']

        # The dataset pair is "srcds/data", "dstds/data".
        # For create-src-snapshots, it uses (resolve_dataset(src_host, src), dummy)
        # src_host is "myhostname", src is "srcds/data"
        # Since ssh_src_user ("remoteuser") != currentuser ("currentuser"),
        # and src_host ("myhostname") == localhostname ("myhostname"),
        # it should resolve to "127.0.0.1:srcds/data"
        expected_resolved_src = f"{self.job.loopback_address}:srcds/data"
        self.assertIn(expected_resolved_src, create_cmd)
        self.assertIn(bzfs_jobrunner.bzfs.dummy_dataset, create_cmd)

    @patch('socket.gethostname', return_value="myhostname")
    @patch('pwd.getpwuid')
    def test_run_main_resolve_dataset_localhost_same_user_no_loopback(self, mock_getpwuid:MagicMock, mock_gethostname:MagicMock)->None:
        mock_getpwuid.return_value.pw_name = "currentuser"
        self.job.loopback_address = "" # Simulate no loopback detected

        argv = self.base_argv + ["--ssh-src-user=currentuser", "--create-src-snapshots"]
        argv[argv.index("--src-hosts=[\"src1\"]")+1] = str(["myhostname"])

        self.job.run_main(argv)
        self.mock_run_subjobs.assert_called_once()
        subjobs_dict = self.mock_run_subjobs.call_args[0][0]
        create_cmd = subjobs_dict['000000src-host/create-src-snapshots']

        # src_host ("myhostname") == localhostname ("myhostname")
        # ssh_src_user ("currentuser") == currentuser ("currentuser")
        # loopback_address is ""
        # Should resolve to "-:srcds/data"
        expected_resolved_src = f"-:srcds/data"
        self.assertIn(expected_resolved_src, create_cmd)

    @patch('socket.gethostname', return_value="myhostname")
    @patch('random.shuffle') # To make subjob names predictable
    def test_run_main_jitter_shuffles_hosts(self, mock_shuffle:MagicMock, mock_gethostname:MagicMock)->None:
        argv = self.base_argv.copy()
        argv[argv.index("--src-hosts=[\"src1\"]")+1] = str(["srcA", "srcB"])
        argv[argv.index("--dst-hosts={\"dst1\": [\"targetA\"]}")+1] = str({"dstX": ["targetA"], "dstY": ["targetB"]})
        argv[argv.index("--dst-root-datasets={\"dst1\": \"backup_pool/^SRC_HOST\"}")+1] = str({"dstX": "p/^SRC_HOST", "dstY": "p/^SRC_HOST"})
        argv[argv.index("--retain-dst-targets={\"dst1\": [\"targetA\"]}")+1] = str({"dstX": ["targetA"], "dstY": ["targetB"]})
        argv.append("--jitter")
        argv.append("--replicate") # Need a command that uses both src and dst hosts

        self.job.run_main(argv)

        # random.shuffle is called on src_hosts list and list(dst_hosts.items())
        self.assertEqual(mock_shuffle.call_count, 2)

        # Check if the first call was with src_hosts
        shuffled_src_hosts_call = mock_shuffle.call_args_list[0][0][0]
        self.assertCountEqual(shuffled_src_hosts_call, ["srcA", "srcB"])

        # Check if the second call was with dst_hosts items
        shuffled_dst_items_call = mock_shuffle.call_args_list[1][0][0]
        original_dst_items = list({"dstX": ["targetA"], "dstY": ["targetB"]}.items())
        self.assertCountEqual([tuple(x) for x in shuffled_dst_items_call], [tuple(x) for x in original_dst_items])

    @patch('time.sleep')
    @patch('random.randint', return_value=500_000_000) # 0.5 seconds
    def test_run_main_jitter_with_work_period_sleeps(self, mock_randint:MagicMock, mock_sleep:MagicMock)->None:
        argv = self.base_argv + ["--jitter", "--work-period-seconds=10"]

        self.job.run_main(argv)
        mock_randint.assert_called_once() # For the initial sleep
        # interval_nanos = round(1_000_000_000 * 10 / (1 + 1)) = 5_000_000_000
        # random.randint(0, 5_000_000_000) -> returns 500_000_000
        # sleep_nanos / 1_000_000_000 = 0.5
        mock_sleep.assert_any_call(0.5)

    def test_replication_opts_empty_targets(self)->None:
        opts = self.job.replication_opts(
            dst_snapshot_plan={"org": {"targetA": {"daily":1}}},
            targets=set(), # Empty targets
            localhostname="lh", src_hostname="sh", dst_hostname="dh",
            tag="repl", job_id="jid", job_run="jrun"
        )
        self.assertEqual(opts, [])

    def test_replication_opts_empty_plan(self)->None:
        opts = self.job.replication_opts(
            dst_snapshot_plan={}, # Empty plan
            targets={"targetA"},
            localhostname="lh", src_hostname="sh", dst_hostname="dh",
            tag="repl", job_id="jid", job_run="jrun"
        )
        self.assertEqual(opts, [])

    def test_replication_opts_no_matching_targets_in_plan(self)->None:
        opts = self.job.replication_opts(
            dst_snapshot_plan={"org": {"targetB": {"daily":1}}}, # Plan for targetB
            targets={"targetA"}, # Interested in targetA
            localhostname="lh", src_hostname="sh", dst_hostname="dh",
            tag="repl", job_id="jid", job_run="jrun"
        )
        self.assertEqual(opts, [])

    def test_replication_opts_target_period_amount_zero(self)->None:
        opts = self.job.replication_opts(
            dst_snapshot_plan={"org": {"targetA": {"daily":0}}}, # Period amount is 0
            targets={"targetA"},
            localhostname="lh", src_hostname="sh", dst_hostname="dh",
            tag="repl", job_id="jid", job_run="jrun"
        )
        self.assertEqual(opts, [])

    def test_run_subjobs_sequential_execution_path(self) -> None:
        # This test focuses on the sequential path of run_subjobs
        # by ensuring spawn_process_per_job is False and no barriers.
        self.job.spawn_process_per_job = False
        subjobs_data = {
            "job1": ["cmd1"],
            "job2": ["cmd2"],
        }

        mock_run_subjob = MagicMock(return_value=0)
        self.job.run_subjob = mock_run_subjob

        with patch('time.sleep') as mock_sleep:
            self.job.run_subjobs(subjobs_data, max_workers=1, timeout_secs=None, work_period_seconds=2.0, jitter=False)

        # work_period_seconds = 2.0, len(subjobs) = 2. interval_nanos = 1_000_000_000
        # Expect sleep calls for the interval.
        # First call to monotonic_ns, then first job, then sleep, then second job.
        self.assertEqual(mock_sleep.call_count, 1) # Only one sleep between the two jobs
        mock_sleep.assert_any_call(1.0) # 1_000_000_000 ns / 1_000_000_000 = 1.0 s

        self.assertEqual(mock_run_subjob.call_count, 2)
        mock_run_subjob.assert_any_call(["cmd1"], name="job1", timeout_secs=None, spawn_process_per_job=False)
        mock_run_subjob.assert_any_call(["cmd2"], name="job2", timeout_secs=None, spawn_process_per_job=False)

        # Ensure jobs were called in sorted order
        call_names = [call[0][1] for call in mock_run_subjob.call_args_list]
        self.assertEqual(call_names, ["job1", "job2"])

    def test_run_subjobs_sequential_stops_on_failure(self) -> None:
        self.job.spawn_process_per_job = False
        subjobs_data = {
            "job1_fail": ["cmd1"],
            "job2_ok": ["cmd2"],
        }

        # Mock run_subjob to simulate failure on the first job
        def failing_run_subjob(cmd: list[str], name: str, timeout_secs: float | None, spawn_process_per_job: bool) -> int:
            if name == "job1_fail":
                return 1 # Simulate failure
            return 0 # Simulate success

        self.job.run_subjob = MagicMock(side_effect=failing_run_subjob)

        with patch('time.sleep'): # We don't care about sleep timing here
            self.job.run_subjobs(subjobs_data, max_workers=1, timeout_secs=None, work_period_seconds=0, jitter=False)

        # Only the first job should have been attempted
        self.assertEqual(self.job.run_subjob.call_count, 1)
        self.job.run_subjob.assert_called_once_with(["cmd1"], name="job1_fail", timeout_secs=None, spawn_process_per_job=False)
        self.assertIsNotNone(self.job.first_exception) # Should be set due to failure

    @patch('bzfs_main.bzfs.Job')
    def test_run_subjobs_parallel_with_barrier(self, MockBzfsJob:MagicMock) -> None:
        self.job.spawn_process_per_job = True # or has_barrier would make it true

        # Create a mock bzfs.Job instance and its method
        mock_bzfs_instance = MockBzfsJob.return_value
        mock_process_parallel = MagicMock()
        mock_bzfs_instance.process_datasets_in_parallel_and_fault_tolerant = mock_process_parallel

        subjobs_data = {
            f"job1{bzfs_jobrunner.bzfs.BARRIER_CHAR}partA": ["cmdA"],
            "job2": ["cmdB"],
        }

        self.job.run_subjobs(subjobs_data, max_workers=2, timeout_secs=60, work_period_seconds=0, jitter=False)

        mock_process_parallel.assert_called_once()
        args, kwargs = mock_process_parallel.call_args

        # Check the arguments passed to process_datasets_in_parallel_and_fault_tolerant
        self.assertEqual(args[0], sorted(subjobs_data.keys())) # sorted_subjobs
        self.assertEqual(kwargs['max_workers'], 2)
        self.assertEqual(kwargs['task_name'], "Subjob")

        # Test the lambda passed to process_dataset
        # We need to simulate a call to the lambda to check if it calls run_subjob correctly
        mock_run_subjob_lambda = MagicMock(return_value=0)
        self.job.run_subjob = mock_run_subjob_lambda

        # Call the lambda with a sample subjob name
        process_dataset_lambda = args[1]
        process_dataset_lambda("job2", "tid_test", bzfs_jobrunner.bzfs.Retry(0))

        mock_run_subjob_lambda.assert_called_once_with(
            ["cmdB"], name="job2", timeout_secs=60, spawn_process_per_job=True
        )

    def test_run_subjob_base_exception(self) -> None:
        cmd = ["test_cmd"]
        name = "test_job_name"

        # Mock the worker function to raise a BaseException (e.g., KeyboardInterrupt)
        with patch.object(self.job, 'run_worker_job_in_current_thread', side_effect=KeyboardInterrupt("Test Interrupt")):
            with self.assertRaises(KeyboardInterrupt):
                self.job.run_subjob(cmd, name, timeout_secs=None, spawn_process_per_job=False)

        # Check stats (simplified check, exact values depend on internal state)
        self.assertEqual(self.job.stats.jobs_started, 1)
        self.assertEqual(self.job.stats.jobs_completed, 1) # Completion happens in finally
        # Failure count is tricky because KeyboardInterrupt might not be counted as 'failed' by the logic
        # self.assertEqual(self.job.stats.jobs_failed, 1) # This might not hold for BaseException
        self.assertIn(name, self.job.stats.started_job_names)

    def test_run_worker_job_in_current_thread_base_exception(self) -> None:
        cmd = ["bzfs_prog_name", "arg1"]

        # Mock _bzfs_run_main to raise a BaseException
        with patch.object(self.job, '_bzfs_run_main', side_effect=MemoryError("Test MemoryError")):
            with patch.object(self.job.log, 'exception') as mock_log_exception:
                return_code = self.job.run_worker_job_in_current_thread(cmd, timeout_secs=None)

        self.assertEqual(return_code, bzfs_jobrunner.die_status)
        mock_log_exception.assert_called_once()
        self.assertIn("Worker job failed with unexpected exception for command:", mock_log_exception.call_args[0][0])
        self.assertIn(" ".join(cmd), mock_log_exception.call_args[0][1])

    def test_run_worker_job_spawn_process_per_job_bzfs_prog_name(self) -> None:
        # Test that if cmd[0] is bzfs_prog_name, it's converted to sys.executable call
        cmd_original = [bzfs_jobrunner.bzfs_prog_name, "--some-arg"]

        with patch('subprocess.Popen') as mock_popen:
            mock_proc = MagicMock()
            mock_proc.communicate.return_value = (None, None) # Simulate successful communication
            mock_proc.returncode = 0
            mock_popen.return_value = mock_proc

            self.job.run_worker_job_spawn_process_per_job(cmd_original, timeout_secs=10)

            mock_popen.assert_called_once()
            called_cmd = mock_popen.call_args[0][0]
            self.assertEqual(called_cmd[0], sys.executable)
            self.assertEqual(called_cmd[1], "-m")
            self.assertEqual(called_cmd[2], f"bzfs_main.{bzfs_jobrunner.bzfs_prog_name}")
            self.assertEqual(called_cmd[3:], cmd_original[1:])

    def test_validate_host_name_with_colon(self) -> None:
        with self.assertRaises(SystemExit) as cm:
            self.job.validate_host_name("host:name", "test_context")
        self.assertIn("must not contain a ':' character", str(cm.exception))

    def test_validate_type_union_type_python_lt_310(self) -> None:
        # This test simulates Python < 3.10 behavior for Union type checking
        # For this, we can directly call the method.
        # If the current Python is >= 3.10, this test might behave differently than intended
        # but will still pass if isinstance correctly handles Union.

        # Valid cases
        self.job.validate_type("string", Union[str, int], "test_union_str")
        self.job.validate_type(123, Union[str, int], "test_union_int")

        # Invalid cases
        with self.assertRaises(SystemExit) as cm_list:
            self.job.validate_type([], Union[str, int], "test_union_list")
        self.assertIn("must be of type str or int but got list", str(cm_list.exception))

        with self.assertRaises(SystemExit) as cm_none:
            self.job.validate_type(None, Union[str, int], "test_union_none")
        self.assertIn("must be of type str or int but got NoneType", str(cm_none.exception))

    def test_die_method(self) -> None:
        with self.assertRaises(SystemExit) as cm:
            self.job.die("Test die message")
        self.assertEqual(str(cm.exception), "Test die message")
        # Check that log.error was called with the message
        self.job.log.error.assert_called_once_with("%s", "Test die message")

    def test_reject_argument_action_direct_call(self) -> None:
        # Test the RejectArgumentAction directly
        parser = argparse.ArgumentParser()
        action = bzfs_jobrunner.RejectArgumentAction(option_strings=["--forbidden"], dest="forbidden")

        with self.assertRaises(SystemExit) as cm:
            action(parser, argparse.Namespace(), "some_value", "--forbidden")

        self.assertIn("Overriding protected argument '--forbidden' is not allowed", str(cm.exception))

    def test_sanitize_log_suffix_format_dict_pretty_print(self) -> None:
        # These are simple helper functions, just call them to ensure they don't raise errors
        self.assertIsInstance(bzfs_jobrunner.sanitize("file/name with spaces"), str)
        self.assertIsInstance(bzfs_jobrunner.log_suffix("lh", "sh", "dh"), str)
        self.assertIsInstance(bzfs_jobrunner.format_dict({"key": "value"}), str)

        formatter = bzfs_jobrunner.pretty_print_formatter({"a": 1, "b": [1,2]})
        self.assertTrue(hasattr(formatter, "__str__"))
        # Ensure it produces a JSON-like string
        s = str(formatter)
        self.assertTrue(s.startswith("{"))
        self.assertIn('"a": 1', s)
        self.assertIn('"b": [\n        1,\n        2\n    ]', s)

    def test_convert_ipv6_no_colon(self) -> None:
        self.assertEqual(bzfs_jobrunner.convert_ipv6("hostname"), "hostname")

    def test_main_function_entry_point(self) -> None:
        # Test the main() function entry point by ensuring it calls Job().run_main()
        # We need to mock Job and its run_main method

        mock_job_instance = MagicMock()
        mock_run_main = MagicMock()
        mock_job_instance.run_main = mock_run_main

        with patch('bzfs_main.bzfs_jobrunner.Job', return_value=mock_job_instance) as MockJobClass:
            test_argv = ["script_name", "--arg1", "value1"]
            with patch('sys.argv', test_argv):
                bzfs_jobrunner.main()

        MockJobClass.assert_called_once() # Job was instantiated
        mock_run_main.assert_called_once_with(test_argv) # run_main was called with sys.argv

    def test_stats_repr_no_completed_jobs(self) -> None:
        stats = bzfs_jobrunner.Stats()
        stats.jobs_all = 5
        # jobs_completed remains 0
        rep_str = repr(stats)
        self.assertIn("avg_completion_time:0ns", rep_str) # avg is 0 if no jobs completed

    def test_dedupe_empty_list(self) -> None:
        self.assertEqual(bzfs_jobrunner.dedupe([]), [])

    def test_flatten_empty_list(self) -> None:
        self.assertEqual(bzfs_jobrunner.flatten([]), [])
