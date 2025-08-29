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
"""Unit tests for the ``bzfs_joblauncher`` CLI; Validates YAML parsing and command forwarding."""

from __future__ import annotations
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - handled via skip
    yaml = None

from bzfs_tests.abstract_testcase import AbstractTestCase


#############################################################################
def suite() -> unittest.TestSuite:
    if yaml is None:
        return unittest.TestSuite()
    test_cases = [TestJobLauncher]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(tc) for tc in test_cases)


#############################################################################
class TestJobLauncher(AbstractTestCase):

    @patch("subprocess.run")
    def test_launch(self, mock_run: MagicMock) -> None:
        mock_run.return_value.returncode = 0
        config = {
            "root_dataset_pairs": [
                ["tank1/foo", "tank1/foo"],
                ["tank1/bar", "tank1/bar"],
            ],
            "recursive": True,
            "src_hosts": ["nas"],
            "dst_hosts": {"nas": [""]},
            "retain_dst_targets": {"nas": [""]},
            "dst_root_datasets": {"nas": ""},
            "src_snapshot_plan": {"prod": {"onsite": {"daily": 1}}},
            "dst_snapshot_plan": {"prod": {"onsite": {"daily": 1}}},
            "src_bookmark_plan": {"prod": {"onsite": {"daily": 1}}},
            "monitor_snapshot_plan": {"prod": {"onsite": {"daily": {"warning": "1 hour", "critical": "2 hours"}}}},
            "workers": "100%",
            "work_period_seconds": 0,
            "jitter": False,
            "worker_timeout_seconds": None,
            "extra_args": ["--job-id=test"],
            "env": {"TEST_ENV": "1"},
        }
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            yaml.safe_dump(config, tmp)
            path = tmp.name
        try:
            with patch.dict(os.environ, {}, clear=True):
                from bzfs_main import bzfs_joblauncher

                exit_code = bzfs_joblauncher.run([path, "--create-src-snapshots"])
                self.assertEqual(0, exit_code)
                mock_run.assert_called_once()
                cmd = mock_run.call_args.args[0]
                self.assertIn("--recursive", cmd)
                self.assertIn("--dst-hosts={'nas': ['']}", cmd)
                self.assertIn("--root-dataset-pairs", cmd)
                self.assertIn("--job-id=test", cmd)
                self.assertEqual("['nas']", mock_run.call_args.kwargs["input"])
                self.assertNotIn("TEST_ENV", os.environ)
        finally:
            os.unlink(path)

    def test_build_cmd_mapping(self) -> None:
        config = {
            "root_dataset_pairs": [["tank/a", "tank/a"], ["tank/b", "tank/b"]],
            "recursive": True,
            "src_hosts": ["s"],
            "dst_hosts": {"d": ["t"]},
            "retain_dst_targets": {"d": ["t"]},
            "dst_root_datasets": {"d": "root"},
            "src_snapshot_plan": {"prod": {"offsite": {"daily": 1}}},
            "src_bookmark_plan": {"prod": {"offsite": {"daily": 1}}},
            "dst_snapshot_plan": {"prod": {"offsite": {"daily": 1}}},
            "monitor_snapshot_plan": {"prod": {"offsite": {"daily": {"warning": "1h", "critical": "2h"}}}},
            "workers": "100%",
            "work_period_seconds": 60,
            "jitter": True,
            "worker_timeout_seconds": 30,
            "extra_args": ["--foo"],
        }
        from bzfs_main import bzfs_joblauncher

        cmd, stdin_input = bzfs_joblauncher._build_cmd(config, ["--bar"])
        expected_flags = [
            "bzfs_jobrunner",
            "--recursive",
            "--dst-hosts={'d': ['t']}",
            "--retain-dst-targets={'d': ['t']}",
            "--dst-root-datasets={'d': 'root'}",
            "--src-snapshot-plan={'prod': {'offsite': {'daily': 1}}}",
            "--src-bookmark-plan={'prod': {'offsite': {'daily': 1}}}",
            "--dst-snapshot-plan={'prod': {'offsite': {'daily': 1}}}",
            "--monitor-snapshot-plan={'prod': {'offsite': {'daily': {'warning': '1h', 'critical': '2h'}}}}",
            "--workers=100%",
            "--work-period-seconds=60",
            "--jitter",
            "--worker-timeout-seconds=30",
            "--foo",
            "--bar",
            "--root-dataset-pairs",
            "tank/a",
            "tank/a",
            "tank/b",
            "tank/b",
        ]
        self.assertEqual(expected_flags, cmd)
        self.assertEqual("['s']", stdin_input)

    def test_run_missing_command(self) -> None:
        config = {"src_hosts": ["s"]}
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            yaml.safe_dump(config, tmp)
            path = tmp.name
        try:
            from bzfs_main import bzfs_joblauncher

            with patch("sys.stderr", new_callable=lambda: tempfile.TemporaryFile(mode="w+")) as fake_err:
                exit_code = bzfs_joblauncher.run([path])
                fake_err.seek(0)
                err = fake_err.read()
            self.assertEqual(5, exit_code)
            self.assertIn("Missing command", err)
        finally:
            os.unlink(path)

    @patch("subprocess.run")
    def test_run_command_in_config(self, mock_run: MagicMock) -> None:
        config = {"src_hosts": ["s"], "extra_args": ["--replicate"]}
        mock_run.return_value.returncode = 0
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            yaml.safe_dump(config, tmp)
            path = tmp.name
        try:
            from bzfs_main import bzfs_joblauncher

            exit_code = bzfs_joblauncher.run([path])
            self.assertEqual(0, exit_code)
            mock_run.assert_called_once()
            self.assertIn("--replicate", mock_run.call_args.args[0])
        finally:
            os.unlink(path)

    def test_invalid_yaml(self) -> None:
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            tmp.write("root_dataset_pairs: [ [tank1/foo, tank1/bar]")
            path = tmp.name
        try:
            from bzfs_main import bzfs_joblauncher

            with patch("sys.stderr", new_callable=lambda: tempfile.TemporaryFile(mode="w+")) as fake_err:
                exit_code = bzfs_joblauncher.run([path, "--replicate"])
                fake_err.seek(0)
                err = fake_err.read()
            self.assertEqual(5, exit_code)
            self.assertIn("Failed to parse YAML", err)
        finally:
            os.unlink(path)

    def test_missing_dependency(self) -> None:
        from bzfs_main import bzfs_joblauncher

        with patch.object(bzfs_joblauncher, "yaml", None):
            with patch("sys.stderr", new_callable=lambda: tempfile.TemporaryFile(mode="w+")) as fake_err:
                exit_code = bzfs_joblauncher.run(["config.yaml", "--replicate"])
                fake_err.seek(0)
                err = fake_err.read()
        self.assertEqual(5, exit_code)
        self.assertIn("Install optional dependency", err)
