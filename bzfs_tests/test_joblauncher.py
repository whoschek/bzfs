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
#
"""Unit tests for the YAML-driven ``bzfs_joblauncher`` CLI.

Purpose:
    Validate command construction, host subsetting, passthrough of extra args,
    environment export, and plan normalization.

Skip behavior:
    Tests are skipped if the optional YAML parser (PyYAML) is not installed.
"""

from __future__ import annotations
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

YAML_AVAILABLE = importlib.util.find_spec("yaml") is not None

if YAML_AVAILABLE:
    from bzfs_main import bzfs_joblauncher


@unittest.skipUnless(YAML_AVAILABLE, "PyYAML not installed; skipping joblauncher tests")
class TestJobLauncher(unittest.TestCase):
    """High-value tests around YAML -> jobrunner translation."""

    def _make_yaml(self, payload: dict[str, Any]) -> str:
        path = os.path.join(tempfile.gettempdir(), "bzfs_joblauncher_test.yaml")
        import yaml

        with open(path, "w", encoding="utf-8") as fd:
            yaml.safe_dump(payload, fd, sort_keys=True)
        return path

    def _base_payload(self) -> dict[str, Any]:
        return {
            "root_dataset_pairs": [
                {"src": "tank1/foo", "dst": "tank1/foo"},
            ],
            "recursive": True,
            "src_hosts": ["nas"],
            "dst_hosts": {"nas": ["onsite"]},
            "retain_dst_targets": {"nas": ["onsite"]},
            "dst_root_datasets": {"nas": ""},
            "src_snapshot_plan": {"prod": {"onsite": {"hourly": 36, "daily": 31}}},
            "src_bookmark_plan": {"prod": {"onsite": {"hourly": 36, "daily": 31}}},
            "dst_snapshot_plan": {"prod": {"onsite": {"hourly": 72, "daily": 62}}},
            "monitor_snapshot_plan": {"prod": {"onsite": {"hourly": {"warning": "30 minutes", "critical": "300 minutes"}}}},
            "workers": "100%",
            "work_period_seconds": 0,
            "jitter": False,
            "extra_args": [],
        }

    @patch("bzfs_main.bzfs_jobrunner.Job.run_main")
    def test_build_command_minimal_create_src(self, mock_run_main: MagicMock) -> None:
        yml = self._base_payload()
        path = self._make_yaml(yml)

        argv = ["bzfs_joblauncher", "--config", path, "--create-src-snapshots"]
        captured: dict[str, str] = {}

        def fake_run_main(*_args: Any, **_kwargs: Any) -> None:
            captured["stdin"] = sys.stdin.read()

        mock_run_main.side_effect = fake_run_main

        with patch.object(sys, "argv", argv):
            with self.assertRaises(SystemExit) as cm:
                bzfs_joblauncher.main()
        self.assertEqual(0, cm.exception.code)

        self.assertTrue(mock_run_main.called)
        cmd = mock_run_main.call_args.args[0]
        self.assertIsInstance(cmd, list)
        self.assertEqual("bzfs_jobrunner", cmd[0])
        self.assertIn("--recursive", cmd)
        self.assertIn("--create-src-snapshots", cmd)
        self.assertIn("--root-dataset-pairs", cmd)
        idx = cmd.index("--root-dataset-pairs")
        self.assertEqual(["tank1/foo", "tank1/foo"], cmd[idx + 1 : idx + 3])

        # check literal flags exist
        self.assertTrue(any(x.startswith("--src-snapshot-plan=") for x in cmd))
        self.assertTrue(any(x.startswith("--dst-snapshot-plan=") for x in cmd))
        self.assertTrue(any(x.startswith("--monitor-snapshot-plan=") for x in cmd))

        # stdin passed hosts as a literal list
        self.assertEqual(json.dumps(["nas"], separators=(",", ":")), captured.get("stdin"))

    @patch("bzfs_main.bzfs_jobrunner.Job.run_main")
    def test_subset_hosts_and_extra_args(self, mock_run_main: MagicMock) -> None:
        yml = self._base_payload()
        yml["extra_args"] = ["--jobrunner-dryrun", "--daemon-replication-frequency=secondly"]
        path = self._make_yaml(yml)

        argv = [
            "bzfs_joblauncher",
            "--config",
            path,
            "--replicate",
            "--src-host",
            "nas",
            "--dst-host",
            "nas",
        ]
        with patch.object(sys, "argv", argv):
            with self.assertRaises(SystemExit) as cm:
                bzfs_joblauncher.main()
        self.assertEqual(0, cm.exception.code)

        cmd = mock_run_main.call_args.args[0]
        self.assertIn("--replicate", cmd)
        # host subsetting flags preserved
        self.assertIn("--src-host", cmd)
        self.assertIn("nas", cmd)
        self.assertIn("--dst-host", cmd)
        # extra args forwarded
        self.assertIn("--jobrunner-dryrun", cmd)
        self.assertIn("--daemon-replication-frequency=secondly", cmd)

    @patch("bzfs_main.bzfs_jobrunner.Job.run_main")
    def test_dst_plan_multiplier(self, mock_run_main: MagicMock) -> None:
        yml = self._base_payload()
        # Remove explicit dst plan; use multiplier instead
        yml.pop("dst_snapshot_plan")
        yml["dst_snapshot_plan_multiplier"] = 2
        path = self._make_yaml(yml)

        argv = ["bzfs_joblauncher", "--config", path, "--prune-dst-snapshots"]
        with patch.object(sys, "argv", argv):
            with self.assertRaises(SystemExit) as cm:
                bzfs_joblauncher.main()
        self.assertEqual(0, cm.exception.code)

        cmd = mock_run_main.call_args.args[0]
        flag = next(x for x in cmd if x.startswith("--dst-snapshot-plan="))
        literal = flag.split("=", 1)[1]
        plan = json.loads(literal)
        self.assertEqual(72, plan["prod"]["onsite"]["hourly"])  # 36 * 2
        self.assertEqual(62, plan["prod"]["onsite"]["daily"])  # 31 * 2

    # env export was intentionally removed for security reasons


def suite() -> unittest.TestSuite:
    if not YAML_AVAILABLE:
        return unittest.TestSuite()  # empty suite
    return unittest.TestLoader().loadTestsFromTestCase(TestJobLauncher)
