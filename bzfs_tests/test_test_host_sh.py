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
"""Unit tests for `test_host.sh` input validation.

These tests ensure dangerous `bzfs_test_remote_path` values are rejected before any external commands (rsync/ssh) run.
"""

from __future__ import (
    annotations,
)
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import (
    Path,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestRemotePathValidation,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
@unittest.skipIf(shutil.which("bash") is None, "Requires bash")
class TestRemotePathValidation(unittest.TestCase):

    def setUp(self) -> None:
        self.repo_root: Path = Path(__file__).resolve().parents[1]
        self.script: str = os.path.join(self.repo_root, "test_host.sh")

    def test_rejects_dangerous_remote_paths_before_running_rsync(self) -> None:
        # Assert that input validation rejects dangerous paths before rsync/ssh runs.
        invalid_paths: list[str] = [
            "/",
            "//",
            "///",
            ".",
            "..",
            "./x",
            "../x",
            "/.",
            "/..",
            "/./x",
            "/../x",
            "x/.",
            "x/..",
            "x/./y",
            "x/../y",
            "a/.hidden/b",
            "/tmp/bzfs",
            "/tmp/.hidden",
            "/var/tmp/bzfs-tests_1.2.3",
        ]

        for remote_path in invalid_paths:
            with self.subTest(remote_path=remote_path):
                result, marker_content = self._run_test_host_with_stubs(remote_path)
                self.assertEqual(1, result.returncode)
                self.assertIn("error: invalid bzfs_test_remote_path:", result.stderr)
                self.assertIsNone(marker_content)

    def test_allows_reasonable_remote_paths(self) -> None:
        # For valid paths, execution reaches the `rsync` invocation. Since `test_host.sh` runs
        # with `set -e`, the rsync stub's exit code becomes the script's exit code.
        valid_paths: list[str] = [
            "foo",
            "tmp/bzfs",
        ]

        for remote_path in valid_paths:
            with self.subTest(remote_path=remote_path):
                result, marker_content = self._run_test_host_with_stubs(remote_path)
                self.assertEqual(99, result.returncode)
                self.assertEqual("rsync_called\n", marker_content)

    def _run_test_host_with_stubs(self, remote_path: str) -> tuple[subprocess.CompletedProcess[str], str | None]:
        """Runs `test_host.sh` with stubbed rsync/ssh and returns process + marker content."""
        # The test prepends a tmp dir to PATH env var so `test_host.sh` only invokes dummy/stub `rsync`/`ssh` executables.
        # The stubs record whether they were invoked by appending a line to a marker file.
        with tempfile.TemporaryDirectory(prefix="bzfs_test_test_host_") as tmpdir:
            marker_file = Path(os.path.join(tmpdir, "stub_called.txt"))
            self._write_executable_stub(
                Path(os.path.join(tmpdir, "rsync")),
                f"#!/usr/bin/env sh\necho rsync_called >> {marker_file!s}\nexit 99\n",
            )
            self._write_executable_stub(
                Path(os.path.join(tmpdir, "ssh")),
                f"#!/usr/bin/env sh\necho ssh_called >> {marker_file!s}\nexit 98\n",
            )

            env = os.environ.copy()
            env["PATH"] = f"{tmpdir}{os.pathsep}{env.get('PATH', '')}"
            env["bzfs_test_remote_userhost"] = "u@h"
            env["bzfs_test_remote_path"] = remote_path
            env["bzfs_test_remote_private_key"] = "_my_test_dummy_"

            marker_file.unlink(missing_ok=True)
            result = subprocess.run(
                ["bash", str(self.script)],
                check=False,
                cwd=self.repo_root,
                env=env,
                text=True,
                capture_output=True,
            )
            marker_content = marker_file.read_text(encoding="utf-8") if marker_file.exists() else None
            return result, marker_content

    def _write_executable_stub(self, path: Path, content: str) -> None:
        """Writes an executable shell stub to `path` with the given `content`."""
        path.write_text(content, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
