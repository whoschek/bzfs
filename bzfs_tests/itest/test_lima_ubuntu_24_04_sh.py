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
"""Integration tests for `lima_ubuntu_24_04.sh` using real `limactl` without mocks or stubs.

The test exercises scenarios with zero, one, and two test VMs. Assumes a local Lima installation and explicit opt-in via
environment variable flag.
"""

from __future__ import (
    annotations,
)
import os
import re
import shlex
import shutil
import subprocess
import unittest
import uuid
from collections.abc import (
    Iterator,
)
from contextlib import (
    contextmanager,
)
from dataclasses import (
    dataclass,
)
from typing import (
    final,
)


#############################################################################
def suite() -> unittest.TestSuite:
    """Returns the test suite for this module."""
    test_cases = [
        TestLimaUbuntuScript,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
@final
@dataclass(frozen=True)
class ScriptRunResult:
    """Captures one script run and parsed `limactl` calls from xtrace output."""

    returncode: int
    stdout: str
    stderr: str
    calls: list[list[str]]


#############################################################################
@unittest.skipIf(shutil.which("bash") is None, "Requires bash")
@unittest.skipIf(shutil.which("limactl") is None, "Requires limactl")
@unittest.skipIf(
    os.getenv("bzfs_test_enable_lima_tests", "false") != "true", "Requires bzfs_test_enable_lima_tests=true (opt-in)"
)
@final
class TestLimaUbuntuScript(unittest.TestCase):

    def setUp(self) -> None:
        """Resolves repository paths used by the Lima workflow script tests."""
        self.repo_root: str = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        self.script_path: str = os.path.join(self.repo_root, ".github-workflow-scripts", "lima_ubuntu_24_04.sh")

    def test_a_zero_one_two_existing_test_vms(self) -> None:
        """Validates script behavior across 0/1/2 existing test VM scenarios."""
        test_suffix = uuid.uuid4().hex[:8]
        target_vm = f"bzfs-lima-{test_suffix}-target"
        other_vm = f"bzfs-lima-{test_suffix}-other"

        with self._cleanup_vms(target_vm, other_vm):
            # Scenario 0: no existing test VMs.
            self.assertIsNone(self._get_vm_status(target_vm))
            self.assertIsNone(self._get_vm_status(other_vm))
            result_zero = self._run_script_once(vm_name=target_vm)
            self.assertEqual(1, self._count_command_calls(result_zero.calls, "create"))
            self.assertEqual(1, self._count_command_calls(result_zero.calls, "start"))
            self.assertEqual("Running", self._get_vm_status(target_vm))
            self.assertIsNone(self._get_vm_status(other_vm))

            # Scenario 1: one existing running test VM (target).
            result_one = self._run_script_once(vm_name=target_vm)
            self.assertEqual(0, self._count_command_calls(result_one.calls, "create"))
            self.assertEqual(0, self._count_command_calls(result_one.calls, "start"))
            self.assertEqual("Running", self._get_vm_status(target_vm))
            self.assertIsNone(self._get_vm_status(other_vm))

            # Scenario 2: two existing test VMs where target is stopped.
            self._ensure_vm_exists(other_vm, running=True)
            self._set_vm_running(target_vm, running=False)
            self.assertEqual("Stopped", self._get_vm_status(target_vm))
            self.assertEqual("Running", self._get_vm_status(other_vm))
            result_two = self._run_script_once(vm_name=target_vm)
            self.assertEqual(0, self._count_command_calls(result_two.calls, "create"))
            self.assertEqual(1, self._count_command_calls(result_two.calls, "start"))
            self.assertEqual("Running", self._get_vm_status(target_vm))
            self.assertEqual("Running", self._get_vm_status(other_vm))

    def test_mesh_vms_shares_public_keys_across_matched_vms(self) -> None:
        """Ensures matched VMs trust each other and can ssh by VM name."""
        test_suffix = uuid.uuid4().hex[:8]
        target_vm = f"bzfs-lima-{test_suffix}-mesh-target"
        source_vm = f"bzfs-lima-{test_suffix}-mesh-source"

        with self._cleanup_vms(target_vm, source_vm):
            self._run_script_once(vm_name=source_vm)
            self._set_vm_running(source_vm, running=True)
            self.assertEqual("Running", self._get_vm_status(source_vm))
            self._run_script_once(
                vm_name=target_vm,
                extra_env={"LIMA_MESH_VMS": f"^({target_vm}|{source_vm})$"},
            )
            self.assertEqual("Running", self._get_vm_status(target_vm))
            self.assertEqual("Running", self._get_vm_status(source_vm))

            target_public_key = self._get_vm_public_key(target_vm)
            source_public_key = self._get_vm_public_key(source_vm)
            target_authorized_keys = self._get_vm_authorized_keys(target_vm)
            source_authorized_keys = self._get_vm_authorized_keys(source_vm)

            self.assertIn(source_public_key, target_authorized_keys)
            self.assertIn(target_public_key, source_authorized_keys)
            self._assert_vm_can_ssh_host_by_name(source_vm=target_vm, target_host=source_vm)
            self._assert_vm_can_ssh_host_by_name(source_vm=source_vm, target_host=target_vm)

    @contextmanager
    def _cleanup_vms(self, *vm_names: str) -> Iterator[None]:
        """Deletes listed test VMs before/after a block for idempotent cleanup."""
        for vm_name in vm_names:
            self._ensure_vm_deleted(vm_name)
        try:
            yield
        finally:
            for vm_name in vm_names:
                self._ensure_vm_deleted(vm_name)

    def _run_script_once(self, vm_name: str, extra_env: dict[str, str] | None = None) -> ScriptRunResult:
        """Runs script with isolated LIMA env, asserts success, and returns parsed limactl calls."""
        env = {key: value for key, value in os.environ.items() if not key.startswith("LIMA_")}
        env["LIMA_VM_NAME"] = vm_name
        env["LIMA_NO_RUN_TESTS"] = "true"
        env["LIMA_START_PROGRESS"] = "false"
        if extra_env is not None:
            env.update(extra_env)
        env["PS4"] = "+ "  # Keep xtrace prefix stable so _parse_limactl_calls can reliably match traced commands.
        process = subprocess.run(
            ["bash", "-x", self.script_path],
            check=False,
            cwd=self.repo_root,
            env=env,
            text=True,
            capture_output=True,
        )
        calls = self._parse_limactl_calls(process.stderr)
        result = ScriptRunResult(process.returncode, process.stdout, process.stderr, calls)
        self.assertEqual(0, result.returncode, msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}")
        return result

    def _parse_limactl_calls(self, stderr: str) -> list[list[str]]:
        """Extracts traced `limactl` command arguments from bash xtrace output."""
        calls: list[list[str]] = []
        for line in stderr.splitlines():
            match = re.match(r"^\++\s+(limactl\b.*)$", line)
            if match is None:
                continue
            command = match.group(1)
            try:
                tokens = shlex.split(command)
            except ValueError:
                continue
            if not tokens or tokens[0] != "limactl":
                continue
            calls.append(tokens[1:])
        return calls

    def _count_command_calls(self, calls: list[list[str]], command: str) -> int:
        """Counts how often a top-level `limactl` command appears."""
        return sum(1 for call in calls if call and call[0] == command)

    def _ensure_vm_deleted(self, vm_name: str) -> None:
        """Stops and deletes a VM; ignores errors when VM is absent."""
        self._run_limactl(["stop", "--tty=false", "--force", vm_name], check=False)
        self._run_limactl(["delete", "--tty=false", "--force", vm_name], check=False)

    def _ensure_vm_exists(self, vm_name: str, running: bool) -> None:
        """Creates a test VM if missing and sets requested running state."""
        if self._get_vm_status(vm_name) is None:
            self._run_limactl(
                [
                    "create",
                    "--tty=false",
                    f"--name={vm_name}",
                    "--containerd=none",
                    "template:ubuntu-24.04",
                ],
                check=True,
            )
        self._set_vm_running(vm_name, running=running)

    def _set_vm_running(self, vm_name: str, running: bool) -> None:
        """Starts or stops a VM so its status matches the requested state."""
        status = self._get_vm_status(vm_name)
        if status is None:
            return
        if running and status != "Running":
            self._run_limactl(
                [
                    "start",
                    "--tty=false",
                    vm_name,
                    "--ssh-port=0",
                    "--timeout=2m",
                    "--progress=false",
                ],
                check=True,
            )
        if not running and status == "Running":
            self._run_limactl(["stop", "--tty=false", "--force", vm_name], check=True)

    def _get_vm_status(self, vm_name: str) -> str | None:
        """Returns VM status or None when the VM does not exist."""
        result = self._run_limactl(["list", "--tty=false", "--format={{.Status}}", vm_name], check=False)
        if result.returncode != 0:
            return None
        status = result.stdout.strip()
        if status == "":
            return None
        return status

    def _get_vm_public_key(self, vm_name: str) -> str:
        """Returns the VM SSH public key and fails when missing."""
        result = self._run_limactl(
            ["shell", "--tty=false", "--workdir=/", vm_name, "--", "bash", "-lc", "cat ~/.ssh/id_rsa.pub"], check=False
        )
        self.assertEqual(0, result.returncode, msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}")
        public_key = result.stdout.strip()
        self.assertNotEqual("", public_key, msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}")
        return public_key

    def _get_vm_authorized_keys(self, vm_name: str) -> set[str]:
        """Returns non-empty authorized_keys lines for a VM."""
        result = self._run_limactl(
            ["shell", "--tty=false", "--workdir=/", vm_name, "--", "bash", "-lc", "cat ~/.ssh/authorized_keys"],
            check=False,
        )
        self.assertEqual(0, result.returncode, msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}")
        return {line.strip() for line in result.stdout.splitlines() if line.strip() != ""}

    def _assert_vm_can_ssh_host_by_name(self, source_vm: str, target_host: str) -> None:
        """Asserts source VM reaches target using plain `ssh <hostname>`."""
        result = self._run_limactl(
            [
                "shell",
                "--tty=false",
                "--workdir=/",
                source_vm,
                "--",
                "ssh",
                "-n",
                "-oBatchMode=yes",
                "-oStrictHostKeyChecking=accept-new",
                "-oConnectTimeout=5",
                target_host,
                "true",
            ],
            check=False,
        )
        self.assertEqual(0, result.returncode, msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}")

    def _run_limactl(self, args: list[str], check: bool) -> subprocess.CompletedProcess[str]:
        """Runs limactl with captured output for deterministic test assertions."""
        return subprocess.run(["limactl"] + args, check=check, text=True, capture_output=True)
