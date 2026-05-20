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
"""Integration tests for `lima_vm.sh` using real `limactl` without mocks or stubs.

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
import time
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

from bzfs_main import (
    argparse_cli,
)
from bzfs_main.util import (
    parallel_iterator,
    utils,
)


#############################################################################
def suite() -> unittest.TestSuite:
    """Returns the test suite for this module."""
    test_cases = [
        TestLimaVmScript,
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
class TestLimaVmScript(unittest.TestCase):

    def setUp(self) -> None:
        """Resolves repo paths and the timestamp prefix used by this test run."""
        self.repo_root: str = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        self.script_path: str = os.path.join(self.repo_root, "bzfs_testbed", "lima_vm.sh")
        self.testbed_script_path: str = os.path.join(self.repo_root, "bzfs_testbed", "lima_testbed.sh")
        self.log_time_prefix: str = time.strftime("%Y-%m-%d_%H-%M-%S")

    def test_a_zero_one_two_existing_test_vms(self) -> None:
        """Validates script behavior across 0/1/2 existing test VM scenarios."""
        test_suffix = uuid.uuid4().hex[:8]
        target_vm = f"bzfs-lima-{test_suffix}-target"
        other_vm = f"bzfs-lima-{test_suffix}-other"
        script_env = {"LIMA_VM_UPGRADE": "false"}

        with self._cleanup_vms(target_vm, other_vm):
            # Scenario 0: no existing test VMs.
            self.assertIsNone(self._get_vm_status(target_vm))
            self.assertIsNone(self._get_vm_status(other_vm))
            result_zero = self._run_script_once(vm_name=target_vm, extra_env=script_env)
            self.assertEqual(1, self._count_command_calls(result_zero.calls, "create"))
            self.assertEqual(1, self._count_command_calls(result_zero.calls, "start"))
            self.assertEqual("Running", self._get_vm_status(target_vm))
            self.assertIsNone(self._get_vm_status(other_vm))

            # Scenario 1: one existing running test VM (target).
            result_one = self._run_script_once(vm_name=target_vm, extra_env=script_env)
            self.assertEqual(0, self._count_command_calls(result_one.calls, "create"))
            self.assertEqual(0, self._count_command_calls(result_one.calls, "start"))
            self.assertEqual("Running", self._get_vm_status(target_vm))
            self.assertIsNone(self._get_vm_status(other_vm))

            # Scenario 2: two existing test VMs where target is stopped.
            self._ensure_vm_exists(other_vm, running=True)
            self._set_vm_running(target_vm, running=False)
            self.assertEqual("Stopped", self._get_vm_status(target_vm))
            self.assertEqual("Running", self._get_vm_status(other_vm))
            result_two = self._run_script_once(vm_name=target_vm, extra_env=script_env)
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

    def test_smoke_test_and_zpool_survive_reboot_across_supported_templates(self) -> None:
        """Ensures supported template/ZFS combinations pass smoke tests, survive reboot, and report expected ZFS versions."""
        env = {key: value for key, value in os.environ.items() if not key.startswith("LIMA_")}
        env["bzfs_test_mode"] = "smoke"
        env["bzfs_test_no_run_quietly"] = "true"
        test_suffix = uuid.uuid4().hex[:8]
        vm_name = f"bzfs-lima-{test_suffix}-reboot"
        log_dir = os.path.join(utils.get_home_directory(), argparse_cli.LOG_DIR_DEFAULT + "-test-vm")
        os.makedirs(log_dir, exist_ok=True)
        log_sequence = 0

        for template in [
            "template:experimental/ubuntu-26.04",
            "template:ubuntu-24.04",
            "template:almalinux-9",
            "template:almalinux-10",
            "template:rocky-9",
            "template:rocky-10",
        ]:
            if "ubuntu-26.04" in template:
                zfs_versions = ["", "zfs-2.4", "tag:zfs-2.4.2", "tag:zfs-2.3.7"]
            elif "ubuntu-24.04" in template:
                zfs_versions = ["", "zfs-2.4", "tag:zfs-2.4.2", "tag:zfs-2.3.7", "tag:zfs-2.2.9"]
            else:  # RHEL/EL family
                zfs_versions = ["zfs-2.4", "zfs-2.3", "zfs-2.2"]
            for zfs_version in zfs_versions:
                log_sequence += 1
                run_env = dict(env)
                run_env["LIMA_VM_NAME"] = vm_name
                run_env["LIMA_VM_TEMPLATE"] = template
                run_env["LIMA_ZFS_VERSION"] = zfs_version
                run_env["LIMA_START_TIMEOUT"] = "3m"
                version_name = zfs_version if zfs_version != "" else "default"
                log_path = os.path.join(
                    log_dir, f"{self.log_time_prefix}+run{log_sequence:02d}+{template.replace('/','_')}+{version_name}.log"
                )
                with self._cleanup_vms(vm_name):
                    self._run_logged_bash_script(
                        log_path,
                        run_env,
                        shlex.quote(self.script_path),
                    )
                    version_stdout: str = self.validate_zfs_version_string(zfs_version, None, vm_name)
                    self._run_logged_bash_script(
                        log_path,
                        run_env,
                        f"""
                        limactl shell --tty=false --workdir=/ {shlex.quote(vm_name)} -- bash -lc 'truncate -s 100M ~/test_pool_smoke'
                        limactl shell --tty=false --workdir=/ {shlex.quote(vm_name)} -- bash -lc 'sudo zpool create -f test-pool-smoke ~/test_pool_smoke'
                        limactl shell --tty=false --workdir=/ {shlex.quote(vm_name)} -- bash -lc 'zpool list -H test-pool-smoke | grep -q .'
                        limactl stop --tty=false {shlex.quote(vm_name)}
                        limactl start --tty=false {shlex.quote(vm_name)} --timeout=1m
                        limactl shell --tty=false --workdir=/ {shlex.quote(vm_name)} -- bash -lc 'zpool list -H test-pool-smoke | grep -q .'
                        """,
                        log_mode="a",
                    )
                    self.validate_zfs_version_string(zfs_version, version_stdout, vm_name)

    def validate_zfs_version_string(self, zfs_version: str, prev_version_stdout: str | None, vm_name: str) -> str:
        """Validates the output of `zfs --version` before and after reboot."""
        version_stdout: str = self._run_limactl(
            ["shell", "--tty=false", "--workdir=/", vm_name, "--", "zfs", "--version"], check=True
        ).stdout
        msg = f"zfs_version: {zfs_version}\nactual zfs --version stdout:\n{version_stdout}"

        # `zfs --version` output after reboot must be the same as before reboot
        if prev_version_stdout is not None:
            self.assertEqual(prev_version_stdout, version_stdout, f"{msg}\nprev_version_stdout:\n{prev_version_stdout}")

        # `zfs --version` output must be at least two lines (the first is userland and the second is the kmod kernel module)
        version_lines: list[str] = version_stdout.splitlines()
        self.assertGreaterEqual(len(version_lines), 2, msg=msg)

        # zfs userland and kernel module must contain the same version substring
        zfs_version_substring = zfs_version.removeprefix("tag:").removeprefix("zfs")
        if zfs_version_substring:
            for version_line in version_lines[:2]:
                self.assertIn(zfs_version_substring, version_line, msg=msg)

        # zfs userland and kernel module must report the same normalized version
        insignificant_tail = r"(ubuntu)[0-9]*$"  # e.g zfs-2.4.1-1ubuntu5 vs zfs-kmod-2.4.1-1ubuntu4 with LIMA_ZFS_VERSION=""
        zfs_userland_version = re.sub(insignificant_tail, r"\1", version_lines[0])
        zfs_kernel_module_version = re.sub(insignificant_tail, r"\1", version_lines[1].replace("kmod-", ""))
        self.assertEqual(zfs_userland_version, zfs_kernel_module_version, msg)

        return version_stdout

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
                    "--timeout=1m",
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

    def _run_logged_bash_script(self, log_path: str, env: dict[str, str], script: str, log_mode: str = "w") -> None:
        """Runs a strict bash script, logs combined output, and fails with the saved log on error."""
        with open(log_path, log_mode, encoding="utf-8") as log_file:
            process = subprocess.run(
                ["bash", "-e", "-lc", script],
                check=False,
                cwd=self.repo_root,
                env=env,
                text=True,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
        if process.returncode == 0:
            return
        with open(log_path, encoding="utf-8") as log_file:
            log_output = log_file.read()
        self.fail(f"log file: {log_path}\n\n{log_output}")

    def _run_limactl(self, args: list[str], check: bool) -> subprocess.CompletedProcess[str]:
        """Runs limactl with captured output for deterministic test assertions."""
        return subprocess.run(["limactl"] + args, check=check, text=True, capture_output=True)

    def test_docker_smoke(self) -> None:
        """Exercises docker helper smoke flow: up, shell, runjob, monitor, fail2ban, and down."""
        src_vm_name = "testsrc01"
        dst_vm_name = "testdst01"
        log_dir = os.path.join(utils.get_home_directory(), argparse_cli.LOG_DIR_DEFAULT + "-test-vm")
        os.makedirs(log_dir, exist_ok=True)
        log_prefix = os.path.join(log_dir, f"{self.log_time_prefix}+docker-smoke")

        self._run_logged_testbed_script(f"{log_prefix}+testbed.log", "--delete")
        self.assertIsNone(self._get_vm_status(src_vm_name))
        self.assertIsNone(self._get_vm_status(dst_vm_name))

        self._run_logged_testbed_script(f"{log_prefix}+testbed.log", "--create")
        self.assertEqual("Running", self._get_vm_status(src_vm_name))
        self.assertEqual("Running", self._get_vm_status(dst_vm_name))

        parallel_iterator.run_in_parallel(
            lambda: self._build_docker_image(src_vm_name, f"{log_prefix}+{src_vm_name}-image.log"),
            lambda: self._build_docker_image(dst_vm_name, f"{log_prefix}+{dst_vm_name}-image.log"),
        )

        self._start_docker_smoke_container(
            src_vm_name,
            f"{log_prefix}+{src_vm_name}-up.log",
            extra_env={"BZFS_DOCKER_INSTALL_HPNSSH": "true"},
        )
        self._start_docker_smoke_container(
            dst_vm_name,
            f"{log_prefix}+{dst_vm_name}-up.log",
            extra_env={"BZFS_DOCKER_INSTALL_HPNSSH": "false"},
        )
        self._assert_docker_restart_existing_stopped_container_outcome(
            dst_vm_name,
            f"{log_prefix}+{dst_vm_name}-restart",
        )

        for vm_name in [dst_vm_name, src_vm_name]:
            self._assert_docker_shell_outcome(vm_name)

        self._run_logged_vm_bash_script(
            dst_vm_name,
            f"{log_prefix}+runjob.log",
            "/bzfs/bzfs_testbed_docker/docker_run_example.sh runjob -v",
        )
        self._assert_docker_runjob_outcome(src_vm_name, dst_vm_name)

        self._assert_docker_monitor_outcome(dst_vm_name)

        self._assert_docker_cron_runjob_outcome(src_vm_name, dst_vm_name, f"{log_prefix}+cron-runjob")

        self._assert_docker_fail2ban_outcome(dst_vm_name, f"{log_prefix}+fail2ban")

        for vm_name in [dst_vm_name, src_vm_name]:
            self._run_logged_vm_bash_script(
                vm_name,
                f"{log_prefix}+{vm_name}-down.log",
                "/bzfs/bzfs_testbed_docker/docker_run_example.sh down",
            )
            self._assert_docker_container_absent(vm_name)

    def _build_docker_image(self, vm_name: str, log_path: str) -> None:
        """Builds the Docker example image on one VM before the smoke container starts."""
        self._run_logged_vm_bash_script(vm_name, log_path, "sudo /bzfs/bzfs_testbed_docker/docker_image.sh")

    def _docker_smoke_ssh_client_setup_script(self) -> str:
        """Returns bash that locates a usable SSH identity and defines `good_ssh`."""
        return r"""
            identity_file=""
            for candidate in "$HOME"/.ssh/id_ed25519 "$HOME"/.ssh/id_rsa "$HOME"/.ssh/id_ecdsa "$HOME"/.ssh/id_dsa; do
                if [[ -f "$candidate" && -f "$candidate.pub" ]]; then
                    identity_file="$candidate"
                    break
                fi
            done
            if [[ -z "$identity_file" ]]; then
                echo "ERROR: Failed to locate a usable SSH identity file" >&2
                exit 1
            fi

            ssh_base=(
                ssh
                -F /dev/null
                -oBatchMode=yes
                -oStrictHostKeyChecking=no
                -oUserKnownHostsFile=/dev/null
                -oGlobalKnownHostsFile=/dev/null
                -oPreferredAuthentications=publickey
                -oPubkeyAuthentication=yes
                -oPasswordAuthentication=no
                -oKbdInteractiveAuthentication=no
                -oIdentitiesOnly=yes
                -oControlMaster=no
                -oControlPersist=no
                -oConnectTimeout=5
                -p 2222
            )
            good_ssh=("${ssh_base[@]}" -i "$identity_file" "$USER@127.0.0.1")
        """

    def _start_docker_smoke_container(self, vm_name: str, log_path: str, extra_env: dict[str, str] | None = None) -> None:
        """Starts one Docker smoke container and waits for the SSH service it exposes."""
        docker_env: dict[str, str] = {}
        if extra_env is not None:
            docker_env.update(extra_env)
        docker_env_str = " ".join(f"{key}={shlex.quote(value)}" for key, value in docker_env.items())
        self._run_logged_vm_bash_script(
            vm_name,
            log_path,
            f"""
            docker_cli="$(command -v nerdctl || command -v docker)"
            env {docker_env_str} /bzfs/bzfs_testbed_docker/docker_run_example.sh up

            sleep 2
            if [[ "$(sudo "$docker_cli" container inspect --format='{{{{.State.Running}}}}' bzfs)" != "true" ]]; then
                echo "ERROR: docker example container is not running" >&2
                exit 1
            fi

            {self._docker_smoke_ssh_client_setup_script()}
            """,
        )

    def _assert_docker_fail2ban_outcome(self, vm_name: str, log_prefix: str) -> None:
        """Reloads the dst container with fail2ban enabled, then verifies temporary SSH bans behave correctly."""
        self._run_logged_vm_bash_script(
            vm_name,
            f"{log_prefix}+reload-down.log",
            "/bzfs/bzfs_testbed_docker/docker_run_example.sh down",
        )
        self._start_docker_smoke_container(
            vm_name,
            f"{log_prefix}+reload-up.log",
            extra_env={
                "BZFS_DOCKER_INSTALL_HPNSSH": "false",
                "BZFS_FAIL2BAN_ENABLED": "true",
                "BZFS_FAIL2BAN_BANTIME": "15s",
                "BZFS_FAIL2BAN_FINDTIME": "15s",
                "BZFS_FAIL2BAN_MAXRETRY": "3",
            },
        )
        self._run_logged_vm_bash_script(
            vm_name,
            f"{log_prefix}+check.log",
            r"""
            docker_cli="$(command -v nerdctl || command -v docker)"
            tmpdir="$(mktemp -d)"
            container_bash() {
                sudo "$docker_cli" container exec bzfs bash -lc "$1"
            }
            cleanup() {
                rm -rf "$tmpdir"
            }
            trap cleanup EXIT

            container_bash 'fail2ban-client ping'
            container_bash 'fail2ban-client status sshd'
            """
            + self._docker_smoke_ssh_client_setup_script()
            + r"""
            for _ in 1 2 3 4 5 6; do
                "${good_ssh[@]}" true
            done

            status_after_success="$(container_bash 'fail2ban-client status sshd')"
            if ! grep -Eq 'Currently banned:[[:space:]]*0' <<< "$status_after_success"; then
                echo "$status_after_success" >&2
                echo "ERROR: Valid high-frequency SSH requests unexpectedly triggered a ban" >&2
                exit 1
            fi

            ssh-keygen -q -t ed25519 -N '' -f "$tmpdir/invalid-key" >/dev/null
            bad_ssh=("${ssh_base[@]}" -i "$tmpdir/invalid-key" "$USER@127.0.0.1")
            for _ in 1 2 3; do
                if "${bad_ssh[@]}" true; then
                    echo "ERROR: Invalid SSH key unexpectedly authenticated" >&2
                    exit 1
                fi
                sleep 1
            done

            banned=0
            for _ in $(seq 1 20); do
                status_now="$(container_bash 'fail2ban-client status sshd')"
                if grep -Eq 'Currently banned:[[:space:]]*1' <<< "$status_now"; then
                    banned=1
                    break
                fi
                sleep 1
            done
            if [[ "$banned" -ne 1 ]]; then
                container_bash 'fail2ban-client status sshd' >&2 || true
                container_bash 'tail -n 200 /var/log/sshd.log /var/log/fail2ban.log' >&2 || true
                echo "ERROR: fail2ban did not ban repeated invalid SSH attempts" >&2
                exit 1
            fi

            if "${good_ssh[@]}" true; then
                echo "ERROR: Valid SSH request unexpectedly succeeded during active ban" >&2
                exit 1
            fi

            recovered=0
            for _ in $(seq 1 30); do
                sleep 1
                if "${good_ssh[@]}" true; then
                    recovered=1
                    break
                fi
            done
            if [[ "$recovered" -ne 1 ]]; then
                container_bash 'fail2ban-client status sshd' >&2 || true
                echo "ERROR: Valid SSH request did not recover after temporary ban" >&2
                exit 1
            fi

            status_after_recovery="$(container_bash 'fail2ban-client status sshd')"
            if ! grep -Eq 'Currently banned:[[:space:]]*0' <<< "$status_after_recovery"; then
                echo "$status_after_recovery" >&2
                echo "ERROR: Temporary SSH ban did not expire cleanly" >&2
                exit 1
            fi
            """,
        )

    def _assert_docker_restart_existing_stopped_container_outcome(self, vm_name: str, log_prefix: str) -> None:
        """Asserts `up` starts an existing stopped container instead of recreating it."""
        container_id_before: str = self._get_docker_container_id(vm_name)
        self.assertNotEqual("", container_id_before)
        self._run_logged_vm_bash_script(
            vm_name,
            f"{log_prefix}+stop.log",
            r"""
            docker_cli="$(command -v nerdctl || command -v docker)"
            sudo "$docker_cli" container stop bzfs
            if [[ "$(sudo "$docker_cli" container inspect --format='{{.State.Running}}' bzfs)" == "true" ]]; then
                echo "ERROR: docker example container is still running after docker stop" >&2
                exit 1
            fi
            """,
        )
        self._start_docker_smoke_container(vm_name, f"{log_prefix}+up.log")
        container_id_after = self._get_docker_container_id(vm_name)
        self.assertEqual(container_id_before, container_id_after)

    def _assert_docker_runjob_outcome(self, src_vm_name: str, dst_vm_name: str) -> None:
        """Asserts `runjob` created and replicated at least one expected snapshot and dataset payload."""
        common_suffixes: set[str] = self._get_common_vm_snapshot_suffixes(src_vm_name, dst_vm_name)
        self.assertTrue(
            any(suffix.startswith("prod_onsite_") for suffix in common_suffixes),
            msg=f"common snapshots={sorted(common_suffixes)}",
        )
        result = self._run_vm_bash_command(dst_vm_name, "sudo zfs list -H -o name,mountpoint,mounted dst/boo/bar")
        self.assertEqual(0, result.returncode, msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}")

    def _assert_docker_monitor_outcome(self, vm_name: str) -> None:
        """Asserts monitor reports fresh snapshots for the example src and dst datasets."""
        result = self._run_vm_bash_command(vm_name, "/bzfs/bzfs_testbed_docker/docker_run_example.sh monitor -v")
        self.assertEqual(0, result.returncode, msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}")
        output = result.stdout + result.stderr
        self.assertIn("Success!", output, msg=output)
        self.assertRegex(output, r"--monitor_snapshots: OK\. Latest snapshot for src/foo/bar@prod_onsite_", msg=output)
        self.assertRegex(output, r"--monitor_snapshots: OK\. Latest snapshot for dst/boo/bar@prod_onsite_", msg=output)

    def _assert_docker_cron_runjob_outcome(
        self,
        src_vm_name: str,
        dst_vm_name: str,
        log_prefix: str,
        dst_container_env: dict[str, str] | None = None,
    ) -> None:
        """Installs managed cron config, reloads the dst container, and asserts cron replicates a new snapshot."""
        before_common_suffixes: set[str] = self._get_common_vm_snapshot_suffixes(src_vm_name, dst_vm_name)
        self._run_logged_vm_bash_script(
            dst_vm_name,
            f"{log_prefix}+configure.log",
            """
            cron_dir="$HOME/bzfs/bzfs-config/cron.d"
            mkdir -p "$cron_dir"
            sed \
                -e "s/USER_NAME/$USER/g" \
                -e "s#USER_HOME#$HOME#g" \
                /bzfs/bzfs_testbed_docker/cronjob_example > "$cron_dir/cronjob_example"
            rm -rf "$HOME/bzfs/bzfs-job-logs/cron"/*
            """,
        )
        self._run_logged_vm_bash_script(
            dst_vm_name,
            f"{log_prefix}+reload-down.log",
            "/bzfs/bzfs_testbed_docker/docker_run_example.sh down",
        )
        time.sleep(2.0)  # ensure new secondly snapshot can be taken
        self._start_docker_smoke_container(
            dst_vm_name,
            f"{log_prefix}+reload-up.log",
            extra_env=dst_container_env,
        )
        self._run_logged_vm_bash_script(
            dst_vm_name,
            f"{log_prefix}+wait.log",
            """
            mkdir -p "$HOME/bzfs/bzfs-job-logs/cron"
            cron_logs=""
            for _ in $(seq 1 18); do
                cron_logs="$(find "$HOME/bzfs/bzfs-job-logs/cron" -type f -name '*.out' 2>/dev/null)"
                if [[ "$cron_logs" != "" ]]; then
                    break
                fi
                sleep 5
            done
            if [[ "$cron_logs" == "" ]]; then
                echo "ERROR: Cron replication job did not create a log file" >&2
                exit 1
            fi
            sleep 5
            """,
        )

        after_common_suffixes: set[str] = self._get_common_vm_snapshot_suffixes(src_vm_name, dst_vm_name)
        new_common_suffixes: set[str] = after_common_suffixes.difference(before_common_suffixes)
        self.assertTrue(len(after_common_suffixes) > 0)
        self.assertTrue(
            len(new_common_suffixes) > 0,
            msg=(
                f"before_common={sorted(before_common_suffixes)} "
                f"after_common={sorted(after_common_suffixes)} "
                f"new_common={sorted(new_common_suffixes)}"
            ),
        )

    def _assert_docker_shell_outcome(self, vm_name: str) -> None:
        """Asserts `shell` opens an interactive container shell with expected context and ZFS access."""
        command = r"""shell_output="$(
cat <<'EOF' | script -qec '/bzfs/bzfs_testbed_docker/docker_run_example.sh shell' /dev/null
printf '__PWD__%s\n' "$PWD"
printf '__WHOAMI__%s\n' "$(whoami)"
zfs list -H | sed 's/^/__ZFS__/'
exit
EOF
)" &&
grep -F "__PWD__${HOME}" <<< "$shell_output" &&
grep -F "__WHOAMI__${USER}" <<< "$shell_output" &&
grep -E "^__ZFS__(src|dst)" <<< "$shell_output"
"""
        result = self._run_vm_bash_command(vm_name, command)
        self.assertEqual(0, result.returncode, msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}")

    def _assert_docker_container_absent(self, vm_name: str) -> None:
        """Asserts the docker example container no longer exists on the VM."""
        result = self._run_vm_bash_command(
            vm_name,
            "sudo $(command -v nerdctl || command -v docker) container inspect bzfs >/dev/null 2>&1",
        )
        self.assertNotEqual(0, result.returncode, msg="Expected docker example container to be absent")

    def _get_docker_container_id(self, vm_name: str) -> str:
        """Returns the current docker example container id for one VM."""
        result = self._run_vm_bash_command(
            vm_name,
            "sudo $(command -v nerdctl || command -v docker) inspect --format='{{.Id}}' bzfs",
        )
        self.assertEqual(0, result.returncode, msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}")
        return result.stdout.strip()

    def _run_logged_vm_bash_script(self, vm_name: str, log_path: str, script: str) -> None:
        subprocess.run(
            ["limactl", "shell", "--tty=false", "--workdir=/bzfs", vm_name, "--", "bash", "-e", "-s"],
            check=True,
            cwd=self.repo_root,
            input=script,
            text=True,
            stderr=subprocess.STDOUT,
        )

    def _run_vm_bash_command(self, vm_name: str, command: str) -> subprocess.CompletedProcess[str]:
        """Runs one bash command inside a Lima VM and returns captured output."""
        return self._run_limactl(
            ["shell", "--tty=false", "--workdir=/bzfs", vm_name, "--", "bash", "-lc", command],
            check=False,
        )

    def _get_common_vm_snapshot_suffixes(self, src_vm_name: str, dst_vm_name: str) -> set[str]:
        """Returns snapshot suffixes shared by the example src and dst datasets."""
        src_suffixes = self._get_vm_snapshot_suffixes(src_vm_name, "src/foo/bar")
        dst_suffixes = self._get_vm_snapshot_suffixes(dst_vm_name, "dst/boo/bar")
        return src_suffixes.intersection(dst_suffixes)

    def _get_vm_snapshot_suffixes(self, vm_name: str, dataset: str) -> set[str]:
        """Returns snapshot name suffixes for one dataset on one VM."""
        result = self._run_vm_bash_command(vm_name, f"zfs list -H -o name -t snapshot {shlex.quote(dataset)}")
        self.assertEqual(0, result.returncode, msg=f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}")
        return {line.split("@", 1)[1] for line in result.stdout.splitlines() if "@" in line}

    def _run_logged_testbed_script(self, log_path: str, action: str) -> None:
        """Runs `lima_testbed.sh` for the default 1x1 testbed and preserves combined logs."""
        env = {key: value for key, value in os.environ.items() if not key.startswith("LIMA_")}
        env["LIMA_VM_TEMPLATE"] = "template:ubuntu-25.10"
        env["TESTBED_HOSTNAME_PREFIX"] = "test"
        env["TESTBED_NUM_SRC_VMS"] = "1"
        env["TESTBED_NUM_DST_VMS"] = "1"
        self._run_logged_bash_script(log_path, env, f"{shlex.quote(self.testbed_script_path)} {shlex.quote(action)}")
