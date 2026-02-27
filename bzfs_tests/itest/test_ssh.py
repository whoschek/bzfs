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
"""Integration tests for SSH behavior used by bzfs."""

from __future__ import (
    annotations,
)
import os
import platform
import pwd
import subprocess
import time
from subprocess import (
    DEVNULL,
    PIPE,
)
from typing import (
    cast,
    final,
)
from unittest.mock import (
    patch,
)

import bzfs_main.replication
from bzfs_main import (
    bzfs,
)
from bzfs_main.configuration import (
    LogParams,
)
from bzfs_main.loggers import (
    get_simple_logger,
)
from bzfs_main.util.connection import (
    SHARED,
    ConnectionPool,
    create_simple_minijob,
    create_simple_miniremote,
)
from bzfs_main.util.utils import (
    get_home_directory,
    getenv_any,
    human_readable_duration,
)
from bzfs_tests.itest import (
    ibase,
)
from bzfs_tests.itest.ibase import (
    SSH_CONFIG_FILE,
    SSH_PROGRAM,
    IntegrationTestCase,
)
from bzfs_tests.tools import (
    gc_disabled,
    stop_on_failure_subtest,
)


#############################################################################
@final
class TestSSHLatency(IntegrationTestCase):

    def run_latency_cmd(self, cmd: list[str], *, close_fds: bool = True) -> tuple[str, str]:
        process = subprocess.run(cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True, check=True, close_fds=close_fds)
        return process.stdout[0:-1], process.stderr[0:-1]  # omit trailing newline char

    def test_ssh_loopback_latency(self) -> None:
        self.setup_basic()
        args = bzfs.argument_parser().parse_args(args=["src", "dst"])
        log_params = LogParams(args)
        log = bzfs_main.loggers.get_logger(
            log_params=log_params, args=args, log=get_simple_logger("test_ssh_loopback_latency")
        )
        p = self.make_params(args=args, log_params=log_params, log=log)

        ssh_opts_list = list(p.src.ssh_extra_opts) + ["-oStrictHostKeyChecking=no"]
        ssh_opts_list += ["-S", os.path.join(p.src.ssh_exit_on_shutdown_socket_dir, "bzfs_test_ssh_socket")]
        ssh_opts_list += ["-p", cast(str, getenv_any("test_ssh_port", "22"))]

        private_key_file2 = os.path.join(get_home_directory(), ".ssh", "testid_rsa")
        src_private_key2 = ["-i", private_key_file2]
        private_key_file = os.path.join(get_home_directory(), ".ssh", "id_rsa")
        src_private_key = ["-i", private_key_file]
        if platform.system() == "Linux":
            ssh_opts_list += src_private_key
        else:
            ssh_opts_list += src_private_key2
        ssh_opts = " ".join(ssh_opts_list)
        memory_hog: str = "x" * 1_000
        # memory_hog: str = "x" * 1_000_000_000

        def run_benchmark(cmd: list[str], check_cmd: list[str]) -> None:
            _dummy = len(memory_hog)
            for close_fds in [False, True]:
                with gc_disabled(run_gc_first=True):
                    iters = 0
                    start_time_nanos = time.monotonic_ns()
                    while time.monotonic_ns() < start_time_nanos + 5 * 1000_000_000:
                        if check_cmd:
                            _stdout, stderr = self.run_latency_cmd(check_cmd, close_fds=close_fds)
                            self.assertIn("Master running", stderr)
                        _stdout, stderr = self.run_latency_cmd(cmd, close_fds=close_fds)
                        iters += 1
                    elapsed = time.monotonic_ns() - start_time_nanos
                t = human_readable_duration(elapsed / iters)
                log.info(f"time/iter: {t}, check: {bool(check_cmd)}, close_fds: {close_fds}, cmd: {' '.join(cmd)}")

        echo_cmd = "echo hello"
        list_cmd = f"{p.zfs_program} list -t snapshot -s createtxg -d 1 -Hp -o guid,name {ibase.SRC_ROOT_DATASET}"
        for base_cmd in [echo_cmd, list_cmd]:
            run_benchmark(p.split_args(base_cmd), check_cmd=[])

        for mode in range(2):
            with stop_on_failure_subtest(i=mode):
                control_persist = 2 if mode == 0 else 2  # noqa: RUF034  # seconds
                master_cmd = p.split_args(f"{SSH_PROGRAM} {ssh_opts} -M -oControlPersist={control_persist}s 127.0.0.1 exit")
                check_cmd = p.split_args(f"{SSH_PROGRAM} {ssh_opts} -O check 127.0.0.1")
                master_is_running = False
                try:
                    log.info(f"mode: {mode}")
                    master_result = self.run_latency_cmd(master_cmd)
                    log.info(f"master result: {master_result}")
                    master_is_running = True
                    if mode == 0:  # test assertions
                        start_time = time.time()
                        while time.time() - start_time < 5:
                            time.sleep(0.5)
                            stdout, stderr = self.run_latency_cmd(check_cmd)
                            log.info(f"check result: {(stdout, stderr)}")
                            self.assertIn("Master running", stderr)
                        log.info("now waiting for expiration of master ...")
                        time.sleep(control_persist + 1)
                        with self.assertRaises(subprocess.CalledProcessError) as cm:
                            self.run_latency_cmd(check_cmd)
                        e = cm.exception
                        log.info(f"check result after expiration: {(e.stdout, e.stderr)}")
                        self.assertNotIn("Master running", e.stderr)
                        self.assertIn("Control socket connect", e.stderr)
                        self.assertIn("No such file or directory", e.stderr)
                        master_is_running = False
                    elif mode >= 1:  # benchmark latency
                        for base_cmd in [echo_cmd, list_cmd]:
                            for check in [False, True]:
                                cmd = p.split_args(f"{SSH_PROGRAM} {ssh_opts} 127.0.0.1 {base_cmd}")
                                run_benchmark(cmd, check_cmd=check_cmd if check else [])
                except subprocess.CalledProcessError as e:
                    log.error(f"error: {(e.stdout, e.stderr)}")
                    raise
                finally:
                    if master_is_running:
                        master_exit_cmd = p.split_args(f"{SSH_PROGRAM} {ssh_opts} -O exit 127.0.0.1")
                        result = self.run_latency_cmd(master_exit_cmd)
                        log.info(f"exit result: {result}")


#############################################################################
@final
class TestSSHMasterIntermittentFailure(IntegrationTestCase):
    """Documents and verifies SSH master behavior under intermittent failure.

    This test exists to answer a subtle but important question for operators: what happens when ``bzfs`` is configured to
    reuse an SSH ControlMaster (``reuse_ssh_connection=True`` with ``ControlPersist``), that master dies unexpectedly, and
    replication continues to run every few seconds? In particular: do SSH commands fail for the remainder of the persistence
    window, or do they keep working, and when is a new master created?

    The expected behavior is:
    * While the master is alive, ``Connection.run_ssh_command`` calls ``refresh_ssh_connection_if_necessary()``, which
      periodically extends the master lifetime by running ``ssh -O check`` and, if needed, starting a new master via
      ``ssh -M -oControlPersist=... exit``. The timestamp ``_last_refresh_time`` records successful refreshes.
    * If the master dies after a refresh, the next ``run_ssh_command`` call detects this and recreates a new ControlMaster.
      From that point on, commands are again multiplexed over a hot TCP connection with low startup latency.
    * If said detection logic would somehow fail (no known case for this exists), then ...
        * subsequent ``run_ssh_command`` calls initially take the fast path:
          they skip the ``-O check`` and directly invoke ``ssh -S <socket> ...``.  OpenSSH then automatically falls back to
          a fresh direct TCP connection when the control socket is missing, so commands continue to succeed, albeit with
          higher latency per call.
        * Once the refresh window has elapsed (ssh_control_persist_secs=600 secs by default), the next
          ``refresh_ssh_connection_if_necessary()`` invocation leaves the fast path, detects that ``-O check`` fails, and
          recreates a new ControlMaster.  From that point on, commands are again multiplexed over a hot TCP connection with
          low startup latency.
    This test method verifies that the actual behavior is as described above even if said detection logic would fail.
    """

    def test_master_dies_then_recovers_via_refresh(self) -> None:
        username = pwd.getpwuid(os.getuid()).pw_name
        ssh_port = int(cast(str, getenv_any("test_ssh_port", "22")))
        args = bzfs.argument_parser().parse_args(args=["src", "dst"])
        log_params = LogParams(args)
        log = bzfs_main.loggers.get_logger(
            log_params=log_params, args=args, log=get_simple_logger("test_ssh_master_intermittent_failure")
        )
        remote = create_simple_miniremote(
            log=log,
            ssh_user_host=f"{username}@127.0.0.1",
            ssh_port=ssh_port,
            ssh_config_file=SSH_CONFIG_FILE,
            ssh_program=SSH_PROGRAM,
            reuse_ssh_connection=True,
            ssh_control_persist_secs=4,  # to speed up the test, reduce 600 sec refresh window down to a few secs
            ssh_control_persist_margin_secs=1,
        )
        pool = ConnectionPool(remote, SHARED)
        try:
            conn = pool.get_connection()
            job = create_simple_minijob(timeout_duration_secs=10.0)
            # Initial command: should create a master and succeed.
            proc1 = conn.run_ssh_command(["echo", "one"], job=job, stdout=PIPE, stderr=PIPE, text=True, check=True)
            self.assertEqual("one\n", proc1.stdout)

            ssh_cmd = conn.ssh_cmd
            ssh_sock_cmd = ssh_cmd[0:-1]
            ssh_user_host = remote.ssh_user_host
            check_cmd = ssh_sock_cmd + ["-O", "check", ssh_user_host]
            exit_cmd = ssh_sock_cmd + ["-O", "exit", ssh_user_host]

            # Confirm master is running.
            check_proc1 = subprocess.run(check_cmd, stdout=PIPE, stderr=PIPE, text=True)
            self.assertEqual(0, check_proc1.returncode, check_proc1.stderr)
            self.assertIn("Master running", check_proc1.stderr)

            # Kill the master to simulate an intermittent failure.
            subprocess.run(exit_cmd, stdout=PIPE, stderr=PIPE, text=True)
            check_proc2 = subprocess.run(check_cmd, stdout=PIPE, stderr=PIPE, text=True)
            self.assertNotEqual(0, check_proc2.returncode)
            self.assertIn("Control socket connect", check_proc2.stderr)

            last_refresh_before = conn._last_refresh_time
            with patch.object(conn, "_is_ssh_control_socket_usable", return_value=True):
                # Simulate a detection failure: refresh takes the fast path, ssh falls back to direct connection and succeeds.
                proc2 = conn.run_ssh_command(["echo", "two"], job=job, stdout=PIPE, stderr=PIPE, text=True, check=True)
                self.assertEqual("two\n", proc2.stdout)
                check_proc3 = subprocess.run(check_cmd, stdout=PIPE, stderr=PIPE, text=True)
                self.assertNotEqual(0, check_proc3.returncode)  # assert master is not running

                # Wait long enough so that a subsequent refresh performs -O check and recreates the master.
                time.sleep(remote.ssh_control_persist_secs - remote.ssh_control_persist_margin_secs + 1)

                proc3 = conn.run_ssh_command(["echo", "three"], job=job, stdout=PIPE, stderr=PIPE, text=True, check=True)
                self.assertEqual("three\n", proc3.stdout)
            check_proc4 = subprocess.run(check_cmd, stdout=PIPE, stderr=PIPE, text=True)
            self.assertEqual(0, check_proc4.returncode, check_proc4.stderr)
            self.assertIn("Master running", check_proc4.stderr)
            self.assertGreater(conn._last_refresh_time, last_refresh_before)

            # Replace the Unix domain socket file with a regular empty file, which will make _is_ssh_control_socket_usable()
            # return False and trigger the stale socket unlinking logic.
            socket_path = conn._ssh_socket_path
            self.assertIsNotNone(socket_path)
            socket_path = cast(str, socket_path)
            os.unlink(socket_path)
            with open(socket_path, "w", encoding="utf-8"):
                pass
            proc4 = conn.run_ssh_command(
                ["echo", "four-after-stale-socket-file"], job=job, stdout=PIPE, stderr=PIPE, text=True, check=True
            )
            self.assertEqual("four-after-stale-socket-file\n", proc4.stdout)
            check_proc5 = subprocess.run(check_cmd, stdout=PIPE, stderr=PIPE, text=True)
            self.assertEqual(0, check_proc5.returncode, check_proc5.stderr)
        finally:
            pool.shutdown()
