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
"""Integration tests for remote replication modes."""

from __future__ import (
    annotations,
)
import os
from typing import (
    final,
)

import bzfs_main.detect
from bzfs_main.util.utils import (
    DIE_STATUS,
)
from bzfs_tests.itest import (
    ibase,
)
from bzfs_tests.itest.ibase import (
    SSH_PROGRAM,
    IntegrationTestCase,
    is_pv_at_least_1_9_0,
)
from bzfs_tests.itest.test_local import (
    LocalTestCase,
)
from bzfs_tests.tools import (
    stop_on_failure_subtest,
)
from bzfs_tests.zfs_util import (
    dataset_exists,
)


#############################################################################
class MinimalRemoteTestCase(IntegrationTestCase):

    def test_aaa_log_diagnostics_first(self) -> None:
        LocalTestCase(param=self.param).test_aaa_log_diagnostics_first()

    def test_basic_replication_flat_simple(self) -> None:
        LocalTestCase(param=self.param).test_basic_replication_flat_simple()

    def test_basic_replication_recursive1(self) -> None:
        LocalTestCase(param=self.param).test_basic_replication_recursive1()

    def test_delete_dst_datasets_recursive_with_dummy_src(self) -> None:
        LocalTestCase(param=self.param).test_delete_dst_datasets_recursive_with_dummy_src()

    def test_zfs_set_via_recv_o(self) -> None:
        LocalTestCase(param=self.param).test_zfs_set_via_recv_o()

    def test_basic_replication_recursive_parallel(self) -> None:
        LocalTestCase(param=self.param).test_basic_replication_recursive_parallel()

    def test_inject_unavailable_sudo(self) -> None:
        expected_error = DIE_STATUS if os.getuid() != 0 and not self.is_no_privilege_elevation() else 0
        self.inject_unavailable_program("inject_unavailable_sudo", expected_error=expected_error)
        self.tearDownAndSetup()
        expected_error = 1 if os.getuid() != 0 and not self.is_no_privilege_elevation() else 0
        self.inject_unavailable_program("inject_failing_sudo", expected_error=expected_error)

    def test_disabled_sudo(self) -> None:
        expected_status = 0
        if os.getuid() != 0 and not self.is_no_privilege_elevation():
            expected_status = DIE_STATUS
        self.inject_disabled_program("sudo", expected_error=expected_status)

    def inject_disabled_program(self, prog: str, expected_error: int = 0) -> None:
        self.setup_basic()
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            f"--{prog}-program=" + bzfs_main.detect.DISABLE_PRG,
            expected_status=expected_error,
        )
        if expected_error != 0:
            self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)

    def inject_unavailable_program(self, *flags: str, expected_error: int = 0) -> None:
        self.setup_basic()
        inject_params: dict[str, bool] = {}
        for flag in flags:
            inject_params[flag] = True
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            expected_status=expected_error,
            inject_params=inject_params,
        )
        if expected_error != 0:
            self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
        else:
            self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")


#############################################################################
@final
class FullRemoteTestCase(MinimalRemoteTestCase):

    def test_ssh_program_must_not_be_disabled_in_nonlocal_mode(self) -> None:
        LocalTestCase(param=self.param).test_ssh_program_must_not_be_disabled_in_nonlocal_mode()

    def test_basic_replication_flat_nothing_todo(self) -> None:
        LocalTestCase(param=self.param).test_basic_replication_flat_nothing_todo()

    def test_basic_replication_without_source(self) -> None:
        LocalTestCase(param=self.param).test_basic_replication_without_source()

    def test_complex_replication_flat_use_bookmarks(self) -> None:
        LocalTestCase(param=self.param).test_complex_replication_flat_use_bookmarks()

    def test_basic_replication_flat_send_recv_flags(self) -> None:
        LocalTestCase(param=self.param).test_basic_replication_flat_send_recv_flags()

    def test_basic_replication_flat_simple_with_multiple_root_datasets(self) -> None:
        LocalTestCase(param=self.param).test_basic_replication_flat_simple_with_multiple_root_datasets()

    def test_basic_replication_dataset_with_spaces(self) -> None:
        LocalTestCase(param=self.param).test_basic_replication_dataset_with_spaces()

    def test_basic_replication_flat_with_multiple_root_datasets_converted_from_recursive(self) -> None:
        LocalTestCase(param=self.param).test_basic_replication_flat_with_multiple_root_datasets_converted_from_recursive()

    def test_basic_replication_flat_with_ssh_exit_on_shutdown(self) -> None:
        LocalTestCase(param=self.param).test_basic_replication_flat_with_ssh_exit_on_shutdown()

    def test_zfs_set(self) -> None:
        LocalTestCase(param=self.param).test_zfs_set()

    def test_zfs_set_via_set_include(self) -> None:
        LocalTestCase(param=self.param).test_zfs_set_via_set_include()

    def test_inject_src_pipe_fail(self) -> None:
        self.inject_pipe_error("inject_src_pipe_fail", expected_error=[1, DIE_STATUS])

    def test_inject_src_pipe_garble(self) -> None:
        if is_pv_at_least_1_9_0():
            self.skipTest("workaround for zfs send-receive pipeline hang")
        self.inject_pipe_error("inject_src_pipe_garble")

    def test_inject_dst_pipe_garble(self) -> None:
        self.inject_pipe_error("inject_dst_pipe_garble")

    def test_inject_src_send_error(self) -> None:
        self.inject_pipe_error("inject_src_send_error")

    def test_inject_dst_receive_error(self) -> None:
        self.inject_pipe_error("inject_dst_receive_error", expected_error=2)

    def inject_pipe_error(self, flag: str, expected_error: int | list[int] = 1) -> None:
        self.setup_basic()
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                inject_params: dict[str, bool] = {}
                if i == 0:
                    inject_params[flag] = True
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    expected_status=expected_error if i == 0 else 0,
                    inject_params=inject_params,
                )
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

    def test_inject_unavailable_mbuffer(self) -> None:
        self.inject_unavailable_program("inject_unavailable_mbuffer")
        if self.param and self.param.get("ssh_mode") != "local" and self.param.get("min_pipe_transfer_size", -1) == 0:
            self.tearDownAndSetup()
            self.inject_unavailable_program("inject_failing_mbuffer", expected_error=1)

    def test_inject_unavailable_ps(self) -> None:
        self.inject_unavailable_program("inject_unavailable_ps")

    def test_inject_unavailable_pv(self) -> None:
        self.inject_unavailable_program("inject_unavailable_pv")

    def test_inject_unavailable_sh(self) -> None:
        self.inject_unavailable_program("inject_unavailable_sh")
        self.tearDownAndSetup()
        self.inject_unavailable_program("inject_failing_sh")

    def test_inject_unavailable_zstd(self) -> None:
        self.inject_unavailable_program("inject_unavailable_zstd")

    def test_inject_unavailable_zpool(self) -> None:
        self.inject_unavailable_program("inject_unavailable_zpool")
        self.tearDownAndSetup()
        self.inject_unavailable_program("inject_failing_zpool")

    def test_inject_unavailable_ssh(self) -> None:
        if self.param and self.param.get("ssh_mode") != "local":
            self.inject_unavailable_program("inject_unavailable_" + SSH_PROGRAM, expected_error=DIE_STATUS)
            self.tearDownAndSetup()
            self.inject_unavailable_program("inject_failing_" + SSH_PROGRAM, expected_error=DIE_STATUS)

    def test_inject_unavailable_zfs(self) -> None:
        self.inject_unavailable_program("inject_unavailable_zfs", expected_error=DIE_STATUS)
        self.tearDownAndSetup()
        self.inject_unavailable_program("inject_failing_zfs", expected_error=DIE_STATUS)

    def test_disabled_mbuffer(self) -> None:
        self.inject_disabled_program("mbuffer")

    def test_disabled_ps(self) -> None:
        self.inject_disabled_program("ps")

    def test_disabled_pv(self) -> None:
        self.inject_disabled_program("pv")

    def test_disabled_sh(self) -> None:
        self.inject_disabled_program("shell")

    def test_disabled_compression(self) -> None:
        self.inject_disabled_program("compression")

    def test_disabled_zpool(self) -> None:
        self.inject_disabled_program("zpool")

    def test_ssh_master_check_keeps_tcp_connection_alive_with_replication_recursive(self) -> None:
        self.setup_basic()
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", ssh_control_persist_margin_secs=2**64)
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))  # b/c src has no snapshots
