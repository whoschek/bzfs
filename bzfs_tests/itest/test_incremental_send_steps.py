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
"""Integration tests covering incremental send step selection end-to-end."""

from __future__ import (
    annotations,
)
import subprocess
from typing import (
    final,
)

from bzfs_tests.itest import (
    ibase,
)
from bzfs_tests.itest.ibase import (
    IntegrationTestCase,
    are_bookmarks_enabled,
)
from bzfs_tests.test_incremental_send_steps import (
    TestIncrementalSendSteps,
)
from bzfs_tests.zfs_util import (
    create_bookmark,
    create_filesystem,
    dataset_exists,
    destroy,
    take_snapshot,
)


#############################################################################
@final
class IncrementalSendStepsTestCase(IntegrationTestCase):

    def test_snapshot_series_excluding_hourlies(self) -> None:
        testcase = {None: ["d1", "h1", "d2", "d3", "d4"]}
        expected_results = ["d1", "d2", "d3", "d4"]
        dst_foo = ibase.DST_ROOT_DATASET + "/foo"

        src_foo = create_filesystem(ibase.SRC_ROOT_DATASET, "foo")
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        self.run_bzfs(src_foo, dst_foo, "--quiet", "--include-snapshot-regex", "d.*", "--exclude-snapshot-regex", "h.*")
        self.assert_snapshot_names(dst_foo, expected_results)

        self.tearDownAndSetup()
        src_foo = create_filesystem(ibase.SRC_ROOT_DATASET, "foo")
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[0]}"
        cmd = f"sudo -n zfs send {src_snapshot} | sudo -n zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assert_snapshot_names(dst_foo, [expected_results[0]])
        self.run_bzfs(src_foo, dst_foo, "--include-snapshot-regex", "d.*", "--exclude-snapshot-regex", "h.*")
        self.assert_snapshot_names(dst_foo, expected_results)

        self.tearDownAndSetup()
        src_foo = create_filesystem(ibase.SRC_ROOT_DATASET, "foo")
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[0]}"
        cmd = f"sudo -n zfs send {src_snapshot} | sudo -n zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assert_snapshot_names(dst_foo, [expected_results[0]])
        if are_bookmarks_enabled("src"):
            create_bookmark(src_foo, expected_results[0], expected_results[0])
            destroy(src_snapshot)
            self.run_bzfs(src_foo, dst_foo, "--include-snapshot-regex", "d.*", "--exclude-snapshot-regex", "h.*")
            self.assert_snapshot_names(dst_foo, expected_results)
            src_snapshot2 = f"{src_foo}@{expected_results[-1]}"
            destroy(src_snapshot2)  # no problem because bookmark still exists
            take_snapshot(src_foo, "d99")
            self.run_bzfs(src_foo, dst_foo, "--include-snapshot-regex", "d.*", "--exclude-snapshot-regex", "h.*")
            self.assert_snapshot_names(dst_foo, expected_results + ["d99"])

        self.tearDownAndSetup()
        src_foo = create_filesystem(ibase.SRC_ROOT_DATASET, "foo")
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[1]}"  # Note: [1]
        cmd = f"sudo -n zfs send {src_snapshot} | sudo -n zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assert_snapshot_names(dst_foo, [expected_results[1]])
        self.run_bzfs(
            src_foo,
            dst_foo,
            "--skip-missing-snapshots=continue",
            "--include-snapshot-regex",
            "d.*",
            "--exclude-snapshot-regex",
            "h.*",
        )
        self.assert_snapshot_names(dst_foo, expected_results[1:])

        self.tearDownAndSetup()
        src_foo = create_filesystem(ibase.SRC_ROOT_DATASET, "foo")
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[1]}"  # Note: [1]
        cmd = f"sudo -n zfs send {src_snapshot} | sudo -n zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assert_snapshot_names(dst_foo, [expected_results[1]])
        self.run_bzfs(
            src_foo,
            dst_foo,
            "--force",
            "--skip-missing-snapshots=continue",
            "--exclude-snapshot-regex",
            ".*",
        )
        self.assert_snapshot_names(dst_foo, [expected_results[1]])

        self.tearDownAndSetup()
        src_foo = create_filesystem(ibase.SRC_ROOT_DATASET, "foo")
        for snapshot in testcase[None]:
            take_snapshot(src_foo, snapshot)
        src_snapshot = f"{src_foo}@{expected_results[1]}"  # Note: [1]
        cmd = f"sudo -n zfs send {src_snapshot} | sudo -n zfs receive -F -u {dst_foo}"  # full zfs send
        subprocess.run(cmd, text=True, check=True, shell=True)
        self.assert_snapshot_names(dst_foo, [expected_results[1]])
        self.run_bzfs(
            src_foo,
            dst_foo,
            "--force",
            "--skip-missing-snapshots=continue",
            "--include-snapshot-regex",
            "!.*",
        )
        self.assert_snapshot_names(dst_foo, [expected_results[1]])

    def test_snapshot_series_excluding_hourlies_with_permutations(self) -> None:
        for testcase in TestIncrementalSendSteps().permute_snapshot_series(5):
            self.tearDownAndSetup()
            src_foo = create_filesystem(ibase.SRC_ROOT_DATASET, "foo")
            dst_foo = ibase.DST_ROOT_DATASET + "/foo"
            for snapshot in testcase[None]:
                take_snapshot(src_foo, snapshot)
            expected_results = testcase["d"]
            if len(expected_results) > 0 and expected_results[0].startswith("h"):
                expected_results = expected_results[1:]
            # logging.info(f"input   : {','.join(testcase[None])}")
            # logging.info(f"expected: {','.join(expected_results)}")
            self.run_bzfs(
                src_foo,
                dst_foo,
                "--skip-missing-snapshots=continue",
                "--include-snapshot-regex",
                "d.*",
                "--exclude-snapshot-regex",
                "h.*",
            )
            if len(expected_results) > 0:
                self.assert_snapshot_names(dst_foo, expected_results)
            else:
                self.assertFalse(dataset_exists(dst_foo))
