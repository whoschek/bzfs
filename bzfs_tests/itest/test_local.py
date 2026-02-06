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
"""Integration tests for local replication and jobrunner workflows."""

from __future__ import (
    annotations,
)
import fcntl
import glob
import itertools
import logging
import os
import platform
import shlex
import shutil
import socket
import subprocess
import sys
import time
from collections import (
    Counter,
)
from pathlib import (
    Path,
)
from subprocess import (
    DEVNULL,
    PIPE,
)
from typing import (
    Any,
    final,
)
from unittest.mock import (
    patch,
)

import bzfs_main.replication
from bzfs_main import (
    argparse_cli,
    bzfs,
    bzfs_jobrunner,
)
from bzfs_main.configuration import (
    LogParams,
)
from bzfs_main.detect import (
    DUMMY_DATASET,
)
from bzfs_main.parallel_batch_cmd import (
    _get_max_command_line_bytes,
)
from bzfs_main.snapshot_cache import (
    MATURITY_TIME_THRESHOLD_SECS,
)
from bzfs_main.util import (
    utils,
)
from bzfs_main.util.connection import (
    dquote,
)
from bzfs_main.util.utils import (
    DIE_STATUS,
    ENV_VAR_PREFIX,
    getenv_any,
    human_readable_duration,
    replace_prefix,
    unixtime_fromisoformat,
)
from bzfs_tests.itest import (
    ibase,
)
from bzfs_tests.itest.ibase import (
    CREATION_PREFIX,
    DST_POOL_NAME,
    ENCRYPTION_ALGO,
    POOL_SIZE_BYTES_DEFAULT,
    SRC_POOL_NAME,
    SSH_CONFIG_FILE,
    SSH_PROGRAM,
    SUDO_CMD,
    IntegrationTestCase,
    are_bookmarks_enabled,
    create_filesystems,
    create_volumes,
    fix,
    is_cache_snapshots_enabled,
    is_zfs_at_least_2_2_0,
    is_zpool_feature_enabled_or_active,
    is_zpool_recv_resume_feature_enabled_or_active,
    natsorted,
    recreate_filesystem,
)
from bzfs_tests.tools import (
    stop_on_failure_subtest,
)
from bzfs_tests.zfs_util import (
    bookmarks,
    build,
    create_bookmark,
    create_filesystem,
    create_volume,
    dataset_exists,
    dataset_property,
    datasets,
    destroy,
    destroy_pool,
    destroy_snapshots,
    run_cmd,
    snapshot_name,
    snapshot_property,
    snapshots,
    take_snapshot,
    zfs_list,
    zfs_set,
)


#############################################################################
@final
class LocalTestCase(IntegrationTestCase):

    def test_aaa_log_diagnostics_first(self) -> None:
        logging.getLogger(__name__).warning(f"itest: self.param={self.param}")

    def test_basic_snapshotting_flat_simple(self) -> None:
        destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.assert_snapshots(ibase.SRC_ROOT_DATASET, 0)
        for i in range(6):
            with stop_on_failure_subtest(i=i):
                localperiods = {"yearly": 1, "adhoc": 1}
                localperiods["secondly" if i != 4 else "daily"] = 1
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    DUMMY_DATASET,
                    "--skip-replication",
                    "--create-src-snapshots",
                    "--create-src-snapshots-plan=" + str({"s1": {"onsite": localperiods}}),
                    "--create-src-snapshots-timeformat=%Y-%m-%d_%H:%M:%S.%f",
                    dry_run=(i == 0 or i == 5),
                )
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
                if i == 0:
                    self.assert_snapshots(ibase.SRC_ROOT_DATASET, 0)
                elif i == 1:
                    self.assert_snapshot_name_regexes(ibase.SRC_ROOT_DATASET, ["s1.*_adhoc", "s1.*_secondly", "s1.*_yearly"])
                    time.sleep(1.1)
                elif i == 2:
                    self.assert_snapshot_name_regexes(
                        ibase.SRC_ROOT_DATASET,
                        ["s1.*_adhoc", "s1.*_secondly", "s1.*_yearly", "s1.*_adhoc", "s1.*_secondly"],
                    )
                    time.sleep(1.1)
                    take_snapshot(ibase.SRC_ROOT_DATASET, fix("x1"))
                elif i == 3:
                    self.assert_snapshot_name_regexes(
                        ibase.SRC_ROOT_DATASET,
                        [
                            "s1.*_adhoc",
                            "s1.*_secondly",
                            "s1.*_yearly",
                            "s1.*_adhoc",
                            "s1.*_secondly",
                            "s1.*_adhoc",
                            "s1.*_secondly",
                            "x1",
                        ],
                    )
                else:
                    self.assert_snapshot_name_regexes(
                        ibase.SRC_ROOT_DATASET,
                        [
                            "s1.*_adhoc",
                            "s1.*_secondly",
                            "s1.*_yearly",
                            "s1.*_adhoc",
                            "s1.*_secondly",
                            "s1.*_adhoc",
                            "s1.*_secondly",
                            "s1.*_adhoc",
                            "s1.*_daily",
                            "x1",
                        ],
                    )

    def test_basic_snapshotting_recursive_simple(self) -> None:
        q = 0
        for m in range(2):
            for k in range(2):
                for j in range(2):
                    for i in range(2):
                        self.tearDownAndSetup()
                        self.setup_basic()
                        create_filesystem(ibase.SRC_ROOT_DATASET, "boo")
                        create_filesystem(ibase.SRC_ROOT_DATASET, "xoo")
                        n = 3
                        self.assert_snapshots(ibase.SRC_ROOT_DATASET, n, "s")
                        self.assert_snapshots(ibase.SRC_ROOT_DATASET + "/boo", 0)
                        self.assert_snapshots(ibase.SRC_ROOT_DATASET + "/foo/b", 0)
                        for b in range(3):
                            with stop_on_failure_subtest(i=q):
                                logging.getLogger(__name__).warning(
                                    f"itest: zzz, m={m}, k={k}, j={j}, i={i}, b={b}, q={q}, self.param={self.param}"
                                )
                                q += 1
                                self.run_bzfs(
                                    ibase.SRC_ROOT_DATASET,
                                    ibase.DST_ROOT_DATASET,
                                    "--recursive",
                                    "--create-src-snapshots",
                                    "--create-src-snapshots-plan=" + str({"z": {"onsite": {"yearly": 1}}}),
                                    "--create-src-snapshots-timeformat=",
                                    "--yearly_month=6",
                                    *(["--skip-replication"] if m == 0 else []),
                                    dry_run=(i == 0),
                                    cache_snapshots=(k > 0),
                                )
                                if i == 0:
                                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET)))
                                    self.assertEqual(0, len(snapshots(ibase.SRC_ROOT_DATASET + "/boo")))
                                    self.assertEqual(0, len(snapshots(ibase.SRC_ROOT_DATASET + "/xoo")))
                                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo")))
                                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/a")))
                                    self.assertEqual(0, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/b")))
                                else:
                                    lst = (
                                        [ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET]
                                        if m > 0
                                        else [ibase.SRC_ROOT_DATASET]
                                    )
                                    for dataset in lst:
                                        self.assertEqual(n + 1, len(snapshots(dataset)))
                                        self.assertEqual(1, len(snapshots(dataset + "/boo")))
                                        self.assertEqual(1, len(snapshots(dataset + "/xoo")))
                                        self.assertEqual(n + 1, len(snapshots(dataset + "/foo")))
                                        self.assertEqual(n + 1, len(snapshots(dataset + "/foo/a")))
                                        self.assertEqual(1, len(snapshots(dataset + "/foo/b")))
                                    self.assert_snapshotting_generates_identical_createtxg()

    def test_basic_snapshotting_recursive_with_skip_parent(self) -> None:
        self.setup_basic()
        destroy(ibase.DST_ROOT_DATASET, recursive=True)
        n = 3
        self.assert_snapshots(ibase.SRC_ROOT_DATASET, n, "s")
        self.assert_snapshots(ibase.SRC_ROOT_DATASET + "/foo/b", 0)
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    DUMMY_DATASET,
                    "--skip-parent",
                    "--recursive",
                    "--skip-replication",
                    "--create-src-snapshots",
                    "--create-src-snapshots-plan=" + str({"z": {"onsite": {"monthly": 1}}}),
                    "--create-src-snapshots-timeformat=",
                    dry_run=(i == 0),
                )
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
                if i == 0:
                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET)))
                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo")))
                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/a")))
                    self.assertEqual(0, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/b")))
                else:
                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET)))
                    self.assertEqual(n + 1, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo")))
                    self.assertEqual(n + 1, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/a")))
                    self.assertEqual(1, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/b")))
                    self.assert_snapshotting_generates_identical_createtxg()

    def test_basic_snapshotting_recursive_simple_with_incompatible_pruning(self) -> None:
        self.setup_basic()
        destroy(ibase.DST_ROOT_DATASET, recursive=True)
        n = 3
        self.assert_snapshots(ibase.SRC_ROOT_DATASET, n, "s")
        self.assert_snapshots(ibase.SRC_ROOT_DATASET + "/foo/b", 0)
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--exclude-dataset-regex=.*foo/a.*",
                    "--recursive",
                    "--skip-replication",
                    "--create-src-snapshots",
                    "--create-src-snapshots-plan=" + str({"z": {"onsite": {"yearly": 1}}}),
                    "--create-src-snapshots-timeformat=",
                    dry_run=(i == 0),
                )
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
                if i == 0:
                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET)))
                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo")))
                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/a")))
                    self.assertEqual(0, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/b")))
                else:
                    self.assertEqual(n + 1, len(snapshots(ibase.SRC_ROOT_DATASET)))
                    self.assertEqual(n + 1, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo")))
                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/a")))
                    self.assertEqual(1, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/b")))
                    self.assert_snapshotting_generates_identical_createtxg()

    def test_basic_snapshotting_recursive_simple_with_incompatible_pruning_with_skip_parent(self) -> None:
        self.setup_basic()
        destroy(ibase.DST_ROOT_DATASET, recursive=True)
        n = 3
        self.assert_snapshots(ibase.SRC_ROOT_DATASET, n, "s")
        self.assert_snapshots(ibase.SRC_ROOT_DATASET + "/foo/b", 0)
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--skip-parent",
                    "--exclude-dataset-regex=.*foo/a.*",
                    "--recursive",
                    "--skip-replication",
                    "--create-src-snapshots",
                    "--create-src-snapshots-plan=" + str({"z": {"onsite": {"yearly": 1}}}),
                    "--create-src-snapshots-timeformat=",
                    dry_run=(i == 0),
                )
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
                if i == 0:
                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET)))
                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo")))
                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/a")))
                    self.assertEqual(0, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/b")))
                else:
                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET)))
                    self.assertEqual(n + 1, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo")))
                    self.assertEqual(n, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/a")))
                    self.assertEqual(1, len(snapshots(ibase.SRC_ROOT_DATASET + "/foo/b")))
                    self.assert_snapshotting_generates_identical_createtxg()

    def test_big_snapshotting_generates_identical_createtxg_despite_incompatible_pruning(self) -> None:
        if self.test_mode == "functional":
            self.skipTest("Skipping slow test")
        # k = 50
        k = 1
        n = 100 * k
        self.setUp(pool_size_bytes=max(k * 50 * 1024 * 1024, POOL_SIZE_BYTES_DEFAULT))
        logging.getLogger(__name__).warning(f"Creating {n} filesystems which may take a while ...")
        sys.stdout.flush()
        create_filesystem(ibase.SRC_ROOT_DATASET, "xxx")
        start_time_nanos = time.time_ns()
        for i in range(n):
            create_filesystem(ibase.SRC_ROOT_DATASET, f"foo{i}")
        logging.getLogger(__name__).warning(
            f"Creating {n} filesystems took {human_readable_duration(time.time_ns() - start_time_nanos)}"
        )
        start_time_nanos = time.time_ns()
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--exclude-dataset-regex=.*xxx",  # induce incompatible pruning
            "--recursive",
            "--skip-replication",
            "--create-src-snapshots",
            "--create-src-snapshots-plan=" + str({"z": {"onsite": {"foo": 1}}}),
            "--create-src-snapshots-timeformat=",
        )
        logging.getLogger(__name__).warning(
            f"Snapshotting took {human_readable_duration(time.time_ns() - start_time_nanos)}"
        )
        self.assert_snapshotting_generates_identical_createtxg()

    def assert_snapshotting_generates_identical_createtxg(self) -> None:
        createtxgs = set()
        creations = set()
        for line in zfs_list([ibase.SRC_ROOT_DATASET], props=["name,createtxg,creation"], types=["snapshot"]):
            name, createtxg, creation = line.split("\t", 2)
            if "@z" in name:
                createtxgs.add(createtxg)
                creations.add(creation)

        # the following assertion nomore holds if all datasets nomore fit onto a single command line (many thousands of
        # non-recursive snapshots)
        self.assertEqual(1, len(createtxgs))

        # the following assertion nomore holds if snapshotting takes too long even if 'zfs snapshot -r' is used with a
        # single root dataset.
        # e.g. k=50 in test_big_snapshotting_generates_identical_createtxg_despite_incompatible_pruning() on Linux
        # even if "--exclude-dataset-regex=.*xxx" is commented out therein.
        # self.assertEqual(1, len(creations))

    def test_basic_snapshotting_flat_empty(self) -> None:
        destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.assert_snapshots(ibase.SRC_ROOT_DATASET, 0)
        for i in range(1):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--skip-replication",
                    "--exclude-dataset-regex=.*",
                    "--create-src-snapshots",
                    "--create-src-snapshots-plan=" + str({"s1": {"onsite": {"secondly": 1}}}),
                )
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
                self.assert_snapshots(ibase.SRC_ROOT_DATASET, 0)

    def test_basic_snapshotting_flat_non_existing_root(self) -> None:
        destroy(ibase.SRC_ROOT_DATASET, recursive=True)
        destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--create-src-snapshots",
            "--create-src-snapshots-plan=" + str({"s1": {"onsite": {"secondly": 1}}}),
            expected_status=DIE_STATUS,
        )

    def test_basic_snapshotting_flat_even_if_not_due(self) -> None:
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_9999-01-01_00:00:00_hourly")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_9999-01-01_00:00:00_daily")
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--create-src-snapshots-even-if-not-due",
            "--create-src-snapshots",
            "--create-src-snapshots-plan=" + str({"s1": {"onsite": {"hourly": 1}}}),
        )
        self.assert_snapshot_name_regexes(ibase.SRC_ROOT_DATASET, ["s1.*_hourly", "s1.*_daily", "s1.*_hourly"])

    def test_basic_snapshotting_flat_daemon(self) -> None:
        if self.param and self.param.get("ssh_mode", "local") not in ["local"]:
            self.skipTest("Test is only working in local mode because of timing issues")
        destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--create-src-snapshots",
            "--create-src-snapshots-plan=" + str({"s1": {"onsite": {"secondly": 1}}}),
            "--create-src-snapshots-timeformat=%Y-%m-%d_%H:%M:%S.%f",
            "--daemon-lifetime=2seconds",
        )
        expected = ["s1.*_secondly", "s1.*_secondly", "s1.*_secondly"]
        try:
            self.assert_snapshot_name_regexes(ibase.SRC_ROOT_DATASET, expected)
            self.assert_snapshot_name_regexes(ibase.DST_ROOT_DATASET, expected)
        except AssertionError:  # harmless
            expected = ["s1.*_secondly", "s1.*_secondly"]
            self.assert_snapshot_name_regexes(ibase.SRC_ROOT_DATASET, expected)
            self.assert_snapshot_name_regexes(ibase.DST_ROOT_DATASET, expected)

    def test_snapshots_changed_does_not_update_on_bookmark_creation(self) -> None:
        """Verify ZFS property 'snapshots_changed' does not update when a bookmark is created."""
        if not is_zfs_at_least_2_2_0():
            self.skipTest("snapshots_changed requires ZFS >= 2.2")
        if not are_bookmarks_enabled("src"):
            self.skipTest("Bookmarks not enabled on source pool")

        ds = create_filesystem(ibase.SRC_ROOT_DATASET, fix("foo"))
        snap_tag = fix("s1")
        take_snapshot(ds, snap_tag)

        # First read: 'snapshots_changed' after snapshot creation
        v1 = dataset_property(ds, "snapshots_changed")
        self.assertTrue(v1 and v1 != "-")

        # Ensure we land in a different integer second
        time.sleep(2.1)

        # Change snapshots/bookmarks state: create a bookmark for the snapshot
        bm_tag = fix("b1")
        create_bookmark(ds, snap_tag, bm_tag)

        # Second read should not differ
        v2 = dataset_property(ds, "snapshots_changed")
        self.assertTrue(v2 and v2 != "-")
        self.assertEqual(v1, v2)

    def test_daemon_frequency(self) -> None:
        self.setup_basic()
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--daemon-frequency=1secondly")

    def test_daemon_frequency_invalid(self) -> None:
        self.setup_basic()
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--daemon-frequency=1second", expected_status=DIE_STATUS
        )

    def test_invalid_use_of_dummy(self) -> None:
        self.run_bzfs(ibase.SRC_ROOT_DATASET, DUMMY_DATASET, expected_status=DIE_STATUS)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            DUMMY_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            expected_status=DIE_STATUS,
        )
        self.run_bzfs(
            DUMMY_DATASET,
            DUMMY_DATASET,
            "--skip-replication",
            "--create-src-snapshots",
            "--create-src-snapshots-plan=" + str({"s1": {"onsite": {"secondly": 1}}}),
            expected_status=DIE_STATUS,
        )
        self.run_bzfs(DUMMY_DATASET, DUMMY_DATASET, expected_status=DIE_STATUS)

    def test_include_snapshots_plan(self) -> None:
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:00:00_secondly")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:00:01_secondly")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:00:00_daily")
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--include-snapshot-plan", str({"s1": {"onsite": {}}}))
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, [])

        time.sleep(1.1)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--include-snapshot-plan",
            str({"s1": {"onsite": {"secondly": 1, "hourly": 1, "minutely": 0}}}),
        )
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s1_onsite_2024-01-01_00:00:01_secondly"])

    def test_delete_dst_snapshots_except_plan(self) -> None:
        if self.is_no_privilege_elevation():
            self.skipTest("Destroying snapshots on src needs extra permissions")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:00:00_secondly")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:01:00_secondly")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:02:00_secondly")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:00:00_daily")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-02_00:00:00_daily")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-03_00:00:00_daily")
        self.assert_snapshot_name_regexes(
            ibase.SRC_ROOT_DATASET,
            [
                "s1.*_daily",
                "s1.*_secondly",
                "s1.*_secondly",
                "s1.*_secondly",
                "s1.*_daily",
                "s1.*_daily",
            ],
        )
        time.sleep(1.1)
        self.run_bzfs(
            DUMMY_DATASET,
            ibase.SRC_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-except-plan",
            str({"s1": {"onsite": {"secondly": 1, "hourly": 1, "minutely": 0}}}),
        )
        self.assert_snapshot_names(ibase.SRC_ROOT_DATASET, ["s1_onsite_2024-01-01_00:02:00_secondly"])

        # multiple --delete-dst-snapshots-except-plan expressions are UNIONized:
        take_snapshot(ibase.SRC_ROOT_DATASET, "s2_onsite_2024-01-01_00:02:00_secondly")
        self.run_bzfs(
            DUMMY_DATASET,
            ibase.SRC_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-except-plan",
            str({"s1": {"onsite": {"secondly": 1}}}),
            "--delete-dst-snapshots-except-plan",
            str({"s2": {"onsite": {"secondly": 1}}}),
        )
        self.assert_snapshot_names(
            ibase.SRC_ROOT_DATASET, ["s1_onsite_2024-01-01_00:02:00_secondly", "s2_onsite_2024-01-01_00:02:00_secondly"]
        )

        self.run_bzfs(
            DUMMY_DATASET,
            ibase.SRC_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-except-plan",
            str({"s1": {"onsite": {"secondly": 0, "hourly": 1}}}),
        )
        self.assert_snapshot_names(ibase.SRC_ROOT_DATASET, [])

        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-04_00:00:00_adhoc")
        self.run_bzfs(
            DUMMY_DATASET,
            ibase.SRC_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-except-plan",
            str({"s1": {"onsite": {"secondly": 1, "adhoc": 1}}}),  # adhoc is retained despite no time period
        )
        self.assert_snapshot_names(ibase.SRC_ROOT_DATASET, ["s1_onsite_2024-01-04_00:00:00_adhoc"])

        self.run_bzfs(
            DUMMY_DATASET,
            ibase.SRC_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-except-plan",
            str({"s1": {"onsite": {"adhoc": 0}}}),  # zero amount
        )
        self.assert_snapshot_names(ibase.SRC_ROOT_DATASET, [])

        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-04_00:00:00_adhoc")
        self.run_bzfs(
            DUMMY_DATASET,
            ibase.SRC_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-except-plan",
            str({}),  # error out on empty dict as deleting all snapshots is likely a pilot error rather than intended
            expected_status=2,
        )
        self.assert_snapshot_names(ibase.SRC_ROOT_DATASET, ["s1_onsite_2024-01-04_00:00:00_adhoc"])

        self.run_bzfs(
            DUMMY_DATASET,
            ibase.SRC_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-except-plan",
            str({"s1": {"onsite": {"secondly": -1, "hourly": 1}}}),  # parser error: negative int isn't accepted
            expected_status=2,
        )

    def test_delete_dst_snapshots_except_with_source(self) -> None:
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("s1_hourly"))
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("s2_daily"))
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("s3_weekly"))
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)

        # Add extra snapshots only to destination
        take_snapshot(ibase.DST_ROOT_DATASET, fix("d_extra_hourly"))
        take_snapshot(ibase.DST_ROOT_DATASET, fix("d_extra_daily"))
        self.assert_snapshot_names(
            ibase.DST_ROOT_DATASET, ["d_extra_daily", "d_extra_hourly", "s1_hourly", "s2_daily", "s3_weekly"]
        )

        # Policy: Retain dailies. Mode: delete-except. Check with source.
        # Means: Keep snapshots on DST if (they are daily AND on SRC). Delete all others.
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-except",
            "--include-snapshot-regex=.*_daily",  # Policy: retain dailies
        )
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s2_daily"])

    def test_delete_dst_snapshots_except_with_dummy_source0(self) -> None:
        take_snapshot(ibase.DST_ROOT_DATASET, fix("s1_hourly"))
        take_snapshot(ibase.DST_ROOT_DATASET, fix("s2_daily"))
        take_snapshot(ibase.DST_ROOT_DATASET, fix("s3_weekly"))

        # Policy: Retain dailies. Mode: delete-except. Source is dummy.
        # Means: Keep snapshots on DST if (they are daily). Delete all others.
        self.run_bzfs(
            DUMMY_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-except",
            "--include-snapshot-regex=.*_daily",  # Policy: retain dailies
        )
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s2_daily"])

    def test_delete_dst_snapshots_except_with_dummy_source1(self) -> None:
        if self.is_no_privilege_elevation():
            self.skipTest("Destroying snapshots on src needs extra permissions")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:00:00_secondly")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:01:00_secondly")
        time.sleep(1.1)
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:02:00_secondly")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:00:00_millisecondly")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-02_00:00:00_millisecondly")
        time.sleep(1.1)
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-03_00:00:00_millisecondly")
        self.assert_snapshot_name_regexes(
            ibase.SRC_ROOT_DATASET,
            [
                "s1.*_millisecondly",
                "s1.*_secondly",
                "s1.*_secondly",
                "s1.*_secondly",
                "s1.*_millisecondly",
                "s1.*_millisecondly",
            ],
        )
        self.run_bzfs(
            DUMMY_DATASET,
            ibase.SRC_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-except-plan",
            str({"s1": {"onsite": {"secondly": 1, "millisecondly": 1}}}),
        )
        self.assert_snapshot_names(
            ibase.SRC_ROOT_DATASET,
            ["s1_onsite_2024-01-01_00:02:00_secondly", "s1_onsite_2024-01-03_00:00:00_millisecondly"],
        )

    def test_delete_dst_snapshots_except_with_dummy_source2(self) -> None:
        if self.is_no_privilege_elevation():
            self.skipTest("Destroying snapshots on src needs extra permissions")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:00:00_secondly")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:01:00_secondly")
        time.sleep(1.1)
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:02:00_secondly")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-01_00:00:00_millisecondly")
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-02_00:00:00_millisecondly")
        time.sleep(1.1)
        take_snapshot(ibase.SRC_ROOT_DATASET, "s1_onsite_2024-01-03_00:00:00_millisecondly")
        self.assert_snapshot_name_regexes(
            ibase.SRC_ROOT_DATASET,
            [
                "s1.*_millisecondly",
                "s1.*_secondly",
                "s1.*_secondly",
                "s1.*_secondly",
                "s1.*_millisecondly",
                "s1.*_millisecondly",
            ],
        )
        self.run_bzfs(
            DUMMY_DATASET,
            ibase.SRC_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--include-snapshot-regex=.*_(secondly|millisecondly)",  # i.e. retain all snapshots, is unioned with plan below
            "--delete-dst-snapshots-except-plan",
            str({"s1": {"onsite": {"secondly": 1, "millisecondly": 1}}}),
        )
        self.assert_snapshot_name_regexes(
            ibase.SRC_ROOT_DATASET,
            [
                "s1.*_millisecondly",
                "s1.*_secondly",
                "s1.*_secondly",
                "s1.*_secondly",
                "s1.*_millisecondly",
                "s1.*_millisecondly",
            ],
        )

    def test_basic_replication_flat_simple(self) -> None:
        self.setup_basic()
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                if i <= 1:
                    job = self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, dry_run=(i == 0))
                else:
                    job = self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--quiet", dry_run=(i == 0))
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
                for loc in ["local", "src", "dst"]:
                    if loc != "local":
                        self.assertTrue(job.params.is_program_available("zfs", loc))
                    self.assertTrue(job.params.is_program_available("zpool", loc))
                    self.assertTrue(job.params.is_program_available("ssh", loc))
                    self.assertTrue(job.params.is_program_available("sh", loc))
                    self.assertTrue(job.params.is_program_available("sudo", loc))
                    self.assertTrue(job.params.is_program_available("zstd", loc))
                    self.assertTrue(job.params.is_program_available("mbuffer", loc))
                    self.assertTrue(job.params.is_program_available("pv", loc))
                    self.assertTrue(job.params.is_program_available("ps", loc))

    def test_basic_replication_flat_simple_with_progress_reporter(self) -> None:
        for pv_program in ["pv", bzfs_main.detect.DISABLE_PRG]:
            for i in range(3):
                with stop_on_failure_subtest(i=i):
                    self.tearDownAndSetup()
                    n = 10
                    self.create_resumable_snapshots(1, n + 1)
                    self.run_bzfs(
                        ibase.SRC_ROOT_DATASET,
                        ibase.DST_ROOT_DATASET,
                        f"--pv-program={pv_program}",
                        isatty=i > 0,
                        use_select=(i % 2 == 1),
                        progress_update_intervals=(0.01, 0.015),
                    )
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, n, "s")

    def test_progress_reporter_validation(self) -> None:
        self.setup_basic()

        # --pv-program-opts must contain one of --bytes or --bits for progress metrics to function
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--pv-program-opts=", expected_status=DIE_STATUS)
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)

        # --pv-program-opts must contain --eta for progress report line to function
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--pv-program-opts=--bytes",
            isatty=True,
            expected_status=DIE_STATUS,
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)

        # --pv-program-opts must contain --eta --fineta --average-rate for progress report line to function
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--pv-program-opts=--bytes --eta",
            isatty=True,
            expected_status=DIE_STATUS,
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)

        # --pv-program-opts must contain --eta --fineta --average-rate for progress report line to function
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--pv-program-opts=--bytes --eta --average-rate",
            isatty=True,
            expected_status=DIE_STATUS,
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)

        # normal config passes
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--pv-program-opts=--bytes --eta --fineta --average-rate",
            isatty=True,
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

    def test_basic_replication_recursive1_with_volume(self) -> None:
        self.test_basic_replication_recursive1(volume=True)

    def test_basic_replication_recursive1(self, volume: bool = False) -> None:
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.setup_basic(volume=volume)
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", dry_run=(i == 0))
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))  # b/c src has no snapshots

                    compression_prop = dataset_property(ibase.DST_ROOT_DATASET + "/foo", "compression")
                    self.assertEqual("on", compression_prop)
                    encryption_prop = dataset_property(ibase.DST_ROOT_DATASET, "encryption")
                    self.assertEqual("off", encryption_prop)
                    encryption_prop = dataset_property(ibase.DST_ROOT_DATASET + "/foo", "encryption")
                    self.assertEqual(ENCRYPTION_ALGO if self.is_encryption_mode() else "off", encryption_prop)
                    encryption_prop = dataset_property(ibase.DST_ROOT_DATASET + "/foo/a", "encryption")
                    self.assertEqual(ENCRYPTION_ALGO if self.is_encryption_mode() else "off", encryption_prop)

    def test_basic_replication_recursive_parallel(self) -> None:
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.setup_basic()
        w, q = 3, 3
        threads = 4
        maxsessions = threads // 2
        self.setup_basic_woo(w=w, q=q)
        for i in range(1):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--recursive",
                    f"--threads={threads}",
                    f"--max-concurrent-ssh-sessions-per-tcp-connection={maxsessions}",
                    "--mbuffer-program-opts=-q -m 16M",
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))  # b/c src has no snapshots
                for j in range(w):
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + f"/woo{j}", 3, "w")
                    for k in range(q):
                        self.assert_snapshots(ibase.DST_ROOT_DATASET + f"/woo{j}/qoo{k}", 3, "q")
                if i == 0 and are_bookmarks_enabled("src") and not self.is_no_privilege_elevation():
                    self.run_bzfs(
                        DUMMY_DATASET,
                        ibase.SRC_ROOT_DATASET,
                        "--skip-replication",
                        "--recursive",
                        "--delete-dst-snapshots=bookmarks",
                    )
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, [])
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", [])
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo/a", [])

    def test_basic_replication_flat_pool(self) -> None:
        for child in datasets(ibase.SRC_POOL) + datasets(ibase.DST_POOL):
            destroy(child, recursive=True)
        for snapshot in snapshots(ibase.SRC_POOL) + snapshots(ibase.DST_POOL):
            destroy(snapshot)
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_POOL, ibase.DST_POOL, dry_run=(i == 0))
                self.assert_snapshots(ibase.DST_POOL, 0, "p")  # nothing has changed

        take_snapshot(ibase.SRC_POOL, fix("p1"))
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                if i > 0 and self.is_no_privilege_elevation():  # maybe related: https://github.com/openzfs/zfs/issues/10461
                    self.skipTest("'cannot unmount '/wb_dest': permission denied' error on zfs receive -F -u wb_dest")
                self.run_bzfs(ibase.SRC_POOL, ibase.DST_POOL, dry_run=(i == 0))
                if i == 0:
                    self.assert_snapshots(ibase.DST_POOL, 0, "p")  # nothing has changed
                else:
                    self.assert_snapshots(ibase.DST_POOL, 1, "p")

        for snapshot in snapshots(ibase.DST_POOL):
            destroy(snapshot)
        take_snapshot(ibase.DST_POOL, fix("q1"))
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_POOL, ibase.DST_POOL, "--force", "--force-once", dry_run=(i == 0))
                if i == 0:
                    self.assert_snapshots(ibase.DST_POOL, 1, "q")
                else:
                    self.assert_snapshots(ibase.DST_POOL, 1, "p")

    def test_basic_replication_missing_pools(self) -> None:
        for child in datasets(ibase.SRC_POOL) + datasets(ibase.DST_POOL):
            destroy(child, recursive=True)
        for snapshot in snapshots(ibase.SRC_POOL) + snapshots(ibase.DST_POOL):
            destroy(snapshot)
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_POOL, ibase.DST_POOL, dry_run=(i == 0))
                self.assert_snapshots(ibase.DST_POOL, 0)  # nothing has changed

        destroy_pool(ibase.DST_POOL)
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_POOL, ibase.DST_POOL, dry_run=(i == 0), expected_status=DIE_STATUS)
                self.assertFalse(dataset_exists(ibase.DST_POOL))

        destroy_pool(ibase.SRC_POOL)
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_POOL, ibase.DST_POOL, dry_run=(i == 0), expected_status=DIE_STATUS)
                self.assertFalse(dataset_exists(ibase.SRC_POOL))

    def test_basic_replication_flat_nonzero_snapshots_create_parents(self) -> None:
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))
        self.setup_basic()
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET + "/foo/a", ibase.DST_ROOT_DATASET + "/foo/a", dry_run=(i == 0))
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))
                if i == 0:
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")

    def test_basic_replication_flat_no_snapshot_dont_create_parents(self) -> None:
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))
        self.setup_basic()
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET + "/foo/b", ibase.DST_ROOT_DATASET + "/foo/b", dry_run=(i == 0))
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))

    def test_basic_replication_flat_simple_with_dry_run_no_send(self) -> None:
        self.setup_basic()
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                dry_run_no_send = ["--dryrun=send"] if i == 0 else []
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, *dry_run_no_send)
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

    def test_basic_replication_flat_with_ssh_exit_on_shutdown(self) -> None:
        self.setup_basic()
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--ssh-exit-on-shutdown")
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

    def test_basic_replication_flat_nothing_todo(self) -> None:
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, dry_run=(i == 0))
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))

    def test_basic_replication_without_source(self) -> None:
        destroy(ibase.SRC_ROOT_DATASET, recursive=True)
        recreate_filesystem(ibase.DST_ROOT_DATASET)
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, dry_run=(i == 0), expected_status=DIE_STATUS)
                self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)

    def test_basic_replication_flat_simple_with_skip_parent(self) -> None:
        self.setup_basic()
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--skip-parent", dry_run=(i == 0))
                self.assert_snapshots(ibase.SRC_ROOT_DATASET, 3, "s")
                self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)

        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--skip-parent",
                    "--delete-dst-datasets",
                    dry_run=(i == 0),
                )
                self.assert_snapshots(ibase.SRC_ROOT_DATASET, 3, "s")
                self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)

    def test_basic_replication_recursive_with_skip_parent(self) -> None:
        self.setup_basic()
        self.setup_basic_woo()
        destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--skip-parent", "--recursive", dry_run=(i == 0)
                )
                self.assert_snapshots(ibase.SRC_ROOT_DATASET, 3, "s")
                if i == 0:
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/woo0", 3, "w")

        destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--skip-parent",
                    "--delete-dst-datasets",
                    "--recursive",
                    "--log-file-prefix=test_",
                    "--log-file-suffix=-daily",
                    dry_run=(i == 0),
                )
                self.assert_snapshots(ibase.SRC_ROOT_DATASET, 3, "s")
                if i == 0:
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/woo0", 3, "w")

    def test_last_replicated_cache_with_no_sleep(self) -> None:
        destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--create-src-snapshots",
            "--create-src-snapshots-plan=" + str({"z": {"onsite": {"secondly": 1}}}),
            "--create-src-snapshots-timeformat=%Y-%m-%d_%H:%M:%S.%f",
            cache_snapshots=True,
        )
        self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, cache_snapshots=True)
        self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

    def test_last_replicated_cache_with_sleep_longer_than_threshold(self) -> None:
        destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--create-src-snapshots",
            "--create-src-snapshots-plan=" + str({"z": {"onsite": {"secondly": 1}}}),
            "--create-src-snapshots-timeformat=%Y-%m-%d_%H:%M:%S.%f",
            cache_snapshots=True,
        )
        self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))
        time.sleep(2.2)
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, cache_snapshots=True)
        self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

    def test_include_snapshot_time_range_full(self) -> None:
        self.setup_basic()
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--include-snapshot-times-and-ranks=60secs ago..2999-01-01",
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

    def test_include_snapshot_time_range_empty(self) -> None:
        self.setup_basic()
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--include-snapshot-times-and-ranks=2999-01-01..2999-01-01",
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)

    def test_include_snapshot_time_range_full_with_existing_nonempty_dst(self) -> None:
        self.setup_basic()
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
        destroy(snapshots(ibase.DST_ROOT_DATASET)[-1])
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--include-snapshot-times-and-ranks=60secs ago..2999-01-01",
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

    def test_include_snapshot_time_range_full_with_existing_nonempty_dst_no_use_bookmark(self) -> None:
        self.setup_basic()
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
        destroy(snapshots(ibase.DST_ROOT_DATASET)[-1])
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--no-use-bookmark",
            "--include-snapshot-times-and-ranks=60secs ago..2999-01-01",
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

    def test_include_snapshot_rank_range_full(self) -> None:
        self.setup_basic()
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--include-snapshot-times-and-ranks",
            "0..0",
            "latest0%..latest100%",
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

    def test_include_snapshot_rank_range_empty(self) -> None:
        self.setup_basic()
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--include-snapshot-times-and-ranks",
            "0..0",
            "latest0%..latest0%",
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)

    def run_snapshot_filters(self, filter1: list[str], filter2: list[str], filter3: list[str]) -> None:
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            *filter1,
            *filter2,
            *filter3,
            creation_prefix=CREATION_PREFIX,
        )

    def test_snapshot_filter_order_matters(self) -> None:
        # "creation" zfs property cannot be manually set or modified or easily mocked.
        # Thus, for checks during testing we use the "bzfs_test:creation" property instead of the "creation" property.
        for snap in ["2024-01-01d", "2024-01-02d", "2024-01-03d", "2024-01-04d", "2024-01-05h"]:
            unix_time = unixtime_fromisoformat(snap[0 : len("2024-01-01")])
            take_snapshot(ibase.SRC_ROOT_DATASET, fix(snap), props=["-o", CREATION_PREFIX + f"creation={unix_time}"])

        regex_filter = ["--include-snapshot-regex=.*d.*"]  # include dailies only
        times_filter = ["--include-snapshot-times-and-ranks=2024-01-03..*"]
        ranks_filter = ["--include-snapshot-times-and-ranks", "0..0", "oldest 1"]

        if dataset_exists(ibase.DST_ROOT_DATASET):
            destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_snapshot_filters(regex_filter, times_filter, ranks_filter)
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["2024-01-03d"])

        if dataset_exists(ibase.DST_ROOT_DATASET):
            destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_snapshot_filters(regex_filter, ranks_filter, times_filter)
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

        if dataset_exists(ibase.DST_ROOT_DATASET):
            destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_snapshot_filters(ranks_filter, regex_filter, times_filter)
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

        if dataset_exists(ibase.DST_ROOT_DATASET):
            destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_snapshot_filters(times_filter, regex_filter, ranks_filter)
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["2024-01-03d"])

        if dataset_exists(ibase.DST_ROOT_DATASET):
            destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_snapshot_filters(times_filter, ranks_filter, regex_filter)
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["2024-01-03d"])

    def test_snapshot_filter_regexes_dont_merge_across_groups(self) -> None:
        for snap in ["2024-01-01d", "2024-01-02d", "2024-01-03h", "2024-01-04dt"]:
            unix_time = unixtime_fromisoformat(snap[0 : len("2024-01-01")])
            take_snapshot(ibase.SRC_ROOT_DATASET, fix(snap), props=["-o", CREATION_PREFIX + f"creation={unix_time}"])

        regex_filter_daily = ["--include-snapshot-regex=.*d.*"]  # include dailies only
        regex_filter_test = ["--include-snapshot-regex=.*t.*"]
        ranks_filter = ["--include-snapshot-times-and-ranks", "0..0", "oldest 1"]

        if dataset_exists(ibase.DST_ROOT_DATASET):
            destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_snapshot_filters(regex_filter_daily, ranks_filter, regex_filter_test)
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

        if dataset_exists(ibase.DST_ROOT_DATASET):
            destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_snapshot_filters(regex_filter_daily, regex_filter_test, ranks_filter)
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["2024-01-01d"])

    def test_snapshot_filter_ranks_dont_merge_across_groups(self) -> None:
        for snap in ["2024-01-01d", "2024-01-02h", "2024-01-03d", "2024-01-04dt"]:
            unix_time = unixtime_fromisoformat(snap[0 : len("2024-01-01")])
            take_snapshot(ibase.SRC_ROOT_DATASET, fix(snap), props=["-o", CREATION_PREFIX + f"creation={unix_time}"])

        regex_filter_daily = ["--include-snapshot-regex=.*d.*"]  # include dailies only
        ranks_filter1 = ["--include-snapshot-times-and-ranks", "0..0", "oldest 1..oldest100%"]
        ranks_filter2 = ["--include-snapshot-times-and-ranks", "0..0", "oldest 1"]

        if dataset_exists(ibase.DST_ROOT_DATASET):
            destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_snapshot_filters(ranks_filter1, ranks_filter2, regex_filter_daily)
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

        if dataset_exists(ibase.DST_ROOT_DATASET):
            destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_snapshot_filters(ranks_filter1, regex_filter_daily, ranks_filter2)
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["2024-01-03d"])

    def test_snapshot_filter_groups(self) -> None:
        for snap in ["2024-01-01d", "2024-01-02h", "2024-01-03d", "2024-01-04dt"]:
            unix_time = unixtime_fromisoformat(snap[0 : len("2024-01-01")])
            take_snapshot(ibase.SRC_ROOT_DATASET, fix(snap), props=["-o", CREATION_PREFIX + f"creation={unix_time}"])

        snapshot_filter = [
            "--include-snapshot-regex=.*01d",
            "--include-snapshot-times-and-ranks=2024-01-01..2024-01-02",
            "--new-snapshot-filter-group",
            "--include-snapshot-regex=.*02h",
            "--include-snapshot-times-and-ranks=2024-01-02..2024-01-03",
            "--new-snapshot-filter-group",
            "--include-snapshot-regex=.*04dt",
            "--include-snapshot-times-and-ranks=2024-01-04..2024-01-05",
        ]
        if dataset_exists(ibase.DST_ROOT_DATASET):
            destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, *snapshot_filter, creation_prefix=CREATION_PREFIX)
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["2024-01-01d", "2024-01-02h", "2024-01-04dt"])

    def test_nostream(self) -> None:
        self.setup_basic()
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--no-stream", dry_run=(i == 0))
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                else:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s3"])

        take_snapshot(ibase.SRC_ROOT_DATASET, fix("s4"))
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--no-stream", dry_run=(i == 0))
                if i == 0:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s3"])
                else:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s3", "s4"])

        take_snapshot(ibase.SRC_ROOT_DATASET, fix("s5"))
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("s6"))
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--no-stream", dry_run=(i == 0))
                if i == 0:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s3", "s4"])
                else:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s3", "s4", "s6"])

    def test_basic_replication_with_no_datasets_1(self) -> None:
        self.setup_basic()
        self.run_bzfs(expected_status=2)

    @patch("sys.argv", ["bzfs.py"])
    def test_basic_replication_with_no_datasets_2(self) -> None:
        with self.assertRaises(SystemExit) as e:
            bzfs.main()
        self.assertEqual(2, e.exception.code)

    def test_basic_replication_dataset_with_spaces(self) -> None:
        d1 = " foo  zoo  "
        src_foo = create_filesystem(ibase.SRC_ROOT_DATASET, d1)
        s1 = fix(" s  nap1   ")
        take_snapshot(src_foo, fix(s1))
        d2 = "..::   exit HOME f1.2 echo "
        src_foo_a = create_filesystem(src_foo, d2)
        t1 = fix(d2 + "snap")
        take_snapshot(src_foo_a, fix(t1))
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "-v", "-v", "-v")
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/" + d1))
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET + "/" + d1, [s1])
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/" + d1 + "/" + d2))
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET + "/" + d1 + "/" + d2, [t1])

    def test_basic_replication_flat_simple_with_timeout_infinity(self) -> None:
        self.setup_basic()
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--timeout=1000seconds")
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

    def test_basic_replication_flat_simple_with_timeout_zero(self) -> None:
        self.setup_basic()
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--timeout=0seconds", expected_status=DIE_STATUS)

    def test_basic_replication_flat_simple_with_timeout_almost_zero(self) -> None:
        self.setup_basic()
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--timeout=1milliseconds", expected_status=DIE_STATUS)

    def test_basic_replication_flat_simple_with_exclude_envvar(self) -> None:
        self.setup_basic()
        param_name = "EDITOR"
        old_value = os.environ.get(param_name)
        try:
            os.environ[param_name] = "foo"
            self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
            self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
        finally:
            if old_value is None:
                os.environ.pop(param_name, None)
            else:
                os.environ[param_name] = old_value

    def test_basic_replication_flat_simple_with_multiple_root_datasets(self) -> None:
        self.setup_basic()
        param_name = ENV_VAR_PREFIX + "reuse_ssh_connection"
        old_value = os.environ.get(param_name)
        try:
            os.environ[param_name] = "False"
            for i in range(2):
                with stop_on_failure_subtest(i=i):
                    self.run_bzfs(
                        ibase.SRC_ROOT_DATASET,
                        ibase.DST_ROOT_DATASET,
                        ibase.SRC_ROOT_DATASET,
                        ibase.DST_ROOT_DATASET,
                        "-v",
                        "-v",
                        "-v",
                        dry_run=(i == 0),
                    )
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
                    if i == 0:
                        self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                    else:
                        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
        finally:
            if old_value is None:
                os.environ.pop(param_name, None)
            else:
                os.environ[param_name] = old_value

    def test_basic_replication_flat_simple_with_multiple_root_datasets_with_skip_on_error(self) -> None:
        self.setup_basic()
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "_nonexistingdataset1",
                    ibase.DST_ROOT_DATASET,
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    ibase.SRC_ROOT_DATASET + "_nonexistingdataset2",
                    ibase.DST_ROOT_DATASET,
                    "--delete-dst-snapshots",
                    "--delete-dst-datasets",
                    dry_run=(i == 0),
                    skip_on_error="dataset",
                    expected_status=DIE_STATUS,
                    max_exceptions_to_summarize=1000 if i == 0 else 0,
                )
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

    def test_basic_replication_flat_with_multiple_root_datasets_converted_from_recursive(self, volume: bool = False) -> None:
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.setup_basic(volume=volume)
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                src_datasets = zfs_list([ibase.SRC_ROOT_DATASET], types=["filesystem", "volume"], max_depth=None)
                dst_datasets = [
                    replace_prefix(src_dataset, ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
                    for src_dataset in src_datasets
                ]
                opts = [elem for pair in zip(src_datasets, dst_datasets) for elem in pair]
                self.run_bzfs(*opts, dry_run=(i == 0))
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))  # b/c src has no snapshots

                    compression_prop = dataset_property(ibase.DST_ROOT_DATASET + "/foo", "compression")
                    self.assertEqual("on", compression_prop)
                    encryption_prop = dataset_property(ibase.DST_ROOT_DATASET, "encryption")
                    self.assertEqual("off", encryption_prop)
                    encryption_prop = dataset_property(ibase.DST_ROOT_DATASET + "/foo", "encryption")
                    self.assertEqual(ENCRYPTION_ALGO if self.is_encryption_mode() else "off", encryption_prop)
                    encryption_prop = dataset_property(ibase.DST_ROOT_DATASET + "/foo/a", "encryption")
                    self.assertEqual(ENCRYPTION_ALGO if self.is_encryption_mode() else "off", encryption_prop)

    def test_basic_replication_flat_send_recv_flags(self) -> None:
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))
        self.setup_basic()
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                extra_opt = ""
                if is_zpool_feature_enabled_or_active("src", "feature@large_blocks"):
                    extra_opt = " --large-block"
                props = self.properties_with_special_characters()
                opts = [f"{name}={value}" for name, value in props.items()]
                opts = [f"--zfs-recv-program-opt={item}" for opt in opts for item in ("-o", opt)]
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo/a",
                    ibase.DST_ROOT_DATASET + "/foo/a",
                    "-v",
                    "-v",
                    "--zfs-send-program-opts=-v --dryrun" + extra_opt,
                    "--zfs-recv-program-opts=-v -n",
                    "--zfs-recv-program-opt=-u",
                    *opts,
                    dry_run=(i == 0),
                )
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))
                if i == 0:
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))
                else:
                    foo_a = ibase.DST_ROOT_DATASET + "/foo/a"
                    self.assert_snapshots(foo_a, 3, "u")
                    for name, value in props.items():
                        self.assertEqual(value, dataset_property(foo_a, name))

    def test_basic_replication_recursive_with_exclude_dataset(self) -> None:
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.setup_basic()
        goo = create_filesystem(ibase.SRC_ROOT_DATASET, "goo")
        take_snapshot(goo, fix("g1"))
        boo = create_filesystem(ibase.SRC_ROOT_DATASET, "boo")
        take_snapshot(boo, fix("b1"))
        moo = create_filesystem(ibase.SRC_ROOT_DATASET, "moo")
        take_snapshot(moo, fix("m1"))
        zoo = create_filesystem(ibase.SRC_ROOT_DATASET, "zoo")
        take_snapshot(zoo, fix("z1"))
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--recursive",
                    "--include-dataset=",
                    "--include-dataset=/" + ibase.SRC_ROOT_DATASET,
                    "--exclude-dataset=/" + ibase.SRC_ROOT_DATASET + "/foo",
                    "--include-dataset=/" + ibase.SRC_ROOT_DATASET + "/foo",
                    "--exclude-dataset=/" + ibase.DST_ROOT_DATASET + "/goo/",
                    "--include-dataset=/" + ibase.DST_ROOT_DATASET + "/goo",
                    "--include-dataset=/xxxxxxxxx",
                    "--exclude-dataset=boo/",
                    "--include-dataset=boo",
                    "--force-rollback-to-latest-snapshot",
                    dry_run=(i == 0),
                )
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/goo"))
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/boo"))
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/moo", 1, "m")
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/zoo", 1, "z")

    def test_basic_replication_recursive_with_exclude_property(self) -> None:
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.setup_basic()
        goo = create_filesystem(ibase.SRC_ROOT_DATASET, "goo")
        take_snapshot(goo, fix("g1"))
        goo_child = create_filesystem(ibase.SRC_ROOT_DATASET, "goo/child")
        take_snapshot(goo_child, fix("c1"))
        boo = create_filesystem(ibase.SRC_ROOT_DATASET, "boo")
        take_snapshot(boo, fix("b1"))
        moo = create_filesystem(ibase.SRC_ROOT_DATASET, "moo")
        take_snapshot(moo, fix("m1"))
        zoo = create_filesystem(ibase.SRC_ROOT_DATASET, "zoo")
        take_snapshot(zoo, fix("z1"))
        xoo = create_filesystem(ibase.SRC_ROOT_DATASET, "xoo")
        take_snapshot(xoo, fix("x1"))
        sync_false = {"synchoid:sync": "false"}
        sync_true = {"synchoid:sync": "true"}
        sync_true_empty = {"synchoid:sync": ""}
        sync_host_match = {"synchoid:sync": f"xxx.example.com,{socket.gethostname()}"}
        sync_host_mismatch = {"synchoid:sync": "xxx.example.com"}
        zfs_set([ibase.SRC_ROOT_DATASET + "/foo"], sync_false)
        zfs_set([ibase.SRC_ROOT_DATASET + "/goo"], sync_false)
        zfs_set([ibase.SRC_ROOT_DATASET + "/boo"], sync_host_mismatch)
        zfs_set([ibase.SRC_ROOT_DATASET + "/moo"], sync_true)
        zfs_set([ibase.SRC_ROOT_DATASET + "/zoo"], sync_true_empty)
        zfs_set([ibase.SRC_ROOT_DATASET + "/xoo"], sync_host_match)
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--exclude-dataset-property=synchoid:sync",
                    "--recursive",
                    "--force-rollback-to-latest-snapshot",
                    dry_run=(i == 0),
                )
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/goo"))
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/boo"))
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/moo", 1, "m")
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/zoo", 1, "z")
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/xoo", 1, "x")

    def test_basic_replication_recursive_with_exclude_property_with_injected_dataset_deletes(self) -> None:
        self.setup_basic()
        moo = create_filesystem(ibase.SRC_ROOT_DATASET, "moo")
        take_snapshot(moo, fix("m1"))
        sync_true = {"synchoid:sync": "true"}
        zfs_set([ibase.SRC_ROOT_DATASET + "/moo"], sync_true)
        destroy(ibase.DST_ROOT_DATASET, recursive=True)

        # inject deletes for this many times. only after that stop deleting datasets
        counter = Counter(zfs_list_exclude_property=1)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--exclude-dataset-property=synchoid:sync",
            "--recursive",
            delete_injection_triggers={"before": counter},
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
        self.assertEqual(0, counter["zfs_list_exclude_property"])

    def test_basic_replication_recursive_with_skip_on_error(self) -> None:
        for j in range(3):
            self.tearDownAndSetup()
            src_user1 = create_filesystem(ibase.SRC_ROOT_DATASET, "user1")
            src_user1_foo = create_filesystem(src_user1, "foo")
            src_user2 = create_filesystem(ibase.SRC_ROOT_DATASET, "user2")
            src_user2_bar = create_filesystem(src_user2, "bar")

            dst_user1 = create_filesystem(ibase.DST_ROOT_DATASET, "user1")
            dst_user1_foo = ibase.DST_ROOT_DATASET + "/user1/foo"
            dst_user2 = ibase.DST_ROOT_DATASET + "/user2"
            dst_user2_bar = ibase.DST_ROOT_DATASET + "/user2/bar"

            take_snapshot(src_user1, fix("u1"))
            take_snapshot(dst_user1, fix("U1"))  # conflict triggers error as there's no common snapshot

            take_snapshot(src_user1_foo, fix("f1"))
            take_snapshot(src_user2, fix("v1"))
            take_snapshot(src_user2_bar, fix("b1"))

            if j == 0:
                # test skip_on_error='tree'
                for i in range(2):
                    with stop_on_failure_subtest(i=i):
                        self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--skip-parent",
                            "--recursive",
                            dry_run=(i == 0),
                            skip_on_error="tree",
                            expected_status=DIE_STATUS,
                        )
                        if i == 0:
                            self.assertFalse(dataset_exists(dst_user1_foo))
                            self.assertFalse(dataset_exists(dst_user2))
                            self.assert_snapshots(dst_user1, 1, "U")
                        else:
                            self.assert_snapshots(dst_user1, 1, "U")
                            self.assertFalse(dataset_exists(dst_user1_foo))
                            self.assert_snapshots(dst_user2, 1, "v")
                            self.assert_snapshots(dst_user2_bar, 1, "b")
            elif j == 1:
                # test skip_on_error='dataset'
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--skip-parent",
                    "--recursive",
                    dry_run=(i == 0),
                    skip_on_error="dataset",
                    expected_status=DIE_STATUS,
                )
                self.assert_snapshots(dst_user1, 1, "U")
                self.assert_snapshots(dst_user1_foo, 1, "f")
                self.assert_snapshots(dst_user2, 1, "v")
                self.assert_snapshots(dst_user2_bar, 1, "b")
            else:
                # skip_on_error = 'dataset' with a non-existing destination dataset
                destroy(dst_user1, recursive=True)

                # inject send failures before this many tries. only after that succeed the operation
                counter = Counter(full_zfs_send=1)

                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--skip-parent",
                    "--recursive",
                    skip_on_error="dataset",
                    expected_status=1,
                    error_injection_triggers={"before": counter},
                )
                self.assertEqual(0, counter["full_zfs_send"])
                self.assertFalse(dataset_exists(dst_user1))
                self.assert_snapshots(dst_user2, 1, "v")
                self.assert_snapshots(dst_user2_bar, 1, "b")

    def test_basic_replication_flat_simple_using_main(self) -> None:
        self.setup_basic()
        with patch("sys.argv", ["bzfs.py", ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET] + self.log_dir_opt()):
            bzfs.main()
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

        with self.assertRaises(SystemExit) as e:
            with patch("sys.argv", ["bzfs.py", "nonexisting_dataset", ibase.DST_ROOT_DATASET] + self.log_dir_opt()):
                bzfs.main()
            self.assertEqual(e.exception.code, DIE_STATUS)

    def test_basic_replication_with_overlapping_datasets(self) -> None:
        if self.param and self.param.get("ssh_mode", "local") not in ["local", "pull-push"]:
            self.skipTest("Test is only meaningful in local or pull-push mode")
        self.assertTrue(dataset_exists(ibase.SRC_ROOT_DATASET))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        self.setup_basic()
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.SRC_ROOT_DATASET, expected_status=DIE_STATUS)
        self.run_bzfs(ibase.DST_ROOT_DATASET, ibase.DST_ROOT_DATASET, expected_status=DIE_STATUS)
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.SRC_ROOT_DATASET + "/tmp", "--recursive", expected_status=DIE_STATUS)
        self.run_bzfs(ibase.SRC_ROOT_DATASET + "/tmp", ibase.SRC_ROOT_DATASET, "--recursive", expected_status=DIE_STATUS)
        self.run_bzfs(ibase.DST_ROOT_DATASET, ibase.DST_ROOT_DATASET + "/tmp", "--recursive", expected_status=DIE_STATUS)
        self.run_bzfs(ibase.DST_ROOT_DATASET + "/tmp", ibase.DST_ROOT_DATASET, "--recursive", expected_status=DIE_STATUS)

    def test_max_command_line_bytes(self) -> None:
        job = self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--skip-replication")
        self.assertTrue(_get_max_command_line_bytes(job, "dst", os_name="Linux") > 0)
        self.assertTrue(_get_max_command_line_bytes(job, "dst", os_name="FreeBSD") > 0)
        self.assertTrue(_get_max_command_line_bytes(job, "dst", os_name="Darwin") > 0)
        self.assertTrue(_get_max_command_line_bytes(job, "dst", os_name="Windows") > 0)
        self.assertTrue(_get_max_command_line_bytes(job, "dst", os_name="unknown") > 0)

    def test_zfs_set(self) -> None:
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        job = self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--skip-replication")
        props = self.properties_with_special_characters()
        props_list = [f"{name}={value}" for name, value in props.items()]
        bzfs_main.replication._zfs_set(job, [], job.params.dst, ibase.DST_ROOT_DATASET)
        bzfs_main.replication._zfs_set(job, props_list, job.params.dst, ibase.DST_ROOT_DATASET)
        for name, value in props.items():
            self.assertEqual(value, dataset_property(ibase.DST_ROOT_DATASET, name))

    def test_zfs_set_via_recv_o(self) -> None:
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        for i in range(5):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                self.setup_basic()
                props = self.properties_with_special_characters()
                zfs_set([ibase.SRC_ROOT_DATASET + "/foo"], props)
                disable_pv = [] if i <= 0 else ["--pv-program=" + bzfs_main.detect.DISABLE_PRG]
                disable_mbuffer = [] if i <= 1 else ["--mbuffer-program=" + bzfs_main.detect.DISABLE_PRG]
                disable_zstd = [] if i <= 2 else ["--compression-program=" + bzfs_main.detect.DISABLE_PRG]
                disable_sh = [] if i <= 3 else ["--shell-program=" + bzfs_main.detect.DISABLE_PRG]
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    "--zfs-send-program-opts=",
                    "--zfs-recv-o-include-regex",
                    *list(props.keys()),
                    *disable_pv,
                    *disable_mbuffer,
                    *disable_zstd,
                    *disable_sh,
                )
                for name, value in props.items():
                    self.assertEqual(value, dataset_property(ibase.DST_ROOT_DATASET + "/foo", name))

    def test_zfs_set_via_set_include(self) -> None:
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        self.setup_basic()
        props = self.properties_with_special_characters()
        zfs_set([ibase.SRC_ROOT_DATASET + "/foo"], props)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET + "/foo",
            ibase.DST_ROOT_DATASET + "/foo",
            "--zfs-send-program-opts=",
            "--zfs-set-include-regex",
            *list(props.keys()),
        )
        for name, value in props.items():
            self.assertEqual(value, dataset_property(ibase.DST_ROOT_DATASET + "/foo", name))

    def test_dquote_over_real_ssh_backslash_backtick(self) -> None:
        """Exercises dquote via the real ssh CLI with a backslash+backtick payload.

        Builds a nested shell command of the form

            sh -c "ssh -F SSH_CONFIG_FILE 127.0.0.1 sh -c \"<script>\""

        where <script> is constructed via dquote(shlex.join([...])) and the final argument contains "/tmp/foo\\`bar".
        With the correct dquote implementation (which escapes backslashes), the command succeeds and the payload
        round-trips unchanged.
        """
        if shutil.which("ssh") is None:
            self.skipTest("ssh client not available")

        bad_value = r"/tmp/foo\`bar"
        prop_arg = f"prop={bad_value}"
        recv_cmd = [
            sys.executable,
            "-c",
            "import sys; print(sys.argv[-1])",
            prop_arg,
        ]
        recv_cmd_str = shlex.join(recv_cmd)

        # Build the remote script using the same dquote helper used in replication.
        script = dquote(recv_cmd_str)

        # Use the same SSH config and port as the integration harness so that
        # authentication works consistently on localhost.
        cmd: list[str] = [SSH_PROGRAM, "-F", SSH_CONFIG_FILE]
        port = getenv_any("test_ssh_port")
        if port is not None:
            cmd += ["-p", str(port)]
        cmd += ["127.0.0.1", "sh", "-c", script]
        completed = subprocess.run(cmd, capture_output=True, text=True, check=True)
        self.assertEqual(prop_arg, completed.stdout.rstrip("\n"))

    @staticmethod
    def zfs_recv_x_excludes() -> list[str]:
        return []

    def test_zfs_recv_include_regex(self) -> None:
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.tearDownAndSetup()
                self.setup_basic()
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))

                included_props = {"include_bzfs:p1": "value1", "include_bzfs:p2": "value2"}
                excluded_props = {"exclude_bzfs:p3": "value3"}
                zfs_set([ibase.SRC_ROOT_DATASET + "/foo"], included_props)
                zfs_set([ibase.SRC_ROOT_DATASET + "/foo"], excluded_props)
                extra_args = ["--quiet"] if i == 0 else []
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    *extra_args,
                    "--zfs-send-program-opts=",
                    "--zfs-recv-o-targets=full",
                    "--zfs-recv-o-sources=local,inherited",
                    "--zfs-recv-o-include-regex=include_bzfs.*",
                    "--zfs-recv-x-targets=full+incremental",
                    "--zfs-recv-x-include-regex=.*",
                    "--zfs-recv-x-exclude-regex",
                    "include_bzfs.*",
                    *self.zfs_recv_x_excludes(),
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
                for name, value in included_props.items():
                    self.assertEqual(value, dataset_property(ibase.DST_ROOT_DATASET + "/foo", name))
                for name, _ in excluded_props.items():
                    self.assertEqual("-", dataset_property(ibase.DST_ROOT_DATASET + "/foo", name))

    def test_zfs_recv_include_regex_with_duplicate_o_and_x_names(self) -> None:
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        self.setup_basic()
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))

        included_props = {"include_bzfs:p1": "v1", "include_bzfs:p2": "v2", "include_bzfs:p3": "v3"}
        excluded_props = {"exclude_bzfs:p4": "v4", "exclude_bzfs:p5": "v5"}
        zfs_set([ibase.SRC_ROOT_DATASET + "/foo"], included_props)
        zfs_set([ibase.SRC_ROOT_DATASET + "/foo"], excluded_props)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET + "/foo",
            ibase.DST_ROOT_DATASET + "/foo",
            "--zfs-send-program-opts=--raw",
            "--zfs-recv-program-opts",
            "-u -o include_bzfs:p1=v1 -x exclude_bzfs:p4",
            "--zfs-recv-o-include-regex=include_bzfs.*",
            "--zfs-recv-x-include-regex=.*",  # will not append include.* as those names already exist in -o options
            "--zfs-recv-x-exclude-regex",
            "xxxxxx",
            *self.zfs_recv_x_excludes(),
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
        for name, value in included_props.items():
            self.assertEqual(value, dataset_property(ibase.DST_ROOT_DATASET + "/foo", name))
        for name, _ in excluded_props.items():
            self.assertEqual("-", dataset_property(ibase.DST_ROOT_DATASET + "/foo", name))

    def test_preserve_recordsize(self) -> None:
        if self.is_no_privilege_elevation():
            self.skipTest("setting properties via zfs receive -o needs extra permissions")
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.tearDownAndSetup()
                self.setup_basic()
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))

                old_recordsize = int(dataset_property(ibase.DST_ROOT_DATASET, "recordsize"))
                new_recordsize = 8 * 1024
                assert old_recordsize != new_recordsize
                zfs_set([ibase.SRC_ROOT_DATASET + "/foo"], {"recordsize": str(new_recordsize)})
                preserve = ["recordsize", "volblocksize"] if i > 0 else []
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    "--zfs-recv-o-include-regex",
                    *preserve,
                    "--zfs-send-program-opts=",
                )
                expected = old_recordsize if i == 0 else new_recordsize
                self.assertEqual(str(expected), dataset_property(ibase.DST_ROOT_DATASET + "/foo", "recordsize"))

    def test_check_zfs_dataset_busy_without_force(self) -> None:
        self.setup_basic()
        inject_params = {"is_zfs_dataset_busy": True}
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, expected_status=DIE_STATUS, inject_params=inject_params
        )

    def test_check_zfs_dataset_busy_with_force(self) -> None:
        self.setup_basic()
        inject_params = {"is_zfs_dataset_busy": True}
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--force",
            expected_status=DIE_STATUS,
            inject_params=inject_params,
        )

    def test_periodic_job_locking(self) -> None:
        if self.param and self.param.get("ssh_mode", "local") != "local":
            self.skipTest(
                "run_bzfs changes params so this test only passes in local mode (the feature works "
                "correctly in any mode though)"
            )
        self.setup_basic()
        args = bzfs.argument_parser().parse_args(args=[ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET])
        log_params = LogParams(args)
        params = self.make_params(args=args, log_params=log_params)
        lock_file = params.lock_file_name()
        with open(lock_file, "w", encoding="utf-8") as lock_file_fd:
            # Acquire an exclusive lock; will raise an error if the lock is already held by another process.
            # The (advisory) lock is auto-released when the process terminates or the file descriptor is closed.
            self.assertTrue(os.path.exists(lock_file))
            fcntl.flock(lock_file_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # LOCK_NB ... non-blocking
            try:
                # Run bzfs in a separate process so advisory flock conflicts across processes,
                # matching real-world cron/daemon behavior. Use minimal args to match the lock hash.
                cmd = [sys.executable, "-m", "bzfs_main.bzfs", ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET]
                result = subprocess.run(cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True)
                self.assertEqual(bzfs.STILL_RUNNING_STATUS, result.returncode, f"{result.stdout}\n{result.stderr}")
                self.assertTrue(os.path.exists(lock_file))
            finally:
                Path(lock_file).unlink(missing_ok=True)  # avoid accumulation of stale lock files
                self.assertFalse(os.path.exists(lock_file))

    def test_basic_replication_with_delegation_disabled(self) -> None:
        if not self.is_no_privilege_elevation():
            self.skipTest("Test requires --no-privilege-elevation")
        self.setup_basic()

        run_cmd(SUDO_CMD + ["zpool", "set", "delegation=off", SRC_POOL_NAME])
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, expected_status=DIE_STATUS)

        run_cmd(SUDO_CMD + ["zpool", "set", "delegation=on", SRC_POOL_NAME])
        run_cmd(SUDO_CMD + ["zpool", "set", "delegation=off", DST_POOL_NAME])
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, expected_status=DIE_STATUS)
        run_cmd(SUDO_CMD + ["zpool", "set", "delegation=on", DST_POOL_NAME])

    def test_regex_compilation_error(self) -> None:
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--include-snapshot-regex=(xxx",
            "--skip-missing-snapshots=dataset",
            expected_status=DIE_STATUS,
        )

    def test_basic_replication_skip_missing_snapshots(self) -> None:
        self.assertTrue(dataset_exists(ibase.SRC_ROOT_DATASET))
        destroy(ibase.DST_ROOT_DATASET)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-missing-snapshots=fail",
            expected_status=DIE_STATUS,
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--skip-missing-snapshots=dataset")
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-missing-snapshots=dataset",
            "--include-snapshot-regex=",
            "--recursive",
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--skip-missing-snapshots=continue")
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

        self.setup_basic()
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-missing-snapshots=fail",
            "--include-snapshot-regex=",
            "--recursive",
            expected_status=DIE_STATUS,
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-missing-snapshots=dataset",
            "--include-snapshot-regex=!.*",
            "--recursive",
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-missing-snapshots=continue",
            "--include-snapshot-regex=",
            "--recursive",
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

    def test_no_common_snapshot_basic(self) -> None:
        self.setup_basic()
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET + "/foo",
            ibase.DST_ROOT_DATASET + "/foo",
            "--recursive",
            no_create_bookmark=True,
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 0, "s")
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--force",
            "--f1",
            no_create_bookmark=True,
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))  # b/c src has no snapshots

    def test_no_common_snapshot_with_conflicting_dst_snapshot(self) -> None:
        self.setup_basic()
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET + "/foo",
            ibase.DST_ROOT_DATASET + "/foo",
            "--recursive",
            no_create_bookmark=True,
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 0, "s")
        take_snapshot(ibase.DST_ROOT_DATASET, fix("w1"))
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            no_create_bookmark=True,
            expected_status=DIE_STATUS,
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 1, "w")
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--force",
            "--f1",
            no_create_bookmark=True,
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))  # b/c src has no snapshots

    def test_basic_replication_with_injected_dataset_deletes(self) -> None:
        destroy(ibase.DST_ROOT_DATASET)
        self.setup_basic()
        self.assertTrue(dataset_exists(ibase.SRC_ROOT_DATASET))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

        # inject deletes for this many times. only after that stop deleting datasets
        counter = Counter(zfs_list_snapshot_src=1)

        self.run_bzfs(
            ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "-v", "-v", delete_injection_triggers={"before": counter}
        )
        self.assertFalse(dataset_exists(ibase.SRC_ROOT_DATASET))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
        self.assertEqual(0, counter["zfs_list_snapshot_dst"])

    def test_basic_replication_flat_simple_with_sufficiently_many_retries_on_error_injection(self) -> None:
        self.basic_replication_flat_simple_with_retries_on_error_injection(retries=6, expected_status=0)

    def test_basic_replication_flat_simple_with_insufficiently_many_retries_on_error_injection(self) -> None:
        self.basic_replication_flat_simple_with_retries_on_error_injection(retries=5, expected_status=1)

    def basic_replication_flat_simple_with_retries_on_error_injection(
        self, retries: int = 0, expected_status: int = 0
    ) -> None:
        self.setup_basic()
        create_filesystem(ibase.DST_ROOT_DATASET)

        # inject failures for this many tries. only after that finally succeed the operation
        counter = Counter(zfs_list_snapshot_dst=2, full_zfs_send=2, incr_zfs_send=2)

        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            retries=retries,
            expected_status=expected_status,
            error_injection_triggers={"before": counter},
        )
        self.assertEqual(0, counter["zfs_list_snapshot_dst"])  # i.e, it took 2-0=2 retries to succeed
        self.assertEqual(0, counter["full_zfs_send"])
        self.assertEqual(0, counter["incr_zfs_send"])
        if expected_status == 0:
            self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

    def test_basic_replication_flat_simple_with_retryable_run_tasks_error(self) -> None:
        self.setup_basic()

        # inject failures before this many tries. only after that succeed the operation
        counter = Counter(retryable_run_tasks=1)

        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            expected_status=1,
            error_injection_triggers={"before": counter},
        )
        self.assertEqual(0, counter["retryable_run_tasks"])
        self.assertEqual(0, len(snapshots(ibase.DST_ROOT_DATASET, max_depth=None)))

    def test_basic_replication_recursive_simple_with_force_unmount(self) -> None:
        if self.is_encryption_mode():
            self.skipTest("encryption key not loaded")
        self.setup_basic()
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive")
        dst_foo = ibase.DST_ROOT_DATASET + "/foo"
        dst_foo_a = dst_foo + "/a"
        run_cmd(["sudo", "-n", "zfs", "mount", dst_foo])
        # run_cmd(["sudo", "-n", "zfs", "mount", dst_foo_a])
        take_snapshot(dst_foo, fix("x1"))  # --force will need to rollback that dst snap
        take_snapshot(dst_foo_a, fix("y1"))  # --force will need to rollback that dst snap
        # self.run_bzfs(ibase.src_root_dataset, ibase.dst_root_dataset, '--force', '--recursive')
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--force-rollback-to-latest-common-snapshot",
            "--recursive",
            "--force-unmount",
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))  # b/c src has no snapshots

    def test_basic_replication_flat_with_bookmarks0(self) -> None:
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        for j in range(2):
            for i in range(3):
                with stop_on_failure_subtest(i=j * 3 + i):
                    self.tearDownAndSetup()
                    if j == 0:
                        take_snapshot(ibase.SRC_ROOT_DATASET, fix("a_minutely"))
                        take_snapshot(ibase.SRC_ROOT_DATASET, fix("b_minutely"))
                        take_snapshot(ibase.SRC_ROOT_DATASET, fix("c_minutely"))
                        take_snapshot(ibase.SRC_ROOT_DATASET, fix("d_daily"))
                    else:
                        take_snapshot(ibase.SRC_ROOT_DATASET, fix("a_minutely"))
                        take_snapshot(ibase.SRC_ROOT_DATASET, fix("d_daily"))
                        take_snapshot(ibase.SRC_ROOT_DATASET, fix("b_minutely"))
                        take_snapshot(ibase.SRC_ROOT_DATASET, fix("c_minutely"))
                    self.run_bzfs(
                        ibase.SRC_ROOT_DATASET,
                        ibase.DST_ROOT_DATASET,
                        "--create-bookmarks=" + ["none", "hourly", "all"][i],
                        dry_run=(i == 0),
                    )
                    if i == 0:
                        self.assert_snapshot_names(
                            ibase.SRC_ROOT_DATASET, ["a_minutely", "b_minutely", "c_minutely", "d_daily"]
                        )
                        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, [])
                        self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, [])
                    elif i == 1:
                        self.assert_snapshot_names(
                            ibase.DST_ROOT_DATASET, ["a_minutely", "b_minutely", "c_minutely", "d_daily"]
                        )
                        if j == 0:
                            self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["a_minutely", "d_daily"])
                        else:
                            self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["a_minutely", "c_minutely", "d_daily"])
                    else:
                        self.assert_snapshot_names(
                            ibase.DST_ROOT_DATASET, ["a_minutely", "b_minutely", "c_minutely", "d_daily"]
                        )
                        self.assert_bookmark_names(
                            ibase.SRC_ROOT_DATASET, ["a_minutely", "b_minutely", "c_minutely", "d_daily"]
                        )

    def test_xbasic_replication_flat_with_bookmarks1(self) -> None:
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("d1"))
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, dry_run=(i == 0))
                if i == 0:
                    self.assert_snapshot_names(ibase.SRC_ROOT_DATASET, ["d1"])
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, [])
                else:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["d1"])
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1"])

        # delete snapshot, which will cause no problem as we still have its bookmark
        destroy(snapshots(ibase.SRC_ROOT_DATASET)[0])
        self.assert_snapshot_names(ibase.SRC_ROOT_DATASET, [])
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--skip-missing-snapshots=fail", dry_run=(i == 0)
                )
                self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["d1"])  # nothing has changed
                self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1"])  # nothing has changed

        # take another snapshot and replicate it without problems as we still have the bookmark
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("d2"))
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--skip-missing-snapshots=fail", dry_run=(i == 0)
                )
                if i == 0:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["d1"])  # nothing has changed
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1"])  # nothing has changed
                else:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["d1", "d2"])
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1", "d2"])

    def test_basic_replication_flat_with_bookmarks2(self) -> None:
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("d1"))
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, dry_run=(i == 0))
                if i == 0:
                    self.assert_snapshot_names(ibase.SRC_ROOT_DATASET, ["d1"])
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, [])
                else:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["d1"])
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1"])

        # rename snapshot, which will cause no problem as we still have its bookmark
        cmd = SUDO_CMD + [
            "zfs",
            "rename",
            snapshots(ibase.SRC_ROOT_DATASET)[0],
            snapshots(ibase.SRC_ROOT_DATASET)[0] + "h",
        ]
        run_cmd(cmd)

        for i in range(2):
            # replicate while excluding the rename snapshot
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--exclude-snapshot-regex=.*h",
                    "--skip-missing-snapshots=fail",
                    dry_run=(i == 0),
                )
                self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["d1"])  # nothing has changed
                self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1"])  # nothing has changed

        # take another snapshot and replicate it without problems as we still have the bookmark
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("d2"))
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--exclude-snapshot-regex=.*h",
                    "--skip-missing-snapshots=fail",
                    dry_run=(i == 0),
                )
                if i == 0:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["d1"])  # nothing has changed
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1"])  # nothing has changed
                else:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["d1", "d2"])
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1", "d2"])

    def test_basic_replication_flat_with_bookmarks3(self) -> None:
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("d1"))
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, no_create_bookmark=True, dry_run=(i == 0))
                if i == 0:
                    self.assert_snapshot_names(ibase.SRC_ROOT_DATASET, ["d1"])
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, [])
                else:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["d1"])
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, [])
        snapshot_tag = snapshots(ibase.SRC_ROOT_DATASET)[0].split("@", 1)[1]
        create_bookmark(ibase.SRC_ROOT_DATASET, snapshot_tag, snapshot_tag + "h")
        create_bookmark(ibase.SRC_ROOT_DATASET, snapshot_tag, snapshot_tag)
        self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1", "d1h"])

        # delete snapshot, which will cause no problem as we still have its bookmark
        destroy(snapshots(ibase.SRC_ROOT_DATASET)[0])
        self.assert_snapshot_names(ibase.SRC_ROOT_DATASET, [])

        for i in range(1, 2):
            # replicate while excluding hourly snapshots
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--exclude-snapshot-regex=.*h",
                    "--skip-missing-snapshots=fail",
                    dry_run=(i == 0),
                )
                self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["d1"])  # nothing has changed
                self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1", "d1h"])  # nothing has changed

        # take another snapshot and replicate it without problems as we still have the bookmark
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("d2"))
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--exclude-snapshot-regex=.*h",
                    "--skip-missing-snapshots=fail",
                    dry_run=(i == 0),
                )
                if i == 0:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["d1"])  # nothing has changed
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1", "d1h"])  # nothing has changed
                else:
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["d1", "d2"])
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1", "d1h", "d2"])

    def test_basic_replication_flat_with_bookmarks_already_exists(self) -> None:
        """Check that run_bzfs works as usual even if the bookmark already exists."""
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("d1"))
        snapshot_tag = snapshots(ibase.SRC_ROOT_DATASET)[0].split("@", 1)[1]

        create_bookmark(ibase.SRC_ROOT_DATASET, snapshot_tag, snapshot_tag)

        self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1"])
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, dry_run=(i == 0))
                if i == 0:
                    self.assert_snapshot_names(ibase.SRC_ROOT_DATASET, ["d1"])
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1"])
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, [])
                else:
                    self.assert_snapshot_names(ibase.SRC_ROOT_DATASET, ["d1"])
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["d1"])
                    self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["d1"])

    def test_complex_replication_flat_with_no_create_bookmark(self) -> None:
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.setup_basic()
        src_foo = build(ibase.SRC_ROOT_DATASET + "/foo")
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                if i == 0:
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))

        # on src take some snapshots
        take_snapshot(src_foo, fix("t4"))
        take_snapshot(src_foo, fix("t5"))
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 5, "t")

        # on src take another snapshot
        take_snapshot(src_foo, fix("t6"))
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 5, "t")
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 6, "t")

        # on dst (rather than src) take some snapshots, which is asking for trouble...
        dst_foo = build(ibase.DST_ROOT_DATASET + "/foo")
        take_snapshot(dst_foo, fix("t7"))
        take_snapshot(dst_foo, fix("t8"))
        # Conflict: Most recent destination snapshot is more recent than most recent common snapshot
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                    expected_status=DIE_STATUS,
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 8, "t")  # nothing has changed on dst

        # resolve conflict via dst rollback to most recent common snapshot
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    "--force-rollback-to-latest-common-snapshot",
                    "--force-once",
                    dry_run=(i == 0),
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 8, "t")  # nothing has changed on dst
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 6, "t")

        # on src and dst, take some snapshots, which is asking for trouble again...
        src_guid = snapshot_property(take_snapshot(src_foo, fix("t7")), "guid")
        dst_guid = snapshot_property(take_snapshot(dst_foo, fix("t7")), "guid")
        # names of t7 are the same but GUIDs are different as they are not replicas of each other - t7 is not a common snap.
        self.assertNotEqual(src_guid, dst_guid)
        take_snapshot(dst_foo, fix("t8"))
        # Conflict: Most recent destination snapshot is more recent than most recent common snapshot
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                    expected_status=DIE_STATUS,
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 8, "t")  # unchanged
                self.assertEqual(
                    dst_guid, snapshot_property(snapshots(build(ibase.DST_ROOT_DATASET + "/foo"))[6], "guid")
                )  # nothing has changed on dst

        # resolve conflict via dst rollback to most recent common snapshot prior to replicating
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    "--force-rollback-to-latest-common-snapshot",
                    "--force-once",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                )  # resolve conflict via dst rollback
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 8, "t")  # unchanged
                    self.assertEqual(
                        dst_guid, snapshot_property(snapshots(build(ibase.DST_ROOT_DATASET + "/foo"))[6], "guid")
                    )  # nothing has changed on dst
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 7, "t")
                    self.assertEqual(
                        src_guid, snapshot_property(snapshots(build(ibase.DST_ROOT_DATASET + "/foo"))[6], "guid")
                    )  # now they are true replicas

        # on src delete some snapshots are older than most recent common snap, which is normal and won't cause changes to dst
        destroy(snapshots(src_foo)[0])
        destroy(snapshots(src_foo)[2])
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 7, "t")

        # replicate a child dataset
        self.run_bzfs(ibase.SRC_ROOT_DATASET + "/foo/a", ibase.DST_ROOT_DATASET + "/foo/a", no_create_bookmark=True)
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 7, "t")

        # on src delete all snapshots so now there is no common snapshot anymore, which is trouble...
        for snap in snapshots(src_foo):
            destroy(snap)
        take_snapshot(src_foo, fix("t9"))
        take_snapshot(src_foo, fix("t10"))
        take_snapshot(src_foo, fix("t11"))
        self.assert_snapshots(ibase.SRC_ROOT_DATASET + "/foo", 3, "t", offset=8)
        # Conflict: no common snapshot was found.
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                    expected_status=DIE_STATUS,
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 7, "t")  # nothing has changed on dst
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")

        # resolve conflict via deleting all dst snapshots prior to replication
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                if i > 0 and self.is_encryption_mode():
                    # potential workaround?: rerun once with -R --skip-missing added to zfs_send_program_opts
                    self.skipTest(
                        "zfs receive -F cannot be used to destroy an encrypted filesystem - https://github.com/openzfs/zfs/issues/6793"
                    )
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    "--force",
                    "--f1",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 7, "t")  # nothing changed
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t", offset=8)

        # no change on src means replication is a noop:
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t", offset=8)

        # no change on src means replication is a noop:
        for i in range(2):
            # rollback dst to most recent snapshot
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    "--force-rollback-to-latest-common-snapshot",
                    dry_run=(i == 0),
                    no_create_bookmark=True,
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t", offset=8)

    def test_complex_replication_flat_use_bookmarks_with_volume(self) -> None:
        self.test_complex_replication_flat_use_bookmarks(volume=True)

    def test_complex_replication_flat_use_bookmarks(self, volume: bool = False) -> None:
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        xtra = ["--create-bookmarks=hourly"]
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.setup_basic()
        src_foo = build(ibase.SRC_ROOT_DATASET + "/foo")
        if volume:
            destroy(src_foo, recursive=True)
            src_foo = create_volume(src_foo, size="1M")
            take_snapshot(src_foo, fix("t1"))
            take_snapshot(src_foo, fix("t2"))
            take_snapshot(src_foo, fix("t3"))

        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET + "/foo", ibase.DST_ROOT_DATASET + "/foo", *xtra, dry_run=(i == 0))
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                if i == 0:
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", [])
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3"])
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))

        # on src take some snapshots
        take_snapshot(src_foo, fix("t4"))
        take_snapshot(src_foo, fix("t5"))
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET + "/foo", ibase.DST_ROOT_DATASET + "/foo", *xtra, dry_run=(i == 0))
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3"])
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 5, "t")
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5"])

        # on src take another snapshot
        take_snapshot(src_foo, fix("t6"))
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET + "/foo", ibase.DST_ROOT_DATASET + "/foo", *xtra, dry_run=(i == 0))
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 5, "t")
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5"])
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 6, "t")
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6"])

        # on dst (rather than src) take some snapshots, which is asking for trouble...
        dst_foo = build(ibase.DST_ROOT_DATASET + "/foo")
        take_snapshot(dst_foo, fix("t7"))
        take_snapshot(dst_foo, fix("t8"))
        # Conflict: Most recent destination snapshot is more recent than most recent common snapshot
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    *xtra,
                    dry_run=(i == 0),
                    expected_status=DIE_STATUS,
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 8, "t")  # nothing has changed on dst
                self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6"])

        # resolve conflict via dst rollback to most recent common snapshot
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    "--force-rollback-to-latest-common-snapshot",
                    "--force-once",
                    *xtra,
                    dry_run=(i == 0),
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 8, "t")  # nothing has changed on dst
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6"])
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 6, "t")
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6"])

        # on src and dst, take some snapshots, which is asking for trouble again...
        src_guid = snapshot_property(take_snapshot(src_foo, fix("t7")), "guid")
        dst_guid = snapshot_property(take_snapshot(dst_foo, fix("t7")), "guid")
        # names of t7 are the same but GUIDs are different as they are not replicas of each other - t7 is not a common snap.
        self.assertNotEqual(src_guid, dst_guid)
        take_snapshot(dst_foo, fix("t8"))
        # Conflict: Most recent destination snapshot is more recent than most recent common snapshot
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    *xtra,
                    dry_run=(i == 0),
                    expected_status=DIE_STATUS,
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 8, "t")  # nothing has changed on dst
                self.assertEqual(
                    dst_guid, snapshot_property(snapshots(build(ibase.DST_ROOT_DATASET + "/foo"))[6], "guid")
                )  # nothing has changed on dst
                self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6"])

        # resolve conflict via dst rollback to most recent common snapshot prior to replicating
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    "--force-rollback-to-latest-common-snapshot",
                    "--force-once",
                    *xtra,
                    dry_run=(i == 0),
                )  # resolve conflict via dst rollback
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 8, "t")  # nothing has changed on dst
                    self.assertEqual(
                        dst_guid, snapshot_property(snapshots(build(ibase.DST_ROOT_DATASET + "/foo"))[6], "guid")
                    )  # nothing has changed on dst
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6"])
                else:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 7, "t")
                    self.assertEqual(
                        src_guid, snapshot_property(snapshots(build(ibase.DST_ROOT_DATASET + "/foo"))[6], "guid")
                    )  # now they are true replicas
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6", "t7"])

        # on src delete some snapshots are older than most recent common snap, which is normal and won't cause changes to dst
        destroy(snapshots(src_foo)[0])
        destroy(snapshots(src_foo)[2])
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET + "/foo", ibase.DST_ROOT_DATASET + "/foo", *xtra, dry_run=(i == 0))
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 7, "t")
                self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6", "t7"])

        # replicate a child dataset
        if not volume:
            self.run_bzfs(ibase.SRC_ROOT_DATASET + "/foo/a", ibase.DST_ROOT_DATASET + "/foo/a", *xtra)
            self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")
            self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo/a", ["u1", "u3"])
            self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 7, "t")

        # on src delete all snapshots so now there is no common snapshot anymore,
        # which isn't actually trouble because we have bookmarks for them...
        for snap in snapshots(src_foo):
            destroy(snap)
        # No Conflict: no common snapshot was found, but we found a (common) bookmark that can be used instead
        # so replication is a noop and won't fail:
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET + "/foo", ibase.DST_ROOT_DATASET + "/foo", *xtra, dry_run=(i == 0))
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 7, "t")  # nothing has changed on dst
                self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6", "t7"])
                if not volume:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")

        take_snapshot(src_foo, fix("t9"))
        take_snapshot(src_foo, fix("t10"))
        take_snapshot(src_foo, fix("t11"))
        self.assert_snapshots(ibase.SRC_ROOT_DATASET + "/foo", 3, "t", offset=8)

        # No Conflict: no common snapshot was found, but we found a (common) bookmark that can be used instead
        # so replication will succeed:
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET + "/foo", ibase.DST_ROOT_DATASET + "/foo", *xtra, dry_run=(i == 0))
                if not volume:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")
                if i == 0:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 7, "t")  # nothing has changed on dst
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6", "t7"])
                else:
                    self.assert_snapshot_names(
                        ibase.DST_ROOT_DATASET + "/foo", ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t9", "t10", "t11"]
                    )
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6", "t7", "t11"])

        # no change on src means replication is a noop:
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(ibase.SRC_ROOT_DATASET + "/foo", ibase.DST_ROOT_DATASET + "/foo", *xtra, dry_run=(i == 0))
                self.assert_snapshot_names(
                    ibase.DST_ROOT_DATASET + "/foo", ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t9", "t10", "t11"]
                )
                self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6", "t7", "t11"])

        # on src delete the most recent snapshot and its bookmark, which is trouble as now src has nothing
        # in common anymore with the most recent dst snapshot:
        destroy(natsorted(snapshots(src_foo), key=lambda s: s)[-1])  # destroy t11
        destroy(natsorted(bookmarks(src_foo), key=lambda b: b)[-1])  # destroy t11
        self.assert_snapshot_names(ibase.SRC_ROOT_DATASET + "/foo", ["t9", "t10"])
        self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6", "t7"])
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    *xtra,
                    dry_run=(i == 0),
                    expected_status=DIE_STATUS,
                )
                self.assert_snapshot_names(
                    ibase.DST_ROOT_DATASET + "/foo", ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t9", "t10", "t11"]
                )  # nothing has changed
                self.assert_bookmark_names(
                    ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6", "t7"]
                )  # nothing has changed

        # resolve conflict via dst rollback to most recent common snapshot prior to replicating
        take_snapshot(src_foo, fix("t12"))
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET + "/foo",
                    ibase.DST_ROOT_DATASET + "/foo",
                    "--force-rollback-to-latest-common-snapshot",
                    *xtra,
                    dry_run=(i == 0),
                )
                if not volume:
                    self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")
                if i == 0:
                    self.assert_snapshot_names(
                        ibase.DST_ROOT_DATASET + "/foo", ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t9", "t10", "t11"]
                    )  # nothing has changed
                    self.assert_bookmark_names(
                        ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6", "t7"]
                    )  # nothing has changed
                else:
                    self.assert_snapshot_names(
                        ibase.DST_ROOT_DATASET + "/foo", ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t9", "t10", "t12"]
                    )  # nothing has changed
                    self.assert_bookmark_names(ibase.SRC_ROOT_DATASET + "/foo", ["t1", "t3", "t5", "t6", "t7", "t12"])

    def test_create_zfs_bookmarks_raises_unexpected_error(self) -> None:
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        self.setup_basic()
        non_existing_snapshot = snapshots(ibase.SRC_ROOT_DATASET)[0] + "$"
        job = self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--create-bookmarks=all")
        with self.assertRaises(subprocess.CalledProcessError):
            bzfs_main.replication._create_zfs_bookmarks(job, job.params.src, ibase.SRC_ROOT_DATASET, [non_existing_snapshot])

    def test_create_zfs_bookmarks_existing_bookmark(self) -> None:
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        self.setup_basic()
        first_snapshot = snapshots(ibase.SRC_ROOT_DATASET)[0]
        first_tag = snapshot_name(first_snapshot)
        create_bookmark(ibase.SRC_ROOT_DATASET, first_tag, first_tag)
        self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["s1"])
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--create-bookmarks=all")
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s1", "s2", "s3"])
        self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["s1", "s2", "s3"])

    @staticmethod
    def create_resumable_snapshots(lo: int, hi: int, size_in_bytes: int = 1024 * 1024) -> None:
        """Large enough to be interruptible and resumable.

        interacts with bzfs.py/inject_dst_pipe_fail
        """
        run_cmd(f"sudo -n zfs mount {ibase.SRC_ROOT_DATASET}".split())
        for j in range(lo, hi):
            tmp_file = "/" + ibase.SRC_ROOT_DATASET + "/tmpfile"
            run_cmd(f"sudo -n dd if=/dev/urandom of={tmp_file} bs={size_in_bytes} count=1".split())
            take_snapshot(ibase.SRC_ROOT_DATASET, fix(f"s{j}"))
        run_cmd(f"sudo -n zfs unmount {ibase.SRC_ROOT_DATASET}".split())

    def test_snapshots_are_all_fully_replicated_even_though_every_recv_is_interrupted_and_resumed(self) -> None:
        if not is_zpool_recv_resume_feature_enabled_or_active():
            self.skipTest("No recv resume zfs feature is available")
        n = 4
        self.create_resumable_snapshots(1, n)
        prev_token = None
        max_iters = 20
        for _ in range(max_iters):
            self.run_bzfs(
                ibase.SRC_ROOT_DATASET,
                ibase.DST_ROOT_DATASET,
                "-v",
                expected_status=[0, 1],
                inject_params={"inject_dst_pipe_fail": True},  # interrupt every 'zfs receive' prematurely
            )
            if len(snapshots(ibase.DST_ROOT_DATASET)) == n - 1:
                break
            curr_token = self.assert_receive_resume_token(ibase.DST_ROOT_DATASET, exists=True)
            self.assertIsNotNone(curr_token)  # assert clear_resumable_recv_state() didn't get called
            self.assertNotEqual(prev_token, curr_token)  # assert send/recv transfers are making progress
            prev_token = curr_token
        logging.getLogger(__name__).warning(f"iterations to fully replicate all snapshots: {max_iters}")
        self.assert_receive_resume_token(ibase.DST_ROOT_DATASET, exists=False)
        self.assert_snapshots(ibase.DST_ROOT_DATASET, n - 1, "s")
        self.assertGreater(max_iters, 2)

    def test_send_full_no_resume_recv_with_resume_token_present(self) -> None:
        if not is_zpool_recv_resume_feature_enabled_or_active():
            self.skipTest("No recv resume zfs feature is available")
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                self.create_resumable_snapshots(1, 4)
                self.generate_recv_resume_token(None, ibase.SRC_ROOT_DATASET + "@s1", ibase.DST_ROOT_DATASET)
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0, "s")
                if i == 0:
                    self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--no-resume-recv", retries=1)
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
                else:
                    self.run_bzfs(
                        ibase.SRC_ROOT_DATASET,
                        ibase.DST_ROOT_DATASET,
                        "--no-resume-recv",
                        retries=0,
                        expected_status=1,
                    )
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 0, "s")
                    self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--no-resume-recv", retries=0)
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

    def test_send_incr_no_resume_recv_with_resume_token_present(self) -> None:
        if not is_zpool_recv_resume_feature_enabled_or_active():
            self.skipTest("No recv resume zfs feature is available")
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                self.create_resumable_snapshots(1, 2)
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
                self.assert_receive_resume_token(ibase.DST_ROOT_DATASET, exists=False)
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 1, "s")
                n = 4
                self.create_resumable_snapshots(2, n)
                self.generate_recv_resume_token(
                    ibase.SRC_ROOT_DATASET + "@s1", ibase.SRC_ROOT_DATASET + "@s3", ibase.DST_ROOT_DATASET
                )
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 1, "s")
                if i == 0:
                    self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--no-resume-recv", retries=1)
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
                else:
                    self.run_bzfs(
                        ibase.SRC_ROOT_DATASET,
                        ibase.DST_ROOT_DATASET,
                        "--no-resume-recv",
                        retries=0,
                        expected_status=1,
                    )
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 1, "s")
                    self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--no-resume-recv", retries=0)
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")

    def test_send_incr_resume_recv(self) -> None:
        if not is_zpool_recv_resume_feature_enabled_or_active():
            self.skipTest("No recv resume zfs feature is available")
        for i in range(4):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                self.create_resumable_snapshots(1, 2)
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
                self.assert_receive_resume_token(ibase.DST_ROOT_DATASET, exists=False)
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 1, "s")
                for bookmark in bookmarks(ibase.SRC_ROOT_DATASET):
                    destroy(bookmark)
                n = 4
                self.create_resumable_snapshots(2, n)
                if i == 0:
                    # inject send failures before this many tries. only after that succeed the operation
                    counter = Counter(incr_zfs_send_params=1)
                    self.run_bzfs(
                        ibase.SRC_ROOT_DATASET,
                        ibase.DST_ROOT_DATASET,
                        retries=1,
                        error_injection_triggers={"before": counter},
                        param_injection_triggers={"incr_zfs_send_params": {"inject_dst_pipe_fail": True}},
                    )
                    self.assertEqual(0, counter["incr_zfs_send_params"])
                else:
                    self.run_bzfs(
                        ibase.SRC_ROOT_DATASET,
                        ibase.DST_ROOT_DATASET,
                        "-v",
                        expected_status=1,
                        inject_params={"inject_dst_pipe_fail": True},
                    )
                    self.assert_receive_resume_token(ibase.DST_ROOT_DATASET, exists=True)
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 1, "s")
                    if i == 1:
                        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
                    else:
                        destroy_snapshots(ibase.SRC_ROOT_DATASET, snapshots(ibase.SRC_ROOT_DATASET)[1:])
                        self.create_resumable_snapshots(2, n)
                        if i == 2:
                            self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, expected_status=255)
                            self.assert_receive_resume_token(ibase.DST_ROOT_DATASET, exists=False)
                            self.assert_snapshots(ibase.DST_ROOT_DATASET, 1, "s")
                            self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
                        else:
                            self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, retries=1)
                self.assert_receive_resume_token(ibase.DST_ROOT_DATASET, exists=False)
                self.assert_snapshots(ibase.DST_ROOT_DATASET, n - 1, "s")

    def test_send_full_resume_recv(self) -> None:
        if not is_zpool_recv_resume_feature_enabled_or_active():
            self.skipTest("No recv resume zfs feature is available")
        for i in range(4):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                destroy(ibase.DST_ROOT_DATASET, recursive=True)
                self.create_resumable_snapshots(1, 2)
                if i == 0:
                    # inject send failures before this many tries. only after that succeed the operation
                    counter = Counter(full_zfs_send_params=1)
                    self.run_bzfs(
                        ibase.SRC_ROOT_DATASET,
                        ibase.DST_ROOT_DATASET,
                        retries=1,
                        error_injection_triggers={"before": counter},
                        param_injection_triggers={"full_zfs_send_params": {"inject_dst_pipe_fail": True}},
                    )
                    self.assertEqual(0, counter["full_zfs_send_params"])
                else:
                    self.run_bzfs(
                        ibase.SRC_ROOT_DATASET,
                        ibase.DST_ROOT_DATASET,
                        "-v",
                        expected_status=1,
                        inject_params={"inject_dst_pipe_fail": True},
                    )
                    self.assert_receive_resume_token(ibase.DST_ROOT_DATASET, exists=True)
                    self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 0, "s")
                    if i == 1:
                        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
                    else:
                        destroy(snapshots(ibase.SRC_ROOT_DATASET)[0])
                        self.create_resumable_snapshots(1, 2)
                        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
                        if i == 2:
                            self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, expected_status=255)
                            # cannot resume send: 'wb_src/tmp/src@s1' is no longer the same snapshot used in the initial send
                            self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))
                            self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
                        else:
                            self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, retries=1)
                self.assert_receive_resume_token(ibase.DST_ROOT_DATASET, exists=False)
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 1, "s")

    def test_compare_snapshot_lists_with_nonexisting_source(self) -> None:
        destroy(ibase.SRC_ROOT_DATASET, recursive=True)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--compare-snapshot-lists",
            expected_status=DIE_STATUS,
        )

    def test_compare_snapshot_lists(self) -> None:
        def snapshot_list(_job: bzfs.Job, location: str = "") -> list[str]:
            log_file = _job.params.log_params.log_file
            tsv_file = glob.glob(glob.escape(log_file[0 : log_file.rindex(".log")] + ".cmp/") + "*.tsv")[0]
            with open(tsv_file, "r", encoding="utf-8") as fd:
                return [line.strip() for line in fd if line.startswith(location) and not line.startswith("location")]

        def stats(_job: bzfs.Job) -> tuple[int, int, int]:
            lines_ = snapshot_list(_job)
            n_src_ = sum(1 for line in lines_ if line.startswith("src"))
            n_dst_ = sum(1 for line in lines_ if line.startswith("dst"))
            n_all_ = sum(1 for line in lines_ if line.startswith("all"))
            return n_src_, n_dst_, n_all_

        k = -1
        for i in range(2):
            for j in range(3):
                k += 1
                with stop_on_failure_subtest(i=k):
                    if k > 0:
                        self.tearDownAndSetup()
                    no_use_bookmark = j == 0
                    xtra = ["--no-use-bookmark"] if no_use_bookmark else []
                    # For j==2 we rely on the new default --create-bookmarks=all (no explicit flag needed)
                    xtra += ["--create-bookmarks=hourly"] if j <= 1 else []
                    param_name = ENV_VAR_PREFIX + "max_datasets_per_batch_on_list_snaps"
                    old_value = os.environ.get(param_name)
                    try:
                        if i > 0:
                            os.environ[param_name] = "0"
                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--skip-replication",
                            "--compare-snapshot-lists",
                            *xtra,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(0, n_src)
                        self.assertEqual(0, n_dst)
                        self.assertEqual(0, n_all)

                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--skip-replication",
                            "--compare-snapshot-lists",
                            "-r",
                            *xtra,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(0, n_src)
                        self.assertEqual(0, n_dst)
                        self.assertEqual(0, n_all)

                        self.setup_basic()
                        cmp_choices: list[str] = []
                        for w in range(len(argparse_cli.CMP_CHOICES_ITEMS)):
                            cmp_choices += [
                                "+".join(c) for c in itertools.combinations(argparse_cli.CMP_CHOICES_ITEMS, w + 1)
                            ]
                        for cmp in cmp_choices:
                            job = self.run_bzfs(
                                ibase.SRC_ROOT_DATASET,
                                ibase.DST_ROOT_DATASET,
                                "--skip-replication",
                                f"--compare-snapshot-lists={cmp}",
                                *xtra,
                            )
                            n_src, n_dst, n_all = stats(job)
                            self.assertEqual(3 if "src" in cmp else 0, n_src)
                            self.assertEqual(0, n_dst)
                            self.assertEqual(0, n_all)
                            if self.is_smoke_test:
                                break
                        if self.is_smoke_test:
                            continue

                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--compare-snapshot-lists", *xtra
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(0, n_src)
                        self.assertEqual(0, n_dst)
                        self.assertEqual(3, n_all)
                        self.assertEqual(3, len(snapshot_list(job, "all")))
                        lines = snapshot_list(job)
                        self.assertEqual(3, len(lines))
                        rel_name = 3
                        root_dataset = 5
                        rel_dataset = 6
                        name = 7
                        creation = 8
                        written = 9
                        self.assertEqual("@s1", lines[0].split("\t")[rel_name])
                        self.assertEqual("@s2", lines[1].split("\t")[rel_name])
                        self.assertEqual("@s3", lines[2].split("\t")[rel_name])
                        self.assertEqual(ibase.SRC_ROOT_DATASET, lines[0].split("\t")[root_dataset])
                        self.assertEqual(ibase.SRC_ROOT_DATASET, lines[1].split("\t")[root_dataset])
                        self.assertEqual(ibase.SRC_ROOT_DATASET, lines[2].split("\t")[root_dataset])
                        self.assertEqual("", lines[0].split("\t")[rel_dataset])
                        self.assertEqual("", lines[1].split("\t")[rel_dataset])
                        self.assertEqual("", lines[2].split("\t")[rel_dataset])
                        self.assertEqual(ibase.SRC_ROOT_DATASET + "@s1", lines[0].split("\t")[name])
                        self.assertEqual(ibase.SRC_ROOT_DATASET + "@s2", lines[1].split("\t")[name])
                        self.assertEqual(ibase.SRC_ROOT_DATASET + "@s3", lines[2].split("\t")[name])
                        self.assertLessEqual(0, int(lines[0].split("\t")[creation]))
                        self.assertLessEqual(0, int(lines[1].split("\t")[creation]))
                        self.assertLessEqual(0, int(lines[2].split("\t")[creation]))
                        self.assertLessEqual(0, int(lines[0].split("\t")[written]))
                        self.assertLessEqual(0, int(lines[1].split("\t")[written]))
                        self.assertLessEqual(0, int(lines[2].split("\t")[written]))

                        for cmp in cmp_choices:
                            job = self.run_bzfs(
                                ibase.SRC_ROOT_DATASET,
                                ibase.DST_ROOT_DATASET,
                                f"--compare-snapshot-lists={cmp}",
                                "-r",
                                *xtra,
                                max_datasets_per_minibatch_on_list_snaps=1,
                            )
                            n_src, n_dst, n_all = stats(job)
                            self.assertEqual(0, n_src)
                            self.assertEqual(0, n_dst)
                            self.assertEqual(3 + 3 + 3 if "all" in cmp else 0, n_all)

                        for threads in range(1, 6):
                            job = self.run_bzfs(
                                ibase.SRC_ROOT_DATASET,
                                ibase.DST_ROOT_DATASET,
                                "--skip-replication",
                                "--compare-snapshot-lists",
                                "-r",
                                "-v",
                                f"--threads={threads}",
                                *xtra,
                                max_datasets_per_minibatch_on_list_snaps=1,
                            )
                            n_src, n_dst, n_all = stats(job)
                            self.assertEqual(0, n_src)
                            self.assertEqual(0, n_dst)
                            self.assertEqual(3 + 3 + 3, n_all)

                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--compare-snapshot-lists",
                            *xtra,
                            dry_run=True,
                        )

                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--skip-replication",
                            "--compare-snapshot-lists",
                            "-r",
                            "--include-snapshot-regex=[su]1.*",
                            *xtra,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(0, n_src)
                        self.assertEqual(0, n_dst)
                        self.assertEqual(1 + 1, n_all)

                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--skip-replication",
                            "--compare-snapshot-lists",
                            "-r",
                            "--include-snapshot-times-and-ranks",
                            "0..0",
                            "latest 1",
                            *xtra,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(0, n_src)
                        self.assertEqual(0, n_dst)
                        self.assertEqual(1 + 1 + 1, n_all)
                        lines = snapshot_list(job)
                        self.assertEqual("@s3", lines[0].split("\t")[rel_name])
                        self.assertEqual("/foo@t3", lines[1].split("\t")[rel_name])
                        self.assertEqual("/foo/a@u3", lines[2].split("\t")[rel_name])

                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--skip-replication",
                            "--compare-snapshot-lists",
                            "-r",
                            "--include-snapshot-times-and-ranks=0..2999-01-01",
                            *xtra,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(0, n_src)
                        self.assertEqual(0, n_dst)
                        self.assertEqual(3 + 3 + 3, n_all)

                        destroy(snapshots(ibase.DST_ROOT_DATASET)[0])
                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--skip-replication",
                            "--compare-snapshot-lists",
                            "-r",
                            *xtra,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(1, n_src)
                        self.assertEqual(0, n_dst)
                        self.assertEqual(3 + 3 + 3 - 1, n_all)

                        destroy_snapshots(ibase.DST_ROOT_DATASET, snapshots(ibase.DST_ROOT_DATASET))
                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--skip-replication",
                            "--compare-snapshot-lists",
                            "-r",
                            *xtra,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(3, n_src)
                        self.assertEqual(0, n_dst)
                        self.assertEqual(3 + 3, n_all)

                        if not are_bookmarks_enabled("src"):
                            continue

                        src_foo = ibase.SRC_ROOT_DATASET + "/foo"
                        destroy(snapshots(src_foo)[0])  # but retain bookmark
                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--skip-replication",
                            "--compare-snapshot-lists",
                            "-r",
                            *xtra,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(3, n_src)
                        self.assertEqual(1 if no_use_bookmark else 0, n_dst)
                        self.assertEqual(3 + 3 - 1 if no_use_bookmark else 3 + 3, n_all)
                        lines = snapshot_list(job, "dst")
                        self.assertEqual(1 if no_use_bookmark else 0, len(lines))

                        src_foo = ibase.SRC_ROOT_DATASET + "/foo"
                        destroy(bookmarks(src_foo)[0])  # also delete bookmark now
                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--skip-replication",
                            "--compare-snapshot-lists",
                            "-r",
                            *xtra,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(3, n_src)
                        self.assertEqual(1, n_dst)
                        self.assertEqual(3 + 3 - 1, n_all)
                        lines = snapshot_list(job, "dst")
                        self.assertEqual(1, len(lines))
                        self.assertEqual("/foo@t1", lines[0].split("\t")[rel_name])
                        self.assertEqual(ibase.DST_ROOT_DATASET, lines[0].split("\t")[root_dataset])
                        self.assertEqual(ibase.DST_ROOT_DATASET + "/foo@t1", lines[0].split("\t")[name])

                        destroy_snapshots(src_foo, snapshots(src_foo))
                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--skip-replication",
                            "--compare-snapshot-lists",
                            "-r",
                            *xtra,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(3, n_src)
                        # With default --create-bookmarks=all (j==2), two bookmarks (#t2, #t3) remain on src after
                        # deleting all src snapshots of /foo (we already deleted #t1 above), so only one dst-only entry
                        # remains. For j<=1 (explicit --create-bookmarks=hourly), only the latest snapshot (#t3) is
                        # bookmarked, hence two dst-only entries remain. When --no-use-bookmark is set, bookmarks are
                        # ignored and all three appear only on dst.
                        self.assertEqual(3 if no_use_bookmark else 3 - (2 if j == 2 else 1), n_dst)
                        self.assertEqual(3 if no_use_bookmark else 3 + (2 if j == 2 else 1), n_all)

                        for bookmark in bookmarks(src_foo):
                            destroy(bookmark)
                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--skip-replication",
                            "--compare-snapshot-lists",
                            "-r",
                            *xtra,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(3, n_src)
                        self.assertEqual(3, n_dst)
                        self.assertEqual(3, n_all)

                        src_foo_a = ibase.SRC_ROOT_DATASET + "/foo/a"
                        dst_foo_a = ibase.DST_ROOT_DATASET + "/foo/a"
                        destroy(snapshots(src_foo_a)[-1])
                        destroy(snapshots(dst_foo_a)[-1])
                        job = self.run_bzfs(
                            ibase.SRC_ROOT_DATASET,
                            ibase.DST_ROOT_DATASET,
                            "--skip-replication",
                            "--compare-snapshot-lists",
                            "-r",
                            *xtra,
                        )
                        n_src, n_dst, n_all = stats(job)
                        self.assertEqual(3 if no_use_bookmark else 3 + 1, n_src)
                        self.assertEqual(3, n_dst)
                        self.assertEqual(3 - 1, n_all)
                    finally:
                        if old_value is None:
                            os.environ.pop(param_name, None)
                        else:
                            os.environ[param_name] = old_value

    def test_delete_dst_datasets_with_missing_src_root(self) -> None:
        destroy(ibase.SRC_ROOT_DATASET, recursive=True)
        recreate_filesystem(ibase.DST_ROOT_DATASET)
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--skip-replication",
                    "--delete-dst-datasets",
                    dry_run=(i == 0),
                )
                if i == 0:
                    self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
                else:
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

    def test_delete_dst_datasets_recursive_with_dummy_src(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        for i in range(3):
            with stop_on_failure_subtest(i=i):
                self.run_bzfs(
                    DUMMY_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--skip-replication",
                    "--delete-dst-datasets",
                    dry_run=(i == 0),
                )
                if i == 0:
                    self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
                else:
                    self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

    def test_delete_dst_datasets_recursive_with_non_included_dataset(self) -> None:
        dst_foo1 = create_filesystem(ibase.DST_ROOT_DATASET, "foo1")
        dst_foo1a = create_filesystem(dst_foo1, "a")
        self.assertIsNotNone(dst_foo1a)
        dst_foo2 = create_filesystem(ibase.DST_ROOT_DATASET, "foo2")
        dst_foo2a = create_filesystem(dst_foo2, "a")
        self.assertIsNotNone(dst_foo2a)
        self.run_bzfs(
            DUMMY_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-datasets",
            "--recursive",
            "--include-dataset-regex",
            "!.*foo1",
        )
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo1"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo1/a"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo2"))

    def test_delete_dst_datasets_flat_nothing_todo(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_filesystem(ibase.DST_ROOT_DATASET, "bar"), "b1")
        destroy(build(ibase.SRC_ROOT_DATASET + "/foo"), recursive=True)
        self.assertFalse(dataset_exists(ibase.SRC_ROOT_DATASET + "/foo"))
        self.assertTrue(dataset_exists(ibase.SRC_ROOT_DATASET))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--skip-replication", "--delete-dst-datasets")
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/bar"))

    def test_delete_dst_datasets_recursive1(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_filesystem(ibase.DST_ROOT_DATASET, "bar"), fix("b1"))
        take_snapshot(create_filesystem(ibase.DST_ROOT_DATASET, "zoo"), fix("z1"))
        destroy(build(ibase.SRC_ROOT_DATASET + "/foo"), recursive=True)
        self.assertFalse(dataset_exists(ibase.SRC_ROOT_DATASET + "/foo"))
        self.assertTrue(dataset_exists(ibase.SRC_ROOT_DATASET))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--recursive",
            "--skip-replication",
            "--delete-dst-datasets",
        )
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/bar"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/zoo"))

    def test_delete_dst_datasets_with_exclude_regex1(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_filesystem(ibase.DST_ROOT_DATASET, "bar"), fix("b1"))
        take_snapshot(create_filesystem(ibase.DST_ROOT_DATASET, "zoo"), fix("z1"))
        destroy(build(ibase.SRC_ROOT_DATASET + "/foo"), recursive=True)
        self.assertFalse(dataset_exists(ibase.SRC_ROOT_DATASET + "/foo"))
        self.assertTrue(dataset_exists(ibase.SRC_ROOT_DATASET))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--recursive",
            "--skip-replication",
            "--delete-dst-datasets",
            "--exclude-dataset-regex",
            "bar?",
            "--exclude-dataset-regex",
            "zoo*",
        )
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/bar"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/zoo"))

    def test_delete_dst_datasets_with_exclude_regex2(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_filesystem(ibase.DST_ROOT_DATASET, "bar"), fix("b1"))
        take_snapshot(create_filesystem(ibase.DST_ROOT_DATASET, "zoo"), fix("z1"))
        destroy(build(ibase.SRC_ROOT_DATASET + "/foo"), recursive=True)
        self.assertFalse(dataset_exists(ibase.SRC_ROOT_DATASET + "/foo"))
        self.assertTrue(dataset_exists(ibase.SRC_ROOT_DATASET))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--recursive",
            "--skip-replication",
            "--delete-dst-datasets",
            "--exclude-dataset-regex",
            "!bar",
        )
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/bar"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/zoo"))

    def test_delete_dst_datasets_with_exclude_dataset(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        take_snapshot(create_filesystem(ibase.DST_ROOT_DATASET, "bar"), fix("b1"))
        take_snapshot(create_filesystem(ibase.DST_ROOT_DATASET, "zoo"), fix("z1"))
        destroy(build(ibase.SRC_ROOT_DATASET + "/foo"), recursive=True)
        self.assertFalse(dataset_exists(ibase.SRC_ROOT_DATASET + "/foo"))
        self.assertTrue(dataset_exists(ibase.SRC_ROOT_DATASET))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--recursive",
            "--skip-replication",
            "--delete-dst-datasets",
            "--exclude-dataset",
            "foo",
            "--exclude-dataset",
            "zoo",
            "--exclude-dataset",
            "foo/b",
            "--exclude-dataset",
            "xxxxx",
        )
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/bar"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/zoo"))

    def test_delete_dst_datasets_and_empty_datasets(self) -> None:
        create_filesystems("axe")
        create_filesystems("foo/a")
        create_filesystems("foo/a/b")
        create_filesystems("foo/a/b/c")
        create_filesystems("foo/a/b/d")
        take_snapshot(create_filesystems("foo/a/e"), fix("e1"))
        create_filesystems("foo/b/c")
        create_filesystems("foo/b/c/d")
        create_filesystems("foo/b/d")
        take_snapshot(create_filesystems("foo/c"), fix("c1"))
        create_volumes("zoo")
        create_filesystems("boo")
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--recursive",
            "--skip-replication",
            "--delete-empty-dst-datasets",
            "--exclude-dataset",
            "boo",
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/axe"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a/b"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a/e"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/c"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/zoo"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/boo"))

        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--recursive",
            "--skip-replication",
            "--delete-dst-datasets",
            "--exclude-dataset",
            "boo",
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/axe"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a/e"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/c"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/zoo"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/boo"))

        self.run_bzfs(
            DUMMY_DATASET,
            ibase.DST_ROOT_DATASET,
            "--recursive",
            "--skip-replication",
            "--delete-dst-datasets",
            "--exclude-dataset",
            "boo",
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/axe"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/zoo"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/boo"))

    def test_delete_dst_snapshots_and_empty_datasets(self) -> None:
        create_filesystems("axe")
        create_filesystems("foo/a")
        create_filesystems("foo/a/b")
        create_filesystems("foo/a/b/c")
        create_filesystems("foo/a/b/d")
        take_snapshot(create_filesystems("foo/a/e"), fix("e1"))
        create_filesystems("foo/b/c")
        create_filesystems("foo/b/c/d")
        create_filesystems("foo/b/d")
        take_snapshot(create_filesystems("foo/c"), fix("c1"))
        create_volumes("zoo")
        create_filesystems("boo")
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--recursive",
            "--skip-replication",
            "--delete-empty-dst-datasets",
            "--delete-dst-snapshots",
            "--exclude-dataset",
            "boo",
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/axe"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a/b"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a/e"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/c"))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/zoo"))
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/boo"))

    def test_delete_empty_dst_datasets_with_excluded_children_having_snapshots(self) -> None:
        # Setup:
        # dst_root/parent       (policy-selected, no snapshots itself)
        # dst_root/parent/child_excluded (policy-excluded, HAS snapshots)
        # dst_root/parent/child_included (policy-selected, no snapshots)
        parent_ds = create_filesystem(ibase.DST_ROOT_DATASET, "parent")
        child_excluded_ds = create_filesystem(parent_ds, "child_excluded")
        child_included_ds = create_filesystem(parent_ds, "child_included")
        take_snapshot(child_excluded_ds, fix("snap_on_excluded"))

        # Check initial state
        self.assertTrue(dataset_exists(parent_ds))
        self.assertTrue(dataset_exists(child_excluded_ds))
        self.assertTrue(dataset_exists(child_included_ds))
        self.assertEqual(1, len(snapshots(child_excluded_ds)))

        # Run bzfs: delete empty datasets, exclude child_excluded from management
        # Source is dummy, so all selected datasets on dst are candidates for deletion if empty.
        self.run_bzfs(
            DUMMY_DATASET,
            ibase.DST_ROOT_DATASET,
            "--recursive",
            "--skip-replication",
            "--delete-empty-dst-datasets",  # The command under test
            "--include-dataset-regex=parent",  # Selects parent and parent/child_included
            "--include-dataset-regex=parent/child_included",  # Explicitly
            "--exclude-dataset-regex=parent/child_excluded",  # Exclude this one
        )

        # Expected: parent_ds should NOT be deleted because its child_excluded (though not managed) has a snapshot.
        self.assertTrue(
            dataset_exists(parent_ds), "Parent dataset should not be deleted as its excluded child has snapshots"
        )
        self.assertTrue(dataset_exists(child_excluded_ds), "Excluded child with snapshot should remain")
        self.assertFalse(dataset_exists(child_included_ds), "Included empty child should be deleted")

    def test_delete_dst_snapshots_nothing_todo(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        self.assertTrue(dataset_exists(ibase.SRC_ROOT_DATASET + "/foo/b"))
        self.assertEqual(0, len(snapshots(build(ibase.SRC_ROOT_DATASET + "/foo/b"))))
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET + "/foo/b",
            ibase.DST_ROOT_DATASET + "/foo/b",
            "--skip-replication",
            "--delete-dst-snapshots",
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/b"))

    def test_delete_dst_bookmarks_flat_with_replication(self) -> None:
        self.setup_basic()
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--delete-dst-snapshots=bookmarks")
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
        if are_bookmarks_enabled("src") and not self.is_no_privilege_elevation():
            self.run_bzfs(DUMMY_DATASET, ibase.SRC_ROOT_DATASET, "--skip-replication", "--delete-dst-snapshots=bookmarks")
            self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, [])

    def test_delete_dst_snapshots_flat_with_replication_with_crosscheck(self) -> None:
        self.setup_basic()
        xtra = ["--create-bookmarks=hourly"]
        for j in range(2):
            for i in range(3):
                with stop_on_failure_subtest(i=j * 3 + i):
                    destroy_snapshots(ibase.SRC_ROOT_DATASET, snapshots(ibase.SRC_ROOT_DATASET))
                    destroy_snapshots(ibase.DST_ROOT_DATASET, snapshots(ibase.DST_ROOT_DATASET))
                    if are_bookmarks_enabled("src"):
                        for bookmark in bookmarks(ibase.SRC_ROOT_DATASET):
                            destroy(bookmark)
                    take_snapshot(ibase.SRC_ROOT_DATASET, fix("s1"))
                    take_snapshot(ibase.SRC_ROOT_DATASET, fix("s2"))
                    take_snapshot(ibase.SRC_ROOT_DATASET, fix("s3"))
                    self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, *xtra)
                    self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
                    if i > 0:
                        destroy_snapshots(ibase.SRC_ROOT_DATASET, snapshots(ibase.SRC_ROOT_DATASET))
                    if i > 1 and are_bookmarks_enabled("src"):
                        for bookmark in bookmarks(ibase.SRC_ROOT_DATASET):
                            destroy(bookmark)
                    crosscheck = [] if j == 0 else ["--delete-dst-snapshots-no-crosscheck"]
                    self.run_bzfs(
                        ibase.SRC_ROOT_DATASET,
                        ibase.DST_ROOT_DATASET,
                        "--skip-replication",
                        "--delete-dst-snapshots",
                        *crosscheck,
                        *xtra,
                    )
                    if i == 0:
                        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
                    elif i == 1:
                        if j == 0 and are_bookmarks_enabled("src"):
                            self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s1", "s3"])
                        else:
                            self.assert_snapshot_names(ibase.DST_ROOT_DATASET, [])
                    else:
                        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, [])

    def test_delete_dst_snapshots_flat(self) -> None:
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                self.setup_basic_with_recursive_replication_done()
                destroy(snapshots(ibase.SRC_ROOT_DATASET)[2])
                destroy(snapshots(ibase.SRC_ROOT_DATASET)[0])
                src_foo = build(ibase.SRC_ROOT_DATASET + "/foo")
                destroy(snapshots(src_foo)[1])
                src_foo_a = build(ibase.SRC_ROOT_DATASET + "/foo/a")
                destroy(snapshots(src_foo_a)[2])
                max_bytes = 1 if i != 0 else None
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--skip-replication",
                    "--delete-dst-snapshots",
                    "--delete-dst-snapshots-no-crosscheck",
                    max_command_line_bytes=max_bytes,
                )
                self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s2"])
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")

        if are_bookmarks_enabled("src"):
            tag0 = snapshot_name(snapshots(ibase.SRC_ROOT_DATASET)[0])
            create_bookmark(ibase.DST_ROOT_DATASET, tag0, tag0)
            for snap in snapshots(ibase.SRC_ROOT_DATASET, max_depth=None) + snapshots(
                ibase.DST_ROOT_DATASET, max_depth=None
            ):
                destroy(snap)
            self.run_bzfs(
                ibase.SRC_ROOT_DATASET,
                ibase.DST_ROOT_DATASET,
                "--skip-replication",
                "--delete-empty-dst-datasets",
                "--recursive",
            )
            self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))
        else:
            for snap in snapshots(ibase.SRC_ROOT_DATASET, max_depth=None) + snapshots(
                ibase.DST_ROOT_DATASET, max_depth=None
            ):
                destroy(snap)

        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--delete-empty-dst-datasets=snapshots",
            "--recursive",
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

    def test_delete_dst_snapshots_skips_held_snapshots(self) -> None:
        """Ensures destination snapshots that carry a `zfs hold` are skipped by --delete-dst-snapshots."""
        unaffected_snapshot = take_snapshot(ibase.SRC_ROOT_DATASET, "s2")
        held_snapshot = take_snapshot(ibase.SRC_ROOT_DATASET, "dstonly_hold")
        nonheld_snapshot = take_snapshot(ibase.SRC_ROOT_DATASET, "dstonly_nohold")
        self.assertIn(unaffected_snapshot, snapshots(ibase.SRC_ROOT_DATASET))
        self.assertIn(held_snapshot, snapshots(ibase.SRC_ROOT_DATASET))
        self.assertIn(nonheld_snapshot, snapshots(ibase.SRC_ROOT_DATASET))

        hold_tag = "myhold"
        self.assertEqual("0", snapshot_property(held_snapshot, "userrefs"))
        run_cmd(SUDO_CMD + ["zfs", "hold", hold_tag, held_snapshot])
        self.assertEqual("1", snapshot_property(held_snapshot, "userrefs"))

        try:
            self.run_bzfs(
                DUMMY_DATASET,
                ibase.SRC_ROOT_DATASET,
                "--skip-replication",
                "--delete-dst-snapshots",
                "--include-snapshot-regex=dstonly_.*",
                delete_injection_triggers={},
            )
            self.assertIn(unaffected_snapshot, snapshots(ibase.SRC_ROOT_DATASET))
            self.assertIn(held_snapshot, snapshots(ibase.SRC_ROOT_DATASET))
            self.assertNotIn(nonheld_snapshot, snapshots(ibase.SRC_ROOT_DATASET))
        finally:
            if held_snapshot in snapshots(ibase.SRC_ROOT_DATASET):
                run_cmd(SUDO_CMD + ["zfs", "release", hold_tag, held_snapshot])
                self.assertEqual("0", snapshot_property(held_snapshot, "userrefs"))

    def test_delete_dst_bookmarks_flat(self) -> None:
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        xtra = ["--create-bookmarks=hourly"]
        for i in range(1):
            with stop_on_failure_subtest(i=i):
                if i > 0:
                    self.tearDownAndSetup()
                self.setup_basic()
                self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, *xtra)
                tag0 = snapshot_name(snapshots(ibase.SRC_ROOT_DATASET)[0])
                create_bookmark(ibase.DST_ROOT_DATASET, tag0, tag0)
                tag1 = snapshot_name(snapshots(ibase.SRC_ROOT_DATASET)[1])
                create_bookmark(ibase.DST_ROOT_DATASET, tag1, tag1)
                tag2 = snapshot_name(snapshots(ibase.SRC_ROOT_DATASET)[2])
                create_bookmark(ibase.DST_ROOT_DATASET, tag2, tag2)
                self.assert_bookmark_names(ibase.SRC_ROOT_DATASET, ["s1", "s3"])
                self.assert_bookmark_names(ibase.DST_ROOT_DATASET, ["s1", "s2", "s3"])
                for snapshot in snapshots(ibase.SRC_ROOT_DATASET) + snapshots(ibase.DST_ROOT_DATASET):
                    destroy(snapshot)
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--skip-replication",
                    "--delete-dst-snapshots=bookmarks",
                    *xtra,
                )
                self.assert_bookmark_names(ibase.DST_ROOT_DATASET, ["s1", "s3"])

                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--skip-replication",
                    "--delete-dst-snapshots=bookmarks",
                    *xtra,
                )
                self.assert_bookmark_names(ibase.DST_ROOT_DATASET, ["s1", "s3"])

                for bookmark in bookmarks(ibase.SRC_ROOT_DATASET):
                    destroy(bookmark)
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--skip-replication",
                    "--delete-dst-snapshots=bookmarks",
                    "--delete-empty-dst-datasets",
                    *xtra,
                )
                self.assert_bookmark_names(ibase.DST_ROOT_DATASET, [])
                self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))

                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    "--skip-replication",
                    "--delete-empty-dst-datasets",
                    "--recursive",
                    *xtra,
                )
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

    def test_delete_dst_snapshots_despite_same_name(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        destroy(snapshots(ibase.SRC_ROOT_DATASET)[2])
        destroy(snapshots(ibase.SRC_ROOT_DATASET)[0])
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("s1"))  # Note: not the same as prior snapshot (has different GUID)
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("s3"))  # Note: not the same as prior snapshot (has different GUID)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
        )
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s2"])

    def test_delete_dst_snapshots_recursive(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        destroy(snapshots(ibase.SRC_ROOT_DATASET)[2])
        destroy(snapshots(ibase.SRC_ROOT_DATASET)[0])
        src_foo = build(ibase.SRC_ROOT_DATASET + "/foo")
        destroy(snapshots(src_foo)[1])
        src_foo_a = build(ibase.SRC_ROOT_DATASET + "/foo/a")
        destroy(src_foo_a, recursive=True)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--recursive",
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
        )
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s2"])
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET + "/foo", ["t1", "t3"])
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET + "/foo/a", [])

    def test_delete_dst_snapshots_recursive_with_delete_empty_dst_datasets(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        destroy(snapshots(ibase.SRC_ROOT_DATASET)[2])
        destroy(snapshots(ibase.SRC_ROOT_DATASET)[0])
        src_foo = build(ibase.SRC_ROOT_DATASET + "/foo")
        destroy(snapshots(src_foo)[1])
        src_foo_a = build(ibase.SRC_ROOT_DATASET + "/foo/a")
        destroy(src_foo_a, recursive=True)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--recursive",
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--delete-empty-dst-datasets",
        )
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s2"])
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET + "/foo", ["t1", "t3"])
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo/a"))

    def test_delete_dst_snapshots_recursive_with_dummy(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        for i in range(2):
            self.run_bzfs(
                DUMMY_DATASET,
                ibase.DST_ROOT_DATASET,
                "--recursive",
                "--skip-replication",
                "--delete-dst-snapshots",
                dry_run=(i == 0),
            )
            if i == 0:
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
            else:
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 0)
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 0)
                self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 0)

    def test_delete_dst_snapshots_recursive_with_delete_empty_dst_datasets_with_dummy(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        for i in range(3):
            self.run_bzfs(
                DUMMY_DATASET,
                ibase.DST_ROOT_DATASET,
                "--recursive",
                "--skip-replication",
                "--delete-dst-snapshots",
                "--delete-dst-snapshots-no-crosscheck",
                "--delete-empty-dst-datasets",
                dry_run=(i == 0),
            )
            if i == 0:
                self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
            else:
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

    def test_delete_dst_snapshots_flat_with_nonexisting_destination(self) -> None:
        self.setup_basic()
        destroy(ibase.DST_ROOT_DATASET, recursive=True)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
        )
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

    def test_delete_dst_snapshots_recursive_with_injected_errors(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))

        # inject send failures before this many tries. only after that succeed the operation
        counter = Counter(zfs_delete_snapshot=2)
        self.run_bzfs(
            DUMMY_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--recursive",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            retries=2,
            error_injection_triggers={"before": counter},
        )
        self.assertEqual(0, counter["zfs_delete_snapshot"])
        self.assertEqual(0, len(snapshots(ibase.DST_ROOT_DATASET, max_depth=None)))

    def test_delete_dst_snapshots_recursive_with_injected_dataset_deletes(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        self.assertTrue(dataset_exists(ibase.DST_ROOT_DATASET))

        # inject deletes for this many times. only after that stop deleting datasets
        counter = Counter(zfs_list_delete_dst_snapshots=1)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--recursive",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            delete_injection_triggers={"before": counter},
        )
        self.assertEqual(0, counter["zfs_list_delete_dst_snapshots"])
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

    def test_rollback_dst_snapshots_flat_with_injected_recv_token1(self) -> None:
        if not is_zpool_recv_resume_feature_enabled_or_active():
            self.skipTest("No recv resume zfs feature is available")
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("s1"))
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("s2"))
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--no-use-bookmark", no_create_bookmark=True)
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 2, "s")
        self.create_resumable_snapshots(3, 4)
        self.generate_recv_resume_token(
            ibase.SRC_ROOT_DATASET + "@s2", ibase.SRC_ROOT_DATASET + "@s3", ibase.DST_ROOT_DATASET
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 2, "s")
        destroy_snapshots(ibase.SRC_ROOT_DATASET, snapshots(ibase.SRC_ROOT_DATASET)[1:2])
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--force-rollback-to-latest-common-snapshot",
            "--no-use-bookmark",
            "--create-bookmarks=none",
            retries=1,
        )
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s1", "s3"])

    def test_delete_dst_snapshots_flat_with_injected_recv_token1(self) -> None:
        if not is_zpool_recv_resume_feature_enabled_or_active():
            self.skipTest("No recv resume zfs feature is available")
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("s1"))
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 1, "s")
        self.create_resumable_snapshots(2, 3)
        self.generate_recv_resume_token(
            ibase.SRC_ROOT_DATASET + "@s1", ibase.SRC_ROOT_DATASET + "@s2", ibase.DST_ROOT_DATASET
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 1, "s")
        self.run_bzfs(
            "dummy",
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            retries=1,
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 0, "s")
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 2, "s")

    def test_delete_dst_snapshots_flat_with_injected_recv_token2(self) -> None:
        if not is_zpool_recv_resume_feature_enabled_or_active():
            self.skipTest("No recv resume zfs feature is available")
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("s1"))
        take_snapshot(ibase.SRC_ROOT_DATASET, fix("s2"))
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 2, "s")
        self.create_resumable_snapshots(3, 5)
        self.generate_recv_resume_token(
            ibase.SRC_ROOT_DATASET + "@s2", ibase.SRC_ROOT_DATASET + "@s3", ibase.DST_ROOT_DATASET
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 2, "s")
        self.run_bzfs(
            "dummy",
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--include-snapshot-regex=s2",
            retries=1,
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 1, "s")
        self.run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET)
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 4, "s")

    def test_delete_dst_snapshots_flat_with_time_range_full(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        destroy(snapshots(ibase.SRC_ROOT_DATASET)[2])
        destroy(snapshots(ibase.SRC_ROOT_DATASET)[0])
        src_foo = build(ibase.SRC_ROOT_DATASET + "/foo")
        destroy(snapshots(src_foo)[1])
        src_foo_a = build(ibase.SRC_ROOT_DATASET + "/foo/a")
        destroy(snapshots(src_foo_a)[2])
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--include-snapshot-times-and-ranks=60secs ago..2999-01-01",
        )
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s2"])
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")

    def test_delete_dst_snapshots_flat_with_time_range_empty(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        destroy(snapshots(ibase.SRC_ROOT_DATASET)[2])
        destroy(snapshots(ibase.SRC_ROOT_DATASET)[0])
        src_foo = build(ibase.SRC_ROOT_DATASET + "/foo")
        destroy(snapshots(src_foo)[1])
        src_foo_a = build(ibase.SRC_ROOT_DATASET + "/foo/a")
        destroy(snapshots(src_foo_a)[2])
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--include-snapshot-times-and-ranks=2999-01-01..2999-01-01",
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")

    def test_delete_dst_snapshots_with_excludes_flat_nothing_todo(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--exclude-snapshot-regex",
            "xxxx*",
        )
        self.assert_snapshots(ibase.DST_ROOT_DATASET, 3, "s")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")

    def test_delete_dst_snapshots_with_excludes_flat(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        for snap in snapshots(ibase.SRC_ROOT_DATASET):
            destroy(snap)
        for snap in snapshots(build(ibase.SRC_ROOT_DATASET + "/foo")):
            destroy(snap)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--exclude-snapshot-regex",
            r"!.*s[1-2]+.*",
        )
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s3"])
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")

    def test_delete_dst_snapshots_with_excludes_recursive(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        for snap in snapshots(ibase.SRC_ROOT_DATASET):
            destroy(snap)
        for snap in snapshots(build(ibase.SRC_ROOT_DATASET + "/foo")):
            destroy(snap)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--recursive",
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--delete-empty-dst-datasets",
            "--exclude-snapshot-regex",
            ".*s[1-2]+.*",
            "--exclude-snapshot-regex",
            ".*t1.*",
            "--exclude-snapshot-regex",
            ".*u.*",
        )
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s1", "s2"])
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET + "/foo", ["t1"])
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET + "/foo/a", ["u1", "u2", "u3"])

    def test_delete_dst_snapshots_with_excludes_recursive_and_excluding_dataset_regex(self) -> None:
        self.setup_basic_with_recursive_replication_done()
        for snap in snapshots(ibase.SRC_ROOT_DATASET):
            destroy(snap)
        for snap in snapshots(build(ibase.SRC_ROOT_DATASET + "/foo")):
            destroy(snap)
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--recursive",
            "--skip-replication",
            "--delete-dst-snapshots",
            "--delete-dst-snapshots-no-crosscheck",
            "--delete-empty-dst-datasets",
            "--exclude-dataset-regex",
            "foo",
            "--exclude-snapshot-regex",
            ".*s[1-2]+.*",
            "--exclude-snapshot-regex",
            ".*t1.*",
            "--exclude-snapshot-regex",
            ".*u1.*",
        )
        self.assert_snapshot_names(ibase.DST_ROOT_DATASET, ["s1", "s2"])
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo", 3, "t")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo/a", 3, "u")

    def test_syslog(self) -> None:
        syslog_path = "/var/log/syslog"
        if "Ubuntu" not in platform.version() or not os.path.exists(syslog_path) or not os.access(syslog_path, os.R_OK):
            self.skipTest("It is sufficient to only test this on Ubuntu where syslog paths are well known")
        for i in range(2):
            if i > 0:
                self.tearDownAndSetup()
            with stop_on_failure_subtest(i=i):
                syslog_prefix = "bzfs_backup"
                verbose = ["-v"] if i == 0 else []
                self.run_bzfs(
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    *verbose,
                    "--log-syslog-address=/dev/log",
                    "--log-syslog-socktype=UDP",
                    "--log-syslog-facility=2",
                    "--log-syslog-level=TRACE",
                    "--log-syslog-prefix=" + syslog_prefix,
                    "--skip-replication",
                )
                lines = list(utils.tail(syslog_path, n=100, errors="surrogateescape"))
                k = -1
                for kk, line in enumerate(lines):
                    if syslog_prefix in line and "Log file is:" in line:
                        k = kk
                self.assertGreaterEqual(k, 0)
                lines = lines[k:]
                found_msg = False
                for line in lines:
                    if syslog_prefix in line:
                        if i == 0:
                            found_msg = found_msg or " [T] " in line
                        else:
                            found_msg = found_msg or " [D] " in line
                            self.assertNotIn(" [T] ", line)
                self.assertTrue(found_msg, "No bzfs syslog message was found")

    def test_main(self) -> None:
        self.setup_basic()
        with self.assertRaises(SystemExit):
            bzfs.main()
        with self.assertRaises(SystemExit):
            bzfs.run_main(bzfs.argument_parser().parse_args(["xxxx", ibase.DST_ROOT_DATASET]))
        bzfs.run_main(bzfs.argument_parser().parse_args([ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET]))

    def test_ssh_program_must_not_be_disabled_in_nonlocal_mode(self) -> None:
        if not self.param or self.param.get("ssh_mode", "local") == "local" or SSH_PROGRAM != "ssh":
            self.skipTest("ssh is only required in nonlocal mode")
        self.run_bzfs(
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            "--ssh-program=" + bzfs_main.detect.DISABLE_PRG,
            expected_status=DIE_STATUS,
        )

    def test_cache_flat_simple(self) -> None:
        if not is_cache_snapshots_enabled():
            self.skipTest("Cache not supported")

        def _run_bzfs(*args: str, **kwargs: Any) -> bzfs.Job:
            return self.run_bzfs(*args, **kwargs, cache_snapshots=True, force_stable_cache_namespace=True)

        delay_secs = 2 * MATURITY_TIME_THRESHOLD_SECS
        create_filesystem(ibase.SRC_ROOT_DATASET, "foo1")
        take_snapshot(ibase.SRC_ROOT_DATASET + "/foo1", fix("s1"))

        job = _run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "--skip-parent", dry_run=True)
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo1"))
        self.assertEqual(0, job.num_cache_hits)
        self.assertEqual(1, job.num_cache_misses)

        job = _run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "--skip-parent")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo1", 1, "s")
        self.assertEqual(0, job.num_cache_hits)
        self.assertEqual(1, job.num_cache_misses)

        time.sleep(delay_secs)
        job = _run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "--skip-parent")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo1", 1, "s")
        self.assertEqual(1, job.num_cache_hits)
        self.assertEqual(0, job.num_cache_misses)

        create_filesystem(ibase.SRC_ROOT_DATASET, "foo2")
        take_snapshot(ibase.SRC_ROOT_DATASET + "/foo2", fix("s1"))
        time.sleep(delay_secs)
        job = _run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "--skip-parent")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo2", 1, "s")
        self.assertEqual(1, job.num_cache_hits)
        self.assertEqual(1, job.num_cache_misses)

        time.sleep(delay_secs)
        job = _run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "--skip-parent")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo2", 1, "s")
        self.assertEqual(2, job.num_cache_hits)
        self.assertEqual(0, job.num_cache_misses)

        take_snapshot(ibase.SRC_ROOT_DATASET + "/foo1", fix("s2"))
        time.sleep(delay_secs)
        job = _run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "--skip-parent")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo1", 2, "s")
        self.assertEqual(1, job.num_cache_hits)
        self.assertEqual(1, job.num_cache_misses)

        time.sleep(delay_secs)
        job = _run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "--skip-parent")
        self.assertEqual(2, job.num_cache_hits)
        self.assertEqual(0, job.num_cache_misses)

        destroy_snapshots(ibase.DST_ROOT_DATASET + "/foo1", snapshots(ibase.DST_ROOT_DATASET + "/foo1")[1:])
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo1", 1, "s")
        time.sleep(delay_secs)
        job = _run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "--skip-parent")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo1", 2, "s")
        self.assertEqual(1, job.num_cache_hits)
        self.assertEqual(1, job.num_cache_misses)

        time.sleep(delay_secs)
        job = _run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "--skip-parent")
        self.assertEqual(2, job.num_cache_hits)
        self.assertEqual(0, job.num_cache_misses)

    def test_cache_flat_simple_subset(self) -> None:
        if not is_cache_snapshots_enabled():
            self.skipTest("Cache not supported")

        def _run_bzfs(*args: str, **kwargs: Any) -> bzfs.Job:
            return self.run_bzfs(*args, **kwargs, cache_snapshots=True, force_stable_cache_namespace=True)

        delay_secs = 2 * MATURITY_TIME_THRESHOLD_SECS
        create_filesystem(ibase.SRC_ROOT_DATASET, "foo1")
        take_snapshot(ibase.SRC_ROOT_DATASET + "/foo1", fix("s1"))

        create_filesystem(ibase.SRC_ROOT_DATASET, "foo2")
        take_snapshot(ibase.SRC_ROOT_DATASET + "/foo2", fix("s1"))
        job = _run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "--skip-parent")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo1", 1, "s")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo2", 1, "s")
        self.assertEqual(0, job.num_cache_hits)
        self.assertEqual(2, job.num_cache_misses)

        time.sleep(delay_secs)
        job = _run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "--skip-parent")
        self.assertEqual(2, job.num_cache_hits)
        self.assertEqual(0, job.num_cache_misses)

        destroy(ibase.DST_ROOT_DATASET + "/foo2", recursive=True)
        self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET + "/foo2"))
        time.sleep(delay_secs)
        job = _run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "--skip-parent")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo1", 1, "s")
        self.assert_snapshots(ibase.DST_ROOT_DATASET + "/foo2", 1, "s")
        self.assertEqual(1, job.num_cache_hits)
        self.assertEqual(1, job.num_cache_misses)

        time.sleep(delay_secs)
        job = _run_bzfs(ibase.SRC_ROOT_DATASET, ibase.DST_ROOT_DATASET, "--recursive", "--skip-parent")
        self.assertEqual(2, job.num_cache_hits)
        self.assertEqual(0, job.num_cache_misses)

    def test_jobrunner_flat_simple(self) -> None:

        def run_jobrunner(*args: str, **kwargs: Any) -> bzfs_jobrunner.Job:
            return self.run_bzfs_jobrunner(
                *args,
                **kwargs,
                spawn_process_per_job=spawn_process_per_job,
                cache_snapshots=cache_snapshots,
            )

        if self.is_no_privilege_elevation():
            self.skipTest("Destroying snapshots on src needs extra permissions")
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        for jobiter, spawn_process_per_job in enumerate([None, True]):
            cache_snapshots = spawn_process_per_job is None
            with stop_on_failure_subtest(i=jobiter):
                self.tearDownAndSetup()
                self.assert_snapshots(ibase.SRC_ROOT_DATASET, 0)
                loopback = "127.0.0.2" if platform.system() == "Linux" else "127.0.0.1"
                delay_secs = MATURITY_TIME_THRESHOLD_SECS
                localhostname = socket.gethostname()
                src_hosts = [localhostname]  # for local mode (no ssh, no network)
                dst_hosts_pull = {localhostname: ["", "onsite"]}
                dst_hosts_pull_bad = {localhostname: ["xxxxonsite"]}
                dst_hosts_push = {loopback: ["onsite"], "127.0.0.1": ["onsite"]}
                dst_hosts_push_bad = {loopback: ["xxxonsite"]}
                dst_root_datasets = {localhostname: "", loopback: "", "127.0.0.1": ""}
                retain_dst_targets = dst_hosts_pull.copy()
                retain_dst_targets.update(dst_hosts_push)
                src_snapshot_plan = {"z": {"onsite": {"millisecondly": 1, "daily": 0}}}
                dst_snapshot_plan = {"z": {"onsite": {"millisecondly": 1, "daily": 0}, "offsite": {}}}
                src_bookmark_plan = dst_snapshot_plan
                monitor_dst_snapshot_plan = {
                    "z": {
                        "onsite": {
                            "millisecondly": {
                                "warning": "1 hours",
                                "critical": "2 hours",
                                "src_snapshot_cycles": 1,
                                "dst_snapshot_cycles": 1,
                            }
                        },
                        "offsite": {"millisecondly": {"warning": "1 hours", "critical": "2 hours"}},
                    }
                }
                monitor_src_snapshot_plan = {
                    "z": {"onsite": {"millisecondly": {"warning": "1 hours", "critical": "2 hours"}}}
                }
                args = [
                    "--root-dataset-pairs",
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    f"--src-hosts={src_hosts}",
                    f"--localhost={localhostname}",
                    f"--retain-dst-targets={retain_dst_targets}",
                    f"--dst-root-datasets={dst_root_datasets}",
                    f"--src-snapshot-plan={src_snapshot_plan}",
                    f"--src-bookmark-plan={src_bookmark_plan}",
                    f"--dst-snapshot-plan={dst_snapshot_plan}",
                    f"--monitor-snapshot-plan={monitor_dst_snapshot_plan}",
                    "--create-src-snapshots-timeformat=%Y-%m-%d_%H:%M:%S.%f",
                    "--job-id=myjobid",
                    "--jobrunner-log-level=TRACE",
                    f"--ssh-src-port={getenv_any('test_ssh_port', '22')}",
                    f"--ssh-dst-port={getenv_any('test_ssh_port', '22')}",
                ]
                pull_args = args + [f"--dst-hosts={dst_hosts_pull}"]
                pull_args_bad = args + [f"--dst-hosts={dst_hosts_pull_bad}"]
                push_args = args + [f"--dst-hosts={dst_hosts_push}"]
                push_args_bad = args + [f"--dst-hosts={dst_hosts_push_bad}"]

                # next iteration: create a src snapshot
                run_jobrunner("--create-src-snapshots", *pull_args)
                self.assertEqual(1, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(0, len(bookmarks(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(0, len(snapshots(ibase.DST_ROOT_DATASET)))

                # monitoring says latest src snapshots aren't too old:
                time.sleep(delay_secs)
                pull_args_no_monitoring = [arg for arg in pull_args if not arg.startswith("--monitor-snapshot-plan=")]
                run_jobrunner(
                    "--monitor-src-snapshots",
                    f"--monitor-snapshot-plan={monitor_src_snapshot_plan}",
                    *pull_args_no_monitoring,
                )
                run_jobrunner(
                    "--monitor-src-snapshots",
                    *[f"--src-host={host}" for host in src_hosts + src_hosts],
                    *[f"--dst-host={host}" for host in src_hosts + src_hosts],
                    f"--monitor-snapshot-plan={monitor_src_snapshot_plan}",
                    *pull_args_no_monitoring,
                )

                # monitoring says critical as there is no latest dst snapshot:
                self.assertEqual(0, len(snapshots(ibase.DST_ROOT_DATASET)))
                pull_args_no_monitoring = [arg for arg in pull_args if not arg.startswith("--monitor-snapshot-plan=")]
                monitor_dst_snapshot_plan = {"z": {"onsite": {"millisecondly": {"critical": "60 seconds"}}}}
                run_jobrunner(
                    "--monitor-snapshots-no-oldest-check",
                    "--monitor-dst-snapshots",
                    f"--monitor-snapshot-plan={monitor_dst_snapshot_plan}",
                    *pull_args_no_monitoring,
                    expected_status=bzfs.CRITICAL_STATUS,
                )

                # monitoring says critical as there is no oldest dst snapshot:
                self.assertEqual(0, len(snapshots(ibase.DST_ROOT_DATASET)))
                pull_args_no_monitoring = [arg for arg in pull_args if not arg.startswith("--monitor-snapshot-plan=")]
                monitor_dst_snapshot_plan = {"z": {"onsite": {"millisecondly": {"critical": "60 seconds"}}}}
                run_jobrunner(
                    "--monitor-snapshots-no-latest-check",
                    "--monitor-dst-snapshots",
                    f"--monitor-snapshot-plan={monitor_dst_snapshot_plan}",
                    *pull_args_no_monitoring,
                    expected_status=bzfs.CRITICAL_STATUS,
                )

                # monitoring says critical (but with zero exit code b/c of dont-crit) as there is no dst snapshot:
                self.assertEqual(0, len(snapshots(ibase.DST_ROOT_DATASET)))
                pull_args_no_monitoring = [arg for arg in pull_args if not arg.startswith("--monitor-snapshot-plan=")]
                monitor_dst_snapshot_plan = {"z": {"onsite": {"millisecondly": {"critical": "60 seconds"}}}}
                run_jobrunner(
                    "--monitor-dst-snapshots",
                    "--monitor-snapshots-dont-crit",
                    f"--monitor-snapshot-plan={monitor_dst_snapshot_plan}",
                    *pull_args_no_monitoring,
                )

                # replicate from src to dst:
                run_jobrunner("--replicate", *pull_args)
                self.assertEqual(1, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(bookmarks(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

                # no snapshots to prune yet on src:
                run_jobrunner("--prune-src-snapshots", *pull_args)
                self.assertEqual(1, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

                # no bookmarks to prune yet on src:
                run_jobrunner("--prune-src-bookmarks", *pull_args)
                self.assertEqual(1, len(bookmarks(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

                # no snapshots to prune yet on dst:
                run_jobrunner("--prune-dst-snapshots", *pull_args)
                self.assertEqual(1, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

                # monitoring says latest dst snapshots aren't too old:
                run_jobrunner("--monitor-dst-snapshots", *pull_args)
                run_jobrunner("--quiet", "--monitor-dst-snapshots", *pull_args)

                # next iteration: create another src snapshot
                run_jobrunner("--create-src-snapshots", *pull_args)
                self.assertEqual(2, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

                # replication to nonexistingpool target does nothing:
                run_jobrunner("--replicate", *pull_args_bad)
                self.assertEqual(2, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(bookmarks(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

                # replication to nonexistingpool destination pool does nothing:
                run_jobrunner(
                    "--replicate",
                    "--root-dataset-pairs",
                    *([ibase.SRC_ROOT_DATASET, "nonexistingpool/" + ibase.DST_ROOT_DATASET] + pull_args[3:]),
                )
                self.assertEqual(2, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(bookmarks(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

                # monitoring says latest dst snapshot is critically too old:
                pull_args_no_monitoring = [arg for arg in pull_args if not arg.startswith("--monitor-snapshot-plan=")]
                monitor_dst_snapshot_plan = {"z": {"onsite": {"millisecondly": {"critical": "1 millis"}}}}
                run_jobrunner(
                    "--monitor-snapshots-no-oldest-check",
                    "--monitor-dst-snapshots",
                    f"--monitor-snapshot-plan={monitor_dst_snapshot_plan}",
                    *pull_args_no_monitoring,
                    expected_status=bzfs.CRITICAL_STATUS,
                )

                # monitoring says oldest dst snapshot is critically too old:
                pull_args_no_monitoring = [arg for arg in pull_args if not arg.startswith("--monitor-snapshot-plan=")]
                monitor_dst_snapshot_plan = {"z": {"onsite": {"millisecondly": {"critical": "1 millis"}}}}
                run_jobrunner(
                    "--monitor-snapshots-no-latest-check",
                    "--monitor-dst-snapshots",
                    f"--monitor-snapshot-plan={monitor_dst_snapshot_plan}",
                    *pull_args_no_monitoring,
                    expected_status=bzfs.CRITICAL_STATUS,
                )

                # monitoring says latest dst snap is critically too old, but we only inform about this rather than error out:
                run_jobrunner(
                    "--monitor-snapshots-dont-crit",
                    "--monitor-dst-snapshots",
                    f"--monitor-snapshot-plan={monitor_dst_snapshot_plan}",
                    *pull_args_no_monitoring,
                )

                # monitoring says latest dst snapshot is warning too old:
                monitor_dst_snapshot_plan = {"z": {"onsite": {"millisecondly": {"warning": "1 millis"}}}}
                run_jobrunner(
                    "--monitor-dst-snapshots",
                    f"--monitor-snapshot-plan={monitor_dst_snapshot_plan}",
                    *pull_args_no_monitoring,
                    expected_status=bzfs.WARNING_STATUS,
                )

                # monitoring says latest dst snap is warning too old, but we only inform about this rather than error out:
                run_jobrunner(
                    "--monitor-snapshots-dont-warn",
                    "--monitor-dst-snapshots",
                    f"--monitor-snapshot-plan={monitor_dst_snapshot_plan}",
                    *pull_args_no_monitoring,
                )

                # monitoring freshness on nonexistingpool destination pool does nothing:
                pull_args_no_monitoring = [arg for arg in pull_args if not arg.startswith("--monitor-snapshot-plan=")]
                monitor_dst_snapshot_plan = {
                    "z": {"onsite": {"millisecondly": {"warning": "1 millis", "critical": "2 millis"}}}
                }
                run_jobrunner(
                    "--monitor-dst-snapshots",
                    f"--monitor-snapshot-plan={monitor_dst_snapshot_plan}",
                    "--root-dataset-pairs",
                    *([ibase.SRC_ROOT_DATASET, "nonexistingpool/" + ibase.DST_ROOT_DATASET] + pull_args_no_monitoring[3:]),
                )

                # replicate new snapshot from src to dst:
                nonexisting_snap = "s1_nonexisting_2024-01-01_00:00:00_secondly"
                take_snapshot(ibase.SRC_ROOT_DATASET, nonexisting_snap)
                run_jobrunner("--replicate", *pull_args)
                self.assertEqual(3, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(2, len(bookmarks(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(2, len(snapshots(ibase.DST_ROOT_DATASET)))
                destroy_snapshots(
                    ibase.SRC_ROOT_DATASET, [s for s in snapshots(ibase.SRC_ROOT_DATASET) if nonexisting_snap in s]
                )
                destroy_snapshots(
                    ibase.DST_ROOT_DATASET, [s for s in snapshots(ibase.DST_ROOT_DATASET) if nonexisting_snap in s]
                )

                # delete one old snapshot on src:
                run_jobrunner("--prune-src-snapshots", *pull_args)
                self.assertEqual(1, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(2, len(snapshots(ibase.DST_ROOT_DATASET)))

                # delete one old bookmark on src:
                run_jobrunner("--prune-src-bookmarks", *pull_args)
                self.assertEqual(1, len(bookmarks(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(2, len(snapshots(ibase.DST_ROOT_DATASET)))

                # pruning a nonexistingpool destination pool does nothing:
                run_jobrunner(
                    "--prune-dst-snapshots",
                    "--root-dataset-pairs",
                    *([ibase.SRC_ROOT_DATASET, "nonexistingpool/" + ibase.DST_ROOT_DATASET] + pull_args[3:]),
                )
                self.assertEqual(1, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(2, len(snapshots(ibase.DST_ROOT_DATASET)))

                # delete one old snapshot on dst:
                run_jobrunner("--prune-dst-snapshots", *pull_args)
                self.assertEqual(1, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

                # monitoring says latest dst snapshots aren't too old:
                run_jobrunner("--monitor-dst-snapshots", *pull_args)

                # next iteration: create another src snapshot
                run_jobrunner("--create-src-snapshots", *push_args)
                self.assertEqual(2, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

                # push replication does nothing if target isn't mapped to destination host:
                run_jobrunner("--replicate", "--workers=1", *push_args_bad)
                self.assertEqual(2, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(bookmarks(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

                # push replication does nothing if period isn't greater than zero:
                dst_snapshot_plan_empty = {"z": {"onsite": {"daily": 0}}}
                push_args_empty = [arg for arg in push_args if not arg.startswith("--dst-snapshot-plan=")]
                push_args_empty += [f"--dst-snapshot-plan={dst_snapshot_plan_empty}"]
                run_jobrunner("--replicate", *push_args_empty)
                self.assertEqual(2, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(bookmarks(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

                # push replication does nothing if periods are empty:
                dst_snapshot_plan_empty = {"z": {"onsite": {}}}
                push_args_empty = [arg for arg in push_args if not arg.startswith("--dst-snapshot-plan=")]
                push_args_empty += [f"--dst-snapshot-plan={dst_snapshot_plan_empty}"]
                run_jobrunner("--replicate", *push_args_empty)
                self.assertEqual(2, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(bookmarks(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(1, len(snapshots(ibase.DST_ROOT_DATASET)))

                if platform.system() != "Linux":
                    continue  # only Linux supports 127.0.0.2 and 127.0.0.1 test scenario simultaneously by default

                # push replicate successfully from src to dst:
                run_jobrunner("--replicate", "--workers=1", *push_args)
                self.assertEqual(2, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(2, len(bookmarks(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(2, len(snapshots(ibase.DST_ROOT_DATASET)))

                # monitoring says latest dst snapshots aren't too old:
                run_jobrunner("--monitor-dst-snapshots", *pull_args)

                # non-existing CLI option will cause failure:
                run_jobrunner(
                    "--create-src-snapshots",
                    "--prune-src-snapshots",
                    *(["--nonexistingoption"] + push_args),
                    expected_status=2,
                )
                self.assertEqual(2, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(2, len(bookmarks(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(2, len(snapshots(ibase.DST_ROOT_DATASET)))

                # Forgetting to specify src dataset (or dst dataset) will cause failure:
                run_jobrunner(
                    "--create-src-snapshots",
                    "--root-dataset-pairs",
                    *(push_args[2:]),  # Each SRC_DATASET must have a corresponding DST_DATASET
                    expected_status=2,
                )
                self.assertEqual(2, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(2, len(bookmarks(ibase.SRC_ROOT_DATASET)))
                self.assertEqual(2, len(snapshots(ibase.DST_ROOT_DATASET)))

                # monitoring says critical as there is no yearly snapshot:
                pull_args_no_monitoring = [arg for arg in pull_args if not arg.startswith("--monitor-snapshot-plan=")]
                monitor_dst_snapshot_plan = {"z": {"onsite": {"yearly": {"critical": "60 minutes"}}}}
                run_jobrunner(
                    "--monitor-dst-snapshots",
                    f"--monitor-snapshot-plan={monitor_dst_snapshot_plan}",
                    *pull_args_no_monitoring,
                    expected_status=bzfs.CRITICAL_STATUS,
                )

                # error: the following arguments are required: --root-dataset-pairs
                with self.assertRaises(SystemExit) as context:
                    bzfs_jobrunner.main()
                self.assertEqual(2, context.exception.code)

                # cmd = ["find", get_home_directory()]
                # subprocess.run(args=cmd, stdin=DEVNULL, check=True)

    def test_jobrunner_flat_simple_with_empty_targets(self) -> None:
        def run_jobrunner(*args: str, **kwargs: Any) -> bzfs_jobrunner.Job:
            return self.run_bzfs_jobrunner(
                *args,
                **kwargs,
                spawn_process_per_job=spawn_process_per_job,
                cache_snapshots=cache_snapshots,
            )

        if self.is_no_privilege_elevation():
            self.skipTest("Destroying snapshots on src needs extra permissions")
        if not are_bookmarks_enabled("src"):
            self.skipTest("ZFS has no bookmark feature")
        for jobiter, spawn_process_per_job in enumerate([None, True]):
            cache_snapshots = spawn_process_per_job is None
            with stop_on_failure_subtest(i=jobiter):
                self.tearDownAndSetup()
                destroy(ibase.DST_ROOT_DATASET, recursive=True)
                self.assert_snapshots(ibase.SRC_ROOT_DATASET, 0)

                localhostname = socket.gethostname()
                src_hosts = [localhostname]  # for local mode (no ssh, no network)
                dst_hosts_pull = {localhostname: ["onsite", ""]}
                # dst_root_datasets = {localhostname: "", "localhost": ""}
                dst_root_datasets = {localhostname: ""}
                src_snapshot_plan = {"z": {"onsite": {"yearly": 1, "daily": 0}}}
                dst_snapshot_plan = {"z": {"": {"yearly": 1, "daily": 0}}}
                src_bookmark_plan = dst_snapshot_plan
                args = [
                    "--root-dataset-pairs",
                    ibase.SRC_ROOT_DATASET,
                    ibase.DST_ROOT_DATASET,
                    f"--src-hosts={src_hosts}",
                    f"--localhost={localhostname}",
                    f"--retain-dst-targets={dst_hosts_pull}",
                    f"--dst-root-datasets={dst_root_datasets}",
                    f"--src-snapshot-plan={src_snapshot_plan}",
                    f"--src-bookmark-plan={src_bookmark_plan}",
                    f"--dst-snapshot-plan={dst_snapshot_plan}",
                    "--create-src-snapshots-timeformat=%Y-%m-%d_%H:%M:%S.%f",
                    "--job-id=myjobid",
                    "--jitter",
                    f"--ssh-src-port={getenv_any('test_ssh_port', '22')}",
                    f"--ssh-dst-port={getenv_any('test_ssh_port', '22')}",
                ]
                pull_args = args + [f"--dst-hosts={dst_hosts_pull}"]

                # next iteration: create a src snapshot with non-empty target
                run_jobrunner("--create-src-snapshots", *pull_args)
                self.assert_snapshot_name_regexes(ibase.SRC_ROOT_DATASET, ["z_onsite.*_yearly"])
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

                # next iteration: create a src snapshot with empty target
                src_snapshot_plan_empty = {"z": {"": {"yearly": 1}}}
                pull_args_empty = [arg for arg in pull_args if not arg.startswith("--src-snapshot-plan=")]
                pull_args_empty += [f"--src-snapshot-plan={src_snapshot_plan_empty}"]
                run_jobrunner("--create-src-snapshots", *pull_args_empty)
                self.assert_snapshot_name_regexes(ibase.SRC_ROOT_DATASET, ["z_(?!onsite).*_yearly", "z_onsite.*_yearly"])
                self.assertFalse(dataset_exists(ibase.DST_ROOT_DATASET))

                # replicate empty targets from src to dst:
                run_jobrunner("--replicate", *pull_args_empty)
                self.assertEqual(2, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assert_snapshot_name_regexes(ibase.DST_ROOT_DATASET, ["z_(?!onsite).*_yearly"])

                # with empty dst, replicate non-empty targets from src to dst
                destroy(ibase.DST_ROOT_DATASET, recursive=True)
                dst_snapshot_plan_nonempty = {"z": {"onsite": {"yearly": 1, "daily": 0}}}
                pull_args_nonempty = [arg for arg in pull_args if not arg.startswith("--dst-snapshot-plan=")]
                pull_args_nonempty += [f"--dst-snapshot-plan={dst_snapshot_plan_nonempty}"]
                run_jobrunner("--replicate", *pull_args_nonempty)
                self.assertEqual(2, len(snapshots(ibase.SRC_ROOT_DATASET)))
                self.assert_snapshot_name_regexes(ibase.DST_ROOT_DATASET, ["z_onsite_.*_yearly"])

                # only retain src snapshots with non-empty target:
                src_snapshot_plan_nonempty = {"z": {"onsite": {"yearly": 1}}}
                pull_args_nonempty = [arg for arg in pull_args if not arg.startswith("--src-snapshot-plan=")]
                pull_args_nonempty += [f"--src-snapshot-plan={src_snapshot_plan_nonempty}"]
                run_jobrunner("--prune-src-snapshots", *pull_args_nonempty)
                self.assert_snapshot_name_regexes(ibase.SRC_ROOT_DATASET, ["z_onsite.*_yearly"])

                # only retain src snapshots with empty target:
                src_snapshot_plan_empty = {"z": {"": {"yearly": 1}}}
                pull_args_empty = [arg for arg in pull_args if not arg.startswith("--src-snapshot-plan=")]
                pull_args_empty += [f"--src-snapshot-plan={src_snapshot_plan_empty}"]
                run_jobrunner("--prune-src-snapshots", *pull_args_empty)
                self.assertEqual(0, len(snapshots(ibase.SRC_ROOT_DATASET)))

    def test_jobrunner_monitor_oldest_skip_holds(self) -> None:
        """Integration coverage for `oldest_skip_holds` configuration option in bzfs_jobrunner monitoring.

        Creates two snapshots for the same monitored label: an older snapshot that is held (`zfs hold` implies `userrefs>0`)
        and a newer snapshot without holds. The test asserts that `oldest_skip_holds=True` reports OK (the held snapshot is
        ignored when choosing the oldest), and that `oldest_skip_holds=False` reports CRITICAL (the held snapshot becomes the
        oldest and is therefore too old).
        The test intentionally uses a short sleep to ensure the held snapshot becomes stale while the non-held snapshot
        remains fresh.
        """

        def run_jobrunner(*args: str, **kwargs: Any) -> bzfs_jobrunner.Job:
            return self.run_bzfs_jobrunner(*args, **kwargs)

        self.tearDownAndSetup()
        self.assert_snapshots(ibase.SRC_ROOT_DATASET, 0)

        localhostname = socket.gethostname()
        src_hosts = [localhostname]  # for local mode (no ssh, no network)
        dst_hosts = {localhostname: ["onsite"]}
        retain_dst_targets = dst_hosts.copy()
        dst_root_datasets = {localhostname: ""}
        src_snapshot_plan = {"z": {"onsite": {"millisecondly": 1}}}
        dst_snapshot_plan = {"z": {"onsite": {"millisecondly": 1}}}
        src_bookmark_plan = dst_snapshot_plan

        jobrunner_args = [
            "--root-dataset-pairs",
            ibase.SRC_ROOT_DATASET,
            ibase.DST_ROOT_DATASET,
            f"--src-hosts={src_hosts}",
            f"--localhost={localhostname}",
            f"--dst-hosts={dst_hosts}",
            f"--retain-dst-targets={retain_dst_targets}",
            f"--dst-root-datasets={dst_root_datasets}",
            f"--src-snapshot-plan={src_snapshot_plan}",
            f"--src-bookmark-plan={src_bookmark_plan}",
            f"--dst-snapshot-plan={dst_snapshot_plan}",
            "--job-id=myjobid",
            f"--ssh-src-port={getenv_any('test_ssh_port', '22')}",
            f"--ssh-dst-port={getenv_any('test_ssh_port', '22')}",
        ]

        held_snapshot = take_snapshot(ibase.SRC_ROOT_DATASET, "z_onsite_20000101_000000_millisecondly")
        hold_tag = "bzfs_test_hold"
        self.assertEqual("0", snapshot_property(held_snapshot, "userrefs"))
        run_cmd(SUDO_CMD + ["zfs", "hold", hold_tag, held_snapshot])
        self.assertEqual("1", snapshot_property(held_snapshot, "userrefs"))
        try:
            time.sleep(3.0)  # ensure the held snapshot becomes older than the critical threshold
            nonheld_snapshot = take_snapshot(ibase.SRC_ROOT_DATASET, "z_onsite_20000101_000001_millisecondly")
            self.assertEqual("0", snapshot_property(nonheld_snapshot, "userrefs"))

            def monitor_plan(oldest_skip_holds: bool) -> dict[str, dict[str, dict[str, dict[str, Any]]]]:
                return {
                    "z": {
                        "onsite": {
                            "millisecondly": {
                                "critical": "2 seconds",
                                "src_snapshot_cycles": 0,  # do not extend threshold by period duration
                                "oldest_skip_holds": oldest_skip_holds,
                            }
                        }
                    }
                }

            run_jobrunner(
                "--monitor-snapshots-no-latest-check",
                "--monitor-src-snapshots",
                f"--monitor-snapshot-plan={monitor_plan(True)}",
                *jobrunner_args,
            )
            run_jobrunner(
                "--monitor-snapshots-no-latest-check",
                "--monitor-src-snapshots",
                f"--monitor-snapshot-plan={monitor_plan(False)}",
                *jobrunner_args,
                expected_status=bzfs.CRITICAL_STATUS,
            )
        finally:
            if held_snapshot in snapshots(ibase.SRC_ROOT_DATASET):
                run_cmd(SUDO_CMD + ["zfs", "release", hold_tag, held_snapshot])
                self.assertEqual("0", snapshot_property(held_snapshot, "userrefs"))
