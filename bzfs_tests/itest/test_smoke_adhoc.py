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
"""Smoke and adhoc integration test suites (subsets delegating to LocalTestCase)."""

from __future__ import (
    annotations,
)
from typing import (
    final,
)

from bzfs_tests.itest.ibase import (
    IntegrationTestCase,
)
from bzfs_tests.itest.test_local import (
    LocalTestCase,
)


#############################################################################
@final
class SmokeTestCase(IntegrationTestCase):
    """Runs only a small subset of tests."""

    def test_aaa_log_diagnostics_first(self) -> None:
        LocalTestCase(param=self.param).test_aaa_log_diagnostics_first()

    def test_include_snapshots_plan(self) -> None:
        LocalTestCase(param=self.param).test_include_snapshots_plan()

    def test_basic_replication_recursive1(self) -> None:
        LocalTestCase(param=self.param).test_basic_replication_recursive1()

    def test_delete_dst_snapshots_except_plan(self) -> None:
        LocalTestCase(param=self.param).test_delete_dst_snapshots_except_plan()

    def test_basic_snapshotting_flat_simple(self) -> None:
        LocalTestCase(param=self.param).test_basic_snapshotting_flat_simple()

    def test_jobrunner_flat_simple(self) -> None:
        LocalTestCase(param=self.param).test_jobrunner_flat_simple()

    def test_xbasic_replication_flat_with_bookmarks1(self) -> None:
        LocalTestCase(param=self.param).test_xbasic_replication_flat_with_bookmarks1()

    def test_compare_snapshot_lists(self) -> None:
        LocalTestCase(param=self.param).test_compare_snapshot_lists()


#############################################################################
@final
class AdhocTestCase(IntegrationTestCase):
    """For testing isolated changes you are currently working on.

    You can temporarily change the list of tests here. The current list is arbitrary and subject to change at any time.
    """

    def test_aaa_log_diagnostics_first(self) -> None:
        LocalTestCase(param=self.param).test_aaa_log_diagnostics_first()

    def test_include_snapshots_plan(self) -> None:
        LocalTestCase(param=self.param).test_include_snapshots_plan()

    def test_delete_dst_snapshots_except_plan(self) -> None:
        LocalTestCase(param=self.param).test_delete_dst_snapshots_except_plan()

    def test_daemon_frequency(self) -> None:
        LocalTestCase(param=self.param).test_daemon_frequency()

    def test_daemon_frequency_invalid(self) -> None:
        LocalTestCase(param=self.param).test_daemon_frequency_invalid()

    def test_basic_snapshotting_flat_simple(self) -> None:
        LocalTestCase(param=self.param).test_basic_snapshotting_flat_simple()

    def test_basic_snapshotting_flat_non_existing_root(self) -> None:
        LocalTestCase(param=self.param).test_basic_snapshotting_flat_non_existing_root()

    def test_basic_snapshotting_flat_empty(self) -> None:
        LocalTestCase(param=self.param).test_basic_snapshotting_flat_empty()

    def test_basic_snapshotting_recursive_simple(self) -> None:
        LocalTestCase(param=self.param).test_basic_snapshotting_recursive_simple()

    def test_basic_snapshotting_recursive_with_skip_parent(self) -> None:
        LocalTestCase(param=self.param).test_basic_snapshotting_recursive_with_skip_parent()

    def test_basic_snapshotting_recursive_simple_with_incompatible_pruning(self) -> None:
        LocalTestCase(param=self.param).test_basic_snapshotting_recursive_simple_with_incompatible_pruning()

    def test_basic_snapshotting_recursive_simple_with_incompatible_pruning_with_skip_parent(self) -> None:
        LocalTestCase(param=self.param).test_basic_snapshotting_recursive_simple_with_incompatible_pruning_with_skip_parent()

    def test_basic_snapshotting_flat_even_if_not_due(self) -> None:
        LocalTestCase(param=self.param).test_basic_snapshotting_flat_even_if_not_due()

    def test_invalid_use_of_dummy(self) -> None:
        LocalTestCase(param=self.param).test_invalid_use_of_dummy()

    def test_basic_snapshotting_flat_daemon(self) -> None:
        LocalTestCase(param=self.param).test_basic_snapshotting_flat_daemon()

    def test_big_snapshotting_generates_identical_createtxg_despite_incompatible_pruning(self) -> None:
        LocalTestCase(param=self.param).test_big_snapshotting_generates_identical_createtxg_despite_incompatible_pruning()

    def test_jobrunner_flat_simple(self) -> None:
        LocalTestCase(param=self.param).test_jobrunner_flat_simple()

    def test_jobrunner_flat_simple_with_empty_targets(self) -> None:
        LocalTestCase(param=self.param).test_jobrunner_flat_simple_with_empty_targets()

    def test_last_replicated_cache_with_no_sleep(self) -> None:
        LocalTestCase(param=self.param).test_last_replicated_cache_with_no_sleep()

    def test_last_replicated_cache_with_sleep_longer_than_threshold(self) -> None:
        LocalTestCase(param=self.param).test_last_replicated_cache_with_sleep_longer_than_threshold()

    def test_cache_flat_simple(self) -> None:
        LocalTestCase(param=self.param).test_cache_flat_simple()

    def test_cache_flat_simple_subset(self) -> None:
        LocalTestCase(param=self.param).test_cache_flat_simple_subset()
