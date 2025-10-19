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
"""Aggregates all tests into one centralized suite for easy execution; ensures predictable order across CI and local runs."""

from __future__ import (
    annotations,
)
import os
import sys
import unittest

import bzfs_main.utils
import bzfs_tests.test_argparse_actions
import bzfs_tests.test_bzfs
import bzfs_tests.test_compare_snapshot_lists
import bzfs_tests.test_configuration
import bzfs_tests.test_connection
import bzfs_tests.test_connection_lease
import bzfs_tests.test_detect
import bzfs_tests.test_filter
import bzfs_tests.test_incremental_send_steps
import bzfs_tests.test_interner
import bzfs_tests.test_jobrunner
import bzfs_tests.test_loggers
import bzfs_tests.test_parallel_batch_cmd
import bzfs_tests.test_parallel_tasktree
import bzfs_tests.test_parallel_tasktree_policy
import bzfs_tests.test_period_anchors
import bzfs_tests.test_progress_reporter
import bzfs_tests.test_replication
import bzfs_tests.test_retry
import bzfs_tests.test_snapshot_cache
import bzfs_tests.test_utils


def main() -> None:
    suite = unittest.TestSuite()
    suites = [
        bzfs_tests.test_utils.suite(),
        bzfs_tests.test_interner.suite(),
        bzfs_tests.test_loggers.suite(),
        bzfs_tests.test_retry.suite(),
        bzfs_tests.test_period_anchors.suite(),
        bzfs_tests.test_progress_reporter.suite(),
        bzfs_tests.test_parallel_batch_cmd.suite(),
        bzfs_tests.test_parallel_tasktree.suite(),
        bzfs_tests.test_parallel_tasktree_policy.suite(),
        bzfs_tests.test_filter.suite(),
        bzfs_tests.test_connection.suite(),
        bzfs_tests.test_connection_lease.suite(),
        bzfs_tests.test_detect.suite(),
        bzfs_tests.test_incremental_send_steps.suite(),
        bzfs_tests.test_argparse_actions.suite(),
        bzfs_tests.test_configuration.suite(),
        bzfs_tests.test_bzfs.suite(),
        bzfs_tests.test_replication.suite(),
        bzfs_tests.test_compare_snapshot_lists.suite(),
        bzfs_tests.test_snapshot_cache.suite(),
        bzfs_tests.test_jobrunner.suite(),
    ]
    suite.addTests(suites)
    test_mode = bzfs_main.utils.getenv_any("test_mode", "")  # Consider toggling this when testing
    is_unit_test = test_mode == "unit"  # run only unit tests (skip integration tests)
    if not is_unit_test:
        from bzfs_tests import (
            test_integrations,
        )

        suite.addTest(test_integrations.suite())

    failfast = False if os.getenv("CI") else True  # no need to fail fast when running within GitHub Action
    print(f"Running in failfast mode: {failfast} ...")
    result = unittest.TextTestRunner(failfast=failfast, verbosity=2, descriptions=False).run(suite)
    sys.exit(not result.wasSuccessful())


if __name__ == "__main__":
    main()
