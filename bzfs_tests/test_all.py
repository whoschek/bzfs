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
import types
import unittest

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
import bzfs_tests.test_parallel_iterator
import bzfs_tests.test_parallel_tasktree
import bzfs_tests.test_parallel_tasktree_policy
import bzfs_tests.test_period_anchors
import bzfs_tests.test_progress_reporter
import bzfs_tests.test_replication
import bzfs_tests.test_retry
import bzfs_tests.test_snapshot_cache
import bzfs_tests.test_utils
from bzfs_main.util.utils import (
    getenv_any,
)
from bzfs_tests.tools import (
    TestSuiteCompleteness,
)


def main() -> None:
    modules: list[types.ModuleType] = [
        bzfs_tests.test_utils,
        bzfs_tests.test_interner,
        bzfs_tests.test_loggers,
        bzfs_tests.test_retry,
        bzfs_tests.test_period_anchors,
        bzfs_tests.test_progress_reporter,
        bzfs_tests.test_parallel_iterator,
        bzfs_tests.test_parallel_batch_cmd,
        bzfs_tests.test_parallel_tasktree,
        bzfs_tests.test_parallel_tasktree_policy,
        bzfs_tests.test_filter,
        bzfs_tests.test_connection,
        bzfs_tests.test_connection_lease,
        bzfs_tests.test_detect,
        bzfs_tests.test_incremental_send_steps,
        bzfs_tests.test_argparse_actions,
        bzfs_tests.test_configuration,
        bzfs_tests.test_bzfs,
        bzfs_tests.test_replication,
        bzfs_tests.test_compare_snapshot_lists,
        bzfs_tests.test_snapshot_cache,
        bzfs_tests.test_jobrunner,
    ]
    incomplete_modules: list[types.ModuleType] = []

    test_mode = getenv_any("test_mode", "")  # Consider toggling this when testing
    is_unit_test = test_mode == "unit"  # run only unit tests (skip integration tests)
    if not is_unit_test:
        from bzfs_tests import (
            test_integrations,
        )

        modules.append(test_integrations)
        incomplete_modules.append(test_integrations)

    suite = unittest.TestSuite()
    suite.addTests(module.suite() for module in modules)

    # Verify each test module's suite() includes all locally defined test classes to avoid accidentally orphaned tests
    def class_predicate(cls: type[unittest.TestCase]) -> bool:
        """Returns True if the given test must be included in the suite()."""
        excluded_classes: frozenset[type[unittest.TestCase]] = frozenset(
            [
                # bzfs_tests.test_bzfs.TestPerformance,
            ]
        )
        if cls in excluded_classes:
            return False
        if cls.__name__.startswith("Test"):
            return True
        return False

    complete_modules: list[types.ModuleType] = [module for module in modules if module not in incomplete_modules]
    for name in unittest.TestLoader().getTestCaseNames(TestSuiteCompleteness):
        suite.addTest(TestSuiteCompleteness(name, modules=complete_modules, class_predicate=class_predicate))

    failfast = False if os.getenv("CI") else True  # no need to fail fast when running within GitHub Action
    print(f"Running in failfast mode: {failfast} ...")
    result = unittest.TextTestRunner(failfast=failfast, verbosity=2, descriptions=False).run(suite)
    sys.exit(not result.wasSuccessful())


if __name__ == "__main__":
    main()
