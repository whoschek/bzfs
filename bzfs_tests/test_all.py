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

from __future__ import annotations
import os
import sys
import unittest

import bzfs_main.utils
import bzfs_tests.test_bzfs
import bzfs_tests.test_connection
import bzfs_tests.test_detect
import bzfs_tests.test_filter
import bzfs_tests.test_incremental_send_steps
import bzfs_tests.test_integrations
import bzfs_tests.test_jobrunner
import bzfs_tests.test_loggers
import bzfs_tests.test_parallel_engine
import bzfs_tests.test_parallel_iterator
import bzfs_tests.test_period_anchors
import bzfs_tests.test_progress_reporter
import bzfs_tests.test_retry
import bzfs_tests.test_utils


def main() -> None:
    suite = unittest.TestSuite()
    suite.addTests(bzfs_tests.test_utils.suite())
    suite.addTests(bzfs_tests.test_loggers.suite())
    suite.addTests(bzfs_tests.test_retry.suite())
    suite.addTests(bzfs_tests.test_period_anchors.suite())
    suite.addTests(bzfs_tests.test_progress_reporter.suite())
    suite.addTests(bzfs_tests.test_parallel_iterator.suite())
    suite.addTests(bzfs_tests.test_parallel_engine.suite())
    suite.addTests(bzfs_tests.test_filter.suite())
    suite.addTests(bzfs_tests.test_connection.suite())
    suite.addTests(bzfs_tests.test_detect.suite())
    suite.addTests(bzfs_tests.test_incremental_send_steps.suite())
    suite.addTests(bzfs_tests.test_bzfs.suite())
    suite.addTests(bzfs_tests.test_jobrunner.suite())
    test_mode = bzfs_main.utils.getenv_any("test_mode", "")  # Consider toggling this when testing
    is_unit_test = test_mode == "unit"  # run only unit tests (skip integration tests)
    if not is_unit_test:
        suite.addTests(bzfs_tests.test_integrations.suite())

    failfast = False if os.getenv("CI") else True  # no need to fail fast when running within GitHub Action
    print(f"Running in failfast mode: {failfast} ...")
    result = unittest.TextTestRunner(failfast=failfast, verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())


if __name__ == "__main__":
    main()
