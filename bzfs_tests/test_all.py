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

from __future__ import annotations
import os
import sys
import unittest

from bzfs_tests.test_utils import suite as test_utils_suite
from bzfs_tests.test_retry import suite as test_retry_suite
from bzfs_tests.test_period_anchors import suite as test_period_anchors_suite
from bzfs_tests.test_progress_reporter import suite as test_progress_reporter_utils_suite
from bzfs_tests.test_parallel_engine import suite as test_parallel_engine_suite
from bzfs_tests.test_bzfs import suite as test_bzfs_suite
from bzfs_tests.test_jobrunner import suite as test_jobrunner_suite
from bzfs_tests.test_integrations import suite as test_integrations_suite
from bzfs_main.bzfs import getenv_any


def main() -> None:
    suite = unittest.TestSuite()
    suite.addTests(test_utils_suite())
    suite.addTests(test_retry_suite())
    suite.addTests(test_period_anchors_suite())
    suite.addTests(test_progress_reporter_utils_suite())
    suite.addTests(test_parallel_engine_suite())
    suite.addTests(test_bzfs_suite())
    suite.addTests(test_jobrunner_suite())
    test_mode = getenv_any("test_mode", "")  # Consider toggling this when testing
    is_unit_test = test_mode == "unit"  # run only unit tests (skip integration tests)
    if not is_unit_test:
        suite.addTests(test_integrations_suite())

    failfast = False if os.getenv("CI") else True  # no need to fail fast when running within GitHub Action
    print(f"Running in failfast mode: {failfast} ...")
    result = unittest.TextTestRunner(failfast=failfast, verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())


if __name__ == "__main__":
    main()
