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
"""Test case base class used by most unit tests.

Provides shared setup for consistent CLI argument parsing and environment control. Tests may run in unit, smoke, functional
or adhoc modes, toggled by environment variables.
"""

from __future__ import annotations
import argparse
import os
import unittest

from bzfs_main.bzfs import (
    argument_parser,
    log_dir_default,
)
from bzfs_main.utils import (
    get_home_directory,
    getenv_any,
)


#############################################################################
class AbstractTestCase(unittest.TestCase):

    def __init__(self, methodName: str = "runTest") -> None:  # noqa: N803
        super().__init__(methodName)
        # immutable variables:
        self.test_mode: str = getenv_any("test_mode", "") or ""  # Consider toggling this when testing
        self.is_unit_test: bool = self.test_mode == "unit"  # run only unit tests aka skip integration tests
        self.is_smoke_test: bool = self.test_mode == "smoke"  # run only a small subset of tests
        self.is_functional_test: bool = self.test_mode == "functional"  # most tests but only in a single local config combo
        self.is_adhoc_test: bool = self.test_mode == "adhoc"  # run only a few isolated changes

    @staticmethod
    def argparser_parse_args(args: list[str]) -> argparse.Namespace:
        return argument_parser().parse_args(
            args + ["--log-dir", os.path.join(get_home_directory(), log_dir_default + "-test")]
        )
