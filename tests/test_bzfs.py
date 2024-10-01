#!/usr/bin/env python3
#
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

import os
import sys
import unittest

from tests.test_units import suite as test_units_suite
from tests.test_integrations import suite as test_integrations_suite


def main():
    suite = unittest.TestSuite()
    suite.addTests(test_units_suite())
    suite.addTests(test_integrations_suite())

    failfast = False if os.getenv("CI") else True  # no need to fail fast when running within GitHub Action
    print(f"Running in failfast mode: {failfast} ...")
    result = unittest.TextTestRunner(failfast=failfast, verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())


if __name__ == "__main__":
    main()
