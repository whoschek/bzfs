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

import unittest

from bzfs_main import utils


def suite():
    suite = unittest.TestSuite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestBzfsUtils))
    return suite


#############################################################################
class TestBzfsUtils(unittest.TestCase):

    def test_cut_field1(self):
        lines = ["a\tb\tc", "d\te\tf"]
        expected = ["a", "d"]
        self.assertEqual(utils.cut(field=1, lines=lines), expected)

    def test_cut_field2(self):
        lines = ["a\tb\tc", "d\te\tf"]
        expected = ["b\tc", "e\tf"]
        self.assertEqual(utils.cut(field=2, lines=lines), expected)

    def test_cut_invalid_field(self):
        lines = ["a\tb\tc"]
        with self.assertRaises(ValueError):
            utils.cut(field=3, lines=lines)

    def test_cut_empty_lines(self):
        self.assertEqual(utils.cut(field=1, lines=[]), [])

    def test_cut_different_separator(self):
        lines = ["a,b,c", "d,e,f"]
        expected = ["a", "d"]
        self.assertEqual(utils.cut(field=1, separator=",", lines=lines), expected)
