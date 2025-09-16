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
"""Unit tests for argparse action classes used by ``bzfs``."""

from __future__ import (
    annotations,
)
import argparse
import unittest
from unittest.mock import (
    mock_open,
    patch,
)

import bzfs_main.utils
from bzfs_main import (
    argparse_actions,
)
from bzfs_main.check_range import (
    CheckRange,
)
from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)
from bzfs_tests.tools import (
    suppress_output,
)


###############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestDatasetPairsAction,
        TestFileOrLiteralAction,
        TestNewSnapshotFilterGroupAction,
        TestNonEmptyStringAction,
        TestLogConfigVariablesAction,
        SSHConfigFileNameAction,
        TestSafeFileNameAction,
        TestSafeDirectoryNameAction,
        TestValidateNoArgumentFile,
        TestCheckRange,
        TestCheckPercentRange,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


###############################################################################
class TestDatasetPairsAction(AbstractTestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--input", nargs="+", action=argparse_actions.DatasetPairsAction)

    def test_direct_value(self) -> None:
        args = self.parser.parse_args(["--input", "src1", "dst1"])
        self.assertEqual([("src1", "dst1")], args.input)

    def test_direct_value_without_corresponding_dst(self) -> None:
        with self.assertRaises(SystemExit), suppress_output():
            self.parser.parse_args(["--input", "src1"])

    def test_file_input(self) -> None:
        with patch("bzfs_main.argparse_actions.open_nofollow", mock_open(read_data="src1\tdst1\nsrc2\tdst2\n")):
            args = self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
            self.assertEqual([("src1", "dst1"), ("src2", "dst2")], args.input)

    def test_file_input_without_trailing_newline(self) -> None:
        with patch("bzfs_main.argparse_actions.open_nofollow", mock_open(read_data="src1\tdst1\nsrc2\tdst2")):
            args = self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
            self.assertEqual([("src1", "dst1"), ("src2", "dst2")], args.input)

    def test_mixed_input(self) -> None:
        with patch("bzfs_main.argparse_actions.open_nofollow", mock_open(read_data="src1\tdst1\nsrc2\tdst2\n")):
            args = self.parser.parse_args(["--input", "src0", "dst0", "+test_bzfs_argument_file"])
            self.assertEqual([("src0", "dst0"), ("src1", "dst1"), ("src2", "dst2")], args.input)

    def test_file_skip_comments_and_empty_lines(self) -> None:
        with patch(
            "bzfs_main.argparse_actions.open_nofollow", mock_open(read_data="\n\n#comment\nsrc1\tdst1\nsrc2\tdst2\n")
        ):
            args = self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
            self.assertEqual([("src1", "dst1"), ("src2", "dst2")], args.input)

    def test_file_skip_stripped_empty_lines(self) -> None:
        with patch("bzfs_main.argparse_actions.open_nofollow", mock_open(read_data=" \t \nsrc1\tdst1")):
            args = self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
            self.assertEqual([("src1", "dst1")], args.input)

    def test_file_missing_tab(self) -> None:
        with patch("bzfs_main.argparse_actions.open_nofollow", mock_open(read_data="src1\nsrc2")):
            with self.assertRaises(SystemExit), suppress_output():
                self.parser.parse_args(["--input", "+test_bzfs_argument_file"])

    def test_file_whitespace_only(self) -> None:
        with patch("bzfs_main.argparse_actions.open_nofollow", mock_open(read_data=" \tdst1")):
            with self.assertRaises(SystemExit), suppress_output():
                self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
        with patch("bzfs_main.argparse_actions.open_nofollow", mock_open(read_data="src1\t ")):
            with self.assertRaises(SystemExit), suppress_output():
                self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
        with patch("bzfs_main.argparse_actions.open_nofollow", side_effect=FileNotFoundError):
            with self.assertRaises(SystemExit), suppress_output():
                self.parser.parse_args(["--input", "+nonexistent_test_bzfs_argument_file"])

    def test_option_not_specified(self) -> None:
        args = self.parser.parse_args([])
        self.assertIsNone(args.input)

    def test_dataset_pairs_action_invalid_basename(self) -> None:
        with patch("bzfs_main.argparse_actions.open_nofollow", mock_open(read_data="src\tdst\n")):
            with self.assertRaises(SystemExit), suppress_output():
                self.parser.parse_args(["--input", "+bad_file_name"])


###############################################################################
class TestFileOrLiteralAction(AbstractTestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--input", nargs="+", action=argparse_actions.FileOrLiteralAction)

    def test_direct_value(self) -> None:
        args = self.parser.parse_args(["--input", "literalvalue"])
        self.assertEqual(["literalvalue"], args.input)

    def test_file_input(self) -> None:
        with patch("bzfs_main.argparse_actions.open_nofollow", mock_open(read_data="line 1\nline 2  \n")):
            args = self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
            self.assertEqual(["line 1", "line 2  "], args.input)

    def test_mixed_input(self) -> None:
        with patch("bzfs_main.argparse_actions.open_nofollow", mock_open(read_data="line 1\nline 2")):
            args = self.parser.parse_args(["--input", "literalvalue", "+test_bzfs_argument_file"])
            self.assertEqual(["literalvalue", "line 1", "line 2"], args.input)

    def test_skip_comments_and_empty_lines(self) -> None:
        with patch("bzfs_main.argparse_actions.open_nofollow", mock_open(read_data="\n\n#comment\nline 1\n\n\nline 2\n")):
            args = self.parser.parse_args(["--input", "+test_bzfs_argument_file"])
            self.assertEqual(["line 1", "line 2"], args.input)

    def test_file_not_found(self) -> None:
        with patch("bzfs_main.argparse_actions.open_nofollow", side_effect=FileNotFoundError):
            with self.assertRaises(SystemExit), suppress_output():
                self.parser.parse_args(["--input", "+nonexistent_test_bzfs_argument_file"])

    def test_option_not_specified(self) -> None:
        args = self.parser.parse_args([])
        self.assertIsNone(args.input)

    def test_file_or_literal_action_invalid_basename(self) -> None:
        with patch("bzfs_main.argparse_actions.open_nofollow", mock_open(read_data="line")):
            with self.assertRaises(SystemExit), suppress_output():
                self.parser.parse_args(["--input", "+bad_file_name"])


###############################################################################
class TestNewSnapshotFilterGroupAction(AbstractTestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument(
            "--new-snapshot-filter-group", action=argparse_actions.NewSnapshotFilterGroupAction, nargs=0
        )

    def test_basic0(self) -> None:
        args = self.parser.parse_args(["--new-snapshot-filter-group"])
        self.assertListEqual([[]], getattr(args, bzfs_main.utils.SNAPSHOT_FILTERS_VAR))

    def test_basic1(self) -> None:
        args = self.parser.parse_args(["--new-snapshot-filter-group", "--new-snapshot-filter-group"])
        self.assertListEqual([[]], getattr(args, bzfs_main.utils.SNAPSHOT_FILTERS_VAR))


###############################################################################
class TestNonEmptyStringAction(AbstractTestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--name", action=argparse_actions.NonEmptyStringAction)

    def test_non_empty_string_action_empty(self) -> None:
        with self.assertRaises(SystemExit), suppress_output():
            self.parser.parse_args(["--name", " "])


###############################################################################
class TestLogConfigVariablesAction(AbstractTestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("--log-config-var", nargs="+", action=argparse_actions.LogConfigVariablesAction)

    def test_basic(self) -> None:
        args = self.parser.parse_args(["--log-config-var", "name1:val1", "name2:val2"])
        self.assertEqual(["name1:val1", "name2:val2"], args.log_config_var)

        for var in ["", "  ", "varWithoutColon", ":valueWithoutName", " nameWithWhitespace:value"]:
            with self.assertRaises(SystemExit), suppress_output():
                self.parser.parse_args(["--log-config-var", var])


###############################################################################
class SSHConfigFileNameAction(AbstractTestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("filename", action=argparse_actions.SSHConfigFileNameAction)

    def test_safe_filename(self) -> None:
        args = self.parser.parse_args(["file1.txt"])
        self.assertEqual("file1.txt", args.filename)

    def test_empty_filename(self) -> None:
        with self.assertRaises(SystemExit), suppress_output():
            self.parser.parse_args([""])

    def test_filename_in_subdirectory(self) -> None:
        self.parser.parse_args(["subdir/safe_file.txt"])

    def test_filename_with_single_dot_slash(self) -> None:
        self.parser.parse_args(["./file.txt"])

    def test_ssh_config_filename_action_invalid_chars(self) -> None:
        with self.assertRaises(SystemExit), suppress_output():
            self.parser.parse_args(["foo bar"])


###############################################################################
class TestSafeFileNameAction(AbstractTestCase):

    def setUp(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("filename", action=argparse_actions.SafeFileNameAction)

    def test_safe_filename(self) -> None:
        args = self.parser.parse_args(["file1.txt"])
        self.assertEqual("file1.txt", args.filename)

    def test_empty_filename(self) -> None:
        args = self.parser.parse_args([""])
        self.assertEqual("", args.filename)

    def test_filename_in_subdirectory(self) -> None:
        with self.assertRaises(SystemExit), suppress_output():
            self.parser.parse_args(["subdir/safe_file.txt"])

    def test_unsafe_filename_with_parent_directory_reference(self) -> None:
        with self.assertRaises(SystemExit), suppress_output():
            self.parser.parse_args(["../escape.txt"])

    def test_unsafe_filename_with_absolute_path(self) -> None:
        with self.assertRaises(SystemExit), suppress_output():
            self.parser.parse_args(["/unsafe_file.txt"])

    def test_unsafe_nested_parent_directory(self) -> None:
        with self.assertRaises(SystemExit), suppress_output():
            self.parser.parse_args(["../../another_escape.txt"])

    def test_filename_with_single_dot_slash(self) -> None:
        with self.assertRaises(SystemExit), suppress_output():
            self.parser.parse_args(["./file.txt"])

    def test_filename_with_tab(self) -> None:
        with self.assertRaises(SystemExit), suppress_output():
            self.parser.parse_args(["foo\nbar.txt"])


###############################################################################
class TestSafeDirectoryNameAction(AbstractTestCase):

    def test_valid_directory_name_is_accepted(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--dir", action=argparse_actions.SafeDirectoryNameAction)
        args = parser.parse_args(["--dir", "valid_directory"])
        assert args.dir == "valid_directory"

    def test_empty_directory_name_raises_error(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--dir", action=argparse_actions.SafeDirectoryNameAction)
        with self.assertRaises(SystemExit), suppress_output():
            parser.parse_args(["--dir", ""])

    def test_directory_name_with_invalid_whitespace_raises_error(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--dir", action=argparse_actions.SafeDirectoryNameAction)
        with self.assertRaises(SystemExit), suppress_output():
            parser.parse_args(["--dir", "invalid\nname"])

    def test_directory_name_with_leading_or_trailing_spaces_is_trimmed(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--dir", action=argparse_actions.SafeDirectoryNameAction)
        args = parser.parse_args(["--dir", "  valid_directory  "])
        assert args.dir == "valid_directory"


###############################################################################
class TestValidateNoArgumentFile(AbstractTestCase):

    def test_validate_no_argument_file_raises(self) -> None:
        parser = argparse.ArgumentParser()
        ns = argparse.Namespace(no_argument_file=True)
        with self.assertRaises(SystemExit), suppress_output():
            argparse_actions.validate_no_argument_file("afile", ns, err_prefix="e", parser=parser)


###############################################################################
class TestCheckRange(AbstractTestCase):

    def test_valid_range_min_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange, min=0, max=100)
        args = parser.parse_args(["--age", "50"])
        self.assertEqual(50, args.age)

    def test_valid_range_inf_sup(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange, inf=0, sup=100)
        args = parser.parse_args(["--age", "50"])
        self.assertEqual(50, args.age)

    def test_invalid_range_min_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange, min=0, max=100)
        with self.assertRaises(SystemExit), suppress_output():
            parser.parse_args(["--age", "-1"])

    def test_invalid_range_inf_sup(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange, inf=0, sup=100)
        with self.assertRaises(SystemExit), suppress_output():
            parser.parse_args(["--age", "101"])

    def test_invalid_combination_min_inf(self) -> None:
        with self.assertRaises(ValueError):
            parser = argparse.ArgumentParser()
            parser.add_argument("--age", type=int, action=CheckRange, min=0, inf=100)

    def test_invalid_combination_max_sup(self) -> None:
        with self.assertRaises(ValueError):
            parser = argparse.ArgumentParser()
            parser.add_argument("--age", type=int, action=CheckRange, max=0, sup=100)

    def test_valid_float_range_min_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(["--age", "50.5"])
        self.assertEqual(50.5, args.age)

    def test_invalid_float_range_min_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange, min=0.0, max=100.0)
        with self.assertRaises(SystemExit), suppress_output():
            parser.parse_args(["--age", "-0.1"])

    def test_valid_edge_case_min(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(["--age", "0.0"])
        self.assertEqual(0.0, args.age)

    def test_valid_edge_case_max(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange, min=0.0, max=100.0)
        args = parser.parse_args(["--age", "100.0"])
        self.assertEqual(100.0, args.age)

    def test_invalid_edge_case_sup(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange, inf=0.0, sup=100.0)
        with self.assertRaises(SystemExit), suppress_output():
            parser.parse_args(["--age", "100.0"])

    def test_invalid_edge_case_inf(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange, inf=0.0, sup=100.0)
        with self.assertRaises(SystemExit), suppress_output():
            parser.parse_args(["--age", "0.0"])

    def test_no_range_constraints(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange)
        args = parser.parse_args(["--age", "150"])
        self.assertEqual(150, args.age)

    def test_no_range_constraints_float(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=float, action=CheckRange)
        args = parser.parse_args(["--age", "150.5"])
        self.assertEqual(150.5, args.age)

    def test_very_large_value(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange, max=10**18)
        args = parser.parse_args(["--age", "999999999999999999"])
        self.assertEqual(999999999999999999, args.age)

    def test_very_small_value(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange, min=-(10**18))
        args = parser.parse_args(["--age", "-999999999999999999"])
        self.assertEqual(-999999999999999999, args.age)

    def test_default_interval(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange)
        action = CheckRange(option_strings=["--age"], dest="age")
        self.assertEqual("valid range: (-infinity, +infinity)", action.interval())

    def test_interval_with_inf_sup(self) -> None:
        action = CheckRange(option_strings=["--age"], dest="age", inf=0, sup=100)
        self.assertEqual("valid range: (0, 100)", action.interval())

    def test_interval_with_min_max(self) -> None:
        action = CheckRange(option_strings=["--age"], dest="age", min=0, max=100)
        self.assertEqual("valid range: [0, 100]", action.interval())

    def test_interval_with_min(self) -> None:
        action = CheckRange(option_strings=["--age"], dest="age", min=0)
        self.assertEqual("valid range: [0, +infinity)", action.interval())

    def test_interval_with_max(self) -> None:
        action = CheckRange(option_strings=["--age"], dest="age", max=100)
        self.assertEqual("valid range: (-infinity, 100]", action.interval())

    def test_call_without_range_constraints(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--age", type=int, action=CheckRange)
        args = parser.parse_args(["--age", "50"])
        self.assertEqual(50, args.age)


###############################################################################
class TestCheckPercentRange(AbstractTestCase):

    def test_valid_range_min(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--threads", action=argparse_actions.CheckPercentRange, min=1)
        args = parser.parse_args(["--threads", "1"])
        threads, is_percent = args.threads
        self.assertEqual(1.0, threads)
        self.assertFalse(is_percent)

    def test_valid_range_percent(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--threads", action=argparse_actions.CheckPercentRange, min=1)
        args = parser.parse_args(["--threads", "5.2%"])
        threads, is_percent = args.threads
        self.assertEqual(5.2, threads)
        self.assertTrue(is_percent)

    def test_invalid(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--threads", action=argparse_actions.CheckPercentRange, min=1)
        with self.assertRaises(SystemExit), suppress_output():
            parser.parse_args(["--threads", "0"])
        with self.assertRaises(SystemExit), suppress_output():
            parser.parse_args(["--threads", "0%"])
        with self.assertRaises(SystemExit), suppress_output():
            parser.parse_args(["--threads", "abc"])
