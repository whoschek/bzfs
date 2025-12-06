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
"""Unit tests for various small utility helpers."""

from __future__ import (
    annotations,
)
import argparse
import base64
import errno
import hashlib
import logging
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from collections.abc import (
    Sequence,
)
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
)
from dataclasses import (
    dataclass,
)
from datetime import (
    datetime,
    timedelta,
    timezone,
    tzinfo,
)
from logging import (
    Logger,
)
from subprocess import (
    DEVNULL,
    PIPE,
)
from typing import (
    Any,
    Callable,
    Union,
    cast,
)
from unittest.mock import (
    MagicMock,
    patch,
)
from zoneinfo import (
    ZoneInfo,
)

from bzfs_main.util import (
    utils,
)
from bzfs_main.util.utils import (
    DESCENDANTS_RE_SUFFIX,
    DIR_PERMISSIONS,
    FILE_PERMISSIONS,
    JobStats,
    SmallPriorityQueue,
    SnapshotPeriods,
    Subprocesses,
    SynchronizedBool,
    SynchronizedDict,
    SynchronousExecutor,
    _get_descendant_processes,
    append_if_absent,
    binary_search,
    close_quietly,
    compile_regexes,
    current_datetime,
    cut,
    dataset_paths,
    die,
    drain,
    find_match,
    format_dict,
    get_home_directory,
    get_timezone,
    has_duplicates,
    human_readable_bytes,
    human_readable_duration,
    human_readable_float,
    is_descendant,
    is_included,
    isotime_from_unixtime,
    list_formatter,
    open_nofollow,
    parse_duration_to_milliseconds,
    percent,
    pid_exists,
    pretty_print_formatter,
    replace_capturing_groups_with_non_capturing_groups,
    sha256_hex,
    sha256_urlsafe_base64,
    shuffle_dict,
    sorted_dict,
    subprocess_run,
    tail,
    terminate_process_subtree,
    termination_signal_handler,
    unixtime_fromisoformat,
    urlsafe_base64,
    validate_file_permissions,
    xfinally,
    xprint,
)
from bzfs_tests.tools import (
    suppress_output,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestHelperFunctions,
        TestBase64,
        TestCut,
        TestDrain,
        TestShuffleDict,
        TestSortedDict,
        TestBinarySearch,
        TestTail,
        TestGetHomeDirectory,
        TestHumanReadable,
        TestOpenNoFollow,
        TestCloseQuietly,
        TestValidateFilePermissions,
        TestFindMatch,
        TestReplaceCapturingGroups,
        TestSubprocessRun,
        TestSubprocessRunWithSubprocesses,
        TestSubprocessRunLogging,
        TestPIDExists,
        TestTerminateProcessSubtree,
        TestTerminationSignalHandler,
        TestJobStats,
        TestSmallPriorityQueue,
        TestSynchronizedBool,
        TestSynchronizedDict,
        TestSynchronousExecutor,
        TestSnapshotPeriods,
        TestXFinally,
        TestCurrentDateTime,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestHelperFunctions(unittest.TestCase):

    def test_die_with_parser(self) -> None:
        parser = argparse.ArgumentParser()
        with self.assertRaises(SystemExit), suppress_output():
            die("boom", parser=parser)

    def test_has_duplicates(self) -> None:
        self.assertFalse(has_duplicates([]))
        self.assertFalse(has_duplicates([42]))
        self.assertFalse(has_duplicates([1, 2, 3, 4, 5]))
        self.assertTrue(has_duplicates([2, 2, 3, 4, 5]))
        self.assertTrue(has_duplicates([1, 2, 3, 3, 4, 5]))
        self.assertTrue(has_duplicates([1, 2, 3, 4, 5, 5]))
        self.assertTrue(has_duplicates([1, 1, 2, 3, 3, 4, 4, 5]))
        self.assertTrue(has_duplicates(["a", "b", "b", "c"]))
        self.assertFalse(has_duplicates(["ant", "bee", "cat"]))

    def test_is_descendant(self) -> None:
        self.assertTrue(is_descendant("pool/fs/child", "pool/fs"))
        self.assertTrue(is_descendant("pool/fs/child/grandchild", "pool/fs"))
        self.assertTrue(is_descendant("a/b/c/d", "a/b"))
        self.assertTrue(is_descendant("pool/fs", "pool/fs"))
        self.assertFalse(is_descendant("pool/otherfs", "pool/fs"))
        self.assertFalse(is_descendant("a/c", "a/b"))
        self.assertFalse(is_descendant("pool/fs", "pool/fs/child"))
        self.assertFalse(is_descendant("a/b", "a/b/c"))
        self.assertFalse(is_descendant("tank1/data", "tank2/backup"))
        self.assertFalse(is_descendant("a", ""))
        self.assertFalse(is_descendant("", "a"))
        self.assertTrue(is_descendant("", ""))
        self.assertFalse(is_descendant("pool/fs-backup", "pool/fs"))
        self.assertTrue(is_descendant("pool/fs", "pool"))

    def test_has_siblings(self) -> None:
        def has_siblings(sorted_datasets: list[str]) -> bool:
            return utils.has_siblings(sorted_datasets, is_test_mode=True)

        self.assertFalse(has_siblings([]))
        self.assertFalse(has_siblings(["a"]))
        self.assertFalse(has_siblings(["a", "a/b"]))
        self.assertFalse(has_siblings(["a", "a/b", "a/b/c"]))
        self.assertTrue(has_siblings(["a", "b"]))
        self.assertTrue(has_siblings(["a", "a/b", "a/d"]))
        self.assertTrue(has_siblings(["a", "a/b", "a/b/c", "a/b/d"]))
        self.assertTrue(has_siblings(["a/b/c", "d/e/f"]))  # multiple root datasets can be processed in parallel
        self.assertFalse(has_siblings(["a", "a/b/c"]))
        self.assertFalse(has_siblings(["a", "a/b/c/d"]))
        self.assertTrue(has_siblings(["a", "a/b/c", "a/b/d"]))
        self.assertTrue(has_siblings(["a", "a/b/c/d", "a/b/c/e"]))

    def test_dataset_paths(self) -> None:
        self.assertEqual(list(dataset_paths("a")), ["a"])
        self.assertEqual(list(dataset_paths("a/b/c")), ["a", "a/b", "a/b/c"])
        self.assertEqual(list(dataset_paths("")), [""])

    def test_append_if_absent(self) -> None:
        self.assertListEqual([], append_if_absent([]))
        self.assertListEqual(["a"], append_if_absent([], "a"))
        self.assertListEqual(["a"], append_if_absent([], "a", "a"))

    def test_compile_regexes(self) -> None:
        def _assert_full_match(text: str, regex: str, re_suffix: str = "", expected: bool = True) -> None:
            match = compile_regexes([regex], suffix=re_suffix)[0][0].fullmatch(text)
            if expected:
                self.assertTrue(match)
            else:
                self.assertFalse(match)

        def assert_full_match(text: str, regex: str, re_suffix: str = "") -> None:
            _assert_full_match(text=text, regex=regex, re_suffix=re_suffix, expected=True)

        def assert_not_full_match(text: str, regex: str, re_suffix: str = "") -> None:
            _assert_full_match(text=text, regex=regex, re_suffix=re_suffix, expected=False)

        re_suffix = DESCENDANTS_RE_SUFFIX
        assert_full_match("foo", "foo")
        assert_not_full_match("xfoo", "foo")
        assert_not_full_match("fooy", "foo")
        assert_not_full_match("foo/bar", "foo")
        assert_full_match("foo", "foo$")
        assert_full_match("foo", ".*")
        assert_full_match("foo/bar", ".*")
        assert_full_match("foo", ".*", re_suffix)
        assert_full_match("foo/bar", ".*", re_suffix)
        assert_full_match("foo", "foo", re_suffix)
        assert_full_match("foo/bar", "foo", re_suffix)
        assert_full_match("foo/bar/baz", "foo", re_suffix)
        assert_full_match("foo", "foo$", re_suffix)
        assert_full_match("foo$", "foo\\$", re_suffix)
        assert_full_match("foo", "!foo", re_suffix)
        assert_full_match("foo", "!foo")
        with self.assertRaises(re.error):
            compile_regexes(["fo$o"], re_suffix)

    def test_is_included_with_negated_exclude_regex(self) -> None:
        """Negated exclude regex excludes non-matching names but not matching ones."""
        exclude_regexes = compile_regexes(["!foo"])
        include_regexes = compile_regexes([".*"])
        self.assertTrue(is_included("foo", include_regexes, exclude_regexes))
        self.assertFalse(is_included("bar", include_regexes, exclude_regexes))

    def test_is_included_with_negated_include_regex(self) -> None:
        """Negated include regex includes non-matching names and excludes matching ones."""
        include_regexes = compile_regexes(["!foo"])
        exclude_regexes = compile_regexes([])
        self.assertTrue(is_included("bar", include_regexes, exclude_regexes))
        self.assertFalse(is_included("foo", include_regexes, exclude_regexes))

    def test_parse_duration_to_milliseconds(self) -> None:
        self.assertEqual(5000, parse_duration_to_milliseconds("5 seconds"))
        self.assertEqual(
            300000,
            parse_duration_to_milliseconds("5 minutes ago", regex_suffix=r"\s*ago"),
        )
        with self.assertRaises(ValueError):
            parse_duration_to_milliseconds("foo")
        with self.assertRaises(SystemExit), suppress_output():
            parse_duration_to_milliseconds("foo", context="ctx")

    def test_format_dict(self) -> None:
        self.assertEqual("\"{'a': 1}\"", format_dict({"a": 1}))

    def test_pretty_print_formatter(self) -> None:
        self.assertIsNotNone(str(pretty_print_formatter(argparse.Namespace(src="src", dst="dst"))))

    def test_list_formatter_lstrip_branch(self) -> None:
        """list_formatter strips or preserves leading whitespace depending on the lstrip flag."""
        items = ["  leading", "space"]
        formatter_no_lstrip = list_formatter(items, separator=" ", lstrip=False)
        formatter_lstrip = list_formatter(items, separator=" ", lstrip=True)
        self.assertEqual("  leading space", str(formatter_no_lstrip))
        self.assertEqual("leading space", str(formatter_lstrip))

    def test_xprint(self) -> None:
        log = MagicMock(spec=Logger)
        xprint(log, "foo")
        xprint(log, "foo", run=True)
        xprint(log, "foo", run=False)
        xprint(log, "foo", file=sys.stdout)
        xprint(log, "")
        xprint(log, "", run=True)
        xprint(log, "", run=False)
        xprint(log, None)


#############################################################################
class TestBase64(unittest.TestCase):

    def test_sha256_hex_basic(self) -> None:
        """sha256_hex returns 64 lowercase hex chars and is deterministic."""
        inputs = [
            "",
            "a",
            "abc",
            "hello",
            "hello world",
            "The quick brown fox jumps over the lazy dog",
            "häßlich",  # non-ASCII, ensure UTF-8 handling
        ]
        hex_re = re.compile(r"[0-9a-f]{64}")
        for text in inputs:
            out = sha256_hex(text)
            self.assertEqual(64, len(out))
            self.assertIsNotNone(hex_re.fullmatch(out))
            self.assertEqual(out, sha256_hex(text))  # Deterministic

    def test_sha256_hex_matches_stdlib(self) -> None:
        for text in ["", "abc", "hello world", "häßlich"]:
            expected = hashlib.sha256(text.encode("utf-8")).hexdigest()
            self.assertEqual(expected, sha256_hex(text))

    def test_sha256_base64_urlsafe_length_charset_padding(self) -> None:
        """All SHA-256 digests are 32 bytes; URL-safe Base64 yields 44 chars with 1 '=' trailing pad for 32-byte input."""
        inputs = [
            "",
            "a",
            "abc",
            "hello",
            "hello world",
            "The quick brown fox jumps over the lazy dog",
            "häßlich",  # non-ASCII, ensure UTF-8 handling
        ]
        # Allow only URL-safe Base64 characters (no padding expected when padding=False)
        pattern = re.compile(r"[A-Za-z0-9_-]+")
        for text in inputs:
            out = sha256_urlsafe_base64(text, padding=True)
            self.assertEqual(44, len(out))
            self.assertTrue(out.endswith("="))
            out = sha256_urlsafe_base64(text, padding=False)
            self.assertEqual(43, len(out))
            self.assertTrue(pattern.fullmatch(out))
            self.assertFalse(out.endswith("="))
            self.assertEqual(out, sha256_urlsafe_base64(text, padding=False))  # Deterministic

    def test_sha256_urlsafe_base64_matches_stdlib_composition(self) -> None:
        for text in ["", "abc", "hello world", "häßlich"]:
            expected = base64.urlsafe_b64encode(hashlib.sha256(text.encode("utf-8")).digest()).decode()
            self.assertEqual(expected, sha256_urlsafe_base64(text, padding=True))
            expected = base64.urlsafe_b64encode(hashlib.sha256(text.encode("utf-8")).digest()).decode().rstrip("=")
            self.assertEqual(expected, sha256_urlsafe_base64(text, padding=False))

    def test_urlsafe_base64_fixed_length_24_bits(self) -> None:
        max_value = 2**24 - 1
        samples = [0, 1, 42, 2**20, max_value]
        pattern = re.compile(r"[A-Za-z0-9_-]+")  # URL-safe Base64 alphabet
        for n in samples:
            out = urlsafe_base64(n, max_value, padding=True)
            self.assertEqual(4, len(out))
            out = urlsafe_base64(n, max_value, padding=False)
            self.assertEqual(4, len(out))
            self.assertIsNotNone(pattern.fullmatch(out))
            self.assertNotIn("=", out)
        self.assertEqual("A" * 4, urlsafe_base64(0, max_value, padding=False))

    def test_urlsafe_base64_fixed_length_64_bits(self) -> None:
        max_value = 2**64 - 1
        samples = [0, 1, 42, 2**20, max_value]
        pattern = re.compile(r"[A-Za-z0-9_-]+")  # URL-safe Base64 alphabet
        for n in samples:
            out = urlsafe_base64(n, max_value, padding=True)
            self.assertEqual(12, len(out))
            self.assertTrue(out.endswith("="))
            out = urlsafe_base64(n, max_value, padding=False)
            self.assertEqual(11, len(out))
            self.assertIsNotNone(pattern.fullmatch(out))
            self.assertNotIn("=", out)
        self.assertEqual("A" * 11, urlsafe_base64(0, max_value, padding=False))

    def test_urlsafe_base64_expected_lengths_for_various_byte_widths(self) -> None:
        """Validate that for byte widths 1..5 the URL-safe Base64 length matches spec, with and without '=' padding."""
        for byte_len in range(1, 6):
            expected_len = len(base64.urlsafe_b64encode(b"\x00" * byte_len).decode())
            expected_len_stripped = len(base64.urlsafe_b64encode(b"\x00" * byte_len).decode().rstrip("="))
            max_value = (1 << (8 * byte_len)) - 1
            for n in [0, 1, max_value // 2, max_value]:
                out = urlsafe_base64(n, max_value, padding=True)
                self.assertEqual(expected_len, len(out))
                out = urlsafe_base64(n, max_value, padding=False)
                self.assertEqual(expected_len_stripped, len(out))

    def test_urlsafe_base64_zero_max_value_edge_case(self) -> None:
        self.assertEqual("", urlsafe_base64(0, 0, padding=True))
        self.assertEqual("", urlsafe_base64(0, 0, padding=False))


###############################################################################
class TestSnapshotPeriods(unittest.TestCase):

    def test_label_milliseconds(self) -> None:
        xperiods = SnapshotPeriods()
        self.assertEqual(xperiods.suffix_milliseconds["yearly"] * 1, xperiods.label_milliseconds("foo_yearly"))
        self.assertEqual(xperiods.suffix_milliseconds["yearly"] * 2, xperiods.label_milliseconds("foo_2yearly"))
        self.assertEqual(xperiods.suffix_milliseconds["yearly"] * 2, xperiods.label_milliseconds("_2yearly"))
        self.assertEqual(0, xperiods.label_milliseconds("_0yearly"))
        self.assertEqual(0, xperiods.label_milliseconds("2yearly"))
        self.assertEqual(0, xperiods.label_milliseconds("x"))
        self.assertEqual(0, xperiods.label_milliseconds(""))
        self.assertEqual(xperiods.suffix_milliseconds["monthly"], xperiods.label_milliseconds("foo_monthly"))
        self.assertEqual(xperiods.suffix_milliseconds["weekly"], xperiods.label_milliseconds("foo_weekly"))
        self.assertEqual(xperiods.suffix_milliseconds["daily"], xperiods.label_milliseconds("foo_daily"))
        self.assertEqual(xperiods.suffix_milliseconds["hourly"], xperiods.label_milliseconds("foo_hourly"))
        self.assertEqual(xperiods.suffix_milliseconds["minutely"] * 60, xperiods.label_milliseconds("foo_60minutely"))
        self.assertEqual(xperiods.suffix_milliseconds["minutely"] * 59, xperiods.label_milliseconds("foo_59minutely"))
        self.assertEqual(xperiods.suffix_milliseconds["secondly"], xperiods.label_milliseconds("foo_secondly"))
        self.assertEqual(xperiods.suffix_milliseconds["millisecondly"], xperiods.label_milliseconds("foo_millisecondly"))


#############################################################################
class TestCut(unittest.TestCase):

    def test_cut(self) -> None:
        lines = ["34\td1@s1", "56\td2@s2"]
        self.assertListEqual(["34", "56"], cut(1, lines=lines))
        self.assertListEqual(["d1@s1", "d2@s2"], cut(2, lines=lines))
        self.assertListEqual([], cut(1, lines=[]))
        self.assertListEqual([], cut(2, lines=[]))
        with self.assertRaises(ValueError):
            cut(0, lines=lines)

    def test_cut_field1(self) -> None:
        lines = ["a\tb\tc", "d\te\tf"]
        expected = ["a", "d"]
        self.assertEqual(expected, cut(field=1, lines=lines))

    def test_cut_field2(self) -> None:
        lines = ["a\tb\tc", "d\te\tf"]
        expected = ["b\tc", "e\tf"]
        self.assertEqual(expected, cut(field=2, lines=lines))

    def test_cut_invalid_field(self) -> None:
        lines = ["a\tb\tc"]
        with self.assertRaises(ValueError):
            cut(field=3, lines=lines)

    def test_cut_empty_lines(self) -> None:
        self.assertEqual([], cut(field=1, lines=[]))

    def test_cut_different_separator(self) -> None:
        lines = ["a,b,c", "d,e,f"]
        expected = ["a", "d"]
        self.assertEqual(expected, cut(field=1, separator=",", lines=lines))


#############################################################################
class TestDrain(unittest.TestCase):

    def test_drain(self) -> None:
        itr = iter(["foo", "bar"])
        drain(itr)
        with self.assertRaises(StopIteration):
            next(itr)


#############################################################################
class TestShuffleDict(unittest.TestCase):

    def test_shuffle_dict_preserves_items(self) -> None:
        d = {"a": 1, "b": 2, "c": 3}

        def fake_shuffle(lst: list) -> None:
            lst.reverse()

        with patch("bzfs_main.util.utils.random.SystemRandom.shuffle", side_effect=fake_shuffle) as mock_shuffle:
            result = shuffle_dict(d)
            self.assertEqual({"c": 3, "b": 2, "a": 1}, result)
            mock_shuffle.assert_called_once()

    def test_shuffle_dict_empty(self) -> None:
        self.assertEqual({}, shuffle_dict({}))


#############################################################################
class TestSortedDict(unittest.TestCase):

    def test_sorted_dict_empty_dictionary_returns_empty(self) -> None:
        result: dict[str, int] = sorted_dict({})
        self.assertEqual({}, result)

    def test_sorted_dict_single_key_value_pair_is_sorted_correctly(self) -> None:
        result: dict[str, int] = sorted_dict({"a": 1})
        self.assertEqual({"a": 1}, result)

    def test_sorted_dict_multiple_key_value_pairs_are_sorted_by_keys(self) -> None:
        result: dict[str, int] = sorted_dict({"b": 2, "a": 1, "c": 3})
        self.assertEqual({"a": 1, "b": 2, "c": 3}, result)

    def test_sorted_dict_with_numeric_keys_is_sorted_correctly(self) -> None:
        result: dict[int, str] = sorted_dict({3: "three", 1: "one", 2: "two"})
        self.assertEqual({1: "one", 2: "two", 3: "three"}, result)

    def test_sorted_dict_with_mixed_key_types_raises_error(self) -> None:
        with self.assertRaises(TypeError):
            sorted_dict({"a": 1, 2: "two"})


#############################################################################
class TestBinarySearch(unittest.TestCase):

    @staticmethod
    def insertion_point(result: int) -> int:
        return result if result >= 0 else -result - 1

    def test_found_first(self) -> None:
        a = [1, 3, 5, 7]
        self.assertEqual(binary_search(a, 1), 0)

    def test_found_last(self) -> None:
        a = [1, 3, 5, 7]
        self.assertEqual(binary_search(a, 7), 3)

    def test_found_middle(self) -> None:
        a = [1, 3, 5, 7]
        self.assertEqual(binary_search(a, 5), 2)

    def test_not_found_before_first(self) -> None:
        a = [1, 3, 5, 7]
        r = binary_search(a, 0)
        self.assertLess(r, 0)
        self.assertEqual(self.insertion_point(r), 0)

    def test_not_found_after_last(self) -> None:
        a = [1, 3, 5, 7]
        r = binary_search(a, 8)
        self.assertLess(r, 0)
        self.assertEqual(self.insertion_point(r), 4)

    def test_not_found_middle(self) -> None:
        a = [1, 3, 5, 7]
        r = binary_search(a, 4)
        self.assertLess(r, 0)
        self.assertEqual(self.insertion_point(r), 2)

    def test_duplicates_returns_leftmost(self) -> None:
        a = [1, 3, 3, 3, 5]
        self.assertEqual(binary_search(a, 3), 1)

    def test_empty_list(self) -> None:
        self.assertEqual(binary_search([], 42), -1)

    def test_string_found(self) -> None:
        a = ["ant", "bee", "cat", "dog"]
        self.assertEqual(binary_search(a, "bee"), 1)

    def test_string_not_found_middle(self) -> None:
        a = ["ant", "bee", "cat", "dog"]
        r = binary_search(a, "cow")  # between cat and dog
        self.assertLess(r, 0)
        self.assertEqual(self.insertion_point(r), 3)

    def test_string_not_found_before_first(self) -> None:
        a = ["ant", "bee", "cat", "dog"]
        r = binary_search(a, "aardvark")
        self.assertLess(r, 0)
        self.assertEqual(self.insertion_point(r), 0)


#############################################################################
class TestTail(unittest.TestCase):

    def test_tail(self) -> None:
        fd, file = tempfile.mkstemp(prefix="test_bzfs.tail_")
        os.write(fd, "line1\nline2\n".encode("utf-8"))
        os.close(fd)
        self.assertEqual(["line1\n", "line2\n"], list(tail(file, n=10)))
        self.assertEqual(["line1\n", "line2\n"], list(tail(file, n=2)))
        self.assertEqual(["line2\n"], list(tail(file, n=1)))
        self.assertEqual([], list(tail(file, n=0)))
        os.remove(file)
        self.assertEqual([], list(tail(file, n=2)))


#############################################################################
class TestGetHomeDirectory(unittest.TestCase):

    def test_get_home_directory(self) -> None:
        old_home = os.environ.get("HOME")
        if old_home is not None:
            self.assertEqual(old_home, get_home_directory())
            os.environ.pop("HOME")
            try:
                self.assertEqual(old_home, get_home_directory())
            finally:
                os.environ["HOME"] = old_home


#############################################################################
class TestHumanReadable(unittest.TestCase):

    def assert_human_readable_float(self, actual: float, expected: str) -> None:
        self.assertEqual(expected, human_readable_float(actual))
        self.assertEqual("-" + expected, human_readable_float(-actual))

    def test_human_readable_float_with_one_digit_before_decimal(self) -> None:
        self.assert_human_readable_float(3.14159, "3.14")
        self.assert_human_readable_float(5.0, "5")
        self.assert_human_readable_float(0.5, "0.5")
        self.assert_human_readable_float(0.499999, "0.5")
        self.assert_human_readable_float(3.1477, "3.15")
        self.assert_human_readable_float(1.999999, "2")
        self.assert_human_readable_float(2.5, "2.5")
        self.assert_human_readable_float(3.5, "3.5")

    def test_human_readable_float_with_two_digits_before_decimal(self) -> None:
        self.assert_human_readable_float(12.34, "12.3")
        self.assert_human_readable_float(12.0, "12")
        self.assert_human_readable_float(12.54, "12.5")
        self.assert_human_readable_float(12.56, "12.6")

    def test_human_readable_float_with_three_or_more_digits_before_decimal(self) -> None:
        self.assert_human_readable_float(123.456, "123")
        self.assert_human_readable_float(123.516, "124")
        self.assert_human_readable_float(1234.4678, "1234")
        self.assert_human_readable_float(1234.5678, "1235")
        self.assert_human_readable_float(12345.078, "12345")
        self.assert_human_readable_float(12345.678, "12346")
        self.assert_human_readable_float(999.99, "1000")

    def test_human_readable_float_with_zero(self) -> None:
        self.assertEqual("0", human_readable_float(0.0))
        self.assertEqual("0", human_readable_float(-0.0))
        self.assertEqual("0", human_readable_float(0.001))
        self.assertEqual("0", human_readable_float(-0.001))

    def test_human_readable_float_with_halfway_rounding_behavior(self) -> None:
        # For |n| < 10 => 2 decimals
        self.assert_human_readable_float(1.15, "1.15")
        self.assert_human_readable_float(1.25, "1.25")
        self.assert_human_readable_float(1.35, "1.35")
        self.assert_human_readable_float(1.45, "1.45")

        # 10.xx => one decimal
        eps = 1.0e-15
        self.assert_human_readable_float(10.15, "10.2")
        self.assert_human_readable_float(10.25, "10.2")
        self.assert_human_readable_float(10.35 + eps, "10.4")
        self.assert_human_readable_float(10.45, "10.4")

    def test_human_readable_bytes(self) -> None:
        self.assertEqual("0 B", human_readable_bytes(0))
        self.assertEqual("581 B", human_readable_bytes(0.567 * 1024**1))
        self.assertEqual("2 KiB", human_readable_bytes(2 * 1024**1))
        self.assertEqual("1 MiB", human_readable_bytes(1 * 1024**2))
        self.assertEqual("1 GiB", human_readable_bytes(1 * 1024**3))
        self.assertEqual("1 TiB", human_readable_bytes(1 * 1024**4))
        self.assertEqual("1 PiB", human_readable_bytes(1 * 1024**5))
        self.assertEqual("1 EiB", human_readable_bytes(1 * 1024**6))
        self.assertEqual("1 ZiB", human_readable_bytes(1 * 1024**7))
        self.assertEqual("1 YiB", human_readable_bytes(1 * 1024**8))
        self.assertEqual("1 RiB", human_readable_bytes(1 * 1024**9))
        self.assertEqual("1 QiB", human_readable_bytes(1 * 1024**10))
        self.assertEqual("1024 QiB", human_readable_bytes(1 * 1024**11))
        self.assertEqual("3 B", human_readable_bytes(2.567, precision=0))
        self.assertEqual("2.6 B", human_readable_bytes(2.567, precision=1))
        self.assertEqual("2.57 B", human_readable_bytes(2.567, precision=2))
        self.assertEqual("2.57 B", human_readable_bytes(2.567, precision=None))

    def test_human_readable_duration(self) -> None:
        ms = 1000_000
        self.assertEqual("0ns", human_readable_duration(0))
        self.assertEqual("3ns", human_readable_duration(3))
        self.assertEqual("3μs", human_readable_duration(3 * 1000))
        self.assertEqual("3ms", human_readable_duration(3 * ms))
        self.assertEqual("1s", human_readable_duration(1000 * ms))
        self.assertEqual("3s", human_readable_duration(3000 * ms))
        self.assertEqual("3m", human_readable_duration(3000 * 60 * ms))
        self.assertEqual("3h", human_readable_duration(3000 * 60 * 60 * ms))
        self.assertEqual("1.25d", human_readable_duration(3000 * 60 * 60 * 10 * ms))
        self.assertEqual("12.5d", human_readable_duration(3000 * 60 * 60 * 100 * ms))
        self.assertEqual("1250d", human_readable_duration(3000 * 60 * 60 * 10000 * ms))
        self.assertEqual("125ns", human_readable_duration(125))
        self.assertEqual("125μs", human_readable_duration(125 * 1000))
        self.assertEqual("125ms", human_readable_duration(125 * 1000 * 1000))
        self.assertEqual("2.08m", human_readable_duration(125 * 1000 * 1000 * 1000))

        self.assertEqual("0s", human_readable_duration(0, unit="s"))
        self.assertEqual("3s", human_readable_duration(3, unit="s"))
        self.assertEqual("3m", human_readable_duration(3 * 60, unit="s"))
        self.assertEqual("0h", human_readable_duration(0, unit="h"))
        self.assertEqual("3h", human_readable_duration(3, unit="h"))
        self.assertEqual("7.5d", human_readable_duration(3 * 60, unit="h"))
        self.assertEqual("0.3ns", human_readable_duration(0.3))
        self.assertEqual("300ns", human_readable_duration(0.3, unit="μs"))
        self.assertEqual("300μs", human_readable_duration(0.3, unit="ms"))
        self.assertEqual("300ms", human_readable_duration(0.3, unit="s"))
        self.assertEqual("18s", human_readable_duration(0.3, unit="m"))
        self.assertEqual("18m", human_readable_duration(0.3, unit="h"))
        self.assertEqual("7.2h", human_readable_duration(0.3, unit="d"))
        self.assertEqual("259ns", human_readable_duration(3 / 1000_000_000_000, unit="d"))
        self.assertEqual("0.26ns", human_readable_duration(3 / 1000_000_000_000_000, unit="d"))
        with self.assertRaises(ValueError):
            human_readable_duration(3, unit="hhh")  # invalid unit

        self.assertEqual("3ns", human_readable_duration(2.567, precision=0))
        self.assertEqual("2.6ns", human_readable_duration(2.567, precision=1))
        self.assertEqual("2.57ns", human_readable_duration(2.567, precision=2))
        self.assertEqual("2.57ns", human_readable_duration(2.567, precision=None))

    def test_percent(self) -> None:
        self.assertEqual("3=30%", percent(3, 10))
        self.assertEqual("0=inf%", percent(0, 0))
        self.assertEqual("3/10=30%", percent(3, 10, print_total=True))
        self.assertEqual("0/0=inf%", percent(0, 0, print_total=True))


#############################################################################
class TestOpenNoFollow(unittest.TestCase):

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.real_path = os.path.join(self.tmpdir, "file.txt")
        with open(self.real_path, "w", encoding="utf-8") as f:
            f.write("hello")
        self.symlink_path = os.path.join(self.tmpdir, "link.txt")
        os.symlink(self.real_path, self.symlink_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)

    def test_read_text(self) -> None:
        with open_nofollow(self.real_path, "r", encoding="utf-8") as f:
            data = f.read()
        self.assertEqual("hello", data)

    def test_read_binary(self) -> None:
        with open(self.real_path, "rb") as f:
            raw = f.read()
        with open_nofollow(self.real_path, "rb") as f:
            data = f.read()
        self.assertEqual(raw, data)

    def test_write_truncate(self) -> None:
        with open_nofollow(self.real_path, "w", encoding="utf-8") as f:
            f.write("world")
        with open(self.real_path, "r", encoding="utf-8") as f:
            self.assertEqual("world", f.read())

    def test_append(self) -> None:
        with open_nofollow(self.real_path, "a", encoding="utf-8") as f:
            f.write(" world")
        with open(self.real_path, "r", encoding="utf-8") as f:
            self.assertEqual("hello world", f.read())

    def test_exclusive_create(self) -> None:
        new_path = os.path.join(self.tmpdir, "new.txt")
        f = open_nofollow(new_path, "x", encoding="utf-8")
        f.write("new")
        f.close()
        # second open should fail
        with self.assertRaises(FileExistsError):
            open_nofollow(new_path, "x")

    def test_plus_mode(self) -> None:
        with open_nofollow(self.real_path, "r+") as f:
            content = f.read()
            self.assertEqual("hello", content)
            f.seek(0)
            f.write("HELLO")
        with open(self.real_path, "r", encoding="utf-8") as f:
            self.assertEqual("HELLO", f.read())

    def test_symlink_blocked(self) -> None:
        with self.assertRaises(OSError) as cm:
            open_nofollow(self.symlink_path, "r")
        self.assertIn(cm.exception.errno, (errno.ELOOP, errno.EMLINK))

    def test_nonexistent_read(self) -> None:
        missing = os.path.join(self.tmpdir, "missing.txt")
        with self.assertRaises(FileNotFoundError):
            open_nofollow(missing, "r")

    def test_permission_bits(self) -> None:
        # set umask to zero temporarily so we can observe raw permission bits
        old_umask = os.umask(0)
        try:
            new_path = os.path.join(self.tmpdir, "perm.txt")
            open_nofollow(new_path, "w", perm=0o600).close()
            mode = stat.S_IMODE(os.stat(new_path).st_mode)
            self.assertEqual(0o600, mode)
        finally:
            os.umask(old_umask)

    def test_invalid_empty_mode(self) -> None:
        with self.assertRaises(ValueError):
            open_nofollow(self.real_path, "")

    def test_invalid_mode(self) -> None:
        with self.assertRaises(ValueError):
            open_nofollow(self.real_path, "z")

    def test_fdopen_failure_closes_fd(self) -> None:
        err = RuntimeError("fdopen boom")
        orig_close = os.close
        with patch("os.fdopen", side_effect=err) as m_fdopen, patch("os.close", side_effect=orig_close) as m_close:
            with self.assertRaises(RuntimeError):
                open_nofollow(self.real_path, "r")
        m_fdopen.assert_called_once()
        m_close.assert_called_once()

    def test_check_owner_skipped(self) -> None:
        """check_owner=False should skip ownership verification."""
        with patch("os.fstat", side_effect=AssertionError("should not call")) as m_fstat:
            with open_nofollow(self.real_path, "r", check_owner=False, encoding="utf-8") as f:
                self.assertEqual("hello", f.read())
        m_fstat.assert_not_called()

    def test_owner_mismatch_raises_and_closes_fd(self) -> None:
        class Stat:
            st_uid = os.geteuid() + 1

        orig_close = os.close
        with patch("os.fstat", return_value=Stat()), patch("os.close", side_effect=orig_close) as m_close:
            with self.assertRaises(PermissionError):
                open_nofollow(self.real_path, "r")
        m_close.assert_called_once()

    def test_close_error_ignored(self) -> None:
        err = RuntimeError("boom")
        orig_close = os.close

        def failing_close(fd: int) -> None:
            orig_close(fd)
            raise OSError("close fail")

        with patch("os.fdopen", side_effect=err), patch("os.close", side_effect=failing_close) as m_close:
            with self.assertRaises(RuntimeError):
                open_nofollow(self.real_path, "r")
        m_close.assert_called_once()


#############################################################################
class TestCloseQuietly(unittest.TestCase):

    def test_closes_valid_fd(self) -> None:
        """Closes a valid FD so subsequent close raises OSError (EBADF)."""
        r, w = os.pipe()
        try:
            close_quietly(w)
            with self.assertRaises(OSError):
                os.close(w)  # already closed
        finally:
            close_quietly(r)

    def test_ignores_negative_fd(self) -> None:
        """Negative FD is a no-op and must not raise."""
        close_quietly(-1)

    def test_swallow_error_on_already_closed_fd(self) -> None:
        """Closing an already-closed FD must not propagate exceptions."""
        r, w = os.pipe()
        try:
            os.close(w)
            close_quietly(w)  # should not raise
        finally:
            close_quietly(r)

    def test_does_not_affect_duplicated_fd(self) -> None:
        """Closing one FD must not close its duplicate; IO via duplicate works."""
        r, w = os.pipe()
        d = os.dup(w)
        try:
            close_quietly(w)
            os.write(d, b"X")  # duplicate remains valid
            data = os.read(r, 1)
            self.assertEqual(b"X", data)
        finally:
            close_quietly(d)
            close_quietly(r)


#############################################################################
class TestValidateFilePermissions(unittest.TestCase):

    def test_ok_when_owner_and_mode_match(self) -> None:
        with tempfile.NamedTemporaryFile(delete=True) as f:
            os.chmod(f.name, FILE_PERMISSIONS)
            validate_file_permissions(f.name, mode=FILE_PERMISSIONS)

    def test_owner_mismatch_raises(self) -> None:
        with tempfile.NamedTemporaryFile(delete=True) as f:
            os.chmod(f.name, FILE_PERMISSIONS)
            real_uid = os.stat(f.name).st_uid
            fake_euid = real_uid + 1
            with patch("os.geteuid", return_value=fake_euid):
                with self.assertRaises(SystemExit) as cm, suppress_output():
                    validate_file_permissions(f.name, mode=FILE_PERMISSIONS)
        msg = str(cm.exception)
        self.assertIn(str(real_uid), msg)
        self.assertIn(str(fake_euid), msg)
        self.assertIn("is owned by uid", msg)

    def test_mode_mismatch_raises_and_reports_bits_and_text(self) -> None:
        with tempfile.NamedTemporaryFile(delete=True) as f:
            # os.chmod(f.name, 0o600)
            # expected_mode = 0o700
            os.chmod(f.name, FILE_PERMISSIONS)
            expected_mode = DIR_PERMISSIONS
            with self.assertRaises(SystemExit) as cm, suppress_output():
                validate_file_permissions(f.name, mode=expected_mode)
        msg = str(cm.exception)
        actual = FILE_PERMISSIONS  # aka 0o600
        # Ensure message mentions both numeric and textual representations
        self.assertIn(f"{actual:03o}", msg)
        self.assertIn("rw-------", msg)  # actual 0o600
        self.assertIn(f"{expected_mode:03o}", msg)
        self.assertIn("rwx------", msg)  # expected 0o700

    def test_symlink_path_is_rejected(self) -> None:
        """Validation must not follow symlinks; lstat should reject symlink path.

        Creates a real directory and a symlink pointing to it. Validation on the symlink path must raise, because permissions
        are checked on the symlink itself (lstat), not on the target.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            real_dir = os.path.join(tmpdir, "real")
            os.mkdir(real_dir)
            os.chmod(real_dir, DIR_PERMISSIONS)
            link_dir = os.path.join(tmpdir, "link")
            os.symlink(real_dir, link_dir)
            with self.assertRaises(SystemExit):
                validate_file_permissions(link_dir, mode=DIR_PERMISSIONS)


#############################################################################
class TestFindMatch(unittest.TestCase):

    def test_basic(self) -> None:
        def condition(arg: str) -> bool:
            return arg.startswith("-")

        lst: list[str] = ["a", "b", "-c", "d"]
        self.assert_find_match(2, lst, condition)

        self.assert_find_match(2, lst, condition, -3)
        self.assert_find_match(2, lst, condition, -2)
        self.assert_find_match(-1, lst, condition, -1)
        self.assert_find_match(2, lst, condition, 0)
        self.assert_find_match(2, lst, condition, 1)
        self.assert_find_match(2, lst, condition, 2)
        self.assert_find_match(-1, lst, condition, 3)
        self.assert_find_match(-1, lst, condition, 4)
        self.assert_find_match(-1, lst, condition, 5)

        self.assert_find_match(-1, lst, condition, end=-3)
        self.assert_find_match(-1, lst, condition, end=-2)
        self.assert_find_match(2, lst, condition, end=-1)
        self.assert_find_match(-1, lst, condition, end=0)
        self.assert_find_match(-1, lst, condition, end=1)
        self.assert_find_match(-1, lst, condition, end=2)
        self.assert_find_match(2, lst, condition, end=3)
        self.assert_find_match(2, lst, condition, end=4)
        self.assert_find_match(2, lst, condition, end=5)
        self.assert_find_match(2, lst, condition, end=6)

        self.assert_find_match(2, lst, condition, start=2, end=-1)
        self.assert_find_match(-1, lst, condition, start=2, end=-2)
        self.assert_find_match(-1, lst, condition, start=3, end=-1)
        self.assert_find_match(-1, lst, condition, start=3, end=-1, raises=None)
        self.assert_find_match(-1, lst, condition, start=3, end=-1, raises=False)

        self.assert_find_match(2, lst, condition, raises=None)
        self.assert_find_match(2, lst, condition, raises=False)
        self.assert_find_match(2, lst, condition, raises=True)
        with self.assertRaises(ValueError):
            find_match(lst, condition, start=0, end=2, raises=True)
        x = 2
        with self.assertRaises(ValueError) as e:
            find_match(lst, condition, start=0, end=2, raises=f"foo: {x}")
        self.assertEqual(f"foo: {x}", str(e.exception))
        with self.assertRaises(ValueError) as e:
            find_match(lst, condition, start=0, end=2, raises=lambda: f"foo: {x}")
        self.assertEqual(f"foo: {x}", str(e.exception))
        with self.assertRaises(ValueError) as e:
            find_match(lst, condition, start=0, end=2, raises="")
        self.assertEqual("", str(e.exception))

        lst = ["-c"]
        self.assert_find_match(0, lst, condition)
        self.assert_find_match(0, lst, condition, -1)
        self.assert_find_match(0, lst, condition, 0)
        self.assert_find_match(-1, lst, condition, 1)

        self.assert_find_match(-1, lst, condition, end=-1)
        self.assert_find_match(-1, lst, condition, end=0)
        self.assert_find_match(0, lst, condition, end=1)

        self.assert_find_match(-1, lst, condition, start=2, end=-1)
        self.assert_find_match(-1, lst, condition, start=2, end=-2)
        self.assert_find_match(-1, lst, condition, start=3, end=-1)

        lst = []
        self.assert_find_match(-1, lst, condition)
        self.assert_find_match(-1, lst, condition, -1)
        self.assert_find_match(-1, lst, condition, 0)
        self.assert_find_match(-1, lst, condition, 1)

        self.assert_find_match(-1, lst, condition, end=-1)
        self.assert_find_match(-1, lst, condition, end=0)
        self.assert_find_match(-1, lst, condition, end=1)

        self.assert_find_match(-1, lst, condition, start=2, end=-1)
        self.assert_find_match(-1, lst, condition, start=2, end=-2)
        self.assert_find_match(-1, lst, condition, start=3, end=-1)

        lst = ["a", "b", "-c", "-d"]
        self.assertEqual(2, find_match(lst, condition, start=None, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=None, end=None, reverse=True))
        self.assertEqual(2, find_match(lst, condition, start=2, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=2, end=None, reverse=True))
        self.assertEqual(3, find_match(lst, condition, start=3, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=3, end=None, reverse=True))

        self.assertEqual(2, find_match(lst, condition, start=0, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=0, end=None, reverse=True))
        self.assertEqual(3, find_match(lst, condition, start=-1, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=-1, end=None, reverse=True))
        self.assertEqual(2, find_match(lst, condition, start=-2, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=-2, end=None, reverse=True))
        self.assertEqual(2, find_match(lst, condition, start=-3, end=None, reverse=False))
        self.assertEqual(3, find_match(lst, condition, start=-3, end=None, reverse=True))

        lst = ["-a", "-b", "c", "d"]
        self.assertEqual(0, find_match(lst, condition, end=-1, reverse=False))
        self.assertEqual(1, find_match(lst, condition, end=-1, reverse=True))
        self.assertEqual(0, find_match(lst, condition, end=-2, reverse=False))
        self.assertEqual(1, find_match(lst, condition, end=-2, reverse=True))
        self.assertEqual(0, find_match(lst, condition, end=-3, reverse=False))
        self.assertEqual(0, find_match(lst, condition, end=-3, reverse=True))
        self.assertEqual(-1, find_match(lst, condition, end=-4, reverse=False))
        self.assertEqual(-1, find_match(lst, condition, end=-4, reverse=True))

        lst = ["a", "-b", "-c", "d"]
        self.assertEqual(1, find_match(lst, condition, start=1, end=-1, reverse=False))
        self.assertEqual(2, find_match(lst, condition, start=1, end=-1, reverse=True))
        self.assertEqual(1, find_match(lst, condition, start=1, end=-2, reverse=False))
        self.assertEqual(1, find_match(lst, condition, start=1, end=-2, reverse=True))

    def assert_find_match(
        self,
        expected: int,
        lst: Sequence[str],
        condition: Callable[[str], bool],
        start: int | None = None,
        end: int | None = None,
        raises: bool | str | Callable[[], str] | None = False,
    ) -> None:
        raise_arg = cast(Union[bool, str, Callable[[], str]], raises)
        self.assertEqual(expected, find_match(lst, condition, start=start, end=end, reverse=False, raises=raise_arg))
        self.assertEqual(expected, find_match(lst, condition, start=start, end=end, reverse=True, raises=raise_arg))


#############################################################################
class TestReplaceCapturingGroups(unittest.TestCase):

    def test_basic_case(self) -> None:
        self.assertEqual("(?:abc)", replace_capturing_groups_with_non_capturing_groups("(abc)"))

    def test_nested_groups(self) -> None:
        self.assertEqual("(?:a(?:bc)d)", replace_capturing_groups_with_non_capturing_groups("(a(bc)d)"))

    def test_preceding_backslash(self) -> None:
        self.assertEqual("\\(abc)", replace_capturing_groups_with_non_capturing_groups("\\(abc)"))

    def test_group_starting_with_question_mark(self) -> None:
        self.assertEqual("(?abc)", replace_capturing_groups_with_non_capturing_groups("(?abc)"))

    def test_multiple_groups(self) -> None:
        self.assertEqual("(?:abc)(?:def)", replace_capturing_groups_with_non_capturing_groups("(abc)(def)"))

    def test_mixed_cases(self) -> None:
        self.assertEqual(
            "a(?:bc\\(de)f(?:gh)?i",
            replace_capturing_groups_with_non_capturing_groups("a(bc\\(de)f(gh)?i"),
        )

    def test_empty_group(self) -> None:
        self.assertEqual("(?:)", replace_capturing_groups_with_non_capturing_groups("()"))

    def test_named_capturing_groups(self) -> None:
        testcases: dict[str, str] = {
            "(?P<name>abc)": "(?:abc)",
            "(?P<name1>(?P<name2>abc))": "(?:(?:abc))",
            "(?P<name1>ab(?P<name2>c))": "(?:ab(?:c))",
            "(?P<n>abc)": "(?:abc)",
            "(?P<n789>abc)": "(?:abc)",
            "(?P<789>abc)": "(?P<789>abc)",  # first char of name of named capturing group must not be a digit
            "(?P<789>(?P<name1>a)bc)": "(?P<789>(?:a)bc)",  # first char of name of named capturing group must not be a digit
            "(?P<789>(?P<345>a)bc)": "(?P<789>(?P<345>a)bc)",  # ... must not be a digit
            "(?": "(?",  # not a valid capturing group
            "(?P": "(?P",  # not a valid capturing group
        }
        for i, (pattern, expected_result) in enumerate(testcases.items()):
            with self.subTest(i=i):
                self.assertEqual(expected_result, replace_capturing_groups_with_non_capturing_groups(pattern))

    def test_group_with_non_capturing_group(self) -> None:
        self.assertEqual("(?:a(?:bc)d)", replace_capturing_groups_with_non_capturing_groups("(a(?:bc)d)"))

    def test_group_with_lookahead(self) -> None:
        self.assertEqual("(?:abc)(?=def)", replace_capturing_groups_with_non_capturing_groups("(abc)(?=def)"))

    def test_group_with_lookbehind(self) -> None:
        self.assertEqual("(?<=abc)(?:def)", replace_capturing_groups_with_non_capturing_groups("(?<=abc)(def)"))

    def test_escaped_characters(self) -> None:
        pattern = re.escape("(abc)")
        self.assertEqual(pattern, replace_capturing_groups_with_non_capturing_groups(pattern))

    def test_complex_pattern_with_escape(self) -> None:
        complex_pattern = re.escape("(a[b]c{d}e|f.g)")
        self.assertEqual(complex_pattern, replace_capturing_groups_with_non_capturing_groups(complex_pattern))

    def test_complex_pattern(self) -> None:
        complex_pattern = "(a[b]c{d}e|f.g)(h(i|j)k)?(\\(l\\))"
        # Conservative fallback: presence of [ and ( anywhere => skip rewrite.
        self.assertEqual(complex_pattern, replace_capturing_groups_with_non_capturing_groups(complex_pattern))

    def test_example(self) -> None:
        pattern = "(.*/)?tmp(foo|bar)(?!public)("
        expected_result = "(?:.*/)?tmp(?:foo|bar)(?!public)("
        self.assertEqual(expected_result, replace_capturing_groups_with_non_capturing_groups(pattern))

        pattern = "(.*/)?tmp(foo|bar)(?!public)\\("
        expected_result = "(?:.*/)?tmp(?:foo|bar)(?!public)\\("
        self.assertEqual(expected_result, replace_capturing_groups_with_non_capturing_groups(pattern))

        pattern += ")"
        expected_result += ")"
        self.assertEqual(expected_result, replace_capturing_groups_with_non_capturing_groups(pattern))

    def test_many_cases(self) -> None:
        testcases: dict[str, str] = {
            "": "",
            "a": "a",
            "ab": "ab",
            "abc": "abc",
            "(a)": "(?:a)",
            "(ab)": "(?:ab)",
            "(abc)": "(?:abc)",
            "a(b)c": "a(?:b)c",
            "(a)(b)": "(?:a)(?:b)",  # consecutive non-empty groups
            "(a(b(c)))": "(?:a(?:b(?:c)))",  # triple-nested groups
            "(((abc)))": "(?:(?:(?:abc)))",  # triple-nested groups
            "(((a(b(c)))))": "(?:(?:(?:a(?:b(?:c)))))",  # six-nested groups
            "(?abc)": "(?abc)",  # special groups
            "(?:abc)": "(?:abc)",  # special groups
            "(?=abc)": "(?=abc)",  # special groups
            "(": "(",  # single opening parenthesis
            "()": "(?:)",  # empty group
            "()()": "(?:)(?:)",  # consecutive empty groups
            "a\\(b)c": "a\\(b)c",  # escaped parentheses
            r"a\\(b)": r"a\\(b)",  # double escaped parentheses
            r"(a)\(b)": r"(?:a)\(b)",  # group followed immediately by an escaped parenthesis
            r"(?=a)(b)\(c)(?:d)(e)": r"(?=a)(?:b)\(c)(?:d)(?:e)",  # complex mix of groups
            "(你好)": "(?:你好)",  # group with Unicode characters
            "a(?:b)": "a(?:b)",  # string ending with a non-capturing group
            "([*+?^])": "([*+?^])",  # group containing only special regex metachars; presence of [ and ( => skip rewrite
            r"\(a)": r"\(a)",  # one backslash -> escaped -> NO change
            r"\\(a)": r"\\(a)",  # double-escaped parentheses -> escaped -> NO change
            r"\\\\(a)": r"\\\\(a)",  # quadruple-escaped parentheses -> escaped -> NO change
            r"(a)\(b)(?:c)(?=d)(e)": r"(?:a)\(b)(?:c)(?=d)(?:e)",  # mixed complex case
            # Conservative fallback: presence of [ and ( anywhere => skip rewrite.
            "(a[b]c{d}e|f.g)(h(i|j)k)?(\\(l\\))": "(a[b]c{d}e|f.g)(h(i|j)k)?(\\(l\\))",  # mixed complex case
        }
        for i, (pattern, expected_result) in enumerate(testcases.items()):
            with self.subTest(i=i):
                self.assertEqual(expected_result, replace_capturing_groups_with_non_capturing_groups(pattern))

    def test_char_class_with_parentheses_remains_unchanged(self) -> None:
        # If a character class contains parentheses, rewriting can corrupt the regex.
        # Since the rewrite is a perf-only optimization, leave such patterns unchanged.
        patterns = [
            "[()]",
            "a[()]b",
            "[(]",
            "[a()b]",
        ]
        for i, pattern in enumerate(patterns):
            with self.subTest(i=i):
                self.assertEqual(pattern, replace_capturing_groups_with_non_capturing_groups(pattern))

    def test_char_class_left_bracket_escapes_remain_unchanged(self) -> None:
        # Escaped forms of '[' that the regex engine recognizes should also trigger the conservative fallback.
        patterns = [
            "\\N{LEFT SQUARE BRACKET}()\\]",  # named Unicode escape for '[' expressed literally
            r"\x5b()\]",  # hex escape for '[' (lowercase)
            r"\x5B()\]",  # hex escape for '[' (uppercase)
            "\\u005b()\\]",  # 4-digit Unicode escape for '[' (lowercase), expressed literally
            "\\u005B()\\]",  # 4-digit Unicode escape for '[' (uppercase), expressed literally
            "\\U0000005b()\\]",  # 8-digit Unicode escape for '[' (lowercase), expressed literally
            "\\U0000005B()\\]",  # 8-digit Unicode escape for '[' (uppercase), expressed literally
            r"\133()\]",  # octal escape for '['
        ]
        for i, pattern in enumerate(patterns):
            with self.subTest(i=i):
                self.assertEqual(pattern, replace_capturing_groups_with_non_capturing_groups(pattern))


#############################################################################
class TestSubprocessRun(unittest.TestCase):

    def test_successful_command(self) -> None:
        result = subprocess_run(["true"], stdout=PIPE, stderr=subprocess.PIPE)
        self.assertEqual(0, result.returncode)
        self.assertEqual(b"", result.stdout)
        self.assertEqual(b"", result.stderr)

    def test_failing_command_no_check(self) -> None:
        result = subprocess_run(["false"], stdout=PIPE, stderr=PIPE)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(b"", result.stdout)
        self.assertEqual(b"", result.stderr)

    def test_failing_command_with_check(self) -> None:
        with self.assertRaises(subprocess.CalledProcessError) as context:
            subprocess_run(["false"], stdout=PIPE, stderr=PIPE, check=True)
        self.assertIsInstance(context.exception, subprocess.CalledProcessError)
        self.assertIsInstance(context.exception.returncode, int)
        self.assertTrue(context.exception.returncode != 0)

    def test_input_bytes(self) -> None:
        result = subprocess_run(["cat"], input=b"hello", stdout=PIPE, stderr=PIPE)
        self.assertEqual(b"hello", result.stdout)

    def test_valueerror_input_and_stdin(self) -> None:
        with self.assertRaises(ValueError):
            subprocess_run(["cat"], input=b"hello", stdin=PIPE, stdout=PIPE, stderr=PIPE)

    def test_timeout_expired(self) -> None:
        with self.assertRaises(subprocess.TimeoutExpired) as context:
            subprocess_run(["sleep", "1"], timeout=0.01, stdout=PIPE, stderr=PIPE)
        self.assertIsInstance(context.exception, subprocess.TimeoutExpired)
        self.assertEqual(["sleep", "1"], context.exception.cmd)

    def test_keyboardinterrupt_signal(self) -> None:
        old_handler = signal.signal(signal.SIGINT, signal.default_int_handler)  # install a handler that ignores SIGINT
        try:

            def send_sigint_to_sleep_cli() -> None:
                time.sleep(0.1)  # ensure sleep CLI has started before we kill it
                os.kill(os.getpid(), signal.SIGINT)  # send SIGINT to all members of the process group, including `sleep` CLI

            thread = threading.Thread(target=send_sigint_to_sleep_cli)
            thread.start()
            with self.assertRaises(KeyboardInterrupt):
                subprocess_run(["sleep", "1"])
            thread.join()
        finally:
            signal.signal(signal.SIGINT, old_handler)  # restore original signal handler


#############################################################################
class TestSubprocessRunWithSubprocesses(unittest.TestCase):

    def test_unregisters_on_success(self) -> None:
        """Registers a PID and ensures it is unregistered on success."""
        sp = Subprocesses()
        result = subprocess_run(["true"], stdout=PIPE, stderr=PIPE, subprocesses=sp)
        self.assertEqual(0, result.returncode)
        # After completion, no PIDs should remain tracked
        self.assertEqual({}, sp._child_pids)

    def test_unregisters_on_check_exception(self) -> None:
        """Ensures PID is unregistered when check=True raises an error."""
        sp = Subprocesses()
        with self.assertRaises(subprocess.CalledProcessError):
            subprocess_run(["false"], stdout=PIPE, stderr=PIPE, check=True, subprocesses=sp)
        # Ensure no PIDs remain tracked after exception
        self.assertEqual({}, sp._child_pids)

    def test_timeout_terminates_subtree_and_unregisters(self) -> None:
        """On timeout, terminates subtree and unregisters the PID."""
        sp = Subprocesses()
        with patch("bzfs_main.util.utils.terminate_process_subtree") as mock_term:
            with self.assertRaises(subprocess.TimeoutExpired):
                subprocess_run(["sleep", "1"], timeout=0.01, stdout=PIPE, stderr=PIPE, subprocesses=sp)
            self.assertTrue(mock_term.called)
        # Ensure no PIDs remain tracked after timeout
        self.assertEqual({}, sp._child_pids)

    def test_terminate_all_kills_running_child(self) -> None:
        """terminate_process_subtrees kills child and clears tracking."""
        sp = Subprocesses()
        result: subprocess.CompletedProcess | None = None

        def run_sleep() -> None:
            nonlocal result
            result = sp.subprocess_run(["sleep", "5"], stdout=PIPE, stderr=PIPE)

        t = threading.Thread(target=run_sleep)
        t.start()
        time.sleep(0.05)  # ensure child started and registered
        self.assertEqual(1, len(sp._child_pids))
        sp.terminate_process_subtrees()
        self.assertEqual({}, sp._child_pids)
        t.join(timeout=1.0)
        self.assertFalse(t.is_alive(), "Expected sleep subprocess to be terminated by terminate_all()")
        self.assertIsNotNone(result)
        assert result is not None
        self.assertNotEqual(0, result.returncode)


#############################################################################
class TestSubprocessRunLogging(unittest.TestCase):
    """Tests logging behavior of subprocess_run() finalization block.

    Covers success, failure, timeout statuses and argv formatting via positional args, kwargs, tuple and string (with lstrip)
    while ensuring deterministic elapsed time formatting using patched monotonic_ns.
    """

    class _CapturingHandler(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.records: list[str] = []
            self.setFormatter(logging.Formatter("%(message)s"))

        def emit(self, record: logging.LogRecord) -> None:
            self.records.append(self.format(record))  # capture the emitted records for later inspection

    def _make_logger(self) -> tuple[Logger, _CapturingHandler]:
        logger = logging.getLogger(self.id())
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        handler = self._CapturingHandler()
        logger.handlers.clear()
        logger.addHandler(handler)
        self.addCleanup(logger.handlers.clear)
        return logger, handler

    def test_logs_success_with_positional_args_list(self) -> None:
        log, handler = self._make_logger()
        with patch("bzfs_main.util.utils.time.monotonic_ns", side_effect=[0, 1_230_000]):
            result = subprocess_run(["true"], stdout=PIPE, stderr=PIPE, log=log, loglevel=logging.INFO)
        self.assertEqual(0, result.returncode)
        self.assertEqual(["Executed [success] [1.23ms]: true"], handler.records)

    def test_logs_failure_without_check(self) -> None:
        log, handler = self._make_logger()
        with patch("bzfs_main.util.utils.time.monotonic_ns", side_effect=[0, 12_340_000]):
            result = subprocess_run(["false"], stdout=PIPE, stderr=PIPE, log=log, loglevel=logging.INFO)
        self.assertNotEqual(0, result.returncode)
        # For values between 10 and 100, human_readable_float uses 1 decimal
        self.assertEqual(["Executed [failure] [12.3ms]: false"], handler.records)

    def test_logs_failure_with_check_exception(self) -> None:
        log, handler = self._make_logger()
        with patch("bzfs_main.util.utils.time.monotonic_ns", side_effect=[0, 9_990_000]):
            with self.assertRaises(subprocess.CalledProcessError):
                subprocess_run(["false"], stdout=PIPE, stderr=PIPE, check=True, log=log, loglevel=logging.INFO)
        self.assertEqual(["Executed [failure] [9.99ms]: false"], handler.records)

    def test_logs_timeout_status(self) -> None:
        log, handler = self._make_logger()
        with patch("bzfs_main.util.utils.time.monotonic_ns", side_effect=[0, 12_350_000]):
            with self.assertRaises(subprocess.TimeoutExpired):
                subprocess_run(["sleep", "1"], timeout=0.01, stdout=PIPE, stderr=PIPE, log=log, loglevel=logging.INFO)
        # 12.35 is not exactly representable in binary; formatting with one decimal may yield 12.3
        self.assertEqual(["Executed [timeout] [12.3ms]: sleep 1"], handler.records)

    def test_logs_argv_from_kwargs_list(self) -> None:
        log, handler = self._make_logger()
        with patch("bzfs_main.util.utils.time.monotonic_ns", side_effect=[0, 500_000]):
            result = subprocess_run(stdout=PIPE, stderr=PIPE, args=["true"], log=log, loglevel=logging.INFO)
        self.assertEqual(0, result.returncode)
        self.assertEqual(["Executed [success] [0.5ms]: true"], handler.records)

    def test_logs_tuple_args_and_string_with_lstrip(self) -> None:
        # Tuple args formatting
        log1, handler1 = self._make_logger()
        with patch("bzfs_main.util.utils.time.monotonic_ns", side_effect=[0, 2_000_000]):
            result1 = subprocess_run(args=("true",), stdout=PIPE, stderr=PIPE, log=log1, loglevel=logging.INFO)
        self.assertEqual(0, result1.returncode)
        self.assertEqual(["Executed [success] [2ms]: true"], handler1.records)

        # String args with leading whitespace should not be lstrip()'d in logs
        log2, handler2 = self._make_logger()
        with patch("bzfs_main.util.utils.time.monotonic_ns", side_effect=[0, 1_500_000]):
            result2 = subprocess_run(
                args="  echo foo", shell=True, stdout=PIPE, stderr=PIPE, log=log2, loglevel=logging.INFO
            )
        self.assertEqual(0, result2.returncode)
        self.assertEqual(["Executed [success] [1.5ms]:   echo foo"], handler2.records)

    def test_no_logging_when_level_disabled(self) -> None:
        logger = logging.getLogger(self.id() + ":disabled")
        logger.setLevel(logging.ERROR)  # INFO not enabled
        logger.propagate = False
        handler = self._CapturingHandler()
        logger.handlers.clear()
        logger.addHandler(handler)
        self.addCleanup(logger.handlers.clear)

        result = subprocess_run(["true"], stdout=PIPE, stderr=PIPE, log=logger, loglevel=logging.INFO)
        self.assertEqual(0, result.returncode)
        self.assertEqual([], handler.records)

    def test_logs_failure_when_popen_raises_oserror(self) -> None:
        """Logs a failure when Popen raises OSError before starting the process (exitcode stays None)."""
        log, handler = self._make_logger()
        with patch("bzfs_main.util.utils.subprocess.Popen", side_effect=OSError("boom")):
            with patch("bzfs_main.util.utils.time.monotonic_ns", side_effect=[0, 1_000_000]):
                with self.assertRaises(OSError):
                    subprocess_run(["true"], stdout=PIPE, stderr=PIPE, log=log, loglevel=logging.INFO)
        self.assertEqual(["Executed [failure] [1ms]: true"], handler.records)


#############################################################################
class TestPIDExists(unittest.TestCase):

    def test_pid_exists(self) -> None:
        self.assertTrue(pid_exists(os.getpid()))
        self.assertFalse(pid_exists(-1))
        self.assertFalse(pid_exists(0))
        # This fake PID is extremely unlikely to be alive because it is orders of magnitude higher than the typical
        # range of process IDs on Unix-like systems:
        fake_pid = 2**31 - 1  # 2147483647
        self.assertFalse(pid_exists(fake_pid))

        # Process 1 is typically owned by root; if not running as root, this should return True because it raises EPERM
        if os.getuid() != 0:
            self.assertTrue(pid_exists(1))

    @patch("os.kill")
    def test_pid_exists_with_unexpected_oserror_returns_none(self, mock_kill: MagicMock) -> None:
        # Simulate an unexpected OSError (e.g., EINVAL) and verify that pid_exists returns None.
        err = OSError()
        err.errno = errno.EINVAL
        mock_kill.side_effect = err
        self.assertIsNone(pid_exists(1234))


#############################################################################
class TestTerminateProcessSubtree(unittest.TestCase):

    def setUp(self) -> None:
        self.children: list[subprocess.Popen[Any]] = []

    def tearDown(self) -> None:
        for child in self.children:
            try:
                child.kill()
            except OSError:
                pass
        self.children = []

    def test_get_descendant_processes(self) -> None:
        try:
            subprocess.run(["ps", "-p", str(os.getpid()), "-o", "pid="], stdin=DEVNULL, stdout=PIPE, stderr=PIPE, check=True)
        except PermissionError:
            self.skipTest("ps not permitted/available in this sandboxed environment")

        child = subprocess.Popen(["sleep", "1"])
        self.children.append(child)
        time.sleep(0.1)
        descendants = _get_descendant_processes([os.getpid()])
        self.assertEqual(1, len(descendants))
        self.assertIn(child.pid, descendants[0], "Child PID not found in descendants")

    @patch("bzfs_main.util.utils.subprocess.run")
    def test_get_descendant_processes_permission_denied_returns_empty_per_root(self, mock_run: MagicMock) -> None:
        # Simulate PermissionError when trying to invoke 'ps' in restricted sandboxes
        mock_run.side_effect = PermissionError(errno.EPERM, "Operation not permitted", "ps")
        self.assertEqual([[]], _get_descendant_processes([os.getpid()]))

    @patch("bzfs_main.util.utils.subprocess.run")
    @patch("os.kill")
    def test_terminate_process_subtree_permission_denied_kills_roots(
        self, mock_kill: MagicMock, mock_run: MagicMock
    ) -> None:
        # When descendant discovery is denied, ensure we still signal the provided roots and preserve shape
        mock_run.side_effect = PermissionError(errno.EPERM, "Operation not permitted", "ps")
        current = os.getpid()
        other = 2**31 - 2  # a fake PID unlikely to exist; os.kill is mocked anyway
        # except_current_process=True should NOT signal current process but should signal the other root
        terminate_process_subtree(except_current_process=True, root_pids=[current, other], sig=signal.SIGTERM)
        mock_kill.assert_any_call(other, signal.SIGTERM)
        # Ensure we did NOT signal current process in this case
        for call in mock_kill.call_args_list:
            self.assertNotEqual(call.args, (current, signal.SIGTERM))
        mock_kill.reset_mock()
        # except_current_process=False should signal both roots
        terminate_process_subtree(except_current_process=False, root_pids=[current, other], sig=signal.SIGTERM)
        mock_kill.assert_any_call(current, signal.SIGTERM)
        mock_kill.assert_any_call(other, signal.SIGTERM)

    def test_terminate_process_subtree_excluding_current(self) -> None:
        try:
            subprocess.run(["ps", "-p", str(os.getpid()), "-o", "pid="], stdin=DEVNULL, stdout=PIPE, stderr=PIPE, check=True)
        except PermissionError:
            self.skipTest("ps not permitted/available in this sandboxed environment")

        child = subprocess.Popen(["sleep", "1"])
        self.children.append(child)
        time.sleep(0.1)
        self.assertIsNone(child.poll(), "Child process should be running before termination")
        terminate_process_subtree(except_current_process=True)
        time.sleep(0.1)
        self.assertIsNotNone(child.poll(), "Child process should be terminated")

    def test_get_descendant_processes_empty_list(self) -> None:
        self.assertEqual([], _get_descendant_processes([]))


#############################################################################
class TestTerminationSignalHandler(unittest.TestCase):

    def test_sets_event_and_calls_custom_handler_on_sigint(self) -> None:
        old_sigint = signal.getsignal(signal.SIGINT)
        old_sigterm = signal.getsignal(signal.SIGTERM)
        try:
            event = threading.Event()
            mock_handler = MagicMock()
            with termination_signal_handler(event, termination_handler=mock_handler):
                self.assertFalse(event.is_set(), "Event should not be set before a signal is received")
                os.kill(os.getpid(), signal.SIGINT)
                self.assertTrue(event.wait(1.0), "Event should be set after SIGINT")
                mock_handler.assert_called_once()
        finally:
            # Ensure global handlers are restored to what they were before the test
            signal.signal(signal.SIGINT, old_sigint)
            signal.signal(signal.SIGTERM, old_sigterm)

    def test_sets_event_and_calls_custom_handler_on_sigterm(self) -> None:
        old_sigint = signal.getsignal(signal.SIGINT)
        old_sigterm = signal.getsignal(signal.SIGTERM)
        try:
            event = threading.Event()
            mock_handler = MagicMock()
            with termination_signal_handler(event, termination_handler=mock_handler):
                self.assertFalse(event.is_set(), "Event should not be set before a signal is received")
                os.kill(os.getpid(), signal.SIGTERM)
                self.assertTrue(event.wait(1.0), "Event should be set after SIGTERM")
                mock_handler.assert_called_once()
        finally:
            signal.signal(signal.SIGINT, old_sigint)
            signal.signal(signal.SIGTERM, old_sigterm)

    def test_restores_signal_handlers_on_exit(self) -> None:
        old_sigint = signal.getsignal(signal.SIGINT)
        old_sigterm = signal.getsignal(signal.SIGTERM)
        try:
            event = threading.Event()
            with termination_signal_handler(event):
                # Inside context, handlers should differ from originals
                self.assertNotEqual(old_sigint, signal.getsignal(signal.SIGINT))
                self.assertNotEqual(old_sigterm, signal.getsignal(signal.SIGTERM))
            # After exit, original handlers are restored
            self.assertEqual(old_sigint, signal.getsignal(signal.SIGINT))
            self.assertEqual(old_sigterm, signal.getsignal(signal.SIGTERM))
        finally:
            signal.signal(signal.SIGINT, old_sigint)
            signal.signal(signal.SIGTERM, old_sigterm)

    def test_restores_handlers_on_exception(self) -> None:
        old_sigint = signal.getsignal(signal.SIGINT)
        old_sigterm = signal.getsignal(signal.SIGTERM)
        try:
            event = threading.Event()
            with self.assertRaises(RuntimeError):
                with termination_signal_handler(event):
                    raise RuntimeError("boom")
            # Even on exception, original handlers are restored
            self.assertEqual(old_sigint, signal.getsignal(signal.SIGINT))
            self.assertEqual(old_sigterm, signal.getsignal(signal.SIGTERM))
        finally:
            signal.signal(signal.SIGINT, old_sigint)
            signal.signal(signal.SIGTERM, old_sigterm)

    @patch("bzfs_main.util.utils.terminate_process_subtree")
    def test_default_handler_invokes_terminate_process_subtree(self, mock_terminate: MagicMock) -> None:
        old_sigint = signal.getsignal(signal.SIGINT)
        old_sigterm = signal.getsignal(signal.SIGTERM)
        try:
            event = threading.Event()
            with termination_signal_handler(event):
                os.kill(os.getpid(), signal.SIGINT)
                self.assertTrue(event.wait(1.0), "Event should be set after SIGINT")
                mock_terminate.assert_called_once()
                # Verify called with except_current_process=True to avoid killing current process
                _, kwargs = mock_terminate.call_args
                self.assertIsNone(kwargs.get("except_current_process"))
        finally:
            signal.signal(signal.SIGINT, old_sigint)
            signal.signal(signal.SIGTERM, old_sigterm)


#############################################################################
class TestJobStats(unittest.TestCase):

    def test_stats_repr(self) -> None:
        stats = JobStats(jobs_all=10)
        stats.jobs_started = 5
        stats.jobs_completed = 5
        stats.jobs_failed = 2
        stats.jobs_running = 1
        stats.sum_elapsed_nanos = 1_000_000_000
        expect = "all:10, started:5/10=50%, completed:5/10=50%, failed:2/10=20%, running:1, avg_completion_time:200ms"
        self.assertEqual(expect, repr(stats))


#############################################################################
class TestSmallPriorityQueue(unittest.TestCase):

    def setUp(self) -> None:
        self.pq: SmallPriorityQueue[int] = SmallPriorityQueue()
        self.pq_reverse: SmallPriorityQueue[int] = SmallPriorityQueue(reverse=True)

    def test_basic(self) -> None:
        self.assertEqual(0, len(self.pq))
        self.assertTrue(len(str(self.pq)) > 0)
        self.pq.push(2)
        self.assertEqual(1, len(self.pq))
        self.pq.push(1)
        self.assertEqual(2, len(self.pq))
        self.assertTrue(2 in self.pq)
        self.assertTrue(1 in self.pq)
        self.assertFalse(0 in self.pq)
        self.pq.clear()
        self.assertEqual(0, len(self.pq))

    def test_push_and_pop(self) -> None:
        self.pq.push(3)
        self.pq.push(1)
        self.pq.push(2)
        self.assertEqual([1, 2, 3], self.pq._lst)
        self.assertEqual(1, self.pq.pop())
        self.assertEqual([2, 3], self.pq._lst)

    def test_pop_empty(self) -> None:
        with self.assertRaises(IndexError):  # Generic IndexError from list.pop()
            self.pq.pop()

    def test_remove(self) -> None:
        self.pq.push(3)
        self.pq.push(1)
        self.pq.push(2)
        self.pq.remove(2)
        self.assertEqual([1, 3], self.pq._lst)
        self.assertFalse(self.pq.remove(0))
        self.assertTrue(self.pq.remove(1))
        self.assertEqual([3], self.pq._lst)

    def test_remove_nonexistent_element(self) -> None:
        self.pq.push(1)
        self.pq.push(3)
        self.pq.push(2)

        # Attempt to remove an element that doesn't exist (should raise IndexError)
        self.assertFalse(self.pq.remove(4))

    def test_peek(self) -> None:
        self.pq.push(3)
        self.pq.push(1)
        self.pq.push(2)
        self.assertEqual(1, self.pq.peek())
        self.assertEqual([1, 2, 3], self.pq._lst)
        self.pq_reverse.push(3)
        self.pq_reverse.push(1)
        self.pq_reverse.push(2)
        self.assertEqual(3, self.pq_reverse.peek())
        self.assertEqual([1, 2, 3], self.pq_reverse._lst)

    def test_peek_empty(self) -> None:
        with self.assertRaises(IndexError):  # Generic IndexError from list indexing
            self.pq.peek()

        with self.assertRaises(IndexError):  # Generic IndexError from list indexing
            self.pq_reverse.peek()

    def test_reverse_order(self) -> None:
        self.pq_reverse.push(1)
        self.pq_reverse.push(3)
        self.pq_reverse.push(2)
        self.assertEqual(3, self.pq_reverse.pop())
        self.assertEqual([1, 2], self.pq_reverse._lst)

    def test_iter(self) -> None:
        self.pq.push(3)
        self.pq.push(1)
        self.pq.push(2)
        self.assertListEqual([1, 2, 3], list(iter(self.pq)))
        self.assertEqual("[1, 2, 3]", str(self.pq))
        self.pq_reverse.push(1)
        self.pq_reverse.push(3)
        self.pq_reverse.push(2)
        self.assertListEqual([3, 2, 1], list(iter(self.pq_reverse)))
        self.assertEqual("[3, 2, 1]", str(self.pq_reverse))

    def test_duplicates(self) -> None:
        self.pq.push(2)
        self.pq.push(2)
        self.pq.push(1)
        self.assertEqual([1, 2, 2], self.pq._lst)

        # Pop should remove the smallest duplicate first
        self.assertEqual(1, self.pq.pop())
        self.assertEqual([2, 2], self.pq._lst)

        # Remove one duplicate, leaving another
        self.pq.remove(2)
        self.assertEqual([2], self.pq._lst)

        # Peek and pop should now work on the remaining duplicate
        self.assertEqual(2, self.pq.peek())
        self.assertEqual(2, self.pq.pop())
        self.assertEqual(0, len(self.pq))

    def test_reverse_with_duplicates(self) -> None:
        self.pq_reverse.push(2)
        self.pq_reverse.push(2)
        self.pq_reverse.push(3)
        self.pq_reverse.push(1)
        self.assertEqual([1, 2, 2, 3], self.pq_reverse._lst)

        # Pop the largest first in reverse order
        self.assertEqual(3, self.pq_reverse.pop())
        self.assertEqual([1, 2, 2], self.pq_reverse._lst)

        # Remove a duplicate
        self.pq_reverse.remove(2)
        self.assertEqual([1, 2], self.pq_reverse._lst)

        # Peek and pop the remaining elements
        self.assertEqual(2, self.pq_reverse.peek())
        self.assertEqual(2, self.pq_reverse.pop())
        self.assertEqual(1, self.pq_reverse.pop())
        self.assertEqual(0, len(self.pq_reverse))

    def assert_list_equal_ids(self, lst1: list, lst2: list) -> None:
        self.assertListEqual(lst1, lst2)
        for i, value in enumerate(lst1):
            self.assertIs(value, lst2[i])

    def test_duplicates_order(self) -> None:
        v1 = IntHolder(1)
        v2a = IntHolder(2)
        v2b = IntHolder(2)
        self.assertTrue(v2a is not v2b)
        self.assertTrue(v2a == v2b)
        self.assertTrue(v1 < v2a)
        self.assertTrue(v1 < v2b)
        pq: SmallPriorityQueue[IntHolder] = SmallPriorityQueue()
        pq.push(v2a)
        pq.push(v2b)
        pq.push(v1)
        self.assert_list_equal_ids([v1, v2a, v2b], pq._lst)

        # Pop should remove the smallest element first
        self.assertIs(v1, pq.peek())
        self.assertIs(v1, pq.pop())
        self.assert_list_equal_ids([v2a, v2b], pq._lst)

        # Remove first duplicate, leaving another
        self.assertTrue(pq.remove(v2a))
        self.assert_list_equal_ids([v2b], pq._lst)

        # Reinsertion of a duplicate preserves insertion order
        pq.push(v2a)
        self.assert_list_equal_ids([v2b, v2a], pq._lst)

        # Remove first duplicate, leaving another
        self.assertTrue(pq.remove(v2a))
        self.assert_list_equal_ids([v2a], pq._lst)

        # Peek and pop should now work on the remaining duplicate
        self.assertIs(v2a, pq.peek())
        self.assertIs(v2a, pq.pop())
        self.assertEqual(0, len(pq))

    def test_reverse_with_duplicates_order(self) -> None:
        v1 = IntHolder(1)
        v2a = IntHolder(2)
        v2b = IntHolder(2)
        self.assertTrue(v2a is not v2b)
        self.assertTrue(v2a == v2b)
        self.assertTrue(v1 < v2a)
        self.assertTrue(v1 < v2b)
        pq: SmallPriorityQueue[IntHolder] = SmallPriorityQueue(reverse=True)
        pq.push(v2a)
        pq.push(v2b)
        pq.push(v1)
        self.assert_list_equal_ids([v1, v2a, v2b], pq._lst)

        # Pop should remove the largest element first
        self.assertIs(v2b, pq.peek())
        self.assertIs(v2b, pq.pop())
        self.assert_list_equal_ids([v1, v2a], pq._lst)

        # Reinsertion of a duplicate preserves insertion order
        pq.push(v2b)
        self.assert_list_equal_ids([v1, v2a, v2b], pq._lst)

        # Remove first duplicate, leaving another
        self.assertTrue(pq.remove(v2a))
        self.assert_list_equal_ids([v1, v2b], pq._lst)

        # Reinsertion of a duplicate preserves insertion order
        pq.push(v2a)
        self.assert_list_equal_ids([v1, v2b, v2a], pq._lst)

        # Remove first duplicate, leaving another
        self.assertTrue(pq.remove(v2a))
        self.assert_list_equal_ids([v1, v2a], pq._lst)

        # Peek and pop should now work on the remaining duplicate
        self.assertIs(v2a, pq.peek())
        self.assertIs(v2a, pq.pop())
        self.assertEqual(1, len(pq))


@dataclass(order=True, frozen=True)
class IntHolder:
    value: int


#############################################################################
class TestSynchronizedBool(unittest.TestCase):

    def test_initialization(self) -> None:
        b = SynchronizedBool(True)
        self.assertTrue(b.value)

        b = SynchronizedBool(False)
        self.assertFalse(b.value)

        with self.assertRaises(AssertionError):
            SynchronizedBool(cast(Any, "not_a_bool"))

    def test_value_property(self) -> None:
        b = SynchronizedBool(True)
        self.assertTrue(b.value)

        b.value = False
        self.assertFalse(b.value)

    def test_loop(self) -> None:
        b = SynchronizedBool(False)
        for _ in range(3):
            b.value = not b.value
        self.assertIsInstance(b.value, bool)

    def test_bool_conversion(self) -> None:
        b = SynchronizedBool(True)
        self.assertTrue(bool(b))

        b.value = False
        self.assertFalse(bool(b))

    def test_get_and_set(self) -> None:
        b = SynchronizedBool(True)
        self.assertTrue(b.get_and_set(False))
        self.assertFalse(b.value)

    def test_compare_and_set(self) -> None:
        b = SynchronizedBool(True)
        self.assertTrue(b.compare_and_set(True, False))
        self.assertFalse(b.value)

        b = SynchronizedBool(True)
        self.assertFalse(b.compare_and_set(False, False))
        self.assertTrue(b.value)

    def test_str_and_repr(self) -> None:
        b = SynchronizedBool(True)
        self.assertEqual("True", str(b))
        self.assertEqual("True", repr(b))

        b.value = False
        self.assertEqual("False", str(b))
        self.assertEqual("False", repr(b))


#############################################################################
class TestSynchronizedDict(unittest.TestCase):

    def setUp(self) -> None:
        self.sync_dict: SynchronizedDict = SynchronizedDict({"a": 1, "b": 2, "c": 3})

    def test_getitem(self) -> None:
        self.assertEqual(1, self.sync_dict["a"])
        self.assertEqual(2, self.sync_dict["b"])

    def test_setitem(self) -> None:
        self.sync_dict["d"] = 4
        self.assertEqual(4, self.sync_dict["d"])

    def test_delitem(self) -> None:
        del self.sync_dict["a"]
        self.assertNotIn("a", self.sync_dict)

    def test_contains(self) -> None:
        self.assertTrue("a" in self.sync_dict)
        self.assertFalse("z" in self.sync_dict)

    def test_len(self) -> None:
        self.assertEqual(3, len(self.sync_dict))
        del self.sync_dict["a"]
        self.assertEqual(2, len(self.sync_dict))

    def test_repr(self) -> None:
        self.assertEqual(repr({"a": 1, "b": 2, "c": 3}), repr(self.sync_dict))

    def test_str(self) -> None:
        self.assertEqual(str({"a": 1, "b": 2, "c": 3}), str(self.sync_dict))

    def test_get(self) -> None:
        self.assertEqual(1, self.sync_dict.get("a"))
        self.assertEqual(42, self.sync_dict.get("z", 42))

    def test_pop(self) -> None:
        value = self.sync_dict.pop("b")
        self.assertEqual(2, value)
        self.assertNotIn("b", self.sync_dict)

    def test_clear(self) -> None:
        self.sync_dict.clear()
        self.assertEqual(0, len(self.sync_dict))

    def test_items(self) -> None:
        items = self.sync_dict.items()
        self.assertEqual({("a", 1), ("b", 2), ("c", 3)}, set(items))

    def test_loop(self) -> None:
        self.sync_dict["key"] = 1
        self.assertIn("key", self.sync_dict)


#############################################################################
class TestSynchronousExecutor(unittest.TestCase):

    def test_submit_runs_inline_on_current_thread(self) -> None:
        main_ident: int = threading.get_ident()
        with SynchronousExecutor() as ex:
            fut = ex.submit(lambda: threading.get_ident())
            self.assertTrue(fut.done())
            self.assertEqual(main_ident, fut.result())

    def test_result_propagates_exception(self) -> None:
        def boom() -> None:
            raise ValueError("boom")

        with SynchronousExecutor() as ex:
            fut = ex.submit(boom)
            self.assertTrue(fut.done())
            with self.assertRaises(ValueError):
                _ = fut.result()

    def test_map_preserves_order_and_multiple_iterables(self) -> None:
        with SynchronousExecutor() as ex:
            res = list(ex.map(lambda x, y: x + y, [1, 2, 3], [10, 20, 30]))
            self.assertEqual([11, 22, 33], res)

    def test_shutdown_blocks_new_submissions(self) -> None:
        ex = SynchronousExecutor()
        try:
            self.assertEqual(42, ex.submit(lambda: 42).result())
            ex.shutdown()
            with self.assertRaises(RuntimeError):
                _ = ex.submit(lambda: 0)
        finally:
            ex.shutdown()

    def test_shutdown_cancel_futures_argument_accepted(self) -> None:
        ex = SynchronousExecutor()
        ex.shutdown(wait=False, cancel_futures=True)
        with self.assertRaises(RuntimeError):
            _ = ex.submit(lambda: 1)

    def test_future_cancel_on_completed_future(self) -> None:
        with SynchronousExecutor() as ex:
            fut = ex.submit(lambda: 123)
            self.assertTrue(fut.done())
            self.assertFalse(fut.cancel())
            self.assertEqual(123, fut.result())

    def test_nested_submit(self) -> None:
        with SynchronousExecutor() as ex:

            def outer() -> int:
                inner_fut: Future[int] = ex.submit(lambda: 5)
                return 2 * inner_fut.result()

            fut = ex.submit(outer)
            self.assertEqual(10, fut.result())

    def test_factory_for_max_workers_one_returns_sync(self) -> None:
        with SynchronousExecutor.executor_for(1) as ex:
            self.assertIsInstance(ex, SynchronousExecutor)
            self.assertEqual(7, ex.submit(lambda: 7).result())

    def test_factory_for_max_workers_many_returns_threadpool(self) -> None:
        with SynchronousExecutor.executor_for(3) as ex:
            self.assertIsInstance(ex, ThreadPoolExecutor)
            fut = ex.submit(lambda: 9)
            self.assertEqual(9, fut.result())

    def test_factory_invalid_raises(self) -> None:
        with self.assertRaises(ValueError):
            _ = SynchronousExecutor.executor_for(-1)


#############################################################################
class TestXFinally(unittest.TestCase):

    def test_xfinally_executes_cleanup_on_success(self) -> None:
        cleanup = MagicMock()
        with xfinally(cleanup):
            pass
        cleanup.assert_called_once()

    def test_xfinally_executes_cleanup_on_exception(self) -> None:
        cleanup = MagicMock()
        with self.assertRaises(ValueError):
            with xfinally(cleanup):
                raise ValueError("Body error")
        cleanup.assert_called_once()

    def test_xfinally_propagates_cleanup_exception(self) -> None:
        cleanup = MagicMock(side_effect=RuntimeError("Cleanup error"))
        with self.assertRaises(RuntimeError) as cm:
            with xfinally(cleanup):
                pass
        self.assertEqual("Cleanup error", str(cm.exception))
        cleanup.assert_called_once()

    # @unittest.skipIf(sys.version_info != (3, 10), "Requires Python <= 3.10")
    @unittest.skipIf(sys.version_info < (3, 10), "Requires Python >= 3.10")
    def test_xfinally_handles_cleanup_exception_python_3_10_or_lower(self) -> None:
        cleanup = MagicMock(side_effect=RuntimeError("Cleanup error"))
        with self.assertRaises(ValueError) as cm:
            with xfinally(cleanup):
                raise ValueError("Body error")
        self.assertIsInstance(cm.exception.__context__, RuntimeError)
        self.assertEqual("Cleanup error", str(cm.exception.__context__))
        cleanup.assert_called_once()

    @unittest.skipIf(not sys.version_info >= (3, 11), "Requires Python >= 3.11")
    def test_xfinally_handles_cleanup_exception_python_3_11_or_higher(self) -> None:
        self.skipTest("disabled until python 3.11 is the minimum supported")
        cleanup = MagicMock(side_effect=RuntimeError("Cleanup error"))
        with self.assertRaises(ExceptionGroup) as cm:  # type: ignore  # noqa: F821
            with xfinally(cleanup):
                raise ValueError("Body error")
        self.assertEqual(2, len(cm.exception.exceptions))
        self.assertIsInstance(cm.exception.exceptions[0], ValueError)
        self.assertIsInstance(cm.exception.exceptions[1], RuntimeError)
        cleanup.assert_called_once()


###############################################################################
class TestCurrentDateTime(unittest.TestCase):

    def setUp(self) -> None:
        self.fixed_datetime = datetime(2024, 1, 1, 12, 0, 0)

    def test_now(self) -> None:
        self.assertIsNotNone(current_datetime(tz_spec=None, now_fn=None))
        self.assertIsNotNone(current_datetime(tz_spec="UTC", now_fn=None))
        self.assertIsNotNone(current_datetime(tz_spec="+0530", now_fn=None))
        self.assertIsNotNone(current_datetime(tz_spec="+05:30", now_fn=None))
        self.assertIsNotNone(current_datetime(tz_spec="-0430", now_fn=None))
        self.assertIsNotNone(current_datetime(tz_spec="-04:30", now_fn=None))

    def test_local_timezone(self) -> None:
        expected = self.fixed_datetime.astimezone(tz=None)
        actual = current_datetime(tz_spec=None, now_fn=lambda tz=None: self.fixed_datetime.astimezone(tz=tz))  # type: ignore
        self.assertEqual(expected, actual)

        fmt = "%Y-%m-%dT%H:%M:%S"
        self.assertEqual("2024-01-01T12:00:00", actual.strftime(fmt))
        fmt = "%Y-%m-%d_%H:%M:%S"
        self.assertEqual("2024-01-01_12:00:00", actual.strftime(fmt))
        fmt = "%Y-%m-%d_%H:%M:%S_%Z"
        expected_prefix = "2024-01-01_12:00:00_"
        self.assertTrue(actual.strftime(fmt).startswith(expected_prefix))
        suffix = actual.strftime(fmt)[len(expected_prefix) :]
        self.assertGreater(len(suffix), 0)

        fmt = "%Y-%m-%d_%H:%M:%S%z"
        expected_prefix = "2024-01-01_12:00:00"
        self.assertTrue(actual.strftime(fmt).startswith(expected_prefix))
        suffix = actual.strftime(fmt)[len(expected_prefix) :]
        self.assertIn(suffix[0], ["+", "-"])
        self.assertTrue(suffix[1:].isdigit())

    def test_utc_timezone(self) -> None:
        expected = self.fixed_datetime.astimezone(tz=timezone.utc)

        def now_fn(tz: tzinfo | None = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = current_datetime(tz_spec="UTC", now_fn=now_fn)
        self.assertEqual(expected, actual)

    def test_tzoffset_positive(self) -> None:
        tz_spec = "+0530"
        tz = timezone(timedelta(hours=5, minutes=30))
        expected = self.fixed_datetime.astimezone(tz=tz)

        def now_fn(_: tzinfo | None = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = current_datetime(tz_spec=tz_spec, now_fn=now_fn)
        self.assertEqual(expected, actual)

    def test_tzoffset_negative(self) -> None:
        tz_spec = "-0430"
        tz = timezone(timedelta(hours=-4, minutes=-30))
        expected = self.fixed_datetime.astimezone(tz=tz)

        def now_fn(_: tzinfo | None = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = current_datetime(tz_spec=tz_spec, now_fn=now_fn)
        self.assertEqual(expected, actual)

    def test_iana_timezone(self) -> None:
        tz_spec = "Asia/Tokyo"
        self.assertIsNotNone(current_datetime(tz_spec=tz_spec, now_fn=None))
        tz = ZoneInfo(tz_spec)
        expected = self.fixed_datetime.astimezone(tz=tz)

        def now_fn(_: tzinfo | None = None) -> datetime:
            return self.fixed_datetime.astimezone(tz=tz)

        actual = current_datetime(tz_spec=tz_spec, now_fn=now_fn)
        self.assertEqual(expected, actual)

        fmt = "%Y-%m-%dT%H:%M:%S%z"
        expected_prefix = "2024-01-01T20:00:00"
        suffix = actual.strftime(fmt)[len(expected_prefix) :]
        self.assertIn(suffix[0], ["+", "-"])
        self.assertTrue(suffix[1:].isdigit())

    def test_invalid_timezone(self) -> None:
        with self.assertRaises(ValueError) as context:
            current_datetime(tz_spec="INVALID")
        self.assertIn("Invalid timezone specification", str(context.exception))

    def test_invalid_timezone_format_missing_sign(self) -> None:
        with self.assertRaises(ValueError):
            current_datetime(tz_spec="0400")

    def test_unixtime_conversion(self) -> None:
        iso = "2024-01-02T03:04:05+00:00"
        unix = unixtime_fromisoformat(iso)
        expected = "2024-01-02_03:04:05+00:00"
        self.assertEqual(expected, isotime_from_unixtime(unix))

    def test_get_timezone_variants(self) -> None:
        self.assertIsNone(get_timezone())
        self.assertEqual(timezone.utc, get_timezone("UTC"))
        tz = get_timezone("+0130")
        assert tz is not None
        self.assertEqual(90 * 60, cast(timedelta, tz.utcoffset(None)).total_seconds())
        zone = get_timezone("Europe/Vienna")
        self.assertEqual("Europe/Vienna", getattr(zone, "key", None))
        with self.assertRaises(ValueError):
            get_timezone("bad-tz")
