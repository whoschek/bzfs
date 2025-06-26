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

from __future__ import annotations
import errno
import os
import shutil
import stat
import tempfile
import unittest
from unittest import mock

from bzfs_main import utils
from bzfs_main.utils import open_nofollow


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestUtils,
        OpenNoFollowTest,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestUtils(unittest.TestCase):

    def test_cut_field1(self) -> None:
        lines = ["a\tb\tc", "d\te\tf"]
        expected = ["a", "d"]
        self.assertEqual(utils.cut(field=1, lines=lines), expected)

    def test_cut_field2(self) -> None:
        lines = ["a\tb\tc", "d\te\tf"]
        expected = ["b\tc", "e\tf"]
        self.assertEqual(utils.cut(field=2, lines=lines), expected)

    def test_cut_invalid_field(self) -> None:
        lines = ["a\tb\tc"]
        with self.assertRaises(ValueError):
            utils.cut(field=3, lines=lines)

    def test_cut_empty_lines(self) -> None:
        self.assertEqual(utils.cut(field=1, lines=[]), [])

    def test_cut_different_separator(self) -> None:
        lines = ["a,b,c", "d,e,f"]
        expected = ["a", "d"]
        self.assertEqual(utils.cut(field=1, separator=",", lines=lines), expected)


#############################################################################
class OpenNoFollowTest(unittest.TestCase):
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
        self.assertEqual(data, "hello")

    def test_read_binary(self) -> None:
        with open(self.real_path, "rb") as f:
            raw = f.read()
        with open_nofollow(self.real_path, "rb") as f:
            data = f.read()
        self.assertEqual(data, raw)

    def test_write_truncate(self) -> None:
        with open_nofollow(self.real_path, "w", encoding="utf-8") as f:
            f.write("world")
        with open(self.real_path, "r", encoding="utf-8") as f:
            self.assertEqual(f.read(), "world")

    def test_append(self) -> None:
        with open_nofollow(self.real_path, "a", encoding="utf-8") as f:
            f.write(" world")
        with open(self.real_path, "r", encoding="utf-8") as f:
            self.assertEqual(f.read(), "hello world")

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
            self.assertEqual(content, "hello")
            f.seek(0)
            f.write("HELLO")
        with open(self.real_path, "r", encoding="utf-8") as f:
            self.assertEqual(f.read(), "HELLO")

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
            self.assertEqual(mode, 0o600)
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
        with mock.patch("os.fdopen", side_effect=err) as m_fdopen, mock.patch("os.close", side_effect=orig_close) as m_close:
            with self.assertRaises(RuntimeError):
                open_nofollow(self.real_path, "r")
        m_fdopen.assert_called_once()
        m_close.assert_called_once()
