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
"""Unit tests for the ``bzfs`` CLI."""

from __future__ import annotations
import contextlib
import io
import subprocess
import time
import unittest
from typing import (
    List,
    cast,
)
from unittest.mock import (
    MagicMock,
    patch,
)

import bzfs_main.detect
import bzfs_main.utils
from bzfs_main import bzfs
from bzfs_main.configuration import LogParams, Remote
from bzfs_main.connection import (
    ConnectionPools,
)
from bzfs_main.detect import (
    ZFS_VERSION_IS_AT_LEAST_2_2_0,
    RemoteConfCacheItem,
    detect_available_programs,
)
from bzfs_main.replication import (
    add_recv_property_options,
    is_zfs_dataset_busy,
    pv_cmd,
)
from bzfs_main.utils import (
    DIE_STATUS,
)
from bzfs_tests.abstract_testcase import AbstractTestCase
from bzfs_tests.zfs_util import (
    is_solaris_zfs,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestHelperFunctions,
        TestAdditionalHelpers,
        TestParseDatasetLocator,
        TestArgumentParser,
        TestAddRecvPropertyOptions,
        TestPreservePropertiesValidation,
        TestPythonVersionCheck,
        # TestPerformance,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestHelperFunctions(AbstractTestCase):
    def test_validate_port(self) -> None:
        bzfs.validate_port(47, "msg")
        bzfs.validate_port("47", "msg")
        bzfs.validate_port(0, "msg")
        bzfs.validate_port("", "msg")
        with self.assertRaises(SystemExit):
            bzfs.validate_port("xxx47", "msg")

    def test_run_main_with_unexpected_exception(self) -> None:
        args = self.argparser_parse_args(args=["src", "dst"])
        job = bzfs.Job()
        job.params = self.make_params(args=args)

        with patch("time.monotonic_ns", side_effect=ValueError("my value error")):
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit):
                job.run_main(args)

    def test_recv_option_property_names(self) -> None:
        def names(lst: list[str]) -> set[str]:
            return bzfs.Job().recv_option_property_names(lst)

        self.assertSetEqual(set(), names([]))
        self.assertSetEqual(set(), names(["name1=value1"]))
        self.assertSetEqual(set(), names(["name1"]))
        self.assertSetEqual({"name1"}, names(["-o", "name1=value1"]))
        self.assertSetEqual({"name1"}, names(["-x", "name1"]))
        self.assertSetEqual({"name1"}, names(["-o", "name1=value1", "-o", "name1=value2", "-x", "name1"]))
        self.assertSetEqual({"name1", "name2"}, names(["-o", "name1=value1", "-o", "name2=value2"]))
        self.assertSetEqual({"name1", "name2"}, names(["-o", "name1=value1", "-x", "name2"]))
        self.assertSetEqual({"name1", "name2"}, names(["-v", "-o", "name1=value1", "-o", "name2=value2"]))
        self.assertSetEqual({"name1", "name2"}, names(["-v", "-o", "name1=value1", "-o", "name2=value2", "-F"]))
        self.assertSetEqual({"name1", "name2"}, names(["-v", "-o", "name1=value1", "-n", "-o", "name2=value2", "-F"]))
        with self.assertRaises(SystemExit):
            names(["-o", "name1"])
        with self.assertRaises(SystemExit):
            names(["-o", "=value1"])
        with self.assertRaises(SystemExit):
            names(["-o", ""])
        with self.assertRaises(SystemExit):
            names(["-x", "=value1"])
        with self.assertRaises(SystemExit):
            names(["-x", ""])
        with self.assertRaises(SystemExit):
            names(["-o"])
        with self.assertRaises(SystemExit):
            names(["-o", "name1=value1", "-o"])
        with self.assertRaises(SystemExit):
            names(["-o", "name1=value1", "-x"])
        with self.assertRaises(SystemExit):
            names(["-o", "-o", "name1=value1"])
        with self.assertRaises(SystemExit):
            names(["-x", "-x", "name1=value1"])
        with self.assertRaises(SystemExit):
            names([" -o ", " -o ", "name1=value1"])

    def test_fix_solaris_raw_mode(self) -> None:
        self.assertListEqual(["-w", "none"], bzfs.fix_solaris_raw_mode(["-w"]))
        self.assertListEqual(["-w", "none"], bzfs.fix_solaris_raw_mode(["--raw"]))
        self.assertListEqual(["-w", "none"], bzfs.fix_solaris_raw_mode(["-w", "none"]))
        self.assertListEqual(["-w", "compress"], bzfs.fix_solaris_raw_mode(["-w", "compress"]))
        self.assertListEqual(["-w", "compress"], bzfs.fix_solaris_raw_mode(["-w", "--compressed"]))
        self.assertListEqual(["-w", "crypto"], bzfs.fix_solaris_raw_mode(["-w", "crypto"]))
        self.assertListEqual(["-w", "none", "foo"], bzfs.fix_solaris_raw_mode(["-w", "foo"]))
        self.assertListEqual(["-F"], bzfs.fix_solaris_raw_mode(["-F"]))

    def test_has_siblings(self) -> None:
        self.assertFalse(bzfs.has_siblings([]))
        self.assertFalse(bzfs.has_siblings(["a"]))
        self.assertFalse(bzfs.has_siblings(["a", "a/b"]))
        self.assertFalse(bzfs.has_siblings(["a", "a/b", "a/b/c"]))
        self.assertTrue(bzfs.has_siblings(["a", "b"]))
        self.assertTrue(bzfs.has_siblings(["a", "a/b", "a/d"]))
        self.assertTrue(bzfs.has_siblings(["a", "a/b", "a/b/c", "a/b/d"]))
        self.assertTrue(bzfs.has_siblings(["a/b/c", "d/e/f"]))  # multiple root datasets can be processed in parallel
        self.assertFalse(bzfs.has_siblings(["a", "a/b/c"]))
        self.assertFalse(bzfs.has_siblings(["a", "a/b/c/d"]))
        self.assertTrue(bzfs.has_siblings(["a", "a/b/c", "a/b/d"]))
        self.assertTrue(bzfs.has_siblings(["a", "a/b/c/d", "a/b/c/e"]))

    def test_is_zfs_dataset_busy_match(self) -> None:
        def is_busy(proc: str, dataset: str, busy_if_send: bool = True) -> bool:
            return is_zfs_dataset_busy([proc], dataset, busy_if_send=busy_if_send)

        ds = "tank/foo/bar"
        self.assertTrue(is_busy("zfs receive " + ds, ds))
        self.assertTrue(is_busy("zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("sudo zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("/bin/sudo zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("/usr/bin/sudo zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("sudo /sbin/zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("sudo /usr/sbin/zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("/bin/sudo /sbin/zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("doas zfs receive -u " + ds, ds))
        self.assertTrue(is_busy("doas zfs receive -u " + ds, ds, busy_if_send=False))
        self.assertTrue(is_busy("sudo zfs receive -u -o foo:bar=/baz " + ds, ds))
        self.assertFalse(is_busy("sudo zfs xxxxxx -u " + ds, ds))
        self.assertFalse(is_busy("sudo zfs xxxxxx -u " + ds, ds, busy_if_send=False))
        self.assertFalse(is_busy("xsudo zfs receive -u " + ds, ds))
        self.assertFalse(is_busy("sudo zfs receive -u", ds))
        self.assertFalse(is_busy("sudo receive -u " + ds, ds))
        self.assertFalse(is_busy("zfs send " + ds + "@snap", ds, busy_if_send=False))
        self.assertTrue(is_busy("zfs send " + ds + "@snap", ds))

    def test_pv_cmd(self) -> None:
        args = self.argparser_parse_args(args=["src", "dst"])
        job = bzfs.Job()
        job.params = self.make_params(args=args, log_params=LogParams(args))
        job.params.available_programs = {"src": {"pv": "pv"}}
        self.assertNotEqual("cat", pv_cmd(job, "src", 1024 * 1024, "foo"))

    @staticmethod
    def root_datasets_if_recursive_zfs_snapshot_is_possible_slow_but_correct(  # compare faster algos to this baseline impl
        src_datasets: list[str], basis_src_datasets: list[str]
    ) -> list[str] | None:
        # Assumes that src_datasets and basis_src_datasets are both sorted (and thus root_datasets is sorted too)
        src_datasets_set = set(src_datasets)
        root_datasets = bzfs.Job().find_root_datasets(src_datasets)
        for basis_dataset in basis_src_datasets:
            for root_dataset in root_datasets:
                if bzfs_main.utils.is_descendant(basis_dataset, of_root_dataset=root_dataset):
                    if basis_dataset not in src_datasets_set:
                        return None
        return root_datasets

    def test_root_datasets_if_recursive_zfs_snapshot_is_possible(self) -> None:
        def run_filter(src_datasets: list[str], basis_src_datasets: list[str]) -> list[str]:
            assert set(src_datasets).issubset(set(basis_src_datasets))
            src_datasets = sorted(src_datasets)
            basis_src_datasets = sorted(basis_src_datasets)
            expected = self.root_datasets_if_recursive_zfs_snapshot_is_possible_slow_but_correct(
                src_datasets, basis_src_datasets
            )
            actual = bzfs.Job().root_datasets_if_recursive_zfs_snapshot_is_possible(src_datasets, basis_src_datasets)
            if expected is not None:
                self.assertListEqual(expected, cast(List[str], actual))
            self.assertEqual(expected, actual)
            return cast(List[str], actual)

        basis_src_datasets = ["a", "a/b", "a/b/c", "a/d"]
        self.assertListEqual([], run_filter([], basis_src_datasets))
        self.assertListEqual(["a"], run_filter(basis_src_datasets, basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["a", "a/b", "a/b/c"], basis_src_datasets)))
        self.assertIsNone(cast(None, run_filter(["a/b", "a/d"], basis_src_datasets)))
        self.assertListEqual(["a/b"], run_filter(["a/b", "a/b/c"], basis_src_datasets))
        self.assertListEqual(["a/b", "a/d"], run_filter(["a/b", "a/b/c", "a/d"], basis_src_datasets))
        self.assertListEqual(["a/b", "a/d"], run_filter(["a/b", "a/b/c", "a/d"], basis_src_datasets))
        self.assertListEqual(["a/d"], run_filter(["a/d"], basis_src_datasets))

        basis_src_datasets = ["a", "a/b", "a/b/c", "a/d", "e", "e/f"]
        self.assertListEqual(["a", "e"], run_filter(basis_src_datasets, basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["e"], basis_src_datasets)))
        self.assertListEqual(["e/f"], run_filter(["e/f"], basis_src_datasets))
        self.assertListEqual(["a", "e/f"], run_filter(["a", "a/b", "a/b/c", "a/d", "e/f"], basis_src_datasets))
        self.assertListEqual(["a/b"], run_filter(["a/b", "a/b/c"], basis_src_datasets))
        self.assertListEqual(["a/b", "a/d"], run_filter(["a/b", "a/b/c", "a/d"], basis_src_datasets))
        self.assertListEqual(["a/d"], run_filter(["a/d"], basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["a/b", "a/d"], basis_src_datasets)))
        self.assertIsNone(cast(None, run_filter(["a", "a/b", "a/d"], basis_src_datasets)))

        basis_src_datasets = ["a", "e", "h"]
        self.assertListEqual([], run_filter([], basis_src_datasets))
        self.assertListEqual(["a", "e", "h"], run_filter(basis_src_datasets, basis_src_datasets))
        self.assertListEqual(["a", "e"], run_filter(["a", "e"], basis_src_datasets))
        self.assertListEqual(["e", "h"], run_filter(["e", "h"], basis_src_datasets))
        self.assertListEqual(["a", "h"], run_filter(["a", "h"], basis_src_datasets))
        self.assertListEqual(["a"], run_filter(["a"], basis_src_datasets))
        self.assertListEqual(["e"], run_filter(["e"], basis_src_datasets))
        self.assertListEqual(["h"], run_filter(["h"], basis_src_datasets))

        basis_src_datasets = ["a", "e", "e/f", "h", "h/g"]
        self.assertListEqual([], run_filter([], basis_src_datasets))
        self.assertListEqual(["a", "e", "h"], run_filter(basis_src_datasets, basis_src_datasets))

        self.assertIsNone(cast(None, run_filter(["a", "h"], basis_src_datasets)))
        self.assertListEqual(["a", "h/g"], run_filter(["a", "h/g"], basis_src_datasets))

        basis_src_datasets = ["a", "e", "e/f", "h", "h/g", "k", "k/l"]
        self.assertListEqual([], run_filter([], basis_src_datasets))
        self.assertListEqual(["a", "e", "h", "k"], run_filter(basis_src_datasets, basis_src_datasets))

        self.assertIsNone(cast(None, run_filter(["a", "h"], basis_src_datasets)))
        self.assertListEqual(["a", "h/g"], run_filter(["a", "h/g"], basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["a", "k"], basis_src_datasets)))
        self.assertListEqual(["a", "k/l"], run_filter(["a", "k/l"], basis_src_datasets))

        basis_src_datasets = ["a", "e", "e/f", "h", "h/g", "hh", "hh/g", "k", "kk", "k/l", "kk/l"]
        self.assertIsNone(cast(None, run_filter(["a", "hh"], basis_src_datasets)))
        self.assertListEqual(["a", "hh/g"], run_filter(["a", "hh/g"], basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["kk"], basis_src_datasets)))
        self.assertListEqual(["kk/l"], run_filter(["kk/l"], basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["k"], basis_src_datasets)))
        self.assertListEqual(["k/l"], run_filter(["k/l"], basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["h"], basis_src_datasets)))
        self.assertListEqual(["h/g"], run_filter(["h/g"], basis_src_datasets))

        basis_src_datasets = ["a", "e", "e/f", "h", "h/g", "hh", "hh/g", "kk", "kk/l"]
        self.assertIsNone(cast(None, run_filter(["kk"], basis_src_datasets)))
        self.assertListEqual(["kk/l"], run_filter(["kk/l"], basis_src_datasets))
        self.assertIsNone(cast(None, run_filter(["hh"], basis_src_datasets)))

        # sorted()
        basis_src_datasets = ["a", "a/b", "a/b/c", "a/B", "a/B/c", "a/D", "a/X", "a/X/c"]
        self.assertListEqual(["a/D", "a/b"], run_filter(["a/b", "a/b/c", "a/D"], basis_src_datasets))
        self.assertListEqual(["a/B", "a/D", "a/b"], run_filter(["a/B", "a/B/c", "a/b", "a/b/c", "a/D"], basis_src_datasets))
        self.assertListEqual(["a/B", "a/D", "a/X"], run_filter(["a/B", "a/B/c", "a/X", "a/X/c", "a/D"], basis_src_datasets))


#############################################################################
class TestAdditionalHelpers(AbstractTestCase):

    def test_job_shutdown_calls_pool_shutdown(self) -> None:
        job = bzfs.Job()
        mock_pool = MagicMock()
        job.remote_conf_cache = {("k",): RemoteConfCacheItem(mock_pool, {}, {})}
        job.shutdown()
        mock_pool.shutdown.assert_called_once()

    @patch("bzfs_main.bzfs.argument_parser")
    @patch("bzfs_main.bzfs.run_main")
    def test_main_handles_calledprocesserror(self, mock_run_main: MagicMock, mock_arg_parser: MagicMock) -> None:
        mock_arg_parser.return_value.parse_args.return_value = self.argparser_parse_args(["src", "dst"])
        mock_run_main.side_effect = subprocess.CalledProcessError(returncode=5, cmd=["cmd"])
        with self.assertRaises(SystemExit) as ctx:
            bzfs.main()
        self.assertEqual(5, ctx.exception.code)

    @patch("bzfs_main.bzfs.Job.run_main")
    def test_run_main_delegates_to_job(self, mock_run_main: MagicMock) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        bzfs.run_main(args=args, sys_argv=["p"])
        mock_run_main.assert_called_once_with(args, ["p"], None)


#############################################################################
class TestParseDatasetLocator(AbstractTestCase):
    def run_test(
        self,
        input_value: str,
        expected_user: str,
        expected_host: str,
        expected_dataset: str,
        expected_user_host: str,
        expected_error: bool,
    ) -> None:
        expected_status = 0 if not expected_error else 3
        passed = False

        # Run without validation
        user, host, user_host, pool, dataset = bzfs.parse_dataset_locator(input_value, validate=False)

        if (
            user == expected_user
            and host == expected_host
            and dataset == expected_dataset
            and user_host == expected_user_host
        ):
            passed = True

        # Rerun with validation
        status = 0
        try:
            bzfs.parse_dataset_locator(input_value, validate=True)
        except (ValueError, SystemExit):
            status = 3

        if status != expected_status or (not passed):
            if status != expected_status:
                print("Validation Test failed:")
            else:
                print("Test failed:")
            print(
                f"input: {input_value}\nuser exp: '{expected_user}' vs '{user}'\nhost exp: '{expected_host}' vs "
                f"'{host}'\nuserhost exp: '{expected_user_host}' vs '{user_host}'\ndataset exp: '{expected_dataset}' vs "
                f"'{dataset}'"
            )
            self.fail()

    def test_basic(self) -> None:
        # Input format is [[user@]host:]dataset
        # test columns indicate values for: input | user | host | dataset | userhost | validationError
        self.run_test(
            "user@host.example.com:tank1/foo/bar",
            "user",
            "host.example.com",
            "tank1/foo/bar",
            "user@host.example.com",
            False,
        )
        self.run_test(
            "joe@192.168.1.1:tank1/foo/bar:baz:boo",
            "joe",
            "192.168.1.1",
            "tank1/foo/bar:baz:boo",
            "joe@192.168.1.1",
            False,
        )
        self.run_test("tank1/foo/bar", "", "", "tank1/foo/bar", "", False)
        self.run_test("-:tank1/foo/bar:baz:boo", "", "", "tank1/foo/bar:baz:boo", "", False)
        self.run_test("host.example.com:tank1/foo/bar", "", "host.example.com", "tank1/foo/bar", "host.example.com", False)
        self.run_test("root@host.example.com:tank1", "root", "host.example.com", "tank1", "root@host.example.com", False)
        self.run_test("192.168.1.1:tank1/foo/bar", "", "192.168.1.1", "tank1/foo/bar", "192.168.1.1", False)
        self.run_test("user@192.168.1.1:tank1/foo/bar", "user", "192.168.1.1", "tank1/foo/bar", "user@192.168.1.1", False)
        self.run_test(
            "user@host_2024-01-02:a3:04:56:tank1/foo/bar",
            "user",
            "host_2024-01-02",
            "a3:04:56:tank1/foo/bar",
            "user@host_2024-01-02",
            False,
        )
        self.run_test(
            "user@host_2024-01-02:a3:04:56:tank1:/foo:/:bar",
            "user",
            "host_2024-01-02",
            "a3:04:56:tank1:/foo:/:bar",
            "user@host_2024-01-02",
            False,
        )
        self.run_test(
            "user@host_2024-01-02:03:04:56:tank1/foo/bar",
            "user",
            "host_2024-01-02",
            "03:04:56:tank1/foo/bar",
            "user@host_2024-01-02",
            True,
        )
        self.run_test("user@localhost:tank1/foo/bar", "user", "localhost", "tank1/foo/bar", "user@localhost", False)
        self.run_test("host.local:tank1/foo/bar", "", "host.local", "tank1/foo/bar", "host.local", False)
        self.run_test("host.local:tank1/foo/bar", "", "host.local", "tank1/foo/bar", "host.local", False)
        self.run_test("-host.local:tank1/foo/bar", "", "-host.local", "tank1/foo/bar", "-host.local", True)
        self.run_test("user@host:", "user", "host", "", "user@host", True)
        self.run_test("@host:tank1/foo/bar", "", "host", "tank1/foo/bar", "host", False)
        self.run_test("@host:tank1/foo/bar", "", "host", "tank1/foo/bar", "host", False)
        self.run_test("@host:", "", "host", "", "host", True)
        self.run_test("user@:tank1/foo/bar", "", "user@", "tank1/foo/bar", "user@", True)
        self.run_test("user@:", "", "user@", "", "user@", True)
        self.run_test("@", "", "", "@", "", True)
        self.run_test("@foo", "", "", "@foo", "", True)
        self.run_test("@@", "", "", "@@", "", True)
        self.run_test(":::tank1:foo:bar:", "", "", ":::tank1:foo:bar:", "", True)
        self.run_test(":::tank1/bar", "", "", ":::tank1/bar", "", True)
        self.run_test(":::", "", "", ":::", "", True)
        self.run_test("::tank1/bar", "", "", "::tank1/bar", "", True)
        self.run_test("::", "", "", "::", "", True)
        self.run_test(":tank1/bar", "", "", ":tank1/bar", "", True)
        self.run_test(":", "", "", ":", "", True)
        self.run_test("", "", "", "", "", True)
        self.run_test("/", "", "", "/", "", True)
        self.run_test("tank//foo", "", "", "tank//foo", "", True)
        self.run_test("/tank1", "", "", "/tank1", "", True)
        self.run_test("tank1/", "", "", "tank1/", "", True)
        self.run_test(".", "", "", ".", "", True)
        self.run_test("..", "", "", "..", "", True)
        self.run_test("./tank", "", "", "./tank", "", True)
        self.run_test("../tank", "", "", "../tank", "", True)
        self.run_test("tank/..", "", "", "tank/..", "", True)
        self.run_test("tank/.", "", "", "tank/.", "", True)
        self.run_test("tank/fo`o", "", "", "tank/fo`o", "", True)
        self.run_test("tank/fo$o", "", "", "tank/fo$o", "", True)
        self.run_test("tank/fo\\o", "", "", "tank/fo\\o", "", True)
        self.run_test("u`ser@localhost:tank1/foo/bar", "u`ser", "localhost", "tank1/foo/bar", "u`ser@localhost", True)
        self.run_test("u'ser@localhost:tank1/foo/bar", "u'ser", "localhost", "tank1/foo/bar", "u'ser@localhost", True)
        self.run_test('u"ser@localhost:tank1/foo/bar', 'u"ser', "localhost", "tank1/foo/bar", 'u"ser@localhost', True)
        self.run_test("user@l`ocalhost:tank1/foo/bar", "user", "l`ocalhost", "tank1/foo/bar", "user@l`ocalhost", True)
        self.run_test("user@l'ocalhost:tank1/foo/bar", "user", "l'ocalhost", "tank1/foo/bar", "user@l'ocalhost", True)
        self.run_test('user@l"ocalhost:tank1/foo/bar', "user", 'l"ocalhost', "tank1/foo/bar", 'user@l"ocalhost', True)
        self.run_test("user@host.ex.com:tank1/foo@bar", "user", "host.ex.com", "tank1/foo@bar", "user@host.ex.com", True)
        self.run_test("user@host.ex.com:tank1/foo#bar", "user", "host.ex.com", "tank1/foo#bar", "user@host.ex.com", True)
        self.run_test(
            "whitespace user@host.ex.com:tank1/foo/bar",
            "whitespace user",
            "host.ex.com",
            "tank1/foo/bar",
            "whitespace user@host.ex.com",
            True,
        )
        self.run_test(
            "user@whitespace\thost:tank1/foo/bar",
            "user",
            "whitespace\thost",
            "tank1/foo/bar",
            "user@whitespace\thost",
            True,
        )
        self.run_test("user@host:tank1/foo/whitespace\tbar", "user", "host", "tank1/foo/whitespace\tbar", "user@host", True)
        self.run_test("user@host:tank1/foo/whitespace\nbar", "user", "host", "tank1/foo/whitespace\nbar", "user@host", True)
        self.run_test("user@host:tank1/foo/whitespace\rbar", "user", "host", "tank1/foo/whitespace\rbar", "user@host", True)
        self.run_test("user@host:tank1/foo/space bar", "user", "host", "tank1/foo/space bar", "user@host", False)
        self.run_test("user@||1:tank1/foo/space bar", "user", "::1", "tank1/foo/space bar", "user@::1", False)
        self.run_test("user@::1:tank1/foo/space bar", "", "user@", ":1:tank1/foo/space bar", "user@", True)


#############################################################################
class TestArgumentParser(AbstractTestCase):

    def test_help(self) -> None:
        if is_solaris_zfs():
            self.skipTest("FIXME: BlockingIOError: [Errno 11] write could not complete without blocking")
        parser = bzfs.argument_parser()
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as e:
            parser.parse_args(["--help"])
        self.assertEqual(0, e.exception.code)

    def test_version(self) -> None:
        parser = bzfs.argument_parser()
        with self.assertRaises(SystemExit) as e:
            parser.parse_args(["--version"])
        self.assertEqual(0, e.exception.code)

    def test_missing_datasets(self) -> None:
        parser = bzfs.argument_parser()
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as e:
            parser.parse_args(["--retries=1"])
        self.assertEqual(2, e.exception.code)

    def test_missing_dst_dataset(self) -> None:
        parser = bzfs.argument_parser()
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as e:
            parser.parse_args(["src_dataset"])  # Each SRC_DATASET must have a corresponding DST_DATASET
        self.assertEqual(2, e.exception.code)

    def test_program_must_not_be_empty_string(self) -> None:
        parser = bzfs.argument_parser()
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as e:
            parser.parse_args(["src_dataset", "src_dataset", "--zfs-program="])
        self.assertEqual(2, e.exception.code)


###############################################################################
class TestAddRecvPropertyOptions(AbstractTestCase):

    def setUp(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        self.p = self.make_params(args=args)
        self.p.src = Remote("src", args, self.p)
        self.p.dst = Remote("dst", args, self.p)
        self.p.zfs_recv_x_names = ["xprop1", "xprop2"]
        self.p.zfs_recv_ox_names = {"existing"}
        self.job = bzfs.Job()
        self.job.params = self.p

    def test_appends_x_options_when_supported(self) -> None:
        recv_opts: list[str] = []
        with patch.object(self.p, "is_program_available", return_value=True):
            result_opts, set_opts = add_recv_property_options(self.job, True, recv_opts, "ds", {})
        self.assertEqual(["-x", "xprop1", "-x", "xprop2"], result_opts)
        self.assertEqual([], set_opts)
        # original zfs_recv_ox_names remains unchanged
        self.assertEqual({"existing"}, self.p.zfs_recv_ox_names)

    def test_skips_x_options_when_not_supported(self) -> None:
        recv_opts: list[str] = []
        with patch.object(self.p, "is_program_available", return_value=False):
            result_opts, set_opts = add_recv_property_options(self.job, True, recv_opts, "ds", {})
        self.assertEqual([], result_opts)
        self.assertEqual([], set_opts)
        self.assertEqual({"existing"}, self.p.zfs_recv_ox_names)


#############################################################################
class TestPreservePropertiesValidation(AbstractTestCase):
    def setUp(self) -> None:
        self.args = self.argparser_parse_args(
            [
                "src",
                "dst",
                "--preserve-properties",
                "recordsize",
                "--zfs-send-program-opts=--props --raw --compressed",
            ]
        )
        self.p = self.make_params(args=self.args)
        self.job = bzfs.Job()
        self.job.params = self.p

        # Setup minimal remote objects. The specific details don't matter much as they are mocked.
        self.p.src = Remote("src", self.args, self.p)
        self.p.dst = Remote("dst", self.args, self.p)
        self.p.src.ssh_user_host = "src_host"
        self.p.dst.ssh_user_host = "dst_host"
        self.p.connection_pools = {
            "local": MagicMock(spec=ConnectionPools),
            "src": MagicMock(spec=ConnectionPools),
            "dst": MagicMock(spec=ConnectionPools),
        }
        self.p.zpool_features = {"src": {}, "dst": {}}
        self.p.available_programs = {"local": {"ssh": ""}, "src": {}, "dst": {}}

    @patch.object(bzfs_main.detect, "detect_zpool_features")
    @patch.object(bzfs_main.detect, "detect_available_programs_remote")
    def test_preserve_properties_fails_on_old_zfs_with_props(
        self, mock_detect_zpool_features: MagicMock, mock_detect_available_programs_remote: MagicMock
    ) -> None:
        """Verify that using --preserve-properties with --props fails on ZFS < 2.2.0."""
        with patch.object(self.p, "is_program_available") as mock_is_available:
            # Make the mock specific to the zfs>=2.2.0 check on dst
            def side_effect(program: str, location: str) -> bool:
                if program == ZFS_VERSION_IS_AT_LEAST_2_2_0 and location == "dst":
                    return False
                return True  # Assume other programs are available

            mock_is_available.side_effect = side_effect
            with self.assertRaises(SystemExit) as cm:
                detect_available_programs(self.job)
            self.assertEqual(cm.exception.code, DIE_STATUS)
            self.assertIn("--preserve-properties is unreliable on destination ZFS < 2.2.0", str(cm.exception))

    @patch.object(bzfs_main.detect, "detect_zpool_features")
    @patch.object(bzfs_main.detect, "detect_available_programs_remote")
    def test_preserve_properties_succeeds_on_new_zfs_with_props(
        self, mock_detect_zpool_features: MagicMock, mock_detect_available_programs_remote: MagicMock
    ) -> None:
        """Verify that --preserve-properties is allowed on ZFS >= 2.2.0."""
        with patch.object(self.p, "is_program_available", return_value=True):
            # This should not raise an exception
            detect_available_programs(self.job)

    @patch.object(bzfs_main.detect, "detect_zpool_features")
    @patch.object(bzfs_main.detect, "detect_available_programs_remote")
    def test_preserve_properties_succeeds_on_old_zfs_without_props(
        self, mock_detect_zpool_features: MagicMock, mock_detect_available_programs_remote: MagicMock
    ) -> None:
        """Verify --preserve-properties is allowed on old ZFS if --props is not used."""
        self.p.zfs_send_program_opts.remove("--props")  # Modify the params for this test case
        with patch.object(self.p, "is_program_available") as mock_is_available:

            def side_effect(program: str, location: str) -> bool:
                # Simulate old ZFS by returning False for version checks, but True for other programs like ssh.
                if program == ZFS_VERSION_IS_AT_LEAST_2_2_0:
                    return False
                return True

            mock_is_available.side_effect = side_effect
            # This should not raise an exception
            detect_available_programs(self.job)


#############################################################################
class TestPythonVersionCheck(AbstractTestCase):
    """Test version check near top of program:
    if sys.version_info < (3, 8):
        print(f"ERROR: {prog_name} requires Python version >= 3.8!", file=sys.stderr)
        sys.exit(die_status)
    """

    @patch("sys.exit")
    @patch("sys.version_info", new=(3, 6))
    def test_version_below_3_8(self, mock_exit: MagicMock) -> None:
        with patch("sys.stderr"):
            import importlib

            from bzfs_main import bzfs

            importlib.reload(bzfs)  # Reload module to apply version patch
            mock_exit.assert_called_with(DIE_STATUS)

    @patch("sys.exit")
    @patch("sys.version_info", new=(3, 8))
    def test_version_3_8_or_higher(self, mock_exit: MagicMock) -> None:
        import importlib

        from bzfs_main import bzfs

        importlib.reload(bzfs)  # Reload module to apply version patch
        mock_exit.assert_not_called()


#############################################################################
class TestPerformance(AbstractTestCase):

    def test_close_fds(self) -> None:
        """See https://bugs.python.org/issue42738
        and https://github.com/python/cpython/pull/113118/commits/654f63053aee162a6e59fa894b2cd8ee82a33a77"""
        iters = 2000
        runs = 3
        for close_fds in [False, True]:
            for _ in range(runs):
                start_time_nanos = time.time_ns()
                for _ in range(iters):
                    cmd = ["echo", "hello"]
                    stdout = subprocess.run(
                        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True, close_fds=close_fds
                    ).stdout
                    self.assertTrue(stdout)
                secs = (time.time_ns() - start_time_nanos) / 1000_000_000
                print(f"close_fds={close_fds}: Took {secs:.1f} seconds, iters/sec: {iters/secs:.1f}")
