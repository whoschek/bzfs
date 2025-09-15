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
"""Unit tests for the program detection helpers used by ``bzfs``."""

from __future__ import annotations
import platform
import subprocess
import time
import unittest
from unittest.mock import (
    patch,
)

import bzfs_main.detect
from bzfs_main import bzfs
from bzfs_main.configuration import Remote
from bzfs_main.connection import (
    DEDICATED,
    SHARED,
    ConnectionPools,
)
from bzfs_main.detect import (
    RemoteConfCacheItem,
    _validate_default_shell,
    detect_available_programs,
)
from bzfs_tests.abstract_testcase import AbstractTestCase
from bzfs_tests.tools import stop_on_failure_subtest


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestRemoteConfCache,
        TestDisableAndHelpers,
        TestDetectAvailablePrograms,
        TestDetectAvailableProgramsRemote,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestRemoteConfCache(AbstractTestCase):

    def test_remote_conf_cache_hit_skips_detection(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        p = self.make_params(args=args)
        job = bzfs.Job()
        job.params = p
        p.src = Remote("src", args, p)
        p.dst = Remote("dst", args, p)
        p.src.ssh_host = "host"
        p.src.ssh_user_host = "host"
        p.dst.ssh_host = "host2"
        p.dst.ssh_user_host = "host2"
        job.params.available_programs["local"] = {"ssh": ""}
        pools = ConnectionPools(p.src, {SHARED: 1, DEDICATED: 1})
        item = RemoteConfCacheItem(pools, {"os": "Linux", "ssh": ""}, {"feat": "on"}, time.monotonic_ns())
        job.remote_conf_cache[p.src.cache_key()] = item
        job.remote_conf_cache[p.dst.cache_key()] = item
        with patch.object(bzfs_main.detect, "_detect_available_programs_remote") as d1, patch.object(
            bzfs_main.detect, "_detect_zpool_features"
        ) as d2:
            detect_available_programs(job)
            d1.assert_not_called()
            d2.assert_not_called()

    def test_remote_conf_cache_miss_runs_detection(self) -> None:
        args = self.argparser_parse_args(["src", "dst", "--daemon-remote-conf-cache-ttl", "1000 milliseconds"])
        p = self.make_params(args=args)
        job = bzfs.Job()
        job.params = p
        p.src = Remote("src", args, p)
        p.dst = Remote("dst", args, p)
        p.src.ssh_host = "host"
        p.src.ssh_user_host = "host"
        p.dst.ssh_host = "host2"
        p.dst.ssh_user_host = "host2"
        job.params.available_programs["local"] = {"ssh": ""}
        pools = ConnectionPools(p.src, {SHARED: 1, DEDICATED: 1})
        expired_ts = time.monotonic_ns() - p.remote_conf_cache_ttl_nanos - 1
        item = RemoteConfCacheItem(pools, {"os": "Linux"}, {"feat": "on"}, expired_ts)
        job.remote_conf_cache[p.src.cache_key()] = item
        job.remote_conf_cache[p.dst.cache_key()] = item
        with patch.object(bzfs_main.detect, "_detect_available_programs_remote") as d1, patch.object(
            bzfs_main.detect, "_detect_zpool_features"
        ) as d2:
            d1.side_effect = lambda job_, r, host: {"ssh": ""}
            d2.side_effect = lambda job_, r, available_programs: {"feat": "on"}
            detect_available_programs(job)
            self.assertEqual(2, d1.call_count)
            self.assertEqual(2, d2.call_count)


#############################################################################
class TestDisableAndHelpers(AbstractTestCase):

    def test_disable_program(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        p = self.make_params(args=args)
        p.available_programs = {"local": {"zpool": ""}, "src": {"zpool": ""}}
        bzfs_main.detect._disable_program(p, "zpool", ["local", "src"])
        self.assertNotIn("zpool", p.available_programs["local"])
        self.assertNotIn("zpool", p.available_programs["src"])

    def test_find_available_programs_contains_commands(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        p = self.make_params(args=args)
        cmds = bzfs_main.detect._find_available_programs(p)
        self.assertIn("default_shell-", cmds)
        self.assertIn(f"command -v {p.zpool_program}", cmds)

    def test_is_solaris_zfs_and_location(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        p = self.make_params(args=args)
        p.available_programs = {"src": {"zfs": "notOpenZFS"}}
        with patch.object(platform, "system", return_value="SunOS"):
            self.assertTrue(bzfs_main.detect.is_solaris_zfs_location(p, "local"))
        self.assertTrue(bzfs_main.detect.is_solaris_zfs(p, p.src))

    def test_is_dummy(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        p = self.make_params(args=args)
        r = Remote("src", args, p)
        r.root_dataset = bzfs_main.detect.DUMMY_DATASET
        self.assertTrue(bzfs_main.detect.is_dummy(r))
        r.root_dataset = "nondummy"
        self.assertFalse(bzfs_main.detect.is_dummy(r))

    def test_validate_default_shell(self) -> None:
        args = self.argparser_parse_args(args=["src", "dst"])
        p = self.make_params(args=args)
        remote = Remote("src", args, p)
        _validate_default_shell("/bin/sh", remote.location, remote.ssh_user_host)
        _validate_default_shell("/bin/bash", remote.location, remote.ssh_user_host)
        with self.assertRaises(SystemExit):
            _validate_default_shell("/bin/csh", remote.location, remote.ssh_user_host)
        with self.assertRaises(SystemExit):
            _validate_default_shell("/bin/tcsh", remote.location, remote.ssh_user_host)
        with self.assertRaises(SystemExit):
            _validate_default_shell("csh", remote.location, remote.ssh_user_host)
        with self.assertRaises(SystemExit):
            _validate_default_shell("tcsh", remote.location, remote.ssh_user_host)


#############################################################################
class TestDetectAvailablePrograms(AbstractTestCase):

    def _setup_job(self) -> bzfs.Job:
        args = self.argparser_parse_args(["src", "dst"])
        p = self.make_params(args=args)
        job = bzfs.Job()
        job.params = p
        p.src = Remote("src", args, p)
        p.dst = Remote("dst", args, p)
        job.params.available_programs = {"local": {"sh": ""}, "src": {}, "dst": {}}
        job.params.zpool_features = {"src": {}, "dst": {}}
        return job

    def test_disable_flags_remove_programs(self) -> None:
        job = self._setup_job()
        p = job.params
        for attr, prog in (
            ("compression_program", "zstd"),
            ("mbuffer_program", "mbuffer"),
            ("ps_program", "ps"),
            ("pv_program", "pv"),
            ("shell_program", "sh"),
            ("sudo_program", "sudo"),
            ("zpool_program", "zpool"),
        ):
            with stop_on_failure_subtest(prog=prog):
                setattr(p, attr, bzfs_main.detect.DISABLE_PRG)
                p.available_programs = {"local": {prog: ""}, "src": {prog: ""}, "dst": {prog: ""}}
                with patch.object(bzfs_main.detect, "_detect_available_programs_remote"), patch.object(
                    bzfs_main.detect, "_detect_zpool_features"
                ):
                    detect_available_programs(job)
                self.assertNotIn(prog, p.available_programs["local"])
                self.assertNotIn(prog, p.available_programs["src"])
                self.assertNotIn(prog, p.available_programs["dst"])

    def test_local_shell_exit_codes(self) -> None:
        job = self._setup_job()
        p = job.params
        outputs = [
            subprocess.CompletedProcess([], 0, stdout="sh\n"),
            subprocess.CompletedProcess([], 0, stdout=""),
        ]
        p.available_programs.pop("local", None)
        with patch("subprocess.run", side_effect=outputs), patch.object(
            bzfs_main.detect, "_detect_available_programs_remote"
        ), patch.object(bzfs_main.detect, "_detect_zpool_features"):
            detect_available_programs(job)
        self.assertIn("sh", p.available_programs["local"])
        job = self._setup_job()
        p = job.params
        outputs = [
            subprocess.CompletedProcess([], 0, stdout="sh\n"),
            subprocess.CompletedProcess([], 1, stdout=""),
        ]
        p.available_programs.pop("local", None)
        with patch("subprocess.run", side_effect=outputs), patch.object(
            bzfs_main.detect, "_detect_available_programs_remote"
        ), patch.object(bzfs_main.detect, "_detect_zpool_features"):
            detect_available_programs(job)
        self.assertNotIn("sh", p.available_programs["local"])

    def test_preserve_and_sudo_and_delegation(self) -> None:
        job = self._setup_job()
        p = job.params
        p.args.preserve_properties = ["x"]
        p.zfs_send_program_opts = ["--props"]
        p.available_programs[p.dst.location] = {}
        with patch.object(bzfs_main.detect, "_detect_available_programs_remote", return_value={}), patch.object(
            bzfs_main.detect, "_detect_zpool_features", return_value={}
        ), self.assertRaises(SystemExit):
            detect_available_programs(job)

        job = self._setup_job()
        p = job.params
        p.dst.sudo = "sudo -n"
        p.available_programs[p.dst.location] = {}
        with patch.object(bzfs_main.detect, "_detect_available_programs_remote", return_value={}), patch.object(
            bzfs_main.detect, "_detect_zpool_features", return_value={}
        ), self.assertRaises(SystemExit):
            detect_available_programs(job)

        job = self._setup_job()
        p = job.params
        p.dst.use_zfs_delegation = True
        p.zpool_features[p.dst.location] = {"delegation": "off"}
        with patch.object(bzfs_main.detect, "_detect_available_programs_remote", return_value={}), patch.object(
            bzfs_main.detect,
            "_detect_zpool_features",
            side_effect=lambda job_, r, ap: {"delegation": "off"} if r.location == p.dst.location else {},
        ), self.assertRaises(SystemExit):
            detect_available_programs(job)


#############################################################################
class TestDetectAvailableProgramsRemote(AbstractTestCase):

    def _setup(self) -> tuple[bzfs.Job, Remote]:
        args = self.argparser_parse_args(["src", "dst"])
        p = self.make_params(args=args)
        job = bzfs.Job()
        job.params = p
        p.src = Remote("src", args, p)
        return job, p.src

    def test_zfs_version_parsing_and_shell(self) -> None:
        job, remote = self._setup()
        p = job.params

        def run(*args: str, cmd: list[str] | None = None, **kw: str) -> str:
            command = cmd if cmd is not None else args[0]
            if "--version" in command:
                return "zfs-2.2.3\n"
            return "sh\n"

        with patch.object(bzfs_main.detect, "run_ssh_command", side_effect=run):
            avail = bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
            p.available_programs[remote.location] = avail
        self.assertEqual("2.2.3", p.available_programs[remote.location]["zfs"])
        self.assertIn("sh", p.available_programs[remote.location])
        self.assertIn(bzfs_main.detect.ZFS_VERSION_IS_AT_LEAST_2_2_0, p.available_programs[remote.location])

    def test_zfs_version_parsing_space_variant(self) -> None:
        """Accept version strings like 'zfs 2.2.4' without a hyphen."""
        job, remote = self._setup()
        p = job.params

        def run(*args: str, cmd: list[str] | None = None, **kw: str) -> str:
            command = cmd if cmd is not None else args[0]
            if "--version" in command:
                return "zfs 2.2.4\n"
            return "sh\n"

        with patch.object(bzfs_main.detect, "run_ssh_command", side_effect=run):
            avail = bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
            p.available_programs[remote.location] = avail
        self.assertEqual("2.2.4", p.available_programs[remote.location]["zfs"])
        self.assertIn("sh", p.available_programs[remote.location])
        self.assertIn(bzfs_main.detect.ZFS_VERSION_IS_AT_LEAST_2_2_0, p.available_programs[remote.location])

    def test_zfs_version_parsing_unexpected_format_dies(self) -> None:
        """Die cleanly on unparseable 'zfs --version' output."""
        job, remote = self._setup()

        def run(*args: str, cmd: list[str] | None = None, **kw: str) -> str:
            command = cmd if cmd is not None else args[0]
            if "--version" in command:
                return "zfs-version unknown build\n"
            return "sh\n"

        with patch.object(bzfs_main.detect, "run_ssh_command", side_effect=run), self.assertRaises(SystemExit) as cm:
            bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
        self.assertIn("Unparsable zfs version string", str(cm.exception))

    def test_zfs_version_parsing_variants(self) -> None:
        job, remote = self._setup()
        p = job.params
        cases = [
            ("zfs-2.2.4\n", "2.2.4", False),
            ("zfs 2.2.4\n", "2.2.4", False),
            ("zfs 2.2.4~ubuntu5\n", "2.2.4", False),
            ("zfs 2.24~ubuntu5\n", None, True),  # only two components -> should die
            ("zfs-2.2.4rc5\n", "2.2.4", False),
        ]

        for output, expect_version, expect_die in cases:
            with self.subTest(output=output.strip()):
                out = output  # bind loop var for closure

                def run(*args: str, cmd: list[str] | None = None, out: str = out, **kw: str) -> str:
                    command = cmd if cmd is not None else args[0]
                    if "--version" in command:
                        return out
                    return "sh\n"

                if expect_die:
                    with patch.object(bzfs_main.detect, "run_ssh_command", side_effect=run), self.assertRaises(
                        SystemExit
                    ) as cm:
                        bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
                    self.assertIn("Unparsable zfs version string", str(cm.exception))
                else:
                    with patch.object(bzfs_main.detect, "run_ssh_command", side_effect=run):
                        avail = bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
                        p.available_programs[remote.location] = avail
                    self.assertEqual(expect_version, p.available_programs[remote.location]["zfs"])
                    self.assertIn("sh", p.available_programs[remote.location])
                    # 2.2.4 >= 2.2.0
                    self.assertIn(bzfs_main.detect.ZFS_VERSION_IS_AT_LEAST_2_2_0, p.available_programs[remote.location])

    def test_shell_program_disabled(self) -> None:
        job, remote = self._setup()
        p = job.params
        p.shell_program = bzfs_main.detect.DISABLE_PRG
        with patch.object(bzfs_main.detect, "run_ssh_command", return_value="zfs-2.1.0\n"):
            avail = bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
            p.available_programs[remote.location] = avail
        self.assertNotIn("zpool", p.available_programs[remote.location])
        self.assertNotIn("sh", p.available_programs[remote.location])

    def test_errors_raise_die(self) -> None:
        job, remote = self._setup()
        with patch.object(
            bzfs_main.detect,
            "run_ssh_command",
            side_effect=FileNotFoundError(),
        ), self.assertRaises(SystemExit):
            bzfs_main.detect._detect_available_programs_remote(job, remote, "host")

    def test_not_openzfs_handling(self) -> None:
        job, remote = self._setup()
        p = job.params
        err = subprocess.CalledProcessError(
            returncode=1, cmd="zfs", output="", stderr="unrecognized command '--version'\nrun: zfs help"
        )
        with patch.object(bzfs_main.detect, "run_ssh_command", side_effect=err):
            avail = bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
            p.available_programs[remote.location] = avail
        self.assertEqual("notOpenZFS", p.available_programs[remote.location]["zfs"])

    def test_called_process_error_non_zfs(self) -> None:
        job, remote = self._setup()
        err = subprocess.CalledProcessError(returncode=1, cmd="zfs", output="bad", stderr="fail")
        with patch.object(bzfs_main.detect, "run_ssh_command", side_effect=err), self.assertRaises(SystemExit):
            bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
