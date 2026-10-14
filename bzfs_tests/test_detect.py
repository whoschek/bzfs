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

from __future__ import (
    annotations,
)
import errno
import os
import platform
import subprocess
import tempfile
import threading
import time
import unittest
from typing import (
    cast,
)
from unittest.mock import (
    MagicMock,
    patch,
)

import bzfs_main.detect
from bzfs_main import (
    bzfs,
)
from bzfs_main.configuration import (
    Remote,
)
from bzfs_main.detect import (
    RemoteConfCacheItem,
    _validate_default_shell,
    detect_available_programs,
    is_version_at_least,
    is_version_at_most,
)
from bzfs_main.util.connection import (
    DEDICATED,
    SHARED,
    ConnectionPools,
)
from bzfs_main.util.connection_lease import (
    ConnectionLease,
)
from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)
from bzfs_tests.tools import (
    stop_on_failure_subtest,
    suppress_output,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestVersionComparison,
        TestRemoteConfCache,
        TestDetectionConnectionCleanup,
        TestDisableAndHelpers,
        TestDetectAvailablePrograms,
        TestDetectAvailableProgramsRemote,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestVersionComparison(AbstractTestCase):

    def test_is_version_at_least(self) -> None:
        self.assertFalse(is_version_at_least("0.9.9", "1.0.0"))
        self.assertTrue(is_version_at_least("1.0.0", "1.0.0"))
        self.assertTrue(is_version_at_least("1.0.1", "1.0.0"))

        self.assertFalse(is_version_at_least("0.11.9", "1.10.5"))
        self.assertFalse(is_version_at_least("1.9.9", "1.10.5"))
        self.assertFalse(is_version_at_least("1.10.0", "1.10.5"))
        self.assertFalse(is_version_at_least("1.10.4", "1.10.5"))
        self.assertTrue(is_version_at_least("1.10.5", "1.10.5"))
        self.assertTrue(is_version_at_least("1.10.6", "1.10.5"))
        self.assertTrue(is_version_at_least("1.11.0", "1.10.5"))
        self.assertTrue(is_version_at_least("2.0.0", "1.10.5"))

    def test_is_version_at_most(self) -> None:
        self.assertTrue(is_version_at_most("0.9.9", "1.0.0"))
        self.assertTrue(is_version_at_most("1.0.0", "1.0.0"))
        self.assertFalse(is_version_at_most("1.0.1", "1.0.0"))

        self.assertTrue(is_version_at_most("0.11.9", "1.10.5"))
        self.assertTrue(is_version_at_most("1.9.9", "1.10.5"))
        self.assertTrue(is_version_at_most("1.10.0", "1.10.5"))
        self.assertTrue(is_version_at_most("1.10.4", "1.10.5"))
        self.assertTrue(is_version_at_most("1.10.5", "1.10.5"))
        self.assertFalse(is_version_at_most("1.10.6", "1.10.5"))
        self.assertFalse(is_version_at_most("1.11.0", "1.10.5"))
        self.assertFalse(is_version_at_most("2.0.0", "1.10.5"))


#############################################################################
class TestRemoteConfCache(AbstractTestCase):

    def test_remote_conf_cache_hit_skips_detection(self) -> None:
        args = self.argparser_parse_args(["src", "dst"])
        p = self.make_params(args=args)
        job = bzfs.Job()
        job.params = p
        p.src = Remote("src", args, p)  # type: ignore[misc]  # cannot assign to final attribute
        p.dst = Remote("dst", args, p)  # type: ignore[misc]  # cannot assign to final attribute
        p.src.pool = "tank"
        p.src.ssh_host = "host"
        p.src.ssh_user_host = "host"
        p.dst.pool = "tank"
        p.dst.ssh_host = "host2"
        p.dst.ssh_user_host = "host2"
        job.params.available_programs["local"] = {"ssh": ""}
        pools = ConnectionPools(remote=p.src, capacities={SHARED: 1, DEDICATED: 1})
        item = RemoteConfCacheItem(pools, {"os": "Linux", "ssh": ""}, {"tank": {"feat": "on"}}, time.monotonic_ns())
        job.remote_conf_cache[p.src.cache_key()] = item
        job.remote_conf_cache[p.dst.cache_key()] = item
        with (
            patch.object(bzfs_main.detect, "_detect_available_programs_remote") as d1,
            patch.object(bzfs_main.detect, "_detect_zpool_features") as d2,
        ):
            detect_available_programs(job)
            d1.assert_not_called()
            d2.assert_not_called()

    def test_remote_conf_cache_miss_runs_detection(self) -> None:
        args = self.argparser_parse_args(["src", "dst", "--daemon-remote-conf-cache-ttl", "1000 milliseconds"])
        p = self.make_params(args=args)
        job = bzfs.Job()
        job.params = p
        p.src = Remote("src", args, p)  # type: ignore[misc]  # cannot assign to final attribute
        p.dst = Remote("dst", args, p)  # type: ignore[misc]  # cannot assign to final attribute
        p.src.pool = "tank"
        p.src.ssh_host = "host"
        p.src.ssh_user_host = "host"
        p.dst.pool = "tank"
        p.dst.ssh_host = "host2"
        p.dst.ssh_user_host = "host2"
        job.params.available_programs["local"] = {"ssh": ""}
        pools = ConnectionPools(remote=p.src, capacities={SHARED: 1, DEDICATED: 1})
        expired_ts = time.monotonic_ns() - p.remote_conf_cache_ttl_nanos - 1
        item = RemoteConfCacheItem(pools, {"os": "Linux"}, {"tank": {"feat": "on"}}, expired_ts)
        job.remote_conf_cache[p.src.cache_key()] = item
        job.remote_conf_cache[p.dst.cache_key()] = item
        # Create thread-safe wrappers around mocks so concurrent calls are counted reliably
        lock = threading.Lock()
        d1 = MagicMock(side_effect=lambda job_, r, host: {"ssh": ""})
        d2 = MagicMock(side_effect=lambda job_, r, available_programs: {"feat": "on"})

        def _ts1(
            job_: bzfs.Job,
            r: Remote,
            host: str,
            _lock: threading.Lock = lock,
            _mock: MagicMock = d1,
        ) -> dict[str, str]:
            with _lock:
                return cast(dict[str, str], _mock(job_, r, host))

        def _ts2(
            job_: bzfs.Job,
            r: Remote,
            available_programs: dict[str, str],
            _lock: threading.Lock = lock,
            _mock: MagicMock = d2,
        ) -> dict[str, str]:
            with _lock:
                return cast(dict[str, str], _mock(job_, r, available_programs))

        with (
            patch.object(bzfs_main.detect, "_detect_available_programs_remote", new=_ts1),
            patch.object(bzfs_main.detect, "_detect_zpool_features", new=_ts2),
        ):
            detect_available_programs(job)
        self.assertEqual(2, d1.call_count)
        self.assertEqual(2, d2.call_count)

    def test_remote_conf_cache_keeps_pool_guids_scoped_by_pool(self) -> None:
        """Verify zpool GUIDs remain pool-keyed within an endpoint cache.

        The source entry is pre-cached so observed zpool queries belong only to the destination. Visiting pools A, B, then A
        on one endpoint must query A and B once each, retain both GUIDs under their pool keys, and reuse A on the final visit.
        Rejecting the GUID in endpoint-scoped `available_programs` guards against reusing one pool's validation result when
        visiting another pool.
        """
        args = self.argparser_parse_args(["src", "dst", "--daemon-remote-conf-cache-ttl", "1 days"])
        p = self.make_params(args=args)
        job = bzfs.Job()
        job.params = p
        p.src.pool = "srcpool"
        p.dst.pool = "pool-a"
        p.available_programs["local"] = {"ssh": ""}
        src_pools = ConnectionPools(remote=p.src, capacities={SHARED: 1, DEDICATED: 1})
        job.remote_conf_cache[p.src.cache_key()] = RemoteConfCacheItem(src_pools, {}, {p.src.pool: {}}, time.monotonic_ns())
        pool_guids: dict[str, str] = {"pool-a": "1229782938247303441", "pool-b": "2459565876494606882"}
        queried_pools: list[str] = []

        def detect_pool_guid(remote: Remote, *_args: object, **_kwargs: object) -> str:
            queried_pools.append(remote.pool)
            return f"{bzfs_main.detect.POOL_GUID}\t{pool_guids[remote.pool]}\n"

        with (
            patch.object(bzfs_main.detect, "_detect_available_programs_remote", return_value={"zpool": ""}),
            patch.object(job, "run_ssh_command_with_retries", side_effect=detect_pool_guid),
            patch.object(job, "try_ssh_command_with_retries") as fallback,
        ):
            detect_available_programs(job)
            p.dst.pool = "pool-b"
            detect_available_programs(job)
            p.dst.pool = "pool-a"
            detect_available_programs(job)

        self.assertEqual(["pool-a", "pool-b"], queried_pools)
        self.assertEqual(
            {
                "pool-a": {bzfs_main.detect.POOL_GUID: pool_guids["pool-a"]},
                "pool-b": {bzfs_main.detect.POOL_GUID: pool_guids["pool-b"]},
            },
            p.zpool_features["dst"],
        )
        self.assertNotIn(bzfs_main.detect.POOL_GUID, p.available_programs["dst"])
        fallback.assert_not_called()


#############################################################################
class TestDetectionConnectionCleanup(AbstractTestCase):
    """Exercises detection failures with real leases and mocked remote probes to verify deterministic pool ownership."""

    def setUp(self) -> None:
        """Isolates SSH leases and caches the destination so most tests exercise one detection worker."""
        tmpdir = tempfile.TemporaryDirectory(dir="/tmp" if platform.system() == "Darwin" else None)
        self.addCleanup(tmpdir.cleanup)
        args = self.argparser_parse_args(
            [
                "src",
                "dst",
                "--ssh-src-config-file=none",
                "--ssh-dst-config-file=none",
                "--daemon-remote-conf-cache-ttl=1 days",
            ]
        )
        self.job = bzfs.Job()
        self.job.params = p = self.make_params(args=args)
        p.available_programs["local"] = {"ssh": ""}
        for remote in (p.src, p.dst):
            remote.ssh_user_host = f"{remote.location}-host"
            remote.pool = "tank"
            remote.basis_root_dataset = "tank/data"
            remote.ssh_socket_dir = tmpdir.name
        dst_pools = ConnectionPools(p.dst, capacities={SHARED: 1, DEDICATED: 1})
        self.job.remote_conf_cache[p.dst.cache_key()] = RemoteConfCacheItem(dst_pools, {}, {p.dst.pool: {}})
        self.pools: list[ConnectionPools] = [dst_pools]
        self.leases: dict[str, ConnectionLease] = {}
        self.error: BaseException | None = None
        self.fail_phase = "programs"
        self.barrier: threading.Barrier | None = None
        self.lock = threading.Lock()
        self.addCleanup(self._cleanup_pools)
        for name, probe in (
            ("_detect_available_programs_remote", self._detect_programs),
            ("_detect_zpool_features", self._detect_features),
        ):
            patcher = patch.object(bzfs_main.detect, name, new=probe)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _cleanup_pools(self) -> None:
        for pools in self.pools:
            pools.shutdown()

    def _detect_programs(self, job: bzfs.Job, remote: Remote, _ssh_user_host: str) -> dict[str, str]:
        pools = job.params.connection_pools[remote.location]
        with self.lock:
            self.pools.append(pools)
        with pools.pool(SHARED).connection() as conn:
            lease = conn._connection_lease
            assert lease is not None
            with self.lock:
                self.leases[remote.location] = lease
            if self.barrier is not None:
                self.barrier.wait(timeout=10)
            if remote.location == "src" and self.fail_phase == "programs" and self.error is not None:
                raise self.error
        return {"zfs": "2.2.0"}

    def _detect_features(self, _job: bzfs.Job, remote: Remote, _available_programs: dict[str, str]) -> dict[str, str]:
        if remote.location == "src" and self.fail_phase == "features" and self.error is not None:
            raise self.error
        return {"delegation": "on"}

    def _assert_released(self, lease: ConnectionLease) -> None:
        self.assertFalse(os.path.exists(lease.used_path))
        self.assertTrue(os.path.isfile(lease.free_path))
        with self.assertRaises(OSError) as cm:
            os.fstat(lease.fd)
        self.assertEqual(errno.EBADF, cm.exception.errno)

    def test_initial_failures_release_leases_and_allow_recovery(self) -> None:
        """Repeated transport and missing-pool failures must close leases; a later successful attempt remains reusable."""
        src = self.job.params.src
        failures = [
            ("programs", subprocess.TimeoutExpired("ssh", 1)),
            ("features", SystemExit("Pool does not exist")),
            ("programs", SystemExit("ssh exit code 255")),
        ]
        for self.fail_phase, self.error in failures:
            with stop_on_failure_subtest(phase=self.fail_phase, error=type(self.error).__name__):
                with self.assertRaises(type(self.error)) as cm:
                    detect_available_programs(self.job)
                self.assertIs(self.error, cm.exception)
                self.assertNotIn(src.cache_key(), self.job.remote_conf_cache)
                self._assert_released(self.leases["src"])

        self.error = None
        detect_available_programs(self.job)
        pools = self.job.params.connection_pools["src"]
        self.assertIs(pools, self.job.remote_conf_cache[src.cache_key()].connection_pools)
        with patch.object(bzfs_main.detect, "_detect_available_programs_remote", side_effect=AssertionError("cache miss")):
            detect_available_programs(self.job)
        with pools.pool(SHARED).connection() as conn:
            self.assertIs(self.leases["src"], conn._connection_lease)
        os.fstat(self.leases["src"].fd)

    def test_failed_refresh_preserves_cached_pool(self) -> None:
        """Failure while detecting another zpool on a cached endpoint must preserve its connection for subsequent retries."""
        detect_available_programs(self.job)
        src = self.job.params.src
        pools = self.job.params.connection_pools["src"]
        lease = self.leases["src"]
        src.pool = "missingpool"
        self.fail_phase = "features"
        self.error = SystemExit("Pool does not exist")
        with self.assertRaises(SystemExit):
            detect_available_programs(self.job)
        self.assertIs(pools, self.job.remote_conf_cache[src.cache_key()].connection_pools)
        self.assertIs(lease, self.leases["src"])
        os.fstat(lease.fd)

        self.error = None
        detect_available_programs(self.job)
        self.assertIs(lease, self.leases["src"])
        os.fstat(lease.fd)

    def test_failure_after_cache_publication_preserves_pool(self) -> None:
        """A delegation error after cache insertion leaves the pool owned by the cache and available for normal shutdown."""
        src = self.job.params.src
        src.use_zfs_delegation = True
        with (
            patch.object(bzfs_main.detect, "_detect_zpool_features", return_value={"delegation": "off"}),
            self.assertRaises(SystemExit),
            suppress_output(),
        ):
            detect_available_programs(self.job)
        self.assertIs(self.job.params.connection_pools["src"], self.job.remote_conf_cache[src.cache_key()].connection_pools)
        lease = self.leases["src"]
        os.fstat(lease.fd)
        self.job.shutdown()
        self._assert_released(lease)

    def test_cleanup_failure_preserves_detection_error(self) -> None:
        """Keep the detection failure primary when lease cleanup also raises, while retaining cleanup diagnostics."""
        self.error = SystemExit("ssh exit code 255")
        cleanup_error = OSError("lease cleanup failed")
        release = ConnectionLease.release

        def failing_release(lease: ConnectionLease) -> None:
            release(lease)
            raise cleanup_error

        with patch.object(ConnectionLease, "release", new=failing_release), self.assertRaises(SystemExit) as cm:
            detect_available_programs(self.job)
        self.assertIs(self.error, cm.exception)
        self.assertIs(cleanup_error, cm.exception.__context__)
        self._assert_released(self.leases["src"])

    def test_parallel_failure_preserves_successful_peer(self) -> None:
        """Synchronize both workers with live leases, then fail one and verify that only its lease is released."""
        dst = self.job.params.dst
        del self.job.remote_conf_cache[dst.cache_key()]
        self.barrier = threading.Barrier(2)
        self.error = SystemExit("ssh exit code 255")
        with self.assertRaises(SystemExit):
            detect_available_programs(self.job)
        self._assert_released(self.leases["src"])
        self.assertIs(self.job.params.connection_pools["dst"], self.job.remote_conf_cache[dst.cache_key()].connection_pools)
        os.fstat(self.leases["dst"].fd)


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
        for shell in [
            "csh",
            "tcsh",
            "elvish",
            "fish",
            "nu",
            "nushell",
            "xonsh",
            "/bin/csh",
            "/bin/tcsh",
            "/usr/local/bin/elvish",
            "/usr/local/bin/fish",
            "/usr/local/bin/nu",
            "/usr/local/bin/nushell",
            "/opt/homebrew/bin/xonsh",
        ]:
            with self.assertRaises(SystemExit, msg=shell):
                _validate_default_shell(shell, remote.location, remote.ssh_user_host)


#############################################################################
class TestDetectAvailablePrograms(AbstractTestCase):

    def _setup_job(self) -> bzfs.Job:
        args = self.argparser_parse_args(["src", "dst"])
        p = self.make_params(args=args)
        job = bzfs.Job()
        job.params = p
        p.src = Remote("src", args, p)  # type: ignore[misc]  # cannot assign to final attribute
        p.dst = Remote("dst", args, p)  # type: ignore[misc]  # cannot assign to final attribute
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
                with (
                    patch.object(bzfs_main.detect, "_detect_available_programs_remote"),
                    patch.object(bzfs_main.detect, "_detect_zpool_features"),
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
        with (
            patch.object(job.subprocesses, "subprocess_run", side_effect=outputs),
            patch.object(bzfs_main.detect, "_detect_available_programs_remote"),
            patch.object(bzfs_main.detect, "_detect_zpool_features"),
        ):
            detect_available_programs(job)
        self.assertIn("sh", p.available_programs["local"])
        job = self._setup_job()
        p = job.params
        outputs = [
            subprocess.CompletedProcess([], 0, stdout="sh\n"),
            subprocess.CompletedProcess([], 1, stdout=""),
        ]
        p.available_programs.pop("local", None)
        with (
            patch.object(job.subprocesses, "subprocess_run", side_effect=outputs),
            patch.object(bzfs_main.detect, "_detect_available_programs_remote"),
            patch.object(bzfs_main.detect, "_detect_zpool_features"),
        ):
            detect_available_programs(job)
        self.assertNotIn("sh", p.available_programs["local"])

    def test_preserve_and_sudo_and_delegation(self) -> None:
        job = self._setup_job()
        p = job.params
        p.args.preserve_properties = ["x"]
        p.zfs_send_program_opts = ["--props"]  # type: ignore[misc]  # cannot assign to final attribute
        p.available_programs[p.dst.location] = {}
        with (
            patch.object(bzfs_main.detect, "_detect_available_programs_remote", return_value={}),
            patch.object(bzfs_main.detect, "_detect_zpool_features", return_value={}),
            self.assertRaises(SystemExit),
        ):
            detect_available_programs(job)

        job = self._setup_job()
        p = job.params
        p.dst.sudo = "sudo -n"
        p.available_programs[p.dst.location] = {}
        with (
            patch.object(bzfs_main.detect, "_detect_available_programs_remote", return_value={}),
            patch.object(bzfs_main.detect, "_detect_zpool_features", return_value={}),
            self.assertRaises(SystemExit),
        ):
            detect_available_programs(job)

        job = self._setup_job()
        p = job.params
        p.dst.use_zfs_delegation = True
        p.zpool_features[p.dst.location] = {"tank": {"delegation": "off"}}
        with (
            patch.object(bzfs_main.detect, "_detect_available_programs_remote", return_value={}),
            patch.object(
                bzfs_main.detect,
                "_detect_zpool_features",
                side_effect=lambda job_, r, ap: {"delegation": "off"} if r.location == p.dst.location else {},
            ),
            self.assertRaises(SystemExit),
        ):
            detect_available_programs(job)

    def test_detect_available_programs_parses_uname_default_shell_and_getconf(self) -> None:
        """Verifies detect_available_programs parses and normalizes special keys.

        Assumes the remote detection returns `uname-*`, `default_shell-*`, and `getconf_cpu_count-*` sentinel keys.
        Design rationale: ensures the post-processing replaces sentinel keys with stable `uname/os/default_shell/getconf_cpu_count`.
        """
        job = self._setup_job()
        p = job.params
        p.available_programs["local"] = {
            "uname-Linux local 6.1.0 x86_64": "",
            "default_shell-/bin/sh": "",
            "getconf_cpu_count-4": "",
            "sh": "",
        }

        def detect_remote(_job: bzfs.Job, _remote: Remote, _ssh_user_host: str) -> dict[str, str]:
            return {
                "uname-Linux remote 5.15.0 x86_64": "",
                "default_shell-/bin/bash": "",
                "getconf_cpu_count-8": "",
                "sh": "",
                "zfs": "2.2.4",
            }

        with (
            patch.object(bzfs_main.detect, "_detect_available_programs_remote", side_effect=detect_remote),
            patch.object(bzfs_main.detect, "_detect_zpool_features", return_value={}),
        ):
            detect_available_programs(job)

        for key in ("local", "src", "dst"):
            with self.subTest(location=key):
                programs = p.available_programs[key]
                self.assertIn("uname", programs)
                self.assertIn("os", programs)
                self.assertIn("default_shell", programs)
                self.assertIn("getconf_cpu_count", programs)
                self.assertTrue(all(not name.startswith("uname-") for name in programs))
                self.assertTrue(all(not name.startswith("default_shell-") for name in programs))
                self.assertTrue(all(not name.startswith("getconf_cpu_count-") for name in programs))

        self.assertEqual("Linux", p.available_programs["src"]["os"])
        self.assertEqual("Linux", p.available_programs["dst"]["os"])

    def test_detect_available_programs_dies_on_incompatible_remote_default_shell(self) -> None:
        """Ensures incompatible remote default shells are rejected during detection.

        Detection must fail fast because bzfs relies on POSIX shell quoting semantics.
        """
        for shell in ["/bin/csh", "/usr/local/bin/fish", "/opt/homebrew/bin/elvish", "/opt/homebrew/bin/nushell"]:
            job = self._setup_job()
            p = job.params
            p.src.ssh_host = "src.example.com"
            p.src.ssh_user_host = "src.example.com"
            p.dst.ssh_host = "dst.example.com"
            p.dst.ssh_user_host = "dst.example.com"
            p.available_programs["local"] = {"default_shell-/bin/sh": "", "sh": ""}

            def my_detect_avail_programs_remote(
                _job: bzfs.Job, _remote: Remote, _ssh_user_host: str, *, detected_shell: str = shell
            ) -> dict[str, str]:
                return {
                    f"default_shell-{detected_shell}": "",
                    "sh": "",
                    "zfs": "2.2.4",
                }

            with (
                patch.object(
                    bzfs_main.detect, "_detect_available_programs_remote", side_effect=my_detect_avail_programs_remote
                ),
                patch.object(bzfs_main.detect, "_detect_zpool_features", return_value={}),
                self.assertRaises(SystemExit, msg=shell) as cm,
            ):
                detect_available_programs(job)
            self.assertIn("Cowardly refusing to proceed", str(cm.exception))

    def test_detect_available_programs_allows_incompatible_local_default_shell(self) -> None:
        """Ensures local-mode endpoints do not fail on non-POSIX login shells.

        Assumes the local process and local-mode src/dst endpoints execute either direct argv or an explicit `sh -c`.
        Design rationale: the login shell is only relevant when traversing an SSH remote-shell boundary.
        """
        job = self._setup_job()
        p = job.params
        available_progs = {
            "default_shell-/usr/local/bin/fish": "",
            "sh": "",
            "zfs": "2.4.1",
        }
        p.available_programs["local"] = available_progs.copy()

        def my_detect_avail_programs_remote(_job: bzfs.Job, _remote: Remote, _ssh_user_host: str) -> dict[str, str]:
            return available_progs.copy()

        with (
            patch.object(bzfs_main.detect, "_detect_available_programs_remote", side_effect=my_detect_avail_programs_remote),
            patch.object(bzfs_main.detect, "_detect_zpool_features", return_value={}),
        ):
            detect_available_programs(job)

        self.assertEqual("/usr/local/bin/fish", p.available_programs["local"]["default_shell"])
        self.assertEqual("/usr/local/bin/fish", p.available_programs["src"]["default_shell"])
        self.assertEqual("/usr/local/bin/fish", p.available_programs["dst"]["default_shell"])


#############################################################################
class TestDetectAvailableProgramsRemote(AbstractTestCase):

    def _setup(self) -> tuple[bzfs.Job, Remote]:
        args = self.argparser_parse_args(["src", "dst"])
        p = self.make_params(args=args)
        job = bzfs.Job()
        job.params = p
        p.src = Remote("src", args, p)  # type: ignore[misc]  # cannot assign to final attribute
        return job, p.src

    def test_zfs_version_parsing_and_shell(self) -> None:
        job, remote = self._setup()
        p = job.params

        def run(*args: str, cmd: list[str] | None = None, **kw: str) -> str:
            command = cmd if cmd is not None else args[0]
            if "--version" in command:
                return "zfs-2.2.3\n"
            return "sh\n"

        with patch.object(bzfs.Job, "run_ssh_command", side_effect=run):
            avail = bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
            p.available_programs[remote.location] = avail
        self.assertEqual("2.2.3", p.available_programs[remote.location]["zfs"])
        self.assertIn("sh", p.available_programs[remote.location])
        self.assertIn(bzfs_main.detect.ZFS_VERSION_IS_AT_LEAST_2_2_0, p.available_programs[remote.location])

    def test_dummy_remote_returns_empty(self) -> None:
        """Confirms dummy remotes skip detection and return an empty map.

        Assumes dummy datasets are synthetic and must not trigger SSH/ZFS probing.
        Design rationale: avoid side effects and external dependencies when `root_dataset` is the dummy marker.
        """
        job, remote = self._setup()
        remote.root_dataset = bzfs_main.detect.DUMMY_DATASET
        with patch.object(bzfs.Job, "run_ssh_command") as mock_run:
            avail = bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
        self.assertEqual({}, avail)
        mock_run.assert_not_called()

    def test_shell_detection_called_process_error_falls_back(self) -> None:
        """Falls back to minimum assumptions when shell probing fails.

        Assumes `zfs --version` works, but running the shell probe raises `CalledProcessError`.
        Design rationale: warn and continue with a minimal `available_programs` set to avoid hard failures.
        """
        job, remote = self._setup()
        p = job.params

        def run(*_args: str, cmd: list[str] | None = None, **_kw: str) -> str:
            assert cmd is not None
            if "--version" in cmd:
                return "zfs-2.2.4\n"
            raise subprocess.CalledProcessError(returncode=1, cmd="sh", output="", stderr="fail")

        with patch.object(bzfs.Job, "run_ssh_command", side_effect=run):
            avail = bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
            p.available_programs[remote.location] = avail
        self.assertEqual("2.2.4", p.available_programs[remote.location]["zfs"])
        self.assertIn("sudo", p.available_programs[remote.location])
        self.assertNotIn("sh", p.available_programs[remote.location])
        log = cast(MagicMock, p.log)
        log.warning.assert_called_once()

    def test_zfs_version_parsing_space_variant(self) -> None:
        """Accept version strings like 'zfs 2.2.4' without a hyphen."""
        job, remote = self._setup()
        p = job.params

        def run(*args: str, cmd: list[str] | None = None, **kw: str) -> str:
            command = cmd if cmd is not None else args[0]
            if "--version" in command:
                return "zfs 2.2.4\n"
            return "sh\n"

        with patch.object(bzfs.Job, "run_ssh_command", side_effect=run):
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

        with patch.object(bzfs.Job, "run_ssh_command", side_effect=run), self.assertRaises(SystemExit) as cm:
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
                    with (
                        patch.object(bzfs.Job, "run_ssh_command", side_effect=run),
                        self.assertRaises(SystemExit) as cm,
                    ):
                        bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
                    self.assertIn("Unparsable zfs version string", str(cm.exception))
                else:
                    with patch.object(bzfs.Job, "run_ssh_command", side_effect=run):
                        avail = bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
                        p.available_programs[remote.location] = avail
                    self.assertEqual(expect_version, p.available_programs[remote.location]["zfs"])
                    self.assertIn("sh", p.available_programs[remote.location])
                    # 2.2.4 >= 2.2.0
                    self.assertIn(bzfs_main.detect.ZFS_VERSION_IS_AT_LEAST_2_2_0, p.available_programs[remote.location])

    def test_shell_program_disabled(self) -> None:
        job, remote = self._setup()
        p = job.params
        p.shell_program = bzfs_main.detect.DISABLE_PRG  # type: ignore[misc]  # cannot assign to final attribute
        with patch.object(bzfs.Job, "run_ssh_command", return_value="zfs-2.1.0\n"):
            avail = bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
            p.available_programs[remote.location] = avail
        self.assertNotIn("zpool", p.available_programs[remote.location])
        self.assertNotIn("sh", p.available_programs[remote.location])

    def test_errors_raise_die(self) -> None:
        job, remote = self._setup()
        with (
            patch.object(
                bzfs.Job,
                "run_ssh_command",
                side_effect=FileNotFoundError(),
            ),
            self.assertRaises(SystemExit),
        ):
            bzfs_main.detect._detect_available_programs_remote(job, remote, "host")

    def test_not_openzfs_handling(self) -> None:
        job, remote = self._setup()
        err = subprocess.CalledProcessError(
            returncode=1, cmd="zfs", output="", stderr="unrecognized command '--version'\nrun: zfs help"
        )
        with patch.object(bzfs.Job, "run_ssh_command", side_effect=err), self.assertRaises(SystemExit) as cm:
            bzfs_main.detect._detect_available_programs_remote(job, remote, "host")
        self.assertIn("Unsupported ZFS platform", str(cm.exception))
        self.assertIn("unrecognized command '--version'", str(cm.exception))

    def test_called_process_error_non_zfs(self) -> None:
        job, remote = self._setup()
        err = subprocess.CalledProcessError(returncode=1, cmd="zfs", output="bad", stderr="fail")
        with patch.object(bzfs.Job, "run_ssh_command", side_effect=err), self.assertRaises(SystemExit):
            bzfs_main.detect._detect_available_programs_remote(job, remote, "host")

    def test_zpool_features_command_failure_warns_and_falls_back(self) -> None:
        """Verify an ordinary zpool command failure retains the zfs fallback."""
        job, remote = self._setup()
        remote.pool = "tank"
        error = subprocess.CalledProcessError(
            returncode=1,
            cmd=["zpool", "get"],
            stderr="cannot open 'tank': permission denied",
        )

        with (
            patch.object(job, "run_ssh_command_with_retries", side_effect=error),
            patch.object(job, "try_ssh_command_with_retries", return_value="tank\n") as mock_try,
        ):
            features = bzfs_main.detect._detect_zpool_features(job, remote, {"zpool": ""})

        self.assertEqual({}, features)
        mock_try.assert_called_once()
        log = cast(MagicMock, job.params.log)
        log.warning.assert_called_once()
        self.assertIn("Failed to detect zpool features on", log.warning.call_args.args[1])

    def test_zpool_features_exhausted_ssh_transport_failure_falls_back(self) -> None:
        """Verify an exhausted SSH transport failure is swallowed after retries and uses the zfs fallback."""
        job, remote = self._setup()
        remote.pool = "tank"
        error = subprocess.CalledProcessError(
            returncode=255,
            cmd=["ssh", "host", "zpool", "get"],
            stderr="ssh: connect to host example.invalid port 22: Connection timed out",
        )

        with (
            patch.object(job, "run_ssh_command_with_retries", side_effect=error),
            patch.object(job, "try_ssh_command_with_retries", return_value="tank\n") as mock_try,
        ):
            features = bzfs_main.detect._detect_zpool_features(job, remote, {"zpool": ""})

        self.assertEqual({}, features)
        mock_try.assert_called_once()
        log = cast(MagicMock, job.params.log)
        log.warning.assert_called_once()
        self.assertIn("Failed to detect zpool features on", log.warning.call_args.args[1])

    def test_zpool_features_file_not_found_warns_and_falls_back(self) -> None:
        """Verify a missing `zpool` executable warns and uses the ZFS fallback.

        Feature discovery raises FileNotFoundError, but the independent `zfs list` probe must run once and detection must
        return normally. The caller's available-program map remains unchanged because executable discovery and pool probing
        have separate responsibilities.
        """
        job, remote = self._setup()
        p = job.params
        # Use a program name that is virtually guaranteed to be missing.
        p.zpool_program = "bzfs_zpool_nonexistent_cli"  # type: ignore[misc]  # cannot assign to final attribute
        remote.pool = "tank"
        remote.basis_root_dataset = "tank/ds"
        # Advertise zpool as "available" so detection attempts to run it.
        available_programs = {"zpool": ""}
        # Provide a real connection pool for local-mode ssh (ssh_user_host == "").
        p.connection_pools[remote.location] = ConnectionPools(
            remote=remote,
            capacities={SHARED: remote.max_concurrent_ssh_sessions_per_tcp_connection, DEDICATED: 1},
        )

        # Avoid touching the real zfs CLI in the fallback path.
        with patch.object(bzfs.Job, "try_ssh_command_with_retries", return_value="tank\n") as mock_try:
            features = bzfs_main.detect._detect_zpool_features(job, remote, available_programs)

        self.assertEqual({}, features)
        self.assertEqual({"zpool": ""}, available_programs)
        mock_try.assert_called_once()
        # The missing zpool binary should have triggered a warning.
        log = cast(MagicMock, p.log)
        log.warning.assert_called_once()
        warn_args, _ = log.warning.call_args
        self.assertEqual("%s", warn_args[0])
        self.assertIn("Failed to detect zpool features on", warn_args[1])

    def test_ssh_transport_error_during_zfs_version_dies(self) -> None:
        """Uses the real ssh CLI to trigger an ssh transport error during 'zfs --version' detection."""
        job, remote = self._setup()
        p = job.params
        # Mark ssh as available on the local host so Remote.is_ssh_available() passes.
        p.available_programs["local"] = {"ssh": ""}
        # Configure the remote to talk to a local port that is expected to refuse connections quickly.
        remote.ssh_user_host = "127.0.0.1"
        remote.ssh_host = "127.0.0.1"
        remote.ssh_port = 1  # type: ignore[misc]  # cannot assign to final attribute
        remote.reuse_ssh_connection = False  # avoid ssh master setup for faster failure
        remote.ssh_extra_opts = tuple(list(remote.ssh_extra_opts) + ["-oConnectTimeout=1"])
        # Provide a real connection pool so Job.run_ssh_command() can establish ssh connections.
        p.connection_pools[remote.location] = ConnectionPools(
            remote=remote,
            capacities={SHARED: remote.max_concurrent_ssh_sessions_per_tcp_connection, DEDICATED: 1},
        )

        with self.assertRaises(SystemExit) as cm, suppress_output():
            bzfs_main.detect._detect_available_programs_remote(job, remote, remote.ssh_user_host)

        msg = str(cm.exception)
        self.assertIn("ssh exit code 255:", msg)
        self.assertIn("ssh: ", msg)
