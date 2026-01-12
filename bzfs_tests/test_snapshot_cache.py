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
"""Snapshot cache correctness test suite for --cache-snapshots CLI option.

This module exercises the correctness invariants of bzfs snapshot caching across all cache families and interactions:

- Dataset-level caches "=": One per dataset and location (src/dst) that stores the ZFS snapshots_changed property.
  Used to cheaply decide whether "zfs list -t snapshot" (aka probing) is necessary. Writes must be monotonic and
  trusted only when mature.

- Replication-scoped caches "==": One per src dataset, destination user@host+dataset and snapshot-filter hash that
  records the last time a successful replication observed the src at a given snapshots_changed. Critical for cheap
  skip decisions during replicate_datasets(). Writes must be monotonic.

- Monitor caches "===": Per dataset, label and check-kind (Latest/Oldest). atime stores the creation time of the
  relevant min/max snapshot; mtime stores snapshots_changed. Used to detect stale snapshots efficiently. Writes must be
  monotonic and trusted only when mature.

- Per-label creation caches (under src dataset): Store latest snapshot creation time for each label used by
  --create-src-snapshots scheduling. Must not be clobbered by dataset-level updates.

Concurrency and safety: All cache writes go through set_last_modification_time_safe(), which takes an advisory flock
and honors if_more_recent=True to prevent regressions. The suite contains two-way and three-way interleavings to prove
monotonicity under concurrent runs. MATURITY_TIME_THRESHOLD_SECS gates trust in just-updated values to avoid
equal-second races; too-recent values force safe fallback probing.

Invalidation: Two flavors are tested. (1) Selective: when snapshots_changed is unavailable (0), only the dataset-level
"=" is zeroed while per-label creation times are preserved. (2) Dataset mismatch: find_datasets_to_snapshot() may
invalidate the entire dataset cache directory to (0,0) to force probing; subsequent runs repopulate caches using
monotonic writes.

See individual tests for precise guarantees, including: monotonicity (src "==", dst "=", monitor "==="), threshold
gating for monitor and replicate paths, invalidate-and-repopulate flows, and concurrency across monitor/snapshot/
replication phases. These tests are hermetic (no ZFS), deterministic, and use real file timestamps to validate the
observables that production code relies on.
"""

from __future__ import (
    annotations,
)
import errno
import logging
import os
import random
import string
import subprocess
import tempfile
import threading
import time
import unittest
from collections.abc import (
    Iterator,
)
from contextlib import (
    contextmanager,
)
from datetime import (
    datetime,
    timezone,
)
from typing import (
    Callable,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from bzfs_main import (
    snapshot_cache,
)
from bzfs_main.bzfs import (
    CRITICAL_STATUS,
    DatasetProperties,
    Job,
)
from bzfs_main.configuration import (
    AlertConfig,
    MonitorSnapshotAlert,
    Remote,
    SnapshotLabel,
)
from bzfs_main.snapshot_cache import (
    MATURITY_TIME_THRESHOLD_SECS,
    MONITOR_CACHE_FILE_PREFIX,
    REPLICATION_CACHE_FILE_PREFIX,
    SnapshotCache,
    set_last_modification_time,
    set_last_modification_time_safe,
)
from bzfs_main.util.retry import (
    Retry,
    RetryConfig,
    RetryPolicy,
)
from bzfs_main.util.utils import (
    sha256_85_urlsafe_base64,
    sha256_128_urlsafe_base64,
)
from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestSnapshotCache,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


# constants:
SRC_DATASET: str = "pool/src"
DST_DATASET: str = "pool/dst"


#############################################################################
class TestSnapshotCache(AbstractTestCase):

    @contextmanager
    def job_context(self, args: list[str]) -> Iterator[tuple[Job, str]]:
        """Context manager yielding (job, tmpdir) with patched homedir.

        Purpose: Provide a ready-to-use Job whose cache directory lives under
        a TemporaryDirectory and whose CLI args are parsed consistently via the
        project's parser. Assumptions: Most tests want default datasets
        [src_dataset, dst_dataset]. Design Rationale: Centralize the very common
        pattern of creating a temp dir, patching get_home_directory, building
        args, log_params and a Job.
        """
        with tempfile.TemporaryDirectory() as tmpdir, patch("bzfs_main.util.utils.get_home_directory", return_value=tmpdir):
            job = self.make_job_for_tmp(tmpdir, args)
            yield job, tmpdir

    def make_job_for_tmp(self, tmpdir: str, args: list[str]) -> Job:
        assert args
        ns = self.argparser_parse_args(args)
        log_params = MagicMock()
        log_params.last_modified_cache_dir = tmpdir
        job = Job()
        job.params = self.make_params(args=ns, log_params=log_params)
        return job

    def replication_cache_label(self, job: Job, dst_dataset: str) -> str:
        """Build the src-->dst replication-scoped cache label ("==").

        Purpose: Provide the canonical label used to key the "last replicated"
        cache entry. Assumptions: Label depends on dst user@host namespace,
        dst dataset and snapshot filter hash. Design Rationale: Prevent drift
        and code duplication across tests computing this label.
        """
        assert dst_dataset
        filter_key = tuple(tuple(f) for f in job.params.snapshot_filters)
        filter_hash_code: str = sha256_85_urlsafe_base64(str(filter_key))
        userhost_dir: str = sha256_85_urlsafe_base64(job.params.dst.cache_namespace())
        dst_dataset_hash: str = sha256_85_urlsafe_base64(dst_dataset)
        return os.path.join(REPLICATION_CACHE_FILE_PREFIX, userhost_dir, dst_dataset_hash, filter_hash_code)

    def monitor_cache_label(self, cfg_kind: str, label: SnapshotLabel, alerts: list[MonitorSnapshotAlert]) -> str:
        assert cfg_kind
        assert label is not None
        assert len(alerts) > 0
        alerts_hash_code = sha256_128_urlsafe_base64(str(tuple(alerts)))
        label_hash_code = sha256_128_urlsafe_base64(label.notimestamp_str())
        kind_hash_code = cfg_kind[0]
        return os.path.join(MONITOR_CACHE_FILE_PREFIX, kind_hash_code, label_hash_code, alerts_hash_code)

    def snapshot_cache_label(self, label: SnapshotLabel) -> str:
        return sha256_128_urlsafe_base64(label.notimestamp_str())

    def test_set_last_modification_time(self) -> None:
        for func in [set_last_modification_time, set_last_modification_time_old]:
            with self.subTest(func=func.__name__):
                with tempfile.TemporaryDirectory() as tmpdir:
                    file = os.path.join(tmpdir, "foo")
                    func(file, unixtime_in_secs=0)
                    self.assertEqual(0, round(os.stat(file).st_mtime))
                    func(file, unixtime_in_secs=0)
                    self.assertEqual(0, round(os.stat(file).st_mtime))
                    func(file, unixtime_in_secs=1000, if_more_recent=True)
                    self.assertEqual(1000, round(os.stat(file).st_mtime))
                    func(file, unixtime_in_secs=0, if_more_recent=True)
                    self.assertEqual(1000, round(os.stat(file).st_mtime))
                    set_last_modification_time_safe(file, unixtime_in_secs=1001, if_more_recent=True)
                    self.assertEqual(1001, round(os.stat(file).st_mtime))

    def test_initial_write_on_new_cache_file_sets_requested_time(self) -> None:
        """On a brand-new cache file, an initial write with if_more_recent=True must not be skipped even if the desired
        timestamp is in the (recent) past relative to current time.

        The file's mtime must equal the provided timestamp, not the implicit creation time ("now").
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            file = os.path.join(tmpdir, "f")
            expected = int(time.time()) - 5  # recent past
            set_last_modification_time_safe(file, unixtime_in_secs=expected, if_more_recent=True)
            self.assertEqual(expected, round(os.stat(file).st_mtime))

    def test_monotonic_guard_updates_creation_time_when_mtime_equal(self) -> None:
        """Ensures equal snapshots_changed still allow updating cached creation time.

        Scenario: A cache file initially encodes (creation=90, snapshots_changed=100). A new snapshot is discovered
        within the same snapshots_changed second with (creation=100, snapshots_changed=100). The guard must update
        atime from 90 to 100 instead of skipping, so per-label/monitor caches don't retain stale creation timestamps.
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "cachefile")
            # Simulate an existing cache file with older creation time but the same snapshots_changed value.
            with open(path, "wb") as _:
                pass
            os.utime(path, (90, 100))  # atime=90, mtime=100

            # Now write a newer creation time in the same snapshots_changed second.
            set_last_modification_time(path, unixtime_in_secs=(100, 100), if_more_recent=True)

            st = os.stat(path)
            self.assertEqual(100, round(st.st_mtime))  # snapshots_changed anchor unchanged
            self.assertEqual(100, round(st.st_atime))  # creation time must be updated

    def test_set_last_modification_time_with_file_not_found_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file = os.path.join(tmpdir, "foo")
            with patch("os.utime", side_effect=FileNotFoundError):
                with self.assertRaises(FileNotFoundError):
                    set_last_modification_time(file + "nonexisting", unixtime_in_secs=1001, if_more_recent=False)
                file = os.path.join(file, "x", "nonexisting2")
                set_last_modification_time_safe(file, unixtime_in_secs=1001, if_more_recent=False)

    def test_open_existing_file_after_create_race(self) -> None:
        """Covers the race where the file appears between open() attempts.

        First os.open() raises FileNotFoundError; the exclusive create raises FileExistsError; finally we open the now-
        existing file and write timestamps.
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "racefile")

            # Keep original open for creating the file inside the mocked function
            orig_open = os.open

            call_state = {"i": 0}

            def fake_open(p: str, flags: int, mode: int | None = None) -> int:
                self.assertEqual(path, p)
                i = call_state["i"]
                call_state["i"] = i + 1
                if i == 0:
                    # Initial open without O_CREAT fails with FileNotFoundError
                    raise FileNotFoundError
                if i == 1:
                    # Simulate losing the O_CREAT|O_EXCL race: ensure file exists, then raise FileExistsError
                    fd_tmp = orig_open(path, os.O_WRONLY | os.O_CREAT | os.O_CLOEXEC, 0o600)
                    os.close(fd_tmp)
                    raise FileExistsError
                # Third call: open existing file normally
                return orig_open(path, flags, mode) if mode is not None else orig_open(path, flags)

            with patch("os.open", side_effect=fake_open):
                # Use if_more_recent=False to avoid early-return monotonic guard
                set_last_modification_time(path, unixtime_in_secs=1_234_567_890, if_more_recent=False)

            st = os.stat(path)
            self.assertEqual(1_234_567_890, round(st.st_mtime))
            # Ensure our fake_open was exercised exactly three times
            self.assertEqual(3, call_state["i"])

    def test_permission_error_when_cache_owned_by_different_uid(self) -> None:
        """Covers the ownership check raising PermissionError when st_uid != geteuid()."""

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "permfile")
            # Create the file so the first open() succeeds without O_CREAT
            with open(path, "wb") as _:
                pass

            # Return a minimal object with st_uid != os.geteuid(); st_mtime unused due to early raise
            class FakeStat:
                st_uid = (os.geteuid() + 1) % 65536
                st_mtime = 0

            with patch("os.fstat", return_value=FakeStat()):
                with self.assertRaises(PermissionError) as cm:
                    set_last_modification_time(path, unixtime_in_secs=1_000_000_000, if_more_recent=False)

            err = cm.exception
            self.assertEqual(errno.EPERM, err.errno)
            self.assertIn(repr(path), str(err))

    @patch("bzfs_main.snapshot_cache.itr_ssh_cmd_parallel")
    def test_zfs_get_snapshots_changed_parsing(self, mock_itr_parallel: MagicMock) -> None:
        mock_remote = MagicMock(spec=Remote)  # spec helps catch calls to non-existent attrs
        with self.job_context([SRC_DATASET, DST_DATASET]) as (job, _tmpdir):

            mock_itr_parallel.return_value = [  # normal input
                [
                    "12345\tdataset/valid1",
                    "789\tdataset/valid2",
                ]
            ]
        results = job.cache.zfs_get_snapshots_changed(mock_remote, ["d1", "d2", "d3", "d4"])
        self.assertDictEqual({"dataset/valid1": 12345, "dataset/valid2": 789}, results)

        # Simulate output from a failing 'zfs list' command captured on its stdout.
        # This could be partial output, or error messages if zfs wrote them to stdout.
        mock_itr_parallel.return_value = [
            [
                "12345\tdataset/valid1",
                "ERROR: zfs command failed for dataset/invalid2",  # Line without tab, from stdout
                "789\tdataset/valid2",
            ]
        ]
        results = job.cache.zfs_get_snapshots_changed(mock_remote, ["d1", "d2", "d3", "d4"])
        self.assertDictEqual({"dataset/valid1": 12345}, results)

        mock_itr_parallel.return_value = [
            [
                "12345\tdataset/valid1",
                "123\t",  # empty dataset
                "789\tdataset/valid2",
            ]
        ]
        results = job.cache.zfs_get_snapshots_changed(mock_remote, ["d1", "d2", "d3", "d4"])
        self.assertDictEqual({"dataset/valid1": 12345}, results)

        mock_itr_parallel.return_value = [
            [
                "12345\tdataset/valid1",
                "\tfoo",  # missing timestamp
                "789\tdataset/valid2",
            ]
        ]
        results = job.cache.zfs_get_snapshots_changed(mock_remote, ["d1", "d2", "d3", "d4"])
        self.assertDictEqual({"dataset/valid1": 12345, "foo": 0, "dataset/valid2": 789}, results)

    def test_monitor_initial_write_persists_creation_and_changed(self) -> None:
        """Monitor path must persist (creation, snapshots_changed) on first write to a new file.

        Validates that the monotonic guard does not suppress initial writes when desired times are <= the file creation time.
        """

        with self.job_context(
            [
                "--monitor-snapshots",
                str({"org": {"onsite": {"daily": {"latest": {"warning": "1 seconds", "critical": "2 seconds"}}}}}),
                "--monitor-snapshots-dont-warn",
                "--monitor-snapshots-dont-crit",
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            job.params.create_src_snapshots_config.current_datetime = datetime(2024, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
            job.params.src.root_dataset = SRC_DATASET
            creation = 1_700_000_000
            changed = int(time.time()) - 5

            alerts = job.params.monitor_snapshots_config.alerts
            self.assertGreaterEqual(len(alerts), 1)
            alert = alerts[0]
            cfg = alert.latest
            assert cfg is not None
            cache_label = self.monitor_cache_label(cfg.kind, alert.label, alerts)
            cache_file = SnapshotCache(job).last_modified_cache_file(job.params.src, SRC_DATASET, cache_label)

            def fake_handle_minmax_snapshots(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                fn_latest(0, creation, SRC_DATASET, "s")
                return []

            job.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=changed)
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
            ):
                job.monitor_snapshots(job.params.src, [SRC_DATASET])

            at, mt = SnapshotCache(job).get_snapshots_changed2(cache_file)
            self.assertEqual(creation, at)
            self.assertEqual(changed, mt)

    def test_monitor_snapshots_exits_with_worst_severity(self) -> None:
        """Monitor checks all datasets and exits with the worst severity.

        Tie-break: At equal severity, "Latest" takes precedence over "Oldest".
        """
        plan = {
            "org": {
                "tgt": {
                    "secondly": {
                        "latest": {"warning": "1 seconds", "critical": "100 seconds"},
                        "oldest": {"warning": "1 seconds", "critical": "100 seconds"},
                    }
                }
            }
        }
        with self.job_context(["--monitor-snapshots", str(plan), SRC_DATASET, DST_DATASET]) as (job, _tmpdir):
            now_secs = 1_000
            job.params.create_src_snapshots_config.current_datetime = datetime.fromtimestamp(now_secs, tz=timezone.utc)
            job.params.src.root_dataset = SRC_DATASET

            ds_warn = f"{SRC_DATASET}/a"
            ds_crit = f"{SRC_DATASET}/b"

            def fake_handle_minmax_snapshots(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                fn_latest(0, now_secs - 10, ds_warn, "s")  # warning only
                fn_latest(0, now_secs - 200, ds_crit, "s")  # critical
                assert fn_oldest is not None
                fn_oldest(0, now_secs - 200, ds_crit, "s")  # critical (tie-break with latest)
                return []

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=False),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
            ):
                exit_code, exit_kind, exit_msg = job.monitor_snapshots(job.params.src, [ds_warn, ds_crit])
            self.assertEqual(-CRITICAL_STATUS, exit_code)
            self.assertIn("Latest", exit_kind)
            self.assertIn("Latest snapshot", exit_msg)
            self.assertIn(ds_crit, exit_msg)
            self.assertNotIn("Oldest snapshot", exit_msg)

    def test_last_replicated_cache_must_be_monotonic(self) -> None:
        """Purpose: Prove the src-->dst "last replicated" cache (the src-side "==" marker keyed by user/host+dst+filters)
        never regresses even when an older job finishes after a newer job. This cache is a correctness accelerator for
        replicate_datasets() to cheap-skip work; a regression risks false "up-to-date" decisions.

        Assumptions: Timestamps are integer seconds, multiple bzfs runs may overlap, and the only reliable concurrency
        guarantee is monotonic writes guarded by if_more_recent=True with advisory flock.

        Constraints: No ZFS available; we simulate zfs_get_snapshots_changed orderings and rely on real filesystem
        atime/mtime semantics. We avoid network and external processes; the test must be fully deterministic.

        Verification: We execute replicate_datasets() twice against the same cache directory. The first run writes a
        newer src snapshots_changed and, after replication, records the replication-scoped marker. The second run
        pretends to be older (smaller snapshots_changed) and attempts to write after the first. We assert the cache file
        remains at the newer timestamp. If the implementation failed, a later older run could overwrite a newer marker,
        causing subsequent invocations to misclassify datasets as already replicated and silently skip required work.

        Design Rationale: This approach exercises the real cache update path (including locking and if_more_recent
        logic) without needing ZFS. We control the two post-replication property refreshes via a side-effect function to
        emulate realistic sequencing, while maintaining strict determinism. Using actual file timestamps provides a
        faithful signal of what downstream code will observe."""

        with self.job_context([SRC_DATASET, DST_DATASET]) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET
            job.params.dst.root_dataset = DST_DATASET
            job.dst_dataset_exists[DST_DATASET] = True  # avoid skip in scheduler

            # Compute the replication-scoped cache file path used by replicate_datasets()
            cache_label = self.replication_cache_label(job, DST_DATASET)
            last_repl_file = SnapshotCache(job).last_modified_cache_file(job.params.src, SRC_DATASET, cache_label)

            # Patch caching detection and network interactions to keep it unit-level
            call_state = {"i": 0}

            def fake_zfs_get_snapshots_changed(_remote: Remote, _datasets: list[str]) -> dict[str, int]:
                i = call_state["i"]
                call_state["i"] = i + 1
                # Calls: 0=find_stale (maybe empty) -> return {}
                #        1=refresh after replicate -> return first mapping
                #        2=find_stale again -> {}
                #        3=refresh after replicate -> second mapping
                if i in (0, 2):
                    return {}
                return {DST_DATASET: 2_000_000_100} if i == 1 else {DST_DATASET: 1_900_000_100}

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", return_value=True),
                patch.object(SnapshotCache, "zfs_get_snapshots_changed", side_effect=fake_zfs_get_snapshots_changed),
            ):

                # First replication with newer snapshots_changed
                job.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=2_000_000_000)
                job.replicate_datasets([SRC_DATASET], task_description="t", max_workers=1)

                # Second replication with older snapshots_changed (simulates a slower, older run finishing last)
                job.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=1_900_000_000)
                job.replicate_datasets([SRC_DATASET], task_description="t", max_workers=1)

            # Assert: cache must remain at the newer timestamp (monotonic)
            self.assertEqual(2_000_000_000, SnapshotCache(job).get_snapshots_changed(last_repl_file))

    def test_dst_dataset_cache_must_be_monotonic(self) -> None:
        """Purpose: Ensure the destination dataset-level cache file (dst "=") never regresses when concurrent or
        overlapping runs write out of order. This cache participates in cheap "already up-to-date [cached]" decisions
        and must always reflect the latest trustworthy snapshots_changed.

        Assumptions: Cache timestamps are integer seconds, file writes are protected by a monotonic guard
        (if_more_recent=True) and a per-file lock, and the decision logic requires equality between live properties and
        these cached values to skip correctly.

        Constraints: No access to ZFS; we model the two phases of replicate_datasets() (initial stale detection and
        post-replication refresh) and write real files to validate persistent state. Test must remain deterministic.

        Verification: We run replicate_datasets() twice while controlling the side effects of zfs_get_snapshots_changed:
        the first run writes a newer dst "=" time; the second emulates an older finishing later. We assert that the dst
        "=" cache retains the newer timestamp. If this property failed, a later older run could roll the dst cache back
        and falsely satisfy equality checks, causing incorrect cheap-skips or wasted work.

        Design Rationale: Using the real code path and filesystem timestamps yields a high-fidelity check of the
        monotonicity contract without introducing external dependencies or flakiness."""

        with self.job_context([SRC_DATASET, DST_DATASET]) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET
            job.params.dst.root_dataset = DST_DATASET
            job.dst_dataset_exists[DST_DATASET] = True
            dst_cache_file = SnapshotCache(job).last_modified_cache_file(job.params.dst, DST_DATASET)

            call_state = {"i": 0}

            def fake_zfs_get_snapshots_changed(_remote: Remote, _datasets: list[str]) -> dict[str, int]:
                i = call_state["i"]
                call_state["i"] = i + 1
                if i in (0, 2):
                    return {}
                return {DST_DATASET: 2_000_000_050} if i == 1 else {DST_DATASET: 1_900_000_050}

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", return_value=True),
                patch.object(SnapshotCache, "zfs_get_snapshots_changed", side_effect=fake_zfs_get_snapshots_changed),
            ):

                job.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=2_000_000_000)
                job.replicate_datasets([SRC_DATASET], task_description="t", max_workers=1)

                job.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=1_900_000_000)
                job.replicate_datasets([SRC_DATASET], task_description="t", max_workers=1)

            self.assertEqual(2_000_000_050, SnapshotCache(job).get_snapshots_changed(dst_cache_file))

    def test_update_last_modified_cache_does_not_clobber_label_creation_times(self) -> None:
        """Purpose: Ensure update_last_modified_cache() preserves per-label creation times (which drive snapshot
        scheduling) and only updates the dataset-level "=" file. Clobbering label files with snapshots_changed would
        mask overdue snapshots and violate recovery point objectives.

        Assumptions: Label caches store the creation time of the latest snapshot for a naming policy; dataset-level
        caches store snapshots_changed. A single dataset may have multiple labels; per-label creation times are
        independent of the dataset-level modification time.

        Constraints: No ZFS; construct realistic cache files under a temp directory and simulate property retrieval.
        Timestamps are integer seconds; file writes use the same helper as production code.

        Verification: Precreate a label file with an older creation timestamp and a dataset "=" file with a newer
        snapshots_changed. After calling update_last_modified_cache(), the label file must still equal the original
        creation time, and only the dataset "=" file may be updated. If the implementation instead rewrote the label
        file, later runs could incorrectly infer the most recent label snapshot is newer than it really is, causing due
        snapshots to be skipped.

        Design Rationale: Focused unit-level setup isolates the cache responsibility from discovery logic while testing
        the exact write behavior of update_last_modified_cache()."""

        with self.job_context([SRC_DATASET, DST_DATASET]) as (job, _tmpdir):
            snapshots_changed = 2_000_000_000  # arbitrary recent unix time
            job.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=snapshots_changed)

            # Prepare a label representing, e.g., a daily snapshot naming scheme
            label = SnapshotLabel(prefix="org_", infix="target_", timestamp="", suffix="_daily")
            cache = SnapshotCache(job)

            # Create a per-label cache file that records the creation time of the latest label snapshot
            label_cache_file = cache.last_modified_cache_file(job.params.src, SRC_DATASET, self.snapshot_cache_label(label))
            creation_time = 1_900_000_000  # older than snapshots_changed, simulating last daily snapshot time
            set_last_modification_time_safe(label_cache_file, unixtime_in_secs=creation_time, if_more_recent=True)

            # Also create the dataset-level cache file ("=") with the dataset's snapshots_changed value
            dataset_cache_file = cache.last_modified_cache_file(job.params.src, SRC_DATASET)
            set_last_modification_time_safe(dataset_cache_file, unixtime_in_secs=snapshots_changed, if_more_recent=True)

            # Stub zfs_get_snapshots_changed to avoid calling ZFS; return snapshots_changed for our dataset
            with patch.object(SnapshotCache, "zfs_get_snapshots_changed", return_value={SRC_DATASET: snapshots_changed}):
                # Act: Update cache after snapshot creation scheduling
                cache.update_last_modified_cache({label: [SRC_DATASET]})

            # Assert: The per-label cache must still reflect the creation time, not snapshots_changed
            # i.e. it must NOT be clobbered by the dataset-level snapshots_changed value.
            self.assertEqual(creation_time, cache.get_snapshots_changed(label_cache_file))

    def test_update_last_modified_cache_keeps_label_times_when_prop_unavailable(self) -> None:
        """Purpose: Confirm that when the ZFS snapshots_changed property is unavailable (0), only the dataset-level
        "=" cache is invalidated to zero while per-label creation times remain intact. This ensures that temporary
        unavailability of the property does not erase valuable scheduling information.

        Assumptions: Property unavailability can occur transiently; the cache should fail safe by forcing probing for
        dataset modification while retaining per-label creation times for later use.

        Constraints: No ZFS; simulate a 0 property and pre-existing label and dataset caches. Writes use the same helper
        as production code.

        Verification: Precreate a label cache file with a known creation time and a dataset "=" file with a prior
        value. After update_last_modified_cache() with mocked property=0, assert the dataset cache equals 0 while the
        label cache preserved its creation time. If the code cleared label caches too, future runs would lack the data
        needed to decide due snapshots without extra probing; if it left the dataset cache non-zero, we might make stale
        skip decisions.

        Design Rationale: Targeted assertions isolate the selective invalidation semantics, a critical safety behavior
        under degraded conditions.

        When snapshots_changed is unavailable (0), per-label cache files must not be clobbered.
        Purpose: guard against a design bug where update_last_modified_cache() resets all cache entries
        for a dataset (including per-label creation time entries) when the ZFS 'snapshots_changed' property
        is unavailable or returns 0. Only the dataset-level cache ("=") should be invalidated.
        """

        with self.job_context([SRC_DATASET, DST_DATASET]) as (job, _tmpdir):
            snapshots_changed = 0  # Simulate property unavailable
            job.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=snapshots_changed)

            # Prepare a label whose cache file encodes latest snapshot creation time
            label = SnapshotLabel(prefix="org_", infix="target_", timestamp="", suffix="_daily")
            cache = SnapshotCache(job)

            # Create a per-label cache file with a known creation time
            label_cache_file = cache.last_modified_cache_file(job.params.src, SRC_DATASET, self.snapshot_cache_label(label))
            creation_time = 1_900_000_000
            set_last_modification_time_safe(label_cache_file, unixtime_in_secs=creation_time, if_more_recent=True)

            # Also create the dataset-level cache file ("=") with a non-zero prior value to observe invalidation
            dataset_cache_file = cache.last_modified_cache_file(job.params.src, SRC_DATASET)
            set_last_modification_time_safe(dataset_cache_file, unixtime_in_secs=1_800_000_000, if_more_recent=True)

            # Stub zfs_get_snapshots_changed to return 0 for our dataset
            with patch.object(SnapshotCache, "zfs_get_snapshots_changed", return_value={SRC_DATASET: 0}):
                cache.update_last_modified_cache({label: [SRC_DATASET]})

            # Dataset-level cache must be invalidated to 0
            self.assertEqual(0, cache.get_snapshots_changed(dataset_cache_file))
            # Per-label cache must remain intact (not clobbered by invalidation)
            self.assertEqual(creation_time, cache.get_snapshots_changed(label_cache_file))

    def test_monitor_cache_must_be_monotonic(self) -> None:
        """Purpose: Ensure the monitor cache files ("===") that encode (creation_time, snapshots_changed) for a label are
        monotonic across runs. Monitor correctness depends on these times to detect stale/latest/oldest snapshots; a
        regression could suppress warranted warnings/criticals or report false OKs.

        Assumptions: Creation and snapshots_changed are integer seconds; writes use monotonic guards; multiple runs may
        overlap. We can drive the latest/oldest scan via handle_minmax_snapshots.

        Constraints: No ZFS available; we pass controlled creation times and use real cache files to validate state.

        Verification: First, run monitor_snapshots with a newer (creation, snapshots_changed) pair and persist it. Then
        run again with an older pair attempting to regress the cached tuple. We assert that atime (creation) and mtime
        (snapshots_changed) remain the newer values. If this failed, monitoring alerts could be muted or delayed, hiding
        missed schedules and compromising recovery objectives.

        Design Rationale: Exercising monitor's real write path with deterministic inputs tests the behavior that matters
        for operations, without requiring a live pool. This keeps the test fast, hermetic, and high-fidelity."""

        plan = {"org": {"onsite": {"daily": {"latest": {"warning": "1 seconds", "critical": "2 seconds"}}}}}
        with self.job_context(
            [
                "--monitor-snapshots",
                str(plan),
                "--monitor-snapshots-dont-warn",
                "--monitor-snapshots-dont-crit",
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            # Fix current time and dataset
            job.params.create_src_snapshots_config.current_datetime = datetime(2024, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
            job.params.src.root_dataset = SRC_DATASET
            job.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=2_000_000_050)

            # Compute identical cache path as monitor_snapshots()
            alerts = job.params.monitor_snapshots_config.alerts
            self.assertTrue(len(alerts) >= 1)
            alert = alerts[0]
            label = alert.label
            cfg = alert.latest
            assert cfg is not None
            cache_label = self.monitor_cache_label(cfg.kind, label, alerts)
            cache_file = SnapshotCache(job).last_modified_cache_file(job.params.src, SRC_DATASET, cache_label)

            # Stub: force fallback and inject creation times via handle_minmax_snapshots
            latest_times = [2_000_000_000, 1_900_000_000]  # decreasing creation times

            def fake_handle_minmax_snapshots(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                fn_latest(0, latest_times.pop(0), SRC_DATASET, "")
                return []

            # First run (newer)
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
            ):
                job.monitor_snapshots(job.params.src, [SRC_DATASET])

            # Second run (older)
            job.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=1_900_000_050)
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
            ):
                job.monitor_snapshots(job.params.src, [SRC_DATASET])

            atime, mtime = SnapshotCache(job).get_snapshots_changed2(cache_file)
            self.assertEqual(2_000_000_000, atime)
            self.assertEqual(2_000_000_050, mtime)

    def test_monitor_threshold_controls_cache_trust(self) -> None:
        """Purpose: Lock in the intended MATURITY_TIME_THRESHOLD_SECS semantics for monitor: the cache is trusted only
        if the live snapshots_changed is strictly older than the threshold and equals the cached value. Otherwise we
        must fall back to probing. This prevents equal-second races from producing false cache hits.

        Assumptions: OpenZFS timestamps are integer seconds; system clocks may skew slightly; trusting a just-updated
        value is unsafe; fallback is safe but potentially slower.

        Constraints: No ZFS; emulate both sides using real cache files and patch time.time to straddle the threshold.

        Verification: Case 1 (too recent): now < snapshots_changed + threshold ⇒ cache not trusted, fallback handler is
        invoked, and stats record a miss. Case 2 (mature + equal): now > snapshots_changed + threshold and cached equals
        live ⇒ cache trusted, no fallback invocation, and stats record a hit. If this behavior regressed, monitor could
        erroneously skip needed probing and mask stale snapshots, or probe unnecessarily when cache is safe to trust.

        Design Rationale: Using patched time ensures precise boundary testing, while relying on the actual write/read
        helpers validates end-to-end semantics without external dependencies.

        Monitor trusts cache only if snapshots_changed is mature; too-recent values trigger fallback.
        We assert stale_datasets length (misses) and cache hits reflect threshold gating.
        """

        # Configure monitor job for one dataset/label
        plan = {"org": {"onsite": {"daily": {"latest": {"warning": "1 seconds", "critical": "2 seconds"}}}}}
        with self.job_context(
            [
                "--monitor-snapshots",
                str(plan),
                "--monitor-snapshots-dont-warn",
                "--monitor-snapshots-dont-crit",
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET
            dataset = SRC_DATASET
            alerts = job.params.monitor_snapshots_config.alerts
            alert = alerts[0]
            label = alert.label
            cfg = alert.latest
            assert cfg is not None

            # Prepare cached tuple (creation, snapshots_changed)
            creation = 2_000_000_000
            snapshots_changed = 2_000_000_050
            job.src_properties[dataset] = DatasetProperties(recordsize=0, snapshots_changed=snapshots_changed)

            cache_label = self.monitor_cache_label(cfg.kind, label, alerts)
            cache_file = SnapshotCache(job).last_modified_cache_file(job.params.src, dataset, cache_label)
            set_last_modification_time_safe(cache_file, unixtime_in_secs=(creation, snapshots_changed), if_more_recent=True)

            # Helper to drive monitor and capture stale datasets
            received: list[str] = []

            def fake_handle_minmax_snapshots(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                received.extend(datasets)
                return []

            # Case 1: too recent (no trust)
            job.num_cache_hits = job.num_cache_misses = 0
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
                patch("time.time", return_value=snapshots_changed + (MATURITY_TIME_THRESHOLD_SECS / 2)),
            ):
                job.monitor_snapshots(job.params.src, [dataset])
            self.assertListEqual([dataset], received)
            self.assertEqual(0, job.num_cache_hits)
            self.assertEqual(1, job.num_cache_misses)

            # Case 2: mature (trust cache)
            received.clear()
            job.num_cache_hits = job.num_cache_misses = 0
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
                patch("time.time", return_value=snapshots_changed + MATURITY_TIME_THRESHOLD_SECS + 0.2),
            ):
                # keep current_datetime == creation to avoid alerts
                job.params.create_src_snapshots_config.current_datetime = datetime.fromtimestamp(creation, tz=timezone.utc)
                job.monitor_snapshots(job.params.src, [dataset])
            self.assertListEqual([], received)
            self.assertEqual(1, job.num_cache_hits)
            self.assertEqual(0, job.num_cache_misses)

    def test_replicate_threshold_controls_skip_vs_process(self) -> None:
        """Purpose: Validate that replicate_datasets() only performs a cheap skip when both conditions hold: (1) the src
        "==" cache equals the live src snapshots_changed and is older than the threshold; and (2) the dst "=" cache equals
        the live dst snapshots_changed and is older than the threshold. Otherwise, the dataset must be processed.

        Assumptions: Integer-second granularity can yield equal-second races; threshold gating plus equality on both
        sides prevents incorrect cheap-skips.

        Constraints: No ZFS; we count replicate_dataset invocations; we patch time.time; we author cache files directly.

        Verification: Case 1 (too recent): even if equality holds on src, if it is not yet "mature" we must not skip,
        and replicate_dataset is invoked. Case 2 (mature + equal across src and dst): we skip cheaply and do not invoke
        replicate_dataset. If this property failed, replication could be skipped incorrectly (data divergence) or invoked
        unnecessarily (wasteful operations).

        Design Rationale: This test isolates the time-threshold correctness contract using actual cache files and the
        real coordination logic, ensuring we defend against time-of-check hazards.

        Replicate trusts cache only if matured both on src '==' and dst '='; otherwise processes dataset.
        We assert replicate_dataset invocation count and cache hit/miss counters.
        """

        with self.job_context([SRC_DATASET, DST_DATASET]) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET
            job.params.dst.root_dataset = DST_DATASET
            job.dst_dataset_exists[DST_DATASET] = True

            # Build replication-scoped cache file path for src '==' and dst '=' path
            repl_cache_label = self.replication_cache_label(job, DST_DATASET)
            src_repl_file = job.cache.last_modified_cache_file(job.params.src, SRC_DATASET, repl_cache_label)
            dst_eq_file = SnapshotCache(job).last_modified_cache_file(job.params.dst, DST_DATASET)

            # Helper to run replicate and count invocations
            called: list[str] = []

            def fake_replicate_dataset(job: Job, src_dataset: str, tid: str, retry: RetryPolicy) -> bool:
                called.append(src_dataset)
                return True

            # Case 1: too recent on src -> no skip
            t0 = 2_000_000_000
            job.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=t0)
            set_last_modification_time_safe(src_repl_file, unixtime_in_secs=t0, if_more_recent=True)
            job.num_cache_hits = job.num_cache_misses = 0
            called.clear()
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", new=fake_replicate_dataset),
                patch.object(job.cache, "zfs_get_snapshots_changed", return_value={}),
                patch("time.time", return_value=t0 + (MATURITY_TIME_THRESHOLD_SECS / 2)),
            ):
                job.replicate_datasets([SRC_DATASET], "t", 1)
            self.assertListEqual([SRC_DATASET], called)
            self.assertEqual(0, job.num_cache_hits)
            self.assertEqual(1, job.num_cache_misses)

            # Case 2: matured and equal on src and dst -> skip cheaply
            t_dst = t0  # same equality
            set_last_modification_time_safe(dst_eq_file, unixtime_in_secs=t_dst, if_more_recent=True)
            job.num_cache_hits = job.num_cache_misses = 0
            called.clear()
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", new=fake_replicate_dataset),
                patch.object(job.cache, "zfs_get_snapshots_changed", return_value={DST_DATASET: t_dst}),
                patch("time.time", return_value=t0 + MATURITY_TIME_THRESHOLD_SECS + 0.2),
            ):
                job.replicate_datasets([SRC_DATASET], "t", 1)
            self.assertListEqual([], called)
            self.assertEqual(1, job.num_cache_hits)
            self.assertEqual(0, job.num_cache_misses)

    def test_concurrent_jobs_monotonic_last_replicated_and_dst_cache(self) -> None:
        """Purpose: Validate that when two Jobs operate concurrently on the same cache directory, neither the src "=="
        (last replicated) nor the dst "=" file ever regresses, even if the older job finishes last. These files are used
        for cheap skip decisions; regressions can lead to silent data divergence or oscillating work.

        Assumptions: Concurrency is common in daemonized or orchestrated environments; monotonic guards must provide the
        only ordering guarantee required for correctness.

        Constraints: No ZFS; real file IO with advisory locks; deterministic interleaving via threading.Event. Network
        and process execution are stubbed out to keep the test hermetic and fast.

        Verification: Job A (newer) writes first; Job B (older) writes after. We assert the cache retains A's newer
        timestamps for both files. If this failed, cached equality checks might be satisfied by stale values leading to
        incorrect cheap-skips; or the opposite, forcing needless work.

        Design Rationale: Hooking the first write with a wrapper to signal the event provides a simple, deterministic
        interleaving that still tests the real synchronization and monotonic write behavior.

        Integration-style: two concurrent Jobs write shared cache; older must not clobber newer.

        Simulates two bzfs runs racing to update the same cache files. Job A has newer timestamps and writes first,
        Job B has older timestamps and writes later. With monotonic guards, final cache values must remain those of A.
        """

        with self.job_context([SRC_DATASET, DST_DATASET]) as (job_a, tmpdir):
            # Second job shares the same cache directory
            job_b = self.make_job_for_tmp(tmpdir, [SRC_DATASET, DST_DATASET])
            job_a.params.src.root_dataset = SRC_DATASET
            job_a.params.dst.root_dataset = DST_DATASET
            job_b.params.src.root_dataset = SRC_DATASET
            job_b.params.dst.root_dataset = DST_DATASET
            job_a.dst_dataset_exists[DST_DATASET] = True
            job_b.dst_dataset_exists[DST_DATASET] = True

            # Compute file paths to assert at end (same for both jobs)
            cache_label = self.replication_cache_label(job_a, DST_DATASET)
            cache_label = self.replication_cache_label(job_a, DST_DATASET)
            last_repl_file = SnapshotCache(job_a).last_modified_cache_file(job_a.params.src, SRC_DATASET, cache_label)
            dst_cache_file = SnapshotCache(job_a).last_modified_cache_file(job_a.params.dst, DST_DATASET)

            # Configure different timestamps for A (newer) and B (older)
            a_src_changed = 2_000_000_000
            a_dst_changed = 2_000_000_100
            b_src_changed = 1_900_000_000
            b_dst_changed = 1_900_000_100
            job_a.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=a_src_changed)
            job_b.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=b_src_changed)

            # Prepopulate to deterministically exercise find-stale (src '==' equality, matured) and force refresh
            # (dst '=' mismatch):
            set_last_modification_time_safe(last_repl_file, unixtime_in_secs=a_src_changed, if_more_recent=True)
            set_last_modification_time_safe(dst_cache_file, unixtime_in_secs=1_234_567_890, if_more_recent=True)

            # Instrumentation for A's phases and counts
            a_findstale_seen = threading.Event()
            a_repl_started = threading.Event()
            a_repl_done = threading.Event()
            a_dst_written = threading.Event()
            a_counts = {"find": 0, "refresh": 0}

            # Phase-aware zfs_get_snapshots_changed for A/B (deterministic payloads)
            def fake_zfs_get_snapshots_changed_a(_remote: Remote, _datasets: list[str]) -> dict[str, int]:
                if not a_repl_done.is_set():
                    a_counts["find"] += 1
                    a_findstale_seen.set()
                else:
                    a_counts["refresh"] += 1
                return {DST_DATASET: a_dst_changed}

            def fake_zfs_get_snapshots_changed_b(_remote: Remote, _datasets: list[str]) -> dict[str, int]:
                return {DST_DATASET: b_dst_changed}

            orig_set_last_modification_time_safe = snapshot_cache.set_last_modification_time_safe

            def wrapped_set_last_modification_time_safe(
                path: str, unixtime_in_secs: int, if_more_recent: bool = False
            ) -> None:
                # Call through to real function
                orig_set_last_modification_time_safe(path, unixtime_in_secs=unixtime_in_secs, if_more_recent=if_more_recent)
                # Signal only after A has written the destination dataset cache, ensuring A's value lands first.
                if path == dst_cache_file:
                    a_dst_written.set()

            def run_job_a() -> None:
                with patch.object(job_a.cache, "zfs_get_snapshots_changed", side_effect=fake_zfs_get_snapshots_changed_a):
                    job_a.replicate_datasets([SRC_DATASET], task_description="A", max_workers=1)

            def run_job_b() -> None:
                # Wait until A has written the dst '=' cache, then run B so it attempts to write older
                a_dst_written.wait(timeout=5)
                with patch.object(job_b.cache, "zfs_get_snapshots_changed", side_effect=fake_zfs_get_snapshots_changed_b):
                    job_b.replicate_datasets([SRC_DATASET], task_description="B", max_workers=1)

            # Use time patch to ensure maturity for equality/skip decisions
            mature_now = max(a_src_changed, a_dst_changed) + MATURITY_TIME_THRESHOLD_SECS + 5.0

            def fake_replicate_dataset(job: Job, src_dataset: str, tid: str, retry: Retry) -> bool:
                # Simple phase markers for test determinism
                if job is job_a:
                    a_repl_started.set()
                # No real work; replicate successfully
                if job is job_a:
                    a_repl_done.set()
                return True

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", side_effect=fake_replicate_dataset),
                patch("bzfs_main.bzfs.set_last_modification_time_safe", side_effect=wrapped_set_last_modification_time_safe),
                patch("time.time", return_value=mature_now),
            ):
                t_a = threading.Thread(target=run_job_a)
                t_b = threading.Thread(target=run_job_b)
                t_a.start()
                t_b.start()
                t_a.join()
                t_b.join()

            # Assert: both files reflect A's newer timestamps
            self.assertEqual(a_src_changed, SnapshotCache(job_a).get_snapshots_changed(last_repl_file))
            self.assertEqual(a_dst_changed, SnapshotCache(job_a).get_snapshots_changed(dst_cache_file))
            # And both find-stale and refresh were exercised deterministically for A
            self.assertGreaterEqual(a_counts["find"], 1)
            self.assertGreaterEqual(a_counts["refresh"], 1)

    def test_concurrent_monitor_and_replicate_monotonic(self) -> None:
        """Purpose: Demonstrate that monitor (=== caches) and replicate (== and dst "=") can run concurrently without
        causing regressions in each other's files. Independent subsystems must preserve their invariants even when
        interleaved by schedulers or operator actions.

        Assumptions: Monitor and replicate update disjoint files; all writes are monotonic; events can coordinate a
        deterministic interleaving.

        Constraints: No ZFS; thread-based concurrency with events orders writes; inputs are controlled and realistic.

        Verification: Monitor first writes a newer (creation, snapshots_changed) pair to its per-label file; replicate
        later writes older values to its own caches. We assert monitor's file retains the newer tuple, and replicate's
        files reflect their writes. If this failed, alerts could be suppressed or replication might skip incorrectly.

        Design Rationale: This isolates cross-subsystem interference risks using real IO and the production helpers,
        avoiding external flakiness while remaining faithful to real behavior.

        Monitor and replicate run concurrently; each cache remains monotonic for its file set.
        Monitor writes per-label monitor caches (===) with newer values; Replication then attempts older writes to its own
        caches (== last replicated, and dst '='). Each should preserve monotonic values independently.
        """

        plan = {"org": {"onsite": {"daily": {"latest": {"warning": "1 seconds", "critical": "2 seconds"}}}}}
        with self.job_context(
            [
                "--monitor-snapshots",
                str(plan),
                "--monitor-snapshots-dont-warn",
                "--monitor-snapshots-dont-crit",
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job_m, tmpdir):
            job_m.params.src.root_dataset = SRC_DATASET
            job_m.params.dst.root_dataset = DST_DATASET
            job_r = self.make_job_for_tmp(tmpdir, [SRC_DATASET, DST_DATASET])
            job_r.params.src.root_dataset = SRC_DATASET
            job_r.params.dst.root_dataset = DST_DATASET
            job_r.dst_dataset_exists[DST_DATASET] = True

            # Identify monitor cache file path
            alerts = job_m.params.monitor_snapshots_config.alerts
            alert = alerts[0]
            lbl = alert.label
            cfg = alert.latest
            assert cfg is not None
            mon_cache_label = self.monitor_cache_label(cfg.kind, lbl, alerts)
            mon_cache_file = SnapshotCache(job_m).last_modified_cache_file(job_m.params.src, SRC_DATASET, mon_cache_label)

            # Identify replicate cache files
            cache_label = self.replication_cache_label(job_r, DST_DATASET)
            last_repl_file = SnapshotCache(job_r).last_modified_cache_file(job_r.params.src, SRC_DATASET, cache_label)
            dst_cache_file = SnapshotCache(job_r).last_modified_cache_file(job_r.params.dst, DST_DATASET)

            # Values: Monitor newer, Replicate attempts older
            mon_creation = 2_000_000_000
            mon_snap_changed = 2_000_000_050
            repl_src_changed_a = 1_900_000_000
            repl_dst_changed_a = 1_900_000_030

            # Setup monitor job properties and behavior
            job_m.params.create_src_snapshots_config.current_datetime = datetime(2024, 1, 1, 0, 0, 12, tzinfo=timezone.utc)
            job_m.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=mon_snap_changed)

            def fake_handle_minmax_snapshots(
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                fn_latest(0, mon_creation, SRC_DATASET, "")
                return []

            # Setup replicate job behavior
            job_r.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=repl_src_changed_a)
            event_monitoring_written = threading.Event()
            r_repl_done = threading.Event()
            r_counts = {"find": 0, "refresh": 0}

            orig_set_last_modification_time_safe = snapshot_cache.set_last_modification_time_safe

            def wrapped_set_last_modification_time_safe(
                path: str, unixtime_in_secs: int, if_more_recent: bool = False
            ) -> None:
                orig_set_last_modification_time_safe(path, unixtime_in_secs=unixtime_in_secs, if_more_recent=if_more_recent)
                if path == mon_cache_file:
                    event_monitoring_written.set()

            def fake_zfs_get_snapshots_changed_r(_remote: Remote, _datasets: list[str]) -> dict[str, int]:
                # Phase-aware counts for replicate job
                if not r_repl_done.is_set():
                    r_counts["find"] += 1
                else:
                    r_counts["refresh"] += 1
                return {DST_DATASET: repl_dst_changed_a}

            def run_monitor_snapshots() -> None:
                with patch.object(job_m, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots):
                    job_m.monitor_snapshots(job_m.params.src, [SRC_DATASET])

            def run_replicate_datasets() -> None:
                event_monitoring_written.wait(timeout=5)
                with patch.object(job_r.cache, "zfs_get_snapshots_changed", side_effect=fake_zfs_get_snapshots_changed_r):
                    job_r.replicate_datasets([SRC_DATASET], task_description="R", max_workers=1)

            # Prepopulate to force find-stale and refresh for replicate: src '==' equality (mature), dst '=' mismatch
            set_last_modification_time_safe(last_repl_file, unixtime_in_secs=repl_src_changed_a, if_more_recent=True)
            set_last_modification_time_safe(dst_cache_file, unixtime_in_secs=1_111_111_111, if_more_recent=True)
            mature_now = max(repl_src_changed_a, repl_dst_changed_a) + MATURITY_TIME_THRESHOLD_SECS + 5.0

            def fake_replicate_dataset(job: Job, src_dataset: str, tid: str, retry: Retry) -> bool:
                if job is job_r:
                    r_repl_done.set()
                return True

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", side_effect=fake_replicate_dataset),
                patch("bzfs_main.bzfs.set_last_modification_time_safe", side_effect=wrapped_set_last_modification_time_safe),
                patch("time.time", return_value=mature_now),
            ):
                t_m = threading.Thread(target=run_monitor_snapshots)
                t_r = threading.Thread(target=run_replicate_datasets)
                t_m.start()
                t_r.start()
                t_m.join()
                t_r.join()

            # Assert: monitor cache remains at newer values; replicate caches reflect its own writes
            atime, mtime = SnapshotCache(job_m).get_snapshots_changed2(mon_cache_file)
            self.assertEqual(mon_creation, atime)
            self.assertEqual(mon_snap_changed, mtime)
            self.assertEqual(repl_src_changed_a, SnapshotCache(job_r).get_snapshots_changed(last_repl_file))
            self.assertEqual(repl_dst_changed_a, SnapshotCache(job_r).get_snapshots_changed(dst_cache_file))
            self.assertGreaterEqual(r_counts["find"], 1)
            self.assertGreaterEqual(r_counts["refresh"], 1)

    def test_concurrent_snapshot_and_replicate_monotonic(self) -> None:
        """Purpose: Validate that snapshot scheduling updates (per-label creation cache and src "=") and replication
        updates (src "==" and dst "=") remain monotonic when executed concurrently, preserving correctness of both
        scheduling and cheap-skip decisions.

        Assumptions: Each path writes different files; writes are monotonic; a simple event can coordinate ordering.

        Constraints: No ZFS; drive the min/max scan with a stub that supplies a controlled creation time; real IO for
        cache writes; deterministic sequencing with events.

        Verification: Snapshot path writes newer values first, replicate writes older values second. Assertions verify
        that snapshot per-label cache retains the newer creation (and src "=" reflects the newer snapshots_changed) and
        that replication's caches reflect their own values without regressing snapshot caches. If this failed, snapshot
        due-ness computation or replication skip logic could be corrupted.

        Design Rationale: This targeted concurrency test covers the most common interleaving between creation and
        replication phases without external dependencies."""
        """Snapshot scheduling (find_datasets_to_snapshot) and replicate run concurrently; caches remain monotonic.

        Snapshot scheduling writes per-label creation caches and src '='; Replicate writes last replicated and dst '='. This
        ensures no regression across either while running in parallel.
        """

        with self.job_context(
            [
                "--create-src-snapshots",
                "--create-src-snapshots-plan",
                str({"bzfs": {"onsite": {"hourly": 1}}}),
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job_s, tmpdir):
            job_s.params.src.root_dataset = SRC_DATASET
            job_s.params.dst.root_dataset = DST_DATASET
            job_r = self.make_job_for_tmp(tmpdir, [SRC_DATASET, DST_DATASET])  # replicate job
            job_r.params.src.root_dataset = SRC_DATASET
            job_r.params.dst.root_dataset = DST_DATASET
            job_r.dst_dataset_exists[DST_DATASET] = True

            # Labels and cache files for snapshot path
            labels = job_s.params.create_src_snapshots_config.snapshot_labels()
            lbl = labels[0]
            lbl_str = self.snapshot_cache_label(lbl)
            label_cache_file = SnapshotCache(job_s).last_modified_cache_file(job_s.params.src, SRC_DATASET, lbl_str)
            src_cache_file = SnapshotCache(job_s).last_modified_cache_file(job_s.params.src, SRC_DATASET)

            # Replicate cache files
            cache_label = self.replication_cache_label(job_r, DST_DATASET)
            last_repl_file = SnapshotCache(job_r).last_modified_cache_file(job_r.params.src, SRC_DATASET, cache_label)
            dst_cache_file = SnapshotCache(job_r).last_modified_cache_file(job_r.params.dst, DST_DATASET)

            # Times: snapshot newer; replicate older
            snap_creation = 2_000_000_000
            snap_src_changed = 2_000_000_090
            repl_src_changed = 1_900_000_000
            repl_dst_changed = 1_900_000_070

            job_s.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=snap_src_changed)
            job_r.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=repl_src_changed)

            # Patch handle_minmax to provide creation time; ensure caching path is taken
            def fake_handle_minmax_snapshots(
                remote: Remote,
                datasets: list[str],
                labels_list: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                fn_latest(0, snap_creation, SRC_DATASET, "")
                if fn_on_finish_dataset:
                    fn_on_finish_dataset(SRC_DATASET)
                return []

            event_snapshot_written = threading.Event()
            r_repl_done2 = threading.Event()
            r_counts2 = {"find": 0, "refresh": 0}

            orig_set_last_modification_time = snapshot_cache.set_last_modification_time_safe

            def wrapped_set_last_modification_time(path: str, unixtime_in_secs: int, if_more_recent: bool = False) -> None:
                orig_set_last_modification_time(path, unixtime_in_secs=unixtime_in_secs, if_more_recent=if_more_recent)
                if path in (label_cache_file, src_cache_file):
                    event_snapshot_written.set()

            def fake_zfs_get_snapshots_changed_r(_remote: Remote, _datasets: list[str]) -> dict[str, int]:
                if not r_repl_done2.is_set():
                    r_counts2["find"] += 1
                else:
                    r_counts2["refresh"] += 1
                return {DST_DATASET: repl_dst_changed}

            def run_find_datasets_to_snapshot() -> None:
                with patch.object(job_s, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots):
                    # Just compute plan; we don't need to create snapshots
                    job_s.find_datasets_to_snapshot([SRC_DATASET])

            def run_replicate_datasets() -> None:
                event_snapshot_written.wait(timeout=5)
                with patch.object(job_r.cache, "zfs_get_snapshots_changed", side_effect=fake_zfs_get_snapshots_changed_r):
                    job_r.replicate_datasets([SRC_DATASET], task_description="R", max_workers=1)

            # Prepopulate replicate caches to force find-stale and refresh
            set_last_modification_time_safe(last_repl_file, unixtime_in_secs=repl_src_changed, if_more_recent=True)
            set_last_modification_time_safe(dst_cache_file, unixtime_in_secs=1_222_222_222, if_more_recent=True)
            mature_now2 = max(repl_src_changed, repl_dst_changed) + MATURITY_TIME_THRESHOLD_SECS + 5.0

            def fake_replicate_dataset(job: Job, src_dataset: str, tid: str, retry: Retry) -> bool:
                if job is job_r:
                    r_repl_done2.set()
                return True

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", side_effect=fake_replicate_dataset),
                patch("bzfs_main.bzfs.set_last_modification_time_safe", side_effect=wrapped_set_last_modification_time),
                patch("time.time", return_value=mature_now2),
            ):
                t_s = threading.Thread(target=run_find_datasets_to_snapshot)
                t_r = threading.Thread(target=run_replicate_datasets)
                t_s.start()
                t_r.start()
                t_s.join()
                t_r.join()

            # Assert snapshot caches reflect newer values and replicate caches reflect their written values
            at, mt = SnapshotCache(job_s).get_snapshots_changed2(label_cache_file)
            self.assertEqual(snap_creation, at)
            # Scheduler per-label mtime must equal snapshots_changed observed at write time
            self.assertEqual(snap_src_changed, mt)
            self.assertEqual(snap_src_changed, SnapshotCache(job_s).get_snapshots_changed(src_cache_file))
            self.assertEqual(repl_src_changed, SnapshotCache(job_r).get_snapshots_changed(last_repl_file))
            self.assertEqual(repl_dst_changed, SnapshotCache(job_r).get_snapshots_changed(dst_cache_file))
            self.assertGreaterEqual(r_counts2["find"], 1)
            self.assertGreaterEqual(r_counts2["refresh"], 1)

    def test_three_way_monitor_snapshot_replicate_interleaving_monotonic(self) -> None:
        """Purpose: Stress a three-way interleaving of monitor, snapshot scheduling, and replication. We first perform a
        sequence (monitor --> snapshot --> replicate) writing newer values, then re-run each with older values to attempt
        regressions. All cache families must retain the newer values.

        Assumptions: Subsystems touch distinct files; writes are monotonic; ordering can be steered with events;
        integer-second times.

        Constraints: No ZFS; use deterministic stubs for min/max and property refresh; real IO for cache files.

        Verification: After phase 1, per-label monitor caches hold (creation, snapshots_changed), per-label snapshot
        caches hold creation (duplicated to mtime by design), src "=" reflects newer snapshots_changed, and src "=="/dst
        "=" reflect replication values. Phase 2 runs older attempts; all files must remain unchanged. If not, alerts or
        replication decisions could be wrong.

        Design Rationale: A phased scenario captures realistic orchestration while keeping the test hermetic and fast,
        ensuring the system's invariants hold under complex but common operational patterns.

        Three-way interleaving of monitor + snapshot + replicate with later older attempts; all caches stay monotonic.

        Order:
          1) Monitor writes newer monitor cache (===)
          2) Snapshot scheduling writes newer per-label creation cache and src '='
          3) Replicate writes older last-replicated (==) and dst '='
          4) Monitor attempts older write (must not regress)
          5) Snapshot attempts older write (must not regress)
          6) Replicate attempts older write (must not regress)
        """
        plan = {"org": {"onsite": {"daily": {"latest": {"warning": "1 seconds", "critical": "2 seconds"}}}}}
        with self.job_context(
            [
                "--monitor-snapshots",
                str(plan),
                "--monitor-snapshots-dont-warn",
                "--monitor-snapshots-dont-crit",
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job_m, tmpdir):
            job_m.params.src.root_dataset = SRC_DATASET
            job_m.params.dst.root_dataset = DST_DATASET
            job_s = self.make_job_for_tmp(
                tmpdir,
                [
                    "--create-src-snapshots",
                    "--create-src-snapshots-plan",
                    str({"bzfs": {"onsite": {"hourly": 1}}}),
                    SRC_DATASET,
                    DST_DATASET,
                ],
            )
            job_s.params.src.root_dataset = SRC_DATASET
            job_s.params.dst.root_dataset = DST_DATASET
            job_r = self.make_job_for_tmp(tmpdir, [SRC_DATASET, DST_DATASET])  # datasets only
            job_r.params.src.root_dataset = SRC_DATASET
            job_r.params.dst.root_dataset = DST_DATASET
            job_r.dst_dataset_exists[DST_DATASET] = True

            # Resolve cache file paths
            alerts = job_m.params.monitor_snapshots_config.alerts
            alert = alerts[0]
            lbl_mon = alert.label
            cfg = alert.latest
            assert cfg is not None
            mon_cache_label = self.monitor_cache_label(cfg.kind, lbl_mon, alerts)
            mon_cache_file = SnapshotCache(job_m).last_modified_cache_file(job_m.params.src, SRC_DATASET, mon_cache_label)

            lbl_s = job_s.params.create_src_snapshots_config.snapshot_labels()[0]
            lbl_str = self.snapshot_cache_label(lbl_s)
            label_cache_file = SnapshotCache(job_s).last_modified_cache_file(job_s.params.src, SRC_DATASET, lbl_str)
            src_cache_file = SnapshotCache(job_s).last_modified_cache_file(job_s.params.src, SRC_DATASET)

            repl_cache_label = self.replication_cache_label(job_r, DST_DATASET)
            last_repl_file = SnapshotCache(job_r).last_modified_cache_file(job_r.params.src, SRC_DATASET, repl_cache_label)
            dst_cache_file = SnapshotCache(job_r).last_modified_cache_file(job_r.params.dst, DST_DATASET)

            # Timestamps
            m_cre, m_sc = 2_000_000_000, 2_000_000_080
            s_cre, s_sc = 1_950_000_000, 1_950_000_050
            r_src_sc, r_dst_sc = 1_900_000_000, 1_900_000_030
            m_cre_old, m_sc_old = 1_800_000_000, 1_800_000_010
            r_src_old, r_dst_old = 1_600_000_000, 1_600_000_005

            # Configure jobs
            job_m.params.create_src_snapshots_config.current_datetime = datetime(2024, 1, 1, 0, 0, 20, tzinfo=timezone.utc)
            job_m.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=m_sc)
            job_s.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=s_sc)
            job_r.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=r_src_sc)

            # Event sequencing
            event_monitoring_written = threading.Event()
            event_snap_written = threading.Event()

            orig_set_last_modification_time = snapshot_cache.set_last_modification_time_safe

            def wrapped_set_last_modification_time(path: str, unixtime_in_secs: int, if_more_recent: bool = False) -> None:
                orig_set_last_modification_time(path, unixtime_in_secs=unixtime_in_secs, if_more_recent=if_more_recent)
                if path == mon_cache_file:
                    event_monitoring_written.set()
                if path in (label_cache_file, src_cache_file):
                    event_snap_written.set()

            def fake_handle_minmax_snapshots_monitor(
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                fn_latest(0, m_cre, SRC_DATASET, "")
                return []

            def fake_handle_minmax_snapshots_on_find_snapshots(
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                fn_latest(0, s_cre, SRC_DATASET, "")
                if fn_on_finish_dataset:
                    fn_on_finish_dataset(SRC_DATASET)
                return []

            r3_repl_done = threading.Event()
            r3_counts = {"find": 0, "refresh": 0}

            def fake_zfs_get_snapshots_changed_r(_remote: Remote, _datasets: list[str]) -> dict[str, int]:
                if not r3_repl_done.is_set():
                    r3_counts["find"] += 1
                else:
                    r3_counts["refresh"] += 1
                return {DST_DATASET: r_dst_sc}

            # First phase: Monitor -> Snapshot -> Replicate (older)
            def run_monitor_snapshots() -> None:
                with patch.object(job_m, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots_monitor):
                    job_m.monitor_snapshots(job_m.params.src, [SRC_DATASET])

            def run_find_datasets_to_snapshot() -> None:
                event_monitoring_written.wait(timeout=5)
                with patch.object(job_s, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots_on_find_snapshots):
                    job_s.find_datasets_to_snapshot([SRC_DATASET])

            def run_replicate_datasets() -> None:
                event_snap_written.wait(timeout=5)
                with patch.object(job_r.cache, "zfs_get_snapshots_changed", side_effect=fake_zfs_get_snapshots_changed_r):
                    job_r.replicate_datasets([SRC_DATASET], task_description="R", max_workers=1)

            # Prepopulate replicate caches to force find-stale and refresh in phase 1
            set_last_modification_time_safe(last_repl_file, unixtime_in_secs=r_src_sc, if_more_recent=True)
            set_last_modification_time_safe(dst_cache_file, unixtime_in_secs=1_333_333_333, if_more_recent=True)
            mature_now3 = max(r_src_sc, r_dst_sc) + MATURITY_TIME_THRESHOLD_SECS + 5.0

            def fake_replicate_dataset(job: Job, src_dataset: str, tid: str, retry: Retry) -> bool:
                if job is job_r:
                    r3_repl_done.set()
                return True

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", side_effect=fake_replicate_dataset),
                patch("bzfs_main.bzfs.set_last_modification_time_safe", side_effect=wrapped_set_last_modification_time),
                patch("time.time", return_value=mature_now3),
            ):
                t_m = threading.Thread(target=run_monitor_snapshots)
                t_s = threading.Thread(target=run_find_datasets_to_snapshot)
                t_r = threading.Thread(target=run_replicate_datasets)
                t_m.start()
                t_s.start()
                t_r.start()
                t_m.join()
                t_s.join()
                t_r.join()

            # Second phase: older attempts shall not regress
            job_m.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=m_sc_old)
            job_s.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=s_sc)
            job_r.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=r_src_old)

            def fake_handle_minmax_snapshots_monitor_old(
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                fn_latest(0, m_cre_old, SRC_DATASET, "")
                return []

            def fake_handle_minmax_snapshots_on_find_snapshots_old(
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                if fn_on_finish_dataset:
                    fn_on_finish_dataset(SRC_DATASET)
                return []

            def fake_zfs_get_snapshots_changed_r_old(_remote: Remote, _datasets: list[str]) -> dict[str, int]:
                # Deterministic: always return the intended dst snapshots_changed for replicate
                return {DST_DATASET: r_dst_old}

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", return_value=True),
            ):
                with (
                    patch.object(job_m, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots_monitor_old),
                    patch("bzfs_main.bzfs.set_last_modification_time_safe", side_effect=wrapped_set_last_modification_time),
                ):
                    job_m.monitor_snapshots(job_m.params.src, [SRC_DATASET])

                with (
                    patch.object(job_s, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots_on_find_snapshots_old),
                    patch("bzfs_main.bzfs.set_last_modification_time_safe", side_effect=wrapped_set_last_modification_time),
                ):
                    job_s.find_datasets_to_snapshot([SRC_DATASET])

                with patch.object(
                    job_r.cache, "zfs_get_snapshots_changed", side_effect=fake_zfs_get_snapshots_changed_r_old
                ):
                    job_r.replicate_datasets([SRC_DATASET], task_description="R2", max_workers=1)

            # Final assertions across all families
            at, mt = SnapshotCache(job_m).get_snapshots_changed2(mon_cache_file)
            self.assertEqual(m_cre, at)
            self.assertEqual(m_sc, mt)
            at, mt = SnapshotCache(job_s).get_snapshots_changed2(label_cache_file)
            self.assertEqual(s_cre, at)
            # Scheduler per-label mtime must equal snapshots_changed
            self.assertEqual(s_sc, mt)
            self.assertEqual(s_sc, SnapshotCache(job_s).get_snapshots_changed(src_cache_file))
            self.assertEqual(r_src_sc, SnapshotCache(job_r).get_snapshots_changed(last_repl_file))
            self.assertEqual(r_dst_sc, SnapshotCache(job_r).get_snapshots_changed(dst_cache_file))
            self.assertGreaterEqual(r3_counts["find"], 1)
            self.assertGreaterEqual(r3_counts["refresh"], 1)

    def test_three_way_invalidation_and_repopulate(self) -> None:
        """Purpose: Demonstrate safe dataset-level invalidation and subsequent repopulation across all cache families.
        When the src "=" cache mismatches the live snapshots_changed, find_datasets_to_snapshot() must invalidate the
        dataset's cache directory (zero atime/mtime) to force probing; later monitor/snapshot/replicate runs must
        repopulate their caches with monotonic writes.

        Assumptions: Property mismatches can occur; invalidation is intended and safe; later writes must not regress.

        Constraints: No ZFS; pre-populate mismatched values and suppress fallback to observe pure invalidation; then run
        each subsystem to repopulate.

        Verification: Phase A sets up mismatched src "=" and per-label snapshot caches, then confirms both are zeroed by
        invalidation. Phase B repopulates via monitor (===), snapshot (per-label creation and src "="), and replication
        (src "==" and dst "=") and asserts monotonic values stick. If this failed, stale caches could remain in force or
        later runs could regress freshly written values, degrading correctness.

        Design Rationale: A two-phase structure isolates invalidation behavior, then validates recovery, mirroring real
        operational timelines while keeping tests hermetic and deterministic.

        Demonstrates dataset-level invalidation to zero on mismatch and correct repopulation afterwards.

        Phase A: Prepopulate mismatching src '=' and per-label file; call find_datasets_to_snapshot with caching enabled
                 but suppress fallback population. Verify that invalidate_last_modified_cache_dataset() set both to zero.
        Phase B: Run monitor, snapshot, and replicate to repopulate each cache, then attempt older writes and assert
                 monotonic behavior.
        """

        plan = {"org": {"onsite": {"daily": {"latest": {"warning": "1 seconds", "critical": "2 seconds"}}}}}
        with self.job_context(
            [
                "--monitor-snapshots",
                str(plan),
                "--monitor-snapshots-dont-warn",
                "--monitor-snapshots-dont-crit",
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job_m, tmpdir):
            job_m.params.src.root_dataset = SRC_DATASET
            job_m.params.dst.root_dataset = DST_DATASET
            job_s = self.make_job_for_tmp(
                tmpdir,
                [
                    "--create-src-snapshots",
                    "--create-src-snapshots-plan",
                    str({"bzfs": {"onsite": {"hourly": 1}}}),
                    SRC_DATASET,
                    DST_DATASET,
                ],
            )
            job_s.params.src.root_dataset = SRC_DATASET
            job_s.params.dst.root_dataset = DST_DATASET
            job_r = self.make_job_for_tmp(tmpdir, [SRC_DATASET, DST_DATASET])  # datasets only
            job_r.params.src.root_dataset = SRC_DATASET
            job_r.params.dst.root_dataset = DST_DATASET
            job_r.dst_dataset_exists[DST_DATASET] = True

            # Resolve cache files
            labels = job_s.params.create_src_snapshots_config.snapshot_labels()
            lbl = labels[0]
            lbl_str = self.snapshot_cache_label(lbl)
            label_cache_file = SnapshotCache(job_s).last_modified_cache_file(job_s.params.src, SRC_DATASET, lbl_str)
            src_cache_file = SnapshotCache(job_s).last_modified_cache_file(job_s.params.src, SRC_DATASET)

            repl_cache_label = self.replication_cache_label(job_r, DST_DATASET)
            last_repl_file = SnapshotCache(job_r).last_modified_cache_file(job_r.params.src, SRC_DATASET, repl_cache_label)
            dst_cache_file = SnapshotCache(job_r).last_modified_cache_file(job_r.params.dst, DST_DATASET)

            # Prepopulate with mismatching values to trigger invalidation
            s_sc = 2_000_000_090
            job_s.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=s_sc)
            set_last_modification_time_safe(src_cache_file, unixtime_in_secs=1_700_000_010, if_more_recent=True)
            set_last_modification_time_safe(label_cache_file, unixtime_in_secs=1_700_000_000, if_more_recent=True)

            # Phase A: call find_datasets_to_snapshot but suppress fallback to observe invalidation effect
            def fake_handle_minmax_snapshots_noop(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels_list: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                return []

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots_noop),
            ):
                job_s.find_datasets_to_snapshot([SRC_DATASET])

            # Assert invalidation to zero happened for src '=' and per-label snapshot files
            self.assertEqual(0, SnapshotCache(job_s).get_snapshots_changed(src_cache_file))
            self.assertEqual(0, SnapshotCache(job_s).get_snapshots_changed(label_cache_file))

            # Phase B: repopulate via monitor, snapshot, replicate and assert monotonic
            # Monitor
            job_m.params.create_src_snapshots_config.current_datetime = datetime(2024, 1, 1, 0, 0, 30, tzinfo=timezone.utc)
            job_m.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=2_000_000_120)
            alerts = job_m.params.monitor_snapshots_config.alerts
            alert = alerts[0]
            lbl_mon = alert.label
            cfg = alert.latest
            assert cfg is not None
            mon_cache_label = self.monitor_cache_label(cfg.kind, lbl_mon, alerts)
            mon_cache_file = SnapshotCache(job_m).last_modified_cache_file(job_m.params.src, SRC_DATASET, mon_cache_label)

            def fake_handle_minmax_snapshots_monitor(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels_list: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                fn_latest(0, 2_000_000_100, SRC_DATASET, "")
                return []

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots_monitor),
            ):
                job_m.monitor_snapshots(job_m.params.src, [SRC_DATASET])

            at, mt = SnapshotCache(job_m).get_snapshots_changed2(mon_cache_file)
            self.assertEqual(2_000_000_100, at)
            self.assertEqual(2_000_000_120, mt)

            # Snapshot scheduling repopulates src '=' and per-label
            job_s.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=s_sc)

            def fake_handle_minmax_snapshots_snap(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels_list: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                fn_latest(0, 2_000_000_090, SRC_DATASET, "")
                if fn_on_finish_dataset:
                    fn_on_finish_dataset(SRC_DATASET)
                return []

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots_snap),
            ):
                job_s.find_datasets_to_snapshot([SRC_DATASET])

            at, mt = SnapshotCache(job_s).get_snapshots_changed2(label_cache_file)
            self.assertEqual(2_000_000_090, at)
            # Scheduler per-label mtime must equal snapshots_changed
            self.assertEqual(s_sc, mt)
            self.assertEqual(s_sc, SnapshotCache(job_s).get_snapshots_changed(src_cache_file))

            # Replicate repopulates last replicated and dst '='
            job_r.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=2_000_000_130)

            fake_zfs_get_snapshots_changed_r_state4 = {"i": 0}

            def fake_zfs_get_snapshots_changed_r(_remote: Remote, _datasets: list[str]) -> dict[str, int]:
                i = fake_zfs_get_snapshots_changed_r_state4["i"]
                fake_zfs_get_snapshots_changed_r_state4["i"] = i + 1
                return {} if i == 0 else {DST_DATASET: 2_000_000_140}

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", return_value=True),
                patch.object(job_r.cache, "zfs_get_snapshots_changed", side_effect=fake_zfs_get_snapshots_changed_r),
            ):
                job_r.replicate_datasets([SRC_DATASET], task_description="R", max_workers=1)

            self.assertEqual(2_000_000_130, SnapshotCache(job_r).get_snapshots_changed(last_repl_file))
            self.assertEqual(2_000_000_140, SnapshotCache(job_r).get_snapshots_changed(dst_cache_file))

    def test_snapshot_scheduler_reads_creation_from_label_atime_not_mtime(self) -> None:
        """find_datasets_to_snapshot must read creation time from the per-label cache file's atime, not its mtime.

        Bug being demonstrated: When a prior monitor run writes (creation, snapshots_changed) into the per-label cache
        (atime=creation, mtime=snapshots_changed), the current implementation incorrectly reads mtime via
        get_snapshots_changed(), treating snapshots_changed as the latest creation time. This can cause the scheduler to
        falsely conclude a label is up-to-date when another label updated snapshots_changed more recently.

        This test populates the dataset-level '=' cache to match src_properties.snapshots_changed so the caching path is
        taken, then writes a per-label file with (creation << snapshots_changed). The correct behavior is to schedule a
        new snapshot because the label's latest creation is older than the hourly interval. The current code (using mtime)
        would incorrectly skip. The fix is for find_datasets_to_snapshot to use get_snapshots_changed2()[0] for labels.
        """

        with self.job_context(
            [
                "--create-src-snapshots",
                "--create-src-snapshots-plan",
                str({"bzfs": {"onsite": {"hourly": 1}}}),
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET

            # Times: creation much older than hourly interval; snapshots_changed is recent
            creation_old = 2_000_000_000  # e.g., 03:33:20
            snapshots_changed_new = 2_000_003_600  # 04:00:00 (crosses hour boundary)

            # Use current_datetime after one hour has elapsed since creation
            # Choose a current time that is after 04:00:00 but before 05:00:00; e.g., 04:30:00
            job.params.create_src_snapshots_config.current_datetime = datetime.fromtimestamp(2_000_003_400, tz=timezone.utc)
            job.params.create_src_snapshots_config.tz = timezone.utc  # type: ignore[misc]  # cannot assign to final attribute

            # Label under test
            label = job.params.create_src_snapshots_config.snapshot_labels()[0]
            dataset = SRC_DATASET
            job.src_properties[dataset] = DatasetProperties(recordsize=0, snapshots_changed=snapshots_changed_new)

            # Populate dataset-level '=' cache to match live property so the caching branch is taken
            dataset_eq_file = SnapshotCache(job).last_modified_cache_file(job.params.src, dataset)
            set_last_modification_time_safe(dataset_eq_file, unixtime_in_secs=snapshots_changed_new, if_more_recent=True)

            # Populate per-label cache file as monitor would: (atime=creation, mtime=snapshots_changed)
            label_file = SnapshotCache(job).last_modified_cache_file(
                job.params.src, dataset, self.snapshot_cache_label(label)
            )
            set_last_modification_time_safe(
                label_file, unixtime_in_secs=(creation_old, snapshots_changed_new), if_more_recent=True
            )

            # Stub handle_minmax to prove we did not need fallback; it should not be called if cache is used correctly
            called: list[str] = []

            def fake_handle_minmax_snapshots(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                called.extend(datasets)
                return []

            # Run scheduler with caching enabled; ensure caches are considered mature
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
                patch("time.time", return_value=snapshots_changed_new + MATURITY_TIME_THRESHOLD_SECS + 0.2),
            ):
                result = job.find_datasets_to_snapshot([dataset])

            # Correct behavior: dataset must be scheduled for this label (creation is older than 1h)
            self.assertIn(dataset, result[label])
            # And we should not have needed to fall back to handle_minmax
            self.assertListEqual([], called)

    def test_snapshot_scheduler_label_cache_atime_greater_than_mtime_is_untrusted(self) -> None:
        """Snapshot scheduler must treat per-label caches with atime > mtime as untrusted and fall back.

        We seed the dataset-level '=' cache with a matured snapshots_changed (t0) and create a per-label cache file whose
        atime is later than its mtime (creation > snapshots_changed). With caching enabled and current time matured,
        find_datasets_to_snapshot() must invoke the fallback handler for the dataset.
        """

        with self.job_context(
            [
                "--create-src-snapshots",
                "--create-src-snapshots-plan",
                str({"bzfs": {"onsite": {"hourly": 1}}}),
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET

            dataset = SRC_DATASET
            label = job.params.create_src_snapshots_config.snapshot_labels()[0]
            label_hash = self.snapshot_cache_label(label)
            t0 = 2_000_000_000
            creation_after = t0 + 1
            job.src_properties[dataset] = DatasetProperties(recordsize=0, snapshots_changed=t0)

            # Seed dataset '=' with matured t0
            dataset_eq = SnapshotCache(job).last_modified_cache_file(job.params.src, dataset)
            set_last_modification_time_safe(dataset_eq, unixtime_in_secs=t0, if_more_recent=True)

            # Seed per-label file with atime > mtime (untrusted)
            label_path = SnapshotCache(job).last_modified_cache_file(job.params.src, dataset, label_hash)
            set_last_modification_time_safe(label_path, unixtime_in_secs=(creation_after, t0), if_more_recent=True)

            received: list[str] = []

            def record_fallback(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                received.extend(datasets)
                return []

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=record_fallback),
                patch("time.time", return_value=t0 + MATURITY_TIME_THRESHOLD_SECS + 0.5),
            ):
                job.find_datasets_to_snapshot([dataset])

            self.assertListEqual([dataset], received)

    def test_snapshot_scheduler_reads_written_label_cache_and_avoids_fallback(self) -> None:
        """End-to-end: first run writes per-label cache; second run reads it and avoids fallback.

        The first run forces fallback (dataset '=' absent), writes (creation, snapshots_changed) per-label cache and marks
        the dataset '=' in on_finish. The second run advances time to satisfy maturity and asserts no fallback.
        """

        with self.job_context(
            [
                "--create-src-snapshots",
                "--create-src-snapshots-plan",
                str({"bzfs": {"onsite": {"hourly": 1}}}),
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET

            dataset = SRC_DATASET
            creation = 2_000_000_000
            snapshots_changed = 2_000_000_050
            job.src_properties[dataset] = DatasetProperties(recordsize=0, snapshots_changed=snapshots_changed)

            # First run: force fallback (dataset '=' missing); supply creation and mark dataset '=' via on_finish
            fallback1: list[str] = []

            def provide_creation_then_mark(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                fallback1.extend(datasets)
                for ds in datasets:
                    fn_latest(0, creation, ds, "")
                    if fn_on_finish_dataset:
                        fn_on_finish_dataset(ds)
                return []

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=provide_creation_then_mark),
            ):
                job.find_datasets_to_snapshot([dataset])

            self.assertListEqual([dataset], fallback1)

            # Second run: matured time; should read cache and avoid fallback
            fallback2: list[str] = []

            def record_any_fallback(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                fallback2.extend(datasets)
                return []

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=record_any_fallback),
                patch("time.time", return_value=snapshots_changed + MATURITY_TIME_THRESHOLD_SECS + 0.5),
            ):
                job.find_datasets_to_snapshot([dataset])

            self.assertListEqual([], fallback2)

    def test_snapshot_scheduler_populates_label_cache_with_snapshots_changed_mtime(self) -> None:
        """Snapshot scheduler must persist per-label caches as (atime=creation, mtime=snapshots_changed).

        Purpose: Lock in the intended contract for the snapshot scheduler's per-label cache format under
        ``--cache-snapshots``. The cache for each label must encode two distinct signals that downstream logic relies
        on: the creation time of the latest matching snapshot (atime) and the dataset's current ``snapshots_changed``
        property observed at the time of writing (mtime). Correctly separating these concerns enables a safe, cheap
        fast path in subsequent scheduler runs: we may trust the cached creation time only when the label file's mtime
        equals the dataset-level "=" cache file's mtime, i.e. when both reflect the same live ``snapshots_changed``.

        Background: The scheduler first attempts to answer "is a new snapshot due?" purely from cache. If the dataset-
        level "=" cache indicates a mature, unchanged ``snapshots_changed`` value and the per-label cache also reflects
        that same value in mtime, then the per-label atime is considered trustworthy and an expensive probe via
        ``zfs list -t snapshot`` can be avoided. However, if the label cache encodes the wrong mtime (e.g., duplicates
        the creation time instead of the live ``snapshots_changed``), the equality check fails and the scheduler is
        forced to fall back to probing on every run, silently defeating the design and its performance/correctness
        trade-off.

        Setup: We construct a minimal scheduler job with an hourly plan and deterministic timestamps. We deliberately
        drive the fallback path by leaving the dataset "=" cache initially empty, then supply a specific creation time
        through a stubbed ``handle_minmax_snapshots`` while also invoking the scheduler's dataset-finish hook so that the
        dataset-level "=" file records the same ``snapshots_changed`` used by the scheduler during this write.

        Verification: After ``find_datasets_to_snapshot`` completes, we read the per-label cache and assert that
        atime equals the supplied creation time and mtime equals the scheduler's observed ``snapshots_changed``. This
        proves that future runs can trust the label atime when the dataset "=" cache matches, satisfying the design's
        safety invariant.

        Non-goals: This test does not exercise scheduling decisions themselves nor timerange logic; those are covered
        elsewhere. It strictly validates the persistence format and its precondition for safe reuse.
        """

        # Arrange: scheduler job with hourly plan
        with self.job_context(
            [
                "--create-src-snapshots",
                "--create-src-snapshots-plan",
                str({"bzfs": {"onsite": {"hourly": 1}}}),
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET

            # Fixed times
            creation = 2_000_000_000
            snapshots_changed = 2_000_003_600
            dataset = SRC_DATASET
            job.src_properties[dataset] = DatasetProperties(recordsize=0, snapshots_changed=snapshots_changed)

            label = job.params.create_src_snapshots_config.snapshot_labels()[0]
            label_str = self.snapshot_cache_label(label)
            label_path = SnapshotCache(job).last_modified_cache_file(job.params.src, dataset, label_str)

            # Force fallback by leaving dataset '=' cache empty and feed creation via handle_minmax
            def fake_handle_minmax_snapshots(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                fn_latest(0, creation, dataset, "s")
                # Ensure dataset-level '=' is also updated by the scheduler path
                if fn_on_finish_dataset:
                    fn_on_finish_dataset(dataset)
                return []

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
            ):
                job.find_datasets_to_snapshot([dataset])

            # Assert: per-label cache must have (creation, snapshots_changed)
            atime, mtime = SnapshotCache(job).get_snapshots_changed2(label_path)
            self.assertEqual(creation, atime)
            self.assertEqual(snapshots_changed, mtime)

    def test_snapshot_scheduler_threshold_controls_cache_trust(self) -> None:
        """Validates the snapshot scheduler's time-threshold gating for cache trust and clarifies the safety contract.

        Purpose and context: Under ``--cache-snapshots``, the scheduler attempts a fast path that avoids
        ``zfs list -t snapshot`` by reading two local cache files: the dataset-level "=" file
        (mtime = ZFS ``snapshots_changed``) and a per-label file (atime = latest matching snapshot creation,
        mtime = the same ``snapshots_changed`` observed at write time). To defend against equal-second races and clock
        skew, the cache must be "mature" before it is trusted. Monitor and replication already enforce this via
        ``MATURITY_TIME_THRESHOLD_SECS``; this test locks in the same guarantee for the scheduler.

        Scenario and expectations: We construct a single-dataset scheduler job with an hourly plan. We pre-populate the
        dataset-level "=" cache with a recent ``snapshots_changed`` time (``t0``) and the label cache with a creation
        time older than the schedule window, paired with the same ``t0`` in mtime, modeling a fresh write. We then drive
        two cases by patching ``time.time``: (1) "too recent": now <= ``t0 + MATURITY_TIME_THRESHOLD_SECS``; (2) "mature":
        now > ``t0 + MATURITY_TIME_THRESHOLD_SECS``. In the first case, the scheduler must not trust caches and should fall back
        to probing (we assert the fallback handler is invoked for the dataset). In the second case, with the same cache
        and live state, the scheduler should trust caches and avoid probing (we assert the handler is not invoked).

        Why this matters: The maturity gate prevents unsafe reuse of just-written caches when ZFS timestamps advance in
        integer seconds. Without it, the scheduler could incorrectly skip probing immediately after writes, potentially
        producing wrong "due" decisions and undermining both correctness and performance. With the gate, we retain the
        intended performance benefits when it is safe, while guaranteeing a fallback when it is not.

        Test methodology: This test is hermetic (no ZFS). It uses a temporary directory for caches, mocks the scheduler's
        environment, and relies on real atime/mtime updates to mirror production observables. It patches
        ``is_caching_snapshots`` to exercise the fast path and records fallback invocations by substituting a minimal
        ``handle_minmax_snapshots`` stub. The assertions directly encode the scheduler's trust/fallback contract.
        """

        with self.job_context(
            [
                "--create-src-snapshots",
                "--create-src-snapshots-plan",
                str({"bzfs": {"onsite": {"hourly": 1}}}),
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET
            dataset = SRC_DATASET
            label = job.params.create_src_snapshots_config.snapshot_labels()[0]
            t0 = 2_000_000_000
            creation_old = t0 - 120  # creation is older than schedule
            job.src_properties[dataset] = DatasetProperties(recordsize=0, snapshots_changed=t0)

            dataset_eq_file = SnapshotCache(job).last_modified_cache_file(job.params.src, dataset)
            set_last_modification_time_safe(dataset_eq_file, unixtime_in_secs=t0, if_more_recent=True)

            label_file = SnapshotCache(job).last_modified_cache_file(
                job.params.src, dataset, self.snapshot_cache_label(label)
            )
            set_last_modification_time_safe(label_file, unixtime_in_secs=(creation_old, t0), if_more_recent=True)

            received: list[str] = []

            def fake_handle_minmax_snapshots(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                received.extend(datasets)
                return []

            # Too recent -> fallback
            received.clear()
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
                patch("time.time", return_value=t0 + (MATURITY_TIME_THRESHOLD_SECS / 2)),
            ):
                job.find_datasets_to_snapshot([dataset])
            self.assertListEqual([dataset], received)

            # Mature -> trust cache, no fallback
            received.clear()
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
                patch("time.time", return_value=t0 + MATURITY_TIME_THRESHOLD_SECS + 0.2),
            ):
                job.find_datasets_to_snapshot([dataset])
            self.assertListEqual([], received)

    def test_snapshot_scheduler_label_cache_mtime_zero_is_untrusted(self) -> None:
        """Snapshot scheduler must not trust per-label caches when label.mtime == 0.

        This test formalizes a critical safety rule for the snapshot scheduler's fast path when
        ``--cache-snapshots`` is enabled. Our cache model separates two pieces of state per dataset:
        the dataset-level "=" cache file, whose mtime is the ZFS ``snapshots_changed`` property, and
        per-label cache files, which encode a pair (creation, snapshots_changed) as
        (atime, mtime). The scheduler may avoid probing via ``zfs list -t snapshot`` only when it
        can prove that the per-label creation time (atime) was recorded under the same live
        ``snapshots_changed`` value as currently observed in the dataset-level cache. That proof is
        established by strict equality between the label file's mtime and the dataset "=" file's
        mtime. Any deviation - including a zero mtime - indicates unknown provenance or a stale write
        and must force a safe fallback probe.

        We model a realistic scenario: the dataset-level "=" cache is seeded with a matured value
        ``t0``, while the per-label file is pre-populated with a plausible creation time but
        ``mtime=0``. Despite caches being "mature" with respect to the global threshold gating
        (``MATURITY_TIME_THRESHOLD_SECS``), the per-label file lacks the essential consistency witness (the
        matching mtime). Treating ``mtime==0`` as trusted would incorrectly permit reuse of this
        per-label creation time, potentially skipping a due snapshot and undermining correctness.

        The test stubs ``Job.handle_minmax_snapshots`` to record datasets for which a fallback is
        invoked. With caching enabled and time advanced beyond the threshold, the scheduler should
        decline the fast path and call the handler for our dataset. We assert that the dataset is
        indeed recorded, proving that ``mtime==0`` is untrusted and that a probe is required.

        This specification guards against subtle races and partial writes where per-label caches may
        exist without a known-good ``snapshots_changed`` context. By codifying "mtime must equal the
        dataset-level '=' mtime" and treating zero as untrusted, we maintain a soundness property for
        the cache: fast decisions are taken only when both signals - creation time and change marker - are
        internally consistent and sufficiently aged, otherwise the scheduler safely falls back to an
        O(M log M) probe.
        """

        with self.job_context(
            [
                "--create-src-snapshots",
                "--create-src-snapshots-plan",
                str({"bzfs": {"onsite": {"hourly": 1}}}),
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET

            dataset = SRC_DATASET
            label = job.params.create_src_snapshots_config.snapshot_labels()[0]
            t0 = 2_000_000_000
            creation_old = t0 - 3600
            job.src_properties[dataset] = DatasetProperties(recordsize=0, snapshots_changed=t0)

            # Seed dataset '=' with matured t0
            dataset_eq_file = SnapshotCache(job).last_modified_cache_file(job.params.src, dataset)
            set_last_modification_time_safe(dataset_eq_file, unixtime_in_secs=t0, if_more_recent=True)

            # Seed per-label cache with mtime=0 (unknown) and some creation time
            label_file = SnapshotCache(job).last_modified_cache_file(
                job.params.src, dataset, self.snapshot_cache_label(label)
            )
            set_last_modification_time_safe(label_file, unixtime_in_secs=(creation_old, 0), if_more_recent=True)

            received: list[str] = []

            def fake_handle_minmax_snapshots(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                received.extend(datasets)
                return []

            # Advance time beyond threshold so maturity does not block cache trust
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
                patch("time.time", return_value=t0 + MATURITY_TIME_THRESHOLD_SECS + 0.2),
            ):
                job.find_datasets_to_snapshot([dataset])

            # Correct behavior: fallback invoked due to label.mtime==0 (untrusted)
            self.assertListEqual([dataset], received)

    def test_snapshot_scheduler_label_cache_mtime_must_match_dataset_cache(self) -> None:
        """Validates the scheduler's staleness guard for per-label cache files: trust the label's atime (latest matching
        snapshot creation) only if the label file's mtime equals the dataset-level "=" file's ``snapshots_changed`` value at
        the moment of scheduling. Otherwise, fall back to probing via ``handle_minmax_snapshots``.

        Background: Our cache design intentionally separates two concepts. For each dataset, the "=" cache file stores
        the ZFS ``snapshots_changed`` property in its mtime. For each label under a dataset, a per-label cache file
        stores (creation, snapshots_changed) as (atime, mtime), where atime is the creation time of the latest snapshot
        matching that label, and mtime is the dataset's ``snapshots_changed`` at the time of writing. This lets the
        scheduler avoid expensive ``zfs list -t snapshot`` calls when both pieces of information are recent and
        consistent.

        Problem: If the dataset changes after the per-label file was written (i.e., the dataset "=" mtime moves forward
        while the label file's mtime remains behind), then the label's atime may be stale. Trusting it unconditionally
        can cause the scheduler to incorrectly conclude that a label is up-to-date, skipping a snapshot that is due.

        Test setup and expectation: We simulate this staleness by (1) setting the dataset "=" cache mtime to T1, (2)
        writing the per-label cache with atime=creation_old and mtime=T0, where T0 != T1, and (3) configuring
        ``src_properties`` to reflect T1. The scheduler is then invoked with caching enabled. The correct behavior is to
        detect the mismatch (label.mtime != dataset "=".mtime) and decline to trust the label's atime, forcing a
        fallback probe. We verify this by stubbing ``handle_minmax_snapshots`` to record invocations and asserting that
        it is called for the dataset.

        Why this matters: This check hardens correctness for mixed workloads where monitor, replication, and snapshot
        scheduling interleave. It ensures that cheap scheduling decisions are made only when cached state is internally
        consistent, while still enabling fast paths when it is safe to do so. The test is hermetic (no ZFS), uses real
        on-disk timestamps, and thus mirrors production observables faithfully.
        """

        with self.job_context(
            [
                "--create-src-snapshots",
                "--create-src-snapshots-plan",
                str({"bzfs": {"onsite": {"hourly": 1}}}),
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET

            dataset = SRC_DATASET
            label = job.params.create_src_snapshots_config.snapshot_labels()[0]
            creation_old = 2_000_000_000
            t0 = 2_000_000_050
            t1 = 2_000_000_100
            job.src_properties[dataset] = DatasetProperties(recordsize=0, snapshots_changed=t1)

            # dataset '=' cache reflects t1
            dataset_eq_file = SnapshotCache(job).last_modified_cache_file(job.params.src, dataset)
            set_last_modification_time_safe(dataset_eq_file, unixtime_in_secs=t1, if_more_recent=True)

            # per-label cache has mtime=t0 (stale) and atime=creation_old
            label_file = SnapshotCache(job).last_modified_cache_file(
                job.params.src, dataset, self.snapshot_cache_label(label)
            )
            set_last_modification_time_safe(label_file, unixtime_in_secs=(creation_old, t0), if_more_recent=True)

            received: list[str] = []

            def fake_handle_minmax_snapshots(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                received.extend(datasets)
                return []

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
            ):
                job.find_datasets_to_snapshot([dataset])

            # Must fall back to handler due to mtime mismatch
            self.assertListEqual([dataset], received)

    def test_replication_cache_updates_done_datasets_despite_partial_failure(self) -> None:
        """Ensure partial replication failures still refresh caches for done datasets.

        Purpose: A mixed-success batch must not block cache refresh for datasets that actually completed replication
        (the "done_datasets" set), or future runs would pay repeated "zfs list -t snapshot" costs. This test models
        a two-dataset replication where one dataset fails and the other succeeds, and verifies cache files are
        updated for the done dataset.
        """

        with self.job_context([SRC_DATASET, DST_DATASET]) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET
            job.params.dst.root_dataset = DST_DATASET

            ds_ok = f"{SRC_DATASET}/ok"
            ds_fail = f"{SRC_DATASET}/fail"
            datasets = [ds_ok, ds_fail]

            # Pre-populate properties and existence for both datasets
            snapshots_changed_value = 2_000_000_000
            for ds in datasets:
                job.src_properties[ds] = DatasetProperties(recordsize=0, snapshots_changed=snapshots_changed_value)
                job.dst_dataset_exists[ds.replace("src", "dst")] = True

            def fake_replicate_dataset(job: Job, src_dataset: str, tid: str, retry: Retry) -> bool:
                """Simulate replicate_dataset(job, src_dataset, tid, retry) with one failing dataset."""
                if src_dataset == ds_fail:
                    raise subprocess.CalledProcessError(1, "fail")
                return True

            def fake_zfs_get_snapshots_changed(_remote: Remote, datasets_arg: list[str]) -> dict[str, int]:
                """Return dst snapshots_changed timestamps mirroring the provided src datasets list."""

                return {src_dataset.replace("src", "dst"): snapshots_changed_value for src_dataset in datasets_arg}

            def fake_process_datasets(
                log: logging.Logger,
                datasets: list[str],
                process_dataset: Callable[[str, str, Retry], bool],
                skip_tree_on_error: Callable[[str], bool],
                **_: object,
            ) -> bool:
                """Call process_dataset for each dataset and report whether any raised an exception."""

                del log, skip_tree_on_error
                any_failed = False
                for idx, dataset in enumerate(datasets):
                    try:
                        process_dataset(
                            dataset,
                            "tid",
                            Retry(
                                count=idx,
                                start_time_nanos=0,
                                attempt_start_time_nanos=0,
                                policy=RetryPolicy(),
                                config=RetryConfig(),
                                log=None,
                                previous_outcomes=(),
                            ),
                        )
                    except Exception:
                        any_failed = True
                return any_failed

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", side_effect=fake_replicate_dataset),
                patch.object(SnapshotCache, "zfs_get_snapshots_changed", side_effect=fake_zfs_get_snapshots_changed),
                patch("bzfs_main.bzfs.set_last_modification_time_safe") as mock_set_time,
                patch(
                    "bzfs_main.bzfs.process_datasets_in_parallel_and_fault_tolerant",
                    side_effect=fake_process_datasets,
                ),
            ):
                failed = job.replicate_datasets(datasets, "desc", max_workers=2)
                self.assertTrue(failed)

                # Identify cache files for the done dataset only
                cache_label = self.replication_cache_label(job, ds_ok.replace("src", "dst"))
                src_repl_file = SnapshotCache(job).last_modified_cache_file(job.params.src, ds_ok, cache_label)
                dst_eq_file = SnapshotCache(job).last_modified_cache_file(job.params.dst, ds_ok.replace("src", "dst"))

                calls = mock_set_time.call_args_list
                src_calls = [call for call in calls if call.args[0] == src_repl_file]
                dst_calls = [call for call in calls if call.args[0] == dst_eq_file]

                self.assertTrue(src_calls, "Should update src replication cache for successful dataset")
                self.assertTrue(dst_calls, "Should update dst equality cache for successful dataset")

    def test_replication_cache_must_not_collide_across_dst_ports(self) -> None:
        """Purpose: Prove that replication-scoped caches (the src-side "==" markers) must be namespaced by
        destination SSH port in addition to user@host to avoid cross-port collisions.

        Scenario: Operators often reach different destinations behind the same user@host via distinct SSH ports
        (for example, multiple port forwards, multi-tenant gateways, or ephemeral test hosts). If the replication
        cache path only keys on ``user@host`` and omits the port, two independent destinations will read and write
        the same "==" file. That cross-contamination can cause cheap-skip logic to misclassify datasets as
        "Already up-to-date [cached]" on the wrong destination, silently skipping a required replication.

        Setup: We construct two Jobs targeting the same dataset pair and identical snapshot filters, differing only
        by the destination port. We compute the replication-scoped cache file for each and assert those paths are
        different. If they were identical, a later job could overwrite the marker established by another job pointed
        at a different endpoint, undermining correctness.

        Expectation: Paths must differ across ports; otherwise, the design is unsafe.
        """

        with self.job_context([SRC_DATASET, DST_DATASET]) as (job_a, tmpdir):
            # Same user@host, different ports
            job_a.params.dst.basis_ssh_user = "u"  # type: ignore[misc]  # cannot assign to final attribute
            job_a.params.dst.basis_ssh_host = "h"  # type: ignore[misc]  # cannot assign to final attribute
            job_a.params.dst.ssh_port = 1111  # type: ignore[misc]  # cannot assign to final attribute
            job_a.params.dst.ssh_user = "u"
            job_a.params.dst.ssh_host = "h"
            job_a.params.dst.ssh_user_host = "u@h"

            job_b = self.make_job_for_tmp(tmpdir, [SRC_DATASET, DST_DATASET])
            job_b.params.dst.basis_ssh_user = "u"  # type: ignore[misc]  # cannot assign to final attribute
            job_b.params.dst.basis_ssh_host = "h"  # type: ignore[misc]  # cannot assign to final attribute
            job_b.params.dst.ssh_port = 2222  # type: ignore[misc]  # cannot assign to final attribute
            job_b.params.dst.ssh_user = "u"
            job_b.params.dst.ssh_host = "h"
            job_b.params.dst.ssh_user_host = "u@h"

            label_a = self.replication_cache_label(job_a, DST_DATASET)
            path_a = SnapshotCache(job_a).last_modified_cache_file(job_a.params.src, SRC_DATASET, label_a)
            label_b = self.replication_cache_label(job_b, DST_DATASET)
            path_b = SnapshotCache(job_b).last_modified_cache_file(job_b.params.src, SRC_DATASET, label_b)

            self.assertNotEqual(
                path_a, path_b, msg=f"Replication cache path must include port to avoid collisions: {path_a}"
            )

    def test_replication_cache_collision_prevented_by_port_namespace(self) -> None:
        """Purpose: Demonstrate that port-aware cache namespacing prevents false cheap-skips across distinct
        destinations that share the same user@host.

        Scenario: Job A (user@host:1111) replicates and writes both the src "==" marker and the dst "=" cache with a
        mature value. Job B (user@host:2222) subsequently runs against a different endpoint. If cache paths were
        user@host-only, Job B would see Job A's mature markers, conclude that both sides match and are older than the
        threshold, and skip replication for the wrong destination. Such a skip risks divergence.

        Setup: We seed Job A's caches with a mature, equal timestamp and configure Job B to observe the same live
        src/dst values. We then run Job B with port-aware namespacing enabled (via Remote.cache_namespace()).

        Expectation: Because the port is part of the namespace, Job B reads different files from Job A and must not
        see those seeded markers. As a result, it processes the dataset (invokes replicate_dataset), proving that the
        namespacing prevents cross-port contamination in practice.
        """

        with self.job_context([SRC_DATASET, DST_DATASET]) as (job_a, tmpdir):
            job_a.params.src.root_dataset = SRC_DATASET
            job_a.params.dst.root_dataset = DST_DATASET
            job_b = self.make_job_for_tmp(tmpdir, [SRC_DATASET, DST_DATASET])
            job_b.params.src.root_dataset = SRC_DATASET
            job_b.params.dst.root_dataset = DST_DATASET
            # Same user@host, different port
            job_a.params.dst.basis_ssh_user = job_b.params.dst.basis_ssh_user = "u"  # type: ignore[misc]  # cannot assign to final attribute
            job_a.params.dst.basis_ssh_host = job_b.params.dst.basis_ssh_host = "h"  # type: ignore[misc]  # cannot assign to final attribute
            job_a.params.dst.ssh_user = job_b.params.dst.ssh_user = "u"
            job_a.params.dst.ssh_host = job_b.params.dst.ssh_host = "h"
            job_a.params.dst.ssh_user_host = job_b.params.dst.ssh_user_host = "u@h"
            job_a.params.dst.ssh_port = 1111  # type: ignore[misc]  # cannot assign to final attribute
            job_b.params.dst.ssh_port = 2222  # type: ignore[misc]  # cannot assign to final attribute
            job_a.dst_dataset_exists[job_a.params.dst.root_dataset] = True
            job_b.dst_dataset_exists[job_b.params.dst.root_dataset] = True

            # Shared hash but distinct cache file paths due to port segregation
            label = self.replication_cache_label(job_a, DST_DATASET)
            src_repl_file = SnapshotCache(job_a).last_modified_cache_file(job_a.params.src, SRC_DATASET, label)
            dst_eq_file = SnapshotCache(job_a).last_modified_cache_file(job_a.params.dst, DST_DATASET)

            # Prepare timestamps: mature + equal across caches
            t_src = int(time.time()) - int(MATURITY_TIME_THRESHOLD_SECS) - 5
            t_dst = t_src

            # Seed Job A caches as if it had just replicated
            set_last_modification_time_safe(src_repl_file, unixtime_in_secs=t_src, if_more_recent=True)
            set_last_modification_time_safe(dst_eq_file, unixtime_in_secs=t_dst, if_more_recent=True)

            # Job B observed live properties (simulate via stubs)
            job_b.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=t_src)

            called: list[str] = []

            def fake_replicate_dataset(job: Job, src_dataset: str, tid: str, retry: RetryPolicy) -> bool:
                called.append(src_dataset)
                return True

            # zfs_get_snapshots_changed(dst) returns t_dst for dst_dataset (equal to cached dst '=')
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", new=fake_replicate_dataset),
                patch.object(job_b.cache, "zfs_get_snapshots_changed", return_value={DST_DATASET: t_dst}),
            ):
                job_b.replicate_datasets([SRC_DATASET], task_description="T", max_workers=1)

            # Because paths are segregated by port, Job B processes the dataset (no false cheap-skip)
            self.assertListEqual([SRC_DATASET], called)

    def test_xcache_hits_when_replicating_many_unchanging_datasets(self) -> None:
        """Stress test: validate high cache hit rates for replicating large number of datasets that do not change.

        This test exercises the replicate path across a sizable, flat dataset tree (n=1200) to prove that the snapshot
        cache eliminates repeated probing when input state is stable. The first pass initializes the replication-scoped
        src "==" markers and the dst dataset-level "=" markers from real filesystem timestamps via
        set_last_modification_time_safe(), guarded by flock and if_more_recent=True. The second pass is intentionally a
        no-change cycle: src snapshots_changed remains identical and the dst snapshots_changed returned by the stubbed
        zfs_get_snapshots_changed() side effect matches. The replicate planner therefore classifies all datasets as
        already up-to-date using cache equality under the MATURITY_TIME_THRESHOLD_SECS maturity guard, yielding 100%
        cache hits and zero further work (replicate_dataset is never invoked).

        The test verifies: (1) hit/miss counters (first run: misses=n, second run: hits=n), (2) on-disk cache contents
        for a representative dataset (src "==" and dst "=" carry the expected integer seconds), and (3) strict
        unit-level isolation without requiring the zfs CLI or network access. This provides a realistic, repeatable
        performance signal for the --cache-snapshots feature in large deployments.
        """

        # Large but safe number of datasets for stress; adjust if CI is slow
        n = self._stresstest_num_datasets()
        now = int(time.time())
        sc = now - 5  # matured snapshots_changed to pass threshold gating; pretends last snapshot was created 5 seconds ago

        with self.job_context([SRC_DATASET, DST_DATASET]) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET
            job.params.dst.root_dataset = DST_DATASET

            # Build a large, flat tree of src datasets and corresponding dst datasets
            src_datasets = [f"{SRC_DATASET}/d{i:04d}" for i in range(n)]
            dst_datasets = [ds.replace(SRC_DATASET, DST_DATASET, 1) for ds in src_datasets]

            # Mark dst existence to avoid error-based skipping
            for ds in dst_datasets:
                job.dst_dataset_exists[ds] = True

            # Populate src properties with unchanged snapshots_changed
            for ds in src_datasets:
                job.src_properties[ds] = DatasetProperties(recordsize=0, snapshots_changed=sc)

            # Stub out remote queries; caching is enabled and dst snapshots_changed remains unchanged
            def fake_zfs_get_snapshots_changed(_remote: Remote, datasets: list[str]) -> dict[str, int]:
                return dict.fromkeys(datasets, sc)

            called: list[str] = []

            def fake_replicate_dataset(job: Job, src_dataset: str, tid: str, retry: RetryPolicy) -> bool:
                called.append(src_dataset)
                return True

            # First run: populate caches (expect misses == N, hits == 0; replicate called N times)
            job.num_cache_hits = job.num_cache_misses = 0
            called.clear()
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", new=fake_replicate_dataset),
                patch.object(SnapshotCache, "zfs_get_snapshots_changed", side_effect=fake_zfs_get_snapshots_changed),
            ):
                job.replicate_datasets(src_datasets, task_description="stress-1", max_workers=1)

            self.assertEqual(n, job.num_cache_misses)
            self.assertEqual(0, job.num_cache_hits)
            self.assertEqual(n, len(called))  # processed all datasets on first run

            # Verify on-disk cache timestamps for a sample dataset hit disk (src "==" and dst "=")
            sample_src = src_datasets[0]
            sample_dst = dst_datasets[0]
            # Replication-scoped src "==" and dst "=" cache paths
            cache_label = self.replication_cache_label(job, sample_dst)
            src_repl_file = SnapshotCache(job).last_modified_cache_file(job.params.src, sample_src, cache_label)
            dst_eq_file = SnapshotCache(job).last_modified_cache_file(job.params.dst, sample_dst)
            # Read back (ensure actual disk timestamps match expected values)
            self.assertEqual(sc, SnapshotCache(job).get_snapshots_changed(src_repl_file))
            self.assertEqual(sc, SnapshotCache(job).get_snapshots_changed(dst_eq_file))

            # Second run: unchanged; should be cheap-skipped entirely via caches
            before_hits, before_misses = job.num_cache_hits, job.num_cache_misses
            called.clear()
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", new=fake_replicate_dataset),
                patch.object(SnapshotCache, "zfs_get_snapshots_changed", side_effect=fake_zfs_get_snapshots_changed),
            ):
                job.replicate_datasets(src_datasets, task_description="stress-2", max_workers=1)

            self.assertEqual(before_misses, job.num_cache_misses)  # no additional misses
            self.assertEqual(before_hits + n, job.num_cache_hits)  # all datasets hit cache
            self.assertEqual(0, len(called))  # replicate not invoked on second run

    def test_xcache_hits_when_monitoring_many_unchanging_datasets(self) -> None:
        """Stress test: monitor path fully leverages the cache under no-change cycles across many datasets.

        This test validates that --monitor-snapshots achieves complete cache hits when snapshots_changed has not
        advanced since the prior run. We simulate a large set of datasets (n=1200) and perform two monitor passes. The
        first pass uses a stubbed handle_minmax_snapshots() to provide the creation time for each monitored label and
        writes per-dataset-and-label "===" cache entries on disk: atime stores the creation, mtime stores the dataset's
        snapshots_changed. On the second pass, we advance wall-clock time beyond MATURITY_TIME_THRESHOLD_SECS so those
        entries are considered "mature". Given equality between live snapshots_changed (from job.src_properties) and
        cached mtime, monitor trusts the cache and emits alerts (OK in our setup) without performing any fallback scanning.

        We assert: (1) first run increments misses by n (cache population), (2) second run increments hits by n with no
        additional misses, (3) a sample "===" cache file contains the exact creation and snapshots_changed seconds, and
        (4) no fallback to handle_minmax_snapshots occurs on the second pass. No zfs CLI is required.
        """

        n = self._stresstest_num_datasets()
        creation = 1_700_000_000  # arbitrary, stable
        sc = creation + 50  # snapshots_changed > creation

        plan = {"org": {"onsite": {"daily": {"latest": {"warning": "1 seconds", "critical": "2 seconds"}}}}}
        with self.job_context(
            [
                "--monitor-snapshots",
                str(plan),
                "--monitor-snapshots-dont-warn",
                "--monitor-snapshots-dont-crit",
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET
            job.params.dst.root_dataset = DST_DATASET
            # prevent alerts by aligning current time with creation
            job.params.create_src_snapshots_config.current_datetime = datetime.fromtimestamp(creation, tz=timezone.utc)

            src_datasets = [f"{SRC_DATASET}/d{i:04d}" for i in range(n)]
            for ds in src_datasets:
                job.src_properties[ds] = DatasetProperties(recordsize=0, snapshots_changed=sc)

            # First run: populate monitor caches via handle_minmax
            def fake_handle_minmax_snapshots1(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                for ds in datasets:
                    fn_latest(0, creation, ds, "s")
                return []

            job.num_cache_hits = job.num_cache_misses = 0
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots1),
            ):
                job.monitor_snapshots(job.params.src, src_datasets)

            self.assertEqual(n, job.num_cache_misses)
            self.assertEqual(0, job.num_cache_hits)

            # Verify on-disk per-label monitor cache for one dataset
            alerts = job.params.monitor_snapshots_config.alerts
            alert = alerts[0]
            cfg = alert.latest
            assert cfg is not None
            cache_label = self.monitor_cache_label(cfg.kind, alert.label, alerts)
            sample = src_datasets[0]
            cache_file = SnapshotCache(job).last_modified_cache_file(job.params.src, sample, cache_label)
            at, mt = SnapshotCache(job).get_snapshots_changed2(cache_file)
            self.assertEqual(creation, at)
            self.assertEqual(sc, mt)

            # Second run: matured trust --> all cache hits; ensure no fallback invocation
            received_count = {"n": 0}

            def fake_handle_minmax_snapshots2(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                received_count["n"] += len(datasets)
                return []

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots2),
                patch("time.time", return_value=sc + MATURITY_TIME_THRESHOLD_SECS + 1.0),
            ):
                job.monitor_snapshots(job.params.src, src_datasets)

            self.assertEqual(n, job.num_cache_hits)  # added n hits
            self.assertEqual(n, job.num_cache_misses)  # unchanged
            self.assertEqual(0, received_count["n"])  # no fallback

    def test_xcache_hits_when_snapshotting_many_unchanging_datasets_that_are_not_due_yet(self) -> None:
        """Stress test: snapshot scheduler relies exclusively on caches when the system is steady-state.

        This test covers the snapshot scheduling path used by --create-src-snapshots. We pre-populate both the dataset-
        level "=" cache (mtime = snapshots_changed) and the per-label creation caches (atime = creation, mtime = the
        same snapshots_changed) for n=1200 datasets. With these cache invariants in place, find_datasets_to_snapshot()
        can answer entirely from the cache as long as two conditions hold: (1) the dataset-level "=" entry equals the
        currently observed snapshots_changed (from job.src_properties) and is older than MATURITY_TIME_THRESHOLD_SECS;
        and (2) the per-label cache's mtime equals the dataset "=" entry, preventing stale creation times.

        We set the current scheduler time to a known instant (timezone-aware) and ensure that no dataset is due. The
        test asserts: (a) zero fallback to handle_minmax_snapshots(), and (b) the returned per-label mapping contains
        only empty lists (no datasets scheduled). This validates the intended, high-performance "all cache, no probing"
        steady-state behavior..
        """

        n = self._stresstest_num_datasets()
        # Use a fixed timestamp for deterministic label and creation
        base_dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        creation = int(base_dt.timestamp())
        sc = creation + 50

        with self.job_context(
            [
                "--create-src-snapshots",
                "--create-src-snapshots-plan",
                str({"bzfs": {"onsite": {"hourly": 1}}}),
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET
            job.params.dst.root_dataset = DST_DATASET
            job.params.create_src_snapshots_config.current_datetime = base_dt
            # Ensure scheduler uses timezone-aware datetimes for cache trust and next_event comparisons
            job.params.create_src_snapshots_config.tz = timezone.utc  # type: ignore[misc]  # cannot assign to final attribute

            src_datasets = [f"{SRC_DATASET}/d{i:04d}" for i in range(n)]
            src_datasets.sort()
            for ds in src_datasets:
                job.src_properties[ds] = DatasetProperties(recordsize=0, snapshots_changed=sc)

            # Prepare caches: dataset-level '=' and per-label caches with matching mtime=sc and atime=creation
            labels = job.params.create_src_snapshots_config.snapshot_labels()
            self.assertGreaterEqual(len(labels), 1)
            for ds in src_datasets:
                # '=' dataset-level cache
                set_last_modification_time_safe(
                    SnapshotCache(job).last_modified_cache_file(job.params.src, ds),
                    unixtime_in_secs=sc,
                    if_more_recent=True,
                )
                # Per-label caches (atime=creation, mtime=sc)
                for lbl in labels:
                    set_last_modification_time_safe(
                        SnapshotCache(job).last_modified_cache_file(job.params.src, ds, self.snapshot_cache_label(lbl)),
                        unixtime_in_secs=(creation, sc),
                        if_more_recent=True,
                    )

            # Detect any fallback: should be none when caches are trusted and equal
            received_count = {"n": 0}

            def fake_handle_minmax_snapshots(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels_: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                received_count["n"] += len(datasets)
                return []

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
                patch("time.time", return_value=sc + MATURITY_TIME_THRESHOLD_SECS + 1.0),
            ):
                mapping = job.find_datasets_to_snapshot(src_datasets)

            # No datasets are due; mapping contains only empty lists; and no fallback probing was required
            self.assertTrue(all(len(v) == 0 for v in mapping.values()))
            self.assertEqual(0, received_count["n"])  # everything answered from caches

    def test_clock_skew_snapshotting_when_initiator_vs_src_clocks_disagree(self) -> None:
        """Demonstrates the snapshoting scheduler's skew-resilient fast path when using --cache-snapshots.

        The scheduler may avoid `zfs list -t snapshot` by consulting two caches per dataset: the dataset-level "=" file,
        whose mtime is the ZFS snapshots_changed property, and per-label files whose atime is the latest creation time
        for that label and whose mtime records the snapshots_changed observed at write time. The fast path is trusted
        only if the dataset-level value equals the live property and is mature, and each label's mtime equals that same
        value, providing a precise provenance witness. We simulate initiator time behind the src host (src value in the
        "future") and pre-populate both caches to reflect the same future-valued snapshots_changed with a plausible
        creation time.

        In Phase 1, because initiator now is ≤ src snapshots_changed + threshold, the cache entries are not mature; the
        scheduler must fall back to probing, which we detect by our handler being invoked for the dataset.
        In Phase 2, we advance initiator time beyond the threshold so the same cache becomes mature and equal;
        the scheduler can then trust the caches, and no probing occurs (handler not invoked). This test preserves the
        safety contract: cached decisions are used only when maturity and equality validate them despite skew.

        - Cache model:
          - Dataset-level "=" file:
            - mtime: current ZFS snapshots_changed value.
          - Per-label file (under src dataset):
            - atime: latest matching snapshot creation time for the label.
            - mtime: snapshots_changed observed when the label file was written.

        - Trust conditions:
          - "=" equals live snapshots_changed and is mature.
          - Per-label mtime equals the dataset-level "=" value (provenance).
          - Only then may the scheduler safely reuse the label atime without probing.

        - Scenario and phases:
          - Initiator behind src (src value in the "future").
          - Phase 1: now <= src snapshots_changed + threshold -> not mature -> fallback (handler invoked).
          - Phase 2: advance time beyond threshold -> mature + equal -> trust caches -> no probing.

        - Outcome:
          - Preserves safety under skew and retains performance once caches are trustworthy.
        """

        with self.job_context(
            [
                "--create-src-snapshots",
                "--create-src-snapshots-plan",
                str({"bzfs": {"onsite": {"hourly": 1}}}),
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET
            dataset = SRC_DATASET

            now_i = 2_000_000_000
            t_src_future = now_i + 5_000
            creation_old = now_i - 3_600

            # Live property and caches
            job.src_properties[dataset] = DatasetProperties(recordsize=0, snapshots_changed=t_src_future)
            dataset_eq_file = SnapshotCache(job).last_modified_cache_file(job.params.src, dataset)
            set_last_modification_time_safe(dataset_eq_file, t_src_future, if_more_recent=True)
            label = job.params.create_src_snapshots_config.snapshot_labels()[0]
            label_file = SnapshotCache(job).last_modified_cache_file(
                job.params.src, dataset, self.snapshot_cache_label(label)
            )
            set_last_modification_time_safe(label_file, (creation_old, t_src_future), if_more_recent=True)

            received: list[str] = []

            def fake_handle_minmax_snapshots(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                received.extend(datasets)
                return []

            # Phase 1: initiator behind -> fallback
            received.clear()
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
                patch("time.time", return_value=now_i),
            ):
                job.find_datasets_to_snapshot([dataset])
            self.assertListEqual([dataset], received)

            # Phase 2: initiator catches up -> trust cache (no fallback)
            received.clear()
            now_i2 = t_src_future + MATURITY_TIME_THRESHOLD_SECS + 0.2
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
                patch("time.time", return_value=now_i2),
            ):
                job.find_datasets_to_snapshot([dataset])
            self.assertListEqual([], received)

    def test_clock_skew_replicate_when_src_dst_initiator_clocks_disagree(self) -> None:
        """Validates replication's cache-skip logic when the initiator observes skew relative to src and dst.

        The fast skip path in replicate_datasets() requires two independent cache validations:
        (A) the src "==" (last replicated) cache must equal the live src snapshots_changed and be older than the
        maturity threshold; and
        (B) the dst "=" (dataset-level) cache must equal the live dst snapshots_changed and also be mature.

        If either side is not mature or unequal, replication must process the dataset (no cheap skip), ensuring
        correctness despite clock skew. We construct a concrete skew: src's snapshots_changed is in the future
        relative to the initiator, while dst's is in the past. We then seed caches to equal these values.

        In Phase 1, initiator time is before the src value, so the src side fails the maturity test; we assert a
        cache miss and confirm that replicate_dataset is invoked.
        In Phase 2, we advance initiator time beyond the src maturity threshold while leaving dst unchanged and equal;
        now both sides are mature and equal, so a cheap skip is valid. We assert a cache hit and that replicate_dataset
        is not called. These checks lock in skew-robust semantics: equality is insufficient without maturity, and both
        src and dst must pass to safely skip work, preventing silent divergence or wasted replication.

        - Cheap-skip prerequisites in replicate_datasets():
          - Src "==" (last replicated) cache equals live src snapshots_changed and is mature.
          - Dst "=" (dataset) cache equals live dst snapshots_changed and is mature.
          - If either side fails, the dataset must be processed (no skip).

        - Scenario:
          - Src snapshots_changed is in the future vs. initiator; dst snapshots_changed is in the past.
          - Caches are seeded to equal the respective live values.

        - Phases:
          - Phase 1: Initiator before src value -> src not mature -> miss -> replicate_dataset runs.
          - Phase 2: Initiator beyond maturity -> both mature + equal -> hit -> replicate_dataset not called.

        - Outcome:
          - Locks in skew-robust semantics: equality alone is insufficient; both sides must also be mature.
          - Prevents silent divergence and avoids wasted work once maturity is established.
        """

        with self.job_context([SRC_DATASET, DST_DATASET]) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET
            job.params.dst.root_dataset = DST_DATASET
            job.dst_dataset_exists[DST_DATASET] = True

            # Disagreeing clocks
            now_i = 2_000_000_000
            t_src_future = now_i + 20_000
            t_dst_past = now_i - 30_000

            job.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=t_src_future)

            # Prepare replication-scoped caches
            repl_cache_label = self.replication_cache_label(job, DST_DATASET)
            src_repl_file = SnapshotCache(job).last_modified_cache_file(job.params.src, SRC_DATASET, repl_cache_label)
            dst_eq_file = SnapshotCache(job).last_modified_cache_file(job.params.dst, DST_DATASET)
            set_last_modification_time_safe(src_repl_file, t_src_future, if_more_recent=True)  # equality on src
            set_last_modification_time_safe(dst_eq_file, t_dst_past, if_more_recent=True)  # equality on dst

            # Phase 1: src is in the future -> miss; no cheap skip
            called: list[str] = []

            def fake_replicate_dataset(job: Job, src_dataset: str, tid: str, retry: Retry) -> bool:
                called.append(src_dataset)
                return True

            job.num_cache_hits = job.num_cache_misses = 0
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", new=fake_replicate_dataset),
                patch.object(job.cache, "zfs_get_snapshots_changed", return_value={DST_DATASET: t_dst_past}),
                patch("time.time", return_value=now_i),
            ):
                job.replicate_datasets([SRC_DATASET], task_description="skew", max_workers=1)
            self.assertListEqual([SRC_DATASET], called)
            self.assertEqual(0, job.num_cache_hits)
            self.assertEqual(1, job.num_cache_misses)

            # Phase 2: initiator catches up beyond maturity; equality on src and dst -> cheap skip
            now_i2 = t_src_future + MATURITY_TIME_THRESHOLD_SECS + 1.0
            called.clear()
            job.num_cache_hits = job.num_cache_misses = 0
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch("bzfs_main.bzfs.replicate_dataset", new=fake_replicate_dataset),
                patch.object(job.cache, "zfs_get_snapshots_changed", return_value={DST_DATASET: t_dst_past}),
                patch("time.time", return_value=now_i2),
            ):
                job.replicate_datasets([SRC_DATASET], task_description="skew2", max_workers=1)
            self.assertListEqual([], called)
            self.assertEqual(1, job.num_cache_hits)
            self.assertEqual(0, job.num_cache_misses)

    def test_clock_skew_monitor_when_src_dst_initiator_clocks_disagree(self) -> None:
        """Comprehensively exercises monitor behavior when src, dst, and the initiator disagree about the current time,
        modeling both sudden and creeping clock skew. We simulate a scenario where the source host's ZFS property
        snapshots_changed lies in the future relative to the initiator's wall clock, while the destination host's
        snapshots_changed lies in the past. Under --cache-snapshots, monitor attempts a fast path, trusting the local monitor
        caches (per-dataset/label "===" files) if and only if three conditions hold simultaneously:

        (1) the remote's snapshots_changed is non-zero;
        (2) it is "mature," i.e., strictly older than now - MATURITY_TIME_THRESHOLD_SECS to avoid equal-second races and skew; and
        (3) the cached tuple (creation_atime, snapshots_changed_mtime) equals the live state (specifically, mtime equals
        the current snapshots_changed and atime is not later than it).

        This test writes monitor caches for both src and dst with realistic values and then runs two phases.
        Phase 1 models a sudden skew: initiator now is before the src's snapshots_changed and after the dst's. We assert
        a cache miss for src (maturity not satisfied) and a cache hit for dst (mature and equal).
        Phase 2 models creeping catch-up: we advance initiator time beyond the maturity threshold, at which point both
        sides are eligible for cache hits. Throughout, we verify the cache hit/miss counters to ensure the monitor path
        makes correct trust vs. fallback decisions under skew without requiring any snapshot probing. This protects alert
        correctness while preserving the intended performance characteristics.

        - Scenario:
          - Source snapshots_changed is in the future relative to the initiator's clock.
          - Destination snapshots_changed is in the past relative to the initiator's clock.

        - Trust conditions with --cache-snapshots:
          - Non-zero: remote snapshots_changed is non-zero.
          - Mature: snapshots_changed < (now - MATURITY_TIME_THRESHOLD_SECS) to avoid equal-second races and skew hazards.
          - Equal tuple: cached (creation_atime, snapshots_changed_mtime) equals live:
            - mtime equals current snapshots_changed.
            - atime is not later than snapshots_changed.

        - Phases:
          - Phase 1 (sudden skew): initiator now < src snapshots_changed and > dst snapshots_changed.
            - src: miss (not mature) -> would fallback.
            - dst: hit (mature + equal).
          - Phase 2 (creeping catch-up): advance initiator beyond src maturity threshold.
            - src: now hit (mature + equal).
            - dst: still hit.

        - Verification:
          - Assert num_cache_hits/num_cache_misses across phases.
          - Probing handler is stubbed so the test is hermetic.

        - Outcome:
          - Confirms monitor preserves correctness under skew and regains performance once entries mature.
        """

        plan = {"org": {"onsite": {"daily": {"latest": {"warning": "1 hours", "critical": "2 hours"}}}}}
        with self.job_context(
            [
                "--monitor-snapshots",
                str(plan),
                "--monitor-snapshots-dont-warn",
                "--monitor-snapshots-dont-crit",
                SRC_DATASET,
                DST_DATASET,
            ]
        ) as (job, _tmpdir):
            job.params.src.root_dataset = SRC_DATASET
            job.params.dst.root_dataset = DST_DATASET

            # Times: initiator behind src (src in the "future"), ahead of dst (dst in the "past").
            now_i = 2_000_000_000
            t_src_future = now_i + 10_000
            t_dst_past = now_i - 10_000
            creation_src = now_i - 100  # any plausible creation <= snapshots_changed
            creation_dst = t_dst_past - 100  # ensure creation <= snapshots_changed for dst

            # Seed live properties and caches for equality checks
            job.src_properties[SRC_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=t_src_future)
            job.dst_properties[DST_DATASET] = DatasetProperties(recordsize=0, snapshots_changed=t_dst_past)

            alerts = job.params.monitor_snapshots_config.alerts
            self.assertGreaterEqual(len(alerts), 1)
            alert = alerts[0]
            cfg = alert.latest
            assert cfg is not None
            mon_cache_label_src = self.monitor_cache_label(cfg.kind, alert.label, alerts)
            mon_cache_label_dst = mon_cache_label_src  # same label space; different remote namespace ensures separation
            cache_file_src = SnapshotCache(job).last_modified_cache_file(job.params.src, SRC_DATASET, mon_cache_label_src)
            cache_file_dst = SnapshotCache(job).last_modified_cache_file(job.params.dst, DST_DATASET, mon_cache_label_dst)
            set_last_modification_time_safe(cache_file_src, (creation_src, t_src_future), if_more_recent=True)
            set_last_modification_time_safe(cache_file_dst, (creation_dst, t_dst_past), if_more_recent=True)

            # Phase 1: initiator behind src (future) and ahead of dst (past)
            job.num_cache_hits = job.num_cache_misses = 0

            def fake_handle_minmax_snapshots(
                self: Job,
                remote: Remote,
                datasets: list[str],
                labels: list[SnapshotLabel],
                *,
                fn_latest: Callable[[int, int, str, str], None],
                fn_oldest: Callable[[int, int, str, str], None] | None = None,
                fn_oldest_skip_holds: bool = False,
                fn_on_finish_dataset: Callable[[str], None] | None = None,
            ) -> list[str]:
                # Do not probe anything; just return empty list (no datasets without snapshots)
                return []

            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
                patch("time.time", return_value=now_i),
            ):
                job.monitor_snapshots(job.params.src, [SRC_DATASET])  # src -> miss
                job.monitor_snapshots(job.params.dst, [DST_DATASET])  # dst -> hit
            self.assertEqual(1, job.num_cache_hits)
            self.assertEqual(1, job.num_cache_misses)

            # Phase 2: creeping catch-up beyond maturity for src; both become hits
            now_i2 = t_src_future + MATURITY_TIME_THRESHOLD_SECS + 5.0
            job.num_cache_hits = job.num_cache_misses = 0
            with (
                patch("bzfs_main.bzfs.is_caching_snapshots", return_value=True),
                patch.object(Job, "handle_minmax_snapshots", new=fake_handle_minmax_snapshots),
                patch("time.time", return_value=now_i2),
            ):
                job.monitor_snapshots(job.params.src, [SRC_DATASET])  # src -> hit now that it's matured
                job.monitor_snapshots(job.params.dst, [DST_DATASET])  # dst -> still hit
            self.assertEqual(2, job.num_cache_hits)
            self.assertEqual(0, job.num_cache_misses)

    def test_replication_cache_must_not_collide_across_dst_ssh_config_file(self) -> None:
        """Replication cache ("==") must namespace by dst ssh_config_file to avoid collisions.

        Purpose: Ensure different `--ssh-dst-config-file` values yield distinct replication-scoped cache labels.
        Assumptions: Label construction includes `dst.cache_namespace()` which hashes ssh_config_file path when present.
        Design Rationale: Prevents cross-environment conflation when multiple ssh configs target same host/port.
        """

        cfg_a = "/tmp/bzfs_ssh_config_a"  # only the basename constraint matters; file need not exist
        cfg_b = "/tmp/bzfs_ssh_config_b"
        with (
            self.job_context(
                [
                    "--ssh-src-host",
                    "127.0.0.1",
                    "--ssh-dst-host",
                    "127.0.0.1",
                    "--ssh-dst-config-file",
                    cfg_a,
                    SRC_DATASET,
                    DST_DATASET,
                ]
            ) as (job_a, _tmp_a),
            self.job_context(
                [
                    "--ssh-src-host",
                    "127.0.0.1",
                    "--ssh-dst-host",
                    "127.0.0.1",
                    "--ssh-dst-config-file",
                    cfg_b,
                    SRC_DATASET,
                    DST_DATASET,
                ]
            ) as (job_b, _tmp_b),
        ):
            job_a.params.dst.ssh_user_host = "u@h"
            job_b.params.dst.ssh_user_host = "u@h"
            label_a = self.replication_cache_label(job_a, DST_DATASET)
            label_b = self.replication_cache_label(job_b, DST_DATASET)
            self.assertNotEqual(label_a, label_b)

    def test_dataset_cache_namespace_prevents_collision_across_src_ssh_config_file(self) -> None:
        """Dataset-level '=' cache paths must differ across src ssh_config_file values.

        Purpose: Ensure per-dataset '=' files are segregated by `src.cache_namespace()` including ssh_config_file hash.
        Assumptions: Remotes with same host but different ssh config must not share cache directories.
        Design Rationale: Avoids stale or incorrect cache reuse across distinct SSH configurations.
        """

        cfg_a = "/tmp/bzfs_ssh_config_src_a"
        cfg_b = "/tmp/bzfs_ssh_config_src_b"
        dataset = SRC_DATASET
        with (
            self.job_context(
                [
                    "--ssh-src-host",
                    "127.0.0.1",
                    "--ssh-src-config-file",
                    cfg_a,
                    dataset,
                    DST_DATASET,
                ]
            ) as (job_a, _tmp_a),
            self.job_context(
                [
                    "--ssh-src-host",
                    "127.0.0.1",
                    "--ssh-src-config-file",
                    cfg_b,
                    dataset,
                    DST_DATASET,
                ]
            ) as (job_b, _tmp_b),
        ):
            job_a.params.src.ssh_user_host = "u@h"
            job_b.params.src.ssh_user_host = "u@h"
            cache = SnapshotCache(job_a)
            path_a = cache.last_modified_cache_file(job_a.params.src, dataset)
            cache = SnapshotCache(job_b)
            path_b = cache.last_modified_cache_file(job_b.params.src, dataset)
            self.assertNotEqual(path_a, path_b)

    def test_monitor_cache_scoped_by_alert_plan(self) -> None:
        """Monitor cache ("===") must be scoped by alert plan hash.

        Purpose: Distinct monitor plans (e.g., cycles) should produce different cache subdirectories.
        Assumptions: monitor label uses hash(notimestamp_label); final segment hashes the tuple(alerts).
        Design Rationale: Prevents mixing state across incompatible alerting thresholds.
        """

        lbl = SnapshotLabel(prefix="bzfs_", infix="us_", timestamp="", suffix="_hourly")
        alerts_a = [
            MonitorSnapshotAlert(
                label=lbl,
                latest=AlertConfig("Latest", warning_millis=3600_000, critical_millis=7200_000),
                oldest=None,
                oldest_skip_holds=False,
            )
        ]
        alerts_b = [
            MonitorSnapshotAlert(
                label=lbl,
                latest=AlertConfig("Latest", warning_millis=2 * 3600_000, critical_millis=3 * 3600_000),
                oldest=None,
                oldest_skip_holds=False,
            )
        ]
        cache_label_a = self.monitor_cache_label("Latest", lbl, alerts_a)
        cache_label_b = self.monitor_cache_label("Latest", lbl, alerts_b)
        self.assertNotEqual(cache_label_a, cache_label_b)

    def test_scheduler_label_hash_excludes_timestamp(self) -> None:
        """Scheduler per-label cache key must ignore the timestamp field.

        Purpose: Two labels differing only in timestamp must map to the same per-label cache file name.
        Assumptions: snapshot_cache_label() digests label.notimestamp_str().
        Design Rationale: Ensures stable cache reuse across runs and avoids ephemeral paths.
        """

        base = {"prefix": "bzfs_", "infix": "us_", "suffix": "_hourly"}
        a = SnapshotLabel(**base, timestamp="2025-01-01_00-00-00")
        b = SnapshotLabel(**base, timestamp="2025-01-01_00-00-01")
        self.assertEqual(self.snapshot_cache_label(a), self.snapshot_cache_label(b))

    @unittest.skip("benchmark; enable for performance measurements")
    def test_benchmark_last_modified_cache_file(self) -> None:
        """Benchmark: Measure calls/sec for last_modified_cache_file() with large inputs."""
        # Construct fixed-size inputs based on deterministic ASCII randomness
        rng = random.Random(12345)
        letters = string.ascii_letters + string.digits
        dataset_100 = "".join(rng.choice(letters) for _ in range(100))
        prefix_10 = "".join(rng.choice(letters) for _ in range(9)) + "_"  # length 10
        infix_10 = "".join(rng.choice(letters) for _ in range(9)) + "_"  # length 10
        suffix_20 = "_" + "".join(rng.choice(letters) for _ in range(19))  # length 20; total label visible length = 40
        label_40 = self.snapshot_cache_label(
            SnapshotLabel(prefix=prefix_10, infix=infix_10, timestamp="2025-01-01_12-34:56", suffix=suffix_20)
        )

        with self.job_context([SRC_DATASET, DST_DATASET]) as (job, _tmpdir):
            cache = SnapshotCache(job)
            remote: Remote = job.params.src
            # Configure a 100-char cache namespace using a 98-char random ssh_user_host plus two separators
            remote.ssh_user_host = "".join(rng.choice(letters) for _ in range(98))
            remote.ssh_port = 22  # type: ignore[misc]  # cannot assign to final attribute
            remote.ssh_config_file_hash = ""  # type: ignore[misc]  # cannot assign to final attribute
            self.assertEqual(100 + 2, len(remote.cache_namespace()))

            # Warmup to let Python/CPU settle and populate caches
            warmup = 100
            sink: str = ""
            for _ in range(warmup):
                sink = cache.last_modified_cache_file(remote, dataset_100, label_40)

            # Measured run
            iters = 10_000
            start = time.perf_counter()
            for _ in range(iters):
                sink = cache.last_modified_cache_file(remote, dataset_100, label_40)
            elapsed = time.perf_counter() - start
            self.assertTrue(bool(sink))

            # Report throughput
            ops_per_sec = iters / elapsed if elapsed > 0 else float("inf")
            logging.getLogger(__name__).warning(
                f"last_modified_cache_file(): {iters} iters in {elapsed:.3f}s -> {ops_per_sec:.0f} ops/sec"
            )

    @unittest.skip("benchmark; enable for performance comparison")
    def test_benchmark_set_last_modification_time(self) -> None:
        funcs = {"old": set_last_modification_time_old, "flock": set_last_modification_time}
        with tempfile.TemporaryDirectory() as tmpdir:
            file = os.path.join(tmpdir, "f")
            iterations = 100_000
            for name, func in funcs.items():
                start = time.perf_counter()
                for i in range(iterations):
                    func(file, i)
                elapsed = time.perf_counter() - start
                logging.getLogger(__name__).warning(f"{name}: {elapsed}")
        self.assertEqual(iterations - 1, round(os.stat(file).st_mtime))

    def _stresstest_num_datasets(self) -> int:
        return 5 if self.is_unit_test or self.is_smoke_test else 1200


def set_last_modification_time_old(  # racy
    path: str,
    unixtime_in_secs: int | tuple[int, int],
    if_more_recent: bool = False,
) -> None:
    unixtime_in_secs = (unixtime_in_secs, unixtime_in_secs) if isinstance(unixtime_in_secs, int) else unixtime_in_secs
    if not os.path.exists(path):
        with open(path, "ab"):
            pass
    elif if_more_recent and unixtime_in_secs[1] <= round(os.stat(path).st_mtime):
        return
    os.utime(path, times=unixtime_in_secs)
