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
"""Stores snapshot metadata in fast disk inodes to avoid repeated 'zfs list' calls, without adding external dependencies or
complex databases."""

from __future__ import annotations
import os
from collections import defaultdict
from os import stat as os_stat
from os import utime as os_utime
from os.path import exists as os_path_exists
from os.path import join as os_path_join
from subprocess import CalledProcessError
from typing import TYPE_CHECKING

from bzfs_main.connection import (
    run_ssh_command,
)
from bzfs_main.detect import (
    is_caching_snapshots,
)
from bzfs_main.parallel_batch_cmd import (
    itr_ssh_cmd_parallel,
)
from bzfs_main.utils import (
    SortedInterner,
    stderr_to_str,
)

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from bzfs_main.bzfs import Job
    from bzfs_main.configuration import Remote, SnapshotLabel


class SnapshotCache:
    """Handles last-modified cache operations for snapshot management."""

    def __init__(self, job: Job) -> None:
        # immutable variables:
        self.job: Job = job

    def get_snapshots_changed(self, path: str) -> int:
        """Returns numeric timestamp from cached snapshots-changed file."""
        return self.get_snapshots_changed2(path)[1]

    @staticmethod
    def get_snapshots_changed2(path: str) -> tuple[int, int]:
        """Like zfs_get_snapshots_changed() but reads from local cache."""
        try:  # perf: inode metadata reads and writes are fast - ballpark O(200k) ops/sec.
            s = os_stat(path)
            return round(s.st_atime), round(s.st_mtime)
        except FileNotFoundError:
            return 0, 0  # harmless

    def last_modified_cache_file(self, remote: Remote, dataset: str, label: SnapshotLabel | None = None) -> str:
        """Returns the path of the cache file that is tracking last snapshot modification."""
        cache_file: str = "=" if label is None else f"{label.prefix}{label.infix}{label.suffix}"
        userhost_dir: str = remote.ssh_user_host if remote.ssh_user_host else "-"
        return os_path_join(self.job.params.log_params.last_modified_cache_dir, userhost_dir, dataset, cache_file)

    def invalidate_last_modified_cache_dataset(self, dataset: str) -> None:
        """Resets the last_modified timestamp of all cache files of the given dataset to zero."""
        p = self.job.params
        cache_file: str = self.last_modified_cache_file(p.src, dataset)
        if not p.dry_run:
            try:
                zero_times = (0, 0)
                for entry in os.scandir(os.path.dirname(cache_file)):
                    os_utime(entry.path, times=zero_times)
                os_utime(cache_file, times=zero_times)
            except FileNotFoundError:
                pass  # harmless

    def update_last_modified_cache(self, datasets_to_snapshot: dict[SnapshotLabel, list[str]]) -> None:
        """perf: copy lastmodified time of source dataset into local cache to reduce future 'zfs list -t snapshot' calls."""
        p = self.job.params
        src = p.src
        if not is_caching_snapshots(p, src):
            return
        src_datasets_set: set[str] = set()
        dataset_labels: dict[str, list[SnapshotLabel]] = defaultdict(list)
        for label, datasets in datasets_to_snapshot.items():
            src_datasets_set.update(datasets)  # union
            for dataset in datasets:
                dataset_labels[dataset].append(label)

        sorted_datasets: list[str] = sorted(src_datasets_set)
        snapshots_changed_dict: dict[str, int] = self.zfs_get_snapshots_changed(src, sorted_datasets)
        for src_dataset in sorted_datasets:
            snapshots_changed: int = snapshots_changed_dict.get(src_dataset, 0)
            self.job.src_properties[src_dataset].snapshots_changed = snapshots_changed
            if snapshots_changed == 0:
                self.invalidate_last_modified_cache_dataset(src_dataset)
            else:
                cache_file: str = self.last_modified_cache_file(src, src_dataset)
                cache_dir: str = os.path.dirname(cache_file)
                if not p.dry_run:
                    try:
                        os.makedirs(cache_dir, exist_ok=True)
                        set_last_modification_time(cache_file, unixtime_in_secs=snapshots_changed, if_more_recent=True)
                        for label in dataset_labels[src_dataset]:
                            cache_file = self.last_modified_cache_file(src, src_dataset, label)
                            set_last_modification_time(cache_file, unixtime_in_secs=snapshots_changed, if_more_recent=True)
                    except FileNotFoundError:
                        pass  # harmless

    def zfs_get_snapshots_changed(self, remote: Remote, sorted_datasets: list[str]) -> dict[str, int]:
        """Returns the ZFS dataset property "snapshots_changed", which is a UTC Unix time in integer seconds;
        See https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html#snapshots_changed"""

        def try_zfs_list_command(_cmd: list[str], batch: list[str]) -> list[str]:
            try:
                return run_ssh_command(self.job, remote, print_stderr=False, cmd=_cmd + batch).splitlines()
            except CalledProcessError as e:
                return stderr_to_str(e.stdout).splitlines()
            except UnicodeDecodeError:
                return []

        assert (not self.job.is_test_mode) or sorted_datasets == sorted(sorted_datasets), "List is not sorted"
        p = self.job.params
        cmd: list[str] = p.split_args(f"{p.zfs_program} list -t filesystem,volume -s name -Hp -o snapshots_changed,name")
        results: dict[str, int] = {}
        interner: SortedInterner[str] = SortedInterner(sorted_datasets)  # reduces memory footprint
        for lines in itr_ssh_cmd_parallel(
            self.job, remote, [(cmd, sorted_datasets)], lambda _cmd, batch: try_zfs_list_command(_cmd, batch), ordered=False
        ):
            for line in lines:
                if "\t" not in line:
                    break  # partial output from failing 'zfs list' command
                snapshots_changed, dataset = line.split("\t", 1)
                if not dataset:
                    break  # partial output from failing 'zfs list' command
                dataset = interner.interned(dataset)
                if snapshots_changed == "-" or not snapshots_changed:
                    snapshots_changed = "0"
                results[dataset] = int(snapshots_changed)
        return results


def set_last_modification_time_safe(
    path: str,
    unixtime_in_secs: int | tuple[int, int],
    if_more_recent: bool = False,
) -> None:
    """Like set_last_modification_time() but creates directories if necessary."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        set_last_modification_time(path, unixtime_in_secs=unixtime_in_secs, if_more_recent=if_more_recent)
    except FileNotFoundError:
        pass  # harmless


def set_last_modification_time(
    path: str,
    unixtime_in_secs: int | tuple[int, int],
    if_more_recent: bool = False,
) -> None:
    """if_more_recent=True is a concurrency control mechanism that prevents us from overwriting a newer (monotonically
    increasing) snapshots_changed value (which is a UTC Unix time in integer seconds) that might have been written to the
    cache file by a different, more up-to-date bzfs process."""
    unixtime_in_secs = (unixtime_in_secs, unixtime_in_secs) if isinstance(unixtime_in_secs, int) else unixtime_in_secs
    if not os_path_exists(path):
        with open(path, "ab"):
            pass
    elif if_more_recent and unixtime_in_secs[1] <= round(os_stat(path).st_mtime):
        return
    os_utime(path, times=unixtime_in_secs)
