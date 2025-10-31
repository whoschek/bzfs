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
"""Caching snapshot metadata to minimize 'zfs list -t snapshot' calls.

Purpose
=======
The ``--cache-snapshots`` mode speeds up snapshot scheduling, replication, and monitoring by storing just enough
metadata in fast local inodes (no external DB, no daemon). Instead of repeatedly invoking costly
``zfs list -t snapshot ...`` across potentially thousands or even millions of datasets, we keep tiny (i.e. empty)
per-dataset files whose inode atime/mtime atomically encode what we need to know. This reduces latency, load on ZFS, and
network chatter, while remaining dependency free and robust under crashes or concurrent runs.

Assumptions
===========
- OpenZFS >= 2.2 provides two key UTC times with integer-second resolution: ``snapshots_changed`` (dataset level)
  and snapshot ``creation`` (snapshot level).
  - ``snapshots_changed``: Specifies the UTC time at which a snapshot for a dataset was last created or deleted.
    See https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html#snapshots_changed
  - ``creation`` specifies the UTC time the snapshot was created.
    See https://openzfs.github.io/openzfs-docs/man/master/7/zfsprops.7.html#creation
- Unix atime/mtime are reliable to read and atomically updatable;
- Multiple jobs may touch the same cache files concurrently and out of order. Correctness must rely on per-file locking
  plus monotonicity guards rather than global serialization or a single writer model.
- System clocks may differ by small skews across hosts; equal-second races can happen. We gate "freshness" with a small
  maturity time threshold (``MATURITY_TIME_THRESHOLD_SECS``) before trusting a value as authoritative.

Design Rationale
================
We intentionally encode only minimal invariants into inode timestamps and not arbitrary text payloads. This keeps I/O
tiny and allows safe, atomic low-latency updates via a single ``utime`` call under an exclusive advisory file lock via
flock(2).

Cache root and hashed path segments
-----------------------------------
The cache tree lives under ``<log_parent_dir>/.cache/mods`` (see ``LogParams.last_modified_cache_dir``). To keep paths
short and safe, variable path segments are stored as URL-safe base64-encoded SHA-256 digests without padding. In what
follows, ``hash(X)`` denotes ``sha256_urlsafe_base64(str(X), padding=False)`` or a truncated variant used for brevity.

The cache consists of four families:
------------------------------------
1) Dataset-level ("=") per dataset and location (src or dst); for --create-src-snapshots, --replicate, --monitor-snapshots
   - Path: ``<cache_root>/<hash(user@host[#port])>/<hash(dataset)>/=``
   - mtime: the ZFS ``snapshots_changed`` time observed for that dataset. Monotonic writes only.
   - Used by: snapshot scheduler, replicate, monitor - as the anchor for cache equality checks.

2) Replication-scoped ("==") per source dataset and destination dataset+filters; for --replicate
   - Path: ``<cache_root>/<hash(src_user@host[#port])>/<hash(src_dataset)>/==/<hash(dst_user@host[#port])>/<hash(dst_dataset)>/<hash(filters)>``
   - Path label encodes destination namespace, destination dataset and the snapshot-filter hash.
   - mtime: last replicated source ``snapshots_changed`` for that destination and filter set. Monotonic.
   - Used by: replicate - to cheaply decide "src unchanged since last successful run to this dst+filters".

3) Monitor ("===") per dataset and label (Latest/Oldest); for --monitor-snapshots
   - Path: ``<cache_root>/<hash(user@host[#port])>/<hash(dataset)>/===/<kind>/<hash(notimestamp_label)>/<hash(alert_plan)>``
     - ``kind``: alert check mode; either "L" (Latest) or "O" (Oldest).
     - ``hash(alert_plan)``: stable digest over the monitor alert plan to scope caches per plan.
   - atime: creation time of the relevant latest/oldest snapshot.
   - mtime: dataset ``snapshots_changed`` observed when that creation was recorded. Monotonic.
   - Used by: monitor - to alert on stale snapshots without listing them every time.

4) Snapshot scheduler per-label files (under the source dataset); for --create-src-snapshots
   - Path: ``<cache_root>/<hash(src_user@host[#port])>/<hash(src_dataset)>/<hash(notimestamp_label)>``
   - atime: creation time of the latest snapshot matching that label.
   - mtime: the dataset-level "=" value at the time of the write (i.e., the then-current ``snapshots_changed``).
   - Used by: ``--create-src-snapshots`` - to cheaply decide whether a label is due without ``zfs list -t snapshot``.

How trust in a cache file is established
========================================
For a cache file to be trusted and used as a fast path, three conditions must hold:
1) Equality: the dataset-level "=" mtime must equal the live ZFS ``snapshots_changed`` of the corresponding dataset.
   This ensures the filesystem state that the cache describes is the same as the live state.
2) Maturity: that live ``snapshots_changed`` is strictly older than ``now - MATURITY_TIME_THRESHOLD_SECS`` to avoid
   equal-second races and tame small clock skew between initiator and ZFS hosts.
3) Internal consistency for per-label/monitor cache files: their mtime must equal the current dataset-level "=" value,
   and their atime must be a plausible creation time not later than mtime (atime <= mtime). A zero atime/mtime indicates
   unknown provenance and must force fallback.

If any condition fails, the code falls back to ``zfs list -t snapshot`` for just those datasets; upon completion it
rewrites the relevant cache files, monotonically.

Concurrency and correctness mechanics
=====================================
All writes go through ``set_last_modification_time_safe()`` which:
- Creates parent directories if necessary, opens the file with ``O_NOFOLLOW|O_CLOEXEC`` and takes an exclusive ``flock``.
- Updates times atomically via ``os.utime(fd, times=(atime, mtime))``.
- Applies a monotonic guard: with ``if_more_recent=True``, older timestamps never clobber newer ones. This is what
  makes concurrent runs safe and idempotent.

Cache invalidation - what and why
=================================
Two forms exist:
1) Dataset-level invalidation by directory (non-recursive): when a mismatch is detected, top-level files for the
   dataset ("=" and flat per-label files) are zeroed to force subsequent ``zfs list -t snapshot``. Monitor caches
   live in subdirectories and are refreshed by monitor runs; they are trusted only under the equality+maturity criteria
   above.
2) Selective invalidation on property unavailability: when ZFS reports ``snapshots_changed=0`` (unavailable), the
   dataset-level "=" file is reset to 0, while per-label creation caches are preserved.

What could be removed without losing correctness - and why we keep it
=====================================================================
Because all consumers already require equality + maturity before trusting cache state, explicit invalidation is not
strictly necessary for correctness; stale cache files would simply be ignored and later overwritten. However, we keep
the invalidation steps to improve operational observability and clarity:

The equality+maturity gates already prevent incorrect cache hits, but invalidation improves operational clarity.
Zeroing the top-level "=" is an explicit "do not trust" signal. All processes then deterministically skip cache and
probe via ``zfs list -t snapshot`` once, after which monotonic rewrites restore a consistent, trusted state. This
simplifies observability for operators inspecting (or debugging) cache trees, and immediately establishes a stable
cache snapshot of reality. The cost is tiny (an inode metadata write) while the benefit is more operational simplicity.

The result is a design that favors simplicity and safety: tiny inode-based atomic updates, conservative guardrails before
any cache is trusted, and minimal, well-scoped invalidation to keep the system observable under change and concurrency.
"""

from __future__ import (
    annotations,
)
import errno
import fcntl
import os
import stat
from subprocess import (
    CalledProcessError,
)
from typing import (
    TYPE_CHECKING,
    Final,
)

from bzfs_main.connection import (
    run_ssh_command,
)
from bzfs_main.parallel_batch_cmd import (
    itr_ssh_cmd_parallel,
)
from bzfs_main.utils import (
    DIR_PERMISSIONS,
    LOG_TRACE,
    SortedInterner,
    sha256_urlsafe_base64,
    stderr_to_str,
)

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from bzfs_main.bzfs import (
        Job,
    )
    from bzfs_main.configuration import (
        Remote,
        SnapshotLabel,
    )

# constants:
DATASET_CACHE_FILE_PREFIX: Final[str] = "="
REPLICATION_CACHE_FILE_PREFIX: Final[str] = "=="
MONITOR_CACHE_FILE_PREFIX: Final[str] = "==="


#############################################################################
class SnapshotCache:
    """Handles last-modified cache operations for snapshot management."""

    def __init__(self, job: Job) -> None:
        # immutable variables:
        self.job: Final[Job] = job

    def get_snapshots_changed(self, path: str) -> int:
        """Returns numeric timestamp from cached snapshots-changed file."""
        return self.get_snapshots_changed2(path)[1]

    @staticmethod
    def get_snapshots_changed2(path: str) -> tuple[int, int]:
        """Like zfs_get_snapshots_changed() but reads from local cache."""
        try:  # perf: inode metadata reads and writes are fast - ballpark O(200k) ops/sec.
            s = os.stat(path, follow_symlinks=False)
            return round(s.st_atime), round(s.st_mtime)
        except FileNotFoundError:
            return 0, 0  # harmless

    def last_modified_cache_file(self, remote: Remote, dataset: str, label: str | None = None) -> str:
        """Returns the path of the cache file that is tracking last snapshot modification."""
        cache_file: str = DATASET_CACHE_FILE_PREFIX if label is None else label
        userhost_dir: str = sha256_urlsafe_base64(remote.cache_namespace(), padding=False)
        dataset_dir: str = sha256_urlsafe_base64(dataset, padding=False)
        return os.path.join(self.job.params.log_params.last_modified_cache_dir, userhost_dir, dataset_dir, cache_file)

    def invalidate_last_modified_cache_dataset(self, dataset: str) -> None:
        """Resets the timestamps of top-level cache files of the given dataset to zero.

        Purpose: Best-effort invalidation to force ``zfs list -t snapshot`` when the dataset-level '=' cache is stale.
        Assumptions: Only top-level files (the '=' file and flat per-label files) are reset; nested monitor caches
        (e.g., '===/...') are not recursively traversed.
        Design Rationale: Monitor caches are refreshed by monitor runs and guarded by snapshots_changed equality and
        maturity checks, preserving correctness without recursive work.
        """
        p = self.job.params
        cache_file: str = self.last_modified_cache_file(p.src, dataset)
        if not p.dry_run:
            try:  # Best-effort: no locking needed. Not recursive on purpose.
                zero_times = (0, 0)
                os_utime = os.utime
                with os.scandir(os.path.dirname(cache_file)) as iterator:
                    for entry in iterator:
                        os_utime(entry.path, times=zero_times)
                os_utime(cache_file, times=zero_times)
            except FileNotFoundError:
                pass  # harmless

    def update_last_modified_cache(self, datasets_to_snapshot: dict[SnapshotLabel, list[str]]) -> None:
        """Perf: copy last-modified time of the source dataset into the local cache to reduce future 'zfs list -t snapshot' calls."""
        p = self.job.params
        src = p.src
        src_datasets_set: set[str] = set()
        for datasets in datasets_to_snapshot.values():
            src_datasets_set.update(datasets)  # union

        sorted_datasets: list[str] = sorted(src_datasets_set)
        snapshots_changed_dict: dict[str, int] = self.zfs_get_snapshots_changed(src, sorted_datasets)
        for src_dataset in sorted_datasets:
            snapshots_changed: int = snapshots_changed_dict.get(src_dataset, 0)
            self.job.src_properties[src_dataset].snapshots_changed = snapshots_changed
            dataset_cache_file: str = self.last_modified_cache_file(src, src_dataset)
            if not p.dry_run:
                if snapshots_changed == 0:
                    try:  # selective invalidation: only zero the dataset-level '=' cache file
                        os.utime(dataset_cache_file, times=(0, 0))
                    except FileNotFoundError:
                        pass  # harmless
                else:  # update dataset-level '=' cache monotonically; do NOT touch per-label creation caches here
                    set_last_modification_time_safe(
                        dataset_cache_file, unixtime_in_secs=snapshots_changed, if_more_recent=True
                    )

    def zfs_get_snapshots_changed(self, remote: Remote, sorted_datasets: list[str]) -> dict[str, int]:
        """For each given dataset, returns the ZFS dataset property "snapshots_changed", which is a UTC Unix time in integer
        seconds; See https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html#snapshots_changed"""

        def try_zfs_list_command(_cmd: list[str], batch: list[str]) -> list[str]:
            try:
                return run_ssh_command(self.job, remote, LOG_TRACE, print_stderr=False, cmd=_cmd + batch).splitlines()
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
                    break  # partial output from failing 'zfs list' command; subsequent lines in curr batch cannot be trusted
                snapshots_changed, dataset = line.split("\t", 1)
                if not dataset:
                    break  # partial output from failing 'zfs list' command; subsequent lines in curr batch cannot be trusted
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
        os.makedirs(os.path.dirname(path), mode=DIR_PERMISSIONS, exist_ok=True)
        set_last_modification_time(path, unixtime_in_secs=unixtime_in_secs, if_more_recent=if_more_recent)
    except FileNotFoundError:
        pass  # harmless


def set_last_modification_time(
    path: str,
    unixtime_in_secs: int | tuple[int, int],
    if_more_recent: bool = False,
) -> None:
    """Atomically sets the atime/mtime of the file with the given ``path``, with a monotonic guard.

    if_more_recent=True is a concurrency control mechanism that prevents us from overwriting a newer (monotonically
    increasing) snapshots_changed value (which is a UTC Unix time in integer seconds) that might have been written to the
    cache file by a different, more up-to-date bzfs process.

    For a brand-new file created by this call, we always update the file's timestamp to avoid retaining the file's implicit
    creation time ("now") instead of the intended timestamp.

    Design Rationale: Open without O_CREAT first; if missing, create exclusively (O_CREAT|O_EXCL) to detect that this call
    created the file. Only apply the monotonic early-return check when the file pre-existed; otherwise perform the initial
    timestamp write unconditionally. This preserves concurrency safety and prevents silent skips on first write.
    """
    unixtimes = (unixtime_in_secs, unixtime_in_secs) if isinstance(unixtime_in_secs, int) else unixtime_in_secs
    perm: int = stat.S_IRUSR | stat.S_IWUSR  # rw------- (user read + write)
    flags_base: int = os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC
    preexisted: bool = True

    try:
        fd = os.open(path, flags_base)
    except FileNotFoundError:
        try:
            fd = os.open(path, flags_base | os.O_CREAT | os.O_EXCL, mode=perm)
            preexisted = False
        except FileExistsError:
            fd = os.open(path, flags_base)  # we lost the race, open existing file

    try:
        # Acquire an exclusive lock; will block if lock is already held by this process or another process.
        # The (advisory) lock is auto-released when the process terminates or the fd is closed.
        fcntl.flock(fd, fcntl.LOCK_EX)

        stats = os.fstat(fd)
        st_uid: int = stats.st_uid
        if st_uid != os.geteuid():  # verify ownership is current effective UID; same as open_nofollow()
            raise PermissionError(errno.EPERM, f"{path!r} is owned by uid {st_uid}, not {os.geteuid()}", path)

        # Monotonic guard: only skip when the file pre-existed, to not skip the very first write.
        if preexisted and if_more_recent and unixtimes[1] <= round(stats.st_mtime):
            return
        os.utime(fd, times=unixtimes)  # write timestamps
    finally:
        os.close(fd)
