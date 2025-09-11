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
"""The core replication algorithm is in replicate_dataset(), which performs reliable full and/or incremental 'zfs send' and
'zfs receive' operations on snapshots, using resumable ZFS sends when possible.

For replication of multiple datasets, including recursive replication, see bzfs.py/replicate_datasets().
"""

from __future__ import annotations
import os
import re
import shlex
import subprocess
import sys
import threading
import time
from subprocess import DEVNULL, PIPE
from typing import (
    TYPE_CHECKING,
    Iterable,
)

from bzfs_main.argparse_actions import (
    has_timerange_filter,
)
from bzfs_main.connection import (
    DEDICATED,
    SHARED,
    ConnectionPool,
    maybe_inject_error,
    refresh_ssh_connection_if_necessary,
    run_ssh_command,
    timeout,
    try_ssh_command,
)
from bzfs_main.detect import (
    ZFS_VERSION_IS_AT_LEAST_2_1_0,
    ZFS_VERSION_IS_AT_LEAST_2_2_0,
    are_bookmarks_enabled,
    is_solaris_zfs,
    is_solaris_zfs_location,
    is_zpool_feature_enabled_or_active,
)
from bzfs_main.filter import (
    filter_properties,
    filter_snapshots,
)
from bzfs_main.incremental_send_steps import (
    incremental_send_steps,
)
from bzfs_main.parallel_batch_cmd import (
    run_ssh_cmd_batched,
    run_ssh_cmd_parallel,
)
from bzfs_main.parallel_iterator import (
    run_in_parallel,
)
from bzfs_main.progress_reporter import (
    PV_FILE_THREAD_SEPARATOR,
)
from bzfs_main.retry import (
    Retry,
    RetryableError,
)
from bzfs_main.utils import (
    DONT_SKIP_DATASET,
    FILE_PERMISSIONS,
    LOG_DEBUG,
    LOG_TRACE,
    append_if_absent,
    cut,
    die,
    getenv_bool,
    human_readable_bytes,
    is_descendant,
    list_formatter,
    open_nofollow,
    replace_prefix,
    stderr_to_str,
    subprocess_run,
    xprint,
)

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from bzfs_main.bzfs import Job
    from bzfs_main.configuration import Params, Remote


# constants:
INJECT_DST_PIPE_FAIL_KBYTES: int = 400  # for testing only
RIGHT_JUST: int = 7


def replicate_dataset(job: Job, src_dataset: str, tid: str, retry: Retry) -> bool:
    """Replicates src_dataset to dst_dataset (thread-safe); For replication of multiple datasets, including recursive
    replication, see bzfs.py/replicate_datasets()."""
    p, log = job.params, job.params.log
    src, dst = p.src, p.dst
    retry_count: int = retry.count
    dst_dataset: str = replace_prefix(src_dataset, old_prefix=src.root_dataset, new_prefix=dst.root_dataset)
    log.debug(p.dry(f"{tid} Replicating: %s"), f"{src_dataset} --> {dst_dataset} ...")

    list_result: bool | tuple[list[str], list[str], list[str], set[str], str, str] = _list_and_filter_src_and_dst_snapshots(
        job, src_dataset, dst_dataset
    )
    if isinstance(list_result, bool):
        return list_result
    (
        basis_src_snapshots_with_guids,
        _src_snapshots_with_guids,
        dst_snapshots_with_guids,
        included_src_guids,
        latest_src_snapshot,
        oldest_src_snapshot,
    ) = list_result
    log.debug("latest_src_snapshot: %s", latest_src_snapshot)
    latest_dst_snapshot: str = ""
    latest_common_src_snapshot: str = ""
    done_checking: bool = False

    if job.dst_dataset_exists[dst_dataset]:
        rollback_result: bool | tuple[str, str, bool] = _rollback_dst_dataset_if_necessary(
            job,
            dst_dataset,
            latest_src_snapshot,
            basis_src_snapshots_with_guids,
            dst_snapshots_with_guids,
            done_checking,
            tid,
        )
        if isinstance(rollback_result, bool):
            return rollback_result
        latest_dst_snapshot, latest_common_src_snapshot, done_checking = rollback_result

    log.debug("latest_common_src_snapshot: %s", latest_common_src_snapshot)  # is a snapshot or bookmark
    log.log(LOG_TRACE, "latest_dst_snapshot: %s", latest_dst_snapshot)
    props_cache: dict[tuple[str, str, str], dict[str, str | None]] = {}
    dry_run_no_send: bool = False
    if not latest_common_src_snapshot:
        # no common snapshot exists; delete all dst snapshots and perform a full send of the oldest selected src snapshot
        full_result: tuple[str, bool, bool, int] = _replicate_dataset_fully(
            job,
            src_dataset,
            dst_dataset,
            oldest_src_snapshot,
            latest_src_snapshot,
            latest_dst_snapshot,
            dst_snapshots_with_guids,
            props_cache,
            dry_run_no_send,
            done_checking,
            retry_count,
            tid,
        )  # we have now created a common snapshot
        latest_common_src_snapshot, dry_run_no_send, done_checking, retry_count = full_result
    if latest_common_src_snapshot:
        # finally, incrementally replicate all selected snapshots from latest common snapshot until latest src snapshot
        _replicate_dataset_incrementally(
            job,
            src_dataset,
            dst_dataset,
            latest_common_src_snapshot,
            latest_src_snapshot,
            basis_src_snapshots_with_guids,
            included_src_guids,
            props_cache,
            dry_run_no_send,
            done_checking,
            retry_count,
            tid,
        )
    return True


def _list_and_filter_src_and_dst_snapshots(
    job: Job, src_dataset: str, dst_dataset: str
) -> bool | tuple[list[str], list[str], list[str], set[str], str, str]:
    """On replication, list and filter src and dst snapshots."""
    p, log = job.params, job.params.log
    src, dst = p.src, p.dst

    # list GUID and name for dst snapshots, sorted ascending by createtxg (more precise than creation time)
    dst_cmd: list[str] = p.split_args(f"{p.zfs_program} list -t snapshot -d 1 -s createtxg -Hp -o guid,name", dst_dataset)

    # list GUID and name for src snapshots + bookmarks, primarily sort ascending by transaction group (which is more
    # precise than creation time), secondarily sort such that snapshots appear after bookmarks for the same GUID.
    # Note: A snapshot and its ZFS bookmarks always have the same GUID, creation time and transaction group. A snapshot
    # changes its transaction group but retains its creation time and GUID on 'zfs receive' on another pool, i.e.
    # comparing createtxg is only meaningful within a single pool, not across pools from src to dst. Comparing creation
    # time remains meaningful across pools from src to dst. Creation time is a UTC Unix time in integer seconds.
    # Note that 'zfs create', 'zfs snapshot' and 'zfs bookmark' CLIs enforce that snapshot names must not contain a '#'
    # char, bookmark names must not contain a '@' char, and dataset names must not contain a '#' or '@' char.
    # GUID and creation time also do not contain a '#' or '@' char.
    filter_needs_creation_time: bool = has_timerange_filter(p.snapshot_filters)
    types: str = "snapshot,bookmark" if p.use_bookmark and are_bookmarks_enabled(p, src) else "snapshot"
    props: str = job.creation_prefix + "creation,guid,name" if filter_needs_creation_time else "guid,name"
    src_cmd = p.split_args(f"{p.zfs_program} list -t {types} -s createtxg -s type -d 1 -Hp -o {props}", src_dataset)
    job.maybe_inject_delete(src, dataset=src_dataset, delete_trigger="zfs_list_snapshot_src")
    src_snapshots_and_bookmarks, dst_snapshots_with_guids_str = run_in_parallel(  # list src+dst snapshots in parallel
        lambda: try_ssh_command(job, src, LOG_TRACE, cmd=src_cmd),
        lambda: try_ssh_command(job, dst, LOG_TRACE, cmd=dst_cmd, error_trigger="zfs_list_snapshot_dst"),
    )
    job.dst_dataset_exists[dst_dataset] = dst_snapshots_with_guids_str is not None
    dst_snapshots_with_guids: list[str] = (dst_snapshots_with_guids_str or "").splitlines()
    if src_snapshots_and_bookmarks is None:
        log.warning("Third party deleted source: %s", src_dataset)
        return False  # src dataset has been deleted by some third party while we're running - nothing to do anymore
    src_snapshots_with_guids: list[str] = src_snapshots_and_bookmarks.splitlines()
    src_snapshots_and_bookmarks = None
    if len(dst_snapshots_with_guids) == 0 and "bookmark" in types:
        # src bookmarks serve no purpose if the destination dataset has no snapshot; ignore them
        src_snapshots_with_guids = [snapshot for snapshot in src_snapshots_with_guids if "@" in snapshot]
    num_src_snapshots_found: int = sum(1 for snapshot in src_snapshots_with_guids if "@" in snapshot)
    with job.stats_lock:
        job.num_snapshots_found += num_src_snapshots_found
    # apply include/exclude regexes to ignore irrelevant src snapshots
    basis_src_snapshots_with_guids: list[str] = src_snapshots_with_guids
    src_snapshots_with_guids = filter_snapshots(job, src_snapshots_with_guids)
    if filter_needs_creation_time:
        src_snapshots_with_guids = cut(field=2, lines=src_snapshots_with_guids)
        basis_src_snapshots_with_guids = cut(field=2, lines=basis_src_snapshots_with_guids)

    # find oldest and latest "true" snapshot, as well as GUIDs of all snapshots and bookmarks.
    # a snapshot is "true" if it is not a bookmark.
    oldest_src_snapshot: str = ""
    latest_src_snapshot: str = ""
    included_src_guids: set[str] = set()
    for line in src_snapshots_with_guids:
        guid, snapshot = line.split("\t", 1)
        if "@" in snapshot:
            included_src_guids.add(guid)
            latest_src_snapshot = snapshot
            if not oldest_src_snapshot:
                oldest_src_snapshot = snapshot
    if len(src_snapshots_with_guids) == 0:
        if p.skip_missing_snapshots == "fail":
            die(f"Source dataset includes no snapshot: {src_dataset}. Consider using --skip-missing-snapshots=dataset")
        elif p.skip_missing_snapshots == "dataset":
            log.warning("Skipping source dataset because it includes no snapshot: %s", src_dataset)
            if p.recursive and not job.dst_dataset_exists[dst_dataset]:
                log.warning("Also skipping descendant datasets as dst dataset does not exist for %s", src_dataset)
            return job.dst_dataset_exists[dst_dataset]
    return (
        basis_src_snapshots_with_guids,
        src_snapshots_with_guids,
        dst_snapshots_with_guids,
        included_src_guids,
        latest_src_snapshot,
        oldest_src_snapshot,
    )


def _rollback_dst_dataset_if_necessary(
    job: Job,
    dst_dataset: str,
    latest_src_snapshot: str,
    src_snapshots_with_guids: list[str],
    dst_snapshots_with_guids: list[str],
    done_checking: bool,
    tid: str,
) -> bool | tuple[str, str, bool]:
    """On replication, rollback dst if necessary."""
    p, log = job.params, job.params.log
    dst = p.dst
    latest_dst_snapshot: str = ""
    latest_dst_guid: str = ""
    if len(dst_snapshots_with_guids) > 0:
        latest_dst_guid, latest_dst_snapshot = dst_snapshots_with_guids[-1].split("\t", 1)
        if p.force_rollback_to_latest_snapshot:
            log.info(p.dry(f"{tid} Rolling back destination to most recent snapshot: %s"), latest_dst_snapshot)
            # rollback just in case the dst dataset was modified since its most recent snapshot
            done_checking = done_checking or _check_zfs_dataset_busy(job, dst, dst_dataset)
            cmd: list[str] = p.split_args(f"{dst.sudo} {p.zfs_program} rollback", latest_dst_snapshot)
            try_ssh_command(job, dst, LOG_DEBUG, is_dry=p.dry_run, print_stdout=True, cmd=cmd, exists=False)
    elif latest_src_snapshot == "":
        log.info(f"{tid} Already-up-to-date: %s", dst_dataset)
        return True

    # find most recent snapshot (or bookmark) that src and dst have in common - we'll start to replicate
    # from there up to the most recent src snapshot. any two snapshots are "common" iff their ZFS GUIDs (i.e.
    # contents) are equal. See https://github.com/openzfs/zfs/commit/305bc4b370b20de81eaf10a1cf724374258b74d1
    def latest_common_snapshot(snapshots_with_guids: list[str], intersect_guids: set[str]) -> tuple[str | None, str]:
        """Returns a true snapshot instead of its bookmark with the same GUID, per the sort order previously used for 'zfs
        list -s ...'."""
        for _line in reversed(snapshots_with_guids):
            guid_, snapshot_ = _line.split("\t", 1)
            if guid_ in intersect_guids:
                return guid_, snapshot_  # can be a snapshot or bookmark
        return None, ""

    latest_common_guid, latest_common_src_snapshot = latest_common_snapshot(
        src_snapshots_with_guids, set(cut(field=1, lines=dst_snapshots_with_guids))
    )
    log.debug("latest_common_src_snapshot: %s", latest_common_src_snapshot)  # is a snapshot or bookmark
    log.log(LOG_TRACE, "latest_dst_snapshot: %s", latest_dst_snapshot)

    if latest_common_src_snapshot and latest_common_guid != latest_dst_guid:
        # found latest common snapshot but dst has an even newer snapshot. rollback dst to that common snapshot.
        assert latest_common_guid
        _, latest_common_dst_snapshot = latest_common_snapshot(dst_snapshots_with_guids, {latest_common_guid})
        if not (p.force_rollback_to_latest_common_snapshot or p.force):
            die(
                f"Conflict: Most recent destination snapshot {latest_dst_snapshot} is more recent than "
                f"most recent common snapshot {latest_common_dst_snapshot}. Rollback destination first, "
                "for example via --force-rollback-to-latest-common-snapshot (or --force) option."
            )
        if p.force_once:
            p.force.value = False
            p.force_rollback_to_latest_common_snapshot.value = False
        log.info(p.dry(f"{tid} Rolling back destination to most recent common snapshot: %s"), latest_common_dst_snapshot)
        done_checking = done_checking or _check_zfs_dataset_busy(job, dst, dst_dataset)
        cmd = p.split_args(
            f"{dst.sudo} {p.zfs_program} rollback -r {p.force_unmount} {p.force_hard}", latest_common_dst_snapshot
        )
        try:
            run_ssh_command(job, dst, LOG_DEBUG, is_dry=p.dry_run, print_stdout=True, cmd=cmd)
        except (subprocess.CalledProcessError, UnicodeDecodeError) as e:
            stderr: str = stderr_to_str(e.stderr) if hasattr(e, "stderr") else ""
            no_sleep: bool = _clear_resumable_recv_state_if_necessary(job, dst_dataset, stderr)
            # op isn't idempotent so retries regather current state from the start of replicate_dataset()
            raise RetryableError("Subprocess failed", no_sleep=no_sleep) from e

    if latest_src_snapshot and latest_src_snapshot == latest_common_src_snapshot:
        log.info(f"{tid} Already up-to-date: %s", dst_dataset)
        return True
    return latest_dst_snapshot, latest_common_src_snapshot, done_checking


def _replicate_dataset_fully(
    job: Job,
    src_dataset: str,
    dst_dataset: str,
    oldest_src_snapshot: str,
    latest_src_snapshot: str,
    latest_dst_snapshot: str,
    dst_snapshots_with_guids: list[str],
    props_cache: dict[tuple[str, str, str], dict[str, str | None]],
    dry_run_no_send: bool,
    done_checking: bool,
    retry_count: int,
    tid: str,
) -> tuple[str, bool, bool, int]:
    """On replication, deletes all dst snapshots and performs a full send of the oldest selected src snapshot, which in turn
    creates a common snapshot."""
    p, log = job.params, job.params.log
    src, dst = p.src, p.dst
    latest_common_src_snapshot: str = ""
    if latest_dst_snapshot:
        if not p.force:
            die(
                f"Conflict: No common snapshot found between {src_dataset} and {dst_dataset} even though "
                "destination has at least one snapshot. Aborting. Consider using --force option to first "
                "delete all existing destination snapshots in order to be able to proceed with replication."
            )
        if p.force_once:
            p.force.value = False
        done_checking = done_checking or _check_zfs_dataset_busy(job, dst, dst_dataset)
        delete_snapshots(job, dst, dst_dataset, snapshot_tags=cut(2, separator="@", lines=dst_snapshots_with_guids))
        if p.dry_run:
            # As we're in --dryrun (--force) mode this conflict resolution step (see above) wasn't really executed:
            # "no common snapshot was found. delete all dst snapshots". In turn, this would cause the subsequent
            # 'zfs receive -n' to fail with "cannot receive new filesystem stream: destination has snapshots; must
            # destroy them to overwrite it". So we skip the zfs send/receive step and keep on trucking.
            dry_run_no_send = True

    # to start with, fully replicate oldest snapshot, which in turn creates a common snapshot
    if p.no_stream:
        oldest_src_snapshot = latest_src_snapshot
    if oldest_src_snapshot:
        if not job.dst_dataset_exists[dst_dataset]:
            # on destination, create parent filesystem and ancestors if they do not yet exist
            dst_dataset_parent: str = os.path.dirname(dst_dataset)
            if not job.dst_dataset_exists[dst_dataset_parent]:
                if p.dry_run:
                    dry_run_no_send = True
                if dst_dataset_parent != "":
                    _create_zfs_filesystem(job, dst_dataset_parent)

        recv_resume_token_result: tuple[str | None, list[str], list[str]] = _recv_resume_token(job, dst_dataset, retry_count)
        recv_resume_token, send_resume_opts, recv_resume_opts = recv_resume_token_result
        curr_size: int = _estimate_send_size(job, src, dst_dataset, recv_resume_token, oldest_src_snapshot)
        humansize: str = _format_size(curr_size)
        if recv_resume_token:
            send_opts: list[str] = send_resume_opts  # e.g. ["-t", "1-c740b4779-..."]
        else:
            send_opts = p.curr_zfs_send_program_opts + [oldest_src_snapshot]
        send_cmd: list[str] = p.split_args(f"{src.sudo} {p.zfs_program} send", send_opts)
        recv_opts: list[str] = p.zfs_full_recv_opts.copy() + recv_resume_opts
        recv_opts, set_opts = _add_recv_property_options(job, True, recv_opts, src_dataset, props_cache)
        recv_cmd: list[str] = p.split_args(
            f"{dst.sudo} {p.zfs_program} receive -F", p.dry_run_recv, recv_opts, dst_dataset, allow_all=True
        )
        log.info(p.dry(f"{tid} Full send: %s"), f"{oldest_src_snapshot} --> {dst_dataset} ({humansize.strip()}) ...")
        done_checking = done_checking or _check_zfs_dataset_busy(job, dst, dst_dataset)
        dry_run_no_send = dry_run_no_send or p.dry_run_no_send
        job.maybe_inject_params(error_trigger="full_zfs_send_params")
        humansize = humansize.rjust(RIGHT_JUST * 3 + 2)
        _run_zfs_send_receive(
            job, src_dataset, dst_dataset, send_cmd, recv_cmd, curr_size, humansize, dry_run_no_send, "full_zfs_send"
        )
        latest_common_src_snapshot = oldest_src_snapshot  # we have now created a common snapshot
        if not dry_run_no_send and not p.dry_run:
            job.dst_dataset_exists[dst_dataset] = True
        with job.stats_lock:
            job.num_snapshots_replicated += 1
        _create_zfs_bookmarks(job, src, src_dataset, [oldest_src_snapshot])
        _zfs_set(job, set_opts, dst, dst_dataset)
        dry_run_no_send = dry_run_no_send or p.dry_run
        retry_count = 0

    return latest_common_src_snapshot, dry_run_no_send, done_checking, retry_count


def _replicate_dataset_incrementally(
    job: Job,
    src_dataset: str,
    dst_dataset: str,
    latest_common_src_snapshot: str,
    latest_src_snapshot: str,
    basis_src_snapshots_with_guids: list[str],
    included_src_guids: set[str],
    props_cache: dict[tuple[str, str, str], dict[str, str | None]],
    dry_run_no_send: bool,
    done_checking: bool,
    retry_count: int,
    tid: str,
) -> None:
    """Incrementally replicates all selected snapshots from latest common snapshot until latest src snapshot."""
    p, log = job.params, job.params.log
    src, dst = p.src, p.dst

    def replication_candidates() -> tuple[list[str], list[str]]:
        assert len(basis_src_snapshots_with_guids) > 0
        result_snapshots: list[str] = []
        result_guids: list[str] = []
        last_appended_guid: str = ""
        snapshot_itr = reversed(basis_src_snapshots_with_guids)
        while True:
            guid, snapshot = snapshot_itr.__next__().split("\t", 1)
            if "@" in snapshot:
                result_snapshots.append(snapshot)
                result_guids.append(guid)
                last_appended_guid = guid
            if snapshot == latest_common_src_snapshot:  # latest_common_src_snapshot is a snapshot or bookmark
                if guid != last_appended_guid and "@" not in snapshot:
                    # only appends the src bookmark if it has no snapshot. If the bookmark has a snapshot then that
                    # snapshot has already been appended, per the sort order previously used for 'zfs list -s ...'
                    result_snapshots.append(snapshot)
                    result_guids.append(guid)
                break
        result_snapshots.reverse()
        result_guids.reverse()
        assert len(result_snapshots) > 0
        assert len(result_snapshots) == len(result_guids)
        return result_guids, result_snapshots

    # collect the most recent common snapshot (which may be a bookmark) followed by all src snapshots
    # (that are not a bookmark) that are more recent than that.
    cand_guids, cand_snapshots = replication_candidates()
    if len(cand_snapshots) == 1:
        # latest_src_snapshot is a (true) snapshot that is equal to latest_common_src_snapshot or LESS recent
        # than latest_common_src_snapshot. The latter case can happen if latest_common_src_snapshot is a
        # bookmark whose snapshot has been deleted on src.
        return  # nothing more tbd

    recv_resume_token_result: tuple[str | None, list[str], list[str]] = _recv_resume_token(job, dst_dataset, retry_count)
    recv_resume_token, send_resume_opts, recv_resume_opts = recv_resume_token_result
    recv_opts: list[str] = p.zfs_recv_program_opts.copy() + recv_resume_opts
    recv_opts, set_opts = _add_recv_property_options(job, False, recv_opts, src_dataset, props_cache)
    if p.no_stream:
        # skip intermediate snapshots
        steps_todo: list[tuple[str, str, str, list[str]]] = [
            ("-i", latest_common_src_snapshot, latest_src_snapshot, [latest_src_snapshot])
        ]
    else:
        # include intermediate src snapshots that pass --{include,exclude}-snapshot-* policy, using
        # a series of -i/-I send/receive steps that skip excluded src snapshots.
        steps_todo = _incremental_send_steps_wrapper(
            p, cand_snapshots, cand_guids, included_src_guids, recv_resume_token is not None
        )
    log.log(LOG_TRACE, "steps_todo: %s", list_formatter(steps_todo, "; "))
    estimate_send_sizes: list[int] = [
        _estimate_send_size(job, src, dst_dataset, recv_resume_token if i == 0 else None, incr_flag, from_snap, to_snap)
        for i, (incr_flag, from_snap, to_snap, to_snapshots) in enumerate(steps_todo)
    ]
    total_size: int = sum(estimate_send_sizes)
    total_num: int = sum(len(to_snapshots) for incr_flag, from_snap, to_snap, to_snapshots in steps_todo)
    done_size: int = 0
    done_num: int = 0
    for i, (incr_flag, from_snap, to_snap, to_snapshots) in enumerate(steps_todo):
        curr_num_snapshots: int = len(to_snapshots)
        curr_size: int = estimate_send_sizes[i]
        humansize: str = _format_size(total_size) + "/" + _format_size(done_size) + "/" + _format_size(curr_size)
        human_num: str = f"{total_num}/{done_num}/{curr_num_snapshots} snapshots"
        if recv_resume_token:
            send_opts: list[str] = send_resume_opts  # e.g. ["-t", "1-c740b4779-..."]
        else:
            send_opts = p.curr_zfs_send_program_opts + [incr_flag, from_snap, to_snap]
        send_cmd: list[str] = p.split_args(f"{src.sudo} {p.zfs_program} send", send_opts)
        recv_cmd: list[str] = p.split_args(
            f"{dst.sudo} {p.zfs_program} receive", p.dry_run_recv, recv_opts, dst_dataset, allow_all=True
        )
        dense_size: str = p.two_or_more_spaces_regex.sub("", humansize.strip())
        log.info(
            p.dry(f"{tid} Incremental send {incr_flag}: %s"),
            f"{from_snap} .. {to_snap[to_snap.index('@'):]} --> {dst_dataset} ({dense_size}) ({human_num}) ...",
        )
        done_checking = done_checking or _check_zfs_dataset_busy(job, dst, dst_dataset, busy_if_send=False)
        if p.dry_run and not job.dst_dataset_exists[dst_dataset]:
            dry_run_no_send = True
        dry_run_no_send = dry_run_no_send or p.dry_run_no_send
        job.maybe_inject_params(error_trigger="incr_zfs_send_params")
        _run_zfs_send_receive(
            job, src_dataset, dst_dataset, send_cmd, recv_cmd, curr_size, humansize, dry_run_no_send, "incr_zfs_send"
        )
        done_size += curr_size
        done_num += curr_num_snapshots
        recv_resume_token = None
        with job.stats_lock:
            job.num_snapshots_replicated += curr_num_snapshots
        assert p.create_bookmarks
        if p.create_bookmarks == "all":
            _create_zfs_bookmarks(job, src, src_dataset, to_snapshots)
        elif p.create_bookmarks != "none":
            threshold_millis: int = p.xperiods.label_milliseconds("_" + p.create_bookmarks)
            to_snapshots = [snap for snap in to_snapshots if p.xperiods.label_milliseconds(snap) >= threshold_millis]
            if i == len(steps_todo) - 1 and (len(to_snapshots) == 0 or to_snapshots[-1] != to_snap):
                to_snapshots.append(to_snap)
            _create_zfs_bookmarks(job, src, src_dataset, to_snapshots)
    _zfs_set(job, set_opts, dst, dst_dataset)


def _format_size(num_bytes: int) -> str:
    """Formats a byte count for human-readable logs."""
    return human_readable_bytes(num_bytes, separator="").rjust(RIGHT_JUST)


def _prepare_zfs_send_receive(
    job: Job, src_dataset: str, send_cmd: list[str], recv_cmd: list[str], size_estimate_bytes: int, size_estimate_human: str
) -> tuple[str, str, str]:
    """Constructs zfs send/recv pipelines with optional compression and pv."""
    p = job.params
    send_cmd_str: str = shlex.join(send_cmd)
    recv_cmd_str: str = shlex.join(recv_cmd)

    if p.is_program_available("zstd", "src") and p.is_program_available("zstd", "dst"):
        compress_cmd_: str = _compress_cmd(p, "src", size_estimate_bytes)
        decompress_cmd_: str = _decompress_cmd(p, "dst", size_estimate_bytes)
    else:  # no compression is used if source and destination do not both support compression
        compress_cmd_, decompress_cmd_ = "cat", "cat"

    recordsize: int = abs(job.src_properties[src_dataset].recordsize)
    src_buffer: str = _mbuffer_cmd(p, "src", size_estimate_bytes, recordsize)
    dst_buffer: str = _mbuffer_cmd(p, "dst", size_estimate_bytes, recordsize)
    local_buffer: str = _mbuffer_cmd(p, "local", size_estimate_bytes, recordsize)

    pv_src_cmd: str = ""
    pv_dst_cmd: str = ""
    pv_loc_cmd: str = ""
    if p.src.ssh_user_host == "":
        pv_src_cmd = _pv_cmd(job, "local", size_estimate_bytes, size_estimate_human)
    elif p.dst.ssh_user_host == "":
        pv_dst_cmd = _pv_cmd(job, "local", size_estimate_bytes, size_estimate_human)
    elif compress_cmd_ == "cat":
        pv_loc_cmd = _pv_cmd(job, "local", size_estimate_bytes, size_estimate_human)  # compression disabled
    else:
        # pull-push mode with compression enabled: reporting "percent complete" isn't straightforward because
        # localhost observes the compressed data instead of the uncompressed data, so we disable the progress bar.
        pv_loc_cmd = _pv_cmd(job, "local", size_estimate_bytes, size_estimate_human, disable_progress_bar=True)

    # assemble pipeline running on source leg
    src_pipe: str = ""
    if job.inject_params.get("inject_src_pipe_fail", False):
        # for testing; initially forward some bytes and then fail
        src_pipe = f"{src_pipe} | dd bs=64 count=1 2>/dev/null && false"
    if job.inject_params.get("inject_src_pipe_garble", False):
        src_pipe = f"{src_pipe} | base64"  # for testing; forward garbled bytes
    if pv_src_cmd != "" and pv_src_cmd != "cat":
        src_pipe = f"{src_pipe} | {pv_src_cmd}"
    if compress_cmd_ != "cat":
        src_pipe = f"{src_pipe} | {compress_cmd_}"
    if src_buffer != "cat":
        src_pipe = f"{src_pipe} | {src_buffer}"
    if src_pipe.startswith(" |"):
        src_pipe = src_pipe[2:]  # strip leading ' |' part
    if job.inject_params.get("inject_src_send_error", False):
        send_cmd_str = f"{send_cmd_str} --injectedGarbageParameter"  # for testing; induce CLI parse error
    if src_pipe != "":
        src_pipe = f"{send_cmd_str} | {src_pipe}"
        if p.src.ssh_user_host != "":
            src_pipe = p.shell_program + " -c " + _dquote(src_pipe)
    else:
        src_pipe = send_cmd_str

    # assemble pipeline running on middle leg between source and destination. only enabled for pull-push mode
    local_pipe: str = ""
    if local_buffer != "cat":
        local_pipe = f"{local_buffer}"
    if pv_loc_cmd != "" and pv_loc_cmd != "cat":
        local_pipe = f"{local_pipe} | {pv_loc_cmd}"
    if local_buffer != "cat":
        local_pipe = f"{local_pipe} | {local_buffer}"
    if local_pipe.startswith(" |"):
        local_pipe = local_pipe[2:]  # strip leading ' |' part
    if local_pipe != "":
        local_pipe = f"| {local_pipe}"

    # assemble pipeline running on destination leg
    dst_pipe: str = ""
    if dst_buffer != "cat":
        dst_pipe = f"{dst_buffer}"
    if decompress_cmd_ != "cat":
        dst_pipe = f"{dst_pipe} | {decompress_cmd_}"
    if pv_dst_cmd != "" and pv_dst_cmd != "cat":
        dst_pipe = f"{dst_pipe} | {pv_dst_cmd}"
    if job.inject_params.get("inject_dst_pipe_fail", False):
        # interrupt zfs receive for testing retry/resume; initially forward some bytes and then stop forwarding
        dst_pipe = f"{dst_pipe} | dd bs=1024 count={INJECT_DST_PIPE_FAIL_KBYTES} 2>/dev/null"
    if job.inject_params.get("inject_dst_pipe_garble", False):
        dst_pipe = f"{dst_pipe} | base64"  # for testing; forward garbled bytes
    if dst_pipe.startswith(" |"):
        dst_pipe = dst_pipe[2:]  # strip leading ' |' part
    if job.inject_params.get("inject_dst_receive_error", False):
        recv_cmd_str = f"{recv_cmd_str} --injectedGarbageParameter"  # for testing; induce CLI parse error
    if dst_pipe != "":
        dst_pipe = f"{dst_pipe} | {recv_cmd_str}"
        if p.dst.ssh_user_host != "":
            dst_pipe = p.shell_program + " -c " + _dquote(dst_pipe)
    else:
        dst_pipe = recv_cmd_str

    # If there's no support for shell pipelines, we can't do compression, mbuffering, monitoring and rate-limiting,
    # so we fall back to simple zfs send/receive.
    if not p.is_program_available("sh", "src"):
        src_pipe = send_cmd_str
    if not p.is_program_available("sh", "dst"):
        dst_pipe = recv_cmd_str
    if not p.is_program_available("sh", "local"):
        local_pipe = ""

    src_pipe = _squote(p.src, src_pipe)
    dst_pipe = _squote(p.dst, dst_pipe)
    return src_pipe, local_pipe, dst_pipe


def _run_zfs_send_receive(
    job: Job,
    src_dataset: str,
    dst_dataset: str,
    send_cmd: list[str],
    recv_cmd: list[str],
    size_estimate_bytes: int,
    size_estimate_human: str,
    dry_run_no_send: bool,
    error_trigger: str | None = None,
) -> None:
    """Executes a zfs send/receive pipeline between source and destination."""
    p, log = job.params, job.params.log
    pipes: tuple[str, str, str] = _prepare_zfs_send_receive(
        job, src_dataset, send_cmd, recv_cmd, size_estimate_bytes, size_estimate_human
    )
    src_pipe, local_pipe, dst_pipe = pipes
    conn_pool_name: str = DEDICATED if job.dedicated_tcp_connection_per_zfs_send else SHARED
    src_conn_pool: ConnectionPool = p.connection_pools["src"].pool(conn_pool_name)
    dst_conn_pool: ConnectionPool = p.connection_pools["dst"].pool(conn_pool_name)
    with src_conn_pool.connection() as src_conn, dst_conn_pool.connection() as dst_conn:
        refresh_ssh_connection_if_necessary(job, p.src, src_conn)
        refresh_ssh_connection_if_necessary(job, p.dst, dst_conn)
        src_ssh_cmd: str = " ".join(src_conn.ssh_cmd_quoted)
        dst_ssh_cmd: str = " ".join(dst_conn.ssh_cmd_quoted)
        cmd: list[str] = [p.shell_program_local, "-c", f"{src_ssh_cmd} {src_pipe} {local_pipe} | {dst_ssh_cmd} {dst_pipe}"]
        msg: str = "Would execute: %s" if dry_run_no_send else "Executing: %s"
        log.debug(msg, cmd[2].lstrip())
        if not dry_run_no_send:
            try:
                maybe_inject_error(job, cmd=cmd, error_trigger=error_trigger)
                process = subprocess_run(
                    cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True, timeout=timeout(job), check=True
                )
            except (subprocess.CalledProcessError, UnicodeDecodeError) as e:
                no_sleep: bool = False
                if not isinstance(e, UnicodeDecodeError):
                    xprint(log, stderr_to_str(e.stdout), file=sys.stdout)
                    log.warning("%s", stderr_to_str(e.stderr).rstrip())
                if isinstance(e, subprocess.CalledProcessError):
                    no_sleep = _clear_resumable_recv_state_if_necessary(job, dst_dataset, e.stderr)
                # op isn't idempotent so retries regather current state from the start of replicate_dataset()
                raise RetryableError("Subprocess failed", no_sleep=no_sleep) from e
            else:
                xprint(log, process.stdout, file=sys.stdout)
                xprint(log, process.stderr, file=sys.stderr)


def _clear_resumable_recv_state_if_necessary(job: Job, dst_dataset: str, stderr: str) -> bool:
    """Deletes leftover state when resume tokens fail to apply."""

    def clear_resumable_recv_state() -> bool:
        log.warning(p.dry("Aborting an interrupted zfs receive -s, deleting partially received state: %s"), dst_dataset)
        cmd: list[str] = p.split_args(f"{p.dst.sudo} {p.zfs_program} receive -A", dst_dataset)
        try_ssh_command(job, p.dst, LOG_TRACE, is_dry=p.dry_run, print_stdout=True, cmd=cmd)
        log.log(LOG_TRACE, p.dry("Done Aborting an interrupted zfs receive -s: %s"), dst_dataset)
        return True

    p, log = job.params, job.params.log
    # "cannot resume send: 'wb_src/tmp/src@s1' is no longer the same snapshot used in the initial send"
    # "cannot resume send: 'wb_src/tmp/src@s1' used in the initial send no longer exists"
    # "cannot resume send: incremental source 0xa000000000000000 no longer exists"
    if "cannot resume send" in stderr and (
        "is no longer the same snapshot used in the initial send" in stderr
        or "used in the initial send no longer exists" in stderr
        or re.match(r".*incremental source [0-9a-fx]+ no longer exists", stderr)
    ):
        return clear_resumable_recv_state()

    # "cannot receive resume stream: incompatible embedded data stream feature with encrypted receive."
    #     see https://github.com/openzfs/zfs/issues/12480
    # 'cannot receive new filesystem stream: destination xx contains partially-complete state from "zfs receive -s"'
    #     this indicates that --no-resume-recv detects that dst contains a previously interrupted recv -s
    elif "cannot receive" in stderr and (
        "cannot receive resume stream: incompatible embedded data stream feature with encrypted receive" in stderr
        or 'contains partially-complete state from "zfs receive -s"' in stderr
    ):
        return clear_resumable_recv_state()

    elif (  # this signals normal behavior on interrupt of 'zfs receive -s' if running without --no-resume-recv
        "cannot receive new filesystem stream: checksum mismatch or incomplete stream" in stderr
        and "Partially received snapshot is saved" in stderr
    ):
        return True

    # "cannot destroy 'wb_dest/tmp/dst@s1': snapshot has dependent clones ... use '-R' to destroy the following
    # datasets: wb_dest/tmp/dst/%recv" # see https://github.com/openzfs/zfs/issues/10439#issuecomment-642774560
    # This msg indicates a failed 'zfs destroy' via --delete-dst-snapshots. This "clone" is caused by a previously
    # interrupted 'zfs receive -s'. The fix used here is to delete the partially received state of said
    # 'zfs receive -s' via 'zfs receive -A', followed by an automatic retry, which will now succeed to delete the
    # snapshot without user intervention.
    elif (
        "cannot destroy" in stderr
        and "snapshot has dependent clone" in stderr
        and "use '-R' to destroy the following dataset" in stderr
        and f"\n{dst_dataset}/%recv\n" in stderr
    ):
        return clear_resumable_recv_state()

    # Same cause as above, except that this error can occur during 'zfs rollback'
    # Also see https://github.com/openzfs/zfs/blob/master/cmd/zfs/zfs_main.c
    elif (
        "cannot rollback to" in stderr
        and "clones of previous snapshots exist" in stderr
        and "use '-R' to force deletion of the following clones and dependents" in stderr
        and f"\n{dst_dataset}/%recv\n" in stderr
    ):
        return clear_resumable_recv_state()

    return False


def _recv_resume_token(job: Job, dst_dataset: str, retry_count: int) -> tuple[str | None, list[str], list[str]]:
    """Gets recv_resume_token ZFS property from dst_dataset and returns corresponding opts to use for send+recv."""
    p, log = job.params, job.params.log
    if not p.resume_recv:
        return None, [], []
    warning: str | None = None
    if not is_zpool_feature_enabled_or_active(p, p.dst, "feature@extensible_dataset"):
        warning = "not available on destination dataset"
    elif not p.is_program_available(ZFS_VERSION_IS_AT_LEAST_2_1_0, "dst"):
        warning = "unreliable as zfs version is too old"  # e.g. zfs-0.8.3 "internal error: Unknown error 1040"
    if warning:
        log.warning(f"ZFS receive resume feature is {warning}. Falling back to --no-resume-recv: %s", dst_dataset)
        return None, [], []
    recv_resume_token: str | None = None
    send_resume_opts: list[str] = []
    if job.dst_dataset_exists[dst_dataset]:
        cmd: list[str] = p.split_args(f"{p.zfs_program} get -Hp -o value -s none receive_resume_token", dst_dataset)
        recv_resume_token = run_ssh_command(job, p.dst, LOG_TRACE, cmd=cmd).rstrip()
        if recv_resume_token == "-" or not recv_resume_token:  # noqa: S105
            recv_resume_token = None
        else:
            send_resume_opts += ["-n"] if p.dry_run else []
            send_resume_opts += ["-v"] if p.verbose_zfs else []
            send_resume_opts += ["-t", recv_resume_token]
    recv_resume_opts = ["-s"]
    return recv_resume_token, send_resume_opts, recv_resume_opts


def _mbuffer_cmd(p: Params, loc: str, size_estimate_bytes: int, recordsize: int) -> str:
    """If mbuffer command is on the PATH, uses it in the ssh network pipe between 'zfs send' and 'zfs receive' to smooth out
    the rate of data flow and prevent bottlenecks caused by network latency or speed fluctuation."""
    if (
        size_estimate_bytes >= p.min_pipe_transfer_size
        and (
            (loc == "src" and (p.src.is_nonlocal or p.dst.is_nonlocal))
            or (loc == "dst" and (p.src.is_nonlocal or p.dst.is_nonlocal))
            or (loc == "local" and p.src.is_nonlocal and p.dst.is_nonlocal)
        )
        and p.is_program_available("mbuffer", loc)
    ):
        recordsize = max(recordsize, 128 * 1024 if is_solaris_zfs_location(p, loc) else 2 * 1024 * 1024)
        return shlex.join([p.mbuffer_program, "-s", str(recordsize)] + p.mbuffer_program_opts)
    else:
        return "cat"


def _compress_cmd(p: Params, loc: str, size_estimate_bytes: int) -> str:
    """If zstd command is on the PATH, uses it in the ssh network pipe between 'zfs send' and 'zfs receive' to reduce network
    bottlenecks by sending compressed data."""
    if (
        size_estimate_bytes >= p.min_pipe_transfer_size
        and (p.src.is_nonlocal or p.dst.is_nonlocal)
        and p.is_program_available("zstd", loc)
    ):
        return shlex.join([p.compression_program, "-c"] + p.compression_program_opts)
    else:
        return "cat"


def _decompress_cmd(p: Params, loc: str, size_estimate_bytes: int) -> str:
    """Returns decompression command for network pipe if remote supports it."""
    if (
        size_estimate_bytes >= p.min_pipe_transfer_size
        and (p.src.is_nonlocal or p.dst.is_nonlocal)
        and p.is_program_available("zstd", loc)
    ):
        return shlex.join([p.compression_program, "-dc"])
    else:
        return "cat"


WORKER_THREAD_NUMBER_REGEX: re.Pattern = re.compile(r"ThreadPoolExecutor-\d+_(\d+)")


def _pv_cmd(
    job: Job, loc: str, size_estimate_bytes: int, size_estimate_human: str, disable_progress_bar: bool = False
) -> str:
    """If pv command is on the PATH, monitors the progress of data transfer from 'zfs send' to 'zfs receive'; Progress can be
    viewed via "tail -f $pv_log_file" aka tail -f ~/bzfs-logs/current.pv or similar."""
    p = job.params
    if p.is_program_available("pv", loc):
        size: str = f"--size={size_estimate_bytes}"
        if disable_progress_bar or size_estimate_bytes == 0:
            size = ""
        pv_log_file: str = p.log_params.pv_log_file
        thread_name: str = threading.current_thread().name
        if match := WORKER_THREAD_NUMBER_REGEX.fullmatch(thread_name):
            worker = int(match.group(1))
            if worker > 0:
                pv_log_file += PV_FILE_THREAD_SEPARATOR + f"{worker:04}"
        if job.is_first_replication_task.get_and_set(False):
            if job.isatty and not p.quiet:
                job.progress_reporter.start()
            job.replication_start_time_nanos = time.monotonic_ns()
        if job.isatty and not p.quiet:
            with open_nofollow(pv_log_file, mode="a", encoding="utf-8", perm=FILE_PERMISSIONS) as fd:
                fd.write("\n")  # mark start of new stream so ProgressReporter can reliably reset bytes_in_flight
            job.progress_reporter.enqueue_pv_log_file(pv_log_file)
        pv_program_opts: list[str] = [p.pv_program] + p.pv_program_opts
        if job.progress_update_intervals is not None:  # for testing
            pv_program_opts += [f"--interval={job.progress_update_intervals[0]}"]
        pv_program_opts += ["--force", f"--name={size_estimate_human}"]
        pv_program_opts += [size] if size else []
        return f"LC_ALL=C {shlex.join(pv_program_opts)} 2>> {shlex.quote(pv_log_file)}"
    else:
        return "cat"


def _squote(remote: Remote, arg: str) -> str:
    """Quotes an argument only when running remotely over ssh."""
    return arg if remote.ssh_user_host == "" else shlex.quote(arg)


def _dquote(arg: str) -> str:
    """Shell-escapes double quotes and dollar and backticks, then surrounds with double quotes."""
    return '"' + arg.replace('"', '\\"').replace("$", "\\$").replace("`", "\\`") + '"'


def delete_snapshots(job: Job, remote: Remote, dataset: str, snapshot_tags: list[str]) -> None:
    """Deletes snapshots in manageable batches on the specified remote."""
    if len(snapshot_tags) == 0:
        return
    p, log = job.params, job.params.log
    log.info(p.dry(f"Deleting {len(snapshot_tags)} snapshots within %s: %s"), dataset, snapshot_tags)
    # delete snapshots in batches without creating a command line that's too big for the OS to handle
    run_ssh_cmd_batched(
        job,
        remote,
        _delete_snapshot_cmd(p, remote, dataset + "@"),
        snapshot_tags,
        lambda batch: _delete_snapshot(job, remote, dataset, dataset + "@" + ",".join(batch)),
        max_batch_items=1 if is_solaris_zfs(p, remote) else job.params.max_snapshots_per_minibatch_on_delete_snaps,
        sep=",",
    )


def _delete_snapshot(job: Job, r: Remote, dataset: str, snapshots_to_delete: str) -> None:
    """Runs zfs destroy for a comma-separated snapshot list."""
    p = job.params
    cmd: list[str] = _delete_snapshot_cmd(p, r, snapshots_to_delete)
    is_dry: bool = p.dry_run and is_solaris_zfs(p, r)  # solaris-11.4 knows no 'zfs destroy -n' flag
    try:
        maybe_inject_error(job, cmd=cmd, error_trigger="zfs_delete_snapshot")
        run_ssh_command(job, r, LOG_DEBUG, is_dry=is_dry, print_stdout=True, cmd=cmd)
    except (subprocess.CalledProcessError, UnicodeDecodeError) as e:
        stderr: str = stderr_to_str(e.stderr) if hasattr(e, "stderr") else ""
        no_sleep: bool = _clear_resumable_recv_state_if_necessary(job, dataset, stderr)
        # op isn't idempotent so retries regather current state from the start
        raise RetryableError("Subprocess failed", no_sleep=no_sleep) from e


def _delete_snapshot_cmd(p: Params, r: Remote, snapshots_to_delete: str) -> list[str]:
    """Builds zfs destroy command for given snapshots."""
    return p.split_args(
        f"{r.sudo} {p.zfs_program} destroy", p.force_hard, p.verbose_destroy, p.dry_run_destroy, snapshots_to_delete
    )


def delete_bookmarks(job: Job, remote: Remote, dataset: str, snapshot_tags: list[str]) -> None:
    """Removes bookmarks individually since zfs lacks batch deletion."""
    if len(snapshot_tags) == 0:
        return
    # Unfortunately ZFS has no syntax yet to delete multiple bookmarks in a single CLI invocation
    p, log = job.params, job.params.log
    log.info(
        p.dry(f"Deleting {len(snapshot_tags)} bookmarks within %s: %s"), dataset, dataset + "#" + ",".join(snapshot_tags)
    )
    cmd: list[str] = p.split_args(f"{remote.sudo} {p.zfs_program} destroy")
    run_ssh_cmd_parallel(
        job,
        remote,
        [(cmd, (f"{dataset}#{snapshot_tag}" for snapshot_tag in snapshot_tags))],
        lambda _cmd, batch: try_ssh_command(
            job, remote, LOG_DEBUG, is_dry=p.dry_run, print_stdout=True, cmd=_cmd + batch, exists=False
        ),
        max_batch_items=1,
    )


def delete_datasets(job: Job, remote: Remote, datasets: Iterable[str]) -> None:
    """Deletes the given datasets via zfs destroy -r on the given remote."""
    # Impl is batch optimized to minimize CLI + network roundtrips: only need to run zfs destroy if previously
    # destroyed dataset (within sorted datasets) is not a prefix (aka ancestor) of current dataset
    p, log = job.params, job.params.log
    last_deleted_dataset: str = DONT_SKIP_DATASET
    for dataset in sorted(datasets):
        if is_descendant(dataset, of_root_dataset=last_deleted_dataset):
            continue
        log.info(p.dry("Deleting dataset tree: %s"), f"{dataset} ...")
        cmd: list[str] = p.split_args(
            f"{remote.sudo} {p.zfs_program} destroy -r {p.force_unmount} {p.force_hard} {p.verbose_destroy}",
            p.dry_run_destroy,
            dataset,
        )
        is_dry = p.dry_run and is_solaris_zfs(p, remote)  # solaris-11.4 knows no 'zfs destroy -n' flag
        run_ssh_command(job, remote, LOG_DEBUG, is_dry=is_dry, print_stdout=True, cmd=cmd)
        last_deleted_dataset = dataset


def _create_zfs_filesystem(job: Job, filesystem: str) -> None:
    """Creates destination filesystem hierarchies without mounting them."""
    # zfs create -p -u $filesystem
    # To ensure the filesystems that we create do not get mounted, we apply a separate 'zfs create -p -u'
    # invocation for each non-existing ancestor. This is because a single 'zfs create -p -u' applies the '-u'
    # part only to the immediate filesystem, rather than to the not-yet existing ancestors.
    p = job.params
    parent: str = ""
    no_mount: str = "-u" if p.is_program_available(ZFS_VERSION_IS_AT_LEAST_2_1_0, "dst") else ""
    for component in filesystem.split("/"):
        parent += component
        if not job.dst_dataset_exists[parent]:
            cmd: list[str] = p.split_args(f"{p.dst.sudo} {p.zfs_program} create -p", no_mount, parent)
            try:
                run_ssh_command(job, p.dst, LOG_DEBUG, is_dry=p.dry_run, print_stdout=True, cmd=cmd)
            except subprocess.CalledProcessError as e:
                # ignore harmless error caused by 'zfs create' without the -u flag, or by dataset already existing
                if not (
                    "filesystem successfully created, but it may only be mounted by root" in e.stderr
                    or "filesystem successfully created, but not mounted" in e.stderr  # SolarisZFS
                    or "dataset already exists" in e.stderr
                    or "filesystem already exists" in e.stderr  # SolarisZFS?
                ):
                    raise
            if not p.dry_run:
                job.dst_dataset_exists[parent] = True
        parent += "/"


def _create_zfs_bookmarks(job: Job, remote: Remote, dataset: str, snapshots: list[str]) -> None:
    """Creates bookmarks for the given snapshots, using the 'zfs bookmark' CLI."""
    # Unfortunately ZFS has no syntax yet to create multiple bookmarks in a single CLI invocation
    p = job.params

    def create_zfs_bookmark(cmd: list[str]) -> None:
        snapshot = cmd[-1]
        assert "@" in snapshot
        bookmark_cmd: list[str] = cmd + [replace_prefix(snapshot, old_prefix=f"{dataset}@", new_prefix=f"{dataset}#")]
        try:
            run_ssh_command(job, remote, LOG_DEBUG, is_dry=p.dry_run, print_stderr=False, cmd=bookmark_cmd)
        except subprocess.CalledProcessError as e:
            # ignore harmless zfs error caused by bookmark with the same name already existing
            if ": bookmark exists" not in e.stderr:
                print(e.stderr, file=sys.stderr, end="")
                raise

    if p.create_bookmarks != "none" and are_bookmarks_enabled(p, remote):
        cmd: list[str] = p.split_args(f"{remote.sudo} {p.zfs_program} bookmark")
        run_ssh_cmd_parallel(
            job, remote, [(cmd, snapshots)], lambda _cmd, batch: create_zfs_bookmark(_cmd + batch), max_batch_items=1
        )


def _estimate_send_size(job: Job, remote: Remote, dst_dataset: str, recv_resume_token: str | None, *items: str) -> int:
    """Estimates num bytes to transfer via 'zfs send'."""
    p = job.params
    if p.no_estimate_send_size or is_solaris_zfs(p, remote):
        return 0  # solaris-11.4 does not have a --parsable equivalent
    zfs_send_program_opts: list[str] = ["--parsable" if opt == "-P" else opt for opt in p.curr_zfs_send_program_opts]
    zfs_send_program_opts = append_if_absent(zfs_send_program_opts, "-v", "-n", "--parsable")
    if recv_resume_token:
        zfs_send_program_opts = ["-Pnv", "-t", recv_resume_token]
        items = ()
    cmd: list[str] = p.split_args(f"{remote.sudo} {p.zfs_program} send", zfs_send_program_opts, items)
    try:
        lines: str | None = try_ssh_command(job, remote, LOG_TRACE, cmd=cmd)
    except RetryableError as retryable_error:
        assert retryable_error.__cause__ is not None
        if recv_resume_token:
            e = retryable_error.__cause__
            stderr: str = stderr_to_str(e.stderr) if hasattr(e, "stderr") else ""
            retryable_error.no_sleep = _clear_resumable_recv_state_if_necessary(job, dst_dataset, stderr)
        # op isn't idempotent so retries regather current state from the start of replicate_dataset()
        raise
    if lines is None:
        return 0  # src dataset or snapshot has been deleted by third party
    size: str = lines.splitlines()[-1]
    assert size.startswith("size")
    return int(size[size.index("\t") + 1 :])


def _zfs_set(job: Job, properties: list[str], remote: Remote, dataset: str) -> None:
    """Applies the given property key=value pairs via 'zfs set' CLI to the given dataset on the given remote."""
    p = job.params
    if len(properties) == 0:
        return
    # set properties in batches without creating a command line that's too big for the OS to handle
    cmd: list[str] = p.split_args(f"{remote.sudo} {p.zfs_program} set")
    run_ssh_cmd_batched(
        job,
        remote,
        cmd,
        properties,
        lambda batch: run_ssh_command(
            job, remote, LOG_DEBUG, is_dry=p.dry_run, print_stdout=True, cmd=cmd + batch + [dataset]
        ),
        max_batch_items=1 if is_solaris_zfs(p, remote) else 2**29,  # solaris-11.4 CLI doesn't accept multiple props
    )


def _zfs_get(
    job: Job,
    remote: Remote,
    dataset: str,
    sources: str,
    output_columns: str,
    propnames: str,
    splitlines: bool,
    props_cache: dict[tuple[str, str, str], dict[str, str | None]],
) -> dict[str, str | None]:
    """Returns the results of 'zfs get' CLI on the given dataset on the given remote."""
    assert dataset
    assert sources
    assert output_columns
    if not propnames:
        return {}
    p = job.params
    cache_key = (sources, output_columns, propnames)
    props: dict[str, str | None] | None = props_cache.get(cache_key)
    if props is None:
        cmd: list[str] = p.split_args(f"{p.zfs_program} get -Hp -o {output_columns} -s {sources} {propnames}", dataset)
        lines: str = run_ssh_command(job, remote, LOG_TRACE, cmd=cmd)
        is_name_value_pair: bool = "," in output_columns
        props = {}
        # if not splitlines: omit single trailing newline that was appended by 'zfs get' CLI
        for line in lines.splitlines() if splitlines else [lines[0:-1]]:
            if is_name_value_pair:
                propname, propvalue = line.split("\t", 1)
                props[propname] = propvalue
            else:
                props[line] = None
        props_cache[cache_key] = props
    return props


def _incremental_send_steps_wrapper(
    p: Params, src_snapshots: list[str], src_guids: list[str], included_guids: set[str], is_resume: bool
) -> list[tuple[str, str, str, list[str]]]:
    """Returns incremental send steps, optionally converting -I to -i."""
    force_convert_I_to_i: bool = p.src.use_zfs_delegation and not getenv_bool("no_force_convert_I_to_i", True)  # noqa: N806
    # force_convert_I_to_i == True implies that:
    # If using 'zfs allow' delegation mechanism, force convert 'zfs send -I' to a series of
    # 'zfs send -i' as a workaround for zfs issue https://github.com/openzfs/zfs/issues/16394
    return incremental_send_steps(src_snapshots, src_guids, included_guids, is_resume, force_convert_I_to_i)


def _add_recv_property_options(
    job: Job, full_send: bool, recv_opts: list[str], dataset: str, cache: dict[tuple[str, str, str], dict[str, str | None]]
) -> tuple[list[str], list[str]]:
    """Reads the ZFS properties of the given src dataset; Appends zfs recv -o and -x values to recv_opts according to CLI
    params, and returns properties to explicitly set on the dst dataset after 'zfs receive' completes successfully."""
    p = job.params
    set_opts: list[str] = []
    x_names: list[str] = p.zfs_recv_x_names
    x_names_set: set[str] = set(x_names)
    ox_names: set[str] = p.zfs_recv_ox_names.copy()
    if p.is_program_available(ZFS_VERSION_IS_AT_LEAST_2_2_0, p.dst.location):
        # workaround for https://github.com/openzfs/zfs/commit/b0269cd8ced242e66afc4fa856d62be29bb5a4ff
        # 'zfs recv -x foo' on zfs < 2.2 errors out if the 'foo' property isn't contained in the send stream
        for propname in x_names:
            recv_opts.append("-x")
            recv_opts.append(propname)
        ox_names.update(x_names)  # union
    for config in [p.zfs_recv_o_config, p.zfs_recv_x_config, p.zfs_set_config]:
        if len(config.include_regexes) == 0:
            continue  # this is the default - it's an instant noop
        if (full_send and "full" in config.targets) or (not full_send and "incremental" in config.targets):
            # 'zfs get' uses newline as record separator and tab as separator between output columns. A ZFS user property
            # may contain newline and tab characters (indeed anything). Together, this means that there is no reliable
            # way to determine where a record ends and the next record starts when listing multiple arbitrary records in
            # a single 'zfs get' call. Therefore, here we use a separate 'zfs get' call for each ZFS user property.
            # TODO: perf: on zfs >= 2.3 use json via zfs get -j to safely merge all zfs gets into one 'zfs get' call
            try:
                props_any: dict = _zfs_get(job, p.src, dataset, config.sources, "property", "all", True, cache)
                props_filtered: dict = filter_properties(p, props_any, config.include_regexes, config.exclude_regexes)
                user_propnames: list[str] = [name for name in props_filtered if ":" in name]
                sys_propnames: str = ",".join(name for name in props_filtered if ":" not in name)
                props: dict = _zfs_get(job, p.src, dataset, config.sources, "property,value", sys_propnames, True, cache)
                for propnames in user_propnames:
                    props.update(_zfs_get(job, p.src, dataset, config.sources, "property,value", propnames, False, cache))
            except (subprocess.CalledProcessError, UnicodeDecodeError) as e:
                raise RetryableError("Subprocess failed") from e
            for propname in sorted(props.keys()):
                if config is p.zfs_recv_o_config:
                    if not (propname in ox_names or propname in x_names_set):
                        recv_opts.append("-o")
                        recv_opts.append(f"{propname}={props[propname]}")
                        ox_names.add(propname)
                elif config is p.zfs_recv_x_config:
                    if propname not in ox_names:
                        recv_opts.append("-x")
                        recv_opts.append(propname)
                        ox_names.add(propname)
                else:
                    assert config is p.zfs_set_config
                    set_opts.append(f"{propname}={props[propname]}")
    return recv_opts, set_opts


def _check_zfs_dataset_busy(job: Job, remote: Remote, dataset: str, busy_if_send: bool = True) -> bool:
    """Decline to start a state changing ZFS operation that is, although harmless, likely to collide with other currently
    running processes. Instead, retry the operation later, after some delay. For example, decline to start a 'zfs receive'
    into a destination dataset if another process is already running another 'zfs receive' into the same destination dataset,
    as ZFS would reject any such attempt. However, it's actually fine to run an incremental 'zfs receive' into a dataset in
    parallel with a 'zfs send' out of the very same dataset. This also helps daisy chain use cases where A replicates to B,
    and B replicates to C.

    check_zfs_dataset_busy() offers no guarantees, it merely proactively avoids likely collisions. In other words, even if
    the process check below passes there is no guarantee that the destination dataset won't be busy by the time we actually
    execute the 'zfs send' operation. In such an event ZFS will reject the operation, we'll detect that, and we'll simply
    retry, after some delay. check_zfs_dataset_busy() can be disabled via --ps-program=-.

    TLDR: As is common for long-running operations in distributed systems, we use coordination-free optimistic concurrency
    control where the parties simply retry on collision detection (rather than coordinate concurrency via a remote lock
    server).
    """
    p, log = job.params, job.params.log
    if not p.is_program_available("ps", remote.location):
        return True
    cmd: list[str] = p.split_args(f"{p.ps_program} -Ao args")
    procs: list[str] = (try_ssh_command(job, remote, LOG_TRACE, cmd=cmd) or "").splitlines()
    if job.inject_params.get("is_zfs_dataset_busy", False):
        procs += ["sudo -n zfs receive -u -o foo:bar=/baz " + dataset]  # for unit testing only
    if not _is_zfs_dataset_busy(procs, dataset, busy_if_send=busy_if_send):
        return True
    op: str = "zfs {receive" + ("|send" if busy_if_send else "") + "} operation"
    try:
        die(f"Cannot continue now: Destination is already busy with {op} from another process: {dataset}")
    except SystemExit as e:
        log.warning("%s", e)
        raise RetryableError("dst currently busy with zfs mutation op") from e


ZFS_DATASET_BUSY_PREFIX: str = r"(([^ ]*?/)?(sudo|doas)( +-n)? +)?([^ ]*?/)?zfs (receive|recv"
ZFS_DATASET_BUSY_IF_MODS: re.Pattern[str] = re.compile((ZFS_DATASET_BUSY_PREFIX + ") .*").replace("(", "(?:"))
ZFS_DATASET_BUSY_IF_SEND: re.Pattern[str] = re.compile((ZFS_DATASET_BUSY_PREFIX + "|send) .*").replace("(", "(?:"))


def _is_zfs_dataset_busy(procs: list[str], dataset: str, busy_if_send: bool) -> bool:
    """Checks if any process list entry indicates ZFS activity on dataset."""
    regex: re.Pattern[str] = ZFS_DATASET_BUSY_IF_SEND if busy_if_send else ZFS_DATASET_BUSY_IF_MODS
    suffix: str = " " + dataset
    infix: str = " " + dataset + "@"
    return any((proc.endswith(suffix) or infix in proc) and regex.fullmatch(proc) for proc in procs)
