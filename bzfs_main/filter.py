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

"""
* The filter algorithms that apply include/exclude policies are in filter_datasets() and filter_snapshots().
"""

from __future__ import annotations
import math
import os
import re
import socket
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Iterable,
    Optional,
    Tuple,
    Union,
)

from bzfs_main.connection import (
    try_ssh_command,
)
from bzfs_main.utils import (
    DONT_SKIP_DATASET,
    RegexList,
    is_descendant,
    is_included,
    log_debug,
    log_trace,
    relativize_dataset,
    unixtime_infinity_secs,
)

if TYPE_CHECKING:  # pragma: no cover
    from bzfs_main.bzfs import Job, Remote

# constants:
snapshot_regex_filter_name = "snapshot_regex"


UnixTimeRange = Optional[Tuple[Union[timedelta, int], Union[timedelta, int]]]  # Type alias
RankRange = Tuple[Tuple[str, int, bool], Tuple[str, int, bool]]  # Type alias


def filter_datasets(job: Job, remote: Remote, sorted_datasets: list[str]) -> list[str]:
    """Returns all datasets (and their descendants) that match at least one of the include regexes but none of the
    exclude regexes. Assumes the list of input datasets is sorted. The list of output datasets will be sorted too."""
    p, log = job.params, job.params.log
    results = []
    for i, dataset in enumerate(sorted_datasets):
        if i == 0 and p.skip_parent:
            continue
        rel_dataset = relativize_dataset(dataset, remote.root_dataset)
        if rel_dataset.startswith("/"):
            rel_dataset = rel_dataset[1:]  # strip leading '/' char if any
        if is_included(rel_dataset, p.include_dataset_regexes, p.exclude_dataset_regexes):
            results.append(dataset)
            log.debug("Including b/c dataset regex: %s", dataset)
        else:
            log.debug("Excluding b/c dataset regex: %s", dataset)
    if p.exclude_dataset_property:
        results = filter_datasets_by_exclude_property(job, remote, results)
    is_debug = p.log.isEnabledFor(log_debug)
    for dataset in results:
        if is_debug:
            log.debug("Finally included %s dataset: %s", remote.location, dataset)
    if job.is_test_mode:
        # Asserts the following: If a dataset is excluded its descendants are automatically excluded too, and this
        # decision is never reconsidered even for the descendants because exclude takes precedence over include.
        resultset = set(results)
        root_datasets = [dataset for dataset in results if os.path.dirname(dataset) not in resultset]  # have no parent
        for root in root_datasets:  # each root is not a descendant of another dataset
            assert not any(is_descendant(root, of_root_dataset=dataset) for dataset in results if dataset != root)
        for dataset in results:  # each dataset belongs to a subtree rooted at one of the roots
            assert any(is_descendant(dataset, of_root_dataset=root) for root in root_datasets)
    return results


def filter_datasets_by_exclude_property(job: Job, remote: Remote, sorted_datasets: list[str]) -> list[str]:
    """Excludes datasets that are marked with a ZFS user property value that, in effect, says 'skip me'."""
    p, log = job.params, job.params.log
    results = []
    localhostname = None
    skip_dataset = DONT_SKIP_DATASET
    for dataset in sorted_datasets:
        if is_descendant(dataset, of_root_dataset=skip_dataset):
            # skip_dataset shall be ignored or has been deleted by some third party while we're running
            continue  # nothing to do anymore for this dataset subtree (note that datasets is sorted)
        skip_dataset = DONT_SKIP_DATASET
        # TODO perf: on zfs >= 2.3 use json via zfs list -j to safely merge all zfs list's into one 'zfs list' call
        cmd = p.split_args(f"{p.zfs_program} list -t filesystem,volume -Hp -o {p.exclude_dataset_property}", dataset)
        job.maybe_inject_delete(remote, dataset=dataset, delete_trigger="zfs_list_exclude_property")
        property_value = try_ssh_command(job, remote, log_trace, cmd=cmd)
        if property_value is None:
            log.warning(f"Third party deleted {remote.location}: %s", dataset)
            skip_dataset = dataset
        else:
            reason = ""
            property_value = property_value.strip()
            if not property_value or property_value == "-" or property_value.lower() == "true":
                sync = True
            elif property_value.lower() == "false":
                sync = False
            else:
                localhostname = localhostname or socket.gethostname()
                sync = any(localhostname == hostname.strip() for hostname in property_value.split(","))
                reason = f", localhostname: {localhostname}, hostnames: {property_value}"

            if sync:
                results.append(dataset)
                log.debug("Including b/c dataset prop: %s%s", dataset, reason)
            else:
                skip_dataset = dataset
                log.debug("Excluding b/c dataset prop: %s%s", dataset, reason)
    return results


def filter_snapshots(job: Job, basis_snapshots: list[str], all_except: bool = False) -> list[str]:
    """Returns all snapshots that pass all include/exclude policies.
    `all_except=False` returns snapshots *matching* the filters,
    for example those that should be deleted if we are in "delete selected" mode.
    `all_except=True` returns snapshots *not* matching the filters,
    for example those that should be deleted if we are in "retain selected" mode."""

    def resolve_timerange(timerange: UnixTimeRange) -> UnixTimeRange:
        assert timerange is not None
        lo, hi = timerange
        if isinstance(lo, timedelta):
            lo = math.ceil(current_unixtime_in_secs - lo.total_seconds())
        if isinstance(hi, timedelta):
            hi = math.ceil(current_unixtime_in_secs - hi.total_seconds())
        assert isinstance(lo, int)
        assert isinstance(hi, int)
        return (lo, hi) if lo <= hi else (hi, lo)

    p, log = job.params, job.params.log
    current_unixtime_in_secs: float = p.create_src_snapshots_config.current_datetime.timestamp()
    resultset = set()
    for snapshot_filter in p.snapshot_filters:
        snapshots = basis_snapshots
        for _filter in snapshot_filter:
            name = _filter.name
            if name == snapshot_regex_filter_name:
                snapshots = filter_snapshots_by_regex(job, snapshots, regexes=_filter.options)
            elif name == "include_snapshot_times":
                timerange = resolve_timerange(_filter.timerange) if _filter.timerange is not None else _filter.timerange
                snapshots = filter_snapshots_by_creation_time(job, snapshots, include_snapshot_times=timerange)
            else:
                assert name == "include_snapshot_times_and_ranks"
                timerange = resolve_timerange(_filter.timerange) if _filter.timerange is not None else _filter.timerange
                snapshots = filter_snapshots_by_creation_time_and_rank(
                    job, snapshots, include_snapshot_times=timerange, include_snapshot_ranks=_filter.options
                )
        resultset.update(snapshots)  # union
    snapshots = [line for line in basis_snapshots if "#" in line or ((line in resultset) != all_except)]
    is_debug = log.isEnabledFor(log_debug)
    for snapshot in snapshots:
        if is_debug:
            log.debug("Finally included snapshot: %s", snapshot[snapshot.rindex("\t") + 1 :])
    return snapshots


def filter_snapshots_by_regex(job: Job, snapshots: list[str], regexes: tuple[RegexList, RegexList]) -> list[str]:
    """Returns all snapshots that match at least one of the include regexes but none of the exclude regexes."""
    exclude_snapshot_regexes, include_snapshot_regexes = regexes
    log = job.params.log
    is_debug = log.isEnabledFor(log_debug)
    results = []
    for snapshot in snapshots:
        i = snapshot.find("@")  # snapshot separator
        if i < 0:
            continue  # retain bookmarks to help find common snapshots, apply filter only to snapshots
        elif is_included(snapshot[i + 1 :], include_snapshot_regexes, exclude_snapshot_regexes):
            results.append(snapshot)
            if is_debug:
                log.debug("Including b/c snapshot regex: %s", snapshot[snapshot.rindex("\t") + 1 :])
        else:
            if is_debug:
                log.debug("Excluding b/c snapshot regex: %s", snapshot[snapshot.rindex("\t") + 1 :])
    return results


def filter_snapshots_by_creation_time(job: Job, snapshots: list[str], include_snapshot_times: UnixTimeRange) -> list[str]:
    log = job.params.log
    is_debug = log.isEnabledFor(log_debug)
    lo_snaptime, hi_snaptime = include_snapshot_times or (0, unixtime_infinity_secs)
    assert isinstance(lo_snaptime, int)
    assert isinstance(hi_snaptime, int)
    results = []
    for snapshot in snapshots:
        if "@" not in snapshot:
            continue  # retain bookmarks to help find common snapshots, apply filter only to snapshots
        elif lo_snaptime <= int(snapshot[0 : snapshot.index("\t")]) < hi_snaptime:
            results.append(snapshot)
            if is_debug:
                log.debug("Including b/c creation time: %s", snapshot[snapshot.rindex("\t") + 1 :])
        else:
            if is_debug:
                log.debug("Excluding b/c creation time: %s", snapshot[snapshot.rindex("\t") + 1 :])
    return results


def filter_snapshots_by_creation_time_and_rank(
    job: Job, snapshots: list[str], include_snapshot_times: UnixTimeRange, include_snapshot_ranks: list[RankRange]
) -> list[str]:

    def get_idx(rank: tuple[str, int, bool], n: int) -> int:
        kind, num, is_percent = rank
        m = round(n * num / 100) if is_percent else min(n, num)
        assert kind == "latest" or kind == "oldest"
        return m if kind == "oldest" else n - m

    assert isinstance(include_snapshot_ranks, list)
    assert len(include_snapshot_ranks) > 0
    log = job.params.log
    is_debug = log.isEnabledFor(log_debug)
    lo_time, hi_time = include_snapshot_times or (0, unixtime_infinity_secs)
    assert isinstance(lo_time, int)
    assert isinstance(hi_time, int)
    n = sum(1 for snapshot in snapshots if "@" in snapshot)
    for rank_range in include_snapshot_ranks:
        lo_rank, hi_rank = rank_range
        lo = get_idx(lo_rank, n)
        hi = get_idx(hi_rank, n)
        lo, hi = (lo, hi) if lo <= hi else (hi, lo)
        i = 0
        results = []
        for snapshot in snapshots:
            if "@" not in snapshot:
                continue  # retain bookmarks to help find common snapshots, apply filter only to snapshots
            else:
                msg = None
                if lo <= i < hi:
                    msg = "Including b/c snapshot rank: %s"
                elif lo_time <= int(snapshot[0 : snapshot.index("\t")]) < hi_time:
                    msg = "Including b/c creation time: %s"
                if msg:
                    results.append(snapshot)
                else:
                    msg = "Excluding b/c snapshot rank: %s"
                if is_debug:
                    log.debug(msg, snapshot[snapshot.rindex("\t") + 1 :])
                i += 1
        snapshots = results
        n = hi - lo
    return snapshots


def filter_properties(
    job: Job, props: dict[str, str | None], include_regexes: RegexList, exclude_regexes: RegexList
) -> dict[str, str | None]:
    """Returns ZFS props whose name matches at least one of the include regexes but none of the exclude regexes."""
    log = job.params.log
    is_debug = log.isEnabledFor(log_debug)
    results: dict[str, str | None] = {}
    for propname, propvalue in props.items():
        if is_included(propname, include_regexes, exclude_regexes):
            results[propname] = propvalue
            if is_debug:
                log.debug("Including b/c property regex: %s", propname)
        else:
            if is_debug:
                log.debug("Excluding b/c property regex: %s", propname)
    return results


def filter_lines(input_list: Iterable[str], input_set: set[str]) -> list[str]:
    """For each line in input_list, includes the line if input_set contains the first column field of that line."""
    if len(input_set) == 0:
        return []
    return [line for line in input_list if line[0 : line.index("\t")] in input_set]


def filter_lines_except(input_list: list[str], input_set: set[str]) -> list[str]:
    """For each line in input_list, includes the line if input_set does not contain the first column field of that line."""
    if len(input_set) == 0:
        return input_list
    return [line for line in input_list if line[0 : line.index("\t")] not in input_set]


def dataset_regexes(src: Remote, dst: Remote, datasets: list[str]) -> list[str]:
    results = []
    for dataset in datasets:
        if dataset.startswith("/"):
            # it's an absolute dataset - convert it to a relative dataset
            dataset = dataset[1:]
            if is_descendant(dataset, of_root_dataset=src.root_dataset):
                dataset = relativize_dataset(dataset, src.root_dataset)
            elif is_descendant(dataset, of_root_dataset=dst.root_dataset):
                dataset = relativize_dataset(dataset, dst.root_dataset)
            else:
                continue  # ignore datasets that make no difference
            if dataset.startswith("/"):
                dataset = dataset[1:]
        if dataset.endswith("/"):
            dataset = dataset[0:-1]
        if dataset:
            regex = re.escape(dataset)
        else:
            regex = ".*"
        results.append(regex)
    return results
