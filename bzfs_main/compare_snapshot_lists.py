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
"""Implementation of bzfs --compare-snapshot-lists."""

from __future__ import annotations
import itertools
import os
import time
from collections import (
    defaultdict,
)
from dataclasses import (
    dataclass,
    field,
)
from typing import (
    TYPE_CHECKING,
    Callable,
    Iterable,
    Iterator,
    Sequence,
)

from bzfs_main.argparse_cli import (
    CMP_CHOICES_ITEMS,
)
from bzfs_main.detect import (
    are_bookmarks_enabled,
    is_solaris_zfs,
)
from bzfs_main.filter import (
    filter_snapshots,
)
from bzfs_main.parallel_batch_cmd import (
    zfs_list_snapshots_in_parallel,
)
from bzfs_main.parallel_iterator import (
    run_in_parallel,
)
from bzfs_main.utils import (
    FILE_PERMISSIONS,
    Interner,
    T,
    human_readable_bytes,
    human_readable_duration,
    isotime_from_unixtime,
    open_nofollow,
    relativize_dataset,
)

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from bzfs_main.bzfs import Job
    from bzfs_main.configuration import Remote


@dataclass(order=True, frozen=True)
class ComparableSnapshot:
    """Snapshot entry comparable by rel_dataset and GUID for sorting and merging."""

    key: tuple[str, str]  # rel_dataset, guid
    cols: list[str] = field(compare=False)


def run_compare_snapshot_lists(job: Job, src_datasets: list[str], dst_datasets: list[str]) -> None:
    """Compares source and destination dataset trees recursively with respect to snapshots, for example to check if all
    recently taken snapshots have been successfully replicated by a periodic job; implements --compare-snapshot-lists.

    Lists snapshots only contained in source (tagged with 'src'), only contained in destination (tagged with 'dst'), and
    contained in both source and destination (tagged with 'all'), in the form of a TSV file, along with other snapshot
    metadata.

    Implemented with a time and space efficient streaming algorithm; easily scales to millions of datasets and any number of
    snapshots. Time complexity is O(max(N log N, M log M)) where N is the number of datasets and M is the number of snapshots
    per dataset. Space complexity is O(max(N, M)). Assumes that both src_datasets and dst_datasets are sorted.
    """
    p, log = job.params, job.params.log
    src, dst = p.src, p.dst
    task: str = src.root_dataset + " vs. " + dst.root_dataset
    tsv_dir: str = p.log_params.log_file[0 : -len(".log")] + ".cmp"
    os.makedirs(tsv_dir, exist_ok=True)
    tsv_file: str = os.path.join(tsv_dir, (src.root_dataset + "%" + dst.root_dataset).replace("/", "~") + ".tsv")
    tmp_tsv_file: str = tsv_file + ".tmp"
    compare_snapshot_lists: set[str] = set(p.compare_snapshot_lists.split("+"))
    is_src_dst_all: bool = all(choice in compare_snapshot_lists for choice in CMP_CHOICES_ITEMS)
    all_src_dst: list[str] = [loc for loc in ("all", "src", "dst") if loc in compare_snapshot_lists]
    is_first_row: bool = True
    now: int | None = None

    def zfs_list_snapshot_iterator(r: Remote, sorted_datasets: list[str]) -> Iterator[str]:
        """Lists snapshots sorted by dataset name; All snapshots of a given dataset will be adjacent."""
        assert (not job.is_test_mode) or sorted_datasets == sorted(sorted_datasets), "List is not sorted"
        written_zfs_prop: str = "written"  # https://openzfs.github.io/openzfs-docs/man/master/7/zfsprops.7.html#written
        if is_solaris_zfs(p, r):  # solaris-11.4 zfs does not know the "written" ZFS snapshot property
            written_zfs_prop = "type"  # for simplicity, fill in the non-integer dummy constant type="snapshot"
        props: str = job.creation_prefix + f"creation,guid,createtxg,{written_zfs_prop},name"
        types: str = "snapshot"
        if p.use_bookmark and r.location == "src" and are_bookmarks_enabled(p, r):
            types = "snapshot,bookmark"  # output list ordering: intentionally makes bookmarks appear *after* snapshots
        cmd: list[str] = p.split_args(f"{p.zfs_program} list -t {types} -d 1 -Hp -o {props}")  # sorted by dataset, createtxg
        for lines in zfs_list_snapshots_in_parallel(job, r, cmd, sorted_datasets):
            yield from lines

    def snapshot_iterator(root_dataset: str, sorted_itr: Iterator[str]) -> Iterator[ComparableSnapshot]:
        """Splits/groups snapshot stream into distinct datasets, sorts by GUID within a dataset such that any two snapshots
        with the same GUID will lie adjacent to each other during the upcoming phase that merges src snapshots and dst
        snapshots."""
        # streaming group by dataset name (consumes constant memory only)
        for dataset, group in itertools.groupby(
            sorted_itr, key=lambda line: line.rsplit("\t", 1)[1].replace("#", "@", 1).split("@", 1)[0]
        ):
            snapshots: list[str] = list(group)  # fetch all snapshots of current dataset, e.g. dataset=tank1/src/foo
            snapshots = filter_snapshots(job, snapshots, filter_bookmarks=True)  # apply include/exclude policy
            snapshots.sort(key=lambda line: line.split("\t", 2)[1])  # stable sort by GUID (2nd remains createtxg)
            rel_dataset: str = relativize_dataset(dataset, root_dataset)  # rel_dataset=/foo, root_dataset=tank1/src
            last_guid: str = ""
            for line in snapshots:
                cols = line.split("\t")
                creation, guid, createtxg, written, snapshot_name = cols
                if guid == last_guid:
                    assert "#" in snapshot_name
                    continue  # ignore bookmarks whose snapshot still exists. also ignore dupes of bookmarks
                last_guid = guid
                if written == "snapshot":
                    written = "-"  # sanitize solaris-11.4 work-around (solaris-11.4 also has no bookmark feature)
                    cols = [creation, guid, createtxg, written, snapshot_name]
                key = (rel_dataset, guid)  # ensures src snapshots and dst snapshots with the same GUID will be adjacent
                yield ComparableSnapshot(key, cols)

    def print_dataset(rel_dataset: str, entries: Iterable[tuple[str, ComparableSnapshot]]) -> None:
        entries = sorted(  # fetch all snapshots of current dataset and sort em by creation, createtxg, snapshot_tag
            entries,
            key=lambda entry: (
                int((cols := entry[1].cols)[0]),  # creation
                int(cols[2]),  # createtxg
                cols[-1].replace("#", "@", 1).split("@", 1)[1],  # snapshot_tag
            ),
        )

        @dataclass
        class SnapshotStats:
            snapshot_count: int = field(default=0)
            sum_written: int = field(default=0)
            snapshot_count_since: int = field(default=0)
            sum_written_since: int = field(default=0)
            latest_snapshot_idx: int | None = field(default=None)
            latest_snapshot_row_str: str | None = field(default=None)
            latest_snapshot_creation: str | None = field(default=None)
            oldest_snapshot_row_str: str | None = field(default=None)
            oldest_snapshot_creation: str | None = field(default=None)

        # print metadata of snapshots of current dataset to TSV file; custom stats can later be computed from there
        stats: defaultdict[str, SnapshotStats] = defaultdict(SnapshotStats)
        header: str = "location creation_iso createtxg rel_name guid root_dataset rel_dataset name creation written"
        nonlocal is_first_row
        if is_first_row:
            fd.write(header.replace(" ", "\t") + "\n")
            is_first_row = False
        for i, entry in enumerate(entries):  # entry is tuple[location:str, ComparableSnapshot]
            location: str = entry[0]  # "src" or "dst" or "all"
            creation, guid, createtxg, written, name = entry[1].cols
            root_dataset: str = dst.root_dataset if location == CMP_CHOICES_ITEMS[1] else src.root_dataset
            rel_name: str = relativize_dataset(name, root_dataset)
            creation_iso: str = isotime_from_unixtime(int(creation))
            row = (location, creation_iso, createtxg, rel_name, guid, root_dataset, rel_dataset, name, creation, written)
            # Example: src 2024-11-06_08:30:05 17435050 /foo@test_2024-11-06_08:30:05_daily 2406491805272097867 tank1/src /foo tank1/src/foo@test_2024-10-06_08:30:04_daily 1730878205 24576
            row_str = "\t".join(row)
            if not p.dry_run:
                fd.write(row_str + "\n")
            s = stats[location]
            s.snapshot_count += 1
            s.sum_written += int(written) if written != "-" else 0
            s.latest_snapshot_idx = i
            s.latest_snapshot_row_str = row_str
            s.latest_snapshot_creation = creation
            if not s.oldest_snapshot_row_str:
                s.oldest_snapshot_row_str = row_str
                s.oldest_snapshot_creation = creation

        # for convenience, directly log basic summary stats of current dataset
        k = stats["all"].latest_snapshot_idx  # defaults to None
        k = k if k is not None else -1
        for entry in entries[k + 1 :]:  # aggregate basic stats since latest common snapshot
            location = entry[0]
            creation, guid, createtxg, written, name = entry[1].cols
            s = stats[location]
            s.snapshot_count_since += 1
            s.sum_written_since += int(written) if written != "-" else 0
        prefix: str = f"Comparing {rel_dataset}~"
        msgs: list[str] = []
        msgs.append(f"{prefix} of {task}")
        msgs.append(
            f"{prefix} Q: No src snapshots are missing on dst, and no dst snapshots are missing on src, "
            "and there is a common snapshot? A: "
            + (
                "n/a"
                if not is_src_dst_all
                else str(
                    stats["src"].snapshot_count == 0 and stats["dst"].snapshot_count == 0 and stats["all"].snapshot_count > 0
                )
            )
        )
        nonlocal now
        now = now or round(time.time())  # uses the same timestamp across the entire dataset tree
        latcom = "latest common snapshot"
        for loc in all_src_dst:
            s = stats[loc]
            msgs.append(f"{prefix} Latest snapshot only in {loc}: {s.latest_snapshot_row_str or 'n/a'}")
            msgs.append(f"{prefix} Oldest snapshot only in {loc}: {s.oldest_snapshot_row_str or 'n/a'}")
            msgs.append(f"{prefix} Snapshots only in {loc}: {s.snapshot_count}")
            msgs.append(f"{prefix} Snapshot data written only in {loc}: {human_readable_bytes(s.sum_written)}")
            if loc != "all":
                na = None if k >= 0 else "n/a"
                msgs.append(f"{prefix} Snapshots only in {loc} since {latcom}: {na or s.snapshot_count_since}")
                msgs.append(
                    f"{prefix} Snapshot data written only in {loc} since {latcom}: "
                    f"{na or human_readable_bytes(s.sum_written_since)}"
                )
            all_creation = stats["all"].latest_snapshot_creation
            latest = ("latest", s.latest_snapshot_creation)
            oldest = ("oldest", s.oldest_snapshot_creation)
            for label, s_creation in latest, oldest:
                if loc != "all":
                    hd = "n/a"
                    if s_creation and k >= 0:
                        assert all_creation is not None
                        hd = human_readable_duration(int(all_creation) - int(s_creation), unit="s")
                    msgs.append(f"{prefix} Time diff between {latcom} and {label} snapshot only in {loc}: {hd}")
            for label, s_creation in latest, oldest:
                hd = "n/a" if not s_creation else human_readable_duration(now - int(s_creation), unit="s")
                msgs.append(f"{prefix} Time diff between now and {label} snapshot only in {loc}: {hd}")
        log.info("%s", "\n".join(msgs))

    # setup streaming pipeline
    src_snapshot_itr: Iterator = snapshot_iterator(src.root_dataset, zfs_list_snapshot_iterator(src, src_datasets))
    dst_snapshot_itr: Iterator = snapshot_iterator(dst.root_dataset, zfs_list_snapshot_iterator(dst, dst_datasets))
    merge_itr = _merge_sorted_iterators(CMP_CHOICES_ITEMS, p.compare_snapshot_lists, src_snapshot_itr, dst_snapshot_itr)

    interner: Interner[str] = Interner()  # reduces memory footprint
    rel_datasets: dict[str, set[str]] = defaultdict(set)
    for datasets, remote in (src_datasets, src), (dst_datasets, dst):
        for dataset in datasets:  # rel_dataset=/foo, root_dataset=tank1/src
            rel_datasets[remote.location].add(interner.intern(relativize_dataset(dataset, remote.root_dataset)))
    rel_src_or_dst: list[str] = sorted(rel_datasets["src"].union(rel_datasets["dst"]))

    log.debug("%s", f"Temporary TSV output file comparing {task} is: {tmp_tsv_file}")
    with open_nofollow(tmp_tsv_file, "w", encoding="utf-8", perm=FILE_PERMISSIONS) as fd:
        # streaming group by rel_dataset (consumes constant memory only); entry is a Tuple[str, ComparableSnapshot]
        group = itertools.groupby(merge_itr, key=lambda entry: entry[1].key[0])
        _print_datasets(group, lambda rel_ds, entries: print_dataset(rel_ds, entries), rel_src_or_dst)
    os.rename(tmp_tsv_file, tsv_file)
    log.info("%s", f"Final TSV output file comparing {task} is: {tsv_file}")

    tsv_file = tsv_file[0 : tsv_file.rindex(".")] + ".rel_datasets_tsv"
    tmp_tsv_file = tsv_file + ".tmp"
    with open_nofollow(tmp_tsv_file, "w", encoding="utf-8", perm=FILE_PERMISSIONS) as fd:
        header: str = "location rel_dataset src_dataset dst_dataset"
        fd.write(header.replace(" ", "\t") + "\n")
        src_only: set[str] = rel_datasets["src"].difference(rel_datasets["dst"])
        dst_only: set[str] = rel_datasets["dst"].difference(rel_datasets["src"])
        for rel_dataset in rel_src_or_dst:
            loc = "src" if rel_dataset in src_only else "dst" if rel_dataset in dst_only else "all"
            src_dataset = src.root_dataset + rel_dataset if rel_dataset not in dst_only else ""
            dst_dataset = dst.root_dataset + rel_dataset if rel_dataset not in src_only else ""
            row = (loc, rel_dataset, src_dataset, dst_dataset)  # Example: all /foo/bar tank1/src/foo/bar tank2/dst/foo/bar
            if not p.dry_run:
                fd.write("\t".join(row) + "\n")
    os.rename(tmp_tsv_file, tsv_file)


def _print_datasets(group: itertools.groupby, fn: Callable[[str, Iterable], None], rel_datasets: Iterable[str]) -> None:
    """Iterate over grouped datasets and apply fn, adding gaps for missing ones."""
    rel_datasets = sorted(rel_datasets)
    n = len(rel_datasets)
    i = 0
    for rel_dataset, entries in group:
        while i < n and rel_datasets[i] < rel_dataset:
            fn(rel_datasets[i], [])  # Also print summary stats for datasets whose snapshot stream is empty
            i += 1
        assert i >= n or rel_datasets[i] == rel_dataset
        i += 1
        fn(rel_dataset, entries)
    while i < n:
        fn(rel_datasets[i], [])  # Also print summary stats for datasets whose snapshot stream is empty
        i += 1


def _merge_sorted_iterators(
    choices: Sequence[str],  # ["src", "dst", "all"]
    choice: str,  # Example: "src+dst+all"
    src_itr: Iterator[T],
    dst_itr: Iterator[T],
) -> Iterator[tuple[str, T] | tuple[str, T, T]]:
    """The typical pipelined merge algorithm of a merge sort, slightly adapted to our specific use case."""
    assert len(choices) == 3
    assert choice
    flags: int = 0
    for i, item in enumerate(choices):
        if item in choice:
            flags |= 1 << i
    src_next, dst_next = run_in_parallel(lambda: next(src_itr, None), lambda: next(dst_itr, None))
    while not (src_next is None and dst_next is None):
        if src_next == dst_next:
            n = 2
            if (flags & (1 << n)) != 0:
                assert src_next is not None
                assert dst_next is not None
                yield choices[n], src_next, dst_next
            src_next = next(src_itr, None)
            dst_next = next(dst_itr, None)
        elif src_next is None or (dst_next is not None and dst_next < src_next):
            n = 1
            if (flags & (1 << n)) != 0:
                assert dst_next is not None
                yield choices[n], dst_next
            dst_next = next(dst_itr, None)
        else:
            n = 0
            if (flags & (1 << n)) != 0:
                yield choices[n], src_next
            src_next = next(src_itr, None)
