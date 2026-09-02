#!/usr/bin/env python3
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
#
"""Tier-2 jobconfig: pull replication from the always-on `bckphost` into the deep archive on `archost`.

Purpose: `archost` has vast storage but is online only for an indeterminate window each day, so it pulls whatever
`bckphost` has accumulated meanwhile and keeps daily snapshots essentially forever. Assumptions: `bckphost` already
holds the snapshots produced by tier 1 (bzfs_job_src_to_bckp.py); snapshot names are identical on all three hosts
because `zfs send` preserves them. Design rationale: pull mode, because only `archost` knows when `archost` is up.
"""

import argparse
import os
import pathlib
import pwd
import subprocess
import sys
from typing import (
    Final,
)

parser = argparse.ArgumentParser(description="""
Jobconfig script for tier 2 of a three-tier snapshot backup chain: srchost --> bckphost --> archost.

This tier forwards the snapshots that tier 1 (bzfs_job_src_to_bckp.py) delivered to the backup host on to the archive
host, which retains daily snapshots forever and sub-daily snapshots for a month.

This script submits parameters plus all unknown CLI arguments to `bzfs_jobrunner`, which in turn delegates most of the
actual work to the `bzfs` CLI.
""")
known_args, unknown_args = parser.parse_known_args()  # forward all unknown args to `bzfs_jobrunner`
if len(unknown_args) == 0:
    print(
        "ERROR: Missing command. Usage: " + sys.argv[0] + " --create-src-snapshots|--replicate|--prune-src-snapshots|"
        "--prune-src-bookmarks|--prune-dst-snapshots|--monitor-src-snapshots|--monitor-dst-snapshots",
        file=sys.stderr,
    )
    sys.exit(5)


# Hostnames, as printed by the `hostname` CLI on the respective machine. In this tier the always-on backup host acts
# as the *source*, and the deep archive host acts as the destination:
src_host: Final = "srchost"  # EDIT ME: only used to spell out the dataset paths below; must match tier 1
bckp_host: Final = "bckphost"  # EDIT ME: must match tier 1
arc_host: Final = "archost"  # EDIT ME: the archive host that is online at an indeterminate time each day


# Must match tier 1, because both tiers move the very same snapshots:
org: Final = "backup"  # EDIT ME (optional): must match tier 1
target: Final = "onsite"  # EDIT ME (optional): must match tier 1


# Root dataset on `bckphost` below which tier 1 stores its replicas; must match `dst_root_datasets` in tier 1:
bckp_root_dataset: Final = "bak"  # EDIT ME: must match tier 1


# Source and destination datasets that will be managed, in the form of one or more (src, dst) pairs, excluding
# usernames and excluding hostnames, which will all be auto-appended later. The src side is the path that tier 1
# replicates *into* on `bckphost`; the dst side is identical to the dst side of tier 1, so that the dataset layout
# below `dst_root_datasets` is the same on `bckphost` and on `archost`:
root_dataset_pairs: Final = [  # EDIT ME: must match tier 1
    f"{bckp_root_dataset}/{src_host}/work",  # bckphost:bak/srchost/work
    f"{src_host}/work",  # --> archost:archive/srchost/work
]


# Include descendant datasets, i.e. datasets within the dataset tree, including children, children of children, etc:
recursive: Final = True


src_hosts: Final = [bckp_host]


# `archost` receives replicas of all snapshots whose target is `target`:
dst_hosts: Final = {arc_host: [target]}


# As part of --prune-dst-snapshots, `archost` deletes any snapshot it has stored whose target has no mapping here.
# Do not remove a mapping here unless you are sure it's ok to delete all those snapshots on that destination host!
retain_dst_targets: Final = dst_hosts


# Root dataset on `archost` that the dst side of `root_dataset_pairs` is prepended with, i.e. the dst side of the
# example pair above lands in `archive/srchost/work`:
dst_root_datasets: Final = {arc_host: "archive"}  # EDIT ME: name of the ZFS pool or dataset holding the archive


# 36500 days is a hundred years, i.e. "keep forever" for all practical purposes. `bzfs` retention is expressed as a
# number of period cycles, so an explicit large number is how "forever" is spelled:
forever: Final = 36500


# Retention periods for snapshots on the *source* of this tier, i.e. on `bckphost`. Tier 1 is what actually prunes
# `bckphost` (via its --prune-dst-snapshots); this plan is a deliberate copy of tier 1's `dst_snapshot_plan` so that
# an accidental `--prune-src-snapshots` run against this jobconfig cannot delete anything that tier 1 wants to keep.
# Never schedule --prune-src-snapshots for this tier; see bzfs_backup_run.sh:
src_snapshot_plan: Final = {org: {target: {"15minutely": 672, "hourly": 168, "daily": 30}}}  # EDIT ME: must match tier 1


# Retention periods for snapshots on `archost`, i.e. the permanent archive: every daily snapshot is kept forever,
# while the more frequent snapshots are thinned out after a month.
# This plan does double duty: --prune-dst-snapshots enforces it on `archost`, and --replicate derives from it which
# snapshots are worth pulling in the first place:
dst_snapshot_plan: Final = {
    org: {target: {"15minutely": 96 * 30, "hourly": 24 * 30, "daily": forever}}  # EDIT ME (optional)
}


# Retention periods for bookmarks on `bckphost`. Bookmarks are tiny and immutable, and they let `bzfs` resume
# incremental replication from a snapshot that `bckphost` has already pruned. Keeping daily bookmarks forever is what
# guarantees that `archost` can catch up incrementally no matter how long it stayed offline - without them, an outage
# longer than the `bckphost` window would force a full re-send of the whole dataset:
src_bookmark_plan: Final = dst_snapshot_plan


# Alerts if the ZFS 'creation' time of the latest snapshot on `archost` is too old, i.e. if `archost` has stopped
# fetching. `archost` is expected to come online once a day, so anything beyond two days is a genuine problem.
# Note that the far more common symptom - `archost` being unreachable altogether - surfaces as a failed monitoring
# run rather than as a stale snapshot; bzfs_alert.sh treats both the same way:
monitor_snapshot_plan: Final = {
    org: {
        target: {
            "daily": {"warning": "26 hours", "critical": "48 hours"},  # EDIT ME (optional)
        },
    },
}


# Max worker jobs to run in parallel at any time; specified as a positive integer, or as a percentage of num CPU cores:
workers: Final = "100%"


home_dir: Final = pwd.getpwuid(os.getuid()).pw_dir  # returns the user's home directory without reading the $HOME env var
basename_stem: Final = pathlib.Path(sys.argv[0]).stem  # the file's basename without file extension
extra_args: Final = [
    f"--job-id={basename_stem}",
    f"--log-dir={os.path.join(home_dir, 'bzfs-job-logs', 'bzfs-logs-' + basename_stem)}",
    # "--ssh-src-user=root",  # EDIT ME (optional): ssh username on bckphost; for pull mode
    # "--cache-snapshots",  # perf: fewer 'zfs list -t snapshot' calls before replication & monitoring
]

cmd = ["bzfs_jobrunner"]  # not Final: built up incrementally below
cmd += ["--recursive"] if recursive else []
cmd += [f"--src-hosts={src_hosts}"]
cmd += [f"--dst-hosts={dst_hosts}"]
cmd += [f"--retain-dst-targets={retain_dst_targets}"]
cmd += [f"--dst-root-datasets={dst_root_datasets}"]
cmd += [f"--src-snapshot-plan={src_snapshot_plan}"]
cmd += [f"--src-bookmark-plan={src_bookmark_plan}"]
cmd += [f"--dst-snapshot-plan={dst_snapshot_plan}"]
cmd += [f"--monitor-snapshot-plan={monitor_snapshot_plan}"]
cmd += [f"--workers={workers}"]
cmd += extra_args + unknown_args
cmd += ["--root-dataset-pairs"] + root_dataset_pairs
sys.exit(subprocess.run(cmd, stdin=subprocess.DEVNULL, text=True, check=False).returncode)
