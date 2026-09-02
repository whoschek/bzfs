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
"""Tier-1 jobconfig: snapshot creation on `srchost` plus push replication to the always-on `bckphost`.

Purpose: `srchost` is a workstation that is powered on only while somebody works on it, so it both takes its own
snapshots and pushes them to `bckphost` while it happens to be awake. `bckphost` keeps a sliding window of those
snapshots. Assumptions: this file is deployed unchanged to `srchost` and `bckphost`; job actions are supplied by the
caller (see bzfs_backup_run.sh), never hardcoded here. Design rationale: push mode, because `bckphost` cannot pull
from a machine that is asleep most of the time.
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
Jobconfig script for tier 1 of a three-tier snapshot backup chain: srchost --> bckphost --> archost.

This tier creates 15-minutely, hourly and daily snapshots on the source workstation, pushes them to the always-on
low power backup host, and prunes the source. Tier 2 (bzfs_job_bckp_to_arc.py) forwards the same snapshots from the
backup host to the archive host.

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


# Hostnames, as printed by the `hostname` CLI on the respective machine:
src_host: Final = "srchost"  # EDIT ME: the workstation that is powered on only while in use
bckp_host: Final = "bckphost"  # EDIT ME: the always-on low power backup host


# Organization and replication target; together with the timestamp and the period they form the snapshot name,
# for example `backup_onsite_2026-09-01_14:15:00_15minutely`. Both tiers of the chain use the very same names because
# both tiers move the very same snapshots; only the retention periods differ per tier.
org: Final = "backup"  # EDIT ME (optional)
target: Final = "onsite"  # EDIT ME (optional)


# Source and destination datasets that will be managed, in the form of one or more (src, dst) pairs, excluding
# usernames and excluding hostnames, which will all be auto-appended later. The dst side is prefixed with `src_host`
# so that `bckphost` can store replicas of several source hosts side by side, and so that the dataset layout below
# `dst_root_datasets` is identical on `bckphost` and on `archost`:
root_dataset_pairs: Final = ["tank/work", f"{src_host}/work"]  # EDIT ME: srchost:tank/work --> bckphost:bak/srchost/work


# Include descendant datasets, i.e. datasets within the dataset tree, including children, children of children, etc:
recursive: Final = True


src_hosts: Final = [src_host]


# `bckphost` receives replicas of all snapshots whose target is `target`:
dst_hosts: Final = {bckp_host: [target]}


# As part of --prune-dst-snapshots, `bckphost` deletes any snapshot it has stored whose target has no mapping here.
# Do not remove a mapping here unless you are sure it's ok to delete all those snapshots on that destination host!
retain_dst_targets: Final = dst_hosts


# Root dataset on `bckphost` that the dst side of `root_dataset_pairs` is prepended with, i.e. the dst side of the
# example pair above lands in `bak/srchost/work`:
dst_root_datasets: Final = {bckp_host: "bak"}  # EDIT ME: name of the ZFS pool or dataset holding backups on bckphost


# Retention periods for snapshots on `srchost`; also determines which snapshots are created there.
# For example, "daily": 30 retains all daily snapshots that were created less than 30 days ago, and ensures that the
# latest 30 daily snapshots (per dataset) are retained regardless of creation time.
# `srchost` intentionally retains substantially more than one push interval worth of snapshots, so that a push outage
# of a few days does not silently drop snapshots that `bckphost` has not received yet:
src_snapshot_plan: Final = {org: {target: {"15minutely": 192, "hourly": 96, "daily": 30}}}


# Retention periods for snapshots on `bckphost`, i.e. the sliding window that the low power host keeps.
# This plan does double duty: --prune-dst-snapshots enforces it on `bckphost`, and --replicate derives from it which
# snapshots are worth sending there in the first place.
# Note that "daily" is deliberately retained much longer than the one week window of the sub-daily periods: `archost`
# is offline most of the time, and a daily snapshot that `bckphost` deletes before `archost` fetched it is gone for
# good. 30 days of dailies means `archost` may stay offline for up to a month without punching a hole into the
# permanent archive. Raise this if `archost` can be offline for longer:
dst_snapshot_plan: Final = {org: {target: {"15minutely": 672, "hourly": 168, "daily": 30}}}  # 672 = 7 days * 96/day


# Retention periods for bookmarks on `srchost`. A bookmark is a tiny, immutable marker that lets `bzfs` continue
# incremental replication even after the corresponding snapshot has already been pruned from `srchost`. Retaining
# bookmarks at least as long as `bckphost` retains snapshots means a long push outage costs a few skipped snapshots
# but never a full re-send:
src_bookmark_plan: Final = dst_snapshot_plan


# Alerts if the ZFS 'creation' time of the latest snapshot on `bckphost` is too old, i.e. if `srchost` has stopped
# delivering. Because `srchost` is expected to be powered off much of the time, these thresholds are deliberately
# coarse; they are the "srchost never came back" safety net, whereas the fine grained "the push is failing right now"
# detection is done by bzfs_alert.sh around the push job itself.
# `dst_snapshot_cycles` disables the companion check on the *oldest* snapshot: while `srchost` sleeps, nothing on
# `bckphost` gets pruned or added, so the oldest snapshot keeps aging without anything actually being wrong:
monitor_snapshot_plan: Final = {
    org: {
        target: {
            "daily": {"warning": "7 days", "critical": "21 days", "dst_snapshot_cycles": 36500},  # EDIT ME (optional)
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
    # "--ssh-dst-user=root",  # EDIT ME (optional): ssh username on bckphost; for push mode
    # "--cache-snapshots",  # perf: fewer 'zfs list -t snapshot' calls before snapshot creation, replication & monitoring
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
