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

import argparse
import subprocess
import sys

parser = argparse.ArgumentParser(
    description=f"""
CLI that defines user specific parameters and passes them to `bzfs_cron`, along with all unknown CLI arguments, using a
"Configuration as code" approach.
Usage: {sys.argv[0]} [--create-src-snapshots|--replicate|--prune-src-snapshots|--prune-src-bookmarks|--prune-dst-snapshots]
"""
)
_, unknown_args = parser.parse_known_args()  # forward all unknown args to `bzfs_cron`
unknown_args = unknown_args if len(unknown_args) > 0 else ["--create-src-snapshots"]


# Source and destination datasets that will be managed, in the form of (src, dst) pairs, excluding usernames and excluding
# host names, which will all be auto-appended later:
root_dataset_pairs = ["tank1/foo/bar", "tank2/boo/bar", "tank1/baz", "tank2/baz"]


# Include descendant datasets, i.e. datasets within the dataset tree, including children, and children of children, etc
# recursive = False
recursive = True


# Network host name of src. Used if replicating in pull mode:
# src_host = "prod001.example.com"
# src_host = "-"  # for local mode (no ssh, no network)
src_host = "127.0.0.1"


# Dictionary that maps logical replication target names (the infix portion of a snapshot name) to actual destination network
# host names
# dst_hosts = {
#     "onsite": "nas",
#     "us-west-1": "bak-us-west-1.example.com",
#     "eu-west-1": "bak-eu-west-1.example.com",
#     "offsite": "archive.example.com",
# }
dst_hosts = {"onsite": "nas"}


# Retention periods for snapshots to be used if pruning src, and when creating new snapshots on src.
# Uses snapshot names like 'prod_<timestamp>_onsite_secondly', 'prod_<timestamp>_onsite_minutely', etc:
# src_snapshot_periods = {
#     "prod": {
#         "onsite": {"secondly": 150, "minutely": 90, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18, "yearly": 5},
#         "us-west-1": {"secondly": 0, "minutely": 0, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18, "yearly": 5},
#         "eu-west-1": {"secondly": 0, "minutely": 0, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18, "yearly": 5},
#     },
#     "test": {
#         "offsite": {"12hourly": 42, "weekly": 12},
#     },
# }
src_snapshot_periods = {
    "prod": {
        "onsite": {"secondly": 150, "minutely": 90, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18, "yearly": 5}
    }
}


# Retention periods for bookmarks to be used if pruning src. Has same format as --src-snapshot-periods:
# src_bookmark_periods = {
#     "prod": {
#         "onsite": {"secondly": 150, "minutely": 90, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18, "yearly": 5}
#     }
# }
N = 5  # multiply src_snapshot_periods by a factor N
src_bookmark_periods = {
    org: {target: {period: N * count for period, count in periods.items()} for target, periods in target_periods.items()}
    for org, target_periods in src_snapshot_periods.items()
}


# Retention periods for snapshots to be used if pruning dst. Has same format as --src-snapshot-periods:
dst_snapshot_periods = {
    "prod": {
        "onsite": {"secondly": 150, "minutely": 90, "hourly": 48, "daily": 62, "weekly": 52, "monthly": 36, "yearly": 5}
    }
}


# daemon_replication_frequency = "minutely"
# daemon_replication_frequency = "secondly"
daemon_replication_frequency = "10secondly"

daemon_prune_src_frequency = "minutely"

daemon_prune_dst_frequency = "minutely"


ssh_src_port = 22
# ssh_src_port = 40999

ssh_dst_port = 22
# ssh_dst_port = 40999

extra_args = []
# extra_args = ["--src-user=alice"]  # ssh username on src
# extra_args = ["--dst-user=root"]  # ssh username on dst

cmd = ["bzfs_cron"]
cmd += ["--recursive"] if recursive else []
cmd += [f"--src-host={src_host}"]
cmd += [f"--dst-hosts={dst_hosts}"]
cmd += [f"--src-snapshot-periods={src_snapshot_periods}"]
cmd += [f"--src-bookmark-periods={src_bookmark_periods}"]
cmd += [f"--dst-snapshot-periods={dst_snapshot_periods}"]
cmd += [f"--daemon-replication-frequency={daemon_replication_frequency}"]
cmd += [f"--daemon-prune-src-frequency={daemon_prune_src_frequency}"]
cmd += [f"--daemon-prune-dst-frequency={daemon_prune_dst_frequency}"]
cmd += [f"--ssh-src-port={ssh_src_port}", f"--ssh-dst-port={ssh_dst_port}"]
cmd += extra_args + unknown_args + ["--"] + root_dataset_pairs
subprocess.run(cmd, text=True, check=True)
