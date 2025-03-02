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
Periodic replica management job, using an "Infrastructure as Code" approach, in the form of a CLI that generates deployment 
specific parameters and passes the parameters to `bzfs_cron`, along with all unknown CLI arguments.

Usage: {sys.argv[0]} [--create-src-snapshots|--replicate|--prune-src-snapshots|--prune-src-bookmarks|--prune-dst-snapshots]

Edit this python cronjob file in a central place (e.g. versioned in a git repo), then copy the (very same) shared file onto 
the source host and all destination hosts, and add crontab entries or systemd timers or similar, along these lines: 

crontab on source host:
```
* * * * * testuser /etc/bzfs/bzfs_cronjob_example.py --create-src-snapshots --prune-src-snapshots --prune-src-bookmarks
```

crontab on destination host(s):
```
* * * * * testuser /etc/bzfs/bzfs_cronjob_example.py --replicate --prune-dst-snapshots
```
"""
)
_, unknown_args = parser.parse_known_args()  # forward all unknown args to `bzfs_cron`
unknown_args = unknown_args if len(unknown_args) > 0 else ["--create-src-snapshots"]


# Source and destination datasets that will be managed, in the form of one or more (src, dst) pairs, excluding
# usernames and excluding hostnames, which will all be auto-appended later:
# root_dataset_pairs = ["tank1/foo/bar", "boo/bar", "tank1/baz", "baz"]  # replicate from tank1 to dst
root_dataset_pairs = ["tank1/foo/bar", "tank2/boo/bar"]  # replicate from tank1 to tank2


# Include descendant datasets, i.e. datasets within the dataset tree, including children, children of children, etc.
# recursive = False
recursive = True


# Network hostname of src. Used if replicating in pull mode:
# src_host = "prod001.example.com"
# src_host = "-"  # for local mode (no ssh, no network)
src_host = "127.0.0.1"


# Dictionary that maps logical replication target names (the infix portion of a snapshot name) to actual destination
# network hostnames:
# dst_hosts = {
#     "onsite": "nas",
#     "us-west-1": "bak-us-west-1.example.com",
#     "eu-west-1": "bak-eu-west-1.example.com",
#     "offsite": "archive.example.com",
# }
# dst_hosts = {"onsite": "nas"}
# dst_hosts = {"": "nas"}  # missing target name is ok
dst_hosts = {"onsite": "nas", "": "nas"}


# Dictionary that maps each destination hostname to a root dataset located on that destination host. Typically, this
# is the backup ZFS pool or a ZFS dataset path within that pool. The root dataset name is a prefix that will be
# prepended to each dataset on replication to that destination host.
# For example, dst_root_datasets = {"nas":"tank2/bak"} turns "boo/bar" into "tank2/bak/boo/bar" on dst host "nas",
# and with root_dataset_pairs = ["tank1/foo/bar", "boo/bar" ] it turns tank1/foo/bar on src into tank2/bak/boo/bar
# on dst host "nas".
# For example, dst_root_datasets = {"nas":""} turns "boo/bar" into "boo/bar" on dst host "nas", and with
# root_dataset_pairs = ["tank1/foo/bar", "tank1/foo/bar" ] it turns tank1/foo/bar on src into tank1/foo/bar
# on dst host "nas".
# dst_root_datasets = {
#     "nas": "tank2/bak",
#     "bak-us-west-1.example.com": "backups/bak001",
#     "bak-eu-west-1.example.com": "backups/bak999",
#     "archive.example.com": "archives/zoo",
# }
dst_root_datasets = {
    "nas": "",  # Empty string means 'Don't prepend a prefix' (for safety, the hostname must always be in the dict)
}


# Organization (the prefix portion of a snapshot name):
# org = "autosnap"
# org = "zrepl"
# org = "bzfs"
# org = "test"
org = "prod"


# Retention periods for snapshots to be used if pruning src, and when creating new snapshots on src.
# For example, "daily": 31 specifies to retain all daily snapshots that were created less than 31 days ago, and
# ensure that the latest 31 daily snapshots (per dataset) are retained regardless of creation time.
# Each target of each organization can have separate retention periods.
# Uses snapshot names like 'prod_onsite_<timestamp>_daily', 'prod_onsite_<timestamp>_minutely', etc.:
# src_snapshot_periods = {
#     org: {
#         "onsite": {"secondly": 45, "minutely": 45, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18, "yearly": 5},
#         "us-west-1": {"secondly": 0, "minutely": 0, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18, "yearly": 5},
#         "eu-west-1": {"secondly": 0, "minutely": 0, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18, "yearly": 5},
#     },
#     "test": {
#         "offsite": {"12hourly": 42, "weekly": 12},
#     },
# }
# src_snapshot_periods = {  # missing target name is ok
#     org: {"": {"secondly": 45, "minutely": 45, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18, "yearly": 5}}
# }
src_snapshot_periods = {
    org: {"onsite": {"secondly": 45, "minutely": 45, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18, "yearly": 5}}
}


# Retention periods for snapshots to be used if pruning dst. Has same format as --src-snapshot-periods:
# dst_snapshot_periods = {
#     org: {"onsite": {"secondly": 45, "minutely": 45, "hourly": 48, "daily": 62, "weekly": 52, "monthly": 36, "yearly": 5}}
# }
N = 2  # multiply src_snapshot_periods by a factor N
dst_snapshot_periods = {
    org: {target: {period: round(N * M) for period, M in periods.items()} for target, periods in target_periods.items()}
    for org, target_periods in src_snapshot_periods.items()
}


# Retention periods for bookmarks to be used if pruning src. Has same format as --src-snapshot-periods:
# src_bookmark_periods = {
#     org: {
#         "onsite": {"secondly": 45, "minutely": 45, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18, "yearly": 5}
#     }
# }
src_bookmark_periods = dst_snapshot_periods


ssh_src_port = 22
# ssh_src_port = 40999


ssh_dst_port = 22
# ssh_dst_port = 40999


extra_args = []
# extra_args += ["--src-user=alice"]  # ssh username on src; for pull mode
# extra_args += ["--dst-user=root"]  # ssh username on dst; for push mode
# extra_args += ["--include-dataset=foo"]  # see bzfs --help
# extra_args += ["--exclude-dataset=bar"]
# extra_args += ["--include-dataset-regex=foo.*"]
# extra_args += ["--exclude-dataset-regex=bar.*"]
# extra_args += ["--create-src-snapshots-timeformat=%Y-%m-%d_%H:%M:%S"]  # this is already the default anyway
# extra_args += ["--create-src-snapshots-timeformat=%Y-%m-%d_%H:%M:%S.%f"]  # adds microseconds
# extra_args += ["--create-src-snapshots-timezone=UTC"]
# extra_args += ["--zfs-send-program-opts=''"]
# extra_args += ["--zfs-recv-program-opts=''"]
# extra_args += ["--force-rollback-to-latest-snapshot"]
# extra_args += ["--force-rollback-to-latest-common-snapshot"]
# extra_args += ["--force"]
# extra_args += ["--force-destroy-dependents"]
# extra_args += ["--force-unmount"]
# extra_args += ["--force-once"]
# extra_args += ["--skip-parent"]
# extra_args += ["--skip-missing-snapshots=xyz"]
# extra_args += ["--retries=2"]
# extra_args += ["--retry-min-sleep-secs=0.125"]
# extra_args += ["--retry-max-sleep-secs=300"]
# extra_args += ["--retry-max-elapsed-secs=3600"]
# extra_args += ["--skip-on-error=dataset"]
# extra_args += ["--dryrun"]
# extra_args += ["--verbose"]
# extra_args += ["--quiet"]
# extra_args += ["--no-privilege-elevation"]
# ... and so on (include all other options from bzfs --help here too)
# extra_args += ["--daily_hour=23"]  # take daily snapshots at 23:59
# extra_args += ["--daily_minute=59"]
# extra_args += ["--weekly_weekday=0"]  # take weekly snapshots on Sunday 2:00am (0=Sunday, 1=Monday, ..., 6=Saturday)
# extra_args += ["--weekly_hour=2"]
# extra_args += ["--weekly_minute=0"]
# ... and so on (include all other options from bzfs --help here too)


# daemon_replication_frequency = "minutely"
# daemon_replication_frequency = "secondly"
daemon_replication_frequency = "10secondly"

cmd = ["bzfs_cron"]
cmd += ["--recursive"] if recursive else []
cmd += [f"--src-host={src_host}"]
cmd += [f"--dst-hosts={dst_hosts}"]
cmd += [f"--dst-root-datasets={dst_root_datasets}"]
cmd += [f"--src-snapshot-periods={src_snapshot_periods}"]
cmd += [f"--src-bookmark-periods={src_bookmark_periods}"]
cmd += [f"--dst-snapshot-periods={dst_snapshot_periods}"]
cmd += [f"--daemon-replication-frequency={daemon_replication_frequency}"]
cmd += [f"--ssh-src-port={ssh_src_port}", f"--ssh-dst-port={ssh_dst_port}"]
cmd += extra_args + unknown_args + ["--"] + root_dataset_pairs
subprocess.run(cmd, text=True, check=True)
