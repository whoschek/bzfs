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

#############################################################################
# Quick Start for local replication: Edit this script. Replace all occurances of the word "nas" with the hostname of your
# local machine. Edit root_dataset_pairs to specify datasets. Make sure `bzfs` and `bzfs_jobrunner` CLIs are on the PATH.
# Run the final script like so:
#
# /etc/bzfs/bzfs_job_example.py --create-src-snapshots --replicate --prune-src-snapshots --prune-src-bookmarks --prune-dst-snapshots
#
# Add this command to your crontab file such that the command runs every minute, or every hour, or every day, or similar.
#############################################################################

import argparse
import os
import subprocess
import sys

parser = argparse.ArgumentParser(
    description=f"""
Periodic replica management jobconfig file, in the form of a script that generates deployment specific parameters and 
submits the parameters to `bzfs_jobrunner`, along with all unknown CLI arguments, using an "Infrastructure as Code" approach.

Usage: {sys.argv[0]} [--create-src-snapshots|--replicate|--prune-src-snapshots|--prune-src-bookmarks|--prune-dst-snapshots]
"""
)
_, unknown_args = parser.parse_known_args()  # forward all unknown args to `bzfs_jobrunner`
unknown_args = unknown_args if len(unknown_args) > 0 else ["--create-src-snapshots"]


# Source and destination datasets that will be managed, in the form of one or more (src, dst) pairs, excluding
# usernames and excluding hostnames, which will all be auto-appended later:
# root_dataset_pairs = ["tank1/foo/bar", "boo/bar", "tank1/baz", "baz"]  # replicate from tank1 to dst
root_dataset_pairs = ["tank1/foo/bar", "tank2/boo/bar"]  # replicate from tank1 to tank2


# Include descendant datasets, i.e. datasets within the dataset tree, including children, children of children, etc.
# recursive = False
recursive = True


# Network hostname of src. Used if replicating in pull mode:
# src_host = "prod001"
# src_host = "127.0.0.1"
src_host = "-"  # for local mode (no ssh, no network)


# Dictionary that maps logical replication target names (the infix portion of a snapshot name) to destination hostnames.
# A destination host will 'pull' replicate snapshots for all targets that map to its --localhost name. Removing a mapping
# can be used to temporarily suspend replication to a given destination host.
# dst_hosts = {
#     "onsite": "nas",
#     "us-west-1": "bak-us-west-1",
#     "eu-west-1": "bak-eu-west-1",
#     "hotspare": "hotspare",
#     "offsite": "archive",
# }
# dst_hosts = {"onsite": "nas"}
# dst_hosts = {"": "nas"}  # empty string as target name is ok
dst_hosts = {"onsite": "nas", "": "nas"}


# Dictionary that maps logical replication target names (the infix portion of a snapshot name) to destination hostnames.
# Has same format as dst_hosts. As part of --prune-dst-snapshots, a destination host will delete any snapshot it has stored
# whose target has no mapping to its --localhost in this dictionary.
# Do not remove a mapping unless you are sure it's ok to delete all those snapshots on that destination host!
# retain_dst_targets = dst_hosts
retain_dst_targets = {"onsite": "nas", "": "nas"}


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
#     "bak-us-west-1": "backups/bak001",
#     "bak-eu-west-1": "backups/bak999",
#     "hotspare": "",
#     "archive": "archives/zoo",
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
# src_snapshot_plan = {
#     org: {
#         "onsite": {"secondly": 40, "minutely": 40, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5},
#         "us-west-1": {"secondly": 0, "minutely": 0, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5},
#         "eu-west-1": {"secondly": 0, "minutely": 0, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5},
#         "hotspare": {"secondly": 40, "minutely": 40, "hourly": 36, "daily": 31},
#     },
#     "test": {
#         "offsite": {"12hourly": 14, "weekly": 12},
#         "onsite": {"100millisecondly": 40},
#     },
# }
# src_snapshot_plan = {  # empty string as target name is ok
#     org: {"": {"secondly": 40, "minutely": 40, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5}}
# }
src_snapshot_plan = {
    org: {"onsite": {"secondly": 40, "minutely": 40, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5}}
}


# Retention periods for snapshots to be used if pruning dst. Has same format as --src-snapshot-plan:
# dst_snapshot_plan = {
#     org: {"onsite": {"secondly": 40, "minutely": 40, "hourly": 36, "daily": 62, "weekly": 52, "monthly": 36, "yearly": 5}}
# }
N = 2  # multiply src_snapshot_plan by a factor N
dst_snapshot_plan = {
    org: {target: {period: round(N * M) for period, M in periods.items()} for target, periods in target_periods.items()}
    for org, target_periods in src_snapshot_plan.items()
}


# Retention periods for bookmarks to be used if pruning src. Has same format as --src-snapshot-plan:
# src_bookmark_plan = {
#     org: {
#         "onsite": {"secondly": 40, "minutely": 40, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5}
#     }
# }
src_bookmark_plan = dst_snapshot_plan


ssh_src_port = 22
# ssh_src_port = 2222  # for hpnssh
# ssh_src_port = 40999


ssh_dst_port = 22
# ssh_dst_port = 2222  # for hpnssh
# ssh_dst_port = 40999


extra_args = []
extra_args += [f"--log-dir={os.path.join(os.path.expanduser('~'), 'bzfs-job-logs', os.path.basename(sys.argv[0]))}"]
# extra_args += ["--localhost=bak-us-west-1"]
# extra_args += ["--src-user=alice"]  # ssh username on src; for pull mode
# extra_args += ["--dst-user=root"]  # ssh username on dst; for push mode
# extra_args += ["--include-dataset=foo"]  # see bzfs --help
# extra_args += ["--exclude-dataset=bar"]
# extra_args += ["--include-dataset-regex=foo.*"]
# extra_args += ["--exclude-dataset-regex=bar.*"]
# extra_args += ["--create-src-snapshots-timeformat=%Y-%m-%d_%H:%M:%S"]  # this is already the default anyway
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


# Taking snapshots, and/or replicating, from every N milliseconds to every 10 seconds or so is considered high frequency.
# For such cases, use daemon mode and consider adding these options to improve performance:
# extra_args += ["--daemon-lifetime=300seconds"]  # daemon exits after this much time has elapsed
# extra_args += ["--daemon-lifetime=86400seconds"]  # daemon exits after this much time has elapsed
# extra_args += ["--daemon-replication-frequency=secondly"]  # replicate every second
# extra_args += ["--daemon-replication-frequency=10secondly"]  # replicate every 10 seconds
# extra_args += ["--daemon-prune-src-frequency=10secondly"]  # prune src snapshots and src bookmarks every 10 seconds
# extra_args += ["--daemon-prune-dst-frequency=10secondly"]  # prune dst snapshots every 10 seconds
# extra_args += ["--create-src-snapshots-timeformat=%Y-%m-%d_%H:%M:%S.%f"]  # adds microseconds to snapshot names
# extra_args += ["--create-src-snapshots-even-if-not-due"]  # nomore run 'zfs list -t snapshot' before snapshot creation
# extra_args += ["--no-resume-recv"]  # skip another 'zfs list' check
# extra_args += ["--no-estimate-send-size"]  # skip 'zfs send -n -v'
# extra_args += ["--pv-program=-"]  # nomore measure via 'pv' / stats
# extra_args += ["--ps-program=-"]  # disable safety check for simultaneous 'zfs receive' to same dataset by another process
# extra_args += ["--mbuffer-program=-"]  # shorten latency of queue handoffs and disable large buffer size allocations
# extra_args += ["--compression-program=-"]  # save CPU by disabling compression-on-the-wire
# extra_args += ["--compression-program=lz4"]  # use less CPU for compression-on-the-wire
# extra_args += ["--threads=200%"]  # use more CPU cores when operating on many datasets
# extra_args += ["--ssh-program=hpnssh"]  # use larger TCP window size over high speed long distance networks
# os.environ["bzfs_no_force_convert_I_to_i"] = "true"  # enable batched incremental replication w/ --no-privilege-elevation


cmd = ["bzfs_jobrunner"]
cmd += ["--recursive"] if recursive else []
cmd += [f"--src-host={src_host}"]
cmd += [f"--dst-hosts={dst_hosts}"]
cmd += [f"--retain-dst-targets={retain_dst_targets}"]
cmd += [f"--dst-root-datasets={dst_root_datasets}"]
cmd += [f"--src-snapshot-plan={src_snapshot_plan}"]
cmd += [f"--src-bookmark-plan={src_bookmark_plan}"]
cmd += [f"--dst-snapshot-plan={dst_snapshot_plan}"]
cmd += [f"--ssh-src-port={ssh_src_port}", f"--ssh-dst-port={ssh_dst_port}"]
cmd += extra_args + unknown_args + ["--"] + root_dataset_pairs
subprocess.run(cmd, text=True, check=True)
