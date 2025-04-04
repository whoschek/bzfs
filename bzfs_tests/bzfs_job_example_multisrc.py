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
# Quickstart for local replication test: Edit this script. Replace all occurrences of the word "nas" w/ the hostname of your
# local machine. Edit root_dataset_pairs and dst_root_datasets to specify datasets.
# Make sure `bzfs` and `bzfs_jobrunner` and `bzfs_jobscheduler` CLIs are on the PATH. Run the final script like so:
#
# /etc/bzfs/bzfs_job_example_multisrc.py --location=src --create-src-snapshots --prune-src-snapshots --prune-src-bookmarks --monitor-src-snapshots
# /etc/bzfs/bzfs_job_example_multisrc.py --location=dst --replicate=pull --prune-dst-snapshots --monitor-dst-snapshots
#
# Add this command to your crontab file (or systemd or monit or similar), such that the command runs every minute, or
# every hour, or every day, or similar.
#############################################################################

import argparse
import os
import subprocess
import sys
import uuid

parser = argparse.ArgumentParser(
    description=f"""
Multisrc Jobconfig script for an efficient geo-replicated backup service where each of the N destination hosts is located in 
a separate geographic region and pulls replicas from (the same set of) M source hosts, using the same shared multisrc
jobconfig script. The number of source hosts can be large. N=2 or N=3 are typical geo-replication factors.

Generates deployment specific parameters to manage periodic ZFS snapshot creation, replication, pruning, and monitoring.
This script should be periodically executed on each source host and each destination host, e.g. by a cron job (or similar).

Requires that `dst_root_datasets` (see below) is configured to use a separate destination root dataset per source host.

The script submits parameters plus all unknown CLI arguments to `bzfs_jobscheduler`, which in turn submits jobs to
`bzfs_jobrunner`, which in turn delegates most of the actual work to the `bzfs` CLI. Uses an "Infrastructure as Code"
approach.
"""
)
parser.add_argument("--location", choices=["src", "dst"], required=True, help="Act as src or dst host.")
known_args, unknown_args = parser.parse_known_args()  # forward all unknown args to `bzfs_jobrunner`
if len(unknown_args) == 0:
    print(
        "ERROR: Missing command. Usage: " + sys.argv[0] + " --create-src-snapshots|--replicate|--prune-src-snapshots|"
        "--prune-src-bookmarks|--prune-dst-snapshots|--monitor-src-snapshots|--monitor-dst-snapshots",
        sys.stderr,
    )
    sys.exit(5)


# List of one or more source hostnames. Each of the N destination hosts pulls replicas from (the same set of) M source hosts:
# src_hosts = ["prod001", "prod002", "prod999"]
# src_hosts = ["127.0.0.1", "nas"]
# src_hosts = ["nas"]
src_hosts = ["-"]  # for local mode (no ssh, no network)


commands = []
src_hosts = ["-"] if known_args.location == "src" else src_hosts
jobid_prefix = uuid.uuid1().hex
for i, src_host in enumerate(src_hosts):

    # Source and destination datasets that will be managed, in the form of one or more (src, dst) pairs, excluding
    # usernames and excluding hostnames, which will all be auto-appended later:
    # root_dataset_pairs = ["tank1/foo/bar", "boo/bar", "tank1/baz", "baz"]  # replicate from tank1 to dst
    root_dataset_pairs = ["tank1/foo/bar", "tank2/boo/bar"]  # replicate from tank1 to tank2

    # Include descendant datasets, i.e. datasets within the dataset tree, including children, children of children, etc.
    # recursive = False
    recursive = True

    # Dictionary that maps each destination hostname to a list of zero or more logical replication target names (the infix
    # portion of a snapshot name).
    # A destination host will 'pull' replicate snapshots for all targets that map to its --localhost name. Removing a mapping
    # can be used to temporarily suspend replication to a given destination host.
    # dst_hosts = {
    #     "nas": ["onsite"],
    #     "bak-us-west-1": ["us-west-1"],
    #     "bak-eu-west-1": ["eu-west-1"],
    #     "hotspare": ["hotspare"],
    #     "archive": ["offsite"],
    # }
    # dst_hosts = {"nas": ["onsite"]}
    # dst_hosts = {"nas": [""]}  # empty string as target name is ok
    dst_hosts = {"nas": ["", "onsite"]}

    # Dictionary that maps each destination hostname to a list of zero or more logical replication target names (the infix
    # portion of a snapshot name).
    # Has same format as dst_hosts. As part of --prune-dst-snapshots, a destination host will delete any snapshot it has
    # stored whose target has no mapping to its --localhost in this dictionary.
    # Do not remove a mapping unless you are sure it's ok to delete all those snapshots on that destination host! If in
    # doubt, use --dryrun mode first.
    # retain_dst_targets = dst_hosts
    retain_dst_targets = {"nas": ["", "onsite"]}

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
    #     "nas": f"tank2/bak/{src_host}",
    #     "bak-us-west-1": f"backups/bak001/{src_host}",
    #     "bak-eu-west-1": f"backups/bak999/{src_host}",
    #     "archive": f"archives/zoo/{src_host}",  # force use of a separate destination root dataset per source host
    # }
    dst_root_datasets = {
        "nas": f"tank2/bak/{src_host}",
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
    #         "offsite": {"12hourly": 14, "weekly": 12},  # take snapshots every 12 hours, and weekly
    #         "onsite": {"100millisecondly": 40},  # take snapshots every 100 milliseconds
    #     },
    # }
    # src_snapshot_plan = {  # empty string as target name is ok
    #     org: {"": {"secondly": 40, "minutely": 40, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5}}
    # }
    src_snapshot_plan = {
        org: {
            "onsite": {"secondly": 40, "minutely": 40, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5}
        }
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

    # Alert the user if the ZFS 'creation' time property of the latest or oldest snapshot for any specified snapshot name
    # pattern within the selected datasets is too old wrt. the specified age limit. The purpose is to check if snapshots
    # are successfully taken on schedule, successfully replicated on schedule, and successfully pruned on schedule.
    # Process exit code is 0, 1, 2 on OK, WARNING, CRITICAL, respectively.
    # The example below alerts the user if the latest src or dst snapshot named `prod_onsite_<timestamp>_hourly` is more than 30
    # minutes late (i.e. more than 30+60=90 minutes old) [warning], or more than 300 minutes late (i.e. more than 300+60=360
    # minutes old) [critical].
    # In addition, the example alerts the user if the oldest src or dst snapshot named `prod_onsite_<timestamp>_hourly` is more
    # than 30 + 60x36 minutes old [warning] or more than 300 + 60x36 minutes old [critical], where 36 is the number of period
    # cycles specified in `src_snapshot_plan` or `dst_snapshot_plan`, respectively.
    # Analog for the latest snapshot named `prod_<timestamp>_daily`, and so on.
    # Note: A duration that is missing or zero (e.g. '0 minutes') indicates that no snapshots shall be checked for the given
    # snapshot name pattern.
    # monitor_snapshot_plan = {
    #     org: {
    #         "onsite": {
    #             "100millisecondly": {"warning": "650 milliseconds", "critical": "2 seconds"},
    #             "secondly": {"warning": "2 seconds", "critical": "14 seconds"},
    #             "minutely": {"warning": "30 seconds", "critical": "300 seconds"},
    #             "hourly": {"warning": "30 minutes", "critical": "300 minutes"},
    #             "daily": {"warning": "4 hours", "critical": "8 hours"},
    #             "weekly": {"warning": "2 days", "critical": "8 days"},
    #             "monthly": {"warning": "2 days", "critical": "8 days"},
    #             "yearly": {"warning": "5 days", "critical": "14 days"},
    #         },
    #         "": {
    #             "daily": {"warning": "4 hours", "critical": "8 hours"},
    #         },
    #     },
    # }
    monitor_snapshot_plan = {
        org: {
            "onsite": {
                "hourly": {"warning": "30 minutes", "critical": "300 minutes"},
                "daily": {"warning": "4 hours", "critical": "8 hours"},
            },
        },
    }

    extra_args = []
    extra_args += [f"--log-dir={os.path.join(os.path.expanduser('~'), 'bzfs-job-logs', os.path.basename(sys.argv[0]))}"]
    # extra_args += ["--ssh-src-port=2222"]  # for hpnssh
    # extra_args += ["--ssh-dst-port=2222"]  # for hpnssh
    # extra_args += ["--localhost=bak-us-west-1"]
    # extra_args += ["--src-user=alice"]  # ssh username on src; for pull mode
    # extra_args += ["--dst-user=root"]  # ssh username on dst; for push mode
    # extra_args += ["--include-dataset", "foo", "zoo"]  # see bzfs --help
    # extra_args += ["--exclude-dataset", "bar", "baz"]
    # extra_args += ["--include-dataset-regex", "foo.*", "zoo.*"]
    # extra_args += ["--exclude-dataset-regex", "bar.*", "baz.*"]
    # extra_args += ["--create-src-snapshots-timeformat=%Y-%m-%d_%H:%M:%S"]  # this is already the default anyway
    # extra_args += ["--create-src-snapshots-timezone=UTC"]
    # extra_args += ["--zfs-send-program-opts=--props --raw --compressed"]  # this is already the default anyway
    # extra_args += ["--zfs-send-program-opts="]  # run 'zfs send' without options
    # extra_args += ["--zfs-recv-program-opts=-u"]  # this is already the default anyway
    # extra_args += ["--zfs-recv-program-opts=-u -o canmount=noauto -o readonly=on -x keylocation -x keyformat -x encryption"]
    # extra_args += ["--zfs-recv-program-opts="]  # run 'zfs receive' without options
    # extra_args += ["--force-rollback-to-latest-snapshot"]
    # extra_args += ["--force-rollback-to-latest-common-snapshot"]
    # extra_args += ["--force"]
    # extra_args += ["--force-destroy-dependents"]
    # extra_args += ["--force-unmount"]
    # extra_args += ["--force-once"]
    # extra_args += ["--skip-parent"]
    # extra_args += ["--skip-missing-snapshots=dataset"]  # this is already the default anyway
    # extra_args += ["--skip-missing-snapshots=fail"]
    # extra_args += ["--skip-missing-snapshots=continue"]
    # extra_args += ["--retries=2"]
    # extra_args += ["--retry-min-sleep-secs=0.125"]
    # extra_args += ["--retry-max-sleep-secs=300"]
    # extra_args += ["--retry-max-elapsed-secs=3600"]
    # extra_args += ["--skip-on-error=dataset"]  # this is already the default anyway
    # extra_args += ["--skip-on-error=fail"]
    # extra_args += ["--skip-on-error=tree"]
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
    # extra_args += ["--log-subdir=daily"]  # this is already the default anyway
    # extra_args += ["--log-subdir=hourly"]
    # extra_args += ["--log-subdir=minutely"]
    # ... and so on (include all other options from bzfs --help here too)
    # extra_args += ["--monitor-snapshots-dont-warn"]
    # extra_args += ["--monitor-snapshots-dont-crit"]
    # os.environ["TZ"] = "UTC"  # change timezone in all respects for the entire program

    # Taking snapshots, and/or replicating, from every N milliseconds to every 10 seconds or so is considered high frequency.
    # For such cases, use daemon mode and consider adding these options to improve performance:
    # extra_args += ["--daemon-lifetime=300seconds"]  # daemon exits after this much time has elapsed
    # extra_args += ["--daemon-lifetime=86400seconds"]  # daemon exits after this much time has elapsed
    # extra_args += ["--daemon-replication-frequency=secondly"]  # replicate every second
    # extra_args += ["--daemon-replication-frequency=750millisecondly"]  # replicate every 750 milliseconds
    # extra_args += ["--daemon-prune-src-frequency=5secondly"]  # prune src snapshots and src bookmarks every 5 seconds
    # extra_args += ["--daemon-prune-dst-frequency=10secondly"]  # prune dst snapshots every 10 seconds
    # extra_args += ["--daemon-monitor-snapshots-frequency=minutely"]  # monitor snapshots every minute
    # extra_args += ["--daemon-monitor-snapshots-frequency=secondly"]  # monitor snapshots every second
    # extra_args += ["--create-src-snapshots-timeformat=%Y-%m-%d_%H:%M:%S.%f"]  # adds microseconds to snapshot names
    # extra_args += ["--create-src-snapshots-even-if-not-due"]  # nomore run 'zfs list -t snapshot' before snapshot creation
    # extra_args += ["--create-src-snapshots-enable-snapshots-changed-cache"]  # no 'zfs list -t snapshot' before snap creation
    # extra_args += ["--no-resume-recv"]  # skip 'zfs list' check for resume token
    # extra_args += ["--no-estimate-send-size"]  # skip 'zfs send -n -v'
    # extra_args += ["--pv-program=-"]  # nomore measure via 'pv' / stats
    # extra_args += ["--ps-program=-"]  # disable safety check for simultaneous 'zfs receive' to same dataset by another process
    # extra_args += ["--mbuffer-program=-"]  # shorten latency of queue handoffs and disable large buffer size allocations
    # extra_args += ["--compression-program=-"]  # save CPU by disabling compression-on-the-wire
    # extra_args += ["--compression-program=lz4"]  # use less CPU for compression-on-the-wire
    # extra_args += ["--threads=200%"]  # use more CPU cores when operating on many datasets
    # extra_args += ["--ssh-program=hpnssh"]  # use larger TCP window size over high speed long distance networks
    # os.environ["bzfs_name_of_a_unix_env_var"] = "true"  # toggle example tuning knob

    cmd = ["bzfs_jobrunner"]
    cmd += ["--recursive"] if recursive else []
    cmd += [f"--src-host={src_host}"]
    cmd += [f"--dst-hosts={dst_hosts}"]
    cmd += [f"--retain-dst-targets={retain_dst_targets}"]
    cmd += [f"--dst-root-datasets={dst_root_datasets}"]
    cmd += [f"--src-snapshot-plan={src_snapshot_plan}"]
    cmd += [f"--src-bookmark-plan={src_bookmark_plan}"]
    cmd += [f"--dst-snapshot-plan={dst_snapshot_plan}"]
    cmd += [f"--monitor-snapshot-plan={monitor_snapshot_plan}"]
    cmd += [f"--job-id={jobid_prefix}_{i:05}"]
    cmd += extra_args + unknown_args + ["--root-dataset-pairs"] + root_dataset_pairs
    commands.append(cmd)


# Max worker jobs to run in parallel at any time; specified as a positive integer, or as a percentage of num CPU cores:
workers = "100%"  # aka max_workers = 1.0 * num_cores
# workers = "200%"  # aka max_workers = 2.0 * num_cores
# workers = "25%"  # aka max_workers = 0.25 * num_cores
# workers = 4  # aka max_workers = 4
# workers = 1  # aka max_workers = 1


# Reduce bandwidth spikes by evenly spreading the start of worker jobs over this much time; 0 disables this feature:
# work_period_seconds = 60
# work_period_seconds = 86400
work_period_seconds = 0


# If this much time has passed after a worker process has started executing, kill the straggling worker. Other workers remain
# unaffected. A value of None disables this feature:
# worker_timeout_seconds = 3600
worker_timeout_seconds = None


scheduler_cmd = ["bzfs_jobscheduler"]
scheduler_cmd += [f"--workers={workers}"]
scheduler_cmd += [f"--work-period-seconds={work_period_seconds}"]
scheduler_cmd += [] if worker_timeout_seconds is None else [f"--worker-timeout-seconds={worker_timeout_seconds}"]
sys.exit(subprocess.run(scheduler_cmd, input=f"{commands}", text=True).returncode)
