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

# Inline script metadata conforming to https://packaging.python.org/specifications/inline-script-metadata
# /// script
# requires-python = ">=3.8"
# dependencies = []
# ///

"""WARNING: For now, `bzfs_jobrunner` is work-in-progress, and as such may still change in incompatible ways."""

import argparse
import ast
import contextlib
import importlib.machinery
import importlib.util
import os
import pwd
import shutil
import socket
import subprocess
import sys
import threading
import time
import types
import uuid
from logging import Logger
from subprocess import DEVNULL, PIPE
from typing import Dict, List, Optional, Set, Tuple, Iterable

prog_name = "bzfs_jobrunner"
src_magic_substitution_token = "^SRC_HOST"
dst_magic_substitution_token = "^DST_HOST"


def argument_parser() -> argparse.ArgumentParser:
    # fmt: off
    parser = argparse.ArgumentParser(
        prog=prog_name,
        allow_abbrev=False,
        formatter_class=argparse.RawTextHelpFormatter,
        description=f"""
WARNING: For now, `bzfs_jobrunner` is work-in-progress, and as such may still change in incompatible ways.

This program is a convenience wrapper around [bzfs](README.md) that simplifies periodic ZFS snapshot creation, replication,
pruning, and monitoring, across N source hosts and M destination hosts, using a single shared
[jobconfig](bzfs_tests/bzfs_job_example.py) script.
For example, this simplifies the deployment of an efficient geo-replicated backup service where each of the M destination
hosts is located in a separate geographic region and receives replicas from (the same set of) N source hosts. It also
simplifies low latency replication from a primary to a secondary, or backup to removable drives, etc.

This program can be used to efficiently replicate ...

a) within a single machine, or

b) from a single source host to one or more destination hosts (pull or push or pull-push mode), or

c) from multiple source hosts to a single destination host (pull or push or pull-push mode), or

d) from N source hosts to M destination hosts (pull or push or pull-push mode, N can be large, M=2 or M=3 are typical
geo-replication factors)

You can run this program on a single third-party host and have that talk to all source hosts and destination hosts, which is
convenient for basic use cases and for testing.
However, typically, a cron job on each source host runs `{prog_name}` periodically to create new snapshots (via
--create-src-snapshots) and prune outdated snapshots and bookmarks on the source (via --prune-src-snapshots and
--prune-src-bookmarks), whereas another cron job on each destination host runs `{prog_name}` periodically to prune
outdated destination snapshots (via --prune-dst-snapshots), and to replicate the recently created snapshots from the source
to the destination (via --replicate).
Yet another cron job on each source and each destination runs `{prog_name}` periodically to alert the user if the latest or
oldest snapshot is somehow too old (via --monitor-src-snapshots and --monitor-dst-snapshots).
The frequency of these periodic activities can vary by activity, and is typically every second, minute, hour, day, week,
month and/or year (or multiples thereof).

Edit the jobconfig script in a central place (e.g. versioned in a git repo), then copy the (very same) shared file onto all
source hosts and all destination hosts, and add crontab entries (or systemd timers or Monit entries or similar), along these
lines:

* crontab on source hosts:

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --src-host="$(hostname)" --create-src-snapshots --prune-src-snapshots --prune-src-bookmarks`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --src-host="$(hostname)" --monitor-src-snapshots`


* crontab on destination hosts:

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --dst-host="$(hostname)" --replicate --prune-dst-snapshots`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --dst-host="$(hostname)" --monitor-dst-snapshots`

### High Frequency Replication (Experimental Feature)

Taking snapshots, and/or replicating, from every N milliseconds to every 10 seconds or so is considered high frequency. For
such use cases, consider that `zfs list -t snapshot` performance degrades as more and more snapshots currently exist within
the selected datasets, so try to keep the number of currently existing snapshots small, and prune them at a frequency that
is proportional to the frequency with which snapshots are created. Consider using `--skip-parent` and `--exclude-dataset*`
filters to limit the selected datasets only to those that require this level of frequency.

In addition, use the `--daemon-*` options to reduce startup overhead, in combination with splitting the crontab entry (or
better: high frequency systemd timer) into multiple processes, from a single source host to a single destination host,
along these lines:

* crontab on source hosts:

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --src-host="$(hostname)" --dst-host="foo" --create-src-snapshots`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --src-host="$(hostname)" --dst-host="foo" --prune-src-snapshots`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --src-host="$(hostname)" --dst-host="foo" --prune-src-bookmarks`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --src-host="$(hostname)" --dst-host="foo" --monitor-src-snapshots`


* crontab on destination hosts:

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --src-host="bar" --dst-host="$(hostname)" --replicate`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --src-host="bar" --dst-host="$(hostname)" --prune-dst-snapshots`

`* * * * * testuser /etc/bzfs/bzfs_job_example.py --src-host="bar" --dst-host="$(hostname)" --monitor-dst-snapshots`

The daemon processes work like non-daemon processes except that they loop, handle time events and sleep between events, and
finally exit after, say, 86400 seconds (whatever you specify via `--daemon-lifetime`). The daemons will subsequently be
auto-restarted by 'cron', or earlier if they fail. While the daemons are running, 'cron' will attempt to start new
(unnecessary) daemons but this is benign as these new processes immediately exit with a message like this:
"Exiting as same previous periodic job is still running without completion yet"
""")

    # commands:
    parser.add_argument(
        "--create-src-snapshots", action="store_true",
        help="Take snapshots on the selected source hosts as necessary. Typically, this command should be called by a "
             "program (or cron job) running on each src host.\n\n")
    parser.add_argument(
        "--replicate", choices=["pull", "push"], default=None, const="pull", nargs="?", metavar="",
        help="Replicate snapshots from the selected source hosts to the selected destinations hosts as necessary. For pull "
             "mode (recommended), this command should be called by a program (or cron job) running on each dst "
             "host; for push mode, on the src host; for pull-push mode on a third-party host.\n\n")
    parser.add_argument(
        "--prune-src-snapshots", action="store_true",
        help="Prune snapshots on the selected source hosts as necessary. Typically, this command should be called by a "
             "program (or cron job) running on each src host.\n\n")
    parser.add_argument(
        "--prune-src-bookmarks", action="store_true",
        help="Prune bookmarks on the selected source hosts as necessary. Typically, this command should be called by a "
             "program (or cron job) running on each src host.\n\n")
    parser.add_argument(
        "--prune-dst-snapshots", action="store_true",
        help="Prune snapshots on the selected destination hosts as necessary. Typically, this command should be called by a "
             "program (or cron job) running on each dst host.\n\n")
    parser.add_argument(
        "--monitor-src-snapshots", action="store_true",
        help="Alert the user if snapshots on the selected source hosts are too old, using --monitor-snapshot-plan (see "
             "below). Typically, this command should be called by a program (or cron job) running on each src host.\n\n")
    parser.add_argument(
        "--monitor-dst-snapshots", action="store_true",
        help="Alert the user if snapshots on the selected destination hosts are too old, using --monitor-snapshot-plan (see "
             "below). Typically, this command should be called by a program (or cron job) running on each dst host.\n\n")

    # options:
    parser.add_argument(
        "--localhost", default=None, action=bzfs.NonEmptyStringAction, metavar="STRING",
        help="Hostname of localhost. Default is the hostname without the domain name, querying the Operating System.\n\n")
    parser.add_argument(
        "--src-hosts", default=None, metavar="LIST_STRING",
        help="Hostnames of the sources to operate on.\n\n")
    parser.add_argument(
        "--src-host", default=None, action="append", metavar="STRING",
        help="For subsetting --src-hosts; Can be specified multiple times; Indicates to only use the --src-hosts that are "
             "contained in the specified --src-host values (optional).\n\n")
    dst_hosts_example = {"nas": ["onsite"], "bak-us-west-1": ["us-west-1"],
                         "bak-eu-west-1": ["eu-west-1"], "archive": ["offsite"]}
    parser.add_argument(
        "--dst-hosts", default="{}", metavar="DICT_STRING",
        help="Dictionary that maps each destination hostname to a list of zero or more logical replication target names "
             "(the infix portion of snapshot name). "
             f"Example: `{format_dict(dst_hosts_example)}`.\n\n"
             "With this, given a snapshot name, we can find the destination hostname to which the snapshot shall be "
             "replicated. Also, given a snapshot name and its own name, a destination host can determine if it shall "
             "replicate the given snapshot from the source host, or if the snapshot is intended for another destination "
             f"host, in which case it skips the snapshot. A destination host running {prog_name} will receive replicas of "
             "snapshots for all targets that map to that destination host.\n\n")
    parser.add_argument(
        "--dst-host", default=None, action="append", metavar="STRING",
        help="For subsetting --dst-hosts; Can be specified multiple times; Indicates to only use the --dst-hosts keys that "
             "are contained in the specified --dst-host values (optional).\n\n")
    parser.add_argument(
        "--retain-dst-targets", default="{}", metavar="DICT_STRING",
        help="Dictionary that maps each destination hostname to a list of zero or more logical replication target names "
             "(the infix portion of snapshot name). "
             f"Example: `{format_dict(dst_hosts_example)}`. Has same format as --dst-hosts.\n\n"
             "As part of --prune-dst-snapshots, a destination host will delete any snapshot it has stored whose target has "
             "no mapping to that destination host in this dictionary. Do not remove a mapping here unless you are sure it's "
             "ok to delete all those snapshots on that destination host! If in doubt, use --dryrun mode first.\n\n")
    dst_root_datasets_example = {
        "nas": "tank2/bak",
        "bak-us-west-1": "backups/bak001",
        "bak-eu-west-1": "backups/bak999",
        "archive": f"archives/zoo/{src_magic_substitution_token}",
        "hotspare": "",
    }
    parser.add_argument(
        "--dst-root-datasets", default="{}", metavar="DICT_STRING",
        help="Dictionary that maps each destination hostname to a root dataset located on that destination host. The root "
             "dataset name is an (optional) prefix that will be prepended to each dataset that is replicated to that "
             "destination host. For backup use cases, this is the backup ZFS pool or a ZFS dataset path within that pool, "
             "whereas for cloning, master slave replication, or replication from a primary to a secondary, this can also be "
             "the empty string.\n\n"
             f"`{src_magic_substitution_token}` and `{dst_magic_substitution_token}` are optional magic substitution tokens "
             "that will be auto-replaced at runtime with the actual hostname. This can be used to force the use of a "
             "separate destination root dataset per source host or per destination host.\n\n"
             f"Example: `{format_dict(dst_root_datasets_example)}`\n\n")
    src_snapshot_plan_example = {
        "prod": {
            "onsite": {"secondly": 40, "minutely": 40, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5},
            "us-west-1": {"secondly": 0, "minutely": 0, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5},
            "eu-west-1": {"secondly": 0, "minutely": 0, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5},
        },
        "test": {
            "offsite": {"12hourly": 42, "weekly": 12},
        },
    }
    parser.add_argument(
        "--src-snapshot-plan", default="{}", metavar="DICT_STRING",
        help="Retention periods for snapshots to be used if pruning src, and when creating new snapshots on src. "
             "Snapshots that do not match a retention period will be deleted. A zero within a retention period indicates "
             "that no snapshots shall be retained (or even be created) for the given period.\n\n"
             f"Example: `{format_dict(src_snapshot_plan_example)}`. This example will, for the organization 'prod' and "
             "the intended logical target 'onsite', create and then retain secondly snapshots that were created less "
             "than 40 seconds ago, yet retain the latest 40 secondly snapshots regardless of creation time. Analog for "
             "the latest 40 minutely snapshots, 36 hourly snapshots, etc. "
             "It will also create and retain snapshots for the targets 'us-west-1' and 'eu-west-1' within the 'prod' "
             "organization. "
             "In addition, it will create and retain snapshots every 12 hours and every week for the 'test' organization, "
             "and name them as being intended for the 'offsite' replication target. "
             "The example creates snapshots with names like "
             "`prod_onsite_<timestamp>_secondly`, `prod_onsite_<timestamp>_minutely`, "
             "`prod_us-west-1_<timestamp>_hourly`, `prod_us-west-1_<timestamp>_daily`, "
             "`prod_eu-west-1_<timestamp>_hourly`, `prod_eu-west-1_<timestamp>_daily`, "
             "`test_offsite_<timestamp>_12hourly`, `test_offsite_<timestamp>_weekly`, and so on.\n\n")
    parser.add_argument(
        "--src-bookmark-plan", default="{}", metavar="DICT_STRING",
        help="Retention periods for bookmarks to be used if pruning src. Has same format as --src-snapshot-plan.\n\n")
    parser.add_argument(
        "--dst-snapshot-plan", default="{}", metavar="DICT_STRING",
        help="Retention periods for snapshots to be used if pruning dst. Has same format as --src-snapshot-plan.\n\n")
    monitor_snapshot_plan_example = {
        "prod": {
            "onsite": {
                "100millisecondly": {"warning": "650 milliseconds", "critical": "2 seconds"},
                "secondly": {"warning": "2 seconds", "critical": "14 seconds"},
                "minutely": {"warning": "30 seconds", "critical": "300 seconds"},
                "hourly": {"warning": "30 minutes", "critical": "300 minutes"},
                "daily": {"warning": "4 hours", "critical": "8 hours"},
                "weekly": {"warning": "2 days", "critical": "8 days"},
                "monthly": {"warning": "2 days", "critical": "8 days"},
                "yearly": {"warning": "5 days", "critical": "14 days"},
                "10minutely": {"warning": "0 minutes", "critical": "0 minutes"},
            },
            "": {
                "daily": {"warning": "4 hours", "critical": "8 hours"},
            },
        },
    }
    parser.add_argument(
        "--monitor-snapshot-plan", default="{}", metavar="DICT_STRING",
        help="Alert the user if the ZFS 'creation' time property of the latest or oldest snapshot for any specified "
             "snapshot pattern within the selected datasets is too old wrt. the specified age limit. The purpose is to "
             "check if snapshots are successfully taken on schedule, successfully replicated on schedule, and successfully "
             "pruned on schedule. "
             "Process exit code is 0, 1, 2 on OK, WARNING, CRITICAL, respectively. "
             f"Example DICT_STRING: `{format_dict(monitor_snapshot_plan_example)}`. "
             "This example alerts the user if the latest src or dst snapshot named `prod_onsite_<timestamp>_hourly` is more "
             "than 30 minutes late (i.e. more than 30+60=90 minutes old) [warning] or more than 300 minutes late (i.e. more "
             "than 300+60=360 minutes old) [critical]. In addition, the example alerts the user if the oldest src or dst "
             "snapshot named `prod_onsite_<timestamp>_hourly` is more than 30 + 60x36 minutes old [warning] or more than "
             "300 + 60x36 minutes old [critical], where 36 is the number of period cycles specified in `src_snapshot_plan` "
             "or `dst_snapshot_plan`, respectively. "
             "Analog for the latest snapshot named `prod_<timestamp>_daily`, and so on.\n\n"
             "Note: A duration that is missing or zero (e.g. '0 minutes') indicates that no snapshots shall be checked for "
             "the given snapshot name pattern.\n\n")
    parser.add_argument(
        "--src-user", default="", metavar="STRING",
        help="SSH username on src hosts. Used in pull mode and pull-push mode. Example: 'alice'\n\n")
    parser.add_argument(
        "--dst-user", default="", metavar="STRING",
        help="SSH username on dst hosts. Used in push mode and pull-push mode. Example: 'root'\n\n")
    parser.add_argument(
        "--job-id", default=None, metavar="STRING",
        help="The job identifier that shall be included in the log file name suffix. Default is a hex UUID. "
             "Example: 0badc0f003a011f0a94aef02ac16083c\n\n")
    parser.add_argument(
        "--jobid", default=None, metavar="STRING",
        help=argparse.SUPPRESS)   # deprecated; was renamed to --job-id
    workers_default = 100  # percent
    parser.add_argument(
        "--workers", min=1, default=(workers_default, True), action=bzfs.CheckPercentRange, metavar="INT[%]",
        help="The maximum number of jobs to run in parallel at any time; can be given as a positive integer, "
             f"optionally followed by the %% percent character (min: 1, default: {workers_default}%%). Percentages "
             "are relative to the number of CPU cores on the machine. Example: 200%% uses twice as many parallel jobs as "
             "there are cores on the machine; 75%% uses num_procs = num_cores * 0.75. Examples: 1, 4, 75%%, 150%%\n\n")
    parser.add_argument(
        "--work-period-seconds", type=float, min=0, default=0, action=bzfs.CheckRange, metavar="FLOAT",
        help="Reduces bandwidth spikes by evenly spreading the start of worker jobs over this much time; "
             "0 disables this feature (default: %(default)s). Examples: 0, 60, 86400\n\n")
    parser.add_argument(
        "--worker-timeout-seconds", type=float, min=0, default=None, action=bzfs.CheckRange, metavar="FLOAT",
        help="If this much time has passed after a worker process has started executing, kill the straggling worker "
             "(optional). Other workers remain unaffected. Examples: 60, 3600\n\n")
    parser.add_argument(
        "--spawn_process_per_job", action="store_true",
        help=argparse.SUPPRESS)
    parser.add_argument(
        "--jobrunner-dryrun", action="store_true",
        help="Do a dry run (aka 'no-op') to print what operations would happen if the command were to be executed "
             "for real (optional). This option treats both the ZFS source and destination as read-only. Can also be used to "
             "check if the configuration options are valid.\n\n")
    parser.add_argument(
        "--jobrunner-log-level", choices=["CRITICAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"], default="INFO",
        help="Only emit jobrunner messages with equal or higher priority than this log level. Default is 'INFO'.\n\n")
    parser.add_argument(
        "--help, -h", action="help",
        help="Show this help message and exit.\n\n")
    parser.add_argument(
        "--daemon-replication-frequency", default="minutely", metavar="STRING",
        help="Specifies how often the bzfs daemon shall replicate from src to dst if --daemon-lifetime is nonzero.\n\n")
    parser.add_argument(
        "--daemon-prune-src-frequency", default="minutely", metavar="STRING",
        help="Specifies how often the bzfs daemon shall prune src if --daemon-lifetime is nonzero.\n\n")
    parser.add_argument(
        "--daemon-prune-dst-frequency", default="minutely", metavar="STRING",
        help="Specifies how often the bzfs daemon shall prune dst if --daemon-lifetime is nonzero.\n\n")
    parser.add_argument(
        "--daemon-monitor-snapshots-frequency", default="minutely", metavar="STRING",
        help="Specifies how often the bzfs daemon shall monitor snapshot age if --daemon-lifetime is nonzero.\n\n")
    parser.add_argument(
        "--root-dataset-pairs", required=True, nargs="+", action=bzfs.DatasetPairsAction, metavar="SRC_DATASET DST_DATASET",
        help="Source and destination dataset pairs (excluding usernames and excluding hostnames, which will all be "
             "auto-appended later).\n\n")
    return parser
    # fmt: on


def load_module(progname: str) -> types.ModuleType:
    prog_path = shutil.which(progname)
    if not prog_path:
        sibling_prog_path = os.path.join(os.path.dirname(sys.argv[0]), progname)
        prog_path = sibling_prog_path if os.path.isfile(sibling_prog_path) else prog_path
    assert prog_path, f"{progname}: command not found on PATH"
    prog_path = os.path.realpath(prog_path)  # resolve symlink, if any
    loader = importlib.machinery.SourceFileLoader(progname, prog_path)
    spec = importlib.util.spec_from_loader(progname, loader)
    module = importlib.util.module_from_spec(spec)
    if spec.name not in sys.modules:
        sys.modules[spec.name] = module
    loader.exec_module(module)
    if hasattr(module, "run_main"):
        return module
    else:  # It's a wrapper script as `bzfs` was installed as a package by 'pip install'; load that installed package
        return importlib.import_module(f"{progname}.{progname}")


# constants:
bzfs: types.ModuleType = load_module("bzfs")
die_status = bzfs.die_status
dummy_dataset = bzfs.dummy_dataset
sep = ","


#############################################################################
def main():
    Job().run_main(sys.argv)


#############################################################################
class Job:
    def __init__(self, log: Optional[Logger] = None):
        # immutable variables:
        self.jobrunner_dryrun: bool = False
        self.spawn_process_per_job: Optional[bool] = None
        self.log: Logger = log if log is not None else bzfs.get_simple_logger(prog_name)
        self.bzfs_argument_parser: argparse.ArgumentParser = bzfs.argument_parser()
        self.argument_parser: argparse.ArgumentParser = argument_parser()

        # mutable variables:
        self.first_exception: Optional[int] = None
        self.stats: Stats = Stats()
        self.cache_existing_dst_pools: Set[str] = set()
        self.cache_known_dst_pools: Set[str] = set()

    def run_main(self, sys_argv: List[str]) -> None:
        self.first_exception = None
        log = self.log
        log.info(
            "WARNING: For now, `bzfs_jobrunner` is work-in-progress, and as such may still change in incompatible ways."
        )
        log.info("CLI arguments: %s", " ".join(sys_argv))
        args, unknown_args = self.argument_parser.parse_known_args(sys_argv[1:])  # forward all unknown args to `bzfs`
        log.setLevel(args.jobrunner_log_level)
        self.jobrunner_dryrun = args.jobrunner_dryrun
        src_snapshot_plan = validate_snapshot_plan(ast.literal_eval(args.src_snapshot_plan), "--src-snapshot-plan")
        src_bookmark_plan = validate_snapshot_plan(ast.literal_eval(args.src_bookmark_plan), "--src-bookmark-plan")
        dst_snapshot_plan = validate_snapshot_plan(ast.literal_eval(args.dst_snapshot_plan), "--dst-snapshot-plan")
        monitor_snapshot_plan = validate_monitor_snapshot_plan(ast.literal_eval(args.monitor_snapshot_plan))
        localhostname = args.localhost if args.localhost else socket.gethostname()
        assert localhostname, "localhostname must not be empty!"
        log.debug("localhostname: %s", localhostname)
        src_hosts = validate_src_hosts(ast.literal_eval(args.src_hosts if args.src_hosts is not None else sys.stdin.read()))
        dst_hosts = validate_dst_hosts(ast.literal_eval(args.dst_hosts))
        if args.src_host is not None:  # retain only the src hosts that are also contained in args.src_host
            retain_src_hosts = set(args.src_host).difference({"^NONE"})
            validate_is_subset(retain_src_hosts, src_hosts, "--src-host", "--src-hosts")
            src_hosts = [host for host in src_hosts if host in retain_src_hosts]
        if args.dst_host is not None:  # retain only the dst hosts that are also contained in args.dst_host
            retain_dst_hosts = set(args.dst_host).difference({"^NONE"})
            validate_is_subset(retain_dst_hosts, dst_hosts.keys(), "--dst-host", "--dst-hosts.keys")
            dst_hosts = {dst_host: lst for dst_host, lst in dst_hosts.items() if dst_host in retain_dst_hosts}
        retain_dst_targets = validate_dst_hosts(ast.literal_eval(args.retain_dst_targets))
        validate_is_subset(dst_hosts.keys(), retain_dst_targets.keys(), "--dst-hosts.keys", "--retain-dst-targets.keys")
        dst_root_datasets = validate_dst_root_datasets(ast.literal_eval(args.dst_root_datasets))
        validate_is_subset(
            dst_root_datasets.keys(), retain_dst_targets.keys(), "--dst-root-dataset.keys", "--retain-dst-targets.keys"
        )
        validate_is_subset(dst_hosts.keys(), dst_root_datasets.keys(), "--dst-hosts.keys", "--dst-root-dataset.keys")
        assert not (
            len(src_hosts) > 1
            and any(src_magic_substitution_token not in dst_root_datasets[dst_host] for dst_host in dst_hosts)
        ), "Multiple sources must not write to the same destination dataset"
        jobid = args.job_id if args.job_id is not None else args.jobid  # --jobid is deprecated
        jobid = sanitize(jobid) if jobid else uuid.uuid1().hex
        workers, workers_is_percent = args.workers
        max_workers = max(1, round(os.cpu_count() * workers / 100.0) if workers_is_percent else round(workers))
        worker_timeout_seconds = args.worker_timeout_seconds
        if args.spawn_process_per_job:
            self.spawn_process_per_job = args.spawn_process_per_job
        username = pwd.getpwuid(os.geteuid()).pw_name
        assert username

        def zero_pad(number: int, width: int = 6) -> str:
            return f"{number:0{width}d}"  # pad number with leading '0' chars to the given width

        def ipad() -> str:
            return zero_pad(i)

        def jpad(jj: int) -> str:
            return zero_pad(jj)

        def npad() -> str:
            return sep + zero_pad(len(subjobs))

        def update_subjob_name(tag: str) -> str:
            if j <= 0:
                return subjob_name
            elif j == 1:
                return subjob_name + f"/{jpad(j-1)}{tag}"
            else:
                return subjob_name + "/" + bzfs.BARRIER_CHAR

        def resolve_dataset(hostname: str, dataset: str, is_src: bool = True) -> str:
            ssh_user = args.src_user if is_src else args.dst_user
            ssh_user = ssh_user if ssh_user else username
            hostname = hostname if hostname != localhostname else "127.0.0.1" if username != ssh_user else "-"
            return f"{hostname}:{dataset}"

        def resolve_dst_dataset(dst_hostname: str, dst_dataset: str) -> str:
            root_dataset = dst_root_datasets.get(dst_hostname)
            assert root_dataset is not None, f"Hostname '{dst_hostname}' missing in --dst-root-datasets"
            root_dataset = root_dataset.replace(src_magic_substitution_token, src_host)
            root_dataset = root_dataset.replace(dst_magic_substitution_token, dst_hostname)
            dst_dataset = root_dataset + "/" + dst_dataset if root_dataset else dst_dataset
            return resolve_dataset(dst_hostname, dst_dataset, is_src=False)

        subjobs: Dict[str, List[str]] = {}
        for i, src_host in enumerate(src_hosts):
            subjob_name: str = ipad() + sanitize(src_host)

            if args.create_src_snapshots:
                opts = ["--create-src-snapshots", f"--create-src-snapshots-plan={src_snapshot_plan}", "--skip-replication"]
                opts += [f"--log-file-prefix={prog_name}{sep}create-src-snapshots{sep}"]
                opts += [f"--log-file-suffix={sep}{jobid}{npad()}{log_suffix(src_host, None)}{sep}"]
                opts += [f"--ssh-src-user={args.src_user}"] if args.src_user else []
                opts += unknown_args + ["--"]
                opts += dedupe([(resolve_dataset(src_host, src), dummy_dataset) for src, dst in args.root_dataset_pairs])
                subjob_name += "/create_src_snapshots"
                subjobs[subjob_name] = ["bzfs"] + opts

            if args.replicate:
                j = 0
                for dst_hostname, targets in dst_hosts.items():
                    opts = self.replication_opts(dst_snapshot_plan, set(targets), src_host, dst_hostname, jobid + npad())
                    if len(opts) > 0:
                        opts += [f"--ssh-src-user={args.src_user}"] if args.src_user else []
                        opts += [f"--ssh-dst-user={args.dst_user}"] if args.dst_user else []
                        opts += unknown_args + ["--"]
                        old_len_opts = len(opts)
                        pairs = [
                            (resolve_dataset(src_host, src), resolve_dst_dataset(dst_hostname, dst))
                            for src, dst in args.root_dataset_pairs
                        ]
                        for src, dst in self.skip_datasets_with_nonexisting_dst_pool(pairs):
                            opts += [src, dst]
                        if len(opts) > old_len_opts:
                            daemon_opts = [f"--daemon-frequency={args.daemon_replication_frequency}"]
                            subjobs[subjob_name + f"/{jpad(j)}replicate"] = ["bzfs"] + daemon_opts + opts
                            j += 1
                subjob_name = update_subjob_name(args.replicate)

            def prune_src(opts: List[str], retention_plan: Dict, tag: str) -> None:
                opts += [
                    "--skip-replication",
                    f"--delete-dst-snapshots-except-plan={retention_plan}",
                    f"--log-file-prefix={prog_name}{sep}{tag}{sep}",
                    f"--log-file-suffix={sep}{jobid}{npad()}{log_suffix(src_host, None)}{sep}",
                    f"--daemon-frequency={args.daemon_prune_src_frequency}",
                ]
                opts += [f"--ssh-dst-user={args.src_user}"] if args.src_user else []
                opts += unknown_args + ["--"]
                opts += dedupe([(dummy_dataset, resolve_dataset(src_host, src)) for src, dst in args.root_dataset_pairs])
                nonlocal subjob_name
                subjob_name += f"/{tag}"
                subjobs[subjob_name] = ["bzfs"] + opts

            if args.prune_src_snapshots:
                prune_src(["--delete-dst-snapshots"], src_snapshot_plan, "prune-src-snapshots")

            if args.prune_src_bookmarks:
                prune_src(["--delete-dst-snapshots=bookmarks"], src_bookmark_plan, "prune-src-bookmarks")

            if args.prune_dst_snapshots:
                assert (
                    retain_dst_targets
                ), "--retain-dst-targets must not be empty. Cowardly refusing to delete all snapshots!"
                j = 0
                for dst_hostname, targets in dst_hosts.items():
                    retain_targets = set(retain_dst_targets[dst_hostname])
                    dst_snapshot_plan = {  # only retain targets that belong to the host
                        org: {target: periods for target, periods in target_periods.items() if target in retain_targets}
                        for org, target_periods in dst_snapshot_plan.items()
                    }
                    opts = ["--delete-dst-snapshots", "--skip-replication"]
                    opts += [f"--delete-dst-snapshots-except-plan={dst_snapshot_plan}"]
                    opts += [f"--log-file-prefix={prog_name}{sep}prune-dst-snapshots{sep}"]
                    opts += [f"--log-file-suffix={sep}{jobid}{npad()}{log_suffix(src_host, dst_hostname)}{sep}"]
                    opts += [f"--daemon-frequency={args.daemon_prune_dst_frequency}"]
                    opts += [f"--ssh-dst-user={args.dst_user}"] if args.dst_user else []
                    opts += unknown_args + ["--"]
                    old_len_opts = len(opts)
                    pairs = [(dummy_dataset, resolve_dst_dataset(dst_hostname, dst)) for src, dst in args.root_dataset_pairs]
                    for src, dst in self.skip_datasets_with_nonexisting_dst_pool(pairs):
                        opts += [src, dst]
                    if len(opts) > old_len_opts:
                        subjobs[subjob_name + f"/{jpad(j)}prune_dst_snapshots"] = ["bzfs"] + opts
                        j += 1
                subjob_name = update_subjob_name("prune_dst_snapshots")

            def monitor_snapshots_opts(tag: str, monitor_plan: Dict, logsuffix: str) -> List[str]:
                opts = [f"--monitor-snapshots={monitor_plan}", "--skip-replication"]
                opts += [f"--daemon-frequency={args.daemon_monitor_snapshots_frequency}"]
                opts += [f"--log-file-prefix={prog_name}{sep}{tag}{sep}"]
                opts += [f"--log-file-suffix={sep}{jobid}{npad()}{logsuffix}{sep}"]
                return opts

            def build_monitor_plan(monitor_plan: Dict, snapshot_plan: Dict) -> Dict:
                return {
                    org: {
                        target: {
                            periodunit: {
                                "latest": alertdict,
                                "oldest": {
                                    **alertdict,
                                    "cycles": snapshot_plan.get(org, {}).get(target, {}).get(periodunit, 1),
                                },
                            }
                            for periodunit, alertdict in periods.items()
                        }
                        for target, periods in target_periods.items()
                    }
                    for org, target_periods in monitor_plan.items()
                }

            if args.monitor_src_snapshots:
                monitor_plan = build_monitor_plan(monitor_snapshot_plan, src_snapshot_plan)
                opts = monitor_snapshots_opts("monitor-src-snapshots", monitor_plan, log_suffix(src_host, None))
                opts += [f"--ssh-dst-user={args.src_user}"] if args.src_user else []
                opts += unknown_args + ["--"]
                opts += dedupe([(dummy_dataset, resolve_dataset(src_host, src)) for src, dst in args.root_dataset_pairs])
                subjob_name += "/monitor_src_snapshots"
                subjobs[subjob_name] = ["bzfs"] + opts

            if args.monitor_dst_snapshots:
                j = 0
                for dst_hostname, targets in dst_hosts.items():
                    targets = set(targets).intersection(set(retain_dst_targets[dst_hostname]))
                    monitor_plan = {  # only retain targets that belong to the host
                        org: {target: periods for target, periods in target_periods.items() if target in targets}
                        for org, target_periods in monitor_snapshot_plan.items()
                    }
                    monitor_plan = build_monitor_plan(monitor_plan, dst_snapshot_plan)
                    opts = monitor_snapshots_opts("monitor-dst-snapshots", monitor_plan, log_suffix(src_host, dst_hostname))
                    opts += [f"--ssh-dst-user={args.dst_user}"] if args.dst_user else []
                    opts += unknown_args + ["--"]
                    old_len_opts = len(opts)
                    pairs = [(dummy_dataset, resolve_dst_dataset(dst_hostname, dst)) for src, dst in args.root_dataset_pairs]
                    for src, dst in self.skip_datasets_with_nonexisting_dst_pool(pairs):
                        opts += [src, dst]
                    if len(opts) > old_len_opts:
                        subjobs[subjob_name + f"/{jpad(j)}monitor_dst_snapshots"] = ["bzfs"] + opts
                        j += 1
                subjob_name = update_subjob_name("monitor_dst_snapshots")

        log.trace("Ready to run subjobs: \n%s", pretty_print_formatter(subjobs))
        self.run_subjobs(subjobs, max_workers, worker_timeout_seconds, args.work_period_seconds)
        ex = self.first_exception
        if isinstance(ex, int):
            assert ex != 0
            sys.exit(ex)
        assert ex is None, ex
        log.info("Succeeded. Bye!")

    def replication_opts(
        self, dst_snapshot_plan: Dict, targets: Set[str], src_hostname: str, dst_hostname: str, jobid: str
    ) -> List[str]:
        log = self.log
        log.debug("%s", f"Replicating targets {sorted(targets)} from {src_hostname} to {dst_hostname} ...")
        include_snapshot_plan = {  # only replicate targets that belong to the destination host and are relevant
            org: {
                target: {
                    duration_unit: duration_amount
                    for duration_unit, duration_amount in periods.items()
                    if duration_amount > 0
                }
                for target, periods in target_periods.items()
                if target in targets
            }
            for org, target_periods in dst_snapshot_plan.items()
        }
        include_snapshot_plan = {  # only replicate orgs that have at least one relevant target_period
            org: target_periods
            for org, target_periods in include_snapshot_plan.items()
            if any(len(periods) > 0 for target, periods in target_periods.items())
        }
        opts = []
        if len(include_snapshot_plan) > 0:
            opts += [f"--include-snapshot-plan={include_snapshot_plan}"]
            opts += [f"--log-file-prefix={prog_name}{sep}replicate{sep}"]
            opts += [f"--log-file-suffix={sep}{jobid}{log_suffix(src_hostname, dst_hostname)}{sep}"]
        return opts

    def skip_datasets_with_nonexisting_dst_pool(self, root_dataset_pairs) -> List[Tuple[str, str]]:
        def zpool(dataset: str) -> str:
            return dataset.split("/", 1)[0]

        assert len(root_dataset_pairs) > 0
        unknown_dst_pools = {zpool(dst) for src, dst in root_dataset_pairs}
        unknown_dst_pools = unknown_dst_pools.difference(self.cache_known_dst_pools)
        # Here we treat a zpool as existing if the zpool isn't local, aka if it isn't prefixed with "-:". A remote host
        # will raise an appropriate error anyway if it turns out that the remote zpool doesn't actually exist.
        if len(unknown_dst_pools) > 0 and all(pool.startswith("-:") for pool in unknown_dst_pools):  # `zfs list` if local
            existing_pools = {pool[len("-:") :] for pool in unknown_dst_pools}
            cmd = "zfs list -t filesystem,volume -Hp -o name".split(" ") + sorted(existing_pools)
            existing_pools = set(subprocess.run(cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True).stdout.splitlines())
            existing_pools = {"-:" + pool for pool in existing_pools}
            self.cache_existing_dst_pools.update(existing_pools)  # union
        else:
            self.cache_existing_dst_pools.update(unknown_dst_pools)  # union
        self.cache_known_dst_pools.update(unknown_dst_pools)  # union
        results = []
        for src, dst in root_dataset_pairs:
            if zpool(dst) in self.cache_existing_dst_pools:
                results.append((src, dst))
            else:
                self.log.warning("Skipping dst dataset for which dst pool does not exist: %s", dst)
        return results

    def run_subjobs(
        self, subjobs: Dict[str, List[str]], max_workers: int, timeout_secs: Optional[float], work_period_seconds: float
    ) -> None:
        self.stats = Stats()
        self.stats.jobs_all = len(subjobs)
        log = self.log
        interval_nanos = round(1_000_000_000 * max(0.0, work_period_seconds) / max(1, len(subjobs)))
        sorted_subjobs = sorted(subjobs.keys())
        has_barrier = any(bzfs.BARRIER_CHAR in subjob.split("/") for subjob in sorted_subjobs)
        if self.spawn_process_per_job or has_barrier or bzfs.has_siblings(sorted_subjobs):  # siblings can run in parallel
            log.trace("%s", "spawn_process_per_job: True")
            helper = bzfs.Job()
            helper.params = bzfs.Params(self.bzfs_argument_parser.parse_args(args=["src", "dst", "--retries=0"]), log=log)

            helper.process_datasets_in_parallel_and_fault_tolerant(
                sorted_subjobs,
                process_dataset=lambda subjob, tid, retry: self.run_subjob(
                    subjobs[subjob], timeout_secs=timeout_secs, spawn_process_per_job=True
                ),
                skip_tree_on_error=lambda subjob: True,
                max_workers=max_workers,
                interval_nanos=lambda subjob: interval_nanos,
                task_name="Subjob",
            )
        else:
            log.trace("%s", "spawn_process_per_job: False")
            next_update_nanos = time.monotonic_ns()
            for subjob in sorted_subjobs:
                time.sleep(max(0, next_update_nanos - time.monotonic_ns()) / 1_000_000_000)
                next_update_nanos += interval_nanos
                if not self.run_subjob(subjobs[subjob], timeout_secs=timeout_secs, spawn_process_per_job=False):
                    break

    def run_subjob(self, cmd: List[str], timeout_secs: Optional[float], spawn_process_per_job: bool) -> bool:
        returncode = self.run_worker_job(cmd, timeout_secs=timeout_secs, spawn_process_per_job=spawn_process_per_job)
        if returncode != 0:
            with self.stats.lock:
                if self.first_exception is None:
                    self.first_exception = die_status if returncode is None else returncode
            return False
        return True

    def run_worker_job_in_current_thread(self, cmd: List[str], timeout_secs: Optional[float]) -> Optional[int]:
        if timeout_secs is not None:
            cmd = cmd[0:1] + [f"--timeout={round(1000 * timeout_secs)}milliseconds"] + cmd[1:]
        try:
            if not self.jobrunner_dryrun:
                self._bzfs_run_main(cmd)
            return 0
        except subprocess.CalledProcessError as e:
            return e.returncode
        except SystemExit as e:
            return e.code
        except:
            return die_status

    def _bzfs_run_main(self, cmd: List[str]) -> None:
        bzfs.run_main(self.bzfs_argument_parser.parse_args(cmd[1:]), cmd)

    def run_worker_job_spawn_process_per_job(self, cmd: List[str], timeout_secs: Optional[float]) -> Optional[int]:
        log = self.log
        if self.jobrunner_dryrun:
            return 0
        proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, text=True)  # run job in a separate subprocess
        try:
            proc.communicate(timeout=timeout_secs)  # Wait for the subprocess to exit
        except subprocess.TimeoutExpired:
            cmd_str = " ".join(cmd)
            log.error("%s", f"Terminating worker job as it failed to complete within {timeout_secs}s: {cmd_str}")
            proc.terminate()  # Sends SIGTERM signal to job subprocess
            assert isinstance(timeout_secs, float)
            timeout_secs = min(1.0, timeout_secs)
            try:
                proc.communicate(timeout=timeout_secs)  # Wait for the subprocess to exit
            except subprocess.TimeoutExpired:
                log.error("%s", f"Killing worker job as it failed to terminate within {timeout_secs}s: {cmd_str}")
                proc.kill()  # Sends SIGKILL signal to job subprocess because SIGTERM wasn't enough
                timeout_secs = min(0.025, timeout_secs)
                with contextlib.suppress(subprocess.TimeoutExpired):
                    proc.communicate(timeout=timeout_secs)  # Wait for the subprocess to exit
            return proc.returncode
        else:
            return proc.returncode

    def run_worker_job(self, cmd: List[str], timeout_secs: Optional[float], spawn_process_per_job: bool) -> Optional[int]:
        log = self.log
        returncode = None
        start_time_nanos = time.monotonic_ns()
        stats = self.stats
        cmd_str = " ".join(cmd)
        try:
            log.trace("Starting worker job: %s", cmd_str)
            with stats.lock:
                stats.jobs_started += 1
                stats.jobs_running += 1
                msg = str(stats)
            log.info("Progress: %s", msg)
            start_time_nanos = time.monotonic_ns()
            if spawn_process_per_job:
                returncode = self.run_worker_job_spawn_process_per_job(cmd, timeout_secs)
            else:
                returncode = self.run_worker_job_in_current_thread(cmd, timeout_secs)
        except BaseException as e:
            log.error("Worker job failed with unexpected exception: %s for command: %s", e, cmd_str)
            raise e
        else:
            elapsed_nanos = time.monotonic_ns() - start_time_nanos
            elapsed_human = bzfs.human_readable_duration(elapsed_nanos, separator="")
            if returncode != 0:
                log.error("Worker job failed with exit code %s in %s: %s", returncode, elapsed_human, cmd_str)
            else:
                log.debug("Worker job succeeded in %s: %s", elapsed_human, cmd_str)
            return returncode
        finally:
            elapsed_nanos = time.monotonic_ns() - start_time_nanos
            with stats.lock:
                stats.jobs_running -= 1
                stats.jobs_completed += 1
                stats.sum_elapsed_nanos += elapsed_nanos
                stats.jobs_failed += 1 if returncode != 0 else 0
                msg = str(stats)
            log.info("Progress: %s", msg)


#############################################################################
class Stats:
    def __init__(self):
        self.lock: threading.Lock = threading.Lock()
        self.jobs_all: int = 0
        self.jobs_started: int = 0
        self.jobs_completed: int = 0
        self.jobs_failed: int = 0
        self.jobs_running: int = 0
        self.sum_elapsed_nanos: int = 0

    def __repr__(self) -> str:
        def pct(number: int) -> str:
            return f"{number}={bzfs.human_readable_float(100 * number / self.jobs_all)}%"

        all, started, completed, failed = self.jobs_all, self.jobs_started, self.jobs_completed, self.jobs_failed
        running = self.jobs_running
        t = "avg_completion_time:" + bzfs.human_readable_duration(self.sum_elapsed_nanos / max(1, completed), separator="")
        return f"all:{all}, started:{pct(started)}, completed:{pct(completed)}, failed:{pct(failed)}, running:{running}, {t}"


#############################################################################
def dedupe(root_dataset_pairs: List[Tuple[str, str]]) -> List[str]:
    results = []
    for src, dst in dict.fromkeys(root_dataset_pairs).keys():
        results.append(src)
        results.append(dst)
    return results


def validate_src_hosts(src_hosts: List) -> List[str]:
    assert isinstance(src_hosts, list)
    for src_hostname in src_hosts:
        assert isinstance(src_hostname, str)
        assert src_hostname, "src hostname must not be empty!"
        bzfs.validate_host_name(src_hostname, "src host", extra_invalid_chars=":")
    return src_hosts


def validate_dst_hosts(dst_hosts: Dict) -> Dict:
    assert isinstance(dst_hosts, dict)
    for dst_hostname, targets in dst_hosts.items():
        assert isinstance(dst_hostname, str)
        assert dst_hostname, "dst hostname must not be empty!"
        bzfs.validate_host_name(dst_hostname, "dst host", extra_invalid_chars=":")
        assert isinstance(targets, list)
        for target in targets:
            assert isinstance(target, str)
    return dst_hosts


def validate_dst_root_datasets(dst_root_datasets: Dict) -> Dict:
    assert isinstance(dst_root_datasets, dict)
    for dst_hostname, dst_root_dataset in dst_root_datasets.items():
        assert isinstance(dst_hostname, str)
        assert dst_hostname, "dst hostname must not be empty!"
        bzfs.validate_host_name(dst_hostname, "dst host", extra_invalid_chars=":")
        assert isinstance(dst_root_dataset, str)
    return dst_root_datasets


def validate_snapshot_plan(snapshot_plan: Dict, context: str) -> Dict:
    assert isinstance(snapshot_plan, dict)
    for org, target_periods in snapshot_plan.items():
        assert isinstance(org, str)
        assert isinstance(target_periods, dict)
        for target, periods in target_periods.items():
            assert isinstance(target, str)
            assert isinstance(periods, dict)
            for period_unit, period_amount in periods.items():  # e.g. period_unit can be "10minutely" or "minutely"
                assert isinstance(period_unit, str)
                assert not (
                    not isinstance(period_amount, int) or period_amount < 0
                ), f"{context}: Period amount must be a non-negative integer: {period_amount} within plan {snapshot_plan}"
    return snapshot_plan


def validate_monitor_snapshot_plan(monitor_snapshot_plan: Dict) -> Dict:
    assert isinstance(monitor_snapshot_plan, dict)
    for org, target_periods in monitor_snapshot_plan.items():
        assert isinstance(org, str)
        assert isinstance(target_periods, dict)
        for target, periods in target_periods.items():
            assert isinstance(target, str)
            assert isinstance(periods, dict)
            for period_unit, alert_dict in periods.items():
                assert isinstance(period_unit, str)
                assert isinstance(alert_dict, dict)
                for key, value in alert_dict.items():
                    assert isinstance(key, str)
                    assert isinstance(value, str)
    return monitor_snapshot_plan


def validate_is_subset(x: Iterable[str], y: Iterable[str], x_name: str, y_name: str) -> None:
    assert isinstance(x, Iterable)
    assert not isinstance(x, str)
    assert isinstance(y, Iterable)
    assert not isinstance(y, str)
    if not set(x).issubset(set(y)):
        diff = sorted(set(x).difference(set(y)))
        assert False, f"{x_name} must be a subset of {y_name}. diff: {diff}, {x_name}: {sorted(x)}, {y_name}: {sorted(y)}"


def sanitize(filename: str) -> str:
    for s in (" ", "..", "/", "\\", bzfs.BARRIER_CHAR):
        filename = filename.replace(s, "!")
    return filename


def log_suffix(src_hostname: str, dst_hostname: str) -> str:
    sanitized_dst_hostname = sanitize(dst_hostname) if dst_hostname else ""
    return f"{sep}{sanitize(src_hostname)}{sep}{sanitized_dst_hostname}"


def format_dict(dictionary) -> str:
    return bzfs.format_dict(dictionary)


def pretty_print_formatter(dictionary):  # For lazy/noop evaluation in disabled log levels
    class PrettyPrintFormatter:
        def __str__(self):
            import json

            return json.dumps(dictionary, indent=4, sort_keys=True)

    return PrettyPrintFormatter()


#############################################################################
if __name__ == "__main__":
    main()
