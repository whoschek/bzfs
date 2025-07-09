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
#
"""
* Overview of the bzfs_jobrunner.py codebase:
* The codebase starts with docs, definition of input data and associated argument parsing of CLI options/parameters.
* Control flow starts in main(), far below ..., which kicks off a "Job".
* A Job creates zero or more "subjobs" for each local or remote host, via run_main().
* It executes the subjobs, serially or in parallel, via run_subjobs(), which in turn delegates parallel job coordination to
  bzfs.process_datasets_in_parallel_and_fault_tolerant().
* README_bzfs_jobrunner.md is mostly auto-generated from the ArgumentParser help texts as the source of "truth", via
update_readme.sh. Simply run that script whenever you change or add ArgumentParser help text.
"""

from __future__ import annotations
import argparse
import contextlib
import os
import pwd
import random
import socket
import subprocess
import sys
import threading
import time
import uuid
from ast import literal_eval
from logging import Logger
from subprocess import DEVNULL, PIPE
from typing import Any, Iterable, TypeVar, Union

import bzfs_main.utils
from bzfs_main import bzfs
from bzfs_main.check_range import CheckRange
from bzfs_main.detect import dummy_dataset
from bzfs_main.loggers import get_simple_logger
from bzfs_main.parallel_engine import (
    BARRIER_CHAR,
    process_datasets_in_parallel_and_fault_tolerant,
)
from bzfs_main.utils import (
    die_status,
    human_readable_duration,
    log_trace,
    percent,
    shuffle_dict,
)
from bzfs_main.utils import prog_name as bzfs_prog_name

# constants:
prog_name = "bzfs_jobrunner"
src_magic_substitution_token = "^SRC_HOST"  # noqa: S105
dst_magic_substitution_token = "^DST_HOST"  # noqa: S105
sep = ","


def argument_parser() -> argparse.ArgumentParser:
    """Returns the CLI parser used by bzfs_jobrunner."""
    # fmt: off
    parser = argparse.ArgumentParser(
        prog=prog_name,
        allow_abbrev=False,
        formatter_class=argparse.RawTextHelpFormatter,
        description=f"""
This program is a convenience wrapper around [bzfs](README.md) that simplifies periodic ZFS snapshot creation, replication,
pruning, and monitoring, across N source hosts and M destination hosts, using a single shared
[jobconfig](bzfs_tests/bzfs_job_example.py) script.
For example, this simplifies the deployment of an efficient geo-replicated backup service where each of the M destination
hosts is located in a separate geographic region and receives replicas from (the same set of) N source hosts. It also
simplifies low latency replication from a primary to a secondary or to M read replicas, or backup to removable drives, etc.

This program can be used to efficiently replicate ...

a) within a single machine (local mode), or

b) from a single source host to one or more destination hosts (pull or push or pull-push mode), or

c) from multiple source hosts to a single destination host (pull or push or pull-push mode), or

d) from N source hosts to M destination hosts (pull or push or pull-push mode, N and M can be large, M=2 or M=3 are typical
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
""")  # noqa: E501

    # commands:
    parser.add_argument(
        "--create-src-snapshots", action="store_true",
        help="Take snapshots on the selected source hosts as necessary. Typically, this command should be called by a "
             "program (or cron job) running on each src host.\n\n")
    parser.add_argument(  # `choices` are deprecated; use --replicate without argument instead
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
             "With this, given a snapshot name, we can find the destination hostnames to which the snapshot shall be "
             "replicated. Also, given a snapshot name and its own name, a destination host can determine if it shall "
             "replicate the given snapshot from the source host, or if the snapshot is intended for another destination "
             "host, in which case it skips the snapshot. A destination host will receive replicas of snapshots for all "
             "targets that map to that destination host.\n\n"
             "Removing a mapping can be used to temporarily suspend replication to a given destination host.\n\n")
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
             "Snapshots that do not match a retention period will be deleted. A zero or missing retention period indicates "
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
    locations = ["src", "dst"]
    for loc in locations:
        parser.add_argument(
            f"--ssh-{loc}-user", default="", metavar="STRING",
            help=f"Remote SSH username on {loc} hosts to connect to (optional). Examples: 'root', 'alice'.\n\n")
    for loc in locations:
        parser.add_argument(
            f"--ssh-{loc}-port", type=int, metavar="INT",
            help=f"Remote SSH port on {loc} host to connect to (optional).\n\n")
    for loc in locations:
        parser.add_argument(
            f"--ssh-{loc}-config-file", type=str, action=bzfs.SSHConfigFileNameAction, metavar="FILE",
            help=f"Path to SSH ssh_config(5) file to connect to {loc} (optional); will be passed into ssh -F CLI. "
                 "The basename must contain the substring 'bzfs_ssh_config'.\n\n")
    parser.add_argument(
        "--src-user", default="", metavar="STRING",
        help=argparse.SUPPRESS)  # deprecated; was renamed to --ssh-src-user
    parser.add_argument(
        "--dst-user", default="", metavar="STRING",
        help=argparse.SUPPRESS)  # deprecated; was renamed to --ssh-dst-user
    parser.add_argument(
        "--job-id", required=True, action=bzfs.NonEmptyStringAction, metavar="STRING",
        help="The identifier that remains constant across all runs of this particular job; will be included in the log file "
             "name infix. Example: mytestjob\n\n")
    parser.add_argument(
        "--jobid", default=None, action=bzfs.NonEmptyStringAction, metavar="STRING",
        help=argparse.SUPPRESS)   # deprecated; was renamed to --job-run
    parser.add_argument(
        "--job-run", default=None, action=bzfs.NonEmptyStringAction, metavar="STRING",
        help="The identifier of this particular run of the overall job; will be included in the log file name suffix. "
             "Default is a hex UUID. Example: 0badc0f003a011f0a94aef02ac16083c\n\n")
    workers_default = 100  # percent
    parser.add_argument(
        "--workers", min=1, default=(workers_default, True), action=bzfs.CheckPercentRange, metavar="INT[%]",
        help="The maximum number of jobs to run in parallel at any time; can be given as a positive integer, "
             f"optionally followed by the %% percent character (min: %(min)s, default: {workers_default}%%). Percentages "
             "are relative to the number of CPU cores on the machine. Example: 200%% uses twice as many parallel jobs as "
             "there are cores on the machine; 75%% uses num_procs = num_cores * 0.75. Examples: 1, 4, 75%%, 150%%\n\n")
    parser.add_argument(
        "--work-period-seconds", type=float, min=0, default=0, action=CheckRange, metavar="FLOAT",
        help="Reduces bandwidth spikes by spreading out the start of worker jobs over this much time; "
             "0 disables this feature (default: %(default)s). Examples: 0, 60, 86400\n\n")
    parser.add_argument(
        "--jitter", action="store_true",
        help="Randomize job start time and host order to avoid potential thundering herd problems in large distributed "
             "systems (optional). Randomizing job start time is only relevant if --work-period-seconds > 0.\n\n")
    parser.add_argument(
        "--worker-timeout-seconds", type=float, min=0, default=None, action=CheckRange, metavar="FLOAT",
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
        help="Only emit jobrunner messages with equal or higher priority than this log level. Default is '%(default)s'.\n\n")
    parser.add_argument(
        "--version", action="version", version=f"{prog_name}-{bzfs.__version__}, by {bzfs.prog_author}",
        help="Display version information and exit.\n\n")
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
    bad_opts = ["--daemon-frequency", "--include-snapshot-plan", "--create-src-snapshots-plan", "--skip-replication",
                "--log-file-prefix", "--log-file-infix", "--log-file-suffix", "--log-config-file", "--log-config-var",
                "--delete-dst-datasets", "--delete-dst-snapshots", "--delete-dst-snapshots-except",
                "--delete-dst-snapshots-except-plan", "--delete-empty-dst-datasets",
                "--monitor-snapshots", "--timeout"]
    for loc in locations:
        bad_opts += [f"--ssh-{loc}-host"]  # reject this arg as jobrunner will auto-generate it
    for bad_opt in bad_opts:
        parser.add_argument(bad_opt, action=RejectArgumentAction, nargs=0, help=argparse.SUPPRESS)
    parser.add_argument(
        "--root-dataset-pairs", required=True, nargs="+", action=bzfs.DatasetPairsAction, metavar="SRC_DATASET DST_DATASET",
        help="Source and destination dataset pairs (excluding usernames and excluding hostnames, which will all be "
             "auto-appended later).\n\n")
    return parser
    # fmt: on


#############################################################################
def main() -> None:
    """API for command line clients."""
    Job().run_main(sys.argv)


#############################################################################
class Job:
    """Coordinates subjobs per the CLI flags; Each subjob handles one host pair and may run in its own process or thread."""

    def __init__(self, log: Logger | None = None) -> None:
        """Initialize caches, parsers and logger shared across subjobs."""
        # immutable variables:
        self.jobrunner_dryrun: bool = False
        self.spawn_process_per_job: bool = False
        self.log: Logger = log if log is not None else get_simple_logger(prog_name)
        self.bzfs_argument_parser: argparse.ArgumentParser = bzfs.argument_parser()
        self.argument_parser: argparse.ArgumentParser = argument_parser()
        self.loopback_address: str = detect_loopback_address()

        # mutable variables:
        self.first_exception: int | None = None
        self.stats: Stats = Stats()
        self.cache_existing_dst_pools: set[str] = set()
        self.cache_known_dst_pools: set[str] = set()

        self.is_test_mode: bool = False  # for testing only

    def run_main(self, sys_argv: list[str]) -> None:
        """API for Python clients; visible for testing; may become a public API eventually."""
        self.first_exception = None
        log = self.log
        log.info("CLI arguments: %s", " ".join(sys_argv))

        # disable --root-dataset-pairs='+path/to/file' option in DatasetPairsAction
        nsp = argparse.Namespace(no_argument_file=True)

        args, unknown_args = self.argument_parser.parse_known_args(sys_argv[1:], nsp)  # forward all unknown args to `bzfs`
        log.setLevel(args.jobrunner_log_level)
        self.jobrunner_dryrun = args.jobrunner_dryrun
        assert len(args.root_dataset_pairs) > 0
        src_snapshot_plan = self.validate_snapshot_plan(literal_eval(args.src_snapshot_plan), "--src-snapshot-plan")
        src_bookmark_plan = self.validate_snapshot_plan(literal_eval(args.src_bookmark_plan), "--src-bookmark-plan")
        dst_snapshot_plan = self.validate_snapshot_plan(literal_eval(args.dst_snapshot_plan), "--dst-snapshot-plan")
        monitor_snapshot_plan = self.validate_monitor_snapshot_plan(literal_eval(args.monitor_snapshot_plan))
        localhostname = args.localhost if args.localhost else socket.gethostname()
        self.validate_host_name(localhostname, "--localhost")
        log.debug("localhostname: %s", localhostname)
        src_hosts = self.validate_src_hosts(literal_eval(args.src_hosts if args.src_hosts is not None else sys.stdin.read()))
        basis_src_hosts = src_hosts
        nb_src_hosts = len(basis_src_hosts)
        log.debug("src_hosts before subsetting: %s", src_hosts)
        if args.src_host is not None:  # retain only the src hosts that are also contained in args.src_host
            assert isinstance(args.src_host, list)
            retain_src_hosts = set(args.src_host)
            self.validate_is_subset(retain_src_hosts, src_hosts, "--src-host", "--src-hosts")
            src_hosts = [host for host in src_hosts if host in retain_src_hosts]
        dst_hosts = self.validate_dst_hosts(literal_eval(args.dst_hosts))
        nb_dst_hosts = len(dst_hosts)
        if args.dst_host is not None:  # retain only the dst hosts that are also contained in args.dst_host
            assert isinstance(args.dst_host, list)
            retain_dst_hosts = set(args.dst_host)
            self.validate_is_subset(retain_dst_hosts, dst_hosts.keys(), "--dst-host", "--dst-hosts.keys")
            dst_hosts = {dst_host: lst for dst_host, lst in dst_hosts.items() if dst_host in retain_dst_hosts}
        retain_dst_targets = self.validate_dst_hosts(literal_eval(args.retain_dst_targets))
        self.validate_is_subset(dst_hosts.keys(), retain_dst_targets.keys(), "--dst-hosts.keys", "--retain-dst-targets.keys")
        dst_root_datasets = self.validate_dst_root_datasets(literal_eval(args.dst_root_datasets))
        self.validate_is_subset(
            dst_root_datasets.keys(), retain_dst_targets.keys(), "--dst-root-dataset.keys", "--retain-dst-targets.keys"
        )
        self.validate_is_subset(dst_hosts.keys(), dst_root_datasets.keys(), "--dst-hosts.keys", "--dst-root-dataset.keys")
        bad_root_datasets = {
            dst_host: root_dataset
            for dst_host in sorted(dst_hosts.keys())
            if src_magic_substitution_token not in (root_dataset := dst_root_datasets[dst_host])
        }
        if len(src_hosts) > 1 and len(bad_root_datasets) > 0:
            self.die(
                "Cowardly refusing to proceed as multiple source hosts must not be configured to write to the same "
                "destination dataset. "
                f"Problematic subset of --dst-root-datasets: {bad_root_datasets} for src_hosts: {sorted(src_hosts)}"
            )
        bad_root_datasets = {
            dst_host: root_dataset
            for dst_host, root_dataset in sorted(dst_root_datasets.items())
            if root_dataset and src_magic_substitution_token not in root_dataset
        }
        if len(basis_src_hosts) > 1 and len(bad_root_datasets) > 0:
            self.die(
                "Cowardly refusing to proceed as multiple source hosts are defined in the configuration, but "
                f"not all non-empty root datasets in --dst-root-datasets contain the '{src_magic_substitution_token}' "
                "substitution token to prevent collisions on writing destination datasets. "
                f"Problematic subset of --dst-root-datasets: {bad_root_datasets} for src_hosts: {sorted(basis_src_hosts)}"
            )
        if args.jitter:  # randomize host order to avoid potential thundering herd problems in large distributed systems
            random.shuffle(src_hosts)
            dst_hosts = shuffle_dict(dst_hosts)
        ssh_src_user = args.ssh_src_user if args.ssh_src_user is not None else args.src_user  # --src-user is deprecated
        ssh_dst_user = args.ssh_dst_user if args.ssh_dst_user is not None else args.dst_user  # --dst-user is deprecated
        ssh_src_port = args.ssh_src_port
        ssh_dst_port = args.ssh_dst_port
        ssh_src_config_file = args.ssh_src_config_file
        ssh_dst_config_file = args.ssh_dst_config_file
        job_id = sanitize(args.job_id)
        job_run = args.job_run if args.job_run is not None else args.jobid  # --jobid is deprecated; was renamed to --job-run
        job_run = sanitize(job_run) if job_run else uuid.uuid1().hex
        workers, workers_is_percent = args.workers
        max_workers = max(1, round(os.cpu_count() * workers / 100.0) if workers_is_percent else round(workers))
        worker_timeout_seconds = args.worker_timeout_seconds
        self.spawn_process_per_job = args.spawn_process_per_job
        username: str = pwd.getpwuid(os.geteuid()).pw_name
        assert username
        localhost_ids: set[str] = {"localhost", "127.0.0.1", "::1", socket.gethostname()}  # ::1 is IPv6 loopback address
        localhost_ids.update(self.get_localhost_ips())  # union
        localhost_ids.add(localhostname)

        def zero_pad(number: int, width: int = 6) -> str:
            """Pads number with leading '0' chars to the given width."""
            return f"{number:0{width}d}"

        def jpad(jj: int, tag: str) -> str:
            """Returns ``tag`` prefixed with slash and zero padded index."""
            return "/" + zero_pad(jj) + tag

        def npad() -> str:
            """Returns standardized subjob count suffix."""
            return sep + zero_pad(len(subjobs))

        def update_subjob_name(tag: str) -> str:
            """Derives next subjob name based on ``tag`` and index ``j``."""
            if j <= 0:
                return subjob_name
            elif j == 1:
                return subjob_name + jpad(j - 1, tag)
            else:
                return subjob_name + "/" + BARRIER_CHAR

        def resolve_dataset(hostname: str, dataset: str, is_src: bool = True) -> str:
            """Returns host:dataset string resolving IPv6 and localhost cases."""
            ssh_user = ssh_src_user if is_src else ssh_dst_user
            ssh_user = ssh_user if ssh_user else username
            lb = self.loopback_address
            lhi = localhost_ids
            hostname = hostname if hostname not in lhi else (lb if lb else hostname) if username != ssh_user else "-"
            hostname = convert_ipv6(hostname)
            return f"{hostname}:{dataset}"

        def resolve_dst_dataset(dst_hostname: str, dst_dataset: str) -> str:
            """Expands ``dst_dataset`` relative to ``dst_hostname`` roots."""
            root_dataset = dst_root_datasets.get(dst_hostname)
            assert root_dataset is not None, dst_hostname  # f"Hostname '{dst_hostname}' missing in --dst-root-datasets"
            root_dataset = root_dataset.replace(src_magic_substitution_token, src_host)
            root_dataset = root_dataset.replace(dst_magic_substitution_token, dst_hostname)
            resolved_dst_dataset = root_dataset + "/" + dst_dataset if root_dataset else dst_dataset
            bzfs.validate_dataset_name(resolved_dst_dataset, dst_dataset)
            return resolve_dataset(dst_hostname, resolved_dst_dataset, is_src=False)

        dummy: str = dummy_dataset
        lhn = localhostname
        bzfs_prog_header = [bzfs_prog_name, "--no-argument-file"] + unknown_args
        subjobs: dict[str, list[str]] = {}
        for i, src_host in enumerate(src_hosts):
            subjob_name: str = zero_pad(i) + "src-host"

            if args.create_src_snapshots:
                opts = ["--create-src-snapshots", f"--create-src-snapshots-plan={src_snapshot_plan}", "--skip-replication"]
                opts += [f"--log-file-prefix={prog_name}{sep}create-src-snapshots{sep}"]
                opts += [f"--log-file-infix={sep}{job_id}"]
                opts += [f"--log-file-suffix={sep}{job_run}{npad()}{log_suffix(lhn, src_host, '')}{sep}"]
                self.add_ssh_opts(
                    opts, ssh_src_user=ssh_src_user, ssh_src_port=ssh_src_port, ssh_src_config_file=ssh_src_config_file
                )
                opts += ["--"]
                opts += flatten(dedupe([(resolve_dataset(src_host, src), dummy) for src, dst in args.root_dataset_pairs]))
                subjob_name += "/create-src-snapshots"
                subjobs[subjob_name] = bzfs_prog_header + opts

            if args.replicate:
                j = 0
                marker = "replicate"
                for dst_hostname, targets in dst_hosts.items():
                    opts = self.replication_opts(
                        dst_snapshot_plan, set(targets), lhn, src_host, dst_hostname, marker, job_id, job_run + npad()
                    )
                    if len(opts) > 0:
                        opts += [f"--daemon-frequency={args.daemon_replication_frequency}"]
                        self.add_ssh_opts(
                            opts,
                            ssh_src_user=ssh_src_user,
                            ssh_dst_user=ssh_dst_user,
                            ssh_src_port=ssh_src_port,
                            ssh_dst_port=ssh_dst_port,
                            ssh_src_config_file=ssh_src_config_file,
                            ssh_dst_config_file=ssh_dst_config_file,
                        )
                        opts += ["--"]
                        dataset_pairs = [
                            (resolve_dataset(src_host, src), resolve_dst_dataset(dst_hostname, dst))
                            for src, dst in args.root_dataset_pairs
                        ]
                        dataset_pairs = self.skip_nonexisting_local_dst_pools(dataset_pairs)
                        if len(dataset_pairs) > 0:
                            subjobs[subjob_name + jpad(j, marker)] = bzfs_prog_header + opts + flatten(dataset_pairs)
                            j += 1
                subjob_name = update_subjob_name(marker)

            def prune_src(opts: list[str], retention_plan: dict, tag: str) -> None:
                """Creates prune subjob options for ``tag`` using ``retention_plan``."""
                opts += [
                    "--skip-replication",
                    f"--delete-dst-snapshots-except-plan={retention_plan}",
                    f"--log-file-prefix={prog_name}{sep}{tag}{sep}",
                    f"--log-file-infix={sep}{job_id}",
                    f"--log-file-suffix={sep}{job_run}{npad()}{log_suffix(lhn, src_host, '')}{sep}",  # noqa: B023
                    f"--daemon-frequency={args.daemon_prune_src_frequency}",
                ]
                self.add_ssh_opts(  # i.e. dst=src, src=dummy
                    opts, ssh_dst_user=ssh_src_user, ssh_dst_port=ssh_src_port, ssh_dst_config_file=ssh_src_config_file
                )
                opts += ["--"]
                opts += flatten(
                    dedupe([(dummy, resolve_dataset(src_host, src)) for src, dst in args.root_dataset_pairs])  # noqa: B023
                )
                nonlocal subjob_name
                subjob_name += f"/{tag}"
                subjobs[subjob_name] = bzfs_prog_header + opts

            if args.prune_src_snapshots:
                prune_src(["--delete-dst-snapshots"], src_snapshot_plan, "prune-src-snapshots")

            if args.prune_src_bookmarks:
                prune_src(["--delete-dst-snapshots=bookmarks"], src_bookmark_plan, "prune-src-bookmarks")

            if args.prune_dst_snapshots:
                self.validate_true(
                    retain_dst_targets, "--retain-dst-targets must not be empty. Cowardly refusing to delete all snapshots!"
                )
                j = 0
                marker = "prune-dst-snapshots"
                for dst_hostname, _ in dst_hosts.items():
                    curr_retain_targets = set(retain_dst_targets[dst_hostname])
                    curr_dst_snapshot_plan = {  # only retain targets that belong to the host
                        org: {target: periods for target, periods in target_periods.items() if target in curr_retain_targets}
                        for org, target_periods in dst_snapshot_plan.items()
                    }
                    opts = ["--delete-dst-snapshots", "--skip-replication"]
                    opts += [f"--delete-dst-snapshots-except-plan={curr_dst_snapshot_plan}"]
                    opts += [f"--log-file-prefix={prog_name}{sep}{marker}{sep}"]
                    opts += [f"--log-file-infix={sep}{job_id}"]
                    opts += [f"--log-file-suffix={sep}{job_run}{npad()}{log_suffix(lhn, src_host, dst_hostname)}{sep}"]
                    opts += [f"--daemon-frequency={args.daemon_prune_dst_frequency}"]
                    self.add_ssh_opts(
                        opts, ssh_dst_user=ssh_dst_user, ssh_dst_port=ssh_dst_port, ssh_dst_config_file=ssh_dst_config_file
                    )
                    opts += ["--"]
                    dataset_pairs = [(dummy, resolve_dst_dataset(dst_hostname, dst)) for src, dst in args.root_dataset_pairs]
                    dataset_pairs = self.skip_nonexisting_local_dst_pools(dataset_pairs)
                    if len(dataset_pairs) > 0:
                        subjobs[subjob_name + jpad(j, marker)] = bzfs_prog_header + opts + flatten(dataset_pairs)
                        j += 1
                subjob_name = update_subjob_name(marker)

            def monitor_snapshots_opts(tag: str, monitor_plan: dict, logsuffix: str) -> list[str]:
                """Returns monitor subjob options for ``tag`` and ``monitor_plan``."""
                opts = [f"--monitor-snapshots={monitor_plan}", "--skip-replication"]
                opts += [f"--log-file-prefix={prog_name}{sep}{tag}{sep}"]
                opts += [f"--log-file-infix={sep}{job_id}"]
                opts += [f"--log-file-suffix={sep}{job_run}{npad()}{logsuffix}{sep}"]
                opts += [f"--daemon-frequency={args.daemon_monitor_snapshots_frequency}"]
                return opts

            def build_monitor_plan(monitor_plan: dict, snapshot_plan: dict, cycles_prefix: str) -> dict:
                """Expands ``monitor_plan`` with cycle defaults from ``snapshot_plan``."""

                def alert_dicts(alertdict: dict, cycles: int) -> dict:
                    """Returns alert dictionaries with explicit ``cycles`` value."""
                    latest_dict = alertdict.copy()
                    for prefix in ("src_snapshot_", "dst_snapshot_", ""):
                        latest_dict.pop(f"{prefix}cycles", None)
                    oldest_dict = latest_dict.copy()
                    oldest_dict["cycles"] = int(alertdict.get(f"{cycles_prefix}cycles", cycles))
                    return {"latest": latest_dict, "oldest": oldest_dict}

                return {
                    org: {
                        target: {
                            periodunit: alert_dicts(alertdict, snapshot_plan.get(org, {}).get(target, {}).get(periodunit, 1))
                            for periodunit, alertdict in periods.items()
                        }
                        for target, periods in target_periods.items()
                    }
                    for org, target_periods in monitor_plan.items()
                }

            if args.monitor_src_snapshots:
                marker = "monitor-src-snapshots"
                monitor_plan = build_monitor_plan(monitor_snapshot_plan, src_snapshot_plan, "src_snapshot_")
                opts = monitor_snapshots_opts(marker, monitor_plan, log_suffix(lhn, src_host, ""))
                self.add_ssh_opts(  # i.e. dst=src, src=dummy
                    opts, ssh_dst_user=ssh_src_user, ssh_dst_port=ssh_src_port, ssh_dst_config_file=ssh_src_config_file
                )
                opts += ["--"]
                opts += flatten(dedupe([(dummy, resolve_dataset(src_host, src)) for src, dst in args.root_dataset_pairs]))
                subjob_name += "/" + marker
                subjobs[subjob_name] = bzfs_prog_header + opts

            if args.monitor_dst_snapshots:
                j = 0
                marker = "monitor-dst-snapshots"
                for dst_hostname, targets in dst_hosts.items():
                    monitor_targets = set(targets).intersection(set(retain_dst_targets[dst_hostname]))
                    monitor_plan = {  # only retain targets that belong to the host
                        org: {target: periods for target, periods in target_periods.items() if target in monitor_targets}
                        for org, target_periods in monitor_snapshot_plan.items()
                    }
                    monitor_plan = build_monitor_plan(monitor_plan, dst_snapshot_plan, "dst_snapshot_")
                    opts = monitor_snapshots_opts(marker, monitor_plan, log_suffix(lhn, src_host, dst_hostname))
                    self.add_ssh_opts(
                        opts, ssh_dst_user=ssh_dst_user, ssh_dst_port=ssh_dst_port, ssh_dst_config_file=ssh_dst_config_file
                    )
                    opts += ["--"]
                    dataset_pairs = [(dummy, resolve_dst_dataset(dst_hostname, dst)) for src, dst in args.root_dataset_pairs]
                    dataset_pairs = self.skip_nonexisting_local_dst_pools(dataset_pairs)
                    if len(dataset_pairs) > 0:
                        subjobs[subjob_name + jpad(j, marker)] = bzfs_prog_header + opts + flatten(dataset_pairs)
                        j += 1
                subjob_name = update_subjob_name(marker)

        msg = "Ready to run %s subjobs using %s/%s src hosts: %s, %s/%s dst hosts: %s"
        log.info(
            msg, len(subjobs), len(src_hosts), nb_src_hosts, src_hosts, len(dst_hosts), nb_dst_hosts, list(dst_hosts.keys())
        )
        log.log(log_trace, "subjobs: \n%s", pretty_print_formatter(subjobs))
        self.run_subjobs(subjobs, max_workers, worker_timeout_seconds, args.work_period_seconds, args.jitter)
        ex = self.first_exception
        if isinstance(ex, int):
            assert ex != 0
            sys.exit(ex)
        assert ex is None, ex
        log.info("Succeeded. Bye!")

    def replication_opts(
        self,
        dst_snapshot_plan: dict[str, dict[str, dict[str, int]]],
        targets: set[str],
        localhostname: str,
        src_hostname: str,
        dst_hostname: str,
        tag: str,
        job_id: str,
        job_run: str,
    ) -> list[str]:
        """Returns CLI options for one replication subjob."""
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
            opts += [f"--log-file-prefix={prog_name}{sep}{tag}{sep}"]
            opts += [f"--log-file-infix={sep}{job_id}"]
            opts += [f"--log-file-suffix={sep}{job_run}{log_suffix(localhostname, src_hostname, dst_hostname)}{sep}"]
        return opts

    def skip_nonexisting_local_dst_pools(self, root_dataset_pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
        """Skip datasets that point to removeable destination drives that are not currently (locally) attached, if any."""

        def zpool(dataset: str) -> str:
            """Returns pool name portion of ``dataset``."""
            return dataset.split("/", 1)[0]

        assert len(root_dataset_pairs) > 0
        unknown_dst_pools = {zpool(dst) for src, dst in root_dataset_pairs}
        unknown_dst_pools = unknown_dst_pools.difference(self.cache_known_dst_pools)

        # Here we treat a zpool as existing if the zpool isn't local, aka if it isn't prefixed with "-:". A remote host
        # will raise an appropriate error if it turns out that the remote zpool doesn't actually exist.
        unknown_local_dst_pools = {pool for pool in unknown_dst_pools if pool.startswith("-:")}
        if len(unknown_local_dst_pools) > 0:  # `zfs list` if local
            existing_pools = {pool[len("-:") :] for pool in unknown_local_dst_pools}
            cmd = "zfs list -t filesystem,volume -Hp -o name".split(" ") + sorted(existing_pools)
            sp = subprocess.run(cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True)
            if sp.returncode not in (0, 1):  # 1 means dataset not found
                self.die(f"Unexpected error {sp.returncode} on checking for existing local dst pools: {sp.stderr.strip()}")
            existing_pools = {"-:" + pool for pool in sp.stdout.splitlines()}
            self.cache_existing_dst_pools.update(existing_pools)  # union
        unknown_remote_dst_pools = unknown_dst_pools.difference(unknown_local_dst_pools)
        self.cache_existing_dst_pools.update(unknown_remote_dst_pools)  # union
        self.cache_known_dst_pools.update(unknown_dst_pools)  # union
        results = []
        for src, dst in root_dataset_pairs:
            if zpool(dst) in self.cache_existing_dst_pools:
                results.append((src, dst))
            else:
                self.log.warning("Skipping dst dataset for which local dst pool does not exist: %s", dst)
        return results

    @staticmethod
    def add_ssh_opts(
        opts: list[str],
        ssh_src_user: str | None = None,
        ssh_dst_user: str | None = None,
        ssh_src_port: str | None = None,
        ssh_dst_port: str | None = None,
        ssh_src_config_file: str | None = None,
        ssh_dst_config_file: str | None = None,
    ) -> None:
        """Appends ssh related options to ``opts`` if specified."""
        assert isinstance(opts, list)
        opts += [f"--ssh-src-user={ssh_src_user}"] if ssh_src_user else []
        opts += [f"--ssh-dst-user={ssh_dst_user}"] if ssh_dst_user else []
        opts += [f"--ssh-src-port={ssh_src_port}"] if ssh_src_port else []
        opts += [f"--ssh-dst-port={ssh_dst_port}"] if ssh_dst_port else []
        opts += [f"--ssh-src-config-file={ssh_src_config_file}"] if ssh_src_config_file else []
        opts += [f"--ssh-dst-config-file={ssh_dst_config_file}"] if ssh_dst_config_file else []

    def run_subjobs(
        self,
        subjobs: dict[str, list[str]],
        max_workers: int,
        timeout_secs: float | None,
        work_period_seconds: float,
        jitter: bool,
    ) -> None:
        """Executes subjobs sequentially or in parallel, respecting barriers."""
        self.stats = Stats()
        self.stats.jobs_all = len(subjobs)
        log = self.log
        num_intervals = 1 + len(subjobs) if jitter else len(subjobs)
        interval_nanos = 0 if len(subjobs) == 0 else round(1_000_000_000 * max(0.0, work_period_seconds) / num_intervals)
        assert interval_nanos >= 0
        if jitter:  # randomize job start time to avoid potential thundering herd problems in large distributed systems
            sleep_nanos = random.randint(0, interval_nanos)  # noqa: S311
            log.info("Jitter: Delaying job start time by sleeping for %s ...", human_readable_duration(sleep_nanos))
            time.sleep(sleep_nanos / 1_000_000_000)  # seconds
        sorted_subjobs = sorted(subjobs.keys())
        has_barrier = any(BARRIER_CHAR in subjob.split("/") for subjob in sorted_subjobs)
        if self.spawn_process_per_job or has_barrier or bzfs.has_siblings(sorted_subjobs):  # siblings can run in parallel
            log.log(log_trace, "%s", "spawn_process_per_job: True")
            params = bzfs.Params(self.bzfs_argument_parser.parse_args(args=["src", "dst", "--retries=0"]), log=log)
            process_datasets_in_parallel_and_fault_tolerant(
                log=params.log,
                datasets=sorted_subjobs,
                process_dataset=lambda subjob, tid, retry: self.run_subjob(
                    subjobs[subjob], name=subjob, timeout_secs=timeout_secs, spawn_process_per_job=True
                )
                == 0,
                skip_tree_on_error=lambda subjob: True,
                skip_on_error=params.skip_on_error,
                max_workers=max_workers,
                interval_nanos=lambda subjob: interval_nanos,
                task_name="Subjob",
                retry_policy=params.retry_policy,
                dry_run=params.dry_run,
                is_test_mode=self.is_test_mode,
            )
        else:
            log.log(log_trace, "%s", "spawn_process_per_job: False")
            next_update_nanos = time.monotonic_ns()
            for subjob in sorted_subjobs:
                time.sleep(max(0, next_update_nanos - time.monotonic_ns()) / 1_000_000_000)  # seconds
                next_update_nanos += interval_nanos
                s = subjob
                if not self.run_subjob(subjobs[subjob], name=s, timeout_secs=timeout_secs, spawn_process_per_job=False) == 0:
                    break
        stats = self.stats
        jobs_skipped = stats.jobs_all - stats.jobs_started
        msg = f"{stats}, skipped:" + percent(jobs_skipped, total=stats.jobs_all)
        log.info("Final Progress: %s", msg)
        assert stats.jobs_running == 0, msg
        assert stats.jobs_completed == stats.jobs_started, msg
        skipped_jobs_dict = {subjob: subjobs[subjob] for subjob in sorted_subjobs if subjob not in stats.started_job_names}
        if len(skipped_jobs_dict) > 0:
            log.debug("Skipped subjobs: \n%s", pretty_print_formatter(skipped_jobs_dict))
        assert jobs_skipped == len(skipped_jobs_dict), msg

    def run_subjob(
        self, cmd: list[str], name: str, timeout_secs: float | None, spawn_process_per_job: bool
    ) -> int | None:  # thread-safe
        """Executes one worker job and updates shared Stats."""
        start_time_nanos = time.monotonic_ns()
        returncode = None
        log = self.log
        cmd_str = " ".join(cmd)
        stats = self.stats
        try:
            with stats.lock:
                stats.jobs_started += 1
                stats.jobs_running += 1
                stats.started_job_names.add(name)
                msg = str(stats)
            log.log(log_trace, "Starting worker job: %s", cmd_str)
            log.info("Progress: %s", msg)
            start_time_nanos = time.monotonic_ns()
            if spawn_process_per_job:
                returncode = self.run_worker_job_spawn_process_per_job(cmd, timeout_secs)
            else:
                returncode = self.run_worker_job_in_current_thread(cmd, timeout_secs)
        except BaseException as e:
            log.error("Worker job failed with unexpected exception: %s for command: %s", e, cmd_str)
            raise
        else:
            elapsed_nanos = time.monotonic_ns() - start_time_nanos
            elapsed_human = human_readable_duration(elapsed_nanos)
            if returncode != 0:
                with stats.lock:
                    if self.first_exception is None:
                        self.first_exception = die_status if returncode is None else returncode
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
                assert stats.jobs_all >= 0, msg
                assert stats.jobs_started >= 0, msg
                assert stats.jobs_completed >= 0, msg
                assert stats.jobs_failed >= 0, msg
                assert stats.jobs_running >= 0, msg
                assert stats.sum_elapsed_nanos >= 0, msg
                assert stats.jobs_failed <= stats.jobs_completed, msg
                assert stats.jobs_completed <= stats.jobs_started, msg
                assert stats.jobs_started <= stats.jobs_all, msg
            log.info("Progress: %s", msg)

    def run_worker_job_in_current_thread(self, cmd: list[str], timeout_secs: float | None) -> int | None:
        """Runs ``bzfs`` in-process and return its exit code."""
        log = self.log
        if timeout_secs is not None:
            cmd = cmd[0:1] + [f"--timeout={round(1000 * timeout_secs)}milliseconds"] + cmd[1:]
        try:
            if not self.jobrunner_dryrun:
                self._bzfs_run_main(cmd)
            return 0
        except subprocess.CalledProcessError as e:
            return e.returncode
        except SystemExit as e:
            assert e.code is None or isinstance(e.code, int)
            return e.code
        except BaseException:
            log.exception("Worker job failed with unexpected exception for command: %s", " ".join(cmd))
            return die_status

    def _bzfs_run_main(self, cmd: list[str]) -> None:
        """Delegate execution to :mod:`bzfs` using parsed arguments."""
        bzfs_job = bzfs.Job()
        bzfs_job.is_test_mode = self.is_test_mode
        bzfs_job.run_main(self.bzfs_argument_parser.parse_args(cmd[1:]), cmd)

    def run_worker_job_spawn_process_per_job(self, cmd: list[str], timeout_secs: float | None) -> int | None:
        """Spawns a subprocess for the worker job and waits for completion."""
        log = self.log
        if len(cmd) > 0 and cmd[0] == bzfs_prog_name:
            cmd = [sys.executable, "-m", "bzfs_main." + cmd[0]] + cmd[1:]
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

    def validate_src_hosts(self, src_hosts: list[str]) -> list[str]:
        """Checks ``src_hosts`` contains valid hostnames."""
        context = "--src-hosts"
        self.validate_type(src_hosts, list, context)
        for src_hostname in src_hosts:
            self.validate_host_name(src_hostname, context)
        return src_hosts

    def validate_dst_hosts(self, dst_hosts: dict[str, list[str]]) -> dict[str, list[str]]:
        """Checks destination hosts dictionary."""
        context = "--dst-hosts"
        self.validate_type(dst_hosts, dict, context)
        for dst_hostname, targets in dst_hosts.items():
            self.validate_host_name(dst_hostname, context)
            self.validate_type(targets, list, f"{context} targets")
            for target in targets:
                self.validate_type(target, str, f"{context} target")
        return dst_hosts

    def validate_dst_root_datasets(self, dst_root_datasets: dict[str, str]) -> dict[str, str]:
        """Checks that each destination root dataset string is valid."""
        context = "--dst-root-datasets"
        self.validate_type(dst_root_datasets, dict, context)
        for dst_hostname, dst_root_dataset in dst_root_datasets.items():
            self.validate_host_name(dst_hostname, context)
            self.validate_type(dst_root_dataset, str, f"{context} root dataset")
        return dst_root_datasets

    def validate_snapshot_plan(
        self, snapshot_plan: dict[str, dict[str, dict[str, int]]], context: str
    ) -> dict[str, dict[str, dict[str, int]]]:
        """Checks snapshot plan structure and value types."""
        self.validate_type(snapshot_plan, dict, context)
        for org, target_periods in snapshot_plan.items():
            self.validate_type(org, str, f"{context} org")
            self.validate_type(target_periods, dict, f"{context} target_periods")
            for target, periods in target_periods.items():
                self.validate_type(target, str, f"{context} org/target")
                self.validate_type(periods, dict, f"{context} org/periods")
                for period_unit, period_amount in periods.items():
                    self.validate_non_empty_string(period_unit, f"{context} org/target/period_unit")
                    self.validate_non_negative_int(period_amount, f"{context} org/target/period_amount")
        return snapshot_plan

    def validate_monitor_snapshot_plan(
        self, monitor_snapshot_plan: dict[str, dict[str, dict[str, dict[str, str | int]]]]
    ) -> dict[str, dict[str, dict[str, dict[str, str | int]]]]:
        """Checks snapshot monitoring plan configuration."""
        context = "--monitor-snapshot-plan"
        self.validate_type(monitor_snapshot_plan, dict, context)
        for org, target_periods in monitor_snapshot_plan.items():
            self.validate_type(org, str, f"{context} org")
            self.validate_type(target_periods, dict, f"{context} target_periods")
            for target, periods in target_periods.items():
                self.validate_type(target, str, f"{context} org/target")
                self.validate_type(periods, dict, f"{context} org/periods")
                for period_unit, alert_dict in periods.items():
                    self.validate_non_empty_string(period_unit, f"{context} org/target/period_unit")
                    self.validate_type(alert_dict, dict, f"{context} org/target/alert_dict")
                    for key, value in alert_dict.items():
                        self.validate_non_empty_string(key, f"{context} org/target/alert_dict/key")
                        self.validate_type(value, Union[str, int], f"{context} org/target/alert_dict/value")
        return monitor_snapshot_plan

    def validate_is_subset(self, x: Iterable[str], y: Iterable[str], x_name: str, y_name: str) -> None:
        """Raises error if ``x`` contains an item not present in ``y``."""
        if isinstance(x, str) or not isinstance(x, Iterable):
            self.die(f"{x_name} must be an Iterable")
        if isinstance(y, str) or not isinstance(y, Iterable):
            self.die(f"{y_name} must be an Iterable")
        if not set(x).issubset(set(y)):
            diff = sorted(set(x).difference(set(y)))
            self.die(f"{x_name} must be a subset of {y_name}. diff: {diff}, {x_name}: {sorted(x)}, {y_name}: {sorted(y)}")

    def validate_host_name(self, hostname: str, context: str) -> None:
        """Checks host name string."""
        self.validate_non_empty_string(hostname, f"{context} hostname")
        bzfs.validate_host_name(hostname, context)

    def validate_non_empty_string(self, value: str, name: str) -> None:
        """Checks that ``value`` is a non-empty string."""
        self.validate_type(value, str, name)
        if not value:
            self.die(f"{name} must not be empty!")

    def validate_non_negative_int(self, value: int, name: str) -> None:
        """Checks ``value`` is an int >= 0."""
        self.validate_type(value, int, name)
        if value < 0:
            self.die(f"{name} must be a non-negative integer: {value}")

    def validate_true(self, expr: Any, msg: str) -> None:
        """Raises error if ``expr`` evaluates to ``False``."""
        if not bool(expr):
            self.die(msg)

    def validate_type(self, value: Any, expected_type: Any, name: str) -> None:
        """Checks ``value`` is instance of ``expected_type`` or union thereof."""
        if hasattr(expected_type, "__origin__") and expected_type.__origin__ is Union:  # for compat with python < 3.10
            union_types = expected_type.__args__
            for t in union_types:
                if isinstance(value, t):
                    return
            type_msg = " or ".join([t.__name__ for t in union_types])
            self.die(f"{name} must be of type {type_msg} but got {type(value).__name__}: {value}")
        elif not isinstance(value, expected_type):
            self.die(f"{name} must be of type {expected_type.__name__} but got {type(value).__name__}: {value}")

    def die(self, msg: str) -> None:
        """Log ``msg`` and exit the program."""
        self.log.error("%s", msg)
        bzfs_main.utils.die(msg)

    def get_localhost_ips(self) -> set[str]:
        """Returns all network addresses of the local host, i.e. all configured addresses on all network interfaces, without
        depending on name resolution."""
        if sys.platform == "linux":
            try:
                proc = subprocess.run(["hostname", "-I"], stdin=DEVNULL, stdout=PIPE, text=True, check=True)  # noqa: S607
            except Exception as e:
                self.log.warning("Cannot run 'hostname -I' on localhost: %s", e)
            else:
                return {ip for ip in proc.stdout.strip().split() if ip}
        return set()


#############################################################################
class Stats:
    """Thread-safe counters summarizing subjob progress."""

    def __init__(self) -> None:
        self.lock: threading.Lock = threading.Lock()
        self.jobs_all: int = 0
        self.jobs_started: int = 0
        self.jobs_completed: int = 0
        self.jobs_failed: int = 0
        self.jobs_running: int = 0
        self.sum_elapsed_nanos: int = 0
        self.started_job_names: set[str] = set()

    def __repr__(self) -> str:

        def pct(number: int) -> str:
            """Returns percentage string relative to total jobs."""
            return percent(number, total=self.jobs_all)

        al, started, completed, failed = self.jobs_all, self.jobs_started, self.jobs_completed, self.jobs_failed
        running = self.jobs_running
        t = "avg_completion_time:" + human_readable_duration(self.sum_elapsed_nanos / max(1, completed))
        return f"all:{al}, started:{pct(started)}, completed:{pct(completed)}, failed:{pct(failed)}, running:{running}, {t}"


#############################################################################
class RejectArgumentAction(argparse.Action):
    """An argparse Action that immediately fails if it is ever triggered."""

    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        """Abort argument parsing if a protected option is seen."""
        parser.error(f"Security: Overriding protected argument '{option_string}' is not allowed.")


#############################################################################
def dedupe(root_dataset_pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Returns a list with duplicate dataset pairs removed while preserving order."""
    return list(dict.fromkeys(root_dataset_pairs).keys())


T = TypeVar("T")


def flatten(root_dataset_pairs: Iterable[Iterable[T]]) -> list[T]:
    """Flattens an iterable of pairs into a single list."""
    return [item for pair in root_dataset_pairs for item in pair]


def sanitize(filename: str) -> str:
    """Replaces potentially problematic characters in ``filename`` with '!'."""
    for s in (" ", "..", "/", "\\", sep):
        filename = filename.replace(s, "!")
    return filename


def log_suffix(localhostname: str, src_hostname: str, dst_hostname: str) -> str:
    """Returns a log file suffix in a format that contains the given hostnames."""
    sanitized_dst_hostname = sanitize(dst_hostname) if dst_hostname else ""
    return f"{sep}{sanitize(localhostname)}{sep}{sanitize(src_hostname)}{sep}{sanitized_dst_hostname}"


def format_dict(dictionary: dict[str, Any]) -> str:
    """Pretty prints the given dictionary for logging and docs."""
    return bzfs.format_dict(dictionary)


def pretty_print_formatter(dictionary: dict[str, Any]) -> Any:
    """Lazy JSON formatter used to avoid overhead in disabled log levels."""

    class PrettyPrintFormatter:
        """Wrapper returning formatted JSON on ``str`` conversion."""

        def __str__(self) -> str:
            import json

            return json.dumps(dictionary, indent=4, sort_keys=True)

    return PrettyPrintFormatter()


def detect_loopback_address() -> str:
    """Detects if a loopback connection over IPv4 or IPv6 is possible."""
    try:
        addr = "127.0.0.1"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((addr, 0))
        return addr
    except OSError:
        pass

    try:
        addr = "::1"
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
            s.bind((addr, 0))
        return addr
    except OSError:
        pass

    return ""


def convert_ipv6(hostname: str) -> str:
    """Supports IPv6 without getting confused by host:dataset colon separator and any colons that may be part of a (valid)
    ZFS dataset name."""
    return hostname.replace(":", "|")  # Also see bzfs.convert_ipv6() for the reverse conversion


#############################################################################
if __name__ == "__main__":
    main()
