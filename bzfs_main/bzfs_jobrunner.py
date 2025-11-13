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
# requires-python = ">=3.9"
# dependencies = []
# ///
#
"""
* High-level orchestrator that calls `bzfs` as part of complex, periodic workflows to manage backup, replication, and pruning
  jobs across a fleet of multiple source and destination hosts; driven by a fleet-wide job config file
  (e.g., `bzfs_job_example.py`).
* Overview of the bzfs_jobrunner.py codebase:
* The codebase starts with docs, definition of input data and associated argument parsing of CLI options/parameters.
* Control flow starts in main(), far below ..., which kicks off a "Job".
* A Job creates zero or more "subjobs" for each local or remote host, via run_main().
* It executes the subjobs, serially or in parallel, via run_subjobs(), which in turn delegates parallel job coordination to
  bzfs.process_datasets_in_parallel_and_fault_tolerant().
* README_bzfs_jobrunner.md is mostly auto-generated from the ArgumentParser help texts as the source of "truth", via
update_readme.sh. Simply run that script whenever you change or add ArgumentParser help text.
"""

from __future__ import (
    annotations,
)
import argparse
import contextlib
import os
import platform
import pwd
import random
import socket
import subprocess
import sys
import threading
import time
import uuid
from ast import (
    literal_eval,
)
from collections.abc import (
    Iterable,
)
from logging import (
    Logger,
)
from subprocess import (
    DEVNULL,
    PIPE,
)
from typing import (
    Any,
    Final,
    NoReturn,
    TypeVar,
    Union,
)

import bzfs_main.argparse_actions
from bzfs_main import (
    bzfs,
)
from bzfs_main.argparse_cli import (
    PROG_AUTHOR,
)
from bzfs_main.detect import (
    DUMMY_DATASET,
)
from bzfs_main.loggers import (
    get_simple_logger,
    reset_logger,
    set_logging_runtime_defaults,
)
from bzfs_main.util import (
    check_range,
    utils,
)
from bzfs_main.util.parallel_tasktree import (
    BARRIER_CHAR,
)
from bzfs_main.util.parallel_tasktree_policy import (
    process_datasets_in_parallel_and_fault_tolerant,
)
from bzfs_main.util.utils import (
    DIE_STATUS,
    LOG_TRACE,
    UMASK,
    JobStats,
    Subprocesses,
    dry,
    format_dict,
    format_obj,
    getenv_bool,
    human_readable_duration,
    percent,
    shuffle_dict,
    terminate_process_subtree,
    termination_signal_handler,
)
from bzfs_main.util.utils import PROG_NAME as BZFS_PROG_NAME

# constants:
PROG_NAME: Final[str] = "bzfs_jobrunner"
SRC_MAGIC_SUBSTITUTION_TOKEN: Final[str] = "^SRC_HOST"  # noqa: S105
DST_MAGIC_SUBSTITUTION_TOKEN: Final[str] = "^DST_HOST"  # noqa: S105
SEP: Final[str] = ","
POSIX_END_OF_OPTIONS_MARKER: Final[str] = "--"  # args following -- are treated as operands, even if they begin with a hyphen


def argument_parser() -> argparse.ArgumentParser:
    """Returns the CLI parser used by bzfs_jobrunner."""
    # fmt: off
    parser = argparse.ArgumentParser(
        prog=PROG_NAME,
        allow_abbrev=False,
        formatter_class=argparse.RawTextHelpFormatter,
        description=f"""
This program is a convenience wrapper around [bzfs](README.md) that simplifies periodic ZFS snapshot creation, replication,
pruning, and monitoring, across a fleet of N source hosts and M destination hosts, using a single fleet-wide shared
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
However, typically, a cron job on each source host runs `{PROG_NAME}` periodically to create new snapshots (via
--create-src-snapshots) and prune outdated snapshots and bookmarks on the source (via --prune-src-snapshots and
--prune-src-bookmarks), whereas another cron job on each destination host runs `{PROG_NAME}` periodically to prune
outdated destination snapshots (via --prune-dst-snapshots), and to replicate the recently created snapshots from the source
to the destination (via --replicate).
Yet another cron job on each source and each destination runs `{PROG_NAME}` periodically to alert the user if the latest or
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
        "--localhost", default=None, action=bzfs_main.argparse_actions.NonEmptyStringAction, metavar="STRING",
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
        "archive": f"archives/zoo/{SRC_MAGIC_SUBSTITUTION_TOKEN}",
        "hotspare": "",
    }
    parser.add_argument(
        "--dst-root-datasets", default="{}", metavar="DICT_STRING",
        help="Dictionary that maps each destination hostname to a root dataset located on that destination host. The root "
             "dataset name is an (optional) prefix that will be prepended to each dataset that is replicated to that "
             "destination host. For backup use cases, this is the backup ZFS pool or a ZFS dataset path within that pool, "
             "whereas for cloning, master slave replication, or replication from a primary to a secondary, this can also be "
             "the empty string.\n\n"
             f"`{SRC_MAGIC_SUBSTITUTION_TOKEN}` and `{DST_MAGIC_SUBSTITUTION_TOKEN}` are optional magic substitution tokens "
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
            f"--ssh-{loc}-port", type=int, min=1, max=65535, action=check_range.CheckRange, metavar="INT",
            help=f"Remote SSH port on {loc} host to connect to (optional).\n\n")
    for loc in locations:
        parser.add_argument(
            f"--ssh-{loc}-config-file", type=str, action=bzfs_main.argparse_actions.SSHConfigFileNameAction, metavar="FILE",
            help=f"Path to SSH ssh_config(5) file to connect to {loc} (optional); will be passed into ssh -F CLI. "
                 "The basename must contain the substring 'bzfs_ssh_config'.\n\n")
    parser.add_argument(
        "--src-user", default="", metavar="STRING",
        help=argparse.SUPPRESS)  # deprecated; was renamed to --ssh-src-user
    parser.add_argument(
        "--dst-user", default="", metavar="STRING",
        help=argparse.SUPPRESS)  # deprecated; was renamed to --ssh-dst-user
    parser.add_argument(
        "--job-id", required=True, action=bzfs_main.argparse_actions.NonEmptyStringAction, metavar="STRING",
        help="The identifier that remains constant across all runs of this particular job; will be included in the log file "
             "name infix. Example: mytestjob\n\n")
    parser.add_argument(
        "--jobid", default=None, action=bzfs_main.argparse_actions.NonEmptyStringAction, metavar="STRING",
        help=argparse.SUPPRESS)   # deprecated; was renamed to --job-run
    parser.add_argument(
        "--job-run", default=None, action=bzfs_main.argparse_actions.NonEmptyStringAction, metavar="STRING",
        help="The identifier of this particular run of the overall job; will be included in the log file name suffix. "
             "Default is a hex UUID. Example: 0badc0f003a011f0a94aef02ac16083c\n\n")
    workers_default = 100  # percent
    parser.add_argument(
        "--workers", min=1, default=(workers_default, True), action=bzfs_main.argparse_actions.CheckPercentRange,
        metavar="INT[%]",
        help="The maximum number of jobs to run in parallel at any time; can be given as a positive integer, "
             f"optionally followed by the %% percent character (min: %(min)s, default: {workers_default}%%). Percentages "
             "are relative to the number of CPU cores on the machine. Example: 200%% uses twice as many parallel jobs as "
             "there are cores on the machine; 75%% uses num_procs = num_cores * 0.75. Examples: 1, 4, 75%%, 150%%\n\n")
    parser.add_argument(
        "--work-period-seconds", type=float, min=0, default=0, action=check_range.CheckRange, metavar="FLOAT",
        help="Reduces bandwidth spikes by spreading out the start of worker jobs over this much time; "
             "0 disables this feature (default: %(default)s). Examples: 0, 60, 86400\n\n")
    parser.add_argument(
        "--jitter", action="store_true",
        help="Randomize job start time and host order to avoid potential thundering herd problems in large distributed "
             "systems (optional). Randomizing job start time is only relevant if --work-period-seconds > 0.\n\n")
    parser.add_argument(
        "--worker-timeout-seconds", type=float, min=0.001, default=None, action=check_range.CheckRange,
        metavar="FLOAT",
        help="If this much time has passed after a worker process has started executing, kill the straggling worker "
             "(optional). Other workers remain unaffected. Examples: 60, 3600\n\n")
    parser.add_argument(
        "--spawn-process-per-job", action="store_true",
        help="Spawn a Python process per subjob instead of a Python thread per subjob (optional). The former is only "
             "recommended for a job operating in parallel on a large number of hosts as it helps avoid exceeding "
             "per-process limits such as the default max number of open file descriptors, at the expense of increased "
             "startup latency.\n\n")
    parser.add_argument(
        "--jobrunner-dryrun", action="store_true",
        help="Do a dry run (aka 'no-op') to print what operations would happen if the command were to be executed "
             "for real (optional). This option treats both the ZFS source and destination as read-only. Can also be used to "
             "check if the configuration options are valid.\n\n")
    parser.add_argument(
        "--jobrunner-log-level", choices=["CRITICAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"], default="INFO",
        help="Only emit jobrunner messages with equal or higher priority than this log level. Default is '%(default)s'.\n\n")
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
                "--log-file-prefix", "--log-file-infix", "--log-file-suffix",
                "--delete-dst-datasets", "--delete-dst-snapshots", "--delete-dst-snapshots-except",
                "--delete-dst-snapshots-except-plan", "--delete-empty-dst-datasets",
                "--monitor-snapshots", "--timeout"]
    for loc in locations:
        bad_opts += [f"--ssh-{loc}-host"]  # reject this arg as jobrunner will auto-generate it
    for bad_opt in bad_opts:
        parser.add_argument(bad_opt, action=RejectArgumentAction, nargs=0, help=argparse.SUPPRESS)
    parser.add_argument(
        "--version", action="version", version=f"{PROG_NAME}-{bzfs_main.argparse_cli.__version__}, by {PROG_AUTHOR}",
        help="Display version information and exit.\n\n")
    parser.add_argument(
        "--help, -h", action="help",  # trick to ensure both --help and -h are shown in the help msg
        help="Show this help message and exit.\n\n")
    parser.add_argument(
        "--root-dataset-pairs", required=True, nargs="+", action=bzfs_main.argparse_actions.DatasetPairsAction,
        metavar="SRC_DATASET DST_DATASET",
        help="Source and destination dataset pairs (excluding usernames and excluding hostnames, which will all be "
             "auto-appended later).\n\n")
    return parser
    # fmt: on


#############################################################################
def main() -> None:
    """API for command line clients."""
    prev_umask: int = os.umask(UMASK)
    try:
        set_logging_runtime_defaults()
        # On CTRL-C and SIGTERM, send signal to all descendant processes to terminate them
        termination_event: threading.Event = threading.Event()
        with termination_signal_handler(termination_event=termination_event):
            Job(log=None, termination_event=termination_event).run_main(sys_argv=sys.argv)
    finally:
        os.umask(prev_umask)  # restore prior global state


#############################################################################
class Job:
    """Coordinates subjobs per the CLI flags; Each subjob handles one host pair and may run in its own process or thread."""

    def __init__(self, log: Logger | None, termination_event: threading.Event) -> None:
        # immutable variables:
        self.log_was_None: Final[bool] = log is None
        self.log: Final[Logger] = get_simple_logger(PROG_NAME) if log is None else log
        self.termination_event: Final[threading.Event] = termination_event
        self.subprocesses: Final[Subprocesses] = Subprocesses()
        self.jobrunner_dryrun: bool = False
        self.spawn_process_per_job: bool = False
        self.loopback_address: Final[str] = _detect_loopback_address()

        # mutable variables:
        self.first_exception: int | None = None
        self.stats: JobStats = JobStats(jobs_all=0)
        self.cache_existing_dst_pools: set[str] = set()
        self.cache_known_dst_pools: set[str] = set()

        self.is_test_mode: bool = False  # for testing only

    def run_main(self, sys_argv: list[str]) -> None:
        """API for Python clients; visible for testing; may become a public API eventually."""
        try:
            self._run_main(sys_argv)
        finally:
            if self.log_was_None:  # reset Logger unless it's a Logger outside of our control
                reset_logger(self.log)

    def _run_main(self, sys_argv: list[str]) -> None:
        self.first_exception = None
        log: Logger = self.log
        log.info("CLI arguments: %s", " ".join(sys_argv))
        nsp = argparse.Namespace(no_argument_file=True)  # disable --root-dataset-pairs='+file' option in DatasetPairsAction
        args, unknown_args = argument_parser().parse_known_args(sys_argv[1:], nsp)  # forward all unknown args to `bzfs`
        log.setLevel(args.jobrunner_log_level)
        self.jobrunner_dryrun = args.jobrunner_dryrun
        assert len(args.root_dataset_pairs) > 0
        src_snapshot_plan: dict = self.validate_snapshot_plan(literal_eval(args.src_snapshot_plan), "--src-snapshot-plan")
        src_bookmark_plan: dict = self.validate_snapshot_plan(literal_eval(args.src_bookmark_plan), "--src-bookmark-plan")
        dst_snapshot_plan: dict = self.validate_snapshot_plan(literal_eval(args.dst_snapshot_plan), "--dst-snapshot-plan")
        monitor_snapshot_plan: dict = self.validate_monitor_snapshot_plan(literal_eval(args.monitor_snapshot_plan))
        localhostname: str = args.localhost if args.localhost else socket.gethostname()
        self.validate_host_name(localhostname, "--localhost")
        log.debug("localhostname: %s", localhostname)
        src_hosts: list[str] = self.validate_src_hosts(self.parse_src_hosts_from_cli_or_stdin(args.src_hosts))
        basis_src_hosts: list[str] = src_hosts
        nb_src_hosts: int = len(basis_src_hosts)
        log.debug("src_hosts before subsetting: %s", src_hosts)
        if args.src_host is not None:  # retain only the src hosts that are also contained in args.src_host
            assert isinstance(args.src_host, list)
            retain_src_hosts: set[str] = set(args.src_host)
            self.validate_is_subset(retain_src_hosts, src_hosts, "--src-host", "--src-hosts")
            src_hosts = [host for host in src_hosts if host in retain_src_hosts]
        dst_hosts: dict[str, list[str]] = self.validate_dst_hosts(literal_eval(args.dst_hosts))
        nb_dst_hosts: int = len(dst_hosts)
        if args.dst_host is not None:  # retain only the dst hosts that are also contained in args.dst_host
            assert isinstance(args.dst_host, list)
            retain_dst_hosts: set[str] = set(args.dst_host)
            self.validate_is_subset(retain_dst_hosts, dst_hosts.keys(), "--dst-host", "--dst-hosts.keys")
            dst_hosts = {dst_host: lst for dst_host, lst in dst_hosts.items() if dst_host in retain_dst_hosts}
        retain_dst_targets: dict[str, list[str]] = self.validate_dst_hosts(literal_eval(args.retain_dst_targets))
        self.validate_is_subset(dst_hosts.keys(), retain_dst_targets.keys(), "--dst-hosts.keys", "--retain-dst-targets.keys")
        dst_root_datasets: dict[str, str] = self.validate_dst_root_datasets(literal_eval(args.dst_root_datasets))
        self.validate_is_subset(
            dst_root_datasets.keys(), retain_dst_targets.keys(), "--dst-root-dataset.keys", "--retain-dst-targets.keys"
        )
        self.validate_is_subset(dst_hosts.keys(), dst_root_datasets.keys(), "--dst-hosts.keys", "--dst-root-dataset.keys")
        bad_root_datasets: dict[str, str] = {
            dst_host: root_dataset
            for dst_host in sorted(dst_hosts.keys())
            if SRC_MAGIC_SUBSTITUTION_TOKEN not in (root_dataset := dst_root_datasets[dst_host])
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
            if root_dataset and SRC_MAGIC_SUBSTITUTION_TOKEN not in root_dataset
        }
        if len(basis_src_hosts) > 1 and len(bad_root_datasets) > 0:
            self.die(
                "Cowardly refusing to proceed as multiple source hosts are defined in the configuration, but "
                f"not all non-empty root datasets in --dst-root-datasets contain the '{SRC_MAGIC_SUBSTITUTION_TOKEN}' "
                "substitution token to prevent collisions on writing destination datasets. "
                f"Problematic subset of --dst-root-datasets: {bad_root_datasets} for src_hosts: {sorted(basis_src_hosts)}"
            )
        if args.jitter:  # randomize host order to avoid potential thundering herd problems in large distributed systems
            random.SystemRandom().shuffle(src_hosts)
            dst_hosts = shuffle_dict(dst_hosts)
        ssh_src_user: str = args.ssh_src_user or args.src_user  # --src-user is deprecated
        ssh_dst_user: str = args.ssh_dst_user or args.dst_user  # --dst-user is deprecated
        ssh_src_port: int | None = args.ssh_src_port
        ssh_dst_port: int | None = args.ssh_dst_port
        ssh_src_config_file: str | None = args.ssh_src_config_file
        ssh_dst_config_file: str | None = args.ssh_dst_config_file
        job_id: str = _sanitize(args.job_id)
        job_run: str = args.job_run if args.job_run is not None else args.jobid  # --jobid deprecat; was renamed to --job-run
        job_run = _sanitize(job_run) if job_run else uuid.uuid1().hex
        workers, workers_is_percent = args.workers
        max_workers: int = max(1, round((os.cpu_count() or 1) * workers / 100.0) if workers_is_percent else round(workers))
        worker_timeout_seconds: int = args.worker_timeout_seconds
        self.spawn_process_per_job = args.spawn_process_per_job
        username: str = pwd.getpwuid(os.getuid()).pw_name
        assert username
        loopback_ids: set[str] = {"localhost", "127.0.0.1", "::1", socket.gethostname()}  # ::1 is IPv6 loopback address
        loopback_ids.update(self.get_localhost_ips())  # union
        loopback_ids.add(localhostname)
        loopback_ids = set() if getenv_bool("disable_loopback", False) else loopback_ids
        log.log(LOG_TRACE, "loopback_ids: %s", sorted(loopback_ids))

        def zero_pad(number: int, width: int = 6) -> str:
            """Pads number with leading '0' chars to the given width."""
            return f"{number:0{width}d}"

        def jpad(jj: int, tag: str) -> str:
            """Returns ``tag`` prefixed with slash and zero padded index."""
            return "/" + zero_pad(jj) + tag

        def runpad() -> str:
            """Returns standardized subjob count suffix."""
            return job_run + SEP + zero_pad(len(subjobs))

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
            assert hostname
            assert dataset
            ssh_user = ssh_src_user if is_src else ssh_dst_user
            ssh_user = ssh_user if ssh_user else username
            lb: str = self.loopback_address
            loopbck_ids: set[str] = loopback_ids
            hostname = hostname if hostname not in loopbck_ids else (lb if lb else hostname) if username != ssh_user else "-"
            hostname = convert_ipv6(hostname)
            return f"{hostname}:{dataset}"

        def resolve_dst_dataset(dst_hostname: str, dst_dataset: str) -> str:
            """Expands ``dst_dataset`` relative to ``dst_hostname`` roots."""
            assert dst_hostname
            assert dst_dataset
            root_dataset: str | None = dst_root_datasets.get(dst_hostname)
            assert root_dataset is not None, dst_hostname  # f"Hostname '{dst_hostname}' missing in --dst-root-datasets"
            root_dataset = root_dataset.replace(SRC_MAGIC_SUBSTITUTION_TOKEN, src_host)
            root_dataset = root_dataset.replace(DST_MAGIC_SUBSTITUTION_TOKEN, dst_hostname)
            resolved_dst_dataset: str = f"{root_dataset}/{dst_dataset}" if root_dataset else dst_dataset
            utils.validate_dataset_name(resolved_dst_dataset, dst_dataset)
            return resolve_dataset(dst_hostname, resolved_dst_dataset, is_src=False)

        for src_host in src_hosts:
            assert src_host
        for dst_hostname in dst_hosts:
            assert dst_hostname
        dummy: Final[str] = DUMMY_DATASET
        lhn: Final[str] = localhostname
        bzfs_prog_header: Final[list[str]] = [BZFS_PROG_NAME, "--no-argument-file"] + unknown_args
        subjobs: dict[str, list[str]] = {}
        for i, src_host in enumerate(src_hosts):
            subjob_name: str = zero_pad(i) + "src-host"
            src_log_suffix: str = _log_suffix(localhostname, src_host, "")
            j: int = 0
            opts: list[str]

            if args.create_src_snapshots:
                opts = ["--create-src-snapshots", f"--create-src-snapshots-plan={src_snapshot_plan}", "--skip-replication"]
                self.add_log_file_opts(opts, "create-src-snapshots", job_id, runpad(), src_log_suffix)
                self.add_ssh_opts(
                    opts, ssh_src_user=ssh_src_user, ssh_src_port=ssh_src_port, ssh_src_config_file=ssh_src_config_file
                )
                opts += [POSIX_END_OF_OPTIONS_MARKER]
                opts += _flatten(_dedupe([(resolve_dataset(src_host, src), dummy) for src, dst in args.root_dataset_pairs]))
                subjob_name += "/create-src-snapshots"
                subjobs[subjob_name] = bzfs_prog_header + opts

            if args.replicate:
                j = 0
                marker: str = "replicate"
                for dst_hostname, targets in dst_hosts.items():
                    opts = self.replication_opts(
                        dst_snapshot_plan, set(targets), lhn, src_host, dst_hostname, marker, job_id, runpad()
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
                        opts += [POSIX_END_OF_OPTIONS_MARKER]
                        dataset_pairs: list[tuple[str, str]] = [
                            (resolve_dataset(src_host, src), resolve_dst_dataset(dst_hostname, dst))
                            for src, dst in args.root_dataset_pairs
                        ]
                        dataset_pairs = self.skip_nonexisting_local_dst_pools(dataset_pairs, worker_timeout_seconds)
                        if len(dataset_pairs) > 0:
                            subjobs[subjob_name + jpad(j, marker)] = bzfs_prog_header + opts + _flatten(dataset_pairs)
                            j += 1
                subjob_name = update_subjob_name(marker)

            def prune_src(
                opts: list[str], retention_plan: dict, tag: str, src_host: str = src_host, logsuffix: str = src_log_suffix
            ) -> None:
                """Creates prune subjob options for ``tag`` using ``retention_plan``."""
                opts += ["--skip-replication", f"--delete-dst-snapshots-except-plan={retention_plan}"]
                opts += [f"--daemon-frequency={args.daemon_prune_src_frequency}"]
                self.add_log_file_opts(opts, tag, job_id, runpad(), logsuffix)
                self.add_ssh_opts(  # i.e. dst=src, src=dummy
                    opts, ssh_dst_user=ssh_src_user, ssh_dst_port=ssh_src_port, ssh_dst_config_file=ssh_src_config_file
                )
                opts += [POSIX_END_OF_OPTIONS_MARKER]
                opts += _flatten(_dedupe([(dummy, resolve_dataset(src_host, src)) for src, dst in args.root_dataset_pairs]))
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
                    curr_retain_targets: set[str] = set(retain_dst_targets[dst_hostname])
                    curr_dst_snapshot_plan = {  # only retain targets that belong to the host
                        org: {target: periods for target, periods in target_periods.items() if target in curr_retain_targets}
                        for org, target_periods in dst_snapshot_plan.items()
                    }
                    opts = ["--delete-dst-snapshots", "--skip-replication"]
                    opts += [f"--delete-dst-snapshots-except-plan={curr_dst_snapshot_plan}"]
                    opts += [f"--daemon-frequency={args.daemon_prune_dst_frequency}"]
                    self.add_log_file_opts(opts, marker, job_id, runpad(), _log_suffix(lhn, src_host, dst_hostname))
                    self.add_ssh_opts(
                        opts, ssh_dst_user=ssh_dst_user, ssh_dst_port=ssh_dst_port, ssh_dst_config_file=ssh_dst_config_file
                    )
                    opts += [POSIX_END_OF_OPTIONS_MARKER]
                    dataset_pairs = [(dummy, resolve_dst_dataset(dst_hostname, dst)) for src, dst in args.root_dataset_pairs]
                    dataset_pairs = _dedupe(dataset_pairs)
                    dataset_pairs = self.skip_nonexisting_local_dst_pools(dataset_pairs, worker_timeout_seconds)
                    if len(dataset_pairs) > 0:
                        subjobs[subjob_name + jpad(j, marker)] = bzfs_prog_header + opts + _flatten(dataset_pairs)
                        j += 1
                subjob_name = update_subjob_name(marker)

            def monitor_snapshots_opts(tag: str, monitor_plan: dict, logsuffix: str) -> list[str]:
                """Returns monitor subjob options for ``tag`` and ``monitor_plan``."""
                opts = [f"--monitor-snapshots={monitor_plan}", "--skip-replication"]
                opts += [f"--daemon-frequency={args.daemon_monitor_snapshots_frequency}"]
                self.add_log_file_opts(opts, tag, job_id, runpad(), logsuffix)
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
                opts = monitor_snapshots_opts(marker, monitor_plan, src_log_suffix)
                self.add_ssh_opts(  # i.e. dst=src, src=dummy
                    opts, ssh_dst_user=ssh_src_user, ssh_dst_port=ssh_src_port, ssh_dst_config_file=ssh_src_config_file
                )
                opts += [POSIX_END_OF_OPTIONS_MARKER]
                opts += _flatten(_dedupe([(dummy, resolve_dataset(src_host, src)) for src, dst in args.root_dataset_pairs]))
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
                    opts = monitor_snapshots_opts(marker, monitor_plan, _log_suffix(lhn, src_host, dst_hostname))
                    self.add_ssh_opts(
                        opts, ssh_dst_user=ssh_dst_user, ssh_dst_port=ssh_dst_port, ssh_dst_config_file=ssh_dst_config_file
                    )
                    opts += [POSIX_END_OF_OPTIONS_MARKER]
                    dataset_pairs = [(dummy, resolve_dst_dataset(dst_hostname, dst)) for src, dst in args.root_dataset_pairs]
                    dataset_pairs = _dedupe(dataset_pairs)
                    dataset_pairs = self.skip_nonexisting_local_dst_pools(dataset_pairs, worker_timeout_seconds)
                    if len(dataset_pairs) > 0:
                        subjobs[subjob_name + jpad(j, marker)] = bzfs_prog_header + opts + _flatten(dataset_pairs)
                        j += 1
                subjob_name = update_subjob_name(marker)

        msg = dry("Ready to run %s subjobs using %s/%s src hosts: %s, %s/%s dst hosts: %s", is_dry_run=self.jobrunner_dryrun)
        log.info(
            msg, len(subjobs), len(src_hosts), nb_src_hosts, src_hosts, len(dst_hosts), nb_dst_hosts, list(dst_hosts.keys())
        )
        log.log(LOG_TRACE, "subjobs: \n%s", _pretty_print_formatter(subjobs))
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
        opts: list[str] = []
        if len(include_snapshot_plan) > 0:
            opts += [f"--include-snapshot-plan={include_snapshot_plan}"]
            self.add_log_file_opts(opts, tag, job_id, job_run, _log_suffix(localhostname, src_hostname, dst_hostname))
        return opts

    def skip_nonexisting_local_dst_pools(
        self, root_dataset_pairs: list[tuple[str, str]], timeout_secs: float | None = None
    ) -> list[tuple[str, str]]:
        """Skip datasets that point to removable destination drives that are not currently (locally) attached, if any."""

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
            sp = subprocess.run(cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True, timeout=timeout_secs)
            if sp.returncode not in (0, 1):  # 1 means dataset not found
                self.die(f"Unexpected error {sp.returncode} on checking for existing local dst pools: {sp.stderr.strip()}")
            existing_pools = {"-:" + pool for pool in sp.stdout.splitlines() if pool}
            self.cache_existing_dst_pools.update(existing_pools)  # union
        unknown_remote_dst_pools = unknown_dst_pools.difference(unknown_local_dst_pools)
        self.cache_existing_dst_pools.update(unknown_remote_dst_pools)  # union
        self.cache_known_dst_pools.update(unknown_dst_pools)  # union
        results: list[tuple[str, str]] = []
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
        ssh_src_port: int | None = None,
        ssh_dst_port: int | None = None,
        ssh_src_config_file: str | None = None,
        ssh_dst_config_file: str | None = None,
    ) -> None:
        """Appends ssh related options to ``opts`` if specified."""
        assert isinstance(opts, list)
        opts += [f"--ssh-src-user={ssh_src_user}"] if ssh_src_user else []
        opts += [f"--ssh-dst-user={ssh_dst_user}"] if ssh_dst_user else []
        opts += [f"--ssh-src-port={ssh_src_port}"] if ssh_src_port is not None else []
        opts += [f"--ssh-dst-port={ssh_dst_port}"] if ssh_dst_port is not None else []
        opts += [f"--ssh-src-config-file={ssh_src_config_file}"] if ssh_src_config_file else []
        opts += [f"--ssh-dst-config-file={ssh_dst_config_file}"] if ssh_dst_config_file else []

    @staticmethod
    def add_log_file_opts(opts: list[str], tag: str, job_id: str, job_run: str, logsuffix: str) -> None:
        """Appends standard log-file CLI options to ``opts``."""
        opts += [f"--log-file-prefix={PROG_NAME}{SEP}{tag}{SEP}"]
        opts += [f"--log-file-infix={SEP}{job_id}"]
        opts += [f"--log-file-suffix={SEP}{job_run}{logsuffix}{SEP}"]

    def run_subjobs(
        self,
        subjobs: dict[str, list[str]],
        max_workers: int,
        timeout_secs: float | None,
        work_period_seconds: float,
        jitter: bool,
    ) -> None:
        """Executes subjobs sequentially or in parallel, respecting '~' barriers.

        Design note on subjob failure, isolation and termination:
        - On subjob failure the subjob's subtree is skipped.
        - Subjob failures are converted to return codes (not exceptions) so the policy does not invoke a termination handler
          on such failures and sibling subjobs continue unaffected. This preserves per-subjob isolation, both within a single
          process (thread-per-subjob mode; default) as well as in process-per-subjob mode (``--spawn-process-per-job``).
        """
        self.stats = JobStats(len(subjobs))
        log = self.log
        num_intervals = 1 + len(subjobs) if jitter else len(subjobs)
        interval_nanos = 0 if len(subjobs) == 0 else round(1_000_000_000 * max(0.0, work_period_seconds) / num_intervals)
        assert interval_nanos >= 0
        if jitter:  # randomize job start time to avoid potential thundering herd problems in large distributed systems
            sleep_nanos = random.SystemRandom().randint(0, interval_nanos)
            log.info("Jitter: Delaying job start time by sleeping for %s ...", human_readable_duration(sleep_nanos))
            self.termination_event.wait(sleep_nanos / 1_000_000_000)  # allow early wakeup on async termination
        sorted_subjobs: list[str] = sorted(subjobs.keys())
        spawn_process_per_job: bool = self.spawn_process_per_job
        log.log(LOG_TRACE, "%s: %s", "spawn_process_per_job", spawn_process_per_job)
        if process_datasets_in_parallel_and_fault_tolerant(
            log=log,
            datasets=sorted_subjobs,
            process_dataset=lambda subjob, tid, retry: self.run_subjob(
                subjobs[subjob], name=subjob, timeout_secs=timeout_secs, spawn_process_per_job=spawn_process_per_job
            )
            == 0,
            skip_tree_on_error=lambda subjob: True,
            skip_on_error="dataset",
            max_workers=max_workers,
            interval_nanos=lambda last_update_nanos, dataset, submitted_count: interval_nanos,
            termination_event=self.termination_event,
            termination_handler=self.subprocesses.terminate_process_subtrees,
            task_name="Subjob",
            retry_policy=None,  # no retries
            dry_run=False,
            is_test_mode=self.is_test_mode,
        ):
            self.first_exception = DIE_STATUS if self.first_exception is None else self.first_exception
        stats = self.stats
        jobs_skipped = stats.jobs_all - stats.jobs_started
        msg = f"{stats}, skipped:" + percent(jobs_skipped, total=stats.jobs_all, print_total=True)
        log.info("Final Progress: %s", msg)
        assert stats.jobs_running == 0, msg
        assert stats.jobs_completed == stats.jobs_started, msg
        skipped_jobs_dict = {subjob: subjobs[subjob] for subjob in sorted_subjobs if subjob not in stats.started_job_names}
        if len(skipped_jobs_dict) > 0:
            log.debug("Skipped subjobs: \n%s", _pretty_print_formatter(skipped_jobs_dict))
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
            msg: str = stats.submit_job(name)
            log.log(LOG_TRACE, "Starting worker job: %s", cmd_str)
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
            elapsed_human: str = human_readable_duration(time.monotonic_ns() - start_time_nanos)
            if returncode != 0:
                with stats.lock:
                    if self.first_exception is None:
                        self.first_exception = DIE_STATUS if returncode is None else returncode
                log.error("Worker job failed with exit code %s in %s: %s", returncode, elapsed_human, cmd_str)
            else:
                log.debug("Worker job succeeded in %s: %s", elapsed_human, cmd_str)
            return returncode
        finally:
            msg = stats.complete_job(failed=returncode != 0, elapsed_nanos=time.monotonic_ns() - start_time_nanos)
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
            return DIE_STATUS

    def _bzfs_run_main(self, cmd: list[str]) -> None:
        """Delegates execution to :mod:`bzfs` using parsed arguments."""
        bzfs_job = bzfs.Job(termination_event=self.termination_event)
        bzfs_job.is_test_mode = self.is_test_mode
        bzfs_job.run_main(bzfs.argument_parser().parse_args(cmd[1:]), cmd)

    def run_worker_job_spawn_process_per_job(self, cmd: list[str], timeout_secs: float | None) -> int | None:
        """Spawns a subprocess for the worker job and waits for completion."""
        log = self.log
        if len(cmd) > 0 and cmd[0] == BZFS_PROG_NAME:
            cmd = [sys.executable, "-m", "bzfs_main." + cmd[0]] + cmd[1:]
        if self.jobrunner_dryrun:
            return 0
        proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, text=True)  # run job in a separate subprocess
        pid: int = proc.pid
        self.subprocesses.register_child_pid(pid)
        try:
            if self.termination_event.is_set():
                timeout_secs = 1.0 if timeout_secs is None else timeout_secs
                raise subprocess.TimeoutExpired(cmd, timeout_secs)  # do not wait for normal completion
            proc.communicate(timeout=timeout_secs)  # Wait for the subprocess to complete and exit normally
        except subprocess.TimeoutExpired:
            cmd_str = " ".join(cmd)
            if self.termination_event.is_set():
                log.error("%s", f"Terminating worker job due to async termination request: {cmd_str}")
            else:
                log.error("%s", f"Terminating worker job as it failed to complete within {timeout_secs}s: {cmd_str}")
            proc.terminate()  # Sends SIGTERM signal to job subprocess
            assert isinstance(timeout_secs, float)
            timeout_secs = min(1.0, timeout_secs)
            try:
                proc.communicate(timeout=timeout_secs)  # Wait for the subprocess to exit
            except subprocess.TimeoutExpired:
                log.error("%s", f"Killing worker job as it failed to terminate within {timeout_secs}s: {cmd_str}")
                terminate_process_subtree(root_pids=[proc.pid])  # Send SIGTERM to process subtree
                proc.kill()  # Sends SIGKILL signal to job subprocess because SIGTERM wasn't enough
                timeout_secs = min(0.025, timeout_secs)
                with contextlib.suppress(subprocess.TimeoutExpired):
                    proc.communicate(timeout=timeout_secs)  # Wait for the subprocess to exit
        finally:
            self.subprocesses.unregister_child_pid(pid)
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

    def parse_src_hosts_from_cli_or_stdin(self, raw_src_hosts: str | None) -> list:
        """Resolve --src-hosts from CLI or stdin with robust TTY/empty handling."""
        if raw_src_hosts is None:
            # If stdin is an interactive TTY, don't block waiting for input; fail clearly instead
            try:
                is_tty: bool = getattr(sys.stdin, "isatty", lambda: False)()
            except Exception:
                is_tty = False
            if is_tty:
                self.die("Missing --src-hosts and stdin is a TTY. Provide --src-hosts or pipe the list on stdin.")
            stdin_text: str = sys.stdin.read()
            if not stdin_text.strip():  # avoid literal_eval("") SyntaxError and provide a clear message
                self.die("Missing --src-hosts and stdin is empty. Provide --src-hosts or pipe a list on stdin.")
            raw_src_hosts = stdin_text
        try:
            value = literal_eval(raw_src_hosts)
        except Exception as e:
            self.die(f"Invalid --src-hosts format: {e} for input: {raw_src_hosts}")
        if not isinstance(value, list):
            example: str = format_obj(["hostname1", "hostname2"])
            self.die(f"Invalid --src-hosts: expected a Python list literal, e.g. {example} but got: {format_obj(value)}")
        return value

    def die(self, msg: str) -> NoReturn:
        """Log ``msg`` and exit the program."""
        self.log.error("%s", msg)
        utils.die(msg)

    def get_localhost_ips(self) -> set[str]:
        """Returns all network addresses of the local host, i.e. all configured addresses on all network interfaces, without
        depending on name resolution."""
        ips: set[str] = set()
        if platform.system() == "Linux":
            try:
                proc = subprocess.run(["hostname", "-I"], stdin=DEVNULL, stdout=PIPE, text=True, check=True)  # noqa: S607
            except Exception as e:
                self.log.warning("Cannot run 'hostname -I' on localhost: %s", e)
            else:
                ips = {ip for ip in proc.stdout.strip().split() if ip}
        self.log.log(LOG_TRACE, "localhost_ips: %s", sorted(ips))
        return ips


#############################################################################
class RejectArgumentAction(argparse.Action):
    """An argparse Action that immediately fails if it is ever triggered."""

    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        """Abort argument parsing if a protected option is seen."""
        parser.error(f"Security: Overriding protected argument '{option_string}' is not allowed.")


#############################################################################
def _dedupe(root_dataset_pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Returns a list with duplicate dataset pairs removed while preserving order."""
    return list(dict.fromkeys(root_dataset_pairs))


T = TypeVar("T")


def _flatten(root_dataset_pairs: Iterable[Iterable[T]]) -> list[T]:
    """Flattens an iterable of pairs into a single list."""
    return [item for pair in root_dataset_pairs for item in pair]


def _sanitize(filename: str) -> str:
    """Replaces potentially problematic characters in ``filename`` with '!'."""
    for s in (" ", "..", "/", "\\", SEP):
        filename = filename.replace(s, "!")
    return filename


def _log_suffix(localhostname: str, src_hostname: str, dst_hostname: str) -> str:
    """Returns a log file suffix in a format that contains the given hostnames."""
    return f"{SEP}{_sanitize(localhostname)}{SEP}{_sanitize(src_hostname)}{SEP}{_sanitize(dst_hostname)}"


def _pretty_print_formatter(dictionary: dict[str, Any]) -> Any:
    """Lazy JSON formatter used to avoid overhead in disabled log levels."""

    class PrettyPrintFormatter:
        """Wrapper returning formatted JSON on ``str`` conversion."""

        def __str__(self) -> str:
            import json

            return json.dumps(dictionary, indent=4, sort_keys=True)

    return PrettyPrintFormatter()


def _detect_loopback_address() -> str:
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
