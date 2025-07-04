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

"""
* Overview of the bzfs.py codebase:
* The codebase starts with docs, definition of input data and associated argument parsing into a "Params" class.
* All CLI option/parameter values are reachable from the "Params" class.
* Control flow starts in main(), far below ..., which kicks off a "Job".
* A Job runs one or more "tasks" via run_tasks(), each task replicating a separate dataset tree.
* The core replication algorithm is in run_task() and especially in replicate_datasets() and replicate_dataset().
* The filter algorithms that apply include/exclude policies are in filter_datasets() and filter_snapshots().
* The --create-src-snapshots-* and --delete-* and --compare-* and --monitor-* algorithms also start in run_task().
* Consider using an IDE/editor that can open multiple windows for the same (long) file, such as PyCharm or Sublime Text, etc.
* The main retry logic is in run_with_retries() and clear_resumable_recv_state_if_necessary().
* Progress reporting for use during `zfs send/recv` data transfers is in class ProgressReporter.
* Executing a CLI commmand on a local or remote host is in run_ssh_command().
* Network connection management is in refresh_ssh_connection_if_necessary() and class ConnectionPool.
* Cache functionality can be found by searching for this regex: .*cach.*
* The parallel processing engine is in itr_ssh_cmd_parallel() and process_datasets_in_parallel_and_fault_tolerant().
* README.md is mostly auto-generated from the ArgumentParser help texts as the source of "truth", via update_readme.sh.
Simply run that script whenever you change or add ArgumentParser help text.
"""

from __future__ import annotations
import argparse
import ast
import collections
import contextlib
import dataclasses
import fcntl
import hashlib
import heapq
import itertools
import os
import platform
import random
import re
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, tzinfo
from logging import Logger
from os import stat as os_stat
from os import utime as os_utime
from os.path import exists as os_path_exists
from os.path import join as os_path_join
from pathlib import Path
from subprocess import DEVNULL, PIPE, CalledProcessError
from typing import (
    IO,
    Any,
    Callable,
    Counter,
    Final,
    Generator,
    Iterable,
    Iterator,
    Literal,
    NamedTuple,
    Sequence,
    TypeVar,
    cast,
)

import bzfs_main.utils
from bzfs_main.check_range import CheckRange
from bzfs_main.connection import (
    DEDICATED,
    SHARED,
    Connection,
    ConnectionPool,
    ConnectionPools,
    decrement_injection_counter,
    maybe_inject_error,
    refresh_ssh_connection_if_necessary,
    timeout,
)
from bzfs_main.detect import (
    RemoteConfCacheItem,
    are_bookmarks_enabled,
    detect_available_programs,
    disable_prg,
    dummy_dataset,
    is_caching_snapshots,
    is_dummy,
    is_solaris_zfs,
    is_solaris_zfs_location,
    is_zpool_feature_enabled_or_active,
    zfs_version_is_at_least_2_1_0,
    zfs_version_is_at_least_2_2_0,
)
from bzfs_main.filter import (
    RankRange,
    UnixTimeRange,
    dataset_regexes,
    filter_datasets,
    filter_lines,
    filter_lines_except,
    filter_properties,
    filter_snapshots,
    snapshot_regex_filter_name,
)
from bzfs_main.incremental_send_steps import (
    incremental_send_steps,
)
from bzfs_main.loggers import (
    Tee,
    get_logger,
    get_simple_logger,
    reset_logger,
    validate_log_config_variable,
)
from bzfs_main.parallel_engine import (
    process_datasets_in_parallel_and_fault_tolerant,
)
from bzfs_main.parallel_iterator import (
    parallel_iterator,
    run_in_parallel,
)
from bzfs_main.period_anchors import (
    PeriodAnchors,
    round_datetime_up_to_duration_multiple,
)
from bzfs_main.progress_reporter import (
    ProgressReporter,
    count_num_bytes_transferred_by_zfs_send,
    pv_file_thread_separator,
)
from bzfs_main.retry import (
    Retry,
    RetryableError,
    RetryPolicy,
)
from bzfs_main.utils import (
    DONT_SKIP_DATASET,
    RegexList,
    SynchronizedBool,
    SynchronizedDict,
    compile_regexes,
    cut,
    descendants_re_suffix,
    die,
    die_status,
    drain,
    env_var_prefix,
    get_home_directory,
    getenv_bool,
    getenv_int,
    has_duplicates,
    human_readable_bytes,
    human_readable_duration,
    is_descendant,
    is_included,
    list_formatter,
    log_debug,
    log_trace,
    open_nofollow,
    percent,
    pid_exists,
    pretty_print_formatter,
    prog_name,
    relativize_dataset,
    replace_in_lines,
    replace_prefix,
    stderr_to_str,
    subprocess_run,
    terminate_process_subtree,
    unixtime_infinity_secs,
    xfinally,
    xprint,
)

# constants:
__version__ = "1.12.0-dev"
prog_author = "Wolfgang Hoschek"
critical_status = 2
warning_status = 1
still_running_status = 4
min_python_version = (3, 8)
if sys.version_info < min_python_version:
    print(f"ERROR: {prog_name} requires Python version >= {'.'.join(map(str, min_python_version))}!")
    sys.exit(die_status)
exclude_dataset_regexes_default = r"(.*/)?[Tt][Ee]?[Mm][Pp][-_]?[0-9]*"  # skip tmp datasets by default
log_dir_default = prog_name + "-logs"
create_src_snapshots_prefix_dflt = prog_name + "_"
create_src_snapshots_suffix_dflt = "_adhoc"
time_threshold_secs = 1.1  # 1 second ZFS creation time resolution + NTP clock skew is typically < 10ms
zfs_recv_groups = {"zfs_recv_o": "-o", "zfs_recv_x": "-x", "zfs_set": ""}
snapshot_regex_filter_names = frozenset({"include_snapshot_regex", "exclude_snapshot_regex"})
snapshot_filters_var = "snapshot_filters_var"
cmp_choices_items = ("src", "dst", "all")
inject_dst_pipe_fail_kbytes = 400
year_with_four_digits_regex = re.compile(r"[1-9][0-9][0-9][0-9]")  # regex for empty target shall not match non-empty target
FILE_PERMISSIONS = stat.S_IRUSR | stat.S_IWUSR  # rw------- (owner read + write)
DIR_PERMISSIONS = stat.S_IRWXU  # rwx------ (owner read + write + execute)
SNAPSHOTS_CHANGED = "snapshots_changed"  # See https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html#snapshots_changed
SHELL_CHARS = '"' + "'`~!@#$%^&*()+={}[]|;<>?,\\"


def argument_parser() -> argparse.ArgumentParser:
    create_src_snapshots_plan_example1 = str({"test": {"": {"adhoc": 1}}}).replace(" ", "")
    create_src_snapshots_plan_example2 = str({"prod": {"us-west-1": {"hourly": 36, "daily": 31}}}).replace(" ", "")
    delete_dst_snapshots_except_plan_example1 = str(
        {
            "prod": {
                "onsite": {
                    "secondly": 40,
                    "minutely": 40,
                    "hourly": 36,
                    "daily": 31,
                    "weekly": 12,
                    "monthly": 18,
                    "yearly": 5,
                }
            }
        }
    ).replace(" ", "")

    # fmt: off
    parser = argparse.ArgumentParser(
        prog=prog_name,
        allow_abbrev=False,
        formatter_class=argparse.RawTextHelpFormatter,
        description=f"""
*{prog_name} is a backup command line tool that reliably replicates ZFS snapshots from a (local or remote)
source ZFS dataset (ZFS filesystem or ZFS volume) and its descendant datasets to a (local or remote)
destination ZFS dataset to make the destination dataset a recursively synchronized copy of the source dataset,
using zfs send/receive/rollback/destroy and ssh tunnel as directed. For example, {prog_name} can be used to
incrementally replicate all ZFS snapshots since the most recent common snapshot from source to destination,
in order to help protect against data loss or ransomware.*

When run for the first time, {prog_name} replicates the dataset and all its snapshots from the source to the
destination. On subsequent runs, {prog_name} transfers only the data that has changed since the previous run,
i.e. it incrementally replicates to the destination all intermediate snapshots that have been created on
the source since the last run. Source ZFS snapshots older than the most recent common snapshot found on the
destination are auto-skipped.

Unless {prog_name} is explicitly told to create snapshots on the source, it treats the source as read-only,
thus the source remains unmodified. With the --dryrun flag, {prog_name} also treats the destination as read-only.
In normal operation, {prog_name} treats the destination as append-only. Optional CLI flags are available to
delete destination snapshots and destination datasets as directed, for example to make the destination
identical to the source if the two have somehow diverged in unforeseen ways. This easily enables
(re)synchronizing the backup from the production state, as well as restoring the production state from
backup.

In the spirit of rsync, {prog_name} supports a variety of powerful include/exclude filters that can be combined to
select which datasets, snapshots and properties to create, replicate, delete or compare.

Typically, a `cron` job on the source host runs `{prog_name}` periodically to create new snapshots and prune outdated
snapshots on the source, whereas another `cron` job on the destination host runs `{prog_name}` periodically to prune
outdated destination snapshots. Yet another `cron` job runs `{prog_name}` periodically to replicate the recently created
snapshots from the source to the destination. The frequency of these periodic activities is typically every N milliseconds,
every second, minute, hour, day, week, month and/or year (or multiples thereof).

All {prog_name} functions including snapshot creation, replication, deletion, monitoring, comparison, etc. happily work
with any snapshots in any format, even created or managed by third party ZFS snapshot management tools, including manual
zfs snapshot/destroy. All functions can also be used independently. That is, if you wish you can use {prog_name} just
for creating snapshots, or just for replicating, or just for deleting/pruning, or just for monitoring, or just for
comparing snapshot lists.

The source 'pushes to' the destination whereas the destination 'pulls from' the source. {prog_name} is installed
and executed on the 'initiator' host which can be either the host that contains the source dataset (push mode),
or the destination dataset (pull mode), or both datasets (local mode, no network required, no ssh required),
or any third-party (even non-ZFS OSX) host as long as that host is able to SSH (via standard 'ssh' OpenSSH CLI) into
both the source and destination host (pull-push mode). In pull-push mode the source 'zfs send's the data stream
to the initiator which immediately pipes the stream (without storing anything locally) to the destination
host that 'zfs receive's it. Pull-push mode means that {prog_name} need not be installed or executed on either
source or destination host. Only the underlying 'zfs' CLI must be installed on both source and destination host.
{prog_name} can run as root or non-root user, in the latter case via a) sudo or b) when granted corresponding
ZFS permissions by administrators via 'zfs allow' delegation mechanism.

{prog_name} is written in Python and continously runs a wide set of unit tests and integration tests to ensure
coverage and compatibility with old and new versions of ZFS on Linux, FreeBSD and Solaris, on all Python
versions >= 3.8 (including latest stable which is currently python-3.13).

{prog_name} is a stand-alone program with zero required dependencies, akin to a
stand-alone shell script or binary executable. It is designed to be able to run in restricted barebones server
environments. No external Python packages are required; indeed no Python package management at all is required.
You can just symlink the program wherever you like, for example into /usr/local/bin or similar, and simply run it like
any stand-alone shell script or binary executable.

{prog_name} automatically replicates the snapshots of multiple datasets in parallel for best performance.
Similarly, it quickly deletes (or monitors or compares) snapshots of multiple datasets in parallel. Atomic snapshots can be
created as frequently as every N milliseconds.

Optionally, {prog_name} applies bandwidth rate-limiting and progress monitoring (via 'pv' CLI) during 'zfs
send/receive' data transfers. When run across the network, {prog_name} also transparently inserts lightweight
data compression (via 'zstd -1' CLI) and efficient data buffering (via 'mbuffer' CLI) into the pipeline
between network endpoints during 'zfs send/receive' network transfers. If one of these utilities is not
installed this is auto-detected, and the operation continues reliably without the corresponding auxiliary
feature.

# Periodic Jobs with bzfs_jobrunner

The software also ships with the [bzfs_jobrunner](README_bzfs_jobrunner.md) companion program, which is a convenience
wrapper around `{prog_name}` that simplifies efficient periodic ZFS snapshot creation, replication, pruning, and monitoring,
across N source hosts and M destination hosts, using a single shared [jobconfig](bzfs_tests/bzfs_job_example.py) script.
For example, this simplifies the deployment of an efficient geo-replicated backup service where each of the M destination
hosts is located in a separate geographic region and pulls replicas from (the same set of) N source hosts. It also
simplifies low latency replication from a primary to a secondary or to M read replicas, or backup to removable drives, etc.

# Quickstart

* Create adhoc atomic snapshots without a schedule:

```$ {prog_name} tank1/foo/bar dummy --recursive --skip-replication --create-src-snapshots
--create-src-snapshots-plan "{create_src_snapshots_plan_example1}"```

```$ zfs list -t snapshot tank1/foo/bar

tank1/foo/bar@test_2024-11-06_08:30:05_adhoc```

* Create periodic atomic snapshots on a schedule, every hour and every day, by launching this from a periodic `cron` job:

```$ {prog_name} tank1/foo/bar dummy --recursive --skip-replication --create-src-snapshots
--create-src-snapshots-plan "{create_src_snapshots_plan_example2}"```

```$ zfs list -t snapshot tank1/foo/bar

tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_daily

tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_hourly```

Note: A periodic snapshot is created if it is due per the schedule indicated by its suffix (e.g. `_daily` or `_hourly`
or `_minutely` or `_2secondly` or `_100millisecondly`), or if the --create-src-snapshots-even-if-not-due flag is specified,
or if the most recent scheduled snapshot is somehow missing. In the latter case {prog_name} immediately creates a snapshot
(named with the current time, not backdated to the missed time), and then resumes the original schedule. If the suffix is
`_adhoc` or not a known period then a snapshot is considered non-periodic and is thus created immediately regardless of the
creation time of any existing snapshot.

* Replication example in local mode (no network, no ssh), to replicate ZFS dataset tank1/foo/bar to tank2/boo/bar:

```$ {prog_name} tank1/foo/bar tank2/boo/bar```

```$ zfs list -t snapshot tank1/foo/bar

tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_daily

tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_hourly```

```$ zfs list -t snapshot tank2/boo/bar

tank2/boo/bar@prod_us-west-1_2024-11-06_08:30:05_daily

tank2/boo/bar@prod_us-west-1_2024-11-06_08:30:05_hourly```

* Same example in pull mode:

```$ {prog_name} root@host1.example.com:tank1/foo/bar tank2/boo/bar```

* Same example in push mode:

```$ {prog_name} tank1/foo/bar root@host2.example.com:tank2/boo/bar```

* Same example in pull-push mode:

```$ {prog_name} root@host1:tank1/foo/bar root@host2:tank2/boo/bar```

* Example in local mode (no network, no ssh) to recursively replicate ZFS dataset tank1/foo/bar and its descendant
datasets to tank2/boo/bar:

```$ {prog_name} tank1/foo/bar tank2/boo/bar --recursive```

```$ zfs list -t snapshot -r tank1/foo/bar

tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_daily

tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_hourly

tank1/foo/bar/baz@prod_us-west-1_2024-11-06_08:40:00_daily

tank1/foo/bar/baz@prod_us-west-1_2024-11-06_08:40:00_hourly```

```$ zfs list -t snapshot -r tank2/boo/bar

tank2/boo/bar@prod_us-west-1_2024-11-06_08:30:05_daily

tank2/boo/bar@prod_us-west-1_2024-11-06_08:30:05_hourly

tank2/boo/bar/baz@prod_us-west-1_2024-11-06_08:40:00_daily

tank2/boo/bar/baz@prod_us-west-1_2024-11-06_08:40:00_hourly```

* Example that makes destination identical to source even if the two have drastically diverged:

```$ {prog_name} tank1/foo/bar tank2/boo/bar --recursive --force --delete-dst-datasets --delete-dst-snapshots```

* Replicate all daily snapshots created during the last 7 days, and at the same time ensure that the latest 7 daily
snapshots (per dataset) are replicated regardless of creation time:

```$ {prog_name} tank1/foo/bar tank2/boo/bar --recursive --include-snapshot-regex '.*_daily'
--include-snapshot-times-and-ranks '7 days ago..anytime' 'latest 7'```

Note: The example above compares the specified times against the standard ZFS 'creation' time property of the snapshots
(which is a UTC Unix time in integer seconds), rather than against a timestamp that may be part of the snapshot name.

* Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per dataset) are retained
regardless of creation time:

```$ {prog_name} {dummy_dataset} tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots
--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks notime 'all except latest 7'
--include-snapshot-times-and-ranks 'anytime..7 days ago'```

Note: This also prints how many GB of disk space in total would be freed if the command were to be run for real without
the --dryrun flag.

* Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per dataset) are retained
regardless of creation time. Additionally, only delete a snapshot if no corresponding snapshot or bookmark exists in
the source dataset (same as above except replace the 'dummy' source with 'tank1/foo/bar'):

```$ {prog_name} tank1/foo/bar tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots
--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks notime 'all except latest 7'
--include-snapshot-times-and-ranks '7 days ago..anytime'```

* Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per dataset) are retained
regardless of creation time. Additionally, only delete a snapshot if no corresponding snapshot exists in the source
dataset (same as above except append 'no-crosscheck'):

```$ {prog_name} tank1/foo/bar tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots
--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks notime 'all except latest 7'
--include-snapshot-times-and-ranks 'anytime..7 days ago' --delete-dst-snapshots-no-crosscheck```

* Delete all daily bookmarks older than 90 days, but retain the latest 200 daily bookmarks (per dataset) regardless
of creation time:

```$ {prog_name} {dummy_dataset} tank1/foo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots=bookmarks
--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks notime 'all except latest 200'
--include-snapshot-times-and-ranks 'anytime..90 days ago'```

* Delete all tmp datasets within tank2/boo/bar:

```$ {prog_name} {dummy_dataset} tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-datasets
--include-dataset-regex '(.*/)?tmp.*' --exclude-dataset-regex '!.*'```

* Retain all secondly snapshots that were created less than 40 seconds ago, and ensure that the latest 40
secondly snapshots (per dataset) are retained regardless of creation time. Same for 40 minutely snapshots, 36 hourly
snapshots, 31 daily snapshots, 12 weekly snapshots, 18 monthly snapshots, and 5 yearly snapshots:

```$ {prog_name} {dummy_dataset} tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots
--delete-dst-snapshots-except
--include-snapshot-regex '.*_secondly' --include-snapshot-times-and-ranks '40 seconds ago..anytime' 'latest 40'
--new-snapshot-filter-group
--include-snapshot-regex '.*_minutely' --include-snapshot-times-and-ranks '40 minutes ago..anytime' 'latest 40'
--new-snapshot-filter-group
--include-snapshot-regex '.*_hourly' --include-snapshot-times-and-ranks '36 hours ago..anytime' 'latest 36'
--new-snapshot-filter-group
--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks '31 days ago..anytime' 'latest 31'
--new-snapshot-filter-group
--include-snapshot-regex '.*_weekly' --include-snapshot-times-and-ranks '12 weeks ago..anytime' 'latest 12'
--new-snapshot-filter-group
--include-snapshot-regex '.*_monthly' --include-snapshot-times-and-ranks '18 months ago..anytime' 'latest 18'
--new-snapshot-filter-group
--include-snapshot-regex '.*_yearly' --include-snapshot-times-and-ranks '5 years ago..anytime' 'latest 5'```

For convenience, the lengthy command line above can be expressed in a more concise way, like so:

```$ {prog_name} {dummy_dataset} tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots
--delete-dst-snapshots-except-plan "{delete_dst_snapshots_except_plan_example1}"```

* Compare source and destination dataset trees recursively, for example to check if all recently taken snapshots have
been successfully replicated by a periodic job. List snapshots only contained in src (tagged with 'src'),
only contained in dst (tagged with 'dst'), and contained in both src and dst (tagged with 'all'), restricted to hourly
and daily snapshots taken within the last 7 days, excluding the last 4 hours (to allow for some slack/stragglers),
excluding temporary datasets:

```$ {prog_name} tank1/foo/bar tank2/boo/bar --skip-replication --compare-snapshot-lists=src+dst+all --recursive
--include-snapshot-regex '.*_(hourly|daily)' --include-snapshot-times-and-ranks '7 days ago..4 hours ago'
--exclude-dataset-regex '(.*/)?tmp.*'```

If the resulting TSV output file contains zero lines starting with the prefix 'src' and zero lines starting with the
prefix 'dst' then no source snapshots are missing on the destination, and no destination snapshots are missing
on the source, indicating that the periodic replication and pruning jobs perform as expected. The TSV output is sorted
by dataset, and by ZFS creation time within each dataset - the first and last line prefixed with 'all' contains the
metadata of the oldest and latest common snapshot, respectively. The --compare-snapshot-lists option also directly
logs various summary stats, such as the metadata of the latest common snapshot, latest snapshots and oldest snapshots,
as well as the time diff between the latest common snapshot and latest snapshot only in src (and only in dst), as well
as how many src snapshots and how many GB of data are missing on dst, etc.

* Example with further options:

```$ {prog_name} tank1/foo/bar root@host2.example.com:tank2/boo/bar --recursive
--exclude-snapshot-regex '.*_(secondly|minutely)' --exclude-snapshot-regex 'test_.*'
--include-snapshot-times-and-ranks '7 days ago..anytime' 'latest 7' --exclude-dataset /tank1/foo/bar/temporary
--exclude-dataset /tank1/foo/bar/baz/trash --exclude-dataset-regex '(.*/)?private'
--exclude-dataset-regex '(.*/)?[Tt][Ee]?[Mm][Pp][-_]?[0-9]*'```
""")  # noqa: S608

    parser.add_argument(
        "--no-argument-file", action="store_true",
        # help="Disable support for reading the names of datasets and snapshots from a file.\n\n")
        help=argparse.SUPPRESS)
    parser.add_argument(
        "root_dataset_pairs", nargs="+", action=DatasetPairsAction, metavar="SRC_DATASET DST_DATASET",
        help="SRC_DATASET: "
             "Source ZFS dataset (and its descendants) that will be replicated. Can be a ZFS filesystem or ZFS volume. "
             "Format is [[user@]host:]dataset. The host name can also be an IPv4 address (or an IPv6 address where "
             "each ':' colon character must be replaced with a '|' pipe character for disambiguation). If the "
             "host name is '-', the dataset will be on the local host, and the corresponding SSH leg will be omitted. "
             "The same is true if the host is omitted and the dataset does not contain a ':' colon at the same time. "
             "Local dataset examples: `tank1/foo/bar`, `tank1`, `-:tank1/foo/bar:baz:boo` "
             "Remote dataset examples: `host:tank1/foo/bar`, `host.example.com:tank1/foo/bar`, "
             "`root@host:tank`, `root@host.example.com:tank1/foo/bar`, `user@127.0.0.1:tank1/foo/bar:baz:boo`, "
             "`user@||1:tank1/foo/bar:baz:boo`. "
             "The first component of the ZFS dataset name is the ZFS pool name, here `tank1`. "
             "If the option starts with a `+` prefix then dataset names are read from the UTF-8 text file given "
             "after the `+` prefix, with each line in the file containing a SRC_DATASET and a DST_DATASET, "
             "separated by a tab character. Example: `+root_dataset_names.txt`, `+/path/to/root_dataset_names.txt`\n\n"
             "DST_DATASET: "
             "Destination ZFS dataset for replication and deletion. Has same naming format as SRC_DATASET. During "
             "replication, destination datasets that do not yet exist are created as necessary, along with their "
             "parent and ancestors.\n\n"
             f"*Performance Note:* {prog_name} automatically replicates multiple datasets in parallel. It replicates "
             "snapshots in parallel across datasets and serially within a dataset. All child datasets of a dataset "
             "may be processed in parallel. For consistency, processing of a dataset only starts after processing of "
             "all its ancestor datasets has completed. Further, when a thread is ready to start processing another "
             "dataset, it chooses the next dataset wrt. lexicographical sort order from the datasets that are "
             "currently available for start of processing. Initially, only the roots of the selected dataset subtrees "
             "are available for start of processing. The degree of parallelism is configurable with the --threads "
             "option (see below).\n\n")
    parser.add_argument(
        "--recursive", "-r", action="store_true",
        help="During snapshot creation, replication, deletion and comparison, also consider descendant datasets, i.e. "
             "datasets within the dataset tree, including children, and children of children, etc.\n\n")
    parser.add_argument(
        "--include-dataset", action=FileOrLiteralAction, nargs="+", default=[], metavar="DATASET",
        help="During snapshot creation, replication, deletion and comparison, select any ZFS dataset (and its descendants) "
             "that is contained within SRC_DATASET (DST_DATASET in case of deletion) if its dataset name is one of the "
             "given include dataset names but none of the exclude dataset names. If a dataset is excluded its descendants "
             "are automatically excluded too, and this decision is never reconsidered even for the descendants because "
             "exclude takes precedence over include.\n\n"
             "A dataset name is absolute if the specified dataset is prefixed by `/`, e.g. `/tank/baz/tmp`. "
             "Otherwise the dataset name is relative wrt. source and destination, e.g. `baz/tmp` if the source "
             "is `tank`.\n\n"
             "This option is automatically translated to an --include-dataset-regex (see below) and can be "
             "specified multiple times.\n\n"
             "If the option starts with a `+` prefix then dataset names are read from the newline-separated "
             "UTF-8 text file given after the `+` prefix, one dataset per line inside of the text file.\n\n"
             "Examples: `/tank/baz/tmp` (absolute), `baz/tmp` (relative), "
             "`+dataset_names.txt`, `+/path/to/dataset_names.txt`\n\n")
    parser.add_argument(
        "--exclude-dataset", action=FileOrLiteralAction, nargs="+", default=[], metavar="DATASET",
        help="Same syntax as --include-dataset (see above) except that the option is automatically translated to an "
             "--exclude-dataset-regex (see below).\n\n")
    parser.add_argument(
        "--include-dataset-regex", action=FileOrLiteralAction, nargs="+", default=[], metavar="REGEX",
        help="During snapshot creation, replication (and deletion) and comparison, select any ZFS dataset (and its "
             "descendants) that is contained within SRC_DATASET (DST_DATASET in case of deletion) if its relative dataset "
             "path (e.g. `baz/tmp`) wrt. SRC_DATASET (DST_DATASET in case of deletion) matches at least one of the given "
             "include regular expressions but none of the exclude regular expressions. "
             "If a dataset is excluded its descendants are automatically excluded too, and this decision is never "
             "reconsidered even for the descendants because exclude takes precedence over include.\n\n"
             "This option can be specified multiple times. "
             "A leading `!` character indicates logical negation, i.e. the regex matches if the regex with the "
             "leading `!` character removed does not match.\n\n"
             "If the option starts with a `+` prefix then regex names are read from the newline-separated "
             "UTF-8 text file given after the `+` prefix, one regex per line inside of the text file.\n\n"
             "Default: `.*` (include all datasets).\n\n"
             "Examples: `baz/tmp`, `(.*/)?doc[^/]*/(private|confidential).*`, `!public`, "
             "`+dataset_regexes.txt`, `+/path/to/dataset_regexes.txt`\n\n")
    parser.add_argument(
        "--exclude-dataset-regex", action=FileOrLiteralAction, nargs="+", default=[], metavar="REGEX",
        help="Same syntax as --include-dataset-regex (see above) except that the default is "
             f"`{exclude_dataset_regexes_default}` (exclude tmp datasets). Example: `!.*` (exclude no dataset)\n\n")
    parser.add_argument(
        "--exclude-dataset-property", default=None, action=NonEmptyStringAction, metavar="STRING",
        help="The name of a ZFS dataset user property (optional). If this option is specified, the effective value "
             "(potentially inherited) of that user property is read via 'zfs list' for each selected source dataset "
             "to determine whether the dataset will be included or excluded, as follows:\n\n"
             "a) Value is 'true' or '-' or empty string or the property is missing: Include the dataset.\n\n"
             "b) Value is 'false': Exclude the dataset and its descendants.\n\n"
             "c) Value is a comma-separated list of host names (no spaces, for example: "
             "'store001,store002'): Include the dataset if the host name of "
             f"the host executing {prog_name} is contained in the list, otherwise exclude the dataset and its "
             "descendants.\n\n"
             "If a dataset is excluded its descendants are automatically excluded too, and the property values of the "
             "descendants are ignored because exclude takes precedence over include.\n\n"
             "Examples: 'syncoid:sync', 'com.example.eng.project.x:backup'\n\n"
             "*Note:* The use of --exclude-dataset-property is discouraged for most use cases. It is more flexible, "
             "more powerful, *and* more efficient to instead use a combination of --include/exclude-dataset-regex "
             "and/or --include/exclude-dataset to achieve the same or better outcome.\n\n")
    parser.add_argument(
        "--include-snapshot-regex", action=FileOrLiteralAction, nargs="+", default=[], metavar="REGEX",
        help="During replication, deletion and comparison, select any source ZFS snapshot that has a name (i.e. the part "
             "after the '@') that matches at least one of the given include regular expressions but none of the "
             "exclude regular expressions. If a snapshot is excluded this decision is never reconsidered because "
             "exclude takes precedence over include.\n\n"
             "This option can be specified multiple times. "
             "A leading `!` character indicates logical negation, i.e. the regex matches if the regex with the "
             "leading `!` character removed does not match.\n\n"
             "Default: `.*` (include all snapshots). "
             "Examples: `test_.*`, `!prod_.*`, `.*_(hourly|frequent)`, `!.*_(weekly|daily)`\n\n"
             "*Note:* All --include/exclude-snapshot-* CLI option groups are combined into a mini filter pipeline. "
             "A filter pipeline is executed in the order given on the command line, left to right. For example if "
             "--include-snapshot-times-and-ranks (see below) is specified on the command line before "
             "--include/exclude-snapshot-regex, then --include-snapshot-times-and-ranks will be applied before "
             "--include/exclude-snapshot-regex. The pipeline results would not always be the same if the order were "
             "reversed. Order matters.\n\n"
             "*Note:* During replication, bookmarks are always retained aka selected in order to help find common "
             "snapshots between source and destination.\n\n")
    parser.add_argument(
        "--exclude-snapshot-regex", action=FileOrLiteralAction, nargs="+", default=[], metavar="REGEX",
        help="Same syntax as --include-snapshot-regex (see above) except that the default is to exclude no "
             "snapshots.\n\n")
    parser.add_argument(
        "--include-snapshot-times-and-ranks", action=TimeRangeAndRankRangeAction, nargs="+", default=[],
        metavar=("TIMERANGE", "RANKRANGE"),
        help="This option takes as input parameters a time range filter and an optional rank range filter. It "
             "separately computes the results for each filter and selects the UNION of both results. "
             "To instead use a pure rank range filter (no UNION), or a pure time range filter (no UNION), simply "
             "use 'notime' aka '0..0' to indicate an empty time range, or omit the rank range, respectively. "
             "This option can be specified multiple times.\n\n"
             "<b>*Replication Example (UNION):* </b>\n\n"
             "Specify to replicate all daily snapshots created during the last 7 days, "
             "and at the same time ensure that the latest 7 daily snapshots (per dataset) are replicated regardless "
             "of creation time, like so: "
             "`--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks '7 days ago..anytime' 'latest 7'`\n\n"
             "<b>*Deletion Example (no UNION):* </b>\n\n"
             "Specify to delete all daily snapshots older than 7 days, but ensure that the "
             "latest 7 daily snapshots (per dataset) are retained regardless of creation time, like so: "
             "`--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks notime 'all except latest 7' "
             "--include-snapshot-times-and-ranks 'anytime..7 days ago'`"
             "\n\n"
             "This helps to safely cope with irregular scenarios where no snapshots were created or received within "
             "the last 7 days, or where more than 7 daily snapshots were created within the last 7 days. It can also "
             "help to avoid accidental pruning of the last snapshot that source and destination have in common.\n\n"
             ""
             "<b>*TIMERANGE:* </b>\n\n"
             "The ZFS 'creation' time of a snapshot (and bookmark) must fall into this time range in order for the "
             "snapshot to be included. The time range consists of a 'start' time, followed by a '..' separator, "
             "followed by an 'end' time. For example '2024-01-01..2024-04-01', or 'anytime..anytime' aka `*..*` aka all "
             "times, or 'notime' aka '0..0' aka empty time range. Only snapshots (and bookmarks) in the half-open time "
             "range [start, end) are included; other snapshots (and bookmarks) are excluded. If a snapshot is excluded "
             "this decision is never reconsidered because exclude takes precedence over include. Each of the two specified "
             "times can take any of the following forms:\n\n"
             "* a) `anytime` aka `*` wildcard; represents negative or positive infinity.\n\n"
             "* b) a non-negative integer representing a UTC Unix time in seconds. Example: 1728109805\n\n"
             "* c) an ISO 8601 datetime string with or without timezone. Examples: '2024-10-05', "
             "'2024-10-05T14:48:55', '2024-10-05T14:48:55+02', '2024-10-05T14:48:55-04:30'. If the datetime string "
             "does not contain time zone info then it is assumed to be in the local time zone. Timezone string support "
             "requires Python >= 3.11.\n\n"
             "* d) a duration that indicates how long ago from the current time, using the following syntax: "
             "a non-negative integer, followed by an optional space, followed by a duration unit that is "
             "*one* of 'seconds', 'secs', 'minutes', 'mins', 'hours', 'days', 'weeks', 'months', 'years', "
             "followed by an optional space, followed by the word 'ago'. "
             "Examples: '0secs ago', '40 mins ago', '36hours ago', '90days ago', '12weeksago'.\n\n"
             "* Note: This option compares the specified time against the standard ZFS 'creation' time property of the "
             "snapshot (which is a UTC Unix time in integer seconds), rather than against a timestamp that may be "
             "part of the snapshot name. You can list the ZFS creation time of snapshots and bookmarks as follows: "
             "`zfs list -t snapshot,bookmark -o name,creation -s creation -d 1 $SRC_DATASET` (optionally add "
             "the -p flag to display UTC Unix time in integer seconds).\n\n"
             "*Note:* During replication, bookmarks are always retained aka selected in order to help find common "
             "snapshots between source and destination.\n\n"
             ""
             "<b>*RANKRANGE:* </b>\n\n"
             "Specifies to include the N (or N%%) oldest snapshots or latest snapshots, and exclude all other "
             "snapshots (default: include no snapshots). Snapshots are sorted by creation time (actually, by the "
             "'createtxg' ZFS property, which serves the same purpose but is more precise). The rank position of a "
             "snapshot is the zero-based integer position of the snapshot within that sorted list. A rank consists of the "
             "optional words 'all except' (followed by an optional space), followed by the word 'oldest' or 'latest', "
             "followed by a non-negative integer, followed by an optional '%%' percent sign. A rank range consists of a "
             "lower rank, followed by a '..' separator, followed by a higher rank. "
             "If the optional lower rank is missing it is assumed to be 0. Examples:\n\n"
             "* 'oldest 10%%' aka 'oldest 0..oldest 10%%' (include the oldest 10%% of all snapshots)\n\n"
             "* 'latest 10%%' aka 'latest 0..latest 10%%' (include the latest 10%% of all snapshots)\n\n"
             "* 'all except latest 10%%' aka 'oldest 90%%' aka 'oldest 0..oldest 90%%' (include all snapshots except the "
             "latest 10%% of all snapshots)\n\n"
             "* 'oldest 90' aka 'oldest 0..oldest 90' (include the oldest 90 snapshots)\n\n"
             "* 'latest 90' aka 'latest 0..latest 90' (include the latest 90 snapshots)\n\n"
             "* 'all except oldest 90' aka 'oldest 90..oldest 100%%' (include all snapshots except the oldest 90 snapshots)"
             "\n\n"
             "* 'all except latest 90' aka 'latest 90..latest 100%%' (include all snapshots except the latest 90 snapshots)"
             "\n\n"
             "* 'latest 1' aka 'latest 0..latest 1' (include the latest snapshot)\n\n"
             "* 'all except latest 1' aka 'latest 1..latest 100%%' (include all snapshots except the latest snapshot)\n\n"
             "* 'oldest 2' aka 'oldest 0..oldest 2' (include the oldest 2 snapshots)\n\n"
             "* 'all except oldest 2' aka 'oldest 2..oldest 100%%' (include all snapshots except the oldest 2 snapshots)\n\n"
             "* 'oldest 100%%' aka 'oldest 0..oldest 100%%' (include all snapshots)\n\n"
             "* 'oldest 0%%' aka 'oldest 0..oldest 0%%' (include no snapshots)\n\n"
             "* 'oldest 0' aka 'oldest 0..oldest 0' (include no snapshots)\n\n"
             "*Note:* If multiple RANKRANGEs are specified within a single --include-snapshot-times-and-ranks option, each "
             "subsequent rank range operates on the output of the preceding rank rage.\n\n"
             "*Note:* Percentage calculations are not based on the number of snapshots "
             "contained in the dataset on disk, but rather based on the number of snapshots arriving at the filter. "
             "For example, if only two daily snapshots arrive at the filter because a prior filter excludes hourly "
             "snapshots, then 'latest 10' will only include these two daily snapshots, and 'latest 50%%' will only "
             "include one of these two daily snapshots.\n\n"
             "*Note:* During replication, bookmarks are always retained aka selected in order to help find common "
             "snapshots between source and destination. Bookmarks do not count towards N or N%% wrt. rank.\n\n"
             "*Note:* If a snapshot is excluded this decision is never reconsidered because exclude takes precedence "
             "over include.\n\n")

    src_snapshot_plan_example = {
        "prod": {
            "onsite": {"secondly": 40, "minutely": 40, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5},
            "us-west-1": {"secondly": 0, "minutely": 0, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5},
            "eu-west-1": {"secondly": 0, "minutely": 0, "hourly": 36, "daily": 31, "weekly": 12, "monthly": 18, "yearly": 5},
        },
        "test": {
            "offsite": {"12hourly": 42, "weekly": 12},
            "onsite": {"100millisecondly": 42},
        },
    }
    parser.add_argument(
        "--include-snapshot-plan", action=IncludeSnapshotPlanAction, default=None, metavar="DICT_STRING",
        help="Replication periods to be used if replicating snapshots within the selected destination datasets. "
             "Has the same format as --create-src-snapshots-plan and --delete-dst-snapshots-except-plan (see below). "
             "Snapshots that do not match a period will not be replicated. To avoid unexpected surprises, make sure to "
             "carefully specify ALL snapshot names and periods that shall be replicated, in combination with --dryrun.\n\n"
             f"Example: `{format_dict(src_snapshot_plan_example)}`. This example will, for the organization 'prod' and the "
             "intended logical target 'onsite', replicate secondly snapshots that were created less than 40 seconds ago, "
             "yet replicate the latest 40 secondly snapshots regardless of creation time. Analog for the latest 40 minutely "
             "snapshots, latest 36 hourly snapshots, etc. "
             "Note: A zero within a period (e.g. 'hourly': 0) indicates that no snapshots shall be replicated for the given "
             "period.\n\n"
             "Note: --include-snapshot-plan is a convenience option that auto-generates a series of the following other "
             "options: --new-snapshot-filter-group, --include-snapshot-regex, --include-snapshot-times-and-ranks\n\n")
    parser.add_argument(
        "--new-snapshot-filter-group", action=NewSnapshotFilterGroupAction, nargs=0,
        help="Starts a new snapshot filter group containing separate --{include|exclude}-snapshot-* filter options. The "
             "program separately computes the results for each filter group and selects the UNION of all results. "
             "This option can be specified multiple times and serves as a separator between groups. Example:\n\n"
             "Delete all minutely snapshots older than 40 minutes, but ensure that the latest 40 minutely snapshots (per "
             "dataset) are retained regardless of creation time. Additionally, delete all hourly snapshots older than 36 "
             "hours, but ensure that the latest 36 hourly snapshots (per dataset) are retained regardless of creation time. "
             "Additionally, delete all daily snapshots older than 31 days, but ensure that the latest 31 daily snapshots "
             "(per dataset) are retained regardless of creation time: "
             f"`{prog_name} {dummy_dataset} tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots "
             "--include-snapshot-regex '.*_minutely' --include-snapshot-times-and-ranks notime 'all except latest 40' "
             "--include-snapshot-times-and-ranks 'anytime..40 minutes ago' "
             "--new-snapshot-filter-group "
             "--include-snapshot-regex '.*_hourly' --include-snapshot-times-and-ranks notime 'all except latest 36' "
             "--include-snapshot-times-and-ranks 'anytime..36 hours ago' "
             "--new-snapshot-filter-group "
             "--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks notime 'all except latest 31' "
             "--include-snapshot-times-and-ranks 'anytime..31 days ago'`\n\n")
    parser.add_argument(
        "--create-src-snapshots", action="store_true",
        help="Do nothing if the --create-src-snapshots flag is missing. Otherwise, before the replication step (see below), "
             "atomically create new snapshots of the source datasets selected via --{include|exclude}-dataset* policy. "
             "The names of the snapshots can be configured via --create-src-snapshots-* suboptions (see below). "
             "To create snapshots only, without any other processing such as replication, etc, consider using this flag "
             "together with the --skip-replication flag.\n\n"
             "A periodic snapshot is created if it is due per the schedule indicated by --create-src-snapshots-plan "
             "(for example '_daily' or '_hourly' or _'10minutely' or '_2secondly' or '_100millisecondly'), or if the "
             "--create-src-snapshots-even-if-not-due flag is specified, or if the most recent scheduled snapshot "
             f"is somehow missing. In the latter case {prog_name} immediately creates a snapshot (tagged with the current "
             "time, not backdated to the missed time), and then resumes the original schedule.\n\n"
             "If the snapshot suffix is '_adhoc' or not a known period then a snapshot is considered "
             "non-periodic and is thus created immediately regardless of the creation time of any existing snapshot.\n\n"
             "The implementation attempts to fit as many datasets as possible into a single (atomic) 'zfs snapshot' command "
             "line, using lexicographical sort order, and using 'zfs snapshot -r' to the extent that this is compatible "
             "with the actual results of the schedule and the actual results of the --{include|exclude}-dataset* pruning "
             "policy. The snapshots of all datasets that fit "
             "within the same single 'zfs snapshot' CLI invocation will be taken within the same ZFS transaction group, and "
             "correspondingly have identical 'createtxg' ZFS property (but not necessarily identical 'creation' ZFS time "
             "property as ZFS actually provides no such guarantee), and thus be consistent. Dataset names that can't fit "
             "into a single command line are spread over multiple command line invocations, respecting the limits that the "
             "operating system places on the maximum length of a single command line, per `getconf ARG_MAX`.\n\n"
             f"Note: All {prog_name} functions including snapshot creation, replication, deletion, monitoring, comparison, "
             "etc. happily work with any snapshots in any format, even created or managed by third party ZFS snapshot "
             "management tools, including manual zfs snapshot/destroy.\n\n")
    parser.add_argument(
        "--create-src-snapshots-plan", default=None, type=str, metavar="DICT_STRING",
        help="Creation periods that specify a schedule for when new snapshots shall be created on src within the selected "
             "datasets. Has the same format as --delete-dst-snapshots-except-plan.\n\n"
             f"Example: `{format_dict(src_snapshot_plan_example)}`. This example will, for the organization 'prod' and "
             "the intended logical target 'onsite', create 'secondly' snapshots every second, 'minutely' snapshots every "
             "minute, hourly snapshots every hour, and so on. "
             "It will also create snapshots for the targets 'us-west-1' and 'eu-west-1' within the 'prod' organization. "
             "In addition, it will create snapshots every 12 hours and every week for the 'test' organization, "
             "and name them as being intended for the 'offsite' replication target. Analog for snapshots that are taken "
             "every 100 milliseconds within the 'test' organization.\n\n"
             "The example creates ZFS snapshots with names like "
             "`prod_onsite_<timestamp>_secondly`, `prod_onsite_<timestamp>_minutely`, "
             "`prod_us-west-1_<timestamp>_hourly`, `prod_us-west-1_<timestamp>_daily`, "
             "`prod_eu-west-1_<timestamp>_hourly`, `prod_eu-west-1_<timestamp>_daily`, "
             "`test_offsite_<timestamp>_12hourly`, `test_offsite_<timestamp>_weekly`, and so on.\n\n"
             "Note: A period name that is missing indicates that no snapshots shall be created for the given period.\n\n"
             "The period name can contain an optional positive integer immediately preceding the time period unit, for "
             "example `_2secondly` or `_10minutely` or `_100millisecondly` to indicate that snapshots are taken every 2 "
             "seconds, or every 10 minutes, or every 100 milliseconds, respectively.\n\n")

    def argparser_escape(text: str) -> str:
        return text.replace('%', '%%')

    parser.add_argument(
        "--create-src-snapshots-timeformat", default="%Y-%m-%d_%H:%M:%S", metavar="STRFTIME_SPEC",
        help="Default is `%(default)s`. For the strftime format, see "
             "https://docs.python.org/3.11/library/datetime.html#strftime-strptime-behavior. "
             f"Examples: `{argparser_escape('%Y-%m-%d_%H:%M:%S.%f')}` (adds microsecond resolution), "
             f"`{argparser_escape('%Y-%m-%d_%H:%M:%S%z')}` (adds timezone offset), "
             f"`{argparser_escape('%Y-%m-%dT%H-%M-%S')}` (no colons).\n\n"
             "The name of the snapshot created on the src is `$org_$target_strftime(--create-src-snapshots-time*)_$period`. "
             "Example: `tank/foo@prod_us-west-1_2024-09-03_12:26:15_daily`\n\n")
    parser.add_argument(
        "--create-src-snapshots-timezone", default="", type=str, metavar="TZ_SPEC",
        help=f"Default is the local timezone of the system running {prog_name}. When creating a new snapshot on the source, "
             "fetch the current time in the specified timezone, and feed that time, and the value of "
             "--create-src-snapshots-timeformat, into the standard strftime() function to generate the timestamp portion "
             "of the snapshot name. The TZ_SPEC input parameter is of the form 'UTC' or '+HHMM' or '-HHMM' for fixed UTC "
             "offsets, or an IANA TZ identifier for auto-adjustment to daylight savings time, or the empty string to use "
             "the local timezone, for example '', 'UTC', '+0000', '+0530', '-0400', 'America/Los_Angeles', 'Europe/Vienna'. "
             "For a list of valid IANA TZ identifiers see https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List"
             "\n\nTo change the timezone not only for snapshot name creation, but in all respects for the entire program, "
             "use the standard 'TZ' Unix environment variable, like so: `export TZ=UTC`.\n\n")
    parser.add_argument(
        "--create-src-snapshots-even-if-not-due", action="store_true",
        help="Take snapshots immediately regardless of the creation time of any existing snapshot, even if snapshots "
             "are periodic and not actually due per the schedule.\n\n")
    parser.add_argument(
        "--create-src-snapshots-enable-snapshots-changed-cache", action="store_true",
        help=argparse.SUPPRESS)  # deprecated; was replaced by --cache-snapshots
    parser.add_argument(
        "--zfs-send-program-opts", type=str, default="--props --raw --compressed", metavar="STRING",
        help="Parameters to fine-tune 'zfs send' behaviour (optional); will be passed into 'zfs send' CLI. "
             "The value is split on runs of one or more whitespace characters. "
             "Default is '%(default)s'. To run `zfs send` without options, specify the empty "
             "string: `--zfs-send-program-opts=''`. "
             "See https://openzfs.github.io/openzfs-docs/man/master/8/zfs-send.8.html "
             "and https://github.com/openzfs/zfs/issues/13024\n\n")
    parser.add_argument(
        "--zfs-recv-program-opts", type=str, default="-u", metavar="STRING",
        help="Parameters to fine-tune 'zfs receive' behaviour (optional); will be passed into 'zfs receive' CLI. "
             "The value is split on runs of one or more whitespace characters. "
             "Default is '%(default)s'. To run `zfs receive` without options, specify the empty "
             "string: `--zfs-recv-program-opts=''`. "
             "Example: '-u -o canmount=noauto -o readonly=on -x keylocation -x keyformat -x encryption'. "
             "See https://openzfs.github.io/openzfs-docs/man/master/8/zfs-receive.8.html "
             "and https://openzfs.github.io/openzfs-docs/man/master/7/zfsprops.7.html\n\n")
    parser.add_argument(
        "--zfs-recv-program-opt", action="append", default=[], metavar="STRING",
        help="Parameter to fine-tune 'zfs receive' behaviour (optional); will be passed into 'zfs receive' CLI. "
             "The value can contain spaces and is not split. This option can be specified multiple times. Example: `"
             "--zfs-recv-program-opt=-o "
             "--zfs-recv-program-opt='org.zfsbootmenu:commandline=ro debug zswap.enabled=1'`\n\n")
    parser.add_argument(
        "--preserve-properties", nargs="+", default=[], metavar="STRING",
        help="On replication, preserve the current value of ZFS properties with the given names on the destination "
             "datasets. The destination ignores the property value it 'zfs receive's from the source if the property name "
             "matches one of the given blacklist values. This prevents a compromised or untrusted source from overwriting "
             "security-critical properties on the destination. The default is to preserve none, i.e. an empty blacklist.\n\n"
             "Example blacklist that protects against dangerous overwrites: "
             "mountpoint overlay sharenfs sharesmb exec setuid devices encryption keyformat keylocation volsize\n\n"
             "See https://openzfs.github.io/openzfs-docs/man/master/7/zfsprops.7.html and "
             "https://openzfs.github.io/openzfs-docs/man/master/8/zfs-receive.8.html#x\n\n"
             "Note: --preserve-properties uses the 'zfs recv -x' option and thus requires either OpenZFS >= 2.2.0 "
             "(see https://github.com/openzfs/zfs/commit/b0269cd8ced242e66afc4fa856d62be29bb5a4ff), or that "
             "'zfs send --props' is not used.\n\n")
    parser.add_argument(
        "--force-rollback-to-latest-snapshot", action="store_true",
        help="Before replication, rollback the destination dataset to its most recent destination snapshot (if there "
             "is one), via 'zfs rollback', just in case the destination dataset was modified since its most recent "
             "snapshot. This is much less invasive than the other --force* options (see below).\n\n")
    parser.add_argument(
        "--force-rollback-to-latest-common-snapshot", action="store_true",
        help="Before replication, delete destination ZFS snapshots that are more recent than the most recent common "
             "snapshot selected on the source ('conflicting snapshots'), via 'zfs rollback'. Do no rollback if no common "
             "snapshot is selected.\n\n")
    parser.add_argument(
        "--force", action="store_true",
        help="Same as --force-rollback-to-latest-common-snapshot (see above), except that additionally, if no common "
             "snapshot is selected, then delete all destination snapshots before starting replication, and proceed "
             "without aborting. Without the --force* flags, the destination dataset is treated as append-only, hence "
             "no destination snapshot that already exists is deleted, and instead the operation is aborted with an "
             "error when encountering a conflicting snapshot.\n\n"
             "Analogy: --force-rollback-to-latest-snapshot is a tiny hammer, whereas "
             "--force-rollback-to-latest-common-snapshot is a medium sized hammer, --force is a large hammer, and "
             "--force-destroy-dependents is a very large hammer. "
             "Consider using the smallest hammer that can fix the problem. No hammer is ever used by default.\n\n")
    parser.add_argument(
        "--force-destroy-dependents", action="store_true",
        help="On destination, --force and --force-rollback-to-latest-common-snapshot and --delete-* will add the "
             "'-R' flag to their use of 'zfs rollback' and 'zfs destroy', causing them to delete dependents such as "
             "clones and bookmarks. This can be very destructive and is rarely advisable.\n\n")
    parser.add_argument(
        "--force-hard", action="store_true",  # deprecated; was renamed to --force-destroy-dependents
        help=argparse.SUPPRESS)
    parser.add_argument(
        "--force-unmount", action="store_true",
        help="On destination, --force and --force-rollback-to-latest-common-snapshot will add the '-f' flag to their "
             "use of 'zfs rollback' and 'zfs destroy'.\n\n")
    parser.add_argument(
        "--force-once", "--f1", action="store_true",
        help="Use the --force option or --force-rollback-to-latest-common-snapshot option at most once to resolve a "
             "conflict, then abort with an error on any subsequent conflict. This helps to interactively resolve "
             "conflicts, one conflict at a time.\n\n")
    parser.add_argument(
        "--skip-parent", action="store_true",
        help="During replication and deletion, skip processing of the SRC_DATASET and DST_DATASET and only process "
             "their descendant datasets, i.e. children, and children of children, etc (with --recursive). No dataset "
             "is processed unless --recursive is also specified. "
             f"Analogy: `{prog_name} --recursive --skip-parent src dst` is akin to Unix `cp -r src/* dst/` whereas "
             f" `{prog_name} --recursive --skip-parent --skip-replication --delete-dst-datasets dummy dst` is akin to "
             "Unix `rm -r dst/*`\n\n")
    parser.add_argument(
        "--skip-missing-snapshots", choices=["fail", "dataset", "continue"], default="dataset", nargs="?",
        help="During replication, handle source datasets that select no snapshots (and no relevant bookmarks) "
             "as follows:\n\n"
             "a) 'fail': Abort with an error.\n\n"
             "b) 'dataset' (default): Skip the source dataset with a warning. Skip descendant datasets if "
             "--recursive and destination dataset does not exist. Otherwise skip to the next dataset.\n\n"
             "c) 'continue': Skip nothing. If destination snapshots exist, delete them (with --force) or abort "
             "with an error (without --force). If there is no such abort, continue processing with the next dataset. "
             "Eventually create empty destination dataset and ancestors if they do not yet exist and source dataset "
             "has at least one descendant that selects at least one snapshot.\n\n")
    parser.add_argument(
        "--retries", type=int, min=0, default=2, action=CheckRange, metavar="INT",
        help="The maximum number of times a retryable replication or deletion step shall be retried if it fails, for "
             "example because of network hiccups (default: %(default)s, min: %(min)s). "
             "Also consider this option if a periodic pruning script may simultaneously delete a dataset or "
             f"snapshot or bookmark while {prog_name} is running and attempting to access it.\n\n")
    parser.add_argument(
        "--retry-min-sleep-secs", type=float, min=0, default=0.125, action=CheckRange, metavar="FLOAT",
        help="The minimum duration to sleep between retries (default: %(default)s).\n\n")
    parser.add_argument(
        "--retry-max-sleep-secs", type=float, min=0, default=5 * 60, action=CheckRange, metavar="FLOAT",
        help="The maximum duration to sleep between retries initially starts with --retry-min-sleep-secs (see above), "
             "and doubles on each retry, up to the final maximum of --retry-max-sleep-secs "
             "(default: %(default)s). On each retry a random sleep time in the "
             "[--retry-min-sleep-secs, current max] range is picked. The timer resets after each operation.\n\n")
    parser.add_argument(
        "--retry-max-elapsed-secs", type=float, min=0, default=60 * 60, action=CheckRange, metavar="FLOAT",
        help="A single operation (e.g. 'zfs send/receive' of the current dataset, or deletion of a list of snapshots "
             "within the current dataset) will not be retried (or not retried anymore) once this much time has elapsed "
             "since the initial start of the operation, including retries (default: %(default)s). "
             "The timer resets after each operation completes or retries exhaust, such that subsequently failing "
             "operations can again be retried.\n\n")
    parser.add_argument(
        "--skip-on-error", choices=["fail", "tree", "dataset"], default="dataset",
        help="During replication and deletion, if an error is not retryable, or --retries has been exhausted, "
             "or --skip-missing-snapshots raises an error, proceed as follows:\n\n"
             "a) 'fail': Abort the program with an error. This mode is ideal for testing, clear "
             "error reporting, and situations where consistency trumps availability.\n\n"
             "b) 'tree': Log the error, skip the dataset tree rooted at the dataset for which the error "
             "occurred, and continue processing the next (sibling) dataset tree. "
             "Example: Assume datasets tank/user1/foo and tank/user2/bar and an error occurs while processing "
             "tank/user1. In this case processing skips tank/user1/foo and proceeds with tank/user2.\n\n"
             "c) 'dataset' (default): Same as 'tree' except if the destination dataset already exists, skip to "
             "the next dataset instead.\n\n"
             "Example: Assume datasets tank/user1/foo and tank/user2/bar and an error occurs while "
             "processing tank/user1. In this case processing skips tank/user1 and proceeds with tank/user1/foo "
             "if the destination already contains tank/user1. Otherwise processing continues with tank/user2. "
             "This mode is for production use cases that require timely forward progress even in the presence of "
             "partial failures. For example, assume the job is to backup the home directories or virtual machines "
             "of thousands of users across an organization. Even if replication of some of the datasets for some "
             "users fails due too conflicts, busy datasets, etc, the replication job will continue for the "
             "remaining datasets and the remaining users.\n\n")
    parser.add_argument(
        "--skip-replication", action="store_true",
        help="Skip replication step (see above) and proceed to the optional --delete-dst-datasets step "
             "immediately (see below).\n\n")
    parser.add_argument(
        "--delete-dst-datasets", action="store_true",
        help="Do nothing if the --delete-dst-datasets option is missing. Otherwise, after successful replication "
             "step, if any, delete existing destination datasets that are selected via --{include|exclude}-dataset* "
             "policy yet do not exist within SRC_DATASET (which can be an empty dataset, such as the hardcoded virtual "
             f"dataset named '{dummy_dataset}'!). Do not recurse without --recursive. With --recursive, never delete "
             "non-selected dataset subtrees or their ancestors.\n\n"
             "For example, if the destination contains datasets h1,h2,h3,d1 whereas source only contains h3, "
             "and the include/exclude policy selects h1,h2,h3,d1, then delete datasets h1,h2,d1 on "
             "the destination to make it 'the same'. On the other hand, if the include/exclude policy "
             "only selects h1,h2,h3 then only delete datasets h1,h2 on the destination to make it 'the same'.\n\n"
             "Example to delete all tmp datasets within tank2/boo/bar: "
             f"`{prog_name} {dummy_dataset} tank2/boo/bar --dryrun --skip-replication --recursive "
             "--delete-dst-datasets --include-dataset-regex '(.*/)?tmp.*' --exclude-dataset-regex '!.*'`\n\n")
    parser.add_argument(
        "--delete-dst-snapshots", choices=["snapshots", "bookmarks"], default=None, const="snapshots", nargs="?",
        help="Do nothing if the --delete-dst-snapshots option is missing. Otherwise, after successful "
             "replication, and successful --delete-dst-datasets step, if any, delete existing destination snapshots "
             "whose GUID does not exist within the source dataset (which can be an empty dummy dataset!) if the "
             "destination snapshots are selected by the --include/exclude-snapshot-* policy, and the destination "
             "dataset is selected via --{include|exclude}-dataset* policy. Does not recurse without --recursive.\n\n"
             "For example, if the destination dataset contains snapshots h1,h2,h3,d1 (h=hourly, d=daily) whereas "
             "the source dataset only contains snapshot h3, and the include/exclude policy selects "
             "h1,h2,h3,d1, then delete snapshots h1,h2,d1 on the destination dataset to make it 'the same'. "
             "On the other hand, if the include/exclude policy only selects snapshots h1,h2,h3 then only "
             "delete snapshots h1,h2 on the destination dataset to make it 'the same'.\n\n"
             "*Note:* To delete snapshots regardless, consider using --delete-dst-snapshots in combination with a "
             f"source that is an empty dataset, such as the hardcoded virtual dataset named '{dummy_dataset}', like so:"
             f" `{prog_name} {dummy_dataset} tank2/boo/bar --dryrun --skip-replication --delete-dst-snapshots "
             "--include-snapshot-regex '.*_daily' --recursive`\n\n"
             "*Note:* Use --delete-dst-snapshots=bookmarks to delete bookmarks instead of snapshots, in which "
             "case no snapshots are selected and the --{include|exclude}-snapshot-* filter options treat bookmarks as "
             "snapshots wrt. selecting.\n\n"
             "*Performance Note:* --delete-dst-snapshots operates on multiple datasets in parallel (and serially "
             f"within a dataset), using the same dataset order as {prog_name} replication. "
             "The degree of parallelism is configurable with the --threads option (see below).\n\n")
    parser.add_argument(
        "--delete-dst-snapshots-no-crosscheck", action="store_true",
        help="This flag indicates that --delete-dst-snapshots=snapshots shall check the source dataset only for "
             "a snapshot with the same GUID, and ignore whether a bookmark with the same GUID is present in the "
             "source dataset. Similarly, it also indicates that --delete-dst-snapshots=bookmarks shall check the "
             "source dataset only for a bookmark with the same GUID, and ignore whether a snapshot with the same GUID "
             "is present in the source dataset.\n\n")
    parser.add_argument(
        "--delete-dst-snapshots-except", action="store_true",
        help="This flag indicates that the --include/exclude-snapshot-* options shall have inverted semantics for the "
             "--delete-dst-snapshots option, thus deleting all snapshots except for the selected snapshots (within the "
             "specified datasets), instead of deleting all selected snapshots (within the specified datasets). In other "
             "words, this flag enables to specify which snapshots to retain instead of which snapshots to delete.\n\n"
             "*Synchronization vs. Backup*: When a real (non-dummy) source dataset is specified in combination with "
             "--delete-dst-snapshots-except, then any destination snapshot retained by the rules above is actually only "
             "retained if it also exists in the source dataset - __all other destination snapshots are deleted__. This is "
             "great for synchronization use cases but should __NEVER BE USED FOR LONG-TERM ARCHIVAL__. Long-term archival "
             "use cases should instead specify the `dummy` source dataset as they require an independent retention policy "
             "that is not tied to the current contents of the source dataset.\n\n")
    parser.add_argument(
        "--delete-dst-snapshots-except-plan", action=DeleteDstSnapshotsExceptPlanAction, default=None, metavar="DICT_STRING",
        help="Retention periods to be used if pruning snapshots or bookmarks within the selected destination datasets via "
             "--delete-dst-snapshots. Has the same format as --create-src-snapshots-plan. "
             "Snapshots (--delete-dst-snapshots=snapshots) or bookmarks (with --delete-dst-snapshots=bookmarks) that "
             "do not match a period will be deleted. To avoid unexpected surprises, make sure to carefully specify ALL "
             "snapshot names and periods that shall be retained, in combination with --dryrun.\n\n"
             f"Example: `{format_dict(src_snapshot_plan_example)}`. This example will, for the organization 'prod' and "
             "the intended logical target 'onsite', retain secondly snapshots that were created less than 40 seconds ago, "
             "yet retain the latest 40 secondly snapshots regardless of creation time. Analog for the latest 40 minutely "
             "snapshots, latest 36 hourly snapshots, etc. "
             "It will also retain snapshots for the targets 'us-west-1' and 'eu-west-1' within the 'prod' organization. "
             "In addition, within the 'test' organization, it will retain snapshots that are created every 12 hours and "
             "every week as specified, and name them as being intended for the 'offsite' replication target. Analog for "
             "snapshots that are taken every 100 milliseconds within the 'test' organization. "
             "All other snapshots within the selected datasets will be deleted - you've been warned!\n\n"
             "The example scans the selected ZFS datasets for snapshots with names like "
             "`prod_onsite_<timestamp>_secondly`, `prod_onsite_<timestamp>_minutely`, "
             "`prod_us-west-1_<timestamp>_hourly`, `prod_us-west-1_<timestamp>_daily`, "
             "`prod_eu-west-1_<timestamp>_hourly`, `prod_eu-west-1_<timestamp>_daily`, "
             "`test_offsite_<timestamp>_12hourly`, `test_offsite_<timestamp>_weekly`, and so on, and deletes all snapshots "
             "that do not match a retention rule.\n\n"
             "Note: A zero within a period (e.g. 'hourly': 0) indicates that no snapshots shall be retained for the given "
             "period.\n\n"
             "Note: --delete-dst-snapshots-except-plan is a convenience option that auto-generates a series of the "
             "following other options: --delete-dst-snapshots-except, "
             "--new-snapshot-filter-group, --include-snapshot-regex, --include-snapshot-times-and-ranks\n\n")
    parser.add_argument(
        "--delete-empty-dst-datasets", choices=["snapshots", "snapshots+bookmarks"], default=None,
        const="snapshots+bookmarks", nargs="?",
        help="Do nothing if the --delete-empty-dst-datasets option is missing or --recursive is missing. Otherwise, "
             "after successful replication "
             "step and successful --delete-dst-datasets and successful --delete-dst-snapshots steps, if any, "
             "delete any selected destination dataset that has no snapshot and no bookmark if all descendants of "
             "that destination dataset are also selected and do not have a snapshot or bookmark either "
             "(again, only if the existing destination dataset is selected via --{include|exclude}-dataset* policy). "
             "Never delete non-selected dataset subtrees or their ancestors.\n\n"
             "For example, if the destination contains datasets h1,d1, and the include/exclude policy "
             "selects h1,d1, then check if h1,d1 can be deleted. "
             "On the other hand, if the include/exclude policy only selects h1 then only check if h1 can be deleted.\n\n"
             "*Note:* Use --delete-empty-dst-datasets=snapshots to delete snapshot-less datasets even if they still "
             "contain bookmarks.\n\n")
    monitor_snapshot_plan_example = {
        "prod": {
            "onsite": {
                "100millisecondly": {"latest": {"warning": "300 milliseconds", "critical": "2 seconds"}},
                "secondly": {"latest": {"warning": "2 seconds", "critical": "14 seconds"}},
                "minutely": {"latest": {"warning": "30 seconds", "critical": "300 seconds"}},
                "hourly": {"latest": {"warning": "30 minutes", "critical": "300 minutes"}},
                "daily": {"latest": {"warning": "4 hours", "critical": "8 hours"}},
                "weekly": {"latest": {"warning": "2 days", "critical": "8 days"}},
                "monthly": {"latest": {"warning": "2 days", "critical": "8 days"}},
                "yearly": {"latest": {"warning": "5 days", "critical": "14 days"}},
                "10minutely": {"latest": {"warning": "0 minutes", "critical": "0 minutes"}},
            },
            "": {
                "daily": {"latest": {"warning": "4 hours", "critical": "8 hours"}},
            },
        },
    }
    parser.add_argument(
        "--monitor-snapshots", default="{}", type=str, metavar="DICT_STRING",
        help="Do nothing if the --monitor-snapshots flag is missing. Otherwise, after all other steps, "
             "alert the user if the ZFS 'creation' time property of the latest snapshot for any specified snapshot name "
             "pattern within the selected datasets is too old wrt. the specified age limit. The purpose is to check if "
             "snapshots are successfully taken on schedule, successfully replicated on schedule, and successfully pruned on "
             "schedule. Process exit code is 0, 1, 2 on OK, WARNING, CRITICAL, respectively. "
             f"Example DICT_STRING: `{format_dict(monitor_snapshot_plan_example)}`. "
             "This example alerts the user if the latest src or dst snapshot named `prod_onsite_<timestamp>_hourly` is more "
             "than 30 minutes late (i.e. more than 30+60=90 minutes old) [warning] or more than 300 minutes late (i.e. more "
             "than 300+60=360 minutes old) [critical]. "
             "Analog for the latest snapshot named `prod_<timestamp>_daily`, and so on.\n\n"
             "Note: A duration that is missing or zero (e.g. '0 minutes') indicates that no snapshots shall be checked for "
             "the given snapshot name pattern.\n\n")
    parser.add_argument(
        "--monitor-snapshots-dont-warn", action="store_true",
        help="Log a message for monitoring warnings but nonetheless exit with zero exit code.\n\n")
    parser.add_argument(
        "--monitor-snapshots-dont-crit", action="store_true",
        help="Log a message for monitoring criticals but nonetheless exit with zero exit code.\n\n")
    parser.add_argument(
        "--monitor-snapshots-no-latest-check", action="store_true",
        # help="Disable monitoring check of latest snapshot.\n\n")
        help=argparse.SUPPRESS)
    parser.add_argument(
        "--monitor-snapshots-no-oldest-check", action="store_true",
        # help="Disable monitoring check of oldest snapshot.\n\n")
        help=argparse.SUPPRESS)
    cmp_choices_dflt = "+".join(cmp_choices_items)
    cmp_choices: list[str] = []
    for i in range(len(cmp_choices_items)):
        cmp_choices += map(lambda item: "+".join(item), itertools.combinations(cmp_choices_items, i + 1))
    parser.add_argument(
        "--compare-snapshot-lists", choices=cmp_choices, default="", const=cmp_choices_dflt, nargs="?",
        help="Do nothing if the --compare-snapshot-lists option is missing. Otherwise, after successful replication "
             "step and successful --delete-dst-datasets, --delete-dst-snapshots steps and --delete-empty-dst-datasets "
             "steps, if any, proceed as follows:\n\n"
             "Compare source and destination dataset trees recursively wrt. snapshots, for example to check if all "
             "recently taken snapshots have been successfully replicated by a periodic job.\n\n"
             "Example: List snapshots only contained in source (tagged with 'src'), only contained in destination "
             "(tagged with 'dst'), and contained in both source and destination (tagged with 'all'), restricted to "
             "hourly and daily snapshots taken within the last 7 days, excluding the last 4 hours (to allow for some "
             "slack/stragglers), excluding temporary datasets: "
             f"`{prog_name} tank1/foo/bar tank2/boo/bar --skip-replication "
             "--compare-snapshot-lists=src+dst+all --recursive --include-snapshot-regex '.*_(hourly|daily)' "
             "--include-snapshot-times-and-ranks '7 days ago..4 hours ago' --exclude-dataset-regex 'tmp.*'`\n\n"
             "This outputs a TSV file containing the following columns:\n\n"
             "`location creation_iso createtxg rel_name guid root_dataset rel_dataset name creation written`\n\n"
             "Example output row:\n\n"
             "`src 2024-11-06_08:30:05 17435050 /foo@test_2024-11-06_08:30:05_daily 2406491805272097867 tank1/src "
             "/foo tank1/src/foo@test_2024-10-06_08:30:04_daily 1730878205 24576`\n\n"
             "If the TSV output file contains zero lines starting with the prefix 'src' and zero lines starting with "
             "the prefix 'dst' then no source snapshots are missing on the destination, and no destination "
             "snapshots are missing on the source, indicating that the periodic replication and pruning jobs perform "
             "as expected. The TSV output is sorted by rel_dataset, and by ZFS creation time within each rel_dataset "
             "- the first and last line prefixed with 'all' contains the metadata of the oldest and latest common "
             "snapshot, respectively. Third party tools can use this info for post-processing, for example using "
             "custom scripts using 'csplit' or duckdb analytics queries.\n\n"
             "The --compare-snapshot-lists option also directly logs various summary stats, such as the metadata of "
             "the latest common snapshot, latest snapshots and oldest snapshots, as well as the time diff between the "
             "latest common snapshot and latest snapshot only in src (and only in dst), as well as how many src "
             "snapshots and how many GB of data are missing on dst, etc.\n\n"
             "*Note*: Consider omitting the 'all' flag to reduce noise and instead focus on missing snapshots only, "
             "like so: --compare-snapshot-lists=src+dst \n\n"
             "*Note*: The source can also be an empty dataset, such as the hardcoded virtual dataset named "
             f"'{dummy_dataset}'.\n\n"
             "*Note*: --compare-snapshot-lists is typically *much* faster than standard 'zfs list -t snapshot' CLI "
             "usage because the former issues requests with a higher degree of parallelism than the latter. The "
             "degree is configurable with the --threads option (see below).\n\n")
    parser.add_argument(
        "--cache-snapshots", choices=["true", "false"], default="false", const="true", nargs="?",
        help="Default is '%(default)s'. If 'true', maintain a persistent local cache of recent snapshot creation times, "
             "recent successful replication times, and recent monitoring times, and compare them to a quick "
             "'zfs list -t filesystem,volume -p -o snapshots_changed' to help determine if a new snapshot shall be created "
             "on the src, and if there are any changes that need to be replicated or monitored. Enabling the cache "
             "improves performance if --create-src-snapshots and/or replication and/or --monitor-snapshots is invoked "
             "frequently (e.g. every minute via cron) over a large number of datasets, with each dataset containing a large "
             "number of snapshots, yet it is seldom for a new src snapshot to actually be created, or there are seldom any "
             "changes to replicate or monitor (e.g. a snapshot is only created every day and/or deleted every day).\n\n"
             "*Note:* This flag only has an effect on OpenZFS >= 2.2.\n\n"
             "*Note:* This flag is only relevant for snapshot creation on the src if --create-src-snapshots-even-if-not-due "
             "is not specified.\n\n")
    parser.add_argument(
        "--dryrun", "-n", choices=["recv", "send"], default=None, const="send", nargs="?",
        help="Do a dry run (aka 'no-op') to print what operations would happen if the command were to be executed "
             "for real (optional). This option treats both the ZFS source and destination as read-only. "
             "Accepts an optional argument for fine tuning that is handled as follows:\n\n"
             "a) 'recv': Send snapshot data via 'zfs send' to the destination host and receive it there via "
             "'zfs receive -n', which discards the received data there.\n\n"
             "b) 'send': Do not execute 'zfs send' and do not execute 'zfs receive'. This is a less 'realistic' form "
             "of dry run, but much faster, especially for large snapshots and slow networks/disks, as no snapshot is "
             "actually transferred between source and destination. This is the default when specifying --dryrun.\n\n"
             "Examples: --dryrun, --dryrun=send, --dryrun=recv\n\n")
    parser.add_argument(
        "--verbose", "-v", action="count", default=0,
        help="Print verbose information. This option can be specified multiple times to increase the level of "
             "verbosity. To print what ZFS/SSH operation exactly is happening (or would happen), add the `-v -v -v` "
             "flag, maybe along with --dryrun. All ZFS and SSH commands (even with --dryrun) are logged such that "
             "they can be inspected, copy-and-pasted into a terminal shell and run manually to help anticipate or "
             "diagnose issues. ERROR, WARN, INFO, DEBUG, TRACE output lines are identified by [E], [W], [I], [D], [T] "
             "prefixes, respectively.\n\n")
    parser.add_argument(
        "--quiet", "-q", action="store_true",
        help="Suppress non-error, info, debug, and trace output.\n\n")
    parser.add_argument(
        "--no-privilege-elevation", "-p", action="store_true",
        help="Do not attempt to run state changing ZFS operations 'zfs create/rollback/destroy/send/receive/snapshot' as "
             "root (via 'sudo -u root' elevation granted by administrators appending the following to /etc/sudoers: "
             "`<NON_ROOT_USER_NAME> ALL=NOPASSWD:/path/to/zfs`\n\n"
             "Instead, the --no-privilege-elevation flag is for non-root users that have been granted corresponding "
             "ZFS permissions by administrators via 'zfs allow' delegation mechanism, like so: "
             "sudo zfs allow -u $SRC_NON_ROOT_USER_NAME snapshot,destroy,send,bookmark,hold $SRC_DATASET; "
             "sudo zfs allow -u $DST_NON_ROOT_USER_NAME mount,create,receive,rollback,destroy,canmount,mountpoint,"
             "readonly,compression,encryption,keylocation,recordsize $DST_DATASET_OR_POOL.\n\n"
             "For extra security $SRC_NON_ROOT_USER_NAME should be different than $DST_NON_ROOT_USER_NAME, i.e. the "
             "sending Unix user on the source and the receiving Unix user at the destination should be separate Unix "
             "user accounts with separate private keys even if both accounts reside on the same machine, per the "
             "principle of least privilege. Further, if you do not plan to use the --force* flags and "
             "--delete-* CLI options then ZFS permissions 'rollback,destroy' can "
             "be omitted. If you do not plan to customize the respective ZFS dataset property then ZFS permissions "
             "'canmount,mountpoint,readonly,compression,encryption,keylocation,recordsize' can be omitted, arriving "
             "at the absolutely minimal set of required destination permissions: "
             "`mount,create,receive`.\n\n"
             "Also see https://openzfs.github.io/openzfs-docs/man/master/8/zfs-allow.8.html#EXAMPLES and "
             "https://tinyurl.com/9h97kh8n and "
             "https://youtu.be/o_jr13Z9f1k?si=7shzmIQJpzNJV6cq\n\n")
    parser.add_argument(
        "--no-stream", action="store_true",
        help="During replication, only replicate the most recent selected source snapshot of a dataset (using -i "
             "incrementals instead of -I incrementals), hence skip all intermediate source snapshots that may exist "
             "between that and the most recent common snapshot. If there is no common snapshot also skip all other "
             "source snapshots for the dataset, except for the most recent selected source snapshot. This option helps "
             "the destination to 'catch up' with the source ASAP, consuming a minimum of disk space, at the expense "
             "of reducing reliable options for rolling back to intermediate snapshots in the future.\n\n")
    parser.add_argument(
        "--no-resume-recv", action="store_true",
        help="Replication of snapshots via 'zfs send/receive' can be interrupted by intermittent network hiccups, "
             "reboots, hardware issues, etc. Interrupted 'zfs send/receive' operations are retried if the --retries "
             f"and --retry-* options enable it (see above). In normal operation {prog_name} automatically retries "
             "such that only the portion of the snapshot is transmitted that has not yet been fully received on the "
             "destination. For example, this helps to progressively transfer a large individual snapshot over a "
             "wireless network in a timely manner despite frequent intermittent network hiccups. This optimization is "
             "called 'resume receive' and uses the 'zfs receive -s' and 'zfs send -t' feature.\n\n"
             "The --no-resume-recv option disables this optimization such that a retry now retransmits the entire "
             "snapshot from scratch, which could slow down or even prohibit progress in case of frequent network "
             f"hiccups. {prog_name} automatically falls back to using the --no-resume-recv option if it is "
             "auto-detected that the ZFS pool does not reliably support the 'resume receive' optimization.\n\n"
             "*Note:* Snapshots that have already been fully transferred as part of the current 'zfs send/receive' "
             "operation need not be retransmitted regardless of the --no-resume-recv flag. For example, assume "
             "a single 'zfs send/receive' operation is transferring incremental snapshots 1 through 10 via "
             "'zfs send -I', but the operation fails while transferring snapshot 10, then snapshots 1 through 9 "
             "need not be retransmitted regardless of the --no-resume-recv flag, as these snapshots have already "
             "been successfully received at the destination either way.\n\n")
    parser.add_argument(
        "--create-bookmarks", choices=["all", "many", "none"], default="many",
        help=f"For increased safety, {prog_name} replication behaves as follows wrt. ZFS bookmark creation, if it is "
             "autodetected that the source ZFS pool support bookmarks:\n\n"
             "* `many` (default): Whenever it has successfully completed replication of the most recent source snapshot, "
             f"{prog_name} creates a ZFS bookmark of that snapshot, and attaches it to the source dataset. In addition, "
             f"whenever it has successfully completed a 'zfs send' operation, {prog_name} creates a ZFS bookmark of each "
             f"hourly, daily, weekly, monthly and yearly source snapshot that was sent during that 'zfs send' operation, "
             "and attaches it to the source dataset.\n\n"
             "* `all`: Whenever it has successfully completed a 'zfs send' operation, "
             f"{prog_name} creates a ZFS bookmark of each source snapshot that was sent during that 'zfs send' operation, "
             "and attaches it to the source dataset. This increases safety at the expense of some performance.\n\n"
             "* `none`: No bookmark is created.\n\n"
             "Bookmarks exist so an incremental stream can continue to be sent from the source dataset without having "
             "to keep the already replicated snapshot around on the source dataset until the next upcoming snapshot "
             "has been successfully replicated. This way you can send the snapshot from the source dataset to another "
             "host, then bookmark the snapshot on the source dataset, then delete the snapshot from the source "
             "dataset to save disk space, and then still incrementally send the next upcoming snapshot from the "
             "source dataset to the other host by referring to the bookmark.\n\n"
             "The --create-bookmarks=none option disables this safety feature but is discouraged, because bookmarks "
             "are tiny and relatively cheap and help to ensure that ZFS replication can continue even if source and "
             "destination dataset somehow have no common snapshot anymore. "
             "For example, if a pruning script has accidentally deleted too many (or even all) snapshots on the "
             "source dataset in an effort to reclaim disk space, replication can still proceed because it can use "
             "the info in the bookmark (the bookmark must still exist in the source dataset) instead of the info in "
             "the metadata of the (now missing) source snapshot.\n\n"
             "A ZFS bookmark is a tiny bit of metadata extracted from a ZFS snapshot by the 'zfs bookmark' CLI, and "
             "attached to a dataset, much like a ZFS snapshot. Note that a ZFS bookmark does not contain user data; "
             "instead a ZFS bookmark is essentially a tiny pointer in the form of the GUID of the snapshot and 64-bit "
             "transaction group number of the snapshot and creation time of the snapshot, which is sufficient to tell "
             "the destination ZFS pool how to find the destination snapshot corresponding to the source bookmark "
             "and (potentially already deleted) source snapshot. A bookmark can be fed into 'zfs send' as the "
             "source of an incremental send. Note that while a bookmark allows for its snapshot "
             "to be deleted on the source after successful replication, it still requires that its snapshot is not "
             "somehow deleted prematurely on the destination dataset, so be mindful of that. "
             f"By convention, a bookmark created by {prog_name} has the same name as its corresponding "
             "snapshot, the only difference being the leading '#' separator instead of the leading '@' separator. "
             "Also see https://www.youtube.com/watch?v=LaNgoAZeTww&t=316s.\n\n"
             "You can list bookmarks, like so: "
             "`zfs list -t bookmark -o name,guid,createtxg,creation -d 1 $SRC_DATASET`, and you can (and should) "
             "periodically prune obsolete bookmarks just like snapshots, like so: "
             "`zfs destroy $SRC_DATASET#$BOOKMARK`. Typically, bookmarks should be pruned less aggressively "
             "than snapshots, and destination snapshots should be pruned less aggressively than source snapshots. "
             "As an example starting point, here is a command that deletes all bookmarks older than "
             "90 days, but retains the latest 200 bookmarks (per dataset) regardless of creation time: "
             f"`{prog_name} {dummy_dataset} tank2/boo/bar --dryrun --recursive --skip-replication "
             "--delete-dst-snapshots=bookmarks --include-snapshot-times-and-ranks notime 'all except latest 200' "
             "--include-snapshot-times-and-ranks 'anytime..90 days ago'`\n\n")
    parser.add_argument(
        "--no-create-bookmark", action="store_true",
        help=argparse.SUPPRESS)  # deprecated; was replaced by --create-bookmarks=none
    parser.add_argument(
        "--no-use-bookmark", action="store_true",
        help=f"For increased safety, in normal replication operation {prog_name} replication also looks for bookmarks "
             "(in addition to snapshots) on the source dataset in order to find the most recent common snapshot wrt. the "
             "destination dataset, if it is auto-detected that the source ZFS pool support bookmarks. "
             "The --no-use-bookmark option disables this safety feature but is discouraged, because bookmarks help "
             "to ensure that ZFS replication can continue even if source and destination dataset somehow have no "
             "common snapshot anymore.\n\n"
             f"Note that it does not matter whether a bookmark was created by {prog_name} or a third party script, "
             "as only the GUID of the bookmark and the GUID of the snapshot is considered for comparison, and ZFS "
             "guarantees that any bookmark of a given snapshot automatically has the same GUID, transaction group "
             "number and creation time as the snapshot. Also note that you can create, delete and prune bookmarks "
             f"any way you like, as {prog_name} (without --no-use-bookmark) will happily work with whatever "
             "bookmarks currently exist, if any.\n\n")

    # ^aes256-gcm@openssh.com cipher: for speed with confidentiality and integrity
    # measure cipher perf like so: count=5000; for i in $(seq 1 3); do echo "iteration $i:"; for cipher in $(ssh -Q cipher); do dd if=/dev/zero bs=1M count=$count 2> /dev/null | ssh -c $cipher -p 40999 127.0.0.1 "(time -p cat) > /dev/null" 2>&1 | grep real | awk -v count=$count -v cipher=$cipher '{print cipher ": " count / $2 " MB/s"}'; done; done  # noqa: E501
    # see https://gbe0.com/posts/linux/server/benchmark-ssh-ciphers/
    # and https://crypto.stackexchange.com/questions/43287/what-are-the-differences-between-these-aes-ciphers

    locations = ["src", "dst"]
    for loc in locations:
        parser.add_argument(
            f"--ssh-{loc}-user", type=str, metavar="STRING",
            help=f"Remote SSH username on {loc} host to connect to (optional). Overrides username given in "
                 f"{loc.upper()}_DATASET.\n\n")
    for loc in locations:
        parser.add_argument(
            f"--ssh-{loc}-host", type=str, metavar="STRING",
            help=f"Remote SSH hostname of {loc} host to connect to (optional). Can also be an IPv4 or IPv6 address. "
                 f"Overrides hostname given in {loc.upper()}_DATASET.\n\n")
    for loc in locations:
        parser.add_argument(
            f"--ssh-{loc}-port", type=int, metavar="INT",
            help=f"Remote SSH port on {loc} host to connect to (optional).\n\n")
    for loc in locations:
        parser.add_argument(
            f"--ssh-{loc}-config-file", type=str, action=SSHConfigFileNameAction, metavar="FILE",
            help=f"Path to SSH ssh_config(5) file to connect to {loc} (optional); will be passed into ssh -F CLI. "
                 "The basename must contain the substring 'bzfs_ssh_config'.\n\n")
    parser.add_argument(
        "--timeout", default=None, metavar="DURATION",
        # help="Exit the program (or current task with non-zero --daemon-lifetime) with an error after this much time has "
        #      "elapsed. Default is to never timeout. Examples: '600 seconds', '90 minutes', '10years'\n\n")
        help=argparse.SUPPRESS)
    threads_default = 100  # percent
    parser.add_argument(
        "--threads", min=1, max=1600, default=(threads_default, True), action=CheckPercentRange, metavar="INT[%]",
        help="The maximum number of threads to use for parallel operations; can be given as a positive integer, "
             f"optionally followed by the %% percent character (min: %(min)s, default: {threads_default}%%). Percentages "
             "are relative to the number of CPU cores on the machine. Example: 200%% uses twice as many threads as "
             "there are cores on the machine; 75%% uses num_threads = num_cores * 0.75. Currently this option only "
             "applies to dataset and snapshot replication, --create-src-snapshots, --delete-dst-snapshots, "
             "--delete-empty-dst-datasets, --monitor-snapshots and --compare-snapshot-lists. The ideal value for this "
             "parameter depends on the use case and its performance requirements, as well as the number of available CPU "
             "cores and the parallelism offered by SSDs vs. HDDs, ZFS topology and configuration, as well as the network "
             "bandwidth and other workloads simultaneously running on the system. The current default is geared towards a "
             "high degreee of parallelism, and as such may perform poorly on HDDs. Examples: 1, 4, 75%%, 150%%\n\n")
    parser.add_argument(
        "--max-concurrent-ssh-sessions-per-tcp-connection", type=int, min=1, default=8, action=CheckRange, metavar="INT",
        help=f"For best throughput, {prog_name} uses multiple SSH TCP connections in parallel, as indicated by "
             "--threads (see above). For best startup latency, each such parallel TCP connection can carry a "
             "maximum of S concurrent SSH sessions, where "
             "S=--max-concurrent-ssh-sessions-per-tcp-connection (default: %(default)s, min: %(min)s). "
             "Concurrent SSH sessions are mostly used for metadata operations such as listing ZFS datasets and their "
             "snapshots. This client-side max sessions parameter must not be higher than the server-side "
             "sshd_config(5) MaxSessions parameter (which defaults to 10, see "
             "https://manpages.ubuntu.com/manpages/man5/sshd_config.5.html).\n\n"
             f"*Note:* For better throughput, {prog_name} uses one dedicated TCP connection per ZFS "
             "send/receive operation such that the dedicated connection is never used by any other "
             "concurrent SSH session, effectively ignoring the value of the "
             "--max-concurrent-ssh-sessions-per-tcp-connection parameter in the ZFS send/receive case.\n\n")
    parser.add_argument(
        "--bwlimit", default=None, action=NonEmptyStringAction, metavar="STRING",
        help="Sets 'pv' bandwidth rate limit for zfs send/receive data transfer (optional). Example: `100m` to cap "
             "throughput at 100 MB/sec. Default is unlimited. Also see "
             "https://manpages.ubuntu.com/manpages/man1/pv.1.html\n\n")
    parser.add_argument(
        "--daemon-lifetime", default="0 seconds", metavar="DURATION",
        # help="Exit the daemon after this much time has elapsed. Default is '0 seconds', i.e. no daemon mode. "
        #      "Examples: '600 seconds', '86400 seconds', '1000years'\n\n")
        help=argparse.SUPPRESS)
    parser.add_argument(
        "--daemon-frequency", default="minutely", metavar="STRING",
        # help="Run a daemon iteration every N time units. Default is '%(default)s'. "
        #      "Examples: '100 millisecondly', '10secondly, 'minutely' to request the daemon to run every 100 milliseconds, "
        #      "or every 10 seconds, or every minute, respectively. Only has an effect if --daemon-lifetime is nonzero.\n\n")
        help=argparse.SUPPRESS)
    parser.add_argument(
        "--daemon-remote-conf-cache-ttl", default="300 seconds", metavar="DURATION",
        # help="The Time-To-Live for the remote host configuration cache, which stores available programs and "
        #      f"ZFS features. After this duration, {prog_name} will re-detect the remote environment. Set to '0 seconds' "
        #      "to re-detect on every daemon iteration. Default: %(default)s.\n\n")
        help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-estimate-send-size", action="store_true",
        # help="Skip 'zfs send -n -v'. This may improve performance if replicating small snapshots at high frequency.\n\n")
        help=argparse.SUPPRESS)

    def hlp(program: str) -> str:
        return f"The name of the '{program}' executable (optional). Default is '{program}'. "

    msg = f"Use '{disable_prg}' to disable the use of this program.\n\n"
    parser.add_argument(
        "--compression-program", default="zstd", choices=["zstd", "lz4", "pzstd", "pigz", "gzip", "bzip2", disable_prg],
        help=hlp("zstd") + msg.rstrip() + " The use is auto-disabled if data is transferred locally instead of via the "
                                          "network. This option is about transparent compression-on-the-wire, not about "
                                          "compression-at-rest.\n\n")
    parser.add_argument(
        "--compression-program-opts", default="-1", metavar="STRING",
        help="The options to be passed to the compression program on the compression step (optional). "
             "Default is '%(default)s' (fastest).\n\n")
    parser.add_argument(
        "--mbuffer-program", default="mbuffer", choices=["mbuffer", disable_prg],
        help=hlp("mbuffer") + msg.rstrip() + " The use is auto-disabled if data is transferred locally "
                                             "instead of via the network. This tool is used to smooth out the rate "
                                             "of data flow and prevent bottlenecks caused by network latency or "
                                             "speed fluctuation.\n\n")
    parser.add_argument(
        "--mbuffer-program-opts", default="-q -m 128M", metavar="STRING",
        help="Options to be passed to 'mbuffer' program (optional). Default: '%(default)s'.\n\n")
    parser.add_argument(
        "--ps-program", default="ps", choices=["ps", disable_prg],
        help=hlp("ps") + msg)
    parser.add_argument(
        "--pv-program", default="pv", choices=["pv", disable_prg],
        help=hlp("pv") + msg.rstrip() + " This is used for bandwidth rate-limiting and progress monitoring.\n\n")
    parser.add_argument(
        "--pv-program-opts", metavar="STRING",
        default="--progress --timer --eta --fineta --rate --average-rate --bytes --interval=1 --width=120 --buffer-size=2M",
        help="The options to be passed to the 'pv' program (optional). Default: '%(default)s'.\n\n")
    parser.add_argument(
        "--shell-program", default="sh", choices=["sh", disable_prg],
        help=hlp("sh") + msg)
    parser.add_argument(
        "--ssh-program", default="ssh", choices=["ssh", "hpnssh", disable_prg],
        help=hlp("ssh") + msg)
    parser.add_argument(
        "--sudo-program", default="sudo", choices=["sudo", disable_prg],
        help=hlp("sudo") + msg)
    parser.add_argument(
        "--zpool-program", default="zpool", choices=["zpool", disable_prg],
        help=hlp("zpool") + msg)
    parser.add_argument(
        "--log-dir", type=str, action=SafeDirectoryNameAction, metavar="DIR",
        help=f"Path to the log output directory on local host (optional). Default: $HOME/{log_dir_default}. The logger "
             "that is used by default writes log files there, in addition to the console. The basename of --log-dir must "
             f"contain the substring '{log_dir_default}' as this helps prevent accidents. The current.dir symlink "
             "always points to the subdirectory containing the most recent log file. The current.log symlink "
             "always points to the most recent log file. The current.pv symlink always points to the most recent "
             "data transfer monitoring log. Run `tail --follow=name --max-unchanged-stats=1` on both symlinks to "
             "follow what's currently going on. Parallel replication generates a separate .pv file per thread. To "
             "monitor these, run something like "
             "`while true; do clear; for f in $(realpath $HOME/bzfs-logs/current/current.pv)*; "
             "do tac -s $(printf '\\r') $f | tr '\\r' '\\n' | grep -m1 -v '^$'; done; sleep 1; done`\n\n")
    h_fix = ("The path name of the log file on local host is "
             "`${--log-dir}/${--log-file-prefix}<timestamp>${--log-file-infix}${--log-file-suffix}-<random>.log`. "
             "Example: `--log-file-prefix=zrun_us-west-1_ --log-file-suffix=_daily` will generate log "
             "file names such as `zrun_us-west-1_2024-09-03_12:26:15_daily-bl4i1fth.log`\n\n")
    parser.add_argument(
        "--log-file-prefix", default="zrun_", action=SafeFileNameAction, metavar="STRING",
        help="Default is %(default)s. " + h_fix)
    parser.add_argument(
        "--log-file-infix", default="", action=SafeFileNameAction, metavar="STRING",
        help="Default is the empty string. " + h_fix)
    parser.add_argument(
        "--log-file-suffix", default="", action=SafeFileNameAction, metavar="STRING",
        help="Default is the empty string. " + h_fix)
    parser.add_argument(
        "--log-subdir", choices=["daily", "hourly", "minutely"], default="daily",
        help="Make a new subdirectory in --log-dir every day, hour or minute; write log files there. "
             "Default is '%(default)s'.")
    parser.add_argument(
        "--log-syslog-address", default=None, action=NonEmptyStringAction, metavar="STRING",
        help="Host:port of the syslog machine to send messages to (e.g. 'foo.example.com:514' or '127.0.0.1:514'), or "
             "the file system path to the syslog socket file on localhost (e.g. '/dev/log'). The default is no "
             "address, i.e. do not log anything to syslog by default. See "
             "https://docs.python.org/3/library/logging.handlers.html#sysloghandler\n\n")
    parser.add_argument(
        "--log-syslog-socktype", choices=["UDP", "TCP"], default="UDP",
        help="The socket type to use to connect if no local socket file system path is used. Default is '%(default)s'.\n\n")
    parser.add_argument(
        "--log-syslog-facility", type=int, min=0, max=7, default=1, action=CheckRange, metavar="INT",
        help="The local facility aka category that identifies msg sources in syslog "
             "(default: %(default)s, min=%(min)s, max=%(max)s).\n\n")
    parser.add_argument(
        "--log-syslog-prefix", default=prog_name, action=NonEmptyStringAction, metavar="STRING",
        help=f"The name to prepend to each message that is sent to syslog; identifies {prog_name} messages as opposed "
             "to messages from other sources. Default is '%(default)s'.\n\n")
    parser.add_argument(
        "--log-syslog-level", choices=["CRITICAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
        default="ERROR",
        help="Only send messages with equal or higher priority than this log level to syslog. Default is '%(default)s'.\n\n")
    parser.add_argument(
        "--log-config-file", default=None, action=NonEmptyStringAction, metavar="STRING",
        help="The contents of a JSON file that defines a custom python logging configuration to be used (optional). "
             "If the option starts with a `+` prefix then the contents are read from the UTF-8 JSON file given "
             "after the `+` prefix. Examples: +log_config.json, +/path/to/log_config.json. "
             "Here is an example config file that demonstrates usage: "
             "https://github.com/whoschek/bzfs/blob/main/bzfs_tests/log_config.json\n\n"
             "For more examples see "
             "https://stackoverflow.com/questions/7507825/where-is-a-complete-example-of-logging-config-dictconfig "
             "and for details see "
             "https://docs.python.org/3/library/logging.config.html#configuration-dictionary-schema\n\n"
             "*Note:* Lines starting with a # character are ignored as comments within the JSON. Also, if a line ends "
             "with a # character the portion between that # character and the preceding # character on the same line "
             "is ignored as a comment.\n\n")
    parser.add_argument(
        "--log-config-var", action=LogConfigVariablesAction, nargs="+", default=[], metavar="NAME:VALUE",
        help="User defined variables in the form of zero or more NAME:VALUE pairs (optional). "
             "These variables can be used within the JSON passed with --log-config-file (see above) via "
             "`${name[:default]}` references, which are substituted (aka interpolated) as follows:\n\n"
             "If the variable contains a non-empty CLI value then that value is used. Else if a default value for the "
             "variable exists in the JSON file that default value is used. Else the program aborts with an error. "
             "Example: In the JSON variable `${syslog_address:/dev/log}`, the variable name is 'syslog_address' "
             "and the default value is '/dev/log'. The default value is the portion after the optional : colon "
             "within the variable declaration. The default value is used if the CLI user does not specify a non-empty "
             "value via --log-config-var, for example via "
             "--log-config-var syslog_address:/path/to/socket_file or via "
             "--log-config-var syslog_address:[host,port].\n\n"
             f"{prog_name} automatically supplies the following convenience variables: "
             "`${bzfs.log_level}`, `${bzfs.log_dir}`, `${bzfs.log_file}`, `${bzfs.sub.logger}`, "
             "`${bzfs.get_default_log_formatter}`, `${bzfs.timestamp}`. "
             "For a complete list see the source code of get_dict_config_logger().\n\n")
    parser.add_argument(
        "--include-envvar-regex", action=FileOrLiteralAction, nargs="+", default=[], metavar="REGEX",
        help="On program startup, unset all Unix environment variables for which the full environment variable "
             "name matches at least one of the excludes but none of the includes. If an environment variable is "
             "included this decision is never reconsidered because include takes precedence over exclude. "
             "The purpose is to tighten security and help guard against accidental inheritance or malicious "
             "injection of environment variable values that may have unintended effects.\n\n"
             "This option can be specified multiple times. "
             "A leading `!` character indicates logical negation, i.e. the regex matches if the regex with the "
             "leading `!` character removed does not match. "
             "The default is to include no environment variables, i.e. to make no exceptions to --exclude-envvar-regex. "
             "Example that retains at least these two env vars: "
             "`--include-envvar-regex PATH "
             f"--include-envvar-regex {env_var_prefix}min_pipe_transfer_size`. "
             "Example that retains all environment variables without tightened security: `'.*'`\n\n")
    parser.add_argument(
        "--exclude-envvar-regex", action=FileOrLiteralAction, nargs="+", default=[], metavar="REGEX",
        help="Same syntax as --include-envvar-regex (see above) except that the default is to exclude no "
             f"environment variables. Example: `{env_var_prefix}.*`\n\n")

    for period, label in {"yearly": "years", "monthly": "months", "weekly": "weeks", "daily": "days", "hourly": "hours",
                          "minutely": "minutes", "secondly": "seconds", "millisecondly": "milliseconds"}.items():
        anchor_group = parser.add_argument_group(
            f"{period.title()} period anchors", "Use these options to customize when snapshots that happen "
            f"every N {label} are scheduled to be created on the source by the --create-src-snapshots option.")
        for f in [f for f in dataclasses.fields(PeriodAnchors) if f.name.startswith(period + "_")]:
            min_ = f.metadata.get("min")
            max_ = f.metadata.get("max")
            anchor_group.add_argument(
                "--" + f.name, type=int, min=min_, max=max_, default=f.default, action=CheckRange, metavar="INT",
                help=f"{f.metadata.get('help')} ({min_} ≤ x ≤ {max_}, default: %(default)s).\n\n")

    for option_name, flag in zfs_recv_groups.items():
        grup = option_name.replace("_", "-")  # one of zfs_recv_o, zfs_recv_x
        flag = "'" + flag + "'"  # one of -o or -x

        def h(text: str) -> str:
            return argparse.SUPPRESS if option_name == "zfs_set" else text  # noqa: B023

        argument_group = parser.add_argument_group(
            grup + " (Experimental)",
            description=h(f"The following group of parameters specifies additional zfs receive {flag} options that "
                          "can be used to configure the copying of ZFS dataset properties from the source dataset to "
                          "its corresponding destination dataset. The 'zfs-recv-o' group of parameters is applied "
                          "before the 'zfs-recv-x' group."))
        target_choices_items = ["full", "incremental"]
        target_choices_default = "+".join(target_choices_items)
        target_choices = target_choices_items + [target_choices_default]
        qq = "'"
        argument_group.add_argument(
            f"--{grup}-targets", choices=target_choices, default=target_choices_default,
            help=h(f"The zfs send phase or phases during which the extra {flag} options are passed to 'zfs receive'. "
                   "This can be one of the following choices: "
                   f"{', '.join([f'{qq}{x}{qq}' for x in target_choices])}. "
                   "Default is '%(default)s'. "
                   "A 'full' send is sometimes also known as an 'initial' send.\n\n"))
        msg = "Thus, -x opts do not benefit from source != 'local' (which is the default already)." \
            if flag == "'-x'" else ""
        argument_group.add_argument(
            f"--{grup}-sources", action=NonEmptyStringAction, default="local", metavar="STRING",
            help=h("The ZFS sources to provide to the 'zfs get -s' CLI in order to fetch the ZFS dataset properties "
                   f"that will be fed into the --{grup}-include/exclude-regex filter (see below). The sources are in "
                   "the form of a comma-separated list (no spaces) containing one or more of the following choices: "
                   "'local', 'default', 'inherited', 'temporary', 'received', 'none', with the default being '%(default)s'. "
                   f"Uses 'zfs get -p -s ${grup}-sources all $SRC_DATASET' to fetch the "
                   "properties to copy - https://openzfs.github.io/openzfs-docs/man/master/8/zfs-get.8.html. P.S: Note "
                   "that the existing 'zfs send --props' option does not filter and that --props only reads properties "
                   f"from the 'local' ZFS property source (https://github.com/openzfs/zfs/issues/13024). {msg}\n\n"))
        argument_group.add_argument(
            f"--{grup}-include-regex", action=FileOrLiteralAction, nargs="+", default=[], metavar="REGEX",
            help=h(f"Take the output properties of --{grup}-sources (see above) and filter them such that we only "
                   "retain the properties whose name matches at least one of the --include regexes but none of the "
                   "--exclude regexes. If a property is excluded this decision is never reconsidered because exclude "
                   f"takes precedence over include. Append each retained property to the list of {flag} options in "
                   "--zfs-recv-program-opt(s), unless another '-o' or '-x' option with the same name already exists "
                   "therein. In other words, --zfs-recv-program-opt(s) takes precedence.\n\n"
                   f"The --{grup}-include-regex option can be specified multiple times. "
                   "A leading `!` character indicates logical negation, i.e. the regex matches if the regex with the "
                   "leading `!` character removed does not match. "
                   "If the option starts with a `+` prefix then regexes are read from the newline-separated "
                   "UTF-8 text file given after the `+` prefix, one regex per line inside of the text file.\n\n"
                   f"The default is to include no properties, thus by default no extra {flag} option is appended. "
                   f"Example: `--{grup}-include-regex recordsize volblocksize`. "
                   "More examples: `.*` (include all properties), `foo bar myapp:.*` (include three regexes) "
                   f"`+{grup}_regexes.txt`, `+/path/to/{grup}_regexes.txt`\n\n"))
        argument_group.add_argument(
            f"--{grup}-exclude-regex", action=FileOrLiteralAction, nargs="+", default=[], metavar="REGEX",
            help=h(f"Same syntax as --{grup}-include-regex (see above), and the default is to exclude no properties. "
                   f"Example: --{grup}-exclude-regex encryptionroot keystatus origin volblocksize volsize\n\n"))
    parser.add_argument(
        "--version", action="version", version=f"{prog_name}-{__version__}, by {prog_author}",
        help="Display version information and exit.\n\n")
    parser.add_argument(
        "--help, -h", action="help",
        help="Show this help message and exit.\n\n")
    return parser
    # fmt: on


#############################################################################
class LogParams:
    def __init__(self, args: argparse.Namespace) -> None:
        """Option values for logging; reads from ArgumentParser via args."""
        # immutable variables:
        if args.quiet:
            self.log_level = "ERROR"
        elif args.verbose >= 2:
            self.log_level = "TRACE"
        elif args.verbose >= 1:
            self.log_level = "DEBUG"
        else:
            self.log_level = "INFO"
        self.log_config_file: str = args.log_config_file
        if self.log_config_file:
            validate_no_argument_file(self.log_config_file, args, err_prefix="--log-config-file: ")
        self.log_config_vars: dict[str, str] = dict(var.split(":", 1) for var in args.log_config_var)
        timestamp = datetime.now().isoformat(sep="_", timespec="seconds")  # 2024-09-03_12:26:15
        self.timestamp: str = timestamp
        self.home_dir: str = get_home_directory()
        log_parent_dir: str = args.log_dir if args.log_dir else os.path.join(self.home_dir, log_dir_default)
        if log_dir_default not in os.path.basename(log_parent_dir):
            die(f"Basename of --log-dir must contain the substring '{log_dir_default}', but got: {log_parent_dir}")
        sep = "_" if args.log_subdir == "daily" else ":"
        subdir = timestamp[0 : timestamp.rindex(sep) if args.log_subdir == "minutely" else timestamp.index(sep)]
        self.log_dir: str = os.path.join(log_parent_dir, subdir)  # 2024-09-03 (d), 2024-09-03_12 (h), 2024-09-03_12:26 (m)
        os.makedirs(log_parent_dir, mode=DIR_PERMISSIONS, exist_ok=True)
        validate_is_not_a_symlink("--log-dir ", log_parent_dir)
        os.makedirs(self.log_dir, mode=DIR_PERMISSIONS, exist_ok=True)
        validate_is_not_a_symlink("--log-dir subdir ", self.log_dir)
        self.log_file_prefix: str = args.log_file_prefix
        self.log_file_infix: str = args.log_file_infix
        self.log_file_suffix: str = args.log_file_suffix
        fd, self.log_file = tempfile.mkstemp(
            suffix=".log",
            prefix=f"{self.log_file_prefix}{self.timestamp}{self.log_file_infix}{self.log_file_suffix}-",
            dir=self.log_dir,
        )
        os.close(fd)
        self.pv_log_file: str = self.log_file[0 : -len(".log")] + ".pv"
        self.last_modified_cache_dir: str = os.path.join(log_parent_dir, ".cache", "last_modified")
        os.makedirs(os.path.dirname(self.last_modified_cache_dir), mode=DIR_PERMISSIONS, exist_ok=True)

        # Create/update "current" symlink to current_dir, which is a subdir containing further symlinks to log files.
        # For parallel usage, ensures there is no time window when the symlinks are inconsistent or do not exist.
        current = "current"
        dot_current_dir = os.path.join(log_parent_dir, f".{current}")
        current_dir = os.path.join(dot_current_dir, os.path.basename(self.log_file)[0 : -len(".log")])
        os.makedirs(dot_current_dir, mode=DIR_PERMISSIONS, exist_ok=True)
        validate_is_not_a_symlink("--log-dir: .current ", dot_current_dir)
        os.makedirs(current_dir, mode=DIR_PERMISSIONS, exist_ok=True)
        validate_is_not_a_symlink("--log-dir: current ", current_dir)
        create_symlink(self.log_file, current_dir, f"{current}.log")
        create_symlink(self.pv_log_file, current_dir, f"{current}.pv")
        create_symlink(self.log_dir, current_dir, f"{current}.dir")
        dst_file = os.path.join(current_dir, current)
        os.symlink(os.path.relpath(current_dir, start=log_parent_dir), dst_file)
        os.replace(dst_file, os.path.join(log_parent_dir, current))  # atomic rename
        delete_stale_files(dot_current_dir, prefix="", millis=10, dirs=True, exclude=os.path.basename(current_dir))
        self.params: Params | None = None

    def __repr__(self) -> str:
        return str(self.__dict__)


#############################################################################
class Params:
    def __init__(
        self,
        args: argparse.Namespace,
        sys_argv: list[str] | None = None,
        log_params: LogParams | None = None,
        log: Logger | None = None,
        inject_params: dict[str, bool] | None = None,
    ) -> None:
        """Option values for all aspects; reads from ArgumentParser via args."""
        # immutable variables:
        assert args is not None
        self.args: argparse.Namespace = args
        self.sys_argv: list[str] = sys_argv if sys_argv is not None else []
        assert isinstance(self.sys_argv, list)
        self.log_params: LogParams = cast(LogParams, log_params)
        self.log: Logger = cast(Logger, log)
        self.inject_params: dict[str, bool] = inject_params if inject_params is not None else {}  # for testing only
        self.one_or_more_whitespace_regex: re.Pattern = re.compile(r"\s+")
        self.two_or_more_spaces_regex: re.Pattern = re.compile(r"  +")
        self.unset_matching_env_vars(args)
        self.xperiods: SnapshotPeriods = SnapshotPeriods()

        assert len(args.root_dataset_pairs) > 0
        self.root_dataset_pairs: list[tuple[str, str]] = args.root_dataset_pairs
        self.recursive: bool = args.recursive
        self.recursive_flag: str = "-r" if args.recursive else ""

        self.dry_run: bool = args.dryrun is not None
        self.dry_run_recv: str = "-n" if self.dry_run else ""
        self.dry_run_destroy: str = self.dry_run_recv
        self.dry_run_no_send: bool = args.dryrun == "send"
        self.verbose_zfs: bool = args.verbose >= 2
        self.verbose_destroy: str = "" if args.quiet else "-v"
        self.quiet: bool = args.quiet

        self.zfs_send_program_opts: list[str] = self.fix_send_opts(self.split_args(args.zfs_send_program_opts))
        zfs_recv_program_opts: list[str] = self.split_args(args.zfs_recv_program_opts)
        for extra_opt in args.zfs_recv_program_opt:
            zfs_recv_program_opts.append(self.validate_arg_str(extra_opt, allow_all=True))
        preserve_properties = [validate_property_name(name, "--preserve-properties") for name in args.preserve_properties]
        zfs_recv_program_opts, zfs_recv_x_names = self.fix_recv_opts(zfs_recv_program_opts, frozenset(preserve_properties))
        self.zfs_recv_program_opts: list[str] = zfs_recv_program_opts
        self.zfs_recv_x_names: list[str] = zfs_recv_x_names
        if self.verbose_zfs:
            append_if_absent(self.zfs_send_program_opts, "-v")
            append_if_absent(self.zfs_recv_program_opts, "-v")
        self.zfs_full_recv_opts: list[str] = self.zfs_recv_program_opts.copy()
        cpconfigs = [CopyPropertiesConfig(group, flag, args, self) for group, flag in zfs_recv_groups.items()]
        self.zfs_recv_o_config, self.zfs_recv_x_config, self.zfs_set_config = cpconfigs

        self.force_rollback_to_latest_snapshot: bool = args.force_rollback_to_latest_snapshot
        self.force_rollback_to_latest_common_snapshot = SynchronizedBool(args.force_rollback_to_latest_common_snapshot)
        self.force: SynchronizedBool = SynchronizedBool(args.force)
        self.force_once: bool = args.force_once
        self.force_unmount: str = "-f" if args.force_unmount else ""
        force_hard: str = "-R" if args.force_destroy_dependents else ""
        self.force_hard: str = "-R" if args.force_hard else force_hard  # --force-hard is deprecated

        self.skip_parent: bool = args.skip_parent
        self.skip_missing_snapshots: str = args.skip_missing_snapshots
        self.skip_on_error: str = args.skip_on_error
        self.retry_policy: RetryPolicy = RetryPolicy(args)
        self.skip_replication: bool = args.skip_replication
        self.delete_dst_snapshots: bool = args.delete_dst_snapshots is not None
        self.delete_dst_bookmarks: bool = args.delete_dst_snapshots == "bookmarks"
        self.delete_dst_snapshots_no_crosscheck: bool = args.delete_dst_snapshots_no_crosscheck
        self.delete_dst_snapshots_except: bool = args.delete_dst_snapshots_except
        self.delete_dst_datasets: bool = args.delete_dst_datasets
        self.delete_empty_dst_datasets: bool = args.delete_empty_dst_datasets is not None
        self.delete_empty_dst_datasets_if_no_bookmarks_and_no_snapshots: bool = (
            args.delete_empty_dst_datasets == "snapshots+bookmarks"
        )
        self.compare_snapshot_lists: str = args.compare_snapshot_lists
        self.daemon_lifetime_nanos: int = 1_000_000 * parse_duration_to_milliseconds(args.daemon_lifetime)
        self.daemon_frequency: str = args.daemon_frequency
        self.enable_privilege_elevation: bool = not args.no_privilege_elevation
        self.no_stream: bool = args.no_stream
        self.resume_recv: bool = not args.no_resume_recv
        self.create_bookmarks: str = "none" if args.no_create_bookmark else args.create_bookmarks  # no_create_bookmark depr
        self.use_bookmark: bool = not args.no_use_bookmark

        self.src: Remote = Remote("src", args, self)  # src dataset, host and ssh options
        self.dst: Remote = Remote("dst", args, self)  # dst dataset, host and ssh options
        self.create_src_snapshots_config: CreateSrcSnapshotConfig = CreateSrcSnapshotConfig(args, self)
        self.monitor_snapshots_config: MonitorSnapshotsConfig = MonitorSnapshotsConfig(args, self)
        self.is_caching_snapshots: bool = args.cache_snapshots == "true"

        self.compression_program: str = self.program_name(args.compression_program)
        self.compression_program_opts: list[str] = self.split_args(args.compression_program_opts)
        if "-o" in self.compression_program_opts or "--output-file" in self.compression_program_opts:
            die("--compression-program-opts: -o and --output-file are disallowed for security reasons.")
        self.getconf_program: str = self.program_name("getconf")  # print number of CPUs on POSIX except Solaris
        self.psrinfo_program: str = self.program_name("psrinfo")  # print number of CPUs on Solaris
        self.mbuffer_program: str = self.program_name(args.mbuffer_program)
        self.mbuffer_program_opts: list[str] = self.split_args(args.mbuffer_program_opts)
        if "-o" in self.mbuffer_program_opts:
            die("--mbuffer-program-opts: -o is disallowed for security reasons.")
        self.ps_program: str = self.program_name(args.ps_program)
        self.pv_program: str = self.program_name(args.pv_program)
        self.pv_program_opts: list[str] = self.split_args(args.pv_program_opts)
        if "-f" in self.pv_program_opts or "--log-file" in self.pv_program_opts:
            die("--pv-program-opts: -f and --log-file are disallowed for security reasons.")
        self.isatty: bool = getenv_bool("isatty", True)
        if args.bwlimit:
            self.pv_program_opts += [f"--rate-limit={self.validate_arg_str(args.bwlimit)}"]
        self.shell_program_local: str = "sh"
        self.shell_program: str = self.program_name(args.shell_program)
        self.ssh_program: str = self.program_name(args.ssh_program)
        self.sudo_program: str = self.program_name(args.sudo_program)
        self.uname_program: str = self.program_name("uname")
        self.zfs_program: str = self.program_name("zfs")
        self.zpool_program: str = self.program_name(args.zpool_program)

        # no point creating complex shell pipeline commands for tiny data transfers:
        self.min_pipe_transfer_size: int = getenv_int("min_pipe_transfer_size", 1024 * 1024)
        self.max_datasets_per_batch_on_list_snaps: int = getenv_int("max_datasets_per_batch_on_list_snaps", 1024)
        self.max_datasets_per_minibatch_on_list_snaps: int = getenv_int("max_datasets_per_minibatch_on_list_snaps", -1)
        self.max_snapshots_per_minibatch_on_delete_snaps = getenv_int("max_snapshots_per_minibatch_on_delete_snaps", 2**29)
        self.dedicated_tcp_connection_per_zfs_send: bool = getenv_bool("dedicated_tcp_connection_per_zfs_send", True)
        self.threads: tuple[int, bool] = (1, False) if self.force_once else args.threads
        timeout_nanos = None if args.timeout is None else 1_000_000 * parse_duration_to_milliseconds(args.timeout)
        self.timeout_nanos: int | None = timeout_nanos
        self.no_estimate_send_size: bool = args.no_estimate_send_size
        self.remote_conf_cache_ttl_nanos: int = 1_000_000 * parse_duration_to_milliseconds(args.daemon_remote_conf_cache_ttl)
        self.terminal_columns: int = (
            getenv_int("terminal_columns", shutil.get_terminal_size(fallback=(120, 24)).columns)
            if self.isatty and self.pv_program != disable_prg and not self.quiet
            else 0
        )

        self.os_cpu_count: int | None = os.cpu_count()
        self.os_geteuid: int = os.geteuid()
        self.prog_version: str = __version__
        self.python_version: str = sys.version
        self.platform_version: str = platform.version()
        self.platform_platform: str = platform.platform()

        # mutable variables:
        snapshot_filters = args.snapshot_filters_var if hasattr(args, snapshot_filters_var) else [[]]
        self.snapshot_filters: list[list[SnapshotFilter]] = [optimize_snapshot_filters(f) for f in snapshot_filters]
        self.exclude_dataset_property: str | None = args.exclude_dataset_property
        self.exclude_dataset_regexes: RegexList = []  # deferred to validate_task() phase
        self.include_dataset_regexes: RegexList = []  # deferred to validate_task() phase
        self.tmp_exclude_dataset_regexes: RegexList = []  # deferred to validate_task() phase
        self.tmp_include_dataset_regexes: RegexList = []  # deferred to validate_task() phase
        self.abs_exclude_datasets: list[str] = []  # deferred to validate_task() phase
        self.abs_include_datasets: list[str] = []  # deferred to validate_task() phase

        self.curr_zfs_send_program_opts: list[str] = []
        self.zfs_recv_ox_names: set[str] = set()
        self.available_programs: dict[str, dict[str, str]] = {}
        self.zpool_features: dict[str, dict[str, str]] = {}
        self.connection_pools: dict[str, ConnectionPools] = {}

    def split_args(self, text: str, *items: str | Iterable[str], allow_all: bool = False) -> list[str]:
        """Splits option string on runs of one or more whitespace into an option list."""
        text = text.strip()
        opts = self.one_or_more_whitespace_regex.split(text) if text else []
        xappend(opts, items)
        if not allow_all:
            self.validate_quoting(opts)
        return opts

    def validate_arg(self, opt: str, allow_spaces: bool = False, allow_all: bool = False) -> str | None:
        """allow_all permits all characters, including whitespace and quotes. See squote() and dquote()."""
        if allow_all or opt is None:
            return opt
        if any(char.isspace() and (char != " " or not allow_spaces) for char in opt):
            die(f"Option must not contain a whitespace character{' other than space' if allow_spaces else ''}: {opt}")
        self.validate_quoting([opt])
        return opt

    def validate_arg_str(self, opt: str, allow_spaces: bool = False, allow_all: bool = False) -> str:
        if opt is None:
            die("Option must not be missing")
        self.validate_arg(opt, allow_spaces=allow_spaces, allow_all=allow_all)
        return opt

    @staticmethod
    def validate_quoting(opts: list[str]) -> None:
        for opt in opts:
            if "'" in opt or '"' in opt or "$" in opt or "`" in opt:
                die(f"Option must not contain a single quote or double quote or dollar or backtick character: {opt}")

    @staticmethod
    def fix_recv_opts(opts: list[str], preserve_properties: frozenset[str]) -> tuple[list[str], list[str]]:
        return fix_send_recv_opts(
            opts,
            exclude_long_opts={"--dryrun"},
            exclude_short_opts="n",
            include_arg_opts={"-o", "-x"},
            preserve_properties=preserve_properties,
        )

    @staticmethod
    def fix_send_opts(opts: list[str]) -> list[str]:
        return fix_send_recv_opts(
            opts,
            exclude_long_opts={"--dryrun"},
            exclude_short_opts="den",
            include_arg_opts={"-X", "--exclude", "--redact"},
            exclude_arg_opts=frozenset({"-i", "-I"}),
        )[0]

    def program_name(self, program: str) -> str:
        """For testing: helps simulate errors caused by external programs."""
        self.validate_arg_str(program)
        if not program:
            die(f"Program name must not be missing: {program}")
        for char in SHELL_CHARS + ":":
            if char in program:
                die(f"Program name must not contain a '{char}' character: {program}")
        if self.inject_params.get("inject_unavailable_" + program, False):
            return program + "-xxx"  # substitute a program that cannot be found on the PATH
        if self.inject_params.get("inject_failing_" + program, False):
            return "false"  # substitute a program that will error out with non-zero return code
        return program

    def unset_matching_env_vars(self, args: argparse.Namespace) -> None:
        exclude_envvar_regexes = compile_regexes(args.exclude_envvar_regex)
        include_envvar_regexes = compile_regexes(args.include_envvar_regex)
        for envvar_name in list(os.environ.keys()):
            if is_included(envvar_name, exclude_envvar_regexes, include_envvar_regexes):
                os.environ.pop(envvar_name, None)
                self.log.debug("Unsetting b/c envvar regex: %s", envvar_name)

    def lock_file_name(self) -> str:
        """Makes it such that a job that runs periodically declines to start if the same previous periodic
        job is still running without completion yet."""
        # fmt: off
        key = (tuple(self.root_dataset_pairs), self.args.recursive, self.args.exclude_dataset_property,
               tuple(self.args.include_dataset), tuple(self.args.exclude_dataset),
               tuple(self.args.include_dataset_regex), tuple(self.args.exclude_dataset_regex),
               tuple(tuple(f) for f in self.snapshot_filters), self.args.skip_replication, self.args.create_src_snapshots,
               self.args.create_src_snapshots_plan, self.args.create_src_snapshots_timeformat,
               self.create_src_snapshots_config.anchors,
               self.args.delete_dst_datasets, self.args.delete_dst_snapshots, self.args.delete_dst_snapshots_except,
               self.args.delete_empty_dst_datasets,
               self.args.compare_snapshot_lists, self.args.monitor_snapshots,
               self.args.log_file_infix,
               self.src.basis_ssh_host, self.dst.basis_ssh_host,
               self.src.basis_ssh_user, self.dst.basis_ssh_user)
        # fmt: on
        hash_code = hashlib.sha256(str(key).encode("utf-8")).hexdigest()
        return os.path.join(tempfile.gettempdir(), f"{prog_name}-lockfile-{hash_code}.lock")

    def dry(self, msg: str) -> str:
        return bzfs_main.utils.dry(msg, self.dry_run)

    def is_program_available(self, program: str, location: str) -> bool:
        return program in self.available_programs.get(location, {})


#############################################################################
class Remote:
    def __init__(self, loc: str, args: argparse.Namespace, p: Params) -> None:
        """Option values for either location=='src' or location=='dst'; reads from ArgumentParser via args."""
        # immutable variables:
        assert loc == "src" or loc == "dst"
        self.location: str = loc
        self.params: Params = p
        self.basis_ssh_user: str = getattr(args, f"ssh_{loc}_user")
        self.basis_ssh_host: str = getattr(args, f"ssh_{loc}_host")
        self.ssh_port: int = getattr(args, f"ssh_{loc}_port")
        self.ssh_config_file: str | None = p.validate_arg(getattr(args, f"ssh_{loc}_config_file"))
        if self.ssh_config_file:
            if "bzfs_ssh_config" not in os.path.basename(self.ssh_config_file):
                die(f"Basename of --ssh-{loc}-config-file must contain substring 'bzfs_ssh_config': {self.ssh_config_file}")
        # disable interactive password prompts and X11 forwarding and pseudo-terminal allocation:
        self.ssh_extra_opts: list[str] = ["-oBatchMode=yes", "-oServerAliveInterval=0", "-x", "-T"] + (
            ["-v"] if args.verbose >= 3 else []
        )
        self.max_concurrent_ssh_sessions_per_tcp_connection: int = args.max_concurrent_ssh_sessions_per_tcp_connection
        self.reuse_ssh_connection: bool = getenv_bool("reuse_ssh_connection", True)
        if self.reuse_ssh_connection:
            self.ssh_socket_dir: str = os.path.join(get_home_directory(), ".ssh", "bzfs")
            os.makedirs(os.path.dirname(self.ssh_socket_dir), exist_ok=True)
            os.makedirs(self.ssh_socket_dir, mode=DIR_PERMISSIONS, exist_ok=True)
            self.socket_prefix: str = "s"
            delete_stale_files(self.ssh_socket_dir, self.socket_prefix, ssh=True)
        self.sanitize1_regex = re.compile(r"[\s\\/@$]")  # replace whitespace, /, $, \, @ with a ~ tilde char
        self.sanitize2_regex = re.compile(rf"[^a-zA-Z0-9{re.escape('~.:_-')}]")  # Remove chars not in the allowed set

        # mutable variables:
        self.root_dataset: str = ""  # deferred until run_main()
        self.basis_root_dataset: str = ""  # deferred until run_main()
        self.pool: str = ""
        self.sudo: str = ""
        self.use_zfs_delegation: bool = False
        self.ssh_user: str = ""
        self.ssh_host: str = ""
        self.ssh_user_host: str = ""
        self.is_nonlocal: bool = False

    def local_ssh_command(self) -> list[str]:
        """Returns the ssh CLI command to run locally in order to talk to the remote host. This excludes the (trailing)
        command to run on the remote host, which will be appended later."""
        if self.ssh_user_host == "":
            return []  # dataset is on local host - don't use ssh

        # dataset is on remote host
        p = self.params
        if p.ssh_program == disable_prg:
            die("Cannot talk to remote host because ssh CLI is disabled.")
        ssh_cmd = [p.ssh_program] + self.ssh_extra_opts
        if self.ssh_config_file:
            ssh_cmd += ["-F", self.ssh_config_file]
        if self.ssh_port:
            ssh_cmd += ["-p", str(self.ssh_port)]
        if self.reuse_ssh_connection:
            # Performance: reuse ssh connection for low latency startup of frequent ssh invocations via the 'ssh -S' and
            # 'ssh -S -M -oControlPersist=60s' options. See https://en.wikibooks.org/wiki/OpenSSH/Cookbook/Multiplexing
            # Generate unique private Unix domain socket file name in user's home dir and pass it to 'ssh -S /path/to/socket'
            def sanitize(name: str) -> str:
                name = self.sanitize1_regex.sub("~", name)  # replace whitespace, /, $, \, @ with a ~ tilde char
                name = self.sanitize2_regex.sub("", name)  # Remove chars not in the allowed set
                return name

            unique = f"{os.getpid()}@{time.time_ns()}@{random.SystemRandom().randint(0, 999_999_999_999)}"
            socket_name = f"{self.socket_prefix}{unique}@{sanitize(self.ssh_host)[:45]}@{sanitize(self.ssh_user)}"
            socket_file = os.path.join(self.ssh_socket_dir, socket_name)[: max(100, len(self.ssh_socket_dir) + 10)]
            ssh_cmd += ["-S", socket_file]
        ssh_cmd += [self.ssh_user_host]
        return ssh_cmd

    def cache_key(self) -> tuple:
        return self.location, self.pool, self.ssh_user_host, self.ssh_port, self.ssh_config_file

    def __repr__(self) -> str:
        return str(self.__dict__)


#############################################################################
class CopyPropertiesConfig:
    def __init__(self, group: str, flag: str, args: argparse.Namespace, p: Params) -> None:
        """Option values for --zfs-recv-o* and --zfs-recv-x* option groups; reads from ArgumentParser via args."""
        # immutable variables:
        grup = group
        self.group: str = group
        self.flag: str = flag  # one of -o or -x
        sources: str = p.validate_arg_str(getattr(args, f"{grup}_sources"))
        self.sources: str = ",".join(sorted([s.strip() for s in sources.strip().split(",")]))  # canonicalize
        self.targets: str = p.validate_arg_str(getattr(args, f"{grup}_targets"))
        self.include_regexes: RegexList = compile_regexes(getattr(args, f"{grup}_include_regex"))
        self.exclude_regexes: RegexList = compile_regexes(getattr(args, f"{grup}_exclude_regex"))

    def __repr__(self) -> str:
        return str(self.__dict__)


#############################################################################
class SnapshotLabel(NamedTuple):
    """Contains the individual parts that are concatenated into a ZFS snapshot name."""

    prefix: str  # bzfs_
    infix: str  # us-west-1_
    timestamp: str  # 2024-11-06_08:30:05
    suffix: str  # _hourly

    def __str__(self) -> str:  # bzfs_us-west-1_2024-11-06_08:30:05_hourly
        return f"{self.prefix}{self.infix}{self.timestamp}{self.suffix}"

    def validate_label(self, input_text: str) -> None:
        name = str(self)
        validate_dataset_name(name, input_text)
        if "/" in name:
            die(f"Invalid ZFS snapshot name: '{name}' for: '{input_text}*'")
        for key, value in {"prefix": self.prefix, "infix": self.infix, "suffix": self.suffix}.items():
            if key == "prefix":
                if not value.endswith("_"):
                    die(f"Invalid {input_text}{key}: Must end with an underscore character: '{value}'")
                if value.count("_") > 1:
                    die(f"Invalid {input_text}{key}: Must not contain multiple underscore characters: '{value}'")
            elif key == "infix":
                if value:
                    if not value.endswith("_"):
                        die(f"Invalid {input_text}{key}: Must end with an underscore character: '{value}'")
                    if value.count("_") > 1:
                        die(f"Invalid {input_text}{key}: Must not contain multiple underscore characters: '{value}'")
            elif value:
                if not value.startswith("_"):
                    die(f"Invalid {input_text}{key}: Must start with an underscore character: '{value}'")
                if value.count("_") > 1:
                    die(f"Invalid {input_text}{key}: Must not contain multiple underscore characters: '{value}'")


#############################################################################
class SnapshotPeriods:  # thread-safe
    def __init__(self) -> None:
        # immutable variables:
        self.suffix_milliseconds: Final = {
            "yearly": 365 * 86400 * 1000,
            "monthly": round(30.5 * 86400 * 1000),
            "weekly": 7 * 86400 * 1000,
            "daily": 86400 * 1000,
            "hourly": 60 * 60 * 1000,
            "minutely": 60 * 1000,
            "secondly": 1000,
            "millisecondly": 1,
        }
        self.period_labels: Final = {
            "yearly": "years",
            "monthly": "months",
            "weekly": "weeks",
            "daily": "days",
            "hourly": "hours",
            "minutely": "minutes",
            "secondly": "seconds",
            "millisecondly": "milliseconds",
        }
        self._suffix_regex0: Final = re.compile(rf"([1-9][0-9]*)?({'|'.join(self.suffix_milliseconds.keys())})")
        self._suffix_regex1: Final = re.compile("_" + self._suffix_regex0.pattern)

    def suffix_to_duration0(self, suffix: str) -> tuple[int, str]:
        return self._suffix_to_duration(suffix, self._suffix_regex0)

    def suffix_to_duration1(self, suffix: str) -> tuple[int, str]:
        return self._suffix_to_duration(suffix, self._suffix_regex1)

    @staticmethod
    def _suffix_to_duration(suffix: str, regex: re.Pattern) -> tuple[int, str]:
        """Ex: Converts "2 hourly" to (2, "hourly") and "hourly" to (1, "hourly"), i.e. perform some action every N hours."""
        if match := regex.fullmatch(suffix):
            duration_amount = int(match.group(1)) if match.group(1) else 1
            assert duration_amount > 0
            duration_unit = match.group(2)
            return duration_amount, duration_unit
        else:
            return 0, ""

    def label_milliseconds(self, snapshot: str) -> int:
        i = snapshot.rfind("_")
        snapshot = "" if i < 0 else snapshot[i + 1 :]
        duration_amount, duration_unit = self._suffix_to_duration(snapshot, self._suffix_regex0)
        return duration_amount * self.suffix_milliseconds.get(duration_unit, 0)


#############################################################################
class CreateSrcSnapshotConfig:
    def __init__(self, args: argparse.Namespace, p: Params) -> None:
        """Option values for --create-src-snapshots*; reads from ArgumentParser via args."""
        # immutable variables:
        self.skip_create_src_snapshots: bool = not args.create_src_snapshots
        self.create_src_snapshots_even_if_not_due: bool = args.create_src_snapshots_even_if_not_due
        tz_spec: str | None = args.create_src_snapshots_timezone if args.create_src_snapshots_timezone else None
        self.tz: tzinfo | None = get_timezone(tz_spec)
        self.current_datetime: datetime = current_datetime(tz_spec)
        self.timeformat: str = args.create_src_snapshots_timeformat
        self.anchors: PeriodAnchors = PeriodAnchors.parse(args)

        # Compute the schedule for upcoming periodic time events (suffix_durations). This event schedule is also used in
        # daemon mode via sleep_until_next_daemon_iteration()
        suffixes: list[str] = []
        labels = []
        create_src_snapshots_plan = args.create_src_snapshots_plan or str({"bzfs": {"onsite": {"adhoc": 1}}})
        for org, target_periods in ast.literal_eval(create_src_snapshots_plan).items():
            for target, periods in target_periods.items():
                for period_unit, period_amount in periods.items():  # e.g. period_unit can be "10minutely" or "minutely"
                    if not isinstance(period_amount, int) or period_amount < 0:
                        die(f"--create-src-snapshots-plan: Period amount must be a non-negative integer: {period_amount}")
                    if period_amount > 0:
                        suffix = nsuffix(period_unit)
                        suffixes.append(suffix)
                        labels.append(SnapshotLabel(prefix=nprefix(org), infix=ninfix(target), timestamp="", suffix=suffix))
        xperiods: SnapshotPeriods = p.xperiods
        if self.skip_create_src_snapshots:
            duration_amount, duration_unit = p.xperiods.suffix_to_duration0(p.daemon_frequency)
            if duration_amount <= 0 or not duration_unit:
                die(f"Invalid --daemon-frequency: {p.daemon_frequency}")
            suffixes = [nsuffix(p.daemon_frequency)]
            labels = []
        suffix_durations = {suffix: xperiods.suffix_to_duration1(suffix) for suffix in suffixes}

        def suffix_key(suffix: str) -> tuple[int, str]:
            duration_amount, duration_unit = suffix_durations[suffix]
            duration_milliseconds = duration_amount * xperiods.suffix_milliseconds.get(duration_unit, 0)
            if suffix.endswith(("hourly", "minutely", "secondly")):
                if duration_milliseconds != 0 and 86400 * 1000 % duration_milliseconds != 0:
                    die(
                        "Invalid --create-src-snapshots-plan: Period duration should be a divisor of 86400 seconds "
                        f"without remainder so that snapshots will be created at the same time of day every day: {suffix}"
                    )
            if suffix.endswith("monthly"):
                if duration_amount != 0 and 12 % duration_amount != 0:
                    die(
                        "Invalid --create-src-snapshots-plan: Period duration should be a divisor of 12 months "
                        f"without remainder so that snapshots will be created at the same time every year: {suffix}"
                    )
            return duration_milliseconds, suffix

        suffixes = sorted(suffixes, key=suffix_key, reverse=True)  # take snapshots for dailies before hourlies, and so on
        self.suffix_durations: dict[str, tuple[int, str]] = {suffix: suffix_durations[suffix] for suffix in suffixes}  # sort
        suffix_indexes = {suffix: k for k, suffix in enumerate(suffixes)}
        labels.sort(key=lambda label: (suffix_indexes[label.suffix], label))  # take snapshots for dailies before hourlies
        self._snapshot_labels: list[SnapshotLabel] = labels
        for label in self.snapshot_labels():
            label.validate_label("--create-src-snapshots-plan ")

    def snapshot_labels(self) -> list[SnapshotLabel]:
        """Returns the snapshot name patterns for which snapshots shall be created."""
        timeformat = self.timeformat
        is_millis = timeformat.endswith("%F")  # non-standard hack to append milliseconds
        if is_millis:
            timeformat = timeformat[0:-1] + "f"  # replace %F with %f (append microseconds)
        timestamp: str = self.current_datetime.strftime(timeformat)
        if is_millis:
            timestamp = timestamp[0 : -len("000")]  # replace microseconds with milliseconds
        timestamp = timestamp.replace("+", "z")  # zfs CLI does not accept the '+' character in snapshot names
        return [SnapshotLabel(label.prefix, label.infix, timestamp, label.suffix) for label in self._snapshot_labels]

    def __repr__(self) -> str:
        return str(self.__dict__)


#############################################################################
@dataclass(frozen=True)
class AlertConfig:
    kind: Literal["Latest", "Oldest"]
    warning_millis: int
    critical_millis: int


#############################################################################
@dataclass(frozen=True)
class MonitorSnapshotAlert:
    label: SnapshotLabel
    latest: AlertConfig | None
    oldest: AlertConfig | None


#############################################################################
class MonitorSnapshotsConfig:
    def __init__(self, args: argparse.Namespace, p: Params) -> None:
        """Option values for --monitor-snapshots*; reads from ArgumentParser via args."""
        # immutable variables:
        self.monitor_snapshots: dict = ast.literal_eval(args.monitor_snapshots)
        self.dont_warn: bool = args.monitor_snapshots_dont_warn
        self.dont_crit: bool = args.monitor_snapshots_dont_crit
        self.no_latest_check: bool = args.monitor_snapshots_no_latest_check
        self.no_oldest_check: bool = args.monitor_snapshots_no_oldest_check
        alerts = []
        xperiods: SnapshotPeriods = p.xperiods
        for org, target_periods in self.monitor_snapshots.items():
            prefix = nprefix(org)
            for target, periods in target_periods.items():
                for period_unit, alert_dicts in periods.items():  # e.g. period_unit can be "10minutely" or "minutely"
                    label = SnapshotLabel(prefix=prefix, infix=ninfix(target), timestamp="", suffix=nsuffix(period_unit))
                    alert_latest, alert_oldest = None, None
                    for alert_type, alert_dict in alert_dicts.items():
                        m = "--monitor-snapshots: "
                        if alert_type not in ["latest", "oldest"]:
                            die(f"{m}'{alert_type}' must be 'latest' or 'oldest' within {args.monitor_snapshots}")
                        warning_millis: int = 0
                        critical_millis: int = 0
                        cycles: int = 1
                        for kind, value in alert_dict.items():
                            context = args.monitor_snapshots
                            if kind == "warning":
                                warning_millis = max(0, parse_duration_to_milliseconds(str(value), context=context))
                            elif kind == "critical":
                                critical_millis = max(0, parse_duration_to_milliseconds(str(value), context=context))
                            elif kind == "cycles":
                                cycles = max(0, int(value))
                            else:
                                die(f"{m}'{kind}' must be 'warning', 'critical' or 'cycles' within {context}")
                        if warning_millis > 0 or critical_millis > 0:
                            duration_amount, duration_unit = xperiods.suffix_to_duration1(label.suffix)
                            duration_milliseconds = duration_amount * xperiods.suffix_milliseconds.get(duration_unit, 0)
                            warning_millis += 0 if warning_millis <= 0 else cycles * duration_milliseconds
                            critical_millis += 0 if critical_millis <= 0 else cycles * duration_milliseconds
                            warning_millis = unixtime_infinity_secs if warning_millis <= 0 else warning_millis
                            critical_millis = unixtime_infinity_secs if critical_millis <= 0 else critical_millis
                            capitalized_alert_type = cast(Literal["Latest", "Oldest"], sys.intern(alert_type.capitalize()))
                            alert_config = AlertConfig(capitalized_alert_type, warning_millis, critical_millis)
                            if alert_type == "latest":
                                if not self.no_latest_check:
                                    alert_latest = alert_config
                            else:
                                assert alert_type == "oldest"
                                if not self.no_oldest_check:
                                    alert_oldest = alert_config
                    if alert_latest is not None or alert_oldest is not None:
                        alerts.append(MonitorSnapshotAlert(label, alert_latest, alert_oldest))

        def alert_sort_key(alert: MonitorSnapshotAlert) -> tuple[int, SnapshotLabel]:
            duration_amount, duration_unit = xperiods.suffix_to_duration1(alert.label.suffix)
            duration_milliseconds = duration_amount * xperiods.suffix_milliseconds.get(duration_unit, 0)
            return duration_milliseconds, alert.label

        alerts.sort(key=alert_sort_key, reverse=True)  # check snapshots for dailies before hourlies, and so on
        self.alerts: list[MonitorSnapshotAlert] = alerts
        self.enable_monitor_snapshots: bool = len(alerts) > 0

    def __repr__(self) -> str:
        return str(self.__dict__)


#############################################################################
def main() -> None:
    """API for command line clients."""
    try:
        run_main(argument_parser().parse_args(), sys.argv)
    except subprocess.CalledProcessError as e:
        sys.exit(e.returncode)


def run_main(args: argparse.Namespace, sys_argv: list[str] | None = None, log: Logger | None = None) -> None:
    """API for Python clients; visible for testing; may become a public API eventually."""
    Job().run_main(args, sys_argv, log)


#############################################################################
class Job:
    def __init__(self) -> None:
        self.params: Params
        self.all_dst_dataset_exists: dict[str, dict[str, bool]] = defaultdict(lambda: defaultdict(bool))
        self.dst_dataset_exists: SynchronizedDict[str, bool] = SynchronizedDict({})
        self.src_properties: dict[str, dict[str, str | int]] = {}
        self.dst_properties: dict[str, dict[str, str | int]] = {}
        self.all_exceptions: list[str] = []
        self.all_exceptions_count: int = 0
        self.max_exceptions_to_summarize: int = 10000
        self.first_exception: BaseException | None = None
        self.remote_conf_cache: dict[tuple, RemoteConfCacheItem] = {}
        self.dedicated_tcp_connection_per_zfs_send: bool = True
        self.max_datasets_per_minibatch_on_list_snaps: dict[str, int] = {}
        self.max_workers: dict[str, int] = {}
        self.stats_lock: threading.Lock = threading.Lock()
        self.num_cache_hits: int = 0
        self.num_cache_misses: int = 0
        self.num_snapshots_found: int = 0
        self.num_snapshots_replicated: int = 0
        self.control_persist_secs: int = 90
        self.control_persist_margin_secs: int = 2
        self.progress_reporter: ProgressReporter = cast(ProgressReporter, None)
        self.is_first_replication_task: SynchronizedBool = SynchronizedBool(True)
        self.replication_start_time_nanos: int = time.monotonic_ns()
        self.timeout_nanos: int | None = None

        self.is_test_mode: bool = False  # for testing only
        self.creation_prefix: str = ""  # for testing only
        self.isatty: bool | None = None  # for testing only
        self.use_select: bool = False  # for testing only
        self.progress_update_intervals: tuple[float, float] | None = None  # for testing only
        self.error_injection_triggers: dict[str, Counter[str]] = {}  # for testing only
        self.delete_injection_triggers: dict[str, Counter[str]] = {}  # for testing only
        self.param_injection_triggers: dict[str, dict[str, bool]] = {}  # for testing only
        self.inject_params: dict[str, bool] = {}  # for testing only
        self.injection_lock: threading.Lock = threading.Lock()  # for testing only
        self.max_command_line_bytes: int | None = None  # for testing only

    def shutdown(self) -> None:
        """Exit any multiplexed ssh sessions that may be leftover."""
        cache_items = self.remote_conf_cache.values()
        for i, cache_item in enumerate(cache_items):
            cache_item.connection_pools.shutdown(f"{i + 1}/{len(cache_items)}")

    def terminate(self, old_term_handler: Any, except_current_process: bool = False) -> None:
        def post_shutdown() -> None:
            signal.signal(signal.SIGTERM, old_term_handler)  # restore original signal handler
            terminate_process_subtree(except_current_process=except_current_process)

        with xfinally(post_shutdown):
            self.shutdown()

    def run_main(self, args: argparse.Namespace, sys_argv: list[str] | None = None, log: Logger | None = None) -> None:
        assert isinstance(self.error_injection_triggers, dict)
        assert isinstance(self.delete_injection_triggers, dict)
        assert isinstance(self.inject_params, dict)
        with xfinally(reset_logger):  # runs reset_logger() on exit, without masking exception raised in body of `with` block
            try:
                log_params = LogParams(args)
                log = get_logger(log_params, args, log)
                log.info("%s", f"Log file is: {log_params.log_file}")
            except BaseException as e:
                get_simple_logger(prog_name).error("Log init: %s", e, exc_info=False if isinstance(e, SystemExit) else True)
                raise

            aux_args: list[str] = []
            if getattr(args, "include_snapshot_plan", None):
                aux_args += args.include_snapshot_plan
            if getattr(args, "delete_dst_snapshots_except_plan", None):
                aux_args += args.delete_dst_snapshots_except_plan
            if len(aux_args) > 0:
                log.info("Auxiliary CLI arguments: %s", " ".join(aux_args))
                args = argument_parser().parse_args(xappend(aux_args, "--", args.root_dataset_pairs), namespace=args)

            def log_error_on_exit(error: Any, status_code: Any, exc_info: bool = False) -> None:
                log.error("%s%s", f"Exiting {prog_name} with status code {status_code}. Cause: ", error, exc_info=exc_info)

            try:
                log.info("CLI arguments: %s %s", " ".join(sys_argv or []), f"[euid: {os.geteuid()}]")
                if self.is_test_mode:
                    log.log(log_trace, "Parsed CLI arguments: %s", args)
                self.params = p = Params(args, sys_argv, log_params, log, self.inject_params)
                log_params.params = p
                with open_nofollow(log_params.log_file, "a", encoding="utf-8", perm=FILE_PERMISSIONS) as log_file_fd:
                    with contextlib.redirect_stderr(cast(IO, Tee(log_file_fd, sys.stderr))):  # stderr to logfile+stderr
                        lock_file = p.lock_file_name()
                        lock_fd = os.open(lock_file, os.O_WRONLY | os.O_TRUNC | os.O_CREAT | os.O_NOFOLLOW, FILE_PERMISSIONS)
                        with xfinally(lambda: os.close(lock_fd)):
                            try:
                                # Acquire an exclusive lock; will raise an error if lock is already held by another process.
                                # The (advisory) lock is auto-released when the process terminates or the fd is closed.
                                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # LOCK_NB ... non-blocking
                            except BlockingIOError:
                                msg = "Exiting as same previous periodic job is still running without completion yet per "
                                die(msg + lock_file, still_running_status)
                            with xfinally(lambda: Path(lock_file).unlink(missing_ok=True)):  # don't accumulate stale files
                                # On CTRL-C and SIGTERM, send signal to descendant processes to also terminate descendants
                                old_term_handler = signal.getsignal(signal.SIGTERM)
                                signal.signal(signal.SIGTERM, lambda sig, f: self.terminate(old_term_handler))
                                old_int_handler = signal.signal(signal.SIGINT, lambda s, f: self.terminate(old_term_handler))
                                try:
                                    self.run_tasks()
                                except BaseException:
                                    self.terminate(old_term_handler, except_current_process=True)
                                    raise
                                finally:
                                    signal.signal(signal.SIGTERM, old_term_handler)  # restore original signal handler
                                    signal.signal(signal.SIGINT, old_int_handler)  # restore original signal handler
                                for _ in range(2 if self.max_command_line_bytes else 1):
                                    self.shutdown()
                                print("", end="", file=sys.stderr)
                                sys.stderr.flush()
            except subprocess.CalledProcessError as e:
                log_error_on_exit(e, e.returncode)
                raise
            except SystemExit as e:
                log_error_on_exit(e, e.code)
                raise
            except (subprocess.TimeoutExpired, UnicodeDecodeError) as e:
                log_error_on_exit(e, die_status)
                raise SystemExit(die_status) from e
            except re.error as e:
                log_error_on_exit(f"{e} within regex {e.pattern!r}", die_status)
                raise SystemExit(die_status) from e
            except BaseException as e:
                log_error_on_exit(e, die_status, exc_info=True)
                raise SystemExit(die_status) from e
            finally:
                log.info("%s", f"Log file was: {log_params.log_file}")
            log.info("Success. Goodbye!")
            sys.stderr.flush()

    def run_tasks(self) -> None:
        p, log = self.params, self.params.log
        self.all_exceptions = []
        self.all_exceptions_count = 0
        self.first_exception = None
        self.remote_conf_cache = {}
        self.isatty = self.isatty if self.isatty is not None else p.isatty
        self.validate_once()
        self.replication_start_time_nanos = time.monotonic_ns()
        self.progress_reporter = ProgressReporter(log, p.pv_program_opts, self.use_select, self.progress_update_intervals)
        with xfinally(lambda: self.progress_reporter.stop()):
            daemon_stoptime_nanos = time.monotonic_ns() + p.daemon_lifetime_nanos
            while True:  # loop for daemon mode
                self.timeout_nanos = None if p.timeout_nanos is None else time.monotonic_ns() + p.timeout_nanos
                self.all_dst_dataset_exists.clear()
                self.progress_reporter.reset()
                src, dst = p.src, p.dst
                for src_root_dataset, dst_root_dataset in p.root_dataset_pairs:
                    src.root_dataset = src.basis_root_dataset = src_root_dataset
                    dst.root_dataset = dst.basis_root_dataset = dst_root_dataset
                    p.curr_zfs_send_program_opts = p.zfs_send_program_opts.copy()
                    if p.daemon_lifetime_nanos > 0:
                        self.timeout_nanos = None if p.timeout_nanos is None else time.monotonic_ns() + p.timeout_nanos
                    recurs_sep = " " if p.recursive_flag else ""
                    task_description = f"{src.basis_root_dataset} {p.recursive_flag}{recurs_sep}--> {dst.basis_root_dataset}"
                    if len(p.root_dataset_pairs) > 1:
                        log.info("Starting task: %s", task_description + " ...")
                    try:
                        try:
                            maybe_inject_error(self, cmd=[], error_trigger="retryable_run_tasks")
                            timeout(self)
                            self.validate_task()
                            self.run_task()
                        except RetryableError as retryable_error:
                            assert retryable_error.__cause__ is not None
                            raise retryable_error.__cause__ from None
                    except (CalledProcessError, subprocess.TimeoutExpired, SystemExit, UnicodeDecodeError) as e:
                        if p.skip_on_error == "fail" or (
                            isinstance(e, subprocess.TimeoutExpired) and p.daemon_lifetime_nanos == 0
                        ):
                            raise
                        log.error("%s", e)
                        self.append_exception(e, "task", task_description)
                if not self.sleep_until_next_daemon_iteration(daemon_stoptime_nanos):
                    break
        if not p.skip_replication:
            self.print_replication_stats(self.replication_start_time_nanos)
        error_count = self.all_exceptions_count
        if error_count > 0 and p.daemon_lifetime_nanos == 0:
            msgs = "\n".join([f"{i + 1}/{error_count}: {e}" for i, e in enumerate(self.all_exceptions)])
            log.error("%s", f"Tolerated {error_count} errors. Error Summary: \n{msgs}")
            assert self.first_exception is not None
            raise self.first_exception

    def append_exception(self, e: BaseException, task_name: str, task_description: str) -> None:
        self.first_exception = self.first_exception or e
        if len(self.all_exceptions) < self.max_exceptions_to_summarize:  # cap max memory consumption
            self.all_exceptions.append(str(e))
        self.all_exceptions_count += 1
        self.params.log.error(f"#{self.all_exceptions_count}: Done with %s: %s", task_name, task_description)

    def sleep_until_next_daemon_iteration(self, daemon_stoptime_nanos: int) -> bool:
        sleep_nanos = daemon_stoptime_nanos - time.monotonic_ns()
        if sleep_nanos <= 0:
            return False
        self.progress_reporter.pause()
        p, log = self.params, self.params.log
        config = p.create_src_snapshots_config
        curr_datetime = config.current_datetime + timedelta(microseconds=1)
        next_snapshotting_event_dt = min(
            (
                round_datetime_up_to_duration_multiple(curr_datetime, duration_amount, duration_unit, config.anchors)
                for duration_amount, duration_unit in config.suffix_durations.values()
            ),
            default=curr_datetime + timedelta(days=10 * 365),  # infinity
        )
        offset: timedelta = next_snapshotting_event_dt - datetime.now(config.tz)
        offset_nanos = (offset.days * 86400 + offset.seconds) * 1_000_000_000 + offset.microseconds * 1_000
        sleep_nanos = min(sleep_nanos, max(0, offset_nanos))
        log.info("Daemon sleeping for: %s%s", human_readable_duration(sleep_nanos), f" ... [Log {p.log_params.log_file}]")
        time.sleep(sleep_nanos / 1_000_000_000)
        config.current_datetime = datetime.now(config.tz)
        return daemon_stoptime_nanos - time.monotonic_ns() > 0

    def print_replication_stats(self, start_time_nanos: int) -> None:
        p, log = self.params, self.params.log
        elapsed_nanos = time.monotonic_ns() - start_time_nanos
        msg = p.dry(f"Replicated {self.num_snapshots_replicated} snapshots in {human_readable_duration(elapsed_nanos)}.")
        if p.is_program_available("pv", "local"):
            sent_bytes = count_num_bytes_transferred_by_zfs_send(p.log_params.pv_log_file)
            sent_bytes_per_sec = round(1_000_000_000 * sent_bytes / elapsed_nanos)
            msg += f" zfs sent {human_readable_bytes(sent_bytes)} [{human_readable_bytes(sent_bytes_per_sec)}/s]."
        log.info("%s", msg.ljust(p.terminal_columns - len("2024-01-01 23:58:45 [I] ")))

    def validate_once(self) -> None:
        p = self.params
        p.zfs_recv_ox_names = self.recv_option_property_names(p.zfs_recv_program_opts)
        for snapshot_filter in p.snapshot_filters:
            for _filter in snapshot_filter:
                if _filter.name == snapshot_regex_filter_name:
                    exclude_snapshot_regexes = compile_regexes(_filter.options[0])
                    include_snapshot_regexes = compile_regexes(_filter.options[1] or [".*"])
                    _filter.options = (exclude_snapshot_regexes, include_snapshot_regexes)

        exclude_regexes = [exclude_dataset_regexes_default]
        if len(p.args.exclude_dataset_regex) > 0:  # some patterns don't exclude anything
            exclude_regexes = [regex for regex in p.args.exclude_dataset_regex if regex != "" and regex != "!.*"]
        include_regexes = p.args.include_dataset_regex

        # relative datasets need not be compiled more than once as they don't change between tasks
        def separate_abs_vs_rel_datasets(datasets: list[str]) -> tuple[list[str], list[str]]:
            abs_datasets: list[str] = []
            rel_datasets: list[str] = []
            for dataset in datasets:
                (abs_datasets if dataset.startswith("/") else rel_datasets).append(dataset)
            return abs_datasets, rel_datasets

        p.abs_exclude_datasets, rel_exclude_datasets = separate_abs_vs_rel_datasets(p.args.exclude_dataset)
        p.abs_include_datasets, rel_include_datasets = separate_abs_vs_rel_datasets(p.args.include_dataset)
        suffix = descendants_re_suffix
        p.tmp_exclude_dataset_regexes, p.tmp_include_dataset_regexes = (
            compile_regexes(exclude_regexes + dataset_regexes(p.src, p.dst, rel_exclude_datasets), suffix=suffix),
            compile_regexes(include_regexes + dataset_regexes(p.src, p.dst, rel_include_datasets), suffix=suffix),
        )

        if p.pv_program != disable_prg:
            pv_program_opts_set = set(p.pv_program_opts)
            if pv_program_opts_set.isdisjoint({"--bytes", "-b", "--bits", "-8"}):
                die("--pv-program-opts must contain one of --bytes or --bits for progress metrics to function.")
            if self.isatty and not p.quiet:
                for opts in [["--eta", "-e"], ["--fineta", "-I"], ["--average-rate", "-a"]]:
                    if pv_program_opts_set.isdisjoint(opts):
                        die(f"--pv-program-opts must contain one of {', '.join(opts)} for progress report line to function.")

        src, dst = p.src, p.dst
        for remote in [src, dst]:
            r, loc = remote, remote.location
            validate_user_name(r.basis_ssh_user, f"--ssh-{loc}-user")
            validate_host_name(r.basis_ssh_host, f"--ssh-{loc}-host")
            validate_port(r.ssh_port, f"--ssh-{loc}-port ")

    def validate_task(self) -> None:
        p, log = self.params, self.params.log
        src, dst = p.src, p.dst
        for remote in [src, dst]:
            r = remote
            r.ssh_user, r.ssh_host, r.ssh_user_host, r.pool, r.root_dataset = parse_dataset_locator(
                r.basis_root_dataset, user=r.basis_ssh_user, host=r.basis_ssh_host, port=r.ssh_port
            )
            r.sudo, r.use_zfs_delegation = self.sudo_cmd(r.ssh_user_host, r.ssh_user)
            local_addrs = ("",) if self.is_test_mode else ("", "127.0.0.1", "::1")  # ::1 is IPv6 version of loopback address
            remote.is_nonlocal = r.ssh_host not in local_addrs
        self.dst_dataset_exists = SynchronizedDict(self.all_dst_dataset_exists[dst.ssh_user_host])

        if src.ssh_host == dst.ssh_host:
            msg = f"src: {src.basis_root_dataset}, dst: {dst.basis_root_dataset}"
            if src.root_dataset == dst.root_dataset:
                die(f"Source and destination dataset must not be the same! {msg}")
            if p.recursive and (
                is_descendant(src.root_dataset, of_root_dataset=dst.root_dataset)
                or is_descendant(dst.root_dataset, of_root_dataset=src.root_dataset)
            ):
                die(f"Source and destination dataset trees must not overlap! {msg}")

        suffx = descendants_re_suffix  # also match descendants of a matching dataset
        p.exclude_dataset_regexes, p.include_dataset_regexes = (
            p.tmp_exclude_dataset_regexes + compile_regexes(dataset_regexes(src, dst, p.abs_exclude_datasets), suffix=suffx),
            p.tmp_include_dataset_regexes + compile_regexes(dataset_regexes(src, dst, p.abs_include_datasets), suffix=suffx),
        )
        if len(p.include_dataset_regexes) == 0:
            p.include_dataset_regexes = [(re.compile(r".*"), False)]

        detect_available_programs(self)

        zfs_send_program_opts = p.curr_zfs_send_program_opts
        if is_zpool_feature_enabled_or_active(p, dst, "feature@large_blocks"):
            append_if_absent(zfs_send_program_opts, "--large-block")  # solaris-11.4 does not have this feature
        if is_solaris_zfs(p, dst):
            p.dry_run_destroy = ""  # solaris-11.4 knows no 'zfs destroy -n' flag
            p.verbose_destroy = ""  # solaris-11.4 knows no 'zfs destroy -v' flag
        if is_solaris_zfs(p, src):  # solaris-11.4 only knows -w compress
            zfs_send_program_opts = ["-p" if opt == "--props" else opt for opt in zfs_send_program_opts]
            zfs_send_program_opts = fix_solaris_raw_mode(zfs_send_program_opts)
        p.curr_zfs_send_program_opts = zfs_send_program_opts

        self.max_workers = {}
        self.max_datasets_per_minibatch_on_list_snaps = {}
        for r in [src, dst]:
            cpus = int(p.available_programs[r.location].get("getconf_cpu_count", 8))
            threads, is_percent = p.threads
            cpus = max(1, round(cpus * threads / 100.0) if is_percent else round(threads))
            self.max_workers[r.location] = cpus
            bs = max(1, p.max_datasets_per_batch_on_list_snaps)  # 1024 by default
            max_datasets_per_minibatch = p.max_datasets_per_minibatch_on_list_snaps
            if max_datasets_per_minibatch <= 0:
                max_datasets_per_minibatch = max(1, bs // cpus)
            max_datasets_per_minibatch = min(bs, max_datasets_per_minibatch)
            self.max_datasets_per_minibatch_on_list_snaps[r.location] = max_datasets_per_minibatch
            log.log(
                log_trace,
                "%s",
                f"max_datasets_per_batch_on_list_snaps: {p.max_datasets_per_batch_on_list_snaps}, "
                f"max_datasets_per_minibatch_on_list_snaps: {max_datasets_per_minibatch}, "
                f"max_workers: {self.max_workers[r.location]}, "
                f"location: {r.location}",
            )
        if self.is_test_mode:
            log.log(log_trace, "Validated Param values: %s", pretty_print_formatter(self.params))

    def sudo_cmd(self, ssh_user_host: str, ssh_user: str) -> tuple[str, bool]:
        p = self.params
        assert isinstance(ssh_user_host, str)
        assert isinstance(ssh_user, str)
        assert isinstance(p.sudo_program, str)
        assert isinstance(p.enable_privilege_elevation, bool)

        is_root = True
        if ssh_user_host != "":
            if ssh_user == "":
                if os.geteuid() != 0:
                    is_root = False
            elif ssh_user != "root":
                is_root = False
        elif os.geteuid() != 0:
            is_root = False

        if is_root:
            sudo = ""  # using sudo in an attempt to make ZFS operations work even if we are not root user?
            use_zfs_delegation = False  # or instead using 'zfs allow' delegation?
            return sudo, use_zfs_delegation
        elif p.enable_privilege_elevation:
            if p.sudo_program == disable_prg:
                die(f"sudo CLI is not available on host: {ssh_user_host or 'localhost'}")
            # The '-n' option makes 'sudo' safer and more fail-fast. It avoids having sudo prompt the user for input of any
            # kind. If a password is required for the sudo command to run, sudo will display an error message and exit.
            return p.sudo_program + " -n", False
        else:
            return "", True

    def run_task(self) -> None:
        def filter_src_datasets() -> list[str]:  # apply --{include|exclude}-dataset policy
            return filter_datasets(self, src, basis_src_datasets) if src_datasets is None else src_datasets

        p, log = self.params, self.params.log
        src, dst = p.src, p.dst
        max_workers = min(self.max_workers[src.location], self.max_workers[dst.location])
        recursive_sep = " " if p.recursive_flag else ""
        task_description = f"{src.basis_root_dataset} {p.recursive_flag}{recursive_sep}--> {dst.basis_root_dataset} ..."
        failed = False
        src_datasets = None
        basis_src_datasets = []
        self.src_properties = {}
        self.dst_properties = {}
        if not is_dummy(src):  # find src dataset or all datasets in src dataset tree (with --recursive)
            is_caching = is_caching_snapshots(p, src)
            props = "volblocksize,recordsize,name"
            props = "snapshots_changed," + props if is_caching else props
            cmd = p.split_args(
                f"{p.zfs_program} list -t filesystem,volume -s name -Hp -o {props} {p.recursive_flag}", src.root_dataset
            )
            for line in (self.try_ssh_command(src, log_debug, cmd=cmd) or "").splitlines():
                cols = line.split("\t")
                snapshots_changed, volblocksize, recordsize, src_dataset = cols if is_caching else ["-"] + cols
                self.src_properties[src_dataset] = {
                    "recordsize": int(recordsize) if recordsize != "-" else -int(volblocksize),
                    SNAPSHOTS_CHANGED: int(snapshots_changed) if snapshots_changed and snapshots_changed != "-" else 0,
                }
                basis_src_datasets.append(src_dataset)
            assert not self.is_test_mode or basis_src_datasets == sorted(basis_src_datasets), "List is not sorted"

        # Optionally, atomically create a new snapshot of the src datasets selected by --{include|exclude}-dataset* policy.
        # The implementation attempts to fit as many datasets as possible into a single (atomic) 'zfs snapshot' command line,
        # using lexicographical sort order, and using 'zfs snapshot -r' to the extent that this is compatible with the
        # --{include|exclude}-dataset* pruning policy. The snapshots of all datasets that fit within the same single
        # 'zfs snapshot' CLI invocation will be taken within the same ZFS transaction group, and correspondingly have
        # identical 'createtxg' ZFS property (but not necessarily identical 'creation' ZFS time property as ZFS actually
        # provides no such guarantee), and thus be consistent. Dataset names that can't fit into a single command line are
        # spread over multiple command line invocations, respecting the limits that the operating system places on the
        # maximum length of a single command line, per `getconf ARG_MAX`.
        if not p.create_src_snapshots_config.skip_create_src_snapshots:
            log.info(p.dry("--create-src-snapshots: %s"), f"{src.basis_root_dataset} {p.recursive_flag}{recursive_sep}...")
            if len(basis_src_datasets) == 0:
                die(f"Source dataset does not exist: {src.basis_root_dataset}")
            src_datasets = filter_src_datasets()  # apply include/exclude policy
            datasets_to_snapshot: dict[SnapshotLabel, list[str]] = self.find_datasets_to_snapshot(src_datasets)
            datasets_to_snapshot = {label: datasets for label, datasets in datasets_to_snapshot.items() if len(datasets) > 0}
            basis_datasets_to_snapshot = datasets_to_snapshot.copy()  # shallow copy
            commands = {}
            for label, datasets in datasets_to_snapshot.items():
                cmd = p.split_args(f"{src.sudo} {p.zfs_program} snapshot")
                if p.recursive:
                    # Run 'zfs snapshot -r' on the roots of subtrees if possible, else fallback to non-recursive CLI flavor
                    root_datasets = self.root_datasets_if_recursive_zfs_snapshot_is_possible(datasets, basis_src_datasets)
                    if root_datasets is not None:
                        cmd.append("-r")  # recursive; takes a snapshot of all datasets in the subtree(s)
                        datasets_to_snapshot[label] = root_datasets
                commands[label] = cmd
            creation_msg = f"Creating {sum(len(datasets) for datasets in basis_datasets_to_snapshot.values())} snapshots"
            log.info(p.dry("--create-src-snapshots: %s"), f"{creation_msg} within {len(src_datasets)} datasets ...")
            # create snapshots in large (parallel) batches, without using a command line that's too big for the OS to handle
            self.run_ssh_cmd_parallel(
                src,
                [(commands[lbl], [f"{ds}@{lbl}" for ds in datasets]) for lbl, datasets in datasets_to_snapshot.items()],
                fn=lambda cmd, batch: self.run_ssh_command(src, is_dry=p.dry_run, print_stdout=True, cmd=cmd + batch),
                max_batch_items=1 if is_solaris_zfs(p, src) else 2**29,  # solaris CLI doesn't accept multiple datasets
            )
            # perf: copy lastmodified time of source dataset into local cache to reduce future 'zfs list -t snapshot' calls
            self.update_last_modified_cache(basis_datasets_to_snapshot)

        # Optionally, replicate src.root_dataset (optionally including its descendants) to dst.root_dataset
        if not p.skip_replication:
            if len(basis_src_datasets) == 0:
                die(f"Replication: Source dataset does not exist: {src.basis_root_dataset}")
            if is_dummy(dst):
                die("Replication: Destination may be a dummy dataset only if exclusively creating snapshots on the source!")
            src_datasets = filter_src_datasets()  # apply include/exclude policy
            failed = self.replicate_datasets(src_datasets, task_description, max_workers)

        if failed or not (
            p.delete_dst_datasets
            or p.delete_dst_snapshots
            or p.delete_empty_dst_datasets
            or p.compare_snapshot_lists
            or p.monitor_snapshots_config.enable_monitor_snapshots
        ):
            return
        log.info("Listing dst datasets: %s", task_description)
        if is_dummy(dst):
            die("Destination may be a dummy dataset only if exclusively creating snapshots on the source!")
        is_caching = is_caching_snapshots(p, dst) and p.monitor_snapshots_config.enable_monitor_snapshots
        props = "name"
        props = "snapshots_changed," + props if is_caching else props
        cmd = p.split_args(
            f"{p.zfs_program} list -t filesystem,volume -s name -Hp -o {props}", p.recursive_flag, dst.root_dataset
        )
        basis_dst_datasets = []
        basis_dst_datasets_str = self.try_ssh_command(dst, log_trace, cmd=cmd)
        if basis_dst_datasets_str is None:
            log.warning("Destination dataset does not exist: %s", dst.root_dataset)
        else:
            for line in basis_dst_datasets_str.splitlines():
                cols = line.split("\t")
                snapshots_changed, dst_dataset = cols if is_caching else ["-"] + cols
                self.dst_properties[dst_dataset] = {
                    SNAPSHOTS_CHANGED: int(snapshots_changed) if snapshots_changed and snapshots_changed != "-" else 0,
                }
                basis_dst_datasets.append(dst_dataset)

        assert not self.is_test_mode or basis_dst_datasets == sorted(basis_dst_datasets), "List is not sorted"
        dst_datasets = filter_datasets(self, dst, basis_dst_datasets)  # apply include/exclude policy

        # Optionally, delete existing destination datasets that do not exist within the source dataset if they are
        # included via --{include|exclude}-dataset* policy. Do not recurse without --recursive. With --recursive,
        # never delete non-selected dataset subtrees or their ancestors.
        if p.delete_dst_datasets and not failed:
            log.info(p.dry("--delete-dst-datasets: %s"), task_description)
            children = defaultdict(set)
            for dst_dataset in basis_dst_datasets:  # Compute the direct children of each NON-FILTERED dataset
                parent = os.path.dirname(dst_dataset)
                children[parent].add(dst_dataset)
            to_delete: set[str] = set()
            for dst_dataset in reversed(dst_datasets):
                if children[dst_dataset].issubset(to_delete):
                    to_delete.add(dst_dataset)  # all children are deletable, thus the dataset itself is deletable too
            to_delete = to_delete.difference(
                {replace_prefix(src_dataset, src.root_dataset, dst.root_dataset) for src_dataset in basis_src_datasets}
            )
            self.delete_datasets(dst, to_delete)
            dst_datasets = sorted(set(dst_datasets).difference(to_delete))
            basis_dst_datasets = sorted(set(basis_dst_datasets).difference(to_delete))

        # Optionally, delete existing destination snapshots that do not exist within the source dataset if they
        # are included by the --{include|exclude}-snapshot-* policy, and the destination dataset is included
        # via --{include|exclude}-dataset* policy.
        if p.delete_dst_snapshots and not failed:
            log.info(p.dry("--delete-dst-snapshots: %s"), task_description + f" [{len(dst_datasets)} datasets]")
            kind = "bookmark" if p.delete_dst_bookmarks else "snapshot"
            filter_needs_creation_time = has_timerange_filter(p.snapshot_filters)
            props = self.creation_prefix + "creation,guid,name" if filter_needs_creation_time else "guid,name"
            basis_src_datasets_set = set(basis_src_datasets)
            num_snapshots_found, num_snapshots_deleted = 0, 0

            def delete_destination_snapshots(dst_dataset: str, tid: str, retry: Retry) -> bool:  # thread-safe
                src_dataset = replace_prefix(dst_dataset, old_prefix=dst.root_dataset, new_prefix=src.root_dataset)
                if src_dataset in basis_src_datasets_set and (are_bookmarks_enabled(p, src) or not p.delete_dst_bookmarks):
                    src_kind = kind
                    if not p.delete_dst_snapshots_no_crosscheck:
                        src_kind = "snapshot,bookmark" if are_bookmarks_enabled(p, src) else "snapshot"
                    src_cmd = p.split_args(f"{p.zfs_program} list -t {src_kind} -d 1 -s name -Hp -o guid", src_dataset)
                else:
                    src_cmd = None
                dst_cmd = p.split_args(f"{p.zfs_program} list -t {kind} -d 1 -s createtxg -Hp -o {props}", dst_dataset)
                self.maybe_inject_delete(dst, dataset=dst_dataset, delete_trigger="zfs_list_delete_dst_snapshots")
                src_snaps_with_guids, dst_snaps_with_guids = run_in_parallel(  # list src+dst snapshots in parallel
                    lambda: set(self.run_ssh_command(src, log_trace, cmd=src_cmd).splitlines() if src_cmd else []),
                    lambda: self.try_ssh_command(dst, log_trace, cmd=dst_cmd),
                )
                if dst_snaps_with_guids is None:
                    log.warning("Third party deleted destination: %s", dst_dataset)
                    return False
                dst_snaps_with_guids = dst_snaps_with_guids.splitlines()
                num_dst_snaps_with_guids = len(dst_snaps_with_guids)
                basis_dst_snaps_with_guids = dst_snaps_with_guids.copy()
                if p.delete_dst_bookmarks:
                    replace_in_lines(dst_snaps_with_guids, old="#", new="@", count=1)  # treat bookmarks as snapshots
                #  The check against the source dataset happens *after* filtering the dst snapshots with filter_snapshots().
                # `p.delete_dst_snapshots_except` means the user wants to specify snapshots to *retain* aka *keep*
                all_except = p.delete_dst_snapshots_except
                if p.delete_dst_snapshots_except and not is_dummy(src):
                    # However, as here we are in "except" mode AND the source is NOT a dummy, we first filter to get what
                    # the policy says to *keep* (so all_except=False for the filter_snapshots() call), then from that "keep"
                    # list, we later further refine by checking what's on the source dataset.
                    all_except = False
                dst_snaps_with_guids = filter_snapshots(self, dst_snaps_with_guids, all_except=all_except)
                if p.delete_dst_bookmarks:
                    replace_in_lines(dst_snaps_with_guids, old="@", new="#", count=1)  # restore pre-filtering bookmark state
                if filter_needs_creation_time:
                    dst_snaps_with_guids = cut(field=2, lines=dst_snaps_with_guids)
                    basis_dst_snaps_with_guids = cut(field=2, lines=basis_dst_snaps_with_guids)
                if p.delete_dst_snapshots_except and not is_dummy(src):  # Non-dummy Source + "Except" (Keep) Mode
                    # Retain dst snapshots that match snapshot filter policy AND are on src dataset, aka
                    # Delete dst snapshots except snapshots that match snapshot filter policy AND are on src dataset.
                    # Concretely, `dst_snaps_with_guids` contains GUIDs of DST snapshots that the filter policy says to KEEP.
                    # We only actually keep them if they are ALSO on the SRC.
                    # So, snapshots to DELETE (`dst_tags_to_delete`) are ALL snapshots on DST (`basis_dst_snaps_with_guids`)
                    # EXCEPT those whose GUIDs are in `dst_snaps_with_guids` AND ALSO in `src_snaps_with_guids`.
                    except_dst_guids = set(cut(field=1, lines=dst_snaps_with_guids)).intersection(src_snaps_with_guids)
                    dst_tags_to_delete = filter_lines_except(basis_dst_snaps_with_guids, except_dst_guids)
                else:  # Standard Delete Mode OR Dummy Source + "Except" (Keep) Mode
                    # In standard delete mode:
                    #   `dst_snaps_with_guids` contains GUIDs of policy-selected snapshots on DST.
                    #   We delete those that are NOT on SRC.
                    #   `dst_tags_to_delete` = `dst_snaps_with_guids` - `src_snaps_with_guids`.
                    # In dummy source + "except" (keep) mode:
                    #   `all_except` was True.
                    #   `dst_snaps_with_guids` contains snaps NOT matching the "keep" policy -- these are the ones to delete.
                    #   `src_snaps_with_guids` is empty.
                    #   `dst_tags_to_delete` = `dst_snaps_with_guids` - {} = `dst_snaps_with_guids`.
                    dst_guids_to_delete = set(cut(field=1, lines=dst_snaps_with_guids)).difference(src_snaps_with_guids)
                    dst_tags_to_delete = filter_lines(dst_snaps_with_guids, dst_guids_to_delete)
                separator = "#" if p.delete_dst_bookmarks else "@"
                dst_tags_to_delete = cut(field=2, separator=separator, lines=dst_tags_to_delete)
                if p.delete_dst_bookmarks:
                    self.delete_bookmarks(dst, dst_dataset, snapshot_tags=dst_tags_to_delete)
                else:
                    self.delete_snapshots(dst, dst_dataset, snapshot_tags=dst_tags_to_delete)
                with self.stats_lock:
                    nonlocal num_snapshots_found
                    num_snapshots_found += num_dst_snaps_with_guids
                    nonlocal num_snapshots_deleted
                    num_snapshots_deleted += len(dst_tags_to_delete)
                    if len(dst_tags_to_delete) > 0 and not p.delete_dst_bookmarks:
                        self.dst_properties[dst_dataset][SNAPSHOTS_CHANGED] = 0  # invalidate cache
                return True

            # Run delete_destination_snapshots(dataset) for each dataset, while handling errors, retries + parallel exec
            if are_bookmarks_enabled(p, dst) or not p.delete_dst_bookmarks:
                start_time_nanos = time.monotonic_ns()
                failed = process_datasets_in_parallel_and_fault_tolerant(
                    log=p.log,
                    datasets=dst_datasets,
                    process_dataset=delete_destination_snapshots,  # lambda
                    skip_tree_on_error=lambda dataset: False,
                    skip_on_error=p.skip_on_error,
                    max_workers=max_workers,
                    enable_barriers=False,
                    task_name="--delete-dst-snapshots",
                    append_exception=self.append_exception,
                    retry_policy=p.retry_policy,
                    dry_run=p.dry_run,
                    is_test_mode=self.is_test_mode,
                )
                elapsed_nanos = time.monotonic_ns() - start_time_nanos
                log.info(
                    p.dry("--delete-dst-snapshots: %s"),
                    task_description + f" [Deleted {num_snapshots_deleted} out of {num_snapshots_found} {kind}s "
                    f"within {len(dst_datasets)} datasets; took {human_readable_duration(elapsed_nanos)}]",
                )

        # Optionally, delete any existing destination dataset that has no snapshot and no bookmark if all descendants
        # of that dataset do not have a snapshot or bookmark either. To do so, we walk the dataset list (conceptually,
        # a tree) depth-first (i.e. sorted descending). If a dst dataset has zero snapshots and zero bookmarks and all
        # its children are already marked as orphans, then it is itself an orphan, and we mark it as such. Walking in
        # a reverse sorted way means that we efficiently check for zero snapshots/bookmarks not just over the direct
        # children but the entire tree. Finally, delete all orphan datasets in an efficient batched way.
        if p.delete_empty_dst_datasets and p.recursive and not failed:
            log.info(p.dry("--delete-empty-dst-datasets: %s"), task_description)
            delete_empty_dst_datasets_if_no_bookmarks_and_no_snapshots = (
                p.delete_empty_dst_datasets_if_no_bookmarks_and_no_snapshots and are_bookmarks_enabled(p, dst)
            )

            # Compute the direct children of each NON-FILTERED dataset. Thus, no non-selected dataset and no ancestor of a
            # non-selected dataset will ever be added to the "orphan" set. In other words, this treats non-selected dataset
            # subtrees as if they all had snapshots, so non-selected dataset subtrees and their ancestors are guaranteed
            # to not get deleted.
            children = defaultdict(set)
            for dst_dataset in basis_dst_datasets:
                parent = os.path.dirname(dst_dataset)
                children[parent].add(dst_dataset)

            # Find and mark orphan datasets, finally delete them in an efficient way. Using two filter runs instead of one
            # filter run is an optimization. The first run only computes candidate orphans, without incurring I/O, to reduce
            # the list of datasets for which we list snapshots via 'zfs list -t snapshot ...' from dst_datasets to a subset
            # of dst_datasets, which in turn reduces I/O and improves perf. Essentially, this eliminates the I/O to list
            # snapshots for ancestors of excluded datasets. The second run computes the real orphans.
            btype = "bookmark,snapshot" if delete_empty_dst_datasets_if_no_bookmarks_and_no_snapshots else "snapshot"
            dst_datasets_having_snapshots: set[str] = set()
            for run in range(2):
                orphans: set[str] = set()
                for dst_dataset in reversed(dst_datasets):
                    if children[dst_dataset].issubset(orphans):
                        # all children turned out to be orphans, thus the dataset itself could be an orphan
                        if dst_dataset not in dst_datasets_having_snapshots:  # always True during first filter run
                            orphans.add(dst_dataset)
                if run == 0:
                    # find datasets with >= 1 snapshot; update dst_datasets_having_snapshots for real use in the 2nd run
                    cmd = p.split_args(f"{p.zfs_program} list -t {btype} -d 1 -S name -Hp -o name")
                    for datasets_having_snapshots in self.zfs_list_snapshots_in_parallel(
                        dst, cmd, sorted(orphans), ordered=False
                    ):
                        if delete_empty_dst_datasets_if_no_bookmarks_and_no_snapshots:
                            replace_in_lines(datasets_having_snapshots, old="#", new="@", count=1)  # treat bookmarks as snap
                        datasets_having_snapshots = set(cut(field=1, separator="@", lines=datasets_having_snapshots))
                        dst_datasets_having_snapshots.update(datasets_having_snapshots)  # union
                else:
                    self.delete_datasets(dst, orphans)
                    dst_datasets = sorted(set(dst_datasets).difference(orphans))

        # Optionally, compare source and destination dataset trees recursively wrt. snapshots, for example to check if all
        # recently taken snapshots have been successfully replicated by a periodic job.
        if p.compare_snapshot_lists and not failed:
            log.info("--compare-snapshot-lists: %s", task_description)
            if len(basis_src_datasets) == 0 and not is_dummy(src):
                die(f"Source dataset does not exist: {src.basis_root_dataset}")
            src_datasets = filter_src_datasets()  # apply include/exclude policy
            self.run_compare_snapshot_lists(src_datasets, dst_datasets)

        # Optionally, alert the user if the ZFS 'creation' time property of the latest or oldest snapshot for any specified
        # snapshot name pattern within the selected datasets is too old wrt. the specified age limit. The purpose is to
        # check if snapshots are successfully taken on schedule, successfully replicated on schedule, and successfully
        # pruned on schedule. Process exit code is 0, 1, 2 on OK, WARNING, CRITICAL, respectively.
        if p.monitor_snapshots_config.enable_monitor_snapshots and not failed:
            log.info("--monitor-snapshots: %s", task_description)
            src_datasets = filter_src_datasets()  # apply include/exclude policy
            num_cache_hits = self.num_cache_hits
            num_cache_misses = self.num_cache_misses
            start_time_nanos = time.monotonic_ns()
            run_in_parallel(
                lambda: self.monitor_snapshots(dst, dst_datasets), lambda: self.monitor_snapshots(src, src_datasets)
            )
            elapsed = human_readable_duration(time.monotonic_ns() - start_time_nanos)
            if num_cache_hits != self.num_cache_hits or num_cache_misses != self.num_cache_misses:
                total = self.num_cache_hits + self.num_cache_misses
                msg = f", cache hits: {percent(self.num_cache_hits, total)}, misses: {percent(self.num_cache_misses, total)}"
            else:
                msg = ""
            log.info(
                "--monitor-snapshots done: %s",
                f"{task_description} [{len(src_datasets) + len(dst_datasets)} datasets; took {elapsed}{msg}]",
            )

    def monitor_snapshots(self, remote: Remote, sorted_datasets: list[str]) -> None:
        p, log = self.params, self.params.log
        alerts: list[MonitorSnapshotAlert] = p.monitor_snapshots_config.alerts
        labels: list[SnapshotLabel] = [alert.label for alert in alerts]
        current_unixtime_millis: float = p.create_src_snapshots_config.current_datetime.timestamp() * 1000
        is_debug: bool = log.isEnabledFor(log_debug)
        if is_caching_snapshots(p, remote):
            props = self.dst_properties if remote is p.dst else self.src_properties
            snapshots_changed_dict: dict[str, int] = {
                dataset: int(vals[SNAPSHOTS_CHANGED]) for dataset, vals in props.items()
            }
            hash_code: str = hashlib.sha256(str(tuple(alerts)).encode("utf-8")).hexdigest()
        is_caching = False

        def monitor_last_modified_cache_file(r: Remote, dataset: str, label: SnapshotLabel, alert_cfg: AlertConfig) -> str:
            cache_label = SnapshotLabel(os_path_join("===", alert_cfg.kind, str(label), hash_code), "", "", "")
            return self.last_modified_cache_file(r, dataset, cache_label)

        def alert_msg(
            kind: str, dataset: str, snapshot: str, label: SnapshotLabel, snapshot_age_millis: float, delta_millis: int
        ) -> str:
            assert kind == "Latest" or kind == "Oldest"
            lbl = f"{label.prefix}{label.infix}<timestamp>{label.suffix}"
            if snapshot_age_millis >= current_unixtime_millis:
                return f"No snapshot exists for {dataset}@{lbl}"
            msg = f"{kind} snapshot for {dataset}@{lbl} is {human_readable_duration(snapshot_age_millis, unit='ms')} old"
            s = f" ({snapshot})" if snapshot else ""
            if delta_millis == -1:
                return f"{msg}{s}"
            return f"{msg} but should be at most {human_readable_duration(delta_millis, unit='ms')} old{s}"

        def check_alert(
            label: SnapshotLabel,
            alert_cfg: AlertConfig | None,
            creation_unixtime_secs: int,
            dataset: str,
            snapshot: str,
        ) -> None:
            if alert_cfg is None:
                return
            if is_caching and not p.dry_run:  # update cache with latest state from 'zfs list -t snapshot'
                snapshots_changed = snapshots_changed_dict.get(dataset, 0)
                cache_file = monitor_last_modified_cache_file(remote, dataset, label, alert_cfg)
                set_last_modification_time_safe(cache_file, unixtime_in_secs=(creation_unixtime_secs, snapshots_changed))
            warning_millis = alert_cfg.warning_millis
            critical_millis = alert_cfg.critical_millis
            alert_kind = alert_cfg.kind
            snapshot_age_millis = current_unixtime_millis - creation_unixtime_secs * 1000
            m = "--monitor_snapshots: "
            if snapshot_age_millis > critical_millis:
                msg = m + alert_msg(alert_kind, dataset, snapshot, label, snapshot_age_millis, critical_millis)
                log.critical("%s", msg)
                if not p.monitor_snapshots_config.dont_crit:
                    die(msg, exit_code=critical_status)
            elif snapshot_age_millis > warning_millis:
                msg = m + alert_msg(alert_kind, dataset, snapshot, label, snapshot_age_millis, warning_millis)
                log.warning("%s", msg)
                if not p.monitor_snapshots_config.dont_warn:
                    die(msg, exit_code=warning_status)
            elif is_debug:
                msg = m + "OK. " + alert_msg(alert_kind, dataset, snapshot, label, snapshot_age_millis, delta_millis=-1)
                log.debug("%s", msg)

        def alert_latest_snapshot(i: int, creation_unixtime_secs: int, dataset: str, snapshot: str) -> None:
            alert: MonitorSnapshotAlert = alerts[i]
            check_alert(alert.label, alert.latest, creation_unixtime_secs, dataset, snapshot)

        def alert_oldest_snapshot(i: int, creation_unixtime_secs: int, dataset: str, snapshot: str) -> None:
            alert: MonitorSnapshotAlert = alerts[i]
            check_alert(alert.label, alert.oldest, creation_unixtime_secs, dataset, snapshot)

        def find_stale_datasets_and_check_alerts() -> list[str]:
            """If the cache is enabled, check which datasets have changed to determine which datasets can be skipped cheaply,
            i.e. without incurring 'zfs list -t snapshots'. This is done by comparing the "snapshots_changed" ZFS dataset
            property with the local cache - https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html#snapshots_changed"""
            stale_datasets = []
            time_threshold = time.time() - time_threshold_secs
            for dataset in sorted_datasets:
                is_stale_dataset = False
                snapshots_changed: int = snapshots_changed_dict.get(dataset, 0)
                for alert in alerts:
                    for cfg in (alert.latest, alert.oldest):
                        if cfg is None:
                            continue
                        if (
                            snapshots_changed != 0
                            and snapshots_changed < time_threshold
                            and (
                                cached_unix_times := self.cache_get_snapshots_changed2(
                                    monitor_last_modified_cache_file(remote, dataset, alert.label, cfg)
                                )
                            )
                            and snapshots_changed == cached_unix_times[1]  # cached snapshots_changed aka last modified time
                            and snapshots_changed >= cached_unix_times[0]  # creation time of minmax snapshot aka access time
                        ):  # cached state is still valid; emit an alert if the latest/oldest snapshot is too old
                            lbl = alert.label
                            check_alert(lbl, cfg, creation_unixtime_secs=cached_unix_times[0], dataset=dataset, snapshot="")
                        else:  # cached state is nomore valid; fallback to 'zfs list -t snapshot'
                            is_stale_dataset = True
                if is_stale_dataset:
                    stale_datasets.append(dataset)
            return stale_datasets

        # satisfy request from local cache as much as possible
        if is_caching_snapshots(p, remote):
            stale_datasets = find_stale_datasets_and_check_alerts()
            with self.stats_lock:
                self.num_cache_misses += len(stale_datasets)
                self.num_cache_hits += len(sorted_datasets) - len(stale_datasets)
        else:
            stale_datasets = sorted_datasets

        # fallback to 'zfs list -t snapshot' for any remaining datasets, as these couldn't be satisfied from local cache
        is_caching = is_caching_snapshots(p, remote)
        datasets_without_snapshots = self.handle_minmax_snapshots(
            remote, stale_datasets, labels, fn_latest=alert_latest_snapshot, fn_oldest=alert_oldest_snapshot
        )
        for dataset in datasets_without_snapshots:
            for i in range(len(alerts)):
                alert_latest_snapshot(i, creation_unixtime_secs=0, dataset=dataset, snapshot="")
                alert_oldest_snapshot(i, creation_unixtime_secs=0, dataset=dataset, snapshot="")

    def replicate_datasets(self, src_datasets: list[str], task_description: str, max_workers: int) -> bool:
        assert not self.is_test_mode or src_datasets == sorted(src_datasets), "List is not sorted"
        p, log = self.params, self.params.log
        src, dst = p.src, p.dst
        self.num_snapshots_found = 0
        self.num_snapshots_replicated = 0
        # perf/latency: no need to set up a dedicated TCP connection if no parallel replication is possible
        self.dedicated_tcp_connection_per_zfs_send = (
            p.dedicated_tcp_connection_per_zfs_send
            and max_workers > 1
            and has_siblings(src_datasets)  # siblings can be replicated in parallel
        )
        log.info("Starting replication task: %s", task_description + f" [{len(src_datasets)} datasets]")
        start_time_nanos = time.monotonic_ns()

        def src2dst(src_dataset: str) -> str:
            return replace_prefix(src_dataset, old_prefix=src.root_dataset, new_prefix=dst.root_dataset)

        def dst2src(dst_dataset: str) -> str:
            return replace_prefix(dst_dataset, old_prefix=dst.root_dataset, new_prefix=src.root_dataset)

        def find_stale_datasets() -> tuple[list[str], dict[str, str]]:
            """If the cache is enabled on replication, check which src datasets or dst datasets have changed to determine
            which datasets can be skipped cheaply, i.e. without incurring 'zfs list -t snapshots'. This is done by comparing
            the "snapshots_changed" ZFS dataset property with the local cache.
            See https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html#snapshots_changed"""
            # First, check which src datasets have changed since the last replication to that destination
            cache_files = {}
            stale_src_datasets1 = []
            maybe_stale_dst_datasets = []
            userhost_dir = p.dst.ssh_user_host  # cache is only valid for identical destination username+host
            userhost_dir = userhost_dir if userhost_dir else "-"
            hash_key = tuple(tuple(f) for f in p.snapshot_filters)  # cache is only valid for same --include/excl-snapshot*
            hash_code = hashlib.sha256(str(hash_key).encode("utf-8")).hexdigest()
            for src_dataset in src_datasets:
                dst_dataset = src2dst(src_dataset)  # cache is only valid for identical destination dataset
                cache_label = SnapshotLabel(os.path.join("==", userhost_dir, dst_dataset, hash_code), "", "", "")
                cache_file = self.last_modified_cache_file(src, src_dataset, cache_label)
                cache_files[src_dataset] = cache_file
                snapshots_changed: int = int(self.src_properties[src_dataset][SNAPSHOTS_CHANGED])  # get prop "for free"
                if (
                    snapshots_changed != 0
                    and time.time() > snapshots_changed + time_threshold_secs
                    and snapshots_changed == self.cache_get_snapshots_changed(cache_file)
                ):
                    maybe_stale_dst_datasets.append(dst_dataset)
                else:
                    stale_src_datasets1.append(src_dataset)

            # For each src dataset that hasn't changed, check if the corresponding dst dataset has changed
            stale_src_datasets2 = []
            dst_snapshots_changed_dict = self.zfs_get_snapshots_changed(dst, maybe_stale_dst_datasets)
            for dst_dataset in maybe_stale_dst_datasets:
                snapshots_changed = dst_snapshots_changed_dict.get(dst_dataset, 0)
                cache_file = self.last_modified_cache_file(dst, dst_dataset)
                if (
                    snapshots_changed != 0
                    and time.time() > snapshots_changed + time_threshold_secs
                    and snapshots_changed == self.cache_get_snapshots_changed(cache_file)
                ):
                    log.info("Already up-to-date [cached]: %s", dst_dataset)
                else:
                    stale_src_datasets2.append(dst2src(dst_dataset))
            assert not self.is_test_mode or stale_src_datasets1 == sorted(stale_src_datasets1), "List is not sorted"
            assert not self.is_test_mode or stale_src_datasets2 == sorted(stale_src_datasets2), "List is not sorted"
            stale_src_datasets = list(heapq.merge(stale_src_datasets1, stale_src_datasets2))  # merge two sorted lists
            assert not self.is_test_mode or not has_duplicates(stale_src_datasets), "List contains duplicates"
            return stale_src_datasets, cache_files

        if is_caching_snapshots(p, src):
            stale_src_datasets, cache_files = find_stale_datasets()
            num_cache_misses = len(stale_src_datasets)
            num_cache_hits = len(src_datasets) - len(stale_src_datasets)
            self.num_cache_misses += num_cache_misses
            self.num_cache_hits += num_cache_hits
            total = self.num_cache_hits + self.num_cache_misses
            cmsg = f", cache hits: {percent(self.num_cache_hits, total)}, misses: {percent(self.num_cache_misses, total)}"
        else:
            stale_src_datasets = src_datasets
            cache_files = {}
            cmsg = ""

        # Run replicate_dataset(dataset) for each dataset, while taking care of errors, retries + parallel execution
        failed = process_datasets_in_parallel_and_fault_tolerant(
            log=p.log,
            datasets=stale_src_datasets,
            process_dataset=self.replicate_dataset,  # lambda
            skip_tree_on_error=lambda dataset: not self.dst_dataset_exists[src2dst(dataset)],
            skip_on_error=p.skip_on_error,
            max_workers=max_workers,
            enable_barriers=False,
            task_name="Replication",
            append_exception=self.append_exception,
            retry_policy=p.retry_policy,
            dry_run=p.dry_run,
            is_test_mode=self.is_test_mode,
        )

        if is_caching_snapshots(p, src) and not failed:
            # refresh "snapshots_changed" ZFS dataset property from dst
            stale_dst_datasets = [src2dst(src_dataset) for src_dataset in stale_src_datasets]
            dst_snapshots_changed_dict = self.zfs_get_snapshots_changed(dst, stale_dst_datasets)
            for dst_dataset in stale_dst_datasets:  # update local cache
                dst_snapshots_changed = dst_snapshots_changed_dict.get(dst_dataset, 0)
                dst_cache_file = self.last_modified_cache_file(dst, dst_dataset)
                src_dataset = dst2src(dst_dataset)
                src_snapshots_changed: int = int(self.src_properties[src_dataset][SNAPSHOTS_CHANGED])
                if not p.dry_run:
                    set_last_modification_time_safe(cache_files[src_dataset], unixtime_in_secs=src_snapshots_changed)
                    set_last_modification_time_safe(dst_cache_file, unixtime_in_secs=dst_snapshots_changed)

        elapsed_nanos = time.monotonic_ns() - start_time_nanos
        log.info(
            p.dry("Replication done: %s"),
            f"{task_description} [Replicated {self.num_snapshots_replicated} out of {self.num_snapshots_found} snapshots"
            f" within {len(src_datasets)} datasets; took {human_readable_duration(elapsed_nanos)}{cmsg}]",
        )
        return failed

    def replicate_dataset(self, src_dataset: str, tid: str, retry: Retry) -> bool:
        """Replicates src_dataset (without handling descendants) to dst_dataset (thread-safe)."""

        p, log = self.params, self.params.log
        src, dst = p.src, p.dst
        retry_count = retry.count
        dst_dataset = replace_prefix(src_dataset, old_prefix=src.root_dataset, new_prefix=dst.root_dataset)
        log.debug(p.dry(f"{tid} Replicating: %s"), f"{src_dataset} --> {dst_dataset} ...")

        # list GUID and name for dst snapshots, sorted ascending by createtxg (more precise than creation time)
        dst_cmd = p.split_args(f"{p.zfs_program} list -t snapshot -d 1 -s createtxg -Hp -o guid,name", dst_dataset)

        # list GUID and name for src snapshots + bookmarks, primarily sort ascending by transaction group (which is more
        # precise than creation time), secondarily sort such that snapshots appear after bookmarks for the same GUID.
        # Note: A snapshot and its ZFS bookmarks always have the same GUID, creation time and transaction group. A snapshot
        # changes its transaction group but retains its creation time and GUID on 'zfs receive' on another pool, i.e.
        # comparing createtxg is only meaningful within a single pool, not across pools from src to dst. Comparing creation
        # time remains meaningful across pools from src to dst. Creation time is a UTC Unix time in integer seconds.
        # Note that 'zfs create', 'zfs snapshot' and 'zfs bookmark' CLIs enforce that snapshot names must not contain a '#'
        # char, bookmark names must not contain a '@' char, and dataset names must not contain a '#' or '@' char.
        # GUID and creation time also do not contain a '#' or '@' char.
        filter_needs_creation_time = has_timerange_filter(p.snapshot_filters)
        types = "snapshot,bookmark" if p.use_bookmark and are_bookmarks_enabled(p, src) else "snapshot"
        props = self.creation_prefix + "creation,guid,name" if filter_needs_creation_time else "guid,name"
        src_cmd = p.split_args(f"{p.zfs_program} list -t {types} -s createtxg -s type -d 1 -Hp -o {props}", src_dataset)
        self.maybe_inject_delete(src, dataset=src_dataset, delete_trigger="zfs_list_snapshot_src")
        src_snapshots_and_bookmarks, dst_snapshots_with_guids = run_in_parallel(  # list src+dst snapshots in parallel
            lambda: self.try_ssh_command(src, log_trace, cmd=src_cmd),
            lambda: self.try_ssh_command(dst, log_trace, cmd=dst_cmd, error_trigger="zfs_list_snapshot_dst"),
        )
        self.dst_dataset_exists[dst_dataset] = dst_snapshots_with_guids is not None
        dst_snapshots_with_guids = dst_snapshots_with_guids.splitlines() if dst_snapshots_with_guids is not None else []
        if src_snapshots_and_bookmarks is None:
            log.warning("Third party deleted source: %s", src_dataset)
            return False  # src dataset has been deleted by some third party while we're running - nothing to do anymore
        src_snapshots_with_guids: list[str] = src_snapshots_and_bookmarks.splitlines()
        src_snapshots_and_bookmarks = None
        if len(dst_snapshots_with_guids) == 0 and "bookmark" in types:
            # src bookmarks serve no purpose if the destination dataset has no snapshot; ignore them
            src_snapshots_with_guids = [snapshot for snapshot in src_snapshots_with_guids if "@" in snapshot]
        num_src_snapshots_found = sum(1 for snapshot in src_snapshots_with_guids if "@" in snapshot)
        with self.stats_lock:
            self.num_snapshots_found += num_src_snapshots_found
        # apply include/exclude regexes to ignore irrelevant src snapshots
        basis_src_snapshots_with_guids = src_snapshots_with_guids
        src_snapshots_with_guids = filter_snapshots(self, src_snapshots_with_guids)
        if filter_needs_creation_time:
            src_snapshots_with_guids = cut(field=2, lines=src_snapshots_with_guids)
            basis_src_snapshots_with_guids = cut(field=2, lines=basis_src_snapshots_with_guids)

        # find oldest and latest "true" snapshot, as well as GUIDs of all snapshots and bookmarks.
        # a snapshot is "true" if it is not a bookmark.
        oldest_src_snapshot = ""
        latest_src_snapshot = ""
        included_src_guids: set[str] = set()
        for line in src_snapshots_with_guids:
            guid, snapshot = line.split("\t", 1)
            included_src_guids.add(guid)
            if "@" in snapshot:
                latest_src_snapshot = snapshot
                if not oldest_src_snapshot:
                    oldest_src_snapshot = snapshot
        if len(src_snapshots_with_guids) == 0:
            if p.skip_missing_snapshots == "fail":
                die(f"Source dataset includes no snapshot: {src_dataset}. Consider using --skip-missing-snapshots=dataset")
            elif p.skip_missing_snapshots == "dataset":
                log.warning("Skipping source dataset because it includes no snapshot: %s", src_dataset)
                if p.recursive and not self.dst_dataset_exists[dst_dataset]:
                    log.warning("Also skipping descendant datasets as dst dataset does not exist for %s", src_dataset)
                return self.dst_dataset_exists[dst_dataset]

        log.debug("latest_src_snapshot: %s", latest_src_snapshot)
        latest_dst_snapshot = ""
        latest_dst_guid = ""
        latest_common_src_snapshot = ""
        props_cache: dict[tuple[str, str, str], dict[str, str | None]] = {}
        done_checking = False

        if self.dst_dataset_exists[dst_dataset]:
            if len(dst_snapshots_with_guids) > 0:
                latest_dst_guid, latest_dst_snapshot = dst_snapshots_with_guids[-1].split("\t", 1)
                if p.force_rollback_to_latest_snapshot:
                    log.info(p.dry(f"{tid} Rolling back destination to most recent snapshot: %s"), latest_dst_snapshot)
                    # rollback just in case the dst dataset was modified since its most recent snapshot
                    done_checking = done_checking or self.check_zfs_dataset_busy(dst, dst_dataset)
                    cmd = p.split_args(f"{dst.sudo} {p.zfs_program} rollback", latest_dst_snapshot)
                    self.try_ssh_command(dst, log_debug, is_dry=p.dry_run, print_stdout=True, cmd=cmd, exists=False)
            elif latest_src_snapshot == "":
                log.info(f"{tid} Already-up-to-date: %s", dst_dataset)
                return True

            # find most recent snapshot (or bookmark) that src and dst have in common - we'll start to replicate
            # from there up to the most recent src snapshot. any two snapshots are "common" iff their ZFS GUIDs (i.e.
            # contents) are equal. See https://github.com/openzfs/zfs/commit/305bc4b370b20de81eaf10a1cf724374258b74d1
            def latest_common_snapshot(snapshots_with_guids: list[str], intersect_guids: set[str]) -> tuple[str | None, str]:
                """Returns a true snapshot instead of its bookmark with the same GUID, per the sort order previously
                used for 'zfs list -s ...'"""
                for _line in reversed(snapshots_with_guids):
                    guid_, snapshot_ = _line.split("\t", 1)
                    if guid_ in intersect_guids:
                        return guid_, snapshot_  # can be a snapshot or bookmark
                return None, ""

            latest_common_guid, latest_common_src_snapshot = latest_common_snapshot(
                src_snapshots_with_guids, set(cut(field=1, lines=dst_snapshots_with_guids))
            )
            log.debug("latest_common_src_snapshot: %s", latest_common_src_snapshot)  # is a snapshot or bookmark
            log.log(log_trace, "latest_dst_snapshot: %s", latest_dst_snapshot)

            if latest_common_src_snapshot and latest_common_guid != latest_dst_guid:
                # found latest common snapshot but dst has an even newer snapshot. rollback dst to that common snapshot.
                assert latest_common_guid
                _, latest_common_dst_snapshot = latest_common_snapshot(dst_snapshots_with_guids, {latest_common_guid})
                if not (p.force_rollback_to_latest_common_snapshot or p.force):
                    die(
                        f"Conflict: Most recent destination snapshot {latest_dst_snapshot} is more recent than "
                        f"most recent common snapshot {latest_common_dst_snapshot}. Rollback destination first, "
                        "for example via --force-rollback-to-latest-common-snapshot (or --force) option."
                    )
                if p.force_once:
                    p.force.value = False
                    p.force_rollback_to_latest_common_snapshot.value = False
                log.info(
                    p.dry(f"{tid} Rolling back destination to most recent common snapshot: %s"), latest_common_dst_snapshot
                )
                done_checking = done_checking or self.check_zfs_dataset_busy(dst, dst_dataset)
                cmd = p.split_args(
                    f"{dst.sudo} {p.zfs_program} rollback -r {p.force_unmount} {p.force_hard}", latest_common_dst_snapshot
                )
                try:
                    self.run_ssh_command(dst, log_debug, is_dry=p.dry_run, print_stdout=True, cmd=cmd)
                except (subprocess.CalledProcessError, UnicodeDecodeError) as e:
                    stderr = stderr_to_str(e.stderr) if hasattr(e, "stderr") else ""
                    no_sleep = self.clear_resumable_recv_state_if_necessary(dst_dataset, stderr)
                    # op isn't idempotent so retries regather current state from the start of replicate_dataset()
                    raise RetryableError("Subprocess failed", no_sleep=no_sleep) from e

            if latest_src_snapshot and latest_src_snapshot == latest_common_src_snapshot:
                log.info(f"{tid} Already up-to-date: %s", dst_dataset)
                return True

        # endif self.dst_dataset_exists[dst_dataset]
        log.debug("latest_common_src_snapshot: %s", latest_common_src_snapshot)  # is a snapshot or bookmark
        log.log(log_trace, "latest_dst_snapshot: %s", latest_dst_snapshot)
        dry_run_no_send = False
        right_just = 7

        def format_size(num_bytes: int) -> str:
            return human_readable_bytes(num_bytes, separator="").rjust(right_just)

        if not latest_common_src_snapshot:
            # no common snapshot was found. delete all dst snapshots, if any
            if latest_dst_snapshot:
                if not p.force:
                    die(
                        f"Conflict: No common snapshot found between {src_dataset} and {dst_dataset} even though "
                        "destination has at least one snapshot. Aborting. Consider using --force option to first "
                        "delete all existing destination snapshots in order to be able to proceed with replication."
                    )
                if p.force_once:
                    p.force.value = False
                done_checking = done_checking or self.check_zfs_dataset_busy(dst, dst_dataset)
                self.delete_snapshots(dst, dst_dataset, snapshot_tags=cut(2, separator="@", lines=dst_snapshots_with_guids))
                if p.dry_run:
                    # As we're in --dryrun (--force) mode this conflict resolution step (see above) wasn't really executed:
                    # "no common snapshot was found. delete all dst snapshots". In turn, this would cause the subsequent
                    # 'zfs receive -n' to fail with "cannot receive new filesystem stream: destination has snapshots; must
                    # destroy them to overwrite it". So we skip the zfs send/receive step and keep on trucking.
                    dry_run_no_send = True

            # to start with, fully replicate oldest snapshot, which in turn creates a common snapshot
            if p.no_stream:
                oldest_src_snapshot = latest_src_snapshot
            if oldest_src_snapshot:
                if not self.dst_dataset_exists[dst_dataset]:
                    # on destination, create parent filesystem and ancestors if they do not yet exist
                    dst_dataset_parent = os.path.dirname(dst_dataset)
                    if not self.dst_dataset_exists[dst_dataset_parent]:
                        if p.dry_run:
                            dry_run_no_send = True
                        if dst_dataset_parent != "":
                            self.create_zfs_filesystem(dst_dataset_parent)

                recv_resume_token, send_resume_opts, recv_resume_opts = self._recv_resume_token(dst_dataset, retry_count)
                curr_size = self.estimate_send_size(src, dst_dataset, recv_resume_token, oldest_src_snapshot)
                humansize = format_size(curr_size)
                if recv_resume_token:
                    send_opts = send_resume_opts  # e.g. ["-t", "1-c740b4779-..."]
                else:
                    send_opts = p.curr_zfs_send_program_opts + [oldest_src_snapshot]
                send_cmd = p.split_args(f"{src.sudo} {p.zfs_program} send", send_opts)
                recv_opts = p.zfs_full_recv_opts.copy() + recv_resume_opts
                recv_opts, set_opts = self.add_recv_property_options(True, recv_opts, src_dataset, props_cache)
                recv_cmd = p.split_args(
                    f"{dst.sudo} {p.zfs_program} receive -F", p.dry_run_recv, recv_opts, dst_dataset, allow_all=True
                )
                log.info(p.dry(f"{tid} Full send: %s"), f"{oldest_src_snapshot} --> {dst_dataset} ({humansize.strip()}) ...")
                done_checking = done_checking or self.check_zfs_dataset_busy(dst, dst_dataset)
                dry_run_no_send = dry_run_no_send or p.dry_run_no_send
                self.maybe_inject_params(error_trigger="full_zfs_send_params")
                humansize = humansize.rjust(right_just * 3 + 2)
                self.run_zfs_send_receive(
                    src_dataset, dst_dataset, send_cmd, recv_cmd, curr_size, humansize, dry_run_no_send, "full_zfs_send"
                )
                latest_common_src_snapshot = oldest_src_snapshot  # we have now created a common snapshot
                if not dry_run_no_send and not p.dry_run:
                    self.dst_dataset_exists[dst_dataset] = True
                with self.stats_lock:
                    self.num_snapshots_replicated += 1
                self.create_zfs_bookmarks(src, src_dataset, [oldest_src_snapshot])
                self.zfs_set(set_opts, dst, dst_dataset)
                dry_run_no_send = dry_run_no_send or p.dry_run
                retry_count = 0

        # endif not latest_common_src_snapshot
        # finally, incrementally replicate all snapshots from most recent common snapshot until most recent src snapshot
        if latest_common_src_snapshot:

            def replication_candidates() -> tuple[list[str], list[str]]:
                assert len(basis_src_snapshots_with_guids) > 0
                result_snapshots = []
                result_guids = []
                last_appended_guid = ""
                snapshot_itr = reversed(basis_src_snapshots_with_guids)
                while True:
                    guid, snapshot = snapshot_itr.__next__().split("\t", 1)
                    if "@" in snapshot:
                        result_snapshots.append(snapshot)
                        result_guids.append(guid)
                        last_appended_guid = guid
                    if snapshot == latest_common_src_snapshot:  # latest_common_src_snapshot is a snapshot or bookmark
                        if guid != last_appended_guid and "@" not in snapshot:
                            # only appends the src bookmark if it has no snapshot. If the bookmark has a snapshot then that
                            # snapshot has already been appended, per the sort order previously used for 'zfs list -s ...'
                            result_snapshots.append(snapshot)
                            result_guids.append(guid)
                        break
                result_snapshots.reverse()
                result_guids.reverse()
                assert len(result_snapshots) > 0
                assert len(result_snapshots) == len(result_guids)
                return result_guids, result_snapshots

            # collect the most recent common snapshot (which may be a bookmark) followed by all src snapshots
            # (that are not a bookmark) that are more recent than that.
            cand_guids, cand_snapshots = replication_candidates()
            if len(cand_snapshots) == 1:
                # latest_src_snapshot is a (true) snapshot that is equal to latest_common_src_snapshot or LESS recent
                # than latest_common_src_snapshot. The latter case can happen if latest_common_src_snapshot is a
                # bookmark whose snapshot has been deleted on src.
                return True  # nothing more tbd

            recv_resume_token, send_resume_opts, recv_resume_opts = self._recv_resume_token(dst_dataset, retry_count)
            recv_opts = p.zfs_recv_program_opts.copy() + recv_resume_opts
            recv_opts, set_opts = self.add_recv_property_options(False, recv_opts, src_dataset, props_cache)
            if p.no_stream:
                # skip intermediate snapshots
                steps_todo = [("-i", latest_common_src_snapshot, latest_src_snapshot, [latest_src_snapshot])]
            else:
                # include intermediate src snapshots that pass --{include,exclude}-snapshot-* policy, using
                # a series of -i/-I send/receive steps that skip excluded src snapshots.
                steps_todo = self.incremental_send_steps_wrapper(
                    cand_snapshots, cand_guids, included_src_guids, recv_resume_token is not None
                )
            log.log(log_trace, "steps_todo: %s", list_formatter(steps_todo, "; "))
            estimate_send_sizes = [
                self.estimate_send_size(
                    src, dst_dataset, recv_resume_token if i == 0 else None, incr_flag, from_snap, to_snap
                )
                for i, (incr_flag, from_snap, to_snap, to_snapshots) in enumerate(steps_todo)
            ]
            total_size = sum(estimate_send_sizes)
            total_num = sum(len(to_snapshots) for incr_flag, from_snap, to_snap, to_snapshots in steps_todo)
            done_size = 0
            done_num = 0
            for i, (incr_flag, from_snap, to_snap, to_snapshots) in enumerate(steps_todo):
                assert len(to_snapshots) >= 1
                curr_num_snapshots = len(to_snapshots)
                curr_size = estimate_send_sizes[i]
                humansize = format_size(total_size) + "/" + format_size(done_size) + "/" + format_size(curr_size)
                human_num = f"{total_num}/{done_num}/{curr_num_snapshots} snapshots"
                if recv_resume_token:
                    send_opts = send_resume_opts  # e.g. ["-t", "1-c740b4779-..."]
                else:
                    send_opts = p.curr_zfs_send_program_opts + [incr_flag, from_snap, to_snap]
                send_cmd = p.split_args(f"{src.sudo} {p.zfs_program} send", send_opts)
                recv_cmd = p.split_args(
                    f"{dst.sudo} {p.zfs_program} receive", p.dry_run_recv, recv_opts, dst_dataset, allow_all=True
                )
                dense_size = p.two_or_more_spaces_regex.sub("", humansize.strip())
                log.info(
                    p.dry(f"{tid} Incremental send {incr_flag}: %s"),
                    f"{from_snap} .. {to_snap[to_snap.index('@'):]} --> {dst_dataset} ({dense_size}) ({human_num}) ...",
                )
                done_checking = done_checking or self.check_zfs_dataset_busy(dst, dst_dataset, busy_if_send=False)
                if p.dry_run and not self.dst_dataset_exists[dst_dataset]:
                    dry_run_no_send = True
                dry_run_no_send = dry_run_no_send or p.dry_run_no_send
                self.maybe_inject_params(error_trigger="incr_zfs_send_params")
                self.run_zfs_send_receive(
                    src_dataset, dst_dataset, send_cmd, recv_cmd, curr_size, humansize, dry_run_no_send, "incr_zfs_send"
                )
                done_size += curr_size
                done_num += curr_num_snapshots
                recv_resume_token = None
                with self.stats_lock:
                    self.num_snapshots_replicated += curr_num_snapshots
                if p.create_bookmarks == "all":
                    self.create_zfs_bookmarks(src, src_dataset, to_snapshots)
                elif p.create_bookmarks == "many":
                    to_snapshots = [snap for snap in to_snapshots if p.xperiods.label_milliseconds(snap) >= 60 * 60 * 1000]
                    if i == len(steps_todo) - 1 and (len(to_snapshots) == 0 or to_snapshots[-1] != to_snap):
                        to_snapshots.append(to_snap)
                    self.create_zfs_bookmarks(src, src_dataset, to_snapshots)
            self.zfs_set(set_opts, dst, dst_dataset)
        return True

    def prepare_zfs_send_receive(
        self, src_dataset: str, send_cmd: list[str], recv_cmd: list[str], size_estimate_bytes: int, size_estimate_human: str
    ) -> tuple[str, str, str]:
        p = self.params
        send_cmd_str = shlex.join(send_cmd)
        recv_cmd_str = shlex.join(recv_cmd)

        if p.is_program_available("zstd", "src") and p.is_program_available("zstd", "dst"):
            compress_cmd_ = self.compress_cmd("src", size_estimate_bytes)
            decompress_cmd_ = self.decompress_cmd("dst", size_estimate_bytes)
        else:  # no compression is used if source and destination do not both support compression
            compress_cmd_, decompress_cmd_ = "cat", "cat"

        recordsize = abs(int(self.src_properties[src_dataset]["recordsize"]))
        src_buffer = self.mbuffer_cmd("src", size_estimate_bytes, recordsize)
        dst_buffer = self.mbuffer_cmd("dst", size_estimate_bytes, recordsize)
        local_buffer = self.mbuffer_cmd("local", size_estimate_bytes, recordsize)

        pv_src_cmd = ""
        pv_dst_cmd = ""
        pv_loc_cmd = ""
        if p.src.ssh_user_host == "":
            pv_src_cmd = self.pv_cmd("local", size_estimate_bytes, size_estimate_human)
        elif p.dst.ssh_user_host == "":
            pv_dst_cmd = self.pv_cmd("local", size_estimate_bytes, size_estimate_human)
        elif compress_cmd_ == "cat":
            pv_loc_cmd = self.pv_cmd("local", size_estimate_bytes, size_estimate_human)  # compression disabled
        else:
            # pull-push mode with compression enabled: reporting "percent complete" isn't straightforward because
            # localhost observes the compressed data instead of the uncompressed data, so we disable the progress bar.
            pv_loc_cmd = self.pv_cmd("local", size_estimate_bytes, size_estimate_human, disable_progress_bar=True)

        # assemble pipeline running on source leg
        src_pipe = ""
        if self.inject_params.get("inject_src_pipe_fail", False):
            # for testing; initially forward some bytes and then fail
            src_pipe = f"{src_pipe} | dd bs=64 count=1 2>/dev/null && false"
        if self.inject_params.get("inject_src_pipe_garble", False):
            src_pipe = f"{src_pipe} | base64"  # for testing; forward garbled bytes
        if pv_src_cmd != "" and pv_src_cmd != "cat":
            src_pipe = f"{src_pipe} | {pv_src_cmd}"
        if compress_cmd_ != "cat":
            src_pipe = f"{src_pipe} | {compress_cmd_}"
        if src_buffer != "cat":
            src_pipe = f"{src_pipe} | {src_buffer}"
        if src_pipe.startswith(" |"):
            src_pipe = src_pipe[2:]  # strip leading ' |' part
        if self.inject_params.get("inject_src_send_error", False):
            send_cmd_str = f"{send_cmd_str} --injectedGarbageParameter"  # for testing; induce CLI parse error
        if src_pipe != "":
            src_pipe = f"{send_cmd_str} | {src_pipe}"
            if p.src.ssh_user_host != "":
                src_pipe = p.shell_program + " -c " + self.dquote(src_pipe)
        else:
            src_pipe = send_cmd_str

        # assemble pipeline running on middle leg between source and destination. only enabled for pull-push mode
        local_pipe = ""
        if local_buffer != "cat":
            local_pipe = f"{local_buffer}"
        if pv_loc_cmd != "" and pv_loc_cmd != "cat":
            local_pipe = f"{local_pipe} | {pv_loc_cmd}"
        if local_buffer != "cat":
            local_pipe = f"{local_pipe} | {local_buffer}"
        if local_pipe.startswith(" |"):
            local_pipe = local_pipe[2:]  # strip leading ' |' part
        if local_pipe != "":
            local_pipe = f"| {local_pipe}"

        # assemble pipeline running on destination leg
        dst_pipe = ""
        if dst_buffer != "cat":
            dst_pipe = f"{dst_buffer}"
        if decompress_cmd_ != "cat":
            dst_pipe = f"{dst_pipe} | {decompress_cmd_}"
        if pv_dst_cmd != "" and pv_dst_cmd != "cat":
            dst_pipe = f"{dst_pipe} | {pv_dst_cmd}"
        if self.inject_params.get("inject_dst_pipe_fail", False):
            # interrupt zfs receive for testing retry/resume; initially forward some bytes and then stop forwarding
            dst_pipe = f"{dst_pipe} | dd bs=1024 count={inject_dst_pipe_fail_kbytes} 2>/dev/null"
        if self.inject_params.get("inject_dst_pipe_garble", False):
            dst_pipe = f"{dst_pipe} | base64"  # for testing; forward garbled bytes
        if dst_pipe.startswith(" |"):
            dst_pipe = dst_pipe[2:]  # strip leading ' |' part
        if self.inject_params.get("inject_dst_receive_error", False):
            recv_cmd_str = f"{recv_cmd_str} --injectedGarbageParameter"  # for testing; induce CLI parse error
        if dst_pipe != "":
            dst_pipe = f"{dst_pipe} | {recv_cmd_str}"
            if p.dst.ssh_user_host != "":
                dst_pipe = p.shell_program + " -c " + self.dquote(dst_pipe)
        else:
            dst_pipe = recv_cmd_str

        # If there's no support for shell pipelines, we can't do compression, mbuffering, monitoring and rate-limiting,
        # so we fall back to simple zfs send/receive.
        if not p.is_program_available("sh", "src"):
            src_pipe = send_cmd_str
        if not p.is_program_available("sh", "dst"):
            dst_pipe = recv_cmd_str
        if not p.is_program_available("sh", "local"):
            local_pipe = ""

        src_pipe = self.squote(p.src, src_pipe)
        dst_pipe = self.squote(p.dst, dst_pipe)
        return src_pipe, local_pipe, dst_pipe

    def run_zfs_send_receive(
        self,
        src_dataset: str,
        dst_dataset: str,
        send_cmd: list[str],
        recv_cmd: list[str],
        size_estimate_bytes: int,
        size_estimate_human: str,
        dry_run_no_send: bool,
        error_trigger: str | None = None,
    ) -> None:
        p, log = self.params, self.params.log
        src_pipe, local_pipe, dst_pipe = self.prepare_zfs_send_receive(
            src_dataset, send_cmd, recv_cmd, size_estimate_bytes, size_estimate_human
        )
        conn_pool_name = DEDICATED if self.dedicated_tcp_connection_per_zfs_send else SHARED
        src_conn_pool: ConnectionPool = p.connection_pools["src"].pool(conn_pool_name)
        src_conn: Connection = src_conn_pool.get_connection()
        dst_conn_pool: ConnectionPool = p.connection_pools["dst"].pool(conn_pool_name)
        dst_conn: Connection = dst_conn_pool.get_connection()
        try:
            refresh_ssh_connection_if_necessary(self, p.src, src_conn)
            refresh_ssh_connection_if_necessary(self, p.dst, dst_conn)
            src_ssh_cmd = " ".join(src_conn.ssh_cmd_quoted)
            dst_ssh_cmd = " ".join(dst_conn.ssh_cmd_quoted)
            cmd = [p.shell_program_local, "-c", f"{src_ssh_cmd} {src_pipe} {local_pipe} | {dst_ssh_cmd} {dst_pipe}"]
            msg = "Would execute: %s" if dry_run_no_send else "Executing: %s"
            log.debug(msg, cmd[2].lstrip())
            if not dry_run_no_send:
                try:
                    maybe_inject_error(self, cmd=cmd, error_trigger=error_trigger)
                    process = subprocess_run(
                        cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True, timeout=timeout(self), check=True
                    )
                except (subprocess.CalledProcessError, UnicodeDecodeError) as e:
                    no_sleep = False
                    if not isinstance(e, UnicodeDecodeError):
                        xprint(log, stderr_to_str(e.stdout), file=sys.stdout)
                        log.warning("%s", stderr_to_str(e.stderr).rstrip())
                    if isinstance(e, subprocess.CalledProcessError):
                        no_sleep = self.clear_resumable_recv_state_if_necessary(dst_dataset, e.stderr)
                    # op isn't idempotent so retries regather current state from the start of replicate_dataset()
                    raise RetryableError("Subprocess failed", no_sleep=no_sleep) from e
                else:
                    xprint(log, process.stdout, file=sys.stdout)
                    xprint(log, process.stderr, file=sys.stderr)
        finally:
            dst_conn_pool.return_connection(dst_conn)
            src_conn_pool.return_connection(src_conn)

    def clear_resumable_recv_state_if_necessary(self, dst_dataset: str, stderr: str) -> bool:
        def clear_resumable_recv_state() -> bool:
            log.warning(p.dry("Aborting an interrupted zfs receive -s, deleting partially received state: %s"), dst_dataset)
            cmd = p.split_args(f"{p.dst.sudo} {p.zfs_program} receive -A", dst_dataset)
            self.try_ssh_command(p.dst, log_trace, is_dry=p.dry_run, print_stdout=True, cmd=cmd)
            log.log(log_trace, p.dry("Done Aborting an interrupted zfs receive -s: %s"), dst_dataset)
            return True

        p, log = self.params, self.params.log
        # "cannot resume send: 'wb_src/tmp/src@s1' is no longer the same snapshot used in the initial send"
        # "cannot resume send: 'wb_src/tmp/src@s1' used in the initial send no longer exists"
        # "cannot resume send: incremental source 0xa000000000000000 no longer exists"
        if "cannot resume send" in stderr and (
            "is no longer the same snapshot used in the initial send" in stderr
            or "used in the initial send no longer exists" in stderr
            or re.match(r".*incremental source [0-9a-fx]+ no longer exists", stderr)
        ):
            return clear_resumable_recv_state()

        # "cannot receive resume stream: incompatible embedded data stream feature with encrypted receive."
        #     see https://github.com/openzfs/zfs/issues/12480
        # 'cannot receive new filesystem stream: destination xx contains partially-complete state from "zfs receive -s"'
        #     this indicates that --no-resume-recv detects that dst contains a previously interrupted recv -s
        elif "cannot receive" in stderr and (
            "cannot receive resume stream: incompatible embedded data stream feature with encrypted receive" in stderr
            or 'contains partially-complete state from "zfs receive -s"' in stderr
        ):
            return clear_resumable_recv_state()

        elif (  # this signals normal behavior on interrupt of 'zfs receive -s' if running without --no-resume-recv
            "cannot receive new filesystem stream: checksum mismatch or incomplete stream" in stderr
            and "Partially received snapshot is saved" in stderr
        ):
            return True

        # "cannot destroy 'wb_dest/tmp/dst@s1': snapshot has dependent clones ... use '-R' to destroy the following
        # datasets: wb_dest/tmp/dst/%recv" # see https://github.com/openzfs/zfs/issues/10439#issuecomment-642774560
        # This msg indicates a failed 'zfs destroy' via --delete-dst-snapshots. This "clone" is caused by a previously
        # interrupted 'zfs receive -s'. The fix used here is to delete the partially received state of said
        # 'zfs receive -s' via 'zfs receive -A', followed by an automatic retry, which will now succeed to delete the
        # snapshot without user intervention.
        elif (
            "cannot destroy" in stderr
            and "snapshot has dependent clone" in stderr
            and "use '-R' to destroy the following dataset" in stderr
            and f"\n{dst_dataset}/%recv\n" in stderr
        ):
            return clear_resumable_recv_state()

        # Same cause as above, except that this error can occur during 'zfs rollback'
        # Also see https://github.com/openzfs/zfs/blob/master/cmd/zfs/zfs_main.c
        elif (
            "cannot rollback to" in stderr
            and "clones of previous snapshots exist" in stderr
            and "use '-R' to force deletion of the following clones and dependents" in stderr
            and f"\n{dst_dataset}/%recv\n" in stderr
        ):
            return clear_resumable_recv_state()

        return False

    def _recv_resume_token(self, dst_dataset: str, retry_count: int) -> tuple[str | None, list[str], list[str]]:
        """Gets recv_resume_token ZFS property from dst_dataset and returns corresponding opts to use for send+recv."""
        p, log = self.params, self.params.log
        if not p.resume_recv:
            return None, [], []
        warning = None
        if not is_zpool_feature_enabled_or_active(p, p.dst, "feature@extensible_dataset"):
            warning = "not available on destination dataset"
        elif not p.is_program_available(zfs_version_is_at_least_2_1_0, "dst"):
            warning = "unreliable as zfs version is too old"  # e.g. zfs-0.8.3 "internal error: Unknown error 1040"
        if warning:
            log.warning(f"ZFS receive resume feature is {warning}. Falling back to --no-resume-recv: %s", dst_dataset)
            return None, [], []
        recv_resume_token = None
        send_resume_opts = []
        if self.dst_dataset_exists[dst_dataset]:
            cmd = p.split_args(f"{p.zfs_program} get -Hp -o value -s none receive_resume_token", dst_dataset)
            recv_resume_token = self.run_ssh_command(p.dst, log_trace, cmd=cmd).rstrip()
            if recv_resume_token == "-" or not recv_resume_token:  # noqa: S105
                recv_resume_token = None
            else:
                send_resume_opts += ["-n"] if p.dry_run else []
                send_resume_opts += ["-v"] if p.verbose_zfs else []
                send_resume_opts += ["-t", recv_resume_token]
        recv_resume_opts = ["-s"]
        return recv_resume_token, send_resume_opts, recv_resume_opts

    def mbuffer_cmd(self, loc: str, size_estimate_bytes: int, recordsize: int) -> str:
        """If mbuffer command is on the PATH, uses it in the ssh network pipe between 'zfs send' and 'zfs receive' to
        smooth out the rate of data flow and prevent bottlenecks caused by network latency or speed fluctuation."""
        p = self.params
        if (
            size_estimate_bytes >= p.min_pipe_transfer_size
            and (
                (loc == "src" and (p.src.is_nonlocal or p.dst.is_nonlocal))
                or (loc == "dst" and (p.src.is_nonlocal or p.dst.is_nonlocal))
                or (loc == "local" and p.src.is_nonlocal and p.dst.is_nonlocal)
            )
            and p.is_program_available("mbuffer", loc)
        ):
            recordsize = max(recordsize, 128 * 1024 if is_solaris_zfs_location(p, loc) else 2 * 1024 * 1024)
            return shlex.join([p.mbuffer_program, "-s", str(recordsize)] + p.mbuffer_program_opts)
        else:
            return "cat"

    def compress_cmd(self, loc: str, size_estimate_bytes: int) -> str:
        """If zstd command is on the PATH, uses it in the ssh network pipe between 'zfs send' and 'zfs receive' to
        reduce network bottlenecks by sending compressed data."""
        p = self.params
        if (
            size_estimate_bytes >= p.min_pipe_transfer_size
            and (p.src.is_nonlocal or p.dst.is_nonlocal)
            and p.is_program_available("zstd", loc)
        ):
            return shlex.join([p.compression_program, "-c"] + p.compression_program_opts)
        else:
            return "cat"

    def decompress_cmd(self, loc: str, size_estimate_bytes: int) -> str:
        p = self.params
        if (
            size_estimate_bytes >= p.min_pipe_transfer_size
            and (p.src.is_nonlocal or p.dst.is_nonlocal)
            and p.is_program_available("zstd", loc)
        ):
            return shlex.join([p.compression_program, "-dc"])
        else:
            return "cat"

    worker_thread_number_regex: re.Pattern = re.compile(r"ThreadPoolExecutor-\d+_(\d+)")

    def pv_cmd(
        self, loc: str, size_estimate_bytes: int, size_estimate_human: str, disable_progress_bar: bool = False
    ) -> str:
        """If pv command is on the PATH, monitors the progress of data transfer from 'zfs send' to 'zfs receive'.
        Progress can be viewed via "tail -f $pv_log_file" aka tail -f ~/bzfs-logs/current.pv or similar."""
        p = self.params
        if p.is_program_available("pv", loc):
            size = f"--size={size_estimate_bytes}"
            if disable_progress_bar or size_estimate_bytes == 0:
                size = ""
            pv_log_file = p.log_params.pv_log_file
            thread_name = threading.current_thread().name
            if match := Job.worker_thread_number_regex.fullmatch(thread_name):
                worker = int(match.group(1))
                if worker > 0:
                    pv_log_file += pv_file_thread_separator + f"{worker:04}"
            if self.is_first_replication_task.get_and_set(False):
                if self.isatty and not p.quiet:
                    self.progress_reporter.start()
                self.replication_start_time_nanos = time.monotonic_ns()
            if self.isatty and not p.quiet:
                self.progress_reporter.enqueue_pv_log_file(pv_log_file)
            pv_program_opts = [p.pv_program] + p.pv_program_opts
            if self.progress_update_intervals is not None:  # for testing
                pv_program_opts += [f"--interval={self.progress_update_intervals[0]}"]
            pv_program_opts += ["--force", f"--name={size_estimate_human}"]
            pv_program_opts += [size] if size else []
            return f"LC_ALL=C {shlex.join(pv_program_opts)} 2>> {shlex.quote(pv_log_file)}"
        else:
            return "cat"

    def run_ssh_command(
        self,
        remote: Remote,
        level: int = -1,
        is_dry: bool = False,
        check: bool = True,
        print_stdout: bool = False,
        print_stderr: bool = True,
        cmd: list[str] | None = None,
    ) -> str:
        return bzfs_main.connection.run_ssh_command(
            self,
            remote=remote,
            level=level,
            is_dry=is_dry,
            check=check,
            print_stdout=print_stdout,
            print_stderr=print_stderr,
            cmd=cmd,
        )

    def try_ssh_command(
        self,
        remote: Remote,
        level: int,
        is_dry: bool = False,
        print_stdout: bool = False,
        cmd: list[str] | None = None,
        exists: bool = True,
        error_trigger: str | None = None,
    ) -> str | None:
        return bzfs_main.connection.try_ssh_command(
            self,
            remote=remote,
            level=level,
            is_dry=is_dry,
            print_stdout=print_stdout,
            cmd=cmd,
            exists=exists,
            error_trigger=error_trigger,
        )

    def maybe_inject_delete(self, remote: Remote, dataset: str, delete_trigger: str) -> None:
        """For testing only; for unit tests to delete datasets during replication and test correct handling of that."""
        assert delete_trigger
        counter = self.delete_injection_triggers.get("before")
        if counter and decrement_injection_counter(self, counter, delete_trigger):
            p = self.params
            cmd = p.split_args(f"{remote.sudo} {p.zfs_program} destroy -r", p.force_unmount, p.force_hard, dataset or "")
            self.run_ssh_command(remote, log_debug, print_stdout=True, cmd=cmd)

    def maybe_inject_params(self, error_trigger: str) -> None:
        """For testing only; for unit tests to simulate errors during replication and test correct handling of them."""
        assert error_trigger
        counter = self.error_injection_triggers.get("before")
        if counter and decrement_injection_counter(self, counter, error_trigger):
            self.inject_params = self.param_injection_triggers[error_trigger]
        elif error_trigger in self.param_injection_triggers:
            self.inject_params = {}

    @staticmethod
    def squote(remote: Remote, arg: str) -> str:
        return arg if remote.ssh_user_host == "" else shlex.quote(arg)

    @staticmethod
    def dquote(arg: str) -> str:
        """shell-escapes double quotes and dollar and backticks, then surrounds with double quotes."""
        return '"' + arg.replace('"', '\\"').replace("$", "\\$").replace("`", "\\`") + '"'

    def delete_snapshots(self, remote: Remote, dataset: str, snapshot_tags: list[str]) -> None:
        if len(snapshot_tags) == 0:
            return
        p, log = self.params, self.params.log
        log.info(p.dry(f"Deleting {len(snapshot_tags)} snapshots within %s: %s"), dataset, snapshot_tags)
        # delete snapshots in batches without creating a command line that's too big for the OS to handle
        self.run_ssh_cmd_batched(
            remote,
            self.delete_snapshot_cmd(remote, dataset + "@"),
            snapshot_tags,
            lambda batch: self.delete_snapshot(remote, dataset, dataset + "@" + ",".join(batch)),
            max_batch_items=1 if is_solaris_zfs(p, remote) else self.params.max_snapshots_per_minibatch_on_delete_snaps,
            sep=",",
        )

    def delete_snapshot(self, r: Remote, dataset: str, snapshots_to_delete: str) -> None:
        p = self.params
        cmd = self.delete_snapshot_cmd(r, snapshots_to_delete)
        is_dry = p.dry_run and is_solaris_zfs(p, r)  # solaris-11.4 knows no 'zfs destroy -n' flag
        try:
            maybe_inject_error(self, cmd=cmd, error_trigger="zfs_delete_snapshot")
            self.run_ssh_command(r, log_debug, is_dry=is_dry, print_stdout=True, cmd=cmd)
        except (subprocess.CalledProcessError, UnicodeDecodeError) as e:
            stderr = stderr_to_str(e.stderr) if hasattr(e, "stderr") else ""
            no_sleep = self.clear_resumable_recv_state_if_necessary(dataset, stderr)
            # op isn't idempotent so retries regather current state from the start
            raise RetryableError("Subprocess failed", no_sleep=no_sleep) from e

    def delete_snapshot_cmd(self, r: Remote, snapshots_to_delete: str) -> list[str]:
        p = self.params
        return p.split_args(
            f"{r.sudo} {p.zfs_program} destroy", p.force_hard, p.verbose_destroy, p.dry_run_destroy, snapshots_to_delete
        )

    def delete_bookmarks(self, remote: Remote, dataset: str, snapshot_tags: list[str]) -> None:
        if len(snapshot_tags) == 0:
            return
        # Unfortunately ZFS has no syntax yet to delete multiple bookmarks in a single CLI invocation
        p, log = self.params, self.params.log
        log.info(
            p.dry(f"Deleting {len(snapshot_tags)} bookmarks within %s: %s"), dataset, dataset + "#" + ",".join(snapshot_tags)
        )
        cmd = p.split_args(f"{remote.sudo} {p.zfs_program} destroy")
        self.run_ssh_cmd_parallel(
            remote,
            [(cmd, [f"{dataset}#{snapshot_tag}" for snapshot_tag in snapshot_tags])],
            lambda _cmd, batch: self.try_ssh_command(
                remote, log_debug, is_dry=p.dry_run, print_stdout=True, cmd=_cmd + batch, exists=False
            ),
            max_batch_items=1,
        )

    def delete_datasets(self, remote: Remote, datasets: Iterable[str]) -> None:
        """Deletes the given datasets via zfs destroy -r on the given remote."""
        # Impl is batch optimized to minimize CLI + network roundtrips: only need to run zfs destroy if previously
        # destroyed dataset (within sorted datasets) is not a prefix (aka ancestor) of current dataset
        p, log = self.params, self.params.log
        last_deleted_dataset = DONT_SKIP_DATASET
        for dataset in sorted(datasets):
            if is_descendant(dataset, of_root_dataset=last_deleted_dataset):
                continue
            log.info(p.dry("Deleting dataset tree: %s"), f"{dataset} ...")
            cmd = p.split_args(
                f"{remote.sudo} {p.zfs_program} destroy -r {p.force_unmount} {p.force_hard} {p.verbose_destroy}",
                p.dry_run_destroy,
                dataset,
            )
            is_dry = p.dry_run and is_solaris_zfs(p, remote)  # solaris-11.4 knows no 'zfs destroy -n' flag
            self.run_ssh_command(remote, log_debug, is_dry=is_dry, print_stdout=True, cmd=cmd)
            last_deleted_dataset = dataset

    def create_zfs_filesystem(self, filesystem: str) -> None:
        # zfs create -p -u $filesystem
        # To ensure the filesystems that we create do not get mounted, we apply a separate 'zfs create -p -u'
        # invocation for each non-existing ancestor. This is because a single 'zfs create -p -u' applies the '-u'
        # part only to the immediate filesystem, rather than to the not-yet existing ancestors.
        p = self.params
        parent = ""
        no_mount = "-u" if p.is_program_available(zfs_version_is_at_least_2_1_0, "dst") else ""
        for component in filesystem.split("/"):
            parent += component
            if not self.dst_dataset_exists[parent]:
                cmd = p.split_args(f"{p.dst.sudo} {p.zfs_program} create -p", no_mount, parent)
                try:
                    self.run_ssh_command(p.dst, log_debug, is_dry=p.dry_run, print_stdout=True, cmd=cmd)
                except subprocess.CalledProcessError as e:
                    # ignore harmless error caused by 'zfs create' without the -u flag, or by dataset already existing
                    if (
                        "filesystem successfully created, but it may only be mounted by root" not in e.stderr
                        and "filesystem successfully created, but not mounted" not in e.stderr  # SolarisZFS
                        and "dataset already exists" not in e.stderr
                        and "filesystem already exists" not in e.stderr  # SolarisZFS?
                    ):
                        raise
                if not p.dry_run:
                    self.dst_dataset_exists[parent] = True
            parent += "/"

    def create_zfs_bookmarks(self, remote: Remote, dataset: str, snapshots: list[str]) -> None:
        """Creates bookmarks for the given snapshots, using the 'zfs bookmark' CLI."""
        # Unfortunately ZFS has no syntax yet to create multiple bookmarks in a single CLI invocation
        p = self.params

        def create_zfs_bookmark(cmd: list[str]) -> None:
            snapshot = cmd[-1]
            assert "@" in snapshot
            bookmark_cmd = cmd + [replace_prefix(snapshot, old_prefix=f"{dataset}@", new_prefix=f"{dataset}#")]
            try:
                self.run_ssh_command(remote, log_debug, is_dry=p.dry_run, print_stderr=False, cmd=bookmark_cmd)
            except subprocess.CalledProcessError as e:
                # ignore harmless zfs error caused by bookmark with the same name already existing
                if ": bookmark exists" not in e.stderr:
                    print(e.stderr, file=sys.stderr, end="")
                    raise

        if p.create_bookmarks != "none" and are_bookmarks_enabled(p, remote):
            cmd = p.split_args(f"{remote.sudo} {p.zfs_program} bookmark")
            self.run_ssh_cmd_parallel(
                remote, [(cmd, snapshots)], lambda _cmd, batch: create_zfs_bookmark(_cmd + batch), max_batch_items=1
            )

    def estimate_send_size(self, remote: Remote, dst_dataset: str, recv_resume_token: str | None, *items: str) -> int:
        """Estimates num bytes to transfer via 'zfs send'."""
        p = self.params
        if p.no_estimate_send_size or is_solaris_zfs(p, remote):
            return 0  # solaris-11.4 does not have a --parsable equivalent
        zfs_send_program_opts = ["--parsable" if opt == "-P" else opt for opt in p.curr_zfs_send_program_opts]
        zfs_send_program_opts = append_if_absent(zfs_send_program_opts, "-v", "-n", "--parsable")
        if recv_resume_token:
            zfs_send_program_opts = ["-Pnv", "-t", recv_resume_token]
            items = ()
        cmd = p.split_args(f"{remote.sudo} {p.zfs_program} send", zfs_send_program_opts, items)
        try:
            lines = self.try_ssh_command(remote, log_trace, cmd=cmd)
        except RetryableError as retryable_error:
            assert retryable_error.__cause__ is not None
            if recv_resume_token:
                e = retryable_error.__cause__
                stderr = stderr_to_str(e.stderr) if hasattr(e, "stderr") else ""
                retryable_error.no_sleep = self.clear_resumable_recv_state_if_necessary(dst_dataset, stderr)
            # op isn't idempotent so retries regather current state from the start of replicate_dataset()
            raise
        if lines is None:
            return 0  # src dataset or snapshot has been deleted by third party
        size = lines.splitlines()[-1]
        assert size.startswith("size")
        return int(size[size.index("\t") + 1 :])

    def zfs_set(self, properties: list[str], remote: Remote, dataset: str) -> None:
        """Applies the given property key=value pairs via 'zfs set' CLI to the given dataset on the given remote."""
        p = self.params
        if len(properties) == 0:
            return
        # set properties in batches without creating a command line that's too big for the OS to handle
        cmd = p.split_args(f"{remote.sudo} {p.zfs_program} set")
        self.run_ssh_cmd_batched(
            remote,
            cmd,
            properties,
            lambda batch: self.run_ssh_command(
                remote, log_debug, is_dry=p.dry_run, print_stdout=True, cmd=cmd + batch + [dataset]
            ),
            max_batch_items=1 if is_solaris_zfs(p, remote) else 2**29,  # solaris-11.4 CLI doesn't accept multiple props
        )

    def zfs_get(
        self,
        remote: Remote,
        dataset: str,
        sources: str,
        output_columns: str,
        propnames: str,
        splitlines: bool,
        props_cache: dict[tuple[str, str, str], dict[str, str | None]],
    ) -> dict[str, str | None]:
        """Returns the results of 'zfs get' CLI on the given dataset on the given remote."""
        if not propnames:
            return {}
        p = self.params
        cache_key = (sources, output_columns, propnames)
        props = props_cache.get(cache_key)
        if props is None:
            cmd = p.split_args(f"{p.zfs_program} get -Hp -o {output_columns} -s {sources} {propnames}", dataset)
            lines = self.run_ssh_command(remote, log_trace, cmd=cmd)
            is_name_value_pair = "," in output_columns
            props = {}
            # if not splitlines: omit single trailing newline that was appended by 'zfs get' CLI
            for line in lines.splitlines() if splitlines else [lines[0:-1]]:
                if is_name_value_pair:
                    propname, propvalue = line.split("\t", 1)
                    props[propname] = propvalue
                else:
                    props[line] = None
            props_cache[cache_key] = props
        return props

    def incremental_send_steps_wrapper(
        self, src_snapshots: list[str], src_guids: list[str], included_guids: set[str], is_resume: bool
    ) -> list[tuple[str, str, str, list[str]]]:
        force_convert_I_to_i = self.params.src.use_zfs_delegation and not getenv_bool(  # noqa: N806
            "no_force_convert_I_to_i", True
        )
        # force_convert_I_to_i == True implies that:
        # If using 'zfs allow' delegation mechanism, force convert 'zfs send -I' to a series of
        # 'zfs send -i' as a workaround for zfs issue https://github.com/openzfs/zfs/issues/16394
        return incremental_send_steps(src_snapshots, src_guids, included_guids, is_resume, force_convert_I_to_i)

    def add_recv_property_options(
        self, full_send: bool, recv_opts: list[str], dataset: str, cache: dict[tuple[str, str, str], dict[str, str | None]]
    ) -> tuple[list[str], list[str]]:
        """Reads the ZFS properties of the given src dataset. Appends zfs recv -o and -x values to recv_opts according to CLI
        params, and returns properties to explicitly set on the dst dataset after 'zfs receive' completes successfully."""
        p = self.params
        set_opts = []
        x_names = p.zfs_recv_x_names
        x_names_set = set(x_names)
        ox_names = p.zfs_recv_ox_names.copy()
        if p.is_program_available(zfs_version_is_at_least_2_2_0, p.dst.location):
            # workaround for https://github.com/openzfs/zfs/commit/b0269cd8ced242e66afc4fa856d62be29bb5a4ff
            # 'zfs recv -x foo' on zfs < 2.2 errors out if the 'foo' property isn't contained in the send stream
            for propname in x_names:
                recv_opts.append("-x")
                recv_opts.append(propname)
            ox_names.update(x_names)  # union
        for config in [p.zfs_recv_o_config, p.zfs_recv_x_config, p.zfs_set_config]:
            if len(config.include_regexes) == 0:
                continue  # this is the default - it's an instant noop
            if (full_send and "full" in config.targets) or (not full_send and "incremental" in config.targets):
                # 'zfs get' uses newline as record separator and tab as separator between output columns. A ZFS user property
                # may contain newline and tab characters (indeed anything). Together, this means that there is no reliable
                # way to determine where a record ends and the next record starts when listing multiple arbitrary records in
                # a single 'zfs get' call. Therefore, here we use a separate 'zfs get' call for each ZFS user property.
                # TODO: perf: on zfs >= 2.3 use json via zfs get -j to safely merge all zfs gets into one 'zfs get' call
                try:
                    props_any = self.zfs_get(p.src, dataset, config.sources, "property", "all", True, cache)
                    props_filtered = filter_properties(self, props_any, config.include_regexes, config.exclude_regexes)
                    user_propnames = [name for name in props_filtered.keys() if ":" in name]
                    sys_propnames = ",".join([name for name in props_filtered.keys() if ":" not in name])
                    props = self.zfs_get(p.src, dataset, config.sources, "property,value", sys_propnames, True, cache)
                    for propnames in user_propnames:
                        props.update(self.zfs_get(p.src, dataset, config.sources, "property,value", propnames, False, cache))
                except (subprocess.CalledProcessError, UnicodeDecodeError) as e:
                    raise RetryableError("Subprocess failed") from e
                for propname in sorted(props.keys()):
                    if config is p.zfs_recv_o_config:
                        if not (propname in ox_names or propname in x_names_set):
                            recv_opts.append("-o")
                            recv_opts.append(f"{propname}={props[propname]}")
                            ox_names.add(propname)
                    elif config is p.zfs_recv_x_config:
                        if propname not in ox_names:
                            recv_opts.append("-x")
                            recv_opts.append(propname)
                            ox_names.add(propname)
                    else:
                        set_opts.append(f"{propname}={props[propname]}")
        return recv_opts, set_opts

    @staticmethod
    def recv_option_property_names(recv_opts: list[str]) -> set[str]:
        """Extracts -o and -x property names that are already specified on the command line. This can be used to check
        for dupes because 'zfs receive' does not accept multiple -o or -x options with the same property name."""
        propnames = set()
        i = 0
        n = len(recv_opts)
        while i < n:
            stripped = recv_opts[i].strip()
            if stripped in ("-o", "-x"):
                i += 1
                if i == n or recv_opts[i].strip() in ("-o", "-x"):
                    die(f"Missing value for {stripped} option in --zfs-recv-program-opt(s): {' '.join(recv_opts)}")
                if stripped == "-o" and "=" not in recv_opts[i]:
                    die(f"Missing value for {stripped} name=value pair in --zfs-recv-program-opt(s): {' '.join(recv_opts)}")
                propname = recv_opts[i] if stripped == "-x" else recv_opts[i].split("=", 1)[0]
                validate_property_name(propname, "--zfs-recv-program-opt(s)")
                propnames.add(propname)
            i += 1
        return propnames

    def root_datasets_if_recursive_zfs_snapshot_is_possible(
        self, datasets: list[str], basis_datasets: list[str]
    ) -> list[str] | None:
        """Returns the root datasets within the (filtered) `datasets` list if no incompatible pruning is detected. A dataset
        within `datasets` is considered a root dataset if it has no parent, i.e. it is not a descendant of any dataset in
        `datasets`. Returns `None` if any (unfiltered) dataset in `basis_dataset` that is a descendant of at least one of
        the root datasets is missing in `datasets`, indicating that --include/exclude-dataset* or the snapshot schedule
        have pruned a dataset in a way that is incompatible with 'zfs snapshot -r' CLI semantics, thus requiring a switch
        to the non-recursive 'zfs snapshot snapshot1 .. snapshot N' CLI flavor.
        Assumes that set(datasets).issubset(set(basis_datasets)). Also assumes that datasets and basis_datasets are both
        sorted (and thus the output root_datasets is sorted too), which is why this algorithm is efficient - O(N) time
        complexity. The impl is akin to the merge algorithm of a merge sort, adapted to our specific use case.
        See root_datasets_if_recursive_zfs_snapshot_is_possible_slow_but_correct() in the unit test suite for an alternative
        impl that's easier to grok."""
        datasets_set: set[str] = set(datasets)
        root_datasets: list[str] = self.find_root_datasets(datasets)
        len_root_datasets = len(root_datasets)
        len_basis_datasets = len(basis_datasets)
        i, j = 0, 0
        while i < len_root_datasets and j < len_basis_datasets:  # walk and "merge" both sorted lists, in sync
            if basis_datasets[j] < root_datasets[i]:  # irrelevant subtree?
                j += 1  # move to the next basis_src_dataset
            elif is_descendant(basis_datasets[j], of_root_dataset=root_datasets[i]):  # relevant subtree?
                if basis_datasets[j] not in datasets_set:  # was dataset chopped off by schedule or --incl/exclude-dataset*?
                    return None  # detected filter pruning that is incompatible with 'zfs snapshot -r'
                j += 1  # move to the next basis_src_dataset
            else:
                i += 1  # move to next root dataset; no need to check root_datasets that are nomore (or not yet) reachable
        return root_datasets

    @staticmethod
    def find_root_datasets(sorted_datasets: list[str]) -> list[str]:
        """Returns the roots of the subtrees in the (sorted) input datasets. The output root dataset list is sorted, too.
        A dataset is a root dataset if it has no parent, i.e. it is not a descendant of any dataset in the input datasets."""
        root_datasets = []
        skip_dataset = DONT_SKIP_DATASET
        for dataset in sorted_datasets:
            if is_descendant(dataset, of_root_dataset=skip_dataset):
                continue
            skip_dataset = dataset
            root_datasets.append(dataset)
        return root_datasets

    def find_datasets_to_snapshot(self, sorted_datasets: list[str]) -> dict[SnapshotLabel, list[str]]:
        """Given a (sorted) list of source datasets, returns a dict where the key is a snapshot name (aka SnapshotLabel, e.g.
        bzfs_2024-11-06_08:30:05_hourly) and the value is the (sorted) (sub)list of datasets for which a snapshot needs to
        be created with that name, because these datasets are due per the schedule, either because the 'creation' time of
        their most recent snapshot with that name pattern is now too old, or such a snapshot does not even exist.
        The baseline implementation uses the 'zfs list -t snapshot' CLI to find the most recent snapshots, which is simple
        but doesn't scale well with the number of snapshots, at least if the goal is to take snapshots every second.
        An alternative, much more scalable, implementation queries the standard ZFS "snapshots_changed" dataset property
        (requires zfs >= 2.2.0), in combination with a local cache that stores this property, as well as the creation time
        of the most recent snapshot, for each SnapshotLabel and each dataset."""
        p, log = self.params, self.params.log
        src, config = p.src, p.create_src_snapshots_config
        datasets_to_snapshot: dict[SnapshotLabel, list[str]] = defaultdict(list)
        is_caching = False
        msgs = []

        def create_snapshot_if_latest_is_too_old(
            datasets_to_snapshot: dict[SnapshotLabel, list[str]], dataset: str, label: SnapshotLabel, creation_unixtime: int
        ) -> None:
            """Schedules creation of a snapshot for the given label if the label's existing latest snapshot is too old."""
            creation_dt = datetime.fromtimestamp(creation_unixtime, tz=config.tz)
            log.log(log_trace, "Latest snapshot creation: %s for %s", creation_dt, label)
            duration_amount, duration_unit = config.suffix_durations[label.suffix]
            next_event_dt = round_datetime_up_to_duration_multiple(
                creation_dt + timedelta(microseconds=1), duration_amount, duration_unit, config.anchors
            )
            msg = ""
            if config.current_datetime >= next_event_dt:
                datasets_to_snapshot[label].append(dataset)  # mark it as scheduled for snapshot creation
                msg = " has passed"
            msgs.append((next_event_dt, dataset, label, msg))
            if is_caching and not p.dry_run:  # update cache with latest state from 'zfs list -t snapshot'
                cache_file = self.last_modified_cache_file(src, dataset, label)
                set_last_modification_time_safe(cache_file, unixtime_in_secs=creation_unixtime, if_more_recent=True)

        labels = []
        config_labels: list[SnapshotLabel] = config.snapshot_labels()
        for label in config_labels:
            duration_amount_, duration_unit_ = config.suffix_durations[label.suffix]
            if duration_amount_ == 0 or config.create_src_snapshots_even_if_not_due:
                datasets_to_snapshot[label] = sorted_datasets  # take snapshot regardless of creation time of existing snaps
            else:
                labels.append(label)
        if len(labels) == 0:
            return datasets_to_snapshot  # nothing more TBD

        # satisfy request from local cache as much as possible
        cached_datasets_to_snapshot: dict[SnapshotLabel, list[str]] = defaultdict(list)
        if is_caching_snapshots(p, src):
            sorted_datasets_todo = []
            for dataset in sorted_datasets:
                cached_snapshots_changed: int = self.cache_get_snapshots_changed(self.last_modified_cache_file(src, dataset))
                if cached_snapshots_changed == 0:
                    sorted_datasets_todo.append(dataset)  # request cannot be answered from cache
                    continue
                if cached_snapshots_changed != self.src_properties[dataset][SNAPSHOTS_CHANGED]:  # get that prop "for free"
                    self.invalidate_last_modified_cache_dataset(dataset)
                    sorted_datasets_todo.append(dataset)  # request cannot be answered from cache
                    continue
                creation_unixtimes = []
                for label in labels:
                    creation_unixtime = self.cache_get_snapshots_changed(self.last_modified_cache_file(src, dataset, label))
                    if creation_unixtime == 0:
                        sorted_datasets_todo.append(dataset)  # request cannot be answered from cache
                        break
                    creation_unixtimes.append(creation_unixtime)
                if len(creation_unixtimes) == len(labels):
                    for j, label in enumerate(labels):
                        create_snapshot_if_latest_is_too_old(
                            cached_datasets_to_snapshot, dataset, label, creation_unixtimes[j]
                        )
            sorted_datasets = sorted_datasets_todo

        def create_snapshot_fn(i: int, creation_unixtime_secs: int, dataset: str, snapshot: str) -> None:
            create_snapshot_if_latest_is_too_old(datasets_to_snapshot, dataset, labels[i], creation_unixtime_secs)

        def on_finish_dataset(dataset: str) -> None:
            if is_caching and not p.dry_run:
                set_last_modification_time_safe(
                    self.last_modified_cache_file(src, dataset),
                    unixtime_in_secs=int(self.src_properties[dataset][SNAPSHOTS_CHANGED]),
                    if_more_recent=True,
                )

        # fallback to 'zfs list -t snapshot' for any remaining datasets, as these couldn't be satisfied from local cache
        is_caching = is_caching_snapshots(p, src)
        datasets_without_snapshots = self.handle_minmax_snapshots(
            src, sorted_datasets, labels, fn_latest=create_snapshot_fn, fn_on_finish_dataset=on_finish_dataset
        )
        for lbl in labels:  # merge (sorted) results from local cache + 'zfs list -t snapshot' into (sorted) combined result
            datasets_to_snapshot[lbl].sort()
            if datasets_without_snapshots or (lbl in cached_datasets_to_snapshot):  # +take snaps for snapshot-less datasets
                datasets_to_snapshot[lbl] = list(  # inputs to merge() are sorted, and outputs are sorted too
                    heapq.merge(datasets_to_snapshot[lbl], cached_datasets_to_snapshot[lbl], datasets_without_snapshots)
                )
        msgs.sort()
        prefx = "Next scheduled snapshot time: "
        text = "\n".join(f"{prefx}{next_event_dt} for {dataset}@{label}{msg}" for next_event_dt, dataset, label, msg in msgs)
        if len(text) > 0:
            log.info("Next scheduled snapshot times ...\n%s", text)
        # sort to ensure that we take snapshots for dailies before hourlies, and so on
        label_indexes = {label: k for k, label in enumerate(config_labels)}
        datasets_to_snapshot = dict(sorted(datasets_to_snapshot.items(), key=lambda kv: label_indexes[kv[0]]))
        return datasets_to_snapshot

    def handle_minmax_snapshots(
        self,
        remote: Remote,
        sorted_datasets: list[str],
        labels: list[SnapshotLabel],
        fn_latest: Callable[[int, int, str, str], None],  # callback function for latest snapshot
        fn_oldest: Callable[[int, int, str, str], None] | None = None,  # callback function for oldest snapshot
        fn_on_finish_dataset: Callable[[str], None] = lambda dataset: None,
    ) -> list[str]:  # thread-safe
        """For each dataset in `sorted_datasets`, for each label in `labels`, finds the latest and oldest snapshot, and runs
        the callback functions on them. Ignores the timestamp of the input labels and the timestamp of the snapshot names."""
        p = self.params
        cmd = p.split_args(f"{p.zfs_program} list -t snapshot -d 1 -Hp -o createtxg,creation,name")  # sort dataset,createtxg
        datasets_with_snapshots: set[str] = set()
        for lines in self.zfs_list_snapshots_in_parallel(remote, cmd, sorted_datasets, ordered=False):
            # streaming group by dataset name (consumes constant memory only)
            for dataset, group in itertools.groupby(lines, key=lambda line: line[line.rindex("\t") + 1 : line.index("@")]):
                snapshots = sorted(  # fetch all snapshots of current dataset and sort by createtxg,creation,name
                    (int(createtxg), int(creation_unixtime_secs), name[name.index("@") + 1 :])
                    for createtxg, creation_unixtime_secs, name in (line.split("\t", 2) for line in group)
                )
                assert len(snapshots) > 0
                datasets_with_snapshots.add(dataset)
                snapshot_names = [snapshot[-1] for snapshot in snapshots]
                year_with_4_digits_regex = year_with_four_digits_regex
                fns = ((fn_latest, True),) if fn_oldest is None else ((fn_latest, True), (fn_oldest, False))
                for i, label in enumerate(labels):
                    infix = label.infix
                    start = label.prefix + infix
                    end = label.suffix
                    startlen = len(start)
                    endlen = len(end)
                    minlen = startlen + endlen if infix else 4 + startlen + endlen  # year_with_four_digits_regex
                    year_slice = slice(startlen, startlen + 4)  # [startlen:startlen+4]  # year_with_four_digits_regex
                    for fn, is_reverse in fns:
                        creation_unixtime_secs: int = 0  # find creation time of latest or oldest snapshot matching the label
                        minmax_snapshot = ""
                        for j, s in enumerate(reversed(snapshot_names) if is_reverse else snapshot_names):
                            if (
                                s.endswith(end)
                                and s.startswith(start)
                                and len(s) >= minlen
                                and (infix or year_with_4_digits_regex.fullmatch(s[year_slice]))
                            ):
                                k = len(snapshots) - j - 1 if is_reverse else j
                                creation_unixtime_secs = snapshots[k][1]
                                minmax_snapshot = s
                                break
                        fn(i, creation_unixtime_secs, dataset, minmax_snapshot)
                fn_on_finish_dataset(dataset)

        datasets_without_snapshots = [dataset for dataset in sorted_datasets if dataset not in datasets_with_snapshots]
        return datasets_without_snapshots

    def cache_get_snapshots_changed(self, path: str) -> int:
        return self.cache_get_snapshots_changed2(path)[1]

    @staticmethod
    def cache_get_snapshots_changed2(path: str) -> tuple[int, int]:
        """Like zfs_get_snapshots_changed() but reads from local cache."""
        try:  # perf: inode metadata reads and writes are fast - ballpark O(200k) ops/sec.
            s = os_stat(path)
            return round(s.st_atime), round(s.st_mtime)
        except FileNotFoundError:
            return 0, 0  # harmless

    def last_modified_cache_file(self, remote: Remote, dataset: str, label: SnapshotLabel | None = None) -> str:
        cache_file = "=" if label is None else f"{label.prefix}{label.infix}{label.suffix}"
        userhost_dir = remote.ssh_user_host if remote.ssh_user_host else "-"
        return os_path_join(self.params.log_params.last_modified_cache_dir, userhost_dir, dataset, cache_file)

    def invalidate_last_modified_cache_dataset(self, dataset: str) -> None:
        """Resets the last_modified timestamp of all cache files of the given dataset to zero."""
        p = self.params
        cache_file = self.last_modified_cache_file(p.src, dataset)
        if not p.dry_run:
            try:
                zero_times = (0, 0)
                for entry in os.scandir(os.path.dirname(cache_file)):
                    os_utime(entry.path, times=zero_times)
                os_utime(cache_file, times=zero_times)
            except FileNotFoundError:
                pass  # harmless

    def update_last_modified_cache(self, datasets_to_snapshot: dict[SnapshotLabel, list[str]]) -> None:
        """perf: copy lastmodified time of source dataset into local cache to reduce future 'zfs list -t snapshot' calls."""
        p = self.params
        src = p.src
        if not is_caching_snapshots(p, src):
            return
        src_datasets_set: set[str] = set()
        dataset_labels: dict[str, list[SnapshotLabel]] = defaultdict(list)
        for label, datasets in datasets_to_snapshot.items():
            src_datasets_set.update(datasets)  # union
            for dataset in datasets:
                dataset_labels[dataset].append(label)

        sorted_datasets = sorted(src_datasets_set)
        snapshots_changed_dict = self.zfs_get_snapshots_changed(src, sorted_datasets)
        for src_dataset in sorted_datasets:
            snapshots_changed = snapshots_changed_dict.get(src_dataset, 0)
            self.src_properties[src_dataset][SNAPSHOTS_CHANGED] = snapshots_changed
            if snapshots_changed == 0:
                self.invalidate_last_modified_cache_dataset(src_dataset)
            else:
                cache_file = self.last_modified_cache_file(src, src_dataset)
                cache_dir = os.path.dirname(cache_file)
                if not p.dry_run:
                    try:
                        os.makedirs(cache_dir, exist_ok=True)
                        set_last_modification_time(cache_file, unixtime_in_secs=snapshots_changed, if_more_recent=True)
                        for label in dataset_labels[src_dataset]:
                            cache_file = self.last_modified_cache_file(src, src_dataset, label)
                            set_last_modification_time(cache_file, unixtime_in_secs=snapshots_changed, if_more_recent=True)
                    except FileNotFoundError:
                        pass  # harmless

    def zfs_get_snapshots_changed(self, remote: Remote, datasets: list[str]) -> dict[str, int]:
        """Returns the ZFS dataset property "snapshots_changed", which is a UTC Unix time in integer seconds.
        See https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html#snapshots_changed"""

        def try_zfs_list_command(_cmd: list[str], batch: list[str]) -> list[str]:
            try:
                return self.run_ssh_command(remote, print_stderr=False, cmd=_cmd + batch).splitlines()
            except CalledProcessError as e:
                return stderr_to_str(e.stdout).splitlines()
            except UnicodeDecodeError:
                return []

        p = self.params
        cmd = p.split_args(f"{p.zfs_program} list -t filesystem,volume -s name -Hp -o snapshots_changed,name")
        results = {}
        for lines in self.itr_ssh_cmd_parallel(
            remote, [(cmd, datasets)], lambda _cmd, batch: try_zfs_list_command(_cmd, batch), ordered=False
        ):
            for line in lines:
                if "\t" not in line:
                    break  # partial output from failing 'zfs list' command
                snapshots_changed, dataset = line.split("\t", 1)
                if not dataset:
                    break  # partial output from failing 'zfs list' command
                if snapshots_changed == "-" or not snapshots_changed:
                    snapshots_changed = "0"
                results[dataset] = int(snapshots_changed)
        return results

    @dataclass(order=True, frozen=True)
    class ComparableSnapshot:
        key: tuple[str, str]  # rel_dataset, guid
        cols: list[str] = field(compare=False)

    def run_compare_snapshot_lists(self, src_datasets: list[str], dst_datasets: list[str]) -> None:
        """Compares source and destination dataset trees recursively wrt. snapshots, for example to check if all recently
        taken snapshots have been successfully replicated by a periodic job. Lists snapshots only contained in source
        (tagged with 'src'), only contained in destination (tagged with 'dst'), and contained in both source and destination
        (tagged with 'all'), in the form of a TSV file, along with other snapshot metadata. Implemented with a time and
        space efficient streaming algorithm; easily scales to millions of datasets and any number of snapshots.
        Assumes that both src_datasets and dst_datasets are sorted."""
        p, log = self.params, self.params.log
        src, dst = p.src, p.dst
        task = src.root_dataset + " vs. " + dst.root_dataset
        tsv_dir = p.log_params.log_file[0 : -len(".log")] + ".cmp"
        os.makedirs(tsv_dir, exist_ok=True)
        tsv_file = os.path.join(tsv_dir, (src.root_dataset + "%" + dst.root_dataset).replace("/", "~") + ".tsv")
        tmp_tsv_file = tsv_file + ".tmp"
        compare_snapshot_lists = set(p.compare_snapshot_lists.split("+"))
        is_src_dst_all = all(choice in compare_snapshot_lists for choice in cmp_choices_items)
        all_src_dst = [loc for loc in ("all", "src", "dst") if loc in compare_snapshot_lists]
        is_first_row = True
        now = None

        def zfs_list_snapshot_iterator(r: Remote, sorted_datasets: list[str]) -> Generator[str, None, None]:
            """Lists snapshots sorted by dataset name. All snapshots of a given dataset will be adjacent."""
            assert not self.is_test_mode or sorted_datasets == sorted(sorted_datasets), "List is not sorted"
            written_zfs_prop = "written"  # https://openzfs.github.io/openzfs-docs/man/master/7/zfsprops.7.html#written
            if is_solaris_zfs(p, r):  # solaris-11.4 zfs does not know the "written" ZFS snapshot property
                written_zfs_prop = "type"  # for simplicity, fill in the non-integer dummy constant type="snapshot"
            props = self.creation_prefix + f"creation,guid,createtxg,{written_zfs_prop},name"
            types = "snapshot"
            if p.use_bookmark and r.location == "src" and are_bookmarks_enabled(p, r):
                types = "snapshot,bookmark"  # output list ordering: intentionally makes bookmarks appear *after* snapshots
            cmd = p.split_args(f"{p.zfs_program} list -t {types} -d 1 -Hp -o {props}")  # sorted by dataset, createtxg
            for lines in self.zfs_list_snapshots_in_parallel(r, cmd, sorted_datasets):
                yield from lines

        def snapshot_iterator(
            root_dataset: str, sorted_itr: Generator[str, None, None]
        ) -> Generator[Job.ComparableSnapshot, None, None]:
            """Splits/groups snapshot stream into distinct datasets, sorts by GUID within a dataset such that any two
            snapshots with the same GUID will lie adjacent to each other during the upcoming phase that merges
            src snapshots and dst snapshots."""
            # streaming group by dataset name (consumes constant memory only)
            for dataset, group in itertools.groupby(
                sorted_itr, key=lambda line: line[line.rindex("\t") + 1 : line.replace("#", "@").index("@")]
            ):
                snapshots = list(group)  # fetch all snapshots of current dataset, e.g. dataset=tank1/src/foo
                snapshots = filter_snapshots(self, snapshots)  # apply include/exclude policy
                snapshots.sort(key=lambda line: line.split("\t", 2)[1])  # stable sort by GUID (2nd remains createtxg)
                rel_dataset = relativize_dataset(dataset, root_dataset)  # rel_dataset=/foo, root_dataset=tank1/src
                last_guid = ""
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
                    yield Job.ComparableSnapshot(key, cols)

        def print_dataset(rel_dataset: str, entries: Iterable[tuple[str, Job.ComparableSnapshot]]) -> None:
            entries = sorted(  # fetch all snapshots of current dataset and sort em by creation, createtxg, snapshot_tag
                entries,
                key=lambda entry: (
                    int((cols := entry[1].cols)[0]),
                    int(cols[2]),
                    (snapshot_name := cols[-1])[snapshot_name.replace("#", "@").index("@") + 1 :],
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
            header = "location creation_iso createtxg rel_name guid root_dataset rel_dataset name creation written"
            nonlocal is_first_row
            if is_first_row:
                fd.write(header.replace(" ", "\t") + "\n")
                is_first_row = False
            for i, entry in enumerate(entries):
                loc = location = entry[0]
                creation, guid, createtxg, written, name = entry[1].cols
                root_dataset = dst.root_dataset if location == cmp_choices_items[1] else src.root_dataset
                rel_name = relativize_dataset(name, root_dataset)
                creation_iso = isotime_from_unixtime(int(creation))
                row = loc, creation_iso, createtxg, rel_name, guid, root_dataset, rel_dataset, name, creation, written
                # Example: src 2024-11-06_08:30:05 17435050 /foo@test_2024-11-06_08:30:05_daily 2406491805272097867 tank1/src /foo tank1/src/foo@test_2024-10-06_08:30:04_daily 1730878205 24576  # noqa: E501
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
            prefix = f"Comparing {rel_dataset}~"
            msgs = []
            msgs.append(f"{prefix} of {task}")
            msgs.append(
                f"{prefix} Q: No src snapshots are missing on dst, and no dst snapshots are missing on src, "
                "and there is a common snapshot? A: "
                + (
                    "n/a"
                    if not is_src_dst_all
                    else str(
                        stats["src"].snapshot_count == 0
                        and stats["dst"].snapshot_count == 0
                        and stats["all"].snapshot_count > 0
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
        src_snap_itr = snapshot_iterator(src.root_dataset, zfs_list_snapshot_iterator(src, src_datasets))
        dst_snap_itr = snapshot_iterator(dst.root_dataset, zfs_list_snapshot_iterator(dst, dst_datasets))
        merge_itr = self.merge_sorted_iterators(cmp_choices_items, p.compare_snapshot_lists, src_snap_itr, dst_snap_itr)

        rel_datasets: dict[str, set[str]] = defaultdict(set)
        for datasets, remote in (src_datasets, src), (dst_datasets, dst):
            for dataset in datasets:  # rel_dataset=/foo, root_dataset=tank1/src
                rel_datasets[remote.location].add(relativize_dataset(dataset, remote.root_dataset))
        rel_src_or_dst: list[str] = sorted(rel_datasets["src"].union(rel_datasets["dst"]))

        log.debug("%s", f"Temporary TSV output file comparing {task} is: {tmp_tsv_file}")
        with open_nofollow(tmp_tsv_file, "w", encoding="utf-8", perm=FILE_PERMISSIONS) as fd:
            # streaming group by rel_dataset (consumes constant memory only); entry is a Tuple[str, ComparableSnapshot]
            group = itertools.groupby(merge_itr, key=lambda entry: entry[1].key[0])
            self.print_datasets(group, lambda rel_ds, entries: print_dataset(rel_ds, entries), rel_src_or_dst)
        os.rename(tmp_tsv_file, tsv_file)
        log.info("%s", f"Final TSV output file comparing {task} is: {tsv_file}")

        tsv_file = tsv_file[0 : tsv_file.rindex(".")] + ".rel_datasets_tsv"
        tmp_tsv_file = tsv_file + ".tmp"
        with open_nofollow(tmp_tsv_file, "w", encoding="utf-8", perm=FILE_PERMISSIONS) as fd:
            header = "location rel_dataset src_dataset dst_dataset"
            fd.write(header.replace(" ", "\t") + "\n")
            src_only: set[str] = rel_datasets["src"].difference(rel_datasets["dst"])
            dst_only: set[str] = rel_datasets["dst"].difference(rel_datasets["src"])
            for rel_dataset in rel_src_or_dst:
                loc = "src" if rel_dataset in src_only else "dst" if rel_dataset in dst_only else "all"
                src_dataset = src.root_dataset + rel_dataset if rel_dataset not in dst_only else ""
                dst_dataset = dst.root_dataset + rel_dataset if rel_dataset not in src_only else ""
                row = loc, rel_dataset, src_dataset, dst_dataset  # Example: all /foo/bar tank1/src/foo/bar tank2/dst/foo/bar
                if not p.dry_run:
                    fd.write("\t".join(row) + "\n")
        os.rename(tmp_tsv_file, tsv_file)

    @staticmethod
    def print_datasets(group: itertools.groupby, fn: Callable[[str, Iterable], None], rel_datasets: Iterable[str]) -> None:
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

    def merge_sorted_iterators(
        self,
        choices: Sequence[str],  # ["src", "dst", "all"]
        choice: str,  # Example: "src+dst+all"
        src_itr: Iterator,
        dst_itr: Iterator,
    ) -> Generator[tuple[Any, ...], None, None]:
        """This is the typical merge algorithm of a merge sort, slightly adapted to our specific use case."""
        assert len(choices) == 3
        assert choice
        flags = 0
        for i, item in enumerate(choices):
            if item in choice:
                flags |= 1 << i
        src_next, dst_next = run_in_parallel(lambda: next(src_itr, None), lambda: next(dst_itr, None))
        while not (src_next is None and dst_next is None):
            if src_next == dst_next:
                n = 2
                if (flags & (1 << n)) != 0:
                    yield choices[n], src_next, dst_next
                src_next = next(src_itr, None)
                dst_next = next(dst_itr, None)
            elif src_next is None or (dst_next is not None and dst_next < src_next):
                n = 1
                if (flags & (1 << n)) != 0:
                    yield choices[n], dst_next
                dst_next = next(dst_itr, None)
            else:
                n = 0
                if (flags & (1 << n)) != 0:
                    if isinstance(src_next, Job.ComparableSnapshot):
                        name = src_next.cols[-1]
                        if "@" in name:
                            yield choices[n], src_next  # include snapshot
                        else:  # ignore src bookmarks for which no snapshot exists in dst; those aren't useful
                            assert "#" in name
                    else:
                        yield choices[n], src_next
                src_next = next(src_itr, None)

    def check_zfs_dataset_busy(self, remote: Remote, dataset: str, busy_if_send: bool = True) -> bool:
        """Decline to start a state changing ZFS operation that is, although harmless, likely to collide with other
        currently running processes. Instead, retry the operation later, after some delay. For example, decline to
        start a 'zfs receive' into a destination dataset if another process is already running another 'zfs receive'
        into the same destination dataset, as ZFS would reject any such attempt. However, it's actually fine to run an
        incremental 'zfs receive' into a dataset in parallel with a 'zfs send' out of the very same dataset. This also
        helps daisy chain use cases where A replicates to B, and B replicates to C.

        check_zfs_dataset_busy() offers no guarantees, it merely proactively avoids likely collisions. In other words,
        even if the process check below passes there is no guarantee that the destination dataset won't be busy by the
        time we actually execute the 'zfs send' operation. In such an event ZFS will reject the operation, we'll detect
        that, and we'll simply retry, after some delay. check_zfs_dataset_busy() can be disabled via --ps-program=-.

        TLDR: As is common for long-running operations in distributed systems, we use coordination-free optimistic
        concurrency control where the parties simply retry on collision detection (rather than coordinate concurrency
        via a remote lock server)."""
        p, log = self.params, self.params.log
        if not p.is_program_available("ps", remote.location):
            return True
        cmd = p.split_args(f"{p.ps_program} -Ao args")
        procs = (self.try_ssh_command(remote, log_trace, cmd=cmd) or "").splitlines()
        if self.inject_params.get("is_zfs_dataset_busy", False):
            procs += ["sudo -n zfs receive -u -o foo:bar=/baz " + dataset]  # for unit testing only
        if not self.is_zfs_dataset_busy(procs, dataset, busy_if_send=busy_if_send):
            return True
        op = "zfs {receive" + ("|send" if busy_if_send else "") + "} operation"
        try:
            die(f"Cannot continue now: Destination is already busy with {op} from another process: {dataset}")
        except SystemExit as e:
            log.warning("%s", e)
            raise RetryableError("dst currently busy with zfs mutation op") from e

    zfs_dataset_busy_prefix = r"(([^ ]*?/)?(sudo|doas)( +-n)? +)?([^ ]*?/)?zfs (receive|recv"
    zfs_dataset_busy_if_mods = re.compile((zfs_dataset_busy_prefix + ") .*").replace("(", "(?:"))
    zfs_dataset_busy_if_send = re.compile((zfs_dataset_busy_prefix + "|send) .*").replace("(", "(?:"))

    @staticmethod
    def is_zfs_dataset_busy(procs: list[str], dataset: str, busy_if_send: bool) -> bool:
        regex = Job.zfs_dataset_busy_if_send if busy_if_send else Job.zfs_dataset_busy_if_mods
        suffix = " " + dataset
        infix = " " + dataset + "@"
        return any((proc.endswith(suffix) or infix in proc) and regex.fullmatch(proc) for proc in procs)

    def run_ssh_cmd_batched(
        self,
        r: Remote,
        cmd: list[str],
        cmd_args: list[str],
        fn: Callable[[list[str]], Any],
        max_batch_items: int = 2**29,
        sep: str = " ",
    ) -> None:
        drain(self.itr_ssh_cmd_batched(r, cmd, cmd_args, fn, max_batch_items=max_batch_items, sep=sep))

    def itr_ssh_cmd_batched(
        self,
        r: Remote,
        cmd: list[str],
        cmd_args: list[str],
        fn: Callable[[list[str]], Any],
        max_batch_items: int = 2**29,
        sep: str = " ",
    ) -> Generator[Any, None, None]:
        """Runs fn(cmd_args) in batches w/ cmd, without creating a command line that's too big for the OS to handle."""
        max_bytes = min(self.get_max_command_line_bytes("local"), self.get_max_command_line_bytes(r.location))
        assert isinstance(sep, str)
        # Max size of a single argument is 128KB on Linux - https://lists.gnu.org/archive/html/bug-bash/2020-09/msg00095.html
        max_bytes = max_bytes if sep == " " else min(max_bytes, 131071)  # e.g. 'zfs destroy foo@s1,s2,...,sN'
        fsenc = sys.getfilesystemencoding()
        seplen = len(sep.encode(fsenc))
        conn_pool: ConnectionPool = self.params.connection_pools[r.location].pool(SHARED)
        conn: Connection = conn_pool.get_connection()
        cmd = conn.ssh_cmd + cmd
        conn_pool.return_connection(conn)
        header_bytes: int = len(" ".join(cmd).encode(fsenc))
        batch: list[str] = []
        total_bytes: int = header_bytes
        max_items = max_batch_items

        def flush() -> Any:
            if len(batch) > 0:
                return fn(batch)
            return None

        for cmd_arg in cmd_args:
            curr_bytes = seplen + len(cmd_arg.encode(fsenc))
            if total_bytes + curr_bytes > max_bytes or max_items <= 0:
                results = flush()
                if results is not None:
                    yield results
                batch, total_bytes, max_items = [], header_bytes, max_batch_items
            batch.append(cmd_arg)
            total_bytes += curr_bytes
            max_items -= 1
        results = flush()
        if results is not None:
            yield results

    def run_ssh_cmd_parallel(
        self,
        r: Remote,
        cmd_args_list: list[tuple[list[str], list[str]]],
        fn: Callable[[list[str], list[str]], Any],
        max_batch_items: int = 2**29,
    ) -> None:
        drain(self.itr_ssh_cmd_parallel(r, cmd_args_list, fn=fn, max_batch_items=max_batch_items, ordered=False))

    def itr_ssh_cmd_parallel(
        self,
        r: Remote,
        cmd_args_list: list[tuple[list[str], list[str]]],
        fn: Callable[[list[str], list[str]], Any],
        max_batch_items: int = 2**29,
        ordered: bool = True,
    ) -> Generator[Any, None, Any]:
        """Returns output datasets in the same order as the input datasets (not in random order) if ordered == True."""
        return parallel_iterator(
            iterator_builder=lambda executor: [
                self.itr_ssh_cmd_batched(
                    r, cmd, cmd_args, lambda batch, cmd=cmd: executor.submit(fn, cmd, batch), max_batch_items=max_batch_items  # type: ignore[misc]
                )
                for cmd, cmd_args in cmd_args_list
            ],
            max_workers=self.max_workers[r.location],
            ordered=ordered,
        )

    def zfs_list_snapshots_in_parallel(
        self, r: Remote, cmd: list[str], datasets: list[str], ordered: bool = True
    ) -> Generator[Any, None, Any]:
        """Runs 'zfs list -t snapshot' on multiple datasets at the same time."""
        max_workers = self.max_workers[r.location]
        return self.itr_ssh_cmd_parallel(
            r,
            [(cmd, datasets)],
            fn=lambda cmd, batch: (self.try_ssh_command(r, log_trace, cmd=cmd + batch) or "").splitlines(),
            max_batch_items=min(
                self.max_datasets_per_minibatch_on_list_snaps[r.location],
                max(
                    len(datasets) // (max_workers if r.ssh_user_host else max_workers * 8),
                    max_workers if r.ssh_user_host else 1,
                ),
            ),
            ordered=ordered,
        )

    def get_max_command_line_bytes(self, location: str, os_name: str | None = None) -> int:
        """Remote flavor of os.sysconf("SC_ARG_MAX") - size(os.environb) - safety margin"""
        os_name = os_name if os_name else self.params.available_programs[location].get("os")
        if os_name == "Linux":
            arg_max = 2 * 1024 * 1024
        elif os_name == "FreeBSD":
            arg_max = 256 * 1024
        elif os_name == "SunOS":
            arg_max = 1 * 1024 * 1024
        elif os_name == "Darwin":
            arg_max = 1 * 1024 * 1024
        elif os_name == "Windows":
            arg_max = 32 * 1024
        else:
            arg_max = 256 * 1024  # unknown

        environ_size = 4 * 1024  # typically is 1-4 KB
        safety_margin = (8 * 2 * 4 + 4) * 1024 if arg_max >= 1 * 1024 * 1024 else 8 * 1024
        max_bytes = max(4 * 1024, arg_max - environ_size - safety_margin)
        if self.max_command_line_bytes is not None:
            return self.max_command_line_bytes  # for testing only
        else:
            return max_bytes


#############################################################################
def fix_send_recv_opts(
    opts: list[str],
    exclude_long_opts: set[str],
    exclude_short_opts: str,
    include_arg_opts: set[str],
    exclude_arg_opts: frozenset[str] = frozenset(),
    preserve_properties: frozenset[str] = frozenset(),
) -> tuple[list[str], list[str]]:
    """These opts are instead managed via bzfs CLI args --dryrun, etc."""
    assert "-" not in exclude_short_opts
    results = []
    x_names = set(preserve_properties)
    i = 0
    n = len(opts)
    while i < n:
        opt = opts[i]
        i += 1
        if opt in exclude_arg_opts:  # example: {"-X", "--exclude"}
            i += 1
            continue
        elif opt in include_arg_opts:  # example: {"-o", "-x"}
            results.append(opt)
            if i < n:
                if opt == "-o" and "=" in opts[i] and opts[i].split("=", 1)[0] in preserve_properties:
                    die(f"--preserve-properties: Disallowed ZFS property found in --zfs-recv-program-opt(s): -o {opts[i]}")
                if opt == "-x":
                    x_names.discard(opts[i])
                results.append(opts[i])
                i += 1
        elif opt not in exclude_long_opts:  # example: {"--dryrun", "--verbose"}
            if opt.startswith("-") and opt != "-" and not opt.startswith("--"):
                for char in exclude_short_opts:  # example: "den"
                    opt = opt.replace(char, "")
                if opt == "-":
                    continue
            results.append(opt)
    return results, sorted(x_names)


def fix_solaris_raw_mode(lst: list[str]) -> list[str]:
    lst = ["-w" if opt == "--raw" else opt for opt in lst]
    lst = ["compress" if opt == "--compressed" else opt for opt in lst]
    i = lst.index("-w") if "-w" in lst else -1
    if i >= 0:
        i += 1
        if i == len(lst) or (lst[i] != "none" and lst[i] != "compress" and lst[i] != "crypto"):
            lst.insert(i, "none")
    return lst


ssh_master_domain_socket_file_pid_regex = re.compile(r"^[0-9]+")  # see socket_name in local_ssh_command()


def delete_stale_files(
    root_dir: str,
    prefix: str,
    millis: int = 60 * 60 * 1000,
    dirs: bool = False,
    exclude: str | None = None,
    ssh: bool = False,
) -> None:
    """Cleans up obsolete files. For example caused by abnormal termination, OS crash."""
    seconds = millis / 1000
    now = time.time()
    validate_is_not_a_symlink("", root_dir)
    for entry in os.scandir(root_dir):
        if entry.name == exclude or not entry.name.startswith(prefix):
            continue
        try:
            if ((dirs and entry.is_dir()) or (not dirs and not entry.is_dir())) and now - entry.stat().st_mtime >= seconds:
                if dirs:
                    shutil.rmtree(entry.path, ignore_errors=True)
                elif not (ssh and stat.S_ISSOCK(entry.stat().st_mode)):
                    os.remove(entry.path)
                elif match := ssh_master_domain_socket_file_pid_regex.match(entry.name[len(prefix) :]):
                    pid = int(match.group(0))
                    if pid_exists(pid) is False or now - entry.stat().st_mtime >= 31 * 24 * 60 * 60:
                        os.remove(entry.path)  # bzfs process is nomore alive hence its ssh master process isn't alive either
        except FileNotFoundError:
            pass  # harmless


def has_siblings(sorted_datasets: list[str]) -> bool:
    """Returns whether the (sorted) list of input datasets contains any siblings."""
    skip_dataset = DONT_SKIP_DATASET
    parents: set[str] = set()
    for dataset in sorted_datasets:
        assert dataset
        parent = os.path.dirname(dataset)
        if parent in parents:
            return True  # I have a sibling if my parent already has another child
        parents.add(parent)
        if is_descendant(dataset, of_root_dataset=skip_dataset):
            continue
        if skip_dataset != DONT_SKIP_DATASET:
            return True  # I have a sibling if I am a root dataset and another root dataset already exists
        skip_dataset = dataset
    return False


TAPPEND = TypeVar("TAPPEND")


def xappend(lst: list[TAPPEND], *items: TAPPEND | Iterable[TAPPEND]) -> list[TAPPEND]:
    """Appends each of the items to the given list if the item is "truthy", e.g. not None and not an empty string.
    If an item is an iterable does so recursively, flattening the output."""
    for item in items:
        if isinstance(item, str) or not isinstance(item, collections.abc.Iterable):
            if item:
                lst.append(cast(TAPPEND, item))
        else:
            xappend(lst, *item)
    return lst


def parse_duration_to_milliseconds(duration: str, regex_suffix: str = "", context: str = "") -> int:
    unit_milliseconds = {
        "milliseconds": 1,
        "millis": 1,
        "seconds": 1000,
        "secs": 1000,
        "minutes": 60 * 1000,
        "mins": 60 * 1000,
        "hours": 60 * 60 * 1000,
        "days": 86400 * 1000,
        "weeks": 7 * 86400 * 1000,
        "months": round(30.5 * 86400 * 1000),
        "years": 365 * 86400 * 1000,
    }
    match = re.fullmatch(
        r"(\d+)\s*(milliseconds|millis|seconds|secs|minutes|mins|hours|days|weeks|months|years)" + regex_suffix, duration
    )
    if not match:
        if context:
            die(f"Invalid duration format: {duration} within {context}")
        else:
            raise ValueError(f"Invalid duration format: {duration}")
    assert match
    quantity = int(match.group(1))
    unit = match.group(2)
    return quantity * unit_milliseconds[unit]


def create_symlink(src: str, dst_dir: str, dst: str) -> None:
    rel_path = os.path.relpath(src, start=dst_dir)
    os.symlink(src=rel_path, dst=os.path.join(dst_dir, dst))


def append_if_absent(lst: list[TAPPEND], *items: TAPPEND) -> list[TAPPEND]:
    for item in items:
        if item not in lst:
            lst.append(item)
    return lst


def set_last_modification_time_safe(
    path: str,
    unixtime_in_secs: int | tuple[int, int],
    if_more_recent: bool = False,
) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        set_last_modification_time(path, unixtime_in_secs=unixtime_in_secs, if_more_recent=if_more_recent)
    except FileNotFoundError:
        pass  # harmless


def set_last_modification_time(
    path: str,
    unixtime_in_secs: int | tuple[int, int],
    if_more_recent: bool = False,
) -> None:
    """if_more_recent=True is a concurrency control mechanism that prevents us from overwriting a newer (monotonically
    increasing) snapshots_changed value (which is a UTC Unix time in integer seconds) that might have been written to the
    cache file by a different, more up-to-date bzfs process."""
    unixtime_in_secs = (unixtime_in_secs, unixtime_in_secs) if isinstance(unixtime_in_secs, int) else unixtime_in_secs
    if not os_path_exists(path):
        with open(path, "ab"):
            pass
    elif if_more_recent and unixtime_in_secs[1] <= round(os_stat(path).st_mtime):
        return
    os_utime(path, times=unixtime_in_secs)


def nprefix(s: str) -> str:
    return sys.intern(s + "_")


def ninfix(s: str) -> str:
    return sys.intern(s + "_") if s else ""


def nsuffix(s: str) -> str:
    return sys.intern("_" + s) if s else ""


def format_dict(dictionary: dict[Any, Any]) -> str:
    return f'"{dictionary}"'


def unixtime_fromisoformat(datetime_str: str) -> int:
    """Converts an ISO 8601 datetime string into a UTC Unix time in integer seconds. If the datetime string does not
    contain time zone info then it is assumed to be in the local time zone."""
    return int(datetime.fromisoformat(datetime_str).timestamp())


def isotime_from_unixtime(unixtime_in_seconds: int) -> str:
    """Converts a UTC Unix time in integer seconds into an ISO 8601 datetime string in the local time zone.
    Example: 2024-09-03_12:26:15"""
    tz: tzinfo = timezone.utc  # outputs time in UTC
    # tz = None  # outputs time in local time zone
    dt = datetime.fromtimestamp(unixtime_in_seconds, tz=tz)
    return dt.isoformat(sep="_", timespec="seconds")


def current_datetime(
    tz_spec: str | None = None,
    now_fn: Callable[[tzinfo | None], datetime] | None = None,
) -> datetime:
    """Returns a datetime that is the current time in the given timezone, or in the local timezone if tz_spec is absent."""
    if now_fn is None:
        now_fn = datetime.now
    return now_fn(get_timezone(tz_spec))


def get_timezone(tz_spec: str | None = None) -> tzinfo | None:
    """Returns the given timezone, or the local timezone if the timezone spec is absent. The optional timezone spec is of
    the form "UTC" or "+HH:MM" or "-HH:MM" for fixed UTC offsets."""
    tz: tzinfo | None
    if tz_spec is None:
        tz = None  # i.e. local timezone
    elif tz_spec == "UTC":
        tz = timezone.utc
    else:
        if match := re.fullmatch(r"([+-])(\d\d):?(\d\d)", tz_spec):
            sign, hours, minutes = match.groups()
            offset = int(hours) * 60 + int(minutes)
            offset = -offset if sign == "-" else offset
            tz = timezone(timedelta(minutes=offset))
        elif "/" in tz_spec and sys.version_info >= (3, 9):
            from zoneinfo import ZoneInfo  # requires python >= 3.9

            tz = ZoneInfo(tz_spec)  # Standard IANA timezone. Example: "Europe/Vienna"
        else:
            raise ValueError(f"Invalid timezone specification: {tz_spec}")
    return tz


def parse_dataset_locator(
    input_text: str, validate: bool = True, user: str | None = None, host: str | None = None, port: int | None = None
) -> tuple[str, str, str, str, str]:
    def convert_ipv6(hostname: str) -> str:  # support IPv6 without getting confused by host:dataset colon separator ...
        return hostname.replace("|", ":")  # ... and any colons that may be part of a (valid) ZFS dataset name

    user_undefined = user is None
    if user is None:
        user = ""
    host_undefined = host is None
    if host is None:
        host = ""
    host = convert_ipv6(host)
    user_host, dataset, pool = "", "", ""

    # Input format is [[user@]host:]dataset
    #                          1234         5          6
    if match := re.fullmatch(r"(((([^@]*)@)?([^:]+)):)?(.*)", input_text, re.DOTALL):
        if user_undefined:
            user = match.group(4) or ""
        if host_undefined:
            host = match.group(5) or ""
            host = convert_ipv6(host)
        if host == "-":
            host = ""
        dataset = match.group(6) or ""
        i = dataset.find("/")
        pool = dataset[0:i] if i >= 0 else dataset

        if user and host:
            user_host = f"{user}@{host}"
        elif host:
            user_host = host

    if validate:
        validate_user_name(user, input_text)
        validate_host_name(host, input_text)
        if port is not None:
            validate_port(port, f"Invalid port number: '{port}' for: '{input_text}' - ")
        validate_dataset_name(dataset, input_text)

    return user, host, user_host, pool, dataset


def validate_dataset_name(dataset: str, input_text: str) -> None:
    """'zfs create' CLI does not accept dataset names that are empty or start or end in a slash, etc."""
    # Also see https://github.com/openzfs/zfs/issues/439#issuecomment-2784424
    # and https://github.com/openzfs/zfs/issues/8798
    # and (by now nomore accurate): https://docs.oracle.com/cd/E26505_01/html/E37384/gbcpt.html
    if (
        dataset in ("", ".", "..")
        or any(dataset.startswith(prefix) for prefix in ("/", "./", "../"))
        or any(dataset.endswith(suffix) for suffix in ("/", "/.", "/.."))
        or any(substring in dataset for substring in ("//", "/./", "/../"))
        or any(char in SHELL_CHARS or (char.isspace() and char != " ") for char in dataset)
        or not dataset[0].isalpha()
    ):
        die(f"Invalid ZFS dataset name: '{dataset}' for: '{input_text}'")


def validate_property_name(propname: str, input_text: str) -> str:
    invalid_chars = SHELL_CHARS
    if not propname or any(c.isspace() or c in invalid_chars for c in propname):
        die(f"Invalid ZFS property name: '{propname}' for: '{input_text}'")
    return propname


def validate_user_name(user: str, input_text: str) -> None:
    invalid_chars = SHELL_CHARS + "/"
    if user and (".." in user or any(c.isspace() or c in invalid_chars for c in user)):
        die(f"Invalid user name: '{user}' for: '{input_text}'")


def validate_host_name(host: str, input_text: str, extra_invalid_chars: str = "") -> None:
    invalid_chars = SHELL_CHARS + "/" + extra_invalid_chars
    if host and (host.startswith("-") or ".." in host or any(c.isspace() or c in invalid_chars for c in host)):
        die(f"Invalid host name: '{host}' for: '{input_text}'")


def validate_port(port: str | int, message: str) -> None:
    if isinstance(port, int):
        port = str(port)
    if port and not port.isdigit():
        die(message + f"must be empty or a positive integer: '{port}'")


def validate_is_not_a_symlink(msg: str, path: str, parser: argparse.ArgumentParser | None = None) -> None:
    if os.path.islink(path):
        die(f"{msg}must not be a symlink: {path}", parser=parser)


def validate_default_shell(path_to_default_shell: str, r: Remote) -> None:
    if path_to_default_shell.endswith(("/csh", "/tcsh")):
        # On some old FreeBSD systems the default shell is still csh. Also see https://www.grymoire.com/unix/CshTop10.txt
        die(
            f"Cowardly refusing to proceed because {prog_name} is not compatible with csh-style quoting of special "
            f"characters. The safe workaround is to first manually set 'sh' instead of '{path_to_default_shell}' as "
            f"the default shell of the Unix user on {r.location} host: {r.ssh_user_host or 'localhost'}, like so: "
            "chsh -s /bin/sh YOURUSERNAME"
        )


def validate_no_argument_file(
    path: str, namespace: argparse.Namespace, err_prefix: str, parser: argparse.ArgumentParser | None = None
) -> None:
    if getattr(namespace, "no_argument_file", False):
        die(f"{err_prefix}Argument file inclusion is disabled: {path}", parser=parser)


#############################################################################
class NonEmptyStringAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        values = values.strip()
        if values == "":
            parser.error(f"{option_string}: Empty string is not valid")
        setattr(namespace, self.dest, values)


#############################################################################
class DatasetPairsAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        datasets = []
        err_prefix = f"{option_string or self.dest}: "
        for value in values:
            if not value.startswith("+"):
                datasets.append(value)
            else:
                path = value[1:]
                validate_no_argument_file(path, namespace, err_prefix=err_prefix, parser=parser)
                if "bzfs_argument_file" not in os.path.basename(path):
                    parser.error(f"{err_prefix}basename must contain substring 'bzfs_argument_file': {path}")
                try:
                    with open_nofollow(path, "r", encoding="utf-8") as fd:
                        for i, line in enumerate(fd.read().splitlines()):
                            if line.startswith("#") or not line.strip():
                                continue  # skip comment lines and empty lines
                            splits = line.split("\t", 1)
                            if len(splits) <= 1:
                                parser.error(f"{err_prefix}Line must contain tab-separated SRC_DATASET and DST_DATASET: {i}")
                            src_root_dataset, dst_root_dataset = splits
                            if not src_root_dataset.strip() or not dst_root_dataset.strip():
                                parser.error(
                                    f"{err_prefix}SRC_DATASET and DST_DATASET must not be empty or whitespace-only: {i}"
                                )
                            datasets.append(src_root_dataset)
                            datasets.append(dst_root_dataset)
                except OSError as e:
                    parser.error(f"{err_prefix}{e}")

        if len(datasets) % 2 != 0:
            parser.error(f"{err_prefix}Each SRC_DATASET must have a corresponding DST_DATASET: {datasets}")
        root_dataset_pairs = [(datasets[i], datasets[i + 1]) for i in range(0, len(datasets), 2)]
        setattr(namespace, self.dest, root_dataset_pairs)


#############################################################################
class SSHConfigFileNameAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        values = values.strip()
        if values == "":
            parser.error(f"{option_string}: Empty string is not valid")
        if any(char in SHELL_CHARS or char.isspace() for char in values):
            parser.error(f"{option_string}: Invalid file name '{values}': must not contain whitespace or special chars.")
        setattr(namespace, self.dest, values)


#############################################################################
class SafeFileNameAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        if ".." in values or "/" in values or "\\" in values:
            parser.error(f"{option_string}: Invalid file name '{values}': must not contain '..' or '/' or '\\'.")
        if any(char.isspace() and char != " " for char in values):
            parser.error(f"{option_string}: Invalid file name '{values}': must not contain whitespace other than space.")
        setattr(namespace, self.dest, values)


#############################################################################
class SafeDirectoryNameAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        values = values.strip()
        if values == "":
            parser.error(f"{option_string}: Empty string is not valid")
        if any(char.isspace() and char != " " for char in values):
            parser.error(f"{option_string}: Invalid dir name '{values}': must not contain whitespace other than space.")
        setattr(namespace, self.dest, values)


#############################################################################
class NewSnapshotFilterGroupAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, args: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        if not hasattr(args, snapshot_filters_var):
            args.snapshot_filters_var = [[]]
        elif len(args.snapshot_filters_var[-1]) > 0:
            args.snapshot_filters_var.append([])


#############################################################################
class FileOrLiteralAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        current_values = getattr(namespace, self.dest, None)
        if current_values is None:
            current_values = []
        extra_values = []
        err_prefix = f"{option_string or self.dest}: "
        for value in values:
            if not value.startswith("+"):
                extra_values.append(value)
            else:
                path = value[1:]
                validate_no_argument_file(path, namespace, err_prefix=err_prefix, parser=parser)
                if "bzfs_argument_file" not in os.path.basename(path):
                    parser.error(f"{err_prefix}basename must contain substring 'bzfs_argument_file': {path}")
                try:
                    with open_nofollow(path, "r", encoding="utf-8") as fd:
                        for line in fd.read().splitlines():
                            if line.startswith("#") or not line.strip():
                                continue  # skip comment lines and empty lines
                            extra_values.append(line)
                except OSError as e:
                    parser.error(f"{err_prefix}{e}")
        current_values += extra_values
        setattr(namespace, self.dest, current_values)
        if self.dest in snapshot_regex_filter_names:
            add_snapshot_filter(namespace, SnapshotFilter(self.dest, None, extra_values))


#############################################################################
class IncludeSnapshotPlanAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        opts = getattr(namespace, self.dest, None)
        opts = [] if opts is None else opts
        # The bzfs_include_snapshot_plan_excludes_outdated_snapshots env var flag is a work-around for (rare) replication
        # situations where a common snapshot cannot otherwise be found because bookmarks are disabled and a common
        # snapshot is actually available but not included by the --include-snapshot-plan policy chosen by the user, and the
        # user cannot change the content of the --include-snapshot-plan for some reason. The flag makes replication work even
        # in this scenario, at the expense of including (and thus replicating) old snapshots that will immediately be deleted
        # on the destination by the next pruning action. In a proper production setup, it should never be necessary to set
        # the flag to 'False'.
        include_snapshot_times_and_ranks = getenv_bool("include_snapshot_plan_excludes_outdated_snapshots", True)
        if not self._add_opts(opts, include_snapshot_times_and_ranks, parser, values, option_string=option_string):
            opts += ["--new-snapshot-filter-group", "--include-snapshot-regex=!.*"]
        setattr(namespace, self.dest, opts)

    def _add_opts(
        self,
        opts: list[str],
        include_snapshot_times_and_ranks: bool,
        parser: argparse.ArgumentParser,
        values: str,
        option_string: str | None = None,
    ) -> bool:
        """Generates extra options to be parsed later during second parse_args() pass, within run_main()"""
        xperiods = SnapshotPeriods()
        has_at_least_one_filter_clause = False
        for org, target_periods in ast.literal_eval(values).items():
            prefix = re.escape(nprefix(org))
            for target, periods in target_periods.items():
                infix = re.escape(ninfix(target)) if target else year_with_four_digits_regex.pattern  # disambiguate
                for period_unit, period_amount in periods.items():  # e.g. period_unit can be "10minutely" or "minutely"
                    if not isinstance(period_amount, int) or period_amount < 0:
                        parser.error(f"{option_string}: Period amount must be a non-negative integer: {period_amount}")
                    suffix = re.escape(nsuffix(period_unit))
                    regex = f"{prefix}{infix}.*{suffix}"
                    opts += ["--new-snapshot-filter-group", f"--include-snapshot-regex={regex}"]
                    if include_snapshot_times_and_ranks:
                        duration_amount, duration_unit = xperiods.suffix_to_duration0(period_unit)  # --> 10, "minutely"
                        duration_unit_label = xperiods.period_labels.get(duration_unit)  # duration_unit_label = "minutes"
                        opts += [
                            "--include-snapshot-times-and-ranks",
                            (
                                "notime"
                                if duration_unit_label is None or duration_amount * period_amount == 0
                                else f"{duration_amount * period_amount}{duration_unit_label}ago..anytime"
                            ),
                            f"latest{period_amount}",
                        ]
                    has_at_least_one_filter_clause = True
        return has_at_least_one_filter_clause


#############################################################################
class DeleteDstSnapshotsExceptPlanAction(IncludeSnapshotPlanAction):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        opts = getattr(namespace, self.dest, None)
        opts = [] if opts is None else opts
        opts += ["--delete-dst-snapshots-except"]
        if not self._add_opts(opts, True, parser, values, option_string=option_string):
            parser.error(
                f"{option_string}: Cowardly refusing to delete all snapshots on "
                f"--delete-dst-snapshots-except-plan='{values}' (which means 'retain no snapshots' aka "
                "'delete all snapshots'). Assuming this is an unintended pilot error rather than intended carnage. "
                "Aborting. If this is really what is intended, use `--delete-dst-snapshots --include-snapshot-regex=.*` "
                "instead to force the deletion."
            )
        setattr(namespace, self.dest, opts)


#############################################################################
class TimeRangeAndRankRangeAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        def parse_time(time_spec: str) -> int | timedelta | None:
            time_spec = time_spec.strip()
            if time_spec == "*" or time_spec == "anytime":
                return None
            if time_spec.isdigit():
                return int(time_spec)  # Input is a Unix time in integer seconds
            try:
                return timedelta(milliseconds=parse_duration_to_milliseconds(time_spec, regex_suffix=r"\s*ago"))
            except ValueError:
                try:  # If it's not a duration, try parsing as an ISO 8601 datetime
                    return unixtime_fromisoformat(time_spec)
                except ValueError:
                    parser.error(f"{option_string}: Invalid duration, Unix time, or ISO 8601 datetime: {time_spec}")

        assert isinstance(values, list)
        assert len(values) > 0
        value = values[0].strip()
        if value == "notime":
            value = "0..0"
        if ".." not in value:
            parser.error(f"{option_string}: Invalid time range: Missing '..' separator: {value}")
        timerange_specs = [parse_time(time_spec) for time_spec in value.split("..", 1)]
        rankranges = self.parse_rankranges(parser, values[1:], option_string=option_string)
        setattr(namespace, self.dest, [timerange_specs] + rankranges)  # for testing only
        timerange = self.get_include_snapshot_times(timerange_specs)
        add_time_and_rank_snapshot_filter(namespace, self.dest, timerange, rankranges)

    @staticmethod
    def get_include_snapshot_times(times: list[timedelta | int | None]) -> UnixTimeRange:
        def utc_unix_time_in_seconds(time_spec: timedelta | int | None, default: int) -> timedelta | int:
            if isinstance(time_spec, timedelta):
                return time_spec
            if isinstance(time_spec, int):
                return int(time_spec)
            return default

        lo, hi = times
        if lo is None and hi is None:
            return None
        lo = utc_unix_time_in_seconds(lo, default=0)
        hi = utc_unix_time_in_seconds(hi, default=unixtime_infinity_secs)
        if isinstance(lo, int) and isinstance(hi, int):
            return (lo, hi) if lo <= hi else (hi, lo)
        return lo, hi

    @staticmethod
    def parse_rankranges(parser: argparse.ArgumentParser, values: Any, option_string: str | None = None) -> list[RankRange]:
        def parse_rank(spec: str) -> tuple[bool, str, int, bool]:
            spec = spec.strip()
            if not (match := re.fullmatch(r"(all\s*except\s*)?(oldest|latest)\s*(\d+)%?", spec)):
                parser.error(f"{option_string}: Invalid rank format: {spec}")
            assert match
            is_except = bool(match.group(1))
            kind = match.group(2)
            num = int(match.group(3))
            is_percent = spec.endswith("%")
            if is_percent and num > 100:
                parser.error(f"{option_string}: Invalid rank: Percent must not be greater than 100: {spec}")
            return is_except, kind, num, is_percent

        rankranges = []
        for value in values:
            value = value.strip()
            if ".." in value:
                lo_split, hi_split = value.split("..", 1)
                lo = parse_rank(lo_split)
                hi = parse_rank(hi_split)
                if lo[0] or hi[0]:
                    # Example: 'all except latest 90..except latest 95' or 'all except latest 90..latest 95'
                    parser.error(f"{option_string}: Invalid rank range: {value}")
                if lo[1] != hi[1]:
                    # Example: 'latest10..oldest10' and 'oldest10..latest10' may be somewhat unambigous if there are 40
                    # input snapshots, but they are tricky/not well-defined if there are less than 20 input snapshots.
                    parser.error(f"{option_string}: Ambiguous rank range: Must not compare oldest with latest: {value}")
            else:
                hi = parse_rank(value)
                is_except, kind, num, is_percent = hi
                if is_except:
                    if is_percent:
                        # 'all except latest 10%' aka 'oldest 90%' aka 'oldest 0..oldest 90%'
                        # 'all except oldest 10%' aka 'latest 90%' aka 'latest 0..oldest 90%'
                        negated_kind = "oldest" if kind == "latest" else "latest"
                        lo = parse_rank(f"{negated_kind}0")
                        hi = parse_rank(f"{negated_kind}{100-num}%")
                    else:
                        # 'all except latest 90' aka 'latest 90..latest 100%'
                        # 'all except oldest 90' aka 'oldest 90..oldest 100%'
                        lo = parse_rank(f"{kind}{num}")
                        hi = parse_rank(f"{kind}100%")
                else:
                    # 'latest 90' aka 'latest 0..latest 90'
                    lo = parse_rank(f"{kind}0")
            rankranges.append((lo[1:], hi[1:]))
        return rankranges


#############################################################################
@dataclass(order=True)
class SnapshotFilter:
    name: str
    timerange: UnixTimeRange
    options: Any = field(compare=False, default=None)


def add_snapshot_filter(args: argparse.Namespace, _filter: SnapshotFilter) -> None:
    if not hasattr(args, snapshot_filters_var):
        args.snapshot_filters_var = [[]]
    args.snapshot_filters_var[-1].append(_filter)


def add_time_and_rank_snapshot_filter(
    args: argparse.Namespace, dst: str, timerange: UnixTimeRange, rankranges: list[RankRange]
) -> None:
    if timerange is None or len(rankranges) == 0 or any(rankrange[0] == rankrange[1] for rankrange in rankranges):
        add_snapshot_filter(args, SnapshotFilter("include_snapshot_times", timerange, None))
    else:
        assert timerange is not None
        add_snapshot_filter(args, SnapshotFilter(dst, timerange, rankranges))


def has_timerange_filter(snapshot_filters: list[list[SnapshotFilter]]) -> bool:
    """Interacts with add_time_and_rank_snapshot_filter() and optimize_snapshot_filters()"""
    return any(f.timerange is not None for snapshot_filter in snapshot_filters for f in snapshot_filter)


def optimize_snapshot_filters(snapshot_filters: list[SnapshotFilter]) -> list[SnapshotFilter]:
    """Not intended to be a full query execution plan optimizer, but we still apply some basic plan optimizations."""
    merge_adjacent_snapshot_filters(snapshot_filters)
    merge_adjacent_snapshot_regexes(snapshot_filters)
    snapshot_filters = [f for f in snapshot_filters if f.timerange or f.options]  # drop noop --include-snapshot-times
    reorder_snapshot_time_filters(snapshot_filters)
    return snapshot_filters


def merge_adjacent_snapshot_filters(snapshot_filters: list[SnapshotFilter]) -> None:
    """Merges filter operators of the same kind if they are next to each other and carry an option list, for example
    --include-snapshot-ranks and --include-snapshot-regex and --exclude-snapshot-regex.  This improves execution perf
    and makes handling easier in later stages.
    Example: merges --include-snapshot-times-and-ranks 0..9 oldest10% --include-snapshot-times-and-ranks 0..9 latest20%
    into --include-snapshot-times-and-ranks 0..9 oldest10% latest20%"""
    i = len(snapshot_filters) - 1
    while i >= 0:
        filter_i = snapshot_filters[i]
        if isinstance(filter_i.options, list):
            j = i - 1
            if j >= 0 and snapshot_filters[j] == filter_i:
                lst = snapshot_filters[j].options
                assert isinstance(lst, list)
                lst += filter_i.options
                snapshot_filters.pop(i)
        i -= 1


def merge_adjacent_snapshot_regexes(snapshot_filters: list[SnapshotFilter]) -> None:
    # Merge regex filter operators of the same kind as long as they are within the same group, aka as long as they
    # are not separated by a non-regex filter. This improves execution perf and makes handling easier in later stages.
    # Example: --include-snapshot-regex .*daily --exclude-snapshot-regex .*weekly --include-snapshot-regex .*hourly
    # --exclude-snapshot-regex .*monthly
    # gets merged into the following:
    # --include-snapshot-regex .*daily .*hourly --exclude-snapshot-regex .*weekly .*monthly
    i = len(snapshot_filters) - 1
    while i >= 0:
        filter_i = snapshot_filters[i]
        if filter_i.name in snapshot_regex_filter_names:
            assert isinstance(filter_i.options, list)
            j = i - 1
            while j >= 0 and snapshot_filters[j].name in snapshot_regex_filter_names:
                if snapshot_filters[j].name == filter_i.name:
                    lst = snapshot_filters[j].options
                    assert isinstance(lst, list)
                    lst += filter_i.options
                    snapshot_filters.pop(i)
                    break
                j -= 1
        i -= 1

    # Merge --include-snapshot-regex and --exclude-snapshot-regex filters that are part of the same group (i.e. next
    # to each other) into a single combined filter operator that contains the info of both, and hence all info for the
    # group, which makes handling easier in later stages.
    # Example: --include-snapshot-regex .*daily .*hourly --exclude-snapshot-regex .*weekly .*monthly
    # gets merged into the following: --snapshot-regex(excludes=[.*weekly, .*monthly], includes=[.*daily, .*hourly])
    i = len(snapshot_filters) - 1
    while i >= 0:
        filter_i = snapshot_filters[i]
        name = filter_i.name
        if name in snapshot_regex_filter_names:
            j = i - 1
            if j >= 0 and snapshot_filters[j].name in snapshot_regex_filter_names:
                filter_j = snapshot_filters[j]
                assert filter_j.name != name
                snapshot_filters.pop(i)
                i -= 1
            else:
                name_j = next(iter(snapshot_regex_filter_names.difference({name})))
                filter_j = SnapshotFilter(name_j, None, [])
            sorted_filters = sorted([filter_i, filter_j])
            exclude_regexes, include_regexes = sorted_filters[0].options, sorted_filters[1].options
            snapshot_filters[i] = SnapshotFilter(snapshot_regex_filter_name, None, (exclude_regexes, include_regexes))
        i -= 1


def reorder_snapshot_time_filters(snapshot_filters: list[SnapshotFilter]) -> None:
    """In an execution plan that contains filter operators based on sort order (the --include-snapshot-times-and-ranks
    operator with non-empty ranks), filters cannot freely be reordered without violating correctness, but they can
    still be partially reordered for better execution performance. The filter list is partitioned into sections such
    that sections are separated by --include-snapshot-times-and-ranks operators with non-empty ranks. Within each
    section, we move include_snapshot_times operators aka --include-snapshot-times-and-ranks operators with empty ranks
    before --include/exclude-snapshot-regex operators because the former involves fast integer comparisons and the
    latter involves more expensive regex matching.
    Example: reorders --include-snapshot-regex .*daily --include-snapshot-times-and-ranks 2024-01-01..2024-04-01 into
    --include-snapshot-times-and-ranks 2024-01-01..2024-04-01 --include-snapshot-regex .*daily"""

    def reorder_time_filters_within_section(i: int, j: int) -> None:
        while j > i:
            filter_j = snapshot_filters[j]
            if filter_j.name == "include_snapshot_times":
                snapshot_filters.pop(j)
                snapshot_filters.insert(i + 1, filter_j)
            j -= 1

    i = len(snapshot_filters) - 1
    j = i
    while i >= 0:
        name = snapshot_filters[i].name
        if name == "include_snapshot_times_and_ranks":
            reorder_time_filters_within_section(i, j)
            j = i - 1
        i -= 1
    reorder_time_filters_within_section(i, j)


#############################################################################
class LogConfigVariablesAction(argparse.Action):
    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        current_values = getattr(namespace, self.dest, None)
        if current_values is None:
            current_values = []
        for variable in values:
            error_msg = validate_log_config_variable(variable)
            if error_msg:
                parser.error(error_msg)
            current_values.append(variable)
        setattr(namespace, self.dest, current_values)


#############################################################################
class CheckPercentRange(CheckRange):

    def __call__(
        self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Any, option_string: str | None = None
    ) -> None:
        assert isinstance(values, str)
        original = values
        values = values.strip()
        is_percent = values.endswith("%")
        if is_percent:
            values = values[0:-1]
        try:
            values = float(values)
        except ValueError:
            parser.error(f"{option_string}: Invalid percentage or number: {original}")
        super().__call__(parser, namespace, values, option_string=option_string)
        setattr(namespace, self.dest, (getattr(namespace, self.dest), is_percent))


#############################################################################
if __name__ == "__main__":
    main()
