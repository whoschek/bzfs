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

# /// script
# requires-python = ">=3.7"
# dependencies = []
# ///

"""
* The codebase starts with docs, definition of input data and associated argument parsing into a "Params" class.
* All CLI option/parameter values are reachable from the "Params" class.
* Control flow starts in main(), far below ..., which kicks off a "Job".
* A Job runs one or more "tasks" via run_tasks(), each task replicating a separate dataset tree.
* The core replication algorithm is in run_task() and especially in replicate_dataset().
* The filter algorithms that apply include/exclude policies are in filter_datasets() and filter_snapshots().
* Consider using an IDE/editor that can open multiple windows for the same file, such as PyCharm or Sublime Text, etc.
* README.md is mostly auto-generated from the ArgumentParser help texts as the source of "truth", via update_readme.py.
Simply run that script whenever you change or add ArgumentParser help text.
"""

import argparse
import collections
import fcntl
import hashlib
import inspect
import itertools
import json
import logging
import logging.config
import logging.handlers
import operator
import os
import platform
import pprint
import pwd
import re
import random
import shlex
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
from argparse import Namespace
from collections import defaultdict, deque, Counter
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import redirect_stderr
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from itertools import groupby
from logging import Logger
from math import ceil
from pathlib import Path
from subprocess import CalledProcessError, TimeoutExpired
from typing import Iterable, Dict, List, Set, Tuple, Any, Callable, Generator, Optional, Union

__version__ = "1.6.0"
prog_name = "bzfs"
prog_author = "Wolfgang Hoschek"
die_status = 3
min_python_version = (3, 7)
if sys.version_info < min_python_version:
    print(f"ERROR: {prog_name} requires Python version >= {'.'.join(map(str, min_python_version))}!")
    sys.exit(die_status)
exclude_dataset_regexes_default = r"(.*/)?[Tt][Ee]?[Mm][Pp][-_]?[0-9]*"  # skip tmp datasets by default
disable_prg = "-"
env_var_prefix = prog_name + "_"
dummy_dataset = "dummy"
zfs_version_is_at_least_2_1_0 = "zfs>=2.1.0"
zfs_recv_groups = {"zfs_recv_o": "-o", "zfs_recv_x": "-x", "zfs_set": ""}
snapshot_regex_filter_names = {"include_snapshot_regex", "exclude_snapshot_regex"}
snapshot_regex_filter_name = "snapshot_regex"
snapshot_filters_var = "snapshot_filters_var"
cmp_choices_items = ["src", "dst", "all"]
inject_dst_pipe_fail_kbytes = 400
unixtime_infinity_secs = 2**64  # billions of years in the future and to be extra safe, larger than the largest ZFS GUID
log_stderr = (logging.INFO + logging.WARN) // 2  # custom log level is halfway in between
log_stdout = (log_stderr + logging.INFO) // 2  # custom log level is halfway in between
log_debug = logging.DEBUG
log_trace = logging.DEBUG // 2  # custom log level is halfway in between
DONT_SKIP_DATASET = ""
PIPE = subprocess.PIPE


# fmt: off
def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog_name,
        allow_abbrev=False,
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

{prog_name} does not create or delete ZFS snapshots on the source - it assumes you have a ZFS snapshot
management tool to do so, for example policy-driven Sanoid, zrepl, pyznap, zfs-auto-snapshot, zfs_autobackup,
manual zfs snapshot/destroy, etc. {prog_name} treats the source as read-only, thus the source remains unmodified.
With the --dryrun flag, {prog_name} also treats the destination as read-only.
In normal operation, {prog_name} treats the destination as append-only. Optional CLI flags are available to
delete destination snapshots and destination datasets as directed, for example to make the destination
identical to the source if the two have somehow diverged in unforeseen ways. This easily enables
(re)synchronizing the backup from the production state, as well as restoring the production state from
backup.

In the spirit of rsync, {prog_name} supports a variety of powerful include/exclude filters that can be combined to
select which datasets, snapshots and properties to replicate or delete or compare.

The source 'pushes to' the destination whereas the destination 'pulls from' the source. {prog_name} is installed
and executed on the 'initiator' host which can be either the host that contains the source dataset (push mode),
or the destination dataset (pull mode), or both datasets (local mode, no network required, no ssh required),
or any third-party (even non-ZFS OSX) host as long as that host is able to SSH (via standard 'ssh' CLI) into
both the source and destination host (pull-push mode). In pull-push mode the source 'zfs send's the data stream
to the initiator which immediately pipes the stream (without storing anything locally) to the destination
host that 'zfs receive's it. Pull-push mode means that {prog_name} need not be installed or executed on either
source or destination host. Only the underlying 'zfs' CLI must be installed on both source and destination host.
{prog_name} can run as root or non-root user, in the latter case via a) sudo or b) when granted corresponding
ZFS permissions by administrators via 'zfs allow' delegation mechanism.

{prog_name} is written in Python and continously runs a wide set of unit tests and integration tests to ensure
coverage and compatibility with old and new versions of ZFS on Linux, FreeBSD and Solaris, on all Python
versions >= 3.7 (including latest stable which is currently python-3.13).

{prog_name} is a stand-alone program with zero required dependencies, consisting of a single file, akin to a
stand-alone shell script or binary executable. It is designed to be able to run in restricted barebones server
environments. No external Python packages are required; indeed no Python package management at all is required.
You can just copy the file wherever you like, for example into /usr/local/bin or similar, and simply run it like
any stand-alone shell script or binary executable.

Optionally, {prog_name} applies bandwidth rate-limiting and progress monitoring (via 'pv' CLI) during 'zfs
send/receive' data transfers. When run across the network, {prog_name} also transparently inserts lightweight
data compression (via 'zstd -1' CLI) and efficient data buffering (via 'mbuffer' CLI) into the pipeline
between network endpoints during 'zfs send/receive' network transfers. If one of these utilities is not
installed this is auto-detected, and the operation continues reliably without the corresponding auxiliary
feature.


# Example Usage

* Example in local mode (no network, no ssh) to replicate ZFS dataset tank1/foo/bar to tank2/boo/bar:

```$ {prog_name} tank1/foo/bar tank2/boo/bar```

```$ zfs list -t snapshot tank1/foo/bar
tank1/foo/bar@test_2024-11-06_08:30:05_daily
tank1/foo/bar@test_2024-11-06_08:30:05_hourly

$ zfs list -t snapshot tank2/boo/bar
tank2/boo/bar@test_2024-11-06_08:30:05_daily
tank2/boo/bar@test_2024-11-06_08:30:05_hourly
```

* Same example in pull mode:

`   {prog_name} root@host1.example.com:tank1/foo/bar tank2/boo/bar`

* Same example in push mode:

`   {prog_name} tank1/foo/bar root@host2.example.com:tank2/boo/bar`

* Same example in pull-push mode:

`   {prog_name} root@host1:tank1/foo/bar root@host2:tank2/boo/bar`

* Example in local mode (no network, no ssh) to recursively replicate ZFS dataset tank1/foo/bar and its descendant
datasets to tank2/boo/bar:

```$ {prog_name} tank1/foo/bar tank2/boo/bar --recursive```

```$ zfs list -t snapshot -r tank1/foo/bar
tank1/foo/bar@test_2024-11-06_08:30:05_daily
tank1/foo/bar@test_2024-11-06_08:30:05_hourly
tank1/foo/bar/baz@test_2024-11-06_08:40:00_daily
tank1/foo/bar/baz@test_2024-11-06_08:40:00_hourly

$ zfs list -t snapshot -r tank2/boo/bar
tank2/boo/bar@test_2024-11-06_08:30:05_daily
tank2/boo/bar@test_2024-11-06_08:30:05_hourly
tank2/boo/bar/baz@test_2024-11-06_08:40:00_daily
tank2/boo/bar/baz@test_2024-11-06_08:40:00_hourly
```

* Example that makes destination identical to source even if the two have drastically diverged:

`   {prog_name} tank1/foo/bar tank2/boo/bar --recursive --force --delete-dst-datasets --delete-dst-snapshots`

* Replicate all daily snapshots created during the last 7 days, and at the same time ensure that the latest 7 daily
snapshots (per dataset) are replicated regardless of creation time:

`   {prog_name} tank1/foo/bar tank2/boo/bar --recursive --include-snapshot-regex '.*_daily'
--include-snapshot-times-and-ranks '7 days ago..*' 'latest 7'`

* Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per dataset) are retained
regardless of creation time:

`   {prog_name} {dummy_dataset} tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots
--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks '0..0' 'latest 7..latest 100%'
--include-snapshot-times-and-ranks '*..7 days ago'`

* Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per dataset) are retained
regardless of creation time. Additionally, only delete a snapshot if no corresponding snapshot or bookmark exists in
the source dataset (same as above except replace the 'dummy' source with 'tank1/foo/bar'):

`   {prog_name} tank1/foo/bar tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots
--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks '0..0' 'latest 7..latest 100%'
--include-snapshot-times-and-ranks '*..7 days ago'`

* Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per dataset) are retained
regardless of creation time. Additionally, only delete a snapshot if no corresponding snapshot exists in the source
dataset (same as above except append 'no-crosscheck'):

`   {prog_name} tank1/foo/bar tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots
--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks '0..0' 'latest 7..latest 100%'
--include-snapshot-times-and-ranks '*..7 days ago' --delete-dst-snapshots-no-crosscheck`

* Delete all daily bookmarks older than 90 days, but retain the latest 200 daily bookmarks (per dataset) regardless
of creation time:

`   {prog_name} {dummy_dataset} tank1/foo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots=bookmarks
--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks '0..0' 'latest 200..latest 100%'
--include-snapshot-times-and-ranks '*..90 days ago'`

* Delete all tmp datasets within tank2/boo/bar:

`   {prog_name} {dummy_dataset} tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-datasets
--include-dataset-regex '(.*/)?tmp.*'`

* Compare source and destination dataset trees recursively, for example to check if all recently taken snapshots have 
been successfully replicated by a periodic job. List snapshots only contained in src (tagged with 'src'), 
only contained in dst (tagged with 'dst'), and contained in both src and dst (tagged with 'all'), restricted to hourly 
and daily snapshots taken within the last 7 days, excluding the last 4 hours (to allow for some slack/stragglers), 
excluding temporary datasets:

`   {prog_name} tank1/foo/bar tank2/boo/bar --skip-replication --compare-snapshot-lists=src+dst+all --recursive
--include-snapshot-regex '.*_(hourly|daily)' --include-snapshot-times-and-ranks '7 days ago..4 hours ago' 
--exclude-dataset-regex '(.*/)?tmp.*'`

If the resulting TSV output file contains zero lines starting with the prefix 'src' and zero lines starting with the 
prefix 'dst' then no source snapshots are missing on the destination, and no destination snapshots are missing 
on the source, indicating that the periodic replication and pruning jobs perform as expected. The TSV output is sorted 
by dataset, and by ZFS creation time within each dataset - the first and last line prefixed with 'all' contains the 
metadata of the oldest and latest common snapshot, respectively. The --compare-snapshot-lists option also directly 
logs various summary stats, such as the metadata of the latest common snapshot, latest snapshots and oldest snapshots, 
as well as the time diff between the latest common snapshot and latest snapshot only in src (and only in dst), as well 
as how many src snapshots and how many GB of data are missing on dst, etc.

* Example with further options:

`   {prog_name} tank1/foo/bar root@host2.example.com:tank2/boo/bar --recursive
--exclude-snapshot-regex '.*_(hourly|frequent)' --exclude-snapshot-regex 'test_.*'
--include-snapshot-times-and-ranks '7 days ago..*' 'latest 7' --exclude-dataset /tank1/foo/bar/temporary
--exclude-dataset /tank1/foo/bar/baz/trash --exclude-dataset-regex '(.*/)?private'
--exclude-dataset-regex '(.*/)?[Tt][Ee]?[Mm][Pp][-_]?[0-9]*' --ssh-dst-private-key /root/.ssh/id_rsa`
""", formatter_class=argparse.RawTextHelpFormatter)

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
             "parent and ancestors.\n\n")
    parser.add_argument(
        "--recursive", "-r", action="store_true",
        help="During replication and deletion, also consider descendant datasets, i.e. datasets within the dataset "
             "tree, including children, and children of children, etc.\n\n")
    parser.add_argument(
        "--include-dataset", action=FileOrLiteralAction, nargs="+", default=[], metavar="DATASET",
        help="During replication and deletion, select any ZFS dataset (and its descendants) that is contained within "
             "SRC_DATASET (DST_DATASET in case of deletion) if its dataset name is one of the given include dataset "
             "names but none of the exclude dataset names. If a dataset is excluded its descendants are automatically "
             "excluded too, and this decision is never reconsidered even for the descendants because exclude takes "
             "precedence over include.\n\n"
             "A dataset name is absolute if the specified dataset is prefixed by `/`, e.g. `/tank/baz/tmp`. "
             "Otherwise the dataset name is relative wrt. source and destination, e.g. `baz/tmp` if the source "
             "is `tank`.\n\n"
             "This option is automatically translated to an --include-dataset-regex (see below) and can be "
             "specified multiple times.\n\n"
             "If the option starts with a `+` prefix then dataset names are read from the newline-separated "
             "UTF-8 text file given after the `+` prefix, one dataset per line inside of the text file. "
             "Examples: `/tank/baz/tmp` (absolute), `baz/tmp` (relative), "
             "`+dataset_names.txt`, `+/path/to/dataset_names.txt`\n\n")
    parser.add_argument(
        "--exclude-dataset", action=FileOrLiteralAction, nargs="+", default=[], metavar="DATASET",
        help="Same syntax as --include-dataset (see above) except that the option is automatically translated to an "
             "--exclude-dataset-regex (see below).\n\n")
    parser.add_argument(
        "--include-dataset-regex", action=FileOrLiteralAction, nargs="+", default=[], metavar="REGEX",
        help="During replication (and deletion), select any ZFS dataset (and its descendants) that is contained "
             "within SRC_DATASET (DST_DATASET in case of deletion) if its relative dataset path (e.g. `baz/tmp`) wrt. "
             "SRC_DATASET (DST_DATASET in case of deletion) matches at least one of the given include regular "
             "expressions but none of the exclude regular expressions. "
             "If a dataset is excluded its descendants are automatically excluded too, and this decision is never "
             "reconsidered even for the descendants because exclude takes precedence over include.\n\n"
             "This option can be specified multiple times. "
             "A leading `!` character indicates logical negation, i.e. the regex matches if the regex with the "
             "leading `!` character removed does not match.\n\n"
             "Default: `.*` (include all datasets). "
             "Examples: `baz/tmp`, `(.*/)?doc[^/]*/(private|confidential).*`, `!public`\n\n")
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
             "c) Value is a comma-separated list of fully qualified host names (no spaces, for example: "
             "'store001.example.com,store002.example.com'): Include the dataset if the fully qualified host name of "
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
        help="During replication and deletion, select any source ZFS snapshot that has a name (i.e. the part after "
             "the '@') that matches at least one of the given include regular expressions but none of the "
             "exclude regular expressions. If a snapshot is excluded this decision is never reconsidered because "
             "exclude takes precedence over include.\n\n"
             "This option can be specified multiple times. "
             "A leading `!` character indicates logical negation, i.e. the regex matches if the regex with the "
             "leading `!` character removed does not match.\n\n"
             "Default: `.*` (include all snapshots). "
             "Examples: `test_.*`, `!prod_.*`, `.*_(hourly|frequent)`, `!.*_(weekly|daily)`\n\n"
             "*Note:* All --include/exclude-snapshot-* CLI option groups are combined into a mini filter pipeline. "
             "A filter pipeline is executed in the order given on the command line, left to right. For example if "
             "--include-snapshot-ranks (see below) is specified on the command line before "
             "--include/exclude-snapshot-regex, then --include-snapshot-ranks will be applied before "
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
             "use '0..0' to indicate an empty time range, or omit the rank range, respectively. "
             "This option can be specified multiple times.\n\n"
             "<b>*Replication Example (UNION):* </b>\n\n"
             "Specify to replicate all daily snapshots created during the last 7 days, "
             "and at the same time ensure that the latest 7 daily snapshots (per dataset) are replicated regardless "
             "of creation time, like so: "
             "`--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks '7 days ago..*' 'latest 7'`\n\n"
             "<b>*Deletion Example (no UNION):* </b>\n\n"
             "Specify to delete all daily snapshots older than 7 days, but ensure that the "
             "latest 7 daily snapshots (per dataset) are retained regardless of creation time, like so: "
             "`--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks '0..0' 'latest 7..latest 100%%' "
             "--include-snapshot-times-and-ranks '*..7 days ago'`"
             "\n\n"
             "This helps to safely cope with irregular scenarios where no snapshots were created or received within "
             "the last 7 days, or where more than 7 daily snapshots were created within the last 7 days. It can also "
             "help to avoid accidental pruning of the last snapshot that source and destination have in common.\n\n"
             ""
             "<b>*TIMERANGE:* </b>\n\n"
             "The ZFS 'creation' time of a snapshot (and bookmark) must fall into this time range in order for the "
             "snapshot to be included. The time range consists of a 'start' time, followed by a '..' separator, "
             "followed by an 'end' time. For example '2024-01-01..2024-04-01' or `*..*` aka all times. Only "
             "snapshots (and bookmarks) in the half-open time range [start, end) are included; other snapshots "
             "(and bookmarks) are excluded. If a snapshot is excluded this decision is never reconsidered because "
             "exclude takes precedence over include. Each of the two specified times can take any of the following "
             "forms:\n\n"
             "* a) a `*` wildcard character representing negative or positive infinity.\n\n"
             "* b) a non-negative integer representing a UTC Unix time in seconds. Example: 1728109805\n\n"
             "* c) an ISO 8601 datetime string with or without timezone. Examples: '2024-10-05', "
             "'2024-10-05T14:48:55', '2024-10-05T14:48:55+02', '2024-10-05T14:48:55-04:30'. If the datetime string "
             "does not contain time zone info then it is assumed to be in the local time zone. Timezone string support "
             "requires Python >= 3.11.\n\n"
             "* d) a duration that indicates how long ago from the current time, using the following syntax: "
             "a non-negative integer, followed by an optional space, followed by a duration unit that is "
             "*one* of 'seconds', 'secs', 'minutes', 'mins', 'hours', 'days', 'weeks', "
             "followed by an optional space, followed by the word 'ago'. "
             "Examples: '0secs ago', '90 mins ago', '48hours ago', '90days ago', '12weeksago'.\n\n"
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
             "snapshot is the zero-based integer position of the snapshot within that sorted list. A rank consist of "
             "the word 'oldest' or 'latest', followed by a non-negative integer, followed by an optional '%%' percent "
             "sign. A rank range consists of a lower rank, followed by a '..' separator, followed by a higher rank. "
             "If the optional lower rank is missing it is assumed to be 0. Examples:\n\n"
             "* 'oldest 10%%' aka 'oldest 0..oldest 10%%' (include the oldest 10%% of all snapshots)\n\n"
             "* 'latest 10%%' aka 'latest 0..latest 10%%' (include the latest 10%% of all snapshots)\n\n"
             "* 'oldest 90' aka 'oldest 0..oldest 90' (include the oldest 90 snapshots)\n\n"
             "* 'latest 90' aka 'latest 0..latest 90' (include the latest 90 snapshots)\n\n"
             "* 'oldest 90..oldest 100%%' (include all snapshots except the oldest 90 snapshots)\n\n"
             "* 'latest 90..latest 100%%' (include all snapshots except the latest 90 snapshots)\n\n"
             "* 'latest 1' aka 'latest 0..latest 1' (include the latest snapshot)\n\n"
             "* 'latest 1..latest 100%%' (include all snapshots except the latest snapshot)\n\n"
             "* 'oldest 2' aka 'oldest 0..oldest 2' (include the oldest 2 snapshots)\n\n"
             "* 'oldest 2..oldest 100%%' (include all snapshots except the oldest 2 snapshots)\n\n"
             "* 'oldest 100%%' aka 'oldest 0..oldest 100%%' (include all snapshots)\n\n"
             "* 'oldest 0%%' aka 'oldest 0..oldest 0%%' (include no snapshots)\n\n"
             "* 'oldest 0' aka 'oldest 0..oldest 0' (include no snapshots)\n\n"
             "*Note:* Percentage calculations are not based on the number of snapshots "
             "contained in the dataset on disk, but rather based on the number of snapshots arriving at the filter. "
             "For example, if only two daily snapshots arrive at the filter because a prior filter excludes hourly "
             "snapshots, then 'latest 10' will only include these two daily snapshots, and 'latest 50%%' will only "
             "include one of these two daily snapshots.\n\n"
             "*Note:* During replication, bookmarks are always retained aka selected in order to help find common "
             "snapshots between source and destination. Bookmarks do not count towards N or N%% wrt. rank.\n\n"
             "*Note:* If a snapshot is excluded this decision is never reconsidered because exclude takes precedence "
             "over include.\n\n")
    zfs_send_program_opts_default = "--props --raw --compressed"
    parser.add_argument(
        "--zfs-send-program-opts", type=str, default=zfs_send_program_opts_default, metavar="STRING",
        help="Parameters to fine-tune 'zfs send' behaviour (optional); will be passed into 'zfs send' CLI. "
             "The value is split on runs of one or more whitespace characters. "
             f"Default is '{zfs_send_program_opts_default}'. "
             "See https://openzfs.github.io/openzfs-docs/man/master/8/zfs-send.8.html "
             "and https://github.com/openzfs/zfs/issues/13024\n\n")
    zfs_recv_program_opts_default = "-u"
    parser.add_argument(
        "--zfs-recv-program-opts", type=str, default=zfs_recv_program_opts_default, metavar="STRING",
        help="Parameters to fine-tune 'zfs receive' behaviour (optional); will be passed into 'zfs receive' CLI. "
             "The value is split on runs of one or more whitespace characters. "
             f"Default is '{zfs_recv_program_opts_default}'. "
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
        "--force-rollback-to-latest-snapshot", action="store_true",
        help="Before replication, rollback the destination dataset to its most recent destination snapshot (if there "
             "is one), via 'zfs rollback', just in case the destination dataset was modified since its most recent "
             "snapshot. This is much less invasive than the other --force* options (see below).\n\n")
    parser.add_argument(
        "--force-rollback-to-latest-common-snapshot", action="store_true",
        help="Before replication, delete destination ZFS snapshots that are more recent than the most recent common "
             "snapshot selected on the source ('conflicting snapshots'), via 'zfs rollback'. Abort with an error if "
             "no common snapshot is selected but the destination already contains a snapshot.\n\n")
    parser.add_argument(
        "--force", action="store_true",
        help="Same as --force-rollback-to-latest-common-snapshot (see above), except that additionally, if no common "
             "snapshot is selected, then delete all destination snapshots before starting replication, and proceed "
             "without aborting. Without the --force* flags, the destination dataset is treated as append-only, hence "
             "no destination snapshot that already exists is deleted, and instead the operation is aborted with an "
             "error when encountering a conflicting snapshot.\n\n"
             "Analogy: --force-rollback-to-latest-snapshot is a tiny hammer, whereas "
             "--force-rollback-to-latest-common-snapshot is a medium sized hammer, and --force is a large hammer. "
             "Consider using the smallest hammer that can fix the problem. No hammer is ever used by default.\n\n")
    parser.add_argument(
        "--force-unmount", action="store_true",
        help="On destination, --force and --force-rollback-to-latest-common-snapshot will add the '-f' flag to their "
             "use of 'zfs rollback' and 'zfs destroy'.\n\n")
    parser.add_argument(
        "--force-hard", action="store_true",
        # help="On destination, --force will also delete dependents such as clones and bookmarks via "
        #      "'zfs rollback -R' and 'zfs destroy -R'. This can be very destructive and is rarely what you "
        #      "want!\n\n")
        help=argparse.SUPPRESS)
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
             f"Unix `rm -r dst/*`\n\n")
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
    retries_default = 2
    parser.add_argument(
        "--retries", type=int, min=0, default=retries_default, action=CheckRange, metavar="INT",
        help="The maximum number of times a retryable replication or deletion step shall be retried if it fails, for "
             f"example because of network hiccups (default: {retries_default}, min: 0). "
             "Also consider this option if a periodic pruning script may simultaneously delete a dataset or "
             f"snapshot or bookmark while {prog_name} is running and attempting to access it.\n\n")
    retry_min_sleep_secs_default = 0.125
    parser.add_argument(
        "--retry-min-sleep-secs", type=float, min=0, default=retry_min_sleep_secs_default,
        action=CheckRange, metavar="FLOAT",
        help=f"The minimum duration to sleep between retries (default: {retry_min_sleep_secs_default}).\n\n")
    retry_max_sleep_secs_default = 5 * 60
    parser.add_argument(
        "--retry-max-sleep-secs", type=float, min=0, default=retry_max_sleep_secs_default,
        action=CheckRange, metavar="FLOAT",
        help="The maximum duration to sleep between retries initially starts with --retry-min-sleep-secs (see above), "
             "and doubles on each retry, up to the final maximum of --retry-max-sleep-secs "
             f"(default: {retry_max_sleep_secs_default}). On each retry a random sleep time in the "
             "[--retry-min-sleep-secs, current max] range is picked. The timer resets after each operation.\n\n")
    retry_max_elapsed_secs_default = 60 * 60
    parser.add_argument(
        "--retry-max-elapsed-secs", type=float, min=0, default=retry_max_elapsed_secs_default,
        action=CheckRange, metavar="FLOAT",
        help="A single operation (e.g. 'zfs send/receive' of the current dataset, or deletion of a list of snapshots "
             "within the current dataset) will not be retried (or not retried anymore) once this much time has elapsed "
             f"since the initial start of the operation, including retries (default: {retry_max_elapsed_secs_default})."
             " The timer resets after each operation completes or retries exhaust, such that subsequently failing "
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
             "the next dataset instead. "
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
             f"dataset named '{dummy_dataset}'!). Does not recurse without --recursive.\n\n"
             "For example, if the destination contains datasets h1,h2,h3,d1 whereas source only contains h3, "
             "and the include/exclude policy selects h1,h2,h3,d1, then delete datasets h1,h2,d1 on "
             "the destination to make it 'the same'. On the other hand, if the include/exclude policy "
             "only selects h1,h2,h3 then only delete datasets h1,h2 on the destination to make it 'the same'.\n\n"
             "Example to delete all tmp datasets within tank2/boo/bar: "
             f"`{prog_name} {dummy_dataset} tank2/boo/bar --dryrun --skip-replication "
             "--delete-dst-datasets --include-dataset-regex 'tmp.*' --recursive`\n\n")
    parser.add_argument(
        "--delete-dst-snapshots", choices=["snapshots", "bookmarks"], default=None, const="snapshots",
        nargs="?",
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
             "snapshots wrt. selecting."
             "\n\n")
    parser.add_argument(
        "--delete-dst-snapshots-no-crosscheck", action="store_true",
        help="This flag indicates that --delete-dst-snapshots=snapshots shall check the source dataset only for "
             "a snapshot with the same GUID, and ignore whether a bookmark with the same GUID is present in the "
             "source dataset. Similarly, it also indicates that --delete-dst-snapshots=bookmarks shall check the "
             "source dataset only for a bookmark with the same GUID, and ignore whether a snapshot with the same GUID "
             "is present in the source dataset.")
    parser.add_argument(
        "--delete-empty-dst-datasets", choices=["snapshots", "snapshots+bookmarks"], default=None,
        const="snapshots+bookmarks", nargs="?",
        help="Do nothing if the --delete-empty-dst-datasets option is missing or --recursive is missing. Otherwise, "
             "after successful replication "
             "step and successful --delete-dst-datasets and successful --delete-dst-snapshots steps, if any, "
             "delete any selected destination dataset that has no snapshot and no bookmark if all descendants of "
             "that destination dataset are also selected and do not have a snapshot or bookmark either "
             "(again, only if the existing destination dataset is selected via --{include|exclude}-dataset* policy). "
             "Never delete excluded dataset subtrees or their ancestors.\n\n"
             "For example, if the destination contains datasets h1,d1, and the include/exclude policy "
             "selects h1,d1, then check if h1,d1 can be deleted. "
             "On the other hand, if the include/exclude policy only selects h1 then only check if h1 "
             "can be deleted.\n\n"
             "*Note:* Use --delete-empty-dst-datasets=snapshots to delete snapshot-less datasets even if they still "
             "contain bookmarks.\n\n")
    cmp_choices_dflt = "+".join(cmp_choices_items)
    cmp_choices: List[str] = []
    for i in range(0, len(cmp_choices_items)):
        cmp_choices += map(lambda item: "+".join(item), itertools.combinations(cmp_choices_items, i + 1))
    parser.add_argument(
        "--compare-snapshot-lists", choices=cmp_choices, default=None, const=cmp_choices_dflt, nargs="?",
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
             "verbosity. To print what ZFS/SSH operation exactly is happening (or would happen), add the `-v -v` "
             "flag, maybe along with --dryrun. All ZFS and SSH commands (even with --dryrun) are logged such that "
             "they can be inspected, copy-and-pasted into a terminal shell and run manually to help anticipate or "
             "diagnose issues. ERROR, WARN, INFO, DEBUG, TRACE output lines are identified by [E], [W], [I], [D], [T] "
             "prefixes, respectively.\n\n")
    parser.add_argument(
        "--quiet", "-q", action="store_true",
        help="Suppress non-error, info, debug, and trace output.\n\n")
    parser.add_argument(
        "--no-privilege-elevation", "-p", action="store_true",
        help="Do not attempt to run state changing ZFS operations 'zfs create/rollback/destroy/send/receive' as root "
             "(via 'sudo -u root' elevation granted by administrators appending the following to /etc/sudoers: "
             "`<NON_ROOT_USER_NAME> ALL=NOPASSWD:/path/to/zfs`\n\n"
             "Instead, the --no-privilege-elevation flag is for non-root users that have been granted corresponding "
             "ZFS permissions by administrators via 'zfs allow' delegation mechanism, like so: "
             "sudo zfs allow -u $SRC_NON_ROOT_USER_NAME send,bookmark $SRC_DATASET; "
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
        "--no-create-bookmark", action="store_true",
        help=f"For increased safety, in normal operation {prog_name} behaves as follows wrt. ZFS bookmark creation, "
             "if it is autodetected that the source ZFS pool support bookmarks: "
             f"Whenever it has successfully completed replication of the most recent source snapshot, {prog_name} "
             "creates a ZFS bookmark of that snapshot and attaches it to the source dataset. "
             "Bookmarks exist so an incremental stream can continue to be sent from the source dataset without having "
             "to keep the already replicated snapshot around on the source dataset until the next upcoming snapshot "
             "has been successfully replicated. This way you can send the snapshot from the source dataset to another "
             "host, then bookmark the snapshot on the source dataset, then delete the snapshot from the source "
             "dataset to save disk space, and then still incrementally send the next upcoming snapshot from the "
             "source dataset to the other host by referring to the bookmark.\n\n"
             "The --no-create-bookmark option disables this safety feature but is discouraged, because bookmarks "
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
             "--delete-dst-snapshots=bookmarks --include-snapshot-times-and-ranks '0..0' 'latest 200..latest 100%%' "
             "--include-snapshot-times-and-ranks '*..90 days ago'`\n\n")
    parser.add_argument(
        "--no-use-bookmark", action="store_true",
        help=f"For increased safety, in normal replication operation {prog_name} also looks for bookmarks (in addition "
             "to snapshots) on the source dataset in order to find the most recent common snapshot wrt. the "
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

    ssh_cipher_default = "^aes256-gcm@openssh.com" if platform.system() != "SunOS" else ""
    # for speed with confidentiality and integrity
    # measure cipher perf like so: count=5000; for i in $(seq 1 3); do echo "iteration $i:"; for cipher in $(ssh -Q cipher); do dd if=/dev/zero bs=1M count=$count 2> /dev/null | ssh -c $cipher -p 40999 127.0.0.1 "(time -p cat) > /dev/null" 2>&1 | grep real | awk -v count=$count -v cipher=$cipher '{print cipher ": " count / $2 " MB/s"}'; done; done
    # see https://gbe0.com/posts/linux/server/benchmark-ssh-ciphers/
    # and https://crypto.stackexchange.com/questions/43287/what-are-the-differences-between-these-aes-ciphers
    parser.add_argument(
        "--ssh-cipher", type=str, default=ssh_cipher_default, metavar="STRING",
        help="SSH cipher specification for encrypting the session (optional); will be passed into ssh -c CLI. "
             "--ssh-cipher is a comma-separated list of ciphers listed in order of preference. See the 'Ciphers' "
             "keyword in ssh_config(5) for more information: "
             f"https://manpages.ubuntu.com/manpages/man5/sshd_config.5.html. Default: `{ssh_cipher_default}`\n\n")

    ssh_private_key_file_default = ".ssh/id_rsa"
    locations = ["src", "dst"]
    for loc in locations:
        parser.add_argument(
            f"--ssh-{loc}-private-key", action="append", default=[], metavar="FILE",
            help=f"Path to SSH private key file on local host to connect to {loc} (optional); will be passed into "
                 "ssh -i CLI. This option can be specified multiple times. "
                 f"default: $HOME/{ssh_private_key_file_default}\n\n")
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
            f"--ssh-{loc}-extra-opts", type=str, default="", metavar="STRING",
            help=f"Additional options to be passed to ssh CLI when connecting to {loc} host (optional). "
                 "The value is split on runs of one or more whitespace characters. "
                 f"Example: `--ssh-{loc}-extra-opts='-v -v'` to debug ssh config issues.\n\n")
        parser.add_argument(
            f"--ssh-{loc}-extra-opt", action="append", default=[], metavar="STRING",
            help=f"Additional option to be passed to ssh CLI when connecting to {loc} host (optional). The value "
                 "can contain spaces and is not split. This option can be specified multiple times. "
                 f"Example: `--ssh-{loc}-extra-opt='-oProxyCommand=nc %%h %%p'` to disable the TCP_NODELAY "
                 "socket option for OpenSSH.\n\n")
    for loc in locations:
        parser.add_argument(
            f"--ssh-{loc}-config-file", type=str, metavar="FILE",
            help=f"Path to SSH ssh_config(5) file to connect to {loc} (optional); will be passed into ssh -F CLI.\n\n")
    threads_default = 150  # percent
    parser.add_argument(
        "--threads", min=1, default=(threads_default, True), action=CheckPercentRange, metavar="INT[%]",
        help="The maximum number of threads to use for parallel operations; can be given as a positive integer, "
             f"optionally followed by the %% percent character (min: 1, default: {threads_default}%%). Percentages "
             "are relative to the number of CPU cores on the machine. Example: 200%% uses twice as many threads as "
             "there are cores on the machine; 75%% uses num_threads = num_cores * 0.75. Currently this option only "
             "applies to --compare-snapshot-lists and --delete-empty-dst-datasets. Examples: 4, 75%%\n\n")
    parser.add_argument(
        "--bwlimit", default=None, action=NonEmptyStringAction, metavar="STRING",
        help="Sets 'pv' bandwidth rate limit for zfs send/receive data transfer (optional). Example: `100m` to cap "
             "throughput at 100 MB/sec. Default is unlimited. Also see https://linux.die.net/man/1/pv\n\n")

    def hlp(program: str) -> str:
        return f"The name or path to the '{program}' executable (optional). Default is '{program}'. "

    msg = f"Use '{disable_prg}' to disable the use of this program.\n\n"
    parser.add_argument(
        "--compression-program", default="zstd", action=NonEmptyStringAction, metavar="STRING",
        help=hlp("zstd") + "Examples: 'lz4', 'pigz', 'gzip', '/opt/bin/zstd'. " + msg.rstrip() + " The use is "
                           "auto-disabled if data is transferred locally instead of via the network. This "
                           "option is about transparent compression-on-the-wire, not about compression-at-rest.\n\n")
    parser.add_argument(
        "--compression-program-opts", default="-1", metavar="STRING",
        help="The options to be passed to the compression program on the compression step (optional). "
             "Default is '-1' (fastest).\n\n")
    parser.add_argument(
        "--mbuffer-program", default="mbuffer", action=NonEmptyStringAction, metavar="STRING",
        help=hlp("mbuffer") + msg.rstrip() + " The use on dst is auto-disabled if data is transferred locally "
                                             "instead of via the network. This tool is used to smooth out the rate "
                                             "of data flow and prevent bottlenecks caused by network latency or "
                                             "speed fluctuation.\n\n")
    mbuffer_program_opts_default = "-q -m 128M"
    parser.add_argument(
        "--mbuffer-program-opts", default=mbuffer_program_opts_default, metavar="STRING",
        help=f"Options to be passed to 'mbuffer' program (optional). Default: '{mbuffer_program_opts_default}'.\n\n")
    parser.add_argument(
        "--ps-program", default="ps", action=NonEmptyStringAction, metavar="STRING",
        help=hlp("ps") + msg)
    parser.add_argument(
        "--pv-program", default="pv", action=NonEmptyStringAction, metavar="STRING",
        help=hlp("pv") + msg.rstrip() + " This is used for bandwidth rate-limiting and progress monitoring.\n\n")
    pv_program_opts_default = ("--progress --timer --eta --fineta --rate --average-rate --bytes --interval=1 "
                               "--width=120 --buffer-size=2M")
    parser.add_argument(
        "--pv-program-opts", default=pv_program_opts_default, metavar="STRING",
        help=f"The options to be passed to the 'pv' program (optional). Default: '{pv_program_opts_default}'.\n\n")
    parser.add_argument(
        "--shell-program", default="sh", action=NonEmptyStringAction, metavar="STRING",
        help=hlp("sh") + msg)
    parser.add_argument(
        "--ssh-program", default="ssh", action=NonEmptyStringAction, metavar="STRING",
        help=hlp("ssh") + "Examples: 'hpnssh' or 'ssh' or '/opt/bin/ssh' or wrapper scripts around 'ssh'. " + msg)
    parser.add_argument(
        "--sudo-program", default="sudo", action=NonEmptyStringAction, metavar="STRING",
        help=hlp("sudo") + msg)
    parser.add_argument(
        "--zfs-program", default="zfs", action=NonEmptyStringAction, metavar="STRING",
        help=hlp("zfs") + "\n\n")
    parser.add_argument(
        "--zpool-program", default="zpool", action=NonEmptyStringAction, metavar="STRING",
        help=hlp("zpool") + msg)
    parser.add_argument(
        "--log-dir", type=str, metavar="DIR",
        help=f"Path to the log output directory on local host (optional). Default: $HOME/{prog_name}-logs. The logger "
             "that is used by default writes log files there, in addition to the console. The current.log symlink "
             "always points to the most recent log file. The current.pv symlink always points to the most recent "
             "data transfer monitoring log. Run 'tail --follow=name --max-unchanged-stats=1' on both symlinks to "
             "follow what's currently going on. The current.dir symlink always points to the sub directory containing "
             "the most recent log file.\n\n")
    h_fix = ("The path name of the log file on local host is "
             "`${--log-dir}/${--log-file-prefix}<timestamp>${--log-file-suffix}-<random>.log`. "
             "Example: `--log-file-prefix=zrun_ --log-file-suffix=_daily` will generate log file names such as "
             "`zrun_2024-09-03_12:26:15_daily-bl4i1fth.log`\n\n")
    parser.add_argument(
        "--log-file-prefix", default="zrun_", action=SafeFileNameAction, metavar="STRING",
        help="Default is zrun_. " + h_fix)
    parser.add_argument(
        "--log-file-suffix", default="", action=SafeFileNameAction, metavar="STRING",
        help="Default is the empty string. " + h_fix)
    parser.add_argument(
        "--log-syslog-address", default=None, action=NonEmptyStringAction, metavar="STRING",
        help="Host:port of the syslog machine to send messages to (e.g. 'foo.example.com:514' or '127.0.0.1:514'), or "
             "the file system path to the syslog socket file on localhost (e.g. '/dev/log'). The default is no "
             "address, i.e. do not log anything to syslog by default. See "
             "https://docs.python.org/3/library/logging.handlers.html#sysloghandler\n\n")
    parser.add_argument(
        "--log-syslog-socktype", choices=["UDP", "TCP"], default="UDP",
        help="The socket type to use to connect if no local socket file system path is used. Default is 'UDP'.\n\n")
    parser.add_argument(
        "--log-syslog-facility", type=int, min=0, max=7, default=1, action=CheckRange, metavar="INT",
        help="The local facility aka category that identifies msg sources in syslog (default: 1, min=0, max=7).\n\n")
    parser.add_argument(
        "--log-syslog-prefix", default=prog_name, action=NonEmptyStringAction, metavar="STRING",
        help=f"The name to prepend to each message that is sent to syslog; identifies {prog_name} messages as opposed "
             f"to messages from other sources. Default is '{prog_name}'.\n\n")
    parser.add_argument(
        "--log-syslog-level", choices=["CRITICAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
        default="ERROR",
        help="Only send messages with equal or higher priority than this log level to syslog. Default is 'ERROR'.\n\n")
    parser.add_argument(
        "--log-config-file", default=None, action=NonEmptyStringAction, metavar="STRING",
        help="The contents of a JSON file that defines a custom python logging configuration to be used (optional). "
             "If the option starts with a `+` prefix then the contents are read from the UTF-8 JSON file given "
             "after the `+` prefix. Examples: +log_config.json, +/path/to/log_config.json. "
             "Here is an example config file that demonstrates usage: "
             "https://github.com/whoschek/bzfs/blob/main/tests/log_config.json\n\n"
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
             "The default is to include no environment variables, i.e. to make no exceptions to "
             "--exclude-envvar-regex. "
             "Example that retains at least these two env vars: "
             "`--include-envvar-regex PATH "
             f"--include-envvar-regex {env_var_prefix}min_pipe_transfer_size`. "
             "Example that retains all environment variables without tightened security: `'.*'`\n\n")
    parser.add_argument(
        "--exclude-envvar-regex", action=FileOrLiteralAction, nargs="+", default=[], metavar="REGEX",
        help="Same syntax as --include-envvar-regex (see above) except that the default is to exclude no "
             f"environment variables. Example: `{env_var_prefix}.*`\n\n")

    for option_name, flag in zfs_recv_groups.items():
        grup = option_name.replace("_", "-")  # one of zfs_recv_o, zfs_recv_x
        flag = "'" + flag + "'"  # one of -o or -x

        def h(text: str) -> str:
            return argparse.SUPPRESS if option_name == "zfs_set" else text

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
                   f"Default is '{target_choices_default}'. "
                   "A 'full' send is sometimes also known as an 'initial' send.\n\n"))
        msg = "Thus, -x opts do not benefit from source != 'local' (which is the default already)." \
            if flag == "'-x'" else ""
        argument_group.add_argument(
            f"--{grup}-sources", action=NonEmptyStringAction, default="local", metavar="STRING",
            help=h("The ZFS sources to provide to the 'zfs get -s' CLI in order to fetch the ZFS dataset properties "
                   f"that will be fed into the --{grup}-include/exclude-regex filter (see below). The sources are in "
                   "the form of a comma-separated list (no spaces) containing one or more of the following choices: "
                   "'local', 'default', 'inherited', 'temporary', 'received', 'none', with the default being 'local'. "
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
    def __init__(self, args: argparse.Namespace):
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
        self.log_config_file = args.log_config_file
        self.log_config_vars = dict(var.split(":", 1) for var in args.log_config_var)
        self.timestamp: str = datetime.now().isoformat(sep="_", timespec="seconds")  # 2024-09-03_12:26:15
        self.home_dir: str = get_home_directory()
        log_parent_dir: str = args.log_dir if args.log_dir else os.path.join(self.home_dir, prog_name + "-logs")
        self.log_dir: str = os.path.join(log_parent_dir, self.timestamp[0 : self.timestamp.index("_")])  # 2024-09-03
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file_prefix = args.log_file_prefix
        self.log_file_suffix = args.log_file_suffix
        fd, self.log_file = tempfile.mkstemp(
            suffix=".log", prefix=f"{self.log_file_prefix}{self.timestamp}{self.log_file_suffix}-", dir=self.log_dir
        )
        os.close(fd)
        self.pv_log_file = self.log_file[0 : -len(".log")] + ".pv"
        Path(self.pv_log_file).touch()

        # Create/update "current" symlink to current_dir, which is a subdir containing further symlinks to log files.
        # For parallel usage, ensures there is no time window when the symlinks are inconsistent or do not exist.
        current = "current"
        dot_current_dir = os.path.join(log_parent_dir, f".{current}")
        current_dir = os.path.join(dot_current_dir, os.path.basename(self.log_file)[0 : -len(".log")])
        os.makedirs(current_dir, exist_ok=True)
        create_symlink(self.log_file, current_dir, f"{current}.log")
        create_symlink(self.pv_log_file, current_dir, f"{current}.pv")
        create_symlink(self.log_dir, current_dir, f"{current}.dir")
        dst_file = os.path.join(current_dir, current)
        os.symlink(os.path.relpath(current_dir, start=log_parent_dir), dst_file)
        os.replace(dst_file, os.path.join(log_parent_dir, current))  # atomic rename
        delete_stale_files(dot_current_dir, prefix="", secs=60, dirs=True, exclude=os.path.basename(current_dir))

    def __repr__(self) -> str:
        return str(self.__dict__)


#############################################################################
RegexList = List[Tuple[re.Pattern, bool]]  # Type alias
UnixTimeRange = Optional[Tuple[int, int]]  # Type alias
RankRange = Tuple[Tuple[str, int, bool], Tuple[str, int, bool]]  # Type alias


#############################################################################
class Params:
    def __init__(
        self,
        args: argparse.Namespace,
        sys_argv: Optional[List[str]] = None,
        log_params: LogParams = None,
        log: Logger = None,
        inject_params: Optional[Dict[str, bool]] = None,
    ):
        """Option values for all aspects; reads from ArgumentParser via args."""
        # immutable variables:
        assert args is not None
        self.args: argparse.Namespace = args
        self.sys_argv: List[str] = sys_argv if sys_argv is not None else []
        assert isinstance(self.sys_argv, list)
        self.log_params: LogParams = log_params
        self.log: Logger = log
        self.inject_params: Dict[str, bool] = inject_params if inject_params is not None else {}  # for testing only
        self.one_or_more_whitespace_regex: re.Pattern = re.compile(r"\s+")
        self.unset_matching_env_vars(args)

        assert len(args.root_dataset_pairs) > 0
        self.root_dataset_pairs: List[Tuple[str, str]] = args.root_dataset_pairs
        self.recursive: bool = args.recursive
        self.recursive_flag: str = "-r" if args.recursive else ""

        self.dry_run: bool = args.dryrun is not None
        self.dry_run_recv: str = "-n" if self.dry_run else ""
        self.dry_run_destroy: str = self.dry_run_recv
        self.dry_run_no_send: bool = args.dryrun == "send"
        self.verbose_zfs: bool = args.verbose >= 2
        self.verbose_destroy: str = "" if args.quiet else "-v"

        self.zfs_send_program_opts: List[str] = self.fix_send_opts(self.split_args(args.zfs_send_program_opts))
        zfs_recv_program_opts: List[str] = self.split_args(args.zfs_recv_program_opts)
        for extra_opt in args.zfs_recv_program_opt:
            zfs_recv_program_opts.append(self.validate_arg(extra_opt, allow_all=True))
        self.zfs_recv_program_opts: List[str] = self.fix_recv_opts(zfs_recv_program_opts)
        if self.verbose_zfs:
            append_if_absent(self.zfs_send_program_opts, "-v")
            append_if_absent(self.zfs_recv_program_opts, "-v")
        self.zfs_full_recv_opts: List[str] = self.zfs_recv_program_opts.copy()
        cpconfigs = [CopyPropertiesConfig(group, flag, args, self) for group, flag in zfs_recv_groups.items()]
        self.zfs_recv_o_config, self.zfs_recv_x_config, self.zfs_set_config = cpconfigs

        self.force_rollback_to_latest_snapshot: bool = args.force_rollback_to_latest_snapshot
        self.force_rollback_to_latest_common_snapshot: bool = args.force_rollback_to_latest_common_snapshot
        self.force: bool = args.force
        self.force_once: bool = args.force_once
        self.force_unmount: str = "-f" if args.force_unmount else ""
        self.force_hard: str = "-R" if args.force_hard else ""

        self.skip_parent: bool = args.skip_parent
        self.skip_missing_snapshots: str = args.skip_missing_snapshots
        self.skip_on_error: str = args.skip_on_error
        self.retry_policy: RetryPolicy = RetryPolicy(args, self)
        self.skip_replication: bool = args.skip_replication
        self.delete_dst_snapshots: bool = args.delete_dst_snapshots is not None
        self.delete_dst_bookmarks: bool = args.delete_dst_snapshots == "bookmarks"
        self.delete_dst_snapshots_no_crosscheck: bool = args.delete_dst_snapshots_no_crosscheck
        self.delete_dst_datasets: bool = args.delete_dst_datasets
        self.delete_empty_dst_datasets: bool = args.delete_empty_dst_datasets is not None
        self.delete_empty_dst_datasets_if_no_bookmarks_and_no_snapshots: bool = (
            args.delete_empty_dst_datasets == "snapshots+bookmarks"
        )
        self.compare_snapshot_lists: Optional[str] = args.compare_snapshot_lists
        self.enable_privilege_elevation: bool = not args.no_privilege_elevation
        self.no_stream: bool = args.no_stream
        self.resume_recv: bool = not args.no_resume_recv
        self.create_bookmark: bool = not args.no_create_bookmark
        self.use_bookmark: bool = not args.no_use_bookmark

        self.src: Remote = Remote("src", args, self)  # src dataset, host and ssh options
        self.dst: Remote = Remote("dst", args, self)  # dst dataset, host and ssh options
        self.reuse_ssh_connection: bool = getenv_bool("reuse_ssh_connection", True)

        self.compression_program: str = self.program_name(args.compression_program)
        self.compression_program_opts: List[str] = self.split_args(args.compression_program_opts)
        self.getconf_program: str = self.program_name("getconf")  # print number of CPUs on POSIX except Solaris
        self.psrinfo_program: str = self.program_name("psrinfo")  # print number of CPUs on Solaris
        self.mbuffer_program: str = self.program_name(args.mbuffer_program)
        self.mbuffer_program_opts: List[str] = self.split_args(args.mbuffer_program_opts)
        self.ps_program: str = self.program_name(args.ps_program)
        self.pv_program: str = self.program_name(args.pv_program)
        self.pv_program_opts: List[str] = self.split_args(args.pv_program_opts)
        if args.bwlimit:
            self.pv_program_opts += [f"--rate-limit={self.validate_arg(args.bwlimit)}"]
        self.shell_program_local: str = "sh"
        self.shell_program: str = self.program_name(args.shell_program)
        self.ssh_program: str = self.program_name(args.ssh_program)
        self.sudo_program: str = self.program_name(args.sudo_program)
        self.uname_program: str = self.program_name("uname")
        self.zfs_program: str = self.program_name(args.zfs_program)
        self.zpool_program: str = self.program_name(args.zpool_program)

        # no point creating complex shell pipeline commands for tiny data transfers:
        self.min_pipe_transfer_size: int = int(getenv_any("min_pipe_transfer_size", 1024 * 1024))
        self.max_datasets_per_batch_on_list_snaps = int(getenv_any("max_datasets_per_batch_on_list_snaps", 1024))
        self.max_datasets_per_minibatch_on_list_snaps = int(getenv_any("max_datasets_per_minibatch_on_list_snaps", -1))
        self.threads: Tuple[int, bool] = args.threads

        self.os_cpu_count: int = os.cpu_count()
        self.os_geteuid: int = os.geteuid()
        self.prog_version: str = __version__
        self.python_version: str = sys.version
        self.platform_version: str = platform.version()
        self.platform_platform: str = platform.platform()

        # mutable variables:
        snapshot_filters = args.snapshot_filters_var if hasattr(args, snapshot_filters_var) else []
        self.snapshot_filters: List[SnapshotFilter] = optimize_snapshot_filters(snapshot_filters)
        self.exclude_dataset_property: Optional[str] = args.exclude_dataset_property
        self.exclude_dataset_regexes: RegexList = []  # deferred to validate_task() phase
        self.include_dataset_regexes: RegexList = []  # deferred to validate_task() phase
        self.tmp_exclude_dataset_regexes: RegexList = []  # deferred to validate_task() phase
        self.tmp_include_dataset_regexes: RegexList = []  # deferred to validate_task() phase
        self.abs_exclude_datasets: List[str] = []  # deferred to validate_task() phase
        self.abs_include_datasets: List[str] = []  # deferred to validate_task() phase

        self.curr_zfs_send_program_opts: List[str] = []
        self.zfs_recv_ox_names: Set[str] = set()
        self.available_programs: Dict[str, Dict[str, str]] = {}
        self.zpool_features: Dict[str, Dict[str, str]] = {}

    def split_args(self, text: str, *items, allow_all: bool = False) -> List[str]:
        """Splits option string on runs of one or more whitespace into an option list."""
        text = text.strip()
        opts = self.one_or_more_whitespace_regex.split(text) if text else []
        xappend(opts, items)
        if not allow_all:
            self.validate_quoting(opts)
        return opts

    def validate_arg(self, opt: str, allow_spaces: bool = False, allow_all: bool = False) -> Optional[str]:
        """allow_all permits all characters, including whitespace and quotes. See squote() and dquote()."""
        if allow_all or opt is None:
            return opt
        if any(char.isspace() and (char != " " or not allow_spaces) for char in opt):
            die(f"Option must not contain a whitespace character {'other than space' if allow_spaces else ''} : {opt}")
        self.validate_quoting([opt])
        return opt

    @staticmethod
    def validate_quoting(opts: List[str]) -> None:
        for opt in opts:
            if "'" in opt or '"' in opt or "`" in opt:
                die(f"Option must not contain a single quote or double quote or backtick character: {opt}")

    @staticmethod
    def fix_recv_opts(opts: List[str]) -> List[str]:
        return fix_send_recv_opts(
            opts, exclude_long_opts={"--dryrun"}, exclude_short_opts="n", include_arg_opts={"-o", "-x"}
        )

    @staticmethod
    def fix_send_opts(opts: List[str]) -> List[str]:
        return fix_send_recv_opts(
            opts,
            exclude_long_opts={"--dryrun"},
            exclude_short_opts="den",
            include_arg_opts={"-X", "--exclude", "--redact"},
            exclude_arg_opts={"-i", "-I"},
        )

    def program_name(self, program: str) -> str:
        """For testing: helps simulate errors caused by external programs."""
        self.validate_arg(program)
        if not program:
            die("Program name must not be the empty string")
        if self.inject_params.get("inject_unavailable_" + program, False):
            return program + "-xxx"  # substitute a program that cannot be found on the PATH
        if self.inject_params.get("inject_failing_" + program, False):
            return "false"  # substitute a program that will error out with non-zero return code
        else:
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
               tuple(self.snapshot_filters), self.args.skip_replication,
               self.args.delete_dst_datasets, self.args.delete_dst_snapshots, self.args.delete_empty_dst_datasets,
               self.src.basis_ssh_host, self.dst.basis_ssh_host,
               self.src.basis_ssh_user, self.dst.basis_ssh_user)
        # fmt: on
        hash_code = hashlib.sha256(str(key).encode("utf-8")).hexdigest()
        return os.path.join(tempfile.gettempdir(), f"{prog_name}-lockfile-{hash_code}.lock")

    def dry(self, msg: str) -> str:
        return "Dry " + msg if self.dry_run else msg


#############################################################################
class Remote:
    def __init__(self, loc: str, args: argparse.Namespace, p: Params):
        """Option values for either location=='src' or location=='dst'; reads from ArgumentParser via args."""
        # immutable variables:
        assert loc == "src" or loc == "dst"
        self.location: str = loc
        self.basis_ssh_user: str = getattr(args, f"ssh_{loc}_user")
        self.basis_ssh_host: str = getattr(args, f"ssh_{loc}_host")
        self.ssh_port: int = getattr(args, f"ssh_{loc}_port")
        self.ssh_config_file: str = p.validate_arg(getattr(args, f"ssh_{loc}_config_file"))
        self.ssh_cipher: str = p.validate_arg(args.ssh_cipher)
        self.ssh_private_key_files: List[str] = [p.validate_arg(key) for key in getattr(args, f"ssh_{loc}_private_key")]
        # disable interactive password prompts and X11 forwarding and pseudo-terminal allocation:
        self.ssh_extra_opts: List[str] = ["-oBatchMode=yes", "-oServerAliveInterval=0", "-x", "-T"]
        self.ssh_extra_opts += p.split_args(getattr(args, f"ssh_{loc}_extra_opts"))
        for extra_opt in getattr(args, f"ssh_{loc}_extra_opt"):
            self.ssh_extra_opts.append(p.validate_arg(extra_opt, allow_spaces=True))

        # mutable variables:
        self.root_dataset: str = ""  # deferred until run_main()
        self.basis_root_dataset: str = ""  # deferred until run_main()
        self.pool: str = ""
        self.sudo: str = ""
        self.use_zfs_delegation: bool = False
        self.ssh_cmd: List[str] = []
        self.ssh_cmd_quoted: List[str] = []
        self.ssh_user: str = ""
        self.ssh_host: str = ""
        self.ssh_user_host: str = ""

    def set_ssh_cmd(self, ssh_cmd: List[str]) -> None:
        self.ssh_cmd = ssh_cmd
        self.ssh_cmd_quoted = [shlex.quote(item) for item in ssh_cmd]

    def cache_key(self) -> Tuple:
        # fmt: off
        return (self.pool, self.ssh_user_host, self.ssh_port, self.ssh_config_file, self.ssh_cipher,
                tuple(self.ssh_private_key_files), tuple(self.ssh_extra_opts))
        # fmt: on

    def __repr__(self) -> str:
        return str(self.__dict__)


#############################################################################
class CopyPropertiesConfig:
    def __init__(self, group: str, flag: str, args: argparse.Namespace, p: Params):
        """Option values for --zfs-recv-o* and --zfs-recv-x* option groups; reads from ArgumentParser via args."""
        # immutable variables:
        grup = group
        self.group: str = group
        self.flag: str = flag  # one of -o or -x
        sources: str = p.validate_arg(getattr(args, f"{grup}_sources"))
        self.sources: str = ",".join(sorted([s.strip() for s in sources.strip().split(",")]))  # canonicalize
        self.targets: str = p.validate_arg(getattr(args, f"{grup}_targets"))
        self.include_regexes: RegexList = compile_regexes(getattr(args, f"{grup}_include_regex"))
        self.exclude_regexes: RegexList = compile_regexes(getattr(args, f"{grup}_exclude_regex"))

    def __repr__(self) -> str:
        return str(self.__dict__)


#############################################################################
class RetryPolicy:
    def __init__(self, args: argparse.Namespace, p: Params):
        """Option values for retries; reads from ArgumentParser via args."""
        # immutable variables:
        self.retries: int = args.retries
        self.min_sleep_secs: float = args.retry_min_sleep_secs
        self.max_sleep_secs: float = args.retry_max_sleep_secs
        self.max_elapsed_secs: float = args.retry_max_elapsed_secs
        self.min_sleep_nanos: int = int(self.min_sleep_secs * 1000_000_000)
        self.max_sleep_nanos: int = int(self.max_sleep_secs * 1000_000_000)
        self.max_elapsed_nanos: int = int(self.max_elapsed_secs * 1000_000_000)
        self.min_sleep_nanos = max(1, self.min_sleep_nanos)
        self.max_sleep_nanos = max(self.min_sleep_nanos, self.max_sleep_nanos)

    def __repr__(self) -> str:
        return (
            f"retries: {self.retries}, min_sleep_secs: {self.min_sleep_secs}, "
            f"max_sleep_secs: {self.max_sleep_secs}, max_elapsed_secs: {self.max_elapsed_secs}"
        )


#############################################################################
def main() -> None:
    """API for command line clients."""
    try:
        run_main(argument_parser().parse_args(), sys.argv)
    except subprocess.CalledProcessError as e:
        sys.exit(e.returncode)


def run_main(args: argparse.Namespace, sys_argv: Optional[List[str]] = None, log: Optional[Logger] = None) -> None:
    """API for Python clients; visible for testing; may become a public API eventually."""
    # On CTRL-C send SIGTERM to the entire process group to also terminate child processes started via subprocess.run()
    old_sigint_handler = signal.signal(signal.SIGINT, lambda signum, frame: terminate_process_group())
    try:
        Job().run_main(args, sys_argv, log)
    except BaseException as e:
        terminate_process_group(except_current_process=True)
        raise e
    finally:
        signal.signal(signal.SIGINT, old_sigint_handler)  # restore original signal handler


#############################################################################
class Job:
    def __init__(self):
        self.params: Params
        self.all_dst_dataset_exists: Dict[str, Dict[str, bool]] = defaultdict(lambda: defaultdict(bool))
        self.dst_dataset_exists: Dict[str, bool] = {}
        self.src_properties: Dict[str, Dict[str, str | int]] = {}
        self.mbuffer_current_opts: List[str] = []
        self.all_exceptions: List[str] = []
        self.all_exceptions_count = 0
        self.max_exceptions_to_summarize = 10000
        self.first_exception: Optional[BaseException] = None
        self.remote_conf_cache: Dict[Tuple, Tuple[Dict[str, str], Dict[str, str], List[str]]] = {}
        self.zfs_dataset_busy_if_mods, self.zfs_dataset_busy_if_send = self.get_is_zfs_dataset_busy_regexes()  # Pattern
        self.max_datasets_per_minibatch_on_list_snaps: Dict[str, int] = {}
        self.max_workers: Dict[str, int] = {}
        self.re_suffix = r"(?:/.*)?"  # also match descendants of a matching dataset

        self.is_test_mode: bool = False  # for testing only
        self.creation_prefix = ""  # for testing only
        self.error_injection_triggers: Dict[str, Counter] = {}  # for testing only
        self.delete_injection_triggers: Dict[str, Counter] = {}  # for testing only
        self.param_injection_triggers: Dict[str, Dict[str, bool]] = {}  # for testing only
        self.inject_params: Dict[str, bool] = {}  # for testing only
        self.max_command_line_bytes: Optional[int] = None  # for testing only

    def run_main(self, args: argparse.Namespace, sys_argv: Optional[List[str]] = None, log: Optional[Logger] = None):
        assert isinstance(self.error_injection_triggers, dict)
        assert isinstance(self.delete_injection_triggers, dict)
        assert isinstance(self.inject_params, dict)
        log_params = LogParams(args)
        try:
            log = get_logger(log_params, args, log)
            log.info("%s", "Log file is: " + log_params.log_file)
            log.info("CLI arguments: %s %s", " ".join(sys_argv or []), f"[euid: {os.geteuid()}]")
            log.debug("Parsed CLI arguments: %s", args)
            try:
                self.params = p = Params(args, sys_argv, log_params, log, self.inject_params)
            except SystemExit as e:
                log.error("%s", str(e))
                raise
            with open(log_params.log_file, "a", encoding="utf-8") as log_file_fd:
                with redirect_stderr(Tee(log_file_fd, sys.stderr)):  # send stderr to both logfile and stderr
                    lock_file = p.lock_file_name()
                    with open(lock_file, "w") as lock_fd:
                        try:
                            # Acquire an exclusive lock; will raise an error if lock is already held by another process.
                            # The (advisory) lock is auto-released when the process terminates or the fd is closed.
                            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # LOCK_NB ... non-blocking
                        except BlockingIOError as e:
                            log.error("Exiting as same previous periodic job is still running without completion yet.")
                            raise SystemExit(die_status) from e
                        try:
                            self.run_tasks()
                        finally:
                            unlink_missing_ok(lock_file)  # avoid accumulation of stale lock files
        finally:
            reset_logger()

    def run_tasks(self) -> None:
        p, log = self.params, self.params.log
        try:
            self.validate_once()
            self.all_exceptions = []
            self.all_exceptions_count = 0
            self.first_exception = None
            self.remote_conf_cache = {}
            src, dst = p.src, p.dst
            for src_root_dataset, dst_root_dataset in p.root_dataset_pairs:
                src.root_dataset = src.basis_root_dataset = src_root_dataset
                dst.root_dataset = dst.basis_root_dataset = dst_root_dataset
                p.curr_zfs_send_program_opts = p.zfs_send_program_opts.copy()
                task_description = f"{src.basis_root_dataset} {p.recursive_flag} --> {dst.basis_root_dataset}"
                log.info("Starting task: %s", task_description + " ...")
                try:
                    try:
                        self.maybe_inject_error(cmd=[], error_trigger="retryable_run_tasks")
                        self.validate_task()
                        self.run_task()
                    except RetryableError as retryable_error:
                        raise retryable_error.__cause__
                except (CalledProcessError, TimeoutExpired, SystemExit, UnicodeDecodeError) as e:
                    log.error("%s", str(e))
                    if p.skip_on_error == "fail":
                        raise
                    self.append_exception(e, "task", task_description)
            error_count = self.all_exceptions_count
            if error_count > 0:
                msgs = "\n".join([f"{i + 1}/{error_count}: {e}" for i, e in enumerate(self.all_exceptions)])
                log.error("%s", f"Tolerated {error_count} errors. Error Summary: \n{msgs}")
                raise self.first_exception  # reraise first swallowed exception
        except subprocess.CalledProcessError as e:
            log.error(f"Exiting with status code: {e.returncode}")
            raise
        except SystemExit as e:
            log.error(f"Exiting with status code: {e.code}")
            raise
        except (subprocess.TimeoutExpired, UnicodeDecodeError) as e:
            log.error(f"Exiting with status code: {die_status}")
            raise SystemExit(die_status) from e
        except re.error as e:
            log.error("%s within regex %s", e, e.pattern)
            raise SystemExit(die_status) from e
        finally:
            log.info("%s", "Log file was: " + p.log_params.log_file)

        for line in tail(p.log_params.pv_log_file, 10):
            log.log(log_stdout, "%s", line.rstrip())
        log.info("Success. Goodbye!")
        print("", end="", file=sys.stderr)
        sys.stderr.flush()

    def append_exception(self, e: Exception, task_name: str, task_description: str) -> None:
        self.first_exception = self.first_exception or e
        if len(self.all_exceptions) < self.max_exceptions_to_summarize:
            self.all_exceptions.append(str(e))
        self.all_exceptions_count += 1
        self.params.log.error(f"#{self.all_exceptions_count}: Done with %s: %s", task_name, task_description)

    def validate_once(self) -> None:
        p = self.params
        p.zfs_recv_ox_names = self.recv_option_property_names(p.zfs_recv_program_opts)
        for _filter in p.snapshot_filters:
            if _filter.name == snapshot_regex_filter_name:
                exclude_snapshot_regexes = compile_regexes(_filter.options[0])
                include_snapshot_regexes = compile_regexes(_filter.options[1] or [".*"])
                _filter.options = (exclude_snapshot_regexes, include_snapshot_regexes)

        exclude_regexes = [exclude_dataset_regexes_default]
        if len(p.args.exclude_dataset_regex) > 0:  # some patterns don't exclude anything
            exclude_regexes = [regex for regex in p.args.exclude_dataset_regex if regex != "" and regex != "!.*"]
        include_regexes = p.args.include_dataset_regex

        # relative datasets need not be compiled more than once as they don't change between tasks
        def separate_abs_vs_rel_datasets(datasets: List[str]) -> Tuple[List[str], List[str]]:
            abs_datasets, rel_datasets = [], []
            for dataset in datasets:
                (abs_datasets if dataset.startswith("/") else rel_datasets).append(dataset)
            return abs_datasets, rel_datasets

        p.abs_exclude_datasets, rel_exclude_datasets = separate_abs_vs_rel_datasets(p.args.exclude_dataset)
        p.abs_include_datasets, rel_include_datasets = separate_abs_vs_rel_datasets(p.args.include_dataset)
        p.tmp_exclude_dataset_regexes, p.tmp_include_dataset_regexes = (
            compile_regexes(exclude_regexes + self.dataset_regexes(rel_exclude_datasets), suffix=self.re_suffix),
            compile_regexes(include_regexes + self.dataset_regexes(rel_include_datasets), suffix=self.re_suffix),
        )

    def validate_task(self) -> None:
        p, log = self.params, self.params.log
        src, dst = p.src, p.dst
        for remote in [src, dst]:
            r, loc = remote, remote.location
            validate_user_name(r.basis_ssh_user, f"--ssh-{loc}-user")
            validate_host_name(r.basis_ssh_host, f"--ssh-{loc}-host")
            validate_port(r.ssh_port, f"--ssh-{loc}-port ")
            r.ssh_user, r.ssh_host, r.ssh_user_host, r.pool, r.root_dataset = parse_dataset_locator(
                r.basis_root_dataset, user=r.basis_ssh_user, host=r.basis_ssh_host, port=r.ssh_port
            )
            r.sudo, r.use_zfs_delegation = self.sudo_cmd(r.ssh_user_host, r.ssh_user)
        self.dst_dataset_exists = self.all_dst_dataset_exists[dst.ssh_user_host]  # returns False for absent keys

        if src.ssh_host == dst.ssh_host:
            if src.root_dataset == dst.root_dataset:
                die(
                    "Source and destination dataset must not be the same! "
                    f"src: {src.basis_root_dataset}, dst: {dst.basis_root_dataset}"
                )
            if p.recursive and (
                is_descendant(src.root_dataset, of_root_dataset=dst.root_dataset)
                or is_descendant(dst.root_dataset, of_root_dataset=src.root_dataset)
            ):
                die(
                    "Source and destination dataset trees must not overlap! "
                    f"src: {src.basis_root_dataset}, dst: {dst.basis_root_dataset}"
                )

        suffx = self.re_suffix  # also match descendants of a matching dataset
        p.exclude_dataset_regexes, p.include_dataset_regexes = (
            p.tmp_exclude_dataset_regexes + compile_regexes(self.dataset_regexes(p.abs_exclude_datasets), suffix=suffx),
            p.tmp_include_dataset_regexes + compile_regexes(self.dataset_regexes(p.abs_include_datasets), suffix=suffx),
        )
        if len(p.include_dataset_regexes) == 0:
            p.include_dataset_regexes = compile_regexes([".*"], suffix=suffx)

        self.detect_available_programs()

        zfs_send_program_opts = p.curr_zfs_send_program_opts
        if self.is_zpool_feature_enabled_or_active(dst, "feature@large_blocks"):
            append_if_absent(zfs_send_program_opts, "--large-block")  # solaris-11.4 does not have this feature
        if self.is_solaris_zfs(dst):
            p.dry_run_destroy = ""  # solaris-11.4 knows no 'zfs destroy -n' flag
            p.verbose_destroy = ""  # solaris-11.4 knows no 'zfs destroy -v' flag
        if self.is_solaris_zfs(src):  # solaris-11.4 only knows -w compress
            zfs_send_program_opts = ["-p" if opt == "--props" else opt for opt in zfs_send_program_opts]
            zfs_send_program_opts = fix_solaris_raw_mode(zfs_send_program_opts)
        p.curr_zfs_send_program_opts = zfs_send_program_opts

        self.max_workers = {}
        self.max_datasets_per_minibatch_on_list_snaps = {}
        for r in [src, dst]:
            cpus = int(p.available_programs[r.location].get("getconf_cpu_count", 8))
            threads, is_percent = p.threads
            cpus = max(1, round(cpus * threads / 100.0) if is_percent else round(threads))
            # ssh default is max 10 multiplexed sessions over the same TCP connection per sshd_config(5) MaxSessions
            cpus = cpus if not r.ssh_user_host else min(cpus, 8 if src.cache_key() != dst.cache_key() else 4)
            self.max_workers[r.location] = cpus
            bs = max(1, p.max_datasets_per_batch_on_list_snaps)  # 1024 by default
            max_datasets_per_minibatch = p.max_datasets_per_minibatch_on_list_snaps
            if max_datasets_per_minibatch <= 0:
                max_datasets_per_minibatch = max(1, bs // cpus if r.ssh_user_host else bs // cpus // 8)
            max_datasets_per_minibatch = min(bs, max_datasets_per_minibatch)
            self.max_datasets_per_minibatch_on_list_snaps[r.location] = max_datasets_per_minibatch
            log.trace(
                "%s",
                f"max_datasets_per_batch_on_list_snaps: {p.max_datasets_per_batch_on_list_snaps}, "
                f"max_datasets_per_minibatch_on_list_snaps: {max_datasets_per_minibatch}, "
                f"max_workers: {self.max_workers[r.location]}, "
                f"location: {r.location}",
            )
        log.trace("Validated Param values: %s", pretty_print_formatter(self.params))

    def sudo_cmd(self, ssh_user_host: str, ssh_user: str) -> Tuple[str, bool]:
        is_root = True
        if ssh_user_host != "":
            if not ssh_user:
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
        elif self.params.enable_privilege_elevation:
            if self.params.sudo_program == disable_prg:
                die(f"sudo CLI is not available on host: {ssh_user_host or 'localhost'}")
            return self.params.sudo_program, False
        else:
            return "", True

    def process_datasets_fault_tolerant(
        self,
        datasets: List[str],
        process_dataset: Callable[[str], bool],  # lambda
        skip_tree_on_error: Callable[[str], bool],  # lambda
        task_name: str,
    ) -> bool:
        """Runs process_dataset(dataset) for each dataset in datasets, while taking care of error handling + retries.
        Assumes that the input dataset list is sorted."""
        p, log = self.params, self.params.log
        log.trace("Retry policy: %s", p.retry_policy)
        failed = False
        skip_dataset = DONT_SKIP_DATASET
        for dataset in datasets:
            if is_descendant(dataset, of_root_dataset=skip_dataset):
                # skip_dataset has been deleted by some third party while we're running
                continue  # nothing to do anymore for this dataset subtree (note that datasets is sorted)
            skip_dataset = DONT_SKIP_DATASET
            start_time_nanos = time.time_ns()
            try:
                if not self.run_with_retries(p.retry_policy, lambda: process_dataset(dataset)):
                    skip_dataset = dataset
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, SystemExit, UnicodeDecodeError) as e:
                failed = True
                if p.skip_on_error == "fail":
                    raise
                elif p.skip_on_error == "tree" or skip_tree_on_error(dataset):
                    skip_dataset = dataset
                log.error("%s", str(e))
                self.append_exception(e, task_name, dataset)
            finally:
                elapsed_nanos = int(time.time_ns() - start_time_nanos)
                log.debug(task_name + " done: %s took %s", dataset, human_readable_duration(elapsed_nanos))
        return failed

    def run_task(self) -> None:
        p, log = self.params, self.params.log
        src, dst = p.src, p.dst
        task_description = f"{src.basis_root_dataset} {p.recursive_flag} --> {dst.basis_root_dataset} ..."

        # find src dataset or all datasets in src dataset tree (with --recursive)
        cmd = p.split_args(
            f"{p.zfs_program} list -t filesystem,volume -Hp -o volblocksize,recordsize,name -s name {p.recursive_flag}",
            src.root_dataset,
        )
        src_datasets_with_sizes = []
        if not self.is_dummy_src(src):
            src_datasets_with_sizes = (self.try_ssh_command(src, log_debug, cmd=cmd) or "").splitlines()
        src_datasets = []
        src_properties = {}
        for line in src_datasets_with_sizes:
            volblocksize, recordsize, src_dataset = line.split("\t", 2)
            src_properties[src_dataset] = {"recordsize": int(recordsize) if recordsize != "-" else -int(volblocksize)}
            src_datasets.append(src_dataset)
        self.src_properties = src_properties
        src_datasets_with_sizes = None  # help gc
        selected_src_datasets = None
        failed = False

        # Optionally, replicate src.root_dataset (optionally including its descendants) to dst.root_dataset
        if not p.skip_replication:
            log.info("Starting replication task: %s", task_description)
            if len(src_datasets) == 0:
                die(f"Source dataset does not exist: {src.basis_root_dataset}")
            selected_src_datasets = isorted(self.filter_datasets(src, src_datasets))  # apply include/exclude policy
            failed = self.process_datasets_fault_tolerant(
                selected_src_datasets,
                process_dataset=lambda dataset: self.replicate_dataset(dataset),
                skip_tree_on_error=lambda dataset: not self.dst_dataset_exists[
                    replace_prefix(dataset, old_prefix=src.root_dataset, new_prefix=dst.root_dataset)
                ],
                task_name="Replicating",
            )

        if failed or not (
            p.delete_dst_datasets or p.delete_dst_snapshots or p.delete_empty_dst_datasets or p.compare_snapshot_lists
        ):
            return
        log.info("Listing dst datasets: %s", task_description)
        cmd = p.split_args(f"{p.zfs_program} list -t filesystem,volume -Hp -o name", p.recursive_flag, dst.root_dataset)
        basis_dst_datasets = self.try_ssh_command(dst, log_trace, cmd=cmd)
        if basis_dst_datasets is None:
            log.warning("Destination dataset does not exist: %s", dst.root_dataset)
            basis_dst_datasets = ""
        basis_dst_datasets = basis_dst_datasets.splitlines()
        dst_datasets = isorted(self.filter_datasets(dst, basis_dst_datasets))  # apply include/exclude policy

        # Optionally, delete existing destination datasets that do not exist within the source dataset if they are
        # included via --{include|exclude}-dataset* policy. Does not recurse without --recursive.
        if p.delete_dst_datasets and not failed:
            log.info(p.dry("--delete-dst-datasets: %s"), task_description)
            dst_datasets = set(dst_datasets)
            to_delete = dst_datasets.difference(
                {replace_prefix(src_dataset, src.root_dataset, dst.root_dataset) for src_dataset in src_datasets}
            )
            self.delete_datasets(dst, to_delete)
            dst_datasets = isorted(dst_datasets.difference(to_delete))

        # Optionally, delete existing destination snapshots that do not exist within the source dataset if they
        # are included by the --{include|exclude}-snapshot-* policy, and the destination dataset is included
        # via --{include|exclude}-dataset* policy.
        if p.delete_dst_snapshots and not failed:
            log.info(p.dry("--delete-dst-snapshots: %s"), task_description)
            kind = "bookmark" if p.delete_dst_bookmarks else "snapshot"
            filter_needs_creation_time = has_timerange_filter(p.snapshot_filters)
            props = self.creation_prefix + "creation,guid,name" if filter_needs_creation_time else "guid,name"
            src_datasets_set = set(src_datasets)

            def delete_destination_snapshots(dst_dataset: str) -> bool:
                cmd = p.split_args(f"{p.zfs_program} list -t {kind} -d 1 -s createtxg -Hp -o {props}", dst_dataset)
                self.maybe_inject_delete(dst, dataset=dst_dataset, delete_trigger="zfs_list_delete_dst_snapshots")
                dst_snaps_with_guids = self.try_ssh_command(dst, log_trace, cmd=cmd)
                if dst_snaps_with_guids is None:
                    log.warning("Third party deleted destination: %s", dst_dataset)
                    return False
                dst_snaps_with_guids = dst_snaps_with_guids.splitlines()
                if p.delete_dst_bookmarks:
                    replace_in_lines(dst_snaps_with_guids, old="#", new="@")  # treat bookmarks as snapshots
                dst_snaps_with_guids = self.filter_snapshots(dst_snaps_with_guids)  # apply include/exclude
                if p.delete_dst_bookmarks:
                    replace_in_lines(dst_snaps_with_guids, old="@", new="#")  # restore pre-filtering bookmark state
                if filter_needs_creation_time:
                    dst_snaps_with_guids = cut(field=2, lines=dst_snaps_with_guids)
                src_dataset = replace_prefix(dst_dataset, old_prefix=dst.root_dataset, new_prefix=src.root_dataset)
                if src_dataset in src_datasets_set and (self.are_bookmarks_enabled(src) or not p.delete_dst_bookmarks):
                    src_kind = kind
                    if not p.delete_dst_snapshots_no_crosscheck:
                        src_kind = "snapshot,bookmark" if self.are_bookmarks_enabled(src) else "snapshot"
                    cmd = p.split_args(f"{p.zfs_program} list -t {src_kind} -d 1 -s name -Hp -o guid", src_dataset)
                    src_snapshots_with_guids = self.run_ssh_command(src, log_trace, cmd=cmd).splitlines()
                    missing_snapshot_guids = set(cut(field=1, lines=dst_snaps_with_guids)).difference(
                        set(src_snapshots_with_guids)
                    )
                    missing_snapshot_tags = self.filter_lines(dst_snaps_with_guids, missing_snapshot_guids)
                else:
                    missing_snapshot_tags = dst_snaps_with_guids
                separator = "#" if p.delete_dst_bookmarks else "@"
                missing_snapshot_tags = cut(field=2, separator=separator, lines=missing_snapshot_tags)
                if p.delete_dst_bookmarks:
                    self.delete_bookmarks(dst, dst_dataset, snapshot_tags=missing_snapshot_tags)
                else:
                    self.delete_snapshots(dst, dst_dataset, snapshot_tags=missing_snapshot_tags)
                return True

            if self.are_bookmarks_enabled(dst) or not p.delete_dst_bookmarks:
                failed = self.process_datasets_fault_tolerant(
                    dst_datasets,
                    process_dataset=lambda dataset: delete_destination_snapshots(dataset),
                    skip_tree_on_error=lambda dataset: False,
                    task_name="--delete-dst-snapshots",
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
                p.delete_empty_dst_datasets_if_no_bookmarks_and_no_snapshots and self.are_bookmarks_enabled(dst)
            )

            # Compute the direct children of each NON-FILTERED dataset. Thus, no excluded dataset and no ancestor of
            # an excluded dataset will ever be added to the "orphan" set. In other words, this treats excluded dataset
            # subtrees as if they all had snapshots, so excluded dataset subtrees and their ancestors are guaranteed
            # to not get deleted.
            children = defaultdict(list)
            for dst_dataset in basis_dst_datasets:
                parent = os.path.dirname(dst_dataset)
                children[parent].append(dst_dataset)

            # find datasets that have at least one snapshot
            dst_datasets_having_snapshots: Set[str] = set()
            btype = "bookmark,snapshot" if delete_empty_dst_datasets_if_no_bookmarks_and_no_snapshots else "snapshot"
            cmd = p.split_args(f"{p.zfs_program} list -t {btype} -d 1 -S name -Hp -o name")

            # Find and mark orphan datasets, finally delete them in an efficient way. Using two filter runs instead of
            # one filter run is an optimization. The first run only computes candidate orphans, without incurring I/O,
            # to reduce the list of datasets for which we list snapshots via 'zfs list -t snapshot ...' from
            # dst_datasets to a subset of dst_datasets, which in turn reduces I/O and improves perf. Essentially, this
            # eliminates the I/O to list snapshots for ancestors of excluded datasets. The second run computes the
            # real orphans.
            for run in range(0, 2):
                orphans: Set[str] = set()
                for dst_dataset in reversed(dst_datasets):
                    if not any(filter(lambda child: child not in orphans, children[dst_dataset])):
                        # all children turned out to be orphans so the dataset itself could be an orphan
                        if dst_dataset not in dst_datasets_having_snapshots:  # always True during first filter run
                            orphans.add(dst_dataset)
                if run == 0:
                    # update dst_datasets_having_snapshots for real use in the second run
                    for datasets_having_snapshots in self.itr_ssh_command_parallel(dst, cmd, isorted(orphans)):
                        if delete_empty_dst_datasets_if_no_bookmarks_and_no_snapshots:
                            replace_in_lines(datasets_having_snapshots, old="#", new="@")  # treat bookmarks as snaps
                        datasets_having_snapshots = set(cut(field=1, separator="@", lines=datasets_having_snapshots))
                        dst_datasets_having_snapshots.update(datasets_having_snapshots)  # union
                else:
                    self.delete_datasets(dst, orphans)
                    dst_datasets = isorted(set(dst_datasets).difference(orphans))

        if p.compare_snapshot_lists and not failed:
            log.info("--compare-snapshot-lists: %s", task_description)
            if len(src_datasets) == 0 and not self.is_dummy_src(src):
                die(f"Source dataset does not exist: {src.basis_root_dataset}")
            if selected_src_datasets is None:
                selected_src_datasets = self.filter_datasets(src, src_datasets)  # apply include/exclude policy
            self.run_compare_snapshot_lists(selected_src_datasets, dst_datasets)

    def replicate_dataset(self, src_dataset: str) -> bool:
        """Replicates src_dataset (without handling descendants) to dst_dataset."""

        p, log = self.params, self.params.log
        src, dst = p.src, p.dst
        dst_dataset = replace_prefix(src_dataset, old_prefix=src.root_dataset, new_prefix=dst.root_dataset)
        log.debug(p.dry("Replicating: %s"), f"{src_dataset} --> {dst_dataset} ...")

        # list GUID and name for dst snapshots, sorted ascending by createtxg (more precise than creation time)
        cmd = p.split_args(f"{p.zfs_program} list -t snapshot -d 1 -s createtxg -Hp -o guid,name", dst_dataset)
        dst_snapshots_with_guids = self.try_ssh_command(dst, log_trace, cmd=cmd, error_trigger="zfs_list_snapshot_dst")
        self.dst_dataset_exists[dst_dataset] = dst_snapshots_with_guids is not None
        dst_snapshots_with_guids = dst_snapshots_with_guids.splitlines() if dst_snapshots_with_guids is not None else []
        use_bookmark = p.use_bookmark and self.are_bookmarks_enabled(src) and len(dst_snapshots_with_guids) > 0
        filter_needs_creation_time = has_timerange_filter(p.snapshot_filters)

        # list GUID and name for src snapshots + bookmarks, primarily sort ascending by transaction group (which is more
        # precise than creation time), secondarily sort such that snapshots appear after bookmarks for the same GUID.
        # Note: A snapshot and its ZFS bookmarks always have the same GUID, creation time and transaction group.
        # A snapshot changes its transaction group but retains its creation time and GUID on 'zfs receive' on another
        # pool, i.e. comparing createtxg is only meaningful within a single pool, not across pools from src to dst.
        # Comparing creation time remains meaningful across pools from src to dst. Creation time is a UTC Unix time
        # in integer seconds.
        # Note that 'zfs create', 'zfs snapshot' and 'zfs bookmark' CLIs enforce that snapshot names must not
        # contain a '#' char, bookmark names must not contain a '@' char, and dataset names must not
        # contain a '#' or '@' char. GUID and creation time also do not contain a '#' or '@' char.
        types = "snapshot,bookmark" if use_bookmark else "snapshot"
        props = self.creation_prefix + "creation,guid,name" if filter_needs_creation_time else "guid,name"
        self.maybe_inject_delete(src, dataset=src_dataset, delete_trigger="zfs_list_snapshot_src")
        cmd = p.split_args(f"{p.zfs_program} list -t {types} -s createtxg -s type -d 1 -Hp -o {props}", src_dataset)
        src_snapshots_and_bookmarks = self.try_ssh_command(src, log_trace, cmd=cmd)
        if src_snapshots_and_bookmarks is None:
            log.warning("Third party deleted source: %s", src_dataset)
            return False  # src dataset has been deleted by some third party while we're running - nothing to do anymore
        src_snapshots_with_guids: List[str] = src_snapshots_and_bookmarks.splitlines()
        src_snapshots_and_bookmarks = None

        # apply include/exclude regexes to ignore irrelevant src snapshots
        basis_src_snapshots_with_guids = src_snapshots_with_guids
        src_snapshots_with_guids = self.filter_snapshots(src_snapshots_with_guids)
        if filter_needs_creation_time:
            src_snapshots_with_guids = cut(field=2, lines=src_snapshots_with_guids)
            basis_src_snapshots_with_guids = cut(field=2, lines=basis_src_snapshots_with_guids)

        # find oldest and latest "true" snapshot, as well as GUIDs of all snapshots and bookmarks.
        # a snapshot is "true" if it is not a bookmark.
        oldest_src_snapshot = ""
        latest_src_snapshot = ""
        included_src_guids: Set[str] = set()
        for line in src_snapshots_with_guids:
            guid, snapshot = line.split("\t", 1)
            included_src_guids.add(guid)
            if "@" in snapshot:
                latest_src_snapshot = snapshot
                if not oldest_src_snapshot:
                    oldest_src_snapshot = snapshot
        if len(src_snapshots_with_guids) == 0:
            if p.skip_missing_snapshots == "fail":
                die(
                    f"Found source dataset that includes no snapshot: {src_dataset}. Consider "
                    "using --skip-missing-snapshots=dataset"
                )
            elif p.skip_missing_snapshots == "dataset":
                log.warning("Skipping source dataset because it includes no snapshot: %s", src_dataset)
                if not self.dst_dataset_exists[dst_dataset] and p.recursive:
                    log.warning("Also skipping descendant datasets as dst dataset does not exist:%s", src_dataset)
                return self.dst_dataset_exists[dst_dataset]

        log.debug("latest_src_snapshot: %s", latest_src_snapshot)
        latest_dst_snapshot = ""
        latest_dst_guid = ""
        latest_common_src_snapshot = ""
        props_cache = {}
        done_checking = False

        if self.dst_dataset_exists[dst_dataset]:
            if len(dst_snapshots_with_guids) > 0:
                latest_dst_guid, latest_dst_snapshot = dst_snapshots_with_guids[-1].split("\t", 1)
                if p.force_rollback_to_latest_snapshot or p.force:
                    log.info(p.dry("Rolling back destination to most recent snapshot: %s"), latest_dst_snapshot)
                    # rollback just in case the dst dataset was modified since its most recent snapshot
                    done_checking = done_checking or self.check_zfs_dataset_busy(dst, dst_dataset)
                    cmd = p.split_args(f"{dst.sudo} {p.zfs_program} rollback", latest_dst_snapshot)
                    self.try_ssh_command(dst, log_debug, is_dry=p.dry_run, print_stdout=True, cmd=cmd, exists=False)
            elif latest_src_snapshot == "":
                log.info("Already-up-to-date: %s", dst_dataset)
                return True

            # find most recent snapshot (or bookmark) that src and dst have in common - we'll start to replicate
            # from there up to the most recent src snapshot. any two snapshots are "common" iff their ZFS GUIDs (i.e.
            # contents) are equal. See https://github.com/openzfs/zfs/commit/305bc4b370b20de81eaf10a1cf724374258b74d1
            def latest_common_snapshot(snapshots_with_guids: List[str], intersect_guids: Set[str]) -> Tuple[str, str]:
                """Returns a true snapshot instead of its bookmark with the same GUID, per the sort order previously
                used for 'zfs list -s ...'"""
                for _line in reversed(snapshots_with_guids):
                    _guid, _snapshot = _line.split("\t", 1)
                    if _guid in intersect_guids:
                        return _guid, _snapshot  # can be a snapshot or bookmark
                return None, ""

            latest_common_guid, latest_common_src_snapshot = latest_common_snapshot(
                src_snapshots_with_guids, set(cut(field=1, lines=dst_snapshots_with_guids))
            )
            log.debug("latest_common_src_snapshot: %s", latest_common_src_snapshot)  # is a snapshot or bookmark
            log.trace("latest_dst_snapshot: %s", latest_dst_snapshot)

            if latest_common_src_snapshot and latest_common_guid != latest_dst_guid:
                # found latest common snapshot but dst has an even newer snapshot. rollback dst to that common snapshot.
                _, latest_common_dst_snapshot = latest_common_snapshot(dst_snapshots_with_guids, {latest_common_guid})
                if not (p.force_rollback_to_latest_common_snapshot or p.force):
                    die(
                        f"Conflict: Most recent destination snapshot {latest_dst_snapshot} is more recent than "
                        f"most recent common snapshot {latest_common_dst_snapshot}. Rollback destination first, "
                        "for example via --force-rollback-to-latest-common-snapshot (or --force) option."
                    )
                if p.force_once:
                    p.force = False
                    p.force_rollback_to_latest_common_snapshot = False
                log.info(
                    p.dry("Rolling back destination to most recent common snapshot: %s"), latest_common_dst_snapshot
                )
                done_checking = done_checking or self.check_zfs_dataset_busy(dst, dst_dataset)
                cmd = p.split_args(
                    f"{dst.sudo} {p.zfs_program} rollback -r {p.force_unmount} {p.force_hard}",
                    latest_common_dst_snapshot,
                )
                self.run_ssh_command(dst, log_debug, is_dry=p.dry_run, print_stdout=True, cmd=cmd)

            if latest_src_snapshot and latest_src_snapshot == latest_common_src_snapshot:
                log.info("Already up-to-date: %s", dst_dataset)
                return True

        # endif self.dst_dataset_exists[dst_dataset]
        log.debug("latest_common_src_snapshot: %s", latest_common_src_snapshot)  # is a snapshot or bookmark
        log.trace("latest_dst_snapshot: %s", latest_dst_snapshot)
        self.mbuffer_current_opts = ["-s", str(max(128 * 1024, abs(self.src_properties[src_dataset]["recordsize"])))]
        self.mbuffer_current_opts += p.mbuffer_program_opts
        dry_run_no_send = False
        right_just = 8

        def format_size(num_bytes: int) -> str:
            return human_readable_bytes(num_bytes, long=False).rjust(right_just)

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
                    p.force = False
                done_checking = done_checking or self.check_zfs_dataset_busy(dst, dst_dataset)
                if self.is_solaris_zfs(dst):
                    # solaris-11.4 has no wildcard syntax to delete all snapshots in a single CLI invocation
                    self.delete_snapshots(
                        dst, dst_dataset, snapshot_tags=cut(2, separator="@", lines=dst_snapshots_with_guids)
                    )
                else:
                    cmd = p.split_args(
                        f"{dst.sudo} {p.zfs_program} destroy {p.force_hard} {p.verbose_destroy} {p.dry_run_destroy}",
                        f"{dst_dataset}@%",
                    )  # delete all dst snapshots in a batch
                    self.run_ssh_command(dst, log_debug, cmd=cmd, print_stdout=True)
                if p.dry_run:
                    # As we're in --dryrun (--force) mode this conflict resolution step (see above) wasn't really
                    # executed: "no common snapshot was found. delete all dst snapshots". In turn, this would cause the
                    # subsequent 'zfs receive -n' to fail with "cannot receive new filesystem stream: destination has
                    # snapshots; must destroy them to overwrite it". So we skip the zfs send/receive step and keep on
                    # trucking.
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
                            self.create_filesystem(dst_dataset_parent)

                recv_resume_token, send_resume_opts, recv_resume_opts = self.get_receive_resume_token(dst_dataset)
                curr_size = self.estimate_send_size(src, dst_dataset, recv_resume_token, oldest_src_snapshot)
                human_size = format_size(curr_size)
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
                log.info(
                    p.dry("Full zfs send: %s"), f"{oldest_src_snapshot} --> {dst_dataset} ({human_size.strip()}) ..."
                )
                done_checking = done_checking or self.check_zfs_dataset_busy(dst, dst_dataset)
                dry_run_no_send = dry_run_no_send or p.dry_run_no_send
                self.maybe_inject_params(error_trigger="full_zfs_send_params")
                human_size = human_size.rjust(right_just * 3 + 2)
                self.run_zfs_send_receive(
                    dst_dataset, send_cmd, recv_cmd, curr_size, human_size, dry_run_no_send, "full_zfs_send"
                )
                latest_common_src_snapshot = oldest_src_snapshot  # we have now created a common snapshot
                if not dry_run_no_send and not p.dry_run:
                    self.dst_dataset_exists[dst_dataset] = True
                self.create_zfs_bookmark(src, oldest_src_snapshot, src_dataset)
                self.zfs_set(set_opts, dst, dst_dataset)

        # endif not latest_common_src_snapshot
        # finally, incrementally replicate all snapshots from most recent common snapshot until most recent src snapshot
        if latest_common_src_snapshot:

            def replication_candidates() -> Tuple[List[str], List[str]]:
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
                            # only appends the src bookmark if it has no snapshot. If the bookmark has a snap then that
                            # snap has already been appended, per the sort order previously used for 'zfs list -s ...'
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

            recv_resume_token, send_resume_opts, recv_resume_opts = self.get_receive_resume_token(dst_dataset)
            recv_opts = p.zfs_recv_program_opts.copy() + recv_resume_opts
            recv_opts, set_opts = self.add_recv_property_options(False, recv_opts, src_dataset, props_cache)
            if p.no_stream:
                # skip intermediate snapshots
                steps_todo = [("-i", latest_common_src_snapshot, latest_src_snapshot)]
            else:
                # include intermediate src snapshots that pass --{include,exclude}-snapshot-* policy, using
                # a series of -i/-I send/receive steps that skip excluded src snapshots.
                steps_todo = self.incremental_send_steps_wrapper(
                    cand_snapshots, cand_guids, included_src_guids, recv_resume_token is not None
                )
            log.trace("steps_todo: %s", list_formatter(steps_todo, "; "))
            estimate_send_sizes = [
                self.estimate_send_size(
                    src, dst_dataset, recv_resume_token if i == 0 else None, incr_flag, from_snap, to_snap
                )
                for i, (incr_flag, from_snap, to_snap) in enumerate(steps_todo)
            ]
            total_size = sum(estimate_send_sizes)
            done_size = 0
            for i, (incr_flag, from_snap, to_snap) in enumerate(steps_todo):
                curr_size = estimate_send_sizes[i]
                human_size = format_size(total_size) + "/" + format_size(done_size) + "/" + format_size(curr_size)
                if recv_resume_token:
                    send_opts = send_resume_opts  # e.g. ["-t", "1-c740b4779-..."]
                else:
                    send_opts = p.curr_zfs_send_program_opts + [incr_flag, from_snap, to_snap]
                send_cmd = p.split_args(f"{src.sudo} {p.zfs_program} send", send_opts)
                recv_cmd = p.split_args(
                    f"{dst.sudo} {p.zfs_program} receive", p.dry_run_recv, recv_opts, dst_dataset, allow_all=True
                )
                log.info(
                    p.dry(f"Incremental zfs send {incr_flag}: %s"),
                    f"{from_snap} {to_snap} --> {dst_dataset} ({human_size.strip()}) ...",
                )
                done_checking = done_checking or self.check_zfs_dataset_busy(dst, dst_dataset, busy_if_send=False)
                if p.dry_run and not self.dst_dataset_exists[dst_dataset]:
                    dry_run_no_send = True
                dry_run_no_send = dry_run_no_send or p.dry_run_no_send
                self.maybe_inject_params(error_trigger="incr_zfs_send_params")
                self.run_zfs_send_receive(
                    dst_dataset, send_cmd, recv_cmd, curr_size, human_size, dry_run_no_send, "incr_zfs_send"
                )
                done_size += curr_size
                recv_resume_token = None
                if i == len(steps_todo) - 1:
                    self.create_zfs_bookmark(src, to_snap, src_dataset)
            self.zfs_set(set_opts, dst, dst_dataset)
        return True

    def run_zfs_send_receive(
        self,
        dst_dataset: str,
        send_cmd: List[str],
        recv_cmd: List[str],
        size_estimate_bytes: int,
        size_estimate_human: str,
        dry_run_no_send: bool,
        error_trigger: Optional[str] = None,
    ) -> None:
        p, log = self.params, self.params.log
        send_cmd = " ".join([shlex.quote(item) for item in send_cmd])
        recv_cmd = " ".join([shlex.quote(item) for item in recv_cmd])

        if self.is_program_available("zstd", "src") and self.is_program_available("zstd", "dst"):
            _compress_cmd = self.compress_cmd("src", size_estimate_bytes)
            _decompress_cmd = self.decompress_cmd("dst", size_estimate_bytes)
        else:  # no compression is used if source and destination do not both support compression
            _compress_cmd, _decompress_cmd = "cat", "cat"

        src_buffer = self.mbuffer_cmd("src", size_estimate_bytes)
        dst_buffer = self.mbuffer_cmd("dst", size_estimate_bytes)
        local_buffer = self.mbuffer_cmd("local", size_estimate_bytes)

        pv_src_cmd = ""
        pv_dst_cmd = ""
        pv_loc_cmd = ""
        if p.src.ssh_user_host == "":
            pv_src_cmd = self.pv_cmd("local", size_estimate_bytes, size_estimate_human)
        elif p.dst.ssh_user_host == "":
            pv_dst_cmd = self.pv_cmd("local", size_estimate_bytes, size_estimate_human)
        elif _compress_cmd == "cat":
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
        if _compress_cmd != "cat":
            src_pipe = f"{src_pipe} | {_compress_cmd}"
        if src_buffer != "cat":
            src_pipe = f"{src_pipe} | {src_buffer}"
        if src_pipe.startswith(" |"):
            src_pipe = src_pipe[2:]  # strip leading ' |' part
        if self.inject_params.get("inject_src_send_error", False):
            send_cmd = f"{send_cmd} --injectedGarbageParameter"  # for testing; induce CLI parse error
        if src_pipe != "":
            src_pipe = f"{send_cmd} | {src_pipe}"
            if len(p.src.ssh_cmd) > 0:
                src_pipe = p.shell_program + " -c " + self.dquote(src_pipe)
        else:
            src_pipe = send_cmd

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
        if _decompress_cmd != "cat":
            dst_pipe = f"{dst_pipe} | {_decompress_cmd}"
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
            recv_cmd = f"{recv_cmd} --injectedGarbageParameter"  # for testing; induce CLI parse error
        if dst_pipe != "":
            dst_pipe = f"{dst_pipe} | {recv_cmd}"
            if len(p.dst.ssh_cmd) > 0:
                dst_pipe = p.shell_program + " -c " + self.dquote(dst_pipe)
        else:
            dst_pipe = recv_cmd

        # If there's no support for shell pipelines, we can't do compression, mbuffering, monitoring and rate-limiting,
        # so we fall back to simple zfs send/receive.
        if not self.is_program_available("sh", "src"):
            src_pipe = send_cmd
        if not self.is_program_available("sh", "dst"):
            dst_pipe = recv_cmd
        if not self.is_program_available("sh", "local"):
            local_pipe = ""

        src_pipe = self.squote(p.src.ssh_cmd, src_pipe)
        dst_pipe = self.squote(p.dst.ssh_cmd, dst_pipe)
        src_ssh_cmd = " ".join(p.src.ssh_cmd_quoted)
        dst_ssh_cmd = " ".join(p.dst.ssh_cmd_quoted)

        cmd = [p.shell_program_local, "-c", f"{src_ssh_cmd} {src_pipe} {local_pipe} | {dst_ssh_cmd} {dst_pipe}"]
        msg = "Would execute: %s" if dry_run_no_send else "Executing: %s"
        log.debug(msg, cmd[2].lstrip())
        if not dry_run_no_send:
            try:
                self.maybe_inject_error(cmd=cmd, error_trigger=error_trigger)
                process = subprocess.run(cmd, stdout=PIPE, stderr=PIPE, text=True, check=True)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, UnicodeDecodeError) as e:
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

    def clear_resumable_recv_state_if_necessary(self, dst_dataset: str, stderr: str) -> bool:
        def clear_resumable_recv_state() -> bool:
            log.warning(
                p.dry("Aborting an interrupted zfs receive -s, deleting partially received state: %s"), dst_dataset
            )
            cmd = p.split_args(f"{p.dst.sudo} {p.zfs_program} receive -A", dst_dataset)
            self.try_ssh_command(p.dst, log_trace, is_dry=p.dry_run, print_stdout=True, cmd=cmd)
            log.trace(p.dry("Done Aborting an interrupted zfs receive -s: %s"), dst_dataset)
            return True

        p, log = self.params, self.params.log
        # "cannot resume send: 'wb_src/tmp/src@s1' is no longer the same snapshot used in the initial send"
        # "cannot resume send: 'wb_src/tmp/src@s1' used in the initial send no longer exists"
        # "cannot resume send: incremental source 0xa000000000000000 no longer exists"
        if "cannot resume send" in stderr and (
            "is no longer the same snapshot used in the initial send" in stderr
            or "used in the initial send no longer exists" in stderr
            or re.match(r"incremental source [0-9a-fx]+ no longer exists", stderr)
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

        return False

    def get_receive_resume_token(self, dst_dataset: str) -> Tuple[Optional[str], List[str], List[str]]:
        """Get receive_resume_token ZFS property from dst_dataset and return corresponding opts to use for send+recv"""
        p, log = self.params, self.params.log
        if not p.resume_recv:
            return None, [], []
        warning = None
        if not self.is_zpool_feature_enabled_or_active(p.dst, "feature@extensible_dataset"):
            warning = "not available on destination dataset"
        elif not self.is_program_available(zfs_version_is_at_least_2_1_0, "dst"):
            warning = "unreliable as zfs version is too old"  # e.g. zfs-0.8.3 "internal error: Unknown error 1040"
        if warning:
            log.warning(f"zfs receive resume feature is {warning}. Falling back to --no-resume-recv: %s", dst_dataset)
            return None, [], []
        recv_resume_token = None
        send_resume_opts = []
        if self.dst_dataset_exists[dst_dataset]:
            cmd = p.split_args(f"{p.zfs_program} get -Hp -o value -s none receive_resume_token", dst_dataset)
            recv_resume_token = self.run_ssh_command(p.dst, log_trace, cmd=cmd).rstrip()
            if recv_resume_token == "-" or not recv_resume_token:
                recv_resume_token = None
            else:
                send_resume_opts += ["-n"] if p.dry_run else []
                send_resume_opts += ["-v"] if p.verbose_zfs else []
                send_resume_opts += ["-t", recv_resume_token]
        recv_resume_opts = ["-s"]
        return recv_resume_token, send_resume_opts, recv_resume_opts

    def mbuffer_cmd(self, loc: str, size_estimate_bytes: int) -> str:
        """If mbuffer command is on the PATH, uses it in the ssh network pipe between 'zfs send' and 'zfs receive' to
        smooth out the rate of data flow and prevent bottlenecks caused by network latency or speed fluctuation."""
        p = self.params
        if (
            size_estimate_bytes >= p.min_pipe_transfer_size
            and (
                loc == "src"
                or (loc == "dst" and (p.src.ssh_user_host != "" or p.dst.ssh_user_host != ""))
                or (loc == "local" and p.src.ssh_user_host != "" and p.dst.ssh_user_host != "")
            )
            and self.is_program_available("mbuffer", loc)
        ):
            return f"{p.mbuffer_program} {' '.join(self.mbuffer_current_opts)}"
        else:
            return "cat"

    def compress_cmd(self, loc: str, size_estimate_bytes: int) -> str:
        """If zstd command is on the PATH, uses it in the ssh network pipe between 'zfs send' and 'zfs receive' to
        reduce network bottlenecks by sending compressed data."""
        p = self.params
        if (
            size_estimate_bytes >= p.min_pipe_transfer_size
            and (p.src.ssh_user_host != "" or p.dst.ssh_user_host != "")
            and self.is_program_available("zstd", loc)
        ):
            return f"{p.compression_program} {' '.join(p.compression_program_opts)}"
        else:
            return "cat"

    def decompress_cmd(self, loc: str, size_estimate_bytes: int) -> str:
        p = self.params
        if (
            size_estimate_bytes >= p.min_pipe_transfer_size
            and (p.src.ssh_user_host != "" or p.dst.ssh_user_host != "")
            and self.is_program_available("zstd", loc)
        ):
            return f"{p.compression_program} -dc"
        else:
            return "cat"

    def pv_cmd(self, loc: str, size_estimate_bytes: int, size_estimate_human: str, disable_progress_bar=False) -> str:
        """If pv command is on the PATH, monitors the progress of data transfer from 'zfs send' to 'zfs receive'.
        Progress can be viewed via "tail -f $pv_log_file" aka tail -f ~/bzfs-logs/current.pv or similar."""
        p = self.params
        if size_estimate_bytes >= p.min_pipe_transfer_size and self.is_program_available("pv", loc):
            size = f"--size={size_estimate_bytes}"
            if disable_progress_bar:
                size = ""
            readable = shlex.quote(size_estimate_human)
            lp = p.log_params
            return f"{p.pv_program} {' '.join(p.pv_program_opts)} --force --name={readable} {size} 2>> {lp.pv_log_file}"
        else:
            return "cat"

    def local_ssh_command(self, remote: Remote) -> List[str]:
        """Returns the ssh CLI command to run locally in order to talk to the remote host. This excludes the (trailing)
        command to run on the remote host, which will be appended later."""
        if remote.ssh_user_host == "":
            return []  # dataset is on local host - don't use ssh

        # dataset is on remote host
        p = self.params
        if p.ssh_program == disable_prg:
            die("Cannot talk to remote host because ssh CLI is disabled.")
        ssh_cmd = [p.ssh_program] + remote.ssh_extra_opts
        if remote.ssh_config_file:
            ssh_cmd += ["-F", remote.ssh_config_file]
        for ssh_private_key_file in remote.ssh_private_key_files:
            ssh_cmd += ["-i", ssh_private_key_file]
        if remote.ssh_cipher:
            ssh_cmd += ["-c", remote.ssh_cipher]
        if remote.ssh_port:
            ssh_cmd += ["-p", str(remote.ssh_port)]
        if p.reuse_ssh_connection:
            # performance: reuse ssh connection for low latency startup of frequent ssh invocations
            # see https://www.cyberciti.biz/faq/linux-unix-reuse-openssh-connection/
            # generate unique private socket file name in user's home dir
            socket_dir = os.path.join(p.log_params.home_dir, ".ssh", "bzfs")
            os.makedirs(os.path.dirname(socket_dir), exist_ok=True)
            os.makedirs(socket_dir, mode=stat.S_IRWXU, exist_ok=True)  # aka chmod u=rwx,go=
            prefix = "s"
            delete_stale_files(socket_dir, prefix)

            def sanitize(name: str) -> str:
                # replace any whitespace, /, $, \, @ with a ~ tilde char
                name = re.sub(r"[\s\\/@$]", "~", name)
                # Remove characters not in the allowed set
                name = re.sub(r"[^a-zA-Z0-9;:,<.>?~`!%#$^&*+=_-]", "", name)
                return name

            unique = f"{time.time_ns()}@{random.SystemRandom().randint(0, 999_999)}"
            socket_name = f"{prefix}{os.getpid()}@{unique}@{sanitize(remote.ssh_host)[:45]}@{sanitize(remote.ssh_user)}"
            socket_file = os.path.join(socket_dir, socket_name)[: max(100, len(socket_dir) + 10)]
            ssh_cmd += ["-S", socket_file]
        ssh_cmd += [remote.ssh_user_host]
        return ssh_cmd

    def run_ssh_command(
        self, remote: Remote, level: int = -1, is_dry=False, check=True, print_stdout=False, print_stderr=True, cmd=None
    ) -> str:
        """Runs the given cmd via ssh on the given remote, and returns stdout. The full command is the concatenation
        of both the command to run on the localhost in order to talk to the remote host ($remote.ssh_cmd) and the
        command to run on the given remote host ($cmd)."""
        level = level if level >= 0 else logging.INFO
        assert cmd is not None and isinstance(cmd, list) and len(cmd) > 0
        p, log = self.params, self.params.log
        quoted_cmd = [shlex.quote(arg) for arg in cmd]
        ssh_cmd: List[str] = remote.ssh_cmd
        if len(ssh_cmd) > 0:
            if not self.is_program_available("ssh", "local"):
                die(f"{p.ssh_program} CLI is not available to talk to remote host. Install {p.ssh_program} first!")
            cmd = quoted_cmd
            if p.reuse_ssh_connection:
                # performance: reuse ssh connection for low latency startup of frequent ssh invocations
                # see https://www.cyberciti.biz/faq/linux-unix-reuse-openssh-connection/
                # 'ssh -S /path/socket -O check' doesn't talk over the network so common case is a low latency fast path
                ssh_socket_cmd = ssh_cmd[0:-1]  # omit trailing ssh_user_host
                ssh_socket_cmd += ["-O", "check", remote.ssh_user_host]
                if subprocess.run(ssh_socket_cmd, stdout=PIPE, stderr=PIPE, text=True).returncode == 0:
                    log.trace("ssh connection is alive: %s", list_formatter(ssh_socket_cmd))
                else:
                    log.trace("ssh connection is not yet alive: %s", list_formatter(ssh_socket_cmd))
                    ssh_socket_cmd = ssh_cmd[0:-1]  # omit trailing ssh_user_host
                    ssh_socket_cmd += ["-M", "-o", "ControlPersist=60s", remote.ssh_user_host, "exit"]
                    log.debug("Executing: %s", list_formatter(ssh_socket_cmd))
                    process = subprocess.run(ssh_socket_cmd, stderr=PIPE, text=True)
                    if process.returncode != 0:
                        log.error("%s", process.stderr.rstrip())
                        die(
                            f"Cannot ssh into remote host via '{' '.join(ssh_socket_cmd)}'. "
                            "Fix ssh configuration first, considering diagnostic log file output from running "
                            f"{prog_name} with: -v -v --ssh-src-extra-opts='-v -v' --ssh-dst-extra-opts='-v -v'"
                        )

        msg = "Would execute: %s" if is_dry else "Executing: %s"
        log.log(level, msg, list_formatter(remote.ssh_cmd_quoted + quoted_cmd, lstrip=True))
        if is_dry:
            return ""
        else:
            try:
                process = subprocess.run(ssh_cmd + cmd, stdout=PIPE, stderr=PIPE, text=True, check=check)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, UnicodeDecodeError) as e:
                if not isinstance(e, UnicodeDecodeError):
                    xprint(log, stderr_to_str(e.stdout), run=print_stdout, end="")
                    xprint(log, stderr_to_str(e.stderr), run=print_stderr, end="")
                raise
            else:
                xprint(log, process.stdout, run=print_stdout, end="")
                xprint(log, process.stderr, run=print_stderr, end="")
                return process.stdout

    def try_ssh_command(
        self, remote: Remote, level: int, is_dry=False, print_stdout=False, cmd=None, exists=True, error_trigger=None
    ):
        """Convenience method that helps retry/react to a dataset or pool that potentially doesn't exist anymore."""
        log = self.params.log
        try:
            self.maybe_inject_error(cmd=cmd, error_trigger=error_trigger)
            return self.run_ssh_command(remote, level=level, is_dry=is_dry, print_stdout=print_stdout, cmd=cmd)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, UnicodeDecodeError) as e:
            if not isinstance(e, UnicodeDecodeError):
                stderr = stderr_to_str(e.stderr)
                if exists and (
                    ": dataset does not exist" in stderr
                    or ": filesystem does not exist" in stderr  # solaris 11.4.0
                    or ": does not exist" in stderr  # solaris 11.4.0 'zfs send' with missing snapshot
                    or ": no such pool" in stderr
                ):
                    return None
                log.warning("%s", stderr.rstrip())
            raise RetryableError("Subprocess failed") from e

    def maybe_inject_error(self, cmd=None, error_trigger: Optional[str] = None) -> None:
        """For testing only; for unit tests to simulate errors during replication and test correct handling of them."""
        if error_trigger:
            counter = self.error_injection_triggers.get("before")
            if counter and counter[error_trigger] > 0:
                counter[error_trigger] -= 1
                try:
                    raise CalledProcessError(returncode=1, cmd=" ".join(cmd), stderr=error_trigger + ":dataset is busy")
                except subprocess.CalledProcessError as e:
                    if error_trigger.startswith("retryable_"):
                        raise RetryableError("Subprocess failed") from e
                    else:
                        raise

    def maybe_inject_delete(self, remote: Remote, dataset=None, delete_trigger=None) -> None:
        """For testing only; for unit tests to delete datasets during replication and test correct handling of that."""
        assert delete_trigger
        counter = self.delete_injection_triggers.get("before")
        if counter and counter[delete_trigger] > 0:
            counter[delete_trigger] -= 1
            p, log = self.params, self.params.log
            cmd = p.split_args(f"{remote.sudo} {p.zfs_program} destroy -r", p.force_unmount, p.force_hard, dataset)
            self.run_ssh_command(remote, log_debug, print_stdout=True, cmd=cmd)

    def maybe_inject_params(self, error_trigger: str) -> None:
        """For testing only; for unit tests to simulate errors during replication and test correct handling of them."""
        assert error_trigger
        counter = self.error_injection_triggers.get("before")
        if counter and counter[error_trigger] > 0:
            counter[error_trigger] -= 1
            self.inject_params = self.param_injection_triggers[error_trigger]
        elif error_trigger in self.param_injection_triggers:
            self.inject_params = {}

    def squote(self, ssh_cmd: List[str], arg: str) -> str:
        return arg if len(ssh_cmd) == 0 else shlex.quote(arg)

    def dquote(self, arg: str) -> str:
        """shell-escapes double quotes and backticks, then surrounds with double quotes."""
        return '"' + arg.replace('"', '\\"').replace("`", "\\`") + '"'

    def filter_datasets(self, remote: Remote, datasets: List[str]) -> List[str]:
        """Returns all datasets (and their descendants) that match at least one of the include regexes but none of the
        exclude regexes."""
        p, log = self.params, self.params.log
        results = []
        for i, dataset in enumerate(datasets):
            if i == 0 and p.skip_parent:
                continue
            rel_dataset = relativize_dataset(dataset, remote.root_dataset)
            if rel_dataset.startswith("/"):
                rel_dataset = rel_dataset[1:]  # strip leading '/' char if any
            if is_included(rel_dataset, p.include_dataset_regexes, p.exclude_dataset_regexes):
                results.append(dataset)
                log.debug("Including b/c dataset regex: %s", dataset)
            else:
                log.debug("Excluding b/c dataset regex: %s", dataset)
        if p.exclude_dataset_property:
            results = self.filter_datasets_by_exclude_property(remote, results)
        is_debug = p.log.isEnabledFor(log_debug)
        for dataset in results:
            is_debug and log.debug(f"Finally included {remote.location} dataset: %s", dataset)
        return results

    def filter_datasets_by_exclude_property(self, remote: Remote, datasets: List[str]) -> List[str]:
        """Excludes datasets that are marked with a ZFS user property value that, in effect, says 'skip me'."""
        p, log = self.params, self.params.log
        results = []
        localhostname = None
        skip_dataset = DONT_SKIP_DATASET
        for dataset in datasets:
            if is_descendant(dataset, of_root_dataset=skip_dataset):
                # skip_dataset shall be ignored or has been deleted by some third party while we're running
                continue  # nothing to do anymore for this dataset subtree (note that datasets is sorted)
            skip_dataset = DONT_SKIP_DATASET
            # TODO perf: on zfs >= 2.3 use json via zfs list -j to safely merge all zfs list's into one 'zfs list' call
            cmd = p.split_args(
                f"{p.zfs_program} list -t filesystem,volume -Hp -o {p.exclude_dataset_property}", dataset
            )
            self.maybe_inject_delete(remote, dataset=dataset, delete_trigger="zfs_list_exclude_property")
            property_value = self.try_ssh_command(remote, log_trace, cmd=cmd)
            if property_value is None:
                log.warning(f"Third party deleted {remote.location}: %s", dataset)
                skip_dataset = dataset
            else:
                reason = ""
                property_value = property_value.strip()
                if not property_value or property_value == "-" or property_value.lower() == "true":
                    sync = True
                elif property_value.lower() == "false":
                    sync = False
                else:
                    localhostname = localhostname or socket.getfqdn()
                    sync = any(localhostname == hostname.strip() for hostname in property_value.split(","))
                    reason = f", localhostname: {localhostname}, hostnames: {property_value}"

                if sync:
                    results.append(dataset)
                    log.debug("Including b/c dataset prop: %s%s", dataset, reason)
                else:
                    skip_dataset = dataset
                    log.debug("Excluding b/c dataset prop: %s%s", dataset, reason)
        return results

    def filter_snapshots(self, snapshots: List[str]) -> List[str]:
        """Returns all snapshots that pass all include/exclude policies."""
        p, log = self.params, self.params.log
        for _filter in p.snapshot_filters:
            name = _filter.name
            if name == snapshot_regex_filter_name:
                snapshots = self.filter_snapshots_by_regex(snapshots, regexes=_filter.options)
            elif name == "include_snapshot_times":
                snapshots = self.filter_snapshots_by_creation_time(snapshots, include_snapshot_times=_filter.timerange)
            else:
                assert name == "include_snapshot_times_and_ranks"
                snapshots = self.filter_snapshots_by_creation_time_and_rank(
                    snapshots, include_snapshot_times=_filter.timerange, include_snapshot_ranks=_filter.options
                )
        is_debug = p.log.isEnabledFor(log_debug)
        for snapshot in snapshots:
            is_debug and log.debug("Finally included snapshot: %s", snapshot[snapshot.rindex("\t") + 1 :])
        return snapshots

    def filter_snapshots_by_regex(self, snapshots: List[str], regexes: Tuple[RegexList, RegexList]) -> List[str]:
        """Returns all snapshots that match at least one of the include regexes but none of the exclude regexes."""
        exclude_snapshot_regexes, include_snapshot_regexes = regexes
        p, log = self.params, self.params.log
        is_debug = log.isEnabledFor(log_debug)
        results = []
        for snapshot in snapshots:
            i = snapshot.find("@")  # snapshot separator
            if i < 0:
                results.append(snapshot)  # retain bookmarks to help find common snaps, apply filter only to snapshots
            elif is_included(snapshot[i + 1 :], include_snapshot_regexes, exclude_snapshot_regexes):
                results.append(snapshot)
                is_debug and log.debug("Including b/c snapshot regex: %s", snapshot[snapshot.rindex("\t") + 1 :])
            else:
                is_debug and log.debug("Excluding b/c snapshot regex: %s", snapshot[snapshot.rindex("\t") + 1 :])
        return results

    def filter_snapshots_by_creation_time(self, snaps: List[str], include_snapshot_times: UnixTimeRange) -> List[str]:
        p, log = self.params, self.params.log
        is_debug = log.isEnabledFor(log_debug)
        lo_snaptime, hi_snaptime = include_snapshot_times or (0, unixtime_infinity_secs)
        results = []
        for snapshot in snaps:
            if "@" not in snapshot:
                results.append(snapshot)  # retain bookmarks to help find common snaps, apply filter only to snapshots
            elif lo_snaptime <= int(snapshot[0 : snapshot.index("\t")]) < hi_snaptime:
                results.append(snapshot)
                is_debug and log.debug("Including b/c creation time: %s", snapshot[snapshot.rindex("\t") + 1 :])
            else:
                is_debug and log.debug("Excluding b/c creation time: %s", snapshot[snapshot.rindex("\t") + 1 :])
        return results

    def filter_snapshots_by_creation_time_and_rank(
        self, snapshots: List[str], include_snapshot_times: UnixTimeRange, include_snapshot_ranks: List[RankRange]
    ) -> List[str]:

        def get_idx(rank: Tuple[str, int, bool], n: int) -> int:
            kind, num, is_percent = rank
            m = round(n * num / 100) if is_percent else min(n, num)
            assert kind == "latest" or kind == "oldest"
            return m if kind == "oldest" else n - m

        assert isinstance(include_snapshot_ranks, list)
        assert len(include_snapshot_ranks) > 0
        p, log = self.params, self.params.log
        is_debug = log.isEnabledFor(log_debug)
        lo_time, hi_time = include_snapshot_times or (0, unixtime_infinity_secs)
        n = sum(1 for snapshot in snapshots if "@" in snapshot)
        for rank_range in include_snapshot_ranks:
            lo_rank, hi_rank = rank_range
            lo = get_idx(lo_rank, n)
            hi = get_idx(hi_rank, n)
            lo, hi = (lo, hi) if lo <= hi else (hi, lo)
            i = 0
            results = []
            for snapshot in snapshots:
                if "@" not in snapshot:
                    results.append(snapshot)  # retain bookmarks to help find common snaps, apply filter only to snaps
                else:
                    msg = None
                    if lo <= i < hi:
                        msg = "Including b/c snapshot rank: %s"
                    elif lo_time <= int(snapshot[0 : snapshot.index("\t")]) < hi_time:
                        msg = "Including b/c creation time: %s"
                    if msg:
                        results.append(snapshot)
                    else:
                        msg = "Excluding b/c snapshot rank: %s"
                    is_debug and log.debug(msg, snapshot[snapshot.rindex("\t") + 1 :])
                    i += 1
            snapshots = results
            n = hi - lo
        return snapshots

    def filter_properties(self, props: Dict[str, str], include_regexes, exclude_regexes) -> Dict[str, str]:
        """Returns ZFS props whose name matches at least one of the include regexes but none of the exclude regexes."""
        p, log = self.params, self.params.log
        is_debug = log.isEnabledFor(log_debug)
        results = {}
        for propname, propvalue in props.items():
            if is_included(propname, include_regexes, exclude_regexes):
                results[propname] = propvalue
                is_debug and log.debug("Including b/c property regex: %s", propname)
            else:
                is_debug and log.debug("Excluding b/c property regex: %s", propname)
        return results

    @staticmethod
    def filter_lines(input_list: Iterable[str], input_set: Set[str]) -> List[str]:
        """For each line in input_list, includes the line if input_set contains the first column field of that line."""
        if len(input_set) == 0:
            return []
        return [line for line in input_list if line[0 : line.index("\t")] in input_set]

    def delete_snapshots(self, remote: Remote, dataset: str, snapshot_tags: List[str]) -> None:
        if len(snapshot_tags) == 0:
            return
        if self.is_solaris_zfs(remote):
            # solaris-11.4 has no syntax to delete multiple snapshots in a single CLI invocation
            for snapshot_tag in snapshot_tags:
                self.delete_snapshot(remote, f"{dataset}@{snapshot_tag}")
        else:  # delete snapshots in batches without creating a command line that's too big for the OS to handle
            self.run_ssh_cmd_batched(
                remote,
                self.delete_snapshot_cmd(remote, dataset + "@"),
                snapshot_tags,
                lambda batch: self.delete_snapshot(remote, dataset + "@" + ",".join(batch)),
            )

    def delete_snapshot(self, r: Remote, snaps_to_delete: str) -> None:
        p, log = self.params, self.params.log
        log.info(p.dry("Deleting snapshot(s): %s"), snaps_to_delete)
        cmd = self.delete_snapshot_cmd(r, snaps_to_delete)
        is_dry = p.dry_run and self.is_solaris_zfs(r)  # solaris-11.4 knows no 'zfs destroy -n' flag
        self.try_ssh_command(
            r, log_debug, is_dry=is_dry, print_stdout=True, cmd=cmd, exists=False, error_trigger="zfs_delete_snapshot"
        )

    def delete_snapshot_cmd(self, r: Remote, snaps_to_delete: str) -> List[str]:
        p = self.params
        return p.split_args(
            f"{r.sudo} {p.zfs_program} destroy", p.force_hard, p.verbose_destroy, p.dry_run_destroy, snaps_to_delete
        )

    def delete_bookmarks(self, remote: Remote, dataset: str, snapshot_tags: List[str]) -> None:
        if len(snapshot_tags) == 0:
            return
        # Unfortunately ZFS has no syntax yet to delete multiple bookmarks in a single CLI invocation
        p, log = self.params, self.params.log
        log.info(p.dry("Deleting bookmark(s): %s"), dataset + "#" + ",".join(snapshot_tags))
        for i, snapshot_tag in enumerate(snapshot_tags):
            log.debug(p.dry(f"Deleting bookmark {i+1}/{len(snapshot_tags)}: %s"), snapshot_tag)
            cmd = p.split_args(f"{remote.sudo} {p.zfs_program} destroy", f"{dataset}#{snapshot_tag}")
            self.try_ssh_command(remote, log_debug, is_dry=p.dry_run, print_stdout=True, cmd=cmd, exists=False)

    def delete_datasets(self, remote: Remote, datasets: Iterable[str]) -> None:
        """Deletes the given datasets via zfs destroy -r on the given remote."""
        # Impl is batch optimized to minimize CLI + network roundtrips: only need to run zfs destroy if previously
        # destroyed dataset (within sorted datasets) is not a prefix (aka ancestor) of current dataset
        p, log = self.params, self.params.log
        last_deleted_dataset = DONT_SKIP_DATASET
        for dataset in isorted(datasets):
            if is_descendant(dataset, of_root_dataset=last_deleted_dataset):
                continue
            log.info(p.dry("Deleting dataset tree: %s"), f"{dataset} ...")
            cmd = p.split_args(
                f"{remote.sudo} {p.zfs_program} destroy -r {p.force_unmount} {p.force_hard} {p.verbose_destroy}",
                p.dry_run_destroy,
                dataset,
            )
            is_dry = p.dry_run and self.is_solaris_zfs(remote)  # solaris-11.4 knows no 'zfs destroy -n' flag
            self.run_ssh_command(remote, log_debug, is_dry=is_dry, print_stdout=True, cmd=cmd)
            last_deleted_dataset = dataset

    def create_filesystem(self, filesystem: str) -> None:
        # zfs create -p -u $filesystem
        # To ensure the filesystems that we create do not get mounted, we apply a separate 'zfs create -p -u'
        # invocation for each non-existing ancestor. This is because a single 'zfs create -p -u' applies the '-u'
        # part only to the immediate filesystem, rather than to the not-yet existing ancestors.
        p, log = self.params, self.params.log
        parent = ""
        no_mount = "-u" if self.is_program_available(zfs_version_is_at_least_2_1_0, "dst") else ""
        for component in filesystem.split("/"):
            parent += component
            if not self.dst_dataset_exists[parent]:
                cmd = p.split_args(f"{p.dst.sudo} {p.zfs_program} create -p", no_mount, parent)
                try:
                    self.run_ssh_command(p.dst, log_debug, is_dry=p.dry_run, print_stdout=True, cmd=cmd)
                except subprocess.CalledProcessError as e:
                    # ignore harmless error caused by 'zfs create' without the -u flag
                    if (
                        "filesystem successfully created, but it may only be mounted by root" not in e.stderr
                        and "filesystem successfully created, but not mounted" not in e.stderr  # SolarisZFS
                    ):
                        raise
                if not p.dry_run:
                    self.dst_dataset_exists[parent] = True
            parent += "/"

    def create_zfs_bookmark(self, remote: Remote, src_snapshot: str, src_dataset: str) -> None:
        p, log = self.params, self.params.log
        assert "@" in src_snapshot
        bookmark = replace_prefix(src_snapshot, old_prefix=f"{src_dataset}@", new_prefix=f"{src_dataset}#")
        if p.create_bookmark and self.are_bookmarks_enabled(remote):
            cmd = p.split_args(f"{remote.sudo} {p.zfs_program} bookmark", src_snapshot, bookmark)
            try:
                self.run_ssh_command(remote, log_debug, is_dry=p.dry_run, print_stderr=False, cmd=cmd)
            except subprocess.CalledProcessError as e:
                # ignore harmless zfs error caused by bookmark with the same name already existing
                if ": bookmark exists" not in e.stderr:
                    print(e.stderr, file=sys.stderr, end="")
                    raise

    def estimate_send_size(self, remote: Remote, dst_dataset: str, recv_resume_token: str, *items) -> int:
        """Estimates num bytes to transfer via 'zfs send'."""
        p, log = self.params, self.params.log
        if self.is_solaris_zfs(remote):
            return 0  # solaris-11.4 does not have a --parsable equivalent
        zfs_send_program_opts = ["--parsable" if opt == "-P" else opt for opt in p.curr_zfs_send_program_opts]
        zfs_send_program_opts = append_if_absent(zfs_send_program_opts, "-v", "-n", "--parsable")
        if recv_resume_token:
            zfs_send_program_opts = ["-Pnv", "-t", recv_resume_token]
            items = ""
        cmd = p.split_args(f"{remote.sudo} {p.zfs_program} send", zfs_send_program_opts, items)
        try:
            lines = self.try_ssh_command(remote, log_trace, cmd=cmd)
        except RetryableError as retryable_error:
            if recv_resume_token:
                e = retryable_error.__cause__
                stderr = stderr_to_str(e.stderr) if hasattr(e, "stderr") else ""
                retryable_error.no_sleep = self.clear_resumable_recv_state_if_necessary(dst_dataset, stderr)
            # op isn't idempotent so retries regather current state from the start of replicate_dataset()
            raise retryable_error
        if lines is None:
            return 0  # src dataset or snapshot has been deleted by third party
        size = lines.splitlines()[-1]
        assert size.startswith("size")
        return int(size[size.index("\t") + 1 :])

    def dataset_regexes(self, datasets: List[str]) -> List[str]:
        src, dst = self.params.src, self.params.dst
        results = []
        for dataset in datasets:
            if dataset.startswith("/"):
                # it's an absolute dataset - convert it to a relative dataset
                dataset = dataset[1:]
                if is_descendant(dataset, of_root_dataset=src.root_dataset):
                    dataset = relativize_dataset(dataset, src.root_dataset)
                elif is_descendant(dataset, of_root_dataset=dst.root_dataset):
                    dataset = relativize_dataset(dataset, dst.root_dataset)
                else:
                    continue  # ignore datasets that make no difference
                if dataset.startswith("/"):
                    dataset = dataset[1:]
            if dataset.endswith("/"):
                dataset = dataset[0:-1]
            if dataset:
                regex = re.escape(dataset)
            else:
                regex = ".*"
            results.append(regex)
        return results

    def run_with_retries(self, policy: RetryPolicy, func, *args, **kwargs) -> Any:
        """Runs the given function with the given arguments, and retries on failure as indicated by policy."""
        log = self.params.log
        max_sleep_mark = policy.min_sleep_nanos
        retry_count = 0
        start_time_nanos = time.time_ns()
        while True:
            try:
                return func(*args, **kwargs)  # Call the target function with the provided arguments
            except RetryableError as retryable_error:
                elapsed_nanos = time.time_ns() - start_time_nanos
                if retry_count < policy.retries and elapsed_nanos < policy.max_elapsed_nanos:
                    retry_count = retry_count + 1
                    if retryable_error.no_sleep and retry_count <= 1:
                        log.info(f"Retrying [{retry_count}/{policy.retries}] immediately ...")
                        continue
                    # pick a random sleep duration within the range [min_sleep_nanos, max_sleep_mark] as delay
                    sleep_nanos = random.randint(policy.min_sleep_nanos, max_sleep_mark)
                    human_duration = human_readable_duration(sleep_nanos, long=False)
                    log.info(f"Retrying [{retry_count}/{policy.retries}] in {human_duration} ...")
                    time.sleep(sleep_nanos / 1_000_000_000)
                    max_sleep_mark = min(policy.max_sleep_nanos, 2 * max_sleep_mark)  # exponential backoff with cap
                else:
                    if policy.retries > 0:
                        log.error(
                            f"Giving up because the last [{retry_count}/{policy.retries}] retries across "
                            f"[{elapsed_nanos // 1_000_000_000}/{policy.max_elapsed_nanos // 1_000_000_000}] "
                            "seconds for the current request failed!"
                        )
                    raise retryable_error.__cause__

    def incremental_send_steps_wrapper(
        self, src_snapshots: List[str], src_guids: List[str], included_guids: Set[str], is_resume: bool
    ) -> List[Tuple[str, str, str]]:
        force_convert_I_to_i = False
        if self.params.src.use_zfs_delegation and not getenv_bool("no_force_convert_I_to_i", False):
            # If using 'zfs allow' delegation mechanism, force convert 'zfs send -I' to a series of
            # 'zfs send -i' as a workaround for zfs issue https://github.com/openzfs/zfs/issues/16394
            force_convert_I_to_i = True
        return self.incremental_send_steps(src_snapshots, src_guids, included_guids, is_resume, force_convert_I_to_i)

    def incremental_send_steps(
        self, src_snapshots: List[str], src_guids: List[str], included_guids: Set[str], is_resume, force_convert_I_to_i
    ) -> List[Tuple[str, str, str]]:
        """Computes steps to incrementally replicate the given src snapshots with the given src_guids such that we
        include intermediate src snapshots that pass the policy specified by --{include,exclude}-snapshot-*
        (represented here by included_guids), using an optimal series of -i/-I send/receive steps that skip
        excluded src snapshots. The steps are optimal in the sense that no solution with fewer steps exists. A step
        corresponds to a single ZFS send/receive operation. Fewer steps translate to better performance, especially
        when sending many small snapshots. For example, 1 step that sends 100 small snapshots in a single operation is
        much faster than 100 steps that each send only 1 such snapshot per ZFS send/receive operation.
        Example: skip hourly snapshots and only include daily shapshots for replication
        Example: [d1, h1, d2, d3, d4] (d is daily, h is hourly) --> [d1, d2, d3, d4] via
        -i d1:d2 (i.e. exclude h1; '-i' and ':' indicate 'skip intermediate snapshots')
        -I d2-d4 (i.e. also include d3; '-I' and '-' indicate 'include intermediate snapshots')
        * The force_convert_I_to_i param is necessary as a work-around for https://github.com/openzfs/zfs/issues/16394
        * 'zfs send' CLI with a bookmark as starting snapshot does not (yet) support including intermediate
        src_snapshots via -I flag per https://github.com/openzfs/zfs/issues/12415. Thus, if the replication source
        is a bookmark we convert a -I step to a -i step followed by zero or more -i/-I steps.
        * The is_resume param is necessary as 'zfs send -t' does not support sending more than a single snapshot
        on resuming a previously interrupted 'zfs receive -s'. Thus, here too, we convert a -I step to a -i step
        followed by zero or more -i/-I steps."""

        def append_run(i: int, label: str) -> int:
            step = ("-I", src_snapshots[start], src_snapshots[i])
            # print(f"{label} {self.send_step_to_str(step)}")
            is_not_resume = len(steps) > 0 or not is_resume
            if i - start > 1 and not force_convert_I_to_i and "@" in src_snapshots[start] and is_not_resume:
                steps.append(step)
            elif "@" in src_snapshots[start] and is_not_resume:
                for j in range(start, i):  # convert -I step to -i steps
                    steps.append(("-i", src_snapshots[j], src_snapshots[j + 1]))
            else:  # it's a bookmark src or zfs send -t; convert -I step to -i step followed by zero or more -i/-I steps
                steps.append(("-i", src_snapshots[start], src_snapshots[start + 1]))
                i = start + 1
            return i - 1

        assert len(src_guids) == len(src_snapshots)
        assert len(included_guids) >= 0
        steps = []
        guids = src_guids
        n = len(guids)
        i = 0
        while i < n and guids[i] not in included_guids:  # skip hourlies
            i += 1

        while i < n:
            assert guids[i] in included_guids  # it's a daily
            start = i
            i += 1
            while i < n and guids[i] in included_guids:  # skip dailies
                i += 1
            if i < n:
                if i - start == 1:
                    # it's a single daily (that was already replicated) followed by an hourly
                    i += 1
                    while i < n and guids[i] not in included_guids:  # skip hourlies
                        i += 1
                    if i < n:
                        assert start != i
                        step = ("-i", src_snapshots[start], src_snapshots[i])
                        # print(f"r1 {self.send_step_to_str(step)}")
                        steps.append(step)
                        i -= 1
                else:  # it's a run of more than one daily
                    i -= 1
                    assert start != i
                    i = append_run(i, "r2")
            else:  # finish up run of trailing dailies
                i -= 1
                if start != i:
                    i = append_run(i, "r3")
            i += 1
        return steps

    @staticmethod
    def send_step_to_str(step: Tuple[str, str, str]) -> str:
        # return str(step[1]) + ('-' if step[0] == '-I' else ':') + str(step[2])
        return str(step)

    def zfs_set(self, properties: List[str], remote: Remote, dataset: str) -> None:
        """Applies the given property key=value pairs via 'zfs set' CLI to the given dataset on the given remote."""

        def run_zfs_set(props: List[str]) -> None:
            p, log = self.params, self.params.log
            cmd = p.split_args(f"{remote.sudo} {p.zfs_program} set") + props + [dataset]
            self.run_ssh_command(remote, log_debug, is_dry=p.dry_run, print_stdout=True, cmd=cmd)

        if len(properties) > 0:
            if not self.is_solaris_zfs(remote):
                run_zfs_set(properties)  # send all properties in a batch
            else:
                for prop in properties:  # solaris-11.4 does not accept multiple properties per 'zfs set' CLI call
                    run_zfs_set([prop])

    def zfs_get(
        self,
        remote: Remote,
        dataset: str,
        sources: str,
        output_columns: str,
        propnames: str,
        splitlines: bool,
        props_cache: Dict[Tuple[str, str, str], Dict[str, str]],
    ) -> Dict[str, Optional[str]]:
        """Returns the results of 'zfs get' CLI on the given dataset on the given remote."""
        if not propnames:
            return {}
        p, log = self.params, self.params.log
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

    def add_recv_property_options(
        self, full_send: bool, recv_opts: List[str], dataset: str, cache: Dict[Tuple[str, str, str], Dict[str, str]]
    ) -> Tuple[List[str], List[str]]:
        """Reads the ZFS properties of the given src dataset. Appends zfs recv -o and -x values to recv_opts according
        to CLI params, and returns properties to explicitly set on the dst dataset after 'zfs receive' completes
        successfully."""
        p = self.params
        set_opts = []
        ox_names = p.zfs_recv_ox_names.copy()
        for config in [p.zfs_recv_o_config, p.zfs_recv_x_config, p.zfs_set_config]:
            if len(config.include_regexes) == 0:
                continue  # this is the default - it's an instant noop
            if (full_send and "full" in config.targets) or (not full_send and "incremental" in config.targets):
                # 'zfs get' uses newline as record separator and tab as separator between output columns. A ZFS user
                # property may contain newline and tab characters (indeed anything). Together, this means that there
                # is no reliable way to determine where a record ends and the next record starts when listing multiple
                # arbitrary records in a single 'zfs get' call. Therefore, here we use a separate 'zfs get' call for
                # each ZFS user property.
                # TODO: perf: on zfs >= 2.3 use json via zfs get -j to safely merge all zfs gets into one 'zfs get' call
                try:
                    props = self.zfs_get(p.src, dataset, config.sources, "property", "all", True, cache)
                    props = self.filter_properties(props, config.include_regexes, config.exclude_regexes)
                    user_propnames = [name for name in props.keys() if ":" in name]
                    sys_propnames = ",".join([name for name in props.keys() if ":" not in name])
                    props = self.zfs_get(p.src, dataset, config.sources, "property,value", sys_propnames, True, cache)
                    for propnames in user_propnames:
                        props.update(
                            self.zfs_get(p.src, dataset, config.sources, "property,value", propnames, False, cache)
                        )
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired, UnicodeDecodeError) as e:
                    raise RetryableError("Subprocess failed") from e
                for propname in sorted(props.keys()):
                    if config is p.zfs_recv_o_config:
                        if propname not in ox_names:
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
    def recv_option_property_names(recv_opts: List[str]) -> Set[str]:
        """Extracts -o and -x property names that are already specified on the command line. This can be used to check
        for dupes because 'zfs receive' does not accept multiple -o or -x options with the same property name."""
        propnames = set()
        i = 0
        n = len(recv_opts)
        while i < n:
            stripped = recv_opts[i].strip()
            if stripped in {"-o", "-x"}:
                i += 1
                if i == n or recv_opts[i].strip() in {"-o", "-x"}:
                    die(f"Missing value for {stripped} option in --zfs-receive-program-opt(s): {' '.join(recv_opts)}")
                propnames.add(recv_opts[i] if stripped == "-x" else recv_opts[i].split("=", 1)[0])
            i += 1
        return propnames

    @dataclass(order=True)
    class ComparableSnapshot:
        key: Tuple[str, str]
        cols: List[str] = field(compare=False)

    def run_compare_snapshot_lists(self, src_datasets: List[str], dst_datasets: List[str]) -> None:
        """Compares source and destination dataset trees recursively wrt. snapshots, for example to check if all
        recently taken snapshots have been successfully replicated by a periodic job. Lists snapshots only contained in
        source (tagged with 'src'), only contained in destination (tagged with 'dst'), and contained in both source
        and destination (tagged with 'all'), in the form of a TSV file, along with other snapshot metadata.
        Implemented with a time and space efficient streaming algorithm; easily scales to millions of datasets and
        any number of snapshots."""
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
        is_first_row = [True]

        def zfs_list_snapshot_iterator(r: Remote, datasets: List[str]) -> Generator[str, None, None]:
            """Lists snapshots sorted by dataset name. All snapshots of a given dataset will be adjacent."""
            written_zfs_prop = "written"  # https://openzfs.github.io/openzfs-docs/man/master/7/zfsprops.7.html#written
            if self.is_solaris_zfs(r):  # solaris-11.4 zfs does not know the "written" ZFS snapshot property
                written_zfs_prop = "type"  # for simplicity, fill in the non-integer dummy constant type="snapshot"
            props = self.creation_prefix + f"creation,guid,createtxg,{written_zfs_prop},name"
            types = "snapshot"
            if p.use_bookmark and r.location == "src" and self.are_bookmarks_enabled(r):
                types = "snapshot,bookmark"
            cmd = p.split_args(f"{p.zfs_program} list -t {types} -d 1 -Hp -o {props}")  # sorted by dataset, createtxg
            for lines in self.itr_ssh_command_parallel(r, cmd, sorted(datasets)):
                yield from lines

        def snapshot_iterator(
            root_dataset: str, sorted_itr: Generator[str, None, None]
        ) -> Generator[Job.ComparableSnapshot, None, None]:
            """Splits/groups snapshot stream into distinct datasets, sorts by GUID within a dataset such that two
            snapshots with the same GUID will lie adjacent to each other during the upcoming phase that merges
            src snapshots and dst snapshots."""
            # streaming group by dataset name (consumes constant memory only)
            for dataset, group in groupby(
                sorted_itr, key=lambda line: line[line.rindex("\t") + 1 : line.replace("#", "@").index("@")]
            ):
                snapshots = list(group)  # fetch all snapshots of current dataset, e.g. dataset=tank1/src/foo
                snapshots = self.filter_snapshots(snapshots)  # apply include/exclude policy
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
                        written = "-"  # sanitize solaris-11.4 work-around
                        cols = [creation, guid, createtxg, written, snapshot_name]
                    key = (rel_dataset, guid)  # ensures src snaps and dst snaps with the same GUID will be adjacent
                    yield Job.ComparableSnapshot(key, cols)

        def print_dataset(rel_dataset: str, entries: Iterable[Tuple[str, Job.ComparableSnapshot]]) -> None:
            entries = sorted(  # fetch all snaps of current dataset and sort em by creation, createtxg, snapshot_tag
                entries,
                key=lambda entry: (
                    int(entry[1].cols[0]),
                    int(entry[1].cols[2]),
                    entry[1].cols[-1][entry[1].cols[-1].replace("#", "@").index("@") + 1 :],
                ),
            )

            @dataclass
            class SnapshotStats:
                snapshot_count: int = field(default=0)
                sum_written: int = field(default=0)
                snapshot_count_since: int = field(default=0)
                sum_written_since: int = field(default=0)
                latest_snapshot_idx: int = field(default=None)
                latest_snapshot_row_str: str = field(default=None)
                latest_snapshot_creation: str = field(default=None)
                oldest_snapshot_row_str: str = field(default=None)
                oldest_snapshot_creation: str = field(default=None)

            # print metadata of snapshots of current dataset to TSV file; custom stats can later be computed from there
            stats = defaultdict(SnapshotStats)
            header = "#location creation_iso createtxg rel_name guid root_dataset rel_dataset name creation written"
            for i, entry in enumerate(entries):
                is_first_row[0] = is_first_row[0] and (fd.write(header.replace(" ", "\t") + "\n") and False)
                loc = location = entry[0]
                creation, guid, createtxg, written, name = entry[1].cols
                root_dataset = dst.root_dataset if location == cmp_choices_items[1] else src.root_dataset
                rel_name = relativize_dataset(name, root_dataset)
                creation_iso = isotime_from_unixtime(int(creation))
                r = loc, creation_iso, createtxg, rel_name, guid, root_dataset, rel_dataset, name, creation, written
                # Example: src 2024-11-06_08:30:05 17435050 /foo@test_2024-11-06_08:30:05_daily 2406491805272097867 tank1/src /foo tank1/src/foo@test_2024-10-06_08:30:04_daily 1730878205 24576
                row_str = "\t".join(r)
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
                f"{prefix} Q: No src snapshots are missing on dst, and no dst snapshots are missing "
                f"on src, and there is a common snapshot? A: "
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
            now = time.time()
            latcom = "latest common snapshot"
            for loc in all_src_dst:
                s = stats[loc]
                msgs.append(f"{prefix} Latest snapshot only in {loc}: {s.latest_snapshot_row_str or 'n/a'}")
                msgs.append(f"{prefix} Oldest snapshot only in {loc}: {s.oldest_snapshot_row_str or 'n/a'}")
                msgs.append(f"{prefix} Snapshots only in {loc}: {s.snapshot_count}")
                msgs.append(f"{prefix} Snapshot data written only in {loc}: {human_readable_bytes(s.sum_written)}")
                na = None if loc != "all" and k >= 0 else "n/a"
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
                            hd = human_readable_duration(int(all_creation) - int(s_creation), unit="s")
                        msgs.append(f"{prefix} Time diff between {latcom} and {label} snapshot only in {loc}: {hd}")
                for label, s_creation in latest, oldest:
                    hd = "n/a" if not s_creation else human_readable_duration(round(now - int(s_creation)), unit="s")
                    msgs.append(f"{prefix} Time diff between now and {label} snapshot only in {loc}: {hd}")
            log.info("%s", "\n".join(msgs))

        # setup streaming pipeline
        src_snap_itr = snapshot_iterator(src.root_dataset, zfs_list_snapshot_iterator(src, src_datasets))
        dst_snap_itr = snapshot_iterator(dst.root_dataset, zfs_list_snapshot_iterator(dst, dst_datasets))
        merge_itr = self.merge_sorted_iterators(cmp_choices_items, p.compare_snapshot_lists, src_snap_itr, dst_snap_itr)

        rel_datasets: Dict[str, Set[str]] = defaultdict(set)
        for datasets, remote in (src_datasets, src), (dst_datasets, dst):
            for dataset in datasets:  # rel_dataset=/foo, root_dataset=tank1/src
                rel_datasets[remote.location].add(relativize_dataset(dataset, remote.root_dataset))
        rel_src_or_dst: List[str] = sorted(rel_datasets["src"].union(rel_datasets["dst"]))

        log.debug("%s", f"Temporary TSV output file comparing {task} is: {tmp_tsv_file}")
        with open(tmp_tsv_file, "w", encoding="utf-8") as fd:
            # streaming group by rel_dataset (consumes constant memory only); entry is a Tuple[str, ComparableSnapshot]
            group = groupby(merge_itr, key=lambda entry: entry[1].key[0])
            self.print_datasets(group, lambda rel_ds, entries: print_dataset(rel_ds, entries), rel_src_or_dst)
        os.rename(tmp_tsv_file, tsv_file)
        log.info("%s", f"Final TSV output file comparing {task} is: {tsv_file}")

        tsv_file = tsv_file[0 : tsv_file.rindex(".")] + ".rel_datasets_tsv"
        tmp_tsv_file = tsv_file + ".tmp"
        with open(tmp_tsv_file, "w", encoding="utf-8") as fd:
            header = "#location rel_dataset src_dataset dst_dataset"
            fd.write(header.replace(" ", "\t") + "\n")
            src_only: Set[str] = rel_datasets["src"].difference(rel_datasets["dst"])
            dst_only: Set[str] = rel_datasets["dst"].difference(rel_datasets["src"])
            for rel_dataset in rel_src_or_dst:
                loc = "src" if rel_dataset in src_only else "dst" if rel_dataset in dst_only else "all"
                src_dataset = src.root_dataset + rel_dataset if rel_dataset not in dst_only else ""
                dst_dataset = dst.root_dataset + rel_dataset if rel_dataset not in src_only else ""
                row = loc, rel_dataset, src_dataset, dst_dataset
                # Example: all /foo/bar tank1/src/foo/bar tank2/dst/foo/bar
                if not p.dry_run:
                    fd.write("\t".join(row) + "\n")
        os.rename(tmp_tsv_file, tsv_file)

    @staticmethod
    def print_datasets(group: groupby, func: Callable[[str, Iterable], None], rel_datasets: Iterable[str]) -> None:
        rel_datasets = sorted(rel_datasets)
        n = len(rel_datasets)
        i = 0
        for rel_dataset, entries in group:
            while i < n and rel_datasets[i] < rel_dataset:
                func(rel_datasets[i], [])  # Also print summary stats for datasets whose snapshot stream is empty
                i += 1
            assert i >= n or rel_datasets[i] == rel_dataset
            i += 1
            func(rel_dataset, entries)
        while i < n:
            func(rel_datasets[i], [])  # Also print summary stats for datasets whose snapshot stream is empty
            i += 1

    @staticmethod
    def merge_sorted_iterators(
        choices: List[str],  # ["src", "dst", "all"]
        choice: str,  # Example: "src+dst+all"
        src_itr: Generator[ComparableSnapshot, None, None],
        dst_itr: Generator[ComparableSnapshot, None, None],
    ) -> Generator[Tuple[str, ComparableSnapshot], None, None]:
        """This is the typical merge algorithm of a merge sort, slightly adapted to our specific use case."""
        assert len(choices) == 3
        assert choice
        flags = 0
        for i, item in enumerate(choices):
            if item in choice:
                flags |= 1 << i
        with ThreadPoolExecutor(max_workers=1) as executor:
            # perf: fetch initial results in parallel from src and dst in order to reduce latency
            future: Future = executor.submit(lambda: next(src_itr, None))  # async 'zfs list -t snapshot' on src
            dst_next = next(dst_itr, None)  # blocks until 'zfs list -t snapshot' CLI on dst returns with first results
            src_next = future.result()  # blocks until 'zfs list -t snapshot' CLI on src returns with first results
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

    def is_program_available(self, program: str, location: str) -> bool:
        return program in self.params.available_programs[location]

    def detect_available_programs(self) -> None:
        p = params = self.params
        log = p.log
        available_programs = params.available_programs
        if "local" not in available_programs:
            cmd = [p.shell_program_local, "-c", self.find_available_programs()]
            available_programs["local"] = dict.fromkeys(
                subprocess.run(cmd, stdout=PIPE, stderr=sys.stderr, text=True, check=False).stdout.splitlines()
            )
            cmd = [p.shell_program_local, "-c", "exit"]
            if subprocess.run(cmd, stdout=PIPE, stderr=sys.stderr, text=True).returncode != 0:
                self.disable_program("sh", ["local"])

        for r in [p.dst, p.src]:
            loc = r.location
            remote_conf_cache_key = r.cache_key()
            cache_item = self.remote_conf_cache.get(remote_conf_cache_key)
            if cache_item is not None:
                # startup perf: cache avoids ssh connect setup and feature detection roundtrips on revisits to same site
                available_programs[loc], p.zpool_features[loc], ssh_cmd = cache_item
                r.set_ssh_cmd(ssh_cmd)
                continue
            r.set_ssh_cmd(self.local_ssh_command(r))
            self.detect_zpool_features(r)
            self.detect_available_programs_remote(r, available_programs, r.ssh_user_host)
            self.remote_conf_cache[remote_conf_cache_key] = (available_programs[loc], p.zpool_features[loc], r.ssh_cmd)
            if r.use_zfs_delegation and p.zpool_features[loc].get("delegation") == "off":
                die(
                    f"Permission denied as ZFS delegation is disabled for {r.location} "
                    f"dataset: {r.basis_root_dataset}. Manually enable it via 'sudo zpool set delegation=on {r.pool}'"
                )

        locations = ["src", "dst", "local"]
        if params.compression_program == disable_prg:
            self.disable_program("zstd", locations)
        if params.mbuffer_program == disable_prg:
            self.disable_program("mbuffer", locations)
        if params.ps_program == disable_prg:
            self.disable_program("ps", locations)
        if params.pv_program == disable_prg:
            self.disable_program("pv", locations)
        if params.shell_program == disable_prg:
            self.disable_program("sh", locations)
        if params.sudo_program == disable_prg:
            self.disable_program("sudo", locations)
        if params.zpool_program == disable_prg:
            self.disable_program("zpool", locations)

        for key in locations:
            for program in list(available_programs[key].keys()):
                if program.startswith("uname-"):
                    # uname-Linux foo 5.15.0-69-generic #76-Ubuntu SMP Fri Mar 17 17:19:29 UTC 2023 x86_64 x86_64 x86_64 GNU/Linux
                    # uname-FreeBSD freebsd 14.1-RELEASE FreeBSD 14.1-RELEASE releng/14.1-n267679-10e31f0946d8 GENERIC amd64
                    # uname-SunOS solaris 5.11 11.4.42.111.0 i86pc i386 i86pc # https://blogs.oracle.com/solaris/post/building-open-source-software-on-oracle-solaris-114-cbe-release
                    # uname-SunOS solaris 5.11 11.4.0.15.0 i86pc i386 i86pc
                    # uname-Darwin foo 23.6.0 Darwin Kernel Version 23.6.0: Mon Jul 29 21:13:04 PDT 2024; root:xnu-10063.141.2~1/RELEASE_ARM64_T6020 arm64
                    available_programs[key].pop(program)
                    uname = program[len("uname-") :]
                    available_programs[key]["uname"] = uname
                    log.trace(f"available_programs[{key}][uname]: %s", uname)
                    available_programs[key]["os"] = uname.split(" ")[0]  # Linux|FreeBSD|SunOS|Darwin
                    log.trace(f"available_programs[{key}][os]: %s", available_programs[key]["os"])
                elif program.startswith("getconf_cpu_count-"):
                    available_programs[key].pop(program)
                    getconf_cpu_count = program[len("getconf_cpu_count-") :]
                    available_programs[key]["getconf_cpu_count"] = getconf_cpu_count
                    log.trace(f"available_programs[{key}][getconf_cpu_count]: %s", getconf_cpu_count)

        for key, value in available_programs.items():
            log.debug(f"available_programs[{key}]: %s", list_formatter(value, separator=", "))

        for r in [p.dst, p.src]:
            if r.sudo and not self.is_program_available("sudo", r.location):
                die(f"{p.sudo_program} CLI is not available on {r.location} host: {r.ssh_user_host or 'localhost'}")

    def disable_program(self, program: str, locations: List[str]) -> None:
        for location in locations:
            self.params.available_programs[location].pop(program, None)

    def find_available_programs(self) -> str:
        params = self.params
        return f"""
        command -v echo > /dev/null && echo echo
        command -v {params.zpool_program} > /dev/null && echo zpool
        command -v {params.ssh_program} > /dev/null && echo ssh
        command -v {params.shell_program} > /dev/null && echo sh
        command -v {params.sudo_program} > /dev/null && echo sudo
        command -v {params.compression_program} > /dev/null && echo zstd
        command -v {params.mbuffer_program} > /dev/null && echo mbuffer
        command -v {params.pv_program} > /dev/null && echo pv
        command -v {params.ps_program} > /dev/null && echo ps
        command -v {params.psrinfo_program} > /dev/null && printf getconf_cpu_count- && {params.psrinfo_program} -p  # print num CPUs on Solaris
        ! command -v {params.psrinfo_program} && command -v {params.getconf_program} > /dev/null && printf getconf_cpu_count- && {params.getconf_program} _NPROCESSORS_ONLN  # print num CPUs on POSIX except Solaris
        command -v {params.uname_program} > /dev/null && printf uname- && {params.uname_program} -a || true
        """

    def detect_available_programs_remote(self, remote: Remote, available_programs: Dict, ssh_user_host: str) -> None:
        p, log = self.params, self.params.log
        location = remote.location
        available_programs_minimum = {"zpool": None, "sudo": None}
        available_programs[location] = {}
        lines = None
        try:
            # on Linux, 'zfs --version' returns with zero status and prints the correct info
            # on FreeBSD, 'zfs --version' always prints the same (correct) info as Linux, but nonetheless sometimes
            # returns with non-zero status (sometimes = if the zfs kernel module is not loaded)
            # on Solaris, 'zfs --version' returns with non-zero status without printing useful info as the --version
            # option is not known there
            lines = self.run_ssh_command(remote, log_trace, print_stderr=False, cmd=[p.zfs_program, "--version"])
            assert lines
        except (FileNotFoundError, PermissionError):  # location is local and program file was not found
            die(f"{p.zfs_program} CLI is not available on {location} host: {ssh_user_host or 'localhost'}")
        except subprocess.CalledProcessError as e:
            if "unrecognized command '--version'" in e.stderr and "run: zfs help" in e.stderr:
                available_programs[location]["zfs"] = "notOpenZFS"  # solaris-11.4 zfs does not know --version flag
            elif not e.stdout.startswith("zfs"):
                die(f"{p.zfs_program} CLI is not available on {location} host: {ssh_user_host or 'localhost'}")
            else:
                lines = e.stdout  # FreeBSD if the zfs kernel module is not loaded
                assert lines
        if lines:
            line = lines.splitlines()[0]
            assert line.startswith("zfs")
            # Example: zfs-2.1.5~rc5-ubuntu3 -> 2.1.5, zfswin-2.2.3rc5 -> 2.2.3
            version = line.split("-")[1].strip()
            match = re.fullmatch(r"(\d+\.\d+\.\d+).*", version)
            assert match, "Unparsable zfs version string: " + version
            version = match.group(1)
            available_programs[location]["zfs"] = version
            if is_version_at_least(version, "2.1.0"):
                available_programs[location][zfs_version_is_at_least_2_1_0] = True
        log.trace(f"available_programs[{location}][zfs]: %s", available_programs[location]["zfs"])

        if p.shell_program != disable_prg:
            try:
                cmd = [p.shell_program, "-c", self.find_available_programs()]
                available_programs[location].update(
                    dict.fromkeys(self.run_ssh_command(remote, log_trace, cmd=cmd).splitlines())
                )
                return
            except (FileNotFoundError, PermissionError) as e:  # location is local and shell program file was not found
                if e.filename != p.shell_program:
                    raise
            except subprocess.CalledProcessError:
                pass
            log.warning("%s", f"Failed to find {p.shell_program} on {location}. Continuing with minimal assumptions...")
        available_programs[location].update(available_programs_minimum)

    def is_solaris_zfs(self, remote: Remote) -> bool:
        return self.params.available_programs[remote.location].get("zfs") == "notOpenZFS"

    @staticmethod
    def is_dummy_src(r: Remote) -> bool:
        return r.root_dataset == dummy_dataset and r.location == "src"

    def detect_zpool_features(self, remote: Remote) -> None:
        p = params = self.params
        r, loc, log = remote, remote.location, p.log
        lines = []
        features = {}
        if self.is_dummy_src(r):
            params.zpool_features[loc] = {}
            return
        if params.zpool_program != disable_prg:
            cmd = params.split_args(f"{params.zpool_program} get -Hp -o property,value all", r.pool)
            try:
                lines = self.run_ssh_command(remote, log_trace, check=False, cmd=cmd).splitlines()
            except (FileNotFoundError, PermissionError) as e:
                if e.filename != params.zpool_program:
                    raise
                log.warning(
                    "%s", f"Failed to detect zpool features on {loc}: {r.pool}. Continuing with minimal assumptions ..."
                )
            else:
                props = {line.split("\t", 1)[0]: line.split("\t", 1)[1] for line in lines}
                features = {k: v for k, v in props.items() if k.startswith("feature@") or k == "delegation"}
        if len(lines) == 0:
            cmd = p.split_args(f"{p.zfs_program} list -t filesystem -Hp -o name -s name", r.pool)
            if self.try_ssh_command(remote, log_trace, cmd=cmd) is None:
                die(f"Pool does not exist for {loc} dataset: {r.basis_root_dataset}. Manually create the pool first!")
        params.zpool_features[loc] = features

    def is_zpool_feature_enabled_or_active(self, remote: Remote, feature: str) -> bool:
        return self.params.zpool_features[remote.location].get(feature) in ("active", "enabled")

    def are_bookmarks_enabled(self, remote: Remote) -> bool:
        return self.is_zpool_feature_enabled_or_active(
            remote, "feature@bookmark_v2"
        ) and self.is_zpool_feature_enabled_or_active(remote, "feature@bookmark_written")

    def check_zfs_dataset_busy(self, remote: Remote, dataset: str, busy_if_send: bool = True) -> bool:
        """Decline to start a state changing ZFS operation that may conflict with other currently running processes.
        Instead, retry the operation later and only execute it when it's become safe. For example, decline to start
        a 'zfs receive' into a destination dataset if another process is already running another 'zfs receive' into
        the same destination dataset. However, it's actually safe to run an incremental 'zfs receive' into a dataset
        in parallel with a 'zfs send' out of the very same dataset. This also helps daisy chain use cases where
        A replicates to B, and B replicates to C."""
        p, log = self.params, self.params.log
        if not self.is_program_available("ps", remote.location):
            return True
        cmd = p.split_args(f"{p.ps_program} -Ao args")
        procs = (self.try_ssh_command(remote, log_trace, cmd=cmd) or "").splitlines()
        if self.inject_params.get("is_zfs_dataset_busy", False):
            procs += ["sudo zfs receive -u -o foo:bar=/baz " + dataset]  # for unit testing only
        if not self.is_zfs_dataset_busy(procs, dataset, busy_if_send=busy_if_send):
            return True
        op = "zfs {receive" + ("|send" if busy_if_send else "") + "} operation"
        try:
            die(f"Cannot continue now: Destination is already busy with {op} from another process: {dataset}")
        except SystemExit as e:
            log.warning("%s", str(e))
            raise RetryableError("dst currently busy with zfs mutation op") from e

    def is_zfs_dataset_busy(self, procs: List[str], dataset: str, busy_if_send: bool) -> bool:
        regex = self.zfs_dataset_busy_if_send if busy_if_send else self.zfs_dataset_busy_if_mods
        suffix = " " + dataset
        infix = " " + dataset + "@"
        return any(filter(lambda proc: (proc.endswith(suffix) or infix in proc) and regex.fullmatch(proc), procs))

    @staticmethod
    def get_is_zfs_dataset_busy_regexes() -> Tuple[re.Pattern, re.Pattern]:
        zfs_dataset_busy_prefix = r"(([^ ]*?/)?(sudo|doas) )?([^ ]*?/)?zfs (receive|recv"
        zfs_dataset_busy_if_mods = (zfs_dataset_busy_prefix + ") .*").replace("(", "(?:")
        zfs_dataset_busy_if_send = (zfs_dataset_busy_prefix + "|send) .*").replace("(", "(?:")
        return re.compile(zfs_dataset_busy_if_mods), re.compile(zfs_dataset_busy_if_send)

    def run_ssh_cmd_batched(
        self, r: Remote, cmd: List[str], cmd_args: List[str], func: Callable[[List[str]], Any], max_batch_items=2**29
    ) -> None:
        deque(self.itr_ssh_cmd_batched(r, cmd, cmd_args, func, max_batch_items=max_batch_items), maxlen=0)

    def itr_ssh_cmd_batched(
        self, r: Remote, cmd: List[str], cmd_args: List[str], func: Callable[[List[str]], Any], max_batch_items=2**29
    ) -> Generator[Any, None, None]:
        """Runs func(cmd_args) in batches w/ cmd, without creating a command line that's too big for the OS to handle"""
        max_bytes = min(self.get_max_command_line_bytes("local"), self.get_max_command_line_bytes(r.location))
        fsenc = sys.getfilesystemencoding()
        header_bytes = len(" ".join(r.ssh_cmd + cmd).encode(fsenc))
        batch: List[str] = []
        total_bytes: int = header_bytes

        def flush() -> Any:
            if len(batch) > 0:
                return func(batch)

        for cmd_arg in cmd_args:
            curr_bytes = len(f" {cmd_arg}".encode(fsenc))
            if total_bytes + curr_bytes > max_bytes or len(batch) >= max_batch_items:
                results = flush()
                if results is not None:
                    yield results
                batch, total_bytes = [], header_bytes
            batch.append(cmd_arg)
            total_bytes += curr_bytes
        results = flush()
        if results is not None:
            yield results

    def itr_ssh_command_parallel(self, r: Remote, cmd: List[str], datasets: List[str]) -> Generator:
        max_workers = self.max_workers[r.location]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            iterator = self.itr_ssh_cmd_batched(
                r,
                cmd,
                datasets,
                lambda batch: executor.submit(
                    lambda: (self.try_ssh_command(r, log_trace, cmd=cmd + batch) or "").splitlines()
                ),
                self.max_datasets_per_minibatch_on_list_snaps[r.location],
            )
            # Materialize the next N futures into a buffer, causing submission + parallel execution of their CLI calls
            fifo_buffer: deque[Future] = deque(itertools.islice(iterator, max_workers))

            while fifo_buffer:
                curr_future: Future = fifo_buffer.popleft()
                next_future: Future = next(iterator, None)
                if next_future is not None:
                    fifo_buffer.append(next_future)
                yield curr_future.result()  # blocks until CLI returns

    def get_max_command_line_bytes(self, location: str, os_name: Optional[str] = None) -> int:
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
    opts: List[str],
    exclude_long_opts: Set[str],
    exclude_short_opts: str,
    include_arg_opts: Set[str],
    exclude_arg_opts: Set[str] = set(),
) -> List[str]:
    """These opts are instead managed via bzfs CLI args --dryrun, etc."""
    assert "-" not in exclude_short_opts
    results = []
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
                results.append(opts[i])
                i += 1
        elif opt not in exclude_long_opts:  # example: {"--dryrun", "--verbose"}
            if opt.startswith("-") and opt != "-" and not opt.startswith("--"):
                for char in exclude_short_opts:  # example: "den"
                    opt = opt.replace(char, "")
                if opt == "-":
                    continue
            results.append(opt)
    return results


def fix_solaris_raw_mode(lst: List[str]) -> List[str]:
    lst = ["-w" if opt == "--raw" else opt for opt in lst]
    lst = ["compress" if opt == "--compressed" else opt for opt in lst]
    i = lst.index("-w") if "-w" in lst else -1
    if i >= 0:
        i += 1
        if i == len(lst) or (lst[i] != "none" and lst[i] != "compress"):
            lst.insert(i, "none")
    return lst


def delete_stale_files(root_dir: str, prefix: str, secs: int = 30 * 24 * 60 * 60, dirs=False, exclude=None) -> None:
    """Cleans up obsolete files. For example caused by abnormal termination, OS crash."""
    now = time.time()
    for entry in os.scandir(root_dir):
        if entry.name == exclude or not entry.name.startswith(prefix):
            continue
        try:
            if ((dirs and entry.is_dir()) or (not dirs and not entry.is_dir())) and now - entry.stat().st_mtime >= secs:
                if dirs:
                    shutil.rmtree(entry.path, ignore_errors=True)
                else:
                    os.remove(entry.path)
        except FileNotFoundError:
            pass  # harmless


def die(msg: str) -> None:
    ex = SystemExit(msg)
    ex.code = die_status
    raise ex


def cut(field: int = -1, separator: str = "\t", lines: List[str] = None) -> List[str]:
    """Retains only column number 'field' in a list of TSV/CSV lines; Analog to Unix 'cut' CLI command."""
    if field == 1:
        return [line[0 : line.index(separator)] for line in lines]
    elif field == 2:
        return [line[line.index(separator) + 1 :] for line in lines]
    else:
        raise ValueError("Unsupported parameter value")


def is_descendant(dataset: str, of_root_dataset: str) -> bool:
    return f"{dataset}/".startswith(f"{of_root_dataset}/")


def relativize_dataset(dataset: str, root_dataset: str) -> str:
    """Converts an absolute dataset path to a relative dataset path wrt root_dataset
    Example: root_dataset=tank/foo, dataset=tank/foo/bar/baz --> relative_path=/bar/baz"""
    return dataset[len(root_dataset) :]


def replace_prefix(s: str, old_prefix: str, new_prefix: str) -> str:
    """In a string s, replaces a leading old_prefix string with new_prefix. Assumes the leading string is present."""
    return new_prefix + s[len(old_prefix) :]


def replace_in_lines(lines: List[str], old: str, new: str) -> None:
    for i in range(len(lines)):
        lines[i] = lines[i].replace(old, new)


def is_included(name: str, include_regexes: RegexList, exclude_regexes: RegexList) -> bool:
    """Returns True if the name matches at least one of the include regexes but none of the exclude regexes;
    else False. A regex that starts with a `!` is a negation - the regex matches if the regex without the
    `!` prefix does not match."""
    for regex, is_negation in exclude_regexes:
        is_match = regex.fullmatch(name) if regex.pattern != ".*" else True
        if is_negation:
            is_match = not is_match
        if is_match:
            return False

    for regex, is_negation in include_regexes:
        is_match = regex.fullmatch(name) if regex.pattern != ".*" else True
        if is_negation:
            is_match = not is_match
        if is_match:
            return True

    return False


def compile_regexes(regexes: List[str], suffix: str = "") -> RegexList:
    assert isinstance(regexes, list)
    compiled_regexes = []
    for regex in regexes:
        is_negation = regex.startswith("!")
        if is_negation:
            regex = regex[1:]
        regex = replace_capturing_groups_with_non_capturing_groups(regex)
        if regex != ".*" or not (suffix.startswith("(") and suffix.endswith(")?")):
            regex = f"{regex}{suffix}"
        compiled_regexes.append((re.compile(regex), is_negation))
    return compiled_regexes


def replace_capturing_groups_with_non_capturing_groups(regex: str) -> str:
    """Replaces regex capturing groups with non-capturing groups for better matching performance.
    Example: '(.*/)?tmp(foo|bar)(?!public)\\(' --> '(?:.*/)?tmp(?:foo|bar)(?!public)\\()'
    Aka replaces brace '(' followed by a char other than question mark '?', but not preceded by a backslash
    with the replacement string '(?:'
    Also see https://docs.python.org/3/howto/regex.html#non-capturing-and-named-groups
    """
    # pattern = re.compile(r'(?<!\\)\((?!\?)')
    # return pattern.sub('(?:', regex)
    i = len(regex) - 2
    while i >= 0:
        i = regex.rfind("(", 0, i + 1)
        if i >= 0 and regex[i] == "(" and (regex[i + 1] != "?") and (i == 0 or regex[i - 1] != "\\"):
            regex = f"{regex[0:i]}(?:{regex[i + 1:]}"
        i -= 1
    return regex


def getenv_any(key: str, default=None) -> str:
    """All shell environment variable names used for configuration start with this prefix."""
    return os.getenv(env_var_prefix + key, default)


def getenv_bool(key: str, default: bool = False) -> bool:
    return getenv_any(key, str(default).lower()).strip().lower() == "true"


def isorted(iterable: Iterable[str], reverse: bool = False) -> List[str]:
    """case-insensitive sort (A < a < B < b and so on)."""
    return sorted(iterable, key=str.casefold, reverse=reverse)


def xappend(lst, *items) -> List[str]:
    """Appends each of the items to the given list if the item is "truthy", e.g. not None and not an empty string.
    If an item is an iterable does so recursively, flattening the output."""
    for item in items:
        if isinstance(item, str) or not isinstance(item, collections.abc.Iterable):
            if item:
                lst.append(item)
        else:
            xappend(lst, *item)
    return lst


def human_readable_bytes(size: int, long=True) -> str:
    sign = "-" if size < 0 else ""
    s = abs(size)
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB")
    i = 0
    long_form = f" ({size} bytes)" if long else ""
    while s >= 1024 and i < len(units) - 1:
        s //= 1024
        i += 1
    return f"{sign}{s} {units[i]}{long_form}"


def human_readable_duration(duration: int, unit="ns", long=True) -> str:
    sign = "-" if duration < 0 else ""
    t = abs(duration)
    units = ("ns", "μs", "ms", "s", "m", "h", "d")
    seconds = (1.0 / 1000_000_000, 1.0 / 1000_000, 1.0 / 1000, 1, 60, 60 * 60, 60 * 60 * 24)
    i = units.index(unit)
    long_form = f" ({round(duration * seconds[i])} seconds)" if long else ""
    while t >= 1000 and i < 3:
        t //= 1000
        i += 1
    if i >= 3:
        while t >= 60 and i < 5:
            t //= 60
            i += 1
    if i >= 5:
        while t >= 24 and i < len(units) - 1:
            t //= 24
            i += 1
    return f"{sign}{t} {units[i]}{long_form}"


def get_home_directory() -> str:
    """Reliably detects home dir without using HOME env var."""
    # thread-safe version of: os.environ.pop('HOME', None); os.path.expanduser('~')
    return pwd.getpwuid(os.getuid()).pw_dir


def create_symlink(src: str, dst_dir: str, dst: str) -> None:
    rel_path = os.path.relpath(src, start=dst_dir)
    os.symlink(rel_path, os.path.join(dst_dir, dst))


def is_version_at_least(version_str: str, min_version_str: str) -> bool:
    """Checks if the version string is at least the minimum version string."""
    return tuple(map(int, version_str.split("."))) >= tuple(map(int, min_version_str.split(".")))


def tail(file, n: int):
    if not os.path.isfile(file):
        return []
    with open(file, "r", encoding="utf-8") as fd:
        return deque(fd, maxlen=n)


def append_if_absent(lst: List, *items) -> List:
    for item in items:
        if item not in lst:
            lst.append(item)
    return lst


def stderr_to_str(stderr) -> str:
    """Workaround for https://github.com/python/cpython/issues/87597"""
    return stderr if not isinstance(stderr, bytes) else stderr.decode("utf-8")


def xprint(log: Logger, value, run: bool = True, end: str = "\n", file=None) -> None:
    if run and value:
        value = value if end else value.rstrip()
        level = log_stdout if file is sys.stdout else log_stderr
        log.log(level, "%s", value)


def unlink_missing_ok(file: str) -> None:  # workaround for compat with python < 3.8
    try:
        Path(file).unlink()
    except FileNotFoundError:
        pass


def terminate_process_group(except_current_process=False):
    """Sends SIGTERM to the entire process group to also terminate child processes started via subprocess.run()"""
    signalnum = signal.SIGTERM
    old_sigterm_handler = (
        signal.signal(signalnum, lambda signum, frame: None)  # temporarily disable signal handler on current process
        if except_current_process
        else signal.getsignal(signalnum)
    )
    try:
        is_test = any("unittest" in frame.filename for frame in inspect.stack())
        is_test or os.killpg(os.getpgrp(), signalnum)  # avoid confusing python's unit test framework with killpg()
    finally:
        signal.signal(signalnum, old_sigterm_handler)  # reenable and restore original handler


def parse_dataset_locator(input_text: str, validate: bool = True, user: str = None, host: str = None, port: int = None):
    def convert_ipv6(hostname: str) -> str:  # support IPv6 without getting confused by host:dataset colon separator
        return hostname.replace("|", ":")

    user_undefined = user is None
    if user is None:
        user = ""
    host_undefined = host is None
    if host is None:
        host = ""
    host = convert_ipv6(host)
    user_host, dataset, pool = "", "", ""

    # Input format is [[user@]host:]dataset
    #                      1234         5          6
    match = re.fullmatch(r"(((([^@]*)@)?([^:]+)):)?(.*)", input_text, re.DOTALL)
    if match:
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
        validate_port(port, f"Invalid port number: '{port}' for: '{input_text}' - ")
        validate_dataset_name(dataset, input_text)

    return user, host, user_host, pool, dataset


def validate_dataset_name(dataset: str, input_text: str) -> None:
    # 'zfs create' CLI does not accept dataset names that are empty or start or end in a slash, etc.
    # Also see https://github.com/openzfs/zfs/issues/439#issuecomment-2784424
    # and https://github.com/openzfs/zfs/issues/8798
    # and (by now nomore accurate): https://docs.oracle.com/cd/E26505_01/html/E37384/gbcpt.html
    if (
        dataset in ["", ".", ".."]
        or "//" in dataset
        or dataset.startswith("/")
        or dataset.endswith("/")
        or dataset.startswith("./")
        or dataset.startswith("../")
        or dataset.endswith("/.")
        or dataset.endswith("/..")
        or "@" in dataset
        or "#" in dataset
        or '"' in dataset
        or "'" in dataset
        or "`" in dataset
        or "%" in dataset
        or "$" in dataset
        or "\\" in dataset
        or any(char.isspace() and char != " " for char in dataset)
        or not dataset[0].isalpha()
    ):
        die(f"Invalid ZFS dataset name: '{dataset}' for: '{input_text}'")


def validate_user_name(user: str, input_text: str) -> None:
    if user and any(char.isspace() or char == '"' or char == "'" or char == "`" for char in user):
        die(f"Invalid user name: '{user}' for: '{input_text}'")


def validate_host_name(host: str, input_text: str) -> None:
    if host and any(char.isspace() or char == "@" or char == '"' or char == "'" or char == "`" for char in host):
        die(f"Invalid host name: '{host}' for: '{input_text}'")


def validate_port(port: int, message: str) -> None:
    if isinstance(port, int):
        port = str(port)
    if port and not port.isdigit():
        die(message + f"must be empty or a positive integer: '{port}'")


def list_formatter(iterable: Iterable, separator=" ", lstrip=False):  # For lazy/noop evaluation in disabled log levels
    class CustomListFormatter:
        def __str__(self):
            s = separator.join(map(str, iterable))
            return s.lstrip() if lstrip else s

    return CustomListFormatter()


def pretty_print_formatter(obj_to_format):  # For lazy/noop evaluation in disabled log levels
    class PrettyPrintFormatter:
        def __str__(self):
            return pprint.pformat(vars(obj_to_format))

    return PrettyPrintFormatter()


def reset_logger() -> None:
    """Remove and close logging handlers (and close their files) and reset loggers to default state."""
    for log in [logging.getLogger(__name__), logging.getLogger(get_logger_subname())]:
        for handler in log.handlers.copy():
            log.removeHandler(handler)
            handler.flush()
            handler.close()
        for _filter in log.filters.copy():
            log.removeFilter(_filter)
        log.setLevel(logging.NOTSET)
        log.propagate = True


def get_logger_subname() -> str:
    return __name__ + ".sub"  # the logger name for use by --log-config-file


def get_logger(log_params: LogParams, args: argparse.Namespace, log: Optional[Logger] = None) -> Logger:
    _log_trace = log_trace
    if not hasattr(logging.Logger, "trace"):  # add convenience function for custom log level to the logger
        logging.Logger.trace = lambda self, msg, *arguments: (
            self._log(_log_trace, msg, arguments) if self.isEnabledFor(_log_trace) else None
        )
    logging.addLevelName(log_trace, "TRACE")
    logging.addLevelName(log_stderr, "STDERR")
    logging.addLevelName(log_stdout, "STDOUT")

    if log is not None:
        assert isinstance(log, Logger)
        return log  # use third party provided logger object
    elif args.log_config_file:
        log = get_dict_config_logger(log_params, args)  # use logger defined in config file, and afterwards ...
    # ... add our own handlers unless matching handlers are already present
    default_log = get_default_logger(log_params, args)
    return log if args.log_config_file else default_log


def get_default_logger(log_params: LogParams, args: argparse.Namespace) -> Logger:
    sublog = logging.getLogger(get_logger_subname())
    log = logging.getLogger(__name__)
    log.setLevel(log_params.log_level)
    log.propagate = False  # don't propagate log messages up to the root logger to avoid emitting duplicate messages

    if not any(isinstance(h, logging.StreamHandler) and h.stream in [sys.stdout, sys.stderr] for h in sublog.handlers):
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(get_default_log_formatter())
        handler.setLevel(log_params.log_level)
        log.addHandler(handler)

    abs_log_file = os.path.abspath(log_params.log_file)
    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == abs_log_file for h in sublog.handlers):
        handler = logging.FileHandler(log_params.log_file, encoding="utf-8")
        handler.setFormatter(get_default_log_formatter())
        handler.setLevel(log_params.log_level)
        log.addHandler(handler)

    address = args.log_syslog_address
    if address:  # optionally, also log to local or remote syslog
        address, socktype = get_syslog_address(address, args.log_syslog_socktype)
        log_syslog_prefix = str(args.log_syslog_prefix).strip().replace("%", "")  # sanitize
        handler = logging.handlers.SysLogHandler(address=address, facility=args.log_syslog_facility, socktype=socktype)
        handler.setFormatter(get_default_log_formatter(prefix=log_syslog_prefix + " "))
        handler.setLevel(args.log_syslog_level)
        log.addHandler(handler)
        if handler.level < sublog.getEffectiveLevel():
            log_level_name = logging.getLevelName(sublog.getEffectiveLevel())
            log.warning(
                "%s",
                f"No messages with priority lower than {log_level_name} will be sent to syslog because syslog "
                f"log level {args.log_syslog_level} is lower than overall log level {log_level_name}.",
            )

    # perf: tell logging framework not to gather unnecessary expensive info for each log record
    logging.logProcesses = False
    logging.logThreads = False
    logging.logMultiprocessing = False
    return log


def get_default_log_formatter(prefix: str = "") -> logging.Formatter:
    level_prefixes = {
        logging.CRITICAL: "[C] CRITICAL:",
        logging.ERROR: "[E] ERROR:",
        logging.WARNING: "[W]",
        logging.INFO: "[I]",
        logging.DEBUG: "[D]",
        log_trace: "[T]",
    }
    _log_stderr = log_stderr
    _log_stdout = log_stdout

    class DefaultLogFormatter(logging.Formatter):
        def format(self, record) -> str:
            levelno = record.levelno
            if levelno != _log_stderr and levelno != _log_stdout:  # emit stdout and stderr "as-is" (no formatting)
                timestamp = datetime.now().isoformat(sep=" ", timespec="seconds")
                ts_level = f"{timestamp} {level_prefixes.get(levelno, '')} "
                msg = record.msg
                i = msg.find("%s")
                msg = ts_level + msg
                if i >= 1:
                    i += len(ts_level)
                    msg = msg[0:i].ljust(54) + msg[i:]  # right-pad msg if record.msg contains "%s" unless at start
                if record.args:
                    msg = msg % record.args
                return prefix + msg
            return prefix + super().format(record)

    return DefaultLogFormatter()


def get_syslog_address(address: str, log_syslog_socktype: str) -> Tuple:
    socktype = None
    address = address.strip()
    if ":" in address:
        host, port = address.rsplit(":", 1)
        address = (host.strip(), int(port.strip()))
        socktype = socket.SOCK_DGRAM if log_syslog_socktype == "UDP" else socket.SOCK_STREAM  # for TCP
    return address, socktype


def get_dict_config_logger(log_params: LogParams, args: argparse.Namespace) -> Logger:
    prefix = prog_name + "."
    log_config_vars = {
        prefix + "sub.logger": get_logger_subname(),
        prefix + "get_default_log_formatter": __name__ + ".get_default_log_formatter",
        prefix + "log_level": log_params.log_level,
        prefix + "log_dir": log_params.log_dir,
        prefix + "log_file": os.path.basename(log_params.log_file),
        prefix + "timestamp": log_params.timestamp,
        prefix + "dryrun": "dryrun" if args.dryrun else "",
    }
    log_config_vars.update(log_params.log_config_vars)  # merge variables passed into CLI with convenience variables

    log_config_file_str = log_params.log_config_file
    if log_config_file_str.startswith("+"):
        with open(log_config_file_str[1:], "r", encoding="utf-8") as fd:
            log_config_file_str = fd.read()

    def remove_json_comments(config_str: str) -> str:  # not standard but practical
        lines = []
        for line in config_str.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                line = ""  # replace comment line with empty line to preserve line numbering
            elif stripped.endswith("#"):
                i = line.rfind("#", 0, line.rindex("#"))
                if i >= 0:
                    line = line[0:i]  # strip line-ending comment
            lines.append(line)
        return "\n".join(lines)

    def substitute_log_config_vars(config_str: str, log_config_variables: Dict[str, str]) -> str:
        """Substitute ${name[:default]} placeholders within JSON with values from log_config_variables"""

        def substitute_fn(match: re.Match) -> str:
            varname = match.group(1)
            error_msg = validate_log_config_variable_name(varname)
            if error_msg:
                raise ValueError(error_msg)
            replacement = log_config_variables.get(varname)
            if not replacement:
                default = match.group(3)
                if default is None:
                    raise ValueError("Missing default value in JSON for empty log config variable: ${" + varname + "}")
                replacement = default
            replacement = json.dumps(replacement)  # JSON escape special chars such as newlines, quotes, etc
            assert len(replacement) >= 2
            assert replacement.startswith('"')
            assert replacement.endswith('"')
            return replacement[1:-1]  # strip surrounding quotes added by dumps()

        pattern = re.compile(r"\$\{([^}:]*?)(:([^}]*))?}")  # Any char except } and :, followed by optional default part
        return pattern.sub(substitute_fn, config_str)

    log_config_file_str = remove_json_comments(log_config_file_str)
    if not log_config_file_str.strip().startswith("{"):
        log_config_file_str = "{\n" + log_config_file_str  # lenient JSON parsing
    if not log_config_file_str.strip().endswith("}"):
        log_config_file_str = log_config_file_str + "\n}"  # lenient JSON parsing
    log_config_file_str = substitute_log_config_vars(log_config_file_str, log_config_vars)
    if args is not None and args.verbose >= 2:
        print("[T] Substituted log_config_file_str:\n" + log_config_file_str, flush=True)
    log_config_dict = json.loads(log_config_file_str)
    logging.config.dictConfig(log_config_dict)
    return logging.getLogger(get_logger_subname())


def validate_log_config_variable(var: str) -> str:
    if not var.strip():
        return "Invalid log config NAME:VALUE variable. Variable must not be empty: " + var
    if ":" not in var:
        return "Invalid log config NAME:VALUE variable. Variable is missing a colon character: " + var
    return validate_log_config_variable_name(var[0 : var.index(":")])


def validate_log_config_variable_name(name: str):
    if not name:
        return "Invalid log config variable name. Name must not be empty: " + name
    bad_chars = "${} " + '"' + "'"
    if any(char in bad_chars for char in name):
        return f"Invalid log config variable name. Name must not contain forbidden {bad_chars} characters: " + name
    if any(char.isspace() for char in name):
        return "Invalid log config variable name. Name must not contain whitespace: " + name
    return None


def unixtime_fromisoformat(datetime_str: str) -> int:
    """Converts an ISO 8601 datetime string into a UTC Unix time in integer seconds. If the datetime string does not
    contain time zone info then it is assumed to be in the local time zone."""
    return int(datetime.fromisoformat(datetime_str).timestamp())


def isotime_from_unixtime(unixtime_in_seconds: int) -> str:
    """Converts a UTC Unix time in integer seconds into an ISO 8601 datetime string in the local time zone.
    Example: 2024-09-03_12:26:15"""
    tz = timezone.utc  # outputs time in UTC
    tz = None  # outputs time in local time zone
    dt = datetime.fromtimestamp(unixtime_in_seconds, tz=tz)
    return dt.isoformat(sep="_", timespec="seconds")


#############################################################################
class RetryableError(Exception):
    """Indicates that the task that caused the underlying exception can be retried and might eventually succeed."""

    def __init__(self, message, no_sleep: bool = False):
        super().__init__(message)
        self.no_sleep: bool = no_sleep


#############################################################################
class Tee:
    def __init__(self, *files):
        self.files = files

    def write(self, obj) -> None:
        for file in self.files:
            file.write(obj)
            file.flush()  # Ensure each write is flushed immediately

    def flush(self) -> None:
        for file in self.files:
            file.flush()

    def fileno(self) -> int:
        return self.files[0].fileno()


#############################################################################
class NonEmptyStringAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        values = values.strip()
        if values == "":
            parser.error(f"{option_string}: Empty string is not valid")
        setattr(namespace, self.dest, values)


#############################################################################
class DatasetPairsAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        datasets = []
        for value in values:
            if not value.startswith("+"):
                datasets.append(value)
            else:
                try:
                    with open(value[1:], "r", encoding="utf-8") as fd:
                        for line in fd.read().splitlines():
                            if not line.strip() or line.startswith("#"):
                                continue  # skip empty lines and comment lines
                            splits = line.split("\t", 1)
                            if len(splits) <= 1:
                                parser.error("Line must contain tab-separated SRC_DATASET and DST_DATASET: " + line)
                            src_root_dataset, dst_root_dataset = splits
                            if not src_root_dataset.strip() or not dst_root_dataset.strip():
                                parser.error("SRC_DATASET and DST_DATASET must not be empty or whitespace-only:" + line)
                            datasets.append(src_root_dataset)
                            datasets.append(dst_root_dataset)
                except FileNotFoundError:
                    parser.error(f"File not found: {value[1:]}")

        if len(datasets) % 2 != 0:
            parser.error("Each SRC_DATASET must have a corresponding DST_DATASET.")
        root_dataset_pairs = [(datasets[i], datasets[i + 1]) for i in range(0, len(datasets), 2)]
        setattr(namespace, self.dest, root_dataset_pairs)


#############################################################################
class SafeFileNameAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if ".." in values or "/" in values or "\\" in values:
            parser.error(f"Invalid file name '{values}': must not contain '..' or '/' or '\\'.")
        setattr(namespace, self.dest, values)


#############################################################################
class FileOrLiteralAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        current_values = getattr(namespace, self.dest, None)
        if current_values is None:
            current_values = []
        extra_values = []
        for value in values:
            if not value.startswith("+"):
                extra_values.append(value)
            else:
                try:
                    with open(value[1:], "r", encoding="utf-8") as fd:
                        for line in fd.read().splitlines():
                            if not line.strip() or line.startswith("#"):
                                continue  # skip empty lines and comment lines
                            extra_values.append(line)
                except FileNotFoundError:
                    parser.error(f"File not found: {value[1:]}")
        current_values += extra_values
        setattr(namespace, self.dest, current_values)
        if self.dest in snapshot_regex_filter_names:
            add_snapshot_filter(namespace, SnapshotFilter(self.dest, None, extra_values))


#############################################################################
class TimeRangeAndRankRangeAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        def parse_time(time_spec):
            time_spec = time_spec.strip()
            if time_spec == "*":
                return None
            if time_spec.isdigit():
                return int(time_spec)  # Input is a Unix time in integer seconds
            try:
                return timedelta(seconds=self.parse_duration_to_seconds(time_spec))
            except ValueError:
                try:  # If it's not a duration, try parsing as an ISO 8601 datetime
                    return unixtime_fromisoformat(time_spec)
                except ValueError:
                    parser.error(f"{option_string}: Invalid duration, Unix time, or ISO 8601 datetime: {time_spec}")

        assert isinstance(values, list)
        assert len(values) > 0
        value = values[0].strip()
        if ".." not in value:
            parser.error(f"{option_string}: Invalid time range: Missing '..' separator: {value}")
        timerange = [parse_time(time_spec) for time_spec in value.split("..", 1)]
        rankranges = self.parse_rankranges(parser, values[1:], option_string=option_string)
        setattr(namespace, self.dest, [timerange] + rankranges)  # for testing only
        timerange = self.get_include_snapshot_times(timerange)
        add_time_and_rank_snapshot_filter(namespace, self.dest, timerange, rankranges)

    @staticmethod
    def parse_duration_to_seconds(duration: str) -> int:
        unit_seconds = {
            "seconds": 1,
            "secs": 1,
            "minutes": 60,
            "mins": 60,
            "hours": 60 * 60,
            "days": 86400,
            "weeks": 7 * 86400,
        }
        match = re.fullmatch(r"(\d+) ?(secs|seconds|mins|minutes|hours|days|weeks) ?ago", duration)
        if not match:
            raise ValueError("Invalid duration format")
        quantity = int(match.group(1))
        unit = match.group(2)
        return quantity * unit_seconds[unit]

    @staticmethod
    def get_include_snapshot_times(times) -> UnixTimeRange:
        def utc_unix_time_in_seconds(time_spec: Union[timedelta, int], default: int) -> int:
            if isinstance(time_spec, timedelta):
                return ceil(current_time - time_spec.total_seconds())
            if isinstance(time_spec, int):
                return int(time_spec)
            return default

        lo, hi = times
        if lo is None and hi is None:
            return None
        current_time = time.time()
        lo = utc_unix_time_in_seconds(lo, default=0)
        hi = utc_unix_time_in_seconds(hi, default=unixtime_infinity_secs)
        return (lo, hi) if lo <= hi else (hi, lo)

    @staticmethod
    def parse_rankranges(parser, values, option_string=None) -> List[RankRange]:
        def parse_rank(spec):
            spec = spec.strip()
            match = re.fullmatch(r"(oldest|latest) ?(\d+)%?", spec)
            if not match:
                parser.error(f"{option_string}: Invalid rank format: {spec}")
            kind = match.group(1)
            num = int(match.group(2))
            is_percent = spec.endswith("%")
            if is_percent and num > 100:
                parser.error(f"{option_string}: Invalid rank: Percent must not be greater than 100: {spec}")
            return kind, num, is_percent

        rankranges = []
        for value in values:
            value = value.strip()
            if ".." in value:
                lo, hi = value.split("..", 1)
                lo, hi = [parse_rank(spec) for spec in [lo, hi]]
                if lo[0] != hi[0]:
                    # Example: latest10..oldest10 and oldest10..latest10 may be somewhat unambigous if there are 40
                    # input snapshots, but they are tricky/not well-defined if there are less than 20 input snapshots.
                    parser.error(f"{option_string}: Ambiguous rank range: Must not compare oldest with latest: {value}")
            else:
                hi = parse_rank(value)
                lo = parse_rank(hi[0] + "0")
            rankranges.append((lo, hi))
        return rankranges


#############################################################################
@dataclass(order=True)
class SnapshotFilter:
    name: str
    timerange: UnixTimeRange
    options: Any = field(compare=False, default=None)


def add_snapshot_filter(args: argparse.Namespace, _filter: SnapshotFilter) -> None:
    if not hasattr(args, snapshot_filters_var):
        args.snapshot_filters_var = []
    args.snapshot_filters_var.append(_filter)


def add_time_and_rank_snapshot_filter(args: Namespace, dst: str, timerange: UnixTimeRange, rankranges: List[RankRange]):
    if timerange is None or len(rankranges) == 0 or any(rankrange[0] == rankrange[1] for rankrange in rankranges):
        add_snapshot_filter(args, SnapshotFilter("include_snapshot_times", timerange, None))
    else:
        assert timerange is not None
        add_snapshot_filter(args, SnapshotFilter(dst, timerange, rankranges))


def has_timerange_filter(snapshot_filters: List[SnapshotFilter]) -> bool:
    """Interacts with add_time_and_rank_snapshot_filter() and optimize_snapshot_filters()"""
    return any(f.timerange is not None for f in snapshot_filters)


def optimize_snapshot_filters(snapshot_filters: List[SnapshotFilter]) -> List[SnapshotFilter]:
    """Not intended to be a full query execution plan optimizer, but we still apply some basic plan optimizations."""
    merge_adjacent_snapshot_filters(snapshot_filters)
    merge_adjacent_snapshot_regexes(snapshot_filters)
    snapshot_filters = [f for f in snapshot_filters if f.timerange or f.options]  # drop noop --include-snapshot-times
    reorder_snapshot_time_filters(snapshot_filters)
    return snapshot_filters


def merge_adjacent_snapshot_filters(snapshot_filters: List[SnapshotFilter]) -> None:
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


def merge_adjacent_snapshot_regexes(snapshot_filters: List[SnapshotFilter]) -> None:
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


def reorder_snapshot_time_filters(snapshot_filters: List[SnapshotFilter]) -> None:
    """In an execution plan that contains filter operators based on sort order (the --include-snapshot-times-and-ranks
    operator with non-empty ranks), filters cannot freely be reordered without violating correctness, but they can
    still be partially reordered for better execution performance. The filter list is partitioned into sections such
    that sections are separated by --include-snapshot-times-and-ranks operators with non-empty ranks. Within each
    section, we move include_snapshot_times operators aka --include-snapshot-times-and-ranks operators with empty ranks
    before --include/exclude-snapshot-regex operators because the former involves fast integer comparisons and the
    latter involves more expensive regex matching.
    Example: reorders --include-snapshot-regex .*daily --include-snapshot-times-and-ranks 2024-01-01..2024-04-01 into
    --include-snapshot-times-and-ranks 2024-01-01..2024-04-01 --include-snapshot-regex .*daily"""

    def reorder_time_filters_within_section(i: int, j: int):
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
    def __call__(self, parser, namespace, values, option_string=None):
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
# Copied from https://gist.github.com/dmitriykovalev/2ab1aa33a8099ef2d514925d84aa89e7/30961300d3f8192f775709c06ff9a5b777475adf
# Written by Dmitriy Kovalev
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
# Allows you to validate open, closed, and half-open intervals on int as well as float arguments.
# Each endpoint can be either a number or positive or negative infinity:
# [a, b] --> min=a, max=b
# [a, b) --> min=a, sup=b
# (a, b] --> inf=a, max=b
# (a, b) --> inf=a, sup=b
# [a, +infinity) --> min=a
# (a, +infinity) --> inf=a
# (-infinity, b] --> max=b
# (-infinity, b) --> sup=b
# fmt: off
class CheckRange(argparse.Action):
    ops = {'inf': operator.gt,
           'min': operator.ge,
           'sup': operator.lt,
           'max': operator.le}

    def __init__(self, *args, **kwargs):
        if 'min' in kwargs and 'inf' in kwargs:
            raise ValueError('either min or inf, but not both')
        if 'max' in kwargs and 'sup' in kwargs:
            raise ValueError('either max or sup, but not both')

        for name in self.ops:
            if name in kwargs:
                setattr(self, name, kwargs.pop(name))

        super().__init__(*args, **kwargs)

    def interval(self):
        if hasattr(self, 'min'):
            l = f'[{self.min}'
        elif hasattr(self, 'inf'):
            l = f'({self.inf}'
        else:
            l = '(-infinity'

        if hasattr(self, 'max'):
            u = f'{self.max}]'
        elif hasattr(self, 'sup'):
            u = f'{self.sup})'
        else:
            u = '+infinity)'

        return f'valid range: {l}, {u}'

    def __call__(self, parser, namespace, values, option_string=None):
        for name, op in self.ops.items():
            if hasattr(self, name) and not op(values, getattr(self, name)):
                raise argparse.ArgumentError(self, self.interval())
        setattr(namespace, self.dest, values)
# fmt: on


#############################################################################
class CheckPercentRange(CheckRange):

    def __call__(self, parser, namespace, values, option_string=None):
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
