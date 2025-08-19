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
"""Documentation, definition of input data and ArgumentParser used by the 'bzfs' CLI."""

from __future__ import annotations
import argparse
import dataclasses
import itertools

from bzfs_main.argparse_actions import (
    CheckPercentRange,
    DatasetPairsAction,
    DeleteDstSnapshotsExceptPlanAction,
    FileOrLiteralAction,
    IncludeSnapshotPlanAction,
    LogConfigVariablesAction,
    NewSnapshotFilterGroupAction,
    NonEmptyStringAction,
    SafeDirectoryNameAction,
    SafeFileNameAction,
    SSHConfigFileNameAction,
    TimeRangeAndRankRangeAction,
)
from bzfs_main.check_range import CheckRange
from bzfs_main.detect import (
    DISABLE_PRG,
    DUMMY_DATASET,
)
from bzfs_main.period_anchors import (
    PeriodAnchors,
)
from bzfs_main.utils import (
    ENV_VAR_PREFIX,
    PROG_NAME,
    format_dict,
)

# constants:
__version__: str = "1.12.0.dev0"
PROG_AUTHOR: str = "Wolfgang Hoschek"
EXCLUDE_DATASET_REGEXES_DEFAULT: str = r"(.*/)?[Tt][Ee]?[Mm][Pp][-_]?[0-9]*"  # skip tmp datasets by default
LOG_DIR_DEFAULT: str = PROG_NAME + "-logs"
SKIP_ON_ERROR_DEFAULT: str = "dataset"
CMP_CHOICES_ITEMS: tuple[str, str, str] = ("src", "dst", "all")
ZFS_RECV_O: str = "zfs_recv_o"
ZFS_RECV_X: str = "zfs_recv_x"
ZFS_RECV_GROUPS: dict[str, str] = {ZFS_RECV_O: "-o", ZFS_RECV_X: "-x", "zfs_set": ""}
ZFS_RECV_O_INCLUDE_REGEX_DEFAULT: str = "|".join(
    [
        "aclinherit",
        "aclmode",
        "acltype",
        "atime",
        "checksum",
        "compression",
        "copies",
        "logbias",
        "primarycache",
        "recordsize",
        "redundant_metadata",
        "relatime",
        "secondarycache",
        "snapdir",
        "sync",
        "xattr",
    ]
)


def argument_parser() -> argparse.ArgumentParser:
    """Returns the CLI parser used by bzfs."""
    create_src_snapshots_plan_example1: str = str({"test": {"": {"adhoc": 1}}}).replace(" ", "")
    create_src_snapshots_plan_example2: str = str({"prod": {"us-west-1": {"hourly": 36, "daily": 31}}}).replace(" ", "")
    delete_dst_snapshots_except_plan_example1: str = str(
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
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        prog=PROG_NAME,
        allow_abbrev=False,
        formatter_class=argparse.RawTextHelpFormatter,
        description=f"""
*{PROG_NAME} is a backup command line tool that reliably replicates ZFS snapshots from a (local or remote)
source ZFS dataset (ZFS filesystem or ZFS volume) and its descendant datasets to a (local or remote)
destination ZFS dataset to make the destination dataset a recursively synchronized copy of the source dataset,
using zfs send/receive/rollback/destroy and ssh tunnel as directed. For example, {PROG_NAME} can be used to
incrementally replicate all ZFS snapshots since the most recent common snapshot from source to destination,
in order to help protect against data loss or ransomware.*

When run for the first time, {PROG_NAME} replicates the dataset and all its snapshots from the source to the
destination. On subsequent runs, {PROG_NAME} transfers only the data that has changed since the previous run,
i.e. it incrementally replicates to the destination all intermediate snapshots that have been created on
the source since the last run. Source ZFS snapshots older than the most recent common snapshot found on the
destination are auto-skipped.

Unless {PROG_NAME} is explicitly told to create snapshots on the source, it treats the source as read-only,
thus the source remains unmodified. With the --dryrun flag, {PROG_NAME} also treats the destination as read-only.
In normal operation, {PROG_NAME} treats the destination as append-only. Optional CLI flags are available to
delete destination snapshots and destination datasets as directed, for example to make the destination
identical to the source if the two have somehow diverged in unforeseen ways. This easily enables
(re)synchronizing the backup from the production state, as well as restoring the production state from
backup.

In the spirit of rsync, {PROG_NAME} supports a variety of powerful include/exclude filters that can be combined to
select which datasets, snapshots and properties to create, replicate, delete or compare.

Typically, a `cron` job on the source host runs `{PROG_NAME}` periodically to create new snapshots and prune outdated
snapshots on the source, whereas another `cron` job on the destination host runs `{PROG_NAME}` periodically to prune
outdated destination snapshots. Yet another `cron` job runs `{PROG_NAME}` periodically to replicate the recently created
snapshots from the source to the destination. The frequency of these periodic activities is typically every N milliseconds,
every second, minute, hour, day, week, month and/or year (or multiples thereof).

All {PROG_NAME} functions including snapshot creation, replication, deletion, monitoring, comparison, etc. happily work
with any snapshots in any format, even created or managed by third party ZFS snapshot management tools, including manual
zfs snapshot/destroy. All functions can also be used independently. That is, if you wish you can use {PROG_NAME} just
for creating snapshots, or just for replicating, or just for deleting/pruning, or just for monitoring, or just for
comparing snapshot lists.

The source 'pushes to' the destination whereas the destination 'pulls from' the source. {PROG_NAME} is installed
and executed on the 'initiator' host which can be either the host that contains the source dataset (push mode),
or the destination dataset (pull mode), or both datasets (local mode, no network required, no ssh required),
or any third-party (even non-ZFS OSX) host as long as that host is able to SSH (via standard 'ssh' OpenSSH CLI) into
both the source and destination host (pull-push mode). In pull-push mode the source 'zfs send's the data stream
to the initiator which immediately pipes the stream (without storing anything locally) to the destination
host that 'zfs receive's it. Pull-push mode means that {PROG_NAME} need not be installed or executed on either
source or destination host. Only the underlying 'zfs' CLI must be installed on both source and destination host.
{PROG_NAME} can run as root or non-root user, in the latter case via a) sudo or b) when granted corresponding
ZFS permissions by administrators via 'zfs allow' delegation mechanism.

{PROG_NAME} is written in Python and continuously runs a wide set of unit tests and integration tests to ensure
coverage and compatibility with old and new versions of ZFS on Linux, FreeBSD and Solaris, on all Python
versions ≥ 3.8 (including latest stable which is currently python-3.13).

{PROG_NAME} is a stand-alone program with zero required dependencies, akin to a
stand-alone shell script or binary executable. It is designed to be able to run in restricted barebones server
environments. No external Python packages are required; indeed no Python package management at all is required.
You can just symlink the program wherever you like, for example into /usr/local/bin or similar, and simply run it like
any stand-alone shell script or binary executable.

{PROG_NAME} automatically replicates the snapshots of multiple datasets in parallel for best performance.
Similarly, it quickly deletes (or monitors or compares) snapshots of multiple datasets in parallel. Atomic snapshots can be
created as frequently as every N milliseconds.

Optionally, {PROG_NAME} applies bandwidth rate-limiting and progress monitoring (via 'pv' CLI) during 'zfs
send/receive' data transfers. When run across the network, {PROG_NAME} also transparently inserts lightweight
data compression (via 'zstd -1' CLI) and efficient data buffering (via 'mbuffer' CLI) into the pipeline
between network endpoints during 'zfs send/receive' network transfers. If one of these utilities is not
installed this is auto-detected, and the operation continues reliably without the corresponding auxiliary
feature.

# Periodic Jobs with bzfs_jobrunner

The software also ships with the [bzfs_jobrunner](README_bzfs_jobrunner.md) companion program, which is a convenience
wrapper around `{PROG_NAME}` that simplifies efficient periodic ZFS snapshot creation, replication, pruning, and monitoring,
across a fleet of N source hosts and M destination hosts, using a single shared fleet-wide
[jobconfig](bzfs_tests/bzfs_job_example.py) script. For example, this simplifies the deployment of an efficient
geo-replicated backup service where each of the M destination hosts is located in a separate geographic region and pulls
replicas from (the same set of) N source hosts. It also simplifies low latency replication from a primary to a secondary or
to M read replicas, or backup to removable drives, etc.

# Quickstart

* Create adhoc atomic snapshots without a schedule:

```$ {PROG_NAME} tank1/foo/bar dummy --recursive --skip-replication --create-src-snapshots
--create-src-snapshots-plan "{create_src_snapshots_plan_example1}"```

```$ zfs list -t snapshot tank1/foo/bar

tank1/foo/bar@test_2024-11-06_08:30:05_adhoc```

* Create periodic atomic snapshots on a schedule, every hour and every day, by launching this from a periodic `cron` job:

```$ {PROG_NAME} tank1/foo/bar dummy --recursive --skip-replication --create-src-snapshots
--create-src-snapshots-plan "{create_src_snapshots_plan_example2}"```

```$ zfs list -t snapshot tank1/foo/bar

tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_daily

tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_hourly```

Note: A periodic snapshot is created if it is due per the schedule indicated by its suffix (e.g. `_daily` or `_hourly`
or `_minutely` or `_2secondly` or `_100millisecondly`), or if the --create-src-snapshots-even-if-not-due flag is specified,
or if the most recent scheduled snapshot is somehow missing. In the latter case {PROG_NAME} immediately creates a snapshot
(named with the current time, not backdated to the missed time), and then resumes the original schedule. If the suffix is
`_adhoc` or not a known period then a snapshot is considered non-periodic and is thus created immediately regardless of the
creation time of any existing snapshot.

* Replication example in local mode (no network, no ssh), to replicate ZFS dataset tank1/foo/bar to tank2/boo/bar:

```$ {PROG_NAME} tank1/foo/bar tank2/boo/bar```

```$ zfs list -t snapshot tank1/foo/bar

tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_daily

tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_hourly```

```$ zfs list -t snapshot tank2/boo/bar

tank2/boo/bar@prod_us-west-1_2024-11-06_08:30:05_daily

tank2/boo/bar@prod_us-west-1_2024-11-06_08:30:05_hourly```

* Same example in pull mode:

```$ {PROG_NAME} root@host1.example.com:tank1/foo/bar tank2/boo/bar```

* Same example in push mode:

```$ {PROG_NAME} tank1/foo/bar root@host2.example.com:tank2/boo/bar```

* Same example in pull-push mode:

```$ {PROG_NAME} root@host1:tank1/foo/bar root@host2:tank2/boo/bar```

* Example in local mode (no network, no ssh) to recursively replicate ZFS dataset tank1/foo/bar and its descendant
datasets to tank2/boo/bar:

```$ {PROG_NAME} tank1/foo/bar tank2/boo/bar --recursive```

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

```$ {PROG_NAME} tank1/foo/bar tank2/boo/bar --recursive --force --delete-dst-datasets --delete-dst-snapshots```

* Replicate all daily snapshots created during the last 7 days, and at the same time ensure that the latest 7 daily
snapshots (per dataset) are replicated regardless of creation time:

```$ {PROG_NAME} tank1/foo/bar tank2/boo/bar --recursive --include-snapshot-regex '.*_daily'
--include-snapshot-times-and-ranks '7 days ago..anytime' 'latest 7'```

Note: The example above compares the specified times against the standard ZFS 'creation' time property of the snapshots
(which is a UTC Unix time in integer seconds), rather than against a timestamp that may be part of the snapshot name.

* Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per dataset) are retained
regardless of creation time:

```$ {PROG_NAME} {DUMMY_DATASET} tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots
--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks notime 'all except latest 7'
--include-snapshot-times-and-ranks 'anytime..7 days ago'```

Note: This also prints how many GB of disk space in total would be freed if the command were to be run for real without
the --dryrun flag.

* Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per dataset) are retained
regardless of creation time. Additionally, only delete a snapshot if no corresponding snapshot or bookmark exists in
the source dataset (same as above except replace the 'dummy' source with 'tank1/foo/bar'):

```$ {PROG_NAME} tank1/foo/bar tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots
--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks notime 'all except latest 7'
--include-snapshot-times-and-ranks '7 days ago..anytime'```

* Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per dataset) are retained
regardless of creation time. Additionally, only delete a snapshot if no corresponding snapshot exists in the source
dataset (same as above except append 'no-crosscheck'):

```$ {PROG_NAME} tank1/foo/bar tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots
--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks notime 'all except latest 7'
--include-snapshot-times-and-ranks 'anytime..7 days ago' --delete-dst-snapshots-no-crosscheck```

* Delete all daily bookmarks older than 90 days, but retain the latest 200 daily bookmarks (per dataset) regardless
of creation time:

```$ {PROG_NAME} {DUMMY_DATASET} tank1/foo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots=bookmarks
--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks notime 'all except latest 200'
--include-snapshot-times-and-ranks 'anytime..90 days ago'```

* Delete all tmp datasets within tank2/boo/bar:

```$ {PROG_NAME} {DUMMY_DATASET} tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-datasets
--include-dataset-regex '(.*/)?tmp.*' --exclude-dataset-regex '!.*'```

* Retain all secondly snapshots that were created less than 40 seconds ago, and ensure that the latest 40
secondly snapshots (per dataset) are retained regardless of creation time. Same for 40 minutely snapshots, 36 hourly
snapshots, 31 daily snapshots, 12 weekly snapshots, 18 monthly snapshots, and 5 yearly snapshots:

```$ {PROG_NAME} {DUMMY_DATASET} tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots
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

```$ {PROG_NAME} {DUMMY_DATASET} tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots
--delete-dst-snapshots-except-plan "{delete_dst_snapshots_except_plan_example1}"```

* Compare source and destination dataset trees recursively, for example to check if all recently taken snapshots have
been successfully replicated by a periodic job. List snapshots only contained in src (tagged with 'src'),
only contained in dst (tagged with 'dst'), and contained in both src and dst (tagged with 'all'), restricted to hourly
and daily snapshots taken within the last 7 days, excluding the last 4 hours (to allow for some slack/stragglers),
excluding temporary datasets:

```$ {PROG_NAME} tank1/foo/bar tank2/boo/bar --skip-replication --compare-snapshot-lists=src+dst+all --recursive
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

```$ {PROG_NAME} tank1/foo/bar root@host2.example.com:tank2/boo/bar --recursive
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
             f"*Performance Note:* {PROG_NAME} automatically replicates multiple datasets in parallel. It replicates "
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
             f"`{EXCLUDE_DATASET_REGEXES_DEFAULT}` (exclude tmp datasets). Example: `!.*` (exclude no dataset)\n\n")
    parser.add_argument(
        "--exclude-dataset-property", default=None, action=NonEmptyStringAction, metavar="STRING",
        help="The name of a ZFS dataset user property (optional). If this option is specified, the effective value "
             "(potentially inherited) of that user property is read via 'zfs list' for each selected source dataset "
             "to determine whether the dataset will be included or excluded, as follows:\n\n"
             "a) Value is 'true' or '-' or empty string or the property is missing: Include the dataset.\n\n"
             "b) Value is 'false': Exclude the dataset and its descendants.\n\n"
             "c) Value is a comma-separated list of host names (no spaces, for example: "
             "'store001,store002'): Include the dataset if the host name of "
             f"the host executing {PROG_NAME} is contained in the list, otherwise exclude the dataset and its "
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
             "requires Python ≥ 3.11.\n\n"
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
             f"`{PROG_NAME} {DUMMY_DATASET} tank2/boo/bar --dryrun --recursive --skip-replication --delete-dst-snapshots "
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
             f"is somehow missing. In the latter case {PROG_NAME} immediately creates a snapshot (tagged with the current "
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
             f"Note: All {PROG_NAME} functions including snapshot creation, replication, deletion, monitoring, comparison, "
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
        return text.replace("%", "%%")

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
        help=f"Default is the local timezone of the system running {PROG_NAME}. When creating a new snapshot on the source, "
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
        "--zfs-send-program-opts", type=str, default="--raw --compressed", metavar="STRING",
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
             "Note: --preserve-properties uses the 'zfs recv -x' option and thus requires either OpenZFS ≥ 2.2.0 "
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
             f"Analogy: `{PROG_NAME} --recursive --skip-parent src dst` is akin to Unix `cp -r src/* dst/` whereas "
             f" `{PROG_NAME} --recursive --skip-parent --skip-replication --delete-dst-datasets dummy dst` is akin to "
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
             f"snapshot or bookmark while {PROG_NAME} is running and attempting to access it.\n\n")
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
        "--skip-on-error", choices=["fail", "tree", "dataset"], default=SKIP_ON_ERROR_DEFAULT,
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
             f"dataset named '{DUMMY_DATASET}'!). Do not recurse without --recursive. With --recursive, never delete "
             "non-selected dataset subtrees or their ancestors.\n\n"
             "For example, if the destination contains datasets h1,h2,h3,d1 whereas source only contains h3, "
             "and the include/exclude policy selects h1,h2,h3,d1, then delete datasets h1,h2,d1 on "
             "the destination to make it 'the same'. On the other hand, if the include/exclude policy "
             "only selects h1,h2,h3 then only delete datasets h1,h2 on the destination to make it 'the same'.\n\n"
             "Example to delete all tmp datasets within tank2/boo/bar: "
             f"`{PROG_NAME} {DUMMY_DATASET} tank2/boo/bar --dryrun --skip-replication --recursive "
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
             f"source that is an empty dataset, such as the hardcoded virtual dataset named '{DUMMY_DATASET}', like so:"
             f" `{PROG_NAME} {DUMMY_DATASET} tank2/boo/bar --dryrun --skip-replication --delete-dst-snapshots "
             "--include-snapshot-regex '.*_daily' --recursive`\n\n"
             "*Note:* Use --delete-dst-snapshots=bookmarks to delete bookmarks instead of snapshots, in which "
             "case no snapshots are selected and the --{include|exclude}-snapshot-* filter options treat bookmarks as "
             "snapshots wrt. selecting.\n\n"
             "*Performance Note:* --delete-dst-snapshots operates on multiple datasets in parallel (and serially "
             f"within a dataset), using the same dataset order as {PROG_NAME} replication. "
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
    cmp_choices_dflt: str = "+".join(CMP_CHOICES_ITEMS)
    cmp_choices: list[str] = []
    for i in range(len(CMP_CHOICES_ITEMS)):
        cmp_choices += ["+".join(c) for c in itertools.combinations(CMP_CHOICES_ITEMS, i + 1)]
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
             f"`{PROG_NAME} tank1/foo/bar tank2/boo/bar --skip-replication "
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
             f"'{DUMMY_DATASET}'.\n\n"
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
             "*Note:* This flag only has an effect on OpenZFS ≥ 2.2.\n\n"
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
             "sudo zfs allow -u $DST_NON_ROOT_USER_NAME mount,create,receive,rollback,destroy $DST_DATASET_OR_POOL.\n\n"
             "If you do not plan to use the --force* flags and --delete-* CLI options then ZFS permissions "
             "'rollback,destroy' can be omitted, arriving at the absolutely minimal set of required destination "
             "permissions: `mount,create,receive`.\n\n"
             "For extra security $SRC_NON_ROOT_USER_NAME should be different than $DST_NON_ROOT_USER_NAME, i.e. the "
             "sending Unix user on the source and the receiving Unix user at the destination should be separate Unix "
             "user accounts with separate private keys even if both accounts reside on the same machine, per the "
             "principle of least privilege.\n\n"
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
             f"and --retry-* options enable it (see above). In normal operation {PROG_NAME} automatically retries "
             "such that only the portion of the snapshot is transmitted that has not yet been fully received on the "
             "destination. For example, this helps to progressively transfer a large individual snapshot over a "
             "wireless network in a timely manner despite frequent intermittent network hiccups. This optimization is "
             "called 'resume receive' and uses the 'zfs receive -s' and 'zfs send -t' feature.\n\n"
             "The --no-resume-recv option disables this optimization such that a retry now retransmits the entire "
             "snapshot from scratch, which could slow down or even prohibit progress in case of frequent network "
             f"hiccups. {PROG_NAME} automatically falls back to using the --no-resume-recv option if it is "
             "auto-detected that the ZFS pool does not reliably support the 'resume receive' optimization.\n\n"
             "*Note:* Snapshots that have already been fully transferred as part of the current 'zfs send/receive' "
             "operation need not be retransmitted regardless of the --no-resume-recv flag. For example, assume "
             "a single 'zfs send/receive' operation is transferring incremental snapshots 1 through 10 via "
             "'zfs send -I', but the operation fails while transferring snapshot 10, then snapshots 1 through 9 "
             "need not be retransmitted regardless of the --no-resume-recv flag, as these snapshots have already "
             "been successfully received at the destination either way.\n\n")
    parser.add_argument(
        "--create-bookmarks", choices=["all", "many", "none"], default="many",
        help=f"For increased safety, {PROG_NAME} replication behaves as follows wrt. ZFS bookmark creation, if it is "
             "autodetected that the source ZFS pool support bookmarks:\n\n"
             "* `many` (default): Whenever it has successfully completed replication of the most recent source snapshot, "
             f"{PROG_NAME} creates a ZFS bookmark of that snapshot, and attaches it to the source dataset. In addition, "
             f"whenever it has successfully completed a 'zfs send' operation, {PROG_NAME} creates a ZFS bookmark of each "
             f"hourly, daily, weekly, monthly and yearly source snapshot that was sent during that 'zfs send' operation, "
             "and attaches it to the source dataset.\n\n"
             "* `all`: Whenever it has successfully completed a 'zfs send' operation, "
             f"{PROG_NAME} creates a ZFS bookmark of each source snapshot that was sent during that 'zfs send' operation, "
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
             f"By convention, a bookmark created by {PROG_NAME} has the same name as its corresponding "
             "snapshot, the only difference being the leading '#' separator instead of the leading '@' separator. "
             "Also see https://www.youtube.com/watch?v=LaNgoAZeTww&t=316s.\n\n"
             "You can list bookmarks, like so: "
             "`zfs list -t bookmark -o name,guid,createtxg,creation -d 1 $SRC_DATASET`, and you can (and should) "
             "periodically prune obsolete bookmarks just like snapshots, like so: "
             "`zfs destroy $SRC_DATASET#$BOOKMARK`. Typically, bookmarks should be pruned less aggressively "
             "than snapshots, and destination snapshots should be pruned less aggressively than source snapshots. "
             "As an example starting point, here is a command that deletes all bookmarks older than "
             "90 days, but retains the latest 200 bookmarks (per dataset) regardless of creation time: "
             f"`{PROG_NAME} {DUMMY_DATASET} tank2/boo/bar --dryrun --recursive --skip-replication "
             "--delete-dst-snapshots=bookmarks --include-snapshot-times-and-ranks notime 'all except latest 200' "
             "--include-snapshot-times-and-ranks 'anytime..90 days ago'`\n\n")
    parser.add_argument(
        "--no-create-bookmark", action="store_true",
        help=argparse.SUPPRESS)  # deprecated; was replaced by --create-bookmarks=none
    parser.add_argument(
        "--no-use-bookmark", action="store_true",
        help=f"For increased safety, in normal replication operation {PROG_NAME} replication also looks for bookmarks "
             "(in addition to snapshots) on the source dataset in order to find the most recent common snapshot wrt. the "
             "destination dataset, if it is auto-detected that the source ZFS pool support bookmarks. "
             "The --no-use-bookmark option disables this safety feature but is discouraged, because bookmarks help "
             "to ensure that ZFS replication can continue even if source and destination dataset somehow have no "
             "common snapshot anymore.\n\n"
             f"Note that it does not matter whether a bookmark was created by {PROG_NAME} or a third party script, "
             "as only the GUID of the bookmark and the GUID of the snapshot is considered for comparison, and ZFS "
             "guarantees that any bookmark of a given snapshot automatically has the same GUID, transaction group "
             "number and creation time as the snapshot. Also note that you can create, delete and prune bookmarks "
             f"any way you like, as {PROG_NAME} (without --no-use-bookmark) will happily work with whatever "
             "bookmarks currently exist, if any.\n\n")

    # ^aes256-gcm@openssh.com cipher: for speed with confidentiality and integrity
    # measure cipher perf like so: count=5000; for i in $(seq 1 3); do echo "iteration $i:"; for cipher in $(ssh -Q cipher); do dd if=/dev/zero bs=1M count=$count 2> /dev/null | ssh -c $cipher -p 40999 127.0.0.1 "(time -p cat) > /dev/null" 2>&1 | grep real | awk -v count=$count -v cipher=$cipher '{print cipher ": " count / $2 " MB/s"}'; done; done
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
            f"--ssh-{loc}-port", type=int, min=1, max=65535, action=CheckRange, metavar="INT",
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
             "high degree of parallelism, and as such may perform poorly on HDDs. Examples: 1, 4, 75%%, 150%%\n\n")
    parser.add_argument(
        "--max-concurrent-ssh-sessions-per-tcp-connection", type=int, min=1, default=8, action=CheckRange, metavar="INT",
        help=f"For best throughput, {PROG_NAME} uses multiple SSH TCP connections in parallel, as indicated by "
             "--threads (see above). For best startup latency, each such parallel TCP connection can carry a "
             "maximum of S concurrent SSH sessions, where "
             "S=--max-concurrent-ssh-sessions-per-tcp-connection (default: %(default)s, min: %(min)s). "
             "Concurrent SSH sessions are mostly used for metadata operations such as listing ZFS datasets and their "
             "snapshots. This client-side max sessions parameter must not be higher than the server-side "
             "sshd_config(5) MaxSessions parameter (which defaults to 10, see "
             "https://manpages.ubuntu.com/manpages/man5/sshd_config.5.html).\n\n"
             f"*Note:* For better throughput, {PROG_NAME} uses one dedicated TCP connection per ZFS "
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

    msg: str = f"Use '{DISABLE_PRG}' to disable the use of this program.\n\n"
    parser.add_argument(
        "--compression-program", default="zstd", choices=["zstd", "lz4", "pzstd", "pigz", "gzip", "bzip2", DISABLE_PRG],
        help=hlp("zstd") + msg.rstrip() + " The use is auto-disabled if data is transferred locally instead of via the "
                                          "network. This option is about transparent compression-on-the-wire, not about "
                                          "compression-at-rest.\n\n")
    parser.add_argument(
        "--compression-program-opts", default="-1", metavar="STRING",
        help="The options to be passed to the compression program on the compression step (optional). "
             "Default is '%(default)s' (fastest).\n\n")
    parser.add_argument(
        "--mbuffer-program", default="mbuffer", choices=["mbuffer", DISABLE_PRG],
        help=hlp("mbuffer") + msg.rstrip() + " The use is auto-disabled if data is transferred locally "
                                             "instead of via the network. This tool is used to smooth out the rate "
                                             "of data flow and prevent bottlenecks caused by network latency or "
                                             "speed fluctuation.\n\n")
    parser.add_argument(
        "--mbuffer-program-opts", default="-q -m 128M", metavar="STRING",
        help="Options to be passed to 'mbuffer' program (optional). Default: '%(default)s'.\n\n")
    parser.add_argument(
        "--ps-program", default="ps", choices=["ps", DISABLE_PRG],
        help=hlp("ps") + msg)
    parser.add_argument(
        "--pv-program", default="pv", choices=["pv", DISABLE_PRG],
        help=hlp("pv") + msg.rstrip() + " This is used for bandwidth rate-limiting and progress monitoring.\n\n")
    parser.add_argument(
        "--pv-program-opts", metavar="STRING",
        default="--progress --timer --eta --fineta --rate --average-rate --bytes --interval=1 --width=120 --buffer-size=2M",
        help="The options to be passed to the 'pv' program (optional). Default: '%(default)s'.\n\n")
    parser.add_argument(
        "--shell-program", default="sh", choices=["sh", DISABLE_PRG],
        help=hlp("sh") + msg)
    parser.add_argument(
        "--ssh-program", default="ssh", choices=["ssh", "hpnssh", DISABLE_PRG],
        help=hlp("ssh") + msg)
    parser.add_argument(
        "--sudo-program", default="sudo", choices=["sudo", DISABLE_PRG],
        help=hlp("sudo") + msg)
    parser.add_argument(
        "--zpool-program", default="zpool", choices=["zpool", DISABLE_PRG],
        help=hlp("zpool") + msg)
    parser.add_argument(
        "--log-dir", type=str, action=SafeDirectoryNameAction, metavar="DIR",
        help=f"Path to the log output directory on local host (optional). Default: $HOME/{LOG_DIR_DEFAULT}. The logger "
             "that is used by default writes log files there, in addition to the console. The basename of --log-dir must "
             f"contain the substring '{LOG_DIR_DEFAULT}' as this helps prevent accidents. The current.dir symlink "
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
        "--log-syslog-prefix", default=PROG_NAME, action=NonEmptyStringAction, metavar="STRING",
        help=f"The name to prepend to each message that is sent to syslog; identifies {PROG_NAME} messages as opposed "
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
             f"{PROG_NAME} automatically supplies the following convenience variables: "
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
             f"--include-envvar-regex {ENV_VAR_PREFIX}min_pipe_transfer_size`. "
             "Example that retains all environment variables without tightened security: `'.*'`\n\n")
    parser.add_argument(
        "--exclude-envvar-regex", action=FileOrLiteralAction, nargs="+", default=[], metavar="REGEX",
        help="Same syntax as --include-envvar-regex (see above) except that the default is to exclude no "
             f"environment variables. Example: `{ENV_VAR_PREFIX}.*`\n\n")

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

    for option_name, flag in ZFS_RECV_GROUPS.items():
        grup: str = option_name.replace("_", "-")  # one of zfs_recv_o, zfs_recv_x
        flag = "'" + flag + "'"  # one of -o or -x

        def h(text: str, option_name: str=option_name) -> str:
            return argparse.SUPPRESS if option_name not in (ZFS_RECV_O, ZFS_RECV_X) else text

        argument_group = parser.add_argument_group(
            grup + " (Experimental)",
            description=h(f"The following group of parameters specifies additional zfs receive {flag} options that "
                          "can be used to configure copying of ZFS dataset properties from the source dataset to "
                          "its corresponding destination dataset. The 'zfs-recv-o' group of parameters is applied "
                          "before the 'zfs-recv-x' group."))
        target_choices = ["full", "incremental", "full+incremental"]
        target_choices_default = "full+incremental" if option_name == ZFS_RECV_X else "full"
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
        if option_name == ZFS_RECV_O:
            group_include_regex_default_help: str = f"The default regex is '{ZFS_RECV_O_INCLUDE_REGEX_DEFAULT}'."
        else:
            group_include_regex_default_help = ("The default is to include no properties, thus by default no extra "
                                                f"{flag} option is appended. ")
        argument_group.add_argument(
            f"--{grup}-include-regex", action=FileOrLiteralAction, default=None, const=[], nargs="*", metavar="REGEX",
            help=h(f"Take the output properties of --{grup}-sources (see above) and filter them such that we only "
                   "retain the properties whose name matches at least one of the --include regexes but none of the "
                   "--exclude regexes. If a property is excluded this decision is never reconsidered because exclude "
                   f"takes precedence over include. Append each retained property to the list of {flag} options in "
                   "--zfs-recv-program-opt(s), unless another '-o' or '-x' option with the same name already exists "
                   "therein. In other words, --zfs-recv-program-opt(s) takes precedence.\n\n"
                   f"Zero or more regexes can be specified. Specify zero regexes to append no extra {flag} option. "
                   "A leading `!` character indicates logical negation, i.e. the regex matches if the regex with the "
                   "leading `!` character removed does not match. "
                   "If the option starts with a `+` prefix then regexes are read from the newline-separated "
                   "UTF-8 text file given after the `+` prefix, one regex per line inside of the text file.\n\n"
                   f"{group_include_regex_default_help} "
                   f"Example: `--{grup}-include-regex compression recordsize`. "
                   "More examples: `.*` (include all properties), `foo bar myapp:.*` (include three regexes) "
                   f"`+{grup}_regexes.txt`, `+/path/to/{grup}_regexes.txt`\n\n"
                   "See https://openzfs.github.io/openzfs-docs/man/master/7/zfsprops.7.html\n\n"))
        argument_group.add_argument(
            f"--{grup}-exclude-regex", action=FileOrLiteralAction, nargs="+", default=[], metavar="REGEX",
            help=h(f"Same syntax as --{grup}-include-regex (see above), and the default is to exclude no properties. "
                   f"Example: --{grup}-exclude-regex encryptionroot keystatus origin volblocksize volsize\n\n"))
    parser.add_argument(
        "--version", action="version", version=f"{PROG_NAME}-{__version__}, by {PROG_AUTHOR}",
        help="Display version information and exit.\n\n")
    parser.add_argument(
        "--help, -h", action="help",
        help="Show this help message and exit.\n\n")
    return parser
    # fmt: on
