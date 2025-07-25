<!--
 Copyright 2024 Wolfgang Hoschek AT mac DOT com

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.-->
# bzfs
[![Build](https://github.com/whoschek/bzfs/actions/workflows/python-app.yml/badge.svg)](https://github.com/whoschek/bzfs/actions/workflows/python-app.yml)
[![Coverage](https://whoschek.github.io/bzfs/badges/coverage-badge.svg)](https://whoschek.github.io/bzfs/coverage/)
[![os](https://whoschek.github.io/bzfs/badges/os-badge.svg)](https://github.com/whoschek/bzfs/blob/main/.github/workflows/python-app.yml)
[![python](https://whoschek.github.io/bzfs/badges/python-badge.svg)](https://github.com/whoschek/bzfs/blob/main/.github/workflows/python-app.yml)
[![pypi](https://img.shields.io/pypi/v/bzfs.svg)](https://pypi.org/project/bzfs)
[![zfs](https://whoschek.github.io/bzfs/badges/zfs-badge.svg)](https://github.com/whoschek/bzfs/blob/main/.github/workflows/python-app.yml)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/whoschek/bzfs)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
- [Introduction](#Introduction)
- [Periodic Jobs with bzfs_jobrunner](#Periodic-Jobs-with-bzfs_jobrunner)
- [Quickstart](#Quickstart)
- [Installation](#Installation)
- [Design Aspects](#Design-Aspects)
- [Continuous Testing](#Continuous-Testing)
- [Unit Testing on GitHub](#Unit-Testing-on-GitHub)
- [Unit Testing Locally](#Unit-Testing-Locally)
- [Man Page](#Man-Page)
# Introduction
<!-- DO NOT EDIT (This section was auto-generated from ArgumentParser help text as the source of "truth", via update_readme.sh) -->
<!-- BEGIN DESCRIPTION SECTION -->
*bzfs is a backup command line tool that reliably replicates ZFS snapshots from a (local or
remote) source ZFS dataset (ZFS filesystem or ZFS volume) and its descendant datasets to a (local
or remote) destination ZFS dataset to make the destination dataset a recursively synchronized copy
of the source dataset, using zfs send/receive/rollback/destroy and ssh tunnel as directed. For
example, bzfs can be used to incrementally replicate all ZFS snapshots since the most recent
common snapshot from source to destination, in order to help protect against data loss or
ransomware.*

When run for the first time, bzfs replicates the dataset and all its snapshots from the source to
the destination. On subsequent runs, bzfs transfers only the data that has changed since the
previous run, i.e. it incrementally replicates to the destination all intermediate snapshots that
have been created on the source since the last run. Source ZFS snapshots older than the most
recent common snapshot found on the destination are auto-skipped.

Unless bzfs is explicitly told to create snapshots on the source, it treats the source as
read-only, thus the source remains unmodified. With the --dryrun flag, bzfs also treats the
destination as read-only. In normal operation, bzfs treats the destination as append-only.
Optional CLI flags are available to delete destination snapshots and destination datasets as
directed, for example to make the destination identical to the source if the two have somehow
diverged in unforeseen ways. This easily enables (re)synchronizing the backup from the production
state, as well as restoring the production state from backup.

In the spirit of rsync, bzfs supports a variety of powerful include/exclude filters that can be
combined to select which datasets, snapshots and properties to create, replicate, delete or
compare.

Typically, a `cron` job on the source host runs `bzfs` periodically to create new snapshots
and prune outdated snapshots on the source, whereas another `cron` job on the destination host
runs `bzfs` periodically to prune outdated destination snapshots. Yet another `cron` job runs
`bzfs` periodically to replicate the recently created snapshots from the source to the
destination. The frequency of these periodic activities is typically every N milliseconds, every
second, minute, hour, day, week, month and/or year (or multiples thereof).

All bzfs functions including snapshot creation, replication, deletion, monitoring, comparison,
etc. happily work with any snapshots in any format, even created or managed by third party ZFS
snapshot management tools, including manual zfs snapshot/destroy. All functions can also be used
independently. That is, if you wish you can use bzfs just for creating snapshots, or just for
replicating, or just for deleting/pruning, or just for monitoring, or just for comparing snapshot
lists.

The source 'pushes to' the destination whereas the destination 'pulls from' the source. bzfs
is installed and executed on the 'initiator' host which can be either the host that contains the
source dataset (push mode), or the destination dataset (pull mode), or both datasets (local mode,
no network required, no ssh required), or any third-party (even non-ZFS OSX) host as long as that
host is able to SSH (via standard 'ssh' OpenSSH CLI) into both the source and destination host
(pull-push mode). In pull-push mode the source 'zfs send's the data stream to the initiator
which immediately pipes the stream (without storing anything locally) to the destination host that
'zfs receive's it. Pull-push mode means that bzfs need not be installed or executed on either
source or destination host. Only the underlying 'zfs' CLI must be installed on both source and
destination host. bzfs can run as root or non-root user, in the latter case via a) sudo or b) when
granted corresponding ZFS permissions by administrators via 'zfs allow' delegation mechanism.

bzfs is written in Python and continously runs a wide set of unit tests and integration tests to
ensure coverage and compatibility with old and new versions of ZFS on Linux, FreeBSD and Solaris,
on all Python versions >= 3.8 (including latest stable which is currently python-3.13).

bzfs is a stand-alone program with zero required dependencies, akin to a stand-alone shell script
or binary executable. It is designed to be able to run in restricted barebones server
environments. No external Python packages are required; indeed no Python package management at all
is required. You can just symlink the program wherever you like, for example into /usr/local/bin
or similar, and simply run it like any stand-alone shell script or binary executable.

bzfs automatically replicates the snapshots of multiple datasets in parallel for best performance.
Similarly, it quickly deletes (or monitors or compares) snapshots of multiple datasets in
parallel. Atomic snapshots can be created as frequently as every N milliseconds.

Optionally, bzfs applies bandwidth rate-limiting and progress monitoring (via 'pv' CLI) during
'zfs send/receive' data transfers. When run across the network, bzfs also transparently inserts
lightweight data compression (via 'zstd -1' CLI) and efficient data buffering (via 'mbuffer'
CLI) into the pipeline between network endpoints during 'zfs send/receive' network transfers. If
one of these utilities is not installed this is auto-detected, and the operation continues
reliably without the corresponding auxiliary feature.

# Periodic Jobs with bzfs_jobrunner

The software also ships with the [bzfs_jobrunner](README_bzfs_jobrunner.md) companion program,
which is a convenience wrapper around `bzfs` that simplifies efficient periodic ZFS snapshot
creation, replication, pruning, and monitoring, across N source hosts and M destination hosts,
using a single shared [jobconfig](bzfs_tests/bzfs_job_example.py) script. For example, this
simplifies the deployment of an efficient geo-replicated backup service where each of the M
destination hosts is located in a separate geographic region and pulls replicas from (the same set
of) N source hosts. It also simplifies low latency replication from a primary to a secondary or to
M read replicas, or backup to removable drives, etc.

# Quickstart

* Create adhoc atomic snapshots without a schedule:


```
$ bzfs tank1/foo/bar dummy --recursive --skip-replication --create-src-snapshots \
--create-src-snapshots-plan "{'test':{'':{'adhoc':1}}}"
```



```
$ zfs list -t snapshot tank1/foo/bar
tank1/foo/bar@test_2024-11-06_08:30:05_adhoc
```


* Create periodic atomic snapshots on a schedule, every hour and every day, by launching this
from a periodic `cron` job:


```
$ bzfs tank1/foo/bar dummy --recursive --skip-replication --create-src-snapshots \
--create-src-snapshots-plan "{'prod':{'us-west-1':{'hourly':36,'daily':31}}}"
```



```
$ zfs list -t snapshot tank1/foo/bar
tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_daily
tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_hourly
```


Note: A periodic snapshot is created if it is due per the schedule indicated by its suffix (e.g.
`_daily` or `_hourly` or `_minutely` or `_2secondly` or `_100millisecondly`), or if
the --create-src-snapshots-even-if-not-due flag is specified, or if the most recent scheduled
snapshot is somehow missing. In the latter case bzfs immediately creates a snapshot (named with
the current time, not backdated to the missed time), and then resumes the original schedule. If
the suffix is `_adhoc` or not a known period then a snapshot is considered non-periodic and is
thus created immediately regardless of the creation time of any existing snapshot.

* Replication example in local mode (no network, no ssh), to replicate ZFS dataset tank1/foo/bar
to tank2/boo/bar:


```
$ bzfs tank1/foo/bar tank2/boo/bar
```



```
$ zfs list -t snapshot tank1/foo/bar
tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_daily
tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_hourly
```



```
$ zfs list -t snapshot tank2/boo/bar
tank2/boo/bar@prod_us-west-1_2024-11-06_08:30:05_daily
tank2/boo/bar@prod_us-west-1_2024-11-06_08:30:05_hourly
```


* Same example in pull mode:


```
$ bzfs root@host1.example.com:tank1/foo/bar tank2/boo/bar
```


* Same example in push mode:


```
$ bzfs tank1/foo/bar root@host2.example.com:tank2/boo/bar
```


* Same example in pull-push mode:


```
$ bzfs root@host1:tank1/foo/bar root@host2:tank2/boo/bar
```


* Example in local mode (no network, no ssh) to recursively replicate ZFS dataset tank1/foo/bar
and its descendant datasets to tank2/boo/bar:


```
$ bzfs tank1/foo/bar tank2/boo/bar --recursive
```



```
$ zfs list -t snapshot -r tank1/foo/bar
tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_daily
tank1/foo/bar@prod_us-west-1_2024-11-06_08:30:05_hourly
tank1/foo/bar/baz@prod_us-west-1_2024-11-06_08:40:00_daily
tank1/foo/bar/baz@prod_us-west-1_2024-11-06_08:40:00_hourly
```



```
$ zfs list -t snapshot -r tank2/boo/bar
tank2/boo/bar@prod_us-west-1_2024-11-06_08:30:05_daily
tank2/boo/bar@prod_us-west-1_2024-11-06_08:30:05_hourly
tank2/boo/bar/baz@prod_us-west-1_2024-11-06_08:40:00_daily
tank2/boo/bar/baz@prod_us-west-1_2024-11-06_08:40:00_hourly
```


* Example that makes destination identical to source even if the two have drastically diverged:


```
$ bzfs tank1/foo/bar tank2/boo/bar --recursive --force --delete-dst-datasets \
--delete-dst-snapshots
```


* Replicate all daily snapshots created during the last 7 days, and at the same time ensure that
the latest 7 daily snapshots (per dataset) are replicated regardless of creation time:


```
$ bzfs tank1/foo/bar tank2/boo/bar --recursive --include-snapshot-regex '.*_daily' \
--include-snapshot-times-and-ranks '7 days ago..anytime' 'latest 7'
```


Note: The example above compares the specified times against the standard ZFS 'creation' time
property of the snapshots (which is a UTC Unix time in integer seconds), rather than against a
timestamp that may be part of the snapshot name.

* Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per
dataset) are retained regardless of creation time:


```
$ bzfs dummy tank2/boo/bar --dryrun --recursive --skip-replication \
--delete-dst-snapshots --include-snapshot-regex '.*_daily' \
--include-snapshot-times-and-ranks notime 'all except latest 7' \
--include-snapshot-times-and-ranks 'anytime..7 days ago'
```


Note: This also prints how many GB of disk space in total would be freed if the command were to be
run for real without the --dryrun flag.

* Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per
dataset) are retained regardless of creation time. Additionally, only delete a snapshot if no
corresponding snapshot or bookmark exists in the source dataset (same as above except replace the
'dummy' source with 'tank1/foo/bar'):


```
$ bzfs tank1/foo/bar tank2/boo/bar --dryrun --recursive --skip-replication \
--delete-dst-snapshots --include-snapshot-regex '.*_daily' \
--include-snapshot-times-and-ranks notime 'all except latest 7' \
--include-snapshot-times-and-ranks '7 days ago..anytime'
```


* Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per
dataset) are retained regardless of creation time. Additionally, only delete a snapshot if no
corresponding snapshot exists in the source dataset (same as above except append
'no-crosscheck'):


```
$ bzfs tank1/foo/bar tank2/boo/bar --dryrun --recursive --skip-replication \
--delete-dst-snapshots --include-snapshot-regex '.*_daily' \
--include-snapshot-times-and-ranks notime 'all except latest 7' \
--include-snapshot-times-and-ranks 'anytime..7 days ago' \
--delete-dst-snapshots-no-crosscheck
```


* Delete all daily bookmarks older than 90 days, but retain the latest 200 daily bookmarks (per
dataset) regardless of creation time:


```
$ bzfs dummy tank1/foo/bar --dryrun --recursive --skip-replication \
--delete-dst-snapshots=bookmarks --include-snapshot-regex '.*_daily' \
--include-snapshot-times-and-ranks notime 'all except latest 200' \
--include-snapshot-times-and-ranks 'anytime..90 days ago'
```


* Delete all tmp datasets within tank2/boo/bar:


```
$ bzfs dummy tank2/boo/bar --dryrun --recursive --skip-replication \
--delete-dst-datasets --include-dataset-regex '(.*/)?tmp.*' --exclude-dataset-regex \
'!.*'
```


* Retain all secondly snapshots that were created less than 40 seconds ago, and ensure that the
latest 40 secondly snapshots (per dataset) are retained regardless of creation time. Same for 40
minutely snapshots, 36 hourly snapshots, 31 daily snapshots, 12 weekly snapshots, 18 monthly
snapshots, and 5 yearly snapshots:


```
$ bzfs dummy tank2/boo/bar --dryrun --recursive --skip-replication \
--delete-dst-snapshots --delete-dst-snapshots-except --include-snapshot-regex '.*_secondly' \
--include-snapshot-times-and-ranks '40 seconds ago..anytime' 'latest 40' \
--new-snapshot-filter-group --include-snapshot-regex '.*_minutely' \
--include-snapshot-times-and-ranks '40 minutes ago..anytime' 'latest 40' \
--new-snapshot-filter-group --include-snapshot-regex '.*_hourly' \
--include-snapshot-times-and-ranks '36 hours ago..anytime' 'latest 36' \
--new-snapshot-filter-group --include-snapshot-regex '.*_daily' \
--include-snapshot-times-and-ranks '31 days ago..anytime' 'latest 31' \
--new-snapshot-filter-group --include-snapshot-regex '.*_weekly' \
--include-snapshot-times-and-ranks '12 weeks ago..anytime' 'latest 12' \
--new-snapshot-filter-group --include-snapshot-regex '.*_monthly' \
--include-snapshot-times-and-ranks '18 months ago..anytime' 'latest 18' \
--new-snapshot-filter-group --include-snapshot-regex '.*_yearly' \
--include-snapshot-times-and-ranks '5 years ago..anytime' 'latest 5'
```


For convenience, the lengthy command line above can be expressed in a more concise way, like so:


```
$ bzfs dummy tank2/boo/bar --dryrun --recursive --skip-replication \
--delete-dst-snapshots --delete-dst-snapshots-except-plan \
"{'prod':{'onsite':{'secondly':40,'minutely':40,'hourly':36,'daily':31,'weekly':12,'monthly':18,'yearly':5}}}"
```


* Compare source and destination dataset trees recursively, for example to check if all recently
taken snapshots have been successfully replicated by a periodic job. List snapshots only contained
in src (tagged with 'src'), only contained in dst (tagged with 'dst'), and contained in both
src and dst (tagged with 'all'), restricted to hourly and daily snapshots taken within the last
7 days, excluding the last 4 hours (to allow for some slack/stragglers), excluding temporary
datasets:


```
$ bzfs tank1/foo/bar tank2/boo/bar --skip-replication \
--compare-snapshot-lists=src+dst+all --recursive --include-snapshot-regex \
'.*_(hourly|daily)' --include-snapshot-times-and-ranks '7 days ago..4 hours ago' \
--exclude-dataset-regex '(.*/)?tmp.*'
```


If the resulting TSV output file contains zero lines starting with the prefix 'src' and zero
lines starting with the prefix 'dst' then no source snapshots are missing on the destination,
and no destination snapshots are missing on the source, indicating that the periodic replication
and pruning jobs perform as expected. The TSV output is sorted by dataset, and by ZFS creation
time within each dataset - the first and last line prefixed with 'all' contains the metadata of
the oldest and latest common snapshot, respectively. The --compare-snapshot-lists option also
directly logs various summary stats, such as the metadata of the latest common snapshot, latest
snapshots and oldest snapshots, as well as the time diff between the latest common snapshot and
latest snapshot only in src (and only in dst), as well as how many src snapshots and how many GB
of data are missing on dst, etc.

* Example with further options:


```
$ bzfs tank1/foo/bar root@host2.example.com:tank2/boo/bar --recursive \
--exclude-snapshot-regex '.*_(secondly|minutely)' --exclude-snapshot-regex 'test_.*' \
--include-snapshot-times-and-ranks '7 days ago..anytime' 'latest 7' --exclude-dataset \
/tank1/foo/bar/temporary --exclude-dataset /tank1/foo/bar/baz/trash --exclude-dataset-regex \
'(.*/)?private' --exclude-dataset-regex \
'(.*/)?[Tt][Ee]?[Mm][Pp][-_]?[0-9]*'
```

<!-- END DESCRIPTION SECTION -->


<!-- FINE TO EDIT -->
# Installation

```
# Ubuntu / Debian:
sudo apt-get -y install zfsutils-linux python3  # ensure zfs and python are installed
sudo apt-get -y install zstd pv mbuffer  # auxiliary helpers are optional

git clone https://github.com/whoschek/bzfs.git
cd bzfs/bzfs_main
./bzfs --help  # Run the CLI
./bzfs_jobrunner --help
sudo ln -sf $(pwd)/bzfs /usr/local/bin/bzfs  # Optional system installation
sudo ln -sf $(pwd)/bzfs_jobrunner /usr/local/bin/bzfs_jobrunner  # Optional system installation

# Alternatively, install a release via pip:
pip install bzfs
bzfs --help  # Run the CLI
bzfs_jobrunner --help

# Alternatively, setup the environment for software development:
git clone https://github.com/whoschek/bzfs.git
cd bzfs
python3 -m venv venv                # Create a Python virtual environment
source venv/bin/activate            # Activate it
pip install -e '.[dev]'             # Install all development dependencies
pre-commit install --install-hooks  # Ensure Linters and Formatters run on every commit
```


<!-- FINE TO EDIT -->
# Design Aspects

* Rsync'ish look and feel.
* Supports a variety of powerful include/exclude filters that can be combined to select which datasets, snapshots and
properties to create or replicate or delete or compare.
* Supports pull, push, pull-push and local transfer mode.
* Prioritizes safe, reliable and predictable operations. Clearly separates read-only mode, append-only mode and
delete mode.
* Continously tested on Linux, FreeBSD and Solaris.
* Code is almost 100% covered by tests.
* Automatically replicates the snapshots of multiple datasets in parallel for best performance. Similarly, quickly
deletes (or monitors or compares) snapshots of multiple datasets in parallel. Atomic snapshots can be created as frequently
as every N milliseconds.
* For replication, periodically prints progress bar, throughput metrics, ETA, etc, to the same console status line (but not
to the log file), which is helpful if the program runs in an interactive terminal session. The metrics represent aggregates
over the parallel replication tasks.
Example console status line:
```
2025-01-17 01:23:04 [I] zfs sent 41.7 GiB 0:00:46 [963 MiB/s] [907 MiB/s] 80% ETA 0:00:04 ETA 01:23:08
```
* Simple and straightforward: Can be installed in less than a minute. Can be fully scripted without configuration
files, or scheduled via cron or similar. Does not require a daemon other than ubiquitous sshd.
* Stays true to the ZFS send/receive spirit. Retains the ability to use ZFS CLI options for fine tuning. Does not
attempt to "abstract away" ZFS concepts and semantics. Keeps simple things simple, and makes complex things possible.
* All ZFS and SSH commands (even in --dryrun mode) are logged such that they can be inspected, copy-and-pasted into
a terminal/shell, and run manually to help anticipate or diagnose issues.
* Supports snapshotting, replicating (or deleting) dataset subsets via powerful include/exclude regexes and other filters,
which can be combined into a mini filter pipeline. Can be told to do such deletions only if a corresponding source dataset
does not exist. For example, can snapshot, replicate (or delete) all except temporary datasets and private datasets.
* Supports replicating (or deleting) snapshot subsets via powerful include/exclude regexes, time based filters, and
oldest N/latest N filters, which can also be combined into a mini filter pipeline.
    * For example, can replicate (or delete) daily and weekly snapshots while ignoring hourly and 5 minute snapshots.
Or, can replicate daily and weekly snapshots to a remote destination while replicating hourly and 5 minute snapshots
to a local destination.
    * For example, can replicate (or delete) all daily snapshots older (or newer) than 90 days, and all weekly snapshots
older (or newer) than 12 weeks.
    * For example, can replicate (or delete) all daily snapshots except the latest (or oldest) 90 daily snapshots,
and all weekly snapshots except the latest (or oldest) 12 weekly snapshots.
    * For example, can replicate all daily snapshots that were created during the last 7 days, and at the
same time ensure that at least the latest 7 daily snapshots are replicated regardless of creation time.
This helps to safely cope with irregular scenarios where no snapshots were created or received within the last 7 days,
or where more than 7 daily snapshots were created or received within the last 7 days.
    * For example, can delete all daily snapshots older than 7 days, but retain the latest 7 daily
snapshots regardless of creation time. It can help to avoid accidental pruning of the last snapshot that source and
destination have in common.
    * Can be told to do such deletions only if a corresponding snapshot does not exist in the source dataset.
    * Optionally, deletions can specify which snapshots to retain instead of which snapshots to delete.
    * Prints how many GB of disk space in total would be freed if the delete command were to be run for real without the
--dryrun flag.
* Compare source and destination dataset trees recursively, in combination with snapshot filters and dataset filters.
* Also supports replicating arbitrary dataset tree subsets by feeding it a list of flat datasets.
* Efficiently supports complex replication policies with multiple sources and multiple destinations for each source.
* Can be told what ZFS dataset properties to copy, also via include/exclude regexes.
* Full and precise ZFS bookmark support for additional safety, or to reclaim disk space earlier.
* Can be strict or told to be tolerant of runtime errors.
* Automatically resumes ZFS send/receive operations that have been interrupted by network hiccups or other
intermittent failures, via efficient 'zfs receive -s' and 'zfs send -t'.
* Similarly, can be told to automatically retry snapshot delete operations.
* Parametrizable retry logic.
* Multiple bzfs processes can run in parallel. If multiple processes attempt to write to the same destination dataset
simultaneously this is detected and the operation can be auto-retried safely.
* A job that runs periodically declines to start if the same previous periodic job is still running without
completion yet.
* Can log to local and remote destinations out of the box. Logging mechanism is customizable and plugable for smooth
integration.
* Codebase is easy to change and maintain. No hidden magic. Python is very readable to contemporary engineers.
Chances are that CI tests will catch changes that have unintended side effects.
* It's fast!


# Continuous Testing

Results of continuous test runs on a matrix of various old and new versions of ZFS/Python/Linux/FreeBSD/Solaris are
[here](https://github.com/whoschek/bzfs/actions/workflows/python-app.yml?query=event%3Aschedule), as generated
by [this script](https://github.com/whoschek/bzfs/blob/main/.github/workflows/python-app.yml).
The script also demonstrates functioning installation steps on Ubuntu, FreeBSD, Solaris, etc.
The script also generates code coverage reports which are published
[here](https://whoschek.github.io/bzfs/coverage).

The gist is that it should work on any flavor, with python (3.8 or higher, no additional python packages required)
only needed on the initiator host.


# Unit Testing on GitHub

* First, on the GitHub page of this repo, click on "Fork/Create a new fork".
* Click the 'Actions' menu on your repo, and then enable GitHub Actions on your fork.
* Then select 'All workflows' -> 'Tests' on the left side.
* Then click the 'Run workflow' dropdown menu on the right side, which looks something like
[this screenshot](https://raw.githubusercontent.com/whoschek/bzfs/main/bzfs_docs/run_workflow_dialog.jpg).
* Select the name of the job to run (e.g. 'test_ubuntu_24_04' or 'test_freebsd_14_1' or 'test_solaris_11_4', etc) or
select 'Run all jobs' to test all supported platforms, plus select the git branch to run with (typically the branch
containing your changes).
* Then click the 'Run workflow' button which kicks off the job.
* Click on the job to watch job progress.
* Once the run completes, you can click on the wheel on the top right to select 'Download log archive', which is
a zip file containing the output of all jobs of the run. This is useful for debugging.
* Once the job completes, also a coverage report appears on the bottom of the run page, which you can download by
clicking on it. Unzip and browse the HTML coverage report to see if the code you added or changed is actually executed
by a test. Experience shows that code that isn't executed likely contains bugs, so all changes (code lines and
branch conditions) should be covered by a test before a merge is considered. In practice, this means to watch out
for coverage report lines that aren't colored green.
* FYI, example test runs with coverage reports are
[here](https://github.com/whoschek/bzfs/actions/workflows/python-app.yml?query=event%3Aschedule).
Click on any run and browse to the bottom of the resulting run page to find the coverage reports, including a combined
coverage report that merges all jobs of the run.


# Unit Testing Locally
```bash
# Check prerequisites
zfs --version             # verify ZFS is installed
python3 --version         # verify Python 3.8 or newer is installed
sudo ls                   # verify sudo works

# If sshd uses a non-standard port
# export bzfs_test_ssh_port=12345

# Choose test scope
# export bzfs_test_mode=unit       # unit tests only (takes < 5 seconds)
export bzfs_test_mode=smoke        # small, quick integration test subset (takes < 1 minute)
# export bzfs_test_mode=functional # most integration tests but only in a single local config (takes ~4 minutes)
# unset bzfs_test_mode             # all tests (takes ~1 hour)

# Confirm passwordless ssh works on connecting to localhost loopback address
# You should not get prompted for a password
ssh -p ${bzfs_test_ssh_port:-22} 127.0.0.1 echo hello

# Ensure tools are on remote PATH
ssh -p ${bzfs_test_ssh_port:-22} 127.0.0.1 zfs --version
ssh -p ${bzfs_test_ssh_port:-22} 127.0.0.1 zpool --version
ssh -p ${bzfs_test_ssh_port:-22} 127.0.0.1 pv --version      # enables progress reporting
ssh -p ${bzfs_test_ssh_port:-22} 127.0.0.1 zstd --version    # enables compression-on-the-wire
ssh -p ${bzfs_test_ssh_port:-22} 127.0.0.1 mbuffer --version # enables buffering

# Run the tests
./test.sh        # when cloned from git
# or
bzfs-test        # when installed via pip
```


# Man Page

<!-- DO NOT EDIT (This section was auto-generated from ArgumentParser help text as the source of "truth", via update_readme.sh) -->
<!-- BEGIN HELP OVERVIEW SECTION -->
```
usage: bzfs [-h] [--recursive]
            [--include-dataset DATASET [DATASET ...]]
            [--exclude-dataset DATASET [DATASET ...]]
            [--include-dataset-regex REGEX [REGEX ...]]
            [--exclude-dataset-regex REGEX [REGEX ...]]
            [--exclude-dataset-property STRING]
            [--include-snapshot-regex REGEX [REGEX ...]]
            [--exclude-snapshot-regex REGEX [REGEX ...]]
            [--include-snapshot-times-and-ranks TIMERANGE [RANKRANGE ...]]
            [--include-snapshot-plan DICT_STRING]
            [--new-snapshot-filter-group] [--create-src-snapshots]
            [--create-src-snapshots-plan DICT_STRING]
            [--create-src-snapshots-timeformat STRFTIME_SPEC]
            [--create-src-snapshots-timezone TZ_SPEC]
            [--create-src-snapshots-even-if-not-due]
            [--zfs-send-program-opts STRING]
            [--zfs-recv-program-opts STRING]
            [--zfs-recv-program-opt STRING]
            [--preserve-properties STRING [STRING ...]]
            [--force-rollback-to-latest-snapshot]
            [--force-rollback-to-latest-common-snapshot] [--force]
            [--force-destroy-dependents] [--force-unmount]
            [--force-once] [--skip-parent]
            [--skip-missing-snapshots [{fail,dataset,continue}]]
            [--retries INT] [--retry-min-sleep-secs FLOAT]
            [--retry-max-sleep-secs FLOAT]
            [--retry-max-elapsed-secs FLOAT]
            [--skip-on-error {fail,tree,dataset}] [--skip-replication]
            [--delete-dst-datasets]
            [--delete-dst-snapshots [{snapshots,bookmarks}]]
            [--delete-dst-snapshots-no-crosscheck]
            [--delete-dst-snapshots-except]
            [--delete-dst-snapshots-except-plan DICT_STRING]
            [--delete-empty-dst-datasets [{snapshots,snapshots+bookmarks}]]
            [--monitor-snapshots DICT_STRING]
            [--monitor-snapshots-dont-warn]
            [--monitor-snapshots-dont-crit]
            [--compare-snapshot-lists [{src,dst,all,src+dst,src+all,dst+all,src+dst+all}]]
            [--cache-snapshots [{true,false}]]
            [--dryrun [{recv,send}]] [--verbose] [--quiet]
            [--no-privilege-elevation] [--no-stream]
            [--no-resume-recv] [--create-bookmarks {all,many,none}]
            [--no-use-bookmark] [--ssh-src-user STRING]
            [--ssh-dst-user STRING] [--ssh-src-host STRING]
            [--ssh-dst-host STRING] [--ssh-src-port INT]
            [--ssh-dst-port INT] [--ssh-src-config-file FILE]
            [--ssh-dst-config-file FILE] [--threads INT[%]]
            [--max-concurrent-ssh-sessions-per-tcp-connection INT]
            [--bwlimit STRING]
            [--compression-program {zstd,lz4,pzstd,pigz,gzip,bzip2,-}]
            [--compression-program-opts STRING]
            [--mbuffer-program {mbuffer,-}]
            [--mbuffer-program-opts STRING] [--ps-program {ps,-}]
            [--pv-program {pv,-}] [--pv-program-opts STRING]
            [--shell-program {sh,-}] [--ssh-program {ssh,hpnssh,-}]
            [--sudo-program {sudo,-}] [--zpool-program {zpool,-}]
            [--log-dir DIR] [--log-file-prefix STRING]
            [--log-file-infix STRING] [--log-file-suffix STRING]
            [--log-subdir {daily,hourly,minutely}]
            [--log-syslog-address STRING]
            [--log-syslog-socktype {UDP,TCP}]
            [--log-syslog-facility INT] [--log-syslog-prefix STRING]
            [--log-syslog-level {CRITICAL,ERROR,WARN,INFO,DEBUG,TRACE}]
            [--log-config-file STRING]
            [--log-config-var NAME:VALUE [NAME:VALUE ...]]
            [--include-envvar-regex REGEX [REGEX ...]]
            [--exclude-envvar-regex REGEX [REGEX ...]]
            [--yearly_year INT] [--yearly_month INT]
            [--yearly_monthday INT] [--yearly_hour INT]
            [--yearly_minute INT] [--yearly_second INT]
            [--monthly_month INT] [--monthly_monthday INT]
            [--monthly_hour INT] [--monthly_minute INT]
            [--monthly_second INT] [--weekly_weekday INT]
            [--weekly_hour INT] [--weekly_minute INT]
            [--weekly_second INT] [--daily_hour INT]
            [--daily_minute INT] [--daily_second INT]
            [--hourly_minute INT] [--hourly_second INT]
            [--minutely_second INT] [--secondly_millisecond INT]
            [--millisecondly_microsecond INT]
            [--zfs-recv-o-targets {full,incremental,full+incremental}]
            [--zfs-recv-o-sources STRING]
            [--zfs-recv-o-include-regex REGEX [REGEX ...]]
            [--zfs-recv-o-exclude-regex REGEX [REGEX ...]]
            [--zfs-recv-x-targets {full,incremental,full+incremental}]
            [--zfs-recv-x-sources STRING]
            [--zfs-recv-x-include-regex REGEX [REGEX ...]]
            [--zfs-recv-x-exclude-regex REGEX [REGEX ...]] [--version]
            [--help, -h]
            SRC_DATASET DST_DATASET [SRC_DATASET DST_DATASET ...]
```
<!-- END HELP OVERVIEW SECTION -->

<!-- BEGIN HELP DETAIL SECTION -->
<div id="SRC_DATASET_DST_DATASET"></div>

**SRC_DATASET DST_DATASET**

*  SRC_DATASET: Source ZFS dataset (and its descendants) that will be replicated. Can be a ZFS
    filesystem or ZFS volume. Format is [[user@]host:]dataset. The host name can also be an
    IPv4 address (or an IPv6 address where each ':' colon character must be replaced with a
    '|' pipe character for disambiguation). If the host name is '-', the dataset will be on
    the local host, and the corresponding SSH leg will be omitted. The same is true if the host is
    omitted and the dataset does not contain a ':' colon at the same time. Local dataset
    examples: `tank1/foo/bar`, `tank1`, `-:tank1/foo/bar:baz:boo` Remote dataset examples:
    `host:tank1/foo/bar`, `host.example.com:tank1/foo/bar`, `root@host:tank`,
    `root@host.example.com:tank1/foo/bar`, `user@127.0.0.1:tank1/foo/bar:baz:boo`,
    `user@||1:tank1/foo/bar:baz:boo`. The first component of the ZFS dataset name is the ZFS
    pool name, here `tank1`. If the option starts with a `+` prefix then dataset names are
    read from the UTF-8 text file given after the `+` prefix, with each line in the file
    containing a SRC_DATASET and a DST_DATASET, separated by a tab character. Example:
    `+root_dataset_names.txt`, `+/path/to/root_dataset_names.txt`

    DST_DATASET: Destination ZFS dataset for replication and deletion. Has same naming format as
    SRC_DATASET. During replication, destination datasets that do not yet exist are created as
    necessary, along with their parent and ancestors.

    *Performance Note:* bzfs automatically replicates multiple datasets in parallel. It
    replicates snapshots in parallel across datasets and serially within a dataset. All child
    datasets of a dataset may be processed in parallel. For consistency, processing of a dataset
    only starts after processing of all its ancestor datasets has completed. Further, when a
    thread is ready to start processing another dataset, it chooses the next dataset wrt.
    lexicographical sort order from the datasets that are currently available for start of
    processing. Initially, only the roots of the selected dataset subtrees are available for start
    of processing. The degree of parallelism is configurable with the --threads option (see
    below).



<div id="--recursive"></div>

**--recursive**, **-r**

*  During snapshot creation, replication, deletion and comparison, also consider descendant
    datasets, i.e. datasets within the dataset tree, including children, and children of children,
    etc.

<!-- -->

<div id="--include-dataset"></div>

**--include-dataset** *DATASET [DATASET ...]*

*  During snapshot creation, replication, deletion and comparison, select any ZFS dataset (and
    its descendants) that is contained within SRC_DATASET (DST_DATASET in case of deletion) if its
    dataset name is one of the given include dataset names but none of the exclude dataset names.
    If a dataset is excluded its descendants are automatically excluded too, and this decision is
    never reconsidered even for the descendants because exclude takes precedence over include.

    A dataset name is absolute if the specified dataset is prefixed by `/`, e.g.
    `/tank/baz/tmp`. Otherwise the dataset name is relative wrt. source and destination, e.g.
    `baz/tmp` if the source is `tank`.

    This option is automatically translated to an --include-dataset-regex (see below) and can be
    specified multiple times.

    If the option starts with a `+` prefix then dataset names are read from the
    newline-separated UTF-8 text file given after the `+` prefix, one dataset per line inside of
    the text file.

    Examples: `/tank/baz/tmp` (absolute), `baz/tmp` (relative), `+dataset_names.txt`,
    `+/path/to/dataset_names.txt`

<!-- -->

<div id="--exclude-dataset"></div>

**--exclude-dataset** *DATASET [DATASET ...]*

*  Same syntax as --include-dataset (see above) except that the option is automatically
    translated to an --exclude-dataset-regex (see below).

<!-- -->

<div id="--include-dataset-regex"></div>

**--include-dataset-regex** *REGEX [REGEX ...]*

*  During snapshot creation, replication (and deletion) and comparison, select any ZFS dataset
    (and its descendants) that is contained within SRC_DATASET (DST_DATASET in case of deletion)
    if its relative dataset path (e.g. `baz/tmp`) wrt. SRC_DATASET (DST_DATASET in case of
    deletion) matches at least one of the given include regular expressions but none of the
    exclude regular expressions. If a dataset is excluded its descendants are automatically
    excluded too, and this decision is never reconsidered even for the descendants because exclude
    takes precedence over include.

    This option can be specified multiple times. A leading `!` character indicates logical
    negation, i.e. the regex matches if the regex with the leading `!` character removed does
    not match.

    If the option starts with a `+` prefix then regex names are read from the newline-separated
    UTF-8 text file given after the `+` prefix, one regex per line inside of the text file.

    Default: `.*` (include all datasets).

    Examples: `baz/tmp`, `(.*/)?doc[^/]*/(private|confidential).*`, `!public`,
    `+dataset_regexes.txt`, `+/path/to/dataset_regexes.txt`

<!-- -->

<div id="--exclude-dataset-regex"></div>

**--exclude-dataset-regex** *REGEX [REGEX ...]*

*  Same syntax as --include-dataset-regex (see above) except that the default is
    `(.*/)?[Tt][Ee]?[Mm][Pp][-_]?[0-9]*` (exclude tmp datasets). Example:
    `!.*` (exclude no dataset)

<!-- -->

<div id="--exclude-dataset-property"></div>

**--exclude-dataset-property** *STRING*

*  The name of a ZFS dataset user property (optional). If this option is specified, the effective
    value (potentially inherited) of that user property is read via 'zfs list' for each selected
    source dataset to determine whether the dataset will be included or excluded, as follows:

    a) Value is 'true' or '-' or empty string or the property is missing: Include the
    dataset.

    b) Value is 'false': Exclude the dataset and its descendants.

    c) Value is a comma-separated list of host names (no spaces, for example:
    'store001,store002'): Include the dataset if the host name of the host executing bzfs is
    contained in the list, otherwise exclude the dataset and its descendants.

    If a dataset is excluded its descendants are automatically excluded too, and the property
    values of the descendants are ignored because exclude takes precedence over include.

    Examples: 'syncoid:sync', 'com.example.eng.project.x:backup'

    *Note:* The use of --exclude-dataset-property is discouraged for most use cases. It is more
    flexible, more powerful, *and* more efficient to instead use a combination of
    --include/exclude-dataset-regex and/or --include/exclude-dataset to achieve the same or
    better outcome.

<!-- -->

<div id="--include-snapshot-regex"></div>

**--include-snapshot-regex** *REGEX [REGEX ...]*

*  During replication, deletion and comparison, select any source ZFS snapshot that has a name
    (i.e. the part after the '@') that matches at least one of the given include regular
    expressions but none of the exclude regular expressions. If a snapshot is excluded this
    decision is never reconsidered because exclude takes precedence over include.

    This option can be specified multiple times. A leading `!` character indicates logical
    negation, i.e. the regex matches if the regex with the leading `!` character removed does
    not match.

    Default: `.*` (include all snapshots). Examples: `test_.*`, `!prod_.*`,
    `.*_(hourly|frequent)`, `!.*_(weekly|daily)`

    *Note:* All --include/exclude-snapshot-* CLI option groups are combined into a mini filter
    pipeline. A filter pipeline is executed in the order given on the command line, left to right.
    For example if --include-snapshot-times-and-ranks (see below) is specified on the command
    line before --include/exclude-snapshot-regex, then --include-snapshot-times-and-ranks will
    be applied before --include/exclude-snapshot-regex. The pipeline results would not always be
    the same if the order were reversed. Order matters.

    *Note:* During replication, bookmarks are always retained aka selected in order to help find
    common snapshots between source and destination.

<!-- -->

<div id="--exclude-snapshot-regex"></div>

**--exclude-snapshot-regex** *REGEX [REGEX ...]*

*  Same syntax as --include-snapshot-regex (see above) except that the default is to exclude no
    snapshots.

<!-- -->

<div id="--include-snapshot-times-and-ranks"></div>

**--include-snapshot-times-and-ranks** *TIMERANGE [RANKRANGE ...]*

*  This option takes as input parameters a time range filter and an optional rank range filter.
    It separately computes the results for each filter and selects the UNION of both results. To
    instead use a pure rank range filter (no UNION), or a pure time range filter (no UNION),
    simply use 'notime' aka '0..0' to indicate an empty time range, or omit the rank range,
    respectively. This option can be specified multiple times.

    <b>*Replication Example (UNION):* </b>

    Specify to replicate all daily snapshots created during the last 7 days, and at the same time
    ensure that the latest 7 daily snapshots (per dataset) are replicated regardless of creation
    time, like so: `--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks
    '7 days ago..anytime' 'latest 7'`

    <b>*Deletion Example (no UNION):* </b>

    Specify to delete all daily snapshots older than 7 days, but ensure that the latest 7 daily
    snapshots (per dataset) are retained regardless of creation time, like so:
    `--include-snapshot-regex '.*_daily' --include-snapshot-times-and-ranks notime 'all
    except latest 7' --include-snapshot-times-and-ranks 'anytime..7 days ago'`

    This helps to safely cope with irregular scenarios where no snapshots were created or received
    within the last 7 days, or where more than 7 daily snapshots were created within the last 7
    days. It can also help to avoid accidental pruning of the last snapshot that source and
    destination have in common.

    <b>*TIMERANGE:* </b>

    The ZFS 'creation' time of a snapshot (and bookmark) must fall into this time range in order
    for the snapshot to be included. The time range consists of a 'start' time, followed by a
    '..' separator, followed by an 'end' time. For example '2024-01-01..2024-04-01', or
    'anytime..anytime' aka `*..*` aka all times, or 'notime' aka '0..0' aka empty time
    range. Only snapshots (and bookmarks) in the half-open time range [start, end) are included;
    other snapshots (and bookmarks) are excluded. If a snapshot is excluded this decision is never
    reconsidered because exclude takes precedence over include. Each of the two specified times
    can take any of the following forms:

    * a) `anytime` aka `*` wildcard; represents negative or positive infinity.

    * b) a non-negative integer representing a UTC Unix time in seconds. Example: 1728109805

    * c) an ISO 8601 datetime string with or without timezone. Examples: '2024-10-05',
    '2024-10-05T14:48:55', '2024-10-05T14:48:55+02', '2024-10-05T14:48:55-04:30'. If the
    datetime string does not contain time zone info then it is assumed to be in the local time
    zone. Timezone string support requires Python >= 3.11.

    * d) a duration that indicates how long ago from the current time, using the following
    syntax: a non-negative integer, followed by an optional space, followed by a duration unit
    that is *one* of 'seconds', 'secs', 'minutes', 'mins', 'hours', 'days',
    'weeks', 'months', 'years', followed by an optional space, followed by the word 'ago'.
    Examples: '0secs ago', '40 mins ago', '36hours ago', '90days ago', '12weeksago'.

    * Note: This option compares the specified time against the standard ZFS 'creation' time
    property of the snapshot (which is a UTC Unix time in integer seconds), rather than against a
    timestamp that may be part of the snapshot name. You can list the ZFS creation time of
    snapshots and bookmarks as follows: `zfs list -t snapshot,bookmark -o name,creation -s
    creation -d 1 $SRC_DATASET` (optionally add the -p flag to display UTC Unix time in integer
    seconds).

    *Note:* During replication, bookmarks are always retained aka selected in order to help find
    common snapshots between source and destination.

    <b>*RANKRANGE:* </b>

    Specifies to include the N (or N%) oldest snapshots or latest snapshots, and exclude all other
    snapshots (default: include no snapshots). Snapshots are sorted by creation time (actually, by
    the 'createtxg' ZFS property, which serves the same purpose but is more precise). The rank
    position of a snapshot is the zero-based integer position of the snapshot within that sorted
    list. A rank consists of the optional words 'all except' (followed by an optional space),
    followed by the word 'oldest' or 'latest', followed by a non-negative integer, followed by
    an optional '%' percent sign. A rank range consists of a lower rank, followed by a '..'
    separator, followed by a higher rank. If the optional lower rank is missing it is assumed to
    be 0. Examples:

    * 'oldest 10%' aka 'oldest 0..oldest 10%' (include the oldest 10% of all snapshots)

    * 'latest 10%' aka 'latest 0..latest 10%' (include the latest 10% of all snapshots)

    * 'all except latest 10%' aka 'oldest 90%' aka 'oldest 0..oldest 90%' (include all
    snapshots except the latest 10% of all snapshots)

    * 'oldest 90' aka 'oldest 0..oldest 90' (include the oldest 90 snapshots)

    * 'latest 90' aka 'latest 0..latest 90' (include the latest 90 snapshots)

    * 'all except oldest 90' aka 'oldest 90..oldest 100%' (include all snapshots except the
    oldest 90 snapshots)

    * 'all except latest 90' aka 'latest 90..latest 100%' (include all snapshots except the
    latest 90 snapshots)

    * 'latest 1' aka 'latest 0..latest 1' (include the latest snapshot)

    * 'all except latest 1' aka 'latest 1..latest 100%' (include all snapshots except the
    latest snapshot)

    * 'oldest 2' aka 'oldest 0..oldest 2' (include the oldest 2 snapshots)

    * 'all except oldest 2' aka 'oldest 2..oldest 100%' (include all snapshots except the
    oldest 2 snapshots)

    * 'oldest 100%' aka 'oldest 0..oldest 100%' (include all snapshots)

    * 'oldest 0%' aka 'oldest 0..oldest 0%' (include no snapshots)

    * 'oldest 0' aka 'oldest 0..oldest 0' (include no snapshots)

    *Note:* If multiple RANKRANGEs are specified within a single
    --include-snapshot-times-and-ranks option, each subsequent rank range operates on the output
    of the preceding rank rage.

    *Note:* Percentage calculations are not based on the number of snapshots contained in the
    dataset on disk, but rather based on the number of snapshots arriving at the filter. For
    example, if only two daily snapshots arrive at the filter because a prior filter excludes
    hourly snapshots, then 'latest 10' will only include these two daily snapshots, and 'latest
    50%' will only include one of these two daily snapshots.

    *Note:* During replication, bookmarks are always retained aka selected in order to help find
    common snapshots between source and destination. Bookmarks do not count towards N or N% wrt.
    rank.

    *Note:* If a snapshot is excluded this decision is never reconsidered because exclude takes
    precedence over include.

<!-- -->

<div id="--include-snapshot-plan"></div>

**--include-snapshot-plan** *DICT_STRING*

*  Replication periods to be used if replicating snapshots within the selected destination
    datasets. Has the same format as --create-src-snapshots-plan and
    --delete-dst-snapshots-except-plan (see below). Snapshots that do not match a period will not
    be replicated. To avoid unexpected surprises, make sure to carefully specify ALL snapshot
    names and periods that shall be replicated, in combination with --dryrun.

    Example: `"{'prod': {'onsite': {'secondly': 40, 'minutely': 40, 'hourly': 36,
    'daily': 31, 'weekly': 12, 'monthly': 18, 'yearly': 5}, 'us-west-1': {'secondly':
    0, 'minutely': 0, 'hourly': 36, 'daily': 31, 'weekly': 12, 'monthly': 18,
    'yearly': 5}, 'eu-west-1': {'secondly': 0, 'minutely': 0, 'hourly': 36, 'daily':
    31, 'weekly': 12, 'monthly': 18, 'yearly': 5}}, 'test': {'offsite': {'12hourly':
    42, 'weekly': 12}, 'onsite': {'100millisecondly': 42}}}"`. This example will, for the
    organization 'prod' and the intended logical target 'onsite', replicate secondly snapshots
    that were created less than 40 seconds ago, yet replicate the latest 40 secondly snapshots
    regardless of creation time. Analog for the latest 40 minutely snapshots, latest 36 hourly
    snapshots, etc. Note: A zero within a period (e.g. 'hourly': 0) indicates that no snapshots
    shall be replicated for the given period.

    Note: --include-snapshot-plan is a convenience option that auto-generates a series of the
    following other options: --new-snapshot-filter-group, --include-snapshot-regex,
    --include-snapshot-times-and-ranks

<!-- -->

<div id="--new-snapshot-filter-group"></div>

**--new-snapshot-filter-group**

*  Starts a new snapshot filter group containing separate --{include|exclude}-snapshot-*
    filter options. The program separately computes the results for each filter group and selects
    the UNION of all results. This option can be specified multiple times and serves as a
    separator between groups. Example:

    Delete all minutely snapshots older than 40 minutes, but ensure that the latest 40 minutely
    snapshots (per dataset) are retained regardless of creation time. Additionally, delete all
    hourly snapshots older than 36 hours, but ensure that the latest 36 hourly snapshots (per
    dataset) are retained regardless of creation time. Additionally, delete all daily snapshots
    older than 31 days, but ensure that the latest 31 daily snapshots (per dataset) are retained
    regardless of creation time: `bzfs dummy tank2/boo/bar --dryrun --recursive
    --skip-replication --delete-dst-snapshots --include-snapshot-regex '.*_minutely'
    --include-snapshot-times-and-ranks notime 'all except latest 40'
    --include-snapshot-times-and-ranks 'anytime..40 minutes ago' --new-snapshot-filter-group
    --include-snapshot-regex '.*_hourly' --include-snapshot-times-and-ranks notime 'all
    except latest 36' --include-snapshot-times-and-ranks 'anytime..36 hours ago'
    --new-snapshot-filter-group --include-snapshot-regex '.*_daily'
    --include-snapshot-times-and-ranks notime 'all except latest 31'
    --include-snapshot-times-and-ranks 'anytime..31 days ago'`

<!-- -->

<div id="--create-src-snapshots"></div>

**--create-src-snapshots**

*  Do nothing if the --create-src-snapshots flag is missing. Otherwise, before the replication
    step (see below), atomically create new snapshots of the source datasets selected via
    --{include|exclude}-dataset* policy. The names of the snapshots can be configured via
    --create-src-snapshots-* suboptions (see below). To create snapshots only, without any other
    processing such as replication, etc, consider using this flag together with the
    --skip-replication flag.

    A periodic snapshot is created if it is due per the schedule indicated by
    --create-src-snapshots-plan (for example '_daily' or '_hourly' or _'10minutely' or
    '_2secondly' or '_100millisecondly'), or if the --create-src-snapshots-even-if-not-due
    flag is specified, or if the most recent scheduled snapshot is somehow missing. In the latter
    case bzfs immediately creates a snapshot (tagged with the current time, not backdated to the
    missed time), and then resumes the original schedule.

    If the snapshot suffix is '_adhoc' or not a known period then a snapshot is considered
    non-periodic and is thus created immediately regardless of the creation time of any existing
    snapshot.

    The implementation attempts to fit as many datasets as possible into a single (atomic) 'zfs
    snapshot' command line, using lexicographical sort order, and using 'zfs snapshot -r' to
    the extent that this is compatible with the actual results of the schedule and the actual
    results of the --{include|exclude}-dataset* pruning policy. The snapshots of all datasets
    that fit within the same single 'zfs snapshot' CLI invocation will be taken within the same
    ZFS transaction group, and correspondingly have identical 'createtxg' ZFS property (but not
    necessarily identical 'creation' ZFS time property as ZFS actually provides no such
    guarantee), and thus be consistent. Dataset names that can't fit into a single command line
    are spread over multiple command line invocations, respecting the limits that the operating
    system places on the maximum length of a single command line, per `getconf ARG_MAX`.

    Note: All bzfs functions including snapshot creation, replication, deletion, monitoring,
    comparison, etc. happily work with any snapshots in any format, even created or managed by
    third party ZFS snapshot management tools, including manual zfs snapshot/destroy.

<!-- -->

<div id="--create-src-snapshots-plan"></div>

**--create-src-snapshots-plan** *DICT_STRING*

*  Creation periods that specify a schedule for when new snapshots shall be created on src within
    the selected datasets. Has the same format as --delete-dst-snapshots-except-plan.

    Example: `"{'prod': {'onsite': {'secondly': 40, 'minutely': 40, 'hourly': 36,
    'daily': 31, 'weekly': 12, 'monthly': 18, 'yearly': 5}, 'us-west-1': {'secondly':
    0, 'minutely': 0, 'hourly': 36, 'daily': 31, 'weekly': 12, 'monthly': 18,
    'yearly': 5}, 'eu-west-1': {'secondly': 0, 'minutely': 0, 'hourly': 36, 'daily':
    31, 'weekly': 12, 'monthly': 18, 'yearly': 5}}, 'test': {'offsite': {'12hourly':
    42, 'weekly': 12}, 'onsite': {'100millisecondly': 42}}}"`. This example will, for the
    organization 'prod' and the intended logical target 'onsite', create 'secondly'
    snapshots every second, 'minutely' snapshots every minute, hourly snapshots every hour, and
    so on. It will also create snapshots for the targets 'us-west-1' and 'eu-west-1' within
    the 'prod' organization. In addition, it will create snapshots every 12 hours and every week
    for the 'test' organization, and name them as being intended for the 'offsite' replication
    target. Analog for snapshots that are taken every 100 milliseconds within the 'test'
    organization.

    The example creates ZFS snapshots with names like `prod_onsite_<timestamp>_secondly`,
    `prod_onsite_<timestamp>_minutely`, `prod_us-west-1_<timestamp>_hourly`,
    `prod_us-west-1_<timestamp>_daily`, `prod_eu-west-1_<timestamp>_hourly`,
    `prod_eu-west-1_<timestamp>_daily`, `test_offsite_<timestamp>_12hourly`,
    `test_offsite_<timestamp>_weekly`, and so on.

    Note: A period name that is missing indicates that no snapshots shall be created for the given
    period.

    The period name can contain an optional positive integer immediately preceding the time period
    unit, for example `_2secondly` or `_10minutely` or `_100millisecondly` to indicate
    that snapshots are taken every 2 seconds, or every 10 minutes, or every 100 milliseconds,
    respectively.

<!-- -->

<div id="--create-src-snapshots-timeformat"></div>

**--create-src-snapshots-timeformat** *STRFTIME_SPEC*

*  Default is `%Y-%m-%d_%H:%M:%S`. For the strftime format, see
    https://docs.python.org/3.11/library/datetime.html#strftime-strptime-behavior. Examples:
    `%Y-%m-%d_%H:%M:%S.%f` (adds microsecond resolution), `%Y-%m-%d_%H:%M:%S%z` (adds
    timezone offset), `%Y-%m-%dT%H-%M-%S` (no colons).

    The name of the snapshot created on the src is
    `$org_$target_strftime(--create-src-snapshots-time*)_$period`. Example:
    `tank/foo@prod_us-west-1_2024-09-03_12:26:15_daily`

<!-- -->

<div id="--create-src-snapshots-timezone"></div>

**--create-src-snapshots-timezone** *TZ_SPEC*

*  Default is the local timezone of the system running bzfs. When creating a new snapshot on the
    source, fetch the current time in the specified timezone, and feed that time, and the value of
    --create-src-snapshots-timeformat, into the standard strftime() function to generate the
    timestamp portion of the snapshot name. The TZ_SPEC input parameter is of the form 'UTC' or
    '+HHMM' or '-HHMM' for fixed UTC offsets, or an IANA TZ identifier for auto-adjustment to
    daylight savings time, or the empty string to use the local timezone, for example '',
    'UTC', '+0000', '+0530', '-0400', 'America/Los_Angeles', 'Europe/Vienna'. For a
    list of valid IANA TZ identifiers see
    https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List

    To change the timezone not only for snapshot name creation, but in all respects for the entire
    program, use the standard 'TZ' Unix environment variable, like so: `export TZ=UTC`.

<!-- -->

<div id="--create-src-snapshots-even-if-not-due"></div>

**--create-src-snapshots-even-if-not-due**

*  Take snapshots immediately regardless of the creation time of any existing snapshot, even if
    snapshots are periodic and not actually due per the schedule.

<!-- -->

<div id="--zfs-send-program-opts"></div>

**--zfs-send-program-opts** *STRING*

*  Parameters to fine-tune 'zfs send' behaviour (optional); will be passed into 'zfs
    send' CLI. The value is split on runs of one or more whitespace characters. Default is
    '--props --raw --compressed'. To run `zfs send` without options, specify the empty
    string: `--zfs-send-program-opts=''`. See
    https://openzfs.github.io/openzfs-docs/man/master/8/zfs-send.8.html and
    https://github.com/openzfs/zfs/issues/13024

<!-- -->

<div id="--zfs-recv-program-opts"></div>

**--zfs-recv-program-opts** *STRING*

*  Parameters to fine-tune 'zfs receive' behaviour (optional); will be passed into 'zfs
    receive' CLI. The value is split on runs of one or more whitespace characters. Default is
    '-u'. To run `zfs receive` without options, specify the empty string:
    `--zfs-recv-program-opts=''`. Example: '-u -o canmount=noauto -o readonly=on -x
    keylocation -x keyformat -x encryption'. See
    https://openzfs.github.io/openzfs-docs/man/master/8/zfs-receive.8.html and
    https://openzfs.github.io/openzfs-docs/man/master/7/zfsprops.7.html

<!-- -->

<div id="--zfs-recv-program-opt"></div>

**--zfs-recv-program-opt** *STRING*

*  Parameter to fine-tune 'zfs receive' behaviour (optional); will be passed into 'zfs
    receive' CLI. The value can contain spaces and is not split. This option can be specified
    multiple times. Example: `--zfs-recv-program-opt=-o
    --zfs-recv-program-opt='org.zfsbootmenu:commandline=ro debug zswap.enabled=1'`

<!-- -->

<div id="--preserve-properties"></div>

**--preserve-properties** *STRING [STRING ...]*

*  On replication, preserve the current value of ZFS properties with the given names on the
    destination datasets. The destination ignores the property value it 'zfs receive's from the
    source if the property name matches one of the given blacklist values. This prevents a
    compromised or untrusted source from overwriting security-critical properties on the
    destination. The default is to preserve none, i.e. an empty blacklist.

    Example blacklist that protects against dangerous overwrites: mountpoint overlay sharenfs
    sharesmb exec setuid devices encryption keyformat keylocation volsize

    See https://openzfs.github.io/openzfs-docs/man/master/7/zfsprops.7.html and
    https://openzfs.github.io/openzfs-docs/man/master/8/zfs-receive.8.html#x

    Note: --preserve-properties uses the 'zfs recv -x' option and thus requires either OpenZFS
    >= 2.2.0 (see
    https://github.com/openzfs/zfs/commit/b0269cd8ced242e66afc4fa856d62be29bb5a4ff), or that 'zfs
    send --props' is not used.

<!-- -->

<div id="--force-rollback-to-latest-snapshot"></div>

**--force-rollback-to-latest-snapshot**

*  Before replication, rollback the destination dataset to its most recent destination snapshot
    (if there is one), via 'zfs rollback', just in case the destination dataset was modified
    since its most recent snapshot. This is much less invasive than the other --force* options
    (see below).

<!-- -->

<div id="--force-rollback-to-latest-common-snapshot"></div>

**--force-rollback-to-latest-common-snapshot**

*  Before replication, delete destination ZFS snapshots that are more recent than the most recent
    common snapshot selected on the source ('conflicting snapshots'), via 'zfs rollback'. Do
    no rollback if no common snapshot is selected.

<!-- -->

<div id="--force"></div>

**--force**

*  Same as --force-rollback-to-latest-common-snapshot (see above), except that additionally, if
    no common snapshot is selected, then delete all destination snapshots before starting
    replication, and proceed without aborting. Without the --force* flags, the destination
    dataset is treated as append-only, hence no destination snapshot that already exists is
    deleted, and instead the operation is aborted with an error when encountering a conflicting
    snapshot.

    Analogy: --force-rollback-to-latest-snapshot is a tiny hammer, whereas
    --force-rollback-to-latest-common-snapshot is a medium sized hammer, --force is a large
    hammer, and --force-destroy-dependents is a very large hammer. Consider using the smallest
    hammer that can fix the problem. No hammer is ever used by default.

<!-- -->

<div id="--force-destroy-dependents"></div>

**--force-destroy-dependents**

*  On destination, --force and --force-rollback-to-latest-common-snapshot and --delete-* will
    add the '-R' flag to their use of 'zfs rollback' and 'zfs destroy', causing them to
    delete dependents such as clones and bookmarks. This can be very destructive and is rarely
    advisable.

<!-- -->

<div id="--force-unmount"></div>

**--force-unmount**

*  On destination, --force and --force-rollback-to-latest-common-snapshot will add the '-f'
    flag to their use of 'zfs rollback' and 'zfs destroy'.

<!-- -->

<div id="--force-once"></div>

**--force-once**, **--f1**

*  Use the --force option or --force-rollback-to-latest-common-snapshot option at most once to
    resolve a conflict, then abort with an error on any subsequent conflict. This helps to
    interactively resolve conflicts, one conflict at a time.

<!-- -->

<div id="--skip-parent"></div>

**--skip-parent**

*  During replication and deletion, skip processing of the SRC_DATASET and DST_DATASET and only
    process their descendant datasets, i.e. children, and children of children, etc (with
    --recursive). No dataset is processed unless --recursive is also specified. Analogy: `bzfs
    --recursive --skip-parent src dst` is akin to Unix `cp -r src/* dst/` whereas `bzfs
    --recursive --skip-parent --skip-replication --delete-dst-datasets dummy dst` is akin to
    Unix `rm -r dst/*`

<!-- -->

<div id="--skip-missing-snapshots"></div>

**--skip-missing-snapshots** *[{fail,dataset,continue}]*

*  During replication, handle source datasets that select no snapshots (and no relevant
    bookmarks) as follows:

    a) 'fail': Abort with an error.

    b) 'dataset' (default): Skip the source dataset with a warning. Skip descendant datasets if
    --recursive and destination dataset does not exist. Otherwise skip to the next dataset.

    c) 'continue': Skip nothing. If destination snapshots exist, delete them (with --force) or
    abort with an error (without --force). If there is no such abort, continue processing with
    the next dataset. Eventually create empty destination dataset and ancestors if they do not yet
    exist and source dataset has at least one descendant that selects at least one snapshot.

<!-- -->

<div id="--retries"></div>

**--retries** *INT*

*  The maximum number of times a retryable replication or deletion step shall be retried if it
    fails, for example because of network hiccups (default: 2, min: 0). Also consider this option
    if a periodic pruning script may simultaneously delete a dataset or snapshot or bookmark while
    bzfs is running and attempting to access it.

<!-- -->

<div id="--retry-min-sleep-secs"></div>

**--retry-min-sleep-secs** *FLOAT*

*  The minimum duration to sleep between retries (default: 0.125).

<!-- -->

<div id="--retry-max-sleep-secs"></div>

**--retry-max-sleep-secs** *FLOAT*

*  The maximum duration to sleep between retries initially starts with --retry-min-sleep-secs
    (see above), and doubles on each retry, up to the final maximum of --retry-max-sleep-secs
    (default: 300). On each retry a random sleep time in the [--retry-min-sleep-secs, current
    max] range is picked. The timer resets after each operation.

<!-- -->

<div id="--retry-max-elapsed-secs"></div>

**--retry-max-elapsed-secs** *FLOAT*

*  A single operation (e.g. 'zfs send/receive' of the current dataset, or deletion of a list of
    snapshots within the current dataset) will not be retried (or not retried anymore) once this
    much time has elapsed since the initial start of the operation, including retries (default:
    3600). The timer resets after each operation completes or retries exhaust, such that
    subsequently failing operations can again be retried.

<!-- -->

<div id="--skip-on-error"></div>

**--skip-on-error** *{fail,tree,dataset}*

*  During replication and deletion, if an error is not retryable, or --retries has been
    exhausted, or --skip-missing-snapshots raises an error, proceed as follows:

    a) 'fail': Abort the program with an error. This mode is ideal for testing, clear error
    reporting, and situations where consistency trumps availability.

    b) 'tree': Log the error, skip the dataset tree rooted at the dataset for which the error
    occurred, and continue processing the next (sibling) dataset tree. Example: Assume datasets
    tank/user1/foo and tank/user2/bar and an error occurs while processing tank/user1. In this
    case processing skips tank/user1/foo and proceeds with tank/user2.

    c) 'dataset' (default): Same as 'tree' except if the destination dataset already exists,
    skip to the next dataset instead.

    Example: Assume datasets tank/user1/foo and tank/user2/bar and an error occurs while
    processing tank/user1. In this case processing skips tank/user1 and proceeds with
    tank/user1/foo if the destination already contains tank/user1. Otherwise processing continues
    with tank/user2. This mode is for production use cases that require timely forward progress
    even in the presence of partial failures. For example, assume the job is to backup the home
    directories or virtual machines of thousands of users across an organization. Even if
    replication of some of the datasets for some users fails due too conflicts, busy datasets,
    etc, the replication job will continue for the remaining datasets and the remaining users.

<!-- -->

<div id="--skip-replication"></div>

**--skip-replication**

*  Skip replication step (see above) and proceed to the optional --delete-dst-datasets step
    immediately (see below).

<!-- -->

<div id="--delete-dst-datasets"></div>

**--delete-dst-datasets**

*  Do nothing if the --delete-dst-datasets option is missing. Otherwise, after successful
    replication step, if any, delete existing destination datasets that are selected via
    --{include|exclude}-dataset* policy yet do not exist within SRC_DATASET (which can be an
    empty dataset, such as the hardcoded virtual dataset named 'dummy'!). Do not recurse without
    --recursive. With --recursive, never delete non-selected dataset subtrees or their
    ancestors.

    For example, if the destination contains datasets h1,h2,h3,d1 whereas source only contains h3,
    and the include/exclude policy selects h1,h2,h3,d1, then delete datasets h1,h2,d1 on the
    destination to make it 'the same'. On the other hand, if the include/exclude policy only
    selects h1,h2,h3 then only delete datasets h1,h2 on the destination to make it 'the same'.

    Example to delete all tmp datasets within tank2/boo/bar: `bzfs dummy tank2/boo/bar --dryrun
    --skip-replication --recursive --delete-dst-datasets --include-dataset-regex
    '(.*/)?tmp.*' --exclude-dataset-regex '!.*'`

<!-- -->

<div id="--delete-dst-snapshots"></div>

**--delete-dst-snapshots** *[{snapshots,bookmarks}]*

*  Do nothing if the --delete-dst-snapshots option is missing. Otherwise, after successful
    replication, and successful --delete-dst-datasets step, if any, delete existing destination
    snapshots whose GUID does not exist within the source dataset (which can be an empty dummy
    dataset!) if the destination snapshots are selected by the --include/exclude-snapshot-*
    policy, and the destination dataset is selected via --{include|exclude}-dataset* policy.
    Does not recurse without --recursive.

    For example, if the destination dataset contains snapshots h1,h2,h3,d1 (h=hourly, d=daily)
    whereas the source dataset only contains snapshot h3, and the include/exclude policy selects
    h1,h2,h3,d1, then delete snapshots h1,h2,d1 on the destination dataset to make it 'the
    same'. On the other hand, if the include/exclude policy only selects snapshots h1,h2,h3 then
    only delete snapshots h1,h2 on the destination dataset to make it 'the same'.

    *Note:* To delete snapshots regardless, consider using --delete-dst-snapshots in
    combination with a source that is an empty dataset, such as the hardcoded virtual dataset
    named 'dummy', like so: `bzfs dummy tank2/boo/bar --dryrun --skip-replication
    --delete-dst-snapshots --include-snapshot-regex '.*_daily' --recursive`

    *Note:* Use --delete-dst-snapshots=bookmarks to delete bookmarks instead of snapshots, in
    which case no snapshots are selected and the --{include|exclude}-snapshot-* filter options
    treat bookmarks as snapshots wrt. selecting.

    *Performance Note:* --delete-dst-snapshots operates on multiple datasets in parallel (and
    serially within a dataset), using the same dataset order as bzfs replication. The degree of
    parallelism is configurable with the --threads option (see below).

<!-- -->

<div id="--delete-dst-snapshots-no-crosscheck"></div>

**--delete-dst-snapshots-no-crosscheck**

*  This flag indicates that --delete-dst-snapshots=snapshots shall check the source dataset only
    for a snapshot with the same GUID, and ignore whether a bookmark with the same GUID is present
    in the source dataset. Similarly, it also indicates that --delete-dst-snapshots=bookmarks
    shall check the source dataset only for a bookmark with the same GUID, and ignore whether a
    snapshot with the same GUID is present in the source dataset.

<!-- -->

<div id="--delete-dst-snapshots-except"></div>

**--delete-dst-snapshots-except**

*  This flag indicates that the --include/exclude-snapshot-* options shall have inverted
    semantics for the --delete-dst-snapshots option, thus deleting all snapshots except for the
    selected snapshots (within the specified datasets), instead of deleting all selected snapshots
    (within the specified datasets). In other words, this flag enables to specify which snapshots
    to retain instead of which snapshots to delete.

    *Synchronization vs. Backup*: When a real (non-dummy) source dataset is specified in
    combination with --delete-dst-snapshots-except, then any destination snapshot retained by the
    rules above is actually only retained if it also exists in the source dataset - __all other
    destination snapshots are deleted__. This is great for synchronization use cases but should
    __NEVER BE USED FOR LONG-TERM ARCHIVAL__. Long-term archival use cases should instead
    specify the `dummy` source dataset as they require an independent retention policy that is
    not tied to the current contents of the source dataset.

<!-- -->

<div id="--delete-dst-snapshots-except-plan"></div>

**--delete-dst-snapshots-except-plan** *DICT_STRING*

*  Retention periods to be used if pruning snapshots or bookmarks within the selected destination
    datasets via --delete-dst-snapshots. Has the same format as --create-src-snapshots-plan.
    Snapshots (--delete-dst-snapshots=snapshots) or bookmarks (with
    --delete-dst-snapshots=bookmarks) that do not match a period will be deleted. To avoid
    unexpected surprises, make sure to carefully specify ALL snapshot names and periods that shall
    be retained, in combination with --dryrun.

    Example: `"{'prod': {'onsite': {'secondly': 40, 'minutely': 40, 'hourly': 36,
    'daily': 31, 'weekly': 12, 'monthly': 18, 'yearly': 5}, 'us-west-1': {'secondly':
    0, 'minutely': 0, 'hourly': 36, 'daily': 31, 'weekly': 12, 'monthly': 18,
    'yearly': 5}, 'eu-west-1': {'secondly': 0, 'minutely': 0, 'hourly': 36, 'daily':
    31, 'weekly': 12, 'monthly': 18, 'yearly': 5}}, 'test': {'offsite': {'12hourly':
    42, 'weekly': 12}, 'onsite': {'100millisecondly': 42}}}"`. This example will, for the
    organization 'prod' and the intended logical target 'onsite', retain secondly snapshots
    that were created less than 40 seconds ago, yet retain the latest 40 secondly snapshots
    regardless of creation time. Analog for the latest 40 minutely snapshots, latest 36 hourly
    snapshots, etc. It will also retain snapshots for the targets 'us-west-1' and 'eu-west-1'
    within the 'prod' organization. In addition, within the 'test' organization, it will
    retain snapshots that are created every 12 hours and every week as specified, and name them as
    being intended for the 'offsite' replication target. Analog for snapshots that are taken
    every 100 milliseconds within the 'test' organization. All other snapshots within the
    selected datasets will be deleted - you've been warned!

    The example scans the selected ZFS datasets for snapshots with names like
    `prod_onsite_<timestamp>_secondly`, `prod_onsite_<timestamp>_minutely`,
    `prod_us-west-1_<timestamp>_hourly`, `prod_us-west-1_<timestamp>_daily`,
    `prod_eu-west-1_<timestamp>_hourly`, `prod_eu-west-1_<timestamp>_daily`,
    `test_offsite_<timestamp>_12hourly`, `test_offsite_<timestamp>_weekly`, and so on,
    and deletes all snapshots that do not match a retention rule.

    Note: A zero within a period (e.g. 'hourly': 0) indicates that no snapshots shall be
    retained for the given period.

    Note: --delete-dst-snapshots-except-plan is a convenience option that auto-generates a series
    of the following other options: --delete-dst-snapshots-except, --new-snapshot-filter-group,
    --include-snapshot-regex, --include-snapshot-times-and-ranks

<!-- -->

<div id="--delete-empty-dst-datasets"></div>

**--delete-empty-dst-datasets** *[{snapshots,snapshots+bookmarks}]*

*  Do nothing if the --delete-empty-dst-datasets option is missing or --recursive is missing.
    Otherwise, after successful replication step and successful --delete-dst-datasets and
    successful --delete-dst-snapshots steps, if any, delete any selected destination dataset that
    has no snapshot and no bookmark if all descendants of that destination dataset are also
    selected and do not have a snapshot or bookmark either (again, only if the existing
    destination dataset is selected via --{include|exclude}-dataset* policy). Never delete
    non-selected dataset subtrees or their ancestors.

    For example, if the destination contains datasets h1,d1, and the include/exclude policy
    selects h1,d1, then check if h1,d1 can be deleted. On the other hand, if the include/exclude
    policy only selects h1 then only check if h1 can be deleted.

    *Note:* Use --delete-empty-dst-datasets=snapshots to delete snapshot-less datasets even if
    they still contain bookmarks.

<!-- -->

<div id="--monitor-snapshots"></div>

**--monitor-snapshots** *DICT_STRING*

*  Do nothing if the --monitor-snapshots flag is missing. Otherwise, after all other steps,
    alert the user if the ZFS 'creation' time property of the latest snapshot for any specified
    snapshot name pattern within the selected datasets is too old wrt. the specified age limit.
    The purpose is to check if snapshots are successfully taken on schedule, successfully
    replicated on schedule, and successfully pruned on schedule. Process exit code is 0, 1, 2 on
    OK, WARNING, CRITICAL, respectively. Example DICT_STRING: `"{'prod': {'onsite':
    {'100millisecondly': {'latest': {'warning': '300 milliseconds', 'critical': '2
    seconds'}}, 'secondly': {'latest': {'warning': '2 seconds', 'critical': '14
    seconds'}}, 'minutely': {'latest': {'warning': '30 seconds', 'critical': '300
    seconds'}}, 'hourly': {'latest': {'warning': '30 minutes', 'critical': '300
    minutes'}}, 'daily': {'latest': {'warning': '4 hours', 'critical': '8 hours'}},
    'weekly': {'latest': {'warning': '2 days', 'critical': '8 days'}}, 'monthly':
    {'latest': {'warning': '2 days', 'critical': '8 days'}}, 'yearly': {'latest':
    {'warning': '5 days', 'critical': '14 days'}}, '10minutely': {'latest':
    {'warning': '0 minutes', 'critical': '0 minutes'}}}, '': {'daily': {'latest':
    {'warning': '4 hours', 'critical': '8 hours'}}}}}"`. This example alerts the user if
    the latest src or dst snapshot named `prod_onsite_<timestamp>_hourly` is more than 30
    minutes late (i.e. more than 30+60=90 minutes old) [warning] or more than 300 minutes late
    (i.e. more than 300+60=360 minutes old) [critical]. Analog for the latest snapshot named
    `prod_<timestamp>_daily`, and so on.

    Note: A duration that is missing or zero (e.g. '0 minutes') indicates that no snapshots
    shall be checked for the given snapshot name pattern.

<!-- -->

<div id="--monitor-snapshots-dont-warn"></div>

**--monitor-snapshots-dont-warn**

*  Log a message for monitoring warnings but nonetheless exit with zero exit code.

<!-- -->

<div id="--monitor-snapshots-dont-crit"></div>

**--monitor-snapshots-dont-crit**

*  Log a message for monitoring criticals but nonetheless exit with zero exit code.

<!-- -->

<div id="--compare-snapshot-lists"></div>

**--compare-snapshot-lists** *[{src,dst,all,src+dst,src+all,dst+all,src+dst+all}]*

*  Do nothing if the --compare-snapshot-lists option is missing. Otherwise, after successful
    replication step and successful --delete-dst-datasets, --delete-dst-snapshots steps and
    --delete-empty-dst-datasets steps, if any, proceed as follows:

    Compare source and destination dataset trees recursively wrt. snapshots, for example to check
    if all recently taken snapshots have been successfully replicated by a periodic job.

    Example: List snapshots only contained in source (tagged with 'src'), only contained in
    destination (tagged with 'dst'), and contained in both source and destination (tagged with
    'all'), restricted to hourly and daily snapshots taken within the last 7 days, excluding the
    last 4 hours (to allow for some slack/stragglers), excluding temporary datasets: `bzfs
    tank1/foo/bar tank2/boo/bar --skip-replication --compare-snapshot-lists=src+dst+all
    --recursive --include-snapshot-regex '.*_(hourly|daily)'
    --include-snapshot-times-and-ranks '7 days ago..4 hours ago' --exclude-dataset-regex
    'tmp.*'`

    This outputs a TSV file containing the following columns:

    `location creation_iso createtxg rel_name guid root_dataset rel_dataset name creation
    written`

    Example output row:

    `src 2024-11-06_08:30:05 17435050 /foo@test_2024-11-06_08:30:05_daily 2406491805272097867
    tank1/src /foo tank1/src/foo@test_2024-10-06_08:30:04_daily 1730878205 24576`

    If the TSV output file contains zero lines starting with the prefix 'src' and zero lines
    starting with the prefix 'dst' then no source snapshots are missing on the destination, and
    no destination snapshots are missing on the source, indicating that the periodic replication
    and pruning jobs perform as expected. The TSV output is sorted by rel_dataset, and by ZFS
    creation time within each rel_dataset - the first and last line prefixed with 'all' contains
    the metadata of the oldest and latest common snapshot, respectively. Third party tools can use
    this info for post-processing, for example using custom scripts using 'csplit' or duckdb
    analytics queries.

    The --compare-snapshot-lists option also directly logs various summary stats, such as the
    metadata of the latest common snapshot, latest snapshots and oldest snapshots, as well as the
    time diff between the latest common snapshot and latest snapshot only in src (and only in
    dst), as well as how many src snapshots and how many GB of data are missing on dst, etc.

    *Note*: Consider omitting the 'all' flag to reduce noise and instead focus on missing
    snapshots only, like so: --compare-snapshot-lists=src+dst

    *Note*: The source can also be an empty dataset, such as the hardcoded virtual dataset named
    'dummy'.

    *Note*: --compare-snapshot-lists is typically *much* faster than standard 'zfs list -t
    snapshot' CLI usage because the former issues requests with a higher degree of parallelism
    than the latter. The degree is configurable with the --threads option (see below).

<!-- -->

<div id="--cache-snapshots"></div>

**--cache-snapshots** *[{true,false}]*

*  Default is 'false'. If 'true', maintain a persistent local cache of recent snapshot
    creation times, recent successful replication times, and recent monitoring times, and compare
    them to a quick 'zfs list -t filesystem,volume -p -o snapshots_changed' to help determine if
    a new snapshot shall be created on the src, and if there are any changes that need to be
    replicated or monitored. Enabling the cache improves performance if --create-src-snapshots
    and/or replication and/or --monitor-snapshots is invoked frequently (e.g. every minute via
    cron) over a large number of datasets, with each dataset containing a large number of
    snapshots, yet it is seldom for a new src snapshot to actually be created, or there are seldom
    any changes to replicate or monitor (e.g. a snapshot is only created every day and/or deleted
    every day).

    *Note:* This flag only has an effect on OpenZFS >= 2.2.

    *Note:* This flag is only relevant for snapshot creation on the src if
    --create-src-snapshots-even-if-not-due is not specified.

<!-- -->

<div id="--dryrun"></div>

**--dryrun** *[{recv,send}]*, **-n** *[{recv,send}]*

*  Do a dry run (aka 'no-op') to print what operations would happen if the command were to be
    executed for real (optional). This option treats both the ZFS source and destination as
    read-only. Accepts an optional argument for fine tuning that is handled as follows:

    a) 'recv': Send snapshot data via 'zfs send' to the destination host and receive it there
    via 'zfs receive -n', which discards the received data there.

    b) 'send': Do not execute 'zfs send' and do not execute 'zfs receive'. This is a less
    'realistic' form of dry run, but much faster, especially for large snapshots and slow
    networks/disks, as no snapshot is actually transferred between source and destination. This is
    the default when specifying --dryrun.

    Examples: --dryrun, --dryrun=send, --dryrun=recv

<!-- -->

<div id="--verbose"></div>

**--verbose**, **-v**

*  Print verbose information. This option can be specified multiple times to increase the level
    of verbosity. To print what ZFS/SSH operation exactly is happening (or would happen), add the
    `-v -v -v` flag, maybe along with --dryrun. All ZFS and SSH commands (even with --dryrun)
    are logged such that they can be inspected, copy-and-pasted into a terminal shell and run
    manually to help anticipate or diagnose issues. ERROR, WARN, INFO, DEBUG, TRACE output lines
    are identified by [E], [W], [I], [D], [T] prefixes, respectively.

<!-- -->

<div id="--quiet"></div>

**--quiet**, **-q**

*  Suppress non-error, info, debug, and trace output.

<!-- -->

<div id="--no-privilege-elevation"></div>

**--no-privilege-elevation**, **-p**

*  Do not attempt to run state changing ZFS operations 'zfs
    create/rollback/destroy/send/receive/snapshot' as root (via 'sudo -u root' elevation
    granted by administrators appending the following to /etc/sudoers: `<NON_ROOT_USER_NAME>
    ALL=NOPASSWD:/path/to/zfs`

    Instead, the --no-privilege-elevation flag is for non-root users that have been granted
    corresponding ZFS permissions by administrators via 'zfs allow' delegation mechanism, like
    so: sudo zfs allow -u $SRC_NON_ROOT_USER_NAME snapshot,destroy,send,bookmark,hold
    $SRC_DATASET; sudo zfs allow -u $DST_NON_ROOT_USER_NAME
    mount,create,receive,rollback,destroy,canmount,mountpoint,readonly,compression,encryption,keylocation,recordsize
    $DST_DATASET_OR_POOL.

    For extra security $SRC_NON_ROOT_USER_NAME should be different than $DST_NON_ROOT_USER_NAME,
    i.e. the sending Unix user on the source and the receiving Unix user at the destination should
    be separate Unix user accounts with separate private keys even if both accounts reside on the
    same machine, per the principle of least privilege. Further, if you do not plan to use the
    --force* flags and --delete-* CLI options then ZFS permissions 'rollback,destroy' can be
    omitted. If you do not plan to customize the respective ZFS dataset property then ZFS
    permissions 'canmount,mountpoint,readonly,compression,encryption,keylocation,recordsize' can
    be omitted, arriving at the absolutely minimal set of required destination permissions:
    `mount,create,receive`.

    Also see https://openzfs.github.io/openzfs-docs/man/master/8/zfs-allow.8.html#EXAMPLES and
    https://tinyurl.com/9h97kh8n and https://youtu.be/o_jr13Z9f1k?si=7shzmIQJpzNJV6cq

<!-- -->

<div id="--no-stream"></div>

**--no-stream**

*  During replication, only replicate the most recent selected source snapshot of a dataset
    (using -i incrementals instead of -I incrementals), hence skip all intermediate source
    snapshots that may exist between that and the most recent common snapshot. If there is no
    common snapshot also skip all other source snapshots for the dataset, except for the most
    recent selected source snapshot. This option helps the destination to 'catch up' with the
    source ASAP, consuming a minimum of disk space, at the expense of reducing reliable options
    for rolling back to intermediate snapshots in the future.

<!-- -->

<div id="--no-resume-recv"></div>

**--no-resume-recv**

*  Replication of snapshots via 'zfs send/receive' can be interrupted by intermittent network
    hiccups, reboots, hardware issues, etc. Interrupted 'zfs send/receive' operations are
    retried if the --retries and --retry-* options enable it (see above). In normal operation
    bzfs automatically retries such that only the portion of the snapshot is transmitted that has
    not yet been fully received on the destination. For example, this helps to progressively
    transfer a large individual snapshot over a wireless network in a timely manner despite
    frequent intermittent network hiccups. This optimization is called 'resume receive' and uses
    the 'zfs receive -s' and 'zfs send -t' feature.

    The --no-resume-recv option disables this optimization such that a retry now retransmits the
    entire snapshot from scratch, which could slow down or even prohibit progress in case of
    frequent network hiccups. bzfs automatically falls back to using the --no-resume-recv option
    if it is auto-detected that the ZFS pool does not reliably support the 'resume receive'
    optimization.

    *Note:* Snapshots that have already been fully transferred as part of the current 'zfs
    send/receive' operation need not be retransmitted regardless of the --no-resume-recv flag.
    For example, assume a single 'zfs send/receive' operation is transferring incremental
    snapshots 1 through 10 via 'zfs send -I', but the operation fails while transferring
    snapshot 10, then snapshots 1 through 9 need not be retransmitted regardless of the
    --no-resume-recv flag, as these snapshots have already been successfully received at the
    destination either way.

<!-- -->

<div id="--create-bookmarks"></div>

**--create-bookmarks** *{all,many,none}*

*  For increased safety, bzfs replication behaves as follows wrt. ZFS bookmark creation, if it is
    autodetected that the source ZFS pool support bookmarks:

    * `many` (default): Whenever it has successfully completed replication of the most recent
    source snapshot, bzfs creates a ZFS bookmark of that snapshot, and attaches it to the source
    dataset. In addition, whenever it has successfully completed a 'zfs send' operation, bzfs
    creates a ZFS bookmark of each hourly, daily, weekly, monthly and yearly source snapshot that
    was sent during that 'zfs send' operation, and attaches it to the source dataset.

    * `all`: Whenever it has successfully completed a 'zfs send' operation, bzfs creates a
    ZFS bookmark of each source snapshot that was sent during that 'zfs send' operation, and
    attaches it to the source dataset. This increases safety at the expense of some performance.

    * `none`: No bookmark is created.

    Bookmarks exist so an incremental stream can continue to be sent from the source dataset
    without having to keep the already replicated snapshot around on the source dataset until the
    next upcoming snapshot has been successfully replicated. This way you can send the snapshot
    from the source dataset to another host, then bookmark the snapshot on the source dataset,
    then delete the snapshot from the source dataset to save disk space, and then still
    incrementally send the next upcoming snapshot from the source dataset to the other host by
    referring to the bookmark.

    The --create-bookmarks=none option disables this safety feature but is discouraged, because
    bookmarks are tiny and relatively cheap and help to ensure that ZFS replication can continue
    even if source and destination dataset somehow have no common snapshot anymore. For example,
    if a pruning script has accidentally deleted too many (or even all) snapshots on the source
    dataset in an effort to reclaim disk space, replication can still proceed because it can use
    the info in the bookmark (the bookmark must still exist in the source dataset) instead of the
    info in the metadata of the (now missing) source snapshot.

    A ZFS bookmark is a tiny bit of metadata extracted from a ZFS snapshot by the 'zfs bookmark'
    CLI, and attached to a dataset, much like a ZFS snapshot. Note that a ZFS bookmark does not
    contain user data; instead a ZFS bookmark is essentially a tiny pointer in the form of the
    GUID of the snapshot and 64-bit transaction group number of the snapshot and creation time of
    the snapshot, which is sufficient to tell the destination ZFS pool how to find the destination
    snapshot corresponding to the source bookmark and (potentially already deleted) source
    snapshot. A bookmark can be fed into 'zfs send' as the source of an incremental send. Note
    that while a bookmark allows for its snapshot to be deleted on the source after successful
    replication, it still requires that its snapshot is not somehow deleted prematurely on the
    destination dataset, so be mindful of that. By convention, a bookmark created by bzfs has the
    same name as its corresponding snapshot, the only difference being the leading '#' separator
    instead of the leading '@' separator. Also see
    https://www.youtube.com/watch?v=LaNgoAZeTww&t=316s.

    You can list bookmarks, like so: `zfs list -t bookmark -o name,guid,createtxg,creation -d 1
    $SRC_DATASET`, and you can (and should) periodically prune obsolete bookmarks just like
    snapshots, like so: `zfs destroy $SRC_DATASET#$BOOKMARK`. Typically, bookmarks should be
    pruned less aggressively than snapshots, and destination snapshots should be pruned less
    aggressively than source snapshots. As an example starting point, here is a command that
    deletes all bookmarks older than 90 days, but retains the latest 200 bookmarks (per dataset)
    regardless of creation time: `bzfs dummy tank2/boo/bar --dryrun --recursive
    --skip-replication --delete-dst-snapshots=bookmarks --include-snapshot-times-and-ranks
    notime 'all except latest 200' --include-snapshot-times-and-ranks 'anytime..90 days
    ago'`

<!-- -->

<div id="--no-use-bookmark"></div>

**--no-use-bookmark**

*  For increased safety, in normal replication operation bzfs replication also looks for
    bookmarks (in addition to snapshots) on the source dataset in order to find the most recent
    common snapshot wrt. the destination dataset, if it is auto-detected that the source ZFS pool
    support bookmarks. The --no-use-bookmark option disables this safety feature but is
    discouraged, because bookmarks help to ensure that ZFS replication can continue even if source
    and destination dataset somehow have no common snapshot anymore.

    Note that it does not matter whether a bookmark was created by bzfs or a third party script,
    as only the GUID of the bookmark and the GUID of the snapshot is considered for comparison,
    and ZFS guarantees that any bookmark of a given snapshot automatically has the same GUID,
    transaction group number and creation time as the snapshot. Also note that you can create,
    delete and prune bookmarks any way you like, as bzfs (without --no-use-bookmark) will happily
    work with whatever bookmarks currently exist, if any.

<!-- -->

<div id="--ssh-src-user"></div>

**--ssh-src-user** *STRING*

*  Remote SSH username on src host to connect to (optional). Overrides username given in
    SRC_DATASET.

<!-- -->

<div id="--ssh-dst-user"></div>

**--ssh-dst-user** *STRING*

*  Remote SSH username on dst host to connect to (optional). Overrides username given in
    DST_DATASET.

<!-- -->

<div id="--ssh-src-host"></div>

**--ssh-src-host** *STRING*

*  Remote SSH hostname of src host to connect to (optional). Can also be an IPv4 or IPv6 address.
    Overrides hostname given in SRC_DATASET.

<!-- -->

<div id="--ssh-dst-host"></div>

**--ssh-dst-host** *STRING*

*  Remote SSH hostname of dst host to connect to (optional). Can also be an IPv4 or IPv6 address.
    Overrides hostname given in DST_DATASET.

<!-- -->

<div id="--ssh-src-port"></div>

**--ssh-src-port** *INT*

*  Remote SSH port on src host to connect to (optional).

<!-- -->

<div id="--ssh-dst-port"></div>

**--ssh-dst-port** *INT*

*  Remote SSH port on dst host to connect to (optional).

<!-- -->

<div id="--ssh-src-config-file"></div>

**--ssh-src-config-file** *FILE*

*  Path to SSH ssh_config(5) file to connect to src (optional); will be passed into ssh -F CLI.
    The basename must contain the substring 'bzfs_ssh_config'.

<!-- -->

<div id="--ssh-dst-config-file"></div>

**--ssh-dst-config-file** *FILE*

*  Path to SSH ssh_config(5) file to connect to dst (optional); will be passed into ssh -F CLI.
    The basename must contain the substring 'bzfs_ssh_config'.

<!-- -->

<div id="--threads"></div>

**--threads** *INT[%]*

*  The maximum number of threads to use for parallel operations; can be given as a positive
    integer, optionally followed by the % percent character (min: 1, default: 100%). Percentages
    are relative to the number of CPU cores on the machine. Example: 200% uses twice as many
    threads as there are cores on the machine; 75% uses num_threads = num_cores * 0.75. Currently
    this option only applies to dataset and snapshot replication, --create-src-snapshots,
    --delete-dst-snapshots, --delete-empty-dst-datasets, --monitor-snapshots and
    --compare-snapshot-lists. The ideal value for this parameter depends on the use case and its
    performance requirements, as well as the number of available CPU cores and the parallelism
    offered by SSDs vs. HDDs, ZFS topology and configuration, as well as the network bandwidth and
    other workloads simultaneously running on the system. The current default is geared towards a
    high degreee of parallelism, and as such may perform poorly on HDDs. Examples: 1, 4, 75%, 150%

<!-- -->

<div id="--max-concurrent-ssh-sessions-per-tcp-connection"></div>

**--max-concurrent-ssh-sessions-per-tcp-connection** *INT*

*  For best throughput, bzfs uses multiple SSH TCP connections in parallel, as indicated by
    --threads (see above). For best startup latency, each such parallel TCP connection can carry
    a maximum of S concurrent SSH sessions, where
    S=--max-concurrent-ssh-sessions-per-tcp-connection (default: 8, min: 1). Concurrent SSH
    sessions are mostly used for metadata operations such as listing ZFS datasets and their
    snapshots. This client-side max sessions parameter must not be higher than the server-side
    sshd_config(5) MaxSessions parameter (which defaults to 10, see
    https://manpages.ubuntu.com/manpages/man5/sshd_config.5.html).

    *Note:* For better throughput, bzfs uses one dedicated TCP connection per ZFS send/receive
    operation such that the dedicated connection is never used by any other concurrent SSH
    session, effectively ignoring the value of the
    --max-concurrent-ssh-sessions-per-tcp-connection parameter in the ZFS send/receive case.

<!-- -->

<div id="--bwlimit"></div>

**--bwlimit** *STRING*

*  Sets 'pv' bandwidth rate limit for zfs send/receive data transfer (optional). Example:
    `100m` to cap throughput at 100 MB/sec. Default is unlimited. Also see
    https://manpages.ubuntu.com/manpages/man1/pv.1.html

<!-- -->

<div id="--compression-program"></div>

**--compression-program** *{zstd,lz4,pzstd,pigz,gzip,bzip2,-}*

*  The name of the 'zstd' executable (optional). Default is 'zstd'. Use '-' to disable the
    use of this program. The use is auto-disabled if data is transferred locally instead of via
    the network. This option is about transparent compression-on-the-wire, not about
    compression-at-rest.

<!-- -->

<div id="--compression-program-opts"></div>

**--compression-program-opts** *STRING*

*  The options to be passed to the compression program on the compression step (optional).
    Default is '-1' (fastest).

<!-- -->

<div id="--mbuffer-program"></div>

**--mbuffer-program** *{mbuffer,-}*

*  The name of the 'mbuffer' executable (optional). Default is 'mbuffer'. Use '-' to
    disable the use of this program. The use is auto-disabled if data is transferred locally
    instead of via the network. This tool is used to smooth out the rate of data flow and prevent
    bottlenecks caused by network latency or speed fluctuation.

<!-- -->

<div id="--mbuffer-program-opts"></div>

**--mbuffer-program-opts** *STRING*

*  Options to be passed to 'mbuffer' program (optional). Default: '-q -m 128M'.

<!-- -->

<div id="--ps-program"></div>

**--ps-program** *{ps,-}*

*  The name of the 'ps' executable (optional). Default is 'ps'. Use '-' to disable the use
    of this program.

<!-- -->

<div id="--pv-program"></div>

**--pv-program** *{pv,-}*

*  The name of the 'pv' executable (optional). Default is 'pv'. Use '-' to disable the use
    of this program. This is used for bandwidth rate-limiting and progress monitoring.

<!-- -->

<div id="--pv-program-opts"></div>

**--pv-program-opts** *STRING*

*  The options to be passed to the 'pv' program (optional). Default: '--progress --timer
    --eta --fineta --rate --average-rate --bytes --interval=1 --width=120
    --buffer-size=2M'.

<!-- -->

<div id="--shell-program"></div>

**--shell-program** *{sh,-}*

*  The name of the 'sh' executable (optional). Default is 'sh'. Use '-' to disable the use
    of this program.

<!-- -->

<div id="--ssh-program"></div>

**--ssh-program** *{ssh,hpnssh,-}*

*  The name of the 'ssh' executable (optional). Default is 'ssh'. Use '-' to disable the
    use of this program.

<!-- -->

<div id="--sudo-program"></div>

**--sudo-program** *{sudo,-}*

*  The name of the 'sudo' executable (optional). Default is 'sudo'. Use '-' to disable the
    use of this program.

<!-- -->

<div id="--zpool-program"></div>

**--zpool-program** *{zpool,-}*

*  The name of the 'zpool' executable (optional). Default is 'zpool'. Use '-' to disable
    the use of this program.

<!-- -->

<div id="--log-dir"></div>

**--log-dir** *DIR*

*  Path to the log output directory on local host (optional). Default: $HOME/bzfs-logs. The
    logger that is used by default writes log files there, in addition to the console. The
    basename of --log-dir must contain the substring 'bzfs-logs' as this helps prevent
    accidents. The current.dir symlink always points to the subdirectory containing the most
    recent log file. The current.log symlink always points to the most recent log file. The
    current.pv symlink always points to the most recent data transfer monitoring log. Run `tail
    --follow=name --max-unchanged-stats=1` on both symlinks to follow what's currently going
    on. Parallel replication generates a separate .pv file per thread. To monitor these, run
    something like `while true; do clear; for f in $(realpath
    $HOME/bzfs-logs/current/current.pv)*; do tac -s $(printf '\r') $f | tr '\r' '\n'
    | grep -m1 -v '^$'; done; sleep 1; done`

<!-- -->

<div id="--log-file-prefix"></div>

**--log-file-prefix** *STRING*

*  Default is zrun_. The path name of the log file on local host is
    `${--log-dir}/${--log-file-prefix}<timestamp>${--log-file-infix}${--log-file-suffix}-<random>.log`.
    Example: `--log-file-prefix=zrun_us-west-1_ --log-file-suffix=_daily` will generate log
    file names such as `zrun_us-west-1_2024-09-03_12:26:15_daily-bl4i1fth.log`

<!-- -->

<div id="--log-file-infix"></div>

**--log-file-infix** *STRING*

*  Default is the empty string. The path name of the log file on local host is
    `${--log-dir}/${--log-file-prefix}<timestamp>${--log-file-infix}${--log-file-suffix}-<random>.log`.
    Example: `--log-file-prefix=zrun_us-west-1_ --log-file-suffix=_daily` will generate log
    file names such as `zrun_us-west-1_2024-09-03_12:26:15_daily-bl4i1fth.log`

<!-- -->

<div id="--log-file-suffix"></div>

**--log-file-suffix** *STRING*

*  Default is the empty string. The path name of the log file on local host is
    `${--log-dir}/${--log-file-prefix}<timestamp>${--log-file-infix}${--log-file-suffix}-<random>.log`.
    Example: `--log-file-prefix=zrun_us-west-1_ --log-file-suffix=_daily` will generate log
    file names such as `zrun_us-west-1_2024-09-03_12:26:15_daily-bl4i1fth.log`

<!-- -->

<div id="--log-subdir"></div>

**--log-subdir** *{daily,hourly,minutely}*

*  Make a new subdirectory in --log-dir every day, hour or minute; write log files there.
    Default is 'daily'.

<!-- -->

<div id="--log-syslog-address"></div>

**--log-syslog-address** *STRING*

*  Host:port of the syslog machine to send messages to (e.g. 'foo.example.com:514' or
    '127.0.0.1:514'), or the file system path to the syslog socket file on localhost (e.g.
    '/dev/log'). The default is no address, i.e. do not log anything to syslog by default. See
    https://docs.python.org/3/library/logging.handlers.html#sysloghandler

<!-- -->

<div id="--log-syslog-socktype"></div>

**--log-syslog-socktype** *{UDP,TCP}*

*  The socket type to use to connect if no local socket file system path is used. Default is
    'UDP'.

<!-- -->

<div id="--log-syslog-facility"></div>

**--log-syslog-facility** *INT*

*  The local facility aka category that identifies msg sources in syslog (default: 1, min=0,
    max=7).

<!-- -->

<div id="--log-syslog-prefix"></div>

**--log-syslog-prefix** *STRING*

*  The name to prepend to each message that is sent to syslog; identifies bzfs messages as
    opposed to messages from other sources. Default is 'bzfs'.

<!-- -->

<div id="--log-syslog-level"></div>

**--log-syslog-level** *{CRITICAL,ERROR,WARN,INFO,DEBUG,TRACE}*

*  Only send messages with equal or higher priority than this log level to syslog. Default is
    'ERROR'.

<!-- -->

<div id="--log-config-file"></div>

**--log-config-file** *STRING*

*  The contents of a JSON file that defines a custom python logging configuration to be used
    (optional). If the option starts with a `+` prefix then the contents are read from the UTF-8
    JSON file given after the `+` prefix. Examples: +log_config.json, +/path/to/log_config.json.
    Here is an example config file that demonstrates usage:
    https://github.com/whoschek/bzfs/blob/main/bzfs_tests/log_config.json

    For more examples see
    https://stackoverflow.com/questions/7507825/where-is-a-complete-example-of-logging-config-dictconfig
    and for details see
    https://docs.python.org/3/library/logging.config.html#configuration-dictionary-schema

    *Note:* Lines starting with a # character are ignored as comments within the JSON. Also, if
    a line ends with a # character the portion between that # character and the preceding #
    character on the same line is ignored as a comment.

<!-- -->

<div id="--log-config-var"></div>

**--log-config-var** *NAME:VALUE [NAME:VALUE ...]*

*  User defined variables in the form of zero or more NAME:VALUE pairs (optional). These
    variables can be used within the JSON passed with --log-config-file (see above) via
    `${name[:default]}` references, which are substituted (aka interpolated) as follows:

    If the variable contains a non-empty CLI value then that value is used. Else if a default
    value for the variable exists in the JSON file that default value is used. Else the program
    aborts with an error. Example: In the JSON variable `${syslog_address:/dev/log}`, the
    variable name is 'syslog_address' and the default value is '/dev/log'. The default value
    is the portion after the optional : colon within the variable declaration. The default value
    is used if the CLI user does not specify a non-empty value via --log-config-var, for example
    via --log-config-var syslog_address:/path/to/socket_file or via --log-config-var
    syslog_address:[host,port].

    bzfs automatically supplies the following convenience variables: `${bzfs.log_level}`,
    `${bzfs.log_dir}`, `${bzfs.log_file}`, `${bzfs.sub.logger}`,
    `${bzfs.get_default_log_formatter}`, `${bzfs.timestamp}`. For a complete list see the
    source code of get_dict_config_logger().

<!-- -->

<div id="--include-envvar-regex"></div>

**--include-envvar-regex** *REGEX [REGEX ...]*

*  On program startup, unset all Unix environment variables for which the full environment
    variable name matches at least one of the excludes but none of the includes. If an environment
    variable is included this decision is never reconsidered because include takes precedence over
    exclude. The purpose is to tighten security and help guard against accidental inheritance or
    malicious injection of environment variable values that may have unintended effects.

    This option can be specified multiple times. A leading `!` character indicates logical
    negation, i.e. the regex matches if the regex with the leading `!` character removed does
    not match. The default is to include no environment variables, i.e. to make no exceptions to
    --exclude-envvar-regex. Example that retains at least these two env vars:
    `--include-envvar-regex PATH --include-envvar-regex bzfs_min_pipe_transfer_size`. Example
    that retains all environment variables without tightened security: `'.*'`

<!-- -->

<div id="--exclude-envvar-regex"></div>

**--exclude-envvar-regex** *REGEX [REGEX ...]*

*  Same syntax as --include-envvar-regex (see above) except that the default is to exclude no
    environment variables. Example: `bzfs_.*`

<!-- -->

<div id="--version"></div>

**--version**

*  Display version information and exit.

<!-- -->

<div id="--help,_-h"></div>

**--help, -h**

*  Show this help message and exit.

# YEARLY PERIOD ANCHORS

Use these options to customize when snapshots that happen every N years are scheduled to be
created on the source by the --create-src-snapshots option.

<div id="--yearly_year"></div>

**--yearly_year** *INT*

*  The anchor year of multi-year periods (1 ≤ x ≤ 9999, default: 2025).

<!-- -->

<div id="--yearly_month"></div>

**--yearly_month** *INT*

*  The month within a year (1 ≤ x ≤ 12, default: 1).

<!-- -->

<div id="--yearly_monthday"></div>

**--yearly_monthday** *INT*

*  The day within a month (1 ≤ x ≤ 31, default: 1).

<!-- -->

<div id="--yearly_hour"></div>

**--yearly_hour** *INT*

*  The hour within a day (0 ≤ x ≤ 23, default: 0).

<!-- -->

<div id="--yearly_minute"></div>

**--yearly_minute** *INT*

*  The minute within an hour (0 ≤ x ≤ 59, default: 0).

<!-- -->

<div id="--yearly_second"></div>

**--yearly_second** *INT*

*  The second within a minute (0 ≤ x ≤ 59, default: 0).

# MONTHLY PERIOD ANCHORS

Use these options to customize when snapshots that happen every N months are scheduled to be
created on the source by the --create-src-snapshots option.

<div id="--monthly_month"></div>

**--monthly_month** *INT*

*  The anchor month of multi-month periods (1 ≤ x ≤ 12, default: 1).

<!-- -->

<div id="--monthly_monthday"></div>

**--monthly_monthday** *INT*

*  The day within a month (1 ≤ x ≤ 31, default: 1).

<!-- -->

<div id="--monthly_hour"></div>

**--monthly_hour** *INT*

*  The hour within a day (0 ≤ x ≤ 23, default: 0).

<!-- -->

<div id="--monthly_minute"></div>

**--monthly_minute** *INT*

*  The minute within an hour (0 ≤ x ≤ 59, default: 0).

<!-- -->

<div id="--monthly_second"></div>

**--monthly_second** *INT*

*  The second within a minute (0 ≤ x ≤ 59, default: 0).

# WEEKLY PERIOD ANCHORS

Use these options to customize when snapshots that happen every N weeks are scheduled to be
created on the source by the --create-src-snapshots option.

<div id="--weekly_weekday"></div>

**--weekly_weekday** *INT*

*  The weekday within a week: 0=Sunday, 1=Monday, ..., 6=Saturday (0 ≤ x ≤ 6, default: 0).

<!-- -->

<div id="--weekly_hour"></div>

**--weekly_hour** *INT*

*  The hour within a day (0 ≤ x ≤ 23, default: 0).

<!-- -->

<div id="--weekly_minute"></div>

**--weekly_minute** *INT*

*  The minute within an hour (0 ≤ x ≤ 59, default: 0).

<!-- -->

<div id="--weekly_second"></div>

**--weekly_second** *INT*

*  The second within a minute (0 ≤ x ≤ 59, default: 0).

# DAILY PERIOD ANCHORS

Use these options to customize when snapshots that happen every N days are scheduled to be created
on the source by the --create-src-snapshots option.

<div id="--daily_hour"></div>

**--daily_hour** *INT*

*  The hour within a day (0 ≤ x ≤ 23, default: 0).

<!-- -->

<div id="--daily_minute"></div>

**--daily_minute** *INT*

*  The minute within an hour (0 ≤ x ≤ 59, default: 0).

<!-- -->

<div id="--daily_second"></div>

**--daily_second** *INT*

*  The second within a minute (0 ≤ x ≤ 59, default: 0).

# HOURLY PERIOD ANCHORS

Use these options to customize when snapshots that happen every N hours are scheduled to be
created on the source by the --create-src-snapshots option.

<div id="--hourly_minute"></div>

**--hourly_minute** *INT*

*  The minute within an hour (0 ≤ x ≤ 59, default: 0).

<!-- -->

<div id="--hourly_second"></div>

**--hourly_second** *INT*

*  The second within a minute (0 ≤ x ≤ 59, default: 0).

# MINUTELY PERIOD ANCHORS

Use these options to customize when snapshots that happen every N minutes are scheduled to be
created on the source by the --create-src-snapshots option.

<div id="--minutely_second"></div>

**--minutely_second** *INT*

*  The second within a minute (0 ≤ x ≤ 59, default: 0).

# SECONDLY PERIOD ANCHORS

Use these options to customize when snapshots that happen every N seconds are scheduled to be
created on the source by the --create-src-snapshots option.

<div id="--secondly_millisecond"></div>

**--secondly_millisecond** *INT*

*  The millisecond within a second (0 ≤ x ≤ 999, default: 0).

# MILLISECONDLY PERIOD ANCHORS

Use these options to customize when snapshots that happen every N milliseconds are scheduled to be
created on the source by the --create-src-snapshots option.

<div id="--millisecondly_microsecond"></div>

**--millisecondly_microsecond** *INT*

*  The microsecond within a millisecond (0 ≤ x ≤ 999, default: 0).

# ZFS-RECV-O (EXPERIMENTAL)

The following group of parameters specifies additional zfs receive '-o' options that can be used
to configure the copying of ZFS dataset properties from the source dataset to its corresponding
destination dataset. The 'zfs-recv-o' group of parameters is applied before the 'zfs-recv-x'
group.

<div id="--zfs-recv-o-targets"></div>

**--zfs-recv-o-targets** *{full,incremental,full+incremental}*

*  The zfs send phase or phases during which the extra '-o' options are passed to 'zfs
    receive'. This can be one of the following choices: 'full', 'incremental',
    'full+incremental'. Default is 'full+incremental'. A 'full' send is sometimes also known
    as an 'initial' send.

<!-- -->

<div id="--zfs-recv-o-sources"></div>

**--zfs-recv-o-sources** *STRING*

*  The ZFS sources to provide to the 'zfs get -s' CLI in order to fetch the ZFS dataset
    properties that will be fed into the --zfs-recv-o-include/exclude-regex filter (see below).
    The sources are in the form of a comma-separated list (no spaces) containing one or more of
    the following choices: 'local', 'default', 'inherited', 'temporary', 'received',
    'none', with the default being 'local'. Uses 'zfs get -p -s $zfs-recv-o-sources all
    $SRC_DATASET' to fetch the properties to copy -
    https://openzfs.github.io/openzfs-docs/man/master/8/zfs-get.8.html. P.S: Note that the
    existing 'zfs send --props' option does not filter and that --props only reads properties
    from the 'local' ZFS property source (https://github.com/openzfs/zfs/issues/13024).

<!-- -->

<div id="--zfs-recv-o-include-regex"></div>

**--zfs-recv-o-include-regex** *REGEX [REGEX ...]*

*  Take the output properties of --zfs-recv-o-sources (see above) and filter them such that we
    only retain the properties whose name matches at least one of the --include regexes but none
    of the --exclude regexes. If a property is excluded this decision is never reconsidered
    because exclude takes precedence over include. Append each retained property to the list of
    '-o' options in --zfs-recv-program-opt(s), unless another '-o' or '-x' option with the
    same name already exists therein. In other words, --zfs-recv-program-opt(s) takes precedence.

    The --zfs-recv-o-include-regex option can be specified multiple times. A leading `!`
    character indicates logical negation, i.e. the regex matches if the regex with the leading
    `!` character removed does not match. If the option starts with a `+` prefix then regexes
    are read from the newline-separated UTF-8 text file given after the `+` prefix, one regex
    per line inside of the text file.

    The default is to include no properties, thus by default no extra '-o' option is appended.
    Example: `--zfs-recv-o-include-regex recordsize volblocksize`. More examples: `.*`
    (include all properties), `foo bar myapp:.*` (include three regexes)
    `+zfs-recv-o_regexes.txt`, `+/path/to/zfs-recv-o_regexes.txt`

<!-- -->

<div id="--zfs-recv-o-exclude-regex"></div>

**--zfs-recv-o-exclude-regex** *REGEX [REGEX ...]*

*  Same syntax as --zfs-recv-o-include-regex (see above), and the default is to exclude no
    properties. Example: --zfs-recv-o-exclude-regex encryptionroot keystatus origin volblocksize
    volsize

# ZFS-RECV-X (EXPERIMENTAL)

The following group of parameters specifies additional zfs receive '-x' options that can be used
to configure the copying of ZFS dataset properties from the source dataset to its corresponding
destination dataset. The 'zfs-recv-o' group of parameters is applied before the 'zfs-recv-x'
group.

<div id="--zfs-recv-x-targets"></div>

**--zfs-recv-x-targets** *{full,incremental,full+incremental}*

*  The zfs send phase or phases during which the extra '-x' options are passed to 'zfs
    receive'. This can be one of the following choices: 'full', 'incremental',
    'full+incremental'. Default is 'full+incremental'. A 'full' send is sometimes also known
    as an 'initial' send.

<!-- -->

<div id="--zfs-recv-x-sources"></div>

**--zfs-recv-x-sources** *STRING*

*  The ZFS sources to provide to the 'zfs get -s' CLI in order to fetch the ZFS dataset
    properties that will be fed into the --zfs-recv-x-include/exclude-regex filter (see below).
    The sources are in the form of a comma-separated list (no spaces) containing one or more of
    the following choices: 'local', 'default', 'inherited', 'temporary', 'received',
    'none', with the default being 'local'. Uses 'zfs get -p -s $zfs-recv-x-sources all
    $SRC_DATASET' to fetch the properties to copy -
    https://openzfs.github.io/openzfs-docs/man/master/8/zfs-get.8.html. P.S: Note that the
    existing 'zfs send --props' option does not filter and that --props only reads properties
    from the 'local' ZFS property source (https://github.com/openzfs/zfs/issues/13024). Thus, -x
    opts do not benefit from source != 'local' (which is the default already).

<!-- -->

<div id="--zfs-recv-x-include-regex"></div>

**--zfs-recv-x-include-regex** *REGEX [REGEX ...]*

*  Take the output properties of --zfs-recv-x-sources (see above) and filter them such that we
    only retain the properties whose name matches at least one of the --include regexes but none
    of the --exclude regexes. If a property is excluded this decision is never reconsidered
    because exclude takes precedence over include. Append each retained property to the list of
    '-x' options in --zfs-recv-program-opt(s), unless another '-o' or '-x' option with the
    same name already exists therein. In other words, --zfs-recv-program-opt(s) takes precedence.

    The --zfs-recv-x-include-regex option can be specified multiple times. A leading `!`
    character indicates logical negation, i.e. the regex matches if the regex with the leading
    `!` character removed does not match. If the option starts with a `+` prefix then regexes
    are read from the newline-separated UTF-8 text file given after the `+` prefix, one regex
    per line inside of the text file.

    The default is to include no properties, thus by default no extra '-x' option is appended.
    Example: `--zfs-recv-x-include-regex recordsize volblocksize`. More examples: `.*`
    (include all properties), `foo bar myapp:.*` (include three regexes)
    `+zfs-recv-x_regexes.txt`, `+/path/to/zfs-recv-x_regexes.txt`

<!-- -->

<div id="--zfs-recv-x-exclude-regex"></div>

**--zfs-recv-x-exclude-regex** *REGEX [REGEX ...]*

*  Same syntax as --zfs-recv-x-include-regex (see above), and the default is to exclude no
    properties. Example: --zfs-recv-x-exclude-regex encryptionroot keystatus origin volblocksize
    volsize
