<!--
 Copyright 2024 Wolfgang Hoschek AT mac DOT com

 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# wbackup-zfs
[![Build](https://github.com/whoschek/wbackup-zfs/actions/workflows/python-app.yml/badge.svg)](https://github.com/whoschek/wbackup-zfs/actions/workflows/python-app.yml)
[![Coverage](https://whoschek.github.io/wbackup-zfs/badges/coverage-badge.svg)](https://whoschek.github.io/wbackup-zfs/coverage/)
[![os](https://whoschek.github.io/wbackup-zfs/badges/os-badge.svg)](https://github.com/whoschek/wbackup-zfs/blob/main/.github/workflows/python-app.yml)
[![zfs](https://whoschek.github.io/wbackup-zfs/badges/zfs-badge.svg)](https://github.com/whoschek/wbackup-zfs/blob/main/.github/workflows/python-app.yml)
[![python](https://whoschek.github.io/wbackup-zfs/badges/python-badge.svg)](https://github.com/whoschek/wbackup-zfs/blob/main/.github/workflows/python-app.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

<!-- DESCRIPTION BEGIN -->
*wbackup-zfs is a backup command line tool that reliably replicates ZFS
snapshots from a (local or remote) source ZFS dataset (ZFS filesystem or
ZFS volume) and its descendant datasets to a (local or remote)
destination ZFS dataset to make the destination dataset a recursively
synchronized copy of the source dataset, using zfs
send/receive/rollback/destroy and ssh tunnel as directed. For example,
wbackup-zfs can be used to incrementally replicate all ZFS snapshots
since the most recent common snapshot from source to destination, in
order to help protect against data loss or ransomware.*

When run for the first time, wbackup-zfs replicates the dataset and all
its snapshots from the source to the destination. On subsequent runs,
wbackup-zfs transfers only the data that has changed since the previous
run, i.e. it incrementally replicates to the destination all
intermediate snapshots that have been created on the source since the
last run. Source ZFS snapshots older than the most recent common
snapshot found on the destination are auto-skipped.

wbackup-zfs does not create or delete ZFS snapshots on the source - it
assumes you have a ZFS snapshot management tool to do so, for example
policy-driven Sanoid, zrepl, pyznap, zfs-auto-snapshot, manual zfs
snapshot/destroy, etc. wbackup-zfs treats the source as read-only, thus
the source remains unmodified. With the --dry-run flag, wbackup-zfs
also treats the destination as read-only. In normal operation,
wbackup-zfs treats the destination as append-only. Optional CLI flags
are available to delete destination snapshots and destination datasets
as directed, for example to make the destination identical to the source
if the two have somehow diverged in unforeseen ways. This easily enables
(re)synchronizing the backup from the production state, as well as
restoring the production state from backup.

The source 'pushes to' the destination whereas the destination 'pulls
from' the source. wbackup-zfs is installed and executed on the
'coordinator' host which can be either the host that contains the
source dataset (push mode), or the destination dataset (pull mode), or
both datasets (local mode, no network required, no ssh required), or any
third-party (even non-ZFS OSX) host as long as that host is able to SSH
(via standard 'ssh' CLI) into both the source and destination host
(pull-push mode). In Pull-push mode the source 'zfs send's the data
stream to the coordinator which immediately pipes the stream (without
storing anything locally) to the destination host that 'zfs receive's
it. Pull-push mode means that wbackup-zfs need not be installed or
executed on either source or destination host. Only the underlying
'zfs' CLI must be installed on both source and destination host.
wbackup-zfs can run as root or non-root user, in the latter case via a)
sudo or b) when granted corresponding ZFS permissions by administrators
via 'zfs allow' delegation mechanism.

wbackup-zfs is written in Python and continously runs a wide set of unit
tests and integration tests to ensure coverage and compatibility with
old and new versions of ZFS on Linux, FreeBSD and Solaris, on all Python
versions >= 3.7 (including latest stable which is currently
python-3.12). wbackup-zfs is a stand-alone program, akin to a
stand-alone shell script, and no additional Python packages are
required.

Optionally, wbackup-zfs applies bandwidth rate-limiting and progress
monitoring (via 'pv' CLI) during 'zfs send/receive' data transfers.
When run across the network, wbackup-zfs also transparently inserts
lightweight data compression (via 'zstd -1' CLI) and efficient data
buffering (via 'mbuffer' CLI) into the pipeline between network
endpoints during 'zfs send/receive' network transfers. If one of these
utilities is not installed this is auto-detected, and the operation
continues reliably without the corresponding auxiliary feature.

# Example Usage

* Example in local mode (no network, no ssh) to replicate ZFS dataset
tank1/foo/bar to tank2/boo/bar:

` wbackup-zfs tank1/foo/bar tank2/boo/bar`

* Same example in pull mode:

` wbackup-zfs root@host1.example.com:tank1/foo/bar tank2/boo/bar`

* Same example in push mode:

` wbackup-zfs tank1/foo/bar root@host2.example.com:tank2/boo/bar`

* Same example in pull-push mode:

` wbackup-zfs root@host1:tank1/foo/bar root@host2:tank2/boo/bar`

* Example in local mode (no network, no ssh) to recursively replicate
ZFS dataset tank1/foo/bar and its descendant datasets to tank2/boo/bar:

` wbackup-zfs tank1/foo/bar tank2/boo/bar --recursive`

* Example that makes destination identical to source even if the two
have drastically diverged:

` wbackup-zfs tank1/foo/bar tank2/boo/bar --recursive --force
--delete-missing-snapshots --delete-missing-datasets`

* Example with further options:

` wbackup-zfs tank1/foo/bar root@host2.example.com:tank2/boo/bar
--recursive --exclude-snapshot-regex '.*_(hourly|frequent)'
--exclude-snapshot-regex 'test_.*' --exclude-dataset
/tank1/foo/bar/temporary --exclude-dataset /tank1/foo/bar/baz/trash
--exclude-dataset-regex '(.*/)?private' --exclude-dataset-regex
'(.*/)?[Tt][Ee]?[Mm][Pp][0-9]*' --ssh-private-key
/root/.ssh/id_rsa`

# How To Install and Run

```
# Ubuntu / Debian:
sudo apt-get -y install zfsutils-linux python3 # ensure zfs and python are installed
sudo apt-get -y install zstd pv mbuffer # auxiliary helpers are optional

git clone https://github.com/whoschek/wbackup-zfs.git
cd wbackup-zfs
./wbackup-zfs --help # Run the CLI
sudo cp wbackup-zfs /usr/local/bin/ # Optional system installation
```

# How To Run Unit Tests
```
# verify zfs is installed
zfs --version

# verify python 3.7 or higher is installed
python3 --version

# verify sudo is working
sudo ls

# set this for unit tests if sshd is on a non-standard port (default is 22)
# export wbackup_zfs_test_ssh_port=12345
# export wbackup_zfs_test_ssh_port=22

# verify user can ssh in passwordless via loopback interface & private key
ssh -p $wbackup_zfs_test_ssh_port 127.0.0.1 echo hello

# verify zfs is on PATH
ssh -p $wbackup_zfs_test_ssh_port 127.0.0.1 zfs --version

# verify zpool is on PATH
ssh -p $wbackup_zfs_test_ssh_port 127.0.0.1 zpool --version

# verify zstd is on PATH for compression to become enabled
ssh -p $wbackup_zfs_test_ssh_port 127.0.0.1 zstd --version

# verify pv is on PATH for progress monitoring to become enabled
ssh -p $wbackup_zfs_test_ssh_port 127.0.0.1 pv --version

# verify mbuffer is on PATH for efficient buffering to become enabled
ssh -p $wbackup_zfs_test_ssh_port 127.0.0.1 mbuffer --version

# Finally, run unit tests
./test.sh
```


# Automated Test Runs

Results of automated test runs on a matrix of various old and new versions of ZFS/Python/Linux/FreeBSD/Solaris are
[here](https://github.com/whoschek/wbackup-zfs/actions/workflows/python-app.yml?query=event%3Aschedule), as generated
by [this script](https://github.com/whoschek/wbackup-zfs/blob/main/.github/workflows/python-app.yml).
The script also demonstrates functioning installation steps on Ubuntu, FreeBSD, Solaris, etc.
The script also generates code coverage reports which are published
[here](https://whoschek.github.io/wbackup-zfs/coverage).

The gist is that it should work on any flavor, with python (3.7 or higher, no additional python packages required)
only needed on the coordinator host.


# Usage

<!-- Docs: The blurb below is copied from the output of wbackup_zfs/wbackup_zfs.py --help -->
```
usage: wbackup-zfs [-h] [--recursive]
                   [--include-dataset DATASET [DATASET ...]]
                   [--exclude-dataset DATASET [DATASET ...]]
                   [--include-dataset-regex REGEX [REGEX ...]]
                   [--exclude-dataset-regex REGEX [REGEX ...]]
                   [--include-snapshot-regex REGEX [REGEX ...]]
                   [--exclude-snapshot-regex REGEX [REGEX ...]] [--force]
                   [--force-unmount] [--force-once]
                   [--zfs-send-program-opts STRING]
                   [--zfs-receive-program-opts STRING] [--skip-parent]
                   [--skip-missing-snapshots [{fail,dataset,continue}]]
                   [--max-retries INT] [--skip-on-error [{fail,tree,dataset}]]
                   [--skip-replication] [--delete-missing-snapshots]
                   [--delete-missing-datasets] [--no-privilege-elevation]
                   [--no-stream] [--no-create-bookmark] [--no-use-bookmark]
                   [--dry-run] [--verbose] [--quiet] [--logdir DIR]
                   [--ssh-config-file FILE] [--ssh-cipher STRING]
                   [--ssh-src-private-key FILE] [--ssh-dst-private-key FILE]
                   [--ssh-src-user STRING] [--ssh-dst-user STRING]
                   [--ssh-src-host STRING] [--ssh-dst-host STRING]
                   [--ssh-src-port INT] [--ssh-dst-port INT]
                   [--ssh-src-extra-opt STRING] [--ssh-dst-extra-opt STRING]
                   [--bwlimit STRING] [--compression-program STRING]
                   [--compression-program-opts STRING]
                   [--mbuffer-program STRING] [--mbuffer-program-opts STRING]
                   [--pv-program STRING] [--pv-program-opts STRING]
                   [--shell-program STRING] [--ssh-program STRING]
                   [--sudo-program STRING] [--zfs-program STRING]
                   [--zpool-program STRING]
                   [--include-envvar-regex REGEX [REGEX ...]]
                   [--exclude-envvar-regex REGEX [REGEX ...]] [--version]
                   [--help, -h]
                   SRC_DATASET DST_DATASET [SRC_DATASET DST_DATASET ...]
```

<!--
Docs: Generate pretty GitHub Markdown for ArgumentParser options and auto-update README.md below, like so:
./test/update-readme.py wbackup_zfs/wbackup_zfs.py README.md
-->

**SRC_DATASET DST_DATASET**

*  SRC_DATASET: Source ZFS dataset (and its descendants) that will be
    replicated. Can be a ZFS filesystem or ZFS volume. Format is
    [[user@]host:]dataset. The host name can also be an IPv4
    address. If the host name is '-', the dataset will be on the local
    host, and the corresponding SSH leg will be omitted. The same is
    true if the host is omitted and the dataset does not contain a ':'
    colon at the same time. Local dataset examples: `tank1/foo/bar`,
    `tank1`, `-:tank1/foo/bar:baz:boo` Remote dataset examples:
    `host:tank1/foo/bar`, `host.example.com:tank1/foo/bar`,
    `root@host:tank`, `root@host.example.com:tank1/foo/bar`,
    `user@127.0.0.1:tank1/foo/bar:baz:boo`. The first component of the
    ZFS dataset name is the ZFS pool name, here `tank1`.

    DST_DATASET: Destination ZFS dataset for replication. Has same
    naming format as SRC_DATASET. During replication, destination
    datasets that do not yet exist are created as necessary, along with
    their parent and ancestors.



**--recursive**, **-r**

*  During replication, also consider descendant datasets, i.e. datasets
    within the dataset tree, including children, and children of
    children, etc.

<!-- -->

**--include-dataset** *DATASET [DATASET ...]*

*  During replication, include any ZFS dataset (and its descendants)
    that is contained within SRC_DATASET if its dataset name is one of
    the given include dataset names but none of the exclude dataset
    names. A dataset name is absolute if the specified dataset is
    prefixed by `/`, e.g. `/tank/baz/tmp`. Otherwise the dataset
    name is relative wrt. source and destination, e.g. `baz/tmp` if
    the source is `tank`. This option is automatically translated to
    an --include-dataset-regex (see below) and can be specified
    multiple times. If the option starts with a `@` prefix then
    dataset names are read from the newline-separated UTF-8 file given
    after the `@` prefix. Examples: `/tank/baz/tmp` (absolute),
    `baz/tmp` (relative), `@dataset_names.txt`,
    `@/path/to/dataset_names.txt`

<!-- -->

**--exclude-dataset** *DATASET [DATASET ...]*

*  Same syntax as --include-dataset (see above) except that the option
    is automatically translated to an --exclude-dataset-regex (see
    below).

<!-- -->

**--include-dataset-regex** *REGEX [REGEX ...]*

*  During replication, include any ZFS dataset (and its descendants)
    that is contained within SRC_DATASET if its relative dataset path
    (e.g. `baz/tmp`) wrt SRC_DATASET matches at least one of the given
    include regular expressions but none of the exclude regular
    expressions. This option can be specified multiple times. A leading
    `!` character indicates logical negation, i.e. the regex matches
    if the regex with the leading `!` character removed does not
    match. Default: `.*` (include all datasets). Examples:
    `baz/tmp`, `(.*/)?doc[^/]*/(private|confidential).*`,
    `!public`

<!-- -->

**--exclude-dataset-regex** *REGEX [REGEX ...]*

*  Same syntax as --include-dataset-regex (see above) except that the
    default is `(.*/)?[Tt][Ee]?[Mm][Pp][0-9]*`

<!-- -->

**--include-snapshot-regex** *REGEX [REGEX ...]*

*  During replication, include any source ZFS snapshot or bookmark that
    has a name (i.e. the part after the '@' and '#') that matches at
    least one of the given include regular expressions but none of the
    exclude regular expressions. This option can be specified multiple
    times. A leading `!` character indicates logical negation, i.e.
    the regex matches if the regex with the leading `!` character
    removed does not match. Default: `.*` (include all snapshots).
    Examples: `test_.*`, `!prod_.*`,
    `.*_(hourly|frequent)`, `!.*_(weekly|daily)`

<!-- -->

**--exclude-snapshot-regex** *REGEX [REGEX ...]*

*  Same syntax as --include-snapshot-regex (see above) except that the
    default is to exclude no snapshots.

<!-- -->

**--force**

*  Before replication, delete destination ZFS snapshots that are more
    recent than the most recent common snapshot included on the source
    ('conflicting snapshots') and rollback the destination dataset
    correspondingly before starting replication. Also, if no common
    snapshot is included then delete all destination snapshots before
    starting replication. Without the --force flag, the destination
    dataset is treated as append-only, hence no destination snapshot
    that already exists is deleted, and instead the operation is aborted
    with an error when encountering a conflicting snapshot.

<!-- -->

**--force-unmount**

*  On destination, --force will also forcibly unmount file systems via
    'zfs rollback -f' and 'zfs destroy -f'.

<!-- -->

**--force-once**, **--f1**

*  Use the --force option at most once to resolve a conflict, then
    abort with an error on any subsequent conflict. This helps to
    interactively resolve conflicts, one conflict at a time.

<!-- -->

**--zfs-send-program-opts** *STRING*

*  Parameters to fine-tune 'zfs send' behaviour (optional); will be
    passed into 'zfs send' CLI. Default is '--props --raw
    --compressed'. See
    https://openzfs.github.io/openzfs-docs/man/master/8/zfs-send.8.html
    and https://github.com/openzfs/zfs/issues/13024

<!-- -->

**--zfs-receive-program-opts** *STRING*

*  Parameters to fine-tune 'zfs receive' behaviour (optional); will
    be passed into 'zfs receive' CLI. Default is '-u'. See
    https://openzfs.github.io/openzfs-docs/man/master/8/zfs-receive.8.html
    and
    https://openzfs.github.io/openzfs-docs/man/master/7/zfsprops.7.html

<!-- -->

**--skip-parent**

*  Skip processing of the SRC_DATASET and DST_DATASET and only process
    their descendant datasets, i.e. children, and children of children,
    etc (with --recursive). No dataset is processed unless --recursive
    is also specified. Analogy: `wbackup-zfs --recursive
    --skip-parent src dst` is akin to Unix `cp -r src/* dst/`

<!-- -->

**--skip-missing-snapshots** *[{fail,dataset,continue}]*

*  During replication, handle source datasets that include no snapshots
    (and no relevant bookmarks) as follows:

    a) 'fail': Abort with an error.

    b) 'dataset' (default): Skip the source dataset with a warning.
    Skip descendant datasets if --recursive and destination dataset
    does not exist. Otherwise skip to the next dataset.

    c) 'continue': Skip nothing. If destination snapshots exist,
    delete them (with --force) or abort with an error (without
    --force). If there is no such abort, continue processing with the
    next dataset. Eventually create empty destination dataset and
    ancestors if they do not yet exist and source dataset has at least
    one descendant that includes a snapshot.

<!-- -->

**--max-retries** *INT*

*  The number of times a retryable replication step shall be retried if
    it fails, for example because of network hiccups (default: 0). Also
    consider this option if a periodic pruning script may simultaneously
    delete a dataset or snapshot or bookmark while wbackup-zfs is
    running and attempting to access it.

<!-- -->

**--skip-on-error** *[{fail,tree,dataset}]*

*  During replication, if an error is not retryable, or --max-retries
    has been exhausted, or --skip-missing-snapshots raises an error,
    proceed as follows:

    a) 'fail': Abort the program with an error. This mode is ideal
    for testing, clear error reporting, and situations where consistency
    trumps availability.

    b) 'tree': Log the error, skip the dataset tree rooted at the
    dataset for which the error occurred, and continue processing the
    next (sibling) dataset tree. Example: Assume datasets tank/user1/foo
    and tank/user2/bar and an error occurs while processing tank/user1.
    In this case processing skips tank/user1/foo and proceeds with
    tank/user2.

    c) 'dataset' (default): Same as 'tree' except if the
    destination dataset already exists, skip to the next dataset
    instead. Example: Assume datasets tank/user1/foo and tank/user2/bar
    and an error occurs while processing tank/user1. In this case
    processing skips tank/user1 and proceeds with tank/user1/foo if the
    destination already contains tank/user1. Otherwise processing
    continues with tank/user2. This mode is for production use cases
    that require timely forward progress even in the presence of partial
    failures. For example, assume the job is to backup the home
    directories or virtual machines of thousands of users across an
    organization. Even if replication of some of the datasets for some
    users fails due too conflicts, busy datasets, etc, the replication
    job will continue for the remaining datasets and the remaining
    users.

<!-- -->

**--skip-replication**

*  Skip replication step (see above) and proceed to the optional
    --delete-missing-snapshots step immediately (see below).

<!-- -->

**--delete-missing-snapshots**

*  After successful replication, delete existing destination snapshots
    that do not exist within the source dataset if they match at least
    one of --include-snapshot-regex but none of
    --exclude-snapshot-regex and the destination dataset is included
    via --{include|exclude}-dataset-regex
    --{include|exclude}-dataset policy. Does not recurse without
    --recursive.

<!-- -->

**--delete-missing-datasets**

*  After successful replication step and successful
    --delete-missing-snapshots step, if any, delete existing
    destination datasets that do not exist within the source dataset if
    they are included via --{include|exclude}-dataset-regex
    --{include|exclude}-dataset policy. Also delete an existing
    destination dataset that has no snapshot if all descendants of that
    dataset do not have a snapshot either (again, only if the existing
    destination dataset is included via
    --{include|exclude}-dataset-regex --{include|exclude}-dataset
    policy). Does not recurse without --recursive.

<!-- -->

**--no-privilege-elevation**, **-p**

*  Do not attempt to run state changing ZFS operations 'zfs
    create/rollback/destroy/send/receive' as root (via 'sudo -u root'
    elevation granted by administrators appending the following to
    /etc/sudoers: `<NON_ROOT_USER_NAME> ALL=NOPASSWD:/path/to/zfs`).

    Instead, the --no-privilege-elevation flag is for non-root users
    that have been granted corresponding ZFS permissions by
    administrators via 'zfs allow' delegation mechanism, like so: sudo
    zfs allow -u $SRC_NON_ROOT_USER_NAME send,bookmark $SRC_DATASET;
    sudo zfs allow -u $DST_NON_ROOT_USER_NAME
    mount,create,receive,rollback,destroy,canmount,mountpoint,readonly,compression,encryption,keylocation,recordsize
    $DST_DATASET_OR_POOL.

    For extra security $SRC_NON_ROOT_USER_NAME should be different than
    $DST_NON_ROOT_USER_NAME, i.e. the sending Unix user on the source
    and the receiving Unix user at the destination should be separate
    Unix user accounts with separate private keys, per the principle of
    least privilege. Further, if you do not plan to use the --force
    flag or --delete-missing-snapshots or --delete-missing-dataset
    then ZFS permissions 'rollback,destroy' can be omitted. If you do
    not plan to customize the respective ZFS dataset property then ZFS
    permissions
    'canmount,mountpoint,readonly,compression,encryption,keylocation,recordsize'
    can be omitted, arriving at the absolutely minimal set of required
    destination permissions: `mount,create,receive`. Also see
    https://openzfs.github.io/openzfs-docs/man/master/8/zfs-allow.8.html#EXAMPLES
    and https://tinyurl.com/9h97kh8n and
    https://youtu.be/o_jr13Z9f1k?si=7shzmIQJpzNJV6cq

<!-- -->

**--no-stream**

*  During replication, only replicate the most recent source snapshot
    of a dataset (using -i incrementals instead of -I incrementals),
    hence skip all intermediate source snapshots that may exist between
    that and the most recent common snapshot. If there is no common
    snapshot also skip all other source snapshots for the dataset,
    except for the most recent source snapshot. This option helps the
    destination to 'catch up' with the source ASAP, consuming a
    minimum of disk space, at the expense of reducing reliable options
    for rolling back to intermediate snapshots in the future.

<!-- -->

**--no-create-bookmark**

*  For increased safety, in normal operation wbackup-zfs behaves as
    follows wrt. ZFS bookmark creation, if it is autodetected that the
    source ZFS pool support bookmarks: Whenever it has successfully
    completed replication of the most recent source snapshot,
    wbackup-zfs creates a ZFS bookmark of that snapshot and attaches it
    to the source dataset. Bookmarks exist so an incremental stream can
    continue to be sent from the source dataset without having to keep
    the already replicated snapshot around on the source dataset until
    the next upcoming snapshot has been successfully replicated. This
    way you can send the snapshot from the source dataset to another
    host, then bookmark the snapshot on the source dataset, then delete
    the snapshot from the source dataset to save disk space, and then
    still incrementally send the next upcoming snapshot from the source
    dataset to the other host by referring to the bookmark.

    The --no-create-bookmark option disables this safety feature but is
    discouraged, because bookmarks are tiny and relatively cheap and
    help to ensure that ZFS replication can continue even if source and
    destination dataset somehow have no common snapshot anymore. For
    example, if a pruning script has accidentally deleted too many (or
    even all) snapshots on the source dataset in an effort to reclaim
    disk space, replication can still proceed because it can use the
    info in the bookmark (the bookmark must still exist in the source
    dataset) instead of the info in the metadata of the (now missing)
    source snapshot.

    A ZFS bookmark is a tiny bit of metadata extracted from a ZFS
    snapshot by the 'zfs bookmark' CLI, and attached to a dataset,
    much like a ZFS snapshot. Note that a ZFS bookmark does not contain
    user data; instead a ZFS bookmark is essentially a tiny pointer in
    the form of the GUID of the snapshot and 64-bit transaction group
    number of the snapshot and creation time of the snapshot, which is
    sufficient to tell the destination ZFS pool how to find the
    destination snapshot corresponding to the source bookmark and
    (potentially already deleted) source snapshot. A bookmark can be fed
    into 'zfs send' as the source of an incremental send. Note that
    while a bookmark allows for its snapshot to be deleted on the source
    after successful replication, it still requires that its snapshot is
    not somehow deleted prematurely on the destination dataset, so be
    mindful of that. By convention, a bookmark created by wbackup-zfs
    has the same name as its corresponding snapshot, the only difference
    being the leading '#' separator instead of the leading '@'
    separator. wbackup-zfs itself never deletes any bookmark.

    You can list bookmarks, like so: `zfs list -t bookmark -o
    name,guid,createtxg,creation -d 1 $SRC_DATASET`, and you can (and
    should) periodically prune obsolete bookmarks just like snapshots,
    like so: `zfs destroy $SRC_DATASET#$BOOKMARK`. Typically,
    bookmarks should be pruned less aggressively than snapshots, and
    destination snapshots should be pruned less aggressively than source
    snapshots. As an example starting point, here is a script that
    deletes all bookmarks older than X days in a given dataset and its
    descendants: `days=90; dataset=tank/foo/bar; zfs list -t bookmark
    -o name,creation -Hp -r $dataset | while read -r BOOKMARK
    CREATION_TIME; do [ $CREATION_TIME -le $(($(date +%s) - days *
    86400)) ] && echo $BOOKMARK; done | xargs -I {} sudo zfs destroy
    {}` A better example starting point can be found in third party
    tools or this script:
    https://github.com/whoschek/wbackup-zfs/blob/main/test/prune_bookmarks.py

<!-- -->

**--no-use-bookmark**

*  For increased safety, in normal operation wbackup-zfs also looks for
    bookmarks (in addition to snapshots) on the source dataset in order
    to find the most recent common snapshot wrt. the destination
    dataset, if it is auto-detected that the source ZFS pool support
    bookmarks. The --no-use-bookmark option disables this safety
    feature but is discouraged, because bookmarks help to ensure that
    ZFS replication can continue even if source and destination dataset
    somehow have no common snapshot anymore.

    Note that it does not matter whether a bookmark was created by
    wbackup-zfs or a third party script, as only the GUID of the
    bookmark and the GUID of the snapshot is considered for comparison,
    and ZFS guarantees that any bookmark of a given snapshot
    automatically has the same GUID, transaction group number and
    creation time as the snapshot. Also note that you can create, delete
    and prune bookmarks any way you like, as wbackup-zfs (without
    --no-use-bookmark) will happily work with whatever bookmarks
    currently exist, if any.

<!-- -->

**--dry-run**, **-n**

*  Do a dry-run (aka 'no-op') to print what operations would happen
    if the command were to be executed for real. This option treats both
    the ZFS source and destination as read-only.

<!-- -->

**--verbose**, **-v**

*  Print verbose information. This option can be specified multiple
    times to increase the level of verbosity. To print what ZFS/SSH
    operation exactly is happening (or would happen), add the `-v -v`
    flag, maybe along with --dry-run. ERROR, WARN, INFO, DEBUG, TRACE
    output lines are identified by [E], [W], [I], [D], [T]
    prefixes, respectively.

<!-- -->

**--quiet**, **-q**

*  Suppress non-error, info, debug, and trace output.

<!-- -->

**--logdir** *DIR*

*  Path to log output directory (optional). Default is
    $HOME/wbackup-zfs-logs

<!-- -->

**--ssh-config-file** *FILE*

*  Path to SSH ssh_config(5) file (optional); will be passed into ssh
    -F CLI.

<!-- -->

**--ssh-cipher** *STRING*

*  SSH cipher specification for encrypting the session (optional); will
    be passed into ssh -c CLI. --ssh-cipher is a comma-separated list
    of ciphers listed in order of preference. See the 'Ciphers'
    keyword in ssh_config(5) for more information:
    https://manpages.ubuntu.com/manpages/man5/sshd_config.5.html.
    Default: `^aes256-gcm@openssh.com`

<!-- -->

**--ssh-src-private-key** *FILE*

*  Path to SSH private key file on local host to connect to source
    (optional); will be passed into ssh -i CLI. default:
    $HOME/.ssh/id_rsa

<!-- -->

**--ssh-dst-private-key** *FILE*

*  Path to SSH private key file on local host to connect to destination
    (optional); will be passed into ssh -i CLI. default:
    $HOME/.ssh/id_rsa

<!-- -->

**--ssh-src-user** *STRING*

*  Remote SSH username of source host to connect to (optional).
    Overrides username given in SRC_DATASET.

<!-- -->

**--ssh-dst-user** *STRING*

*  Remote SSH username of destination host to connect to (optional).
    Overrides username given in DST_DATASET.

<!-- -->

**--ssh-src-host** *STRING*

*  Remote SSH hostname of source host to connect to (optional). Can
    also be an IPv4 or IPv6 address. Overrides hostname given in
    SRC_DATASET.

<!-- -->

**--ssh-dst-host** *STRING*

*  Remote SSH hostname of destination host to connect to (optional).
    Can also be an IPv4 or IPv6 address. Overrides hostname given in
    DST_DATASET.

<!-- -->

**--ssh-src-port** *INT*

*  Remote SSH port of source host to connect to (optional).

<!-- -->

**--ssh-dst-port** *INT*

*  Remote SSH port of destination host to connect to (optional).

<!-- -->

**--ssh-src-extra-opt** *STRING*

*  Additional option to be passed to ssh CLI when connecting to source
    host (optional). This option can be specified multiple times.
    Example: `--ssh-src-extra-opt='-v -v'` to debug ssh config
    issues.

<!-- -->

**--ssh-dst-extra-opt** *STRING*

*  Additional option to be passed to ssh CLI when connecting to
    destination host (optional). This option can be specified multiple
    times. Example: `--ssh-dst-extra-opt='-v -v'` to debug ssh
    config issues.

<!-- -->

**--bwlimit** *STRING*

*  Sets 'pv' bandwidth rate limit for zfs send/receive data transfer
    (optional). Example: `100m` to cap throughput at 100 MB/sec.
    Default is unlimited. Also see https://linux.die.net/man/1/pv

<!-- -->

**--compression-program** *STRING*

*  The name or path to the 'zstd' executable (optional). Default is
    'zstd'. Examples: 'lz4', 'pigz', '/opt/bin/zstd' Use '-'
    to disable the use of this program.

<!-- -->

**--compression-program-opts** *STRING*

*  The options to be passed to the compression program on the
    compression step (optional). Default is '-1'.

<!-- -->

**--mbuffer-program** *STRING*

*  The name or path to the 'mbuffer' executable (optional). Default
    is 'mbuffer'. Use '-' to disable the use of this program.

<!-- -->

**--mbuffer-program-opts** *STRING*

*  Options to be passed to 'mbuffer' program (optional). Default:
    '-q -m 128M'.

<!-- -->

**--pv-program** *STRING*

*  The name or path to the 'pv' executable (optional). Default is
    'pv'. Use '-' to disable the use of this program.

<!-- -->

**--pv-program-opts** *STRING*

*  The options to be passed to the 'pv' program (optional). Default:
    '--progress --timer --eta --fineta --rate --average-rate
    --bytes --interval=1 --width=120 --buffer-size=1M'.

<!-- -->

**--shell-program** *STRING*

*  The name or path to the 'sh' executable (optional). Default is
    'sh'. Use '-' to disable the use of this program.

<!-- -->

**--ssh-program** *STRING*

*  The name or path to the 'ssh' executable (optional). Default is
    'ssh'. Examples: 'hpnssh' or 'ssh' or '/opt/bin/ssh' or
    wrapper scripts around 'ssh'. Use '-' to disable the use of this
    program.

<!-- -->

**--sudo-program** *STRING*

*  The name or path to the 'sudo' executable (optional). Default is
    'sudo'. Use '-' to disable the use of this program.

<!-- -->

**--zfs-program** *STRING*

*  The name or path to the 'zfs' executable (optional). Default is
    'zfs'.

<!-- -->

**--zpool-program** *STRING*

*  The name or path to the 'zpool' executable (optional). Default is
    'zpool'. Use '-' to disable the use of this program.

<!-- -->

**--include-envvar-regex** *REGEX [REGEX ...]*

*  On program startup, unset all Unix environment variables for which
    the full environment variable name matches at least one of the
    excludes but none of the includes. The purpose is to tighten
    security and help guard against accidental inheritance or malicious
    injection of environment variable values that may have unintended
    effects. This option can be specified multiple times. A leading
    `!` character indicates logical negation, i.e. the regex matches
    if the regex with the leading `!` character removed does not
    match. The default is to include no environment variables, i.e. to
    make no exceptions to --exclude-envvar-regex. Example that retains
    at least these three env vars: `--include-envvar-regex
    wbackup_zfs_min_sleep_secs --include-envvar-regex
    wbackup_zfs_max_sleep_secs --include-envvar-regex
    wbackup_zfs_max_elapsed_secs`. Example that retains all environment
    variables without tightened security: `'.*'`

<!-- -->

**--exclude-envvar-regex** *REGEX [REGEX ...]*

*  Same syntax as --include-envvar-regex (see above) except that the
    default is to exclude no environment variables. Example:
    `wbackup_zfs_.*`

<!-- -->

**--version**

*  Display version information and exit.

<!-- -->

**--help, -h**

*  show this help message and exit.
