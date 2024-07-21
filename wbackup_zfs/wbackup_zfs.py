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
# Unless required by azpplicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import collections
import operator
import os
import platform
import pprint
import pwd
import re
import random
import shlex
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from collections import defaultdict
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional, Iterable, Set

prog_name = 'wbackup-zfs'
prog_version = "0.9.0"
prog_author = "Wolfgang Hoschek"
die_status = 3
if sys.version_info < (3, 7):
    print(f"ERROR: {prog_name} requires Python version >= 3.7!", file=sys.stderr)
    sys.exit(die_status)
exclude_dataset_regexes_default = r'(.*/)?[Tt][Ee]?[Mm][Pp][0-9]*'  # skip tmp datasets by default
max_retries_default = 0
ssh_private_key_file_default = ".ssh/id_rsa"
zfs_version_is_at_least_2_1_0 = 'zfs>=2.1.0'
PIPE = subprocess.PIPE


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog_name,
        allow_abbrev=False,
        description=f'''
*{prog_name} is a backup command line tool that reliably replicates ZFS snapshots from a (local or remote)
source ZFS dataset (aka ZFS filesystem) and its descendant datasets to a (local or remote) destination ZFS dataset 
to make the destination dataset a recursively synchronized copy of the source dataset, using 
zfs send/receive/rollback/destroy and ssh tunnel as directed. For example, {prog_name} can be used to incrementally 
replicate all ZFS snapshots since the most recent common snapshot from source to destination, in order to help 
protect against data loss or ransomware.*

When run for the first time, {prog_name} replicates the dataset and all its snapshots from the source to the
destination. On subsequent runs, {prog_name} transfers only the data that has changed since the previous run,
i.e. it incrementally replicates to the destination all intermediate snapshots that have been created on
the source since the last run. Source ZFS snapshots older than the most recent common snapshot found on the 
destination are auto-skipped.

{prog_name} does not create or delete ZFS snapshots on the source - it assumes you have a ZFS snapshot
management tool to do so, for example policy-driven Sanoid, pyznap, zrepl, zfs-auto-snapshot, manual zfs
snapshot/destroy, etc. {prog_name} treats the source as read-only, thus the source remains unmodified.
With the --dry-run flag, {prog_name} also treats the destination as read-only.
In normal operation, {prog_name} treats the destination as append-only. Optional CLI flags are available to
delete destination snapshots and destination datasets as directed, for example to make the destination
identical to the source if the two have somehow diverged in unforeseen ways. This easily enables
(re)synchronizing the backup from the production state, as well as restoring the production state from
backup.

The source 'pushes to' the destination whereas the destination 'pulls from' the source. {prog_name} is installed 
and executed on the 'coordinator' host which can be either the host that contains the source dataset (push mode), 
or the destination dataset (pull mode), or both datasets (local mode, no network required, no ssh required), 
or any third-party (even non-ZFS) host as long as that host is able to SSH (via standard 'ssh' CLI) into 
both the source and destination host (pull-push mode). In Pull-push mode the source 'zfs send's the data stream  
to the coordinator which immediately pipes the stream (without storing it locally) to the destination 
host that 'zfs receive's it. Pull-push mode means that {prog_name} need not be installed 
or executed on either source or destination host. Only the underlying 'zfs' CLI must be installed on both source 
and destination host. {prog_name} can run as root or non-root user, in the latter case via a) sudo or b) when 
granted corresponding ZFS permissions by administrators via 'zfs allow' delegation mechanism.

{prog_name} is written in Python and continously runs a wide set of unit tests and integration tests to ensure 
coverage and compatibility with old and new versions of ZFS on Linux, FreeBSD and Solaris, on all Python 
versions >= 3.7 (including latest stable which is currently python-3.12). No additional python packages are required.

Optionally, {prog_name} applies bandwidth rate-limiting and progress monitoring (via 'pv' CLI) during 'zfs
send/receive' data transfers. When run across the network, {prog_name} also transparently inserts lightweight
data compression (via 'zstd -1' CLI) and efficient data buffering (via 'mbuffer' CLI) into the pipeline
between network endpoints during 'zfs send/receive' network transfers. If one of these utilities is not
installed this is auto-detected, and the operation continues reliably without the corresponding auxiliary
feature.


Example Usage
==========
Example in local mode (no network, no ssh) to replicate ZFS dataset tank1/foo/bar to tank2/boo/bar:

`   {prog_name} tank1/foo/bar tank2/boo/bar`

Same example in pull mode:

`   {prog_name} root@host1.example.com:tank1/foo/bar tank2/boo/bar`

Same example in push mode:

`   {prog_name} tank1/foo/bar root@host2.example.com:tank2/boo/bar`

Same example in pull-push mode:

`   {prog_name} root@host1:tank1/foo/bar root@host2:tank2/boo/bar`

Example in local mode (no network, no ssh) to recursively replicate ZFS dataset tank1/foo/bar and its descendant datasets to tank2/boo/bar:

`   {prog_name} tank1/foo/bar tank2/boo/bar --recursive`

Example that makes destination identical to source even if the two have drastically diverged:

`   {prog_name} tank1/foo/bar tank2/boo/bar --recursive --force --delete-missing-snapshots --delete-missing-datasets`

Example with further options:

`   {prog_name} tank1/foo/bar root@host2.example.com:tank2/boo/bar --recursive --exclude-dataset /tank1/foo/bar/temporary --exclude-dataset /tank1/foo/bar/baz/trash --exclude-dataset-regex '!(.*/)?public' --exclude-dataset-regex '(.*/)?[Tt][Ee]?[Mm][Pp][0-9]*' --ssh-private-key /root/.ssh/id_rsa`
''', formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument(
        'src_root_dataset', metavar='SRC_DATASET', type=str,
        help=("Source ZFS dataset (and its descendants) that will be replicated. Format is "
              "[[user@]host:]dataset. The host name can also be an IPv4 address. If the host name is '-', "
              "the dataset will be on the local host, and the corresponding SSH leg will be omitted. The same is true "
              "if the host is omitted and the dataset does not contain a ':' colon at the same time. "
              "Local dataset examples: `tank1/foo/bar`, `tank1`, `-:tank1/foo/bar:baz:boo` "
              "Remote dataset examples: `host:tank1/foo/bar`, `host.example.com:tank1/foo/bar`, "
              "`root@host:tank`, `root@host.example.com:tank1/foo/bar`, `user@127.0.0.1:tank1/foo/bar:baz:boo`. "
              "The first component of the ZFS dataset name is the ZFS pool name, here `tank1`."))
    parser.add_argument(
        'dst_root_dataset', metavar='DST_DATASET', type=str,
        help=("Destination ZFS dataset for replication. Has same naming format as SRC_DATASET. During replication, "
              "destination datasets that do not yet exist are created as necessary, along with their parent and "
              "ancestors."))
    parser.add_argument(
        '--no-stream', action='store_true',
        help=argparse.SUPPRESS)
    parser.add_argument(
        '--no-create-bookmark', action='store_true',
        help=argparse.SUPPRESS)
    parser.add_argument(
        '--no-use-bookmark', action='store_true',
        help=argparse.SUPPRESS)
    parser.add_argument(
        '--recursive', '-r', action='store_true',
        help=("During replication, also include descendant datasets, i.e. datasets within the dataset tree, "
              "including children, and children of children, etc."))
    parser.add_argument(
        '--include-dataset', action=FileOrLiteralAction, nargs='+', default=[], metavar='DATASET',
        help=("During replication, include any ZFS dataset (and its descendants) that is contained within SRC_DATASET "
              "if its dataset name is one of the given include dataset names but none of the exclude dataset names. "
              "A dataset name is absolute if the specified dataset is prefixed by `/`, e.g. `/tank/baz/tmp`. "
              "Otherwise the dataset name is relative wrt. source and destination, e.g. `baz/tmp` if the source "
              "is `tank`. "
              "This option is automatically translated to an --include-dataset-regex (see below) and can be "
              "specified multiple times. "
              "If the option starts with a `@` prefix then dataset names are read from the newline-separated "
              "UTF-8 file given after the `@` prefix. "
              "Examples: `/tank/baz/tmp` (absolute), `baz/tmp` (relative), "
              "`@dataset_names.txt`, `@/path/to/dataset_names.txt`"))
    parser.add_argument(
        '--exclude-dataset', action=FileOrLiteralAction, nargs='+', default=[], metavar='DATASET',
        help=("Same syntax as --include-dataset (see above) except that the option is automatically translated to an "
              "--exclude-dataset-regex (see below)."))
    parser.add_argument(
        '--include-dataset-regex', action=FileOrLiteralAction, nargs='+', default=[], metavar='REGEX',
        help=("During replication, include any ZFS dataset (and its descendants) that is contained within SRC_DATASET "
              "if its relative dataset path (e.g. `baz/tmp`) wrt SRC_DATASET matches at least one of the given include "
              "regular expressions but none of the exclude regular expressions. "
              "This option can be specified multiple times. "
              "A leading `!` character indicates logical negation, i.e. the regex matches if the regex with the "
              "leading `!` character removed does not match. "
              "Default: `.*` (include all datasets). "
              "Examples: `baz/tmp`, `(.*/)?doc[^/]*/(private|confidential).*`, `!public`"))
    parser.add_argument(
        '--exclude-dataset-regex', action=FileOrLiteralAction, nargs='+', default=[], metavar='REGEX',
        help=("Same syntax as --include-dataset-regex (see above) except that the default "
              f"is `{exclude_dataset_regexes_default}`"))
    parser.add_argument(
        '--include-snapshot-regex', action=FileOrLiteralAction, nargs='+', default=[], metavar='REGEX',
        help=("During replication, include any ZFS snapshot that has a name (i.e. the part after the '@') that matches "
              "at least one of the given include regular expressions but none of the exclude regular expressions. "
              "This option can be specified multiple times. "
              "A leading `!` character indicates logical negation, i.e. the regex matches if the regex with the "
              "leading `!` character removed does not match. "
              "Default: `.*` (include all snapshots). "
              "Examples: `test_.*`, `!prod_.*`, `.*_(hourly|frequent)`, `!.*_(weekly|daily)`"))
    parser.add_argument(
        '--exclude-snapshot-regex', action=FileOrLiteralAction, nargs='+', default=[], metavar='REGEX',
        help="Same syntax as --include-snapshot-regex (see above) except that the default is to exclude no snapshots.")
    parser.add_argument(
        '--force', action='store_true',
        help=("Before replication, delete destination ZFS snapshots that are more recent than the most recent common "
              "snapshot found on the source ('conflicting snapshots') and rollback the destination dataset "
              "correspondingly before starting replication. Also, if no common snapshot is found then delete all "
              "destination snapshots before starting replication. Without the --force flag, the destination dataset is "
              "treated as append-only, hence no destination snapshot that already exists is deleted, and instead the "
              "operation is aborted with an error when encountering a conflicting snapshot."))
    parser.add_argument(
        '--force-once', '-f1', action='store_true',
        help=("Use the --force option at most once to resolve a conflict, then abort with an error on any subsequent "
              "conflict. This helps to interactively resolve conflicts, one conflict at a time."))
    parser.add_argument(
        '--force-hard', action='store_true',
        # help=("On destination, --force will also delete dependents such as clones and bookmarks via "
        #       "'zfs rollback -R' and 'zfs destroy -R'. This can be very destructive and is rarely what you want!"))
        help=argparse.SUPPRESS)
    parser.add_argument(
        '--skip-missing-snapshots', choices=['true', 'false', 'error'], default='true', nargs='?',
        help=("Default is 'true'. During replication, handle source datasets that have no snapshots as follows: "
              "a) 'error': Abort with an error if --recursive. "
              "b) 'true': Skip the source dataset and its descendants with a warning if --recursive. "
              "c) otherwise (regardless of --recursive flag): If destination snapshots exist, delete them (with "
              "--force) or abort with an error (without --force). Create empty destination dataset and ancestors "
              "if they do not yet exist and source dataset has at least one descendant with a snapshot."))
    parser.add_argument(
        '--skip-replication', action='store_true',
        help="Skip replication step (see above) and proceed to the optional --delete-missing-snapshots step "
             "immediately (see below).")
    parser.add_argument(
        '--delete-missing-snapshots', action='store_true',
        help=("After successful replication, delete existing destination snapshots that do not exist within the source "
              "dataset if they match at least one of --include-snapshot-regex but none of --exclude-snapshot-regex "
              "and the destination dataset is included via --{include|exclude}-dataset-regex "
              "--{include|exclude}-dataset policy."))
    parser.add_argument(
        '--delete-missing-datasets', action='store_true',
        help=("After successful replication step and successful --delete-missing-snapshots step, if any, delete "
              "existing destination datasets that do not exist within the source dataset if they are included via "
              "--{include|exclude}-dataset-regex --{include|exclude}-dataset policy. "
              "Also delete an existing destination dataset that has no snapshot if all descendants of that dataset do "
              "not have a snapshot either (again, only if the existing destination dataset is included via "
              "--{include|exclude}-dataset-regex --{include|exclude}-dataset policy). "
              "Does not recurse without --recursive."))
    parser.add_argument(
        '--ssh-config-file', type=str, metavar='FILE',
        help='Path to SSH ssh_config(5) file (optional); will be passed into ssh -F CLI.')
    parser.add_argument(
        '--ssh-private-key', type=str, metavar='FILE',
        help=f'Path to SSH private key file on local host (optional); will be passed into ssh -i CLI. '
             f'default: $HOME/{ssh_private_key_file_default}')
    parser.add_argument(
        '--ssh-cipher', type=str, default='aes256-gcm@openssh.com', metavar='STRING',
        help=f'Name of SSH cipher specification for encrypting the session (optional); will be passed into ssh -c CLI. '
             f'(default: aes256-gcm@openssh.com)')
    parser.add_argument(
        '--ssh-src-user', type=str, metavar='STRING',
        help='Remote SSH username of source host to connect to (optional). Overrides username given in SRC_DATASET.')
    parser.add_argument(
        '--ssh-dst-user', type=str, metavar='STRING',
        help='Remote SSH username of destination host to connect to (optional). Overrides username given in DST_DATASET.')
    parser.add_argument(
        '--ssh-src-host', type=str, metavar='STRING',
        help='Remote SSH hostname of source host to connect to (optional). Can also be an IPv4 or IPv6 address. '
             'Overrides hostname given in SRC_DATASET.')
    parser.add_argument(
        '--ssh-dst-host', type=str, metavar='STRING',
        help='Remote SSH hostname of destination host to connect to (optional). Can also be an IPv4 or IPv6 address. '
             'Overrides hostname given in DST_DATASET.')
    parser.add_argument(
        '--ssh-src-port', type=int, metavar='INT',
        help='Remote SSH port of source host to connect to (optional).')
    parser.add_argument(
        '--ssh-dst-port', type=int, metavar='INT',
        help='Remote SSH port of destination host to connect to (optional).')
    parser.add_argument(
        '--ssh-src-extra-opt', action='append', default=[], metavar='STRING',
        help=("Additional option to be passed to ssh CLI when connecting to source host (optional). This option "
              "can be specified multiple times. Example: `-v -v --ssh-src-extra-opt '-v -v'` to "
              "debug ssh config issues."))
    parser.add_argument(
        '--ssh-dst-extra-opt', action='append', default=[], metavar='STRING',
        help=("Additional option to be passed to ssh CLI when connecting to destination host (optional). This option "
              "can be specified multiple times. Example: `-v -v --ssh-dst-extra-opt '-v -v'` to "
              "debug ssh config issues."))
    parser.add_argument(
        '--max-retries', type=int, min=0, default=max_retries_default, action=CheckRange, metavar='INT',
        help=(f'The number of times a zfs send/receive data transfer shall be retried if it fails across the network '
              f'(default: {max_retries_default}).'))
    parser.add_argument(
        '--bwlimit', type=str, metavar='STRING',
        help=("Sets 'pv' bandwidth rate limit for zfs send/receive data transfer (optional). Example: `100m` to cap "
              "throughput at 100 MB/sec. Default is unlimited. Also see https://linux.die.net/man/1/pv"))
    parser.add_argument(
        '--no-privilege-elevation', '-p', action='store_true',
        help=("Do not attempt to run state changing ZFS operations 'zfs create/rollback/destroy/send/receive' as root "
              "(via 'sudo -u root' elevation granted by appending this to /etc/sudoers: "
              "`<NON_ROOT_USER_NAME> ALL=NOPASSWD:/path/to/zfs`). "
              "Instead, the --no-privilege-elevation flag is for non-root users that have been granted corresponding "
              "ZFS permissions by administrators via 'zfs allow' delegation mechanism, like so: "
              "sudo zfs allow -u $NON_ROOT_USER_NAME send,hold,bookmark $SRC_DATASET; "
              "sudo zfs allow -u $NON_ROOT_USER_NAME mount,create,receive,rollback,destroy,canmount,mountpoint,"
              "readonly,compression,encryption,keylocation,recordsize $DST_DATASET_OR_POOL; "
              "If you do not plan to use the --force flag or --delete-missing-snapshots or --delete-missing-dataset "
              "then ZFS permissions 'rollback,destroy' can be omitted. "
              "If you do not plan to customize the respective ZFS dataset property then "
              "ZFS permissions 'canmount,mountpoint,readonly,compression,encryption,keylocation,recordsize' can be "
              "omitted. Also see https://tinyurl.com/yuyj23pz and https://tinyurl.com/9h97kh8n and "
              "https://github.com/openzfs/zfs/issues/13024"))
    parser.add_argument(
        '--dry-run', '-n', action='store_true',
        help=("Do a dry-run (aka 'no-op') to print what operations would happen if the command were to be executed "
              "for real. This option treats both the ZFS source and destination as read-only."))
    parser.add_argument(
        '--verbose', '-v', action='count', default=0,
        help=("Print verbose information. This option can be specified multiple times to increase the level of "
              "verbosity. To print what ZFS/SSH operation exactly is happening (or would happen), add the `-v -v` "
              "flag, maybe along with --dry-run. "
              "ERROR, WARN, INFO, DEBUG, TRACE output lines are identified by [E], [W], [I], [D], [T] prefixes, "
              "respectively."))
    parser.add_argument(
        '--quiet', '-q', action='store_true',
        help='Suppress non-error, info, debug, and trace output.')
    parser.add_argument(
        '--version', action='version', version=f"{prog_name}-{prog_version}, by {prog_author}",
        help='Display version information and exit.')
    parser.add_argument(
        '--help, -h', action='help',
        help='show this help message and exit.')
    parser.add_argument('--is-test-mode', action='store_true', help=argparse.SUPPRESS)
    return parser


#############################################################################
class Params:
    def __init__(self, args: argparse.Namespace, sys_argv: Optional[List[str]] = None):
        self.args = args
        self.sys_argv = sys_argv if sys_argv is not None else []
        if not args.src_root_dataset or not args.dst_root_dataset:
            argument_parser().print_help()
            sys.exit(die_status)
        self.src_root_dataset = args.src_root_dataset
        self.dst_root_dataset = args.dst_root_dataset
        self.origin_src_root_dataset = self.src_root_dataset
        self.origin_dst_root_dataset = self.dst_root_dataset
        self.recursive = args.recursive
        self.recursive_flag = "-r" if args.recursive else ""
        self.force = args.force
        self.force_once = args.force_once
        if self.force_once:
            self.force = True
        self.force_hard = "-R" if args.force_hard else ""
        self.skip_missing_snapshots = {'true': True, 'false': False, 'error': None}[args.skip_missing_snapshots]
        self.create_bookmark = not args.no_create_bookmark
        self.use_bookmark = not args.no_use_bookmark
        self.no_stream = args.no_stream
        self.delete_missing_datasets = args.delete_missing_datasets
        self.delete_empty_datasets = args.delete_missing_datasets
        self.delete_missing_snapshots = args.delete_missing_snapshots
        self.skip_replication = args.skip_replication
        self.dry_run = args.dry_run
        self.dry_run_recv = "-n" if args.dry_run else ""
        self.dry_run_destroy = self.dry_run_recv
        self.verbose = "-v" if args.verbose >= 1 else ""
        self.verbose_zfs = True if args.verbose >= 2 else False
        self.quiet = "" if args.quiet else "-v"
        self.verbose_destroy = self.quiet
        self.verbose_trace = True if args.verbose >= 2 else False
        self.enable_privilege_elevation = not args.no_privilege_elevation
        self.exclude_dataset_regexes = None  # deferred to validate() phase
        self.include_dataset_regexes = None
        self.exclude_snapshot_regexes = None
        self.include_snapshot_regexes = None
        self.zfs_create_program_opts = self.split_args(self.getenv('zfs_create_program_opts', ''))
        self.zfs_send_program_opts = self.split_args(self.getenv('zfs_send_program_opts', '--props --raw --compressed'))
        self.zfs_recv_program_opts = self.split_args(self.getenv('zfs_recv_program_opts', '-u'))
        self.zfs_full_recv_opts = self.zfs_recv_program_opts.copy()
        if self.verbose_zfs:
            self.zfs_send_program_opts += ['-v']
            self.zfs_recv_program_opts += ['-v']
        self.timestamp = datetime.now().strftime('%Y_%m_%d__%H_%M_%S')
        self.home_dir = get_home_directory()
        self.log_dir = self.validate_arg(self.getenv('log_dir', f"{self.home_dir}/{prog_name}-logs"))
        os.makedirs(self.log_dir, exist_ok=True)
        fd, self.log_file = tempfile.mkstemp(suffix='.log', prefix=f"{self.timestamp}__", dir=self.log_dir)
        os.close(fd)
        fd, self.pv_log_file = tempfile.mkstemp(suffix='.pv', prefix=f"{self.timestamp}__", dir=self.log_dir)
        os.close(fd)
        self.pv_program = self.program_name('pv')
        self.pv_program_opts = self.split_args(self.getenv(
            'pv_program_opts', '--progress --timer --eta --rate --bytes --interval=1 --width=100 --buffer-size=1M'))
        if args.bwlimit:
            self.pv_program_opts = [f"--rate-limit={self.validate_arg(args.bwlimit.strip())}"] + self.pv_program_opts
        self.mbuffer_program = self.program_name('mbuffer')
        self.mbuffer_program_opts = self.split_args(self.getenv('mbuffer_program_opts', '-q -Q -m 128M'))
        self.compression_program = self.program_name('zstd')
        self.compression_program_opts = self.split_args(self.getenv('compression_program_opts', '-1'))
        # no point trying to be fancy for smaller data transfers:
        self.min_transfer_size = int(self.getenv('min_transfer_size', 1024 * 1024))

        self.ssh_config_file = self.validate_arg(args.ssh_config_file)
        self.ssh_private_key_file = self.validate_arg(args.ssh_private_key)
        self.ssh_src_user = args.ssh_src_user
        self.ssh_dst_user = args.ssh_dst_user
        self.ssh_src_host = args.ssh_src_host
        self.ssh_dst_host = args.ssh_dst_host
        self.ssh_src_port = args.ssh_src_port
        self.ssh_dst_port = args.ssh_dst_port
        self.ssh_src_user_host = None
        self.ssh_dst_user_host = None
        self.ssh_src_cmd = None
        self.ssh_dst_cmd = None

        self.src_pool = None
        self.dst_pool = None
        self.src_sudo = None
        self.dst_sudo = None

        self.ssh_default_opts = ['-o', 'ServerAliveInterval=0']
        self.ssh_src_extra_opts = ['-x', '-T']
        self.ssh_dst_extra_opts = self.ssh_src_extra_opts.copy()
        for extra_opt in args.ssh_src_extra_opt:
            self.ssh_src_extra_opts += self.split_args(extra_opt)
        for extra_opt in args.ssh_dst_extra_opt:
            self.ssh_dst_extra_opts += self.split_args(extra_opt)
        self.ssh_socket_enabled = self.getenv_bool('ssh_socket_enabled', True)
        self.ssh_cipher = self.validate_arg(args.ssh_cipher)  # for speed with confidentiality and integrity
        # measure cipher perf like so: count=5000; for i in $(seq 1 3); do echo "iteration $i:"; for cipher in $(ssh -Q cipher); do dd if=/dev/zero bs=1M count=$count 2> /dev/null | ssh -c $cipher -p 40999 localhost "(time -p cat) > /dev/null" 2>&1 | grep real | awk -v count=$count -v cipher=$cipher '{print cipher ": " count / $2 " MB/s"}'; done; done
        # see https://gbe0.com/posts/linux/server/benchmark-ssh-ciphers/
        # and https://crypto.stackexchange.com/questions/43287/what-are-the-differences-between-these-aes-ciphers

        self.zfs_program = self.program_name('zfs')
        self.zpool_program = self.program_name('zpool')
        self.ssh_program = self.program_name('ssh')
        self.sudo_program = self.program_name('sudo')
        self.shell_program_local = 'sh'
        self.shell_program = self.program_name(self.shell_program_local)
        self.uname_program = self.program_name('uname')

        self.max_retries = args.max_retries
        self.min_sleep_secs = float(self.getenv('min_sleep_secs', 0.125))
        self.max_sleep_secs = float(self.getenv('max_sleep_secs', 5 * 60))
        self.max_elapsed_secs = float(self.getenv('max_elapsed_secs', 3600))
        self.min_sleep_nanos = int(self.min_sleep_secs * 1000_000_000)
        self.max_sleep_nanos = int(self.max_sleep_secs * 1000_000_000)
        self.max_elapsed_nanos = int(self.max_elapsed_secs * 1000_000_000)
        self.min_sleep_nanos = max(1, self.min_sleep_nanos)
        self.max_sleep_nanos = max(self.min_sleep_nanos, self.max_sleep_nanos)
        self.debug_retries = self.getenv_bool('debug_retries', False)
        self.is_test_mode = args.is_test_mode

        self.available_programs = {}
        self.zpool_features = {}

        self.os_geteuid = os.geteuid()
        self.prog_version = prog_version
        self.python_version = sys.version
        self.platform_version = platform.version()
        self.platform_platform = platform.platform()

    def getenv(self, key, default=None):
        # All shell environment variable names used for configuration start with this prefix
        return os.getenv('wbackup_zfs_' + key, default)

    def getenv_bool(self, key, default=False):
        return self.getenv(key, str(default).lower()).strip().lower() == "true"

    def split_args(self, text: str, *items) -> List[str]:
        """ split option string on runs of one or more whitespace into an option list """
        opts = xappend(re.split(r'\s+', text.strip()), items)
        for opt in opts:
            self.validate_quoting(opt)
        return opts

    def validate_arg(self, opt: str):
        if opt is None:
            return opt
        if any(char.isspace() for char in opt):
            die(f"Option must not contain a whitespace character: {opt}")
        return self.validate_quoting(opt)

    def validate_quoting(self, opt: str):
        if any(c in opt for c in ["'", '"']):
            die(f"Option must not contain a single quote or a double quote character: {opt}")
        return opt

    def program_name(self, program: str) -> str:
        """For testing: help simulate errors caused by external programs"""
        if self.getenv_bool('inject_unavailable_' + program, False):
            return 'xxx-' + program  # substitute a program that cannot be found on the PATH
        if self.getenv_bool('inject_failing_' + program, False):
            return 'false'  # substitute a program that will error out with non-zero return code
        else:
            return program


#############################################################################
def main():
    """API for command line clients """
    try:
        run_main(argument_parser().parse_args(), sys.argv)
    except subprocess.CalledProcessError as e:
        sys.exit(e.returncode)


def run_main(args: argparse.Namespace, sys_argv: Optional[List[str]] = None):
    """API for Python clients; visible for testing; may become a public API eventually """
    Job().run_main(args, sys_argv)


#############################################################################
class Job:
    def __init__(self):
        self.params = None
        self.dst_dataset_exists = defaultdict(bool)  # returns False for absent keys
        self.recordsizes = None
        self.mbuffer_current_opts = None

    def run_main(self, args: argparse.Namespace, sys_argv: Optional[List[str]] = None):
        self.__init__()
        self.params = Params(args, sys_argv)
        params = self.params
        create_symlink(params.log_file, params.log_dir, 'current.log')
        self.info_raw("Log file is: " + params.log_file)
        create_symlink(params.pv_log_file, params.log_dir, 'current.pv')

        with open(params.log_file, 'a', encoding='utf-8') as log_fileFD:
            with redirect_stdout(Tee(log_fileFD, sys.stdout)), redirect_stderr(Tee(log_fileFD, sys.stderr)):
                try:
                    self.validate()
                    self.run_main_action()
                except subprocess.CalledProcessError as e:
                    error(f"Exiting with status code: {e.returncode}")
                    raise e
                except SystemExit as e:
                    error(f"Exiting with status code: {e.code}")
                    raise e

                for line in tail(params.pv_log_file, 10):
                    print(line, end='')
                self.info_raw("Success. Goodbye!")
        self.info_raw("Log file was: " + params.log_file)

    def validate(self):
        params = self.params
        p = params
        self.info("CLI arguments:", ' '.join(params.sys_argv), f"[euid: {os.geteuid()}]")
        self.debug("Parsed CLI arguments:", str(params.args))

        validate_user_name(params.ssh_src_user, "--ssh-src-user")
        validate_user_name(params.ssh_dst_user, "--ssh-dst-user")
        validate_host_name(params.ssh_src_host, "--ssh-src-host")
        validate_host_name(params.ssh_dst_host, "--ssh-dst-host")
        validate_port(params.ssh_src_port, "--ssh-src-port ")
        validate_port(params.ssh_dst_port, "--ssh-dst-port ")

        p.ssh_src_user, p.ssh_src_host, p.ssh_src_user_host, p.src_pool, p.src_root_dataset = \
            parse_dataset_locator(p.src_root_dataset, user=p.ssh_src_user, host=p.ssh_src_host, port=p.ssh_src_port)

        p.ssh_dst_user, p.ssh_dst_host, p.ssh_dst_user_host, p.dst_pool, p.dst_root_dataset = \
            parse_dataset_locator(p.dst_root_dataset, user=p.ssh_dst_user, host=p.ssh_dst_host, port=p.ssh_dst_port)

        if p.ssh_src_host == p.ssh_dst_host:
            if p.src_root_dataset == p.dst_root_dataset:
                die(f"Source and destination dataset must not be the same! "
                    f"src: {p.origin_src_root_dataset}, dst: {p.origin_dst_root_dataset}")
            if p.recursive:
                if (f"{p.src_root_dataset}/".startswith(f"{p.dst_root_dataset}/")
                        or f"{p.dst_root_dataset}/".startswith(f"{p.src_root_dataset}/")):
                    die(f"Source and destination dataset trees must not overlap! "
                        f"src: {p.origin_src_root_dataset}, dst: {p.origin_dst_root_dataset}")

        if params.ssh_src_user_host == "" and params.ssh_dst_user_host == "" and params.debug_retries is False:
            params.max_retries = 0  # no need to retry transfer from local disk to local disk

        re_suffix = r'(?:/.*)?'  # also match descendants of a matching dataset
        exclude_regexes = self.dataset_regexes(p.args.exclude_dataset or []) + (p.args.exclude_dataset_regex or [])
        include_regexes = self.dataset_regexes(p.args.include_dataset or []) + (p.args.include_dataset_regex or [])
        p.exclude_dataset_regexes = compile_regexes(exclude_regexes or [exclude_dataset_regexes_default], suffix=re_suffix)
        p.include_dataset_regexes = compile_regexes(include_regexes or ['.*'], suffix=re_suffix)
        p.exclude_snapshot_regexes = compile_regexes(p.args.exclude_snapshot_regex or [])
        p.include_snapshot_regexes = compile_regexes(p.args.include_snapshot_regex or ['.*'])

        params.src_sudo = self.sudo_cmd(params.ssh_src_user_host, params.ssh_src_user)
        params.dst_sudo = self.sudo_cmd(params.ssh_dst_user_host, params.ssh_dst_user)

        p.ssh_src_cmd = self.ssh_command(p.ssh_src_user, p.ssh_src_host, p.ssh_src_user_host, p.ssh_src_port,
                                         p.ssh_default_opts + p.ssh_src_extra_opts)
        p.ssh_dst_cmd = self.ssh_command(p.ssh_dst_user, p.ssh_dst_host, p.ssh_dst_user_host, p.ssh_dst_port,
                                         p.ssh_default_opts + p.ssh_dst_extra_opts)

        try:
            self.detect_available_programs()
        finally:
            self.trace("Validated Param values:", pprint.pformat(vars(params)))

        cmd = p.split_args(f"{p.zfs_program} list -t filesystem -Hp -o name -s name", p.src_pool)
        if self.run_ssh_command('src', self.trace, check=False, cmd=cmd) != p.src_pool + '\n':
            die(f"Pool does not exist for source dataset: {p.origin_src_root_dataset}. Manually create the pool first!")

        cmd = p.split_args(f"{p.zfs_program} list -t filesystem -Hp -o name -s name", p.dst_pool)
        if self.run_ssh_command('dst', self.trace, check=False, cmd=cmd) != p.dst_pool + '\n':
            die(f"Pool does not exist for destination dataset: {p.origin_dst_root_dataset}. "
                f"Manually create the pool first!")

        self.detect_zpool_features('src', p.src_pool)
        self.detect_zpool_features('dst', p.dst_pool)

        if self.is_zpool_feature_enabled_or_active('dst', 'feature@large_blocks'):
            append_if_absent(p.zfs_send_program_opts, '--large-block')  # solaris-11.4.0 does not have this feature

        if self.is_solaris_zfs('dst'):
            self.params.dry_run_destroy = ""  # solaris-11.4.0 knows no 'zfs destroy -n' flag
            self.params.verbose_destroy = ""  # solaris-11.4.0 knows no 'zfs destroy -v' flag
        if self.is_solaris_zfs('src'):  # solaris-11.4.0 only knows -w compress
            p.zfs_send_program_opts = ['-w' if opt == '--raw' else opt for opt in p.zfs_send_program_opts]
            p.zfs_send_program_opts = ['compress' if opt == '--compressed' else opt for opt in p.zfs_send_program_opts]
            p.zfs_send_program_opts = ['-p' if opt == '--props' else opt for opt in p.zfs_send_program_opts]

    def sudo_cmd(self, ssh_user_host: str, ssh_user: str) -> str:
        params = self.params
        sudo = ""
        if ssh_user_host != "":
            if params.enable_privilege_elevation:
                # attempt to make ZFS operations work even if we are not root user
                if not ssh_user:
                    if os.geteuid() != 0:
                        sudo = params.sudo_program
                elif ssh_user != "root":
                    sudo = params.sudo_program
        elif params.enable_privilege_elevation and os.geteuid() != 0:
            sudo = params.sudo_program  # attempt to make ZFS operations work even if we are not root user
        return sudo

    def run_main_action(self):
        params = self.params
        p = params
        self.info("ZFS source --> destination:",
                  f"{p.origin_src_root_dataset} {p.recursive_flag} --> {p.origin_dst_root_dataset}  ...")

        # find src dataset or all datasets in src dataset tree (with --recursive)
        cmd = p.split_args(f"{p.zfs_program} list -t filesystem -Hp -o recordsize,name",
                           p.recursive_flag, p.src_root_dataset)
        src_datasets_with_record_sizes = self.run_ssh_command('src', self.trace, check=False, cmd=cmd)
        # debug("src_datasets_with_record_sizes:" + src_datasets_with_record_sizes)
        src_datasets_with_record_sizes = src_datasets_with_record_sizes.splitlines()
        self.recordsizes = {}
        for line in src_datasets_with_record_sizes:
            recordsize, src_dataset = line.split('\t', 1)
            self.recordsizes[src_dataset] = int(recordsize)

        src_datasets = {line[line.index('\t') + 1:] for line in src_datasets_with_record_sizes}
        origin_src_datasets = src_datasets
        src_datasets = isorted(self.filter_datasets(src_datasets, p.src_root_dataset))

        # Optionally, replicate src_root_dataset (optionally including its descendants) to dst_root_dataset
        if not params.skip_replication:
            self.info("ZFS dataset replication:",
                      f"{p.origin_src_root_dataset} {p.recursive_flag} --> {p.origin_dst_root_dataset}  ...")
            if len(origin_src_datasets) == 0:
                die(f"Source dataset does not exist: {params.origin_src_root_dataset}")
            self.debug("Retry policy:", f"max_retries: {p.max_retries}, min_sleep_secs: {p.min_sleep_secs}, "
                       f"max_sleep_secs: {p.max_sleep_secs}, max_elapsed_secs: {p.max_elapsed_secs}")

            skip_src_dataset = ""
            for src_dataset in src_datasets:
                if f"{src_dataset}/".startswith(f"{skip_src_dataset}/"):
                    # skip_src_dataset shall be ignored or has been deleted by some third party while we're running
                    continue  # nothing to do anymore for this dataset subtree (note that src_datasets is sorted)
                skip_src_dataset = ""
                dst_dataset = p.dst_root_dataset + relativize_dataset(src_dataset, p.src_root_dataset)
                self.debug("Replicating:", f"{src_dataset} --> {dst_dataset} ...")
                self.mbuffer_current_opts = (['-s', str(max(128 * 1024, self.recordsizes[src_dataset]))]
                                             + p.mbuffer_program_opts)
                if not self.run_with_retries(self.replicate_flat_dataset, src_dataset, dst_dataset):
                    skip_src_dataset = src_dataset

        # Optionally, delete existing destination snapshots that do not exist within the source dataset if they
        # match at least one of --include-snapshot-regex but none of --exclude-snapshot-regex and the destination
        # dataset is included via --{include|exclude}-dataset-regex --{include|exclude}-dataset policy
        if params.delete_missing_snapshots:
            self.info("--delete-missing-snapshots:",
                      f"{p.origin_src_root_dataset} {p.recursive_flag} --> {p.origin_dst_root_dataset}  ...")
            for src_dataset in src_datasets:
                dst_dataset = p.dst_root_dataset + relativize_dataset(src_dataset, p.src_root_dataset)
                cmd = p.split_args(f"{p.zfs_program} list -t snapshot -d 1 -s name -Hp -o guid,name", dst_dataset)
                dst_snapshots_with_guids = self.run_ssh_command('dst', self.trace, check=False, cmd=cmd)
                dst_snapshots_with_guids = self.filter_snapshots(dst_snapshots_with_guids.splitlines())
                cmd = p.split_args(f"{p.zfs_program} list -t snapshot -d 1 -s name -Hp -o guid,name", src_dataset)
                src_snapshots_with_guids = self.run_ssh_command('src', self.trace, cmd=cmd).splitlines()
                src_snapshots_with_guids = self.filter_snapshots(src_snapshots_with_guids)

                missing_snapshot_guids = set(cut(field=1, lines=dst_snapshots_with_guids)).difference(
                    set(cut(1, lines=src_snapshots_with_guids)))
                missing_snapshot_tags = self.filter_lines(dst_snapshots_with_guids, missing_snapshot_guids)
                missing_snapshot_tags = cut(2, separator='@', lines=missing_snapshot_tags)
                self.delete_snapshots(dst_dataset, snapshot_tags=missing_snapshot_tags)

        # Optionally, delete existing destination datasets that do not exist within the source dataset if they are
        # included via --{include|exclude}-dataset-regex --{include|exclude}-dataset policy.
        # Also delete an existing destination dataset that has no snapshot if all descendants of that dataset do not
        # have a snapshot either (again, only if the existing destination dataset is included via
        # --{include|exclude}-dataset-regex --{include|exclude}-dataset policy). Does not recurse without --recursive.
        if params.delete_missing_datasets:
            self.info("--delete-missing-datasets:",
                      f"{p.origin_src_root_dataset} {p.recursive_flag} --> {p.origin_dst_root_dataset}  ...")
            cmd = p.split_args(f"{p.zfs_program} list -t filesystem -Hp -o name", p.recursive_flag, p.dst_root_dataset)
            dst_datasets = self.run_ssh_command('dst', self.trace, check=False, cmd=cmd).splitlines()
            dst_datasets = set(self.filter_datasets(dst_datasets, p.dst_root_dataset))
            origins = {replace_prefix(src_ds, p.src_root_dataset, p.dst_root_dataset) for src_ds in origin_src_datasets}
            to_delete = dst_datasets.difference(origins)
            self.delete_datasets(to_delete)

            # Optionally, delete any existing destination dataset that has no snapshot if all descendants of that dataset do
            # not have a snapshot either. To do so, we walk the dataset list (conceptually, a tree) depth-first (i.e. sorted
            # descending). If a dst dataset has zero snapshots and all its children are already marked as orphans, then it
            # is itself an orphan, and we mark it as such. Walking in a reverse sorted way means that we efficiently check
            # for zero snapshots not just over the direct children but the entire tree. Finally, delete all orphan datasets
            # in an efficient batched way.
            if p.delete_empty_datasets:
                self.info("--delete-empty-datasets:",
                          f"{p.origin_src_root_dataset} {p.recursive_flag} --> {p.origin_dst_root_dataset}  ...")
                dst_datasets = isorted(dst_datasets.difference(to_delete))

                # preparation: compute the direct children of each dataset
                children = defaultdict(list)
                for dst_dataset in dst_datasets:
                    parent = os.path.dirname(dst_dataset)
                    children[parent].append(dst_dataset)

                # find and mark orphan datasets
                orphans = set()
                for dst_dataset in reversed(dst_datasets):
                    if not any(filter(lambda child: child not in orphans, children[dst_dataset])):
                        # no child of the dataset turned out to be an orphan so the dataset itself could be an orphan
                        cmd = p.split_args(f"{p.zfs_program} list -t snapshot -d 1 -Hp -o name", dst_dataset)
                        if not self.run_ssh_command('dst', self.trace, check=False, cmd=cmd):
                            orphans.add(dst_dataset)  # found zero snapshots - mark dataset as an orphan

                self.delete_datasets(orphans)

    def replicate_flat_dataset(self, src_dataset: str, dst_dataset: str):
        """Replicate src_dataset (without handling descendants) to dst_dataset"""

        # list GUID and name for dst snapshots, sorted ascending by txn (more precise than creation time)
        params = self.params
        p = params
        cmd = p.split_args(f"{p.zfs_program} list -t snapshot -d 1 -s createtxg -Hp -o guid,name", dst_dataset)
        dst_snapshots_with_guids = self.run_ssh_command('dst', self.trace, check=None, cmd=cmd)
        self.dst_dataset_exists[dst_dataset] = dst_snapshots_with_guids is not None
        dst_snapshots_with_guids = dst_snapshots_with_guids.splitlines() if dst_snapshots_with_guids is not None else []

        oldest_dst_snapshot_createtxg = None
        latest_dst_snapshot_createtxg = None
        if len(dst_snapshots_with_guids) > 0 and params.use_bookmark:
            if self.is_zpool_bookmarks_feature_enabled_or_active('src'):
                oldest_dst_snapshot_createtxg = dst_snapshots_with_guids[0].split('\t', 1)[0]
                latest_dst_snapshot_createtxg = dst_snapshots_with_guids[-1].split('\t', 1)[0]

        # list GUID and name for src snapshots + bookmarks, primarily sort ascending by transaction group (which is more
        # precise than creation time), secondarily sort such that snapshots appear before bookmarks for the same GUID.
        # Note: A snapshot and its ZFS bookmarks always have the same GUID as well as the same transaction group.
        props = "guid,name"
        types = "snapshot,bookmark"
        if oldest_dst_snapshot_createtxg is None:
            types = "snapshot"
        cmd = p.split_args(f"{p.zfs_program} list -t {types} -s createtxg -S type -d 1 -Hp -o {props}", src_dataset)
        src_snapshots_and_bookmarks = self.run_ssh_command('src', self.trace, check=None, cmd=cmd)
        if src_snapshots_and_bookmarks is None:
            self.warn("Third party deleted source:", src_dataset)
            return False  # src dataset has been deleted by some third party while we're running - nothing to do anymore

        src_snapshots_and_bookmarks = src_snapshots_and_bookmarks.splitlines()
        if oldest_dst_snapshot_createtxg is not None:
            src_snapshots_and_bookmarks = self.filter_bookmarks(src_snapshots_and_bookmarks,
                                                                oldest_dst_snapshot_createtxg,
                                                                latest_dst_snapshot_createtxg)
        origin_src_snapshots_and_bookmarks = src_snapshots_and_bookmarks
        src_snapshots_and_bookmarks = self.filter_snapshots(src_snapshots_and_bookmarks)

        # ignore src bookmarks for which a src snapshot exists (where snapshot and its bookmarks have the same GUID)
        # otherwise treat bookmarks as snapshots without loosing track of what's really a snapshot or a bookmark
        src_snapshots_with_guids = []
        oldest_src_snapshot = ""
        latest_src_snapshot = ""
        bookmarks = {}
        has_snapshot = set()
        for line in src_snapshots_and_bookmarks:
            guid, snapshot = line.split('\t', 1)
            if '@' in snapshot:
                src_snapshots_with_guids.append(line)
                has_snapshot.add(guid)
                latest_src_snapshot = snapshot
                if oldest_src_snapshot == "":
                    oldest_src_snapshot = snapshot
            elif guid not in has_snapshot:
                # it's a bookmark - and the snapshot corresponding to this bookmark is missing aka snap has been deleted
                bookmark = snapshot
                snapshot = snapshot.replace('#', '@', 1)
                if bookmarks.get(snapshot) is None:  # not yet seen?
                    bookmarks[snapshot] = bookmark
                    tab = '\t'
                    src_snapshots_with_guids.append(f"{guid}{tab}{snapshot}")
        src_snapshots_and_bookmarks = None
        has_snapshot = None
        if not latest_src_snapshot and params.recursive:
            if params.skip_missing_snapshots is None:
                die(f"Found source dataset that has no snapshot: {src_dataset}. Consider "
                    f"using --skip-missing-snapshots=true")
            elif params.skip_missing_snapshots:
                self.warn("Skipping source dataset and its descendants because it has no snapshot:", src_dataset)
                return False

        self.debug("latest_src_snapshot:", latest_src_snapshot)
        latest_dst_snapshot = ""
        latest_common_src_snapshot = ""
        latest_common_dst_snapshot = ""
        origin_dst_snapshots_with_guids = dst_snapshots_with_guids

        if self.dst_dataset_exists[dst_dataset]:
            dst_snapshots_with_guids = self.filter_snapshots(dst_snapshots_with_guids)
            dst_snapshots = [line[line.index('\t') + 1:] for line in dst_snapshots_with_guids]  # cut -f2-
            latest_dst_snapshot = dst_snapshots[-1] if len(dst_snapshots) > 0 else ""
            if latest_dst_snapshot != "" and params.force:
                self.info("Rolling back dst to most recent snapshot:", latest_dst_snapshot)
                # rollback just in case the dst dataset was modified since its most recent snapshot
                cmd = p.split_args(f"{p.dst_sudo} {p.zfs_program} rollback", latest_dst_snapshot)
                self.run_ssh_command('dst', self.debug, is_dry=p.dry_run, cmd=cmd)
            if latest_src_snapshot == "" and latest_dst_snapshot == "":
                self.info("Already-up-to-date:", dst_dataset)
                return True

            # find most recent snapshot that $src_dataset and $dst_dataset have in common - we'll start to replicate
            # from there up to the most recent src snapshot. any two snapshots are "common" iff their ZFS GUIDs (i.e.
            # contents) are equal. See https://github.com/openzfs/zfs/commit/305bc4b370b20de81eaf10a1cf724374258b74d1
            common_snapshot_guids = set(cut(field=1, lines=dst_snapshots_with_guids)).intersection(
                set(cut(1, lines=src_snapshots_with_guids)))
            common_src_snapshot_tags = self.filter_lines(src_snapshots_with_guids, common_snapshot_guids)
            common_src_snapshot_tags = cut(2, separator='@', lines=common_src_snapshot_tags)
            common_dst_snapshot_tags = self.filter_lines(dst_snapshots_with_guids, common_snapshot_guids)
            common_dst_snapshot_tags = cut(2, separator='@', lines=common_dst_snapshot_tags)
            if len(common_snapshot_guids) > 0:
                # debug("common_src_snapshot_tags:", str(common_src_snapshot_tags))
                latest_common_src_snapshot = f"{src_dataset}@{common_src_snapshot_tags[-1]}"
                latest_common_dst_snapshot = f"{dst_dataset}@{common_dst_snapshot_tags[-1]}"

            self.debug("latest_dst_snapshot:", latest_dst_snapshot)
            self.debug("latest_common_src_snapshot:", self.snapshot_or_bookmark(latest_common_src_snapshot, bookmarks))

            if latest_common_src_snapshot:
                # common snapshot was found. rollback dst to that common snapshot
                if latest_common_dst_snapshot != latest_dst_snapshot:
                    if not params.force:
                        die(f"Conflict: Most recent destination snapshot {latest_dst_snapshot} is more recent than " 
                            f"most recent common snapshot {latest_common_dst_snapshot}. Rollback destination first, "
                            f"for example via --force option.")
                    if params.force_once:
                        params.force = False
                    self.info("Rolling back dst to most recent common snapshot:", latest_common_dst_snapshot)
                    cmd = p.split_args(f"{p.dst_sudo} {p.zfs_program} rollback -r {p.force_hard}",
                                       latest_common_dst_snapshot)
                    self.run_ssh_command('dst', self.debug, is_dry=params.dry_run, cmd=cmd)
                    latest_dst_snapshot = latest_common_dst_snapshot

            if latest_src_snapshot and latest_src_snapshot == latest_common_src_snapshot:
                self.info("Already up-to-date:", dst_dataset)
                return True
        self.debug("latest_dst_snapshot:", latest_dst_snapshot)
        self.debug("latest_common_src_snapshot:", self.snapshot_or_bookmark(latest_common_src_snapshot, bookmarks))

        is_dry_send_receive = False
        if not latest_common_src_snapshot:
            # no common snapshot was found. delete all dst snapshots, if any
            if latest_dst_snapshot:
                if not params.force:
                    die(f"Conflict: No common snapshot found between {src_dataset} and {dst_dataset}. "
                        f"Consider using --force option.")
                if params.force_once:
                    params.force = False
                if self.is_solaris_zfs('dst'):
                    # solaris-11.4.0 has no wildcard syntax to delete all snapshots in a single CLI invocation
                    self.delete_snapshots(
                        dst_dataset, snapshot_tags=cut(2, separator='@', lines=origin_dst_snapshots_with_guids))
                else:
                    cmd = p.split_args(
                        f"{p.dst_sudo} {p.zfs_program} destroy {p.force_hard} {p.verbose_destroy} {p.dry_run_destroy}",
                        f"{dst_dataset}@%")
                    self.run_ssh_command('dst', self.debug, cmd=cmd)  # delete all dst snapshots in a batch
                if params.dry_run:
                    # As we're in --dry-run (--force) mode this conflict resolution step (see above) wasn't really
                    # executed: "no common snapshot was found. delete all dst snapshots". In turn, this would cause the
                    # subsequent 'zfs receive' to fail with "cannot receive new filesystem stream: destination has
                    # snapshots; must destroy them to overwrite it". So we skip the zfs send/receive step and keep on
                    # trucking.
                    is_dry_send_receive = True

            # to start with, fully replicate oldest snapshot, which in turn creates a common snapshot
            if params.no_stream:
                oldest_src_snapshot = latest_src_snapshot
            if oldest_src_snapshot:
                if not self.dst_dataset_exists[dst_dataset]:
                    # on destination, create parent dataset and ancestors if they do not yet exist
                    dst_dataset_parent = os.path.dirname(dst_dataset)
                    if not self.dst_dataset_exists[dst_dataset_parent]:
                        if params.dry_run:
                            is_dry_send_receive = True
                        elif dst_dataset_parent != "":
                            self.create_dataset(dst_dataset_parent)

                size_estimate_bytes = self.estimate_zfs_send_size(oldest_src_snapshot)
                size_estimate_human = human_readable_bytes(size_estimate_bytes)
                send_cmd = p.split_args(f"{p.src_sudo} {p.zfs_program} send", p.zfs_send_program_opts, oldest_src_snapshot)
                recv_opts = p.zfs_full_recv_opts.copy()
                if p.getenv_bool('preserve_recordsize', False):
                    recv_opts += ['-o', f"recordsize={self.recordsizes[src_dataset]}"]
                recv_cmd = p.split_args(f"{p.dst_sudo} {p.zfs_program} receive -F", p.dry_run_recv, recv_opts, dst_dataset)
                self.info("Full zfs send:", f"{oldest_src_snapshot} --> {dst_dataset} ({size_estimate_human}) ...")
                self.run_zfs_send_receive(send_cmd, recv_cmd, size_estimate_bytes, size_estimate_human, is_dry_send_receive)

                latest_common_src_snapshot = oldest_src_snapshot  # we have now created a common snapshot
                if not is_dry_send_receive and not params.dry_run:
                    self.dst_dataset_exists[dst_dataset] = True
                self.create_zfs_bookmark(oldest_src_snapshot, bookmarks, src_dataset)

        # finally, replicate all snapshots from most recent common snapshot until most recent src snapshot
        if latest_common_src_snapshot:
            if latest_common_src_snapshot != latest_src_snapshot and latest_src_snapshot != "":
                # TODO handle case if there are excluded snapshots between "$latest_common_src_snapshot" and "$latest_src_snapshot"
                latest_common_src_snapshot_index = None
                latest_src_snapshot_index = None
                for latest_common_src_snapshot_index, snapshot_with_guid in enumerate(reversed(origin_src_snapshots_and_bookmarks)):
                    snapshot = snapshot_with_guid.split('\t', 1)[1]
                    if latest_src_snapshot_index is None and snapshot == latest_src_snapshot:
                        latest_src_snapshot_index = latest_common_src_snapshot_index
                    if snapshot == latest_common_src_snapshot:
                        break
                latest_common_src_snapshot_index = len(origin_src_snapshots_and_bookmarks) - 1 - latest_common_src_snapshot_index
                assert 0 <= latest_common_src_snapshot_index < len(origin_src_snapshots_and_bookmarks)
                latest_src_snapshot_index = len(origin_src_snapshots_and_bookmarks) - 1 - latest_src_snapshot_index
                assert 0 <= latest_src_snapshot_index < len(origin_src_snapshots_and_bookmarks)

                incr_flag = "-I"  # include intermediate snapshots
                if p.no_stream:
                    incr_flag = "-i"  # skip intermediate snapshots
                bookmark = bookmarks.get(latest_common_src_snapshot)
                if bookmark:
                    # 'zfs send' with bookmark source does not (yet) support including intermediate snapshots via -I flag
                    # TODO convert: run -i $start $middle (if start==end is a noop) followed by run -I $middle $end
                    incr_flag = "-i"  # skip intermediate snapshots
                    latest_common_src_snapshot = bookmark  # aka change # to @ in latest_common_src_snapshot
                    self.info("Incremental zfs send using bookmark as source ...")

                size_estimate_bytes = self.estimate_zfs_send_size(incr_flag, latest_common_src_snapshot, latest_src_snapshot)
                size_estimate_human = human_readable_bytes(size_estimate_bytes)
                send_cmd = p.split_args(f"{p.src_sudo} {p.zfs_program} send", p.zfs_send_program_opts, incr_flag, latest_common_src_snapshot, latest_src_snapshot)
                recv_cmd = p.split_args(f"{p.dst_sudo} {p.zfs_program} receive", p.dry_run_recv, p.zfs_recv_program_opts, dst_dataset)
                self.info(f"Incremental zfs send: {incr_flag}", f"{latest_common_src_snapshot} {latest_src_snapshot} --> {dst_dataset} ({size_estimate_human}) ...")
                if p.dry_run and not self.dst_dataset_exists[dst_dataset]:
                    is_dry_send_receive = True
                self.run_zfs_send_receive(send_cmd, recv_cmd, size_estimate_bytes, size_estimate_human, is_dry_send_receive)

                self.create_zfs_bookmark(latest_common_src_snapshot, bookmarks, src_dataset)
                self.create_zfs_bookmark(latest_src_snapshot, bookmarks, src_dataset)

        return True

    def snapshot_or_bookmark(self, snapshot: str, bookmarks: Dict[str, str]):
        bookmark = bookmarks.get(snapshot)
        if bookmark:
            return bookmark
        else:
            return snapshot

    def run_zfs_send_receive(self, send_cmd: List[str], recv_cmd: List[str],
                             size_estimate_bytes: int, size_estimate_human: str, is_dry_send_receive: bool):
        params = self.params
        send_cmd = ' '.join([self.cquote(item) for item in send_cmd])
        recv_cmd = ' '.join([self.cquote(item) for item in recv_cmd])

        _compress_cmd = self.compress_cmd('src', size_estimate_bytes)
        _decompress_cmd = self.decompress_cmd('dst', size_estimate_bytes)
        src_buffer = self.mbuffer_cmd('src', size_estimate_bytes)
        dst_buffer = self.mbuffer_cmd('dst', size_estimate_bytes)
        local_buffer = self.mbuffer_cmd('local', size_estimate_bytes)

        pv_src_cmd = ""
        pv_dst_cmd = ""
        pv_loc_cmd = ""
        if params.ssh_src_user_host == "":
            pv_src_cmd = self.pv_cmd('local', size_estimate_bytes, size_estimate_human)
        elif params.ssh_dst_user_host == "":
            pv_dst_cmd = self.pv_cmd('local', size_estimate_bytes, size_estimate_human)
        elif _compress_cmd == 'cat':
            pv_loc_cmd = self.pv_cmd('local', size_estimate_bytes, size_estimate_human)  # compression disabled
        else:
            # pull-push mode with compression enabled: reporting "percent complete" isn't straightforward because
            # localhost observes the compressed data instead of the uncompressed data, so we disable the progress bar.
            pv_loc_cmd = self.pv_cmd('local', size_estimate_bytes, size_estimate_human, disable_progress_bar=True)

        # assemble pipeline running on source leg
        src_pipe = ""
        if params.getenv_bool('inject_src_pipe_fail', False):
            src_pipe = f"{src_pipe} | head -c 64 && false"  # for testing; initially forward some bytes and then fail
        if params.getenv_bool('inject_src_pipe_garble', False):
            src_pipe = f"{src_pipe} | base64"  # for testing; forward garbled bytes
        if pv_src_cmd != "" and pv_src_cmd != "cat":
            src_pipe = f"{src_pipe} | {pv_src_cmd}"
        if _compress_cmd != "cat":
            src_pipe = f"{src_pipe} | {_compress_cmd}"
        if src_buffer != "cat":
            src_pipe = f"{src_pipe} | {src_buffer}"
        if src_pipe.startswith(" |"):
            src_pipe = src_pipe[2:]  # strip leading ' |' part
        if params.getenv_bool('inject_src_send_error', False):
            send_cmd = f"{send_cmd} --injectedGarbageParameter"  # for testing; induce CLI parse error
        if src_pipe != "":
            src_shell_program = params.shell_program if len(params.ssh_src_cmd) > 0 else params.shell_program_local
            src_pipe = f'{src_shell_program} -c "{send_cmd} | {src_pipe}"'
        else:
            src_pipe = send_cmd

        # assemble pipeline running on middle leg between source and destination
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
            # local_pipe = self.cquote(local_pipe)
            local_pipe = f'| {params.shell_program_local} -c "{local_pipe}"'

        # assemble pipeline running on destination leg
        dst_pipe = ""
        if dst_buffer != "cat":
            dst_pipe = f"{dst_buffer}"
        if _decompress_cmd != "cat":
            dst_pipe = f"{dst_pipe} | {_decompress_cmd}"
        if pv_dst_cmd != "" and pv_dst_cmd != "cat":
            dst_pipe = f"{dst_pipe} | {pv_dst_cmd}"
        if params.getenv_bool('inject_dst_pipe_fail', False):
            dst_pipe = f"{dst_pipe} | head -c 64 && false"  # for testing; initially forward some bytes and then fail
        if params.getenv_bool('inject_dst_pipe_garble', False):
            dst_pipe = f"{dst_pipe} | base64"  # for testing; forward garbled bytes
        if dst_pipe.startswith(" |"):
            dst_pipe = dst_pipe[2:]  # strip leading ' |' part
        if params.getenv_bool('inject_dst_receive_error', False):
            recv_cmd = f"{recv_cmd} --injectedGarbageParameter"  # for testing; induce CLI parse error
        if dst_pipe != "":
            dst_shell_program = params.shell_program if len(params.ssh_dst_cmd) > 0 else params.shell_program_local
            dst_pipe = f'{dst_shell_program} -c "{dst_pipe} | {recv_cmd}"'
        else:
            dst_pipe = recv_cmd

        # If there's no support for shell pipelines, we can't do compression, mbuffering, monitoring and rate-limiting,
        # so we fall back to simple zfs send/receive.
        if not self.is_program_available('sh', 'src'):
            src_pipe = send_cmd
        if not self.is_program_available('sh', 'dst'):
            dst_pipe = recv_cmd
        if not self.is_program_available('sh', 'local'):
            local_pipe = ""

        src_pipe = self.dquote(params.ssh_src_cmd, src_pipe)
        dst_pipe = self.dquote(params.ssh_dst_cmd, dst_pipe)
        src_cmd = ' '.join([self.cquote(item) for item in params.ssh_src_cmd])
        dst_cmd = ' '.join([self.cquote(item) for item in params.ssh_dst_cmd])

        cmd = [params.shell_program_local, '-c', f"{src_cmd} {src_pipe} {local_pipe} | {dst_cmd} {dst_pipe}"]
        msg = "Would execute:" if is_dry_send_receive else "Executing:"
        self.debug(msg, ' '.join(cmd))
        if not is_dry_send_receive:
            try:
                subprocess.run(cmd, stdout=PIPE, check=True)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
                # op isn't idempotent so retries regather current state from the start of replicate_flat_dataset()
                raise RetryableError("Subprocess failed") from e

    def filter_datasets(self, datasets: List[str], root_dataset: str) -> List[str]:
        """Returns all datasets (and their descendants) that match at least one of the include regexes but none of the
        exclude regexes."""
        params = self.params
        results = []
        for dataset in datasets:
            rel_dataset = relativize_dataset(dataset, root_dataset)
            if rel_dataset.startswith('/'):
                rel_dataset = rel_dataset[1:]  # strip leading '/' char if any
            if self.is_included(rel_dataset, params.include_dataset_regexes, params.exclude_dataset_regexes):
                results.append(dataset)
            else:
                self.debug("Excluding b/c dataset regex:", dataset)
        return results

    def filter_snapshots(self, snapshots: List[str]) -> List[str]:
        """Returns all snapshots that match at least one of the include regexes but none of the exclude regexes."""
        results = []
        for snapshot in snapshots:
            if self.is_included_snapshot(snapshot):
                results.append(snapshot)
            else:
                self.debug("Excluding b/c snaphot regex:", snapshot)
        return results

    def is_included_snapshot(self, snapshot: str) -> bool:
        params = self.params
        p = snapshot.find('@')  # snapshot separator
        q = snapshot.find('#')  # bookmark separator
        i = min(p, q) + 1 if p >= 0 and q >= 0 else max(p, q) + 1
        snapshot_tag = snapshot[i:]  # substring after the first '@' or '#' char, whichever comes first
        return self.is_included(snapshot_tag, params.include_snapshot_regexes, params.exclude_snapshot_regexes)

    def is_included(self, name: str,
                    include_regexes: List[Tuple[re.Pattern, bool]],
                    exclude_regexes: List[Tuple[re.Pattern, bool]]) -> bool:
        """Returns True if the name matches at least one of the include regexes but none of the exclude regexes;
        else False. A regex that starts with a `!` is a negation - the regex matches if the regex without the
        `!` prefix does not match."""
        is_match = False
        for regex, is_negation in include_regexes:
            is_match = regex.fullmatch(name) if regex.pattern != '.*' else True
            if is_negation:
                is_match = not is_match
            if is_match:
                break

        if not is_match:
            return False

        for regex, is_negation in exclude_regexes:
            is_match = regex.fullmatch(name) if regex.pattern != '.*' else True
            if is_negation:
                is_match = not is_match
            if is_match:
                return False
        return True

    def filter_bookmarks(self, snapshots_and_bookmarks: List[str],
                         oldest_dst_snapshot_createtxg: Optional[int],
                         newest_dst_snapshot_createtxg: Optional[int]) -> List[str]:
        return snapshots_and_bookmarks  # TODO find correct optimization

        if oldest_dst_snapshot_createtxg is not None:
            oldest_dst_snapshot_createtxg = int(oldest_dst_snapshot_createtxg)
        if newest_dst_snapshot_createtxg is not None:
            newest_dst_snapshot_createtxg = int(newest_dst_snapshot_createtxg)

        results = []
        for snapshot in snapshots_and_bookmarks:
            if snapshot.find('@') >= 0:
                results.append(snapshot)  # it's a real snapshot
            else:
                # src bookmarks serve no purpose if the destination dataset has no snapshot, or if the src bookmark is
                # older than the oldest destination snapshot or newer than the newest destination snapshot. So here we
                # ignore them if that's the case. This is an optimization that helps if a large number of bookmarks
                # accumulate over time without periodic pruning.
                if oldest_dst_snapshot_createtxg is not None:
                    createtxg = int(snapshot[0:snapshot.index('\t')])
                    if oldest_dst_snapshot_createtxg <= createtxg <= newest_dst_snapshot_createtxg:
                        results.append(snapshot)
                    else:
                        self.debug("Excluding b/c bookmark createtxg:", snapshot)
        return results

    def filter_lines(self, input_list: Iterable[str], input_set: Set[str]) -> List[str]:
        """For each line in input_list, print the line if input_set contains the first column field of that line."""
        results = []
        if len(input_set) == 0:
            return results
        for line in input_list:
            if line[0:line.index('\t')] in input_set:
                results.append(line)
        return results

    def delete_snapshots(self, dataset: str, snapshot_tags: List[str] = []) -> None:
        if len(snapshot_tags) > 0:
            if self.is_solaris_zfs('dst'):
                # solaris-11.4.0 has no syntax to delete multiple snapshots in a single CLI invocation
                for snapshot_tag in reversed(snapshot_tags):
                    self.delete_snapshot(f"{dataset}@{snapshot_tag}")
            else:
                self.delete_snapshot(dataset + '@' + ','.join(reversed(snapshot_tags)))

    def delete_snapshot(self, snaps_to_delete: str) -> None:
        p = self.params
        self.info("Deleting snapshot(s):", snaps_to_delete)
        cmd = p.split_args(f"{p.dst_sudo} {p.zfs_program} destroy",
                           p.force_hard, p.verbose_destroy, p.dry_run_destroy, snaps_to_delete)
        is_dry = p.dry_run and self.is_solaris_zfs('dst')  # solaris-11.4.0 knows no 'zfs destroy -n' flag
        self.run_ssh_command('dst', self.debug, is_dry=is_dry, cmd=cmd)

    def delete_datasets(self, datasets: Iterable[str]) -> None:
        """Delete the given datasets via zfs destroy -r """
        # Impl is batch optimized to minimize CLI + network roundtrips: only need to run zfs destroy if previously
        # destroyed dataset (within sorted datasets) is not a prefix (aka ancestor) of current dataset
        last_deleted_dataset = ""
        for dataset in isorted(datasets):
            if not f"{dataset}/".startswith(f"{last_deleted_dataset}/"):
                self.info("Delete missing dataset tree:", f"{dataset} ...")
                p = self.params
                cmd = p.split_args(f"{p.dst_sudo} {p.zfs_program} destroy -r", p.force_hard, p.verbose_destroy, p.dry_run_destroy,
                                   dataset)
                is_dry = p.dry_run and self.is_solaris_zfs('dst')  # solaris-11.4.0 knows no 'zfs destroy -n' flag
                self.run_ssh_command('dst', self.debug, is_dry=is_dry, cmd=cmd)
                last_deleted_dataset = dataset

    def create_dataset(self, dataset: str) -> None:
        # zfs create -p -u $dataset
        # To ensure the datasets that we create do not get mounted, we apply a separate 'zfs create -p -u' invocation
        # for each non-existing ancestor. This is because a single 'zfs create -p -u' applies the '-u' part only to
        # the immediate dataset, rather than to the not-yet existing ancestors.
        p = self.params
        parent = ''
        no_mount = '-u' if self.is_program_available(zfs_version_is_at_least_2_1_0, 'dst') else ''
        for component in dataset.split('/'):
            parent += component
            if not self.dst_dataset_exists[parent]:
                cmd = p.split_args(f"{p.dst_sudo} {p.zfs_program} create -p", no_mount, parent)
                try:
                    self.run_ssh_command('dst', self.debug, stderr=PIPE, cmd=cmd)
                except subprocess.CalledProcessError as e:
                    print(e.stderr, sys.stderr, end='')
                    # ignore harmless error caused by zfs create without the -u flag
                    if ('filesystem successfully created, but it may only be mounted by root' not in e.stderr
                            and 'filesystem successfully created, but not mounted' not in e.stderr):  # SolarisZFS
                        raise e
                self.dst_dataset_exists[parent] = True
            parent += '/'

    def run_with_retries(self, func, *args, **kwargs) -> Any:
        """Run the given function with the given arguments, and retry on failure as indicated by params """
        params = self.params
        max_sleep_mark = params.min_sleep_nanos
        retry_count = 0
        start_time_nanos = time.time_ns()
        while True:
            try:
                return func(*args, **kwargs)  # Call the target function with the provided arguments
            except RetryableError as retryable_error:
                elapsed_nanos = time.time_ns() - start_time_nanos
                if retry_count < params.max_retries and elapsed_nanos < params.max_elapsed_nanos:
                    # pick a random sleep duration within the range [min_sleep_nanos, max_sleep_mark] as delay
                    sleep_nanos = random.randint(params.min_sleep_nanos, max_sleep_mark)
                    self.info(f"Retrying [{retry_count + 1}/{params.max_retries}] soon ...")
                    time.sleep(sleep_nanos / 1_000_000_000)
                    retry_count = retry_count + 1
                    max_sleep_mark = min(params.max_sleep_nanos, 2 * max_sleep_mark)  # exponential backoff with cap
                else:
                    if params.max_retries > 0:
                        error(f"Giving up because the last [{retry_count}/{params.max_retries}] retries across " 
                              f"[{elapsed_nanos // 1_000_000_000}/{params.max_elapsed_nanos // 1_000_000_000}] "
                              "seconds for the current request failed!")
                    raise retryable_error.__cause__

    def estimate_zfs_send_size(self, *items) -> int:
        """estimate num bytes to transfer via 'zfs send'"""
        if self.is_solaris_zfs('src'):
            return 0  # solaris-11.4.0 does not have a --parsable equivalent
        params = self.params
        p = params
        cmd = p.split_args(f"{p.src_sudo} {p.zfs_program} send -n -v --parsable",
                           p.zfs_send_program_opts, items)
        lines = self.run_ssh_command('src', self.trace, cmd=cmd)
        size = None
        for line in lines.splitlines():
            if line.startswith('size'):
                size = line
        return int(size[size.index('\t')+1:])

    def create_zfs_bookmark(self, src_snapshot: str, bookmarks: Dict[str, str], src_dataset: str) -> None:
        params = self.params
        p = params
        if '@' in src_snapshot:
            if not bookmarks.get(src_snapshot):  # not yet seen?
                bookmark = replace_prefix(src_snapshot, f"{src_dataset}@", f"{src_dataset}#")
                if params.create_bookmark and self.is_zpool_bookmarks_feature_enabled_or_active('src'):
                    cmd = p.split_args(f"{p.src_sudo} {p.zfs_program} bookmark", src_snapshot, bookmark)
                    self.run_ssh_command('src', self.debug, is_dry=p.dry_run, check=False, cmd=cmd)

    def warn(self, *items):
        self.log("[W]", *items)

    def info_raw(self, *items):
        if self.params.quiet != "":
            print(f"{current_time()} [I] {' '.join(items)}", file=sys.stderr)

    def info(self, *items):
        self.log("[I]", *items)

    def debug(self, *items):
        if self.params.verbose != "":
            self.log("[D]", *items)

    def trace(self, *items):
        if self.params.verbose_trace:
            self.log("[T]", *items)

    def log(self, first, second, *items):
        if self.params.quiet != "":
            print(f"{current_time()} {first} {second:<28} {' '.join(items)}", file=sys.stderr)  # right-pad second arg

    def ssh_command(self, ssh_user: str, ssh_host: str, ssh_user_host: str, ssh_port: str, ssh_extra_opts: List[str]) \
            -> List[str]:
        params = self.params
        ssh_cmd = []  # pool is on local host
        if ssh_user_host != "":  # pool is on remote host
            ssh_cmd = [params.ssh_program] + ssh_extra_opts
            if params.ssh_config_file:
                if not os.path.isfile(params.ssh_config_file):
                    die("ssh config file does not exist: " + params.ssh_config_file)
                ssh_cmd += ['-F', params.ssh_config_file]
            if params.ssh_private_key_file:
                if not os.path.isfile(params.ssh_private_key_file):
                    die("ssh private key file does not exist: " + params.ssh_private_key_file)
                ssh_cmd += ['-i', params.ssh_private_key_file]
            if params.ssh_cipher:
                ssh_cmd += ['-c', params.ssh_cipher]
            if ssh_port:
                ssh_cmd += ['-p', str(ssh_port)]
            if params.ssh_socket_enabled:
                # performance: (re)use ssh socket for low latency ssh startup of frequent ssh invocations
                # see https://www.cyberciti.biz/faq/linux-unix-reuse-openssh-connection/
                # generate unique private socket file name in user's home dir
                socket_dir = os.path.join(params.home_dir, '.ssh', 'wbackup-zfs')
                os.makedirs(os.path.dirname(socket_dir), exist_ok=True)
                os.makedirs(socket_dir, mode=stat.S_IRWXU, exist_ok=True)  # chmod u=rwx,go=

                def sanitize(name):
                    # replace any whitespace, /, $, \, @ with a ~ tilde char
                    name = re.sub(r'[\s\\/@$]', '~', name)
                    # Remove characters not in the allowed set
                    name = re.sub(r'[^a-zA-Z0-9;:,<.>?~`!%#$%^&*+=_-]', '', name)
                    return name

                unique = f"{time.time_ns()}@{random.randint(0, 999_999)}"
                if params.is_test_mode:
                    unique = 'x$#^&*(x'  # faster for running large numbers of short unit tests, also tests quoting
                socket_name = f"{os.getpid()}@{unique}@{sanitize(ssh_host)[:45]}@{sanitize(ssh_user)}"
                socket_file = os.path.join(socket_dir, socket_name)[:max(100, len(socket_dir)+10)]
                ssh_cmd += ['-S', socket_file]
            ssh_cmd += [ssh_user_host]
        return ssh_cmd

    def run_ssh_command(self, target='src', level=info, is_dry=False, check=True, stderr=None, cmd=None):
        assert cmd is not None and len(cmd) > 0
        p = self.params
        ssh_cmd = p.ssh_src_cmd
        ssh_user_host = p.ssh_src_user_host
        if target == 'dst':
            ssh_cmd = p.ssh_dst_cmd
            ssh_user_host = p.ssh_dst_user_host
        if len(ssh_cmd) > 0:
            if "ssh" not in p.available_programs['local']:
                die(f"{p.ssh_program} CLI is not available to talk to remote host. Install {p.ssh_program} first!")
            cmd = [self.dquote(ssh_cmd, arg) for arg in cmd]
            if p.ssh_socket_enabled:
                # performance: (re)use ssh socket for low latency ssh startup of frequent ssh invocations
                # see https://www.cyberciti.biz/faq/linux-unix-reuse-openssh-connection/
                # 'ssh -S /path/socket -O check' doesn't talk over the network so common case is a low latency fast path
                ssh_cmd_trimmed = ssh_cmd[0:-1]  # remove trailing $ssh_user_host
                ssh_socket_cmd = xappend(ssh_cmd_trimmed.copy(), '-O', 'check', ssh_user_host)
                if subprocess.run(ssh_socket_cmd, capture_output=True, text=True).returncode == 0:  # &> /dev/null
                    self.trace("ssh socket is alive:", ' '.join(ssh_socket_cmd))
                else:
                    self.trace("ssh socket is not yet alive:", ' '.join(ssh_socket_cmd))
                    ssh_socket_cmd = xappend(ssh_cmd[0:-1], '-M', '-o', 'ControlPersist=60s', ssh_user_host, 'exit')
                    self.debug("Executing:", ' '.join(ssh_socket_cmd))
                    if subprocess.run(ssh_socket_cmd, stdout=PIPE, text=True).returncode != 0:
                        die(f"Cannot ssh into remote host via {ssh_socket_cmd}. "
                            f"Fix ssh configuration first, considering diagnostic output from running {prog_name} with: " 
                            f"-v -v --ssh-src-extra-opt '-v -v' --ssh-dst-extra-opt '-v -v'")

        msg = "Would execute:" if is_dry else "Executing:"
        level(msg, ' '.join(ssh_cmd + cmd))
        if not is_dry:
            std_check = False if check is None else check
            if stderr != PIPE:
                stderr = None if check else PIPE
            process = subprocess.run(ssh_cmd + cmd, stdout=PIPE, stderr=stderr, text=True, check=std_check)
            if check is not None:
                return process.stdout
            else:
                # The value of subprocess.run(stdout=subprocess.PIPE, ...).stdout is an empty string if the process
                # exits with a nonzero return code. Here we turn that empty string into None to indicate to the caller
                # a nonzero return code vs a process success with empty string result, e.g. to differentiate between
                # 'zfs list' on an empty dataset vs a non-existing dataset.
                return process.stdout if process.returncode == 0 else None

    def cquote(self, arg: str):
        return shlex.quote(arg)

    def dquote(self, ssh_cmd, arg: str):
        return arg if len(ssh_cmd) == 0 else self.cquote(arg)

    def mbuffer_cmd(self, loc: str, size_estimate_bytes: int) -> str:
        """If mbuffer command is on the PATH, use it in the ssh network pipe between 'zfs send' and 'zfs receive' to
        smooth out the rate of data flow and prevent bottlenecks caused by network latency or speed fluctuation"""
        params = self.params
        p = params
        if size_estimate_bytes >= params.min_transfer_size and \
            ( (loc == "src" and (p.ssh_src_user_host != "" or p.ssh_dst_user_host != "") )
            or (loc == "dst" and (p.ssh_src_user_host != "" or p.ssh_dst_user_host != "") )
            or (loc == "local" and p.ssh_src_user_host != "" and p.ssh_dst_user_host != "") ) \
            and self.is_program_available('mbuffer', loc):
            return f"{p.mbuffer_program} {' '.join(self.mbuffer_current_opts)} 2> /dev/null"
        else:
            return 'cat'

    def compress_cmd(self, loc: str, size_estimate_bytes: int) -> str:
        """If zstd command is on the PATH, use it in the ssh network pipe between 'zfs send' and 'zfs receive' to
        reduce network bottlenecks by sending compressed data."""
        params = self.params
        p = params
        if size_estimate_bytes >= p.min_transfer_size and (p.ssh_src_user_host != "" or p.ssh_dst_user_host != "") \
            and self.is_program_available('zstd', loc):
            return f"{p.compression_program} {' '.join(p.compression_program_opts)}"
        else:
            return 'cat'

    def decompress_cmd(self, loc: str, size_estimate_bytes: int) -> str:
        params = self.params
        p = params
        if size_estimate_bytes >= p.min_transfer_size and (p.ssh_src_user_host != "" or p.ssh_dst_user_host != "") \
            and self.is_program_available('zstd', loc):
            return f"{p.compression_program} -d"
        else:
            return 'cat'

    def pv_cmd(self, loc: str, size_estimate_bytes: int, size_estimate_human: str, disable_progress_bar=False) -> str:
        """If pv command is on the PATH, monitor the progress of data transfer from 'zfs send' to 'zfs receive'.
        Progress can be viewed via "tail -f $pv_log_file" aka current.pv or similar """
        params = self.params
        p = params
        if size_estimate_bytes >= p.min_transfer_size and self.is_program_available('pv', loc):
            size = f"--size={size_estimate_bytes}"
            if disable_progress_bar:
                size = ""
            readable = size_estimate_human.replace(' ', '')
            return f"{p.pv_program} {' '.join(p.pv_program_opts)} --force --name={readable} {size} 2>> {p.pv_log_file}"
        else:
            return 'cat'

    def is_program_available(self, program: str, location: str) -> bool:
        return program in self.params.available_programs[location]

    def detect_available_programs(self) -> None:
        params = self.params
        p = params
        available_programs = params.available_programs
        available_programs['local'] = dict.fromkeys(subprocess.run(
            [p.shell_program_local, '-c', self.find_available_programs()],
            stdout=PIPE, text=True, check=False).stdout.splitlines())
        if subprocess.run([params.shell_program_local, '-c', 'exit'], stdout=PIPE, text=True).returncode != 0:
            self.disable_program_internal('sh', 'local')

        # check if we can ssh into the remote hosts at all
        if len(params.ssh_src_cmd) > 0:
            self.run_ssh_command('src', self.debug, cmd=['exit'])
        if len(params.ssh_dst_cmd) > 0:
            self.run_ssh_command('dst', self.debug, cmd=['exit'])

        # if 'sh' and 'echo' are available on remote hosts then detect available programs there
        self.detect_available_programs_remote('src', available_programs, p.ssh_src_user_host)
        self.detect_available_programs_remote('dst', available_programs, p.ssh_dst_user_host)

        if not ('zstd' in available_programs['src'] and 'zstd' in available_programs['dst']):
            self.disable_program('zstd')  # no compression is used if source and dst do not both support compression
        if params.getenv_bool('disable_compression', False):
            self.disable_program('zstd')
        if params.getenv_bool('disable_mbuffer', False):
            self.disable_program('mbuffer')
        if params.getenv_bool('disable_pv', False):
            self.disable_program('pv')
        if params.getenv_bool('disable_sh', False):
            self.disable_program('sh')
        if params.getenv_bool('disable_sudo', False):
            self.disable_program('sudo')
        if params.getenv_bool('disable_zpool', False):
            self.disable_program('zpool')

        for key in ['local', 'src', 'dst']:
            for program in list(available_programs[key].keys()):
                if program.startswith('uname-'):
                    # uname-SunOS solaris 5.11 11.4.0.15.0 i86pc i386 i86pc
                    # uname-Linux foo 5.15.0-69-generic #76-Ubuntu SMP Fri Mar 17 17:19:29 UTC 2023 x86_64 x86_64 x86_64 GNU/Linux
                    available_programs[key].pop(program)
                    uname = program[len('uname-'):]
                    available_programs[key]['uname'] = uname
                    self.trace(f"available_programs[{key}][uname]:", uname)
                    available_programs[key]['os'] = uname.split(' ')[0]  # Linux|FreeBSD|SunOS
                    self.trace(f"available_programs[{key}][os]:", f"{available_programs[key]['os']}")

        for key, value in available_programs.items():
            self.debug(f"available_programs[{key}]:", ', '.join(value))

        if not self.is_program_available('zfs', 'src'):
            die(f"{params.zfs_program} CLI is not available on src host: {params.ssh_src_user_host or 'localhost'}")
        if not self.is_program_available('zfs', 'dst'):
            die(f"{params.zfs_program} CLI is not available on dst host: {params.ssh_dst_user_host or 'localhost'}")

        if params.src_sudo and not self.is_program_available('sudo', 'src'):
            die(f"{params.sudo_program} CLI is not available on src host: {params.ssh_src_user_host or 'localhost'}")
        if params.dst_sudo and not self.is_program_available('sudo', 'dst'):
            die(f"{params.sudo_program} CLI is not available on dst host: {params.ssh_dst_user_host or 'localhost'}")

    def disable_program(self, program: str):
        self.disable_program_internal(program, 'src')
        self.disable_program_internal(program, 'dst')
        self.disable_program_internal(program, 'local')

    def disable_program_internal(self, program: str, location: str):
        self.params.available_programs[location].pop(program, None)

    def find_available_programs(self):
        params = self.params
        return f'''
        command -v echo > /dev/null && echo echo
        command -v {params.zpool_program} > /dev/null && echo zpool
        command -v {params.ssh_program} > /dev/null && echo ssh
        command -v {params.shell_program} > /dev/null && echo sh
        command -v {params.sudo_program} > /dev/null && echo sudo
        command -v {params.compression_program} > /dev/null && echo zstd
        command -v {params.mbuffer_program} > /dev/null && echo mbuffer
        command -v {params.pv_program} > /dev/null && echo pv
        command -v {params.uname_program} > /dev/null && printf uname- && {params.uname_program} -a || true
        '''

    def detect_available_programs_remote(self, location: str, available_programs: Dict, ssh_user_host):
        p = self.params
        available_programs_minimum = {'zpool': None, 'sudo': None}
        available_programs[location] = {}
        lines = None
        try:
            lines = self.run_ssh_command(location, self.debug, stderr=PIPE, cmd=[p.zfs_program, '--version'])
        except FileNotFoundError as e:  # location is local and program file was not found
            die(f"{p.zfs_program} CLI is not available on {location} host: {ssh_user_host or 'localhost'}")
        except subprocess.CalledProcessError as e:
            if "unrecognized command '--version'" in e.stderr and "run: zfs help" in e.stderr:
                available_programs[location]['zfs'] = 'notOpenZFS'  # solaris-11.4.0 zfs does not know --version flag
            else:
                print(e.stderr, sys.stderr, end='')
                die(f"{p.zfs_program} CLI is not available on {location} host: {ssh_user_host or 'localhost'}")

        if lines is not None:
            line = lines.splitlines()[0]
            assert line.startswith('zfs-')
            # Example: zfs-2.1.5~rc5-ubuntu3 -> 2.1.5
            version = line.split('-')[1].strip()
            match = re.fullmatch(r'(\d+\.\d+\.\d+).*', version)
            if match:
                version = match.group(1)
            else:
                raise ValueError("Unparsable zfs version string: " + version)
            available_programs[location]['zfs'] = version
            try:
                if is_version_at_least(version, '2.1.0'):
                    available_programs[location][zfs_version_is_at_least_2_1_0] = True
            except Exception:
                pass
        self.trace(f"available_programs[{location}][zfs]:", available_programs[location]['zfs'])

        try:
            cmd = [p.shell_program, '-c', 'echo hello world']
            if self.run_ssh_command(target=location, level=self.trace, check=False, cmd=cmd) == 'hello world\n':
                cmd = [p.shell_program, '-c', self.find_available_programs()]
                available_programs[location].update(dict.fromkeys(
                    self.run_ssh_command(location, self.trace, cmd=cmd).splitlines()))
            else:
                self.warn(f"Failed to run {p.shell_program} on {location}. Continuing with minimal assumptions...")
                available_programs[location].update(available_programs_minimum)
        except FileNotFoundError as e:  # location is local and shell program file was not found
            if e.filename != p.shell_program:
                raise e
            self.warn(f"Failed to find {p.shell_program} on {location}. Continuing with minimal assumptions...")
            available_programs[location].update(available_programs_minimum)

    def is_solaris_zfs(self, location: str):
        return self.params.available_programs[location].get('zfs') == 'notOpenZFS'

    def detect_zpool_features(self, location: str, pool: str) -> None:
        params = self.params
        features = {}
        if self.is_program_available('zpool', location):
            cmd = params.split_args(f"{params.zpool_program} get -Hp -o property,value all", pool)
            try:
                lines = self.run_ssh_command(location, self.trace, check=False, cmd=cmd).splitlines()
            except FileNotFoundError as e:
                if e.filename != params.zpool_program:
                    raise e
                lines = []
            if len(lines) == 0:
                self.warn(f"Failed to detect zpool features on {location}: {pool}. "
                          f"Continuing with minimal assumptions...")
            props = {line.split('\t', 1)[0]: line.split('\t', 1)[1] for line in lines}
            features = {k: v for k, v in props.items() if k.startswith('feature@')}
            str_features = '\n'.join([f"{k}: {v}" for k, v in sorted(features.items())])
            self.trace(f"{location} zpool features:", str_features)
        params.zpool_features[location] = features

    def is_zpool_feature_enabled_or_active(self, location: str, feature: str) -> bool:
        value = self.params.zpool_features[location].get(feature, None)
        return value == 'active' or value == 'enabled'

    def is_zpool_bookmarks_feature_enabled_or_active(self, location: str) -> bool:
        return (self.is_zpool_feature_enabled_or_active(location, 'feature@bookmark_v2')
                and self.is_zpool_feature_enabled_or_active(location, 'feature@bookmark_written'))

    def dataset_regexes(self, datasets: List[str]) -> List[str]:
        params = self.params
        results = []
        for dataset in datasets:
            if dataset.startswith('/'):
                # it's an absolute dataset - convert it to a relative dataset
                dataset = dataset[1:]
                if f"{dataset}/".startswith(f"{params.src_root_dataset}/"):
                    dataset = relativize_dataset(dataset, params.src_root_dataset)
                elif f"{dataset}/".startswith(f"{params.dst_root_dataset}/"):
                    dataset = relativize_dataset(dataset, params.dst_root_dataset)
                else:
                    continue  # ignore datasets that make no difference
                if dataset.startswith('/'):
                    dataset = dataset[1:]
            if dataset:
                regex = re.escape(dataset)
            else:
                regex = '.*'
            results.append(regex)
        return results


#############################################################################
def error(*items):
    print(f"{current_time()} [E] ERROR: {' '.join(items)}", file=sys.stderr)


def die(*items):
    error(*items)
    exit(die_status)


def cut(field, separator='\t', lines=None) -> List[str]:
    if field == 1:
        return [line[0:line.index(separator)] for line in lines]
    elif field == 2:
        return [line[line.index(separator) + 1:] for line in lines]
    else:
        raise ValueError("Unsupported parameter value")


def relativize_dataset(dataset: str, root_dataset: str) -> str:
    """ converts an absolute dataset path to a relative dataset path wrt root_dataset
        Example: src_root_dataset=tank/foo, dataset=tank/foo/bar/baz --> relative_path=/bar/baz """
    return dataset[len(root_dataset):]


def compile_regexes(regexes: List[str], suffix='') -> List[Tuple[re.Pattern, bool]]:
    compiled_regexes = []
    for regex in regexes:
        is_negation = regex.startswith('!')
        if is_negation:
            regex = regex[1:]
        regex = replace_capturing_groups_with_non_capturing_groups(regex)
        if regex != '.*' or not (suffix.startswith('(') and suffix.endswith(')?')):
            regex = f"{regex}{suffix}"
        compiled_regexes.append((re.compile(regex), is_negation))
    return compiled_regexes


def replace_capturing_groups_with_non_capturing_groups(regex: str) -> str:
    """ Replace regex capturing groups with non-capturing groups for better matching performance.
    Example: '(.*/)?tmp(foo|bar)(?!public)\\(' --> '(?:.*/)?tmp(?:foo|bar)(?!public)\\()'
    Aka replace brace '(' followed by a char other than question mark '?', but not preceded by a backslash
    with the replacement string '(?:'
    Also see https://docs.python.org/3/howto/regex.html#non-capturing-and-named-groups
    """
    # pattern = re.compile(r'(?<!\\)\((?!\?)')
    # return pattern.sub('(?:', regex)
    i = len(regex)-2
    while i >= 0:
        i = regex.rfind('(', 0, i+1)
        if i >= 0 and regex[i] == '(' and (regex[i+1] != '?') and (i == 0 or regex[i-1] != '\\'):
            regex = f"{regex[0:i]}(?:{regex[i+1:]}"
        i -= 1
    return regex


def isorted(iterable, reverse=False) -> List:
    """ case-insensitive sort (A < a < B < b and so on) """
    return sorted(iterable, key=str.casefold, reverse=reverse)


def xappend(_list, *items) -> List[str]:
    """Append each of the items to the given list if the item is "truthy", e.g. not None and not an empty string.
       If an item is an iterable do so recursively, flattening the output."""
    for item in items:
        if isinstance(item, collections.abc.Iterable) and not isinstance(item, str):
            xappend(_list, *item)
        elif item:
            _list.append(item)
    return _list


def current_time() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def replace_prefix(line, s1, s2):
    """In a line, replace a leading s1 string with s2. Assumes the leading string is present. """
    return s2 + line.rstrip()[len(s1):]


def replace_suffix(line, s1, s2):
    """In a line, replace a trailing s1 string with s2. Assumes the trailing string is present."""
    return line.rstrip()[-len(s1):] + s2


def human_readable_bytes(size) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB"]
    i = 0
    while size >= 1024 and i < len(units) - 1:
        size //= 1024
        i += 1
    return f"{size} {units[i]}"


def get_home_directory() -> str:
    """ reliably detect home dir even if HOME env var is undefined"""
    home = os.getenv("HOME")
    if not home:
        # thread-safe version of: os.environ.pop("HOME", None); os.path.expanduser("~")
        home = pwd.getpwuid(os.getuid()).pw_dir
    return home


def create_symlink(src, dst_dir, dst):
    """For parallel usage, ensure there is no time window when the symlink does not exist; uses atomic os.rename()"""
    uniq = f".tmp_{os.getpid()}_{time.time_ns()}_{uuid.uuid4().hex}"
    fd, temp_link = tempfile.mkstemp(suffix='.tmp', prefix=uniq, dir=dst_dir)
    os.close(fd)
    os.remove(temp_link)
    os.symlink(os.path.basename(src), temp_link)
    os.rename(temp_link, os.path.join(dst_dir, dst))


def is_version_at_least(version_str: str, min_version_str: str) -> bool:
    """Check if the version string is at least the minimum version string."""
    return tuple(map(int, version_str.split('.'))) >= tuple(map(int, min_version_str.split('.')))


def tail(file, n: int):
    if not os.path.isfile(file):
        return []
    with open(file, 'r', encoding='utf-8') as fd:
        return collections.deque(fd, maxlen=n)


def append_if_absent(list: List, item):
    if item not in list:
        list.append(item)
    return list


def parse_dataset_locator(input_text, validate=True, user=None, host=None, port=None):
    user_undefined = user is None
    if user is None:
        user = ""
    host_undefined = host is None
    if host is None:
        host = ""
    user_host = ""
    dataset = ""
    pool = ""

    # Input format is [[user@]host:]dataset
    #                           1234         5          6
    match = re.fullmatch(r'(((([^@]*)@)?([^:]+)):)?(.*)', input_text)
    if match:
        if user_undefined:
            user = match.group(4) or ""
        if host_undefined:
            host = match.group(5) or ""
        if host == '-':
            host = ""
        dataset = match.group(6) or ""
        i = dataset.find('/')
        pool = dataset[0:i] if i >= 0 else dataset

        if user and host:
            user_host = f"{user}@{host}"
        elif host:
            user_host = host

    if validate:
        validate_user_name(user, input_text)
        validate_host_name(host, input_text)
        validate_port(port, f"Illegal port number: '{port}' for: '{input_text}' - ")
        validate_dataset_name(dataset, input_text)

    return user, host, user_host, pool, dataset


def validate_dataset_name(dataset, input_text):
    # 'zfs create' CLI does not accept dataset names that are empty or start or end in a slash, etc.
    # Also see https://github.com/openzfs/zfs/issues/439#issuecomment-2784424
    # and https://github.com/openzfs/zfs/issues/8798
    # and (by now nomore accurate): https://docs.oracle.com/cd/E26505_01/html/E37384/gbcpt.html
    ds = dataset
    if (ds in ['', '.', '..'] or '//' in ds or ds.startswith('/') or ds.endswith('/') or ds.startswith('./')
            or ds.startswith('../') or ds.endswith('/.') or ds.endswith('/..') or '@' in ds or '#' in ds
            or '"' in ds or "'" in ds or (len(ds) > 0 and not ds[0].isalpha())):
        raise ValueError(f"Illegal ZFS dataset name: '{dataset}' for: '{input_text}'")


def validate_user_name(user, input_text):
    if user and any(char.isspace() or char == '"' or char == "'" for char in user):
        raise ValueError(f"Illegal user name: '{user}' for: '{input_text}'")


def validate_host_name(host, input_text):
    if host and any(char.isspace() or char == '@' or char == '"' or char == "'" for char in host):
        raise ValueError(f"Illegal host name: '{host}' for: '{input_text}'")


def validate_port(port, message):
    if isinstance(port, int):
        port = str(port)
    if port and not port.isdigit():
        raise ValueError(message + f"must be empty or a positive integer: '{port}'")


#############################################################################
class RetryableError(Exception):
    """ Indicates that the task that caused the underlying exception can be retried and might eventually succeed """
    def __init__(self, message):
        super().__init__(message)


#############################################################################
class Tee:
    def __init__(self, *files):
        self.files = files

    def write(self, obj):
        for file in self.files:
            file.write(obj)
            file.flush()  # Ensure each write is flushed immediately

    def flush(self):
        for file in self.files:
            file.flush()


#############################################################################
class FileOrLiteralAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        current_values = getattr(namespace, self.dest, None)
        if current_values is None:
            current_values = []
        for value in values:
            if not value.startswith('@'):
                current_values.append(value)
            else:
                try:
                    with open(value[1:], 'r', encoding='utf-8') as fd:
                        for line in fd:
                            current_values.append(line.strip())
                except FileNotFoundError:
                    parser.error(f"File not found: {value[1:]}")
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


#############################################################################
if __name__ == "__main__":
    main()
