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
"""Configuration subsystem; All CLI option/parameter values are reachable from the "Params" class."""

from __future__ import (
    annotations,
)
import argparse
import ast
import os
import platform
import random
import re
import shutil
import stat
import sys
import tempfile
import threading
import time
from collections.abc import (
    Iterable,
)
from dataclasses import (
    dataclass,
)
from datetime import (
    datetime,
    tzinfo,
)
from logging import (
    Logger,
)
from typing import (
    TYPE_CHECKING,
    Final,
    Literal,
    NamedTuple,
    cast,
)

import bzfs_main.utils
from bzfs_main.argparse_actions import (
    SnapshotFilter,
    optimize_snapshot_filters,
)
from bzfs_main.argparse_cli import (
    LOG_DIR_DEFAULT,
    ZFS_RECV_GROUPS,
    ZFS_RECV_O,
    ZFS_RECV_O_INCLUDE_REGEX_DEFAULT,
    __version__,
)
from bzfs_main.detect import (
    DISABLE_PRG,
)
from bzfs_main.period_anchors import (
    PeriodAnchors,
)
from bzfs_main.retry import (
    RetryPolicy,
)
from bzfs_main.utils import (
    DIR_PERMISSIONS,
    FILE_PERMISSIONS,
    PROG_NAME,
    SHELL_CHARS,
    SNAPSHOT_FILTERS_VAR,
    UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH,
    UNIX_TIME_INFINITY_SECS,
    RegexList,
    SnapshotPeriods,
    SynchronizedBool,
    append_if_absent,
    compile_regexes,
    current_datetime,
    die,
    get_home_directory,
    get_timezone,
    getenv_bool,
    getenv_int,
    is_included,
    ninfix,
    nprefix,
    nsuffix,
    parse_duration_to_milliseconds,
    pid_exists,
    sha256_hex,
    sha256_urlsafe_base64,
    urlsafe_base64,
    validate_dataset_name,
    validate_file_permissions,
    validate_is_not_a_symlink,
    validate_property_name,
    xappend,
)

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from bzfs_main.connection import (
        ConnectionPools,
    )

# constants:
HOME_DIRECTORY: Final[str] = get_home_directory()
_UNSET_ENV_VARS_LOCK: Final[threading.Lock] = threading.Lock()
_UNSET_ENV_VARS_LATCH: Final[SynchronizedBool] = SynchronizedBool(True)


#############################################################################
class LogParams:
    """Option values for logging."""

    def __init__(self, args: argparse.Namespace) -> None:
        """Reads from ArgumentParser via args."""
        # immutable variables:
        if args.quiet:
            log_level: str = "ERROR"
        elif args.verbose >= 2:
            log_level = "TRACE"
        elif args.verbose >= 1:
            log_level = "DEBUG"
        else:
            log_level = "INFO"
        self.log_level: Final[str] = log_level
        self.timestamp: Final[str] = datetime.now().isoformat(sep="_", timespec="seconds")  # 2024-09-03_12:26:15
        self.isatty: Final[bool] = getenv_bool("isatty", True)
        self.quiet: Final[bool] = args.quiet
        self.terminal_columns: Final[int] = (
            getenv_int("terminal_columns", shutil.get_terminal_size(fallback=(120, 24)).columns)
            if self.isatty and args.pv_program != DISABLE_PRG and not self.quiet
            else 0
        )
        self.home_dir: Final[str] = HOME_DIRECTORY
        log_parent_dir: Final[str] = args.log_dir if args.log_dir else os.path.join(self.home_dir, LOG_DIR_DEFAULT)
        if LOG_DIR_DEFAULT not in os.path.basename(log_parent_dir):
            die(f"Basename of --log-dir must contain the substring '{LOG_DIR_DEFAULT}', but got: {log_parent_dir}")
        sep: str = "_" if args.log_subdir == "daily" else ":"
        timestamp: str = self.timestamp
        subdir: str = timestamp[0 : timestamp.rindex(sep) if args.log_subdir == "minutely" else timestamp.index(sep)]
        # 2024-09-03 (d), 2024-09-03_12 (h), 2024-09-03_12:26 (m)
        self.log_dir: Final[str] = os.path.join(log_parent_dir, subdir)
        os.makedirs(log_parent_dir, mode=DIR_PERMISSIONS, exist_ok=True)
        validate_is_not_a_symlink("--log-dir ", log_parent_dir)
        validate_file_permissions(log_parent_dir, DIR_PERMISSIONS)
        os.makedirs(self.log_dir, mode=DIR_PERMISSIONS, exist_ok=True)
        validate_is_not_a_symlink("--log-dir subdir ", self.log_dir)
        validate_file_permissions(self.log_dir, DIR_PERMISSIONS)
        self.log_file_prefix: Final[str] = args.log_file_prefix
        self.log_file_infix: Final[str] = args.log_file_infix
        self.log_file_suffix: Final[str] = args.log_file_suffix
        fd, self.log_file = tempfile.mkstemp(
            suffix=".log",
            prefix=f"{self.log_file_prefix}{self.timestamp}{self.log_file_infix}{self.log_file_suffix}-",
            dir=self.log_dir,
        )
        os.fchmod(fd, FILE_PERMISSIONS)
        os.close(fd)
        self.pv_log_file: Final[str] = self.log_file[0 : -len(".log")] + ".pv"
        log_file_stem: str = os.path.basename(self.log_file)[0 : -len(".log")]
        # Python's standard logger naming API interprets chars such as '.', '-', ':', spaces, etc in special ways, e.g.
        # logging.getLogger("foo.bar") vs logging.getLogger("foo-bar"). Thus, we sanitize the Python logger name via a regex:
        self.logger_name_suffix: Final[str] = re.sub(r"[^A-Za-z0-9_]", repl="_", string=log_file_stem)
        cache_root_dir: str = os.path.join(log_parent_dir, ".cache")
        os.makedirs(cache_root_dir, mode=DIR_PERMISSIONS, exist_ok=True)
        validate_file_permissions(cache_root_dir, DIR_PERMISSIONS)
        self.last_modified_cache_dir: Final[str] = os.path.join(cache_root_dir, "mods")

        # Create/update "current" symlink to current_dir, which is a subdir containing further symlinks to log files.
        # For parallel usage, ensures there is no time window when the symlinks are inconsistent or do not exist.
        current: str = "current"
        dot_current_dir: str = os.path.join(log_parent_dir, f".{current}")
        current_dir: str = os.path.join(dot_current_dir, log_file_stem)
        os.makedirs(dot_current_dir, mode=DIR_PERMISSIONS, exist_ok=True)
        validate_is_not_a_symlink("--log-dir: .current ", dot_current_dir)
        try:
            os.makedirs(current_dir, mode=DIR_PERMISSIONS, exist_ok=True)
            _create_symlink(self.log_file, current_dir, f"{current}.log")
            _create_symlink(self.pv_log_file, current_dir, f"{current}.pv")
            _create_symlink(self.log_dir, current_dir, f"{current}.dir")
            dst_file: str = os.path.join(current_dir, current)
            os.symlink(os.path.relpath(current_dir, start=log_parent_dir), dst_file)
            os.replace(dst_file, os.path.join(log_parent_dir, current))  # atomic rename
            _delete_stale_files(dot_current_dir, prefix="", millis=5000, dirs=True, exclude=os.path.basename(current_dir))
        except FileNotFoundError:
            pass  # harmless concurrent cleanup

    def __repr__(self) -> str:
        return str(self.__dict__)


#############################################################################
class Params:
    """All parsed CLI options combined into a single bundle; simplifies passing around numerous settings and defaults."""

    def __init__(
        self,
        args: argparse.Namespace,
        sys_argv: list[str],
        log_params: LogParams,
        log: Logger,
        inject_params: dict[str, bool] | None = None,
    ) -> None:
        """Reads from ArgumentParser via args."""
        # immutable variables:
        assert args is not None
        assert isinstance(sys_argv, list)
        assert log_params is not None
        assert log is not None
        self.args: Final[argparse.Namespace] = args
        self.sys_argv: Final[list[str]] = sys_argv
        self.log_params: Final[LogParams] = log_params
        self.log: Final[Logger] = log
        self.inject_params: Final[dict[str, bool]] = inject_params if inject_params is not None else {}  # for testing only
        self.one_or_more_whitespace_regex: Final[re.Pattern[str]] = re.compile(r"\s+")
        self.two_or_more_spaces_regex: Final[re.Pattern[str]] = re.compile(r"  +")
        self._unset_matching_env_vars(args)
        self.xperiods: Final[SnapshotPeriods] = SnapshotPeriods()

        assert len(args.root_dataset_pairs) > 0
        self.root_dataset_pairs: Final[list[tuple[str, str]]] = args.root_dataset_pairs
        self.recursive: Final[bool] = args.recursive
        self.recursive_flag: Final[str] = "-r" if args.recursive else ""

        self.dry_run: Final[bool] = args.dryrun is not None
        self.dry_run_recv: Final[str] = "-n" if self.dry_run else ""
        self.dry_run_destroy: Final[str] = self.dry_run_recv
        self.dry_run_no_send: Final[bool] = args.dryrun == "send"
        self.verbose_zfs: Final[bool] = args.verbose >= 2
        self.verbose_destroy: Final[str] = "" if args.quiet else "-v"

        self.zfs_send_program_opts: Final[list[str]] = self._fix_send_opts(self.split_args(args.zfs_send_program_opts))
        zfs_recv_program_opts: list[str] = self.split_args(args.zfs_recv_program_opts)
        for extra_opt in args.zfs_recv_program_opt:
            zfs_recv_program_opts.append(self.validate_arg_str(extra_opt, allow_all=True))
        preserve_properties = [validate_property_name(name, "--preserve-properties") for name in args.preserve_properties]
        zfs_recv_program_opts, zfs_recv_x_names = self._fix_recv_opts(zfs_recv_program_opts, frozenset(preserve_properties))
        self.zfs_recv_program_opts: Final[list[str]] = zfs_recv_program_opts
        self.zfs_recv_x_names: Final[list[str]] = zfs_recv_x_names
        if self.verbose_zfs:
            append_if_absent(self.zfs_send_program_opts, "-v")
            append_if_absent(self.zfs_recv_program_opts, "-v")
        # zfs_full_recv_opts: dataset-specific dynamic -o/-x property options are computed later per dataset in
        # replication._add_recv_property_options():
        self.zfs_full_recv_opts: Final[list[str]] = self.zfs_recv_program_opts.copy()
        cpconfigs = [CopyPropertiesConfig(group, flag, args, self) for group, flag in ZFS_RECV_GROUPS.items()]
        self.zfs_recv_o_config, self.zfs_recv_x_config, self.zfs_set_config = cpconfigs

        self.force_rollback_to_latest_snapshot: Final[bool] = args.force_rollback_to_latest_snapshot
        self.force_rollback_to_latest_common_snapshot: Final[SynchronizedBool] = SynchronizedBool(
            args.force_rollback_to_latest_common_snapshot
        )
        self.force: Final[SynchronizedBool] = SynchronizedBool(args.force)
        self.force_once: Final[bool] = args.force_once
        self.force_unmount: Final[str] = "-f" if args.force_unmount else ""
        force_hard: str = "-R" if args.force_destroy_dependents else ""
        self.force_hard: Final[str] = "-R" if args.force_hard else force_hard  # --force-hard is deprecated

        self.skip_parent: Final[bool] = args.skip_parent
        self.skip_missing_snapshots: Final[str] = args.skip_missing_snapshots
        self.skip_on_error: Final[str] = args.skip_on_error
        self.retry_policy: Final[RetryPolicy] = RetryPolicy(args)
        self.skip_replication: Final[bool] = args.skip_replication
        self.delete_dst_snapshots: Final[bool] = args.delete_dst_snapshots is not None
        self.delete_dst_bookmarks: Final[bool] = args.delete_dst_snapshots == "bookmarks"
        self.delete_dst_snapshots_no_crosscheck: Final[bool] = args.delete_dst_snapshots_no_crosscheck
        self.delete_dst_snapshots_except: Final[bool] = args.delete_dst_snapshots_except
        self.delete_dst_datasets: Final[bool] = args.delete_dst_datasets
        self.delete_empty_dst_datasets: Final[bool] = args.delete_empty_dst_datasets is not None
        self.delete_empty_dst_datasets_if_no_bookmarks_and_no_snapshots: Final[bool] = (
            args.delete_empty_dst_datasets == "snapshots+bookmarks"
        )
        self.compare_snapshot_lists: Final[str] = args.compare_snapshot_lists
        self.daemon_lifetime_nanos: Final[int] = 1_000_000 * parse_duration_to_milliseconds(args.daemon_lifetime)
        self.daemon_frequency: Final[str] = args.daemon_frequency
        self.enable_privilege_elevation: Final[bool] = not args.no_privilege_elevation
        self.no_stream: Final[bool] = args.no_stream
        self.resume_recv: Final[bool] = not args.no_resume_recv
        self.create_bookmarks: Final[str] = (
            "none" if args.no_create_bookmark else args.create_bookmarks
        )  # no_create_bookmark depr
        self.use_bookmark: Final[bool] = not args.no_use_bookmark

        self.src: Final[Remote] = Remote("src", args, self)  # src dataset, host and ssh options
        self.dst: Final[Remote] = Remote("dst", args, self)  # dst dataset, host and ssh options
        self.create_src_snapshots_config: Final[CreateSrcSnapshotConfig] = CreateSrcSnapshotConfig(args, self)
        self.monitor_snapshots_config: Final[MonitorSnapshotsConfig] = MonitorSnapshotsConfig(args, self)
        self.is_caching_snapshots: Final[bool] = args.cache_snapshots == "true"

        self.compression_program: Final[str] = self._program_name(args.compression_program)
        self.compression_program_opts: Final[list[str]] = self.split_args(args.compression_program_opts)
        for opt in {"-o", "--output-file"}.intersection(self.compression_program_opts):
            die(f"--compression-program-opts: {opt} is disallowed for security reasons.")
        self.getconf_program: Final[str] = self._program_name("getconf")  # print number of CPUs on POSIX
        self.mbuffer_program: Final[str] = self._program_name(args.mbuffer_program)
        self.mbuffer_program_opts: Final[list[str]] = self.split_args(args.mbuffer_program_opts)
        for opt in {"-i", "-I", "-o", "-O", "-l", "-L", "-t", "-T", "-a", "-A"}.intersection(self.mbuffer_program_opts):
            die(f"--mbuffer-program-opts: {opt} is disallowed for security reasons.")
        self.ps_program: Final[str] = self._program_name(args.ps_program)
        self.pv_program: Final[str] = self._program_name(args.pv_program)
        self.pv_program_opts: Final[list[str]] = self.split_args(args.pv_program_opts)
        bad_pv_opts = {"-o", "--output", "-f", "--log-file", "-S", "--stop-at-size", "-Y", "--sync", "-X", "--discard",
                       "-U", "--store-and-forward", "-d", "--watchfd", "-R", "--remote", "-P", "--pidfile"}  # fmt: skip
        for opt in bad_pv_opts.intersection(self.pv_program_opts):
            die(f"--pv-program-opts: {opt} is disallowed for security reasons.")
        if args.bwlimit:
            self.pv_program_opts.extend([f"--rate-limit={self.validate_arg_str(args.bwlimit)}"])
        self.shell_program_local: Final[str] = "sh"
        self.shell_program: Final[str] = self._program_name(args.shell_program)
        self.ssh_program: Final[str] = self._program_name(args.ssh_program)
        self.sudo_program: Final[str] = self._program_name(args.sudo_program)
        self.uname_program: Final[str] = self._program_name("uname")
        self.zfs_program: Final[str] = self._program_name("zfs")
        self.zpool_program: Final[str] = self._program_name(args.zpool_program)

        # no point creating complex shell pipeline commands for tiny data transfers:
        self.min_pipe_transfer_size: Final[int] = getenv_int("min_pipe_transfer_size", 1024 * 1024)
        self.max_datasets_per_batch_on_list_snaps: Final[int] = getenv_int("max_datasets_per_batch_on_list_snaps", 1024)
        self.max_datasets_per_minibatch_on_list_snaps: int = getenv_int("max_datasets_per_minibatch_on_list_snaps", -1)
        self.max_snapshots_per_minibatch_on_delete_snaps = getenv_int("max_snapshots_per_minibatch_on_delete_snaps", 2**29)
        self.dedicated_tcp_connection_per_zfs_send: Final[bool] = getenv_bool("dedicated_tcp_connection_per_zfs_send", True)
        # threads: with --force-once we intentionally coerce to a single-threaded run to ensure deterministic serial behavior
        self.threads: Final[tuple[int, bool]] = (1, False) if self.force_once else args.threads
        timeout_nanos = None if args.timeout is None else 1_000_000 * parse_duration_to_milliseconds(args.timeout)
        self.timeout_nanos: Final[int | None] = timeout_nanos
        self.no_estimate_send_size: Final[bool] = args.no_estimate_send_size
        self.remote_conf_cache_ttl_nanos: Final[int] = 1_000_000 * parse_duration_to_milliseconds(
            args.daemon_remote_conf_cache_ttl
        )

        self.os_cpu_count: Final[int | None] = os.cpu_count()
        self.os_getuid: Final[int] = os.getuid()
        self.os_geteuid: Final[int] = os.geteuid()
        self.prog_version: Final[str] = __version__
        self.python_version: Final[str] = sys.version
        self.platform_version: Final[str] = platform.version()
        self.platform_platform: Final[str] = platform.platform()

        # mutable variables:
        snapshot_filters = args.snapshot_filters_var if hasattr(args, SNAPSHOT_FILTERS_VAR) else [[]]
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
        self.zpool_features: dict[str, dict[str, dict[str, str]]] = {r.location: {} for r in [self.src, self.dst]}
        self.connection_pools: dict[str, ConnectionPools] = {}

    def split_args(self, text: str, *items: str | Iterable[str], allow_all: bool = False) -> list[str]:
        """Splits option string on runs of one or more whitespace into an option list."""
        text = text.strip()
        opts = self.one_or_more_whitespace_regex.split(text) if text else []
        xappend(opts, items)
        if not allow_all:
            self._validate_quoting(opts)
        return opts

    def validate_arg(self, opt: str, allow_spaces: bool = False, allow_all: bool = False) -> str | None:
        """allow_all permits all characters, including whitespace and quotes; See squote() and dquote()."""
        if allow_all or opt is None:
            return opt
        if any(char.isspace() and (char != " " or not allow_spaces) for char in opt):
            die(f"Option must not contain a whitespace character{' other than space' if allow_spaces else ''}: {opt}")
        self._validate_quoting([opt])
        return opt

    def validate_arg_str(self, opt: str, allow_spaces: bool = False, allow_all: bool = False) -> str:
        """Returns validated option string, raising if missing or illegal."""
        if opt is None:
            die("Option must not be missing")
        self.validate_arg(opt, allow_spaces=allow_spaces, allow_all=allow_all)
        return opt

    @staticmethod
    def _validate_quoting(opts: list[str]) -> None:
        """Raises an error if any option contains a quote or shell metacharacter."""
        for opt in opts:
            if "'" in opt or '"' in opt or "$" in opt or "`" in opt:
                die(f"Option must not contain a single quote or double quote or dollar or backtick character: {opt}")

    @staticmethod
    def _fix_recv_opts(opts: list[str], preserve_properties: frozenset[str]) -> tuple[list[str], list[str]]:
        """Returns sanitized ``zfs recv`` options and captured ``-o/-x`` args."""
        return _fix_send_recv_opts(
            opts,
            exclude_long_opts={"--dryrun"},
            exclude_short_opts="n",
            include_arg_opts={"-o", "-x"},
            preserve_properties=preserve_properties,
        )

    @staticmethod
    def _fix_send_opts(opts: list[str]) -> list[str]:
        """Returns sanitized ``zfs send`` options."""
        return _fix_send_recv_opts(
            opts,
            exclude_long_opts={"--dryrun"},
            exclude_short_opts="den",
            include_arg_opts={"-X", "--exclude", "--redact"},
            exclude_arg_opts=frozenset({"-i", "-I"}),
        )[0]

    def _program_name(self, program: str) -> str:
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

    def _unset_matching_env_vars(self, args: argparse.Namespace) -> None:
        """Unset environment variables matching regex filters."""
        if len(args.exclude_envvar_regex) == 0 and len(args.include_envvar_regex) == 0:
            return  # fast path
        exclude_envvar_regexes: RegexList = compile_regexes(args.exclude_envvar_regex)
        include_envvar_regexes: RegexList = compile_regexes(args.include_envvar_regex)
        # Mutate global state at most once, atomically. First thread wins. The latch isn't strictly necessary for
        # correctness as all concurrent bzfs.Job instances in bzfs_jobrunner have identical include/exclude_envvar_regex
        # anyway. It's just for reduced latency.
        with _UNSET_ENV_VARS_LOCK:
            if _UNSET_ENV_VARS_LATCH.get_and_set(False):
                for envvar_name in list(os.environ):
                    # order of include vs exclude is intentionally reversed to correctly implement semantics:
                    # "unset env var iff excluded and not included (include takes precedence)."
                    if is_included(envvar_name, exclude_envvar_regexes, include_envvar_regexes):
                        os.environ.pop(envvar_name, None)
                        self.log.debug("Unsetting b/c envvar regex: %s", envvar_name)

    def lock_file_name(self) -> str:
        """Returns unique path used to detect concurrently running jobs.

        Makes it such that a job that runs periodically declines to start if the same previous periodic job is still running
        without completion yet. Hashed key avoids overly long filenames while remaining deterministic.
        """
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
               self.src.basis_ssh_host, self.dst.basis_ssh_host,
               self.src.basis_ssh_user, self.dst.basis_ssh_user)
        # fmt: on
        hash_code: str = sha256_hex(str(key))
        log_parent_dir: str = os.path.dirname(self.log_params.log_dir)
        locks_dir: str = os.path.join(log_parent_dir, ".locks")
        os.makedirs(locks_dir, mode=DIR_PERMISSIONS, exist_ok=True)
        validate_is_not_a_symlink("--locks-dir ", locks_dir)
        validate_file_permissions(locks_dir, DIR_PERMISSIONS)
        return os.path.join(locks_dir, f"{PROG_NAME}-lockfile-{hash_code}.lock")

    def dry(self, msg: str) -> str:
        """Prefix ``msg`` with 'Dry' when running in dry-run mode."""
        return bzfs_main.utils.dry(msg, self.dry_run)

    def is_program_available(self, program: str, location: str) -> bool:
        """Return True if ``program`` was detected on ``location`` host."""
        return program in self.available_programs.get(location, {})


#############################################################################
class Remote:
    """Connection settings for either source or destination host."""

    def __init__(self, loc: str, args: argparse.Namespace, p: Params) -> None:
        """Reads from ArgumentParser via args."""
        # immutable variables:
        assert loc == "src" or loc == "dst"
        self.location: Final[str] = loc
        self.params: Final[Params] = p
        self.basis_ssh_user: Final[str] = getattr(args, f"ssh_{loc}_user")
        self.basis_ssh_host: Final[str] = getattr(args, f"ssh_{loc}_host")
        self.ssh_port: Final[int | None] = getattr(args, f"ssh_{loc}_port")
        self.ssh_config_file: Final[str | None] = p.validate_arg(getattr(args, f"ssh_{loc}_config_file"))
        if self.ssh_config_file:
            if "bzfs_ssh_config" not in os.path.basename(self.ssh_config_file):
                die(f"Basename of --ssh-{loc}-config-file must contain substring 'bzfs_ssh_config': {self.ssh_config_file}")
        self.ssh_config_file_hash: Final[str] = (
            sha256_urlsafe_base64(os.path.abspath(self.ssh_config_file), padding=False) if self.ssh_config_file else ""
        )
        self.ssh_cipher: Final[str] = p.validate_arg_str(args.ssh_cipher)
        # disable interactive password prompts and X11 forwarding and pseudo-terminal allocation:
        self.ssh_extra_opts: Final[list[str]] = ["-oBatchMode=yes", "-oServerAliveInterval=0", "-x", "-T"] + (
            ["-v"] if args.verbose >= 3 else []
        )
        self.max_concurrent_ssh_sessions_per_tcp_connection: Final[int] = args.max_concurrent_ssh_sessions_per_tcp_connection
        self.ssh_exit_on_shutdown: Final[bool] = args.ssh_exit_on_shutdown
        self.ssh_control_persist_secs: Final[int] = args.ssh_control_persist_secs
        self.socket_prefix: Final[str] = "s"
        self.reuse_ssh_connection: Final[bool] = getenv_bool("reuse_ssh_connection", True)
        if self.reuse_ssh_connection:
            ssh_home_dir: str = os.path.join(HOME_DIRECTORY, ".ssh")
            os.makedirs(ssh_home_dir, mode=DIR_PERMISSIONS, exist_ok=True)
            self.ssh_socket_dir: Final[str] = os.path.join(ssh_home_dir, "bzfs")
            os.makedirs(self.ssh_socket_dir, mode=DIR_PERMISSIONS, exist_ok=True)
            validate_file_permissions(self.ssh_socket_dir, mode=DIR_PERMISSIONS)
            self.ssh_exit_on_shutdown_socket_dir: Final[str] = os.path.join(self.ssh_socket_dir, "x")
            os.makedirs(self.ssh_exit_on_shutdown_socket_dir, mode=DIR_PERMISSIONS, exist_ok=True)
            validate_file_permissions(self.ssh_exit_on_shutdown_socket_dir, mode=DIR_PERMISSIONS)
            _delete_stale_files(self.ssh_exit_on_shutdown_socket_dir, self.socket_prefix, ssh=True)
        self.sanitize1_regex: Final[re.Pattern[str]] = re.compile(r"[\s\\/@$]")  # replace whitespace, /, $, \, @ with ~ char
        self.sanitize2_regex: Final[re.Pattern[str]] = re.compile(rf"[^a-zA-Z0-9{re.escape('~.:_-')}]")  # remove bad chars

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

    def local_ssh_command(self, socket_file: str | None) -> list[str]:
        """Returns the ssh CLI command to run locally in order to talk to the remote host; This excludes the (trailing)
        command to run on the remote host, which will be appended later."""
        if not self.ssh_user_host:
            return []  # dataset is on local host - don't use ssh

        # dataset is on remote host
        p: Params = self.params
        if p.ssh_program == DISABLE_PRG:
            die("Cannot talk to remote host because ssh CLI is disabled.")
        ssh_cmd: list[str] = [p.ssh_program] + self.ssh_extra_opts
        if self.ssh_config_file:
            ssh_cmd += ["-F", self.ssh_config_file]
        if self.ssh_cipher:
            ssh_cmd += ["-c", self.ssh_cipher]
        if self.ssh_port:
            ssh_cmd += ["-p", str(self.ssh_port)]
        if self.reuse_ssh_connection:
            # Performance: reuse ssh connection for low latency startup of frequent ssh invocations via the 'ssh -S' and
            # 'ssh -S -M -oControlPersist=60s' options. See https://en.wikibooks.org/wiki/OpenSSH/Cookbook/Multiplexing
            if not socket_file:
                # Generate unique private Unix domain socket file name in user's home dir and pass it to 'ssh -S /path/to/socket'
                def sanitize(name: str) -> str:
                    name = self.sanitize1_regex.sub("~", name)  # replace whitespace, /, $, \, @ with a ~ tilde char
                    name = self.sanitize2_regex.sub("", name)  # Remove disallowed chars
                    return name

                max_rand: int = 999_999_999_999
                rand_str: str = urlsafe_base64(random.SystemRandom().randint(0, max_rand), max_value=max_rand, padding=False)
                curr_time: str = urlsafe_base64(time.time_ns(), max_value=2**64 - 1, padding=False)
                unique: str = f"{os.getpid()}@{curr_time}@{rand_str}"
                optional: str = f"@{sanitize(self.ssh_host)[:45]}@{sanitize(self.ssh_user)}"
                socket_name: str = f"{self.socket_prefix}{unique}{optional}"
                socket_file = os.path.join(self.ssh_exit_on_shutdown_socket_dir, socket_name)
                socket_file = socket_file[0 : max(UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH, len(socket_file) - len(optional))]
                # `ssh` will error out later if the max OS Unix domain socket path limit cannot be met reasonably as the
                # home directory path is too long, typically because the Unix user name is unreasonably long.
            ssh_cmd += ["-S", socket_file]
        ssh_cmd += [self.ssh_user_host]
        return ssh_cmd

    def cache_key(self) -> tuple[str, str, int | None, str | None]:
        """Returns tuple uniquely identifying this Remote for caching."""
        return self.location, self.ssh_user_host, self.ssh_port, self.ssh_config_file

    def cache_namespace(self) -> str:
        """Returns cache namespace string which is a stable, unique directory component for snapshot caches that
        distinguishes endpoints by username+host+port+ssh_config_file where applicable, and uses '-' when no user/host is
        present (local mode)."""
        if not self.ssh_user_host:
            return "-"  # local mode
        return f"{self.ssh_user_host}#{self.ssh_port or ''}#{self.ssh_config_file_hash}"

    def __repr__(self) -> str:
        return str(self.__dict__)


#############################################################################
class CopyPropertiesConfig:
    """--zfs-recv-o* and --zfs-recv-x* option groups for copying or excluding ZFS properties on receive."""

    def __init__(self, group: str, flag: str, args: argparse.Namespace, p: Params) -> None:
        """Reads from ArgumentParser via args."""
        assert group in ZFS_RECV_GROUPS
        # immutable variables:
        grup: str = group
        self.group: Final[str] = group  # one of zfs_recv_o, zfs_recv_x
        self.flag: Final[str] = flag  # one of -o or -x
        sources: str = p.validate_arg_str(getattr(args, f"{grup}_sources"))
        self.sources: Final[str] = ",".join(sorted([s.strip() for s in sources.strip().split(",")]))  # canonicalize
        self.targets: Final[str] = p.validate_arg_str(getattr(args, f"{grup}_targets"))
        include_regexes: list[str] | None = getattr(args, f"{grup}_include_regex")
        assert ZFS_RECV_O in ZFS_RECV_GROUPS
        if include_regexes is None:
            include_regexes = [ZFS_RECV_O_INCLUDE_REGEX_DEFAULT] if grup == ZFS_RECV_O else []
        self.include_regexes: Final[RegexList] = compile_regexes(include_regexes)
        self.exclude_regexes: Final[RegexList] = compile_regexes(getattr(args, f"{grup}_exclude_regex"))

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

    def notimestamp_str(self) -> str:  # bzfs_us-west-1_hourly
        """Returns the concatenation of all parts except for the timestamp part."""
        return f"{self.prefix}{self.infix}{self.suffix}"

    def validate_label(self, input_text: str) -> None:
        """Validates that the composed snapshot label forms a legal name."""
        name: str = str(self)
        validate_dataset_name(name, input_text)
        if "/" in name:
            die(f"Invalid ZFS snapshot name: '{name}' for: '{input_text}'")
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
class CreateSrcSnapshotConfig:
    """Option values for --create-src-snapshots, that is, for automatically creating source snapshots."""

    def __init__(self, args: argparse.Namespace, p: Params) -> None:
        """Option values for --create-src-snapshots*; reads from ArgumentParser via args."""
        # immutable variables:
        self.skip_create_src_snapshots: Final[bool] = not args.create_src_snapshots
        self.create_src_snapshots_even_if_not_due: Final[bool] = args.create_src_snapshots_even_if_not_due
        tz_spec: str | None = args.create_src_snapshots_timezone if args.create_src_snapshots_timezone else None
        self.tz: Final[tzinfo | None] = get_timezone(tz_spec)
        self.current_datetime: datetime = current_datetime(tz_spec)
        self.timeformat: Final[str] = args.create_src_snapshots_timeformat
        self.anchors: Final[PeriodAnchors] = PeriodAnchors.parse(args)

        # Compute the schedule for upcoming periodic time events (suffix_durations). This event schedule is also used in
        # daemon mode via sleep_until_next_daemon_iteration()
        suffixes: list[str] = []
        labels: list[SnapshotLabel] = []
        create_src_snapshots_plan: str = args.create_src_snapshots_plan or str({"bzfs": {"onsite": {"adhoc": 1}}})
        for org, target_periods in ast.literal_eval(create_src_snapshots_plan).items():
            for target, periods in target_periods.items():
                for period_unit, period_amount in periods.items():  # e.g. period_unit can be "10minutely" or "minutely"
                    if not isinstance(period_amount, int) or period_amount < 0:
                        die(f"--create-src-snapshots-plan: Period amount must be a non-negative integer: {period_amount}")
                    if period_amount > 0:
                        suffix: str = nsuffix(period_unit)
                        suffixes.append(suffix)
                        labels.append(SnapshotLabel(prefix=nprefix(org), infix=ninfix(target), timestamp="", suffix=suffix))
        xperiods: SnapshotPeriods = p.xperiods
        if self.skip_create_src_snapshots:
            duration_amount, duration_unit = p.xperiods.suffix_to_duration0(p.daemon_frequency)
            if duration_amount <= 0 or not duration_unit:
                die(f"Invalid --daemon-frequency: {p.daemon_frequency}")
            suffixes = [nsuffix(p.daemon_frequency)]
            labels = []
        suffix_durations: dict[str, tuple[int, str]] = {suffix: xperiods.suffix_to_duration1(suffix) for suffix in suffixes}

        def suffix_key(suffix: str) -> tuple[int, str]:
            duration_amount, duration_unit = suffix_durations[suffix]
            duration_milliseconds: int = duration_amount * xperiods.suffix_milliseconds.get(duration_unit, 0)
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
        self.suffix_durations: Final[dict[str, tuple[int, str]]] = {sfx: suffix_durations[sfx] for sfx in suffixes}  # sort
        suffix_indexes: dict[str, int] = {suffix: k for k, suffix in enumerate(suffixes)}
        labels.sort(key=lambda label: (suffix_indexes[label.suffix], label))  # take snapshots for dailies before hourlies
        self._snapshot_labels: Final[list[SnapshotLabel]] = labels
        for label in self.snapshot_labels():
            label.validate_label("--create-src-snapshots-plan ")

    def snapshot_labels(self) -> list[SnapshotLabel]:
        """Returns the snapshot name patterns for which snapshots shall be created."""
        timeformat: str = self.timeformat
        is_millis: bool = timeformat.endswith("%F")  # non-standard hack to append milliseconds
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
    """Thresholds controlling when alerts fire for snapshot age."""

    kind: Literal["Latest", "Oldest"]
    warning_millis: int
    critical_millis: int


#############################################################################
@dataclass(frozen=True)
class MonitorSnapshotAlert:
    """Alert configuration for a single monitored snapshot label."""

    label: SnapshotLabel
    latest: AlertConfig | None
    oldest: AlertConfig | None


#############################################################################
class MonitorSnapshotsConfig:
    """Option values for --monitor-snapshots*, that is, policy describing which snapshots to monitor for staleness."""

    def __init__(self, args: argparse.Namespace, p: Params) -> None:
        """Reads from ArgumentParser via args."""
        # immutable variables:
        self.monitor_snapshots: Final[dict] = ast.literal_eval(args.monitor_snapshots)
        self.dont_warn: Final[bool] = args.monitor_snapshots_dont_warn
        self.dont_crit: Final[bool] = args.monitor_snapshots_dont_crit
        self.no_latest_check: Final[bool] = args.monitor_snapshots_no_latest_check
        self.no_oldest_check: Final[bool] = args.monitor_snapshots_no_oldest_check
        alerts: list[MonitorSnapshotAlert] = []
        xperiods: SnapshotPeriods = p.xperiods
        for org, target_periods in self.monitor_snapshots.items():
            prefix: str = nprefix(org)
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
                            context: str = args.monitor_snapshots
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
                            duration_milliseconds: int = duration_amount * xperiods.suffix_milliseconds.get(duration_unit, 0)
                            warning_millis += 0 if warning_millis <= 0 else cycles * duration_milliseconds
                            critical_millis += 0 if critical_millis <= 0 else cycles * duration_milliseconds
                            warning_millis = UNIX_TIME_INFINITY_SECS if warning_millis <= 0 else warning_millis
                            critical_millis = UNIX_TIME_INFINITY_SECS if critical_millis <= 0 else critical_millis
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
            duration_milliseconds: int = duration_amount * xperiods.suffix_milliseconds.get(duration_unit, 0)
            return duration_milliseconds, alert.label

        alerts.sort(key=alert_sort_key, reverse=True)  # check snapshots for dailies before hourlies, and so on
        self.alerts: Final[list[MonitorSnapshotAlert]] = alerts
        self.enable_monitor_snapshots: Final[bool] = len(alerts) > 0

    def __repr__(self) -> str:
        return str(self.__dict__)


#############################################################################
def _fix_send_recv_opts(
    opts: list[str],
    exclude_long_opts: set[str],
    exclude_short_opts: str,
    include_arg_opts: set[str],
    exclude_arg_opts: frozenset[str] = frozenset(),
    preserve_properties: frozenset[str] = frozenset(),
) -> tuple[list[str], list[str]]:
    """These opts are instead managed via bzfs CLI args --dryrun, etc."""
    assert "-" not in exclude_short_opts
    results: list[str] = []
    x_names: set[str] = set(preserve_properties)
    i = 0
    n = len(opts)
    while i < n:
        opt: str = opts[i]
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


_SSH_MASTER_DOMAIN_SOCKET_FILE_PID_REGEX: Final[re.Pattern[str]] = re.compile(r"^[0-9]+")  # see local_ssh_command()


def _delete_stale_files(
    root_dir: str,
    prefix: str,
    millis: int = 60 * 60 * 1000,
    dirs: bool = False,
    exclude: str | None = None,
    ssh: bool = False,
) -> None:
    """Cleans up obsolete files; For example caused by abnormal termination, OS crash."""
    seconds: float = millis / 1000
    now: float = time.time()
    validate_is_not_a_symlink("", root_dir)
    with os.scandir(root_dir) as iterator:
        for entry in iterator:
            if entry.name == exclude or not entry.name.startswith(prefix):
                continue
            try:
                stats = entry.stat(follow_symlinks=False)
                is_dir = entry.is_dir(follow_symlinks=False)
                if ((dirs and is_dir) or (not dirs and not is_dir)) and now - stats.st_mtime >= seconds:
                    if dirs:
                        shutil.rmtree(entry.path, ignore_errors=True)
                    elif not (ssh and stat.S_ISSOCK(stats.st_mode)):
                        os.remove(entry.path)
                    elif match := _SSH_MASTER_DOMAIN_SOCKET_FILE_PID_REGEX.match(entry.name[len(prefix) :]):
                        pid: int = int(match.group(0))
                        if pid_exists(pid) is False or now - stats.st_mtime >= 31 * 24 * 60 * 60:
                            os.remove(entry.path)  # bzfs process is no longer alive; its ssh master process isn't either
            except FileNotFoundError:
                pass  # harmless


def _create_symlink(src: str, dst_dir: str, dst: str) -> None:
    """Creates dst symlink pointing to src using a relative path."""
    rel_path: str = os.path.relpath(src, start=dst_dir)
    os.symlink(src=rel_path, dst=os.path.join(dst_dir, dst))
