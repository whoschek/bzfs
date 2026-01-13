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
* Main CLI entry point for replicating and managing ZFS snapshots. It handles the low-level mechanics of `zfs send/receive`,
  data transfer, and snapshot management between two hosts.
* Overview of the bzfs.py codebase:
* The codebase starts with docs, definition of input data and associated argument parsing into a "Params" class.
* All CLI option/parameter values are reachable from the "Params" class.
* Control flow starts in main(), which kicks off a "Job".
* A Job runs one or more "tasks" via run_tasks(), each task replicating a separate dataset tree.
* The core replication algorithm is in run_task() and especially in replicate_datasets() and replicate_dataset().
* The filter algorithms that apply include/exclude policies are in filter_datasets() and filter_snapshots().
* The --create-src-snapshots-* and --delete-* and --compare-* and --monitor-* algorithms also start in run_task().
* The main retry logic is in call_with_retries() and clear_resumable_recv_state_if_necessary().
* Progress reporting for use during `zfs send/recv` data transfers is in class ProgressReporter.
* Executing a CLI command on a local or remote host is in run_ssh_command().
* Network connection management is in refresh_ssh_connection_if_necessary() and class ConnectionPool.
* Cache functionality can be found by searching for this regex: .*cach.*
* The parallel processing engine is in itr_ssh_cmd_parallel() and process_datasets_in_parallel_and_fault_tolerant().
* README.md is mostly auto-generated from the ArgumentParser help texts as the source of "truth", via update_readme.sh.
  Simply run that script whenever you change or add ArgumentParser help text.
"""

from __future__ import (
    annotations,
)
import argparse
import contextlib
import fcntl
import heapq
import itertools
import logging
import os
import re
import subprocess
import sys
import threading
import time
from collections import (
    Counter,
    defaultdict,
)
from collections.abc import (
    Collection,
    Sequence,
)
from datetime import (
    datetime,
    timedelta,
)
from logging import (
    Logger,
)
from pathlib import (
    Path,
)
from subprocess import (
    DEVNULL,
    PIPE,
    CalledProcessError,
)
from typing import (
    Any,
    Callable,
    Final,
    cast,
    final,
)

import bzfs_main.loggers
from bzfs_main.argparse_actions import (
    has_timerange_filter,
)
from bzfs_main.argparse_cli import (
    EXCLUDE_DATASET_REGEXES_DEFAULT,
)
from bzfs_main.compare_snapshot_lists import (
    run_compare_snapshot_lists,
)
from bzfs_main.configuration import (
    AlertConfig,
    CreateSrcSnapshotConfig,
    LogParams,
    MonitorSnapshotAlert,
    Params,
    Remote,
    SnapshotLabel,
)
from bzfs_main.detect import (
    DISABLE_PRG,
    RemoteConfCacheItem,
    are_bookmarks_enabled,
    detect_available_programs,
    is_caching_snapshots,
    is_dummy,
    is_zpool_feature_enabled_or_active,
)
from bzfs_main.filter import (
    SNAPSHOT_REGEX_FILTER_NAME,
    dataset_regexes,
    filter_datasets,
    filter_lines,
    filter_lines_except,
    filter_snapshots,
)
from bzfs_main.loggers import (
    get_simple_logger,
    reset_logger,
    set_logging_runtime_defaults,
)
from bzfs_main.parallel_batch_cmd import (
    run_ssh_cmd_parallel,
    zfs_list_snapshots_in_parallel,
)
from bzfs_main.progress_reporter import (
    ProgressReporter,
    count_num_bytes_transferred_by_zfs_send,
)
from bzfs_main.replication import (
    delete_bookmarks,
    delete_datasets,
    delete_snapshots,
    replicate_dataset,
)
from bzfs_main.snapshot_cache import (
    MATURITY_TIME_THRESHOLD_SECS,
    MONITOR_CACHE_FILE_PREFIX,
    REPLICATION_CACHE_FILE_PREFIX,
    SnapshotCache,
    set_last_modification_time_safe,
)
from bzfs_main.util.connection import (
    SHARED,
    ConnectionPool,
    MiniJob,
    MiniRemote,
    timeout,
)
from bzfs_main.util.parallel_iterator import (
    run_in_parallel,
)
from bzfs_main.util.parallel_tasktree_policy import (
    process_datasets_in_parallel_and_fault_tolerant,
)
from bzfs_main.util.retry import (
    Retry,
    RetryableError,
    RetryConfig,
    RetryOptions,
    call_with_retries,
    on_exhaustion_raise,
)
from bzfs_main.util.utils import (
    DESCENDANTS_RE_SUFFIX,
    DIE_STATUS,
    DONT_SKIP_DATASET,
    FILE_PERMISSIONS,
    LOG_DEBUG,
    LOG_TRACE,
    PROG_NAME,
    SHELL_CHARS_AND_SLASH,
    UMASK,
    YEAR_WITH_FOUR_DIGITS_REGEX,
    HashedInterner,
    SortedInterner,
    Subprocesses,
    SynchronizedBool,
    SynchronizedDict,
    append_if_absent,
    compile_regexes,
    cut,
    die,
    has_duplicates,
    human_readable_bytes,
    human_readable_duration,
    is_descendant,
    percent,
    pretty_print_formatter,
    replace_in_lines,
    replace_prefix,
    sha256_85_urlsafe_base64,
    sha256_128_urlsafe_base64,
    stderr_to_str,
    termination_signal_handler,
    validate_dataset_name,
    validate_property_name,
    xappend,
    xfinally,
    xprint,
)

# constants:
__version__: Final[str] = bzfs_main.argparse_cli.__version__
CRITICAL_STATUS: Final[int] = 2
WARNING_STATUS: Final[int] = 1
STILL_RUNNING_STATUS: Final[int] = 4
MIN_PYTHON_VERSION: Final[tuple[int, int]] = (3, 9)
if sys.version_info < MIN_PYTHON_VERSION:
    print(f"ERROR: {PROG_NAME} requires Python version >= {'.'.join(map(str, MIN_PYTHON_VERSION))}!")
    sys.exit(DIE_STATUS)


#############################################################################
def argument_parser() -> argparse.ArgumentParser:
    """Returns the CLI parser used by bzfs."""
    return bzfs_main.argparse_cli.argument_parser()


def main() -> None:
    """API for command line clients."""
    prev_umask: int = os.umask(UMASK)
    try:
        set_logging_runtime_defaults()
        # On CTRL-C and SIGTERM, send signal to all descendant processes to terminate them
        termination_event: threading.Event = threading.Event()
        with termination_signal_handler(termination_events=[termination_event]):
            run_main(argument_parser().parse_args(), sys.argv, termination_event=termination_event)
    except subprocess.CalledProcessError as e:
        sys.exit(normalize_called_process_error(e))
    finally:
        os.umask(prev_umask)  # restore prior global state


def run_main(
    args: argparse.Namespace,
    sys_argv: list[str] | None = None,
    log: Logger | None = None,
    termination_event: threading.Event | None = None,
) -> None:
    """API for Python clients; visible for testing; may become a public API eventually."""
    Job(termination_event=termination_event).run_main(args, sys_argv, log)


#############################################################################
@final
class Job(MiniJob):
    """Executes one bzfs run, coordinating snapshot replication tasks."""

    def __init__(self, termination_event: threading.Event | None = None) -> None:
        self.params: Params
        self.termination_event: Final[threading.Event] = termination_event or threading.Event()
        self.subprocesses: Subprocesses = Subprocesses(termination_event=self.termination_event)
        self.retry_options: Final[RetryOptions] = RetryOptions(config=RetryConfig(termination_event=self.termination_event))
        self.all_dst_dataset_exists: Final[dict[str, dict[str, bool]]] = defaultdict(lambda: defaultdict(bool))
        self.dst_dataset_exists: SynchronizedDict[str, bool] = SynchronizedDict({})
        self.src_properties: dict[str, DatasetProperties] = {}
        self.dst_properties: dict[str, DatasetProperties] = {}
        self.all_exceptions: list[str] = []
        self.all_exceptions_count: int = 0
        self.max_exceptions_to_summarize: int = 10000
        self.first_exception: BaseException | None = None
        self.remote_conf_cache: dict[tuple, RemoteConfCacheItem] = {}
        self.max_datasets_per_minibatch_on_list_snaps: dict[str, int] = {}
        self.max_workers: dict[str, int] = {}
        self.progress_reporter: ProgressReporter = cast(ProgressReporter, None)
        self.is_first_replication_task: Final[SynchronizedBool] = SynchronizedBool(True)
        self.replication_start_time_nanos: int = time.monotonic_ns()
        self.timeout_nanos: int | None = None  # timestamp aka instant in time
        self.timeout_duration_nanos: int | None = None  # duration (not a timestamp); for logging only
        self.cache: SnapshotCache = SnapshotCache(self)
        self.stats_lock: Final[threading.Lock] = threading.Lock()
        self.num_cache_hits: int = 0
        self.num_cache_misses: int = 0
        self.num_snapshots_found: int = 0
        self.num_snapshots_replicated: int = 0

        self.is_test_mode: bool = False  # for testing only
        self.creation_prefix: str = ""  # for testing only
        self.use_select: bool = False  # for testing only
        self.progress_update_intervals: tuple[float, float] | None = None  # for testing only
        self.error_injection_triggers: dict[str, Counter[str]] = {}  # for testing only
        self.delete_injection_triggers: dict[str, Counter[str]] = {}  # for testing only
        self.param_injection_triggers: dict[str, dict[str, bool]] = {}  # for testing only
        self.inject_params: dict[str, bool] = {}  # for testing only
        self.injection_lock: threading.Lock = threading.Lock()  # for testing only
        self.max_command_line_bytes: int | None = None  # for testing only

    def shutdown(self) -> None:
        """Exits any multiplexed ssh sessions that may be leftover."""
        cache_items: Collection[RemoteConfCacheItem] = self.remote_conf_cache.values()
        for i, cache_item in enumerate(cache_items):
            cache_item.connection_pools.shutdown(f"{i + 1}/{len(cache_items)}")

    def terminate(self) -> None:
        """Shuts down gracefully; also terminates descendant processes, if any."""
        with xfinally(self.subprocesses.terminate_process_subtrees):
            self.shutdown()

    def run_main(self, args: argparse.Namespace, sys_argv: list[str] | None = None, log: Logger | None = None) -> None:
        """Parses CLI arguments, sets up logging, and executes main job loop."""
        assert isinstance(self.error_injection_triggers, dict)
        assert isinstance(self.delete_injection_triggers, dict)
        assert isinstance(self.inject_params, dict)
        logger_name_suffix: str = ""

        def _reset_logger() -> None:
            if logger_name_suffix and log is not None:  # reset Logger unless it's a Logger outside of our control
                reset_logger(log)

        with xfinally(_reset_logger):  # runs _reset_logger() on exit, without masking error raised in body of `with` block
            try:
                log_params: LogParams = LogParams(args)
                logger_name_suffix = "" if log is not None else log_params.logger_name_suffix
                log = bzfs_main.loggers.get_logger(
                    log_params=log_params, args=args, log=log, logger_name_suffix=logger_name_suffix
                )
                log.info("%s", f"Log file is: {log_params.log_file}")
            except BaseException as e:
                simple_log: Logger = get_simple_logger(PROG_NAME, logger_name_suffix=logger_name_suffix)
                try:
                    simple_log.error("Log init: %s", e, exc_info=not isinstance(e, SystemExit))
                finally:
                    reset_logger(simple_log)
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
                log.error("%s%s", f"Exiting {PROG_NAME} with status code {status_code}. Cause: ", error, exc_info=exc_info)

            try:
                log.info("CLI arguments: %s %s", " ".join(sys_argv or []), f"[uid: {os.getuid()}, euid: {os.geteuid()}]")
                if self.is_test_mode:
                    log.log(LOG_TRACE, "Parsed CLI arguments: %s", args)
                self.params = p = Params(args, sys_argv or [], log_params, log, self.inject_params)
                self.timeout_duration_nanos = p.timeout_duration_nanos
                lock_file: str = p.lock_file_name()
                lock_fd = os.open(
                    lock_file, os.O_WRONLY | os.O_TRUNC | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, FILE_PERMISSIONS
                )
                with xfinally(lambda: os.close(lock_fd)):
                    try:
                        # Acquire an exclusive lock; will raise a BlockingIOError if lock is already held by this process or
                        # another process. The (advisory) lock is auto-released when the process terminates or the fd is
                        # closed.
                        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # LOCK_NB ... non-blocking
                    except BlockingIOError:
                        msg = "Exiting as same previous periodic job is still running without completion yet per "
                        die(msg + lock_file, STILL_RUNNING_STATUS)

                    # xfinally: unlink the lock_file while still holding the flock on its fd - it's a correct and safe
                    # standard POSIX pattern:
                    # - Performing unlink() before close(fd) avoids a race where a subsequent bzfs process could recreate and
                    #   lock a fresh inode for the same path between our close() and a later unlink(). In that case, a late
                    #   unlink would delete the newer process's lock_file path.
                    # - At this point, critical work is complete; the remaining steps are shutdown mechanics that have no
                    #   side effect, so this pattern is correct, safe, and simple.
                    with xfinally(lambda: Path(lock_file).unlink(missing_ok=True)):  # don't accumulate stale files
                        try:
                            self.run_tasks()  # do the real work
                        except BaseException:
                            self.terminate()
                            raise
                        self.shutdown()
                        with contextlib.suppress(BrokenPipeError):
                            sys.stderr.flush()
                            sys.stdout.flush()
            except subprocess.CalledProcessError as e:
                log_error_on_exit(e, e.returncode)
                raise
            except SystemExit as e:
                log_error_on_exit(e, e.code)
                raise
            except (subprocess.TimeoutExpired, UnicodeDecodeError) as e:
                log_error_on_exit(e, DIE_STATUS)
                raise SystemExit(DIE_STATUS) from e
            except re.error as e:
                log_error_on_exit(f"{e} within regex {e.pattern!r}", DIE_STATUS)
                raise SystemExit(DIE_STATUS) from e
            except BaseException as e:
                log_error_on_exit(e, DIE_STATUS, exc_info=True)
                raise SystemExit(DIE_STATUS) from e
            finally:
                log.info("%s", f"Log file was: {log_params.log_file}")
            log.info("Success. Goodbye!")
            with contextlib.suppress(BrokenPipeError):
                sys.stderr.flush()
                sys.stdout.flush()

    def run_tasks(self) -> None:
        """Executes replication cycles, repeating until daemon lifetime expires."""
        p, log = self.params, self.params.log
        self.all_exceptions = []
        self.all_exceptions_count = 0
        self.first_exception = None
        self.remote_conf_cache = {}
        self.validate_once()
        self.replication_start_time_nanos = time.monotonic_ns()
        self.progress_reporter = ProgressReporter(log, p.pv_program_opts, self.use_select, self.progress_update_intervals)
        with xfinally(lambda: self.progress_reporter.stop()):
            daemon_stoptime_nanos: int = time.monotonic_ns() + p.daemon_lifetime_nanos
            while True:  # loop for daemon mode
                self.timeout_nanos = (
                    None if p.timeout_duration_nanos is None else time.monotonic_ns() + p.timeout_duration_nanos
                )
                self.all_dst_dataset_exists.clear()
                self.progress_reporter.reset()
                src, dst = p.src, p.dst
                for src_root_dataset, dst_root_dataset in p.root_dataset_pairs:
                    if self.termination_event.is_set():
                        self.terminate()
                        break
                    src.root_dataset = src.basis_root_dataset = src_root_dataset
                    dst.root_dataset = dst.basis_root_dataset = dst_root_dataset
                    p.curr_zfs_send_program_opts = p.zfs_send_program_opts.copy()
                    if p.daemon_lifetime_nanos > 0:
                        self.timeout_nanos = (
                            None if p.timeout_duration_nanos is None else time.monotonic_ns() + p.timeout_duration_nanos
                        )
                    recurs_sep = " " if p.recursive_flag else ""
                    task_description = f"{src.basis_root_dataset} {p.recursive_flag}{recurs_sep}--> {dst.basis_root_dataset}"
                    if len(p.root_dataset_pairs) > 1:
                        log.info("Starting task: %s", task_description + " ...")
                    try:
                        try:
                            self.maybe_inject_error(cmd=[], error_trigger="retryable_run_tasks")
                            timeout(self)
                            self.validate_task()
                            self.run_task()  # do the real work
                        except RetryableError as retryable_error:
                            cause: BaseException | None = retryable_error.__cause__
                            assert cause is not None
                            raise cause.with_traceback(cause.__traceback__)  # noqa: B904 re-raise of cause without chaining
                    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, SystemExit, UnicodeDecodeError) as e:
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
            msgs = "\n".join(f"{i + 1}/{error_count}: {e}" for i, e in enumerate(self.all_exceptions))
            log.error("%s", f"Tolerated {error_count} errors. Error Summary: \n{msgs}")
            assert self.first_exception is not None
            raise self.first_exception

    def append_exception(self, e: BaseException, task_name: str, task_description: str) -> None:
        """Records and logs an exception that was encountered while running a subtask."""
        self.first_exception = self.first_exception or e
        if len(self.all_exceptions) < self.max_exceptions_to_summarize:  # cap max memory consumption
            self.all_exceptions.append(str(e))
        self.all_exceptions_count += 1
        self.params.log.error(f"#{self.all_exceptions_count}: Done with %s: %s", task_name, task_description)

    def sleep_until_next_daemon_iteration(self, daemon_stoptime_nanos: int) -> bool:
        """Pauses until next scheduled snapshot time or daemon stop; Returns True to continue daemon loop; False to stop."""
        sleep_nanos: int = daemon_stoptime_nanos - time.monotonic_ns()
        if sleep_nanos <= 0:
            return False
        self.progress_reporter.pause()
        p, log = self.params, self.params.log
        config: CreateSrcSnapshotConfig = p.create_src_snapshots_config
        curr_datetime: datetime = config.current_datetime + timedelta(microseconds=1)
        next_snapshotting_event_dt: datetime = min(
            (
                config.anchors.round_datetime_up_to_duration_multiple(curr_datetime, duration_amount, duration_unit)
                for duration_amount, duration_unit in config.suffix_durations.values()
            ),
            default=curr_datetime + timedelta(days=10 * 365),  # infinity
        )
        offset: timedelta = next_snapshotting_event_dt - datetime.now(config.tz)
        offset_nanos: int = (offset.days * 86400 + offset.seconds) * 1_000_000_000 + offset.microseconds * 1_000
        sleep_nanos = min(sleep_nanos, max(0, offset_nanos))
        log.info("Daemon sleeping for: %s%s", human_readable_duration(sleep_nanos), f" ... [Log {p.log_params.log_file}]")
        self.termination_event.wait(sleep_nanos / 1_000_000_000)  # allow early wakeup on async termination
        config.current_datetime = datetime.now(config.tz)
        return time.monotonic_ns() < daemon_stoptime_nanos and not self.termination_event.is_set()

    def print_replication_stats(self, start_time_nanos: int) -> None:
        """Logs overall replication statistics after a job cycle completes."""
        p, log = self.params, self.params.log
        elapsed_nanos: int = time.monotonic_ns() - start_time_nanos
        msg = p.dry(f"zfs sent {self.num_snapshots_replicated} snapshots in {human_readable_duration(elapsed_nanos)}.")
        if p.is_program_available("pv", "local"):
            sent_bytes: int = count_num_bytes_transferred_by_zfs_send(p.log_params.pv_log_file)
            sent_bytes_per_sec: int = round(1_000_000_000 * sent_bytes / (elapsed_nanos or 1))
            msg += f" zfs sent {human_readable_bytes(sent_bytes)} [{human_readable_bytes(sent_bytes_per_sec)}/s]."
        log.info("%s", msg.ljust(p.log_params.terminal_columns - len("2024-01-01 23:58:45 [I] ")))

    def validate_once(self) -> None:
        """Validates CLI parameters and compiles regex lists one time only, which will later be reused many times."""
        p = self.params
        p.zfs_recv_ox_names = self.recv_option_property_names(p.zfs_recv_program_opts)
        for snapshot_filter in p.snapshot_filters:
            for _filter in snapshot_filter:
                if _filter.name == SNAPSHOT_REGEX_FILTER_NAME:
                    exclude_snapshot_regexes = compile_regexes(_filter.options[0])
                    include_snapshot_regexes = compile_regexes(_filter.options[1] or [".*"])
                    _filter.options = (exclude_snapshot_regexes, include_snapshot_regexes)

        exclude_regexes: list[str] = [EXCLUDE_DATASET_REGEXES_DEFAULT]
        if len(p.args.exclude_dataset_regex) > 0:  # some patterns don't exclude anything
            exclude_regexes = [regex for regex in p.args.exclude_dataset_regex if regex != "" and regex != "!.*"]
        include_regexes: list[str] = p.args.include_dataset_regex

        # relative datasets need not be compiled more than once as they don't change between tasks
        def separate_abs_vs_rel_datasets(datasets: list[str]) -> tuple[list[str], list[str]]:
            abs_datasets: list[str] = []
            rel_datasets: list[str] = []
            for dataset in datasets:
                (abs_datasets if dataset.startswith("/") else rel_datasets).append(dataset)
            return abs_datasets, rel_datasets

        p.abs_exclude_datasets, rel_exclude_datasets = separate_abs_vs_rel_datasets(p.args.exclude_dataset)
        p.abs_include_datasets, rel_include_datasets = separate_abs_vs_rel_datasets(p.args.include_dataset)
        suffix = DESCENDANTS_RE_SUFFIX
        p.tmp_exclude_dataset_regexes, p.tmp_include_dataset_regexes = (
            compile_regexes(exclude_regexes + dataset_regexes(p.src, p.dst, rel_exclude_datasets), suffix=suffix),
            compile_regexes(include_regexes + dataset_regexes(p.src, p.dst, rel_include_datasets), suffix=suffix),
        )

        if p.pv_program != DISABLE_PRG:
            pv_program_opts_set = set(p.pv_program_opts)
            if pv_program_opts_set.isdisjoint({"--bytes", "-b", "--bits", "-8"}):
                die("--pv-program-opts must contain one of --bytes or --bits for progress metrics to function.")
            if not p.log_params.quiet:
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
        """Validates a single replication task before execution."""
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

        suffx: str = DESCENDANTS_RE_SUFFIX  # also match descendants of a matching dataset
        p.exclude_dataset_regexes, p.include_dataset_regexes = (
            p.tmp_exclude_dataset_regexes + compile_regexes(dataset_regexes(src, dst, p.abs_exclude_datasets), suffix=suffx),
            p.tmp_include_dataset_regexes + compile_regexes(dataset_regexes(src, dst, p.abs_include_datasets), suffix=suffx),
        )
        if len(p.include_dataset_regexes) == 0:
            p.include_dataset_regexes = [(re.compile(r".*"), False)]

        detect_available_programs(self)

        if is_zpool_feature_enabled_or_active(p, dst, "feature@large_blocks"):
            append_if_absent(p.curr_zfs_send_program_opts, "--large-block")

        self.max_workers = {}
        self.max_datasets_per_minibatch_on_list_snaps = {}
        for r in [src, dst]:
            cpus: int = int(p.available_programs[r.location].get("getconf_cpu_count", 8))
            threads, is_percent = p.threads
            cpus = max(1, round(cpus * threads / 100.0) if is_percent else round(threads))
            self.max_workers[r.location] = cpus
            bs: int = max(1, p.max_datasets_per_batch_on_list_snaps)  # 1024 by default
            max_datasets_per_minibatch: int = p.max_datasets_per_minibatch_on_list_snaps
            if max_datasets_per_minibatch <= 0:
                max_datasets_per_minibatch = max(1, bs // cpus)
            max_datasets_per_minibatch = min(bs, max_datasets_per_minibatch)
            self.max_datasets_per_minibatch_on_list_snaps[r.location] = max_datasets_per_minibatch
            log.log(
                LOG_TRACE,
                "%s",
                f"max_datasets_per_batch_on_list_snaps: {p.max_datasets_per_batch_on_list_snaps}, "
                f"max_datasets_per_minibatch_on_list_snaps: {max_datasets_per_minibatch}, "
                f"max_workers: {self.max_workers[r.location]}, "
                f"location: {r.location}",
            )
        if self.is_test_mode:
            log.log(LOG_TRACE, "Validated Param values: %s", pretty_print_formatter(self.params))

    def sudo_cmd(self, ssh_user_host: str, ssh_user: str) -> tuple[str, bool]:
        """Returns sudo command prefix and whether root privileges are required."""
        p: Params = self.params
        assert isinstance(ssh_user_host, str)
        assert isinstance(ssh_user, str)
        assert isinstance(p.sudo_program, str)
        assert isinstance(p.enable_privilege_elevation, bool)

        is_root: bool = True
        if ssh_user_host != "":
            if ssh_user == "":
                if os.getuid() != 0:
                    is_root = False
            elif ssh_user != "root":
                is_root = False
        elif os.getuid() != 0:
            is_root = False

        if is_root:
            sudo = ""  # using sudo in an attempt to make ZFS operations work even if we are not root user?
            use_zfs_delegation = False  # or instead using 'zfs allow' delegation?
            return sudo, use_zfs_delegation
        elif p.enable_privilege_elevation:
            if p.sudo_program == DISABLE_PRG:
                die(f"sudo CLI is not available on host: {ssh_user_host or 'localhost'}")
            # The '-n' option makes 'sudo' safer and more fail-fast. It avoids having sudo prompt the user for input of any
            # kind. If a password is required for the sudo command to run, sudo will display an error message and exit.
            return p.sudo_program + " -n", False
        else:
            return "", True

    def run_task(self) -> None:
        """Replicates all snapshots for the current root dataset pair."""

        def filter_src_datasets() -> list[str]:  # apply --{include|exclude}-dataset policy
            return filter_datasets(self, src, basis_src_datasets) if src_datasets is None else src_datasets

        p, log = self.params, self.params.log
        src, dst = p.src, p.dst
        max_workers: int = min(self.max_workers[src.location], self.max_workers[dst.location])
        recursive_sep: str = " " if p.recursive_flag else ""
        task_description: str = f"{src.basis_root_dataset} {p.recursive_flag}{recursive_sep}--> {dst.basis_root_dataset} ..."
        failed: bool = False
        src_datasets: list[str] | None = None
        basis_src_datasets: list[str] = []
        self.src_properties = {}
        self.dst_properties = {}
        if not is_dummy(src):  # find src dataset or all datasets in src dataset tree (with --recursive)
            basis_src_datasets = self.list_src_datasets_task()

        if not p.create_src_snapshots_config.skip_create_src_snapshots:
            log.info(p.dry("--create-src-snapshots: %s"), f"{src.basis_root_dataset} {p.recursive_flag}{recursive_sep}...")
            src_datasets = filter_src_datasets()  # apply include/exclude policy
            self.create_src_snapshots_task(basis_src_datasets, src_datasets)

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
        basis_dst_datasets: list[str] = self.list_dst_datasets_task()
        dst_datasets: list[str] = filter_datasets(self, dst, basis_dst_datasets)  # apply include/exclude policy

        if p.delete_dst_datasets and not failed:
            log.info(p.dry("--delete-dst-datasets: %s"), task_description)
            basis_dst_datasets, dst_datasets = self.delete_dst_datasets_task(
                basis_src_datasets, basis_dst_datasets, dst_datasets
            )

        if p.delete_dst_snapshots and not failed:
            log.info(p.dry("--delete-dst-snapshots: %s"), task_description + f" [{len(dst_datasets)} datasets]")
            failed = self.delete_destination_snapshots_task(basis_src_datasets, dst_datasets, max_workers, task_description)

        if p.delete_empty_dst_datasets and p.recursive and not failed:
            log.info(p.dry("--delete-empty-dst-datasets: %s"), task_description)
            basis_dst_datasets, dst_datasets = self.delete_empty_dst_datasets_task(basis_dst_datasets, dst_datasets)

        if p.compare_snapshot_lists and not failed:
            log.info("--compare-snapshot-lists: %s", task_description)
            if len(basis_src_datasets) == 0 and not is_dummy(src):
                die(f"Source dataset does not exist: {src.basis_root_dataset}")
            src_datasets = filter_src_datasets()  # apply include/exclude policy
            run_compare_snapshot_lists(self, src_datasets, dst_datasets)

        if p.monitor_snapshots_config.enable_monitor_snapshots and not failed:
            log.info("--monitor-snapshots: %s", task_description)
            src_datasets = filter_src_datasets()  # apply include/exclude policy
            self.monitor_snapshots_task(src_datasets, dst_datasets, task_description)

    def list_src_datasets_task(self) -> list[str]:
        """Lists datasets on the source host."""
        p = self.params
        src = p.src
        basis_src_datasets: list[str] = []
        is_caching: bool = is_caching_snapshots(p, src)
        props: str = "volblocksize,recordsize,name"
        props = "snapshots_changed," + props if is_caching else props
        cmd: list[str] = p.split_args(
            f"{p.zfs_program} list -t filesystem,volume -s name -Hp -o {props} {p.recursive_flag}", src.root_dataset
        )
        for line in (self.try_ssh_command_with_retries(src, LOG_DEBUG, cmd=cmd) or "").splitlines():
            cols: list[str] = line.split("\t")
            snapshots_changed, volblocksize, recordsize, src_dataset = cols if is_caching else ["-"] + cols
            self.src_properties[src_dataset] = DatasetProperties(
                recordsize=int(recordsize) if recordsize != "-" else -int(volblocksize),
                snapshots_changed=int(snapshots_changed) if snapshots_changed and snapshots_changed != "-" else 0,
            )
            basis_src_datasets.append(src_dataset)
        assert (not self.is_test_mode) or basis_src_datasets == sorted(basis_src_datasets), "List is not sorted"
        return basis_src_datasets

    def list_dst_datasets_task(self) -> list[str]:
        """Lists datasets on the destination host."""
        p, log = self.params, self.params.log
        dst = p.dst
        is_caching: bool = is_caching_snapshots(p, dst) and p.monitor_snapshots_config.enable_monitor_snapshots
        props: str = "name"
        props = "snapshots_changed," + props if is_caching else props
        cmd: list[str] = p.split_args(
            f"{p.zfs_program} list -t filesystem,volume -s name -Hp -o {props} {p.recursive_flag}", dst.root_dataset
        )
        basis_dst_datasets: list[str] = []
        basis_dst_datasets_str: str | None = self.try_ssh_command_with_retries(dst, LOG_TRACE, cmd=cmd)
        if basis_dst_datasets_str is None:
            log.warning("Destination dataset does not exist: %s", dst.root_dataset)
        else:
            for line in basis_dst_datasets_str.splitlines():
                cols: list[str] = line.split("\t")
                snapshots_changed, dst_dataset = cols if is_caching else ["-"] + cols
                self.dst_properties[dst_dataset] = DatasetProperties(
                    recordsize=0,
                    snapshots_changed=int(snapshots_changed) if snapshots_changed and snapshots_changed != "-" else 0,
                )
                basis_dst_datasets.append(dst_dataset)
        assert (not self.is_test_mode) or basis_dst_datasets == sorted(basis_dst_datasets), "List is not sorted"
        return basis_dst_datasets

    def create_src_snapshots_task(self, basis_src_datasets: list[str], src_datasets: list[str]) -> None:
        """Atomically creates a new snapshot of the src datasets selected by --{include|exclude}-dataset* policy; implements
        --create-src-snapshots.

        The implementation attempts to fit as many datasets as possible into a single (atomic) 'zfs snapshot' command line,
        using lexicographical sort order, and using 'zfs snapshot -r' to the extent that this is compatible with the
        --{include|exclude}-dataset* pruning policy. The snapshots of all datasets that fit within the same single 'zfs
        snapshot' CLI invocation will be taken within the same ZFS transaction group, and correspondingly have identical
        'createtxg' ZFS property (but not necessarily identical 'creation' ZFS time property as ZFS actually provides no such
        guarantee), and thus be consistent. Dataset names that can't fit into a single command line are spread over multiple
        command line invocations, respecting the limits that the operating system places on the maximum length of a single
        command line, per `getconf ARG_MAX`.

        Time complexity is O((N log N) + (N * M log M)) where N is the number of datasets and M is the number of snapshots
        per dataset. Space complexity is O(max(N, M)).
        """
        p, log = self.params, self.params.log
        src = p.src
        if len(basis_src_datasets) == 0:
            die(f"Source dataset does not exist: {src.basis_root_dataset}")
        datasets_to_snapshot: dict[SnapshotLabel, list[str]] = self.find_datasets_to_snapshot(src_datasets)
        datasets_to_snapshot = {label: datasets for label, datasets in datasets_to_snapshot.items() if len(datasets) > 0}
        basis_datasets_to_snapshot: dict[SnapshotLabel, list[str]] = datasets_to_snapshot.copy()  # shallow copy
        commands: dict[SnapshotLabel, list[str]] = {}
        for label, datasets in datasets_to_snapshot.items():
            cmd: list[str] = p.split_args(f"{src.sudo} {p.zfs_program} snapshot")
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
        run_ssh_cmd_parallel(
            self,
            src,
            ((commands[lbl], (f"{ds}@{lbl}" for ds in datasets)) for lbl, datasets in datasets_to_snapshot.items()),
            fn=lambda cmd, batch: self.run_ssh_command_with_retries(
                src, is_dry=p.dry_run, print_stdout=True, cmd=cmd + batch, retry_on_generic_ssh_error=False
            ),  # retry_on_generic_ssh_error=False means only retry on SSH connect errors b/c `zfs snapshot` isn't idempotent
            max_batch_items=2**29,
        )
        if is_caching_snapshots(p, src):
            # perf: copy lastmodified time of source dataset into local cache to reduce future 'zfs list -t snapshot' calls
            self.cache.update_last_modified_cache(basis_datasets_to_snapshot)

    def delete_destination_snapshots_task(
        self, basis_src_datasets: list[str], dst_datasets: list[str], max_workers: int, task_description: str
    ) -> bool:
        """Deletes existing destination snapshots that do not exist within the source dataset if they are included by the
        --{include|exclude}-snapshot-* policy, and the destination dataset is included via --{include|exclude}-dataset*
        policy; implements --delete-dst-snapshots. Does not attempt to delete snapshots that carry a `zfs hold`."""
        p, log = self.params, self.params.log
        src, dst = p.src, p.dst
        kind: str = "bookmark" if p.delete_dst_bookmarks else "snapshot"
        filter_needs_creation_time: bool = has_timerange_filter(p.snapshot_filters)
        props: str = "guid,name,userrefs"
        props = self.creation_prefix + "creation," + props if filter_needs_creation_time else props
        basis_src_datasets_set: set[str] = set(basis_src_datasets)
        num_snapshots_found, num_snapshots_deleted = 0, 0

        def delete_destination_snapshots(dst_dataset: str, tid: str, retry: Retry) -> bool:  # thread-safe
            src_dataset: str = replace_prefix(dst_dataset, old_prefix=dst.root_dataset, new_prefix=src.root_dataset)
            if src_dataset in basis_src_datasets_set and (are_bookmarks_enabled(p, src) or not p.delete_dst_bookmarks):
                src_kind: str = kind
                if not p.delete_dst_snapshots_no_crosscheck:
                    src_kind = "snapshot,bookmark" if are_bookmarks_enabled(p, src) else "snapshot"
                src_cmd = p.split_args(f"{p.zfs_program} list -t {src_kind} -d 1 -s name -Hp -o guid", src_dataset)
            else:
                src_cmd = None
            dst_cmd = p.split_args(f"{p.zfs_program} list -t {kind} -d 1 -s createtxg -Hp -o {props}", dst_dataset)
            self.maybe_inject_delete(dst, dataset=dst_dataset, delete_trigger="zfs_list_delete_dst_snapshots")
            src_snaps_with_guids, dst_snaps_with_guids_str = run_in_parallel(  # list src+dst snapshots in parallel
                lambda: set(self.run_ssh_command(src, LOG_TRACE, cmd=src_cmd).splitlines() if src_cmd else []),
                lambda: self.try_ssh_command(dst, LOG_TRACE, cmd=dst_cmd),
            )
            if dst_snaps_with_guids_str is None:
                log.warning("Third party deleted destination: %s", dst_dataset)
                return False
            held_dst_snapshots: set[str] = set()
            dst_snaps_with_guids: list[str] = []
            no_userrefs: tuple[str, ...] = ("", "-", "0")  # ZFS snapshot property userrefs > 0 indicates a zfs hold
            for line in dst_snaps_with_guids_str.splitlines():
                dst_snaps_with_guids.append(line[: line.rindex("\t")])  # strip off trailing userrefs column
                _, name, userrefs = line.rsplit("\t", 2)
                if userrefs not in no_userrefs:
                    tag: str = name[name.index("@") + 1 :]
                    held_dst_snapshots.add(tag)  # don't attempt to delete snapshots that carry a `zfs hold`
            num_dst_snaps_with_guids = len(dst_snaps_with_guids)
            basis_dst_snaps_with_guids: list[str] = dst_snaps_with_guids.copy()
            if p.delete_dst_bookmarks:
                replace_in_lines(dst_snaps_with_guids, old="#", new="@", count=1)  # treat bookmarks as snapshots
            #  The check against the source dataset happens *after* filtering the dst snapshots with filter_snapshots().
            # `p.delete_dst_snapshots_except` means the user wants to specify snapshots to *retain* aka *keep*
            all_except: bool = p.delete_dst_snapshots_except
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
                except_dst_guids: set[str] = set(cut(field=1, lines=dst_snaps_with_guids)).intersection(src_snaps_with_guids)
                dst_tags_to_delete: list[str] = filter_lines_except(basis_dst_snaps_with_guids, except_dst_guids)
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
            separator: str = "#" if p.delete_dst_bookmarks else "@"
            dst_tags_to_delete = cut(field=2, separator=separator, lines=dst_tags_to_delete)
            if p.delete_dst_bookmarks:
                delete_bookmarks(self, dst, dst_dataset, snapshot_tags=dst_tags_to_delete)
            else:
                dst_tags_to_delete = [tag for tag in dst_tags_to_delete if tag not in held_dst_snapshots]
                delete_snapshots(self, dst, dst_dataset, snapshot_tags=dst_tags_to_delete)
            with self.stats_lock:
                nonlocal num_snapshots_found
                num_snapshots_found += num_dst_snaps_with_guids
                nonlocal num_snapshots_deleted
                num_snapshots_deleted += len(dst_tags_to_delete)
                if len(dst_tags_to_delete) > 0 and not p.delete_dst_bookmarks:
                    self.dst_properties[dst_dataset].snapshots_changed = 0  # invalidate cache
            return True

        # Run delete_destination_snapshots(dataset) for each dataset, while handling errors, retries + parallel exec
        failed: bool = False
        if are_bookmarks_enabled(p, dst) or not p.delete_dst_bookmarks:
            start_time_nanos = time.monotonic_ns()
            failed = process_datasets_in_parallel_and_fault_tolerant(
                log=log,
                datasets=dst_datasets,
                process_dataset=delete_destination_snapshots,  # lambda
                skip_tree_on_error=lambda dataset: False,
                skip_on_error=p.skip_on_error,
                max_workers=max_workers,
                termination_event=self.termination_event,
                termination_handler=self.terminate,
                enable_barriers=False,
                task_name="--delete-dst-snapshots",
                append_exception=self.append_exception,
                retry_options=self.retry_options.copy(policy=p.retry_policy),
                dry_run=p.dry_run,
                is_test_mode=self.is_test_mode,
            )
            elapsed_nanos = time.monotonic_ns() - start_time_nanos
            log.info(
                p.dry("--delete-dst-snapshots: %s"),
                task_description + f" [Deleted {num_snapshots_deleted} out of {num_snapshots_found} {kind}s "
                f"within {len(dst_datasets)} datasets; took {human_readable_duration(elapsed_nanos)}]",
            )
        return failed

    def delete_dst_datasets_task(
        self, basis_src_datasets: list[str], basis_dst_datasets: list[str], sorted_dst_datasets: list[str]
    ) -> tuple[list[str], list[str]]:
        """Deletes existing destination datasets that do not exist within the source dataset if they are included via
        --{include|exclude}-dataset* policy; implements --delete-dst-datasets.

        Do not recurse without --recursive. With --recursive, never delete non-selected dataset subtrees or their ancestors.
        """
        p = self.params
        src, dst = p.src, p.dst
        children: dict[str, set[str]] = defaultdict(set)
        for dst_dataset in basis_dst_datasets:  # Compute the direct children of each NON-FILTERED dataset
            parent: str = os.path.dirname(dst_dataset)
            children[parent].add(dst_dataset)
        to_delete: set[str] = set()
        for dst_dataset in reversed(sorted_dst_datasets):  # Reverse order facilitates efficient O(N) time algorithm
            if children[dst_dataset].issubset(to_delete):
                to_delete.add(dst_dataset)  # all children are deletable, thus the dataset itself is deletable too
        to_delete = to_delete.difference(
            replace_prefix(src_dataset, src.root_dataset, dst.root_dataset) for src_dataset in basis_src_datasets
        )
        delete_datasets(self, dst, to_delete)
        sorted_dst_datasets = sorted(set(sorted_dst_datasets).difference(to_delete))
        basis_dst_datasets = sorted(set(basis_dst_datasets).difference(to_delete))
        return basis_dst_datasets, sorted_dst_datasets

    def delete_empty_dst_datasets_task(
        self, basis_dst_datasets: list[str], sorted_dst_datasets: list[str]
    ) -> tuple[list[str], list[str]]:
        """Deletes any existing destination dataset that has no snapshot and no bookmark if all descendants of that dataset
        do not have a snapshot or bookmark either; implements --delete-empty-dst-datasets.

        To do so, we walk the dataset list (conceptually, a tree) depth-first (i.e. sorted descending). If a dst dataset has
        zero snapshots and zero bookmarks and all its children are already marked as orphans, then it is itself an orphan,
        and we mark it as such. Walking in a reverse sorted way means that we efficiently check for zero snapshots/bookmarks
        not just over the direct children but the entire tree. Finally, delete all orphan datasets in an efficient batched
        way.
        """
        p = self.params
        dst = p.dst

        # Compute the direct children of each NON-FILTERED dataset. Thus, no non-selected dataset and no ancestor of a
        # non-selected dataset will ever be added to the "orphan" set. In other words, this treats non-selected dataset
        # subtrees as if they all had snapshots, so non-selected dataset subtrees and their ancestors are guaranteed
        # to not get deleted.
        children: dict[str, set[str]] = defaultdict(set)
        for dst_dataset in basis_dst_datasets:
            parent: str = os.path.dirname(dst_dataset)
            children[parent].add(dst_dataset)

        def compute_orphans(datasets_having_snapshots: set[str]) -> set[str]:
            """Returns destination datasets having zero snapshots whose children are all orphans."""
            orphans: set[str] = set()
            for dst_dataset in reversed(sorted_dst_datasets):  # Reverse order facilitates efficient O(N) time algorithm
                if (dst_dataset not in datasets_having_snapshots) and children[dst_dataset].issubset(orphans):
                    orphans.add(dst_dataset)
            return orphans

        # Compute candidate orphan datasets, which reduces the list of datasets for which we list snapshots via
        # 'zfs list -t snapshot ...' from dst_datasets to a subset of dst_datasets, which in turn reduces I/O and improves
        # perf. Essentially, this eliminates the I/O to list snapshots for ancestors of excluded datasets.
        candidate_orphans: set[str] = compute_orphans(set())

        # Compute destination datasets having more than zero snapshots
        dst_datasets_having_snapshots: set[str] = set()
        with_bookmarks: bool = p.delete_empty_dst_datasets_if_no_bookmarks_and_no_snapshots and are_bookmarks_enabled(p, dst)
        btype: str = "bookmark,snapshot" if with_bookmarks else "snapshot"
        cmd: list[str] = p.split_args(f"{p.zfs_program} list -t {btype} -d 1 -S name -Hp -o name")
        for snapshots in zfs_list_snapshots_in_parallel(self, dst, cmd, sorted(candidate_orphans), ordered=False):
            if with_bookmarks:
                replace_in_lines(snapshots, old="#", new="@", count=1)  # treat bookmarks as snapshots
            dst_datasets_having_snapshots.update(snap[: snap.index("@")] for snap in snapshots)  # union

        orphans = compute_orphans(dst_datasets_having_snapshots)  # compute the real orphans
        delete_datasets(self, dst, orphans)  # finally, delete the orphan datasets in an efficient way
        sorted_dst_datasets = sorted(set(sorted_dst_datasets).difference(orphans))
        basis_dst_datasets = sorted(set(basis_dst_datasets).difference(orphans))
        return basis_dst_datasets, sorted_dst_datasets

    def monitor_snapshots_task(
        self, sorted_src_datasets: list[str], sorted_dst_datasets: list[str], task_description: str
    ) -> None:
        """Monitors src and dst snapshots; implements --monitor-snapshots."""
        p, log = self.params, self.params.log
        src, dst = p.src, p.dst
        num_cache_hits: int = self.num_cache_hits
        num_cache_misses: int = self.num_cache_misses
        start_time_nanos: int = time.monotonic_ns()
        dst_alert, src_alert = run_in_parallel(
            lambda: self.monitor_snapshots(dst, sorted_dst_datasets),
            lambda: self.monitor_snapshots(src, sorted_src_datasets),
        )
        exit_code, _exit_kind, exit_msg = min(dst_alert, src_alert)
        if exit_code != 0:
            die(exit_msg, -exit_code)
        elapsed: str = human_readable_duration(time.monotonic_ns() - start_time_nanos)
        num_cache_hits = self.num_cache_hits - num_cache_hits
        num_cache_misses = self.num_cache_misses - num_cache_misses
        if num_cache_hits > 0 or num_cache_misses > 0:
            msg = self._cache_hits_msg(hits=num_cache_hits, misses=num_cache_misses)
        else:
            msg = ""
        log.info(
            "--monitor-snapshots done: %s",
            f"{task_description} [{len(sorted_src_datasets) + len(sorted_dst_datasets)} datasets; took {elapsed}{msg}]",
        )

    def monitor_snapshots(self, remote: Remote, sorted_datasets: list[str]) -> tuple[int, str, str]:
        """Checks snapshot freshness and warns or errors out when limits are exceeded.

        Alerts the user if the ZFS 'creation' time property of the latest or oldest snapshot for any specified snapshot name
        pattern within the selected datasets is too old wrt. the specified age limit. The purpose is to check if snapshots
        are successfully taken on schedule, successfully replicated on schedule, and successfully pruned on schedule. Process
        exit code is 0, 1, 2 on OK, WARNING, CRITICAL, respectively.

        Time complexity is O((N log N) + (N * M log M)) where N is the number of datasets and M is the number of snapshots
        per dataset. Space complexity is O(max(N, M)).
        """
        p, log = self.params, self.params.log
        alerts: list[MonitorSnapshotAlert] = p.monitor_snapshots_config.alerts
        labels: list[SnapshotLabel] = [alert.label for alert in alerts]
        oldest_skip_holds: list[bool] = [alert.oldest_skip_holds for alert in alerts]
        current_unixtime_millis: float = p.create_src_snapshots_config.current_datetime.timestamp() * 1000
        is_debug: bool = log.isEnabledFor(LOG_DEBUG)
        if is_caching_snapshots(p, remote):
            props: dict[str, DatasetProperties] = self.dst_properties if remote is p.dst else self.src_properties
            snapshots_changed_dict: dict[str, int] = {dataset: vals.snapshots_changed for dataset, vals in props.items()}
            alerts_hash: str = sha256_128_urlsafe_base64(str(tuple(alerts)))
            label_hashes: dict[SnapshotLabel, str] = {
                label: sha256_128_urlsafe_base64(label.notimestamp_str()) for label in labels
            }
        is_caching: bool = False
        worst_alert: tuple[int, str, str] = (0, "", "")  # -exit_code, exit_kind, exit_msg

        def record_alert(exit_code: int, exit_kind: str, exit_msg: str) -> None:
            nonlocal worst_alert
            worst_alert = min(worst_alert, (-exit_code, exit_kind, exit_msg))  # min() sorts "Latest" before "Oldest" on tie

        def monitor_last_modified_cache_file(r: Remote, dataset: str, label: SnapshotLabel, alert_cfg: AlertConfig) -> str:
            cache_label: str = os.path.join(MONITOR_CACHE_FILE_PREFIX, alert_cfg.kind[0], label_hashes[label], alerts_hash)
            return self.cache.last_modified_cache_file(r, dataset, cache_label)

        def alert_msg(
            kind: str, dataset: str, snapshot: str, label: SnapshotLabel, snapshot_age_millis: float, delta_millis: int
        ) -> str:
            assert kind == "Latest" or kind == "Oldest"
            lbl = f"{label.prefix}{label.infix}<timestamp>{label.suffix}"
            if snapshot_age_millis >= current_unixtime_millis:
                return f"No snapshot exists for {dataset}@{lbl}"
            msg = f"{kind} snapshot for {dataset}@{lbl} is {human_readable_duration(snapshot_age_millis, unit='ms')} old"
            s = f": @{snapshot}" if snapshot else ""
            if delta_millis == -1:
                return f"{msg}{s}"
            return f"{msg} but should be at most {human_readable_duration(delta_millis, unit='ms')} old{s}"

        def check_alert(
            label: SnapshotLabel, alert_cfg: AlertConfig | None, creation_unixtime_secs: int, dataset: str, snapshot: str
        ) -> None:  # thread-safe
            if alert_cfg is None:
                return
            if is_caching and not p.dry_run:  # update cache with latest state from 'zfs list -t snapshot'
                snapshots_changed: int = snapshots_changed_dict.get(dataset, 0)
                cache_file: str = monitor_last_modified_cache_file(remote, dataset, label, alert_cfg)
                set_last_modification_time_safe(
                    cache_file, unixtime_in_secs=(creation_unixtime_secs, snapshots_changed), if_more_recent=True
                )
            warning_millis: int = alert_cfg.warning_millis
            critical_millis: int = alert_cfg.critical_millis
            alert_kind = alert_cfg.kind
            snapshot_age_millis: float = current_unixtime_millis - creation_unixtime_secs * 1000
            m = "--monitor_snapshots: "
            if snapshot_age_millis > critical_millis:
                msg = m + alert_msg(alert_kind, dataset, snapshot, label, snapshot_age_millis, critical_millis)
                log.critical("%s", msg)
                if not p.monitor_snapshots_config.dont_crit:
                    record_alert(CRITICAL_STATUS, alert_kind, msg)
            elif snapshot_age_millis > warning_millis:
                msg = m + alert_msg(alert_kind, dataset, snapshot, label, snapshot_age_millis, warning_millis)
                log.warning("%s", msg)
                if not p.monitor_snapshots_config.dont_warn:
                    record_alert(WARNING_STATUS, alert_kind, msg)
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
            that is, without incurring 'zfs list -t snapshots'.

            This is done by comparing the "snapshots_changed" ZFS dataset property with the local cache. See
            https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html#snapshots_changed
            """
            stale_datasets: list[str] = []
            time_threshold: float = time.time() - MATURITY_TIME_THRESHOLD_SECS
            for dataset in sorted_datasets:
                is_stale_dataset: bool = False
                snapshots_changed: int = snapshots_changed_dict.get(dataset, 0)
                for alert in alerts:
                    for cfg in (alert.latest, alert.oldest):
                        if cfg is None:
                            continue
                        if (
                            snapshots_changed != 0
                            and snapshots_changed < time_threshold
                            and (  # always True
                                cached_unix_times := self.cache.get_snapshots_changed2(
                                    monitor_last_modified_cache_file(remote, dataset, alert.label, cfg)
                                )
                            )
                            and snapshots_changed == cached_unix_times[1]  # cached snapshots_changed aka last modified time
                            and snapshots_changed >= cached_unix_times[0]  # creation time of minmax snapshot aka access time
                        ):  # cached state is still valid; emit an alert if the latest/oldest snapshot is too old
                            lbl = alert.label
                            check_alert(lbl, cfg, creation_unixtime_secs=cached_unix_times[0], dataset=dataset, snapshot="")
                        else:  # cached state is no longer valid; fallback to 'zfs list -t snapshot'
                            is_stale_dataset = True
                if is_stale_dataset:
                    stale_datasets.append(dataset)
            return stale_datasets

        # satisfy request from local cache as much as possible
        if is_caching_snapshots(p, remote):
            stale_datasets: list[str] = find_stale_datasets_and_check_alerts()
            with self.stats_lock:
                self.num_cache_misses += len(stale_datasets)
                self.num_cache_hits += len(sorted_datasets) - len(stale_datasets)
        else:
            stale_datasets = sorted_datasets

        # fallback to 'zfs list -t snapshot' for any remaining datasets, as these couldn't be satisfied from local cache
        is_caching = is_caching_snapshots(p, remote)
        datasets_without_snapshots: list[str] = self.handle_minmax_snapshots(
            remote,
            stale_datasets,
            labels,
            fn_latest=alert_latest_snapshot,
            fn_oldest=alert_oldest_snapshot,
            fn_oldest_skip_holds=oldest_skip_holds,
        )
        for dataset in datasets_without_snapshots:
            for i in range(len(alerts)):
                alert_latest_snapshot(i, creation_unixtime_secs=0, dataset=dataset, snapshot="")
                alert_oldest_snapshot(i, creation_unixtime_secs=0, dataset=dataset, snapshot="")
        return worst_alert

    def replicate_datasets(self, src_datasets: list[str], task_description: str, max_workers: int) -> bool:
        """Replicates a list of datasets."""
        assert (not self.is_test_mode) or src_datasets == sorted(src_datasets), "List is not sorted"
        p, log = self.params, self.params.log
        src, dst = p.src, p.dst
        self.num_snapshots_found = 0
        self.num_snapshots_replicated = 0
        log.info("Starting replication task: %s", task_description + f" [{len(src_datasets)} datasets]")
        start_time_nanos: int = time.monotonic_ns()

        def src2dst(src_dataset: str) -> str:
            return replace_prefix(src_dataset, old_prefix=src.root_dataset, new_prefix=dst.root_dataset)

        def dst2src(dst_dataset: str) -> str:
            return replace_prefix(dst_dataset, old_prefix=dst.root_dataset, new_prefix=src.root_dataset)

        def find_stale_datasets() -> tuple[list[str], dict[str, str]]:
            """If the cache is enabled on replication, check which src datasets or dst datasets have changed to determine
            which datasets can be skipped cheaply, i.e. without incurring 'zfs list -t snapshots'.

            This is done by comparing the "snapshots_changed" ZFS dataset property with the local cache. See
            https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html#snapshots_changed
            """
            # First, check which src datasets have changed since the last replication to that destination
            cache_files: dict[str, str] = {}
            stale_src_datasets1: list[str] = []
            maybe_stale_dst_datasets: list[str] = []
            userhost_dir: str = sha256_85_urlsafe_base64(p.dst.cache_namespace())
            filter_key = tuple(tuple(f) for f in p.snapshot_filters)  # cache is only valid for same --include/excl-snapshot*
            filter_hash_code: str = sha256_85_urlsafe_base64(str(filter_key))
            for src_dataset in src_datasets:
                dst_dataset: str = src2dst(src_dataset)  # cache is only valid for identical destination dataset
                dst_dataset_dir: str = sha256_85_urlsafe_base64(dst_dataset)
                cache_label: str = os.path.join(
                    REPLICATION_CACHE_FILE_PREFIX, userhost_dir, dst_dataset_dir, filter_hash_code
                )
                cache_file: str = self.cache.last_modified_cache_file(src, src_dataset, cache_label)
                cache_files[src_dataset] = cache_file
                snapshots_changed: int = self.src_properties[src_dataset].snapshots_changed  # get prop "for free"
                if (
                    snapshots_changed != 0
                    and time.time() > snapshots_changed + MATURITY_TIME_THRESHOLD_SECS
                    and snapshots_changed == self.cache.get_snapshots_changed(cache_file)
                ):
                    maybe_stale_dst_datasets.append(dst_dataset)
                else:
                    stale_src_datasets1.append(src_dataset)

            # For each src dataset that hasn't changed, check if the corresponding dst dataset has changed
            stale_src_datasets2: list[str] = []
            dst_snapshots_changed_dict: dict[str, int] = self.cache.zfs_get_snapshots_changed(dst, maybe_stale_dst_datasets)
            for dst_dataset in maybe_stale_dst_datasets:
                snapshots_changed = dst_snapshots_changed_dict.get(dst_dataset, 0)
                if (
                    snapshots_changed != 0
                    and time.time() > snapshots_changed + MATURITY_TIME_THRESHOLD_SECS
                    and snapshots_changed
                    == self.cache.get_snapshots_changed(self.cache.last_modified_cache_file(dst, dst_dataset))
                ):
                    log.info("Already up-to-date [cached]: %s", dst_dataset)
                else:
                    stale_src_datasets2.append(dst2src(dst_dataset))
            assert (not self.is_test_mode) or stale_src_datasets1 == sorted(stale_src_datasets1), "List is not sorted"
            assert (not self.is_test_mode) or stale_src_datasets2 == sorted(stale_src_datasets2), "List is not sorted"
            stale_src_datasets = list(heapq.merge(stale_src_datasets1, stale_src_datasets2))  # merge two sorted lists
            assert (not self.is_test_mode) or not has_duplicates(stale_src_datasets), "List contains duplicates"
            return stale_src_datasets, cache_files

        if is_caching_snapshots(p, src):
            stale_src_datasets, cache_files = find_stale_datasets()
            num_cache_misses = len(stale_src_datasets)
            num_cache_hits = len(src_datasets) - len(stale_src_datasets)
            self.num_cache_misses += num_cache_misses
            self.num_cache_hits += num_cache_hits
            cmsg = self._cache_hits_msg(hits=num_cache_hits, misses=num_cache_misses)
        else:
            stale_src_datasets = src_datasets
            cache_files = {}
            cmsg = ""

        done_src_datasets: list[str] = []
        done_src_datasets_lock: threading.Lock = threading.Lock()

        def _process_dataset_fn(src_dataset: str, tid: str, retry: Retry) -> bool:
            result: bool = replicate_dataset(job=self, src_dataset=src_dataset, tid=tid, retry=retry)
            with done_src_datasets_lock:
                done_src_datasets.append(src_dataset)  # record datasets that were actually replicated (not skipped)
            return result

        # Run replicate_dataset(dataset) for each dataset, while taking care of errors, retries + parallel execution
        failed: bool = process_datasets_in_parallel_and_fault_tolerant(
            log=log,
            datasets=stale_src_datasets,
            process_dataset=_process_dataset_fn,
            skip_tree_on_error=lambda dataset: not self.dst_dataset_exists[src2dst(dataset)],
            skip_on_error=p.skip_on_error,
            max_workers=max_workers,
            termination_event=self.termination_event,
            termination_handler=self.terminate,
            enable_barriers=False,
            task_name="Replication",
            append_exception=self.append_exception,
            retry_options=self.retry_options.copy(policy=p.retry_policy),
            dry_run=p.dry_run,
            is_test_mode=self.is_test_mode,
        )

        if is_caching_snapshots(p, src) and len(done_src_datasets) > 0:
            # refresh "snapshots_changed" ZFS dataset property from dst
            stale_dst_datasets: list[str] = [src2dst(src_dataset) for src_dataset in sorted(done_src_datasets)]
            dst_snapshots_changed_dict: dict[str, int] = self.cache.zfs_get_snapshots_changed(dst, stale_dst_datasets)
            for dst_dataset in stale_dst_datasets:  # update local cache
                dst_snapshots_changed: int = dst_snapshots_changed_dict.get(dst_dataset, 0)
                dst_cache_file: str = self.cache.last_modified_cache_file(dst, dst_dataset)
                src_dataset: str = dst2src(dst_dataset)
                src_snapshots_changed: int = self.src_properties[src_dataset].snapshots_changed
                if not p.dry_run:
                    set_last_modification_time_safe(
                        cache_files[src_dataset], unixtime_in_secs=src_snapshots_changed, if_more_recent=True
                    )
                    set_last_modification_time_safe(
                        dst_cache_file, unixtime_in_secs=dst_snapshots_changed, if_more_recent=True
                    )

        elapsed_nanos: int = time.monotonic_ns() - start_time_nanos
        log.info(
            p.dry("Replication done: %s"),
            f"{task_description} [Replicated {self.num_snapshots_replicated} out of {self.num_snapshots_found} snapshots"
            f" within {len(src_datasets)} datasets; took {human_readable_duration(elapsed_nanos)}{cmsg}]",
        )
        return failed

    def maybe_inject_delete(self, remote: Remote, dataset: str, delete_trigger: str) -> None:
        """For testing only; for unit tests to delete datasets during replication and test correct handling of that."""
        assert delete_trigger
        counter = self.delete_injection_triggers.get("before")
        if counter and self.decrement_injection_counter(counter, delete_trigger):
            p = self.params
            cmd = p.split_args(f"{remote.sudo} {p.zfs_program} destroy -r", p.force_unmount, p.force_hard, dataset or "")
            self.run_ssh_command(remote, LOG_DEBUG, print_stdout=True, cmd=cmd)

    def maybe_inject_params(self, error_trigger: str) -> None:
        """For testing only; for unit tests to simulate errors during replication and test correct handling of them."""
        assert error_trigger
        counter = self.error_injection_triggers.get("before")
        if counter and self.decrement_injection_counter(counter, error_trigger):
            self.inject_params = self.param_injection_triggers[error_trigger]
        elif error_trigger in self.param_injection_triggers:
            self.inject_params = {}

    @staticmethod
    def recv_option_property_names(recv_opts: list[str]) -> set[str]:
        """Extracts -o and -x property names that are already specified on the command line; This can be used to check for
        dupes because 'zfs receive' does not accept multiple -o or -x options with the same property name."""
        propnames: set[str] = set()
        i = 0
        n = len(recv_opts)
        while i < n:
            stripped: str = recv_opts[i].strip()
            if stripped in ("-o", "-x"):
                i += 1
                if i == n or recv_opts[i].strip() in ("-o", "-x"):
                    die(f"Missing value for {stripped} option in --zfs-recv-program-opt(s): {' '.join(recv_opts)}")
                if stripped == "-o" and "=" not in recv_opts[i]:
                    die(f"Missing value for {stripped} name=value pair in --zfs-recv-program-opt(s): {' '.join(recv_opts)}")
                propname: str = recv_opts[i] if stripped == "-x" else recv_opts[i].split("=", 1)[0]
                validate_property_name(propname, "--zfs-recv-program-opt(s)")
                propnames.add(propname)
            i += 1
        return propnames

    def root_datasets_if_recursive_zfs_snapshot_is_possible(
        self, datasets: list[str], basis_datasets: list[str]
    ) -> list[str] | None:
        """Returns the root datasets within the (filtered) `datasets` list if no incompatible pruning is detected. A dataset
        within `datasets` is considered a root dataset if it has no parent, i.e. it is not a descendant of any dataset in
        `datasets`. Returns `None` if any (unfiltered) dataset in `basis_dataset` that is a descendant of at least one of the
        root datasets is missing in `datasets`, indicating that --include/exclude-dataset* or the snapshot schedule have
        pruned a dataset in a way that is incompatible with 'zfs snapshot -r' CLI semantics, thus requiring a switch to the
        non-recursive 'zfs snapshot snapshot1 .. snapshot N' CLI flavor.

        Assumes that set(datasets).issubset(set(basis_datasets)). Also assumes that datasets and basis_datasets are both
        sorted (and thus the output root_datasets list is sorted too), which is why this algorithm is efficient - O(N) time
        complexity. The impl is akin to the merge algorithm of a merge sort, adapted to our specific use case.
        See root_datasets_if_recursive_zfs_snapshot_is_possible_slow_but_correct() in the unit test suite for an alternative
        impl that's easier to grok.
        """
        assert (not self.is_test_mode) or datasets == sorted(datasets), "List is not sorted"
        assert (not self.is_test_mode) or not has_duplicates(datasets), "List contains duplicates"
        assert (not self.is_test_mode) or basis_datasets == sorted(basis_datasets), "List is not sorted"
        assert (not self.is_test_mode) or not has_duplicates(basis_datasets), "List contains duplicates"
        assert (not self.is_test_mode) or set(datasets).issubset(set(basis_datasets)), "Not a subset"
        root_datasets: list[str] = self.find_root_datasets(datasets)
        i = 0
        j = 0
        k = 0
        len_root_datasets = len(root_datasets)
        len_basis_datasets = len(basis_datasets)
        len_datasets = len(datasets)
        while i < len_root_datasets and j < len_basis_datasets:  # walk and "merge" the sorted lists, in sync
            if basis_datasets[j] < root_datasets[i]:  # irrelevant subtree?
                j += 1  # move to next basis_datasets[j]
            elif is_descendant(basis_datasets[j], of_root_dataset=root_datasets[i]):  # relevant subtree?
                while k < len_datasets and datasets[k] < basis_datasets[j]:
                    k += 1  # move to next datasets[k]
                if k == len_datasets or datasets[k] != basis_datasets[j]:  # dataset chopped off by schedule or --incl/excl*?
                    return None  # detected filter pruning that is incompatible with 'zfs snapshot -r'
                j += 1  # move to next basis_datasets[j]
            else:
                i += 1  # move to next root_dataset[i]; no need to check root_datasets that are no longer (or not yet) reachable
        return root_datasets

    @staticmethod
    def find_root_datasets(sorted_datasets: list[str]) -> list[str]:
        """Returns the roots of the subtrees in the (sorted) input datasets; The output root dataset list is sorted, too; A
        dataset is a root dataset if it has no parent, i.e. it is not a descendant of any dataset in the input datasets."""
        root_datasets: list[str] = []
        skip_dataset: str = DONT_SKIP_DATASET
        for dataset in sorted_datasets:
            if is_descendant(dataset, of_root_dataset=skip_dataset):
                continue
            skip_dataset = dataset
            root_datasets.append(dataset)
        return root_datasets

    def find_datasets_to_snapshot(self, sorted_datasets: list[str]) -> dict[SnapshotLabel, list[str]]:
        """Given a (sorted) list of source datasets, returns a dict where the key is a snapshot name (aka SnapshotLabel, e.g.
        bzfs_2024-11-06_08:30:05_hourly) and the value is the (sorted) (sub)list of datasets for which a snapshot needs to be
        created with that name, because these datasets are due per the schedule, either because the 'creation' time of their
        most recent snapshot with that name pattern is now too old, or such a snapshot does not even exist.

        The baseline implementation uses the 'zfs list -t snapshot' CLI to find the most recent snapshots, which is simple
        but doesn't scale well with the number of snapshots, at least if the goal is to take snapshots every second. An
        alternative, much more scalable, implementation queries the standard ZFS "snapshots_changed" dataset property
        (requires zfs >= 2.2.0), in combination with a local cache that stores this property, as well as the creation time of
        the most recent snapshot, for each SnapshotLabel and each dataset.
        """
        p, log = self.params, self.params.log
        src = p.src
        config: CreateSrcSnapshotConfig = p.create_src_snapshots_config
        datasets_to_snapshot: dict[SnapshotLabel, list[str]] = defaultdict(list)
        is_caching: bool = False
        interner: HashedInterner[datetime] = HashedInterner()  # reduces memory footprint
        msgs: list[tuple[datetime, str, SnapshotLabel, str]] = []

        def create_snapshot_if_latest_is_too_old(
            datasets_to_snapshot: dict[SnapshotLabel, list[str]], dataset: str, label: SnapshotLabel, creation_unixtime: int
        ) -> None:  # thread-safe
            """Schedules creation of a snapshot for the given label if the label's existing latest snapshot is too old."""
            creation_dt: datetime = datetime.fromtimestamp(creation_unixtime, tz=config.tz)
            log.log(LOG_TRACE, "Latest snapshot creation: %s for %s", creation_dt, label)
            duration_amount, duration_unit = config.suffix_durations[label.suffix]
            next_event_dt: datetime = config.anchors.round_datetime_up_to_duration_multiple(
                creation_dt + timedelta(microseconds=1), duration_amount, duration_unit
            )
            msg: str = ""
            if config.current_datetime >= next_event_dt:
                datasets_to_snapshot[label].append(dataset)  # mark it as scheduled for snapshot creation
                msg = " has passed"
            next_event_dt = interner.intern(next_event_dt)
            msgs.append((next_event_dt, dataset, label, msg))
            if is_caching and not p.dry_run:  # update cache with latest state from 'zfs list -t snapshot'
                # Per-label cache stores (atime=creation, mtime=snapshots_changed) so later runs can safely trust creation
                # only when the label's mtime matches the current dataset-level '=' cache value. Excludes timestamp of label.
                cache_file: str = self.cache.last_modified_cache_file(src, dataset, label_hashes[label])
                unixtimes: tuple[int, int] = (creation_unixtime, self.src_properties[dataset].snapshots_changed)
                set_last_modification_time_safe(cache_file, unixtime_in_secs=unixtimes, if_more_recent=True)

        labels: list[SnapshotLabel] = []
        config_labels: list[SnapshotLabel] = config.snapshot_labels()
        for label in config_labels:
            duration_amount_, _duration_unit = config.suffix_durations[label.suffix]
            if duration_amount_ == 0 or config.create_src_snapshots_even_if_not_due:
                datasets_to_snapshot[label] = sorted_datasets  # take snapshot regardless of creation time of existing snaps
            else:
                labels.append(label)
        if len(labels) == 0:
            return datasets_to_snapshot  # nothing more TBD
        label_hashes: dict[SnapshotLabel, str] = {
            label: sha256_128_urlsafe_base64(label.notimestamp_str()) for label in labels
        }

        # satisfy request from local cache as much as possible
        cached_datasets_to_snapshot: dict[SnapshotLabel, list[str]] = defaultdict(list)
        if is_caching_snapshots(p, src):
            sorted_datasets_todo: list[str] = []
            time_threshold: float = time.time() - MATURITY_TIME_THRESHOLD_SECS
            for dataset in sorted_datasets:
                cache: SnapshotCache = self.cache
                cached_snapshots_changed: int = cache.get_snapshots_changed(cache.last_modified_cache_file(src, dataset))
                if cached_snapshots_changed == 0:
                    sorted_datasets_todo.append(dataset)  # request cannot be answered from cache
                    continue
                if cached_snapshots_changed != self.src_properties[dataset].snapshots_changed:  # get that prop "for free"
                    cache.invalidate_last_modified_cache_dataset(dataset)
                    sorted_datasets_todo.append(dataset)  # request cannot be answered from cache
                    continue
                if cached_snapshots_changed >= time_threshold:  # Avoid equal-second races: only trust matured cache entries
                    sorted_datasets_todo.append(dataset)  # cache entry isn't mature enough to be trusted; skip cache
                    continue
                creation_unixtimes: list[int] = []
                for label_hash in label_hashes.values():
                    # For per-label files, atime stores the latest matching snapshot's creation time, while mtime stores
                    # the dataset-level snapshots_changed observed when this label file was written.
                    atime, mtime = cache.get_snapshots_changed2(cache.last_modified_cache_file(src, dataset, label_hash))
                    # Sanity check: trust per-label cache only when:
                    #  - mtime equals the dataset-level '=' cache (same snapshots_changed), and
                    #  - atime is plausible and not later than mtime (creation <= snapshots_changed), and
                    #  - neither atime nor mtime is zero (unknown provenance).
                    # Otherwise fall back to 'zfs list -t snapshot' to avoid stale creation times after newer changes.
                    if atime == 0 or mtime == 0 or mtime != cached_snapshots_changed or atime > mtime:
                        sorted_datasets_todo.append(dataset)  # request cannot be answered from cache
                        break
                    creation_unixtimes.append(atime)
                if len(creation_unixtimes) == len(labels):
                    for j, label in enumerate(labels):
                        create_snapshot_if_latest_is_too_old(
                            cached_datasets_to_snapshot, dataset, label, creation_unixtimes[j]
                        )
            sorted_datasets = sorted_datasets_todo

        def create_snapshot_fn(i: int, creation_unixtime_secs: int, dataset: str, snapshot: str) -> None:
            create_snapshot_if_latest_is_too_old(datasets_to_snapshot, dataset, labels[i], creation_unixtime_secs)

        def on_finish_dataset(dataset: str) -> None:
            if is_caching_snapshots(p, src) and not p.dry_run:
                set_last_modification_time_safe(
                    self.cache.last_modified_cache_file(src, dataset),
                    unixtime_in_secs=self.src_properties[dataset].snapshots_changed,
                    if_more_recent=True,
                )

        # fallback to 'zfs list -t snapshot' for any remaining datasets, as these couldn't be satisfied from local cache
        is_caching = is_caching_snapshots(p, src)
        datasets_without_snapshots: list[str] = self.handle_minmax_snapshots(
            src, sorted_datasets, labels, fn_latest=create_snapshot_fn, fn_on_finish_dataset=on_finish_dataset
        )
        for lbl in labels:  # merge (sorted) results from local cache + 'zfs list -t snapshot' into (sorted) combined result
            datasets_to_snapshot[lbl].sort()
            if datasets_without_snapshots or (lbl in cached_datasets_to_snapshot):  # +take snaps for snapshot-less datasets
                datasets_to_snapshot[lbl] = list(  # inputs to merge() are sorted, and outputs are sorted too
                    heapq.merge(datasets_to_snapshot[lbl], cached_datasets_to_snapshot[lbl], datasets_without_snapshots)
                )
        for label, datasets in datasets_to_snapshot.items():
            assert (not self.is_test_mode) or datasets == sorted(datasets), "List is not sorted"
            assert (not self.is_test_mode) or not has_duplicates(datasets), "List contains duplicates"
            assert label

        msgs.sort()  # sort by time, dataset, label
        for i in range(0, len(msgs), 10_000):  # reduce logging overhead via mini-batching
            text = "".join(
                f"\nNext scheduled snapshot time: {next_event_dt} for {dataset}@{label}{msg}"
                for next_event_dt, dataset, label, msg in msgs[i : i + 10_000]
            )
            log.info("Next scheduled snapshot times ...%s", text)

        # sort keys to ensure that we take snapshots for dailies before hourlies, and so on
        label_indexes: dict[SnapshotLabel, int] = {label: k for k, label in enumerate(config_labels)}
        datasets_to_snapshot = dict(sorted(datasets_to_snapshot.items(), key=lambda kv: label_indexes[kv[0]]))
        return datasets_to_snapshot

    def handle_minmax_snapshots(
        self,
        remote: Remote,
        sorted_datasets: list[str],
        labels: list[SnapshotLabel],
        *,
        fn_latest: Callable[[int, int, str, str], None],  # callback function for latest snapshot
        fn_oldest: Callable[[int, int, str, str], None] | None = None,  # callback function for oldest snapshot
        fn_oldest_skip_holds: Sequence[bool] = (),
        fn_on_finish_dataset: Callable[[str], None] = lambda dataset: None,
    ) -> list[str]:  # thread-safe
        """For each dataset in `sorted_datasets`, for each label in `labels`, finds the latest and oldest snapshot, and runs
        the callback functions on them; Ignores the timestamp of the input labels and the timestamp of the snapshot names.

        If the (optional) fn_oldest_skip_holds=True for a given label, then snapshots for that label that carry a 'zfs hold'
        are skipped (ignored) when finding the oldest snapshot for fn_oldest. This can be useful for monitor_snapshots(),
        given that users often intentionally retain holds for longer than the "normal" snapshot retention period, and this
        shouldn't necessarily cause monitoring to emit alerts.
        """
        if fn_oldest is not None:
            assert len(labels) == len(fn_oldest_skip_holds)
        assert (not self.is_test_mode) or sorted_datasets == sorted(sorted_datasets), "List is not sorted"
        no_userrefs: tuple[str, ...] = ("", "-", "0")  # ZFS snapshot property userrefs > 0 indicates a zfs hold

        def extract_fields(line: str) -> tuple[int, int, str, bool]:
            fields: list[str] = line.split("\t")
            if len(fields) == 3:
                name, createtxg, creation_unixtime_secs = fields
                userrefs = ""
            else:
                name, createtxg, creation_unixtime_secs, userrefs = fields
            return (
                int(createtxg),
                int(creation_unixtime_secs),
                name.split("@", 1)[1],
                userrefs in no_userrefs,
            )

        p = self.params
        props: str = "name,createtxg,creation"
        props = props if fn_oldest is None or not any(fn_oldest_skip_holds) else props + ",userrefs"
        cmd = p.split_args(f"{p.zfs_program} list -t snapshot -d 1 -Hp -o {props}")  # sorts by dataset,creation
        datasets_with_snapshots: set[str] = set()
        interner: SortedInterner[str] = SortedInterner(sorted_datasets)  # reduces memory footprint
        for lines in zfs_list_snapshots_in_parallel(self, remote, cmd, sorted_datasets, ordered=False):
            # streaming group by dataset name (consumes constant memory only)
            for dataset, group in itertools.groupby(lines, key=lambda line: line.split("\t", 1)[0].split("@", 1)[0]):
                dataset = interner.interned(dataset)
                snapshots = sorted(  # fetch all snapshots of current dataset and sort by createtxg,creation,name
                    extract_fields(line) for line in group
                )  # perf: sorted() is fast because Timsort is close to O(N) for nearly sorted input, which is our case
                assert len(snapshots) > 0
                datasets_with_snapshots.add(dataset)
                snapshot_names: tuple[str, ...] = tuple(snapshot[2] for snapshot in snapshots)
                year_with_4_digits_regex: re.Pattern[str] = YEAR_WITH_FOUR_DIGITS_REGEX
                year_with_4_digits_regex_fullmatch = year_with_4_digits_regex.fullmatch
                startswith = str.startswith
                endswith = str.endswith
                fns = ((fn_latest, True),) if fn_oldest is None else ((fn_latest, True), (fn_oldest, False))
                for i, label in enumerate(labels):
                    infix: str = label.infix
                    start: str = label.prefix + infix
                    end: str = label.suffix
                    startlen: int = len(start)
                    endlen: int = len(end)
                    minlen: int = startlen + endlen if infix else 4 + startlen + endlen  # year_with_four_digits_regex
                    startlen_4: int = startlen + 4  # [startlen:startlen+4]  # year_with_four_digits_regex
                    has_infix: bool = bool(infix)
                    for fn, is_reverse in fns:
                        creation_unixtime_secs: int = 0  # find creation time of latest or oldest snapshot matching the label
                        minmax_snapshot: str = ""
                        no_skip_holds: bool = is_reverse or not fn_oldest_skip_holds[i]
                        for j in range(len(snapshot_names) - 1, -1, -1) if is_reverse else range(len(snapshot_names)):
                            snapshot_name: str = snapshot_names[j]
                            if (
                                endswith(snapshot_name, end)  # aka snapshot_name.endswith(end)
                                and startswith(snapshot_name, start)  # aka snapshot_name.startswith(start)
                                and len(snapshot_name) >= minlen
                                and (has_infix or year_with_4_digits_regex_fullmatch(snapshot_name, startlen, startlen_4))
                                and (no_skip_holds or snapshots[j][3])
                            ):
                                creation_unixtime_secs = snapshots[j][1]
                                minmax_snapshot = snapshot_name
                                break
                        fn(i, creation_unixtime_secs, dataset, minmax_snapshot)
                fn_on_finish_dataset(dataset)
        datasets_without_snapshots = [dataset for dataset in sorted_datasets if dataset not in datasets_with_snapshots]
        return datasets_without_snapshots

    @staticmethod
    def _cache_hits_msg(hits: int, misses: int) -> str:
        total = hits + misses
        return f", cache hits: {percent(hits, total, print_total=True)}, misses: {percent(misses, total, print_total=True)}"

    def run_ssh_command(
        self,
        remote: MiniRemote,
        loglevel: int = logging.INFO,
        is_dry: bool = False,
        check: bool = True,
        print_stdout: bool = False,
        print_stderr: bool = True,
        cmd: list[str] | None = None,
        retry_on_generic_ssh_error: bool = True,
    ) -> str:
        """Runs the given CLI cmd via ssh on the given remote, and returns stdout."""
        assert cmd is not None and isinstance(cmd, list) and len(cmd) > 0
        conn_pool: ConnectionPool = self.params.connection_pools[remote.location].pool(SHARED)
        with conn_pool.connection() as conn:
            log: logging.Logger = self.params.log
            try:
                process: subprocess.CompletedProcess[str] = conn.run_ssh_command(
                    cmd=cmd,
                    job=self,
                    loglevel=loglevel,
                    is_dry=is_dry,
                    check=check,
                    stdin=DEVNULL,
                    stdout=PIPE,
                    stderr=PIPE,
                    text=True,
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
                xprint(log, stderr_to_str(e.stdout) if print_stdout else e.stdout, run=print_stdout, file=sys.stdout, end="")
                xprint(log, stderr_to_str(e.stderr) if print_stderr else e.stderr, run=print_stderr, file=sys.stderr, end="")
                if retry_on_generic_ssh_error and isinstance(e, subprocess.CalledProcessError):
                    stderr: str = stderr_to_str(e.stderr)
                    if stderr.startswith("ssh: "):
                        assert e.returncode == 255, e.returncode  # error within SSH itself (not during the remote command)
                        raise RetryableError("Subprocess failed", display_msg="ssh") from e
                raise
            else:
                if is_dry:
                    return ""
                xprint(log, process.stdout, run=print_stdout, file=sys.stdout, end="")
                xprint(log, process.stderr, run=print_stderr, file=sys.stderr, end="")
                return process.stdout

    def try_ssh_command(
        self,
        remote: MiniRemote,
        loglevel: int,
        is_dry: bool = False,
        print_stdout: bool = False,
        cmd: list[str] | None = None,
        exists: bool = True,
        error_trigger: str | None = None,
    ) -> str | None:
        """Convenience method that helps retry/react to a dataset or pool that potentially doesn't exist anymore."""
        assert cmd is not None and isinstance(cmd, list) and len(cmd) > 0
        log = self.params.log
        try:
            self.maybe_inject_error(cmd=cmd, error_trigger=error_trigger)
            return self.run_ssh_command(remote=remote, loglevel=loglevel, is_dry=is_dry, print_stdout=print_stdout, cmd=cmd)
        except (subprocess.CalledProcessError, UnicodeDecodeError) as e:
            if not isinstance(e, UnicodeDecodeError):
                stderr: str = stderr_to_str(e.stderr)
                if exists and (
                    ": dataset does not exist" in stderr
                    or ": filesystem does not exist" in stderr  # solaris 11.4.0
                    or ": no such pool" in stderr
                    or "does not have any resumable receive state to abort" in stderr  # harmless `zfs receive -A` race
                ):
                    return None
                log.warning("%s", stderr.rstrip())
            raise RetryableError("Subprocess failed") from e

    def try_ssh_command_with_retries(self, *args: Any, **kwargs: Any) -> str | None:
        """Convenience method that auto-retries try_ssh_command() on failure."""
        p = self.params
        return call_with_retries(
            fn=lambda retry: self.try_ssh_command(*args, **kwargs),
            policy=p.retry_policy,
            config=self.retry_options.config,
            giveup=self.retry_options.giveup,
            after_attempt=self.retry_options.after_attempt,
            on_exhaustion=on_exhaustion_raise,
            log=p.log,
        )

    def run_ssh_command_with_retries(self, *args: Any, **kwargs: Any) -> str:
        """Convenience method that auto-retries run_ssh_command() on transport failure (not on remote command failure)."""
        p = self.params
        return call_with_retries(
            fn=lambda retry: self.run_ssh_command(*args, **kwargs),
            policy=p.retry_policy,
            config=self.retry_options.config,
            giveup=self.retry_options.giveup,
            after_attempt=self.retry_options.after_attempt,
            on_exhaustion=on_exhaustion_raise,
            log=p.log,
        )

    def maybe_inject_error(self, cmd: list[str], error_trigger: str | None = None) -> None:
        """For testing only; for unit tests to simulate errors during replication and test correct handling of them."""
        if error_trigger:
            counter = self.error_injection_triggers.get("before")
            if counter and self.decrement_injection_counter(counter, error_trigger):
                try:
                    raise CalledProcessError(returncode=1, cmd=" ".join(cmd), stderr=error_trigger + ":dataset is busy")
                except subprocess.CalledProcessError as e:
                    if error_trigger.startswith("retryable_"):
                        raise RetryableError("Subprocess failed") from e
                    else:
                        raise

    def decrement_injection_counter(self, counter: Counter[str], trigger: str) -> bool:
        """For testing only."""
        with self.injection_lock:
            if counter[trigger] <= 0:
                return False
            counter[trigger] -= 1
            return True


#############################################################################
@final
class DatasetProperties:
    """Properties of a ZFS dataset."""

    __slots__ = ("recordsize", "snapshots_changed")  # uses more compact memory layout than __dict__

    def __init__(self, recordsize: int, snapshots_changed: int) -> None:
        # immutable variables:
        self.recordsize: Final[int] = recordsize

        # mutable variables:
        self.snapshots_changed: int = snapshots_changed


#############################################################################
# Input format is [[user@]host:]dataset
#                                                             1234         5          6
_DATASET_LOCATOR_REGEX: Final[re.Pattern[str]] = re.compile(r"(((([^@]*)@)?([^:]+)):)?(.*)", flags=re.DOTALL)


def parse_dataset_locator(
    input_text: str, validate: bool = True, user: str | None = None, host: str | None = None, port: int | None = None
) -> tuple[str, str, str, str, str]:
    """Splits user@host:dataset into its components with optional checks."""

    def convert_ipv6(hostname: str) -> str:  # support IPv6 without getting confused by host:dataset colon separator ...
        return hostname.replace("|", ":")  # ... and any colons that may be part of a (valid) ZFS dataset name

    user_undefined: bool = user is None
    if user is None:
        user = ""
    host_undefined: bool = host is None
    if host is None:
        host = ""
    host = convert_ipv6(host)
    user_host, dataset, pool = "", "", ""

    if match := _DATASET_LOCATOR_REGEX.fullmatch(input_text):
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


def validate_user_name(user: str, input_text: str) -> None:
    """Checks that the username is safe for ssh or local usage."""
    invalid_chars: str = SHELL_CHARS_AND_SLASH
    if user and (user.startswith("-") or ".." in user or any(c.isspace() or c in invalid_chars for c in user)):
        die(f"Invalid user name: '{user}' for: '{input_text}'")


def validate_host_name(host: str, input_text: str) -> None:
    """Checks hostname for forbidden characters or patterns."""
    invalid_chars: str = SHELL_CHARS_AND_SLASH
    if host and (host.startswith("-") or ".." in host or any(c.isspace() or c in invalid_chars for c in host)):
        die(f"Invalid host name: '{host}' for: '{input_text}'")


def validate_port(port: str | int | None, message: str) -> None:
    """Checks that port specification is a valid integer."""
    if isinstance(port, int):
        port = str(port)
    if port and not port.isdigit():
        die(message + f"must be empty or a positive integer: '{port}'")


def normalize_called_process_error(error: subprocess.CalledProcessError) -> int:
    """Normalizes `CalledProcessError.returncode` to avoid reserved exit codes so callers don't misclassify them."""
    ret: int = error.returncode
    ret = DIE_STATUS if isinstance(ret, int) and 1 <= ret <= STILL_RUNNING_STATUS else ret
    return ret


#############################################################################
if __name__ == "__main__":
    main()
