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
"""Logging helpers that build default and syslog-enabled loggers; Centralizes logger setup so that all bzfs tools share
uniform formatting and configuration.

Each bzfs.Job has its own separate Logger object such that multiple Jobs can run concurrently in the same Python process
without interfering with each other's logging semantics. This is achieved by passing a unique ``logger_name_suffix`` per job
to ``get_logger()`` so each job receives an isolated Logger instance. Callers are responsible for closing any loggers they
own via ``reset_logger()``.
"""
from __future__ import (
    annotations,
)
import argparse
import contextlib
import logging
import sys
from datetime import (
    datetime,
)
from logging import (
    Logger,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
)

from bzfs_main.utils import (
    LOG_STDERR,
    LOG_STDOUT,
    LOG_TRACE,
)

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from bzfs_main.configuration import (
        LogParams,
    )


def _get_logger_name() -> str:
    """Returns the canonical logger name used throughout bzfs."""
    return "bzfs_main.bzfs"


def _resolve_logger_name(logger_name_suffix: str) -> str:
    """Returns the logger name for the given optional logger suffix.

    Each bzfs.Job instance gets a distinct Logger name and hence uses a separate private (thread-safe) logging.Logger object.
    """
    logger_name: str = _get_logger_name()
    return logger_name + "." + logger_name_suffix if logger_name_suffix else logger_name


def reset_logger(log: Logger) -> None:
    """Removes and closes logging handlers (and closes their files) and resets logger to default state."""
    for handler in log.handlers.copy():
        log.removeHandler(handler)
        with contextlib.suppress(BrokenPipeError):
            handler.flush()
        handler.close()
    for _filter in log.filters.copy():
        log.removeFilter(_filter)
    log.setLevel(logging.NOTSET)
    log.propagate = True


def get_logger(
    log_params: LogParams, args: argparse.Namespace, log: Logger | None = None, logger_name_suffix: str = ""
) -> Logger:
    """Returns a logger configured from CLI arguments or an optional base logger."""
    _add_trace_loglevel()
    logging.addLevelName(LOG_STDERR, "STDERR")
    logging.addLevelName(LOG_STDOUT, "STDOUT")

    if log is not None:
        assert isinstance(log, Logger)
        return log  # use third party provided logger object
    return _get_default_logger(log_params, args, logger_name_suffix=logger_name_suffix)


def _get_default_logger(log_params: LogParams, args: argparse.Namespace, logger_name_suffix: str = "") -> Logger:
    """Creates the default logger with stream, file and optional syslog handlers."""
    logger_name: str = _resolve_logger_name(logger_name_suffix)
    log = Logger(logger_name)  # noqa: LOG001 do not register logger with Logger.manager to avoid potential memory leak
    log.setLevel(log_params.log_level)
    log.propagate = False  # don't propagate log messages up to the root logger to avoid emitting duplicate messages

    handler: logging.Handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(get_default_log_formatter(log_params=log_params))
    handler.setLevel(log_params.log_level)
    log.addHandler(handler)

    handler = logging.FileHandler(log_params.log_file, encoding="utf-8")
    handler.setFormatter(get_default_log_formatter())
    handler.setLevel(log_params.log_level)
    log.addHandler(handler)

    address = args.log_syslog_address
    if address:  # optionally, also log to local or remote syslog
        from logging import handlers  # lazy import for startup perf

        address, socktype = _get_syslog_address(address, args.log_syslog_socktype)
        log_syslog_prefix = str(args.log_syslog_prefix).strip().replace("%", "")  # sanitize
        handler = handlers.SysLogHandler(address=address, facility=args.log_syslog_facility, socktype=socktype)
        handler.setFormatter(get_default_log_formatter(prefix=log_syslog_prefix + " "))
        handler.setLevel(args.log_syslog_level)
        log.addHandler(handler)
        if handler.level < log.getEffectiveLevel():
            log_level_name: str = logging.getLevelName(log.getEffectiveLevel())
            log.warning(
                "%s",
                f"No messages with priority lower than {log_level_name} will be sent to syslog because syslog "
                f"log level {args.log_syslog_level} is lower than overall log level {log_level_name}.",
            )

    # perf: tell logging framework not to gather unnecessary expensive info for each log record
    logging.logProcesses = False
    logging.logThreads = False
    logging.logMultiprocessing = False
    logging.raiseExceptions = False  # avoid noisy tracebacks from logging handler errors like BrokenPipeError in production
    return log


LOG_LEVEL_PREFIXES: Final[dict[int, str]] = {
    logging.CRITICAL: "[C] CRITICAL:",
    logging.ERROR: "[E] ERROR:",
    logging.WARNING: "[W]",
    logging.INFO: "[I]",
    logging.DEBUG: "[D]",
    LOG_TRACE: "[T]",
}


def get_default_log_formatter(prefix: str = "", log_params: LogParams | None = None) -> logging.Formatter:
    """Returns a formatter for bzfs logs with optional prefix and column padding."""
    level_prefixes_: dict[int, str] = LOG_LEVEL_PREFIXES.copy()
    log_stderr_: int = LOG_STDERR
    log_stdout_: int = LOG_STDOUT
    terminal_cols: list[int | None] = [0 if log_params is None else None]  # 'None' indicates "configure value later"

    class DefaultLogFormatter(logging.Formatter):
        """Formatter adding timestamps and padding for progress output."""

        def format(self, record: logging.LogRecord) -> str:
            """Formats the given record, adding timestamp and level prefix and padding."""
            levelno: int = record.levelno
            if levelno != log_stderr_ and levelno != log_stdout_:  # emit stdout and stderr "as-is" (no formatting)
                timestamp: str = datetime.now().isoformat(sep=" ", timespec="seconds")  # 2024-09-03 12:26:15
                ts_level: str = f"{timestamp} {level_prefixes_.get(levelno, '')} "
                msg: str = record.msg
                i: int = msg.find("%s")
                msg = ts_level + msg
                if i >= 1:
                    i += len(ts_level)
                    msg = msg[0:i].ljust(54) + msg[i:]  # right-pad msg if record.msg contains "%s" unless at start
                if record.exc_info or record.exc_text or record.stack_info:
                    record.msg = msg
                    msg = super().format(record)
                elif record.args:
                    msg = msg % record.args
            else:
                msg = super().format(record)

            msg = prefix + msg
            cols: int | None = terminal_cols[0]
            if cols is None:
                cols = self.ljust_cols()
            msg = msg.ljust(cols)  # w/ progress line, "overwrite" trailing chars of previous msg with spaces
            return msg

        @staticmethod
        def ljust_cols() -> int:
            """Lazily determines padding width from ProgressReporter settings."""
            # lock-free yet thread-safe late configuration-based init for prettier ProgressReporter output
            # log_params.params and available_programs are not fully initialized yet before detect_available_programs() ends
            cols: int = 0
            assert log_params is not None
            p = log_params.params
            if p is not None and "local" in p.available_programs:
                if "pv" in p.available_programs["local"]:
                    cols = p.terminal_columns
                    assert cols is not None
                terminal_cols[0] = cols  # finally, resolve to use this specific value henceforth
            return cols

    return DefaultLogFormatter()


def get_simple_logger(program: str, logger_name_suffix: str = "") -> Logger:
    """Returns a minimal logger for simple tools."""

    level_prefixes_: dict[int, str] = LOG_LEVEL_PREFIXES.copy()
    logger_name = program + "." + logger_name_suffix if logger_name_suffix else program

    class LevelFormatter(logging.Formatter):
        """Injects level prefix and program name into log records."""

        def format(self, record: logging.LogRecord) -> str:
            """Attaches extra fields before delegating to base formatter."""
            record.level_prefix = level_prefixes_.get(record.levelno, "")
            record.program = program
            return super().format(record)

    _add_trace_loglevel()
    log = Logger(logger_name)  # noqa: LOG001 do not register logger with Logger.manager to avoid potential memory leak
    log.setLevel(logging.INFO)
    log.propagate = False
    handler = logging.StreamHandler()
    handler.setFormatter(
        LevelFormatter(fmt="%(asctime)s %(level_prefix)s [%(program)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    log.addHandler(handler)
    return log


def _add_trace_loglevel() -> None:
    """Registers a custom TRACE logging level with the standard python logging framework."""
    logging.addLevelName(LOG_TRACE, "TRACE")


def _get_syslog_address(address: str, log_syslog_socktype: str) -> tuple[str | tuple[str, int], Any]:
    """Normalizes syslog address to tuple form and returns socket type."""
    import socket  # lazy import for startup perf

    address = address.strip()
    socktype: socket.SocketKind | None = None
    if ":" in address:
        host, port_str = address.rsplit(":", 1)
        addr = (host.strip(), int(port_str.strip()))
        scktype: socket.SocketKind = socket.SOCK_DGRAM if log_syslog_socktype == "UDP" else socket.SOCK_STREAM  # for TCP
        return addr, scktype
    return address, socktype
