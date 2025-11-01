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

Each bzfs.Job has its own separate logger object such that multiple Jobs can run concurrently in the same Python process
without interfering with each other's logging semantics. This is achieved by passing a unique logger_name_suffix per job to
get_logger(). reset_logger() closes only those objects tied to that particular logger_name_suffix.
"""
from __future__ import (
    annotations,
)
import argparse
import contextlib
import logging
import os
import re
import sys
from datetime import (
    datetime,
)
from logging import (
    Logger,
)
from pathlib import (
    Path,
)
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Final,
)

from bzfs_main.utils import (
    LOG_STDERR,
    LOG_STDOUT,
    LOG_TRACE,
    PROG_NAME,
    die,
    open_nofollow,
)

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from bzfs_main.configuration import (
        LogParams,
    )


def _get_logger_name() -> str:
    """Returns the canonical logger name used throughout bzfs."""
    return "bzfs_main.bzfs"


def _get_logger_subname() -> str:
    """Returns the logger name for use by --log-config-file."""
    return _get_logger_name() + ".sub"


def _resolve_logger_names(logger_name_suffix: str) -> tuple[str, str]:
    """Returns the logger and sublogger names for the given optional logger suffix.

    Each bzfs.Job instance gets a distinct Logger name and hence uses a separate private (thread-safe) logging.Logger object.
    """
    logger_name: str = _get_logger_name()
    logger_subname: str = _get_logger_subname()
    if logger_name_suffix:
        return logger_name + "." + logger_name_suffix, logger_subname + "." + logger_name_suffix
    else:
        return logger_name, logger_subname


def reset_logger(logger_name_suffix: str = "") -> None:
    """Removes and closes logging handlers (and closes their files) and resets loggers to default state."""
    logger_name, logger_subname = _resolve_logger_names(logger_name_suffix)
    for log in [logging.getLogger(logger_name), logging.getLogger(logger_subname)]:
        reset_logger_obj(log)


def reset_logger_obj(log: Logger) -> None:
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
    elif args.log_config_file:
        clog = _get_dict_config_logger(log_params, args, logger_name_suffix=logger_name_suffix)  # read config file
    # ... add our own handlers unless matching handlers are already present
    default_log = _get_default_logger(log_params, args, logger_name_suffix=logger_name_suffix)
    return clog if args.log_config_file else default_log


def _get_default_logger(log_params: LogParams, args: argparse.Namespace, logger_name_suffix: str = "") -> Logger:
    """Creates the default logger with stream, file and optional syslog handlers."""
    logger_name, logger_subname = _resolve_logger_names(logger_name_suffix)
    sublog = logging.getLogger(logger_subname)
    log = logging.getLogger(logger_name)
    log.setLevel(log_params.log_level)
    log.propagate = False  # don't propagate log messages up to the root logger to avoid emitting duplicate messages

    if not any(
        isinstance(h, logging.StreamHandler) and h.stream in [sys.stdout, sys.stderr] for h in log.handlers + sublog.handlers
    ):
        handler: logging.Handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(get_default_log_formatter(log_params=log_params))
        handler.setLevel(log_params.log_level)
        log.addHandler(handler)

    abs_log_file: str = os.path.abspath(log_params.log_file)
    if not any(
        isinstance(h, logging.FileHandler) and h.baseFilename == abs_log_file for h in log.handlers + sublog.handlers
    ):
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
        if handler.level < sublog.getEffectiveLevel():
            log_level_name: str = logging.getLevelName(sublog.getEffectiveLevel())
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
        scktype: socket.SocketKind = (
            socket.SOCK_DGRAM
            if log_syslog_socktype == "UDP"
            else socket.SOCK_STREAM  # for TCP # pytype: disable=annotation-type-mismatch
        )
        return addr, scktype
    return address, socktype


def _remove_json_comments(config_str: str) -> str:
    """Strips line and end-of-line comments from a JSON string; not standard but practical."""
    lines: list[str] = []
    for line in config_str.splitlines():
        stripped: str = line.strip()
        if stripped.startswith("#"):
            line = ""  # replace comment line with empty line to preserve line numbering
        elif stripped.endswith("#"):
            i = line.rfind("#", 0, line.rindex("#"))
            if i >= 0:
                line = line[0:i]  # strip line-ending comment
        lines.append(line)
    return "\n".join(lines)


def _get_dict_config_logger(log_params: LogParams, args: argparse.Namespace, logger_name_suffix: str = "") -> Logger:
    """Creates a logger from a JSON config file with variable substitution."""
    import json  # lazy import for startup perf
    from logging.config import dictConfig  # lazy import for startup perf

    _, logger_subname = _resolve_logger_names(logger_name_suffix)
    prefix: str = PROG_NAME + "."
    log_config_vars: dict[str, str] = {
        prefix + "sub.logger": logger_subname,
        prefix + "get_default_log_formatter": __name__ + ".get_default_log_formatter",
        prefix + "log_level": log_params.log_level,
        prefix + "log_dir": log_params.log_dir,
        prefix + "log_file": os.path.basename(log_params.log_file),
        prefix + "timestamp": log_params.timestamp,
        prefix + "dryrun": "dryrun" if args.dryrun else "",
    }
    log_config_vars.update(log_params.log_config_vars)  # merge variables passed into CLI with convenience variables

    log_config_file_str: str = log_params.log_config_file
    if log_config_file_str.startswith("+"):
        path: str = log_config_file_str[1:]
        basename_stem: str = Path(path).stem  # stem is basename without file extension ("bzfs_log_config")
        if not ("bzfs_log_config" in basename_stem and os.path.basename(path).endswith(".json")):
            die(f"--log-config-file: basename must contain 'bzfs_log_config' and end with '.json': {path}")
        with open_nofollow(path, "r", encoding="utf-8") as fd:
            log_config_file_str = fd.read()

    def substitute_log_config_vars(config_str: str, log_config_variables: dict[str, str]) -> str:
        """Substitutes ${name[:default]} placeholders within JSON with values from log_config_variables."""

        def substitute_fn(match: re.Match) -> str:
            """Returns JSON replacement for variable placeholder."""
            varname: str = match.group(1)
            error_msg: str | None = _validate_log_config_variable_name(varname)
            if error_msg:
                raise ValueError(error_msg)
            replacement: str | None = log_config_variables.get(varname)
            if not replacement:
                default: str | None = match.group(3)
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

    log_config_file_str = _remove_json_comments(log_config_file_str)
    if not log_config_file_str.strip().startswith("{"):
        log_config_file_str = "{\n" + log_config_file_str  # lenient JSON parsing
    if not log_config_file_str.strip().endswith("}"):
        log_config_file_str = log_config_file_str + "\n}"  # lenient JSON parsing
    log_config_file_str = substitute_log_config_vars(log_config_file_str, log_config_vars)
    # if args is not None and args.verbose >= 2:
    #     print("[T] Substituted log_config_file_str:\n" + log_config_file_str, flush=True)
    log_config_dict: dict = json.loads(log_config_file_str)
    _validate_log_config_dict(log_config_dict)
    dictConfig(log_config_dict)
    return logging.getLogger(logger_subname)


def validate_log_config_variable(var: str) -> str | None:
    """Returns error message if NAME:VALUE pair is malformed else ``None``."""
    if not var.strip():
        return "Invalid log config NAME:VALUE variable. Variable must not be empty: " + var
    if ":" not in var:
        return "Invalid log config NAME:VALUE variable. Variable is missing a colon character: " + var
    return _validate_log_config_variable_name(var[0 : var.index(":")])


def _validate_log_config_variable_name(name: str) -> str | None:
    """Validates log config variable name and return error message if invalid."""
    if not name:
        return "Invalid log config variable name. Name must not be empty: " + name
    bad_chars: str = "${} " + '"' + "'"
    if any(char in bad_chars for char in name):
        return f"Invalid log config variable name. Name must not contain forbidden {bad_chars} characters: " + name
    if any(char.isspace() for char in name):
        return "Invalid log config variable name. Name must not contain whitespace: " + name
    return None


def _validate_log_config_dict(config: dict) -> None:
    """Recursively scans the logging configuration dictionary to ensure that any instantiated objects via the '()' key and
    'class' key are on an approved whitelist; This prevents arbitrary code execution from a malicious config file."""
    callable_whitelist: set[str] = {
        # Safe factories/callables
        __name__ + ".get_default_log_formatter",
        # Constants resolved via ext:// resolution
        "socket.SOCK_DGRAM",
        "socket.SOCK_STREAM",
        "sys.stdout",
        "sys.stderr",
    }
    class_whitelist: set[str] = {
        # Safe handler/formatter classes
        "logging.StreamHandler",
        "logging.FileHandler",
        "logging.handlers.SysLogHandler",
        "logging.Formatter",
    }
    if isinstance(config, dict):
        for key, value in config.items():
            if key == "()" and value not in callable_whitelist:
                die(f"--log-config-file: Disallowed callable '{value}'. For security, only specific classes are permitted.")
            if key == "class" and value not in class_whitelist:
                die(f"--log-config-file: Disallowed class '{value}'. For security, only specific classes are permitted.")
            _validate_log_config_dict(value)
    elif isinstance(config, list):
        for item in config:
            _validate_log_config_dict(item)


#############################################################################
class Tee:
    """File-like object that duplicates writes to multiple streams."""

    def __init__(self, *files: IO[Any]) -> None:
        self.files: Final = files

    def write(self, obj: str) -> None:
        """Write ``obj`` to all target files and flush immediately."""
        for file in self.files:
            file.write(obj)
            file.flush()

    def flush(self) -> None:
        """Flush all target streams."""
        for file in self.files:
            file.flush()

    def fileno(self) -> int:
        """Return file descriptor of the first target stream."""
        return self.files[0].fileno()
