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

from __future__ import annotations
import argparse
import logging
import logging.config
import logging.handlers
import os
import re
import socket
import sys
from datetime import datetime
from logging import Logger
from pathlib import Path
from typing import (
    IO,
    TYPE_CHECKING,
)

from bzfs_main.utils import (
    die,
    log_stderr,
    log_stdout,
    log_trace,
    open_nofollow,
    prog_name,
)

if TYPE_CHECKING:  # pragma: no cover
    from bzfs_main.bzfs import LogParams


def get_logger_name() -> str:
    from bzfs_main import bzfs

    return bzfs.__name__


def get_logger_subname() -> str:
    return get_logger_name() + ".sub"  # the logger name for use by --log-config-file


def reset_logger() -> None:
    """Remove and close logging handlers (and close their files) and reset loggers to default state."""
    for log in [logging.getLogger(get_logger_name()), logging.getLogger(get_logger_subname())]:
        for handler in log.handlers.copy():
            log.removeHandler(handler)
            handler.flush()
            handler.close()
        for _filter in log.filters.copy():
            log.removeFilter(_filter)
        log.setLevel(logging.NOTSET)
        log.propagate = True


def get_logger(log_params: LogParams, args: argparse.Namespace, log: Logger | None = None) -> Logger:
    add_trace_loglevel()
    logging.addLevelName(log_stderr, "STDERR")
    logging.addLevelName(log_stdout, "STDOUT")

    if log is not None:
        assert isinstance(log, Logger)
        return log  # use third party provided logger object
    elif args.log_config_file:
        clog = get_dict_config_logger(log_params, args)  # use logger defined in config file, and afterwards ...
    # ... add our own handlers unless matching handlers are already present
    default_log = get_default_logger(log_params, args)
    return clog if args.log_config_file else default_log


def get_default_logger(log_params: LogParams, args: argparse.Namespace) -> Logger:
    sublog = logging.getLogger(get_logger_subname())
    log = logging.getLogger(get_logger_name())
    log.setLevel(log_params.log_level)
    log.propagate = False  # don't propagate log messages up to the root logger to avoid emitting duplicate messages

    if not any(isinstance(h, logging.StreamHandler) and h.stream in [sys.stdout, sys.stderr] for h in sublog.handlers):
        handler: logging.Handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(get_default_log_formatter(log_params=log_params))
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


log_level_prefixes = {
    logging.CRITICAL: "[C] CRITICAL:",
    logging.ERROR: "[E] ERROR:",
    logging.WARNING: "[W]",
    logging.INFO: "[I]",
    logging.DEBUG: "[D]",
    log_trace: "[T]",
}


def get_default_log_formatter(prefix: str = "", log_params: LogParams | None = None) -> logging.Formatter:
    level_prefixes_ = log_level_prefixes
    log_stderr_ = log_stderr
    log_stdout_ = log_stdout
    terminal_cols = [0 if log_params is None else None]  # 'None' indicates "configure value later"

    class DefaultLogFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            levelno = record.levelno
            if levelno != log_stderr_ and levelno != log_stdout_:  # emit stdout and stderr "as-is" (no formatting)
                timestamp = datetime.now().isoformat(sep=" ", timespec="seconds")  # 2024-09-03 12:26:15
                ts_level = f"{timestamp} {level_prefixes_.get(levelno, '')} "
                msg = record.msg
                i = msg.find("%s")
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
            cols = terminal_cols[0]
            if cols is None:
                cols = self.ljust_cols()
            msg = msg.ljust(cols)  # w/ progress line, "overwrite" trailing chars of previous msg with spaces
            return msg

        @staticmethod
        def ljust_cols() -> int:
            # lock-free yet thread-safe late configuration-based init for prettier ProgressReporter output
            # log_params.params and available_programs are not fully initialized yet before detect_available_programs() ends
            cols = 0
            assert log_params is not None
            p = log_params.params
            if p is not None and "local" in p.available_programs:
                if "pv" in p.available_programs["local"]:
                    cols = p.terminal_columns
                    assert cols is not None
                terminal_cols[0] = cols  # finally, resolve to use this specific value henceforth
            return cols

    return DefaultLogFormatter()


def get_simple_logger(program: str) -> Logger:
    class LevelFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            record.level_prefix = log_level_prefixes.get(record.levelno, "")
            record.program = program
            return super().format(record)

    add_trace_loglevel()
    log = logging.getLogger(program)
    log.setLevel(logging.INFO)
    log.propagate = False
    if not any(isinstance(h, logging.StreamHandler) for h in log.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(
            LevelFormatter(fmt="%(asctime)s %(level_prefix)s [%(program)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        log.addHandler(handler)
    return log


def add_trace_loglevel() -> None:
    logging.addLevelName(log_trace, "TRACE")


def get_syslog_address(address: str, log_syslog_socktype: str) -> tuple[str | tuple[str, int], socket.SocketKind | None]:
    address = address.strip()
    socktype: socket.SocketKind | None = None
    if ":" in address:
        host, port_str = address.rsplit(":", 1)
        addr = (host.strip(), int(port_str.strip()))
        socktype = socket.SOCK_DGRAM if log_syslog_socktype == "UDP" else socket.SOCK_STREAM  # for TCP
        return addr, socktype
    return address, socktype


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


def get_dict_config_logger(log_params: LogParams, args: argparse.Namespace) -> Logger:
    import json

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
        path = log_config_file_str[1:]
        basename_stem = Path(path).stem  # stem is basename without file extension ("bzfs_log_config")
        if not ("bzfs_log_config" in basename_stem and os.path.basename(path).endswith(".json")):
            die(f"--log-config-file: basename must contain 'bzfs_log_config' and end with '.json': {path}")
        with open_nofollow(path, "r", encoding="utf-8") as fd:
            log_config_file_str = fd.read()

    def substitute_log_config_vars(config_str: str, log_config_variables: dict[str, str]) -> str:
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
    # if args is not None and args.verbose >= 2:
    #     print("[T] Substituted log_config_file_str:\n" + log_config_file_str, flush=True)
    log_config_dict = json.loads(log_config_file_str)
    validate_log_config_dict(log_config_dict)
    logging.config.dictConfig(log_config_dict)
    return logging.getLogger(get_logger_subname())


def validate_log_config_variable(var: str) -> str | None:
    if not var.strip():
        return "Invalid log config NAME:VALUE variable. Variable must not be empty: " + var
    if ":" not in var:
        return "Invalid log config NAME:VALUE variable. Variable is missing a colon character: " + var
    return validate_log_config_variable_name(var[0 : var.index(":")])


def validate_log_config_variable_name(name: str) -> str | None:
    if not name:
        return "Invalid log config variable name. Name must not be empty: " + name
    bad_chars = "${} " + '"' + "'"
    if any(char in bad_chars for char in name):
        return f"Invalid log config variable name. Name must not contain forbidden {bad_chars} characters: " + name
    if any(char.isspace() for char in name):
        return "Invalid log config variable name. Name must not contain whitespace: " + name
    return None


def validate_log_config_dict(config: dict) -> None:
    """Recursively scans the logging configuration dictionary to ensure that any instantiated objects via the '()' key are
    on an approved whitelist. This prevents arbitrary code execution from a malicious config file."""
    whitelist = {
        "logging.StreamHandler",
        "logging.FileHandler",
        "logging.handlers.SysLogHandler",
        "socket.SOCK_DGRAM",
        "socket.SOCK_STREAM",
        "sys.stdout",
        "sys.stderr",
        __name__ + ".get_default_log_formatter",
    }
    if isinstance(config, dict):
        for key, value in config.items():
            if key == "()" and value not in whitelist:
                die(f"--log-config-file: Disallowed callable '{value}'. For security, only specific classes are permitted.")
            validate_log_config_dict(value)
    elif isinstance(config, list):
        for item in config:
            validate_log_config_dict(item)


#############################################################################
class Tee:
    def __init__(self, *files: IO) -> None:
        self.files = files

    def write(self, obj: str) -> None:
        for file in self.files:
            file.write(obj)
            file.flush()  # Ensure each write is flushed immediately

    def flush(self) -> None:
        for file in self.files:
            file.flush()

    def fileno(self) -> int:
        return self.files[0].fileno()
