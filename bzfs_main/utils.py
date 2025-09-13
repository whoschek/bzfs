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
"""Collection of helper functions used across bzfs; includes environment variable parsing, process management and lightweight
concurrency primitives.

Everything in this module relies only on the standard library so other modules remain dependency free. Each utility favors
simple, predictable behavior on all supported platforms.
"""

from __future__ import annotations
import argparse
import bisect
import collections
import contextlib
import errno
import logging
import os
import pwd
import random
import re
import signal
import stat
import subprocess
import sys
import threading
import time
import types
from collections import (
    defaultdict,
    deque,
)
from concurrent.futures import (
    Executor,
    Future,
    ThreadPoolExecutor,
)
from datetime import (
    datetime,
    timedelta,
    timezone,
    tzinfo,
)
from subprocess import (
    DEVNULL,
    PIPE,
)
from typing import (
    IO,
    Any,
    Callable,
    Final,
    Generic,
    ItemsView,
    Iterable,
    Iterator,
    List,
    NoReturn,
    Protocol,
    Sequence,
    TextIO,
    Tuple,
    TypeVar,
    cast,
)

# constants:
PROG_NAME: str = "bzfs"
ENV_VAR_PREFIX: str = PROG_NAME + "_"
DIE_STATUS: int = 3
DESCENDANTS_RE_SUFFIX: str = r"(?:/.*)?"  # also match descendants of a matching dataset
LOG_STDERR: int = (logging.INFO + logging.WARNING) // 2  # custom log level is halfway in between
LOG_STDOUT: int = (LOG_STDERR + logging.INFO) // 2  # custom log level is halfway in between
LOG_DEBUG: int = logging.DEBUG
LOG_TRACE: int = logging.DEBUG // 2  # custom log level is halfway in between
SNAPSHOT_FILTERS_VAR: str = "snapshot_filters_var"
YEAR_WITH_FOUR_DIGITS_REGEX: re.Pattern[str] = re.compile(r"[1-9][0-9][0-9][0-9]")  # empty shall not match non-empty target
UNIX_TIME_INFINITY_SECS: int = 2**64  # billions of years and to be extra safe, larger than the largest ZFS GUID
DONT_SKIP_DATASET: str = ""
SHELL_CHARS: str = '"' + "'`~!@#$%^&*()+={}[]|;<>?,\\"
FILE_PERMISSIONS: int = stat.S_IRUSR | stat.S_IWUSR  # rw------- (user read + write)
DIR_PERMISSIONS: int = stat.S_IRWXU  # rwx------ (user read + write + execute)

RegexList = List[Tuple[re.Pattern, bool]]  # Type alias


def getenv_any(key: str, default: str | None = None) -> str | None:
    """All shell environment variable names used for configuration start with this prefix."""
    return os.getenv(ENV_VAR_PREFIX + key, default)


def getenv_int(key: str, default: int) -> int:
    """Returns environment variable ``key`` as int with ``default`` fallback."""
    return int(cast(str, getenv_any(key, str(default))))


def getenv_bool(key: str, default: bool = False) -> bool:
    """Returns environment variable ``key`` as bool with ``default`` fallback."""
    return cast(str, getenv_any(key, str(default))).lower().strip() == "true"


def cut(field: int = -1, separator: str = "\t", lines: list[str] | None = None) -> list[str]:
    """Retains only column number 'field' in a list of TSV/CSV lines; Analog to Unix 'cut' CLI command."""
    assert lines is not None
    assert isinstance(lines, list)
    assert len(separator) == 1
    if field == 1:
        return [line[0 : line.index(separator)] for line in lines]
    elif field == 2:
        return [line[line.index(separator) + 1 :] for line in lines]
    else:
        raise ValueError("Unsupported parameter value")


def drain(iterable: Iterable[Any]) -> None:
    """Consumes all items in the iterable, effectively draining it."""
    for _ in iterable:
        _ = None  # help gc


K_ = TypeVar("K_")
V_ = TypeVar("V_")
R_ = TypeVar("R_")


def shuffle_dict(dictionary: dict[K_, V_]) -> dict[K_, V_]:
    """Returns a new dict with items shuffled randomly."""
    items: list[tuple[K_, V_]] = list(dictionary.items())
    random.shuffle(items)
    return dict(items)


def sorted_dict(dictionary: dict[K_, V_]) -> dict[K_, V_]:
    """Returns a new dict with items sorted primarily by key and secondarily by value."""
    return dict(sorted(dictionary.items()))


def tail(file: str, n: int, errors: str | None = None) -> Sequence[str]:
    """Return the last ``n`` lines of ``file`` without following symlinks."""
    if not os.path.isfile(file):
        return []
    with open_nofollow(file, "r", encoding="utf-8", errors=errors, check_owner=False) as fd:
        return deque(fd, maxlen=n)


NAMED_CAPTURING_GROUP: re.Pattern[str] = re.compile(r"^" + re.escape("(?P<") + r"[^\W\d]\w*" + re.escape(">"))


def replace_capturing_groups_with_non_capturing_groups(regex: str) -> str:
    """Replaces regex capturing groups with non-capturing groups for better matching performance.

    Unnamed capturing groups example: '(.*/)?tmp(foo|bar)(?!public)\\(' --> '(?:.*/)?tmp(?:foo|bar)(?!public)\\('
    Aka replaces parenthesis '(' followed by a char other than question mark '?', but not preceded by a backslash
    with the replacement string '(?:'

    Named capturing group example: '(?P<name>abc)' --> '(?:abc)'
    Aka replaces '(?P<' followed by a valid name followed by '>', but not preceded by a backslash
    with the replacement string '(?:'

    Also see https://docs.python.org/3/howto/regex.html#non-capturing-and-named-groups
    """
    i = len(regex) - 2
    while i >= 0:
        i = regex.rfind("(", 0, i + 1)
        if i >= 0 and (i == 0 or regex[i - 1] != "\\"):
            if regex[i + 1] != "?":
                regex = f"{regex[0:i]}(?:{regex[i + 1:]}"  # unnamed capturing group
            else:  # potentially a valid named capturing group
                regex = regex[0:i] + NAMED_CAPTURING_GROUP.sub(repl="(?:", string=regex[i:], count=1)
        i -= 1
    return regex


def get_home_directory() -> str:
    """Reliably detects home dir without using HOME env var."""
    # thread-safe version of: os.environ.pop('HOME', None); os.path.expanduser('~')
    return pwd.getpwuid(os.getuid()).pw_dir


def human_readable_bytes(num_bytes: float, separator: str = " ", precision: int | None = None) -> str:
    """Formats 'num_bytes' as a human-readable size; for example "567 MiB"."""
    sign = "-" if num_bytes < 0 else ""
    s = abs(num_bytes)
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB", "RiB", "QiB")
    n = len(units) - 1
    i = 0
    while s >= 1024 and i < n:
        s /= 1024
        i += 1
    formatted_num = human_readable_float(s) if precision is None else f"{s:.{precision}f}"
    return f"{sign}{formatted_num}{separator}{units[i]}"


def human_readable_duration(duration: float, unit: str = "ns", separator: str = "", precision: int | None = None) -> str:
    """Formats a duration in human units, automatically scaling as needed; for example "567ms"."""
    sign = "-" if duration < 0 else ""
    t = abs(duration)
    units = ("ns", "μs", "ms", "s", "m", "h", "d")
    i = units.index(unit)
    if t < 1 and t != 0:
        nanos = (1, 1_000, 1_000_000, 1_000_000_000, 60 * 1_000_000_000, 60 * 60 * 1_000_000_000, 3600 * 24 * 1_000_000_000)
        t *= nanos[i]
        i = 0
    while t >= 1000 and i < 3:
        t /= 1000
        i += 1
    if i >= 3:
        while t >= 60 and i < 5:
            t /= 60
            i += 1
    if i >= 5:
        while t >= 24 and i < len(units) - 1:
            t /= 24
            i += 1
    formatted_num = human_readable_float(t) if precision is None else f"{t:.{precision}f}"
    return f"{sign}{formatted_num}{separator}{units[i]}"


def human_readable_float(number: float) -> str:
    """Formats ``number`` with a variable precision depending on magnitude.

    This design mirrors the way humans round values when scanning logs.

    If the number has one digit before the decimal point (0 <= abs(number) < 10):
      Round and use two decimals after the decimal point (e.g., 3.14559 --> "3.15").

    If the number has two digits before the decimal point (10 <= abs(number) < 100):
      Round and use one decimal after the decimal point (e.g., 12.36 --> "12.4").

    If the number has three or more digits before the decimal point (abs(number) >= 100):
      Round and use zero decimals after the decimal point (e.g., 123.556 --> "124").

    Ensures no unnecessary trailing zeroes are retained: Example: 1.500 --> "1.5", 1.00 --> "1"
    """
    abs_number = abs(number)
    precision = 2 if abs_number < 10 else 1 if abs_number < 100 else 0
    if precision == 0:
        return str(round(number))
    result = f"{number:.{precision}f}"
    assert "." in result
    result = result.rstrip("0").rstrip(".")  # Remove trailing zeros and trailing decimal point if empty
    return "0" if result == "-0" else result


def percent(number: int, total: int) -> str:
    """Returns percentage string of ``number`` relative to ``total``."""
    return f"{number}={'inf' if total == 0 else human_readable_float(100 * number / total)}%"


def open_nofollow(
    path: str,
    mode: str = "r",
    buffering: int = -1,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    *,
    perm: int = stat.S_IRUSR | stat.S_IWUSR,  # rw------- (user read + write)
    check_owner: bool = True,
    **kwargs: Any,
) -> IO[Any]:
    """Behaves exactly like built-in open(), except that it refuses to follow symlinks, i.e. raises OSError with
    errno.ELOOP/EMLINK if basename of path is a symlink.

    Also, can specify permissions on O_CREAT, and verify ownership.
    """
    if not mode:
        raise ValueError("Must have exactly one of create/read/write/append mode and at most one plus")
    flags = {
        "r": os.O_RDONLY,
        "w": os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        "a": os.O_WRONLY | os.O_CREAT | os.O_APPEND,
        "x": os.O_WRONLY | os.O_CREAT | os.O_EXCL,
    }.get(mode[0])
    if flags is None:
        raise ValueError(f"invalid mode {mode!r}")
    if "+" in mode:
        flags = (flags & ~os.O_WRONLY) | os.O_RDWR
    flags |= os.O_NOFOLLOW | os.O_CLOEXEC
    fd = os.open(path, flags=flags, mode=perm)
    try:
        if check_owner:
            st_uid: int = os.fstat(fd).st_uid
            if st_uid != os.geteuid():  # verify ownership is current effective UID
                raise PermissionError(errno.EPERM, f"{path!r} is owned by uid {st_uid}, not {os.geteuid()}", path)
        return os.fdopen(fd, mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline, **kwargs)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        raise


P = TypeVar("P")


def find_match(
    seq: Sequence[P],
    predicate: Callable[[P], bool],
    start: int | None = None,
    end: int | None = None,
    reverse: bool = False,
    raises: bool | str | Callable[[], str] = False,  # raises: bool | str | Callable = False,  # python >= 3.10
) -> int:
    """Returns the integer index within seq of the first item (or last item if reverse==True) that matches the given
    predicate condition. If no matching item is found returns -1 or ValueError, depending on the raises parameter, which is a
    bool indicating whether to raise an error, or a string containing the error message, but can also be a Callable/lambda in
    order to support efficient deferred generation of error messages. Analog to str.find(), including slicing semantics with
    parameters start and end. For example, seq can be a list, tuple or str.

    Example usage:
        lst = ["a", "b", "-c", "d"]
        i = find_match(lst, lambda arg: arg.startswith("-"), start=1, end=3, reverse=True)
        if i >= 0:
            ...
        i = find_match(lst, lambda arg: arg.startswith("-"), raises=f"Tag {tag} not found in {file}")
        i = find_match(lst, lambda arg: arg.startswith("-"), raises=lambda: f"Tag {tag} not found in {file}")
    """
    offset: int = 0 if start is None else start if start >= 0 else len(seq) + start
    if start is not None or end is not None:
        seq = seq[start:end]
    for i, item in enumerate(reversed(seq) if reverse else seq):
        if predicate(item):
            if reverse:
                return len(seq) - i - 1 + offset
            else:
                return i + offset
    if raises is False or raises is None:
        return -1
    if raises is True:
        raise ValueError("No matching item found in sequence")
    if callable(raises):
        raises = raises()
    raise ValueError(raises)


def is_descendant(dataset: str, of_root_dataset: str) -> bool:
    """Returns True if ``dataset`` lies under ``of_root_dataset`` in the dataset hierarchy, or is the same."""
    return dataset == of_root_dataset or dataset.startswith(of_root_dataset + "/")


def has_duplicates(sorted_list: list[Any]) -> bool:
    """Returns True if any adjacent items within the given sorted sequence are equal."""
    return any(a == b for a, b in zip(sorted_list, sorted_list[1:]))


def dry(msg: str, is_dry_run: bool) -> str:
    """Prefix ``msg`` with 'Dry' when in dry-run mode."""
    return "Dry " + msg if is_dry_run else msg


def relativize_dataset(dataset: str, root_dataset: str) -> str:
    """Converts an absolute dataset path to one relative to ``root_dataset``.

    Example: root_dataset=tank/foo, dataset=tank/foo/bar/baz --> relative_path=/bar/baz.
    """
    return dataset[len(root_dataset) :]


def dataset_paths(dataset: str) -> Iterator[str]:
    """Enumerates all paths of a valid ZFS dataset name; Example: "a/b/c" --> yields "a", "a/b", "a/b/c"."""
    i: int = 0
    while i >= 0:
        i = dataset.find("/", i)
        if i < 0:
            yield dataset
        else:
            yield dataset[:i]
            i += 1


def replace_prefix(s: str, old_prefix: str, new_prefix: str) -> str:
    """In a string s, replaces a leading old_prefix string with new_prefix; assumes the leading string is present."""
    assert s.startswith(old_prefix)
    return new_prefix + s[len(old_prefix) :]


def replace_in_lines(lines: list[str], old: str, new: str, count: int = -1) -> None:
    """Replaces ``old`` with ``new`` in-place for every string in ``lines``."""
    for i in range(len(lines)):
        lines[i] = lines[i].replace(old, new, count)


TAPPEND = TypeVar("TAPPEND")


def append_if_absent(lst: list[TAPPEND], *items: TAPPEND) -> list[TAPPEND]:
    """Appends items to list if they are not already present."""
    for item in items:
        if item not in lst:
            lst.append(item)
    return lst


def xappend(lst: list[TAPPEND], *items: TAPPEND | Iterable[TAPPEND]) -> list[TAPPEND]:
    """Appends each of the items to the given list if the item is "truthy", for example not None and not an empty string; If
    an item is an iterable does so recursively, flattening the output."""
    for item in items:
        if isinstance(item, str) or not isinstance(item, collections.abc.Iterable):
            if item:
                lst.append(cast(TAPPEND, item))
        else:
            xappend(lst, *item)
    return lst


def is_included(name: str, include_regexes: RegexList, exclude_regexes: RegexList) -> bool:
    """Returns True if the name matches at least one of the include regexes but none of the exclude regexes; else False.

    A regex that starts with a `!` is a negation - the regex matches if the regex without the `!` prefix does not match.
    """
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


def compile_regexes(regexes: list[str], suffix: str = "") -> RegexList:
    """Compiles regex strings and keeps track of negations."""
    assert isinstance(regexes, list)
    compiled_regexes: RegexList = []
    for regex in regexes:
        if suffix:  # disallow non-trailing end-of-str symbol in dataset regexes to ensure descendants will also match
            if regex.endswith("\\$"):
                pass  # trailing literal $ is ok
            elif regex.endswith("$"):
                regex = regex[0:-1]  # ok because all users of compile_regexes() call re.fullmatch()
            elif "$" in regex:
                raise re.error("Must not use non-trailing '$' character", regex)
        if is_negation := regex.startswith("!"):
            regex = regex[1:]
        regex = replace_capturing_groups_with_non_capturing_groups(regex)
        if regex != ".*" or not (suffix.startswith("(") and suffix.endswith(")?")):
            regex = f"{regex}{suffix}"
        compiled_regexes.append((re.compile(regex), is_negation))
    return compiled_regexes


def list_formatter(iterable: Iterable[Any], separator: str = " ", lstrip: bool = False) -> Any:
    """Lazy formatter joining items with ``separator`` used to avoid overhead in disabled log levels."""

    class CustomListFormatter:
        """Formatter object that joins items when converted to ``str``."""

        def __str__(self) -> str:
            s = separator.join(map(str, iterable))
            return s.lstrip() if lstrip else s

    return CustomListFormatter()


def pretty_print_formatter(obj_to_format: Any) -> Any:
    """Lazy pprint formatter used to avoid overhead in disabled log levels."""

    class PrettyPrintFormatter:
        """Formatter that pretty-prints the object on conversion to ``str``."""

        def __str__(self) -> str:
            import pprint

            return pprint.pformat(vars(obj_to_format))

    return PrettyPrintFormatter()


def stderr_to_str(stderr: Any) -> str:
    """Workaround for https://github.com/python/cpython/issues/87597."""
    return str(stderr) if not isinstance(stderr, bytes) else stderr.decode("utf-8")


def xprint(log: logging.Logger, value: Any, run: bool = True, end: str = "\n", file: TextIO | None = None) -> None:
    """Optionally logs ``value`` at stdout/stderr level."""
    if run and value:
        value = value if end else str(value).rstrip()
        level = LOG_STDOUT if file is sys.stdout else LOG_STDERR
        log.log(level, "%s", value)


def die(msg: str, exit_code: int = DIE_STATUS, parser: argparse.ArgumentParser | None = None) -> NoReturn:
    """Exits the program with ``exit_code`` after logging ``msg``."""
    if parser is None:
        ex = SystemExit(msg)
        ex.code = exit_code
        raise ex
    else:
        parser.error(msg)


def subprocess_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess:
    """Drop-in replacement for subprocess.run() that mimics its behavior except it enhances cleanup on TimeoutExpired."""
    input_value = kwargs.pop("input", None)
    timeout = kwargs.pop("timeout", None)
    check = kwargs.pop("check", False)
    if input_value is not None:
        if kwargs.get("stdin") is not None:
            raise ValueError("input and stdin are mutually exclusive")
        kwargs["stdin"] = subprocess.PIPE

    with subprocess.Popen(*args, **kwargs) as proc:
        try:
            stdout, stderr = proc.communicate(input_value, timeout=timeout)
        except BaseException as e:
            try:
                if isinstance(e, subprocess.TimeoutExpired):
                    terminate_process_subtree(root_pid=proc.pid)  # send SIGTERM to child process and its descendants
            finally:
                proc.kill()
                raise
        else:
            exitcode: int | None = proc.poll()
            assert exitcode is not None
            if check and exitcode:
                raise subprocess.CalledProcessError(exitcode, proc.args, output=stdout, stderr=stderr)
    return subprocess.CompletedProcess(proc.args, exitcode, stdout, stderr)


def terminate_process_subtree(
    except_current_process: bool = False, root_pid: int | None = None, sig: signal.Signals = signal.SIGTERM
) -> None:
    """Sends ``sig`` to ``root_pid`` and all of its descendant processes."""
    current_pid: int = os.getpid()
    root_pid = current_pid if root_pid is None else root_pid
    pids: list[int] = _get_descendant_processes(root_pid)
    if root_pid == current_pid:
        pids += [] if except_current_process else [current_pid]
    else:
        pids.insert(0, root_pid)
    for pid in pids:
        with contextlib.suppress(OSError):
            os.kill(pid, sig)


def _get_descendant_processes(root_pid: int) -> list[int]:
    """Returns the list of all descendant process IDs for the given root PID, on Unix systems."""
    procs: defaultdict[int, list[int]] = defaultdict(list)
    cmd: list[str] = ["ps", "-Ao", "pid,ppid"]
    lines: list[str] = subprocess.run(cmd, stdin=DEVNULL, stdout=PIPE, text=True, check=True).stdout.splitlines()
    for line in lines[1:]:  # all lines except the header line
        splits: list[str] = line.split()
        assert len(splits) == 2
        pid = int(splits[0])
        ppid = int(splits[1])
        procs[ppid].append(pid)
    descendants: list[int] = []

    def recursive_append(ppid: int) -> None:
        """Recursively collect descendant PIDs starting from ``ppid``."""
        for child_pid in procs[ppid]:
            descendants.append(child_pid)
            recursive_append(child_pid)

    recursive_append(root_pid)
    return descendants


def pid_exists(pid: int) -> bool | None:
    """Returns True if a process with PID exists, False if not, or None on error."""
    if pid <= 0:
        return False
    try:  # with signal=0, no signal is actually sent, but error checking is still performed
        os.kill(pid, 0)  # ... which can be used to check for process existence on POSIX systems
    except OSError as err:
        if err.errno == errno.ESRCH:  # No such process
            return False
        if err.errno == errno.EPERM:  # Operation not permitted
            return True
        return None
    return True


def nprefix(s: str) -> str:
    """Returns a canonical snapshot prefix with trailing underscore."""
    return sys.intern(s + "_")


def ninfix(s: str) -> str:
    """Returns a canonical infix with trailing underscore when not empty."""
    return sys.intern(s + "_") if s else ""


def nsuffix(s: str) -> str:
    """Returns a canonical suffix with leading underscore when not empty."""
    return sys.intern("_" + s) if s else ""


def format_dict(dictionary: dict[Any, Any]) -> str:
    """Returns a formatted dictionary using repr for consistent output."""
    return f'"{dictionary}"'


def validate_dataset_name(dataset: str, input_text: str) -> None:
    """'zfs create' CLI does not accept dataset names that are empty or start or end in a slash, etc."""
    # Also see https://github.com/openzfs/zfs/issues/439#issuecomment-2784424
    # and https://github.com/openzfs/zfs/issues/8798
    # and (by now no longer accurate): https://docs.oracle.com/cd/E26505_01/html/E37384/gbcpt.html
    if (
        dataset in ("", ".", "..")
        or any(dataset.startswith(prefix) for prefix in ("/", "./", "../"))
        or any(dataset.endswith(suffix) for suffix in ("/", "/.", "/.."))
        or any(substring in dataset for substring in ("//", "/./", "/../"))
        or any(char in SHELL_CHARS or (char.isspace() and char != " ") for char in dataset)
        or not dataset[0].isalpha()
    ):
        die(f"Invalid ZFS dataset name: '{dataset}' for: '{input_text}'")


def validate_property_name(propname: str, input_text: str) -> str:
    """Checks that the ZFS property name contains no spaces or shell chars."""
    invalid_chars: str = SHELL_CHARS
    if not propname or any(c.isspace() or c in invalid_chars for c in propname):
        die(f"Invalid ZFS property name: '{propname}' for: '{input_text}'")
    return propname


def validate_is_not_a_symlink(msg: str, path: str, parser: argparse.ArgumentParser | None = None) -> None:
    """Checks that the given path is not a symbolic link."""
    if os.path.islink(path):
        die(f"{msg}must not be a symlink: {path}", parser=parser)


def parse_duration_to_milliseconds(duration: str, regex_suffix: str = "", context: str = "") -> int:
    """Parses human duration strings like '5m' or '2 hours' to milliseconds."""
    unit_milliseconds: dict[str, int] = {
        "milliseconds": 1,
        "millis": 1,
        "seconds": 1000,
        "secs": 1000,
        "minutes": 60 * 1000,
        "mins": 60 * 1000,
        "hours": 60 * 60 * 1000,
        "days": 86400 * 1000,
        "weeks": 7 * 86400 * 1000,
        "months": round(30.5 * 86400 * 1000),
        "years": 365 * 86400 * 1000,
    }
    match = re.fullmatch(
        r"(\d+)\s*(milliseconds|millis|seconds|secs|minutes|mins|hours|days|weeks|months|years)" + regex_suffix,
        duration,
    )
    if not match:
        if context:
            die(f"Invalid duration format: {duration} within {context}")
        else:
            raise ValueError(f"Invalid duration format: {duration}")
    assert match
    quantity: int = int(match.group(1))
    unit: str = match.group(2)
    return quantity * unit_milliseconds[unit]


def unixtime_fromisoformat(datetime_str: str) -> int:
    """Converts ISO 8601 datetime string into UTC Unix time seconds."""
    return int(datetime.fromisoformat(datetime_str).timestamp())


def isotime_from_unixtime(unixtime_in_seconds: int) -> str:
    """Converts UTC Unix time seconds into ISO 8601 datetime string."""
    tz: tzinfo = timezone.utc
    dt: datetime = datetime.fromtimestamp(unixtime_in_seconds, tz=tz)
    return dt.isoformat(sep="_", timespec="seconds")


def current_datetime(
    tz_spec: str | None = None,
    now_fn: Callable[[tzinfo | None], datetime] | None = None,
) -> datetime:
    """Returns current time in ``tz_spec`` timezone or local timezone."""
    if now_fn is None:
        now_fn = datetime.now
    return now_fn(get_timezone(tz_spec))


def get_timezone(tz_spec: str | None = None) -> tzinfo | None:
    """Returns timezone from spec or local timezone if unspecified."""
    tz: tzinfo | None
    if tz_spec is None:
        tz = None
    elif tz_spec == "UTC":
        tz = timezone.utc
    else:
        if match := re.fullmatch(r"([+-])(\d\d):?(\d\d)", tz_spec):
            sign, hours, minutes = match.groups()
            offset: int = int(hours) * 60 + int(minutes)
            offset = -offset if sign == "-" else offset
            tz = timezone(timedelta(minutes=offset))
        elif "/" in tz_spec and sys.version_info >= (3, 9):
            from zoneinfo import ZoneInfo  # requires python >= 3.9

            tz = ZoneInfo(tz_spec)
        else:
            raise ValueError(f"Invalid timezone specification: {tz_spec}")
    return tz


###############################################################################
S = TypeVar("S")


class Interner(Generic[S]):
    """Same as sys.intern() except that it isn't global and can also be used for types other than str."""

    def __init__(self, items: Iterable[S] = frozenset()) -> None:
        self._items: dict[S, S] = {v: v for v in items}

    def intern(self, item: S) -> S:
        """Interns the given item."""
        return self._items.setdefault(item, item)

    def interned(self, item: S) -> S:
        """Returns the interned (aka deduped) item if an equal item is contained, else returns the non-interned item."""
        return self._items.get(item, item)

    def __contains__(self, item: S) -> bool:
        return item in self._items


###############################################################################
class SnapshotPeriods:  # thread-safe
    """Parses snapshot suffix strings and converts between durations."""

    def __init__(self) -> None:
        """Initialize lookup tables of suffixes and corresponding millis."""
        self.suffix_milliseconds: Final = {
            "yearly": 365 * 86400 * 1000,
            "monthly": round(30.5 * 86400 * 1000),
            "weekly": 7 * 86400 * 1000,
            "daily": 86400 * 1000,
            "hourly": 60 * 60 * 1000,
            "minutely": 60 * 1000,
            "secondly": 1000,
            "millisecondly": 1,
        }
        self.period_labels: Final = {
            "yearly": "years",
            "monthly": "months",
            "weekly": "weeks",
            "daily": "days",
            "hourly": "hours",
            "minutely": "minutes",
            "secondly": "seconds",
            "millisecondly": "milliseconds",
        }
        self._suffix_regex0: Final = re.compile(rf"([1-9][0-9]*)?({'|'.join(self.suffix_milliseconds.keys())})")
        self._suffix_regex1: Final = re.compile("_" + self._suffix_regex0.pattern)

    def suffix_to_duration0(self, suffix: str) -> tuple[int, str]:
        """Parse suffix like '10minutely' to (10, 'minutely')."""
        return self._suffix_to_duration(suffix, self._suffix_regex0)

    def suffix_to_duration1(self, suffix: str) -> tuple[int, str]:
        """Like :meth:`suffix_to_duration0` but expects an underscore prefix."""
        return self._suffix_to_duration(suffix, self._suffix_regex1)

    @staticmethod
    def _suffix_to_duration(suffix: str, regex: re.Pattern) -> tuple[int, str]:
        """Example: Converts '2 hourly' to (2, 'hourly') and 'hourly' to (1, 'hourly')."""
        if match := regex.fullmatch(suffix):
            duration_amount: int = int(match.group(1)) if match.group(1) else 1
            assert duration_amount > 0
            duration_unit: str = match.group(2)
            return duration_amount, duration_unit
        else:
            return 0, ""

    def label_milliseconds(self, snapshot: str) -> int:
        """Returns duration encoded in ``snapshot`` suffix, in milliseconds."""
        i = snapshot.rfind("_")
        snapshot = "" if i < 0 else snapshot[i + 1 :]
        duration_amount, duration_unit = self._suffix_to_duration(snapshot, self._suffix_regex0)
        return duration_amount * self.suffix_milliseconds.get(duration_unit, 0)


#############################################################################
class Comparable(Protocol):
    """Partial ordering protocol used by :class:`SmallPriorityQueue`."""

    def __lt__(self, other: Any) -> bool:  # pragma: no cover - behavior defined by implementer
        ...


T = TypeVar("T", bound=Comparable)  # Generic type variable for elements stored in a SmallPriorityQueue


class SmallPriorityQueue(Generic[T]):
    """A priority queue that can handle updates to the priority of any element that is already contained in the queue, and
    does so very efficiently if there are a small number of elements in the queue (no more than thousands), as is the case
    for us.

    Could be implemented using a SortedList via https://github.com/grantjenks/python-sortedcontainers or using an indexed
    priority queue via
    https://github.com/nvictus/pqdict.
    But, to avoid an external dependency, is actually implemented
    using a simple yet effective binary search-based sorted list that can handle updates to the priority of elements that
    are already contained in the queue, via removal of the element, followed by update of the element, followed by
    (re)insertion. Duplicate elements (if any) are maintained in their order of insertion relative to other duplicates.
    """

    def __init__(self, reverse: bool = False) -> None:
        """Creates an empty queue; sort order flips when ``reverse`` is True."""
        self._lst: list[T] = []
        self._reverse: bool = reverse

    def clear(self) -> None:
        """Removes all elements from the queue."""
        self._lst.clear()

    def push(self, element: T) -> None:
        """Inserts ``element`` while maintaining sorted order."""
        bisect.insort(self._lst, element)

    def pop(self) -> T:
        """Removes and returns the smallest (or largest if reverse == True) element from the queue."""
        return self._lst.pop() if self._reverse else self._lst.pop(0)

    def peek(self) -> T:
        """Returns the smallest (or largest if reverse == True) element without removing it."""
        return self._lst[-1] if self._reverse else self._lst[0]

    def remove(self, element: T) -> bool:
        """Removes the first occurrence of ``element`` and returns True if it was present."""
        lst = self._lst
        i = bisect.bisect_left(lst, element)
        is_contained = i < len(lst) and lst[i] == element
        if is_contained:
            del lst[i]  # is an optimized memmove()
        return is_contained

    def __len__(self) -> int:
        """Returns the number of queued elements."""
        return len(self._lst)

    def __contains__(self, element: T) -> bool:
        """Returns ``True`` if ``element`` is present."""
        lst = self._lst
        i = bisect.bisect_left(lst, element)
        return i < len(lst) and lst[i] == element

    def __iter__(self) -> Iterator[T]:
        """Iterates over queued elements in priority order."""
        return reversed(self._lst) if self._reverse else iter(self._lst)

    def __repr__(self) -> str:
        """Representation showing queue contents in current order."""
        return repr(list(reversed(self._lst))) if self._reverse else repr(self._lst)


###############################################################################
class SortedInterner(Generic[T]):
    """Same as sys.intern() except that it isn't global and that it assumes the input list is sorted (for binary search)."""

    def __init__(self, sorted_list: list[T]) -> None:
        self._lst: list[T] = sorted_list

    def interned(self, element: T) -> T:
        """Returns the interned (aka deduped) item if an equal item is contained, else returns the non-interned item."""
        lst = self._lst
        i = binary_search(lst, element)
        return lst[i] if i >= 0 else element

    def __contains__(self, element: T) -> bool:
        """Returns ``True`` if ``element`` is present."""
        return binary_search(self._lst, element) >= 0


def binary_search(sorted_list: list[T], item: T) -> int:
    """Java-style binary search; Returns index >=0 if an equal item is found in list, else '-insertion_point-1'; If it
    returns index >=0, the index will be the left-most index in case multiple such equal items are contained."""
    i = bisect.bisect_left(sorted_list, item)
    return i if i < len(sorted_list) and sorted_list[i] == item else -i - 1


#############################################################################
class SynchronizedBool:
    """Thread-safe wrapper around a regular bool."""

    def __init__(self, val: bool) -> None:
        assert isinstance(val, bool)
        self._lock: threading.Lock = threading.Lock()
        self._value: bool = val

    @property
    def value(self) -> bool:
        """Returns the current boolean value."""
        with self._lock:
            return self._value

    @value.setter
    def value(self, new_value: bool) -> None:
        """Atomically assign ``new_value``."""
        with self._lock:
            self._value = new_value

    def get_and_set(self, new_value: bool) -> bool:
        """Swaps in ``new_value`` and return the previous value."""
        with self._lock:
            old_value = self._value
            self._value = new_value
            return old_value

    def compare_and_set(self, expected_value: bool, new_value: bool) -> bool:
        """Sets to ``new_value`` only if current value equals ``expected_value``."""
        with self._lock:
            eq: bool = self._value == expected_value
            if eq:
                self._value = new_value
            return eq

    def __bool__(self) -> bool:
        return self.value

    def __repr__(self) -> str:
        return repr(self.value)

    def __str__(self) -> str:
        return str(self.value)


#############################################################################
K = TypeVar("K")
V = TypeVar("V")


class SynchronizedDict(Generic[K, V]):
    """Thread-safe wrapper around a regular dict."""

    def __init__(self, val: dict[K, V]) -> None:
        assert isinstance(val, dict)
        self._lock: threading.Lock = threading.Lock()
        self._dict: dict[K, V] = val

    def __getitem__(self, key: K) -> V:
        with self._lock:
            return self._dict[key]

    def __setitem__(self, key: K, value: V) -> None:
        with self._lock:
            self._dict[key] = value

    def __delitem__(self, key: K) -> None:
        with self._lock:
            self._dict.pop(key)

    def __contains__(self, key: K) -> bool:
        with self._lock:
            return key in self._dict

    def __len__(self) -> int:
        with self._lock:
            return len(self._dict)

    def __repr__(self) -> str:
        with self._lock:
            return repr(self._dict)

    def __str__(self) -> str:
        with self._lock:
            return str(self._dict)

    def get(self, key: K, default: V | None = None) -> V | None:
        """Returns ``self[key]`` or ``default`` if missing."""
        with self._lock:
            return self._dict.get(key, default)

    def pop(self, key: K, default: V | None = None) -> V | None:
        """Removes ``key`` and returns its value."""
        with self._lock:
            return self._dict.pop(key, default)

    def clear(self) -> None:
        """Removes all items atomically."""
        with self._lock:
            self._dict.clear()

    def items(self) -> ItemsView[K, V]:
        """Returns a snapshot of dictionary items."""
        with self._lock:
            return self._dict.copy().items()


#############################################################################
class InterruptibleSleep:
    """Provides a sleep(timeout) function that can be interrupted by another thread."""

    def __init__(self, lock: threading.Lock | None = None) -> None:
        self._is_stopping: bool = False
        self._lock: threading.Lock = lock if lock is not None else threading.Lock()
        self._condition: threading.Condition = threading.Condition(self._lock)

    def sleep(self, duration_nanos: int) -> bool:
        """Delays the current thread by the given number of nanoseconds; Returns True if the sleep got interrupted."""
        end_time_nanos: int = time.monotonic_ns() + duration_nanos
        with self._lock:
            while not self._is_stopping:
                diff_nanos: int = end_time_nanos - time.monotonic_ns()
                if diff_nanos <= 0:
                    return False
                self._condition.wait(timeout=diff_nanos / 1_000_000_000)  # release, then block until notified or timeout
        return True

    def interrupt(self) -> None:
        """Wakes sleeping threads and makes any future sleep()s a no-op."""
        with self._lock:
            if not self._is_stopping:
                self._is_stopping = True
                self._condition.notify_all()

    def reset(self) -> None:
        """Makes any future sleep()s no longer a no-op."""
        with self._lock:
            self._is_stopping = False


#############################################################################
class SynchronousExecutor(Executor):
    """Executor that runs tasks inline in the calling thread, sequentially."""

    def __init__(self) -> None:
        self._shutdown: bool = False

    def submit(self, fn: Callable[..., R_], /, *args: Any, **kwargs: Any) -> Future[R_]:  # type: ignore[override]
        """Executes `fn(*args, **kwargs)` immediately and returns its Future."""
        future: Future[R_] = Future()
        if self._shutdown:
            raise RuntimeError("cannot schedule new futures after shutdown")
        try:
            result: R_ = fn(*args, **kwargs)
        except BaseException as exc:
            future.set_exception(exc)
        else:
            future.set_result(result)
        return future

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Prevents new submissions; no worker resources to join/cleanup."""
        self._shutdown = True

    @classmethod
    def executor_for(cls, max_workers: int) -> Executor:
        """Factory returning a SyncExecutor if max_workers == 1; else a ThreadPoolExecutor."""
        return cls() if max_workers == 1 else ThreadPoolExecutor(max_workers=max_workers)


#############################################################################
class _XFinally(contextlib.AbstractContextManager):
    """Context manager ensuring cleanup code executes after ``with`` blocks."""

    def __init__(self, cleanup: Callable[[], None]) -> None:
        """Records the callable to run upon exit."""
        self._cleanup = cleanup  # Zero-argument callable executed after the `with` block exits.

    def __exit__(  # type: ignore[exit-return]  # need to ignore on python <= 3.8
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: types.TracebackType | None
    ) -> bool:
        """Runs cleanup and propagate any exceptions appropriately."""
        try:
            self._cleanup()
        except BaseException as cleanup_exc:
            if exc is None:
                raise  # No main error --> propagate cleanup error normally
            # Both failed
            # if sys.version_info >= (3, 11):
            #     raise ExceptionGroup("main error and cleanup error", [exc, cleanup_exc]) from None
            # <= 3.10: attach so it shows up in traceback but doesn't mask
            exc.__context__ = cleanup_exc
            return False  # reraise original exception
        return False  # propagate main exception if any


def xfinally(cleanup: Callable[[], None]) -> _XFinally:
    """Usage: with xfinally(lambda: cleanup()): ...
    Returns a context manager that guarantees that cleanup() runs on exit and guarantees any error in cleanup() will never
    mask an exception raised earlier inside the body of the `with` block, while still surfacing both problems when possible.

    Problem it solves
    -----------------
    A naive ``try ... finally`` may lose the original exception:

        try:
            work()
        finally:
            cleanup()  # <-- if this raises an exception, it replaces the real error!

    `_XFinally` preserves exception priority:

    * Body raises, cleanup succeeds --> original body exception is re-raised.
    * Body raises, cleanup also raises --> re-raises body exception; cleanup exception is linked via ``__context__``.
    * Body succeeds, cleanup raises --> cleanup exception propagates normally.

    Example:
    -------
    >>> with xfinally(reset_logger):   # doctest: +SKIP
    ...     run_tasks()

    The single *with* line replaces verbose ``try/except/finally`` boilerplate while preserving full error information.
    """
    return _XFinally(cleanup)
