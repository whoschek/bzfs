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
import bisect
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
from collections import defaultdict, deque
from subprocess import DEVNULL, PIPE
from typing import (
    IO,
    Any,
    Callable,
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
prog_name = "bzfs"
env_var_prefix = prog_name + "_"
die_status = 3
descendants_re_suffix = r"(?:/.*)?"  # also match descendants of a matching dataset
log_stderr = (logging.INFO + logging.WARNING) // 2  # custom log level is halfway in between
log_stdout = (log_stderr + logging.INFO) // 2  # custom log level is halfway in between
log_debug = logging.DEBUG
log_trace = logging.DEBUG // 2  # custom log level is halfway in between
unixtime_infinity_secs = 2**64  # billions of years in the future and to be extra safe, larger than the largest ZFS GUID
DONT_SKIP_DATASET = ""

RegexList = List[Tuple[re.Pattern, bool]]  # Type alias


def getenv_any(key: str, default: str | None = None) -> str | None:
    """All shell environment variable names used for configuration start with this prefix."""
    return os.getenv(env_var_prefix + key, default)


def getenv_int(key: str, default: int) -> int:
    return int(cast(str, getenv_any(key, str(default))))


def getenv_bool(key: str, default: bool = False) -> bool:
    return cast(str, getenv_any(key, str(default))).lower().strip().lower() == "true"


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
    deque(iterable, maxlen=0)


K_ = TypeVar("K_")
V_ = TypeVar("V_")


def shuffle_dict(dictionary: dict[K_, V_]) -> dict[K_, V_]:
    items = list(dictionary.items())
    random.shuffle(items)
    return dict(items)


def sorted_dict(dictionary: dict[K_, V_]) -> dict[K_, V_]:
    return dict(sorted(dictionary.items()))


def tail(file: str, n: int, errors: str | None = None) -> Sequence[str]:
    if not os.path.isfile(file):
        return []
    with open_nofollow(file, "r", encoding="utf-8", errors=errors, check_owner=False) as fd:
        return deque(fd, maxlen=n)


def replace_capturing_groups_with_non_capturing_groups(regex: str) -> str:
    """Replaces regex capturing groups with non-capturing groups for better matching performance.
    Example: '(.*/)?tmp(foo|bar)(?!public)\\(' --> '(?:.*/)?tmp(?:foo|bar)(?!public)\\()'
    Aka replaces brace '(' followed by a char other than question mark '?', but not preceded by a backslash
    with the replacement string '(?:'
    Also see https://docs.python.org/3/howto/regex.html#non-capturing-and-named-groups"""
    # pattern = re.compile(r'(?<!\\)\((?!\?)')
    # return pattern.sub('(?:', regex)
    i = len(regex) - 2
    while i >= 0:
        i = regex.rfind("(", 0, i + 1)
        if i >= 0 and regex[i] == "(" and (regex[i + 1] != "?") and (i == 0 or regex[i - 1] != "\\"):
            regex = f"{regex[0:i]}(?:{regex[i + 1:]}"
        i -= 1
    return regex


def get_home_directory() -> str:
    """Reliably detects home dir without using HOME env var."""
    # thread-safe version of: os.environ.pop('HOME', None); os.path.expanduser('~')
    return pwd.getpwuid(os.getuid()).pw_dir


def human_readable_bytes(num_bytes: float, separator: str = " ", precision: int | None = None, long: bool = False) -> str:
    sign = "-" if num_bytes < 0 else ""
    s = abs(num_bytes)
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB", "RiB", "QiB")
    n = len(units) - 1
    i = 0
    long_form = f" ({num_bytes} bytes)" if long else ""
    while s >= 1024 and i < n:
        s /= 1024
        i += 1
    formatted_num = human_readable_float(s) if precision is None else f"{s:.{precision}f}"
    return f"{sign}{formatted_num}{separator}{units[i]}{long_form}"


def human_readable_duration(
    duration: float, unit: str = "ns", separator: str = "", precision: int | None = None, long: bool = False
) -> str:
    sign = "-" if duration < 0 else ""
    t = abs(duration)
    units = ("ns", "μs", "ms", "s", "m", "h", "d")
    nanos = (1, 1_000, 1_000_000, 1_000_000_000, 60 * 1_000_000_000, 60 * 60 * 1_000_000_000, 60 * 60 * 24 * 1_000_000_000)
    i = units.index(unit)
    long_form = f" ({round(duration * nanos[i] / 1_000_000_000)} seconds)" if long else ""
    if t < 1 and t != 0:
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
    return f"{sign}{formatted_num}{separator}{units[i]}{long_form}"


def human_readable_float(number: float) -> str:
    """If the number has one digit before the decimal point (0 <= abs(number) < 10):
      Round and use two decimals after the decimal point (e.g., 3.14559 --> "3.15").

    If the number has two digits before the decimal point (10 <= abs(number) < 100):
      Round and use one decimal after the decimal point (e.g., 12.36 --> "12.4").

    If the number has three or more digits before the decimal point (abs(number) >= 100):
      Round and use zero decimals after the decimal point (e.g., 123.556 --> "124").

    Ensure no unnecessary trailing zeroes are retained: Example: 1.500 --> "1.5", 1.00 --> "1"
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
    return f"{number}={'NaN' if total == 0 else human_readable_float(100 * number / total)}%"


def open_nofollow(
    path: str,
    mode: str = "r",
    buffering: int = -1,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    *,
    perm: int = stat.S_IRUSR | stat.S_IWUSR,  # rw------- (owner read + write)
    check_owner: bool = True,
    **kwargs: Any,
) -> IO:
    """Behaves exactly like built-in open(), except that it refuses to follow symlinks, i.e. raises OSError with
    errno.ELOOP/EMLINK if basename of path is a symlink. Also, can specify permissions on O_CREAT, and verify ownership."""
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
            st_uid = os.fstat(fd).st_uid
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
    predicate condition. If no matching item is found returns -1 or ValueError, depending on the raises parameter,
    which is a bool indicating whether to raise an error, or a string containing the error message, but can also be a
    Callable/lambda in order to support efficient deferred generation of error messages.
    Analog to str.find(), including slicing semantics with parameters start and end.
    For example, seq can be a list, tuple or str.

    Example usage:
        lst = ["a", "b", "-c", "d"]
        i = find_match(lst, lambda arg: arg.startswith("-"), start=1, end=3, reverse=True)
        if i >= 0:
            ...
        i = find_match(lst, lambda arg: arg.startswith("-"), raises=f"Tag {tag} not found in {file}")
        i = find_match(lst, lambda arg: arg.startswith("-"), raises=lambda: f"Tag {tag} not found in {file}")
    """
    offset = 0 if start is None else start if start >= 0 else len(seq) + start
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
    return (dataset + "/").startswith(of_root_dataset + "/")


def has_duplicates(sorted_list: list[Any]) -> bool:
    """Returns True if any adjacent items within the given sorted sequence are equal."""
    return any(a == b for a, b in zip(sorted_list, sorted_list[1:]))


def dry(msg: str, is_dry_run: bool) -> str:
    return "Dry " + msg if is_dry_run else msg


def relativize_dataset(dataset: str, root_dataset: str) -> str:
    """Converts an absolute dataset path to a relative dataset path wrt root_dataset
    Example: root_dataset=tank/foo, dataset=tank/foo/bar/baz --> relative_path=/bar/baz"""
    return dataset[len(root_dataset) :]


def replace_prefix(s: str, old_prefix: str, new_prefix: str) -> str:
    """In a string s, replaces a leading old_prefix string with new_prefix. Assumes the leading string is present."""
    assert s.startswith(old_prefix)
    return new_prefix + s[len(old_prefix) :]


def replace_in_lines(lines: list[str], old: str, new: str, count: int = -1) -> None:
    for i in range(len(lines)):
        lines[i] = lines[i].replace(old, new, count)


def is_included(name: str, include_regexes: RegexList, exclude_regexes: RegexList) -> bool:
    """Returns True if the name matches at least one of the include regexes but none of the exclude regexes; else False.
    A regex that starts with a `!` is a negation - the regex matches if the regex without the `!` prefix does not match."""
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
    assert isinstance(regexes, list)
    compiled_regexes = []
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
    # For lazy/noop evaluation in disabled log levels
    class CustomListFormatter:
        def __str__(self) -> str:
            s = separator.join(map(str, iterable))
            return s.lstrip() if lstrip else s

    return CustomListFormatter()


def pretty_print_formatter(obj_to_format: Any) -> Any:  # For lazy/noop evaluation in disabled log levels
    class PrettyPrintFormatter:
        def __str__(self) -> str:
            import pprint

            return pprint.pformat(vars(obj_to_format))

    return PrettyPrintFormatter()


def stderr_to_str(stderr: Any) -> str:
    """Workaround for https://github.com/python/cpython/issues/87597"""
    return str(stderr) if not isinstance(stderr, bytes) else stderr.decode("utf-8")


def xprint(log: logging.Logger, value: Any, run: bool = True, end: str = "\n", file: TextIO | None = None) -> None:
    if run and value:
        value = value if end else str(value).rstrip()
        level = log_stdout if file is sys.stdout else log_stderr
        log.log(level, "%s", value)


def die(msg: str, exit_code: int = die_status, parser: argparse.ArgumentParser | None = None) -> NoReturn:
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
    """Sends signal also to descendant processes to also terminate processes started via subprocess.run()"""
    current_pid = os.getpid()
    root_pid = current_pid if root_pid is None else root_pid
    pids = get_descendant_processes(root_pid)
    if root_pid == current_pid:
        pids += [] if except_current_process else [current_pid]
    else:
        pids.insert(0, root_pid)
    for pid in pids:
        with contextlib.suppress(OSError):
            os.kill(pid, sig)


def get_descendant_processes(root_pid: int) -> list[int]:
    """Returns the list of all descendant process IDs for the given root PID, on Unix systems."""
    procs = defaultdict(list)
    cmd = ["ps", "-Ao", "pid,ppid"]
    lines = subprocess.run(cmd, stdin=DEVNULL, stdout=PIPE, text=True, check=True).stdout.splitlines()
    for line in lines[1:]:  # all lines except the header line
        splits = line.split()
        assert len(splits) == 2
        pid = int(splits[0])
        ppid = int(splits[1])
        procs[ppid].append(pid)
    descendants: list[int] = []

    def recursive_append(ppid: int) -> None:
        for child_pid in procs[ppid]:
            descendants.append(child_pid)
            recursive_append(child_pid)

    recursive_append(root_pid)
    return descendants


def pid_exists(pid: int) -> bool | None:
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


#############################################################################
class Comparable(Protocol):
    def __lt__(self, other: Any) -> bool:  # pragma: no cover - behavior defined by implementor
        ...


T = TypeVar("T", bound=Comparable)  # Generic type variable for elements stored in a SmallPriorityQueue


class SmallPriorityQueue(Generic[T]):
    """A priority queue that can handle updates to the priority of any element that is already contained in the queue, and
    does so very efficiently if there are a small number of elements in the queue (no more than thousands), as is the case
    for us. Could be implemented using a SortedList via https://github.com/grantjenks/python-sortedcontainers or using an
    indexed priority queue via https://github.com/nvictus/pqdict but, to avoid an external dependency, is actually
    implemented using a simple yet effective binary search-based sorted list that can handle updates to the priority of
    elements that are already contained in the queue, via removal of the element, followed by update of the element, followed
    by (re)insertion. Duplicate elements (if any) are maintained in their order of insertion relative to other duplicates."""

    def __init__(self, reverse: bool = False) -> None:
        self._lst: list[T] = []
        self._reverse: bool = reverse

    def clear(self) -> None:
        self._lst.clear()

    def push(self, element: T) -> None:
        bisect.insort(self._lst, element)

    def pop(self) -> T:
        """Removes and return the smallest (or largest if reverse == True) element from the queue."""
        return self._lst.pop() if self._reverse else self._lst.pop(0)

    def peek(self) -> T:
        """Returns the smallest (or largest if reverse == True) element without removing it."""
        return self._lst[-1] if self._reverse else self._lst[0]

    def remove(self, element: T) -> bool:
        """Removes the first occurrence of the specified element from the queue; returns True if the element was contained"""
        lst = self._lst
        i = bisect.bisect_left(lst, element)
        is_contained = i < len(lst) and lst[i] == element
        if is_contained:
            del lst[i]  # is an optimized memmove()
        return is_contained

    def __len__(self) -> int:
        return len(self._lst)

    def __contains__(self, element: T) -> bool:
        lst = self._lst
        i = bisect.bisect_left(lst, element)
        return i < len(lst) and lst[i] == element

    def __iter__(self) -> Iterator[T]:
        return reversed(self._lst) if self._reverse else iter(self._lst)

    def __repr__(self) -> str:
        return repr(list(reversed(self._lst))) if self._reverse else repr(self._lst)


#############################################################################
class SynchronizedBool:
    """Thread-safe bool."""

    def __init__(self, val: bool) -> None:
        assert isinstance(val, bool)
        self._lock: threading.Lock = threading.Lock()
        self._value: bool = val

    @property
    def value(self) -> bool:
        with self._lock:
            return self._value

    @value.setter
    def value(self, new_value: bool) -> None:
        with self._lock:
            self._value = new_value

    def get_and_set(self, new_value: bool) -> bool:
        with self._lock:
            old_value = self._value
            self._value = new_value
            return old_value

    def compare_and_set(self, expected_value: bool, new_value: bool) -> bool:
        with self._lock:
            eq = self._value == expected_value
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
    """Thread-safe dict."""

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
        with self._lock:
            return self._dict.get(key, default)

    def pop(self, key: K, default: V | None = None) -> V | None:
        with self._lock:
            return self._dict.pop(key, default)

    def clear(self) -> None:
        with self._lock:
            self._dict.clear()

    def items(self) -> ItemsView[K, V]:
        with self._lock:
            return self._dict.copy().items()


#############################################################################
class InterruptibleSleep:
    """Provides a sleep(timeout) function that can be interrupted by another thread."""

    def __init__(self, lock: threading.Lock | None = None) -> None:
        self.is_stopping: bool = False
        self._lock: threading.Lock = lock if lock is not None else threading.Lock()
        self._condition: threading.Condition = threading.Condition(self._lock)

    def sleep(self, duration_nanos: int) -> None:
        """Delays the current thread by the given number of nanoseconds."""
        end_time_nanos = time.monotonic_ns() + duration_nanos
        with self._lock:
            while not self.is_stopping:
                diff_nanos = end_time_nanos - time.monotonic_ns()
                if diff_nanos <= 0:
                    return
                self._condition.wait(timeout=diff_nanos / 1_000_000_000)  # release, then block until notified or timeout

    def interrupt(self) -> None:
        """Wakes up currently sleeping threads and makes any future sleep()s a noop."""
        with self._lock:
            if not self.is_stopping:
                self.is_stopping = True
                self._condition.notify_all()


#############################################################################
class _XFinally(contextlib.AbstractContextManager):
    def __init__(self, cleanup: Callable[[], None]) -> None:
        self._cleanup = cleanup  # Zero-argument callable executed after the `with` block exits.

    def __exit__(  # type: ignore[exit-return]  # need to ignore on python <= 3.8
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: types.TracebackType | None
    ) -> bool:
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
