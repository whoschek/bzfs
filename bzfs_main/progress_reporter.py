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
"""Thread-based progress monitor for `pv` output during data transfers.

It tails the log files produced by (parallel) ``pv`` processes and periodically prints a single status line showing
aggregated throughput, ETA, etc. The reporter runs in a separate daemon thread to avoid blocking replication. All methods
are designed for minimal synchronization overhead.
"""

from __future__ import (
    annotations,
)
import argparse
import glob
import os
import re
import selectors
import sys
import threading
import time
from collections import (
    deque,
)
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    datetime,
)
from enum import (
    Enum,
    auto,
)
from logging import (
    Logger,
)
from pathlib import (
    Path,
)
from typing import (
    IO,
    Any,
    Final,
    NamedTuple,
)

from bzfs_main.utils import (
    FILE_PERMISSIONS,
    InterruptibleSleep,
    human_readable_bytes,
    open_nofollow,
)

# constants
PV_FILE_THREAD_SEPARATOR: Final[str] = "_"
_ARABIC_DECIMAL_SEPARATOR: Final[str] = "\u066b"  # "٫"  # noqa: RUF003
_PV_SIZE_TO_BYTES_REGEX: Final[re.Pattern[str]] = re.compile(
    rf"(\d+[.,{_ARABIC_DECIMAL_SEPARATOR}]?\d*)\s*([KMGTPEZYRQ]?)(i?)([Bb])(.*)"
)


#############################################################################
class State(Enum):
    """Progress reporter lifecycle state transitions."""

    IS_PAUSING = auto()
    IS_RESETTING = auto()


#############################################################################
class ProgressReporter:
    """Periodically prints progress updates to the same console status line, which is helpful if the program runs in an
    interactive Unix terminal session.

    Tails the 'pv' output log files that are being written to by (parallel) replication,
    and extracts aggregate progress and throughput metrics from them, such as MB, MB/s, ETA, etc. Periodically prints these
    metrics to the console status line (but not to the log file), and in doing so "visually overwrites" the previous status
    line, via appending a \r carriage return control char rather than a \n newline char. Does not print a status line if the
    Unix environment var 'bzfs_isatty' is set to 'false', in order not to confuse programs that scrape redirected stdout.
    Example console status line:
    2025-01-17 01:23:04 [I] zfs sent 41.7 GiB 0:00:46 [963 MiB/s] [907 MiB/s] [==========>  ] 80% ETA 0:00:04 ETA 01:23:08
    """

    def __init__(
        self,
        log: Logger,
        pv_program_opts: list[str],
        use_select: bool,
        progress_update_intervals: tuple[float, float] | None,
        fail: bool = False,
    ) -> None:
        """Creates a reporter configured for ``pv`` log parsing."""
        # immutable variables:
        self._log: Final[Logger] = log
        self._pv_program_opts: Final[list[str]] = pv_program_opts
        self._use_select: Final[bool] = use_select
        self._progress_update_intervals: Final[tuple[float, float] | None] = progress_update_intervals
        self._inject_error: bool = fail  # for testing only

        # mutable variables:
        self._thread: threading.Thread | None = None
        self._exception: BaseException | None = None
        self._lock: Final[threading.Lock] = threading.Lock()
        self._sleeper: Final[InterruptibleSleep] = InterruptibleSleep(self._lock)  # sleeper shares lock with reporter
        self._file_name_queue: set[str] = set()
        self._file_name_set: Final[set[str]] = set()
        self._is_stopping: bool = False
        self._states: list[State] = [State.IS_RESETTING]

    def start(self) -> None:
        """Starts the monitoring thread and begins asynchronous parsing of ``pv`` log files."""
        with self._lock:
            assert self._thread is None
            self._thread = threading.Thread(target=lambda: self._run(), name="progress_reporter", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        """Blocks until reporter is stopped, then reraises any exception that may have happened during log processing."""
        with self._lock:
            self._is_stopping = True
        self._sleeper.interrupt()
        t = self._thread
        if t is not None:
            t.join()
        e = self._exception
        if e is not None:
            raise e  # reraise exception in current thread

    def pause(self) -> None:
        """Temporarily suspends status logging."""
        self._append_state(State.IS_PAUSING)

    def reset(self) -> None:
        """Clears metrics before processing a new batch of logs; the purpose is to discard previous totals to avoid mixing
        unrelated transfers."""
        self._append_state(State.IS_RESETTING)

    def _append_state(self, state: State) -> None:
        with self._lock:
            states: list[State] = self._states
            if len(states) > 0 and states[-1] is state:
                return  # same state twice in a row is a no-op
            states.append(state)
            if len(states) >= 3:
                del states[0]  # cap time and memory consumption by removing redundant state transitions
        self._sleeper.interrupt()

    def enqueue_pv_log_file(self, pv_log_file: str) -> None:
        """Tells progress reporter thread to also monitor and tail the given pv log file."""
        with self._lock:
            if pv_log_file not in self._file_name_set:
                self._file_name_queue.add(pv_log_file)

    def _run(self) -> None:
        """Thread entry point consuming pv logs and updating metrics."""
        log = self._log
        try:
            fds: list[IO[Any]] = []
            try:
                selector = selectors.SelectSelector() if self._use_select else selectors.PollSelector()
                try:
                    self._run_internal(fds, selector)
                finally:
                    selector.close()
            finally:
                for fd in fds:
                    fd.close()
        except BaseException as e:
            if not isinstance(e, BrokenPipeError):
                self._exception = e  # will be reraised in stop()
                log.exception("%s", "ProgressReporter:")

    @dataclass
    class TransferStat:
        """Tracks per-file transfer state and ETA."""

        @dataclass(order=True)
        class ETA:
            """Estimated time of arrival."""

            timestamp_nanos: int  # sorted by future time at which current zfs send/recv transfer is estimated to complete
            seq_nr: int  # tiebreaker wrt. sort order
            line_tail: str = field(compare=False)  # trailing pv log line part w/ progress bar, duration ETA, timestamp ETA

        bytes_in_flight: int
        eta: ETA

    def _run_internal(self, fds: list[IO[Any]], selector: selectors.BaseSelector) -> None:
        """Tails pv log files and periodically logs aggregated progress."""

        class Sample(NamedTuple):
            """Sliding window entry for throughput calculation."""

            sent_bytes: int
            timestamp_nanos: int

        log = self._log
        update_interval_secs, sliding_window_secs = (
            self._progress_update_intervals if self._progress_update_intervals is not None else self._get_update_intervals()
        )
        update_interval_nanos: int = round(update_interval_secs * 1_000_000_000)
        sliding_window_nanos: int = round(sliding_window_secs * 1_000_000_000)
        sleep_nanos: int = 0
        etas: list[ProgressReporter.TransferStat.ETA] = []
        while True:
            empty_file_name_queue: set[str] = set()
            empty_states: list[State] = []
            with self._lock:
                if self._is_stopping:
                    return
                # progress reporter thread picks up pv log files that so far aren't being tailed
                n = len(self._file_name_queue)
                m = len(self._file_name_set)
                self._file_name_set.update(self._file_name_queue)  # union
                assert len(self._file_name_set) == n + m  # aka assert (previous) file_name_set.isdisjoint(file_name_queue)
                local_file_name_queue: set[str] = self._file_name_queue
                self._file_name_queue = empty_file_name_queue  # exchange buffers
                states: list[State] = self._states
                self._states = empty_states  # exchange buffers
            for state in states:
                if state is State.IS_PAUSING:
                    next_update_nanos: int = time.monotonic_ns() + 10 * 365 * 86400 * 1_000_000_000  # infinity
                    sleep_nanos = next_update_nanos
                else:
                    assert state is State.IS_RESETTING
                    sent_bytes, last_status_len = 0, 0
                    num_lines, num_readables = 0, 0
                    start_time_nanos = time.monotonic_ns()
                    next_update_nanos = start_time_nanos + update_interval_nanos
                    sleep_nanos = round(update_interval_nanos / 2.5)
                    latest_samples: deque[Sample] = deque([Sample(0, start_time_nanos)])  # sliding window w/ recent measures
            for pv_log_file in local_file_name_queue:
                try:
                    Path(pv_log_file).touch(mode=FILE_PERMISSIONS)
                    fd = open_nofollow(pv_log_file, mode="r", newline="", encoding="utf-8")
                except FileNotFoundError:  # a third party has somehow deleted the log file or directory
                    with self._lock:
                        self._file_name_set.discard(pv_log_file)  # enable re-adding the file later via enqueue_pv_log_file()
                    log.warning("ProgressReporter: pv log file disappeared before initial open, skipping: %s", pv_log_file)
                    continue  # skip to the next file in the queue
                fds.append(fd)
                eta = self.TransferStat.ETA(timestamp_nanos=0, seq_nr=-len(fds), line_tail="")
                selector.register(fd, selectors.EVENT_READ, data=(iter(fd), self.TransferStat(bytes_in_flight=0, eta=eta)))
                etas.append(eta)
            readables: list[tuple[selectors.SelectorKey, int]] = selector.select(timeout=0)  # 0 indicates "don't block"
            has_line: bool = False
            curr_time_nanos: int = time.monotonic_ns()
            for selector_key, _ in readables:  # for each file that's ready for non-blocking read
                num_readables += 1
                iter_fd, transfer_stat = selector_key.data
                for line in iter_fd:  # aka iter(fd)
                    sent_bytes += self._update_transfer_stat(line, transfer_stat, curr_time_nanos)
                    num_lines += 1
                    has_line = True
            if curr_time_nanos >= next_update_nanos:
                elapsed_nanos: int = curr_time_nanos - start_time_nanos
                msg0, msg3 = self._format_sent_bytes(sent_bytes, elapsed_nanos)  # throughput etc since replication starttime
                msg1: str = self._format_duration(elapsed_nanos)  # duration since replication start time
                oldest: Sample = latest_samples[0]  # throughput etc, over sliding window
                _, msg2 = self._format_sent_bytes(sent_bytes - oldest.sent_bytes, curr_time_nanos - oldest.timestamp_nanos)
                msg4: str = max(etas).line_tail if len(etas) > 0 else ""  # progress bar, ETAs
                timestamp: str = datetime.now().isoformat(sep=" ", timespec="seconds")  # 2024-09-03 12:26:15
                status_line: str = f"{timestamp} [I] zfs sent {msg0} {msg1} {msg2} {msg3} {msg4}"
                status_line = status_line.ljust(last_status_len)  # "overwrite" trailing chars of previous status with spaces

                # The Unix console skips back to the beginning of the console line when it sees this \r control char:
                sys.stdout.write(f"{status_line}\r")
                sys.stdout.flush()

                # log.log(log_trace, "\nnum_lines: %s, num_readables: %s", num_lines, num_readables)
                last_status_len = len(status_line.rstrip())
                next_update_nanos += update_interval_nanos
                latest_samples.append(Sample(sent_bytes, curr_time_nanos))
                if elapsed_nanos >= sliding_window_nanos:
                    latest_samples.popleft()  # slide the sliding window containing recent measurements
            elif not has_line:
                # Avoid burning CPU busily spinning on I/O readiness as fds are almost always ready for non-blocking read
                # even if no new pv log line has been written. Yet retain ability to wake up immediately on reporter.stop().
                if self._sleeper.sleep(min(sleep_nanos, next_update_nanos - curr_time_nanos)):
                    self._sleeper.reset()  # sleep was interrupted; ensure we can sleep normally again
            if self._inject_error:
                raise ValueError("Injected ProgressReporter error")  # for testing only

    def _update_transfer_stat(self, line: str, s: TransferStat, curr_time_nanos: int) -> int:
        """Update ``s`` from one pv status line and return bytes delta."""
        num_bytes, s.eta.timestamp_nanos, s.eta.line_tail = self._parse_pv_line(line, curr_time_nanos)
        bytes_in_flight: int = s.bytes_in_flight
        s.bytes_in_flight = num_bytes if line.endswith("\r") else 0  # intermediate vs. final status update of each transfer
        return num_bytes - bytes_in_flight

    NO_RATES_REGEX = re.compile(r".*/s\s*[)\]]?\s*")  # matches until end of last pv rate, e.g. "834MiB/s]" or "834MiB/s)"
    # time remaining --eta "ETA 00:00:39" or "ETA 2+0:00:39" or "ETA 2:0:00:39", followed by trailing --fineta timestamp ETA
    TIME_REMAINING_ETA_REGEX = re.compile(r".*?ETA\s*((\d+)[+:])?(\d\d?):(\d\d):(\d\d).*(ETA|FIN).*")

    @staticmethod
    def _parse_pv_line(line: str, curr_time_nanos: int) -> tuple[int, int, str]:
        """Parses a pv status line into transferred bytes and ETA timestamp."""
        assert isinstance(line, str)
        if ":" in line:
            line = line.split(":", 1)[1].strip()
            sent_bytes, line = _pv_size_to_bytes(line)
            line = ProgressReporter.NO_RATES_REGEX.sub("", line.lstrip(), count=1)  # strip --timer, --rate, --avg-rate
            if match := ProgressReporter.TIME_REMAINING_ETA_REGEX.fullmatch(line):  # extract pv --eta duration
                _, days, hours, minutes, secs, _ = match.groups()
                time_remaining_secs = (86400 * int(days) if days else 0) + int(hours) * 3600 + int(minutes) * 60 + int(secs)
                curr_time_nanos += time_remaining_secs * 1_000_000_000  # ETA timestamp = now + time remaining duration
            return sent_bytes, curr_time_nanos, line
        return 0, curr_time_nanos, ""

    @staticmethod
    def _format_sent_bytes(num_bytes: int, duration_nanos: int) -> tuple[str, str]:
        """Returns a human-readable byte count and rate."""
        bytes_per_sec: int = round(1_000_000_000 * num_bytes / max(1, duration_nanos))
        return f"{human_readable_bytes(num_bytes, precision=2)}", f"[{human_readable_bytes(bytes_per_sec, precision=2)}/s]"

    @staticmethod
    def _format_duration(duration_nanos: int) -> str:
        """Formats ``duration_nanos`` as HH:MM:SS string."""
        total_seconds: int = duration_nanos // 1_000_000_000
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours}:{minutes:02d}:{seconds:02d}"

    def _get_update_intervals(self) -> tuple[float, float]:
        """Extracts polling intervals from ``pv_program_opts``."""
        parser = argparse.ArgumentParser(allow_abbrev=False)
        parser.add_argument("--interval", "-i", type=float, default=1)
        parser.add_argument("--average-rate-window", "-m", type=float, default=30)
        args, _ = parser.parse_known_args(args=self._pv_program_opts)
        interval: float = min(60 * 60, max(args.interval, 0.1))
        return interval, min(60 * 60, max(args.average_rate_window, interval))


def _pv_size_to_bytes(
    size: str,
) -> tuple[int, str]:  # example inputs: "800B", "4.12 KiB", "510 MiB", "510 MB", "4Gb", "2TiB"
    """Converts pv size string to bytes and returns remaining text."""
    if match := _PV_SIZE_TO_BYTES_REGEX.fullmatch(size):
        number: float = float(match.group(1).replace(",", ".").replace(_ARABIC_DECIMAL_SEPARATOR, "."))
        i: int = "KMGTPEZYRQ".index(match.group(2)) if match.group(2) else -1
        m: int = 1024 if match.group(3) == "i" else 1000
        b: int = 1 if match.group(4) == "B" else 8
        line_tail: str = match.group(5)
        if line_tail and line_tail.startswith("/s"):
            raise ValueError("Invalid pv_size: " + size)  # stems from 'pv --rate' or 'pv --average-rate'
        size_in_bytes: int = round(number * (m ** (i + 1)) / b)
        return size_in_bytes, line_tail
    else:
        return 0, ""  # skip partial or bad 'pv' log file line (pv process killed while writing?)


def count_num_bytes_transferred_by_zfs_send(basis_pv_log_file: str) -> int:
    """Scrapes the .pv log file(s) and sums up the 'pv --bytes' column."""

    def parse_pv_line(line: str) -> int:
        """Extracts byte count from a single pv log line."""
        if ":" in line:
            col: str = line.split(":", 1)[1].strip()
            num_bytes, _ = _pv_size_to_bytes(col)
            return num_bytes
        return 0

    total_bytes: int = 0
    files: list[str] = [basis_pv_log_file] + glob.glob(basis_pv_log_file + PV_FILE_THREAD_SEPARATOR + "[0-9]*")
    for file in files:
        if os.path.isfile(file):
            try:
                with open_nofollow(file, mode="r", newline="", encoding="utf-8") as fd:
                    line: str | None = None
                    for line in fd:
                        assert line is not None
                        if line.endswith("\r"):
                            continue  # skip all but the most recent status update of each transfer
                        total_bytes += parse_pv_line(line)
                        line = None
                    if line is not None:
                        total_bytes += parse_pv_line(line)  # consume last line of file w/ intermediate status update, if any
            except FileNotFoundError:
                pass  # harmless
    return total_bytes
