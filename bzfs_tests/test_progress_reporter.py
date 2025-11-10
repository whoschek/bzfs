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
"""Unit tests for the progress reporting utilities during replication; Verifies parsing and display of streaming subprocess
output."""

from __future__ import (
    annotations,
)
import io
import os
import selectors
import shutil
import subprocess
import tempfile
import time
import types
import unittest
from logging import (
    Logger,
)
from pathlib import (
    Path,
)
from typing import (
    IO,
    Any,
    cast,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from bzfs_main import (
    bzfs,
    progress_reporter,
)
from bzfs_main.configuration import (
    LogParams,
)
from bzfs_main.loggers import (
    get_logger,
    reset_logger,
)
from bzfs_main.progress_reporter import (
    PV_FILE_THREAD_SEPARATOR,
    ProgressReporter,
    State,
    count_num_bytes_transferred_by_zfs_send,
)
from bzfs_main.utils import (
    tail,
)
from bzfs_tests.tools import (
    capture_stdout,
    stop_on_failure_subtest,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestHelperFunctions,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestHelperFunctions(unittest.TestCase):

    def setUp(self) -> None:
        self.default_opts = bzfs.argument_parser().get_default("pv_program_opts")

    def test_pv_size_to_bytes(self) -> None:
        def pv_size_to_bytes(line: str) -> int:
            num_bytes, _ = progress_reporter._pv_size_to_bytes(line)
            return num_bytes

        self.assertEqual(800, pv_size_to_bytes("800B foo"))
        self.assertEqual(1, pv_size_to_bytes("8b"))
        self.assertEqual(round(4.12 * 1024), pv_size_to_bytes("4.12 KiB"))
        self.assertEqual(round(46.2 * 1024**3), pv_size_to_bytes("46,2GiB"))
        self.assertEqual(round(46.2 * 1024**3), pv_size_to_bytes("46.2GiB"))
        self.assertEqual(
            round(46.2 * 1024**3), pv_size_to_bytes("46" + progress_reporter._ARABIC_DECIMAL_SEPARATOR + "2GiB")
        )
        self.assertEqual(2 * 1024**2, pv_size_to_bytes("2 MiB"))
        self.assertEqual(1000**2, pv_size_to_bytes("1 MB"))
        self.assertEqual(1024**3, pv_size_to_bytes("1 GiB"))
        self.assertEqual(1024**3, pv_size_to_bytes("8 Gib"))
        self.assertEqual(1000**3, pv_size_to_bytes("8 Gb"))
        self.assertEqual(1024**4, pv_size_to_bytes("1 TiB"))
        self.assertEqual(1024**5, pv_size_to_bytes("1 PiB"))
        self.assertEqual(1024**6, pv_size_to_bytes("1 EiB"))
        self.assertEqual(1024**7, pv_size_to_bytes("1 ZiB"))
        self.assertEqual(1024**8, pv_size_to_bytes("1 YiB"))
        self.assertEqual(1024**9, pv_size_to_bytes("1 RiB"))
        self.assertEqual(1024**10, pv_size_to_bytes("1 QiB"))

        self.assertEqual(0, pv_size_to_bytes("foo"))
        self.assertEqual(0, pv_size_to_bytes("46-2GiB"))
        with self.assertRaises(ValueError):
            pv_size_to_bytes("4.12 KiB/s")

    def test_count_num_bytes_transferred_by_pv(self) -> None:
        self.assertTrue(isinstance(self.default_opts, str) and self.default_opts)
        pv_fd, pv_file = tempfile.mkstemp(prefix="test_bzfs.test_count_num_byte", suffix=".pv")
        os.close(pv_fd)
        cmd = (
            "dd if=/dev/zero bs=1k count=16 |"
            f" LC_ALL=C pv {self.default_opts} --size 16K --name=275GiB --force 2> {pv_file} >/dev/null"
        )
        try:
            subprocess.run(cmd, shell=True, check=True)
            num_bytes = count_num_bytes_transferred_by_zfs_send(pv_file)
            # print("pv_log_file: " + "\n".join(tail(pv_file, 10)))
            self.assertTrue(tail(pv_file, 10)[-1].lstrip().startswith("275GiB: 16.0KiB"))
            self.assertEqual(16 * 1024, num_bytes)
        finally:
            Path(pv_file).unlink(missing_ok=True)

    def test_count_num_bytes_transferred_by_zfs_send(self) -> None:
        self.assertEqual(0, count_num_bytes_transferred_by_zfs_send("/tmp/nonexisting_bzfs_test_file"))
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                pv1_fd, pv1 = tempfile.mkstemp(prefix="test_bzfs.test_count_num_byte", suffix=".pv")
                os.write(pv1_fd, ": 800B foo".encode("utf-8"))
                pv2 = pv1 + PV_FILE_THREAD_SEPARATOR + "001"
                with open(pv2, "w", encoding="utf-8") as fd:
                    fd.write("1 KB\r\n")
                    fd.write("2 KiB:20 KB\r")
                    fd.write("3 KiB:300 KB\r\n")
                    fd.write("4 KiB:4000 KB\r")
                    fd.write("4 KiB:50000 KB\r")
                    fd.write("4 KiB:600000 KB\r" + ("\n" if i == 0 else ""))
                pv3 = pv1 + PV_FILE_THREAD_SEPARATOR + "002"
                os.makedirs(pv3, exist_ok=True)
                try:
                    num_bytes = count_num_bytes_transferred_by_zfs_send(pv1)
                    self.assertEqual(800 + 300 * 1000 + 600000 * 1000, num_bytes)
                finally:
                    os.remove(pv1)
                    os.remove(pv2)
                    shutil.rmtree(pv3)

    def test_progress_reporter_parse_pv_line(self) -> None:
        reporter = ProgressReporter(
            MagicMock(spec=Logger), self.default_opts, use_select=False, progress_update_intervals=None
        )
        curr_time_nanos = 123
        eols = ["", "\n", "\r", "\r\n"]
        for eol in eols:
            with stop_on_failure_subtest(i=eols.index(eol)):
                # normal intermediate line
                line = "125 GiB: 2,71GiB 0:00:08 [98,8MiB/s] [ 341MiB/s] [>                ]  2% ETA 0:06:03 ETA 17:27:49"
                num_bytes, eta_timestamp_nanos, line_tail = reporter._parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(2.71 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), eta_timestamp_nanos)
                self.assertEqual("[>                ]  2% ETA 0:06:03 ETA 17:27:49", line_tail)

                # intermediate line with duration ETA that contains days
                line = "98 GiB/ 0 B/  98 GiB: 93.1GiB 0:12:12 [ 185MiB/s] [ 130MiB/s] [==>  ] 94% ETA 2+0:00:39 ETA 17:55:48"
                num_bytes, eta_timestamp_nanos, line_tail = reporter._parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(93.1 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (2 * 86400 + 39), eta_timestamp_nanos)
                self.assertEqual("[==>  ] 94% ETA 2+0:00:39 ETA 17:55:48", line_tail)

                # intermediate line with duration ETA that contains other days syntax and timestamp ETA that contains days
                line = "98 GiB/ 0 B/  98 GiB: 93.1GiB 0:12:12 ( 185MiB/s) ( 130MiB/s) [==>  ] ETA 1:00:07:16 ETA 2025-01-23 14:06:02"
                num_bytes, eta_timestamp_nanos, line_tail = reporter._parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(93.1 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (1 * 86400 + 7 * 60 + 16), eta_timestamp_nanos)
                self.assertEqual("[==>  ] ETA 1:00:07:16 ETA 2025-01-23 14:06:02", line_tail)

                # intermediate line with duration ETA that contains other days syntax and timestamp ETA using FIN marker
                line = "98 GiB/ 0 B/  98 GiB: 93.1GiB 9:0:12:12 [ 185MiB/s] [ 130MiB/s] [==>  ] ETA 1:00:07:16 FIN 2025-01-23 14:06:02"
                num_bytes, eta_timestamp_nanos, line_tail = reporter._parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(93.1 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (1 * 86400 + 7 * 60 + 16), eta_timestamp_nanos)
                self.assertEqual("[==>  ] ETA 1:00:07:16 FIN 2025-01-23 14:06:02", line_tail)

                # final line on transfer completion does not contain duration ETA
                line = "98 GiB/ 0 B/  98 GiB: 98,1GiB 0:12:39 [ 132MiB/s] [ 132MiB/s] [=====>] 100%             ETA 17:55:37"
                num_bytes, eta_timestamp_nanos, line_tail = reporter._parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(98.1 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos, eta_timestamp_nanos)
                self.assertEqual("[=====>] 100%             ETA 17:55:37", line_tail)

                # final line on transfer completion does not contain duration ETA
                line = "12.6KiB: 44.2KiB 0:00:00 [3.14MiB/s] [3.14MiB/s] [===================] 350%             ETA 14:48:27"
                num_bytes, eta_timestamp_nanos, line_tail = reporter._parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(44.2 * 1024), num_bytes)
                self.assertEqual(curr_time_nanos, eta_timestamp_nanos)
                self.assertEqual("[===================] 350%             ETA 14:48:27", line_tail)

                # missing from --pv--program-opts: --timer, --rate, --average-rate
                line = "125 GiB: 2.71GiB[ >                    ]  2% ETA 0:06:03 ETA 17:27:49"
                num_bytes, eta_timestamp_nanos, line_tail = reporter._parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(2.71 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), eta_timestamp_nanos)
                self.assertEqual("[ >                    ]  2% ETA 0:06:03 ETA 17:27:49", line_tail)

                # missing from --pv--program-opts: --rate, --average-rate
                line = "125 GiB: 2.71GiB 0:00:08 [ >                    ]  2% ETA 0:06:03 ETA 17:27:49"
                num_bytes, eta_timestamp_nanos, line_tail = reporter._parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(2.71 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), eta_timestamp_nanos)
                self.assertEqual("0:00:08 [ >                    ]  2% ETA 0:06:03 ETA 17:27:49", line_tail)

                # intermediate line with square brackets after the first ETA (not sure if this actually occurs in the wild)
                line = "125 GiB: 2,71GiB 0:00:08 [98,8MiB/s] [ 341MiB/s] [>            ]  2% ETA 0:06:03 ] [ ETA 17:27:49]"
                num_bytes, eta_timestamp_nanos, line_tail = reporter._parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(2.71 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), eta_timestamp_nanos)
                self.assertEqual("[>            ]  2% ETA 0:06:03 ] [ ETA 17:27:49]", line_tail)

                # zero line with final line on transfer completion does not contain duration ETA
                line = "275GiB: 0.00 B 0:00:00 [0.00 B/s] [0.00 B/s] [>                   ]  0%             ETA 03:24:32"
                num_bytes, eta_timestamp_nanos, line_tail = reporter._parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(0, num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * 0, eta_timestamp_nanos)
                self.assertEqual("[>                   ]  0%             ETA 03:24:32", line_tail)

                for line in eols:
                    num_bytes, eta_timestamp_nanos, line_tail = reporter._parse_pv_line(line + eol, curr_time_nanos)
                    self.assertEqual(0, num_bytes)
                    self.assertEqual(curr_time_nanos, eta_timestamp_nanos)
                    self.assertEqual("", line_tail)

    def test_progress_reporter_update_transfer_stat(self) -> None:
        curr_time_nanos = 123
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                reporter = ProgressReporter(
                    MagicMock(spec=Logger), self.default_opts, use_select=False, progress_update_intervals=None
                )
                eta = ProgressReporter.TransferStat.ETA(timestamp_nanos=0, seq_nr=0, line_tail="")
                bytes_in_flight = 789
                stat = ProgressReporter.TransferStat(bytes_in_flight=bytes_in_flight, eta=eta)
                line = "125 GiB: 2,71GiB 0:00:08 [98,8MiB/s] [ 341MiB/s] [>                   ]  2% ETA 0:06:03 ETA 17:27:49"
                expected_bytes = round(2.71 * 1024**3)
                if i > 0:
                    line = line + "\r"
                num_bytes = reporter._update_transfer_stat(line, stat, curr_time_nanos)
                if i == 0:
                    self.assertEqual(expected_bytes - bytes_in_flight, num_bytes)
                    self.assertEqual(0, stat.bytes_in_flight)
                else:
                    self.assertEqual(expected_bytes - bytes_in_flight, num_bytes)
                    self.assertEqual(expected_bytes, stat.bytes_in_flight)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), stat.eta.timestamp_nanos)
                self.assertEqual("[>                   ]  2% ETA 0:06:03 ETA 17:27:49", stat.eta.line_tail)

    def test_progress_reporter_stop(self) -> None:
        reporter = ProgressReporter(
            MagicMock(spec=Logger), self.default_opts, use_select=False, progress_update_intervals=None
        )
        reporter.stop()
        reporter.stop()  # test stopping more than once is ok
        reporter._exception = ValueError()
        with self.assertRaises(ValueError):
            reporter.stop()
        self.assertIsInstance(reporter._exception, ValueError)
        with self.assertRaises(ValueError):
            reporter.stop()
        self.assertIsInstance(reporter._exception, ValueError)

        reporter = ProgressReporter(
            MagicMock(spec=Logger), [], use_select=False, progress_update_intervals=(0.01, 0.02), fail=True
        )
        reporter._run()
        self.assertIsInstance(reporter._exception, ValueError)
        with self.assertRaises(ValueError):
            reporter.stop()
        self.assertIsInstance(reporter._exception, ValueError)
        with self.assertRaises(ValueError):
            reporter.stop()
        self.assertIsInstance(reporter._exception, ValueError)

    def test_progress_reporter_exhausted_iterator_sees_appended_data(self) -> None:
        # Create a temporary file. We will keep the file descriptor from mkstemp open for the read iterator
        temp_file_fd_write, temp_file_path = tempfile.mkstemp(prefix="test_iterator_behavior_")
        try:
            # Write initial lines to the file using the write file descriptor
            initial_lines = ["line1\n", "line2\n"]
            with open(temp_file_fd_write, "w", encoding="utf-8") as fd_write:
                fd_write.writelines(initial_lines)
                # fd_write is closed here, but temp_file_path is still valid.

            # Open the file for reading and keep this file descriptor (fd_read) open for the duration of the iterator's life.
            with open(temp_file_path, "r", encoding="utf-8") as fd_read:  # fd_read will remain open
                iter_fd = iter(fd_read)  # Create the iterator on the open fd_read

                # Consume all lines from iter_fd
                read_lines_initial = []
                for line in iter_fd:
                    read_lines_initial.append(line)
                self.assertEqual(read_lines_initial, initial_lines, "Initial lines not read correctly.")

                # Verify that the iterator is indeed exhausted (while fd_read is still open)
                with self.assertRaises(StopIteration, msg="Iterator should be exhausted after reading all initial lines."):
                    next(iter_fd)

                # Append new lines to the *same underlying file path*.
                # The original fd_read is still open and iter_fd is tied to it.
                appended_lines = ["line3_appended\n", "line4_appended\n"]
                # Open for append - this is a *different* file descriptor instance, but it operates on the same file on disk.
                with open(temp_file_path, "a", encoding="utf-8") as f_append:
                    for line in appended_lines:
                        f_append.write(line)

                # Attempt to read from the original, exhausted iter_fd again.
                # fd_read (to which iter_fd is bound) is still open.
                read_lines_after_append_from_original_iterator = []
                # This loop should yield additional lines even though 'iter_fd' was previously exhausted.
                for line in iter_fd:  # Attempting to iterate over the *exhausted* iterator
                    read_lines_after_append_from_original_iterator.append(line)

                self.assertEqual(
                    ["line3_appended\n", "line4_appended\n"],
                    read_lines_after_append_from_original_iterator,
                    "Exhausted iterator (on an still-open file) should yiels new lines even if the underlying file "
                    "was appended to.",
                )
        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    def test_progress_reporter_state_with_reused_pv_file(self) -> None:
        # We will manually call the reporter's update method to simulate its internal loop.
        # mock_params = MagicMock(spec=bzfs.Params)
        reporter = ProgressReporter(MagicMock(spec=Logger), [], use_select=False, progress_update_intervals=(0.01, 0.02))

        # Simulate ProgressReporter opening the file (it does this internally based on enqueue), For this test, we'll
        # manually manage a TransferStat object, as the reporter would. The key is that if the filename is the same,
        # the *same* TransferStat object associated with that selector key would be reused.

        # Create a TransferStat as the reporter would associate with pv_log_file_base
        eta_dummy = reporter.TransferStat.ETA(timestamp_nanos=0, seq_nr=0, line_tail="")
        stat_obj_for_pv_log_file = reporter.TransferStat(bytes_in_flight=0, eta=eta_dummy)
        current_time_ns = time.monotonic_ns()

        # Simulate First PV Operation
        # Format: "TOTAL_KNOWN_SIZE: CURRENT_BYTES OTHER_PV_STATS\r"
        op1_lines = [
            "100KiB: 10KiB [=>   ] 10%\r",  # 10KB transferred
            "100KiB: 50KiB [==>  ] 50%\r",  # 50KB transferred
            "100KiB: 100KiB [====>] 100%\n",  # 100KB transferred (final line)
        ]
        total_bytes_op1 = 0
        for line in op1_lines:
            delta_bytes = reporter._update_transfer_stat(line, stat_obj_for_pv_log_file, current_time_ns)
            total_bytes_op1 += delta_bytes
            current_time_ns += 1000  # Advance time slightly
        self.assertEqual(100 * 1024, total_bytes_op1, "Total bytes for Op1 incorrect")
        # After op1 final line (ends with \n), bytes_in_flight in stat_obj_for_pv_log_file should be 0
        self.assertEqual(0, stat_obj_for_pv_log_file.bytes_in_flight, "bytes_in_flight should be 0 after final line of Op1")

        # Simulate Second PV Operation (reusing the same pv_log_file_base name)
        # If the pv_log_file name is not unique per op, the reporter continues to use the *same* stat_obj_for_pv_log_file
        op2_lines = [
            "200KiB: 20KiB [=>   ] 10%\r",  # 20KB transferred for THIS operation
            "200KiB: 100KiB [==>  ] 50%\r",  # 100KB transferred for THIS operation
            "200KiB: 200KiB [====>] 100%\n",  # 200KB transferred for THIS operation (final line)
        ]
        total_bytes_op2 = 0
        for line in op2_lines:
            # CRITICAL: We are passing the SAME stat_obj_for_pv_log_file
            delta_bytes = reporter._update_transfer_stat(line, stat_obj_for_pv_log_file, current_time_ns)
            total_bytes_op2 += delta_bytes
            current_time_ns += 1000
        self.assertEqual(200 * 1024, total_bytes_op2, "Total bytes for Op2 incorrect")
        self.assertEqual(0, stat_obj_for_pv_log_file.bytes_in_flight, "bytes_in_flight should be 0 after final line of Op2")

    def test_reporter_handles_pv_log_file_disappearing_before_initial_open(self) -> None:
        mock_log = MagicMock(spec=Logger)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, temp_dir)
        pv_log_file = os.path.join(temp_dir, "test.pv")
        reporter = ProgressReporter(mock_log, [], use_select=False, progress_update_intervals=None)
        self.assertSetEqual(set(), reporter._file_name_set)
        self.assertSetEqual(set(), reporter._file_name_queue)
        reporter.enqueue_pv_log_file(pv_log_file)
        self.assertSetEqual(set(), reporter._file_name_set)
        self.assertSetEqual({pv_log_file}, reporter._file_name_queue)

        def mock_open(*args: Any, **kwargs: Any) -> Any:
            if args[0] == pv_log_file:
                raise FileNotFoundError(f"File {pv_log_file} disappeared before opening.")
            return open(*args, **kwargs)

        with patch.object(progress_reporter, "open_nofollow", mock_open):
            reporter.start()
            time.sleep(0.1)  # Give the reporter thread a moment to attempt to open the file
            reporter.stop()

        mock_log.warning.assert_called_with(
            "ProgressReporter: pv log file disappeared before initial open, skipping: %s", pv_log_file
        )
        self.assertSetEqual(set(), reporter._file_name_set)
        self.assertSetEqual(set(), reporter._file_name_queue)
        self.assertIsNone(reporter._exception)

    def test_run_internal_returns_when_stopping(self) -> None:
        reporter = ProgressReporter(MagicMock(spec=Logger), [], use_select=False, progress_update_intervals=(0.01, 0.02))
        fds: list[IO[Any]] = []

        class DummySelector:
            def __init__(self) -> None:
                self.select_called = False

            def register(self, fd: IO[Any], events: int, data: Any) -> None:
                pass

            def select(self, timeout: int = 0) -> list[tuple[Any, Any]]:
                self.select_called = True
                return []

            def close(self) -> None:
                pass

        selector = DummySelector()
        reporter._is_stopping = True
        reporter._run_internal(fds, cast(selectors.BaseSelector, selector))
        self.assertFalse(selector.select_called)
        self.assertEqual([], fds)

    def test_run_internal_reset_triggers_progress_output(self) -> None:
        log = MagicMock(spec=Logger)
        reporter = ProgressReporter(log, [], use_select=False, progress_update_intervals=(1e-6, 2e-6))
        fds: list[IO[Any]] = []
        selector = MagicMock()
        selector.select.return_value = []
        with patch("sys.stdout.write"), patch("sys.stdout.flush"):
            with self.assertRaises(ValueError):
                reporter._inject_error = True
                reporter._run_internal(fds, selector)
            log.log.assert_called()

    def test_run_internal_logs_missing_file(self) -> None:
        mock_log = MagicMock(spec=Logger)
        reporter = ProgressReporter(mock_log, [], use_select=False, progress_update_intervals=(0.01, 0.02))
        pv_file = "nonexist.pv"
        with reporter._lock:
            reporter._file_name_queue.add(pv_file)
        selector = MagicMock()
        selector.select.return_value = []
        with (
            patch.object(progress_reporter, "open_nofollow", side_effect=FileNotFoundError()),
            patch.object(Path, "touch", lambda self, *args, **kwargs: None),
        ):
            with self.assertRaises(ValueError):
                reporter._inject_error = True
                reporter._run_internal([], selector)
        mock_log.warning.assert_called_with(
            "ProgressReporter: pv log file disappeared before initial open, skipping: %s", pv_file
        )
        self.assertNotIn(pv_file, reporter._file_name_set)

    def test_run_internal_reads_lines(self) -> None:
        reporter = ProgressReporter(MagicMock(spec=Logger), [], use_select=False, progress_update_intervals=(0.01, 0.02))
        fds: list[IO[Any]] = []
        fake_file = io.StringIO("a\nb\n")
        stat = reporter.TransferStat(bytes_in_flight=0, eta=reporter.TransferStat.ETA(0, 0, ""))
        key = types.SimpleNamespace(data=(iter(fake_file), stat))
        selector = MagicMock()
        selector.select.return_value = [(key, None)]
        with patch.object(reporter, "_update_transfer_stat", return_value=1) as upd_mock:
            with self.assertRaises(ValueError):
                reporter._inject_error = True
                reporter._run_internal(fds, selector)
            self.assertEqual(2, upd_mock.call_count)

    def test_run_internal_progress_line_includes_latest_eta_line_tail(self) -> None:
        log = MagicMock(spec=Logger)
        reporter = ProgressReporter(log, [], use_select=False, progress_update_intervals=(1e-6, 2e-6))
        fds: list[IO[Any]] = []
        fd1 = io.StringIO("x\n")
        fd2 = io.StringIO("y\n")
        eta1 = reporter.TransferStat.ETA(timestamp_nanos=1, seq_nr=0, line_tail="tail1")
        eta2 = reporter.TransferStat.ETA(timestamp_nanos=2, seq_nr=-1, line_tail="tail2")
        stat1 = reporter.TransferStat(bytes_in_flight=0, eta=eta1)
        stat2 = reporter.TransferStat(bytes_in_flight=0, eta=eta2)
        key1 = types.SimpleNamespace(data=(iter(fd1), stat1))
        key2 = types.SimpleNamespace(data=(iter(fd2), stat2))
        with reporter._lock:
            reporter._file_name_queue.update({"f1", "f2"})

        selector = MagicMock()
        selector.select.return_value = [(key1, None), (key2, None)]

        def open_side_effect(name: str, **_: Any) -> io.StringIO:
            return fd1 if name.endswith("f1") else fd2

        with (
            patch.object(progress_reporter, "open_nofollow", side_effect=open_side_effect),
            patch.object(Path, "touch", lambda self, *args, **kwargs: None),
            patch.object(reporter, "_update_transfer_stat", return_value=0),
        ):
            with patch("sys.stdout.write"), patch("sys.stdout.flush"):
                with self.assertRaises(ValueError):
                    reporter._inject_error = True
                    reporter._run_internal(fds, selector)
                log.log.assert_called()

    def test_run_internal_calls_sleep_when_no_lines(self) -> None:
        reporter = ProgressReporter(MagicMock(spec=Logger), [], use_select=False, progress_update_intervals=(2, 2))
        fds: list[IO[Any]] = []
        sleep_count = 0

        def fake_sleep(duration_nanos: int) -> None:
            nonlocal sleep_count
            sleep_count += 1
            reporter._is_stopping = True

        reporter._sleeper.sleep = fake_sleep  # type: ignore[assignment]
        selector = MagicMock()
        selector.select.return_value = []
        reporter._run_internal(fds, selector)
        self.assertEqual(1, sleep_count)

    def test_run_internal_inject_error_propagates(self) -> None:
        reporter = ProgressReporter(
            MagicMock(spec=Logger), [], use_select=False, progress_update_intervals=(0.01, 0.02), fail=True
        )
        with self.assertRaises(ValueError):
            reporter._run_internal([], MagicMock())

    def test_run_internal_registers_all_new_files(self) -> None:
        reporter = ProgressReporter(MagicMock(spec=Logger), [], use_select=False, progress_update_intervals=(0.01, 0.02))
        paths = ["f1", "f2"]
        for p in paths:
            with reporter._lock:
                reporter._file_name_queue.add(p)
        selector_calls: list[Any] = []

        class DummySelector:
            def register(self, fd: IO[Any], events: int, data: Any) -> None:
                selector_calls.append(fd)

            def select(self, timeout: int = 0) -> list[tuple[Any, Any]]:
                reporter._is_stopping = True
                return []

            def close(self) -> None:
                pass

        with (
            patch.object(progress_reporter, "open_nofollow", return_value=io.StringIO("")),
            patch.object(Path, "touch", lambda self, *args, **kwargs: None),
        ):
            reporter._run_internal([], cast(selectors.BaseSelector, DummySelector()))
        self.assertEqual(2, len(selector_calls))

    def test_run_internal_status_line_without_etas(self) -> None:
        reporter = ProgressReporter(MagicMock(spec=Logger), [], use_select=False, progress_update_intervals=(1e-6, 2e-6))
        self.assertListEqual([State.IS_RESETTING], reporter._states)
        fds: list[IO[Any]] = []
        selector = MagicMock()
        selector.select.return_value = []
        with patch("sys.stdout.write") as write_mock, patch("sys.stdout.flush"):
            with self.assertRaises(ValueError):
                reporter._inject_error = True
                reporter._run_internal(fds, selector)
            written = "".join(call.args[0] for call in write_mock.mock_calls)
            self.assertNotIn("ETA", written.split())
        self.assertListEqual([], reporter._states)

    def test_pause_and_reset_flags(self) -> None:
        reporter = ProgressReporter(MagicMock(spec=Logger), [], use_select=False, progress_update_intervals=None)
        self.assertListEqual([State.IS_RESETTING], reporter._states)
        reporter.pause()
        with reporter._lock:
            self.assertListEqual([State.IS_RESETTING, State.IS_PAUSING], reporter._states)
        reporter.pause()
        with reporter._lock:
            self.assertListEqual([State.IS_RESETTING, State.IS_PAUSING], reporter._states)
        reporter.reset()
        with reporter._lock:
            self.assertListEqual([State.IS_PAUSING, State.IS_RESETTING], reporter._states)
        reporter.reset()
        with reporter._lock:
            self.assertListEqual([State.IS_PAUSING, State.IS_RESETTING], reporter._states)
        reporter.pause()
        with reporter._lock:
            self.assertListEqual([State.IS_RESETTING, State.IS_PAUSING], reporter._states)

        fds: list[IO[Any]] = []
        sleep_count = 0

        def fake_sleep(duration_nanos: int) -> None:
            nonlocal sleep_count
            sleep_count += 1
            reporter._is_stopping = True

        reporter._sleeper.sleep = fake_sleep  # type: ignore[assignment]
        selector = MagicMock()
        selector.select.return_value = []
        reporter._run_internal(fds, selector)
        self.assertEqual(1, sleep_count)
        self.assertListEqual([], reporter._states)

    def test_run_finally_closes_fds(self) -> None:
        mock_log = MagicMock(spec=Logger)
        reporter = ProgressReporter(mock_log, [], use_select=False, progress_update_intervals=None)
        fake_fd = MagicMock()

        def fake_run_internal(fds: list[IO[Any]], selector: Any) -> None:
            fds.append(fake_fd)
            raise ValueError()

        selector = MagicMock()
        with patch.object(reporter, "_run_internal", side_effect=fake_run_internal):
            with patch("selectors.PollSelector", return_value=selector):
                reporter._run()
        fake_fd.close.assert_called_once()
        selector.close.assert_called_once()
        self.assertIsInstance(reporter._exception, ValueError)

    def test_format_sent_bytes(self) -> None:
        self.assertEqual(("0.00 B", "[0.00 B/s]"), ProgressReporter._format_sent_bytes(0, 1))
        self.assertEqual(
            ("1.00 MiB", "[512.00 KiB/s]"),
            ProgressReporter._format_sent_bytes(1024 * 1024, 2_000_000_000),
        )

    def test_format_duration(self) -> None:
        self.assertEqual("0:00:05", ProgressReporter._format_duration(5_000_000_000))
        self.assertEqual("1:01:01", ProgressReporter._format_duration(3_661_000_000_000))

    def test_progress_reporter_emits_carriage_return_to_log_and_stream(self) -> None:
        """ProgressReporter must end status lines with \r (not \n) for both stream and file handlers."""
        with capture_stdout() as stream_buf:
            args = bzfs.argument_parser().parse_args(["src", "dst"])  # minimal valid args
            log_params = LogParams(args)
            log = get_logger(log_params, args)
            try:
                # Write a few pv-style status updates terminated by "\r" so ProgressReporter has content to parse and emit
                sample = "125 GiB: 2.71GiB 0:00:08 [98.8MiB/s] [341MiB/s] [>   ]  2% ETA 0:06:03 ETA 17:27:49\r"
                pv_file = log_params.pv_log_file
                with open(pv_file, "a", encoding="utf-8") as fd:
                    fd.write(sample)
                    fd.write(sample)

                # Create ProgressReporter with fast update intervals; consume the pv log file
                reporter = ProgressReporter(log, [], use_select=False, progress_update_intervals=(0.05, 0.1))
                reporter.enqueue_pv_log_file(pv_file)
                reporter.start()
                reporter.wait_for_first_status_line()
                reporter.stop()

                # Assert stream handler captured CR immediately after status msg and not followed by "\n"
                out: str = stream_buf.getvalue()
                self.assertTrue(len(out) > 0)
                self.assertIn("\r", out)
                self.assertNotIn("\r\n", out, "must not emit CRLF")
                self.assertTrue(out.endswith("\r"), "stream output must end with CR, no trailing LF")
                self.assertNotIn("\n", out, "must not emit LF")

                # Assert log file contains CR immediately after status msg and not followed by "\n"
                with open(log_params.log_file, "rb") as f:
                    data: str = f.read().decode("utf-8")
                self.assertTrue(len(data) > 0)
                self.assertIn("\r", data)
                self.assertNotIn("\r\n", data, "must not emit CRLF")
                self.assertTrue(data.endswith("\r"), "file output must end with CR, no trailing LF")
                self.assertNotIn("\n", data, "must not emit LF")
            finally:
                reset_logger(log)
