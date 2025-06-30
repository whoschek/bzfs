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
import os
import shutil
import subprocess
import tempfile
import time
import unittest
from logging import Logger
from pathlib import Path
from typing import (
    Any,
)
from unittest.mock import MagicMock, patch

from bzfs_main import bzfs, progress_reporter, utils
from bzfs_main.progress_reporter import ProgressReporter, count_num_bytes_transferred_by_zfs_send, pv_file_thread_separator
from bzfs_tests.test_utils import stop_on_failure_subtest


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
            num_bytes, _ = progress_reporter.pv_size_to_bytes(line)
            return num_bytes

        self.assertEqual(800, pv_size_to_bytes("800B foo"))
        self.assertEqual(1, pv_size_to_bytes("8b"))
        self.assertEqual(round(4.12 * 1024), pv_size_to_bytes("4.12 KiB"))
        self.assertEqual(round(46.2 * 1024**3), pv_size_to_bytes("46,2GiB"))
        self.assertEqual(round(46.2 * 1024**3), pv_size_to_bytes("46.2GiB"))
        self.assertEqual(round(46.2 * 1024**3), pv_size_to_bytes("46" + progress_reporter.arabic_decimal_separator + "2GiB"))
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
            print("pv_log_file: " + "\n".join(utils.tail(pv_file, 10)))
            self.assertEqual(16 * 1024, num_bytes)
        finally:
            Path(pv_file).unlink(missing_ok=True)

    def test_count_num_bytes_transferred_by_zfs_send(self) -> None:
        self.assertEqual(0, count_num_bytes_transferred_by_zfs_send("/tmp/nonexisting_bzfs_test_file"))
        for i in range(2):
            with stop_on_failure_subtest(i=i):
                pv1_fd, pv1 = tempfile.mkstemp(prefix="test_bzfs.test_count_num_byte", suffix=".pv")
                os.write(pv1_fd, ": 800B foo".encode("utf-8"))
                pv2 = pv1 + pv_file_thread_separator + "001"
                with open(pv2, "w", encoding="utf-8") as fd:
                    fd.write("1 KB\r\n")
                    fd.write("2 KiB:20 KB\r")
                    fd.write("3 KiB:300 KB\r\n")
                    fd.write("4 KiB:4000 KB\r")
                    fd.write("4 KiB:50000 KB\r")
                    fd.write("4 KiB:600000 KB\r" + ("\n" if i == 0 else ""))
                pv3 = pv1 + pv_file_thread_separator + "002"
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
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(2.71 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), eta_timestamp_nanos)
                self.assertEqual("[>                ]  2% ETA 0:06:03 ETA 17:27:49", line_tail)

                # intermediate line with duration ETA that contains days
                line = "98 GiB/ 0 B/  98 GiB: 93.1GiB 0:12:12 [ 185MiB/s] [ 130MiB/s] [==>  ] 94% ETA 2+0:00:39 ETA 17:55:48"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(93.1 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (2 * 86400 + 39), eta_timestamp_nanos)
                self.assertEqual("[==>  ] 94% ETA 2+0:00:39 ETA 17:55:48", line_tail)

                # intermediate line with duration ETA that contains other days syntax and timestamp ETA that contains days
                line = "98 GiB/ 0 B/  98 GiB: 93.1GiB 0:12:12 ( 185MiB/s) ( 130MiB/s) [==>  ] ETA 1:00:07:16 ETA 2025-01-23 14:06:02"  # noqa: E501
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(93.1 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (1 * 86400 + 7 * 60 + 16), eta_timestamp_nanos)
                self.assertEqual("[==>  ] ETA 1:00:07:16 ETA 2025-01-23 14:06:02", line_tail)

                # intermediate line with duration ETA that contains other days syntax and timestamp ETA using FIN marker
                line = "98 GiB/ 0 B/  98 GiB: 93.1GiB 9:0:12:12 [ 185MiB/s] [ 130MiB/s] [==>  ] ETA 1:00:07:16 FIN 2025-01-23 14:06:02"  # noqa: E501
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(93.1 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (1 * 86400 + 7 * 60 + 16), eta_timestamp_nanos)
                self.assertEqual("[==>  ] ETA 1:00:07:16 FIN 2025-01-23 14:06:02", line_tail)

                # final line on transfer completion does not contain duration ETA
                line = "98 GiB/ 0 B/  98 GiB: 98,1GiB 0:12:39 [ 132MiB/s] [ 132MiB/s] [=====>] 100%             ETA 17:55:37"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(98.1 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos, eta_timestamp_nanos)
                self.assertEqual("[=====>] 100%             ETA 17:55:37", line_tail)

                # final line on transfer completion does not contain duration ETA
                line = "12.6KiB: 44.2KiB 0:00:00 [3.14MiB/s] [3.14MiB/s] [===================] 350%             ETA 14:48:27"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(44.2 * 1024), num_bytes)
                self.assertEqual(curr_time_nanos, eta_timestamp_nanos)
                self.assertEqual("[===================] 350%             ETA 14:48:27", line_tail)

                # missing from --pv--program-opts: --timer, --rate, --average-rate
                line = "125 GiB: 2.71GiB[ >                    ]  2% ETA 0:06:03 ETA 17:27:49"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(2.71 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), eta_timestamp_nanos)
                self.assertEqual("[ >                    ]  2% ETA 0:06:03 ETA 17:27:49", line_tail)

                # missing from --pv--program-opts: --rate, --average-rate
                line = "125 GiB: 2.71GiB 0:00:08 [ >                    ]  2% ETA 0:06:03 ETA 17:27:49"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(2.71 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), eta_timestamp_nanos)
                self.assertEqual("0:00:08 [ >                    ]  2% ETA 0:06:03 ETA 17:27:49", line_tail)

                # intermediate line with square brackets after the first ETA (not sure if this actually occurs in the wild)
                line = "125 GiB: 2,71GiB 0:00:08 [98,8MiB/s] [ 341MiB/s] [>            ]  2% ETA 0:06:03 ] [ ETA 17:27:49]"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(round(2.71 * 1024**3), num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * (6 * 60 + 3), eta_timestamp_nanos)
                self.assertEqual("[>            ]  2% ETA 0:06:03 ] [ ETA 17:27:49]", line_tail)

                # zero line with final line on transfer completion does not contain duration ETA
                line = "275GiB: 0.00 B 0:00:00 [0.00 B/s] [0.00 B/s] [>                   ]  0%             ETA 03:24:32"
                num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
                self.assertEqual(0, num_bytes)
                self.assertEqual(curr_time_nanos + 1_000_000_000 * 0, eta_timestamp_nanos)
                self.assertEqual("[>                   ]  0%             ETA 03:24:32", line_tail)

                for line in eols:
                    num_bytes, eta_timestamp_nanos, line_tail = reporter.parse_pv_line(line + eol, curr_time_nanos)
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
                num_bytes = reporter.update_transfer_stat(line, stat, curr_time_nanos)
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
        reporter.exception = ValueError()
        with self.assertRaises(ValueError):
            reporter.stop()
        self.assertIsInstance(reporter.exception, ValueError)
        with self.assertRaises(ValueError):
            reporter.stop()
        self.assertIsInstance(reporter.exception, ValueError)

        reporter = ProgressReporter(MagicMock(spec=Logger), [], use_select=False, progress_update_intervals=None, fail=True)
        reporter._run()
        self.assertIsInstance(reporter.exception, ValueError)
        with self.assertRaises(ValueError):
            reporter.stop()
        self.assertIsInstance(reporter.exception, ValueError)
        with self.assertRaises(ValueError):
            reporter.stop()
        self.assertIsInstance(reporter.exception, ValueError)

    def test_progress_reporter_exhausted_iterator_sees_appended_data(self) -> None:
        # Create a temporary file. We will keep the file descriptor from mkstemp open for the read iterator
        temp_file_fd_write, temp_file_path = tempfile.mkstemp(prefix="test_iterator_behavior_")
        try:
            # Write initial lines to the file using the write file descriptor
            initial_lines = ["line1\n", "line2\n"]
            with open(temp_file_fd_write, "w", encoding="utf-8") as fd_write:
                for line in initial_lines:
                    fd_write.write(line)
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
                    read_lines_after_append_from_original_iterator,
                    ["line3_appended\n", "line4_appended\n"],
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
            delta_bytes = reporter.update_transfer_stat(line, stat_obj_for_pv_log_file, current_time_ns)
            total_bytes_op1 += delta_bytes
            current_time_ns += 1000  # Advance time slightly
        self.assertEqual(total_bytes_op1, 100 * 1024, "Total bytes for Op1 incorrect")
        # After op1 final line (ends with \n), bytes_in_flight in stat_obj_for_pv_log_file should be 0
        self.assertEqual(stat_obj_for_pv_log_file.bytes_in_flight, 0, "bytes_in_flight should be 0 after final line of Op1")

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
            delta_bytes = reporter.update_transfer_stat(line, stat_obj_for_pv_log_file, current_time_ns)
            total_bytes_op2 += delta_bytes
            current_time_ns += 1000
        self.assertEqual(total_bytes_op2, 200 * 1024, "Total bytes for Op2 incorrect")
        self.assertEqual(stat_obj_for_pv_log_file.bytes_in_flight, 0, "bytes_in_flight should be 0 after final line of Op2")

    def test_reporter_handles_pv_log_file_disappearing_before_initial_open(self) -> None:
        mock_log = MagicMock(spec=Logger)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, temp_dir)
        pv_log_file = os.path.join(temp_dir, "test.pv")
        reporter = ProgressReporter(mock_log, [], use_select=False, progress_update_intervals=None)
        self.assertSetEqual(set(), reporter.file_name_set)
        self.assertSetEqual(set(), reporter.file_name_queue)
        reporter.enqueue_pv_log_file(pv_log_file)
        self.assertSetEqual(set(), reporter.file_name_set)
        self.assertSetEqual({pv_log_file}, reporter.file_name_queue)

        def mock_open(*args: Any, **kwargs: Any) -> Any:
            if args[0] == pv_log_file:
                raise FileNotFoundError(f"File {pv_log_file} disappeared before opening.")
            return open(*args, **kwargs)

        with patch("bzfs_main.utils.open_nofollow", mock_open):
            reporter.start()
            time.sleep(0.1)  # Give the reporter thread a moment to attempt to open the file
            reporter.stop()

        mock_log.warning.assert_called_with(
            "ProgressReporter: pv log file disappeared before initial open, skipping: %s", pv_log_file
        )
        self.assertSetEqual(set(), reporter.file_name_set)
        self.assertSetEqual(set(), reporter.file_name_queue)
        self.assertIsNone(reporter.exception)
