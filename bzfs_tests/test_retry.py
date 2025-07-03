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
import unittest
from logging import Logger
from unittest.mock import MagicMock, patch

from bzfs_main.retry import Retry, RetryableError, RetryPolicy, run_with_retries
from bzfs_tests.abstract_test import AbstractTest


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestRunWithRetries,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestRunWithRetries(AbstractTest):

    def test_retry_policy_repr(self) -> None:
        args = argparse.Namespace(retries=2, retry_min_sleep_secs=0.1, retry_max_sleep_secs=0.5, retry_max_elapsed_secs=1)
        rp = RetryPolicy(args)
        self.assertIn("retries: 2", repr(rp))

    @patch("time.sleep")
    def test_run_with_retries_success(self, mock_sleep: MagicMock) -> None:
        calls: list[int] = []
        args = argparse.Namespace(retries=2, retry_min_sleep_secs=0, retry_max_sleep_secs=0, retry_max_elapsed_secs=1)
        retry_policy = RetryPolicy(args)

        def fn(*, retry: Retry) -> str:
            calls.append(retry.count)
            if retry.count < 2:
                raise RetryableError("fail", no_sleep=(retry.count == 0)) from ValueError("boom")
            return "ok"

        self.assertEqual("ok", run_with_retries(MagicMock(spec=Logger), retry_policy, fn))
        self.assertEqual([0, 1, 2], calls)

    @patch("time.sleep")
    def test_run_with_retries_gives_up(self, mock_sleep: MagicMock) -> None:
        args = argparse.Namespace(retries=1, retry_min_sleep_secs=0, retry_max_sleep_secs=0, retry_max_elapsed_secs=1)
        retry_policy = RetryPolicy(args)

        def fn(*, retry: Retry) -> None:
            raise RetryableError("fail", no_sleep=True) from ValueError("boom")

        with self.assertRaises(ValueError):
            run_with_retries(MagicMock(spec=Logger), retry_policy, fn)

    @patch("time.sleep")
    def test_run_with_retries_no_retries(self, mock_sleep: MagicMock) -> None:
        args = argparse.Namespace(retries=0, retry_min_sleep_secs=0, retry_max_sleep_secs=0, retry_max_elapsed_secs=1)
        retry_policy = RetryPolicy(args)
        mock_log = MagicMock(spec=Logger)

        def fn(*, retry: Retry) -> None:
            raise RetryableError("fail") from ValueError("boom")

        with self.assertRaises(ValueError):
            run_with_retries(mock_log, retry_policy, fn)
        mock_log.warning.assert_not_called()
        mock_sleep.assert_not_called()

    @patch("time.sleep")
    def test_run_with_retries_elapsed_time(self, mock_sleep: MagicMock) -> None:
        args = argparse.Namespace(retries=5, retry_min_sleep_secs=0, retry_max_sleep_secs=0, retry_max_elapsed_secs=0)
        retry_policy = RetryPolicy(args)
        mock_log = MagicMock(spec=Logger)
        max_elapsed = retry_policy.max_elapsed_nanos

        with patch("time.monotonic_ns", side_effect=[0, max_elapsed + 1]):

            def fn(*, retry: Retry) -> None:
                raise RetryableError("fail", no_sleep=True) from ValueError("boom")

            with self.assertRaises(ValueError):
                run_with_retries(mock_log, retry_policy, fn)
        mock_log.warning.assert_called_once()
