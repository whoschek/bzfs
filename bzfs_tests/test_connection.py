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
import itertools
import logging
import random
import subprocess
import threading
import time
import unittest
from collections import Counter
from types import SimpleNamespace
from typing import (
    Any,
    cast,
)
from unittest import mock

import bzfs_main.loggers
from bzfs_main import bzfs, connection
from bzfs_main.bzfs import (
    LogParams,
    Params,
    Remote,
)
from bzfs_main.connection import (
    DEDICATED,
    SHARED,
    Connection,
    ConnectionPool,
    ConnectionPools,
)
from bzfs_main.retry import RetryableError
from bzfs_main.utils import log_trace
from bzfs_tests.abstract_testcase import AbstractTestCase


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestConnectionPool,
        TestRunSshCommand,
        TestTrySshCommand,
        TestRefreshSshConnection,
        TestTimeout,
        TestMaybeInjectError,
        TestDecrementInjectionCounter,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class SlowButCorrectConnectionPool(ConnectionPool):  # validate a better implementation against this baseline

    def __init__(self, remote: Remote, max_concurrent_ssh_sessions_per_tcp_connection: int) -> None:
        super().__init__(remote, max_concurrent_ssh_sessions_per_tcp_connection)
        self.priority_queue: list[Connection] = []  # type: ignore

    def get_connection(self) -> Connection:
        with self._lock:
            self.priority_queue.sort()
            conn = self.priority_queue[-1] if self.priority_queue else None
            if conn is None or conn.is_full():
                conn = Connection(self.remote, self.capacity, self.cid)
                self.last_modified += 1
                conn.update_last_modified(self.last_modified)  # LIFO tiebreaker favors latest conn as that's most alive
                self.cid += 1
                self.priority_queue.append(conn)
            conn.increment_free(-1)
            return conn

    def return_connection(self, old_conn: Connection) -> None:
        assert old_conn is not None
        with self._lock:
            assert any(old_conn is c for c in self.priority_queue)
            old_conn.increment_free(1)
            self.last_modified += 1
            old_conn.update_last_modified(self.last_modified)

    def __repr__(self) -> str:
        with self._lock:
            return str({"capacity": self.capacity, "queue_len": len(self.priority_queue), "queue": self.priority_queue})


#############################################################################
class TestConnectionPool(AbstractTestCase):
    def setUp(self) -> None:
        args = self.argparser_parse_args(args=["src", "dst", "-v"])
        p = Params(args, log=bzfs_main.loggers.get_logger(LogParams(args), args))
        self.src = p.src
        self.dst = p.dst
        self.dst.ssh_user_host = "127.0.0.1"
        self.remote = bzfs.Remote("src", args, p)
        self.src2 = bzfs.Remote("src", args, p)

    def assert_priority_queue(self, cpool: ConnectionPool, queuelen: int) -> None:
        self.assertEqual(queuelen, len(cpool.priority_queue))

    def assert_equal_connections(self, conn: Connection, donn: Connection) -> None:
        self.assertTupleEqual((donn.cid, donn.ssh_cmd), (conn.cid, conn.ssh_cmd))
        self.assertEqual(donn.free, conn.free)
        self.assertEqual(donn.last_modified, conn.last_modified)

    def get_connection(self, cpool: ConnectionPool, dpool: SlowButCorrectConnectionPool) -> tuple[Connection, Connection]:
        conn = cpool.get_connection()
        donn = dpool.get_connection()
        self.assert_equal_connections(conn, donn)
        return conn, donn

    def return_connection(
        self,
        cpool: ConnectionPool,
        conn: Connection,
        dpool: SlowButCorrectConnectionPool,
        donn: Connection,
    ) -> None:
        self.assertTupleEqual((donn.cid, donn.ssh_cmd), (conn.cid, conn.ssh_cmd))
        cpool.return_connection(conn)
        dpool.return_connection(donn)

    def test_basic(self) -> None:
        counter1a = itertools.count()
        counter2a = itertools.count()
        counter1b = itertools.count()
        self.src.local_ssh_command = lambda counter=counter1a: [str(next(counter))]  # type: ignore
        self.src2.local_ssh_command = lambda counter=counter1b: [str(next(counter))]  # type: ignore

        with self.assertRaises(AssertionError):
            ConnectionPool(self.src, 0)

        capacity = 2
        cpool = ConnectionPool(self.src, capacity)
        dpool = SlowButCorrectConnectionPool(self.src2, capacity)
        self.assert_priority_queue(cpool, 0)
        self.assertIsNotNone(repr(cpool))
        self.assertIsNotNone(str(cpool))
        cpool.shutdown("foo")

        conn1, donn1 = self.get_connection(cpool, dpool)
        self.assert_priority_queue(cpool, 1)
        self.assertEqual((capacity - 1) * 1, conn1.free)
        i = [str(next(counter2a))]
        self.assertEqual(i, conn1.ssh_cmd)
        self.assertEqual(i, conn1.ssh_cmd_quoted)
        self.assertIsNotNone(repr(conn1))
        self.assertIsNotNone(str(conn1))

        self.return_connection(cpool, conn1, dpool, donn1)
        self.assert_priority_queue(cpool, 1)

        conn2, donn2 = self.get_connection(cpool, dpool)
        self.assert_priority_queue(cpool, 1)
        self.assertIs(conn1, conn2)
        self.assertEqual((capacity - 1) * 1, conn2.free)

        conn3, donn3 = self.get_connection(cpool, dpool)
        self.assert_priority_queue(cpool, 1)
        self.assertIs(conn2, conn3)

        self.return_connection(cpool, conn2, dpool, donn2)
        self.assert_priority_queue(cpool, 1)

        self.return_connection(cpool, conn3, dpool, donn3)
        self.assert_priority_queue(cpool, 1)

        with self.assertRaises(AssertionError):
            cpool.return_connection(cast(Any, None))
        with self.assertRaises(AssertionError):
            cpool.return_connection(conn3)
        cpool.shutdown("bar")
        cpool.return_connection(conn3)

    def test_multiple_tcp_connections(self) -> None:
        capacity = 2
        cpool = ConnectionPool(self.remote, capacity)

        conn1 = cpool.get_connection()
        self.assertEqual((capacity - 1) * 1, conn1.free)
        conn2 = cpool.get_connection()
        self.assertIs(conn1, conn2)
        self.assertEqual((capacity - 2) * 1, conn2.free)
        self.assert_priority_queue(cpool, 1)

        conn3 = cpool.get_connection()
        self.assertIsNot(conn2, conn3)
        self.assertEqual((capacity - 1) * 1, conn3.free)
        self.assertEqual((capacity - 2) * 1, conn2.free)
        self.assert_priority_queue(cpool, 2)
        conn4 = cpool.get_connection()
        self.assertIs(conn3, conn4)
        self.assertEqual((capacity - 2) * 1, conn4.free)
        self.assert_priority_queue(cpool, 2)

        conn5 = cpool.get_connection()
        self.assertIsNot(conn4, conn5)
        self.assertEqual((capacity - 1) * 1, conn5.free)
        self.assertEqual((capacity - 2) * 1, conn4.free)
        self.assert_priority_queue(cpool, 3)

        cpool.return_connection(conn3)
        self.assert_priority_queue(cpool, 3)
        cpool.return_connection(conn4)
        t4 = cpool.last_modified
        self.assert_priority_queue(cpool, 3)

        cpool.return_connection(conn2)
        self.assert_priority_queue(cpool, 3)
        cpool.return_connection(conn1)
        t2 = cpool.last_modified
        self.assert_priority_queue(cpool, 3)

        cpool.return_connection(conn5)
        t5 = cpool.last_modified
        self.assert_priority_queue(cpool, 3)

        # assert sort order evens out the number of concurrent sessions among the TCP connections
        conn5a = cpool.get_connection()
        self.assertEqual((capacity - 1) * 1, conn5a.free)

        conn2a = cpool.get_connection()
        self.assertEqual((capacity - 1) * 1, conn2a.free)

        conn4a = cpool.get_connection()
        self.assertEqual((capacity - 1) * 1, conn4a.free)

        # assert tie-breaker in favor of most recently returned TCP connection
        conn6 = cpool.get_connection()
        self.assertEqual((capacity - 2) * 1, conn6.free)
        self.assertEqual(t5, abs(conn6.last_modified))

        conn1a = cpool.get_connection()
        self.assertEqual((capacity - 2) * 1, conn1a.free)
        self.assertEqual(t2, abs(conn1a.last_modified))

        conn4a = cpool.get_connection()
        self.assertEqual((capacity - 2) * 1, conn4a.free)
        self.assertEqual(t4, abs(conn4a.last_modified))

        cpool.shutdown("foo")

    def test_pools(self) -> None:
        with self.assertRaises(AssertionError):
            ConnectionPools(self.dst, {SHARED: 0})

        ConnectionPools(self.dst, {}).shutdown("foo")

        pools = ConnectionPools(self.dst, {SHARED: 8, DEDICATED: 1})
        self.assertIsNotNone(pools.pool(SHARED))
        self.assertIsNotNone(repr(pools))
        self.assertIsNotNone(str(pools))
        pools.shutdown("foo")
        pools.pool(SHARED).get_connection()
        pools.shutdown("foo")

    def test_return_sequence(self) -> None:
        maxsessions = 10
        items = 10
        for j in range(3):
            cpool = ConnectionPool(self.src, maxsessions)
            dpool = SlowButCorrectConnectionPool(self.src2, maxsessions)
            rng = random.Random(12345)
            conns = [self.get_connection(cpool, dpool) for _ in range(items)]
            while conns:
                i = rng.randint(0, len(conns) - 1) if j == 0 else 0 if j == 1 else len(conns) - 1
                conn, donn = conns.pop(i)
                self.return_connection(cpool, conn, dpool, donn)
            print(f"cpool: {cpool}")

    def test_long_random_walk(self) -> None:
        log = logging.getLogger(bzfs.__name__)
        # loglevel = logging.DEBUG
        loglevel = log_trace
        is_logging = log.isEnabledFor(loglevel)
        num_steps = 75 if self.is_unit_test or self.is_smoke_test or self.is_functional_test or self.is_adhoc_test else 1000
        log.info(f"num_random_steps: {num_steps}")
        start_time_nanos = time.time_ns()
        for maxsessions in range(1, 10 + 1):
            for items in range(64 + 1):
                counter1a = itertools.count()
                counter1b = itertools.count()
                self.src.local_ssh_command = lambda counter=counter1a: [str(next(counter))]  # type: ignore
                self.src2.local_ssh_command = lambda counter=counter1b: [str(next(counter))]  # type: ignore
                cpool = ConnectionPool(self.src, maxsessions)
                dpool = SlowButCorrectConnectionPool(self.src2, maxsessions)
                # dpool = ConnectionPool(self.src2, maxsessions)
                rng = random.Random(12345)
                conns = []
                try:
                    for _ in range(items):
                        conns.append(self.get_connection(cpool, dpool))
                    item = -1
                    for step in range(num_steps):
                        if is_logging:
                            log.log(loglevel, f"itr maxsessions: {maxsessions}, items: {items}, step: {step}")
                            log.log(loglevel, f"clen: {len(cpool.priority_queue)}, cpool: {cpool.priority_queue}")
                            log.log(loglevel, f"dlen: {len(dpool.priority_queue)}, dpool: {dpool.priority_queue}")
                        if not conns or rng.randint(0, 1):
                            log.log(loglevel, "get")
                            conns.append(self.get_connection(cpool, dpool))
                        else:
                            # k = rng.randint(0, 2)
                            k = 0
                            if k == 0:
                                i = rng.randint(0, len(conns) - 1)
                            elif k == 1:
                                i = 0
                            else:
                                i = len(conns) - 1
                            conn, donn = conns.pop(i)
                            if is_logging:
                                log.log(loglevel, f"return {i}/{len(conns)+1}: conn: {conn}, donn: {donn} ")
                            self.return_connection(cpool, conn, dpool, donn)
                except Exception:
                    print("Ooops!")
                    print(f"maxsessions: {maxsessions}, items: {items}, step: {step}, item: {item}")
                    print(f"clen: {len(cpool.priority_queue)}, cpool: {cpool.priority_queue}")
                    print(f"dlen: {len(dpool.priority_queue)}, dpool: {dpool.priority_queue}")
                    raise
                log.log(loglevel, "cpool: %s", cpool)
                # log.log(bzfs.log_debug, "cpool: %s", cpool)
        elapsed_secs = (time.time_ns() - start_time_nanos) / 1000_000_000
        log.info("random_walk took %s secs", elapsed_secs)


#############################################################################
class _FakeParams:
    def __init__(self) -> None:
        self.log = logging.getLogger(__name__)
        self.ssh_program = "ssh"
        self.connection_pools: dict[str, connection.ConnectionPools] = {}
        self.timeout_nanos = None

    def is_program_available(self, prog: str, loc: str) -> bool:
        return True


#############################################################################
class _FakeRemote(SimpleNamespace):
    def local_ssh_command(self) -> list[str]:
        return ["ssh", self.ssh_user_host or "localhost"]


#############################################################################
class TestRunSshCommand(AbstractTestCase):
    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.params = cast(bzfs.Params, _FakeParams())
        self.job.control_persist_secs = 90
        self.job.control_persist_margin_secs = 2
        self.job.timeout_nanos = None
        self.remote = cast(
            bzfs.Remote, _FakeRemote(location="dst", ssh_user_host="", reuse_ssh_connection=True, ssh_extra_opts=[])
        )
        self.conn = mock.Mock()
        self.conn.ssh_cmd = ["ssh"]
        self.conn.ssh_cmd_quoted = ["ssh"]
        self.conn_pool = mock.Mock()
        self.conn_pool.get_connection.return_value = self.conn
        pool_wrapper = mock.Mock(pool=mock.Mock(return_value=self.conn_pool))
        self.job.params.connection_pools = {"dst": pool_wrapper}

    @mock.patch("bzfs_main.connection.subprocess_run")
    def test_dry_run_skips_execution(self, mock_run: mock.Mock) -> None:
        result = connection.run_ssh_command(self.job, self.remote, cmd=["ls"], is_dry=True)
        self.assertEqual("", result)
        mock_run.assert_not_called()
        self.conn_pool.return_connection.assert_called_once_with(self.conn)

    @mock.patch("bzfs_main.connection.refresh_ssh_connection_if_necessary")
    @mock.patch("bzfs_main.connection.subprocess_run")
    def test_remote_calls_refresh_and_executes(self, mock_run: mock.Mock, mock_refresh: mock.Mock) -> None:
        self.remote.ssh_user_host = "host"
        mock_run.return_value = mock.Mock(stdout="out", stderr="err")
        result = connection.run_ssh_command(self.job, self.remote, cmd=["echo"], print_stdout=True, print_stderr=True)
        self.assertEqual("out", result)
        mock_refresh.assert_called_once_with(self.job, self.remote, self.conn)
        mock_run.assert_called_once()
        self.conn_pool.return_connection.assert_called_once_with(self.conn)

    @mock.patch("bzfs_main.connection.refresh_ssh_connection_if_necessary")
    @mock.patch(
        "bzfs_main.connection.subprocess_run",
        side_effect=subprocess.CalledProcessError(returncode=1, cmd="cmd", output="o", stderr="e"),
    )
    def test_calledprocesserror_propagates(self, mock_run: mock.Mock, mock_refresh: mock.Mock) -> None:
        self.remote.ssh_user_host = "h"
        with self.assertRaises(subprocess.CalledProcessError):
            connection.run_ssh_command(self.job, self.remote, cmd=["boom"], print_stdout=True)
        mock_run.assert_called_once()
        self.conn_pool.return_connection.assert_called_once_with(self.conn)

    @mock.patch("bzfs_main.connection.refresh_ssh_connection_if_necessary")
    @mock.patch(
        "bzfs_main.connection.subprocess_run",
        side_effect=subprocess.TimeoutExpired(cmd="cmd", timeout=1),
    )
    def test_timeout_expired_propagates(self, mock_run: mock.Mock, mock_refresh: mock.Mock) -> None:
        self.remote.ssh_user_host = "h"
        with self.assertRaises(subprocess.TimeoutExpired):
            connection.run_ssh_command(self.job, self.remote, cmd=["sleep"], print_stdout=True)
        mock_run.assert_called_once()
        self.conn_pool.return_connection.assert_called_once_with(self.conn)

    @mock.patch("bzfs_main.connection.refresh_ssh_connection_if_necessary")
    @mock.patch(
        "bzfs_main.connection.subprocess_run",
        side_effect=UnicodeDecodeError("utf-8", b"x", 0, 1, "boom"),
    )
    def test_unicode_error_propagates_without_logging(self, mock_run: mock.Mock, mock_refresh: mock.Mock) -> None:
        self.remote.ssh_user_host = "h"
        with self.assertRaises(UnicodeDecodeError):
            connection.run_ssh_command(self.job, self.remote, cmd=["foo"])
        mock_run.assert_called_once()
        self.conn_pool.return_connection.assert_called_once_with(self.conn)


#############################################################################
class TestTrySshCommand(AbstractTestCase):
    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.params = cast(bzfs.Params, _FakeParams())
        self.remote = cast(
            bzfs.Remote, _FakeRemote(location="dst", ssh_user_host="host", reuse_ssh_connection=True, ssh_extra_opts=[])
        )

    @mock.patch("bzfs_main.connection.run_ssh_command", return_value="ok")
    @mock.patch("bzfs_main.connection.maybe_inject_error")
    def test_success(self, mock_inject: mock.Mock, mock_run: mock.Mock) -> None:
        result = connection.try_ssh_command(self.job, self.remote, level=0, cmd=["ls"])
        self.assertEqual("ok", result)
        mock_inject.assert_called_once()
        mock_run.assert_called_once()

    @mock.patch(
        "bzfs_main.connection.run_ssh_command",
        side_effect=subprocess.CalledProcessError(returncode=1, cmd="cmd", stderr="zfs: dataset does not exist"),
    )
    def test_dataset_missing_returns_none(self, mock_run: mock.Mock) -> None:
        result = connection.try_ssh_command(self.job, self.remote, level=0, cmd=["zfs"], exists=True)
        self.assertIsNone(result)
        mock_run.assert_called_once()

    @mock.patch(
        "bzfs_main.connection.run_ssh_command",
        side_effect=subprocess.CalledProcessError(returncode=1, cmd="cmd", stderr="boom"),
    )
    def test_other_error_raises_retryable(self, mock_run: mock.Mock) -> None:
        with self.assertRaises(RetryableError):
            connection.try_ssh_command(self.job, self.remote, level=0, cmd=["zfs"], exists=False)
        mock_run.assert_called_once()

    @mock.patch(
        "bzfs_main.connection.run_ssh_command",
        side_effect=UnicodeDecodeError("utf-8", b"x", 0, 1, "boom"),
    )
    def test_unicode_error_wrapped(self, mock_run: mock.Mock) -> None:
        with self.assertRaises(RetryableError):
            connection.try_ssh_command(self.job, self.remote, level=0, cmd=["x"])
        mock_run.assert_called_once()

    @mock.patch("bzfs_main.connection.maybe_inject_error", side_effect=RetryableError("Subprocess failed"))
    def test_injected_retryable_error_propagates(self, mock_inject: mock.Mock) -> None:
        with self.assertRaises(RetryableError):
            connection.try_ssh_command(self.job, self.remote, level=0, cmd=["x"])
        mock_inject.assert_called_once()


#############################################################################
class TestRefreshSshConnection(AbstractTestCase):
    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.params = cast(bzfs.Params, _FakeParams())
        self.job.control_persist_secs = 4
        self.job.control_persist_margin_secs = 1
        self.job.timeout_nanos = None
        self.remote = cast(
            bzfs.Remote, _FakeRemote(location="dst", ssh_user_host="host", reuse_ssh_connection=True, ssh_extra_opts=[])
        )
        self.conn = connection.Connection(self.remote, 1, 0)
        self.conn.last_refresh_time = 0

    @mock.patch("bzfs_main.connection.subprocess_run")
    def test_local_mode_returns_immediately(self, mock_run: mock.Mock) -> None:
        self.remote.ssh_user_host = ""
        connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        mock_run.assert_not_called()

    @mock.patch("bzfs_main.connection.die", side_effect=SystemExit)
    def test_ssh_unavailable_dies(self, mock_die: mock.Mock) -> None:
        self.job.params.is_program_available = mock.Mock(return_value=False)  # type: ignore[method-assign]
        with self.assertRaises(SystemExit):
            connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        mock_die.assert_called_once()

    @mock.patch("bzfs_main.connection.subprocess_run")
    def test_reuse_disabled_no_action(self, mock_run: mock.Mock) -> None:
        self.remote.reuse_ssh_connection = False
        connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        mock_run.assert_not_called()

    @mock.patch("bzfs_main.connection.subprocess_run")
    def test_connection_alive_fast_path(self, mock_run: mock.Mock) -> None:
        self.conn.last_refresh_time = time.monotonic_ns()
        connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        mock_run.assert_not_called()

    @mock.patch("bzfs_main.connection.subprocess_run")
    def test_master_alive_refreshes_timestamp(self, mock_run: mock.Mock) -> None:
        mock_run.return_value = mock.Mock(returncode=0)
        connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        mock_run.assert_called_once()
        self.assertGreater(self.conn.last_refresh_time, 0)

    @mock.patch("bzfs_main.connection.die", side_effect=SystemExit)
    @mock.patch("bzfs_main.connection.subprocess_run")
    def test_master_start_failure_dies(self, mock_run: mock.Mock, mock_die: mock.Mock) -> None:
        mock_run.side_effect = [mock.Mock(returncode=1), mock.Mock(returncode=1, stderr="bad")]
        with self.assertRaises(SystemExit):
            connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        self.assertEqual(2, mock_run.call_count)
        mock_die.assert_called_once()

    @mock.patch("bzfs_main.connection.subprocess_run")
    def test_verbose_option_limits_persist_time(self, mock_run: mock.Mock) -> None:
        self.remote.ssh_extra_opts = ["-v"]
        mock_run.side_effect = [mock.Mock(returncode=1), mock.Mock(returncode=0)]
        connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        args = mock_run.call_args_list[1][0][0]
        self.assertIn("-oControlPersist=1s", args)


#############################################################################
class TestTimeout(AbstractTestCase):
    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.params = cast(bzfs.Params, _FakeParams())

    def test_no_timeout_returns_none(self) -> None:
        self.job.timeout_nanos = None
        self.assertIsNone(connection.timeout(self.job))

    def test_expired_timeout_raises(self) -> None:
        self.job.params.timeout_nanos = 1
        self.job.timeout_nanos = time.monotonic_ns() - 1
        with self.assertRaises(subprocess.TimeoutExpired):
            connection.timeout(self.job)

    def test_seconds_remaining_returned(self) -> None:
        self.job.timeout_nanos = time.monotonic_ns() + 1_000_000_000
        result = connection.timeout(self.job)
        self.assertIsNotNone(result)
        self.assertGreaterEqual(cast(float, result), 0.0)


#############################################################################
class TestMaybeInjectError(AbstractTestCase):
    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.injection_lock = threading.Lock()
        self.job.error_injection_triggers = {"before": Counter()}

    def test_no_trigger_noop(self) -> None:
        connection.maybe_inject_error(self.job, ["cmd"])

    def test_missing_counter_noop(self) -> None:
        connection.maybe_inject_error(self.job, ["cmd"], error_trigger="boom")

    def test_counter_zero_noop(self) -> None:
        self.job.error_injection_triggers["before"]["boom"] = 0
        connection.maybe_inject_error(self.job, ["cmd"], error_trigger="boom")
        self.assertEqual(0, self.job.error_injection_triggers["before"]["boom"])

    def test_nonretryable_error_raised(self) -> None:
        self.job.error_injection_triggers["before"]["boom"] = 1
        with self.assertRaises(subprocess.CalledProcessError):
            connection.maybe_inject_error(self.job, ["cmd"], error_trigger="boom")
        self.assertEqual(0, self.job.error_injection_triggers["before"]["boom"])

    def test_retryable_error_raised(self) -> None:
        self.job.error_injection_triggers["before"]["retryable_boom"] = 1
        with self.assertRaises(RetryableError):
            connection.maybe_inject_error(self.job, ["cmd"], error_trigger="retryable_boom")


#############################################################################
class TestDecrementInjectionCounter(AbstractTestCase):
    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.injection_lock = threading.Lock()

    def test_zero_counter_returns_false(self) -> None:
        counter: Counter = Counter()
        self.assertFalse(connection.decrement_injection_counter(self.job, counter, "x"))

    def test_positive_counter_decrements(self) -> None:
        counter: Counter = Counter({"x": 2})
        self.assertTrue(connection.decrement_injection_counter(self.job, counter, "x"))
        self.assertEqual(1, counter["x"])
