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
"""Unit tests for SSH connection management utilities."""

from __future__ import (
    annotations,
)
import contextlib
import itertools
import logging
import os
import platform
import random
import subprocess
import tempfile
import threading
import time
import unittest
from collections import (
    Counter,
)
from collections.abc import (
    Iterator,
)
from types import (
    SimpleNamespace,
)
from typing import (
    Any,
    cast,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from bzfs_main import (
    bzfs,
    connection,
)
from bzfs_main.configuration import (
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
from bzfs_main.connection_lease import (
    FREE_DIR,
    USED_DIR,
    ConnectionLease,
    ConnectionLeaseManager,
)
from bzfs_main.retry import (
    RetryableError,
)
from bzfs_main.utils import (
    LOG_TRACE,
    UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH,
)
from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)


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
        TestConnectionLease,
        TestSshExitOnShutdown,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


# constants:
TEST_DIR_PREFIX: str = "t_bzfs"


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
        p = self.make_params(args=args)
        self.src = p.src
        self.dst = p.dst
        self.dst.ssh_user_host = "127.0.0.1"
        self.remote = Remote("src", args, p)
        self.src2 = Remote("src", args, p)

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
        self.src.local_ssh_command = lambda _socket_path=None, counter=counter1a: [str(next(counter))]  # type: ignore
        self.src2.local_ssh_command = lambda _socket_path=None, counter=counter1b: [str(next(counter))]  # type: ignore

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

    def test_long_random_walk(self) -> None:
        log = logging.getLogger(bzfs.__name__)
        # loglevel = logging.DEBUG
        loglevel = LOG_TRACE
        is_logging = log.isEnabledFor(loglevel)
        num_steps = 75 if self.is_unit_test or self.is_smoke_test or self.is_functional_test or self.is_adhoc_test else 1000
        log.info(f"num_random_steps: {num_steps}")
        start_time_nanos = time.time_ns()
        for maxsessions in range(1, 10 + 1):
            for items in range(64 + 1):
                counter1a = itertools.count()
                counter1b = itertools.count()
                self.src.local_ssh_command = lambda _socket_path=None, counter=counter1a: [str(next(counter))]  # type: ignore
                self.src2.local_ssh_command = lambda _socket_path=None, counter=counter1b: [str(next(counter))]  # type: ignore
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
def make_fake_params() -> Params:
    mock = MagicMock(spec=Params)
    mock.log = logging.getLogger(__name__)
    mock.ssh_program = "ssh"
    mock.connection_pools = {}
    mock.timeout_nanos = None
    return mock


#############################################################################
class _FakeRemote(SimpleNamespace):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # Provide minimal attributes expected by Connection for lease acquisition.
        if not hasattr(self, "ssh_socket_dir"):
            self.ssh_socket_dir = tempfile.mkdtemp(prefix=TEST_DIR_PREFIX)
        if not hasattr(self, "cache_namespace"):
            self.cache_namespace = lambda: "user@host#22#cfg"
        if not hasattr(self, "pool"):
            self.pool = SHARED

    def local_ssh_command(self, socket_path: str | None = None) -> list[str]:
        return ["ssh", self.ssh_user_host or "localhost"]


#############################################################################
class TestRunSshCommand(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.params = make_fake_params()
        self.job.control_persist_margin_secs = 2
        self.job.timeout_nanos = None
        self.remote = cast(
            Remote,
            _FakeRemote(
                location="dst", ssh_user_host="", reuse_ssh_connection=True, ssh_control_persist_secs=90, ssh_extra_opts=[]
            ),
        )
        self.conn = MagicMock()
        self.conn.ssh_cmd = ["ssh"]
        self.conn.ssh_cmd_quoted = ["ssh"]
        self.conn_pool = MagicMock()
        self.conn_pool.get_connection.return_value = self.conn
        self.conn_pool.return_connection = MagicMock()

        @contextlib.contextmanager
        def _connection_cm() -> Iterator[Connection]:
            yield self.conn
            self.conn_pool.return_connection(self.conn)

        self.conn_pool.connection.side_effect = lambda: _connection_cm()
        pool_wrapper = MagicMock(pool=MagicMock(return_value=self.conn_pool))
        self.job.params.connection_pools = {"dst": pool_wrapper}

    @patch("bzfs_main.connection.subprocess_run")
    def test_dry_run_skips_execution(self, mock_run: MagicMock) -> None:
        result = connection.run_ssh_command(self.job, self.remote, cmd=["ls"], is_dry=True)
        self.assertEqual("", result)
        mock_run.assert_not_called()
        self.conn_pool.connection.assert_called_once()

    @patch("bzfs_main.connection.refresh_ssh_connection_if_necessary")
    @patch("bzfs_main.connection.subprocess_run")
    def test_remote_calls_refresh_and_executes(self, mock_run: MagicMock, mock_refresh: MagicMock) -> None:
        self.remote.ssh_user_host = "host"
        mock_run.return_value = MagicMock(stdout="out", stderr="err")
        result = connection.run_ssh_command(self.job, self.remote, cmd=["echo"], print_stdout=True, print_stderr=True)
        self.assertEqual("out", result)
        mock_refresh.assert_called_once_with(self.job, self.remote, self.conn)
        mock_run.assert_called_once()
        self.conn_pool.connection.assert_called_once()

    @patch("bzfs_main.connection.refresh_ssh_connection_if_necessary")
    @patch(
        "bzfs_main.connection.subprocess_run",
        side_effect=subprocess.CalledProcessError(returncode=1, cmd="cmd", output="o", stderr="e"),
    )
    def test_calledprocesserror_propagates(self, mock_run: MagicMock, mock_refresh: MagicMock) -> None:
        self.remote.ssh_user_host = "h"
        with self.assertRaises(subprocess.CalledProcessError):
            connection.run_ssh_command(self.job, self.remote, cmd=["boom"], print_stdout=True)
        mock_run.assert_called_once()
        self.conn_pool.connection.assert_called_once()

    @patch("bzfs_main.connection.refresh_ssh_connection_if_necessary")
    @patch(
        "bzfs_main.connection.subprocess_run",
        side_effect=subprocess.TimeoutExpired(cmd="cmd", timeout=1),
    )
    def test_timeout_expired_propagates(self, mock_run: MagicMock, mock_refresh: MagicMock) -> None:
        self.remote.ssh_user_host = "h"
        with self.assertRaises(subprocess.TimeoutExpired):
            connection.run_ssh_command(self.job, self.remote, cmd=["sleep"], print_stdout=True)
        mock_run.assert_called_once()
        self.conn_pool.connection.assert_called_once()

    @patch("bzfs_main.connection.refresh_ssh_connection_if_necessary")
    @patch(
        "bzfs_main.connection.subprocess_run",
        side_effect=UnicodeDecodeError("utf-8", b"x", 0, 1, "boom"),
    )
    def test_unicode_error_propagates_without_logging(self, mock_run: MagicMock, mock_refresh: MagicMock) -> None:
        self.remote.ssh_user_host = "h"
        with self.assertRaises(UnicodeDecodeError):
            connection.run_ssh_command(self.job, self.remote, cmd=["foo"])
        mock_run.assert_called_once()
        self.conn_pool.connection.assert_called_once()


#############################################################################
class TestTrySshCommand(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.params = make_fake_params()
        self.remote = cast(
            Remote, _FakeRemote(location="dst", ssh_user_host="host", reuse_ssh_connection=True, ssh_extra_opts=[])
        )

    @patch("bzfs_main.connection.run_ssh_command", return_value="ok")
    @patch("bzfs_main.connection.maybe_inject_error")
    def test_success(self, mock_inject: MagicMock, mock_run: MagicMock) -> None:
        result = connection.try_ssh_command(self.job, self.remote, level=0, cmd=["ls"])
        self.assertEqual("ok", result)
        mock_inject.assert_called_once()
        mock_run.assert_called_once()

    @patch(
        "bzfs_main.connection.run_ssh_command",
        side_effect=subprocess.CalledProcessError(returncode=1, cmd="cmd", stderr="zfs: dataset does not exist"),
    )
    def test_dataset_missing_returns_none(self, mock_run: MagicMock) -> None:
        result = connection.try_ssh_command(self.job, self.remote, level=0, cmd=["zfs"], exists=True)
        self.assertIsNone(result)
        mock_run.assert_called_once()

    @patch(
        "bzfs_main.connection.run_ssh_command",
        side_effect=subprocess.CalledProcessError(returncode=1, cmd="cmd", stderr="boom"),
    )
    def test_other_error_raises_retryable(self, mock_run: MagicMock) -> None:
        with self.assertRaises(RetryableError):
            connection.try_ssh_command(self.job, self.remote, level=0, cmd=["zfs"], exists=False)
        mock_run.assert_called_once()

    @patch(
        "bzfs_main.connection.run_ssh_command",
        side_effect=UnicodeDecodeError("utf-8", b"x", 0, 1, "boom"),
    )
    def test_unicode_error_wrapped(self, mock_run: MagicMock) -> None:
        with self.assertRaises(RetryableError):
            connection.try_ssh_command(self.job, self.remote, level=0, cmd=["x"])
        mock_run.assert_called_once()

    @patch("bzfs_main.connection.maybe_inject_error", side_effect=RetryableError("Subprocess failed"))
    def test_injected_retryable_error_propagates(self, mock_inject: MagicMock) -> None:
        with self.assertRaises(RetryableError):
            connection.try_ssh_command(self.job, self.remote, level=0, cmd=["x"])
        mock_inject.assert_called_once()


#############################################################################
class TestRefreshSshConnection(AbstractTestCase):
    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.params = make_fake_params()
        self.job.control_persist_margin_secs = 1
        self.job.timeout_nanos = None
        self.remote = cast(
            Remote,
            _FakeRemote(
                params=self.job.params,
                location="dst",
                ssh_user_host="host",
                reuse_ssh_connection=True,
                ssh_control_persist_secs=4,
                ssh_exit_on_shutdown=False,
                ssh_extra_opts=[],
            ),
        )
        self.conn = connection.Connection(self.remote, 1, 0)
        self.conn.last_refresh_time = 0

    @patch("bzfs_main.connection.subprocess_run")
    def test_local_mode_returns_immediately(self, mock_run: MagicMock) -> None:
        self.remote.ssh_user_host = ""
        connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        mock_run.assert_not_called()

    @patch("bzfs_main.connection.die", side_effect=SystemExit)
    def test_ssh_unavailable_dies(self, mock_die: MagicMock) -> None:
        self.job.params.is_program_available = MagicMock(return_value=False)  # type: ignore[method-assign]
        with self.assertRaises(SystemExit):
            connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        mock_die.assert_called_once()

    @patch("bzfs_main.connection.subprocess_run")
    def test_reuse_disabled_no_action(self, mock_run: MagicMock) -> None:
        self.remote.reuse_ssh_connection = False
        connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        mock_run.assert_not_called()

    @patch("bzfs_main.connection.subprocess_run")
    def test_connection_alive_fast_path(self, mock_run: MagicMock) -> None:
        self.conn.last_refresh_time = time.monotonic_ns()
        connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        mock_run.assert_not_called()

    @patch("bzfs_main.connection.subprocess_run")
    def test_master_alive_refreshes_timestamp(self, mock_run: MagicMock) -> None:
        mock_run.return_value = MagicMock(returncode=0)
        connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        mock_run.assert_called_once()
        self.assertGreater(self.conn.last_refresh_time, 0)

    @patch("bzfs_main.connection.die", side_effect=SystemExit)
    @patch("bzfs_main.connection.subprocess_run")
    def test_master_start_failure_dies(self, mock_run: MagicMock, mock_die: MagicMock) -> None:
        mock_run.side_effect = [MagicMock(returncode=1), MagicMock(returncode=1, stderr="bad")]
        with self.assertRaises(SystemExit):
            connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        self.assertEqual(2, mock_run.call_count)
        mock_die.assert_called_once()

    @patch("bzfs_main.connection.subprocess_run")
    def test_verbose_option_limits_persist_time(self, mock_run: MagicMock) -> None:
        self.remote.ssh_extra_opts = ["-v"]
        mock_run.side_effect = [MagicMock(returncode=1), MagicMock(returncode=0)]
        connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        args = mock_run.call_args_list[1][0][0]
        self.assertIn("-oControlPersist=1s", args)


#############################################################################
class TestTimeout(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.params = make_fake_params()

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


#############################################################################
@unittest.skipIf(platform.system() == "SunOS", "skip on solaris")
class TestConnectionLease(AbstractTestCase):

    def test_manager_acquire_release(self) -> None:
        """Purpose: Validate the end-to-end lifecycle of a connection lease, covering acquisition of two distinct
        leases, directory placement in ``used/`` during ownership, and safe release back to ``free/`` with proper file
        movement. Also verifies the ControlPath directory is consistent across leases and that unique names are
        allocated.

        Assumptions: POSIX filesystem semantics, advisory ``flock`` behavior, and atomic ``os.replace`` are available.
        The temporary directory is not meddled with by other processes. Only this test manipulates files under the
        manager's root. The OS permits creating Unix domain socket paths in the generated directory.

        Design Rationale: The test asserts the strongest externally observable invariants rather than internal
        implementation details. It checks both presence and absence of files in ``used/`` and ``free/`` at the right
        times, uniqueness of socket names, and directory equivalence to ensure that distinct leases share the same
        ControlPath base. The finally block releases in reverse order to surface ordering bugs, and validates that
        releases are idempotent with the expected terminal state.
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = ConnectionLeaseManager(root_dir=tmpdir, namespace="user@host#22#cfg", log=MagicMock(logging.Logger))
            lease1: ConnectionLease = mgr.acquire()
            lease2: ConnectionLease = mgr.acquire()
            try:
                self.assertTrue(os.path.isfile(lease1.used_path))
                self.assertTrue(os.path.isfile(lease2.used_path))
                self.assertFalse(os.path.exists(lease1.free_path))
                self.assertFalse(os.path.exists(lease2.free_path))
                self.assertEqual(USED_DIR, os.path.basename(os.path.dirname(lease1.used_path)))
                self.assertEqual(FREE_DIR, os.path.basename(os.path.dirname(lease1.free_path)))
                self.assertEqual(USED_DIR, os.path.basename(os.path.dirname(lease2.used_path)))
                self.assertEqual(FREE_DIR, os.path.basename(os.path.dirname(lease2.free_path)))

                self.assertNotEqual(lease1.used_path, lease2.used_path)
                self.assertNotEqual(lease1.free_path, lease2.free_path)
                self.assertNotEqual(lease1.socket_path, lease2.socket_path)
                self.assertTrue(os.path.isdir(os.path.dirname(lease1.socket_path)))
                self.assertTrue(os.path.isdir(os.path.dirname(lease2.socket_path)))
                self.assertEqual(os.path.dirname(lease1.socket_path), os.path.dirname(lease2.socket_path))
            finally:
                lease2.release()
                lease1.release()
                self.assertTrue(os.path.isfile(lease1.free_path))
                self.assertTrue(os.path.isfile(lease2.free_path))
                self.assertFalse(os.path.exists(lease1.used_path))
                self.assertFalse(os.path.exists(lease2.used_path))

    def test_acquire_from_free_prefers_free(self) -> None:
        """Purpose: Ensure that acquisition prefers reusing a previously freed ControlPath over generating a new one.
        After priming the ``free/`` directory by acquiring and releasing a lease, the next acquisition should return
        the identical socket name moved to ``used/``.

        Assumptions: The ``free/`` directory contains exactly one matching candidate and no other concurrent writer is
        racing on the same root. The OS path length cap is respected by construction. Non-following open flags and
        symlink checks remain active and uncompromised.

        Design Rationale: Reuse minimizes churn and preserves operating system caches, while also reducing namespace
        growth and directory fragmentation. The test inspects both name equality and directory placement to confirm that
        reuse occurred through a lock-based atomic move rather than accidental regeneration. A negative probe via
        ``_try_lock`` on a second manager demonstrates that re-acquired leases cannot be stolen while held, capturing
        the concurrency contract.
        """
        root_dir: str = tempfile.mkdtemp(prefix=TEST_DIR_PREFIX)
        mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="user@host:22", log=MagicMock(logging.Logger))
        # Create a lease then release to populate free/
        lease1: ConnectionLease = mgr.acquire()
        socket_name = os.path.basename(lease1.socket_path)
        lease1.release()

        lease2: ConnectionLease = mgr.acquire()
        try:
            self.assertEqual(socket_name, os.path.basename(lease2.socket_path))
            # File should have moved to used/
            self.assertTrue(os.path.exists(lease2.used_path))
            self.assertFalse(os.path.exists(lease2.free_path))
            # Path length should not exceed OS domain socket limits in typical setups
            self.assertLessEqual(len(lease2.socket_path), UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH)
            # Another contender cannot acquire the same lock while lease is held
            other = ConnectionLeaseManager(root_dir=root_dir, namespace="user@host:22", log=MagicMock(logging.Logger))
            self.assertIsNone(other._try_lock(lease2.used_path, open_flags=os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC))
        finally:
            lease2.release()
            # After release, the file should be back in free/
            self.assertTrue(os.path.exists(lease2.free_path))
            self.assertFalse(os.path.exists(lease2.used_path))

    def test_acquire_salvages_from_used_when_unlocked(self) -> None:
        """Purpose: Verify salvage semantics from ``used/`` when a previous owner crashed and released the advisory lock
        implicitly. The manager should detect the unlocked file, acquire it, and maintain it in ``used/`` during the
        new lease.

        Assumptions: The test can safely write a file into ``used/`` to simulate a crash scenario. File system state is
        otherwise quiescent, and no lock is present on the crafted file. Only this process owns the directory.

        Design Rationale: Salvage is essential for robustness; it avoids leaking names and prevents unbounded growth of
        ControlPath files after process failures. Directly creating an unlocked file in ``used/`` and then acquiring a
        lease exercises the recovery path deterministically. A secondary lock attempt via ``_try_lock`` affirms that
        once the test holds the lease, normal exclusion guarantees apply. The test keeps assertions focused on visible
        behaviors rather than implementation internals.
        """
        root_dir: str = tempfile.mkdtemp(prefix=TEST_DIR_PREFIX)
        mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
        mgr._validate_dirs()

        # Create an unlocked file in used/ to simulate a crashed process
        socket_name: str = "ssalvage"
        used_path: str = os.path.join(mgr._used_dir, socket_name)
        with open(used_path, "w", encoding="utf-8"):
            pass

        lease: ConnectionLease = mgr.acquire()
        try:
            self.assertEqual(socket_name, os.path.basename(lease.socket_path))
            # The path should remain in used/ during the lease
            self.assertTrue(os.path.exists(lease.used_path))
            # Another lock attempt should fail while we hold the lease
            other = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
            self.assertIsNone(other._try_lock(lease.used_path, open_flags=os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC))
        finally:
            lease.release()

    def test_acquire_creates_when_empty_and_obeys_cap(self) -> None:
        """Purpose: Confirm that the manager creates a new, well-formed ControlPath when both ``free/`` and ``used/``
        are empty, and enforces the Unix domain socket path length cap to remain compatible with portable kernels.

        Assumptions: The temporary root is empty and writable, the OS path length limit from the utility module closely
        matches the platform's effective limit, and base32 encoding avoids dangerous characters. Only the current
        process interacts with the directory.

        Design Rationale: Creation is a critical fallback that must be correct on first attempt without retries or
        partial files. The test asserts a recognizable "s" prefix, presence in ``used/`` while held, and length not
        exceeding the configured cap. This keeps the test fast, deterministic, and independent from system SSH
        behavior, while still validating correctness constraints that impact real-world stability.
        """
        root_dir: str = tempfile.mkdtemp(prefix=TEST_DIR_PREFIX)
        mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="abc", log=MagicMock(logging.Logger))

        lease: ConnectionLease = mgr.acquire()
        try:
            self.assertTrue(os.path.basename(lease.socket_path).startswith("s"))
            self.assertTrue(os.path.exists(lease.used_path))
            self.assertLessEqual(len(lease.socket_path), UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH)
        finally:
            lease.release()

    def test_salvage_from_used_after_crash(self) -> None:
        """Purpose: Exercise the crash-recovery scenario in which a process that owned a lease terminates abruptly,
        closing the file descriptor and implicitly dropping the lock while leaving the file in ``used/``. A subsequent
        acquisition should salvage the exact same ControlPath and paths.

        Assumptions: Closing the file descriptor simulates the crash accurately; no other process intervenes. Atomic
        renames and locks behave per POSIX. The test can create and manipulate temporary directories safely.

        Design Rationale: By intentionally closing the fd, we model the authoritative signal of ownership loss without
        introducing timing races or external dependencies. Assertions compare all three paths for equality to rule out
        accidental re-creation and to confirm a true salvage. Final checks ensure that releasing the salvaged lease
        returns the file to ``free/``, leaving the directory tree in a clean, reusable state for subsequent tests.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = ConnectionLeaseManager(root_dir=tmpdir, namespace="user@host#22#cfg", log=MagicMock(logging.Logger))
            lease1: ConnectionLease = mgr.acquire()
            # Crash simulation: drop the flock but leave the file in used/
            os.close(lease1.fd)

            # Next acquisition should salvage from locked/
            lease2: ConnectionLease = mgr.acquire()
            try:
                # Salvage should return the identical ControlPath/lock file name
                self.assertEqual(lease1.socket_path, lease2.socket_path)
                self.assertEqual(lease1.used_path, lease2.used_path)
                self.assertEqual(lease1.free_path, lease2.free_path)
                self.assertTrue(os.path.isfile(lease2.used_path))
            finally:
                lease2.release()
                self.assertTrue(os.path.isfile(lease2.free_path))
                self.assertFalse(os.path.exists(lease2.used_path))

    def test_release_ignores_missing_used_path_and_closes_fd(self) -> None:
        """Acquire a lease and then remove its used_path to simulate external deletion; Call release() and ensure no
        exception and the fd is closed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = ConnectionLeaseManager(root_dir=tmpdir, namespace="ns", log=MagicMock(logging.Logger))
            lease: ConnectionLease = mgr.acquire()
            # Remove the file to force FileNotFoundError in os.replace during release
            os.remove(lease.used_path)
            # Should not raise; should still close the fd
            lease.release()
            with self.assertRaises(OSError):
                os.fstat(lease.fd)  # fd must be closed

    def test_find_and_acquire_skips_non_matching_entries(self) -> None:
        """Ensures junk files and non-files are ignored and _try_lock is not invoked."""
        root_dir: str = tempfile.mkdtemp(prefix=TEST_DIR_PREFIX)
        mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
        mgr._validate_dirs()

        # Create entries that must be skipped by the scanner
        junk_file = os.path.join(mgr._free_dir, "junk")  # does not start with 's'
        with open(junk_file, "w", encoding="utf-8"):
            pass
        os.mkdir(os.path.join(mgr._free_dir, "sdir"))  # starts with 's' but is a directory

        # Ensure _try_lock is not called for skipped entries
        with patch.object(mgr, "_try_lock", side_effect=AssertionError("_try_lock should not be called")):
            self.assertIsNone(mgr._find_and_acquire(mgr._free_dir))

    def test_create_and_acquire_retries_on_first_lock_failure(self) -> None:
        """BlockingIOError from flock; second attempt must succeed and return a lease."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = ConnectionLeaseManager(root_dir=tmpdir, namespace="ns", log=MagicMock(logging.Logger))

            # First flock call fails with BlockingIOError, second succeeds
            call_sequence: list[Exception | None] = [BlockingIOError(), None]

            def fake_flock(fd: int, flags: int) -> None:
                result = call_sequence.pop(0)
                if isinstance(result, Exception):
                    raise result

            with patch("bzfs_main.connection_lease.fcntl.flock", side_effect=fake_flock):
                lease = mgr.acquire()
                try:
                    self.assertTrue(os.path.exists(lease.used_path))
                finally:
                    lease.release()

    def test_try_lock_file_not_found_calls_validate_and_returns_none(self) -> None:
        """Calling _try_lock on a path whose parent does not exist must call _validate_dirs() and return None."""
        root_dir: str = tempfile.mkdtemp(prefix=TEST_DIR_PREFIX)
        mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
        with patch.object(mgr, "_validate_dirs") as mock_validate:
            missing_parent = os.path.join(mgr._free_dir, "missing", "file")
            result = mgr._try_lock(missing_parent, open_flags=os.O_WRONLY)
            self.assertIsNone(result)
            self.assertTrue(mock_validate.called)

    def test_try_lock_other_oserror_is_raised(self) -> None:
        """Simulate a non-BlockingIOError, non-FileNotFoundError (e.g., PermissionError) from os.open and assert it is re-
        raised by _try_lock."""
        root_dir: str = tempfile.mkdtemp(prefix=TEST_DIR_PREFIX)
        mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
        with patch("bzfs_main.connection_lease.os.open", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError):
                _ = mgr._try_lock(os.path.join(mgr._free_dir, "sabc"), open_flags=os.O_WRONLY)


#############################################################################
class TestSshExitOnShutdown(AbstractTestCase):

    @patch("bzfs_main.connection.subprocess.run")
    def test_ssh_exit_on_shutdown_true_triggers_exit(self, mock_run: MagicMock) -> None:
        """Purpose: Validate that when ``--ssh-exit-on-shutdown`` is enabled, the connection shutdown triggers an SSH
        ControlMaster administrative ``-O exit`` request, asking the master to terminate immediately once idle.

        Assumptions: The test substitutes ``subprocess.run`` with a mock to avoid invoking real SSH. The remote's
        configuration has exit-on-shutdown enabled and does not attempt ControlPath reuse, so a targeted control
        command is appropriate. The CLI argument parser populates the parameter object correctly.

        Design Rationale: Inspecting the captured argv is the most reliable and fast way to validate orchestration
        logic without requiring a live SSH daemon. The test focuses on the presence of ``-O`` and ``exit`` flags rather
        than full command equivalence to remain resilient to harmless option reordering. This creates a stable,
        low-flake assertion that still proves the intended behavior is wired end-to-end.
        """

        args = self.argparser_parse_args(["src", "dst", "--ssh-exit-on-shutdown"])
        p = self.make_params(args=args)
        r = cast(
            Remote,
            _FakeRemote(
                params=p,
                location="src",
                ssh_user_host="host",
                reuse_ssh_connection=True,
                ssh_exit_on_shutdown=True,
                ssh_extra_opts=[],
            ),
        )
        conn: Connection = Connection(r, 1, 0)
        conn.shutdown("test", p)
        self.assertTrue(mock_run.called)
        argv = mock_run.call_args[0][0]
        self.assertIn("-O", argv)
        self.assertIn("exit", argv)

    @patch("bzfs_main.connection.subprocess.run")
    def test_ssh_exit_on_shutdown_false_skips_exit(self, mock_run: MagicMock) -> None:
        """Purpose: Ensure that the default behavior, and explicit disabling of immediate exit, avoids sending the
        ``-O exit`` administrative command to an SSH ControlMaster during shutdown. Reusable masters should persist to
        amortize connection setup costs across jobs.

        Assumptions: The test runs with ``reuse_ssh_connection=True`` and without the ``--ssh-exit-on-shutdown`` flag.
        ``subprocess.run`` is mocked to prevent external effects and to make call absence observable. Socket directories
        are created in a temporary location and not shared with other tests.

        Design Rationale: Negative tests guard against accidental regressions that would prematurely tear down masters,
        harming performance and stability. By simply asserting the mock was not called, the test remains robust and
        independent of incidental command formatting or environment. This keeps the specification crisp: in this mode,
        shutdown should be a no-op with respect to master termination.
        """
        args = self.argparser_parse_args(["src", "dst"])  # default is to NOT exit masters immediately
        p = self.make_params(args=args)
        r = cast(
            Remote,
            _FakeRemote(
                params=p,
                location="src",
                ssh_user_host="host",
                reuse_ssh_connection=True,
                ssh_exit_on_shutdown=False,
                ssh_extra_opts=[],
                ssh_socket_dir=tempfile.mkdtemp(prefix=TEST_DIR_PREFIX),
            ),
        )
        conn: Connection = Connection(r, 1, 0)
        conn.shutdown("test", p)
        # No call to run expected when immediate exit is false
        self.assertFalse(mock_run.called)
