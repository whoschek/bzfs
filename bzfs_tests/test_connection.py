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
from bzfs_main.retry import (
    RetryableError,
)
from bzfs_main.utils import (
    LOG_TRACE,
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
        TestSshExitOnShutdown,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


# constants:
TEST_DIR_PREFIX: str = "t_bzfs"


#############################################################################
class SlowButCorrectConnectionPool(ConnectionPool):  # validate a better implementation against this baseline

    def __init__(self, remote: Remote, max_concurrent_ssh_sessions_per_tcp_connection: int) -> None:
        super().__init__(remote, max_concurrent_ssh_sessions_per_tcp_connection, SHARED)
        self._priority_queue: list[Connection] = []  # type: ignore

    def get_connection(self) -> Connection:
        with self._lock:
            self._priority_queue.sort()
            conn = self._priority_queue[-1] if self._priority_queue else None
            if conn is None or conn.is_full():
                conn = Connection(self._remote, self._capacity, self._cid, SHARED)
                self._last_modified += 1
                conn.update_last_modified(self._last_modified)  # LIFO tiebreaker favors latest conn as that's most alive
                self._cid += 1
                self._priority_queue.append(conn)
            conn.increment_free(-1)
            return conn

    def return_connection(self, old_conn: Connection) -> None:
        assert old_conn is not None
        with self._lock:
            assert any(old_conn is c for c in self._priority_queue)
            old_conn.increment_free(1)
            self._last_modified += 1
            old_conn.update_last_modified(self._last_modified)

    def __repr__(self) -> str:
        with self._lock:
            return str({"capacity": self._capacity, "queue_len": len(self._priority_queue), "queue": self._priority_queue})


#############################################################################
class TestConnectionPool(AbstractTestCase):

    def setUp(self) -> None:
        args = self.argparser_parse_args(args=["src", "dst", "-v", "--ssh-exit-on-shutdown"])
        p = self.make_params(args=args)
        self.src = p.src
        self.dst = p.dst
        self.dst.ssh_user_host = "127.0.0.1"
        self.remote = Remote("src", args, p)
        self.src2 = Remote("src", args, p)

    def assert_priority_queue(self, cpool: ConnectionPool, queuelen: int) -> None:
        self.assertEqual(queuelen, len(cpool._priority_queue))

    def assert_equal_connections(self, conn: Connection, donn: Connection) -> None:
        self.assertTupleEqual((donn._cid, donn.ssh_cmd), (conn._cid, conn.ssh_cmd))
        self.assertEqual(donn._free, conn._free)
        self.assertEqual(donn._last_modified, conn._last_modified)

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
        self.assertTupleEqual((donn._cid, donn.ssh_cmd), (conn._cid, conn.ssh_cmd))
        cpool.return_connection(conn)
        dpool.return_connection(donn)

    def test_basic(self) -> None:
        counter1a = itertools.count()
        counter2a = itertools.count()
        counter1b = itertools.count()
        self.src.local_ssh_command = lambda _socket_path=None, counter=counter1a: [str(next(counter))]  # type: ignore
        self.src2.local_ssh_command = lambda _socket_path=None, counter=counter1b: [str(next(counter))]  # type: ignore

        with self.assertRaises(AssertionError):
            ConnectionPool(self.src, 0, SHARED)

        capacity = 2
        cpool = ConnectionPool(self.src, capacity, SHARED)
        dpool = SlowButCorrectConnectionPool(self.src2, capacity)
        self.assert_priority_queue(cpool, 0)
        self.assertIsNotNone(repr(cpool))
        self.assertIsNotNone(str(cpool))
        cpool.shutdown("foo")

        conn1, donn1 = self.get_connection(cpool, dpool)
        self.assert_priority_queue(cpool, 1)
        self.assertEqual((capacity - 1) * 1, conn1._free)
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
        self.assertEqual((capacity - 1) * 1, conn2._free)

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
        cpool = ConnectionPool(self.remote, capacity, SHARED)

        conn1 = cpool.get_connection()
        self.assertEqual((capacity - 1) * 1, conn1._free)
        conn2 = cpool.get_connection()
        self.assertIs(conn1, conn2)
        self.assertEqual((capacity - 2) * 1, conn2._free)
        self.assert_priority_queue(cpool, 1)

        conn3 = cpool.get_connection()
        self.assertIsNot(conn2, conn3)
        self.assertEqual((capacity - 1) * 1, conn3._free)
        self.assertEqual((capacity - 2) * 1, conn2._free)
        self.assert_priority_queue(cpool, 2)
        conn4 = cpool.get_connection()
        self.assertIs(conn3, conn4)
        self.assertEqual((capacity - 2) * 1, conn4._free)
        self.assert_priority_queue(cpool, 2)

        conn5 = cpool.get_connection()
        self.assertIsNot(conn4, conn5)
        self.assertEqual((capacity - 1) * 1, conn5._free)
        self.assertEqual((capacity - 2) * 1, conn4._free)
        self.assert_priority_queue(cpool, 3)

        cpool.return_connection(conn3)
        self.assert_priority_queue(cpool, 3)
        cpool.return_connection(conn4)
        t4 = cpool._last_modified
        self.assert_priority_queue(cpool, 3)

        cpool.return_connection(conn2)
        self.assert_priority_queue(cpool, 3)
        cpool.return_connection(conn1)
        t2 = cpool._last_modified
        self.assert_priority_queue(cpool, 3)

        cpool.return_connection(conn5)
        t5 = cpool._last_modified
        self.assert_priority_queue(cpool, 3)

        # assert sort order evens out the number of concurrent sessions among the TCP connections
        conn5a = cpool.get_connection()
        self.assertEqual((capacity - 1) * 1, conn5a._free)

        conn2a = cpool.get_connection()
        self.assertEqual((capacity - 1) * 1, conn2a._free)

        conn4a = cpool.get_connection()
        self.assertEqual((capacity - 1) * 1, conn4a._free)

        # assert tie-breaker in favor of most recently returned TCP connection
        conn6 = cpool.get_connection()
        self.assertEqual((capacity - 2) * 1, conn6._free)
        self.assertEqual(t5, abs(conn6._last_modified))

        conn1a = cpool.get_connection()
        self.assertEqual((capacity - 2) * 1, conn1a._free)
        self.assertEqual(t2, abs(conn1a._last_modified))

        conn4a = cpool.get_connection()
        self.assertEqual((capacity - 2) * 1, conn4a._free)
        self.assertEqual(t4, abs(conn4a._last_modified))

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
            cpool = ConnectionPool(self.src, maxsessions, SHARED)
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
                cpool = ConnectionPool(self.src, maxsessions, SHARED)
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
                            log.log(loglevel, f"clen: {len(cpool._priority_queue)}, cpool: {cpool._priority_queue}")
                            log.log(loglevel, f"dlen: {len(dpool._priority_queue)}, dpool: {dpool._priority_queue}")
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
                    logging.getLogger(__name__).warning("Ooops!")
                    logging.getLogger(__name__).warning(
                        f"maxsessions: {maxsessions}, items: {items}, step: {step}, item: {item}"
                    )
                    logging.getLogger(__name__).warning(
                        f"clen: {len(cpool._priority_queue)}, cpool: {cpool._priority_queue}"
                    )
                    logging.getLogger(__name__).warning(
                        f"dlen: {len(dpool._priority_queue)}, dpool: {dpool._priority_queue}"
                    )
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
                ssh_exit_on_shutdown=True,
                ssh_extra_opts=[],
            ),
        )
        self.conn = connection.Connection(self.remote, 1, 0, SHARED)
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

    @patch("bzfs_main.connection.subprocess_run")
    def test_master_start_failure_is_retryable(self, mock_run: MagicMock) -> None:
        # First call: '-O check' returns non-zero, indicating master not alive.
        # Second call: starting master with check=True raises CalledProcessError.
        mock_run.side_effect = [
            MagicMock(returncode=1),
            subprocess.CalledProcessError(returncode=1, cmd="ssh", stderr="bad"),
        ]
        with self.assertRaises(RetryableError):
            connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)

    @patch("bzfs_main.connection.subprocess_run")
    def test_verbose_option_limits_persist_time(self, mock_run: MagicMock) -> None:
        self.remote.ssh_extra_opts = ["-v"]
        mock_run.side_effect = [MagicMock(returncode=1), MagicMock(returncode=0)]
        connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        args = mock_run.call_args_list[1][0][0]
        self.assertIn("-oControlPersist=1s", args)

    @patch("bzfs_main.connection_lease.ConnectionLease.set_socket_mtime_to_now")
    @patch("bzfs_main.connection.subprocess_run")
    def test_mtime_now_invoked_when_connection_lease_present(self, mock_run: MagicMock, mock_utime: MagicMock) -> None:
        """Ensures set_socket_mtime_to_now() is called when a Connection has a lease (socket path).

        Uses a remote with ssh_exit_on_shutdown=False so Connection acquires a lease in __init__. Mocks subprocess_run to the
        fast 'alive' path (returncode=0).
        """
        mock_run.return_value = MagicMock(returncode=0)
        with get_tmpdir() as ssh_socket_dir:
            remote = cast(
                Remote,
                _FakeRemote(
                    params=self.job.params,
                    location="dst",
                    ssh_user_host="host",
                    reuse_ssh_connection=True,
                    ssh_control_persist_secs=4,
                    ssh_exit_on_shutdown=False,  # acquire lease
                    ssh_extra_opts=[],
                    ssh_socket_dir=ssh_socket_dir,
                ),
            )
            conn = connection.Connection(remote, 1, 0, SHARED)
            try:
                conn.last_refresh_time = 0  # avoid fast return
                connection.refresh_ssh_connection_if_necessary(self.job, remote, conn)
                self.assertTrue(mock_utime.called)
                self.assertIsNotNone(conn.connection_lease)
                mock_utime.assert_called_once()
            finally:
                conn.shutdown("test", self.job.params)

    @patch("bzfs_main.connection_lease.ConnectionLease.set_socket_mtime_to_now")
    @patch("bzfs_main.connection.subprocess_run")
    def test_mtime_now_not_invoked_when_no_connection_lease(self, mock_run: MagicMock, mock_utime: MagicMock) -> None:
        """Ensures set_socket_mtime_to_now() is not called when a Connection has no lease (lease is None)."""
        mock_run.return_value = MagicMock(returncode=0)
        self.conn.last_refresh_time = 0  # avoid fast return
        # In setUp, ssh_exit_on_shutdown=True ensures no lease is acquired.
        self.assertIsNone(self.conn.connection_lease)
        connection.refresh_ssh_connection_if_necessary(self.job, self.remote, self.conn)
        mock_utime.assert_not_called()


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

        args = self.argparser_parse_args(["src", "dst"])
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
        conn: Connection = Connection(r, 1, 0, SHARED)
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
        with get_tmpdir() as ssh_socket_dir:
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
                    ssh_control_persist_secs=90,
                    ssh_extra_opts=[],
                    ssh_socket_dir=ssh_socket_dir,
                ),
            )
            conn: Connection = Connection(r, 1, 0, SHARED)
            conn.shutdown("test", p)
            # No call to run expected when immediate exit is false
            self.assertFalse(mock_run.called)


def get_tmpdir() -> tempfile.TemporaryDirectory[str]:
    # on OSX the default test tmp dir is unreasonably long, e.g.
    # /var/folders/dj/y5mjnplj0l79dq4dkknbsjbh0000gp/T/tmp7j51fpp/
    root_dir = "/tmp" if platform.system() == "Darwin" else None
    return tempfile.TemporaryDirectory(dir=root_dir)
