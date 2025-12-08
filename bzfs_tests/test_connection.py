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
import shlex
import subprocess
import sys
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
)
from bzfs_main.configuration import (
    Params,
    Remote,
)
from bzfs_main.util import (
    connection,
)
from bzfs_main.util.connection import (
    DEDICATED,
    SHARED,
    Connection,
    ConnectionPool,
    ConnectionPools,
    dquote,
    squote,
)
from bzfs_main.util.retry import (
    RetryableError,
)
from bzfs_main.util.utils import (
    LOG_TRACE,
    sha256_urlsafe_base64,
)
from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)
from bzfs_tests.tools import (
    suppress_output,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestSimpleMiniRemote,
        TestSimpleMiniJob,
        TestQuoting,
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
        super().__init__(remote, SHARED, max_concurrent_ssh_sessions_per_tcp_connection)
        self._priority_queue: list[Connection] = []  # type: ignore

    def get_connection(self) -> Connection:
        with self._lock:
            self._priority_queue.sort()
            conn = self._priority_queue[-1] if self._priority_queue else None
            if conn is None or conn._is_full():
                conn = Connection(self._remote, self._capacity)
                self._last_modified += 1
                conn._update_last_modified(self._last_modified)  # LIFO tiebreaker favors latest conn as that's most alive
                self._priority_queue.append(conn)
            conn._increment_free(-1)
            return conn

    def return_connection(self, old_conn: Connection) -> None:
        assert old_conn is not None
        with self._lock:
            assert any(old_conn is c for c in self._priority_queue)
            old_conn._increment_free(1)
            self._last_modified += 1
            old_conn._update_last_modified(self._last_modified)

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
        self.assertEqual(donn._ssh_cmd, conn._ssh_cmd)
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
        self.assertEqual(donn._ssh_cmd, conn._ssh_cmd)
        cpool.return_connection(conn)
        dpool.return_connection(donn)

    def test_basic(self) -> None:
        counter1a = itertools.count()
        counter2a = itertools.count()
        counter1b = itertools.count()
        self.src.local_ssh_command = lambda _socket_path=None, counter=counter1a: [str(next(counter))]  # type: ignore
        self.src2.local_ssh_command = lambda _socket_path=None, counter=counter1b: [str(next(counter))]  # type: ignore

        with self.assertRaises(AssertionError):
            ConnectionPool(self.src, SHARED, 0)

        capacity = 2
        cpool = ConnectionPool(self.src, SHARED, capacity)
        dpool = SlowButCorrectConnectionPool(self.src2, capacity)
        self.assert_priority_queue(cpool, 0)
        self.assertIsNotNone(repr(cpool))
        self.assertIsNotNone(str(cpool))
        cpool.shutdown("foo")

        conn1, donn1 = self.get_connection(cpool, dpool)
        self.assert_priority_queue(cpool, 1)
        self.assertEqual((capacity - 1) * 1, conn1._free)
        i = [str(next(counter2a))]
        self.assertEqual(i, conn1._ssh_cmd)
        self.assertEqual(i, conn1._ssh_cmd_quoted)
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

    def test_pool_acquires_lease_when_reuse_enabled(self) -> None:
        """When SSH reuse is enabled, the pool creates a single manager and acquires a lease for new connections."""
        with get_tmpdir() as ssh_socket_dir:
            args = self.argparser_parse_args(["src", "dst"])  # do not set --ssh-exit-on-shutdown
            p = self.make_params(args=args)
            remote = cast(
                Remote,
                _FakeRemote(
                    params=p,
                    location="dst",
                    ssh_user_host="host",
                    reuse_ssh_connection=True,
                    ssh_exit_on_shutdown=False,
                    ssh_control_persist_secs=90,
                    ssh_extra_opts=[],
                    ssh_socket_dir=ssh_socket_dir,
                ),
            )
            cpool = ConnectionPool(remote, SHARED, 1)
            try:
                conn = cpool.get_connection()
                self.assertIsNotNone(cpool._lease_mgr)
                self.assertIsNotNone(conn._connection_lease)
            finally:
                conn.shutdown("test")

    def test_multiple_tcp_connections(self) -> None:
        capacity = 2
        cpool = ConnectionPool(self.remote, SHARED, capacity)

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
            cpool = ConnectionPool(self.src, SHARED, maxsessions)
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
        num_steps = (
            25 if self.is_unit_test else 75 if self.is_smoke_test or self.is_functional_test or self.is_adhoc_test else 1000
        )
        log.info(f"num_random_steps: {num_steps}")
        start_time_nanos = time.time_ns()
        for maxsessions in range(1, 10 + 1):
            for items in range(64 + 1):
                counter1a = itertools.count()
                counter1b = itertools.count()
                self.src.local_ssh_command = lambda _socket_path=None, counter=counter1a: [str(next(counter))]  # type: ignore
                self.src2.local_ssh_command = lambda _socket_path=None, counter=counter1b: [str(next(counter))]  # type: ignore
                cpool = ConnectionPool(self.src, SHARED, maxsessions)
                dpool = SlowButCorrectConnectionPool(self.src2, maxsessions)
                # dpool = ConnectionPool(self.src2, SHARED, maxsessions)
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
                            if is_logging:
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
class TestSimpleMiniRemote(AbstractTestCase):
    """Covers all control-flow branches in SimpleMiniRemote."""

    def setUp(self) -> None:
        self.log = MagicMock(logging.Logger)

    def test_init_builds_expected_options_defaults(self) -> None:
        r = connection.create_simple_miniremote(log=self.log, ssh_user_host="")
        self.assertEqual(
            ["-oBatchMode=yes", "-oServerAliveInterval=0", "-x", "-T", "-c", "^aes256-gcm@openssh.com"],
            list(r.ssh_extra_opts),
        )
        # SimpleMiniRemote always disables immediate master exit on shutdown
        self.assertFalse(r.ssh_exit_on_shutdown)
        self.assertEqual("-", r.cache_namespace())  # local mode
        self.assertEqual([], r.local_ssh_command(socket_file=None))

    def test_init_with_all_extras_and_port(self) -> None:
        with get_tmpdir() as tmpdir:
            with tempfile.NamedTemporaryFile(dir=tmpdir, delete=False) as cfg:
                cfg_path = cfg.name
            r = connection.create_simple_miniremote(
                log=self.log,
                ssh_user_host="user@host",
                ssh_port=2222,
                ssh_verbose=True,
                ssh_config_file=cfg_path,
                ssh_cipher="aes128-gcm@openssh.com",
                reuse_ssh_connection=True,
            )
            # Verify options order and presence
            expected_prefix = [
                "-oBatchMode=yes",
                "-oServerAliveInterval=0",
                "-x",
                "-T",
                "-v",
                "-F",
                cfg_path,
                "-c",
                "aes128-gcm@openssh.com",
                "-p",
                "2222",
            ]
            self.assertEqual(expected_prefix, list(r.ssh_extra_opts))
            # cache namespace includes user@host, port and cfg hash
            expected_hash = sha256_urlsafe_base64(os.path.abspath(cfg_path), padding=False)
            self.assertEqual(f"user@host#2222#{expected_hash}", r.cache_namespace())
            # local_ssh_command includes -S only when socket provided
            cmd_no_sock = r.local_ssh_command(socket_file=None)
            self.assertEqual([r.params.ssh_program] + expected_prefix + ["user@host"], cmd_no_sock)
            cmd_with_sock = r.local_ssh_command(socket_file="/tmp/sock")
            self.assertEqual(
                [r.params.ssh_program] + expected_prefix + ["-S", "/tmp/sock", "user@host"],
                cmd_with_sock,
            )

    def test_init_with_custom_extra_opts_is_copied(self) -> None:
        custom = ["-K"]
        r = connection.create_simple_miniremote(
            log=self.log,
            ssh_user_host="user@h",
            ssh_extra_opts=custom,
            ssh_verbose=True,
            ssh_cipher="^aes256-gcm@openssh.com",
        )
        # The instance copies and extends the provided list, leaving the caller's list untouched.
        self.assertIsNot(custom, r.ssh_extra_opts)
        self.assertEqual(["-K", "-v", "-c", "^aes256-gcm@openssh.com"], list(r.ssh_extra_opts))
        self.assertListEqual(["-K"], custom)

    def test_no_cipher_no_config_no_port(self) -> None:
        r = connection.create_simple_miniremote(
            log=self.log,
            ssh_user_host="user@h",
            ssh_cipher="",  # do not include -c
            ssh_config_file="",  # do not include -F
            ssh_port=None,  # do not include -p
        )
        opts = r.ssh_extra_opts
        self.assertNotIn("-c", opts)
        self.assertNotIn("-F", opts)
        self.assertNotIn("-p", opts)
        self.assertEqual("user@h##", r.cache_namespace())

    def test_invalid_parameters_raise_valueerror(self) -> None:
        # ssh_control_persist_secs must be >= 1
        with self.assertRaises(ValueError):
            connection.create_simple_miniremote(log=self.log, ssh_control_persist_secs=0)
        # location must be 'src' or 'dst'
        with self.assertRaises(ValueError):
            connection.create_simple_miniremote(log=self.log, location="invalid")

    def test_invalid_ssh_program_raises_valueerror(self) -> None:
        with self.assertRaises(ValueError):
            connection.create_simple_miniremote(log=self.log, ssh_user_host="user@h", ssh_program="")

    def test_none_log_raises_valueerror(self) -> None:
        with self.assertRaises(ValueError):
            connection.create_simple_miniremote(log=cast(logging.Logger, None), ssh_user_host="user@h")

    def test_local_ssh_command_branching(self) -> None:
        r = connection.create_simple_miniremote(log=self.log, ssh_user_host="user@h", reuse_ssh_connection=False)
        # No -S because reuse is False
        self.assertEqual([r.params.ssh_program] + list(r.ssh_extra_opts) + ["user@h"], r.local_ssh_command("/tmp/s"))

    def test_factory_method_round_trips_arguments(self) -> None:
        r = connection.create_simple_miniremote(
            log=self.log,
            ssh_user_host="user@host",
            ssh_port=2222,
            ssh_verbose=True,
        )
        self.assertEqual("user@host", r.ssh_user_host)
        self.assertEqual(2222, cast(Any, r).ssh_port)

    def test_simple_mini_remote_is_ssh_available_always_true(self) -> None:
        """Ensure SimpleMiniRemote.is_ssh_available always reports True."""
        r = connection.create_simple_miniremote(log=self.log, ssh_user_host="user@host")
        self.assertTrue(r.is_ssh_available())


#############################################################################
class TestSimpleMiniJob(AbstractTestCase):
    """Smoke tests for SimpleMiniJob wiring to ensure attributes are set correctly."""

    def test_initialization_defaults(self) -> None:
        j = connection.create_simple_minijob()
        self.assertIsNone(j.timeout_nanos)
        self.assertIsNone(j.timeout_duration_nanos)
        self.assertIsNotNone(j.subprocesses)

    @patch("bzfs_main.util.connection.time.monotonic_ns", return_value=1_000_000_000)
    def test_timeout_fields_from_duration(self, mock_monotonic: MagicMock) -> None:
        j = connection.create_simple_minijob(timeout_duration_secs=0.987654321)
        self.assertEqual(987654321, j.timeout_duration_nanos)
        self.assertEqual(1_000_000_000 + 987_654_321, j.timeout_nanos)
        mock_monotonic.assert_called_once_with()


#############################################################################
class TestQuoting(AbstractTestCase):
    """Covers quoting helpers used across modules."""

    def test_squote_local_no_quote(self) -> None:
        remote = MagicMock(ssh_user_host="")
        self.assertEqual("foo bar", squote(remote, "foo bar"))

    def test_squote_remote_quote(self) -> None:
        remote = MagicMock(ssh_user_host="host")
        self.assertEqual("'foo bar'", squote(remote, "foo bar"))

    def test_dquote_empty_string(self) -> None:
        """Empty arguments are quoted without escaping."""
        self.assertEqual('""', dquote(""))

    def test_dquote_preserves_single_quotes_and_spaces(self) -> None:
        """Single quotes and spaces remain untouched inside the quotes."""
        self.assertEqual("\"foo 'bar baz' qux\"", dquote("foo 'bar baz' qux"))

    def test_dquote_multiple_specials(self) -> None:
        """Every occurrence of special characters is escaped."""
        cases = [
            ('""', '"' + '\\"' * 2 + '"'),
            ("$$$", '"' + "\\$" * 3 + '"'),
            ("``", '"' + "\\`" * 2 + '"'),
        ]
        for arg, expected in cases:
            with self.subTest(arg=arg):
                self.assertEqual(expected, dquote(arg))

    def test_dquote_prevents_command_substitution(self) -> None:
        """Escaping ensures the shell prints literals rather than executing."""
        for arg in ["$(echo hacked)", "`echo hacked`"]:
            with self.subTest(arg=arg):
                self.assertEqual(arg, self._sh_roundtrip(arg))

    def test_dquote_roundtrip_empty_string(self) -> None:
        """Ensures empty arguments survive shell evaluation unchanged."""
        arg = ""
        self.assertEqual(arg, self._sh_roundtrip(arg))

    def test_dquote_roundtrip_with_spaces(self) -> None:
        """Verifies that spaces are preserved when passed through a shell."""
        arg = "foo bar baz"
        self.assertEqual(arg, self._sh_roundtrip(arg))

    def test_dquote_multiple_instances(self) -> None:
        """All repeated special chars must be escaped and round-trip."""
        arg = 'aa""bb$$cc``dd'
        quoted = dquote(arg)
        self.assertEqual(2, quoted.count('\\"'))
        self.assertEqual(2, quoted.count("\\$"))
        self.assertEqual(2, quoted.count("\\`"))
        self.assertEqual(arg, self._sh_roundtrip(arg))

    def test_dquote_preserves_unrelated_chars(self) -> None:
        """Characters outside the escape set remain intact after quoting."""
        arg = "path/with 'single' and \\backslash"
        self.assertEqual(arg, self._sh_roundtrip(arg))

    def test_dquote_preserves_dollars_backticks_quotes(self) -> None:
        samples = [
            r"a$b",  # $ should not expand
            r"a`uname`b",  # backticks should not execute
            r'a"b',  # embedded double quote
            r"plain",  # trivial
        ]
        for s in samples:
            with self.subTest(s=s):
                self.assertEqual(s, self._sh_roundtrip(s))

    def test_dquote_handles_spaces_and_newlines(self) -> None:
        samples = [
            "a b c",  # spaces must remain a single arg
            "line1\nline2",  # newline stays literal
            " tab\tsep ",  # tabs preserved
        ]
        for s in samples:
            with self.subTest(s=s):
                self.assertEqual(s, self._sh_roundtrip(s))

    def test_dquote_does_not_require_backslash_escaping(self) -> None:
        samples = [
            r"a\b",  # normal backslash
        ]
        for s in samples:
            with self.subTest(s=s):
                self.assertEqual(s, self._sh_roundtrip(s))

    def test_dquote_globally_escapes_specials(self) -> None:
        s = "pre 'a\"b $USER `uname` \\ tail' post"
        out = dquote(s)
        payload = out[1:-1]  # strip outer double quotes

        # Every occurrence of ", $, ` in the payload must be backslash-escaped
        specials = {'"', "$", "`"}
        for i, ch in enumerate(payload):
            with self.subTest(i=i):
                if ch in specials:
                    self.assertTrue(i > 0)
                    self.assertEqual("\\", payload[i - 1], f"unescaped {ch!r} at index {i}")

    @staticmethod
    def _sh_roundtrip(arg: str) -> str:
        quoted = dquote(arg)
        return subprocess.run(["sh", "-c", f"printf %s {quoted}"], stdout=subprocess.PIPE, text=True, check=True).stdout

    @staticmethod
    def _nested_sh_roundtrip(arg: str) -> str:
        """Simulates nested 'sh -c' usage for dquote as in replication pipelines.

        Builds a command list with ``arg`` as the final argv element, joins it via ``shlex.join``, wraps it with ``dquote``
        and executes it through an outer ``sh -c`` that invokes an inner ``sh -c``. Returns the string observed by the inner
        Python process so tests can assert end-to-end preservation.
        """
        recv_cmd: list[str] = [sys.executable, "-c", "import sys; print(sys.argv[-1])", arg]
        recv_cmd_str: str = shlex.join(recv_cmd)
        script: str = dquote(recv_cmd_str)
        stdout: str = subprocess.run(["sh", "-c", f"sh -c {script}"], stdout=subprocess.PIPE, text=True, check=True).stdout
        return stdout.rstrip("\n")

    def test_dquote_backslash_before_quote(self) -> None:
        """Backslashes immediately before a quote survive shell evaluation."""
        arg = r"a\"b"
        self.assertEqual(arg, self._sh_roundtrip(arg))

    def test_dquote_escapes(self) -> None:
        self.assertEqual('"a\\"b\\$c\\`d"', dquote('a"b$c`d'))

    def test_dquote_nested_sh_backslash_backtick(self) -> None:
        """Nested sh -c round-trips values with backslash+backtick."""
        value = r"/tmp/foo\`bar"
        prop_arg = f"prop={value}"
        self.assertEqual(prop_arg, self._nested_sh_roundtrip(prop_arg))

    def test_dquote_preserves_double_backslashes_in_nested_sh(self) -> None:
        """Ensures nested sh -c round-trips values containing double backslashes."""
        samples = [
            r"\\server\share",  # UNC-style path
            r"\\path\\with\\many",  # multiple segments
        ]
        for value in samples:
            with self.subTest(value=value):
                prop_arg: str = f"foo={value}"
                self.assertEqual(prop_arg, self._nested_sh_roundtrip(prop_arg))


def make_fake_params() -> Params:
    mock = MagicMock(spec=Params)
    mock.log = logging.getLogger(__name__)
    mock.ssh_program = "ssh"
    mock.connection_pools = {}
    return mock


#############################################################################
class _FakeRemote(SimpleNamespace):
    ssh_extra_opts: tuple[str, ...]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if not hasattr(self, "params"):
            self.params = make_fake_params()
        if not hasattr(self, "ssh_extra_opts"):
            self.ssh_extra_opts = ()
        if not hasattr(self, "ssh_socket_dir"):
            self.ssh_socket_dir = tempfile.mkdtemp(prefix=TEST_DIR_PREFIX)
        if not hasattr(self, "ssh_control_persist_margin_secs"):
            self.ssh_control_persist_margin_secs = 2
        if not hasattr(self, "cache_namespace"):
            self.cache_namespace = lambda: "user@host#22#cfg"
        if not hasattr(self, "pool"):
            self.pool = SHARED

    def local_ssh_command(self, socket_path: str | None = None) -> list[str]:
        return ["ssh", self.ssh_user_host or "localhost"]

    def is_ssh_available(self) -> bool:
        return True


#############################################################################
class TestRunSshCommand(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.params = make_fake_params()
        self.job.timeout_nanos = None
        self.remote = cast(
            Remote,
            _FakeRemote(
                location="dst", ssh_user_host="", reuse_ssh_connection=True, ssh_control_persist_secs=90, ssh_extra_opts=[]
            ),
        )
        self.conn_pools = ConnectionPools(self.remote, {SHARED: 1})
        self.conn_pool = self.conn_pools.pool(SHARED)
        self.conn: Connection | None = None

        original_connection = self.conn_pool.connection

        @contextlib.contextmanager
        def _connection_cm(original_connection: Any = original_connection) -> Iterator[Connection]:
            with original_connection() as conn:
                self.conn = conn
                yield conn

        # Wrap connection() so tests can assert it was used.
        self.conn_pool_connection_mock = MagicMock(side_effect=_connection_cm)
        self.conn_pool.connection = self.conn_pool_connection_mock  # type: ignore[method-assign]  # replace context manager with mock
        self.job.params.connection_pools = {"dst": self.conn_pools}

    def test_empty_cmd_raises_value_error(self) -> None:
        job = connection.create_simple_minijob()
        conn = Connection(self.remote, 1)
        with self.assertRaises(ValueError):
            conn.run_ssh_command(cmd=[], job=job)

    @patch("bzfs_main.util.utils.Subprocesses.subprocess_run")
    def test_dry_run_skips_execution(self, mock_run: MagicMock) -> None:
        result = self.job.run_ssh_command(self.remote, cmd=["ls"], is_dry=True)
        self.assertEqual("", result)
        mock_run.assert_not_called()
        self.conn_pool_connection_mock.assert_called_once()

    @patch("bzfs_main.util.connection.Connection.refresh_ssh_connection_if_necessary", autospec=True)
    @patch("bzfs_main.util.utils.Subprocesses.subprocess_run")
    def test_remote_calls_refresh_and_executes(self, mock_run: MagicMock, mock_refresh: MagicMock) -> None:
        self.remote.ssh_user_host = "host"
        self.conn_pool._remote.ssh_user_host = "host"
        mock_run.return_value = MagicMock(stdout="out", stderr="err")
        result = self.job.run_ssh_command(self.remote, cmd=["echo"], print_stdout=True, print_stderr=True)
        self.assertEqual("out", result)
        assert self.conn is not None
        mock_refresh.assert_called_once_with(self.conn, self.job)
        mock_run.assert_called_once()
        self.conn_pool_connection_mock.assert_called_once()

    @patch("bzfs_main.util.connection.Connection.refresh_ssh_connection_if_necessary", autospec=True)
    @patch(
        "bzfs_main.util.utils.Subprocesses.subprocess_run",
        side_effect=subprocess.CalledProcessError(returncode=1, cmd="cmd", output="o", stderr="e"),
    )
    def test_calledprocesserror_propagates(self, mock_run: MagicMock, mock_refresh: MagicMock) -> None:
        self.remote.ssh_user_host = "h"
        self.conn_pool._remote.ssh_user_host = "h"
        with self.assertRaises(subprocess.CalledProcessError):
            self.job.run_ssh_command(self.remote, cmd=["boom"], print_stdout=True)
        mock_run.assert_called_once()
        self.conn_pool_connection_mock.assert_called_once()

    def test_real_ssh_transport_error_raises_retryable_error(self) -> None:
        """Uses the real ssh CLI to trigger a transport error and exercise RetryableError wrapping."""
        # Use real Subprocesses.subprocess_run (no patch) and a host name that cannot resolve.
        self.remote.ssh_user_host = ""
        # Ensure the underlying Connection uses a short timeout for the failing ssh call.
        with self.conn_pool.connection() as conn:
            conn._ssh_cmd = ["ssh", "-T", "-x", "-oBatchMode=yes", "-oConnectTimeout=1"]  # type: ignore[misc]  # override test-only ssh_cmd
        self.conn_pool_connection_mock.reset_mock()

        with self.assertRaises(RetryableError) as cm, suppress_output():
            # Pass an invalid host name as the first argument so ssh treats it as the remote host.
            self.job.run_ssh_command(self.remote, cmd=["nonexistent.invalid"], print_stdout=False, print_stderr=True)

        self.conn_pool_connection_mock.assert_called_once()
        self.assertEqual("ssh", cm.exception.display_msg)
        cause = cm.exception.__cause__
        self.assertIsNotNone(cause)
        assert isinstance(cause, subprocess.CalledProcessError)
        self.assertEqual(255, cause.returncode)

    @patch("bzfs_main.util.connection.Connection.refresh_ssh_connection_if_necessary", autospec=True)
    @patch(
        "bzfs_main.util.utils.Subprocesses.subprocess_run",
        side_effect=subprocess.TimeoutExpired(cmd="cmd", timeout=1),
    )
    def test_timeout_expired_propagates(self, mock_run: MagicMock, mock_refresh: MagicMock) -> None:
        self.remote.ssh_user_host = "h"
        self.conn_pool._remote.ssh_user_host = "h"
        with self.assertRaises(subprocess.TimeoutExpired):
            self.job.run_ssh_command(self.remote, cmd=["sleep"], print_stdout=True)
        mock_run.assert_called_once()
        self.conn_pool_connection_mock.assert_called_once()

    @patch("bzfs_main.util.connection.Connection.refresh_ssh_connection_if_necessary", autospec=True)
    @patch(
        "bzfs_main.util.utils.Subprocesses.subprocess_run",
        side_effect=UnicodeDecodeError("utf-8", b"x", 0, 1, "boom"),
    )
    def test_unicode_error_propagates_without_logging(self, mock_run: MagicMock, mock_refresh: MagicMock) -> None:
        self.remote.ssh_user_host = "h"
        self.conn_pool._remote.ssh_user_host = "h"
        with self.assertRaises(UnicodeDecodeError):
            self.job.run_ssh_command(self.remote, cmd=["foo"])
        mock_run.assert_called_once()
        self.conn_pool_connection_mock.assert_called_once()


#############################################################################
class TestTrySshCommand(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.params = make_fake_params()
        self.remote = cast(
            Remote, _FakeRemote(location="dst", ssh_user_host="host", reuse_ssh_connection=True, ssh_extra_opts=[])
        )

    @patch("bzfs_main.bzfs.Job.run_ssh_command", return_value="ok")
    @patch("bzfs_main.bzfs.Job.maybe_inject_error")
    def test_success(self, mock_inject: MagicMock, mock_run: MagicMock) -> None:
        result = self.job.try_ssh_command(self.remote, loglevel=0, cmd=["ls"])
        self.assertEqual("ok", result)
        mock_inject.assert_called_once()
        mock_run.assert_called_once()

    @patch(
        "bzfs_main.bzfs.Job.run_ssh_command",
        side_effect=subprocess.CalledProcessError(returncode=1, cmd="cmd", stderr="zfs: dataset does not exist"),
    )
    def test_dataset_missing_returns_none(self, mock_run: MagicMock) -> None:
        result = self.job.try_ssh_command(self.remote, loglevel=0, cmd=["zfs"], exists=True)
        self.assertIsNone(result)
        mock_run.assert_called_once()

    @patch(
        "bzfs_main.bzfs.Job.run_ssh_command",
        side_effect=subprocess.CalledProcessError(returncode=1, cmd="cmd", stderr="boom"),
    )
    def test_other_error_raises_retryable(self, mock_run: MagicMock) -> None:
        with self.assertRaises(RetryableError):
            self.job.try_ssh_command(self.remote, loglevel=0, cmd=["zfs"], exists=False)
        mock_run.assert_called_once()

    @patch(
        "bzfs_main.bzfs.Job.run_ssh_command",
        side_effect=UnicodeDecodeError("utf-8", b"x", 0, 1, "boom"),
    )
    def test_unicode_error_wrapped(self, mock_run: MagicMock) -> None:
        with self.assertRaises(RetryableError):
            self.job.try_ssh_command(self.remote, loglevel=0, cmd=["x"])
        mock_run.assert_called_once()

    @patch("bzfs_main.bzfs.Job.maybe_inject_error", side_effect=RetryableError("Subprocess failed"))
    def test_injected_retryable_error_propagates(self, mock_inject: MagicMock) -> None:
        with self.assertRaises(RetryableError):
            self.job.try_ssh_command(self.remote, loglevel=0, cmd=["x"])
        mock_inject.assert_called_once()


#############################################################################
class TestRefreshSshConnection(AbstractTestCase):
    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.params = make_fake_params()
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
        self.remote.ssh_control_persist_margin_secs = 1
        self.conn = connection.Connection(self.remote, 1)
        self.conn._last_refresh_time = 0

    @patch("bzfs_main.util.utils.Subprocesses.subprocess_run")
    def test_local_mode_returns_immediately(self, mock_run: MagicMock) -> None:
        self.remote.ssh_user_host = ""
        self.conn.refresh_ssh_connection_if_necessary(self.job)
        mock_run.assert_not_called()

    @patch("bzfs_main.util.connection.die", side_effect=SystemExit)
    def test_ssh_unavailable_dies(self, mock_die: MagicMock) -> None:
        self.remote.is_ssh_available = MagicMock(return_value=False)  # type: ignore[method-assign]
        with self.assertRaises(SystemExit):
            self.conn.refresh_ssh_connection_if_necessary(self.job)
        mock_die.assert_called_once()

    @patch("bzfs_main.util.utils.Subprocesses.subprocess_run")
    def test_reuse_disabled_no_action(self, mock_run: MagicMock) -> None:
        self.remote.reuse_ssh_connection = False
        self.conn.refresh_ssh_connection_if_necessary(self.job)
        mock_run.assert_not_called()

    @patch("bzfs_main.util.utils.Subprocesses.subprocess_run")
    def test_connection_alive_fast_path(self, mock_run: MagicMock) -> None:
        self.conn._last_refresh_time = time.monotonic_ns()
        self.conn.refresh_ssh_connection_if_necessary(self.job)
        mock_run.assert_not_called()

    @patch("bzfs_main.util.utils.Subprocesses.subprocess_run")
    def test_master_alive_refreshes_timestamp(self, mock_run: MagicMock) -> None:
        mock_run.return_value = MagicMock(returncode=0)
        self.conn.refresh_ssh_connection_if_necessary(self.job)
        mock_run.assert_called_once()
        self.assertGreater(self.conn._last_refresh_time, 0)

    @patch("bzfs_main.util.utils.Subprocesses.subprocess_run")
    def test_master_start_failure_is_retryable(self, mock_run: MagicMock) -> None:
        # First call: '-O check' returns non-zero, indicating master not alive.
        # Second call: starting master with check=True raises CalledProcessError.
        mock_run.side_effect = [
            MagicMock(returncode=1),
            subprocess.CalledProcessError(returncode=1, cmd="ssh", stderr="bad"),
        ]
        with self.assertRaises(RetryableError):
            self.conn.refresh_ssh_connection_if_necessary(self.job)

    @patch("bzfs_main.util.utils.Subprocesses.subprocess_run")
    def test_verbose_option_limits_persist_time(self, mock_run: MagicMock) -> None:
        self.remote.ssh_extra_opts = ("-v",)
        mock_run.side_effect = [MagicMock(returncode=1), MagicMock(returncode=0)]
        self.conn.refresh_ssh_connection_if_necessary(self.job)
        args = mock_run.call_args_list[1][0][0]
        self.assertIn("-oControlPersist=1s", args)

    @patch("bzfs_main.util.connection_lease.ConnectionLease.set_socket_mtime_to_now")
    @patch("bzfs_main.util.utils.Subprocesses.subprocess_run")
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
            cpool = ConnectionPool(remote, SHARED, 1)
            conn = cpool.get_connection()
            try:
                conn._last_refresh_time = 0  # avoid fast return
                conn.refresh_ssh_connection_if_necessary(self.job)
                self.assertTrue(mock_utime.called)
                self.assertIsNotNone(conn._connection_lease)
                mock_utime.assert_called_once()
            finally:
                conn.shutdown("test")

    @patch("bzfs_main.util.connection_lease.ConnectionLease.set_socket_mtime_to_now")
    @patch("bzfs_main.util.utils.Subprocesses.subprocess_run")
    def test_mtime_now_not_invoked_when_no_connection_lease(self, mock_run: MagicMock, mock_utime: MagicMock) -> None:
        """Ensures set_socket_mtime_to_now() is not called when a Connection has no lease (lease is None)."""
        mock_run.return_value = MagicMock(returncode=0)
        self.conn._last_refresh_time = 0  # avoid fast return
        # In setUp, ssh_exit_on_shutdown=True ensures no lease is acquired.
        self.assertIsNone(self.conn._connection_lease)
        self.conn.refresh_ssh_connection_if_necessary(self.job)
        mock_utime.assert_not_called()


#############################################################################
class TestTimeout(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.params = make_fake_params()

    def test_no_timeout_returns_none(self) -> None:
        self.assertIsNone(connection.timeout(self.job))
        self.job.timeout_nanos = None
        self.job.timeout_duration_nanos = None
        self.assertIsNone(connection.timeout(self.job))

    def test_expired_timeout_raises(self) -> None:
        self.job.timeout_duration_nanos = 1
        self.job.timeout_nanos = time.monotonic_ns() - self.job.timeout_duration_nanos
        with self.assertRaises(subprocess.TimeoutExpired):
            connection.timeout(self.job)

    def test_seconds_remaining_returned(self) -> None:
        self.job.timeout_duration_nanos = 1_000_000_000
        self.job.timeout_nanos = time.monotonic_ns() + self.job.timeout_duration_nanos
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
        self.job.maybe_inject_error(cmd=["cmd"])

    def test_missing_counter_noop(self) -> None:
        self.job.maybe_inject_error(cmd=["cmd"], error_trigger="boom")

    def test_counter_zero_noop(self) -> None:
        self.job.error_injection_triggers["before"]["boom"] = 0
        self.job.maybe_inject_error(cmd=["cmd"], error_trigger="boom")
        self.assertEqual(0, self.job.error_injection_triggers["before"]["boom"])

    def test_nonretryable_error_raised(self) -> None:
        self.job.error_injection_triggers["before"]["boom"] = 1
        with self.assertRaises(subprocess.CalledProcessError):
            self.job.maybe_inject_error(cmd=["cmd"], error_trigger="boom")
        self.assertEqual(0, self.job.error_injection_triggers["before"]["boom"])

    def test_retryable_error_raised(self) -> None:
        self.job.error_injection_triggers["before"]["retryable_boom"] = 1
        with self.assertRaises(RetryableError):
            self.job.maybe_inject_error(cmd=["cmd"], error_trigger="retryable_boom")


#############################################################################
class TestDecrementInjectionCounter(AbstractTestCase):

    def setUp(self) -> None:
        self.job = bzfs.Job()
        self.job.injection_lock = threading.Lock()

    def test_zero_counter_returns_false(self) -> None:
        counter: Counter = Counter()
        self.assertFalse(self.job.decrement_injection_counter(counter=counter, trigger="x"))

    def test_positive_counter_decrements(self) -> None:
        counter: Counter = Counter({"x": 2})
        self.assertTrue(self.job.decrement_injection_counter(counter=counter, trigger="x"))
        self.assertEqual(1, counter["x"])


#############################################################################
class TestSshExitOnShutdown(AbstractTestCase):

    @patch("bzfs_main.util.connection.subprocess.run")
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
        conn: Connection = Connection(r, 1)
        conn.shutdown("test")
        self.assertTrue(mock_run.called)
        argv = mock_run.call_args[0][0]
        self.assertIn("-O", argv)
        self.assertIn("exit", argv)

    @patch("bzfs_main.util.connection.subprocess.run")
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
            cpool = ConnectionPool(r, SHARED, 1)
            conn: Connection = cpool.get_connection()
            conn.shutdown("test")
            # No call to run expected when immediate exit is false
            self.assertFalse(mock_run.called)


def get_tmpdir() -> tempfile.TemporaryDirectory[str]:
    # on OSX the default test tmp dir is unreasonably long, e.g.
    # /var/folders/dj/y5mjnplj0l79dq4dkknbsjbh0000gp/T/tmp7j51fpp/
    root_dir = "/tmp" if platform.system() == "Darwin" else None
    return tempfile.TemporaryDirectory(dir=root_dir)
