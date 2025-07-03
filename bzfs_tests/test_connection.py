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
import time
import unittest
from typing import (
    Any,
    cast,
)

import bzfs_main.loggers
from bzfs_main import bzfs
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
from bzfs_main.utils import log_trace
from bzfs_tests.abstract_testcase import AbstractTestCase


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestConnectionPool,
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
        self.assertEqual(len(cpool.priority_queue), queuelen)

    def assert_equal_connections(self, conn: Connection, donn: Connection) -> None:
        self.assertTupleEqual((conn.cid, conn.ssh_cmd), (donn.cid, donn.ssh_cmd))
        self.assertEqual(conn.free, donn.free)
        self.assertEqual(conn.last_modified, donn.last_modified)

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
        self.assertTupleEqual((conn.cid, conn.ssh_cmd), (donn.cid, donn.ssh_cmd))
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
        self.assertEqual(conn1.free, (capacity - 1) * 1)
        i = [str(next(counter2a))]
        self.assertEqual(conn1.ssh_cmd, i)
        self.assertEqual(conn1.ssh_cmd_quoted, i)
        self.assertIsNotNone(repr(conn1))
        self.assertIsNotNone(str(conn1))

        self.return_connection(cpool, conn1, dpool, donn1)
        self.assert_priority_queue(cpool, 1)

        conn2, donn2 = self.get_connection(cpool, dpool)
        self.assert_priority_queue(cpool, 1)
        self.assertIs(conn1, conn2)
        self.assertEqual(conn2.free, (capacity - 1) * 1)

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
        self.assertEqual(conn1.free, (capacity - 1) * 1)
        conn2 = cpool.get_connection()
        self.assertIs(conn1, conn2)
        self.assertEqual(conn2.free, (capacity - 2) * 1)
        self.assert_priority_queue(cpool, 1)

        conn3 = cpool.get_connection()
        self.assertIsNot(conn2, conn3)
        self.assertEqual(conn3.free, (capacity - 1) * 1)
        self.assertEqual(conn2.free, (capacity - 2) * 1)
        self.assert_priority_queue(cpool, 2)
        conn4 = cpool.get_connection()
        self.assertIs(conn3, conn4)
        self.assertEqual(conn4.free, (capacity - 2) * 1)
        self.assert_priority_queue(cpool, 2)

        conn5 = cpool.get_connection()
        self.assertIsNot(conn4, conn5)
        self.assertEqual(conn5.free, (capacity - 1) * 1)
        self.assertEqual(conn4.free, (capacity - 2) * 1)
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
        self.assertEqual(conn5a.free, (capacity - 1) * 1)

        conn2a = cpool.get_connection()
        self.assertEqual(conn2a.free, (capacity - 1) * 1)

        conn4a = cpool.get_connection()
        self.assertEqual(conn4a.free, (capacity - 1) * 1)

        # assert tie-breaker in favor of most recently returned TCP connection
        conn6 = cpool.get_connection()
        self.assertEqual(conn6.free, (capacity - 2) * 1)
        self.assertEqual(abs(conn6.last_modified), t5)

        conn1a = cpool.get_connection()
        self.assertEqual(conn1a.free, (capacity - 2) * 1)
        self.assertEqual(abs(conn1a.last_modified), t2)

        conn4a = cpool.get_connection()
        self.assertEqual(conn4a.free, (capacity - 2) * 1)
        self.assertEqual(abs(conn4a.last_modified), t4)

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
