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
"""Network connection management is in refresh_ssh_connection_if_necessary() and class ConnectionPool; They reuse multiplexed
ssh connections for low latency."""

from __future__ import (
    annotations,
)
import contextlib
import copy
import logging
import shlex
import subprocess
import sys
import threading
import time
from collections import (
    Counter,
)
from collections.abc import (
    Iterator,
)
from dataclasses import (
    dataclass,
)
from subprocess import (
    DEVNULL,
    PIPE,
    CalledProcessError,
    CompletedProcess,
)
from typing import (
    TYPE_CHECKING,
    Final,
)

from bzfs_main.connection_lease import (
    ConnectionLease,
    ConnectionLeaseManager,
)
from bzfs_main.retry import (
    RetryableError,
)
from bzfs_main.utils import (
    LOG_TRACE,
    PROG_NAME,
    SmallPriorityQueue,
    Subprocesses,
    die,
    list_formatter,
    stderr_to_str,
    xprint,
)

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from bzfs_main.bzfs import (
        Job,
    )
    from bzfs_main.configuration import (
        Params,
        Remote,
    )

# constants:
SHARED: Final[str] = "shared"
DEDICATED: Final[str] = "dedicated"


def run_ssh_command(
    job: Job,
    remote: Remote,
    level: int = -1,
    is_dry: bool = False,
    check: bool = True,
    print_stdout: bool = False,
    print_stderr: bool = True,
    cmd: list[str] | None = None,
) -> str:
    """Runs the given CLI cmd via ssh on the given remote, and returns stdout.

    The full command is the concatenation of both the command to run on the localhost in order to talk to the remote host
    ($remote.local_ssh_command()) and the command to run on the given remote host ($cmd).

    Note: When executing on a remote host (remote.ssh_user_host is set), cmd arguments are pre-quoted with shlex.quote to
    safely traverse the ssh "remote shell" boundary, as ssh concatenates argv into a single remote shell string. In local
    mode (no remote.ssh_user_host) argv is executed directly without an intermediate shell.
    """
    level = level if level >= 0 else logging.INFO
    assert cmd is not None and isinstance(cmd, list) and len(cmd) > 0
    p, log = job.params, job.params.log
    quoted_cmd: list[str] = [shlex.quote(arg) for arg in cmd]
    conn_pool: ConnectionPool = p.connection_pools[remote.location].pool(SHARED)
    with conn_pool.connection() as conn:
        ssh_cmd: list[str] = conn.ssh_cmd
        if remote.ssh_user_host:
            refresh_ssh_connection_if_necessary(job, remote, conn)
            cmd = quoted_cmd
        msg: str = "Would execute: %s" if is_dry else "Executing: %s"
        log.log(level, msg, list_formatter(conn.ssh_cmd_quoted + quoted_cmd, lstrip=True))
        if is_dry:
            return ""
        try:
            sp: Subprocesses = job.subprocesses
            process: CompletedProcess[str] = sp.subprocess_run(
                ssh_cmd + cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True, timeout=timeout(job), check=check
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, UnicodeDecodeError) as e:
            if not isinstance(e, UnicodeDecodeError):
                xprint(log, stderr_to_str(e.stdout), run=print_stdout, file=sys.stdout, end="")
                xprint(log, stderr_to_str(e.stderr), run=print_stderr, file=sys.stderr, end="")
            raise
        else:
            xprint(log, process.stdout, run=print_stdout, file=sys.stdout, end="")
            xprint(log, process.stderr, run=print_stderr, file=sys.stderr, end="")
            return process.stdout


def try_ssh_command(
    job: Job,
    remote: Remote,
    level: int,
    is_dry: bool = False,
    print_stdout: bool = False,
    cmd: list[str] | None = None,
    exists: bool = True,
    error_trigger: str | None = None,
) -> str | None:
    """Convenience method that helps retry/react to a dataset or pool that potentially doesn't exist anymore."""
    assert cmd is not None and isinstance(cmd, list) and len(cmd) > 0
    log = job.params.log
    try:
        maybe_inject_error(job, cmd=cmd, error_trigger=error_trigger)
        return run_ssh_command(job, remote, level=level, is_dry=is_dry, print_stdout=print_stdout, cmd=cmd)
    except (subprocess.CalledProcessError, UnicodeDecodeError) as e:
        if not isinstance(e, UnicodeDecodeError):
            stderr: str = stderr_to_str(e.stderr)
            if exists and (
                ": dataset does not exist" in stderr
                or ": filesystem does not exist" in stderr  # solaris 11.4.0
                or ": no such pool" in stderr
            ):
                return None
            log.warning("%s", stderr.rstrip())
        raise RetryableError("Subprocess failed") from e


def refresh_ssh_connection_if_necessary(job: Job, remote: Remote, conn: Connection) -> None:
    """Maintain or create an ssh master connection for low latency reuse."""
    p, log = job.params, job.params.log
    if not remote.ssh_user_host:
        return  # we're in local mode; no ssh required
    if not p.is_program_available("ssh", "local"):
        die(f"{p.ssh_program} CLI is not available to talk to remote host. Install {p.ssh_program} first!")
    if not remote.reuse_ssh_connection:
        return
    # Performance: reuse ssh connection for low latency startup of frequent ssh invocations via the 'ssh -S' and
    # 'ssh -S -M -oControlPersist=90s' options. See https://en.wikibooks.org/wiki/OpenSSH/Cookbook/Multiplexing
    # and https://chessman7.substack.com/p/how-ssh-multiplexing-reuses-master
    control_persist_limit_nanos: int = (remote.ssh_control_persist_secs - job.control_persist_margin_secs) * 1_000_000_000
    with conn.lock:
        if time.monotonic_ns() < conn.last_refresh_time + control_persist_limit_nanos:
            return  # ssh master is alive, reuse its TCP connection (this is the common case and the ultra-fast path)
        ssh_cmd: list[str] = conn.ssh_cmd
        ssh_socket_cmd: list[str] = ssh_cmd[0:-1]  # omit trailing ssh_user_host
        ssh_socket_cmd += ["-O", "check", remote.ssh_user_host]
        # extend lifetime of ssh master by $ssh_control_persist_secs via 'ssh -O check' if master is still running.
        # 'ssh -S /path/to/socket -O check' doesn't talk over the network, hence is still a low latency fast path.
        sp: Subprocesses = job.subprocesses
        if sp.subprocess_run(ssh_socket_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, timeout=timeout(job)).returncode == 0:
            log.log(LOG_TRACE, "ssh connection is alive: %s", list_formatter(ssh_socket_cmd))
        else:  # ssh master is not alive; start a new master:
            log.log(LOG_TRACE, "ssh connection is not yet alive: %s", list_formatter(ssh_socket_cmd))
            ssh_control_persist_secs: int = remote.ssh_control_persist_secs
            if "-v" in remote.ssh_extra_opts:
                # Unfortunately, with `ssh -v` (debug mode), the ssh master won't background; instead it stays in the
                # foreground and blocks until the ControlPersist timer expires (90 secs). To make progress earlier we ...
                ssh_control_persist_secs = min(1, ssh_control_persist_secs)  # tell ssh to block as briefly as possible (1s)
            ssh_socket_cmd = ssh_cmd[0:-1]  # omit trailing ssh_user_host
            ssh_socket_cmd += ["-M", f"-oControlPersist={ssh_control_persist_secs}s", remote.ssh_user_host, "exit"]
            log.log(LOG_TRACE, "Executing: %s", list_formatter(ssh_socket_cmd))
            try:
                sp.subprocess_run(ssh_socket_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, check=True, timeout=timeout(job))
            except subprocess.CalledProcessError as e:
                log.error("%s", stderr_to_str(e.stderr).rstrip())
                raise RetryableError(
                    f"Cannot ssh into remote host via '{' '.join(ssh_socket_cmd)}'. Fix ssh configuration "
                    f"first, considering diagnostic log file output from running {PROG_NAME} with -v -v -v."
                ) from e
        conn.last_refresh_time = time.monotonic_ns()
        if conn.connection_lease is not None:
            conn.connection_lease.set_socket_mtime_to_now()


def timeout(job: Job) -> float | None:
    """Raises TimeoutExpired if necessary, else returns the number of seconds left until timeout is to occur."""
    timeout_nanos: int | None = job.timeout_nanos
    if timeout_nanos is None:
        return None  # never raise a timeout
    delta_nanos: int = timeout_nanos - time.monotonic_ns()
    if delta_nanos <= 0:
        assert job.params.timeout_nanos is not None
        raise subprocess.TimeoutExpired(PROG_NAME + "_timeout", timeout=job.params.timeout_nanos / 1_000_000_000)
    return delta_nanos / 1_000_000_000  # seconds


def maybe_inject_error(job: Job, cmd: list[str], error_trigger: str | None = None) -> None:
    """For testing only; for unit tests to simulate errors during replication and test correct handling of them."""
    if error_trigger:
        counter = job.error_injection_triggers.get("before")
        if counter and decrement_injection_counter(job, counter, error_trigger):
            try:
                raise CalledProcessError(returncode=1, cmd=" ".join(cmd), stderr=error_trigger + ":dataset is busy")
            except subprocess.CalledProcessError as e:
                if error_trigger.startswith("retryable_"):
                    raise RetryableError("Subprocess failed") from e
                else:
                    raise


def decrement_injection_counter(job: Job, counter: Counter[str], trigger: str) -> bool:
    """For testing only."""
    with job.injection_lock:
        if counter[trigger] <= 0:
            return False
        counter[trigger] -= 1
        return True


#############################################################################
@dataclass(order=True, repr=False)
class Connection:
    """Represents the ability to multiplex N=capacity concurrent SSH sessions over the same TCP connection."""

    _free: int  # sort order evens out the number of concurrent sessions among the TCP connections
    _last_modified: int  # LIFO: tiebreaker favors latest returned conn as that's most alive and hot; also ensures no dupes

    def __init__(
        self,
        remote: Remote,
        max_concurrent_ssh_sessions_per_tcp_connection: int,
        cid: int,
        lease: ConnectionLease | None = None,
    ) -> None:
        assert max_concurrent_ssh_sessions_per_tcp_connection > 0
        self._capacity: Final[int] = max_concurrent_ssh_sessions_per_tcp_connection
        self._free: int = max_concurrent_ssh_sessions_per_tcp_connection
        self._last_modified: int = 0  # monotonically increasing
        self._cid: Final[int] = cid
        self.last_refresh_time: int = 0
        self.lock: Final[threading.Lock] = threading.Lock()
        self._reuse_ssh_connection: Final[bool] = remote.reuse_ssh_connection
        self.connection_lease: Final[ConnectionLease | None] = lease
        self.ssh_cmd: Final[list[str]] = remote.local_ssh_command(
            None if self.connection_lease is None else self.connection_lease.socket_path
        )
        self.ssh_cmd_quoted: Final[list[str]] = [shlex.quote(item) for item in self.ssh_cmd]

    def __repr__(self) -> str:
        return str({"free": self._free, "cid": self._cid})

    def increment_free(self, value: int) -> None:
        """Adjusts the count of available SSH slots."""
        self._free += value
        assert self._free >= 0
        assert self._free <= self._capacity

    def is_full(self) -> bool:
        """Returns True if no more SSH sessions may be opened over this TCP connection."""
        return self._free <= 0

    def update_last_modified(self, last_modified: int) -> None:
        """Records when the connection was last used."""
        self._last_modified = last_modified

    def shutdown(self, msg_prefix: str, p: Params) -> None:
        """Closes the underlying SSH master connection and releases the corresponding connection lease."""
        ssh_cmd: list[str] = self.ssh_cmd
        if ssh_cmd and self._reuse_ssh_connection:
            if self.connection_lease is None:
                ssh_sock_cmd: list[str] = ssh_cmd[0:-1] + ["-O", "exit", ssh_cmd[-1]]
                p.log.log(LOG_TRACE, f"Executing {msg_prefix}: %s", shlex.join(ssh_sock_cmd))
                try:
                    proc: CompletedProcess = subprocess.run(ssh_sock_cmd, stdin=DEVNULL, stderr=PIPE, text=True, timeout=0.1)
                except subprocess.TimeoutExpired as e:  # harmless as master auto-exits after ssh_control_persist_secs anyway
                    p.log.log(LOG_TRACE, "Harmless ssh master connection shutdown timeout: %s", e)
                else:
                    if proc.returncode != 0:  # harmless for the same reason
                        p.log.log(LOG_TRACE, "Harmless ssh master connection shutdown issue: %s", proc.stderr.rstrip())
            else:
                self.connection_lease.release()


#############################################################################
class ConnectionPool:
    """Fetch a TCP connection for use in an SSH session, use it, finally return it back to the pool for future reuse."""

    def __init__(self, remote: Remote, max_concurrent_ssh_sessions_per_tcp_connection: int, connpool_name: str) -> None:
        assert max_concurrent_ssh_sessions_per_tcp_connection > 0
        self._remote: Final[Remote] = copy.copy(remote)  # shallow copy for immutability (Remote is mutable)
        self._capacity: Final[int] = max_concurrent_ssh_sessions_per_tcp_connection
        self._connpool_name: Final[str] = connpool_name
        self._priority_queue: Final[SmallPriorityQueue[Connection]] = SmallPriorityQueue(
            reverse=True  # sorted by #free slots and last_modified
        )
        self._last_modified: int = 0  # monotonically increasing sequence number
        self._cid: int = 0  # monotonically increasing connection number
        self._lock: Final[threading.Lock] = threading.Lock()
        lease_mgr: ConnectionLeaseManager | None = None
        if self._remote.ssh_user_host and self._remote.reuse_ssh_connection and not self._remote.ssh_exit_on_shutdown:
            lease_mgr = ConnectionLeaseManager(
                root_dir=self._remote.ssh_socket_dir,
                namespace=f"{self._remote.location}#{self._remote.cache_namespace()}#{self._connpool_name}",
                ssh_control_persist_secs=max(90 * 60, 2 * self._remote.ssh_control_persist_secs + 2),
                log=self._remote.params.log,
            )
        self._lease_mgr: Final[ConnectionLeaseManager | None] = lease_mgr

    @contextlib.contextmanager
    def connection(self) -> Iterator[Connection]:
        """Context manager that yields a connection from the pool and automatically returns it on __exit__."""
        conn: Connection = self.get_connection()
        try:
            yield conn
        finally:
            self.return_connection(conn)

    def get_connection(self) -> Connection:
        """Any Connection object returned on get_connection() also remains intentionally contained in the priority queue
        while it is "checked out", and that identical Connection object is later, on return_connection(), temporarily removed
        from the priority queue, updated with an incremented "free" slot count and then immediately reinserted into the
        priority queue.

        In effect, any Connection object remains intentionally contained in the priority queue at all times. This design
        keeps ordering/fairness accurate while avoiding duplicate Connection instances.
        """
        with self._lock:
            conn = self._priority_queue.pop() if len(self._priority_queue) > 0 else None
            if conn is None or conn.is_full():
                if conn is not None:
                    self._priority_queue.push(conn)
                lease: ConnectionLease | None = None if self._lease_mgr is None else self._lease_mgr.acquire()
                conn = Connection(self._remote, self._capacity, self._cid, lease=lease)  # add a new connection
                self._last_modified += 1
                conn.update_last_modified(self._last_modified)  # LIFO tiebreaker favors latest conn as that's most alive
                self._cid += 1
            conn.increment_free(-1)
            self._priority_queue.push(conn)
            return conn

    def return_connection(self, conn: Connection) -> None:
        """Returns the given connection to the pool and updates its priority."""
        assert conn is not None
        with self._lock:
            # update priority = remove conn from queue, increment priority, finally reinsert updated conn into queue
            if self._priority_queue.remove(conn):  # conn is not contained only if ConnectionPool.shutdown() was called
                conn.increment_free(1)
                self._last_modified += 1
                conn.update_last_modified(self._last_modified)  # LIFO tiebreaker favors latest conn as that's most alive
                self._priority_queue.push(conn)

    def shutdown(self, msg_prefix: str) -> None:
        """Closes all SSH connections managed by this pool."""
        with self._lock:
            try:
                if self._remote.reuse_ssh_connection:
                    for conn in self._priority_queue:
                        conn.shutdown(msg_prefix, self._remote.params)
            finally:
                self._priority_queue.clear()

    def __repr__(self) -> str:
        with self._lock:
            queue = self._priority_queue
            return str({"capacity": self._capacity, "queue_len": len(queue), "cid": self._cid, "queue": queue})


#############################################################################
class ConnectionPools:
    """A bunch of named connection pools with various multiplexing capacities."""

    def __init__(self, remote: Remote, capacities: dict[str, int]) -> None:
        """Creates one connection pool per name with the given capacities."""
        self._pools: Final[dict[str, ConnectionPool]] = {
            name: ConnectionPool(remote, capacity, name) for name, capacity in capacities.items()
        }

    def __repr__(self) -> str:
        return str(self._pools)

    def pool(self, name: str) -> ConnectionPool:
        """Returns the pool associated with the given name."""
        return self._pools[name]

    def shutdown(self, msg_prefix: str) -> None:
        """Shuts down every contained pool."""
        for name, pool in self._pools.items():
            pool.shutdown(msg_prefix + "/" + name)
