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
"""Efficient thread-safe SSH command client; See run_ssh_command() and refresh_ssh_connection_if_necessary() and class
ConnectionPool and class ConnectionLease.

Can be configured to reuse multiplexed SSH connections for low latency, even on fresh process startup, for example leading to
ballpark 3-5ms total time for running `/bin/echo hello` end-to-end over SSH on LAN, which requires two (sequential) network
round trips (one for CHANNEL_OPEN, plus a subsequent one for CHANNEL_REQUEST).
Has zero dependencies beyond the standard OpenSSH client CLI (`ssh`); also works with `hpnssh`. The latter uses larger TCP
window sizes for best throughput over high speed long distance networks, aka paths with large bandwidth-delay product.

Example usage:

import logging
from subprocess import DEVNULL, PIPE
from bzfs_main.util.connection import ConnectionPool, create_simple_minijob, create_simple_miniremote, run_ssh_command
from bzfs_main.util.retry import Retry, RetryPolicy, run_with_retries

log = logging.getLogger(__name__)
remote = create_simple_miniremote(log=log, ssh_user_host="alice@127.0.0.1")
connection_pool = ConnectionPool(remote, connpool_name="example")
try:
    job = create_simple_minijob()
    retry_policy = RetryPolicy(
        max_retries=10,
        min_sleep_secs=0,
        initial_max_sleep_secs=0.125,
        max_sleep_secs=10,
        max_elapsed_secs=60,
    )

    def run_cmd(retry: Retry) -> str:
        with connection_pool.connection() as conn:
            stdout: str = conn.run_ssh_command(
                cmd=["echo", "hello"], conn=conn, job=job, check=True, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, text=True
            ).stdout
            return stdout

    stdout = run_with_retries(fn=run_cmd, policy=retry_policy, log=log)
    print(f"stdout: {stdout}")
finally:
    connection_pool.shutdown()
"""

from __future__ import (
    annotations,
)
import contextlib
import copy
import logging
import os
import shlex
import subprocess
import threading
import time
from collections.abc import (
    Iterator,
)
from dataclasses import (
    dataclass,
)
from subprocess import (
    DEVNULL,
    PIPE,
)
from typing import (
    Any,
    Final,
    Protocol,
    final,
    runtime_checkable,
)

from bzfs_main.util.connection_lease import (
    ConnectionLease,
    ConnectionLeaseManager,
)
from bzfs_main.util.retry import (
    RetryableError,
)
from bzfs_main.util.utils import (
    LOG_TRACE,
    SmallPriorityQueue,
    Subprocesses,
    die,
    get_home_directory,
    list_formatter,
    sha256_urlsafe_base64,
    stderr_to_str,
)

# constants:
SHARED: Final[str] = "shared"
DEDICATED: Final[str] = "dedicated"


#############################################################################
@runtime_checkable
class MiniJob(Protocol):
    """Minimal Job interface required by the connections module; for loose coupling."""

    timeout_nanos: int | None  # timestamp aka instant in time
    timeout_duration_nanos: int | None  # duration (not a timestamp); for logging only
    subprocesses: Subprocesses


#############################################################################
@runtime_checkable
class MiniParams(Protocol):
    """Minimal Params interface used by the connections module; for loose coupling."""

    log: logging.Logger
    ssh_program: str  # name or path of executable; "hpnssh" is also valid


#############################################################################
@runtime_checkable
class MiniRemote(Protocol):
    """Minimal Remote interface used by the connections module; for loose coupling."""

    params: MiniParams
    location: str  # "src" or "dst"
    ssh_user_host: str  # use the empty string to indicate local mode (no ssh)
    ssh_extra_opts: tuple[str, ...]
    reuse_ssh_connection: bool
    ssh_control_persist_secs: int
    ssh_control_persist_margin_secs: int
    ssh_exit_on_shutdown: bool
    ssh_socket_dir: str

    def is_ssh_available(self) -> bool:
        """Return True if the ssh client program required for this remote is available on the local host."""

    def local_ssh_command(self, socket_file: str | None) -> list[str]:
        """Returns the ssh CLI command to run locally in order to talk to the remote host; This excludes the (trailing)
        command to run on the remote host, which will be appended later."""

    def cache_namespace(self) -> str:
        """Returns cache namespace string which is a stable, unique directory component for caches that distinguishes
        endpoints by username+host+port+ssh_config_file where applicable, and uses '-' when no user/host is present (local
        mode)."""


#############################################################################
def create_simple_miniremote(
    log: logging.Logger,
    ssh_user_host: str = "",  # option passed to `ssh` CLI; empty string indicates local mode
    ssh_port: int | None = None,  # option passed to `ssh` CLI
    ssh_extra_opts: list[str] | None = None,  # optional args passed to `ssh` CLI
    ssh_verbose: bool = False,  # option passed to `ssh` CLI
    ssh_config_file: str = "",  # option passed to `ssh` CLI; example: /path/to/homedir/.ssh/config
    ssh_cipher: str = "^aes256-gcm@openssh.com",  # option passed to `ssh` CLI
    ssh_program: str = "ssh",  # name or path of CLI executable; "hpnssh" is also valid
    reuse_ssh_connection: bool = True,
    ssh_control_persist_secs: int = 90,
    ssh_control_persist_margin_secs: int = 2,
    ssh_socket_dir: str = os.path.join(get_home_directory(), ".ssh", "bzfs"),
    location: str = "dst",
) -> MiniRemote:
    """Factory that returns a simple implementation of the MiniRemote interface."""

    @dataclass(frozen=True)  # aka immutable
    @final
    class SimpleMiniParams(MiniParams):
        log: logging.Logger
        ssh_program: str

    @dataclass(frozen=True)  # aka immutable
    @final
    class SimpleMiniRemote(MiniRemote):
        params: MiniParams
        location: str  # "src" or "dst"
        ssh_user_host: str
        ssh_extra_opts: tuple[str, ...]
        reuse_ssh_connection: bool
        ssh_control_persist_secs: int
        ssh_control_persist_margin_secs: int
        ssh_exit_on_shutdown: bool
        ssh_socket_dir: str
        ssh_port: int | None
        ssh_config_file: str
        ssh_config_file_hash: str

        def is_ssh_available(self) -> bool:
            return True

        def local_ssh_command(self, socket_file: str | None) -> list[str]:
            if not self.ssh_user_host:
                return []  # local mode
            ssh_cmd: list[str] = [self.params.ssh_program]
            ssh_cmd.extend(self.ssh_extra_opts)
            if self.reuse_ssh_connection and socket_file:
                ssh_cmd.append("-S")
                ssh_cmd.append(socket_file)
            ssh_cmd.append(self.ssh_user_host)
            return ssh_cmd

        def cache_namespace(self) -> str:
            if not self.ssh_user_host:
                return "-"  # local mode
            return f"{self.ssh_user_host}#{self.ssh_port or ''}#{self.ssh_config_file_hash}"

    if log is None:
        raise ValueError("log must not be None")
    if not ssh_program:
        raise ValueError("ssh_program must be a non-empty string")
    if location not in ("src", "dst"):
        raise ValueError("location must be 'src' or 'dst'")
    if ssh_control_persist_secs < 1:
        raise ValueError("ssh_control_persist_secs must be >= 1")
    params: MiniParams = SimpleMiniParams(log=log, ssh_program=ssh_program)

    ssh_extra_opts = (  # disable interactive password prompts and X11 forwarding and pseudo-terminal allocation
        ["-oBatchMode=yes", "-oServerAliveInterval=0", "-x", "-T"] if ssh_extra_opts is None else list(ssh_extra_opts)
    )
    ssh_extra_opts += ["-v"] if ssh_verbose else []
    ssh_extra_opts += ["-F", ssh_config_file] if ssh_config_file else []
    ssh_extra_opts += ["-c", ssh_cipher] if ssh_cipher else []
    ssh_extra_opts += ["-p", str(ssh_port)] if ssh_port is not None else []
    ssh_config_file_hash = sha256_urlsafe_base64(os.path.abspath(ssh_config_file), padding=False) if ssh_config_file else ""
    return SimpleMiniRemote(
        params=params,
        location=location,
        ssh_user_host=ssh_user_host,
        ssh_extra_opts=tuple(ssh_extra_opts),
        reuse_ssh_connection=reuse_ssh_connection,
        ssh_control_persist_secs=ssh_control_persist_secs,
        ssh_control_persist_margin_secs=ssh_control_persist_margin_secs,
        ssh_exit_on_shutdown=False,
        ssh_socket_dir=ssh_socket_dir,
        ssh_port=ssh_port,
        ssh_config_file=ssh_config_file,
        ssh_config_file_hash=ssh_config_file_hash,
    )


def create_simple_minijob(timeout_duration_secs: float | None = None, subprocesses: Subprocesses | None = None) -> MiniJob:
    """Factory that returns a simple implementation of the MiniJob interface."""

    @dataclass(frozen=True)  # aka immutable
    @final
    class SimpleMiniJob(MiniJob):
        timeout_nanos: int | None  # timestamp aka instant in time
        timeout_duration_nanos: int | None  # duration (not a timestamp); for logging only
        subprocesses: Subprocesses

    t_duration_nanos: int | None = None if timeout_duration_secs is None else int(timeout_duration_secs * 1_000_000_000)
    timeout_nanos: int | None = None if t_duration_nanos is None else time.monotonic_ns() + t_duration_nanos
    subprocesses = Subprocesses() if subprocesses is None else subprocesses
    return SimpleMiniJob(timeout_nanos=timeout_nanos, timeout_duration_nanos=t_duration_nanos, subprocesses=subprocesses)


#############################################################################
def timeout(job: MiniJob) -> float | None:
    """Raises TimeoutExpired if necessary, else returns the number of seconds left until timeout is to occur."""
    timeout_nanos: int | None = job.timeout_nanos
    if timeout_nanos is None:
        return None  # never raise a timeout
    assert job.timeout_duration_nanos is not None
    delta_nanos: int = timeout_nanos - time.monotonic_ns()
    if delta_nanos <= 0:
        raise subprocess.TimeoutExpired("_timeout", timeout=job.timeout_duration_nanos / 1_000_000_000)
    return delta_nanos / 1_000_000_000  # seconds


def squote(remote: MiniRemote, arg: str) -> str:
    """Quotes an argument only when running remotely over ssh."""
    assert arg is not None
    return shlex.quote(arg) if remote.ssh_user_host else arg


def dquote(arg: str) -> str:
    """Shell-escapes backslash and double quotes and dollar and backticks, then surrounds with double quotes; For an example
    how to safely construct and quote complex shell pipeline commands for use over SSH, see
    replication.py:_prepare_zfs_send_receive()"""
    arg = arg.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$").replace("`", "\\`")
    return '"' + arg + '"'


#############################################################################
@dataclass(order=True, repr=False)
@final
class Connection:
    """Represents the ability to multiplex N=capacity concurrent SSH sessions over the same TCP connection."""

    _free: int  # sort order evens out the number of concurrent sessions among the TCP connections
    _last_modified: int  # LIFO: tiebreaker favors latest returned conn as that's most alive and hot; also ensures no dupes

    def __init__(
        self,
        remote: MiniRemote,
        max_concurrent_ssh_sessions_per_tcp_connection: int,
        lease: ConnectionLease | None = None,
    ) -> None:
        assert max_concurrent_ssh_sessions_per_tcp_connection > 0
        self._remote: Final[MiniRemote] = remote
        self._capacity: Final[int] = max_concurrent_ssh_sessions_per_tcp_connection
        self._free: int = max_concurrent_ssh_sessions_per_tcp_connection
        self._last_modified: int = 0  # monotonically increasing
        self._last_refresh_time: int = 0
        self._lock: Final[threading.Lock] = threading.Lock()
        self._reuse_ssh_connection: Final[bool] = remote.reuse_ssh_connection
        self._connection_lease: Final[ConnectionLease | None] = lease
        self._ssh_cmd: Final[list[str]] = remote.local_ssh_command(
            None if self._connection_lease is None else self._connection_lease.socket_path
        )
        self._ssh_cmd_quoted: Final[list[str]] = [shlex.quote(item) for item in self._ssh_cmd]

    @property
    def ssh_cmd(self) -> list[str]:
        return self._ssh_cmd.copy()

    @property
    def ssh_cmd_quoted(self) -> list[str]:
        return self._ssh_cmd_quoted.copy()

    def __repr__(self) -> str:
        return str({"free": self._free})

    def run_ssh_command(
        self,
        cmd: list[str],
        job: MiniJob,
        loglevel: int = logging.INFO,
        is_dry: bool = False,
        **kwargs: Any,  # optional low-level keyword args to be forwarded to subprocess.run()
    ) -> subprocess.CompletedProcess:
        """Runs the given CLI cmd via ssh on the given remote, and returns CompletedProcess including stdout and stderr.

        The full command is the concatenation of both the command to run on the localhost in order to talk to the remote host
        ($remote.local_ssh_command()) and the command to run on the given remote host ($cmd).

        Note: When executing on a remote host (remote.ssh_user_host is set), cmd arguments are pre-quoted with shlex.quote to
        safely traverse the ssh "remote shell" boundary, as ssh concatenates argv into a single remote shell string. In local
        mode (no remote.ssh_user_host) argv is executed directly without an intermediate shell.
        """
        if not cmd:
            raise ValueError("run_ssh_command requires a non-empty cmd list")
        log: logging.Logger = self._remote.params.log
        quoted_cmd: list[str] = [shlex.quote(arg) for arg in cmd]
        ssh_cmd: list[str] = self._ssh_cmd
        if self._remote.ssh_user_host:
            self.refresh_ssh_connection_if_necessary(job)
            cmd = quoted_cmd
        msg: str = "Would execute: %s" if is_dry else "Executing: %s"
        log.log(loglevel, msg, list_formatter(self._ssh_cmd_quoted + quoted_cmd, lstrip=True))
        if is_dry:
            return subprocess.CompletedProcess(ssh_cmd + cmd, returncode=0, stdout=None, stderr=None)
        else:
            sp: Subprocesses = job.subprocesses
            return sp.subprocess_run(ssh_cmd + cmd, timeout=timeout(job), log=log, **kwargs)

    def refresh_ssh_connection_if_necessary(self, job: MiniJob) -> None:
        """Maintain or create an ssh master connection for low latency reuse."""
        remote: MiniRemote = self._remote
        p: MiniParams = remote.params
        log: logging.Logger = p.log
        if not remote.ssh_user_host:
            return  # we're in local mode; no ssh required
        if not remote.is_ssh_available():
            die(f"{p.ssh_program} CLI is not available to talk to remote host. Install {p.ssh_program} first!")
        if not remote.reuse_ssh_connection:
            return

        # Performance: reuse ssh connection for low latency startup of frequent ssh invocations via the 'ssh -S' and
        # 'ssh -S -M -oControlPersist=90s' options. See https://en.wikibooks.org/wiki/OpenSSH/Cookbook/Multiplexing
        # and https://chessman7.substack.com/p/how-ssh-multiplexing-reuses-master
        control_limit_nanos: int = (remote.ssh_control_persist_secs - remote.ssh_control_persist_margin_secs) * 1_000_000_000
        with self._lock:
            if time.monotonic_ns() < self._last_refresh_time + control_limit_nanos:
                return  # ssh master is alive, reuse its TCP connection (this is the common case and the ultra-fast path)
            ssh_cmd: list[str] = self._ssh_cmd
            ssh_sock_cmd: list[str] = ssh_cmd[0:-1]  # omit trailing ssh_user_host
            ssh_sock_cmd += ["-O", "check", remote.ssh_user_host]
            # extend lifetime of ssh master by $ssh_control_persist_secs via `ssh -O check` if master is still running.
            # `ssh -S /path/to/socket -O check` doesn't talk over the network, hence is still a low latency fast path.
            sp: Subprocesses = job.subprocesses
            t: float | None = timeout(job)
            if sp.subprocess_run(ssh_sock_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, timeout=t, log=log).returncode == 0:
                log.log(LOG_TRACE, "ssh connection is alive: %s", list_formatter(ssh_sock_cmd))
            else:  # ssh master is not alive; start a new master:
                log.log(LOG_TRACE, "ssh connection is not yet alive: %s", list_formatter(ssh_sock_cmd))
                ssh_control_persist_secs: int = max(1, remote.ssh_control_persist_secs)
                if "-v" in remote.ssh_extra_opts:
                    # Unfortunately, with `ssh -v` (debug mode), the ssh master won't background; instead it stays in the
                    # foreground and blocks until the ControlPersist timer expires (90 secs). To make progress earlier we ...
                    ssh_control_persist_secs = min(1, ssh_control_persist_secs)  # tell ssh block as briefly as possible (1s)
                ssh_sock_cmd = ssh_cmd[0:-1]  # omit trailing ssh_user_host
                ssh_sock_cmd += ["-M", f"-oControlPersist={ssh_control_persist_secs}s", remote.ssh_user_host, "exit"]
                log.log(LOG_TRACE, "Executing: %s", list_formatter(ssh_sock_cmd))
                t = timeout(job)
                try:
                    sp.subprocess_run(ssh_sock_cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE, check=True, timeout=t, log=log)
                except subprocess.CalledProcessError as e:
                    log.error("%s", stderr_to_str(e.stderr).rstrip())
                    raise RetryableError(
                        f"Cannot ssh into remote host via '{' '.join(ssh_sock_cmd)}'. Fix ssh configuration first, "
                        "considering diagnostic log file output from running with -v -v -v.",
                        display_msg="ssh connect",
                    ) from e
            self._last_refresh_time = time.monotonic_ns()
            if self._connection_lease is not None:
                self._connection_lease.set_socket_mtime_to_now()

    def _increment_free(self, value: int) -> None:
        """Adjusts the count of available SSH slots."""
        self._free += value
        assert self._free >= 0
        assert self._free <= self._capacity

    def _is_full(self) -> bool:
        """Returns True if no more SSH sessions may be opened over this TCP connection."""
        return self._free <= 0

    def _update_last_modified(self, last_modified: int) -> None:
        """Records when the connection was last used."""
        self._last_modified = last_modified

    def shutdown(self, msg_prefix: str) -> None:
        """Closes the underlying SSH master connection and releases the corresponding connection lease."""
        ssh_cmd: list[str] = self._ssh_cmd
        if ssh_cmd and self._reuse_ssh_connection:
            if self._connection_lease is None:
                ssh_sock_cmd: list[str] = ssh_cmd[0:-1] + ["-O", "exit", ssh_cmd[-1]]
                log = self._remote.params.log
                log.log(LOG_TRACE, f"Executing {msg_prefix}: %s", shlex.join(ssh_sock_cmd))
                try:
                    proc: subprocess.CompletedProcess = subprocess.run(
                        ssh_sock_cmd, stdin=DEVNULL, stderr=PIPE, text=True, timeout=0.1
                    )
                except subprocess.TimeoutExpired as e:  # harmless as master auto-exits after ssh_control_persist_secs anyway
                    log.log(LOG_TRACE, "Harmless ssh master connection shutdown timeout: %s", e)
                else:
                    if proc.returncode != 0:  # harmless for the same reason
                        log.log(LOG_TRACE, "Harmless ssh master connection shutdown issue: %s", proc.stderr.rstrip())
            else:
                self._connection_lease.release()


#############################################################################
class ConnectionPool:
    """Fetch a TCP connection for use in an SSH session, use it, finally return it back to the pool for future reuse;
    Note that max_concurrent_ssh_sessions_per_tcp_connection must not be larger than the server-side sshd_config(5)
    MaxSessions parameter (which defaults to 10, see https://manpages.ubuntu.com/manpages/man5/sshd_config.5.html)."""

    def __init__(
        self, remote: MiniRemote, connpool_name: str, max_concurrent_ssh_sessions_per_tcp_connection: int = 8
    ) -> None:
        assert max_concurrent_ssh_sessions_per_tcp_connection > 0
        self._remote: Final[MiniRemote] = copy.copy(remote)  # shallow copy for immutability (Remote is mutable)
        self._capacity: Final[int] = max_concurrent_ssh_sessions_per_tcp_connection
        self._connpool_name: Final[str] = connpool_name
        self._priority_queue: Final[SmallPriorityQueue[Connection]] = SmallPriorityQueue(
            reverse=True  # sorted by #free slots and last_modified
        )
        self._last_modified: int = 0  # monotonically increasing sequence number
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
            if conn is None or conn._is_full():  # noqa: SLF001  # pylint: disable=protected-access
                if conn is not None:
                    self._priority_queue.push(conn)
                conn = self._new_connection()  # add a new connection
                self._last_modified += 1
                conn._update_last_modified(self._last_modified)  # noqa: SLF001  # pylint: disable=protected-access
            conn._increment_free(-1)  # noqa: SLF001  # pylint: disable=protected-access
            self._priority_queue.push(conn)
            return conn

    def _new_connection(self) -> Connection:
        lease: ConnectionLease | None = None if self._lease_mgr is None else self._lease_mgr.acquire()
        return Connection(self._remote, self._capacity, lease=lease)

    def return_connection(self, conn: Connection) -> None:
        """Returns the given connection to the pool and updates its priority."""
        assert conn is not None
        with self._lock:
            # update priority = remove conn from queue, increment priority, finally reinsert updated conn into queue
            if self._priority_queue.remove(conn):  # conn is not contained only if ConnectionPool.shutdown() was called
                conn._increment_free(1)  # noqa: SLF001  # pylint: disable=protected-access
                self._last_modified += 1
                conn._update_last_modified(self._last_modified)  # noqa: SLF001  # pylint: disable=protected-access
                self._priority_queue.push(conn)

    def shutdown(self, msg_prefix: str = "") -> None:
        """Closes all SSH connections managed by this pool."""
        with self._lock:
            try:
                if self._remote.reuse_ssh_connection:
                    msg_prefix = msg_prefix + "/" + self._connpool_name
                    for conn in self._priority_queue:
                        conn.shutdown(msg_prefix)
            finally:
                self._priority_queue.clear()

    def __repr__(self) -> str:
        with self._lock:
            queue = self._priority_queue
            return str({"capacity": self._capacity, "queue_len": len(queue), "queue": queue})


#############################################################################
@final
class ConnectionPools:
    """A bunch of named connection pools with various multiplexing capacities."""

    def __init__(self, remote: MiniRemote, capacities: dict[str, int]) -> None:
        """Creates one connection pool per name with the given capacities."""
        self._pools: Final[dict[str, ConnectionPool]] = {
            name: ConnectionPool(remote, name, capacity) for name, capacity in capacities.items()
        }

    def __repr__(self) -> str:
        return str(self._pools)

    def pool(self, name: str) -> ConnectionPool:
        """Returns the pool associated with the given name."""
        return self._pools[name]

    def shutdown(self, msg_prefix: str = "") -> None:
        """Shuts down every contained pool."""
        for pool in self._pools.values():
            pool.shutdown(msg_prefix)
