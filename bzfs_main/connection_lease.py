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
"""
Purpose
-----------------
This module safely reduces SSH startup latency on bzfs process startup. Using this, a fresh bzfs CLI process can attach
immediately to an existing OpenSSH ControlMaster connection, and in doing so skip TCP handshake, SSH handshake, key exchange,
authentication, and other delays from multiple network round-trips. This way, the very first remote ZFS command starts
hundreds of milliseconds earlier as compared to when creating an SSH connection from scratch.

The higher-level goal is predictable performance, shorter critical paths, and less noisy-neighbor impact across periodic
replication jobs at fleet scale. To achieve this, masters remain alive across bzfs process lifecycles unless they become idle
for a specified time period, and inter-process reuse of ControlPath sockets is coordinated safely so every new bzfs process
benefits from an existing connection when present, and can recreate it deterministically when absent. This amortizes startup
costs, reduces tail latency, and improves operational reliability without background daemons, external services, or warm-up
procedures.

How This Is Achieved
--------------------
This module provides a small, fast, safe mechanism to allocate and reuse unique Unix domain socket files (ControlPaths)
in a per-endpoint namespace. A bzfs process acquires an exclusive advisory filesystem lock on an empty lockfile and atomically
moves the lockfile back and forth between ``free/`` and ``used/`` directories to track exclusive ownership. The held lease
exposes the open file descriptor (which maintains the lock) and the computed ControlPath so callers can safely establish or
reuse SSH master connections without races or leaks. Holding a lease's lock for the duration of a `bzfs` process guarantees
exclusive ownership of that SSH master while allowing the underlying TCP connection to persist beyond process exit via
OpenSSH ControlPersist.
Also see https://en.wikibooks.org/wiki/OpenSSH/Cookbook/Multiplexing
and https://chessman7.substack.com/p/how-ssh-multiplexing-reuses-master

Assumptions
-----------
- The filesystem is POSIX-compliant and supports ``fcntl.flock`` advisory locks, atomic ``os.rename`` renames, and
  permissions enforcement. The process runs on the same host as the SSH client using the ControlPath.
- High performance: scanning the ``free/`` and ``used/`` directories has bounded cost and is typically O(1) expected
  time because names are uniformly random. Directory contents are small and ephemeral.
- Crash recovery is acceptable by salvaging an unlocked file from ``used/``; an unlock indicates process termination
  or orderly release. The lock itself is the source of truth; directory names provide fast classification.
- Unix domain socket path length limits apply; paths are truncated to remain within portable OS caps while retaining a
  high-entropy prefix.

Design Rationale
----------------
- File locks plus atomic renames form a minimal, portable concurrency primitive that composes well with process lifecycles.
  Locks are released automatically on process exit or when the file descriptor is closed, ensuring no cleanup logic is
  required after abnormal termination.
- The two-directory layout (``free/`` and ``used/``) makes hot-path acquisition fast. We first probe ``free/`` to reuse
  previously released names, then probe ``used/`` to salvage leftovers from crashed processes that no longer hold locks,
  and finally generate a new name if necessary. This yields a compact, garbage-free namespace without a janitor.
- Name generation mixes a cryptographically strong random component with a monotonic timestamp, encoded in base32 to avoid
  long names and path-unfriendly characters. This reduces collision probability and helps with human diagnostics while
  bypassing filesystem assumptions about case sensitivity. We cap the resulting ControlPath length to satisfy kernel limits
  for Unix domain sockets while still ensuring an atomic path creation flow.
- Security is prioritized: the root, sockets, and lease directories are created with explicit permissions, and symlinks are
  rejected to avoid redirection attacks. Open flags include ``O_NOFOLLOW`` and ``O_CLOEXEC`` to remove common foot-guns.
- The public API is intentionally tiny and ergonomic. ``ConnectionLeaseManager.acquire()`` never blocks: it either returns
  a held lease or silently retries the next candidate, ensuring predictable latency under contention. The ``ConnectionLease``
  is immutable and simple to reason about, with an explicit ``release()`` to return capacity to the pool.
- No external services, daemons, or background threads are required. The design favors determinate behavior under failures,
  idempotent operations, and ease of testing. This approach integrates cleanly with ``bzfs`` where predictable SSH reuse and
  teardown during snapshot replication must remain both fast and safe.
"""

from __future__ import (
    annotations,
)
import fcntl
import os
import random
import time
from logging import (
    Logger,
)
from typing import (
    NamedTuple,
)

from bzfs_main.utils import (
    DIR_PERMISSIONS,
    FILE_PERMISSIONS,
    LOG_TRACE,
    UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH,
    base32_str,
    close_quietly,
    sha256_base32_str,
    validate_is_not_a_symlink,
)

# constants:
SOCKETS_DIR: str = "c"
FREE_DIR: str = "free"
USED_DIR: str = "used"
SOCKET_PREFIX: str = "s"


#############################################################################
class ConnectionLease(NamedTuple):
    """Purpose: Reduce SSH connection startup latency via safe ControlPath reuse.

    Assumptions: Callers hold this object only while the lease is needed. The file descriptor remains valid and keeps an
    exclusive advisory lock until ``release()`` or process exit. Paths are absolute and live within the manager's directory
    tree.

    Design Rationale: A small, immutable class captures just the essential handles needed for correctness and observability:
    the open fd, the ``used/`` and ``free/`` lockfile paths, and the Unix domain socket ControlPath that SSH uses.
    Immutability prevents accidental mutation bugs, keeps equality semantics straightforward, and encourages explicit
    ownership transfer. A dedicated ``release()`` method centralizes cleanup and safe transition of the lockfile to
    ``free/``, followed by closing the fd for deterministic unlock.
    """

    # immutable variables:
    fd: int
    used_path: str
    free_path: str
    socket_path: str

    def release(self) -> None:
        """Releases the lease: moves the lockfile from used/ dir back to free/ dir, then unlocks it by closing its fd."""
        try:
            os.rename(self.used_path, self.free_path)  # mv lockfile atomically
        except FileNotFoundError:
            pass  # harmless
        finally:
            close_quietly(self.fd)


#############################################################################
class ConnectionLeaseManager:
    """Purpose: Enable fast SSH connection reuse to cut startup latency for repeated operations.

    Assumptions: The manager has exclusive control of its root directory subtree. The process operates on a POSIX-compliant
    filesystem and uses advisory locks consistently. Path lengths must respect common Unix domain socket limits. Callers
    use the socket path with OpenSSH.

    Design Rationale: A compact state machine with ``free/`` and ``used/`` stages, guided by atomically acquired file locks
    and renames, yields predictable latency, natural crash recovery, and zero background maintenance. The API is
    intentionally minimal to reduce misuse. Defensive checks and secure open flags balance correctness, performance, and
    security without external dependencies.
    """

    def __init__(self, root_dir: str, namespace: str, log: Logger) -> None:
        # immutable variables:
        assert root_dir
        assert namespace
        self._log: Logger = log
        ns: str = sha256_base32_str(namespace, padding=False)
        ns = ns[0 : len(ns) // 2]
        base_dir: str = os.path.join(root_dir, ns)
        self._sockets_dir: str = os.path.join(base_dir, SOCKETS_DIR)
        self._free_dir: str = os.path.join(base_dir, FREE_DIR)
        self._used_dir: str = os.path.join(base_dir, USED_DIR)
        self._open_flags: int = os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC
        os.makedirs(root_dir, mode=DIR_PERMISSIONS, exist_ok=True)
        validate_is_not_a_symlink("connection lease root_dir ", root_dir)

    def acquire(self) -> ConnectionLease:
        """Acquires and returns a ConnectionLease with an open, flocked fd and the SSH ControlPath aka socket file path."""
        self._validate_dirs()
        lease = self._find_and_acquire(self._free_dir)  # fast path: find free aka unlocked socket name in O(1) expected time
        if lease is not None:
            self._log.log(LOG_TRACE, "_find_and_acquire: %s", self._free_dir)
            return lease
        lease = self._find_and_acquire(self._used_dir)  # salvage from used yet unlocked socket names leftover from crashes
        if lease is not None:
            self._log.log(LOG_TRACE, "_find_and_acquire: %s", self._used_dir)
            return lease
        lease = self._create_and_acquire()  # create new socket name
        self._log.log(LOG_TRACE, "_create_and_acquire: %s", self._free_dir)
        return lease

    def _validate_dirs(self) -> None:
        for d in (self._sockets_dir, self._free_dir, self._used_dir):
            os.makedirs(d, mode=DIR_PERMISSIONS, exist_ok=True)
            validate_is_not_a_symlink("connection lease dir ", d)

    def _find_and_acquire(self, scan_dir: str) -> ConnectionLease | None:
        with os.scandir(scan_dir) as iterator:
            for entry in iterator:
                if (not entry.name.startswith(SOCKET_PREFIX)) or not entry.is_file(follow_symlinks=False):
                    continue
                lease: ConnectionLease | None = self._try_lock(entry.path, open_flags=self._open_flags)
                if lease is not None:
                    return lease
        return None

    def _create_and_acquire(self) -> ConnectionLease:
        max_rand: int = 999_999_999_999
        rand: random.Random = random.SystemRandom()
        while True:
            random_prefix: str = base32_str(rand.randint(0, max_rand), max_value=max_rand, padding=False)
            curr_time: str = base32_str(time.time_ns(), max_value=2**64 - 1, padding=False)
            socket_name: str = f"{SOCKET_PREFIX}{random_prefix}@{curr_time}"

            # Cap the socket_path length because the OS rejects Unix domain socket file paths that are too long. Yet
            # intentionally fail hard later on os.open(socket_path) if the OS cap limit cannot be met reasonably as the
            # Unix user name (which is part of the home directory path) happens to be unreasonably long.
            max_socket_path_len: int = max(
                UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH,
                len(self._sockets_dir) + 1 + len(SOCKET_PREFIX) + len(random_prefix),
            )
            socket_path: str = os.path.join(self._sockets_dir, socket_name)[:max_socket_path_len]

            free_path: str = os.path.join(self._free_dir, os.path.basename(socket_path))
            lease: ConnectionLease | None = self._try_lock(free_path, open_flags=self._open_flags | os.O_CREAT)
            if lease is not None:
                return lease

    def _try_lock(self, src_path: str, open_flags: int) -> ConnectionLease | None:
        fd: int = -1
        try:
            fd = os.open(src_path, flags=open_flags, mode=FILE_PERMISSIONS)

            # Acquire an exclusive lock; will raise an error if lock is already held by another process.
            # The (advisory) lock is auto-released when the process terminates or the fd is closed.
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # LOCK_NB ... non-blocking

            socket_name: str = os.path.basename(src_path)
            used_path: str = os.path.join(self._used_dir, socket_name)
            os.rename(src_path, used_path)  # mv lockfile atomically
        except OSError as e:
            close_quietly(fd)
            if isinstance(e, BlockingIOError):
                return None
            elif isinstance(e, FileNotFoundError):
                self._validate_dirs()
                return None
            raise
        else:
            return ConnectionLease(
                fd=fd,
                used_path=used_path,
                free_path=os.path.join(self._free_dir, socket_name),
                socket_path=os.path.join(self._sockets_dir, socket_name),
            )
