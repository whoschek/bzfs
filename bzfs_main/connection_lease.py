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

The higher-level goal is predictable performance, shorter critical paths, and less noisy-neighbor impact across frequent
periodic replication jobs at fleet scale, including at high concurrency. To achieve this, OpenSSH masters remain alive after
bzfs process termination (unless masters become idle for a specified time period), and inter-process reuse of ControlPath
sockets is coordinated safely so every new bzfs process benefits from an existing connection when present, and can recreate
it deterministically when absent. This amortizes startup costs, reduces tail latency, and improves operational reliability
without background daemons, external services, or warm-up procedures.

How This Is Achieved
--------------------
This module provides a small, fast, safe, and reliable mechanism to allocate and reuse unique Unix domain socket files
(ControlPaths) in a per-endpoint namespace, even under high concurrency. The full socket path namespace is ~/.ssh/bzfs/ (per
local user) plus a hashed subdirectory derived from the remote endpoint identity (user@host, port, ssh_config_file hash).
A bzfs process acquires an exclusive advisory file lock via flock(2) on a named empty lock file in the namespace directory
tree, then atomically moves it between free/ and used/ subdirectories to speed up later searches for available names in each
category of that namespace. The held lease exposes the open file descriptor (which maintains the lock) and the computed
ControlPath so callers can safely establish or reuse SSH master connections without races or leaks. Holding a lease's lock
for the duration of a `bzfs` process guarantees exclusive ownership of that SSH master while allowing the underlying TCP
connection to persist beyond bzfs process exit via OpenSSH ControlPersist.

Also see https://en.wikibooks.org/wiki/OpenSSH/Cookbook/Multiplexing
and https://chessman7.substack.com/p/how-ssh-multiplexing-reuses-master

Assumptions
-----------
- The filesystem is POSIX-compliant and supports ``fcntl.flock`` advisory locks, atomic ``os.rename`` file moves, and
  permissions enforcement. The process runs on the same host as the SSH client using the ControlPath.
- Low latency: scanning the free/ and used/ directories has bounded cost and is O(1) expected time because names are
  uniformly random. Directory contents are small and ephemeral.
- Crash recovery is acceptable by salvaging an unlocked file from used/; an unlock indicates process termination or orderly
  release. The flock itself is the source of truth; directory names merely provide fast classification.

Design Rationale
----------------
- File locks plus atomic renames form a minimal, portable concurrency primitive that composes well with process lifecycles.
  Locks are released automatically on process exit or when the file descriptor is closed, ensuring no cleanup logic is
  required after abnormal termination.
- The two-directory layout (free/ and used/) makes hot-path acquisition fast. We first probe free/ to reuse previously
  released names. If that doesn't produce an acquisition we probe used/ to salvage leftovers from crashed processes that no
  longer hold locks. If that doesn't produce an acquisition either we finally generate a new name. This yields a compact,
  garbage-free namespace without a janitor.
- Name generation mixes in a cryptographically strong random component, encoded in URL-safe base64 to avoid long names and
  path-unfriendly characters. This reduces collision probability and helps with human diagnostics.
- Security is prioritized: the root, sockets, and lease directories are created with explicit permissions, and symlinks are
  rejected to avoid redirection attacks. Open flags include ``O_NOFOLLOW`` and ``O_CLOEXEC`` to remove common foot-guns.
  Foreign file ownership and overly permissive file permissions are rejected to prevent sockets being used by third parties.
- The public API is intentionally tiny and ergonomic. ``ConnectionLeaseManager.acquire()`` never blocks: it either returns
  a lease for an existing name or a new name, ensuring predictable low latency under contention. The ``ConnectionLease``
  is immutable and simple to reason about, with an explicit ``release()`` to return capacity to the pool.
- No external services, daemons, or background threads are required. The design favors determinate behavior under failures,
  idempotent operations, and ease of testing. This approach integrates cleanly with ``bzfs`` where predictable SSH reuse and
  teardown during snapshot replication must remain both fast and safe.
"""

from __future__ import (
    annotations,
)
import fcntl
import logging
import os
import pathlib
import random
import time
from typing import (
    Final,
    NamedTuple,
)

from bzfs_main.utils import (
    DIR_PERMISSIONS,
    FILE_PERMISSIONS,
    LOG_TRACE,
    UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH,
    close_quietly,
    sha256_urlsafe_base64,
    urlsafe_base64,
    validate_file_permissions,
    validate_is_not_a_symlink,
)

# constants:
SOCKETS_DIR: Final[str] = "c"
FREE_DIR: Final[str] = "free"
USED_DIR: Final[str] = "used"
SOCKET_PREFIX: Final[str] = "s"
NAMESPACE_DIR_LENGTH: Final[int] = 43  # 43 Base64 chars contain the entire SHA-256 of the SSH endpoint


#############################################################################
class ConnectionLease(NamedTuple):
    """Purpose: Reduce SSH connection startup latency of a fresh bzfs process via safe OpenSSH ControlPath reuse.

    Assumptions: Callers hold this object only while the lease is needed. The file descriptor remains valid and keeps an
    exclusive advisory lock until ``release()`` or process exit. Paths are absolute and live within the manager's directory
    tree.

    Design Rationale: A small, immutable class captures just the essential handles needed for correctness and observability:
    the open fd, the used/ and free/ lock file paths, and the Unix domain socket ControlPath that SSH uses. Immutability
    prevents accidental mutation bugs, keeps equality semantics straightforward, and encourages explicit ownership transfer.
    A dedicated ``release()`` method centralizes cleanup and safe transition of the lock file to free/, followed by closing
    the fd for deterministic unlock.
    """

    # immutable variables:
    fd: int
    used_path: str
    free_path: str
    socket_path: str

    def release(self) -> None:
        """Releases the lease: moves the lock file from used/ dir back to free/ dir, then unlocks it by closing its fd."""
        try:
            os.rename(self.used_path, self.free_path)  # mv lock file atomically while holding the lock
        except FileNotFoundError:
            pass  # harmless
        finally:
            close_quietly(self.fd)  # release lock

    def set_socket_mtime_to_now(self) -> None:
        """Sets the mtime of the lease's ControlPath socket file to now; noop if the file is missing."""
        try:
            os.utime(self.socket_path, None)
        except FileNotFoundError:
            pass  # harmless


#############################################################################
class ConnectionLeaseManager:
    """Purpose: Reduce SSH connection startup latency of a fresh bzfs process via safe OpenSSH ControlPath reuse.

    Assumptions: The manager has exclusive control of its root directory subtree. The process operates on a POSIX-compliant
    filesystem and uses advisory locks consistently. Path lengths must respect common Unix domain socket limits. Callers
    use the socket path with OpenSSH.

    Design Rationale: A compact state machine with free/ and used/ stages, guided by atomically acquired file locks and
    renames, yields predictable latency, natural crash recovery, and zero background maintenance. The API is intentionally
    minimal to reduce misuse. Defensive checks and secure open flags balance correctness, performance, and security without
    external dependencies.
    """

    def __init__(
        self,
        root_dir: str,  # the local user is implied by root_dir
        namespace: str,  # derived from the remote endpoint identity (ssh_user_host, port, ssh_config_file hash)
        ssh_control_persist_secs: int = 90,  # TTL for garbage collecting stale files while preserving reuse of live masters
        *,
        log: logging.Logger,
    ) -> None:
        """Initializes manager with namespaced dirs and security settings for SSH ControlPath reuse."""
        # immutable variables:
        assert root_dir
        assert namespace
        assert ssh_control_persist_secs >= 1
        self._ssh_control_persist_secs: Final[int] = ssh_control_persist_secs
        self._log: Final[logging.Logger] = log
        ns: str = sha256_urlsafe_base64(namespace, padding=False)
        assert NAMESPACE_DIR_LENGTH >= 22  # a minimum degree of safety: 22 URL-safe Base64 chars = 132 bits of entropy
        ns = ns[0:NAMESPACE_DIR_LENGTH]
        namespace_dir: str = os.path.join(root_dir, ns)
        self._sockets_dir: Final[str] = os.path.join(namespace_dir, SOCKETS_DIR)
        self._free_dir: Final[str] = os.path.join(namespace_dir, FREE_DIR)
        self._used_dir: Final[str] = os.path.join(namespace_dir, USED_DIR)
        self._open_flags: Final[int] = os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC
        os.makedirs(root_dir, mode=DIR_PERMISSIONS, exist_ok=True)
        validate_is_not_a_symlink("connection lease root_dir ", root_dir)
        validate_file_permissions(root_dir, mode=DIR_PERMISSIONS)
        os.makedirs(namespace_dir, mode=DIR_PERMISSIONS, exist_ok=True)
        validate_is_not_a_symlink("connection lease namespace_dir ", namespace_dir)
        validate_file_permissions(namespace_dir, mode=DIR_PERMISSIONS)

    def _validate_dirs(self) -> None:
        """Ensures sockets/free/used directories exist, are not symlinks, and have strict permissions."""
        for _dir in (self._sockets_dir, self._free_dir, self._used_dir):
            os.makedirs(_dir, mode=DIR_PERMISSIONS, exist_ok=True)
            validate_is_not_a_symlink("connection lease dir ", _dir)
            validate_file_permissions(_dir, mode=DIR_PERMISSIONS)

    def acquire(self) -> ConnectionLease:  # thread-safe
        """Acquires and returns a ConnectionLease with an open, flocked fd and the SSH ControlPath aka socket file path."""
        self._validate_dirs()
        lease = self._find_and_acquire(self._free_dir)  # fast path: find free aka unlocked socket name in O(1) expected time
        if lease is not None:
            self._log.log(LOG_TRACE, "_find_and_acquire: %s", self._free_dir)
            return lease
        lease = self._find_and_acquire(self._used_dir)  # salvage from used yet unlocked socket names leftover from crash
        if lease is not None:
            self._log.log(LOG_TRACE, "_find_and_acquire: %s", self._used_dir)
            return lease
        lease = self._create_and_acquire()  # create new socket name
        self._log.log(LOG_TRACE, "_create_and_acquire: %s", self._free_dir)
        return lease

    def _find_and_acquire(self, scan_dir: str) -> ConnectionLease | None:
        """Scans a directory for an unlocked lease, prunes stale entries, and returns a locked lease if found."""
        with os.scandir(scan_dir) as iterator:
            for entry in iterator:
                if entry.name.startswith(SOCKET_PREFIX) and entry.is_file(follow_symlinks=False):
                    lease: ConnectionLease | None = self._try_lock(entry.path, open_flags=self._open_flags)
                    if lease is not None:
                        # If the control socket does not exist or is too old, then prune this entry to keep directory sizes
                        # bounded after crash storms, without losing opportunities to reuse a live SSH master connection.
                        delete_used_path: bool = False
                        try:
                            age_secs: float = time.time() - os.stat(lease.socket_path).st_mtime
                            if age_secs > self._ssh_control_persist_secs:  # old garbage left over from crash?
                                delete_used_path = True
                                os.unlink(lease.socket_path)  # remove control socket garbage while holding the lock
                        except FileNotFoundError:
                            delete_used_path = True  # control socket does not exist anymore
                        if not delete_used_path:
                            return lease  # return locked lease; this is the common case
                        try:  # Remove the renamed lock file at its current location under used/ while holding the lock
                            pathlib.Path(lease.used_path).unlink(missing_ok=True)
                        finally:
                            close_quietly(lease.fd)  # release lock
                        # keep scanning for a better candidate
        return None

    def _create_and_acquire(self) -> ConnectionLease:
        """Creates a new unique lease name, enforces socket path length, and returns a locked lease."""
        max_rand: int = 2**64 - 1
        rand: random.Random = random.SystemRandom()
        while True:
            random_prefix: str = urlsafe_base64(rand.randint(0, max_rand), max_value=max_rand, padding=False)
            socket_name: str = f"{SOCKET_PREFIX}{random_prefix}"
            socket_path: str = os.path.join(self._sockets_dir, socket_name)

            # Intentionally error out hard if the max OS Unix domain socket path limit cannot be met reasonably as the home
            # directory path is too long, typically because the Unix user name is unreasonably long. Failing fast here avoids
            # opaque OS errors later in `ssh`.
            socket_path_bytes: bytes = os.fsencode(socket_path)
            if len(socket_path_bytes) > UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH:
                raise OSError(
                    "SSH ControlPath exceeds Unix domain socket limit "
                    f"({len(socket_path_bytes)} > {UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH}); "
                    f"shorten it by shortening the home directory path: {socket_path}"
                )

            free_path: str = os.path.join(self._free_dir, os.path.basename(socket_path))
            lease: ConnectionLease | None = self._try_lock(free_path, open_flags=self._open_flags | os.O_CREAT)
            if lease is not None:
                return lease

    def _try_lock(self, src_path: str, open_flags: int) -> ConnectionLease | None:
        """Attempts to open and exclusively flock a lease file, atomically moves it to used/, and builds the lease."""
        fd: int = -1
        try:
            fd = os.open(src_path, flags=open_flags, mode=FILE_PERMISSIONS)

            # Acquire an exclusive lock; will raise a BlockingIOError if lock is already held by another process.
            # The (advisory) lock is auto-released when the process terminates or the fd is closed.
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # LOCK_NB ... non-blocking

            socket_name: str = os.path.basename(src_path)
            used_path: str = os.path.join(self._used_dir, socket_name)
            if src_path != used_path and os.path.exists(used_path):  # extremely rare name collision?
                close_quietly(fd)  # release lock
                return None  # harmless; retry with another name. See test_collision_on_create_then_salvage_and_release()

            # Rename cannot race: only free/<name> -> used/<name> produces used/ entries, and requires holding an exclusive
            # flock on free/<name>. We hold that lock while renaming. With exclusive subtree control, used/<name> cannot
            # appear between exists() and rename(). The atomic rename below is safe.
            os.rename(src_path, used_path)  # mv lock file atomically while holding the lock

        except OSError as e:
            close_quietly(fd)  # release lock
            if isinstance(e, BlockingIOError):
                return None  # lock is already held by another process
            elif isinstance(e, FileNotFoundError):
                self._validate_dirs()
                return None
            raise
        else:  # success
            return ConnectionLease(
                fd=fd,
                used_path=used_path,
                free_path=os.path.join(self._free_dir, socket_name),
                socket_path=os.path.join(self._sockets_dir, socket_name),
            )
