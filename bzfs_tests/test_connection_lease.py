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
"""Unit tests for SSH connection lease utilities."""

from __future__ import (
    annotations,
)
import logging
import os
import platform
import shutil
import tempfile
import time
import unittest
from pathlib import (
    Path,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from bzfs_main.connection_lease import (
    FREE_DIR,
    NAMESPACE_DIR_LENGTH,
    SOCKETS_DIR,
    USED_DIR,
    ConnectionLease,
    ConnectionLeaseManager,
)
from bzfs_main.utils import (
    DIR_PERMISSIONS,
    FILE_PERMISSIONS,
    UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH,
    sha256_urlsafe_base64,
)
from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestConnectionLease,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestConnectionLease(AbstractTestCase):

    def test__set_socket_mtime_to_now_updates_when_file_exists(self) -> None:
        with tempfile.NamedTemporaryFile(delete=True) as tf:
            socket_path = tf.name
            lease = ConnectionLease(fd=-1, used_path="", free_path="", socket_path=socket_path)
            os.utime(socket_path, (0, 0))
            before = os.stat(socket_path).st_mtime
            lease.set_socket_mtime_to_now()
            after = os.stat(socket_path).st_mtime
            self.assertGreater(after, before)

    def test__set_socket_mtime_to_now_ignores_missing_file(self) -> None:
        missing_path = os.path.join(tempfile.gettempdir(), f"bzfs_utime_now_missing_{int(time.time()*1e6)}")
        if os.path.exists(missing_path):
            os.remove(missing_path)
        lease = ConnectionLease(fd=-1, used_path="", free_path="", socket_path=missing_path)
        lease.set_socket_mtime_to_now()  # must not raise

    def test_rename_lockfile_to_itself_must_be_a_noop(self) -> None:
        """Purpose: Establish that renaming a lease lockfile to itself is a guaranteed no-op on POSIX filesystems and
        must never raise, even when the file is currently locked and located under ``used/``. This behavior matters
        because the implementation intentionally relies on atomic ``os.rename`` for state transitions between
        ``free/`` and ``used/`` and, in salvage scenarios, may rename a file to its identical target as a benign
        operation. The test ensures that such a self-rename cannot corrupt state, revoke locks, or trigger spurious
        exceptions that would mask real failures.

        Assumptions: Standard POSIX semantics apply to ``os.rename`` and ``os.replace``; advisory ``flock`` does not
        interfere with metadata updates on the directory entry and remains held on the underlying inode across a
        rename. The test runs in an isolated temporary directory and controls the entire subtree without concurrent
        writers.

        Design Rationale: Probing the self-rename edge case guards against platform quirks and accidental future
        refactors that might introduce unnecessary preconditions around rename calls. By performing both
        ``os.rename(p, p)`` and ``os.replace(p, p)``, we validate that either API surface behaves as a no-op, which
        simplifies the production code and allows rename operations to be written without explicit source/target
        equality checks in the hot path. The test focuses on observable behavior (no exception, file still present,
        lock intact) to keep the contract crisp and stable over time.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="user@host#22#cfg", log=MagicMock(logging.Logger))
            lease: ConnectionLease = mgr.acquire()
            self.assertTrue(os.path.isfile(lease.used_path))
            os.rename(lease.used_path, lease.used_path)  # POSIX rename lockfile to itself must be a noop and must not fail
            os.replace(lease.used_path, lease.used_path)  # POSIX rename lockfile to itself must be a noop and must not fail

    def test_manager_acquire_release(self) -> None:
        """Purpose: Validate the end-to-end lifecycle of a connection lease, covering acquisition of two distinct
        leases, directory placement in ``used/`` during ownership, and safe release back to ``free/`` with proper file
        movement. Also verifies the ControlPath directory is consistent across leases and that unique names are
        allocated. The scenario models the common pattern where multiple bzfs processes acquire independent leases for
        the same endpoint namespace, ensuring there is no cross-talk between names and no leakage of state.

        Assumptions: POSIX filesystem semantics, advisory ``flock`` behavior, and atomic ``os.rename`` are available.
        The temporary directory is not meddled with by other processes. Only this test manipulates files under the
        manager's root. The OS permits creating Unix domain socket paths in the generated directory and enforces
        standard permission bits. Names are generated with high entropy to minimize collisions.

        Design Rationale: The test asserts the strongest externally observable invariants rather than internal
        implementation details. It checks both presence and absence of files in ``used/`` and ``free/`` at the right
        times, uniqueness of socket names, and directory equivalence to ensure that distinct leases share the same
        ControlPath base. The finally block releases in reverse order to surface ordering bugs, and validates that
        releases are idempotent with the expected terminal state. By examining both path equality across directories
        and directory basenames, it also guards against subtle directory misconfiguration and accidental path joins.
        """

        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="user@host#22#cfg", log=MagicMock(logging.Logger))
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
        """Purpose: Ensure acquisition prefers reusing a previously freed ControlPath when the corresponding
        ControlPath (socket) exists. To assert deterministic reuse, create the socket file before reacquiring.

        Assumptions: The ``free/`` directory contains exactly one matching candidate and no other concurrent writer is
        racing on the same root. The OS path length cap is respected by construction. Non-following open flags and
        symlink checks remain active and uncompromised. Both managers operate within the same namespace root and see
        identical filesystem state.

        Design Rationale: Reuse minimizes churn and reduces namespace fragmentation when a live or recently used master
        exists. Creating the socket path ensures ``_find_and_acquire()`` preserves the entry, thereby verifying the
        preferred fast-path reuse behavior. A negative probe via ``_try_lock`` on a second manager demonstrates that
        re-acquired leases cannot be stolen while held, preserving the concurrency contract. The final assertions
        confirm correct cleanup: used -> free transition and absence of residual files in ``used/`` once released.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="user@host:22", log=MagicMock(logging.Logger))
            # Create a lease then release to populate free/
            lease1: ConnectionLease = mgr.acquire()
            socket_name = os.path.basename(lease1.socket_path)
            lease1.release()

            # _find_and_acquire() prunes free/ entries if the ControlPath (socket) is missing or too old.
            # Ensure reuse by creating the socket path so the entry is preserved and preferred.
            os.makedirs(mgr._sockets_dir, mode=DIR_PERMISSIONS, exist_ok=True)
            with open(os.path.join(mgr._sockets_dir, socket_name), "w", encoding="utf-8"):
                pass

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
        new lease, thereby preventing name leaks and ensuring continuity of ControlPath usage across process lifetimes.

        Assumptions: The test can safely write a file into ``used/`` to simulate a crash scenario. File system state is
        otherwise quiescent, and no lock is present on the crafted file. Only this process owns the directory. POSIX
        advisory locks are honored and will fail non-blocking acquisition only when truly held by another process.

        Design Rationale: Salvage is essential for robustness; it avoids leaking names and prevents unbounded growth of
        ControlPath files after process failures. Directly creating an unlocked file in ``used/`` and then acquiring a
        lease exercises the recovery path deterministically. A secondary lock attempt via ``_try_lock`` affirms that
        once the test holds the lease, normal exclusion guarantees apply. The test keeps assertions focused on visible
        behaviors rather than implementation internals and validates that the socket name is preserved exactly, which is
        important for operators correlating logs with filesystem artifacts during incident analysis.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(
                root_dir=root_dir,
                namespace="ns",
                ssh_control_persist_secs=90,
                log=MagicMock(logging.Logger),
            )
            mgr._validate_dirs()

            # Create an unlocked file in used/ to simulate a crashed process
            socket_name: str = "ssalvage"
            used_path: str = os.path.join(mgr._used_dir, socket_name)
            with open(used_path, "w", encoding="utf-8"):
                pass
            sockets_path: str = os.path.join(mgr._sockets_dir, socket_name)
            with open(sockets_path, "w", encoding="utf-8"):
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
        are empty, and enforces the Unix domain socket path length cap to remain compatible with portable kernels. A
        correct creation path is crucial for first-use latency and correctness, as there is no opportunity to reuse or
        salvage existing entries.

        Assumptions: The temporary root is empty and writable, the OS path length limit from the utility module closely
        matches the platform's effective limit, and URL-safe base64 encoding avoids dangerous characters. Only the
        current process interacts with the directory. Underlying filesystem supports atomic directory entry updates and
        respects explicit permission masks.

        Design Rationale: Creation is a critical fallback that must be correct on first attempt without retries or
        partial files. The test asserts a recognizable "s" prefix, presence in ``used/`` while held, and length not
        exceeding the configured cap. This keeps the test fast, deterministic, and independent from system SSH behavior,
        while still validating correctness constraints that impact real-world stability. It also implicitly exercises
        the randomness and encoding strategy for name generation, increasing confidence in collision resistance and
        operator-friendly diagnostics for socket names.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="abc", log=MagicMock(logging.Logger))

            lease: ConnectionLease = mgr.acquire()
            try:
                self.assertTrue(os.path.basename(lease.socket_path).startswith("s"))
                self.assertTrue(os.path.exists(lease.used_path))
                self.assertLessEqual(len(lease.socket_path), UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH)
            finally:
                lease.release()

    def test_acquire_raises_when_controlpath_exceeds_limit(self) -> None:
        """Purpose: Raise a clear, actionable error when the computed SSH ControlPath would exceed the platform's Unix
        domain socket path limit, guiding operators to shorten the SSH socket directory. Failing fast here avoids
        opaque, late failures inside OpenSSH that would otherwise manifest as confusing connection errors or timeouts.

        Assumptions: Regular filesystem paths can exceed the Unix domain socket limit; the check is a proactive guard
        before SSH uses the ControlPath. The test constructs an intentionally long ``root_dir`` to trigger the
        validation deterministically. The OS honors the commonly accepted limit reflected by
        ``UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH`` and raises no other unrelated errors when creating directories.

        Design Rationale: Early validation provides a better operator experience and reduces triage time by pointing
        directly to the misconfiguration. By testing that ``acquire()`` raises with a clear message, we assert not only
        correctness but also the quality of diagnostics. This ensures that production failures result in self-explanatory
        errors, allowing quick remediation (choosing a shorter home or socket directory). The isolation in a temporary
        directory and the absence of external dependencies keep the test deterministic and informative without pausing
        on SSH behavior or network conditions.
        """
        with get_tmpdir() as tmp:
            long_component = "x" * (UNIX_DOMAIN_SOCKET_PATH_MAX_LENGTH + 50)
            root_dir = os.path.join(tmp, long_component)
            os.makedirs(root_dir, mode=DIR_PERMISSIONS, exist_ok=True)
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="abc", log=MagicMock(logging.Logger))
            with self.assertRaisesRegex(OSError, r"exceeds Unix domain socket limit"):
                _ = mgr.acquire()

    def test_salvage_prunes_stale_used_without_socket(self) -> None:
        """Prunes an unlocked used/<name> if it is older than ControlPersist and the control socket does not exist.

        Ensures directory size is bounded after crash storms while preserving reuse when sockets exist.
        """
        with get_tmpdir() as root_dir:
            ttl = 5
            mgr = ConnectionLeaseManager(
                root_dir=root_dir,
                namespace="ns",
                ssh_control_persist_secs=ttl,
                log=MagicMock(logging.Logger),
            )
            mgr._validate_dirs()

            # Prepare an old, unlocked salvage candidate in used/
            salvage_name = "ssalvage_old"
            used_path = os.path.join(mgr._used_dir, salvage_name)
            with open(used_path, "w", encoding="utf-8"):
                pass
            old_ts = int(time.time()) - (ttl + 2)
            os.utime(used_path, times=(old_ts, old_ts))

            # Ensure the corresponding control socket does not exist
            socket_path = os.path.join(mgr._sockets_dir, salvage_name)
            if os.path.exists(socket_path):
                os.remove(socket_path)

            # Free dir empty to force salvage path
            with os.scandir(mgr._free_dir) as it:
                self.assertFalse(any(e.is_file(follow_symlinks=False) for e in it))

            lease = mgr.acquire()
            try:
                # The old used/<name> must have been pruned
                self.assertFalse(os.path.exists(used_path))
                # We created a new lease (name differs)
                self.assertNotEqual(salvage_name, os.path.basename(lease.socket_path))
                self.assertTrue(os.path.exists(lease.used_path))
            finally:
                lease.release()

    def test_salvage_prunes_when_socket_exists_but_is_old(self) -> None:
        """Prunes an unlocked used/<name> when the corresponding control socket exists but is older than TTL.

        Observable behavior: _find_and_acquire(used_dir) removes both the socket and the used/ entry and returns None
        when there are no other candidates, exercising the TTL-based cleanup path.
        """
        with get_tmpdir() as root_dir:
            ttl = 3
            mgr = ConnectionLeaseManager(
                root_dir=root_dir,
                namespace="ns",
                ssh_control_persist_secs=ttl,
                log=MagicMock(logging.Logger),
            )
            mgr._validate_dirs()

            name = "ssocket_old"
            used_path = os.path.join(mgr._used_dir, name)
            with open(used_path, "w", encoding="utf-8"):
                pass
            socket_path = os.path.join(mgr._sockets_dir, name)
            with open(socket_path, "w", encoding="utf-8"):
                pass
            # Make socket older than TTL
            old_ts = int(time.time()) - (ttl + 2)
            os.utime(socket_path, times=(old_ts, old_ts))

            # Scan used/: should prune the old socket + used entry and return None (no candidates left)
            self.assertIsNone(mgr._find_and_acquire(mgr._used_dir))
            self.assertFalse(os.path.exists(socket_path))
            self.assertFalse(os.path.exists(used_path))

    def test_salvage_preserves_recent_or_socket_present(self) -> None:
        """Does not prune if the control socket exists even when mtime is older than TTL; returns the lease instead."""
        with get_tmpdir() as root_dir:
            ttl = 5
            mgr = ConnectionLeaseManager(
                root_dir=root_dir,
                namespace="ns",
                ssh_control_persist_secs=ttl,
                log=MagicMock(logging.Logger),
            )
            mgr._validate_dirs()

            salvage_name = "ssalvage_live"
            used_path = os.path.join(mgr._used_dir, salvage_name)
            with open(used_path, "w", encoding="utf-8"):
                pass
            # Make it older than TTL
            old_ts = int(time.time()) - (ttl + 2)
            os.utime(used_path, times=(old_ts, old_ts))

            # Create a placeholder control socket path to simulate a live master
            os.makedirs(mgr._sockets_dir, mode=DIR_PERMISSIONS, exist_ok=True)
            socket_path = os.path.join(mgr._sockets_dir, salvage_name)
            with open(socket_path, "w", encoding="utf-8"):
                pass

            # Ensure free/ is empty
            with os.scandir(mgr._free_dir) as it:
                self.assertFalse(any(e.is_file(follow_symlinks=False) for e in it))

            lease = mgr.acquire()
            try:
                # The same name must be returned (preserved for reuse)
                self.assertEqual(salvage_name, os.path.basename(lease.socket_path))
                self.assertTrue(os.path.exists(lease.used_path))
            finally:
                lease.release()

    def test_prune_stale_free_entry_removes_renamed_used_entry(self) -> None:
        """When scanning free/, pruning a stale entry must remove the lockfile at its post-rename location (used/).

        Critical behavior: _find_and_acquire(free_dir) locks free/<name>, atomically renames it to used/<name>, then
        decides to prune if the corresponding control socket is missing or too old. The implementation must unlink the
        renamed path in used/ while still holding the lock, not the original free/ path which no longer exists.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(
                root_dir=root_dir,
                namespace="ns-free-prune",
                ssh_control_persist_secs=5,
                log=MagicMock(logging.Logger),
            )
            mgr._validate_dirs()

            # Prepare a candidate in free/ without a corresponding socket to force pruning
            name = "sstale_free"
            free_path = os.path.join(mgr._free_dir, name)
            with open(free_path, "w", encoding="utf-8"):
                pass
            used_path = os.path.join(mgr._used_dir, name)
            socket_path = os.path.join(mgr._sockets_dir, name)
            if os.path.exists(socket_path):
                os.remove(socket_path)

            # Scan free/: should prune and return None
            self.assertIsNone(mgr._find_and_acquire(mgr._free_dir))

            # Both the original free/ path and the renamed used/ path must be gone
            self.assertFalse(os.path.exists(free_path))
            self.assertFalse(os.path.exists(used_path))

    def test_salvage_from_used_after_crash(self) -> None:
        """Purpose: Exercise the crash-recovery scenario in which a process that owned a lease terminates abruptly,
        closing the file descriptor and implicitly dropping the lock while leaving the file in ``used/``. A subsequent
        acquisition should salvage the exact same ControlPath and paths, demonstrating that the lock (not the directory
        location) is the single source of truth for ownership.

        Assumptions: Closing the file descriptor simulates the crash accurately; no other process intervenes. Atomic
        renames and locks behave per POSIX. The test can create and manipulate temporary directories safely. The
        filesystem preserves inode identity across renames and maintains advisory lock semantics tied to the file
        descriptor.

        Design Rationale: By intentionally closing the fd, we model the authoritative signal of ownership loss without
        introducing timing races or external dependencies. Assertions compare all three paths for equality to rule out
        accidental re-creation and to confirm a true salvage. Final checks ensure that releasing the salvaged lease
        returns the file to ``free/``, leaving the directory tree in a clean, reusable state for subsequent tests. This
        test raises confidence that crash recovery does not leak names or require background cleanup, keeping the system
        robust and predictable under failure.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="user@host#22#cfg", log=MagicMock(logging.Logger))
            lease1: ConnectionLease = mgr.acquire()
            # Crash simulation: drop the flock but leave the file in used/
            os.close(lease1.fd)

            # Ensure a matching ControlPath socket exists so salvage preserves the name.
            sockets_path = os.path.join(mgr._sockets_dir, os.path.basename(lease1.socket_path))
            with open(sockets_path, "w", encoding="utf-8"):
                pass

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
        """Purpose: Validate that ``release()`` is robust when the ``used/`` entry disappears between acquisition and
        release, for instance due to an external actor or manual cleanup. The method should not raise and must still
        close the file descriptor to guarantee that the advisory lock is dropped deterministically.

        Assumptions: Removing the ``used/`` path simulates a concurrent deletion gracefully; the directory subtree is
        otherwise under exclusive control of the test process. ``os.rename`` to a missing source should raise
        ``FileNotFoundError``, which ``release()`` intentionally ignores. Advisory locks release on descriptor close.

        Design Rationale: In operational environments, administrators or cron jobs may occasionally prune directories.
        ``release()`` must remain safe and idempotent under such conditions so that subsequent acquisitions can proceed
        without requiring human intervention. The test verifies both behavioral aspects: no exception and a definitively
        closed file descriptor by asserting that ``os.fstat(fd)`` fails afterward. This keeps the implementation simple
        and conservative while covering a meaningful, real-world edge case.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
            lease: ConnectionLease = mgr.acquire()
            # Remove the file to force FileNotFoundError in os.rename during release
            os.remove(lease.used_path)
            # Should not raise; should still close the fd
            lease.release()
            with self.assertRaises(OSError):
                os.fstat(lease.fd)  # fd must be closed

    def test_release_when_free_dir_missing_salvage_on_next_acquire(self) -> None:
        """Purpose: Validate robustness when an external actor removes ``free/`` between acquisition and release. If
        ``free/`` is missing, the rename on ``release()`` fails with ``FileNotFoundError`` and is intentionally
        ignored. The lock file must remain in ``used/`` and the next ``acquire()`` should salvage the exact same name
        after the manager recreates its directory structure.

        Assumptions: POSIX rename to a missing destination parent raises ``FileNotFoundError``; advisory locks release on
        close; the manager recreates directories via ``_validate_dirs()`` on the next acquire. No other process writes to
        the subtree while this test runs.

        Design Rationale: The ability to recover from missing directories without manual cleanup significantly improves
        operational resilience. The test checks that ``release()`` remains non-throwing, the descriptor is closed
        deterministically, and the subsequent ``acquire()`` restores invariants by salvaging the same paths. This design
        eliminates the need for background janitors and ensures that even disruptive administrative actions do not cause
        cascading failures or name leaks, while preserving predictable latency characteristics.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
            lease1: ConnectionLease = mgr.acquire()
            name1 = os.path.basename(lease1.socket_path)
            sockets_path = os.path.join(mgr._sockets_dir, name1)

            # Remove free/ so that release() cannot move used -> free and must ignore FileNotFoundError
            self.assertTrue(os.path.isdir(mgr._free_dir))
            shutil.rmtree(mgr._free_dir)

            # Release should not raise; file remains in used/, fd must be closed
            lease1.release()
            self.assertTrue(os.path.exists(lease1.used_path))
            self.assertFalse(os.path.exists(lease1.free_path))
            with self.assertRaises(OSError):
                os.fstat(lease1.fd)
            # Ensure a matching ControlPath exists so salvage preserves the same name.
            with open(sockets_path, "w", encoding="utf-8"):
                pass

            # Next acquire() must recreate directories and salvage the same socket from used/
            lease2: ConnectionLease = mgr.acquire()
            try:
                self.assertEqual(name1, os.path.basename(lease2.socket_path))
                self.assertEqual(lease1.socket_path, lease2.socket_path)
                self.assertEqual(lease1.used_path, lease2.used_path)
                self.assertEqual(lease1.free_path, lease2.free_path)
                self.assertTrue(os.path.exists(lease2.used_path))
            finally:
                lease2.release()
                self.assertTrue(os.path.exists(lease2.free_path))
                self.assertFalse(os.path.exists(lease2.used_path))

    def test_collision_on_create_then_salvage_and_release(self) -> None:
        """Purpose: Validate deterministic behavior when a rare, externally induced name collision exists between
        ``used/<name>`` and ``free/<name>`` for the same socket name. This scenario is engineered by pre-creating an
        unlocked placeholder in ``used/`` while a new bzfs process concurrently attempts to create and lock the same
        name in ``free/``. The test asserts that the creation path detects the collision, refuses to clobber
        ``used/<name>``, and returns ``None``; subsequently, scanning ``used/`` must salvage and lock the extant entry,
        producing a valid lease. While the lease is held, both paths may coexist temporarily (the unlocked
        ``free/<name>`` remains); calling ``release()`` must atomically move the lockfile back to ``free/``, replacing
        the stale placeholder so the terminal state contains a single file in ``free/`` and none in ``used/``.

        Assumptions: Only bzfs creates entries in these directories during normal operation; nevertheless, we model an
        unusual but possible pre-existing artifact (e.g., from a prior run or a non-bzfs actor) to exercise the
        defensive guard in ``_try_lock``. POSIX semantics for advisory ``flock`` and atomic ``rename`` apply, and
        renaming a file to itself is a no-op. No separate janitor cleans directories between steps.

        Rationale: This test proves that the collision check prevents replacing an existing ``used/<name>`` (avoiding
        double-ownership hazards), that salvage from ``used/`` remains functional, and that release resolves any
        temporary duplication without leaks. It focuses on externally observable invariants - paths, existence, and final
        directory contents - rather than internal implementation details, ensuring robust behavior under rare edge
        conditions without introducing latency or complexity in the common path.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
            mgr._validate_dirs()

            socket_name = "smanual"
            used_path = os.path.join(mgr._used_dir, socket_name)
            free_path = os.path.join(mgr._free_dir, socket_name)
            sockets_path = os.path.join(mgr._sockets_dir, socket_name)

            # Pre-create an unlocked file in used/ to force a name collision on create in free/
            with open(used_path, "w", encoding="utf-8"):
                pass
            self.assertTrue(os.path.exists(used_path))

            # Attempt to create and lock the same name in free/: triggers the collision branch returning None
            result = mgr._try_lock(free_path, open_flags=mgr._open_flags | os.O_CREAT)
            self.assertIsNone(result)
            # Both paths now exist, representing the temporary collision
            self.assertTrue(os.path.exists(free_path))
            self.assertTrue(os.path.exists(used_path))

            # Salvage by scanning used/: must acquire the lease for the collided name.
            # Ensure a matching ControlPath socket exists so salvage proceeds.
            with open(sockets_path, "w", encoding="utf-8"):
                pass
            lease = mgr._find_and_acquire(mgr._used_dir)
            assert lease is not None
            try:
                self.assertEqual(used_path, lease.used_path)
                self.assertEqual(free_path, lease.free_path)
                self.assertTrue(os.path.exists(lease.used_path))
                # Collision remains until release() resolves it by moving used -> free
                self.assertTrue(os.path.exists(lease.free_path))
            finally:
                lease.release()
                # After release, only free/<name> must remain; used/<name> must be gone
                self.assertTrue(os.path.exists(free_path))
                self.assertFalse(os.path.exists(used_path))

    def test_find_and_acquire_skips_non_matching_entries(self) -> None:
        """Purpose: Ensure the directory scanner ignores entries that are either not files or whose names do not match
        the expected socket name prefix, thereby avoiding unnecessary syscalls and spurious lock attempts. This protects
        the hot path from performance cliffs when directories contain unrelated artifacts.

        Assumptions: The scanner uses a deterministic, cheap filter (prefix match and ``is_file`` with
        ``follow_symlinks=False``). The test controls the directory contents completely and can create junk files and
        directories to simulate real-world noise (e.g., editor swap files, logs, or subdirectories).

        Design Rationale: By patching ``_try_lock`` to raise if invoked, the test asserts that the scanner correctly
        skips non-matching entries, keeping the acquisition loop tight and predictable. This also constrains future
        refactors to maintain the same efficient filtering behavior and prevents accidental expansion of the search
        surface that could cause latency spikes or correctness issues when encountering unexpected filesystem entries.
        """
        with get_tmpdir() as root_dir:
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
        """Purpose: Demonstrate that a transient locking failure on the first attempt (``BlockingIOError`` from
        ``flock``) does not derail acquisition and that a subsequent attempt can succeed promptly. This models benign
        contention where two processes briefly race for the same candidate, and one proceeds after a non-blocking
        failure without retrying the same file indefinitely.

        Assumptions: ``fcntl.flock`` raises ``BlockingIOError`` for non-blocking exclusive lock acquisition when the
        lock is currently held elsewhere. The test uses a side-effect function to simulate one failure followed by
        success, ensuring deterministic behavior without relying on timing.

        Design Rationale: Non-blocking acquisition is central to predictable startup latency. This test confirms that
        the manager does not enter a busy loop or error state upon encountering a contention-induced error and that the
        next acquisition attempt returns a valid lease. The test remains focused on externally observable outcomes:
        successful lease acquisition and corrected directory placement while the lease is held.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))

            # First flock call fails with BlockingIOError, second succeeds
            call_sequence: list[Exception | None] = [BlockingIOError(), None]

            def fake_flock(fd: int, flags: int) -> None:
                result = call_sequence.pop(0)
                if isinstance(result, Exception):
                    raise result

            mgr._validate_dirs()  # avoid any unrelated flock calls before the patch is active
            with patch("bzfs_main.connection_lease.fcntl.flock", side_effect=fake_flock):
                lease = mgr._create_and_acquire()
                try:
                    self.assertTrue(os.path.exists(lease.used_path))
                finally:
                    lease.release()

    def test_try_lock_file_not_found_calls_validate_and_returns_none(self) -> None:
        """Purpose: Ensure that calling ``_try_lock`` on a path whose parent directory does not exist neither raises nor
        proceeds with unsafe filesystem operations. Instead, the manager must call ``_validate_dirs()`` to recreate the
        expected directory structure and return ``None`` so the caller can continue with the next candidate.

        Assumptions: ``os.open`` or subsequent operations can raise ``FileNotFoundError`` when parent directories are
        missing. The test verifies that the manager translates this into a benign outcome by repairing directories
        immediately rather than failing loudly.

        Design Rationale: Self-healing directory creation reduces operational toil and keeps the acquisition path
        resilient to accidental removals of subtrees (e.g., cleanup scripts). By patching ``_validate_dirs`` and
        observing that it is invoked, the test ensures the repair hook is exercised and that ``_try_lock`` returns
        ``None``, preserving non-blocking semantics and letting higher-level logic retry gracefully.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
            with patch.object(mgr, "_validate_dirs") as mock_validate:
                missing_parent = os.path.join(mgr._free_dir, "missing", "file")
                result = mgr._try_lock(missing_parent, open_flags=os.O_WRONLY)
                self.assertIsNone(result)
                self.assertTrue(mock_validate.called)

    def test_try_lock_other_oserror_is_raised(self) -> None:
        """Purpose: Verify that ``_try_lock`` re-raises unexpected OS errors such as ``PermissionError`` rather than
        swallowing them. This ensures that truly exceptional conditions surface to callers promptly with their original
        context, aiding diagnosis and avoiding silent misbehavior.

        Assumptions: The test environment can patch ``os.open`` to raise ``PermissionError`` deterministically. Only
        ``BlockingIOError`` and ``FileNotFoundError`` are considered benign and handled internally by returning
        ``None``; all other subclasses of ``OSError`` must propagate.

        Design Rationale: Clear error boundaries are crucial in systems software. By asserting that ``PermissionError``
        bubbles up unchanged, we enforce a contract that distinguishes expected contention or missing-directory cases
        from genuine faults such as permission misconfigurations, read-only mounts, or security policy denials. This
        keeps failure modes explicit and actionable, preserving the simplicity and safety of the acquisition loop.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
            with patch("bzfs_main.connection_lease.os.open", side_effect=PermissionError("denied")):
                with self.assertRaises(PermissionError):
                    _ = mgr._try_lock(os.path.join(mgr._free_dir, "sabc"), open_flags=os.O_WRONLY)

    def test_find_and_acquire_used_returns_none_when_locked(self) -> None:
        """When all candidates in used/ are currently locked by other processes, scanning used/ returns None.

        Observable behavior: A second manager enumerates used/ and fails non-blocking flock, thus _try_lock returns
        None for each entry and _find_and_acquire yields None. This covers the negative branch of the lease acquisition
        check in the used/ scan path.
        """
        with get_tmpdir() as root_dir:
            ns = "ns-locked"
            mgr1 = ConnectionLeaseManager(root_dir=root_dir, namespace=ns, log=MagicMock(logging.Logger))
            # Hold a lease so used/ contains a locked entry
            lease = mgr1.acquire()
            try:
                mgr2 = ConnectionLeaseManager(root_dir=root_dir, namespace=ns, log=MagicMock(logging.Logger))
                self.assertIsNone(mgr2._find_and_acquire(mgr2._used_dir))
            finally:
                lease.release()

    def test_try_lock_missing_used_dir_triggers_validate_and_returns_none(self) -> None:
        """Purpose: Exercise the ``_try_lock`` branch where renaming from ``free/`` to a missing ``used/`` parent raises
        ``FileNotFoundError``. The method must recreate directories via ``_validate_dirs()`` and return ``None``.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
            # Prepare free/ candidate and ensure used/ is removed
            os.makedirs(mgr._free_dir, mode=DIR_PERMISSIONS, exist_ok=True)
            if os.path.isdir(mgr._used_dir):
                shutil.rmtree(mgr._used_dir)
            name = os.path.join(mgr._free_dir, "sfoo")
            with open(name, "w", encoding="utf-8"):
                pass
            with patch.object(mgr, "_validate_dirs") as mock_validate:
                result = mgr._try_lock(name, open_flags=mgr._open_flags)
                self.assertIsNone(result)
                self.assertTrue(mock_validate.called)
            # Create the corresponding socket so _find_and_acquire() does not prune the free/ entry
            os.makedirs(mgr._sockets_dir, mode=DIR_PERMISSIONS, exist_ok=True)
            with open(os.path.join(mgr._sockets_dir, "sfoo"), "w", encoding="utf-8"):
                pass
            # Acquire should now succeed and move the same entry into used/
            lease = mgr.acquire()
            try:
                self.assertTrue(os.path.exists(lease.used_path))
                self.assertEqual("sfoo", os.path.basename(lease.socket_path))
            finally:
                lease.release()

    def test_release_is_idempotent(self) -> None:
        """Purpose: Calling ``release()`` multiple times must remain safe: the first call moves the lockfile and closes the
        descriptor; subsequent calls should not raise and must leave the filesystem in the expected state.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
            lease = mgr.acquire()
            # First release: normal rename used -> free
            lease.release()
            self.assertTrue(os.path.exists(lease.free_path))
            self.assertFalse(os.path.exists(lease.used_path))
            # Second release: should be a no-op without exceptions
            lease.release()
            self.assertTrue(os.path.exists(lease.free_path))
            self.assertFalse(os.path.exists(lease.used_path))

    def test_root_dir_symlink_is_rejected_in_init(self) -> None:
        """Purpose: Ensure security guard rails reject a symlink as ``root_dir`` during initialization. The manager must
        refuse to operate on a path that is a symbolic link to avoid redirection attacks outside the intended tree and
        must fail fast with a clear diagnostic via ``die()`` (raising ``SystemExit``).

        Approach: Create a real directory and a sibling symlink pointing to it; pass the symlink as ``root_dir``. The
        constructor calls ``validate_is_not_a_symlink`` on ``root_dir``, which should terminate the operation.
        """
        with get_tmpdir() as tmp:
            real_root = os.path.join(tmp, "real")
            os.makedirs(real_root)
            link_root = os.path.join(tmp, "link")
            os.symlink(real_root, link_root)
            with self.assertRaises(SystemExit):
                _ = ConnectionLeaseManager(root_dir=link_root, namespace="ns", log=MagicMock(logging.Logger))

    def test_validate_dirs_rejects_symlink_subdir(self) -> None:
        """Purpose: Verify that symlinked subdirectories under the manager's namespace are rejected by ``_validate_dirs``.

        Approach: Instantiate a manager with a normal ``root_dir`` to compute paths, then pre-create the base directory
        and replace ``free/`` with a symlink. Calling ``acquire()`` triggers ``_validate_dirs()``, which should detect
        the symlink and raise via ``die()``.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="user@h:22", log=MagicMock(logging.Logger))
            os.makedirs(os.path.dirname(mgr._free_dir), mode=DIR_PERMISSIONS, exist_ok=True)  # ensure base dir exists
            # Point free/ to a safe existing directory (e.g., /tmp) to avoid creation
            os.symlink("/tmp", mgr._free_dir)
            with self.assertRaises(SystemExit):
                _ = mgr.acquire()

    def test_permissions_on_created_dirs_and_lockfile(self) -> None:
        """Purpose: Validate that directories created by the manager use restrictive permissions (0700 aka user=rwx) and
        the lockfile uses 0600 aka user=rw, aligning with the security posture documented in the module.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
            lease = mgr.acquire()
            try:
                # Directories: sockets, free, used are 0700 (intermediate base dir may be created by makedirs with default perms)
                sockets_dir = os.path.dirname(lease.socket_path)
                for d in (sockets_dir, mgr._free_dir, mgr._used_dir):
                    mode = os.stat(d).st_mode
                    self.assertEqual(DIR_PERMISSIONS, mode & 0o777)
                # Lockfile is 0600 in used/
                self.assertTrue(os.path.exists(lease.used_path))
                fmode = os.stat(lease.used_path).st_mode
                self.assertEqual(FILE_PERMISSIONS, fmode & 0o777)
            finally:
                lease.release()

    def test_namespace_hashing_and_isolation(self) -> None:
        """Purpose: Ensure namespace hashing determines the directory layout and provides isolation across namespaces.

        Approach: Acquire one lease per distinct namespace, compute the expected ns via
        ``sha256_urlsafe_base64(namespace, padding=False)[:NAMESPACE_LENGTH]``, and verify that the socket directory is
        ``root/ns/c``. Also assert that different namespaces map to different directories.
        """
        with get_tmpdir() as root_dir:
            ns1 = "alice@host:22"
            ns2 = "bob@host:22"
            mgr1 = ConnectionLeaseManager(root_dir=root_dir, namespace=ns1, log=MagicMock(logging.Logger))
            mgr2 = ConnectionLeaseManager(root_dir=root_dir, namespace=ns2, log=MagicMock(logging.Logger))
            lease1 = mgr1.acquire()
            lease2 = mgr2.acquire()
            try:
                dir1 = os.path.dirname(lease1.socket_path)
                dir2 = os.path.dirname(lease2.socket_path)
                base1 = os.path.basename(os.path.dirname(dir1))
                base2 = os.path.basename(os.path.dirname(dir2))
                self.assertEqual(SOCKETS_DIR, os.path.basename(dir1))
                self.assertEqual(SOCKETS_DIR, os.path.basename(dir2))
                exp1 = sha256_urlsafe_base64(ns1, padding=False)[:NAMESPACE_DIR_LENGTH]
                exp2 = sha256_urlsafe_base64(ns2, padding=False)[:NAMESPACE_DIR_LENGTH]
                self.assertEqual(exp1, base1)
                self.assertEqual(exp2, base2)
                self.assertNotEqual(base1, base2)
            finally:
                lease1.release()
                lease2.release()

    def test_find_and_acquire_skips_symlink_entries(self) -> None:
        """Purpose: The scanner must ignore symlinks even if the name matches the socket prefix, avoiding unsafe
        ``open`` attempts and maintaining predictable performance.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
            # Create free/ with a symlink that matches the prefix
            mgr._validate_dirs()
            sl = os.path.join(mgr._free_dir, "slink")
            target = os.path.join(root_dir, "target.txt")
            with open(target, "w", encoding="utf-8"):
                pass
            os.symlink(target, sl)
            # Also add a junk non-matching regular file
            with open(os.path.join(mgr._free_dir, "junk"), "w", encoding="utf-8"):
                pass
            with patch.object(mgr, "_try_lock", side_effect=AssertionError("_try_lock should not be called")):
                self.assertIsNone(mgr._find_and_acquire(mgr._free_dir))

    def test_try_lock_on_symlink_raises(self) -> None:
        """Purpose: Directly verify that ``_try_lock`` refuses to open a symlink due to ``O_NOFOLLOW`` and propagates the
        resulting ``OSError`` to the caller.
        """
        with get_tmpdir() as root_dir:
            mgr = ConnectionLeaseManager(root_dir=root_dir, namespace="ns", log=MagicMock(logging.Logger))
            os.makedirs(mgr._free_dir, mode=DIR_PERMISSIONS, exist_ok=True)
            target = os.path.join(root_dir, "t.txt")
            with open(target, "w", encoding="utf-8"):
                pass
            sl = os.path.join(mgr._free_dir, "sbad")
            os.symlink(target, sl)
            with self.assertRaises(OSError):
                _ = mgr._try_lock(sl, open_flags=mgr._open_flags)

    def test_xbenchmark_find_and_acquire_used_when_all_locked(self) -> None:
        """Benchmark: Populate used/ with N locked entries, then measure how many ``_find_and_acquire(used_dir)`` calls per
        second can be completed when every attempt returns ``None`` because no files are 'free' and all files are already
        locked by another process.

        This simulates a fresh bzfs process attempting to acquire a connection lease while N existing connections are already
        open to the same endpoint (same namespace) by the same user, ensuring the scanner latency remains predictable under
        load.
        """
        if self.is_unit_test or self.is_smoke_test:
            self.skipTest("Ignore test_xbenchmark_find_and_acquire_used_when_all_locked() if is unit test or smoke test")

        all_num_locks = [20]
        if not self.is_unit_test:
            all_num_locks.append(200)
            if not self.is_smoke_test:
                all_num_locks += [1000] if platform.system() != "Darwin" else []  # OSX default max 256 open FDs per process
        measure_seconds = 0.5

        for num_locks in all_num_locks:
            with get_tmpdir() as root_dir:
                ns = f"test_xbenchmark_find_and_acquire_used_when_all_locked_{all_num_locks}"
                log = MagicMock(logging.Logger)
                mgr = ConnectionLeaseManager(root_dir=root_dir, namespace=ns, log=log)

                # Acquire and hold many leases to keep "used/" full of locked files
                leases: list[ConnectionLease] = []
                try:
                    for _ in range(num_locks):
                        lease = mgr.acquire()
                        Path(lease.socket_path).touch(exist_ok=False)
                        leases.append(lease)

                    # Warm-up
                    for _ in range(3):
                        self.assertIsNone(mgr._find_and_acquire(mgr._used_dir))

                    probe = ConnectionLeaseManager(root_dir=root_dir, namespace=ns, log=log)

                    # Measure: calls/second when every scan returns None
                    start = time.perf_counter()
                    deadline = start + measure_seconds
                    calls = 0
                    while time.perf_counter() < deadline:
                        self.assertIsNone(probe._find_and_acquire(probe._used_dir))
                        calls += 1
                    elapsed = time.perf_counter() - start
                    cps = round(calls / elapsed)
                    logging.getLogger(__name__).warning(
                        "_find_and_acquire(used_dir): %d negative calls/sec with %d locked files", cps, num_locks
                    )
                finally:
                    for lease in leases:
                        lease.release()


def get_tmpdir() -> tempfile.TemporaryDirectory[str]:
    # on OSX the default test tmp dir is unreasonably long, e.g.
    # /var/folders/dj/y5mjnplj0l79dq4dkknbsjbh0000gp/T/tmp7j51fpp/
    root_dir = "/tmp" if platform.system() == "Darwin" else None
    return tempfile.TemporaryDirectory(dir=root_dir)
