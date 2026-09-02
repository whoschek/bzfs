#!/usr/bin/env python3
#
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
"""Runs one periodic backup job and notifies the operator once the job stays broken for longer than a grace period.

Purpose: a single failed run of a replication job is normal (a host was rebooting, a network hiccup, a laptop lid was
closed mid-transfer); a job that keeps failing is not. Assumptions: invoked as `bzfs_alert.py <job> <command> [args]`
by cron or a systemd timer, once per job cycle; the notification transport is an external command that takes the
subject as its only argument and the body on stdin. Design rationale: alert state is derived from the *current* run of
consecutive failures rather than from the time of the last success, because `srchost` is asleep most of the time and
would otherwise alert on its first attempt after every longer absence.
"""

from __future__ import (
    annotations,
)
import os
import socket
import subprocess
import sys
import time
from subprocess import (
    DEVNULL,
    PIPE,
    STDOUT,
)
from typing import (
    Final,
)

# For each job: how long the job must fail continuously before the first alert, how many consecutive failures are
# required regardless of elapsed time, and how long to stay quiet before repeating an alert for an ongoing outage.
# The values encode how much downtime is normal for the host that runs the job: `srchost` retries every 15 minutes
# while it is awake, whereas anything involving `archost` must tolerate a machine that is offline most of the day.
JOBS: Final = {  # EDIT ME (optional): job name -> (grace_secs, min_consecutive_failures, renotify_secs)
    "src-to-bckp": (2 * 3600, 3, 12 * 3600),  # srchost fails to push to bckphost
    "bckp-prune": (6 * 3600, 3, 24 * 3600),  # bckphost fails to prune its sliding window
    "bckp-freshness": (24 * 3600, 2, 7 * 86400),  # bckphost holds nothing recent from srchost
    "bckp-bookmarks": (24 * 3600, 3, 7 * 86400),  # bckphost fails to prune its bookmarks
    "arc-freshness": (36 * 3600, 3, 24 * 3600),  # archost holds nothing recent, or is unreachable from bckphost
    "bckp-to-arc": (36 * 3600, 3, 24 * 3600),  # archost fails to pull from bckphost
}

HOME_DIR: Final = os.path.expanduser("~")  # same home directory that bzfs_alert.sh uses, so the two stay interchangeable
SCRIPT_DIR: Final = os.path.dirname(os.path.abspath(__file__))
NOTIFY_COMMAND: Final = os.path.join(SCRIPT_DIR, "bzfs_notify.sh")  # EDIT ME (optional): the notification transport
STATE_DIR: Final = os.path.join(HOME_DIR, ".bzfs-alert")  # one small state file per job
STILL_RUNNING_STATUS: Final = 4  # `bzfs` exit code meaning "the same previous periodic job has not completed yet"
MAX_OUTPUT_LINES: Final = 40  # how much of the failed job's output to quote in the notification


def read_state(path: str) -> dict[str, int]:
    """Returns the persisted alert state of a job, or an empty state if the job has no history yet."""
    state: dict[str, int] = {}
    try:
        with open(path, encoding="utf-8") as fd:
            for line in fd:
                key, sep, value = line.strip().partition("=")
                if sep and value.lstrip("-").isdigit():
                    state[key] = int(value)
    except FileNotFoundError:
        pass
    return state


def write_state(path: str, state: dict[str, int]) -> None:
    """Persists the alert state of a job, atomically, so that a crash cannot truncate it into an unreadable state."""
    os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as fd:
        fd.writelines(f"{key}={state[key]}\n" for key in sorted(state))
        fd.flush()
        os.fsync(fd.fileno())
    os.replace(tmp_path, path)


def notify(subject: str, body: str) -> None:
    """Hands a notification to the external transport; never lets a broken transport mask the job's own outcome."""
    try:
        subprocess.run([NOTIFY_COMMAND, subject], input=body, stdout=None, stderr=None, text=True, check=True)
    except (OSError, subprocess.SubprocessError) as e:
        print(f"WARNING: notification command {NOTIFY_COMMAND} failed: {e}", file=sys.stderr)


def humanize(seconds: int) -> str:
    """Returns a compact human readable duration such as '3.2h' or '1.5d'."""
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def main() -> None:
    """Runs the job named by the first argument and updates its alert state according to the outcome."""
    if len(sys.argv) < 3:
        sys.exit(f"Usage: {sys.argv[0]} <job> <command> [args...]; known jobs: {sorted(JOBS)}")
    job: Final[str] = sys.argv[1]
    command: Final[list[str]] = sys.argv[2:]
    if job not in JOBS:
        sys.exit(f"Unknown job: {job}; known jobs: {sorted(JOBS)}")
    grace_secs, min_failures, renotify_secs = JOBS[job]
    hostname: Final[str] = socket.gethostname()
    state_path: Final[str] = os.path.join(STATE_DIR, job + ".state")

    result = subprocess.run(command, stdin=DEVNULL, stdout=PIPE, stderr=STDOUT, text=True, check=False)
    sys.stdout.write(result.stdout)
    sys.stdout.flush()

    if result.returncode == STILL_RUNNING_STATUS:  # a longer run of the same job is still in flight; not an outage
        print(f"Skipped {job}: the previous run has not completed yet.")
        return

    now: Final[int] = int(time.time())
    state = read_state(state_path)
    tail: Final[str] = "\n".join(result.stdout.splitlines()[-MAX_OUTPUT_LINES:])

    if result.returncode == 0:
        if state.get("alerting", 0):
            outage_secs = now - state.get("first_failure_epoch", now)
            notify(
                f"[bzfs] RECOVERED: {job} on {hostname}",
                f"Job     : {job}\nHost    : {hostname}\nCommand : {' '.join(command)}\n"
                f"Outage  : {humanize(outage_secs)}\n\nThe job succeeded again; no action required.\n",
            )
        write_state(state_path, {"consecutive_failures": 0, "last_success_epoch": now})
    else:
        failures = state.get("consecutive_failures", 0) + 1
        first_failure_epoch = state.get("first_failure_epoch") or now
        last_alert_epoch = state.get("last_alert_epoch", 0)
        alerting = state.get("alerting", 0)
        failing_secs = now - first_failure_epoch
        due = last_alert_epoch == 0 or now - last_alert_epoch >= renotify_secs
        if failures >= min_failures and failing_secs >= grace_secs and due:
            last_success = state.get("last_success_epoch", 0)
            last_success_text = humanize(now - last_success) + " ago" if last_success else "never (no success recorded)"
            notify(
                f"[bzfs] FAILING: {job} on {hostname}",
                f"Job          : {job}\nHost         : {hostname}\nCommand      : {' '.join(command)}\n"
                f"Exit code    : {result.returncode}\nFailing for  : {humanize(failing_secs)}"
                f" ({failures} consecutive failures)\nLast success : {last_success_text}\n\n"
                f"Last {MAX_OUTPUT_LINES} lines of output:\n{tail}\n",
            )
            last_alert_epoch = now
            alerting = 1
        write_state(
            state_path,
            {
                "consecutive_failures": failures,
                "first_failure_epoch": first_failure_epoch,
                "last_success_epoch": state.get("last_success_epoch", 0),
                "last_alert_epoch": last_alert_epoch,
                "alerting": alerting,
            },
        )
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
