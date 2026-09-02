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
"""Runs one scheduled job of the backup chain srchost --> bckphost --> archost, and alerts if it stays broken.

Purpose: the single thing you schedule. One table maps a job name to the jobconfig, host filter and `bzfs_jobrunner`
actions of that job; the same name selects when a persistent failure becomes a notification. Assumptions: invoked as
`bzfs_backup.py <job>` by a systemd timer or cron, once per job cycle; `bzfs` and `bzfs_jobrunner` are on the PATH.
Design rationale: run and alert live in one file because both are keyed by the job name - split across two files, a
job added to one table and forgotten in the other fails silently, which is the one outcome a backup must not have.
"""

from __future__ import (
    annotations,
)
import os
import shlex
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
    NamedTuple,
)

SCRIPT_DIR: Final = os.path.dirname(os.path.abspath(__file__))
TIER1: Final = os.path.join(SCRIPT_DIR, "bzfs_job_src_to_bckp.py")  # srchost --> bckphost
TIER2: Final = os.path.join(SCRIPT_DIR, "bzfs_job_bckp_to_arc.py")  # bckphost --> archost
NOTIFY_COMMAND: Final = os.path.join(SCRIPT_DIR, "bzfs_notify.sh")  # EDIT ME (optional): the notification transport
STATE_DIR: Final = os.path.join(os.path.expanduser("~"), ".bzfs-alert")  # one small state file per job
STILL_RUNNING_STATUS: Final = 4  # `bzfs` exit code meaning "the same previous periodic job has not completed yet"
MAX_OUTPUT_LINES: Final = 40  # how much of the failed job's output to quote in the notification


class Job(NamedTuple):
    """One scheduled job: what to run, plus how long it may stay broken before the operator is told."""

    config: str  # which tier's jobconfig
    host_filter: str  # makes bzfs_jobrunner refuse to run if the job is scheduled on the wrong machine
    actions: tuple[str, ...]  # bzfs_jobrunner runs these in the given order, so replication precedes pruning
    grace_secs: int  # how long the job must fail continuously before the first alert
    min_failures: int  # how many consecutive failures are required regardless of elapsed time
    renotify_secs: int  # how long to stay quiet before repeating an alert for an ongoing outage


# The grace periods encode how much downtime is normal for whatever the job observes: `srchost` retries every 15
# minutes while it is awake, whereas anything involving `archost` must tolerate a machine that is offline most of the
# day and is only observable during its own online window.
# Never schedule `--prune-src-snapshots` for tier 2: the snapshots on bckphost belong to tier 1, which prunes them via
# the `bckp-prune` job.
JOBS: Final = {  # EDIT ME (optional)
    # on srchost, every 15 min: snapshot, push to bckphost, then prune srchost
    "src-to-bckp": Job(
        TIER1,
        "--src-host",
        ("--create-src-snapshots", "--replicate", "--prune-src-snapshots", "--prune-src-bookmarks"),
        grace_secs=2 * 3600,
        min_failures=3,
        renotify_secs=12 * 3600,
    ),
    # on bckphost, hourly: enforce the sliding window of tier 1
    "bckp-prune": Job(
        TIER1,
        "--dst-host",
        ("--prune-dst-snapshots",),
        grace_secs=6 * 3600,
        min_failures=3,
        renotify_secs=24 * 3600,
    ),
    # on bckphost, hourly: has srchost stopped delivering altogether? Coarse on purpose - an ordinary holiday is not an
    # outage, so this is the "srchost never came back" net, not the "the push is failing right now" detector.
    "bckp-freshness": Job(
        TIER1,
        "--dst-host",
        ("--monitor-dst-snapshots",),
        grace_secs=24 * 3600,
        min_failures=2,
        renotify_secs=7 * 86400,
    ),
    # on bckphost, daily: bound the growth of the bookmarks that keep archost resumable
    "bckp-bookmarks": Job(
        TIER2,
        "--src-host",
        ("--prune-src-bookmarks",),
        grace_secs=24 * 3600,
        min_failures=3,
        renotify_secs=7 * 86400,
    ),
    # on bckphost, hourly: is archost still fetching, and is it still reachable at all? This job can only observe
    # archost while archost happens to be online, so most runs legitimately fail with "unreachable". The grace must
    # therefore span several of archost's daily online windows, otherwise a couple of unlucky misses - checks that all
    # landed outside a short window - would alert while archost is in fact perfectly healthy.
    "arc-freshness": Job(
        TIER2,
        "--src-host",
        ("--monitor-dst-snapshots",),
        grace_secs=72 * 3600,
        min_failures=3,
        renotify_secs=48 * 3600,
    ),
    # on archost, every 15 min while it is up: pull from bckphost, then prune the archive
    "bckp-to-arc": Job(
        TIER2,
        "--dst-host",
        ("--replicate", "--prune-dst-snapshots"),
        grace_secs=36 * 3600,
        min_failures=3,
        renotify_secs=24 * 3600,
    ),
}


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
    with open(path + ".tmp", "w", encoding="utf-8") as fd:
        fd.writelines(f"{key}={state[key]}\n" for key in sorted(state))
        fd.flush()
        os.fsync(fd.fileno())
    os.replace(path + ".tmp", path)


def notify(subject: str, body: str) -> None:
    """Hands a notification to the external transport; never lets a broken transport mask the job's own outcome."""
    try:
        subprocess.run([NOTIFY_COMMAND, subject], input=body, text=True, check=True)
    except (OSError, subprocess.SubprocessError) as e:
        print(f"WARNING: notification command {NOTIFY_COMMAND} failed: {e}", file=sys.stderr)


def humanize(seconds: int) -> str:
    """Returns a compact human readable duration such as '3.2h' or '1.5d'."""
    for limit, divisor, unit in ((60, 1, "s"), (3600, 60, "m"), (86400, 3600, "h")):
        if seconds < limit:
            return f"{seconds}s" if divisor == 1 else f"{seconds / divisor:.1f}{unit}"
    return f"{seconds / 86400:.1f}d"


def main() -> None:
    """Runs the job named by the first argument and updates its alert state according to the outcome."""
    name = sys.argv[1] if len(sys.argv) > 1 else ""
    if name not in JOBS:
        sys.exit(f"Usage: {sys.argv[0]} <job>; known jobs: {' '.join(JOBS)}")
    job: Final[Job] = JOBS[name]
    hostname: Final[str] = socket.gethostname()
    cmd: list[str] = [job.config, f"{job.host_filter}={hostname}", *job.actions]
    if os.getenv("DRYRUN", "1") == "1":  # keep 1 for safety; set DRYRUN=0 only once the plans look right
        cmd.append("--dryrun")  # `bzfs` treats both source and destination as read-only
    print("Review command before run:\n  " + shlex.join(cmd))

    result = subprocess.run(cmd, stdin=DEVNULL, stdout=PIPE, stderr=STDOUT, text=True, check=False)
    sys.stdout.write(result.stdout)
    sys.stdout.flush()
    if result.returncode == STILL_RUNNING_STATUS:  # a longer run of the same job is still in flight; not an outage
        print(f"Skipped {name}: the previous run has not completed yet.")
        return

    now: Final[int] = int(time.time())
    state_path: Final[str] = os.path.join(STATE_DIR, name + ".state")
    state = read_state(state_path)
    header: Final[str] = f"Job     : {name}\nHost    : {hostname}\nCommand : {shlex.join(cmd)}\n"

    if result.returncode == 0:
        if state.get("alerting", 0):
            outage = now - state.get("first_failure_epoch", now)
            notify(
                f"[bzfs] RECOVERED: {name} on {hostname}",
                f"{header}Outage  : {humanize(outage)}\n\nThe job succeeded again; no action required.\n",
            )
        write_state(state_path, {"consecutive_failures": 0, "last_success_epoch": now})
        sys.exit(0)

    failures = state.get("consecutive_failures", 0) + 1
    # The grace period runs from the first failure of the current streak, not from the last success: srchost is asleep
    # most of the time, and measuring from the last success would alert on its first attempt after every long absence.
    first_failure = state.get("first_failure_epoch") or now
    last_alert = state.get("last_alert_epoch", 0)
    alerting = state.get("alerting", 0)
    failing_secs = now - first_failure
    if (
        failures >= job.min_failures
        and failing_secs >= job.grace_secs
        and (last_alert == 0 or now - last_alert >= job.renotify_secs)
    ):
        last_success = state.get("last_success_epoch", 0)
        since = humanize(now - last_success) + " ago" if last_success else "never (no success recorded)"
        notify(
            f"[bzfs] FAILING: {name} on {hostname}",
            f"{header}Exit code    : {result.returncode}\n"
            f"Failing for  : {humanize(failing_secs)} ({failures} consecutive failures)\n"
            f"Last success : {since}\n\nLast {MAX_OUTPUT_LINES} lines of output:\n"
            + "\n".join(result.stdout.splitlines()[-MAX_OUTPUT_LINES:])
            + "\n",
        )
        last_alert, alerting = now, 1
    write_state(
        state_path,
        {
            "alerting": alerting,
            "consecutive_failures": failures,
            "first_failure_epoch": first_failure,
            "last_alert_epoch": last_alert,
            "last_success_epoch": state.get("last_success_epoch", 0),
        },
    )
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
