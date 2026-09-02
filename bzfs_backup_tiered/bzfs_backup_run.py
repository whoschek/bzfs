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
"""Single entry point for every scheduled job of the three-tier backup chain srchost --> bckphost --> archost.

Purpose: maps a job name to the jobconfig, the host filter and the `bzfs_jobrunner` actions of that job, and runs it
under the alerting watchdog. Assumptions: `bzfs` and `bzfs_jobrunner` are on the PATH; dry-run stays enabled until an
operator explicitly sets DRYRUN=0. Design rationale: Python equivalent of bzfs_backup_run.sh - deploy one of the two,
not both, so that a job name means exactly one thing on a given host.
"""

from __future__ import (
    annotations,
)
import os
import shlex
import socket
import subprocess
import sys
from typing import (
    Final,
)

SCRIPT_DIR: Final = os.path.dirname(os.path.abspath(__file__))
TIER1: Final = os.path.join(SCRIPT_DIR, "bzfs_job_src_to_bckp.py")
TIER2: Final = os.path.join(SCRIPT_DIR, "bzfs_job_bckp_to_arc.py")
ALERT_COMMAND: Final = os.path.join(SCRIPT_DIR, "bzfs_alert.py")


def job_command(job: str, hostname: str) -> list[str] | None:
    """Returns the jobconfig invocation of the given job, or None if the job name is unknown.

    The `--src-host` / `--dst-host` filters are not merely cosmetic: they make `bzfs_jobrunner` refuse to run if this
    job is scheduled on the wrong machine, instead of quietly pruning or replicating the wrong side of the chain.
    Never schedule `--prune-src-snapshots` for tier 2: the snapshots on bckphost belong to tier 1, which prunes them
    via the `bckp-prune` job.
    """
    if job == "src-to-bckp":  # on srchost, every 15 minutes: snapshot, push to bckphost, then prune srchost
        return [
            TIER1,
            f"--src-host={hostname}",
            "--create-src-snapshots",
            "--replicate",
            "--prune-src-snapshots",
            "--prune-src-bookmarks",
        ]
    if job == "bckp-prune":  # on bckphost, hourly: enforce the sliding window of tier 1
        return [TIER1, f"--dst-host={hostname}", "--prune-dst-snapshots"]
    if job == "bckp-freshness":  # on bckphost, hourly: has srchost stopped delivering altogether?
        return [TIER1, f"--dst-host={hostname}", "--monitor-dst-snapshots"]
    if job == "bckp-bookmarks":  # on bckphost, daily: bound the growth of the bookmarks that keep archost resumable
        return [TIER2, f"--src-host={hostname}", "--prune-src-bookmarks"]
    if job == "arc-freshness":  # on bckphost, hourly: is archost still fetching, and is it still reachable at all?
        return [TIER2, f"--src-host={hostname}", "--monitor-dst-snapshots"]
    if job == "bckp-to-arc":  # on archost, every 15 minutes while it is up: pull from bckphost, then prune the archive
        return [TIER2, f"--dst-host={hostname}", "--replicate", "--prune-dst-snapshots"]
    return None


def main() -> None:
    """Runs the job named by the first argument, under the alerting watchdog."""
    job = sys.argv[1] if len(sys.argv) > 1 else ""
    cmd = job_command(job, socket.gethostname())
    if cmd is None:
        sys.exit(
            f"Usage: {sys.argv[0]} <job>; jobs on srchost: src-to-bckp; "
            "jobs on bckphost: bckp-prune bckp-freshness bckp-bookmarks arc-freshness; jobs on archost: bckp-to-arc"
        )
    if os.getenv("DRYRUN", "1") == "1":  # keep 1 for safety; set DRYRUN=0 only once the plans look right
        cmd += ["--dryrun"]  # `bzfs` treats both source and destination as read-only

    print("Review command before run:")
    print("  " + shlex.join(cmd))
    sys.exit(subprocess.run([ALERT_COMMAND, job] + cmd, stdin=subprocess.DEVNULL, check=False).returncode)


if __name__ == "__main__":
    main()
