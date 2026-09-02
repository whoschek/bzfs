#!/usr/bin/env bash
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
# Single entry point for every scheduled job of the three-tier backup chain srchost --> bckphost --> archost.
# Maps a job name to the jobconfig, the host filter and the `bzfs_jobrunner` actions of that job, and runs it under
# the alerting watchdog. Keep `bzfs` and `bzfs_jobrunner` on the PATH, e.g. export PATH=$PATH:/bzfs/bzfs_main
# Bash equivalent of bzfs_backup_run.py; deploy one of the two, not both.
# Usage: DRYRUN=0 bzfs_backup_run.sh <job>

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TIER1="$SCRIPT_DIR/bzfs_job_src_to_bckp.py"
TIER2="$SCRIPT_DIR/bzfs_job_bckp_to_arc.py"
DRYRUN="${DRYRUN:-1}"  # keep 1 for safety; set DRYRUN=0 only once the plans below look right
JOB="${1:-}"
hostname_value="$(hostname)"

# The `--src-host` / `--dst-host` filters are not merely cosmetic: they make `bzfs_jobrunner` refuse to run if a job is
# scheduled on the wrong machine, instead of quietly pruning or replicating the wrong side of the chain.
# Never schedule `--prune-src-snapshots` for tier 2: the snapshots on bckphost belong to tier 1, which prunes them via
# the `bckp-prune` job below.
case "$JOB" in
    src-to-bckp)  # on srchost, every 15 minutes: snapshot, push to bckphost, then prune srchost
        cmd=("$TIER1" "--src-host=$hostname_value"
            --create-src-snapshots --replicate --prune-src-snapshots --prune-src-bookmarks)
        ;;
    bckp-prune)  # on bckphost, hourly: enforce the sliding window of tier 1
        cmd=("$TIER1" "--dst-host=$hostname_value" --prune-dst-snapshots)
        ;;
    bckp-freshness)  # on bckphost, hourly: has srchost stopped delivering altogether?
        cmd=("$TIER1" "--dst-host=$hostname_value" --monitor-dst-snapshots)
        ;;
    bckp-bookmarks)  # on bckphost, daily: bound the growth of the bookmarks that keep archost resumable
        cmd=("$TIER2" "--src-host=$hostname_value" --prune-src-bookmarks)
        ;;
    arc-freshness)  # on bckphost, hourly: is archost still fetching, and is it still reachable at all?
        cmd=("$TIER2" "--src-host=$hostname_value" --monitor-dst-snapshots)
        ;;
    bckp-to-arc)  # on archost, every 15 minutes while it is up: pull from bckphost, then prune the archive
        cmd=("$TIER2" "--dst-host=$hostname_value" --replicate --prune-dst-snapshots)
        ;;
    *)
        echo "Usage: $0 <job>; jobs on srchost: src-to-bckp;" \
            "jobs on bckphost: bckp-prune bckp-freshness bckp-bookmarks arc-freshness;" \
            "jobs on archost: bckp-to-arc" >&2
        exit 2
        ;;
esac

if [[ "$DRYRUN" == "1" ]]; then
    cmd+=(--dryrun)  # `bzfs` treats both source and destination as read-only
fi

printf 'Review command before run:\n '
printf ' %q' "${cmd[@]}"
printf '\n'
exec "$SCRIPT_DIR/bzfs_alert.sh" "$JOB" "${cmd[@]}"
