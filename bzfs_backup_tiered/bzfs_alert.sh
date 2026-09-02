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
# Runs one periodic backup job and notifies the operator once the job stays broken for longer than a grace period.
# A single failed run of a replication job is normal (a host was rebooting, a network hiccup, a laptop lid was closed
# mid-transfer); a job that keeps failing is not. The grace period is measured from the first failure of the current
# streak, not from the last success, because `srchost` is asleep most of the time and measuring from the last success
# would alert on its very first attempt after every longer absence.
# Bash equivalent of bzfs_alert.py, using the very same state file format; deploy one of the two, not both.
# Usage: bzfs_alert.sh <job> <command> [args...]

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
NOTIFY_COMMAND="$SCRIPT_DIR/bzfs_notify.sh"  # EDIT ME (optional): the notification transport
STATE_DIR="$HOME/.bzfs-alert"                # one small state file per job
STILL_RUNNING_STATUS=4                       # `bzfs` exit code: "the same previous periodic job hasn't completed yet"
MAX_OUTPUT_LINES=40                          # how much of the failed job's output to quote in the notification

# For each job: how long the job must fail continuously before the first alert, how many consecutive failures are
# required regardless of elapsed time, and how long to stay quiet before repeating an alert for an ongoing outage.
# The values encode how much downtime is normal for the host that runs the job: `srchost` retries every 15 minutes
# while it is awake, whereas anything involving `archost` must tolerate a machine that is offline most of the day.
# EDIT ME (optional):
job_thresholds() {  # prints grace_secs, min_failures and renotify_secs for the given job name
    case "$1" in
        src-to-bckp) echo "7200 3 43200" ;;       # srchost fails to push to bckphost
        bckp-prune) echo "21600 3 86400" ;;       # bckphost fails to prune its sliding window
        bckp-freshness) echo "86400 2 604800" ;;  # bckphost holds nothing recent from srchost
        bckp-bookmarks) echo "86400 3 604800" ;;  # bckphost fails to prune its bookmarks
        arc-freshness) echo "129600 3 86400" ;;   # archost holds nothing recent, or is unreachable from bckphost
        bckp-to-arc) echo "129600 3 86400" ;;     # archost fails to pull from bckphost
        *) return 1 ;;
    esac
}

read_state() {  # prints the value of state key $2 of job $1, or 0 if absent
    local value
    value="$(sed -n "s/^$2=//p" "$STATE_DIR/$1.state" 2> /dev/null || true)"
    [[ "$value" =~ ^-?[0-9]+$ ]] && echo "$value" || echo 0
}

write_state() {  # replaces the state file of job $1 with the key=value lines on stdin, atomically
    mkdir -p "$STATE_DIR"
    chmod 700 "$STATE_DIR"
    cat > "$STATE_DIR/$1.state.tmp"
    mv -f "$STATE_DIR/$1.state.tmp" "$STATE_DIR/$1.state"
}

notify() {  # hands subject $1 plus the body on stdin to the transport; a broken transport must not mask the outcome
    "$NOTIFY_COMMAND" "$1" || echo "WARNING: notification command $NOTIFY_COMMAND failed" >&2
}

humanize() {  # prints a compact human readable duration for $1 seconds
    local secs="$1"
    if [[ "$secs" -lt 60 ]]; then
        echo "${secs}s"
    elif [[ "$secs" -lt 3600 ]]; then
        awk -v s="$secs" 'BEGIN { printf "%.1fm", s / 60 }'
    elif [[ "$secs" -lt 86400 ]]; then
        awk -v s="$secs" 'BEGIN { printf "%.1fh", s / 3600 }'
    else
        awk -v s="$secs" 'BEGIN { printf "%.1fd", s / 86400 }'
    fi
}

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <job> <command> [args...]" >&2
    exit 2
fi
job="$1"
shift
if ! thresholds="$(job_thresholds "$job")"; then
    echo "Unknown job: $job" >&2
    exit 2
fi
read -r grace_secs min_failures renotify_secs <<< "$thresholds"
hostname_value="$(hostname)"

output_file="$(mktemp)"
trap 'rm -f "$output_file"' EXIT
returncode=0
"$@" > "$output_file" 2>&1 || returncode=$?
cat "$output_file"

if [[ "$returncode" -eq "$STILL_RUNNING_STATUS" ]]; then  # a longer run of the same job is still in flight
    echo "Skipped $job: the previous run has not completed yet."
    exit 0
fi

now="$(date +%s)"
last_success_epoch="$(read_state "$job" last_success_epoch)"

if [[ "$returncode" -eq 0 ]]; then
    if [[ "$(read_state "$job" alerting)" -ne 0 ]]; then
        first_failure_epoch="$(read_state "$job" first_failure_epoch)"
        notify "[bzfs] RECOVERED: $job on $hostname_value" << EOF
Job     : $job
Host    : $hostname_value
Command : $*
Outage  : $(humanize $((now - first_failure_epoch)))

The job succeeded again; no action required.
EOF
    fi
    write_state "$job" << EOF
consecutive_failures=0
last_success_epoch=$now
EOF
else
    failures=$(($(read_state "$job" consecutive_failures) + 1))
    first_failure_epoch="$(read_state "$job" first_failure_epoch)"
    if [[ "$first_failure_epoch" -eq 0 ]]; then  # this failure starts a new streak of consecutive failures
        first_failure_epoch="$now"
    fi
    last_alert_epoch="$(read_state "$job" last_alert_epoch)"
    alerting="$(read_state "$job" alerting)"
    failing_secs=$((now - first_failure_epoch))
    if [[ "$failures" -ge "$min_failures" ]] && [[ "$failing_secs" -ge "$grace_secs" ]] &&
        { [[ "$last_alert_epoch" -eq 0 ]] || [[ $((now - last_alert_epoch)) -ge "$renotify_secs" ]]; }; then
        if [[ "$last_success_epoch" -eq 0 ]]; then
            last_success_text="never (no success recorded)"
        else
            last_success_text="$(humanize $((now - last_success_epoch))) ago"
        fi
        notify "[bzfs] FAILING: $job on $hostname_value" << EOF
Job          : $job
Host         : $hostname_value
Command      : $*
Exit code    : $returncode
Failing for  : $(humanize "$failing_secs") ($failures consecutive failures)
Last success : $last_success_text

Last $MAX_OUTPUT_LINES lines of output:
$(tail -n "$MAX_OUTPUT_LINES" "$output_file")
EOF
        last_alert_epoch="$now"
        alerting=1
    fi
    write_state "$job" << EOF
alerting=$alerting
consecutive_failures=$failures
first_failure_epoch=$first_failure_epoch
last_alert_epoch=$last_alert_epoch
last_success_epoch=$last_success_epoch
EOF
fi
exit "$returncode"
