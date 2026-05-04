#!/usr/bin/env bash

set -e

run_with_retries() {
    retries=3
    retry_sleep_secs=5
    attempt=1
    while true; do
        if ${1+"$@"}; then
            break
        fi
        if ((attempt > retries)); then
            echo "error: ${1+"$@"} failed after ${attempt} attempts" >&2
            exit 1
        fi
        echo "${1+"$@"} failed; retrying ${attempt}/${retries} in ${retry_sleep_secs}s ..." >&2
        sleep "${retry_sleep_secs}"
        retry_sleep_secs="$((retry_sleep_secs * 2))"  # exponential backoff
        attempt="$((attempt + 1))"
    done
}

run_with_retries sudo add-apt-repository -y --no-update ${1+"$@"}
script_dir="$(dirname "$(realpath "$0")")"
"${script_dir}/apt-get-update-with-retries.sh"
