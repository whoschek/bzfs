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
# shellcheck disable=SC2154

# Copies the local repo to the given remote user@host:path (via rsync) and runs tests there (via ssh).
# Expects params to be provided via bzfs_test_* env vars.
set -e
cd "$(dirname "$(realpath "$0")")"
echo "Running tests on $bzfs_test_remote_userhost:$bzfs_test_remote_path ..."

if [ "$bzfs_test_ssh_port" = "" ]; then
    bzfs_test_ssh_port="22"
fi
case "$bzfs_test_remote_userhost" in ""|-*|*[![:alnum:]_.@-]* ) \
    echo "error: invalid bzfs_test_remote_userhost: $bzfs_test_remote_userhost" >&2; exit 1;; esac
case "$bzfs_test_remote_path" in ""|-*|*[![:alnum:]/_.-]* ) \
    echo "error: invalid bzfs_test_remote_path: $bzfs_test_remote_path" >&2; exit 1;; esac
case "$bzfs_test_ssh_port" in ""|*[![:digit:]]* ) \
    echo "error: invalid bzfs_test_ssh_port: $bzfs_test_ssh_port" >&2; exit 1;; esac
case "$bzfs_test_remote_private_key" in ""|-*|*[![:alnum:]/_.-]* ) \
    echo "error: invalid bzfs_test_remote_private_key: $bzfs_test_remote_private_key" >&2; exit 1;; esac

flags="-oServerAliveInterval=0 -x -T"
rsync -a --delete --exclude=venv --compress-choice=zstd --compress-level=1 -e \
    "ssh -i $bzfs_test_remote_private_key -p $bzfs_test_ssh_port $flags" ./ \
    "$bzfs_test_remote_userhost:$bzfs_test_remote_path"
# shellcheck disable=SC2086
ssh -i "$bzfs_test_remote_private_key" -p "$bzfs_test_ssh_port" $flags "$bzfs_test_remote_userhost" \
    "bzfs_test_mode=$bzfs_test_mode" "bzfs_test_ssh_port=$bzfs_test_ssh_port" "$bzfs_test_remote_path/test.sh"
