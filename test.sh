#!/usr/bin/env sh
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

set -e
cd "$(dirname "$(realpath "$0")")"
if [ "$bzfs_test_remote_userhost" = "" ]; then
  # Use tmpfs (RAM disk) as fast backing storage where available
  if [ -d "/run/user/$(id -u)" ] && [ -w "/run/user/$(id -u)" ]; then
    TMPDIR="/run/user/$(id -u)/bzfs" # for Github Action on Linux, etc
    export TMPDIR
    mkdir -p "$TMPDIR"
  fi
  echo "Now using TMPDIR: $TMPDIR"
  echo "Now using bzfs_test_mode: $bzfs_test_mode"
  PYTHON_LAZY_IMPORTS=all  # PEP 810
  export PYTHON_LAZY_IMPORTS
  python3 -m bzfs_tests.test_all
else
  ./test_host.sh  # rsync's the local repo to remote host and runs tests there
fi
