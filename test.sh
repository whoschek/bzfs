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

set -e
# Use tmpfs (RAM disk) as fast backing storage where available
if [ -d "/run/user/$(id -u)" ] && [ -w "/run/user/$(id -u)" ]; then
  export TMPDIR="/run/user/$(id -u)/bzfs" # for Github Action on Linux, etc
  mkdir -p "$TMPDIR"
fi
echo "TMPDIR: $TMPDIR"

cd $(dirname $(realpath "$0"))
python3 -m bzfs_tests.test_all
