#!/usr/bin/env bash

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

# Runs the test suite while collecting coverage metrics.
# Prerequisites: See AGENTS.md section "How to Set up the Environment"

set -e
# Use tmpfs (RAM disk) as fast backing storage where available
if [ -d "/run/user/$(id -u)" ] && [ -w "/run/user/$(id -u)" ]; then
    TMPDIR="/run/user/$(id -u)/bzfs" # for Github Action on Linux, etc
    export TMPDIR
    mkdir -p "$TMPDIR"
fi
echo "Now using TMPDIR: $TMPDIR"
echo "Now using bzfs_test_mode: $bzfs_test_mode"

cd "$(dirname "$(realpath "$0")")"
export PYTHON_LAZY_IMPORTS=normal  # PEP 810

# see https://coverage.readthedocs.io/
PYTHONPATH=. python3 -m coverage run -m bzfs_tests.test_all
python3 -m coverage report | tee coverage_report.txt
python3 -m coverage html
python3 -m coverage xml

if [ "$(command -v zfs 2> /dev/null)" != "" ]; then
    PYTHONPATH=. .github-workflow-scripts/generate_badges.py generate
fi
