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

# Executes the given command and only emits its output text if exit code is non-zero.
# Preserves full visibility on failures and preserves exit code semantics.
# This is to prevent polluting the context window with a large amount of token noise on successful verification commands.

# shellcheck disable=SC2154
set -e
subject="$1"
shift
tmpfile=$(mktemp -t bzfs_test_run_quietly."$(date '+%Y-%m-%d_%H-%M-%S')".XXXXXX)
${1+"$@"} 2>&1 | tee "$tmpfile" | while IFS= read -r currline; do
    if [ "$bzfs_test_no_run_quietly" != "" ]; then
        echo "$currline"
    else
        printf '.'  # print progress bar
    fi
done
exitcode=${PIPESTATUS[0]}

echo
if [ "$exitcode" = "0" ]; then
    echo "✓ $subject"  # PASSED
else
    echo "✗ $subject"  # FAILED
    cat "$tmpfile"
fi
rm -f "$tmpfile"
exit "$exitcode"
