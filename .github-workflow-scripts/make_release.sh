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

# Runs build + package + publish steps when cutting a release.
# This script is called by .github/workflows/python-release.yml when a maintainer triggers a release on GitHub.

# shellcheck source=/dev/null
set -e
script_dir="$(dirname "$(realpath "$0")")"
repo_root="$(dirname "$script_dir")"
cd "$repo_root"

echo
echo "========================================================================"
echo "Now building package..."
python3 -m venv .venv
. .venv/bin/activate
./tools/install_uv.sh
uv --version
uv build --python "$(command -v python3)" --clear --offline --no-cache --no-python-downloads --no-sources
for archive in dist/*.tar.gz; do
    echo
    echo "$archive"
    tar -tzvf "$archive"  # print list of generated files for diagnostics
done
for archive in dist/*.whl; do
    echo
    echo "$archive"
    python3 -m zipfile --list "$archive"  # print list of generated files for diagnostics
done

if [[ "${MAKE_RELEASE_PUBLISH:-false}" == "true" ]]; then
    echo
    echo "========================================================================"
    echo "Now publishing package to PyPi..."
    extra_args=()
    if [[ "${MAKE_RELEASE_TESTPYPI:-false}" == "true" ]]; then
        extra_args+=(--publish-url=https://test.pypi.org/legacy/)
    fi
    uv publish "${extra_args[@]}" --trusted-publishing=always --verbose --no-cache
fi

echo
echo "========================================================================"
echo "Success."
