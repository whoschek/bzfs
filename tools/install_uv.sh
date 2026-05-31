#!/usr/bin/env bash

# Copyright 2026 Wolfgang Hoschek AT mac DOT com
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

# Installs the pinned uv release specified in pyproject.toml.
# By default installs into the .venv via UV_INSTALL_DIR.
# See https://docs.astral.sh/uv/getting-started/installation/
# Verifies the checksum of the downloaded installer script. Note that the downloaded installer script contains hard-coded
# checksums and verifies the checksum of the uv binary it downloads against those checksums.

set -eo pipefail

script_dir="$(dirname "$(realpath "$0")")"
repo_root="$(dirname "$script_dir")"
uv_version="$(sed -n 's/^[[:blank:]]*requires[[:blank:]]*=[[:blank:]]*\["uv_build==\([0-9][0-9.]*\)".*$/\1/p' "$repo_root/pyproject.toml")"
if [[ ! "$uv_version" =~ ^[0-9]+[.][0-9]+[.][0-9]+$ ]]; then
    echo "ERROR: expected exactly one uv_build==X.Y.Z pin in pyproject.toml." >&2
    exit 1
fi
uv_install_url="https://astral.sh/uv/${uv_version}/install.sh"
expected_sha256="$(sed -n 's/^[[:blank:]]*uv-install-script-sha256[[:blank:]]*=[[:blank:]]*"\([0-9a-f][0-9a-f]*\)".*$/\1/p' "$repo_root/pyproject.toml")"
if [[ ! "$expected_sha256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "ERROR: expected exactly one uv-install-script-sha256 hash in pyproject.toml." >&2
    exit 1
fi
UV_INSTALL_DIR="${UV_INSTALL_DIR:-$repo_root/.venv/bin}"
export UV_INSTALL_DIR
export UV_NO_MODIFY_PATH="${UV_NO_MODIFY_PATH:-1}"

sha256_file() {
    local path="$1"
    if command -v sha256sum > /dev/null 2>&1; then
        sha256sum "$path" | sed 's/[[:blank:]].*//'
    elif command -v shasum > /dev/null 2>&1; then
        shasum -a 256 "$path" | sed 's/[[:blank:]].*//'
    elif command -v sha256 > /dev/null 2>&1; then
        sha256 -q "$path"
    else
        echo "ERROR: neither sha256sum nor shasum nor sha256 is available for uv installer verification." >&2
        return 1
    fi
}

uv_install_script="$(mktemp)"
trap 'rm -f "$uv_install_script"' EXIT
curl -fsSL --retry 999 --retry-delay 5 --retry-max-time 60 --retry-all-errors --output "$uv_install_script" "$uv_install_url"
actual_sha256="$(sha256_file "$uv_install_script")"
if [[ "$actual_sha256" != "$expected_sha256" ]]; then
    echo "ERROR: sha256 mismatch for uv ${uv_install_script}: ${actual_sha256} != ${expected_sha256}" >&2
    exit 1
fi
sh "$uv_install_script"
