#!/usr/bin/env bash

# When bumping an existing dependency version in `pyproject.toml`, update the requirements lock files by running this
# script. This also pins transitive dependencies. It also respects the cooldown period defined in preinstall_dev.sh.
#
# If you want to re-resolve all versions without preserving existing pins, run this first: `rm requirements*.txt`

# shellcheck source=/dev/null
set -e
cd "$(dirname "$(realpath "$0")")"
python3 -m venv .venv
. .venv/bin/activate
. ./preinstall_dev.sh
./tools/install_uv.sh
uv pip compile --extra=dev --generate-hashes --universal --python-version=3.9 --no-build --no-cache -o requirements-dev.txt pyproject.toml
echo
echo "========================================================================"
echo "git diff for lock file changes:"
git --no-pager diff requirements*.txt
