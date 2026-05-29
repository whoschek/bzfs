#!/usr/bin/env bash

# When bumping an existing dependency version in `pyproject.toml`, regenerate the requirements lock files by running this
# script. This also pins transitive dependencies. It also respects the cooldown period defined in preinstall_dev.sh.

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
git diff requirements*.txt
