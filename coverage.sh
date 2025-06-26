#!/usr/bin/env sh
set -e
# Use tmpfs (RAM disk) as fast backing storage where available
if [ -d "/run/user/$(id -u)" ] && [ -w "/run/user/$(id -u)" ]; then
  export TMPDIR="/run/user/$(id -u)/bzfs" # for Github Action on Linux, etc
  mkdir -p "$TMPDIR"
fi
echo "TMPDIR: $TMPDIR"

cd $(dirname $(realpath "$0"))
if [ "$(uname -s)" = "FreeBSD" ]; then
  echo "Running on FreeBSD"
elif [ "$(uname -s)" = "SunOS" ]; then
  echo "Running on SunOS"
  python3 -m ensurepip --upgrade
  python3 -m pip install 'coverage[toml]'
elif [ "$(which coverage 2> /dev/null)" = "" ]; then
  python3 -m pip install --upgrade pip
  python3 -m pip install --upgrade "coverage[toml]>=7.6"
fi

# see https://coverage.readthedocs.io/
PYTHONPATH=. python3 -m coverage run -m bzfs_tests.test_all
python3 -m coverage report | tee coverage_report.txt
python3 -m coverage html
python3 -m coverage xml

if [ "$(which zfs 2> /dev/null)" != "" ]; then
  PYTHONPATH=. .github-workflow-scripts/generate_badges.py generate
fi
