#!/usr/bin/env sh
set -e
# Use tmpfs (RAM disk) as fast backing storage where available
if [ -d "/run/user/$(id -u)" ] && [ -w "/run/user/$(id -u)" ]; then
  export TMPDIR="/run/user/$(id -u)/bzfs" # for Github Action on Linux, etc
  mkdir -p "$TMPDIR"
fi
echo "TMPDIR: $TMPDIR"

cd $(realpath $(dirname "$0"))
if [ "$(uname -s)" = "FreeBSD" ]; then
  echo "Running on FreeBSD"
elif [ "$(uname -s)" = "SunOS" ]; then
  echo "Running on SunOS"
else
  python3 -m pip install --upgrade pip
  python3 -m pip install --upgrade "coverage==7.*"
fi

# see https://coverage.readthedocs.io/
PYTHONPATH=. python3 -m coverage run --branch --omit='tests/*.py,*/__init__.py' -m tests.test_bzfs
python3 -m coverage report | tee coverage_report.txt
python3 -m coverage html

PYTHONPATH=. .github-workflow-scripts/generate_badges.py generate
