#!/usr/bin/env sh
set -e
# Use tmpfs (RAM disk) as fast backing storage where available
if [ -d "/run/user/$UID" ] && [ -w "/run/user/$UID" ]; then
  TMPDIR="/run/user/$UID/wbackup-zfs" # for Github Action on Linux, etc
  export TMPDIR
  mkdir -p "$TMPDIR"
fi
echo "TMPDIR: $TMPDIR"

cd $(realpath $(dirname "$0"))
if [ "$(uname -s)" != "FreeBSD" ]; then
  python3 -m pip install --upgrade pip
  python3 -m pip install coverage
fi
PYTHONPATH=. coverage run --branch -m test.test_wbackup_zfs
coverage report | tee coverage_report.txt
coverage html
