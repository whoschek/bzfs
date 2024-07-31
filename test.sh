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
python3 -m test.test_wbackup_zfs
