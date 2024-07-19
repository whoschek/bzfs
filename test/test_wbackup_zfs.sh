#!/usr/bin/env bash
# Use tmpfs (RAM disk) as fast backing storage where available
if [ -d "/run/user/$UID" ] && [ -w "/run/user/$UID" ]; then
  TMPDIR="/run/user/$UID/wbackup-zfs" # for Github Action, etc
  export TMPDIR
  mkdir -p "$TMPDIR"
fi
echo "TMPDIR: $TMPDIR"

MYDIR=$(realpath "$(dirname "$0")")
"$MYDIR/test_wbackup_zfs.py" ${1+"$@"}
