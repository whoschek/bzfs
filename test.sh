#!/usr/bin/env sh
set -e
# Use tmpfs (RAM disk) as fast backing storage where available
if [ -d "/run/user/$(id -u)" ] && [ -w "/run/user/$(id -u)" ]; then
  export TMPDIR="/run/user/$(id -u)/bzfs" # for Github Action on Linux, etc
  mkdir -p "$TMPDIR"
fi
echo "TMPDIR: $TMPDIR"

cd $(realpath $(dirname "$0"))
python3 -m test.test_bzfs
