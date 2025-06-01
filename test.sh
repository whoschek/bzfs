#!/usr/bin/env sh
set -e
# Use tmpfs (RAM disk) as fast backing storage where available
if [ -d "/run/user/$(id -u)" ] && [ -w "/run/user/$(id -u)" ]; then
  export TMPDIR="/run/user/$(id -u)/bzfs" # for Github Action on Linux, etc
  mkdir -p "$TMPDIR"
fi
echo "TMPDIR: $TMPDIR"

cd $(realpath $(dirname "$0"))

FAILFAST_OPT=""
# If CI env var is not set (local run), enable failfast
if [ -z "$CI" ]; then
  echo "Running in failfast mode (CI environment variable is not set)."
  FAILFAST_OPT="-f"
else
  echo "Running without failfast mode (CI environment variable is set to '$CI')."
fi

# -v makes unittest verbose (equivalent to verbosity=2 for TextTestRunner from command line)
echo "Running tests with verbosity..."
python3 -m unittest discover -s bzfs_tests -p "test_*.py" $FAILFAST_OPT -v
