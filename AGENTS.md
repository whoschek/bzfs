# AGENT instructions

- Run `pre-commit run --all-files` before each commit.
- Run `bzfs_test_mode=unit ./test.sh` after code changes.
- Integration tests rely on ZFS and should not be run in the sandbox.
