# AGENT instructions

- Run `pre-commit run --all-files` before each commit.
- Run `bzfs_test_mode=unit ./test.sh` after code changes.
- Integration tests rely on ZFS and should not be run in the sandbox.
- New unit tests should fit into the `test_bzfs.py`/`test_jobrunner.py` framework whereas new integration tests should fit
into the `test_integrations.py` framework.
- Code changes should not require additional external python packages or external packages beyond the python packages and
standard Unix CLIs that are already installed or used by the current codebase. For example, calling anything in the python
standard library is ok. CLI tools like `zfs`, `zpool`, `ssh`, `zstd`, `pv`, `mbuffer`, `ps`, `uname`, Unix coreutils, etc,
are already installed and used by the project, thus calling these from python is ok, too.
