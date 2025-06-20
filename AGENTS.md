# AGENT instructions

- Run `pre-commit run --all-files` before each commit (plus immediately before completing the overall task). This runs
  the hooks specified in `.pre-commit-hooks.yaml` and configured in `pyproject.toml`.
- Run `bzfs_test_mode=unit ./test.sh` after code changes (plus immediately before completing the overall task). This
  runs the unit tests. Integration tests rely on ZFS and should not be run in the sandbox.
- New unit tests should fit into the `test_bzfs.py`/`test_jobrunner.py` framework whereas new integration tests should
  fit into the `test_integrations.py` framework. Make sure to include the new tests in the `suite()`.
- Code changes should not require additional external python packages or external packages beyond the python packages
  and standard Unix CLIs that are already installed or used by the current codebase. For example, calling anything in
  the python standard library is ok. CLI tools like `zfs`, `zpool`, `ssh`, `zstd`, `pv`, `mbuffer`, `ps`, `uname`, Unix
  coreutils, etc, are already installed and used by the project, thus calling these from python is ok, too.
- If the task includes adding unit tests to improve coverage, run the following commands after running unit tests
  (plus immediately before completing the overall task):
  ```
  bzfs_test_mode=unit python3 -m coverage run --branch --include="bzfs_main/*.py" --omit='bzfs_tests/*.py,*/__init__.py' -m bzfs_tests.test_all
  python3 -m coverage xml
  cat coverage.xml
  ```
  To see how coverage has changed, compare the `coverage.xml` file with a prior version of that file. In your PR,
  include as an attachment a zip file containing the output directory of `python3 -m coverage html`.
- After every N code changes (depending on your time limit), create an intermediate commit and automatically checkpoint
  or push it to a save place where you can recover it later in case you get unexpectedly aborted. Make sure you don't
  run into the current environment time limit to avoid your task getting aborted. As you get close to the task time
  limit, wrap it up ASAP even if the task is incomplete, and put together a PR containing the results so far.
