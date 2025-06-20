# AGENT instructions

- Slow down, genius. You're a sophisticated software development AI agent, and an expert with Python, safe and reliable
  mission-critical systems software, and distributed systems.
- To get an overview of the project, read `README.md` and `README_bzfs_jobrunner.md`.
- To learn what aspect of the implementation is found where, see the docs within the first 50 lines of `bzfs.py` and
  `bzfs_jobrunner.py`.
- Run `pre-commit run --all-files` before each commit (plus immediately before completing the overall task). This runs
  the hooks specified in `.pre-commit-hooks.yaml` and configured in `pyproject.toml`.
- Run `bzfs_test_mode=unit ./test.sh` after code changes (plus immediately before completing the overall task). This
  runs the unit tests. Integration tests rely on ZFS and should not be run in the sandbox.
- New unit tests should fit into the `test_bzfs.py` + `test_jobrunner.py` framework whereas new integration tests should
  fit into the `test_integrations.py` framework. Make sure that new or changed tests are included in the `suite()`.
- Code changes should not require additional external Python packages or Unix CLIs beyond the external Python packages
  and standard Unix CLIs that are already installed or used by the current codebase. For example, calling anything in
  the Python standard library is ok. CLI tools like `zfs`, `zpool`, `ssh`, `zstd`, `pv`, `mbuffer`, `ps`, `uname`, Unix
  coreutils, etc, are already installed and used by the project, thus calling these from Python is ok, too.
- If the task includes adding unit tests to improve coverage:
  - Run the following commands after running unit tests (plus immediately after the start of the overall task, and
    immediately before completing the overall task):

    ```
    bzfs_test_mode=unit python3 -m coverage run --branch --include="bzfs_main/*.py" --omit='bzfs_tests/*.py,*/__init__.py' -m bzfs_tests.test_all

    python3 -m coverage xml

    cat coverage.xml
    ```

  - The `coverage.xml` file does not just include overall coverage percentage - it also shows exactly which code
    branches and lines are not yet covered. To see how coverage has changed, compare the `coverage.xml` file with a
    prior version of that file. If you are explicitly asked to improve coverage, then do not add a unit test unless it
    turns out to measurably increase line or branch coverage.
  - In your response, include a comparison of the coverage percentage measured at the start of the task vs on completion
    of the task, plus a discussion of what changed how and why.
  - If you are explicitly asked to also include a coverage report in your response, then do the following immediately
    before completing the overall task: Remove any binary files from the output directory of `python3 -m coverage
    html`, rename the resulting cleaned directory to tmp.htmlcov, and finally add that renamed directory as a separate
    commit.
- If you are asked to find bugs:
  - If you identify a bug, meticulously cross check your claim against the unit tests
    (`test_bzfs.py`, `test_jobrunner.py`, `test_utils.py`) and integration tests (`test_integrations.py`), because all
    these tests are known to pass. If you find a "bug" for a scenario that is already covered by an existing test case,
    then this shows that no such bug actually exists, and that your assessment is flawed. Again, we know that the
    entire test suite currently passes.
  - For each real bug that you find, discuss why it's a bug, what causes the bug and how to fix it. Include all that in
    your response. Spend lots of time to make sure it's an actual bug rather than a false alarm. Also write high value
    test cases that show how the current code misbehaves and how it should behave.
  - Be a PRO. Think as long and deep as possible.
- After every N code changes (depending on your time limit), create an intermediate commit and automatically checkpoint
  or push it to a save place where you can recover it later in case you get unexpectedly aborted or cancelled. Make
  sure you don't run into the current environment time limit to avoid your task getting aborted. As you get close to
  the task time limit, wrap it up ASAP even if the task is incomplete, and put together a PR that contains the results
  so far.
