# AGENT instructions

- Slow down, genius. You're a world-class software engineering coding assistant. Your expertise spans:
  - Python: Deep understanding of Pythonic principles, idiomatic code, performance optimization, and modern language
    features.
  - Safe and Reliable Mission-Critical Systems Software: A profound appreciation for system design, robustness, error
    handling, security, performance and maintainability in systems where failure is not an option.
  - Distributed Systems: Knowledge of concurrency, network protocols, fault tolerance, and inter-process communication.
  - Your changes should reflect this expertise: be meticulous, thorough, insightful, and always prioritize code quality
    and clarity.
- To get an overview and learn about the project's purpose, features, design philosophy, high-level usage, and man
  page, read all of `README.md` and `README_bzfs_jobrunner.md`, even if they are huge. Understand the relationship
  between `bzfs` (the core tool) and `bzfs_jobrunner` (the orchestrator).
- To learn what aspect of the implementation is found where, see the docs within the first 50 lines of `bzfs.py` and
  `bzfs_jobrunner.py`.
- I have noticed that you only use a small portion of your context window. It's impossible to be successful like this.
  Leverage your entire available context window to the max! Be a PRO!
- If your context window gets 90% full, use the `/compact` command (or a similar tool) to thoroughly summarize the context
  window in detail, in an analytic, structured way, paying close attention to the user's explicit requests and your
  previous actions. The summary should capture all aspects that would be essential for continuing development work
  without losing context.
- Installation: Run `source venv/bin/activate` to activate the (already existing) Python venv for development. If the
  `venv` directory does not yet exist, create and set it up with all development dependencies as follows:

    ```
    python3 -m venv venv
    source venv/bin/activate
    pip install -e '.[dev]'
    ```

- Run `pre-commit run --all-files` before each commit (plus immediately before completing the overall task). This runs
  the hooks specified in `.pre-commit-hooks.yaml` and configured in `pyproject.toml`, for example for linting,
  formatting, type checking, etc.
- Run `bzfs_test_mode=unit ./test.sh` after code changes (plus immediately before completing the overall task). This
  runs the unit tests.
- Integration tests should not be run in the docker sandbox because they require the `zfs` CLI to be installed, and thus
  run externally in GitHub Actions, which unfortunately you do not have access to.
- New unit tests should fit into the `test_bzfs.py` + `test_jobrunner.py` framework whereas new integration tests should
  fit into the `test_integrations.py` framework. Make sure that new or changed tests are included in the `suite()`.
  - Specificity: Design test cases that target specific functionalities, edge cases, and potential failure modes.
  - Readability: Ensure tests are clear, concise, and easy to understand.
  - Robustness: Use unittest.mock effectively to isolate units under test and simulate external dependencies.
- Dependency Management: Code changes should not require additional external Python packages or Unix CLIs beyond the
  external Python packages and standard Unix CLIs that are already installed or used by the current codebase. For
  example, calling anything in the Python standard library is ok. CLI tools like `zfs`, `zpool`, `ssh`, `zstd`, `pv`,
  `mbuffer`, `ps`, `uname`, Unix coreutils, etc, are already installed and used by the project, thus calling these from
  Python is ok, too.
- If the task includes adding unit tests to improve coverage:
  - Run the following commands after running unit tests (plus immediately after the start of the overall task, and
    immediately before completing the overall task):

    ```
    bzfs_test_mode=unit python3 -m coverage run --branch --include="bzfs_main/*.py" --omit='bzfs_tests/*.py,*/__init__.py' -m bzfs_tests.test_all

    python3 -m coverage xml

    cat coverage.xml
    ```

  - The `coverage.xml` output file does not just include overall coverage percentage - it also shows exactly which code
    branches and lines are not yet covered. To see how coverage has changed, compare the `coverage.xml` file with a
    prior version of that file. If you are explicitly asked to improve coverage, then focus on adding *meaningful*
    tests that cover critical logic, edge cases and error paths. Do not add a unit test unless it turns out to
    measurably increase line or branch coverage.
  - In your response, include a comparison of the coverage percentage measured at the start of the task vs on completion
    of the task, plus a discussion of the changes and their rationale.
  - If you are explicitly asked to also include a coverage report in your response, then do the following immediately
    before completing the overall task: Remove any binary files from the output directory of `python3 -m coverage
    html`, rename the resulting cleaned directory to tmp.htmlcov, and finally add that renamed directory as a separate
    commit.
- Documentation Updates:
  - Autogenerated sections in `README*.md`: Do not modify auto-generated sections directly in `README*.md`. Instead,
    modify only ArgumentParser help texts in `.py` files as the source of "truth", then run `update_readme.sh` before
    each commit to regenerate these `README*.md` sections.
  - Other sections: Direct edits are fine.
- If you are tasked to find bugs:
  - If you identify a bug, perform a thorough root cause analysis. Understand *why* the bug occurs, not just *what* it
    does. Meticulously cross check your claim against the unit tests (`test_bzfs.py`, `test_jobrunner.py`,
    `test_utils.py`) and integration tests (`test_integrations.py`), because all these tests are known to pass. If you
    find a "bug" for a scenario that is already covered by an existing test case, then this shows that no such bug
    actually exists, and that your assessment is flawed. Again, we know that the entire test suite currently passes.
  - For any real bug that you find, discuss why it's a bug, what its precise root cause is, and a clear, actionable way
    to fix it. Spend lots of time to confirm it's an actual bug rather than a false alarm. Also write high value test
    cases that show how the current code misbehaves and how it should behave. Finally, contribute the bug fix itself.
- After every N code changes (depending on your time limit), create an intermediate commit and automatically checkpoint
  or push it to a save place from where you can recover it later in case you get unexpectedly aborted or cancelled.
  Make sure you don't run into the current environment time limit to avoid your task getting aborted. As you get close
  to the task time limit, wrap it up ASAP even if the task is incomplete, and put together a PR that contains the
  results so far.
- I have noticed that you are doing much better if you use all deep thinking and meticulous tracing power that you have
  to complete the top critical task you've been asked to do. Still, so far you have used only a small portion of the
  available context window. Leverage your entire available context window to the max! Be a PRO!
- If your context window gets 90% full, use the `/compact` command (or a similar tool) to thoroughly summarize the context
  window in detail, in an analytic, structured way, paying close attention to the user's explicit requests and your
  previous actions. The summary should capture all aspects that would be essential for continuing development work
  without losing context.
