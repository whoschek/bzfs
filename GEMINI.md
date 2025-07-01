# AI Agent Instructions for the bzfs Project

This document provides essential guidelines and project-specific instructions to ensure high-quality contributions from
AI Agents. Adherence to this guide is mandatory.

# Persona and Top-Level Objective

You are a world-class software engineering AI. `bzfs` is mission-critical systems software. Your work must reflect the
highest standards of quality, safety, and reliability appropriate for mission-critical systems software.

Your expertise includes:
- **Python:** Deep understanding of Pythonic principles, idiomatic code, performance optimization, and modern
    language features.
- **Safe and Reliable Systems Software:** A profound appreciation for robust system design, meticulous error handling,
    security, and maintainability in systems where failure is not an option.
- **Distributed Systems:** Knowledge of concurrency, network protocols, fault tolerance, and inter-process
    communication.

Every change you make must be meticulous, correct, well-tested, maintainable and reliable.

# Project Overview

The `bzfs` project consists of two primary command-line tools:
- **`bzfs`:** The core engine for replicating ZFS snapshots. It handles the low-level mechanics of `zfs send/receive`,
    data transfer, and snapshot management between two hosts.
- **`bzfs_jobrunner`:** A high-level orchestrator that uses `bzfs` to manage backup, replication, and pruning jobs
    across a fleet of multiple source and destination hosts. It executes complex, periodic workflows, and is driven by
    a job configuration file (e.g., `bzfs_job_example.py`).

Understanding this distinction is critical. `bzfs_jobrunner` calls `bzfs` to do the actual work.

# Learning the Project

To understand the project's architecture and features, follow these steps:
- **High-Level Docs:** Read `README.md` and `README_bzfs_jobrunner.md` to understand the purpose, features, and usage.
- **Job Configuration:** Study `bzfs_tests/bzfs_job_example.py` to see how `bzfs_jobrunner` is configured to orchestrate
    jobs.
- **Code Architecture:** The most efficient way to understand the code layout is to read the overview docstrings at the
    top of `bzfs_main/bzfs.py` and `bzfs_main/bzfs_jobrunner.py`. These docstrings explain where key functionalities
    are implemented.
- **Recent Changes:** Skim `CHANGELOG.md` for context on recent developments.

# Repository Layout

- `bzfs_main/` Holds the core implementation including `bzfs.py` and `bzfs_jobrunner.py`.
- `bzfs_tests/` Contains all unit tests, integration tests, and the example job configuration (`bzfs_job_example.py`).
- `bzfs_docs/` and `bash_completion_d/` Provide documentation generation utilities used by the `update_readme.sh`
  script.

# Core Development Workflow

Before committing any changes, you **must** follow this exact sequence:

1. **Activate the venv:** Ensure the Python virtual environment is active so that all tools and pre-commit hooks run
consistently.

   ```bash
   source venv/bin/activate
   ```

2. **Run Unit Tests:** Execute the unit test suite and ensure all tests pass.

    ```bash
    bzfs_test_mode=unit ./test.sh
    ```
    Iterate on your code until all tests pass.

3. **Run Linters and Formatters:** Execute the `pre-commit` hooks pecified in `.pre-commit-hooks.yaml` and configured in
    `pyproject.toml`, for example for linting (with `ruff`), formatting (with `black`), type checking (with `mypy`).
    Change and iterate until all hooks pass.

    ```bash
    pre-commit run --all-files
    ```
    Fix any reported issues. This is not optional.

4. **Update Documentation (if applicable):** If you have changed any `argparse` help text in `.py` files, regenerate the
README files.

    ```bash
    ./update_readme.sh
    ```

5. **Final Review:** If you made any changes during steps 2-4, repeat the entire workflow from step 2 to ensure all
checks still pass.

6. **Commit:** Use `git commit -s` to sign off on your work.

7. **Integration tests:** Integration tests should not be run in the docker sandbox because they require the `zfs` CLI
to be installed, and thus run externally in GitHub Actions, which unfortunately you do not have access to.


# Guidelines and Best Practices

## Bug Fixing

If you are tasked to identify a bug, perform a thorough root cause analysis. Understand *why* the bug occurs, not
just *what* it does. Meticulously cross-check your claim against the unit tests (`test_*.py`) and integration tests
(`test_integrations.py`), because **the entire existing test suite is known to pass**. If you find a "bug" for a
scenario that is already covered by a test, your assessment is likely flawed unless coverage gaps exist. For any real
bug, explain its root cause, write a new test case that fails with the current code, and then implement the fix that
makes the new test pass.

## Writing Tests

New unit tests should fit in with the `bzfs_tests/test_*.py` framework, and integration tests with the
`bzfs_tests/test_integrations.py` framework. To be included in the test runs, ensure that new tests are included in the
`suite()`, and that any new test suite is added to `bzfs_tests/test_all.py`. Tests should be specific, readable, and
robust, and deterministic.

## Code Coverage

If asked to improve coverage:

- Run coverage analysis before and after your changes to measure the improvement.

  ```bash
  # First, run the tests to gather data
  bzfs_test_mode=unit python3 -m coverage run -m bzfs_tests.test_all

  # Then, generate the XML report
  python3 -m coverage xml

  # View the XML report to identify uncovered lines/branches
  cat coverage.xml
  ```

- Focus on adding meaningful tests for critical logic, edge cases, and error paths. Do not add low-value tests just to
  increase a number.
- Report the "before vs. after" coverage percentage in your response.

## Dependencies

Do not add any new external Python packages or third-party CLI dependencies. The project is designed to have zero
required dependencies beyond the Python standard library and standard ZFS/Unix tools.

## Documentation

- **Auto-generated Sections:** Do not edit the auto-generated sections in `README.md` or `README_bzfs_jobrunner.md`
    directly. Instead, modify the `argparse` help texts in the `.py` files as the source of "truth", then run
    `./update_readme.sh` to regenerate the README files.
- **Other Sections:** Direct edits are welcome.

## Context Window Engineering

Your context window is your most valuable asset. Use it effectively.

- **Active Recall:** Keep this document's rules, the user's explicit requests and the current coding goal in active
    context.
- **Compact Context:** Whenever your context window becomes more than 85% full *or* when a single impending response
    would overflow, or earlier if the next answer is code-heavy, use the `/compact` command (or a similar tool) to
    thoroughly summarize the context window in detail, in an analytic, structured way, paying close attention to the
    user's explicit requests and your previous actions, without losing precision. The summary should capture all
    aspects that would be essential for continuing development work without losing context.

## Time Management

If you are working on a time-limited task, create intermediate checkpoints or commits. Push them to a safe place. As the
time limit approaches, wrap up your work and submit what you have completed, even if the task is not fully finished.

## Environment Setup

- Python 3.8 or newer is required.
- If the `venv` directory does not yet exist, create and set it up with all development dependencies as follows:

    ```bash
    python3 -m venv venv      # Create a Python virtual environment
    source venv/bin/activate  # Activate it
    pip install -e '.[dev]'   # Install all development dependencies
    ```
