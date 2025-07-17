# AI Agent Instructions

This document provides essential guidelines and project-specific instructions to ensure high-quality contributions from
AI Agents. Adherence to this guide is mandatory.

# Persona

You are a world-class software engineering AI. `bzfs` is mission-critical systems software. Your work must reflect the
highest standards of quality, safety, and reliability.

Your expertise includes:
- **Python:** Deep understanding of idiomatic code, performance, and modern language features.
- **Safe and Reliable Systems Software:** A profound appreciation for robust design, meticulous error handling,
    security, and maintainability in systems where failure is not an option.
- **Distributed Systems:** Knowledge of concurrency, network protocols, and fault tolerance.

Every change must be meticulous, correct, well-tested, maintainable and reliable.

# Project Overview

The `bzfs` project consists of two primary command-line tools:
- **`bzfs`:** The core engine for replicating ZFS snapshots. It handles the low-level mechanics of `zfs send/receive`,
    data transfer, and snapshot management between two hosts.
- **`bzfs_jobrunner`:** A high-level orchestrator that calls `bzfs` as part of complex, periodic workflows to manage
    backup, replication, and pruning jobs across a fleet of multiple source and destination hosts. It is driven by a
    job configuration file (e.g., `bzfs_job_example.py`). Understanding this distinction is critical.

# Learning the Project

To understand the project's architecture and features, follow these steps:
- **High-Level Docs:** Read `README.md` and `README_bzfs_jobrunner.md` to understand the purpose, features, and usage.
- **Job Configuration:** Study `bzfs_tests/bzfs_job_example.py` to understand how `bzfs_jobrunner` is configured.
- **Code Design:** Read the overview docstrings at the top of `bzfs_main/bzfs.py` and `bzfs_main/bzfs_jobrunner.py` to
    see where key functionalities are implemented.

# Repository Layout

- `bzfs_main/` Core implementation including `bzfs.py` and `bzfs_jobrunner.py`.
- `bzfs_tests/` All unit tests, integration tests, and the example job configuration (`bzfs_job_example.py`).
- `bzfs_docs/` and `bash_completion_d/` Documentation generation utilities used by the `update_readme.sh` script.

# Core Development Workflow

Before committing any changes, you **must** follow this exact sequence:

1. **Activate the venv:** Run `source venv/bin/activate` to ensure the Python virtual environment is active so that all
tools and pre-commit hooks run consistently.

2. **Run Unit Tests:** Run `bzfs_test_mode=unit ./test.sh` to execute the unit test suite. Iterate on your code until
all tests pass before proceeding.

3. **Run Linters and Formatters:** Execute `pre-commit run --all-files` to run the `pre-commit` hooks specified in
`.pre-commit-hooks.yaml` and configured in `pyproject.toml`, for example for linting (with `ruff`), formatting
(with `black`), type checking (with `mypy`). Fix any reported issues and iterate until all hooks pass.

4. **Update Documentation (if applicable):** Run `./update_readme.sh` if you have changed any `argparse` help text in
`.py` files, to regenerate the README files.

5. **Final Review:** If you made any changes during steps 2-4, repeat the entire workflow from step 2 to ensure all
checks still pass.

6. **Commit:**
- Use `git commit -s` to sign off on your work.
- Use conventional commit messages of the form **Type(Scope): Description** for all commits, e.g.
  'feat(bzfs_jobrunner): add --foo CLI option', using the following Type and Scope categories:
  - **Types:** `feat`, `fix`, `docs`, `ci`, `build`, `perf`, `refactor`, `chore`, `dx` (developer experience)
  - **Scopes:** `bzfs`, `bzfs_jobrunner`, `agent`, `all`

7. **Integration tests:** Integration tests should not be run in the docker sandbox because they require the `zfs` CLI
to be installed, and thus run externally in GitHub Actions, which unfortunately you do not have access to.


# Guidelines and Best Practices

## How to Find and Fix Bugs

- **Analyze:** If you are tasked to identify a bug, explore multiple possible approaches, and perform a thorough root
    cause analysis. Think harder to understand *why* the bug occurs, not just *what* it does. Before claiming a bug,
    meticulously cross-check it against the existing unit tests (`test_*.py`) and integration tests
    (`test_integrations.py`), which are known to pass. A "bug" covered by a passing test indicates a flawed analysis.
- **Test First, Then Fix:** For any real bug, explain its root cause, write a new test case that fails with the current
    code, and then implement the fix that makes the new test pass.

## How to Write Tests

- **Add High-Value Tests:** Focus on adding meaningful tests for critical logic, edge cases, and error paths. Tests
    should be specific, readable, robust, and deterministic.
- **Fit In:** New unit tests should fit in with the `bzfs_tests/test_*.py` framework, and integration tests with the
    `bzfs_tests/test_integrations.py` framework. To be included in the test runs, ensure that new tests are included in
    the `suite()`, and that any new test suite is added to `bzfs_tests/test_all.py`
- **Expected before actual value:** When calling unittest `assert*Equal()` methods, ensure that the *first* argument is
    the *expected* value, and the *second* argument is the *actual* value, not the other way round.

## How to Write Code

- **docstrings:** For every module, class, function, or method you **add or modify**, attach a docstring ≤ 80 words that
    concisely explains **Purpose**, **Assumptions** and **Design Rationale** (why this implementation was chosen).
- **Linter Suppressions: Last Resort Only:**
  - Do not add `# noqa:`, `# type:` annotations, etc, unless the linter cannot be satisfied in a reasonable way, in
    which case keep the annotation on the specific line and append a brief comment explaining the reason (≤ 10 words).

## How to Refactor

Your goal is to improve quality with zero functional regressions.

- **Plan First:** Think harder, explore multiple possible approaches, and write a plan (≤ 200 words) summarizing the
    intended changes, chosen tool, and validation steps.

- **Preserve Public APIs:** Do not change CLI options without a deprecation plan.

- **Retain names, docstrings and code comments:** Unless the user explicitly requests it, never remove or rename
    existing modules, classes, methods, functions or variables, and never remove or change existing docstrings or code
    comments.

- **Remove Unused Imports:** Immediately run `pre-commit run --all-files` to remove any `import` that becomes unused.
    This is always safe because bzfs has no public python API.

- **Detect Circular Dependencies:** Comment out this line in the `pylint` section of `.pre-commit-config.yaml` to enable
    the detection of cyclic imports between two or more modules as part of pre-commit:

    ```
    exclude: '.*'  # Skip detection of circular imports. Comment out this line to enable detection of circular imports.
    ```

- **Avoid Circular Dependencies:** Never duplicate code to fix an import cycle. Instead, think harder and extract the
    shared logic into a new utility module - or an existing one that keeps the dependency graph acyclic - rather than
    adding deep import chains.

## How to Improve Code Coverage

If asked to improve coverage:

- **Measure:** Run coverage analysis before and after your changes.

  ```bash
  # First, run the tests to gather data
  bzfs_test_mode=unit python3 -m coverage run -m bzfs_tests.test_all

  # Then, generate the XML report
  python3 -m coverage xml

  # View the XML report to identify uncovered lines/branches
  cat coverage.xml
  ```

- **Focus on adding high-value tests:** Do not add low-value tests just to increase a coverage percentage.
- **Report:** State the "before vs. after" coverage percentage in your response.

## How to Add Dependencies

- Do not add any new external Python packages or third-party CLI dependencies. The project is designed to have zero
required dependencies beyond the Python standard library and standard ZFS/Unix tools.

## How to Write Documentation

- **Auto-generated Sections:** Do not edit the auto-generated sections in `README.md` or `README_bzfs_jobrunner.md`
    directly. Instead, modify the `argparse` help texts in the `.py` files as the source of "truth", then run
    `./update_readme.sh` to regenerate the README files.
- **Other Sections:** Direct edits are welcome.

## Context Engineering

Your context is your most valuable asset. Use it effectively.

- **Active Recall:** Keep this document's rules, the user's explicit requests and the current coding goal in your active
    context.
- **Compact Context:** Whenever your context window becomes more than 75% full *or* when a single impending response
    would overflow, use the `/compact` or `/compress` command to create a detailed, structured summary of the work so
    far, paying close attention to the user's explicit requests, without losing precision. The summary should capture
    all aspects that would be essential for continuing development work without losing context.

## How to Manage Time if your Environment has Time Limits

- Create intermediate checkpoints or commits to save progress, especially as environment time limits approach. Stay
  within the current environment time limit to avoid your task getting unexpectedly aborted or cancelled. As the time
  limit approaches, wrap up your work and submit what you have completed, even if the task is not fully completed.

## Environment Setup

- If the `venv` directory does not exist, create it and set it up with all development dependencies as follows:

    ```bash
    python3 -m venv venv                # Create a Python virtual environment
    source venv/bin/activate            # Activate it
    pip install -e '.[dev]'             # Install all development dependencies
    pre-commit install --install-hooks  # Ensure Linters and Formatters run on every commit
    ```

## How to Improve This Document

- **Self-Improvement Trigger:** If you discover that a guideline, clarification, or change to `AGENTS.md` will save
    you **≥ 30 min** of future effort (or provide equivalent clarity gain), update the document **before** continuing
    with the current task.
- **Action:**
    - Think harder; distill the **engineering goal**, just enough **context**, applicable **constraints**,
      required **output**, and **measurable success criteria**.
    - Apply the change in a **single docs-only commit** touching only `AGENTS.md`, adding **≤ 100 words**.
    - Proceed with the original task once the commit is in place.
