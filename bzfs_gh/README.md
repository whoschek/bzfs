<!--
 Copyright 2024 Wolfgang Hoschek AT mac DOT com

 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# GitHub Workflow Utilities

This directory contains helper tools that simplify interacting with GitHub
Actions workflows.  The main entry point is `submit_gh_workflow.py` which can
submit a workflow via the `gh` command line and wait for its completion.

- [Installation](#installation)
- [Usage](#usage)
- [Output Format](#output-format)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## Installation

1. Ensure Python 3.8 or later is available.
2. Install the GitHub CLI (`gh`) and authenticate it with your GitHub account.
   See <https://cli.github.com/> for installation instructions.
3. Clone the `bzfs` repository and install the development dependencies:
   ```sh
   pip install -e '.[dev]'
   ```
   The repository uses `pre-commit` for formatting and linting.  Enable the
   Git hook once with `pre-commit install`.

## Usage

```
python -m bzfs_gh.submit_gh_workflow REPO BRANCH WORKFLOW_FILE [flags]
```

- `REPO` – GitHub repository in the form `owner/repo`.
- `BRANCH` – branch or tag to run against.
- `WORKFLOW_FILE` – path to a YAML file containing the workflow's
  `workflow_dispatch` inputs section.
- The remaining flags correspond to the workflow inputs.  Boolean flags are
  expressed as `--name`/`--no-name` pairs.  Two additional options control
  polling and timeout behaviour:
  - `--poll-secs` – seconds between status checks (default: 15)
  - `--timeout-secs` – overall timeout in seconds (default: 3600)

The script prints progress dots while waiting.  When the workflow finishes it
emits a single line of JSON to `stdout` describing the result.

## Output Format

The JSON object contains the following keys:

- `run_id` – numeric GitHub Actions run identifier
- `conclusion` – workflow conclusion such as `success`, `failure` or
  `timed_out`
- `html_url` – URL of the workflow run
- `log_archive` – path to the downloaded log archive when the conclusion is not
  `success`
- `log_dir` – directory containing the extracted log files when the conclusion
  is not `success`

Example success output:

```json
{"run_id": 1234, "conclusion": "success", "html_url": "https://github.com/..."}
```

Example failure output:

```json
{"run_id": 5678, "conclusion": "failure", "html_url": "https://github.com/...", "log_archive": "/tmp/gh_5678/logs.zip", "log_dir": "/tmp/gh_logs_5678"}
```

## Example: running python-app.yml

To run the `.github/workflows/python-app.yml` workflow for branch
`wip/mypy5` of the repository `whoschek/bzfs`, invoke the tool like so:

```sh
python -m bzfs_gh.submit_gh_workflow \
    whoschek/bzfs wip/mypy5 .github/workflows/python-app.yml \
    --job-name test_ubuntu_24_04_fast --bzfs-test-mode unit --num-runs 1
```

Swap `unit` with `functional` to run integration tests with ZFS:

```sh
python -m bzfs_gh.submit_gh_workflow \
    whoschek/bzfs wip/mypy5 .github/workflows/python-app.yml \
    --job-name test_ubuntu_24_04_fast --bzfs-test-mode functional --num-runs 1
```

The script prints a single JSON line that Codex or any other tool can
parse with `json.loads`.  When the workflow fails the JSON contains
`log_archive` and `log_dir` fields pointing to the downloaded and
extracted logs.

## Troubleshooting

- **Network unavailable** – the tool checks for connectivity before submitting a
  workflow.  Ensure you have access to the internet and that the `gh` CLI can
  reach GitHub.
- **Command fails repeatedly** – error messages from `gh` are printed to
  `stderr`.  The tool retries failed commands up to three times by default.
- **Workflow takes too long** – adjust `--poll-secs` or `--timeout-secs` as
  needed.  On timeout the result will include `"conclusion": "timed_out"`.
- **Log archive missing** – for failed runs the script downloads the log archive
  to a temporary directory.  If the archive field is absent, re-run the command
  or check the GitHub web interface.

## Development

Improvements are welcome.  Unit tests live in `bzfs_tests/test_submit_gh_workflow.py`.
Run the linters and tests with:

```sh
pre-commit run --all-files
bzfs_test_mode=unit ./test.sh
```

The code has no external dependencies beyond the standard library and `gh`.
New features should include matching unit tests.  See the existing tests for
examples of mocking out subprocess calls and network checks.
