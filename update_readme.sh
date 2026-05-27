#!/usr/bin/env bash

# Regenerate both README files from argparse help text and refresh shell completion.
# Prerequisites: See AGENTS.md section "How to Set up the Environment"

set -e
cd "$(dirname "$(realpath "${BASH_SOURCE[0]}")")"

export NO_COLOR=1  # don't add color codes to generated man pages
python3 -m bzfs_main.util.update_readme_from_argparse --module=bzfs_main.bzfs --readme=README.md
python3 -m bzfs_main.util.update_readme_from_argparse --module=bzfs_main.bzfs_jobrunner --readme=README_bzfs_jobrunner.md
python3 -m bash_completion_d.shell_completion_generator > ./bash_completion_d/bzfs-shell-completion
