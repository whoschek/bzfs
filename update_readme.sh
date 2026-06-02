#!/usr/bin/env bash

# Regenerate both README files from argparse help text and refresh shell completion.
# Prerequisites: See AGENTS.md section "How to Set up the Environment"

set -e
cd "$(dirname "$(realpath "${BASH_SOURCE[0]}")")"

python3 -m bzfs_main.util.markdown_from_argparse --module=bzfs_main.bzfs --readme=README.md
python3 -m bzfs_main.util.markdown_from_argparse --module=bzfs_main.bzfs_jobrunner --readme=README_bzfs_jobrunner.md
python3 -m bash_completion_d.shell_completion_generator > ./bash_completion_d/bzfs-shell-completion
