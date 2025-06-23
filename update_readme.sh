#!/usr/bin/env bash
# Run this script to update README.md from the help info contained in bzfs.py.
set -e
cd $(dirname $(realpath "$0"))

# bzfs_main.* must be part of a venv for `argparse-manpage` to work correctly
tmp_venv=venv-argparse-manpage
if [ -d venv ]; then
  source venv/bin/activate
else
  rm -rf $tmp_venv
  python3 -m venv $tmp_venv
  source $tmp_venv/bin/activate
  pip install -e '.[dev]'
fi

python3 -m bzfs_docs.update_readme bzfs_main.bzfs README.md
python3 -m bzfs_docs.update_readme bzfs_main.bzfs_jobrunner README_bzfs_jobrunner.md
python3 -m bash_completion_d.shell-completion-generator > ./bash_completion_d/bzfs-shell-completion

rm -rf $tmp_venv
