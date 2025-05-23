#!/usr/bin/env sh
# Run this script to update README.md from the help info contained in bzfs.py.
set -e
cd $(realpath $(dirname "$0"))
export PATH=bzfs:$PATH
python3 -m bzfs_docs.update_readme bzfs/bzfs.py README.md
python3 -m bzfs_docs.update_readme bzfs/bzfs_jobrunner.py README_bzfs_jobrunner.md
./bash_completion.d/shell-completion-generator.sh > ./bash_completion.d/bzfs-shell-completion
