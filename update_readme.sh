#!/usr/bin/env sh
# Run this script to update README.md from the help info contained in bzfs.py.
set -e
cd $(realpath $(dirname "$0"))
python3 -m bzfs_docs.update_readme bzfs/bzfs.py README.md
python3 -m bzfs_docs.update_readme bzfs/bzfs_cron.py README_bzfs_cron.md
