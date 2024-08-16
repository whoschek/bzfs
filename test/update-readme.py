#!/usr/bin/env python3
#
# Copyright 2024 Wolfgang Hoschek AT mac DOT com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import subprocess
import os


def main():
    """
    Run this script to update README.md from the help info contained in wbackup_zfs.py.
    This essentially does the following steps:
    brew install pandoc; pip install argparse-manpage
    argparse-manpage --pyfile wbackup_zfs/wbackup_zfs.py --function argument_parser > /tmp/manpage.1
    pandoc -s -t markdown /tmp/manpage.1 -o /tmp/manpage.md
    sed -i.bak -e 's/\\\([`#-_|>\[\*]\)/\1/g' -e "s/\\\'/'/g" -e "s/\\\]/\]/g" -e 's/# OPTIONS//g' -e 's/:   /*  /g' /tmp/manpage.md
    Then take that output and replace certain sections of README.md with it, as shown below:
    """
    if len(sys.argv) != 3:
        print(f"Usage: {os.path.basename(sys.argv[0])} /path/to/wbackup_zfs.py path/to/README.md")
        sys.exit(1)

    wbackup_zfs_py_file, readme_file = sys.argv[1], sys.argv[2]
    tmp_manpage1_path = '/tmp/manpage.1'
    tmp_manpage_md_path = '/tmp/manpage.md'

    # Step 1: Generate manpage
    with open(tmp_manpage1_path, 'w') as fd:
        cmd = ['argparse-manpage', '--pyfile', wbackup_zfs_py_file, '--function', 'argument_parser']
        subprocess.run(cmd, check=True, stdout=fd)

    # Step 2: Convert to markdown using pandoc
    cmd = ['pandoc', '-s', '-t', 'markdown', tmp_manpage1_path, '-o', tmp_manpage_md_path]
    subprocess.run(cmd, check=True)

    # Step 3: Clean up markdown file
    cmd = ['sed', '-i.bak', '-e', r's/\\\([`#-_|>\[\*]\)/\1/g', '-e', r"s/\\\'/'/g", '-e', r"s/\\\]/\]/g", '-e', r's/# OPTIONS//g', '-e', r's/:   /*  /g', tmp_manpage_md_path]
    subprocess.run(cmd, check=True)

    # Read the cleaned markdown file
    with open(tmp_manpage_md_path, 'r') as f:
        manpage_lines = f.readlines()

    # Extract replacement_text from cleaned markdown
    src_dataset_marker = '**SRC_DATASET**'
    description_marker = '# DESCRIPTION'
    start_description = next((i for i, line in enumerate(manpage_lines)
                              if line.startswith(description_marker)), None)
    start_src_dataset = next((i for i, line in enumerate(manpage_lines[start_description:], start=start_description)
                              if src_dataset_marker in line), None)
    if start_description is not None and start_src_dataset is not None:
        replacement_text = ''.join(manpage_lines[start_description + 1:start_src_dataset]).strip()
    else:
        print(f"Markers {description_marker} or {src_dataset_marker} not found in the cleaned markdown.")
        sys.exit(1)

    with open(readme_file, 'r') as f:
        readme_lines = f.readlines()

    # processing to replace text between 'wbackup-zfs' and 'How To Install, Run and Test' in README.md
    wbackup_marker = 'wbackup-zfs'
    install_marker = '# How To Install and Run'

    start_wbackup = next((i for i, line in enumerate(readme_lines)
                          if line.strip() == wbackup_marker), None)
    start_install = next((i for i, line in enumerate(readme_lines[start_wbackup:], start=start_wbackup)
                          if line.strip().startswith(install_marker)), None)

    if start_wbackup is not None and start_install is not None:
        # Retain the first line after the wbackup_marker as is
        updated_lines = readme_lines[:start_wbackup + 2] + [replacement_text + '\n\n'] + readme_lines[start_install:]
        with open(readme_file, 'w') as f:
            f.writelines(updated_lines)
    else:
        print(f"Markers {wbackup_marker} or {install_marker} not found in " + readme_file)
        sys.exit(1)

    with open(readme_file, 'r') as f:
        readme_lines = f.readlines()

    start_index1 = next((i for i, line in enumerate(manpage_lines) if src_dataset_marker in line), None)
    start_index2 = next((i for i, line in enumerate(readme_lines) if src_dataset_marker in line), None)
    if start_index1 is None or start_index2 is None:
        print(f"Marker {src_dataset_marker} not found in one of the files.")
        sys.exit(1)

    # Extract lines after the marker from wbackup_zfs.py
    extracted_lines = manpage_lines[start_index1 + 1:]

    # Retain lines before and including the marker in readme_file and replace the rest
    updated_lines = readme_lines[:start_index2 + 1] + extracted_lines

    with open(readme_file, 'w') as f:
        f.writelines(updated_lines)

    # os.remove(tmp_manpage1_path)
    # os.remove(tmp_manpage_md_path)
    # os.remove(tmp_manpage_md_path + '.bak')
    print("Done.")


if __name__ == "__main__":
    main()
