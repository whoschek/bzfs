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

import re
import sys
import subprocess
import os


def main():
    """
    Run this script to update README.md from the help info contained in bzfs.py.
    Example usage: cd ~/repos/bzfs; test/update-readme.py bzfs/bzfs.py README.md
    This essentially does the following steps:
    brew install pandoc  # OSX
    sudo apt-get -y install pandoc  # Linux
    pip install argparse-manpage
    argparse-manpage --pyfile bzfs/bzfs.py --function argument_parser > /tmp/manpage.1
    pandoc -s -t markdown /tmp/manpage.1 -o /tmp/manpage.md
    Then take that output, auto-clean it and auto-replace certain sections of README.md with it, as shown below:
    """
    if len(sys.argv) != 3:
        print(f"Usage: {os.path.basename(sys.argv[0])} /path/to/bzfs.py path/to/README.md")
        sys.exit(1)

    bzfs_py_file, readme_file = sys.argv[1], sys.argv[2]
    tmp_manpage1_path = "/tmp/manpage.1"
    tmp_manpage_md_path = "/tmp/manpage.md"

    # Step 1: Generate manpage
    with open(tmp_manpage1_path, "w") as fd:
        cmd = ["argparse-manpage", "--pyfile", bzfs_py_file, "--function", "argument_parser"]
        subprocess.run(cmd, check=True, stdout=fd)

    # Step 2: Convert to markdown using pandoc
    cmd = ["pandoc", "-s", "-t", "markdown", tmp_manpage1_path, "-o", tmp_manpage_md_path]
    subprocess.run(cmd, check=True)

    # Step 3: Clean up generated markdown file
    with open(tmp_manpage_md_path, "r", encoding="utf-8") as file:
        content = file.read()
    content = re.sub(r"\\([`#-_|>\[*])", r"\1", content)  # s/\\\([`#-_|>\[\*]\)/\1/g
    content = re.sub(r"\\'", r"'", content)  # s/\\\'/'/g
    content = re.sub(r"\\]", r"\]", content)  # s/\\\]/\]/g
    content = re.sub(r"# OPTIONS", "", content)  # s/# OPTIONS//g
    content = re.sub(r": {3}", r"*  ", content)  # s/:   /*  /g
    with open(tmp_manpage_md_path, "w", encoding="utf-8") as file:
        file.write(content)

    # Extract replacement from cleaned markdown, which is the text between "# DESCRIPTION" and "**SRC_DATASET"
    begin_desc_markdown_tag = "# DESCRIPTION"
    begin_help_markdown_tag = "**SRC_DATASET"
    with open(tmp_manpage_md_path, "r", encoding="utf-8") as f:
        manpage = f.readlines()  # Read the cleaned markdown file
    begin_desc_markdown_idx = next(
        (i for i, line in enumerate(manpage) if line.startswith(begin_desc_markdown_tag)), None
    )
    begin_help_markdown_idx = next(
        (
            i
            for i, line in enumerate(manpage[begin_desc_markdown_idx:], start=begin_desc_markdown_idx)
            if begin_help_markdown_tag in line
        ),
        None,
    )
    if begin_desc_markdown_idx is not None and begin_help_markdown_idx is not None:
        replacement = "".join(manpage[begin_desc_markdown_idx + 1 : begin_help_markdown_idx]).strip()
    else:
        print(f"Markers {begin_desc_markdown_tag} or {begin_help_markdown_tag} not found in the cleaned markdown.")
        sys.exit(1)

    # replace text between '<!-- DESCRIPTION BEGIN -->' and '<!-- END DESCRIPTION SECTION -->' in README.md
    begin_desc_readme_tag = "<!-- BEGIN DESCRIPTION SECTION -->"
    end_desc_readme_tag = "<!-- END DESCRIPTION SECTION -->"
    with open(readme_file, "r", encoding="utf-8") as f:
        readme = f.readlines()
    begin_desc_readme_idx = next((i for i, line in enumerate(readme) if line.strip() == begin_desc_readme_tag), None)
    end_desc_readme_idx = next(
        (
            i
            for i, line in enumerate(readme[begin_desc_readme_idx:], start=begin_desc_readme_idx)
            if line.strip().startswith(end_desc_readme_tag)
        ),
        None,
    )

    if begin_desc_readme_idx is not None and end_desc_readme_idx is not None:
        updated_lines = readme[: begin_desc_readme_idx + 1] + [replacement + "\n\n"] + readme[end_desc_readme_idx:]
        with open(readme_file, "w", encoding="utf-8") as f:
            f.writelines(updated_lines)
    else:
        print(f"Markers {begin_desc_readme_tag} or {end_desc_readme_tag} not found in " + readme_file)
        sys.exit(1)

    begin_help_markdown_idx = next((i for i, line in enumerate(manpage) if begin_help_markdown_tag in line), None)
    if begin_help_markdown_idx is None:
        print(f"Marker {begin_help_markdown_tag} not found in cleaned markdown.")
        sys.exit(1)

    begin_help_readme_tag = "<!-- BEGIN HELP DETAIL SECTION -->"
    with open(readme_file, "r", encoding="utf-8") as f:
        readme = f.readlines()
    begin_help_readme_idx = next((i for i, line in enumerate(readme) if begin_help_readme_tag in line), None)
    if begin_help_readme_idx is None:
        print(f"Marker {begin_help_readme_tag} not found in README.md.")
        sys.exit(1)

    # Extract lines after the marker from bzfs.py
    extracted_lines = manpage[begin_help_markdown_idx:]

    # Retain lines before and including the marker in readme_file and replace the rest
    updated_lines = readme[: begin_help_readme_idx + 1] + extracted_lines

    with open(readme_file, "w", encoding="utf-8") as f:
        f.writelines(updated_lines)

    # os.remove(tmp_manpage1_path)
    # os.remove(tmp_manpage_md_path)
    # os.remove(tmp_manpage_md_path + '.bak')
    print("Done.")


if __name__ == "__main__":
    main()
