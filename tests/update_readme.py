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

import os
import re
import sys
import subprocess

from bzfs import bzfs
from tests.test_units import find_match


def main():
    """
    Run this script to update README.md from the help info contained in bzfs.py.
    Example usage: cd ~/repos/bzfs; python3 -m tests.update_readme bzfs/bzfs.py README.md
    This essentially does the following steps:
    argparse-manpage --pyfile bzfs/bzfs.py --function argument_parser > /tmp/manpage.1
    pandoc -s -t markdown /tmp/manpage.1 -o /tmp/manpage.md
    Then takes that output, auto-cleans it and auto-replaces certain sections of README.md with it.

    Before doing so install prerequisites:
    brew install pandoc  # OSX
    sudo apt-get -y install pandoc  # Linux
    pip install argparse-manpage  # see https://github.com/praiskup/argparse-manpage
    """
    if len(sys.argv) != 3:
        print("Usage: cd ~/repos/bzfs; python3 -m tests.update_readme /path/to/bzfs.py path/to/README.md")
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
    content = content.replace(r"\`\`\`", "\n```\n")
    content = re.sub(r"\\([`#-_|~>\[*])", r"\1", content)  # s/\\\([`#-_|>\[\*]\)/\1/g
    content = re.sub(r"\\'", r"'", content)  # s/\\\'/'/g
    content = re.sub(r"\\]", r"\]", content)  # s/\\\]/\]/g
    content = re.sub(r"# OPTIONS", "", content)  # s/# OPTIONS//g
    content = re.sub(r": {3}", r"*  ", content)  # s/:   /*  /g
    manpage = content.splitlines(keepends=True)

    # Step 4: Replace Description Section
    # Step 4a: Extract replacement from cleaned markdown, which is the text between "# DESCRIPTION" and "**SRC_DATASET"
    begin_desc_markdown_tag = "# DESCRIPTION"
    begin_help_markdown_tag = "**SRC_DATASET"
    begin_desc_markdown_idx = find_match(
        manpage,
        lambda line: line.startswith(begin_desc_markdown_tag),
        raises=f"{begin_desc_markdown_tag} not found in the cleaned markdown",
    )
    begin_help_markdown_idx = find_match(
        manpage,
        lambda line: begin_help_markdown_tag in line,
        start=begin_desc_markdown_idx,
        raises=f"{begin_help_markdown_tag} not found in the cleaned markdown",
    )
    replacement = "".join(manpage[begin_desc_markdown_idx + 1 : begin_help_markdown_idx]).strip()

    # Step 4b: replace text between '<!-- DESCRIPTION BEGIN -->' and '<!-- END DESCRIPTION SECTION -->' in README.md
    begin_desc_readme_tag = "<!-- BEGIN DESCRIPTION SECTION -->"
    end_desc_readme_tag = "<!-- END DESCRIPTION SECTION -->"
    with open(readme_file, "r", encoding="utf-8") as f:
        readme = f.readlines()
    begin_desc_readme_idx = find_match(
        readme,
        lambda line: line.strip() == begin_desc_readme_tag,
        raises=f"{begin_desc_readme_tag} not found in {readme_file}",
    )
    end_desc_readme_idx = find_match(
        readme,
        lambda line: line.strip().startswith(end_desc_readme_tag),
        start=begin_desc_readme_idx,
        raises=f"{end_desc_readme_tag} not found in {readme_file}",
    )
    readme = readme[: begin_desc_readme_idx + 1] + [replacement + "\n\n"] + readme[end_desc_readme_idx:]

    # Step 5: Replace Usage Overview Section
    begin_help_overview_tag = "<!-- BEGIN HELP OVERVIEW SECTION -->"
    begin_help_overview_idx = find_match(
        readme,
        lambda line: begin_help_overview_tag in line,
        raises=f"{begin_help_overview_tag} not found in {readme_file}",
    )
    end_help_overview_tag = "<!-- END HELP OVERVIEW SECTION -->"
    end_help_overview_idx = find_match(
        readme,
        lambda line: end_help_overview_tag in line,
        start=begin_help_overview_idx,
        raises=f"{end_help_overview_tag} not found in {readme_file}",
    )
    os.environ["COLUMNS"] = "72"
    help_msg = bzfs.argument_parser().format_usage()
    help_msg = ["```\n"] + help_msg.splitlines(keepends=True) + ["```\n"]
    readme = readme[: begin_help_overview_idx + 1] + help_msg + readme[end_help_overview_idx:]

    # Step 6: Replace Usage Details Section
    begin_help_markdown_idx = find_match(
        manpage,
        lambda line: begin_help_markdown_tag in line,
        raises=f"Marker {begin_help_markdown_tag} not found in cleaned markdown.",
    )
    begin_help_readme_tag = "<!-- BEGIN HELP DETAIL SECTION -->"
    begin_help_readme_idx = find_match(
        readme,
        lambda line: begin_help_readme_tag in line,
        raises=f"{begin_help_readme_tag} not found in {readme_file}",
    )

    # add anchors to be able to link to each CLI option
    def substitute_fn(match: re.Match) -> str:
        anchor_id = match.group(1).replace(" ", "_")
        return f'<div id="{anchor_id}"></div>\n\n{match.group(0)}'

    manpage = [re.sub(r"\*\*([^*]+?)\*\*.*", substitute_fn, x) for x in manpage[begin_help_markdown_idx:]]

    # Retain lines before and including the marker in readme_file and replace the rest
    readme = readme[: begin_help_readme_idx + 1] + manpage
    with open(readme_file, "w", encoding="utf-8") as f:
        f.writelines(readme)

    print("Done.")


if __name__ == "__main__":
    main()
