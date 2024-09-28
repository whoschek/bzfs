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
from typing import Sequence, Callable, Optional, TypeVar, Union


def main():
    """
    Run this script to update README.md from the help info contained in bzfs.py.
    Example usage: cd ~/repos/bzfs; test/update-readme.py bzfs/bzfs.py README.md
    This essentially does the following steps:
    argparse-manpage --pyfile bzfs/bzfs.py --function argument_parser > /tmp/manpage.1
    pandoc -s -t markdown /tmp/manpage.1 -o /tmp/manpage.md
    Then takes that output, auto-cleans it and auto-replaces certain sections of README.md with it.

    Before doing so install prerequisites:
    brew install pandoc  # OSX
    sudo apt-get -y install pandoc  # Linux
    pip install argparse-manpage
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
    manpage = content.splitlines(keepends=True)

    # Extract replacement from cleaned markdown, which is the text between "# DESCRIPTION" and "**SRC_DATASET"
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

    # replace text between '<!-- DESCRIPTION BEGIN -->' and '<!-- END DESCRIPTION SECTION -->' in README.md
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

    # os.remove(tmp_manpage1_path)
    # os.remove(tmp_manpage_md_path)
    print("Done.")


T = TypeVar("T")


def find_match(
    seq: Sequence[T],
    predicate: Callable[[T], bool],
    start: Optional[int] = None,
    end: Optional[int] = None,
    reverse: bool = False,
    raises: Union[bool, str, Callable[[], str]] = False,  # raises: bool | str | Callable = False,  # python >= 3.10
) -> int:
    """Returns the integer index within seq of the first item (or last item if reverse==True) that matches the given
    predicate condition. If no matching item is found returns -1 or ValueError, depending on the raises parameter,
    which is a bool indicating whether to raise an error, or a string containing the error message, but can also be a
    Callable/lambda in order to support efficient deferred generation of error messages.
    Analog to str.find(), including slicing semantics with parameters start and end.
    For example, seq can be a list, tuple or str.

    Example usage:
        lst = ["a", "b", "-c", "d"]
        i = find_match(lst, lambda arg: arg.startswith("-"), start=1, end=3, reverse=True)
        if i >= 0:
            ...
        i = find_match(lst, lambda arg: arg.startswith("-"), raises=f"Tag {tag} not found in {file}")
        i = find_match(lst, lambda arg: arg.startswith("-"), raises=lambda: f"Tag {tag} not found in {file}")
    """
    offset = 0 if start is None else start if start >= 0 else len(seq) + start
    if start is not None or end is not None:
        seq = seq[start:end]
    for i, item in enumerate(reversed(seq) if reverse else seq):
        if predicate(item):
            if reverse:
                return len(seq) - i - 1 + offset
            else:
                return i + offset
    if raises is False or raises is None:
        return -1
    if raises is True:
        raise ValueError("No matching item found in sequence")
    if callable(raises):
        raises = raises()
    raise ValueError(raises)


if __name__ == "__main__":
    main()
