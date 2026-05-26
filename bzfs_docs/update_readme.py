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
#
"""Regenerate README markdown sections directly from argparse parser definitions."""

from __future__ import (
    annotations,
)
import argparse
import importlib
import os
import re
import sys
import textwrap
from pathlib import (
    Path,
)
from typing import (
    Final,
)

from bzfs_main.util.utils import (
    find_match,
)

# constants:
_WRAP_COLUMNS: Final[int] = 98
_DEFAULT_GROUPS: Final[frozenset] = frozenset(["positional arguments", "optional arguments", "options"])
TRIPLE_BACKTICK: Final[str] = "```"


def main() -> None:
    """Runs the README generation CLI."""
    if len(sys.argv) != 3:
        raise SystemExit("Usage: cd ~/repos/bzfs; python3 -m bzfs_docs.update_readme bzfs_main.bzfs path/to/README.md")

    parser: argparse.ArgumentParser = importlib.import_module(sys.argv[1]).argument_parser()
    readme_path = Path(sys.argv[2])
    readme = readme_path.read_text(encoding="utf-8")
    readme = render_readme(parser, readme)
    readme_path.write_text(readme, encoding="utf-8")
    print("Done.")


def render_readme(parser: argparse.ArgumentParser, readme: str) -> str:
    """Returns README text with generated sections replaced from argparse."""
    description: str = "\n".join(_render_blocks(parser.description or "")).strip() + "\n\n"
    usage: list[str] = [TRIPLE_BACKTICK + "\n"] + _format_usage(parser).splitlines(keepends=True) + [TRIPLE_BACKTICK + "\n"]
    help_detail: str = _help_detail(parser)

    lines: list[str] = readme.splitlines(keepends=True)
    lines = _replace(lines, "<!-- BEGIN DESCRIPTION SECTION -->", [description], end_tag="<!-- END DESCRIPTION SECTION -->")
    lines = _replace(lines, "<!-- BEGIN HELP OVERVIEW SECTION -->", usage, end_tag="<!-- END HELP OVERVIEW SECTION -->")
    lines = _replace(lines, "<!-- BEGIN HELP DETAIL SECTION -->", [help_detail])
    return "".join(lines)


def _replace(lines: list[str], begin_tag: str, replacement: list[str], *, end_tag: str | None = None) -> list[str]:
    """Returns lines with a marked README section replaced; when end_tag is omitted, replacement extends to EOF."""
    begin: int = find_match(lines, lambda line: begin_tag in line, raises=f"{begin_tag} not found")
    if end_tag is None:
        return lines[: begin + 1] + replacement
    else:
        end: int = find_match(lines, lambda line: end_tag in line, start=begin + 1, raises=f"{end_tag} not found")
        return lines[: begin + 1] + replacement + lines[end:]


def _render_blocks(text: str, *, list_item: bool = False) -> list[str]:
    """Renders argparse help into the README's supported Markdown subset.

    Assumes prose paragraphs are separated by blank lines and fenced examples use triple backticks. Prose is normalized and
    wrapped; headings and fenced code stay structural. In list_item mode, the first block forms the bullet body and later
    blocks are indented beneath it, avoiding a Markdown parser while preserving readable generated option help.
    """
    initial_indent: str = "*  " if list_item else ""
    subsequent_indent: str = "    " if list_item else ""

    # Split prose around fenced code so blank lines inside examples survive.
    blocks: list[str] = []
    for i, part in enumerate(text.strip("\n").split(TRIPLE_BACKTICK)):
        if i % 2 != 0:
            blocks.append(TRIPLE_BACKTICK + part + TRIPLE_BACKTICK)
        else:
            blocks += [block for block in re.split(r"\n\s*\n", part) if block.strip()]

    results: list[str] = []
    for i, block in enumerate(blocks):
        if i > 0:
            results.append("")
        indent: str = initial_indent if i == 0 else subsequent_indent
        if block.startswith(TRIPLE_BACKTICK) and block.endswith(TRIPLE_BACKTICK):
            results += [""] + [indent + line for line in block.splitlines()] + [""]
        elif re.fullmatch(r"#{1,6} .+", block):  # Keep Markdown headings unwrapped
            results.append(indent + block)
        else:
            results += _wrap_text(" ".join(block.split()), initial_indent=indent, subsequent_indent=subsequent_indent)
    return results


def _help_detail(parser: argparse.ArgumentParser) -> str:
    """Renders argparse actions as anchored Markdown reference entries."""
    results: list[str] = []
    groups = parser._action_groups  # noqa: SLF001  # pylint: disable=protected-access  # argparse has no public iterator
    for group in groups:
        actions = [
            action
            for action in group._group_actions  # noqa: SLF001  # pylint: disable=protected-access  # no public iterator
            if action.help != argparse.SUPPRESS and "--help" not in action.option_strings
        ]
        if len(actions) == 0:
            continue

        title = None if group.title is None or group.title in _DEFAULT_GROUPS else group.title.upper()
        if title:
            results += [f"# {title}", ""]
            if group.description and group.description != argparse.SUPPRESS:
                results += _render_blocks(group.description)
                results.append("")

        for i, action in enumerate(actions):
            anchor, header = _action_heading(parser, action)
            results += [f'<div id="{anchor}"></div>', "", header, ""]
            if action.help:
                help_text = parser._get_formatter()._expand_help(action)  # noqa: SLF001  # pylint: disable=protected-access
                results += _render_blocks(help_text, list_item=True)
                results.append("")
            if i != len(actions) - 1:
                results += ["<!-- -->", ""]  # Prevent adjacent lists from merging
    return "\n".join(results)


def _action_heading(parser: argparse.ArgumentParser, action: argparse.Action) -> tuple[str, str]:
    """Returns the README anchor and Markdown heading for one argparse action."""
    formatter = parser._get_formatter()  # noqa: SLF001  # pylint: disable=protected-access
    if not action.option_strings:
        label = formatter._metavar_formatter(action, action.dest)(1)[0]  # noqa: SLF001  # pylint: disable=protected-access
        return label.replace(" ", "_"), f"**{label}**"
    elif action.nargs == 0:
        header = ", ".join(f"**{option}**" for option in action.option_strings)
    else:
        args: str = formatter._format_args(action, action.dest.upper())  # noqa: SLF001  # pylint: disable=protected-access
        header = ", ".join(f"**{option}** *{args}*" for option in action.option_strings)
    return action.option_strings[0].replace(" ", "_"), header


def _wrap_text(text: str, *, width: int = _WRAP_COLUMNS, initial_indent: str = "", subsequent_indent: str = "") -> list[str]:
    """Wraps prose without splitting words, option names, or paths."""
    return textwrap.wrap(
        text,
        width=width,
        initial_indent=initial_indent,
        subsequent_indent=subsequent_indent,
        break_long_words=False,
        break_on_hyphens=False,
    )


def _format_usage(parser: argparse.ArgumentParser) -> str:
    previous_columns: str | None = os.environ.get("COLUMNS")
    try:
        os.environ["COLUMNS"] = "18"  # force each option onto a separate line
        return parser.format_usage()
    finally:
        if previous_columns is None:
            os.environ.pop("COLUMNS", None)
        else:
            os.environ["COLUMNS"] = previous_columns


#############################################################################
if __name__ == "__main__":
    main()
