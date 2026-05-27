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
"""
Automatically (re)generate README markdown sections directly from argparse parser definitions.

This avoids manually editing the same docs in two places, namely in the argparse.ArgumentParser help configuration
(help=, description=, etc.), and also in a manually edited man page within README.md.

Has zero dependencies beyond the Python standard library.
"""

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


#############################################################################
def _argument_parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(
        description="Automatically (re)generate README markdown sections directly from argparse parser definitions. This "
        "avoids manually editing the same docs in two places, namely in the argparse.ArgumentParser help configuration "
        "(help=, description=, etc.), and also in a manually edited man page within README.md.",
        allow_abbrev=False,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    cli.add_argument(
        "--module",
        required=True,
        metavar="STRING",
        help="Python module containing the parser factory. Example: 'bzfs_main.bzfs'",
    )
    cli.add_argument(
        "--function",
        default="argument_parser",
        metavar="STRING",
        help="Name of the no-argument parser factory function within the Python module. The function must return an "
        "instance of argparse.ArgumentParser. Default is '%(default)s'.",
    )
    cli.add_argument(
        "--readme",
        required=True,
        type=Path,
        metavar="PATH",
        help="Path of README markdown file to update. Example: path/to/README.md",
    )
    return cli


def main() -> None:
    """API for command line clients."""
    args: argparse.Namespace = _argument_parser().parse_args()
    module = importlib.import_module(args.module)
    parser = getattr(module, args.function)()
    assert isinstance(parser, argparse.ArgumentParser)
    readme_path: Path = args.readme

    readme = readme_path.read_text(encoding="utf-8")
    readme = _render_readme(parser, readme)  # do the real work
    readme_path.write_text(readme, encoding="utf-8")
    print("Success.", file=sys.stderr)


def _render_readme(parser: argparse.ArgumentParser, readme: str) -> str:
    """Returns README text with generated sections replaced from argparse."""
    description: str = "\n".join(_render_blocks(parser.description or "")).strip() + "\n\n"
    usage: list[str] = [TRIPLE_BACKTICK + "\n"] + _format_usage(parser).splitlines(keepends=True) + [TRIPLE_BACKTICK + "\n"]
    help_details: str = _render_help_details(parser)

    lines: list[str] = readme.splitlines(keepends=True)
    lines = _replace(lines, "<!-- BEGIN DESCRIPTION SECTION -->", [description], end_tag="<!-- END DESCRIPTION SECTION -->")
    lines = _replace(lines, "<!-- BEGIN HELP OVERVIEW SECTION -->", usage, end_tag="<!-- END HELP OVERVIEW SECTION -->")
    lines = _replace(lines, "<!-- BEGIN HELP DETAIL SECTION -->", [help_details])
    return "".join(lines)


def _replace(lines: list[str], begin_tag: str, replacement: list[str], *, end_tag: str | None = None) -> list[str]:
    """Replaces the lines between begin_tag and end_tag with the given replacement; if end_tag is None it extends to EOF."""
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
    if text.count(TRIPLE_BACKTICK) % 2 != 0:
        raise ValueError(
            f"Malformed argparse help text: Opening {TRIPLE_BACKTICK} fence without a matching closing fence; "
            f"input text: {text!r}"
        )

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


def _render_help_details(parser: argparse.ArgumentParser) -> str:
    """Renders all argparse actions including their help text as anchored Markdown reference entries."""
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
            anchor, title_line = _action_anchor_and_title_line(parser, action)
            results += [f'<div id="{anchor}"></div>', "", title_line, ""]
            if action.help:
                help_text = parser._get_formatter()._expand_help(action)  # noqa: SLF001  # pylint: disable=protected-access
                results += _render_blocks(help_text, list_item=True)
                results.append("")
            if i != len(actions) - 1:
                results += ["<!-- -->", ""]  # Prevent adjacent lists from merging
    return "\n".join(results)


def _action_anchor_and_title_line(parser: argparse.ArgumentParser, action: argparse.Action) -> tuple[str, str]:
    """Returns the README anchor and Markdown rendered title line for one argparse action."""
    formatter = parser._get_formatter()  # noqa: SLF001  # pylint: disable=protected-access
    if not action.option_strings:
        label = formatter._metavar_formatter(action, action.dest)(1)[0]  # noqa: SLF001  # pylint: disable=protected-access
        return label.replace(" ", "_"), f"**{label}**"
    elif action.nargs == 0:
        title_line = ", ".join(f"**{option}**" for option in action.option_strings)
    else:
        args: str = formatter._format_args(action, action.dest.upper())  # noqa: SLF001  # pylint: disable=protected-access
        title_line = ", ".join(f"**{option}** *{args}*" for option in action.option_strings)
    return action.option_strings[0].replace(" ", "_"), title_line


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
