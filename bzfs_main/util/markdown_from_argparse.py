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
Automatically generate or regenerate a markdown README.md manpage from argparse parser definitions.

This avoids manually editing the same doc in two places, namely in the argparse.ArgumentParser help configuration
(help=, description=, etc.), and also in a manually edited manpage within README.md.

Has zero dependencies beyond the Python standard library.
"""

from __future__ import (
    annotations,
)
import argparse
import html
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
    NoReturn,
)

# constants:
_WRAP_COLUMNS: Final[int] = 98
_DEFAULT_GROUPS: Final[frozenset] = frozenset(["positional arguments", "optional arguments", "options"])
TRIPLE_BACKTICK: Final[str] = "```"


def _markdown_file_template(tool_name: str) -> str:
    return f"""# {tool_name}

<!-- BEGIN-MANPAGE-USAGE -->
<!-- END-MANPAGE-USAGE -->

<!-- BEGIN-MANPAGE-DESCRIPTION -->
<!-- END-MANPAGE-DESCRIPTION -->

# Options

<!-- BEGIN-MANPAGE-DETAILS -->
<!-- END-MANPAGE-DETAILS -->
"""


#############################################################################
def _argument_parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(
        allow_abbrev=False,
        formatter_class=argparse.RawTextHelpFormatter,
        description=f"""
Automatically generate or regenerate a markdown README.md manpage from argparse parser definitions. This avoids manually
editing the same doc in two places, namely in the argparse.ArgumentParser help configuration (help=, description=, etc),
and also in a manually edited manpage within README.md.

Has zero dependencies beyond the Python standard library.

Example README.md file:

```markdown
{_markdown_file_template('Example CLI Tool').replace("-MANPAGE-", ".MANPAGE.").rstrip()}
```

Manually replace all occurrences of '.MANPAGE.' with '-MANPAGE-' in the example file above.
Then run this to generate the manpage blurbs and replace the sections between the BEGIN-MANPAGE-* and END-MANPAGE-*
marker pairs within the given markdown file with those blurbs:

```
python3 -m bzfs_main.util.markdown_from_argparse \\
  --readme=README_markdown_from_argparse.md \\
  --module=bzfs_main.util.markdown_from_argparse \\
  --function=_argument_parser
```

Existing file content outside of the marker pair sections is retained as-is. This enables reordering of sections, adding
custom document headers/footers/notes, and other forms of customization.

The [BEGIN|END]-MANPAGE-DESCRIPTION marker pair is optional.
""",
        epilog="""
# Examples

```shell
python3 -m bzfs_main.util.markdown_from_argparse --readme=README.md --module=bzfs_main.bzfs
```
""",
    )

    cli.add_argument(
        "--readme",
        required=True,
        type=Path,
        metavar="PATH",
        help="Path of README markdown file to update. If the file does not exist a template will be generated. "
        "Example: path/to/README.md",
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
        "--heading-level",
        default=1,
        type=int,
        metavar="INT",
        help="Markdown heading level for generated subparser sections. Must be >= 1. Default is '%(default)s'.",
    )
    return cli


def _markdown_template_name(readme_path: Path) -> str:
    s = readme_path.stem  # basename without file extension
    s = re.sub(r"^README[ _.-]?", "", s, flags=re.IGNORECASE)  # remove prefix if present
    s = re.sub(r"[ _.-]man([ _.-]?page)?$", "", s, flags=re.IGNORECASE)  # remove suffix if present
    s = s + " man page"
    return s.lstrip()


def main() -> None:
    """API for command line clients."""
    args: argparse.Namespace = _argument_parser().parse_args()
    module = importlib.import_module(args.module)
    parser = getattr(module, args.function)()
    assert isinstance(parser, argparse.ArgumentParser)
    readme_path: Path = args.readme
    if readme_path.exists():
        readme = readme_path.read_text(encoding="utf-8")
        msg = "updated"
    else:
        readme = _markdown_file_template(_markdown_template_name(readme_path))
        msg = "created"
    readme = _render_readme(parser, readme, heading_level=args.heading_level)
    readme_path.write_text(readme, encoding="utf-8")
    print(f"Successfully {msg} {readme_path}", file=sys.stderr)


def _render_readme(parser: argparse.ArgumentParser, readme: str, *, heading_level: int = 1) -> str:
    """Returns README text with generated sections replaced from argparse."""
    assert heading_level >= 1, heading_level
    usage: list[str] = [TRIPLE_BACKTICK + "\n"] + _format_usage(parser).splitlines(keepends=True) + [TRIPLE_BACKTICK + "\n"]
    help_details: str = _render_help_details(parser, heading_level=heading_level)
    lines: list[str] = readme.splitlines(keepends=True)
    if "<!-- BEGIN-MANPAGE-DESCRIPTION -->" in readme:
        description: str = "\n".join(_render_blocks(parser.description or "")).strip() + "\n"
        lines = _replace(lines, "<!-- BEGIN-MANPAGE-DESCRIPTION -->", [description], "<!-- END-MANPAGE-DESCRIPTION -->")
    lines = _replace(lines, "<!-- BEGIN-MANPAGE-USAGE -->", usage, end_tag="<!-- END-MANPAGE-USAGE -->")
    lines = _replace(lines, "<!-- BEGIN-MANPAGE-DETAILS -->", [help_details], end_tag="<!-- END-MANPAGE-DETAILS -->")
    return "".join(lines)


def _replace(lines: list[str], begin_tag: str, replacement: list[str], end_tag: str) -> list[str]:
    """Replaces the lines between begin_tag and end_tag with the given replacement."""
    begin: int | None = next((i for i, line in enumerate(lines) if begin_tag in line), None)
    if begin is None:
        _die(f"Marker not found: {begin_tag!r}")
    end: int | None = next((i for i, line in enumerate(lines[begin + 1 :], start=begin + 1) if end_tag in line), None)
    if end is None:
        _die(f"Marker not found: {end_tag!r}")
    return lines[: begin + 1] + replacement + lines[end:]


def _render_blocks(text: str, *, list_item: bool = False) -> list[str]:
    """Renders argparse help into the README's supported Markdown subset.

    Assumes prose paragraphs are separated by blank lines and fenced examples use triple backticks. Prose is normalized and
    wrapped; headings and fenced code stay structural. In list_item mode, the first block forms the bullet body and later
    blocks are indented beneath it, avoiding a full Markdown parser while preserving readable generated option help.
    """
    initial_indent: str = "*  " if list_item else ""
    subsequent_indent: str = "    " if list_item else ""
    if text.count(TRIPLE_BACKTICK) % 2 != 0:
        _die(
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


def _render_help_details(parser: argparse.ArgumentParser, *, heading_level: int = 1) -> str:
    """Renders all argparse actions including their help text as anchored Markdown reference entries."""
    return "\n".join(_render_help_details_recursive(parser, heading_level, anchor_prefix=""))


def _render_help_details_recursive(parser: argparse.ArgumentParser, heading_level: int, *, anchor_prefix: str) -> list[str]:
    """Includes recursive descent into nested subparsers."""
    all_results: list[str] = []
    formatter: argparse.HelpFormatter = parser._get_formatter()  # noqa: SLF001  # pylint: disable=protected-access
    mutually_exclusive_notes: dict[int, str] = _mutually_exclusive_group_notes(parser, formatter)
    for group in parser._action_groups:  # noqa: SLF001  # pylint: disable=protected-access  # no public iterator
        actions: list[argparse.Action] = _visible_group_actions(parser, group)
        results: list[str] = []
        for i, action in enumerate(actions):
            subparser_details: list[str] = []
            if note := mutually_exclusive_notes.get(id(action)):
                results += _render_blocks(note) + [""]
            if not isinstance(action, argparse._SubParsersAction):  # noqa: SLF001  # pylint: disable=protected-access
                anchor, title_line = _action_anchor_and_title_line(parser, action, anchor_prefix=anchor_prefix)
                results += [f'<div id="{html.escape(anchor, quote=True)}"></div>', "", title_line, ""]
                is_list_item = True
            else:
                is_list_item = False
                for name, title, subparser, subaction in _visible_subparser_actions(action):
                    subparser_details += [f"{'#' * heading_level} {_escape_md(title)}", ""]
                    if subparser.description and subparser.description != argparse.SUPPRESS:
                        subparser_details += _render_blocks(subparser.description) + [""]
                    elif subaction is not None and subaction.help:
                        help_text = formatter._expand_help(subaction)  # noqa: SLF001  # pylint: disable=protected-access
                        subparser_details += _render_blocks(help_text) + [""]
                    subparser_details += [TRIPLE_BACKTICK] + _format_usage(subparser).splitlines() + [TRIPLE_BACKTICK, ""]
                    subparser_details += _render_help_details_recursive(
                        subparser, heading_level + 1, anchor_prefix=f"{anchor_prefix}{name}~"
                    )
                if len(subparser_details) == 0:
                    continue

            if action.help:
                help_text = formatter._expand_help(action)  # noqa: SLF001  # pylint: disable=protected-access
                results += _render_blocks(help_text, list_item=is_list_item) + [""]
            results += subparser_details
            if i != len(actions) - 1:
                results += ["<!-- -->", ""]  # Prevent adjacent lists from merging

        if len(results) > 0 and group.title and group.title not in _DEFAULT_GROUPS:
            all_results += [f"{'#' * heading_level} {_escape_md(group.title.upper())}", ""]
            if group.description and group.description != argparse.SUPPRESS:
                all_results += _render_blocks(group.description) + [""]
        all_results += results
    if parser.epilog and parser.epilog != argparse.SUPPRESS:
        all_results += _render_blocks(parser.epilog) + [""]
    return all_results


def _mutually_exclusive_group_notes(parser: argparse.ArgumentParser, fmt: argparse.HelpFormatter) -> dict[int, str]:
    """Returns notes keyed by the first visible action in each mutually exclusive group."""
    notes: dict[int, str] = {}
    for group in parser._mutually_exclusive_groups:  # noqa: SLF001  # pylint: disable=protected-access  # no public iterator
        actions: list[argparse.Action] = _visible_group_actions(parser, group)
        if len(actions) >= 2:
            quantifier = "exactly one" if group.required else "at most one"
            choice_labels: list[str] = []
            for action in actions:
                if action.option_strings:
                    choice_labels.append(" / ".join(f"**{_escape_md(option)}**" for option in action.option_strings))
                else:
                    met = fmt._get_default_metavar_for_positional(action)  # noqa: SLF001  # pylint: disable=protected-access
                    metavars = fmt._metavar_formatter(action, met)(1)  # noqa: SLF001  # pylint: disable=protected-access
                    label = " ".join(metavars)
                    choice_labels.append(f"**{_escape_md(label)}**")
            notes[id(actions[0])] = f"Mutually exclusive group: choose {quantifier} of {', '.join(choice_labels)}."
    return notes


def _visible_group_actions(
    parser: argparse.ArgumentParser, group: argparse._ArgumentGroup  # pylint: disable=protected-access
) -> list[argparse.Action]:
    """Returns documented actions from one argparse action group."""
    results: list[argparse.Action] = []
    prefix: str = "-" if "-" in parser.prefix_chars else parser.prefix_chars[0]
    help_option_strings: tuple[str, str] = (f"{prefix}h", f"{prefix}{prefix}help")
    for action in group._group_actions:  # noqa: SLF001  # pylint: disable=protected-access  # no public iterator
        if action.help != argparse.SUPPRESS and not (
            isinstance(action, argparse._HelpAction)  # noqa: SLF001  # pylint: disable=protected-access
            and action.dest == "help"
            and tuple(action.option_strings) == help_option_strings
        ):
            results.append(action)
    return results


def _visible_subparser_actions(
    action: argparse._SubParsersAction,  # pylint: disable=protected-access
) -> list[tuple[str, str, argparse.ArgumentParser, argparse.Action | None]]:
    subactions: dict[str, argparse.Action] = {
        subact.dest: subact for subact in action._get_subactions()  # noqa: SLF001  # pylint: disable=protected-access
    }
    results: list[tuple[str, str, argparse.ArgumentParser, argparse.Action | None]] = []
    seen: set[int] = set()
    for name, subparser in action.choices.items():
        if id(subparser) not in seen:
            seen.add(id(subparser))
            subaction = subactions.get(name)
            if subaction is None or subaction.help != argparse.SUPPRESS:
                if subaction is not None and subaction.metavar:
                    title = str(subaction.metavar)
                else:
                    aliases = [
                        choice_name
                        for choice_name, choice_parser in action.choices.items()
                        if choice_parser is subparser and choice_name != name
                    ]
                    title = name if len(aliases) == 0 else f"{name} ({', '.join(aliases)})"
                results.append((name, title, subparser, subaction))
    return results


def _action_anchor_and_title_line(
    parser: argparse.ArgumentParser, action: argparse.Action, *, anchor_prefix: str = ""
) -> tuple[str, str]:
    """Returns the README anchor and Markdown rendered title line for one argparse action."""
    formatter: argparse.HelpFormatter = parser._get_formatter()  # noqa: SLF001  # pylint: disable=protected-access
    if not action.option_strings:
        dflt = formatter._get_default_metavar_for_positional(action)  # noqa: SLF001  # pylint: disable=protected-access
        positional_args = formatter._format_args(action, dflt)  # noqa: SLF001  # pylint: disable=protected-access
        return anchor_prefix + action.dest.replace(" ", "_"), f"**{_escape_md(positional_args)}**"
    elif action.nargs == 0:
        title_line = ", ".join(f"**{_escape_md(option)}**" for option in action.option_strings)
    else:
        dflt = formatter._get_default_metavar_for_optional(action)  # noqa: SLF001  # pylint: disable=protected-access
        option_args = formatter._format_args(action, dflt)  # noqa: SLF001  # pylint: disable=protected-access
        title_line = ", ".join(f"**{_escape_md(option)}** *{_escape_md(option_args)}*" for option in action.option_strings)
    if action.required:
        title_line += " _(required)_"
    return anchor_prefix + action.option_strings[0].replace(" ", "_"), title_line


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


def _escape_md(text: str) -> str:
    """Escapes generated parser metadata for Markdown inline contexts."""
    return re.sub(r"([\\`*])", r"\\\1", html.escape(text, quote=False))


def _format_usage(parser: argparse.ArgumentParser) -> str:
    def _restore(key: str, previous_value: str | None) -> None:
        if previous_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = previous_value

    previous_columns: str | None = os.environ.get("COLUMNS")
    previous_colors: str | None = os.environ.get("PYTHON_COLORS")
    try:
        os.environ["COLUMNS"] = "18"  # force each option onto a separate line
        os.environ["PYTHON_COLORS"] = "0"  # don't add color codes to generated man pages
        return parser.format_usage()
    finally:
        _restore("COLUMNS", previous_columns)
        _restore("PYTHON_COLORS", previous_colors)


def _die(msg: str) -> NoReturn:
    raise SystemExit(f"ERROR: {msg}")


#############################################################################
if __name__ == "__main__":
    main()
