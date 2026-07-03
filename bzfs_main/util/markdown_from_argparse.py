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
import urllib.parse
from argparse import (
    ArgumentParser,
)
from pathlib import (
    Path,
)
from typing import (
    Final,
    NoReturn,
    final,
)

# constants:
_WRAP_TEXT_WIDTH: Final[int] = 98
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
def _argument_parser() -> ArgumentParser:
    cli = ArgumentParser(
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

Subparser sections are rendered recursively. Subparsers can be nested arbitrarily.

Generated CLI option entries include explicit HTML `id` anchors and inline permalinks so users can refer to them via copy
and paste. Subparser headings also include explicit HTML `id` anchors.

The renderer expects argparse parser `description`, `epilog`, and `help=` text in the form of blank-line-separated blocks,
where the first block of each `help=` text is prose.

Supported Markdown input contract:

- Blank lines separate blocks. In action help, the first block becomes the generated option bullet text; later blocks are
  indented under it.
- Ordinary prose blocks are rewrapped.
- Single-line Markdown headings (`#` through `######`) are preserved.
- Markdown pipe tables are preserved when the first row contains `|` and the second row is a separator row.
- Homogeneous Markdown list blocks are preserved when the first line starts an ordered or unordered list and following
  nonblank lines are list items or indented continuations.
- Homogeneous multiline blockquote blocks are preserved when every nonblank line starts with `>`.
- Triple-backtick fenced code blocks are preserved, including blank lines inside the fence. Opening and closing fences
  must be balanced.
- Blocks indented with four spaces or one tab are fenced as code.
- Blocks with repeated aligned columns are fenced as code.

Ambiguous layouts are treated as prose and may be rewrapped. Use explicit triple-backtick fences for shell sessions,
configuration, YAML, JSON, mixed prose and examples, or any layout that must survive unchanged.
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
        help="Markdown heading level for generated group/subparser sections. Must be >= 1. Default is '%(default)s'.",
    )
    return cli


def _markdown_template_name(readme_path: Path) -> str:
    s: str = readme_path.stem  # basename without file extension
    s = re.sub(r"^README[ _.-]?", "", s, flags=re.IGNORECASE)  # remove prefix if present
    s = re.sub(r"[ _.-]man([ _.-]?page)?$", "", s, flags=re.IGNORECASE)  # remove suffix if present
    s = s + " man page"
    return s.lstrip()


def main() -> None:
    """API for command line clients."""
    args: argparse.Namespace = _argument_parser().parse_args()
    module = importlib.import_module(args.module)
    parser = getattr(module, args.function)()
    assert isinstance(parser, ArgumentParser)
    readme_path: Path = args.readme
    if readme_path.exists():
        readme: str = readme_path.read_text(encoding="utf-8")
        msg = "updated"
    else:
        readme = _markdown_file_template(_markdown_template_name(readme_path))
        msg = "created"
    renderer: MarkdownFromArgparse = MarkdownFromArgparse(parser=parser, readme=readme, args=args)
    readme = renderer.render_readme()
    readme_path.write_text(readme, encoding="utf-8")
    print(f"Successfully {msg} {readme_path}", file=sys.stderr)


#############################################################################
@final
class MarkdownFromArgparse:
    """API for Python clients."""

    def __init__(self, parser: ArgumentParser, readme: str, args: argparse.Namespace) -> None:
        self.parser: Final[ArgumentParser] = parser
        self.readme: Final[str] = readme
        self.args: Final[argparse.Namespace] = args
        self.heading_level: Final[int] = args.heading_level
        assert self.heading_level >= 1, self.heading_level

    def render_readme(self) -> str:
        """Returns README text with generated sections replaced from argparse."""
        usage: list[str] = (
            [TRIPLE_BACKTICK + "\n"] + self._format_usage(self.parser).splitlines(keepends=True) + [TRIPLE_BACKTICK + "\n"]
        )
        details: str = self._render_help_details()
        lines: list[str] = self.readme.splitlines(keepends=True)
        if "<!-- BEGIN-MANPAGE-DESCRIPTION -->" in self.readme:
            descr: str = "\n".join(self._render_blocks(self.parser.description or "")).strip() + "\n"
            lines = self._replace(lines, "<!-- BEGIN-MANPAGE-DESCRIPTION -->", [descr], "<!-- END-MANPAGE-DESCRIPTION -->")
        lines = self._replace(lines, "<!-- BEGIN-MANPAGE-USAGE -->", usage, end_tag="<!-- END-MANPAGE-USAGE -->")
        lines = self._replace(lines, "<!-- BEGIN-MANPAGE-DETAILS -->", [details], end_tag="<!-- END-MANPAGE-DETAILS -->")
        return "".join(lines)

    def _replace(self, lines: list[str], begin_tag: str, replacement: list[str], end_tag: str) -> list[str]:
        """Replaces the lines between begin_tag and end_tag with the given replacement."""
        begin: int | None = next((i for i, line in enumerate(lines) if begin_tag in line), None)
        if begin is None:
            self._die(f"Marker not found: {begin_tag!r}")
        end: int | None = next((i for i, line in enumerate(lines[begin + 1 :], start=begin + 1) if end_tag in line), None)
        if end is None:
            self._die(f"Marker not found: {end_tag!r}")
        return lines[: begin + 1] + replacement + lines[end:]

    def _render_blocks(self, text: str, *, is_list: bool = False) -> list[str]:
        """Renders argparse help into the README's supported Markdown subset.

        In is_list mode, argparse action help is expected to start with prose as the bullet body, separated from later blocks
        by a blank line, with those later blocks indented beneath it. This is a small block renderer, not a full Markdown
        parser; mixed prose plus verbatim text in one physical block is ambiguous, and structured first-block action help is
        outside the intended contract.
        """
        first_indent: str = "- " if is_list else ""
        later_indent: str = "  " if is_list else ""
        if text.count(TRIPLE_BACKTICK) % 2 != 0:
            self._die(
                f"Malformed argparse help text: Opening {TRIPLE_BACKTICK} fence without a matching closing fence; "
                f"input text: {text!r}"
            )

        # Split prose around fenced code so blank lines inside examples survive.
        blocks: list[str] = []
        for i, part in enumerate(text.strip("\n").split(TRIPLE_BACKTICK)):
            if i % 2 != 0:
                blocks.append(TRIPLE_BACKTICK + part + TRIPLE_BACKTICK)
            else:  # blocks are separated by a blank line
                blocks += [block for block in re.split(r"\n\s*\n", part) if block.strip()]

        results: list[str] = []
        for i, block in enumerate(blocks):
            if i > 0:
                results.append("")
            indent: str = first_indent if i == 0 else later_indent
            if block.startswith(TRIPLE_BACKTICK) and block.endswith(TRIPLE_BACKTICK):
                results += [""] + [indent + line for line in block.splitlines()] + [""]
            elif self._is_markdown_block(block):
                results += [indent + line for line in block.splitlines()]
            elif self._is_verbatim_block(block):
                results += (
                    [""] + [indent + line for line in [TRIPLE_BACKTICK] + block.splitlines() + [TRIPLE_BACKTICK]] + [""]
                )
            else:
                results += self._wrap_text(" ".join(block.split()), first_indent=indent, later_indent=later_indent)
        return results

    def _is_markdown_block(self, block: str) -> bool:
        """Returns True when wrapping would break explicit Markdown structure.

        This is a small block-level recognizer. Its job is not to understand all Markdown; it only answers: "Would normal
        prose wrapping destroy an explicit Markdown structure here?"

        The deeper principle is conservative explicitness: preserve syntax that is obviously Markdown, but do not infer
        ambiguous things like shell commands, config formats, YAML, HTTP examples, or project-specific conventions. For those
        the author should specify explicit fenced code blocks. This keeps the renderer simple and avoids content-specific
        workarounds.
        """

        # First, split the block into nonblank physical lines. This removes blank lines and trailing whitespace, but
        # preserves leading indentation. Preserving leading indentation matters for nested lists.
        lines: list[str] = [line.rstrip() for line in block.splitlines() if line.strip()]

        # 0. Single-line markdown headings are explicit Markdown syntax and should not be wrapped, for example:
        #    # Heading
        #    ### Heading
        markdown_heading = r"#{1,6} .+"
        if len(lines) == 1 and re.fullmatch(markdown_heading, lines[0]):
            return True

        # 1. Empty and one-line non-heading blocks are not Markdown structures under these rules. They are ordinary prose for
        # the caller to wrap.
        if len(lines) < 2:
            return False

        # 2. Markdown pipe tables. This recognizes the canonical Markdown table shape:
        #
        #    | Name | Meaning |
        #    | --- | --- |
        #    | A | B |
        #
        # The first line must look like a table header by containing "|". The second line must be a separator row with at
        # least two columns, each made from at least three dashes, with optional alignment colons.
        #
        # Principle: once a block starts as a Markdown table, trust the author and preserve the whole block line-for-line.
        # Wrapping table rows would destroy the table.
        table_separator = r"\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*"
        if "|" in lines[0] and re.fullmatch(table_separator, lines[1]):
            return True

        # 3. List blocks. The first line must declare the list; later indented lines may continue the current item.
        #
        #    - unordered item
        #    * unordered item
        #    + unordered item
        #    1. ordered item
        #
        # Leading indentation is allowed, so nested lists work:
        #
        #    - Parent
        #      - Child
        list_line = r"\s*([-*+]|\d+\.)\s+\S.*"
        continuation_line = r"\s+\S.*"
        if re.fullmatch(list_line, lines[0]):
            return all(re.fullmatch(list_line, line) or re.fullmatch(continuation_line, line) for line in lines[1:])

        # 4. Homogeneous blockquote blocks. If prose and blockquote syntax are mixed in the same block, do not guess;
        # the author should separate them with a blank line or use explicit fences.
        #
        #    > blockquote
        blockquote_line = r"\s*>\s?.*"
        return all(re.fullmatch(blockquote_line, line) for line in lines)

    def _is_verbatim_block(self, block: str) -> bool:
        """Returns True when physical line layout should be fenced instead of wrapped.

        The recognizer stays layout-based: specifically indented lines are manual code blocks, and repeated wide internal
        whitespace suggests aligned text columns.
        """
        # Split the block into nonblank physical lines.
        lines = [line.rstrip() for line in block.splitlines() if line.strip()]

        # Manual code block: line indented by at least 4 spaces (or 1 tab)
        if any(line.startswith(("    ", "\t")) for line in lines):
            return True

        # Manual text table aligned with whitespace: checks whether at least two lines contain a non-space character that
        # is not common sentence punctuation, followed by two or more whitespace characters, then another non-space char.
        has_aligned_columns: bool = sum(bool(re.search(r"[^\s.!?:;]\s{2,}\S", line)) for line in lines) >= 2
        return has_aligned_columns

    def _render_help_details(self) -> str:
        """Renders all argparse actions including their help text as anchored Markdown reference entries."""
        return "\n".join(self._render_help_details_recursive(self.parser, self.heading_level, anchor_prefix=""))

    def _render_help_details_recursive(self, parser: ArgumentParser, heading_level: int, *, anchor_prefix: str) -> list[str]:
        """Includes recursive descent into nested subparsers."""
        list_separator: list[str] = ["<!-- -->", ""]  # prevent adjacent Markdown lists from merging
        all_results: list[str] = []
        sub_results: list[str] = []  # subparser command details
        formatter: argparse.HelpFormatter = parser._get_formatter()  # noqa: SLF001  # pylint: disable=protected-access
        mutually_exclusive_notes: dict[int, str] = self._mutually_exclusive_group_notes(parser, formatter)
        for group in parser._action_groups:  # noqa: SLF001  # pylint: disable=protected-access  # no public iterator
            group_results: list[str] = []
            for action in self._visible_group_actions(group):
                gists: list[str] = []  # subparser command overview list
                if note := mutually_exclusive_notes.get(id(action)):
                    group_results += self._render_blocks(note) + [""]
                if not isinstance(action, argparse._SubParsersAction):  # noqa: SLF001  # pylint: disable=protected-access
                    anchor, title_line = self._action_anchor_and_title_line(parser, action, anchor_prefix=anchor_prefix)
                    group_results += [f"{_html_option_title(anchor, title_line)} {_html_permalink(anchor)}", ""]
                    is_list = True
                else:
                    is_list = False
                    visible_subparser_actions: list[tuple] = self._visible_subparser_actions(action)
                    if len(visible_subparser_actions) == 0:
                        continue
                    for name, title, subparser, subaction in visible_subparser_actions:  # generate cmd overview + details
                        sub_anchor_prefix = f"{anchor_prefix}{name}~"
                        prefix: str = _link(_bold(_escape_md(title)), sub_anchor_prefix)
                        gist: list[str] = []
                        if subaction is not None and subaction.help:
                            gist = self._render_blocks(f"{prefix}: {self._expand_help(subaction, formatter)}", is_list=True)
                        gists += gist if len(gist) > 0 else [f"- {prefix}"]
                        sub_results += [_heading(heading_level, _escape_md(title), anchor=sub_anchor_prefix), ""]
                        if subparser.description and subparser.description != argparse.SUPPRESS:
                            sub_results += self._render_blocks(subparser.description) + [""]
                        elif subaction is not None and subaction.help:
                            sub_results += self._render_blocks(self._expand_help(subaction, formatter)) + [""]
                        sub_results += [TRIPLE_BACKTICK] + self._format_usage(subparser).splitlines() + [TRIPLE_BACKTICK, ""]
                        sub_results += self._render_help_details_recursive(  # recurse into nested subparser
                            subparser, heading_level + 1, anchor_prefix=sub_anchor_prefix
                        )
                    gists += [""]

                if action.help:
                    group_results += self._render_blocks(self._expand_help(action, formatter), is_list=is_list) + [""]
                    group_results += list_separator if len(gists) > 0 else []
                group_results += gists + list_separator

            skip_builtin_group_titles: set[str] = {"positional arguments", "optional arguments", "options"}
            if len(group_results) > 0 and group.title and group.title not in skip_builtin_group_titles:
                all_results += [_heading(heading_level, _escape_md(group.title), css_class="man-group-heading"), ""]
                if group.description and group.description != argparse.SUPPRESS:
                    all_results += self._render_blocks(group.description) + [""] + list_separator
            all_results += group_results
        if parser.epilog and parser.epilog != argparse.SUPPRESS:
            all_results += self._render_blocks(parser.epilog) + [""]
        all_results += sub_results
        return all_results

    def _mutually_exclusive_group_notes(self, parser: ArgumentParser, fmt: argparse.HelpFormatter) -> dict[int, str]:
        """Returns notes keyed by the first visible action in each mutually exclusive group."""
        notes: dict[int, str] = {}
        for group in parser._mutually_exclusive_groups:  # noqa: SLF001  # pylint: disable=protected-access
            actions: list[argparse.Action] = self._visible_group_actions(group)
            if len(actions) >= 2:
                quantifier: str = "exactly one" if group.required else "at most one"
                choice_labels: list[str] = []
                for action in actions:
                    if action.option_strings:
                        choice_labels.append(" / ".join(_bold(_escape_md(option)) for option in action.option_strings))
                    else:
                        # pylint: disable-next=protected-access
                        met: str = fmt._get_default_metavar_for_positional(action)  # noqa: SLF001
                        metavars = fmt._metavar_formatter(action, met)(1)  # noqa: SLF001  # pylint: disable=protected-access
                        label = " ".join(metavars)
                        choice_labels.append(_bold(_escape_md(label)))
                notes[id(actions[0])] = f"Mutually exclusive group: choose {quantifier} of {', '.join(choice_labels)}."
        return notes

    def _visible_group_actions(
        self, group: argparse._ArgumentGroup  # pylint: disable=protected-access
    ) -> list[argparse.Action]:
        """Returns documented actions from one argparse action group."""
        group_actions: list[argparse.Action] = group._group_actions  # noqa: SLF001  # pylint: disable=protected-access
        return [action for action in group_actions if action.help != argparse.SUPPRESS]

    def _visible_subparser_actions(
        self, action: argparse._SubParsersAction  # pylint: disable=protected-access
    ) -> list[tuple[str, str, ArgumentParser, argparse.Action | None]]:
        subactions: dict[str, argparse.Action] = {
            subact.dest: subact for subact in action._get_subactions()  # noqa: SLF001  # pylint: disable=protected-access
        }
        results: list[tuple[str, str, ArgumentParser, argparse.Action | None]] = []
        seen: set[int] = set()
        for name, subparser in action.choices.items():
            if id(subparser) not in seen:
                seen.add(id(subparser))
                subaction = subactions.get(name)
                if subaction is None or subaction.help != argparse.SUPPRESS:
                    if subaction is not None and subaction.metavar:
                        title = str(subaction.metavar)
                    else:
                        aliases: list[str] = [
                            choice_name
                            for choice_name, choice_parser in action.choices.items()
                            if choice_parser is subparser and choice_name != name
                        ]
                        title = name if len(aliases) == 0 else f"{name} ({', '.join(aliases)})"
                    results.append((name, title, subparser, subaction))
        return results

    def _action_anchor_and_title_line(
        self, parser: ArgumentParser, action: argparse.Action, *, anchor_prefix: str = ""
    ) -> tuple[str, str]:
        """Returns the README anchor and Markdown rendered title line for one argparse action."""
        formatter: argparse.HelpFormatter = parser._get_formatter()  # noqa: SLF001  # pylint: disable=protected-access
        if not action.option_strings:
            dflt = formatter._get_default_metavar_for_positional(action)  # noqa: SLF001  # pylint: disable=protected-access
            positional_args: str = formatter._format_args(action, dflt)  # noqa: SLF001  # pylint: disable=protected-access
            if action.nargs == argparse.REMAINDER and positional_args == "...":
                positional_args = str(action.metavar or dflt)
            return anchor_prefix + action.dest.replace(" ", "_"), _bold(_escape_md(positional_args))
        elif action.nargs == 0:
            title_line: str = ", ".join(_bold(_escape_md(opt)) for opt in action.option_strings)
        else:
            dflt = formatter._get_default_metavar_for_optional(action)  # noqa: SLF001  # pylint: disable=protected-access
            option_args: str = formatter._format_args(action, dflt)  # noqa: SLF001  # pylint: disable=protected-access
            title_line = ", ".join(f"{_bold(_escape_md(opt))} *{_escape_md(option_args)}*" for opt in action.option_strings)
        if action.required:
            title_line += " _(required)_"
        return anchor_prefix + action.option_strings[0].replace(" ", "_"), title_line

    def _wrap_text(self, text: str, *, first_indent: str = "", later_indent: str = "") -> list[str]:
        """Wraps prose without splitting words, option names, or paths."""
        lines: list[str] = textwrap.wrap(
            text,
            width=_WRAP_TEXT_WIDTH,
            initial_indent=first_indent,
            subsequent_indent=later_indent,
            break_long_words=False,
            break_on_hyphens=False,
        )
        for i in range(1, len(lines)):
            # Escape list markers that wrapping moved to column zero, where Markdown would otherwise start a nested list.
            continuation: str = lines[i][len(later_indent) :]
            continuation = re.sub(r"^([-*+]\s)", r"\\\1", continuation)  # "- item" -> "\\- item"
            continuation = re.sub(r"^(\d+)([.)])(\s)", r"\1\\\2\3", continuation)  # "1. x" -> "1\\. x", "1) x" -> "1\\) x"
            lines[i] = later_indent + continuation
        return lines

    def _expand_help(self, action: argparse.Action, formatter: argparse.HelpFormatter) -> str:
        help_text: str = formatter._expand_help(action)  # noqa: SLF001  # pylint: disable=protected-access
        return help_text

    def _format_usage(self, parser: ArgumentParser) -> str:
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
            usage: str = parser.format_usage()
            return usage
        finally:
            _restore("COLUMNS", previous_columns)
            _restore("PYTHON_COLORS", previous_colors)

    def _die(self, msg: str) -> NoReturn:
        raise SystemExit(f"ERROR: {msg}")


def _bold(text: str) -> str:
    """Returns strongly emphasized Markdown."""
    return f"**{text}**"


def _heading(heading_level: int, text: str, *, anchor: str = "", css_class: str = "man-subparser-heading") -> str:
    """Returns a Markdown heading whose title text can be styled by CSS."""
    id_attr = f' id="{html.escape(anchor, quote=True)}"' if anchor else ""
    return f'{"#" * heading_level} <span{id_attr} class="{css_class}">{text}</span>'


def _link(text: str, fragment: str) -> str:
    """Returns an inline Markdown link that points to a URL-encoded fragment."""
    text = text.replace("[", "\\[").replace("]", "\\]")  # escape Markdown link-label delimiters
    fragment = urllib.parse.quote(fragment, safe="-._~")  # quote chars that are unsafe in a URL fragment
    return f"[{text}](#{fragment})"


def _html_option_title(anchor: str, title_line: str) -> str:
    """Returns an inline wrapper for a rendered argparse action title; can be styled by CSS."""
    anchor = html.escape(anchor, quote=True)
    return f'<span id="{anchor}" class="man-option-title">{title_line}</span>'


def _html_permalink(anchor: str) -> str:
    """Returns an inline self-link that the user can copy and paste to refer to the section identified by the anchor."""
    label = html.escape(f"Permalink to {anchor}", quote=True)
    fragment = urllib.parse.quote(anchor, safe="-._~")
    return f'<a href="#{fragment}" title="{label}" aria-label="{label}" class="man-option-permalink">&#x1F517;</a>'


def _escape_md(text: str) -> str:
    """Escapes generated parser metadata for Markdown inline contexts."""
    return re.sub(r"([\\`*])", r"\\\1", html.escape(text, quote=False))


#############################################################################
if __name__ == "__main__":
    main()
