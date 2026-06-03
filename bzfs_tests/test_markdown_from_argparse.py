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
"""Unit tests for README generation from argparse parser definitions."""

from __future__ import (
    annotations,
)
import argparse
import importlib
import os
import runpy
import sys
import unittest
from io import (
    StringIO,
)
from pathlib import (
    Path,
)
from tempfile import (
    TemporaryDirectory,
)
from unittest.mock import (
    patch,
)

from bzfs_main import (
    argparse_actions,
    bzfs_jobrunner,
)
from bzfs_main.util import (
    markdown_from_argparse,
)
from bzfs_tests.abstract_testcase import (
    AbstractTestCase,
)
from bzfs_tests.tools import (
    capture_stderr,
    capture_stdout,
)


###############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestUpdateReadme,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


###############################################################################
class TestUpdateReadme(AbstractTestCase):
    """Checks README help rendering for argparse features used by bzfs CLIs."""

    def test_markdown_template_name(self) -> None:
        def template_name(name: str) -> str:
            return markdown_from_argparse._markdown_template_name(Path(name))

        self.assertEqual("man page", template_name(""))
        self.assertEqual("man page", template_name("README"))
        self.assertEqual("man page", template_name("README.md"))
        self.assertEqual("foo man page", template_name("foo"))
        self.assertEqual("bzfs man page", template_name("bzfs"))
        self.assertEqual("bzfs man page", template_name("bzfs.md"))
        self.assertEqual("bzfs man page", template_name("README_bzfs.md"))
        self.assertEqual("bzfs man page", template_name("README_bzfs.md"))
        self.assertEqual("bzfs man page", template_name("README.bzfs.md"))
        self.assertEqual("bzfs man page", template_name("README_bzfs_man.md"))
        self.assertEqual("bzfs man page", template_name("README_bzfs_manpage.md"))
        self.assertEqual("bzfs man page", template_name("README_bzfs_ManPage.md"))
        self.assertEqual("bzfs man page", template_name("readme_bzfs_man_page.md"))
        self.assertEqual("bzfs man page", template_name("readme_bzfs.man_page.md"))
        self.assertEqual("bzfs_jobrunner man page", template_name("README_bzfs_jobrunner.md"))
        self.assertEqual("bzfs_jobrunner man page", template_name("README.bzfs_jobrunner.md"))
        self.assertEqual("postman man page", template_name("postman.md"))
        self.assertEqual("postman man page", template_name("README_postman.md"))
        self.assertEqual("postman man page", template_name("README.postman.md"))

    def test_replace_rejects_missing_begin_tag(self) -> None:
        """Covers missing generated-section start markers."""
        lines = ["prefix\n", "<!-- END -->\n"]

        with self.assertRaises(SystemExit) as cm:
            markdown_from_argparse._replace(lines, "<!-- BEGIN -->", ["replacement\n"], "<!-- END -->")

        self.assertEqual("ERROR: Marker not found: '<!-- BEGIN -->'", str(cm.exception))

    def test_replace_rejects_missing_end_tag(self) -> None:
        """Covers unterminated generated sections after a start marker."""
        lines = ["prefix\n", "<!-- BEGIN -->\n", "old generated text\n"]

        with self.assertRaises(SystemExit) as cm:
            markdown_from_argparse._replace(lines, "<!-- BEGIN -->", ["replacement\n"], "<!-- END -->")

        self.assertEqual("ERROR: Marker not found: '<!-- END -->'", str(cm.exception))

    def test_common_argparse_features_are_rendered_from_parser_model(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument("src", metavar="SRC", help="Source dataset.")
        group = parser.add_argument_group("Transfer Options", description="Controls transfer behavior.")
        group.add_argument("--mode", choices=["fast", "safe"], default="safe", help="Choose mode. Default: %(default)s.")
        group.add_argument("--item", nargs="+", metavar="NAME", help="Name to process.")
        group.add_argument("--hidden", help=argparse.SUPPRESS)

        details = markdown_from_argparse._render_help_details(parser)

        self.assertIn('<div id="src"></div>', details)
        self.assertIn("**SRC**", details)
        self.assertNotIn('<div id="-h"></div>', details)
        self.assertIn("# TRANSFER OPTIONS", details)
        self.assertIn("Controls transfer behavior.", details)
        self.assertIn("**--mode** *{fast,safe}*", details)
        self.assertIn("Choose mode. Default: safe.", details)
        self.assertIn("**--item** *NAME [NAME ...]*", details)
        self.assertNotIn("--hidden", details)

    def test_required_options_are_marked_in_details(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument("src", metavar="SRC", help="Source dataset.")
        parser.add_argument("--required", required=True, metavar="VALUE", help="Required option.")
        parser.add_argument("--optional", metavar="VALUE", help="Optional option.")
        commands = parser.add_subparsers(dest="command", title="Commands")
        sync = commands.add_parser("sync", help="Sync snapshots.", formatter_class=argparse.RawTextHelpFormatter)
        sync.add_argument("--job-id", required=True, metavar="STRING", help="Job id.")

        details = markdown_from_argparse._render_help_details(parser)

        self.assertIn("**SRC**", details)
        self.assertIn("**--required** *VALUE* _(required)_", details)
        self.assertIn("**--optional** *VALUE*", details)
        self.assertIn("**--job-id** *STRING* _(required)_", details)
        self.assertNotIn("**SRC** _(required)_", details)
        self.assertNotIn("**--optional** *VALUE* _(required)_", details)

    def test_positional_nargs_are_rendered_in_detail_titles(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument("optional_item", nargs="?", metavar="OPTIONAL_ITEM", help="Optional item.")
        parser.add_argument("many_items", nargs="*", metavar="MANY_ITEM", help="Many items.")
        parser.add_argument("required_items", nargs="+", metavar="REQUIRED_ITEM", help="Required items.")
        parser.add_argument("remainder", nargs=argparse.REMAINDER, metavar="ARG", help="Remaining arguments.")
        parser.add_argument("pair", nargs=2, metavar=("SRC", "DST"), help="Source and destination pair.")

        details = markdown_from_argparse._render_help_details(parser)

        self.assertIn('<div id="optional_item"></div>', details)
        self.assertIn("**[OPTIONAL_ITEM]**", details)
        self.assertIn('<div id="many_items"></div>', details)
        self.assertIn("**[MANY_ITEM ...]**", details)
        self.assertIn('<div id="required_items"></div>', details)
        self.assertIn("**REQUIRED_ITEM [REQUIRED_ITEM ...]**", details)
        self.assertIn('<div id="remainder"></div>', details)
        self.assertIn("**...**", details)
        self.assertIn('<div id="pair"></div>', details)
        self.assertIn("**SRC DST**", details)

    def test_metavar_type_formatter_is_used_for_detail_titles(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.MetavarTypeHelpFormatter)
        parser.add_argument("count", type=int, help="Number of items.")
        parser.add_argument("--limit", type=int, help="Limit rows.")

        details = markdown_from_argparse._render_help_details(parser)

        self.assertIn('<div id="count"></div>', details)
        self.assertIn("**int**", details)
        self.assertIn("**--limit** *int*", details)
        self.assertNotIn("**COUNT**", details)
        self.assertNotIn("**--limit** *LIMIT*", details)

    def test_mutually_exclusive_groups_are_rendered_before_member_actions(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        mode = parser.add_mutually_exclusive_group(required=True)
        mode.add_argument("--json", action="store_true", help="Emit JSON.")
        mode.add_argument("--text", action="store_true", help="Emit text.")

        output_group = parser.add_argument_group("Output Options")
        output = output_group.add_mutually_exclusive_group()
        output.add_argument("--table", action="store_true", help="Emit table.")
        output.add_argument("--csv", action="store_true", help="Emit CSV.")

        partially_hidden = parser.add_mutually_exclusive_group()
        partially_hidden.add_argument("--visible", action="store_true", help="Visible alternative.")
        partially_hidden.add_argument("--hidden", action="store_true", help=argparse.SUPPRESS)

        commands = parser.add_subparsers(dest="command", title="Commands")
        sync = commands.add_parser("sync", help="Sync snapshots.", formatter_class=argparse.RawTextHelpFormatter)
        sync_mode = sync.add_mutually_exclusive_group(required=True)
        sync_mode.add_argument("--full", action="store_true", help="Full replication.")
        sync_mode.add_argument("--incremental", action="store_true", help="Incremental replication.")

        details = markdown_from_argparse._render_help_details(parser, heading_level=3)

        self.assertIn("Mutually exclusive group: choose exactly one of **--json**, **--text**.", details)
        self.assertIn("Mutually exclusive group: choose at most one of **--table**, **--csv**.", details)
        self.assertIn("Mutually exclusive group: choose exactly one of **--full**, **--incremental**.", details)
        self.assertLess(details.index("choose exactly one of **--json**"), details.index('<div id="--json"></div>'))
        self.assertLess(details.index("choose at most one of **--table**"), details.index('<div id="--table"></div>'))
        self.assertLess(details.index("choose exactly one of **--full**"), details.index('<div id="sync~--full"></div>'))
        self.assertEqual(3, details.count("Mutually exclusive group:"))
        self.assertNotIn("--hidden", details)

    def test_mutually_exclusive_group_positional_uses_formatter_metavar(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.MetavarTypeHelpFormatter)
        group = parser.add_mutually_exclusive_group()
        group.add_argument("count", nargs="?", type=int, help="Number of items.")
        group.add_argument("--all", action="store_true", help="Include all items.")

        details = markdown_from_argparse._render_help_details(parser)

        self.assertIn("Mutually exclusive group: choose at most one of **int**, **--all**.", details)
        self.assertLess(details.index("choose at most one of **int**"), details.index('<div id="count"></div>'))
        self.assertNotIn("choose at most one of **count**", details)

    def test_mutually_exclusive_group_positional_uses_tuple_metavar(self) -> None:
        """Covers tuple metavar labels where argparse accepts starred positional groups."""
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        group = parser.add_mutually_exclusive_group()
        group.add_argument("pair", nargs="*", metavar=("SRC", "DST"), default=[], help="Dataset pairs.")
        group.add_argument("--all", action="store_true", help="Include all pairs.")

        details = markdown_from_argparse._render_help_details(parser)

        self.assertIn("Mutually exclusive group: choose at most one of **SRC DST**, **--all**.", details)
        self.assertLess(details.index("choose at most one of **SRC DST**"), details.index('<div id="pair"></div>'))
        self.assertNotIn("choose at most one of **SRC**, **--all**", details)

    def test_recursive_subparsers_are_rendered_with_configurable_headings(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument("--root", action="store_true", help="Root option.")
        commands = parser.add_subparsers(dest="command", title="Commands", description="Available commands.")

        sync = commands.add_parser(
            "sync",
            help="Sync snapshots.",
            description="Synchronize selected snapshots.",
            formatter_class=argparse.RawTextHelpFormatter,
        )
        sync.add_argument("--speed", choices=["fast", "safe"], help="Sync speed.")
        modes = sync.add_subparsers(dest="mode", title="Sync Modes", description="Sync variants.")
        full = modes.add_parser("full", help="Full replication.", formatter_class=argparse.RawTextHelpFormatter)
        full.add_argument("--force", action="store_true", help="Force full mode.")

        prune = commands.add_parser("prune", aliases=["trim"], help="Prune snapshots.")
        prune.add_argument("--dry-run", action="store_true", help="Show planned pruning.")
        commands.add_parser("hidden", help=argparse.SUPPRESS)

        details = markdown_from_argparse._render_help_details(parser, heading_level=3)

        self.assertIn('<div id="--root"></div>', details)
        self.assertIn("### COMMANDS", details)
        self.assertIn("Available commands.", details)
        self.assertIn("### sync", details)
        self.assertIn("Synchronize selected snapshots.", details)
        self.assertIn('<div id="sync~--speed"></div>', details)
        self.assertIn("**--speed** *{fast,safe}*", details)
        self.assertIn("#### SYNC MODES", details)
        self.assertIn("Sync variants.", details)
        self.assertIn("#### full", details)
        self.assertIn('<div id="sync~full~--force"></div>', details)
        self.assertIn("### prune (trim)", details)
        self.assertIn('<div id="prune~--dry-run"></div>', details)
        self.assertNotIn('<div id="--speed"></div>', details)
        self.assertNotIn('<div id="--force"></div>', details)
        self.assertNotIn('<div id="--dry-run"></div>', details)
        self.assertEqual(1, details.count("### prune (trim)"))
        self.assertNotIn("hidden", details)

    def test_subparser_usage_blocks_are_rendered_under_subparser_headings(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        commands = parser.add_subparsers(dest="command", title="Commands")
        sync = commands.add_parser(
            "sync",
            help="Sync snapshots.",
            description="Synchronize selected snapshots.",
            formatter_class=argparse.RawTextHelpFormatter,
        )
        sync.add_argument("--job-id", required=True, metavar="STRING", help="Job id.")
        sync_modes = sync.add_subparsers(dest="mode", title="Sync Modes")
        full = sync_modes.add_parser("full", help="Full replication.", formatter_class=argparse.RawTextHelpFormatter)
        full.add_argument("dataset", metavar="DATASET", help="Dataset to replicate.")

        details = markdown_from_argparse._render_help_details(parser, heading_level=3)

        self.assertIn("### sync\n\nSynchronize selected snapshots.\n\n```\nusage: demo sync", details)
        self.assertTrue(  # Python <= 3.12 splits the option and metavar; newer argparse keeps them on one line.
            "       --job-id STRING" in details or "       --job-id\n       STRING" in details,
            details,
        )
        self.assertLess(details.index("--job-id"), details.index("{full}"))
        full_usage_markers = (
            "#### full\n\nFull replication.\n\n```\nusage: demo sync full",
            # Python 3.15-dev includes the parent's required option in the nested subparser usage.
            "#### full\n\nFull replication.\n\n```\nusage: demo sync --job-id STRING full",
        )
        self.assertTrue(any(marker in details for marker in full_usage_markers), details)
        self.assertIn("       DATASET", details)
        self.assertLess(details.index("usage: demo sync"), details.index('<div id="sync~--job-id"></div>'))
        full_usage_index = next(
            details.index(marker.rsplit("\n", maxsplit=1)[-1]) for marker in full_usage_markers if marker in details
        )
        self.assertLess(full_usage_index, details.index('<div id="sync~full~dataset"></div>'))

    def test_epilogs_are_rendered_after_parser_details(self) -> None:
        parser = argparse.ArgumentParser(
            prog="demo",
            epilog="Root epilog.\n\n```\ndemo root\n```",
            formatter_class=argparse.RawTextHelpFormatter,
        )
        parser.add_argument("--root", action="store_true", help="Root option.")
        commands = parser.add_subparsers(dest="command", title="Commands")

        sync = commands.add_parser(
            "sync",
            help="Sync snapshots.",
            epilog="Sync epilog.",
            formatter_class=argparse.RawTextHelpFormatter,
        )
        sync.add_argument("--speed", choices=["fast", "safe"], help="Sync speed.")
        modes = sync.add_subparsers(dest="mode", title="Sync Modes")
        full = modes.add_parser(
            "full",
            help="Full replication.",
            epilog="Full epilog.",
            formatter_class=argparse.RawTextHelpFormatter,
        )
        full.add_argument("--force", action="store_true", help="Force full mode.")

        suppressed = commands.add_parser(
            "suppressed",
            help="Suppress epilog only.",
            epilog=argparse.SUPPRESS,
            formatter_class=argparse.RawTextHelpFormatter,
        )
        suppressed.add_argument("--visible", action="store_true", help="Visible option.")

        details = markdown_from_argparse._render_help_details(parser, heading_level=3)

        self.assertIn("Sync epilog.", details)
        self.assertIn("Full epilog.", details)
        self.assertIn("Root epilog.", details)
        self.assertIn("```\ndemo root\n```", details)
        self.assertNotIn("==SUPPRESS==", details)
        self.assertLess(details.index("Sync speed."), details.index("Sync epilog."))
        self.assertLess(details.index("Force full mode."), details.index("Full epilog."))
        self.assertLess(details.index("Visible option."), details.index("Root epilog."))

    def test_subparser_action_with_only_suppressed_choices_is_omitted(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        commands = parser.add_subparsers(dest="command")
        hidden = commands.add_parser("hidden", help=argparse.SUPPRESS)
        hidden.add_argument("--hidden-option", help="Hidden option.")
        parser.add_argument("--visible", action="store_true", help="Visible option.")

        details = markdown_from_argparse._render_help_details(parser)

        self.assertIn('<div id="--visible"></div>', details)
        self.assertIn("Visible option.", details)
        self.assertNotIn("SUBCOMMANDS", details)
        self.assertNotIn("hidden", details)
        self.assertNotIn("--hidden-option", details)

    def test_subparser_without_description_or_help_still_renders_children(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        commands = parser.add_subparsers(dest="command")
        status = commands.add_parser("status", aliases=["st"])
        status.add_argument("--json", action="store_true", help="Emit JSON.")

        details = markdown_from_argparse._render_help_details(parser)

        self.assertIn("# status (st)", details)
        self.assertIn('<div id="status~--json"></div>', details)
        self.assertIn("Emit JSON.", details)
        self.assertNotIn("SUBCOMMANDS", details)

    def test_subparser_anchor_separator_distinguishes_nested_paths(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        commands = parser.add_subparsers(dest="command")

        flat = commands.add_parser("a_b", help="Flat command.")
        flat.add_argument("--flag", action="store_true", help="Flat flag.")

        nested_root = commands.add_parser("a", help="Nested root.")
        nested_commands = nested_root.add_subparsers(dest="nested_command")
        nested = nested_commands.add_parser("b", help="Nested command.")
        nested.add_argument("--flag", action="store_true", help="Nested flag.")

        details = markdown_from_argparse._render_help_details(parser)

        self.assertIn('<div id="a_b~--flag"></div>', details)
        self.assertIn('<div id="a~b~--flag"></div>', details)
        self.assertEqual(1, details.count('<div id="a_b~--flag"></div>'))
        self.assertEqual(1, details.count('<div id="a~b~--flag"></div>'))

    def test_generated_metadata_escapes_common_markdown_and_html_chars(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument("src", metavar="SRC_<DATA>", help="Source.")
        group = parser.add_argument_group("Danger & *Group* [A]")
        group.add_argument("--path<tag>", metavar="NAME_[X]", help="Path.")
        mode = group.add_mutually_exclusive_group()
        mode.add_argument("--json<tag>", action="store_true", help="JSON.")
        mode.add_argument("--text*plain", action="store_true", help="Text.")
        commands = parser.add_subparsers(dest="command", title="Commands & *Modes*")
        sync = commands.add_parser("sync*<fast>", help="Sync.")
        sync.add_argument('--mode"fast', metavar="VAL&<X>", help="Mode.")

        details = markdown_from_argparse._render_help_details(parser)

        self.assertIn('<div id="src"></div>', details)
        self.assertIn("**SRC_&lt;DATA&gt;**", details)
        self.assertIn("# DANGER &amp; \\*GROUP\\* [A]", details)
        self.assertIn('<div id="--path&lt;tag&gt;"></div>', details)
        self.assertIn("**--path&lt;tag&gt;** *NAME_[X]*", details)
        self.assertIn(
            "Mutually exclusive group: choose at most one of **--json&lt;tag&gt;**, **--text\\*plain**.",
            details,
        )
        self.assertIn("# COMMANDS &amp; \\*MODES\\*", details)
        self.assertIn("# sync\\*&lt;fast&gt;", details)
        self.assertIn('<div id="sync*&lt;fast&gt;~--mode&quot;fast"></div>', details)
        self.assertIn('**--mode"fast** *VAL&amp;&lt;X&gt;*', details)

    def test_custom_help_action_is_rendered(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument("--help, -h", action="help", help="Show help.")

        details = markdown_from_argparse._render_help_details(parser)

        self.assertNotIn('<div id="-h"></div>', details)
        self.assertIn('<div id="--help,_-h"></div>', details)
        self.assertIn("**--help, -h**", details)

    def test_default_help_action_is_hidden_for_alternate_prefix_chars(self) -> None:
        cases = (
            ("+/", "+visible", '<div id="+h"></div>', "**+h**, **++help**"),
            ("+-", "+visible", '<div id="-h"></div>', "**-h**, **--help**"),
            ("/-", "/visible", '<div id="-h"></div>', "**-h**, **--help**"),
        )
        for prefix_chars, visible_option, help_anchor, help_title in cases:
            with self.subTest(prefix_chars=prefix_chars):
                parser = argparse.ArgumentParser(
                    prog="demo", prefix_chars=prefix_chars, formatter_class=argparse.RawTextHelpFormatter
                )
                parser.add_argument(visible_option, metavar="VALUE", help="Visible option.")

                details = markdown_from_argparse._render_help_details(parser)

                self.assertNotIn(help_anchor, details)
                self.assertNotIn(help_title, details)
                self.assertNotIn("show this help message and exit", details)
                self.assertIn(f'<div id="{visible_option}"></div>', details)
                self.assertIn(f"**{visible_option}** *VALUE*", details)

    def test_bzfs_custom_actions_are_rendered_from_parser_model(self) -> None:
        """Covers project-specific argparse actions without invoking their parsers."""
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument(
            "root_dataset_pairs",
            nargs="+",
            action=argparse_actions.DatasetPairsAction,
            metavar="SRC_DATASET DST_DATASET",
            help="Dataset pairs.",
        )
        parser.add_argument(
            "--include-snapshot-regex",
            action=argparse_actions.FileOrLiteralAction,
            nargs="+",
            default=[],
            metavar="REGEX",
            help="Snapshot regex.",
        )
        parser.add_argument(
            "--compare-include-regex",
            action=argparse_actions.FileOrLiteralAction,
            default=None,
            const=[],
            nargs="*",
            metavar="REGEX",
            help="Optional regex list.",
        )
        parser.add_argument(
            "--include-snapshot-times-and-ranks",
            action=argparse_actions.TimeRangeAndRankRangeAction,
            nargs="+",
            default=[],
            metavar=("TIMERANGE", "RANKRANGE"),
            help="Time and rank filters.",
        )
        parser.add_argument(
            "--include-snapshot-plan",
            action=argparse_actions.IncludeSnapshotPlanAction,
            default=None,
            metavar="DICT_STRING",
            help="Snapshot plan.",
        )
        parser.add_argument(
            "--delete-dst-snapshots-except-plan",
            action=argparse_actions.DeleteDstSnapshotsExceptPlanAction,
            default=None,
            metavar="DICT_STRING",
            help="Delete plan.",
        )
        parser.add_argument(
            "--new-snapshot-filter-group",
            action=argparse_actions.NewSnapshotFilterGroupAction,
            nargs=0,
            help="New filter group.",
        )
        parser.add_argument(
            "--log-file-prefix",
            default="zrun_",
            action=argparse_actions.SafeFileNameAction,
            metavar="STRING",
            help="Prefix default %(default)s.",
        )
        parser.add_argument("--log-dir", action=argparse_actions.SafeDirectoryNameAction, metavar="DIR", help="Log dir.")
        parser.add_argument(
            "--workers",
            min=1,
            default=(100, True),
            action=argparse_actions.CheckPercentRange,
            metavar="INT[%]",
            help="Workers min %(min)s default %(default)s.",
        )

        details = markdown_from_argparse._render_help_details(parser)

        self.assertIn('<div id="root_dataset_pairs"></div>', details)
        self.assertIn("**SRC_DATASET DST_DATASET [SRC_DATASET DST_DATASET ...]**", details)
        self.assertIn("**--include-snapshot-regex** *REGEX [REGEX ...]*", details)
        self.assertIn("**--compare-include-regex** *[REGEX ...]*", details)
        self.assertIn("**--include-snapshot-times-and-ranks** *TIMERANGE [RANKRANGE ...]*", details)
        self.assertIn("**--include-snapshot-plan** *DICT_STRING*", details)
        self.assertIn("**--delete-dst-snapshots-except-plan** *DICT_STRING*", details)
        self.assertIn("**--new-snapshot-filter-group**", details)
        self.assertIn("**--log-file-prefix** *STRING*", details)
        self.assertIn("Prefix default zrun_.", details)
        self.assertIn("**--log-dir** *DIR*", details)
        self.assertIn("**--workers** *INT[%]*", details)
        self.assertIn("Workers min 1 default (100, True).", details)

    def test_jobrunner_argparse_features_are_rendered_from_parser_model(self) -> None:
        """Covers argparse features used directly by bzfs_jobrunner."""
        parser = argparse.ArgumentParser(prog="demo", allow_abbrev=False, formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument("--create-src-snapshots", action="store_true", help="Take snapshots.")
        parser.add_argument("--src-host", default=None, action="append", metavar="STRING", help="Subset source hosts.")
        parser.add_argument(
            "--job-id",
            required=True,
            action=argparse_actions.NonEmptyStringAction,
            metavar="STRING",
            help="Job id.",
        )
        parser.add_argument(
            "--ssh-src-config-file",
            type=str,
            action=argparse_actions.SSHConfigFileNameAction,
            metavar="FILE",
            help="SSH config.",
        )
        parser.add_argument(
            "--jobrunner-log-level",
            choices=["CRITICAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
            default="INFO",
            help="Default is '%(default)s'.",
        )
        parser.add_argument("--work-period-seconds", type=float, metavar="FLOAT", help="Spread starts.")
        parser.add_argument("--version", action="version", version="demo-1", help="Display version.")
        parser.add_argument("--timeout", action=bzfs_jobrunner.RejectArgumentAction, nargs=0, help=argparse.SUPPRESS)

        details = markdown_from_argparse._render_help_details(parser)

        self.assertIn("**--create-src-snapshots**", details)
        self.assertIn("**--src-host** *STRING*", details)
        self.assertIn("**--job-id** *STRING*", details)
        self.assertIn("**--ssh-src-config-file** *FILE*", details)
        self.assertIn("**--jobrunner-log-level** *{CRITICAL,ERROR,WARN,INFO,DEBUG,TRACE}*", details)
        self.assertIn("Default is 'INFO'.", details)
        self.assertIn("**--work-period-seconds** *FLOAT*", details)
        self.assertIn("**--version**", details)
        self.assertNotIn("--timeout", details)

    def test_render_blocks_preserves_markdown_headings_and_plain_fenced_code(self) -> None:
        """Covers Markdown blocks that argparse help embeds in README sections."""
        text = "# Heading\n\n```\ndemo --option value \\\n--second value\n\nplain output\n```"

        rendered = "\n".join(markdown_from_argparse._render_blocks(text))

        self.assertIn("# Heading", rendered)
        self.assertIn("```", rendered)
        self.assertIn("demo --option value \\\n--second value", rendered)
        self.assertIn("\n\nplain output", rendered)

    def test_render_blocks_without_language_uses_plain_fence_in_list_item(self) -> None:
        """Covers source-authored plain fenced examples in action help."""
        self.assertListEqual(
            ["*  Intro.", "", "", "    ```", "    demo --flag", "    ```", ""],
            markdown_from_argparse._render_blocks("Intro.\n\n```\ndemo --flag\n```", list_item=True),
        )

    def test_render_blocks_preserves_explicit_line_continuations(self) -> None:
        """Covers explicit shell continuation lines authored in argparse help."""
        self.assertListEqual(
            ["", "```", "cmd1 \\", "cmd2", "", "cmd output", "```", ""],
            markdown_from_argparse._render_blocks("```\ncmd1 \\\ncmd2\n\ncmd output\n```"),
        )

    def test_render_blocks_rejects_unmatched_fenced_code(self) -> None:
        """Covers malformed Markdown examples before README output is written."""
        text = "Intro.\n\n```\ndemo --flag"
        for list_item in (False, True):
            with self.subTest(list_item=list_item):
                with self.assertRaises(SystemExit) as cm:
                    markdown_from_argparse._render_blocks(text, list_item=list_item)
                self.assertIn("Opening ``` fence without a matching closing fence", str(cm.exception))
                self.assertIn(repr(text), str(cm.exception))

    def test_help_detail_handles_titled_group_without_description_or_action_help(self) -> None:
        """Covers titled groups and intentionally terse argparse actions."""
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        group = parser.add_argument_group("Advanced Options")
        group.add_argument("--bare", action="store_true")

        details = markdown_from_argparse._render_help_details(parser)

        self.assertIn("# ADVANCED OPTIONS", details)
        self.assertIn("**--bare**", details)
        self.assertNotIn("*  None", details)

    def test_render_readme_replaces_generated_sections(self) -> None:
        """Covers full README replacement with a small parser."""
        parser = self.make_demo_parser(description="Demo description.")

        rendered = markdown_from_argparse._render_readme(parser, self.readme_template())

        self.assertIn("<!-- BEGIN-MANPAGE-DESCRIPTION -->\nDemo description.\n<!-- END-MANPAGE-DESCRIPTION -->", rendered)
        self.assertIn("<!-- BEGIN-MANPAGE-USAGE -->\n```\nusage: demo", rendered)
        self.assertNotIn("\x1b[", rendered)
        self.assertIn('<!-- BEGIN-MANPAGE-DETAILS -->\n<div id="--flag"></div>', rendered)
        self.assertIn("<!-- END-MANPAGE-DETAILS -->\nafter generated details\n", rendered)
        self.assertNotIn("old description", rendered)
        self.assertNotIn("old overview", rendered)
        self.assertNotIn("old details", rendered)

    def test_render_readme_without_description_section_replaces_usage_and_details(self) -> None:
        """Covers README files that only opt into generated usage and details."""
        parser = self.make_demo_parser(description="Demo description.")
        readme = (
            "manual introduction\n"
            "<!-- BEGIN-MANPAGE-USAGE -->\n"
            "old overview\n"
            "<!-- END-MANPAGE-USAGE -->\n"
            "tail\n"
            "<!-- BEGIN-MANPAGE-DETAILS -->\n"
            "old details\n"
            "<!-- END-MANPAGE-DETAILS -->\n"
            "after generated details\n"
        )

        rendered = markdown_from_argparse._render_readme(parser, readme)

        self.assertIn("manual introduction\n", rendered)
        self.assertNotIn("BEGIN-MANPAGE-DESCRIPTION", rendered)
        self.assertNotIn("Demo description.", rendered)
        self.assertIn("<!-- BEGIN-MANPAGE-USAGE -->\n```\nusage: demo", rendered)
        self.assertIn('<!-- BEGIN-MANPAGE-DETAILS -->\n<div id="--flag"></div>', rendered)
        self.assertIn("<!-- END-MANPAGE-DETAILS -->\nafter generated details\n", rendered)
        self.assertNotIn("old overview", rendered)
        self.assertNotIn("old details", rendered)

    def test_main_requires_module_and_readme_arguments(self) -> None:
        """Covers argparse validation for required update_readme options."""
        with patch.object(sys, "argv", ["update_readme"]), patch("sys.stderr", new_callable=StringIO) as stderr:
            with self.assertRaises(SystemExit) as cm:
                markdown_from_argparse.main()

        self.assertEqual(2, cm.exception.code)
        self.assertIn("usage:", stderr.getvalue())
        self.assertIn("the following arguments are required: --readme, --module", stderr.getvalue())

    def test_main_updates_readme_file(self) -> None:
        """Covers the update_readme command-line success path without invoking real parsers."""
        with TemporaryDirectory() as tmpdir:
            readme_path = Path(tmpdir) / "README.md"
            readme_path.write_text(self.readme_template(), encoding="utf-8")
            parser = self.make_demo_parser(description="Demo description.")

            with (
                patch.object(sys, "argv", ["update_readme", "--module", "bzfs_main.bzfs", "--readme", str(readme_path)]),
                patch.object(importlib, "import_module") as mock_import,
                capture_stdout() as stdout,
                capture_stderr() as stderr,
            ):
                mock_import.return_value.argument_parser.return_value = parser
                runpy.run_path(str(Path(markdown_from_argparse.__file__).resolve()), run_name="__main__")

            rendered = readme_path.read_text(encoding="utf-8")
            self.assertIn("<!-- BEGIN-MANPAGE-DESCRIPTION -->\nDemo description.", rendered)
            self.assertIn("<!-- BEGIN-MANPAGE-USAGE -->\n```\nusage: demo", rendered)
            self.assertIn('<!-- BEGIN-MANPAGE-DETAILS -->\n<div id="--flag"></div>', rendered)
            self.assertIn("<!-- END-MANPAGE-DETAILS -->\nafter generated details\n", rendered)
            mock_import.assert_called_once_with("bzfs_main.bzfs")
            mock_import.return_value.argument_parser.assert_called_once_with()
            self.assertEqual("", stdout.getvalue())
            self.assertIn("Successfully updated", stderr.getvalue())

    def test_main_creates_missing_readme_from_template(self) -> None:
        """Covers the command-line path that bootstraps a README skeleton."""
        with TemporaryDirectory() as tmpdir:
            readme_path = Path(tmpdir) / "GENERATED.md"
            parser = self.make_demo_parser(description="Demo description.")

            with (
                patch.object(sys, "argv", ["update_readme", "--module", "bzfs_main.bzfs", "--readme", str(readme_path)]),
                patch.object(importlib, "import_module") as mock_import,
                capture_stdout() as stdout,
                capture_stderr() as stderr,
            ):
                mock_import.return_value.argument_parser.return_value = parser
                markdown_from_argparse.main()

            rendered = readme_path.read_text(encoding="utf-8")
            self.assertIn("# GENERATED", rendered)
            self.assertIn("<!-- BEGIN-MANPAGE-DESCRIPTION -->\nDemo description.", rendered)
            self.assertIn("<!-- BEGIN-MANPAGE-USAGE -->\n```\nusage: demo", rendered)
            self.assertIn('<!-- BEGIN-MANPAGE-DETAILS -->\n<div id="--flag"></div>', rendered)
            mock_import.assert_called_once_with("bzfs_main.bzfs")
            mock_import.return_value.argument_parser.assert_called_once_with()
            self.assertEqual("", stdout.getvalue())
            self.assertIn("Successfully created", stderr.getvalue())

    def test_main_updates_readme_with_real_cli_modules(self) -> None:
        """Covers README generation with the real bzfs and bzfs_jobrunner parsers."""
        cases = (
            (
                "bzfs_main.bzfs",
                "README.md",
                ("usage: bzfs [-h]", '<div id="root_dataset_pairs"></div>', '<div id="--recursive"></div>'),
            ),
            (
                "bzfs_main.bzfs_jobrunner",
                "README_bzfs_jobrunner.md",
                ("usage: bzfs_jobrunner", '<div id="--create-src-snapshots"></div>', '<div id="--job-id"></div>'),
            ),
        )

        with TemporaryDirectory() as tmpdir:
            for module_name, filename, expected_fragments in cases:
                with self.subTest(module_name=module_name):
                    readme_path = Path(tmpdir) / filename
                    readme_path.write_text(self.readme_template(), encoding="utf-8")

                    with (
                        patch.object(sys, "argv", ["update_readme", f"--module={module_name}", f"--readme={readme_path}"]),
                        capture_stdout() as stdout,
                        capture_stderr() as stderr,
                    ):
                        markdown_from_argparse.main()

                    rendered = readme_path.read_text(encoding="utf-8")
                    for fragment in expected_fragments:
                        self.assertIn(fragment, rendered)
                    self.assertIn("<!-- END-MANPAGE-DETAILS -->\nafter generated details\n", rendered)
                    self.assertNotIn("old description", rendered)
                    self.assertNotIn("old overview", rendered)
                    self.assertNotIn("old details", rendered)
                    self.assertEqual("", stdout.getvalue())
                    self.assertIn("Successfully updated", stderr.getvalue())

    def test_main_rejects_unknown_module_without_modifying_file(self) -> None:
        """Covers the update_readme command-line import error path."""
        with TemporaryDirectory() as tmpdir:
            readme_path = Path(tmpdir) / "README.md"
            original_readme = "old readme"
            readme_path.write_text(original_readme, encoding="utf-8")

            with (
                patch.object(sys, "argv", ["update_readme", "--module", "typo.not_a_module", "--readme", str(readme_path)]),
                patch.object(importlib, "import_module", side_effect=ModuleNotFoundError("No module named typo")),
            ):
                with self.assertRaises(ModuleNotFoundError):
                    markdown_from_argparse.main()

            self.assertEqual(original_readme, readme_path.read_text(encoding="utf-8"))

    def test_format_usage_returns_raw_usage_and_restores_envvars(self) -> None:
        """Covers scoping of temporary argparse usage environment overrides."""
        parser = self.make_demo_parser(description="Demo description.")

        with patch.dict(os.environ, {"COLUMNS": "120", "PYTHON_COLORS": "1"}):
            usage = markdown_from_argparse._format_usage(parser)

            self.assertTrue(usage.startswith("usage: demo"))
            self.assertNotIn("\x1b[", usage)
            self.assertNotIn("```", usage)
            self.assertEqual("120", os.environ["COLUMNS"])
            self.assertEqual("1", os.environ["PYTHON_COLORS"])

    @staticmethod
    def make_demo_parser(description: str) -> argparse.ArgumentParser:
        """Returns a compact parser for README rendering tests."""
        parser = argparse.ArgumentParser(
            prog="demo",
            description=description,
            formatter_class=argparse.RawTextHelpFormatter,
        )
        parser.add_argument("--flag", action="store_true", help="Flag help.")
        return parser

    @staticmethod
    def readme_template() -> str:
        """Returns a minimal README skeleton with the generated section markers."""
        return (
            "prefix\n"
            "<!-- BEGIN-MANPAGE-DESCRIPTION -->\n"
            "old description\n"
            "<!-- END-MANPAGE-DESCRIPTION -->\n"
            "between\n"
            "<!-- BEGIN-MANPAGE-USAGE -->\n"
            "old overview\n"
            "<!-- END-MANPAGE-USAGE -->\n"
            "tail\n"
            "<!-- BEGIN-MANPAGE-DETAILS -->\n"
            "old details\n"
            "<!-- END-MANPAGE-DETAILS -->\n"
            "after generated details\n"
        )
