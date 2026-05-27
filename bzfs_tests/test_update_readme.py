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
from bzfs_docs import (
    update_readme,
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

    def test_common_argparse_features_are_rendered_from_parser_model(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument("src", metavar="SRC", help="Source dataset.")
        group = parser.add_argument_group("Transfer Options", description="Controls transfer behavior.")
        group.add_argument("--mode", choices=["fast", "safe"], default="safe", help="Choose mode. Default: %(default)s.")
        group.add_argument("--item", nargs="+", metavar="NAME", help="Name to process.")
        group.add_argument("--hidden", help=argparse.SUPPRESS)

        details = update_readme._render_help_details(parser)

        self.assertIn('<div id="SRC"></div>', details)
        self.assertIn("**SRC**", details)
        self.assertNotIn('<div id="-h"></div>', details)
        self.assertIn("# TRANSFER OPTIONS", details)
        self.assertIn("Controls transfer behavior.", details)
        self.assertIn("**--mode** *{fast,safe}*", details)
        self.assertIn("Choose mode. Default: safe.", details)
        self.assertIn("**--item** *NAME [NAME ...]*", details)
        self.assertNotIn("--hidden", details)

    def test_custom_help_action_is_rendered(self) -> None:
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument("--help, -h", action="help", help="Show help.")

        details = update_readme._render_help_details(parser)

        self.assertNotIn('<div id="-h"></div>', details)
        self.assertIn('<div id="--help,_-h"></div>', details)
        self.assertIn("**--help, -h**", details)

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

        details = update_readme._render_help_details(parser)

        self.assertIn('<div id="SRC_DATASET_DST_DATASET"></div>', details)
        self.assertIn("**SRC_DATASET DST_DATASET**", details)
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

        details = update_readme._render_help_details(parser)

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

        rendered = "\n".join(update_readme._render_blocks(text))

        self.assertIn("# Heading", rendered)
        self.assertIn("```", rendered)
        self.assertIn("demo --option value \\\n--second value", rendered)
        self.assertIn("\n\nplain output", rendered)

    def test_render_blocks_without_language_uses_plain_fence_in_list_item(self) -> None:
        """Covers source-authored plain fenced examples in action help."""
        self.assertListEqual(
            ["*  Intro.", "", "", "    ```", "    demo --flag", "    ```", ""],
            update_readme._render_blocks("Intro.\n\n```\ndemo --flag\n```", list_item=True),
        )

    def test_render_blocks_preserves_explicit_line_continuations(self) -> None:
        """Covers explicit shell continuation lines authored in argparse help."""
        self.assertListEqual(
            ["", "```", "cmd1 \\", "cmd2", "", "cmd output", "```", ""],
            update_readme._render_blocks("```\ncmd1 \\\ncmd2\n\ncmd output\n```"),
        )

    def test_render_blocks_rejects_unmatched_fenced_code(self) -> None:
        """Covers malformed Markdown examples before README output is written."""
        text = "Intro.\n\n```\ndemo --flag"
        for list_item in (False, True):
            with self.subTest(list_item=list_item):
                with self.assertRaises(ValueError) as cm:
                    update_readme._render_blocks(text, list_item=list_item)
                self.assertIn("Opening ``` fence without a matching closing fence", str(cm.exception))
                self.assertIn(repr(text), str(cm.exception))

    def test_help_detail_handles_titled_group_without_description_or_action_help(self) -> None:
        """Covers titled groups and intentionally terse argparse actions."""
        parser = argparse.ArgumentParser(prog="demo", formatter_class=argparse.RawTextHelpFormatter)
        group = parser.add_argument_group("Advanced Options")
        group.add_argument("--bare", action="store_true")

        details = update_readme._render_help_details(parser)

        self.assertIn("# ADVANCED OPTIONS", details)
        self.assertIn("**--bare**", details)
        self.assertNotIn("*  None", details)

    def test_render_readme_replaces_generated_sections(self) -> None:
        """Covers full README replacement with a small parser."""
        parser = self.make_demo_parser(description="Demo description.")

        with patch.dict(os.environ, {"NO_COLOR": "1"}):
            rendered = update_readme._render_readme(parser, self.readme_template())

        self.assertIn("<!-- BEGIN DESCRIPTION SECTION -->\nDemo description.\n\n<!-- END DESCRIPTION SECTION -->", rendered)
        self.assertIn("<!-- BEGIN HELP OVERVIEW SECTION -->\n```\nusage: demo", rendered)
        self.assertNotIn("\x1b[", rendered)
        self.assertIn('<!-- BEGIN HELP DETAIL SECTION -->\n<div id="--flag"></div>', rendered)
        self.assertNotIn("old description", rendered)
        self.assertNotIn("old overview", rendered)
        self.assertNotIn("old details", rendered)

    def test_main_requires_module_and_readme_arguments(self) -> None:
        """Covers argparse validation for required update_readme options."""
        with patch.object(sys, "argv", ["update_readme"]), patch("sys.stderr", new_callable=StringIO) as stderr:
            with self.assertRaises(SystemExit) as cm:
                update_readme.main()

        self.assertEqual(2, cm.exception.code)
        self.assertIn("usage:", stderr.getvalue())
        self.assertIn("the following arguments are required: --module, --readme", stderr.getvalue())

    def test_main_updates_readme_file(self) -> None:
        """Covers the update_readme command-line success path without invoking real parsers."""
        with TemporaryDirectory() as tmpdir:
            readme_path = Path(tmpdir) / "README.md"
            readme_path.write_text(self.readme_template(), encoding="utf-8")
            parser = self.make_demo_parser(description="Demo description.")

            with (
                patch.object(sys, "argv", ["update_readme", "--module", "bzfs_main.bzfs", "--readme", str(readme_path)]),
                patch.dict(os.environ, {"NO_COLOR": "1"}),
                patch.object(importlib, "import_module") as mock_import,
                capture_stdout() as stdout,
                capture_stderr() as stderr,
            ):
                mock_import.return_value.argument_parser.return_value = parser
                runpy.run_path(str(Path(update_readme.__file__).resolve()), run_name="__main__")

            rendered = readme_path.read_text(encoding="utf-8")
            self.assertIn("<!-- BEGIN DESCRIPTION SECTION -->\nDemo description.", rendered)
            self.assertIn("<!-- BEGIN HELP OVERVIEW SECTION -->\n```\nusage: demo", rendered)
            self.assertIn('<!-- BEGIN HELP DETAIL SECTION -->\n<div id="--flag"></div>', rendered)
            mock_import.assert_called_once_with("bzfs_main.bzfs")
            mock_import.return_value.argument_parser.assert_called_once_with()
            self.assertEqual("", stdout.getvalue())
            self.assertEqual("Success.\n", stderr.getvalue())

    def test_main_updates_readme_with_real_cli_modules(self) -> None:
        """Covers README generation with the real bzfs and bzfs_jobrunner parsers."""
        cases = (
            (
                "bzfs_main.bzfs",
                "README.md",
                ("usage: bzfs [-h]", '<div id="SRC_DATASET_DST_DATASET"></div>', '<div id="--recursive"></div>'),
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
                        patch.dict(os.environ, {"NO_COLOR": "1"}),
                        capture_stdout() as stdout,
                        capture_stderr() as stderr,
                    ):
                        update_readme.main()

                    rendered = readme_path.read_text(encoding="utf-8")
                    for fragment in expected_fragments:
                        self.assertIn(fragment, rendered)
                    self.assertNotIn("old description", rendered)
                    self.assertNotIn("old overview", rendered)
                    self.assertNotIn("old details", rendered)
                    self.assertEqual("", stdout.getvalue())
                    self.assertEqual("Success.\n", stderr.getvalue())

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
                    update_readme.main()

            self.assertEqual(original_readme, readme_path.read_text(encoding="utf-8"))

    def test_format_usage_returns_raw_usage_and_restores_columns_envvar(self) -> None:
        """Covers scoping of the temporary argparse usage width override."""
        parser = self.make_demo_parser(description="Demo description.")

        with patch.dict(os.environ, {"COLUMNS": "120", "NO_COLOR": "1"}):
            usage = update_readme._format_usage(parser)

            self.assertTrue(usage.startswith("usage: demo"))
            self.assertNotIn("```", usage)
            self.assertEqual("120", os.environ["COLUMNS"])

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
            "<!-- BEGIN DESCRIPTION SECTION -->\n"
            "old description\n"
            "<!-- END DESCRIPTION SECTION -->\n"
            "between\n"
            "<!-- BEGIN HELP OVERVIEW SECTION -->\n"
            "old overview\n"
            "<!-- END HELP OVERVIEW SECTION -->\n"
            "tail\n"
            "<!-- BEGIN HELP DETAIL SECTION -->\n"
            "old details\n"
        )
