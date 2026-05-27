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
"""Tests for SVG badge generator."""

from __future__ import (
    annotations,
)
import runpy
import sys
import tempfile
import unittest
import urllib.request
from pathlib import (
    Path,
)
from unittest.mock import (
    patch,
)

from bzfs_main.util.generate_badge import (
    generate_badge,
    main,
)
from bzfs_tests.tools import (
    capture_stdout,
    suppress_output,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestGenerateBadge,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestGenerateBadge(unittest.TestCase):
    """Exercises downloaded and locally generated SVG badge output.

    The tests patch network calls so badge generation remains deterministic while covering the public function and CLI.
    """

    def test_generate_badge_downloads_shields_svg(self) -> None:
        """Exercise the shields.io path for URL construction and file output."""
        with tempfile.TemporaryDirectory(prefix="bzfs_test_badges_") as tmpdir:
            output_file = Path(tmpdir) / "badges" / "coverage-badge.svg"
            downloaded_svg = (
                '<svg xmlns="http://www.w3.org/2000/svg" width="114" height="20" '
                'role="img" aria-label="coverage: 99.53%"><title>coverage: 99.53%</title></svg>'
            )
            requested_urls: list[str] = []

            def urlopen(request: urllib.request.Request, timeout: float) -> _FakeHttpResponse:
                requested_urls.append(request.full_url)
                self.assertEqual(30, timeout)
                self.assertEqual("curl/8.7.1", request.get_header("User-agent"))
                return _FakeHttpResponse(downloaded_svg)

            with (
                patch("urllib.request.urlopen", side_effect=urlopen),
                suppress_output(),
            ):
                generate_badge("coverage", "99.53%", "#4b0", str(output_file), timeout=30)

            svg = output_file.read_text(encoding="utf-8")

        self._assert_badge_title("coverage: 99.53%", svg)
        self.assertEqual(downloaded_svg, svg)
        self.assertEqual(["https://img.shields.io/badge/coverage-99.53%25-%234b0.svg"], requested_urls)

    def test_generate_badge_falls_back_when_download_is_not_svg(self) -> None:
        """Exercise SVG validation for malformed shields.io responses."""
        with tempfile.TemporaryDirectory(prefix="bzfs_test_badges_") as tmpdir:
            output_file = Path(tmpdir) / "badges" / "coverage-badge.svg"

            with (
                patch("urllib.request.urlopen", return_value=_FakeHttpResponse("not an svg")),
                suppress_output(),
            ):
                generate_badge("coverage", "99.53%", "#4b0", str(output_file), timeout=30)

            svg = output_file.read_text(encoding="utf-8")

        self._assert_badge_title("coverage: 99.53%", svg)
        self.assertIn(">99.53%<", svg)
        self.assertIn("#4b0", svg)
        self.assertNotEqual("not an svg", svg)

    def test_generate_badge_falls_back_to_local_svg(self) -> None:
        """Exercise deterministic local SVG generation when shields.io is unavailable."""
        with tempfile.TemporaryDirectory(prefix="bzfs_test_badges_") as tmpdir:
            output_file = Path(tmpdir) / "nested" / "coverage-badge.svg"

            with (
                patch("urllib.request.urlopen", side_effect=OSError("offline")),
                suppress_output(),
            ):
                generate_badge("coverage", "99.53%", "#4b0", str(output_file), timeout=30)

            svg = output_file.read_text(encoding="utf-8")

        self._assert_badge_title("coverage: 99.53%", svg)
        self.assertIn(">99.53%<", svg)
        self.assertIn("#4b0", svg)

    def test_main_passes_parsed_arguments_to_generate_badge(self) -> None:
        """Exercise command-line parsing before badge generation."""
        with tempfile.TemporaryDirectory(prefix="bzfs_test_badges_") as tmpdir:
            output_file = Path(tmpdir) / "coverage.svg"

            with (
                patch.object(
                    sys,
                    "argv",
                    [
                        "generate_badge.py",
                        "--left=coverage",
                        "--right=99.53%",
                        "--color=#4b0",
                        f"--output={output_file}",
                        "--timeout=12.5",
                    ],
                ),
                patch("bzfs_main.util.generate_badge.generate_badge") as generate_badge_mock,
            ):
                main()

        generate_badge_mock.assert_called_once_with("coverage", "99.53%", "#4b0", str(output_file), 12.5)

    def test_main_generates_badge_from_argv(self) -> None:
        """Exercise the direct script entry point with normal package imports."""
        with tempfile.TemporaryDirectory(prefix="bzfs_test_badges_") as tmpdir:
            output_file = Path(tmpdir) / "os.svg"

            with (
                patch.object(
                    sys,
                    "argv",
                    [
                        "generate_badge.py",
                        "--left=os",
                        "--right=Linux | FreeBSD",
                        "--color=",
                        f"--output={output_file}",
                        "--timeout=0",
                    ],
                ),
                suppress_output(),
            ):
                runpy.run_module("bzfs_main.util.generate_badge", run_name="__main__")

            svg = output_file.read_text(encoding="utf-8")

        self._assert_badge_title("os: Linux | FreeBSD", svg)
        self.assertIn(">Linux | FreeBSD<", svg)
        self.assertIn("#007ec6", svg)

    def test_main_prints_help(self) -> None:
        """Exercise argparse help expansion for examples containing percent signs."""
        with (
            patch.object(sys, "argv", ["generate_badge.py", "--help"]),
            capture_stdout() as stdout,
            self.assertRaises(SystemExit) as cm,
        ):
            main()

        self.assertEqual(0, cm.exception.code)
        help_text = stdout.getvalue()
        self.assertIn("--right STRING", help_text)
        self.assertIn("99.53%", help_text)

    def _assert_badge_title(self, expected: str, svg: str) -> None:
        """Check the SVG title text used by badge consumers."""
        self.assertIn(f"<title>{expected}</title>", svg)


class _FakeHttpResponse:
    """Minimal urlopen response context manager with deterministic bytes."""

    def __init__(self, text: str) -> None:
        """Encode the response body once, matching urlopen().read()."""
        self._data = text.encode("utf-8")

    def __enter__(self) -> _FakeHttpResponse:
        """Return this response for `with urlopen(...) as response`."""
        return self

    def __exit__(self, *_unused: object) -> None:
        """Let exceptions from the response scope propagate."""

    def read(self) -> bytes:
        """Return the fake HTTP response bytes."""
        return self._data
