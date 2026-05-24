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

from bzfs_tests.generate_badge import (
    generate_badge,
    main,
)
from bzfs_tests.tools import (
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

    def test_main_generates_badge_from_argv(self) -> None:
        """Exercise the command-line entry point with normal package imports."""
        with tempfile.TemporaryDirectory(prefix="bzfs_test_badges_") as tmpdir:
            output_file = Path(tmpdir) / "os.svg"

            with (
                patch.object(sys, "argv", ["generate_badge.py", "os", "Linux | FreeBSD", "", str(output_file), "0"]),
                suppress_output(),
            ):
                main()

            svg = output_file.read_text(encoding="utf-8")

        self._assert_badge_title("os: Linux | FreeBSD", svg)
        self.assertIn(">Linux | FreeBSD<", svg)
        self.assertIn("#007ec6", svg)

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
