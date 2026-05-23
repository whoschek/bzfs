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
"""Tests for CI badge generation used by coverage and Pages workflows."""

from __future__ import (
    annotations,
)
import importlib.util
import os
import sys
import tempfile
import unittest
from collections.abc import (
    Iterator,
)
from contextlib import (
    contextmanager,
)
from pathlib import (
    Path,
)
from types import (
    ModuleType,
)
from unittest.mock import (
    patch,
)

from bzfs_tests.tools import (
    suppress_output,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestGenerateBadges,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class TestGenerateBadges(unittest.TestCase):
    """Exercises badge merging against synthetic CI artifacts.

    The tests model downloaded GitHub Actions badge marker directories and the combined coverage percentage file.
    Network calls are patched so the shields.io and offline SVG paths remain deterministic.
    """

    def setUp(self) -> None:
        self.repo_root: Path = Path(__file__).resolve().parents[1]
        self.module: ModuleType = self._load_generate_badges_module()

    def test_merge_command_downloads_coverage_badge(self) -> None:
        """Exercise the shields.io path for coverage badge URL construction."""

        with tempfile.TemporaryDirectory(prefix="bzfs_test_badges_") as tmpdir:
            tmpdir_path = Path(tmpdir)
            self._write_coverage_percentage(tmpdir_path / "coverage_report_percentage.txt", "99.53")
            self._write_merge_markers(tmpdir_path)
            downloaded_svg = (
                '<svg xmlns="http://www.w3.org/2000/svg" width="114" height="20" '
                'role="img" aria-label="coverage: 99.53%"><title>coverage: 99.53%</title></svg>'
            )
            requested_urls: list[str] = []

            def urlopen(request: object, timeout: float) -> _FakeHttpResponse:
                requested_urls.append(request.full_url)  # type: ignore[attr-defined]
                self.assertEqual(30, timeout)
                self.assertEqual("curl/8.7.1", request.get_header("User-agent"))  # type: ignore[attr-defined]
                return _FakeHttpResponse(downloaded_svg)

            with (
                self._temporary_working_directory(tmpdir_path),
                patch("urllib.request.urlopen", side_effect=urlopen),
            ):
                self._run_main(["generate_badges.py", "merge"])
                svg = (tmpdir_path / "badges" / "coverage-badge.svg").read_text(encoding="utf-8")

        self._assert_badge_title("coverage: 99.53%", svg)
        self.assertEqual(downloaded_svg, svg)
        self.assertIn("https://img.shields.io/badge/coverage-99.53%25-%234b0.svg", requested_urls)

    def test_merge_command_falls_back_to_local_coverage_badge(self) -> None:
        """Exercise offline coverage badge generation.

        The input file shape matches `coverage report --format=total --precision=2`. Re-running merge after changing the
        percentage verifies that the badge is regenerated from the latest coverage artifact.
        """

        with tempfile.TemporaryDirectory(prefix="bzfs_test_badges_") as tmpdir:
            tmpdir_path = Path(tmpdir)
            coverage_percentage = tmpdir_path / "coverage_report_percentage.txt"
            self._write_coverage_percentage(coverage_percentage, "99.53")
            self._write_merge_markers(tmpdir_path)

            with (
                self._temporary_working_directory(tmpdir_path),
                patch("urllib.request.urlopen", side_effect=OSError("offline")),
            ):
                self._run_main(["generate_badges.py", "merge"])
                svg = (tmpdir_path / "badges" / "coverage-badge.svg").read_text(encoding="utf-8")
                self._write_coverage_percentage(coverage_percentage, "100.00")
                self._run_main(["generate_badges.py", "merge"])
                full_svg = (tmpdir_path / "badges" / "coverage-badge.svg").read_text(encoding="utf-8")

        self._assert_badge_title("coverage: 99.53%", svg)
        self.assertIn(">99.53%<", svg)
        self.assertIn("#4b0", svg)
        self._assert_badge_title("coverage: 100.00%", full_svg)
        self.assertIn(">100.00%<", full_svg)

    def test_merge_command_generates_environment_svgs(self) -> None:
        """Exercise offline badges for merged environment marker directories.

        The marker files represent artifacts uploaded by matrix jobs. Blocking urlopen keeps the test on the local SVG path.
        """

        with tempfile.TemporaryDirectory(prefix="bzfs_test_badges_") as tmpdir:
            tmpdir_path = Path(tmpdir)
            self._write_merge_markers(tmpdir_path)

            with (
                self._temporary_working_directory(tmpdir_path),
                patch("urllib.request.urlopen", side_effect=OSError("offline")),
            ):
                self._run_main(["generate_badges.py", "merge"])
                zfs_svg = (tmpdir_path / "badges" / "zfs-badge.svg").read_text(encoding="utf-8")
                os_svg = (tmpdir_path / "badges" / "os-badge.svg").read_text(encoding="utf-8")
                python_svg = (tmpdir_path / "badges" / "python-badge.svg").read_text(encoding="utf-8")
                pypi_svg = (tmpdir_path / "badges" / "pypi-badge.svg").read_text(encoding="utf-8")

        self._assert_badge_title("os: Linux | FreeBSD", os_svg)
        self._assert_badge_title("python: 3.9 | 3.10 | 3.11 | 3.12 | 3.13 | 3.14 | 3.15", python_svg)
        self._assert_badge_title(
            "zfs: 0.8.3 | 2.1.5 | 2.1.15 | 2.2.2 | 2.2.9 | 2.3.6 | 2.3.7 | 2.4.0 | 2.4.1 | 2.4.2",
            zfs_svg,
        )
        self._assert_badge_title("pypi: ", pypi_svg)

    def _load_generate_badges_module(self) -> ModuleType:
        """Load generate_badges.py despite its non-package parent directory."""
        script = self.repo_root / ".github-workflow-scripts" / "generate_badges.py"
        spec = importlib.util.spec_from_file_location("generate_badges_under_test", script)
        if spec is None or spec.loader is None:
            self.fail(f"Could not load module spec for {script}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _run_main(self, argv: list[str]) -> None:
        """Run the script entry point in-process with explicit argv."""
        with patch.object(sys, "argv", argv), suppress_output():
            self.module.main()

    def _write_merge_markers(self, tmpdir_path: Path) -> None:
        """Create marker files shaped like downloaded CI badge artifacts."""
        for marker in [
            "badges/zfs/0.8.3",
            "badges/zfs/2.1.5",
            "badges/zfs/2.1.15",
            "badges/zfs/2.2.2",
            "badges/zfs/2.2.9",
            "badges/zfs/2.3.6",
            "badges/zfs/2.3.7",
            "badges/zfs/2.4.0",
            "badges/zfs/2.4.1",
            "badges/zfs/2.4.2",
            "badges/os/FreeBSD",
            "badges/os/Linux",
            "badges/python/3.12",
        ]:
            marker_path = tmpdir_path / marker
            marker_path.parent.mkdir(parents=True, exist_ok=True)
            marker_path.touch()

    def _write_coverage_percentage(self, coverage_percentage: Path, percentage: str) -> None:
        """Create coverage.py's total-percentage output file."""
        coverage_percentage.write_text(f"{percentage}\n", encoding="utf-8")

    def _assert_badge_title(self, expected: str, svg: str) -> None:
        """Check the SVG title text used by badge consumers."""
        self.assertIn(f"<title>{expected}</title>", svg)

    @contextmanager
    def _temporary_working_directory(self, path: Path) -> Iterator[None]:
        """Run code from a synthetic artifact directory for one test scope."""
        old_cwd = Path.cwd()
        try:
            os.chdir(path)
            yield
        finally:
            os.chdir(old_cwd)


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
