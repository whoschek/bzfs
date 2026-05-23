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
#
"""Generates README.md badges for tested zfs/os/python versions and coverage percentage.

Called from within CI by coverage.sh with 'generate' option, and in a later CI phase again with 'merge' option.

Downloads SVG badges from shields.io when available, otherwise falls back to local SVG generation.
Has zero dependencies beyond the Python standard library.
ZFS CLI may or may not be available.
"""

from __future__ import (
    annotations,
)
import html
import os
import platform
import re
import sys
import urllib.parse
import urllib.request
from pathlib import (
    Path,
)
from typing import (
    Final,
)

ROOT_DIR: Final[str] = "badges"
COVERAGE_PERCENTAGE_FILE: Final[str] = "coverage_report_percentage.txt"


def main() -> None:
    """API for command line clients."""

    if len(sys.argv) != 2:
        raise SystemExit("Usage: generate_badges.py generate|merge")

    command = sys.argv[1]
    if command == "generate":
        from bzfs_tests.zfs_util import (
            zfs_version,
        )

        version = zfs_version()
        _touch(f"{ROOT_DIR}/zfs", version)
        _touch(f"{ROOT_DIR}/python", f"{sys.version_info.major}.{sys.version_info.minor}")
        _touch(f"{ROOT_DIR}/os", platform.system().split()[0])
    elif command == "merge":
        color = "#007ec6"  # blue; see https://github.com/badges/shields/tree/master/badge-maker#colors
        _generate_badge("zfs", _merge_versions(f"{ROOT_DIR}/zfs", natsort=True), color)
        _generate_badge("os", _merge_versions(f"{ROOT_DIR}/os"), color)
        py_versions = " | ".join(["3.9", "3.10", "3.11", "3.12", "3.13", "3.14", "3.15"])
        _generate_badge("python", py_versions, color)
        pypi_versions = ""
        _generate_badge("pypi", pypi_versions, color)
        coverage_percentage = Path(COVERAGE_PERCENTAGE_FILE)
        if coverage_percentage.exists():
            percent = float(coverage_percentage.read_text(encoding="utf-8").strip())
            if percent >= 90.0:
                color = "#4b0"
            elif percent >= 75.0:
                color = "#97ca00"
            elif percent >= 50.0:
                color = "#fe7d37"
            else:
                color = "#e05d44"
            _generate_badge("coverage", f"{percent:.2f}%", color)
    else:
        raise SystemExit(f"Unsupported badge command: {command}")


def _touch(output_dir: str, path: str) -> None:
    """Creates an empty marker file representing a badge version."""
    os.makedirs(output_dir, exist_ok=True)
    Path(f"{output_dir}/{path}").touch()


def _merge_versions(input_dir: str, natsort: bool = False) -> str:
    """Gathers all versions produced by previous jobs, via marker files, and returns a pipe-separated list of versions."""

    versions = [str(file) for file in os.listdir(input_dir)]
    if natsort:
        versions = _sort_versions(versions)
    else:
        versions = sorted(versions)
        if "Linux" in versions:
            versions = ["Linux"] + [v for v in versions if v != "Linux"]
    return " | ".join(versions)


def _sort_versions(version_list: list[str]) -> list[str]:
    """Sorts a list of version strings in natural order."""

    def is_valid_version(version: str) -> re.Match | None:  # is in the form x.y.z ?
        return re.match(r"^\d+(\.\d+){0,2}$", version)

    def version_key(version: str) -> list[int]:  # Split version into components and convert to integers
        return [int(part) for part in version.split(".")]

    valid_versions = [v for v in version_list if is_valid_version(v)]
    invalid_versions = [v for v in version_list if not is_valid_version(v)]
    return sorted(valid_versions, key=version_key) + sorted(invalid_versions)


def _generate_badge(left_txt: str, right_txt: str, color: str) -> None:
    """Writes an SVG badge for the given text."""

    os.makedirs(ROOT_DIR, exist_ok=True)
    output_file = f"{ROOT_DIR}/{left_txt}-badge.svg"
    try:
        svg: str = _download_svg_badge(left_txt, right_txt, color)
        msg = "Successfully downloaded badge"
    except Exception:  # no network connectivity (or other error): produce badge locally
        svg = _build_svg_badge(left_txt, right_txt, color)
        msg = "Successfully built badge locally"
    Path(output_file).write_text(svg, encoding="utf-8")
    print(f"{msg} '{left_txt}' into {output_file}")


def _download_svg_badge(left_txt: str, right_txt: str, color: str) -> str:
    """Downloads a pretty SVG badge from shields.io."""

    quoted_left_txt = urllib.parse.quote(left_txt, safe="")
    quoted_right_txt = urllib.parse.quote(right_txt, safe="")
    quoted_color = urllib.parse.quote(color, safe="")
    url = f"https://img.shields.io/badge/{quoted_left_txt}-{quoted_right_txt}-{quoted_color}.svg"
    request = urllib.request.Request(url, headers={"User-Agent": "curl/8.7.1"})
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310  # fixed Shields URL
        svg = response.read().decode("utf-8")
    if not svg.lstrip().startswith("<svg"):
        raise ValueError(f"Downloaded badge is not SVG: {url}")
    return svg


def _build_svg_badge(left_txt: str, right_txt: str, color: str) -> str:
    """Locally creates a basic Shields-compatible SVG without requiring network connectivity."""

    def _text_width(text: str) -> int:  # Returns a simple text segment width with padding
        return max(10, round(sum(4 if char in " .,:;|!ilI'`" else 7.2 for char in text) + 10))

    left_text_width = _text_width(left_txt)
    right_text_width = _text_width(right_txt)
    left_width = left_text_width
    right_width = right_text_width
    width = left_width + right_width
    left_x = left_width / 2
    right_x = left_width + right_width / 2
    left = html.escape(left_txt, quote=True)
    right = html.escape(right_txt, quote=True)
    title = html.escape(f"{left_txt}: {right_txt}", quote=True)
    gradient_id = "badge-shine-gradient"
    clip_path_id = "badge-rounded-clip"

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="20" role="img" aria-label="{title}">
<title>{title}</title>
<linearGradient id="{gradient_id}" x2="0" y2="100%">
  <stop offset="0" stop-color="#bbb" stop-opacity=".1"/>
  <stop offset="1" stop-opacity=".1"/>
</linearGradient>
<clipPath id="{clip_path_id}"><rect width="{width}" height="20" rx="3" fill="#fff"/></clipPath>
<g clip-path="url(#{clip_path_id})">
  <rect width="{left_width}" height="20" fill="#555"/>
  <rect x="{left_width}" width="{right_width}" height="20" fill="{color}"/>
  <rect width="{width}" height="20" fill="url(#{gradient_id})"/>
</g>
<g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" font-size="11">
  <text x="{left_x:.1f}" y="15" fill="#010101" fill-opacity=".3">{left}</text>
  <text x="{left_x:.1f}" y="14">{left}</text>
  <text x="{right_x:.1f}" y="15" fill="#010101" fill-opacity=".3">{right}</text>
  <text x="{right_x:.1f}" y="14">{right}</text>
</g>
</svg>
"""


#############################################################################
if __name__ == "__main__":
    main()
