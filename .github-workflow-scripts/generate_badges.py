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
import os
import platform
import re
import sys
from pathlib import (
    Path,
)
from typing import (
    Final,
)

from bzfs_tests.generate_badge import (
    generate_badge,
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
        generate_badge("zfs", _merge_versions(f"{ROOT_DIR}/zfs", natsort=True), color, _output_file("zfs"))
        generate_badge("os", _merge_versions(f"{ROOT_DIR}/os"), color, _output_file("os"))
        py_versions = " | ".join(["3.9", "3.10", "3.11", "3.12", "3.13", "3.14", "3.15"])
        generate_badge("python", py_versions, color, _output_file("python"))
        pypi_versions = ""
        generate_badge("pypi", pypi_versions, color, _output_file("pypi"))
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
            generate_badge("coverage", f"{percent:.2f}%", color, _output_file("coverage"))
    else:
        raise SystemExit(f"Unsupported badge command: {command}")


def _output_file(left_txt: str) -> str:
    return f"{ROOT_DIR}/{left_txt}-badge.svg"


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


#############################################################################
if __name__ == "__main__":
    main()
