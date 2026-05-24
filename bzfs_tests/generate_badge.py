#!/usr/bin/env python3

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

# Inline script metadata conforming to https://packaging.python.org/specifications/inline-script-metadata
# /// script
# requires-python = ">=3.9"
# dependencies = []
# ///
#
"""
Given a label, text and color, generates a corresponding static shields.io SVG badge.

Downloads SVG badge from shields.io when available, otherwise falls back to local SVG generation.
Has zero dependencies beyond the Python standard library.
"""

from __future__ import (
    annotations,
)
import html
import sys
import urllib.parse
import urllib.request
from pathlib import (
    Path,
)


def main() -> None:
    """API for command line clients."""
    if len(sys.argv) != 6:
        raise SystemExit(
            f"Usage: {sys.argv[0]} <left_txt> <right_txt> <color> <output_file> <timeout>\n"
            f"Example: {sys.argv[0]} os 'Linux | FreeBSD' '#007ec6' os.svg 30"
        )

    _, left_txt, right_txt, color, output_file, timeout = sys.argv
    generate_badge(left_txt, right_txt, color, output_file, float(timeout))


def generate_badge(left_txt: str, right_txt: str, color: str, output_file: str, timeout: float = 30) -> None:
    """Writes an SVG badge for the given text."""
    if not color:
        color = "#007ec6"  # blue; see https://github.com/badges/shields/tree/master/badge-maker#colors
    try:
        if timeout == 0:
            raise ValueError("dummy")
        svg: str = _download_svg_badge(left_txt, right_txt, color, timeout=timeout)
        msg = "Successfully downloaded badge"
    except Exception:  # no network connectivity (or other error): produce badge locally
        svg = _build_svg_badge(left_txt, right_txt, color)
        msg = "Successfully built badge locally"
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(svg, encoding="utf-8")
    print(f"{msg} '{left_txt}' into {output_file}")


def _download_svg_badge(left_txt: str, right_txt: str, color: str, timeout: float) -> str:
    """Downloads a pretty SVG badge from shields.io."""
    quoted_left_txt = urllib.parse.quote(left_txt, safe="")
    quoted_right_txt = urllib.parse.quote(right_txt, safe="")
    quoted_color = urllib.parse.quote(color, safe="")
    url = f"https://img.shields.io/badge/{quoted_left_txt}-{quoted_right_txt}-{quoted_color}.svg"
    request = urllib.request.Request(url, headers={"User-Agent": "curl/8.7.1"})
    with urllib.request.urlopen(request, timeout=timeout) as response:  # fixed Shields URL
        svg: str = response.read().decode("utf-8")
    if not svg.lstrip().startswith("<svg"):
        raise ValueError(f"Downloaded badge is not SVG: {url}")
    return svg


def _build_svg_badge(left_txt: str, right_txt: str, color: str) -> str:
    """Locally creates a basic Shields-compatible SVG without requiring network connectivity."""

    def _text_width(text: str) -> int:  # Returns a simple text segment width with padding
        return max(10, round(sum(4 if char in " .,:;|!ilI'`" else 7.2 for char in text) + 10))

    left_width = _text_width(left_txt)
    right_width = _text_width(right_txt)
    width = left_width + right_width
    left_x = left_width / 2
    right_x = left_width + right_width / 2
    left = html.escape(left_txt, quote=True)
    right = html.escape(right_txt, quote=True)
    title = html.escape(f"{left_txt}: {right_txt}", quote=True)
    height = 20
    gradient_id = "badge-shine-gradient"
    clip_path_id = "badge-rounded-clip"

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" role="img" aria-label="{title}">
<title>{title}</title>
<linearGradient id="{gradient_id}" x2="0" y2="100%">
  <stop offset="0" stop-color="#bbb" stop-opacity=".1"/>
  <stop offset="1" stop-opacity=".1"/>
</linearGradient>
<clipPath id="{clip_path_id}"><rect width="{width}" height="{height}" rx="3" fill="#fff"/></clipPath>
<g clip-path="url(#{clip_path_id})">
  <rect width="{left_width}" height="{height}" fill="#555"/>
  <rect x="{left_width}" width="{right_width}" height="{height}" fill="{color}"/>
  <rect width="{width}" height="{height}" fill="url(#{gradient_id})"/>
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
