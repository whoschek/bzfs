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
#
"""Install the pinned shellcheck binary and symlink it into .venv/bin/shellcheck."""

from __future__ import (
    annotations,
)
import os
import platform
import subprocess
import sys
from pathlib import (
    Path,
)
from typing import (
    Final,
)

_DOWNLOAD_URL: Final[str] = "https://github.com/koalaman/shellcheck/releases/download/"
_VERSION: Final[str] = "0.11.0"
_SHA256: Final[dict[str, str]] = {
    "darwin.aarch64": "339b930feb1ea764467013cc1f72d09cd6b869ebf1013296ba9055ab2ffbd26f",
    "darwin.x86_64": "c2c15e08df0e8fbc374c335b230a7ee958c313fa5714817a59aa59f1aa594f51",
    "linux.aarch64": "68a8133197a50beb8803f8d42f9908d1af1c5540d4bb05fdfca8c1fa47decefc",
    "linux.armv6hf": "89f29e76e881122416eb95947f812b1496ff9a46d1e1676abe1e3f3f903b0f46",
    "linux.riscv64": "a70e86454e9ae1a328aeafe62629d04ffea93b99138bfe1203083e2621b5ca4f",
    "linux.x86_64": "b7af85e41cc99489dcc21d66c6d5f3685138f06d34651e6d34b42ec6d54fe6f6",
}


def main() -> None:
    """Install the package."""
    repo = Path(__file__).resolve().parent.parent
    system = platform.system().lower()
    machine = platform.machine().lower()
    machine = {"amd64": "x86_64", "arm64": "aarch64", "armv7l": "armv6hf", "x64": "x86_64"}.get(machine, machine)
    platform_id = f"{system}.{machine}"
    if platform_id not in _SHA256:
        raise SystemExit(f"ERROR: Unsupported shellcheck platform {platform_id}")

    package = f"shellcheck-{platform_id.replace('.', '-')}"
    target_dir = repo / ".venv" / package / f"{package}-{_VERSION}"
    binary = target_dir / f"shellcheck-v{_VERSION}" / "shellcheck"
    link = repo / ".venv" / "bin" / "shellcheck"
    if _is_expected_shellcheck_link(link, binary):
        return  # already installed

    url = f"{_DOWNLOAD_URL}/v{_VERSION}/shellcheck-v{_VERSION}.{platform_id}.tar.gz"
    cmd = [sys.executable, str(repo / "tools" / "install_npm_package.py"), package, _VERSION, url, _SHA256[platform_id]]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
    if not binary.is_file():
        raise SystemExit(f"ERROR: Missing {binary}")

    link.parent.mkdir(parents=True, exist_ok=True)
    tmp = link.with_name(f".shellcheck.{os.getpid()}.tmp")
    tmp.unlink(missing_ok=True)
    tmp.symlink_to(os.path.relpath(binary, link.parent))
    tmp.replace(link)  # atomic rename


def _is_expected_shellcheck_link(link: Path, binary: Path) -> bool:
    if not binary.is_file() or not link.is_symlink():
        return False
    try:
        return link.resolve(strict=True) == binary.resolve(strict=True)
    except FileNotFoundError:
        return False


#############################################################################
if __name__ == "__main__":
    main()
