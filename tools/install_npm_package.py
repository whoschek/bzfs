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
"""Installs an npm package (excluding transitive dependencies) into the local development cache without requiring `npm`.

This avoids the supply chain risk of installing the (about one hundred) transitive dependencies that `npm` requires for its
own internal functioning.

Downloads the pinned `package` tarball, verifies the checksum, and untars it under `.venv`.
"""

from __future__ import (
    annotations,
)
import fcntl
import hashlib
import inspect
import os
import shutil
import sys
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import (
    Path,
)


def main() -> None:
    """Install the package."""
    if len(sys.argv) != 5:
        raise SystemExit(f"usage: {Path(sys.argv[0]).name} package_name package_version package_tarball_url expected_sha256")
    package_name = sys.argv[1]  # e.g. "prettier"
    package_version = sys.argv[2]  # e.g. "3.8.3"
    package_tarball_url = sys.argv[3]  # e.g. "https://registry.npmjs.org/prettier/-/prettier-3.8.3.tgz"
    expected_sha256 = sys.argv[4]  # e.g. "c8a850e71b7366f1bac1885e05b2f929d47168009f3b6914413902ca8b980fde"
    assert package_name
    assert package_version
    assert package_tarball_url
    assert expected_sha256
    tarball_name = urllib.parse.urlsplit(package_tarball_url).path.rsplit("/", 1)[-1]  # e.g. "prettier-3.8.3.tgz"
    if not tarball_name:
        raise SystemExit(f"Missing tarball filename in package_tarball_url: {package_tarball_url}")

    repo_root_dir = Path(__file__).resolve().parent.parent
    install_dir = repo_root_dir / ".venv" / package_name
    tarball = install_dir / tarball_name
    target_dir = install_dir / f"{package_name}-{package_version}"
    assert tarball != target_dir
    tmp_target_dir = install_dir / f".{package_name}-{package_version}.tmp"
    done_marker = target_dir / ".install_done"
    if done_marker.is_file():
        return

    install_dir.mkdir(parents=True, exist_ok=True)
    lock = os.open(install_dir, os.O_RDONLY)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX)  # serialize concurrent installs
        if done_marker.is_file():
            return

        # download
        retries = 3
        retry_sleep_secs = 5
        attempt = 1
        while True:
            if not package_tarball_url.startswith("https://"):
                raise SystemExit(f"Invalid scheme in package_tarball_url: {package_tarball_url}")
            try:
                with urllib.request.urlopen(package_tarball_url, timeout=30) as response:  # noqa: S310
                    tarball.write_bytes(response.read())
                break
            except (TimeoutError, urllib.error.URLError):
                if attempt > retries:
                    print(f"ERROR: {package_tarball_url} download failed after {attempt} attempts", file=sys.stderr)
                    raise
                print(
                    f"ERROR: {package_tarball_url} download failed; retrying {attempt}/{retries} in {retry_sleep_secs}s ...",
                    file=sys.stderr,
                )
                time.sleep(retry_sleep_secs)
                retry_sleep_secs = retry_sleep_secs * 2  # exponential backoff
                attempt += 1

        # verify checksum
        actual_sha256 = hashlib.sha256(tarball.read_bytes()).hexdigest()
        if actual_sha256 != expected_sha256:
            raise SystemExit(f"sha256 mismatch for {tarball}: {actual_sha256} != {expected_sha256}")

        # untar
        if tmp_target_dir.exists():
            shutil.rmtree(tmp_target_dir)
        tmp_target_dir.mkdir()
        with tarfile.open(tarball, "r") as archive:
            if "filter" in inspect.signature(tarfile.TarFile.extractall).parameters:
                archive.extractall(tmp_target_dir, filter="data")
            else:
                archive.extractall(tmp_target_dir)  # noqa: S202  # Python < 3.12 lacks filters

        (tmp_target_dir / done_marker.name).touch()
        tarball.unlink()
        if target_dir.exists():
            shutil.rmtree(target_dir)
        tmp_target_dir.replace(target_dir)
    finally:
        os.close(lock)
    print(f"Installed {package_name}-{package_version} at {target_dir}")


#############################################################################
if __name__ == "__main__":
    main()
