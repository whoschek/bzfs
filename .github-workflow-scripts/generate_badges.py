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

import os
import platform
import re
import subprocess
import sys
from pathlib import Path

dir = "badges"


def main():
    """Generate README.md badges for zfs/os/python version. Called by coverage.sh.
    Uses https://smarie.github.io/python-genbadge/"""
    if sys.argv[1] != "merge":
        from tests.zfs_util import zfs_version

        version = zfs_version()
        if version is None:
            # Example: "11.4" for solaris
            version = subprocess.run(["uname", "-v"], stdout=subprocess.PIPE, text=True, check=True).stdout
            version = version.strip().split()[0]

        touch(f"{dir}/zfs", version)
        touch(f"{dir}/python", f"{sys.version_info.major}.{sys.version_info.minor}")
        touch(f"{dir}/os", platform.system().split()[0])
    else:
        color = "#007ec6"  # "blue" # see https://github.com/badges/shields/tree/master/badge-maker#colors
        generate_badge("zfs", merge_versions(f"{dir}/zfs", natsort=True), color)
        generate_badge("os", merge_versions(f"{dir}/os"), color)
        py_versions = merge_versions(f"{dir}/python")
        py_versions = " | ".join(["3.7", "3.8", "3.9", "3.10", "3.11", "3.12", "3.13"])
        generate_badge("python", py_versions, color)


def touch(dir, path):
    os.makedirs(dir, exist_ok=True)
    Path(f"{dir}/{str(path)}").touch()


def merge_versions(dir, natsort=False):
    versions = [str(file) for file in os.listdir(dir)]
    if natsort:
        versions = sort_versions(versions)
    else:
        versions = sorted(versions)
        versions = ["Solaris" if v == "SunOS" else v for v in versions]
        if "Linux" in versions:
            versions = ["Linux"] + [v for v in versions if v != "Linux"]
    return " | ".join(versions)


def sort_versions(version_list):

    def is_valid_version(version):  # is in the form x.y.z ?
        return re.match(r"^\d+(\.\d+){0,2}$", version)

    def version_key(version):  # Split version into components and convert to integers
        return [int(part) for part in version.split(".")]

    valid_versions = [v for v in version_list if is_valid_version(v)]
    invalid_versions = [v for v in version_list if not is_valid_version(v)]
    return sorted(valid_versions, key=version_key) + sorted(invalid_versions)


def generate_badge(left_txt, right_txt, color):
    from genbadge import Badge

    badge = Badge(left_txt=left_txt, right_txt=right_txt, color=color)
    print(badge)
    output_dir = f"{dir}"
    os.makedirs(output_dir, exist_ok=True)
    badge.write_to(f"{output_dir}/{left_txt}-badge.svg", use_shields=True)


#############################################################################
if __name__ == "__main__":
    main()
