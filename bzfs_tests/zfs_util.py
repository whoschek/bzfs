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

from __future__ import annotations
import platform
import re
import subprocess
from typing import List, Mapping, Sequence, cast

sudo_cmd = []


def set_sudo_cmd(sudo: list[str]) -> None:
    global sudo_cmd
    sudo_cmd = sudo


def destroy_pool(pool_name: str, force: bool = False) -> None:
    force_flag = ["-f"] if force else []
    run_cmd(sudo_cmd + ["zpool", "destroy"] + force_flag + [pool_name])


def destroy(name: str, recursive: bool = False, force: bool = False) -> None:
    cmd = sudo_cmd + ["zfs", "destroy"]
    if recursive:
        cmd.append("-r")
    if force:
        cmd.append("-f")
    cmd.append(name)
    run_cmd(cmd)


def destroy_snapshots(dataset: str, snapshots: Sequence[str] = ()) -> None:
    if len(snapshots) == 0:
        return
    if is_solaris_zfs():  # solaris-11.4 does not support passing multiple snapshots per CLI invocation
        for snapshot in snapshots:
            destroy(snapshot)
    else:
        cmd = sudo_cmd + ["zfs", "destroy"]
        cmd += [dataset + "@" + ",".join([snap[snap.find("@") + 1 :] for snap in snapshots])]
        run_cmd(cmd)


def create_volume(
    dataset: str,
    path: str | None = None,
    mk_parents: bool = True,
    size: int | str | None = None,
    props: list[str] | None = None,
    blocksize: int | None = None,
    sparse: bool = False,
) -> str:
    if props is None:
        props = []
    path = "" if path is None else "/" + path
    dataset = f"{dataset}{path}"
    cmd = sudo_cmd + ["zfs", "create"]
    if mk_parents:
        cmd.append("-p")
    if sparse:
        cmd.append("-s")
    if blocksize:
        cmd.append("-b")
        cmd.append(str(blocksize))
    if props:
        cmd += props
    cmd.append("-V")
    cmd.append(str(size))
    cmd.append(dataset)
    run_cmd(cmd)
    return dataset


def create_filesystem_simple(
    dataset: str,
    path: str | None = None,
    mk_parents: bool = True,
    no_mount: bool = True,
    props: list[str] | None = None,
) -> str:
    if props is None:
        props = []
    path = "" if path is None else "/" + path
    dataset = f"{dataset}{path}"
    cmd = sudo_cmd + ["zfs", "create"]
    if mk_parents:
        cmd += ["-p"]
    if no_mount:
        cmd += ["-u"]
    if props:
        cmd += props
    cmd.append(dataset)
    run_cmd(cmd)
    return dataset


zfs_version_is_at_least_2_1_0 = None


def create_filesystem(
    dataset: str,
    path: str | None = None,
    no_mount: bool = True,
    props: list[str] | None = None,
) -> str:
    """implies mk_parents=True
    if no_mount=True:
    To ensure the datasets that we create do not get mounted, we apply a separate 'zfs create -p -u' invocation
    for each non-existing ancestor. This is because a single 'zfs create -p -u' applies the '-u' part only to
    the immediate dataset, rather than to the not-yet existing ancestors.
    If the zfs version doesn't support the 'zfs create -u' flag then we manually unmount immediately after each
    dataset creation.
    """
    if props is None:
        props = []
    mk_parents = True
    path = "" if path is None else "/" + path
    dataset = f"{dataset}{path}"
    parent = ""
    splits = dataset.split("/")
    for i, component in enumerate(splits):
        parent += component
        cmd = sudo_cmd + ["zfs", "create"]
        if mk_parents:
            cmd.append("-p")
        global zfs_version_is_at_least_2_1_0
        if no_mount:
            if zfs_version_is_at_least_2_1_0 is None:
                version = zfs_version()
                if version is None:
                    version = "0.0.0"
                zfs_version_is_at_least_2_1_0 = is_version_at_least(version, "2.1.0")
            if zfs_version_is_at_least_2_1_0:
                cmd.append("-u")
            else:
                already_exists = dataset_exists(parent)

        if props and i == len(splits) - 1:  # apply props to leaf dataset only, just like 'zfs create -p dataset'
            cmd += props
        cmd.append(parent)
        run_cmd(cmd)

        if no_mount and (not zfs_version_is_at_least_2_1_0) and not already_exists:
            # zfs < 2.1.0 does not know the zfs create -u flag, so we manually unmount after dataset creation
            run_cmd(sudo_cmd + ["zfs", "unmount", parent])

        parent += "/"
    return dataset


def datasets(dataset: str) -> list[str]:
    return cast(List[str], zfs_list([dataset], types=["filesystem", "volume"], max_depth=1))[1:]


def take_snapshot(
    dataset: str,
    snapshot_tag: str,
    recursive: bool = False,
    props: list[str] | None = None,
) -> str:
    if props is None:
        props = []
    snapshot = dataset + "@" + snapshot_tag
    cmd = sudo_cmd + ["zfs", "snapshot"]
    if recursive:
        cmd.append("-r")
    if props:
        cmd += props
    cmd.append(snapshot)
    run_cmd(cmd)
    return snapshot


def snapshots(dataset: str, max_depth: int | None = 1) -> list[str]:
    return cast(List[str], zfs_list([dataset], types=["snapshot"], max_depth=max_depth))


def create_bookmark(dataset: str, snapshot_tag: str, bookmark_tag: str) -> str:
    snapshot = dataset + "@" + snapshot_tag
    bookmark = dataset + "#" + bookmark_tag
    run_cmd(sudo_cmd + ["zfs", "bookmark", snapshot, bookmark])
    return bookmark


def bookmarks(dataset: str, max_depth: int = 1) -> list[str]:
    return cast(List[str], zfs_list([dataset], types=["bookmark"], max_depth=max_depth))


def snapshot_name(snapshot: str) -> str:
    return snapshot[snapshot.find("@") + 1 :]


def bookmark_name(bookmark: str) -> str:
    return bookmark[bookmark.find("#") + 1 :]


def dataset_property(dataset: str, prop: str) -> str:
    return cast(str, zfs_list([dataset], props=[prop], types=["filesystem", "volume"], max_depth=0, splitlines=False))
    # return zfs_get(
    #     [dataset], props=[prop], types=["filesystem", "volume"], max_depth=0, fields=["value"], splitlines=False
    # )


def snapshot_property(snapshot: str, prop: str) -> str:
    return cast(str, zfs_list([snapshot], props=[prop], types=["snapshot"], max_depth=0, splitlines=False))


def zfs_list(
    names: list[str] | None = None,
    props: list[str] | None = None,
    types: list[str] | None = None,
    max_depth: int | None = None,
    parsable: bool = True,
    sort_props: list[str] | None = None,
    splitlines: bool = True,
) -> list[str] | str:
    cmd = ["zfs", "list"]
    if names is None:
        names = []
    if props is None:
        props = ["name"]
    if types is None:
        types = []
    if sort_props is None:
        sort_props = []
    if max_depth is None:
        cmd.append("-r")
    else:
        cmd.append("-d")
        cmd.append(str(max_depth))

    cmd.append("-H")
    if parsable:
        cmd.append("-p")

    if props:
        cmd.append("-o")
        cmd.append(",".join(props))

    if sort_props:
        cmd += sort_props

    if types:
        cmd.append("-t")
        cmd.append(",".join(types))

    if names:
        cmd += names

    return run_cmd(cmd, splitlines=splitlines)


def zfs_get(
    names: list[str] | None = None,
    props: list[str] | None = None,
    types: list[str] | None = None,
    max_depth: int | None = None,
    parsable: bool = True,
    fields: list[str] | None = None,
    sources: list[str] | None = None,
    splitlines: bool = True,
) -> list[str] | str:
    cmd = ["zfs", "get"]
    if names is None:
        names = []
    if props is None:
        props = ["all"]
    if types is None:
        types = []
    if fields is None:
        fields = []
    if sources is None:
        sources = []
    if max_depth is None:
        cmd.append("-r")
    else:
        cmd.append("-d")
        cmd.append(str(max_depth))

    cmd.append("-H")
    if parsable:
        cmd.append("-p")

    if fields:  # defaults to name,property,value,source
        cmd.append("-o")
        cmd.append(",".join(fields))

    if sources:  # 'local', 'default', 'inherited', 'temporary', 'received', 'none'. default is all sources
        cmd.append("-s")
        cmd.append(",".join(sources))

    if types:  # filesystem, snapshot, volume, bookmark, or all. default is all
        cmd.append("-t")
        cmd.append(",".join(types))

    assert len(props) > 0
    cmd.append(",".join(props))

    if names:
        cmd += names

    return run_cmd(cmd, splitlines=splitlines)


def zfs_set(names: list[str] | None = None, properties: Mapping[str, str] | None = None) -> None:
    if names is None:
        names = []
    if properties is None:
        properties = {}

    def run_zfs_set(props: list[str]) -> None:
        cmd = sudo_cmd + ["zfs", "set"]
        for prop in props:
            cmd.append(prop)
        if names:
            cmd += names
        run_cmd(cmd)

    if not is_solaris_zfs():  # send all properties in a batch
        run_zfs_set([f"{name}={value}" for name, value in properties.items()])
    else:  # solaris-14.0 does not accept multiple properties per 'zfs set' CLI call
        for name, value in properties.items():
            run_zfs_set([f"{name}={value}"])


def zfs_inherit(
    names: list[str] | None = None,
    propname: str | None = None,
    recursive: bool = False,
    revert: bool = False,
) -> None:
    if names is None:
        names = []
    assert propname
    cmd = sudo_cmd + ["zfs", "inherit"]
    if recursive:
        cmd.append("-r")
    if revert:
        cmd.append("-S")
    cmd.append(propname)
    if names:
        cmd += names

    run_cmd(cmd)


def dataset_exists(dataset: str) -> bool:
    try:
        build(dataset)
        return True
    except subprocess.CalledProcessError:
        return False


def build(name: str, check: bool = True) -> str:
    if check:
        if "@" in name:
            types = ["snapshot"]
        elif "#" in name:
            types = ["bookmark"]
        else:
            types = ["filesystem", "volume"]

        if len(zfs_list([name], types=types, max_depth=0)) == 0:
            raise ValueError("Cannot zfs_list: " + name)

    return name


def zfs_version() -> str | None:
    """Example zfs-2.1.5~rc5-ubuntu3 -> 2.1.5"""
    try:
        # on Linux, 'zfs --version' returns with zero status and prints the correct info
        # on FreeBSD, 'zfs --version' always prints the same (correct) info as Linux, but nonetheless sometimes
        # returns with non-zero status (sometimes = if the zfs kernel module is not loaded)
        # on Solaris, 'zfs --version' returns with non-zero status without printing useful info as the --version
        # option is not known there
        lines = subprocess.run(["zfs", "--version"], capture_output=True, text=True, check=True).stdout
        assert lines
    except subprocess.CalledProcessError as e:
        if "unrecognized command '--version'" in e.stderr and "run: zfs help" in e.stderr:
            return None  # solaris-11.4 zfs does not know --version flag
        elif not e.stdout.startswith("zfs"):
            raise
        else:
            lines = e.stdout  # FreeBSD if the zfs kernel module is not loaded
            assert lines

    line = lines.splitlines()[0]
    version = line.split("-")[1].strip()
    match = re.fullmatch(r"(\d+\.\d+\.\d+).*", version)
    if match:
        return match.group(1)
    else:
        raise ValueError("Unparsable zfs version string: " + version)


def is_version_at_least(version_str: str, min_version_str: str) -> bool:
    """Check if the version string is at least the minimum version string."""
    return tuple(map(int, version_str.split("."))) >= tuple(map(int, min_version_str.split(".")))


def is_solaris_zfs() -> bool:
    return platform.system() == "SunOS"


def run_cmd(cmd: Sequence[str], splitlines: bool = True) -> list[str] | str:
    stdout = subprocess.run(cmd, stdout=subprocess.PIPE, text=True, check=True).stdout
    assert stdout is not None
    return stdout.splitlines() if splitlines else stdout[0:-1]  # omit trailing newline char
