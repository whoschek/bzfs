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

import re
import subprocess

sudo_cmd = []


def set_sudo_cmd(sudo):
    global sudo_cmd
    sudo_cmd = sudo


def destroy_pool(pool_name, force=False):
    force = ['-f'] if force else []
    run_cmd(sudo_cmd + ['zpool', 'destroy'] + force + [pool_name])


def destroy(name, recursive=False, force=False):
    cmd = sudo_cmd + ['zfs', 'destroy']
    if recursive:
        cmd.append('-r')
    if force:
        cmd.append('-f')
    cmd.append(name)
    run_cmd(cmd)


def create_volume(dataset, path=None, mk_parents=True, size=None, props=[], blocksize=None, sparse=False):
    path = "" if path is None else '/' + path
    dataset = f"{dataset}{path}"
    cmd = sudo_cmd + ['zfs', 'create']
    if mk_parents:
        cmd.append('-p')
    if sparse:
        cmd.append('-s')
    if blocksize:
        cmd.append('-b')
        cmd.append(str(blocksize))
    if props:
        cmd += props
    cmd.append('-V')
    cmd.append(str(size))
    cmd.append(dataset)
    run_cmd(cmd)
    return dataset


def create_filesystem_simple(dataset, path=None, mk_parents=True, no_mount=True, props=[]):
    path = "" if path is None else '/' + path
    dataset = f"{dataset}{path}"
    cmd = sudo_cmd + ['zfs', 'create']
    if mk_parents:
        cmd += ['-p']
    if no_mount:
        cmd += ['-u']
    if props:
        cmd += props
    cmd.append(dataset)
    run_cmd(cmd)
    return dataset


zfs_version_is_at_least_2_1_0 = None


def create_filesystem(dataset, path=None, no_mount=True, props=[]):
    """implies mk_parents=True
    if no_mount=True:
    To ensure the datasets that we create do not get mounted, we apply a separate 'zfs create -p -u' invocation
    for each non-existing ancestor. This is because a single 'zfs create -p -u' applies the '-u' part only to
    the immediate dataset, rather than to the not-yet existing ancestors.
    If the zfs version doesn't support the 'zfs create -u' flag then we manually unmount immediately after each
    dataset creation.
    """
    mk_parents = True
    path = "" if path is None else '/' + path
    dataset = f"{dataset}{path}"
    parent = ""
    splits = dataset.split('/')
    for i, component in enumerate(splits):
        parent += component
        cmd = sudo_cmd + ['zfs', 'create']
        if mk_parents:
            cmd.append('-p')
        global zfs_version_is_at_least_2_1_0
        if no_mount:
            if zfs_version_is_at_least_2_1_0 is None:
                zfs_version_is_at_least_2_1_0 = is_version_at_least(zfs_version(), '2.1.0')
            if zfs_version_is_at_least_2_1_0:
                cmd.append('-u')
            else:
                already_exists = dataset_exists(parent)

        if props and i == len(splits) - 1:  # apply props to leaf dataset only, just like 'zfs create -p dataset'
            cmd += props
        cmd.append(parent)
        run_cmd(cmd)

        if no_mount and (not zfs_version_is_at_least_2_1_0) and not already_exists:
            # zfs < 2.1.0 does not know the zfs create -u flag, so we manually unmount after dataset creation
            cmd = sudo_cmd + ['zfs', 'unmount', parent]
            run_cmd(cmd)

        parent += '/'
    return dataset


def datasets(dataset):
    return zfs_list([dataset], types=['filesystem', 'volume'], max_depth=1)[1:]


def take_snapshot(dataset, snapshot_tag, recursive=False, props=[]):
    snapshot = dataset + '@' + snapshot_tag
    cmd = sudo_cmd + ['zfs', 'snapshot']
    if recursive:
        cmd.append('-r')
    if props:
        cmd += props
    cmd.append(snapshot)
    run_cmd(cmd)
    return snapshot


def snapshots(dataset):
    return zfs_list([dataset], types=['snapshot'], max_depth=1)


def create_bookmark(dataset, snapshot_tag, bookmark_tag):
    snapshot = dataset + '@' + snapshot_tag
    bookmark = dataset + '#' + bookmark_tag
    run_cmd(sudo_cmd + ['zfs', 'bookmark', snapshot, bookmark])
    return bookmark


def bookmarks(dataset):
    return zfs_list([dataset], types=['bookmark'], max_depth=1)


def snapshot_name(snapshot):
    return snapshot[snapshot.find('@') + 1 :]


def bookmark_name(bookmark):
    return bookmark[bookmark.find('#') + 1 :]


def dataset_property(dataset=None, prop=None):
    return zfs_list([dataset], props=[prop], types=['filesystem', 'volume'], max_depth=0)[0]
    # return zfs_get([dataset], props=[prop], types=['filesystem', 'volume'], max_depth=0, fields=['value'])[0]


def snapshot_property(snapshot, prop):
    return zfs_list([snapshot], props=[prop], types=['snapshot'], max_depth=0)[0]


def zfs_list(names=[], props=['name'], types=[], max_depth=None, parsable=True, sort_props=[]):
    cmd = ['zfs', 'list']
    if max_depth is None:
        cmd.append('-r')
    else:
        cmd.append('-d')
        cmd.append(str(max_depth))

    cmd.append('-H')
    if parsable:
        cmd.append('-p')

    if props:
        cmd.append('-o')
        cmd.append(','.join(props))

    if sort_props:
        cmd += sort_props

    if types:
        cmd.append('-t')
        cmd.append(','.join(types))

    if names:
        cmd += names

    return run_cmd(cmd)


def zfs_get(names=[], props=['all'], types=[], max_depth=None, parsable=True, fields=[], sources=[]):
    cmd = ['zfs', 'get']
    if max_depth is None:
        cmd.append('-r')
    else:
        cmd.append('-d')
        cmd.append(str(max_depth))

    cmd.append('-H')
    if parsable:
        cmd.append('-p')

    if fields:  # defaults to name,property,value,source
        cmd.append('-o')
        cmd.append(','.join(fields))

    if sources:  # 'local', 'default', 'inherited', 'temporary', 'received', 'none'. default is all sources
        cmd.append('-s')
        cmd.append(','.join(sources))

    if types:  # filesystem, snapshot, volume, bookmark, or all. default is all
        cmd.append('-t')
        cmd.append(','.join(types))

    assert len(props) > 0
    cmd.append(','.join(props))

    if names:
        cmd += names

    return run_cmd(cmd)


def dataset_exists(dataset):
    try:
        build(dataset)
        return True
    except subprocess.CalledProcessError:
        return False


def build(name, check=True):
    if check:
        if '@' in name:
            types = ['snapshot']
        elif '#' in name:
            types = ['bookmark']
        else:
            types = ['filesystem', 'volume']

        if len(zfs_list([name], types=types, max_depth=0)) == 0:
            raise ValueError('Cannot zfs_list: ' + name)

    return name


def zfs_version():
    """Example zfs-2.1.5~rc5-ubuntu3 -> 2.1.5"""
    try:
        lines = run_cmd(['zfs', '--version'])
    except subprocess.CalledProcessError:
        return None
    version = lines[0].split('-')[1].strip()
    match = re.fullmatch(r'(\d+\.\d+\.\d+).*', version)
    if match:
        return match.group(1)
    else:
        raise ValueError("Unparsable zfs version string: " + version)


def is_version_at_least(version_str: str, min_version_str: str) -> bool:
    """Check if the version string is at least the minimum version string."""
    if version_str is None:
        return False
    return tuple(map(int, version_str.split('.'))) >= tuple(map(int, min_version_str.split('.')))


def run_cmd(*params):
    return subprocess.run(*params, stdout=subprocess.PIPE, text=True, check=True).stdout.splitlines()
